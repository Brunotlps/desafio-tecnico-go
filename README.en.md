# Wager Service — wager processing (Go, PostgreSQL, Keycloak, SQS)

> 🇧🇷 [Versão em português](README.md) · 🇬🇧 English version (this file) · Architecture: [`ARCHITECTURE.en.md`](ARCHITECTURE.en.md)

Implementation of the challenge described in [`descricao-desafio/README.md`](descricao-desafio/README.md) (the normative source, which is not
modified here; it is written in Portuguese and has no English version). This repository contains the full financial flow
*wallet opening → BET / WIN / LOSS / REFUND / ROLLBACK (with pending references) → queries*, available over HTTP and through an SQS consumer with a
durable inbox, with real authentication, a ledger, persistent idempotency, a transactional outbox with a publisher to an SNS FIFO topic, reads (paginated ledger and
reconciliation) and metrics.

> **Summary status** — what exists and what does **not** exist is in the section [Scope and open items](#scope-and-open-items).
> Decisions and interpretations are in [`ARCHITECTURE.en.md`](ARCHITECTURE.en.md).

## Prerequisites

| Tool           | Tested version | Use                                   |
| -------------- | -------------- | ------------------------------------- |
| Go             | 1.26.1         | build and tests (declared in `go.mod` and in the `Dockerfile`) |
| Docker + Compose v2 | 29.x / 5.x | PostgreSQL, Keycloak, MiniStack (broker), API |
| curl, python3  | —              | examples in this README               |

Memory: Keycloak + MiniStack + PostgreSQL use ~2 GB. Published ports (on `127.0.0.1` only):
PostgreSQL **5433**, Keycloak **8081**, broker (MiniStack) **4566**, API **8080** (override with
`POSTGRES_PORT`, `KEYCLOAK_PORT`, `BROKER_PORT`, `API_PORT`).

## Quick start

```sh
docker compose up --build        # postgres, keycloak, ministack, broker-provision and migrate (one-shot) and api
curl -s localhost:8080/health/ready
# {"checks":{"postgres":"ok","sqs":"ok"},"status":"ok"}
```

The first start imports the Keycloak realm (`deploy/keycloak/wager-realm.json`), provisions the broker (queues, topic, identities,
policies and credentials — the one-shot service `broker-provision`, see [Broker](#broker-ministack-with-authtrue)) and applies the migrations
(`migrate up`). The API only starts after both one-shot services finish successfully. To start over from scratch: `docker compose down -v`
(the `.local/broker` directory, which holds synthetic credentials, belongs to the host and stays; provisioning reconciles it).

### Test identities (Keycloak, realm `wager`, `client_credentials`)

| client_id               | secret                              | role               | providerId (claim) | Use                                   |
| ----------------------- | ----------------------------------- | ------------------ | ------------------ | ------------------------------------- |
| `internal-service`      | `internal-service-dev-secret`       | `internal-service` | —                  | open/query wallets; read any transaction |
| `provider-a`            | `provider-a-dev-secret`             | `provider`         | `provider-a`       | send a BET and query its own transactions |
| `provider-b`            | `provider-b-dev-secret`             | `provider`         | `provider-b`       | same, to test isolation                |
| `provider-a-shortlived` | `provider-a-shortlived-dev-secret`  | `provider`         | `provider-a`       | token valid for 3 s (expiration test)  |
| `no-roles-client`       | `no-roles-dev-secret`               | —                  | —                  | authenticated, without permission (403) |
| `wrong-audience-client` | `wrong-audience-dev-secret`         | `provider`         | `provider-a`       | token without the `wager-api` audience (401) |

All of them are **development values**, not real secrets. Keycloak console: <http://localhost:8081> (`admin`/`admin`).

### Complete example

```sh
KC=http://localhost:8081/realms/wager/protocol/openid-connect/token
token() { curl -s -X POST $KC -d grant_type=client_credentials -d client_id=$1 -d client_secret=$2 \
          | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])'; }
INTERNAL=$(token internal-service internal-service-dev-secret)
PROVIDER_A=$(token provider-a provider-a-dev-secret)
PLAYER=$(python3 -c 'import uuid; print(uuid.uuid4())')

# 1. open a wallet (internal service only) -> 201
curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $INTERNAL" \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}"
# {"id":"0192...","playerId":"...","balance":{"amount":"100.00","currency":"BRL"},"version":1}
WALLET=<returned id>

# 2. BET as provider-a -> 200 PROCESSED
curl -s -X POST localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_A" -H "Idempotency-Key: provider-a:transaction-123" \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"80.00\",\"currency\":\"BRL\"}}"
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"20.00","currency":"BRL"},"idempotentReplay":false}

# 3. repeat the same command -> 200 with "idempotentReplay":true and the original balance (20.00)
# 4. another BET of 80.00 (new key and new external id) -> 422 REJECTED / INSUFFICIENT_FUNDS
# 5. queries
curl -s -H "Authorization: Bearer $PROVIDER_A" localhost:8080/providers/provider-a/wagering/transactions/transaction-123
curl -s -H "Authorization: Bearer $INTERNAL"   localhost:8080/wallets/$WALLET
```

### Implemented HTTP contract

| Method and route                                                      | Who                | Success |
| --------------------------------------------------------------------- | ------------------ | ------- |
| `POST /wallets`                                                       | `internal-service` | 201     |
| `GET /wallets/{walletId}`                                             | `internal-service` | 200     |
| `POST /wagering/transactions` (`Idempotency-Key` header required; `BET`, `WIN` (with or without a reference), `LOSS`, `REFUND`, `ROLLBACK`) | `provider` | 200 (`PROCESSED`) · 202 (`PENDING_REFERENCE`) · 422 (`REJECTED`) |
| `GET /wagering/transactions/{transactionId}`                          | `provider` (its own only) · `internal-service` (all) | 200 |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | `provider` (its own id only) · `internal-service` | 200 |
| `GET /health/live`, `GET /health/ready`                               | public             | 200 / 503 |
| `GET /wallets/{walletId}/ledger?cursor=&limit=`                       | `internal-service` | 200 (see "Paginated ledger") |
| `POST /wallets/{walletId}/reconciliation`                             | `internal-service` | 200 (see "Reconciliation") |
| `GET /metrics`                                                        | `internal-service` | 200 (see [Metrics](#metrics)) |

Error responses: `{"code":"...","message":"..."}`.

### Paginated ledger

`GET /wallets/{walletId}/ledger?cursor=<opaque>&limit=<1..100>` — `internal-service` only (a provider gets 403; no token or an invalid token gets 401; no data and no effects).

* Ascending order by the ledger's **`seq` position**, within the wallet's scope; continuation is `seq > last returned` (**no `OFFSET`**). `seq` gaps and
  equal `createdAt` values are valid: the cursor never uses the timestamp. Default `limit` is **50**, minimum 1, maximum 100.
* Response: `{"items":[{"id","walletId","transactionId","direction":"DEBIT|CREDIT","money","balanceBefore","balanceAfter","createdAt"}],"nextCursor":"..."}`.
  Monetary values are `{"amount":"25.00","currency":"BRL"}` (a string, never a number). `nextCursor` only exists when there is a next page (the server reads `limit+1`
  to decide, without losing or duplicating the extra item). An existing wallet without entries → `{"items":[]}`; a nonexistent one → 404 `WALLET_NOT_FOUND`.
* **Opaque, versioned cursor**: base64url of `{"v":1,"w":<wallet>,"s":<seq>}`. The client treats it as opaque; it **authorizes nothing** (authorization and scope
  stay on the server): it only delimits the continuation. It is validated for canonical form, size (≤ 200), version, position (≥ 1) and match with the route's wallet;
  any failure is `400 INVALID_CURSOR` (never a widened query). A `limit` that is not an integer, is outside 1..100 or is repeated → `400 INVALID_LIMIT`; an empty or repeated `cursor` → `400 INVALID_CURSOR`. The query string is parsed explicitly:
  any parsing error (invalid `%` escape, `;` separator, including in unknown parameters) rejects the whole request with `400 INVALID_QUERY`, with no data and without querying the ledger — a malformed
  pair is never silently dropped (which would make an invalid parameter become the default or a repetition look unique). Authentication and authorization come first (no token → 401, provider → 403).
* **Read view**: each page has its own view (one query, no transaction held between requests). Entries committed afterwards may appear in the
  following pages; **there is no frozen snapshot of the whole navigation**. An item that is already navigable is neither lost nor duplicated because every entry is written under the wallet's
  lock, released only at commit: within a wallet, a lower position is never committed after a higher one.

### Reconciliation

`POST /wallets/{walletId}/reconciliation` — `internal-service` only. It reads the stored balance and rebuilds the ledger (including the opening entry) in the **same view**: a PostgreSQL
`READ ONLY, REPEATABLE READ` transaction, with a 10 s deadline, no wallet lock (independent movements do not wait) and no repair. The financial `UnitOfWork` remains `READ COMMITTED`.

```json
{"walletId":"…","storedBalance":{"amount":"75.00","currency":"BRL"},"calculatedBalance":{"amount":"75.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":2}
```

`calculatedBalance` = credits − debits; `difference` = stored − calculated (may be negative); `checkedEntries` = entries considered. A zero wallet without entries → 0.00/0.00/0.00,
`consistent:true`, `checkedEntries:0`. `LOSS` and rejections produce no entry; reversals add their normal movements.

* **Exact arithmetic**: the sum is computed by PostgreSQL in `NUMERIC` (credits − debits, without the `int64` limit: separate sums may exceed `MaxInt64` with a valid final balance) and converted to
  `Money` with a range check; nothing goes through floating point. If the calculated balance or the difference does not fit in `Money`, or there is an entry in another currency, the response is
  `500 RECONCILIATION_FAILED` (log with the wallet) — **never** `consistent:true` and never an invented divergence. A read failure is unavailability (`503`), not a result.
* **Divergence** (`consistent:false`): a response with the values, an `ERROR`-level log (`reconciliation divergence detected`, with wallet, values, `checkedEntries` and version) and an
  integration point for the metric (`app.WithReconciliationObserver`), which feeds `wager_reconciliation_divergences_total` (see [Metrics](#metrics)); balance, ledger, version, transactions and events are **not** changed.

| Situation                                             | HTTP | `code`                                   |
| ----------------------------------------------------- | ---- | ---------------------------------------- |
| Token missing / invalid / expired / wrong audience    | 401  | `UNAUTHENTICATED` / `INVALID_TOKEN`      |
| No permission (role, `providerId` different from the token's) | 403  | `FORBIDDEN`                      |
| Invalid JSON, a number in place of a monetary string  | 400  | `INVALID_JSON`                           |
| Invalid amount/currency/kind/field                    | 400  | `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_KIND`, `INVALID_REQUEST`, `AMOUNT_NOT_ALLOWED_FOR_KIND`, `IDEMPOTENCY_KEY_REQUIRED` |
| Wallet / transaction not found                        | 404  | `WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND` |
| Same key with different content (amount, wallet, **currency or kind**, including a currency that is not enabled or a kind that is not processed) | 409  | `IDEMPOTENCY_KEY_REUSED` |
| Same `(providerId, externalTransactionId)` with another key | 409 | `EXTERNAL_TRANSACTION_ALREADY_REGISTERED` |
| A wallet already exists for the player+currency       | 409  | `WALLET_ALREADY_EXISTS` (+ `walletId`)   |
| Wallet does not belong to the player/currency; currency not enabled (new operation); `OPENING` over HTTP | 422 | `WALLET_MISMATCH`, `CURRENCY_NOT_SUPPORTED`, `KIND_NOT_ALLOWED` |
| Business rejection                                    | 422  | body `{"status":"REJECTED","failureCode":"<code>","balance":...}` — codes below |
| Server shutting down                                  | 503  | `SHUTTING_DOWN` (+ `Retry-After`) — repeat with the same key |
| Reference still unavailable (missing, or existing and pending) in `REFUND`, `ROLLBACK` or `WIN` with `referenceExternalTransactionId` | 202 | body `{"status":"PENDING_REFERENCE",...}` **without a balance**; the operation is persisted with its schedule and resumed by the worker; a replay returns the same 202; query the transaction (`GET`) for the result |
| Reference to the operation itself (`referenceExternalTransactionId` = `externalTransactionId`) | 400 | `INVALID_REQUEST` |
| `WIN` credit that would exceed the `Money` limit      | 400  | `INVALID_AMOUNT` — nothing is persisted  |
| Accepted without a result (`PENDING_REFERENCE`, see above) | 202 | body with `status` and **without** a balance; a replay returns the same 202 |
| Transient unavailability                              | 503  | `TEMPORARILY_UNAVAILABLE` (+ `Retry-After`) |
| Unknown commit outcome                                | 503  | `COMMIT_OUTCOME_UNKNOWN` — repeat with the **same** key |

### Failure codes (`failureCode`) of rejections

Every rejection is **final** and auditable (`WagerTransactionRejected`, no ledger entry, with the observed balance); the client fixes it by sending
**another** operation (another key and another external id). A replay returns the original rejection without re-evaluating it. Correctable inputs (400/404/422 on
input) are not stored.

| `failureCode` | When |
| --- | --- |
| `INSUFFICIENT_FUNDS` | a `BET` whose debit would leave the balance negative |
| `REVERSAL_INSUFFICIENT_FUNDS` | the debit of a `ROLLBACK` (of a `WIN` or `REFUND`) would exceed the balance — distinct from `INSUFFICIENT_FUNDS` |
| `ALREADY_REVERSED` | the target has already received a successful reversal (`REFUND` or `ROLLBACK`): **one per movement, across the whole history** |
| `REFERENCE_NOT_FOUND` | reference missing after the TTL |
| `REFERENCE_PENDING_TIMEOUT` | reference exists but is still pending after the TTL |
| `REFERENCE_NOT_PROCESSED` | reference is `REJECTED` or `FAILED` |
| `REFERENCE_MISMATCH` | scope (provider, player, wallet, currency or round), kind or amount incompatible with the reference |

Reversal policy: `REFUND` fully credits a `BET`; `ROLLBACK` undoes a `BET` (credit), `WIN` or `REFUND` (debit); the reversal amount equals the target's; a `WIN` may
reference a `BET` of the same round (free amount) and does not consume the reversal. `BET → REFUND →
ROLLBACK of the REFUND` restores the debit, and the BET **does not admit another successful reversal**. Rejections and pending operations do not consume the right.
Details and justifications: `ARCHITECTURE.en.md` §8.

## Environment variables

See [`.env.example`](.env.example) (local values, no secrets). The configuration is validated at
startup; all problems are reported at once.

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | — (required) | PostgreSQL URL with the **runtime** role (`wager_app`: owns no tables, is not a superuser, has no UPDATE/DELETE on the ledger) |
| `MIGRATE_DATABASE_URL` (`cmd/migrate` only) | `DATABASE_URL` | URL with the **schema owner** role (`wager`) |
| `RUNTIME_DB_ROLE`, `RUNTIME_DB_PASSWORD` (`cmd/migrate` only) | — | if set, `migrate up` creates/updates the runtime role and its minimal privileges |
| `DB_MAX_CONNS` | `10` | pool connections (1–500) |
| `OIDC_ISSUER` | — (required) | expected value of the `iss` claim |
| `OIDC_JWKS_URL` | — (required) | where to fetch the public keys (may differ from the issuer on the Compose network) |
| `OIDC_AUDIENCE` | — (required) | required audience (`wager-api`) |
| `OIDC_CLOCK_SKEW` | `5s` | clock tolerance when validating the token |
| `AWS_REGION`, `AWS_ENDPOINT_URL` | `us-east-1`, empty | SQS (MiniStack locally); credentials through the SDK's default chain |
| `AWS_SHARED_CREDENTIALS_FILE`, `AWS_PROFILE` | — | the API **selects** the broker's **runtime** principal: profile `wager-runtime` of the credentials file written by `broker-provision` (in Compose: `/run/broker/credentials`). The mounted file contains the **five** profiles, all readable by the process; separating them per process is a technical hardening recommendation, not something this local environment isolates. Do not set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (they take precedence over the profile) |
| `SQS_WAGER_QUEUE_NAME` | `wager-transactions.fifo` | input queue (checked at readiness and consumed by the consumer) |
| `ALLOWED_CURRENCIES` | `BRL` | accepted currencies (3 uppercase letters) |
| `HTTP_ADDR` | `:8080` | server address |
| `REQUEST_TIMEOUT` | `10s` | per-request deadline |
| `SHUTDOWN_TIMEOUT` | `20s` | **global** shutdown deadline (admission, drain, cancellation, cleanup and closing of dependencies) |
| `SHUTDOWN_DRAIN_TIMEOUT` | `12s` | part of the global deadline dedicated to draining; the rest (default 8 s) is the shared reserve for cancellation, cleanup and dependencies. Must be shorter than `SHUTDOWN_TIMEOUT` |
| `REFERENCE_TTL` | `15m` | deadline of a pending reference, counted from the first persistence (not renewed) |
| `REFERENCE_BACKOFF_INITIAL`, `REFERENCE_BACKOFF_MAX` | `1s`, `30s` | exponential backoff with jitter (always within the ceiling) between checks |
| `REFERENCE_POLL_INTERVAL` | `1s` | worker pause between scans |
| `REFERENCE_PHASE_TIMEOUT` | `10s` | limit of an attempt's financial phase (locks and commit) |
| `REFERENCE_WORKERS` | `1` | reference executors per process (`0` disables) |
| `SQS_CONSUMERS` | `4` | SQS consumer executors per process (0 to 16; `0` disables SQS input in this process) |
| `CONSUMER_NAME` | `wager-consumer` | inbox `consumerName`: **stable** across instances and restarts (never a hostname or PID) |
| `SQS_DLQ_NAME` | `wager-transactions-dlq.fifo` | DLQ to which permanent messages are forwarded (and whose approximate depth the metric reads) |
| `CONSUMER_RECEIVE_WAIT` | `10s` | long polling (cancelable at shutdown admission) |
| `CONSUMER_PHASE_TIMEOUT` | `10s` | an attempt's financial phase (locks and commit) |
| `CONSUMER_IO_TIMEOUT` | `5s` | each transport action (Delete, visibility, DLQ) and each diagnostic write/lookup |
| `CONSUMER_BACKOFF_INITIAL`, `CONSUMER_BACKOFF_MAX` | `5s`, `60s` | redelivery backoff: exponential on the `ApproximateReceiveCount`, with jitter in [d/2, d] and a ceiling |
| `CONSUMER_PAUSE_MAX` | `30s` | ceiling of the search pause after consecutive failures (global failure) |
| `SQS_CREDENTIALS_FILE`, `SQS_AWS_PROFILE` | empty, `wager-runtime` | optional: file and profile of the SQS client (default: the SDK chain, runtime profile) |
| `OUTBOX_PUBLISHERS` | `1` | outbox publisher executors per process (0 to 8; `0` disables — no destination or credentials required) |
| `SNS_EVENT_TOPIC_ARN` | — (required with a publisher) | ARN of the output FIFO topic (`arn:aws:sns:us-east-1:000000000000:wager-events.fifo` on MiniStack) |
| `OUTBOX_CREDENTIALS_FILE`, `OUTBOX_AWS_PROFILE` | `AWS_SHARED_CREDENTIALS_FILE`, `wager-publisher` | file and **profile of the publisher principal**, read by the SNS client constructor (the readiness SQS client keeps using `wager-runtime`; no process-global variable is changed) |
| `OUTBOX_LEASE` | `20s` | validity of a reservation (no renewal) |
| `OUTBOX_PUBLISH_TIMEOUT` | `5s` | total deadline of one publication (includes the SDK's internal attempts) |
| `OUTBOX_SQL_TIMEOUT` | `2s` | deadline of each short reserve/confirm/retry/release transaction |
| `OUTBOX_POLL_INTERVAL` | `500ms` | wait when there is no eligible event |
| `OUTBOX_BACKOFF_INITIAL`, `OUTBOX_BACKOFF_MAX` | `1s`, `30s` | exponential retry with uniform jitter in [d/2, d] and a ceiling |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Broker (MiniStack with AUTH=true)

The local broker is **MiniStack 1.5.27** (pinned image) with `AUTH=true`, which evaluates the IAM policies of the principal identified by the access key
(the challenge statement §4 allows LocalStack or MiniStack, and §2 requires messaging access controlled by broker credentials and policies).

> **Emulator limit:** MiniStack **does not validate SigV4 signatures** — the access key only identifies the principal, and the secret is not checked.
> `AUTH=true` demonstrates **emulated policy-based authorization**, not full AWS authentication. The project's real authentication is Keycloak's on the
> HTTP API; on SQS, binding an origin to a `providerId` is the responsibility of the trusted internal producer (external providers
> do **not** get a direct `SendMessage`). The broker is exposed on `127.0.0.1` only, for development. On AWS in production, check: SigV4 signatures
> and real secrets, IAM roles (not users with static keys), resource policies of the queues and the topic, encryption (KMS) and private
> endpoints — none of this is proven here.

**Provisioning** (`cmd/broker-provision`, Go SDK v2, a Compose one-shot service; no `awslocal` and no emulator hooks): idempotently creates or converges
the resources below, the identities and the policies, and writes the per-responsibility credentials to
`.local/broker/credentials` (ignored by git; **synthetic local** keys). Re-running does not duplicate resources, does not duplicate the subscription and
**reuses the keys that are still valid** (credentials in use do not break); a key that the emulator has lost is reissued (restart the API so it
re-reads the file).

| Resource | Configuration |
| --- | --- |
| `wager-transactions.fifo` | FIFO, `ContentBasedDeduplication=false`, `VisibilityTimeout=60`, redrive: `maxReceiveCount=5` → DLQ |
| `wager-transactions-dlq.fifo` | FIFO, 14-day retention |
| `wager-events.fifo` (SNS FIFO) | destination of the output events, fed by the outbox publisher ([Outbox publisher](#outbox-publisher)) |
| `wager-events-audit.fifo` (SQS FIFO) | topic subscriber (`RawMessageDelivery=true`) for auditing/tests; policy that only admits the SNS service **from the events topic** (`aws:SourceArn`) |

| Principal (IAM user = profile) | Permissions (per action and resource, no wildcards) |
| --- | --- |
| `wager-producer` (internal producer) | `SendMessage`, `GetQueueUrl` on the input queue. No consume, delete, DLQ or SNS |
| `wager-runtime` (consumer; **selected by the API**) | on the input queue: `GetQueueUrl`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`; on the DLQ: `GetQueueUrl`, `SendMessage` and `GetQueueAttributes` (only the approximate depth, for the metric; nothing on the input queue or on the audit queue) |
| `wager-publisher` (outbox publisher; its own SNS client) | `sns:Publish` and `sns:GetTopicAttributes` on the events topic. Does not consume the input queue |
| `wager-inspector` (tests/diagnostics) | observes and prepares scenarios on the DLQ and on the audit queue; input queue attributes; topic subscriptions |
| `wager-nobody` | no policy (proves `AccessDenied`) |

The **provisioning** principal is the emulator's root (access key `test`, or a 12-digit account id), used only by `broker-provision`
and by the proof; never by the application. **State:** MiniStack runs without `PERSIST_STATE`: queues, messages, topic and identities are **ephemeral** and
disappear when the `ministack` container is recreated (`docker compose down`/`up` recreates it; a `restart` also restarts the process). After that, `docker compose up -d`
re-runs `broker-provision`; restart `api` so it re-reads the credentials. No durability is promised (not even after `SIGKILL`).

```sh
mkdir -p .local/broker                           # owner: you (provisioning hands the file to the directory owner)
docker compose up -d --wait postgres keycloak ministack
docker compose run --rm broker-provision        # idempotent (make broker-provision)
scripts/check-infra.sh                            # resources, redrive, delivery policy, principal policies, credentials file
docker compose exec ministack awslocal sqs list-queues    # inspection with the emulator root
```

**Authorization proof** (`test/integration/broker_test.go`, run in CI): with the **Go SDK v2 and each principal's real credentials**, on resources
isolated by prefix (`proof-<id>-`, created and removed by the test; they never touch the development queues) — the producer sends, the runtime receives/changes
visibility/deletes, the publisher publishes and the subscriber receives the **raw** content with traceable group and deduplication; 20 typed negatives
(`AccessDenied`/403, not an endpoint error) covering each principal and **the same action on another resource**, with proven absence of effect
(in-flight message intact, empty queues); FIFO (transport deduplication — a temporary broker window, **not** the application's financial guarantee — and distinct
groups in parallel); visibility and redrive with content and group preserved in the DLQ (here with 1 s and 2 receives, only in this isolated test; the defaults 60 s and 5
are verified in another test and in `check-infra`); SNS→SQS **actually delivered**, and **not** delivered when the topic is not the authorized one or when the queue policy is removed
(an accepted `Publish` does not prove delivery); repeated provisioning and reissue of a lost key; API startup with a principal without permission fails with `AccessDenied`.
Emulator note: redrive is applied when the consumer tries to receive again (AWS does it in the background), and absences of delivery are bounded observations (3 s long polling).

## Migrations

Versioned in `internal/infra/postgres/migrations` (goose, embedded in the binary), with a PostgreSQL session lock
against concurrent runs. They run with the **schema owner** role; the API runs with the runtime role.

```sh
export MIGRATE_DATABASE_URL='postgres://wager:wager@localhost:5433/wager?sslmode=disable'
export RUNTIME_DB_ROLE=wager_app RUNTIME_DB_PASSWORD=wager_app_dev_password   # optional, see below
go run ./cmd/migrate up                # applies the pending ones and, if RUNTIME_DB_ROLE is set, creates/updates the runtime role
go run ./cmd/migrate status
go run ./cmd/migrate grants            # only (re)applies the runtime role's minimal privileges (coordinated with the migrator's other operations: waits for one in progress)
go run ./cmd/migrate down              # reverts the last one
go run ./cmd/migrate down-to 0         # destructive RESET: reverts all of them (deletes the data, including the preserved copy of the schedule)
# in Compose: docker compose run --rm migrate up | down | status | grants
```

The API **refuses to start** if the schema is not at the version the binary expects.

### Already initialized databases (migrations 00002 to 00008)

The already published migrations (00001 to 00008) **are not rewritten**; corrections are new migrations. For a database created
by earlier versions, **do not delete anything**: run `migrate up` (in Compose: `docker compose up -d --build migrate api`).

* `00008_inbox_and_message_diagnostics.sql` — **final version expected by the application (8)**; `inbox_messages` (identity `(consumer_name, message_id)`, `body_hash` = SHA-256 of the exact bytes, a row that is **already completed**, `transaction_id` NOT NULL with an FK) and `message_diagnostics` (permanent refusals, with no transaction and no financial body); both append-only (a trigger rejects UPDATE/DELETE/TRUNCATE even for the owner; the runtime only has SELECT and INSERT). The rollback drops both tables (financial idempotency lives in `wager_transactions` and is not affected).
* `00007_outbox_publisher_lease.sql` — previous version; the outbox publisher's reservation: `lease_token` and `lease_expires_at` (both NULL or both set; never on a published event) and the `published_at` marker, once written, becomes immutable. Existing events, snapshots, schedule, attempts, diagnostics and markers are left intact (the new columns arrive NULL and the backlog is published normally). Rollback (`migrate down`, one step): drops only the reservation (transient) and restores the previous guard; nothing else changes.
* `00006_pending_shape_and_schedule_preservation.sql` — fixes two audit findings without rewriting 00005. (1) **Shape of `PENDING_REFERENCE` enforced in the schema**: the external reference, when present, must have a shape that rehydration accepts (non-empty, ≤ 128 characters, no control characters or leading/trailing spaces, different from the operation's own external id) and `PENDING_REFERENCE` only exists for external origin, kind `WIN`/`REFUND`/`ROLLBACK` and with an external reference; incompatible history is **refused** (count and first ids, under writer exclusion), never corrected. (2) **Restores the schedule preserved** by an earlier rollback (see below). Rollback: removes the guarantees; does not touch data.
* `00005_references_and_reversals.sql` — installs in the database the guarantees of reversals and
  pending operations (internal reference consistent with the external one and with the scope, `PROCESSED` target of an eligible kind, full amount, direction of the
  `ROLLBACK`, resolved reference required to complete, **one reversal per target**, restricted mutability of non-terminal transactions, and the
  `pending_references` schedule: `PENDING` is never committed and `PENDING_REFERENCE` always has an open schedule). It takes the same `LOCK TABLE` as 00004 and, with the
  writers excluded, **refuses** (count and first ids, nothing changed) a transaction committed as `PENDING`, a reversal or `WIN` with a
  completed reference without the internal reference, an inconsistent reference, a target with more than one reversal or a `ROLLBACK` with an entry in the wrong
  direction. Its published `Down` removes the guarantees and `DROP`s the schedule; for that reason the migrator **preserves it first** (see "Rollback and the pending-operations schedule"). The runtime role's privileges gain `UPDATE` only on the transition
  columns (`migrate up` or `migrate grants`).
* `00004_revalidate_history_under_writer_exclusion.sql` — window between validating and installing closed. It locks the financial tables
  (`LOCK TABLE ... SHARE ROW EXCLUSIVE`: waits for open writers, holds back new ones, does not block reads), re-reads the **whole**
  history and checks that the guarantee triggers exist and are enabled. It closes the window between validating and installing in the
  earlier migrations and refuses databases where it has already happened. It only validates: it installs nothing and changes no data.
* `00003_completed_movement_requires_ledger.sql` — every `PROCESSED` transaction with a movement (`OPENING`, `BET`, `WIN`,
  `REFUND`, `ROLLBACK`) needs, at COMMIT, the entry of its own wallet, and `result_balance` must be that entry's
  `balance_after` (`LOSS` and rejections have no entry).
* `00002_ledger_chain_and_result_integrity.sql` — ledger protection (continuous balance chain, entry→transaction→wallet
  link) and a non-null result on `PROCESSED`/`REJECTED`.

Each one starts with a **history check**: if there are incompatible records (broken chain, null result,
entry without a consistent transaction; in 00003, a completed transaction without an entry or with a divergent result, with count and
first ids) the migration **aborts without changing data or the schema** — fixing historical records is manual, then
run `migrate up` again. Rollback: `migrate down` (one step at a time) removes the last migration's guarantees (00004 does not
install anything: reverting only moves the version back) and **does not change data**.

**Upgrade procedure** (details and rationale in `ARCHITECTURE.en.md` §10.3):

```sh
docker compose stop api                 # recommended: stop all writers (all API instances)
docker compose run --rm migrate up      # schema owner role; waits for open writers and validates under exclusion
docker compose run --rm migrate status  # the final version (00008) must be listed as applied
docker compose up -d --build api        # the API only starts at the schema's final version
```

* The migrator's session lock only prevents **two migrations** at the same time; what protects against writers is the
  `LOCK TABLE` exclusion of 00004. Even with the API running the result is correct for runtime-role writers; stopping the writers
  reduces the hold time and is the precondition for any other writer (manual maintenance, ETL, another role).
* If 00004 or 00005 aborts (incompatible history, missing/disabled trigger or a writer open for more than 30 s), the schema stays at the
  previous version (e.g. 4), nothing is deleted and **the API refuses to start** (`database schema is at version 4, application expects 8`). Fix the
  indicated records manually (count and first ids in the message) or repeat without open writers, and run `migrate up` again.
* Limits: a writer with owner/superuser privilege can bypass triggers (only the validation sees it, and only at the moment of the
  migration); online upgrade with no hold at all is not a goal.

Runtime role on old databases: the first `migrate up` of this version (with `RUNTIME_DB_ROLE`) creates `wager_app`; afterwards
switch the API's `DATABASE_URL` to it (Compose already does this).

### Rollback and the pending-operations schedule

The schedule (`pending_references`: start, expiry, next attempt, attempts, diagnostic and the completed schedules) is **durable data**,
not disposable state: reverting and reapplying must not delete it or renew an expired deadline. Since 00005 has already been published, its `Down`
(which removes the table) and its `Up` (which rebuilds rows with a new deadline) **were not rewritten**; the compatible solution is a
migrator procedure (`Migrator`, used by `cmd/migrate`) that **preserves or refuses before any loss**:

0. **Coordination.** Every migrator operation (`up`, `down`, `down-to` and `grants`) runs under its own advisory lock (`MigratorLockID`, distinct from goose's lock, which
   the provider acquires internally, always afterwards: a single order, no deadlock), acquired by attempts — a command that is cancelled (Ctrl-C) or killed leaves no session in the queue. Reading the version, the
   preparation, the rollback, the restoration and the granting of privileges do not interleave with another migrator's: `migrate grants` **waits** for a `down` in progress (it never reopens
   writes in the middle of it) and `migrate up` with `RUNTIME_DB_ROLE` performs the `Up` and the provisioning in the same operation.
1. **Operational rollback** — `migrate down` (one step) and `migrate down-to N` with `N > 0`: before reverting 00005, the migrator runs the **preparation**
   under writer exclusion (`LOCK TABLE … SHARE ROW EXCLUSIVE`, the same order as the migrations), in a single transaction:
   * it captures and **revokes** the privileges on the schedule of every role that is not the owner — table **and column** privileges, including those granted to `PUBLIC`
     (`pg_class.relacl` and `pg_attribute.attacl`) — and **verifies the effect in the catalog**: if any login role (that is not a superuser, the owner or the
     migrator's session) can still write, directly, by inheritance or by belonging to a role with write access (e.g. `pg_write_all_data`), the rollback is **refused**,
     with the names, changing nothing;
   * it keeps a **full copy** in `pending_references_preserved` (accessible only to the owner). The **original copy is never replaced by a rebuild**:
     if a copy already exists and the live schedule is the same table that was copied (an earlier `down` failed or was cancelled), the copy is refreshed from the live schedule;
     if the live schedule is the rebuild created by 00005's `Up` (partial reapplication), the original copy is kept and the rollback only proceeds if the live schedule is
     the **intact** rebuild (no row outside the copy, no attempts, diagnostic or completion) — otherwise it is **refused**, with count and ids, changing nothing.
   Reverting 00006 and later migrations does not touch data.
2. **Reapplication** — `migrate up`: before 00006 (and with write access of the non-owner roles **revoked from the decision until the restoration finishes**, privileges given back at the end: a runtime resume in that window is
   denied loudly, never committed only to be overwritten), if the live schedule is the **same table** that was copied (a `down` was cancelled before dropping it), the stale copy is discarded so as not to
   overwrite the work the runtime committed afterwards; if the live schedule is a modified rebuild, the `up` is **refused** before any loss. In the normal case, 00005's `Up`
   rebuilds, with a renewed deadline, one row for every `PENDING_REFERENCE` (published behavior) and **00006 restores the preserved copy over them** — original values, including the completed schedules — and removes it. A copy that
   matches no transaction (e.g. after a reset) **blocks the restoration** with count and ids, without silently discarding it; if the restoration fails, the copy remains.
3. **Deliberately destructive reset** — `migrate down-to 0` (and the test suite): deletes all tables and data, as always, **including** the preserved copy.
4. **Failure or cancellation after the preparation**: the original schedule remains intact and the migrator itself **gives back** the captured privileges (if the schedule
   still exists). If the process dies before that, `migrate grants` (or a `migrate up` with `RUNTIME_DB_ROLE`) gives them back, after the earlier operation has finished.

Limits: the protection is in the migrator; running 00005's `Down` directly through goose, bypassing it, is not protected. Writer exclusion is by
privilege: the **owner** role and superusers are not excluded (they are the operator), and a `SECURITY DEFINER` view or function of another owner that writes to the schedule is not detected. Stop the
API instances before a rollback (as in the upgrade) — the revocation prevents them from writing, but they will fail loudly until the restoration. Pending operations that never had a schedule
(created by SQL in a version earlier than 00005) receive, in 00005's `Up`, the published rebuilt schedule — there is no original data to restore.

## Outbox publisher

Events enter `outbox_events` **in the same transaction** as the financial operation; the publisher only sees what was committed (including the
backlog of earlier versions) and publishes it to the SNS FIFO topic `wager-events.fifo`. Each executor repeats, **with no SQL connection or transaction open
during the network call**:

1. **Reserve** an eligible event — unpublished, `next_attempt_at` due and without a valid reservation — in a short transaction
   (`FOR UPDATE SKIP LOCKED`, oldest first, re-evaluated under the lock): it writes `lease_token` (a new uuid **on every acquisition**; it is neither the
   `eventId` nor the process identity) and `lease_expires_at` (**the database clock**, never the publisher's) and counts an attempt. Events in
   backoff, under a valid lease or locked by another transaction do not enter the selection: no busy prefix blocks the following events, and there is no global lock.
2. **Publish** the envelope **as it was stored** (the text of the stored `jsonb`: PostgreSQL normalizes key order and whitespace, but data, money and `walletVersion` undergo no conversion; nothing is regenerated and no balance is re-read) with `MessageGroupId = walletId` (from `data`; the
   `aggregateId` of transaction events is the transaction) and `MessageDeduplicationId = eventId`, with a total deadline of `OUTBOX_PUBLISH_TIMEOUT`.
3. **Confirm** (`published_at`) — or **record the retry** (`next_attempt_at` with exponential backoff and jitter, `last_error`) or **release** the
   reservation (shutdown) — in a new short transaction. All three require `eventId`, the **current acquisition's token**, an unpublished event and a **lease
   still valid at the instant of the write**. The row is locked first (this is where a lock wait happens) and only then does the `UPDATE` test ownership with
   `clock_timestamp()`: the instant captured before the wait never authorizes the write. Zero rows affected = ownership lost (`ErrLeaseLost`);
   an old executor, or one with an expired lease (with or without a new owner, and including a new acquisition by the same process), changes neither state,
   schedule nor the current owner's reservation. **The token protects the SQL state; it does not prevent an old, already sent publication from reaching the broker.**

**Guarantees and limits.** Delivery is *at-least-once*: an uncertain `Publish` result, a lost confirmation, an expired lease or a dead process
(`SIGKILL` included: recovery does not depend on hooks) lead to **republishing the same event** (same `eventId`, same snapshot; no new
financial effect). The broker's FIFO deduplication is temporary: **consumers deduplicate by `eventId`**. The **original outbox order is not guaranteed**
across publishers, retries and recovery; SNS FIFO only orders the *accepted* publications within the group. Balance projections apply `balanceAfter` only
with a greater `walletVersion`, atomically; the envelope's `version` is the contract version. There is no lease renewal: a publication that takes longer than the
lease may be republished by another executor. `attempts` counts **reservations** (started attempts, including abandoned ones) and feeds the backoff;
`last_error` keeps the last failure diagnostic (short text, with no payload or credentials) and remains after success. Events are **never**
discarded because a counter is exhausted: a persistent failure stays pending, diagnosable and recoverable, and does not prevent the others from advancing.

**Configuration** (variables table above): lease 20 s, publication 5 s, SQL 2 s per transaction, backoff 1 s to 30 s, one executor per process.
Validation rejects a lease that does not exceed publication + 2×SQL by at least 5 s, a publication that does not fit in the drain and an SQL timeout that does not fit in the
shutdown reserve (also for directly built configurations). **Privileges:** the runtime only updates the control columns
(`attempts, next_attempt_at, last_error, published_at, lease_token, lease_expires_at`); `payload`, identity, `occurred_at` and `created_at` are not
updatable, and `DELETE`/`TRUNCATE` do not exist. Startup checks the topic (it exists, is FIFO, the principal can describe it): without permission, the API
fails loudly with `AccessDenied`. **Shutdown:** the publisher is a coordinator component — admission closes first (no new reservation), the
publication/confirmation in progress is drained, at the drain limit the publication is cancelled and the **reservation is released** (never confirmed), and `Done`
only closes after the final SQL action, which uses its own context and is limited **by `OUTBOX_SQL_TIMEOUT` and by the remaining global deadline** (it never enlarges the former or exceeds the latter; the repository's rollback is also bounded). With no remaining budget, no new SQL is started, not even with an artificial deadline: the lease expires and the event is recovered (an already accepted send is republished with the same `eventId`).

## SQS consumer and inbox

The `wager-transactions.fifo` queue belongs to **trusted internal producers**; external providers do not get a direct `SendMessage`. The consumer
(a lifecycle coordinator component, principal **wager-runtime**) receives **one** message per search in each free executor and handles it with the
**same financial core as HTTP** (`Service.processExternal`), in a single transaction that also writes the inbox.

**Envelope** (`WagerTransactionRequested`): `messageId`, `type`, `occurredAt` (RFC 3339) and `data` with `providerId`, `externalTransactionId`,
`idempotencyKey`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money {amount, currency}` and an optional `referenceExternalTransactionId`.
Explicit validation: a single JSON object, no unknown fields, exact types (monetary values are strings; nothing goes through `float`) and `OPENING`
rejected. The financial key is `data.idempotencyKey` (never derived from `messageId`). **`MessageGroupId = walletId`; `MessageDeduplicationId = messageId`.**

**Inbox.** Identity `(consumerName, envelope messageId)` with a UNIQUE constraint in the database; `body_hash` = SHA-256 of the **exact bytes** received (distinct from the canonical
financial hash — the message identity/hash covers the exact body bytes, while the financial identity/hash is the canonical hash of the business fields). One SQL attempt: look up the inbox → `processExternal` → write the inbox (already completed) → commit; balance, transaction, ledger, pending-operation
schedule, outbox and inbox share the commit, and any failure rolls everything back. The same `messageId` and the same hash already completed = **duplicate** (nothing is
reapplied); the same `messageId` with a **different** body = **permanent conflict** (the original inbox row is not touched); a new envelope for an already registered operation =
**financial replay** (a new inbox row, original result). Uniqueness constraints reveal races: the attempt is rolled back and a new one sees the committed row.

| Result | Durable commit and transport action |
| --- | --- |
| PROCESSED, financial replay, duplicate | inbox and effects committed → `DeleteMessage` (receipt handle of the current delivery) |
| REJECTED by a business rule | rejection, event and inbox committed → `DeleteMessage`; no ledger entry |
| PENDING_REFERENCE | pending operation, schedule, event and inbox committed → `DeleteMessage`; the reference worker takes over |
| Transient, contention, uncertain commit, unexpected error | **nothing is deleted**; `ChangeMessageVisibility` with backoff (5 s to 60 s, jitter, from the `ApproximateReceiveCount`, rounded up) |
| Invalid envelope, `OPENING`, nonexistent/mismatched wallet, currency or kind not accepted, identity or idempotency conflict | **confirmed diagnostic → confirmed DLQ send → `DeleteMessage`** |

The diagnostic (`message_diagnostics`: consumer, queue, broker id, hash, stable code) **is not a financial operation**: there is no fictitious transaction or ledger entry. An
existing diagnostic **does not exempt** the DLQ send: after a crash between the diagnostic and the send, the redelivery resumes it (the diagnostic is looked up before
handling; a lookup error is a retry, never "absent"). The original body goes to the DLQ; `MessageGroupId` = `walletId` (or `unparsed` if the body is unreadable) and
`MessageDeduplicationId` = SHA-256 of queue + broker id + hash. A database failure is not a confirmed diagnostic; a send or delete with an uncertain result keeps the source
recoverable (the DLQ may receive the duplicate, accepted so that nothing is lost). An unknown error is always recoverable: **it never becomes a permanent rejection**.

**Limits and parameters.** Visibility timeout 60 s and redrive after 5 receives (`maxReceiveCount` counts receives, not commits: a PROCESSED operation whose deletion fails
repeatedly may reach the DLQ without the financial result changing — the message's destination is not the state of the money). 4 executors per process, financial phase
10 s and transport actions 5 s (validation requires phase + 3×action to be shorter than the visibility). No visibility renewal. Consecutive failures pause the searches (exponential up to
30 s), with no SQL transaction during any network wait. A FIFO group in retry delays only itself; PostgreSQL's locks remain authoritative.

**Shutdown.** Admission cancels the long polling and prevents dispatching new work; the admitted handling proceeds up to the drain limit (a message received by a search
concurrent with the closing is returned with visibility 0, never deleted). At the limit, the handling is cancelled and the message released for redelivery. The final actions on the broker use
the earliest **absolute instant** between their own limit and the global deadline, and with no budget nothing new starts (the visibility timeout returns the message). `Done` covers searches, SQL/rollback and
final actions.

**Internal producer** (`cmd/sqs-producer`, principal **wager-producer**): authenticates the origin (client credentials verified against the IdP by the existing verifier), binds it to the
envelope's `providerId` (**provider-a cannot publish as provider-b**; the internal service publishes for any provider; the body authenticates no one) and only then sends the exact body.
`docker compose run --rm -T -e PRODUCER_CLIENT_ID=provider-a -e PRODUCER_CLIENT_SECRET=... sqs-producer < envelope.json`. MiniStack evaluates policies but **does not validate SigV4**:
this is emulated authorization, not full AWS authentication.

## Metrics

`GET /metrics` (Prometheus-compatible text), **`internal-service` only** (401 without a token, 403 for a provider; `/health/*` stays public). **Process-local** counters
(they restart with the process), observed **after the commit**; they are **not financial accounting** (the ledger, inbox and outbox are the source of truth) and may lose an observation between the commit
and the emission. Labels come from bounded sets (never ids, hashes, free-form `providerId` or error text; the excess becomes `other`). Money never appears.

| Challenge category | Metric |
| --- | --- |
| Results by status | `wager_transactions_total{origin=http\|sqs\|references,kind,status,replay}` (financial replay distinguished) |
| Duplicates | `wager_inbox_total{outcome=new\|duplicate\|conflict}` (inbox duplicate ≠ financial replay) |
| Retries | `wager_retries_total{component=consumer\|outbox\|references,reason}` |
| DLQ | `wager_dlq_forwarded_total{code}` (CONFIRMED manual send) and `wager_dlq_messages_approx{state}` (the broker's approximate gauge, read on demand) |
| Concurrency conflicts | `wager_concurrency_conflicts_total{reason}` (uniqueness constraints resolved by a new attempt, busy wallet, lost lease) |
| Outbox delay | `wager_outbox_pending`, `wager_outbox_reserved`, `wager_outbox_oldest_pending_age_seconds` (on demand, from the durable state) and `wager_outbox_published_total` |
| Latency | `wager_processing_seconds_sum/_count{component=http_submit\|sqs_message\|outbox_publish}` (sum and count; no percentiles) |
| Reconciliation divergences | `wager_reconciliation_divergences_total` (REAL divergence only, via `WithReconciliationObserver`) and `wager_reconciliation_errors_total` |

The gauges are collected **on demand, with a 2 s deadline per source**; if the collection fails, the samples are **omitted** and `wager_collector_up{source="outbox|dlq"}` is 0 — never a fictitious zero.

## Running the application without Docker for the API

```sh
docker compose up -d --wait postgres keycloak ministack && docker compose run --rm broker-provision   # or: make infra
set -a; . ./.env.example; set +a   # AWS_PROFILE=wager-runtime + .local/broker/credentials
go run ./cmd/migrate up      # as the schema owner; creates the wager_app role
go run ./cmd/api             # as wager_app (DATABASE_URL)
```

## Tests

```sh
go test ./...            # unit tests (domain, config, shutdown coordinator, and others) — need no infrastructure
go test -race ./...
go vet ./...
```

### Integration (real PostgreSQL, Keycloak and MiniStack)

Preparation — `make infra` (or `docker compose up -d --wait postgres keycloak ministack && docker compose run --rm broker-provision`). The suite reads the credentials from `BROKER_CREDENTIALS_FILE` (default `.local/broker/credentials`), runs the application with the `wager-runtime` profile and **refuses** to run if the endpoint (`AWS_ENDPOINT_URL`) is not a MiniStack. The tests use the database in
`TEST_DATABASE_URL` (default `wager_test`; **any name**), applying `down-to 0` + `up` on every run (which also
exercises the migrations), and create the suite's own runtime role (`wager_test_app`; roles are server-wide, so it never reuses the application's `wager_app`). The application under test runs with that restricted role.

Safeguards: the suite **refuses** to reapply the schema on a database that is the application's (`DATABASE_URL`/`MIGRATE_DATABASE_URL`),
a system database, or a database with tables that is not marked as a test database (`COMMENT ON DATABASE`, done by the
suite itself when it creates the database or adopts an empty one). The auxiliary databases (unmigrated/outdated schema, corrective
migration) have a name derived from the test one plus a random suffix and are removed at the end. To adopt **one time only** a test database
created by earlier versions of the suite:
`psql ... -c "COMMENT ON DATABASE wager_test IS 'wager-integration-test-database'"`.

```sh
go test -tags integration -count=1 ./test/integration/...          # a few minutes
go test -tags integration -race -count=1 ./test/integration/...
go vet -tags integration ./...
go test -tags integration -count=1 ./test/integration -run 'TestThree|TestKilling' -v   # 3 processes
```

The child binaries of the multi-process tests (API) and of the migration CLI tests are compiled **with `-race`** by the harness (the parent's `-race` does not instrument separate binaries):
a race report, `panic` or `fatal error` in a child's output fails the test; the expected `SIGKILL`/`SIGTERM` of controlled scenarios prints none of that.

Optional variables (defaults match Compose): `TEST_DATABASE_URL`, `TEST_RUNTIME_DB_ROLE`,
`TEST_RUNTIME_DB_PASSWORD`, `KEYCLOAK_TOKEN_URL`, `OIDC_ISSUER`, `OIDC_JWKS_URL`, `AWS_ENDPOINT_URL`.

| Group | File | What it proves |
| --- | --- | --- |
| Use cases | `service_test.go` | wallet opening and BET directly on the service (no HTTP): replay, conflicts with identity precedence, rejection, 50 resends and 2×80.00 over 100.00 |
| JWT verifier | `auth_verifier_test.go` | identities derived from the Keycloak token; tampered, forged, `alg:none`, HS256, different issuer/audience, expired; `Start` fails with an unreachable IdP |
| Flow | `flow_test.go` | positive opening (OPENING, ledger, 2 events, version 1), zero, duplicate (409), strict money parsing over HTTP, accepted/rejected BET, unsupported kinds, input validation |
| WIN and LOSS | `win_loss_test.go` | `WIN` credits once (ledger `CREDIT`, `WagerTransactionProcessed` + `WalletBalanceChanged` with causation and `walletVersion`); overflow without partial effects; `LOSS` with no ledger entry, without changing balance/version and with only `WagerTransactionProcessed`; replays with the original balance; 50 parallel `WIN`/`LOSS` resends; `LOSS` waits for the wallet lock; `WIN` with a reference is processed like the reversals (200 or 202, see the Reversals and Pending references rows); **3 real processes** (resends and distinct credits without loss) |
| Idempotency | `idempotency_test.go` | a replay returns the original balance after other movements; a terminal rejection is not re-evaluated after a credit; payload conflict; the same external operation with another key; keys per provider; **a conflict takes precedence over a nonexistent wallet/mismatched player/currency**; contention for the same identity across different wallets |
| Identity precedence | `identity_precedence_test.go` | use case and HTTP: an already registered key or external id with a **currency that is not enabled or a different kind (WIN/LOSS/REFUND/ROLLBACK)** → 409; structurally invalid inputs remain 400/422; new operations remain 422/501; provider scope; 401/403 before everything; nothing is persisted |
| Concurrency | `concurrency_test.go` | 50 parallel resends → 1 debit; 2×80.00 over 100.00 (with resends) → 1 processed, 1 rejected, balance 20.00; no lost update; independent wallets do not block each other; several wallets in parallel |
| Completed movement | `completed_movement_test.go` | **through the restricted role**, with valid-hash payloads: `BET PROCESSED` without an entry or a wallet change, `OPENING` without an entry, `PENDING`→`PROCESSED` without an entry and a result different from the entry's `balance_after` (777.00 × 90.00; zero × 90.00) are refused at COMMIT; several valid operations in the same SQL transaction keep their own result and the replay (HTTP) returns the original balance; `LOSS` and rejections remain without an entry |
| Outbox publisher (store) | `outbox_store_test.go` | an event of an open/rolled-back transaction is never reserved; exclusive reservation and expiry with a **new token** (same instance); an old executor does not confirm/reschedule/release the new owner's state (not even with the lease expired and no new owner); a **confirmation that waited for a lock** is validated after the wait (ownership expired during it); persisted backoff, and prefixes in retry, under lease or locked do not hide the following events; 8 simultaneous reservers never share an event; runtime privileges (control columns only), immutability and the reservation's shape enforced by the database; upgrade from 6 to the final version, rollback to 6 and reapplication preserve events, schedule, attempts, diagnostics and markers |
| Outbox publisher (SNS) | `outbox_publisher_test.go`, `internal/outbox/outbox_test.go` | every event type (earlier backlog included) with the exact snapshot, `walletId` as the group (≠ `aggregateId`) and `eventId` as the deduplication, observed on the subscriber queue; without permission: `AccessDenied` at startup and a retry without `published_at`; two publishers not sharing events; an abandoned reservation recovered; **a send without confirmation → republication of the same `eventId`/snapshot with no financial effect**; failure/uncertain result of `Publish` and of the confirmation, the database's backoff preserved across instances; an event in persistent failure neither blocks nor disappears; shutdown (drain and cancellation with release; **a short but valid global budget, with the final action waiting on a real lock, finishes within the deadline**; the effective deadline delivered to the final action is compared exactly); two real processes, one killed by `SIGKILL`, and a drained `SIGTERM` |
| Inbox and message input | `inbox_test.go`, `internal/app/inbound_test.go` | inbox and finances in the same commit (balance, ledger, outbox, inbox); 50 redeliveries = one inbox row and no extra effect; the same `messageId` with another body = permanent conflict with the original intact; a new envelope for the same operation = financial replay; a rejection without a ledger entry; a pending operation with a durable schedule; a race between deliveries resolved by the database; permanent inputs without a financial record and an idempotent diagnostic; transient failure/uncertain commit/unexpected error = retry and **an inbox failure rolls back the finances**; append-only and minimal privileges; the 00008 migration applies, rolls back and reapplies; strict envelope parsing |
| SQS consumer | `consumer_test.go`, `sqs_e2e_test.go`, `internal/inbound/consumer_test.go` | every kind through SQS with the same rules; **50 real redeliveries** (received by the application, with a controlled bypass of FIFO deduplication); conflict → diagnostic → DLQ with the exact body before the Delete; invalid messages without a fictitious operation; a crash after the commit and before the Delete recovered by a real redelivery; an already recorded diagnostic does not exempt the DLQ send; retry with backoff without a Delete before the commit; visibility-change failure; redrive after the limit; shutdown by drain and by cancellation; producer bound to the origin and unauthorized principals denied; concurrent HTTP and SQS (two 80 BETs over 100, the same operation, the same key with different content); complete flow up to the subscriber |
| Metrics | `metrics_test.go`, `internal/metrics/metrics_test.go` | 401/403/200; every category with real observations (full application); labels without ids; outbox backlog, age and reserved; real divergence; collection failure without a fictitious zero |
| Three processes with SQS | `sqs_multiprocess_test.go` | binary **compiled with `-race`** (a race or crash of the child fails the test), 50 redeliveries proven by the logs, 80 BETs crossing HTTP and SQS, `SIGKILL` in the middle of handling, drained `SIGTERM` |
| Database | `constraints_test.go`, `db_integrity_test.go` | direct SQL: uniqueness, non-negative balance, append-only ledger (UPDATE/DELETE/TRUNCATE), balance×ledger in the same commit, unique OPENING, internal×external shape, zero policy, immutability of terminal states and of the outbox; **continuous ledger chain** (review scenario, first entry, several entries per transaction, stale concurrent writer, independent wallet not blocked); **entry→transaction→wallet link**; **explicit result (NULL and invalid values)** |
| Roles | `role_test.go` | the runtime role is neither a superuser nor an owner, and 20+ attempts (UPDATE/DELETE/TRUNCATE on the ledger, `DISABLE TRIGGER`, `session_replication_role`, DDL…) are denied; the financial flow and the schema check work with it; idempotent provisioning |
| Upgrade with active writers | `upgrade_window_test.go` | audit probe: an old writer with an open `BET PROCESSED` without an entry → the upgrade waits → commit → the schema **never** ends at the final version with the orphan (it reports the id, nothing is deleted, the API refuses to start; once fixed manually, the upgrade completes); a held writer and a valid writer concurrently; already invalid history (orphan, result, chain, balance, entry of a rejected transaction); missing/disabled trigger; a writer that ignores triggers; wait/hold on the lock; data preserved; rollback/reapplication |
| Migrations | `migration_test.go`, `migration_movement_test.go` | the corrective migrations preserve the data of an existing database (and on rollback), refuse without changing anything historical with a broken chain, a null result, a completed transaction without an entry or a divergent result (reporting count and ids), and apply→revert→reapply works |
| Authentication | `auth_test.go` | missing, tampered, forged, `alg:none`, HS256, expired credentials (real 3 s token), wrong audience → 401; no role → 403; isolation between providers (query, replay, fake headers); no state change on denied access |
| Faults | `faults_test.go` | a failure in the middle of the transaction leaves no partial state; a "forgotten" ledger entry is blocked by the database at COMMIT; a confirmed commit with a lost ACK → 503 and recovery with the same key |
| Lifecycle | `lifecycle_test.go` | a restart preserves idempotency/results; a valid Fx graph, start/stop and release of connections; **7 startup failures (including missing/outdated schema and invalid credentials) with no PostgreSQL connection left over**; shutdown completes an in-flight request |
| Reversals | `reversal_test.go`, `reversal_http_test.go` | the chain `BET → REFUND → ROLLBACK of the REFUND` (balance, ledger, events, replays after new movements) and new reversals `ALREADY_REVERSED` without a ledger entry; `ROLLBACK` of a `BET`/`WIN`; `WIN` with a reference of a different amount without consuming the reversal; a reversal without balance (`REVERSAL_INSUFFICIENT_FUNDS`, the rejection does not consume the target); incompatible references (amount, kind, round, wallet, `LOSS`, `REJECTED`); **simultaneous `REFUND` × `ROLLBACK`**: one success, one rejection, one credit (15 rounds); HTTP contract (200/202/422) |
| Pending operations | `pending_references_test.go`, `shutdown_test.go` | acceptance with a single schedule and event, a deadline that is not renewed on replay/attempt, resolution by the same operation, resolution **after the TTL**, a terminal expiry that does not reopen, `REFERENCE_PENDING_TIMEOUT`, a reference that ends unsuccessfully, a lookup failure that does **not** become `REFERENCE_NOT_FOUND`, a failure before/after the commit (unknown result) without duplicating, 10 executors on the same pending operation, a busy wallet skipped while the others progress, a lagging clock; the TTL is crossed with an **injected clock** (without sleeping) |
| Reference guarantees in the database | `reference_constraints_test.go`, `migration_references_test.go` | direct SQL through the **restricted role**: a reference outside the scope/kind/state/amount, `NULL`/incomplete shapes, a second reversal (unique index), a `ROLLBACK` with an entry in the wrong direction (at COMMIT), immutable columns (privilege and trigger), `PENDING`/`PENDING_REFERENCE` without a schedule, a schedule with no deletion/deadline change; migration 00005: data preserved, incompatible history refused (5 shapes), rollback/reapplication, surviving pending operations |
| Fixes from the audit of increment 2 | `audit_regressions_test.go` | (1) a busy wallet at the start of the scan does not prevent another from advancing (≥ `BatchSize` pending operations on the busy one; several instances; a fully busy scan finishes with few queries, touching no schedule); (2) an old candidate does not bypass the backoff rescheduled by another instance (synchronization through the read decorator's channels); (3) rollback/reapplication preserve the schedule (including completed ones and an expired deadline), the window of 00005's rollback is observed (write revoked, full copy), `down-to 0` is destructive, an orphaned copy blocks the restoration; (4) the database refuses `PENDING_REFERENCE` of BET/LOSS/OPENING/WIN without a reference and references with invalid shapes (NULL, empty, spaces, control characters, long), accepts the valid ones and resumes them, and the migration refuses invalid history with ids. The scans use **test-exclusive databases** |
| Fixes from the re-audit of increment 2 | `reaudit_regressions_test.go` | (1) a free wallet advances behind **120 distinct busy wallets** with `BatchSize=1` (far beyond any page budget), without touching the busy ones, and several concurrent instances advance it **only once**; (2) `DownTo(4) → UpTo(5) → Down → Up` and a `Down` cancelled with goose's lock held by another session **preserve the original copy** (7-min TTL, expired, 7 attempts and diagnostic, completed schedule; all columns), the runtime's write access is given back by the migrator, a repetition refreshes the copy from the live schedule, a rollback that would lose data is **refused without changing anything** (API and `migrate` binary) and a second migrator waits for the first; (3) a role with only **column-level** grants, `PUBLIC` at column level and a role that inherits from a group are left without write access during the window, the default runtime stays blocked and recovers the grants on cancellation, and a writer that cannot be excluded refuses the rollback before any change |
| Fixes from the focused re-audit (2109d38) | `reaudit2_regressions_test.go` | (1) a free wallet advances behind 60 wallets held by **each** lock mode (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, `FOR KEY SHARE`, non-key `UPDATE`) and the prefix's transaction commits with the constraints; (2) a localized failure on one wallet (batches 1 and 20): the free one advances, the failing pending operation stays `PENDING_REFERENCE`, is deferred with backoff and diagnostic and the retry proceeds through the durable state; a general failure stops after 3 consecutive failures; (3) a direct `Up`/`UpTo` after a cancelled `Down` preserve the work the runtime committed on the live schedule (attempts 8, not 7), the following cycle still preserves and a modified rebuild refuses the `Up` without changing anything; (4) the binary `migrate grants` and `migrate up` wait for a prepared `Down`, the runtime stays denied while they wait, cancellation (SIGINT) finishes without leaking a lock, and after `SIGKILL` of the migrator `grants` and `up` recover without losing work |
| Residual fixes of the base (06c6c65) | `reaudit3_regressions_test.go` | (1) three schedules held by real runtime `UPDATE`s (without locking the wallets, batches 1 and 20, `PhaseTimeout` of 2 s): the first scan completes the free wallet, with no error and without waiting, and the held ones are resumed again when released; with the attempt **and** the deferral failing (4 and 7 wallets), the healthy one advances in few calls through the continuation position, with nothing persisted for the prefix; a general failure costs ≤ 3 attempts per call; (2) `DownTo(4) → UpTo(5) → migrate grants → Up/UpTo(6)`: with the decision committed and the restoration held, the runtime has no write access, a real resume is denied, the restoration brings back the original schedule, the privileges return and the following work functions |
| Paginated ledger | `ledger_reads_test.go`, `internal/app/ledger_test.go` | a full traversal across several pages and limits (1, 3, 7, 8, 9, 100; edges `limit` = remaining and +1; default 50) in `seq` order, with no leakage from another wallet; invalid or repeated `limit`/`cursor`, or one from **another wallet** → 400 without data; a zero wallet → `items:[]`, nonexistent → 404; a malformed query (`limit=%ZZ`, `cursor=%ZZ`, a repetition with an invalid escape, the `;` separator, a valid repeated cursor with `%ZZ`, a malformed unknown parameter) → 400 `INVALID_QUERY` without data, and 401/403 before it; equal `createdAt` and `seq` gaps; new entries in the middle of the navigation (without loss or duplication, without an `idle in transaction` session); 6 parallel writers on the same wallet (every traversal is a prefix of the final order); cursor: canonical form, version, position, size |
| Reconciliation | `reconciliation_test.go`, `internal/app/ledger_test.go` | zero wallet, opening, the challenge's example (100.00 + BET 25.00 → 75.00/75.00/0.00, 2 entries), chain BET/WIN/LOSS/rejection/REFUND/ROLLBACK, state intact after reading; **a movement committed between the queries** (barrier): a single-moment response and a writer without waiting; separate sums **above `int64`** with a final balance of `MaxInt64` (exact); a real divergence on an exclusive database (response, log, signal, nothing changed, negative difference); a sum outside `Money` → failure (not a divergence); a read failure → error; arithmetic in units (`big.Int`) |
| Coordinated shutdown | `internal/lifecycle/coordinator_test.go`, `shutdown_test.go` | order (admission on all → drain → cancellation → confirmation), incomplete termination, startup failure; with real infrastructure: a request stuck on the lock is refused/drained/cancelled and the **pool only closes afterwards**, a stuck worker attempt is cancelled and another instance completes, startup with an occupied port without leaking connections, an incoherent configuration refused |
| Multi-process | `multiprocess_test.go`, `win_loss_test.go`, `references_multiprocess_test.go` | **3 processes of the real binary**: 50 spread resends, a 2×80.00 contention across processes, own connections, `SIGTERM` with exit 0; `SIGKILL` of a process in the middle of the load without duplicating or losing a debit; `WIN`/`LOSS` resends; **pending reversals resolved by the executors of the three processes** (per wallet: one processed, one rejected, one credit), `SIGKILL` of the process that accepted the pending operation, expiry with three executors (one rejection, one event). The children binary is compiled **with `-race`** by the harness (see above) |

## Continuous integration

`.github/workflows/ci.yml` runs on pull requests and on `main`; every job must pass for the merge:

| Job | What it runs |
| --- | --- |
| `verify` | `gofmt`, `go mod verify`, `go mod tidy` with no differences, `go vet` (with and without `-tags integration`), `go test -race` |
| `infra` | `docker compose up --wait` of PostgreSQL, Keycloak and MiniStack, `broker-provision` (twice: the credentials do not change) + `scripts/check-infra.sh` (tokens, roles, `provider_id`, queues, redrive, events destination, delivery policy, principal policies) |
| `integration` | the integration tests with `-race` against real PostgreSQL, Keycloak and MiniStack, including the broker authorization proof |
| `stack` | `docker compose up --build` (image, migrations, runtime role, API), waits for the API to become `healthy` (`scripts/wait-healthy.sh`: structured `docker inspect` state, exact comparison, with a deadline and diagnostics) + `scripts/smoke.sh` (HTTP flow, internal SQS producer → consumer, outbox drained and delivered to the audit queue, metrics) |

The same scripts run locally: `scripts/check-infra.sh` (with the infrastructure up), `scripts/wait-healthy.sh <service> [seconds]` and `scripts/smoke.sh` (with the stack up).
To validate Compose without touching your environment, use another project and other ports:

```sh
export POSTGRES_PORT=15433 KEYCLOAK_PORT=18081 BROKER_PORT=14566 API_PORT=18080 BROKER_DIR=/tmp/wager-check-broker   # isolated broker credentials
docker compose -p wager-check up --build -d
API_URL=http://localhost:18080 KEYCLOAK_URL=http://localhost:18081 COMPOSE="docker compose -p wager-check" scripts/smoke.sh
docker compose -p wager-check down -v      # removes only the verification project
```

## Structure

```
cmd/api            HTTP server (Fx)              cmd/migrate        CLI for migrations and the runtime role
cmd/broker-provision idempotent broker provisioning (queues, topic, identities, policies, credentials)
cmd/sqs-producer   internal SQS producer (origin bound to the providerId, principal wager-producer)
internal/domain    Money, Wallet, WagerTransaction, LedgerEntry, events, canonical hash (no Fx/HTTP/SQL)
internal/app       use cases (OpenWallet, Submit, ProcessMessage, reads, reconciliation), persistence ports and the Observer
internal/inbound   SQS consumer (retry, DLQ forwarding, shutdown-aware) and the internal producer's origin binding
internal/outbox    outbox publisher loop (lease, token, retry, lifecycle); no SQL and no SDK
internal/metrics   Prometheus-compatible text exposition; no external dependency
internal/lifecycle shutdown coordinator (admission, drain, cancellation, termination confirmation)
internal/infra/    postgres (pgx, unit of work, repositories, migrations, outbox store) · auth (JWT/JWKS) ·
                   httpapi (handlers, errors, health, metrics, server) · sqs (readiness, consumer transport, producer) · sns (publisher client) ·
                   broker (provisioning and per-responsibility credentials)
internal/bootstrap composition with fx.Module and fx.Lifecycle
deploy/            Keycloak realm
scripts/           check-infra.sh (environment) and smoke.sh (end-to-end flow through the API)
.github/workflows  CI
test/integration   tests with real infrastructure (build tag `integration`)
```

## Scope and open items

**Implemented and tested:** validated configuration; Fx with lifecycle and **coordinated shutdown**; Docker Compose; Keycloak and
MiniStack provisioned; migrations (schema version 8); real health checks; Money; Wallet; WagerTransaction (complete state
machine in the domain); LedgerEntry; wallet opening; **`REFUND`, `ROLLBACK` and `WIN` with a reference**, **pending references
(`PENDING_REFERENCE`) with a durable schedule, TTL, backoff and a worker**, one reversal per target enforced in the database; `BET` with persistent
idempotency, rejection for insufficient balance, atomic ledger and outbox; `WIN` (without a reference) and `LOSS`; queries with per-provider isolation;
real authentication/authorization; the **SQS consumer with a durable inbox**, retry, DLQ and an internal producer; the **outbox publisher** (lease, token, retry, recovery); the **paginated ledger** and
**reconciliation**; and **metrics**. **Database guarantees** (migrations 00001–00003, verified by direct SQL through the restricted
role): append-only ledger with a continuous chain; balance = last `balance_after`; every `PROCESSED` transaction with a movement
requires, at COMMIT, the entry of its own wallet and `result_balance` = its `balance_after` (`LOSS` and rejections have no
entry); identity uniqueness; immutable terminal states. **Identity precedence:** an already registered key or external id is a
replay/conflict (409) before the new-operation policies (currency, kind).

**State of the items the challenge statement lists as required and of the optional ones:**

| Challenge item | Status |
| --- | --- |
| SQS consumer and inbox table | **implemented** (see [SQS consumer and inbox](#sqs-consumer-and-inbox)) |
| Outbox publisher (multiple instances, backoff, recovery) | **implemented** (see [Outbox publisher](#outbox-publisher)); *at-least-once*, no global order |
| Metrics (results, duplicates, retries, DLQ, outbox delay, reconciliation) | **implemented** (see [Metrics](#metrics)); process-local counters, no percentiles; no tracing or dashboards |
| Broker access control (credentials and policies) | **proven as emulated authorization** (MiniStack 1.5.27, `AUTH=true`; proof in CI). MiniStack **does not validate SigV4 signatures**: it is neither full AWS authentication nor production proof (see [Broker](#broker-ministack-with-authtrue)) |
| Crossed SQS/HTTP tests, DLQ, consumer interruption | **proven** (see Tests) |
| 3-process proof for SQS/outbox | **proven**: three real processes of the binary compiled with `-race` (a consumer and a publisher in each), 50 redeliveries, two 80 BETs over 100 crossing HTTP and SQS, one process killed by `SIGKILL` and the others stopped by `SIGTERM` |
| Load tests, OpenTelemetry, double-entry bookkeeping | optional, not done |
| Limits of the current guarantees | the schema owner and superusers can disable triggers (the API runs with a restricted role); the `result_balance` of `LOSS` (observed balance, no entry) cannot be verified against the ledger; the direction of a `ROLLBACK` entry is enforced in the database since 00005 (ARCHITECTURE §10); a clock regression between instances is refused by the domain and handled by whoever calls the transition (ARCHITECTURE §3); IdP key rotation may cause 401 for up to 30 s |

**State:** all the required components exist. What was not done: fuzzing, load testing, tracing, dashboards, double-entry bookkeeping, and lease/visibility renewal. Details and limits in `ARCHITECTURE.en.md`.
