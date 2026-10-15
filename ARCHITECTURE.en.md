# ARCHITECTURE

> 🇧🇷 [Versão em português](ARCHITECTURE.md) · 🇬🇧 English version (this file) · Operation and usage: [`README.en.md`](README.en.md)

This document records the architecture decisions and the state of each delivered increment. Each decision states whether it is an
**explicit requirement** of the challenge statement (`descricao-desafio/README.md`, written in Portuguese), an adopted **interpretation**, a **technical
decision** or an **open item**. The implementation state is at the end and in the README.

## 1. Overview

```
            ┌───────────────┐   JWT (client_credentials)   ┌──────────┐
 provider ─►│  httpapi      │◄─────────────────────────────│ Keycloak │ (JWKS)
 internal ─►│  auth (JWT)   │                              └──────────┘
            └──────┬────────┘
                   │ app.Service  (OpenWallet · Submit · reads)
            ┌──────▼────────┐        ┌──────────────────────────────┐
            │  app (use     │───────►│ domain: Money · Wallet ·     │
            │  cases)       │        │ WagerTransaction · Ledger ·  │
            └──────┬────────┘        │ events · canonical hash      │
                   │ UnitOfWork      └──────────────────────────────┘
            ┌──────▼────────┐
            │ postgres (pgx)│  one SQL transaction: wallet + transaction + ledger + outbox
            └───────────────┘
```

The diagram shows the HTTP entry point. The other entry points and the output side use the same pieces: the **SQS consumer** (`internal/inbound`) calls
`app.Service.ProcessMessage`, which runs the same financial core in one transaction that also writes the inbox (§7); the **reference worker** resumes pending
operations; and the **outbox publisher** (`internal/outbox`) publishes the committed events to SNS, outside any SQL transaction (§7).

* **Domain** (`internal/domain`) does not import Fx, HTTP, SQS or pgx. It depends only on the standard library and `google/uuid`.
* **Application** (`internal/app`) orchestrates use cases and defines the ports (`UnitOfWork`, repositories).
* **Infrastructure** (`internal/infra/*`) implements the ports and the entry points. **Entry points**: HTTP and SQS (the consumer),
  both running the same financial core (`processExternal`, §5).
* No generic hierarchies: one interface per concrete need (`UnitOfWork`, the repositories — wallets, transactions, ledger, outbox, the schedule of pending operations, and the inbox
  and message diagnostics —, `httpapi.Service`/`TokenVerifier` for the handlers). Divergence from the prompt's initial recommendations: **none**.

## 2. Money (requirement §5.1, §6.1)

* `domain.Money` is an immutable value object: **`int64` in cents + currency** (fixed scale of 2). Range
  ±92,233,720,368,547,758.07; `math.MinInt64` is rejected at construction so that negation can never overflow.
* **No `float32/float64`** in parsing, calculation, JSON or persistence. Parsing is by *string*, byte by byte
  (`parseMinor`); the JSON requires `amount` as a **string** (a JSON number is rejected); the database uses `BIGINT`
  (a test scans `information_schema` and fails if there is any `real/double/numeric/money`).
* **Accepted grammar** (interpretation): `0 | [1-9][0-9]*` with an optional fraction of **1 or 2** digits. Therefore
  `"25"`, `"25.5"` and `"25.50"` are accepted and **normalized** to `25.00`/`25.50` before the idempotency hash.
  Rejected: empty, spaces, `+`, `NaN`, `Infinity`, scientific notation, leading zeros, `1.`/`.5`, more than two
  decimal places (`ErrAmountScale`, it never rounds), negatives on external inputs (`ErrAmountNegative`) and overflow
  (`ErrAmountOverflow`, detected in parsing, `Add`, `Sub` and `Neg`).
* **Initialized zero ≠ uninitialized**: `Money{}` is invalid (`ErrUninitializedMoney` in every operation);
  `Zero(currency)` is the valid zero. `Equal` between uninitialized values is false.
* Currencies: format `[A-Z]{3}` in the domain; the accepted set is policy (`ALLOWED_CURRENCIES`, default `BRL`).
  Arithmetic between different currencies fails with `ErrCurrencyMismatch`. The database also prevents a mismatch
  (composite FKs `(wallet_id, currency)`).
* **Zero policy per kind** (`Kind.ValidateAmount`): `LOSS` requires `0.00`; `BET/WIN/REFUND/ROLLBACK` require
  `> 0`; the initial balance accepts `0`. Mirrored in a `CHECK` in the database.

## 3. Domain and encapsulation (§6)

* Private state, validated constructors and explicit transitions. Creation and rehydration are distinct paths:
  `NewWallet` / `RehydrateWallet`, `NewExternalTransaction` / `RehydrateWagerTransaction`,
  `NewLedgerEntry` / `RehydrateLedgerEntry`. Rehydration validates coherence and does **not** reapply movements,
  transitions or events (tested).
* **Wallet**: `Debit`/`Credit` return the corresponding `LedgerEntry` and increment the version only when the balance
  changes; a debit failure (`ErrInsufficientFunds`) does not change the state. Initial version `1`; the opening credit
  does **not** increment it (the opening is the creation state), as the challenge statement asks.
* **WagerTransaction** — state machine (validated in the domain and, for terminal states, in the database by a trigger):

  ```
  PENDING ─► PROCESSED | REJECTED | FAILED          (terminal)
  PENDING ─► PENDING_REFERENCE ─► PROCESSED | REJECTED | FAILED
  ```

  A **transient** failure (database unavailable, timeout, uncertain commit) **aborts the attempt** and produces no state: the
  client repeats with the same key. `FAILED` is reserved for a **permanent** infrastructure failure that needs to remain
  auditable (e.g., a message exhausted on the DLQ, in the SQS increment). `REJECTED` = a business rule, with a `failureCode`.
  **Validation and encapsulation**: `ExternalRequest` is immutable (private fields, getters only, a single constructor
  `NewExternalRequest`) and its hash is derived from the validated fields, so business fields and hash do not diverge;
  `NewExternalTransaction` still revalidates the request (including coherence with the hash) and rejects the zero value.
  `RehydrateWagerTransaction` validates the complete state — kind, value policy per kind, currency and sign of the result,
  external metadata, reference policy, **recomputation of the hash from the fields** and the relation between state,
  `failureCode` and result (PROCESSED/REJECTED require a result; PENDING* do not have one) — without reapplying movements.
  **Single temporal policy** (creation, transitions and rehydration, `checkTimestamps`): `createdAt` and `updatedAt` are not null,
  `updatedAt >= createdAt` and `updatedAt` **never regresses**. `Process`, `Reject`, `Fail` and `AwaitReference` validate the source
  state, the destination, the arguments and the instant **before** changing any field; a zero instant, one earlier than creation or earlier than the
  last update is refused with `ErrInvalidTimestamp` (which is also `ErrInvalidTransaction`) and the entity remains intact
  (status, result, code, reference and timestamps). The same instant as the last update is valid, since steps of the same
  commit use the same clock. Decision on clock regression: the domain **does not silently adjust** the instant; an
  instance whose clock lags behind the last update written by another must retry later or pass the greater
  of its clock and `UpdatedAt()`. Since the persisted state only keeps the last update, rehydration only checks
  `updatedAt >= createdAt`. The chronological rule is a domain decision, not a literal requirement of the challenge statement.
  References (`*uuid.UUID`) are copied on entry to rehydration and on every read (`ReferenceTransactionID()`,
  `External()`), so no caller can change a terminal transaction through an alias.
* **LedgerEntry**: `balanceAfter = balanceBefore ± money` validated at construction (and by a `CHECK` in the database).
* **Events** (`events.go`): concrete types with type and version defined by the constructor
  (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`).
  Envelope: `eventId, eventType, aggregateId, correlationId, causationId?, occurredAt (UTC, ms), version, data`.
  `WalletBalanceChanged.data` = `walletId, transactionId, direction, money, balanceBefore, balanceAfter, walletVersion`.
  Opening events (internal origin) omit the external metadata.
* `domain.OpenWallet` aggregates what goes in the same commit: the wallet, a `PROCESSED` `OPENING`, the credit (previous balance zero)
  and the two events; **a zero balance creates only the wallet** (no OPENING, ledger entry or events).

## 4. SQL transactions and concurrency (§8)

* **Library**: `pgx/v5` with explicit SQL (no ORM). `Store` implements `app.UnitOfWork`:
  `Do(ctx, fn)` opens **one** `READ COMMITTED` transaction and hands `fn` the repositories (`Wallets`, `Transactions`,
  `Ledger`, `Outbox`, plus `Pending`, `Inbox` and `Diagnostics`) **bound to the same `pgx.Tx`**. Reads without a transaction use `Reader()` (repositories over the pool).
  The repositories depend on a minimal interface (`Exec/Query/QueryRow`) satisfied by both the pool and the transaction.
* **Money mapping**: `BIGINT` in cents + `currency TEXT`; rebuilt with `FromMinor`.
* **Pessimistic locking per wallet**: `SELECT … FROM wallets WHERE id = $1 FOR UPDATE` at the start of every financial
  operation. Operations on distinct wallets do not block each other (no global lock; a test holds wallet A's lock
  and proves that B completes). The lock is per row and lasts until the end of the transaction.
  Chosen over optimistic locking because the invariant "balance ≥ 0 with a deterministic decision" needs a total order per
  wallet, and optimistic retries under contention (50 resends, 2×80) would only add latency and spurious 409s.
* **Second barrier (defense in depth)**: `UPDATE … SET balance, version … WHERE id=$1 AND version=$expected`
  (compare-and-swap); rows affected ≠ 1 → `ErrConcurrentModification` (503, safe to repeat). By removing the `FOR UPDATE`
  in a test mutation, the concurrency tests fail — and the CAS prevents a silent lost update.
* **Database as the final arbiter** (§8 of §5): see §10.
* **Commit failure**: if the `COMMIT` returns an error **from the server** (`PgError`, e.g. a deferred constraint), the transaction
  was rolled back. If there is no response (timeout, lost connection), the result is **unknown**:
  `app.ErrCommitUnknown` → HTTP 503 `COMMIT_OUTCOME_UNKNOWN`. The "identity for recovery" is the
  `Idempotency-Key` + `(providerId, externalTransactionId)`: repeating the request returns the original result if the
  commit happened, or processes it if it did not (a test with a lost ACK).

## 5. Idempotency (§5.2, §9)

**`Submit` flow** (one SQL transaction):

1. Parses, validates and **normalizes** the input (`NewExternalRequest`): structurally invalid inputs (unknown kind or `OPENING`,
   malformed amount/currency, zero and reference policy per kind, identifiers) are refused here, even if the identity
   exists. Nothing that depends on policy or on state is evaluated yet.
2. **Identity first**, without a lock, in a **single query** (`FindByIdentity`: key *or* external id, one snapshot — two
   separate queries could see a concurrent commit halfway and report a conflict for a legitimate resend).
   Same key: equal hash → **replay** of the persisted result (`idempotentReplay:true`, **original** balance, including for
   `REJECTED`); different hash → 409 `IDEMPOTENCY_KEY_REUSED`. Only the external id matches (another key) → 409
   `EXTERNAL_TRANSACTION_ALREADY_REGISTERED`. This comes **before** any new-operation policy and any validation that
   depends on the current state: reusing the key with a currency that is not enabled (EUR in a BRL application), with another kind (`BET` → `WIN`,
   `LOSS`, `REFUND`, `ROLLBACK`), a nonexistent wallet, or a different player or currency is a conflict (409), not 422/501/404, and a legitimate
   replay depends neither on the wallet nor on the current currency configuration. The search is always within the scope of the authenticated provider
   (authentication and authorization come before everything and do not change).
2b. **New-operation policies** (only if the identity does not exist), in this order: enabled currency (422 `CURRENCY_NOT_SUPPORTED`) and a kind
   with implemented processing (501 `KIND_NOT_SUPPORTED`, nothing is written; today every external kind has processing, so this branch
   is only a safeguard). Implemented: `BET`, `WIN` (with or without a reference), `LOSS`, `REFUND` and `ROLLBACK`. A received reference is never
   ignored: either it is resolved and validated, or the operation waits for it as `PENDING_REFERENCE` (§8).
3. `GetForUpdate(walletId)`; nonexistent → 404; mismatched player/currency → 422 (**input** errors, not written).
4. With the wallet locked, repeats step 2 (a concurrent operation on the same wallet may have committed between the steps).
5. Applies the kind's rule, writes wallet (CAS), transaction, ledger and outbox, and commits:
   * `BET`: debit, or an `INSUFFICIENT_FUNDS` rejection without a movement;
   * `WIN`: credit (one ledger row `CREDIT`, `WagerTransactionProcessed` + `WalletBalanceChanged`); a credit that would exceed the
     `Money` limit (`ErrAmountOverflow`) interrupts the attempt, since the wallet only changes after the movement has been validated
     in full — nothing is written (HTTP 400 `INVALID_AMOUNT`);
   * `LOSS`: the amount is exactly `0.00`; the wallet is locked to observe the balance, which becomes the `result_balance` (and what the replay
     returns), the transaction is born `PROCESSED` and only produces `WagerTransactionProcessed` — **no** ledger entry, no change to balance or version
     and no `WalletBalanceChanged`.

   **Shareable core.** Steps 2–5 live in `Service.processExternal`, which receives the attempt's repositories (all on the same
   `pgx.Tx`) and **neither opens nor commits** a transaction: `Submit` opens the `UnitOfWork`, calls the core and leaves the commit to the `UnitOfWork`.
   Other entry points call the same core inside *their own* transaction, so that their own records and the financial effect share the commit: the SQS consumer
   (`Service.ProcessMessage`, which also writes the inbox; §7) and the reference worker.
6. If a database uniqueness constraint fires on the INSERT (the same identity applied to **another** wallet, therefore without contending for the lock),
   the attempt is rolled back and **redone in a new transaction** (up to 3×), which then sees the winning commit and answers
   replay/conflict. Uniqueness violations do not become a generic error: `classify` maps them by **constraint name**.

**Payload hash** (`ExternalRequest.PayloadHash`, SHA-256 hex of the canonical JSON), the same for HTTP and SQS (this is the **financial** identity/hash; the SQS message's
own identity/hash is different and covers the exact body bytes, see §7):

* Fields: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}` and, if
  present, `referenceExternalTransactionId`. **Excluded**: the idempotency key and transport metadata.
* Normalizations: lowercase UUIDs; `amount` in fixed scale; absent optionals omitted.
* Serialization: no spaces, keys in lexicographic order, UTF-8, no HTML escaping. There is a fixed vector in the test,
  independently checked in Python (`json.dumps(sort_keys, separators=(",",":"))` + sha256).

**Interpretations**:

* The idempotency key is **scoped per provider** (`UNIQUE(provider_id, idempotency_key)`): the provider comes from the
  token, so providers neither collide nor see each other's results.
* `(providerId, externalTransactionId)` already registered under **another** key → **always 409**, even with equal content.
  The server never swaps a received key for a computed one.
* A replay of a **terminal** operation never re-evaluates: a `REJECTED` for balance stays `REJECTED` after new
  credits (test). The returned balance is the one stored in `wager_transactions.result_balance`.
* Concurrent replay: 50 simultaneous resends → 1 debit and 49 replays (a test also spreads them across 3 processes).

## 6. HTTP contracts and failure codes

Status/`code` table in the README. Principles: a correctable 4xx = input (`400/404/422`) and is **not written**;
a final business result = `422` with a transaction body (`status: REJECTED`, `failureCode`) and **is audited**;
idempotency conflict = `409`; unavailability = `503` + `Retry-After`; uncertain commit = a distinct `503`
(`COMMIT_OUTCOME_UNKNOWN`); `202` = accepted and pending (`PENDING_REFERENCE`: no balance, with durable resumption) — before the references were implemented it was unreachable. `501` remained reserved for a kind without processing (none today)
in this version (there is no asynchronous acceptance).

`failureCode`s emitted: `INSUFFICIENT_FUNDS` (BET) and the reference and reversal ones described in §8 (`REVERSAL_INSUFFICIENT_FUNDS`, `ALREADY_REVERSED`,
`REFERENCE_NOT_FOUND`, `REFERENCE_PENDING_TIMEOUT`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`). `WIN` without a reference and `LOSS` have no
rejection code of their own: the credit does not depend on balance and `LOSS` only records the result.

Processing is **synchronous** when there is no dependency: the transaction is inserted already in its final state, without an intermediate commit of
`PENDING` (allowed by the challenge statement for operations without dependencies). When the reference is unavailable, the only persisted wait is
`PENDING_REFERENCE`, always together with the resumption schedule (§8); `PENDING` is never committed (the database refuses it), so there is no acceptance without
a durable continuation.

## 7. Inbox and outbox (§6.5, §11)

* **Outbox (writing)**: `outbox_events` keeps the complete envelope as `jsonb` (an immutable snapshot, a trigger prevents
  changing it), `eventId` = PK, `aggregate_type/id`, `correlation_id`, `causation_id`, `attempts`, `next_attempt_at`, `last_error`,
  `published_at`, `lease_token`/`lease_expires_at` (migration 00007) and `seq` (insertion order). Events enter **in the same
  transaction** as the operation; the publisher only sees what was committed. The `causationId` of `WalletBalanceChanged` = the `eventId` of the
  `WagerTransactionProcessed` of the same operation.
* **Publisher (increment 3)** — `internal/outbox` (loop, backoff and lifecycle, with no SQL or SDK), `postgres.OutboxStore` (reservation and writes
  conditioned on ownership) and `internal/infra/sns` (the `wager-publisher` principal's client). Three steps, **never with a transaction or connection
  open during the network call**: (1) reserve an event (`FOR UPDATE SKIP LOCKED`, oldest eligible, a 20 s lease with a **new token per
  acquisition**, database clock); (2) publish the stored snapshot to SNS (5 s, outside SQL); (3) confirm, reschedule (backoff 1 s–30 s with jitter,
  persisted in `next_attempt_at`) or release, requiring the token **and** a valid lease **at the instant of the write**. The lock wait happens in a
  `SELECT … FOR UPDATE` and the `UPDATE` tests ownership **afterwards**, with `clock_timestamp()` — a single `UPDATE` would not be enough, because PostgreSQL does not
  re-evaluate the predicate when the lock is released without changing the row. Zero rows = ownership lost. Recovery: an expired lease makes the event
  eligible to any publisher (even after `SIGKILL`); `attempts` counts reservations, `last_error` the last diagnostic; events are never
  discarded. Selection without *starvation*: backoff, a valid lease and locked rows stay out of the ordering (`ORDER BY next_attempt_at, seq`).
  The component joins the lifecycle coordinator (admission → drain → cancellation with **release** of the reservation → `Done` after the final SQL action).
  **Routing**: SNS FIFO `wager-events.fifo`; `MessageGroupId` = the **payload's `walletId`** (not the `aggregateId`: in transaction events the
  aggregate is the transaction) and `MessageDeduplicationId = eventId`. SNS FIFO preserves the order of the **accepted** publications within the same group, but **the
  publisher does not promise to reproduce the outbox's original order** (several publishers, retries and recovery may reorder and republish):
  consumers deduplicate by `eventId` (delivery is *at-least-once*; the broker's deduplication is temporary) and balance projections apply
  `balanceAfter` only if `walletVersion` is greater, atomically; history consumers preserve the relevant events. Limits: no lease
  renewal (a publication longer than the lease may be republished); the token protects the SQL state, not an old, already sent publication.
* **Inbox and consumer (increment 4)** — `inbox_messages` (migration 00008): identity `(consumer_name, envelope message_id)` with a UNIQUE constraint, `body_hash` = SHA-256 of the
  **exact bytes** (≠ the canonical financial hash), a row that is **already completed** and linked by an FK to the handled transaction. `Service.ProcessMessage` opens ONE UnitOfWork, looks up the inbox
  (same hash = duplicate; different hash = permanent conflict, original intact), calls the **same `processExternal` as HTTP** with the attempt's repositories and writes the inbox in the
  same commit as balance, transaction, ledger, schedule and outbox. A uniqueness violation (of the inbox or a financial one) rolls the attempt back and a new one sees it; it is never a success by itself.
  Classified result: Done (delete after the commit), Retry (transient, contention, uncertain commit, unexpected error: never delete) or Permanent (confirmed diagnostic →
  confirmed DLQ send → Delete; no fictitious transaction). `message_diagnostics` is the operational record (consumer, queue, broker id, hash, code), append-only, and is looked up
  BEFORE handling in order to resume the DLQ send. The consumer (`internal/inbound`) is a coordinator component: 4 executors, one message per search, search (context
  cancelled at admission) separated from handling (cancelled at the limit), backoff on the `ApproximateReceiveCount`, a pause on consecutive failure and final actions with the earliest **absolute instant**
  between their own limit and the global deadline. Internal producer: `MessageGroupId = walletId`, `MessageDeduplicationId = messageId`, origin bound to the existing verifier's `Principal`.
  Limits: no visibility renewal; `maxReceiveCount` counts receives (a PROCESSED operation may go to the DLQ after delete failures, without changing the money); FIFO deduplication is temporary
  (durable idempotency is the inbox and financial idempotency).

## 8. Reversals, references and pending operations (implemented)

**Approved policy** (a mentoring decision; the challenge statement requires the `REFUND`×`ROLLBACK` combination to be documented):

| Kind of the referenced movement | Reversals admitted | Effect |
| --- | --- | --- |
| `BET` | `REFUND` or `ROLLBACK`, **a single successful one in total** | full credit |
| `WIN` | `ROLLBACK`, once | full debit |
| `REFUND` | `ROLLBACK`, once | full debit |
| `ROLLBACK`, `LOSS`, `OPENING` | none | `REFERENCE_MISMATCH` |

* **One successful reversal per target movement, across the whole history**, whether `REFUND` or `ROLLBACK` (stricter than the per-kind
  uniqueness literally required). `BET → REFUND → ROLLBACK of the REFUND` restores the debit, and **the BET does not admit another successful
  reversal** (neither `REFUND` nor `ROLLBACK`); the `REFUND` does not admit another one either. There is no `REVERSED` state: the original operations
  remain terminal and the ledger receives new entries. Rejections, failures and pending operations **do not** consume the target's right to be reversed.
  Accepted consequence: the BET's `ROLLBACK` after the REFUND's `ROLLBACK` would be financially viable, but it is refused by the contract.
* A `WIN` may name a `BET` of the same round as a reference; the prize amount is free and the `WIN` **does not** consume the reversal uniqueness.
* **Reference validation** (`WagerTransaction.AssessReference`, in the domain): provider, referenced external id, player, wallet, currency and
  round equal; eligible kind (`REFUND`→`BET`; `WIN`→`BET`; `ROLLBACK`→`BET|WIN|REFUND`); **identical amount** in reversals. Incompatibility →
  `REJECTED/REFERENCE_MISMATCH` (immutable fields: it holds even if the target is still pending). An operation cannot reference its own
  external id (invalid input, 400). Resolution is **always within the scope of the operation's own `providerId`**: another provider's transaction
  with the same external id is never found or revealed.
* **Failure codes** (all `REJECTED`, auditable, with `WagerTransactionRejected` and no ledger entry; all **final** — the client fixes
  the input with *another* operation/identity, never by reopening this one): `INSUFFICIENT_FUNDS` (BET), **`REVERSAL_INSUFFICIENT_FUNDS`** (a
  reversal's debit would exceed the balance — distinct from the BET's, as the challenge statement requires), `ALREADY_REVERSED`, `REFERENCE_NOT_FOUND`,
  `REFERENCE_PENDING_TIMEOUT`, `REFERENCE_NOT_PROCESSED` (reference `REJECTED`/`FAILED`) and `REFERENCE_MISMATCH`. A correctable input (400/404/422
  on input) is not written.

### Pending operations: `PENDING_REFERENCE`, durable schedule and worker

* **Acceptance.** A reference that is still unavailable (missing, or existing and `PENDING*`) ⇒ the operation is written as `PENDING_REFERENCE`, in the **same
  transaction** as the schedule (`pending_references`) and the `WagerTransactionPendingReference` event; HTTP answers `202` without a balance. There is no intermediate
  commit of `PENDING` (the database refuses a committed `PENDING`) and no `PENDING_REFERENCE` without an open schedule (deferred check).
* **Deadline.** `expires_at = first_persisted_at + TTL` (15 min by default), fixed at the first persistence and immutable (trigger and column
  privilege): replay, restart and attempts **do not renew it**. A replay of a pending operation returns the current state, without another schedule or a new event.
* **Evaluation** (the same `Service.decide` function as the initial processing — the financial rules are not duplicated — over the durable state
  revalidated under locks):

  | Valid reference | Before the TTL | After the TTL |
  | --- | --- | --- |
  | missing | waits and reschedules | `REJECTED/REFERENCE_NOT_FOUND` |
  | `PENDING`/`PENDING_REFERENCE` | waits and reschedules | `REJECTED/REFERENCE_PENDING_TIMEOUT` |
  | `PROCESSED` | financial rules | financial rules (**even if it completed after the deadline**) |
  | `REJECTED`/`FAILED` | `REJECTED/REFERENCE_NOT_PROCESSED` | same |

  Finding the `PROCESSED` target does not waive balance, kind/scope/amount or the reversal uniqueness. A rejection by expiry that has already been committed
  is terminal and is not reopened by a late arrival or by a replay. **A failed lookup does not prove absence**: the attempt is aborted (transient
  error), nothing is written and the original deadline keeps applying.
* **Bounded scan that reaches independent wallets** (`Service.ProcessDue`): the candidate read is paginated by key `(next attempt, id)` — without
  `OFFSET` — and **skips, inside the query itself, the wallets that another transaction holds** (`JOIN wallets … FOR UPDATE OF w SKIP LOCKED`). Row lock modes do **not** have the
  same conflicts: `FOR KEY SHARE` is compatible with `FOR NO KEY UPDATE` (that of a non-key-column `UPDATE`, such as the ledger chaining trigger's), whereas
  `FOR UPDATE` conflicts with all of them. The attempt asks for `FOR UPDATE` (`TryLockForUpdate`), so the selection asks for the **same** `FOR UPDATE`: it skips wallets held by `FOR KEY SHARE`,
  `FOR SHARE`, `FOR NO KEY UPDATE`, a non-key `UPDATE` or `FOR UPDATE` (all five are tested), and a prefix of busy candidates, however large, does not hide the
  independent wallets — with no state between scans, hence also across several calls and instances. Merely increasing the batch or the pages would only shift the threshold. The query is only a
  read: the row lock lasts the time of the query (no wait and no lock during the backoff) and the wallet and the eligibility are revalidated under the attempt's locks. The **schedule rows**
  are also skipped when another transaction holds them (`FOR UPDATE OF w, p SKIP LOCKED`), the attempt reads the schedule with `SKIP LOCKED` (`ErrPendingBusy` = busy, like a busy wallet) and `Defer`
  does not wait either: no step waits for a row that someone outside the wallet protocol (another writer, a deferral by another instance) holds, so three schedules held
  by runtime `UPDATE`s do not make the attempt or the deferral exhaust the `PhaseTimeout`.
  * **A wallet that is busy at the attempt or fails** is excluded from the following pages of the same scan. An **infrastructure failure** (transient error, timeout) is never a financial
    rejection: the attempt's transaction is rolled back entirely (no partial effect) and the schedule is **deferred** with the backoff in a short single-row transaction (`Defer`: next
    attempt, counter and diagnostic; it does not renew the deadline, does not touch balance/ledger/state, does not overwrite a newer rescheduling or a completed schedule). Thus a prefix of failures
    on one wallet (batch 1 or 20) does not monopolize the scans or the budget: the independent wallet advances and the retry proceeds through the durable state when the deferral expires. If the deferral
    itself cannot be persisted (or the failure is not deferrable), there is a **continuation position across calls** (in memory, per instance): a scan interrupted by the failure ceiling
    makes the next one restart **after** the prefix that failed, and a scan that ends normally resets it. Thus the healthy wallet advances in few calls even with no persisted
    deferral (tested with 4 and 7 failing wallets, batches 1 and 20, with no working `Defer`), without raising limits; in a real outage the cost stays a few attempts per call.
  * **Bounded work**: at most `BatchSize` effective attempts, 50 pages and **3 consecutive failures** (a general failure: the scan ends with `failed=true` and the worker spaces out the next one, without
    hammering the dependency; the next call continues after the prefix); wallets skipped for being busy, those not yet due and those that failed do not count as effective attempts, but each failing wallet is tried once per
    scan. Cancellation is honored between candidates (and a cancellation error postpones nothing); a fully busy scan ends without trying anything and **without touching** any
    schedule, and the worker only scans again after the wait interval, with no busy loop.
* **Temporal eligibility under the locks**: after locking the wallet and the schedule, the attempt compares the **reloaded** `next_attempt_at` with the clock; an old
  selection does not authorize an attempt that another instance has already rescheduled to the future (the persisted backoff applies to all of them). There is no "forced" path in the worker:
  the tests advance the clock on purpose. Eligibility is not a hard validity deadline: a `PROCESSED` reference still resolves after the TTL.
* **Resumption** (`Service.ProcessPending`): a short transaction (financial phase of up to 10 s, including locks and commit) that reloads and revalidates the
  **original** operation — the same id, hash and key; it neither recreates anything nor uses rehydration as a mutation — and completes state, balance, ledger, outbox and
  schedule in the same commit; or just reschedules (`next_attempt_at`, `attempts`), without repeating the initial event. **Persisted** backoff:
  exponential from 1 s, ceiling 30 s, uniform jitter in `[d/2, d]` (never above the ceiling); no transaction or lock is held during the backoff.
* **Single lock order** (HTTP, resumption and the SQS input): **wallet → pending-operation schedule → transaction row**. The HTTP path
  locks the wallet first; the resumption reads candidates *without locks* (only a listing, which authorizes no movement), locks the wallet with
  `FOR UPDATE SKIP LOCKED` — a busy wallet is **skipped** (the others proceed, with no global lock) and never traps the resumption in the inverse order
  — and then the schedule. Since the target always belongs to the same wallet (another wallet is `REFERENCE_MISMATCH`, decided by immutable fields), the target's state
  and the reversal uniqueness are decided without a race under the wallet lock.
* **Worker**: one executor per process (`REFERENCE_WORKERS=1`), with no lease; several processes may run theirs at the same time, and the result is
  unique because the wallet and the schedule are locked and revalidated (a completed schedule disappears from the candidate queries). Instances with different
  clocks: the resumption uses the **greater** of its clock and the transaction's `updated_at` (the domain never adjusts an instant by itself), so
  that a lagging clock does not become a client input error. The TTL and the backoff use the application's clock (the difference between instances
  shifts the deadline, never renews it). `REFERENCE_PHASE_TIMEOUT` and the shutdown budget bound each attempt.
* **Concurrency of reversals**: simultaneous `REFUND` and `ROLLBACK` against the same BET ⇒ one `PROCESSED`, the other `REJECTED/ALREADY_REVERSED`,
  a single credit. The partial unique index (§10) is the final barrier: a violation (`uq_wager_tx_one_reversal_per_target`) is classified as
  a contention (`ErrReversalTargetTaken`), the attempt is rolled back and **redone in a new transaction**, which sees the winner and rejects (it never commits a partial effect).

## 9. Authentication and authorization (§2)

* **IdP**: Keycloak (realm `wager`, imported from `deploy/keycloak/wager-realm.json`). `client_credentials` between services;
  no password registration and no token issuance of our own.
* **Local JWT validation** (`internal/infra/auth`): **RS256** signature through the IdP's JWKS, `iss`, `aud=wager-api`, mandatory `exp`
  and clock tolerance. No remote introspection per request (latency and availability);
  trade-off: a revoked token is valid until it expires (5 min by default in the realm). Rejected: `alg:none`, HS256 (algorithm
  confusion), invalid signature, unknown key, wrong audience, expired.
* JWKS loaded at **startup** (an initialization failure if the IdP is down) and refreshed every 15 min; an unknown `kid` triggers a re-read limited to **1 per 30 s** and, above the limit, **fails immediately** (401) instead of holding the
  request — this prevents tokens forged with a random `kid` from overloading the IdP or holding goroutines. Trade-off:
  after a key rotation, new tokens may fail for up to 30 s.
* **Permission model** (realm roles): `internal-service` (wallets; reads any transaction) and `provider`
  (sends operations; reads only its own). The **authorized `providerId` comes from the `provider_id` claim** (a Keycloak mapper per
  client). The body's `providerId` is only confirmed (different → 403); arbitrary identity headers are
  ignored (tested). A query by another provider's id → **404** (it does not reveal existence); through
  `/providers/{other}/…` → 403. `OPENING` (internal) is invisible to providers.
* Denied accesses have no effect: tests compare counts of wallets/transactions/ledger/outbox before and after.
* `/health/*` are public and do not expose error details (the reason goes to the log).
* **Messaging — broker access control** (challenge statement §2): access is controlled by credentials and **broker policies**, not by the
  code. Local environment: **MiniStack 1.5.27 with `AUTH=true`** (pinned image), which evaluates the IAM policies of the principal identified by the
  access key. Rationale: LocalStack does not apply IAM without a paid plan, and there is no test AWS account; the challenge statement allows MiniStack. **Assumed
  limit: MiniStack does not validate SigV4 signatures** — the access key identifies the principal, but the secret is not checked. Therefore, what is
  proven is **emulated policy-based authorization**, not full AWS authentication or production suitability; the project's real authentication is
  Keycloak's on the HTTP API. Principles:
  * **One identity per responsibility, no wildcards**: internal producer (only `SendMessage` on the input queue), runtime (message
    lifecycle on the input queue + sending to the DLQ + reading the DLQ's approximate depth), publisher (`Publish`/`GetTopicAttributes` on the output topic), inspection (observing and preparing scenarios
    on the DLQ and on the audit queue) and a principal without policies (to prove `AccessDenied`). The application **selects** the runtime profile
    (`AWS_PROFILE`/`AWS_SHARED_CREDENTIALS_FILE`, the SDK's default chain) and does not receive the provisioning credentials (the emulator's root), which stay only in the
    one-shot and in the proof; the mounted file, however, contains the five profiles and all of them are readable by the process — there is no physical isolation between them (distributing to each
    process only the profile it needs is a technical hardening recommendation). The publisher has its own principal **and** its own SNS client (the profile is read by the constructor, §7); the readiness SQS client keeps using the runtime.
  * **Trust model** (a mentoring decision): the shared queue is for **trusted internal producers**; external providers have no
    direct `SendMessage`. The producer (`cmd/sqs-producer`) binds the authenticated origin (a token verified by the existing verifier, or a controlled configuration) to the allowed `providerId` before
    publishing — a provider cannot publish as another one, and the internal service can publish for any provider; the consumer validates the envelope and the scope, but does not treat a body field as authentication. Implemented in increment 4 (§7).
  * **SNS→SQS delivery** controlled by the audit queue's policy: only the SNS service, only from the events topic (`aws:SourceArn`).
    Without the policy — or with another topic — the message is **not** delivered, even though the `Publish` is accepted (tested).
  * **Idempotent provisioning** (`cmd/broker-provision`, Go SDK v2, a Compose one-shot; also used by the proof): converges resources,
    does not duplicate the subscription, reapplies policies and reuses the keys that are still valid; the credentials file (AWS shared-credentials
    format, one profile per user, synthetic local keys, outside git) is written atomically.
  * **Ephemeral state**: without `PERSIST_STATE`, recreating the emulator container deletes queues, messages and identities; provisioning
    recreates them and the API must be restarted to re-read the credentials. No broker durability is promised (not even after `SIGKILL`).
  * **Proof** (`test/integration/broker_test.go`, in CI): with each principal's real credentials and resources isolated by prefix — positive
    flows, 20 typed negatives (`AccessDenied`/403, including the same action on another resource, with absence of effect), FIFO, visibility and
    redrive with preserved content, SNS→SQS delivered and **not** delivered without the policy, repeated provisioning and API startup without permission.
    There are no "invalid secret" tests: the emulator would not reject them. FIFO deduplication is **transport** (a temporary broker window); the financial guarantee is the application's
    (idempotency and inbox in the database).
  * **For real AWS** (not proven here): SigV4 signatures and real secrets; IAM roles instead of users with static keys; resource policies and encryption (KMS)
    of the queues and the topic; private endpoints and background redrive (MiniStack applies it on the next receive attempt).

## 10. Guarantees enforced by PostgreSQL (§5.3, §5.8)

Migrations `00001_init.sql` to `00008_…` — verified by **direct SQL** attempts (including through the restricted runtime role) in `constraints_test.go`, `db_integrity_test.go` and `completed_movement_test.go`:

| Guarantee | Mechanism |
| --- | --- |
| One wallet per player+currency | `UNIQUE (player_id, currency)` |
| Balance ≥ 0, version ≥ 1 | `CHECK` |
| Version rises by exactly 1 with a balance change and only then; immutable identity; no wallet DELETE/TRUNCATE | trigger `wallets_guard` |
| Balance and ledger commit together | **deferred** *constraint trigger*: at COMMIT, `wallets.balance` = the last entry's `balance_after` (0 without entries), fired by a wallet change **and** by an INSERT on the ledger |
| Append-only ledger | `BEFORE UPDATE/DELETE` (per row) and `BEFORE TRUNCATE` triggers; the runtime role does not even have the privileges (§10.1) |
| **Continuous ledger chain** (migration 00002) | `BEFORE INSERT` trigger `ledger_enforce_chain`: `balance_before` = the previous entry's `balance_after` for the wallet (0 for the first). It **locks the wallet row** (`FOR NO KEY UPDATE`, the same level as the balance UPDATE): writers of the same wallet are serialized — including in direct SQL —, different wallets do not block each other, and several entries in one transaction are validated in sequence. An out-of-order write (`seq` lower than the last) fails as `40001`, handled as transient |
| Balance = credits − debits | continuous chain + the deferred constraint below (balance = the last entry's `balance_after`) ⇒, by induction, balance = Σ credits − Σ debits |
| Entry linked to the wallet and to the financial transaction | composite FK `(transaction_id, wallet_id)` → `wager_transactions(id, wallet_id)`; deferred constraint `ledger_entry_matches_transaction`: `PROCESSED` transaction, same amount, direction consistent with the kind (`BET` = debit; `OPENING/WIN/REFUND` = credit; `LOSS` never has an entry) |
| **A completed transaction requires its movement** (migration 00003) | **deferred** *constraint trigger* `ctrg_wager_tx_requires_movement` on `wager_transactions` (`AFTER INSERT OR UPDATE ... WHEN status = 'PROCESSED' AND kind <> 'LOSS'`): at COMMIT, every `PROCESSED` transaction (`OPENING`, `BET`, `WIN`, `REFUND`, `ROLLBACK`) has its own wallet's entry. `LOSS` and `REJECTED`/`FAILED`/`PENDING*` do not — and an entry for them is refused on the ledger side. **Order:** the ledger's FKs are immediate (`NOT DEFERRABLE`), so the transaction record must exist **before** the entry (the opposite fails immediately, 23503); the **completion** (`PROCESSED`) may come after the entry — inserting it already as `PROCESSED` or the `PENDING → PROCESSED` transition in the same SQL transaction — because the deferred checks evaluate the final state at COMMIT. Several operations may be committed in the same SQL transaction |
| **Result = the balance after its entry** (00003) | in the same trigger and, mirrored, in `ledger_entry_matches_transaction`: `result_balance = balance_after` **of the operation's own entry**. It is not compared with the wallet's current balance (other operations, including ones in the same SQL transaction, may have changed it); that is why a replay returns exactly the balance observed in the original processing |
| Ledger arithmetic and sign | `CHECK` (`CREDIT`: after = before + amount; `DEBIT`: after = before − amount; amount > 0; balances ≥ 0) |
| One entry per `(wallet_id, transaction_id)` | `UNIQUE` |
| Consistent currency/player across wallet, transaction and ledger | composite FKs `(wallet_id, currency)` and `(wallet_id, player_id)` |
| Idempotency | `UNIQUE (provider_id, idempotency_key)` and `UNIQUE (provider_id, external_transaction_id)` |
| Internal × external | `CHECK ck_wager_tx_origin_shape`: `OPENING` ⇒ no external metadata; external ⇒ all present and kind ≠ `OPENING` |
| Single initial credit | partial unique index `(wallet_id) WHERE kind = 'OPENING'` |
| Zero policy | `CHECK` (`LOSS` = 0, the others > 0) |
| Shape by state | `CHECK` with an explicit `IS NOT NULL`: `PROCESSED` requires a non-null `result_balance` ≥ 0; `REJECTED` requires a code and a result; `PENDING*` have neither code nor result; `FAILED` requires a code (a `CHECK` accepts `NULL` as "unknown", so `>= 0` alone is not enough). The `failure_code` format is also enforced by a `CHECK` |
| Immutable terminal states; transactions never deleted | trigger `wager_transactions_guard` |
| Outbox: immutable snapshot; `published_at` immutable once written (00007) | trigger `outbox_events_guard` (only attempts/schedule/reservation/publication change) |
| Inbox and diagnostics append-only (00008) | trigger `message_records_forbid_mutation` (UPDATE/DELETE/TRUNCATE refused, also to the owner); `ck_inbox_*` (32-byte hash, disposition, non-empty identity), FK `transaction_id` NOT NULL |
| Outbox: shape of the reservation (00007) | `CHECK ck_outbox_lease_pair` (token and deadline together) and `ck_outbox_published_has_no_lease` (a published event has no reservation) |
| **Consistent reference** (migration 00005) | `BEFORE INSERT/UPDATE` trigger `wager_tx_reference_guard`: the internal reference points to the target with the **same** provider, referenced external id, player, wallet, currency and round; `PROCESSED` target; eligible kind (`REFUND`→`BET`, `WIN`→`BET`, `ROLLBACK`→`BET|WIN|REFUND`); reversals with an **identical amount**. `CHECK`: an internal reference requires the external one (`ck_wager_tx_reference_pair`) and cannot be the transaction itself |
| **Resolved reference to complete** | `CHECK ck_wager_tx_processed_reference`: `REFUND`, `ROLLBACK` and `WIN` with an external reference only become `PROCESSED` with the internal reference filled in (the `NULL` shape is covered explicitly) |
| **One reversal per target** | partial unique index `uq_wager_tx_one_reversal_per_target (reference_transaction_id) WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'`: it applies to `REFUND` and `ROLLBACK` together; a `WIN` with a reference does not consume it; rejections/failures/pending operations do not either |
| **Direction of a `ROLLBACK` entry** | `ledger_entry_matches_transaction` (deferred): `BET` target ⇒ `CREDIT`; `WIN`/`REFUND` target ⇒ `DEBIT`; a `ROLLBACK` without a resolved target is refused |
| **Mutability of non-terminal states** | `wager_transactions_guard` (replaced): round, game, external reference and creation become immutable, the internal reference does not change once resolved, only `PENDING→*` and `PENDING_REFERENCE→PROCESSED|REJECTED|FAILED` transitions, `updated_at` does not regress |
| **Shape of `PENDING_REFERENCE`** (migration 00006) | `CHECK ck_wager_tx_pending_reference_shape`: external origin only, kind `WIN`/`REFUND`/`ROLLBACK` and external reference filled in; `CHECK ck_wager_tx_reference_form` (in any state): the external reference, when present, is non-empty, has ≤ 128 characters, no control characters, no leading/trailing spaces and differs from the operation's own external id — the shapes that rehydration rejects. The domain was not weakened |
| **Pending operation with resumption** | `pending_references` + deferred constraint triggers: **no `PENDING` is committed**; `PENDING_REFERENCE` requires an open schedule; a terminal transaction leaves no open schedule; the schedule has an immutable identity and deadline, `attempts` does not decrease and it is never deleted |

### 10.1. Database roles: migrations × runtime

* **Schema owner** (`wager` in Compose): runs `migrate up/down`, owns the tables and their triggers.
* **Runtime** (`wager_app`, created by `migrate up` when `RUNTIME_DB_ROLE` is set, or by `migrate grants`):
  `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT`, **owning nothing** and without `CREATE` on the
  schema. Minimal privileges (list in `internal/infra/postgres/roles.go`, reapplied from a known state on every run):
  ledger `SELECT, INSERT`; outbox `SELECT, INSERT` and `UPDATE (attempts, next_attempt_at, last_error, published_at, lease_token, lease_expires_at)` (payload and identity immutable; no `DELETE`/`TRUNCATE`); `inbox_messages` and `message_diagnostics` `SELECT, INSERT` (append-only; no `UPDATE`/`DELETE`/`TRUNCATE`); transactions `SELECT, INSERT` and `UPDATE (status, failure_code, result_balance,
  reference_transaction_id, updated_at)` (identity, amount, provider, key, hash, round, game and external reference are **not** updatable);
  the `pending_references` schedule `SELECT, INSERT` and `UPDATE (next_attempt_at, attempts, last_attempt_at, last_error, completed_at)` (deadline and
  identity immutable, no `DELETE`); wallets `SELECT, INSERT` and `UPDATE (balance, version, updated_at)`; `goose_db_version` `SELECT`. No `UPDATE/DELETE/TRUNCATE` on the ledger, no `DISABLE TRIGGER` (it requires ownership), no
  `session_replication_role` (it requires a superuser), no `DROP`/`ALTER`. Tested with real attempts (`role_test.go`) and with the complete financial
  flow and the schema check running with that role. Widening privileges is an explicit change to that list; 00005 widened the transition columns of the transactions and of the schedule, 00007 only the outbox's control columns
  (omitted by `migrate grants` on a database earlier than 00007) and 00008 added the two append-only tables (omitted on a database earlier than 00008). `migrate grants` omits the schedule on a database still earlier than 00005. The rollback preparation revokes write access to the schedule (§10.2).
* Remaining limitation: the owner and superusers can still disable triggers — which is why the application never runs with
  those roles. In production, create the runtime role outside the migrations pipeline if the migrations role does not have `CREATEROLE`.

### 10.2. Applying the corrections to already initialized databases

Migrations 00001 and 00002 had already been published and executed: they are **not rewritten**; every correction is a new migration (and the same holds for all the migrations published since).
`migrate up` applies the pending ones **without deleting data**, and each one begins with a prior check (*preflight*) of the history that
**aborts with the count of the problem** — and, in 00003, the first ids — if there are records incompatible with the guarantee it
installs, without correcting anything automatically (the decision about historical financial records is human; the schema stays at the
previous version).

| Migration | Preflight (aborts if there are...) | Installs | Rollback (`migrate down`, one step) |
| --- | --- | --- | --- |
| `00008_inbox_and_message_diagnostics` | no preflight (new tables) | `inbox_messages`, `message_diagnostics` and the append-only trigger | drops both tables (per-message deduplication and diagnostics; financial idempotency is not affected) |
| `00007_outbox_publisher_lease` | no preflight (the new columns arrive NULL; the backlog is not touched) | `lease_token`, `lease_expires_at`, `ck_outbox_lease_pair`, `ck_outbox_published_has_no_lease` and the guard that makes `published_at` immutable | drops only the reservation (transient), restores 00001's guard; events, schedule, attempts, diagnostics and markers remain |
| `00006_pending_shape_and_schedule_preservation` | **writer exclusion**; refuses, with count and first ids: an unresumable `PENDING_REFERENCE` (not external, a kind that does not wait for a reference, or a missing/invalid reference), an external reference with a shape that rehydration rejects (empty, > 128, control characters, leading/trailing spaces, equal to its own id) and a preserved copy without a matching transaction | `CHECK ck_wager_tx_reference_form` (any state) and `ck_wager_tx_pending_reference_shape`, function `wager_valid_external_reference`; **restores** the schedule preserved by an earlier rollback (original values, including the completed ones) over the rows that 00005's `Up` rebuilt, and removes the copy | removes the two `CHECK`s and the function; does not touch data |
| `00005_references_and_reversals` | **writer exclusion** (the same `LOCK TABLE` as 00004) and, with them excluded: a transaction committed as `PENDING`; a reversal/`WIN` with a `PROCESSED` reference without the internal reference; an internal reference inconsistent with the external one, the scope, the kind, the state or the amount; a target with more than one `PROCESSED` reversal; a `ROLLBACK` entry in the wrong direction (count and first ids) | everything listed in §10 from the "Consistent reference" row onwards, plus the `pending_references` schedule | removes the guarantees, restores 00001/00003's functions and `DROP`s the schedule (transactions and ledger intact). **The migrator preserves the schedule before this rollback and 00006 restores it on reapplication** — see "Rollback and the pending-operations schedule" below |
| `00004_revalidate_history_under_writer_exclusion` | **writer exclusion** (`LOCK TABLE` on `wallets`, `wager_transactions`, `wallet_ledger_entries`) and, with them excluded: guarantee triggers missing or disabled; a broken ledger chain; an entry inconsistent with the transaction; balance ≠ ledger; a completed transaction without an entry; result ≠ `balance_after` (with count and first ids) | nothing (it only validates; it was the final version expected by the application when it was introduced) | empty: it only moves the version back, without touching data or guarantees |
| `00003_completed_movement_requires_ledger` | a `PROCESSED` transaction (≠ `LOSS`) **without an entry**; `result_balance` ≠ the entry's `balance_after` | the transaction's deferred trigger and the result check on the ledger side | removes the trigger and restores 00002's function; **does not touch data** (after the rollback the gap exists again) |
| `00002_ledger_chain_and_result_integrity` | a broken ledger chain; an entry inconsistent with its transaction; `PROCESSED`/`REJECTED` with a null result; an invalid `failure_code`; balance ≠ ledger | continuous chain, composite FK, explicit result | removes what it installed |

Mechanism of 00003 (why a deferred *constraint trigger*): a `CHECK` cannot see another table; an FK from the transaction to the
ledger would be circular with the ledger's FK to the transaction; an immediate trigger would require the transaction to already be completed and the entry already inserted at the instant of each
command. The deferred one is evaluated at COMMIT over the final state (the order imposed by the immediate FKs — transaction before entry — does not change), is read-only (it locks nothing, does not affect per-wallet concurrency) and looks up the entry
through the unique index `(wallet_id, transaction_id)`; the `WHEN` limits the cost to `PROCESSED` transactions with a movement.

Note: the header comment of `00003` (already published, preserved as it is) says that the transaction and the entry "may be inserted in any order"; the
wording of this section prevails (transaction before the entry; completion possible after it; final deferred check). Tested in `TestInsertionOrderOfTransactionAndLedgerEntry`.

Tested (`migration_test.go`, `migration_movement_test.go`): valid history is preserved (including on rollback); history with a
broken chain, a null result, a completed transaction without an entry or a divergent result is refused, with the schema at the
previous version and the records intact; apply → revert → reapply works.

Limitations: `LOSS` has no entry, so its `result_balance` (the observed balance) cannot be verified against the ledger; the direction of the
`ROLLBACK` entry is enforced since 00005 (opposite to the target's, §10); the schema owner and
superusers can still disable triggers (the application never runs with them).

#### Rollback and the pending-operations schedule (findings of the audit and re-audit of increment 2)

The schedule is durable data (start, expiry, next attempt, attempts, diagnostic, completed schedules). 00005's published `Down`
drops the table and its `Up` rebuilds rows with `now() + 15 min` — which would renew an expired deadline and reset attempts. Documenting that
loss does **not** fix it; since 00005 cannot be rewritten, the fix is composite and compatible with the history. The procedure belongs to the migrator
(`Migrator`, the interfaces of `cmd/migrate up|down|down-to`) and **preserves or refuses before any loss**:

* **Coordination.** Every migrator operation — `Up`, `UpTo`, `Down`, `DownTo`, `UpAndProvision` and `ProvisionRuntime` (`migrate grants`) — runs under its own advisory lock (`MigratorLockID`),
  held on a dedicated connection, and only then does the provider acquire goose's lock (another key, always afterwards: the order is single and there is no deadlock; provisioning uses its own connection and never the
  migrator's lock). The lock is acquired **by attempts** (`pg_try_advisory_lock` every 100 ms), not by a blocking `pg_advisory_lock`: a session blocked in a lock wait only notices the
  client's departure when it wakes up, and a cancelled or killed command would leave an orphan session in the queue that would acquire the lock later. This way the wait ends immediately on cancellation and does not leak.
  The version decision, preparation, rollback, restoration and granting of grants do not interleave with another migrator's; `migrate up` with `RUNTIME_DB_ROLE` performs the `Up` and the provisioning **in the
  same region**.
* **Rollback preparation**, when reverting 00005 (by `Down` at version 5 or `DownTo(N)`, `0 < N < 5`), in a transaction under writer exclusion (`LOCK TABLE wallets,
  wager_transactions, pending_references IN SHARE ROW EXCLUSIVE MODE`): (a) reads the privileges on the schedule **at table and column level** (`pg_class.relacl` and
  `pg_attribute.attacl`, including `PUBLIC`) and revokes them from every role that is not the owner (the table-level `REVOKE` also removes the column-level ones); (b) **verifies in the catalog**:
  any login role — other than superusers, the owner and the migrator's session — that still has write access, directly, by inheritance or by being a member (`pg_has_role(..., 'MEMBER')`) of a
  role with write access (e.g. `pg_write_all_data`), **refuses** the preparation with the names and the transaction is rolled back (nothing changes); (c) writes `pending_references_preserved`
  (a full copy, with no `PUBLIC` access), marked, together with the live table, by a token in a `COMMENT`.
* **The original copy is never replaced by a rebuilt state.** With a copy already present: if the live schedule is the table that was copied (same token, or identical content) — an
  earlier `down` failed or was cancelled after the preparation —, the copy is refreshed from the live schedule, which also contains what the runtime committed after the grants were given
  back; if the live schedule is the rebuild from 00005's `Up` (partial reapplication, `DownTo(4) → UpTo(5) → Down`), the **original copy is kept** and the preparation only proceeds if the
  live schedule is the intact rebuild (every row exists in the copy, with no attempts, diagnostic or completion); otherwise the rollback is **refused** with count and first
  ids, without changing version, schedule, copy or privileges. The API refuses to start on the intermediate schema, so this refusal only happens through manual writes (owner/superuser).
* **Giving the privileges back**: if the rollback fails or is cancelled after the preparation (including while waiting for goose's lock), the migrator gives back the captured privileges when the
  schedule still exists (its own context, not the cancelled one); if the process dies first, `migrate grants` gives them back — after the earlier operation has finished: while a migrator operation
  is in progress (e.g. a prepared `Down`) the command **waits** for the lock, and when cancelled (SIGINT) it finishes without leaking anything. The original schedule remains intact.
* **`Up`/`UpTo` with an existing copy** (`settleScheduleCopyBeforeUp`, at version 5, before 00006 restores), under the same writer exclusion: if the live schedule is **the same table that was
  copied** (a prepared `Down` was cancelled or failed before dropping it), it contains everything — including what the runtime committed after the grants were given back — and the stale copy is
  **discarded** instead of being restored over it; if the live schedule is 00005's rebuild and the copy is the original, the restoration is the expected path as long as the rebuild is intact; if not,
  the `Up` is **refused** before any loss (count and ids; nothing changes). An old snapshot is never applied silently.
* **Exclusion of the whole decision → restoration window.** When the restoration is going to happen (intact rebuild, original copy), the same transaction as the decision **revokes write access** to the schedule from every role
  that is not the owner and verifies the effect in the catalog (the same exclusion as the rollback; if some writer cannot be excluded, the `Up` is refused), and the privileges are only given back after 00006
  has restored (or failed) — including for a runtime that had already recovered access through `migrate grants`. A real runtime resumption in that window is denied loudly instead of being committed
  and then overwritten. An `Up` that would apply 00005 and 00006 at once stops at 00005 to make that decision about the rebuild that has already been created. If the process dies inside the window, `migrate grants` gives access back.
* **Restoration** (00006, on reapplication): replaces the rows rebuilt by 00005's `Up` with the original values, including the completed ones,
  disabling the immutability trigger only during the restoration (owner, under writer exclusion), fires the deferred checks and removes the copy.
  A copy without a matching transaction is refused with count and ids (preflight), not discarded; a restoration that fails leaves the copy (the migration is transactional).
* **Destructive reset ≠ operational rollback**: `down-to 0` deletes everything, as always, and also discards the copy; `down`/`down-to N>0` preserve it.
* Limits: the exclusion is by privilege — the owner and superusers (the operator) are not excluded, and a `SECURITY DEFINER` view/function of another owner that writes to the schedule is not detected;
  00005's `Down` run directly through goose is not protected; data that never had a schedule (a `PENDING_REFERENCE` created by SQL before 00005) receives 00005's rebuilt schedule.
* Tests: `audit_regressions_test.go` (a cycle preserving an expired deadline, attempts, diagnostic and completed schedules; the window observed with the `DROP` held by an open read; a database still at version 5;
  `down-to 0`; an orphan copy) and `reaudit_regressions_test.go` (partial reapplication; a cancelled `Down` with goose's lock held; repetition; refusal due to data loss and to a non-excludable writer, also
  through the binary; column-level grants, `PUBLIC` and inheritance; concurrency between migrators).

### 10.3. Upgrade with active writers: the window between validating and installing, and how it was closed

**The problem (an audit finding).** A migration that validates the history and then installs a guarantee (00002: ledger chain;
00003: mandatory movement) does not see the commit of a writer from the previous schema that has already inserted rows and keeps its transaction
open. Installing the trigger waits for that writer, it commits under the **old** guarantees and the migration finishes successfully:
a new schema, the trigger installed and incompatible history persisted. The atomicity of a migration's SQL transaction does not include
the commits of other connections, and goose's session lock only coordinates concurrent **migrations**, not the application's writers.

**Choice: writer exclusion by the database, in a versioned migration** (`00004`), and not a maintenance window as the only
mechanism. Rationale:

* A maintenance window is only "verifiable" if something guarantees that **all** writers have stopped. The migrator does not control other
  processes or other roles; a check on `pg_stat_activity` is a snapshot (a connection may arrive right afterwards), and preventing
  connections would require altering cluster roles, a disproportionate side effect for a migration.
* `LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE` uses what the writers have in common — the tables they write. It conflicts with
  `INSERT`/`UPDATE`/`DELETE` (ROW EXCLUSIVE), so the migration **waits** for the writer that has already written (and only then reads: under READ
  COMMITTED, the next command sees its commit), **holds back** those that arrive until the migration's `COMMIT` and does not block reads
  or the use case's `SELECT ... FOR UPDATE`. Validating and completing become indivisible for the writers, without their cooperation
  and for any role. The lock order (wallet, transaction, entry) is the use case's, to avoid forming wait cycles.
* With the writers excluded, `00004` re-reads the **whole** history (not only what 00003 validated, since 00002 has the same shape) and
  checks that the guarantee triggers exist and are enabled. Writers that **do not** go through the triggers
  (`session_replication_role = replica`, a restore with triggers disabled, the schema owner) are only detected by this
  validation — which is why it must happen under exclusion (tested).
* 00003 has already been published and is not rewritten; its window is handled **after the fact**: 00004 detects and refuses the
  incompatible history, including on databases where the window has already occurred (and in the 2→4 upgrade with an open writer, which leaves the schema at 3).

**Final-version gate.** An upgrade only ends well at the final version (today 8). If a migration refuses (e.g., 00004 or 00005), the schema stays at the previous version, **nothing is deleted
or rewritten**, and the application (which compares the schema version with the one embedded in the binary) **refuses to start**: the state is visible and
is not treated as a success. Fixing the records is manual; afterwards, `migrate up` completes.

**Procedure** (the README §Migrations repeats the commands):

1. *Recommended:* stop the API instances (all writers) before `migrate up`: it reduces the time during which writers are
   held back and avoids lock conflicts. It is not necessary for the fix to work against runtime-role writers (the guarantees above
   hold with them active), but it is the precondition for **any other** writer (manual maintenance, ETL, another role) that the database
   cannot exclude by itself.
2. Run `migrate up` with the schema owner role; check `migrate status` (final version) and start the new API.
3. If 00004 aborts: read the count and the ids, fix the records manually (no tool changes them) and repeat `migrate up`.

**Limits.**

* `lock_timeout` of 30 s: a writer with a transaction open for longer makes 00004 fail **with no effect** (the held-back ones are
  released); repeat in a window with the writers stopped. Without a limit, new writers would be queued indefinitely.
* While waiting, new writers are held back (latency), and a writer that locks the tables in an order different from the use case's
  may suffer `deadlock detected` (40P01) instead of the migration; it is safe to repeat.
* Between 00003 and 00004 there is, for moments, schema 3 (with 00003's guarantees). A schema-2 writer that commits
  during 00003 may leave incompatible history; that is exactly what 00004 detects.
* The database does not prevent a writer with superuser or owner privilege from disabling triggers after the upgrade (§10.1); the
  validation only holds for the instant of the migration.
* An online upgrade with no downtime is **not** a goal (the challenge statement does not require it); the exclusion makes the upgrade correct, not invisible.

Tests (`upgrade_window_test.go`): the audit probe (an old open writer → upgrade → commit → final version and guarantees),
a valid concurrent writer, already invalid history (5 shapes), a missing or disabled guarantee, a writer that ignores triggers,
wait/hold on the lock, data preserved, rollback/reapplication and a startup refused below the final version.

## 11. Fx composition, lifecycle and shutdown (§4)

* `bootstrap.Options(cfg)` = `fx.Supply(cfg)` + `fx.Module`s: `logging`, `persistence` (pool), `auth` (verifier), `metrics` (the registry and the on-demand collectors),
  `messaging` (SQS client), `outbox` (when `OUTBOX_PUBLISHERS` > 0), `consumer` (when `SQS_CONSUMERS` > 0), `application` (service) and `http` (server). Constructors through `fx.Provide`; wiring of
  hooks through `fx.Invoke`/`fx.Lifecycle`. Fx's logs go out through JSON `slog`.
* **Startup** (`OnStart`): `postgres.Database` — the pool is only opened in `Start`, which does a `Ping`, checks the schema version through a
  `SELECT` on `goose_db_version` (a pending `migrate up` prevents startup) and **closes the pool before returning the error if anything
  fails**: Fx does not call the `OnStop` of a hook whose `OnStart` failed, so releasing does not depend on it. Verifier →
  loads the JWKS; SQS → `GetQueueUrl` of the input queue (and of the DLQ when configured); SNS → checks the topic when the publisher is enabled. Last, the **coordinator** starts the components that admit work, in
  registration order: the reference worker, the **outbox publisher** (when `OUTBOX_PUBLISHERS` > 0), the **SQS consumer** (when `SQS_CONSUMERS` > 0) and, lastly, the HTTP server (`Listen` synchronous, `Serve` in the background) — the entry
  port opens last. If a component fails to start, the coordinator stops the ones already started before returning the error, and Fx undoes the earlier hooks:
  **no PostgreSQL connection remains open** (tested by counting `pg_stat_activity`, including with the worker active and the port
  occupied): PostgreSQL unreachable, schema without migrations or outdated, invalid credentials, IdP unreachable, missing queue, unreachable
  broker and occupied port. The `OnStart` context only bounds initialization: the worker has its own context, cancelable by `Cancel`.
* **Coordinated shutdown** (`internal/lifecycle`): a single global deadline (`SHUTDOWN_TIMEOUT`, 20 s) with a **drain of at most 12 s**
  (`SHUTDOWN_DRAIN_TIMEOUT`) and a **shared reserve of at least 8 s** — not granted again to each component — for
  cancellation, cleanup and closing the dependencies. Sequence: (1) **admission closed on all components at the same time**: the server
  closes the listener and answers `503 SHUTTING_DOWN` to new requests (including on keep-alive connections), the worker stops fetching work, the publisher makes no new reservation and the consumer cancels its searches and dispatches nothing new;
  (2) **simultaneous drain** of the admitted work (including the publisher's publication and confirmation in progress and the consumer's handling already admitted); (3) at the drain limit, **cancellation** of what remains (it closes connections and cancels the
  requests' context; it cancels the worker's attempt, whose transaction is rolled back; it cancels the publication in progress, whose reservation is **released** — the final SQL action uses its own context, limited by `OUTBOX_SQL_TIMEOUT` **and** by the remaining global deadline — with no remaining budget no new SQL starts and the lease expires — and `Done` only closes after it; it cancels the consumer's handling, whose message is released for redelivery, with the final actions on the broker bounded by the earliest absolute instant between their own limit and the global deadline); (4) **observable confirmation of termination** of each
  component (`Done`): the server only confirms when the `http.Server` has finished **and** all handlers have returned — the end of `Serve`, by itself,
  does not prove that, and cancelling a context does not prove the end of a goroutine; (5) only then does Fx run the dependencies' `OnStop` in inverse
  order (verifier, SQS, pool). If a component does not confirm by the global deadline minus a 1 s margin, the shutdown is recorded
  as **incomplete** (error and log) and recovery is left to the durable state; normal termination is not declared without confirmation. Closing the
  pool never holds the shutdown beyond the deadline (`pgxpool.Close` waits for connections to be returned). The *rollback* of a cancelled transaction uses its own
  5 s deadline which, during shutdown, **respects the remaining budget** (`Store.SetShutdownDeadline`). `docker compose` uses
  `stop_grace_period: 25s` so as not to send `SIGKILL` before the end of the 20 s deadline. `SIGTERM` with exit 0 tested in 3 processes; `SIGKILL`
  does not depend on a hook: the accepted pending operation and the committed movement recover through the durable state (tested).
  Note: `net/http` only treats new connections without a request as idle after 5 s, so a client that leaves
  connections open may extend the drain by up to 5 s (within the deadline).
* Configuration validated at composition (`Config.Validate`, also for directly built configurations): `SHUTDOWN_DRAIN_TIMEOUT` shorter than
  `SHUTDOWN_TIMEOUT`; `REFERENCE_BACKOFF_INITIAL` ≤ `REFERENCE_BACKOFF_MAX` and ≤ `REFERENCE_TTL`; `REFERENCE_PHASE_TIMEOUT` shorter than the
  shutdown deadline; all deadlines positive; `REFERENCE_WORKERS` ∈ {0, 1}. For the outbox publisher (when enabled): the lease must exceed publication + 2×SQL by at least 5 s, the publication must be shorter than the drain,
  the SQL timeout shorter than the shutdown reserve, backoff initial ≤ maximum, and the topic ARN and credentials present. For the SQS consumer (when enabled): phase + 3×action shorter than the queue's 60 s visibility, the phase shorter than the shutdown deadline, the action shorter than the shutdown reserve,
  backoff initial ≤ maximum, and a stable consumer name and a DLQ name present. Initial pool: 10 connections per process.

## 12. Observability

* JSON logs (`slog`): requests with `method`, `route`, `status`, `durationMs`, `correlationId`, `clientId`, `providerId`;
  use cases with `transactionId`, `walletId`, `providerId`, `status`, `failureCode`, `replay`. `X-Correlation-Id`
  (validated or generated) is echoed, propagated to the event and to the outbox. Tokens, bodies and financial values are not logged
  beyond what diagnosis needs.
* Health: `/health/live` (process) and `/health/ready` (PostgreSQL `Ping` + SQS `GetQueueUrl`, in parallel, 2 s deadline;
  503 if either fails). The IdP is not part of readiness (the JWKS stays cached; its failure shows up as 401).
* **Metrics (increment 5)**: `GET /metrics` (`internal-service` only), Prometheus-compatible text, process-LOCAL counters observed after the commit through `app.Observer`
  (results by origin/kind/status/replay; inbox new/duplicate/conflict; retries per component; confirmed DLQ sends; concurrency conflicts; REAL reconciliation divergence through
  `WithReconciliationObserver`), latency as sum and count, and gauges collected ON DEMAND with a deadline (backlog, reserved events and age of the outbox; the DLQ's approximate depth with
  `GetQueueAttributes` on the DLQ only). A collection failure omits the samples and marks `wager_collector_up` = 0, never a fictitious zero. Bounded labels, no ids; it is not financial accounting.
  No tracing or dashboards. The reference worker logs (`transactionId`, `walletId`, `providerId`,
  `kind`, `status`, `failureCode`) every pending operation completed and every aborted attempt.

## 13. Adopted interpretations (summary)

1. An amount with 1–2 decimal places is accepted and normalized (§2).
2. Idempotency key scoped per provider; the same external operation with another key → always 409.
3. Nonexistent wallet / mismatched player or currency: an input error (404/422), **not** persisted (there is no valid `wallet_id`
   for an FK; it is deterministic and correctable).
4. `WALLET_ALREADY_EXISTS` returns the existing `walletId`: `POST /wallets` has no idempotency key in the
   challenge statement, so this is the recovery path after an uncertain-commit error.
5. Operations that depend on a reference that is still unavailable answer `202` (`PENDING_REFERENCE`, no balance, with durable resumption, §8); `200`/`422` when the transaction is written already terminal.
6. A replay of `REJECTED` answers `422` with `idempotentReplay:true` (the same status as the original).
7. The internal service may read any transaction; a provider only its own.
8. `GET …/ledger` paginates by `seq` (an opaque, versioned cursor) and `POST …/reconciliation` reconciles in a `READ ONLY, REPEATABLE READ` snapshot; both for the internal service only (§15).
9. Reversals: one successful reversal per target movement across the whole history (§8); the references' TTL counts from the first persistence and
   is not renewed; a `PROCESSED` reference resolves even after the TTL; another provider's reference is never consulted.

## 14. Limitations and work not completed

See "Scope and open items" in the README. In short, **outside the scope delivered**: fuzzing, load testing, tracing, dashboards, double-entry bookkeeping and lease/visibility renewal. Broker access
control is **proven as emulated authorization** (§9), with the limit of not validating SigV4. The three-process proof **exists** for
HTTP (bets, WIN/LOSS), reversals and pending operations with the real worker and, in increment 4, for SQS + outbox (`sqs_multiprocess_test.go`, children compiled with `-race`).

Limits of the current guarantees (details in the indicated sections): the schema owner and superusers can disable triggers — the application
never runs with them (§10.1); `LOSS`'s `result_balance` cannot be verified against the ledger (§10.2); a clock regression between instances
is refused by the domain and handled by whoever calls the transition (§3, §8); IdP key rotation may cause 401 for up to 30 s (§9). From the
reversal policy (§8): the BET's `ROLLBACK` after the REFUND's `ROLLBACK` is refused (a contract choice, not one of financial viability). From the pending operations: the TTL and the backoff use the application's clock (the difference between instances shifts the deadline, never renews it); a
wallet that is continuously busy or has a localized failure only delays the resumption of its own pending operations (the query skips it with the same `FOR UPDATE SKIP LOCKED` as the attempt, the failure defers the schedule with backoff and the scan moves on to the others); the worker has no lease (exclusion is by the locks
and by revalidation). From the tests: the real-process harness compiles the API and migration binaries **with `-race`** and fails a test on a race, `panic` or `fatal error` in a child's output (an adaptation delivered in the final consolidation; the parent's `go test -race` does not instrument separate binaries, which is why the children are built that way). The schedule's protection on rollback is in the migrator (`Migrator`), not in 00005's published SQL: 00005's `Down` run directly through goose, without the migrator, does not preserve the schedule; writer exclusion is by privilege (the owner and superusers are not excluded).

## 15. Reads: paginated ledger and reconciliation

Ports and layers. `app.LedgerReader` (implemented by `postgres.Store`) is a **read** port of its own: `LedgerPage` and `ReconciliationSnapshot`. The financial `UnitOfWork`
does not change (it remains `READ COMMITTED` with the wallet's row lock). `ListLedger` uses `Reader()` only for the wallet's **existence** (404) and reads the page through the dedicated port; the
  **reconciliation uses neither `Reader()` nor the pool between queries**: the balance and the sum come from the same `READ ONLY, REPEATABLE READ` `pgx.Tx`. The use case (`Service.ListLedger`,
`Service.Reconcile`) validates and converts; HTTP only authenticates (`internal-service`) and translates parameters and errors. The domain does not know pgx.

* **Ledger by position.** `SELECT … WHERE wallet_id = $1 AND seq > $2 ORDER BY seq LIMIT $3` over `ix_ledger_wallet_seq (wallet_id, seq)`: no `OFFSET`, a cost
  independent of the page depth. The continuity assumption — within a wallet, `seq` grows in commit order — holds because every entry is inserted
  under the wallet's `FOR UPDATE`, which only ends at the commit, **and** because the database has its own guard against an old `seq`: the identity may be allocated before the lock that the
  `ledger_enforce_chain` trigger takes on the wallet, but an INSERT whose position fell behind a committed one is refused (`40001`) instead of entering out of order (proven by two real SQL writers); a sequence alone would not prove monotonicity (tested with parallel writers on the same wallet: no traversal skips or repeats an item). `seq` is internal and is not exposed.
* **Cursor.** `base64url({"v":1,"w":walletId,"s":seq})`, accepted only in the canonical form the server produces; see the contract in the README. It is not signed: a signature would not be
  the authorization boundary (the server already authorizes and bounds the scope through the route), and a tampered cursor can only point to another position **in the same wallet**.
* **Reconciliation snapshot.** A `READ ONLY, REPEATABLE READ` transaction (10 s deadline): the first query (the wallet's balance, without a lock) fixes the snapshot and the second
  (`count`, the `NUMERIC` sum of credits − debits and the count of mismatched currencies) sees it. With `READ COMMITTED` the two could see different moments and invent a divergence; the test with a barrier
  commits a movement **between** the queries and shows that the response is still from a single moment and that the independent writer does not wait (the read holds no lock).
* **Exact sum.** PostgreSQL's `sum()` over `bigint` returns `NUMERIC`: there is no intermediate overflow. The result's text is read as a `big.Int` and converted to `Money`
  with a check (`IsInt64`, and `MinInt64` is refused). The difference is also computed in `big.Int`. Out of range → `ErrReconciliationFailed` (500), never a success.
* **Divergence**: a response + an `ERROR` log + `app.WithReconciliationObserver` (wired to `wager_reconciliation_divergences_total`); nothing is repaired.
* **Tests**: `internal/app/ledger_test.go` (cursor and arithmetic, unit) and `test/integration/ledger_reads_test.go`, `reconciliation_test.go` (real PostgreSQL, Keycloak and MiniStack;
  divergence and an out-of-range sum only in exclusive fixtures prepared by the owner, never as a feature).
* **Limits**: no frozen snapshot of the ledger navigation (documented); the reconciliation deadline (10 s) is not configurable per environment.
