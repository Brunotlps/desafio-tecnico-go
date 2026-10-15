#!/usr/bin/env bash
# Teste de fumaça ponta a ponta contra a API em execução (Docker Compose): autenticação real,
# abertura de carteira, BET aceita/rejeitada, replay, conflito, isolamento entre provedores, reversões
# (REFUND/ROLLBACK, uma por alvo), uma referência pendente concluída pelo worker de referências e a publicação da outbox no
# tópico de eventos (a fila de auditoria assinante recebe).
#
#   docker compose up --build -d && scripts/smoke.sh
#
# Variáveis: API_URL (http://localhost:8080), KEYCLOAK_URL (http://localhost:8081), COMPOSE ("docker compose"; usado para
# consultar a outbox e a fila de auditoria no stack).
set -euo pipefail

API="${API_URL:-http://localhost:8080}"
KC="${KEYCLOAK_URL:-http://localhost:8081}"
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok:   $*"; }

token() {
  curl -sf -X POST "$KC/realms/wager/protocol/openid-connect/token" \
    -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" | jq -er .access_token
}
# req METHOD PATH TOKEN [IDEMPOTENCY_KEY] [BODY] -> imprime "<status> <corpo>"
req() {
  local args=(-s -o /tmp/smoke_body.$$ -w '%{http_code}' -X "$1" "$API$2")
  [ -n "${3:-}" ] && args+=(-H "Authorization: Bearer $3")
  [ -n "${4:-}" ] && args+=(-H "Idempotency-Key: $4")
  [ -n "${5:-}" ] && args+=(-d "$5")
  local code; code=$(curl "${args[@]}")
  echo "$code $(cat /tmp/smoke_body.$$)"; rm -f /tmp/smoke_body.$$
}
expect() { # descrição, resposta, status esperado, jq-expressão opcional sobre o corpo
  local code body; code=${2%% *}; body=${2#* }
  [ "$code" = "$3" ] || fail "$1: HTTP $code (want $3): $body"
  if [ -n "${4:-}" ]; then jq -e "$4" <<<"$body" >/dev/null || fail "$1: unexpected body: $body"; fi
  ok "$1"
}

INTERNAL=$(token internal-service internal-service-dev-secret)
PA=$(token provider-a provider-a-dev-secret)
PB=$(token provider-b provider-b-dev-secret)
PLAYER=$(cat /proc/sys/kernel/random/uuid)
RUN=$(cat /proc/sys/kernel/random/uuid | cut -c1-8)

expect "liveness" "$(req GET /health/live)" 200
expect "readiness (postgres + sqs)" "$(req GET /health/ready)" 200 '.checks.postgres == "ok" and .checks.sqs == "ok"'
expect "no token" "$(req POST /wallets '' '' '{}')" 401
expect "provider cannot open wallets" "$(req POST /wallets "$PA" '' '{}')" 403

W=$(req POST /wallets "$INTERNAL" '' "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}")
expect "open wallet" "$W" 201 '.balance.amount == "100.00" and .version == 1'
WID=$(jq -er .id <<<"${W#* }")
expect "duplicate wallet" "$(req POST /wallets "$INTERNAL" '' "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1.00\",\"currency\":\"BRL\"}}")" 409 '.code == "WALLET_ALREADY_EXISTS"'

bet() { # chave id-externo valor [token]
  req POST /wagering/transactions "${4:-$PA}" "$1" "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$2\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WID\",\"roundId\":\"r\",\"gameId\":\"g\",\"kind\":\"BET\",\"money\":{\"amount\":\"$3\",\"currency\":\"BRL\"}}"
}
R1=$(bet "k1-$RUN" "t1-$RUN" 80.00)
expect "BET 80.00 processed" "$R1" 200 '.status == "PROCESSED" and .balance.amount == "20.00" and .idempotentReplay == false'
expect "replay returns the original result" "$(bet "k1-$RUN" "t1-$RUN" 80.00)" 200 '.idempotentReplay == true and .balance.amount == "20.00"'
expect "second BET 80.00 rejected" "$(bet "k2-$RUN" "t2-$RUN" 80.00)" 422 '.status == "REJECTED" and .failureCode == "INSUFFICIENT_FUNDS"'
expect "same key, different payload" "$(bet "k1-$RUN" "t1-$RUN" 10.00)" 409 '.code == "IDEMPOTENCY_KEY_REUSED"'
expect "same external id, another key" "$(bet "k9-$RUN" "t1-$RUN" 80.00)" 409 '.code == "EXTERNAL_TRANSACTION_ALREADY_REGISTERED"'
expect "provider-b cannot send as provider-a" "$(bet "kb-$RUN" "tb-$RUN" 1.00 "$PB")" 403
expect "wallet balance" "$(req GET "/wallets/$WID" "$INTERNAL")" 200 '.balance.amount == "20.00" and .version == 2'

TXID=$(jq -er .transactionId <<<"${R1#* }")
expect "provider-a reads its transaction" "$(req GET "/wagering/transactions/$TXID" "$PA")" 200 '.status == "PROCESSED"'
expect "provider-b does not see it" "$(req GET "/wagering/transactions/$TXID" "$PB")" 404
expect "provider-b cannot query provider-a's namespace" "$(req GET "/providers/provider-a/wagering/transactions/t1-$RUN" "$PB")" 403

# --- Reversões e referências pendentes -------------------------------------------------------------
op() { # chave id-externo tipo valor referência [token]
  req POST /wagering/transactions "${6:-$PA}" "$1" "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$2\",\"playerId\":\"$PLAYER\",\"walletId\":\"$WID\",\"roundId\":\"r\",\"gameId\":\"g\",\"kind\":\"$3\",\"money\":{\"amount\":\"$4\",\"currency\":\"BRL\"},\"referenceExternalTransactionId\":\"$5\"}"
}
expect "REFUND of the 80.00 BET" "$(op "kr-$RUN" "tr-$RUN" REFUND 80.00 "t1-$RUN")" 200 '.status == "PROCESSED" and .balance.amount == "100.00"'
expect "a second reversal of the same BET is rejected" "$(op "kx-$RUN" "tx-$RUN" ROLLBACK 80.00 "t1-$RUN")" 422 '.failureCode == "ALREADY_REVERSED" and .balance.amount == "100.00"'
expect "reversal with another amount" "$(op "km-$RUN" "tm-$RUN" REFUND 79.00 "t1-$RUN")" 422 '.failureCode == "REFERENCE_MISMATCH"'
PEND=$(op "kp-$RUN" "tp-$RUN" REFUND 10.00 "late-$RUN")
expect "REFUND before its BET stays pending (202, no balance)" "$PEND" 202 '.status == "PENDING_REFERENCE" and (has("balance") | not)'
PID=$(jq -er .transactionId <<<"${PEND#* }")
expect "replay of the pending operation" "$(op "kp-$RUN" "tp-$RUN" REFUND 10.00 "late-$RUN")" 202 '.idempotentReplay == true and .transactionId == "'"$PID"'"'
expect "the referenced BET arrives" "$(bet "kl-$RUN" "late-$RUN" 10.00)" 200 '.balance.amount == "90.00"'
for _ in $(seq 1 40); do
  S=$(req GET "/wagering/transactions/$PID" "$PA")
  [ "$(jq -r .status <<<"${S#* }")" = "PROCESSED" ] && break
  sleep 1
done
expect "the worker resolved the pending REFUND" "$S" 200 '.status == "PROCESSED" and .balance.amount == "100.00"'
expect "final wallet balance" "$(req GET "/wallets/$WID" "$INTERNAL")" 200 '.balance.amount == "100.00"'
# --- Ledger paginado e reconciliação (somente serviço interno) ----------------------------------------
expect "provider cannot read the ledger" "$(req GET "/wallets/$WID/ledger" "$PA")" 403
expect "provider cannot reconcile" "$(req POST "/wallets/$WID/reconciliation" "$PA")" 403
P1=$(req GET "/wallets/$WID/ledger?limit=2" "$INTERNAL")
expect "ledger page 1 (limit 2)" "$P1" 200 '(.items | length) == 2 and .items[0].money.amount == "100.00" and .items[0].direction == "CREDIT" and (.nextCursor | type) == "string"'
C1=$(jq -er .nextCursor <<<"${P1#* }")
P2=$(req GET "/wallets/$WID/ledger?limit=2&cursor=$C1" "$INTERNAL")
expect "ledger page 2" "$P2" 200 '(.items | length) == 2 and (.nextCursor | type) == "string"'
C2=$(jq -er .nextCursor <<<"${P2#* }")
expect "ledger last page has no nextCursor" "$(req GET "/wallets/$WID/ledger?limit=2&cursor=$C2" "$INTERNAL")" 200 '(.items | length) == 1 and (has("nextCursor") | not)'
expect "invalid cursor" "$(req GET "/wallets/$WID/ledger?cursor=nope" "$INTERNAL")" 400 '.code == "INVALID_CURSOR"'
expect "malformed query is rejected" "$(req GET "/wallets/$WID/ledger?limit=%ZZ" "$INTERNAL")" 400 '.code == "INVALID_QUERY" and (has("items") | not)'
expect "limit above the maximum" "$(req GET "/wallets/$WID/ledger?limit=101" "$INTERNAL")" 400 '.code == "INVALID_LIMIT"'
expect "reconciliation is consistent" "$(req POST "/wallets/$WID/reconciliation" "$INTERNAL")" 200 \
  '.consistent == true and .storedBalance.amount == "100.00" and .calculatedBalance.amount == "100.00" and .difference.amount == "0.00" and .checkedEntries == 5'

# Publicador da outbox: os eventos de todo o fluxo acima são publicados no tópico e entregues à fila de auditoria assinante.
COMPOSE="${COMPOSE:-docker compose}"
unpublished() { $COMPOSE exec -T postgres psql -U wager -d wager -tAc "SELECT count(*) FROM outbox_events WHERE published_at IS NULL" | tr -d '[:space:]'; }
for _ in $(seq 1 40); do
  [ "$(unpublished)" = "0" ] && break
  sleep 1
done
[ "$(unpublished)" = "0" ] || fail "the outbox publisher left $(unpublished) event(s) unpublished"
ok "outbox drained: every event of the flow was published"
AUDIT=$($COMPOSE exec -T ministack awslocal sqs get-queue-url --queue-name wager-events-audit.fifo --query QueueUrl --output text | tr -d '[:space:]')
DELIVERED=$($COMPOSE exec -T ministack awslocal sqs get-queue-attributes --queue-url "$AUDIT" --attribute-names ApproximateNumberOfMessages --query Attributes.ApproximateNumberOfMessages --output text | tr -d '[:space:]')
[ "${DELIVERED:-0}" -gt 0 ] || fail "no event reached the subscriber of the event topic"
ok "events delivered to the subscribed audit queue ($DELIVERED)"

# Entrada por SQS: o produtor INTERNO (principal wager-producer) vincula a origem ao providerId; o consumidor trata com o mesmo núcleo
# do HTTP (inbox e finanças no mesmo commit) e o resultado fica consultável pela API.
envelope() { # providerId externalId
  jq -nc --arg m "msg-$RUN-$2" --arg pr "$1" --arg ext "$2" --arg w "$WID" --arg p "$PLAYER" \
    '{messageId:$m,type:"WagerTransactionRequested",occurredAt:"2026-09-08T12:00:00.000Z",data:{providerId:$pr,externalTransactionId:$ext,idempotencyKey:($pr+":"+$ext),playerId:$p,walletId:$w,roundId:"round-1",gameId:"fortune-chimp",kind:"BET",money:{amount:"10.00",currency:"BRL"}}}'
}
producer() { $COMPOSE run --rm -T -e PRODUCER_CLIENT_ID="$1" -e PRODUCER_CLIENT_SECRET="$2" sqs-producer; }
SQSEXT="sqs-$RUN"
producer provider-a provider-a-dev-secret <<<"$(envelope provider-a "$SQSEXT")" | grep -q "published msg-$RUN-$SQSEXT" || fail "the internal producer did not publish the envelope"
ok "internal producer published an envelope for provider-a"
for _ in $(seq 1 40); do
  S=$(req GET "/providers/provider-a/wagering/transactions/$SQSEXT" "$PA")
  [ "${S%% *}" = "200" ] && break
  sleep 1
done
expect "the SQS message was consumed (same core as HTTP)" "$S" 200 '.status == "PROCESSED" and .balance.amount == "90.00"'
expect "wallet balance after the SQS BET" "$(req GET "/wallets/$WID" "$INTERNAL")" 200 '.balance.amount == "90.00"'
if producer provider-a provider-a-dev-secret <<<"$(envelope provider-b "other-$RUN")" >/dev/null 2>&1; then
  fail "provider-a published an envelope for provider-b"
fi
ok "provider-a cannot publish as provider-b (refused before any send)"

# Métricas: somente o serviço interno; categorias com observações reais.
expect "metrics require authentication" "$(req GET /metrics)" 401
expect "providers cannot read the metrics" "$(req GET /metrics "$PA")" 403
M=$(curl -sf -H "Authorization: Bearer $INTERNAL" "$API/metrics") || fail "internal-service could not read /metrics"
for series in 'wager_transactions_total{origin="http"' 'wager_transactions_total{origin="sqs"' 'wager_inbox_total{outcome="new"}' wager_outbox_pending \
  wager_outbox_oldest_pending_age_seconds 'wager_processing_seconds_count{component="http_submit"}' 'wager_processing_seconds_count{component="sqs_message"}' \
  'wager_collector_up{source="dlq"} 1' 'wager_collector_up{source="outbox"} 1' wager_dlq_messages_approx wager_retries_total wager_reconciliation_divergences_total \
  wager_concurrency_conflicts_total wager_dlq_forwarded_total; do
  grep -qF "$series" <<<"$M" || fail "metric $series is missing"
done
ok "metrics: authorization and every category exposed"
echo "smoke OK"
