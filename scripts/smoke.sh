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
echo "smoke OK"
