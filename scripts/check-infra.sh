#!/usr/bin/env bash
# Verifica o ambiente local provisionado pelo Docker Compose: tokens reais do Keycloak com as
# claims esperadas e filas/redrive/tópico no LocalStack. Sai com código != 0 na primeira falha.
#
#   docker compose up -d --wait postgres keycloak localstack && scripts/check-infra.sh
#
# Variáveis: KEYCLOAK_URL (padrão http://localhost:8081), COMPOSE (padrão "docker compose").
set -euo pipefail

KC="${KEYCLOAK_URL:-http://localhost:8081}"
COMPOSE="${COMPOSE:-docker compose}"
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok:   $*"; }

claims_of() { # imprime as claims (JSON) do access token recebido em $1
  local payload
  payload=$(cut -d. -f2 <<<"$1" | tr '_-' '/+')
  while [ $(( ${#payload} % 4 )) -ne 0 ]; do payload="$payload="; done
  base64 -d <<<"$payload"
}

token() { # client_id secret
  curl -sf -X POST "$KC/realms/wager/protocol/openid-connect/token" \
    -d grant_type=client_credentials -d client_id="$1" -d client_secret="$2" | jq -er .access_token
}

check_token() { # client secret jq-expression description
  local tok claims
  tok=$(token "$1" "$2") || fail "no token for $1"
  claims=$(claims_of "$tok")
  jq -e --arg kc "$KC" "$3" <<<"$claims" >/dev/null || fail "$1: $4 (claims: $claims)"
  ok "$1: $4"
}

check_token internal-service internal-service-dev-secret \
  '.iss == ($kc + "/realms/wager") and (.aud | tostring | contains("wager-api")) and (.realm_access.roles | index("internal-service"))' \
  "issuer, audience wager-api and role internal-service"
check_token provider-a provider-a-dev-secret \
  '.provider_id == "provider-a" and (.realm_access.roles | index("provider"))' "provider_id=provider-a and role provider"
check_token provider-b provider-b-dev-secret \
  '.provider_id == "provider-b" and (.realm_access.roles | index("provider"))' "provider_id=provider-b and role provider"
check_token no-roles-client no-roles-dev-secret \
  '(.realm_access.roles // []) | (index("provider") or index("internal-service")) | not' "authenticated without business roles"
check_token wrong-audience-client wrong-audience-dev-secret \
  '(.aud // "" | tostring | contains("wager-api")) | not' "token without the API audience"

if curl -s -o /dev/null -w '%{http_code}' -X POST "$KC/realms/wager/protocol/openid-connect/token" \
     -d grant_type=client_credentials -d client_id=provider-a -d client_secret=wrong | grep -q '^401$'; then
  ok "wrong client secret is refused"
else
  fail "wrong client secret was not refused"
fi

awslocal() { $COMPOSE exec -T localstack awslocal "$@"; }
queues=$(awslocal sqs list-queues --query 'QueueUrls' --output text)
for q in wager-transactions.fifo wager-transactions-dlq.fifo wager-events-audit.fifo; do
  grep -q "$q" <<<"$queues" || fail "queue $q not provisioned"
  ok "queue $q"
done
url=$(awslocal sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text)
attrs=$(awslocal sqs get-queue-attributes --queue-url "$url" --attribute-names All)
jq -e '.Attributes.FifoQueue == "true" and .Attributes.VisibilityTimeout == "60"' <<<"$attrs" >/dev/null || fail "main queue attributes"
jq -er '.Attributes.RedrivePolicy | fromjson | (.deadLetterTargetArn | endswith(":wager-transactions-dlq.fifo")) and .maxReceiveCount == "5"' <<<"$attrs" >/dev/null \
  || fail "redrive policy to the DLQ after 5 receives is missing"
ok "redrive: wager-transactions.fifo -> wager-transactions-dlq.fifo after 5 receives"
topic=$(awslocal sns list-topics --query 'Topics[].TopicArn' --output text)
grep -q 'wager-events.fifo' <<<"$topic" || fail "SNS topic wager-events.fifo not provisioned"
awslocal sns list-subscriptions --query 'Subscriptions[].Endpoint' --output text | grep -q 'wager-events-audit.fifo' \
  || fail "event topic has no subscription to wager-events-audit.fifo"
ok "events destination: SNS wager-events.fifo -> SQS wager-events-audit.fifo"
echo "infrastructure OK"
