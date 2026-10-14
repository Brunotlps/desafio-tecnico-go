#!/usr/bin/env bash
# Verifica o ambiente local provisionado pelo Docker Compose: tokens reais do Keycloak com as
# claims esperadas e o broker (MiniStack com AUTH=true): filas, redrive, tópico, assinatura, política de
# entrega da fila de auditoria, políticas mínimas por responsabilidade e arquivo de credenciais. Sai com
# código != 0 na primeira falha. A prova comportamental (acessos permitidos e negados com as credenciais
# reais de cada principal) está em test/integration/broker_*_test.go.
#
#   docker compose up -d --wait postgres keycloak ministack && docker compose run --rm broker-provision \
#     && scripts/check-infra.sh
#
# Variáveis: KEYCLOAK_URL (padrão http://localhost:8081), COMPOSE (padrão "docker compose"),
# BROKER_CREDENTIALS_FILE (padrão .local/broker/credentials).
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

# O contêiner do MiniStack traz o awslocal (credenciais de provisionamento/raiz do emulador).
awslocal() { $COMPOSE exec -T ministack awslocal "$@"; }
queues=$(awslocal sqs list-queues --query 'QueueUrls' --output text)
for q in wager-transactions.fifo wager-transactions-dlq.fifo wager-events-audit.fifo; do
  grep -q "$q" <<<"$queues" || fail "queue $q not provisioned"
  ok "queue $q"
done
url=$(awslocal sqs get-queue-url --queue-name wager-transactions.fifo --query QueueUrl --output text)
attrs=$(awslocal sqs get-queue-attributes --queue-url "$url" --attribute-names All)
jq -e '.Attributes.FifoQueue == "true" and .Attributes.ContentBasedDeduplication == "false" and .Attributes.VisibilityTimeout == "60"' <<<"$attrs" >/dev/null || fail "main queue attributes"
jq -er '.Attributes.RedrivePolicy | fromjson | (.deadLetterTargetArn | endswith(":wager-transactions-dlq.fifo")) and .maxReceiveCount == "5"' <<<"$attrs" >/dev/null \
  || fail "redrive policy to the DLQ after 5 receives is missing"
ok "redrive: wager-transactions.fifo -> wager-transactions-dlq.fifo after 5 receives"
dlq=$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query QueueUrl --output text)
jq -e '.Attributes.MessageRetentionPeriod == "1209600"' <<<"$(awslocal sqs get-queue-attributes --queue-url "$dlq" --attribute-names All)" >/dev/null || fail "DLQ retention is not 14 days"
ok "DLQ retention: 14 days"
topic=$(awslocal sns list-topics --query 'Topics[].TopicArn' --output text)
grep -q 'wager-events.fifo' <<<"$topic" || fail "SNS topic wager-events.fifo not provisioned"
TOPIC_ARN=$(tr '\t' '\n' <<<"$topic" | grep 'wager-events.fifo$')
subs=$(awslocal sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN" --query 'Subscriptions[].Endpoint' --output text)
[ "$(wc -w <<<"$subs")" = "1" ] && grep -q 'wager-events-audit.fifo' <<<"$subs" || fail "the event topic must have exactly one subscription, to wager-events-audit.fifo (got: $subs)"
ok "events destination: SNS wager-events.fifo -> SQS wager-events-audit.fifo (one subscription)"
audit=$(awslocal sqs get-queue-url --queue-name wager-events-audit.fifo --query QueueUrl --output text)
apolicy=$(awslocal sqs get-queue-attributes --queue-url "$audit" --attribute-names Policy --query Attributes.Policy --output text)
jq -e --arg t "$TOPIC_ARN" '(.Statement | length) == 1 and .Statement[0].Effect == "Allow" and .Statement[0].Principal.Service == "sns.amazonaws.com" and .Statement[0].Action == ["sqs:SendMessage"] and .Statement[0].Condition.ArnEquals["aws:SourceArn"] == $t' <<<"$apolicy" >/dev/null \
  || fail "the audit queue policy must allow only the SNS service from the event topic (aws:SourceArn)"
ok "audit queue policy: SNS service only, restricted to the event topic by aws:SourceArn"
for role in producer runtime publisher inspector; do
  awslocal iam get-user-policy --user-name "wager-$role" --policy-name "wager-$role" --query PolicyDocument >/dev/null || fail "policy of wager-$role missing"
  if awslocal iam get-user-policy --user-name "wager-$role" --policy-name "wager-$role" --query PolicyDocument --output json | grep -q '"\*"'; then fail "wager-$role has a wildcard"; fi
done
ok "principals wager-{producer,runtime,publisher,inspector} have resource-scoped policies without wildcards"
[ -z "$(awslocal iam list-user-policies --user-name wager-nobody --query PolicyNames --output text | tr -d '[:space:]')" ] || fail "wager-nobody must have no policy"
ok "wager-nobody has no policy"
CREDS="${BROKER_CREDENTIALS_FILE:-.local/broker/credentials}"
for p in producer runtime publisher inspector nobody; do grep -q "^\[wager-$p\]" "$CREDS" || fail "profile wager-$p missing in $CREDS"; done
ok "credentials file with one profile per principal ($CREDS)"
echo "infrastructure OK"
