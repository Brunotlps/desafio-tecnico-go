#!/usr/bin/env bash
# Espera um serviço do Compose ficar saudável. Lê o estado estruturado do health check
# (`docker inspect`) e o compara EXATAMENTE com "healthy": um padrão como *healthy* também
# casaria com "unhealthy". Ao esgotar o prazo, imprime o estado, `compose ps` e os logs do
# serviço e sai com código != 0.
#
#   scripts/wait-healthy.sh api [segundos]
#
# Variáveis: COMPOSE (padrão "docker compose"), WAIT_INTERVAL (padrão 2, em segundos).
set -euo pipefail

service="${1:?uso: wait-healthy.sh <serviço> [segundos]}"
timeout="${2:-180}"
interval="${WAIT_INTERVAL:-2}"
COMPOSE="${COMPOSE:-docker compose}"

health_of() { # devolve healthy | unhealthy | starting | none (sem health check) | missing (sem contêiner)
  local cid
  cid=$($COMPOSE ps -aq "$service" 2>/dev/null | head -n1)
  if [ -z "$cid" ]; then echo missing; return; fi
  docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo missing
}

deadline=$((SECONDS + timeout))
status=missing
while [ "$SECONDS" -lt "$deadline" ]; do
  status=$(health_of)
  if [ "$status" = "healthy" ]; then
    echo "$service is healthy"
    exit 0
  fi
  sleep "$interval"
done

echo "$service did not become healthy within ${timeout}s (last state: $status)" >&2
$COMPOSE ps -a >&2 || true
$COMPOSE logs --no-color --tail 200 "$service" >&2 || true
exit 1
