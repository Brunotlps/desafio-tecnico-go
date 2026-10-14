# Atalhos. Os comandos "reais" estão documentados no README.md.
.PHONY: infra infra-down broker-provision test race vet fmt integration integration-race migrate-up migrate-down migrate-status

ENV_FILE ?= .env.example

infra:            ## sobe PostgreSQL, Keycloak e o broker (MiniStack), e provisiona filas, tópico, identidades e credenciais
	mkdir -p .local/broker
	docker compose up -d --wait postgres keycloak ministack
	docker compose run --rm broker-provision

broker-provision: ## (re)provisiona o broker; idempotente
	docker compose run --rm broker-provision

infra-down:       ## derruba o ambiente e apaga os volumes
	docker compose down -v

fmt:
	gofmt -l -w .

vet:
	go vet ./... && go vet -tags integration ./...

test:             ## testes unitários
	go test ./...

race:
	go test -race ./...

integration:      ## integração com PostgreSQL, Keycloak e MiniStack reais (make infra antes)
	go test -tags integration -count=1 ./test/integration/...

integration-race:
	go test -tags integration -race -count=1 ./test/integration/...

migrate-up:       ## usa DATABASE_URL do ambiente (ou do .env.example)
	set -a; . ./$(ENV_FILE); set +a; go run ./cmd/migrate up

migrate-down:     ## reverte a última migration
	set -a; . ./$(ENV_FILE); set +a; go run ./cmd/migrate down

migrate-status:
	set -a; . ./$(ENV_FILE); set +a; go run ./cmd/migrate status
