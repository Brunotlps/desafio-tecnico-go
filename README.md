# Wager Service — processamento de apostas (Go, PostgreSQL, Keycloak, SQS)

Implementação do desafio descrito em [`descricao-desafio/README.md`](descricao-desafio/README.md) (a fonte normativa;
não é alterada aqui). Este repositório contém o **primeiro incremento funcional**: base operacional
e o fluxo financeiro vertical *abertura de carteira → BET → consulta*, por HTTP, com autenticação
real, ledger, idempotência persistente e eventos gravados na outbox.

> **Status resumido** — o que existe e o que **não** existe está na seção [Escopo e pendências](#escopo-e-pendências).
> Decisões e interpretações estão em [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Pré-requisitos

| Ferramenta     | Versão testada | Uso                                   |
| -------------- | -------------- | ------------------------------------- |
| Go             | 1.26.1         | build e testes (declarado em `go.mod` e no `Dockerfile`) |
| Docker + Compose v2 | 29.x / 5.x | PostgreSQL, Keycloak, LocalStack, API |
| curl, python3  | —              | exemplos deste README                 |

Memória: Keycloak + LocalStack + PostgreSQL usam ~2 GB. Portas publicadas (somente em `127.0.0.1`):
PostgreSQL **5433**, Keycloak **8081**, LocalStack **4566**, API **8080** (sobrescreva com
`POSTGRES_PORT`, `KEYCLOAK_PORT`, `LOCALSTACK_PORT`, `API_PORT`).

## Início rápido

```sh
docker compose up --build        # postgres, keycloak, localstack, migrate (one-shot) e api
curl -s localhost:8080/health/ready
# {"checks":{"postgres":"ok","sqs":"ok"},"status":"ok"}
```

A primeira subida importa o realm do Keycloak (`deploy/keycloak/wager-realm.json`), provisiona as filas
e o destino dos eventos (`deploy/localstack/provision.sh`) e aplica as migrations (`migrate up`).
Para recomeçar do zero: `docker compose down -v`.

### Identidades de teste (Keycloak, realm `wager`, `client_credentials`)

| client_id               | secret                              | papel              | providerId (claim) | Uso                                   |
| ----------------------- | ----------------------------------- | ------------------ | ------------------ | ------------------------------------- |
| `internal-service`      | `internal-service-dev-secret`       | `internal-service` | —                  | abrir/consultar carteiras; ler qualquer transação |
| `provider-a`            | `provider-a-dev-secret`             | `provider`         | `provider-a`       | enviar BET e consultar as próprias transações |
| `provider-b`            | `provider-b-dev-secret`             | `provider`         | `provider-b`       | idem, para testar isolamento           |
| `provider-a-shortlived` | `provider-a-shortlived-dev-secret`  | `provider`         | `provider-a`       | token com validade de 3 s (teste de expiração) |
| `no-roles-client`       | `no-roles-dev-secret`               | —                  | —                  | autenticado, sem permissão (403)       |
| `wrong-audience-client` | `wrong-audience-dev-secret`         | `provider`         | `provider-a`       | token sem a audiência `wager-api` (401) |

Todos são **valores de desenvolvimento**, não segredos reais. Console do Keycloak: <http://localhost:8081> (`admin`/`admin`).

### Exemplo completo

```sh
KC=http://localhost:8081/realms/wager/protocol/openid-connect/token
token() { curl -s -X POST $KC -d grant_type=client_credentials -d client_id=$1 -d client_secret=$2 \
          | python3 -c 'import sys,json; print(json.load(sys.stdin)["access_token"])'; }
INTERNAL=$(token internal-service internal-service-dev-secret)
PROVIDER_A=$(token provider-a provider-a-dev-secret)
PLAYER=$(python3 -c 'import uuid; print(uuid.uuid4())')

# 1. abrir carteira (somente serviço interno) -> 201
curl -s -X POST localhost:8080/wallets -H "Authorization: Bearer $INTERNAL" \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"100.00\",\"currency\":\"BRL\"}}"
# {"id":"0192...","playerId":"...","balance":{"amount":"100.00","currency":"BRL"},"version":1}
WALLET=<id retornado>

# 2. BET como provider-a -> 200 PROCESSED
curl -s -X POST localhost:8080/wagering/transactions \
  -H "Authorization: Bearer $PROVIDER_A" -H "Idempotency-Key: provider-a:transaction-123" \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"80.00\",\"currency\":\"BRL\"}}"
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"20.00","currency":"BRL"},"idempotentReplay":false}

# 3. repetir o mesmo comando -> 200 com "idempotentReplay":true e o saldo original (20.00)
# 4. outra BET de 80.00 (nova chave e novo id externo) -> 422 REJECTED / INSUFFICIENT_FUNDS
# 5. consultas
curl -s -H "Authorization: Bearer $PROVIDER_A" localhost:8080/providers/provider-a/wagering/transactions/transaction-123
curl -s -H "Authorization: Bearer $INTERNAL"   localhost:8080/wallets/$WALLET
```

### Contrato HTTP implementado

| Método e rota                                                         | Quem               | Sucesso |
| --------------------------------------------------------------------- | ------------------ | ------- |
| `POST /wallets`                                                       | `internal-service` | 201     |
| `GET /wallets/{walletId}`                                             | `internal-service` | 200     |
| `POST /wagering/transactions` (header `Idempotency-Key` obrigatório)  | `provider`         | 200 (`PROCESSED`) · 422 (`REJECTED`) |
| `GET /wagering/transactions/{transactionId}`                          | `provider` (só as suas) · `internal-service` (todas) | 200 |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | `provider` (só o seu id) · `internal-service` | 200 |
| `GET /health/live`, `GET /health/ready`                               | público            | 200 / 503 |
| `GET /wallets/{id}/ledger`, `POST /wallets/{id}/reconciliation`       | `internal-service` | **501** (não implementado) |

Respostas de erro: `{"code":"...","message":"..."}`.

| Situação                                              | HTTP | `code`                                   |
| ----------------------------------------------------- | ---- | ---------------------------------------- |
| Token ausente / inválido / expirado / audiência errada | 401  | `UNAUTHENTICATED` / `INVALID_TOKEN`      |
| Sem permissão (papel, `providerId` diferente do token) | 403  | `FORBIDDEN`                              |
| JSON inválido, número no lugar de string monetária    | 400  | `INVALID_JSON`                           |
| Valor/moeda/tipo/campo inválidos                       | 400  | `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_KIND`, `INVALID_REQUEST`, `AMOUNT_NOT_ALLOWED_FOR_KIND`, `IDEMPOTENCY_KEY_REQUIRED` |
| Carteira / transação inexistente                       | 404  | `WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND` |
| Mesma chave com conteúdo diferente                     | 409  | `IDEMPOTENCY_KEY_REUSED`                 |
| Mesmo `(providerId, externalTransactionId)` com outra chave | 409 | `EXTERNAL_TRANSACTION_ALREADY_REGISTERED` |
| Carteira já existe para jogador+moeda                  | 409  | `WALLET_ALREADY_EXISTS` (+ `walletId`)   |
| Carteira não pertence ao jogador/moeda; moeda não aceita; `OPENING` por HTTP | 422 | `WALLET_MISMATCH`, `CURRENCY_NOT_SUPPORTED`, `KIND_NOT_ALLOWED` |
| Rejeição de negócio (saldo insuficiente)               | 422  | corpo `{"status":"REJECTED","failureCode":"INSUFFICIENT_FUNDS",...}` |
| `WIN`, `LOSS`, `REFUND`, `ROLLBACK` (ainda sem processamento) | 501 | `KIND_NOT_SUPPORTED` — nada é persistido |
| Aceito sem resultado (`PENDING*`; **inalcançável nesta versão**) | 202 | corpo com `status`                       |
| Indisponibilidade transitória                          | 503  | `TEMPORARILY_UNAVAILABLE` (+ `Retry-After`) |
| Resultado do commit desconhecido                       | 503  | `COMMIT_OUTCOME_UNKNOWN` — repita com a **mesma** chave |

## Variáveis de ambiente

Veja [`.env.example`](.env.example) (valores locais, sem segredos). A configuração é validada na
partida; todos os problemas são reportados de uma vez.

| Variável | Padrão | Descrição |
| --- | --- | --- |
| `DATABASE_URL` | — (obrigatória) | URL do PostgreSQL com o papel de **runtime** (`wager_app`: sem posse das tabelas, sem superusuário, sem UPDATE/DELETE no ledger) |
| `MIGRATE_DATABASE_URL` (só `cmd/migrate`) | `DATABASE_URL` | URL com o papel **dono do schema** (`wager`) |
| `RUNTIME_DB_ROLE`, `RUNTIME_DB_PASSWORD` (só `cmd/migrate`) | — | se definido, `migrate up` cria/atualiza o papel de runtime e seus privilégios mínimos |
| `DB_MAX_CONNS` | `10` | conexões do pool (1–500) |
| `OIDC_ISSUER` | — (obrigatória) | valor esperado da claim `iss` |
| `OIDC_JWKS_URL` | — (obrigatória) | onde buscar as chaves públicas (pode diferir do issuer na rede do Compose) |
| `OIDC_AUDIENCE` | — (obrigatória) | audiência exigida (`wager-api`) |
| `OIDC_CLOCK_SKEW` | `5s` | tolerância de relógio na validação do token |
| `AWS_REGION`, `AWS_ENDPOINT_URL` | `us-east-1`, vazio | SQS (LocalStack localmente); credenciais pela cadeia padrão do SDK |
| `SQS_WAGER_QUEUE_NAME` | `wager-transactions.fifo` | fila verificada na readiness |
| `ALLOWED_CURRENCIES` | `BRL` | moedas aceitas (códigos de 3 letras maiúsculas) |
| `HTTP_ADDR` | `:8080` | endereço do servidor |
| `REQUEST_TIMEOUT` | `10s` | prazo por requisição |
| `SHUTDOWN_TIMEOUT` | `20s` | prazo do encerramento gracioso |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Filas e eventos (LocalStack)

`deploy/localstack/provision.sh` roda quando o LocalStack fica pronto (idempotente):

| Recurso | Configuração |
| --- | --- |
| `wager-transactions.fifo` | FIFO, `ContentBasedDeduplication=false`, `VisibilityTimeout=60`, redrive: `maxReceiveCount=5` → DLQ |
| `wager-transactions-dlq.fifo` | FIFO, retenção de 14 dias |
| `wager-events.fifo` (SNS FIFO) | destino dos eventos de saída (publicador de outbox: incremento futuro) |
| `wager-events-audit.fifo` (SQS) | assinante do tópico (entrega raw) para auditoria/testes |

Reprovisionar manualmente: `docker compose exec localstack bash /etc/localstack/init/ready.d/10-provision.sh`.
Inspecionar: `docker compose exec localstack awslocal sqs list-queues`.

## Migrations

Versionadas em `internal/infra/postgres/migrations` (goose, embutidas no binário), com lock de sessão
do PostgreSQL contra execuções concorrentes. Rodam com o papel **dono do schema**; a API roda com o papel de runtime.

```sh
export MIGRATE_DATABASE_URL='postgres://wager:wager@localhost:5433/wager?sslmode=disable'
export RUNTIME_DB_ROLE=wager_app RUNTIME_DB_PASSWORD=wager_app_dev_password   # opcional, ver abaixo
go run ./cmd/migrate up                # aplica as pendentes e, se RUNTIME_DB_ROLE estiver definido, cria/atualiza o papel de runtime
go run ./cmd/migrate status
go run ./cmd/migrate grants            # só (re)aplica os privilégios mínimos do papel de runtime
go run ./cmd/migrate down              # reverte a última
go run ./cmd/migrate down-to 0         # reverte todas (apaga os dados!)
# no Compose: docker compose run --rm migrate up | down | status | grants
```

A API **recusa iniciar** se o schema não estiver na versão esperada pelo binário.

### Bancos já inicializados (migrations corretivas 00002 e 00003)

As migrations já publicadas (00001, 00002) **não são reescritas**; as correções são migrations novas. Para um banco criado
por versões anteriores, **não apague nada**: rode `migrate up` (no Compose: `docker compose up -d --build migrate api`).

* `00002_ledger_chain_and_result_integrity.sql` — proteção do ledger (cadeia contínua de saldos, vínculo
  lançamento→transação→carteira) e resultado não nulo em `PROCESSED`/`REJECTED`.
* `00003_completed_movement_requires_ledger.sql` — toda transação `PROCESSED` com movimento (`OPENING`, `BET`, `WIN`,
  `REFUND`, `ROLLBACK`) precisa, no COMMIT, do lançamento da própria carteira, e `result_balance` tem de ser o `balance_after`
  desse lançamento (`LOSS` e rejeições não têm lançamento).

Cada uma começa com uma **verificação do histórico**: se houver registros incompatíveis (cadeia quebrada, resultado nulo,
lançamento sem transação coerente; na 00003, transação concluída sem lançamento ou com resultado divergente, com contagem e
primeiros ids) a migration **aborta sem alterar dados nem o schema** — a correção de registros históricos é manual, depois
rode `migrate up` de novo. Reversão: `migrate down` (um passo por vez) remove as garantias da última migration e **não
altera dados**.

Papel de runtime em bancos antigos: o primeiro `migrate up` desta versão (com `RUNTIME_DB_ROLE`) cria o `wager_app`; depois
troque `DATABASE_URL` da API para ele (o Compose já faz isso).

## Executar a aplicação sem Docker para a API

```sh
docker compose up -d --wait postgres keycloak localstack   # ou: make infra
set -a; . ./.env.example; set +a
go run ./cmd/migrate up      # como dono do schema; cria o papel wager_app
go run ./cmd/api             # como wager_app (DATABASE_URL)
```

## Testes

```sh
go test ./...            # unitários (domínio, config) — não precisam de infraestrutura
go test -race ./...
go vet ./...
```

### Integração (PostgreSQL, Keycloak e LocalStack reais)

Preparação — `make infra` (ou `docker compose up -d --wait postgres keycloak localstack`). Os testes usam o banco de
`TEST_DATABASE_URL` (padrão `wager_test`; **qualquer nome**), aplicando `down-to 0` + `up` a cada execução (o que também
exercita as migrations), e criam o papel de runtime próprio da suíte (`wager_test_app`; papéis valem para o servidor inteiro, então ela nunca reutiliza o `wager_app` da aplicação). A aplicação sob teste roda com esse papel restrito.

Salvaguardas: a suíte **recusa** reaplicar o schema em um banco que seja o da aplicação (`DATABASE_URL`/`MIGRATE_DATABASE_URL`),
um banco de sistema, ou um banco com tabelas que não esteja marcado como de teste (`COMMENT ON DATABASE`, feito pela
própria suíte ao criar o banco ou adotar um banco vazio). Os bancos auxiliares (schema não migrado/desatualizado, migração
corretiva) têm nome derivado do de teste mais sufixo aleatório e são removidos ao fim. Para adotar **uma única vez** um banco
de teste criado por versões anteriores da suíte:
`psql ... -c "COMMENT ON DATABASE wager_test IS 'wager-integration-test-database'"`.

```sh
go test -tags integration -count=1 ./test/integration/...          # ~25 s
go test -tags integration -race -count=1 ./test/integration/...
go vet -tags integration ./...
go test -tags integration -count=1 ./test/integration -run 'TestThree|TestKilling' -v   # 3 processos
```

Variáveis opcionais (padrões coincidem com o Compose): `TEST_DATABASE_URL`, `TEST_RUNTIME_DB_ROLE`,
`TEST_RUNTIME_DB_PASSWORD`, `KEYCLOAK_TOKEN_URL`, `OIDC_ISSUER`, `OIDC_JWKS_URL`, `AWS_ENDPOINT_URL`.

| Grupo | Arquivo | O que prova |
| --- | --- | --- |
| Casos de uso | `service_test.go` | abertura de carteira e BET direto no serviço (sem HTTP): replay, conflitos com precedência da identidade, rejeição, 50 reenvios e 2×80,00 sobre 100,00 |
| Verificador JWT | `auth_verifier_test.go` | identidades derivadas do token do Keycloak; adulterado, forjado, `alg:none`, HS256, emissor/audiência diferentes, expirado; `Start` falha com IdP inacessível |
| Fluxo | `flow_test.go` | abertura positiva (OPENING, ledger, 2 eventos, versão 1), zero, duplicada (409), parsing estrito de dinheiro por HTTP, BET aceita/rejeitada, tipos não suportados, validação de entrada |
| Idempotência | `idempotency_test.go` | replay devolve o saldo original após outras movimentações; rejeição terminal não é reavaliada após crédito; conflito de payload; mesma operação externa com outra chave; chaves por provedor; **conflito tem precedência sobre carteira inexistente/jogador/moeda divergentes**; disputa da mesma identidade entre carteiras diferentes |
| Concorrência | `concurrency_test.go` | 50 reenvios paralelos → 1 débito; 2×80,00 sobre 100,00 (com reenvios) → 1 processada, 1 rejeitada, saldo 20,00; sem lost update; carteiras independentes não se bloqueiam; várias carteiras em paralelo |
| Movimento concluído | `completed_movement_test.go` | **pelo papel restrito**, com payloads de hash válido: `BET PROCESSED` sem lançamento nem alteração da carteira, `OPENING` sem lançamento, `PENDING`→`PROCESSED` sem lançamento e resultado diferente do `balance_after` do lançamento (777,00 × 90,00; zero × 90,00) são recusados no COMMIT; várias operações válidas na mesma transação SQL guardam o resultado próprio e o replay (HTTP) devolve o saldo original; `LOSS` e rejeições seguem sem lançamento |
| Banco | `constraints_test.go`, `db_integrity_test.go` | SQL direto: unicidade, saldo não negativo, ledger append-only (UPDATE/DELETE/TRUNCATE), saldo×ledger no mesmo commit, OPENING único, forma interna×externa, política de zero, imutabilidade de terminais e da outbox; **cadeia contínua do ledger** (cenário da revisão, primeiro lançamento, vários lançamentos por transação, escritor concorrente obsoleto, carteira independente não bloqueada); **vínculo lançamento→transação→carteira**; **resultado explícito (NULL e valores inválidos)** |
| Papéis | `role_test.go` | o papel de runtime não é superusuário nem dono, e 20+ tentativas (UPDATE/DELETE/TRUNCATE do ledger, `DISABLE TRIGGER`, `session_replication_role`, DDL…) são negadas; o fluxo financeiro e a verificação de schema funcionam com ele; provisão idempotente |
| Migrations | `migration_test.go`, `migration_movement_test.go` | as corretivas preservam dados de um banco existente (e na reversão), recusam sem alterar nada histórico com cadeia quebrada, resultado nulo, transação concluída sem lançamento ou resultado divergente (relatando contagem e ids), e aplicar→reverter→reaplicar funciona |
| Autenticação | `auth_test.go` | credenciais ausentes, adulteradas, forjadas, `alg:none`, HS256, expiradas (token real de 3 s), audiência errada → 401; sem papel → 403; isolamento entre provedores (consulta, replay, headers falsos); nenhuma alteração de estado em acessos negados |
| Falhas | `faults_test.go` | falha no meio da transação não deixa estado parcial; ledger "esquecido" é barrado pelo banco no COMMIT; commit confirmado com ACK perdido → 503 e recuperação com a mesma chave |
| Ciclo de vida | `lifecycle_test.go` | reinício preserva idempotência/resultados; grafo Fx válido, start/stop e liberação de conexões; **7 falhas de partida (inclusive schema ausente/desatualizado e credencial inválida) sem conexão PostgreSQL remanescente**; shutdown conclui requisição em andamento |
| Multiprocesso | `multiprocess_test.go` | **3 processos do binário real**: 50 reenvios espalhados, disputa 2×80,00 entre processos, conexões próprias, `SIGTERM` com saída 0; `SIGKILL` de um processo no meio da carga sem duplicar nem perder débito |

## Integração contínua

`.github/workflows/ci.yml` roda em pull requests e no `main`; todos os jobs precisam passar para o merge:

| Job | O que executa |
| --- | --- |
| `verify` | `gofmt`, `go mod verify`, `go mod tidy` sem diferenças, `go vet` (com e sem `-tags integration`), `go test -race` |
| `infra` | `docker compose up --wait` de PostgreSQL, Keycloak e LocalStack + `scripts/check-infra.sh` (tokens, papéis, `provider_id`, filas, redrive, destino dos eventos) |
| `integration` | os testes de integração com `-race` contra PostgreSQL, Keycloak e LocalStack reais |
| `stack` | `docker compose up --build` (imagem, migrations, papel de runtime, API), espera a API ficar `healthy` (`scripts/wait-healthy.sh`: estado estruturado do `docker inspect`, comparação exata, com prazo e diagnóstico) + `scripts/smoke.sh` |

Os mesmos scripts rodam localmente: `scripts/check-infra.sh` (com a infraestrutura de pé), `scripts/wait-healthy.sh <serviço> [segundos]` e `scripts/smoke.sh` (com o stack de pé).
Para validar o Compose sem mexer no seu ambiente, use outro projeto e outras portas:

```sh
export POSTGRES_PORT=15433 KEYCLOAK_PORT=18081 LOCALSTACK_PORT=14566 API_PORT=18080
docker compose -p wager-check up --build -d
API_URL=http://localhost:18080 KEYCLOAK_URL=http://localhost:18081 scripts/smoke.sh
docker compose -p wager-check down -v      # remove apenas o projeto de verificação
```

## Estrutura

```
cmd/api            servidor HTTP (Fx)            cmd/migrate        CLI de migrations e do papel de runtime
internal/domain    Money, Wallet, WagerTransaction, LedgerEntry, eventos, hash canônico (sem Fx/HTTP/SQL)
internal/app       casos de uso (OpenWallet, Submit, consultas) e portas de persistência
internal/infra/    postgres (pgx, unit of work, repositórios, migrations) · auth (JWT/JWKS) ·
                   httpapi (handlers, erros, health, servidor) · sqs (readiness)
internal/bootstrap composição com fx.Module e fx.Lifecycle
deploy/            realm do Keycloak, provisionamento do LocalStack
scripts/           check-infra.sh (ambiente) e smoke.sh (fluxo ponta a ponta pela API)
.github/workflows  CI
test/integration   testes com infraestrutura real (build tag `integration`)
```

## Escopo e pendências

**Implementado e testado (HTTP):** configuração validada; Fx com lifecycle; Docker Compose; Keycloak e
LocalStack provisionados; migrations; health checks reais; Money; Wallet; WagerTransaction (máquina
de estados completa no domínio); LedgerEntry; abertura de carteira; `BET` com idempotência
persistente, rejeição por saldo, ledger e outbox atômicos; consultas com isolamento por provedor;
autenticação/autorização reais.

**Não implementado (explícito):**

| Item do enunciado | Situação |
| --- | --- |
| `WIN`, `LOSS`, `REFUND`, `ROLLBACK` | regras de valor/referência validadas no domínio; processamento **ausente** (HTTP 501, sem persistir). Política de combinações REFUND×ROLLBACK **não decidida** — ver `ARCHITECTURE.md` §Reversões |
| Consumidor SQS e tabela de inbox | ausentes (a fila é provisionada e a readiness a verifica) |
| Publicador de outbox (múltiplas instâncias, backoff, recuperação) | ausente; eventos ficam gravados em `outbox_events` com colunas de tentativas/próximo envio |
| `PENDING_REFERENCE`, worker de referências, backoff/TTL | ausentes (estado e evento existem no domínio) |
| `GET /wallets/{id}/ledger` (cursor) e `POST /wallets/{id}/reconciliation` | 501 |
| Métricas (resultados, duplicatas, retries, DLQ, atraso da outbox, reconciliação) | ausentes; há logs JSON com `correlationId`, `transactionId`, `walletId`, `providerId` |
| Testes SQS/HTTP cruzados, DLQ, interrupção de consumidor, dois publishers | dependem dos itens acima |
| Prova com 3 processos para SQS/outbox | só para o fluxo HTTP implementado |
| Testes de carga, OpenTelemetry, partidas dobradas | opcionais, não feitos |

**Próximo incremento recomendado:** `WIN`/`LOSS` sem referência + `REFUND`/`ROLLBACK` com a política
de reversões decidida; em seguida inbox + consumidor SQS reutilizando `app.Service.Submit` e o
publicador de outbox. Detalhes em `ARCHITECTURE.md`.
