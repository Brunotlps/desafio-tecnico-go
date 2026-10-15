# Wager Service — processamento de apostas (Go, PostgreSQL, Keycloak, SQS)

Implementação do desafio descrito em [`descricao-desafio/README.md`](descricao-desafio/README.md) (a fonte normativa;
não é alterada aqui). Este repositório contém a base operacional e o fluxo financeiro vertical
*abertura de carteira → BET / WIN / LOSS / REFUND / ROLLBACK (com referências pendentes) → consulta*, por HTTP, com autenticação
real, ledger, idempotência persistente e eventos gravados na outbox.

> **Status resumido** — o que existe e o que **não** existe está na seção [Escopo e pendências](#escopo-e-pendências).
> Decisões e interpretações estão em [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Pré-requisitos

| Ferramenta     | Versão testada | Uso                                   |
| -------------- | -------------- | ------------------------------------- |
| Go             | 1.26.1         | build e testes (declarado em `go.mod` e no `Dockerfile`) |
| Docker + Compose v2 | 29.x / 5.x | PostgreSQL, Keycloak, MiniStack (broker), API |
| curl, python3  | —              | exemplos deste README                 |

Memória: Keycloak + MiniStack + PostgreSQL usam ~2 GB. Portas publicadas (somente em `127.0.0.1`):
PostgreSQL **5433**, Keycloak **8081**, broker (MiniStack) **4566**, API **8080** (sobrescreva com
`POSTGRES_PORT`, `KEYCLOAK_PORT`, `BROKER_PORT`, `API_PORT`).

## Início rápido

```sh
docker compose up --build        # postgres, keycloak, ministack, broker-provision e migrate (one-shot) e api
curl -s localhost:8080/health/ready
# {"checks":{"postgres":"ok","sqs":"ok"},"status":"ok"}
```

A primeira subida importa o realm do Keycloak (`deploy/keycloak/wager-realm.json`), provisiona o broker (filas, tópico, identidades,
políticas e credenciais — serviço one-shot `broker-provision`, ver [Broker](#broker-minestack-com-auth-true)) e aplica as migrations
(`migrate up`). A API só inicia depois de os dois one-shots terminarem com sucesso. Para recomeçar do zero: `docker compose down -v`
(o diretório `.local/broker`, com credenciais sintéticas, é do host e fica; o provisionamento o reconcilia).

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
| `POST /wagering/transactions` (header `Idempotency-Key` obrigatório; `BET`, `WIN` (com ou sem referência), `LOSS`, `REFUND`, `ROLLBACK`) | `provider` | 200 (`PROCESSED`) · 202 (`PENDING_REFERENCE`) · 422 (`REJECTED`) |
| `GET /wagering/transactions/{transactionId}`                          | `provider` (só as suas) · `internal-service` (todas) | 200 |
| `GET /providers/{providerId}/wagering/transactions/{externalTransactionId}` | `provider` (só o seu id) · `internal-service` | 200 |
| `GET /health/live`, `GET /health/ready`                               | público            | 200 / 503 |
| `GET /wallets/{walletId}/ledger?cursor=&limit=`                       | `internal-service` | 200 (ver "Ledger paginado") |
| `POST /wallets/{walletId}/reconciliation`                             | `internal-service` | 200 (ver "Reconciliação") |

Respostas de erro: `{"code":"...","message":"..."}`.

### Ledger paginado

`GET /wallets/{walletId}/ledger?cursor=<opaco>&limit=<1..100>` — só `internal-service` (provedor → 403, sem token/token inválido → 401, sem dados nem efeitos).

* Ordem crescente pela **posição `seq`** do ledger, no escopo da carteira; a continuação é `seq > último devolvido` (**sem `OFFSET`**). Gaps de `seq` e
  `createdAt` iguais são válidos: o cursor nunca usa o instante. `limit` padrão **50**, mínimo 1, máximo 100.
* Resposta: `{"items":[{"id","walletId","transactionId","direction":"DEBIT|CREDIT","money","balanceBefore","balanceAfter","createdAt"}],"nextCursor":"..."}`.
  Valores monetários são `{"amount":"25.00","currency":"BRL"}` (string, nunca número). `nextCursor` só existe quando há próxima página (o servidor lê `limit+1`
  para decidir, sem perder nem duplicar o item extra). Carteira existente sem lançamentos → `{"items":[]}`; inexistente → 404 `WALLET_NOT_FOUND`.
* **Cursor opaco e versionado**: base64url de `{"v":1,"w":<carteira>,"s":<seq>}`. O cliente o trata como opaco; ele **não autoriza nada** (autorização e escopo
  continuam no servidor): só delimita a continuação. É validado em formato canônico, tamanho (≤ 200), versão, posição (≥ 1) e correspondência com a carteira da rota;
  qualquer falha é `400 INVALID_CURSOR` (nunca uma consulta ampliada). `limit` não inteiro, fora de 1..100 ou repetido → `400 INVALID_LIMIT`; `cursor` vazio ou repetido → `400 INVALID_CURSOR`. A query é interpretada de forma explícita: qualquer erro de
  parsing (escape `%` inválido, separador `;`, inclusive em parâmetros desconhecidos) recusa a requisição inteira com `400 INVALID_QUERY`, sem dados e sem consultar o ledger — um par malformado
  nunca é descartado em silêncio (o que faria um parâmetro inválido virar o default ou uma repetição parecer única). A autenticação e a autorização vêm antes (sem token → 401, provedor → 403).
* **Visão de leitura**: cada página tem a sua própria visão (uma consulta, sem transação mantida entre requisições). Lançamentos confirmados depois podem aparecer nas
  páginas seguintes; **não há snapshot congelado da navegação inteira**. Não se perde nem se duplica um item já navegável porque todo lançamento é gravado sob o lock da
  carteira, liberado só no commit: dentro de uma carteira, uma posição menor nunca é confirmada depois de uma maior.

### Reconciliação

`POST /wallets/{walletId}/reconciliation` — só `internal-service`. Lê o saldo armazenado e reconstrói o ledger (incluindo a abertura) na **mesma visão**: uma transação
PostgreSQL `READ ONLY, REPEATABLE READ`, com prazo de 10 s, sem lock da carteira (movimentos independentes não esperam) e sem reparo. A `UnitOfWork` financeira continua `READ COMMITTED`.

```json
{"walletId":"…","storedBalance":{"amount":"75.00","currency":"BRL"},"calculatedBalance":{"amount":"75.00","currency":"BRL"},
 "difference":{"amount":"0.00","currency":"BRL"},"consistent":true,"checkedEntries":2}
```

`calculatedBalance` = créditos − débitos; `difference` = armazenado − calculado (pode ser negativo); `checkedEntries` = lançamentos considerados. Carteira zero sem lançamentos → 0,00/0,00/0,00,
`consistent:true`, `checkedEntries:0`. `LOSS` e rejeições não geram lançamento; reversões acrescentam seus movimentos normais.

* **Aritmética exata**: a soma é feita pelo PostgreSQL em `NUMERIC` (créditos − débitos, sem o limite de `int64`: somas separadas podem passar de `MaxInt64` com saldo final válido) e convertida para
  `Money` com verificação de faixa; nada passa por ponto flutuante. Se o saldo calculado ou a diferença não couberem em `Money`, ou houver lançamento em outra moeda, a resposta é
  `500 RECONCILIATION_FAILED` (log com a carteira) — **nunca** `consistent:true` nem uma divergência inventada. Falha de leitura é indisponibilidade (`503`), não resultado.
* **Divergência** (`consistent:false`): resposta com os valores, log de nível `ERROR` (`reconciliation divergence detected`, com carteira, valores, `checkedEntries` e versão) e um
  ponto de integração para a métrica (`app.WithReconciliationObserver`); saldo, ledger, versão, transações e eventos **não** são alterados.
* A métrica de divergência e as demais categorias de `GET /metrics` fazem parte da instrumentação posterior (não implementada).

| Situação                                              | HTTP | `code`                                   |
| ----------------------------------------------------- | ---- | ---------------------------------------- |
| Token ausente / inválido / expirado / audiência errada | 401  | `UNAUTHENTICATED` / `INVALID_TOKEN`      |
| Sem permissão (papel, `providerId` diferente do token) | 403  | `FORBIDDEN`                              |
| JSON inválido, número no lugar de string monetária    | 400  | `INVALID_JSON`                           |
| Valor/moeda/tipo/campo inválidos                       | 400  | `INVALID_AMOUNT`, `INVALID_CURRENCY`, `INVALID_KIND`, `INVALID_REQUEST`, `AMOUNT_NOT_ALLOWED_FOR_KIND`, `IDEMPOTENCY_KEY_REQUIRED` |
| Carteira / transação inexistente                       | 404  | `WALLET_NOT_FOUND`, `TRANSACTION_NOT_FOUND` |
| Mesma chave com conteúdo diferente (valor, carteira, **moeda ou tipo**, inclusive moeda não habilitada ou tipo não processado) | 409  | `IDEMPOTENCY_KEY_REUSED`                 |
| Mesmo `(providerId, externalTransactionId)` com outra chave | 409 | `EXTERNAL_TRANSACTION_ALREADY_REGISTERED` |
| Carteira já existe para jogador+moeda                  | 409  | `WALLET_ALREADY_EXISTS` (+ `walletId`)   |
| Carteira não pertence ao jogador/moeda; moeda não habilitada (operação nova); `OPENING` por HTTP | 422 | `WALLET_MISMATCH`, `CURRENCY_NOT_SUPPORTED`, `KIND_NOT_ALLOWED` |
| Rejeição de negócio                                    | 422  | corpo `{"status":"REJECTED","failureCode":"<código>","balance":...}` — códigos abaixo |
| Servidor encerrando                                    | 503  | `SHUTTING_DOWN` (+ `Retry-After`) — repita com a mesma chave |
| Referência ainda indisponível (ausente, ou existente e pendente) em `REFUND`, `ROLLBACK` ou `WIN` com `referenceExternalTransactionId` | 202 | corpo `{"status":"PENDING_REFERENCE",...}` **sem saldo**; a operação é persistida com a agenda e retomada pelo worker; o replay devolve o mesmo 202; consulte a transação (`GET`) para o resultado |
| Referência com a própria operação (`referenceExternalTransactionId` = `externalTransactionId`) | 400 | `INVALID_REQUEST` |
| Crédito de `WIN` que ultrapassaria o limite de `Money` | 400 | `INVALID_AMOUNT` — nada é persistido |
| Aceito sem resultado (`PENDING_REFERENCE`, ver acima) | 202 | corpo com `status` e **sem** saldo; o replay devolve o mesmo 202 |
| Indisponibilidade transitória                          | 503  | `TEMPORARILY_UNAVAILABLE` (+ `Retry-After`) |
| Resultado do commit desconhecido                       | 503  | `COMMIT_OUTCOME_UNKNOWN` — repita com a **mesma** chave |

### Códigos de falha (`failureCode`) das rejeições

Toda rejeição é **definitiva** e auditável (`WagerTransactionRejected`, sem ledger, com o saldo observado); o cliente a corrige enviando
**outra** operação (outra chave e outro id externo). O replay devolve a rejeição original, sem reavaliar. Entradas corrigíveis (400/404/422 de
entrada) não são gravadas.

| `failureCode` | Quando |
| --- | --- |
| `INSUFFICIENT_FUNDS` | `BET` cujo débito deixaria o saldo negativo |
| `REVERSAL_INSUFFICIENT_FUNDS` | o débito de um `ROLLBACK` (de `WIN` ou `REFUND`) excederia o saldo — distinto de `INSUFFICIENT_FUNDS` |
| `ALREADY_REVERSED` | o alvo já recebeu uma reversão bem-sucedida (`REFUND` ou `ROLLBACK`): **uma por movimento, em todo o histórico** |
| `REFERENCE_NOT_FOUND` | referência ausente depois do TTL |
| `REFERENCE_PENDING_TIMEOUT` | referência existente, ainda pendente depois do TTL |
| `REFERENCE_NOT_PROCESSED` | referência `REJECTED` ou `FAILED` |
| `REFERENCE_MISMATCH` | escopo (provedor, jogador, carteira, moeda ou rodada), tipo ou valor incompatíveis com a referência |

Política de reversões: `REFUND` credita integralmente uma `BET`; `ROLLBACK` desfaz uma `BET` (crédito), `WIN` ou `REFUND` (débito); o valor da
reversão é igual ao do alvo; `WIN` pode referenciar uma `BET` da mesma rodada (valor livre) e não consome a reversão. `BET → REFUND →
ROLLBACK do REFUND` restaura o débito, e a BET **não admite outra reversão bem-sucedida**. Rejeições e pendências não consomem o direito.
Detalhes e justificativas: `ARCHITECTURE.md` §8.

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
| `AWS_REGION`, `AWS_ENDPOINT_URL` | `us-east-1`, vazio | SQS (MiniStack localmente); credenciais pela cadeia padrão do SDK |
| `AWS_SHARED_CREDENTIALS_FILE`, `AWS_PROFILE` | — | a API **seleciona** o principal de **runtime** do broker: perfil `wager-runtime` do arquivo de credenciais escrito pelo `broker-provision` (no Compose: `/run/broker/credentials`). O arquivo montado contém os **cinco** perfis, legíveis pelo processo; separá-los por processo é uma recomendação técnica de endurecimento, não algo que este ambiente local isole. Não defina `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (prevalecem sobre o perfil) |
| `SQS_WAGER_QUEUE_NAME` | `wager-transactions.fifo` | fila verificada na readiness |
| `ALLOWED_CURRENCIES` | `BRL` | moedas aceitas (códigos de 3 letras maiúsculas) |
| `HTTP_ADDR` | `:8080` | endereço do servidor |
| `REQUEST_TIMEOUT` | `10s` | prazo por requisição |
| `SHUTDOWN_TIMEOUT` | `20s` | prazo **global** do encerramento (admissão, drenagem, cancelamento, cleanup e fechamento das dependências) |
| `SHUTDOWN_DRAIN_TIMEOUT` | `12s` | parte do prazo global dedicada à drenagem; o resto (padrão 8 s) é a reserva compartilhada para cancelamento, cleanup e dependências. Deve ser menor que `SHUTDOWN_TIMEOUT` |
| `REFERENCE_TTL` | `15m` | prazo de uma referência pendente, contado da primeira persistência (não renova) |
| `REFERENCE_BACKOFF_INITIAL`, `REFERENCE_BACKOFF_MAX` | `1s`, `30s` | backoff exponencial com jitter (sempre dentro do teto) entre verificações |
| `REFERENCE_POLL_INTERVAL` | `1s` | pausa do worker entre varreduras |
| `REFERENCE_PHASE_TIMEOUT` | `10s` | limite da fase financeira de uma tentativa (locks e commit) |
| `REFERENCE_WORKERS` | `1` | executores de referências por processo (`0` desliga) |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Broker (MiniStack com AUTH=true)

O broker local é o **MiniStack 1.5.27** (imagem fixada) com `AUTH=true`, que avalia as políticas IAM do principal identificado pela access key
(o enunciado §4 permite LocalStack ou MiniStack, e §2 exige acesso à mensageria controlado por credenciais e políticas do broker).

> **Limite do emulador:** o MiniStack **não valida assinaturas SigV4** — a access key só identifica o principal, e o segredo não é conferido.
> `AUTH=true` demonstra **autorização emulada por políticas**, não autenticação AWS completa. A autenticação real do projeto é a do
> Keycloak na API HTTP; no SQS o vínculo entre origem e `providerId` é responsabilidade do produtor interno confiável (provedores
> externos **não** recebem `SendMessage` direto). O broker é exposto só em `127.0.0.1`, para desenvolvimento. Em produção na AWS, confira: assinatura
> SigV4 e segredos reais, papéis IAM (não usuários com chave estática), políticas de recurso das filas e do tópico, criptografia (KMS) e
> endpoints privados — nada disso é provado aqui.

**Provisionamento** (`cmd/broker-provision`, SDK Go v2, serviço one-shot do Compose; sem `awslocal` nem hooks do emulador): cria ou converge,
de forma idempotente, os recursos abaixo, as identidades e as políticas, e escreve as credenciais por responsabilidade em
`.local/broker/credentials` (ignorado pelo git; chaves **sintéticas locais**). Reexecutar não duplica recursos, não duplica a assinatura e
**reaproveita as chaves ainda válidas** (credenciais em uso não quebram); uma chave que o emulador perdeu é reemitida (reinicie a API para ela
reler o arquivo).

| Recurso | Configuração |
| --- | --- |
| `wager-transactions.fifo` | FIFO, `ContentBasedDeduplication=false`, `VisibilityTimeout=60`, redrive: `maxReceiveCount=5` → DLQ |
| `wager-transactions-dlq.fifo` | FIFO, retenção de 14 dias |
| `wager-events.fifo` (SNS FIFO) | destino dos eventos de saída (publicador de outbox: incremento futuro) |
| `wager-events-audit.fifo` (SQS FIFO) | assinante do tópico (`RawMessageDelivery=true`) para auditoria/testes; política que só admite o serviço SNS **a partir do tópico de eventos** (`aws:SourceArn`) |

| Principal (usuário IAM = perfil) | Permissões (por ação e recurso, sem curingas) |
| --- | --- |
| `wager-producer` (produtor interno) | `SendMessage`, `GetQueueUrl` na entrada. Sem consumo, remoção, DLQ ou SNS |
| `wager-runtime` (consumidor; **selecionado pela API**) | na entrada: `GetQueueUrl`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`; na DLQ: `GetQueueUrl`, `SendMessage` |
| `wager-publisher` (futuro publicador da outbox) | `sns:Publish` e `sns:GetTopicAttributes` no tópico de eventos. Não consome a entrada |
| `wager-inspector` (testes/diagnóstico) | observa e prepara cenários na DLQ e na fila de auditoria; atributos da entrada; assinaturas do tópico |
| `wager-nobody` | nenhuma política (comprova `AccessDenied`) |

O principal de **provisionamento** é a raiz do emulador (access key `test`, ou um id de conta de 12 dígitos), usado só por `broker-provision`
e pela prova; nunca pela aplicação. **Estado:** o MiniStack roda sem `PERSIST_STATE`: filas, mensagens, tópico e identidades são **efêmeros** e
somem quando o contêiner `ministack` é recriado (`docker compose down`/`up` recria; um `restart` também reinicia o processo). Depois disso, `docker compose up -d`
reexecuta o `broker-provision`; reinicie a `api` para ela reler as credenciais. Nenhuma durabilidade é prometida (nem após `SIGKILL`).

```sh
mkdir -p .local/broker                           # dono: você (o provisionamento entrega o arquivo ao dono do diretório)
docker compose up -d --wait postgres keycloak ministack
docker compose run --rm broker-provision        # idempotente (make broker-provision)
scripts/check-infra.sh                            # recursos, redrive, política de entrega, políticas dos principais, arquivo de credenciais
docker compose exec ministack awslocal sqs list-queues    # inspeção com a raiz do emulador
```

**Prova de autorização** (`test/integration/broker_test.go`, executada na CI): com o **SDK Go v2 e as credenciais reais de cada principal**, em recursos
isolados por prefixo (`proof-<id>-`, criados e removidos pelo teste; nunca tocam as filas de desenvolvimento) — produtor envia, runtime recebe/altera
visibilidade/remove, publisher publica e o assinante recebe o conteúdo **raw** com grupo e deduplicação rastreáveis; 20 negativas tipadas
(`AccessDenied`/403, não erro de endpoint) cobrindo cada principal e **a mesma ação em outro recurso**, com ausência de efeito comprovada
(mensagem em voo intacta, filas vazias); FIFO (deduplicação de transporte — janela temporária do broker, **não** a garantia financeira da aplicação — e grupos
distintos em paralelo); visibilidade e redrive com conteúdo e grupo preservados na DLQ (aqui com 1 s e 2 recebimentos, só nesse teste isolado; os padrões 60 s e 5
são verificados em outro teste e no `check-infra`); SNS→SQS **efetivamente entregue**, e **não** entregue quando o tópico não é o autorizado ou quando a política da fila é removida
(um `Publish` aceito não prova entrega); provisionamento repetido e reemissão de chave perdida; partida da API com o principal sem permissão falha com `AccessDenied`.
Observação do emulador: o redrive é aplicado quando o consumidor tenta receber de novo (a AWS o faz em segundo plano), e as ausências de entrega são observações limitadas (long polling de 3 s).

## Migrations

Versionadas em `internal/infra/postgres/migrations` (goose, embutidas no binário), com lock de sessão
do PostgreSQL contra execuções concorrentes. Rodam com o papel **dono do schema**; a API roda com o papel de runtime.

```sh
export MIGRATE_DATABASE_URL='postgres://wager:wager@localhost:5433/wager?sslmode=disable'
export RUNTIME_DB_ROLE=wager_app RUNTIME_DB_PASSWORD=wager_app_dev_password   # opcional, ver abaixo
go run ./cmd/migrate up                # aplica as pendentes e, se RUNTIME_DB_ROLE estiver definido, cria/atualiza o papel de runtime
go run ./cmd/migrate status
go run ./cmd/migrate grants            # só (re)aplica os privilégios mínimos do papel de runtime (coordenado com as demais operações do migrador: espera uma em andamento)
go run ./cmd/migrate down              # reverte a última
go run ./cmd/migrate down-to 0         # RESET destrutivo: reverte todas (apaga os dados, inclusive a cópia preservada da agenda)
# no Compose: docker compose run --rm migrate up | down | status | grants
```

A API **recusa iniciar** se o schema não estiver na versão esperada pelo binário.

### Bancos já inicializados (migrations 00002 a 00006)

As migrations já publicadas (00001, 00002) **não são reescritas**; as correções são migrations novas. Para um banco criado
por versões anteriores, **não apague nada**: rode `migrate up` (no Compose: `docker compose up -d --build migrate api`).

* `00002_ledger_chain_and_result_integrity.sql` — proteção do ledger (cadeia contínua de saldos, vínculo
  lançamento→transação→carteira) e resultado não nulo em `PROCESSED`/`REJECTED`.
* `00003_completed_movement_requires_ledger.sql` — toda transação `PROCESSED` com movimento (`OPENING`, `BET`, `WIN`,
  `REFUND`, `ROLLBACK`) precisa, no COMMIT, do lançamento da própria carteira, e `result_balance` tem de ser o `balance_after`
  desse lançamento (`LOSS` e rejeições não têm lançamento).

* `00006_pending_shape_and_schedule_preservation.sql` — **versão final esperada pela aplicação (6)**; corrige dois achados da auditoria sem reescrever a 00005. (1) **Forma de `PENDING_REFERENCE` imposta no schema**: a referência externa, quando existe, precisa ter uma forma que a reidratação aceita (não vazia, ≤ 128 caracteres, sem caracteres de controle nem espaços nas pontas, diferente do próprio id externo) e `PENDING_REFERENCE` só existe para origem externa, tipo `WIN`/`REFUND`/`ROLLBACK` e com referência externa; o histórico incompatível é **recusado** (contagem e primeiros ids, sob exclusão dos escritores), nunca corrigido. (2) **Restaura a agenda preservada** por um rollback anterior (ver abaixo). Reversão: remove as garantias; não toca em dados.
* `00005_references_and_reversals.sql` — Instala no banco as garantias das reversões e das
  pendências (referência interna coerente com a externa e o escopo, alvo `PROCESSED` de tipo elegível, valor integral, sentido do
  `ROLLBACK`, referência resolvida para concluir, **uma reversão por alvo**, mutabilidade restrita das transações não terminais, e a agenda
  `pending_references`: `PENDING` nunca é confirmado e `PENDING_REFERENCE` sempre tem agenda aberta). Faz o mesmo `LOCK TABLE` da 00004 e, com os
  escritores excluídos, **recusa** (contagem e primeiros ids, nada alterado) transação confirmada como `PENDING`, reversão ou `WIN` com
  referência concluída sem a referência interna, referência incoerente, alvo com mais de uma reversão ou `ROLLBACK` com lançamento no sentido
  errado. O `Down` publicado dela remove as garantias e `DROP`a a agenda; por isso o migrador a **preserva antes** (ver "Rollback e a agenda das pendências"). Os privilégios do papel de runtime ganham `UPDATE` só nas colunas de
  transição (`migrate up` ou `migrate grants`).
* `00004_revalidate_history_under_writer_exclusion.sql` — janela entre validar e instalar fechada. Trava as tabelas financeiras
  (`LOCK TABLE ... SHARE ROW EXCLUSIVE`: espera escritores abertos, retém os novos, não bloqueia leituras), relê **todo** o
  histórico e confere que os triggers das garantias existem e estão habilitados. Fecha a janela entre validar e instalar das
  migrations anteriores e recusa bancos onde ela já ocorreu. Só valida: não instala nada nem altera dados.

Cada uma começa com uma **verificação do histórico**: se houver registros incompatíveis (cadeia quebrada, resultado nulo,
lançamento sem transação coerente; na 00003, transação concluída sem lançamento ou com resultado divergente, com contagem e
primeiros ids) a migration **aborta sem alterar dados nem o schema** — a correção de registros históricos é manual, depois
rode `migrate up` de novo. Reversão: `migrate down` (um passo por vez) remove as garantias da última migration (a 00004 não
instala nada: reverter só retrocede a versão) e **não altera dados**.

**Procedimento de upgrade** (detalhes e justificativa em `ARCHITECTURE.md` §10.3):

```sh
docker compose stop api                 # recomendado: parar todos os escritores (todas as instâncias da API)
docker compose run --rm migrate up      # papel dono do schema; espera escritores abertos e valida sob exclusão
docker compose run --rm migrate status  # a versão final (00006) precisa constar como aplicada
docker compose up -d --build api        # a API só inicia na versão final do schema
```

* O lock de sessão do migrador só evita **duas migrations** simultâneas; quem protege contra escritores é a exclusão por
  `LOCK TABLE` da 00004. Mesmo com a API ativa o resultado é correto para escritores do papel de runtime; parar os escritores
  reduz a retenção e é a precondição para qualquer outro escritor (manutenção manual, ETL, outro papel).
* Se a 00004 ou a 00005 abortar (histórico incompatível, trigger ausente/desabilitado ou escritor aberto por mais de 30 s), o schema fica na
  versão anterior (p.ex. 4), nada é apagado e **a API recusa iniciar** (`schema is at version 5, application expects 6`). Corrija os registros
  indicados manualmente (contagem e primeiros ids na mensagem) ou repita sem escritores abertos, e rode `migrate up` de novo.
* Limites: um escritor com privilégio de dono/superusuário pode ignorar triggers (só a validação o enxerga, e só no instante da
  migration); upgrade online sem nenhuma retenção não é objetivo.

Papel de runtime em bancos antigos: o primeiro `migrate up` desta versão (com `RUNTIME_DB_ROLE`) cria o `wager_app`; depois
troque `DATABASE_URL` da API para ele (o Compose já faz isso).

### Rollback e a agenda das pendências

A agenda (`pending_references`: início, vencimento, próxima tentativa, tentativas, diagnóstico e as agendas concluídas) é **dado durável**,
não estado descartável: reverter e reaplicar não pode apagá-la nem renovar um prazo vencido. Como a 00005 já foi publicada, o `Down`
dela (que remove a tabela) e o `Up` (que reconstrói linhas com prazo novo) **não foram reescritos**; a solução compatível é um procedimento do
migrador (`Migrator`, usado por `cmd/migrate`) que **preserva ou recusa antes de qualquer perda**:

0. **Coordenação.** Toda operação do migrador (`up`, `down`, `down-to` e `grants`) roda sob um advisory lock próprio (`MigratorLockID`, distinto do lock do goose, que
   o provider adquire por dentro, sempre depois: ordem única, sem deadlock), adquirido por tentativas — um comando cancelado (Ctrl-C) ou morto não deixa sessão na fila. A leitura da versão, a
   preparação, a reversão, a restauração e a concessão dos privilégios não se intercalam com as de outro migrador: `migrate grants` **espera** um `down` em andamento (nunca reabre a escrita no
   meio dele) e `migrate up` com `RUNTIME_DB_ROLE` faz o `Up` e a provisão na mesma operação.
1. **Reversão operacional** — `migrate down` (um passo) e `migrate down-to N` com `N > 0`: antes de reverter a 00005, o migrador executa a **preparação**
   sob exclusão dos escritores (`LOCK TABLE … SHARE ROW EXCLUSIVE`, mesma ordem das migrations), numa única transação:
   * captura e **revoga** os privilégios sobre a agenda de todo papel que não seja o dono — de tabela **e de coluna**, inclusive os concedidos a `PUBLIC`
     (`pg_class.relacl` e `pg_attribute.attacl`) — e **verifica no catálogo** o efeito: se algum papel de login (que não seja superusuário, o dono nem a sessão do
     migrador) ainda conseguir escrever, direta, herdada ou por pertencer a um papel com escrita (p.ex. `pg_write_all_data`), o rollback é **recusado**,
     com os nomes, sem alterar nada;
   * guarda uma **cópia integral** em `pending_references_preserved` (acessível só ao dono). A cópia **original nunca é substituída por uma reconstrução**:
     se já existe uma cópia e a agenda viva é a mesma tabela que foi copiada (um `down` anterior falhou ou foi cancelado), a cópia é renovada a partir da agenda viva;
     se a agenda viva é a reconstrução criada pelo `Up` da 00005 (reaplicação parcial), a cópia original é mantida e o rollback só prossegue se a agenda viva for
     a reconstrução **intacta** (nenhuma linha fora da cópia, sem tentativas, diagnóstico nem conclusão) — caso contrário é **recusado**, com contagem e ids, sem alterar nada.
   Reverter a 00006 e as posteriores não toca em dados.
2. **Reaplicação** — `migrate up`: antes da 00006 (e com a escrita dos papéis que não são o dono **revogada da decisão até a restauração terminar**, privilégios devolvidos ao final: uma retomada do runtime nessa janela é
   negada em voz alta, nunca confirmada para ser sobrescrita), se a agenda viva é a **mesma tabela** que foi copiada (um `down` foi cancelado antes de apagá-la), a cópia defasada é descartada para não
   sobrescrever o trabalho que o runtime confirmou depois; se a agenda viva é uma reconstrução alterada, o `up` é **recusado** antes de qualquer perda. No caso normal, o `Up` da 00005
   reconstrói, com prazo renovado, uma linha para cada `PENDING_REFERENCE` (comportamento publicado) e a **00006 restaura por cima delas a cópia preservada** — valores originais, inclusive as agendas concluídas — e a remove. Uma cópia que não
   corresponde a nenhuma transação (p.ex. após um reset) **bloqueia a restauração** com contagem e ids, sem descartá-la em silêncio; se a restauração falhar, a cópia permanece.
3. **Reset deliberadamente destrutivo** — `migrate down-to 0` (e a suíte de testes): apaga todas as tabelas e dados, como sempre, **inclusive** a cópia preservada.
4. **Falha ou cancelamento depois da preparação**: a agenda original continua íntegra e o próprio migrador **devolve os privilégios** capturados (se a agenda
   ainda existe). Se o processo morrer antes disso, `migrate grants` (ou um `migrate up` com `RUNTIME_DB_ROLE`) os devolve, depois que a operação anterior terminou.

Limites: a proteção está no migrador; executar o `Down` da 00005 diretamente pelo goose, sem passar por ele, não é protegido. A exclusão dos escritores é por
privilégio: o papel **dono** e superusuários não são excluídos (são o operador), e uma visão ou função `SECURITY DEFINER` de outro dono que escreva na agenda não é detectada. Pare as
instâncias da API antes de um rollback (como no upgrade) — a revogação impede que escrevam, mas elas falharão em voz alta até a restauração. Pendências que nunca tiveram agenda
(criadas por SQL numa versão anterior à 00005) recebem, no `Up` da 00005, a agenda reconstruída publicada — não há dado original a restaurar.

## Executar a aplicação sem Docker para a API

```sh
docker compose up -d --wait postgres keycloak ministack && docker compose run --rm broker-provision   # ou: make infra
set -a; . ./.env.example; set +a   # AWS_PROFILE=wager-runtime + .local/broker/credentials
go run ./cmd/migrate up      # como dono do schema; cria o papel wager_app
go run ./cmd/api             # como wager_app (DATABASE_URL)
```

## Testes

```sh
go test ./...            # unitários (domínio, config, coordenador de shutdown) — não precisam de infraestrutura
go test -race ./...
go vet ./...
```

### Integração (PostgreSQL, Keycloak e MiniStack reais)

Preparação — `make infra` (ou `docker compose up -d --wait postgres keycloak ministack && docker compose run --rm broker-provision`). A suíte lê as credenciais em `BROKER_CREDENTIALS_FILE` (padrão `.local/broker/credentials`), roda a aplicação com o perfil `wager-runtime` e **recusa** executar se o endpoint (`AWS_ENDPOINT_URL`) não for um MiniStack. Os testes usam o banco de
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
| WIN e LOSS | `win_loss_test.go` | `WIN` credita uma vez (ledger `CREDIT`, `WagerTransactionProcessed` + `WalletBalanceChanged` com causalidade e `walletVersion`); overflow sem efeitos parciais; `LOSS` sem ledger, sem alterar saldo/versão e só com `WagerTransactionProcessed`; replays com o saldo original; 50 reenvios de `WIN`/`LOSS` em paralelo; `LOSS` espera o lock da carteira; `WIN` com referência é processado como as reversões (200 ou 202, ver as linhas Reversões e Referências pendentes); **3 processos reais** (reenvios e créditos distintos sem perda) |
| Idempotência | `idempotency_test.go` | replay devolve o saldo original após outras movimentações; rejeição terminal não é reavaliada após crédito; conflito de payload; mesma operação externa com outra chave; chaves por provedor; **conflito tem precedência sobre carteira inexistente/jogador/moeda divergentes**; disputa da mesma identidade entre carteiras diferentes |
| Precedência da identidade | `identity_precedence_test.go` | caso de uso e HTTP: chave ou id externo já registrado com **moeda não habilitada ou tipo diferente (WIN/LOSS/REFUND/ROLLBACK)** → 409; entradas estruturalmente inválidas continuam 400/422; operações novas continuam 422/501; escopo por provedor; 401/403 antes de tudo; nada é persistido |
| Concorrência | `concurrency_test.go` | 50 reenvios paralelos → 1 débito; 2×80,00 sobre 100,00 (com reenvios) → 1 processada, 1 rejeitada, saldo 20,00; sem lost update; carteiras independentes não se bloqueiam; várias carteiras em paralelo |
| Movimento concluído | `completed_movement_test.go` | **pelo papel restrito**, com payloads de hash válido: `BET PROCESSED` sem lançamento nem alteração da carteira, `OPENING` sem lançamento, `PENDING`→`PROCESSED` sem lançamento e resultado diferente do `balance_after` do lançamento (777,00 × 90,00; zero × 90,00) são recusados no COMMIT; várias operações válidas na mesma transação SQL guardam o resultado próprio e o replay (HTTP) devolve o saldo original; `LOSS` e rejeições seguem sem lançamento |
| Banco | `constraints_test.go`, `db_integrity_test.go` | SQL direto: unicidade, saldo não negativo, ledger append-only (UPDATE/DELETE/TRUNCATE), saldo×ledger no mesmo commit, OPENING único, forma interna×externa, política de zero, imutabilidade de terminais e da outbox; **cadeia contínua do ledger** (cenário da revisão, primeiro lançamento, vários lançamentos por transação, escritor concorrente obsoleto, carteira independente não bloqueada); **vínculo lançamento→transação→carteira**; **resultado explícito (NULL e valores inválidos)** |
| Papéis | `role_test.go` | o papel de runtime não é superusuário nem dono, e 20+ tentativas (UPDATE/DELETE/TRUNCATE do ledger, `DISABLE TRIGGER`, `session_replication_role`, DDL…) são negadas; o fluxo financeiro e a verificação de schema funcionam com ele; provisão idempotente |
| Upgrade com escritores ativos | `upgrade_window_test.go` | sondagem da auditoria: escritor antigo com `BET PROCESSED` sem lançamento aberto → upgrade espera → commit → o schema **nunca** termina na versão final com o órfão (relata o id, nada é apagado, a API recusa iniciar; corrigido manualmente, o upgrade conclui); escritor retido e escritor válido concorrentes; histórico já inválido (órfã, resultado, cadeia, saldo, lançamento de rejeitada); trigger ausente/desabilitado; escritor que ignora triggers; espera/retenção pelo lock; dados preservados; reversão/reaplicação |
| Migrations | `migration_test.go`, `migration_movement_test.go` | as corretivas preservam dados de um banco existente (e na reversão), recusam sem alterar nada histórico com cadeia quebrada, resultado nulo, transação concluída sem lançamento ou resultado divergente (relatando contagem e ids), e aplicar→reverter→reaplicar funciona |
| Autenticação | `auth_test.go` | credenciais ausentes, adulteradas, forjadas, `alg:none`, HS256, expiradas (token real de 3 s), audiência errada → 401; sem papel → 403; isolamento entre provedores (consulta, replay, headers falsos); nenhuma alteração de estado em acessos negados |
| Falhas | `faults_test.go` | falha no meio da transação não deixa estado parcial; ledger "esquecido" é barrado pelo banco no COMMIT; commit confirmado com ACK perdido → 503 e recuperação com a mesma chave |
| Ciclo de vida | `lifecycle_test.go` | reinício preserva idempotência/resultados; grafo Fx válido, start/stop e liberação de conexões; **7 falhas de partida (inclusive schema ausente/desatualizado e credencial inválida) sem conexão PostgreSQL remanescente**; shutdown conclui requisição em andamento |
| Reversões | `reversal_test.go`, `reversal_http_test.go` | cadeia `BET → REFUND → ROLLBACK do REFUND` (saldo, ledger, eventos, replays após novas movimentações) e novas reversões `ALREADY_REVERSED` sem ledger; `ROLLBACK` de `BET`/`WIN`; `WIN` com referência de valor diferente sem consumir a reversão; reversão sem saldo (`REVERSAL_INSUFFICIENT_FUNDS`, a rejeição não consome o alvo); referências incompatíveis (valor, tipo, rodada, carteira, `LOSS`, `REJECTED`); **`REFUND` × `ROLLBACK` simultâneos**: um sucesso, uma rejeição, um crédito (15 rodadas); contrato HTTP (200/202/422) |
| Pendências | `pending_references_test.go`, `shutdown_test.go` | aceite com agenda e evento únicos, prazo que não renova no replay/tentativa, resolução pela mesma operação, resolução **depois do TTL**, expiração terminal que não reabre, `REFERENCE_PENDING_TIMEOUT`, referência que termina sem sucesso, falha de consulta que **não** vira `REFERENCE_NOT_FOUND`, falha antes/depois do commit (resultado desconhecido) sem duplicar, 10 executores na mesma pendência, carteira ocupada pulada e outras progredem, relógio atrasado; o TTL é atravessado com **relógio injetado** (sem dormir) |
| Garantias das referências no banco | `reference_constraints_test.go`, `migration_references_test.go` | SQL direto pelo **papel restrito**: referência fora do escopo/tipo/estado/valor, formas `NULL`/incompletas, segunda reversão (índice único), `ROLLBACK` com lançamento no sentido errado (no COMMIT), colunas imutáveis (privilégio e trigger), `PENDING`/`PENDING_REFERENCE` sem agenda, agenda sem exclusão/alteração de prazo; migration 00005: dados preservados, histórico incompatível recusado (5 formas), reversão/reaplicação, pendências sobreviventes |
| Correções da auditoria do incremento 2 | `audit_regressions_test.go` | (1) carteira ocupada no início da varredura não impede o avanço de outra (≥ `BatchSize` pendências na ocupada; várias instâncias; varredura toda ocupada termina com poucas consultas, sem tocar nenhuma agenda); (2) candidato antigo não contorna o backoff reagendado por outra instância (sincronização pelos canais do decorador de leitura); (3) rollback/reaplicação preservam a agenda (inclusive concluídas e prazo vencido), a janela da reversão da 00005 é observada (escrita revogada, cópia completa), `down-to 0` é destrutivo, cópia órfã bloqueia a restauração; (4) o banco recusa `PENDING_REFERENCE` de BET/LOSS/OPENING/WIN sem referência e referências com formas inválidas (NULL, vazia, espaços, controle, longa), aceita as válidas e as retoma, e a migration recusa histórico inválido com ids. As varreduras usam **bancos exclusivos** do teste |
| Correções da reauditoria do incremento 2 | `reaudit_regressions_test.go` | (1) uma carteira livre avança atrás de **120 carteiras ocupadas distintas** com `BatchSize=1` (muito além de qualquer orçamento de páginas), sem tocar nas ocupadas, e várias instâncias concorrentes a avançam **uma só vez**; (2) `DownTo(4) → UpTo(5) → Down → Up` e `Down` cancelado com o lock do goose retido por outra sessão **preservam a cópia original** (TTL de 7 min, vencida, 7 tentativas e diagnóstico, agenda concluída; todas as colunas), a escrita do runtime é devolvida pelo migrador, uma repetição renova a cópia a partir da agenda viva, um rollback que perderia dados é **recusado sem alterar nada** (API e binário `migrate`) e um segundo migrador espera o primeiro; (3) papel só com grants **por coluna**, `PUBLIC` por coluna e papel que herda de um grupo ficam sem escrita durante a janela, o runtime padrão continua bloqueado e recupera os grants ao cancelar, e um escritor que não pode ser excluído recusa o rollback antes de qualquer alteração |
| Correções da reauditoria focada (2109d38) | `reaudit2_regressions_test.go` | (1) carteira livre avança atrás de 60 carteiras seguradas por **cada** modo de lock (`FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE`, `FOR KEY SHARE`, `UPDATE` não chave) e a transação do prefixo confirma com as constraints; (2) falha localizada numa carteira (lotes 1 e 20): a livre avança, a pendência com falha continua `PENDING_REFERENCE`, é adiada com backoff e diagnóstico e o retry segue pelo estado durável; falha geral para após 3 falhas seguidas; (3) `Up`/`UpTo` direto depois de `Down` cancelado preservam o trabalho confirmado pelo runtime na agenda viva (tentativas 8, não 7), o ciclo seguinte ainda preserva e uma reconstrução alterada recusa o `Up` sem mudar nada; (4) o binário `migrate grants` e `migrate up` esperam um `Down` preparado, o runtime continua negado enquanto esperam, o cancelamento (SIGINT) termina sem vazar lock, e após `SIGKILL` do migrador `grants` e `up` recuperam sem perder trabalho |
| Correções residuais da base (06c6c65) | `reaudit3_regressions_test.go` | (1) três agendas seguradas por `UPDATE`s reais do runtime (sem lock das carteiras, lotes 1 e 20, `PhaseTimeout` de 2 s): a primeira varredura conclui a carteira livre, sem erro e sem esperar, e as seguradas voltam a ser retomadas ao serem liberadas; com tentativa **e** adiamento falhando (4 e 7 carteiras), a saudável avança em poucas chamadas pela posição de continuação, sem nada persistido para o prefixo; falha geral custa ≤ 3 tentativas por chamada; (2) `DownTo(4) → UpTo(5) → migrate grants → Up/UpTo(6)`: com a decisão confirmada e a restauração retida, o runtime não tem escrita, uma retomada real é negada, a restauração traz a agenda original, os privilégios voltam e o trabalho seguinte funciona |
| Ledger paginado | `ledger_reads_test.go`, `internal/app/ledger_test.go` | percurso completo por várias páginas e limites (1, 3, 7, 8, 9, 100; bordas `limit` = restantes e +1; padrão 50) em ordem de `seq`, sem vazamento de outra carteira; `limit`/`cursor` inválidos, repetidos ou de **outra carteira** → 400 sem dados; carteira zero → `items:[]`, inexistente → 404; query malformada (`limit=%ZZ`, `cursor=%ZZ`, repetição com escape inválido, separador `;`, cursor válido repetido com `%ZZ`, parâmetro desconhecido malformado) → 400 `INVALID_QUERY` sem dados, e 401/403 antes dela; `createdAt` iguais e gaps de `seq`; lançamentos novos no meio da navegação (sem perda nem duplicação, sem sessão `idle in transaction`); 6 escritores paralelos na mesma carteira (toda travessia é prefixo da ordem final); cursor: forma canônica, versão, posição, tamanho |
| Reconciliação | `reconciliation_test.go`, `internal/app/ledger_test.go` | carteira zero, abertura, o exemplo do enunciado (100,00 + BET 25,00 → 75,00/75,00/0,00, 2 lançamentos), cadeia BET/WIN/LOSS/rejeição/REFUND/ROLLBACK, estado intacto após ler; **movimento confirmado entre as consultas** (barreira): resposta de um único momento e escritor sem espera; somas separadas **acima de `int64`** com saldo final `MaxInt64` (exato); divergência real em banco exclusivo (resposta, log, sinal, nada alterado, diferença negativa); soma fora de `Money` → falha (não divergência); falha de leitura → erro; aritmética em unidades (`big.Int`) |
| Shutdown coordenado | `internal/lifecycle/coordinator_test.go`, `shutdown_test.go` | ordem (admissão em todos → drenagem → cancelamento → confirmação), término incompleto, falha de partida; com infraestrutura real: requisição presa no lock é recusada/drenada/cancelada e o **pool só fecha depois**, tentativa do worker presa é cancelada e outra instância conclui, partida com porta ocupada sem vazar conexões, configuração incoerente recusada |
| Multiprocesso | `multiprocess_test.go`, `win_loss_test.go`, `references_multiprocess_test.go` | **3 processos do binário real**: 50 reenvios espalhados, disputa 2×80,00 entre processos, conexões próprias, `SIGTERM` com saída 0; `SIGKILL` de um processo no meio da carga sem duplicar nem perder débito; reenvios de `WIN`/`LOSS`; **reversões pendentes resolvidas pelos executores dos três processos** (por carteira: uma processada, uma rejeitada, um crédito), `SIGKILL` do processo que aceitou a pendência, expiração com três executores (uma rejeição, um evento). O binário dos filhos é compilado **sem `-race`** (pendência registrada) |

## Integração contínua

`.github/workflows/ci.yml` roda em pull requests e no `main`; todos os jobs precisam passar para o merge:

| Job | O que executa |
| --- | --- |
| `verify` | `gofmt`, `go mod verify`, `go mod tidy` sem diferenças, `go vet` (com e sem `-tags integration`), `go test -race` |
| `infra` | `docker compose up --wait` de PostgreSQL, Keycloak e MiniStack, `broker-provision` (duas vezes: as credenciais não mudam) + `scripts/check-infra.sh` (tokens, papéis, `provider_id`, filas, redrive, destino dos eventos, política de entrega, políticas dos principais) |
| `integration` | os testes de integração com `-race` contra PostgreSQL, Keycloak e MiniStack reais, inclusive a prova de autorização do broker |
| `stack` | `docker compose up --build` (imagem, migrations, papel de runtime, API), espera a API ficar `healthy` (`scripts/wait-healthy.sh`: estado estruturado do `docker inspect`, comparação exata, com prazo e diagnóstico) + `scripts/smoke.sh` |

Os mesmos scripts rodam localmente: `scripts/check-infra.sh` (com a infraestrutura de pé), `scripts/wait-healthy.sh <serviço> [segundos]` e `scripts/smoke.sh` (com o stack de pé).
Para validar o Compose sem mexer no seu ambiente, use outro projeto e outras portas:

```sh
export POSTGRES_PORT=15433 KEYCLOAK_PORT=18081 BROKER_PORT=14566 API_PORT=18080 BROKER_DIR=/tmp/wager-check-broker   # credenciais do broker isoladas
docker compose -p wager-check up --build -d
API_URL=http://localhost:18080 KEYCLOAK_URL=http://localhost:18081 scripts/smoke.sh
docker compose -p wager-check down -v      # remove apenas o projeto de verificação
```

## Estrutura

```
cmd/api            servidor HTTP (Fx)            cmd/migrate        CLI de migrations e do papel de runtime
internal/domain    Money, Wallet, WagerTransaction, LedgerEntry, eventos, hash canônico (sem Fx/HTTP/SQL)
internal/app       casos de uso (OpenWallet, Submit, consultas) e portas de persistência
internal/lifecycle coordenador de shutdown (admissão, drenagem, cancelamento, confirmação de término)
internal/infra/    postgres (pgx, unit of work, repositórios, migrations) · auth (JWT/JWKS) ·
                   httpapi (handlers, erros, health, servidor) · sqs (readiness)
internal/bootstrap composição com fx.Module e fx.Lifecycle
deploy/            realm do Keycloak
cmd/broker-provision provisionamento idempotente do broker (filas, tópico, identidades, políticas, credenciais)
internal/infra/broker provisionamento e credenciais por responsabilidade
scripts/           check-infra.sh (ambiente) e smoke.sh (fluxo ponta a ponta pela API)
.github/workflows  CI
test/integration   testes com infraestrutura real (build tag `integration`)
```

## Escopo e pendências

**Implementado e testado (HTTP):** configuração validada; Fx com lifecycle e **shutdown coordenado**; Docker Compose; Keycloak e
MiniStack provisionados; migrations; health checks reais; Money; Wallet; WagerTransaction (máquina
de estados completa no domínio); LedgerEntry; abertura de carteira; **`REFUND`, `ROLLBACK` e `WIN` com referência**, **referências pendentes
(`PENDING_REFERENCE`) com agenda durável, TTL, backoff e worker**, uma reversão por alvo imposta no banco; `BET` com idempotência
persistente, rejeição por saldo, ledger e outbox atômicos; `WIN` (sem referência) e `LOSS`; consultas com isolamento por provedor;
autenticação/autorização reais. **Garantias do banco** (migrations 00001–00003, verificadas por SQL direto pelo papel
restrito): ledger append-only e de cadeia contínua; saldo = último `balance_after`; toda transação `PROCESSED` com movimento
exige, no COMMIT, o lançamento da própria carteira e `result_balance` = `balance_after` dele (`LOSS` e rejeições não têm
lançamento); unicidade de identidade; terminais imutáveis. **Precedência da identidade:** chave ou id externo já registrado é
replay/conflito (409) antes das políticas de operação nova (moeda, tipo).

**Não implementado (explícito):**

| Item do enunciado | Situação |
| --- | --- |
| Consumidor SQS e tabela de inbox | ausentes (a fila é provisionada e a readiness a verifica) |
| Publicador de outbox (múltiplas instâncias, backoff, recuperação) | ausente; eventos ficam gravados em `outbox_events` com colunas de tentativas/próximo envio |
| Métricas (resultados, duplicatas, retries, DLQ, atraso da outbox, reconciliação) | ausentes (a reconciliação já expõe o ponto de integração e o log de divergência); há logs JSON com `correlationId`, `transactionId`, `walletId`, `providerId` |
| Controle de acesso do broker (credenciais e políticas) | **provado como autorização emulada** (MiniStack 1.5.27, `AUTH=true`; prova na CI). O MiniStack **não valida assinaturas SigV4**: não é autenticação AWS completa nem prova de produção (ver [Broker](#broker-minestack-com-auth-true)). O produtor/consumidor SQS ainda **não** existem |
| Testes SQS/HTTP cruzados, DLQ, interrupção de consumidor, dois publishers | dependem dos itens acima |
| Prova com 3 processos para SQS/outbox | HTTP, reversões e pendências já têm; SQS/outbox dependem dos itens acima |
| Testes de carga, OpenTelemetry, partidas dobradas | opcionais, não feitos |
| Limites das garantias atuais | o dono do schema e superusuários podem desabilitar triggers (a API roda com papel restrito); o `result_balance` de `LOSS` (saldo observado, sem lançamento) não é verificável contra o ledger; o sentido do lançamento de `ROLLBACK` é imposto no banco desde a 00005 (ARCHITECTURE §10); a regressão de relógio entre instâncias é recusada pelo domínio e tratada por quem chama a transição (ARCHITECTURE §3); rotação de chaves do IdP pode causar 401 por até 30 s |

**Próximo incremento recomendado:** publicador da outbox (reserva com lease e token, publicação fora da transação, SNS FIFO); em seguida a
inbox + consumidor SQS sobre o mesmo núcleo financeiro (`Service.processExternal`). A infraestrutura do broker já está decidida e provada (MiniStack 1.5.27, `AUTH=true`; ver "Broker").
Detalhes em `ARCHITECTURE.md`.
