# ARCHITECTURE

Este documento registra as decisões do **primeiro incremento**. Cada decisão aponta se é
**requisito explícito** do enunciado (`descricao-desafio/README.md`), **interpretação** adotada, **decisão
técnica** ou **pendência**. O estado de implementação está no fim e no README.

## 1. Visão geral

```
            ┌───────────────┐   JWT (client_credentials)   ┌──────────┐
 provedor ─►│  httpapi      │◄─────────────────────────────│ Keycloak │ (JWKS)
 interno  ─►│  auth (JWT)   │                              └──────────┘
            └──────┬────────┘
                   │ app.Service  (OpenWallet · Submit · consultas)
            ┌──────▼────────┐        ┌──────────────────────────────┐
            │  app (casos   │───────►│ domain: Money · Wallet ·     │
            │  de uso)      │        │ WagerTransaction · Ledger ·  │
            └──────┬────────┘        │ eventos · hash canônico      │
                   │ UnitOfWork      └──────────────────────────────┘
            ┌──────▼────────┐
            │ postgres (pgx)│  uma transação SQL: carteira + transação + ledger + outbox
            └───────────────┘
```

* **Domínio** (`internal/domain`) não importa Fx, HTTP, SQS nem pgx. Depende só da biblioteca padrão e de `google/uuid`.
* **Aplicação** (`internal/app`) orquestra casos de uso e define as portas (`UnitOfWork`, repositórios).
* **Infraestrutura** (`internal/infra/*`) implementa as portas e as entradas. **Entradas**: HTTP agora; SQS depois,
  chamando o mesmo `app.Service.Submit`.
* Sem hierarquias genéricas: uma interface por necessidade concreta (`UnitOfWork`, os quatro repositórios,
  `httpapi.Service`/`TokenVerifier` para os handlers). Divergência das recomendações iniciais do prompt: **nenhuma**.

## 2. Dinheiro (requisito §5.1, §6.1)

* `domain.Money` é um value object imutável: **`int64` em centavos + moeda** (escala fixa de 2). Faixa
  ±92.233.720.368.547.758,07; `math.MinInt64` é recusado na construção para que a negação nunca transborde.
* **Nenhum `float32/float64`** em parsing, cálculo, JSON ou persistência. Parsing é por *string*, byte a byte
  (`parseMinor`); o JSON exige `amount` como **string** (número JSON é recusado); o banco usa `BIGINT`
  (um teste varre `information_schema` e falha se houver `real/double/numeric/money`).
* **Gramática aceita** (interpretação): `0 | [1-9][0-9]*` com fração opcional de **1 ou 2** dígitos. Logo
  `"25"`, `"25.5"` e `"25.50"` são aceitos e **normalizados** para `25.00`/`25.50` antes do hash de idempotência.
  Recusados: vazio, espaços, `+`, `NaN`, `Infinity`, notação científica, zeros à esquerda, `1.`/`.5`, mais de duas
  casas (`ErrAmountScale`, nunca arredonda), negativos nas entradas externas (`ErrAmountNegative`) e overflow
  (`ErrAmountOverflow`, detectado no parsing, `Add`, `Sub` e `Neg`).
* **Zero inicializado ≠ não inicializado**: `Money{}` é inválido (`ErrUninitializedMoney` em toda operação);
  `Zero(currency)` é o zero válido. `Equal` entre valores não inicializados é falso.
* Moedas: formato `[A-Z]{3}` no domínio; o conjunto aceito é política (`ALLOWED_CURRENCIES`, padrão `BRL`).
  Aritmética entre moedas diferentes falha com `ErrCurrencyMismatch`. O banco também impede divergência
  (FKs compostas `(wallet_id, currency)`).
* **Política de zero por tipo** (`Kind.ValidateAmount`): `LOSS` exige `0.00`; `BET/WIN/REFUND/ROLLBACK` exigem
  `> 0`; saldo inicial aceita `0`. Espelhada em `CHECK` no banco.

## 3. Domínio e encapsulamento (§6)

* Estado privado, construtores validados e transições explícitas. Criação e reidratação são caminhos distintos:
  `NewWallet` / `RehydrateWallet`, `NewExternalTransaction` / `RehydrateWagerTransaction`,
  `NewLedgerEntry` / `RehydrateLedgerEntry`. A reidratação valida coerência, **não** reaplica movimentos,
  transições ou eventos (testado).
* **Wallet**: `Debit`/`Credit` devolvem o `LedgerEntry` correspondente e incrementam a versão só quando o saldo
  muda; falha de débito (`ErrInsufficientFunds`) não altera o estado. Versão inicial `1`; o crédito de abertura
  **não** incrementa (a abertura é o estado de criação), como o enunciado pede.
* **WagerTransaction** — máquina de estados (validada no domínio e, para terminais, no banco por trigger):

  ```
  PENDING ─► PROCESSED | REJECTED | FAILED          (terminais)
  PENDING ─► PENDING_REFERENCE ─► PROCESSED | REJECTED | FAILED
  ```

  Falha **transitória** (banco indisponível, timeout, commit incerto) **aborta a tentativa** e não gera estado: o
  cliente repete com a mesma chave. `FAILED` é reservado a falha **permanente** de infraestrutura que precise ficar
  auditável (ex.: mensagem esgotada na DLQ, no incremento do SQS). `REJECTED` = regra de negócio, com `failureCode`.
  **Validação e encapsulamento**: `ExternalRequest` é imutável (campos privados, só getters, único construtor
  `NewExternalRequest`) e seu hash é derivado dos campos validados, portanto campos de negócio e hash não divergem;
  `NewExternalTransaction` ainda revalida a requisição (inclusive a coerência com o hash) e rejeita o valor zero.
  `RehydrateWagerTransaction` valida o estado completo — tipo, política de valor por tipo, moeda e sinal do resultado,
  metadados externos, política de referência, **recálculo do hash a partir dos campos** e a relação entre estado,
  `failureCode` e resultado (PROCESSED/REJECTED exigem resultado; PENDING* não o têm) — sem reaplicar movimentos.
  **Política temporal única** (criação, transições e reidratação, `checkTimestamps`): `createdAt` e `updatedAt` não são nulos,
  `updatedAt >= createdAt` e `updatedAt` **nunca regride**. `Process`, `Reject`, `Fail` e `AwaitReference` validam estado de
  origem, destino, argumentos e instante **antes** de alterar qualquer campo; um instante zero, anterior à criação ou anterior à
  última atualização é recusado com `ErrInvalidTimestamp` (que também é `ErrInvalidTransaction`) e a entidade permanece intacta
  (status, resultado, código, referência e timestamps). O mesmo instante da última atualização é válido, pois etapas do mesmo
  commit usam o mesmo relógio. Decisão sobre regressão do relógio: o domínio **não ajusta** o instante silenciosamente; uma
  instância com relógio atrasado em relação à última atualização gravada por outra deve repetir mais tarde ou informar o maior
  entre o seu relógio e `UpdatedAt()`. Como o estado persistido só guarda a última atualização, a reidratação verifica apenas
  `updatedAt >= createdAt`. A regra cronológica é uma decisão de domínio, não uma exigência literal do enunciado.
  Referências (`*uuid.UUID`) são copiadas na entrada da reidratação e em cada leitura (`ReferenceTransactionID()`,
  `External()`), então nenhum chamador consegue alterar uma transação terminal por alias.
* **LedgerEntry**: `balanceAfter = balanceBefore ± money` validado na construção (e por `CHECK` no banco).
* **Eventos** (`events.go`): tipos concretos com tipo e versão definidos pelo construtor
  (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WalletBalanceChanged`, `WagerTransactionPendingReference`).
  Envelope: `eventId, eventType, aggregateId, correlationId, causationId?, occurredAt (UTC, ms), version, data`.
  `WalletBalanceChanged.data` = `walletId, transactionId, direction, money, balanceBefore, balanceAfter, walletVersion`.
  Eventos de abertura (origem interna) omitem os metadados externos.
* `domain.OpenWallet` agrega o que vai no mesmo commit: carteira, `OPENING` `PROCESSED`, crédito (saldo anterior zero)
  e os dois eventos; **saldo zero cria apenas a carteira** (sem OPENING, ledger ou eventos).

## 4. Transações SQL e concorrência (§8)

* **Biblioteca**: `pgx/v5` com SQL explícito (sem ORM). `Store` implementa `app.UnitOfWork`:
  `Do(ctx, fn)` abre **uma** transação `READ COMMITTED` e entrega a `fn` repositórios (`Wallets`, `Transactions`,
  `Ledger`, `Outbox`) **vinculados à mesma `pgx.Tx`**. Leituras sem transação usam `Reader()` (repositórios sobre o pool).
  Os repositórios dependem de uma interface mínima (`Exec/Query/QueryRow`) satisfeita por pool e transação.
* **Mapeamento de Money**: `BIGINT` em centavos + `currency TEXT`; reconstruído com `FromMinor`.
* **Locking pessimista por carteira**: `SELECT … FROM wallets WHERE id = $1 FOR UPDATE` no início de toda operação
  financeira. Operações de carteiras distintas não se bloqueiam (sem lock global; um teste mantém o lock da carteira A
  e prova que a B conclui). O lock é por linha e dura até o fim da transação.
  Escolhido em vez de otimista porque a invariante "saldo ≥ 0 com decisão determinística" precisa de ordem total por
  carteira, e retries otimistas sob disputa (50 reenvios, 2×80) só adicionariam latência e 409s espúrios.
* **Segunda barreira (defesa em profundidade)**: `UPDATE … SET balance, version … WHERE id=$1 AND version=$esperada`
  (compare-and-swap); linhas afetadas ≠ 1 → `ErrConcurrentModification` (503, seguro repetir). Removendo o `FOR UPDATE`
  numa mutação de teste, os testes de concorrência falham — e o CAS impede a atualização perdida silenciosa.
* **Banco como árbitro final** (§8 do §5): ver §10.
* **Falha de commit**: se o `COMMIT` retorna erro **do servidor** (`PgError`, p.ex. constraint deferida), a transação
  foi desfeita. Se não há resposta (timeout, conexão perdida), o resultado é **desconhecido**:
  `app.ErrCommitUnknown` → HTTP 503 `COMMIT_OUTCOME_UNKNOWN`. A "identidade para recuperação" é a
  `Idempotency-Key` + `(providerId, externalTransactionId)`: repetir a requisição devolve o resultado original se o
  commit ocorreu, ou o processa se não ocorreu (teste com ACK perdido).

## 5. Idempotência (§5.2, §9)

**Fluxo de `Submit`** (uma transação SQL):

1. Analisa, valida e **normaliza** a entrada (`NewExternalRequest`): entradas estruturalmente inválidas (tipo desconhecido ou `OPENING`,
   valor/moeda malformados, política de zero e de referência por tipo, identificadores) são recusadas aqui, mesmo que a identidade
   exista. Nada que dependa de política ou de estado é avaliado ainda.
2. **Identidade primeiro**, sem lock, numa **única consulta** (`FindByIdentity`: chave *ou* id externo, um snapshot — duas
   consultas separadas poderiam ver um commit concorrente pela metade e reportar conflito para um reenvio legítimo).
   Mesma chave: hash igual → **replay** do resultado persistido (`idempotentReplay:true`, saldo **original**, inclusive para
   `REJECTED`); hash diferente → 409 `IDEMPOTENCY_KEY_REUSED`. Só o id externo coincide (outra chave) → 409
   `EXTERNAL_TRANSACTION_ALREADY_REGISTERED`. Isto vem **antes** de qualquer política de operação nova e de qualquer validação que
   dependa do estado atual: reutilizar a chave com moeda não habilitada (EUR numa aplicação BRL), com outro tipo (`BET` → `WIN`,
   `LOSS`, `REFUND`, `ROLLBACK`), carteira inexistente, jogador ou moeda diferentes é conflito (409), não 422/501/404, e um replay
   legítimo não depende da carteira nem da configuração atual de moedas. A busca é sempre no escopo do provedor autenticado
   (autenticação e autorização antecedem tudo e não mudam).
2b. **Políticas de operação nova** (só se a identidade não existe), nesta ordem: moeda habilitada (422 `CURRENCY_NOT_SUPPORTED`) e tipo
   com processamento implementado (501 `KIND_NOT_SUPPORTED`, nada é gravado).
3. `GetForUpdate(walletId)`; inexistente → 404; jogador/moeda divergentes → 422 (erros de **entrada**, não gravados).
4. Com a carteira travada, repete o passo 2 (uma operação concorrente da mesma carteira pode ter confirmado entre os passos).
5. Aplica a regra (débito ou rejeição), grava carteira (CAS), transação, ledger e outbox, e confirma.
6. Se uma unicidade do banco dispara no INSERT (mesma identidade aplicada a **outra** carteira, portanto sem disputar o lock),
   a tentativa é desfeita e **refeita em nova transação** (até 3×), que então enxerga o commit vencedor e responde
   replay/conflito. Violações de unicidade não viram erro genérico: `classify` as mapeia por **nome de constraint**.

**Hash de payload** (`ExternalRequest.PayloadHash`, SHA-256 hex do JSON canônico), igual para HTTP e SQS:

* Campos: `providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money{amount,currency}` e, se
  presente, `referenceExternalTransactionId`. **Fora**: chave de idempotência e metadados de transporte.
* Normalizações: UUIDs em minúsculas; `amount` em escala fixa; opcionais ausentes omitidos.
* Serialização: sem espaços, chaves em ordem lexicográfica, UTF-8, sem escape de HTML. Há um vetor fixo no teste,
  conferido independentemente em Python (`json.dumps(sort_keys, separators=(",",":"))` + sha256).

**Interpretações**:

* A chave de idempotência tem **escopo por provedor** (`UNIQUE(provider_id, idempotency_key)`): o provedor vem do
  token, então provedores não colidem nem enxergam resultados uns dos outros.
* `(providerId, externalTransactionId)` já registrado sob **outra** chave → **sempre 409**, mesmo com conteúdo igual.
  O servidor nunca troca uma chave recebida por outra calculada.
* Replay de operação **terminal** nunca reavalia: uma `REJECTED` por saldo continua `REJECTED` depois de novos
  créditos (teste). O saldo devolvido é o gravado em `wager_transactions.result_balance`.
* Replay concorrente: 50 reenvios simultâneos → 1 débito e 49 replays (um teste também espalha-os por 3 processos).

## 6. Contratos HTTP e códigos de falha

Tabela de status/`code` no README. Princípios: 4xx corrigível = entrada (`400/404/422`) e **não é gravada**;
resultado definitivo de negócio = `422` com corpo de transação (`status: REJECTED`, `failureCode`) e **é auditado**;
conflito de idempotência = `409`; indisponibilidade = `503` + `Retry-After`; commit incerto = `503` distinto
(`COMMIT_OUTCOME_UNKNOWN`); tipo sem suporte = `501`; `202` (aceito/pendente) existe no contrato mas **não é alcançável**
nesta versão (não há aceite assíncrono).

`failureCode` implementados: `INSUFFICIENT_FUNDS` (BET). Reservados para o incremento de reversões, **ainda não
emitidos**: ver §8.

Processamento é **síncrono**: a transação é inserida já no estado final, sem commit intermediário de `PENDING`
(permitido pelo enunciado para operações sem dependências). Por isso não há o que retomar após queda; o `PENDING`
só passará a ser persistido junto com a retomada durável (worker de referências).

## 7. Inbox e outbox (§6.5, §11)

* **Outbox implementada (gravação)**: `outbox_events` guarda o envelope completo como `jsonb` (snapshot imutável,
  trigger impede alterá-lo), `eventId` = PK, `aggregate_type/id`, `correlation_id`, `causation_id`, `attempts`,
  `next_attempt_at`, `last_error`, `published_at`, e `seq` (ordem de inserção). Os eventos entram **na mesma transação**
  da operação; **nada é publicado** antes do commit (não há publicador ainda, então a garantia é trivial hoje).
  `causationId` de `WalletBalanceChanged` = `eventId` do `WagerTransactionProcessed` da mesma operação.
* **Pendente** (não implementado): publicador com `SELECT … FOR UPDATE SKIP LOCKED` + lease (colunas de lock serão
  adicionadas por migration), backoff exponencial, recuperação de trabalho abandonado, republicação com mesmo `eventId`.
  **Roteamento previsto** (destino já provisionado): SNS FIFO `wager-events.fifo`; `MessageGroupId = walletId`
  (presente em `data` de todos os eventos, preservando a ordem por carteira) e `MessageDeduplicationId = eventId`;
  os consumidores devem deduplicar por `eventId`, pois a entrega é *at-least-once*.
* **Inbox**: tabela e consumidor **não criados** (sem schema fictício). Desenho previsto: `UNIQUE(consumer_name,
  message_id)` + hash, inserida na mesma transação do caso de uso; `Submit` já é reutilizável pela entrada SQS
  (a chave é `data.idempotencyKey`). Fila: `MessageGroupId = walletId`, `MessageDeduplicationId = messageId`,
  visibilidade 60 s, `maxReceiveCount = 5` → DLQ; mensagens inválidas vão direto para a DLQ; rejeição de negócio
  confirmada remove a mensagem.

## 8. Reversões, referências pendentes e `PENDING_REFERENCE` — **decisões em aberto**

Não implementados (HTTP 501). O enunciado exige documentar a política; **não foi inventada nenhuma regra silenciosa**.
Já garantido pelo schema: `reference_external_transaction_id` obrigatório para `REFUND/ROLLBACK`, opcional para `WIN`,
proibido nos demais (CHECK); `reference_transaction_id` para a referência resolvida.

Proposta para aprovação (não aplicada):

* **Regra "uma devolução por movimento"**: cada movimento original (`BET`, `WIN`, `REFUND`) pode receber **no máximo uma
  reversão bem-sucedida** (`REFUND` ou `ROLLBACK`), imposta por índice único parcial sobre uma coluna
  `reversal_target_id` das reversões `PROCESSED`. Consequências: `REFUND`+`ROLLBACK` sobre a mesma `BET` é impossível
  (segunda → `REJECTED/ALREADY_REVERSED`), então o mesmo débito nunca é devolvido duas vezes; `WIN` só aceita
  `ROLLBACK`; `ROLLBACK` de `REFUND` redebita e **encerra** a `BET` (não aceita nova devolução). Em disputa concorrente
  o lock da carteira decide e o índice único é a garantia final.
* **Códigos propostos**: `REFERENCE_NOT_FOUND` (TTL/tentativas esgotados), `REFERENCE_NOT_PROCESSED` (referência
  `REJECTED/FAILED`, definitivo), `REFERENCE_MISMATCH` (provedor/jogador/carteira/moeda/rodada/valor), `ALREADY_REVERSED`,
  `REVERSAL_INSUFFICIENT_FUNDS` (distinto de `INSUFFICIENT_FUNDS`, como exigido).
* **Referência existente mas pendente** (`PENDING`/`PENDING_REFERENCE`): continua em `PENDING_REFERENCE` com o mesmo
  backoff; **terminou sem sucesso**: rejeição imediata e definitiva.
* **Backoff/TTL**: exponencial com jitter, máximo de tentativas ou TTL configurável; ao esgotar → `REJECTED` +
  `WagerTransactionRejected`. Worker com `SKIP LOCKED` para múltiplas instâncias.

## 9. Autenticação e autorização (§2)

* **IdP**: Keycloak (realm `wager`, importado de `deploy/keycloak/wager-realm.json`). `client_credentials` entre serviços;
  sem cadastro de senhas nem emissão própria de tokens.
* **Validação local de JWT** (`internal/infra/auth`): assinatura **RS256** pelo JWKS do IdP, `iss`, `aud=wager-api`, `exp`
  obrigatório e tolerância de relógio. Sem introspecção remota por requisição (latência e disponibilidade);
  contrapartida: um token revogado vale até expirar (5 min por padrão no realm). Recusados: `alg:none`, HS256 (confusão
  de algoritmo), assinatura inválida, chave desconhecida, audiência errada, expirado.
* JWKS carregado na **partida** (falha de inicialização se o IdP estiver fora) e atualizado a cada 15 min; `kid`
  desconhecido provoca releitura limitada a **1 por 30 s** e, acima do limite, **falha na hora** (401) em vez de reter a
  requisição — evita que tokens forjados com `kid` aleatório sobrecarreguem o IdP ou prendam goroutines. Contrapartida:
  após rotação de chaves, tokens novos podem falhar por até 30 s.
* **Modelo de permissões** (papéis de realm): `internal-service` (carteiras; lê qualquer transação) e `provider`
  (envia operações; lê só as suas). O **`providerId` autorizado vem da claim `provider_id`** (mapper do Keycloak por
  client). O `providerId` do corpo é apenas confirmado (diferente → 403); headers arbitrários de identidade são
  ignorados (testado). Consulta por id de outro provedor → **404** (não revela existência); por
  `/providers/{outro}/…` → 403. `OPENING` (interna) é invisível aos provedores.
* Acessos negados não têm efeito: testes comparam contagens de carteiras/transações/ledger/outbox antes e depois.
* `/health/*` são públicos e não expõem detalhes de erro (o motivo vai para o log).
* Mensageria: o controle de acesso ao SQS é do broker (credenciais/políticas AWS); o consumidor continuará validando o
  domínio (pendente).

## 10. Garantias impostas pelo PostgreSQL (§5.3, §5.8)

Migration `00001_init.sql` — verificadas por tentativas **diretas de SQL** em `constraints_test.go`:

| Garantia | Mecanismo |
| --- | --- |
| Carteira única por jogador+moeda | `UNIQUE (player_id, currency)` |
| Saldo ≥ 0, versão ≥ 1 | `CHECK` |
| Versão sobe exatamente 1 com mudança de saldo e só então; identidade imutável; sem DELETE/TRUNCATE de carteira | trigger `wallets_guard` |
| Saldo e ledger confirmam juntos | *constraint trigger* **deferida**: no COMMIT, `wallets.balance` = `balance_after` do último lançamento (0 sem lançamentos), disparada por alteração de carteira **e** por INSERT no ledger |
| Ledger append-only | triggers `BEFORE UPDATE/DELETE` (por linha) e `BEFORE TRUNCATE`; o papel de runtime nem tem os privilégios (§10.1) |
| **Cadeia contínua do ledger** (migration 00002) | trigger `BEFORE INSERT` `ledger_enforce_chain`: `balance_before` = `balance_after` do lançamento anterior da carteira (0 no primeiro). Ele **trava a linha da carteira** (`FOR NO KEY UPDATE`, mesmo nível do UPDATE de saldo): escritores da mesma carteira se serializam — inclusive em SQL direto —, carteiras diferentes não se bloqueiam, e vários lançamentos numa transação são validados em sequência. Escrita fora de ordem (`seq` menor que o último) falha como `40001`, tratada como transitória |
| Saldo = créditos − débitos | cadeia contínua + a constraint deferida abaixo (saldo = `balance_after` do último lançamento) ⇒, por indução, saldo = Σ créditos − Σ débitos |
| Lançamento vinculado à carteira e à transação financeira | FK composta `(transaction_id, wallet_id)` → `wager_transactions(id, wallet_id)`; constraint deferida `ledger_entry_matches_transaction`: transação `PROCESSED`, mesmo valor, sentido coerente com o tipo (`BET` = débito; `OPENING/WIN/REFUND` = crédito; `LOSS` nunca tem lançamento) |
| Aritmética e sinal do ledger | `CHECK` (`CREDIT`: after = before + amount; `DEBIT`: after = before − amount; amount > 0; saldos ≥ 0) |
| Um lançamento por `(wallet_id, transaction_id)` | `UNIQUE` |
| Moeda/jogador consistentes entre carteira, transação e ledger | FKs compostas `(wallet_id, currency)` e `(wallet_id, player_id)` |
| Idempotência | `UNIQUE (provider_id, idempotency_key)` e `UNIQUE (provider_id, external_transaction_id)` |
| Interno × externo | `CHECK ck_wager_tx_origin_shape`: `OPENING` ⇒ sem metadados externos; externo ⇒ todos presentes e tipo ≠ `OPENING` |
| Crédito inicial único | índice único parcial `(wallet_id) WHERE kind = 'OPENING'` |
| Política de zero | `CHECK` (`LOSS` = 0, demais > 0) |
| Forma por estado | `CHECK` com `IS NOT NULL` explícito: `PROCESSED` exige `result_balance` não nulo e ≥ 0; `REJECTED` exige código e resultado; `PENDING*` não têm código nem resultado; `FAILED` exige código (um `CHECK` aceita `NULL` como "desconhecido", então `>= 0` sozinho não basta). Formato do `failure_code` também por `CHECK` |
| Terminais imutáveis; transações nunca apagadas | trigger `wager_transactions_guard` |
| Outbox: snapshot imutável | trigger `outbox_events_guard` (só tentativas/agenda/publicação mudam) |

### 10.1. Papéis de banco: migrations × runtime

* **Dono do schema** (`wager` no Compose): roda `migrate up/down`, é dono das tabelas e de seus triggers.
* **Runtime** (`wager_app`, criado por `migrate up` quando `RUNTIME_DB_ROLE` está definido, ou por `migrate grants`):
  `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT`, **sem posse de nada** e sem `CREATE` no
  schema. Privilégios mínimos (lista em `internal/infra/postgres/roles.go`, reaplicada de um estado conhecido a cada execução):
  ledger `SELECT, INSERT`; transações e outbox `SELECT, INSERT`; carteiras `SELECT, INSERT` e `UPDATE (balance, version,
  updated_at)`; `goose_db_version` `SELECT`. Sem `UPDATE/DELETE/TRUNCATE` no ledger, sem `DISABLE TRIGGER` (exige posse), sem
  `session_replication_role` (exige superusuário), sem `DROP`/`ALTER`. Testado com tentativas reais (`role_test.go`) e com o fluxo
  financeiro completo e a verificação de schema rodando com esse papel. Ampliar privilégios (p.ex. `UPDATE` de transações
  pelo worker de referências, `UPDATE` da outbox pelo publicador) é uma mudança explícita nessa lista.
* Limitação restante: o dono e superusuários continuam podendo desabilitar triggers — por isso a aplicação nunca roda com
  esses papéis. Em produção, crie o papel de runtime fora do pipeline de migrations se o papel das migrations não tiver `CREATEROLE`.

### 10.2. Aplicar a correção a bancos já inicializados

`00002_ledger_chain_and_result_integrity.sql` é aplicada por `migrate up` **sem apagar dados**. Ela começa por uma verificação
prévia (cadeia, vínculo, resultados nulos, saldos×ledger) que **aborta com a contagem do problema** se o histórico estiver
inconsistente, sem corrigir nada automaticamente — a decisão sobre registros históricos é humana. Testado: histórico válido é
preservado (inclusive na reversão); histórico com cadeia quebrada ou resultado nulo é recusado e o schema permanece na versão 1.

## 11. Composição Fx, ciclo de vida e shutdown (§4)

* `bootstrap.Options(cfg)` = `fx.Supply(cfg)` + `fx.Module`s: `logging`, `persistence` (pool), `auth` (verificador),
  `messaging` (cliente SQS), `application` (serviço) e `http` (servidor). Construtores via `fx.Provide`; ligação de
  hooks via `fx.Invoke`/`fx.Lifecycle`. Logs do Fx saem pelo `slog` JSON.
* **Início** (`OnStart`): `postgres.Database` — o pool só é aberto em `Start`, que faz `Ping`, confere a versão do schema por um
  `SELECT` em `goose_db_version` (`migrate up` pendente impede a partida) e **fecha o pool antes de devolver o erro se algo
  falhar**: o Fx não chama o `OnStop` de um hook cujo `OnStart` falhou, então a liberação não depende dele. Verificador →
  carrega o JWKS; SQS → `GetQueueUrl` da fila de entrada; servidor → `Listen` síncrono e `Serve` em segundo plano.
  Qualquer falha aborta a inicialização (o Fx desfaz os hooks já iniciados) e **nenhuma conexão com o PostgreSQL permanece
  aberta** (testado, contando `pg_stat_activity`): PostgreSQL inacessível, schema sem migrations, schema desatualizado,
  credenciais inválidas, IdP inacessível, fila ausente e broker inacessível.
* **Encerramento**: o Fx executa os `OnStop` em ordem **inversa**; como o servidor depende do pool, ele para
  **primeiro**: `Shutdown` fecha o listener, conclui as requisições em andamento até `SHUTDOWN_TIMEOUT` (padrão 20 s) e
  então força o fechamento; depois o verificador e, por último, o pool é fechado (teste: requisição presa no lock
  conclui durante o shutdown; conexões ao banco chegam a zero). `SIGTERM` com saída 0 testado em 3 processos.
  Nota: o `net/http` só trata como ociosas conexões novas sem requisição após 5 s, então um cliente que deixa
  conexões abertas pode alongar o shutdown em até 5 s (dentro do prazo).
* Não há workers neste incremento, logo nada de "worker vazio"; quando existirem, entrarão como hooks próprios,
  parados antes do pool.

## 12. Observabilidade

* Logs JSON (`slog`): requisição com `method`, `route`, `status`, `durationMs`, `correlationId`, `clientId`, `providerId`;
  casos de uso com `transactionId`, `walletId`, `providerId`, `status`, `failureCode`, `replay`. `X-Correlation-Id`
  (validado ou gerado) é ecoado, propagado ao evento e à outbox. Não se registram tokens, corpos nem valores financeiros
  além do necessário ao diagnóstico.
* Health: `/health/live` (processo) e `/health/ready` (PostgreSQL `Ping` + SQS `GetQueueUrl`, em paralelo, prazo de 2 s;
  503 se qualquer um falhar). O IdP não entra na readiness (o JWKS fica em cache; falha dele aparece como 401).
* **Pendente**: métricas (resultados por status, duplicatas, retries, DLQ, conflitos de concorrência, atraso da outbox,
  latência, divergências de reconciliação) e tracing.

## 13. Interpretações adotadas (resumo)

1. Amount com 1–2 casas é aceito e normalizado (§2).
2. Chave de idempotência com escopo por provedor; mesma operação externa com outra chave → 409 sempre.
3. Carteira inexistente / jogador ou moeda divergentes: erro de entrada (404/422), **não** persistido (não há `wallet_id`
   válido para uma FK; é determinístico e corrigível).
4. `WALLET_ALREADY_EXISTS` devolve o `walletId` existente: `POST /wallets` não tem chave de idempotência no
   enunciado, então este é o caminho de recuperação após erro de commit incerto.
5. Operações síncronas: transação gravada já terminal; `202` previsto, não alcançável.
6. Replay de `REJECTED` responde `422` com `idempotentReplay:true` (mesmo status do original).
7. Serviço interno pode ler qualquer transação; provedor só as suas.
8. `GET …/ledger` e `POST …/reconciliation` respondem `501` após autorização.

## 14. Limitações e trabalho não concluído

Ver "Escopo e pendências" no README. Em síntese: `WIN/LOSS/REFUND/ROLLBACK`, política de reversões (§8), SQS + inbox,
publicador de outbox, worker de referências, paginação do ledger, reconciliação, métricas, e a prova com três processos
para fluxos SQS/outbox. A prova com três processos **existe para o fluxo HTTP implementado** (`multiprocess_test.go`).
