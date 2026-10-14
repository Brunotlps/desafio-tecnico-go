# ARCHITECTURE

Este documento registra as decisões de arquitetura e o estado de cada incremento entregue. Cada decisão aponta se é
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
* Sem hierarquias genéricas: uma interface por necessidade concreta (`UnitOfWork`, os cinco repositórios — carteiras, transações, ledger, outbox e agenda de pendências —,
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
   com processamento implementado (501 `KIND_NOT_SUPPORTED`, nada é gravado; hoje todos os tipos externos têm processamento, então esse ramo
   é só uma defesa). Implementados: `BET`, `WIN` (com ou sem referência), `LOSS`, `REFUND` e `ROLLBACK`. Uma referência recebida nunca é
   ignorada: ou é resolvida e validada, ou a operação espera por ela como `PENDING_REFERENCE` (§8).
3. `GetForUpdate(walletId)`; inexistente → 404; jogador/moeda divergentes → 422 (erros de **entrada**, não gravados).
4. Com a carteira travada, repete o passo 2 (uma operação concorrente da mesma carteira pode ter confirmado entre os passos).
5. Aplica a regra do tipo, grava carteira (CAS), transação, ledger e outbox, e confirma:
   * `BET`: débito, ou rejeição `INSUFFICIENT_FUNDS` sem movimentação;
   * `WIN`: crédito (uma linha de ledger `CREDIT`, `WagerTransactionProcessed` + `WalletBalanceChanged`); um crédito que estouraria o
     limite de `Money` (`ErrAmountOverflow`) interrompe a tentativa, pois a carteira só muda depois de a movimentação ser validada
     por inteiro — nada é gravado (HTTP 400 `INVALID_AMOUNT`);
   * `LOSS`: o valor é exatamente `0.00`; a carteira é travada para observar o saldo, que vira o `result_balance` (e o que o replay
     devolve), a transação nasce `PROCESSED` e só gera `WagerTransactionProcessed` — **sem** lançamento, sem alterar saldo ou versão
     e sem `WalletBalanceChanged`.

   **Núcleo compartilhável.** Os passos 2–5 vivem em `Service.processExternal`, que recebe os repositórios da tentativa (todos na mesma
   `pgx.Tx`) e **não abre nem confirma** transação: `Submit` abre a `UnitOfWork`, chama o núcleo e deixa o commit para a `UnitOfWork`.
   Outras entradas (consumidor SQS com inbox, worker de referências) podem chamar o mesmo núcleo dentro da *sua* transação, de modo
   que a inbox e o efeito financeiro compartilhem o commit.
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
(`COMMIT_OUTCOME_UNKNOWN`); `202` = aceito e pendente (`PENDING_REFERENCE`: sem saldo, com retomada durável) — antes da implementação das referências era inalcançável. `501` ficou reservado a um tipo sem processamento (hoje nenhum)
nesta versão (não há aceite assíncrono).

`failureCode` emitidos: `INSUFFICIENT_FUNDS` (BET) e os de referência e reversão descritos em §8 (`REVERSAL_INSUFFICIENT_FUNDS`, `ALREADY_REVERSED`,
`REFERENCE_NOT_FOUND`, `REFERENCE_PENDING_TIMEOUT`, `REFERENCE_NOT_PROCESSED`, `REFERENCE_MISMATCH`). `WIN` sem referência e `LOSS` não têm
código de rejeição próprio: o crédito não depende de saldo e `LOSS` apenas registra o resultado.

Processamento é **síncrono** quando não há dependência: a transação é inserida já no estado final, sem commit intermediário de
`PENDING` (permitido pelo enunciado para operações sem dependências). Quando a referência está indisponível, a única espera persistida é
`PENDING_REFERENCE`, sempre junto com a agenda de retomada (§8); `PENDING` nunca é confirmado (o banco o recusa), então não há aceite sem
continuidade durável.

## 7. Inbox e outbox (§6.5, §11)

* **Outbox implementada (gravação)**: `outbox_events` guarda o envelope completo como `jsonb` (snapshot imutável,
  trigger impede alterá-lo), `eventId` = PK, `aggregate_type/id`, `correlation_id`, `causation_id`, `attempts`,
  `next_attempt_at`, `last_error`, `published_at`, e `seq` (ordem de inserção). Os eventos entram **na mesma transação**
  da operação; **nada é publicado** antes do commit (não há publicador ainda, então a garantia é trivial hoje).
  `causationId` de `WalletBalanceChanged` = `eventId` do `WagerTransactionProcessed` da mesma operação.
* **Pendente** (não implementado): publicador com `SELECT … FOR UPDATE SKIP LOCKED` + lease (colunas de lock serão
  adicionadas por migration), backoff exponencial, recuperação de trabalho abandonado, republicação com mesmo `eventId`.
  **Roteamento previsto** (destino provisionado, §9): SNS FIFO `wager-events.fifo`; `MessageGroupId` = o **`walletId` do payload** (não o
  `aggregateId`: nos eventos de transação o agregado é a transação) e `MessageDeduplicationId = eventId`. O SNS FIFO preserva a ordem das
  publicações **aceitas** no mesmo grupo, mas **o publicador não promete reproduzir a ordem original da outbox** (vários publicadores, retries e
  recuperação podem reordenar e republicar): consumidores deduplicam por `eventId` (a entrega é *at-least-once*; a deduplicação do broker é
  temporária) e projeções de saldo aplicam `balanceAfter` só se `walletVersion` for maior.
* **Inbox**: tabela e consumidor **não criados** (sem schema fictício). Desenho previsto: `UNIQUE(consumer_name,
  message_id)` + hash, inserida na mesma transação do caso de uso; `Submit` já é reutilizável pela entrada SQS
  (a chave é `data.idempotencyKey`). Fila: `MessageGroupId = walletId`, `MessageDeduplicationId = messageId`,
  visibilidade 60 s, `maxReceiveCount = 5` → DLQ; mensagens inválidas vão direto para a DLQ; rejeição de negócio
  confirmada remove a mensagem.

## 8. Reversões, referências e pendências (implementado)

**Política aprovada** (decisão da mentoria; o enunciado exige que a combinação `REFUND`×`ROLLBACK` seja documentada):

| Tipo do movimento referenciado | Reversões admitidas | Efeito |
| --- | --- | --- |
| `BET` | `REFUND` ou `ROLLBACK`, **uma única vez com sucesso no total** | crédito integral |
| `WIN` | `ROLLBACK`, uma vez | débito integral |
| `REFUND` | `ROLLBACK`, uma vez | débito integral |
| `ROLLBACK`, `LOSS`, `OPENING` | nenhuma | `REFERENCE_MISMATCH` |

* **Uma reversão bem-sucedida por movimento alvo, em todo o histórico**, seja `REFUND` ou `ROLLBACK` (mais restritivo que a unicidade
  por tipo exigida literalmente). `BET → REFUND → ROLLBACK do REFUND` restaura o débito, e **a BET não admite outra reversão
  bem-sucedida** (nem `REFUND`, nem `ROLLBACK`); o `REFUND` também não admite outra. Não existe estado `REVERSED`: as operações originais
  continuam terminais e o ledger recebe novos lançamentos. Rejeições, falhas e pendências **não** consomem o direito de o alvo ser revertido.
  Consequência aceita: o `ROLLBACK` da BET depois do `ROLLBACK` do REFUND seria financeiramente viável, mas é recusado pelo contrato.
* `WIN` pode informar uma `BET` da mesma rodada como referência; o valor do prêmio é livre e o `WIN` **não** consome a unicidade da reversão.
* **Validação da referência** (`WagerTransaction.AssessReference`, no domínio): provedor, id externo referenciado, jogador, carteira, moeda e
  rodada iguais; tipo elegível (`REFUND`→`BET`; `WIN`→`BET`; `ROLLBACK`→`BET|WIN|REFUND`); **valor idêntico** nas reversões. Incompatibilidade →
  `REJECTED/REFERENCE_MISMATCH` (campos imutáveis: vale mesmo se o alvo ainda estiver pendente). Uma operação não pode referenciar o próprio
  id externo (entrada inválida, 400). A resolução é **sempre no escopo do `providerId` da própria operação**: a transação de outro provedor
  com o mesmo id externo nunca é encontrada nem revelada.
* **Códigos de falha** (todos `REJECTED`, auditáveis, com `WagerTransactionRejected` e sem ledger; todos **definitivos** — o cliente corrige
  a entrada com *outra* operação/identidade, nunca reabrindo esta): `INSUFFICIENT_FUNDS` (BET), **`REVERSAL_INSUFFICIENT_FUNDS`** (o débito de
  uma reversão excederia o saldo — distinto do da BET, como o enunciado exige), `ALREADY_REVERSED`, `REFERENCE_NOT_FOUND`,
  `REFERENCE_PENDING_TIMEOUT`, `REFERENCE_NOT_PROCESSED` (referência `REJECTED`/`FAILED`) e `REFERENCE_MISMATCH`. Entrada corrigível (400/404/422
  de entrada) não é gravada.

### Pendências: `PENDING_REFERENCE`, agenda durável e worker

* **Aceite.** Referência ainda indisponível (ausente, ou existente e `PENDING*`) ⇒ a operação é gravada em `PENDING_REFERENCE`, na **mesma
  transação** da agenda (`pending_references`) e do evento `WagerTransactionPendingReference`; HTTP responde `202` sem saldo. Não há commit
  intermediário de `PENDING` (o banco recusa um `PENDING` confirmado) nem `PENDING_REFERENCE` sem agenda aberta (verificação deferida).
* **Prazo.** `expires_at = first_persisted_at + TTL` (15 min por padrão), fixado na primeira persistência e imutável (trigger e privilégio de
  coluna): replay, reinício e tentativas **não o renovam**. Replay de pendência devolve o estado atual, sem outra agenda nem novo evento.
* **Avaliação** (a mesma função `Service.decide` do processamento inicial — as regras financeiras não são duplicadas — sobre o estado durável
  revalidado sob locks):

  | Referência válida | Antes do TTL | Depois do TTL |
  | --- | --- | --- |
  | ausente | espera e reagenda | `REJECTED/REFERENCE_NOT_FOUND` |
  | `PENDING`/`PENDING_REFERENCE` | espera e reagenda | `REJECTED/REFERENCE_PENDING_TIMEOUT` |
  | `PROCESSED` | regras financeiras | regras financeiras (**mesmo se concluiu depois do prazo**) |
  | `REJECTED`/`FAILED` | `REJECTED/REFERENCE_NOT_PROCESSED` | idem |

  Encontrar o alvo `PROCESSED` não dispensa saldo, tipo/escopo/valor nem a unicidade da reversão. Uma rejeição por expiração já confirmada
  é terminal e não é reaberta por uma chegada tardia nem por replay. **Falha de consulta não prova ausência**: a tentativa é abortada (erro
  transitório), nada é gravado e o prazo original segue valendo.
* **Varredura limitada que alcança as carteiras independentes** (`Service.ProcessDue`): a leitura de candidatos é paginada por chave `(próxima tentativa, id)` — sem
  `OFFSET` — e **pula, dentro da própria consulta, as carteiras que outra transação segura** (`JOIN wallets … FOR UPDATE OF w SKIP LOCKED`). Os modos de lock de linha **não** têm os
  mesmos conflitos: `FOR KEY SHARE` é compatível com `FOR NO KEY UPDATE` (o de um `UPDATE` de coluna não chave, como o do trigger de encadeamento do ledger), enquanto
  `FOR UPDATE` conflita com todos. A tentativa pede `FOR UPDATE` (`TryLockForUpdate`), então a seleção pede o **mesmo** `FOR UPDATE`: pula as carteiras seguradas por `FOR KEY SHARE`,
  `FOR SHARE`, `FOR NO KEY UPDATE`, `UPDATE` não chave ou `FOR UPDATE` (as cinco são testadas), e um prefixo de candidatos ocupados, por maior que seja, não esconde as carteiras
  independentes — sem estado entre varreduras, portanto também para várias chamadas e instâncias. Aumentar apenas o lote ou as páginas só deslocaria o limiar. A consulta é só uma
  leitura: o lock de linha dura o tempo da consulta (sem espera e sem lock durante o backoff) e a carteira e a elegibilidade são revalidadas sob os locks da tentativa.
  * **Carteira ocupada na tentativa ou com falha** é excluída das páginas seguintes da mesma varredura. Uma **falha de infraestrutura** (erro transitório, timeout) nunca é uma rejeição
    financeira: a transação da tentativa é desfeita por inteiro (sem efeito parcial) e a agenda é **adiada** com o backoff numa transação curta de uma única linha (`Defer`: próxima
    tentativa, contador e diagnóstico; não renova o prazo, não toca saldo/ledger/estado, não sobrescreve um reagendamento mais novo nem uma agenda concluída). Assim um prefixo de falhas
    numa carteira (lote 1 ou 20) não monopoliza as varreduras nem o orçamento: a carteira independente avança e o retry segue pelo estado durável quando o adiamento vence. Se o próprio
    adiamento falhar, a agenda segue como estava e a próxima varredura tenta de novo.
  * **Trabalho limitado**: no máximo `BatchSize` tentativas efetivas, 50 páginas e **3 falhas seguidas** (falha geral: a varredura termina com `failed=true` e o worker espaça a próxima, sem
    martelar a dependência); as puladas por ocupação, as ainda não vencidas e as que falharam não contam como tentativas efetivas, mas cada carteira com falha é tentada uma vez por
    varredura. O cancelamento é respeitado entre candidatos (e um erro por cancelamento não adia nada); uma varredura toda ocupada termina sem tentar nada e **sem tocar** nenhuma
    agenda, e o worker só varre de novo depois do intervalo de espera, sem laço ativo.
* **Elegibilidade temporal sob os locks**: depois de travar a carteira e a agenda, a tentativa compara o `next_attempt_at` **recarregado** com o relógio; uma seleção
  antiga não autoriza a tentativa que outra instância já reagendou para o futuro (o backoff persistido vale para todas). Não existe caminho "forçado" no worker:
  os testes avançam o relógio de propósito. A elegibilidade não é um prazo rígido de validade: uma referência `PROCESSED` ainda resolve depois do TTL.
* **Retomada** (`Service.ProcessPending`): transação curta (fase financeira de até 10 s, incluindo locks e commit) que recarrega e revalida a
  operação **original** — mesmo id, hash e chave; não recria nada nem usa reidratação como mutação — e conclui estado, saldo, ledger, outbox e
  agenda no mesmo commit; ou apenas reagenda (`next_attempt_at`, `attempts`), sem repetir o evento inicial. Backoff **persistido**:
  exponencial a partir de 1 s, teto 30 s, jitter uniforme em `[d/2, d]` (nunca acima do teto); nenhuma transação ou lock é mantido durante o backoff.
* **Ordem única de locks** (HTTP, retomada e a futura entrada SQS): **carteira → agenda da pendência → linha da transação**. O caminho HTTP
  trava a carteira primeiro; a retomada lê candidatos *sem locks* (só uma listagem, que não autoriza movimento), trava a carteira com
  `FOR UPDATE SKIP LOCKED` — uma carteira ocupada é **pulada** (as demais seguem, sem lock global) e nunca prende a retomada na ordem inversa
  — e depois a agenda. Como o alvo é sempre da mesma carteira (outra carteira é `REFERENCE_MISMATCH`, decidido por campos imutáveis), o estado
  do alvo e a unicidade da reversão são decididos sem corrida sob o lock da carteira.
* **Worker**: um executor por processo (`REFERENCE_WORKERS=1`), sem lease; vários processos podem rodar o seu ao mesmo tempo, e o resultado é
  único porque a carteira e a agenda são travadas e revalidadas (a agenda concluída some das consultas de candidatos). Instâncias com relógios
  diferentes: a retomada usa o **maior** entre o seu relógio e `updated_at` da transação (o domínio nunca ajusta um instante sozinho), de
  modo que um relógio atrasado não vira erro de entrada do cliente. O TTL e o backoff usam o relógio da aplicação (a diferença entre instâncias
  desloca o prazo, nunca o renova). `REFERENCE_PHASE_TIMEOUT` e o orçamento de shutdown limitam cada tentativa.
* **Concorrência de reversões**: `REFUND` e `ROLLBACK` simultâneos contra a mesma BET ⇒ um `PROCESSED`, o outro `REJECTED/ALREADY_REVERSED`,
  um único crédito. O índice único parcial (§10) é a barreira final: uma violação (`uq_wager_tx_one_reversal_per_target`) é classificada como
  disputa (`ErrReversalTargetTaken`), a tentativa é desfeita e **refeita em nova transação**, que enxerga o vencedor e rejeita (nunca confirma efeito parcial).

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
* **Mensageria — controle de acesso do broker** (enunciado §2): o acesso é controlado por credenciais e **políticas do broker**, não pelo
  código. Ambiente local: **MiniStack 1.5.27 com `AUTH=true`** (imagem fixada), que avalia as políticas IAM do principal identificado pela
  access key. Escolha: o LocalStack não aplica IAM sem plano pago, e não há conta AWS de teste; o enunciado permite o MiniStack. **Limite
  assumido: o MiniStack não valida assinaturas SigV4** — a access key identifica o principal, mas o segredo não é conferido. Logo, o que se
  prova é **autorização emulada por políticas**, não autenticação AWS completa nem adequação de produção; a autenticação real do projeto é a
  do Keycloak na API HTTP. Princípios:
  * **Uma identidade por responsabilidade, sem curingas**: produtor interno (só `SendMessage` na entrada), runtime (ciclo de vida da
    mensagem na entrada + envio à DLQ), publisher (`Publish`/`GetTopicAttributes` no tópico de saída), inspeção (observar e preparar cenários
    na DLQ e na auditoria) e um principal sem políticas (para comprovar `AccessDenied`). A aplicação **seleciona** o perfil de runtime
    (`AWS_PROFILE`/`AWS_SHARED_CREDENTIALS_FILE`, cadeia padrão do SDK) e não recebe as credenciais de provisionamento (a raiz do emulador), que ficam só no
    one-shot e na prova; o arquivo montado, porém, contém os cinco perfis e todos são legíveis pelo processo — não há isolamento físico entre eles (distribuir a cada
    processo só o perfil necessário é uma recomendação técnica de endurecimento). O publisher já tem principal, mas não há worker.
  * **Modelo de confiança** (decisão da mentoria): a fila compartilhada é para **produtores internos confiáveis**; provedores externos não têm
    `SendMessage` direto. O produtor vincula a origem autenticada (token, ou configuração controlada) ao `providerId` permitido antes de
    publicar; o consumidor valida o envelope e o escopo, mas não trata um campo do corpo como autenticação. Ainda **não implementado** (inbox e consumidor são o incremento 4).
  * **Entrega SNS→SQS** controlada pela política da fila de auditoria: só o serviço SNS, só a partir do tópico de eventos (`aws:SourceArn`).
    Sem a política — ou com outro tópico — a mensagem **não** é entregue, ainda que o `Publish` seja aceito (testado).
  * **Provisionamento idempotente** (`cmd/broker-provision`, SDK Go v2, one-shot do Compose; também usado pela prova): converge recursos,
    não duplica a assinatura, reaplica políticas e reaproveita as chaves ainda válidas; o arquivo de credenciais (formato de credenciais
    compartilhadas da AWS, um perfil por usuário, chaves sintéticas locais, fora do git) é escrito de forma atômica.
  * **Estado efêmero**: sem `PERSIST_STATE`, recriar o contêiner do emulador apaga filas, mensagens e identidades; o provisionamento as
    recria e a API deve ser reiniciada para reler as credenciais. Nenhuma durabilidade do broker é prometida (nem após `SIGKILL`).
  * **Prova** (`test/integration/broker_test.go`, na CI): com as credenciais reais de cada principal e recursos isolados por prefixo — fluxos
    positivos, 20 negativas tipadas (`AccessDenied`/403, inclusive a mesma ação em outro recurso, com ausência de efeito), FIFO, visibilidade e
    redrive com conteúdo preservado, SNS→SQS entregue e **não** entregue sem a política, provisionamento repetido e partida da API sem permissão.
    Não há testes de "segredo inválido": o emulador não os rejeitaria. A deduplicação do FIFO é de **transporte** (janela temporária do
    broker); a garantia financeira é da aplicação (idempotência e inbox no banco).
  * **Para a AWS real** (não provado aqui): assinatura SigV4 e segredos de verdade; papéis IAM no lugar de usuários com chave estática; políticas de
    recurso e criptografia (KMS) das filas e do tópico; endpoints privados e o redrive em segundo plano (o MiniStack o aplica na próxima tentativa de recebimento).

## 10. Garantias impostas pelo PostgreSQL (§5.3, §5.8)

Migrations `00001_init.sql` a `00006_…` — verificadas por tentativas **diretas de SQL** (inclusive pelo papel restrito de runtime) em `constraints_test.go`, `db_integrity_test.go` e `completed_movement_test.go`:

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
| **Transação concluída exige o seu movimento** (migration 00003) | *constraint trigger* **deferido** `ctrg_wager_tx_requires_movement` em `wager_transactions` (`AFTER INSERT OR UPDATE ... WHEN status = 'PROCESSED' AND kind <> 'LOSS'`): no COMMIT, toda transação `PROCESSED` (`OPENING`, `BET`, `WIN`, `REFUND`, `ROLLBACK`) tem o lançamento da própria carteira. `LOSS` e `REJECTED`/`FAILED`/`PENDING*` não têm — e um lançamento para eles é recusado pelo lado do ledger. **Ordem:** as FKs do ledger são imediatas (`NOT DEFERRABLE`), então o registro da transação precisa existir **antes** do lançamento (o contrário falha na hora, 23503); a **conclusão** (`PROCESSED`) pode vir depois do lançamento — a inserção já em `PROCESSED` ou a transição `PENDING → PROCESSED` na mesma transação SQL — porque as verificações deferidas avaliam o estado final no COMMIT. Várias operações podem ser confirmadas na mesma transação SQL |
| **Resultado = saldo posterior do seu lançamento** (00003) | no mesmo trigger e, espelhado, em `ledger_entry_matches_transaction`: `result_balance = balance_after` **do lançamento da própria operação**. Não é comparado com o saldo atual da carteira (outras operações, inclusive da mesma transação SQL, podem tê-lo alterado); por isso um replay devolve exatamente o saldo observado no processamento original |
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
| **Referência coerente** (migration 00005) | trigger `BEFORE INSERT/UPDATE` `wager_tx_reference_guard`: a referência interna aponta para o alvo com o **mesmo** provedor, id externo referenciado, jogador, carteira, moeda e rodada; alvo `PROCESSED`; tipo elegível (`REFUND`→`BET`, `WIN`→`BET`, `ROLLBACK`→`BET|WIN|REFUND`); reversões com **valor idêntico**. `CHECK`: referência interna exige a externa (`ck_wager_tx_reference_pair`) e não pode ser a própria transação |
| **Referência resolvida para concluir** | `CHECK ck_wager_tx_processed_reference`: `REFUND`, `ROLLBACK` e `WIN` com referência externa só ficam `PROCESSED` com a referência interna preenchida (forma `NULL` coberta explicitamente) |
| **Uma reversão por alvo** | índice único parcial `uq_wager_tx_one_reversal_per_target (reference_transaction_id) WHERE kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'`: vale para `REFUND` e `ROLLBACK` em conjunto; `WIN` com referência não o consome; rejeições/falhas/pendências também não |
| **Sentido do lançamento de `ROLLBACK`** | `ledger_entry_matches_transaction` (deferida): alvo `BET` ⇒ `CREDIT`; alvo `WIN`/`REFUND` ⇒ `DEBIT`; `ROLLBACK` sem alvo resolvido é recusado |
| **Mutabilidade das não terminais** | `wager_transactions_guard` (substituída): rodada, jogo, referência externa e criação passam a ser imutáveis, a referência interna não muda depois de resolvida, só transições `PENDING→*` e `PENDING_REFERENCE→PROCESSED|REJECTED|FAILED`, `updated_at` não regride |
| **Forma de `PENDING_REFERENCE`** (migration 00006) | `CHECK ck_wager_tx_pending_reference_shape`: só origem externa, tipo `WIN`/`REFUND`/`ROLLBACK` e referência externa preenchida; `CHECK ck_wager_tx_reference_form` (em qualquer estado): a referência externa, quando existe, não é vazia, tem ≤ 128 caracteres, nenhum caractere de controle, nenhum espaço nas pontas e difere do próprio id externo — as formas que a reidratação rejeita. O domínio não foi enfraquecido |
| **Pendência com retomada** | `pending_references` + constraint triggers deferidos: **nenhum `PENDING` é confirmado**; `PENDING_REFERENCE` exige agenda aberta; transação terminal não deixa agenda aberta; a agenda tem identidade e prazo imutáveis, `attempts` não diminui e nunca é apagada |

### 10.1. Papéis de banco: migrations × runtime

* **Dono do schema** (`wager` no Compose): roda `migrate up/down`, é dono das tabelas e de seus triggers.
* **Runtime** (`wager_app`, criado por `migrate up` quando `RUNTIME_DB_ROLE` está definido, ou por `migrate grants`):
  `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT`, **sem posse de nada** e sem `CREATE` no
  schema. Privilégios mínimos (lista em `internal/infra/postgres/roles.go`, reaplicada de um estado conhecido a cada execução):
  ledger `SELECT, INSERT`; outbox `SELECT, INSERT`; transações `SELECT, INSERT` e `UPDATE (status, failure_code, result_balance,
  reference_transaction_id, updated_at)` (identidade, valor, provedor, chave, hash, rodada, jogo e referência externa **não** são atualizáveis);
  agenda `pending_references` `SELECT, INSERT` e `UPDATE (next_attempt_at, attempts, last_attempt_at, last_error, completed_at)` (prazo e
  identidade imutáveis, sem `DELETE`); carteiras `SELECT, INSERT` e `UPDATE (balance, version, updated_at)`; `goose_db_version` `SELECT`. Sem `UPDATE/DELETE/TRUNCATE` no ledger, sem `DISABLE TRIGGER` (exige posse), sem
  `session_replication_role` (exige superusuário), sem `DROP`/`ALTER`. Testado com tentativas reais (`role_test.go`) e com o fluxo
  financeiro completo e a verificação de schema rodando com esse papel. Ampliar privilégios (p.ex. `UPDATE` da outbox pelo publicador) é uma mudança explícita nessa lista; a 00005 ampliou
  só as colunas de transição das transações e da agenda. `migrate grants` omite a agenda em um banco ainda anterior à 00005. A preparação de rollback revoga a escrita na agenda (§10.2).
* Limitação restante: o dono e superusuários continuam podendo desabilitar triggers — por isso a aplicação nunca roda com
  esses papéis. Em produção, crie o papel de runtime fora do pipeline de migrations se o papel das migrations não tiver `CREATEROLE`.

### 10.2. Aplicar as correções a bancos já inicializados

As migrations 00001 e 00002 já foram publicadas e executadas: **não são reescritas**; cada correção é uma migration nova.
`migrate up` aplica as pendentes **sem apagar dados**, e cada uma começa por uma verificação prévia (*preflight*) do histórico que
**aborta com a contagem do problema** — e, na 00003, os primeiros ids — se houver registros incompatíveis com a garantia que ela
instala, sem corrigir nada automaticamente (a decisão sobre registros financeiros históricos é humana; o schema permanece na
versão anterior).

| Migration | Preflight (aborta se houver...) | Instala | Reversão (`migrate down`, um passo) |
| --- | --- | --- | --- |
| `00002_ledger_chain_and_result_integrity` | cadeia do ledger quebrada; lançamento incoerente com sua transação; `PROCESSED`/`REJECTED` com resultado nulo; `failure_code` inválido; saldo ≠ ledger | cadeia contínua, FK composta, resultado explícito | remove o que instalou |
| `00003_completed_movement_requires_ledger` | transação `PROCESSED` (≠ `LOSS`) **sem lançamento**; `result_balance` ≠ `balance_after` do lançamento | trigger deferido da transação e verificação de resultado no lado do ledger | remove o trigger e restaura a função de 00002; **não toca nos dados** (após a reversão a lacuna volta a existir) |
| `00006_pending_shape_and_schedule_preservation` | **exclusão dos escritores**; recusa, com contagem e primeiros ids: `PENDING_REFERENCE` irretomável (não externo, tipo que não espera referência, ou referência ausente/inválida), referência externa com forma que a reidratação rejeita (vazia, > 128, controle, espaços nas pontas, igual ao próprio id) e cópia preservada sem transação correspondente | `CHECK ck_wager_tx_reference_form` (qualquer estado) e `ck_wager_tx_pending_reference_shape`, função `wager_valid_external_reference`; **restaura** a agenda preservada por um rollback anterior (valores originais, inclusive as concluídas) sobre as linhas que o `Up` da 00005 reconstruiu, e remove a cópia | remove os dois `CHECK` e a função; não toca em dados |
| `00005_references_and_reversals` | **exclusão dos escritores** (mesmo `LOCK TABLE` da 00004) e, com eles excluídos: transação confirmada como `PENDING`; reversão/`WIN` com referência `PROCESSED` sem a referência interna; referência interna incoerente com a externa, o escopo, o tipo, o estado ou o valor; alvo com mais de uma reversão `PROCESSED`; lançamento de `ROLLBACK` no sentido errado (contagem e primeiros ids) | tudo o que está em §10 na linha "Referência coerente" em diante, mais a agenda `pending_references` | remove as garantias, restaura as funções da 00001/00003 e `DROP`a a agenda (transações e ledger intactos). **O migrador preserva a agenda antes dessa reversão e a 00006 a restaura na reaplicação** — ver "Rollback e a agenda das pendências" abaixo |
| `00004_revalidate_history_under_writer_exclusion` | **exclusão dos escritores** (`LOCK TABLE` em `wallets`, `wager_transactions`, `wallet_ledger_entries`) e, com eles excluídos: triggers das garantias ausentes ou desabilitados; cadeia do ledger quebrada; lançamento incoerente com a transação; saldo ≠ ledger; transação concluída sem lançamento; resultado ≠ `balance_after` (com contagem e primeiros ids) | nada (só valida; é a versão final esperada pela aplicação) | vazia: apenas retrocede a versão, sem tocar em dados nem garantias |

Mecanismo da 00003 (por que um *constraint trigger* deferido): um `CHECK` não enxerga outra tabela; uma FK da transação para o
ledger seria circular com a FK do ledger para a transação; um trigger imediato exigiria que a transação já estivesse concluída e o lançamento já inserido no instante de cada
comando. O deferido é avaliado no COMMIT sobre o estado final (a ordem imposta pelas FKs imediatas — transação antes do lançamento — não muda), é somente leitura (não trava nada, não afeta a concorrência por carteira) e consulta o lançamento
pelo índice único `(wallet_id, transaction_id)`; o `WHEN` limita o custo às transações `PROCESSED` com movimento.

Nota: o comentário de cabeçalho da `00003` (já publicada, preservada como está) diz que transação e lançamento "podem ser inseridos em qualquer ordem"; vale a
redação desta seção (transação antes do lançamento; conclusão possível depois dele; verificação final deferida). Testado em `TestInsertionOrderOfTransactionAndLedgerEntry`.

Testado (`migration_test.go`, `migration_movement_test.go`): histórico válido é preservado (inclusive na reversão); histórico com
cadeia quebrada, resultado nulo, transação concluída sem lançamento ou resultado divergente é recusado, com o schema na versão
anterior e os registros intactos; aplicar → reverter → reaplicar funciona.

Limitações: `LOSS` não tem lançamento, então seu `result_balance` (saldo observado) não é verificável contra o ledger; o sentido do
lançamento de `ROLLBACK` é imposto desde a 00005 (direção oposta à do alvo, §10); o dono do schema e
superusuários ainda podem desabilitar triggers (a aplicação nunca roda com eles).

#### Rollback e a agenda das pendências (achados da auditoria e da reauditoria do incremento 2)

A agenda é dado durável (início, vencimento, próxima tentativa, tentativas, diagnóstico, agendas concluídas). O `Down` publicado da 00005
apaga a tabela e o seu `Up` reconstrói linhas com `now() + 15 min` — o que renovaria um prazo vencido e zeraria tentativas. Documentar essa
perda **não** a corrige; como a 00005 não pode ser reescrita, a correção é composta e compatível com o histórico. O procedimento é do migrador
(`Migrator`, interfaces de `cmd/migrate up|down|down-to`) e **preserva ou recusa antes de qualquer perda**:

* **Coordenação.** Cada operação do migrador — `Up`, `UpTo`, `Down`, `DownTo`, `UpAndProvision` e `ProvisionRuntime` (o `migrate grants`) — roda sob um advisory lock próprio (`MigratorLockID`),
  mantido numa conexão dedicada, e só então o provider adquire o lock do goose (outra chave, sempre depois: a ordem é única e não há deadlock; a provisão usa conexão própria e nunca o lock do
  migrador). O lock é adquirido **por tentativas** (`pg_try_advisory_lock` a cada 100 ms), não por um `pg_advisory_lock` bloqueante: uma sessão bloqueada numa espera de lock só percebe a
  saída do cliente ao acordar, e um comando cancelado ou morto deixaria uma sessão órfã na fila que adquiriria o lock depois. Assim a espera termina imediatamente no cancelamento e não vaza.
  Decisão de versão, preparação, reversão, restauração e concessão dos grants não se intercalam com as de outro migrador; `migrate up` com `RUNTIME_DB_ROLE` faz o `Up` e a provisão **na
  mesma região**.
* **Preparação de rollback**, ao reverter a 00005 (por `Down` na versão 5 ou `DownTo(N)`, `0 < N < 5`), numa transação sob exclusão dos escritores (`LOCK TABLE wallets,
  wager_transactions, pending_references IN SHARE ROW EXCLUSIVE MODE`): (a) lê os privilégios sobre a agenda **de tabela e de coluna** (`pg_class.relacl` e
  `pg_attribute.attacl`, inclusive `PUBLIC`) e os revoga de todo papel que não seja o dono (o `REVOKE` de tabela remove também os de coluna); (b) **verifica no catálogo**:
  qualquer papel de login — fora superusuários, o dono e a sessão do migrador — que ainda tenha escrita, direta, herdada ou por pertencer (`pg_has_role(..., 'MEMBER')`) a um
  papel com escrita (p.ex. `pg_write_all_data`), **recusa** a preparação com os nomes e a transação é desfeita (nada muda); (c) grava `pending_references_preserved`
  (cópia integral, sem acesso de `PUBLIC`), marcada, junto com a tabela viva, por um token em `COMMENT`.
* **A cópia original nunca é substituída por um estado reconstruído.** Com uma cópia já existente: se a agenda viva é a tabela que foi copiada (mesmo token, ou conteúdo idêntico) — um
  `down` anterior falhou ou foi cancelado depois da preparação —, a cópia é renovada a partir da agenda viva, que contém também o que o runtime confirmou depois da devolução dos
  grants; se a agenda viva é a reconstrução do `Up` da 00005 (reaplicação parcial, `DownTo(4) → UpTo(5) → Down`), a cópia **original é mantida** e a preparação só prossegue se a
  agenda viva for a reconstrução intacta (toda linha existe na cópia, sem tentativas, diagnóstico nem conclusão); caso contrário o rollback é **recusado** com contagem e primeiros
  ids, sem alterar versão, agenda, cópia nem privilégios. A API recusa partir no schema intermediário, então essa recusa só ocorre por escrita manual (dono/superusuário).
* **Devolução dos privilégios**: se a reversão falhar ou for cancelada depois da preparação (inclusive esperando o lock do goose), o migrador devolve os privilégios capturados quando a
  agenda ainda existe (contexto próprio, não o cancelado); se o processo morrer antes, `migrate grants` os devolve — depois que a operação anterior terminou: enquanto uma operação do migrador
  está em andamento (p.ex. um `Down` preparado) o comando **espera** pelo lock, e cancelado (SIGINT) termina sem vazar nada. A agenda original fica intacta.
* **`Up`/`UpTo` com cópia existente** (`settleScheduleCopyBeforeUp`, na versão 5, antes de a 00006 restaurar), sob a mesma exclusão dos escritores: se a agenda viva é **a mesma tabela que foi
  copiada** (um `Down` preparado foi cancelado ou falhou antes de apagá-la), ela contém tudo — inclusive o que o runtime confirmou depois da devolução dos grants — e a cópia, defasada, é
  **descartada** em vez de restaurada por cima; se a agenda viva é a reconstrução da 00005 e a cópia é o original, a restauração é o esperado desde que a reconstrução esteja intacta; se não,
  o `Up` é **recusado** antes de qualquer perda (contagem e ids; nada muda). Nunca se aplica um snapshot velho em silêncio.
* **Restauração** (00006, na reaplicação): substitui as linhas reconstruídas pelo `Up` da 00005 pelos valores originais, inclusive as concluídas,
  desabilitando o gatilho de imutabilidade só durante a restauração (dono, sob exclusão dos escritores), dispara as verificações deferidas e remove a cópia.
  Uma cópia sem transação correspondente é recusada com contagem e ids (preflight), não descartada; uma restauração que falha deixa a cópia (a migration é transacional).
* **Reset destrutivo ≠ rollback operacional**: `down-to 0` apaga tudo, como sempre, e descarta também a cópia; `down`/`down-to N>0` preservam.
* Limites: a exclusão é por privilégio — dono e superusuários (o operador) não são excluídos, e uma visão/função `SECURITY DEFINER` de outro dono que escreva na agenda não é detectada;
  o `Down` da 00005 executado direto pelo goose não é protegido; dados que nunca tiveram agenda (um `PENDING_REFERENCE` criado por SQL antes da 00005) recebem a agenda reconstruída da 00005.
* Testes: `audit_regressions_test.go` (ciclo preservando prazo vencido, tentativas, diagnóstico e concluídas; janela observada com o `DROP` retido por uma leitura aberta; banco ainda na versão 5;
  `down-to 0`; cópia órfã) e `reaudit_regressions_test.go` (reaplicação parcial; `Down` cancelado com o lock do goose retido; repetição; recusa por perda de dados e por escritor não excluível, também
  pelo binário; grants por coluna, `PUBLIC` e herança; concorrência entre migradores).

### 10.3. Upgrade com escritores ativos: a janela entre validar e instalar, e como foi fechada

**O problema (achado da auditoria).** Uma migration que valida o histórico e depois instala uma garantia (00002: cadeia do ledger;
00003: movimento obrigatório) não enxerga o commit de um escritor do schema anterior que já inseriu linhas e mantém a transação
aberta. A instalação do trigger espera esse escritor, ele confirma sob as garantias **antigas** e a migration termina com sucesso:
schema novo, trigger instalado e histórico incompatível persistido. A atomicidade da transação SQL de uma migration não inclui
o commit de outras conexões, e o lock de sessão do goose só coordena **migrations** concorrentes, não os escritores da aplicação.

**Escolha: exclusão dos escritores pelo banco, numa migration versionada** (`00004`), e não uma janela de manutenção como único
mecanismo. Justificativa:

* Uma janela de manutenção só é "verificável" se algo garantir que **todos** os escritores pararam. O migrador não controla outros
  processos nem outros papéis; uma conferência em `pg_stat_activity` é uma foto (uma conexão pode chegar logo depois), e impedir
  conexões exigiria alterar papéis do cluster, um efeito colateral desproporcional para uma migration.
* O `LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE` usa o que os escritores têm em comum — as tabelas que escrevem. Ele conflita com
  `INSERT`/`UPDATE`/`DELETE` (ROW EXCLUSIVE), então a migration **espera** o escritor que já escreveu (e só depois lê: sob READ
  COMMITTED, o comando seguinte enxerga o commit dele), **retém** os que chegam até o `COMMIT` da migration e não bloqueia leituras
  nem o `SELECT ... FOR UPDATE` do caso de uso. Validar e concluir passam a ser indivisíveis para os escritores, sem cooperação deles
  e para qualquer papel. A ordem do lock (carteira, transação, lançamento) é a do caso de uso, para não formar ciclos de espera.
* Com os escritores excluídos, a `00004` relê **todo** o histórico (não só o que a 00003 validava, pois a 00002 tem a mesma forma) e
  confere que os triggers das garantias existem e estão habilitados. Escritores que **não** passam pelos triggers
  (`session_replication_role = replica`, restauração com triggers desabilitados, dono do schema) só são detectados por essa
  validação — por isso ela precisa ocorrer sob exclusão (testado).
* A 00003 já foi publicada e não é reescrita; sua janela é tratada **depois do fato**: a 00004 detecta e recusa o histórico
  incompatível, inclusive em bancos onde a janela já ocorreu (e no upgrade 2→4 com um escritor aberto, que deixa o schema em 3).

**Portão da versão final.** Um upgrade só termina bem na versão final (hoje 6). Se uma migration recusar (p.ex. a 00004 ou a 00005), o schema fica na versão anterior, **nada é apagado
nem reescrito**, e a aplicação (que compara a versão do schema com a embutida no binário) **recusa iniciar**: o estado é visível e
não é tratado como sucesso. A correção dos registros é manual; depois, `migrate up` conclui.

**Procedimento** (README §Migrations repete os comandos):

1. *Recomendado:* parar as instâncias da API (todos os escritores) antes de `migrate up`: reduz o tempo em que escritores ficam
   retidos e evita conflitos de lock. Não é necessário para a correção contra escritores do papel de runtime (as garantias acima
   valem com eles ativos), mas é a precondição para **qualquer outro** escritor (manutenção manual, ETL, outro papel) que o banco
   não consiga excluir por si só.
2. Executar `migrate up` com o papel dono do schema; conferir `migrate status` (versão final) e subir a API nova.
3. Se a 00004 abortar: ler a contagem e os ids, corrigir os registros manualmente (nenhuma ferramenta os altera) e repetir `migrate up`.

**Limites.**

* `lock_timeout` de 30 s: um escritor com transação aberta por mais tempo faz a 00004 falhar **sem efeito** (os retidos são
  liberados); repita numa janela com os escritores parados. Sem limite, novos escritores ficariam enfileirados indefinidamente.
* Durante a espera, escritores novos ficam retidos (latência), e um escritor que bloqueie as tabelas em ordem diferente da do caso
  de uso pode sofrer `deadlock detected` (40P01) em vez da migration; é seguro repetir.
* Entre a 00003 e a 00004 existe, por instantes, o schema 3 (com as garantias da 00003). Um escritor do schema 2 que confirma
  durante a 00003 pode deixar histórico incompatível; é exatamente o que a 00004 detecta.
* O banco não impede um escritor com privilégio de superusuário ou dono de desabilitar triggers depois do upgrade (§10.1); a
  validação só vale para o instante da migration.
* Upgrade online sem parada **não** é objetivo (o enunciado não o exige); a exclusão torna o upgrade correto, não invisível.

Testes (`upgrade_window_test.go`): a sondagem da auditoria (escritor antigo aberto → upgrade → commit → versão final e garantias),
escritor concorrente válido, histórico já inválido (5 formas), garantia ausente ou desabilitada, escritor que ignora triggers,
espera/retenção pelo lock, dados preservados, reversão/reaplicação e partida recusada abaixo da versão final.

## 11. Composição Fx, ciclo de vida e shutdown (§4)

* `bootstrap.Options(cfg)` = `fx.Supply(cfg)` + `fx.Module`s: `logging`, `persistence` (pool), `auth` (verificador),
  `messaging` (cliente SQS), `application` (serviço) e `http` (servidor). Construtores via `fx.Provide`; ligação de
  hooks via `fx.Invoke`/`fx.Lifecycle`. Logs do Fx saem pelo `slog` JSON.
* **Início** (`OnStart`): `postgres.Database` — o pool só é aberto em `Start`, que faz `Ping`, confere a versão do schema por um
  `SELECT` em `goose_db_version` (`migrate up` pendente impede a partida) e **fecha o pool antes de devolver o erro se algo
  falhar**: o Fx não chama o `OnStop` de um hook cujo `OnStart` falhou, então a liberação não depende dele. Verificador →
  carrega o JWKS; SQS → `GetQueueUrl` da fila de entrada. Por último, o **coordenador** inicia os componentes que admitem trabalho, na
  ordem de registro: worker de referências e, por fim, o servidor HTTP (`Listen` síncrono, `Serve` em segundo plano) — a porta de entrada abre
  por último. Se um componente falha ao iniciar, o coordenador encerra os já iniciados antes de devolver o erro, e o Fx desfaz os hooks
  anteriores: **nenhuma conexão com o PostgreSQL permanece aberta** (testado contando `pg_stat_activity`, inclusive com o worker ativo e a porta
  ocupada): PostgreSQL inacessível, schema sem migrations ou desatualizado, credenciais inválidas, IdP inacessível, fila ausente, broker
  inacessível e porta ocupada. O contexto de `OnStart` só limita a inicialização: o worker tem contexto próprio, cancelável por `Cancel`.
* **Encerramento coordenado** (`internal/lifecycle`): um único prazo global (`SHUTDOWN_TIMEOUT`, 20 s) com **drenagem de no máximo 12 s**
  (`SHUTDOWN_DRAIN_TIMEOUT`) e uma **reserva compartilhada de pelo menos 8 s** — não concedida de novo a cada componente — para
  cancelamento, cleanup e fechamento das dependências. Sequência: (1) **admissão fechada em todos os componentes ao mesmo tempo**: o servidor
  fecha o listener e responde `503 SHUTTING_DOWN` a requisições novas (inclusive em conexões keep-alive), o worker deixa de buscar trabalho;
  (2) **drenagem simultânea** do trabalho admitido; (3) no limite de drenagem, **cancelamento** do que restou (fecha as conexões e cancela o
  contexto das requisições; cancela a tentativa do worker, cuja transação é desfeita); (4) **confirmação observável do término** de cada
  componente (`Done`): o servidor só confirma quando o `http.Server` terminou **e** todos os handlers retornaram — o fim do `Serve`, sozinho,
  não prova isso, e cancelar um contexto não prova o fim de uma goroutine; (5) só então o Fx executa os `OnStop` das dependências em ordem
  inversa (verificador, SQS, pool). Se algum componente não confirmar até o prazo global menos uma margem de 1 s, o encerramento é registrado
  como **incompleto** (erro e log) e a recuperação fica a cargo do estado durável; não se declara término normal sem confirmação. O fechamento do
  pool nunca prende o encerramento além do prazo (`pgxpool.Close` espera conexões devolvidas). O *rollback* de uma transação cancelada usa um
  prazo próprio de 5 s que, durante o encerramento, **respeita o orçamento restante** (`Store.SetShutdownDeadline`). `docker compose` usa
  `stop_grace_period: 25s` para não enviar `SIGKILL` antes do fim do prazo de 20 s. `SIGTERM` com saída 0 testado em 3 processos; `SIGKILL`
  não depende de hook: a pendência aceita e o movimento confirmado se recuperam pelo estado durável (testado).
  Nota: o `net/http` só trata como ociosas conexões novas sem requisição após 5 s, então um cliente que deixa
  conexões abertas pode alongar a drenagem em até 5 s (dentro do prazo).
* Configuração validada na composição (`Config.Validate`, também para configurações montadas diretamente): `SHUTDOWN_DRAIN_TIMEOUT` menor que
  `SHUTDOWN_TIMEOUT`; `REFERENCE_BACKOFF_INITIAL` ≤ `REFERENCE_BACKOFF_MAX` e ≤ `REFERENCE_TTL`; `REFERENCE_PHASE_TIMEOUT` menor que o
  prazo de shutdown; todos os prazos positivos; `REFERENCE_WORKERS` ∈ {0, 1}. Pool inicial: 10 conexões por processo.

## 12. Observabilidade

* Logs JSON (`slog`): requisição com `method`, `route`, `status`, `durationMs`, `correlationId`, `clientId`, `providerId`;
  casos de uso com `transactionId`, `walletId`, `providerId`, `status`, `failureCode`, `replay`. `X-Correlation-Id`
  (validado ou gerado) é ecoado, propagado ao evento e à outbox. Não se registram tokens, corpos nem valores financeiros
  além do necessário ao diagnóstico.
* Health: `/health/live` (processo) e `/health/ready` (PostgreSQL `Ping` + SQS `GetQueueUrl`, em paralelo, prazo de 2 s;
  503 se qualquer um falhar). O IdP não entra na readiness (o JWKS fica em cache; falha dele aparece como 401).
* **Pendente**: métricas (resultados por status, duplicatas, retries, DLQ, conflitos de concorrência, atraso da outbox,
  latência, divergências de reconciliação) e tracing. O worker de referências registra em log (`transactionId`, `walletId`, `providerId`,
  `kind`, `status`, `failureCode`) cada pendência concluída e cada tentativa abortada.

## 13. Interpretações adotadas (resumo)

1. Amount com 1–2 casas é aceito e normalizado (§2).
2. Chave de idempotência com escopo por provedor; mesma operação externa com outra chave → 409 sempre.
3. Carteira inexistente / jogador ou moeda divergentes: erro de entrada (404/422), **não** persistido (não há `wallet_id`
   válido para uma FK; é determinístico e corrigível).
4. `WALLET_ALREADY_EXISTS` devolve o `walletId` existente: `POST /wallets` não tem chave de idempotência no
   enunciado, então este é o caminho de recuperação após erro de commit incerto.
5. Operações que dependem de uma referência ainda indisponível respondem `202` (`PENDING_REFERENCE`, sem saldo, com retomada durável, §8); `200`/`422` quando a transação é gravada já terminal.
6. Replay de `REJECTED` responde `422` com `idempotentReplay:true` (mesmo status do original).
7. Serviço interno pode ler qualquer transação; provedor só as suas.
8. `GET …/ledger` e `POST …/reconciliation` respondem `501` após autorização.
9. Reversões: uma reversão bem-sucedida por movimento alvo em todo o histórico (§8); o TTL das referências conta da primeira persistência e
   não é renovado; uma referência `PROCESSED` resolve mesmo depois do TTL; referência de outro provedor nunca é consultada.

## 14. Limitações e trabalho não concluído

Ver "Escopo e pendências" no README. Em síntese, **ainda ausentes**: SQS + inbox (consumidor, retry/DLQ, produtor autorizado), publicador
de outbox, paginação do ledger, reconciliação, métricas, e a prova com três processos para os fluxos SQS/outbox. O controle de acesso do
broker está **provado como autorização emulada** (§9), com o limite de não validar SigV4. A prova com três processos **existe** para
HTTP (apostas, WIN/LOSS) e para reversões e pendências com o worker real (`references_multiprocess_test.go`).

Limites das garantias atuais (detalhes nas seções indicadas): dono do schema e superusuários podem desabilitar triggers — a aplicação
nunca roda com eles (§10.1); o `result_balance` de `LOSS` não é verificável contra o ledger (§10.2); a regressão de relógio entre instâncias
é recusada pelo domínio e tratada por quem chama a transição (§3, §8); a rotação de chaves do IdP pode causar 401 por até 30 s (§9). Da
política de reversões (§8): o `ROLLBACK` da BET depois do `ROLLBACK` do REFUND é recusado (escolha de contrato, não de viabilidade
financeira). Das pendências: o TTL e o backoff usam o relógio da aplicação (diferença entre instâncias desloca o prazo, nunca o renova); uma
uma carteira continuamente ocupada ou com falha localizada adia apenas a retomada das pendências dela (a consulta a pula com o mesmo `FOR UPDATE SKIP LOCKED` da tentativa, a falha adia a agenda com backoff e a varredura segue para as demais); o worker não tem lease (a exclusão é pelos locks
e pela revalidação). Dos testes: o harness de processos reais compila o binário **sem `-race`** — `go test -race` do processo pai não
instrumenta os filhos (adaptação prevista para a consolidação final). A proteção da agenda no rollback está no migrador (`Migrator`), não no SQL publicado da 00005: o `Down` da 00005 executado diretamente pelo goose, sem o migrador, não preserva a agenda; a exclusão dos escritores é por privilégio (dono e superusuários não são excluídos).
