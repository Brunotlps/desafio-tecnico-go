-- +goose Up

-- Valores monetários são BIGINT em unidades mínimas (centavos, escala fixa 2).
-- Nenhuma coluna monetária usa REAL, DOUBLE PRECISION ou NUMERIC sem escala.

CREATE TABLE wallets (
    id         uuid        PRIMARY KEY,
    player_id  uuid        NOT NULL,
    currency   text        NOT NULL,
    balance    bigint      NOT NULL,
    version    bigint      NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT ck_wallets_currency        CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ck_wallets_balance_nonneg  CHECK (balance >= 0),
    CONSTRAINT ck_wallets_version         CHECK (version >= 1),
    -- Uma carteira por jogador e moeda.
    CONSTRAINT uq_wallets_player_currency UNIQUE (player_id, currency),
    -- Alvos das chaves estrangeiras compostas abaixo: impedem que transações e
    -- lançamentos usem moeda ou jogador diferentes dos da carteira.
    CONSTRAINT uq_wallets_id_currency     UNIQUE (id, currency),
    CONSTRAINT uq_wallets_id_player       UNIQUE (id, player_id)
);

CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL,
    kind                              text        NOT NULL,
    status                            text        NOT NULL,
    wallet_id                         uuid        NOT NULL,
    player_id                         uuid        NOT NULL,
    amount                            bigint      NOT NULL,
    currency                          text        NOT NULL,
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    round_id                          text,
    game_id                           text,
    reference_external_transaction_id text,
    reference_transaction_id          uuid REFERENCES wager_transactions (id),
    failure_code                      text,
    result_balance                    bigint,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,
    CONSTRAINT fk_wager_tx_wallet_currency FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
    CONSTRAINT fk_wager_tx_wallet_player   FOREIGN KEY (wallet_id, player_id) REFERENCES wallets (id, player_id),
    CONSTRAINT ck_wager_tx_origin  CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    CONSTRAINT ck_wager_tx_kind    CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    CONSTRAINT ck_wager_tx_status  CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    CONSTRAINT ck_wager_tx_currency CHECK (currency ~ '^[A-Z]{3}$'),
    -- Política de zero: LOSS exige 0; todos os demais exigem valor positivo.
    CONSTRAINT ck_wager_tx_amount  CHECK ((kind = 'LOSS' AND amount = 0) OR (kind <> 'LOSS' AND amount > 0)),
    -- Transações internas (OPENING) e externas têm formas distintas e exclusivas.
    CONSTRAINT ck_wager_tx_origin_shape CHECK (
        (origin = 'INTERNAL' AND kind = 'OPENING'
            AND provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL' AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    CONSTRAINT ck_wager_tx_reference CHECK (
        (kind IN ('REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL)
        OR (kind = 'WIN')
        OR (kind NOT IN ('REFUND', 'ROLLBACK', 'WIN') AND reference_external_transaction_id IS NULL)
    ),
    CONSTRAINT ck_wager_tx_opening_processed CHECK (kind <> 'OPENING' OR status = 'PROCESSED'),
    -- Forma por estado: resultado financeiro e código de falha coerentes.
    CONSTRAINT ck_wager_tx_status_shape CHECK (
        (status IN ('PENDING', 'PENDING_REFERENCE') AND failure_code IS NULL AND result_balance IS NULL)
        OR (status = 'PROCESSED' AND failure_code IS NULL AND result_balance >= 0)
        OR (status = 'REJECTED'  AND failure_code IS NOT NULL AND result_balance >= 0)
        OR (status = 'FAILED'    AND failure_code IS NOT NULL)
    ),
    -- Idempotência: a chave é única por provedor, e a identidade externa
    -- (provedor + id externo) não pode ser reaplicada com outra chave.
    CONSTRAINT uq_wager_tx_provider_idempotency_key UNIQUE (provider_id, idempotency_key),
    CONSTRAINT uq_wager_tx_provider_external_id     UNIQUE (provider_id, external_transaction_id)
);

-- No máximo um crédito inicial por carteira.
CREATE UNIQUE INDEX uq_wager_tx_one_opening_per_wallet ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
CREATE INDEX ix_wager_tx_wallet ON wager_transactions (wallet_id, created_at);

CREATE TABLE wallet_ledger_entries (
    seq            bigint GENERATED ALWAYS AS IDENTITY,
    id             uuid        PRIMARY KEY,
    wallet_id      uuid        NOT NULL,
    transaction_id uuid        NOT NULL REFERENCES wager_transactions (id),
    direction      text        NOT NULL,
    amount         bigint      NOT NULL,
    currency       text        NOT NULL,
    balance_before bigint      NOT NULL,
    balance_after  bigint      NOT NULL,
    created_at     timestamptz NOT NULL,
    CONSTRAINT fk_ledger_wallet_currency FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
    CONSTRAINT ck_ledger_direction CHECK (direction IN ('DEBIT', 'CREDIT')),
    CONSTRAINT ck_ledger_amount    CHECK (amount > 0),
    CONSTRAINT ck_ledger_balances  CHECK (balance_before >= 0 AND balance_after >= 0),
    CONSTRAINT ck_ledger_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after = balance_before + amount)
        OR (direction = 'DEBIT' AND balance_after = balance_before - amount)
    ),
    -- Um lançamento por transação e carteira.
    CONSTRAINT uq_ledger_wallet_transaction UNIQUE (wallet_id, transaction_id),
    CONSTRAINT uq_ledger_seq UNIQUE (seq)
);

CREATE INDEX ix_ledger_wallet_seq ON wallet_ledger_entries (wallet_id, seq);

CREATE TABLE outbox_events (
    seq            bigint GENERATED ALWAYS AS IDENTITY,
    id             uuid        PRIMARY KEY, -- eventId, estável entre republicações
    aggregate_type text        NOT NULL,
    aggregate_id   uuid        NOT NULL,
    event_type     text        NOT NULL,
    event_version  integer     NOT NULL,
    correlation_id text        NOT NULL,
    causation_id   uuid,
    payload        jsonb       NOT NULL, -- envelope completo (snapshot imutável)
    occurred_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    attempts       integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error     text,
    published_at   timestamptz,
    CONSTRAINT ck_outbox_attempts CHECK (attempts >= 0)
);

CREATE INDEX ix_outbox_pending ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------------
-- Proteções por trigger
-- ---------------------------------------------------------------------------

-- Ledger append-only: sem UPDATE, DELETE ou TRUNCATE.
-- +goose StatementBegin
CREATE FUNCTION ledger_forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% not allowed)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_ledger_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_mutation();
CREATE TRIGGER trg_ledger_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_forbid_mutation();

-- Transações: estados terminais e identidade são imutáveis; nada é apagado.
-- +goose StatementBegin
CREATE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'wager_transactions rows cannot be deleted'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind
       OR NEW.wallet_id <> OLD.wallet_id OR NEW.player_id <> OLD.player_id
       OR NEW.amount <> OLD.amount OR NEW.currency <> OLD.currency
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash THEN
        RAISE EXCEPTION 'wager transaction % identity fields are immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_wager_tx_guard
    BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();
CREATE TRIGGER trg_wager_tx_no_truncate
    BEFORE TRUNCATE ON wager_transactions
    FOR EACH STATEMENT EXECUTE FUNCTION wager_transactions_guard();

-- Carteira: identidade imutável; a versão sobe exatamente 1 a cada mudança de
-- saldo e permanece quando o saldo não muda; carteiras não são apagadas.
-- +goose StatementBegin
CREATE FUNCTION wallets_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency THEN
        RAISE EXCEPTION 'wallet % identity fields are immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'wallet % balance changes must increment version by one', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance = OLD.balance AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'wallet % version changes only with balance', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_wallets_guard
    BEFORE UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard();
CREATE TRIGGER trg_wallets_no_truncate
    BEFORE TRUNCATE ON wallets
    FOR EACH STATEMENT EXECUTE FUNCTION wallets_guard();

-- Saldo e ledger confirmam juntos: ao final da transação, o saldo da carteira
-- deve ser igual ao saldo posterior do último lançamento (0 sem lançamentos).
-- Impede alterar o saldo sem lançamento e lançar sem atualizar o saldo,
-- mesmo por SQL direto.
-- +goose StatementBegin
CREATE FUNCTION wallet_balance_matches_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    target_wallet uuid;
    wallet_balance bigint;
    last_after bigint;
BEGIN
    IF TG_TABLE_NAME = 'wallets' THEN
        target_wallet := NEW.id;
    ELSE
        target_wallet := NEW.wallet_id;
    END IF;
    SELECT balance INTO wallet_balance FROM wallets WHERE id = target_wallet;
    SELECT balance_after INTO last_after FROM wallet_ledger_entries
        WHERE wallet_id = target_wallet ORDER BY seq DESC LIMIT 1;
    IF wallet_balance IS DISTINCT FROM COALESCE(last_after, 0) THEN
        RAISE EXCEPTION 'wallet % balance % does not match ledger (%)',
            target_wallet, wallet_balance, COALESCE(last_after, 0)
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER ctrg_wallets_balance_matches_ledger
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallet_balance_matches_ledger();
CREATE CONSTRAINT TRIGGER ctrg_ledger_balance_matches_wallet
    AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallet_balance_matches_ledger();

-- Outbox: o snapshot do evento é imutável; só o controle de publicação muda.
-- +goose StatementBegin
CREATE FUNCTION outbox_events_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.seq <> OLD.seq OR NEW.aggregate_type <> OLD.aggregate_type
       OR NEW.aggregate_id <> OLD.aggregate_id OR NEW.event_type <> OLD.event_type
       OR NEW.event_version <> OLD.event_version OR NEW.correlation_id <> OLD.correlation_id
       OR NEW.causation_id IS DISTINCT FROM OLD.causation_id
       OR NEW.payload <> OLD.payload OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event % snapshot is immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_outbox_events_guard
    BEFORE UPDATE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();

-- +goose Down

DROP TRIGGER IF EXISTS trg_outbox_events_guard ON outbox_events;
DROP FUNCTION IF EXISTS outbox_events_guard();
DROP TRIGGER IF EXISTS ctrg_ledger_balance_matches_wallet ON wallet_ledger_entries;
DROP TRIGGER IF EXISTS ctrg_wallets_balance_matches_ledger ON wallets;
DROP FUNCTION IF EXISTS wallet_balance_matches_ledger();
DROP TABLE outbox_events;
DROP TABLE wallet_ledger_entries;
DROP TABLE wager_transactions;
DROP TABLE wallets;
DROP FUNCTION IF EXISTS wallets_guard();
DROP FUNCTION IF EXISTS wager_transactions_guard();
DROP FUNCTION IF EXISTS ledger_forbid_mutation();
