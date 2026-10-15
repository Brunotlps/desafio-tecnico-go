-- +goose Up

-- Entrada por SQS (incremento 4).
--
-- inbox_messages: a identidade durável de uma mensagem tratada é (consumer_name, message_id do envelope); body_hash é o SHA-256 dos
-- BYTES EXATOS do corpo recebido (não o hash financeiro canônico). A linha nasce JÁ CONCLUÍDA, na mesma transação do tratamento
-- financeiro (transação, saldo, ledger, agenda de pendência e outbox): não existe estado "aceita, sem conclusão" confirmado.
-- transaction_id liga a conclusão à operação financeira tratada (a própria transação ou, num replay financeiro, a original).
-- Identidade e hash são imutáveis; sem UPDATE, DELETE nem TRUNCATE (nem para o dono: o trigger recusa).
--
-- message_diagnostics: registro operacional de mensagens permanentemente recusadas (envelope inválido, conflito de identidade,
-- entrada que não se corrige sozinha). NÃO é uma operação financeira: sem transação, sem ledger, sem corpo financeiro. A unicidade
-- inclui o hash, então corpos diferentes sob o mesmo messageId são registros distintos e a inbox original nunca é sobrescrita.

CREATE TABLE inbox_messages (
    consumer_name      text        NOT NULL,
    message_id         text        NOT NULL,
    body_hash          bytea       NOT NULL,
    received_at        timestamptz NOT NULL,
    completed_at       timestamptz NOT NULL,
    disposition        text        NOT NULL,
    financial_replay   boolean     NOT NULL,
    transaction_id     uuid        NOT NULL REFERENCES wager_transactions (id),
    PRIMARY KEY (consumer_name, message_id),
    CONSTRAINT ck_inbox_consumer CHECK (consumer_name <> '' AND char_length(consumer_name) <= 128),
    CONSTRAINT ck_inbox_message_id CHECK (message_id <> '' AND char_length(message_id) <= 128),
    CONSTRAINT ck_inbox_hash CHECK (octet_length(body_hash) = 32),
    CONSTRAINT ck_inbox_completion CHECK (completed_at >= received_at),
    CONSTRAINT ck_inbox_disposition CHECK (disposition IN ('PROCESSED', 'REJECTED', 'FAILED', 'PENDING_REFERENCE'))
);

CREATE INDEX ix_inbox_transaction ON inbox_messages (transaction_id);

CREATE TABLE message_diagnostics (
    consumer_name       text        NOT NULL,
    source_queue        text        NOT NULL,
    broker_message_id   text        NOT NULL,
    body_hash           bytea       NOT NULL,
    code                text        NOT NULL,
    envelope_message_id text,
    correlation_id      text,
    created_at          timestamptz NOT NULL,
    PRIMARY KEY (consumer_name, source_queue, broker_message_id, body_hash),
    CONSTRAINT ck_diag_consumer CHECK (consumer_name <> '' AND char_length(consumer_name) <= 128),
    CONSTRAINT ck_diag_queue CHECK (source_queue <> '' AND char_length(source_queue) <= 256),
    CONSTRAINT ck_diag_broker_id CHECK (broker_message_id <> '' AND char_length(broker_message_id) <= 256),
    CONSTRAINT ck_diag_hash CHECK (octet_length(body_hash) = 32),
    CONSTRAINT ck_diag_code CHECK (code ~ '^[A-Z][A-Z0-9_]{0,63}$')
);

-- +goose StatementBegin
CREATE FUNCTION message_records_forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is append-only (% not allowed)', TG_TABLE_NAME, TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_inbox_no_update_delete BEFORE UPDATE OR DELETE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION message_records_forbid_mutation();
CREATE TRIGGER trg_inbox_no_truncate BEFORE TRUNCATE ON inbox_messages
    FOR EACH STATEMENT EXECUTE FUNCTION message_records_forbid_mutation();
CREATE TRIGGER trg_diag_no_update_delete BEFORE UPDATE OR DELETE ON message_diagnostics
    FOR EACH ROW EXECUTE FUNCTION message_records_forbid_mutation();
CREATE TRIGGER trg_diag_no_truncate BEFORE TRUNCATE ON message_diagnostics
    FOR EACH STATEMENT EXECUTE FUNCTION message_records_forbid_mutation();

-- +goose Down

-- A reversão descarta o registro de deduplicação por mensagem e os diagnósticos (a idempotência financeira vive em
-- wager_transactions e não é afetada). `down-to 0` é o reset deliberadamente destrutivo de sempre.
DROP TABLE message_diagnostics;
DROP TABLE inbox_messages;
DROP FUNCTION message_records_forbid_mutation();
