-- +goose Up

-- Publisher da outbox (incremento 3): reserva recuperável por lease.
--
-- Um publisher reserva UM evento elegível numa transação curta, grava nele um token único da aquisição e o prazo da lease, e
-- confirma. A publicação no SNS acontece fora de qualquer transação. Depois, a confirmação, o retry e a liberação só alteram a
-- linha se o token ainda for o da aquisição corrente e a lease ainda não tiver vencido (relógio do banco). Uma lease vencida
-- deixa o evento elegível para outro publisher: é assim que se recupera trabalho abandonado, inclusive por um processo morto.
--
--   lease_token       uuid novo a cada aquisição (não é o eventId nem a identidade do processo); NULL = sem reserva.
--   lease_expires_at  instante (relógio do banco) em que a reserva deixa de valer; NULL exatamente quando não há token.
--
-- Os eventos, snapshots, agendas (`next_attempt_at`), `attempts`, `last_error` e `published_at` existentes ficam intactos: as
-- colunas novas são NULL e o backlog confirmado antes desta versão é publicado normalmente.

ALTER TABLE outbox_events
    ADD COLUMN lease_token      uuid,
    ADD COLUMN lease_expires_at timestamptz,
    ADD CONSTRAINT ck_outbox_lease_pair CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    ADD CONSTRAINT ck_outbox_published_has_no_lease CHECK (published_at IS NULL OR lease_token IS NULL);

-- Além do snapshot (já imutável), o marcador de publicação, uma vez gravado, não muda nem volta a NULL.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION outbox_events_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id <> OLD.id OR NEW.seq <> OLD.seq OR NEW.aggregate_type <> OLD.aggregate_type
       OR NEW.aggregate_id <> OLD.aggregate_id OR NEW.event_type <> OLD.event_type
       OR NEW.event_version <> OLD.event_version OR NEW.correlation_id <> OLD.correlation_id
       OR NEW.causation_id IS DISTINCT FROM OLD.causation_id
       OR NEW.payload <> OLD.payload OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event % snapshot is immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox event % is already published (published_at is immutable)', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose Down

-- Restaura a função da 00001. As reservas em curso (transitórias) são descartadas; eventos, agendas, tentativas, diagnósticos e
-- marcadores de publicação permanecem.
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION outbox_events_guard() RETURNS trigger LANGUAGE plpgsql AS $$
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

ALTER TABLE outbox_events
    DROP CONSTRAINT ck_outbox_published_has_no_lease,
    DROP CONSTRAINT ck_outbox_lease_pair,
    DROP COLUMN lease_expires_at,
    DROP COLUMN lease_token;
