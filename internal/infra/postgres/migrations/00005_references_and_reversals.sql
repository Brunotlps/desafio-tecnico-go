-- +goose Up

-- Referências, reversões e pendências duráveis. As migrations 00001–00004 já foram publicadas e
-- não são reescritas; esta instala, no banco, as garantias do incremento de reversões:
--
--  (1) Referência interna coerente com a externa: mesmo provedor, id externo referenciado,
--      jogador, carteira, moeda e rodada; alvo PROCESSED e de tipo elegível (REFUND → BET;
--      WIN → BET; ROLLBACK → BET, WIN ou REFUND); reversões com valor idêntico ao do alvo.
--  (2) REFUND/ROLLBACK PROCESSED e WIN PROCESSED com referência externa exigem a referência
--      interna resolvida.
--  (3) No máximo UMA reversão (REFUND ou ROLLBACK) PROCESSED por alvo, em todo o histórico.
--  (4) Sentido do lançamento de ROLLBACK conforme o alvo (BET → crédito; WIN/REFUND → débito).
--  (5) Transações não terminais mudam só em campos permitidos e só em transições válidas; os
--      metadados originais (rodada, jogo, referência externa, criação) são imutáveis, e a
--      referência interna, uma vez resolvida, não muda.
--  (6) PENDING nunca é confirmado (não há aceite assíncrono); PENDING_REFERENCE é confirmado
--      sempre junto com a agenda aberta (tabela pending_references), e uma transação terminal
--      não deixa agenda aberta. Verificações deferidas, avaliadas no COMMIT.
--
-- Histórico: NÃO é corrigido automaticamente. Com os escritores excluídos (mesmo mecanismo da
-- 00004: LOCK TABLE ... SHARE ROW EXCLUSIVE, na ordem carteira, transação, lançamento), um
-- preflight aborta a migration, com contagem e primeiros ids, se houver registros incompatíveis;
-- nada é alterado e a aplicação (que exige a versão final) recusa iniciar.

SET LOCAL lock_timeout = '30s';
LOCK TABLE wallets, wager_transactions, wallet_ledger_entries IN SHARE ROW EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
DECLARE
    chk     record;
    bad     bigint;
    samples text;
BEGIN
    FOR chk IN SELECT * FROM (VALUES
        (1, 'transaction(s) committed as PENDING (only PENDING_REFERENCE may wait, with a schedule)',
         $q$SELECT id FROM wager_transactions WHERE status = 'PENDING'$q$),
        (2, 'PROCESSED reversal(s) or WIN(s) with a reference but no resolved internal reference',
         $q$SELECT id FROM wager_transactions
            WHERE status = 'PROCESSED' AND reference_transaction_id IS NULL
              AND (kind IN ('REFUND', 'ROLLBACK') OR (kind = 'WIN' AND reference_external_transaction_id IS NOT NULL))$q$),
        (3, 'transaction(s) whose internal reference is incoherent with the external reference, scope, kind, state or amount',
         $q$SELECT t.id FROM wager_transactions t LEFT JOIN wager_transactions g ON g.id = t.reference_transaction_id
            WHERE t.reference_transaction_id IS NOT NULL AND (
                  g.id IS NULL OR t.reference_external_transaction_id IS NULL OR t.reference_transaction_id = t.id
               OR g.provider_id IS DISTINCT FROM t.provider_id
               OR g.external_transaction_id IS DISTINCT FROM t.reference_external_transaction_id
               OR g.player_id <> t.player_id OR g.wallet_id <> t.wallet_id OR g.currency <> t.currency
               OR g.round_id IS DISTINCT FROM t.round_id OR g.status <> 'PROCESSED'
               OR NOT ((t.kind IN ('REFUND', 'WIN') AND g.kind = 'BET')
                       OR (t.kind = 'ROLLBACK' AND g.kind IN ('BET', 'WIN', 'REFUND')))
               OR (t.kind IN ('REFUND', 'ROLLBACK') AND g.amount <> t.amount))$q$),
        (4, 'target(s) with more than one PROCESSED reversal',
         $q$SELECT reference_transaction_id AS id FROM wager_transactions
            WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED' AND reference_transaction_id IS NOT NULL
            GROUP BY reference_transaction_id HAVING count(*) > 1$q$),
        (5, 'ROLLBACK ledger entr(ies) whose direction does not oppose the target movement',
         $q$SELECT l.transaction_id AS id FROM wallet_ledger_entries l
            JOIN wager_transactions t ON t.id = l.transaction_id AND t.kind = 'ROLLBACK'
            LEFT JOIN wager_transactions g ON g.id = t.reference_transaction_id
            WHERE g.id IS NULL OR (g.kind = 'BET' AND l.direction <> 'CREDIT')
               OR (g.kind IN ('WIN', 'REFUND') AND l.direction <> 'DEBIT')$q$)
    ) AS c(ord, label, ids_sql) ORDER BY ord LOOP
        EXECUTE 'SELECT count(*) FROM (' || chk.ids_sql || ') s' INTO bad;
        IF bad > 0 THEN
            EXECUTE 'SELECT string_agg(id::text, '', '' ORDER BY id) FROM (SELECT id FROM (' || chk.ids_sql || ') s ORDER BY id LIMIT 5) x'
                INTO samples;
            RAISE EXCEPTION 'preflight under writer exclusion: % % (first ids: %)', bad, chk.label, samples;
        END IF;
    END LOOP;
END
$$;
-- +goose StatementEnd

-- (2) e coerência estrutural.
ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_reference_pair
    CHECK (reference_transaction_id IS NULL OR reference_external_transaction_id IS NOT NULL);
ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_reference_not_self
    CHECK (reference_transaction_id IS NULL OR reference_transaction_id <> id);
ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_processed_reference CHECK (
    status <> 'PROCESSED'
    OR NOT (kind IN ('REFUND', 'ROLLBACK') OR (kind = 'WIN' AND reference_external_transaction_id IS NOT NULL))
    OR reference_transaction_id IS NOT NULL
);

-- (3) Uma reversão bem-sucedida por alvo (REFUND e ROLLBACK compartilham o mesmo índice).
CREATE UNIQUE INDEX uq_wager_tx_one_reversal_per_target ON wager_transactions (reference_transaction_id)
    WHERE kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED';

-- (1) Coerência da referência resolvida. BEFORE: o alvo já existe quando a referente é gravada.
-- +goose StatementBegin
CREATE FUNCTION wager_tx_reference_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    tgt wager_transactions%ROWTYPE;
BEGIN
    SELECT * INTO tgt FROM wager_transactions WHERE id = NEW.reference_transaction_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'transaction % references a transaction that does not exist', NEW.id
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    IF tgt.provider_id IS DISTINCT FROM NEW.provider_id
       OR tgt.external_transaction_id IS DISTINCT FROM NEW.reference_external_transaction_id
       OR tgt.player_id <> NEW.player_id OR tgt.wallet_id <> NEW.wallet_id OR tgt.currency <> NEW.currency
       OR tgt.round_id IS DISTINCT FROM NEW.round_id THEN
        RAISE EXCEPTION 'transaction % reference is outside its scope (provider, external id, player, wallet, currency or round)', NEW.id
            USING ERRCODE = 'check_violation';
    END IF;
    IF tgt.status <> 'PROCESSED' THEN
        RAISE EXCEPTION 'transaction % references a transaction that is % (not PROCESSED)', NEW.id, tgt.status
            USING ERRCODE = 'check_violation';
    END IF;
    IF NOT ((NEW.kind IN ('REFUND', 'WIN') AND tgt.kind = 'BET')
            OR (NEW.kind = 'ROLLBACK' AND tgt.kind IN ('BET', 'WIN', 'REFUND'))) THEN
        RAISE EXCEPTION 'a % transaction cannot reference a % transaction', NEW.kind, tgt.kind
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.kind IN ('REFUND', 'ROLLBACK') AND tgt.amount <> NEW.amount THEN
        RAISE EXCEPTION 'reversal % amount % differs from the referenced amount %', NEW.id, NEW.amount, tgt.amount
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_wager_tx_reference_guard
    BEFORE INSERT OR UPDATE ON wager_transactions
    FOR EACH ROW WHEN (NEW.reference_transaction_id IS NOT NULL)
    EXECUTE FUNCTION wager_tx_reference_guard();

-- (5) Mutabilidade restrita das transações não terminais (substitui a função da 00001).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
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
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.round_id IS DISTINCT FROM OLD.round_id
       OR NEW.game_id IS DISTINCT FROM OLD.game_id
       OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wager transaction % identity fields are immutable', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.reference_transaction_id IS NOT NULL AND NEW.reference_transaction_id IS DISTINCT FROM OLD.reference_transaction_id THEN
        RAISE EXCEPTION 'wager transaction % reference is immutable once resolved', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.status <> OLD.status AND NOT (
           (OLD.status = 'PENDING' AND NEW.status IN ('PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED'))
        OR (OLD.status = 'PENDING_REFERENCE' AND NEW.status IN ('PROCESSED', 'REJECTED', 'FAILED'))) THEN
        RAISE EXCEPTION 'wager transaction % cannot go from % to %', OLD.id, OLD.status, NEW.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.updated_at < OLD.updated_at THEN
        RAISE EXCEPTION 'wager transaction % updated_at cannot regress', OLD.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- (6) Agenda durável das pendências. O prazo (expires_at) é fixado na primeira persistência e
-- nunca muda; replay, reinício e tentativas só mexem em next_attempt_at/attempts/diagnóstico.
CREATE TABLE pending_references (
    transaction_id     uuid        PRIMARY KEY,
    wallet_id          uuid        NOT NULL,
    first_persisted_at timestamptz NOT NULL,
    expires_at         timestamptz NOT NULL,
    next_attempt_at    timestamptz NOT NULL,
    attempts           integer     NOT NULL DEFAULT 0,
    last_attempt_at    timestamptz,
    last_error         text,
    completed_at       timestamptz,
    CONSTRAINT fk_pending_ref_transaction FOREIGN KEY (transaction_id, wallet_id)
        REFERENCES wager_transactions (id, wallet_id),
    CONSTRAINT ck_pending_ref_attempts  CHECK (attempts >= 0),
    CONSTRAINT ck_pending_ref_expiry    CHECK (expires_at > first_persisted_at),
    CONSTRAINT ck_pending_ref_completed CHECK (completed_at IS NULL OR completed_at >= first_persisted_at)
);
CREATE INDEX ix_pending_ref_due ON pending_references (next_attempt_at) WHERE completed_at IS NULL;

-- +goose StatementBegin
CREATE FUNCTION pending_references_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' OR TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'pending_references rows cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.completed_at IS NOT NULL THEN
        RAISE EXCEPTION 'pending reference % is completed', OLD.transaction_id USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.transaction_id <> OLD.transaction_id OR NEW.wallet_id <> OLD.wallet_id
       OR NEW.first_persisted_at <> OLD.first_persisted_at OR NEW.expires_at <> OLD.expires_at THEN
        RAISE EXCEPTION 'pending reference % identity and deadline are immutable', OLD.transaction_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.attempts < OLD.attempts THEN
        RAISE EXCEPTION 'pending reference % attempts cannot decrease', OLD.transaction_id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_pending_references_guard
    BEFORE UPDATE OR DELETE ON pending_references
    FOR EACH ROW EXECUTE FUNCTION pending_references_guard();
CREATE TRIGGER trg_pending_references_no_truncate
    BEFORE TRUNCATE ON pending_references
    FOR EACH STATEMENT EXECUTE FUNCTION pending_references_guard();

-- Coerência no COMMIT: nenhuma transação PENDING; PENDING_REFERENCE com agenda aberta; terminal
-- sem agenda aberta; agenda aberta só para PENDING_REFERENCE.
-- +goose StatementBegin
CREATE FUNCTION pending_reference_consistency() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    tx_id    uuid;
    st       text;
    has_row  boolean;
    is_open  boolean;
BEGIN
    IF TG_TABLE_NAME = 'wager_transactions' THEN
        tx_id := NEW.id;
    ELSE
        tx_id := NEW.transaction_id;
    END IF;
    SELECT status INTO st FROM wager_transactions WHERE id = tx_id;
    SELECT true, completed_at IS NULL INTO has_row, is_open FROM pending_references WHERE transaction_id = tx_id;
    has_row := COALESCE(has_row, false);
    is_open := COALESCE(is_open, false);

    IF st = 'PENDING' THEN
        RAISE EXCEPTION 'transaction % cannot be committed as PENDING', tx_id USING ERRCODE = 'check_violation';
    END IF;
    IF st = 'PENDING_REFERENCE' AND NOT is_open THEN
        RAISE EXCEPTION 'transaction % is PENDING_REFERENCE without an open schedule', tx_id USING ERRCODE = 'check_violation';
    END IF;
    IF st IN ('PROCESSED', 'REJECTED', 'FAILED') AND is_open THEN
        RAISE EXCEPTION 'transaction % is % but still has an open schedule', tx_id, st USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER ctrg_wager_tx_pending_consistency
    AFTER INSERT OR UPDATE ON wager_transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION pending_reference_consistency();
CREATE CONSTRAINT TRIGGER ctrg_pending_references_consistency
    AFTER INSERT OR UPDATE ON pending_references
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION pending_reference_consistency();

-- (4) Sentido do lançamento de ROLLBACK (substitui a função da 00003).
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ledger_entry_matches_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    t_kind   text;
    t_status text;
    t_amount bigint;
    t_result bigint;
    t_ref    uuid;
    g_kind   text;
BEGIN
    SELECT kind, status, amount, result_balance, reference_transaction_id INTO t_kind, t_status, t_amount, t_result, t_ref
        FROM wager_transactions WHERE id = NEW.transaction_id;

    IF t_status <> 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger entry % references a transaction that is % (not PROCESSED)', NEW.id, t_status
            USING ERRCODE = 'check_violation';
    END IF;
    IF t_amount <> NEW.amount THEN
        RAISE EXCEPTION 'ledger entry % amount % differs from transaction amount %', NEW.id, NEW.amount, t_amount
            USING ERRCODE = 'check_violation';
    END IF;
    IF t_kind = 'LOSS'
       OR (t_kind = 'BET' AND NEW.direction <> 'DEBIT')
       OR (t_kind IN ('OPENING', 'WIN', 'REFUND') AND NEW.direction <> 'CREDIT') THEN
        RAISE EXCEPTION 'ledger entry % direction % is not valid for a % transaction', NEW.id, NEW.direction, t_kind
            USING ERRCODE = 'check_violation';
    END IF;
    IF t_kind = 'ROLLBACK' THEN
        SELECT kind INTO g_kind FROM wager_transactions WHERE id = t_ref;
        IF g_kind IS NULL
           OR (g_kind = 'BET' AND NEW.direction <> 'CREDIT')
           OR (g_kind IN ('WIN', 'REFUND') AND NEW.direction <> 'DEBIT')
           OR g_kind NOT IN ('BET', 'WIN', 'REFUND') THEN
            RAISE EXCEPTION 'ledger entry % direction % does not oppose the movement of the % transaction rolled back', NEW.id, NEW.direction, g_kind
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;
    IF t_result IS DISTINCT FROM NEW.balance_after THEN
        RAISE EXCEPTION 'ledger entry % balance_after % differs from the result_balance % of its transaction',
            NEW.id, NEW.balance_after, t_result
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

-- Pendências que sobreviveram a um ciclo de reversão desta migration (a reversão apaga a tabela
-- da agenda, mas não as transações): recebem uma agenda nova, para não ficarem sem retomada. O
-- prazo é renovado nesse caso excepcional (antes desta migration nenhuma pendência era gravada).
INSERT INTO pending_references (transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at)
    SELECT id, wallet_id, created_at, GREATEST(created_at, now()) + interval '15 minutes', now()
    FROM wager_transactions WHERE status = 'PENDING_REFERENCE';

-- +goose Down

-- Remove as garantias desta migration e restaura as funções das migrations anteriores. Não altera
-- transações, ledger nem carteiras. A agenda (pending_references) é descartada: pendências ainda
-- abertas continuam como PENDING_REFERENCE, sem retomada até um novo `migrate up` (que recria a
-- agenda delas).
DROP TRIGGER IF EXISTS ctrg_pending_references_consistency ON pending_references;
DROP TRIGGER IF EXISTS ctrg_wager_tx_pending_consistency ON wager_transactions;
DROP FUNCTION IF EXISTS pending_reference_consistency();
DROP TABLE pending_references;
DROP FUNCTION IF EXISTS pending_references_guard();
DROP TRIGGER IF EXISTS trg_wager_tx_reference_guard ON wager_transactions;
DROP FUNCTION IF EXISTS wager_tx_reference_guard();
DROP INDEX IF EXISTS uq_wager_tx_one_reversal_per_target;
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_processed_reference;
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_reference_not_self;
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_reference_pair;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION wager_transactions_guard() RETURNS trigger LANGUAGE plpgsql AS $$
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

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ledger_entry_matches_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    t_kind   text;
    t_status text;
    t_amount bigint;
    t_result bigint;
BEGIN
    SELECT kind, status, amount, result_balance INTO t_kind, t_status, t_amount, t_result
        FROM wager_transactions WHERE id = NEW.transaction_id;

    IF t_status <> 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger entry % references a transaction that is % (not PROCESSED)', NEW.id, t_status
            USING ERRCODE = 'check_violation';
    END IF;
    IF t_amount <> NEW.amount THEN
        RAISE EXCEPTION 'ledger entry % amount % differs from transaction amount %', NEW.id, NEW.amount, t_amount
            USING ERRCODE = 'check_violation';
    END IF;
    IF t_kind = 'LOSS'
       OR (t_kind = 'BET' AND NEW.direction <> 'DEBIT')
       OR (t_kind IN ('OPENING', 'WIN', 'REFUND') AND NEW.direction <> 'CREDIT') THEN
        RAISE EXCEPTION 'ledger entry % direction % is not valid for a % transaction', NEW.id, NEW.direction, t_kind
            USING ERRCODE = 'check_violation';
    END IF;
    IF t_result IS DISTINCT FROM NEW.balance_after THEN
        RAISE EXCEPTION 'ledger entry % balance_after % differs from the result_balance % of its transaction',
            NEW.id, NEW.balance_after, t_result
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd
