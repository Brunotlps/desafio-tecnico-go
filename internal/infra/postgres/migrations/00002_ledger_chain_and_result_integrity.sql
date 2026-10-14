-- +goose Up

-- Migration corretiva sobre 00001, aplicável a bancos já inicializados sem apagar dados.
--
-- 1. Cadeia do ledger: balance_before de cada lançamento deve ser o balance_after do
--    lançamento anterior da mesma carteira (0 para o primeiro). Com a cadeia contínua e o
--    saldo da carteira igual ao balance_after do último lançamento (00001), o saldo é,
--    por indução, créditos - débitos.
-- 2. Vínculo: o lançamento pertence à mesma carteira da transação financeira que o gerou, que
--    precisa estar PROCESSED, com mesmo valor e sentido coerente com o tipo.
-- 3. Resultado: PROCESSED e REJECTED exigem result_balance NOT NULL (um CHECK trata NULL
--    como "desconhecido" e o aceitaria).
--
-- Dados históricos NÃO são corrigidos automaticamente. A verificação abaixo aborta a
-- migration, com a contagem do problema, se encontrar inconsistências; nesse caso
-- investigue e corrija os registros manualmente antes de reaplicar.

-- +goose StatementBegin
DO $$
DECLARE
    bad bigint;
BEGIN
    SELECT count(*) INTO bad FROM wager_transactions
        WHERE status IN ('PROCESSED', 'REJECTED') AND result_balance IS NULL;
    IF bad > 0 THEN
        RAISE EXCEPTION 'preflight: % PROCESSED/REJECTED transaction(s) with NULL result_balance', bad;
    END IF;

    SELECT count(*) INTO bad FROM wager_transactions
        WHERE failure_code IS NOT NULL AND failure_code !~ '^[A-Z][A-Z0-9_]{1,63}$';
    IF bad > 0 THEN
        RAISE EXCEPTION 'preflight: % transaction(s) with an invalid failure_code', bad;
    END IF;

    SELECT count(*) INTO bad FROM (
        SELECT balance_before,
               COALESCE(lag(balance_after) OVER (PARTITION BY wallet_id ORDER BY seq), 0) AS expected_before
        FROM wallet_ledger_entries
    ) chain WHERE balance_before <> expected_before;
    IF bad > 0 THEN
        RAISE EXCEPTION 'preflight: % ledger entr(ies) break the balance chain of their wallet', bad;
    END IF;

    SELECT count(*) INTO bad FROM wallet_ledger_entries l
        JOIN wager_transactions t ON t.id = l.transaction_id
        WHERE t.wallet_id <> l.wallet_id OR t.status <> 'PROCESSED' OR t.amount <> l.amount
           OR t.kind = 'LOSS'
           OR (t.kind = 'BET' AND l.direction <> 'DEBIT')
           OR (t.kind IN ('OPENING', 'WIN', 'REFUND') AND l.direction <> 'CREDIT');
    IF bad > 0 THEN
        RAISE EXCEPTION 'preflight: % ledger entr(ies) inconsistent with their financial transaction', bad;
    END IF;

    SELECT count(*) INTO bad FROM wallets w
        WHERE w.balance <> COALESCE((SELECT l.balance_after FROM wallet_ledger_entries l
                                     WHERE l.wallet_id = w.id ORDER BY l.seq DESC LIMIT 1), 0);
    IF bad > 0 THEN
        RAISE EXCEPTION 'preflight: % wallet(s) whose balance differs from the ledger', bad;
    END IF;
END
$$;
-- +goose StatementEnd

-- (2) Resultado explícito: NOT NULL onde é exigido, sem alterar as regras de PENDING*/FAILED.
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_status_shape;
ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_status_shape CHECK (
    (status IN ('PENDING', 'PENDING_REFERENCE') AND failure_code IS NULL AND result_balance IS NULL)
    OR (status = 'PROCESSED' AND failure_code IS NULL AND result_balance IS NOT NULL AND result_balance >= 0)
    OR (status = 'REJECTED'  AND failure_code IS NOT NULL AND result_balance IS NOT NULL AND result_balance >= 0)
    OR (status = 'FAILED'    AND failure_code IS NOT NULL)
);
ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_failure_code_format
    CHECK (failure_code IS NULL OR failure_code ~ '^[A-Z][A-Z0-9_]{1,63}$');

-- (2) Vínculo lançamento -> transação da mesma carteira.
ALTER TABLE wager_transactions ADD CONSTRAINT uq_wager_tx_id_wallet UNIQUE (id, wallet_id);
ALTER TABLE wallet_ledger_entries ADD CONSTRAINT fk_ledger_transaction_wallet
    FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions (id, wallet_id);

-- (1) Continuidade da cadeia, verificada a cada INSERT. O trigger trava a linha da carteira
-- (mesmo nível de lock do UPDATE de saldo), o que serializa escritores da MESMA carteira
-- mesmo em SQL direto; carteiras diferentes não se bloqueiam. Depois do lock, o SELECT
-- (READ COMMITTED) enxerga os lançamentos confirmados por quem estava na frente e os já
-- inseridos pela própria transação, então vários lançamentos numa transação são validados em
-- sequência. Um seq menor que o último existente (o identity é alocado antes do lock) indica
-- escrita fora de ordem e falha como serialization_failure (40001), que a aplicação trata
-- como transitória.
-- +goose StatementBegin
CREATE FUNCTION ledger_enforce_chain() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    last_seq   bigint;
    last_after bigint;
BEGIN
    PERFORM 1 FROM wallets WHERE id = NEW.wallet_id FOR NO KEY UPDATE;

    SELECT seq, balance_after INTO last_seq, last_after
        FROM wallet_ledger_entries WHERE wallet_id = NEW.wallet_id ORDER BY seq DESC LIMIT 1;

    IF last_seq IS NOT NULL AND NEW.seq < last_seq THEN
        RAISE EXCEPTION 'ledger entry for wallet % written out of order', NEW.wallet_id
            USING ERRCODE = 'serialization_failure';
    END IF;
    IF NEW.balance_before <> COALESCE(last_after, 0) THEN
        RAISE EXCEPTION 'ledger chain broken for wallet %: balance_before % but previous balance_after is %',
            NEW.wallet_id, NEW.balance_before, COALESCE(last_after, 0)
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_ledger_enforce_chain
    BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_enforce_chain();

-- (2) Coerência do lançamento com a transação financeira. Deferida: a transação pode ser
-- concluída (PROCESSED) na mesma transação SQL, depois do INSERT do lançamento.
-- +goose StatementBegin
CREATE FUNCTION ledger_entry_matches_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    t_kind   text;
    t_status text;
    t_amount bigint;
BEGIN
    SELECT kind, status, amount INTO t_kind, t_status, t_amount
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
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER ctrg_ledger_matches_transaction
    AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_entry_matches_transaction();

-- +goose Down

DROP TRIGGER IF EXISTS ctrg_ledger_matches_transaction ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS ledger_entry_matches_transaction();
DROP TRIGGER IF EXISTS trg_ledger_enforce_chain ON wallet_ledger_entries;
DROP FUNCTION IF EXISTS ledger_enforce_chain();
ALTER TABLE wallet_ledger_entries DROP CONSTRAINT fk_ledger_transaction_wallet;
ALTER TABLE wager_transactions DROP CONSTRAINT uq_wager_tx_id_wallet;
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_failure_code_format;
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_status_shape;
ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_status_shape CHECK (
    (status IN ('PENDING', 'PENDING_REFERENCE') AND failure_code IS NULL AND result_balance IS NULL)
    OR (status = 'PROCESSED' AND failure_code IS NULL AND result_balance >= 0)
    OR (status = 'REJECTED'  AND failure_code IS NOT NULL AND result_balance >= 0)
    OR (status = 'FAILED'    AND failure_code IS NOT NULL)
);
