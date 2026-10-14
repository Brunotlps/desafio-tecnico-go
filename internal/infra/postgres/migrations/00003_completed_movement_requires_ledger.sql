-- +goose Up

-- Migration corretiva sobre 00001 e 00002 (já publicadas e executadas; não são reescritas).
--
-- Lacuna: as garantias existentes partiam do lançamento no ledger (ele exige uma transação
-- PROCESSED coerente), nunca da transação. Era possível confirmar uma transação PROCESSED que
-- exige movimento (BET, WIN, REFUND, ROLLBACK, OPENING) sem lançamento e sem alterar a carteira,
-- ou com um result_balance diferente do saldo posterior do lançamento; o replay devolvia esse
-- resultado fictício.
--
-- Garantia nova, avaliada no COMMIT:
--   (a) toda transação PROCESSED que não seja LOSS tem o lançamento da própria carteira
--       (UNIQUE (wallet_id, transaction_id) garante no máximo um);
--   (b) result_balance = balance_after DESSE lançamento. Não se compara com o saldo atual da
--       carteira: outras operações podem tê-lo alterado depois, inclusive na mesma transação SQL.
-- LOSS e transações REJECTED/FAILED/PENDING* continuam sem lançamento (e com ele são recusadas
-- pela verificação do lado do ledger, 00002).
--
-- Mecanismo: constraint trigger DEFERIDO em wager_transactions (AFTER INSERT OR UPDATE ... WHEN
-- status = PROCESSED e kind <> LOSS), em vez de um CHECK (que não enxerga outra tabela) ou de uma
-- FK (circular com a FK do ledger para a transação). Por ser deferido, a transação e o lançamento
-- podem ser inseridos em qualquer ordem e várias operações podem ser confirmadas na mesma
-- transação SQL; a verificação é somente leitura (não trava nada) e consulta o lançamento pelo
-- índice único (wallet_id, transaction_id). A verificação de resultado também passa a existir no
-- lado do ledger, de modo que cada lado valida o outro.
--
-- Histórico: NÃO é corrigido automaticamente. O bloco abaixo aborta a migration, com a contagem
-- e os primeiros ids, se houver transações concluídas sem lançamento ou com resultado divergente;
-- nesse caso nada é alterado e a correção dos registros é manual.

-- +goose StatementBegin
DO $$
DECLARE
    bad     bigint;
    samples text;
BEGIN
    SELECT count(*) INTO bad FROM wager_transactions t
        WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS'
          AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l
                          WHERE l.transaction_id = t.id AND l.wallet_id = t.wallet_id);
    IF bad > 0 THEN
        SELECT string_agg(id::text, ', ') INTO samples FROM (
            SELECT t.id FROM wager_transactions t
            WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS'
              AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l
                              WHERE l.transaction_id = t.id AND l.wallet_id = t.wallet_id)
            ORDER BY t.created_at, t.id LIMIT 5) s;
        RAISE EXCEPTION 'preflight: % PROCESSED movement transaction(s) without a ledger entry (first ids: %)', bad, samples;
    END IF;

    SELECT count(*) INTO bad FROM wager_transactions t
        JOIN wallet_ledger_entries l ON l.transaction_id = t.id AND l.wallet_id = t.wallet_id
        WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS' AND t.result_balance IS DISTINCT FROM l.balance_after;
    IF bad > 0 THEN
        SELECT string_agg(id::text, ', ') INTO samples FROM (
            SELECT t.id FROM wager_transactions t
            JOIN wallet_ledger_entries l ON l.transaction_id = t.id AND l.wallet_id = t.wallet_id
            WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS' AND t.result_balance IS DISTINCT FROM l.balance_after
            ORDER BY t.created_at, t.id LIMIT 5) s;
        RAISE EXCEPTION 'preflight: % PROCESSED movement transaction(s) whose result_balance differs from the balance_after of their ledger entry (first ids: %)', bad, samples;
    END IF;
END
$$;
-- +goose StatementEnd

-- Lado da transação: avaliado no COMMIT sobre o estado final da linha (não sobre o NEW do
-- evento, que pode ser anterior a uma atualização PENDING -> PROCESSED na mesma transação).
-- +goose StatementBegin
CREATE FUNCTION wager_transaction_requires_movement() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    t_kind   text;
    t_status text;
    t_wallet uuid;
    t_result bigint;
    e_after  bigint;
BEGIN
    SELECT kind, status, wallet_id, result_balance INTO t_kind, t_status, t_wallet, t_result
        FROM wager_transactions WHERE id = NEW.id;

    IF t_status IS DISTINCT FROM 'PROCESSED' OR t_kind = 'LOSS' THEN
        RETURN NULL; -- sem movimento exigido
    END IF;

    SELECT balance_after INTO e_after FROM wallet_ledger_entries
        WHERE transaction_id = NEW.id AND wallet_id = t_wallet;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'transaction % (%) is PROCESSED but has no ledger entry', NEW.id, t_kind
            USING ERRCODE = 'check_violation';
    END IF;
    IF t_result IS DISTINCT FROM e_after THEN
        RAISE EXCEPTION 'transaction % result_balance % differs from the balance_after % of its ledger entry',
            NEW.id, t_result, e_after
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE CONSTRAINT TRIGGER ctrg_wager_tx_requires_movement
    AFTER INSERT OR UPDATE ON wager_transactions
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW WHEN (NEW.status = 'PROCESSED' AND NEW.kind <> 'LOSS')
    EXECUTE FUNCTION wager_transaction_requires_movement();

-- Lado do ledger (substitui a função de 00002 por uma versão que também confere o resultado).
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

-- +goose Down

-- Remove as garantias novas e restaura a função de 00002. Não altera dados.
DROP TRIGGER IF EXISTS ctrg_wager_tx_requires_movement ON wager_transactions;
DROP FUNCTION IF EXISTS wager_transaction_requires_movement();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION ledger_entry_matches_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
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
