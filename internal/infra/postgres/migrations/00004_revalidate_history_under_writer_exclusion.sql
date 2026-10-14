-- +goose Up

-- Migration corretiva sobre 00002 e 00003 (já publicadas e executadas; não são reescritas).
--
-- Janela corrigida: uma migration que valida o histórico (preflight) e, depois, instala as
-- garantias não vê o commit de um escritor do schema anterior que já inseriu linhas e mantém a
-- transação aberta. A instalação do trigger espera esse escritor; ele confirma sob as garantias
-- ANTIGAS e a migration termina com sucesso, deixando histórico incompatível. A atomicidade da
-- transação SQL da migration não inclui o commit de outras conexões, e o lock de sessão do
-- migrador só coordena migrations concorrentes, não os escritores da aplicação. A 00002 (validar,
-- depois instalar a cadeia do ledger) e a 00003 (validar, depois instalar o movimento obrigatório)
-- têm a mesma forma.
--
-- Solução: exclusão dos escritores pelas próprias tabelas que eles escrevem.
--   1. LOCK TABLE ... IN SHARE ROW EXCLUSIVE MODE em wallets, wager_transactions e
--      wallet_ledger_entries. O modo conflita com ROW EXCLUSIVE (INSERT/UPDATE/DELETE), então:
--        * a migration ESPERA todo escritor que já escreveu e ainda não confirmou — e só então
--          lê; sob READ COMMITTED, o comando seguinte enxerga o commit dele;
--        * todo escritor novo fica retido até o fim desta transação e, quando prossegue, já
--          encontra as garantias verificadas;
--        * leituras (SELECT) e o lock de linha do caso de uso (SELECT ... FOR UPDATE) não são
--          bloqueados.
--      A ordem (carteira, transação, lançamento) é a do caso de uso, para não formar ciclos de
--      espera com ele.
--   2. Com os escritores excluídos, TODO o histórico é relido (cadeia do ledger, coerência
--      lançamento/transação, saldo × ledger, movimento obrigatório e resultado = balance_after) e
--      os triggers que sustentam as garantias são conferidos (existem e estão habilitados). Tudo
--      na mesma transação, até o COMMIT da migration: validar e concluir são indivisíveis para
--      os escritores.
--   3. Se algo estiver incompatível — inclusive em bancos onde a janela já ocorreu —, a migration
--      aborta com contagem e primeiros ids. Nada é apagado nem reescrito; o schema permanece na
--      versão anterior e a aplicação (que exige esta versão) recusa iniciar. A correção dos
--      registros é manual.
--
-- lock_timeout: se um escritor antigo ficar com a transação aberta por mais de 30 s, a migration
-- falha sem efeito (os escritores retidos são liberados) e pode ser repetida numa janela de
-- manutenção; sem o limite, novos escritores ficariam enfileirados atrás da migration por tempo
-- indeterminado. Esta migration não instala objetos novos, então sua reversão é vazia.

SET LOCAL lock_timeout = '30s';
LOCK TABLE wallets, wager_transactions, wallet_ledger_entries IN SHARE ROW EXCLUSIVE MODE;

-- +goose StatementBegin
DO $$
DECLARE
    chk     record;
    bad     bigint;
    samples text;
BEGIN
    -- Garantias presentes e habilitadas (um trigger desabilitado ou ausente invalida a validação).
    FOR chk IN SELECT * FROM (VALUES
        ('wallets', 'trg_wallets_guard'), ('wallets', 'trg_wallets_no_truncate'),
        ('wallets', 'ctrg_wallets_balance_matches_ledger'),
        ('wager_transactions', 'trg_wager_tx_guard'), ('wager_transactions', 'trg_wager_tx_no_truncate'),
        ('wager_transactions', 'ctrg_wager_tx_requires_movement'),
        ('wallet_ledger_entries', 'trg_ledger_no_update_delete'), ('wallet_ledger_entries', 'trg_ledger_no_truncate'),
        ('wallet_ledger_entries', 'trg_ledger_enforce_chain'), ('wallet_ledger_entries', 'ctrg_ledger_matches_transaction'),
        ('wallet_ledger_entries', 'ctrg_ledger_balance_matches_wallet'),
        ('outbox_events', 'trg_outbox_events_guard')
    ) AS g(tbl, trg) LOOP
        IF NOT EXISTS (SELECT 1 FROM pg_trigger
                       WHERE tgrelid = to_regclass(chk.tbl) AND tgname = chk.trg AND tgenabled = 'O' AND NOT tgisinternal) THEN
            RAISE EXCEPTION 'preflight: guarantee trigger % on % is missing or disabled', chk.trg, chk.tbl;
        END IF;
    END LOOP;

    -- Histórico completo, sob exclusão dos escritores. Cada consulta devolve o id da transação
    -- (ou da carteira) infratora.
    FOR chk IN SELECT * FROM (VALUES
        (1, 'ledger entr(ies) break the balance chain of their wallet',
         $q$SELECT c.transaction_id AS id FROM (
                SELECT transaction_id, balance_before,
                       COALESCE(lag(balance_after) OVER (PARTITION BY wallet_id ORDER BY seq), 0) AS expected_before
                FROM wallet_ledger_entries) c
            WHERE c.balance_before <> c.expected_before$q$),
        (2, 'ledger entr(ies) inconsistent with their financial transaction',
         $q$SELECT l.transaction_id AS id FROM wallet_ledger_entries l
            JOIN wager_transactions t ON t.id = l.transaction_id
            WHERE t.wallet_id <> l.wallet_id OR t.status <> 'PROCESSED' OR t.amount <> l.amount
               OR t.kind = 'LOSS'
               OR (t.kind = 'BET' AND l.direction <> 'DEBIT')
               OR (t.kind IN ('OPENING', 'WIN', 'REFUND') AND l.direction <> 'CREDIT')$q$),
        (3, 'wallet(s) whose balance differs from the ledger',
         $q$SELECT w.id FROM wallets w
            WHERE w.balance <> COALESCE((SELECT l.balance_after FROM wallet_ledger_entries l
                                         WHERE l.wallet_id = w.id ORDER BY l.seq DESC LIMIT 1), 0)$q$),
        (4, 'PROCESSED movement transaction(s) without a ledger entry',
         $q$SELECT t.id FROM wager_transactions t
            WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS'
              AND NOT EXISTS (SELECT 1 FROM wallet_ledger_entries l
                              WHERE l.transaction_id = t.id AND l.wallet_id = t.wallet_id)$q$),
        (5, 'PROCESSED movement transaction(s) whose result_balance differs from the balance_after of their ledger entry',
         $q$SELECT t.id FROM wager_transactions t
            JOIN wallet_ledger_entries l ON l.transaction_id = t.id AND l.wallet_id = t.wallet_id
            WHERE t.status = 'PROCESSED' AND t.kind <> 'LOSS' AND t.result_balance IS DISTINCT FROM l.balance_after$q$)
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

-- +goose Down

-- Esta migration só valida (sob exclusão dos escritores); não instala nada. Reverter apenas
-- retrocede a versão do schema e não altera dados nem garantias.
SELECT 1;
