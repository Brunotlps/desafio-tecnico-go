-- +goose Up

-- Migration corretiva sobre a 00005 (já publicada e executada; não é reescrita). Resolve dois achados da auditoria.
--
-- (1) Forma de PENDING_REFERENCE imposta pelo banco. A 00005 exigia agenda aberta, mas aceitava uma transação em
--     PENDING_REFERENCE que o domínio e o worker não conseguem retomar (p.ex. uma BET sem referência externa:
--     "invalid transaction: PENDING_REFERENCE requires a reference"). Passam a valer, no schema:
--       * a referência externa, quando existe, tem uma forma que a reidratação aceita (não vazia, no máximo 128
--         caracteres, sem caracteres de controle, sem espaço no início ou no fim, diferente do próprio id externo) —
--         em qualquer estado (o `CHECK` pré-existente só exigia "não nula", e '' passava);
--       * PENDING_REFERENCE só existe para origem externa, tipo WIN, REFUND ou ROLLBACK e com referência externa
--         (BET, LOSS e OPENING, e WIN/REFUND/ROLLBACK sem referência, não podem esperar uma referência).
--     O domínio NÃO é enfraquecido para aceitar esses registros: o banco passa a recusá-los.
--     Com os escritores excluídos (mesmo LOCK TABLE da 00004/00005), um preflight recusa o histórico incompatível com
--     contagem e primeiros ids; nada é corrigido nem alterado, a decisão é humana.
--
-- (2) Preservação da agenda no rollback. O Down da 00005 apaga `pending_references`, e o Up dela a recria com um prazo
--     inventado a partir do instante da reaplicação. Como a 00005 não pode ser reescrita, o procedimento seguro é
--     composto: (a) o migrador (Migrator.Down/DownTo, usado por `migrate down` e `migrate down-to`) executa, ANTES de
--     reverter a 00005, a preparação de rollback sob exclusão dos escritores — revoga a escrita na agenda para todo
--     papel que não seja o dono e guarda uma cópia integral (`pending_references_preserved`: início, vencimento,
--     próxima tentativa, tentativas, diagnóstico e as agendas concluídas); (b) esta migration, ao ser reaplicada depois
--     da 00005, restaura essa cópia por cima das linhas que o Up da 00005 reconstruiu, e a remove. Nada é inventado: as
--     datas e os contadores restaurados são os originais. `down-to 0` é o reset deliberadamente destrutivo (apaga tudo,
--     inclusive a cópia).

SET LOCAL lock_timeout = '30s';
LOCK TABLE wallets, wager_transactions, pending_references IN SHARE ROW EXCLUSIVE MODE;

-- +goose StatementBegin
CREATE FUNCTION wager_valid_external_reference(ref text, own text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$
    SELECT ref IS NOT NULL
       AND ref <> ''
       AND char_length(ref) <= 128
       AND ref !~ '[\x01-\x1f\x7f-\x9f]'
       AND ref !~ '^[[:space:] \u0085  -     　]'
       AND ref !~ '[[:space:] \u0085  -     　]$'
       AND ref IS DISTINCT FROM own
$$;
-- +goose StatementEnd

-- +goose StatementBegin
DO $$
DECLARE
    chk     record;
    bad     bigint;
    samples text;
BEGIN
    FOR chk IN SELECT * FROM (VALUES
        (1, 'PENDING_REFERENCE transaction(s) that cannot be resumed (not an external WIN/REFUND/ROLLBACK with a valid external reference)',
         $q$SELECT id FROM wager_transactions
            WHERE status = 'PENDING_REFERENCE'
              AND NOT (origin = 'EXTERNAL' AND kind IN ('WIN', 'REFUND', 'ROLLBACK')
                       AND wager_valid_external_reference(reference_external_transaction_id, external_transaction_id))$q$),
        (2, 'transaction(s) whose external reference has a form the domain rejects (empty, too long, control characters, surrounding spaces or equal to its own id)',
         $q$SELECT id FROM wager_transactions
            WHERE reference_external_transaction_id IS NOT NULL
              AND NOT wager_valid_external_reference(reference_external_transaction_id, external_transaction_id)$q$),
        (3, 'rows left in pending_references_preserved that do not match a transaction (resolve them manually before reapplying)',
         $q$SELECT p.transaction_id AS id FROM pending_references_preserved p
            WHERE NOT EXISTS (SELECT 1 FROM wager_transactions t WHERE t.id = p.transaction_id AND t.wallet_id = p.wallet_id)$q$)
    ) AS c(ord, label, ids_sql) ORDER BY ord LOOP
        IF chk.ord = 3 AND to_regclass('public.pending_references_preserved') IS NULL THEN
            CONTINUE;
        END IF;
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

ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_reference_form
    CHECK (reference_external_transaction_id IS NULL
           OR wager_valid_external_reference(reference_external_transaction_id, external_transaction_id));
ALTER TABLE wager_transactions ADD CONSTRAINT ck_wager_tx_pending_reference_shape
    CHECK (status <> 'PENDING_REFERENCE'
           OR (origin = 'EXTERNAL' AND kind IN ('WIN', 'REFUND', 'ROLLBACK') AND reference_external_transaction_id IS NOT NULL));

-- Restauração da agenda preservada por um rollback anterior (ver o cabeçalho). Só age se a cópia existe.
-- +goose StatementBegin
DO $$
DECLARE
    restored bigint;
BEGIN
    IF to_regclass('public.pending_references_preserved') IS NULL THEN
        RETURN;
    END IF;
    -- O Up da 00005 reconstruiu, com prazo renovado, uma linha para cada PENDING_REFERENCE; os valores originais
    -- (preservados) substituem essas linhas. A agenda tem gatilho de imutabilidade e de não-remoção: o dono da
    -- migration o desabilita só durante a restauração, sob exclusão dos escritores.
    ALTER TABLE pending_references DISABLE TRIGGER trg_pending_references_guard;
    DELETE FROM pending_references d USING pending_references_preserved p WHERE d.transaction_id = p.transaction_id;
    INSERT INTO pending_references (transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts, last_attempt_at, last_error, completed_at)
        SELECT transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts, last_attempt_at, last_error, completed_at
        FROM pending_references_preserved;
    GET DIAGNOSTICS restored = ROW_COUNT;
    -- Dispara agora as verificações deferidas da agenda (estado final já consistente) para poder reabilitar o gatilho:
    -- o PostgreSQL recusa ALTER TABLE enquanto há eventos de gatilho pendentes.
    SET CONSTRAINTS ALL IMMEDIATE;
    ALTER TABLE pending_references ENABLE TRIGGER trg_pending_references_guard;
    DROP TABLE pending_references_preserved;
    RAISE NOTICE 'restored % preserved pending reference schedule row(s) with their original deadlines and counters', restored;
END
$$;
-- +goose StatementEnd

-- +goose Down

-- Remove as garantias desta migration; não toca em dados. A agenda é preservada pelo migrador antes da reversão da 00005.
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_pending_reference_shape;
ALTER TABLE wager_transactions DROP CONSTRAINT ck_wager_tx_reference_form;
DROP FUNCTION wager_valid_external_reference(text, text);
