package postgres

import (
	"context"
	"fmt"
)

// Preparação do rollback da migration 00005, executada por Migrator ANTES de reverter a 00005 (o Down publicado dela
// apaga `pending_references`, e o seu Up recria a agenda com prazos inventados). Sob exclusão dos escritores
// (LOCK TABLE ... SHARE ROW EXCLUSIVE, na ordem carteira, transação, agenda, a mesma das migrations):
//
//  1. revoga toda escrita na agenda para qualquer papel que não seja o dono: o runtime passa a falhar em voz alta
//     (permission denied) em vez de gravar linhas que seriam apagadas;
//  2. guarda uma cópia integral da agenda — início, vencimento, próxima tentativa, tentativas, diagnóstico e as
//     agendas concluídas — em `pending_references_preserved`.
//
// A agenda original permanece intacta até a reversão da 00005 apagá-la; a migration 00006 restaura a cópia, com os
// valores originais, quando é reaplicada, e a remove. Idempotente: uma nova preparação substitui a cópia anterior.
// Se a reversão da 00005 falhar depois da preparação, a agenda continua íntegra; `migrate grants` devolve a escrita.
const preserveScheduleDo = `DO $$
DECLARE
    owner_oid oid;
    g         record;
BEGIN
    SELECT relowner INTO owner_oid FROM pg_class WHERE oid = 'public.pending_references'::regclass;
    FOR g IN SELECT DISTINCT a.grantee FROM pg_class c, LATERAL aclexplode(c.relacl) a
             WHERE c.oid = 'public.pending_references'::regclass AND a.grantee <> owner_oid LOOP
        IF g.grantee = 0 THEN
            EXECUTE 'REVOKE ALL ON public.pending_references FROM PUBLIC';
        ELSE
            EXECUTE format('REVOKE ALL ON public.pending_references FROM %I', (SELECT rolname FROM pg_roles WHERE oid = g.grantee));
        END IF;
    END LOOP;
    DROP TABLE IF EXISTS public.pending_references_preserved;
    CREATE TABLE public.pending_references_preserved AS SELECT *, now() AS preserved_at FROM public.pending_references;
END
$$;`

func (m *Migrator) preserveSchedule(ctx context.Context) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.pending_references') IS NOT NULL`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	for _, stmt := range []string{
		`SET LOCAL lock_timeout = '30s'`,
		`LOCK TABLE wallets, wager_transactions, pending_references IN SHARE ROW EXCLUSIVE MODE`,
		preserveScheduleDo,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("preserve the pending reference schedule before rolling back migration 00005: %w", err)
		}
	}
	return tx.Commit()
}

// dropPreservedSchedule descarta a cópia: usada apenas no reset deliberadamente destrutivo (down-to 0).
func (m *Migrator) dropPreservedSchedule(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `DROP TABLE IF EXISTS public.pending_references_preserved`)
	return err
}
