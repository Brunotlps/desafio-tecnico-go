package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Preparação do rollback da migration 00005, executada por Migrator ANTES de reverter a 00005 (o Down publicado dela
// apaga `pending_references`, e o seu Up recria a agenda com prazos inventados). O Migrator a executa dentro da região
// serializada por MigratorLockID, junto com a decisão da versão, a reversão e a restauração, de modo que nenhum outro
// migrador as intercala. Sob exclusão dos escritores (LOCK TABLE ... SHARE ROW EXCLUSIVE, na ordem carteira, transação,
// agenda, a mesma das migrations), numa única transação:
//
//  1. captura os privilégios concedidos sobre a agenda — de tabela E de coluna (pg_class.relacl e pg_attribute.attacl),
//     inclusive a PUBLIC — e os revoga de todo papel que não seja o dono. Um REVOKE de tabela também remove os
//     privilégios de coluna do mesmo papel. O que restar é verificado no catálogo: qualquer papel de login (que não seja
//     superusuário, o dono ou a própria sessão do migrador) com escrita efetiva — direta, herdada ou por pertencer a um
//     papel com escrita — faz a preparação RECUSAR, sem alterar nada (a transação é desfeita);
//  2. guarda uma cópia integral da agenda — início, vencimento, próxima tentativa, tentativas, diagnóstico e as agendas
//     concluídas — em `pending_references_preserved`, marcada com um token (COMMENT) que também identifica a tabela viva
//     copiada.
//
// A cópia ORIGINAL nunca é substituída por um estado reconstruído. Se já existe uma cópia:
//   - se a agenda viva é a mesma tabela que foi copiada (mesmo token, ou conteúdo idêntico), um Down anterior falhou ou foi
//     cancelado depois da preparação: a cópia é renovada a partir da agenda viva, que contém tudo (inclusive o que o
//     runtime confirmou depois da devolução dos grants);
//   - caso contrário, a agenda viva é a reconstrução criada pelo Up da 00005 (a cópia é o original): a cópia é mantida e a
//     preparação só prossegue se a agenda viva não tiver nada além da reconstrução intacta (linhas que a cópia tem, sem
//     tentativas, diagnóstico nem conclusão). Qualquer outra coisa seria perdida: o rollback é recusado, com ids, sem
//     alterar nada.
//
// A agenda original permanece intacta até a reversão da 00005 apagá-la; a migration 00006 restaura a cópia, com os valores
// originais, quando é reaplicada, e a remove. Se a reversão falhar ou for cancelada depois da preparação, os grants
// capturados são devolvidos pelo próprio migrador; se o processo morrer antes, `migrate grants` os devolve.

const scheduleTable = "public.pending_references"
const preservedTable = "public.pending_references_preserved"

// scheduleColumns são as colunas da agenda comparadas e restauradas (a cópia tem ainda `preserved_at`).
const scheduleColumns = `transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts, last_attempt_at, last_error, completed_at`

const tokenPrefix = "wager:schedule-snapshot:"

// scheduleGrant é um privilégio concedido sobre a agenda (de tabela, se Column vazio, ou de coluna).
type scheduleGrant struct {
	Grantee   string // "" = PUBLIC
	Privilege string
	Column    string
	Grantable bool
}

func (g scheduleGrant) statement() string {
	target := ""
	if g.Column != "" {
		target = " (" + pgx.Identifier{g.Column}.Sanitize() + ")"
	}
	to := "PUBLIC"
	if g.Grantee != "" {
		to = pgx.Identifier{g.Grantee}.Sanitize()
	}
	stmt := "GRANT " + g.Privilege + target + " ON TABLE " + scheduleTable + " TO " + to
	if g.Grantable {
		stmt += " WITH GRANT OPTION"
	}
	return stmt
}

const grantsSQL = `
SELECT ''::text, CASE WHEN a.grantee = 0 THEN '' ELSE (SELECT rolname::text FROM pg_roles WHERE oid = a.grantee) END,
       a.privilege_type, a.is_grantable
FROM pg_class c
CROSS JOIN LATERAL aclexplode(c.relacl) a
WHERE c.oid = 'public.pending_references'::regclass AND a.grantee <> c.relowner
UNION ALL
SELECT at.attname::text, CASE WHEN a.grantee = 0 THEN '' ELSE (SELECT rolname::text FROM pg_roles WHERE oid = a.grantee) END,
       a.privilege_type, a.is_grantable
FROM pg_class c
JOIN pg_attribute at ON at.attrelid = c.oid AND at.attnum > 0 AND NOT at.attisdropped AND at.attacl IS NOT NULL
CROSS JOIN LATERAL aclexplode(at.attacl) a
WHERE c.oid = 'public.pending_references'::regclass AND a.grantee <> c.relowner`

// writersSQL lista os papéis de login que ainda conseguiriam escrever na agenda: por privilégio próprio, herdado ou por
// pertencerem (qualquer modo) a um papel com escrita. Superusuários, o dono e a sessão do migrador não contam (não há como,
// nem sentido em, excluí-los).
const writersSQL = `
SELECT COALESCE(string_agg(r.rolname, ', ' ORDER BY r.rolname), '') FROM pg_roles r
WHERE r.rolcanlogin AND NOT r.rolsuper
  AND r.oid <> (SELECT relowner FROM pg_class WHERE oid = 'public.pending_references'::regclass)
  AND r.rolname NOT IN (current_user, session_user)
  AND EXISTS (SELECT 1 FROM pg_roles g
              WHERE pg_has_role(r.oid, g.oid, 'MEMBER')
                AND (has_table_privilege(g.oid, 'public.pending_references', 'INSERT,UPDATE,DELETE,TRUNCATE')
                     OR has_any_column_privilege(g.oid, 'public.pending_references', 'INSERT,UPDATE')))`

// preserveSchedule executa a preparação. Devolve a função que devolve os grants revogados (nil se a agenda não existe).
func (m *Migrator) preserveSchedule(ctx context.Context) (restore func() error, err error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck
	refuse := func(format string, args ...any) error {
		return fmt.Errorf("refusing to roll back migration 00005, nothing was changed: "+format, args...)
	}
	var exists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.pending_references') IS NOT NULL`).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	for _, stmt := range []string{
		`SET LOCAL lock_timeout = '30s'`,
		`LOCK TABLE wallets, wager_transactions, pending_references IN SHARE ROW EXCLUSIVE MODE`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("preserve the pending reference schedule before rolling back migration 00005: %w", err)
		}
	}

	// 1. Exclusão dos escritores: captura, revoga e verifica o efeito.
	rows, err := tx.QueryContext(ctx, grantsSQL)
	if err != nil {
		return nil, fmt.Errorf("read the schedule privileges: %w", err)
	}
	var grants []scheduleGrant
	for rows.Next() {
		var g scheduleGrant
		if err := rows.Scan(&g.Column, &g.Grantee, &g.Privilege, &g.Grantable); err != nil {
			rows.Close()
			return nil, err
		}
		grants = append(grants, g)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	revoked := map[string]bool{}
	for _, g := range grants {
		if revoked[g.Grantee] {
			continue
		}
		revoked[g.Grantee] = true
		from := "PUBLIC"
		if g.Grantee != "" {
			from = pgx.Identifier{g.Grantee}.Sanitize()
		}
		if _, err := tx.ExecContext(ctx, "REVOKE ALL ON TABLE "+scheduleTable+" FROM "+from); err != nil {
			return nil, fmt.Errorf("revoke the schedule privileges of %s: %w", from, err)
		}
	}
	var writers string
	if err := tx.QueryRowContext(ctx, writersSQL).Scan(&writers); err != nil {
		return nil, fmt.Errorf("verify the writers excluded from the schedule: %w", err)
	}
	if writers != "" {
		return nil, refuse("the role(s) %s would still be able to write the pending reference schedule (privilege inherited or through "+
			"membership of a role with write access); revoke that access or the membership and retry", writers)
	}

	// 2. A cópia: nunca substitui o original por um estado reconstruído.
	var copyExists bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('public.pending_references_preserved') IS NOT NULL`).Scan(&copyExists); err != nil {
		return nil, err
	}
	refresh := !copyExists
	if copyExists {
		var sameTable bool
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(obj_description('public.pending_references'::regclass, 'pg_class') LIKE $1 || '%'
				AND obj_description('public.pending_references'::regclass, 'pg_class') = obj_description('public.pending_references_preserved'::regclass, 'pg_class'), false)
			OR (NOT EXISTS (SELECT `+scheduleColumns+` FROM pending_references EXCEPT SELECT `+scheduleColumns+` FROM pending_references_preserved)
			    AND NOT EXISTS (SELECT `+scheduleColumns+` FROM pending_references_preserved EXCEPT SELECT `+scheduleColumns+` FROM pending_references))`,
			tokenPrefix).Scan(&sameTable); err != nil {
			return nil, fmt.Errorf("compare the schedule with the preserved copy: %w", err)
		}
		refresh = sameTable
	}
	if refresh {
		token := tokenPrefix + uuid.NewString()
		for _, stmt := range []string{
			`DROP TABLE IF EXISTS ` + preservedTable,
			`CREATE TABLE ` + preservedTable + ` AS SELECT *, now() AS preserved_at FROM ` + scheduleTable,
			`REVOKE ALL ON TABLE ` + preservedTable + ` FROM PUBLIC`,
			`COMMENT ON TABLE ` + preservedTable + ` IS '` + token + `'`,
			`COMMENT ON TABLE ` + scheduleTable + ` IS '` + token + `'`,
		} {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return nil, fmt.Errorf("preserve the pending reference schedule before rolling back migration 00005: %w", err)
			}
		}
	} else {
		// A cópia é o original; a agenda viva deve ser só a reconstrução intacta da 00005.
		var n int64
		var ids string
		if err := tx.QueryRowContext(ctx, `SELECT count(*), COALESCE(string_agg(transaction_id::text, ', ' ORDER BY transaction_id) FILTER (WHERE rn <= 5), '') FROM (
				SELECT p.transaction_id, row_number() OVER (ORDER BY p.transaction_id) AS rn FROM pending_references p
				WHERE NOT EXISTS (SELECT 1 FROM pending_references_preserved c WHERE c.transaction_id = p.transaction_id)
				   OR p.attempts <> 0 OR p.completed_at IS NOT NULL OR p.last_attempt_at IS NOT NULL OR p.last_error IS NOT NULL) s`).Scan(&n, &ids); err != nil {
			return nil, fmt.Errorf("compare the schedule with the preserved copy: %w", err)
		}
		if n > 0 {
			return nil, refuse("%d schedule row(s) were created or changed after migration 00005 was reapplied on top of a preserved original copy "+
				"(first ids: %s); rolling back would lose them. Resolve them manually (the preserved copy %s is untouched)", n, ids, "pending_references_preserved")
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return func() error { return m.restoreScheduleGrants(grants) }, nil
}

// restoreScheduleGrants devolve os privilégios revogados pela preparação quando a reversão não chegou a apagar a agenda
// (falha ou cancelamento). Usa um contexto próprio: o do chamador pode ter sido cancelado.
func (m *Migrator) restoreScheduleGrants(grants []scheduleGrant) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
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
		return nil // a reversão chegou a apagar a agenda: não há o que devolver
	}
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '20s'`); err != nil {
		return err
	}
	for _, g := range grants {
		if _, err := tx.ExecContext(ctx, g.statement()); err != nil {
			return fmt.Errorf("give back %q: %w", g.statement(), err)
		}
	}
	return tx.Commit()
}

// withRestoredGrants anexa à falha da reversão o resultado da devolução dos grants.
func withRestoredGrants(err error, restore func() error) error {
	if err == nil || restore == nil {
		return err
	}
	if rerr := restore(); rerr != nil {
		return errors.Join(err, fmt.Errorf("the schedule privileges were NOT given back (run `migrate grants`): %w", rerr))
	}
	return err
}

// dropPreservedSchedule descarta a cópia: usada apenas no reset deliberadamente destrutivo (down-to 0).
func (m *Migrator) dropPreservedSchedule(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, `DROP TABLE IF EXISTS `+preservedTable)
	return err
}
