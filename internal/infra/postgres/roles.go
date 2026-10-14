package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// RuntimeRole descreve o papel de banco usado pela aplicação em execução. É
// distinto do papel que roda as migrations (dono das tabelas): o runtime não é
// superusuário, não é dono de nada e não pode desabilitar triggers, alterar o
// schema nem atualizar/apagar o ledger.
type RuntimeRole struct {
	Name     string
	Password string // vazio: não altera a senha de um papel existente
}

// grants é o conjunto mínimo de privilégios do runtime, por tabela. Cada
// incremento que precisar de mais (p.ex. UPDATE de transações pelo worker de
// referências, UPDATE da outbox pelo publicador) amplia esta lista de forma
// explícita e revisável.
var grants = []string{
	`GRANT SELECT, INSERT ON wallet_ledger_entries TO %[1]s`,          // append-only: sem UPDATE/DELETE/TRUNCATE
	`GRANT SELECT, INSERT ON wager_transactions TO %[1]s`,             // sem UPDATE/DELETE (transações são inseridas já finais)
	`GRANT SELECT, INSERT ON outbox_events TO %[1]s`,                  // sem UPDATE/DELETE até existir o publicador
	`GRANT SELECT, INSERT ON wallets TO %[1]s`,                        // sem DELETE
	`GRANT UPDATE (balance, version, updated_at) ON wallets TO %[1]s`, // identidade da carteira é imutável
	`GRANT SELECT ON goose_db_version TO %[1]s`,                       // verificação da versão do schema
}

// ProvisionRuntimeRole cria (se necessário) o papel de runtime e aplica os
// privilégios mínimos. Idempotente. Deve ser executado com o papel dono das
// tabelas, depois das migrations. Não toca em dados nem em outros papéis.
func ProvisionRuntimeRole(ctx context.Context, ownerURL string, role RuntimeRole) error {
	if role.Name == "" {
		return errors.New("runtime role name is required")
	}
	conn, err := pgx.Connect(ctx, ownerURL)
	if err != nil {
		return fmt.Errorf("connect as owner: %w", err)
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	ident := pgx.Identifier{role.Name}.Sanitize()
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role.Name).Scan(&exists); err != nil {
		return err
	}
	const attrs = `LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT`
	if !exists {
		if _, err := tx.Exec(ctx, `CREATE ROLE `+ident+` `+attrs); err != nil {
			return fmt.Errorf("create role %s (the migration role needs CREATEROLE, or create the role beforehand): %w", role.Name, err)
		}
	} else if _, err := tx.Exec(ctx, `ALTER ROLE `+ident+` `+attrs); err != nil {
		return fmt.Errorf("alter role %s: %w", role.Name, err)
	}
	if role.Password != "" {
		var stmt string
		if err := tx.QueryRow(ctx, `SELECT format('ALTER ROLE %I PASSWORD %L', $1::text, $2::text)`, role.Name, role.Password).Scan(&stmt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("set role password: %w", err)
		}
	}

	var db string
	if err := tx.QueryRow(ctx, `SELECT current_database()`).Scan(&db); err != nil {
		return err
	}
	stmts := []string{
		`GRANT CONNECT ON DATABASE ` + pgx.Identifier{db}.Sanitize() + ` TO ` + ident,
		`GRANT USAGE ON SCHEMA public TO ` + ident,
		`REVOKE CREATE ON SCHEMA public FROM ` + ident,
		// Parte de um estado conhecido: remove qualquer privilégio anterior nas tabelas.
		`REVOKE ALL ON wallets, wager_transactions, wallet_ledger_entries, outbox_events, goose_db_version FROM ` + ident,
	}
	for _, g := range grants {
		stmts = append(stmts, fmt.Sprintf(g, ident))
	}
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w (were the migrations applied with `migrate up`?)", s, err)
		}
	}
	return tx.Commit(ctx)
}
