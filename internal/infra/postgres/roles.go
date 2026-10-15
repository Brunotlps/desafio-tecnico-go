package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

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
// referências, UPDATE da outbox pelo publicador — já incluído) amplia esta lista de forma
// explícita e revisável.
var grants = []string{
	`GRANT SELECT, INSERT ON wallet_ledger_entries TO %[1]s`,                                                                      // append-only: sem UPDATE/DELETE/TRUNCATE
	`GRANT SELECT, INSERT ON wager_transactions TO %[1]s`,                                                                         // sem DELETE; UPDATE só nas colunas de transição abaixo
	`GRANT UPDATE (status, failure_code, result_balance, reference_transaction_id, updated_at) ON wager_transactions TO %[1]s`,    // identidade e metadados originais imutáveis
	`GRANT SELECT, INSERT ON pending_references TO %[1]s`,                                                                         // agenda das pendências; sem DELETE (as concluídas ficam para diagnóstico)
	`GRANT UPDATE (next_attempt_at, attempts, last_attempt_at, last_error, completed_at) ON pending_references TO %[1]s`,          // prazo e identidade imutáveis
	`GRANT SELECT, INSERT ON outbox_events TO %[1]s`,                                                                              // sem DELETE/TRUNCATE; UPDATE só nas colunas de controle abaixo
	`GRANT UPDATE (attempts, next_attempt_at, last_error, published_at, lease_token, lease_expires_at) ON outbox_events TO %[1]s`, // publicador: payload e identidade do evento imutáveis (migration 00007)
	`GRANT SELECT, INSERT ON wallets TO %[1]s`,                                                                                    // sem DELETE
	`GRANT UPDATE (balance, version, updated_at) ON wallets TO %[1]s`,                                                             // identidade da carteira é imutável
	`GRANT SELECT ON goose_db_version TO %[1]s`,                                                                                   // verificação da versão do schema
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
	// pending_references existe a partir da migration 00005; num banco ainda em versão anterior
	// (upgrade em andamento) os privilégios dela são simplesmente omitidos.
	var hasPending bool
	if err := tx.QueryRow(ctx, `SELECT to_regclass('public.pending_references') IS NOT NULL`).Scan(&hasPending); err != nil {
		return err
	}
	revoked := "wallets, wager_transactions, wallet_ledger_entries, outbox_events, goose_db_version"
	if hasPending {
		revoked = "wallets, wager_transactions, wallet_ledger_entries, outbox_events, pending_references, goose_db_version"
	}
	// lease_token/lease_expires_at existem a partir da migration 00007; antes dela (upgrade em andamento) o UPDATE de controle é omitido.
	var hasLease bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass('public.outbox_events')
		AND attname = 'lease_token' AND NOT attisdropped)`).Scan(&hasLease); err != nil {
		return err
	}
	stmts := []string{
		`GRANT CONNECT ON DATABASE ` + pgx.Identifier{db}.Sanitize() + ` TO ` + ident,
		`GRANT USAGE ON SCHEMA public TO ` + ident,
		`REVOKE CREATE ON SCHEMA public FROM ` + ident,
		// Parte de um estado conhecido: remove qualquer privilégio anterior nas tabelas.
		`REVOKE ALL ON ` + revoked + ` FROM ` + ident,
	}
	for _, g := range grants {
		if !hasPending && strings.Contains(g, "pending_references") {
			continue
		}
		if !hasLease && strings.Contains(g, "lease_token") {
			continue
		}
		stmts = append(stmts, fmt.Sprintf(g, ident))
	}
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w (were the migrations applied with `migrate up`?)", s, err)
		}
	}
	return tx.Commit(ctx)
}
