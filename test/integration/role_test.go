//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/infra/postgres"
)

// O papel de runtime não é dono das tabelas nem superusuário, e não consegue
// desabilitar as proteções do ledger por nenhum caminho.

func runtimePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), env.runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestRuntimeRoleHasMinimalPrivileges(t *testing.T) {
	ctx := context.Background()
	rt := runtimePool(t)

	var super, createRole, createDB, bypass, replication bool
	if err := rt.QueryRow(ctx, `SELECT rolsuper, rolcreaterole, rolcreatedb, rolbypassrls, rolreplication FROM pg_roles WHERE rolname = current_user`).
		Scan(&super, &createRole, &createDB, &bypass, &replication); err != nil {
		t.Fatal(err)
	}
	if super || createRole || createDB || bypass || replication {
		t.Fatalf("runtime role has elevated attributes: super=%v createrole=%v createdb=%v bypassrls=%v replication=%v", super, createRole, createDB, bypass, replication)
	}
	var owned int
	if err := rt.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tableowner = current_user`).Scan(&owned); err != nil {
		t.Fatal(err)
	}
	if owned != 0 {
		t.Fatalf("runtime role owns %d table(s)", owned)
	}

	// A verificação do schema usa só SELECT em goose_db_version.
	if err := postgres.CheckSchema(ctx, rt); err != nil {
		t.Fatalf("schema check with restricted role: %v", err)
	}

	w := seedCommitted(t, 10000)
	const denied = "42501" // insufficient_privilege
	forbidden := map[string]string{
		"update ledger":                         `UPDATE wallet_ledger_entries SET amount = amount`,
		"delete ledger":                         `DELETE FROM wallet_ledger_entries`,
		"truncate ledger":                       `TRUNCATE wallet_ledger_entries`,
		"disable ledger triggers":               `ALTER TABLE wallet_ledger_entries DISABLE TRIGGER ALL`,
		"disable one trigger":                   `ALTER TABLE wallet_ledger_entries DISABLE TRIGGER trg_ledger_enforce_chain`,
		"disable wallet guard":                  `ALTER TABLE wallets DISABLE TRIGGER trg_wallets_guard`,
		"drop trigger":                          `DROP TRIGGER trg_ledger_no_update_delete ON wallet_ledger_entries`,
		"drop constraint":                       `ALTER TABLE wallet_ledger_entries DROP CONSTRAINT uq_ledger_wallet_transaction`,
		"drop table":                            `DROP TABLE wallet_ledger_entries`,
		"bypass triggers via replication role":  `SET session_replication_role = replica`,
		"create object in schema":               `CREATE TABLE scratch_runtime (id int)`,
		"replace trigger function":              `CREATE OR REPLACE FUNCTION ledger_forbid_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$`,
		"delete wallet":                         `DELETE FROM wallets`,
		"truncate wallets":                      `TRUNCATE wallets`,
		"change wallet identity":                `UPDATE wallets SET player_id = player_id`,
		"change wallet currency":                `UPDATE wallets SET currency = currency`,
		"update transaction identity":           `UPDATE wager_transactions SET amount = amount`,
		"update transaction provider":           `UPDATE wager_transactions SET provider_id = provider_id`,
		"update transaction round":              `UPDATE wager_transactions SET round_id = round_id`,
		"update transaction external reference": `UPDATE wager_transactions SET reference_external_transaction_id = reference_external_transaction_id`,
		"update transaction hash":               `UPDATE wager_transactions SET payload_hash = payload_hash`,
		"update pending deadline":               `UPDATE pending_references SET expires_at = expires_at`,
		"delete pending schedule":               `DELETE FROM pending_references`,
		"delete transactions":                   `DELETE FROM wager_transactions`,
		// O publicador (00007) atualiza só as colunas de controle da outbox; snapshot e identidade seguem imutáveis para o runtime.
		"update outbox payload":    `UPDATE outbox_events SET payload = payload`,
		"update outbox identity":   `UPDATE outbox_events SET event_type = event_type`,
		"update outbox occurrence": `UPDATE outbox_events SET occurred_at = occurred_at`,
		"delete outbox":            `DELETE FROM outbox_events`,
		"truncate outbox":          `TRUNCATE outbox_events`,
		"write migration history":  `DELETE FROM goose_db_version`,
		"become the owner":         `ALTER TABLE wallet_ledger_entries OWNER TO CURRENT_USER`,
	}
	for name, stmt := range forbidden {
		_, err := rt.Exec(ctx, stmt)
		e := pgErr(err)
		if e == nil {
			t.Errorf("%s: statement succeeded for the runtime role", name)
			continue
		}
		if e.Code != denied && e.Code != "42501" {
			t.Errorf("%s: got %s (%s), want insufficient_privilege", name, e.Code, e.Message)
		}
	}
	// Conceder a si mesmo não tem efeito sem WITH GRANT OPTION (o PostgreSQL só avisa).
	_, _ = rt.Exec(ctx, `GRANT ALL ON wallet_ledger_entries TO CURRENT_USER`)
	_, err := rt.Exec(ctx, `UPDATE wallet_ledger_entries SET amount = amount`)
	expectPg(t, "update after self-grant", err, denied, "")

	// Nada foi alterado pelas tentativas.
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1", w.id); n != 1 {
		t.Fatalf("ledger entries = %d", n)
	}

	// E o que o fluxo financeiro precisa continua permitido.
	for name, stmt := range map[string]string{
		"read wallets":      `SELECT id FROM wallets LIMIT 1`,
		"read ledger":       `SELECT id FROM wallet_ledger_entries LIMIT 1`,
		"read transactions": `SELECT id FROM wager_transactions LIMIT 1`,
		"read outbox":       `SELECT id FROM outbox_events LIMIT 1`,
		// Controle de publicação da outbox (00007): reserva, tentativa, diagnóstico e marcador.
		"update outbox control columns": `UPDATE outbox_events SET attempts = attempts, next_attempt_at = next_attempt_at, last_error = last_error,
			published_at = published_at, lease_token = lease_token, lease_expires_at = lease_expires_at WHERE false`,
	} {
		rows, err := rt.Query(ctx, stmt)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		rows.Close()
	}
}

// A provisão é idempotente, não toca nos dados e restaura privilégios alterados.
func TestRuntimeRoleProvisioningIsIdempotent(t *testing.T) {
	ctx := context.Background()
	w := seedCommitted(t, 12345)
	before := queryInt(t, `SELECT count(*) FROM wallet_ledger_entries`)

	for i := 0; i < 2; i++ {
		if err := postgres.ProvisionRuntimeRole(ctx, env.dbURL, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
			t.Fatalf("provision #%d: %v", i+1, err)
		}
	}
	if after := queryInt(t, `SELECT count(*) FROM wallet_ledger_entries`); after != before {
		t.Fatalf("provisioning changed data: %d -> %d", before, after)
	}
	if bal, _ := walletState(t, w.id.String()); bal != 12345 {
		t.Fatalf("balance %d", bal)
	}

	// Privilégio indevido concedido por fora é revogado na reaplicação.
	if _, err := testPool.Exec(ctx, `GRANT UPDATE, DELETE ON wallet_ledger_entries TO `+pgx.Identifier{env.runtimeRole}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err := postgres.ProvisionRuntimeRole(ctx, env.dbURL, postgres.RuntimeRole{Name: env.runtimeRole}); err != nil {
		t.Fatal(err)
	}
	_, err := runtimePool(t).Exec(ctx, `DELETE FROM wallet_ledger_entries WHERE false`)
	expectPg(t, "delete after re-provisioning", err, "42501", "")

	if err := postgres.ProvisionRuntimeRole(ctx, env.dbURL, postgres.RuntimeRole{}); err == nil {
		t.Fatal("empty role name accepted")
	}
}
