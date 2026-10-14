//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/infra/postgres"
)

// scratchAt cria um banco auxiliar (removido ao fim) e o migra até a versão indicada.
func scratchAt(t *testing.T, purpose string, version int64) (string, *postgres.Migrator, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	url := createScratchDatabase(t, purpose)
	mig, err := postgres.NewMigrator(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { mig.Close() })
	if _, err := mig.UpTo(ctx, version); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(ctx) })
	return url, mig, conn
}

func schemaVersion(t *testing.T, conn *pgx.Conn) int64 {
	t.Helper()
	var v int64
	if err := conn.QueryRow(context.Background(), `SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// A migration corretiva 00002 é aplicável a um banco já inicializado (versão 1) sem
// apagar dados; e recusa, sem alterar nada, um histórico inconsistente.
func TestCorrectiveMigrationPreservesDataAndRefusesInconsistentHistory(t *testing.T) {
	ctx := context.Background()

	newV1 := func(t *testing.T, purpose string) (string, *postgres.Migrator, *pgx.Conn) {
		return scratchAt(t, purpose, 1)
	}
	version := func(conn *pgx.Conn) int64 { return schemaVersion(t, conn) }
	// Dados coerentes da versão 1: abertura de 100,00 e um débito de 10,00.
	populate := func(conn *pgx.Conn, brokenChain, nullResult bool) (wallet uuid.UUID) {
		wallet = uuid.New()
		player, open, bet := uuid.New(), uuid.New(), uuid.New()
		before := int64(10000)
		if brokenChain {
			before = 9000 // a falha reproduzida na revisão: antes não encadeia com o depois anterior
		}
		var result any = int64(9000)
		if nullResult {
			result = nil
		}
		steps := []struct {
			q    string
			args []any
		}{
			{insWallet, []any{wallet, player, before - 1000, 2}}, // saldo = depois do último lançamento
			{insOpen, []any{open, wallet, player, 10000}},
			{insBet, []any{bet, wallet, player, 1000, "BET", "PROCESSED", bet.String(), result}},
			{insEntry, []any{uuid.New(), wallet, open, "CREDIT", 10000, 0, 10000}},
			{insEntry, []any{uuid.New(), wallet, bet, "DEBIT", 1000, before, before - 1000}},
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range steps {
			if _, err := tx.Exec(ctx, s.q, s.args...); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("version 1 must accept this history: %v", err)
		}
		return wallet
	}

	t.Run("valid history is preserved", func(t *testing.T) {
		_, mig, conn := newV1(t, "mig_ok")
		w := populate(conn, false, false)
		counts := func() [4]int64 {
			var c [4]int64
			if err := conn.QueryRow(ctx, `SELECT (SELECT count(*) FROM wallets), (SELECT count(*) FROM wager_transactions),
				(SELECT count(*) FROM wallet_ledger_entries), (SELECT balance FROM wallets WHERE id = $1)`, w).Scan(&c[0], &c[1], &c[2], &c[3]); err != nil {
				t.Fatal(err)
			}
			return c
		}
		before := counts()
		if _, err := mig.Up(ctx); err != nil {
			t.Fatalf("corrective migration failed on valid data: %v", err)
		}
		if after := counts(); after != before {
			t.Fatalf("data changed: %v -> %v", before, after)
		}
		want, err := postgres.ExpectedVersion()
		if err != nil {
			t.Fatal(err)
		}
		if v := version(conn); v != want {
			t.Fatalf("version %d, want %d", v, want)
		}
		// E a reversão da última migration também preserva os dados.
		if _, err := mig.Down(ctx); err != nil {
			t.Fatal(err)
		}
		if after := counts(); after != before {
			t.Fatalf("data changed after down: %v -> %v", before, after)
		}
	})

	for name, tc := range map[string]struct {
		broken, null bool
		want         string
	}{
		"broken ledger chain": {broken: true, want: "break the balance chain"},
		"null result balance": {null: true, want: "NULL result_balance"},
	} {
		t.Run("inconsistent history is refused untouched: "+name, func(t *testing.T) {
			_, mig, conn := newV1(t, "mig_bad")
			w := populate(conn, tc.broken, tc.null)
			_, err := mig.Up(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected preflight failure containing %q, got: %v", tc.want, err)
			}
			if v := version(conn); v != 1 {
				t.Fatalf("schema advanced to %d despite the failure", v)
			}
			var n int64
			if err := conn.QueryRow(ctx, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, w).Scan(&n); err != nil || n != 2 {
				t.Fatalf("historical records were altered: %d %v", n, err)
			}
		})
	}
}
