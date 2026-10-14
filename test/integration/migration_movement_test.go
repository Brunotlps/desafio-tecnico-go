//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// A migration corretiva 00003 (transação concluída exige o lançamento do seu movimento e
// result_balance = balance_after dele) é aplicável a um banco já na versão 2: preserva dados
// válidos, recusa sem alterar nada um histórico incompatível, e sua reversão não toca nos dados.

type v2Data struct{ wallet, open, bet, loss, rejected, orphan uuid.UUID }

// populateV2 grava, no esquema da versão 2, um histórico coerente (abertura 100,00, BET de 10,00,
// uma LOSS e uma rejeição) e, conforme os parâmetros, uma BET PROCESSED sem lançamento
// (missingLedger) ou com resultado diferente do lançamento (staleResult). A versão 2 aceita ambas.
func populateV2(t *testing.T, conn *pgx.Conn, missingLedger, staleResult bool) v2Data {
	t.Helper()
	ctx := context.Background()
	d := v2Data{wallet: uuid.New(), open: uuid.New(), bet: uuid.New(), loss: uuid.New(), rejected: uuid.New(), orphan: uuid.New()}
	player := uuid.New()
	betResult := int64(9000)
	if staleResult {
		betResult = 77700
	}
	steps := []struct {
		q    string
		args []any
	}{
		{insWallet, []any{d.wallet, player, 9000, 2}},
		{insOpen, []any{d.open, d.wallet, player, 10000}},
		{insBet, []any{d.bet, d.wallet, player, 1000, "BET", "PROCESSED", d.bet.String(), betResult}},
		{insEntry, []any{uuid.New(), d.wallet, d.open, "CREDIT", 10000, 0, 10000}},
		{insEntry, []any{uuid.New(), d.wallet, d.bet, "DEBIT", 1000, 10000, 9000}},
		{insBet, []any{d.loss, d.wallet, player, 0, "LOSS", "PROCESSED", d.loss.String(), 9000}},
		{insBet, []any{d.rejected, d.wallet, player, 99999, "BET", "REJECTED", d.rejected.String(), 9000}},
	}
	if missingLedger {
		// a falha da auditoria: BET concluída, com hash/forma válidos, sem lançamento nem alteração da carteira
		steps = append(steps, struct {
			q    string
			args []any
		}{insBet, []any{d.orphan, d.wallet, player, 500, "BET", "PROCESSED", d.orphan.String(), 8500}})
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range steps {
		if s.q == insBet && s.args[5] == "REJECTED" {
			// insBet não grava failure_code; a rejeição precisa dele
			if _, err := tx.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id,
				external_transaction_id, idempotency_key, payload_hash, round_id, game_id, failure_code, result_balance, created_at, updated_at)
				VALUES ($1,'EXTERNAL','BET','REJECTED',$2,$3,$4,'BRL','integrity',$5,$5,'hash','r','g','INSUFFICIENT_FUNDS',$6,now(),now())`,
				s.args[0], s.args[1], s.args[2], s.args[3], s.args[6], s.args[7]); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if _, err := tx.Exec(ctx, s.q, s.args...); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("version 2 must accept this history: %v", err)
	}
	return d
}

func counts(t *testing.T, conn *pgx.Conn, wallet uuid.UUID) [5]int64 {
	t.Helper()
	var c [5]int64
	if err := conn.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM wallets), (SELECT count(*) FROM wager_transactions),
		(SELECT count(*) FROM wallet_ledger_entries), (SELECT balance FROM wallets WHERE id = $1), (SELECT version FROM wallets WHERE id = $1)`, wallet).
		Scan(&c[0], &c[1], &c[2], &c[3], &c[4]); err != nil {
		t.Fatal(err)
	}
	return c
}

func movementGuaranteeInstalled(t *testing.T, conn *pgx.Conn) bool {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM pg_trigger WHERE tgname = 'ctrg_wager_tx_requires_movement' AND NOT tgisinternal`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func TestMovementMigrationPreservesValidDataAndIsReversible(t *testing.T) {
	ctx := context.Background()
	_, mig, conn := scratchAt(t, "mig3_ok", 2)
	d := populateV2(t, conn, false, false)
	before := counts(t, conn, d.wallet)

	if movementGuaranteeInstalled(t, conn) {
		t.Fatal("the guarantee exists before the migration")
	}
	if _, err := mig.Up(ctx); err != nil {
		t.Fatalf("migration failed on valid data: %v", err)
	}
	if v := schemaVersion(t, conn); v != 3 {
		t.Fatalf("version %d", v)
	}
	if after := counts(t, conn, d.wallet); after != before {
		t.Fatalf("data changed by the migration: %v -> %v", before, after)
	}
	if !movementGuaranteeInstalled(t, conn) {
		t.Fatal("the guarantee was not installed")
	}

	// A garantia já vale sobre o banco migrado: a falha da auditoria é recusada no COMMIT.
	player := uuid.New()
	err := inTxOn(conn, func(ctx context.Context, tx pgx.Tx) error {
		w := uuid.New()
		if _, err := tx.Exec(ctx, insWallet, w, player, 0, 1); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, insOpen, uuid.New(), w, player, 500)
		return err
	})
	expectPgMessage(t, "completed movement without ledger after the migration", err, "23514", "no ledger entry")

	// Reversão: documentada, não toca nos dados e restaura o comportamento de 00002.
	if _, err := mig.Down(ctx); err != nil {
		t.Fatalf("down: %v", err)
	}
	if v := schemaVersion(t, conn); v != 2 {
		t.Fatalf("version after down: %d", v)
	}
	if after := counts(t, conn, d.wallet); after != before {
		t.Fatalf("data changed by the reversal: %v -> %v", before, after)
	}
	if movementGuaranteeInstalled(t, conn) {
		t.Fatal("the guarantee survived the reversal")
	}
	var def string
	if err := conn.QueryRow(ctx, `SELECT pg_get_functiondef('ledger_entry_matches_transaction'::regproc)`).Scan(&def); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(def, "result_balance") {
		t.Fatal("the ledger-side function of 00002 was not restored")
	}
	// E reaplicar funciona (reversão e aplicação são simétricas).
	if _, err := mig.Up(ctx); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if !movementGuaranteeInstalled(t, conn) {
		t.Fatal("the guarantee was not reinstalled")
	}
}

func TestMovementMigrationRefusesIncompatibleHistoryUntouched(t *testing.T) {
	for name, tc := range map[string]struct {
		missingLedger, staleResult bool
		want                       string
	}{
		"completed BET without a ledger entry": {missingLedger: true, want: "1 PROCESSED movement transaction(s) without a ledger entry"},
		"result differs from the entry":        {staleResult: true, want: "1 PROCESSED movement transaction(s) whose result_balance differs from the balance_after"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_, mig, conn := scratchAt(t, "mig3_bad", 2)
			d := populateV2(t, conn, tc.missingLedger, tc.staleResult)
			before := counts(t, conn, d.wallet)

			_, err := mig.Up(ctx)
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "first ids:") {
				t.Fatalf("expected a preflight failure containing %q and sample ids, got: %v", tc.want, err)
			}
			if tc.missingLedger && !strings.Contains(err.Error(), d.orphan.String()) {
				t.Fatalf("the report must identify the offending transaction %s: %v", d.orphan, err)
			}
			if v := schemaVersion(t, conn); v != 2 {
				t.Fatalf("schema advanced to %d despite the failure", v)
			}
			if after := counts(t, conn, d.wallet); after != before {
				t.Fatalf("historical records were altered: %v -> %v", before, after)
			}
			if movementGuaranteeInstalled(t, conn) {
				t.Fatal("the guarantee was installed despite the failure")
			}
			// Os registros incompatíveis continuam lá, intactos, para correção manual.
			var n int
			if err := conn.QueryRow(ctx, `SELECT count(*) FROM wager_transactions WHERE id IN ($1, $2)`, d.bet, d.orphan).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if (tc.missingLedger && n != 2) || (tc.staleResult && n != 1) {
				t.Fatalf("offending rows were changed: %d", n)
			}
		})
	}
}

// inTxOn executa fn numa transação da conexão dada e devolve o erro de fn ou do COMMIT.
func inTxOn(conn *pgx.Conn, fn func(ctx context.Context, tx pgx.Tx) error) error {
	ctx := context.Background()
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}
