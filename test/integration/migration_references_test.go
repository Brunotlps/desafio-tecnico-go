//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// A migration 00005 (referências, reversões e pendências duráveis) é aplicável a um banco na versão 4:
// preserva dados válidos, recusa sem alterar nada um histórico incompatível (com contagem e ids), dá
// agenda às pendências que sobreviveram a uma reversão, e sua reversão não toca em transações nem ledger.

func triggerExists(t *testing.T, conn *pgx.Conn, name string) bool {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM pg_trigger WHERE tgname = $1 AND NOT tgisinternal`, name).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// v4Reversal grava, no esquema da versão 4, uma reversão PROCESSED (crédito) com lançamento e saldo coerentes.
func v4Reversal(t *testing.T, conn *pgx.Conn, wallet uuid.UUID, kind string, amount int64, refExt *string, refTx *uuid.UUID) uuid.UUID {
	t.Helper()
	ctx := context.Background()
	var player uuid.UUID
	var balance int64
	if err := conn.QueryRow(ctx, `SELECT player_id, balance FROM wallets WHERE id = $1`, wallet).Scan(&player, &balance); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	after := balance + amount
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct {
		q    string
		args []any
	}{
		{`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
			idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, reference_transaction_id, result_balance, created_at, updated_at)
			VALUES ($1,'EXTERNAL',$2,'PROCESSED',$3,$4,$5,'BRL','integrity',$9,$9,'hash','r','g',$6,$7,$8,now(),now())`,
			[]any{id, kind, wallet, player, amount, refExt, refTx, after, id.String()}},
		{`UPDATE wallets SET balance = $2, version = version + 1, updated_at = now() WHERE id = $1`, []any{wallet, after}},
		{insEntry, []any{uuid.New(), wallet, id, "CREDIT", amount, balance, after}},
	} {
		if _, err := tx.Exec(ctx, s.q, s.args...); err != nil {
			t.Fatalf("version 4 must accept this history: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("version 4 must accept this history: %v", err)
	}
	return id
}

func TestReferencesMigrationPreservesValidDataAndIsReversible(t *testing.T) {
	ctx := context.Background()
	_, mig, conn := scratchAt(t, "mig5_ok", 2)
	d := populateV2(t, conn, false, false)
	if _, err := mig.UpTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	// Histórico válido no esquema 4: uma BET reembolsada com a referência resolvida.
	betExt := d.bet.String()
	refund := v4Reversal(t, conn, d.wallet, "REFUND", 1000, &betExt, &d.bet)
	before := counts(t, conn, d.wallet)

	if _, err := mig.Up(ctx); err != nil {
		t.Fatalf("migration failed on valid data: %v", err)
	}
	if v := schemaVersion(t, conn); v != 5 {
		t.Fatalf("version %d", v)
	}
	if after := counts(t, conn, d.wallet); after != before {
		t.Fatalf("data changed by the migration: %v -> %v", before, after)
	}
	for _, trg := range []string{"trg_wager_tx_reference_guard", "ctrg_wager_tx_pending_consistency", "ctrg_pending_references_consistency", "trg_pending_references_guard"} {
		if !triggerExists(t, conn, trg) {
			t.Errorf("trigger %s not installed", trg)
		}
	}
	// A garantia nova já vale: uma segunda reversão PROCESSED do mesmo alvo é recusada.
	player := uuid.New()
	_ = player
	err := inTxOn(conn, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
			idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, reference_transaction_id, result_balance, created_at, updated_at)
			SELECT gen_random_uuid(), 'EXTERNAL', 'ROLLBACK', 'PROCESSED', wallet_id, player_id, 1000, 'BRL', 'integrity', 'second', 'second', 'h', round_id, 'g',
			       $2, $1, 1, now(), now() FROM wager_transactions WHERE id = $1`, d.bet, betExt)
		return err
	})
	expectPg(t, "second reversal after the migration", err, "23505", "uq_wager_tx_one_reversal_per_target")

	// Reversão: remove as garantias, preserva os dados.
	if _, err := mig.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if v := schemaVersion(t, conn); v != 4 {
		t.Fatalf("version after down: %d", v)
	}
	if after := counts(t, conn, d.wallet); after != before {
		t.Fatalf("data changed by the reversal: %v -> %v", before, after)
	}
	if triggerExists(t, conn, "trg_wager_tx_reference_guard") || triggerExists(t, conn, "ctrg_wager_tx_pending_consistency") {
		t.Fatal("guarantees remain after the reversal")
	}
	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id = $1`, refund).Scan(&status); err != nil || status != "PROCESSED" {
		t.Fatalf("refund after the reversal: %s %v", status, err)
	}
	// Reaplicação.
	if _, err := mig.Up(ctx); err != nil {
		t.Fatalf("reapply: %v", err)
	}
	if v := schemaVersion(t, conn); v != 5 || !triggerExists(t, conn, "trg_wager_tx_reference_guard") {
		t.Fatalf("version %d after reapplying", v)
	}
}

func TestReferencesMigrationRefusesIncompatibleHistoryWithoutTouchingIt(t *testing.T) {
	ctx := context.Background()
	for name, c := range map[string]struct {
		setup func(t *testing.T, conn *pgx.Conn, d v2Data)
		want  string
	}{
		"transaction committed as PENDING": {
			setup: func(t *testing.T, conn *pgx.Conn, d v2Data) {
				var player uuid.UUID
				_ = conn.QueryRow(ctx, `SELECT player_id FROM wallets WHERE id = $1`, d.wallet).Scan(&player)
				id := uuid.New()
				if _, err := conn.Exec(ctx, insBet, id, d.wallet, player, 100, "BET", "PENDING", id.String(), nil); err != nil {
					t.Fatal(err)
				}
			},
			want: "committed as PENDING",
		},
		"PROCESSED REFUND without a resolved internal reference": {
			setup: func(t *testing.T, conn *pgx.Conn, d v2Data) {
				ext := d.bet.String()
				v4Reversal(t, conn, d.wallet, "REFUND", 1000, &ext, nil)
			},
			want: "no resolved internal reference",
		},
		"two PROCESSED reversals of the same target": {
			setup: func(t *testing.T, conn *pgx.Conn, d v2Data) {
				ext := d.bet.String()
				v4Reversal(t, conn, d.wallet, "REFUND", 1000, &ext, &d.bet)
				v4Reversal(t, conn, d.wallet, "ROLLBACK", 1000, &ext, &d.bet)
			},
			want: "more than one PROCESSED reversal",
		},
		"internal reference incoherent with the external one": {
			setup: func(t *testing.T, conn *pgx.Conn, d v2Data) {
				wrong := "another-external-id"
				v4Reversal(t, conn, d.wallet, "REFUND", 1000, &wrong, &d.bet)
			},
			want: "incoherent with the external reference",
		},
		"reversal with another amount than its target": {
			setup: func(t *testing.T, conn *pgx.Conn, d v2Data) {
				ext := d.bet.String()
				v4Reversal(t, conn, d.wallet, "REFUND", 900, &ext, &d.bet)
			},
			want: "incoherent with the external reference",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, mig, conn := scratchAt(t, "mig5_bad", 2)
			d := populateV2(t, conn, false, false)
			if _, err := mig.UpTo(ctx, 4); err != nil {
				t.Fatal(err)
			}
			c.setup(t, conn, d)
			before := counts(t, conn, d.wallet)
			_, err := mig.Up(ctx)
			if err == nil || !strings.Contains(err.Error(), c.want) || !strings.Contains(err.Error(), "first ids:") {
				t.Fatalf("migration error = %v, want it to report %q with ids", err, c.want)
			}
			if v := schemaVersion(t, conn); v != 4 {
				t.Fatalf("schema moved to %d", v)
			}
			if after := counts(t, conn, d.wallet); after != before {
				t.Fatalf("a refused migration changed data: %v -> %v", before, after)
			}
			if triggerExists(t, conn, "trg_wager_tx_reference_guard") {
				t.Fatal("a refused migration left guarantees behind")
			}
		})
	}
}

// Pendências sobrevivem a um ciclo de reversão: a reaplicação cria a agenda de cada uma.
func TestReferencesMigrationGivesASchedulerToSurvivingPendingTransactions(t *testing.T) {
	ctx := context.Background()
	_, mig, conn := scratchAt(t, "mig5_pending", 2)
	d := populateV2(t, conn, false, false)
	if _, err := mig.UpTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	var player uuid.UUID
	_ = conn.QueryRow(ctx, `SELECT player_id FROM wallets WHERE id = $1`, d.wallet).Scan(&player)
	pend := uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
		idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, created_at, updated_at)
		VALUES ($1,'EXTERNAL','REFUND','PENDING_REFERENCE',$2,$3,500,'BRL','integrity','pend','pend','hash','r','g','future-bet',now(),now())`, pend, d.wallet, player); err != nil {
		t.Fatal(err)
	}
	if _, err := mig.Up(ctx); err != nil {
		t.Fatalf("migration with a surviving pending transaction: %v", err)
	}
	var open bool
	var attempts int
	if err := conn.QueryRow(ctx, `SELECT completed_at IS NULL, attempts FROM pending_references WHERE transaction_id = $1`, pend).Scan(&open, &attempts); err != nil || !open || attempts != 0 {
		t.Fatalf("schedule: open=%v attempts=%d err=%v", open, attempts, err)
	}
	// Ciclo completo: reverte (a agenda some, a transação permanece) e reaplica (agenda recriada).
	if _, err := mig.Down(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := conn.QueryRow(ctx, `SELECT status FROM wager_transactions WHERE id = $1`, pend).Scan(&status); err != nil || status != "PENDING_REFERENCE" {
		t.Fatalf("after down: %s %v", status, err)
	}
	if _, err := mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT completed_at IS NULL FROM pending_references WHERE transaction_id = $1`, pend).Scan(&open); err != nil || !open {
		t.Fatalf("schedule recreated: open=%v err=%v", open, err)
	}
}
