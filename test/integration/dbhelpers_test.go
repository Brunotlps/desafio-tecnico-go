//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// creditDirect simula uma movimentação de crédito futura (p.ex. um WIN) direto
// no banco, respeitando todas as constraints: transação, ledger e saldo no
// mesmo commit. Serve para provar que replays não reavaliam resultados antigos.
func creditDirect(t testing.TB, walletID string, amountMinor int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var playerID uuid.UUID
	var balance, version int64
	if err := tx.QueryRow(ctx, `SELECT player_id, balance, version FROM wallets WHERE id = $1 FOR UPDATE`, walletID).Scan(&playerID, &balance, &version); err != nil {
		t.Fatal(err)
	}
	txID := uuid.Must(uuid.NewV7())
	after := balance + amountMinor
	if _, err := tx.Exec(ctx, `INSERT INTO wager_transactions
		(id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id, idempotency_key,
		 payload_hash, round_id, game_id, result_balance, created_at, updated_at)
		VALUES ($1,'EXTERNAL','WIN','PROCESSED',$2,$3,$4,'BRL','test-credit',$5,$5,'hash','r','g',$6,now(),now())`,
		txID, walletID, playerID, amountMinor, txID.String(), after); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = $2, version = version + 1, updated_at = now() WHERE id = $1`, walletID, after); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
		VALUES ($1,$2,$3,'CREDIT',$4,'BRL',$5,$6,now())`, uuid.New(), walletID, txID, amountMinor, balance, after); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}
