//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

// SQL direto pelo papel restrito de runtime tentando contornar as garantias de referência,
// reversão e pendência. As verificações imediatas falham no próprio comando; as deferidas, no COMMIT.

const insertFull = `INSERT INTO wager_transactions
	(id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id, idempotency_key,
	 payload_hash, round_id, game_id, reference_external_transaction_id, reference_transaction_id, failure_code, result_balance, created_at, updated_at)
	VALUES ($1,'EXTERNAL',$2,$3,$4,$5,$6,'BRL',$7,$8,$8,'h',$9,'g',$10,$11,$12,$13,now(),now())`

type refFixture struct {
	svc                           *app.Service
	wallet, player                string
	otherWallet, otherPlayer      string
	bet, win, rejectedBet, refund domain.Status
	ids                           map[string]uuid.UUID // nome -> id da transação
}

func newRefFixture(t *testing.T) (*refFixture, func(string) string) {
	t.Helper()
	svc := newServiceWith(t, nil, nil)
	id := scoped()
	f := &refFixture{svc: svc, ids: map[string]uuid.UUID{}}
	f.wallet, f.player = svcOpen(t, svc, "1000.00")
	f.otherWallet, f.otherPlayer = svcOpen(t, svc, "1000.00")
	add := func(name string, raw domain.RawExternalRequest) {
		res := mustSubmit(t, svc, raw)
		f.ids[name] = res.Transaction.ID()
	}
	add("bet", op(f.wallet, f.player, "BET", id("bet"), "25.00", ""))
	add("bet2", op(f.wallet, f.player, "BET", id("bet2"), "25.00", ""))
	add("bet3", op(f.wallet, f.player, "BET", id("bet3"), "25.00", ""))
	add("win", op(f.wallet, f.player, "WIN", id("win"), "25.00", ""))
	add("otherBet", op(f.otherWallet, f.otherPlayer, "BET", id("other-bet"), "25.00", ""))
	add("refund", op(f.wallet, f.player, "REFUND", id("refund"), "25.00", id("bet")))
	big := op(f.wallet, f.player, "BET", id("rejected-bet"), "999999.00", "")
	add("rejectedBet", big)
	add("pending", op(f.wallet, f.player, "REFUND", id("pending"), "25.00", id("never-arrives")))
	return f, id
}

func TestSQLCannotBypassTheReferenceAndReversalGuarantees(t *testing.T) {
	f, id := newRefFixture(t)
	rt := runtimePool(t)
	ctx := context.Background()
	w, p := uuid.MustParse(f.wallet), uuid.MustParse(f.player)
	exec := func(sql string, args ...any) error { _, err := rt.Exec(ctx, sql, args...); return err }
	var betExt, rejExt string
	if err := testPool.QueryRow(ctx, `SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, f.ids["bet"]).Scan(&betExt); err != nil {
		t.Fatal(err)
	}
	if err := testPool.QueryRow(ctx, `SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, f.ids["rejectedBet"]).Scan(&rejExt); err != nil {
		t.Fatal(err)
	}
	var winExt, otherExt, bet2Ext string
	_ = testPool.QueryRow(ctx, `SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, f.ids["win"]).Scan(&winExt)
	_ = testPool.QueryRow(ctx, `SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, f.ids["otherBet"]).Scan(&otherExt)
	_ = testPool.QueryRow(ctx, `SELECT external_transaction_id FROM wager_transactions WHERE id = $1`, f.ids["bet2"]).Scan(&bet2Ext)

	str := func(s string) *string { return &s }
	i64 := func(n int64) *int64 { return &n }
	code := str("FIXTURE")
	ref := func(target string) *uuid.UUID { u := f.ids[target]; return &u }
	// REJECTED com referência interna: só o que o trigger de coerência admite é aceito.
	rejected := func(kind string, amount int64, provider, round string, refExt *string, refTx *uuid.UUID, wallet, player uuid.UUID) error {
		newID := uuid.Must(uuid.NewV7())
		return exec(insertFull, newID, kind, "REJECTED", wallet, player, amount, provider, "x-"+newID.String(), round, refExt, refTx, code, i64(0))
	}
	svcProvider := "svc-provider"
	roundOf := "round" // rawBet usa a rodada "round"

	t.Run("incoherent references are refused at the statement", func(t *testing.T) {
		expectPg(t, "reference in another wallet",
			rejected("REFUND", 2500, svcProvider, roundOf, str(otherExt), ref("otherBet"), w, p), "23514", "")
		expectPg(t, "another provider", rejected("REFUND", 2500, "other-provider", roundOf, str(betExt), ref("bet"), w, p), "23514", "")
		expectPg(t, "another round", rejected("REFUND", 2500, svcProvider, "other-round", str(betExt), ref("bet"), w, p), "23514", "")
		expectPg(t, "another amount", rejected("REFUND", 2499, svcProvider, roundOf, str(betExt), ref("bet"), w, p), "23514", "")
		expectPg(t, "target not PROCESSED", rejected("REFUND", 99999900, svcProvider, roundOf, str(rejExt), ref("rejectedBet"), w, p), "23514", "")
		expectPg(t, "REFUND of a WIN", rejected("REFUND", 2500, svcProvider, roundOf, str(winExt), ref("win"), w, p), "23514", "")
		expectPg(t, "WIN of a WIN", rejected("WIN", 2500, svcProvider, roundOf, str(winExt), ref("win"), w, p), "23514", "")
		expectPg(t, "external id does not match the internal reference", rejected("REFUND", 2500, svcProvider, roundOf, str("wrong-ext"), ref("bet"), w, p), "23514", "")
		expectPg(t, "unknown internal target", rejected("REFUND", 2500, svcProvider, roundOf, str(betExt), &[]uuid.UUID{uuid.New()}[0], w, p), "23503", "")
		expectPg(t, "internal reference without the external one (NULL form)", rejected("WIN", 2500, svcProvider, roundOf, nil, ref("bet"), w, p), "23514", "")
		expectPg(t, "another player", rejected("REFUND", 2500, svcProvider, roundOf, str(betExt), ref("bet"), uuid.MustParse(f.otherWallet), uuid.MustParse(f.otherPlayer)), "23514", "")
	})

	t.Run("completing without the resolved reference is refused (NULL and incomplete forms)", func(t *testing.T) {
		newID := uuid.Must(uuid.NewV7())
		for name, c := range map[string]struct {
			kind   string
			refExt *string
		}{
			"PROCESSED REFUND with no reference at all":     {"REFUND", str(betExt)},
			"PROCESSED ROLLBACK with no internal reference": {"ROLLBACK", str(betExt)},
			"PROCESSED WIN with an external reference only": {"WIN", str(betExt)},
		} {
			err := exec(insertFull, newID, c.kind, "PROCESSED", w, p, 2500, svcProvider, "n-"+name, roundOf, c.refExt, nil, nil, i64(1))
			expectPg(t, name, err, "23514", "ck_wager_tx_processed_reference")
		}
		// REFUND/ROLLBACK sem referência externa: CHECK pré-existente.
		err := exec(insertFull, uuid.Must(uuid.NewV7()), "ROLLBACK", "REJECTED", w, p, 2500, svcProvider, "n-noref", roundOf, nil, nil, code, i64(0))
		expectPg(t, "ROLLBACK with a NULL external reference", err, "23514", "ck_wager_tx_reference")
	})

	t.Run("a second PROCESSED reversal for the same target hits the unique index", func(t *testing.T) {
		// 'bet' já foi devolvida (refund processado): qualquer outra reversão PROCESSED do alvo é barrada,
		// REFUND ou ROLLBACK, antes mesmo do COMMIT.
		for _, kind := range []string{"REFUND", "ROLLBACK"} {
			newID := uuid.Must(uuid.NewV7())
			err := exec(insertFull, newID, kind, "PROCESSED", w, p, 2500, svcProvider, "dup-"+kind+id("x"), roundOf, str(betExt), ref("bet"), nil, i64(1))
			expectPg(t, "second "+kind+" of the same target", err, "23505", "uq_wager_tx_one_reversal_per_target")
		}
	})

	t.Run("ROLLBACK ledger direction must oppose the target movement (checked at COMMIT)", func(t *testing.T) {
		balance, _ := walletState(t, f.wallet)
		for name, c := range map[string]struct {
			target, targetExt, direction string
			after                        int64
		}{
			"ROLLBACK of a BET debiting":  {"bet2", bet2Ext, "DEBIT", balance - 2500},
			"ROLLBACK of a WIN crediting": {"win", winExt, "CREDIT", balance + 2500},
		} {
			newID := uuid.Must(uuid.NewV7())
			err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, insertFull, newID, "ROLLBACK", "PROCESSED", w, p, 2500, svcProvider, "dir-"+newID.String(), roundOf, str(c.targetExt), ref(c.target), nil, i64(c.after)); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = $2, version = version + 1, updated_at = now() WHERE id = $1`, w, c.after); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
					VALUES ($1,$2,$3,$4,2500,'BRL',$5,$6,now())`, uuid.New(), w, newID, c.direction, balance, c.after)
				return err
			})
			expectPgMessage(t, name, err, "23514", "does not oppose")
		}
		if b, _ := walletState(t, f.wallet); b != balance {
			t.Fatalf("balance changed to %d", b)
		}
	})
}

func TestSQLCannotBreakThePendingScheduleGuarantees(t *testing.T) {
	f, id := newRefFixture(t)
	rt := runtimePool(t)
	ctx := context.Background()
	w, p := uuid.MustParse(f.wallet), uuid.MustParse(f.player)
	pending := f.ids["pending"]
	exec := func(sql string, args ...any) error { _, err := rt.Exec(ctx, sql, args...); return err }
	i64 := func(n int64) *int64 { return &n }
	_ = id

	t.Run("columns outside the transition set are not updatable (privileges)", func(t *testing.T) {
		for name, stmt := range map[string]string{
			"amount": `UPDATE wager_transactions SET amount = 1 WHERE id = $1`, "kind": `UPDATE wager_transactions SET kind = 'BET' WHERE id = $1`,
			"wallet": `UPDATE wager_transactions SET wallet_id = wallet_id WHERE id = $1`, "round": `UPDATE wager_transactions SET round_id = 'x' WHERE id = $1`,
			"game": `UPDATE wager_transactions SET game_id = 'x' WHERE id = $1`, "provider": `UPDATE wager_transactions SET provider_id = 'x' WHERE id = $1`,
			"external id": `UPDATE wager_transactions SET external_transaction_id = 'x' WHERE id = $1`, "key": `UPDATE wager_transactions SET idempotency_key = 'x' WHERE id = $1`,
			"hash": `UPDATE wager_transactions SET payload_hash = 'x' WHERE id = $1`, "external reference": `UPDATE wager_transactions SET reference_external_transaction_id = 'x' WHERE id = $1`,
			"created_at": `UPDATE wager_transactions SET created_at = now() WHERE id = $1`,
		} {
			expectPg(t, name, exec(stmt, pending), "42501", "")
		}
	})
	t.Run("the owner cannot change the original metadata of a non-terminal transaction either", func(t *testing.T) {
		for name, stmt := range map[string]string{
			"round": `UPDATE wager_transactions SET round_id = 'x' WHERE id = $1`, "game": `UPDATE wager_transactions SET game_id = 'x' WHERE id = $1`,
			"external reference": `UPDATE wager_transactions SET reference_external_transaction_id = 'x' WHERE id = $1`,
			"created_at":         `UPDATE wager_transactions SET created_at = created_at - interval '1 hour' WHERE id = $1`,
			"amount":             `UPDATE wager_transactions SET amount = amount + 1 WHERE id = $1`,
		} {
			expectPg(t, name, sqlExec(stmt, pending), "23000", "")
		}
	})
	t.Run("invalid transitions and incoherent resolutions", func(t *testing.T) {
		expectPg(t, "back to PENDING", exec(`UPDATE wager_transactions SET status = 'PENDING' WHERE id = $1`, pending), "23000", "")
		expectPg(t, "updated_at regressing", exec(`UPDATE wager_transactions SET updated_at = updated_at - interval '1 hour' WHERE id = $1`, pending), "23000", "")
		expectPg(t, "PROCESSED without the resolved reference", exec(`UPDATE wager_transactions SET status = 'PROCESSED', result_balance = 1 WHERE id = $1`, pending), "23514", "ck_wager_tx_processed_reference")
		expectPg(t, "resolved to a target of the wrong kind (WIN for a REFUND)",
			exec(`UPDATE wager_transactions SET reference_transaction_id = $2 WHERE id = $1`, pending, f.ids["win"]), "23514", "")
		expectPg(t, "resolved to itself", exec(`UPDATE wager_transactions SET reference_transaction_id = id WHERE id = $1`, pending), "23514", "")
	})
	t.Run("a terminal transaction is immutable, including its resolved reference", func(t *testing.T) {
		expectPg(t, "change the reference of a processed refund",
			exec(`UPDATE wager_transactions SET reference_transaction_id = $2 WHERE id = $1`, f.ids["refund"], f.ids["bet2"]), "23000", "")
		expectPg(t, "change the status", exec(`UPDATE wager_transactions SET status = 'FAILED', failure_code = 'X_Y' WHERE id = $1`, f.ids["refund"]), "23000", "")
	})

	t.Run("a PENDING_REFERENCE without an open schedule is never committed", func(t *testing.T) {
		newID := uuid.Must(uuid.NewV7())
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, insertFull, newID, "REFUND", "PENDING_REFERENCE", w, p, 2500, "svc-provider", "orphan-"+newID.String(), "round", "never", nil, nil, nil)
			return err
		})
		expectPgMessage(t, "orphan PENDING_REFERENCE", err, "23514", "without an open schedule")
		if countRows(t, "wager_transactions", "id = $1", newID) != 0 {
			t.Fatal("persisted")
		}
	})
	t.Run("an open schedule cannot be left behind by a terminal transaction, nor completed under a pending one", func(t *testing.T) {
		newID := uuid.Must(uuid.NewV7())
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, insertFull, newID, "REFUND", "REJECTED", w, p, 2500, "svc-provider", "term-"+newID.String(), "round", "never", nil, "X_Y", i64(0)); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO pending_references (transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at)
				VALUES ($1,$2,now(),now() + interval '1 minute',now())`, newID, w)
			return err
		})
		expectPgMessage(t, "open schedule on a terminal transaction", err, "23514", "still has an open schedule")

		err = rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE pending_references SET completed_at = now() WHERE transaction_id = $1`, pending)
			return err
		})
		expectPgMessage(t, "completing the schedule of a still-pending transaction", err, "23514", "without an open schedule")
		if pendingOf(t, pending).Completed {
			t.Fatal("the schedule was completed")
		}
	})
	t.Run("the schedule keeps its identity and deadline, and is never deleted", func(t *testing.T) {
		expectPg(t, "move the deadline", exec(`UPDATE pending_references SET expires_at = expires_at + interval '1 day' WHERE transaction_id = $1`, pending), "42501", "")
		expectPg(t, "change first_persisted_at", exec(`UPDATE pending_references SET first_persisted_at = now() WHERE transaction_id = $1`, pending), "42501", "")
		expectPg(t, "delete", exec(`DELETE FROM pending_references WHERE transaction_id = $1`, pending), "42501", "")
		expectPg(t, "truncate", exec(`TRUNCATE pending_references`), "42501", "")
		expectPg(t, "owner moves the deadline", sqlExec(`UPDATE pending_references SET expires_at = expires_at + interval '1 day' WHERE transaction_id = $1`, pending), "23000", "")
		expectPg(t, "owner deletes", sqlExec(`DELETE FROM pending_references WHERE transaction_id = $1`, pending), "23000", "")
		expectPg(t, "attempts decreasing", exec(`UPDATE pending_references SET attempts = -1 WHERE transaction_id = $1`, pending), "23000", "")
	})
	_ = time.Second
}
