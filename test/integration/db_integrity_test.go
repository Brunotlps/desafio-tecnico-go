//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Regressões de integridade no PostgreSQL, por SQL direto: cadeia do ledger,
// vínculo lançamento/transação/carteira e resultado explícito nos estados terminais.

const (
	insWallet = `INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) VALUES ($1,$2,'BRL',$3,$4,now(),now())`
	insOpen   = `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, result_balance, created_at, updated_at)
		VALUES ($1,'INTERNAL','OPENING','PROCESSED',$2,$3,$4,'BRL',$4,now(),now())`
	insBet = `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
		idempotency_key, payload_hash, round_id, game_id, result_balance, created_at, updated_at)
		VALUES ($1,'EXTERNAL',$5,$6,$2,$3,$4,'BRL','integrity',$7,$7,'hash','r','g',$8,now(),now())`
	insEntry = `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
		VALUES ($1,$2,$3,$4,$5,'BRL',$6,$7,now())`
)

type sqlWallet struct{ id, player uuid.UUID }

// seedOpened cria, dentro de tx, uma carteira com crédito de abertura (centavos) e saldo igual.
func seedOpened(t *testing.T, ctx context.Context, tx pgx.Tx, opening int64) sqlWallet {
	t.Helper()
	w := sqlWallet{uuid.New(), uuid.New()}
	openID := uuid.New()
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{insWallet, []any{w.id, w.player, opening, 1}},
		{insOpen, []any{openID, w.id, w.player, opening}},
		{insEntry, []any{uuid.New(), w.id, openID, "CREDIT", opening, 0, opening}},
	} {
		if _, err := tx.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	return w
}

// seedCommitted cria e confirma uma carteira com abertura.
func seedCommitted(t *testing.T, opening int64) sqlWallet {
	t.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := seedOpened(t, ctx, tx, opening)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return w
}

func TestLedgerChainMustBeContinuous(t *testing.T) {
	ctx := context.Background()

	t.Run("broken chain from the review scenario is rejected", func(t *testing.T) {
		// OPENING 100,00 (0 -> 100); débito de 10,00 declarando antes=90,00 depois=80,00;
		// carteira em 80,00. Créditos - débitos = 90,00, não 80,00.
		err := inTx(func(ctx context.Context, tx pgx.Tx) error {
			w := seedOpened(t, ctx, tx, 10000)
			betID := uuid.New()
			if _, err := tx.Exec(ctx, insBet, betID, w.id, w.player, 1000, "BET", "PROCESSED", betID.String(), 8000); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, insEntry, uuid.New(), w.id, betID, "DEBIT", 1000, 9000, 8000); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE wallets SET balance = 8000, version = 2 WHERE id = $1`, w.id)
			return err
		})
		expectPg(t, "broken chain", err, "23514", "")
	})

	t.Run("first entry must start from zero", func(t *testing.T) {
		err := inTx(func(ctx context.Context, tx pgx.Tx) error {
			w, open := sqlWallet{uuid.New(), uuid.New()}, uuid.New()
			if _, err := tx.Exec(ctx, insWallet, w.id, w.player, 500, 1); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, insOpen, open, w.id, w.player, 200); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, insEntry, uuid.New(), w.id, open, "CREDIT", 200, 300, 500) // antes deveria ser 0
			return err
		})
		expectPg(t, "first entry not from zero", err, "23514", "")
	})

	t.Run("several entries in one transaction form a valid chain", func(t *testing.T) {
		var w sqlWallet
		err := inTx(func(ctx context.Context, tx pgx.Tx) error {
			w = seedOpened(t, ctx, tx, 10000)
			for i, after := range []int64{9000, 8000} { // dois débitos de 10,00 na mesma transação
				id := uuid.New()
				if _, err := tx.Exec(ctx, insBet, id, w.id, w.player, 1000, "BET", "PROCESSED", id.String(), after); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, insEntry, uuid.New(), w.id, id, "DEBIT", 1000, after+1000, after); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE wallets SET balance = $2, version = $3 WHERE id = $1`, w.id, after, 2+i); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("valid chain rejected: %v", err)
		}
		if bal, ver := walletState(t, w.id.String()); bal != 8000 || ver != 3 {
			t.Fatalf("balance=%d version=%d", bal, ver)
		}
		assertLedgerMatchesBalance(t, w.id.String())
	})

	t.Run("second entry in the same transaction cannot skip the first", func(t *testing.T) {
		err := inTx(func(ctx context.Context, tx pgx.Tx) error {
			w := seedOpened(t, ctx, tx, 10000)
			a, b := uuid.New(), uuid.New()
			for _, e := range []struct {
				id         uuid.UUID
				before, to int64
			}{{a, 10000, 9000}, {b, 10000, 9000}} { // o segundo repete antes=10000 em vez de 9000
				if _, err := tx.Exec(ctx, insBet, e.id, w.id, w.player, 1000, "BET", "PROCESSED", e.id.String(), e.to); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, insEntry, uuid.New(), w.id, e.id, "DEBIT", 1000, e.before, e.to); err != nil {
					return err
				}
			}
			return nil
		})
		expectPg(t, "skipped link", err, "23514", "")
	})

	t.Run("concurrent writers of the same wallet are serialized and the stale one fails", func(t *testing.T) {
		w := seedCommitted(t, 10000)
		debit := func(ctx context.Context, tx pgx.Tx, to int64) error {
			id := uuid.New()
			if _, err := tx.Exec(ctx, insBet, id, w.id, w.player, 1000, "BET", "PROCESSED", id.String(), to); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, insEntry, uuid.New(), w.id, id, "DEBIT", 1000, 10000, to)
			return err
		}
		tx1, err := testPool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx1.Rollback(ctx)
		if err := debit(ctx, tx1, 9000); err != nil {
			t.Fatal(err)
		}
		if _, err := tx1.Exec(ctx, `UPDATE wallets SET balance = 9000, version = 2 WHERE id = $1`, w.id); err != nil {
			t.Fatal(err)
		}

		second := make(chan error, 1)
		go func() { // lê o mesmo estado antigo (antes = 10000)
			second <- inTx(func(ctx context.Context, tx pgx.Tx) error { return debit(ctx, tx, 9000) })
		}()
		select {
		case err := <-second:
			t.Fatalf("second writer was not blocked by the wallet lock: %v", err)
		case <-time.After(400 * time.Millisecond):
		}

		// Outra carteira não é bloqueada.
		other := seedCommitted(t, 5000)
		done := make(chan struct{})
		go func() {
			defer close(done)
			err := inTx(func(ctx context.Context, tx pgx.Tx) error {
				id := uuid.New()
				if _, err := tx.Exec(ctx, insBet, id, other.id, other.player, 500, "BET", "PROCESSED", id.String(), 4500); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, insEntry, uuid.New(), other.id, id, "DEBIT", 500, 5000, 4500); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, `UPDATE wallets SET balance = 4500, version = 2 WHERE id = $1`, other.id)
				return err
			})
			if err != nil {
				t.Errorf("independent wallet: %v", err)
			}
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("a different wallet was blocked (global lock?)")
		}

		if err := tx1.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-second:
			expectPg(t, "stale writer after the first commit", err, "23514", "")
		case <-time.After(5 * time.Second):
			t.Fatal("second writer did not finish after the lock was released")
		}
		if bal, ver := walletState(t, w.id.String()); bal != 9000 || ver != 2 {
			t.Fatalf("balance=%d version=%d", bal, ver)
		}
		assertLedgerMatchesBalance(t, w.id.String())
	})
}

func TestLedgerEntryIsBoundToItsWalletAndTransaction(t *testing.T) {
	t.Run("transaction of another wallet", func(t *testing.T) {
		w1, w2 := seedCommitted(t, 10000), seedCommitted(t, 10000)
		bet := uuid.New()
		err := inTx(func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, insBet, bet, w2.id, w2.player, 1000, "BET", "PROCESSED", bet.String(), 9000); err != nil {
				return err
			}
			// lançamento na carteira 1 apontando para a transação da carteira 2
			_, err := tx.Exec(ctx, insEntry, uuid.New(), w1.id, bet, "DEBIT", 1000, 10000, 9000)
			return err
		})
		expectPg(t, "entry for a transaction of another wallet", err, "23503", "fk_ledger_transaction_wallet")
	})

	// As verificações abaixo são deferidas (a transação financeira pode ser concluída no mesmo commit).
	commitWith := func(t *testing.T, kind, status string, txAmount int64, direction string, entryAmount int64) error {
		w := seedCommitted(t, 10000)
		return inTx(func(ctx context.Context, tx pgx.Tx) error {
			id := uuid.New()
			var result any = int64(0)
			if status == "PENDING" {
				result = nil
			}
			if _, err := tx.Exec(ctx, insBet, id, w.id, w.player, txAmount, kind, status, id.String(), result); err != nil {
				return err
			}
			after := int64(10000) - entryAmount
			if direction == "CREDIT" {
				after = 10000 + entryAmount
			}
			if _, err := tx.Exec(ctx, insEntry, uuid.New(), w.id, id, direction, entryAmount, 10000, after); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE wallets SET balance = $2, version = 2 WHERE id = $1`, w.id, after)
			return err
		})
	}
	t.Run("matching BET debit commits", func(t *testing.T) {
		if err := commitWith(t, "BET", "PROCESSED", 1000, "DEBIT", 1000); err != nil {
			t.Fatalf("valid entry rejected: %v", err)
		}
	})
	t.Run("entry for a transaction that is not PROCESSED", func(t *testing.T) {
		expectPg(t, "pending", commitWith(t, "BET", "PENDING", 1000, "DEBIT", 1000), "23514", "")
	})
	t.Run("entry amount differs from the transaction", func(t *testing.T) {
		expectPg(t, "amount", commitWith(t, "BET", "PROCESSED", 1000, "DEBIT", 900), "23514", "")
	})
	t.Run("BET credited instead of debited", func(t *testing.T) {
		expectPg(t, "direction", commitWith(t, "BET", "PROCESSED", 1000, "CREDIT", 1000), "23514", "")
	})
	t.Run("WIN debited instead of credited", func(t *testing.T) {
		expectPg(t, "direction", commitWith(t, "WIN", "PROCESSED", 1000, "DEBIT", 1000), "23514", "")
	})
}

func TestTerminalStatesRequireAnExplicitResult(t *testing.T) {
	w := seedCommitted(t, 10000)
	insert := func(status string, failure, result any, kind string, ref any) error {
		id := uuid.New()
		return sqlExec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
			idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, failure_code, result_balance, created_at, updated_at)
			VALUES ($1,'EXTERNAL',$2,$3,$4,$5,100,'BRL','results',$6,$6,'h','r','g',$7,$8,$9,now(),now())`,
			id, kind, status, w.id, w.player, id.String(), ref, failure, result)
	}
	const shape = "ck_wager_tx_status_shape"

	// Estados que exigem resultado: NULL não pode passar (um CHECK aceita "desconhecido").
	expectPg(t, "PROCESSED with NULL result", insert("PROCESSED", nil, nil, "BET", nil), "23514", shape)
	expectPg(t, "REJECTED with NULL result", insert("REJECTED", "INSUFFICIENT_FUNDS", nil, "BET", nil), "23514", shape)
	// Valores inválidos.
	expectPg(t, "PROCESSED with negative result", insert("PROCESSED", nil, -1, "BET", nil), "23514", shape)
	expectPg(t, "REJECTED with negative result", insert("REJECTED", "INSUFFICIENT_FUNDS", -1, "BET", nil), "23514", shape)
	expectPg(t, "PROCESSED with a failure code", insert("PROCESSED", "SOME_CODE", 100, "BET", nil), "23514", shape)
	expectPg(t, "REJECTED without failure code", insert("REJECTED", nil, 100, "BET", nil), "23514", shape)
	expectPg(t, "PENDING with a result", insert("PENDING", nil, 100, "BET", nil), "23514", shape)
	expectPg(t, "PENDING with a failure code", insert("PENDING", "SOME_CODE", nil, "BET", nil), "23514", shape)
	expectPg(t, "PENDING_REFERENCE with a result", insert("PENDING_REFERENCE", nil, 100, "REFUND", "ref"), "23514", shape)
	expectPg(t, "FAILED without failure code", insert("FAILED", nil, nil, "BET", nil), "23514", shape)
	expectPg(t, "failure code with lowercase letters", insert("REJECTED", "insufficient", 100, "BET", nil), "23514", "")
	expectPg(t, "unknown status", insert("DONE", nil, 100, "BET", nil), "23514", "ck_wager_tx_status")

	// Combinações válidas continuam aceitas.
	for name, err := range map[string]error{
		"PROCESSED":         insert("PROCESSED", nil, 0, "BET", nil), // zero é um resultado válido, distinto de NULL
		"REJECTED":          insert("REJECTED", "INSUFFICIENT_FUNDS", 100, "BET", nil),
		"PENDING":           insert("PENDING", nil, nil, "BET", nil),
		"PENDING_REFERENCE": insert("PENDING_REFERENCE", nil, nil, "REFUND", "ref"),
		"FAILED":            insert("FAILED", "PERSISTENCE_FAILED", nil, "BET", nil),
	} {
		if err != nil {
			t.Errorf("valid %s rejected: %v", name, err)
		}
	}
}
