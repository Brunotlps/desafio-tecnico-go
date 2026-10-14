//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Tentativas diretas no banco, sem passar pela aplicação: as garantias
// financeiras precisam valer independentemente do código que escreve.

func pgErr(err error) *pgconn.PgError {
	var e *pgconn.PgError
	if errors.As(err, &e) {
		return e
	}
	return nil
}

func expectPg(t *testing.T, name string, err error, code, constraint string) {
	t.Helper()
	e := pgErr(err)
	if e == nil {
		t.Errorf("%s: expected PostgreSQL error %s, got %v", name, code, err)
		return
	}
	if e.Code != code || (constraint != "" && e.ConstraintName != constraint) {
		t.Errorf("%s: got %s %q (%s), want %s %q", name, e.Code, e.ConstraintName, e.Message, code, constraint)
	}
}

// inTx executa fn em uma transação e devolve o primeiro erro, incluindo o do COMMIT
// (onde falham as constraints deferidas).
func inTx(fn func(ctx context.Context, tx pgx.Tx) error) error {
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func sqlExec(sql string, args ...any) error {
	_, err := testPool.Exec(context.Background(), sql, args...)
	return err
}

// insertRejected grava uma transação externa REJECTED (terminal, sem movimento nem lançamento): um registro
// existente e válido, usado como fixture. (Antes era PENDING; PENDING deixou de poder ser confirmado sem
// retomada durável — ver pending_references_test.go.)
const insertRejected = `INSERT INTO wager_transactions
	(id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id, idempotency_key,
	 payload_hash, round_id, game_id, failure_code, result_balance, created_at, updated_at)
	VALUES ($1,'EXTERNAL',$2,'REJECTED',$3,$4,$5,'BRL',$6,$7,$8,'h','r','g','FIXTURE',0,now(),now())`

// insertTx grava uma transação externa PROCESSED. Uma operação com movimento só é confirmada com o
// lançamento no mesmo commit; nos testes que não o gravam, o erro esperado vem de uma constraint imediata.
const insertTx = `INSERT INTO wager_transactions
	(id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id, idempotency_key,
	 payload_hash, round_id, game_id, result_balance, created_at, updated_at)
	VALUES ($1,$2,$3,'PROCESSED',$4,$5,$6,'BRL',$7,$8,$9,$10,$11,$12,$13,now(),now())`

func TestDatabaseConstraintsAndLedgerProtection(t *testing.T) {
	svc := newService(t)
	walletID, _ := svcOpen(t, svc, "100.00")
	w := wallet{ID: walletID}
	var playerID, openingTx uuid.UUID
	if err := testPool.QueryRow(t.Context(), `SELECT player_id FROM wallets WHERE id = $1`, w.ID).Scan(&playerID); err != nil {
		t.Fatal(err)
	}
	if err := testPool.QueryRow(t.Context(), `SELECT id FROM wager_transactions WHERE wallet_id = $1`, w.ID).Scan(&openingTx); err != nil {
		t.Fatal(err)
	}
	extTx := func(kind string, amount int64, provider, ext, key any) error {
		return sqlExec(insertTx, uuid.New(), "EXTERNAL", kind, w.ID, playerID, amount, provider, ext, key, "h", "r", "g", 0)
	}

	t.Run("one wallet per player and currency", func(t *testing.T) {
		err := sqlExec(`INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) VALUES ($1,$2,'BRL',0,1,now(),now())`, uuid.New(), playerID)
		expectPg(t, "duplicate wallet", err, "23505", "uq_wallets_player_currency")
	})
	t.Run("balance cannot be negative", func(t *testing.T) {
		err := sqlExec(`UPDATE wallets SET balance = -1, version = version + 1 WHERE id = $1`, w.ID)
		expectPg(t, "negative balance", err, "23514", "ck_wallets_balance_nonneg")
		err = sqlExec(`INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at) VALUES ($1,$2,'BRL',-5,1,now(),now())`, uuid.New(), uuid.New())
		expectPg(t, "negative initial balance", err, "23514", "ck_wallets_balance_nonneg")
	})
	t.Run("version rules", func(t *testing.T) {
		expectPg(t, "version without balance change", sqlExec(`UPDATE wallets SET version = version + 1 WHERE id = $1`, w.ID), "23000", "")
		expectPg(t, "balance change without version bump", sqlExec(`UPDATE wallets SET balance = balance + 1 WHERE id = $1`, w.ID), "23000", "")
		expectPg(t, "identity change", sqlExec(`UPDATE wallets SET player_id = $2 WHERE id = $1`, w.ID, uuid.New()), "23000", "")
		expectPg(t, "wallet delete", sqlExec(`DELETE FROM wallets WHERE id = $1`, w.ID), "23000", "")
		expectPg(t, "wallet truncate", sqlExec(`TRUNCATE wallets CASCADE`), "23000", "")
	})
	t.Run("ledger is append-only", func(t *testing.T) {
		expectPg(t, "ledger update", sqlExec(`UPDATE wallet_ledger_entries SET amount = amount WHERE wallet_id = $1`, w.ID), "23000", "")
		expectPg(t, "ledger delete", sqlExec(`DELETE FROM wallet_ledger_entries WHERE wallet_id = $1`, w.ID), "23000", "")
		expectPg(t, "ledger truncate", sqlExec(`TRUNCATE wallet_ledger_entries`), "23000", "")
		if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1", w.ID); n != 1 {
			t.Fatalf("ledger entries = %d", n)
		}
	})
	t.Run("ledger uniqueness and arithmetic", func(t *testing.T) {
		err := sqlExec(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
			VALUES ($1,$2,$3,'CREDIT',1,'BRL',10000,10001,now())`, uuid.New(), w.ID, openingTx)
		expectPg(t, "duplicate (wallet, transaction)", err, "23505", "uq_ledger_wallet_transaction")
		err = sqlExec(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
			VALUES ($1,$2,$3,'DEBIT',10,'BRL',10000,9995,now())`, uuid.New(), w.ID, uuid.New())
		expectPg(t, "inconsistent arithmetic", err, "23514", "ck_ledger_arithmetic")
		err = sqlExec(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
			VALUES ($1,$2,$3,'CREDIT',0,'BRL',10000,10000,now())`, uuid.New(), w.ID, uuid.New())
		expectPg(t, "zero amount", err, "23514", "ck_ledger_amount")
		freeTx := uuid.New() // transação existente (REJECTED), sem lançamento
		if err := sqlExec(insertRejected, freeTx, "WIN", w.ID, playerID, 10, "p-ledger", "e-ledger", "k-ledger"); err != nil {
			t.Fatal(err)
		}
		err = sqlExec(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
			VALUES ($1,$2,$3,'CREDIT',10,'USD',10000,10010,now())`, uuid.New(), w.ID, freeTx)
		expectPg(t, "ledger currency differs from wallet", err, "23503", "fk_ledger_wallet_currency")
	})
	t.Run("balance and ledger commit together", func(t *testing.T) {
		// Saldo alterado sem lançamento: o COMMIT falha.
		err := inTx(func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `UPDATE wallets SET balance = balance + 500, version = version + 1 WHERE id = $1`, w.ID)
			return err
		})
		expectPg(t, "balance change without ledger entry", err, "23514", "")
		// Lançamento sem atualização do saldo: o COMMIT falha.
		txID := uuid.New()
		err = inTx(func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, insertTx, txID, "EXTERNAL", "WIN", w.ID, playerID, 700, "p", "e-"+txID.String(), "k-"+txID.String(), "h", "r", "g", 10700); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, direction, amount, currency, balance_before, balance_after, created_at)
				VALUES ($1,$2,$3,'CREDIT',700,'BRL',10000,10700,now())`, uuid.New(), w.ID, txID)
			return err
		})
		expectPg(t, "ledger entry without balance update", err, "23514", "")
		if bal, ver := walletState(t, w.ID); bal != 100_00 || ver != 1 {
			t.Fatalf("wallet changed: balance=%d version=%d", bal, ver)
		}
		if n := countRows(t, "wager_transactions", "id = $1", txID); n != 0 {
			t.Fatalf("rolled-back transaction row persisted")
		}
	})
	t.Run("no duplicate initial credit", func(t *testing.T) {
		err := sqlExec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, result_balance, created_at, updated_at)
			VALUES ($1,'INTERNAL','OPENING','PROCESSED',$2,$3,500,'BRL',500,now(),now())`, uuid.New(), w.ID, playerID)
		expectPg(t, "second OPENING", err, "23505", "uq_wager_tx_one_opening_per_wallet")
	})
	t.Run("internal and external shapes are distinct", func(t *testing.T) {
		expectPg(t, "OPENING with provider metadata",
			sqlExec(insertTx, uuid.New(), "INTERNAL", "OPENING", w.ID, playerID, 5, "p", "e", "k", "h", "r", "g", 5), "23514", "ck_wager_tx_origin_shape")
		expectPg(t, "external OPENING",
			sqlExec(insertTx, uuid.New(), "EXTERNAL", "OPENING", w.ID, playerID, 5, "p", "e", "k", "h", "r", "g", 5), "23514", "ck_wager_tx_origin_shape")
		expectPg(t, "external BET without provider",
			sqlExec(insertTx, uuid.New(), "EXTERNAL", "BET", w.ID, playerID, 5, nil, nil, nil, nil, nil, nil, 5), "23514", "ck_wager_tx_origin_shape")
		expectPg(t, "internal BET",
			sqlExec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, result_balance, created_at, updated_at)
				VALUES ($1,'INTERNAL','BET','PROCESSED',$2,$3,5,'BRL',5,now(),now())`, uuid.New(), w.ID, playerID), "23514", "ck_wager_tx_origin_shape")
	})
	t.Run("amount policy by kind", func(t *testing.T) {
		expectPg(t, "zero BET", extTx("BET", 0, "p", "e0", "k0"), "23514", "ck_wager_tx_amount")
		expectPg(t, "non-zero LOSS", extTx("LOSS", 5, "p", "e1", "k1"), "23514", "ck_wager_tx_amount")
		expectPg(t, "negative BET", extTx("BET", -5, "p", "e2", "k2"), "23514", "ck_wager_tx_amount")
		expectPg(t, "REFUND without reference", sqlExec(insertRejected, uuid.New(), "REFUND", w.ID, playerID, 5, "p", "e3", "k3"), "23514", "ck_wager_tx_reference")
		expectPg(t, "PROCESSED REFUND without a resolved reference", extTx("REFUND", 5, "p", "e3b", "k3b"), "23514", "ck_wager_tx_processed_reference")
	})
	t.Run("financial identity uniqueness", func(t *testing.T) {
		pid := "constraint-" + uuid.NewString()
		// Transações REJECTED (sem movimento) bastam para exercitar a unicidade.
		pending := func(provider, ext, key string) error {
			return sqlExec(insertRejected, uuid.New(), "WIN", w.ID, playerID, 5, provider, ext, key)
		}
		if err := pending(pid, "ext-1", "key-1"); err != nil {
			t.Fatal(err)
		}
		expectPg(t, "same external id, other key", pending(pid, "ext-1", "key-2"), "23505", "uq_wager_tx_provider_external_id")
		expectPg(t, "same key, other external id", pending(pid, "ext-2", "key-1"), "23505", "uq_wager_tx_provider_idempotency_key")
		if err := pending(pid+"-other", "ext-1", "key-1"); err != nil {
			t.Fatalf("another provider may reuse identifiers: %v", err)
		}
	})
	t.Run("wallet identity of transactions", func(t *testing.T) {
		err := sqlExec(insertTx, uuid.New(), "EXTERNAL", "WIN", w.ID, uuid.New(), 5, "p", "e-x", "k-x", "h", "r", "g", 5)
		expectPg(t, "transaction for another player", err, "23503", "fk_wager_tx_wallet_player")
		err = sqlExec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id, result_balance, created_at, updated_at)
			VALUES ($1,'EXTERNAL','WIN','PROCESSED',$2,$3,5,'USD','p','e-y','k-y','h','r','g',5,now(),now())`, uuid.New(), w.ID, playerID)
		expectPg(t, "transaction in another currency", err, "23503", "fk_wager_tx_wallet_currency")
	})
	t.Run("terminal transactions are immutable", func(t *testing.T) {
		expectPg(t, "update terminal", sqlExec(`UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X_Y', result_balance = 0 WHERE id = $1`, openingTx), "23000", "")
		expectPg(t, "delete", sqlExec(`DELETE FROM wager_transactions WHERE id = $1`, openingTx), "23000", "")
		expectPg(t, "truncate", sqlExec(`TRUNCATE wager_transactions CASCADE`), "23000", "")
	})
	t.Run("outbox snapshot is immutable", func(t *testing.T) {
		var eventID uuid.UUID
		if err := testPool.QueryRow(t.Context(), `SELECT id FROM outbox_events WHERE aggregate_id = $1`, w.ID).Scan(&eventID); err != nil {
			t.Fatal(err)
		}
		expectPg(t, "payload edit", sqlExec(`UPDATE outbox_events SET payload = '{}'::jsonb WHERE id = $1`, eventID), "23000", "")
		expectPg(t, "event type edit", sqlExec(`UPDATE outbox_events SET event_type = 'X' WHERE id = $1`, eventID), "23000", "")
		if err := sqlExec(`UPDATE outbox_events SET attempts = attempts + 1, last_error = 'e', next_attempt_at = now() WHERE id = $1`, eventID); err != nil {
			t.Fatalf("publication bookkeeping must be allowed: %v", err)
		}
	})
	t.Run("no floating point money columns", func(t *testing.T) {
		rows, err := testPool.Query(t.Context(), `SELECT table_name, column_name, data_type FROM information_schema.columns
			WHERE table_schema = 'public' AND data_type IN ('real', 'double precision', 'numeric', 'money')`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var tbl, col, typ string
			_ = rows.Scan(&tbl, &col, &typ)
			t.Errorf("%s.%s is %s", tbl, col, typ)
		}
	})
}
