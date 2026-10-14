//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/domain"
)

// Uma transação financeira concluída que exige movimento (OPENING, BET, WIN, REFUND,
// ROLLBACK em PROCESSED) só é confirmada com o lançamento do seu próprio movimento, e o
// resultado devolvido ao provedor é o saldo posterior ao lançamento DESSA operação.
// LOSS e rejeições não têm lançamento. Tudo é verificado por SQL direto pelo PAPEL
// RESTRITO de runtime, com payloads de hash válido, e depois por replay real (HTTP).

func rtInTx(t *testing.T, pool *pgxpool.Pool, fn func(ctx context.Context, tx pgx.Tx) error) error {
	t.Helper()
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := fn(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	return tx.Commit(ctx)
}

func expectPgMessage(t *testing.T, name string, err error, code, contains string) {
	t.Helper()
	e := pgErr(err)
	if e == nil {
		t.Errorf("%s: expected PostgreSQL error %s, got %v", name, code, err)
		return
	}
	if e.Code != code || !strings.Contains(e.Message, contains) {
		t.Errorf("%s: got %s %q, want %s containing %q", name, e.Code, e.Message, code, contains)
	}
}

func minorString(minor int64) string { return fmt.Sprintf("%d.%02d", minor/100, minor%100) }

// directTx descreve uma transação financeira externa gravada por SQL, com o hash de
// payload calculado pelo domínio (a aplicação a reidrata e a reproduz como qualquer outra).
type directTx struct {
	ID                uuid.UUID
	Provider, Key, Ex string
	Kind              string
	Minor             int64
}

func newDirectBet(provider, key, ext string, minor int64) directTx {
	return directTx{ID: uuid.Must(uuid.NewV7()), Provider: provider, Key: key, Ex: ext, Kind: "BET", Minor: minor}
}

func (d directTx) hash(t *testing.T, w wallet) string {
	t.Helper()
	req, err := domain.NewExternalRequest(domain.RawExternalRequest{
		ProviderID: d.Provider, ExternalTransactionID: d.Ex, IdempotencyKey: d.Key, PlayerID: w.PlayerID, WalletID: w.ID,
		RoundID: "round-1", GameID: "fortune-chimp", Kind: d.Kind, Amount: minorString(d.Minor), Currency: "BRL",
	})
	if err != nil {
		t.Fatal(err)
	}
	return req.PayloadHash()
}

const insExternal = `INSERT INTO wager_transactions
	(id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id, idempotency_key,
	 payload_hash, round_id, game_id, failure_code, result_balance, created_at, updated_at)
	VALUES ($1,'EXTERNAL',$2,$3,$4,$5,$6,'BRL',$7,$8,$9,$10,'round-1','fortune-chimp',$11,$12,now(),now())`

// insert grava a transação com o estado e o resultado indicados (result nil = NULL).
func (d directTx) insert(t *testing.T, ctx context.Context, tx pgx.Tx, w wallet, status string, failure, result any) error {
	t.Helper()
	_, err := tx.Exec(ctx, insExternal, d.ID, d.Kind, status, w.ID, w.PlayerID, d.Minor, d.Provider, d.Ex, d.Key, d.hash(t, w), failure, result)
	return err
}

func entry(ctx context.Context, tx pgx.Tx, w wallet, txID uuid.UUID, direction string, amount, before, after int64) error {
	_, err := tx.Exec(ctx, insEntry, uuid.New(), w.ID, txID, direction, amount, before, after)
	return err
}

func setBalance(ctx context.Context, tx pgx.Tx, w wallet, balance int64) error {
	_, err := tx.Exec(ctx, `UPDATE wallets SET balance = $2, version = version + 1, updated_at = now() WHERE id = $1`, w.ID, balance)
	return err
}

// Cenário da auditoria: BET PROCESSED, com hash válido, sem lançamento e sem alterar a carteira.
func TestProcessedMovementWithoutLedgerEntryIsRejected(t *testing.T) {
	a := startApp(t, testConfig())
	rt := runtimePool(t)
	id := scoped()

	t.Run("BET PROCESSED without ledger entry or wallet update", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("k1"), id("t1"), 1000)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			return bet.insert(t, ctx, tx, w, "PROCESSED", nil, int64(9000))
		})
		expectPgMessage(t, "completed BET without movement", err, "23514", "no ledger entry")
		if n := countRows(t, "wager_transactions", "id = $1", bet.ID); n != 0 {
			t.Fatalf("the incomplete transaction was persisted")
		}
		// Sem a linha, a identidade continua livre: o replay não devolve saldo fictício, é uma operação nova.
		r := submit(t, a, "provider-a", bet.Key, betBodyFor(w, bet))
		if r.Status != http.StatusOK || r.Body["idempotentReplay"] != false || r.str("balance", "amount") != "90.00" {
			t.Fatalf("the identity must still be unregistered: %d %s", r.Status, r.Raw)
		}
		assertLedgerMatchesBalance(t, w.ID)
	})

	t.Run("fictitious result with the wallet untouched", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("k2"), id("t2"), 1000)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			return bet.insert(t, ctx, tx, w, "PROCESSED", nil, int64(777_00))
		})
		expectPgMessage(t, "fictitious result", err, "23514", "no ledger entry")
		if bal, ver := walletState(t, w.ID); bal != 100_00 || ver != 1 {
			t.Fatalf("wallet changed: %d v%d", bal, ver)
		}
	})

	t.Run("PROCESSED OPENING without ledger entry", func(t *testing.T) {
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			w := sqlWallet{uuid.New(), uuid.New()}
			if _, err := tx.Exec(ctx, insWallet, w.id, w.player, 0, 1); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, insOpen, uuid.New(), w.id, w.player, 500) // 5,00 de abertura, sem lançamento
			return err
		})
		expectPgMessage(t, "OPENING without movement", err, "23514", "no ledger entry")
	})

	t.Run("PENDING completed to PROCESSED without a ledger entry", func(t *testing.T) {
		// O papel restrito não atualiza transações; a conclusão é feita pelo dono do schema.
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("k3"), id("t3"), 1000)
		err := inTx(func(ctx context.Context, tx pgx.Tx) error {
			if err := bet.insert(t, ctx, tx, w, "PENDING", nil, nil); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE wager_transactions SET status = 'PROCESSED', result_balance = 9000, updated_at = now() WHERE id = $1`, bet.ID)
			return err
		})
		expectPgMessage(t, "PENDING -> PROCESSED without movement", err, "23514", "no ledger entry")
		if n := countRows(t, "wager_transactions", "id = $1", bet.ID); n != 0 {
			t.Fatal("the transaction was persisted")
		}
	})

	t.Run("PENDING needs no movement but is never committed", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("k4"), id("t4"), 1000)
		// Dentro da transação, PENDING não exige lançamento; confirmado, ele é recusado: não há aceite
		// assíncrono, e uma pendência só existe como PENDING_REFERENCE com agenda (pending_references_test.go).
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error { return bet.insert(t, ctx, tx, w, "PENDING", nil, nil) })
		expectPgMessage(t, "committed PENDING", err, "23514", "cannot be committed as PENDING")
		if n := countRows(t, "wager_transactions", "id = $1", bet.ID); n != 0 {
			t.Fatal("the PENDING transaction was persisted")
		}
	})
}

// O resultado de uma operação com movimento é o balance_after do lançamento dela.
func TestProcessedResultMustEqualItsOwnLedgerEntry(t *testing.T) {
	a := startApp(t, testConfig())
	rt := runtimePool(t)
	id := scoped()

	debit := func(t *testing.T, ctx context.Context, tx pgx.Tx, w wallet, bet directTx, before, after int64, result any) error {
		if err := bet.insert(t, ctx, tx, w, "PROCESSED", nil, result); err != nil {
			return err
		}
		if err := entry(ctx, tx, w, bet.ID, "DEBIT", bet.Minor, before, after); err != nil {
			return err
		}
		return setBalance(ctx, tx, w, after)
	}

	t.Run("ledger 100,00 -> 90,00, wallet 90,00, result 777,00 (audit scenario)", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("r1"), id("r1"), 1000)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			return debit(t, ctx, tx, w, bet, 10000, 9000, int64(777_00))
		})
		expectPgMessage(t, "result differs from the ledger entry", err, "23514", "balance_after")
		if bal, ver := walletState(t, w.ID); bal != 100_00 || ver != 1 {
			t.Fatalf("wallet changed: %d v%d", bal, ver)
		}
		if n := countRows(t, "wager_transactions", "id = $1", bet.ID); n != 0 {
			t.Fatal("the transaction was persisted")
		}
		assertLedgerMatchesBalance(t, w.ID)
	})

	t.Run("result zero although balance_after is 9000", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("r2"), id("r2"), 1000)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error { return debit(t, ctx, tx, w, bet, 10000, 9000, int64(0)) })
		expectPgMessage(t, "zero result", err, "23514", "balance_after")
	})

	t.Run("result equals the balance_after of the entry: committed and replayed as recorded", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("r3"), id("r3"), 1000)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error { return debit(t, ctx, tx, w, bet, 10000, 9000, int64(9000)) })
		if err != nil {
			t.Fatalf("valid completed BET rejected: %v", err)
		}
		replay := submit(t, a, "provider-a", bet.Key, betBodyFor(w, bet))
		if replay.Status != http.StatusOK || replay.Body["idempotentReplay"] != true || replay.str("balance", "amount") != "90.00" ||
			replay.str("transactionId") != bet.ID.String() {
			t.Fatalf("replay of the directly written payload: %d %s", replay.Status, replay.Raw)
		}
		assertLedgerMatchesBalance(t, w.ID)
	})

	t.Run("several valid operations in one SQL transaction keep their own results", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		first := newDirectBet("provider-a", id("m1"), id("m1"), 1000)
		second := newDirectBet("provider-a", id("m2"), id("m2"), 2500)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			if err := debit(t, ctx, tx, w, first, 10000, 9000, int64(9000)); err != nil {
				return err
			}
			// A carteira já está em 90,00 dentro desta transação; o resultado do segundo é 65,00.
			return debit(t, ctx, tx, w, second, 9000, 6500, int64(6500))
		})
		if err != nil {
			t.Fatalf("two valid operations in one transaction: %v", err)
		}
		// O resultado histórico da primeira NÃO é o saldo atual (65,00).
		if bal, ver := walletState(t, w.ID); bal != 65_00 || ver != 3 {
			t.Fatalf("wallet %d v%d", bal, ver)
		}
		for _, c := range []struct {
			bet  directTx
			want string
		}{{first, "90.00"}, {second, "65.00"}} {
			r := submit(t, a, "provider-a", c.bet.Key, betBodyFor(w, c.bet))
			if r.Status != http.StatusOK || r.Body["idempotentReplay"] != true || r.str("balance", "amount") != c.want {
				t.Errorf("replay of %s: %d %s, want balance %s", c.bet.Ex, r.Status, r.Raw, c.want)
			}
		}
		assertLedgerMatchesBalance(t, w.ID)
	})

	t.Run("the second operation of a transaction cannot borrow the first one's result", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		first := newDirectBet("provider-a", id("b1"), id("b1"), 1000)
		second := newDirectBet("provider-a", id("b2"), id("b2"), 2500)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			if err := debit(t, ctx, tx, w, first, 10000, 9000, int64(9000)); err != nil {
				return err
			}
			return debit(t, ctx, tx, w, second, 9000, 6500, int64(9000)) // resultado da primeira
		})
		expectPgMessage(t, "borrowed result", err, "23514", "balance_after")
	})

	t.Run("a stale result is not compared with the current wallet balance", func(t *testing.T) {
		// O saldo atual (60,00) difere do resultado histórico (90,00) e isso é legítimo.
		w := openWallet(t, a, "100.00")
		first := newDirectBet("provider-a", id("h1"), id("h1"), 1000)
		if err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			return debit(t, ctx, tx, w, first, 10000, 9000, int64(9000))
		}); err != nil {
			t.Fatal(err)
		}
		later := newDirectBet("provider-a", id("h2"), id("h2"), 3000)
		if err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			return debit(t, ctx, tx, w, later, 9000, 6000, int64(6000))
		}); err != nil {
			t.Fatalf("later operation: %v", err)
		}
		if bal, _ := walletState(t, w.ID); bal != 60_00 {
			t.Fatalf("balance %d", bal)
		}
	})
}

// LOSS e rejeições não produzem lançamento, e um lançamento para elas é recusado.
func TestOperationsWithoutMovementKeepNoLedgerEntry(t *testing.T) {
	a := startApp(t, testConfig())
	rt := runtimePool(t)
	id := scoped()
	w := openWallet(t, a, "100.00")

	loss := directTx{ID: uuid.Must(uuid.NewV7()), Provider: "provider-a", Key: id("l1"), Ex: id("l1"), Kind: "LOSS", Minor: 0}
	if err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
		return loss.insert(t, ctx, tx, w, "PROCESSED", nil, int64(100_00)) // LOSS processada: sem lançamento nem mudança de versão
	}); err != nil {
		t.Fatalf("PROCESSED LOSS needs no ledger entry: %v", err)
	}
	if bal, ver := walletState(t, w.ID); bal != 100_00 || ver != 1 {
		t.Fatalf("LOSS changed the wallet: %d v%d", bal, ver)
	}

	rejected := newDirectBet("provider-a", id("x1"), id("x1"), 90_000)
	if err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
		return rejected.insert(t, ctx, tx, w, "REJECTED", "INSUFFICIENT_FUNDS", int64(100_00))
	}); err != nil {
		t.Fatalf("REJECTED needs no ledger entry: %v", err)
	}
	r := submit(t, a, "provider-a", rejected.Key, betBodyFor(w, rejected))
	if r.Status != http.StatusUnprocessableEntity || r.Body["idempotentReplay"] != true || r.str("failureCode") != "INSUFFICIENT_FUNDS" || r.str("balance", "amount") != "100.00" {
		t.Fatalf("replay of the rejection: %d %s", r.Status, r.Raw)
	}

	// Um lançamento para LOSS ou para uma rejeição continua proibido.
	loss2 := directTx{ID: uuid.Must(uuid.NewV7()), Provider: "provider-a", Key: id("l2"), Ex: id("l2"), Kind: "LOSS", Minor: 0}
	err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
		if err := loss2.insert(t, ctx, tx, w, "PROCESSED", nil, int64(100_00)); err != nil {
			return err
		}
		return entry(ctx, tx, w, loss2.ID, "CREDIT", 1, 10000, 10001) // amount > 0 contradiz LOSS = 0
	})
	expectPg(t, "ledger entry for a LOSS", err, "23514", "")
	rej2 := newDirectBet("provider-a", id("x2"), id("x2"), 1000)
	err = rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
		if err := rej2.insert(t, ctx, tx, w, "REJECTED", "INSUFFICIENT_FUNDS", int64(90_00)); err != nil {
			return err
		}
		if err := entry(ctx, tx, w, rej2.ID, "DEBIT", 1000, 10000, 9000); err != nil {
			return err
		}
		return setBalance(ctx, tx, w, 9000)
	})
	expectPgMessage(t, "ledger entry for a REJECTED operation", err, "23514", "not PROCESSED")
	assertLedgerMatchesBalance(t, w.ID)
}

func betBodyFor(w wallet, d directTx) map[string]any {
	b := betBody(w, d.Provider, d.Ex, minorString(d.Minor))
	b["kind"] = d.Kind
	return b
}

// Ordem de escrita: as FKs do ledger são imediatas. O registro da transação precisa existir
// antes do lançamento; a conclusão (PROCESSED) pode vir depois dele, e a verificação final é
// deferida ao COMMIT.
func TestInsertionOrderOfTransactionAndLedgerEntry(t *testing.T) {
	a := startApp(t, testConfig())
	rt := runtimePool(t)
	id := scoped()

	t.Run("a ledger entry before its transaction fails immediately", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("o1"), id("o1"), 1000)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			// lançamento primeiro: a FK para a transação é imediata (23503 no próprio comando)
			if err := entry(ctx, tx, w, bet.ID, "DEBIT", 1000, 10000, 9000); err != nil {
				return err
			}
			return bet.insert(t, ctx, tx, w, "PROCESSED", nil, int64(9000))
		})
		expectPg(t, "entry before transaction", err, "23503", "")
	})

	t.Run("transaction, then entry, then completion in the same SQL transaction", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("o2"), id("o2"), 1000)
		err := inTx(func(ctx context.Context, tx pgx.Tx) error { // a conclusão exige UPDATE: dono do schema
			if err := bet.insert(t, ctx, tx, w, "PENDING", nil, nil); err != nil {
				return err
			}
			if err := entry(ctx, tx, w, bet.ID, "DEBIT", 1000, 10000, 9000); err != nil {
				return err
			}
			if err := setBalance(ctx, tx, w, 9000); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE wager_transactions SET status = 'PROCESSED', result_balance = 9000, updated_at = now() WHERE id = $1`, bet.ID)
			return err
		})
		if err != nil {
			t.Fatalf("completion after the entry: %v", err)
		}
		assertLedgerMatchesBalance(t, w.ID)
	})

	t.Run("transaction already PROCESSED, then entry", func(t *testing.T) {
		w := openWallet(t, a, "100.00")
		bet := newDirectBet("provider-a", id("o3"), id("o3"), 1000)
		err := rtInTx(t, rt, func(ctx context.Context, tx pgx.Tx) error {
			if err := bet.insert(t, ctx, tx, w, "PROCESSED", nil, int64(9000)); err != nil {
				return err
			}
			if err := entry(ctx, tx, w, bet.ID, "DEBIT", 1000, 10000, 9000); err != nil {
				return err
			}
			return setBalance(ctx, tx, w, 9000)
		})
		if err != nil {
			t.Fatalf("transaction before entry: %v", err)
		}
	})
}
