//go:build integration

package integration

import (
	"context"
	"testing"
	"time"
)

func TestFiftyParallelResendsProduceOneDebit(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")
	key, ext := id("key"), id("tx")

	results := parallel(50, func(int) response {
		return submit(t, a, "provider-a", key, betBody(w, "provider-a", ext, "25.00"))
	})
	var originals, replays int
	txIDs := map[string]bool{}
	for _, r := range results {
		if r.Status != 200 || r.str("status") != "PROCESSED" || r.str("balance", "amount") != "75.00" {
			t.Fatalf("unexpected result: %d %s", r.Status, r.Raw)
		}
		txIDs[r.str("transactionId")] = true
		if r.Body["idempotentReplay"] == true {
			replays++
		} else {
			originals++
		}
	}
	if originals != 1 || replays != 49 || len(txIDs) != 1 {
		t.Fatalf("originals=%d replays=%d distinct transactions=%d", originals, replays, len(txIDs))
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if bal, ver := walletState(t, w.ID); bal != 75_00 || ver != 2 {
		t.Fatalf("balance=%d version=%d", bal, ver)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

// Duas apostas distintas de 80,00 sobre 100,00: uma processada, outra rejeitada
// por saldo insuficiente, saldo 20,00, um único débito. Reenvios não alteram.
func TestTwoDistinctBetsRaceForTheSameBalance(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	const wallets = 25
	type setup struct {
		w wallet
	}
	ws := make([]wallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, a, "100.00")
	}
	// Para cada carteira: 2 apostas distintas, cada uma enviada 3 vezes (6 requisições concorrentes).
	type call6 struct{ wallet, bet int }
	jobs := make([]call6, 0, wallets*6)
	for wi := range ws {
		for bet := 0; bet < 2; bet++ {
			for rep := 0; rep < 3; rep++ {
				jobs = append(jobs, call6{wi, bet})
			}
		}
	}
	results := parallel(len(jobs), func(i int) response {
		j := jobs[i]
		name := id("w" + string(rune('a'+j.wallet)) + "-bet" + string(rune('0'+j.bet)))
		return submit(t, a, "provider-a", name, betBody(ws[j.wallet], "provider-a", name, "80.00"))
	})

	type outcome struct{ processed, rejected map[string]bool }
	per := make([]outcome, wallets)
	for i := range per {
		per[i] = outcome{map[string]bool{}, map[string]bool{}}
	}
	for i, r := range results {
		wi := jobs[i].wallet
		switch {
		case r.Status == 200 && r.str("status") == "PROCESSED":
			per[wi].processed[r.str("transactionId")] = true
			if r.str("balance", "amount") != "20.00" {
				t.Fatalf("processed balance %s", r.Raw)
			}
		case r.Status == 422 && r.str("failureCode") == "INSUFFICIENT_FUNDS":
			per[wi].rejected[r.str("transactionId")] = true
		default:
			t.Fatalf("unexpected result: %d %s", r.Status, r.Raw)
		}
	}
	for wi, w := range ws {
		if len(per[wi].processed) != 1 || len(per[wi].rejected) != 1 {
			t.Fatalf("wallet %d: processed=%d rejected=%d", wi, len(per[wi].processed), len(per[wi].rejected))
		}
		if bal, _ := walletState(t, w.ID); bal != 20_00 {
			t.Fatalf("wallet %d balance %d", wi, bal)
		}
		if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 1 {
			t.Fatalf("wallet %d debits = %d", wi, n)
		}
		if n := countRows(t, "wager_transactions", "wallet_id = $1 AND kind = 'BET'", w.ID); n != 2 {
			t.Fatalf("wallet %d bet rows = %d", wi, n)
		}
		assertLedgerMatchesBalance(t, w.ID)
	}
}

// Muitas apostas pequenas sobre a mesma carteira em paralelo: nenhuma
// atualização é perdida e o saldo nunca fica negativo.
func TestNoLostUpdatesOnOneWallet(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")
	const n = 60 // 60 x 2,00 = 120,00 > 100,00: 50 passam, 10 são rejeitadas
	results := parallel(n, func(i int) response {
		name := id("bet" + string(rune('A'+i/26)) + string(rune('a'+i%26)))
		return submit(t, a, "provider-a", name, betBody(w, "provider-a", name, "2.00"))
	})
	var processed, rejected int
	for _, r := range results {
		switch r.Status {
		case 200:
			processed++
		case 422:
			rejected++
		default:
			t.Fatalf("unexpected: %d %s", r.Status, r.Raw)
		}
	}
	if processed != 50 || rejected != 10 {
		t.Fatalf("processed=%d rejected=%d", processed, rejected)
	}
	if bal, ver := walletState(t, w.ID); bal != 0 || ver != 51 {
		t.Fatalf("balance=%d version=%d", bal, ver)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

// Carteiras diferentes não se bloqueiam: com o lock da carteira A mantido por
// outra transação, uma aposta na carteira B é concluída; a de A só avança
// quando o lock é liberado.
func TestIndependentWalletsProgressInParallel(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	wa, wb := openWallet(t, a, "100.00"), openWallet(t, a, "100.00")

	ctx := context.Background()
	lockTx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, wa.ID); err != nil {
		t.Fatal(err)
	}

	blocked := make(chan response, 1)
	go func() { blocked <- submit(t, a, "provider-a", id("ka"), betBody(wa, "provider-a", id("ta"), "10.00")) }()
	time.Sleep(300 * time.Millisecond) // a requisição de A já está esperando o lock

	done := make(chan response, 1)
	go func() { done <- submit(t, a, "provider-a", id("kb"), betBody(wb, "provider-a", id("tb"), "10.00")) }()
	select {
	case r := <-done:
		if r.Status != 200 {
			t.Fatalf("wallet B: %d %s", r.Status, r.Raw)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("wallet B was blocked by a lock held on wallet A (global lock?)")
	}
	select {
	case r := <-blocked:
		t.Fatalf("wallet A completed while its row was locked: %d %s", r.Status, r.Raw)
	default:
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-blocked:
		if r.Status != 200 {
			t.Fatalf("wallet A after unlock: %d %s", r.Status, r.Raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wallet A did not resume after the lock was released")
	}
	assertLedgerMatchesBalance(t, wa.ID)
	assertLedgerMatchesBalance(t, wb.ID)
}

// Carteiras distintas processadas simultaneamente, cada uma com várias apostas.
func TestManyWalletsProcessedSimultaneously(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	const wallets, betsEach = 30, 4
	ws := make([]wallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, a, "100.00")
	}
	results := parallel(wallets*betsEach, func(i int) response {
		wi := i % wallets
		name := id("m" + string(rune('A'+wi)) + string(rune('0'+i/wallets)))
		return submit(t, a, "provider-b", name, betBody(ws[wi], "provider-b", name, "10.00"))
	})
	for _, r := range results {
		if r.Status != 200 {
			t.Fatalf("unexpected: %d %s", r.Status, r.Raw)
		}
	}
	for _, w := range ws {
		if bal, ver := walletState(t, w.ID); bal != 60_00 || ver != 1+betsEach {
			t.Fatalf("wallet balance=%d version=%d", bal, ver)
		}
		assertLedgerMatchesBalance(t, w.ID)
	}
}
