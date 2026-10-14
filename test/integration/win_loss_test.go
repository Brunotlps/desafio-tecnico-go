//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

// WIN sem referência e LOSS (enunciado §7): WIN credita uma vez e gera ledger e
// os dois eventos; LOSS exige zero, registra o saldo observado e só gera
// WagerTransactionProcessed, sem ledger, sem alterar saldo ou versão.

func rawKind(walletID, playerID, key, ext, kind, amount string) domain.RawExternalRequest {
	raw := rawBet(walletID, playerID, key, ext, amount)
	raw.Kind = kind
	return raw
}

func eventsOf(t *testing.T, txID, eventType string) int64 {
	t.Helper()
	return countRows(t, "outbox_events", "event_type = $1 AND payload->'data'->>'transactionId' = $2", eventType, txID)
}

func TestServiceWinCreditsOnceWithLedgerAndEvents(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	if _, err := svc.Submit(ctx, rawBet(wallet, player, id("bet-k"), id("bet-t"), "30.00"), "c"); err != nil {
		t.Fatal(err)
	}
	key, ext := id("win-k"), id("win-t")
	win, err := svc.Submit(ctx, rawKind(wallet, player, key, ext, "WIN", "50.00"), "c-win")
	if err != nil || win.Replay || win.Transaction.Status() != domain.StatusProcessed || win.Transaction.ResultBalance().Amount() != "120.00" {
		t.Fatalf("win: %+v %v", win, err)
	}
	if bal, ver := walletState(t, wallet); bal != 120_00 || ver != 3 {
		t.Fatalf("balance=%d version=%d, want 12000/3", bal, ver)
	}
	txID := win.Transaction.ID().String()
	if n := countRows(t, "wallet_ledger_entries",
		"transaction_id = $1 AND direction = 'CREDIT' AND amount = 5000 AND balance_before = 7000 AND balance_after = 12000", txID); n != 1 {
		t.Fatalf("credit ledger entries = %d", n)
	}
	if eventsOf(t, txID, "WagerTransactionProcessed") != 1 {
		t.Fatal("WagerTransactionProcessed missing for WIN")
	}
	if n := countRows(t, "outbox_events", `event_type = 'WalletBalanceChanged' AND payload->'data'->>'transactionId' = $1
		AND payload->'data'->>'direction' = 'CREDIT' AND (payload->'data'->>'walletVersion')::int = 3
		AND payload->'data'->'balanceBefore'->>'amount' = '70.00' AND payload->'data'->'balanceAfter'->>'amount' = '120.00'
		AND payload->>'causationId' = (SELECT payload->>'eventId' FROM outbox_events
		   WHERE event_type = 'WagerTransactionProcessed' AND payload->'data'->>'transactionId' = $1)`, txID); n != 1 {
		t.Fatalf("WalletBalanceChanged for WIN: %d", n)
	}

	// Outra movimentação; o replay conserva o saldo observado no processamento original.
	if _, err := svc.Submit(ctx, rawBet(wallet, player, id("bet2-k"), id("bet2-t"), "20.00"), "c"); err != nil {
		t.Fatal(err)
	}
	replay, err := svc.Submit(ctx, rawKind(wallet, player, key, ext, "WIN", "50.00"), "c")
	if err != nil || !replay.Replay || replay.Transaction.ID() != win.Transaction.ID() || replay.Transaction.ResultBalance().Amount() != "120.00" {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet); n != 4 { // OPENING, BET, WIN, BET
		t.Fatalf("ledger entries = %d", n)
	}
	// Conflitos de identidade continuam valendo.
	if _, err := svc.Submit(ctx, rawKind(wallet, player, key, ext, "WIN", "51.00"), "c"); !errors.Is(err, app.ErrIdempotencyKeyReused) {
		t.Errorf("same key, other amount: %v", err)
	}
	if _, err := svc.Submit(ctx, rawKind(wallet, player, id("win-k2"), ext, "WIN", "50.00"), "c"); !errors.Is(err, app.ErrExternalTransactionReused) {
		t.Errorf("same external id, other key: %v", err)
	}
	assertLedgerMatchesBalance(t, wallet)
}

func TestServiceWinOverflowLeavesNoPartialEffects(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()
	wallet, player := svcOpen(t, svc, "92233720368547758.00") // máximo representável: ...758.07
	txs := countRows(t, "wager_transactions", "wallet_id = $1", wallet)
	ledger := countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet)
	events := countRows(t, "outbox_events", "payload->'data'->>'walletId' = $1", wallet)
	balBefore, verBefore := walletState(t, wallet)

	_, err := svc.Submit(ctx, rawKind(wallet, player, id("k"), id("t"), "WIN", "1.00"), "c")
	if !errors.Is(err, domain.ErrAmountOverflow) {
		t.Fatalf("overflowing WIN: %v, want ErrAmountOverflow", err)
	}
	balAfter, verAfter := walletState(t, wallet)
	if balAfter != balBefore || verAfter != verBefore {
		t.Fatalf("wallet changed: %d/%d -> %d/%d", balBefore, verBefore, balAfter, verAfter)
	}
	if countRows(t, "wager_transactions", "wallet_id = $1", wallet) != txs ||
		countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet) != ledger ||
		countRows(t, "outbox_events", "payload->'data'->>'walletId' = $1", wallet) != events {
		t.Fatal("overflow persisted rows")
	}
	assertLedgerMatchesBalance(t, wallet)
}

func TestServiceLossRecordsTheOutcomeWithoutMovement(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")
	if _, err := svc.Submit(ctx, rawBet(wallet, player, id("bet-k"), id("bet-t"), "30.00"), "c"); err != nil {
		t.Fatal(err)
	}
	ledgerBefore := countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet)
	balBefore, verBefore := walletState(t, wallet)
	if balBefore != 70_00 || verBefore != 2 {
		t.Fatalf("setup: %d/%d", balBefore, verBefore)
	}

	key, ext := id("loss-k"), id("loss-t")
	loss, err := svc.Submit(ctx, rawKind(wallet, player, key, ext, "LOSS", "0.00"), "c-loss")
	if err != nil || loss.Replay || loss.Transaction.Status() != domain.StatusProcessed || loss.Transaction.ResultBalance().Amount() != "70.00" {
		t.Fatalf("loss: %+v %v", loss, err)
	}
	txID := loss.Transaction.ID().String()
	if bal, ver := walletState(t, wallet); bal != balBefore || ver != verBefore {
		t.Fatalf("LOSS changed the wallet: %d/%d", bal, ver)
	}
	if countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet) != ledgerBefore || countRows(t, "wallet_ledger_entries", "transaction_id = $1", txID) != 0 {
		t.Fatal("LOSS created a ledger entry")
	}
	if eventsOf(t, txID, "WagerTransactionProcessed") != 1 {
		t.Fatal("WagerTransactionProcessed missing for LOSS")
	}
	if n := countRows(t, "outbox_events", "payload->'data'->>'transactionId' = $1", txID); n != 1 {
		t.Fatalf("LOSS events = %d, want only WagerTransactionProcessed", n)
	}
	if eventsOf(t, txID, "WalletBalanceChanged") != 0 {
		t.Fatal("LOSS produced WalletBalanceChanged")
	}

	// O replay devolve o saldo observado, mesmo depois de outras movimentações.
	if _, err := svc.Submit(ctx, rawKind(wallet, player, id("win-k"), id("win-t"), "WIN", "10.00"), "c"); err != nil {
		t.Fatal(err)
	}
	replay, err := svc.Submit(ctx, rawKind(wallet, player, key, ext, "LOSS", "0.00"), "c")
	if err != nil || !replay.Replay || replay.Transaction.ID() != loss.Transaction.ID() || replay.Transaction.ResultBalance().Amount() != "70.00" {
		t.Fatalf("replay: %+v %v", replay, err)
	}

	// Entradas de carteira inválidas não são gravadas.
	before := countRows(t, "wager_transactions", "provider_id = 'svc-provider'")
	if _, err := svc.Submit(ctx, rawKind(uuid.NewString(), player, id("k"), id("t"), "LOSS", "0.00"), "c"); !errors.Is(err, app.ErrWalletNotFound) {
		t.Errorf("unknown wallet: %v", err)
	}
	if _, err := svc.Submit(ctx, rawKind(wallet, uuid.NewString(), id("k2"), id("t2"), "LOSS", "0.00"), "c"); !errors.Is(err, app.ErrWalletMismatch) {
		t.Errorf("wrong player: %v", err)
	}
	if _, err := svc.Submit(ctx, rawKind(wallet, player, id("k3"), id("t3"), "LOSS", "1.00"), "c"); !errors.Is(err, domain.ErrAmountNotAllowed) {
		t.Errorf("non-zero LOSS: %v", err)
	}
	if after := countRows(t, "wager_transactions", "provider_id = 'svc-provider'"); after != before {
		t.Fatalf("rejected inputs persisted %d rows", after-before)
	}
	assertLedgerMatchesBalance(t, wallet)
}

func TestFiftyParallelWinAndLossResendsApplyOnce(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	winKey, winExt := id("win-k"), id("win-t")
	lossKey, lossExt := id("loss-k"), id("loss-t")
	type outcome struct {
		res app.SubmitResult
		err error
	}
	results := parallel(100, func(i int) outcome {
		var raw domain.RawExternalRequest
		if i%2 == 0 {
			raw = rawKind(wallet, player, winKey, winExt, "WIN", "25.00")
		} else {
			raw = rawKind(wallet, player, lossKey, lossExt, "LOSS", "0.00")
		}
		res, err := svc.Submit(ctx, raw, "c")
		return outcome{res, err}
	})
	originals := map[domain.Kind]int{}
	for _, o := range results {
		if o.err != nil || o.res.Transaction.Status() != domain.StatusProcessed {
			t.Fatalf("unexpected: %+v %v", o.res, o.err)
		}
		if !o.res.Replay {
			originals[o.res.Transaction.Kind()]++
		}
	}
	if originals[domain.KindWin] != 1 || originals[domain.KindLoss] != 1 {
		t.Fatalf("originals = %v, want exactly one WIN and one LOSS", originals)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'CREDIT' AND amount = 2500", wallet); n != 1 {
		t.Fatalf("WIN credits = %d", n)
	}
	if bal, ver := walletState(t, wallet); bal != 125_00 || ver != 2 {
		t.Fatalf("balance=%d version=%d, want 12500/2", bal, ver)
	}
	assertLedgerMatchesBalance(t, wallet)
}

// LOSS observa o saldo sob o lock da carteira: com a linha travada por outra
// transação ele espera, e conclui assim que ela é liberada.
func TestLossWaitsForTheWalletLock(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	lockTx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, wallet); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := svc.Submit(ctx, rawKind(wallet, player, id("k"), id("t"), "LOSS", "0.00"), "c")
		done <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for queryInt(t, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND state = 'active' AND query ILIKE '%FOR UPDATE%'`) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("LOSS never started waiting for the wallet lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("LOSS completed while the wallet was locked: %v", err)
	default:
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("LOSS did not resume after the lock was released")
	}
}

func TestWinAndLossOverHTTP(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")

	winBody := func(ext, amount string) map[string]any {
		b := betBody(w, "provider-a", ext, amount)
		b["kind"] = "WIN"
		return b
	}
	lossBody := func(ext string) map[string]any {
		b := betBody(w, "provider-a", ext, "0.00")
		b["kind"] = "LOSS"
		return b
	}

	r := submit(t, a, "provider-a", id("win-k"), winBody(id("win-t"), "40.00"))
	if r.Status != http.StatusOK || r.str("status") != "PROCESSED" || r.str("balance", "amount") != "140.00" || r.Body["idempotentReplay"] != false {
		t.Fatalf("win: %d %s", r.Status, r.Raw)
	}
	l := submit(t, a, "provider-a", id("loss-k"), lossBody(id("loss-t")))
	if l.Status != http.StatusOK || l.str("status") != "PROCESSED" || l.str("balance", "amount") != "140.00" || l.Body["idempotentReplay"] != false {
		t.Fatalf("loss: %d %s", l.Status, l.Raw)
	}
	if bal, ver := walletState(t, w.ID); bal != 140_00 || ver != 2 {
		t.Fatalf("balance=%d version=%d, want 14000/2 (LOSS must not bump the version)", bal, ver)
	}

	// Replays: mesmo resultado, marcados como replay.
	if rr := submit(t, a, "provider-a", id("win-k"), winBody(id("win-t"), "40.00")); rr.Status != 200 || rr.Body["idempotentReplay"] != true || rr.str("balance", "amount") != "140.00" {
		t.Fatalf("win replay: %d %s", rr.Status, rr.Raw)
	}
	if rr := submit(t, a, "provider-a", id("loss-k"), lossBody(id("loss-t"))); rr.Status != 200 || rr.Body["idempotentReplay"] != true || rr.str("balance", "amount") != "140.00" {
		t.Fatalf("loss replay: %d %s", rr.Status, rr.Raw)
	}
	// Consulta pelo identificador externo.
	q := call(t, a.URL, "GET", "/providers/provider-a/wagering/transactions/"+id("loss-t"), token(t, "provider-a"), nil, nil)
	if q.Status != 200 || q.str("status") != "PROCESSED" || q.str("kind") != "LOSS" {
		t.Fatalf("query: %d %s", q.Status, q.Raw)
	}

	// WIN com referência ausente não é ignorada nem creditada: fica pendente (reversal_test.go cobre o resto).
	before := countRows(t, "wager_transactions", "wallet_id = $1", w.ID)
	ref := winBody(id("winref-t"), "5.00")
	ref["referenceExternalTransactionId"] = "some-bet"
	if rr := submit(t, a, "provider-a", id("winref-k"), ref); rr.Status != http.StatusAccepted || rr.str("status") != "PENDING_REFERENCE" || rr.Body["balance"] != nil {
		t.Fatalf("WIN with an absent reference: %d %s", rr.Status, rr.Raw)
	}
	if bal, _ := walletState(t, w.ID); bal != 140_00 {
		t.Fatalf("a pending WIN changed the balance to %d", bal)
	}
	before++ // a pendência é persistida
	// LOSS com valor e WIN zerado continuam recusados na entrada.
	bad := lossBody(id("bad-t"))
	bad["money"] = money("1.00")
	if rr := submit(t, a, "provider-a", id("bad-k"), bad); rr.Status != 400 || rr.str("code") != "AMOUNT_NOT_ALLOWED_FOR_KIND" {
		t.Fatalf("non-zero LOSS: %d %s", rr.Status, rr.Raw)
	}
	if after := countRows(t, "wager_transactions", "wallet_id = $1", w.ID); after != before {
		t.Fatalf("refused inputs persisted %d rows", after-before)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

// Três processos reais: reenvios de WIN e LOSS e créditos distintos sobre a mesma
// carteira não duplicam nem perdem movimentação.
func TestThreeProcessesApplyWinAndLossOnce(t *testing.T) {
	procs := startCluster(t, 3)
	id := scoped()
	pick := func(i int) *testApp { return procs[i%3].app() }
	w := openWallet(t, pick(0), "100.00")

	kindBody := func(provider, ext, kind, amount string) map[string]any {
		b := betBody(w, provider, ext, amount)
		b["kind"] = kind
		return b
	}
	// 100 envios: 50 da mesma WIN e 50 da mesma LOSS, espalhados entre os processos.
	results := parallel(100, func(i int) response {
		if i%2 == 0 {
			return submit(t, pick(i), "provider-a", id("win"), kindBody("provider-a", id("win-t"), "WIN", "25.00"))
		}
		return submit(t, pick(i), "provider-a", id("loss"), kindBody("provider-a", id("loss-t"), "LOSS", "0.00"))
	})
	originals := 0
	for _, r := range results {
		if r.Status != 200 || r.str("status") != "PROCESSED" {
			t.Fatalf("unexpected: %d %s", r.Status, r.Raw)
		}
		if r.Body["idempotentReplay"] == false {
			originals++
		}
	}
	if originals != 2 {
		t.Fatalf("originals = %d, want one WIN and one LOSS", originals)
	}
	if bal, ver := walletState(t, w.ID); bal != 125_00 || ver != 2 {
		t.Fatalf("balance=%d version=%d, want 12500/2", bal, ver)
	}

	// 30 créditos distintos de 1,00 chegando a processos diferentes: nenhuma atualização perdida.
	distinct := parallel(30, func(i int) response {
		name := id(fmt.Sprintf("many-%d", i))
		return submit(t, pick(i), "provider-b", name, kindBody("provider-b", name, "WIN", "1.00"))
	})
	for _, r := range distinct {
		if r.Status != 200 {
			t.Fatalf("unexpected: %d %s", r.Status, r.Raw)
		}
	}
	if bal, ver := walletState(t, w.ID); bal != 155_00 || ver != 32 {
		t.Fatalf("balance=%d version=%d, want 15500/32", bal, ver)
	}
	assertLedgerMatchesBalance(t, w.ID)
}
