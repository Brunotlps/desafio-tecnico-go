//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

func reconcileHTTP(t *testing.T, a *testApp, walletID string) response {
	t.Helper()
	return call(t, a.URL, "POST", "/wallets/"+walletID+"/reconciliation", token(t, "internal-service"), nil, nil)
}

func expectReconciliation(t *testing.T, r response, stored, calculated, diff string, consistent bool, entries float64) {
	t.Helper()
	got := func(k string) string { return r.str(k, "amount") + " " + r.str(k, "currency") }
	if r.Status != 200 || got("storedBalance") != stored+" BRL" || got("calculatedBalance") != calculated+" BRL" || got("difference") != diff+" BRL" ||
		r.Body["consistent"] != consistent || r.Body["checkedEntries"] != entries {
		t.Fatalf("reconciliation: %d %s\nwant stored=%s calculated=%s difference=%s consistent=%v entries=%v", r.Status, r.Raw, stored, calculated, diff, consistent, entries)
	}
}

// snapshotOfState reúne o que a reconciliação não pode alterar.
func stateOf(t *testing.T, walletID string) string {
	t.Helper()
	var s string
	if err := testPool.QueryRow(context.Background(), `SELECT w.balance::text || '/' || w.version::text || '/' || w.updated_at::text
		|| '/' || (SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = w.id)
		|| '/' || (SELECT count(*) FROM wager_transactions WHERE wallet_id = w.id)
		|| '/' || (SELECT count(*) FROM outbox_events) FROM wallets w WHERE w.id = $1`, walletID).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestReconciliationOfZeroAndOpeningWalletsAndTheEnunciadoExample(t *testing.T) {
	a := startApp(t, testConfig())
	zero := openWallet(t, a, "0.00")
	expectReconciliation(t, reconcileHTTP(t, a, zero.ID), "0.00", "0.00", "0.00", true, 0)

	w := openWallet(t, a, "100.00")
	expectReconciliation(t, reconcileHTTP(t, a, w.ID), "100.00", "100.00", "0.00", true, 1)
	if r := submitBET(t, a, w, "25.00"); r.Status != 200 {
		t.Fatal(r.Raw)
	}
	before := stateOf(t, w.ID)
	r := reconcileHTTP(t, a, w.ID)
	expectReconciliation(t, r, "75.00", "75.00", "0.00", true, 2) // opening 100,00 + BET 25,00
	if r.str("walletId") != w.ID {
		t.Errorf("walletId: %s", r.Raw)
	}
	if after := stateOf(t, w.ID); after != before {
		t.Errorf("the reconciliation changed the wallet, ledger, transactions or outbox: %s -> %s", before, after)
	}
	if got := reconcileHTTP(t, a, uuid.NewString()); got.Status != 404 || got.str("code") != "WALLET_NOT_FOUND" {
		t.Errorf("unknown wallet: %d %s", got.Status, got.Raw)
	}
	if got := call(t, a.URL, "POST", "/wallets/zzz/reconciliation", token(t, "internal-service"), nil, nil); got.Status != 400 {
		t.Errorf("invalid id: %d %s", got.Status, got.Raw)
	}
}

// BET/WIN/LOSS, rejeição por saldo e a cadeia de reversões: só movimentos aceitos entram no ledger.
func TestReconciliationAfterBetWinLossRejectionAndReversalChain(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")
	send := func(kind, ext, amount, ref string, want int) response {
		t.Helper()
		body := betBody(w, "provider-a", ext, amount)
		body["kind"] = kind
		if ref != "" {
			body["referenceExternalTransactionId"] = ref
		}
		r := submit(t, a, "provider-a", ext, body)
		if r.Status != want {
			t.Fatalf("%s %s: %d %s, want %d", kind, ext, r.Status, r.Raw, want)
		}
		return r
	}
	id := uniqueID("chain")
	send("BET", id+"-bet", "25.00", "", 200)               // 75
	send("WIN", id+"-win", "10.00", id+"-bet", 200)        // 85
	send("LOSS", id+"-loss", "0.00", "", 200)              // sem lançamento
	send("BET", id+"-big", "500.00", "", 422)              // rejeitada: sem lançamento
	send("REFUND", id+"-refund", "25.00", id+"-bet", 200)  // 110
	send("ROLLBACK", id+"-rb", "25.00", id+"-refund", 200) // 85: desfaz o REFUND (débito)
	// opening, bet, win, refund, rollback
	expectReconciliation(t, reconcileHTTP(t, a, w.ID), "85.00", "85.00", "0.00", true, 5)
}

// A reconciliação lê tudo numa única visão: um movimento confirmado ENTRE a leitura do saldo e a soma do ledger não gera
// divergência, e a leitura não segura lock que impeça o movimento independente.
func TestReconciliationUsesOneSnapshotAcrossAMovementCommittedBetweenItsQueries(t *testing.T) {
	ctx := context.Background()
	svc, store := ledgerSvc(t, nil)
	wID, pID := svcOpen(t, svc, "100.00")
	wid := uuid.MustParse(wID)

	reached, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	store.SetReconciliationHook(func(stage string) {
		if stage == "after-wallet-read" {
			once.Do(func() { close(reached) })
			<-proceed
		}
	})
	defer store.SetReconciliationHook(nil)

	type out struct {
		res app.ReconciliationResult
		err error
	}
	done := make(chan out, 1)
	go func() {
		r, err := svc.Reconcile(ctx, wid)
		done <- out{r, err}
	}()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the reconciliation never reached the barrier")
	}
	// A leitura do saldo já aconteceu (100,00) e a transação continua aberta: o movimento independente não espera por ela.
	moved := make(chan error, 1)
	go func() {
		_, err := svc.Submit(ctx, op(wID, pID, "BET", uniqueID("between"), "25.00", ""), "snapshot-test")
		moved <- err
	}()
	select {
	case err := <-moved:
		if err != nil {
			t.Fatalf("movement during the open snapshot: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the movement waited for the reconciliation: the read holds a financial lock")
	}
	if n := queryInt(t, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1`, wID); n != 2 {
		t.Fatalf("setup: the movement should have been committed (%d entries)", n)
	}
	close(proceed)
	o := <-done
	if o.err != nil {
		t.Fatal(o.err)
	}
	// Um único momento: saldo e ledger anteriores ao movimento, consistentes entre si.
	if !o.res.Consistent || o.res.Stored.Amount() != "100.00" || o.res.Calculated.Amount() != "100.00" || o.res.Difference.Amount() != "0.00" || o.res.CheckedEntries != 1 {
		t.Fatalf("one-snapshot result: %+v", o.res)
	}
	// Uma reconciliação seguinte enxerga o movimento, também consistente.
	store.SetReconciliationHook(nil)
	next, err := svc.Reconcile(ctx, wid)
	if err != nil || !next.Consistent || next.Stored.Amount() != "75.00" || next.CheckedEntries != 2 {
		t.Fatalf("next reconciliation: %+v %v", next, err)
	}
}

// Somas separadas acima de int64 com saldo final válido: o resultado é exato, sem overflow intermediário.
func TestReconciliationIsExactWhenSeparateSumsExceedInt64(t *testing.T) {
	a := startApp(t, testConfig())
	const maxAmount = "92233720368547758.07" // o maior Money (MaxInt64 centavos)
	w := openWallet(t, a, maxAmount)
	id := uniqueID("big")
	for i := 0; i < 3; i++ {
		bet := fmt.Sprintf("%s-bet%d", id, i)
		if r := submit(t, a, "provider-a", bet, betBody(w, "provider-a", bet, maxAmount)); r.Status != 200 {
			t.Fatalf("bet %d: %d %s", i, r.Status, r.Raw)
		}
		win := fmt.Sprintf("%s-win%d", id, i)
		body := betBody(w, "provider-a", win, maxAmount)
		body["kind"] = "WIN"
		if r := submit(t, a, "provider-a", win, body); r.Status != 200 {
			t.Fatalf("win %d: %d %s", i, r.Status, r.Raw)
		}
	}
	// Prova de que a soma dos créditos (e a dos débitos) sozinha ultrapassa int64.
	if over := queryInt(t, `SELECT count(*) FROM (SELECT sum(amount) FILTER (WHERE direction = 'CREDIT') AS c, sum(amount) FILTER (WHERE direction = 'DEBIT') AS d
		FROM wallet_ledger_entries WHERE wallet_id = $1) s WHERE c > 9223372036854775807::numeric AND d > 9223372036854775807::numeric`, w.ID); over != 1 {
		t.Fatal("setup: separate credit and debit sums must exceed int64")
	}
	expectReconciliation(t, reconcileHTTP(t, a, w.ID), maxAmount, maxAmount, "0.00", true, 7)
}

// Divergência real, preparada só pelo dono num banco EXCLUSIVO (a fixture desliga a trava de consistência; nada disso
// existe como funcionalidade): resposta, log e sinal de divergência coerentes; saldo, ledger e versão intactos depois.
func TestReconciliationReportsARealDivergenceWithoutChangingAnything(t *testing.T) {
	ctx := context.Background()
	d := newUpgradeDB(t, "divergence", finalVersion(t))
	var logs bytes.Buffer
	var logMu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&lockedWriter{&logMu, &logs}, nil))
	store := postgres.NewStore(postgres.StaticPool(d.runtime))
	var detected atomic.Int64
	svc := app.NewService(store, app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef}, logger,
		app.WithLedgerReader(store), app.WithReconciliationObserver(func(app.ReconciliationResult) { detected.Add(1) }))
	wID, pID := svcOpen(t, svc, "100.00")
	mustSubmit(t, svc, op(wID, pID, "BET", uniqueID("div"), "30.00", "")) // saldo 70,00; ledger: +100 −30

	// O dono corrompe o saldo armazenado (para 90,00) com a trava de consistência desligada só nesta fixture.
	for _, stmt := range []string{
		`ALTER TABLE wallets DISABLE TRIGGER ctrg_wallets_balance_matches_ledger`,
		`UPDATE wallets SET balance = 9000, version = version + 1 WHERE id = '` + wID + `'`,
	} {
		if _, err := d.owner.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	type snap struct{ balance, version, entries, txs, events int64 }
	read := func() snap {
		var s snap
		if err := d.owner.QueryRow(ctx, `SELECT w.balance, w.version, (SELECT count(*) FROM wallet_ledger_entries), (SELECT count(*) FROM wager_transactions), (SELECT count(*) FROM outbox_events)
			FROM wallets w WHERE w.id = $1`, wID).Scan(&s.balance, &s.version, &s.entries, &s.txs, &s.events); err != nil {
			t.Fatal(err)
		}
		return s
	}
	before := read()
	res, err := svc.Reconcile(ctx, uuid.MustParse(wID))
	if err != nil {
		t.Fatal(err)
	}
	if res.Consistent || res.Stored.Amount() != "90.00" || res.Calculated.Amount() != "70.00" || res.Difference.Amount() != "20.00" || res.CheckedEntries != 2 {
		t.Fatalf("divergence result: %+v", res)
	}
	if detected.Load() != 1 {
		t.Errorf("divergence signal fired %d times", detected.Load())
	}
	logMu.Lock()
	text := logs.String()
	logMu.Unlock()
	for _, want := range []string{"reconciliation divergence detected", wID, "90.00 BRL", "70.00 BRL", "20.00 BRL", "checkedEntries=2"} {
		if !strings.Contains(text, want) {
			t.Errorf("log lacks %q:\n%s", want, text)
		}
	}
	if after := read(); after != before {
		t.Fatalf("the reconciliation changed data: %+v -> %+v", before, after)
	}
	// Uma divergência negativa (armazenado menor que o calculado) também é exata.
	if _, err := d.owner.Exec(ctx, `UPDATE wallets SET balance = 5000, version = version + 1 WHERE id = $1`, wID); err != nil {
		t.Fatal(err)
	}
	res, err = svc.Reconcile(ctx, uuid.MustParse(wID))
	if err != nil || res.Consistent || res.Difference.Amount() != "-20.00" {
		t.Fatalf("negative divergence: %+v %v", res, err)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// Um histórico em que a soma exata não cabe em Money (fixture exclusiva) é falha de cálculo — 500, sem resposta
// consistente nem divergência inventada.
func TestReconciliationWhoseExactSumDoesNotFitMoneyFailsInsteadOfAnswering(t *testing.T) {
	ctx := context.Background()
	d := newUpgradeDB(t, "reconcile_limit", finalVersion(t))
	store := postgres.NewStore(postgres.StaticPool(d.runtime))
	var detected atomic.Int64
	svc := app.NewService(store, app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		app.WithLedgerReader(store), app.WithReconciliationObserver(func(app.ReconciliationResult) { detected.Add(1) }))
	wID, pID := svcOpen(t, svc, "1.00")
	// Fixture: o dono insere (com as travas desligadas) créditos que, somados, passam de MaxInt64 centavos.
	stmts := []string{
		`ALTER TABLE wallet_ledger_entries DISABLE TRIGGER ALL`,
		`ALTER TABLE wager_transactions DISABLE TRIGGER ALL`,
		`ALTER TABLE wallet_ledger_entries DROP CONSTRAINT ck_ledger_arithmetic`,
		`ALTER TABLE wallet_ledger_entries DROP CONSTRAINT ck_ledger_balances`,
	}
	for _, s := range stmts {
		if _, err := d.owner.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	for i := 0; i < 2; i++ {
		txID := uuid.New()
		if _, err := d.owner.Exec(ctx, insBet, txID, wID, pID, int64(9000000000000000000), "WIN", "PROCESSED", uniqueID("limit"), int64(0)); err != nil {
			t.Fatal(err)
		}
		if _, err := d.owner.Exec(ctx, insEntry, uuid.New(), wID, txID, "CREDIT", int64(9000000000000000000), 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	_, err := svc.Reconcile(ctx, uuid.MustParse(wID))
	if err == nil || !strings.Contains(err.Error(), app.ErrReconciliationFailed.Error()) {
		t.Fatalf("an out-of-range exact sum must fail the reconciliation: %v", err)
	}
	if detected.Load() != 0 {
		t.Error("a calculation failure must not be reported as a divergence")
	}
}

// Falha de leitura é erro de infraestrutura, nunca um resultado: pool fechado → indisponibilidade transitória.
func TestReconciliationReadFailureIsAnInfrastructureErrorNotAResult(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), env.runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	store := postgres.NewStore(postgres.StaticPool(pool))
	svc := app.NewService(store, app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef}, slog.New(slog.NewTextHandler(io.Discard, nil)), app.WithLedgerReader(store))
	pool.Close()
	res, err := svc.Reconcile(context.Background(), uuid.New())
	if err == nil || res.Consistent {
		t.Fatalf("reading from a closed pool answered: %+v %v", res, err)
	}
}
