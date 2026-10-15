//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"desafio-tecnico-go/internal/bootstrap"
	"desafio-tecnico-go/internal/config"
)

// Shutdown coordenado com infraestrutura real: admissão fechada, drenagem, cancelamento, término
// confirmado e dependências fechadas por último. Tudo sincronizado por estado observável (conexões do
// banco, locks esperados), sem dormir às cegas.

func shutdownConfig(appName string, mods ...func(*config.Config)) config.Config {
	return testConfig(append([]func(*config.Config){func(c *config.Config) {
		c.DatabaseURL = withAppName(env.runtimeURL, appName)
		c.ShutdownTimeout, c.ShutdownDrain = 6*time.Second, 700*time.Millisecond
	}}, mods...)...)
}

func eventually(t *testing.T, what string, within time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func waitingForLock(like string) bool {
	var n int64
	_ = testPool.QueryRow(context.Background(), `SELECT count(*) FROM pg_stat_activity
		WHERE wait_event_type = 'Lock' AND state = 'active' AND query ILIKE $1`, like).Scan(&n)
	return n > 0
}

func TestShutdownClosesAdmissionDrainsCancelsAndClosesTheDatabaseLast(t *testing.T) {
	appName := "shutdown-" + uuid.NewString()[:8]
	a := startApp(t, shutdownConfig(appName))
	w := openWallet(t, a, "100.00")
	ctx := context.Background()

	// Um pedido fica preso esperando o lock da carteira (trabalho admitido e em andamento).
	lock, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, w.ID); err != nil {
		t.Fatal(err)
	}
	inflight := make(chan response, 1)
	tok := token(t, "provider-a")
	// tryCall tolera a conexão encerrada pelo cancelamento (Status 0), ao contrário de submit.
	go func() { inflight <- tryCall(a.URL, "k-"+w.ID, tok, betBody(w, "provider-a", "bet-"+w.ID, "10.00")) }()
	eventually(t, "the request to wait for the wallet lock", 5*time.Second, func() bool { return waitingForLock("%FOR UPDATE%") })

	stopped := make(chan struct{})
	started := time.Now()
	go func() { a.Stop(t); close(stopped) }()

	// 1. Admissão: novas entradas passam a ser recusadas (listener fechado ou 503), enquanto o pedido em andamento segue.
	client := &http.Client{Timeout: time.Second}
	eventually(t, "new requests to be refused", 5*time.Second, func() bool {
		resp, err := client.Get(a.URL + "/health/live")
		if err != nil {
			return true
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusServiceUnavailable
	})
	select {
	case r := <-inflight:
		t.Fatalf("the in-flight request ended before the drain limit: %d %s", r.Status, r.Raw)
	default:
	}
	// 2. As dependências continuam abertas enquanto o trabalho admitido existe.
	if n := connectionsOf(t, appName); n == 0 {
		t.Fatal("the database pool was closed while work was in flight")
	}

	// 3. No limite de drenagem o trabalho restante é cancelado e o término é confirmado.
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	if elapsed := time.Since(started); elapsed < 700*time.Millisecond || elapsed > 6*time.Second {
		t.Fatalf("shutdown took %v; expected the drain limit (700ms) <= elapsed <= the global budget (6s)", elapsed)
	}
	select {
	case r := <-inflight:
		if r.Status == http.StatusOK || r.Status == http.StatusCreated {
			t.Fatalf("the cancelled request reported success: %d %s", r.Status, r.Raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight request never returned")
	}
	// 4. Só depois o pool foi fechado: nenhuma conexão restante.
	eventually(t, "all database connections to be released", 5*time.Second, func() bool { return connectionsOf(t, appName) == 0 })

	// 5. Nada ficou pela metade: a aposta cancelada não existe; reenviá-la a processa uma única vez.
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := countRows(t, "wager_transactions", "wallet_id = $1 AND kind = 'BET'", w.ID); n != 0 {
		t.Fatalf("the cancelled request left %d transaction(s)", n)
	}
	b := startApp(t, testConfig())
	for i := 0; i < 2; i++ {
		r := submit(t, b, "provider-a", "k-"+w.ID, betBody(w, "provider-a", "bet-"+w.ID, "10.00"))
		if r.Status != 200 || r.str("balance", "amount") != "90.00" {
			t.Fatalf("resend %d: %d %s", i, r.Status, r.Raw)
		}
	}
	assertLedgerMatchesBalance(t, w.ID)
}

func TestShutdownDrainsAnInFlightRequestThatFinishesInTime(t *testing.T) {
	appName := "drain-" + uuid.NewString()[:8]
	a := startApp(t, shutdownConfig(appName, func(c *config.Config) { c.ShutdownDrain = 4 * time.Second }))
	w := openWallet(t, a, "100.00")
	ctx := context.Background()
	lock, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, w.ID); err != nil {
		t.Fatal(err)
	}
	inflight := make(chan response, 1)
	go func() {
		inflight <- submit(t, a, "provider-a", "k-"+w.ID, betBody(w, "provider-a", "bet-"+w.ID, "10.00"))
	}()
	eventually(t, "the request to wait for the wallet lock", 5*time.Second, func() bool { return waitingForLock("%FOR UPDATE%") })
	stopped := make(chan struct{})
	go func() { a.Stop(t); close(stopped) }()
	eventually(t, "admission to close", 5*time.Second, func() bool {
		resp, err := (&http.Client{Timeout: time.Second}).Get(a.URL + "/health/live")
		if err != nil {
			return true
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusServiceUnavailable
	})
	if err := lock.Rollback(ctx); err != nil { // o trabalho admitido pode terminar dentro da drenagem
		t.Fatal(err)
	}
	if r := <-inflight; r.Status != 200 || r.str("balance", "amount") != "90.00" {
		t.Fatalf("the drained request: %d %s", r.Status, r.Raw)
	}
	<-stopped
	eventually(t, "connections to be released", 5*time.Second, func() bool { return connectionsOf(t, appName) == 0 })
	assertLedgerMatchesBalance(t, w.ID)
}

// Uma tentativa do worker presa (esperando o lock da agenda) é cancelada no limite de drenagem; a pendência
// continua durável e outra instância a conclui.
func TestShutdownCancelsAWorkerAttemptAndTheDurableStateRecovers(t *testing.T) {
	appName := "worker-" + uuid.NewString()[:8]
	cfg := shutdownConfig(appName, func(c *config.Config) { c.ReferenceWorkers = 1 })
	a := startApp(t, cfg)
	w := openWallet(t, a, "100.00")
	ctx := context.Background()
	id := scoped()

	r := submit(t, a, "provider-a", id("k"), refBody(w, "provider-a", id("refund"), "REFUND", "25.00", id("bet")))
	if r.Status != 202 || r.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("pending: %d %s", r.Status, r.Raw)
	}
	txID := uuid.MustParse(r.str("transactionId"))
	// Prende a tentativa do worker: a leitura da transação fica esperando por um lock de tabela. (A agenda e a carteira não prendem mais a
	// tentativa — são puladas com SKIP LOCKED, por desenho —, então o travamento é na leitura da transação, que a tentativa não pula.)
	lock, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `LOCK TABLE wager_transactions IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the worker attempt to wait for the transaction read", 5*time.Second, func() bool { return waitingForLock("%wager_transactions%") })

	stopped := make(chan struct{})
	go func() { a.Stop(t); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish with a stuck worker attempt")
	}
	eventually(t, "connections to be released", 5*time.Second, func() bool { return connectionsOf(t, appName) == 0 })
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// Estado durável intacto.
	if p := pendingOf(t, txID); p.Rows != 1 || p.Completed {
		t.Fatalf("schedule: %+v", p)
	}
	if n, _ := ledgerOf(t, txID); n != 0 {
		t.Fatal("the cancelled attempt left a ledger entry")
	}
	// Uma nova instância retoma: a BET chega e a mesma operação conclui.
	b := startApp(t, testConfig(func(c *config.Config) { c.ReferenceWorkers = 1 }))
	if br := submit(t, b, "provider-a", id("kb"), betBody(w, "provider-a", id("bet"), "25.00")); br.Status != 200 {
		t.Fatalf("bet: %d %s", br.Status, br.Raw)
	}
	eventually(t, "the resumed refund to be processed", 10*time.Second, func() bool {
		return countRows(t, "wager_transactions", "id = $1 AND status = 'PROCESSED'", txID) == 1
	})
	if n, _ := ledgerOf(t, txID); n != 1 {
		t.Fatalf("ledger entries for the refund: %d", n)
	}
	if bal, _ := walletState(t, w.ID); bal != 100_00 {
		t.Fatalf("balance %d", bal)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

func TestAFailedStartupStopsTheWorkerAndReleasesTheDatabase(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	appName := "startfail-" + uuid.NewString()[:8]
	cfg := shutdownConfig(appName, func(c *config.Config) {
		c.ReferenceWorkers = 1
		c.HTTPAddr = occupied.Addr().String() // o worker inicia primeiro; a porta falha depois
	})
	a := fx.New(bootstrap.Options(cfg), fx.NopLogger)
	if err := a.Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := a.Start(ctx); err == nil {
		_ = a.Stop(ctx)
		t.Fatal("startup should have failed on the occupied port")
	}
	eventually(t, "the database connections to be released after the failed startup", 5*time.Second, func() bool { return connectionsOf(t, appName) == 0 })
}

func TestInconsistentShutdownConfigurationIsRefusedAtComposition(t *testing.T) {
	for name, mod := range map[string]func(*config.Config){
		"drain >= timeout":      func(c *config.Config) { c.ShutdownDrain = c.ShutdownTimeout },
		"backoff initial > max": func(c *config.Config) { c.ReferenceBackoffInitial = c.ReferenceBackoffMax + time.Second },
		"backoff initial > ttl": func(c *config.Config) {
			c.ReferenceBackoffInitial, c.ReferenceBackoffMax = 2*time.Minute, 3*time.Minute
		},
		"phase timeout >= timeout":       func(c *config.Config) { c.ReferenceWorkers, c.ReferencePhaseTimeout = 1, c.ShutdownTimeout },
		"zero ttl":                       func(c *config.Config) { c.ReferenceTTL = 0 },
		"more than one reference worker": func(c *config.Config) { c.ReferenceWorkers = 2 },
	} {
		cfg := testConfig(mod)
		a := fx.New(bootstrap.Options(cfg), fx.NopLogger)
		if a.Err() == nil {
			t.Errorf("%s: the composition accepted an inconsistent configuration", name)
		}
	}
}
