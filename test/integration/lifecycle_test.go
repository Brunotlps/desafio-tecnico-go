//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/fx"

	"desafio-tecnico-go/internal/bootstrap"
	"desafio-tecnico-go/internal/config"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Reinício completo da aplicação: idempotência, resultados e consistência
// vêm do banco, não da memória do processo.
func TestRestartPreservesIdempotencyAndResults(t *testing.T) {
	id := scoped()
	app1 := startApp(t, testConfig())
	w := openWallet(t, app1, "100.00")

	ok := submit(t, app1, "provider-a", id("k-ok"), betBody(w, "provider-a", id("t-ok"), "80.00"))
	rej := submit(t, app1, "provider-a", id("k-rej"), betBody(w, "provider-a", id("t-rej"), "80.00"))
	if ok.Status != 200 || rej.Status != 422 {
		t.Fatalf("setup: %d %s | %d %s", ok.Status, ok.Raw, rej.Status, rej.Raw)
	}
	app1.Stop(t)
	if _, err := net.DialTimeout("tcp", strings.TrimPrefix(app1.URL, "http://"), time.Second); err == nil {
		t.Fatal("first instance still listening after stop")
	}

	app2 := startApp(t, testConfig())
	// Mais uma movimentação na nova instância, para o saldo atual divergir dos resultados antigos.
	creditDirect(t, w.ID, 50_00)

	r1 := submit(t, app2, "provider-a", id("k-ok"), betBody(w, "provider-a", id("t-ok"), "80.00"))
	if r1.Status != 200 || r1.Body["idempotentReplay"] != true || r1.str("transactionId") != ok.str("transactionId") || r1.str("balance", "amount") != "20.00" {
		t.Fatalf("replay of processed after restart: %d %s", r1.Status, r1.Raw)
	}
	r2 := submit(t, app2, "provider-a", id("k-rej"), betBody(w, "provider-a", id("t-rej"), "80.00"))
	if r2.Status != 422 || r2.Body["idempotentReplay"] != true || r2.str("transactionId") != rej.str("transactionId") || r2.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("replay of rejection after restart: %d %s", r2.Status, r2.Raw)
	}
	if r3 := submit(t, app2, "provider-a", id("k-ok"), betBody(w, "provider-a", id("t-ok"), "81.00")); r3.Status != 409 {
		t.Fatalf("conflict after restart: %d %s", r3.Status, r3.Raw)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

// Composição Fx: grafo válido, início, serviço, encerramento e liberação de recursos.
func TestFxCompositionStartsAndReleasesResources(t *testing.T) {
	cfg := testConfig()
	if err := fx.ValidateApp(bootstrap.Options(cfg)); err != nil {
		t.Fatalf("dependency graph: %v", err)
	}

	appName := "fxtest-" + uuid.NewString()[:8]
	cfg.DatabaseURL = withAppName(env.runtimeURL, appName)
	a := startApp(t, cfg)
	w := openWallet(t, a, "10.00")
	if r := submit(t, a, "provider-a", uniqueID("k"), betBody(w, "provider-a", uniqueID("t"), "1.00")); r.Status != 200 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	if n := connectionsOf(t, appName); n == 0 {
		t.Fatal("expected open database connections while running")
	}

	a.Stop(t)
	if _, err := net.DialTimeout("tcp", strings.TrimPrefix(a.URL, "http://"), time.Second); err == nil {
		t.Error("HTTP listener still open after stop")
	}
	deadline := time.Now().Add(5 * time.Second)
	for connectionsOf(t, appName) > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if n := connectionsOf(t, appName); n != 0 {
		t.Errorf("%d database connections left after stop", n)
	}
}

// Dependências inválidas impedem a inicialização e não deixam recursos para trás:
// em nenhum caso sobra uma conexão com o PostgreSQL, inclusive quando é o próprio
// componente de persistência que falha (o Fx não chama o OnStop de um hook cujo
// OnStart falhou).
func TestStartupFailsFastAndReleasesResources(t *testing.T) {
	// Bancos exclusivos deste teste, com nomes derivados de TEST_DATABASE_URL.
	notMigrated := createScratchDatabase(t, "unmigrated")
	outdated := createScratchDatabase(t, "outdated")
	mig, err := postgres.NewMigrator(outdated)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mig.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := mig.Down(context.Background()); err != nil { // fica uma versão atrás do binário
		t.Fatal(err)
	}
	mig.Close()
	if err := postgres.ProvisionRuntimeRole(context.Background(), outdated, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		t.Fatal(err)
	}
	runtime := func(ownerURL string) string {
		u, err := withCredentials(ownerURL, env.runtimeRole, env.runtimePass)
		if err != nil {
			t.Fatal(err)
		}
		return u
	}

	cases := map[string]struct {
		mutate  func(*config.Config)
		wantErr string // trecho esperado no erro de partida
	}{
		"postgres unreachable": {func(c *config.Config) {
			c.DatabaseURL = "postgres://wager:wager@127.0.0.1:1/wager?sslmode=disable&connect_timeout=1"
		}, "ping postgres"},
		"schema not migrated":     {func(c *config.Config) { c.DatabaseURL = runtime(notMigrated) }, "not initialized"},
		"schema out of date":      {func(c *config.Config) { c.DatabaseURL = runtime(outdated) }, "schema is at version 1"},
		"idp unreachable":         {func(c *config.Config) { c.OIDCJWKSURL = "http://127.0.0.1:1/certs" }, "JWKS"},
		"queue missing":           {func(c *config.Config) { c.SQSWagerQueue = "does-not-exist.fifo" }, "does-not-exist.fifo"},
		"broker unreachable":      {func(c *config.Config) { c.AWSEndpointURL = "http://127.0.0.1:1" }, "sqs"},
		"invalid database access": {func(c *config.Config) { c.DatabaseURL = withBadPassword(t, env.runtimeURL) }, "ping postgres"},
	}
	for name, tc := range cases {
		mutate := tc.mutate
		t.Run(name, func(t *testing.T) {
			appName := "startfail-" + uuid.NewString()[:8]
			cfg := testConfig(mutate)
			cfg.DatabaseURL = withAppName(cfg.DatabaseURL, appName)
			app := fx.New(bootstrap.Options(cfg), fx.NopLogger)
			if err := app.Err(); err != nil {
				t.Fatalf("composition failed instead of start: %v", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			err := app.Start(ctx)
			cancel()
			if err == nil {
				stopCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
				_ = app.Stop(stopCtx)
				c()
				t.Fatal("started with an invalid dependency")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("start error %q does not mention %q", err, tc.wantErr)
			}
			deadline := time.Now().Add(5 * time.Second)
			for connectionsOf(t, appName) > 0 && time.Now().Before(deadline) {
				time.Sleep(50 * time.Millisecond)
			}
			if n := connectionsOf(t, appName); n != 0 {
				t.Errorf("failed startup (%v) left %d PostgreSQL connection(s) open", err, n)
			}
		})
	}
}

func withBadPassword(t *testing.T, dbURL string) string {
	u, err := withCredentials(dbURL, env.runtimeRole, "definitely-wrong")
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// Shutdown: para de aceitar novas conexões e conclui a requisição em andamento.
func TestGracefulShutdownCompletesInFlightRequests(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")

	ctx := context.Background()
	lockTx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, w.ID); err != nil {
		t.Fatal(err)
	}

	inFlight := make(chan response, 1)
	go func() { inFlight <- submit(t, a, "provider-a", id("k"), betBody(w, "provider-a", id("t"), "10.00")) }()
	time.Sleep(300 * time.Millisecond) // requisição presa no lock da carteira

	stopped := make(chan struct{})
	go func() { a.Stop(t); close(stopped) }()
	time.Sleep(300 * time.Millisecond)

	// Nenhuma entrada nova durante o encerramento.
	if conn, err := net.DialTimeout("tcp", strings.TrimPrefix(a.URL, "http://"), time.Second); err == nil {
		conn.Close()
		t.Error("new connections accepted during shutdown")
	}
	select {
	case <-stopped:
		t.Fatal("shutdown finished before the in-flight request")
	default:
	}

	if err := lockTx.Rollback(ctx); err != nil && err != pgx.ErrTxClosed {
		t.Fatal(err)
	}
	select {
	case r := <-inFlight:
		if r.Status != http.StatusOK || r.str("status") != "PROCESSED" {
			t.Fatalf("in-flight request: %d %s", r.Status, r.Raw)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("in-flight request did not complete")
	}
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish")
	}
	if bal, _ := walletState(t, w.ID); bal != 90_00 {
		t.Fatalf("balance %d", bal)
	}
}
