//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Três processos independentes do binário real (cmd/api), cada um com seu
// próprio pool de conexões e memória, sobre o mesmo PostgreSQL.

var (
	buildOnce sync.Once
	binPath   string
	buildErr  error
)

func apiBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "wager-api-bin")
		if err != nil {
			buildErr = err
			return
		}
		binPath = filepath.Join(dir, "api")
		cmd := exec.Command("go", "build", "-o", binPath, "./cmd/api")
		cmd.Dir = "../.."
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = fmt.Errorf("go build: %v\n%s", err, out)
		}
	})
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	return binPath
}

type proc struct {
	name    string
	URL     string
	cmd     *exec.Cmd
	out     *bytes.Buffer
	appName string
	done    chan struct{}
	exit    error
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startProc(t *testing.T, name string) *proc { return startProcEnv(t, name) }

// startProcEnv inicia um processo real da API; extra (CHAVE=valor) sobrescreve o ambiente padrão.
func startProcEnv(t *testing.T, name string, extra ...string) *proc {
	t.Helper()
	port := freePort(t)
	p := &proc{name: name, URL: fmt.Sprintf("http://127.0.0.1:%d", port), out: &bytes.Buffer{}, appName: name + "-" + uuid.NewString()[:6], done: make(chan struct{})}
	p.cmd = exec.Command(apiBinary(t))
	p.cmd.Env = append(os.Environ(),
		fmt.Sprintf("HTTP_ADDR=127.0.0.1:%d", port),
		"DATABASE_URL="+withAppName(env.runtimeURL, p.appName),
		"DB_MAX_CONNS=10", "LOG_LEVEL=warn", "SHUTDOWN_TIMEOUT=15s", "ALLOWED_CURRENCIES=BRL",
		"OIDC_ISSUER="+env.issuer, "OIDC_JWKS_URL="+env.jwksURL, "OIDC_AUDIENCE=wager-api", "OIDC_CLOCK_SKEW=1s",
		"AWS_ENDPOINT_URL="+env.awsEndpoint, "SQS_WAGER_QUEUE_NAME=wager-transactions.fifo",
		"REFERENCE_WORKERS=0", // o worker é ligado só pelos testes que o exercitam
		"SQS_CONSUMERS=0",     // o consumidor SQS também (os testes que o exercitam o ligam)
		"OUTBOX_PUBLISHERS=0", // o publisher da outbox também (os testes que o exercitam passam o destino e as credenciais)
	)
	p.cmd.Env = append(p.cmd.Env, extra...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.exit = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		<-p.done
		if t.Failed() {
			t.Logf("--- %s output ---\n%s", p.name, p.out.String())
		}
	})

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-p.done:
			t.Fatalf("%s exited during startup: %v\n%s", name, p.exit, p.out.String())
		default:
		}
		if resp, err := http.Get(p.URL + "/health/ready"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return p
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not become ready\n%s", name, p.out.String())
	return nil
}

func startCluster(t *testing.T, n int) []*proc { return startClusterEnv(t, n) }

func startClusterEnv(t *testing.T, n int, extra ...string) []*proc {
	procs := make([]*proc, n)
	for i := range procs {
		procs[i] = startProcEnv(t, fmt.Sprintf("api%d", i+1), extra...)
	}
	return procs
}

func (p *proc) app() *testApp { return &testApp{URL: p.URL} }

func TestThreeIndependentProcessesShareOneConsistentState(t *testing.T) {
	procs := startCluster(t, 3)
	id := scoped()

	pids := map[int]bool{}
	for _, p := range procs {
		pids[p.cmd.Process.Pid] = true
	}
	if len(pids) != 3 {
		t.Fatalf("expected three distinct processes, got %v", pids)
	}
	pick := func(i int) *testApp { return procs[i%3].app() }

	// 1. A mesma aposta 50 vezes, espalhada entre os três processos: um único débito.
	w := openWallet(t, pick(0), "100.00")
	key, ext := id("same"), id("same-tx")
	results := parallel(50, func(i int) response {
		return submit(t, pick(i), "provider-a", key, betBody(w, "provider-a", ext, "25.00"))
	})
	var originals int
	for _, r := range results {
		if r.Status != 200 || r.str("balance", "amount") != "75.00" {
			t.Fatalf("unexpected: %d %s", r.Status, r.Raw)
		}
		if r.Body["idempotentReplay"] == false {
			originals++
		}
	}
	if originals != 1 {
		t.Fatalf("originals = %d, want exactly 1", originals)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	assertLedgerMatchesBalance(t, w.ID)

	// 2. Duas apostas de 80,00 sobre 100,00, cada uma chegando a um processo diferente,
	// com reenvios cruzados, em várias carteiras ao mesmo tempo.
	const wallets = 20
	ws := make([]wallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, pick(i), "100.00")
	}
	type job struct{ w, bet, rep int }
	var jobs []job
	for wi := range ws {
		for bet := 0; bet < 2; bet++ {
			for rep := 0; rep < 3; rep++ {
				jobs = append(jobs, job{wi, bet, rep})
			}
		}
	}
	res := parallel(len(jobs), func(i int) response {
		j := jobs[i]
		name := id(fmt.Sprintf("race-%d-%d", j.w, j.bet))
		return submit(t, pick(j.bet+j.rep+j.w), "provider-b", name, betBody(ws[j.w], "provider-b", name, "80.00"))
	})
	processed, rejected := make([]map[string]bool, wallets), make([]map[string]bool, wallets)
	for i := range processed {
		processed[i], rejected[i] = map[string]bool{}, map[string]bool{}
	}
	for i, r := range res {
		wi := jobs[i].w
		switch {
		case r.Status == 200:
			processed[wi][r.str("transactionId")] = true
		case r.Status == 422 && r.str("failureCode") == "INSUFFICIENT_FUNDS":
			rejected[wi][r.str("transactionId")] = true
		default:
			t.Fatalf("unexpected: %d %s", r.Status, r.Raw)
		}
	}
	for wi, w := range ws {
		if len(processed[wi]) != 1 || len(rejected[wi]) != 1 {
			t.Fatalf("wallet %d: processed=%d rejected=%d", wi, len(processed[wi]), len(rejected[wi]))
		}
		if bal, _ := walletState(t, w.ID); bal != 20_00 {
			t.Fatalf("wallet %d balance %d", wi, bal)
		}
		if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 1 {
			t.Fatalf("wallet %d debits = %d", wi, n)
		}
		assertLedgerMatchesBalance(t, w.ID)
	}

	// 3. Cada processo usou suas próprias conexões.
	for _, p := range procs {
		if n := connectionsOf(t, p.appName); n == 0 {
			t.Errorf("%s has no database connections of its own", p.name)
		}
	}

	// 4. SIGTERM: encerramento gracioso, código de saída 0.
	for _, p := range procs {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range procs {
		select {
		case <-p.done:
			if p.exit != nil {
				t.Errorf("%s exit: %v\n%s", p.name, p.exit, p.out.String())
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("%s did not stop after SIGTERM", p.name)
		}
	}
}

// Encerramento abrupto (SIGKILL) de um processo no meio da carga: os clientes
// repetem as mesmas chaves nos demais processos e cada aposta é aplicada
// exatamente uma vez, haja ou não commit antes da queda.
func TestKillingAProcessMidFlightNeverDuplicatesOrLosesMoney(t *testing.T) {
	procs := startCluster(t, 3)
	id := scoped()
	victim, survivors := procs[0], procs[1:]
	const wallets, betsEach = 30, 10
	const bets = wallets * betsEach
	ws := make([]wallet, wallets)
	for i := range ws {
		ws[i] = openWallet(t, survivors[i%len(survivors)].app(), "100.00")
	}

	var completed, replays atomic.Int64
	killAt := int64(40)
	var killOnce sync.Once
	results := parallel(bets, func(i int) response {
		name := id(fmt.Sprintf("kill-%03d", i))
		body := betBody(ws[i%wallets], "provider-a", name, "1.00")
		doOnce := func(p *proc) (response, error) {
			r := tryCall(p.URL, name, token(t, "provider-a"), body)
			if r.Status == 0 {
				return r, fmt.Errorf("connection error")
			}
			return r, nil
		}
		r, err := doOnce(victim)
		if err == nil && r.Status == 200 && completed.Add(1) >= killAt {
			killOnce.Do(func() { _ = victim.cmd.Process.Kill() })
		}
		for attempt := 0; err != nil || r.Status == 503; attempt++ {
			if attempt > 50 {
				t.Errorf("bet %s never completed", name)
				return r
			}
			time.Sleep(20 * time.Millisecond)
			r, err = doOnce(survivors[attempt%len(survivors)])
			if err == nil && r.Status == 200 && r.Body["idempotentReplay"] == true {
				replays.Add(1)
			}
		}
		return r
	})
	for _, r := range results {
		if r.Status != 200 || r.str("status") != "PROCESSED" {
			t.Fatalf("bet not processed: %d %s", r.Status, r.Raw)
		}
	}
	<-victim.done // o processo realmente caiu
	for wi, w := range ws {
		if bal, ver := walletState(t, w.ID); bal != (100-betsEach)*100 || ver != 1+betsEach {
			t.Fatalf("wallet %d: balance=%d version=%d, want %d / %d", wi, bal, ver, (100-betsEach)*100, 1+betsEach)
		}
		if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != betsEach {
			t.Fatalf("wallet %d: debits = %d, want %d", wi, n, betsEach)
		}
		if n := countRows(t, "wager_transactions", "wallet_id = $1 AND kind = 'BET'", w.ID); n != betsEach {
			t.Fatalf("wallet %d: bet rows = %d, want %d", wi, n, betsEach)
		}
		if n := countRows(t, "outbox_events", "event_type = 'WalletBalanceChanged' AND aggregate_id = $1 AND payload->'data'->>'direction' = 'DEBIT'", uuid.MustParse(w.ID)); n != betsEach {
			t.Fatalf("wallet %d: balance-changed events = %d, want %d", wi, n, betsEach)
		}
		assertLedgerMatchesBalance(t, w.ID)
	}
	t.Logf("victim killed after %d completed requests; %d answers recovered as idempotent replays on surviving processes", completed.Load(), replays.Load())
}

// tryCall é como call, mas devolve Status 0 em erro de conexão em vez de falhar o teste.
func tryCall(baseURL, key, tok string, body any) response {
	raw, _ := jsonMarshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", baseURL+"/wagering/transactions", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Idempotency-Key", key)
	resp, err := httpClient.Do(req)
	if err != nil {
		return response{}
	}
	defer resp.Body.Close()
	return readResponse(resp)
}
