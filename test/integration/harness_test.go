//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/fx"

	"desafio-tecnico-go/internal/bootstrap"
	"desafio-tecnico-go/internal/config"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/httpapi"
)

// --- aplicação sob teste --------------------------------------------------------

func testConfig(mods ...func(*config.Config)) config.Config {
	cfg := config.Config{
		HTTPAddr: "127.0.0.1:0", LogLevel: "warn", RequestTimeout: 10 * time.Second, ShutdownTimeout: 10 * time.Second,
		DatabaseURL: env.runtimeURL, DBMaxConns: 20,
		OIDCIssuer: env.issuer, OIDCJWKSURL: env.jwksURL, OIDCAudience: "wager-api", OIDCClockSkew: time.Second,
		AWSRegion: "us-east-1", AWSEndpointURL: env.awsEndpoint, SQSWagerQueue: "wager-transactions.fifo",
		AllowedCurrencies: []domain.Currency{"BRL", "USD"},
	}
	for _, m := range mods {
		m(&cfg)
	}
	return cfg
}

type testApp struct {
	URL  string
	app  *fx.App
	srv  *httpapi.Server
	stop sync.Once
}

// startApp compõe e inicia a aplicação completa (Fx) com infraestrutura real.
func startApp(t *testing.T, cfg config.Config) *testApp {
	t.Helper()
	a := &testApp{}
	a.app = fx.New(bootstrap.Options(cfg), fx.Populate(&a.srv), fx.NopLogger)
	if err := a.app.Err(); err != nil {
		t.Fatalf("compose app: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.app.Start(ctx); err != nil {
		t.Fatalf("start app: %v", err)
	}
	a.URL = "http://" + a.srv.Addr()
	t.Cleanup(func() { a.Stop(t) })
	return a
}

// Stop encerra a aplicação (idempotente).
func (a *testApp) Stop(t *testing.T) {
	t.Helper()
	a.stop.Do(func() {
		// Conexões ociosas do cliente de teste atrasariam o Shutdown do servidor (o
		// net/http só trata conexões novas e sem requisição como ociosas após 5s).
		httpClient.CloseIdleConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := a.app.Stop(ctx); err != nil {
			t.Errorf("stop app: %v", err)
		}
	})
}

// --- cliente HTTP -------------------------------------------------------------------

type response struct {
	Status int
	Body   map[string]any
	Raw    string
}

func (r response) str(path ...string) string {
	var cur any = r.Body
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[p]
	}
	s, _ := cur.(string)
	return s
}

func call(t testing.TB, baseURL, method, path, token string, headers map[string]string, body any) response {
	t.Helper()
	var rdr io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		rdr = strings.NewReader(b)
	default:
		raw, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, baseURL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	return readResponse(resp)
}

func readResponse(resp *http.Response) response {
	raw, _ := io.ReadAll(resp.Body)
	out := response{Status: resp.StatusCode, Raw: string(raw)}
	_ = json.Unmarshal(raw, &out.Body)
	return out
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

// --- cenários ---------------------------------------------------------------------------

func money(amount string) map[string]string {
	return map[string]string{"amount": amount, "currency": "BRL"}
}

// openWallet abre uma carteira BRL via HTTP (serviço interno).
func openWallet(t testing.TB, a *testApp, initial string) wallet {
	t.Helper()
	player := uuid.NewString()
	r := call(t, a.URL, "POST", "/wallets", token(t, "internal-service"), nil, map[string]any{
		"playerId": player, "initialBalance": money(initial),
	})
	if r.Status != http.StatusCreated {
		t.Fatalf("open wallet: %d %s", r.Status, r.Raw)
	}
	return wallet{ID: r.str("id"), PlayerID: player}
}

func betBody(w wallet, provider, externalID, amount string) map[string]any {
	return map[string]any{
		"providerId": provider, "externalTransactionId": externalID, "playerId": w.PlayerID, "walletId": w.ID,
		"roundId": "round-1", "gameId": "fortune-chimp", "kind": "BET", "money": money(amount),
	}
}

// submit envia uma operação como o provedor indicado, com a chave dada.
func submit(t testing.TB, a *testApp, client, key string, body map[string]any) response {
	t.Helper()
	return call(t, a.URL, "POST", "/wagering/transactions", token(t, client), map[string]string{"Idempotency-Key": key}, body)
}

func uniqueID(prefix string) string { return prefix + "-" + uuid.NewString() }
