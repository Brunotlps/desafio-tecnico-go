//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
)

type endpoint struct {
	method, path string
	body         any
	headers      map[string]string
}

func protectedEndpoints(w wallet) []endpoint {
	idem := map[string]string{"Idempotency-Key": "auth-test-" + uuid.NewString()}
	return []endpoint{
		{"POST", "/wallets", map[string]any{"playerId": uuid.NewString(), "initialBalance": money("5.00")}, nil},
		{"GET", "/wallets/" + w.ID, nil, nil},
		{"GET", "/wallets/" + w.ID + "/ledger", nil, nil},
		{"POST", "/wallets/" + w.ID + "/reconciliation", nil, nil},
		{"POST", "/wagering/transactions", betBody(w, "provider-a", "auth-"+uuid.NewString(), "5.00"), idem},
		{"GET", "/wagering/transactions/" + uuid.NewString(), nil, nil},
		{"GET", "/providers/provider-a/wagering/transactions/x", nil, nil},
	}
}

func rowCounts(t testing.TB) (wallets, txs, entries, events int64) {
	return queryInt(t, `SELECT count(*) FROM wallets`), queryInt(t, `SELECT count(*) FROM wager_transactions`),
		queryInt(t, `SELECT count(*) FROM wallet_ledger_entries`), queryInt(t, `SELECT count(*) FROM outbox_events`)
}

func TestAuthenticationRejectsMissingInvalidAndExpiredCredentials(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "50.00")
	endpoints := protectedEndpoints(w)

	// Token curto emitido pelo Keycloak: vence em 3s (+1s de tolerância).
	short := fetchToken(t, "provider-a-shortlived")
	if r := call(t, a.URL, "GET", "/providers/provider-a/wagering/transactions/none", short, nil, nil); r.Status != http.StatusNotFound {
		t.Fatalf("fresh short-lived token should authenticate (404 expected), got %d %s", r.Status, r.Raw)
	}

	wallets0, txs0, entries0, events0 := rowCounts(t)

	creds := map[string]string{"missing": ""}
	for name, tok := range forgedTokens(t) {
		creds[name] = tok
	}
	creds["wrong audience"] = fetchToken(t, "wrong-audience-client")
	time.Sleep(5 * time.Second)
	creds["expired"] = short

	for name, tok := range creds {
		for _, ep := range endpoints {
			var headers = map[string]string{}
			for k, v := range ep.headers {
				headers[k] = v
			}
			if name == "basic scheme" {
				headers["Authorization"] = "Basic abc"
			}
			r := call(t, a.URL, ep.method, ep.path, tok, headers, ep.body)
			wantCode := "INVALID_TOKEN"
			if name == "missing" {
				wantCode = "UNAUTHENTICATED"
			}
			if r.Status != http.StatusUnauthorized || r.str("code") != wantCode {
				t.Errorf("%s -> %s %s: %d %s", name, ep.method, ep.path, r.Status, r.Raw)
			}
		}
	}
	// Esquema diferente de Bearer.
	r := call(t, a.URL, "GET", "/wallets/"+w.ID, "", map[string]string{"Authorization": "Basic YWRtaW46YWRtaW4="}, nil)
	if r.Status != 401 {
		t.Errorf("basic scheme: %d", r.Status)
	}

	// Nenhum efeito financeiro e nenhum dado exposto.
	wallets1, txs1, entries1, events1 := rowCounts(t)
	if wallets0 != wallets1 || txs0 != txs1 || entries0 != entries1 || events0 != events1 {
		t.Fatalf("unauthenticated requests changed state: wallets %d->%d tx %d->%d ledger %d->%d events %d->%d",
			wallets0, wallets1, txs0, txs1, entries0, entries1, events0, events1)
	}
	if bal, ver := walletState(t, w.ID); bal != 50_00 || ver != 1 {
		t.Fatalf("wallet changed: %d v%d", bal, ver)
	}
}

func TestAuthorizationAndProviderIsolation(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")

	ra := submit(t, a, "provider-a", id("ka"), betBody(w, "provider-a", id("ta"), "10.00"))
	if ra.Status != 200 {
		t.Fatalf("provider-a bet: %d %s", ra.Status, ra.Raw)
	}
	txA := ra.str("transactionId")
	_, txs0, entries0, events0 := func() (int64, int64, int64, int64) { return rowCounts(t) }()

	expect := func(name string, r response, status int, code string) {
		t.Helper()
		if r.Status != status || (code != "" && r.str("code") != code) {
			t.Errorf("%s: %d %s, want %d %s", name, r.Status, r.Raw, status, code)
		}
	}
	tokA, tokB, tokInt, tokNone := token(t, "provider-a"), token(t, "provider-b"), token(t, "internal-service"), token(t, "no-roles-client")

	// Identidade autenticada, mas sem papel: 403 em tudo.
	for _, ep := range protectedEndpoints(w) {
		r := call(t, a.URL, ep.method, ep.path, tokNone, ep.headers, ep.body)
		expect("no roles "+ep.method+" "+ep.path, r, 403, "FORBIDDEN")
	}

	// Operações de carteira são internas: provedor recebe 403.
	expect("provider opens wallet", call(t, a.URL, "POST", "/wallets", tokA, nil, map[string]any{"playerId": uuid.NewString(), "initialBalance": money("1.00")}), 403, "FORBIDDEN")
	expect("provider reads wallet", call(t, a.URL, "GET", "/wallets/"+w.ID, tokA, nil, nil), 403, "FORBIDDEN")
	expect("provider reads ledger", call(t, a.URL, "GET", "/wallets/"+w.ID+"/ledger", tokA, nil, nil), 403, "FORBIDDEN")
	expect("provider reconciles", call(t, a.URL, "POST", "/wallets/"+w.ID+"/reconciliation", tokA, nil, nil), 403, "FORBIDDEN")
	// O serviço interno não é um provedor: não envia operações.
	expect("internal submits", submit(t, a, "internal-service", id("ki"), betBody(w, "provider-a", id("ti"), "1.00")), 403, "FORBIDDEN")
	// Pontos ainda não implementados respondem 501 apenas depois de autenticar/autorizar.
	expect("ledger (not implemented)", call(t, a.URL, "GET", "/wallets/"+w.ID+"/ledger", tokInt, nil, nil), 501, "NOT_IMPLEMENTED")

	// Provedor B não age em nome do A: o providerId do corpo precisa coincidir com o token.
	expect("B sends as A", submit(t, a, "provider-b", id("ka2"), betBody(w, "provider-a", id("ta2"), "10.00")), 403, "FORBIDDEN")
	// Nem reaproveita a chave e o conteúdo do A para obter seu resultado.
	expect("B replays A's request", submit(t, a, "provider-b", id("ka"), betBody(w, "provider-a", id("ta"), "10.00")), 403, "FORBIDDEN")
	// Header de identidade arbitrário não vale nada.
	spoof := call(t, a.URL, "POST", "/wagering/transactions", tokB, map[string]string{
		"Idempotency-Key": id("ka"), "X-Provider-Id": "provider-a", "X-Forwarded-User": "provider-a"}, betBody(w, "provider-a", id("ta"), "10.00"))
	expect("spoofed identity headers", spoof, 403, "FORBIDDEN")

	// Consultas: B não vê transações do A, por id nem por id externo.
	expect("B reads A by id", call(t, a.URL, "GET", "/wagering/transactions/"+txA, tokB, nil, nil), 404, "TRANSACTION_NOT_FOUND")
	expect("B reads A by external id", call(t, a.URL, "GET", "/providers/provider-a/wagering/transactions/"+id("ta"), tokB, nil, nil), 403, "FORBIDDEN")
	expect("B reads own namespace for A's external id", call(t, a.URL, "GET", "/providers/provider-b/wagering/transactions/"+id("ta"), tokB, nil, nil), 404, "TRANSACTION_NOT_FOUND")
	// OPENING (interna) não é visível a provedores.
	var openingID string
	if err := testPool.QueryRow(t.Context(), `SELECT id::text FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING'`, w.ID).Scan(&openingID); err != nil {
		t.Fatal(err)
	}
	expect("provider reads OPENING", call(t, a.URL, "GET", "/wagering/transactions/"+openingID, tokA, nil, nil), 404, "TRANSACTION_NOT_FOUND")

	// Tudo isso sem efeito financeiro.
	_, txs1, entries1, events1 := rowCounts(t)
	if txs0 != txs1 || entries0 != entries1 || events0 != events1 {
		t.Fatalf("unauthorized requests changed state: tx %d->%d ledger %d->%d events %d->%d", txs0, txs1, entries0, entries1, events0, events1)
	}

	// Acessos legítimos: dono vê a sua; o serviço interno vê todas; replay do dono funciona.
	expect("A reads own by id", call(t, a.URL, "GET", "/wagering/transactions/"+txA, tokA, nil, nil), 200, "")
	expect("internal reads A by id", call(t, a.URL, "GET", "/wagering/transactions/"+txA, tokInt, nil, nil), 200, "")
	expect("internal reads OPENING", call(t, a.URL, "GET", "/wagering/transactions/"+openingID, tokInt, nil, nil), 200, "")
	expect("internal reads by external id", call(t, a.URL, "GET", "/providers/provider-a/wagering/transactions/"+id("ta"), tokInt, nil, nil), 200, "")
	replay := submit(t, a, "provider-a", id("ka"), betBody(w, "provider-a", id("ta"), "10.00"))
	if replay.Status != 200 || replay.Body["idempotentReplay"] != true {
		t.Fatalf("owner replay: %d %s", replay.Status, replay.Raw)
	}

	// Rotas públicas.
	expect("live", call(t, a.URL, "GET", "/health/live", "", nil, nil), 200, "")
	ready := call(t, a.URL, "GET", "/health/ready", "", nil, nil)
	if ready.Status != 200 || ready.str("checks", "postgres") != "ok" || ready.str("checks", "sqs") != "ok" {
		t.Errorf("ready: %d %s", ready.Status, ready.Raw)
	}
}
