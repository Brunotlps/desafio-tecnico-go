//go:build integration

package integration

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func TestOpenWalletPositive(t *testing.T) {
	a := startApp(t, testConfig())
	player := uuid.NewString()
	r := call(t, a.URL, "POST", "/wallets", token(t, "internal-service"), nil, map[string]any{
		"playerId": player, "initialBalance": money("1000.00"),
	})
	if r.Status != http.StatusCreated || r.str("balance", "amount") != "1000.00" || r.str("playerId") != player {
		t.Fatalf("open: %d %s", r.Status, r.Raw)
	}
	if v, _ := r.Body["version"].(float64); v != 1 {
		t.Fatalf("version = %v", r.Body["version"])
	}
	id := r.str("id")

	// OPENING PROCESSED, crédito no ledger e os dois eventos, no mesmo commit.
	if n := countRows(t, "wager_transactions", "wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED' AND origin = 'INTERNAL' AND provider_id IS NULL", id); n != 1 {
		t.Fatalf("opening transactions = %d", n)
	}
	var dir string
	var amount, before, after int64
	if err := testPool.QueryRow(t.Context(), `SELECT direction, amount, balance_before, balance_after FROM wallet_ledger_entries WHERE wallet_id = $1`, id).
		Scan(&dir, &amount, &before, &after); err != nil {
		t.Fatal(err)
	}
	if dir != "CREDIT" || amount != 100000 || before != 0 || after != 100000 {
		t.Fatalf("ledger entry %s %d %d %d", dir, amount, before, after)
	}
	types := map[string]int64{}
	rows, err := testPool.Query(t.Context(), `SELECT event_type, count(*) FROM outbox_events
		WHERE (aggregate_type = 'Wallet' AND aggregate_id = $1)
		   OR (aggregate_type = 'WagerTransaction' AND aggregate_id IN (SELECT id FROM wager_transactions WHERE wallet_id = $1))
		GROUP BY 1`, id)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ty string
		var n int64
		_ = rows.Scan(&ty, &n)
		types[ty] = n
	}
	rows.Close()
	if types["WagerTransactionProcessed"] != 1 || types["WalletBalanceChanged"] != 1 || len(types) != 2 {
		t.Fatalf("outbox events = %v", types)
	}

	// Payload de WalletBalanceChanged completo e com valores em string decimal.
	var payload []byte
	if err := testPool.QueryRow(t.Context(), `SELECT payload FROM outbox_events WHERE event_type = 'WalletBalanceChanged' AND aggregate_id = $1`, id).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	var ev struct {
		EventID, EventType, AggregateID, CorrelationID, OccurredAt string
		Version                                                    int
		Data                                                       struct {
			WalletID, TransactionID, Direction string
			Money, BalanceBefore, BalanceAfter map[string]string
			WalletVersion                      int
		}
	}
	if err := json.Unmarshal(payload, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.EventID == "" || ev.CorrelationID == "" || ev.Version != 1 || ev.AggregateID != id || ev.Data.WalletID != id ||
		ev.Data.Direction != "CREDIT" || ev.Data.Money["amount"] != "1000.00" || ev.Data.BalanceBefore["amount"] != "0.00" ||
		ev.Data.BalanceAfter["amount"] != "1000.00" || ev.Data.WalletVersion != 1 {
		t.Fatalf("balance changed payload: %s", payload)
	}

	got := call(t, a.URL, "GET", "/wallets/"+id, token(t, "internal-service"), nil, nil)
	if got.Status != 200 || got.str("balance", "amount") != "1000.00" {
		t.Fatalf("get wallet: %d %s", got.Status, got.Raw)
	}
	assertLedgerMatchesBalance(t, id)
}

func TestOpenWalletZeroCreatesOnlyTheWallet(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "0.00")
	if n := countRows(t, "wager_transactions", "wallet_id = $1", w.ID); n != 0 {
		t.Errorf("transactions = %d", n)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1", w.ID); n != 0 {
		t.Errorf("ledger entries = %d", n)
	}
	if n := countRows(t, "outbox_events", "aggregate_id = $1", uuid.MustParse(w.ID)); n != 0 {
		t.Errorf("events = %d", n)
	}
	bal, ver := walletState(t, w.ID)
	if bal != 0 || ver != 1 {
		t.Errorf("wallet balance=%d version=%d", bal, ver)
	}
}

func TestOpenWalletDuplicateIsConflict(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "10.00")
	r := call(t, a.URL, "POST", "/wallets", token(t, "internal-service"), nil, map[string]any{
		"playerId": w.PlayerID, "initialBalance": money("999.00"),
	})
	if r.Status != http.StatusConflict || r.str("code") != "WALLET_ALREADY_EXISTS" || r.str("walletId") != w.ID {
		t.Fatalf("duplicate: %d %s", r.Status, r.Raw)
	}
	// Nada mudou: um único OPENING, saldo original.
	if n := countRows(t, "wager_transactions", "wallet_id = $1", w.ID); n != 1 {
		t.Errorf("transactions = %d", n)
	}
	if bal, _ := walletState(t, w.ID); bal != 1000 {
		t.Errorf("balance = %d", bal)
	}
	// Mesma moeda é o que identifica; outra moeda é outra carteira.
	other := call(t, a.URL, "POST", "/wallets", token(t, "internal-service"), nil, map[string]any{
		"playerId": w.PlayerID, "initialBalance": map[string]string{"amount": "1.00", "currency": "USD"},
	})
	if other.Status != http.StatusCreated {
		t.Fatalf("second currency: %d %s", other.Status, other.Raw)
	}
}

func TestMoneyInputsAreStrictOverHTTP(t *testing.T) {
	a := startApp(t, testConfig())
	tok := token(t, "internal-service")
	cases := map[string]string{
		"json number":     `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":10.5,"currency":"BRL"}}`,
		"negative":        `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"-1.00","currency":"BRL"}}`,
		"excess scale":    `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.001","currency":"BRL"}}`,
		"scientific":      `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1e3","currency":"BRL"}}`,
		"NaN":             `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"NaN","currency":"BRL"}}`,
		"empty":           `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"","currency":"BRL"}}`,
		"overflow":        `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"92233720368547758.08","currency":"BRL"}}`,
		"lowercase cur":   `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"brl"}}`,
		"missing balance": `{"playerId":"` + uuid.NewString() + `"}`,
		"unknown field":   `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"BRL"},"x":1}`,
		"bad player":      `{"playerId":"nope","initialBalance":{"amount":"1.00","currency":"BRL"}}`,
		"trailing data":   `{"playerId":"` + uuid.NewString() + `","initialBalance":{"amount":"1.00","currency":"BRL"}} {}`,
		"not json":        `hello`,
	}
	for name, body := range cases {
		r := call(t, a.URL, "POST", "/wallets", tok, nil, body)
		if r.Status != http.StatusBadRequest {
			t.Errorf("%s: status %d body %s", name, r.Status, r.Raw)
		}
	}
	// Moeda sintaticamente válida, mas fora da política: 422.
	r := call(t, a.URL, "POST", "/wallets", tok, nil, map[string]any{
		"playerId": uuid.NewString(), "initialBalance": map[string]string{"amount": "1.00", "currency": "EUR"}})
	if r.Status != http.StatusUnprocessableEntity || r.str("code") != "CURRENCY_NOT_SUPPORTED" {
		t.Errorf("unsupported currency: %d %s", r.Status, r.Raw)
	}
}

func TestBetAcceptedAndRejected(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")

	ok := submit(t, a, "provider-a", "k-ok", betBody(w, "provider-a", "tx-ok", "80.00"))
	if ok.Status != 200 || ok.str("status") != "PROCESSED" || ok.str("balance", "amount") != "20.00" || ok.Body["idempotentReplay"] != false {
		t.Fatalf("bet: %d %s", ok.Status, ok.Raw)
	}
	rej := submit(t, a, "provider-a", "k-rej", betBody(w, "provider-a", "tx-rej", "80.00"))
	if rej.Status != http.StatusUnprocessableEntity || rej.str("status") != "REJECTED" || rej.str("failureCode") != "INSUFFICIENT_FUNDS" ||
		rej.str("balance", "amount") != "20.00" {
		t.Fatalf("rejected bet: %d %s", rej.Status, rej.Raw)
	}

	// Rejeição: auditável, sem movimentação e sem versão nova.
	bal, ver := walletState(t, w.ID)
	if bal != 2000 || ver != 2 {
		t.Fatalf("wallet balance=%d version=%d", bal, ver)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if n := countRows(t, "wager_transactions", "id = $1 AND status = 'REJECTED' AND failure_code = 'INSUFFICIENT_FUNDS'", uuid.MustParse(rej.str("transactionId"))); n != 1 {
		t.Fatalf("rejected row missing")
	}
	if n := countRows(t, "outbox_events", "event_type = 'WagerTransactionRejected' AND aggregate_id = $1", uuid.MustParse(rej.str("transactionId"))); n != 1 {
		t.Fatalf("rejected event missing")
	}
	if n := countRows(t, "outbox_events", "event_type = 'WalletBalanceChanged' AND payload->'data'->>'transactionId' = $1", rej.str("transactionId")); n != 0 {
		t.Fatalf("rejection emitted a balance change")
	}
	// Eventos da aposta aceita: Processed + BalanceChanged com versão 2.
	if n := countRows(t, "outbox_events", "event_type = 'WalletBalanceChanged' AND payload->'data'->>'transactionId' = $1 AND (payload->'data'->>'walletVersion')::int = 2 AND payload->'data'->>'direction' = 'DEBIT'", ok.str("transactionId")); n != 1 {
		t.Fatalf("balance changed event for accepted bet missing")
	}
	assertLedgerMatchesBalance(t, w.ID)
}

func TestUnsupportedAndForbiddenKinds(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")
	before := countRows(t, "wager_transactions", "wallet_id = $1", w.ID)

	// WIN e LOSS sem referência são processados (win_loss_test.go); WIN com referência,
	// REFUND e ROLLBACK ainda não, e a referência recebida nunca é ignorada.
	for _, kind := range []string{"WIN", "REFUND", "ROLLBACK"} {
		body := betBody(w, "provider-a", uniqueID("tx"), "10.00")
		body["kind"] = kind
		body["referenceExternalTransactionId"] = "some-bet"
		r := submit(t, a, "provider-a", uniqueID("k"), body)
		if r.Status != http.StatusNotImplemented || r.str("code") != "KIND_NOT_SUPPORTED" {
			t.Errorf("%s: %d %s", kind, r.Status, r.Raw)
		}
	}
	// OPENING nunca é aceito por HTTP.
	body := betBody(w, "provider-a", uniqueID("tx"), "10.00")
	body["kind"] = "OPENING"
	if r := submit(t, a, "provider-a", uniqueID("k"), body); r.Status != http.StatusUnprocessableEntity || r.str("code") != "KIND_NOT_ALLOWED" {
		t.Errorf("OPENING: %d %s", r.Status, r.Raw)
	}
	body["kind"] = "TRANSFER"
	if r := submit(t, a, "provider-a", uniqueID("k"), body); r.Status != http.StatusBadRequest || r.str("code") != "INVALID_KIND" {
		t.Errorf("TRANSFER: %d %s", r.Status, r.Raw)
	}
	if after := countRows(t, "wager_transactions", "wallet_id = $1", w.ID); after != before {
		t.Fatalf("unsupported kinds persisted %d rows", after-before)
	}
	if bal, _ := walletState(t, w.ID); bal != 10000 {
		t.Fatalf("balance changed to %d", bal)
	}
}

func TestSubmitInputValidation(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")
	other := wallet{ID: w.ID, PlayerID: uuid.NewString()}

	type tc struct {
		name   string
		key    string
		mutate func(map[string]any)
		wantSt int
		code   string
	}
	cases := []tc{
		{"zero bet", "k", func(b map[string]any) { b["money"] = money("0.00") }, 400, "AMOUNT_NOT_ALLOWED_FOR_KIND"},
		{"negative", "k", func(b map[string]any) { b["money"] = money("-5.00") }, 400, "INVALID_AMOUNT"},
		{"scale", "k", func(b map[string]any) { b["money"] = money("5.001") }, 400, "INVALID_AMOUNT"},
		{"number amount", "k", func(b map[string]any) { b["money"] = map[string]any{"amount": 5.0, "currency": "BRL"} }, 400, "INVALID_JSON"},
		{"missing money", "k", func(b map[string]any) { delete(b, "money") }, 400, "INVALID_REQUEST"},
		{"missing round", "k", func(b map[string]any) { b["roundId"] = "" }, 400, "INVALID_REQUEST"},
		{"bad wallet id", "k", func(b map[string]any) { b["walletId"] = "x" }, 400, "INVALID_REQUEST"},
		{"unknown wallet", "k", func(b map[string]any) { b["walletId"] = uuid.NewString() }, 404, "WALLET_NOT_FOUND"},
		{"wrong currency", "k", func(b map[string]any) { b["money"] = map[string]string{"amount": "5.00", "currency": "USD"} }, 422, "WALLET_MISMATCH"},
		{"other player", "k", func(b map[string]any) { b["playerId"] = other.PlayerID }, 422, "WALLET_MISMATCH"},
		{"reference on bet", "k", func(b map[string]any) { b["referenceExternalTransactionId"] = "x" }, 400, "INVALID_REQUEST"},
		{"missing key", "", nil, 400, "IDEMPOTENCY_KEY_REQUIRED"},
	}
	before := countRows(t, "wager_transactions", "wallet_id = $1", w.ID)
	for _, c := range cases {
		body := betBody(w, "provider-a", uniqueID("tx"), "5.00")
		if c.mutate != nil {
			c.mutate(body)
		}
		headers := map[string]string{}
		if c.key != "" {
			headers["Idempotency-Key"] = uniqueID(c.key)
		}
		r := call(t, a.URL, "POST", "/wagering/transactions", token(t, "provider-a"), headers, body)
		if r.Status != c.wantSt || r.str("code") != c.code {
			t.Errorf("%s: got %d %s, want %d %s", c.name, r.Status, r.Raw, c.wantSt, c.code)
		}
	}
	if after := countRows(t, "wager_transactions", "wallet_id = $1", w.ID); after != before {
		t.Fatalf("invalid input persisted %d rows", after-before)
	}
}

// O fluxo financeiro completo funciona com o papel restrito, inclusive o gatilho da
// cadeia (que trava a linha da carteira) e a coluna de versão.
func TestFinancialFlowWorksWithRestrictedRole(t *testing.T) {
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")
	r := submit(t, a, "provider-a", uniqueID("k"), betBody(w, "provider-a", uniqueID("t"), "30.00"))
	if r.Status != 200 || r.str("balance", "amount") != "70.00" {
		t.Fatalf("bet with restricted role: %d %s", r.Status, r.Raw)
	}
	assertLedgerMatchesBalance(t, w.ID)
}
