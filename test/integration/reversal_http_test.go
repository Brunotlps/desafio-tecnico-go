//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/config"
)

// Contrato HTTP das reversões e das pendências, com o worker real em execução.

func workerConfig(mods ...func(*config.Config)) config.Config {
	return testConfig(append([]func(*config.Config){func(c *config.Config) { c.ReferenceWorkers = 1 }}, mods...)...)
}

func TestReversalsOverHTTP(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")

	bet := submit(t, a, "provider-a", id("k-bet"), betBody(w, "provider-a", id("bet"), "25.00"))
	if bet.Status != 200 {
		t.Fatalf("bet: %d %s", bet.Status, bet.Raw)
	}
	refund := submit(t, a, "provider-a", id("k-refund"), refBody(w, "provider-a", id("refund"), "REFUND", "25.00", id("bet")))
	if refund.Status != 200 || refund.str("status") != "PROCESSED" || refund.str("balance", "amount") != "100.00" {
		t.Fatalf("refund: %d %s", refund.Status, refund.Raw)
	}
	// Segunda reversão da BET: 422 REJECTED/ALREADY_REVERSED, com o saldo observado e sem ledger.
	again := submit(t, a, "provider-a", id("k-rollback"), refBody(w, "provider-a", id("rollback"), "ROLLBACK", "25.00", id("bet")))
	if again.Status != http.StatusUnprocessableEntity || again.str("status") != "REJECTED" || again.str("failureCode") != "ALREADY_REVERSED" || again.str("balance", "amount") != "100.00" {
		t.Fatalf("second reversal: %d %s", again.Status, again.Raw)
	}
	// Replay da rejeitada: mesmo status, marcado como replay.
	if rr := submit(t, a, "provider-a", id("k-rollback"), refBody(w, "provider-a", id("rollback"), "ROLLBACK", "25.00", id("bet"))); rr.Status != 422 || rr.Body["idempotentReplay"] != true || rr.str("failureCode") != "ALREADY_REVERSED" {
		t.Fatalf("replay of the rejection: %d %s", rr.Status, rr.Raw)
	}
	// Valor diferente do alvo: REFERENCE_MISMATCH (definitivo, auditável).
	mm := submit(t, a, "provider-a", id("k-mm"), refBody(w, "provider-a", id("mm"), "REFUND", "24.00", id("bet")))
	if mm.Status != 422 || mm.str("failureCode") != "REFERENCE_MISMATCH" {
		t.Fatalf("mismatch: %d %s", mm.Status, mm.Raw)
	}
	// Reversão sem saldo: código próprio, distinto de INSUFFICIENT_FUNDS.
	big := submit(t, a, "provider-a", id("k-big"), betBody(w, "provider-a", id("big"), "100.00"))
	if big.Status != 200 {
		t.Fatalf("big: %d %s", big.Status, big.Raw)
	}
	rb := submit(t, a, "provider-a", id("k-rb"), refBody(w, "provider-a", id("rb-refund"), "ROLLBACK", "25.00", id("refund")))
	if rb.Status != 422 || rb.str("failureCode") != "REVERSAL_INSUFFICIENT_FUNDS" {
		t.Fatalf("rollback without funds: %d %s", rb.Status, rb.Raw)
	}
	if bal, _ := walletState(t, w.ID); bal != 0 {
		t.Fatalf("balance %d", bal)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

func TestPendingOperationsOverHTTPWithTheWorkerRunning(t *testing.T) {
	a := startApp(t, workerConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")

	key, ext := id("k-refund"), id("refund")
	body := refBody(w, "provider-a", ext, "REFUND", "25.00", id("bet"))
	pend := submit(t, a, "provider-a", key, body)
	if pend.Status != http.StatusAccepted || pend.str("status") != "PENDING_REFERENCE" || pend.Body["balance"] != nil || pend.Body["idempotentReplay"] != false {
		t.Fatalf("pending: %d %s", pend.Status, pend.Raw)
	}
	txID := pend.str("transactionId")
	// Replay da pendência: 202 com o mesmo id, sem saldo inventado.
	if rp := submit(t, a, "provider-a", key, body); rp.Status != 202 || rp.str("transactionId") != txID || rp.Body["idempotentReplay"] != true || rp.Body["balance"] != nil {
		t.Fatalf("pending replay: %d %s", rp.Status, rp.Raw)
	}
	if n := eventCount(t, uuid.MustParse(txID), "WagerTransactionPendingReference"); n != 1 {
		t.Fatalf("PendingReference events: %d", n)
	}

	// Consultas: o dono vê a pendência sem saldo; outro provedor não a vê; o serviço interno vê.
	get := func(client, path string) response {
		return call(t, a.URL, "GET", path, token(t, client), nil, nil)
	}
	own := get("provider-a", "/wagering/transactions/"+txID)
	if own.Status != 200 || own.str("status") != "PENDING_REFERENCE" || own.Body["balance"] != nil || own.str("referenceExternalTransactionId") != id("bet") {
		t.Fatalf("own query: %d %s", own.Status, own.Raw)
	}
	if other := get("provider-b", "/wagering/transactions/"+txID); other.Status != 404 {
		t.Fatalf("another provider's query: %d %s", other.Status, other.Raw)
	}
	if other := get("provider-b", "/providers/provider-a/wagering/transactions/"+ext); other.Status != 403 {
		t.Fatalf("another provider's path: %d %s", other.Status, other.Raw)
	}
	if internal := get("internal-service", "/wagering/transactions/"+txID); internal.Status != 200 || internal.str("status") != "PENDING_REFERENCE" {
		t.Fatalf("internal query: %d %s", internal.Status, internal.Raw)
	}

	// A BET chega; o worker conclui a MESMA operação.
	if bet := submit(t, a, "provider-a", id("k-bet"), betBody(w, "provider-a", id("bet"), "25.00")); bet.Status != 200 {
		t.Fatalf("bet: %d %s", bet.Status, bet.Raw)
	}
	eventually(t, "the worker to resolve the refund", 10*time.Second, func() bool {
		return get("provider-a", "/wagering/transactions/"+txID).str("status") == "PROCESSED"
	})
	done := get("provider-a", "/wagering/transactions/"+txID)
	if done.str("balance", "amount") != "100.00" || done.str("transactionId") != txID {
		t.Fatalf("resolved: %s", done.Raw)
	}
	if rp := submit(t, a, "provider-a", key, body); rp.Status != 200 || rp.Body["idempotentReplay"] != true || rp.str("balance", "amount") != "100.00" || rp.str("transactionId") != txID {
		t.Fatalf("replay after resolution: %d %s", rp.Status, rp.Raw)
	}
	id2 := uuid.MustParse(txID)
	if eventCount(t, id2, "WagerTransactionProcessed") != 1 || eventCount(t, id2, "WalletBalanceChanged") != 1 || eventCount(t, id2, "WagerTransactionPendingReference") != 1 {
		t.Fatal("events of the resolved operation")
	}
	assertLedgerMatchesBalance(t, w.ID)
}

// Com o TTL curto, o worker real rejeita a pendência cuja referência nunca chega; uma referência do mesmo
// id externo em OUTRO provedor não é vista (nem revelada) e a rejeição não é reaberta por uma chegada tardia.
func TestWorkerExpiresAnAbsentReferenceAndRespectsProviderScope(t *testing.T) {
	a := startApp(t, workerConfig(func(c *config.Config) { c.ReferenceTTL = 600 * time.Millisecond }))
	id := scoped()
	w := openWallet(t, a, "100.00")

	// provider-a tem uma BET com este id externo; provider-b NÃO a pode referenciar.
	if bet := submit(t, a, "provider-a", id("k-bet"), betBody(w, "provider-a", id("shared-ext"), "25.00")); bet.Status != 200 {
		t.Fatalf("bet: %d %s", bet.Status, bet.Raw)
	}
	pend := submit(t, a, "provider-b", id("k-b"), refBody(w, "provider-b", id("refund-b"), "REFUND", "25.00", id("shared-ext")))
	if pend.Status != 202 {
		t.Fatalf("provider-b must not resolve provider-a's transaction: %d %s", pend.Status, pend.Raw)
	}
	txID := pend.str("transactionId")
	eventually(t, "the TTL to expire", 10*time.Second, func() bool {
		return call(t, a.URL, "GET", "/wagering/transactions/"+txID, token(t, "provider-b"), nil, nil).str("status") == "REJECTED"
	})
	got := call(t, a.URL, "GET", "/wagering/transactions/"+txID, token(t, "provider-b"), nil, nil)
	if got.str("failureCode") != "REFERENCE_NOT_FOUND" {
		t.Fatalf("expired: %s", got.Raw)
	}
	if eventCount(t, uuid.MustParse(txID), "WagerTransactionRejected") != 1 {
		t.Fatal("expiry must produce exactly one rejection event")
	}
	// Chegada tardia da referência do provider-b: a rejeição continua terminal.
	late := submit(t, a, "provider-b", id("k-late"), betBody(w, "provider-b", id("shared-ext"), "25.00"))
	if late.Status != 200 {
		t.Fatalf("late bet: %d %s", late.Status, late.Raw)
	}
	rp := submit(t, a, "provider-b", id("k-b"), refBody(w, "provider-b", id("refund-b"), "REFUND", "25.00", id("shared-ext")))
	if rp.Status != 422 || rp.Body["idempotentReplay"] != true || rp.str("failureCode") != "REFERENCE_NOT_FOUND" {
		t.Fatalf("replay of the expired operation: %d %s", rp.Status, rp.Raw)
	}
	if bal, _ := walletState(t, w.ID); bal != 50_00 { // as duas BETs; nenhuma devolução
		t.Fatalf("balance %d", bal)
	}
	assertLedgerMatchesBalance(t, w.ID)
}
