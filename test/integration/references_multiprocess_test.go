//go:build integration

package integration

import (
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Três processos reais (conexões e memória próprias), cada um com o seu executor de referências.

var workerEnv = []string{
	"REFERENCE_WORKERS=1", "REFERENCE_POLL_INTERVAL=30ms", "REFERENCE_BACKOFF_INITIAL=20ms", "REFERENCE_BACKOFF_MAX=100ms",
	"REFERENCE_TTL=1m", "SHUTDOWN_DRAIN_TIMEOUT=5s",
}

func statusOf(t *testing.T, p *proc, txID string) response {
	t.Helper()
	return call(t, p.URL, "GET", "/wagering/transactions/"+txID, token(t, "internal-service"), nil, nil)
}

// Em muitas carteiras, REFUND e ROLLBACK concorrentes do mesmo alvo chegam antes dele, em processos
// diferentes; a BET chega depois. Os executores dos três processos disputam as pendências: por carteira,
// exatamente uma reversão processada, uma rejeitada, um único crédito.
func TestThreeProcessesResolvePendingReversalsExactlyOnce(t *testing.T) {
	procs := startClusterEnv(t, 3, workerEnv...)
	id := scoped()
	pick := func(i int) *proc { return procs[i%3] }
	const wallets = 24

	type pair struct {
		w            wallet
		refund, roll string
	}
	ws := make([]pair, wallets)
	for i := range ws {
		ws[i].w = openWallet(t, pick(i).app(), "100.00")
	}
	// 1. Aceite das pendências (e reenvios paralelos delas, espalhados pelos processos).
	pend := parallel(wallets*2*3, func(n int) response {
		i, which, rep := n/6, (n/3)%2, n%3
		kind, ext := "REFUND", id(fmt.Sprintf("refund-%d", i))
		if which == 1 {
			kind, ext = "ROLLBACK", id(fmt.Sprintf("roll-%d", i))
		}
		return submit(t, pick(i+which+rep).app(), "provider-a", ext, refBody(ws[i].w, "provider-a", ext, kind, "25.00", id(fmt.Sprintf("bet-%d", i))))
	})
	txIDs := map[string]string{}
	for n, r := range pend {
		if r.Status != 202 || r.str("status") != "PENDING_REFERENCE" {
			t.Fatalf("pending #%d: %d %s", n, r.Status, r.Raw)
		}
		i, which := n/6, (n/3)%2
		key := fmt.Sprintf("%d-%d", i, which)
		if prev, ok := txIDs[key]; ok && prev != r.str("transactionId") {
			t.Fatalf("a resend created another transaction: %s vs %s", prev, r.str("transactionId"))
		}
		txIDs[key] = r.str("transactionId")
	}
	for key, txID := range txIDs {
		if p := pendingOf(t, uuid.MustParse(txID)); p.Rows != 1 {
			t.Fatalf("%s: schedule rows = %d", key, p.Rows)
		}
	}
	// 2. As BETs chegam (em processos diferentes dos que aceitaram as pendências).
	bets := parallel(wallets, func(i int) response {
		return submit(t, pick(i+1).app(), "provider-a", id(fmt.Sprintf("kb-%d", i)), betBody(ws[i].w, "provider-a", id(fmt.Sprintf("bet-%d", i)), "25.00"))
	})
	for i, r := range bets {
		if r.Status != 200 {
			t.Fatalf("bet %d: %d %s", i, r.Status, r.Raw)
		}
	}
	// 3. Convergência pelo estado durável.
	eventually(t, "every pending operation to reach a terminal state", 30*time.Second, func() bool {
		for _, txID := range txIDs {
			if s := statusOf(t, procs[0], txID).str("status"); s != "PROCESSED" && s != "REJECTED" {
				return false
			}
		}
		return true
	})
	for i := range ws {
		var processed, rejected int
		for which := 0; which < 2; which++ {
			txID := uuid.MustParse(txIDs[fmt.Sprintf("%d-%d", i, which)])
			r := statusOf(t, procs[0], txID.String())
			switch r.str("status") {
			case "PROCESSED":
				processed++
				if n, _ := ledgerOf(t, txID); n != 1 {
					t.Fatalf("wallet %d: processed reversal with %d ledger entries", i, n)
				}
			case "REJECTED":
				rejected++
				if r.str("failureCode") != "ALREADY_REVERSED" {
					t.Fatalf("wallet %d: rejected with %s", i, r.str("failureCode"))
				}
				if n, _ := ledgerOf(t, txID); n != 0 {
					t.Fatalf("wallet %d: rejected reversal with a ledger entry", i)
				}
			}
			if eventCount(t, txID, "WagerTransactionPendingReference") != 1 {
				t.Fatalf("wallet %d: PendingReference events != 1", i)
			}
			if p := pendingOf(t, txID); !p.Completed {
				t.Fatalf("wallet %d: schedule still open", i)
			}
		}
		if processed != 1 || rejected != 1 {
			t.Fatalf("wallet %d: processed=%d rejected=%d", i, processed, rejected)
		}
		if bal, _ := walletState(t, ws[i].w.ID); bal != 100_00 {
			t.Fatalf("wallet %d: balance %d", i, bal)
		}
		assertLedgerMatchesBalance(t, ws[i].w.ID)
	}
	for _, p := range procs {
		if n := connectionsOf(t, p.appName); n == 0 {
			t.Errorf("%s has no database connections of its own", p.name)
		}
	}
}

// SIGKILL do processo que aceitou a pendência: nada depende de hook; outro processo a conclui.
func TestKillingTheProcessThatAcceptedAPendingOperationDoesNotLoseIt(t *testing.T) {
	procs := startClusterEnv(t, 3, workerEnv...)
	id := scoped()
	w := openWallet(t, procs[0].app(), "100.00")

	r := submit(t, procs[0].app(), "provider-a", id("k"), refBody(w, "provider-a", id("refund"), "REFUND", "25.00", id("bet")))
	if r.Status != 202 {
		t.Fatalf("pending: %d %s", r.Status, r.Raw)
	}
	txID := r.str("transactionId")
	_ = procs[0].cmd.Process.Signal(syscall.SIGKILL)
	<-procs[0].done

	if bet := submit(t, procs[1].app(), "provider-a", id("kb"), betBody(w, "provider-a", id("bet"), "25.00")); bet.Status != 200 {
		t.Fatalf("bet: %d %s", bet.Status, bet.Raw)
	}
	eventually(t, "a surviving process to resolve the pending refund", 30*time.Second, func() bool {
		return statusOf(t, procs[2], txID).str("status") == "PROCESSED"
	})
	if got := statusOf(t, procs[1], txID); got.str("transactionId") != txID || got.str("balance", "amount") != "100.00" {
		t.Fatalf("resolved: %s", got.Raw)
	}
	if n, _ := ledgerOf(t, uuid.MustParse(txID)); n != 1 {
		t.Fatalf("ledger entries = %d", n)
	}
	if bal, _ := walletState(t, w.ID); bal != 100_00 {
		t.Fatalf("balance %d", bal)
	}
	assertLedgerMatchesBalance(t, w.ID)
	// O replay, em qualquer processo vivo, devolve o resultado original.
	if rp := submit(t, procs[2].app(), "provider-a", id("k"), refBody(w, "provider-a", id("refund"), "REFUND", "25.00", id("bet"))); rp.Status != 200 || rp.Body["idempotentReplay"] != true {
		t.Fatalf("replay: %d %s", rp.Status, rp.Raw)
	}
}

// Expiração com três executores: uma única rejeição e um único evento por operação.
func TestThreeProcessesExpireAnAbsentReferenceOnce(t *testing.T) {
	env := append(append([]string{}, workerEnv...), "REFERENCE_TTL=1s")
	procs := startClusterEnv(t, 3, env...)
	id := scoped()
	const n = 12
	w := openWallet(t, procs[0].app(), "100.00")
	txIDs := make([]string, n)
	for i := 0; i < n; i++ {
		ext := id(fmt.Sprintf("refund-%d", i))
		r := submit(t, procs[i%3].app(), "provider-a", ext, refBody(w, "provider-a", ext, "REFUND", "5.00", id(fmt.Sprintf("never-%d", i))))
		if r.Status != 202 {
			t.Fatalf("pending %d: %d %s", i, r.Status, r.Raw)
		}
		txIDs[i] = r.str("transactionId")
	}
	eventually(t, "all pending operations to expire", 30*time.Second, func() bool {
		for _, txID := range txIDs {
			if statusOf(t, procs[1], txID).str("status") != "REJECTED" {
				return false
			}
		}
		return true
	})
	for _, txID := range txIDs {
		u := uuid.MustParse(txID)
		if got := statusOf(t, procs[2], txID); got.str("failureCode") != "REFERENCE_NOT_FOUND" {
			t.Fatalf("expired with %s", got.Raw)
		}
		if eventCount(t, u, "WagerTransactionRejected") != 1 || eventCount(t, u, "WagerTransactionPendingReference") != 1 {
			t.Fatalf("events of %s", txID)
		}
		if p := pendingOf(t, u); !p.Completed || p.Rows != 1 {
			t.Fatalf("schedule of %s: %+v", txID, p)
		}
	}
	if bal, ver := walletState(t, w.ID); bal != 100_00 || ver != 1 {
		t.Fatalf("wallet changed: %d/%d", bal, ver)
	}
}
