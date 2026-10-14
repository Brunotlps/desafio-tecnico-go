//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

// Reversões (enunciado §7) e a política aprovada: uma reversão bem-sucedida por movimento em
// todo o histórico, ROLLBACK de REFUND não reabre a BET, e rejeições não consomem o direito.

func TestReversalPolicyChainBetRefundRollback(t *testing.T) {
	svc := newServiceWith(t, nil, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	bet := mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "25.00", ""))
	mustStatus(t, bet, domain.StatusProcessed, "")
	if bal, _ := walletState(t, wallet); bal != 75_00 {
		t.Fatalf("after BET: %d", bal)
	}

	refund := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	mustStatus(t, refund, domain.StatusProcessed, "")
	if refund.Transaction.ResultBalance().Amount() != "100.00" {
		t.Fatalf("refund result %s", refund.Transaction.ResultBalance().Amount())
	}
	if ref := refund.Transaction.ReferenceTransactionID(); ref == nil || *ref != bet.Transaction.ID() {
		t.Fatalf("refund reference = %v, want the BET %s", ref, bet.Transaction.ID())
	}
	if n, dir := ledgerOf(t, refund.Transaction.ID()); n != 1 || dir != "CREDIT" {
		t.Fatalf("refund ledger: %d %s", n, dir)
	}
	rid := refund.Transaction.ID()
	if eventCount(t, rid, "WagerTransactionProcessed") != 1 || eventCount(t, rid, "WalletBalanceChanged") != 1 {
		t.Fatal("refund events")
	}

	// ROLLBACK do REFUND: redebita; a BET original não ganha nova reversão.
	rollback := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rollback-refund"), "25.00", id("refund")))
	mustStatus(t, rollback, domain.StatusProcessed, "")
	if rollback.Transaction.ResultBalance().Amount() != "75.00" {
		t.Fatalf("rollback result %s", rollback.Transaction.ResultBalance().Amount())
	}
	if n, dir := ledgerOf(t, rollback.Transaction.ID()); n != 1 || dir != "DEBIT" {
		t.Fatalf("rollback ledger: %d %s", n, dir)
	}

	ledger := countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet)
	for name, raw := range map[string]domain.RawExternalRequest{
		"new REFUND of the BET":      op(wallet, player, "REFUND", id("refund-2"), "25.00", id("bet")),
		"new ROLLBACK of the BET":    op(wallet, player, "ROLLBACK", id("rollback-bet"), "25.00", id("bet")),
		"new ROLLBACK of the REFUND": op(wallet, player, "ROLLBACK", id("rollback-refund-2"), "25.00", id("refund")),
	} {
		res := mustSubmit(t, svc, raw)
		mustStatus(t, res, domain.StatusRejected, domain.FailureAlreadyReversed)
		if res.Transaction.ResultBalance().Amount() != "75.00" {
			t.Errorf("%s: observed balance %s", name, res.Transaction.ResultBalance().Amount())
		}
		if n, _ := ledgerOf(t, res.Transaction.ID()); n != 0 {
			t.Errorf("%s: a rejected reversal created a ledger entry", name)
		}
		if eventCount(t, res.Transaction.ID(), "WagerTransactionRejected") != 1 || eventCount(t, res.Transaction.ID(), "WalletBalanceChanged") != 0 {
			t.Errorf("%s: events", name)
		}
	}
	if bal, ver := walletState(t, wallet); bal != 75_00 || ver != 4 {
		t.Fatalf("balance=%d version=%d, want 7500/4", bal, ver)
	}
	if after := countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet); after != ledger {
		t.Fatalf("rejected reversals added %d ledger entries", after-ledger)
	}

	// Replays (inclusive depois de novas movimentações) devolvem os resultados originais.
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet-later"), "5.00", ""))
	for name, c := range map[string]struct {
		raw     domain.RawExternalRequest
		balance string
		status  domain.Status
	}{
		"refund replay":   {op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")), "100.00", domain.StatusProcessed},
		"rollback replay": {op(wallet, player, "ROLLBACK", id("rollback-refund"), "25.00", id("refund")), "75.00", domain.StatusProcessed},
		"rejected replay": {op(wallet, player, "REFUND", id("refund-2"), "25.00", id("bet")), "75.00", domain.StatusRejected},
	} {
		res := mustSubmit(t, svc, c.raw)
		if !res.Replay || res.Transaction.Status() != c.status || res.Transaction.ResultBalance().Amount() != c.balance {
			t.Errorf("%s: %+v %s", name, res, res.Transaction.ResultBalance().Amount())
		}
	}
	assertLedgerMatchesBalance(t, wallet)
}

func TestRollbackOfBetWinAndWinWithReference(t *testing.T) {
	svc := newServiceWith(t, nil, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	mustSubmit(t, svc, op(wallet, player, "BET", id("bet-a"), "30.00", ""))
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet-b"), "10.00", "")) // 60.00

	// WIN com referência: valor livre (diferente da BET), não consome a unicidade de reversão.
	win := mustSubmit(t, svc, op(wallet, player, "WIN", id("win"), "55.00", id("bet-a")))
	mustStatus(t, win, domain.StatusProcessed, "")
	if win.Transaction.ResultBalance().Amount() != "115.00" || win.Transaction.ReferenceTransactionID() == nil {
		t.Fatalf("win: %s ref=%v", win.Transaction.ResultBalance().Amount(), win.Transaction.ReferenceTransactionID())
	}
	// A BET referenciada por um WIN continua revertível uma vez.
	rb := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rollback-bet-a"), "30.00", id("bet-a")))
	mustStatus(t, rb, domain.StatusProcessed, "")
	if n, dir := ledgerOf(t, rb.Transaction.ID()); n != 1 || dir != "CREDIT" { // ROLLBACK de BET credita
		t.Fatalf("rollback of BET ledger: %d %s", n, dir)
	}
	// ROLLBACK de WIN: débito integral.
	rw := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rollback-win"), "55.00", id("win")))
	mustStatus(t, rw, domain.StatusProcessed, "")
	if n, dir := ledgerOf(t, rw.Transaction.ID()); n != 1 || dir != "DEBIT" {
		t.Fatalf("rollback of WIN ledger: %d %s", n, dir)
	}
	if bal, _ := walletState(t, wallet); bal != 90_00 { // 60 + 55 + 30 - 55
		t.Fatalf("balance %d", bal)
	}
	// Um WIN não consome a reversão da BET-b: o REFUND dela ainda funciona; um segundo WIN também referencia.
	mustSubmit(t, svc, op(wallet, player, "WIN", id("win-2"), "1.00", id("bet-b")))
	mustStatus(t, mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund-b"), "10.00", id("bet-b"))), domain.StatusProcessed, "")
	assertLedgerMatchesBalance(t, wallet)
}

func TestReversalWithoutFundsIsRejectedWithItsOwnCodeAndDoesNotConsumeTheTarget(t *testing.T) {
	svc := newServiceWith(t, nil, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	mustSubmit(t, svc, op(wallet, player, "WIN", id("win"), "50.00", ""))                   // 150
	mustSubmit(t, svc, op(wallet, player, "BET", id("spend"), "100.00", ""))                // 50
	rej := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rb"), "50.00", id("win"))) // 50 - 50 = 0: cabe
	mustStatus(t, rej, domain.StatusProcessed, "")
	// Agora o saldo é zero: outro WIN e a tentativa de reverter sem saldo.
	win2 := mustSubmit(t, svc, op(wallet, player, "WIN", id("win2"), "40.00", ""))
	mustSubmit(t, svc, op(wallet, player, "BET", id("spend2"), "40.00", "")) // 0
	_ = win2
	ledger := countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet)
	res := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rb2"), "40.00", id("win2")))
	mustStatus(t, res, domain.StatusRejected, domain.FailureReversalInsufficientFunds)
	if res.Transaction.FailureCode() == domain.FailureInsufficientFunds {
		t.Fatal("the reversal code must differ from the BET code")
	}
	if n, _ := ledgerOf(t, res.Transaction.ID()); n != 0 || countRows(t, "wallet_ledger_entries", "wallet_id = $1", wallet) != ledger {
		t.Fatal("a rejected reversal wrote to the ledger")
	}
	if bal, _ := walletState(t, wallet); bal != 0 {
		t.Fatalf("balance %d", bal)
	}
	if eventCount(t, res.Transaction.ID(), "WagerTransactionRejected") != 1 {
		t.Fatal("rejection event")
	}
	// A rejeição não consome o alvo: com saldo de volta, uma nova operação (outra identidade) é elegível.
	mustSubmit(t, svc, op(wallet, player, "WIN", id("win3"), "40.00", ""))
	retry := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rb2-retry"), "40.00", id("win2")))
	mustStatus(t, retry, domain.StatusProcessed, "")
	// E o replay da rejeitada não é reavaliado.
	replay := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rb2"), "40.00", id("win2")))
	if !replay.Replay || replay.Transaction.Status() != domain.StatusRejected || replay.Transaction.ID() != res.Transaction.ID() {
		t.Fatalf("replay of the rejection: %+v", replay)
	}
	assertLedgerMatchesBalance(t, wallet)
}

func TestIncompatibleReferencesAreRejectedAndAudited(t *testing.T) {
	svc := newServiceWith(t, nil, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "500.00")
	otherWallet, otherPlayer := svcOpen(t, svc, "500.00")

	bet := mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "25.00", ""))
	win := mustSubmit(t, svc, op(wallet, player, "WIN", id("win"), "25.00", ""))
	loss := mustSubmit(t, svc, op(wallet, player, "LOSS", id("loss"), "0.00", ""))
	mustStatus(t, mustSubmit(t, svc, op(wallet, player, "BET", id("big"), "9999.00", "")), domain.StatusRejected, domain.FailureInsufficientFunds)
	_, _, _ = bet, win, loss

	mismatches := map[string]domain.RawExternalRequest{
		"other amount":       op(wallet, player, "REFUND", id("m1"), "24.99", id("bet")),
		"REFUND of a WIN":    op(wallet, player, "REFUND", id("m2"), "25.00", id("win")),
		"ROLLBACK of a LOSS": op(wallet, player, "ROLLBACK", id("m3"), "25.00", id("loss")),
		"WIN of a WIN":       op(wallet, player, "WIN", id("m4"), "25.00", id("win")),
		"other round": func() domain.RawExternalRequest {
			r := op(wallet, player, "REFUND", id("m6"), "25.00", id("bet"))
			r.RoundID = "other"
			return r
		}(),
		"reference in another wallet": op(otherWallet, otherPlayer, "REFUND", id("m7"), "25.00", id("bet")),
	}
	before := countRows(t, "wallet_ledger_entries", "wallet_id IN ($1, $2)", uuid.MustParse(wallet), uuid.MustParse(otherWallet))
	for name, raw := range mismatches {
		res := mustSubmit(t, svc, raw)
		mustStatus(t, res, domain.StatusRejected, domain.FailureReferenceMismatch)
		if res.Transaction.ReferenceTransactionID() != nil {
			t.Errorf("%s: an incompatible reference was recorded as resolved", name)
		}
		if n, _ := ledgerOf(t, res.Transaction.ID()); n != 0 {
			t.Errorf("%s: ledger entry", name)
		}
	}
	if after := countRows(t, "wallet_ledger_entries", "wallet_id IN ($1, $2)", uuid.MustParse(wallet), uuid.MustParse(otherWallet)); after != before {
		t.Fatalf("incompatible references wrote %d ledger entries", after-before)
	}

	// Referência à própria operação é entrada inválida (corrigível), não gravada.
	if _, err := svc.Submit(context.Background(), op(wallet, player, "REFUND", id("m5"), "25.00", id("m5")), "c"); !errors.Is(err, domain.ErrInvalidRequest) {
		t.Errorf("self reference: %v", err)
	}

	// Referência REJECTED: definitivo, mesmo antes do prazo.
	rejectedBet := mustSubmit(t, svc, op(wallet, player, "BET", id("rejected-bet"), "9999.00", ""))
	mustStatus(t, rejectedBet, domain.StatusRejected, domain.FailureInsufficientFunds)
	res := mustSubmit(t, svc, op(wallet, player, "REFUND", id("of-rejected"), "9999.00", id("rejected-bet")))
	mustStatus(t, res, domain.StatusRejected, domain.FailureReferenceNotProcessed)

	// Escopo do provedor: outro provedor não enxerga (nem revela) a transação do primeiro.
	foreign := op(wallet, player, "REFUND", id("foreign"), "25.00", id("bet"))
	foreign.ProviderID = "svc-provider-other"
	pend := mustSubmit(t, svc, foreign)
	mustStatus(t, pend, domain.StatusPendingReference, "")
	if pend.Transaction.ReferenceTransactionID() != nil {
		t.Fatal("a reference of another provider was resolved")
	}
}

// REFUND e ROLLBACK simultâneos contra a mesma BET: exatamente uma reversão processada, uma
// rejeição auditável e um único crédito de reversão.
func TestConcurrentRefundAndRollbackOfTheSameBet(t *testing.T) {
	svc := newServiceWith(t, nil, nil)
	id := scoped()
	for round := 0; round < 15; round++ {
		wallet, player := svcOpen(t, svc, "100.00")
		bet := id("bet") + string(rune('a'+round))
		mustSubmit(t, svc, op(wallet, player, "BET", bet, "25.00", ""))
		type out struct {
			res app.SubmitResult
			err error
		}
		results := parallel(8, func(i int) out {
			kind, ext := "REFUND", id("rev")+string(rune('a'+round))+"-"+string(rune('0'+i))
			if i%2 == 1 {
				kind = "ROLLBACK"
			}
			res, err := svc.Submit(context.Background(), op(wallet, player, kind, ext, "25.00", bet), "c")
			return out{res, err}
		})
		processed, rejected := 0, 0
		for _, o := range results {
			if o.err != nil {
				t.Fatalf("round %d: %v", round, o.err)
			}
			switch o.res.Transaction.Status() {
			case domain.StatusProcessed:
				processed++
			case domain.StatusRejected:
				if o.res.Transaction.FailureCode() != domain.FailureAlreadyReversed {
					t.Fatalf("round %d: rejected with %s", round, o.res.Transaction.FailureCode())
				}
				rejected++
			default:
				t.Fatalf("round %d: status %s", round, o.res.Transaction.Status())
			}
		}
		if processed != 1 || rejected != 7 {
			t.Fatalf("round %d: processed=%d rejected=%d", round, processed, rejected)
		}
		if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'CREDIT'", uuid.MustParse(wallet)); n != 2 { // OPENING + um crédito de reversão
			t.Fatalf("round %d: credits = %d", round, n)
		}
		if bal, _ := walletState(t, wallet); bal != 100_00 {
			t.Fatalf("round %d: balance %d", round, bal)
		}
		assertLedgerMatchesBalance(t, wallet)
	}
}
