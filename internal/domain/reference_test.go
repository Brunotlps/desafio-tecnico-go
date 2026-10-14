package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func targetOf(t *testing.T, kind, ext string, mutate func(*RawExternalRequest)) *WagerTransaction {
	t.Helper()
	raw := rawBet()
	raw.Kind, raw.ExternalTransactionID, raw.IdempotencyKey = kind, ext, "key-"+ext
	if kind == "LOSS" {
		raw.Amount = "0.00"
	}
	if mutate != nil {
		mutate(&raw)
	}
	req, err := NewExternalRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewExternalTransaction(uuid.New(), req, testNow)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func processed(t *testing.T, tx *WagerTransaction) *WagerTransaction {
	t.Helper()
	if err := tx.Process(mustMoney(t, "50.00", "BRL"), testNow); err != nil {
		t.Fatal(err)
	}
	return tx
}

// processedRefund é um REFUND concluído (referência resolvida), identificado como "bet-1".
func processedRefund(t *testing.T) *WagerTransaction {
	t.Helper()
	bet := processed(t, targetOf(t, "BET", "orig-bet", nil))
	refund := targetOf(t, "REFUND", "bet-1", func(r *RawExternalRequest) { r.ReferenceExternalTransactionID = "orig-bet" })
	if err := refund.ResolveReference(bet, testNow); err != nil {
		t.Fatal(err)
	}
	return processed(t, refund)
}

func opWithRef(t *testing.T, kind, amount string, mutate func(*RawExternalRequest)) *WagerTransaction {
	t.Helper()
	return targetOf(t, kind, "op-1", func(r *RawExternalRequest) {
		r.ReferenceExternalTransactionID, r.Amount = "bet-1", amount
		if mutate != nil {
			mutate(r)
		}
	})
}

func TestAssessReferenceMatrix(t *testing.T) {
	bet := func() *WagerTransaction { return targetOf(t, "BET", "bet-1", nil) } // 25.00, PENDING
	cases := []struct {
		name   string
		op     *WagerTransaction
		target *WagerTransaction
		want   ReferenceAssessment
	}{
		{"refund of a processed BET", opWithRef(t, "REFUND", "25.00", nil), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceUsable}},
		{"rollback of a processed BET", opWithRef(t, "ROLLBACK", "25.00", nil), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceUsable}},
		{"win of a processed BET with another amount", opWithRef(t, "WIN", "99.00", nil), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceUsable}},
		{"refund of a pending BET", opWithRef(t, "REFUND", "25.00", nil), bet(), ReferenceAssessment{Outcome: ReferenceUnavailable}},
		{"refund of a BET waiting for its own reference is impossible; pending reference target", opWithRef(t, "REFUND", "25.00", nil), func() *WagerTransaction {
			b := bet()
			_ = b.AwaitReference(testNow) // BET não tem referência: recusado, permanece PENDING
			return b
		}(), ReferenceAssessment{Outcome: ReferenceUnavailable}},
		{"refund of a rejected BET", opWithRef(t, "REFUND", "25.00", nil), func() *WagerTransaction {
			b := bet()
			_ = b.Reject(FailureInsufficientFunds, mustMoney(t, "1.00", "BRL"), testNow)
			return b
		}(), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceNotProcessed}},
		{"refund with another amount", opWithRef(t, "REFUND", "24.99", nil), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"refund of a WIN", opWithRef(t, "REFUND", "25.00", nil), processed(t, targetOf(t, "WIN", "bet-1", nil)), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"rollback of a WIN", opWithRef(t, "ROLLBACK", "25.00", nil), processed(t, targetOf(t, "WIN", "bet-1", nil)), ReferenceAssessment{Outcome: ReferenceUsable}},
		{"rollback of a REFUND", opWithRef(t, "ROLLBACK", "25.00", nil), processedRefund(t), ReferenceAssessment{Outcome: ReferenceUsable}},
		{"rollback of a LOSS", opWithRef(t, "ROLLBACK", "25.00", nil), processed(t, targetOf(t, "LOSS", "bet-1", nil)), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"win of a WIN", opWithRef(t, "WIN", "25.00", nil), processed(t, targetOf(t, "WIN", "bet-1", nil)), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"other round", opWithRef(t, "REFUND", "25.00", func(r *RawExternalRequest) { r.RoundID = "other" }), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"other player", opWithRef(t, "REFUND", "25.00", func(r *RawExternalRequest) { r.PlayerID = uuid.NewString() }), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"other wallet", opWithRef(t, "REFUND", "25.00", func(r *RawExternalRequest) { r.WalletID = uuid.NewString() }), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"other provider", opWithRef(t, "REFUND", "25.00", func(r *RawExternalRequest) { r.ProviderID = "provider-b" }), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"other currency", opWithRef(t, "REFUND", "25.00", func(r *RawExternalRequest) { r.Currency = "USD" }), processed(t, bet()), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
		{"not the referenced external id", opWithRef(t, "REFUND", "25.00", nil), processed(t, targetOf(t, "BET", "another", nil)), ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}},
	}
	for _, c := range cases {
		got, err := c.op.AssessReference(c.target)
		if err != nil || got != c.want {
			t.Errorf("%s: got %+v (%v), want %+v", c.name, got, err, c.want)
		}
	}
	// Sem referência, ou sem alvo, é erro de uso.
	if _, err := targetOf(t, "BET", "x", nil).AssessReference(processed(t, bet())); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("BET has no reference: %v", err)
	}
	if _, err := opWithRef(t, "REFUND", "25.00", nil).AssessReference(nil); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("nil target: %v", err)
	}
}

func TestResolveReferenceAndProcess(t *testing.T) {
	bet := processed(t, targetOf(t, "BET", "bet-1", nil))
	op := opWithRef(t, "REFUND", "25.00", nil)
	bal := mustMoney(t, "75.00", "BRL")

	if err := op.Process(bal, testNow); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("process without a resolved reference: %v", err)
	}
	// Instante incoerente: nada muda.
	before := snapshotOf(op)
	for name, at := range map[string]time.Time{"zero": {}, "before creation": testNow.Add(-time.Second)} {
		if err := op.ResolveReference(bet, at); !errors.Is(err, ErrInvalidTimestamp) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if snapshotOf(op) != before {
		t.Fatal("a refused resolution changed the transaction")
	}
	// Alvo inutilizável: recusado sem alterar.
	if err := op.ResolveReference(targetOf(t, "BET", "bet-1", nil), testNow); !errors.Is(err, ErrInvalidTransition) { // PENDING
		t.Errorf("unusable target: %v", err)
	}
	if err := op.ResolveReference(bet, testNow.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if ref := op.ReferenceTransactionID(); ref == nil || *ref != bet.ID() || !op.UpdatedAt().Equal(testNow.Add(time.Second)) {
		t.Fatalf("reference=%v updatedAt=%v", ref, op.UpdatedAt())
	}
	// Idempotente para o mesmo alvo; outro alvo é recusado.
	if err := op.ResolveReference(bet, testNow.Add(2*time.Second)); err != nil {
		t.Errorf("same target twice: %v", err)
	}
	other := processed(t, targetOf(t, "BET", "bet-1", nil))
	if err := op.ResolveReference(other, testNow.Add(2*time.Second)); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("another target: %v", err)
	}
	if err := op.Process(bal, testNow.Add(2*time.Second)); err != nil || op.Status() != StatusProcessed {
		t.Fatalf("process: %v", err)
	}
	// Terminal: não resolve de novo.
	if err := op.ResolveReference(bet, testNow.Add(3*time.Second)); !errors.Is(err, ErrTerminalTransaction) {
		t.Errorf("terminal: %v", err)
	}
	// A reidratação do estado concluído exige a referência.
	st := stateOf(op)
	if _, err := RehydrateWagerTransaction(st); err != nil {
		t.Fatalf("rehydrate: %v", err)
	}
	st.ReferenceTransactionID = nil
	if _, err := RehydrateWagerTransaction(st); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("PROCESSED reversal without a resolved reference rehydrated: %v", err)
	}
	// Um WIN sem referência informada não precisa de referência interna.
	win := targetOf(t, "WIN", "w", nil)
	if err := win.Process(bal, testNow); err != nil {
		t.Errorf("plain WIN: %v", err)
	}
	// Rejeição não exige referência resolvida (ausente, tempo esgotado etc.).
	rej := opWithRef(t, "ROLLBACK", "25.00", nil)
	if err := rej.Reject(FailureReferenceNotFound, bal, testNow); err != nil {
		t.Errorf("reject without a resolved reference: %v", err)
	}
}

func TestReversalDirection(t *testing.T) {
	for _, c := range []struct {
		kind, target Kind
		want         Direction
		ok           bool
	}{
		{KindRefund, KindBet, Credit, true},
		{KindRollback, KindBet, Credit, true},
		{KindRollback, KindWin, Debit, true},
		{KindRollback, KindRefund, Debit, true},
		{KindRefund, KindWin, "", false},
		{KindRefund, KindRefund, "", false},
		{KindRollback, KindLoss, "", false},
		{KindRollback, KindRollback, "", false},
		{KindRollback, KindOpening, "", false},
		{KindBet, KindBet, "", false},
		{KindWin, KindBet, "", false},
	} {
		got, err := ReversalDirection(c.kind, c.target)
		if (err == nil) != c.ok || got != c.want {
			t.Errorf("%s of %s: %q %v", c.kind, c.target, got, err)
		}
	}
}

func TestAnOperationCannotReferenceItself(t *testing.T) {
	raw := rawBet()
	raw.Kind, raw.ReferenceExternalTransactionID = "REFUND", raw.ExternalTransactionID
	if _, err := NewExternalRequest(raw); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("self reference: %v", err)
	}
}
