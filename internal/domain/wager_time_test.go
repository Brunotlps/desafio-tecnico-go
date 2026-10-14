package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Política temporal de WagerTransaction: criação, transições e reidratação
// compartilham as mesmas regras (instantes não nulos, updatedAt >= createdAt e
// updatedAt nunca regride), e uma transição recusada não altera a entidade.

type txSnapshot struct {
	status        Status
	failureCode   FailureCode
	resultBalance Money
	reference     uuid.UUID
	hasReference  bool
	createdAt     time.Time
	updatedAt     time.Time
}

func snapshotOf(tx *WagerTransaction) txSnapshot {
	s := txSnapshot{
		status: tx.Status(), failureCode: tx.FailureCode(), resultBalance: tx.ResultBalance(),
		createdAt: tx.CreatedAt(), updatedAt: tx.UpdatedAt(),
	}
	if ref := tx.ReferenceTransactionID(); ref != nil {
		s.reference, s.hasReference = *ref, true
	}
	return s
}

// stateOf monta o estado persistido equivalente ao da entidade, como o repositório faria.
func stateOf(tx *WagerTransaction) WagerTransactionState {
	return WagerTransactionState{
		ID: tx.ID(), Origin: tx.Origin(), Kind: tx.Kind(), Status: tx.Status(), WalletID: tx.WalletID(),
		PlayerID: tx.PlayerID(), Money: tx.Money(), External: tx.External(), ReferenceTransactionID: tx.ReferenceTransactionID(),
		FailureCode: tx.FailureCode(), ResultBalance: tx.ResultBalance(), CreatedAt: tx.CreatedAt(), UpdatedAt: tx.UpdatedAt(),
	}
}

func newPendingRefund(t *testing.T, createdAt time.Time) *WagerTransaction {
	t.Helper()
	raw := rawBet()
	raw.Kind, raw.ReferenceExternalTransactionID = "REFUND", "bet-1"
	req, err := NewExternalRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewExternalTransaction(uuid.New(), req, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	// Uma reversão só é concluída com a referência resolvida: resolve contra a BET
	// processada que ela referencia (mesmo instante da criação, sem alterar updatedAt).
	betRaw := rawBet()
	betRaw.ExternalTransactionID, betRaw.IdempotencyKey = "bet-1", "key-bet-1"
	betReq, err := NewExternalRequest(betRaw)
	if err != nil {
		t.Fatal(err)
	}
	bet, err := NewExternalTransaction(uuid.New(), betReq, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := bet.Process(mustMoney(t, "10.00", "BRL"), createdAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.ResolveReference(bet, createdAt); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestTransitionsRejectIncoherentTimestampsWithoutChangingTheTransaction(t *testing.T) {
	balance := mustMoney(t, "75.00", "BRL")
	created := testNow
	afterWait := testNow.Add(10 * time.Second) // instante da espera pela referência

	transitions := map[string]func(*WagerTransaction, time.Time) error{
		"Process": func(tx *WagerTransaction, now time.Time) error { return tx.Process(balance, now) },
		"Reject": func(tx *WagerTransaction, now time.Time) error {
			return tx.Reject(FailureInsufficientFunds, balance, now)
		},
		"Fail":           func(tx *WagerTransaction, now time.Time) error { return tx.Fail("PERSISTENCE_FAILED", now) },
		"AwaitReference": func(tx *WagerTransaction, now time.Time) error { return tx.AwaitReference(now) },
	}
	// Cenários de partida: PENDING recém-criada e PENDING_REFERENCE já atualizada em afterWait.
	starts := map[string]func(*testing.T) *WagerTransaction{
		"PENDING": func(t *testing.T) *WagerTransaction { return newPendingRefund(t, created) },
		"PENDING_REFERENCE": func(t *testing.T) *WagerTransaction {
			tx := newPendingRefund(t, created)
			if err := tx.AwaitReference(afterWait); err != nil {
				t.Fatal(err)
			}
			return tx
		},
	}
	badTimes := map[string]func(*WagerTransaction) time.Time{
		"zero instant":           func(*WagerTransaction) time.Time { return time.Time{} },
		"before creation":        func(tx *WagerTransaction) time.Time { return tx.CreatedAt().Add(-time.Nanosecond) },
		"long before creation":   func(tx *WagerTransaction) time.Time { return tx.CreatedAt().Add(-24 * time.Hour) },
		"before the last update": func(tx *WagerTransaction) time.Time { return tx.UpdatedAt().Add(-time.Nanosecond) },
	}

	for startName, start := range starts {
		for name, transition := range transitions {
			if startName == "PENDING_REFERENCE" && name == "AwaitReference" {
				continue // transição inválida por outro motivo (já em espera)
			}
			for badName, bad := range badTimes {
				if badName == "before the last update" && startName == "PENDING" {
					continue // sem atualização além da criação: coberto por "before creation"
				}
				t.Run(startName+"/"+name+"/"+badName, func(t *testing.T) {
					tx := start(t)
					before := snapshotOf(tx)
					err := transition(tx, bad(tx))
					if !errors.Is(err, ErrInvalidTimestamp) || !errors.Is(err, ErrInvalidTransaction) {
						t.Fatalf("got %v, want ErrInvalidTimestamp (an ErrInvalidTransaction)", err)
					}
					if after := snapshotOf(tx); after != before {
						t.Fatalf("a refused transition changed the transaction:\n before %+v\n after  %+v", before, after)
					}
					// O estado continua reidratável e a transição ainda é possível com um instante coerente.
					if _, err := RehydrateWagerTransaction(stateOf(tx)); err != nil {
						t.Fatalf("state no longer rehydrates: %v", err)
					}
					if err := transition(tx, tx.UpdatedAt()); err != nil {
						t.Fatalf("the transition must still be possible with a coherent instant: %v", err)
					}
				})
			}
		}
	}
}

// O mesmo instante da última atualização é válido (várias etapas no mesmo commit
// usam o mesmo relógio), e todo estado produzido por transições válidas é aceito
// pela reidratação.
func TestValidTransitionsProduceRehydratableStates(t *testing.T) {
	balance := mustMoney(t, "75.00", "BRL")
	steps := []struct {
		name string
		at   time.Duration
		do   func(*WagerTransaction, time.Time) error
	}{
		{"await at the creation instant", 0, func(tx *WagerTransaction, now time.Time) error { return tx.AwaitReference(now) }},
		{"process at the same instant", 0, func(tx *WagerTransaction, now time.Time) error { return tx.Process(balance, now) }},
	}
	tx := newPendingRefund(t, testNow)
	for _, s := range steps {
		if err := s.do(tx, testNow.Add(s.at)); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if _, err := RehydrateWagerTransaction(stateOf(tx)); err != nil {
			t.Fatalf("after %s the state does not rehydrate: %v", s.name, err)
		}
	}

	// Cada transição terminal, a partir de PENDING ou PENDING_REFERENCE, em instantes crescentes.
	for name, finish := range map[string]func(*WagerTransaction, time.Time) error{
		"Process": func(tx *WagerTransaction, now time.Time) error { return tx.Process(balance, now) },
		"Reject": func(tx *WagerTransaction, now time.Time) error {
			return tx.Reject(FailureInsufficientFunds, balance, now)
		},
		"Fail": func(tx *WagerTransaction, now time.Time) error { return tx.Fail("PERSISTENCE_FAILED", now) },
	} {
		tx := newPendingRefund(t, testNow)
		if err := tx.AwaitReference(testNow.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := finish(tx, testNow.Add(2*time.Second)); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		got, err := RehydrateWagerTransaction(stateOf(tx))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !got.UpdatedAt().Equal(testNow.Add(2 * time.Second)) {
			t.Fatalf("%s: updatedAt %v", name, got.UpdatedAt())
		}
	}
}

// Criação e reidratação recusam os mesmos instantes incoerentes.
func TestCreationAndRehydrationShareTheTimestampPolicy(t *testing.T) {
	req, err := NewExternalRequest(rawBet())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewExternalTransaction(uuid.New(), req, time.Time{}); !errors.Is(err, ErrInvalidTimestamp) || !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("external creation at the zero instant: %v", err)
	}
	if _, err := NewOpeningTransaction(uuid.New(), uuid.New(), uuid.New(), mustMoney(t, "10.00", "BRL"), time.Time{}); !errors.Is(err, ErrInvalidTimestamp) {
		t.Errorf("opening creation at the zero instant: %v", err)
	}

	good := processedState(t)
	for name, mutate := range map[string]func(*WagerTransactionState){
		"zero createdAt":        func(s *WagerTransactionState) { s.CreatedAt = time.Time{} },
		"zero updatedAt":        func(s *WagerTransactionState) { s.UpdatedAt = time.Time{} },
		"updatedAt < createdAt": func(s *WagerTransactionState) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) },
	} {
		s := good
		mutate(&s)
		if _, err := RehydrateWagerTransaction(s); !errors.Is(err, ErrInvalidTimestamp) || !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := RehydrateWagerTransaction(good); err != nil {
		t.Fatalf("good state: %v", err)
	}
}
