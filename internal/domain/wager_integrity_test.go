package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Regressões de validação, imutabilidade e encapsulamento de WagerTransaction.

func betRequest(t *testing.T) ExternalRequest {
	t.Helper()
	req, err := NewExternalRequest(RawExternalRequest{
		ProviderID: "provider-a", ExternalTransactionID: "bet-1", IdempotencyKey: "key-1",
		PlayerID: uuid.NewString(), WalletID: uuid.NewString(), RoundID: "round", GameID: "game",
		Kind: "BET", Amount: "10.00", Currency: "BRL",
	})
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func TestConstructorRevalidatesTheRequest(t *testing.T) {
	good := betRequest(t)
	if _, err := NewExternalTransaction(uuid.New(), good, testNow); err != nil {
		t.Fatal(err)
	}

	mutations := map[string]func(*ExternalRequest){
		"invalid kind":                       func(r *ExternalRequest) { r.f.kind = Kind("INVALID") },
		"OPENING kind":                       func(r *ExternalRequest) { r.f.kind = KindOpening },
		"zero BET":                           func(r *ExternalRequest) { r.f.money, _ = Zero("BRL") },
		"negative amount":                    func(r *ExternalRequest) { r.f.money, _ = FromMinor(-100, "BRL") },
		"uninitialized money":                func(r *ExternalRequest) { r.f.money = Money{} },
		"reference on BET":                   func(r *ExternalRequest) { r.f.referenceExternalID = "other" },
		"empty provider":                     func(r *ExternalRequest) { r.f.providerID = "" },
		"padded round":                       func(r *ExternalRequest) { r.f.roundID = " r" },
		"nil wallet":                         func(r *ExternalRequest) { r.f.walletID = uuid.Nil },
		"business field changed, hash stale": func(r *ExternalRequest) { r.f.gameID = "another-game" },
		"hash replaced":                      func(r *ExternalRequest) { r.payloadHash = "0000" },
	}
	for name, mutate := range mutations {
		r := good
		mutate(&r)
		if _, err := NewExternalTransaction(uuid.New(), r, testNow); err == nil {
			t.Errorf("%s: constructor accepted an inconsistent request", name)
		}
	}
	// O valor zero de ExternalRequest nunca gera transação.
	if _, err := NewExternalTransaction(uuid.New(), ExternalRequest{}, testNow); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("zero request: %v", err)
	}
	if _, err := NewExternalTransaction(uuid.Nil, good, testNow); err == nil {
		t.Error("nil id accepted")
	}
}

func TestRequestIsImmutableThroughItsAPI(t *testing.T) {
	r := betRequest(t)
	// Só há getters: copiar a requisição e usar os valores devolvidos não altera o original.
	amount := r.Money()
	if _, err := amount.Add(amount); err != nil {
		t.Fatal(err)
	}
	if r.Money().Amount() != "10.00" || r.Kind() != KindBet || r.PayloadHash() == "" {
		t.Fatalf("request changed: %+v", r)
	}
}

func processedState(t *testing.T) WagerTransactionState {
	t.Helper()
	req := betRequest(t)
	tx, err := NewExternalTransaction(uuid.New(), req, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Process(mustMoney(t, "90.00", "BRL"), testNow); err != nil {
		t.Fatal(err)
	}
	return WagerTransactionState{
		ID: tx.ID(), Origin: OriginExternal, Kind: tx.Kind(), Status: tx.Status(), WalletID: tx.WalletID(),
		PlayerID: tx.PlayerID(), Money: tx.Money(), External: tx.External(), ResultBalance: tx.ResultBalance(),
		CreatedAt: testNow, UpdatedAt: testNow,
	}
}

func TestRehydrationValidatesTheWholeState(t *testing.T) {
	good := processedState(t)
	if _, err := RehydrateWagerTransaction(good); err != nil {
		t.Fatalf("valid state rejected: %v", err)
	}
	neg, _ := FromMinor(-100, "BRL")
	negAmount, _ := FromMinor(-1000, "BRL")
	zero, _ := Zero("BRL")
	ref := uuid.New()

	cases := map[string]func(*WagerTransactionState){
		"negative result on PROCESSED":  func(s *WagerTransactionState) { s.ResultBalance = neg },
		"result in another currency":    func(s *WagerTransactionState) { s.ResultBalance = mustMoney(t, "1.00", "USD") },
		"PROCESSED without result":      func(s *WagerTransactionState) { s.ResultBalance = Money{} },
		"PROCESSED with failure code":   func(s *WagerTransactionState) { s.FailureCode = "SOME_CODE" },
		"PENDING with result":           func(s *WagerTransactionState) { s.Status = StatusPending },
		"REJECTED without failure code": func(s *WagerTransactionState) { s.Status = StatusRejected },
		"REJECTED without result": func(s *WagerTransactionState) {
			s.Status, s.FailureCode, s.ResultBalance = StatusRejected, FailureInsufficientFunds, Money{}
		},
		"FAILED without failure code": func(s *WagerTransactionState) { s.Status, s.ResultBalance = StatusFailed, Money{} },
		"invalid failure code": func(s *WagerTransactionState) {
			s.Status, s.FailureCode = StatusRejected, "not valid"
		},
		"unknown status":                                func(s *WagerTransactionState) { s.Status = "DONE" },
		"invalid kind":                                  func(s *WagerTransactionState) { s.Kind = Kind("INVALID") },
		"zero amount for BET":                           func(s *WagerTransactionState) { s.Money = zero },
		"negative amount":                               func(s *WagerTransactionState) { s.Money = negAmount },
		"uninitialized amount":                          func(s *WagerTransactionState) { s.Money = Money{} },
		"external OPENING":                              func(s *WagerTransactionState) { s.Kind = KindOpening },
		"external without metadata":                     func(s *WagerTransactionState) { s.External = nil },
		"internal origin with metadata":                 func(s *WagerTransactionState) { s.Origin = OriginInternal },
		"unknown origin":                                func(s *WagerTransactionState) { s.Origin = "ELSEWHERE" },
		"empty round":                                   func(s *WagerTransactionState) { s.External.RoundID = "" },
		"stale payload hash":                            func(s *WagerTransactionState) { s.External.GameID = "changed-after-hashing" },
		"malformed payload hash":                        func(s *WagerTransactionState) { s.External.PayloadHash = "abc" },
		"reference on BET":                              func(s *WagerTransactionState) { s.External.ReferenceExternalTransactionID = "x" },
		"resolved reference without external reference": func(s *WagerTransactionState) { s.ReferenceTransactionID = &ref },
		"self reference": func(s *WagerTransactionState) {
			id := s.ID
			s.ReferenceTransactionID = &id
		},
		"nil identity":               func(s *WagerTransactionState) { s.ID = uuid.Nil },
		"updatedAt before createdAt": func(s *WagerTransactionState) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) },
	}
	for name, mutate := range cases {
		s := good
		ext := *good.External // cada caso trabalha numa cópia dos metadados
		s.External = &ext
		mutate(&s)
		if _, err := RehydrateWagerTransaction(s); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRehydrationOfOpeningAndInternalRules(t *testing.T) {
	w, p := uuid.New(), uuid.New()
	st := WagerTransactionState{
		ID: uuid.New(), Origin: OriginInternal, Kind: KindOpening, Status: StatusProcessed, WalletID: w, PlayerID: p,
		Money: mustMoney(t, "5.00", "BRL"), ResultBalance: mustMoney(t, "5.00", "BRL"), CreatedAt: testNow, UpdatedAt: testNow,
	}
	if _, err := RehydrateWagerTransaction(st); err != nil {
		t.Fatalf("valid OPENING rejected: %v", err)
	}
	bad := st
	bad.Status, bad.ResultBalance = StatusPending, Money{}
	if _, err := RehydrateWagerTransaction(bad); err == nil {
		t.Error("OPENING in PENDING accepted")
	}
	bad = st
	bad.Money = mustMoney(t, "0.00", "BRL")
	if _, err := RehydrateWagerTransaction(bad); err == nil {
		t.Error("zero OPENING accepted")
	}
}

func TestReferencesAreCopiedOnTheWayInAndOut(t *testing.T) {
	raw := RawExternalRequest{
		ProviderID: "provider-a", ExternalTransactionID: "win-1", IdempotencyKey: "key-w",
		PlayerID: uuid.NewString(), WalletID: uuid.NewString(), RoundID: "round", GameID: "game",
		Kind: "WIN", Amount: "10.00", Currency: "BRL", ReferenceExternalTransactionID: "original-bet",
	}
	req, err := NewExternalRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewExternalTransaction(uuid.New(), req, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Process(mustMoney(t, "20.00", "BRL"), testNow); err != nil {
		t.Fatal(err)
	}
	ref := uuid.New()
	original := ref
	state := WagerTransactionState{
		ID: tx.ID(), Origin: OriginExternal, Kind: KindWin, Status: StatusProcessed, WalletID: tx.WalletID(), PlayerID: tx.PlayerID(),
		Money: tx.Money(), External: tx.External(), ReferenceTransactionID: &ref, ResultBalance: tx.ResultBalance(),
		CreatedAt: testNow, UpdatedAt: testNow,
	}
	got, err := RehydrateWagerTransaction(state)
	if err != nil {
		t.Fatal(err)
	}

	// Alterar o ponteiro entregue à reidratação não afeta a transação.
	ref = uuid.New()
	*state.ReferenceTransactionID = uuid.New()
	if r := got.ReferenceTransactionID(); r == nil || *r != original {
		t.Fatalf("reference followed the caller's pointer: %v", r)
	}
	// Alterar o valor devolvido pelo getter não afeta a transação.
	*got.ReferenceTransactionID() = uuid.New()
	if *got.ReferenceTransactionID() != original {
		t.Fatal("getter exposed the internal reference")
	}
	// Os metadados também são entregues por cópia.
	got.External().ProviderID = "tampered"
	if got.External().ProviderID != "provider-a" {
		t.Fatal("External() exposed internal metadata")
	}
	state.External.ProviderID = "tampered"
	if got.External().ProviderID != "provider-a" {
		t.Fatal("rehydration kept an alias of the caller's metadata")
	}
	// Terminal permanece terminal.
	if err := got.Reject(FailureInsufficientFunds, mustMoney(t, "1.00", "BRL"), testNow); !errors.Is(err, ErrTerminalTransaction) {
		t.Fatalf("terminal transaction accepted a transition: %v", err)
	}
	if (&WagerTransaction{}).ReferenceTransactionID() != nil {
		t.Fatal("nil reference must stay nil")
	}
}
