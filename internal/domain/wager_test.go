package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func rawBet() RawExternalRequest {
	return RawExternalRequest{
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123", IdempotencyKey: "provider-a:transaction-123",
		PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID: "round-987", GameID: "fortune-chimp", Kind: "BET", Amount: "25.00", Currency: "BRL",
	}
}

func TestExternalRequestKindsAndZeroPolicy(t *testing.T) {
	type tc struct {
		kind, amount, ref string
		want              error
	}
	cases := []tc{
		{"BET", "25.00", "", nil},
		{"BET", "0.00", "", ErrAmountNotAllowed},
		{"WIN", "10.00", "", nil},
		{"WIN", "10.00", "bet-1", nil}, // referência opcional
		{"WIN", "0.00", "", ErrAmountNotAllowed},
		{"LOSS", "0.00", "", nil},
		{"LOSS", "0", "", nil},
		{"LOSS", "1.00", "", ErrAmountNotAllowed},
		{"REFUND", "25.00", "bet-1", nil},
		{"REFUND", "25.00", "", ErrInvalidRequest}, // referência obrigatória
		{"REFUND", "0.00", "bet-1", ErrAmountNotAllowed},
		{"ROLLBACK", "25.00", "bet-1", nil},
		{"ROLLBACK", "25.00", "", ErrInvalidRequest},
		{"ROLLBACK", "0.00", "bet-1", ErrAmountNotAllowed},
		{"BET", "25.00", "bet-1", ErrInvalidRequest}, // referência proibida
		{"LOSS", "0.00", "bet-1", ErrInvalidRequest},
		{"OPENING", "25.00", "", ErrKindNotAllowed},
		{"bet", "25.00", "", ErrInvalidKind},
		{"", "25.00", "", ErrInvalidKind},
		{"TRANSFER", "25.00", "", ErrInvalidKind},
	}
	for _, c := range cases {
		raw := rawBet()
		raw.Kind, raw.Amount, raw.ReferenceExternalTransactionID = c.kind, c.amount, c.ref
		_, err := NewExternalRequest(raw)
		if c.want == nil && err != nil {
			t.Errorf("%s %s ref=%q: unexpected %v", c.kind, c.amount, c.ref, err)
		}
		if c.want != nil && !errors.Is(err, c.want) {
			t.Errorf("%s %s ref=%q: got %v, want %v", c.kind, c.amount, c.ref, err, c.want)
		}
	}
}

func TestExternalRequestFieldValidation(t *testing.T) {
	mutations := map[string]func(*RawExternalRequest){
		"empty provider":     func(r *RawExternalRequest) { r.ProviderID = "" },
		"empty external id":  func(r *RawExternalRequest) { r.ExternalTransactionID = "" },
		"empty key":          func(r *RawExternalRequest) { r.IdempotencyKey = "" },
		"empty round":        func(r *RawExternalRequest) { r.RoundID = "" },
		"empty game":         func(r *RawExternalRequest) { r.GameID = "" },
		"padded provider":    func(r *RawExternalRequest) { r.ProviderID = " provider-a" },
		"control char":       func(r *RawExternalRequest) { r.GameID = "a\nb" },
		"long external id":   func(r *RawExternalRequest) { r.ExternalTransactionID = strings.Repeat("x", 129) },
		"long key":           func(r *RawExternalRequest) { r.IdempotencyKey = strings.Repeat("x", 256) },
		"invalid player":     func(r *RawExternalRequest) { r.PlayerID = "nope" },
		"nil uuid wallet":    func(r *RawExternalRequest) { r.WalletID = uuid.Nil.String() },
		"urn uuid":           func(r *RawExternalRequest) { r.WalletID = "urn:uuid:" + r.WalletID },
		"unhyphenated uuid":  func(r *RawExternalRequest) { r.WalletID = strings.ReplaceAll(r.WalletID, "-", "") },
		"invalid amount":     func(r *RawExternalRequest) { r.Amount = "1e3" },
		"negative amount":    func(r *RawExternalRequest) { r.Amount = "-1.00" },
		"too many decimals":  func(r *RawExternalRequest) { r.Amount = "1.001" },
		"invalid currency":   func(r *RawExternalRequest) { r.Currency = "brl" },
		"invalid utf8 round": func(r *RawExternalRequest) { r.RoundID = "\xff" },
	}
	for name, mutate := range mutations {
		raw := rawBet()
		mutate(&raw)
		if _, err := NewExternalRequest(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPayloadHashCanonical(t *testing.T) {
	a, err := NewExternalRequest(rawBet())
	if err != nil {
		t.Fatal(err)
	}
	// Vetor fixo, conferido de forma independente (Python: json.dumps com
	// sort_keys e separadores compactos + sha256). Protege o contrato do hash.
	const want = "629836932b79106b99523d06a1e7fa80689b0ea1e1c47aa3f0a5a2c87d0c4344"
	if a.PayloadHash() != want {
		t.Fatalf("hash = %q, want %q", a.PayloadHash(), want)
	}

	// Formas equivalentes de amount e UUID normalizam para o mesmo hash.
	eq := rawBet()
	eq.Amount = "25"
	eq.PlayerID = strings.ToUpper(eq.PlayerID)
	eq.WalletID = strings.ToUpper(eq.WalletID)
	b, err := NewExternalRequest(eq)
	if err != nil {
		t.Fatal(err)
	}
	if a.PayloadHash() != b.PayloadHash() {
		t.Error("equivalent payloads produced different hashes")
	}

	// A chave de idempotência não participa do hash.
	k := rawBet()
	k.IdempotencyKey = "another-key"
	c, _ := NewExternalRequest(k)
	if a.PayloadHash() != c.PayloadHash() {
		t.Error("idempotency key changed the hash")
	}

	// Qualquer campo de negócio altera o hash.
	diffs := map[string]func(*RawExternalRequest){
		"provider": func(r *RawExternalRequest) { r.ProviderID = "provider-b" },
		"external": func(r *RawExternalRequest) { r.ExternalTransactionID = "t-2" },
		"player":   func(r *RawExternalRequest) { r.PlayerID = uuid.NewString() },
		"wallet":   func(r *RawExternalRequest) { r.WalletID = uuid.NewString() },
		"round":    func(r *RawExternalRequest) { r.RoundID = "r2" },
		"game":     func(r *RawExternalRequest) { r.GameID = "g2" },
		"kind":     func(r *RawExternalRequest) { r.Kind = "WIN" },
		"amount":   func(r *RawExternalRequest) { r.Amount = "25.01" },
		"currency": func(r *RawExternalRequest) { r.Currency = "USD" },
	}
	for name, mutate := range diffs {
		raw := rawBet()
		mutate(&raw)
		d, err := NewExternalRequest(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if d.PayloadHash() == a.PayloadHash() {
			t.Errorf("%s did not change the hash", name)
		}
	}
	ref := rawBet()
	ref.Kind, ref.ReferenceExternalTransactionID = "REFUND", "bet-1"
	r1, _ := NewExternalRequest(ref)
	ref.ReferenceExternalTransactionID = "bet-2"
	r2, _ := NewExternalRequest(ref)
	if r1.PayloadHash() == r2.PayloadHash() {
		t.Error("reference did not change the hash")
	}
}

func TestCanonicalJSONFormat(t *testing.T) {
	got := string(canonicalJSON(map[string]any{
		"b": "x<y>&z", "a": map[string]any{"z": "1", "y": "é\"\\"},
	}))
	const want = `{"a":{"y":"é\"\\","z":"1"},"b":"x<y>&z"}`
	if got != want {
		t.Fatalf("canonical = %s\nwant       %s", got, want)
	}
}

func newPendingBet(t *testing.T) *WagerTransaction {
	t.Helper()
	req, err := NewExternalRequest(rawBet())
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewExternalTransaction(uuid.New(), req, testNow)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestTransactionStateMachine(t *testing.T) {
	bal := mustMoney(t, "75.00", "BRL")

	tx := newPendingBet(t)
	if tx.Status() != StatusPending || tx.Origin() != OriginExternal || tx.External() == nil {
		t.Fatalf("new transaction: %+v", tx)
	}
	if err := tx.Process(bal, testNow); err != nil || tx.Status() != StatusProcessed || tx.ResultBalance().Amount() != "75.00" {
		t.Fatalf("process: %v", err)
	}
	// Terminal: nenhuma transição.
	for name, fn := range map[string]func() error{
		"process": func() error { return tx.Process(bal, testNow) },
		"reject":  func() error { return tx.Reject(FailureInsufficientFunds, bal, testNow) },
		"fail":    func() error { return tx.Fail("DB_GONE", testNow) },
	} {
		if err := fn(); !errors.Is(err, ErrTerminalTransaction) {
			t.Errorf("%s after PROCESSED: %v", name, err)
		}
	}
	if tx.Status() != StatusProcessed || tx.FailureCode() != "" {
		t.Error("terminal transaction mutated")
	}

	rej := newPendingBet(t)
	if err := rej.Reject(FailureInsufficientFunds, bal, testNow); err != nil || rej.Status() != StatusRejected || rej.FailureCode() != FailureInsufficientFunds {
		t.Fatalf("reject: %v", err)
	}
	if err := rej.Process(bal, testNow); !errors.Is(err, ErrTerminalTransaction) {
		t.Errorf("process after REJECTED: %v", err)
	}

	failed := newPendingBet(t)
	if err := failed.Fail("PERSISTENCE_FAILED", testNow); err != nil || failed.Status() != StatusFailed {
		t.Fatalf("fail: %v", err)
	}
	if err := failed.Reject(FailureInsufficientFunds, bal, testNow); !errors.Is(err, ErrTerminalTransaction) {
		t.Errorf("reject after FAILED: %v", err)
	}

	// BET não tem referência: não pode aguardar uma.
	if err := newPendingBet(t).AwaitReference(testNow); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("await on BET: %v", err)
	}
	// Código de falha inválido e saldo inconsistente.
	if err := newPendingBet(t).Reject("lowercase", bal, testNow); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("bad failure code: %v", err)
	}
	if err := newPendingBet(t).Process(Money{}, testNow); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("uninitialized balance: %v", err)
	}
	if err := newPendingBet(t).Process(mustMoney(t, "1.00", "USD"), testNow); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("balance currency: %v", err)
	}
}

func TestTransactionPendingReferencePath(t *testing.T) {
	raw := rawBet()
	raw.Kind, raw.ReferenceExternalTransactionID = "REFUND", "bet-1"
	req, err := NewExternalRequest(raw)
	if err != nil {
		t.Fatal(err)
	}
	tx, _ := NewExternalTransaction(uuid.New(), req, testNow)
	if err := tx.AwaitReference(testNow); err != nil || tx.Status() != StatusPendingReference {
		t.Fatalf("await: %v", err)
	}
	if tx.Status().IsTerminal() {
		t.Error("PENDING_REFERENCE is not terminal")
	}
	if err := tx.AwaitReference(testNow); !errors.Is(err, ErrInvalidTransition) {
		t.Errorf("await twice: %v", err)
	}
	// Concluir exige a referência resolvida.
	if err := tx.Process(mustMoney(t, "125.00", "BRL"), testNow); !errors.Is(err, ErrInvalidTransition) || tx.Status() != StatusPendingReference {
		t.Fatalf("process without a resolved reference: %v", err)
	}
	betRaw := rawBet()
	betRaw.ExternalTransactionID, betRaw.IdempotencyKey = "bet-1", "key-bet-1"
	betReq, _ := NewExternalRequest(betRaw)
	bet, _ := NewExternalTransaction(uuid.New(), betReq, testNow)
	_ = bet.Process(mustMoney(t, "100.00", "BRL"), testNow)
	if err := tx.ResolveReference(bet, testNow); err != nil {
		t.Fatal(err)
	}
	if err := tx.Process(mustMoney(t, "125.00", "BRL"), testNow); err != nil || tx.Status() != StatusProcessed {
		t.Fatalf("resolve: %v", err)
	}
}

func TestRehydrateTransactionHasNoSideEffects(t *testing.T) {
	tx := newPendingBet(t)
	_ = tx.Reject(FailureInsufficientFunds, mustMoney(t, "20.00", "BRL"), testNow)
	st := WagerTransactionState{
		ID: tx.ID(), Origin: tx.Origin(), Kind: tx.Kind(), Status: tx.Status(), WalletID: tx.WalletID(),
		PlayerID: tx.PlayerID(), Money: tx.Money(), External: tx.External(), FailureCode: tx.FailureCode(),
		ResultBalance: tx.ResultBalance(), CreatedAt: testNow, UpdatedAt: testNow,
	}
	back, err := RehydrateWagerTransaction(st)
	if err != nil {
		t.Fatal(err)
	}
	if back.Status() != StatusRejected || back.FailureCode() != FailureInsufficientFunds || back.ResultBalance().Amount() != "20.00" {
		t.Fatalf("rehydrated: %+v", back)
	}
	if err := back.Process(back.ResultBalance(), testNow); !errors.Is(err, ErrTerminalTransaction) {
		t.Errorf("rehydrated terminal accepted a transition: %v", err)
	}

	inconsistent := map[string]func(*WagerTransactionState){
		"internal with external meta": func(s *WagerTransactionState) { s.Origin = OriginInternal },
		"external without meta":       func(s *WagerTransactionState) { s.External = nil },
		"external OPENING":            func(s *WagerTransactionState) { s.Kind = KindOpening },
		"rejected without code":       func(s *WagerTransactionState) { s.FailureCode = "" },
		"processed without balance": func(s *WagerTransactionState) {
			s.Status, s.FailureCode, s.ResultBalance = StatusProcessed, "", Money{}
		},
		"unknown status": func(s *WagerTransactionState) { s.Status = "DONE" },
	}
	for name, mutate := range inconsistent {
		s := st
		mutate(&s)
		if _, err := RehydrateWagerTransaction(s); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestOpeningTransactionPolicy(t *testing.T) {
	w, p := uuid.New(), uuid.New()
	if _, err := NewOpeningTransaction(uuid.New(), w, p, mustMoney(t, "0.00", "BRL"), testNow); !errors.Is(err, ErrAmountNotAllowed) {
		t.Errorf("zero opening must not exist: %v", err)
	}
	tx, err := NewOpeningTransaction(uuid.New(), w, p, mustMoney(t, "10.00", "BRL"), testNow)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Origin() != OriginInternal || tx.Kind() != KindOpening || tx.External() != nil {
		t.Fatalf("opening metadata: %+v", tx)
	}
}

func TestOpenWallet(t *testing.T) {
	player := uuid.New()
	ids := func() uuid.UUID { return uuid.New() }

	pos, err := OpenWallet(ids, player, mustMoney(t, "1000.00", "BRL"), "corr-1", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if pos.Wallet.Version() != 1 || pos.Wallet.Balance().Amount() != "1000.00" {
		t.Fatalf("wallet: %s v%d", pos.Wallet.Balance(), pos.Wallet.Version())
	}
	if pos.Opening == nil || pos.Opening.Status() != StatusProcessed || pos.Opening.Kind() != KindOpening || pos.Opening.External() != nil {
		t.Fatalf("opening: %+v", pos.Opening)
	}
	if pos.Entry == nil || pos.Entry.Direction() != Credit || !pos.Entry.BalanceBefore().IsZero() || pos.Entry.BalanceAfter().Amount() != "1000.00" ||
		pos.Entry.TransactionID() != pos.Opening.ID() {
		t.Fatalf("entry: %+v", pos.Entry)
	}
	if len(pos.Events) != 2 || pos.Events[0].Type() != EventWagerTransactionProcessed || pos.Events[1].Type() != EventWalletBalanceChanged {
		t.Fatalf("events: %+v", pos.Events)
	}
	// Eventos de origem interna não carregam metadados externos.
	payload, _ := pos.Events[0].Payload()
	for _, forbidden := range []string{"providerId", "externalTransactionId", "roundId", "gameId"} {
		if strings.Contains(string(payload), forbidden) {
			t.Errorf("internal event exposes %s: %s", forbidden, payload)
		}
	}
	var changed struct {
		Data WalletBalanceChangedData `json:"data"`
	}
	payload, _ = pos.Events[1].Payload()
	if err := json.Unmarshal(payload, &changed); err != nil {
		t.Fatal(err)
	}
	if changed.Data.WalletVersion != 1 || changed.Data.Direction != Credit || changed.Data.BalanceBefore.Amount() != "0.00" {
		t.Fatalf("balance changed: %+v", changed.Data)
	}

	zero, err := OpenWallet(ids, player, mustMoney(t, "0.00", "BRL"), "corr-2", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if zero.Opening != nil || zero.Entry != nil || len(zero.Events) != 0 || zero.Wallet.Version() != 1 {
		t.Fatalf("zero opening must create only the wallet: %+v", zero)
	}
	if _, err := OpenWallet(ids, player, Money{}, "c", testNow); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("uninitialized initial balance: %v", err)
	}
}

func TestEventEnvelopes(t *testing.T) {
	tx := newPendingBet(t)
	bal := mustMoney(t, "75.00", "BRL")
	if _, err := NewWagerTransactionProcessed(uuid.New(), "c", nil, tx, testNow); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("processed event for pending tx: %v", err)
	}
	_ = tx.Process(bal, testNow)
	causation := uuid.New()
	ev, err := NewWagerTransactionProcessed(uuid.New(), "corr-9", &causation, tx, testNow)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := ev.Payload()
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(payload, &env); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := env[k]; !ok {
			t.Errorf("envelope missing %s: %s", k, payload)
		}
	}
	if env["eventType"] != EventWagerTransactionProcessed || env["version"] != float64(1) || env["occurredAt"] != "2026-09-08T12:00:00.000Z" {
		t.Errorf("envelope: %s", payload)
	}
	data := env["data"].(map[string]any)
	if data["providerId"] != "provider-a" || data["balance"].(map[string]any)["amount"] != "75.00" {
		t.Errorf("data: %v", data)
	}

	// Snapshot imutável: mutar a causationID devolvida não altera o evento.
	*ev.CausationID() = uuid.Nil
	if *ev.causationID != causation {
		t.Error("event causation aliasing")
	}

	// Rejected e PendingReference.
	rej := newPendingBet(t)
	_ = rej.Reject(FailureInsufficientFunds, bal, testNow)
	rev, err := NewWagerTransactionRejected(uuid.New(), "c", nil, rej, testNow)
	if err != nil || rev.Type() != EventWagerTransactionRejected {
		t.Fatalf("rejected: %v", err)
	}
	if _, err := NewWagerTransactionRejected(uuid.New(), "c", nil, tx, testNow); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("rejected event for processed tx: %v", err)
	}
	raw := rawBet()
	raw.Kind, raw.ReferenceExternalTransactionID = "REFUND", "bet-1"
	req, _ := NewExternalRequest(raw)
	pr, _ := NewExternalTransaction(uuid.New(), req, testNow)
	_ = pr.AwaitReference(testNow)
	pev, err := NewWagerTransactionPendingReference(uuid.New(), "c", nil, pr, testNow)
	if err != nil || pev.Type() != EventWagerTransactionPendingReference {
		t.Fatalf("pending reference: %v", err)
	}
	if _, err := NewWagerTransactionProcessed(uuid.Nil, "c", nil, tx, testNow); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("nil event id: %v", err)
	}
	if _, err := NewWagerTransactionProcessed(uuid.New(), "", nil, tx, testNow); !errors.Is(err, ErrInvalidEvent) {
		t.Errorf("empty correlation id: %v", err)
	}
}
