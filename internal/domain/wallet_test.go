package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

var testNow = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func newTestWallet(t *testing.T, balance string) *Wallet {
	t.Helper()
	w, err := NewWallet(uuid.New(), uuid.New(), mustMoney(t, balance, "BRL"), testNow)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestNewWallet(t *testing.T) {
	w := newTestWallet(t, "1000.00")
	if w.Version() != 1 || w.Balance().Amount() != "1000.00" || w.Currency() != "BRL" {
		t.Fatalf("unexpected wallet %+v", w)
	}
	if z := newTestWallet(t, "0.00"); z.Version() != 1 || !z.Balance().IsZero() {
		t.Fatal("zero wallet")
	}
	neg, _ := FromMinor(-1, "BRL")
	if _, err := NewWallet(uuid.New(), uuid.New(), neg, testNow); !errors.Is(err, ErrAmountNegative) {
		t.Errorf("negative initial balance: %v", err)
	}
	if _, err := NewWallet(uuid.New(), uuid.New(), Money{}, testNow); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("uninitialized balance: %v", err)
	}
	if _, err := NewWallet(uuid.Nil, uuid.New(), mustMoney(t, "1", "BRL"), testNow); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("nil id: %v", err)
	}
}

func TestWalletDebitCredit(t *testing.T) {
	w := newTestWallet(t, "100.00")
	txID := uuid.New()
	e, err := w.Debit(uuid.New(), txID, mustMoney(t, "80.00", "BRL"), testNow.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("after debit: %s v%d", w.Balance(), w.Version())
	}
	if e.Direction() != Debit || e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "20.00" || e.TransactionID() != txID {
		t.Fatalf("entry %+v", e)
	}
	// Segundo débito de 80 deixaria saldo negativo: nada muda.
	before := *w
	if _, err := w.Debit(uuid.New(), uuid.New(), mustMoney(t, "80.00", "BRL"), testNow); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("expected insufficient funds, got %v", err)
	}
	if *w != before {
		t.Fatal("failed debit mutated wallet")
	}
	// Débito exato do saldo é permitido.
	if _, err := w.Debit(uuid.New(), uuid.New(), mustMoney(t, "20.00", "BRL"), testNow); err != nil || !w.Balance().IsZero() {
		t.Fatalf("exact debit: %v %s", err, w.Balance())
	}
	e, err = w.Credit(uuid.New(), uuid.New(), mustMoney(t, "5.50", "BRL"), testNow)
	if err != nil || e.Direction() != Credit || w.Balance().Amount() != "5.50" || w.Version() != 4 {
		t.Fatalf("credit: %v %s v%d", err, w.Balance(), w.Version())
	}
}

func TestWalletRejectsInvalidMovements(t *testing.T) {
	w := newTestWallet(t, "100.00")
	snapshot := *w
	cases := map[string]struct {
		amount Money
		want   error
	}{
		"other currency": {mustMoney(t, "1.00", "USD"), ErrCurrencyMismatch},
		"zero":           {mustMoney(t, "0.00", "BRL"), ErrAmountNotAllowed},
		"uninitialized":  {Money{}, ErrUninitializedMoney},
	}
	for name, c := range cases {
		if _, err := w.Debit(uuid.New(), uuid.New(), c.amount, testNow); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	neg, _ := FromMinor(-100, "BRL")
	if _, err := w.Credit(uuid.New(), uuid.New(), neg, testNow); !errors.Is(err, ErrAmountNotAllowed) {
		t.Errorf("negative credit: %v", err)
	}
	if *w != snapshot {
		t.Fatal("invalid movement mutated wallet")
	}
}

func TestWalletCreditOverflow(t *testing.T) {
	big, _ := FromMinor(9223372036854775807, "BRL")
	w, err := NewWallet(uuid.New(), uuid.New(), big, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), mustMoney(t, "0.01", "BRL"), testNow); !errors.Is(err, ErrAmountOverflow) {
		t.Fatalf("expected overflow, got %v", err)
	}
}

func TestRehydrateWalletDoesNotMutate(t *testing.T) {
	st := WalletState{ID: uuid.New(), PlayerID: uuid.New(), Balance: mustMoney(t, "20.00", "BRL"), Version: 7, CreatedAt: testNow, UpdatedAt: testNow}
	w, err := RehydrateWallet(st)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 7 || w.Balance().Amount() != "20.00" {
		t.Fatalf("rehydrate changed state: v%d %s", w.Version(), w.Balance())
	}
	bad := st
	bad.Version = 0
	if _, err := RehydrateWallet(bad); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("version 0: %v", err)
	}
	bad = st
	bad.Balance, _ = FromMinor(-1, "BRL")
	if _, err := RehydrateWallet(bad); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("negative balance: %v", err)
	}
	bad = st
	bad.Balance = Money{}
	if _, err := RehydrateWallet(bad); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("uninitialized balance: %v", err)
	}
}

func TestLedgerEntryInvariant(t *testing.T) {
	base := LedgerEntryState{
		ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), Direction: Debit,
		Money: mustMoney(t, "10.00", "BRL"), BalanceBefore: mustMoney(t, "30.00", "BRL"),
		BalanceAfter: mustMoney(t, "20.00", "BRL"), CreatedAt: testNow,
	}
	if _, err := NewLedgerEntry(base); err != nil {
		t.Fatal(err)
	}
	wrongAfter := base
	wrongAfter.BalanceAfter = mustMoney(t, "21.00", "BRL")
	if _, err := NewLedgerEntry(wrongAfter); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("wrong balanceAfter: %v", err)
	}
	wrongDir := base
	wrongDir.Direction = Credit
	if _, err := NewLedgerEntry(wrongDir); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("wrong direction: %v", err)
	}
	zero := base
	zero.Money = mustMoney(t, "0.00", "BRL")
	zero.BalanceAfter = base.BalanceBefore
	if _, err := NewLedgerEntry(zero); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("zero amount: %v", err)
	}
	cross := base
	cross.BalanceAfter = mustMoney(t, "20.00", "USD")
	if _, err := NewLedgerEntry(cross); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("mixed currency: %v", err)
	}
	if _, err := RehydrateLedgerEntry(wrongAfter); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Errorf("rehydrate must also validate: %v", err)
	}
}
