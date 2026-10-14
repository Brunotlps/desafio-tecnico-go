package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Direction é o sentido de um lançamento.
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func (d Direction) valid() bool { return d == Debit || d == Credit }

// LedgerEntry é um lançamento imutável do ledger da carteira.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	money         Money
	balanceBefore Money
	balanceAfter  Money
	createdAt     time.Time
}

// LedgerEntryState é o estado persistido usado para reidratar um lançamento.
type LedgerEntryState struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Money         Money
	BalanceBefore Money
	BalanceAfter  Money
	CreatedAt     time.Time
}

// NewLedgerEntry cria um lançamento validando
// balanceAfter = balanceBefore ± money conforme a direção.
func NewLedgerEntry(s LedgerEntryState) (LedgerEntry, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.TransactionID == uuid.Nil {
		return LedgerEntry{}, fmt.Errorf("%w: missing identity", ErrInvalidLedgerEntry)
	}
	if !s.Direction.valid() {
		return LedgerEntry{}, fmt.Errorf("%w: direction %q", ErrInvalidLedgerEntry, s.Direction)
	}
	if !s.Money.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: amount must be positive", ErrInvalidLedgerEntry)
	}
	if s.BalanceBefore.IsNegative() || s.BalanceAfter.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: negative balance", ErrInvalidLedgerEntry)
	}
	var expected Money
	var err error
	if s.Direction == Credit {
		expected, err = s.BalanceBefore.Add(s.Money)
	} else {
		expected, err = s.BalanceBefore.Sub(s.Money)
	}
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvalidLedgerEntry, err)
	}
	if !expected.Equal(s.BalanceAfter) {
		return LedgerEntry{}, fmt.Errorf("%w: balanceAfter %s != %s", ErrInvalidLedgerEntry, s.BalanceAfter, expected)
	}
	if s.CreatedAt.IsZero() {
		return LedgerEntry{}, fmt.Errorf("%w: missing timestamp", ErrInvalidLedgerEntry)
	}
	return LedgerEntry{
		id: s.ID, walletID: s.WalletID, transactionID: s.TransactionID,
		direction: s.Direction, money: s.Money,
		balanceBefore: s.BalanceBefore, balanceAfter: s.BalanceAfter,
		createdAt: s.CreatedAt.UTC(),
	}, nil
}

// RehydrateLedgerEntry reconstrói um lançamento persistido. Aplica as mesmas
// validações de consistência, mas não produz efeitos nem eventos.
func RehydrateLedgerEntry(s LedgerEntryState) (LedgerEntry, error) {
	return NewLedgerEntry(s)
}

func (e LedgerEntry) ID() uuid.UUID            { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID      { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID { return e.transactionID }
func (e LedgerEntry) Direction() Direction     { return e.direction }
func (e LedgerEntry) Money() Money             { return e.money }
func (e LedgerEntry) BalanceBefore() Money     { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() Money      { return e.balanceAfter }
func (e LedgerEntry) CreatedAt() time.Time     { return e.createdAt }
