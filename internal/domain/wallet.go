package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Wallet é a raiz do agregado financeiro. O saldo só muda por Debit/Credit,
// que devolvem o lançamento correspondente do ledger; a persistência de saldo
// e lançamento deve ocorrer na mesma transação SQL.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// WalletState é o estado persistido usado para reidratar a carteira.
type WalletState struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewWallet cria uma carteira nova com saldo inicial (zero ou positivo) e
// versão 1. O crédito inicial não incrementa a versão: ele é o estado de
// criação, e o lançamento OPENING correspondente é feito por OpenWallet.
func NewWallet(id, playerID uuid.UUID, initial Money, now time.Time) (*Wallet, error) {
	if id == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: missing identity", ErrInvalidWallet)
	}
	if !initial.IsInitialized() {
		return nil, ErrUninitializedMoney
	}
	if initial.IsNegative() {
		return nil, fmt.Errorf("%w: %s", ErrAmountNegative, initial)
	}
	if now.IsZero() {
		return nil, fmt.Errorf("%w: missing timestamp", ErrInvalidWallet)
	}
	return &Wallet{id: id, playerID: playerID, balance: initial, version: 1,
		createdAt: now.UTC(), updatedAt: now.UTC()}, nil
}

// RehydrateWallet reconstrói a carteira persistida, validando invariantes sem
// reaplicar movimentações nem emitir eventos.
func RehydrateWallet(s WalletState) (*Wallet, error) {
	if s.ID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: missing identity", ErrInvalidWallet)
	}
	if !s.Balance.IsInitialized() {
		return nil, ErrUninitializedMoney
	}
	if s.Balance.IsNegative() {
		return nil, fmt.Errorf("%w: negative persisted balance", ErrInvalidWallet)
	}
	if s.Version < 1 {
		return nil, fmt.Errorf("%w: version %d", ErrInvalidWallet, s.Version)
	}
	return &Wallet{id: s.ID, playerID: s.PlayerID, balance: s.Balance, version: s.Version,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC()}, nil
}

func (w *Wallet) ID() uuid.UUID        { return w.id }
func (w *Wallet) PlayerID() uuid.UUID  { return w.playerID }
func (w *Wallet) Balance() Money       { return w.balance }
func (w *Wallet) Currency() Currency   { return w.balance.Currency() }
func (w *Wallet) Version() int64       { return w.version }
func (w *Wallet) CreatedAt() time.Time { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time { return w.updatedAt }

// Debit retira amount do saldo. Falha com ErrInsufficientFunds, sem alterar o
// estado, se o saldo ficaria negativo.
func (w *Wallet) Debit(entryID, transactionID uuid.UUID, amount Money, now time.Time) (LedgerEntry, error) {
	return w.move(Debit, entryID, transactionID, amount, now)
}

// Credit soma amount ao saldo.
func (w *Wallet) Credit(entryID, transactionID uuid.UUID, amount Money, now time.Time) (LedgerEntry, error) {
	return w.move(Credit, entryID, transactionID, amount, now)
}

func (w *Wallet) move(dir Direction, entryID, transactionID uuid.UUID, amount Money, now time.Time) (LedgerEntry, error) {
	if !amount.IsInitialized() {
		return LedgerEntry{}, ErrUninitializedMoney
	}
	if amount.Currency() != w.Currency() {
		return LedgerEntry{}, fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.Currency(), amount.Currency())
	}
	if !amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: movement must be positive", ErrAmountNotAllowed)
	}
	var after Money
	var err error
	if dir == Credit {
		after, err = w.balance.Add(amount)
	} else {
		after, err = w.balance.Sub(amount)
	}
	if err != nil {
		return LedgerEntry{}, err
	}
	if after.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: balance %s, debit %s", ErrInsufficientFunds, w.balance, amount)
	}
	entry, err := NewLedgerEntry(LedgerEntryState{
		ID: entryID, WalletID: w.id, TransactionID: transactionID, Direction: dir,
		Money: amount, BalanceBefore: w.balance, BalanceAfter: after, CreatedAt: now,
	})
	if err != nil {
		return LedgerEntry{}, err
	}
	w.balance = after
	w.version++
	w.updatedAt = now.UTC()
	return entry, nil
}
