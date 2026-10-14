package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// IDGenerator produz identidades novas (UUIDv7 em produção).
type IDGenerator func() uuid.UUID

// WalletOpening é o resultado da abertura de uma carteira: tudo o que precisa
// ser confirmado no mesmo commit. Opening, Entry e os eventos financeiros só
// existem para saldo inicial positivo.
type WalletOpening struct {
	Wallet  *Wallet
	Opening *WagerTransaction
	Entry   *LedgerEntry
	Events  []Event
}

// OpenWallet cria a carteira e, quando o saldo inicial é positivo, a
// transação OPENING em PROCESSED, o lançamento de crédito (saldo anterior zero)
// e os eventos WagerTransactionProcessed e WalletBalanceChanged. Saldo inicial
// zero cria apenas a carteira. A versão da carteira recém-aberta é 1.
func OpenWallet(newID IDGenerator, playerID uuid.UUID, initial Money, correlationID string, now time.Time) (WalletOpening, error) {
	wallet, err := NewWallet(newID(), playerID, initial, now)
	if err != nil {
		return WalletOpening{}, err
	}
	out := WalletOpening{Wallet: wallet}
	if initial.IsZero() {
		return out, nil
	}
	opening, err := NewOpeningTransaction(newID(), wallet.ID(), playerID, initial, now)
	if err != nil {
		return WalletOpening{}, err
	}
	zero, err := Zero(initial.Currency())
	if err != nil {
		return WalletOpening{}, err
	}
	entry, err := NewLedgerEntry(LedgerEntryState{
		ID: newID(), WalletID: wallet.ID(), TransactionID: opening.ID(), Direction: Credit,
		Money: initial, BalanceBefore: zero, BalanceAfter: initial, CreatedAt: now,
	})
	if err != nil {
		return WalletOpening{}, err
	}
	if err := opening.Process(wallet.Balance(), now); err != nil {
		return WalletOpening{}, err
	}
	processed, err := NewWagerTransactionProcessed(newID(), correlationID, nil, opening, now)
	if err != nil {
		return WalletOpening{}, fmt.Errorf("opening event: %w", err)
	}
	causation := processed.ID()
	changed, err := NewWalletBalanceChanged(newID(), correlationID, &causation, entry, wallet.Version(), now)
	if err != nil {
		return WalletOpening{}, fmt.Errorf("opening event: %w", err)
	}
	out.Opening, out.Entry, out.Events = opening, &entry, []Event{processed, changed}
	return out, nil
}
