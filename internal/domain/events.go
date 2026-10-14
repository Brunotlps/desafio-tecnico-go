package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Tipos e versões dos eventos de integração. O construtor de cada evento
// define tipo e versão; chamadores não os informam.
const (
	EventWagerTransactionProcessed        = "WagerTransactionProcessed"
	EventWagerTransactionRejected         = "WagerTransactionRejected"
	EventWalletBalanceChanged             = "WalletBalanceChanged"
	EventWagerTransactionPendingReference = "WagerTransactionPendingReference"

	eventVersion = 1

	AggregateWagerTransaction = "WagerTransaction"
	AggregateWallet           = "Wallet"
)

// Event é o envelope de um evento de integração: um snapshot imutável gravado
// na outbox no mesmo commit financeiro.
type Event struct {
	id            uuid.UUID
	eventType     string
	version       int
	aggregateType string
	aggregateID   uuid.UUID
	correlationID string
	causationID   *uuid.UUID
	occurredAt    time.Time
	data          any
}

func (e Event) ID() uuid.UUID          { return e.id }
func (e Event) Type() string           { return e.eventType }
func (e Event) Version() int           { return e.version }
func (e Event) AggregateType() string  { return e.aggregateType }
func (e Event) AggregateID() uuid.UUID { return e.aggregateID }
func (e Event) CorrelationID() string  { return e.correlationID }
func (e Event) OccurredAt() time.Time  { return e.occurredAt }
func (e Event) CausationID() *uuid.UUID {
	if e.causationID == nil {
		return nil
	}
	c := *e.causationID
	return &c
}

// timeLayout é RFC 3339 em UTC com milissegundos.
const timeLayout = "2006-01-02T15:04:05.000Z"

type envelopeJSON struct {
	EventID       uuid.UUID  `json:"eventId"`
	EventType     string     `json:"eventType"`
	AggregateID   uuid.UUID  `json:"aggregateId"`
	CorrelationID string     `json:"correlationId"`
	CausationID   *uuid.UUID `json:"causationId,omitempty"`
	OccurredAt    string     `json:"occurredAt"`
	Version       int        `json:"version"`
	Data          any        `json:"data"`
}

// Payload serializa o envelope completo (snapshot gravado na outbox).
func (e Event) Payload() ([]byte, error) {
	return json.Marshal(envelopeJSON{
		EventID: e.id, EventType: e.eventType, AggregateID: e.aggregateID,
		CorrelationID: e.correlationID, CausationID: e.causationID,
		OccurredAt: e.occurredAt.UTC().Format(timeLayout), Version: e.version, Data: e.data,
	})
}

func newEvent(id uuid.UUID, eventType, aggregateType string, aggregateID uuid.UUID, correlationID string,
	causationID *uuid.UUID, now time.Time, data any) (Event, error) {
	if id == uuid.Nil || aggregateID == uuid.Nil || correlationID == "" || now.IsZero() {
		return Event{}, fmt.Errorf("%w: %s requires id, aggregate, correlationId and timestamp", ErrInvalidEvent, eventType)
	}
	ev := Event{id: id, eventType: eventType, version: eventVersion, aggregateType: aggregateType,
		aggregateID: aggregateID, correlationID: correlationID, occurredAt: now.UTC(), data: data}
	if causationID != nil {
		c := *causationID
		ev.causationID = &c
	}
	return ev, nil
}

// WagerTransactionProcessedData é o payload de WagerTransactionProcessed. Os
// campos externos são omitidos em eventos de origem interna (OPENING).
type WagerTransactionProcessedData struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	Origin                         Origin    `json:"origin"`
	Kind                           Kind      `json:"kind"`
	WalletID                       uuid.UUID `json:"walletId"`
	PlayerID                       uuid.UUID `json:"playerId"`
	Money                          Money     `json:"money"`
	Balance                        Money     `json:"balance"`
	ProviderID                     string    `json:"providerId,omitempty"`
	ExternalTransactionID          string    `json:"externalTransactionId,omitempty"`
	RoundID                        string    `json:"roundId,omitempty"`
	GameID                         string    `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
}

// WagerTransactionRejectedData é o payload de WagerTransactionRejected.
type WagerTransactionRejectedData struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	Kind                           Kind        `json:"kind"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	Money                          Money       `json:"money"`
	FailureCode                    FailureCode `json:"failureCode"`
	Balance                        Money       `json:"balance"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	RoundID                        string      `json:"roundId"`
	GameID                         string      `json:"gameId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}

// WalletBalanceChangedData é o payload de WalletBalanceChanged.
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID `json:"walletId"`
	TransactionID uuid.UUID `json:"transactionId"`
	Direction     Direction `json:"direction"`
	Money         Money     `json:"money"`
	BalanceBefore Money     `json:"balanceBefore"`
	BalanceAfter  Money     `json:"balanceAfter"`
	WalletVersion int64     `json:"walletVersion"`
}

// WagerTransactionPendingReferenceData é o payload de
// WagerTransactionPendingReference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID `json:"transactionId"`
	Kind                           Kind      `json:"kind"`
	WalletID                       uuid.UUID `json:"walletId"`
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
}

// NewWagerTransactionProcessed cria o evento a partir de uma transação em
// PROCESSED. O agregado é a transação.
func NewWagerTransactionProcessed(id uuid.UUID, correlationID string, causationID *uuid.UUID, t *WagerTransaction, now time.Time) (Event, error) {
	if t.status != StatusProcessed {
		return Event{}, fmt.Errorf("%w: transaction %s is %s", ErrInvalidEvent, t.id, t.status)
	}
	d := WagerTransactionProcessedData{
		TransactionID: t.id, Origin: t.origin, Kind: t.kind, WalletID: t.walletID, PlayerID: t.playerID,
		Money: t.money, Balance: t.resultBalance,
	}
	if t.external != nil {
		d.ProviderID, d.ExternalTransactionID = t.external.ProviderID, t.external.ExternalTransactionID
		d.RoundID, d.GameID = t.external.RoundID, t.external.GameID
		d.ReferenceExternalTransactionID = t.external.ReferenceExternalTransactionID
	}
	return newEvent(id, EventWagerTransactionProcessed, AggregateWagerTransaction, t.id, correlationID, causationID, now, d)
}

// NewWagerTransactionRejected cria o evento a partir de uma transação REJECTED.
func NewWagerTransactionRejected(id uuid.UUID, correlationID string, causationID *uuid.UUID, t *WagerTransaction, now time.Time) (Event, error) {
	if t.status != StatusRejected || t.external == nil {
		return Event{}, fmt.Errorf("%w: transaction %s is not an external rejection", ErrInvalidEvent, t.id)
	}
	d := WagerTransactionRejectedData{
		TransactionID: t.id, Kind: t.kind, WalletID: t.walletID, PlayerID: t.playerID, Money: t.money,
		FailureCode: t.failureCode, Balance: t.resultBalance,
		ProviderID: t.external.ProviderID, ExternalTransactionID: t.external.ExternalTransactionID,
		RoundID: t.external.RoundID, GameID: t.external.GameID,
		ReferenceExternalTransactionID: t.external.ReferenceExternalTransactionID,
	}
	return newEvent(id, EventWagerTransactionRejected, AggregateWagerTransaction, t.id, correlationID, causationID, now, d)
}

// NewWalletBalanceChanged cria o evento a partir de um lançamento e da versão
// da carteira após a movimentação. O agregado é a carteira.
func NewWalletBalanceChanged(id uuid.UUID, correlationID string, causationID *uuid.UUID, entry LedgerEntry, walletVersion int64, now time.Time) (Event, error) {
	if entry.id == uuid.Nil || walletVersion < 1 {
		return Event{}, fmt.Errorf("%w: WalletBalanceChanged requires a ledger entry and wallet version", ErrInvalidEvent)
	}
	d := WalletBalanceChangedData{
		WalletID: entry.walletID, TransactionID: entry.transactionID, Direction: entry.direction,
		Money: entry.money, BalanceBefore: entry.balanceBefore, BalanceAfter: entry.balanceAfter,
		WalletVersion: walletVersion,
	}
	return newEvent(id, EventWalletBalanceChanged, AggregateWallet, entry.walletID, correlationID, causationID, now, d)
}

// NewWagerTransactionPendingReference cria o evento de espera por referência.
func NewWagerTransactionPendingReference(id uuid.UUID, correlationID string, causationID *uuid.UUID, t *WagerTransaction, now time.Time) (Event, error) {
	if t.status != StatusPendingReference || t.external == nil {
		return Event{}, fmt.Errorf("%w: transaction %s is not waiting for a reference", ErrInvalidEvent, t.id)
	}
	d := WagerTransactionPendingReferenceData{
		TransactionID: t.id, Kind: t.kind, WalletID: t.walletID,
		ProviderID: t.external.ProviderID, ExternalTransactionID: t.external.ExternalTransactionID,
		ReferenceExternalTransactionID: t.external.ReferenceExternalTransactionID,
	}
	return newEvent(id, EventWagerTransactionPendingReference, AggregateWagerTransaction, t.id, correlationID, causationID, now, d)
}
