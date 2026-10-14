package domain

import (
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
)

// Status é o estado de uma WagerTransaction.
//
// Máquina de estados:
//
//	PENDING ──► PROCESSED            (terminal)
//	PENDING ──► REJECTED             (terminal, regra de negócio)
//	PENDING ──► FAILED               (terminal, falha permanente de infraestrutura)
//	PENDING ──► PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED
//
// Estados terminais não aceitam novas transições.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

func (s Status) valid() bool {
	switch s {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return true
	}
	return false
}

// IsTerminal informa se o estado é final.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Origin distingue transações internas (OPENING) de externas.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// FailureCode é um código estável de rejeição ou falha.
type FailureCode string

const (
	// FailureInsufficientFunds: BET cujo débito deixaria o saldo negativo.
	FailureInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
)

var failureCodeRe = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,63}$`)

// ExternalMeta reúne os metadados que só existem em operações externas.
type ExternalMeta struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
}

// WagerTransaction registra uma operação financeira, interna (OPENING) ou
// externa, e seu resultado.
type WagerTransaction struct {
	id                     uuid.UUID
	origin                 Origin
	kind                   Kind
	status                 Status
	walletID               uuid.UUID
	playerID               uuid.UUID
	money                  Money
	external               *ExternalMeta
	referenceTransactionID *uuid.UUID
	failureCode            FailureCode
	resultBalance          Money // saldo devolvido ao provedor; não inicializado enquanto não há resultado
	createdAt              time.Time
	updatedAt              time.Time
}

// WagerTransactionState é o estado persistido usado para reidratar.
type WagerTransactionState struct {
	ID                     uuid.UUID
	Origin                 Origin
	Kind                   Kind
	Status                 Status
	WalletID               uuid.UUID
	PlayerID               uuid.UUID
	Money                  Money
	External               *ExternalMeta
	ReferenceTransactionID *uuid.UUID
	FailureCode            FailureCode
	ResultBalance          Money
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// Política temporal (criação, transições e reidratação): createdAt e updatedAt
// não são nulos, updatedAt >= createdAt e updatedAt nunca regride. Uma transição
// só é aplicada com um instante que a respeita; caso contrário é recusada, sem
// efeito, com ErrInvalidTimestamp (um ErrInvalidTransaction). O instante igual
// ao da última atualização é válido: etapas do mesmo commit usam o mesmo relógio.
// Uma instância com relógio atrasado em relação à última atualização gravada por
// outra instância deve repetir a transição mais tarde ou informar, como instante,
// o maior entre o seu relógio e UpdatedAt(); o domínio nunca ajusta o instante
// silenciosamente.
func checkTimestamps(createdAt, updatedAt time.Time) error {
	switch {
	case createdAt.IsZero() || updatedAt.IsZero():
		return fmt.Errorf("%w: %w: zero timestamp", ErrInvalidTransaction, ErrInvalidTimestamp)
	case updatedAt.Before(createdAt):
		return fmt.Errorf("%w: %w: updatedAt %s precedes createdAt %s", ErrInvalidTransaction, ErrInvalidTimestamp,
			updatedAt.UTC().Format(time.RFC3339Nano), createdAt.UTC().Format(time.RFC3339Nano))
	}
	return nil
}

// NewExternalTransaction cria uma transação externa em PENDING. A requisição é
// revalidada (incluindo a coerência entre campos e hash): um valor zero ou
// alterado de outra forma nunca produz uma transação.
func NewExternalTransaction(id uuid.UUID, req ExternalRequest, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: incomplete external transaction", ErrInvalidTransaction)
	}
	if err := checkTimestamps(now, now); err != nil {
		return nil, err
	}
	if err := req.check(); err != nil {
		return nil, err
	}
	return &WagerTransaction{
		id: id, origin: OriginExternal, kind: req.f.kind, status: StatusPending,
		walletID: req.f.walletID, playerID: req.f.playerID, money: req.f.money,
		external: &ExternalMeta{
			ProviderID: req.f.providerID, ExternalTransactionID: req.f.externalID,
			IdempotencyKey: req.f.idempotencyKey, PayloadHash: req.payloadHash,
			RoundID: req.f.roundID, GameID: req.f.gameID,
			ReferenceExternalTransactionID: req.f.referenceExternalID,
		},
		createdAt: now.UTC(), updatedAt: now.UTC(),
	}, nil
}

// NewOpeningTransaction cria a transação interna OPENING em PENDING. Só existe
// para saldo inicial positivo.
func NewOpeningTransaction(id, walletID, playerID uuid.UUID, initial Money, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: incomplete opening transaction", ErrInvalidTransaction)
	}
	if err := checkTimestamps(now, now); err != nil {
		return nil, err
	}
	if err := KindOpening.ValidateAmount(initial); err != nil {
		return nil, err
	}
	return &WagerTransaction{
		id: id, origin: OriginInternal, kind: KindOpening, status: StatusPending,
		walletID: walletID, playerID: playerID, money: initial,
		createdAt: now.UTC(), updatedAt: now.UTC(),
	}, nil
}

// RehydrateWagerTransaction reconstrói uma transação persistida. Valida a
// coerência completa do estado (tipo, política de valor, moeda do resultado,
// metadados externos, referência, hash e relação entre estado e resultado), mas
// não executa transições nem emite eventos. Referências recebidas são copiadas.
func RehydrateWagerTransaction(s WagerTransactionState) (*WagerTransaction, error) {
	invalid := func(format string, args ...any) error {
		return fmt.Errorf("%w: "+format, append([]any{ErrInvalidTransaction}, args...)...)
	}
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, invalid("missing identity")
	}
	if !s.Status.valid() {
		return nil, invalid("status %q", s.Status)
	}
	if err := checkTimestamps(s.CreatedAt, s.UpdatedAt); err != nil {
		return nil, err
	}
	if !s.Money.IsInitialized() {
		return nil, ErrUninitializedMoney
	}
	if s.Money.IsNegative() {
		return nil, fmt.Errorf("%w: %s", ErrAmountNegative, s.Money.Amount())
	}
	if err := s.Kind.ValidateAmount(s.Money); err != nil {
		return nil, err
	}

	t := &WagerTransaction{
		id: s.ID, origin: s.Origin, kind: s.Kind, status: s.Status,
		walletID: s.WalletID, playerID: s.PlayerID, money: s.Money,
		failureCode: s.FailureCode, resultBalance: s.ResultBalance,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(),
	}

	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening || s.External != nil || s.ReferenceTransactionID != nil {
			return nil, invalid("internal origin requires OPENING without external metadata or reference")
		}
		if s.Status != StatusProcessed {
			return nil, invalid("OPENING must be PROCESSED")
		}
	case OriginExternal:
		if s.External == nil {
			return nil, invalid("external origin requires external metadata")
		}
		f := externalFields{
			providerID: s.External.ProviderID, externalID: s.External.ExternalTransactionID,
			idempotencyKey: s.External.IdempotencyKey, playerID: s.PlayerID, walletID: s.WalletID,
			roundID: s.External.RoundID, gameID: s.External.GameID, kind: s.Kind, money: s.Money,
			referenceExternalID: s.External.ReferenceExternalTransactionID,
		}
		if err := f.validate(); err != nil {
			return nil, err
		}
		if s.External.PayloadHash != f.hash() {
			return nil, invalid("payload hash does not match the business fields")
		}
		if s.ReferenceTransactionID != nil {
			if *s.ReferenceTransactionID == uuid.Nil || *s.ReferenceTransactionID == s.ID || f.referenceExternalID == "" {
				return nil, invalid("invalid resolved reference")
			}
			ref := *s.ReferenceTransactionID // cópia: o chamador não mantém alias
			t.referenceTransactionID = &ref
		}
		ext := *s.External
		t.external = &ext
	default:
		return nil, invalid("origin %q", s.Origin)
	}

	// Relação entre estado, código de falha e resultado financeiro.
	hasResult := s.ResultBalance.IsInitialized()
	if hasResult {
		if s.ResultBalance.Currency() != s.Money.Currency() {
			return nil, fmt.Errorf("%w: transaction %s, result %s", ErrCurrencyMismatch, s.Money.Currency(), s.ResultBalance.Currency())
		}
		if s.ResultBalance.IsNegative() {
			return nil, invalid("negative result balance")
		}
	}
	hasCode := s.FailureCode != ""
	if hasCode && !failureCodeRe.MatchString(string(s.FailureCode)) {
		return nil, invalid("failure code %q", s.FailureCode)
	}
	switch s.Status {
	case StatusPending, StatusPendingReference:
		if hasCode || hasResult {
			return nil, invalid("%s must not have a result or failure code", s.Status)
		}
	case StatusProcessed:
		if hasCode || !hasResult {
			return nil, invalid("PROCESSED requires a result balance and no failure code")
		}
		if t.needsResolvedReference() && t.referenceTransactionID == nil {
			return nil, invalid("PROCESSED %s requires its reference to be resolved", s.Kind)
		}
	case StatusRejected:
		if !hasCode || !hasResult {
			return nil, invalid("REJECTED requires a failure code and a result balance")
		}
	case StatusFailed:
		if !hasCode {
			return nil, invalid("FAILED requires a failure code")
		}
	}
	if s.Status == StatusPendingReference && (s.External == nil || s.External.ReferenceExternalTransactionID == "") {
		return nil, invalid("PENDING_REFERENCE requires a reference")
	}
	return t, nil
}

func (t *WagerTransaction) ID() uuid.UUID            { return t.id }
func (t *WagerTransaction) Origin() Origin           { return t.origin }
func (t *WagerTransaction) Kind() Kind               { return t.kind }
func (t *WagerTransaction) Status() Status           { return t.status }
func (t *WagerTransaction) WalletID() uuid.UUID      { return t.walletID }
func (t *WagerTransaction) PlayerID() uuid.UUID      { return t.playerID }
func (t *WagerTransaction) Money() Money             { return t.money }
func (t *WagerTransaction) FailureCode() FailureCode { return t.failureCode }

// ReferenceTransactionID devolve uma cópia da referência interna resolvida (nil
// se não houver); alterar o valor devolvido não afeta a transação.
func (t *WagerTransaction) ReferenceTransactionID() *uuid.UUID {
	if t.referenceTransactionID == nil {
		return nil
	}
	ref := *t.referenceTransactionID
	return &ref
}
func (t *WagerTransaction) CreatedAt() time.Time { return t.createdAt }
func (t *WagerTransaction) UpdatedAt() time.Time { return t.updatedAt }

// ResultBalance é o saldo observado quando a operação foi concluída; é o valor
// que um replay devolve. Não inicializado enquanto não houver resultado.
func (t *WagerTransaction) ResultBalance() Money { return t.resultBalance }

// External devolve uma cópia dos metadados externos, ou nil para OPENING.
func (t *WagerTransaction) External() *ExternalMeta {
	if t.external == nil {
		return nil
	}
	ext := *t.external
	return &ext
}

// checkTransition valida a transição para `to` no instante `now` sem alterar a
// entidade: estado de origem, destino permitido e política temporal.
func (t *WagerTransaction) checkTransition(to Status, now time.Time) error {
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: %s", ErrTerminalTransaction, t.status)
	}
	switch {
	case t.status == StatusPending && (to == StatusProcessed || to == StatusRejected || to == StatusFailed || to == StatusPendingReference):
	case t.status == StatusPendingReference && (to == StatusProcessed || to == StatusRejected || to == StatusFailed):
	default:
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, to)
	}
	return checkTimestamps(t.createdAt, now) // zero ou anterior à criação
}

// checkAfterUpdate garante que o histórico não regride: o instante da transição
// não pode preceder a última atualização.
func (t *WagerTransaction) checkAfterUpdate(now time.Time) error {
	return checkTimestamps(t.updatedAt, now)
}

// transitionTo valida e aplica a mudança de estado; nada é alterado se falhar.
func (t *WagerTransaction) transitionTo(to Status, now time.Time) error {
	if err := t.checkTransition(to, now); err != nil {
		return err
	}
	if err := t.checkAfterUpdate(now); err != nil {
		return err
	}
	t.status = to
	t.updatedAt = now.UTC()
	return nil
}

// Process conclui a transação com sucesso, registrando o saldo resultante.
func (t *WagerTransaction) Process(balance Money, now time.Time) error {
	if err := t.checkBalance(balance); err != nil {
		return err
	}
	if err := t.requireResolvedReference(); err != nil {
		return err
	}
	if err := t.transitionTo(StatusProcessed, now); err != nil {
		return err
	}
	t.resultBalance = balance
	return nil
}

// Reject conclui a transação como rejeição de negócio, sem movimentação. O
// saldo registrado é o observado no momento da decisão.
func (t *WagerTransaction) Reject(code FailureCode, balance Money, now time.Time) error {
	if !failureCodeRe.MatchString(string(code)) {
		return fmt.Errorf("%w: failure code %q", ErrInvalidTransaction, code)
	}
	if err := t.checkBalance(balance); err != nil {
		return err
	}
	if err := t.transitionTo(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = code
	t.resultBalance = balance
	return nil
}

// AwaitReference registra a espera por uma referência ainda indisponível.
func (t *WagerTransaction) AwaitReference(now time.Time) error {
	if t.kind.referencePolicy() == referenceForbidden || t.external == nil || t.external.ReferenceExternalTransactionID == "" {
		return fmt.Errorf("%w: %s has no reference to wait for", ErrInvalidTransition, t.kind)
	}
	return t.transitionTo(StatusPendingReference, now)
}

// Fail registra falha permanente de infraestrutura para auditoria.
func (t *WagerTransaction) Fail(code FailureCode, now time.Time) error {
	if !failureCodeRe.MatchString(string(code)) {
		return fmt.Errorf("%w: failure code %q", ErrInvalidTransaction, code)
	}
	if err := t.transitionTo(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = code
	return nil
}

func (t *WagerTransaction) checkBalance(b Money) error {
	if !b.IsInitialized() {
		return ErrUninitializedMoney
	}
	if b.Currency() != t.money.Currency() {
		return fmt.Errorf("%w: transaction %s, balance %s", ErrCurrencyMismatch, t.money.Currency(), b.Currency())
	}
	if b.IsNegative() {
		return fmt.Errorf("%w: negative result balance", ErrInvalidTransaction)
	}
	return nil
}
