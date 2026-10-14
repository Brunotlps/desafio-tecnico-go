package domain

import (
	"fmt"
	"time"
)

// Códigos de falha de referências e reversões. São estáveis e fazem parte do
// contrato (README, "Códigos de falha").
//
//	REFERENCE_NOT_FOUND        resultado definitivo: referência ausente após o TTL
//	REFERENCE_PENDING_TIMEOUT  resultado definitivo: referência existente ainda pendente após o TTL
//	REFERENCE_NOT_PROCESSED    resultado definitivo: referência REJECTED ou FAILED
//	REFERENCE_MISMATCH         resultado definitivo: escopo (provedor, jogador, carteira, moeda, rodada),
//	                           tipo ou valor incompatíveis com a referência
//	ALREADY_REVERSED           resultado definitivo: o alvo já recebeu uma reversão bem-sucedida
//	REVERSAL_INSUFFICIENT_FUNDS resultado definitivo: o débito da reversão excederia o saldo (distinto de
//	                           INSUFFICIENT_FUNDS, que é de BET)
const (
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferencePendingTimeout   FailureCode = "REFERENCE_PENDING_TIMEOUT"
	FailureReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureAlreadyReversed           FailureCode = "ALREADY_REVERSED"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
)

// IsReversal informa se o tipo é uma reversão (REFUND ou ROLLBACK).
func (k Kind) IsReversal() bool { return k == KindRefund || k == KindRollback }

// canReference informa se uma operação deste tipo pode referenciar uma
// operação do tipo target: REFUND → BET; ROLLBACK → BET, WIN ou REFUND; WIN → BET.
func (k Kind) canReference(target Kind) bool {
	switch k {
	case KindRefund, KindWin:
		return target == KindBet
	case KindRollback:
		return target == KindBet || target == KindWin || target == KindRefund
	}
	return false
}

// ReversalDirection devolve o sentido do movimento de uma reversão: REFUND
// credita; ROLLBACK é o movimento contrário ao do alvo (BET → crédito;
// WIN e REFUND → débito).
func ReversalDirection(kind, target Kind) (Direction, error) {
	if !kind.IsReversal() || !kind.canReference(target) {
		return "", fmt.Errorf("%w: %s cannot reference %s", ErrInvalidTransition, kind, target)
	}
	if kind == KindRefund || target == KindBet {
		return Credit, nil
	}
	return Debit, nil
}

// ReferenceOutcome é o resultado da avaliação de uma referência encontrada.
type ReferenceOutcome int

const (
	// ReferenceUsable: a referência é elegível e está PROCESSED; a operação pode
	// seguir pelas suas regras financeiras.
	ReferenceUsable ReferenceOutcome = iota + 1
	// ReferenceUnavailable: a referência é elegível, mas ainda está PENDING ou
	// PENDING_REFERENCE.
	ReferenceUnavailable
	// ReferenceRejected: a referência não serve; Code diz por quê (definitivo).
	ReferenceRejected
)

// ReferenceAssessment é o veredito sobre uma referência encontrada.
type ReferenceAssessment struct {
	Outcome ReferenceOutcome
	Code    FailureCode // preenchido com ReferenceRejected
}

// AssessReference avalia a referência encontrada para esta operação. Valida,
// nesta ordem: escopo (provedor, id externo referenciado, jogador, carteira,
// moeda e rodada), tipo elegível e, para reversões, a igualdade integral do
// valor — incompatibilidades são REFERENCE_MISMATCH, independentemente do
// estado do alvo, pois esses campos são imutáveis —; depois o estado do alvo.
// Não altera nenhuma entidade.
func (t *WagerTransaction) AssessReference(target *WagerTransaction) (ReferenceAssessment, error) {
	if t.external == nil || t.external.ReferenceExternalTransactionID == "" {
		return ReferenceAssessment{}, fmt.Errorf("%w: %s has no reference to assess", ErrInvalidTransition, t.kind)
	}
	if target == nil {
		return ReferenceAssessment{}, fmt.Errorf("%w: missing reference target", ErrInvalidTransaction)
	}
	mismatch := ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceMismatch}
	te := target.external
	if target.id == t.id || te == nil ||
		te.ProviderID != t.external.ProviderID || te.ExternalTransactionID != t.external.ReferenceExternalTransactionID ||
		target.playerID != t.playerID || target.walletID != t.walletID ||
		target.money.Currency() != t.money.Currency() || te.RoundID != t.external.RoundID ||
		!t.kind.canReference(target.kind) {
		return mismatch, nil
	}
	if t.kind.IsReversal() && !target.money.Equal(t.money) {
		return mismatch, nil
	}
	switch target.status {
	case StatusProcessed:
		return ReferenceAssessment{Outcome: ReferenceUsable}, nil
	case StatusPending, StatusPendingReference:
		return ReferenceAssessment{Outcome: ReferenceUnavailable}, nil
	}
	return ReferenceAssessment{Outcome: ReferenceRejected, Code: FailureReferenceNotProcessed}, nil
}

// ResolveReference registra a referência interna resolvida, depois de
// confirmar que ela é utilizável (AssessReference == ReferenceUsable). Só vale
// para transação não terminal; o instante respeita a política temporal e, em
// caso de erro, nada é alterado. Resolver de novo para o mesmo alvo é um no-op;
// para outro alvo é recusado.
func (t *WagerTransaction) ResolveReference(target *WagerTransaction, now time.Time) error {
	if err := t.checkTransition(t.status, now); err != nil && t.status.IsTerminal() {
		return err
	}
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: %s", ErrTerminalTransaction, t.status)
	}
	if err := checkTimestamps(t.createdAt, now); err != nil {
		return err
	}
	if err := t.checkAfterUpdate(now); err != nil {
		return err
	}
	a, err := t.AssessReference(target)
	if err != nil {
		return err
	}
	if a.Outcome != ReferenceUsable {
		return fmt.Errorf("%w: the reference is not usable", ErrInvalidTransition)
	}
	if t.referenceTransactionID != nil {
		if *t.referenceTransactionID != target.id {
			return fmt.Errorf("%w: reference already resolved to another transaction", ErrInvalidTransition)
		}
		return nil
	}
	id := target.id
	t.referenceTransactionID = &id
	t.updatedAt = now.UTC()
	return nil
}

// needsResolvedReference: reversões e WIN com referência informada só podem ser
// concluídas com a referência interna resolvida.
func (t *WagerTransaction) needsResolvedReference() bool {
	if t.kind.IsReversal() {
		return true
	}
	return t.kind == KindWin && t.external != nil && t.external.ReferenceExternalTransactionID != ""
}

// RequireResolvedReference é usada pelas transições de sucesso.
func (t *WagerTransaction) requireResolvedReference() error {
	if t.needsResolvedReference() && t.referenceTransactionID == nil {
		return fmt.Errorf("%w: %s requires its reference to be resolved before it is processed", ErrInvalidTransition, t.kind)
	}
	return nil
}
