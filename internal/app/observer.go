package app

import "errors"

func isErr(err, target error) bool { return errors.Is(err, target) }

// Observer recebe observações operacionais (métricas) em pontos pequenos e claros, SEMPRE depois do commit durável ou da decisão de
// retry. Não é contabilidade financeira: o ledger, a inbox e a outbox são a fonte de verdade. As implementações precisam ser seguras
// para uso concorrente e não podem bloquear. Rótulos: valores de conjuntos limitados e conhecidos (nunca ids nem texto de erro).
type Observer interface {
	// Result: resultado de uma operação confirmada. origin: http, sqs ou references; replay: devolveu o resultado de uma operação já registrada.
	Result(origin, kind, status string, replay bool)
	// InboxOutcome: new, duplicate (mesmo messageId e hash já concluídos) ou conflict (mesmo messageId, outro corpo).
	InboxOutcome(outcome string)
	// Conflict: disputa de concorrência resolvida por nova tentativa (unique_*, wallet_busy, ...).
	Conflict(reason string)
	// Retry: nova tentativa de um componente (references, ...).
	Retry(component, reason string)
	// Reconciliation: result = error quando o cálculo não pôde ser concluído.
	Reconciliation(result string)
}

type nopObserver struct{}

func (nopObserver) Result(string, string, string, bool) {}
func (nopObserver) InboxOutcome(string)                 {}
func (nopObserver) Conflict(string)                     {}
func (nopObserver) Retry(string, string)                {}
func (nopObserver) Reconciliation(string)               {}

// WithObserver registra o observador de métricas.
func WithObserver(o Observer) Option {
	return func(s *Service) {
		if o != nil {
			s.obs = o
		}
	}
}

// conflictReason traduz um sinal de unicidade/concorrência para um motivo limitado.
func conflictReason(err error) string {
	switch {
	case isErr(err, ErrDuplicateIdempotencyKey):
		return "unique_idempotency_key"
	case isErr(err, ErrDuplicateExternalTransaction):
		return "unique_external_transaction"
	case isErr(err, ErrReversalTargetTaken):
		return "unique_reversal_target"
	case isErr(err, ErrDuplicateInbox):
		return "unique_inbox"
	case isErr(err, ErrWalletBusy), isErr(err, ErrPendingBusy):
		return "wallet_busy"
	case isErr(err, ErrConcurrentModification):
		return "concurrent_modification"
	}
	return "other"
}
