package app

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Erros da camada de aplicação. Erros de validação e de regra de negócio do
// domínio (domain.Err*) atravessam esta camada sem tradução.
var (
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrWalletAlreadyExists = errors.New("a wallet already exists for this player and currency")
	ErrTransactionNotFound = errors.New("transaction not found")

	// ErrWalletMismatch: a carteira existe, mas jogador ou moeda informados não
	// pertencem a ela. É erro de entrada, não persistido.
	ErrWalletMismatch = errors.New("wallet does not match player or currency")

	// ErrIdempotencyKeyReused: mesma chave de idempotência com conteúdo diferente.
	ErrIdempotencyKeyReused = errors.New("idempotency key reused with a different payload")
	// ErrExternalTransactionReused: (providerId, externalTransactionId) já usado
	// sob outra chave de idempotência.
	ErrExternalTransactionReused = errors.New("external transaction already registered under another idempotency key")

	// ErrKindNotSupported: tipo válido segundo o enunciado, ainda sem
	// processamento implementado. Nada é persistido.
	ErrKindNotSupported = errors.New("transaction kind not supported yet")
	// ErrCurrencyNotSupported: moeda fora da lista configurada.
	ErrCurrencyNotSupported = errors.New("currency not supported")

	// ErrTransient marca falhas temporárias de infraestrutura: a operação pode
	// ser repetida com a mesma identidade.
	ErrTransient = errors.New("temporary infrastructure failure")
	// ErrCommitUnknown: o resultado do COMMIT é desconhecido. A transação pode ou
	// não ter sido confirmada; a recuperação segura é repetir a mesma operação
	// com a mesma chave de idempotência.
	ErrCommitUnknown = errors.New("commit outcome unknown")

	// Sinais da camada de persistência, tratados dentro do caso de uso.
	ErrDuplicateIdempotencyKey      = errors.New("duplicate idempotency key")
	ErrDuplicateExternalTransaction = errors.New("duplicate external transaction")
	ErrConcurrentModification       = errors.New("wallet was modified concurrently")
	// ErrReversalTargetTaken: a unicidade do banco (uma reversão PROCESSED por alvo) revelou
	// uma reversão concorrente já confirmada; uma nova tentativa a enxerga e rejeita.
	ErrReversalTargetTaken = errors.New("reversal target already reversed")
	// ErrWalletBusy: a carteira está travada por outra transação (TryLockForUpdate).
	ErrWalletBusy = errors.New("wallet is locked by another transaction")
	// ErrPendingBusy: a agenda aberta existe, mas outra transação segura a sua linha (a tentativa não espera por ela).
	ErrPendingBusy = errors.New("pending reference schedule is locked by another transaction")
	// ErrInvalidCursor / ErrInvalidLimit: parâmetros de paginação do ledger inválidos (entrada, nunca uma consulta ampliada).
	ErrInvalidCursor = errors.New("invalid cursor")
	ErrInvalidLimit  = errors.New("invalid limit")
	// ErrReconciliationFailed: a reconstrução não pôde ser concluída com segurança (limite numérico, moeda inconsistente
	// ou leitura impossível de interpretar). Nunca é apresentada como resultado consistente nem como divergência.
	ErrReconciliationFailed = errors.New("reconciliation could not be computed")
	// ErrLedgerNotConfigured: o serviço foi montado sem o leitor do ledger.
	ErrLedgerNotConfigured = errors.New("ledger reader not configured")
	// ErrInboxNotFound: não há registro de inbox para a identidade. ErrDuplicateInbox: outra transação confirmou a mesma
	// identidade (consumerName, messageId); uma nova tentativa a enxerga.
	ErrInboxNotFound  = errors.New("no inbox record for this message")
	ErrDuplicateInbox = errors.New("duplicate inbox identity")
	// ErrPendingNotFound: não há agenda aberta para a transação.
	ErrPendingNotFound = errors.New("no open pending reference schedule")
)

// WalletExistsError acompanha ErrWalletAlreadyExists com o id da carteira
// existente, para que o chamador interno possa se recuperar.
type WalletExistsError struct{ WalletID uuid.UUID }

func (e *WalletExistsError) Error() string {
	return fmt.Sprintf("%v (walletId=%s)", ErrWalletAlreadyExists, e.WalletID)
}
func (e *WalletExistsError) Is(target error) bool { return target == ErrWalletAlreadyExists }
