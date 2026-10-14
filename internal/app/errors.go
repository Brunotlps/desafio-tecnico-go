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
)

// WalletExistsError acompanha ErrWalletAlreadyExists com o id da carteira
// existente, para que o chamador interno possa se recuperar.
type WalletExistsError struct{ WalletID uuid.UUID }

func (e *WalletExistsError) Error() string {
	return fmt.Sprintf("%v (walletId=%s)", ErrWalletAlreadyExists, e.WalletID)
}
func (e *WalletExistsError) Is(target error) bool { return target == ErrWalletAlreadyExists }
