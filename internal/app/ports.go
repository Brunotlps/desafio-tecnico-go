package app

import (
	"context"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/domain"
)

// Repositories agrupa os repositórios usados por uma operação. Dentro de
// UnitOfWork.Do, todos compartilham a mesma transação SQL.
type Repositories struct {
	Wallets      WalletRepository
	Transactions TransactionRepository
	Ledger       LedgerRepository
	Outbox       OutboxRepository
}

// UnitOfWork delimita a transação SQL de uma operação financeira.
type UnitOfWork interface {
	// Do executa fn em uma transação e a confirma se fn retornar nil. Falha de
	// COMMIT sem resposta explícita do servidor devolve um erro que satisfaz
	// errors.Is(err, ErrCommitUnknown).
	Do(ctx context.Context, fn func(ctx context.Context, r Repositories) error) error
	// Reader devolve repositórios para leituras fora de transação.
	Reader() Repositories
}

type WalletRepository interface {
	// Insert devolve ErrWalletAlreadyExists para (playerId, currency) repetido.
	Insert(ctx context.Context, w *domain.Wallet) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// GetForUpdate lê a carteira e trava a linha até o fim da transação.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	FindByPlayerAndCurrency(ctx context.Context, playerID uuid.UUID, currency domain.Currency) (*domain.Wallet, error)
	// UpdateBalance grava saldo e versão; falha com ErrConcurrentModification se
	// a versão persistida não for expectedVersion.
	UpdateBalance(ctx context.Context, w *domain.Wallet, expectedVersion int64) error
}

type TransactionRepository interface {
	// Insert devolve ErrDuplicateIdempotencyKey ou ErrDuplicateExternalTransaction
	// quando as unicidades do banco são violadas.
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	// FindByIdentity devolve, numa única consulta (um snapshot), as transações do
	// provedor identificadas pela chave de idempotência e/ou pelo id externo: no
	// máximo uma por identidade. Lista vazia se nenhuma existe.
	FindByIdentity(ctx context.Context, providerID, idempotencyKey, externalID string) ([]*domain.WagerTransaction, error)
	FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
}

type LedgerRepository interface {
	Append(ctx context.Context, e domain.LedgerEntry) error
}

type OutboxRepository interface {
	Append(ctx context.Context, events []domain.Event) error
}
