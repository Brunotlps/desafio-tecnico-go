package app

import (
	"context"
	"time"

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
	Pending      PendingReferenceRepository
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
	// TryLockForUpdate trava a linha como GetForUpdate, mas sem esperar: devolve
	// ErrWalletBusy se outra transação a mantém travada (o worker de referências
	// não fica atrás de uma carteira ocupada).
	TryLockForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
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
	// Update grava a transição de uma transação não terminal (estado, código de
	// falha, resultado, referência resolvida e instante); nenhum outro campo é
	// atualizável. Falha com ErrConcurrentModification se a transação não estiver
	// mais no estado `from`.
	Update(ctx context.Context, t *domain.WagerTransaction, from domain.Status) error
	// HasProcessedReversal informa se o alvo já recebeu uma REFUND ou ROLLBACK PROCESSED.
	HasProcessedReversal(ctx context.Context, targetID uuid.UUID) (bool, error)
}

type LedgerRepository interface {
	Append(ctx context.Context, e domain.LedgerEntry) error
}

type OutboxRepository interface {
	Append(ctx context.Context, events []domain.Event) error
}

// PendingReference é a agenda durável de uma operação em PENDING_REFERENCE.
// FirstPersistedAt e ExpiresAt são fixados na primeira persistência e nunca mudam.
type PendingReference struct {
	TransactionID    uuid.UUID
	WalletID         uuid.UUID
	FirstPersistedAt time.Time
	ExpiresAt        time.Time
	NextAttemptAt    time.Time
	Attempts         int
	LastAttemptAt    *time.Time
	LastError        string
}

type PendingReferenceRepository interface {
	Insert(ctx context.Context, p PendingReference) error
	// GetOpenForUpdate lê e trava a agenda aberta SEM esperar (SKIP LOCKED): ErrPendingBusy se outra transação segura a linha;
	// ErrPendingNotFound se não existe	// ou já foi concluída.
	GetOpenForUpdate(ctx context.Context, txID uuid.UUID) (PendingReference, error)
	// Reschedule registra uma verificação concluída sem resultado definitivo.
	Reschedule(ctx context.Context, txID uuid.UUID, next, attemptedAt time.Time) error
	// Complete encerra a agenda (a transação passou a um estado terminal).
	Complete(ctx context.Context, txID uuid.UUID, at time.Time) error
	// Defer adia uma agenda aberta cuja tentativa foi abortada por falha de infraestrutura: só agenda (próxima tentativa,
	// contador e diagnóstico); não toca em saldo, ledger nem no estado da transação, e não sobrescreve um reagendamento mais novo
	// de outra instância nem uma agenda já concluída.
	Defer(ctx context.Context, txID uuid.UUID, next, at time.Time, reason string) error
	// Due lista, por ordem de vencimento, agendas abertas cuja próxima tentativa já venceu. É só uma
	// leitura para escolher candidatos: não autoriza movimento (a tentativa revalida tudo sob os locks).
	Due(ctx context.Context, q DueQuery) ([]PendingReference, error)
}

// DueQuery parametriza uma página de candidatos. After continua depois do último candidato examinado (paginação
// por chave, sem OFFSET); ExcludeWallets deixa de fora carteiras já conhecidas como ocupadas na varredura.
type DueQuery struct {
	Now            time.Time
	Limit          int
	After          *DueCursor
	ExcludeWallets []uuid.UUID
}

// DueCursor é a posição (próxima tentativa, transação) do último candidato examinado.
type DueCursor struct {
	NextAttemptAt time.Time
	TransactionID uuid.UUID
}
