// Package app contém os casos de uso. É compartilhado pelas entradas (HTTP e,
// futuramente, SQS) e depende apenas do domínio e das portas de persistência.
package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/domain"
)

// maxConflictRetries limita as repetições internas quando uma unicidade do
// banco revela uma operação concorrente já confirmada.
const maxConflictRetries = 3

// Config reúne a política da aplicação.
type Config struct {
	// AllowedCurrencies lista as moedas aceitas na abertura e nas operações.
	AllowedCurrencies []domain.Currency
}

// Service implementa os casos de uso financeiros.
type Service struct {
	uow     UnitOfWork
	allowed map[domain.Currency]struct{}
	log     *slog.Logger
	now     func() time.Time
	newID   domain.IDGenerator
}

// NewService cria o serviço com relógio e gerador de ids de produção (UUIDv7).
func NewService(uow UnitOfWork, cfg Config, log *slog.Logger) *Service {
	allowed := make(map[domain.Currency]struct{}, len(cfg.AllowedCurrencies))
	for _, c := range cfg.AllowedCurrencies {
		allowed[c] = struct{}{}
	}
	return &Service{
		uow: uow, allowed: allowed, log: log,
		now:   func() time.Time { return time.Now().UTC() },
		newID: func() uuid.UUID { return uuid.Must(uuid.NewV7()) },
	}
}

func (s *Service) checkCurrency(c domain.Currency) error {
	if _, ok := s.allowed[c]; !ok {
		return ErrCurrencyNotSupported
	}
	return nil
}

// OpenWalletCommand é a entrada da abertura de carteira.
type OpenWalletCommand struct {
	PlayerID      string
	Amount        string
	Currency      string
	CorrelationID string
}

// OpenWallet abre uma carteira. Com saldo inicial positivo, carteira, OPENING
// PROCESSED, lançamento de crédito e eventos da outbox são confirmados no mesmo
// commit; com saldo zero, apenas a carteira. Duplicidade de jogador e moeda
// devolve ErrWalletAlreadyExists (como *WalletExistsError).
func (s *Service) OpenWallet(ctx context.Context, cmd OpenWalletCommand) (*domain.Wallet, error) {
	playerID, err := domain.ParseID("playerId", cmd.PlayerID)
	if err != nil {
		return nil, err
	}
	initial, err := domain.ParseMoney(cmd.Amount, cmd.Currency)
	if err != nil {
		return nil, err
	}
	if err := s.checkCurrency(initial.Currency()); err != nil {
		return nil, err
	}
	opening, err := domain.OpenWallet(s.newID, playerID, initial, cmd.CorrelationID, s.now())
	if err != nil {
		return nil, err
	}
	err = s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		if err := r.Wallets.Insert(ctx, opening.Wallet); err != nil {
			return err
		}
		if opening.Opening == nil {
			return nil
		}
		if err := r.Transactions.Insert(ctx, opening.Opening); err != nil {
			return err
		}
		if err := r.Ledger.Append(ctx, *opening.Entry); err != nil {
			return err
		}
		return r.Outbox.Append(ctx, opening.Events)
	})
	switch {
	case errors.Is(err, ErrWalletAlreadyExists):
		existing, ferr := s.uow.Reader().Wallets.FindByPlayerAndCurrency(ctx, playerID, initial.Currency())
		if ferr != nil {
			return nil, err
		}
		return nil, &WalletExistsError{WalletID: existing.ID()}
	case err != nil:
		if errors.Is(err, ErrCommitUnknown) {
			s.log.WarnContext(ctx, "wallet opening commit outcome unknown",
				"walletId", opening.Wallet.ID(), "correlationId", cmd.CorrelationID)
		}
		return nil, err
	}
	s.log.InfoContext(ctx, "wallet opened", "walletId", opening.Wallet.ID(), "correlationId", cmd.CorrelationID)
	return opening.Wallet, nil
}

// GetWallet consulta uma carteira.
func (s *Service) GetWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return s.uow.Reader().Wallets.GetByID(ctx, id)
}

// GetTransaction consulta qualquer transação; uso restrito ao serviço interno.
func (s *Service) GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return s.uow.Reader().Transactions.GetByID(ctx, id)
}

// GetTransactionForProvider consulta uma transação por id interno,
// restringindo-a ao provedor informado. Transações de outros provedores e
// internas respondem ErrTransactionNotFound, sem revelar que existem.
func (s *Service) GetTransactionForProvider(ctx context.Context, providerID string, id uuid.UUID) (*domain.WagerTransaction, error) {
	t, err := s.uow.Reader().Transactions.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if ext := t.External(); ext == nil || ext.ProviderID != providerID {
		return nil, ErrTransactionNotFound
	}
	return t, nil
}

// GetTransactionByExternalID consulta por (providerId, externalTransactionId).
func (s *Service) GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return s.uow.Reader().Transactions.FindByExternalID(ctx, providerID, externalID)
}
