package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

type walletRepo struct{ q querier }

const walletColumns = `id, player_id, currency, balance, version, created_at, updated_at`

func (r walletRepo) Insert(ctx context.Context, w *domain.Wallet) error {
	_, err := r.q.Exec(ctx,
		`INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), string(w.Currency()), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	return classify(err)
}

func (r walletRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return r.get(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id)
}

func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return r.get(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id)
}

func (r walletRepo) FindByPlayerAndCurrency(ctx context.Context, playerID uuid.UUID, currency domain.Currency) (*domain.Wallet, error) {
	return r.get(ctx, `SELECT `+walletColumns+` FROM wallets WHERE player_id = $1 AND currency = $2`, playerID, string(currency))
}

func (r walletRepo) get(ctx context.Context, sql string, args ...any) (*domain.Wallet, error) {
	var (
		id, playerID         uuid.UUID
		currency             string
		balance, version     int64
		createdAt, updatedAt time.Time
	)
	err := r.q.QueryRow(ctx, sql, args...).Scan(&id, &playerID, &currency, &balance, &version, &createdAt, &updatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrWalletNotFound
	}
	if err != nil {
		return nil, classify(err)
	}
	money, err := domain.FromMinor(balance, domain.Currency(currency))
	if err != nil {
		return nil, err
	}
	return domain.RehydrateWallet(domain.WalletState{
		ID: id, PlayerID: playerID, Balance: money, Version: version, CreatedAt: createdAt, UpdatedAt: updatedAt,
	})
}

func (r walletRepo) UpdateBalance(ctx context.Context, w *domain.Wallet, expectedVersion int64) error {
	tag, err := r.q.Exec(ctx,
		`UPDATE wallets SET balance = $2, version = $3, updated_at = $4 WHERE id = $1 AND version = $5`,
		w.ID(), w.Balance().Minor(), w.Version(), w.UpdatedAt(), expectedVersion)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentModification
	}
	return nil
}
