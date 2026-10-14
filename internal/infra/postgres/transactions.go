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

type transactionRepo struct{ q querier }

const txColumns = `id, origin, kind, status, wallet_id, player_id, amount, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, result_balance,
	created_at, updated_at`

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (r transactionRepo) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	var provider, external, key, hash, round, game, refExternal *string
	if ext := t.External(); ext != nil {
		provider, external, key, hash = &ext.ProviderID, &ext.ExternalTransactionID, &ext.IdempotencyKey, &ext.PayloadHash
		round, game, refExternal = &ext.RoundID, &ext.GameID, nullable(ext.ReferenceExternalTransactionID)
	}
	var result *int64
	if rb := t.ResultBalance(); rb.IsInitialized() {
		m := rb.Minor()
		result = &m
	}
	_, err := r.q.Exec(ctx, `INSERT INTO wager_transactions (`+txColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
		t.ID(), string(t.Origin()), string(t.Kind()), string(t.Status()), t.WalletID(), t.PlayerID(),
		t.Money().Minor(), string(t.Money().Currency()),
		provider, external, key, hash, round, game, refExternal, t.ReferenceTransactionID(),
		nullable(string(t.FailureCode())), result, t.CreatedAt(), t.UpdatedAt())
	return classify(err)
}

func (r transactionRepo) Update(ctx context.Context, t *domain.WagerTransaction, from domain.Status) error {
	var result *int64
	if rb := t.ResultBalance(); rb.IsInitialized() {
		m := rb.Minor()
		result = &m
	}
	tag, err := r.q.Exec(ctx, `UPDATE wager_transactions
		SET status = $2, failure_code = $3, result_balance = $4, reference_transaction_id = $5, updated_at = $6
		WHERE id = $1 AND status = $7`,
		t.ID(), string(t.Status()), nullable(string(t.FailureCode())), result, t.ReferenceTransactionID(), t.UpdatedAt(), string(from))
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentModification
	}
	return nil
}

func (r transactionRepo) HasProcessedReversal(ctx context.Context, targetID uuid.UUID) (bool, error) {
	var exists bool
	err := r.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND kind IN ('REFUND', 'ROLLBACK') AND status = 'PROCESSED')`, targetID).Scan(&exists)
	return exists, classify(err)
}

func (r transactionRepo) GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return r.get(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1`, id)
}

func (r transactionRepo) FindByIdentity(ctx context.Context, providerID, key, externalID string) ([]*domain.WagerTransaction, error) {
	rows, err := r.q.Query(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`, providerID, key, externalID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*domain.WagerTransaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	return out, nil
}

func (r transactionRepo) FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return r.get(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID)
}

func (r transactionRepo) get(ctx context.Context, sql string, args ...any) (*domain.WagerTransaction, error) {
	t, err := scanTransaction(r.q.QueryRow(ctx, sql, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrTransactionNotFound
	}
	return t, err
}

// scanTransaction lê uma linha na ordem de txColumns e reidrata a transação.
func scanTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		id, walletID, playerID                                  uuid.UUID
		origin, kind, status, currency                          string
		amount                                                  int64
		provider, external, key, hash, round, game, refExternal *string
		refTx                                                   *uuid.UUID
		failure                                                 *string
		result                                                  *int64
		createdAt, updatedAt                                    time.Time
	)
	err := row.Scan(&id, &origin, &kind, &status, &walletID, &playerID, &amount, &currency,
		&provider, &external, &key, &hash, &round, &game, &refExternal, &refTx, &failure, &result, &createdAt, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		return nil, classify(err)
	}
	cur := domain.Currency(currency)
	money, err := domain.FromMinor(amount, cur)
	if err != nil {
		return nil, err
	}
	st := domain.WagerTransactionState{
		ID: id, Origin: domain.Origin(origin), Kind: domain.Kind(kind), Status: domain.Status(status),
		WalletID: walletID, PlayerID: playerID, Money: money, ReferenceTransactionID: refTx,
		CreatedAt: createdAt, UpdatedAt: updatedAt,
	}
	if failure != nil {
		st.FailureCode = domain.FailureCode(*failure)
	}
	if result != nil {
		if st.ResultBalance, err = domain.FromMinor(*result, cur); err != nil {
			return nil, err
		}
	}
	if provider != nil {
		st.External = &domain.ExternalMeta{
			ProviderID: *provider, ExternalTransactionID: deref(external), IdempotencyKey: deref(key),
			PayloadHash: deref(hash), RoundID: deref(round), GameID: deref(game),
			ReferenceExternalTransactionID: deref(refExternal),
		}
	}
	return domain.RehydrateWagerTransaction(st)
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
