package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
)

type pendingRepo struct{ q querier }

const pendingColumns = `transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts, last_attempt_at, COALESCE(last_error, '')`

func scanPending(row pgx.Row) (app.PendingReference, error) {
	var p app.PendingReference
	err := row.Scan(&p.TransactionID, &p.WalletID, &p.FirstPersistedAt, &p.ExpiresAt, &p.NextAttemptAt, &p.Attempts, &p.LastAttemptAt, &p.LastError)
	return p, err
}

func (r pendingRepo) Insert(ctx context.Context, p app.PendingReference) error {
	_, err := r.q.Exec(ctx, `INSERT INTO pending_references
		(transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts) VALUES ($1,$2,$3,$4,$5,0)`,
		p.TransactionID, p.WalletID, p.FirstPersistedAt, p.ExpiresAt, p.NextAttemptAt)
	return classify(err)
}

func (r pendingRepo) GetOpenForUpdate(ctx context.Context, txID uuid.UUID) (app.PendingReference, error) {
	p, err := scanPending(r.q.QueryRow(ctx, `SELECT `+pendingColumns+` FROM pending_references
		WHERE transaction_id = $1 AND completed_at IS NULL FOR UPDATE`, txID))
	if errors.Is(err, pgx.ErrNoRows) {
		return app.PendingReference{}, app.ErrPendingNotFound
	}
	return p, classify(err)
}

func (r pendingRepo) Reschedule(ctx context.Context, txID uuid.UUID, next, attemptedAt time.Time) error {
	tag, err := r.q.Exec(ctx, `UPDATE pending_references
		SET next_attempt_at = $2, attempts = attempts + 1, last_attempt_at = $3
		WHERE transaction_id = $1 AND completed_at IS NULL`, txID, next, attemptedAt)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrPendingNotFound
	}
	return nil
}

func (r pendingRepo) Complete(ctx context.Context, txID uuid.UUID, at time.Time) error {
	tag, err := r.q.Exec(ctx, `UPDATE pending_references
		SET completed_at = $2, attempts = attempts + 1, last_attempt_at = $2
		WHERE transaction_id = $1 AND completed_at IS NULL`, txID, at)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrPendingNotFound
	}
	return nil
}

func (r pendingRepo) Due(ctx context.Context, q app.DueQuery) ([]app.PendingReference, error) {
	var afterAt *time.Time
	afterID := uuid.Nil
	if q.After != nil {
		afterAt, afterID = &q.After.NextAttemptAt, q.After.TransactionID
	}
	exclude := q.ExcludeWallets
	if exclude == nil {
		exclude = []uuid.UUID{}
	}
	rows, err := r.q.Query(ctx, `SELECT `+pendingColumns+` FROM pending_references
		WHERE completed_at IS NULL AND next_attempt_at <= $1
		  AND ($3::timestamptz IS NULL OR (next_attempt_at, transaction_id) > ($3, $4))
		  AND NOT (wallet_id = ANY($5::uuid[]))
		ORDER BY next_attempt_at, transaction_id LIMIT $2`, q.Now, q.Limit, afterAt, afterID, exclude)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []app.PendingReference
	for rows.Next() {
		p, err := scanPending(rows)
		if err != nil {
			return nil, classify(err)
		}
		out = append(out, p)
	}
	return out, classify(rows.Err())
}
