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

// pendingColumnsP são as mesmas colunas, qualificadas com o alias `p` (consultas com junção).
const pendingColumnsP = `p.transaction_id, p.wallet_id, p.first_persisted_at, p.expires_at, p.next_attempt_at, p.attempts, p.last_attempt_at, COALESCE(p.last_error, '')`

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
	// Candidatos: pendências vencidas em ordem estável, SEM as carteiras travadas por outra transação. `FOR KEY SHARE ... SKIP
	// LOCKED` na carteira faz o banco pular, dentro da própria consulta, as carteiras que uma transação segura com FOR UPDATE
	// (o mesmo conflito que TryLockForUpdate enxerga), de modo que um prefixo ocupado, por maior que seja, não esconde as
	// carteiras independentes que vêm depois dele. É só uma leitura: o lock dura o tempo da consulta (não há espera, nem lock
	// durante o backoff) e a elegibilidade e a carteira são revalidadas sob os locks da tentativa.
	rows, err := r.q.Query(ctx, `SELECT `+pendingColumnsP+` FROM pending_references p
		JOIN wallets w ON w.id = p.wallet_id
		WHERE p.completed_at IS NULL AND p.next_attempt_at <= $1
		  AND ($3::timestamptz IS NULL OR (p.next_attempt_at, p.transaction_id) > ($3, $4))
		  AND NOT (p.wallet_id = ANY($5::uuid[]))
		ORDER BY p.next_attempt_at, p.transaction_id LIMIT $2
		FOR KEY SHARE OF w SKIP LOCKED`, q.Now, q.Limit, afterAt, afterID, exclude)
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
