package postgres

import (
	"context"
	"time"

	"desafio-tecnico-go/internal/app"
)

// OutboxStats é o estado durável do backlog de publicação: tudo o que NÃO está publicado (inclusive em retry e reservado).
type OutboxStats struct {
	Pending       int64         // eventos não publicados
	Reserved      int64         // desses, os sob uma lease ainda válida
	OldestPending time.Duration // idade do mais antigo não publicado (0 sem backlog)
}

// Stats lê o backlog sob demanda (uma consulta curta pelo índice parcial dos não publicados; o chamador impõe o prazo).
func (s *OutboxStore) Stats(ctx context.Context) (OutboxStats, error) {
	p := s.src.Pool()
	if p == nil {
		return OutboxStats{}, app.ErrTransient
	}
	var st OutboxStats
	var ageSeconds float64
	err := p.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE lease_expires_at > clock_timestamp()),
		COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - min(created_at)), 0)
		FROM outbox_events WHERE published_at IS NULL`).Scan(&st.Pending, &st.Reserved, &ageSeconds)
	if err != nil {
		return OutboxStats{}, classify(err)
	}
	st.OldestPending = time.Duration(ageSeconds * float64(time.Second))
	return st, nil
}
