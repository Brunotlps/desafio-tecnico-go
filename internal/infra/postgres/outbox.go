package postgres

import (
	"context"
	"fmt"

	"desafio-tecnico-go/internal/domain"
)

type outboxRepo struct{ q querier }

// Append grava os eventos como snapshots JSON imutáveis, na mesma transação
// da operação financeira. Nada é publicado aqui: a publicação é de um worker
// separado, depois do commit.
func (r outboxRepo) Append(ctx context.Context, events []domain.Event) error {
	for _, ev := range events {
		payload, err := ev.Payload()
		if err != nil {
			return fmt.Errorf("serialize %s: %w", ev.Type(), err)
		}
		_, err = r.q.Exec(ctx, `INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, causation_id, payload, occurred_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
			ev.ID(), ev.AggregateType(), ev.AggregateID(), ev.Type(), ev.Version(), ev.CorrelationID(),
			ev.CausationID(), payload, ev.OccurredAt())
		if err != nil {
			return classify(err)
		}
	}
	return nil
}
