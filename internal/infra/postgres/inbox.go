package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
)

type inboxRepo struct{ q querier }

func (r inboxRepo) Get(ctx context.Context, consumerName, messageID string) (app.InboxRecord, error) {
	var rec app.InboxRecord
	err := r.q.QueryRow(ctx, `SELECT consumer_name, message_id, body_hash, received_at, completed_at, disposition, financial_replay, transaction_id
		FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, consumerName, messageID).
		Scan(&rec.ConsumerName, &rec.MessageID, &rec.BodyHash, &rec.ReceivedAt, &rec.CompletedAt, &rec.Disposition, &rec.FinancialReplay, &rec.TransactionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return app.InboxRecord{}, app.ErrInboxNotFound
	}
	return rec, classify(err)
}

// Insert grava a conclusão. Uma violação da unicidade da identidade vira app.ErrDuplicateInbox (ver classify); nunca é tratada
// como sucesso aqui: o chamador desfaz a tentativa, reconsulta e compara hash e disposição.
func (r inboxRepo) Insert(ctx context.Context, rec app.InboxRecord) error {
	_, err := r.q.Exec(ctx, `INSERT INTO inbox_messages
		(consumer_name, message_id, body_hash, received_at, completed_at, disposition, financial_replay, transaction_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		rec.ConsumerName, rec.MessageID, rec.BodyHash, rec.ReceivedAt, rec.CompletedAt, rec.Disposition, rec.FinancialReplay, rec.TransactionID)
	return classify(err)
}

type diagnosticRepo struct{ q querier }

func (r diagnosticRepo) Record(ctx context.Context, d app.Diagnostic) error {
	var envID, corr *string
	if d.EnvelopeMessageID != "" {
		envID = &d.EnvelopeMessageID
	}
	if d.CorrelationID != "" {
		corr = &d.CorrelationID
	}
	_, err := r.q.Exec(ctx, `INSERT INTO message_diagnostics
		(consumer_name, source_queue, broker_message_id, body_hash, code, envelope_message_id, correlation_id, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`,
		d.ConsumerName, d.SourceQueue, d.BrokerMessageID, d.BodyHash, d.Code, envID, corr, d.CreatedAt)
	return classify(err)
}

func (r diagnosticRepo) Exists(ctx context.Context, consumerName, sourceQueue, brokerMessageID string, bodyHash []byte) (bool, error) {
	var ok bool
	err := r.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM message_diagnostics
		WHERE consumer_name = $1 AND source_queue = $2 AND broker_message_id = $3 AND body_hash = $4)`,
		consumerName, sourceQueue, brokerMessageID, bodyHash).Scan(&ok)
	return ok, classify(err)
}
