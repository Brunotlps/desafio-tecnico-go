package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/outbox"
)

// OutboxStore implementa outbox.Store: reserva por lease com token e escritas condicionadas à posse.
//
// Todo o tempo é o do BANCO (clock_timestamp()), nunca o dos publishers, e é lido no momento da escrita: um UPDATE que esperou
// por um lock não reaproveita o instante em que começou. Por isso confirmar, reagendar e liberar são dois comandos na mesma
// transação curta — primeiro o lock da linha (onde a espera acontece), depois o UPDATE, cujo predicado de posse é avaliado
// só depois do lock. (Um único UPDATE não bastaria: se o lock é solto sem alterar a linha, o PostgreSQL não reavalia o
// predicado, e uma posse vencida durante a espera seria aceita.)
type OutboxStore struct{ src PoolSource }

// NewOutboxStore cria o repositório de controle da publicação. O pool é resolvido a cada operação.
func NewOutboxStore(src PoolSource) *OutboxStore { return &OutboxStore{src: src} }

var _ outbox.Store = (*OutboxStore)(nil)

func (s *OutboxStore) begin(ctx context.Context) (pgx.Tx, error) {
	p := s.src.Pool()
	if p == nil {
		return nil, fmt.Errorf("%w: %w", app.ErrTransient, errNotStarted)
	}
	tx, err := p.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	return tx, classify(err)
}

// rollback desfaz sem depender do contexto da operação (devolve a conexão sem locks pendentes), com prazo próprio e curto.
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// Reserve escolhe, entre os eventos não publicados, vencidos (next_attempt_at) e sem reserva válida, o mais antigo que NÃO esteja
// travado por outra transação (FOR UPDATE SKIP LOCKED) e o reserva. A elegibilidade é reavaliada sob o lock da linha: uma lista
// antiga não autoriza nada, e uma linha reservada, reagendada ou publicada por outro executor entre a leitura e o lock sai da
// escolha. Eventos em backoff ou com reserva válida não entram na ordenação, então nenhum prefixo deles bloqueia os seguintes.
// attempts conta as reservas (tentativas de publicação iniciadas), inclusive as abandonadas.
func (s *OutboxStore) Reserve(ctx context.Context, lease time.Duration) (*outbox.Reservation, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	token := uuid.New()
	var r outbox.Reservation
	var wallet *string
	err = tx.QueryRow(ctx, `
		WITH cand AS (
			SELECT id FROM outbox_events
			WHERE published_at IS NULL
			  AND next_attempt_at <= clock_timestamp()
			  AND (lease_expires_at IS NULL OR lease_expires_at <= clock_timestamp())
			ORDER BY next_attempt_at, seq
			LIMIT 1
			FOR UPDATE SKIP LOCKED)
		UPDATE outbox_events o
		SET lease_token = $1, lease_expires_at = clock_timestamp() + make_interval(secs => $2), attempts = o.attempts + 1
		FROM cand WHERE o.id = cand.id
		RETURNING o.id, o.seq, o.event_type, o.payload #>> '{data,walletId}', o.payload::text, o.attempts, o.lease_expires_at`,
		token, lease.Seconds()).Scan(&r.EventID, &r.Seq, &r.Type, &wallet, &r.Body, &r.Attempts, &r.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, classify(err)
	}
	if err := tx.Commit(ctx); err != nil {
		// Sem resposta do servidor a reserva pode ter sido confirmada: ela vence sozinha e o evento é recuperado.
		return nil, classify(err)
	}
	r.Token = token
	if wallet != nil {
		r.WalletID = *wallet
	}
	return &r, nil
}

// owned trava a linha e depois testa a posse com o relógio do banco, devolvendo o número de linhas alteradas por update.
// update é o SQL de alteração; recebe $1 = eventId, $2 = token e os argumentos extras a partir de $3.
func (s *OutboxStore) owned(ctx context.Context, r outbox.Reservation, update string, extra ...any) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	// A espera por um lock de outra transação acontece aqui, sob o prazo do contexto.
	var id uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT id FROM outbox_events WHERE id = $1 FOR UPDATE`, r.EventID).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return outbox.ErrLeaseLost
		}
		return classify(err)
	}
	args := append([]any{r.EventID, r.Token}, extra...)
	tag, err := tx.Exec(ctx, update, args...)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return outbox.ErrLeaseLost
	}
	return classify(tx.Commit(ctx))
}

// Confirm marca a publicação (published_at) e encerra a reserva. Exige o token da aquisição corrente e a lease ainda válida.
func (s *OutboxStore) Confirm(ctx context.Context, r outbox.Reservation) error {
	return s.owned(ctx, r, `UPDATE outbox_events
		SET published_at = clock_timestamp(), lease_token = NULL, lease_expires_at = NULL
		WHERE id = $1 AND lease_token = $2 AND published_at IS NULL AND lease_expires_at > clock_timestamp()`)
}

// Retry agenda a próxima tentativa (backoff persistido), registra o diagnóstico e encerra a reserva.
func (s *OutboxStore) Retry(ctx context.Context, r outbox.Reservation, delay time.Duration, reason string) error {
	return s.owned(ctx, r, `UPDATE outbox_events
		SET next_attempt_at = clock_timestamp() + make_interval(secs => $3), last_error = $4, lease_token = NULL, lease_expires_at = NULL
		WHERE id = $1 AND lease_token = $2 AND published_at IS NULL AND lease_expires_at > clock_timestamp()`,
		delay.Seconds(), reason)
}

// Release devolve a reserva sem registrar falha nem alterar a agenda (encerramento do publisher).
func (s *OutboxStore) Release(ctx context.Context, r outbox.Reservation) error {
	return s.owned(ctx, r, `UPDATE outbox_events
		SET lease_token = NULL, lease_expires_at = NULL
		WHERE id = $1 AND lease_token = $2 AND published_at IS NULL AND lease_expires_at > clock_timestamp()`)
}
