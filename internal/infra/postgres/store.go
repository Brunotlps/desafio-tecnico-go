// Package postgres implementa a persistência com pgx e SQL explícito.
//
// Money é gravado como BIGINT em unidades mínimas (centavos) e moeda em TEXT;
// nenhum valor monetário passa por ponto flutuante.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/app"
)

// querier é a interface comum a *pgxpool.Pool e pgx.Tx; os repositórios dependem
// apenas dela, o que permite vinculá-los à transação de uma unidade de trabalho.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store implementa app.UnitOfWork sobre um pool pgx.
type Store struct {
	src PoolSource
	// shutdownDeadline, quando definido, limita o rollback ao orçamento restante do encerramento.
	shutdownDeadline atomic.Pointer[time.Time]
	// reconcileHook é um ponto de sincronização de testes (ver SetReconciliationHook); nil em produção.
	reconcileHook atomic.Pointer[func(stage string)]
}

// SetShutdownDeadline informa o prazo global do encerramento: a partir daí, o rollback de
// uma transação cancelada nunca ultrapassa o orçamento restante.
func (s *Store) SetShutdownDeadline(deadline time.Time) { s.shutdownDeadline.Store(&deadline) }

const defaultRollbackTimeout = 5 * time.Second

func (s *Store) rollbackTimeout() time.Duration {
	d := defaultRollbackTimeout
	if p := s.shutdownDeadline.Load(); p != nil {
		if rem := time.Until(*p); rem < d {
			d = max(rem, 100*time.Millisecond)
		}
	}
	return d
}

// NewStore cria a unidade de trabalho. O pool é resolvido a cada operação, o que
// permite que seu ciclo de vida seja gerenciado por Database.
func NewStore(src PoolSource) *Store { return &Store{src: src} }

// pool devolve o pool corrente ou um erro transitório se ainda não iniciado.
func (s *Store) pool() (*pgxpool.Pool, error) {
	if p := s.src.Pool(); p != nil {
		return p, nil
	}
	return nil, fmt.Errorf("%w: %w", app.ErrTransient, errNotStarted)
}

var _ app.UnitOfWork = (*Store)(nil)

func repositories(q querier) app.Repositories {
	return app.Repositories{
		Wallets:      walletRepo{q},
		Transactions: transactionRepo{q},
		Ledger:       ledgerRepo{q},
		Outbox:       outboxRepo{q},
		Pending:      pendingRepo{q},
	}
}

// Reader devolve repositórios que leem diretamente do pool, fora de transação.
func (s *Store) Reader() app.Repositories { return repositories(lazyPool{s}) }

// lazyPool resolve o pool a cada consulta, para que Reader() possa ser obtido
// antes do início do ciclo de vida.
type lazyPool struct{ s *Store }

func (l lazyPool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p, err := l.s.pool()
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return p.Exec(ctx, sql, args...)
}

func (l lazyPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p, err := l.s.pool()
	if err != nil {
		return nil, err
	}
	return p.Query(ctx, sql, args...)
}

func (l lazyPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	p, err := l.s.pool()
	if err != nil {
		return errRow{err}
	}
	return p.QueryRow(ctx, sql, args...)
}

type errRow struct{ err error }

func (e errRow) Scan(...any) error { return e.err }

// Do executa fn em uma transação READ COMMITTED. A coordenação entre escritores
// é feita por lock de linha da carteira (SELECT ... FOR UPDATE) dentro de fn.
// Falha de COMMIT sem resposta do servidor devolve app.ErrCommitUnknown.
func (s *Store) Do(ctx context.Context, fn func(ctx context.Context, r app.Repositories) error) error {
	pool, err := s.pool()
	if err != nil {
		return err
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return classify(err)
	}
	if err := fn(ctx, repositories(tx)); err != nil {
		s.rollback(ctx, tx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// O servidor respondeu com erro: a transação foi desfeita.
			s.rollback(ctx, tx)
			return classify(err)
		}
		if errors.Is(err, pgx.ErrTxClosed) {
			return fmt.Errorf("%w: %w", app.ErrCommitUnknown, err)
		}
		// Sem resposta do servidor (timeout, conexão perdida): o commit pode
		// ter ocorrido. Não é seguro assumir rollback.
		return fmt.Errorf("%w: %w", app.ErrCommitUnknown, err)
	}
	return nil
}

// rollback desfaz a transação mesmo que o contexto da requisição já tenha sido
// cancelado (para devolver a conexão ao pool sem locks pendentes), com um prazo
// próprio de 5 s que, durante o encerramento, respeita o orçamento restante.
func (s *Store) rollback(ctx context.Context, tx pgx.Tx) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.rollbackTimeout())
	defer cancel()
	_ = tx.Rollback(rctx)
}
