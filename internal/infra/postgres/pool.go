package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func fsSub() (fs.FS, error) { return fs.Sub(migrationsFS, "migrations") }

func parseConfig(databaseURL string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid DATABASE_URL: %w", err)
	}
	return cfg, nil
}

// PoolConfig reúne os parâmetros do pool.
type PoolConfig struct {
	URL      string
	MaxConns int32
}

// NewPool configura o pool. As conexões são abertas sob demanda.
func NewPool(ctx context.Context, c PoolConfig) (*pgxpool.Pool, error) {
	cfg, err := parseConfig(c.URL)
	if err != nil {
		return nil, err
	}
	if c.MaxConns > 0 {
		cfg.MaxConns = c.MaxConns
	}
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.HealthCheckPeriod = 30 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open pool: %w", err)
	}
	return pool, nil
}

// Database é o ciclo de vida do pool: as conexões só são adquiridas em Start e
// são liberadas em Stop ou, se Start falhar, pelo próprio Start. Assim nenhum
// recurso depende de um OnStop que o Fx não chama quando o OnStart falha.
type Database struct {
	cfg  PoolConfig
	pool atomic.Pointer[pgxpool.Pool]
}

// NewDatabase cria o componente sem abrir conexões.
func NewDatabase(cfg PoolConfig) *Database { return &Database{cfg: cfg} }

// PoolSource fornece o pool corrente.
type PoolSource interface{ Pool() *pgxpool.Pool }

// StaticPool adapta um pool já aberto a PoolSource.
func StaticPool(p *pgxpool.Pool) PoolSource { return staticPool{p} }

type staticPool struct{ p *pgxpool.Pool }

func (s staticPool) Pool() *pgxpool.Pool { return s.p }

// Pool devolve o pool, ou nil antes de Start e depois de Stop.
func (d *Database) Pool() *pgxpool.Pool { return d.pool.Load() }

// Start abre o pool, confirma a conectividade e a versão do schema. Em caso de
// falha libera tudo o que adquiriu antes de devolver o erro.
func (d *Database) Start(ctx context.Context) error {
	pool, err := NewPool(ctx, d.cfg)
	if err != nil {
		return err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(checkCtx); err != nil {
		pool.Close()
		return fmt.Errorf("ping postgres: %w", err)
	}
	if err := CheckSchema(checkCtx, pool); err != nil {
		pool.Close()
		return err
	}
	d.pool.Store(pool)
	return nil
}

// Stop fecha o pool, aguardando as consultas em andamento.
func (d *Database) Stop(context.Context) error {
	if pool := d.pool.Swap(nil); pool != nil {
		pool.Close()
	}
	return nil
}

// Ping verifica o banco para a readiness.
func (d *Database) Ping(ctx context.Context) error {
	pool := d.Pool()
	if pool == nil {
		return errNotStarted
	}
	return pool.Ping(ctx)
}

var errNotStarted = errors.New("database not started")

// CheckSchema falha se o banco não está na versão de schema esperada pelo
// binário. Lê apenas a tabela de versões das migrations (privilégio SELECT).
func CheckSchema(ctx context.Context, pool *pgxpool.Pool) error {
	want, err := ExpectedVersion()
	if err != nil {
		return err
	}
	var got *int64
	err = pool.QueryRow(ctx, `SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&got)
	var pgErr *pgconn.PgError
	switch {
	case errors.As(err, &pgErr) && pgErr.Code == "42P01": // undefined_table
		return fmt.Errorf("database schema is not initialized: run `migrate up`")
	case err != nil:
		return fmt.Errorf("read schema version: %w", err)
	case got == nil || *got != want:
		have := int64(0)
		if got != nil {
			have = *got
		}
		return fmt.Errorf("database schema is at version %d, application expects %d: run `migrate up`", have, want)
	}
	return nil
}
