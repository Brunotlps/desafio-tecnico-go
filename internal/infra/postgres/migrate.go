package postgres

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrator aplica e reverte migrations versionadas (goose). Um lock de sessão
// do PostgreSQL serializa execuções concorrentes.
type Migrator struct {
	db       *sql.DB
	provider *goose.Provider
}

// NewMigrator abre uma conexão database/sql dedicada às migrations.
func NewMigrator(databaseURL string) (*Migrator, error) {
	cfg, err := parseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	db := stdlib.OpenDB(*cfg.ConnConfig)
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	sub, err := fsSub()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, sub, goose.WithSessionLocker(locker))
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("goose provider: %w", err)
	}
	return &Migrator{db: db, provider: p}, nil
}

func (m *Migrator) Close() error { return m.db.Close() }

// Up aplica todas as migrations pendentes.
func (m *Migrator) Up(ctx context.Context) ([]*goose.MigrationResult, error) {
	return m.provider.Up(ctx)
}

// UpTo aplica as migrations pendentes até a versão informada (inclusive).
func (m *Migrator) UpTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	return m.provider.UpTo(ctx, version)
}

// Down reverte a última migration aplicada.
func (m *Migrator) Down(ctx context.Context) (*goose.MigrationResult, error) {
	return m.provider.Down(ctx)
}

// DownTo reverte até a versão informada (0 reverte todas).
func (m *Migrator) DownTo(ctx context.Context, version int64) ([]*goose.MigrationResult, error) {
	return m.provider.DownTo(ctx, version)
}

// Status lista as migrations e seu estado.
func (m *Migrator) Status(ctx context.Context) ([]*goose.MigrationStatus, error) {
	return m.provider.Status(ctx)
}

// ExpectedVersion é a versão mais recente embutida no binário (maior prefixo
// numérico dos arquivos de migration).
func ExpectedVersion() (int64, error) {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return 0, err
	}
	var latest int64
	for _, e := range entries {
		prefix, _, ok := strings.Cut(e.Name(), "_")
		if !ok {
			continue
		}
		if v, err := strconv.ParseInt(prefix, 10, 64); err == nil && v > latest {
			latest = v
		}
	}
	if latest == 0 {
		return 0, fmt.Errorf("no migrations embedded")
	}
	return latest, nil
}
