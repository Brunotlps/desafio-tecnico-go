package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
	"time"

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
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 300)) // 300 s no total, como o padrão; sonda a cada 1 s
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

// MigratorLockID é a chave do advisory lock de sessão que serializa as operações do Migrator (decisão de versão,
// preparação do rollback, reversão e restauração). É distinta da chave do goose: o Migrator a adquire ANTES de chamar o
// provider, nunca a mesma chave que o provider disputa em outra conexão (o que seria um deadlock).
const MigratorLockID int64 = 1981000005

// exclusive serializa uma operação do Migrator contra os demais migradores, mantendo o lock numa conexão própria. O lock do
// goose (outra chave) é adquirido por dentro, pelo provider, sempre depois deste: a ordem é única, sem deadlock.
func (m *Migrator) exclusive(ctx context.Context, fn func(context.Context) error) error {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, MigratorLockID); err != nil {
		_ = conn.Close()
		return fmt.Errorf("wait for the migrator lock: %w", err)
	}
	defer func() {
		uctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var released bool
		if err := conn.QueryRowContext(uctx, `SELECT pg_advisory_unlock($1)`, MigratorLockID).Scan(&released); err != nil || !released {
			// Estado da sessão desconhecido: descarta a conexão física (o fim da sessão libera o lock).
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}()
	return fn(ctx)
}

// Up aplica todas as migrations pendentes.
func (m *Migrator) Up(ctx context.Context) (res []*goose.MigrationResult, err error) {
	err = m.exclusive(ctx, func(ctx context.Context) error {
		res, err = m.provider.Up(ctx)
		return err
	})
	return res, err
}

// UpTo aplica as migrations pendentes até a versão informada (inclusive).
func (m *Migrator) UpTo(ctx context.Context, version int64) (res []*goose.MigrationResult, err error) {
	err = m.exclusive(ctx, func(ctx context.Context) error {
		res, err = m.provider.UpTo(ctx, version)
		return err
	})
	return res, err
}

// scheduleMigration é a migration que cria a agenda das pendências (00005). Reverter essa migration apaga a
// agenda, que é dado durável (início, vencimento, próxima tentativa, tentativas e diagnóstico).
const scheduleMigration = 5

// Down reverte a última migration aplicada. Se ela for a 00005, antes preserva a agenda das pendências (ver
// preserveSchedule) ou recusa, sem alterar nada, se não puder preservá-la; a reaplicação a restaura com os valores originais.
func (m *Migrator) Down(ctx context.Context) (res *goose.MigrationResult, err error) {
	err = m.exclusive(ctx, func(ctx context.Context) error {
		current, err := m.provider.GetDBVersion(ctx)
		if err != nil {
			return err
		}
		var restore func() error
		if current == scheduleMigration {
			if restore, err = m.preserveSchedule(ctx); err != nil {
				return err
			}
		}
		res, err = m.provider.Down(ctx)
		return withRestoredGrants(err, restore)
	})
	return res, err
}

// DownTo reverte até a versão informada.
//
// Reversão operacional (version > 0): se o percurso inclui a 00005, a agenda das pendências é preservada antes (a
// reaplicação a restaura) ou o rollback é recusado antes de qualquer perda. version = 0 é o reset DELIBERADAMENTE
// destrutivo — apaga todas as tabelas e os dados, como sempre — e descarta também a cópia preservada da agenda.
func (m *Migrator) DownTo(ctx context.Context, version int64) (res []*goose.MigrationResult, err error) {
	err = m.exclusive(ctx, func(ctx context.Context) error {
		current, err := m.provider.GetDBVersion(ctx)
		if err != nil {
			return err
		}
		var restore func() error
		if version > 0 && version < scheduleMigration && current >= scheduleMigration {
			if restore, err = m.preserveSchedule(ctx); err != nil {
				return err
			}
		}
		res, err = m.provider.DownTo(ctx, version)
		if err == nil && version == 0 {
			err = m.dropPreservedSchedule(ctx)
		}
		return withRestoredGrants(err, restore)
	})
	return res, err
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
