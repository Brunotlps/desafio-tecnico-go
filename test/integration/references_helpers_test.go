//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// fakeClock é o relógio injetado no serviço: o TTL e o backoff das pendências são exercitados
// avançando o relógio, sem esperar (nem dormir) minutos.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Now().UTC().Truncate(time.Millisecond)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testRef são os parâmetros de referência usados nos testes de serviço: TTL de 15 min como em
// produção (o relógio falso o atravessa), backoff curto.
var testRef = app.ReferenceConfig{TTL: 15 * time.Minute, BackoffInitial: time.Second, BackoffMax: 4 * time.Second, PhaseTimeout: 5 * time.Second}

// newServiceWith monta o serviço sobre o PostgreSQL real, com papel restrito. wrap permite decorar a
// UnitOfWork (injeção de falhas em pontos observáveis); clock substitui o relógio.
func newServiceWith(t *testing.T, clock *fakeClock, wrap func(app.UnitOfWork) app.UnitOfWork) *app.Service {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), env.runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var uow app.UnitOfWork = postgres.NewStore(postgres.StaticPool(pool))
	if wrap != nil {
		uow = wrap(uow)
	}
	var opts []app.Option
	if clock != nil {
		opts = append(opts, app.WithClock(clock.Now))
	}
	return app.NewService(uow, app.Config{AllowedCurrencies: []domain.Currency{"BRL", "USD"}, Reference: testRef},
		slog.New(slog.NewTextHandler(io.Discard, nil)), opts...)
}

// op monta uma operação do provedor "svc-provider" (chave = id externo).
func op(walletID, playerID, kind, ext, amount, ref string) domain.RawExternalRequest {
	raw := rawKind(walletID, playerID, ext, ext, kind, amount)
	raw.ReferenceExternalTransactionID = ref
	return raw
}

// must submete e falha o teste se houver erro.
func mustSubmit(t *testing.T, svc *app.Service, raw domain.RawExternalRequest) app.SubmitResult {
	t.Helper()
	res, err := svc.Submit(context.Background(), raw, "test-"+raw.ExternalTransactionID)
	if err != nil {
		t.Fatalf("submit %s %s: %v", raw.Kind, raw.ExternalTransactionID, err)
	}
	return res
}

func mustStatus(t *testing.T, res app.SubmitResult, status domain.Status, code domain.FailureCode) {
	t.Helper()
	tx := res.Transaction
	if tx.Status() != status || tx.FailureCode() != code {
		t.Fatalf("%s %s: status=%s code=%q, want %s %q", tx.Kind(), tx.External().ExternalTransactionID, tx.Status(), tx.FailureCode(), status, code)
	}
}

type pendingRow struct {
	First, Expires, Next time.Time
	Attempts             int
	Completed            bool
	Rows                 int
}

func pendingOf(t testing.TB, txID uuid.UUID) pendingRow {
	t.Helper()
	var p pendingRow
	p.Rows = int(queryInt(t, `SELECT count(*) FROM pending_references WHERE transaction_id = $1`, txID))
	if p.Rows == 0 {
		return p
	}
	var completed *time.Time
	if err := testPool.QueryRow(context.Background(),
		`SELECT first_persisted_at, expires_at, next_attempt_at, attempts, completed_at FROM pending_references WHERE transaction_id = $1`, txID).
		Scan(&p.First, &p.Expires, &p.Next, &p.Attempts, &completed); err != nil {
		t.Fatal(err)
	}
	p.Completed = completed != nil
	return p
}

// pendingDue executa uma tentativa para a pendência indicada (a leitura de candidatos é
// exercitada em ProcessDue; aqui o teste controla qual pendência e quando).
func process(t *testing.T, svc *app.Service, txID uuid.UUID) (bool, error) {
	t.Helper()
	ref, err := loadPending(txID)
	if err != nil {
		t.Fatal(err)
	}
	return svc.ProcessPending(context.Background(), ref)
}

func loadPending(txID uuid.UUID) (app.PendingReference, error) {
	var p app.PendingReference
	err := testPool.QueryRow(context.Background(),
		`SELECT transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts FROM pending_references WHERE transaction_id = $1`, txID).
		Scan(&p.TransactionID, &p.WalletID, &p.FirstPersistedAt, &p.ExpiresAt, &p.NextAttemptAt, &p.Attempts)
	return p, err
}

func eventCount(t *testing.T, txID uuid.UUID, eventType string) int64 {
	t.Helper()
	return countRows(t, "outbox_events", "event_type = $1 AND payload->'data'->>'transactionId' = $2", eventType, txID.String())
}

func ledgerOf(t *testing.T, txID uuid.UUID) (n int64, direction string) {
	t.Helper()
	n = countRows(t, "wallet_ledger_entries", "transaction_id = $1", txID)
	if n == 1 {
		if err := testPool.QueryRow(context.Background(), `SELECT direction FROM wallet_ledger_entries WHERE transaction_id = $1`, txID).Scan(&direction); err != nil {
			t.Fatal(err)
		}
	}
	return
}

// wrappedUoW permite sobrescrever Do e/ou os repositórios.
type wrappedUoW struct {
	app.UnitOfWork
	do func(ctx context.Context, inner app.UnitOfWork, fn func(context.Context, app.Repositories) error) error
}

func (w wrappedUoW) Do(ctx context.Context, fn func(context.Context, app.Repositories) error) error {
	return w.do(ctx, w.UnitOfWork, fn)
}
