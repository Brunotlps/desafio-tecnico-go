//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/auth"
	"desafio-tecnico-go/internal/infra/httpapi"
	"desafio-tecnico-go/internal/infra/postgres"
)

type faultMode int32

const (
	faultNone             faultMode = iota
	faultOutboxFails                // falha na última escrita da transação
	faultTransactionFails           // falha depois de o saldo ter sido atualizado
	faultLedgerSkipped              // o ledger é "esquecido": o banco precisa recusar o commit
	faultCommitAckLost              // o COMMIT acontece, mas o chamador recebe erro
)

type faultyUoW struct {
	inner app.UnitOfWork
	mode  *atomic.Int32
}

type failingOutbox struct{ app.OutboxRepository }

func (failingOutbox) Append(context.Context, []domain.Event) error {
	return fmt.Errorf("%w: injected outbox failure", app.ErrTransient)
}

type failingTransactions struct{ app.TransactionRepository }

func (failingTransactions) Insert(context.Context, *domain.WagerTransaction) error {
	return fmt.Errorf("%w: injected insert failure", app.ErrTransient)
}

type skippedLedger struct{ app.LedgerRepository }

func (skippedLedger) Append(context.Context, domain.LedgerEntry) error { return nil }

func (f faultyUoW) Do(ctx context.Context, fn func(ctx context.Context, r app.Repositories) error) error {
	mode := faultMode(f.mode.Load())
	err := f.inner.Do(ctx, func(ctx context.Context, r app.Repositories) error {
		switch mode {
		case faultOutboxFails:
			r.Outbox = failingOutbox{r.Outbox}
		case faultTransactionFails:
			r.Transactions = failingTransactions{r.Transactions}
		case faultLedgerSkipped:
			r.Ledger = skippedLedger{r.Ledger}
		}
		return fn(ctx, r)
	})
	if err == nil && mode == faultCommitAckLost {
		return fmt.Errorf("%w: connection reset after COMMIT", app.ErrCommitUnknown)
	}
	return err
}

func (f faultyUoW) Reader() app.Repositories { return f.inner.Reader() }

// faultyServer monta o serviço sobre o PostgreSQL real, com autenticação real
// (Keycloak) e uma unidade de trabalho que injeta falhas sob demanda.
func faultyServer(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), env.runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	mode := &atomic.Int32{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := app.NewService(faultyUoW{inner: postgres.NewStore(postgres.StaticPool(pool)), mode: mode},
		app.Config{AllowedCurrencies: []domain.Currency{"BRL"}}, logger)
	v := auth.NewVerifier(auth.Config{Issuer: env.issuer, JWKSURL: env.jwksURL, Audience: "wager-api", ClockSkew: time.Second})
	if err := v.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Stop)
	srv := httptest.NewServer(httpapi.New(svc, v, nil, logger, 10*time.Second))
	t.Cleanup(srv.Close)
	return srv.URL, mode
}

func TestIntermediateFailuresLeaveNoPartialState(t *testing.T) {
	url, mode := faultyServer(t)
	id := scoped()
	a := &testApp{URL: url}
	w := openWallet(t, a, "100.00")

	snapshot := func() (bal, ver, txs, entries, events int64) {
		bal, ver = walletState(t, w.ID)
		txs = countRows(t, "wager_transactions", "wallet_id = $1", w.ID)
		entries = countRows(t, "wallet_ledger_entries", "wallet_id = $1", w.ID)
		events = countRows(t, "outbox_events", "aggregate_type = 'Wallet' AND aggregate_id = $1", w.ID)
		return
	}
	b0, v0, t0, e0, o0 := snapshot()

	cases := []struct {
		name   string
		mode   faultMode
		status int
		code   string
	}{
		{"outbox write fails after balance, transaction and ledger", faultOutboxFails, 503, "TEMPORARILY_UNAVAILABLE"},
		{"transaction insert fails after balance update", faultTransactionFails, 503, "TEMPORARILY_UNAVAILABLE"},
		// Código com defeito (esquece o ledger): o próprio banco recusa o commit.
		{"ledger entry missing: database refuses the commit", faultLedgerSkipped, 500, "INTERNAL_ERROR"},
	}
	for _, c := range cases {
		mode.Store(int32(c.mode))
		r := submit(t, a, "provider-a", id("k"), betBody(w, "provider-a", id("t"), "30.00"))
		if r.Status != c.status || r.str("code") != c.code {
			t.Fatalf("%s: %d %s", c.name, r.Status, r.Raw)
		}
		if b, v, tx, e, o := func() (int64, int64, int64, int64, int64) { return snapshot() }(); b != b0 || v != v0 || tx != t0 || e != e0 || o != o0 {
			t.Fatalf("%s left partial state: balance %d->%d version %d->%d tx %d->%d ledger %d->%d events %d->%d",
				c.name, b0, b, v0, v, t0, tx, e0, e, o0, o)
		}
	}
	if n := countRows(t, "wager_transactions", "external_transaction_id = $1", id("t")); n != 0 {
		t.Fatalf("failed attempts left a transaction row")
	}

	// A mesma identidade (chave) pode ser reenviada e é aplicada uma única vez.
	mode.Store(int32(faultNone))
	ok := submit(t, a, "provider-a", id("k"), betBody(w, "provider-a", id("t"), "30.00"))
	if ok.Status != 200 || ok.Body["idempotentReplay"] != false || ok.str("balance", "amount") != "70.00" {
		t.Fatalf("retry after failures: %d %s", ok.Status, ok.Raw)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

// Erro de COMMIT não prova que o commit não ocorreu: a resposta é distinguível
// (503 COMMIT_OUTCOME_UNKNOWN) e a recuperação com a mesma chave devolve o
// resultado original sem novo débito.
func TestCommitOutcomeUnknownIsRecoverableWithTheSameKey(t *testing.T) {
	url, mode := faultyServer(t)
	id := scoped()
	a := &testApp{URL: url}
	w := openWallet(t, a, "100.00")

	mode.Store(int32(faultCommitAckLost))
	r := submit(t, a, "provider-a", id("k"), betBody(w, "provider-a", id("t"), "40.00"))
	if r.Status != http.StatusServiceUnavailable || r.str("code") != "COMMIT_OUTCOME_UNKNOWN" {
		t.Fatalf("unknown commit: %d %s", r.Status, r.Raw)
	}
	// O commit de fato ocorreu.
	if bal, _ := walletState(t, w.ID); bal != 60_00 {
		t.Fatalf("balance %d", bal)
	}

	mode.Store(int32(faultNone))
	rec := submit(t, a, "provider-a", id("k"), betBody(w, "provider-a", id("t"), "40.00"))
	if rec.Status != 200 || rec.Body["idempotentReplay"] != true || rec.str("balance", "amount") != "60.00" {
		t.Fatalf("recovery: %d %s", rec.Status, rec.Raw)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

func TestTransientDatabaseErrorsAreClassified(t *testing.T) {
	// Pool apontando para uma porta sem PostgreSQL: indisponibilidade transitória (503).
	pool, err := pgxpool.New(context.Background(), "postgres://wager:wager@127.0.0.1:1/wager?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := app.NewService(postgres.NewStore(postgres.StaticPool(pool)), app.Config{AllowedCurrencies: []domain.Currency{"BRL"}}, logger)
	v := auth.NewVerifier(auth.Config{Issuer: env.issuer, JWKSURL: env.jwksURL, Audience: "wager-api", ClockSkew: time.Second})
	if err := v.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer v.Stop()
	srv := httptest.NewServer(httpapi.New(svc, v, nil, logger, 5*time.Second))
	defer srv.Close()

	r := submit(t, &testApp{URL: srv.URL}, "provider-a", "k-down", betBody(wallet{ID: "0192f291-27dd-7d3f-8071-5f8685deef37", PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"}, "provider-a", "t-down", "1.00"))
	if r.Status != http.StatusServiceUnavailable || r.str("code") != "TEMPORARILY_UNAVAILABLE" {
		t.Fatalf("database down: %d %s", r.Status, r.Raw)
	}
}
