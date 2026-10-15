//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Regressões permanentes dos dois bloqueadores residuais da reauditoria de 06c6c65.

// --- (1) agendas seguradas localmente não monopolizam as varreduras ----------------------------------------

// Três linhas de agenda de carteiras distintas seguradas por UPDATEs reais do papel de runtime (sem lock das carteiras, sem
// desabilitar constraints) à frente de uma carteira livre. Nem a seleção nem a tentativa nem o adiamento esperam essas linhas:
// a livre avança já na primeira varredura, sem erro, e as linhas seguradas não são tocadas.
func TestLockedScheduleRowsDoNotStallTheSweepOrStarveAHealthyWallet(t *testing.T) {
	for _, batch := range []int{1, 20} {
		t.Run(fmt.Sprint("batch-", batch), func(t *testing.T) {
			ctx := context.Background()
			clock := newFakeClock()
			cfg := testRef
			cfg.BatchSize = batch
			cfg.PhaseTimeout = 2 * time.Second // uma espera pelo lock apareceria como falha/timeout
			iso := newIsolated(t, clock, cfg, nil)
			id := scoped()
			holder, err := iso.db.runtime.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Rollback(ctx) //nolint:errcheck
			var held []uuid.UUID
			for i := 0; i < 3; i++ {
				w, p := svcOpen(t, iso.svc, "100.00")
				r := mustSubmit(t, iso.svc, op(w, p, "REFUND", id(fmt.Sprint("old-", i)), "25.00", id("missing")))
				held = append(held, r.Transaction.ID())
				// SQL válido do runtime sobre a agenda, nada nas carteiras.
				if _, err := holder.Exec(ctx, `UPDATE pending_references SET next_attempt_at = next_attempt_at WHERE transaction_id = $1`, r.Transaction.ID()); err != nil {
					t.Fatal(err)
				}
				clock.Advance(time.Second)
			}
			w, p := svcOpen(t, iso.svc, "100.00")
			free := mustSubmit(t, iso.svc, op(w, p, "REFUND", id("free"), "25.00", id("bet")))
			mustSubmit(t, iso.svc, op(w, p, "BET", id("bet"), "25.00", ""))
			clock.Advance(time.Hour)
			before := map[uuid.UUID]pendingRow{}
			for _, tx := range held {
				before[tx] = iso.pending(t, tx)
			}

			begin := time.Now()
			n, failed := iso.svc.ProcessDue(ctx, make(chan struct{}))
			if elapsed := time.Since(begin); elapsed > time.Second {
				t.Errorf("the sweep waited %v for locked schedule rows", elapsed)
			}
			if failed || n != 1 {
				t.Fatalf("first sweep: completed=%d failed=%v, want the free wallet alone and no failure", n, failed)
			}
			if got, _ := iso.svc.GetTransaction(ctx, free.Transaction.ID()); got.Status() != domain.StatusProcessed {
				t.Fatalf("the healthy wallet did not progress: %s", got.Status())
			}
			for _, tx := range held {
				if a := iso.pending(t, tx); a.Attempts != 0 || !a.Next.Equal(before[tx].Next) || !a.Expires.Equal(before[tx].Expires) || a.Completed {
					t.Fatalf("a locked schedule changed: %+v -> %+v", before[tx], a)
				}
			}
			// O escritor era válido e confirma; liberadas, as agendas voltam a ser retomadas normalmente.
			if err := holder.Commit(ctx); err != nil {
				t.Fatalf("the writer holding the schedules must commit: %v", err)
			}
			for i := 0; i <= len(held); i++ { // BatchSize 1: uma agenda por varredura
				iso.svc.ProcessDue(ctx, make(chan struct{}))
			}
			for _, tx := range held {
				// O relógio da fixture passou do TTL: a retomada legítima encerra a pendência pela expiração (REFERENCE_NOT_FOUND).
				if a := iso.pending(t, tx); a.Attempts < 1 || !a.Completed {
					t.Fatalf("after the release the schedule was not retried to its (expired) end: %+v", a)
				}
			}
		})
	}
}

// setFailingUoW faz as transações de um CONJUNTO de carteiras falharem (depois da leitura real) e, se failDefer, também faz o
// adiamento falhar: a falha localizada que não consegue persistir nenhum adiamento.
type setFailingUoW struct {
	app.UnitOfWork
	targets   map[uuid.UUID]bool
	failDefer bool
	defers    *atomic.Int64
}

func (u setFailingUoW) Do(ctx context.Context, fn func(context.Context, app.Repositories) error) error {
	return u.UnitOfWork.Do(ctx, func(ctx context.Context, r app.Repositories) error {
		r.Transactions = setFailingTransactions{r.Transactions, u.targets}
		if u.failDefer {
			r.Pending = failingDefer{r.Pending, u.defers}
		}
		return fn(ctx, r)
	})
}

type setFailingTransactions struct {
	app.TransactionRepository
	targets map[uuid.UUID]bool
}

func (r setFailingTransactions) GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	tx, err := r.TransactionRepository.GetByID(ctx, id)
	if err == nil && r.targets[tx.WalletID()] {
		return nil, fmt.Errorf("%w: injected failure", app.ErrTransient)
	}
	return tx, err
}

type failingDefer struct {
	app.PendingReferenceRepository
	defers *atomic.Int64
}

func (r failingDefer) Defer(context.Context, uuid.UUID, time.Time, time.Time, string) error {
	r.defers.Add(1)
	return fmt.Errorf("%w: the deferral cannot persist either", app.ErrTransient)
}

// Quando a tentativa falha e o adiamento também não consegue persistir, nada tira o prefixo da frente da ordem; mesmo assim a
// varredura seguinte continua DEPOIS dele (posição de continuação entre chamadas), e a carteira saudável avança em poucas chamadas,
// sem aumentar limites. Cada chamada continua limitada a poucas falhas.
func TestAHealthyWalletProgressesAcrossCallsWhenTheFailingPrefixCannotBeDeferred(t *testing.T) {
	for _, batch := range []int{1, 20} {
		for _, bad := range []int{4, 7} {
			t.Run(fmt.Sprintf("batch-%d-bad-%d", batch, bad), func(t *testing.T) {
				ctx := context.Background()
				clock := newFakeClock()
				cfg := testRef
				cfg.BatchSize = batch
				iso := newIsolated(t, clock, cfg, nil)
				id := scoped()
				targets := map[uuid.UUID]bool{}
				var badTxs []uuid.UUID
				for i := 0; i < bad; i++ {
					w, p := svcOpen(t, iso.svc, "100.00")
					targets[uuid.MustParse(w)] = true
					r := mustSubmit(t, iso.svc, op(w, p, "REFUND", id(fmt.Sprint("bad-", i)), "25.00", id("missing")))
					badTxs = append(badTxs, r.Transaction.ID())
					clock.Advance(time.Second)
				}
				w, p := svcOpen(t, iso.svc, "100.00")
				free := mustSubmit(t, iso.svc, op(w, p, "REFUND", id("free"), "25.00", id("bet")))
				mustSubmit(t, iso.svc, op(w, p, "BET", id("bet"), "25.00", ""))
				clock.Advance(time.Minute)

				var defers atomic.Int64
				failing := iso.another(t, clock, cfg, func(u app.UnitOfWork) app.UnitOfWork {
					return setFailingUoW{u, targets, true, &defers}
				})
				calls := 0
				for ; calls < 6; calls++ {
					failing.ProcessDue(ctx, make(chan struct{}))
					if got, _ := iso.svc.GetTransaction(ctx, free.Transaction.ID()); got.Status() == domain.StatusProcessed {
						break
					}
				}
				wantCalls := (bad+2)/3 + 1 // três falhas por chamada, depois a saudável
				if got, _ := iso.svc.GetTransaction(ctx, free.Transaction.ID()); got.Status() != domain.StatusProcessed || calls+1 > wantCalls {
					t.Fatalf("the healthy wallet needed %d call(s) (status %s); want it processed within %d without any deferral persisted", calls+1, got.Status(), wantCalls)
				}
				if defers.Load() == 0 {
					t.Fatal("setup: the deferral was never attempted")
				}
				for _, tx := range badTxs { // nada persistido para o prefixo, nenhum estado financeiro mudou
					got, _ := iso.svc.GetTransaction(ctx, tx)
					if a := iso.pending(t, tx); got.Status() != domain.StatusPendingReference || a.Attempts != 0 || a.Completed {
						t.Fatalf("a failed attempt left effects: %s %+v", got.Status(), a)
					}
				}
			})
		}
	}
}

// Contenção preservada: com TODAS as carteiras falhando (indisponibilidade real), cada chamada faz poucas tentativas.
func TestAFullyFailingDependencyStillCostsAFewAttemptsPerCall(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	cfg := testRef
	cfg.BatchSize = 20
	iso := newIsolated(t, clock, cfg, nil)
	id := scoped()
	for i := 0; i < 15; i++ {
		w, p := svcOpen(t, iso.svc, "100.00")
		mustSubmit(t, iso.svc, op(w, p, "REFUND", id(fmt.Sprint("r", i)), "25.00", id("missing")))
		clock.Advance(time.Second)
	}
	clock.Advance(time.Minute)
	var calls atomic.Int64
	failing := iso.another(t, clock, cfg, func(u app.UnitOfWork) app.UnitOfWork { return anyWalletFailingUoW{u, &calls} })
	for i := 0; i < 4; i++ {
		before := calls.Load()
		if _, failed := failing.ProcessDue(ctx, make(chan struct{})); !failed {
			t.Fatal("a failing sweep must report failure")
		}
		if n := calls.Load() - before; n < 1 || n > 3 {
			t.Fatalf("call %d made %d attempts under a general failure; want at most 3", i, n)
		}
	}
}

// --- (2) a exclusão dos escritores cobre toda a janela entre decidir e restaurar ----------------------------

// upHeldAfterSettlement inicia op (Up/UpTo) e o conduz até a fase goose: a decisão sobre a cópia (LOCK TABLE) fica em espera por uma
// leitura do teste; quando termina, o lock do goose, retido pelo teste, segura a aplicação das migrations. Ao retornar, a decisão já
// foi confirmada e a restauração da 00006 ainda não começou: é a janela que a exclusão precisa cobrir.
func upHeldAfterSettlement(t *testing.T, d *upgradeDB, op func(context.Context) error) (done chan error, release func()) {
	t.Helper()
	ctx := context.Background()
	gate, err := d.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Exec(ctx, `LOCK TABLE wallets IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	callCtx, cancelCall := context.WithTimeout(ctx, 60*time.Second)
	done = make(chan error, 1)
	go func() { done <- op(callCtx) }()
	eventually(t, "the settlement to wait for the writers' lock", 10*time.Second, func() bool {
		return observe(t, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND wait_event_type = 'Lock'
			AND query LIKE 'LOCK TABLE wallets, wager_transactions, pending_references%'`, d.name) > 0
	})
	hold, err := pgxConnect(ctx, d.url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hold.Exec(ctx, `SELECT pg_advisory_lock($1)`, int64(gooseLockID)); err != nil {
		t.Fatal(err)
	}
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the goose phase to wait for its lock", 10*time.Second, func() bool {
		return observe(t, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND query LIKE '%pg_try_advisory_lock%' AND query NOT LIKE '%wager-migrator-lock%'`, d.name) > 0
	})
	var released atomic.Bool
	release = func() {
		if released.CompareAndSwap(false, true) {
			_, _ = hold.Exec(ctx, `SELECT pg_advisory_unlock($1)`, int64(gooseLockID))
			_ = hold.Close(ctx)
		}
	}
	t.Cleanup(func() { cancelCall(); release() })
	return done, release
}

// runtimeAttemptNow é uma retomada real pelo papel de runtime sobre a agenda RECONSTRUÍDA pela 00005 (prazo de 15 min a partir da
// reaplicação): relógio logo depois de agora, antes do TTL e depois da data agendada.
func runtimeAttemptNow(t *testing.T, d *upgradeDB, id uuid.UUID) error {
	t.Helper()
	sched, err := loadPendingOn(d.owner, id)
	if err != nil {
		return err
	}
	clock := newFakeClock()
	clock.Advance(time.Second)
	svc := app.NewService(newStoreOn(d), app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef}, discardLogger(), app.WithClock(clock.Now))
	done, err := svc.ProcessPending(context.Background(), sched)
	if err == nil && done {
		return errors.New("expected a waiting attempt, got a terminal result")
	}
	return err
}

// DownTo(4) → UpTo(5) → `migrate grants` (o runtime recupera o acesso) → Up/UpTo(6): a exclusão dos escritores continua da decisão
// até a restauração. Uma retomada real do runtime durante a janela é negada em voz alta (nada é confirmado para ser sobrescrito), a
// restauração traz os valores originais e os privilégios do runtime são devolvidos ao final.
func TestUpAndUpToExcludeTheRuntimeFromTheDecisionUntilTheRestoration(t *testing.T) {
	for name, run := range map[string]func(*upgradeDB) func(context.Context) error{
		"Up": func(d *upgradeDB) func(context.Context) error {
			return func(ctx context.Context) error { _, err := d.mig.Up(ctx); return err }
		},
		"UpTo(6)": func(d *upgradeDB) func(context.Context) error {
			return func(ctx context.Context) error { _, err := d.mig.UpTo(ctx, 6); return err }
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			d, open, done := reauditSchedule(t)
			before := scheduleJSON(t, d, "pending_references")
			if _, err := d.mig.DownTo(ctx, 4); err != nil {
				t.Fatal(err)
			}
			if _, err := d.mig.UpTo(ctx, 5); err != nil {
				t.Fatal(err)
			}
			bin := buildMigrate(t)
			if out, err := migrateCmd(d, bin, "grants").CombinedOutput(); err != nil { // caminho público de recuperação do acesso
				t.Fatalf("migrate grants: %v\n%s", err, out)
			}
			if !canWriteSchedule(t, d, env.runtimeRole) {
				t.Fatal("setup: the runtime must have recovered its write access before the Up")
			}

			upDone, release := upHeldAfterSettlement(t, d, run(d))
			// A janela: a decisão foi confirmada e a restauração ainda não aconteceu.
			if canWriteSchedule(t, d, env.runtimeRole) {
				t.Fatal("the runtime can still write the schedule between the decision and the restoration")
			}
			if err := runtimeAttemptNow(t, d, open); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Fatalf("a real runtime attempt during the window must be denied, got: %v", err)
			}
			release()
			if err := <-upDone; err != nil {
				t.Fatalf("the Up failed: %v", err)
			}
			if v := schemaVersion(t, d.owner); v != 6 {
				t.Fatalf("schema %d, want 6", v)
			}
			if after := scheduleJSON(t, d, "pending_references"); after != before {
				t.Fatalf("the restoration did not bring back the original schedule:\n%s\n%s", before, after)
			}
			if a := readSchedules(t, d.owner)[done]; a.Completed == nil {
				t.Fatal("the completed schedule was lost")
			}
			if tableExists(t, d, "pending_references_preserved") {
				t.Fatal("the copy was left behind")
			}
			// Os privilégios do runtime são devolvidos ao final: o trabalho continua possível sobre a agenda restaurada.
			if !canWriteSchedule(t, d, env.runtimeRole) {
				t.Fatal("the runtime did not get its privileges back after the Up")
			}
			if err := runtimeAttempt(t, d, open); err != nil {
				t.Fatalf("runtime attempt after the Up: %v", err)
			}
		})
	}
}

func newStoreOn(d *upgradeDB) *postgres.Store {
	return postgres.NewStore(postgres.StaticPool(d.runtime))
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
