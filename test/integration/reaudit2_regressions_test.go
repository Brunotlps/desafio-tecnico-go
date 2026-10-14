//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Regressões permanentes dos quatro cenários da reauditoria focada de 2109d38 (adaptadas das sondagens independentes).

// --- (1) progresso atrás de escritores de QUALQUER modo de lock que a tentativa enxerga --------------------

// A tentativa trava a carteira com FOR UPDATE, que conflita com todos os outros modos. A seleção de candidatos precisa
// pular as carteiras seguradas por qualquer um deles — não só por FOR UPDATE: um UPDATE de coluna não chave (FOR NO KEY
// UPDATE, como o do trigger de encadeamento do ledger), FOR SHARE e FOR KEY SHARE (como a checagem de FK) também a deixam ocupada.
func TestAFreeWalletProgressesBehindWritersHoldingAnyConflictingRowLockMode(t *testing.T) {
	modes := map[string]string{
		"FOR UPDATE":        `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`,
		"FOR NO KEY UPDATE": `SELECT 1 FROM wallets WHERE id = $1 FOR NO KEY UPDATE`,
		"FOR SHARE":         `SELECT 1 FROM wallets WHERE id = $1 FOR SHARE`,
		"FOR KEY SHARE":     `SELECT 1 FROM wallets WHERE id = $1 FOR KEY SHARE`,
		"UPDATE non-key":    `UPDATE wallets SET updated_at = updated_at WHERE id = $1`,
	}
	for name, hold := range modes {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			b := newBusyPrefixHeldBy(t, 60, 1, hold)
			for scan := 0; scan < 3; scan++ {
				if _, failed := b.iso.svc.ProcessDue(ctx, make(chan struct{})); failed {
					t.Fatal("scan failed")
				}
			}
			if got, _ := b.iso.svc.GetTransaction(ctx, b.free); got.Status() != domain.StatusProcessed {
				t.Fatalf("the independent wallet starved behind %d wallets held by %s: %s, attempts=%d", len(b.busy), name, got.Status(), b.iso.pending(t, b.free).Attempts)
			}
			b.untouched(t)
			// O escritor era válido: a transação do prefixo confirma com todas as constraints financeiras habilitadas.
			if err := b.lock.Commit(ctx); err != nil {
				t.Fatalf("the writer holding the prefix must commit: %v", err)
			}
		})
	}
}

// --- (2) falha localizada não monopoliza a varredura -----------------------------------------------------------

// failingWalletUoW devolve um erro transitório (depois da leitura real) para as transações de UMA carteira; os demais
// repositórios, os dados e o SQL continuam reais.
type failingWalletUoW struct {
	app.UnitOfWork
	target uuid.UUID
	calls  *atomic.Int64
}

func (u failingWalletUoW) Do(ctx context.Context, fn func(context.Context, app.Repositories) error) error {
	return u.UnitOfWork.Do(ctx, func(ctx context.Context, r app.Repositories) error {
		r.Transactions = failingWalletTransactions{r.Transactions, u.target, u.calls}
		return fn(ctx, r)
	})
}

type failingWalletTransactions struct {
	app.TransactionRepository
	target uuid.UUID
	calls  *atomic.Int64
}

func (r failingWalletTransactions) GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	tx, err := r.TransactionRepository.GetByID(ctx, id)
	if err == nil && tx.WalletID() == r.target {
		r.calls.Add(1)
		return nil, fmt.Errorf("%w: injected failure for only this wallet", app.ErrTransient)
	}
	return tx, err
}

func TestALocalizedFailureDoesNotStarveAnotherWalletAndTheRetryIsSafe(t *testing.T) {
	for _, batch := range []int{1, 20} {
		t.Run(fmt.Sprint("batch-", batch), func(t *testing.T) {
			ctx := context.Background()
			clock := newFakeClock()
			cfg := testRef
			cfg.BatchSize = batch
			iso := newIsolated(t, clock, cfg, nil)
			id := scoped()
			badWallet, p := svcOpen(t, iso.svc, "100.00")
			var badTxs []uuid.UUID
			for i := 0; i < batch; i++ { // um prefixo do tamanho do lote, todo numa carteira
				bad := mustSubmit(t, iso.svc, op(badWallet, p, "REFUND", id(fmt.Sprint("bad-", i)), "25.00", id("missing")))
				badTxs = append(badTxs, bad.Transaction.ID())
				clock.Advance(time.Second)
			}
			w, p := svcOpen(t, iso.svc, "100.00")
			free := mustSubmit(t, iso.svc, op(w, p, "REFUND", id("free"), "25.00", id("bet")))
			mustSubmit(t, iso.svc, op(w, p, "BET", id("bet"), "25.00", ""))
			clock.Advance(2 * time.Second)

			var injected atomic.Int64
			failing := iso.another(t, clock, cfg, func(u app.UnitOfWork) app.UnitOfWork {
				return failingWalletUoW{u, uuid.MustParse(badWallet), &injected}
			})
			for i := 0; i < 3; i++ {
				failing.ProcessDue(ctx, make(chan struct{}))
			}
			if got, _ := iso.svc.GetTransaction(ctx, free.Transaction.ID()); got.Status() != domain.StatusProcessed {
				t.Fatalf("localized failures consumed the scan budget and starved the valid wallet: %s (attempts=%d)", got.Status(), iso.pending(t, free.Transaction.ID()).Attempts)
			}
			// Falha de infraestrutura nunca vira rejeição financeira: a pendência continua aberta, adiada e diagnosticada.
			for _, tx := range badTxs {
				got, _ := iso.svc.GetTransaction(ctx, tx)
				if got.Status() != domain.StatusPendingReference {
					t.Fatalf("an infrastructure failure changed the financial state to %s", got.Status())
				}
			}
			first := iso.pending(t, badTxs[0])
			if first.Completed || first.Attempts < 1 || !first.Next.After(clock.Now()) {
				t.Fatalf("the failing schedule was not deferred with a backoff: %+v (now %v)", first, clock.Now())
			}
			if n := iso.ledgerEntries(t, badTxs[0]); n != 0 {
				t.Fatalf("a failed attempt left %d ledger entrie(s)", n)
			}
			// Retirada a falha, o mesmo trabalho prossegue pelo estado durável quando o adiamento vence (sem reparo de dados).
			clock.Advance(time.Minute)
			iso.svc.ProcessDue(ctx, make(chan struct{}))
			for _, tx := range badTxs {
				if a := iso.pending(t, tx); a.Attempts < 1 || a.Completed {
					t.Fatalf("retry after the failure was removed: %+v", a)
				}
			}
		})
	}
}

// Quando a falha é geral (todas as carteiras), a varredura para depois de poucas falhas consecutivas: não martela o banco.
func TestAGeneralFailureStopsTheScanAfterFewConsecutiveFailures(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	cfg := testRef
	cfg.BatchSize = 20
	iso := newIsolated(t, clock, cfg, nil)
	id := scoped()
	var wallets []string
	for i := 0; i < 15; i++ {
		w, p := svcOpen(t, iso.svc, "100.00")
		wallets = append(wallets, w)
		mustSubmit(t, iso.svc, op(w, p, "REFUND", id(fmt.Sprint("r", i)), "25.00", id("missing")))
		clock.Advance(time.Second)
	}
	clock.Advance(time.Minute)
	var calls atomic.Int64
	failing := iso.another(t, clock, cfg, func(u app.UnitOfWork) app.UnitOfWork {
		return anyWalletFailingUoW{u, &calls}
	})
	_, failed := failing.ProcessDue(ctx, make(chan struct{}))
	if !failed {
		t.Fatal("a failing scan must report failure (the worker spaces the next scan)")
	}
	if n := calls.Load(); n == 0 || n > 6 {
		t.Fatalf("%d attempts in a fully failing scan; want a few (bounded), not one per pending", n)
	}
	_ = wallets
}

type anyWalletFailingUoW struct {
	app.UnitOfWork
	calls *atomic.Int64
}

func (u anyWalletFailingUoW) Do(ctx context.Context, fn func(context.Context, app.Repositories) error) error {
	return u.UnitOfWork.Do(ctx, func(ctx context.Context, r app.Repositories) error {
		r.Transactions = anyFailingTransactions{r.Transactions, u.calls}
		return fn(ctx, r)
	})
}

type anyFailingTransactions struct {
	app.TransactionRepository
	calls *atomic.Int64
}

func (r anyFailingTransactions) GetByID(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	r.calls.Add(1)
	return nil, fmt.Errorf("%w: injected general failure", app.ErrTransient)
}

// --- (3) Up depois de um Down cancelado preserva o trabalho confirmado na agenda viva ----------------------------

// runtimeAttempt executa uma retomada REAL pelo papel de runtime, antes do TTL e depois da data agendada (um processo já
// iniciado continua podendo trabalhar depois de os grants serem devolvidos; o portão de schema só impede novas partidas).
func runtimeAttempt(t *testing.T, d *upgradeDB, id uuid.UUID) error {
	t.Helper()
	sched, err := loadPendingOn(d.owner, id)
	if err != nil {
		return err
	}
	clock := newFakeClock()
	clock.Advance(sched.FirstPersistedAt.Add(3 * time.Minute).Sub(clock.Now()))
	svc := app.NewService(postgres.NewStore(postgres.StaticPool(d.runtime)), app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef},
		slog.New(slog.NewTextHandler(io.Discard, nil)), app.WithClock(clock.Now))
	done, err := svc.ProcessPending(context.Background(), sched)
	if err == nil && done {
		return errors.New("expected a waiting attempt before the TTL, got a terminal result")
	}
	return err
}

func TestADirectUpAfterACancelledDownKeepsTheWorkConfirmedOnTheLiveSchedule(t *testing.T) {
	ctx := context.Background()
	d, open, done := reauditSchedule(t)
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5: a agenda original continua viva
		t.Fatal(err)
	}
	downDone, release, cancel := downHeldAfterPreparation(t, d)
	cancel()
	if err := <-downDone; err == nil {
		t.Fatal("the cancelled Down reported success")
	}
	release()
	if err := runtimeAttempt(t, d, open); err != nil {
		t.Fatal(err)
	}
	before := scheduleJSON(t, d, "pending_references")
	if !strings.Contains(before, `"attempts": 8`) {
		t.Fatalf("setup: the runtime attempt must have been confirmed (attempts 8): %s", before)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("a direct Up restored the stale snapshot over work confirmed after the cancelled Down:\n%s\n%s", before, after)
	}
	if tableExists(t, d, "pending_references_preserved") {
		t.Fatal("a copy was left behind")
	}
	if a := readSchedules(t, d.owner)[done]; a.Completed == nil {
		t.Fatal("the completed schedule was lost")
	}
}

// UpTo(6) a partir de um Down cancelado também preserva, e um novo Down depois do Up funciona.
func TestUpToAfterACancelledDownKeepsTheWorkAndTheNextCycleStillPreserves(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	if _, err := d.mig.Down(ctx); err != nil {
		t.Fatal(err)
	}
	downDone, release, cancel := downHeldAfterPreparation(t, d)
	cancel()
	<-downDone
	release()
	if err := runtimeAttempt(t, d, open); err != nil {
		t.Fatal(err)
	}
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.UpTo(ctx, 6); err != nil {
		t.Fatal(err)
	}
	if scheduleJSON(t, d, "pending_references") != before {
		t.Fatal("UpTo(6) lost the confirmed work")
	}
	// O ciclo seguinte continua preservando.
	if _, err := d.mig.DownTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if scheduleJSON(t, d, "pending_references") != before {
		t.Fatal("the next rollback cycle lost data")
	}
}

// Depois de uma reaplicação parcial, uma agenda que não é a reconstrução intacta impede o Up ANTES de qualquer perda.
func TestUpIsRefusedBeforeLossWhenTheLiveScheduleIsNotTheIntactReconstruction(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	if _, err := d.mig.DownTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := d.mig.UpTo(ctx, 5); err != nil {
		t.Fatal(err)
	}
	copyBefore := scheduleJSON(t, d, "pending_references_preserved")
	if _, err := d.owner.Exec(ctx, `UPDATE pending_references SET attempts = 3, last_error = 'written at the intermediate schema' WHERE transaction_id = $1`, open); err != nil {
		t.Fatal(err)
	}
	liveBefore := scheduleJSON(t, d, "pending_references")
	_, err := d.mig.Up(ctx)
	if err == nil || !strings.Contains(err.Error(), open.String()) {
		t.Fatalf("Up must be refused naming the schedule it would overwrite: %v", err)
	}
	if v := schemaVersion(t, d.owner); v != 5 {
		t.Fatalf("schema %d after the refusal, want 5", v)
	}
	if scheduleJSON(t, d, "pending_references") != liveBefore || scheduleJSON(t, d, "pending_references_preserved") != copyBefore {
		t.Fatal("the refused Up changed the schedule or the copy")
	}
}

// --- (4) grants e provisão automática dentro da região coordenada ----------------------------------------------

func buildMigrate(t *testing.T) string {
	t.Helper()
	bin := t.TempDir() + "/migrate"
	build := exec.Command("go", "build", "-o", bin, "./cmd/migrate")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build migrate: %v\n%s", err, out)
	}
	return bin
}

func migrateCmd(d *upgradeDB, bin string, args ...string) *exec.Cmd {
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "MIGRATE_DATABASE_URL="+d.url, "DATABASE_URL=", "RUNTIME_DB_ROLE="+env.runtimeRole, "RUNTIME_DB_PASSWORD="+env.runtimePass)
	return cmd
}

// Com um Down preparado e retido, `migrate grants` ESPERA a operação protegida (não reabre a escrita), pode ser cancelado
// sem vazar lock e, depois que a operação termina, funciona (e a restauração traz os valores originais).
func TestTheGrantsCommandWaitsForAPreparedDownAndCanBeCancelled(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5
		t.Fatal(err)
	}
	downDone, release, _ := downHeldAfterPreparation(t, d)
	bin := buildMigrate(t)

	// 1. `migrate grants` espera pelo lock do migrador.
	grants := migrateCmd(d, bin, "grants")
	var grantsOut strings.Builder
	grants.Stdout, grants.Stderr = &grantsOut, &grantsOut
	if err := grants.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- grants.Wait() }()
	eventually(t, "migrate grants to wait for the migrator lock", 10*time.Second, func() bool {
		select {
		case err := <-exited:
			t.Fatalf("migrate grants ended during the prepared Down (err=%v): %s", err, grantsOut.String())
		default:
		}
		return migratorWaiters(t, d) > 0
	})
	// A janela segue fechada: nenhuma escrita do runtime é aceita enquanto o comando espera.
	if err := runtimeAttempt(t, d, open); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("the runtime wrote (or failed otherwise) during the prepared Down: %v", err)
	}
	// 2. Cancelado, o comando termina sem vazar lock nem alterar privilégios.
	if err := grants.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err == nil {
			t.Fatal("a cancelled migrate grants reported success")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("migrate grants did not honour the cancellation while waiting")
	}
	eventually(t, "the cancelled command to leave the lock queue", 5*time.Second, func() bool { return migratorWaiters(t, d) == 0 })
	if canWriteSchedule(t, d, env.runtimeRole) {
		t.Fatal("the cancelled grants command gave the runtime write access")
	}
	// 3. A operação protegida termina normalmente (o lock não vazou) e o ciclo preserva a agenda.
	release()
	if err := <-downDone; err != nil {
		t.Fatal(err)
	}
	if out, err := migrateCmd(d, bin, "grants").CombinedOutput(); err != nil { // schema 4: sem agenda; o comando conclui
		t.Fatalf("migrate grants after the operation: %v\n%s", err, out)
	}
	if out, err := migrateCmd(d, bin, "up").CombinedOutput(); err != nil {
		t.Fatalf("migrate up: %v\n%s", err, out)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the cycle changed the schedule:\n%s\n%s", before, after)
	}
}

// A provisão automática de `migrate up` também espera a operação protegida (e roda na mesma região do Up).
func TestAutomaticProvisioningAfterUpWaitsForAPreparedDown(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.Down(ctx); err != nil {
		t.Fatal(err)
	}
	downDone, release, _ := downHeldAfterPreparation(t, d)
	bin := buildMigrate(t)
	up := migrateCmd(d, bin, "up")
	var upOut strings.Builder
	up.Stdout, up.Stderr = &upOut, &upOut
	if err := up.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- up.Wait() }()
	eventually(t, "migrate up to wait for the migrator lock", 10*time.Second, func() bool {
		select {
		case err := <-exited:
			t.Fatalf("migrate up ended during the prepared Down (err=%v): %s", err, upOut.String())
		default:
		}
		return migratorWaiters(t, d) > 0
	})
	if err := runtimeAttempt(t, d, open); err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("the runtime wrote during the prepared Down: %v", err)
	}
	release()
	if err := <-downDone; err != nil {
		t.Fatal(err)
	}
	if err := <-exited; err != nil {
		t.Fatalf("migrate up: %v\n%s", err, upOut.String())
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the cycle changed the schedule:\n%s\n%s", before, after)
	}
	// Depois do Up, o runtime tem os privilégios (provisionados pela mesma operação).
	if !canWriteSchedule(t, d, env.runtimeRole) {
		t.Fatal("the runtime did not get its privileges back after migrate up")
	}
}

// Recuperação após a morte do processo: com a operação anterior terminada, `migrate grants` devolve o acesso.
func TestGrantsRecoverAfterAKilledMigratorOnceTheOperationIsOver(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	if _, err := d.mig.Down(ctx); err != nil {
		t.Fatal(err)
	}
	bin := buildMigrate(t)
	gate, err := d.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(ctx) //nolint:errcheck
	if _, err := gate.Exec(ctx, `LOCK TABLE wallets IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	cmd := migrateCmd(d, bin, "down")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	eventually(t, "migrate down to wait at the preparation", 10*time.Second, func() bool {
		return observe(t, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND wait_event_type = 'Lock'
			AND query LIKE 'LOCK TABLE wallets, wager_transactions, pending_references%'`, d.name) > 0
	})
	holder, err := pgxConnect(ctx, d.url)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close(ctx) //nolint:errcheck
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_lock($1)`, int64(gooseLockID)); err != nil {
		t.Fatal(err)
	}
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the preparation to commit (runtime write revoked)", 10*time.Second, func() bool {
		return tableExists(t, d, "pending_references_preserved") && !canWriteSchedule(t, d, env.runtimeRole)
	})
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-exited
	eventually(t, "the killed migrator to release its session lock", 10*time.Second, func() bool {
		return observe(t, `SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory' AND l.granted AND a.datname = $1 AND l.objid::bigint = $2`, d.name, int64(postgres.MigratorLockID)) == 0
	})
	if v := schemaVersion(t, d.owner); v != 5 {
		t.Fatalf("schema %d after the kill, want 5", v)
	}
	_ = holder.Close(ctx) // solta o lock do goose retido pelo teste: o processo morto não o segurava
	if out, err := migrateCmd(d, bin, "grants").CombinedOutput(); err != nil {
		t.Fatalf("migrate grants after the kill: %v\n%s", err, out)
	}
	if err := runtimeAttempt(t, d, open); err != nil {
		t.Fatalf("the runtime did not recover its write: %v", err)
	}
	before := scheduleJSON(t, d, "pending_references")
	if out, err := migrateCmd(d, bin, "up").CombinedOutput(); err != nil {
		t.Fatalf("migrate up after the kill: %v\n%s", err, out)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("up after the kill lost the work confirmed after the recovery:\n%s\n%s", before, after)
	}
}

func pgxConnect(ctx context.Context, url string) (*pgx.Conn, error) { return pgx.Connect(ctx, url) }
