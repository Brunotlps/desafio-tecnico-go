//go:build integration

package integration

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Regressões permanentes dos quatro defeitos reproduzidos pela auditoria independente do incremento 2
// (adaptadas das sondagens: sem diretórios temporários nem dependência de ambiente de terceiros).

// --- (1) progresso de carteiras independentes ---------------------------------------------------------

// dueSpy conta as consultas de candidatos e permite sincronizar a devolução deles.
type dueSpy struct {
	app.PendingReferenceRepository
	calls    *atomic.Int64
	selected chan struct{} // fechado na primeira devolução (opcional)
	release  chan struct{} // devolve só depois de fechado (opcional)
	once     *atomic.Bool
}

func (d dueSpy) Due(ctx context.Context, q app.DueQuery) ([]app.PendingReference, error) {
	list, err := d.PendingReferenceRepository.Due(ctx, q)
	d.calls.Add(1)
	if d.selected != nil && d.once.CompareAndSwap(false, true) {
		close(d.selected)
		select {
		case <-d.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return list, err
}

type spyUoW struct {
	app.UnitOfWork
	spy dueSpy
}

func (u spyUoW) Reader() app.Repositories {
	r := u.UnitOfWork.Reader()
	spy := u.spy
	spy.PendingReferenceRepository = r.Pending
	r.Pending = spy
	return r
}

func TestBusyWalletsAtTheHeadOfTheScanDoNotStarveIndependentWallets(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	ref := testRef
	ref.BatchSize = 5
	iso := newIsolated(t, clock, ref, nil)
	svc := iso.svc
	id := scoped()

	// BatchSize+3 pendências antigas, todas na mesma carteira (que ficará travada), e só depois uma carteira
	// independente e livre cuja referência já chegou.
	busyW, busyP := svcOpen(t, svc, "100.00")
	var busyTx []uuid.UUID
	for i := 0; i < ref.BatchSize+3; i++ {
		r := mustSubmit(t, svc, op(busyW, busyP, "REFUND", id(uuid.NewString()), "25.00", id("never")))
		busyTx = append(busyTx, r.Transaction.ID())
		clock.Advance(2 * time.Second)
	}
	freeW, freeP := svcOpen(t, svc, "100.00")
	free := mustSubmit(t, svc, op(freeW, freeP, "REFUND", id("free"), "25.00", id("arrived-bet")))
	mustSubmit(t, svc, op(freeW, freeP, "BET", id("arrived-bet"), "25.00", ""))
	clock.Advance(time.Minute) // tudo vencido; as da carteira ocupada vêm antes na ordem de vencimento

	lock, err := iso.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, busyW); err != nil {
		t.Fatal(err)
	}
	before := map[uuid.UUID]pendingRow{}
	for _, tx := range busyTx {
		before[tx] = iso.pending(t, tx)
	}

	// Uma única varredura: a carteira livre avança ANTES de a ocupada ser liberada.
	if n, failed := svc.ProcessDue(ctx, make(chan struct{})); failed || n != 1 {
		t.Fatalf("scan: completed=%d failed=%v, want exactly the independent wallet's operation", n, failed)
	}
	if got, _ := svc.GetTransaction(ctx, free.Transaction.ID()); got.Status() != domain.StatusProcessed {
		t.Fatalf("the independent wallet did not progress while another was busy: %s", got.Status())
	}
	// A ocupada não foi tocada: sem tentativa, sem renovar prazo, sem reagendar.
	for _, tx := range busyTx {
		after := iso.pending(t, tx)
		if after.Attempts != 0 || !after.Expires.Equal(before[tx].Expires) || !after.Next.Equal(before[tx].Next) || after.Completed {
			t.Fatalf("a busy wallet's schedule changed: %+v -> %+v", before[tx], after)
		}
	}
	// Liberada, a ocupada também avança: uma tentativa por pendência, no máximo BatchSize por varredura.
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	svc.ProcessDue(ctx, make(chan struct{}))
	svc.ProcessDue(ctx, make(chan struct{}))
	for _, tx := range busyTx {
		if a := iso.pending(t, tx); a.Attempts != 1 || !a.Expires.Equal(before[tx].Expires) {
			t.Fatalf("after releasing the wallet: %+v", a)
		}
	}
}

// Várias instâncias varrendo ao mesmo tempo, com o prefixo todo em carteira ocupada: a carteira independente avança
// exatamente uma vez (os executores não duplicam o efeito) e a ocupada não é tocada.
func TestSeveralInstancesScanningABusyHeadProgressTheIndependentWalletOnce(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	ref := testRef
	ref.BatchSize = 3
	iso := newIsolated(t, clock, ref, nil)
	seed := iso.svc
	id := scoped()
	busyW, busyP := svcOpen(t, seed, "100.00")
	var busyTx []uuid.UUID
	for i := 0; i < ref.BatchSize+2; i++ {
		busyTx = append(busyTx, mustSubmit(t, seed, op(busyW, busyP, "REFUND", id(uuid.NewString()), "25.00", id("never"))).Transaction.ID())
		clock.Advance(2 * time.Second)
	}
	freeW, freeP := svcOpen(t, seed, "100.00")
	free := mustSubmit(t, seed, op(freeW, freeP, "REFUND", id("free"), "25.00", id("arrived")))
	mustSubmit(t, seed, op(freeW, freeP, "BET", id("arrived"), "25.00", ""))
	clock.Advance(time.Minute)
	lock, err := iso.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, busyW); err != nil {
		t.Fatal(err)
	}
	execs := make([]*app.Service, 4)
	for i := range execs {
		execs[i] = iso.another(t, clock, ref, nil)
	}
	var total atomic.Int64
	parallel(len(execs), func(i int) struct{} {
		n, failed := execs[i].ProcessDue(ctx, make(chan struct{}))
		if failed {
			t.Errorf("instance %d: scan failed", i)
		}
		total.Add(int64(n))
		return struct{}{}
	})
	if total.Load() != 1 {
		t.Fatalf("completions across instances = %d, want exactly 1", total.Load())
	}
	if n := iso.ledgerEntries(t, free.Transaction.ID()); n != 1 {
		t.Fatalf("ledger entries of the independent operation: %d", n)
	}
	for _, tx := range busyTx {
		if a := iso.pending(t, tx); a.Attempts != 0 || a.Completed {
			t.Fatalf("a busy wallet's schedule was touched: %+v", a)
		}
	}
}

// Uma varredura em que TODAS as carteiras estão ocupadas termina depressa, sem loop ativo: o número de
// consultas de candidatos é limitado pelo número de carteiras ocupadas (cada uma é excluída da página seguinte),
// nada é tentado e nenhuma agenda muda.
func TestAFullyBusyScanEndsWithoutBusyLoopingAndWithoutTouchingTheSchedules(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	ref := testRef
	ref.BatchSize = 3
	var calls atomic.Int64
	iso := newIsolated(t, clock, ref, func(u app.UnitOfWork) app.UnitOfWork { return spyUoW{u, dueSpy{calls: &calls}} })
	svc := iso.svc
	id := scoped()
	lock, err := iso.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	var txs []uuid.UUID
	for w := 0; w < 3; w++ {
		wid, p := svcOpen(t, svc, "100.00")
		for i := 0; i < ref.BatchSize+2; i++ {
			r := mustSubmit(t, svc, op(wid, p, "REFUND", id(uuid.NewString()), "25.00", id("never")))
			txs = append(txs, r.Transaction.ID())
		}
		if _, err := lock.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, wid); err != nil {
			t.Fatal(err)
		}
	}
	clock.Advance(time.Hour)
	before := map[uuid.UUID]pendingRow{}
	for _, tx := range txs {
		before[tx] = iso.pending(t, tx)
	}
	calls.Store(0)
	finished := make(chan struct{})
	go func() { svc.ProcessDue(ctx, make(chan struct{})); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("a fully busy scan did not end")
	}
	if n := calls.Load(); n > 3+1 { // uma página por carteira ocupada + a página vazia final
		t.Fatalf("candidate queries in one scan = %d, want at most %d", n, 3+1)
	}
	for _, tx := range txs {
		if after := iso.pending(t, tx); after.Attempts != 0 || !after.Next.Equal(before[tx].Next) || !after.Expires.Equal(before[tx].Expires) || after.Completed {
			t.Fatalf("a fully busy scan changed a schedule: %+v -> %+v", before[tx], after)
		}
	}
}

// --- (2) candidato antigo e backoff ---------------------------------------------------------------------

func TestAStaleCandidateRespectsTheRescheduledDate(t *testing.T) {
	clock := newFakeClock()
	iso := newIsolated(t, clock, testRef, nil)
	svc := iso.svc
	id := scoped()
	w, p := svcOpen(t, svc, "100.00")
	pend := mustSubmit(t, svc, op(w, p, "REFUND", id("pending"), "25.00", id("missing")))
	clock.Advance(2 * time.Second)

	// Duas varreduras reais selecionam o mesmo candidato. B termina a seleção ANTES de A reagendar e só
	// entra na tentativa depois do commit de A (sincronização observável pelos canais do decorador).
	selected, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	var once atomic.Bool
	slow := iso.another(t, clock, testRef, func(u app.UnitOfWork) app.UnitOfWork {
		return spyUoW{u, dueSpy{calls: &calls, selected: selected, release: release, once: &once}}
	})
	finished := make(chan bool, 1)
	go func() { _, failed := slow.ProcessDue(context.Background(), make(chan struct{})); finished <- failed }()
	<-selected
	if _, failed := svc.ProcessDue(context.Background(), make(chan struct{})); failed {
		t.Fatal("first scan failed")
	}
	first := iso.pending(t, pend.Transaction.ID())
	if first.Attempts != 1 || !first.Next.After(clock.Now()) {
		t.Fatalf("setup: the first scan must have rescheduled into the future: %+v (clock %v)", first, clock.Now())
	}
	close(release)
	if <-finished {
		t.Fatal("second scan failed")
	}
	second := iso.pending(t, pend.Transaction.ID())
	if second.Attempts != first.Attempts || !second.Next.Equal(first.Next) || !second.Expires.Equal(first.Expires) {
		t.Fatalf("a stale candidate bypassed the durable backoff without the clock advancing: %+v -> %+v", first, second)
	}

	// Com o relógio avançando de propósito, a pendência volta a ser elegível (e o prazo segue o original).
	clock.Advance(time.Minute)
	ref, err := loadPendingOn(iso.owner, pend.Transaction.ID())
	if err != nil {
		t.Fatal(err)
	}
	if done, err := svc.ProcessPending(context.Background(), ref); err != nil || done {
		t.Fatalf("due attempt: done=%v err=%v", done, err)
	}
	if third := iso.pending(t, pend.Transaction.ID()); third.Attempts != 2 || !third.Expires.Equal(first.Expires) {
		t.Fatalf("after the intentional advance: %+v", third)
	}
}

// --- (3) agenda preservada no rollback ---------------------------------------------------------------------

type scheduleRow struct {
	Tx                     uuid.UUID
	First, Expires, Next   time.Time
	Attempts               int
	LastAttempt, Completed *time.Time
	LastError              string
}

func readSchedules(t *testing.T, conn *pgx.Conn) map[uuid.UUID]scheduleRow {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT transaction_id, first_persisted_at, expires_at, next_attempt_at, attempts, last_attempt_at, COALESCE(last_error,''), completed_at
		FROM pending_references`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[uuid.UUID]scheduleRow{}
	for rows.Next() {
		var r scheduleRow
		if err := rows.Scan(&r.Tx, &r.First, &r.Expires, &r.Next, &r.Attempts, &r.LastAttempt, &r.LastError, &r.Completed); err != nil {
			t.Fatal(err)
		}
		out[r.Tx] = r
	}
	return out
}

// scheduleDB prepara um banco isolado na versão final com uma pendência aberta (prazo vencido, tentativas e
// diagnóstico) e uma agenda concluída.
func scheduleDB(t *testing.T, version int64) (*upgradeDB, *app.Service, uuid.UUID, uuid.UUID) {
	t.Helper()
	d := newUpgradeDB(t, "audit_schedule", version)
	clock := newFakeClock()
	clock.Advance(-3 * time.Hour) // o prazo de 15 min já está vencido em relação ao relógio real
	svc := app.NewService(postgres.NewStore(postgres.StaticPool(d.runtime)), app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef},
		slog.New(slog.NewTextHandler(io.Discard, nil)), app.WithClock(clock.Now))
	w, p := svcOpen(t, svc, "100.00")
	id := scoped()
	open := mustSubmit(t, svc, op(w, p, "REFUND", id("open"), "25.00", id("missing")))
	done := mustSubmit(t, svc, op(w, p, "REFUND", id("done"), "25.00", id("bet")))
	mustSubmit(t, svc, op(w, p, "BET", id("bet"), "25.00", ""))
	clock.Advance(time.Minute)
	ref, err := loadPendingOn(d.owner, done.Transaction.ID())
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := svc.ProcessPending(context.Background(), ref); err != nil || !ok {
		t.Fatalf("complete the second schedule: ok=%v err=%v", ok, err)
	}
	if _, err := d.owner.Exec(context.Background(), `UPDATE pending_references SET attempts = 7, last_error = 'temporary database outage',
		last_attempt_at = first_persisted_at + interval '1 minute', next_attempt_at = next_attempt_at + interval '10 minutes' WHERE transaction_id = $1`, open.Transaction.ID()); err != nil {
		t.Fatal(err)
	}
	return d, svc, open.Transaction.ID(), done.Transaction.ID()
}

func loadPendingOn(conn *pgx.Conn, txID uuid.UUID) (app.PendingReference, error) {
	var p app.PendingReference
	err := conn.QueryRow(context.Background(),
		`SELECT transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts FROM pending_references WHERE transaction_id = $1`, txID).
		Scan(&p.TransactionID, &p.WalletID, &p.FirstPersistedAt, &p.ExpiresAt, &p.NextAttemptAt, &p.Attempts)
	return p, err
}

func downUntilBefore(t *testing.T, d *upgradeDB, version int64) {
	t.Helper()
	for schemaVersion(t, d.owner) >= version {
		if _, err := d.mig.Down(context.Background()); err != nil {
			t.Fatalf("down from %d: %v", schemaVersion(t, d.owner), err)
		}
	}
}

// Uma reversão operacional (passo a passo) seguida da reaplicação preserva a agenda — início, vencimento,
// próxima tentativa, tentativas, diagnóstico e as agendas concluídas —, sem renovar o prazo vencido.
func TestRollbackAndReapplyPreserveTheDurableSchedule(t *testing.T) {
	ctx := context.Background()
	final, _ := postgres.ExpectedVersion()
	d, _, openTx, doneTx := scheduleDB(t, final)
	before := readSchedules(t, d.owner)
	if before[openTx].Attempts != 7 || before[doneTx].Completed == nil || !before[openTx].Expires.Before(time.Now()) {
		t.Fatalf("setup: %+v", before)
	}

	downUntilBefore(t, d, 5) // reverte a 00005 (e as posteriores) um passo por vez, como o operador faz
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatalf("reapply: %v", err)
	}
	after := readSchedules(t, d.owner)
	if len(after) != len(before) {
		t.Fatalf("schedules: %d before, %d after", len(before), len(after))
	}
	for tx, b := range before {
		a := after[tx]
		if !a.First.Equal(b.First) || !a.Expires.Equal(b.Expires) || !a.Next.Equal(b.Next) || a.Attempts != b.Attempts || a.LastError != b.LastError ||
			!sameTime(a.LastAttempt, b.LastAttempt) || !sameTime(a.Completed, b.Completed) {
			t.Errorf("schedule of %s changed by the rollback cycle:\n before %+v\n after  %+v", tx, b, a)
		}
	}
	// O estado preservado não sobra: nada fica duplicado para um ciclo seguinte.
	if n := queryOn(t, d.owner, `SELECT count(*) FROM information_schema.tables WHERE table_name = 'pending_references_preserved'`); n != 0 {
		t.Errorf("the preservation table was left behind")
	}
}

func sameTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}

func queryOn(t *testing.T, conn *pgx.Conn, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// Durante a reversão da 00005 — depois da preparação e antes do DROP da agenda — nenhum papel que não seja o dono
// consegue escrever nela: nada se perde em silêncio (o runtime falha em voz alta) e a cópia preservada está completa.
// O DROP é mantido em espera por uma leitura aberta do teste, o que torna a janela observável sem dormir.
func TestWritersAreStoppedWhileTheScheduleIsBeingRolledBack(t *testing.T) {
	ctx := context.Background()
	final, _ := postgres.ExpectedVersion()
	d, _, openTx, doneTx := scheduleDB(t, final)
	before := readSchedules(t, d.owner)
	for schemaVersion(t, d.owner) > 5 { // reverte as migrations posteriores à 00005
		if _, err := d.mig.Down(ctx); err != nil {
			t.Fatal(err)
		}
	}
	hold, err := d.owner.Begin(ctx) // a leitura aberta (ACCESS SHARE) segura o DROP TABLE da 00005
	if err != nil {
		t.Fatal(err)
	}
	defer hold.Rollback(ctx)
	if _, err := hold.Exec(ctx, `SELECT count(*) FROM pending_references`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := d.mig.Down(ctx); done <- err }()
	eventually(t, "the DROP of the schedule to wait", 10*time.Second, func() bool {
		select {
		case err := <-done:
			t.Fatalf("the rollback ended before the observable window (err=%v)", err)
		default:
		}
		return waitingForLock("%pending_references%")
	})

	// A janela: a preparação já concluiu, o DROP ainda não. (Os privilégios são lidos do catálogo: uma escrita real
	// ficaria na fila de locks atrás do DROP em espera e só falharia depois dele.)
	var anyPrivilege bool
	if err := d.owner.QueryRow(ctx, `SELECT has_any_column_privilege($1, 'public.pending_references', 'SELECT,INSERT,UPDATE')`, env.runtimeRole).Scan(&anyPrivilege); err != nil {
		t.Fatal(err)
	}
	if anyPrivilege {
		t.Fatal("the runtime role can still read or write the schedule while it is being rolled back")
	}
	var preserved int64
	if err := d.owner.QueryRow(ctx, `SELECT count(*) FROM pending_references_preserved`).Scan(&preserved); err != nil || preserved != int64(len(before)) {
		t.Fatalf("preserved copy: %d rows (%v), want %d", preserved, err, len(before))
	}

	if err := hold.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("down of 00005: %v", err)
	}
	if v := schemaVersion(t, d.owner); v != 4 {
		t.Fatalf("version %d", v)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	after := readSchedules(t, d.owner)
	if a := after[openTx]; a.Attempts != 7 || !a.Expires.Equal(before[openTx].Expires) || after[doneTx].Completed == nil {
		t.Fatalf("restored schedule: %+v", a)
	}
}

// Um banco ainda na versão 5 (sem a migration corretiva) também preserva a agenda ao reverter a 00005 pela
// interface do migrador, e a reaplicação restaura os valores originais.
func TestMigratorPreservesTheScheduleOfADatabaseStillAtVersionFive(t *testing.T) {
	ctx := context.Background()
	d, _, openTx, doneTx := scheduleDB(t, 5)
	before := readSchedules(t, d.owner)
	if _, err := d.mig.Down(ctx); err != nil {
		t.Fatalf("down of 00005: %v", err)
	}
	if v := schemaVersion(t, d.owner); v != 4 {
		t.Fatalf("version %d", v)
	}
	if n := queryOn(t, d.owner, `SELECT count(*) FROM pending_references_preserved`); n != int64(len(before)) {
		t.Fatalf("preserved rows: %d", n)
	}
	if _, err := d.mig.Up(ctx); err != nil { // reaplica a 00005 e a corretiva (que restaura)
		t.Fatalf("reapply: %v", err)
	}
	after := readSchedules(t, d.owner)
	if a := after[openTx]; a.Attempts != 7 || !a.Expires.Equal(before[openTx].Expires) || a.LastError != "temporary database outage" || after[doneTx].Completed == nil {
		t.Fatalf("restored schedule: %+v", a)
	}
}

// DownTo(version) também preserva, e a restauração recusa uma cópia que não corresponde a nenhuma transação.
func TestDownToPreservesAndAnOrphanedCopyBlocksTheRestoration(t *testing.T) {
	ctx := context.Background()
	final, _ := postgres.ExpectedVersion()
	d, _, openTx, _ := scheduleDB(t, final)
	before := readSchedules(t, d.owner)
	if _, err := d.mig.DownTo(ctx, 3); err != nil {
		t.Fatalf("down-to 3: %v", err)
	}
	if n := queryOn(t, d.owner, `SELECT count(*) FROM pending_references_preserved`); n != int64(len(before)) {
		t.Fatalf("preserved rows: %d", n)
	}
	// Uma cópia órfã (a transação desapareceu) não é descartada em silêncio: a migration recusa, com ids.
	if _, err := d.owner.Exec(ctx, `UPDATE pending_references_preserved SET transaction_id = gen_random_uuid() WHERE transaction_id = $1`, openTx); err != nil {
		t.Fatal(err)
	}
	_, err := d.mig.Up(ctx)
	if err == nil || !strings.Contains(err.Error(), "pending_references_preserved") || !strings.Contains(err.Error(), "first ids") {
		t.Fatalf("restoration over an orphaned copy: %v", err)
	}
	if n := queryOn(t, d.owner, `SELECT count(*) FROM pending_references_preserved`); n != int64(len(before)) {
		t.Fatalf("the refused restoration discarded the copy: %d", n)
	}
}

// down-to 0 é o reset deliberadamente destrutivo (apaga todos os dados, como sempre): não deixa cópia preservada.
func TestFullResetIsDestructiveAndLeavesNothingBehind(t *testing.T) {
	ctx := context.Background()
	final, _ := postgres.ExpectedVersion()
	d, _, _, _ := scheduleDB(t, final)
	if _, err := d.mig.DownTo(ctx, 0); err != nil {
		t.Fatalf("down-to 0: %v", err)
	}
	if n := queryOn(t, d.owner, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'goose_db_version'`); n != 0 {
		t.Fatalf("%d table(s) left after the full reset", n)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatalf("up after the reset: %v", err)
	}
	if n := queryOn(t, d.owner, `SELECT count(*) FROM pending_references`); n != 0 {
		t.Fatalf("schedules reappeared after the reset: %d", n)
	}
}

// --- (4) forma de PENDING_REFERENCE no banco -----------------------------------------------------------------

func TestDatabaseRejectsPendingReferenceShapesThatCannotBeResumed(t *testing.T) {
	ctx := context.Background()
	final, _ := postgres.ExpectedVersion()
	d := newUpgradeDB(t, "audit_pending_shape", final)
	clock := newFakeClock()
	svc := app.NewService(postgres.NewStore(postgres.StaticPool(d.runtime)), app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef},
		slog.New(slog.NewTextHandler(io.Discard, nil)), app.WithClock(clock.Now))
	w, p := svcOpen(t, svc, "100.00")
	wid, pid := uuid.MustParse(w), uuid.MustParse(p)

	// insertPending grava, como o papel restrito, uma transação PENDING_REFERENCE com identidade e hash válidos
	// e a sua agenda; refExt nil = NULL.
	insertPending := func(kind, amount string, refExt *string, origin string) error {
		// O hash do payload vem do domínio. Para as formas que o domínio recusa, usa-se uma referência
		// de enchimento: o banco é quem decide sobre o que é gravado em reference_external_transaction_id.
		raw := op(w, p, kind, uniqueID("shape"), amount, "")
		if kind == "WIN" || kind == "REFUND" || kind == "ROLLBACK" {
			raw.ReferenceExternalTransactionID = "filler-reference"
			if refExt != nil {
				raw.ReferenceExternalTransactionID = *refExt
			}
		}
		req, err := domain.NewExternalRequest(raw)
		if err != nil {
			raw.ReferenceExternalTransactionID = "filler-reference"
			if req, err = domain.NewExternalRequest(raw); err != nil {
				if kind == "BET" || kind == "LOSS" {
					raw.ReferenceExternalTransactionID = ""
					req, err = domain.NewExternalRequest(raw)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		minor := int64(2500)
		if kind == "LOSS" {
			minor = 0
		}
		id := uuid.Must(uuid.NewV7())
		return rtInTx(t, d.runtime, func(ctx context.Context, tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
				idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, created_at, updated_at)
				VALUES ($1,$2,$3,'PENDING_REFERENCE',$4,$5,$6,'BRL',$7,$8,$9,$10,'round','game',$11,now(),now())`,
				id, origin, kind, wid, pid, minor, req.ProviderID(), req.ExternalTransactionID(), req.IdempotencyKey(), req.PayloadHash(), refExt); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO pending_references (transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at)
				VALUES ($1,$2,now() - interval '1 minute',now() + interval '10 minutes',now() - interval '1 second')`, id, wid)
			return err
		})
	}
	str := func(s string) *string { return &s }

	t.Run("kinds that cannot wait for a reference", func(t *testing.T) {
		for _, c := range []struct{ kind, amount string }{{"BET", "25.00"}, {"LOSS", "0.00"}, {"WIN", "25.00"}} {
			for name, ref := range map[string]*string{"NULL reference": nil, "empty reference": str(""), "some reference": str("x")} {
				if c.kind == "WIN" && ref != nil && *ref == "x" {
					continue // WIN com referência válida é elegível (coberto abaixo)
				}
				err := insertPending(c.kind, c.amount, ref, "EXTERNAL")
				if err == nil {
					t.Errorf("%s with %s was committed as PENDING_REFERENCE", c.kind, name)
					continue
				}
				if e := pgErr(err); e == nil || e.Code != "23514" {
					t.Errorf("%s with %s: %v, want a check violation", c.kind, name, err)
				}
			}
		}
	})
	t.Run("OPENING cannot wait for a reference", func(t *testing.T) {
		err := rtInTx(t, d.runtime, func(ctx context.Context, tx pgx.Tx) error {
			id := uuid.Must(uuid.NewV7())
			if _, err := tx.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, created_at, updated_at)
				VALUES ($1,'INTERNAL','OPENING','PENDING_REFERENCE',$2,$3,100,'BRL',now(),now())`, id, wid, pid); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO pending_references (transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at) VALUES ($1,$2,now(),now() + interval '1 minute',now())`, id, wid)
			return err
		})
		if e := pgErr(err); e == nil || e.Code != "23514" {
			t.Errorf("OPENING: %v, want a check violation", err)
		}
	})
	t.Run("reference forms that rehydration rejects", func(t *testing.T) {
		long := strings.Repeat("x", 129)
		for name, ref := range map[string]*string{
			"NULL": nil, "empty": str(""), "leading space": str(" ref"), "trailing space": str("ref "), "control character": str("re\tf"), "too long": &long,
		} {
			for _, kind := range []string{"REFUND", "ROLLBACK", "WIN"} {
				if err := insertPending(kind, "25.00", ref, "EXTERNAL"); err == nil {
					t.Errorf("%s PENDING_REFERENCE with a %s reference was committed", kind, name)
				} else if e := pgErr(err); e == nil || e.Code != "23514" {
					t.Errorf("%s with a %s reference: %v, want a check violation", kind, name, err)
				}
			}
		}
	})
	t.Run("a reference form that is invalid in any status is refused", func(t *testing.T) {
		for name, ref := range map[string]string{"empty": "", "leading space": " ref", "control character": "re\nf"} {
			err := rtInTx(t, d.runtime, func(ctx context.Context, tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
					idempotency_key, payload_hash, round_id, game_id, reference_external_transaction_id, failure_code, result_balance, created_at, updated_at)
					VALUES ($1,'EXTERNAL','REFUND','REJECTED',$2,$3,2500,'BRL','svc-provider',$4,$4,'h','r','g',$5,'X_Y',10000,now(),now())`, uuid.Must(uuid.NewV7()), wid, pid, uuid.NewString(), ref)
				return err
			})
			if e := pgErr(err); e == nil || e.Code != "23514" {
				t.Errorf("REJECTED REFUND with a %s reference: %v, want a check violation", name, err)
			}
		}
	})
	t.Run("valid pending operations keep working and are resumed", func(t *testing.T) {
		id := scoped()
		refs := map[string]string{}
		for _, kind := range []string{"WIN", "REFUND", "ROLLBACK"} {
			refs[kind] = id("arrives-" + kind)
			if err := insertPending(kind, "25.00", str(refs[kind]), "EXTERNAL"); err != nil {
				t.Fatalf("valid %s pending operation refused: %v", kind, err)
			}
		}
		// As referências chegam; a retomada conclui cada operação inserida (mesma identidade).
		for _, kind := range []string{"WIN", "REFUND", "ROLLBACK"} {
			mustSubmit(t, svc, op(w, p, "BET", refs[kind], "25.00", ""))
		}
		clock.Advance(time.Minute)
		stop := make(chan struct{})
		if n, failed := svc.ProcessDue(ctx, stop); failed || n != 3 {
			t.Fatalf("resumption: completed=%d failed=%v, want the three valid pending operations", n, failed)
		}
		if n := queryOn(t, d.owner, `SELECT count(*) FROM wager_transactions WHERE wallet_id = $1 AND kind IN ('WIN','REFUND','ROLLBACK') AND status = 'PROCESSED'`, wid); n != 3 {
			t.Fatalf("processed = %d", n)
		}
	})
}

// O histórico já inválido é recusado pela migration, com contagem e ids, sem corrigir nem alterar nada.
func TestShapeMigrationRefusesInvalidHistoryWithoutTouchingIt(t *testing.T) {
	ctx := context.Background()
	final, _ := postgres.ExpectedVersion()
	if final < 6 {
		t.Skip("arrives with the corrective migration")
	}
	d := newUpgradeDB(t, "audit_shape_history", 5)
	svc := app.NewService(postgres.NewStore(postgres.StaticPool(d.runtime)), app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	w, p := svcOpen(t, svc, "100.00")
	bad := uuid.Must(uuid.NewV7())
	req, _ := domain.NewExternalRequest(op(w, p, "BET", uniqueID("bad"), "1.00", ""))
	// A versão 5 aceita a forma inválida (é o defeito): BET em PENDING_REFERENCE sem referência, com agenda.
	if err := rtInTx(t, d.runtime, func(ctx context.Context, tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount, currency, provider_id, external_transaction_id,
			idempotency_key, payload_hash, round_id, game_id, created_at, updated_at)
			VALUES ($1,'EXTERNAL','BET','PENDING_REFERENCE',$2,$3,100,'BRL',$4,$5,$6,$7,'round','game',now(),now())`,
			bad, w, p, req.ProviderID(), req.ExternalTransactionID(), req.IdempotencyKey(), req.PayloadHash()); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO pending_references (transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at) VALUES ($1,$2,now(),now() + interval '1 minute',now())`, bad, w)
		return err
	}); err != nil {
		t.Fatalf("setup (version 5 accepts the invalid shape): %v", err)
	}
	before := readSchedules(t, d.owner)
	_, err := d.mig.Up(ctx)
	if err == nil || !strings.Contains(err.Error(), "first ids") || !strings.Contains(err.Error(), bad.String()) {
		t.Fatalf("migration error = %v, want the count and the id of the invalid pending transaction", err)
	}
	if v := schemaVersion(t, d.owner); v != 5 {
		t.Fatalf("the refused migration moved the schema to %d", v)
	}
	if after := readSchedules(t, d.owner); len(after) != len(before) {
		t.Fatalf("history changed: %d -> %d schedules", len(before), len(after))
	}
	if n := queryOn(t, d.owner, `SELECT count(*) FROM wager_transactions WHERE id = $1 AND status = 'PENDING_REFERENCE'`, bad); n != 1 {
		t.Fatal("the invalid record was altered")
	}
}

// A interface efetivamente usada pelo operador — o binário `migrate` — também preserva a agenda: `down` passo a
// passo e `up`, com o papel dono do schema, devolvem a agenda com os valores originais.
func TestMigrateCommandPreservesTheScheduleAcrossDownAndUp(t *testing.T) {
	final, _ := postgres.ExpectedVersion()
	d, _, openTx, doneTx := scheduleDB(t, final)
	before := readSchedules(t, d.owner)

	bin := t.TempDir() + "/migrate"
	build := exec.Command("go", "build", "-o", bin, "./cmd/migrate")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build migrate: %v\n%s", err, out)
	}
	run := func(args ...string) string {
		cmd := exec.Command(bin, args...)
		cmd.Env = append(os.Environ(), "MIGRATE_DATABASE_URL="+d.url, "DATABASE_URL=", "RUNTIME_DB_ROLE=")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("migrate %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	for schemaVersion(t, d.owner) > 4 {
		run("down")
	}
	if n := queryOn(t, d.owner, `SELECT count(*) FROM pending_references_preserved`); n != int64(len(before)) {
		t.Fatalf("preserved rows after `migrate down`: %d", n)
	}
	run("up")
	after := readSchedules(t, d.owner)
	if a := after[openTx]; a.Attempts != 7 || !a.Expires.Equal(before[openTx].Expires) || a.LastError != "temporary database outage" || after[doneTx].Completed == nil {
		t.Fatalf("schedule after `migrate down` x2 and `migrate up`: %+v", a)
	}
	// down-to 0 pelo binário: reset destrutivo, sem cópia.
	run("down-to", "0")
	if n := queryOn(t, d.owner, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'goose_db_version'`); n != 0 {
		t.Fatalf("%d table(s) left after `migrate down-to 0`", n)
	}
}
