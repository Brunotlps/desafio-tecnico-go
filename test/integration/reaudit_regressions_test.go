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
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Regressões permanentes dos três defeitos reproduzidos pela reauditoria do incremento 2 (progresso além do orçamento de
// páginas, cópia original da agenda e exclusão efetiva de escritores), adaptadas das sondagens independentes: sem
// diretórios temporários e sem dependência de ambiente de terceiros.

// --- (1) progresso além do orçamento de páginas -------------------------------------------------------------

// busyPrefix é um prefixo de carteiras ocupadas à frente de uma carteira independente e livre com a referência já chegada.
type busyPrefix struct {
	iso   isolated
	clock *fakeClock
	cfg   app.ReferenceConfig
	free  uuid.UUID   // a REFUND da carteira livre
	busy  []uuid.UUID // as pendências antigas das carteiras travadas
	lock  pgx.Tx      // segura FOR UPDATE nas carteiras ocupadas
	rows  map[uuid.UUID]pendingRow
}

// newBusyPrefix cria wallets carteiras distintas, cada uma com uma pendência antiga, todas travadas, e depois uma
// carteira livre com uma REFUND cuja BET já foi processada. Com BatchSize pequeno e muitas carteiras, o prefixo ocupado
// é muito maior que qualquer orçamento de páginas por varredura.
func newBusyPrefix(t *testing.T, wallets, batch int) *busyPrefix {
	t.Helper()
	return newBusyPrefixHeldBy(t, wallets, batch, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`)
}

// newBusyPrefixHeldBy é newBusyPrefix com o SQL que segura cada carteira ocupada (o modo de lock do escritor).
func newBusyPrefixHeldBy(t *testing.T, wallets, batch int, holdSQL string) *busyPrefix {
	t.Helper()
	ctx := context.Background()
	b := &busyPrefix{clock: newFakeClock(), cfg: testRef}
	b.cfg.BatchSize = batch
	b.iso = newIsolated(t, b.clock, b.cfg, nil)
	id := scoped()
	lock, err := b.iso.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Rollback(ctx) })
	b.lock = lock
	for i := 0; i < wallets; i++ {
		w, p := svcOpen(t, b.iso.svc, "100.00")
		r := mustSubmit(t, b.iso.svc, op(w, p, "REFUND", id(fmt.Sprint("old-", i)), "25.00", id("missing")))
		b.busy = append(b.busy, r.Transaction.ID())
		if _, err := lock.Exec(ctx, holdSQL, w); err != nil {
			t.Fatal(err)
		}
		b.clock.Advance(2 * time.Second)
	}
	w, p := svcOpen(t, b.iso.svc, "100.00")
	free := mustSubmit(t, b.iso.svc, op(w, p, "REFUND", id("free"), "25.00", id("bet")))
	mustSubmit(t, b.iso.svc, op(w, p, "BET", id("bet"), "25.00", ""))
	b.free = free.Transaction.ID()
	b.clock.Advance(time.Hour) // tudo vencido; as ocupadas vêm antes na ordem de vencimento
	b.rows = map[uuid.UUID]pendingRow{}
	for _, tx := range b.busy {
		b.rows[tx] = b.iso.pending(t, tx)
	}
	return b
}

// untouched afirma que nenhuma agenda das carteiras ocupadas mudou: sem tentativa, sem reagendar, sem renovar prazo.
func (b *busyPrefix) untouched(t *testing.T) {
	t.Helper()
	for _, tx := range b.busy {
		a, before := b.iso.pending(t, tx), b.rows[tx]
		if a.Attempts != 0 || !a.Expires.Equal(before.Expires) || !a.Next.Equal(before.Next) || a.Completed {
			t.Fatalf("a busy wallet's schedule changed: %+v -> %+v", before, a)
		}
	}
}

// Mais carteiras ocupadas, e distintas, do que qualquer orçamento de páginas por varredura: a carteira livre avança
// mesmo assim, ainda com o prefixo travado, e as ocupadas não são tocadas.
func TestAFreeWalletProgressesBehindMoreBusyWalletsThanTheScanPageBudget(t *testing.T) {
	ctx := context.Background()
	b := newBusyPrefix(t, 120, 1)
	for scan := 0; scan < 3; scan++ {
		if _, failed := b.iso.svc.ProcessDue(ctx, make(chan struct{})); failed {
			t.Fatal("scan failed")
		}
		if got, _ := b.iso.svc.GetTransaction(ctx, b.free); got.Status() == domain.StatusProcessed {
			break
		}
	}
	if got, _ := b.iso.svc.GetTransaction(ctx, b.free); got.Status() != domain.StatusProcessed {
		t.Fatalf("the independent wallet starved behind %d busy wallets: %s, attempts=%d", len(b.busy), got.Status(), b.iso.pending(t, b.free).Attempts)
	}
	b.untouched(t)

	// Liberadas, as ocupadas também avançam: uma tentativa cada, sem renovar prazo.
	if err := b.lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for scan := 0; scan < len(b.busy)+1; scan++ {
		b.iso.svc.ProcessDue(ctx, make(chan struct{}))
	}
	for _, tx := range b.busy {
		if a := b.iso.pending(t, tx); a.Attempts != 1 || !a.Expires.Equal(b.rows[tx].Expires) {
			t.Fatalf("after releasing the wallets: %+v", a)
		}
	}
}

// Várias instâncias varrendo ao mesmo tempo o mesmo prefixo ocupado: a carteira livre avança exatamente uma vez.
func TestSeveralInstancesBehindTheBusyPrefixProgressTheFreeWalletOnce(t *testing.T) {
	ctx := context.Background()
	b := newBusyPrefix(t, 60, 1)
	instances := []*app.Service{b.iso.svc, b.iso.another(t, b.clock, b.cfg, nil), b.iso.another(t, b.clock, b.cfg, nil)}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, svc := range instances {
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				svc.ProcessDue(ctx, make(chan struct{}))
			}()
		}
	}
	close(start)
	wg.Wait()
	if got, _ := b.iso.svc.GetTransaction(ctx, b.free); got.Status() != domain.StatusProcessed {
		t.Fatalf("the independent wallet did not progress: %s", got.Status())
	}
	if n := b.iso.ledgerEntries(t, b.free); n != 1 {
		t.Fatalf("ledger entries of the free wallet = %d, want exactly 1", n)
	}
	b.untouched(t)
}

// --- helpers da agenda preservada --------------------------------------------------------------------------

// reauditSchedule prepara um banco isolado no schema da agenda (6) com TTL não padrão (7 min), relógio duas horas atrás (agenda
// aberta já vencida) com 7 tentativas e diagnóstico, e uma agenda concluída.
func reauditSchedule(t *testing.T) (d *upgradeDB, open, done uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	d = newUpgradeDB(t, "reaudit_schedule", scheduleSchemaVersion)
	clock := newFakeClock()
	clock.Advance(-2 * time.Hour)
	cfg := testRef
	cfg.TTL = 7 * time.Minute
	svc := app.NewService(postgres.NewStore(postgres.StaticPool(d.runtime)), app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: cfg},
		slog.New(slog.NewTextHandler(io.Discard, nil)), app.WithClock(clock.Now))
	w, p := svcOpen(t, svc, "100.00")
	id := scoped()
	o := mustSubmit(t, svc, op(w, p, "REFUND", id("open"), "25.00", id("missing")))
	dn := mustSubmit(t, svc, op(w, p, "REFUND", id("done"), "25.00", id("bet")))
	mustSubmit(t, svc, op(w, p, "BET", id("bet"), "25.00", ""))
	clock.Advance(time.Minute)
	ref, err := loadPendingOn(d.owner, dn.Transaction.ID())
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := svc.ProcessPending(ctx, ref); err != nil || !ok {
		t.Fatalf("complete the second schedule: ok=%v err=%v", ok, err)
	}
	if _, err := d.owner.Exec(ctx, `UPDATE pending_references SET attempts = 7, last_error = 'old outage',
		last_attempt_at = first_persisted_at + interval '30 seconds', next_attempt_at = first_persisted_at + interval '2 minutes'
		WHERE transaction_id = $1`, o.Transaction.ID()); err != nil {
		t.Fatal(err)
	}
	return d, o.Transaction.ID(), dn.Transaction.ID()
}

// scheduleJSON serializa TODAS as colunas da agenda (ou da cópia), sem a coluna de controle da cópia.
func scheduleJSON(t *testing.T, d *upgradeDB, table string) string {
	t.Helper()
	var s string
	if err := d.owner.QueryRow(context.Background(),
		`SELECT COALESCE(jsonb_agg(to_jsonb(p) - 'preserved_at' ORDER BY transaction_id), '[]'::jsonb)::text FROM `+table+` p`).Scan(&s); err != nil {
		t.Fatal(err)
	}
	return s
}

func provisionRuntime(t *testing.T, d *upgradeDB) {
	t.Helper()
	if err := postgres.ProvisionRuntimeRole(context.Background(), d.url, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		t.Fatal(err)
	}
}

func tableExists(t *testing.T, d *upgradeDB, name string) bool {
	t.Helper()
	var ok bool
	if err := d.owner.QueryRow(context.Background(), `SELECT to_regclass('public.`+name+`') IS NOT NULL`).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

// gooseLockID é a chave do advisory lock de sessão do goose (a mesma que o provider disputa).
const gooseLockID = 4097083626

// observe conta, numa conexão própria (o pg_stat_activity é uma fotografia por transação), as linhas de uma consulta de
// observação.
func observe(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := testPool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// migratorWaiters conta as sessões que esperam o lock do migrador: o migrador o adquire por tentativas (pg_try_advisory_lock marcado
// no texto), então quem espera é a sessão com essa consulta que NÃO detém o lock.
func migratorWaiters(t *testing.T, d *upgradeDB) int64 {
	t.Helper()
	return observe(t, `SELECT count(*) FROM pg_stat_activity a WHERE a.datname = $1 AND a.query LIKE '%wager-migrator-lock%'
		AND NOT EXISTS (SELECT 1 FROM pg_locks l WHERE l.pid = a.pid AND l.locktype = 'advisory' AND l.granted AND l.objid::bigint = $2)`, d.name, postgres.MigratorLockID)
}

// downHeldAfterPreparation inicia Down e o conduz até a fase goose: a preparação (LOCK TABLE) é mantida em espera por uma
// leitura do teste; quando ela termina, o advisory lock do goose — retido pelo teste em outra sessão — segura a
// reversão. Quando a função retorna, a preparação já foi concluída e a reversão da 00005 ainda não começou. release
// devolve o lock do goose; cancel cancela o Down.
func downHeldAfterPreparation(t *testing.T, d *upgradeDB) (done chan error, release func(), cancel context.CancelFunc) {
	t.Helper()
	ctx := context.Background()
	gate, err := d.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Exec(ctx, `LOCK TABLE wallets IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	callCtx, cancelCall := context.WithTimeout(ctx, 30*time.Second)
	done = make(chan error, 1)
	go func() { _, err := d.mig.Down(callCtx); done <- err }()
	eventually(t, "the preparation to wait for the writers' lock", 10*time.Second, func() bool {
		return observe(t, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND wait_event_type = 'Lock'
			AND query LIKE 'LOCK TABLE wallets, wager_transactions, pending_references%'`, d.name) > 0
	})
	// A leitura da versão (que também usa o lock do goose) já terminou: agora o teste retém o lock do goose e libera a preparação.
	hold, err := pgx.Connect(ctx, d.url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hold.Exec(ctx, `SELECT pg_advisory_lock($1)`, int64(gooseLockID)); err != nil {
		t.Fatal(err)
	}
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	// O goose sonda o lock com pg_try_advisory_lock (não aparece como espera em pg_locks): a conexão cuja última consulta é essa
	// sonda, depois que o teste reteve o lock, é a reversão esperando a sua vez.
	eventually(t, "the goose phase to wait for its lock", 10*time.Second, func() bool {
		return observe(t, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND query LIKE '%pg_try_advisory_lock%' AND query NOT LIKE '%wager-migrator-lock%'`, d.name) > 0
	})
	var once sync.Once
	release = func() {
		once.Do(func() {
			_, _ = hold.Exec(ctx, `SELECT pg_advisory_unlock($1)`, int64(gooseLockID))
			_ = hold.Close(ctx)
		})
	}
	t.Cleanup(func() { cancelCall(); release() })
	return done, release, cancelCall
}

// --- (2) a cópia original sobrevive a reaplicação parcial e a cancelamento ----------------------------------------

// DownTo(4) → UpTo(5) → Down → Up: a segunda preparação não pode copiar a reconstrução da 00005 por cima do original.
func TestAPartialReapplyThenRollbackKeepsTheOriginalSchedule(t *testing.T) {
	ctx := context.Background()
	d, _, _ := reauditSchedule(t)
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.DownTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	original := scheduleJSON(t, d, "pending_references_preserved")
	if original != before {
		t.Fatalf("the first preparation did not keep every column:\n%s\n%s", before, original)
	}
	if _, err := d.mig.UpTo(ctx, 5); err != nil {
		t.Fatal(err)
	}
	if err := postgres.CheckSchema(ctx, d.runtime); err == nil || !strings.Contains(err.Error(), "version 5") {
		t.Fatalf("the intermediate schema must refuse startup: %v", err)
	}
	if _, err := d.mig.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if got := scheduleJSON(t, d, "pending_references_preserved"); got != original {
		t.Fatalf("the second rollback replaced the original copy:\n%s\n%s", original, got)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the cycle changed the schedule (2 agendas, attempts 7, vencido, diagnóstico):\n%s\n%s", before, after)
	}
	if tableExists(t, d, "pending_references_preserved") {
		t.Fatal("the copy was left behind after the confirmed restoration")
	}
}

// Down cancelado enquanto outra sessão retém o lock do goose: a cópia original continua intacta, o schema não muda e a
// escrita do runtime é devolvida sem intervenção.
func TestACancelledRollbackKeepsTheOriginalCopyAndGivesTheWritesBack(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.DownTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	original := scheduleJSON(t, d, "pending_references_preserved")
	if _, err := d.mig.UpTo(ctx, 5); err != nil {
		t.Fatal(err)
	}
	provisionRuntime(t, d) // a tabela recriada pela 00005 ainda não tem os grants do runtime
	done, release, cancel := downHeldAfterPreparation(t, d)
	if got := scheduleJSON(t, d, "pending_references_preserved"); got != original {
		t.Fatalf("the preparation replaced the original copy while the goose lock was held elsewhere:\n%s\n%s", original, got)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("the cancelled rollback reported success")
	}
	release()
	if v := schemaVersion(t, d.owner); v != 5 {
		t.Fatalf("schema %d, want 5", v)
	}
	if got := scheduleJSON(t, d, "pending_references_preserved"); got != original {
		t.Fatalf("the cancelled rollback lost the original copy:\n%s\n%s", original, got)
	}
	// A escrita do runtime foi devolvida pelo próprio migrador (sem `migrate grants`).
	if _, err := d.runtime.Exec(ctx, `SELECT 1 FROM pending_references`); err != nil {
		t.Fatalf("the runtime read was not given back: %v", err)
	}
	if _, err := d.runtime.Exec(ctx, `UPDATE pending_references SET last_error = last_error WHERE transaction_id = $1`, open); err != nil {
		t.Fatalf("the runtime write was not given back after the cancelled rollback: %v", err)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the cancellation cycle changed the schedule:\n%s\n%s", before, after)
	}
}

// Um rollback que falha DEPOIS da preparação e é repetido depois de o runtime ter escrito: a cópia é renovada a partir
// da agenda original ainda viva (não restaura um estado antigo por cima de uma tentativa confirmada).
func TestARetriedRollbackSnapshotsWhatTheRuntimeWroteInBetween(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5
		t.Fatal(err)
	}
	done, release, cancel := downHeldAfterPreparation(t, d)
	cancel()
	if err := <-done; err == nil {
		t.Fatal("the cancelled rollback reported success")
	}
	release()
	// O runtime volta a trabalhar (a escrita foi devolvida) e confirma uma nova tentativa.
	if _, err := d.runtime.Exec(ctx, `UPDATE pending_references SET attempts = attempts + 1, last_error = 'after the cancelled rollback' WHERE transaction_id = $1`, open); err != nil {
		t.Fatal(err)
	}
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.Down(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the retried rollback restored a stale copy:\n%s\n%s", before, after)
	}
}

// Depois da reaplicação parcial, uma agenda que não é a reconstrução intacta da 00005 (dados que a cópia original não
// tem) impede o rollback ANTES de qualquer alteração: nada é descartado, copiado, revogado ou revertido.
func TestARollbackThatWouldLoseScheduleDataAfterAPartialReapplyIsRefusedUntouched(t *testing.T) {
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
	provisionRuntime(t, d)
	liveBefore := scheduleJSON(t, d, "pending_references")
	_, err := d.mig.Down(ctx)
	if err == nil || !strings.Contains(err.Error(), open.String()) {
		t.Fatalf("the rollback must be refused naming the schedule it would lose: %v", err)
	}
	if v := schemaVersion(t, d.owner); v != 5 {
		t.Fatalf("schema %d after the refusal, want 5", v)
	}
	if scheduleJSON(t, d, "pending_references") != liveBefore || scheduleJSON(t, d, "pending_references_preserved") != copyBefore {
		t.Fatal("the refused rollback changed the schedule or the copy")
	}
	if _, err := d.runtime.Exec(ctx, `SELECT 1 FROM pending_references`); err != nil {
		t.Fatalf("the refusal left the runtime without access: %v", err)
	}
}

// Dois migradores concorrentes: um Up que chega durante a preparação do Down espera o fim dele (não decide a versão nem
// restaura no meio), e o ciclo termina com a agenda original.
func TestAMigratorArrivingDuringTheRollbackPreparationWaitsForIt(t *testing.T) {
	ctx := context.Background()
	d, _, _ := reauditSchedule(t)
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5
		t.Fatal(err)
	}
	gate, err := d.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(ctx) //nolint:errcheck
	if _, err := gate.Exec(ctx, `LOCK TABLE wallets IN ROW EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	downDone := make(chan error, 1)
	go func() { _, err := d.mig.Down(ctx); downDone <- err }()
	eventually(t, "the preparation to wait for the writers' lock", 10*time.Second, func() bool {
		return observe(t, `SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND wait_event_type = 'Lock'
			AND query LIKE 'LOCK TABLE wallets, wager_transactions, pending_references%'`, d.name) > 0
	})
	other, err := postgres.NewMigrator(d.url)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	upDone := make(chan error, 1)
	go func() { _, err := other.Up(ctx); upDone <- err }()
	eventually(t, "the second migrator to wait for the first", 10*time.Second, func() bool {
		return migratorWaiters(t, d) > 0
	})
	select {
	case err := <-upDone:
		t.Fatalf("the second migrator ran during the preparation (err=%v)", err)
	default:
	}
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-downDone; err != nil {
		t.Fatalf("down: %v", err)
	}
	if err := <-upDone; err != nil {
		t.Fatalf("up: %v", err)
	}
	if v := schemaVersion(t, d.owner); v != finalVersion(t) {
		t.Fatalf("schema %d", v)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the concurrent cycle changed the schedule:\n%s\n%s", before, after)
	}
}

// --- (3) exclusão efetiva dos escritores -----------------------------------------------------------------------

// Mesmo papel restrito do runtime, mas com SELECT/UPDATE somente por coluna (sem privilégio de tabela): depois do
// snapshot e antes do DROP nenhuma escrita é confirmada, e a agenda volta com os valores originais.
func TestAColumnOnlyRuntimeCannotWriteAfterThePreservationSnapshot(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5
		t.Fatal(err)
	}
	role := pgx.Identifier{env.runtimeRole}.Sanitize()
	for _, stmt := range []string{
		`REVOKE SELECT, INSERT ON pending_references FROM ` + role,
		`GRANT SELECT (transaction_id, wallet_id, first_persisted_at, expires_at, next_attempt_at, attempts, last_attempt_at, last_error, completed_at) ON pending_references TO ` + role,
	} {
		if _, err := d.owner.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := d.runtime.Exec(ctx, `UPDATE pending_references SET last_error = last_error WHERE transaction_id = $1`, open); err != nil {
		t.Fatalf("setup: the column-only runtime must be able to write before the rollback: %v", err)
	}
	done, release, _ := downHeldAfterPreparation(t, d)
	_, writeErr := d.runtime.Exec(ctx, `UPDATE pending_references SET attempts = attempts + 1, last_error = 'committed after the snapshot' WHERE transaction_id = $1`, open)
	expectPg(t, "column-only runtime write after the snapshot", writeErr, "42501", "")
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the schedule changed:\n%s\n%s", before, after)
	}
}

// Controle positivo: o papel de runtime PADRÃO continua bloqueado (escrita, coluna e leitura da cópia) e recupera os
// grants ao cancelar.
func TestTheDefaultRuntimeIsBlockedDuringTheRollbackAndRecoversWhenItIsCancelled(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	before := scheduleJSON(t, d, "pending_references")
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5
		t.Fatal(err)
	}
	done, release, cancel := downHeldAfterPreparation(t, d)
	var colWrite, copyRead bool
	if err := d.owner.QueryRow(ctx, `SELECT has_any_column_privilege($1, 'pending_references', 'UPDATE'), has_table_privilege($1, 'pending_references_preserved', 'SELECT')`,
		env.runtimeRole).Scan(&colWrite, &copyRead); err != nil {
		t.Fatal(err)
	}
	if colWrite || copyRead {
		t.Fatalf("default runtime during the rollback: column update=%v preserved-copy read=%v", colWrite, copyRead)
	}
	_, err := d.runtime.Exec(ctx, `UPDATE pending_references SET attempts = attempts + 1 WHERE transaction_id = $1`, open)
	expectPg(t, "default runtime write", err, "42501", "")
	cancel()
	if err := <-done; err == nil {
		t.Fatal("the cancelled rollback reported success")
	}
	release()
	if _, err := d.runtime.Exec(ctx, `UPDATE pending_references SET last_error = last_error WHERE transaction_id = $1`, open); err != nil {
		t.Fatalf("the default runtime did not recover its write: %v", err)
	}
	if _, err := d.mig.Up(ctx); err != nil {
		t.Fatal(err)
	}
	if after := scheduleJSON(t, d, "pending_references"); after != before {
		t.Fatalf("the cancellation changed the schedule:\n%s\n%s", before, after)
	}
}

func createRole(t *testing.T, d *upgradeDB, attrs string, statements ...string) string {
	t.Helper()
	ctx := context.Background()
	name := "wager_aud_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	if _, err := d.owner.Exec(ctx, `CREATE ROLE `+name+` `+attrs); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), d.url)
		if err == nil {
			defer c.Close(context.Background())
			_, _ = c.Exec(context.Background(), `DROP OWNED BY `+name)
			_, _ = c.Exec(context.Background(), `DROP ROLE IF EXISTS `+name)
		}
	})
	for _, s := range statements {
		if _, err := d.owner.Exec(ctx, strings.ReplaceAll(s, "$ROLE", name)); err != nil {
			t.Fatal(err)
		}
	}
	return name
}

// PUBLIC (escrita por coluna) e um papel que só herda o privilégio de um grupo: depois da preparação nenhum dos dois pode
// escrever na agenda.
func TestPublicAndInheritedWriteAccessAreExcludedDuringTheRollback(t *testing.T) {
	ctx := context.Background()
	d, _, _ := reauditSchedule(t)
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5
		t.Fatal(err)
	}
	group := createRole(t, d, `NOLOGIN`, `GRANT UPDATE (last_error), INSERT ON pending_references TO $ROLE`)
	member := createRole(t, d, `LOGIN INHERIT`, `GRANT `+group+` TO $ROLE`)
	if _, err := d.owner.Exec(ctx, `GRANT UPDATE (last_error) ON pending_references TO PUBLIC`); err != nil {
		t.Fatal(err)
	}
	outsider := createRole(t, d, `LOGIN`) // só tem o acesso concedido a PUBLIC
	if !canWriteSchedule(t, d, member) || !canWriteSchedule(t, d, outsider) {
		t.Fatal("setup: the member and the outsider must be able to write before the rollback")
	}
	_, release, cancel := downHeldAfterPreparation(t, d)
	defer release()
	defer cancel()
	for _, role := range []string{member, group, outsider, env.runtimeRole} {
		if canWriteSchedule(t, d, role) {
			t.Errorf("role %s can still write the schedule during the rollback", role)
		}
	}
}

func canWriteSchedule(t *testing.T, d *upgradeDB, role string) bool {
	t.Helper()
	var ok bool
	if err := d.owner.QueryRow(context.Background(),
		`SELECT has_table_privilege($1, 'public.pending_references', 'INSERT,UPDATE,DELETE,TRUNCATE') OR has_any_column_privilege($1, 'public.pending_references', 'INSERT,UPDATE')`, role).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

// Uma configuração que o migrador não consegue excluir (um papel com escrita irrevogável aqui, por pertencer a um papel
// predefinido de escrita) recusa o rollback ANTES de qualquer alteração.
func TestAWriterThatCannotBeExcludedRefusesTheRollbackBeforeChangingAnything(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	if _, err := d.mig.Down(ctx); err != nil { // 6 -> 5
		t.Fatal(err)
	}
	writer := createRole(t, d, `LOGIN`, `GRANT pg_write_all_data TO $ROLE`)
	before := scheduleJSON(t, d, "pending_references")
	_, err := d.mig.Down(ctx)
	if err == nil || !strings.Contains(err.Error(), writer) {
		t.Fatalf("the rollback must be refused naming the writer it cannot exclude: %v", err)
	}
	if v := schemaVersion(t, d.owner); v != 5 {
		t.Fatalf("schema %d after the refusal, want 5", v)
	}
	if tableExists(t, d, "pending_references_preserved") {
		t.Fatal("the refused rollback left a copy behind")
	}
	if scheduleJSON(t, d, "pending_references") != before {
		t.Fatal("the refused rollback changed the schedule")
	}
	if _, err := d.runtime.Exec(ctx, `UPDATE pending_references SET last_error = last_error WHERE transaction_id = $1`, open); err != nil {
		t.Fatalf("the refusal revoked the runtime's access: %v", err)
	}
}

// --- binário público --------------------------------------------------------------------------------------------

// Pelo binário `migrate`: o rollback que perderia dados da agenda é recusado (código de saída ≠ 0, ids na mensagem),
// sem alterar versão, agenda, cópia nem grants.
func TestMigrateCommandRefusesARollbackThatWouldLoseTheSchedule(t *testing.T) {
	ctx := context.Background()
	d, open, _ := reauditSchedule(t)
	if _, err := d.mig.DownTo(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if _, err := d.mig.UpTo(ctx, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := d.owner.Exec(ctx, `UPDATE pending_references SET attempts = 2 WHERE transaction_id = $1`, open); err != nil {
		t.Fatal(err)
	}
	liveBefore, copyBefore := scheduleJSON(t, d, "pending_references"), scheduleJSON(t, d, "pending_references_preserved")

	bin := t.TempDir() + "/migrate"
	build := exec.Command("go", "build", "-o", bin, "./cmd/migrate")
	build.Dir = "../.."
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build migrate: %v\n%s", err, out)
	}
	for _, args := range [][]string{{"down"}, {"down-to", "3"}} {
		cmd := exec.Command(bin, args...)
		cmd.Env = append(os.Environ(), "MIGRATE_DATABASE_URL="+d.url, "DATABASE_URL=", "RUNTIME_DB_ROLE=")
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || !strings.Contains(string(out), open.String()) {
			t.Fatalf("migrate %v must fail naming the schedule: err=%v\n%s", args, err, out)
		}
		if v := schemaVersion(t, d.owner); v != 5 {
			t.Fatalf("migrate %v: schema %d, want 5", args, v)
		}
		if scheduleJSON(t, d, "pending_references") != liveBefore || scheduleJSON(t, d, "pending_references_preserved") != copyBefore {
			t.Fatalf("migrate %v changed the schedule or the copy", args)
		}
	}
}
