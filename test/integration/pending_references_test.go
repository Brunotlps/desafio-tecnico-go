//go:build integration

package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

// Referências pendentes: persistência junto com a agenda, retomada pelo estado durável e política
// de expiração. O relógio é injetado: o TTL de 15 minutos e o backoff são atravessados sem dormir.

func TestPendingReferenceIsPersistedWithItsScheduleAndResolvedLater(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")
	balBefore, verBefore := walletState(t, wallet)

	pend := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	mustStatus(t, pend, domain.StatusPendingReference, "")
	txID := pend.Transaction.ID()
	if pend.Replay || pend.Transaction.ResultBalance().IsInitialized() {
		t.Fatalf("a pending operation must not carry an invented balance: %+v", pend)
	}
	p := pendingOf(t, txID)
	if p.Rows != 1 || p.Completed || !p.First.Equal(clock.Now()) || !p.Expires.Equal(clock.Now().Add(15*time.Minute)) || p.Attempts != 0 {
		t.Fatalf("schedule: %+v (clock %v)", p, clock.Now())
	}
	if eventCount(t, txID, "WagerTransactionPendingReference") != 1 {
		t.Fatal("the initial event was not written exactly once")
	}
	if bal, ver := walletState(t, wallet); bal != balBefore || ver != verBefore {
		t.Fatal("a pending operation changed the wallet")
	}
	if n, _ := ledgerOf(t, txID); n != 0 {
		t.Fatal("a pending operation wrote to the ledger")
	}

	// Replay de pendência: o estado atual, sem outra agenda, sem renovar o prazo, sem novo evento.
	clock.Advance(3 * time.Minute)
	replay := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	if !replay.Replay || replay.Transaction.ID() != txID || replay.Transaction.Status() != domain.StatusPendingReference {
		t.Fatalf("replay: %+v", replay)
	}
	if again := pendingOf(t, txID); again.Rows != 1 || !again.Expires.Equal(p.Expires) || !again.First.Equal(p.First) {
		t.Fatalf("replay changed the schedule: %+v -> %+v", p, again)
	}
	if eventCount(t, txID, "WagerTransactionPendingReference") != 1 {
		t.Fatal("replay emitted the initial event again")
	}

	// Uma verificação sem a referência: reagenda, conta a tentativa, não renova o prazo nem repete o evento.
	clock.Advance(10 * time.Second)
	done, err := process(t, svc, txID)
	if err != nil || done {
		t.Fatalf("attempt without reference: done=%v err=%v", done, err)
	}
	if after := pendingOf(t, txID); after.Attempts != 1 || !after.Expires.Equal(p.Expires) || !after.Next.After(clock.Now()) || after.Completed {
		t.Fatalf("after an unsuccessful check: %+v", after)
	}
	if eventCount(t, txID, "WagerTransactionPendingReference") != 1 || eventCount(t, txID, "WagerTransactionRejected") != 0 {
		t.Fatal("events changed while waiting")
	}

	// A BET chega; a retomada conclui a MESMA operação (id, hash, chave) uma única vez.
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "25.00", ""))
	hashBefore := hashOf(t, txID)
	done, err = process(t, svc, txID)
	if err != nil || !done {
		t.Fatalf("resolution: done=%v err=%v", done, err)
	}
	got, err := svc.GetTransaction(context.Background(), txID)
	if err != nil || got.Status() != domain.StatusProcessed || got.ResultBalance().Amount() != "100.00" || got.ReferenceTransactionID() == nil {
		t.Fatalf("resolved: %+v %v", got, err)
	}
	if hashOf(t, txID) != hashBefore {
		t.Fatal("the resumption changed the payload hash")
	}
	if n, dir := ledgerOf(t, txID); n != 1 || dir != "CREDIT" {
		t.Fatalf("ledger: %d %s", n, dir)
	}
	if eventCount(t, txID, "WagerTransactionProcessed") != 1 || eventCount(t, txID, "WalletBalanceChanged") != 1 || eventCount(t, txID, "WagerTransactionPendingReference") != 1 {
		t.Fatal("completion events")
	}
	if c := pendingOf(t, txID); !c.Completed || c.Attempts != 2 {
		t.Fatalf("schedule after completion: %+v", c)
	}
	// Terminal: nova tentativa não faz nada; replay devolve o saldo original mesmo após outra movimentação.
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet-2"), "10.00", ""))
	if done, err := svc.ProcessPending(context.Background(), app.PendingReference{TransactionID: txID, WalletID: uuid.MustParse(wallet)}); err != nil || done {
		t.Fatalf("second attempt: done=%v err=%v", done, err)
	}
	replay = mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	if !replay.Replay || replay.Transaction.ResultBalance().Amount() != "100.00" {
		t.Fatalf("terminal replay: %+v", replay)
	}
	assertLedgerMatchesBalance(t, wallet)
}

func hashOf(t *testing.T, txID uuid.UUID) string {
	t.Helper()
	var h string
	if err := testPool.QueryRow(context.Background(), `SELECT payload_hash || '/' || idempotency_key FROM wager_transactions WHERE id = $1`, txID).Scan(&h); err != nil {
		t.Fatal(err)
	}
	return h
}

// A referência chega depois do prazo e ainda assim resolve, se a operação continua pendente.
func TestAProcessedReferenceResolvesEvenAfterTheTTL(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	pend := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rb-win"), "30.00", id("win")))
	txID := pend.Transaction.ID()
	clock.Advance(16 * time.Minute) // além do TTL de 15 min
	mustSubmit(t, svc, op(wallet, player, "WIN", id("win"), "30.00", ""))
	if done, err := process(t, svc, txID); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	got, _ := svc.GetTransaction(context.Background(), txID)
	if got.Status() != domain.StatusProcessed || got.ResultBalance().Amount() != "100.00" {
		t.Fatalf("after the TTL: %s %s %s", got.Status(), got.FailureCode(), got.ResultBalance().Amount())
	}
	assertLedgerMatchesBalance(t, wallet)
}

func TestExpiryIsTerminalAndNeverReopens(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	pend := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	txID := pend.Transaction.ID()
	clock.Advance(15*time.Minute + time.Second)
	if done, err := process(t, svc, txID); err != nil || !done {
		t.Fatalf("expiry: done=%v err=%v", done, err)
	}
	got, _ := svc.GetTransaction(context.Background(), txID)
	if got.Status() != domain.StatusRejected || got.FailureCode() != domain.FailureReferenceNotFound || got.ResultBalance().Amount() != "100.00" {
		t.Fatalf("expired: %s %s", got.Status(), got.FailureCode())
	}
	if eventCount(t, txID, "WagerTransactionRejected") != 1 || eventCount(t, txID, "WalletBalanceChanged") != 0 {
		t.Fatal("expiry events")
	}
	if n, _ := ledgerOf(t, txID); n != 0 {
		t.Fatal("expiry wrote to the ledger")
	}
	// A referência chega depois: a rejeição confirmada não é reaberta, nem por tentativa nem por replay.
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "25.00", ""))
	if done, err := svc.ProcessPending(context.Background(), app.PendingReference{TransactionID: txID, WalletID: uuid.MustParse(wallet)}); err != nil || done {
		t.Fatalf("late attempt: done=%v err=%v", done, err)
	}
	replay := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	if !replay.Replay || replay.Transaction.Status() != domain.StatusRejected || replay.Transaction.FailureCode() != domain.FailureReferenceNotFound {
		t.Fatalf("replay: %+v", replay)
	}
	if bal, _ := walletState(t, wallet); bal != 75_00 { // só a BET
		t.Fatalf("balance %d", bal)
	}
}

func TestAReferenceThatIsStillPendingTimesOutWithItsOwnCode(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	// O alvo é um WIN que espera a sua própria referência (BET ausente): PENDING_REFERENCE.
	target := mustSubmit(t, svc, op(wallet, player, "WIN", id("win"), "20.00", id("missing-bet")))
	mustStatus(t, target, domain.StatusPendingReference, "")
	dependent := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("rb"), "20.00", id("win")))
	mustStatus(t, dependent, domain.StatusPendingReference, "")

	// Antes do prazo: continua esperando (o alvo existe, mas ainda pendente).
	clock.Advance(time.Minute)
	if done, err := process(t, svc, dependent.Transaction.ID()); err != nil || done {
		t.Fatalf("before the TTL: done=%v err=%v", done, err)
	}
	// Depois do prazo: REFERENCE_PENDING_TIMEOUT (distinto de REFERENCE_NOT_FOUND).
	clock.Advance(15 * time.Minute)
	if done, err := process(t, svc, dependent.Transaction.ID()); err != nil || !done {
		t.Fatalf("after the TTL: done=%v err=%v", done, err)
	}
	got, _ := svc.GetTransaction(context.Background(), dependent.Transaction.ID())
	if got.Status() != domain.StatusRejected || got.FailureCode() != domain.FailureReferencePendingTimeout {
		t.Fatalf("dependent: %s %s", got.Status(), got.FailureCode())
	}
	// O alvo, ele mesmo, expira por ausência da sua referência.
	if done, err := process(t, svc, target.Transaction.ID()); err != nil || !done {
		t.Fatalf("target: done=%v err=%v", done, err)
	}
	tgt, _ := svc.GetTransaction(context.Background(), target.Transaction.ID())
	if tgt.FailureCode() != domain.FailureReferenceNotFound {
		t.Fatalf("target code %s", tgt.FailureCode())
	}
	if bal, ver := walletState(t, wallet); bal != 100_00 || ver != 1 {
		t.Fatalf("wallet changed: %d/%d", bal, ver)
	}
}

// A referência existe mas terminou sem sucesso depois de a operação já estar esperando: rejeição definitiva.
func TestAReferenceThatFinishesWithoutSuccessRejectsTheWaitingOperation(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "10.00")

	pend := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "50.00", id("bet")))
	mustStatus(t, mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "50.00", "")), domain.StatusRejected, domain.FailureInsufficientFunds)
	if done, err := process(t, svc, pend.Transaction.ID()); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	got, _ := svc.GetTransaction(context.Background(), pend.Transaction.ID())
	if got.Status() != domain.StatusRejected || got.FailureCode() != domain.FailureReferenceNotProcessed {
		t.Fatalf("%s %s", got.Status(), got.FailureCode())
	}
}

// Uma retomada concorrente de pendências do mesmo alvo respeita a regra de uma reversão por alvo.
func TestResumedReversalsRespectTheOneReversalRule(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")

	a := mustSubmit(t, svc, op(wallet, player, "REFUND", id("a"), "25.00", id("bet")))
	b := mustSubmit(t, svc, op(wallet, player, "ROLLBACK", id("b"), "25.00", id("bet")))
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "25.00", ""))
	for _, tx := range []uuid.UUID{a.Transaction.ID(), b.Transaction.ID()} {
		if _, err := process(t, svc, tx); err != nil {
			t.Fatal(err)
		}
	}
	ga, _ := svc.GetTransaction(context.Background(), a.Transaction.ID())
	gb, _ := svc.GetTransaction(context.Background(), b.Transaction.ID())
	if ga.Status() != domain.StatusProcessed || gb.Status() != domain.StatusRejected || gb.FailureCode() != domain.FailureAlreadyReversed {
		t.Fatalf("a=%s b=%s/%s", ga.Status(), gb.Status(), gb.FailureCode())
	}
	assertLedgerMatchesBalance(t, wallet)
}

// Dez executores (instâncias separadas do serviço) disputam a mesma pendência: um único efeito.
func TestManyExecutorsOnOnePendingReferenceApplyItOnce(t *testing.T) {
	clock := newFakeClock()
	id := scoped()
	seed := newServiceWith(t, clock, nil)
	wallet, player := svcOpen(t, seed, "100.00")
	pend := mustSubmit(t, seed, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	mustSubmit(t, seed, op(wallet, player, "BET", id("bet"), "25.00", ""))

	execs := make([]*app.Service, 10)
	for i := range execs {
		execs[i] = newServiceWith(t, clock, nil)
	}
	ref, err := loadPending(pend.Transaction.ID())
	if err != nil {
		t.Fatal(err)
	}
	var completed atomic.Int64
	parallel(len(execs), func(i int) struct{} {
		done, err := execs[i].ProcessPending(context.Background(), ref)
		if err != nil {
			t.Errorf("executor %d: %v", i, err)
		}
		if done {
			completed.Add(1)
		}
		return struct{}{}
	})
	if completed.Load() != 1 {
		t.Fatalf("completions = %d, want exactly 1", completed.Load())
	}
	if n, _ := ledgerOf(t, pend.Transaction.ID()); n != 1 {
		t.Fatalf("ledger entries = %d", n)
	}
	if bal, _ := walletState(t, wallet); bal != 100_00 {
		t.Fatalf("balance %d", bal)
	}
	assertLedgerMatchesBalance(t, wallet)
}

// Falha de consulta nunca prova ausência: a tentativa é abortada, o prazo original é preservado e o
// estado fica como estava, mesmo com o prazo já vencido.
func TestAQueryFailureNeverProducesReferenceNotFound(t *testing.T) {
	clock := newFakeClock()
	var failing atomic.Bool
	wrap := func(inner app.UnitOfWork) app.UnitOfWork {
		return wrappedUoW{UnitOfWork: inner, do: func(ctx context.Context, u app.UnitOfWork, fn func(context.Context, app.Repositories) error) error {
			return u.Do(ctx, func(ctx context.Context, r app.Repositories) error {
				if failing.Load() {
					r.Transactions = failingLookup{r.Transactions}
				}
				return fn(ctx, r)
			})
		}}
	}
	svc := newServiceWith(t, clock, wrap)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")
	pend := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	txID := pend.Transaction.ID()
	before := pendingOf(t, txID)

	clock.Advance(20 * time.Minute) // prazo vencido
	failing.Store(true)
	if done, err := process(t, svc, txID); err == nil || done || !errors.Is(err, app.ErrTransient) {
		t.Fatalf("a failing lookup must abort the attempt: done=%v err=%v", done, err)
	}
	got, _ := svc.GetTransaction(context.Background(), txID)
	if got.Status() != domain.StatusPendingReference || got.FailureCode() != "" {
		t.Fatalf("after the failure: %s %s", got.Status(), got.FailureCode())
	}
	if after := pendingOf(t, txID); !after.Expires.Equal(before.Expires) || after.Completed || after.Rows != 1 {
		t.Fatalf("schedule changed: %+v -> %+v", before, after)
	}
	// Restabelecida a consulta, a decisão é tomada sobre o estado real (ainda ausente após o prazo).
	failing.Store(false)
	if done, err := process(t, svc, txID); err != nil || !done {
		t.Fatalf("done=%v err=%v", done, err)
	}
	got, _ = svc.GetTransaction(context.Background(), txID)
	if got.FailureCode() != domain.FailureReferenceNotFound {
		t.Fatalf("code %s", got.FailureCode())
	}
}

type failingLookup struct{ app.TransactionRepository }

func (failingLookup) FindByExternalID(context.Context, string, string) (*domain.WagerTransaction, error) {
	return nil, errors.Join(app.ErrTransient, errors.New("injected lookup failure"))
}

// Falha ANTES do commit não deixa efeito; falha DEPOIS (resultado desconhecido) é recuperada pelo estado
// durável sem duplicar o movimento.
func TestResumeFailuresBeforeAndAfterCommitNeverDuplicate(t *testing.T) {
	clock := newFakeClock()
	var mode atomic.Int32 // 0 normal · 1 falha antes do commit · 2 resultado desconhecido depois do commit
	wrap := func(inner app.UnitOfWork) app.UnitOfWork {
		return wrappedUoW{UnitOfWork: inner, do: func(ctx context.Context, u app.UnitOfWork, fn func(context.Context, app.Repositories) error) error {
			switch mode.Load() {
			case 1:
				return u.Do(ctx, func(ctx context.Context, r app.Repositories) error {
					if err := fn(ctx, r); err != nil {
						return err
					}
					return errors.Join(app.ErrTransient, errors.New("crash before commit")) // desfaz a transação
				})
			case 2:
				if err := u.Do(ctx, fn); err != nil {
					return err
				}
				return errors.Join(app.ErrCommitUnknown, errors.New("connection lost after commit"))
			}
			return u.Do(ctx, fn)
		}}
	}
	svc := newServiceWith(t, clock, wrap)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")
	pend := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "25.00", ""))
	txID := pend.Transaction.ID()

	mode.Store(1)
	if _, err := process(t, svc, txID); err == nil {
		t.Fatal("expected the injected failure")
	}
	if n, _ := ledgerOf(t, txID); n != 0 || pendingOf(t, txID).Completed {
		t.Fatal("a failure before the commit left effects")
	}
	if got, _ := svc.GetTransaction(context.Background(), txID); got.Status() != domain.StatusPendingReference {
		t.Fatalf("status %s", got.Status())
	}

	mode.Store(2)
	if _, err := process(t, svc, txID); !errors.Is(err, app.ErrCommitUnknown) {
		t.Fatalf("err %v", err)
	}
	if n, _ := ledgerOf(t, txID); n != 1 {
		t.Fatalf("the committed movement is missing: %d", n)
	}
	mode.Store(0)
	if done, err := process2(svc, txID); err != nil || done {
		t.Fatalf("retry after an uncertain commit: done=%v err=%v", done, err)
	}
	if n, _ := ledgerOf(t, txID); n != 1 {
		t.Fatalf("duplicated movement: %d", n)
	}
	if bal, _ := walletState(t, wallet); bal != 100_00 {
		t.Fatalf("balance %d", bal)
	}
	assertLedgerMatchesBalance(t, wallet)
}

// process2 tenta mesmo depois de a agenda ter sido concluída (loadPending só enxerga a linha, aberta ou não).
func process2(svc *app.Service, txID uuid.UUID) (bool, error) {
	ref, err := loadPending(txID)
	if err != nil {
		return false, err
	}
	return svc.ProcessPending(context.Background(), ref)
}

// Uma carteira ocupada é pulada (SKIP LOCKED) sem travar as demais: carteiras distintas progridem.
func TestABusyWalletIsSkippedAndOtherWalletsProgress(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	busyW, busyP := svcOpen(t, svc, "100.00")
	freeW, freeP := svcOpen(t, svc, "100.00")
	busy := mustSubmit(t, svc, op(busyW, busyP, "REFUND", id("busy"), "25.00", id("bet-busy")))
	free := mustSubmit(t, svc, op(freeW, freeP, "REFUND", id("free"), "25.00", id("bet-free")))
	mustSubmit(t, svc, op(busyW, busyP, "BET", id("bet-busy"), "25.00", ""))
	mustSubmit(t, svc, op(freeW, freeP, "BET", id("bet-free"), "25.00", ""))

	ctx := context.Background()
	lock, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err := lock.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, busyW); err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Minute)
	stop := make(chan struct{})
	svc.ProcessDue(ctx, stop) // varredura completa, com a carteira ocupada

	if got, _ := svc.GetTransaction(ctx, free.Transaction.ID()); got.Status() != domain.StatusProcessed {
		t.Fatalf("the free wallet did not progress: %s", got.Status())
	}
	if got, _ := svc.GetTransaction(ctx, busy.Transaction.ID()); got.Status() != domain.StatusPendingReference {
		t.Fatalf("the busy wallet was processed while locked: %s", got.Status())
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	svc.ProcessDue(ctx, stop)
	if got, _ := svc.GetTransaction(ctx, busy.Transaction.ID()); got.Status() != domain.StatusProcessed {
		t.Fatalf("the wallet was not resumed after being released: %s", got.Status())
	}
}

// Relógio de uma instância atrasado em relação à última atualização gravada por outra: usa-se o maior
// instante; a retomada não falha por um erro do relógio interno nem o atribui ao cliente.
func TestResumeToleratesABackwardsClock(t *testing.T) {
	clock := newFakeClock()
	svc := newServiceWith(t, clock, nil)
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")
	pend := mustSubmit(t, svc, op(wallet, player, "REFUND", id("refund"), "25.00", id("bet")))
	mustSubmit(t, svc, op(wallet, player, "BET", id("bet"), "25.00", ""))
	// Outra instância gravou a transação "no futuro" (atualização à frente do relógio desta).
	future := clock.Now().Add(5 * time.Second)
	if _, err := testPool.Exec(context.Background(), `UPDATE wager_transactions SET updated_at = $2 WHERE id = $1`, pend.Transaction.ID(), future); err != nil {
		// o dono do schema é o testPool; a atualização de updated_at é permitida
		t.Fatal(err)
	}
	if done, err := process(t, svc, pend.Transaction.ID()); err != nil || !done {
		t.Fatalf("backwards clock: done=%v err=%v", done, err)
	}
	got, _ := svc.GetTransaction(context.Background(), pend.Transaction.ID())
	if got.Status() != domain.StatusProcessed || got.UpdatedAt().Before(future) {
		t.Fatalf("%s updatedAt=%v want >= %v", got.Status(), got.UpdatedAt(), future)
	}
}
