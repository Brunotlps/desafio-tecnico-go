//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/infra/postgres"
	"desafio-tecnico-go/internal/outbox"
)

// Publisher da outbox (incremento 3): reserva por lease com token, escritas condicionadas à posse e privilégios mínimos.
// Cada teste usa um banco EXCLUSIVO (versão final do schema) e, quando precisa do broker, um conjunto próprio de recursos e
// principais: a outbox compartilhada do banco de teste contém eventos de outros testes que um publisher publicaria.

type pubEnv struct {
	t     *testing.T
	db    *upgradeDB
	owner *pgxpool.Pool // papel dono, seguro para uso concorrente
	proof *proofEnv     // tópico FIFO, fila de auditoria e principais próprios (nil se o teste não precisa do broker)
}

func newStoreEnv(t *testing.T) *pubEnv {
	t.Helper()
	final, err := postgres.ExpectedVersion()
	if err != nil {
		t.Fatal(err)
	}
	db := newUpgradeDB(t, "outbox", final)
	owner, err := pgxpool.New(context.Background(), db.url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.Close)
	return &pubEnv{t: t, db: db, owner: owner}
}

func newPubEnv(t *testing.T) *pubEnv {
	t.Helper()
	e := newStoreEnv(t)
	e.proof = provisionProof(t)
	return e
}

func (e *pubEnv) store() *postgres.OutboxStore {
	return postgres.NewOutboxStore(postgres.StaticPool(e.db.runtime))
}

// envelope é um snapshot válido (formato do evento de integração) para o evento id.
func envelope(id uuid.UUID, eventType, wallet string, walletVersion int) string {
	return fmt.Sprintf(`{"eventId":%q,"eventType":%q,"aggregateId":%q,"correlationId":"corr-1","occurredAt":"2026-10-15T00:00:00.000Z","version":1,`+
		`"data":{"walletId":%q,"walletVersion":%d,"balanceAfter":{"amount":"75.00","currency":"BRL"}}}`,
		id, eventType, uuid.New(), wallet, walletVersion)
}

// insertEvent grava um evento direto (papel dono), como se tivesse sido confirmado pela transação financeira.
func (e *pubEnv) insertEvent(wallet, eventType string) uuid.UUID {
	e.t.Helper()
	id := uuid.New()
	if _, err := e.owner.Exec(context.Background(), `INSERT INTO outbox_events
		(id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, occurred_at)
		VALUES ($1,'Wallet',$2,$3,1,'corr-1',$4::jsonb, now())`, id, uuid.New(), eventType, envelope(id, eventType, wallet, 1)); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *pubEnv) insertEvents(n int, wallets int) []uuid.UUID {
	e.t.Helper()
	ids := make([]uuid.UUID, n)
	ws := make([]string, wallets)
	for i := range ws {
		ws[i] = uuid.NewString()
	}
	for i := range ids {
		ids[i] = e.insertEvent(ws[i%wallets], "WalletBalanceChanged")
	}
	return ids
}

type eventState struct {
	Published bool
	Attempts  int
	LastError string
	NextAt    time.Time
	Token     *uuid.UUID
	Expires   *time.Time
	Body      string
}

func (e *pubEnv) state(id uuid.UUID) eventState {
	e.t.Helper()
	var s eventState
	var pub *time.Time
	var lastErr *string
	if err := e.owner.QueryRow(context.Background(), `SELECT published_at, attempts, last_error, next_attempt_at, lease_token, lease_expires_at, payload::text
		FROM outbox_events WHERE id = $1`, id).Scan(&pub, &s.Attempts, &lastErr, &s.NextAt, &s.Token, &s.Expires, &s.Body); err != nil {
		e.t.Fatal(err)
	}
	s.Published = pub != nil
	if lastErr != nil {
		s.LastError = *lastErr
	}
	return s
}

func (e *pubEnv) countUnpublished() int64 {
	e.t.Helper()
	var n int64
	if err := e.owner.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// dbPassed espera o relógio do BANCO passar de t.
func (e *pubEnv) dbPassed(t time.Time) {
	e.t.Helper()
	eventually(e.t, "the database clock to pass the lease", 15*time.Second, func() bool {
		var past bool
		if err := e.owner.QueryRow(context.Background(), `SELECT clock_timestamp() > $1`, t).Scan(&past); err != nil {
			e.t.Fatal(err)
		}
		return past
	})
}

func mustReserve(t *testing.T, s *postgres.OutboxStore, lease time.Duration) *outbox.Reservation {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // uma reserva que espera por lock é uma falha, não um travamento
	defer cancel()
	r, err := s.Reserve(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	if r == nil {
		t.Fatal("expected an eligible event")
	}
	return r
}

func mustNotReserve(t *testing.T, s *postgres.OutboxStore, why string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := s.Reserve(ctx, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if r != nil {
		t.Fatalf("%s: event %s must not be eligible", why, r.EventID)
	}
}

func TestOutboxEventsOfAnOpenOrRolledBackTransactionAreNeverReserved(t *testing.T) {
	e := newStoreEnv(t)
	ctx := context.Background()
	st := e.store()

	open, err := e.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := uuid.New()
	if _, err := open.Exec(ctx, `INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, occurred_at)
		VALUES ($1,'Wallet',$2,'WalletBalanceChanged',1,'c',$3::jsonb, now())`, pending, uuid.New(), envelope(pending, "WalletBalanceChanged", "w1", 1)); err != nil {
		t.Fatal(err)
	}
	mustNotReserve(t, st, "an event of an open transaction")

	rolled, _ := e.owner.Begin(ctx)
	rid := uuid.New()
	if _, err := rolled.Exec(ctx, `INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, occurred_at)
		VALUES ($1,'Wallet',$2,'WalletBalanceChanged',1,'c',$3::jsonb, now())`, rid, uuid.New(), envelope(rid, "WalletBalanceChanged", "w2", 1)); err != nil {
		t.Fatal(err)
	}
	if err := rolled.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := open.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	r := mustReserve(t, st, 20*time.Second)
	if r.EventID != pending || r.WalletID != "w1" || r.Attempts != 1 || r.Token == uuid.Nil || r.Type != "WalletBalanceChanged" {
		t.Fatalf("unexpected reservation %+v", r)
	}
	if s := e.state(pending); s.Body != r.Body || !s.Expires.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("the reserved body must be the stored snapshot: %q vs %q", r.Body, s.Body)
	}
	mustNotReserve(t, st, "the rolled-back event does not exist and the other one is reserved")
	if n := e.countUnpublished(); n != 1 {
		t.Fatalf("%d unpublished, want 1 (the rolled-back event never existed)", n)
	}
}

func TestOutboxReservationIsExclusiveWhileValidThenExpiresIntoANewToken(t *testing.T) {
	e := newStoreEnv(t)
	st := e.store()
	id := e.insertEvent("w", "WalletBalanceChanged")

	r1 := mustReserve(t, st, 500*time.Millisecond)
	mustNotReserve(t, st, "an event under a valid lease")

	e.dbPassed(*e.state(id).Expires)
	r2 := mustReserve(t, st, 20*time.Second) // a MESMA instância do repositório adquire de novo
	if r2.EventID != id || r2.Token == r1.Token || r2.Attempts != 2 {
		t.Fatalf("second acquisition: %+v (first token %s)", r2, r1.Token)
	}
	if s := e.state(id); *s.Token != r2.Token || s.Attempts != 2 || s.Published {
		t.Fatalf("state after re-acquisition: %+v", s)
	}
}

func TestAStaleExecutorCannotConfirmRetryOrReleaseTheNewOwnersEvent(t *testing.T) {
	e := newStoreEnv(t)
	st := e.store()
	ctx := context.Background()
	id := e.insertEvent("w", "WalletBalanceChanged")

	old := mustReserve(t, st, 500*time.Millisecond)
	e.dbPassed(*e.state(id).Expires)
	cur := mustReserve(t, st, 20*time.Second)
	before := e.state(id)

	for name, err := range map[string]error{
		"confirm": st.Confirm(ctx, *old),
		"retry":   st.Retry(ctx, *old, time.Hour, "stale executor"),
		"release": st.Release(ctx, *old),
	} {
		if !errors.Is(err, outbox.ErrLeaseLost) {
			t.Errorf("stale %s = %v, want ErrLeaseLost", name, err)
		}
	}
	after := e.state(id)
	if after.Published || *after.Token != cur.Token || after.LastError != "" || !after.NextAt.Equal(before.NextAt) || after.Attempts != before.Attempts {
		t.Fatalf("the stale executor changed the new owner's state: before %+v after %+v", before, after)
	}
	if err := st.Confirm(ctx, *cur); err != nil {
		t.Fatalf("the current owner must still confirm: %v", err)
	}
	if s := e.state(id); !s.Published || s.Token != nil || s.Expires != nil {
		t.Fatalf("after confirm: %+v", s)
	}
	// Publicado: nem o dono antigo nem o atual alteram mais nada.
	if err := st.Confirm(ctx, *cur); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("a second confirm = %v, want ErrLeaseLost", err)
	}
	mustNotReserve(t, st, "a published event")
}

func TestAnExpiredLeaseWithoutANewOwnerIsNotAuthorizedByTheTokenAlone(t *testing.T) {
	e := newStoreEnv(t)
	st := e.store()
	ctx := context.Background()
	id := e.insertEvent("w", "WalletBalanceChanged")
	r := mustReserve(t, st, 400*time.Millisecond)
	e.dbPassed(*e.state(id).Expires)

	for name, err := range map[string]error{
		"confirm": st.Confirm(ctx, *r), "retry": st.Retry(ctx, *r, time.Second, "late"), "release": st.Release(ctx, *r),
	} {
		if !errors.Is(err, outbox.ErrLeaseLost) {
			t.Errorf("%s with an expired lease = %v, want ErrLeaseLost", name, err)
		}
	}
	if s := e.state(id); s.Published || s.LastError != "" || s.Token == nil || *s.Token != r.Token {
		t.Fatalf("an expired lease must be left untouched (nobody owns it, nobody may change it): %+v", s)
	}
	if again := mustReserve(t, st, time.Second); again.EventID != id || again.Token == r.Token {
		t.Fatalf("the abandoned event must be recoverable: %+v", again)
	}
}

func TestAConfirmationThatWaitedForALockIsValidatedAfterTheWait(t *testing.T) {
	e := newStoreEnv(t)
	st := e.store()
	ctx := context.Background()
	id := e.insertEvent("w", "WalletBalanceChanged")
	r := mustReserve(t, st, time.Second)

	// Quem segura o lock da linha (sem alterá-la) faz a confirmação esperar por mais que a lease.
	holder, err := e.owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.Exec(ctx, `SELECT id FROM outbox_events WHERE id = $1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Rollback(context.Background()) }) // nunca deixa o lock preso se o teste falhar
	done := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		done <- st.Confirm(cctx, *r)
	}()
	eventually(t, "the confirmation to wait for the row lock", 10*time.Second, func() bool {
		return waitingForLock("%outbox_events%WHERE id = $1%") // o SELECT … FOR UPDATE da confirmação (ou, sem ele, o próprio UPDATE)
	})
	e.dbPassed(*e.state(id).Expires) // a posse vence ENQUANTO a confirmação espera
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("a confirmation whose lease expired during the wait = %v, want ErrLeaseLost", err)
	}
	if s := e.state(id); s.Published {
		t.Fatal("the instant captured before the wait must not authorize the write")
	}
}

func TestRetryPersistsTheBackoffAndOlderEventsDoNotBypassIt(t *testing.T) {
	e := newStoreEnv(t)
	st := e.store()
	ctx := context.Background()
	a := e.insertEvent("w", "WalletBalanceChanged")
	b := e.insertEvent("w", "WalletBalanceChanged")
	c := e.insertEvent("w", "WalletBalanceChanged")

	ra := mustReserve(t, st, 20*time.Second)
	if ra.EventID != a {
		t.Fatalf("the oldest eligible event comes first: got %s", ra.EventID)
	}
	if err := st.Retry(ctx, *ra, 1500*time.Millisecond, "broker unavailable"); err != nil {
		t.Fatal(err)
	}
	if s := e.state(a); s.Published || s.Attempts != 1 || s.LastError != "broker unavailable" || s.Token != nil || !s.NextAt.After(time.Now().Add(500*time.Millisecond)) {
		t.Fatalf("after retry: %+v", s)
	}
	// O evento em backoff não bloqueia os seguintes e não é reservado antes da hora.
	if got := mustReserve(t, st, 20*time.Second).EventID; got != b {
		t.Fatalf("expected the next event %s, got %s", b, got)
	}
	if got := mustReserve(t, st, 20*time.Second).EventID; got != c {
		t.Fatalf("expected the next event %s, got %s", c, got)
	}
	mustNotReserve(t, st, "the only remaining event is still in backoff")
	e.dbPassed(e.state(a).NextAt)
	if again := mustReserve(t, st, 20*time.Second); again.EventID != a || again.Attempts != 2 {
		t.Fatalf("after the backoff: %+v", again)
	}
}

func TestABusyOrRetryingPrefixDoesNotStarveLaterEvents(t *testing.T) {
	e := newStoreEnv(t)
	st := e.store()
	ctx := context.Background()
	ids := e.insertEvents(40, 5)

	for i := 0; i < 30; i++ { // 30 em backoff longo
		r := mustReserve(t, st, 20*time.Second)
		if err := st.Retry(ctx, *r, time.Hour, "persistent failure"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ { // 4 sob lease válida
		mustReserve(t, st, 20*time.Second)
	}
	// 3 travados por outras transações (o papel dono segura o lock da linha).
	var held []pgx.Tx
	for _, id := range ids[34:37] {
		tx, err := e.owner.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT 1 FROM outbox_events WHERE id = $1 FOR UPDATE`, id); err != nil {
			t.Fatal(err)
		}
		held = append(held, tx)
	}
	defer func() {
		for _, tx := range held {
			_ = tx.Rollback(ctx)
		}
	}()

	// Sobram exatamente os 3 últimos; ocupados e agendados à frente não os escondem, e a reserva não espera pelos travados.
	want := map[uuid.UUID]bool{ids[37]: true, ids[38]: true, ids[39]: true}
	start := time.Now()
	for i := 0; i < 3; i++ { // contador explícito: remover chaves de um mapa durante o range pode pular entradas
		r := mustReserve(t, st, 20*time.Second)
		if !want[r.EventID] {
			t.Fatalf("reserved %s, which is in backoff, leased or locked", r.EventID)
		}
		delete(want, r.EventID)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("reservation waited for locked rows (%v)", elapsed)
	}
	lastCtx, lastCancel := context.WithTimeout(ctx, 10*time.Second)
	defer lastCancel()
	if r, err := st.Reserve(lastCtx, 20*time.Second); err != nil {
		t.Fatal(err)
	} else if r != nil {
		idx := -1
		for i, id := range ids {
			if id == r.EventID {
				idx = i
			}
		}
		t.Fatalf("everything else is busy, leased or in backoff: event #%d (%s) must not be eligible (state %+v)", idx, r.EventID, e.state(r.EventID))
	}
}

func TestConcurrentReservationsNeverShareAnEvent(t *testing.T) {
	e := newStoreEnv(t)
	st := e.store()
	ids := e.insertEvents(80, 8)

	var mu sync.Mutex
	seen := map[uuid.UUID]int{}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				r, err := st.Reserve(context.Background(), 20*time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				if r == nil {
					return
				}
				mu.Lock()
				seen[r.EventID]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	for _, id := range ids {
		if seen[id] != 1 {
			t.Errorf("event %s reserved %d times", id, seen[id])
		}
	}
	if len(seen) != len(ids) {
		t.Fatalf("%d distinct events reserved, want %d", len(seen), len(ids))
	}
}

func TestOutboxRuntimePrivilegesAndImmutability(t *testing.T) {
	e := newStoreEnv(t)
	ctx := context.Background()
	id := e.insertEvent("w", "WalletBalanceChanged")
	rt := e.db.runtime

	denied := func(what string, err error) {
		t.Helper()
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || pg.Code != "42501" {
			t.Errorf("%s: %v, want permission denied (42501)", what, err)
		}
	}
	_, err := rt.Exec(ctx, `UPDATE outbox_events SET payload = '{}'::jsonb WHERE id = $1`, id)
	denied("runtime UPDATE payload", err)
	_, err = rt.Exec(ctx, `UPDATE outbox_events SET event_type = 'X' WHERE id = $1`, id)
	denied("runtime UPDATE event_type", err)
	_, err = rt.Exec(ctx, `UPDATE outbox_events SET id = $2 WHERE id = $1`, id, uuid.New())
	denied("runtime UPDATE id", err)
	_, err = rt.Exec(ctx, `UPDATE outbox_events SET occurred_at = now() WHERE id = $1`, id)
	denied("runtime UPDATE occurred_at", err)
	_, err = rt.Exec(ctx, `UPDATE outbox_events SET created_at = now() WHERE id = $1`, id)
	denied("runtime UPDATE created_at", err)
	_, err = rt.Exec(ctx, `DELETE FROM outbox_events WHERE id = $1`, id)
	denied("runtime DELETE", err)
	_, err = rt.Exec(ctx, `TRUNCATE outbox_events`)
	denied("runtime TRUNCATE", err)
	if _, err := rt.Exec(ctx, `UPDATE outbox_events SET attempts = attempts + 1, next_attempt_at = now(), last_error = 'x',
		lease_token = $2, lease_expires_at = now() + interval '1 minute' WHERE id = $1`, id, uuid.New()); err != nil {
		t.Fatalf("the control columns must be writable by the runtime: %v", err)
	}

	// O banco impõe as garantias mesmo para o dono: snapshot, marcador de publicação e forma da reserva.
	integrity := func(what string, err error) {
		t.Helper()
		var pg *pgconn.PgError
		if !errors.As(err, &pg) || (pg.Code != "23000" && pg.Code != "23514") {
			t.Errorf("%s: %v, want an integrity violation", what, err)
		}
	}
	_, err = e.owner.Exec(ctx, `UPDATE outbox_events SET payload = '{"x":1}'::jsonb WHERE id = $1`, id)
	integrity("owner UPDATE payload", err)
	_, err = e.owner.Exec(ctx, `UPDATE outbox_events SET lease_token = NULL WHERE id = $1`, id)
	integrity("a token without expiry (or the reverse)", err)
	if _, err := e.owner.Exec(ctx, `UPDATE outbox_events SET published_at = now(), lease_token = NULL, lease_expires_at = NULL WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	_, err = e.owner.Exec(ctx, `UPDATE outbox_events SET published_at = NULL WHERE id = $1`, id)
	integrity("clearing published_at", err)
	_, err = e.owner.Exec(ctx, `UPDATE outbox_events SET published_at = now() + interval '1 hour' WHERE id = $1`, id)
	integrity("changing published_at", err)
	_, err = e.owner.Exec(ctx, `UPDATE outbox_events SET lease_token = $2, lease_expires_at = now() WHERE id = $1`, id, uuid.New())
	integrity("a lease on a published event", err)
}

func TestUpgradeAndRollbackPreserveTheOutbox(t *testing.T) {
	ctx := context.Background()
	url, mig, conn := scratchAt(t, "outboxmig", 6)

	// Schema 6: um evento publicado e três não publicados com agenda, tentativas e diagnóstico próprios.
	type row struct {
		id             uuid.UUID
		publishedAt    *time.Time
		attempts       int
		nextAttemptAt  time.Time
		lastError      *string
		payload, etype string
	}
	insert := func(attempts int, next time.Time, lastErr *string, published bool) uuid.UUID {
		id := uuid.New()
		var pub *time.Time
		if published {
			now := time.Now()
			pub = &now
		}
		if _, err := conn.Exec(ctx, `INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version, correlation_id, payload, occurred_at,
			attempts, next_attempt_at, last_error, published_at) VALUES ($1,'Wallet',$2,'WalletBalanceChanged',1,'c',$3::jsonb,now(),$4,$5,$6,$7)`,
			id, uuid.New(), envelope(id, "WalletBalanceChanged", "w", 3), attempts, next, lastErr, pub); err != nil {
			t.Fatal(err)
		}
		return id
	}
	oops := "sns: broker unavailable"
	ids := []uuid.UUID{
		insert(1, time.Now().Add(-time.Minute), nil, true),
		insert(0, time.Now().Add(-time.Minute), nil, false),
		insert(4, time.Now().Add(time.Hour), &oops, false),
		insert(2, time.Now().Add(-time.Hour), &oops, false),
	}
	snapshot := func(c interface {
		Query(context.Context, string, ...any) (pgx.Rows, error)
	}) map[uuid.UUID]row {
		t.Helper()
		rows, err := c.Query(ctx, `SELECT id, published_at, attempts, next_attempt_at, last_error, payload::text, event_type FROM outbox_events`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[uuid.UUID]row{}
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.id, &r.publishedAt, &r.attempts, &r.nextAttemptAt, &r.lastError, &r.payload, &r.etype); err != nil {
				t.Fatal(err)
			}
			out[r.id] = r
		}
		return out
	}
	same := func(label string, a, b map[uuid.UUID]row) {
		t.Helper()
		if len(a) != len(b) {
			t.Fatalf("%s: %d events, want %d", label, len(b), len(a))
		}
		for id, x := range a {
			y, ok := b[id]
			if !ok || (x.publishedAt == nil) != (y.publishedAt == nil) || (x.publishedAt != nil && !x.publishedAt.Equal(*y.publishedAt)) ||
				x.attempts != y.attempts || !x.nextAttemptAt.Equal(y.nextAttemptAt) || x.payload != y.payload || x.etype != y.etype ||
				(x.lastError == nil) != (y.lastError == nil) || (x.lastError != nil && *x.lastError != *y.lastError) {
				t.Fatalf("%s: event %s changed: %+v -> %+v", label, id, x, y)
			}
		}
	}
	v6 := snapshot(conn)

	// No schema 6 o papel de runtime é provisionado sem o UPDATE de controle (as colunas ainda não existem).
	if err := postgres.ProvisionRuntimeRole(ctx, url, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		t.Fatal(err)
	}
	var canUpdate bool
	if err := conn.QueryRow(ctx, `SELECT has_column_privilege($1, 'outbox_events', 'attempts', 'UPDATE')`, env.runtimeRole).Scan(&canUpdate); err != nil {
		t.Fatal(err)
	}
	if canUpdate {
		t.Fatal("before the publisher migration the runtime must not update the outbox")
	}

	if _, err := mig.Up(ctx); err != nil {
		t.Fatalf("up to the final version: %v", err)
	}
	if v := schemaVersion(t, conn); v != finalVersion(t) {
		t.Fatalf("schema %d, want the final version %d", v, finalVersion(t))
	}
	same("after up", v6, snapshot(conn))
	var leased int64
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE lease_token IS NOT NULL OR lease_expires_at IS NOT NULL`).Scan(&leased); err != nil || leased != 0 {
		t.Fatalf("no event may arrive leased (err=%v, %d)", err, leased)
	}
	// Reprovisionado, o runtime atualiza só as colunas de controle e consegue reservar o backlog pré-existente.
	if err := postgres.ProvisionRuntimeRole(ctx, url, postgres.RuntimeRole{Name: env.runtimeRole, Password: env.runtimePass}); err != nil {
		t.Fatal(err)
	}
	rtURL, _ := withCredentials(url, env.runtimeRole, env.runtimePass)
	rt, err := pgxpool.New(ctx, rtURL)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	st := postgres.NewOutboxStore(postgres.StaticPool(rt))
	got := map[uuid.UUID]bool{}
	for {
		r, err := st.Reserve(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if r == nil {
			break
		}
		got[r.EventID] = true
	}
	// Elegíveis: os não publicados e já vencidos (o terceiro, com agenda de uma hora, fica de fora).
	if len(got) != 2 || !got[ids[1]] || !got[ids[3]] {
		t.Fatalf("the pre-existing backlog was not reserved as expected: %v", got)
	}

	// Reversão: a reserva (transitória) é descartada; eventos, agenda, tentativas, diagnóstico e marcadores permanecem.
	if _, err := mig.DownTo(ctx, 6); err != nil { // desfaz a 00008 (inbox) e a 00007 (reserva); as colunas duráveis da outbox permanecem
		t.Fatalf("down to 6: %v", err)
	}
	if v := schemaVersion(t, conn); v != 6 {
		t.Fatalf("schema %d after down, want 6", v)
	}
	after := snapshot(conn)
	for id := range got { // a reserva contou uma tentativa em cada um
		x := v6[id]
		x.attempts++
		v6[id] = x
	}
	same("after down", v6, after)
	if _, err := mig.Up(ctx); err != nil {
		t.Fatalf("up again: %v", err)
	}
	same("after re-applying", v6, snapshot(conn))
	_ = ids
}
