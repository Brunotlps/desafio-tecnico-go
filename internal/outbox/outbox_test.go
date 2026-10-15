package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func goodConfig() Config {
	return Config{Workers: 1, Lease: 20 * time.Second, PublishTimeout: 5 * time.Second, SQLTimeout: 2 * time.Second,
		PollInterval: 5 * time.Millisecond, BackoffInitial: time.Second, BackoffMax: 30 * time.Second}
}

type call struct {
	op     string
	delay  time.Duration
	reason string
}

// fakeStore entrega as reservas da fila, uma por Reserve, e registra as escritas.
type fakeStore struct {
	mu        sync.Mutex
	queue     []Reservation
	calls     []call
	reserveEr error
	finishErr error
}

func (f *fakeStore) Reserve(context.Context, time.Duration) (*Reservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.reserveEr != nil {
		return nil, f.reserveEr
	}
	if len(f.queue) == 0 {
		return nil, nil
	}
	r := f.queue[0]
	f.queue = f.queue[1:]
	return &r, nil
}

// record honra o contexto como o repositório real: uma ação final com contexto cancelado não se completa.
func (f *fakeStore) record(ctx context.Context, c call) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
	return f.finishErr
}
func (f *fakeStore) Confirm(ctx context.Context, _ Reservation) error {
	return f.record(ctx, call{op: "confirm"})
}
func (f *fakeStore) Retry(ctx context.Context, _ Reservation, d time.Duration, reason string) error {
	return f.record(ctx, call{op: "retry", delay: d, reason: reason})
}
func (f *fakeStore) Release(ctx context.Context, _ Reservation) error {
	return f.record(ctx, call{op: "release"})
}
func (f *fakeStore) got() []call {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]call(nil), f.calls...)
}

type fakeSink struct {
	mu   sync.Mutex
	msgs []Message
	fn   func(ctx context.Context, m Message) error
}

func (s *fakeSink) Publish(ctx context.Context, m Message) error {
	s.mu.Lock()
	s.msgs = append(s.msgs, m)
	fn := s.fn
	s.mu.Unlock()
	if fn != nil {
		return fn(ctx, m)
	}
	return nil
}

func reservation(wallet string, attempts int) Reservation {
	return Reservation{EventID: uuid.New(), Seq: 1, Type: "WalletBalanceChanged", WalletID: wallet,
		Body: `{"eventId":"x","data":{"walletId":"` + wallet + `","walletVersion":3}}`, Attempts: attempts, Token: uuid.New()}
}

func TestSuccessfulPublicationIsConfirmedWithTheStoredEnvelopeAndRouting(t *testing.T) {
	r := reservation("w-1", 1)
	st, sk := &fakeStore{queue: []Reservation{r}}, &fakeSink{}
	p := New(st, sk, goodConfig(), quiet())
	if !p.Step() {
		t.Fatal("expected work")
	}
	if len(sk.msgs) != 1 {
		t.Fatalf("published %d messages", len(sk.msgs))
	}
	m := sk.msgs[0]
	if m.GroupID != "w-1" || m.DedupID != r.EventID.String() || m.Body != r.Body || m.Type != r.Type {
		t.Fatalf("unexpected message %+v", m)
	}
	if c := st.got(); len(c) != 1 || c[0].op != "confirm" {
		t.Fatalf("calls = %+v", c)
	}
}

func TestNoEligibleEventDoesNothing(t *testing.T) {
	st, sk := &fakeStore{}, &fakeSink{}
	if New(st, sk, goodConfig(), quiet()).Step() {
		t.Fatal("no event: Step must report no work")
	}
	if len(sk.msgs) != 0 || len(st.got()) != 0 {
		t.Fatal("nothing may be published or written")
	}
}

func TestFailedPublicationSchedulesARetryAndNeverConfirms(t *testing.T) {
	st := &fakeStore{queue: []Reservation{reservation("w", 1)}}
	sk := &fakeSink{fn: func(context.Context, Message) error {
		return errors.New("broker unavailable\nsecret=hunter2 " + strings.Repeat("x", 600))
	}}
	p := New(st, sk, goodConfig(), quiet())
	p.Step()
	c := st.got()
	if len(c) != 1 || c[0].op != "retry" {
		t.Fatalf("calls = %+v", c)
	}
	if c[0].delay < 500*time.Millisecond || c[0].delay > time.Second {
		t.Fatalf("first retry must wait within [0.5s, 1s], got %v", c[0].delay)
	}
	if strings.ContainsAny(c[0].reason, "\n\t") || len(c[0].reason) > maxErrorLen+len("…") {
		t.Fatalf("reason must be short and free of control characters: %q", c[0].reason)
	}
}

func TestAnEventThatCannotBeRoutedIsRetriedAndNotDropped(t *testing.T) {
	r := reservation("", 1)
	st, sk := &fakeStore{queue: []Reservation{r}}, &fakeSink{}
	New(st, sk, goodConfig(), quiet()).Step()
	if len(sk.msgs) != 0 {
		t.Fatal("an event without walletId must not be published")
	}
	if c := st.got(); len(c) != 1 || c[0].op != "retry" || !strings.Contains(c[0].reason, "cannot be routed") {
		t.Fatalf("calls = %+v", c)
	}
}

func TestCancellationReleasesTheReservationInsteadOfRetryingOrConfirming(t *testing.T) {
	st := &fakeStore{queue: []Reservation{reservation("w", 1)}}
	var p *Publisher
	sk := &fakeSink{fn: func(ctx context.Context, _ Message) error {
		p.Cancel()
		<-ctx.Done()
		return ctx.Err()
	}}
	p = New(st, sk, goodConfig(), quiet())
	p.Step()
	if c := st.got(); len(c) != 1 || c[0].op != "release" {
		t.Fatalf("a cancelled publication must release, got %+v", c)
	}
}

func TestAnAcceptedPublicationIsStillConfirmedAfterTheShutdownCancel(t *testing.T) {
	st := &fakeStore{queue: []Reservation{reservation("w", 1)}}
	var p *Publisher
	sk := &fakeSink{fn: func(context.Context, Message) error { p.Cancel(); return nil }}
	p = New(st, sk, goodConfig(), quiet())
	p.Step()
	if c := st.got(); len(c) != 1 || c[0].op != "confirm" {
		t.Fatalf("accepted + cancelled must still confirm (the final action has its own context), got %+v", c)
	}
}

func TestLostLeaseOrFailedFinishDoesNotRepeatThePublication(t *testing.T) {
	for _, fe := range []error{ErrLeaseLost, errors.New("connection reset")} {
		st := &fakeStore{queue: []Reservation{reservation("w", 1)}, finishErr: fe}
		sk := &fakeSink{}
		p := New(st, sk, goodConfig(), quiet())
		p.Step()
		if len(sk.msgs) != 1 || len(st.got()) != 1 {
			t.Fatalf("finish error %v: one publication and one finish attempt expected, got %d/%d", fe, len(sk.msgs), len(st.got()))
		}
	}
}

func TestReserveFailureIsReportedAsNoWork(t *testing.T) {
	st := &fakeStore{reserveEr: errors.New("database down")}
	if New(st, &fakeSink{}, goodConfig(), quiet()).Step() {
		t.Fatal("a failed reservation is not work")
	}
}

func TestBackoffIsExponentialJitteredCappedAndOverflowSafe(t *testing.T) {
	p := New(&fakeStore{}, &fakeSink{}, goodConfig(), quiet())
	for attempts, wantMax := range map[int]time.Duration{0: time.Second, 1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second,
		6: 30 * time.Second, 7: 30 * time.Second, 1_000_000: 30 * time.Second, int(^uint(0) >> 1): 30 * time.Second} {
		for i := 0; i < 200; i++ {
			d := p.backoff(attempts)
			if d < wantMax/2 || d > wantMax {
				t.Fatalf("attempts=%d: %v outside [%v, %v]", attempts, d, wantMax/2, wantMax)
			}
		}
	}
	// A extremidade do jitter é alcançável nos dois lados.
	p.rand = func(n int64) int64 { return 0 }
	if d := p.backoff(3); d != 2*time.Second {
		t.Fatalf("lowest jitter = %v", d)
	}
	p.rand = func(n int64) int64 { return n - 1 }
	if d := p.backoff(3); d != 4*time.Second {
		t.Fatalf("highest jitter = %v", d)
	}
}

func TestConfigValidation(t *testing.T) {
	if err := goodConfig().Validate(); err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"zero lease":         func(c *Config) { c.Lease = 0 },
		"zero publish":       func(c *Config) { c.PublishTimeout = 0 },
		"negative sql":       func(c *Config) { c.SQLTimeout = -1 },
		"zero poll":          func(c *Config) { c.PollInterval = 0 },
		"backoff inverted":   func(c *Config) { c.BackoffInitial, c.BackoffMax = time.Minute, time.Second },
		"too many workers":   func(c *Config) { c.Workers = 9 },
		"negative workers":   func(c *Config) { c.Workers = -1 },
		"lease too tight":    func(c *Config) { c.Lease = c.PublishTimeout + 2*c.SQLTimeout },
		"lease barely tight": func(c *Config) { c.Lease = c.PublishTimeout + 2*c.SQLTimeout + MinLeaseMargin },
	} {
		c := goodConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
}

func TestLifecycleStopsAdmissionDrainsAndReportsDone(t *testing.T) {
	st := &fakeStore{}
	for i := 0; i < 3; i++ {
		st.queue = append(st.queue, reservation("w", 1))
	}
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	sk := &fakeSink{fn: func(ctx context.Context, m Message) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return nil
	}}
	p := New(st, sk, goodConfig(), quiet())
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-started
	p.StopAdmission()
	select {
	case <-p.Done():
		t.Fatal("Done must wait for the publication in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-p.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Done never closed")
	}
	// A admissão fechou: só o evento já reservado foi publicado e confirmado; os demais continuam na fila.
	if len(sk.msgs) != 1 || len(st.got()) != 1 || len(st.queue) != 2 {
		t.Fatalf("published=%d finished=%d left=%d", len(sk.msgs), len(st.got()), len(st.queue))
	}
}

func TestStartWithoutWorkersIsDoneImmediatelyAfterStart(t *testing.T) {
	cfg := goodConfig()
	cfg.Workers = 0
	p := New(&fakeStore{}, &fakeSink{}, cfg, quiet())
	_ = p.Start(context.Background())
	select {
	case <-p.Done():
	case <-time.After(time.Second):
		t.Fatal("Done never closed")
	}
}
