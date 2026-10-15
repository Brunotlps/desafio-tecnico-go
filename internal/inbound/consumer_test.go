package inbound

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"desafio-tecnico-go/internal/app"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func goodConfig() Config {
	return Config{Workers: 1, ConsumerName: "c", SourceQueue: "q", ReceiveWait: time.Second, PhaseTimeout: 10 * time.Second, IOTimeout: 5 * time.Second,
		BackoffInitial: 5 * time.Second, BackoffMax: 60 * time.Second, PauseMax: 30 * time.Second, Visibility: time.Minute}
}

// log registra a ordem das ações (transporte e diagnóstico) para provar que o commit/diagnóstico precede o Delete e o envio.
type log struct {
	mu      sync.Mutex
	events  []string
	delays  []time.Duration
	dls     []string
	dlGroup []string
	dlDedup []string
	// deadlines dos contextos entregues às ações finais
	deadlines map[string]time.Time
}

func newLog() *log { return &log{deadlines: map[string]time.Time{}} }
func (l *log) add(e string) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}
func (l *log) seq() string { l.mu.Lock(); defer l.mu.Unlock(); return strings.Join(l.events, ",") }

type fakeTransport struct {
	l         *log
	queue     []*Delivery
	mu        sync.Mutex
	failFwd   error
	failDel   error
	failVis   error
	recvErr   error
	recvCalls int
}

func (f *fakeTransport) Receive(ctx context.Context, wait time.Duration) (*Delivery, error) {
	f.mu.Lock()
	f.recvCalls++
	if f.recvErr != nil {
		err := f.recvErr
		f.mu.Unlock()
		return nil, err
	}
	var d *Delivery
	if len(f.queue) > 0 {
		d, f.queue = f.queue[0], f.queue[1:]
	}
	f.mu.Unlock()
	if d == nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Millisecond):
		}
	}
	return d, nil
}
func (f *fakeTransport) record(op string, ctx context.Context) {
	dl, _ := ctx.Deadline()
	f.l.mu.Lock()
	f.l.deadlines[op] = dl
	f.l.mu.Unlock()
	f.l.add(op)
}
func (f *fakeTransport) Delete(ctx context.Context, d Delivery) error {
	f.record("delete", ctx)
	return f.failDel
}
func (f *fakeTransport) ChangeVisibility(ctx context.Context, d Delivery, delay time.Duration) error {
	f.record("visibility", ctx)
	f.l.mu.Lock()
	f.l.delays = append(f.l.delays, delay)
	f.l.mu.Unlock()
	return f.failVis
}
func (f *fakeTransport) ForwardToDLQ(ctx context.Context, body []byte, group, dedup string, attrs map[string]string) error {
	f.record("forward", ctx)
	f.l.mu.Lock()
	f.l.dls = append(f.l.dls, string(body))
	f.l.dlGroup = append(f.l.dlGroup, group)
	f.l.dlDedup = append(f.l.dlDedup, dedup)
	f.l.mu.Unlock()
	return f.failFwd
}

type fakeHandler struct {
	l       *log
	result  app.InboundResult
	hasDiag bool
	diagErr error
	recErr  error
	calls   int
}

func (h *fakeHandler) ProcessMessage(ctx context.Context, consumer string, body []byte) app.InboundResult {
	h.calls++
	h.l.add("process")
	return h.result
}
func (h *fakeHandler) RecordDiagnostic(ctx context.Context, d app.Diagnostic) error {
	h.l.add("diagnostic:" + d.Code)
	return h.recErr
}
func (h *fakeHandler) HasDiagnostic(ctx context.Context, consumer, queue, brokerID string, hash []byte) (bool, error) {
	h.l.add("lookup")
	return h.hasDiag, h.diagErr
}

func oneShot(t *testing.T, h *fakeHandler, tr *fakeTransport, cfg Config, body string) *Consumer {
	t.Helper()
	c := New(h, tr, cfg, quiet(), Hooks{})
	c.rand = func(n int64) int64 { return n - 1 } // jitter no teto: o atraso é o exponencial puro
	c.handle(Delivery{BrokerID: "b-1", ReceiptHandle: "r", Body: []byte(body), ReceiveCount: 1})
	return c
}

const wallet = "0192f291-27dd-7d3f-8071-5f8685deef37"

func TestDoneDeletesOnlyAfterTheCommittedResult(t *testing.T) {
	l := newLog()
	h := &fakeHandler{l: l, result: app.InboundResult{Disposition: app.InboundDone, Status: "PROCESSED"}}
	oneShot(t, h, &fakeTransport{l: l}, goodConfig(), "{}")
	if got := l.seq(); got != "lookup,process,delete" {
		t.Fatalf("order = %s, want the delete only after the processing result", got)
	}
}

func TestRetryNeverDeletesAndSchedulesTheBackoffByTheReceiveCount(t *testing.T) {
	for count, want := range map[int]time.Duration{1: 5 * time.Second, 2: 10 * time.Second, 3: 20 * time.Second, 5: 60 * time.Second, 1000000: 60 * time.Second} {
		l := newLog()
		h := &fakeHandler{l: l, result: app.InboundResult{Disposition: app.InboundRetry, Cause: app.ErrTransient}}
		c := New(h, &fakeTransport{l: l}, goodConfig(), quiet(), Hooks{})
		c.rand = func(n int64) int64 { return n - 1 }
		c.handle(Delivery{BrokerID: "b", ReceiptHandle: "r", Body: []byte("{}"), ReceiveCount: count})
		if got := l.seq(); got != "lookup,process,visibility" {
			t.Fatalf("count %d: order = %s (a retry never deletes)", count, got)
		}
		if l.delays[0] != want {
			t.Errorf("count %d: delay %v, want %v", count, l.delays[0], want)
		}
	}
	// O jitter fica em [d/2, d].
	c := New(&fakeHandler{l: newLog()}, &fakeTransport{l: newLog()}, goodConfig(), quiet(), Hooks{})
	for i := 0; i < 500; i++ {
		if d := c.backoff(3); d < 10*time.Second || d > 20*time.Second {
			t.Fatalf("backoff(3) = %v outside [10s, 20s]", d)
		}
	}
}

func TestAFailedVisibilityChangeNeverDeletes(t *testing.T) {
	l := newLog()
	h := &fakeHandler{l: l, result: app.InboundResult{Disposition: app.InboundRetry, Cause: errors.New("x")}}
	oneShot(t, h, &fakeTransport{l: l, failVis: errors.New("boom")}, goodConfig(), "{}")
	if got := l.seq(); got != "lookup,process,visibility" {
		t.Fatalf("order = %s", got)
	}
}

func TestPermanentRecordsTheDiagnosticBeforeForwardingAndForwardsBeforeDeleting(t *testing.T) {
	l := newLog()
	h := &fakeHandler{l: l, result: app.InboundResult{Disposition: app.InboundPermanent, Code: "WALLET_NOT_FOUND", MessageID: "m1", WalletID: wallet}}
	body := `{"original":"body"}`
	oneShot(t, h, &fakeTransport{l: l}, goodConfig(), body)
	if got := l.seq(); got != "lookup,process,diagnostic:WALLET_NOT_FOUND,forward,delete" {
		t.Fatalf("order = %s", got)
	}
	if l.dls[0] != body || l.dlGroup[0] != wallet || len(l.dlDedup[0]) != 64 {
		t.Fatalf("the DLQ copy is the exact body grouped by the wallet with a deterministic 64-hex dedup: %q %q %q", l.dls[0], l.dlGroup[0], l.dlDedup[0])
	}
}

func TestADiagnosticWriteFailureIsNotAConfirmedDiagnostic(t *testing.T) {
	l := newLog()
	h := &fakeHandler{l: l, recErr: errors.New("database down"), result: app.InboundResult{Disposition: app.InboundPermanent, Code: "INVALID_ENVELOPE"}}
	oneShot(t, h, &fakeTransport{l: l}, goodConfig(), "x")
	if got := l.seq(); strings.Contains(got, "forward") || strings.Contains(got, "delete") {
		t.Fatalf("without a confirmed diagnostic nothing is forwarded or deleted: %s", got)
	}
}

func TestAFailedForwardNeverDeletesAndAnExistingDiagnosticStillForwards(t *testing.T) {
	l := newLog()
	h := &fakeHandler{l: l, result: app.InboundResult{Disposition: app.InboundPermanent, Code: "INVALID_ENVELOPE"}}
	oneShot(t, h, &fakeTransport{l: l, failFwd: errors.New("dlq down")}, goodConfig(), "x")
	if got := l.seq(); strings.Contains(got, "delete") {
		t.Fatalf("a failed DLQ send must keep the origin: %s", got)
	}

	// Diagnóstico já confirmado (queda antes do envio): a reentrega retoma o envio SEM tratar de novo.
	l2 := newLog()
	h2 := &fakeHandler{l: l2, hasDiag: true}
	oneShot(t, h2, &fakeTransport{l: l2}, goodConfig(), `{"definitely":"bad"}`)
	if got := l2.seq(); got != "lookup,forward,delete" || h2.calls != 0 {
		t.Fatalf("order = %s (process calls %d): an existing diagnostic forwards and deletes without reprocessing", got, h2.calls)
	}
}

func TestADiagnosticLookupErrorIsARetryNeverAnAbsence(t *testing.T) {
	l := newLog()
	h := &fakeHandler{l: l, diagErr: errors.New("db timeout"), result: app.InboundResult{Disposition: app.InboundDone}}
	oneShot(t, h, &fakeTransport{l: l}, goodConfig(), "{}")
	if got := l.seq(); got != "lookup,visibility" || h.calls != 0 {
		t.Fatalf("order = %s (process calls %d)", got, h.calls)
	}
}

// As ações finais recebem o MENOR instante absoluto entre agora + IOTimeout e o prazo global: sem recomeçar duração, sem orçamento
// artificial.
func TestFinalActionsGetTheEarliestAbsoluteDeadlineAndNoneWithoutBudget(t *testing.T) {
	cfg := goodConfig()
	cfg.IOTimeout = 40 * time.Millisecond
	cfg.Visibility = time.Minute
	for name, tc := range map[string]struct {
		global        time.Duration // 0 = nenhum
		globalLimited bool
		ran           bool
	}{
		"no global deadline": {0, false, true},
		"plenty":             {time.Hour, false, true},
		"global is limit":    {15 * time.Millisecond, true, true},
		"budget gone":        {-time.Millisecond, false, false},
	} {
		l := newLog()
		h := &fakeHandler{l: l, result: app.InboundResult{Disposition: app.InboundDone}}
		c := New(h, &fakeTransport{l: l}, cfg, quiet(), Hooks{})
		var global time.Time
		if tc.global != 0 {
			global = time.Now().Add(tc.global)
			c.SetShutdownDeadline(global)
		}
		start := time.Now()
		c.handle(Delivery{BrokerID: "b", ReceiptHandle: "r", Body: []byte("{}"), ReceiveCount: 1})
		ran := strings.Contains(l.seq(), "delete")
		if ran != tc.ran {
			t.Errorf("%s: delete ran=%v, want %v", name, ran, tc.ran)
			continue
		}
		if !ran {
			continue
		}
		got := l.deadlines["delete"]
		if got.After(start.Add(cfg.IOTimeout + 15*time.Millisecond)) {
			t.Errorf("%s: deadline %v after the start, more than the I/O timeout", name, got.Sub(start))
		}
		if !global.IsZero() && got.After(global) {
			t.Errorf("%s: the delivered deadline is AFTER the global instant", name)
		}
		if tc.globalLimited && !got.Equal(global) {
			t.Errorf("%s: the global instant limits, so the delivered deadline must be exactly it", name)
		}
	}
}

func TestSearchPausesAfterConsecutiveFailuresAndResumesAfterASuccess(t *testing.T) {
	l := newLog()
	tr := &fakeTransport{l: l, recvErr: errors.New("broker down")}
	c := New(&fakeHandler{l: l}, tr, goodConfig(), quiet(), Hooks{})
	var slept []time.Duration
	var mu sync.Mutex
	c.sleep = func(ctx context.Context, d time.Duration) {
		mu.Lock()
		slept = append(slept, d)
		n := len(slept)
		mu.Unlock()
		if n >= 6 {
			c.StopAdmission()
		}
	}
	c.wg.Add(1)
	c.run()
	mu.Lock()
	defer mu.Unlock()
	long := 0
	for _, d := range slept {
		if d >= 900*time.Millisecond {
			long++
		}
	}
	if long == 0 {
		t.Fatalf("after 3 consecutive failures the searches must pause (no churn): sleeps %v", slept)
	}
	c.noteSuccess()
	if c.pauseUntil.Load() != nil || c.failures.Load() != 0 {
		t.Fatal("a success clears the pause")
	}
}

func TestConfigValidation(t *testing.T) {
	if err := goodConfig().Validate(); err != nil {
		t.Fatalf("defaults must be valid: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"no name":                             func(c *Config) { c.ConsumerName = "" },
		"no queue":                            func(c *Config) { c.SourceQueue = "" },
		"zero io":                             func(c *Config) { c.IOTimeout = 0 },
		"too many workers":                    func(c *Config) { c.Workers = 17 },
		"backoff inverted":                    func(c *Config) { c.BackoffInitial, c.BackoffMax = time.Minute, time.Second },
		"delivery longer than the visibility": func(c *Config) { c.PhaseTimeout = 45 * time.Second },
	} {
		c := goodConfig()
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s must be rejected", name)
		}
	}
}

func TestDLQDedupIDIsDeterministicByQueueBrokerIDAndHash(t *testing.T) {
	a, b := DLQDedupID("q", "b1", []byte("h")), DLQDedupID("q", "b1", []byte("h"))
	if a != b || len(a) != 64 {
		t.Fatal("deterministic, 64 hex")
	}
	for _, other := range []string{DLQDedupID("q2", "b1", []byte("h")), DLQDedupID("q", "b2", []byte("h")), DLQDedupID("q", "b1", []byte("x"))} {
		if other == a {
			t.Fatal("queue, broker id and hash each change the dedup id")
		}
	}
}
