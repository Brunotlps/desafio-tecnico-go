package lifecycle

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

type journal struct {
	mu     sync.Mutex
	events []string
}

func (j *journal) add(e string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, e)
}

func (j *journal) all() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.events...)
}

func (j *journal) index(e string) int {
	for i, x := range j.all() {
		if x == e {
			return i
		}
	}
	return -1
}

// fake é um componente cujo término é controlado pelo teste.
type fake struct {
	name        string
	j           *journal
	startErr    error
	doneOn      string // "admission" | "cancel" | "never"
	done        chan struct{}
	once, onceC sync.Once
}

func newFake(j *journal, name, doneOn string) *fake {
	return &fake{name: name, j: j, doneOn: doneOn, done: make(chan struct{})}
}

func (f *fake) Name() string { return f.name }
func (f *fake) Start(context.Context) error {
	f.j.add("start:" + f.name)
	return f.startErr
}
func (f *fake) StopAdmission() {
	f.once.Do(func() {
		f.j.add("admission:" + f.name)
		if f.doneOn == "admission" {
			close(f.done)
		}
	})
}
func (f *fake) Cancel() {
	f.onceC.Do(func() {
		f.j.add("cancel:" + f.name)
		if f.doneOn == "cancel" {
			close(f.done)
		}
	})
}
func (f *fake) Done() <-chan struct{} { return f.done }

func newCoordinator(drain time.Duration) *Coordinator {
	return New(slog.New(slog.NewTextHandler(io.Discard, nil)), drain)
}

func stopWith(c *Coordinator, budget time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	return c.Stop(ctx)
}

func TestAdmissionIsClosedForEveryComponentBeforeAnythingIsAwaited(t *testing.T) {
	j := &journal{}
	c := newCoordinator(500 * time.Millisecond)
	slow := newFake(j, "slow", "cancel")
	quick := newFake(j, "quick", "admission")
	c.Add(slow)
	c.Add(quick)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stopWith(c, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	// Mesmo com o primeiro componente ainda trabalhando, a admissão do segundo já foi fechada.
	if j.index("admission:slow") < 0 || j.index("admission:quick") < 0 || j.index("cancel:slow") < j.index("admission:quick") {
		t.Fatalf("sequence: %v", j.all())
	}
}

func TestWorkIsDrainedWithoutCancellationWhenItFinishesInTime(t *testing.T) {
	j := &journal{}
	c := newCoordinator(time.Second)
	f := newFake(j, "worker", "never")
	c.Add(f)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	go func() { // o trabalho admitido termina por conta própria durante a drenagem
		for j.index("admission:worker") < 0 {
			time.Sleep(time.Millisecond)
		}
		close(f.done)
	}()
	if err := stopWith(c, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if j.index("cancel:worker") >= 0 {
		t.Fatalf("cancelled although it drained: %v", j.all())
	}
}

func TestRemainingWorkIsCancelledAtTheDrainLimitAndTerminationIsConfirmed(t *testing.T) {
	j := &journal{}
	drain := 150 * time.Millisecond
	c := newCoordinator(drain)
	f := newFake(j, "stuck", "cancel")
	c.Add(f)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	begin := time.Now()
	if err := stopWith(c, 2*time.Second); err != nil {
		t.Fatalf("cancelled work that confirmed its termination must be a clean stop: %v", err)
	}
	if elapsed := time.Since(begin); elapsed < drain {
		t.Fatalf("cancelled before the drain limit: %v", elapsed)
	}
	if j.index("cancel:stuck") < 0 {
		t.Fatalf("not cancelled: %v", j.all())
	}
}

func TestAComponentThatNeverTerminatesMakesTheShutdownIncomplete(t *testing.T) {
	j := &journal{}
	c := newCoordinator(100 * time.Millisecond)
	c.Add(newFake(j, "hung", "never"))
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	begin := time.Now()
	err := stopWith(c, 800*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "shutdown incomplete") || !strings.Contains(err.Error(), "hung") {
		t.Fatalf("err = %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 900*time.Millisecond {
		t.Fatalf("the global deadline was exceeded: %v", elapsed)
	}
	if j.index("cancel:hung") < 0 {
		t.Fatal("not cancelled before being declared stuck")
	}
}

func TestAFailedStartStopsTheComponentsAlreadyStarted(t *testing.T) {
	j := &journal{}
	c := newCoordinator(200 * time.Millisecond)
	first := newFake(j, "first", "admission")
	second := newFake(j, "second", "admission")
	second.startErr = errors.New("address in use")
	c.Add(first)
	c.Add(second)
	err := c.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "second") || !strings.Contains(err.Error(), "address in use") {
		t.Fatalf("err = %v", err)
	}
	select {
	case <-first.Done():
	default:
		t.Fatal("the component started before the failure was left running")
	}
	if j.index("admission:first") < 0 {
		t.Fatalf("sequence: %v", j.all())
	}
}

func TestObserversReceiveTheGlobalDeadline(t *testing.T) {
	j := &journal{}
	c := newCoordinator(100 * time.Millisecond)
	c.Add(newFake(j, "w", "admission"))
	var got time.Time
	c.OnShutdown(func(d time.Time) { got = d })
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want, _ := ctx.Deadline()
	if err := c.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("observer got %v, want %v", got, want)
	}
}

func TestStopWithoutStartedComponentsIsANoop(t *testing.T) {
	if err := stopWith(newCoordinator(time.Second), time.Second); err != nil {
		t.Fatal(err)
	}
}
