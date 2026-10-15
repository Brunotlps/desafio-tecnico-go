package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func render(t *testing.T, r *Registry) string {
	t.Helper()
	var b bytes.Buffer
	if err := r.Render(context.Background(), &b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestCountersAreExposedWithBoundedLabelsAndAreSafeForConcurrency(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Result("sqs", "BET", "PROCESSED", false)
			r.InboxOutcome("duplicate")
			r.Retry("consumer", "transient")
			r.Duration("sqs_message", 20*time.Millisecond)
		}()
	}
	wg.Wait()
	r.Result("http", "BET", "REJECTED", true)
	r.DivergenceDetected()
	out := render(t, r)
	for _, want := range []string{
		`wager_transactions_total{origin="sqs",kind="BET",status="PROCESSED",replay="false"} 50`,
		`wager_transactions_total{origin="http",kind="BET",status="REJECTED",replay="true"} 1`,
		`wager_inbox_total{outcome="duplicate"} 50`,
		`wager_retries_total{component="consumer",reason="transient"} 50`,
		`wager_reconciliation_divergences_total 1`,
		`wager_processing_seconds_count{component="sqs_message"} 50`,
		`wager_processing_seconds_sum{component="sqs_message"} 1.000000`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestFreeTextAndIDsNeverBecomeLabelsAndCardinalityIsCapped(t *testing.T) {
	r := New()
	r.Retry("consumer", "0192f291-27dd-7d3f-8071-5f8685deef37") // um UUID tem '-': vira other
	r.Retry("consumer", "connection reset by peer: wallet w-1")
	r.Retry("consumer", strings.Repeat("x", 100))
	for i := 0; i < 500; i++ {
		r.Retry("consumer", fmt.Sprintf("reason%d", i))
	}
	out := render(t, r)
	if strings.Contains(out, "0192f291") || strings.Contains(out, "connection reset") || strings.Contains(out, "xxxxxxxx") {
		t.Fatal("an id or free text leaked into the labels")
	}
	n := strings.Count(out, "wager_retries_total{")
	if n > maxSeriesPerMetric+1 {
		t.Fatalf("%d series for one metric, cap %d", n, maxSeriesPerMetric)
	}
}

func TestACollectorFailureOmitsTheSamplesAndNeverExposesAFictitiousZero(t *testing.T) {
	r := New()
	r.AddCollector(Collector{Source: "outbox", Collect: func(context.Context) ([]Sample, error) {
		return []Sample{{Name: "wager_outbox_pending", Help: "pending", Value: 3}, {Name: "wager_outbox_oldest_pending_age_seconds", Help: "age", Value: 12.5}}, nil
	}})
	r.AddCollector(Collector{Source: "dlq", Collect: func(context.Context) ([]Sample, error) {
		return []Sample{{Name: "wager_dlq_messages_approx", Help: "dlq", Value: 0}}, errors.New("broker down")
	}})
	out := render(t, r)
	for _, want := range []string{`wager_collector_up{source="outbox"} 1`, `wager_collector_up{source="dlq"} 0`, "wager_outbox_pending 3", "wager_outbox_oldest_pending_age_seconds 12.5"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(out, "wager_dlq_messages_approx") {
		t.Fatalf("a failed collection must omit the gauge, not publish a zero:\n%s", out)
	}
}

func TestACollectorIsBoundedByItsDeadline(t *testing.T) {
	r := New()
	r.AddCollector(Collector{Source: "slow", Collect: func(ctx context.Context) ([]Sample, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	start := time.Now()
	out := render(t, r)
	if time.Since(start) > 4*time.Second || !strings.Contains(out, `wager_collector_up{source="slow"} 0`) {
		t.Fatalf("a hung collector must time out and be reported down (%v)", time.Since(start))
	}
}
