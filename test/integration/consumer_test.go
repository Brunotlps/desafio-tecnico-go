//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/inbound"
	"desafio-tecnico-go/internal/infra/broker"
	infrasqs "desafio-tecnico-go/internal/infra/sqs"
	"desafio-tecnico-go/internal/lifecycle"
)

// Consumidor SQS com PostgreSQL e MiniStack reais: fila de entrada e DLQ próprias (visibilidade curta), principais reais
// (runtime consome, producer envia). A fila FIFO deduplica o envio por cinco minutos: para provar REENTREGA, as fixtures enviam o
// mesmo envelope com outro MessageDeduplicationId (bypass controlado, só da fixture) ou dependem de reentregas reais por visibilidade.

type consEnv struct {
	*inboxEnv
	runtime *infrasqs.Client
	prod    *infrasqs.Producer
	admin   *awssqs.Client
	cfg     inbound.Config
}

func newConsEnv(t *testing.T, mods ...func(*broker.Config)) *consEnv {
	t.Helper()
	e := newStoreEnv(t)
	e.proof = provisionProof(t, append([]func(*broker.Config){func(c *broker.Config) { c.InputVisibility = 2 }}, mods...)...)
	ctx := context.Background()
	rt, err := infrasqs.NewClient(ctx, infrasqs.Config{Region: e.proof.cfg.Region, EndpointURL: e.proof.cfg.Endpoint, QueueName: e.proof.res.Names.Input,
		DLQName: e.proof.res.Names.DLQ, CredentialsFile: e.proof.cfg.CredentialsFile, Profile: e.proof.res.Profiles[broker.Runtime]})
	if err != nil {
		t.Fatal(err)
	}
	prod, err := infrasqs.NewProducer(ctx, infrasqs.ProducerConfig{Region: e.proof.cfg.Region, EndpointURL: e.proof.cfg.Endpoint,
		QueueName: e.proof.res.Names.Input, CredentialsFile: e.proof.cfg.CredentialsFile, Profile: e.proof.res.Profiles[broker.Producer]})
	if err != nil {
		t.Fatal(err)
	}
	c := &consEnv{inboxEnv: &inboxEnv{pubEnv: e, svc: newInboxService(e, nil)}, runtime: rt, prod: prod,
		admin: awssqs.NewFromConfig(e.proof.admin())}
	c.cfg = inbound.Config{Workers: 4, ConsumerName: consumerName, SourceQueue: e.proof.res.Names.Input, ReceiveWait: time.Second,
		PhaseTimeout: 5 * time.Second, IOTimeout: 2 * time.Second, BackoffInitial: 200 * time.Millisecond, BackoffMax: time.Second,
		PauseMax: 2 * time.Second, Visibility: time.Minute}
	return c
}

// tspy registra e injeta falhas no transporte (decorator do cliente real).
type tspy struct {
	inner       inbound.Transport
	mu          sync.Mutex
	receives    []inbound.Delivery
	deletes     int
	deleted     []string
	delays      []time.Duration
	forwards    []string
	failDelete  func(n int) bool
	failForward func(n int) bool
	failVis     func(n int) bool
	nDelete     int
	nForward    int
	nVis        int
}

func (s *tspy) Receive(ctx context.Context, wait time.Duration) (*inbound.Delivery, error) {
	d, err := s.inner.Receive(ctx, wait)
	if d != nil {
		s.mu.Lock()
		s.receives = append(s.receives, *d)
		s.mu.Unlock()
	}
	return d, err
}
func (s *tspy) Delete(ctx context.Context, d inbound.Delivery) error {
	s.mu.Lock()
	s.nDelete++
	n := s.nDelete
	s.mu.Unlock()
	if s.failDelete != nil && s.failDelete(n) {
		return errors.New("simulated: the process died before the delete")
	}
	err := s.inner.Delete(ctx, d)
	if err == nil {
		s.mu.Lock()
		s.deletes++
		s.deleted = append(s.deleted, d.BrokerID)
		s.mu.Unlock()
	}
	return err
}
func (s *tspy) ChangeVisibility(ctx context.Context, d inbound.Delivery, delay time.Duration) error {
	s.mu.Lock()
	s.nVis++
	n := s.nVis
	s.delays = append(s.delays, delay)
	s.mu.Unlock()
	if s.failVis != nil && s.failVis(n) {
		return errors.New("simulated: the visibility change failed")
	}
	return s.inner.ChangeVisibility(ctx, d, delay)
}
func (s *tspy) ForwardToDLQ(ctx context.Context, body []byte, group, dedup string, attrs map[string]string) error {
	s.mu.Lock()
	s.nForward++
	n := s.nForward
	s.mu.Unlock()
	if s.failForward != nil && s.failForward(n) {
		return errors.New("simulated: the process died before the DLQ send")
	}
	err := s.inner.ForwardToDLQ(ctx, body, group, dedup, attrs)
	if err == nil {
		s.mu.Lock()
		s.forwards = append(s.forwards, group)
		s.mu.Unlock()
	}
	return err
}
func (s *tspy) receiveCount() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.receives) }
func (s *tspy) deleteCount() int  { s.mu.Lock(); defer s.mu.Unlock(); return s.deletes }

func (c *consEnv) newConsumer(t *testing.T, h inbound.Handler, tr inbound.Transport, hooks inbound.Hooks) *inbound.Consumer {
	if h == nil {
		h = c.svc
	}
	if tr == nil {
		tr = c.runtime
	}
	return inbound.New(h, tr, c.cfg, quietLog(), hooks)
}

func (c *consEnv) run(t *testing.T, cons *inbound.Consumer) {
	t.Helper()
	if err := cons.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cons.StopAdmission(); cons.Cancel(); <-cons.Done() })
}

// adminSend envia à entrada com o administrador da prova e uma deduplicação própria: o bypass controlado da deduplicação FIFO.
func (c *consEnv) adminSend(t *testing.T, body, group, dedup string) {
	t.Helper()
	if _, err := c.admin.SendMessage(context.Background(), &awssqs.SendMessageInput{QueueUrl: aws.String(c.proof.res.InputURL), MessageBody: aws.String(body),
		MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(dedup)}); err != nil {
		t.Fatal(err)
	}
}

func (c *consEnv) send(t *testing.T, body, wallet, msgID string) {
	t.Helper()
	if err := c.prod.Send(context.Background(), []byte(body), wallet, msgID); err != nil {
		t.Fatal(err)
	}
}

// drain lê e apaga mensagens de uma fila (cliente administrador da prova) até haver `want` ou o prazo acabar.
func (c *consEnv) drain(t *testing.T, queueURL string, want int, within time.Duration) []auditMessage {
	t.Helper()
	var out []auditMessage
	deadline := time.Now().Add(within)
	for len(out) < want && time.Now().Before(deadline) {
		for _, m := range receive(t, c.admin, queueURL, 10, 1, 30) {
			out = append(out, auditMessage{Body: aws.ToString(m.Body), Group: m.Attributes["MessageGroupId"], Dedup: m.Attributes["MessageDeduplicationId"]})
			if _, err := c.admin.DeleteMessage(context.Background(), &awssqs.DeleteMessageInput{QueueUrl: aws.String(queueURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return out
}

// inputEmpty espera a fila de entrada ficar vazia (nada visível nem em voo), observada pelo inspetor.
func (c *consEnv) inputEmpty(t *testing.T, within time.Duration) {
	t.Helper()
	inspector := c.proof.sqs(broker.Inspector)
	eventually(t, "the input queue to be empty", within, func() bool {
		v, f := depth(t, inspector, c.proof.res.InputURL)
		return v == 0 && f == 0
	})
}

func (c *consEnv) inboxRows(walletID string) int64 { return c.view(walletID).inbox }

func TestSQSFlowsOfEveryKindShareTheFinancialRules(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	c.run(t, c.newConsumer(t, nil, nil, inbound.Hooks{}))

	steps := []struct {
		id, kind, amount, ref string
		status                string
		balance               int64
	}{
		{"bet", "BET", "25.00", "", "PROCESSED", 7500},
		{"win", "WIN", "10.00", "", "PROCESSED", 8500},
		{"loss", "LOSS", "0.00", "", "PROCESSED", 8500},
		{"refund", "REFUND", "25.00", "bet", "PROCESSED", 11000},
		{"over", "BET", "9999.00", "", "REJECTED", 11000},
	}
	var wantInbox int64
	for _, s := range steps {
		b := body("m-"+s.id, "provider-a", "k-"+s.id, s.id, w, p, s.kind, s.amount, s.ref)
		c.send(t, b, w, "m-"+s.id)
		wantInbox++
		eventually(t, s.id+" to be handled", 30*time.Second, func() bool { return c.inboxRows(w) == wantInbox })
		if v := c.view(w); v.balance != s.balance {
			t.Fatalf("%s: balance %d, want %d", s.id, v.balance, s.balance)
		}
		var disp string
		_ = c.owner.QueryRow(context.Background(), `SELECT disposition FROM inbox_messages WHERE message_id = $1`, "m-"+s.id).Scan(&disp)
		if disp != s.status {
			t.Fatalf("%s: disposition %s, want %s", s.id, disp, s.status)
		}
	}
	// ROLLBACK de um alvo já revertido: rejeição auditável (ALREADY_REVERSED), removida depois do commit, sem ledger novo.
	led := c.view(w).ledger
	c.send(t, body("m-rb", "provider-a", "k-rb", "rb", w, p, "ROLLBACK", "25.00", "bet"), w, "m-rb")
	wantInbox++
	eventually(t, "the rollback to be handled", 30*time.Second, func() bool { return c.inboxRows(w) == wantInbox })
	if v := c.view(w); v.ledger != led {
		t.Fatalf("a rejected reversal must not write the ledger: %d -> %d", led, v.ledger)
	}
	c.inputEmpty(t, 20*time.Second) // tudo removido depois do commit
	// Uma pendência é removida só com a agenda durável; o worker de referências a resolve quando a BET chega.
	pend := body("m-pend", "provider-a", "k-pend", "pend", w, p, "REFUND", "5.00", "late-bet")
	c.send(t, pend, w, "m-pend")
	eventually(t, "the pending message to be handled", 30*time.Second, func() bool { return c.inboxRows(w) == wantInbox+1 })
	if n := c.count(`SELECT count(*) FROM pending_references WHERE completed_at IS NULL`); n != 1 {
		t.Fatalf("the pending schedule must be durable before the message is removed: %d", n)
	}
	c.inputEmpty(t, 20*time.Second)
	c.assertLedger(w)
}

func TestRealRedeliveriesAreReceivedByTheApplicationAndAppliedOnce(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	spy := &tspy{inner: c.runtime}
	c.run(t, c.newConsumer(t, nil, spy, inbound.Hooks{}))
	b := body("m-50", "provider-a", "k-50", "t-50", w, p, "BET", "25.00", "")
	for i := 0; i < 50; i++ { // o MESMO envelope, 50 vezes, com deduplicação FIFO própria de cada envio
		c.adminSend(t, b, w, fmt.Sprintf("bypass-%d", i))
	}
	eventually(t, "50 deliveries to be received by the application", 90*time.Second, func() bool { return spy.receiveCount() >= 50 })
	c.inputEmpty(t, 60*time.Second)
	if spy.deleteCount() != 50 {
		t.Fatalf("every committed delivery is deleted: %d deletes for %d receives", spy.deleteCount(), spy.receiveCount())
	}
	if v := c.view(w); v.balance != 7500 || v.version != 2 || v.ledger != 2 || v.outbox != 4 || v.inbox != 1 {
		t.Fatalf("50 deliveries, one effect: %+v", v)
	}
}

func TestAConflictingBodyGoesThroughDiagnosticAndTheDLQBeforeTheDelete(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	spy := &tspy{inner: c.runtime}
	c.run(t, c.newConsumer(t, nil, spy, inbound.Hooks{}))
	a := body("m-conf", "provider-a", "k-a", "t-a", w, p, "BET", "25.00", "")
	c.send(t, a, w, "m-conf")
	eventually(t, "the original to be handled", 30*time.Second, func() bool { return c.inboxRows(w) == 1 })
	before := c.view(w)

	conflicting := body("m-conf", "provider-a", "k-b", "t-b", w, p, "BET", "10.00", "")
	c.adminSend(t, conflicting, w, "conflict-1")
	eventually(t, "the conflict diagnostic", 30*time.Second, func() bool {
		return c.count(`SELECT count(*) FROM message_diagnostics WHERE code = 'MESSAGE_ID_CONFLICT' AND envelope_message_id = 'm-conf'`) == 1
	})
	got := c.drain(t, c.proof.res.DLQURL, 1, 30*time.Second)
	if len(got) != 1 || got[0].Body != conflicting || got[0].Group != w {
		t.Fatalf("the DLQ must hold the conflicting body, exactly, grouped by the wallet: %+v", got)
	}
	c.inputEmpty(t, 20*time.Second)
	if after := c.view(w); after != before {
		t.Fatalf("the conflict changed the financial state: %+v -> %+v", before, after)
	}
	if n := c.count(`SELECT count(*) FROM inbox_messages WHERE message_id = 'm-conf'`); n != 1 {
		t.Fatalf("the original inbox row must remain the only one: %d", n)
	}
}

func TestInvalidMessagesAreDiagnosedForwardedToTheDLQAndNeverBecomeOperations(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	c.run(t, c.newConsumer(t, nil, nil, inbound.Hooks{}))
	var txsBefore int64 = c.count(`SELECT count(*) FROM wager_transactions`)
	bodies := map[string]string{
		"not json":       `{"messageId": nope`,
		"opening":        body("m-o", "provider-a", "k", "t", w, p, "OPENING", "10.00", ""),
		"missing wallet": body("m-w", "provider-a", "k", "t", uuid.NewString(), p, "BET", "10.00", ""),
	}
	for name, b := range bodies {
		c.adminSend(t, b, "g-"+strings.ReplaceAll(name, " ", "-"), uuid.NewString())
	}
	got := c.drain(t, c.proof.res.DLQURL, len(bodies), 40*time.Second)
	if len(got) != len(bodies) {
		t.Fatalf("DLQ received %d, want %d", len(got), len(bodies))
	}
	byBody := map[string]auditMessage{}
	for _, m := range got {
		byBody[m.Body] = m
	}
	for name, b := range bodies {
		m, ok := byBody[b]
		if !ok {
			t.Errorf("%s: the original body is not in the DLQ", name)
			continue
		}
		if name == "not json" && m.Group != "unparsed" {
			t.Errorf("an unreadable body is grouped as unparsed, got %q", m.Group)
		}
	}
	c.inputEmpty(t, 20*time.Second)
	if n := c.count(`SELECT count(*) FROM message_diagnostics`); n != int64(len(bodies)) {
		t.Fatalf("diagnostics = %d, want %d", n, len(bodies))
	}
	if txs := c.count(`SELECT count(*) FROM wager_transactions`); txs != txsBefore {
		t.Fatalf("invalid messages created %d financial transactions", txs-txsBefore)
	}
	if v := c.view(w); v.balance != 10000 || v.ledger != 1 {
		t.Fatalf("state: %+v", v)
	}
}

// Queda depois do commit e antes do Delete: a reentrega REAL (visibilidade) é recebida por outra execução, que recupera a conclusão e remove.
func TestADeathAfterTheCommitBeforeTheDeleteIsRecoveredByARealRedelivery(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	spy := &tspy{inner: c.runtime, failDelete: func(n int) bool { return n == 1 }}
	c.run(t, c.newConsumer(t, nil, spy, inbound.Hooks{}))
	b := body("m-del", "provider-a", "k-del", "t-del", w, p, "BET", "25.00", "")
	c.send(t, b, w, "m-del")
	eventually(t, "the redelivery after the lost delete", 60*time.Second, func() bool { return spy.receiveCount() >= 2 && spy.deleteCount() >= 1 })
	c.inputEmpty(t, 30*time.Second)
	if v := c.view(w); v.balance != 7500 || v.version != 2 || v.ledger != 2 || v.inbox != 1 {
		t.Fatalf("one effect after the recovery: %+v", v)
	}
	if spy.receives[0].BrokerID != spy.receives[1].BrokerID {
		t.Fatal("the redelivery is the same broker message")
	}
	if spy.receives[1].ReceiveCount < 2 {
		t.Fatalf("the second receive must be a real redelivery, ApproximateReceiveCount=%d", spy.receives[1].ReceiveCount)
	}
}

// Diagnóstico confirmado, queda antes do envio à DLQ: a reentrega retoma o encaminhamento mesmo com o diagnóstico já concluído.
func TestADiagnosticAlreadyRecordedDoesNotSkipTheDLQForward(t *testing.T) {
	c := newConsEnv(t)
	spy := &tspy{inner: c.runtime, failForward: func(n int) bool { return n == 1 }}
	c.run(t, c.newConsumer(t, nil, spy, inbound.Hooks{}))
	bad := `{"definitely":"not an envelope"}`
	c.adminSend(t, bad, "unparsed", uuid.NewString())
	eventually(t, "the diagnostic", 30*time.Second, func() bool { return c.count(`SELECT count(*) FROM message_diagnostics`) == 1 })
	got := c.drain(t, c.proof.res.DLQURL, 1, 60*time.Second)
	if len(got) != 1 || got[0].Body != bad {
		t.Fatalf("the redelivery must resume the forward after the first attempt died: %+v", got)
	}
	c.inputEmpty(t, 30*time.Second)
	if n := c.count(`SELECT count(*) FROM message_diagnostics`); n != 1 {
		t.Fatalf("one diagnostic: %d", n)
	}
	if spy.receiveCount() < 2 {
		t.Fatalf("the message was redelivered %d time(s)", spy.receiveCount())
	}
}

// Falha transitória com retry/backoff: nada é removido até o sucesso; Delete só depois do commit; efeito único.
func TestTransientFailuresRetryWithBackoffAndNeverDeleteBeforeTheCommit(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	spy := &tspy{inner: c.runtime}
	flaky := &flakyHandler{Handler: c.svc, failures: 2}
	c.run(t, c.newConsumer(t, flaky, spy, inbound.Hooks{}))
	b := body("m-tr", "provider-a", "k-tr", "t-tr", w, p, "BET", "25.00", "")
	c.send(t, b, w, "m-tr")
	eventually(t, "the third delivery to succeed", 90*time.Second, func() bool { return c.inboxRows(w) == 1 })
	c.inputEmpty(t, 30*time.Second)
	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.delays) < 2 {
		t.Fatalf("two failures must have scheduled two retries, got %v", spy.delays)
	}
	for i, d := range spy.delays[:2] {
		if d <= 0 || d > time.Second {
			t.Errorf("retry %d scheduled %v, want within (0, backoff max]", i, d)
		}
	}
	if spy.deletes != 1 {
		t.Fatalf("exactly one delete, after the commit: %d", spy.deletes)
	}
	if v := c.view(w); v.balance != 7500 || v.version != 2 || v.ledger != 2 {
		t.Fatalf("one effect: %+v", v)
	}
}

type flakyHandler struct {
	inbound.Handler
	mu       sync.Mutex
	failures int
}

func (f *flakyHandler) ProcessMessage(ctx context.Context, consumer string, body []byte) app.InboundResult {
	f.mu.Lock()
	if f.failures > 0 {
		f.failures--
		f.mu.Unlock()
		return app.InboundResult{Disposition: app.InboundRetry, Cause: fmt.Errorf("%w: database unavailable", app.ErrTransient)}
	}
	f.mu.Unlock()
	return f.Handler.ProcessMessage(ctx, consumer, body)
}

// Se ChangeMessageVisibility falhar, a mensagem segue recuperável pela visibilidade já existente (nunca removida).
func TestAFailedVisibilityChangeLeavesTheMessageRecoverable(t *testing.T) {
	c := newConsEnv(t)
	w, p := c.open("100.00")
	spy := &tspy{inner: c.runtime, failVis: func(int) bool { return true }}
	flaky := &flakyHandler{Handler: c.svc, failures: 1}
	c.run(t, c.newConsumer(t, flaky, spy, inbound.Hooks{}))
	c.send(t, body("m-vis", "provider-a", "k-vis", "t-vis", w, p, "BET", "25.00", ""), w, "m-vis")
	eventually(t, "the redelivery by the existing visibility", 60*time.Second, func() bool { return c.inboxRows(w) == 1 })
	c.inputEmpty(t, 30*time.Second)
	if spy.deleteCount() != 1 {
		t.Fatalf("deletes = %d", spy.deleteCount())
	}
}

// Esgotados os recebimentos, o redrive do broker leva a mensagem à DLQ com o conteúdo preservado; nada é desfeito ou reaplicado.
func TestRedriveAfterTheReceiveLimitPreservesTheContent(t *testing.T) {
	c := newConsEnv(t, func(cfg *broker.Config) { cfg.InputMaxReceive = 2 })
	w, p := c.open("100.00")
	always := &flakyHandler{Handler: c.svc, failures: 1 << 30}
	spy := &tspy{inner: c.runtime}
	c.run(t, c.newConsumer(t, always, spy, inbound.Hooks{}))
	b := body("m-rd", "provider-a", "k-rd", "t-rd", w, p, "BET", "25.00", "")
	c.send(t, b, w, "m-rd")
	got := c.drain(t, c.proof.res.DLQURL, 1, 90*time.Second)
	if len(got) != 1 || got[0].Body != b {
		t.Fatalf("redrive must deliver the original body to the DLQ: %+v", got)
	}
	if v := c.view(w); v.balance != 10000 || v.inbox != 0 {
		t.Fatalf("a message that never succeeded leaves the state untouched: %+v", v)
	}
	if spy.deleteCount() != 0 {
		t.Fatal("a message that kept failing must never be deleted by the consumer")
	}
}

// Encerramento: a busca é cancelada na admissão, o tratamento admitido termina (Delete depois do commit) e Done só fecha depois disso.
func TestShutdownStopsSearchingDrainsTheAdmittedMessageAndReleasesWhenCancelled(t *testing.T) {
	ctx := context.Background()
	t.Run("drain", func(t *testing.T) {
		c := newConsEnv(t)
		w, p := c.open("100.00")
		gate, inflight := make(chan struct{}), make(chan struct{}, 8)
		slow := &gatedHandler{Handler: c.svc, gate: gate, inflight: inflight}
		spy := &tspy{inner: c.runtime}
		cons := c.newConsumer(t, slow, spy, inbound.Hooks{})
		co := lifecycle.New(quietLog(), 4*time.Second)
		co.OnShutdown(cons.SetShutdownDeadline)
		co.Add(cons)
		if err := co.Start(ctx); err != nil {
			t.Fatal(err)
		}
		c.send(t, body("m-s1", "provider-a", "k-s1", "t-s1", w, p, "BET", "25.00", ""), w, "m-s1")
		<-inflight // uma mensagem está em tratamento
		stopped := make(chan error, 1)
		go func() {
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			stopped <- co.Stop(sctx)
		}()
		time.Sleep(300 * time.Millisecond) // a admissão já fechou; o tratamento admitido segue
		select {
		case <-cons.Done():
			t.Fatal("Done must wait for the admitted message")
		default:
		}
		close(gate)
		if err := <-stopped; err != nil {
			t.Fatalf("stop: %v", err)
		}
		if v := c.view(w); v.inbox != 1 || v.balance != 7500 {
			t.Fatalf("the admitted message must be committed: %+v", v)
		}
		if spy.deleteCount() != 1 {
			t.Fatalf("the committed message must be deleted before Done: %d", spy.deleteCount())
		}
		c.inputEmpty(t, 20*time.Second)
	})

	t.Run("cancel", func(t *testing.T) {
		c := newConsEnv(t)
		w, p := c.open("100.00")
		blocked := make(chan struct{}, 8)
		stuck := &ctxHandler{Handler: c.svc, started: blocked}
		spy := &tspy{inner: c.runtime}
		cons := c.newConsumer(t, stuck, spy, inbound.Hooks{})
		co := lifecycle.New(quietLog(), 500*time.Millisecond)
		co.OnShutdown(cons.SetShutdownDeadline)
		co.Add(cons)
		if err := co.Start(ctx); err != nil {
			t.Fatal(err)
		}
		b := body("m-s2", "provider-a", "k-s2", "t-s2", w, p, "BET", "25.00", "")
		c.send(t, b, w, "m-s2")
		<-blocked
		sctx, cancel := context.WithTimeout(ctx, 6*time.Second)
		defer cancel()
		if err := co.Stop(sctx); err != nil {
			t.Fatalf("stop after cancellation: %v", err)
		}
		select {
		case <-cons.Done():
		default:
			t.Fatal("Done must include the final action on the broker")
		}
		if spy.deleteCount() != 0 {
			t.Fatal("a cancelled treatment must never delete the message")
		}
		if v := c.view(w); v.inbox != 0 || v.balance != 10000 {
			t.Fatalf("nothing was committed: %+v", v)
		}
		// A visibilidade foi liberada: outra instância recebe de imediato (sem esperar os 2s de visibilidade).
		other := c.newConsumer(t, nil, nil, inbound.Hooks{})
		start := time.Now()
		c.run(t, other)
		eventually(t, "another consumer to take the released message", 30*time.Second, func() bool { return c.inboxRows(w) == 1 })
		if time.Since(start) > 15*time.Second {
			t.Fatal("recovery took too long")
		}
	})
}

type gatedHandler struct {
	inbound.Handler
	gate     chan struct{}
	inflight chan struct{}
}

func (g *gatedHandler) ProcessMessage(ctx context.Context, consumer string, body []byte) app.InboundResult {
	g.inflight <- struct{}{}
	<-g.gate
	return g.Handler.ProcessMessage(ctx, consumer, body)
}

type ctxHandler struct {
	inbound.Handler
	started chan struct{}
}

func (g *ctxHandler) ProcessMessage(ctx context.Context, consumer string, body []byte) app.InboundResult {
	g.started <- struct{}{}
	<-ctx.Done()
	return app.InboundResult{Disposition: app.InboundRetry, Cause: ctx.Err()}
}
