//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"go.uber.org/fx"

	"desafio-tecnico-go/internal/bootstrap"
	"desafio-tecnico-go/internal/config"
	"desafio-tecnico-go/internal/infra/broker"
	infrasns "desafio-tecnico-go/internal/infra/sns"
	"desafio-tecnico-go/internal/lifecycle"
	"desafio-tecnico-go/internal/outbox"
)

// Publicação da outbox no SNS FIFO (MiniStack com AUTH=true) com o principal wager-publisher real, o PostgreSQL real e a fila
// de auditoria assinante como observador da entrega. A fila assinante pode deduplicar no transporte (FIFO): a prova de
// republicação observa as chamadas ao SNS (sink contador), nunca um único recebimento.

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fastConfig usa lease e backoff pequenos (o padrão de produção é lease 20 s, backoff 1 s a 30 s).
func fastConfig(workers int) outbox.Config {
	return outbox.Config{Workers: workers, Lease: 20 * time.Second, PublishTimeout: 5 * time.Second, SQLTimeout: 2 * time.Second,
		PollInterval: 20 * time.Millisecond, BackoffInitial: 100 * time.Millisecond, BackoffMax: 400 * time.Millisecond}
}

func (e *pubEnv) snsSink(t *testing.T, role broker.Role) *infrasns.Publisher {
	t.Helper()
	p, err := infrasns.New(context.Background(), infrasns.Config{
		Region: e.proof.cfg.Region, EndpointURL: e.proof.cfg.Endpoint, TopicARN: e.proof.res.TopicARN,
		CredentialsFile: e.proof.cfg.CredentialsFile, Profile: e.proof.res.Profiles[role],
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// countingSink registra cada chamada ao SNS e permite injetar falha antes ou depois do envio real.
type countingSink struct {
	inner  outbox.Sink
	mu     sync.Mutex
	calls  []outbox.Message
	before func(n int, m outbox.Message) error // falha ANTES do envio (nada chega ao broker)
	after  func(n int, m outbox.Message) error // falha DEPOIS do envio real (resultado incerto)
	delay  time.Duration
}

func (s *countingSink) Publish(ctx context.Context, m outbox.Message) error {
	s.mu.Lock()
	s.calls = append(s.calls, m)
	n := len(s.calls)
	s.mu.Unlock()
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.before != nil {
		if err := s.before(n, m); err != nil {
			return err
		}
	}
	if err := s.inner.Publish(ctx, m); err != nil {
		return err
	}
	if s.after != nil {
		return s.after(n, m)
	}
	return nil
}

func (s *countingSink) count(eventID uuid.UUID) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.calls {
		if m.DedupID == eventID.String() {
			n++
		}
	}
	return n
}

func (s *countingSink) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

// storeProbe injeta falha na confirmação: antes de executá-la (processo morto antes de confirmar) ou depois (resposta perdida).
type storeProbe struct {
	outbox.Store
	mu          sync.Mutex
	confirms    int
	failBefore  func(n int) bool // não executa a confirmação e devolve erro
	failAfter   func(n int) bool // executa a confirmação e devolve erro
	confirmedOK int
}

func (s *storeProbe) Confirm(ctx context.Context, r outbox.Reservation) error {
	s.mu.Lock()
	s.confirms++
	n := s.confirms
	s.mu.Unlock()
	if s.failBefore != nil && s.failBefore(n) {
		return errors.New("simulated: the process died before confirming")
	}
	err := s.Store.Confirm(ctx, r)
	if err == nil {
		s.mu.Lock()
		s.confirmedOK++
		s.mu.Unlock()
	}
	if err == nil && s.failAfter != nil && s.failAfter(n) {
		return errors.New("simulated: the confirmation response was lost")
	}
	return err
}

// drainAudit lê E APAGA as mensagens da fila de auditoria (com o cliente administrador da prova: o inspetor só lê, e numa fila FIFO
// uma mensagem em voo segura as seguintes do mesmo grupo) até haver `want` mensagens ou o prazo acabar.
func (e *pubEnv) drainAudit(t *testing.T, want int, within time.Duration) []auditMessage {
	t.Helper()
	admin := sqs.NewFromConfig(e.proof.admin())
	var out []auditMessage
	deadline := time.Now().Add(within)
	for len(out) < want && time.Now().Before(deadline) {
		for _, m := range receive(t, admin, e.proof.res.AuditURL, 10, 1, 30) {
			out = append(out, auditMessage{Body: aws.ToString(m.Body), Group: m.Attributes["MessageGroupId"], Dedup: m.Attributes["MessageDeduplicationId"]})
			if _, err := admin.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(e.proof.res.AuditURL), ReceiptHandle: m.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return out
}

type auditMessage struct{ Body, Group, Dedup string }

func (e *pubEnv) waitPublished(ids []uuid.UUID, within time.Duration) {
	e.t.Helper()
	eventually(e.t, fmt.Sprintf("%d events to be published", len(ids)), within, func() bool {
		var n int64
		if err := e.owner.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE id = ANY($1) AND published_at IS NOT NULL`, ids).Scan(&n); err != nil {
			e.t.Fatal(err)
		}
		return int(n) == len(ids)
	})
}

func (e *pubEnv) newPublisher(sink outbox.Sink, cfg outbox.Config) *outbox.Publisher {
	return outbox.New(e.store(), sink, cfg, quietLog())
}

func (e *pubEnv) appConfig(mods ...func(*config.Config)) config.Config {
	return testConfig(append([]func(*config.Config){func(c *config.Config) {
		c.DatabaseURL = e.db.runtimeURL()
		c.OutboxPublishers = 1
		c.OutboxLease, c.OutboxPublishTimeout, c.OutboxSQLTimeout = 20*time.Second, 3*time.Second, 2*time.Second // testConfig: drenagem de 5 s, reserva de 5 s
		c.OutboxPollInterval, c.OutboxBackoffInitial, c.OutboxBackoffMax = 20*time.Millisecond, 100*time.Millisecond, 400*time.Millisecond
		c.SNSEventTopicARN, c.OutboxCredentialsFile, c.OutboxAWSProfile = e.proof.res.TopicARN, e.proof.cfg.CredentialsFile, e.proof.res.Profiles[broker.Publisher]
		c.AWSEndpointURL = e.proof.cfg.Endpoint
	}}, mods...)...)
}

// 1 e 2: todos os tipos de evento, backlog anterior, envelope/snapshot, grupo por walletId e deduplicação por eventId.
func TestPublisherPublishesEveryEventTypeWithTheStoredSnapshotAndRouting(t *testing.T) {
	e := newPubEnv(t)
	ctx := context.Background()
	// Backlog confirmado antes de o publisher existir neste processo.
	backlogWallet := uuid.NewString()
	backlog := []uuid.UUID{e.insertEvent(backlogWallet, "WalletBalanceChanged"), e.insertEvent(backlogWallet, "WagerTransactionProcessed")}

	// Sem o publisher: o fluxo financeiro grava os eventos e nada é publicado.
	off := startApp(t, e.appConfig(func(c *config.Config) { c.OutboxPublishers = 0 }))
	id := scoped()
	w := openWallet(t, off, "100.00")
	if r := submit(t, off, "provider-a", id("k-bet"), betBody(w, "provider-a", id("bet"), "25.00")); r.Status != 200 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, off, "provider-a", id("k-win"), refBody(w, "provider-a", id("win"), "WIN", "10.00", "")); r.Status != 200 {
		t.Fatalf("win: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, off, "provider-a", id("k-loss"), refBody(w, "provider-a", id("loss"), "LOSS", "0.00", "")); r.Status != 200 {
		t.Fatalf("loss: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, off, "provider-a", id("k-big"), betBody(w, "provider-a", id("big"), "9999.00")); r.Status != 422 {
		t.Fatalf("rejected bet: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, off, "provider-a", id("k-pend"), refBody(w, "provider-a", id("pend"), "REFUND", "5.00", id("never-arrives"))); r.Status != 202 {
		t.Fatalf("pending refund: %d %s", r.Status, r.Raw)
	}
	off.Stop(t)
	var total int64
	_ = e.owner.QueryRow(ctx, `SELECT count(*) FROM outbox_events`).Scan(&total)
	if total < 8 || e.countUnpublished() != total {
		t.Fatalf("with the publisher off %d events must exist and none be published (got %d unpublished)", total, e.countUnpublished())
	}
	types := map[string]bool{}
	rows, err := e.owner.Query(ctx, `SELECT DISTINCT event_type FROM outbox_events`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s string
		_ = rows.Scan(&s)
		types[s] = true
	}
	rows.Close()
	for _, want := range []string{"WagerTransactionProcessed", "WagerTransactionRejected", "WalletBalanceChanged", "WagerTransactionPendingReference"} {
		if !types[want] {
			t.Fatalf("scenario did not produce %s (got %v)", want, types)
		}
	}

	// Com o publisher ativo no mesmo banco: o backlog e os eventos financeiros são publicados.
	on := startApp(t, e.appConfig())
	defer on.Stop(t)
	var all []uuid.UUID
	ids, _ := e.owner.Query(ctx, `SELECT id FROM outbox_events`)
	for ids.Next() {
		var u uuid.UUID
		_ = ids.Scan(&u)
		all = append(all, u)
	}
	ids.Close()
	e.waitPublished(all, 30*time.Second)

	msgs := e.drainAudit(t, len(all), 20*time.Second)
	if len(msgs) != len(all) {
		t.Fatalf("the subscriber received %d messages, want %d", len(msgs), len(all))
	}
	seen := map[uuid.UUID]bool{}
	for _, m := range msgs {
		var env struct {
			EventID uuid.UUID `json:"eventId"`
			Data    struct {
				WalletID string `json:"walletId"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(m.Body), &env); err != nil {
			t.Fatalf("body is not the envelope: %v: %s", err, m.Body)
		}
		st := e.state(env.EventID)
		if m.Body != st.Body { // exatamente o snapshot gravado: dinheiro e walletVersion nunca passam por float
			t.Fatalf("published body differs from the stored snapshot:\n%s\n%s", m.Body, st.Body)
		}
		if m.Group != env.Data.WalletID || m.Group == "" || m.Dedup != env.EventID.String() {
			t.Fatalf("routing: group %q (data.walletId %q), dedup %q (eventId %s)", m.Group, env.Data.WalletID, m.Dedup, env.EventID)
		}
		if seen[env.EventID] {
			t.Fatalf("event %s delivered twice without any fault", env.EventID)
		}
		seen[env.EventID] = true
		if st.Attempts != 1 || st.Token != nil || st.Expires != nil {
			t.Fatalf("event %s after publication: %+v", env.EventID, st)
		}
	}
	for _, id := range backlog {
		if !seen[id] {
			t.Fatalf("backlog event %s was not published", id)
		}
	}
	// O grupo de uma BET/WIN/LOSS é a carteira, não a transação (aggregateId): confira num evento de transação.
	var agg, wallet string
	if err := e.owner.QueryRow(ctx, `SELECT aggregate_id::text, payload #>> '{data,walletId}' FROM outbox_events WHERE event_type = 'WagerTransactionProcessed' AND aggregate_type = 'WagerTransaction' LIMIT 1`).Scan(&agg, &wallet); err != nil {
		t.Fatal(err)
	}
	if agg == wallet {
		t.Fatal("the scenario must exercise an event whose aggregateId (transaction) is not the walletId")
	}
}

// 2: sem permissão, nada é marcado como publicado; a falha de permissão aparece na partida.
func TestPublisherWithoutPermissionNeverMarksAnEventPublished(t *testing.T) {
	e := newPubEnv(t)
	id := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")

	// A partida falha em voz alta com AccessDenied do broker.
	app := fx.New(bootstrap.Options(e.appConfig(func(c *config.Config) { c.OutboxAWSProfile = e.proof.res.Profiles[broker.Nobody] })), fx.NopLogger)
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := app.Start(ctx)
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("start with a principal that cannot use the topic = %v, want AccessDenied", err)
	}

	// Mesmo contornando a verificação de partida, a publicação negada vira retry com diagnóstico e nunca published_at.
	p := e.newPublisher(e.snsSink(t, broker.Nobody), fastConfig(1))
	if !p.Step() {
		t.Fatal("the event must have been reserved")
	}
	if s := e.state(id); s.Published || s.Attempts != 1 || !strings.Contains(s.LastError, "AccessDenied") || s.Token != nil || !s.NextAt.After(time.Now().Add(-time.Second)) {
		t.Fatalf("after a denied publication: %+v", s)
	}
	if got := e.drainAudit(t, 1, 2*time.Second); len(got) != 0 {
		t.Fatalf("a denied publication reached the subscriber: %+v", got)
	}
}

// 3: dois publishers disputam a mesma outbox; cada evento é publicado uma vez (sem falhas) e os dois progridem.
func TestTwoPublishersShareTheOutboxWithoutSharingEvents(t *testing.T) {
	e := newPubEnv(t)
	ids := e.insertEvents(60, 12)
	s1 := &countingSink{inner: e.snsSink(t, broker.Publisher), delay: 15 * time.Millisecond}
	s2 := &countingSink{inner: e.snsSink(t, broker.Publisher), delay: 15 * time.Millisecond}
	p1, p2 := e.newPublisher(s1, fastConfig(2)), e.newPublisher(s2, fastConfig(2))
	for _, p := range []*outbox.Publisher{p1, p2} {
		_ = p.Start(context.Background())
	}
	e.waitPublished(ids, 60*time.Second)
	for _, p := range []*outbox.Publisher{p1, p2} {
		p.StopAdmission()
		<-p.Done()
	}
	if s1.total() == 0 || s2.total() == 0 {
		t.Fatalf("both publishers must make progress (got %d and %d publications)", s1.total(), s2.total())
	}
	for _, id := range ids {
		if n := s1.count(id) + s2.count(id); n != 1 {
			t.Errorf("event %s published %d times by two healthy publishers", id, n)
		}
	}
	if got := e.drainAudit(t, len(ids), 20*time.Second); len(got) != len(ids) {
		t.Fatalf("the subscriber received %d, want %d", len(got), len(ids))
	}
}

// 4a: reserva abandonada (processo morto antes de enviar): a lease vence e outro executor publica.
func TestAnAbandonedReservationIsRecoveredByAnotherExecutor(t *testing.T) {
	e := newPubEnv(t)
	id := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")
	dead := mustReserve(t, e.store(), 700*time.Millisecond) // o "processo" reservou e morreu sem enviar

	sink := &countingSink{inner: e.snsSink(t, broker.Publisher)}
	p := e.newPublisher(sink, fastConfig(1))
	_ = p.Start(context.Background())
	defer func() { p.StopAdmission(); <-p.Done() }()
	time.Sleep(300 * time.Millisecond)
	if sink.total() != 0 {
		t.Fatal("the event is under a valid lease: nobody may publish it yet")
	}
	e.waitPublished([]uuid.UUID{id}, 20*time.Second)
	if s := e.state(id); s.Attempts != 2 || s.Token != nil {
		t.Fatalf("recovered event: %+v", s)
	}
	if err := e.store().Confirm(context.Background(), *dead); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("the dead executor coming back = %v, want ErrLeaseLost", err)
	}
}

// 4b: enviado e não confirmado (processo morto antes de confirmar): republicação com o MESMO eventId e snapshot, sem novo efeito financeiro.
func TestALostConfirmationRepublishesTheSameSnapshotWithoutAnyFinancialEffect(t *testing.T) {
	e := newPubEnv(t)
	off := startApp(t, e.appConfig(func(c *config.Config) { c.OutboxPublishers = 0 }))
	id := scoped()
	w := openWallet(t, off, "100.00")
	if r := submit(t, off, "provider-a", id("k"), betBody(w, "provider-a", id("bet"), "25.00")); r.Status != 200 {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	off.Stop(t)
	var ids []uuid.UUID
	rows, _ := e.owner.Query(context.Background(), `SELECT id FROM outbox_events`)
	for rows.Next() {
		var u uuid.UUID
		_ = rows.Scan(&u)
		ids = append(ids, u)
	}
	rows.Close()
	financial := func() (int64, int64, int64, int64) {
		var bal, ver, led, txs int64
		_ = e.owner.QueryRow(context.Background(), `SELECT balance, version FROM wallets WHERE id = $1`, w.ID).Scan(&bal, &ver)
		_ = e.owner.QueryRow(context.Background(), `SELECT count(*) FROM wallet_ledger_entries`).Scan(&led)
		_ = e.owner.QueryRow(context.Background(), `SELECT count(*) FROM wager_transactions`).Scan(&txs)
		return bal, ver, led, txs
	}
	b0, v0, l0, t0 := financial()

	cfg := fastConfig(1)
	cfg.Lease = 700 * time.Millisecond // só nesta prova (a validação de configuração de produção não se aplica a outbox.New)
	sink := &countingSink{inner: e.snsSink(t, broker.Publisher)}
	// O primeiro executor envia tudo e "morre" antes de cada confirmação.
	p1 := outbox.New(&storeProbe{Store: e.store(), failBefore: func(int) bool { return true }}, sink, cfg, quietLog())
	for range ids {
		if !p1.Step() {
			t.Fatal("expected an event")
		}
	}
	if sink.total() != len(ids) || e.countUnpublished() != int64(len(ids)) {
		t.Fatalf("after the lost confirmations: %d publications, %d unpublished", sink.total(), e.countUnpublished())
	}
	// Outro executor assume quando as leases vencem e republica os mesmos eventos.
	p2 := outbox.New(e.store(), sink, cfg, quietLog())
	_ = p2.Start(context.Background())
	defer func() { p2.StopAdmission(); <-p2.Done() }()
	e.waitPublished(ids, 30*time.Second)
	for _, evID := range ids {
		if sink.count(evID) != 2 {
			t.Errorf("event %s: %d publications, want 2 (original + republication)", evID, sink.count(evID))
		}
	}
	first := map[string]string{}
	for _, m := range sink.calls {
		if prev, ok := first[m.DedupID]; ok && (prev != m.Body) {
			t.Fatalf("republication changed the snapshot of %s", m.DedupID)
		}
		first[m.DedupID] = m.Body
	}
	if b1, v1, l1, t1 := func() (int64, int64, int64, int64) { return financial() }(); b0 != b1 || v0 != v1 || l0 != l1 || t0 != t1 {
		t.Fatalf("republishing produced a financial effect: (%d,%d,%d,%d) -> (%d,%d,%d,%d)", b0, v0, l0, t0, b1, v1, l1, t1)
	}
	if got := e.drainAudit(t, len(ids), 10*time.Second); len(got) < len(ids) {
		t.Fatalf("the subscriber received %d, want at least %d", len(got), len(ids))
	}
}

// 6: falhas e resultados incertos do Publish e da confirmação, e persistência do backoff entre instâncias.
func TestPublishFailuresAreRetriedFromTheDatabaseScheduleAndSurviveRestarts(t *testing.T) {
	e := newPubEnv(t)
	cfg := fastConfig(1)
	cfg.BackoffInitial, cfg.BackoffMax = 800*time.Millisecond, 800*time.Millisecond
	id := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")

	// Broker indisponível: nada chega ao broker, o retry fica no banco.
	sink := &countingSink{inner: e.snsSink(t, broker.Publisher), before: func(n int, _ outbox.Message) error {
		if n == 1 {
			return errors.New("broker temporarily unavailable")
		}
		return nil
	}}
	first := e.newPublisher(sink, cfg)
	if !first.Step() {
		t.Fatal("expected an event")
	}
	s := e.state(id)
	if s.Published || s.Attempts != 1 || !strings.Contains(s.LastError, "broker temporarily unavailable") || s.Token != nil || !s.NextAt.After(time.Now()) {
		t.Fatalf("after the failed publication: %+v", s)
	}
	// "Reinício": outra instância não contorna a agenda que está no banco.
	restarted := e.newPublisher(sink, cfg)
	if restarted.Step() || sink.total() != 1 {
		t.Fatal("a restarted publisher must respect the persisted backoff")
	}
	e.dbPassed(s.NextAt)
	if !restarted.Step() {
		t.Fatal("after the backoff the event is eligible again")
	}
	if st := e.state(id); !st.Published || st.Attempts != 2 || sink.count(id) != 2 {
		t.Fatalf("after the retry: %+v (calls %d)", st, sink.count(id))
	}
	if got := e.drainAudit(t, 1, 10*time.Second); len(got) != 1 || got[0].Dedup != id.String() {
		t.Fatalf("delivery after the retry: %+v", got)
	}

	// Resultado incerto do Publish (aceito pelo broker, erro no retorno): retry e republicação do mesmo evento.
	id2 := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")
	uncertain := &countingSink{inner: e.snsSink(t, broker.Publisher), after: func(n int, _ outbox.Message) error {
		if n == 1 {
			return errors.New("connection reset after the request was sent")
		}
		return nil
	}}
	p := e.newPublisher(uncertain, cfg)
	p.Step()
	if s2 := e.state(id2); s2.Published || s2.Attempts != 1 {
		t.Fatalf("an uncertain publication is not a confirmed one: %+v", s2)
	}
	e.dbPassed(e.state(id2).NextAt)
	p.Step()
	if s2 := e.state(id2); !s2.Published || uncertain.count(id2) != 2 {
		t.Fatalf("uncertain Publish: published=%v calls=%d", s2.Published, uncertain.count(id2))
	}

	// Resultado incerto da confirmação SQL (executada, resposta perdida): o evento está publicado e NÃO é republicado.
	id3 := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")
	plain := &countingSink{inner: e.snsSink(t, broker.Publisher)}
	lost := outbox.New(&storeProbe{Store: e.store(), failAfter: func(int) bool { return true }}, plain, cfg, quietLog())
	lost.Step()
	if !e.state(id3).Published {
		t.Fatal("the confirmation was executed: the event is published")
	}
	for i := 0; i < 3; i++ {
		lost.Step()
	}
	if plain.count(id3) != 1 {
		t.Fatalf("a confirmed event was republished (%d calls)", plain.count(id3))
	}
}

// 7: um evento em falha persistente não impede os demais de avançar nem é descartado.
func TestAPersistentlyFailingEventDoesNotBlockOthersAndIsNeverDropped(t *testing.T) {
	e := newPubEnv(t)
	bad := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")
	good := e.insertEvents(10, 3)
	sink := &countingSink{inner: e.snsSink(t, broker.Publisher), before: func(_ int, m outbox.Message) error {
		if m.DedupID == bad.String() {
			return errors.New("poison: the broker rejects this event")
		}
		return nil
	}}
	p := e.newPublisher(sink, fastConfig(2))
	_ = p.Start(context.Background())
	defer func() { p.StopAdmission(); <-p.Done() }()
	e.waitPublished(good, 30*time.Second)
	eventually(t, "several retries of the failing event", 20*time.Second, func() bool { return e.state(bad).Attempts >= 3 })
	if s := e.state(bad); s.Published || !strings.Contains(s.LastError, "poison") {
		t.Fatalf("the failing event must stay pending and diagnosable: %+v", s)
	}
	if n := e.countUnpublished(); n != 1 {
		t.Fatalf("%d unpublished events, want only the failing one", n)
	}
}

// 8: lifecycle — admissão fechada, publicação admitida drenada, cancelamento libera a reserva sem publicar.
func TestShutdownDrainsAnAdmittedPublicationAndNeverFakesOneThatWasCancelled(t *testing.T) {
	ctx := context.Background()

	t.Run("drain", func(t *testing.T) {
		e := newPubEnv(t)
		first := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")
		second := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")
		inflight, release := make(chan struct{}, 4), make(chan struct{})
		sink := &countingSink{inner: e.snsSink(t, broker.Publisher), before: func(int, outbox.Message) error {
			inflight <- struct{}{}
			<-release
			return nil
		}}
		p := e.newPublisher(sink, fastConfig(1))
		c := lifecycle.New(quietLog(), 3*time.Second)
		c.Add(p)
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		<-inflight
		stopped := make(chan error, 1)
		go func() {
			sctx, cancel := context.WithTimeout(ctx, 8*time.Second)
			defer cancel()
			stopped <- c.Stop(sctx)
		}()
		time.Sleep(200 * time.Millisecond) // a admissão já fechou; a publicação admitida segue
		close(release)
		if err := <-stopped; err != nil {
			t.Fatalf("stop: %v", err)
		}
		select {
		case <-p.Done():
		default:
			t.Fatal("Done must be closed after Stop")
		}
		if s := e.state(first); !s.Published {
			t.Fatalf("the admitted publication must be drained and confirmed: %+v", s)
		}
		if s := e.state(second); s.Published || s.Attempts != 0 || s.Token != nil {
			t.Fatalf("no new reservation after admission closed: %+v", s)
		}
	})

	t.Run("cancel", func(t *testing.T) {
		e := newPubEnv(t)
		id := e.insertEvent(uuid.NewString(), "WalletBalanceChanged")
		inflight := make(chan struct{}, 1)
		// Sink que respeita o cancelamento, como o cliente real: bloqueia até o contexto da publicação ser cancelado.
		ctxSink := sinkFunc(func(c context.Context, m outbox.Message) error {
			inflight <- struct{}{}
			<-c.Done()
			return c.Err()
		})
		p := e.newPublisher(ctxSink, fastConfig(1))
		c := lifecycle.New(quietLog(), 400*time.Millisecond)
		c.Add(p)
		if err := c.Start(ctx); err != nil {
			t.Fatal(err)
		}
		<-inflight
		sctx, cancel := context.WithTimeout(ctx, 6*time.Second)
		defer cancel()
		if err := c.Stop(sctx); err != nil {
			t.Fatalf("stop after cancellation: %v", err)
		}
		select {
		case <-p.Done():
		default:
			t.Fatal("Done must include the final SQL action")
		}
		s := e.state(id)
		if s.Published || s.Token != nil || s.Expires != nil || s.LastError != "" || s.Attempts != 1 {
			t.Fatalf("a cancelled publication must release the reservation, not confirm it nor record a failure: %+v", s)
		}
		// Liberada: elegível de imediato para outro executor (sem esperar a lease).
		if r := mustReserve(t, e.store(), time.Second); r.EventID != id {
			t.Fatalf("the released event must be immediately eligible: %+v", r)
		}
	})
}

type sinkFunc func(context.Context, outbox.Message) error

func (f sinkFunc) Publish(ctx context.Context, m outbox.Message) error { return f(ctx, m) }

// 9: dois processos reais do binário (cmd/api) disputam a outbox; SIGKILL de um publisher não perde eventos; SIGTERM drena.
func TestTwoPublisherProcessesDrainTheOutboxAndRecoverFromSIGKILL(t *testing.T) {
	e := newPubEnv(t)
	ids := e.insertEvents(150, 15)
	extra := func(name string) []string {
		return []string{
			"DATABASE_URL=" + withAppName(e.db.runtimeURL(), name),
			"OUTBOX_PUBLISHERS=1", "SNS_EVENT_TOPIC_ARN=" + e.proof.res.TopicARN,
			"OUTBOX_CREDENTIALS_FILE=" + e.proof.cfg.CredentialsFile, "OUTBOX_AWS_PROFILE=" + e.proof.res.Profiles[broker.Publisher],
			"AWS_ENDPOINT_URL=" + e.proof.cfg.Endpoint,
			"OUTBOX_POLL_INTERVAL=20ms", "OUTBOX_PUBLISH_TIMEOUT=1s", "OUTBOX_SQL_TIMEOUT=1s", "OUTBOX_LEASE=9s", "LOG_LEVEL=info",
		}
	}
	// O primeiro processo publica sozinho e é morto no meio do backlog.
	a := startProcEnv(t, "pub-a", extra("pub-a")...)
	eventually(t, "the first process to publish some events", 30*time.Second, func() bool {
		return e.countUnpublished() < int64(len(ids))*4/5
	})
	var leasedAtKill int64
	_ = e.owner.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND lease_expires_at > clock_timestamp()`).Scan(&leasedAtKill)
	if err := a.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	<-a.done
	killedAt := e.countUnpublished()
	if killedAt == 0 {
		t.Skip("the backlog finished before the kill; nothing to recover (re-run)")
	}
	// Dois processos independentes assumem o restante (inclusive a reserva que o processo morto deixou).
	b := startProcEnv(t, "pub-b", extra("pub-b")...)
	c := startProcEnv(t, "pub-c", extra("pub-c")...)
	e.waitPublished(ids, 60*time.Second)
	msgs := e.drainAudit(t, len(ids), 30*time.Second)
	got := map[string]int{}
	for _, m := range msgs {
		got[m.Dedup]++
	}
	for _, id := range ids {
		if got[id.String()] < 1 {
			t.Errorf("event %s never reached the subscriber", id)
		}
		if s := e.state(id); !s.Published || s.Token != nil {
			t.Errorf("event %s: %+v", id, s)
		}
	}
	// SIGTERM: encerramento gracioso e código de saída 0.
	for _, p := range []*proc{b, c} {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
			if p.exit != nil {
				t.Errorf("%s exited with %v after SIGTERM\n%s", p.name, p.exit, p.out.String())
			}
			if !strings.Contains(p.out.String(), "shutdown drained") {
				t.Errorf("%s did not log a drained shutdown:\n%s", p.name, p.out.String())
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("%s did not stop after SIGTERM", p.name)
		}
	}
	t.Logf("killed the first process with %d of %d events unpublished (%d under its lease when sampled); all were published afterwards", killedAt, len(ids), leasedAtKill)
}
