// Package inbound é o consumidor SQS: recebe UMA mensagem por busca em cada executor livre, trata-a com o mesmo núcleo financeiro do
// HTTP (app.Service.ProcessMessage: inbox e finanças no mesmo commit) e só então age no transporte.
//
// Disposições (a ação de transporte NUNCA precede o commit durável):
//
//	Done       → Delete (receipt handle da entrega atual) depois do commit;
//	Retry      → sem Delete: ChangeMessageVisibility com backoff exponencial e jitter, a partir do ApproximateReceiveCount;
//	Permanent  → diagnóstico confirmado → envio confirmado à DLQ → Delete. Um diagnóstico já existente NÃO dispensa o envio.
//
// A busca (StopAdmission) é separada do tratamento (Cancel): fechar a admissão cancela o long polling e impede despachar trabalho novo,
// mas o tratamento já admitido segue até o limite de drenagem. As ações finais no broker usam o menor deadline ABSOLUTO entre o limite
// próprio de I/O e o prazo global do encerramento; sem orçamento, nada é iniciado (a visibilidade devolve a mensagem).
package inbound

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"desafio-tecnico-go/internal/app"
)

// Delivery é uma entrega: Body são os bytes EXATOS recebidos; ReceiptHandle vale só para esta entrega.
type Delivery struct {
	BrokerID      string
	ReceiptHandle string
	Body          []byte
	ReceiveCount  int // ApproximateReceiveCount (>= 1)
}

// Transport é o SQS visto pelo consumidor (principal de runtime).
type Transport interface {
	Receive(ctx context.Context, wait time.Duration) (*Delivery, error)
	Delete(ctx context.Context, d Delivery) error
	ChangeVisibility(ctx context.Context, d Delivery, delay time.Duration) error
	ForwardToDLQ(ctx context.Context, body []byte, groupID, dedupID string, attrs map[string]string) error
}

// Handler é o caso de uso (app.Service).
type Handler interface {
	ProcessMessage(ctx context.Context, consumerName string, body []byte) app.InboundResult
	RecordDiagnostic(ctx context.Context, d app.Diagnostic) error
	HasDiagnostic(ctx context.Context, consumerName, sourceQueue, brokerMessageID string, bodyHash []byte) (bool, error)
}

var _ Handler = (*app.Service)(nil)

// Config parametriza o consumidor.
type Config struct {
	Workers      int           // executores por processo (padrão 4)
	ConsumerName string        // estável entre instâncias e reinícios (nunca hostname nem PID)
	SourceQueue  string        // nome da fila de entrada (identifica o diagnóstico)
	ReceiveWait  time.Duration // long polling (padrão 10s), cancelável
	PhaseTimeout time.Duration // fase financeira de uma tentativa, incluindo locks e commit (padrão 10s)
	IOTimeout    time.Duration // cada ação de transporte e cada consulta/gravação de diagnóstico (padrão 5s)
	// Backoff de reentrega: inicial 5s, teto 60s, exponencial com jitter uniforme em [d/2, d].
	BackoffInitial, BackoffMax time.Duration
	PauseMax                   time.Duration // teto da pausa de buscas em falha global (padrão 30s)
	Visibility                 time.Duration // visibility timeout da fila (60s), só para validação
}

// Validate confere a configuração (também de configurações montadas diretamente).
func (c Config) Validate() error {
	var errs []error
	if c.Workers < 0 || c.Workers > 16 {
		errs = append(errs, errors.New("consumer workers must be between 0 and 16"))
	}
	if c.ConsumerName == "" || len(c.ConsumerName) > 128 {
		errs = append(errs, errors.New("consumer name is required (at most 128 characters)"))
	}
	if c.SourceQueue == "" {
		errs = append(errs, errors.New("consumer source queue is required"))
	}
	for _, f := range []struct {
		name string
		d    time.Duration
	}{{"receive wait", c.ReceiveWait}, {"phase timeout", c.PhaseTimeout}, {"io timeout", c.IOTimeout},
		{"backoff initial", c.BackoffInitial}, {"backoff max", c.BackoffMax}, {"pause max", c.PauseMax}, {"visibility", c.Visibility}} {
		if f.d <= 0 {
			errs = append(errs, errors.New("consumer "+f.name+" must be greater than zero"))
		}
	}
	if c.BackoffInitial > c.BackoffMax {
		errs = append(errs, errors.New("consumer backoff initial must not exceed backoff max"))
	}
	// Uma entrega precisa caber na visibilidade: consulta de diagnóstico + fase financeira + gravação + (DLQ + Delete) / Delete.
	if c.PhaseTimeout+3*c.IOTimeout >= c.Visibility {
		errs = append(errs, errors.New("consumer phase timeout + 3 x io timeout must be shorter than the queue visibility timeout"))
	}
	return errors.Join(errs...)
}

// Hooks são pontos de observação opcionais (métricas): chamados DEPOIS do commit ou da decisão de transporte.
type Hooks struct {
	// Result: o resultado classificado de uma entrega (depois do commit para Done).
	Result func(res app.InboundResult)
	// Retry: uma entrega foi devolvida para nova tentativa; reason é um valor de um conjunto limitado.
	Retry func(reason string)
	// DLQForwarded: o envio manual à DLQ foi CONFIRMADO pelo broker.
	DLQForwarded func(code string)
	// Latency: duração do tratamento de uma entrega (do recebimento até a ação de transporte).
	Latency func(d time.Duration, disposition string)
}

// Consumer é o componente (lifecycle.Component).
type Consumer struct {
	h     Handler
	t     Transport
	cfg   Config
	log   *slog.Logger
	hooks Hooks

	searchCtx    context.Context
	searchCancel context.CancelFunc
	procCtx      context.Context
	procCancel   context.CancelFunc
	done         chan struct{}
	startOnce    sync.Once
	wg           sync.WaitGroup

	deadline   atomic.Pointer[time.Time]
	failures   atomic.Int64                         // falhas transitórias consecutivas (todas as buscas/tratamentos)
	pauseUntil atomic.Pointer[time.Time]            // buscas pausadas até este instante
	rand       func(n int64) int64                  // injetável nos testes
	sleep      func(context.Context, time.Duration) // injetável nos testes
}

// New cria o componente. Nada roda antes de Start.
func New(h Handler, t Transport, cfg Config, log *slog.Logger, hooks Hooks) *Consumer {
	c := &Consumer{h: h, t: t, cfg: cfg, log: log, hooks: hooks, done: make(chan struct{}), rand: rand.Int64N, sleep: sleepCtx}
	c.searchCtx, c.searchCancel = context.WithCancel(context.Background())
	c.procCtx, c.procCancel = context.WithCancel(context.Background())
	return c
}

func (c *Consumer) Name() string { return "sqs-consumer" }

// Start dispara os executores. A vida deles não depende do contexto de OnStart.
func (c *Consumer) Start(context.Context) error {
	c.startOnce.Do(func() {
		for i := 0; i < c.cfg.Workers; i++ {
			c.wg.Add(1)
			go c.run()
		}
		go func() { c.wg.Wait(); close(c.done) }()
	})
	return nil
}

// StopAdmission cancela a busca em andamento e impede novas buscas; o tratamento admitido continua.
func (c *Consumer) StopAdmission() { c.searchCancel() }

// Cancel aborta o tratamento em andamento (a mensagem é liberada para reentrega, não removida).
func (c *Consumer) Cancel() { c.procCancel() }

// Done fecha quando buscas, tratamentos e ações finais no broker terminaram.
func (c *Consumer) Done() <-chan struct{} { return c.done }

// SetShutdownDeadline informa o prazo global do encerramento (instante absoluto).
func (c *Consumer) SetShutdownDeadline(d time.Time) { c.deadline.Store(&d) }

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func (c *Consumer) run() {
	defer c.wg.Done()
	for c.searchCtx.Err() == nil {
		if until := c.pauseUntil.Load(); until != nil {
			if d := time.Until(*until); d > 0 {
				c.sleep(c.searchCtx, d)
				continue
			}
		}
		d, err := c.t.Receive(c.searchCtx, c.cfg.ReceiveWait)
		if err != nil {
			if c.searchCtx.Err() != nil {
				return
			}
			c.log.Warn("sqs receive failed", "error", sanitize(err))
			c.noteFailure()
			c.sleep(c.searchCtx, 100*time.Millisecond)
			continue
		}
		if d == nil {
			continue
		}
		if c.searchCtx.Err() != nil {
			// Recebida por uma busca concorrente ao fechamento da admissão: não será tratada. Devolve a visibilidade (limitado) ou
			// deixa expirar; nunca remove.
			c.release(*d, "shutdown")
			return
		}
		c.handle(*d)
	}
}

// noteFailure conta uma falha transitória; a partir da terceira consecutiva, as buscas pausam (exponencial até PauseMax).
func (c *Consumer) noteFailure() {
	n := c.failures.Add(1)
	if n < 3 {
		return
	}
	pause := time.Second
	for i := int64(3); i < n && pause < c.cfg.PauseMax; i++ {
		pause *= 2
	}
	if pause > c.cfg.PauseMax {
		pause = c.cfg.PauseMax
	}
	until := time.Now().Add(pause)
	c.pauseUntil.Store(&until)
}

func (c *Consumer) noteSuccess() { c.failures.Store(0); c.pauseUntil.Store(nil) }

func (c *Consumer) handle(d Delivery) {
	started := time.Now()
	hash := app.MessageHash(d.Body)
	disposition := c.process(d, hash)
	if c.hooks.Latency != nil {
		c.hooks.Latency(time.Since(started), disposition)
	}
}

func (c *Consumer) process(d Delivery, hash []byte) string {
	// 1. Diagnóstico já confirmado: retoma o encaminhamento (ele não prova que a DLQ recebeu). Erro de consulta NÃO é ausência.
	dctx, cancel := context.WithTimeout(c.procCtx, c.cfg.IOTimeout)
	has, err := c.h.HasDiagnostic(dctx, c.cfg.ConsumerName, c.cfg.SourceQueue, d.BrokerID, hash)
	cancel()
	if err != nil {
		c.retry(d, "diagnostic_lookup", err)
		return "retry"
	}
	if has {
		c.forwardAndDelete(d, hash, "", "", "DIAGNOSED")
		return "permanent"
	}

	// 2. Tratamento: uma tentativa SQL (inbox + finanças); a fase financeira tem prazo próprio.
	pctx, cancel := context.WithTimeout(c.procCtx, c.cfg.PhaseTimeout)
	res := c.h.ProcessMessage(pctx, c.cfg.ConsumerName, d.Body)
	cancel()
	if c.hooks.Result != nil {
		c.hooks.Result(res)
	}
	switch res.Disposition {
	case app.InboundDone:
		c.noteSuccess()
		c.finish(d, res)
		return "done"
	case app.InboundPermanent:
		// Diagnóstico confirmado ANTES de qualquer envio à DLQ.
		rctx, cancel := context.WithTimeout(c.procCtx, c.cfg.IOTimeout)
		err := c.h.RecordDiagnostic(rctx, app.Diagnostic{
			ConsumerName: c.cfg.ConsumerName, SourceQueue: c.cfg.SourceQueue, BrokerMessageID: d.BrokerID, BodyHash: hash,
			Code: res.Code, EnvelopeMessageID: res.MessageID, CorrelationID: res.MessageID,
		})
		cancel()
		if err != nil {
			c.retry(d, "diagnostic_write", err) // falha do banco não é diagnóstico confirmado
			return "retry"
		}
		c.noteSuccess()
		c.log.Warn("message permanently refused", "code", res.Code, "messageId", res.MessageID, "walletId", res.WalletID,
			"providerId", res.ProviderID, "brokerMessageId", d.BrokerID)
		c.forwardAndDelete(d, hash, res.WalletID, res.Code, res.MessageID)
		return "permanent"
	default:
		c.retry(d, retryReason(res.Cause), res.Cause)
		return "retry"
	}
}

// finish remove a mensagem depois do commit durável.
func (c *Consumer) finish(d Delivery, res app.InboundResult) {
	c.log.Info("message handled", "messageId", res.MessageID, "walletId", res.WalletID, "providerId", res.ProviderID,
		"transactionId", res.TransactionID, "status", res.Status, "duplicate", res.Duplicate, "financialReplay", res.FinancialReplay)
	ctx, cancel, ok := c.finalContext()
	if !ok {
		c.log.Warn("shutdown budget exhausted before the delete; the committed message is redelivered and resolved by identity", "messageId", res.MessageID)
		return
	}
	defer cancel()
	if err := c.t.Delete(ctx, d); err != nil {
		// O resultado durável já está confirmado: outra entrega o resolve pela inbox/idempotência e tenta remover de novo.
		c.log.Warn("delete after commit failed; the redelivery resolves it by identity", "messageId", res.MessageID, "error", sanitize(err))
	}
}

// forwardAndDelete: envio confirmado à DLQ → Delete com o receipt atual. Falha em qualquer passo mantém a origem recuperável.
func (c *Consumer) forwardAndDelete(d Delivery, hash []byte, walletID, code, messageID string) {
	if walletID == "" { // retomada de um diagnóstico anterior: o grupo vem do corpo, se for um envelope legível
		walletID = app.WalletIDOf(d.Body)
	}
	if code == "" {
		code = "DIAGNOSED"
	}
	group := walletID
	if group == "" {
		group = "unparsed"
	}
	attrs := map[string]string{"sourceQueue": c.cfg.SourceQueue, "brokerMessageId": d.BrokerID, "reason": code}
	if messageID != "" && messageID != "DIAGNOSED" {
		attrs["messageId"] = messageID
	}
	ctx, cancel, ok := c.finalContext()
	if !ok {
		c.log.Warn("shutdown budget exhausted before the DLQ forward; the message stays recoverable", "brokerMessageId", d.BrokerID)
		return
	}
	err := c.t.ForwardToDLQ(ctx, d.Body, group, DLQDedupID(c.cfg.SourceQueue, d.BrokerID, hash), attrs)
	cancel()
	if err != nil {
		c.log.Warn("forward to the DLQ failed; the message stays recoverable", "brokerMessageId", d.BrokerID, "code", code, "error", sanitize(err))
		c.retry(d, "dlq_forward", err)
		return
	}
	if c.hooks.DLQForwarded != nil {
		c.hooks.DLQForwarded(code)
	}
	dctx, dcancel, ok := c.finalContext()
	if !ok {
		c.log.Warn("shutdown budget exhausted before the delete of a forwarded message; the repetition is tolerated", "brokerMessageId", d.BrokerID)
		return
	}
	defer dcancel()
	if err := c.t.Delete(dctx, d); err != nil {
		c.log.Warn("delete after the DLQ forward failed; the repetition is tolerated", "brokerMessageId", d.BrokerID, "error", sanitize(err))
	}
}

// DLQDedupID é determinístico pela fila de origem, id do broker e hash do corpo (não só pelo messageId do envelope).
func DLQDedupID(sourceQueue, brokerID string, hash []byte) string {
	h := sha256.New()
	h.Write([]byte(sourceQueue))
	h.Write([]byte{0})
	h.Write([]byte(brokerID))
	h.Write([]byte{0})
	h.Write(hash)
	return hex.EncodeToString(h.Sum(nil))
}

// retry devolve a mensagem para nova tentativa: SEM Delete. Em encerramento a visibilidade é liberada de imediato; caso contrário vale
// o backoff. Se o ajuste falhar, a mensagem segue recuperável pela visibilidade já existente.
func (c *Consumer) retry(d Delivery, reason string, cause error) {
	c.noteFailure()
	if c.hooks.Retry != nil {
		c.hooks.Retry(reason)
	}
	delay := c.backoff(d.ReceiveCount)
	if c.procCtx.Err() != nil || c.searchCtx.Err() != nil {
		delay = 0
	}
	c.log.Warn("message returned for retry", "reason", reason, "brokerMessageId", d.BrokerID, "receiveCount", d.ReceiveCount,
		"retryIn", delay.String(), "error", sanitize(cause))
	ctx, cancel, ok := c.finalContext()
	if !ok {
		return // sem orçamento: a visibilidade atual devolve a mensagem
	}
	defer cancel()
	if err := c.t.ChangeVisibility(ctx, d, delay); err != nil {
		c.log.Warn("changing the visibility failed; the message returns when its current visibility expires", "brokerMessageId", d.BrokerID, "error", sanitize(err))
	}
}

// release devolve imediatamente (visibilidade 0) uma mensagem que não será tratada.
func (c *Consumer) release(d Delivery, reason string) {
	ctx, cancel, ok := c.finalContext()
	if !ok {
		return
	}
	defer cancel()
	if err := c.t.ChangeVisibility(ctx, d, 0); err != nil {
		c.log.Warn("releasing an untreated message failed; its visibility expires", "reason", reason, "brokerMessageId", d.BrokerID, "error", sanitize(err))
	}
}

// finalContext cria o contexto de uma ação final no broker/SQL: independente do cancelamento do tratamento e limitado ao MENOR
// instante ABSOLUTO entre agora + IOTimeout e o prazo global conhecido. Sem orçamento, devolve ok=false (nada novo é iniciado).
func (c *Consumer) finalContext() (context.Context, context.CancelFunc, bool) {
	now := time.Now()
	deadline := now.Add(c.cfg.IOTimeout)
	if g := c.deadline.Load(); g != nil {
		if !g.After(now) {
			return nil, nil, false
		}
		if g.Before(deadline) {
			deadline = *g
		}
	}
	ctx, cancel := context.WithDeadline(context.WithoutCancel(c.procCtx), deadline)
	return ctx, cancel, true
}

// backoff: exponencial a partir de BackoffInitial, jitter uniforme em [d/2, d], teto BackoffMax; receiveCount é o número de
// recebimentos (o cálculo pára no teto: sem overflow).
func (c *Consumer) backoff(receiveCount int) time.Duration {
	d := c.cfg.BackoffInitial
	for i := 1; i < receiveCount && d < c.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > c.cfg.BackoffMax || d <= 0 {
		d = c.cfg.BackoffMax
	}
	half := d / 2
	return half + time.Duration(c.rand(int64(d-half)+1))
}

func retryReason(err error) string {
	switch {
	case errors.Is(err, app.ErrCommitUnknown):
		return "commit_unknown"
	case errors.Is(err, app.ErrTransient):
		return "transient"
	case errors.Is(err, app.ErrConcurrentModification), errors.Is(err, app.ErrWalletBusy):
		return "contention"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "unexpected"
}

// sanitize reduz um erro a um texto curto e sem quebras (nunca o corpo da mensagem).
func sanitize(err error) string {
	if err == nil {
		return ""
	}
	s := fmt.Sprint(err)
	if len(s) > 300 {
		s = s[:300]
	}
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			r = ' '
		}
		out = append(out, r)
	}
	return string(out)
}
