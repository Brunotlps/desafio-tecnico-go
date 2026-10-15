// Package outbox publica os eventos da transactional outbox depois do commit financeiro.
//
// Cada executor repete três passos, nunca com uma conexão ou transação SQL aberta durante a rede:
//
//  1. reservar UM evento elegível (transação curta: lease com token único da aquisição, relógio do banco);
//  2. publicar o snapshot armazenado no tópico (fora de qualquer SQL, com prazo total próprio);
//  3. confirmar a publicação — ou registrar o retry/liberar a reserva — em nova transação curta, condicionada ao token da
//     aquisição corrente e à lease ainda válida.
//
// O contrato é at-least-once: um resultado incerto do envio, a perda da confirmação ou uma lease vencida levam a republicar o
// MESMO evento (mesmo eventId e mesmo snapshot). A ordem original da outbox não é garantida entre publishers e retries.
package outbox

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// ErrLeaseLost: a reserva já não pertence a quem a segura (token trocado, lease vencida ou evento já publicado). Nada foi alterado.
var ErrLeaseLost = errors.New("outbox lease lost")

// ErrInvalidEvent: o evento armazenado não pode ser roteado (por exemplo, sem walletId em data). É permanente para esta
// versão do snapshot, mas nunca descarta o evento: ele segue com retry e diagnóstico em last_error.
var ErrInvalidEvent = errors.New("outbox event cannot be routed")

// Reservation é um evento reservado por uma aquisição (lease) específica.
type Reservation struct {
	EventID   uuid.UUID
	Seq       int64
	Type      string
	WalletID  string // data.walletId do envelope: grupo de mensagens (o aggregateId pode ser a transação)
	Body      string // envelope armazenado, exatamente como foi gravado (jsonb em texto)
	Attempts  int    // reservas já feitas para o evento, esta incluída
	Token     uuid.UUID
	ExpiresAt time.Time // relógio do banco
}

// Store é a persistência da reserva. Cada operação é uma transação curta. Confirm, Retry e Release devolvem ErrLeaseLost
// quando a posse não vale mais no momento da escrita (relógio do banco, depois de qualquer espera por lock).
type Store interface {
	// Reserve devolve nil, nil quando não há evento elegível.
	Reserve(ctx context.Context, lease time.Duration) (*Reservation, error)
	Confirm(ctx context.Context, r Reservation) error
	Retry(ctx context.Context, r Reservation, delay time.Duration, reason string) error
	Release(ctx context.Context, r Reservation) error
}

// Message é o que vai ao tópico FIFO.
type Message struct {
	GroupID string // walletId
	DedupID string // eventId
	Type    string
	Body    string
}

// Sink publica uma mensagem. Retorno nil = aceito pelo broker. Qualquer outro retorno é um resultado NÃO confirmado.
type Sink interface {
	Publish(ctx context.Context, m Message) error
}

// Config parametriza os executores.
type Config struct {
	Workers        int           // executores por processo (padrão 1)
	Lease          time.Duration // validade de uma reserva (padrão 20s), sem renovação
	PublishTimeout time.Duration // prazo total de uma publicação, incluindo as tentativas do SDK (padrão 5s)
	SQLTimeout     time.Duration // prazo de cada transação curta de controle (padrão 2s)
	PollInterval   time.Duration // espera quando não há evento elegível (padrão 500ms)
	BackoffInitial time.Duration // padrão 1s
	BackoffMax     time.Duration // teto, também do jitter (padrão 30s)
}

// Validate confere a configuração (também de configurações montadas diretamente).
func (c Config) Validate() error {
	var errs []error
	if c.Workers < 0 || c.Workers > 8 {
		errs = append(errs, errors.New("outbox workers must be between 0 and 8"))
	}
	for _, f := range []struct {
		name string
		d    time.Duration
	}{
		{"lease", c.Lease}, {"publish timeout", c.PublishTimeout}, {"sql timeout", c.SQLTimeout},
		{"poll interval", c.PollInterval}, {"backoff initial", c.BackoffInitial}, {"backoff max", c.BackoffMax},
	} {
		if f.d <= 0 {
			errs = append(errs, errors.New("outbox "+f.name+" must be greater than zero"))
		}
	}
	if c.BackoffInitial > c.BackoffMax {
		errs = append(errs, errors.New("outbox backoff initial must not exceed backoff max"))
	}
	if c.Lease <= c.PublishTimeout+2*c.SQLTimeout+MinLeaseMargin {
		errs = append(errs, errors.New("outbox lease must exceed publish timeout + 2 x sql timeout by at least 5s"))
	}
	return errors.Join(errs...)
}

// MinLeaseMargin é a folga mínima da lease sobre reservar+publicar+confirmar: o token e o relógio do banco é que
// protegem o estado, mas uma lease apertada demais só produziria republicações.
const MinLeaseMargin = 5 * time.Second

// Hooks são pontos de observação opcionais (métricas), chamados depois da decisão/commit correspondente.
type Hooks struct {
	Published func(d time.Duration) // publicação confirmada (duração da tentativa: reserva até a confirmação)
	Retry     func(reason string)   // falha de publicação agendada para nova tentativa; reason de um conjunto limitado
	LeaseLost func()                // a posse se perdeu antes da escrita final (o evento segue recuperável)
}

// Publisher é o componente (lifecycle.Component) que executa os executores.
type Publisher struct {
	store Store
	sink  Sink
	cfg   Config
	log   *slog.Logger

	runCtx context.Context
	cancel context.CancelFunc
	stop   chan struct{}
	done   chan struct{}

	startOnce, stopOnce sync.Once
	wg                  sync.WaitGroup
	deadline            atomic.Pointer[time.Time]
	rand                func(n int64) int64 // injetável nos testes
	hooks               Hooks
}

// New cria o componente. Nada roda antes de Start.
func New(store Store, sink Sink, cfg Config, log *slog.Logger) *Publisher {
	p := &Publisher{store: store, sink: sink, cfg: cfg, log: log, stop: make(chan struct{}), done: make(chan struct{}), rand: rand.Int64N}
	p.runCtx, p.cancel = context.WithCancel(context.Background())
	return p
}

// WithHooks registra os pontos de observação (antes de Start).
func (p *Publisher) WithHooks(h Hooks) *Publisher { p.hooks = h; return p }

func (p *Publisher) Name() string { return "outbox-publisher" }

// Start dispara os executores. A vida deles não depende do contexto de OnStart.
func (p *Publisher) Start(context.Context) error {
	p.startOnce.Do(func() {
		for i := 0; i < p.cfg.Workers; i++ {
			p.wg.Add(1)
			go p.run(i)
		}
		go func() { p.wg.Wait(); close(p.done) }()
	})
	return nil
}

// StopAdmission impede novas reservas; publicações e confirmações em andamento terminam.
func (p *Publisher) StopAdmission() { p.stopOnce.Do(func() { close(p.stop) }) }

// Cancel aborta as publicações em andamento (a reserva é liberada, não confirmada).
func (p *Publisher) Cancel() { p.cancel() }

// Done fecha quando todos os executores terminaram, incluindo a ação SQL final de cada um.
func (p *Publisher) Done() <-chan struct{} { return p.done }

// SetShutdownDeadline informa o prazo global do encerramento: as ações SQL finais nunca o ultrapassam.
func (p *Publisher) SetShutdownDeadline(d time.Time) {
	p.deadline.Store(&d)
	// O repositório também limita ao prazo global o rollback de uma operação que não terminou.
	if s, ok := p.store.(interface{ SetShutdownDeadline(time.Time) }); ok {
		s.SetShutdownDeadline(d)
	}
}

func (p *Publisher) run(worker int) {
	defer p.wg.Done()
	for {
		select {
		case <-p.stop:
			return
		default:
		}
		if p.Step() {
			continue // havia trabalho: procura o próximo sem esperar
		}
		select {
		case <-p.stop:
			return
		case <-time.After(p.cfg.PollInterval):
		}
	}
}

// Step executa um ciclo completo (reservar, publicar, confirmar/retry/liberar) e devolve se havia um evento. É exportado
// para os testes controlarem o ritmo; o componente o chama em laço.
func (p *Publisher) Step() bool {
	began := time.Now()
	rctx, cancel := context.WithTimeout(p.runCtx, p.cfg.SQLTimeout)
	res, err := p.store.Reserve(rctx, p.cfg.Lease)
	cancel()
	if err != nil {
		if p.runCtx.Err() == nil {
			p.log.Warn("outbox reserve failed", "error", sanitize(err.Error()))
		}
		return false
	}
	if res == nil {
		return false
	}

	// Nenhuma transação SQL está aberta aqui: só a rede.
	pctx, cancel := context.WithTimeout(p.runCtx, p.cfg.PublishTimeout)
	perr := p.publish(pctx, *res)
	cancel()

	// A ação final usa um contexto próprio (o cancelamento do shutdown não deve impedir confirmar um envio já aceito nem
	// liberar a reserva), limitado ao SQLTimeout E ao prazo global restante do encerramento. Sem orçamento, nenhum SQL novo
	// começa (nem com um prazo artificial): a lease vence e o evento é recuperado.
	deadline, ok := p.finalDeadline()
	if !ok {
		p.log.Warn("outbox shutdown budget exhausted; no final SQL action was started and the lease expiry recovers the event",
			"eventId", res.EventID, "published", perr == nil)
		return true
	}
	fctx, cancel := context.WithDeadline(context.WithoutCancel(p.runCtx), deadline)
	defer cancel()
	switch {
	case perr == nil:
		if err := p.store.Confirm(fctx, *res); err != nil {
			p.logFinish("confirm", *res, err) // enviado e não confirmado: a lease vence e o evento é republicado (mesmo eventId)
		} else if p.hooks.Published != nil {
			p.hooks.Published(time.Since(began))
		}
	case p.runCtx.Err() != nil:
		// Encerramento: não é uma falha do evento. Libera a reserva (se ainda for nossa); a expiração garante a recuperação.
		if err := p.store.Release(fctx, *res); err != nil {
			p.logFinish("release", *res, err)
		}
	default:
		delay := p.backoff(res.Attempts)
		p.log.Warn("outbox publish failed; scheduled retry", "eventId", res.EventID, "type", res.Type, "attempts", res.Attempts,
			"retryIn", delay.Round(time.Millisecond).String(), "error", sanitize(perr.Error()))
		if p.hooks.Retry != nil {
			p.hooks.Retry(publishFailureReason(perr))
		}
		if err := p.store.Retry(fctx, *res, delay, sanitize(perr.Error())); err != nil {
			p.logFinish("retry", *res, err)
		}
	}
	return true
}

func (p *Publisher) publish(ctx context.Context, r Reservation) error {
	if strings.TrimSpace(r.WalletID) == "" {
		return ErrInvalidEvent
	}
	return p.sink.Publish(ctx, Message{GroupID: r.WalletID, DedupID: r.EventID.String(), Type: r.Type, Body: r.Body})
}

func (p *Publisher) logFinish(action string, r Reservation, err error) {
	if errors.Is(err, ErrLeaseLost) {
		if p.hooks.LeaseLost != nil {
			p.hooks.LeaseLost()
		}
		p.log.Warn("outbox lease lost before "+action+"; the event stays recoverable", "eventId", r.EventID)
		return
	}
	p.log.Error("outbox "+action+" failed; the lease expires and the event is recovered", "eventId", r.EventID, "error", sanitize(err.Error()))
}

// finalDeadline é o INSTANTE ABSOLUTO limite da ação SQL final: o menor entre agora + SQLTimeout e o prazo global informado pelo
// coordenador. O prazo global não é convertido em duração para recomeçar depois (isso o deslocaria para frente pelo intervalo entre
// medir e criar o contexto); nunca amplia o SQLTimeout e nunca passa do prazo global. Devolve false se o prazo global já acabou.
func (p *Publisher) finalDeadline() (time.Time, bool) {
	now := time.Now()
	deadline := now.Add(p.cfg.SQLTimeout)
	if g := p.deadline.Load(); g != nil {
		if !g.After(now) {
			return time.Time{}, false
		}
		if g.Before(deadline) {
			deadline = *g
		}
	}
	return deadline, true
}

// backoff devolve o intervalo antes de nova tentativa: exponencial a partir de BackoffInitial, jitter uniforme em [d/2, d] e
// teto BackoffMax. attempts é o número de reservas do evento; o cálculo não cresce sem limite (o laço pára no teto).
func (p *Publisher) backoff(attempts int) time.Duration {
	d := p.cfg.BackoffInitial
	for i := 1; i < attempts && d < p.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > p.cfg.BackoffMax || d <= 0 {
		d = p.cfg.BackoffMax
	}
	half := d / 2
	return half + time.Duration(p.rand(int64(d-half)+1))
}

const maxErrorLen = 300

// sanitize produz um diagnóstico curto e sem caracteres de controle. Nunca recebe payload nem credenciais: só o texto do erro.
func sanitize(s string) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.TrimSpace(s)
	if len(s) > maxErrorLen {
		s = strings.ToValidUTF8(s[:maxErrorLen], "") + "…"
	}
	return s
}

// publishFailureReason classifica uma falha de publicação num conjunto limitado de motivos.
func publishFailureReason(err error) string {
	switch {
	case errors.Is(err, ErrInvalidEvent):
		return "invalid_event"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	}
	return "broker"
}
