// Package lifecycle coordena o encerramento de todos os componentes que admitem
// trabalho (servidor HTTP, workers) dentro de um orçamento global, antes de as
// dependências compartilhadas (pool de conexões) serem fechadas.
//
// Sequência de Stop (um único prazo global, vindo do contexto do Fx):
//
//  1. admissão: todos os componentes deixam de aceitar novas entradas e de buscar
//     trabalho, ao mesmo tempo;
//  2. drenagem: o trabalho admitido segue, simultaneamente, até o limite de drenagem;
//  3. cancelamento: no limite de drenagem, o que ainda roda é cancelado;
//  4. confirmação: espera-se o término observável (Done) de cada componente até o
//     prazo global menos uma margem para fechar as dependências;
//  5. só então o Fx fecha as dependências (hooks anteriores, em ordem inversa). Se algum
//     componente não confirmou, o shutdown é registrado como incompleto — o estado
//     durável (banco) é quem garante a recuperação.
//
// A reserva (prazo global − drenagem) não é concedida de novo a cada componente: ela é
// compartilhada por cancelamento, cleanup e dependências.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Component é algo que admite trabalho e precisa terminar de forma observável.
type Component interface {
	Name() string
	// Start inicia o componente. O contexto limita só a inicialização; o componente
	// não pode atrelar sua vida a ele. Se falhar, libera o que adquiriu.
	Start(ctx context.Context) error
	// StopAdmission fecha novas entradas e a busca de trabalho. Idempotente; não bloqueia.
	StopAdmission()
	// Cancel aborta o trabalho em andamento. Idempotente; não bloqueia.
	Cancel()
	// Done é fechado quando todo o trabalho terminou (handlers, goroutines, ações
	// finais). O fim de um laço Serve, sozinho, não prova isso.
	Done() <-chan struct{}
}

// Coordinator registra componentes e conduz seu início e encerramento.
type Coordinator struct {
	log    *slog.Logger
	drain  time.Duration
	margin time.Duration

	mu         sync.Mutex
	components []Component
	started    []Component
	observers  []func(deadline time.Time)
}

// New cria o coordenador. drain é o limite de drenagem contado desde o início do Stop.
func New(log *slog.Logger, drain time.Duration) *Coordinator {
	return &Coordinator{log: log, drain: drain, margin: time.Second}
}

// Add registra um componente (a ordem de registro é a ordem de início).
func (c *Coordinator) Add(comp Component) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.components = append(c.components, comp)
}

// OnShutdown registra um observador do prazo global, chamado no início do Stop
// (p.ex. para limitar o rollback de conexões ao orçamento restante).
func (c *Coordinator) OnShutdown(f func(deadline time.Time)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observers = append(c.observers, f)
}

// Start inicia os componentes na ordem de registro. Se um falhar, os já iniciados
// são encerrados aqui mesmo (o Fx não chama o OnStop de um hook cujo OnStart falhou).
func (c *Coordinator) Start(ctx context.Context) error {
	c.mu.Lock()
	comps := append([]Component(nil), c.components...)
	c.mu.Unlock()
	for _, comp := range comps {
		if err := comp.Start(ctx); err != nil {
			stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.drain+2*time.Second)
			stopErr := c.Stop(stopCtx)
			cancel()
			return errors.Join(fmt.Errorf("start %s: %w", comp.Name(), err), stopErr)
		}
		c.mu.Lock()
		c.started = append(c.started, comp)
		c.mu.Unlock()
	}
	return nil
}

// Stop executa a sequência de encerramento. O prazo global é o do contexto. Devolve
// erro se algum componente não confirmou o término a tempo.
func (c *Coordinator) Stop(ctx context.Context) error {
	c.mu.Lock()
	comps := c.started
	c.started = nil
	observers := append([]func(deadline time.Time){}, c.observers...)
	c.mu.Unlock()
	if len(comps) == 0 {
		return nil
	}
	begin := time.Now()
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = begin.Add(c.drain + 8*time.Second)
	}
	for _, f := range observers {
		f(deadline)
	}

	// 1. admissão
	for _, comp := range comps {
		comp.StopAdmission()
	}
	// 2. drenagem simultânea, no máximo até o limite de drenagem
	drainDeadline := begin.Add(c.drain)
	if drainDeadline.After(deadline) {
		drainDeadline = deadline
	}
	pending := waitAll(comps, drainDeadline)
	if len(pending) == 0 {
		c.log.Info("shutdown drained", "elapsed", time.Since(begin).Round(time.Millisecond).String())
		return nil
	}
	// 3. cancelamento do que restou
	c.log.Warn("shutdown drain limit reached; cancelling remaining work", "components", names(pending),
		"elapsed", time.Since(begin).Round(time.Millisecond).String())
	for _, comp := range pending {
		comp.Cancel()
	}
	// 4. confirmação de término, deixando uma margem para fechar as dependências
	cancelDeadline := deadline.Add(-c.margin)
	if cancelDeadline.Before(time.Now()) {
		cancelDeadline = time.Now()
	}
	if left := waitAll(pending, cancelDeadline); len(left) > 0 {
		c.log.Error("shutdown incomplete: components did not confirm termination; durable state governs recovery",
			"components", names(left), "elapsed", time.Since(begin).Round(time.Millisecond).String())
		return fmt.Errorf("shutdown incomplete: %v still running", names(left))
	}
	c.log.Info("shutdown completed after cancellation", "elapsed", time.Since(begin).Round(time.Millisecond).String())
	return nil
}

// waitAll espera, em paralelo, o Done de cada componente até o prazo e devolve os
// que ainda não terminaram.
func waitAll(comps []Component, deadline time.Time) []Component {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	var pending []Component
	for _, comp := range comps {
		select {
		case <-comp.Done():
		default:
			pending = append(pending, comp)
		}
	}
	var still []Component
	expired := false
	for _, comp := range pending {
		if expired {
			select {
			case <-comp.Done():
			default:
				still = append(still, comp)
			}
			continue
		}
		select {
		case <-comp.Done():
		case <-timer.C:
			expired = true
			select {
			case <-comp.Done():
			default:
				still = append(still, comp)
			}
		}
	}
	return still
}

func names(comps []Component) []string {
	out := make([]string, len(comps))
	for i, c := range comps {
		out[i] = c.Name()
	}
	return out
}
