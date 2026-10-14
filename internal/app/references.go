package app

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"desafio-tecnico-go/internal/domain"
)

// Retomada das operações em PENDING_REFERENCE.
//
// Cada tentativa é uma transação SQL curta (fase financeira limitada por
// PhaseTimeout, incluindo locks e commit) que recarrega e REVALIDA a operação
// original — mesma identidade, hash, chave e agenda — e aplica as mesmas regras
// financeiras do processamento inicial (Service.decide). Selecionar candidatos é só
// uma leitura: não autoriza movimento. Nenhuma transação ou lock é mantido durante o
// backoff, que é apenas a data persistida da próxima tentativa.
//
// Ordem única de locks (HTTP, retomada e, futuramente, SQS): carteira → agenda da
// pendência (e a linha da transação, atualizada por quem a trava). A retomada trava a
// carteira com SKIP LOCKED: uma carteira ocupada é pulada na varredura (outras
// carteiras seguem) e não cria espera na ordem inversa à da entrada HTTP.

type resumeStatus int

const (
	resumeSkipped   resumeStatus = iota // já concluída por outra instância, ou carteira ocupada
	resumeWaiting                       // continua esperando; nova data agendada
	resumeCompleted                     // terminou (PROCESSED ou REJECTED)
)

// backoff devolve o intervalo antes da próxima verificação: exponencial a partir
// de BackoffInitial, com jitter uniforme em [d/2, d] e teto BackoffMax.
func (s *Service) backoff(attempts int) time.Duration {
	d := s.ref.BackoffInitial
	for i := 0; i < attempts && d < s.ref.BackoffMax; i++ {
		d *= 2
	}
	if d > s.ref.BackoffMax || d <= 0 {
		d = s.ref.BackoffMax
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}

// ProcessPending executa uma tentativa para uma pendência (a leitura de ref só
// identifica o candidato). Devolve o desfecho para diagnóstico.
func (s *Service) ProcessPending(ctx context.Context, ref PendingReference) (completed bool, err error) {
	for attempt := 0; ; attempt++ {
		var status resumeStatus
		status, err = s.resumeOnce(ctx, ref)
		if errors.Is(err, ErrReversalTargetTaken) && attempt < maxConflictRetries {
			continue // o banco revelou uma reversão concorrente; uma nova tentativa a enxerga e rejeita
		}
		return status == resumeCompleted, err
	}
}

func (s *Service) resumeOnce(ctx context.Context, ref PendingReference) (resumeStatus, error) {
	status := resumeSkipped
	var done *domain.WagerTransaction
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		status, done = resumeSkipped, nil
		// 1. Carteira (sem esperar), 2. agenda aberta, 3. a transação, revalidada.
		wallet, err := r.Wallets.TryLockForUpdate(ctx, ref.WalletID)
		if err != nil {
			return err
		}
		sched, err := r.Pending.GetOpenForUpdate(ctx, ref.TransactionID)
		if errors.Is(err, ErrPendingNotFound) {
			return nil // outra instância já concluiu
		}
		if err != nil {
			return err
		}
		tx, err := r.Transactions.GetByID(ctx, ref.TransactionID)
		if err != nil {
			return err
		}
		if tx.Status() != domain.StatusPendingReference || tx.WalletID() != wallet.ID() {
			return nil // estado terminal: nada a fazer (o banco impede agenda aberta com terminal)
		}
		// O relógio desta instância pode estar atrás da última atualização gravada por
		// outra: usa-se o maior dos dois (o domínio nunca ajusta o instante sozinho).
		now := s.now()
		if tx.UpdatedAt().After(now) {
			now = tx.UpdatedAt()
		}
		expired := !now.Before(sched.ExpiresAt)
		versionBefore := wallet.Version()
		out, err := s.decide(ctx, r, wallet, tx, "pending-reference-"+tx.ID().String(), now, expired)
		if err != nil {
			return err
		}
		if out.wait {
			status = resumeWaiting
			return r.Pending.Reschedule(ctx, tx.ID(), now.Add(s.backoff(sched.Attempts)), now)
		}
		if out.entry != nil {
			if err := r.Wallets.UpdateBalance(ctx, wallet, versionBefore); err != nil {
				return err
			}
		}
		if err := r.Transactions.Update(ctx, tx, domain.StatusPendingReference); err != nil {
			return err
		}
		if out.entry != nil {
			if err := r.Ledger.Append(ctx, *out.entry); err != nil {
				return err
			}
		}
		if err := r.Outbox.Append(ctx, out.events); err != nil {
			return err
		}
		if err := r.Pending.Complete(ctx, tx.ID(), now); err != nil {
			return err
		}
		status, done = resumeCompleted, tx
		return nil
	})
	switch {
	case errors.Is(err, ErrWalletBusy):
		return resumeSkipped, nil
	case err != nil:
		if errors.Is(err, ErrCommitUnknown) {
			s.log.WarnContext(ctx, "pending reference commit outcome unknown; the durable state decides on the next attempt",
				"transactionId", ref.TransactionID, "walletId", ref.WalletID)
		}
		return resumeSkipped, err
	}
	if done != nil {
		s.log.InfoContext(ctx, "pending reference resolved",
			"transactionId", done.ID(), "walletId", done.WalletID(), "providerId", done.External().ProviderID,
			"kind", done.Kind(), "status", done.Status(), "failureCode", done.FailureCode())
	}
	return status, nil
}

// ProcessDue varre as pendências vencidas (leitura sem locks) e tenta cada uma em
// sequência, parando assim que stop for fechado. Devolve quantas terminaram e se
// alguma tentativa falhou (para o worker espaçar a próxima varredura).
func (s *Service) ProcessDue(ctx context.Context, stop <-chan struct{}) (completed int, failed bool) {
	due, err := s.uow.Reader().Pending.Due(ctx, s.now(), s.ref.BatchSize)
	if err != nil {
		s.log.WarnContext(ctx, "listing due pending references failed", "error", err.Error())
		return 0, true
	}
	for _, p := range due {
		select {
		case <-stop:
			return completed, failed
		case <-ctx.Done():
			return completed, failed
		default:
		}
		attemptCtx, cancel := context.WithTimeout(ctx, s.ref.PhaseTimeout)
		done, err := s.ProcessPending(attemptCtx, p)
		cancel()
		switch {
		case err != nil:
			failed = true
			s.log.WarnContext(ctx, "pending reference attempt aborted; it will be retried from the durable state",
				"transactionId", p.TransactionID, "walletId", p.WalletID, "error", err.Error())
		case done:
			completed++
		}
	}
	return completed, failed
}

// ReferenceWorker é o executor de referências de um processo. Cumpre o contrato de
// componente do coordenador de shutdown (internal/lifecycle): admissão = busca de
// trabalho; Cancel aborta a tentativa em curso; Done confirma o término.
type ReferenceWorker struct {
	svc *Service
	log *slog.Logger

	mu      sync.Mutex
	started bool
	stop    chan struct{} // fechado: não buscar mais trabalho
	cancel  context.CancelFunc
	done    chan struct{}
	once    sync.Once
}

// NewReferenceWorker cria o worker (sem iniciá-lo).
func NewReferenceWorker(svc *Service, log *slog.Logger) *ReferenceWorker {
	return &ReferenceWorker{svc: svc, log: log, stop: make(chan struct{}), done: make(chan struct{})}
}

func (w *ReferenceWorker) Name() string { return "reference-worker" }

// Start inicia o laço em segundo plano. O contexto de OnStart só limita a
// inicialização; a vida do worker tem contexto próprio, cancelado por Cancel.
func (w *ReferenceWorker) Start(context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started {
		return errors.New("reference worker already started")
	}
	w.started = true
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	go w.run(ctx)
	return nil
}

func (w *ReferenceWorker) run(ctx context.Context) {
	defer close(w.done)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		_, _ = w.svc.ProcessDue(ctx, w.stop)
		timer.Reset(w.svc.ref.PollInterval)
	}
}

// StopAdmission impede novas buscas; a tentativa em andamento segue até o fim.
func (w *ReferenceWorker) StopAdmission() { w.once.Do(func() { close(w.stop) }) }

// Cancel aborta a tentativa em curso (a transação é desfeita; o estado durável decide).
func (w *ReferenceWorker) Cancel() {
	w.StopAdmission()
	w.mu.Lock()
	cancel := w.cancel
	w.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Done é fechado quando o laço terminou e nenhuma tentativa está em curso. Sem Start,
// já está fechado (nada a esperar).
func (w *ReferenceWorker) Done() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.started {
		select {
		case <-w.done:
		default:
			c := make(chan struct{})
			close(c)
			return c
		}
	}
	return w.done
}
