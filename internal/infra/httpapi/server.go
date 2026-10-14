package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Server encapsula o http.Server como componente do coordenador de shutdown:
//
//   - StopAdmission fecha o listener e passa a recusar (503) requisições novas, inclusive em
//     conexões keep-alive já abertas;
//   - as requisições admitidas são contadas, e Done só é confirmado quando o http.Server
//     terminou E todos os handlers retornaram (o fim do Serve, sozinho, não prova isso);
//   - Cancel fecha as conexões e cancela o contexto base: handlers e consultas em andamento
//     recebem o cancelamento.
type Server struct {
	srv *http.Server
	log *slog.Logger
	ln  net.Listener

	baseCtx    context.Context
	cancelBase context.CancelFunc

	mu       sync.Mutex
	closing  bool
	started  bool
	inflight sync.WaitGroup

	serveDone chan struct{}
	done      chan struct{}
	admOnce   sync.Once
}

// NewServer cria o servidor com timeouts de conexão explícitos.
func NewServer(addr string, handler http.Handler, log *slog.Logger) *Server {
	s := &Server{log: log, serveDone: make(chan struct{}), done: make(chan struct{})}
	s.baseCtx, s.cancelBase = context.WithCancel(context.Background())
	s.srv = &http.Server{
		Addr: addr, Handler: s.track(handler),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		BaseContext:       func(net.Listener) context.Context { return s.baseCtx },
	}
	return s
}

func (s *Server) Name() string { return "http-server" }

// track conta as requisições admitidas e recusa as que chegam depois do fechamento da admissão.
func (s *Server) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			w.Header().Set("Connection", "close")
			w.Header().Set("Retry-After", "1")
			writeProblem(w, http.StatusServiceUnavailable, "SHUTTING_DOWN", "the server is shutting down; retry with the same Idempotency-Key")
			return
		}
		s.inflight.Add(1)
		s.mu.Unlock()
		defer s.inflight.Done()
		next.ServeHTTP(w, r)
	})
}

// Start abre a porta de forma síncrona (falhas de bind abortam a
// inicialização) e atende em segundo plano.
func (s *Server) Start(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.srv.Addr, err)
	}
	s.mu.Lock()
	s.ln, s.started = ln, true
	s.mu.Unlock()
	go func() {
		defer close(s.serveDone)
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server stopped unexpectedly", "error", err.Error())
		}
	}()
	s.log.Info("http server listening", "addr", ln.Addr().String())
	return nil
}

// Addr devolve o endereço efetivo (útil com porta :0 em testes).
func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// StopAdmission fecha o listener e as conexões ociosas e recusa requisições novas;
// as admitidas seguem. O término é sinalizado por Done.
func (s *Server) StopAdmission() {
	s.admOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		started := s.started
		s.mu.Unlock()
		if !started {
			close(s.done)
			return
		}
		go func() {
			// Shutdown fecha o listener e espera as conexões ficarem ociosas, sem prazo
			// próprio: o prazo é do coordenador, que chama Cancel no limite de drenagem.
			_ = s.srv.Shutdown(context.Background())
			<-s.serveDone
			s.inflight.Wait() // todos os handlers retornaram
			close(s.done)
		}()
	})
}

// Cancel fecha as conexões e cancela o contexto das requisições em andamento.
func (s *Server) Cancel() {
	s.StopAdmission()
	s.cancelBase()
	_ = s.srv.Close()
}

// Done é fechado quando o servidor terminou e nenhum handler está em execução. Sem
// Start, já está fechado.
func (s *Server) Done() <-chan struct{} {
	s.mu.Lock()
	started := s.started
	s.mu.Unlock()
	if !started {
		c := make(chan struct{})
		close(c)
		return c
	}
	return s.done
}
