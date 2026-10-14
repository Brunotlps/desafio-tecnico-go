package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Server encapsula o http.Server com início e encerramento observáveis.
type Server struct {
	srv  *http.Server
	log  *slog.Logger
	ln   net.Listener
	done chan struct{}
}

// NewServer cria o servidor com timeouts de conexão explícitos.
func NewServer(addr string, handler http.Handler, log *slog.Logger) *Server {
	return &Server{
		srv: &http.Server{
			Addr: addr, Handler: handler,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       15 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    16 << 10,
		},
		log: log,
	}
}

// Start abre a porta de forma síncrona (falhas de bind abortam a
// inicialização) e atende em segundo plano.
func (s *Server) Start(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.srv.Addr, err)
	}
	s.ln, s.done = ln, make(chan struct{})
	go func() {
		defer close(s.done)
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server stopped unexpectedly", "error", err.Error())
		}
	}()
	s.log.Info("http server listening", "addr", ln.Addr().String())
	return nil
}

// Addr devolve o endereço efetivo (útil com porta :0 em testes).
func (s *Server) Addr() string {
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

// Stop deixa de aceitar conexões e aguarda as requisições em andamento até o
// prazo de ctx; ao expirar, as conexões restantes são fechadas.
func (s *Server) Stop(ctx context.Context) error {
	err := s.srv.Shutdown(ctx)
	if err != nil {
		_ = s.srv.Close()
	}
	if s.done != nil {
		<-s.done
	}
	if err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}
