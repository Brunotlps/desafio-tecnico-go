package httpapi

import (
	"context"
	"net/http"
	"sync"
	"time"
)

const readinessTimeout = 2 * time.Second

// live: o processo está de pé. Não consulta dependências.
func (h *Handler) live(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready consulta as dependências reais (PostgreSQL e SQS) em paralelo. O corpo
// público indica apenas ok/down por componente; o motivo vai para o log.
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	results := make(map[string]string, len(h.checks))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, c := range h.checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status := "ok"
			if err := c.Check(ctx); err != nil {
				status = "down"
				h.log.WarnContext(ctx, "readiness check failed", "dependency", c.Name, "error", err.Error())
			}
			mu.Lock()
			results[c.Name] = status
			mu.Unlock()
		}()
	}
	wg.Wait()

	overall, code := "ok", http.StatusOK
	for _, s := range results {
		if s != "ok" {
			overall, code = "unavailable", http.StatusServiceUnavailable
		}
	}
	writeJSON(w, code, map[string]any{"status": overall, "checks": results})
}
