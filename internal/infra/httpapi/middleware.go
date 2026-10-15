package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/infra/auth"
)

type ctxKey int

const (
	correlationKey ctxKey = 1
	requestInfoKey ctxKey = 2
)

// requestInfo é preenchido pelo middleware de autenticação para que o log de
// acesso (mais externo) conheça a identidade autenticada.
type requestInfo struct{ clientID, providerID string }

var correlationRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)

func correlationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey).(string)
	return id
}

// statusRecorder captura o status para o log de acesso.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// observe aplica, nesta ordem: correlationId, timeout, recuperação de panic e
// log de acesso (sem corpos nem credenciais).
func (h *Handler) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corr := r.Header.Get("X-Correlation-Id")
		if !correlationRe.MatchString(corr) {
			corr = uuid.NewString()
		}
		w.Header().Set("X-Correlation-Id", corr)
		info := &requestInfo{}
		ctx := context.WithValue(context.WithValue(r.Context(), correlationKey, corr), requestInfoKey, info)
		ctx, cancel := context.WithTimeout(ctx, h.requestTimeout)
		defer cancel()
		r = r.WithContext(ctx)

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		defer func() {
			if v := recover(); v != nil {
				h.log.ErrorContext(ctx, "panic in handler", "panic", v, "correlationId", corr)
				if rec.status == http.StatusOK {
					writeProblem(rec, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
				}
			}
			if h.metrics != nil && r.Pattern == "POST /wagering/transactions" {
				h.metrics.Duration("http_submit", time.Since(start))
			}
			attrs := []any{"method", r.Method, "route", r.Pattern, "status", rec.status,
				"durationMs", time.Since(start).Milliseconds(), "correlationId", corr}
			if info.clientID != "" {
				attrs = append(attrs, "clientId", info.clientID)
			}
			if info.providerID != "" {
				attrs = append(attrs, "providerId", info.providerID)
			}
			h.log.Log(ctx, accessLevel(r.URL.Path), "http request", attrs...)
		}()
		next.ServeHTTP(rec, r)
	})
}

func accessLevel(path string) slog.Level {
	if strings.HasPrefix(path, "/health/") {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

// authenticate exige um Bearer token válido e anexa o Principal ao contexto.
// Todas as rotas de negócio passam por aqui.
func (h *Handler) authenticate(next func(http.ResponseWriter, *http.Request, auth.Principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
			w.Header().Set("WWW-Authenticate", `Bearer`)
			writeProblem(w, http.StatusUnauthorized, "UNAUTHENTICATED", "a Bearer access token is required")
			return
		}
		p, err := h.verifier.Verify(strings.TrimSpace(token))
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
			writeProblem(w, http.StatusUnauthorized, "INVALID_TOKEN", "the access token is invalid or expired")
			return
		}
		if info, ok := r.Context().Value(requestInfoKey).(*requestInfo); ok {
			info.clientID, info.providerID = p.ClientID, p.ProviderID
		}
		next(w, r.WithContext(auth.WithPrincipal(r.Context(), p)), p)
	}
}

func forbidden(w http.ResponseWriter, msg string) {
	writeProblem(w, http.StatusForbidden, "FORBIDDEN", msg)
}
