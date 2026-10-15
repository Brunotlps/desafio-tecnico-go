// Package httpapi expõe o caso de uso por HTTP. A identidade vem sempre de um
// access token validado; nada no corpo ou em headers define o provedor.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/auth"
	"desafio-tecnico-go/internal/metrics"
)

// Service é o contrato do caso de uso usado pelos handlers.
type Service interface {
	OpenWallet(ctx context.Context, cmd app.OpenWalletCommand) (*domain.Wallet, error)
	GetWallet(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	Submit(ctx context.Context, raw domain.RawExternalRequest, correlationID string) (app.SubmitResult, error)
	GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	GetTransactionForProvider(ctx context.Context, providerID string, id uuid.UUID) (*domain.WagerTransaction, error)
	GetTransactionByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	ListLedger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (app.LedgerPage, error)
	Reconcile(ctx context.Context, walletID uuid.UUID) (app.ReconciliationResult, error)
}

// TokenVerifier valida access tokens.
type TokenVerifier interface {
	Verify(raw string) (auth.Principal, error)
}

// ReadinessCheck verifica uma dependência real.
type ReadinessCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

// Option ajusta o handler.
type Option func(*Handler)

// WithMetrics habilita GET /metrics (somente o serviço interno) e a observação da duração de POST /wagering/transactions.
func WithMetrics(reg *metrics.Registry) Option { return func(h *Handler) { h.metrics = reg } }

// Handler agrega as rotas HTTP.
type Handler struct {
	metrics        *metrics.Registry
	svc            Service
	verifier       TokenVerifier
	checks         []ReadinessCheck
	log            *slog.Logger
	requestTimeout time.Duration
}

// New cria o handler raiz.
func New(svc Service, verifier TokenVerifier, checks []ReadinessCheck, log *slog.Logger, requestTimeout time.Duration, opts ...Option) http.Handler {
	h := &Handler{svc: svc, verifier: verifier, checks: checks, log: log, requestTimeout: requestTimeout}
	for _, o := range opts {
		o(h)
	}
	mux := http.NewServeMux()

	// Públicas.
	mux.HandleFunc("GET /health/live", h.live)
	mux.HandleFunc("GET /health/ready", h.ready)

	// Operações de carteira: somente serviço interno.
	mux.HandleFunc("POST /wallets", h.authenticate(h.openWallet))
	mux.HandleFunc("GET /wallets/{walletId}", h.authenticate(h.getWallet))
	mux.HandleFunc("GET /wallets/{walletId}/ledger", h.authenticate(h.getLedger))
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", h.authenticate(h.reconcile))

	// Métricas: somente o serviço interno (a saúde segue pública).
	if h.metrics != nil {
		mux.HandleFunc("GET /metrics", h.authenticate(h.metricsHandler))
	}

	// Operações de provedor.
	mux.HandleFunc("POST /wagering/transactions", h.authenticate(h.submit))
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", h.authenticate(h.getTransaction))
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", h.authenticate(h.getProviderTransaction))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, http.StatusNotFound, "NOT_FOUND", "no such route")
	})
	return h.observe(mux)
}

func (h *Handler) metricsHandler(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !p.IsInternal() {
		forbidden(w, "metrics are restricted to the internal service")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if err := h.metrics.Render(r.Context(), w); err != nil {
		h.log.WarnContext(r.Context(), "writing the metrics failed", "error", err.Error())
	}
}
