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

// Handler agrega as rotas HTTP.
type Handler struct {
	svc            Service
	verifier       TokenVerifier
	checks         []ReadinessCheck
	log            *slog.Logger
	requestTimeout time.Duration
}

// New cria o handler raiz.
func New(svc Service, verifier TokenVerifier, checks []ReadinessCheck, log *slog.Logger, requestTimeout time.Duration) http.Handler {
	h := &Handler{svc: svc, verifier: verifier, checks: checks, log: log, requestTimeout: requestTimeout}
	mux := http.NewServeMux()

	// Públicas.
	mux.HandleFunc("GET /health/live", h.live)
	mux.HandleFunc("GET /health/ready", h.ready)

	// Operações de carteira: somente serviço interno.
	mux.HandleFunc("POST /wallets", h.authenticate(h.openWallet))
	mux.HandleFunc("GET /wallets/{walletId}", h.authenticate(h.getWallet))
	mux.HandleFunc("GET /wallets/{walletId}/ledger", h.authenticate(h.getLedger))
	mux.HandleFunc("POST /wallets/{walletId}/reconciliation", h.authenticate(h.reconcile))

	// Operações de provedor.
	mux.HandleFunc("POST /wagering/transactions", h.authenticate(h.submit))
	mux.HandleFunc("GET /wagering/transactions/{transactionId}", h.authenticate(h.getTransaction))
	mux.HandleFunc("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", h.authenticate(h.getProviderTransaction))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeProblem(w, http.StatusNotFound, "NOT_FOUND", "no such route")
	})
	return h.observe(mux)
}
