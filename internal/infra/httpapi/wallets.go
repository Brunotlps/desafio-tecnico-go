package httpapi

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/auth"
)

// openWallet: POST /wallets. 201 em sucesso; 409 WALLET_ALREADY_EXISTS (com o
// walletId existente) para jogador e moeda repetidos.
func (h *Handler) openWallet(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !p.IsInternal() {
		forbidden(w, "wallet operations are restricted to the internal service")
		return
	}
	var req openWalletRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "INVALID_JSON", "request body must be a single valid JSON object with string amounts")
		return
	}
	if req.InitialBalance == nil {
		writeProblem(w, http.StatusBadRequest, "INVALID_REQUEST", "initialBalance is required")
		return
	}
	wallet, err := h.svc.OpenWallet(r.Context(), app.OpenWalletCommand{
		PlayerID: req.PlayerID, Amount: req.InitialBalance.Amount, Currency: req.InitialBalance.Currency,
		CorrelationID: correlationID(r.Context()),
	})
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toWalletResponse(wallet))
}

// getWallet: GET /wallets/{walletId}, somente serviço interno.
func (h *Handler) getWallet(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !p.IsInternal() {
		forbidden(w, "wallet operations are restricted to the internal service")
		return
	}
	id, err := domain.ParseID("walletId", r.PathValue("walletId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	wallet, err := h.svc.GetWallet(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWalletResponse(wallet))
}

var limitRe = regexp.MustCompile(`^[0-9]{1,4}$`)

// getLedger: GET /wallets/{walletId}/ledger?cursor=&limit=, somente serviço interno. limit padrão 50 (1–100); cursor opaco.
// Parâmetros repetidos ou vazios são entrada inválida (nunca ampliam a consulta).
func (h *Handler) getLedger(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !p.IsInternal() {
		forbidden(w, "wallet operations are restricted to the internal service")
		return
	}
	id, err := domain.ParseID("walletId", r.PathValue("walletId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	q := r.URL.Query()
	limit := app.DefaultLedgerLimit
	if vs, ok := q["limit"]; ok {
		if len(vs) != 1 || !limitRe.MatchString(vs[0]) {
			h.writeError(w, r, fmt.Errorf("%w: limit must be an integer", app.ErrInvalidLimit))
			return
		}
		limit, _ = strconv.Atoi(vs[0])
	}
	cursor := ""
	if vs, ok := q["cursor"]; ok {
		if len(vs) != 1 || vs[0] == "" {
			h.writeError(w, r, fmt.Errorf("%w: cursor must appear once and not be empty", app.ErrInvalidCursor))
			return
		}
		cursor = vs[0]
	}
	page, err := h.svc.ListLedger(r.Context(), id, cursor, limit)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toLedgerResponse(page))
}

// reconcile: POST /wallets/{walletId}/reconciliation, somente serviço interno. Lê; nunca altera saldo nem ledger.
func (h *Handler) reconcile(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !p.IsInternal() {
		forbidden(w, "wallet operations are restricted to the internal service")
		return
	}
	id, err := domain.ParseID("walletId", r.PathValue("walletId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	res, err := h.svc.Reconcile(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toReconciliationResponse(res))
}
