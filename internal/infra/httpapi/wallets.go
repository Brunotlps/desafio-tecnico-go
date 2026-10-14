package httpapi

import (
	"net/http"

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
