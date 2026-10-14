package httpapi

import (
	"net/http"

	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/auth"
)

// submit: POST /wagering/transactions.
//
//	200 PROCESSED (original ou replay; idempotentReplay indica qual)
//	202 PENDING / PENDING_REFERENCE: aceito, ainda sem resultado (consulte a transação)
//	422 REJECTED por regra de negócio, com failureCode (original ou replay)
//	400 entrada inválida · 401/403 autenticação/autorização
//	409 conflito de idempotência · 501 tipo não suportado · 503 indisponibilidade
func (h *Handler) submit(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if !p.IsProvider() {
		forbidden(w, "a provider identity is required")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || keys[0] == "" {
		writeProblem(w, http.StatusBadRequest, "IDEMPOTENCY_KEY_REQUIRED", "exactly one non-empty Idempotency-Key header is required")
		return
	}
	var req submitRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "INVALID_JSON", "request body must be a single valid JSON object with string amounts")
		return
	}
	// O provedor autorizado é o do token. O corpo só pode confirmá-lo.
	if req.ProviderID != p.ProviderID {
		forbidden(w, "providerId does not match the authenticated provider")
		return
	}
	if req.Money == nil {
		writeProblem(w, http.StatusBadRequest, "INVALID_REQUEST", "money is required")
		return
	}
	res, err := h.svc.Submit(r.Context(), domain.RawExternalRequest{
		ProviderID: p.ProviderID, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: keys[0],
		PlayerID: req.PlayerID, WalletID: req.WalletID, RoundID: req.RoundID, GameID: req.GameID,
		Kind: req.Kind, Amount: req.Money.Amount, Currency: req.Money.Currency,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
	}, correlationID(r.Context()))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	t := res.Transaction
	body := submitResponse{
		TransactionID: t.ID().String(), Status: string(t.Status()), Balance: toMoneyDTO(t.ResultBalance()),
		FailureCode: string(t.FailureCode()), IdempotentReplay: res.Replay,
	}
	switch t.Status() {
	case domain.StatusProcessed:
		writeJSON(w, http.StatusOK, body)
	case domain.StatusRejected:
		writeJSON(w, http.StatusUnprocessableEntity, body)
	case domain.StatusFailed: // falha permanente de infraestrutura, registrada para auditoria
		writeJSON(w, http.StatusInternalServerError, body)
	default: // PENDING, PENDING_REFERENCE
		writeJSON(w, http.StatusAccepted, body)
	}
}

// getTransaction: GET /wagering/transactions/{transactionId}. Provedores veem
// apenas as próprias transações (404 para as demais, sem distinguir de
// inexistentes); o serviço interno vê todas.
func (h *Handler) getTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	id, err := domain.ParseID("transactionId", r.PathValue("transactionId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var t *domain.WagerTransaction
	switch {
	case p.IsInternal():
		t, err = h.svc.GetTransaction(r.Context(), id)
	case p.IsProvider():
		t, err = h.svc.GetTransactionForProvider(r.Context(), p.ProviderID, id)
	default:
		forbidden(w, "no permission to read transactions")
		return
	}
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}

// getProviderTransaction: GET /providers/{providerId}/wagering/transactions/{externalTransactionId}.
// Um provedor só consulta o próprio providerId (403 caso contrário).
func (h *Handler) getProviderTransaction(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	providerID := r.PathValue("providerId")
	switch {
	case p.IsInternal():
	case p.IsProvider():
		if providerID != p.ProviderID {
			forbidden(w, "providerId does not match the authenticated provider")
			return
		}
	default:
		forbidden(w, "no permission to read transactions")
		return
	}
	t, err := h.svc.GetTransactionByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransactionResponse(t))
}
