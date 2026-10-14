package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

// errorBody é o corpo de todas as respostas de erro. code é estável e
// destinado a máquinas; message é informativa.
type errorBody struct {
	Code     string `json:"code"`
	Message  string `json:"message"`
	WalletID string `json:"walletId,omitempty"` // WALLET_ALREADY_EXISTS
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeProblem(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Code: code, Message: message})
}

type mapping struct {
	target error
	status int
	code   string
}

// Ordem importa: o primeiro alvo que casar vence.
var errorMappings = []mapping{
	{app.ErrWalletNotFound, http.StatusNotFound, "WALLET_NOT_FOUND"},
	{app.ErrTransactionNotFound, http.StatusNotFound, "TRANSACTION_NOT_FOUND"},
	{app.ErrIdempotencyKeyReused, http.StatusConflict, "IDEMPOTENCY_KEY_REUSED"},
	{app.ErrExternalTransactionReused, http.StatusConflict, "EXTERNAL_TRANSACTION_ALREADY_REGISTERED"},
	{app.ErrWalletMismatch, http.StatusUnprocessableEntity, "WALLET_MISMATCH"},
	{app.ErrCurrencyNotSupported, http.StatusUnprocessableEntity, "CURRENCY_NOT_SUPPORTED"},
	{domain.ErrKindNotAllowed, http.StatusUnprocessableEntity, "KIND_NOT_ALLOWED"},
	{app.ErrKindNotSupported, http.StatusNotImplemented, "KIND_NOT_SUPPORTED"},
	{domain.ErrInvalidAmount, http.StatusBadRequest, "INVALID_AMOUNT"},
	{domain.ErrAmountScale, http.StatusBadRequest, "INVALID_AMOUNT"},
	{domain.ErrAmountNegative, http.StatusBadRequest, "INVALID_AMOUNT"},
	{domain.ErrAmountOverflow, http.StatusBadRequest, "INVALID_AMOUNT"},
	{domain.ErrAmountNotAllowed, http.StatusBadRequest, "AMOUNT_NOT_ALLOWED_FOR_KIND"},
	{domain.ErrInvalidCurrency, http.StatusBadRequest, "INVALID_CURRENCY"},
	{domain.ErrInvalidKind, http.StatusBadRequest, "INVALID_KIND"},
	{domain.ErrInvalidRequest, http.StatusBadRequest, "INVALID_REQUEST"},
	{domain.ErrUninitializedMoney, http.StatusBadRequest, "INVALID_REQUEST"},
	{app.ErrCommitUnknown, http.StatusServiceUnavailable, "COMMIT_OUTCOME_UNKNOWN"},
	{app.ErrTransient, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE"},
	// Não ocorre com o lock da carteira; se ocorrer, nada foi gravado e repetir é seguro.
	{app.ErrConcurrentModification, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE"},
}

// writeError traduz um erro da aplicação para o contrato HTTP.
//
//	400 entrada inválida          404 recurso inexistente
//	409 conflito de idempotência  422 entrada válida que não se aplica (carteira divergente, moeda)
//	501 tipo ainda não suportado  503 indisponibilidade transitória ou commit incerto
//	500 falha interna inesperada
func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	var exists *app.WalletExistsError
	if errors.As(err, &exists) {
		writeJSON(w, http.StatusConflict, errorBody{
			Code: "WALLET_ALREADY_EXISTS", Message: "a wallet already exists for this player and currency",
			WalletID: exists.WalletID.String(),
		})
		return
	}
	for _, m := range errorMappings {
		if errors.Is(err, m.target) {
			if m.status == http.StatusServiceUnavailable {
				w.Header().Set("Retry-After", "1")
				h.log.WarnContext(r.Context(), "transient failure", "code", m.code, "error", err.Error(), "correlationId", correlationID(r.Context()))
				msg := "temporary failure; retry with the same Idempotency-Key"
				if m.code == "COMMIT_OUTCOME_UNKNOWN" {
					msg = "the outcome of the operation is unknown; repeat the same request with the same Idempotency-Key to obtain the persisted result"
				}
				writeProblem(w, m.status, m.code, msg)
				return
			}
			writeProblem(w, m.status, m.code, publicMessage(err))
			return
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		w.Header().Set("Retry-After", "1")
		writeProblem(w, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE", "request cancelled or timed out; retry with the same Idempotency-Key")
		return
	}
	h.log.ErrorContext(r.Context(), "unexpected error", slog.String("error", err.Error()), "correlationId", correlationID(r.Context()))
	writeProblem(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
}

// publicMessage devolve a mensagem de um erro de domínio ou de aplicação; esses
// textos descrevem a entrada e não contêm dados de infraestrutura.
func publicMessage(err error) string { return err.Error() }
