package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"desafio-tecnico-go/internal/domain"
)

const maxBodyBytes = 64 << 10

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func toMoneyDTO(m domain.Money) *moneyDTO {
	if !m.IsInitialized() {
		return nil
	}
	return &moneyDTO{Amount: m.Amount(), Currency: string(m.Currency())}
}

type openWalletRequest struct {
	PlayerID       string    `json:"playerId"`
	InitialBalance *moneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	ID       string    `json:"id"`
	PlayerID string    `json:"playerId"`
	Balance  *moneyDTO `json:"balance"`
	Version  int64     `json:"version"`
}

func toWalletResponse(w *domain.Wallet) walletResponse {
	return walletResponse{ID: w.ID().String(), PlayerID: w.PlayerID().String(), Balance: toMoneyDTO(w.Balance()), Version: w.Version()}
}

type submitRequest struct {
	ProviderID                     string    `json:"providerId"`
	ExternalTransactionID          string    `json:"externalTransactionId"`
	PlayerID                       string    `json:"playerId"`
	WalletID                       string    `json:"walletId"`
	RoundID                        string    `json:"roundId"`
	GameID                         string    `json:"gameId"`
	Kind                           string    `json:"kind"`
	Money                          *moneyDTO `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId"`
}

// submitResponse é o corpo da resposta de POST /wagering/transactions.
// balance é o saldo observado no processamento original (também em replays).
type submitResponse struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          *moneyDTO `json:"balance,omitempty"`
	FailureCode      string    `json:"failureCode,omitempty"`
	IdempotentReplay bool      `json:"idempotentReplay"`
}

type transactionResponse struct {
	TransactionID                  string    `json:"transactionId"`
	Origin                         string    `json:"origin"`
	Kind                           string    `json:"kind"`
	Status                         string    `json:"status"`
	WalletID                       string    `json:"walletId"`
	PlayerID                       string    `json:"playerId"`
	Money                          *moneyDTO `json:"money"`
	Balance                        *moneyDTO `json:"balance,omitempty"`
	FailureCode                    string    `json:"failureCode,omitempty"`
	ProviderID                     string    `json:"providerId,omitempty"`
	ExternalTransactionID          string    `json:"externalTransactionId,omitempty"`
	RoundID                        string    `json:"roundId,omitempty"`
	GameID                         string    `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
	CreatedAt                      string    `json:"createdAt"`
	UpdatedAt                      string    `json:"updatedAt"`
}

func toTransactionResponse(t *domain.WagerTransaction) transactionResponse {
	r := transactionResponse{
		TransactionID: t.ID().String(), Origin: string(t.Origin()), Kind: string(t.Kind()), Status: string(t.Status()),
		WalletID: t.WalletID().String(), PlayerID: t.PlayerID().String(), Money: toMoneyDTO(t.Money()),
		Balance: toMoneyDTO(t.ResultBalance()), FailureCode: string(t.FailureCode()),
		CreatedAt: t.CreatedAt().Format(time.RFC3339Nano), UpdatedAt: t.UpdatedAt().Format(time.RFC3339Nano),
	}
	if ext := t.External(); ext != nil {
		r.ProviderID, r.ExternalTransactionID = ext.ProviderID, ext.ExternalTransactionID
		r.RoundID, r.GameID = ext.RoundID, ext.GameID
		r.ReferenceExternalTransactionID = ext.ReferenceExternalTransactionID
	}
	return r
}

// decodeJSON lê um único objeto JSON estrito: limite de tamanho, sem campos
// desconhecidos, sem dados após o objeto. Números JSON no lugar de strings
// monetárias são recusados pelo próprio decodificador.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after JSON object")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("unexpected data after JSON object")
	}
	return nil
}
