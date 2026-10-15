package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/domain"
)

// Entrada por SQS: o mesmo núcleo financeiro do HTTP (processExternal), numa única UnitOfWork que também grava a inbox.
//
// Fluxo de uma entrega: bytes exatos → hash da mensagem (SHA-256, distinto do hash financeiro canônico) → validação do envelope →
// UMA tentativa SQL: consulta da inbox (identidade já tratada? mesmo hash? conflito?) → processExternal com os repositórios da
// tentativa → inbox (já concluída) → commit. A inbox, as finanças, o ledger, a agenda de pendência e a outbox compartilham o
// commit; falha de qualquer parte desfaz tudo. Nada é decidido por violação de unicidade sem reconsultar: ErrDuplicateInbox e as
// unicidades financeiras desfazem a tentativa e uma nova a enxerga (como Submit).

// ErrInvalidEnvelope: o corpo não é um WagerTransactionRequested válido. É permanente: a mensagem é imutável.
var ErrInvalidEnvelope = errors.New("invalid message envelope")

// Tipo normativo do envelope de entrada.
const EnvelopeTypeRequested = "WagerTransactionRequested"

// Envelope é o corpo normativo de uma mensagem de entrada, já validado.
type Envelope struct {
	MessageID  string
	OccurredAt time.Time
	Data       domain.RawExternalRequest
}

type envelopeJSON struct {
	MessageID  *string   `json:"messageId"`
	Type       *string   `json:"type"`
	OccurredAt *string   `json:"occurredAt"`
	Data       *dataJSON `json:"data"`
}

type dataJSON struct {
	ProviderID                     *string    `json:"providerId"`
	ExternalTransactionID          *string    `json:"externalTransactionId"`
	IdempotencyKey                 *string    `json:"idempotencyKey"`
	PlayerID                       *string    `json:"playerId"`
	WalletID                       *string    `json:"walletId"`
	RoundID                        *string    `json:"roundId"`
	GameID                         *string    `json:"gameId"`
	Kind                           *string    `json:"kind"`
	Money                          *moneyJSON `json:"money"`
	ReferenceExternalTransactionID *string    `json:"referenceExternalTransactionId"`
}

type moneyJSON struct {
	Amount   *string `json:"amount"`
	Currency *string `json:"currency"`
}

const maxMessageIDLen = 128

// ParseEnvelope valida explicitamente o corpo: um único objeto JSON, sem campos desconhecidos, com todos os campos obrigatórios e
// os tipos esperados (valores monetários são strings; nada passa por ponto flutuante). Só valida a forma do envelope; as regras do
// domínio (valores, moeda, tipo) são aplicadas por domain.NewExternalRequest.
func ParseEnvelope(body []byte) (Envelope, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var raw envelopeJSON
	if err := dec.Decode(&raw); err != nil {
		return Envelope{}, fmt.Errorf("%w: %s", ErrInvalidEnvelope, jsonProblem(err))
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Envelope{}, fmt.Errorf("%w: trailing data after the envelope", ErrInvalidEnvelope)
	}
	bad := func(field string) (Envelope, error) {
		return Envelope{}, fmt.Errorf("%w: %s is missing or invalid", ErrInvalidEnvelope, field)
	}
	if raw.MessageID == nil || !validMessageID(*raw.MessageID) {
		return bad("messageId")
	}
	if raw.Type == nil || *raw.Type != EnvelopeTypeRequested {
		return bad("type")
	}
	if raw.OccurredAt == nil {
		return bad("occurredAt")
	}
	occurred, err := time.Parse(time.RFC3339Nano, *raw.OccurredAt)
	if err != nil {
		return bad("occurredAt")
	}
	d := raw.Data
	if d == nil {
		return bad("data")
	}
	required := []struct {
		name string
		v    *string
	}{
		{"data.providerId", d.ProviderID}, {"data.externalTransactionId", d.ExternalTransactionID}, {"data.idempotencyKey", d.IdempotencyKey},
		{"data.playerId", d.PlayerID}, {"data.walletId", d.WalletID}, {"data.roundId", d.RoundID}, {"data.gameId", d.GameID}, {"data.kind", d.Kind},
	}
	for _, f := range required {
		if f.v == nil {
			return bad(f.name)
		}
	}
	if d.Money == nil || d.Money.Amount == nil || d.Money.Currency == nil {
		return bad("data.money")
	}
	out := Envelope{MessageID: *raw.MessageID, OccurredAt: occurred.UTC(), Data: domain.RawExternalRequest{
		ProviderID: *d.ProviderID, ExternalTransactionID: *d.ExternalTransactionID, IdempotencyKey: *d.IdempotencyKey,
		PlayerID: *d.PlayerID, WalletID: *d.WalletID, RoundID: *d.RoundID, GameID: *d.GameID, Kind: *d.Kind,
		Amount: *d.Money.Amount, Currency: *d.Money.Currency,
	}}
	if d.ReferenceExternalTransactionID != nil {
		out.Data.ReferenceExternalTransactionID = *d.ReferenceExternalTransactionID
	}
	return out, nil
}

func validMessageID(s string) bool {
	if s == "" || len(s) > maxMessageIDLen || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// jsonProblem descreve o erro de JSON sem ecoar o conteúdo (pode conter dados financeiros).
func jsonProblem(err error) string {
	var ute *json.UnmarshalTypeError
	var se *json.SyntaxError
	switch {
	case errors.As(err, &ute):
		return "field " + ute.Field + " has the wrong type"
	case errors.As(err, &se):
		return "malformed JSON"
	case strings.HasPrefix(err.Error(), "json: unknown field"):
		return "unknown field"
	}
	return "not a valid envelope"
}

// MessageHash é o hash da mensagem: SHA-256 dos bytes exatos recebidos (não confundir com o hash financeiro canônico).
func MessageHash(body []byte) []byte {
	h := sha256.Sum256(body)
	return h[:]
}

// InboundDisposition é a decisão de transporte para uma entrega.
type InboundDisposition int

const (
	// InboundDone: tratamento durável confirmado (processada, rejeitada, pendente ou duplicata/replay): remover a mensagem.
	InboundDone InboundDisposition = iota
	// InboundRetry: falha transitória, disputa ou commit incerto: NÃO remover; nova entrega com backoff.
	InboundRetry
	// InboundPermanent: entrada inválida ou conflito permanente: diagnóstico confirmado → DLQ confirmada → remover.
	InboundPermanent
)

func (d InboundDisposition) String() string {
	return [...]string{"DONE", "RETRY", "PERMANENT"}[d]
}

// InboundResult é o resultado classificado de ProcessMessage (nunca um erro financeiro: erros viram Retry ou Permanent).
type InboundResult struct {
	Disposition InboundDisposition
	// Code é o código estável de uma recusa permanente (INVALID_ENVELOPE, MESSAGE_ID_CONFLICT, WALLET_NOT_FOUND, ...).
	Code string
	// MessageID é o do envelope quando é válido (para diagnóstico e logs).
	MessageID string
	// Status é a disposição durável da operação (PROCESSED, REJECTED, PENDING_REFERENCE) quando Done.
	Status string
	// Duplicate: a mesma identidade e o mesmo hash já estavam concluídos na inbox (nenhum efeito foi reaplicado).
	Duplicate bool
	// FinancialReplay: uma mensagem nova cuja operação já estava registrada (resultado original devolvido).
	FinancialReplay bool
	TransactionID   uuid.UUID
	WalletID        string // do corpo, quando disponível (grupo da DLQ)
	ProviderID      string
	// Cause é o erro que levou a Retry ou Permanent (para log; nunca contém o corpo).
	Cause error
}

// permanentCodes classifica os erros de entrada/regra que uma mensagem imutável não resolve sozinha. O que não estiver aqui nem
// for transitório conhecido é tratado como transitório: nunca vira rejeição permanente por engano.
var permanentCodes = []struct {
	target error
	code   string
}{
	{ErrWalletNotFound, "WALLET_NOT_FOUND"},
	{ErrIdempotencyKeyReused, "IDEMPOTENCY_KEY_REUSED"},
	{ErrExternalTransactionReused, "EXTERNAL_TRANSACTION_ALREADY_REGISTERED"},
	{ErrWalletMismatch, "WALLET_MISMATCH"},
	{ErrCurrencyNotSupported, "CURRENCY_NOT_SUPPORTED"},
	{domain.ErrKindNotAllowed, "KIND_NOT_ALLOWED"},
	{ErrKindNotSupported, "KIND_NOT_SUPPORTED"},
	{domain.ErrInvalidAmount, "INVALID_AMOUNT"},
	{domain.ErrAmountScale, "INVALID_AMOUNT"},
	{domain.ErrAmountNegative, "INVALID_AMOUNT"},
	{domain.ErrAmountOverflow, "INVALID_AMOUNT"},
	{domain.ErrAmountNotAllowed, "AMOUNT_NOT_ALLOWED_FOR_KIND"},
	{domain.ErrInvalidCurrency, "INVALID_CURRENCY"},
	{domain.ErrInvalidKind, "INVALID_KIND"},
	{domain.ErrInvalidRequest, "INVALID_REQUEST"},
	{domain.ErrUninitializedMoney, "INVALID_REQUEST"},
}

func permanentCode(err error) (string, bool) {
	for _, p := range permanentCodes {
		if errors.Is(err, p.target) {
			return p.code, true
		}
	}
	return "", false
}

// ProcessMessage trata uma entrega SQS. consumerName é estável entre instâncias e reinícios. Não abre SQL durante nenhuma espera
// de transporte e não chama Submit (que abre outra UnitOfWork).
func (s *Service) ProcessMessage(ctx context.Context, consumerName string, body []byte) InboundResult {
	received := s.now()
	hash := MessageHash(body)
	env, err := ParseEnvelope(body)
	if err != nil {
		return InboundResult{Disposition: InboundPermanent, Code: "INVALID_ENVELOPE", Cause: err}
	}
	res := InboundResult{MessageID: env.MessageID, WalletID: env.Data.WalletID, ProviderID: env.Data.ProviderID}
	req, err := domain.NewExternalRequest(env.Data)
	if err != nil {
		if code, ok := permanentCode(err); ok {
			res.Disposition, res.Code, res.Cause = InboundPermanent, code, err
			return res
		}
		res.Disposition, res.Cause = InboundRetry, err
		return res
	}
	for attempt := 0; ; attempt++ {
		r, err := s.messageOnce(ctx, consumerName, env, req, hash, received, res)
		if err == nil {
			return r
		}
		// Unicidade revelou uma operação ou mensagem concorrente já confirmada: a nova tentativa a enxerga (replay, duplicata ou conflito).
		if (errors.Is(err, ErrDuplicateIdempotencyKey) || errors.Is(err, ErrDuplicateExternalTransaction) ||
			errors.Is(err, ErrReversalTargetTaken) || errors.Is(err, ErrDuplicateInbox)) && attempt < maxConflictRetries {
			s.obs.Conflict(conflictReason(err))
			continue
		}
		if code, ok := permanentCode(err); ok {
			res.Disposition, res.Code, res.Cause = InboundPermanent, code, err
			return res
		}
		// Transitório conhecido, commit incerto, cancelamento ou erro inesperado: nunca é rejeição de negócio nem erro permanente.
		if errors.Is(err, ErrCommitUnknown) {
			s.log.WarnContext(ctx, "message commit outcome unknown; the redelivery resolves it by identity",
				"messageId", env.MessageID, "walletId", env.Data.WalletID, "providerId", env.Data.ProviderID)
		}
		res.Disposition, res.Cause = InboundRetry, err
		return res
	}
}

func (s *Service) messageOnce(ctx context.Context, consumerName string, env Envelope, req domain.ExternalRequest, hash []byte,
	received time.Time, base InboundResult) (InboundResult, error) {
	res := base
	txID := s.newID()
	var result SubmitResult
	err := s.uow.Do(ctx, func(ctx context.Context, r Repositories) error {
		res, result = base, SubmitResult{}
		rec, err := r.Inbox.Get(ctx, consumerName, env.MessageID)
		switch {
		case err == nil:
			// A identidade já foi tratada: o mesmo hash é duplicata (nada é reaplicado); outro hash é conflito permanente e
			// o registro original não é tocado.
			if !bytes.Equal(rec.BodyHash, hash) {
				res.Disposition, res.Code = InboundPermanent, "MESSAGE_ID_CONFLICT"
				res.Cause = errors.New("messageId already handled with a different body")
				return nil
			}
			res.Disposition, res.Duplicate, res.Status, res.FinancialReplay, res.TransactionID =
				InboundDone, true, rec.Disposition, rec.FinancialReplay, rec.TransactionID
			return nil
		case !errors.Is(err, ErrInboxNotFound):
			return err
		}
		result, err = s.processExternal(ctx, r, req, txID, env.MessageID)
		if err != nil {
			return err
		}
		tx := result.Transaction
		if err := r.Inbox.Insert(ctx, InboxRecord{
			ConsumerName: consumerName, MessageID: env.MessageID, BodyHash: hash, ReceivedAt: received, CompletedAt: s.now(),
			Disposition: string(tx.Status()), FinancialReplay: result.Replay, TransactionID: tx.ID(),
		}); err != nil {
			return err
		}
		res.Disposition, res.Status, res.FinancialReplay, res.TransactionID = InboundDone, string(tx.Status()), result.Replay, tx.ID()
		return nil
	})
	if err != nil {
		return InboundResult{}, err
	}
	switch {
	case res.Disposition == InboundDone && res.Duplicate:
		s.obs.InboxOutcome("duplicate")
	case res.Disposition == InboundPermanent && res.Code == "MESSAGE_ID_CONFLICT":
		s.obs.InboxOutcome("conflict")
	case res.Disposition == InboundDone && result.Transaction != nil:
		s.logResult(ctx, result, env.MessageID)
		s.obs.InboxOutcome("new")
		s.obs.Result("sqs", string(result.Transaction.Kind()), string(result.Transaction.Status()), result.Replay)
	}
	return res, nil
}

// RecordDiagnostic grava (de forma idempotente) o diagnóstico de uma mensagem permanentemente recusada, numa transação própria. Não
// cria transação financeira nem ledger e não guarda o corpo.
func (s *Service) RecordDiagnostic(ctx context.Context, d Diagnostic) error {
	if d.CreatedAt.IsZero() {
		d.CreatedAt = s.now()
	}
	return s.uow.Do(ctx, func(ctx context.Context, r Repositories) error { return r.Diagnostics.Record(ctx, d) })
}

// HasDiagnostic informa se a mensagem já tem diagnóstico. Um erro de consulta é devolvido como erro: nunca significa "ausente".
func (s *Service) HasDiagnostic(ctx context.Context, consumerName, sourceQueue, brokerMessageID string, bodyHash []byte) (bool, error) {
	return s.uow.Reader().Diagnostics.Exists(ctx, consumerName, sourceQueue, brokerMessageID, bodyHash)
}

// WalletIDOf extrai data.walletId de um corpo, se ele for um envelope legível com um walletId UUID; "" caso contrário. Só serve para
// escolher o MessageGroupId da DLQ (nunca autoriza nada).
func WalletIDOf(body []byte) string {
	var probe struct {
		Data struct {
			WalletID string `json:"walletId"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &probe) != nil {
		return ""
	}
	if id, err := uuid.Parse(probe.Data.WalletID); err == nil && id.String() == probe.Data.WalletID {
		return probe.Data.WalletID
	}
	return ""
}
