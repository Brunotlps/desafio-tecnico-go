package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Kind é o tipo de uma transação.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseExternalKind aceita somente os tipos que provedores podem enviar por
// HTTP ou SQS. OPENING é reservado à abertura interna de carteira.
func ParseExternalKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	case KindOpening:
		return "", fmt.Errorf("%w: OPENING is reserved to internal wallet opening", ErrKindNotAllowed)
	}
	return "", fmt.Errorf("%w: %q", ErrInvalidKind, s)
}

// Política de referência por tipo.
type referencePolicy int

const (
	referenceForbidden referencePolicy = iota
	referenceOptional
	referenceRequired
)

func (k Kind) referencePolicy() referencePolicy {
	switch k {
	case KindRefund, KindRollback:
		return referenceRequired
	case KindWin:
		return referenceOptional
	}
	return referenceForbidden
}

// RequiresReference informa se o tipo exige referenceExternalTransactionId.
func (k Kind) RequiresReference() bool { return k.referencePolicy() == referenceRequired }

// ValidateAmount aplica a política de zero de cada tipo: LOSS exige zero;
// BET, WIN, REFUND e ROLLBACK exigem valor positivo.
func (k Kind) ValidateAmount(m Money) error {
	if !m.IsInitialized() {
		return ErrUninitializedMoney
	}
	switch k {
	case KindLoss:
		if !m.IsZero() {
			return fmt.Errorf("%w: LOSS requires amount 0.00, got %s", ErrAmountNotAllowed, m.Amount())
		}
	case KindBet, KindWin, KindRefund, KindRollback, KindOpening:
		if !m.IsPositive() {
			return fmt.Errorf("%w: %s requires an amount greater than zero", ErrAmountNotAllowed, k)
		}
	default:
		return fmt.Errorf("%w: %q", ErrInvalidKind, string(k))
	}
	return nil
}

// Limites dos identificadores textuais do provedor.
const (
	maxProviderIDLen = 64
	maxExternalIDLen = 128
	maxRoundIDLen    = 128
	maxGameIDLen     = 128
	maxIdemKeyLen    = 255
)

// RawExternalRequest carrega os campos de negócio como chegaram do transporte
// (HTTP ou SQS), ainda sem validação.
type RawExternalRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

// externalFields são os campos de negócio de uma operação externa. É a única
// representação validada e hasheada: ExternalRequest (entrada) e a reidratação
// de WagerTransaction passam pelas mesmas regras.
type externalFields struct {
	providerID          string
	externalID          string
	idempotencyKey      string
	playerID            uuid.UUID
	walletID            uuid.UUID
	roundID             string
	gameID              string
	kind                Kind
	money               Money
	referenceExternalID string
}

func (k Kind) isExternal() bool {
	switch k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return true
	}
	return false
}

// validate aplica todas as regras de uma operação externa: tipo permitido,
// identificadores, política de valor por tipo, moeda e política de referência.
func (f externalFields) validate() error {
	if f.kind == KindOpening {
		return fmt.Errorf("%w: OPENING is reserved to internal wallet opening", ErrKindNotAllowed)
	}
	if !f.kind.isExternal() {
		return fmt.Errorf("%w: %q", ErrInvalidKind, string(f.kind))
	}
	for _, t := range []struct {
		name, val string
		max       int
	}{
		{"providerId", f.providerID, maxProviderIDLen},
		{"externalTransactionId", f.externalID, maxExternalIDLen},
		{"idempotencyKey", f.idempotencyKey, maxIdemKeyLen},
		{"roundId", f.roundID, maxRoundIDLen},
		{"gameId", f.gameID, maxGameIDLen},
	} {
		if err := validateToken(t.name, t.val, t.max); err != nil {
			return err
		}
	}
	if f.playerID == uuid.Nil || f.walletID == uuid.Nil {
		return fmt.Errorf("%w: playerId and walletId are required", ErrInvalidRequest)
	}
	if !f.money.IsInitialized() {
		return ErrUninitializedMoney
	}
	if f.money.IsNegative() {
		return fmt.Errorf("%w: %s", ErrAmountNegative, f.money.Amount())
	}
	if err := f.kind.ValidateAmount(f.money); err != nil {
		return err
	}
	switch f.kind.referencePolicy() {
	case referenceRequired:
		if f.referenceExternalID == "" {
			return fmt.Errorf("%w: referenceExternalTransactionId is required for %s", ErrInvalidRequest, f.kind)
		}
	case referenceForbidden:
		if f.referenceExternalID != "" {
			return fmt.Errorf("%w: referenceExternalTransactionId is not allowed for %s", ErrInvalidRequest, f.kind)
		}
	}
	if f.referenceExternalID != "" {
		if f.referenceExternalID == f.externalID {
			return fmt.Errorf("%w: referenceExternalTransactionId must not be the operation's own externalTransactionId", ErrInvalidRequest)
		}
		return validateToken("referenceExternalTransactionId", f.referenceExternalID, maxExternalIDLen)
	}
	return nil
}

// hash calcula o SHA-256 (hex minúsculo) do JSON canônico dos campos de negócio.
// Algoritmo:
//
//   - campos incluídos: providerId, externalTransactionId, playerId, walletId,
//     roundId, gameId, kind, money{amount,currency} e, quando presente,
//     referenceExternalTransactionId. A chave de idempotência e metadados de
//     transporte (headers, messageId, occurredAt) ficam fora;
//   - normalizações: UUIDs em minúsculas; amount em escala fixa ("25" e "25.0"
//     viram "25.00"); campos opcionais ausentes são omitidos;
//   - serialização: objeto JSON sem espaços, chaves em ordem lexicográfica,
//     strings em UTF-8 sem escape de HTML.
//
// HTTP e SQS usam a mesma função, portanto o mesmo conteúdo gera o mesmo hash.
func (f externalFields) hash() string {
	fields := map[string]any{
		"providerId":            f.providerID,
		"externalTransactionId": f.externalID,
		"playerId":              f.playerID.String(),
		"walletId":              f.walletID.String(),
		"roundId":               f.roundID,
		"gameId":                f.gameID,
		"kind":                  string(f.kind),
		"money": map[string]any{
			"amount":   f.money.Amount(),
			"currency": string(f.money.Currency()),
		},
	}
	if f.referenceExternalID != "" {
		fields["referenceExternalTransactionId"] = f.referenceExternalID
	}
	sum := sha256.Sum256(canonicalJSON(fields))
	return hex.EncodeToString(sum[:])
}

// ExternalRequest é uma operação externa validada e imutável: só existe por
// NewExternalRequest, seus campos são privados e o hash é derivado deles, de
// modo que campos de negócio e hash não podem divergir. O valor zero é inválido
// e rejeitado por NewExternalTransaction.
type ExternalRequest struct {
	f           externalFields
	payloadHash string
}

// NewExternalRequest valida os campos e calcula o hash de idempotência.
func NewExternalRequest(raw RawExternalRequest) (ExternalRequest, error) {
	kind, err := ParseExternalKind(raw.Kind)
	if err != nil {
		return ExternalRequest{}, err
	}
	f := externalFields{
		providerID: raw.ProviderID, externalID: raw.ExternalTransactionID, idempotencyKey: raw.IdempotencyKey,
		roundID: raw.RoundID, gameID: raw.GameID, kind: kind, referenceExternalID: raw.ReferenceExternalTransactionID,
	}
	// Os campos textuais são validados antes de interpretar UUIDs e valores,
	// para que o erro reportado seja o da entrada na ordem do contrato.
	if f.playerID, err = ParseID("playerId", raw.PlayerID); err != nil {
		return ExternalRequest{}, err
	}
	if f.walletID, err = ParseID("walletId", raw.WalletID); err != nil {
		return ExternalRequest{}, err
	}
	if f.money, err = ParseMoney(raw.Amount, raw.Currency); err != nil {
		return ExternalRequest{}, err
	}
	if err := f.validate(); err != nil {
		return ExternalRequest{}, err
	}
	return ExternalRequest{f: f, payloadHash: f.hash()}, nil
}

// check confirma que a requisição foi construída por NewExternalRequest e que
// hash e campos continuam coerentes.
func (r ExternalRequest) check() error {
	if r.payloadHash == "" {
		return fmt.Errorf("%w: uninitialized external request", ErrInvalidRequest)
	}
	if err := r.f.validate(); err != nil {
		return err
	}
	if r.f.hash() != r.payloadHash {
		return fmt.Errorf("%w: payload hash does not match the business fields", ErrInvalidRequest)
	}
	return nil
}

func (r ExternalRequest) PayloadHash() string                    { return r.payloadHash }
func (r ExternalRequest) ProviderID() string                     { return r.f.providerID }
func (r ExternalRequest) ExternalTransactionID() string          { return r.f.externalID }
func (r ExternalRequest) IdempotencyKey() string                 { return r.f.idempotencyKey }
func (r ExternalRequest) PlayerID() uuid.UUID                    { return r.f.playerID }
func (r ExternalRequest) WalletID() uuid.UUID                    { return r.f.walletID }
func (r ExternalRequest) RoundID() string                        { return r.f.roundID }
func (r ExternalRequest) GameID() string                         { return r.f.gameID }
func (r ExternalRequest) Kind() Kind                             { return r.f.kind }
func (r ExternalRequest) Money() Money                           { return r.f.money }
func (r ExternalRequest) ReferenceExternalTransactionID() string { return r.f.referenceExternalID }

// canonicalJSON serializa strings e objetos aninhados com chaves ordenadas.
func canonicalJSON(v any) []byte {
	var buf bytes.Buffer
	writeCanonical(&buf, v)
	return buf.Bytes()
}

func writeCanonical(buf *bytes.Buffer, v any) {
	switch t := v.(type) {
	case string:
		var sb bytes.Buffer
		enc := json.NewEncoder(&sb)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(t) // strings sempre codificam
		buf.Write(bytes.TrimSuffix(sb.Bytes(), []byte("\n")))
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		buf.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				buf.WriteByte(',')
			}
			writeCanonical(buf, k)
			buf.WriteByte(':')
			writeCanonical(buf, t[k])
		}
		buf.WriteByte('}')
	default:
		panic(fmt.Sprintf("canonicalJSON: unsupported type %T", v)) // erro de programação, nunca de entrada
	}
}

// ParseID interpreta um UUID na forma canônica de 36 caracteres.
func ParseID(name, s string) (uuid.UUID, error) {
	// uuid.Parse aceita formas alternativas (urn:uuid:, chaves, sem hífens);
	// exigimos a forma canônica de 36 caracteres.
	if len(s) != 36 {
		return uuid.Nil, fmt.Errorf("%w: %s must be a UUID", ErrInvalidRequest, name)
	}
	id, err := uuid.Parse(s)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: %s must be a UUID", ErrInvalidRequest, name)
	}
	return id, nil
}

func validateToken(name, s string, max int) error {
	if s == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidRequest, name)
	}
	if !utf8.ValidString(s) || utf8.RuneCountInString(s) > max {
		return fmt.Errorf("%w: %s is invalid or longer than %d characters", ErrInvalidRequest, name, max)
	}
	if strings.TrimSpace(s) != s {
		return fmt.Errorf("%w: %s must not have leading or trailing whitespace", ErrInvalidRequest, name)
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: %s contains control characters", ErrInvalidRequest, name)
		}
	}
	return nil
}
