package app

import (
	"errors"
	"strings"
	"testing"
)

const goodBody = `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z","data":{"providerId":"provider-a",` +
	`"externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",` +
	`"walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`

func TestParseEnvelopeAcceptsTheNormativeBody(t *testing.T) {
	env, err := ParseEnvelope([]byte(goodBody))
	if err != nil {
		t.Fatal(err)
	}
	d := env.Data
	if env.MessageID != "msg-123" || env.OccurredAt.IsZero() || d.ProviderID != "provider-a" || d.IdempotencyKey != "provider-a:transaction-123" ||
		d.Kind != "BET" || d.Amount != "25.00" || d.Currency != "BRL" || d.ReferenceExternalTransactionID != "" {
		t.Fatalf("unexpected envelope %+v", env)
	}
	ref := strings.Replace(goodBody, `"kind":"BET"`, `"kind":"REFUND","referenceExternalTransactionId":"bet-1"`, 1)
	if env, err = ParseEnvelope([]byte(ref)); err != nil || env.Data.ReferenceExternalTransactionID != "bet-1" {
		t.Fatalf("reference: %+v %v", env, err)
	}
}

func TestParseEnvelopeRejectsEveryMalformedShape(t *testing.T) {
	repl := func(old, new string) string { return strings.Replace(goodBody, old, new, 1) }
	cases := map[string]string{
		"empty":                   ``,
		"not json":                `not json`,
		"array":                   `[]`,
		"null":                    `null`,
		"trailing data":           goodBody + `{}`,
		"unknown top-level field": repl(`"type"`, `"extra":1,"type"`),
		"unknown data field":      repl(`"gameId"`, `"extra":1,"gameId"`),
		"missing messageId":       repl(`"messageId":"msg-123",`, ``),
		"empty messageId":         repl(`"msg-123"`, `""`),
		"padded messageId":        repl(`"msg-123"`, `" msg-123"`),
		"control in messageId":    repl(`"msg-123"`, `"msg\n123"`),
		"long messageId":          repl(`"msg-123"`, `"`+strings.Repeat("x", 129)+`"`),
		"numeric messageId":       repl(`"msg-123"`, `123`),
		"wrong type":              repl(`WagerTransactionRequested`, `WagerTransactionProcessed`),
		"missing type":            repl(`"type":"WagerTransactionRequested",`, ``),
		"bad occurredAt":          repl(`2026-09-08T12:00:00.000Z`, `yesterday`),
		"missing occurredAt":      repl(`"occurredAt":"2026-09-08T12:00:00.000Z",`, ``),
		"missing data":            `{"messageId":"m","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z"}`,
		"missing providerId":      repl(`"providerId":"provider-a",`, ``),
		"missing idempotencyKey":  repl(`"idempotencyKey":"provider-a:transaction-123",`, ``),
		"missing walletId":        repl(`"walletId":"0192f291-27dd-7d3f-8071-5f8685deef37",`, ``),
		"missing money":           repl(`,"money":{"amount":"25.00","currency":"BRL"}`, ``),
		"money amount missing":    repl(`"amount":"25.00",`, ``),
		"money as number":         repl(`"amount":"25.00"`, `"amount":25.00`),
		"money as float object":   repl(`"money":{"amount":"25.00","currency":"BRL"}`, `"money":25.5`),
		"kind as number":          repl(`"kind":"BET"`, `"kind":1`),
	}
	for name, body := range cases {
		if _, err := ParseEnvelope([]byte(body)); !errors.Is(err, ErrInvalidEnvelope) {
			t.Errorf("%s: err = %v, want ErrInvalidEnvelope", name, err)
		}
	}
}

func TestParseEnvelopeErrorsNeverEchoTheBody(t *testing.T) {
	_, err := ParseEnvelope([]byte(strings.Replace(goodBody, `"amount":"25.00"`, `"amount":9999.99`, 1)))
	if err == nil || strings.Contains(err.Error(), "9999.99") || strings.Contains(err.Error(), "provider-a") {
		t.Fatalf("the error must describe the problem without the financial content: %v", err)
	}
}

func TestMessageHashIsTheSHA256OfTheExactBytes(t *testing.T) {
	a, b := MessageHash([]byte(goodBody)), MessageHash([]byte(strings.Replace(goodBody, `"kind":"BET"`, `"kind": "BET"`, 1)))
	if len(a) != 32 || string(a) == string(b) {
		t.Fatal("a whitespace-only difference must change the exact-bytes hash")
	}
	if string(a) != string(MessageHash([]byte(goodBody))) {
		t.Fatal("the hash must be deterministic")
	}
}
