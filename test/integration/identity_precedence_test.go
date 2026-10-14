//go:build integration

package integration

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
)

// Precedência da identidade: uma operação cuja chave de idempotência ou cujo
// (providerId, externalTransactionId) já está registrado é resolvida (replay ou
// conflito) ANTES das políticas aplicáveis a uma operação nova (moeda habilitada,
// tipo processado). Entradas estruturalmente inválidas continuam inválidas, e o
// escopo por provedor é preservado.

// Caso de uso, sem HTTP. A aplicação de teste habilita BRL e USD; EUR não está habilitada.
func TestServiceIdentityPrecedenceOverNewOperationPolicies(t *testing.T) {
	svc := newService(t)
	ctx := context.Background()
	id := scoped()
	wallet, player := svcOpen(t, svc, "100.00")
	key, ext := id("k"), id("t")

	first, err := svc.Submit(ctx, rawBet(wallet, player, key, ext, "10.00"), "c")
	if err != nil || first.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("first: %+v %v", first, err)
	}
	txs := func() int64 { return countRows(t, "wager_transactions", "provider_id = 'svc-provider'") }
	snapshot := func() [3]int64 {
		bal, ver := walletState(t, wallet)
		return [3]int64{bal, ver, txs()}
	}
	before := snapshot()

	variant := func(key, ext string, mutate func(*domain.RawExternalRequest)) domain.RawExternalRequest {
		raw := rawBet(wallet, player, key, ext, "10.00")
		mutate(&raw)
		return raw
	}
	eur := func(r *domain.RawExternalRequest) { r.Currency = "EUR" }
	win := func(r *domain.RawExternalRequest) { r.Kind = "WIN" }
	loss := func(r *domain.RawExternalRequest) { r.Kind, r.Amount = "LOSS", "0.00" }
	refund := func(r *domain.RawExternalRequest) { r.Kind, r.ReferenceExternalTransactionID = "REFUND", "bet-x" }
	rollback := func(r *domain.RawExternalRequest) { r.Kind, r.ReferenceExternalTransactionID = "ROLLBACK", "bet-x" }
	winEUR := func(r *domain.RawExternalRequest) { win(r); eur(r) }
	winRef := func(r *domain.RawExternalRequest) { r.Kind, r.ReferenceExternalTransactionID = "WIN", "bet-x" }

	t.Run("same key, different content is a conflict", func(t *testing.T) {
		for name, mutate := range map[string]func(*domain.RawExternalRequest){
			"currency not enabled (EUR)": eur, "BET changed to WIN": win, "BET changed to LOSS": loss,
			"BET changed to REFUND": refund, "BET changed to ROLLBACK": rollback, "WIN in a disabled currency": winEUR,
		} {
			if _, err := svc.Submit(ctx, variant(key, ext, mutate), "c"); !errors.Is(err, app.ErrIdempotencyKeyReused) {
				t.Errorf("%s: %v, want ErrIdempotencyKeyReused", name, err)
			}
			// com outro id externo sob a mesma chave
			if _, err := svc.Submit(ctx, variant(key, id("other-"+name), mutate), "c"); !errors.Is(err, app.ErrIdempotencyKeyReused) {
				t.Errorf("%s (other external id): %v, want ErrIdempotencyKeyReused", name, err)
			}
		}
	})

	t.Run("same external id under another key is a conflict", func(t *testing.T) {
		for name, mutate := range map[string]func(*domain.RawExternalRequest){
			"currency not enabled (EUR)": eur, "BET changed to WIN": win, "BET changed to LOSS": loss,
			"BET changed to REFUND": refund, "BET changed to ROLLBACK": rollback, "WIN in a disabled currency": winEUR,
			"unchanged content": func(*domain.RawExternalRequest) {},
		} {
			if _, err := svc.Submit(ctx, variant(id("new-key-"+name), ext, mutate), "c"); !errors.Is(err, app.ErrExternalTransactionReused) {
				t.Errorf("%s: %v, want ErrExternalTransactionReused", name, err)
			}
		}
	})

	t.Run("structurally invalid input stays invalid even for a registered identity", func(t *testing.T) {
		for name, tc := range map[string]struct {
			mutate func(*domain.RawExternalRequest)
			want   error
		}{
			"OPENING is never allowed":   {func(r *domain.RawExternalRequest) { r.Kind = "OPENING" }, domain.ErrKindNotAllowed},
			"unknown kind":               {func(r *domain.RawExternalRequest) { r.Kind = "TRANSFER" }, domain.ErrInvalidKind},
			"malformed currency":         {func(r *domain.RawExternalRequest) { r.Currency = "eur" }, domain.ErrInvalidCurrency},
			"zero WIN":                   {func(r *domain.RawExternalRequest) { r.Kind, r.Amount = "WIN", "0.00" }, domain.ErrAmountNotAllowed},
			"non-zero LOSS":              {func(r *domain.RawExternalRequest) { r.Kind, r.Amount = "LOSS", "1.00" }, domain.ErrAmountNotAllowed},
			"REFUND without a reference": {func(r *domain.RawExternalRequest) { r.Kind = "REFUND" }, domain.ErrInvalidRequest},
			"BET with a reference":       {func(r *domain.RawExternalRequest) { r.ReferenceExternalTransactionID = "x" }, domain.ErrInvalidRequest},
			"scale beyond two places":    {func(r *domain.RawExternalRequest) { r.Amount = "10.001" }, domain.ErrAmountScale},
			"negative amount":            {func(r *domain.RawExternalRequest) { r.Amount = "-1.00" }, domain.ErrAmountNegative},
			"malformed wallet id":        {func(r *domain.RawExternalRequest) { r.WalletID = "not-a-uuid" }, domain.ErrInvalidRequest},
			"empty round":                {func(r *domain.RawExternalRequest) { r.RoundID = "" }, domain.ErrInvalidRequest},
		} {
			_, errKey := svc.Submit(ctx, variant(key, ext, tc.mutate), "c")
			_, errExt := svc.Submit(ctx, variant(id("fresh-"+name), ext, tc.mutate), "c")
			for where, err := range map[string]error{"same key": errKey, "same external id": errExt} {
				if !errors.Is(err, tc.want) {
					t.Errorf("%s (%s): %v, want %v", name, where, err, tc.want)
				}
			}
		}
	})

	t.Run("operations that are really new keep receiving the policy errors", func(t *testing.T) {
		for name, tc := range map[string]struct {
			mutate func(*domain.RawExternalRequest)
			want   error
		}{
			"currency not enabled":                      {eur, app.ErrCurrencyNotSupported},
			"WIN with a reference is not processed yet": {winRef, app.ErrKindNotSupported},
			"REFUND is not processed yet":               {refund, app.ErrKindNotSupported},
			"ROLLBACK is not processed yet":             {rollback, app.ErrKindNotSupported},
			"currency policy comes before kind":         {winEUR, app.ErrCurrencyNotSupported},
		} {
			if _, err := svc.Submit(ctx, variant(id("new-k-"+name), id("new-t-"+name), tc.mutate), "c"); !errors.Is(err, tc.want) {
				t.Errorf("%s: %v, want %v", name, err, tc.want)
			}
		}
	})

	t.Run("the scope is the provider: another provider's identical identity is a new operation", func(t *testing.T) {
		other := func(mutate func(*domain.RawExternalRequest)) domain.RawExternalRequest {
			raw := variant(key, ext, mutate)
			raw.ProviderID = "svc-provider-other"
			return raw
		}
		if _, err := svc.Submit(ctx, other(eur), "c"); !errors.Is(err, app.ErrCurrencyNotSupported) {
			t.Errorf("other provider, EUR: %v", err)
		}
		if _, err := svc.Submit(ctx, other(winRef), "c"); !errors.Is(err, app.ErrKindNotSupported) {
			t.Errorf("other provider, WIN with a reference: %v", err)
		}
	})

	if after := snapshot(); after != before {
		t.Fatalf("refused submissions changed state: %v -> %v", before, after)
	}
	if n := countRows(t, "wager_transactions", "provider_id = 'svc-provider-other'"); n != 0 {
		t.Fatalf("rows for the other provider: %d", n)
	}
	// O replay legítimo continua devolvendo o original.
	replay, err := svc.Submit(ctx, rawBet(wallet, player, key, ext, "10.00"), "c")
	if err != nil || !replay.Replay || replay.Transaction.ID() != first.Transaction.ID() || replay.Transaction.ResultBalance().Amount() != "90.00" {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	assertLedgerMatchesBalance(t, wallet)
}

// Mesmo contrato, ponta a ponta por HTTP, com o token do provedor.
func TestHTTPIdentityPrecedenceOverNewOperationPolicies(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")
	key, ext := id("key"), id("tx")
	first := submit(t, a, "provider-a", key, betBody(w, "provider-a", ext, "10.00"))
	if first.Status != http.StatusOK {
		t.Fatalf("first: %d %s", first.Status, first.Raw)
	}
	snapshot := func() [4]int64 {
		bal, ver := walletState(t, w.ID)
		return [4]int64{bal, ver, countRows(t, "wager_transactions", "wallet_id = $1", uuid.MustParse(w.ID)), countRows(t, "outbox_events", "payload->'data'->>'walletId' = $1", w.ID)}
	}
	before := snapshot()

	body := func(ext string, mutate func(map[string]any)) map[string]any {
		b := betBody(w, "provider-a", ext, "10.00")
		mutate(b)
		return b
	}
	eur := func(b map[string]any) { b["money"] = map[string]string{"amount": "10.00", "currency": "EUR"} }
	win := func(b map[string]any) { b["kind"] = "WIN" }
	loss := func(b map[string]any) { b["kind"], b["money"] = "LOSS", money("0.00") }
	refund := func(b map[string]any) { b["kind"], b["referenceExternalTransactionId"] = "REFUND", "bet-x" }
	rollback := func(b map[string]any) { b["kind"], b["referenceExternalTransactionId"] = "ROLLBACK", "bet-x" }
	winEUR := func(b map[string]any) { win(b); eur(b) }
	winRef := func(b map[string]any) { b["kind"], b["referenceExternalTransactionId"] = "WIN", "bet-x" }
	changes := map[string]func(map[string]any){
		"currency not enabled (EUR)": eur, "BET changed to WIN": win, "BET changed to LOSS": loss,
		"BET changed to REFUND": refund, "BET changed to ROLLBACK": rollback, "WIN in a disabled currency": winEUR,
	}

	for name, mutate := range changes {
		if r := submit(t, a, "provider-a", key, body(ext, mutate)); r.Status != http.StatusConflict || r.str("code") != "IDEMPOTENCY_KEY_REUSED" {
			t.Errorf("same key, %s: %d %s, want 409 IDEMPOTENCY_KEY_REUSED", name, r.Status, r.Raw)
		}
		if r := submit(t, a, "provider-a", id("k2-"+name), body(ext, mutate)); r.Status != http.StatusConflict || r.str("code") != "EXTERNAL_TRANSACTION_ALREADY_REGISTERED" {
			t.Errorf("same external id under another key, %s: %d %s, want 409 EXTERNAL_TRANSACTION_ALREADY_REGISTERED", name, r.Status, r.Raw)
		}
	}

	// Estruturalmente inválidas: continuam 400/422 de entrada, mesmo com identidade registrada.
	for name, tc := range map[string]struct {
		mutate func(map[string]any)
		status int
		code   string
	}{
		"OPENING over HTTP":        {func(b map[string]any) { b["kind"] = "OPENING" }, 422, "KIND_NOT_ALLOWED"},
		"unknown kind":             {func(b map[string]any) { b["kind"] = "TRANSFER" }, 400, "INVALID_KIND"},
		"malformed currency":       {func(b map[string]any) { b["money"] = map[string]string{"amount": "10.00", "currency": "eur"} }, 400, "INVALID_CURRENCY"},
		"zero WIN":                 {func(b map[string]any) { b["kind"], b["money"] = "WIN", money("0.00") }, 400, "AMOUNT_NOT_ALLOWED_FOR_KIND"},
		"REFUND with no reference": {func(b map[string]any) { b["kind"] = "REFUND" }, 400, "INVALID_REQUEST"},
		"scale beyond two places":  {func(b map[string]any) { b["money"] = money("10.001") }, 400, "INVALID_AMOUNT"},
	} {
		for where, k := range map[string]string{"same key": key, "same external id": id("k3-" + name)} {
			if r := submit(t, a, "provider-a", k, body(ext, tc.mutate)); r.Status != tc.status || r.str("code") != tc.code {
				t.Errorf("%s (%s): %d %s, want %d %s", name, where, r.Status, r.Raw, tc.status, tc.code)
			}
		}
	}

	// Operações realmente novas: as políticas continuam valendo.
	for name, tc := range map[string]struct {
		mutate func(map[string]any)
		status int
		code   string
	}{
		"new operation, currency not enabled": {eur, 422, "CURRENCY_NOT_SUPPORTED"},
		"new WIN with a reference":            {winRef, 501, "KIND_NOT_SUPPORTED"},
		"new REFUND":                          {refund, 501, "KIND_NOT_SUPPORTED"},
		"new ROLLBACK":                        {rollback, 501, "KIND_NOT_SUPPORTED"},
		"new WIN in a disabled currency":      {winEUR, 422, "CURRENCY_NOT_SUPPORTED"},
	} {
		if r := submit(t, a, "provider-a", id("nk-"+name), body(id("nt-"+name), tc.mutate)); r.Status != tc.status || r.str("code") != tc.code {
			t.Errorf("%s: %d %s, want %d %s", name, r.Status, r.Raw, tc.status, tc.code)
		}
	}

	// Escopo por provedor: provider-b com a mesma chave e o mesmo id externo é uma operação nova.
	otherBody := func(mutate func(map[string]any)) map[string]any {
		b := body(ext, mutate)
		b["providerId"] = "provider-b"
		return b
	}
	if r := submit(t, a, "provider-b", key, otherBody(eur)); r.Status != 422 || r.str("code") != "CURRENCY_NOT_SUPPORTED" {
		t.Errorf("provider-b, same key, EUR: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, a, "provider-b", key, otherBody(winRef)); r.Status != 501 || r.str("code") != "KIND_NOT_SUPPORTED" {
		t.Errorf("provider-b, same key, WIN with a reference: %d %s", r.Status, r.Raw)
	}

	// Autenticação e autorização antecedem tudo: sem token 401; providerId do corpo diferente do token 403.
	if r := call(t, a.URL, "POST", "/wagering/transactions", "", map[string]string{"Idempotency-Key": key}, body(ext, win)); r.Status != 401 {
		t.Errorf("no token: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, a, "provider-b", key, body(ext, win)); r.Status != 403 {
		t.Errorf("provider-b token with provider-a body: %d %s", r.Status, r.Raw)
	}
	if r := call(t, a.URL, "POST", "/wagering/transactions", token(t, "internal-service"), map[string]string{"Idempotency-Key": key}, body(ext, win)); r.Status != 403 {
		t.Errorf("internal service submitting an operation: %d %s", r.Status, r.Raw)
	}

	if after := snapshot(); after != before {
		t.Fatalf("refused submissions changed state: %v -> %v", before, after)
	}
	if n := countRows(t, "wager_transactions", "provider_id = 'provider-b' AND (idempotency_key = $1 OR external_transaction_id = $2)", key, ext); n != 0 {
		t.Fatalf("provider-b rows: %d", n)
	}
	// O replay legítimo continua devolvendo o original.
	replay := submit(t, a, "provider-a", key, betBody(w, "provider-a", ext, "10.00"))
	if replay.Status != 200 || replay.Body["idempotentReplay"] != true || replay.str("transactionId") != first.str("transactionId") || replay.str("balance", "amount") != "90.00" {
		t.Fatalf("replay: %d %s", replay.Status, replay.Raw)
	}
	assertLedgerMatchesBalance(t, w.ID)
}
