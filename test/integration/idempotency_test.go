//go:build integration

package integration

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

func TestReplayPreservesOriginalResult(t *testing.T) {
	id := scoped()
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")

	first := submit(t, a, "provider-a", id("k1"), betBody(w, "provider-a", id("t1"), "80.00"))
	if first.Status != 200 || first.str("balance", "amount") != "20.00" {
		t.Fatalf("first: %d %s", first.Status, first.Raw)
	}
	// Outra movimentação muda o saldo atual.
	if r := submit(t, a, "provider-a", id("k2"), betBody(w, "provider-a", id("t2"), "5.00")); r.Status != 200 || r.str("balance", "amount") != "15.00" {
		t.Fatalf("second: %d %s", r.Status, r.Raw)
	}
	replay := submit(t, a, "provider-a", id("k1"), betBody(w, "provider-a", id("t1"), "80.00"))
	if replay.Status != 200 || replay.Body["idempotentReplay"] != true || replay.str("transactionId") != first.str("transactionId") ||
		replay.str("balance", "amount") != "20.00" {
		t.Fatalf("replay must return the original balance 20.00, got: %d %s", replay.Status, replay.Raw)
	}
	// Formas equivalentes do mesmo conteúdo ("80" == "80.00") são replay.
	eq := betBody(w, "provider-a", id("t1"), "80")
	if r := submit(t, a, "provider-a", id("k1"), eq); r.Status != 200 || r.Body["idempotentReplay"] != true {
		t.Fatalf("equivalent form: %d %s", r.Status, r.Raw)
	}
	if bal, _ := walletState(t, w.ID); bal != 1500 {
		t.Fatalf("balance %d", bal)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 2 {
		t.Fatalf("debits = %d", n)
	}
}

func TestTerminalRejectionIsNotReevaluatedAfterCredit(t *testing.T) {
	id := scoped()
	a := startApp(t, testConfig())
	w := openWallet(t, a, "10.00")

	rej := submit(t, a, "provider-a", id("k-rej"), betBody(w, "provider-a", id("t-rej"), "80.00"))
	if rej.Status != http.StatusUnprocessableEntity || rej.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("rejection: %d %s", rej.Status, rej.Raw)
	}
	creditDirect(t, w.ID, 100_00) // saldo agora cobriria a aposta

	replay := submit(t, a, "provider-a", id("k-rej"), betBody(w, "provider-a", id("t-rej"), "80.00"))
	if replay.Status != http.StatusUnprocessableEntity || replay.str("status") != "REJECTED" || replay.Body["idempotentReplay"] != true ||
		replay.str("balance", "amount") != "10.00" || replay.str("transactionId") != rej.str("transactionId") {
		t.Fatalf("replay of rejection: %d %s", replay.Status, replay.Raw)
	}
	if n := countRows(t, "wallet_ledger_entries", "wallet_id = $1 AND direction = 'DEBIT'", w.ID); n != 0 {
		t.Fatalf("rejected bet was applied after credit")
	}
	if bal, _ := walletState(t, w.ID); bal != 110_00 {
		t.Fatalf("balance %d", bal)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

func TestKeyConflictsAndExternalIdReuse(t *testing.T) {
	id := scoped()
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")
	first := submit(t, a, "provider-a", id("k1"), betBody(w, "provider-a", id("t1"), "10.00"))
	if first.Status != 200 {
		t.Fatalf("first: %d %s", first.Status, first.Raw)
	}
	want := func(name string, r response, status int, code string) {
		t.Helper()
		if r.Status != status || r.str("code") != code {
			t.Errorf("%s: %d %s, want %d %s", name, r.Status, r.Raw, status, code)
		}
	}
	// Mesma chave, conteúdo diferente (valor, e também carteira/jogo).
	want("different amount", submit(t, a, "provider-a", id("k1"), betBody(w, "provider-a", id("t1"), "11.00")), 409, "IDEMPOTENCY_KEY_REUSED")
	diffGame := betBody(w, "provider-a", id("t1"), "10.00")
	diffGame["gameId"] = "other-game"
	want("different game", submit(t, a, "provider-a", id("k1"), diffGame), 409, "IDEMPOTENCY_KEY_REUSED")
	// Mesma chave com conteúdo diferente por outra carteira: o banco também a barra.
	w2 := openWallet(t, a, "100.00")
	want("different wallet", submit(t, a, "provider-a", id("k1"), betBody(w2, "provider-a", id("t1"), "10.00")), 409, "IDEMPOTENCY_KEY_REUSED")
	// Mesma operação externa sob outra chave: nunca reaplicada.
	want("external id, new key", submit(t, a, "provider-a", id("k-other"), betBody(w, "provider-a", id("t1"), "10.00")), 409, "EXTERNAL_TRANSACTION_ALREADY_REGISTERED")
	want("external id, new key and amount", submit(t, a, "provider-a", id("k-other2"), betBody(w, "provider-a", id("t1"), "99.00")), 409, "EXTERNAL_TRANSACTION_ALREADY_REGISTERED")

	if bal, _ := walletState(t, w.ID); bal != 90_00 {
		t.Fatalf("balance %d: conflicts must not move money", bal)
	}
	if bal, _ := walletState(t, w2.ID); bal != 100_00 {
		t.Fatalf("other wallet balance %d", bal)
	}
	if n := countRows(t, "wager_transactions", "provider_id = 'provider-a' AND external_transaction_id = $1", id("t1")); n != 1 {
		t.Fatalf("external operation rows = %d", n)
	}
	// O original continua replayável.
	if r := submit(t, a, "provider-a", id("k1"), betBody(w, "provider-a", id("t1"), "10.00")); r.Status != 200 || r.Body["idempotentReplay"] != true {
		t.Fatalf("original replay: %d %s", r.Status, r.Raw)
	}
}

func TestIdempotencyKeysAreScopedPerProvider(t *testing.T) {
	id := scoped()
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")
	ra := submit(t, a, "provider-a", id("shared-key"), betBody(w, "provider-a", id("shared-tx"), "10.00"))
	rb := submit(t, a, "provider-b", id("shared-key"), betBody(w, "provider-b", id("shared-tx"), "10.00"))
	if ra.Status != 200 || rb.Status != 200 || ra.Body["idempotentReplay"] != false || rb.Body["idempotentReplay"] != false ||
		ra.str("transactionId") == rb.str("transactionId") {
		t.Fatalf("providers must not share idempotency state: %s | %s", ra.Raw, rb.Raw)
	}
	if bal, _ := walletState(t, w.ID); bal != 80_00 {
		t.Fatalf("balance %d", bal)
	}
}

func TestTransactionQueriesByInternalAndExternalId(t *testing.T) {
	id := scoped()
	a := startApp(t, testConfig())
	w := openWallet(t, a, "100.00")
	r := submit(t, a, "provider-a", id("k"), betBody(w, "provider-a", id("t"), "10.00"))
	txID := uuid.MustParse(r.str("transactionId"))

	byID := call(t, a.URL, "GET", "/wagering/transactions/"+txID.String(), token(t, "provider-a"), nil, nil)
	byExt := call(t, a.URL, "GET", "/providers/provider-a/wagering/transactions/"+id("t"), token(t, "provider-a"), nil, nil)
	for name, g := range map[string]response{"by id": byID, "by external id": byExt} {
		if g.Status != 200 || g.str("transactionId") != txID.String() || g.str("status") != "PROCESSED" || g.str("kind") != "BET" ||
			g.str("providerId") != "provider-a" || g.str("balance", "amount") != "90.00" || g.str("money", "amount") != "10.00" {
			t.Errorf("%s: %d %s", name, g.Status, g.Raw)
		}
	}
	if g := call(t, a.URL, "GET", "/providers/provider-a/wagering/transactions/missing", token(t, "provider-a"), nil, nil); g.Status != 404 {
		t.Errorf("missing: %d", g.Status)
	}
	if g := call(t, a.URL, "GET", "/wagering/transactions/not-a-uuid", token(t, "provider-a"), nil, nil); g.Status != 400 {
		t.Errorf("bad id: %d", g.Status)
	}
}

// A identidade já registrada tem precedência sobre as validações que dependem do
// estado atual (carteira inexistente, jogador ou moeda divergentes): reutilizar a
// chave com outro conteúdo é conflito, não 404/422.
func TestIdempotencyConflictTakesPrecedenceOverInputValidation(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	w := openWallet(t, a, "100.00")
	key, ext := id("key"), id("tx")
	first := submit(t, a, "provider-a", key, betBody(w, "provider-a", ext, "10.00"))
	if first.Status != 200 {
		t.Fatalf("first: %d %s", first.Status, first.Raw)
	}
	snapshot := func() (int64, int64, int64, int64) {
		bal, ver := walletState(t, w.ID)
		return bal, ver, countRows(t, "wager_transactions", "provider_id = 'provider-a' AND external_transaction_id = $1", ext),
			countRows(t, "wallet_ledger_entries", "wallet_id = $1", w.ID)
	}
	b0, v0, t0, e0 := snapshot()

	mutate := func(f func(map[string]any)) map[string]any {
		body := betBody(w, "provider-a", ext, "10.00")
		f(body)
		return body
	}
	conflicts := map[string]map[string]any{
		"nonexistent wallet": mutate(func(b map[string]any) { b["walletId"] = uuid.NewString() }),
		"another player":     mutate(func(b map[string]any) { b["playerId"] = uuid.NewString() }),
		"another currency":   mutate(func(b map[string]any) { b["money"] = map[string]string{"amount": "10.00", "currency": "USD"} }),
		"another amount":     mutate(func(b map[string]any) { b["money"] = money("11.00") }),
		"another round":      mutate(func(b map[string]any) { b["roundId"] = "other-round" }),
		"other external id and nonexistent wallet": mutate(func(b map[string]any) {
			b["externalTransactionId"] = id("other-tx")
			b["walletId"] = uuid.NewString()
		}),
	}
	for name, body := range conflicts {
		r := submit(t, a, "provider-a", key, body)
		if r.Status != http.StatusConflict || r.str("code") != "IDEMPOTENCY_KEY_REUSED" {
			t.Errorf("same key, %s: %d %s, want 409 IDEMPOTENCY_KEY_REUSED", name, r.Status, r.Raw)
		}
	}
	// Mesmo id externo sob outra chave: conflito, mesmo com carteira inexistente ou divergente.
	for name, body := range map[string]map[string]any{
		"nonexistent wallet": mutate(func(b map[string]any) { b["walletId"] = uuid.NewString() }),
		"another player":     mutate(func(b map[string]any) { b["playerId"] = uuid.NewString() }),
		"unchanged":          mutate(func(b map[string]any) {}),
	} {
		r := submit(t, a, "provider-a", id("new-key-"+name), body)
		if r.Status != http.StatusConflict || r.str("code") != "EXTERNAL_TRANSACTION_ALREADY_REGISTERED" {
			t.Errorf("same external id, new key, %s: %d %s", name, r.Status, r.Raw)
		}
	}

	// Outro provedor não vê nem é afetado pela identidade do A: tratamento normal da entrada.
	other := mutate(func(b map[string]any) { b["providerId"] = "provider-b"; b["walletId"] = uuid.NewString() })
	if r := submit(t, a, "provider-b", key, other); r.Status != 404 || r.str("code") != "WALLET_NOT_FOUND" {
		t.Errorf("provider-b with provider-a's key and an unknown wallet: %d %s", r.Status, r.Raw)
	}

	// Operações realmente novas continuam validadas.
	if r := submit(t, a, "provider-a", id("k-new"), mutate(func(b map[string]any) { b["externalTransactionId"] = id("tx-new"); b["walletId"] = uuid.NewString() })); r.Status != 404 {
		t.Errorf("new operation, unknown wallet: %d %s", r.Status, r.Raw)
	}
	if r := submit(t, a, "provider-a", id("k-new2"), mutate(func(b map[string]any) { b["externalTransactionId"] = id("tx-new2"); b["playerId"] = uuid.NewString() })); r.Status != 422 || r.str("code") != "WALLET_MISMATCH" {
		t.Errorf("new operation, wrong player: %d %s", r.Status, r.Raw)
	}

	// Nada se moveu; o replay legítimo, mesmo após outra movimentação, devolve o original.
	if b, v, tx, e := snapshot(); b != b0 || v != v0 || tx != t0 || e != e0 {
		t.Fatalf("conflicts changed state: balance %d->%d version %d->%d tx %d->%d ledger %d->%d", b0, b, v0, v, t0, tx, e0, e)
	}
	if r := submit(t, a, "provider-a", id("other-movement"), betBody(w, "provider-a", id("other-movement-tx"), "5.00")); r.Status != 200 {
		t.Fatalf("other movement: %d %s", r.Status, r.Raw)
	}
	replay := submit(t, a, "provider-a", key, betBody(w, "provider-a", ext, "10.00"))
	if replay.Status != 200 || replay.Body["idempotentReplay"] != true || replay.str("balance", "amount") != "90.00" ||
		replay.str("transactionId") != first.str("transactionId") {
		t.Fatalf("legitimate replay: %d %s", replay.Status, replay.Raw)
	}
	assertLedgerMatchesBalance(t, w.ID)
}

// A mesma identidade disputada entre carteiras diferentes ao mesmo tempo: a
// unicidade do banco elege um vencedor; os demais são replay (mesmo conteúdo) ou
// conflito (conteúdo de outra carteira). Um único débito no total.
func TestConcurrentIdentityRaceAcrossWallets(t *testing.T) {
	a := startApp(t, testConfig())
	id := scoped()
	for round := 0; round < 8; round++ {
		w1, w2 := openWallet(t, a, "100.00"), openWallet(t, a, "100.00")
		key, ext := id(fmt.Sprintf("race-key-%d", round)), id(fmt.Sprintf("race-tx-%d", round))
		wallets := []wallet{w1, w2}
		results := parallel(24, func(i int) response {
			return submit(t, a, "provider-a", key, betBody(wallets[i%2], "provider-a", ext, "10.00"))
		})
		var originals int
		winner := ""
		for _, r := range results {
			if r.Status == 200 && r.Body["idempotentReplay"] == false {
				originals++
				winner = r.str("transactionId")
			}
		}
		if originals != 1 {
			t.Fatalf("round %d: %d original results, want exactly 1", round, originals)
		}
		var winnerWallet string
		if err := testPool.QueryRow(t.Context(), `SELECT wallet_id::text FROM wager_transactions WHERE id = $1`, winner).Scan(&winnerWallet); err != nil {
			t.Fatal(err)
		}
		for i, r := range results {
			sameWallet := wallets[i%2].ID == winnerWallet
			switch {
			case sameWallet && r.Status == 200 && r.str("transactionId") == winner:
			case !sameWallet && r.Status == 409 && r.str("code") == "IDEMPOTENCY_KEY_REUSED":
			default:
				t.Fatalf("round %d request %d (winner wallet=%v): unexpected %d %s", round, i, sameWallet, r.Status, r.Raw)
			}
		}
		debits := countRows(t, "wallet_ledger_entries", "direction = 'DEBIT' AND wallet_id IN ($1, $2)", w1.ID, w2.ID)
		rows := countRows(t, "wager_transactions", "provider_id = 'provider-a' AND external_transaction_id = $1", ext)
		if debits != 1 || rows != 1 {
			t.Fatalf("round %d: debits=%d rows=%d", round, debits, rows)
		}
		for _, w := range wallets {
			assertLedgerMatchesBalance(t, w.ID)
		}
		// O mesmo id externo com chaves diferentes em carteiras diferentes: um vencedor, o resto 409.
		ext2 := id(fmt.Sprintf("race-ext-%d", round))
		res2 := parallel(12, func(i int) response {
			return submit(t, a, "provider-a", id(fmt.Sprintf("distinct-key-%d-%d", round, i)), betBody(wallets[i%2], "provider-a", ext2, "5.00"))
		})
		var wins int
		for _, r := range res2 {
			switch {
			case r.Status == 200:
				wins++
			case r.Status == 409 && r.str("code") == "EXTERNAL_TRANSACTION_ALREADY_REGISTERED":
			default:
				t.Fatalf("round %d distinct keys: unexpected %d %s", round, r.Status, r.Raw)
			}
		}
		if wins != 1 {
			t.Fatalf("round %d: %d winners for one external id", round, wins)
		}
	}
}
