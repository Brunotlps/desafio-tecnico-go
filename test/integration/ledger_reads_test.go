//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/domain"
	"desafio-tecnico-go/internal/infra/postgres"
)

// Ledger paginado e reconciliação (leituras do incremento 5). Os testes HTTP usam a aplicação real (JWT do Keycloak,
// papel restrito do banco); os de serviço usam relógio fixo e o mesmo Store de produção, com um ponto de sincronização
// entre as consultas do snapshot.

// ledgerSvc monta um serviço sobre o banco compartilhado de teste com o leitor do ledger (o Store), pelo papel restrito.
func ledgerSvc(t *testing.T, clock *fakeClock) (*app.Service, *postgres.Store) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), env.runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := postgres.NewStore(postgres.StaticPool(pool))
	opts := []app.Option{app.WithLedgerReader(store)}
	if clock != nil {
		opts = append(opts, app.WithClock(clock.Now))
	}
	return app.NewService(store, app.Config{AllowedCurrencies: []domain.Currency{"BRL"}, Reference: testRef},
		slog.New(slog.NewTextHandler(io.Discard, nil)), opts...), store
}

func ledgerIDsInDB(t *testing.T, walletID string) []string {
	t.Helper()
	rows, err := testPool.Query(context.Background(), `SELECT id::text FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY seq`, walletID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

type ledgerPageHTTP struct {
	ids    []string
	next   string
	raw    string
	status int
}

func getLedger(t *testing.T, a *testApp, tok, walletID string, query url.Values) response {
	t.Helper()
	p := "/wallets/" + walletID + "/ledger"
	if len(query) > 0 {
		p += "?" + query.Encode()
	}
	return call(t, a.URL, "GET", p, tok, nil, nil)
}

func pageOf(t *testing.T, r response) ledgerPageHTTP {
	t.Helper()
	if r.Status != http.StatusOK {
		t.Fatalf("ledger page: %d %s", r.Status, r.Raw)
	}
	items, ok := r.Body["items"].([]any)
	if !ok {
		t.Fatalf("items is not a list: %s", r.Raw)
	}
	p := ledgerPageHTTP{next: r.str("nextCursor"), raw: r.Raw}
	for _, it := range items {
		p.ids = append(p.ids, it.(map[string]any)["id"].(string))
	}
	return p
}

// walkHTTP percorre o ledger inteiro com o limite dado, devolvendo os ids na ordem e a quantidade de páginas.
func walkHTTP(t *testing.T, a *testApp, tok, walletID string, limit int) (ids []string, pages int) {
	t.Helper()
	cursor := ""
	for {
		q := url.Values{"limit": {fmt.Sprint(limit)}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		p := pageOf(t, getLedger(t, a, tok, walletID, q))
		pages++
		if len(p.ids) > limit {
			t.Fatalf("page with %d items for limit %d", len(p.ids), limit)
		}
		ids = append(ids, p.ids...)
		if p.next == "" {
			return ids, pages
		}
		if len(p.ids) == 0 {
			t.Fatal("a next cursor on an empty page")
		}
		cursor = p.next
		if pages > 1000 {
			t.Fatal("pagination does not terminate")
		}
	}
}

func submitBET(t *testing.T, a *testApp, w wallet, amount string) response {
	t.Helper()
	ext := uniqueID("ledger-bet")
	return submit(t, a, "provider-a", ext, betBody(w, "provider-a", ext, amount))
}

func sameIDs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestLedgerPaginationWalksEveryEntryOnceInSeqOrderAndKeepsTheScope(t *testing.T) {
	a := startApp(t, testConfig())
	tok := token(t, "internal-service")
	w := openWallet(t, a, "1000.00")
	other := openWallet(t, a, "500.00") // outra carteira, com lançamentos intercalados
	for i := 0; i < 7; i++ {
		if r := submitBET(t, a, w, "10.00"); r.Status != 200 {
			t.Fatal(r.Raw)
		}
		if r := submitBET(t, a, other, "1.00"); r.Status != 200 {
			t.Fatal(r.Raw)
		}
	}
	want := ledgerIDsInDB(t, w.ID) // OPENING + 7 BETs, em ordem de seq
	if len(want) != 8 {
		t.Fatalf("setup: %d entries", len(want))
	}
	for _, limit := range []int{1, 3, 7, 8, 9, 100} {
		got, pages := walkHTTP(t, a, tok, w.ID, limit)
		if !sameIDs(got, want) {
			t.Fatalf("limit %d: wrong walk\n got %v\nwant %v", limit, got, want)
		}
		if wantPages := (len(want) + limit - 1) / limit; pages != wantPages {
			t.Errorf("limit %d: %d pages, want %d", limit, pages, wantPages)
		}
	}
	// Bordas do limite: exatamente `limit` itens restantes não geram próxima página; um a mais gera.
	if p := pageOf(t, getLedger(t, a, tok, w.ID, url.Values{"limit": {"8"}})); len(p.ids) != 8 || p.next != "" {
		t.Errorf("limit 8 over 8 entries: %d items, nextCursor %q", len(p.ids), p.next)
	}
	if p := pageOf(t, getLedger(t, a, tok, w.ID, url.Values{"limit": {"7"}})); len(p.ids) != 7 || p.next == "" {
		t.Errorf("limit 7 over 8 entries: %d items, nextCursor %q", len(p.ids), p.next)
	}
	// Sem limit: o padrão é 50.
	if p := pageOf(t, getLedger(t, a, tok, w.ID, nil)); len(p.ids) != 8 || p.next != "" {
		t.Errorf("default limit: %d items, nextCursor %q", len(p.ids), p.next)
	}
	// Conteúdo e escopo: só lançamentos da carteira, com dinheiro em string.
	first := getLedger(t, a, tok, w.ID, url.Values{"limit": {"2"}})
	items := first.Body["items"].([]any)
	open, bet := items[0].(map[string]any), items[1].(map[string]any)
	if open["direction"] != "CREDIT" || open["money"].(map[string]any)["amount"] != "1000.00" || open["balanceBefore"].(map[string]any)["amount"] != "0.00" ||
		open["balanceAfter"].(map[string]any)["amount"] != "1000.00" || open["walletId"] != w.ID {
		t.Errorf("opening entry: %v", open)
	}
	if bet["direction"] != "DEBIT" || bet["money"].(map[string]any)["amount"] != "10.00" || bet["balanceAfter"].(map[string]any)["amount"] != "990.00" ||
		bet["money"].(map[string]any)["currency"] != "BRL" {
		t.Errorf("bet entry: %v", bet)
	}
	if strings.Contains(first.Raw, other.ID) {
		t.Error("the page leaks another wallet")
	}
	if _, err := time.Parse(time.RFC3339Nano, open["createdAt"].(string)); err != nil {
		t.Errorf("createdAt: %v", err)
	}
	if _, has := open["seq"]; has {
		t.Error("seq is internal: only the opaque cursor exposes the position")
	}
}

func TestLedgerRejectsInvalidParametersAndForeignCursorsWithoutWidening(t *testing.T) {
	a := startApp(t, testConfig())
	tok := token(t, "internal-service")
	w := openWallet(t, a, "100.00")
	other := openWallet(t, a, "100.00")
	submitBET(t, a, w, "1.00")
	submitBET(t, a, other, "1.00")
	// Um cursor legítimo da OUTRA carteira.
	foreign := pageOf(t, getLedger(t, a, tok, other.ID, url.Values{"limit": {"1"}})).next
	if foreign == "" {
		t.Fatal("setup: no cursor")
	}
	own := pageOf(t, getLedger(t, a, tok, w.ID, url.Values{"limit": {"1"}})).next

	expect := func(name string, r response, status int, code string) {
		t.Helper()
		if r.Status != status || r.str("code") != code {
			t.Errorf("%s: %d %s, want %d %s", name, r.Status, r.Raw, status, code)
		}
		if strings.Contains(r.Raw, "items") {
			t.Errorf("%s: an error must carry no ledger data: %s", name, r.Raw)
		}
	}
	for _, lim := range []string{"0", "-1", "101", "1000", "99999", "abc", "1.5", "", "+5", " 5", "0x10", "٣", "1e1"} {
		expect("limit="+lim, getLedger(t, a, tok, w.ID, url.Values{"limit": {lim}}), 400, "INVALID_LIMIT")
	}
	expect("repeated limit", call(t, a.URL, "GET", "/wallets/"+w.ID+"/ledger?limit=1&limit=2", tok, nil, nil), 400, "INVALID_LIMIT")
	for name, c := range map[string]string{
		"foreign wallet": foreign, "garbage": "not-a-cursor", "padded": own + "=", "truncated": own[:len(own)-3], "tampered": own[:len(own)-2] + "AA",
		"json only":       "e30",
		"huge":            strings.Repeat("A", 5000),
		"unicode escape":  "%E2%82%AC",
		"empty-looking":   "%20",
		"another version": "eyJ2IjoyLCJ3IjoiMDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAwIiwicyI6MX0",
	} {
		expect("cursor "+name, call(t, a.URL, "GET", "/wallets/"+w.ID+"/ledger?cursor="+c, tok, nil, nil), 400, "INVALID_CURSOR")
	}
	expect("empty cursor", call(t, a.URL, "GET", "/wallets/"+w.ID+"/ledger?cursor=", tok, nil, nil), 400, "INVALID_CURSOR")
	expect("repeated cursor", call(t, a.URL, "GET", "/wallets/"+w.ID+"/ledger?cursor="+own+"&cursor="+own, tok, nil, nil), 400, "INVALID_CURSOR")
	expect("invalid wallet id", call(t, a.URL, "GET", "/wallets/not-a-uuid/ledger", tok, nil, nil), 400, "INVALID_REQUEST")
	expect("unknown wallet", getLedger(t, a, tok, uuid.NewString(), nil), 404, "WALLET_NOT_FOUND")
	// O cursor próprio continua válido (nada foi "consumido" pelas rejeições).
	if p := pageOf(t, getLedger(t, a, tok, w.ID, url.Values{"cursor": {own}})); len(p.ids) != 1 {
		t.Errorf("own cursor: %v", p.ids)
	}
}

func TestLedgerOfAnEmptyWalletIsAnEmptyListAndOfAnUnknownWalletIsNotFound(t *testing.T) {
	a := startApp(t, testConfig())
	tok := token(t, "internal-service")
	w := openWallet(t, a, "0.00") // saldo zero: sem OPENING nem lançamentos
	r := getLedger(t, a, tok, w.ID, nil)
	if r.Status != 200 || !strings.Contains(strings.ReplaceAll(r.Raw, " ", ""), `"items":[]`) || r.str("nextCursor") != "" {
		t.Fatalf("empty ledger: %d %s", r.Status, r.Raw)
	}
}

// Timestamps iguais e lacunas de seq são válidos: o cursor usa seq, nunca created_at.
func TestLedgerPaginatesEntriesWithEqualTimestampsAndSeqGaps(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock() // não avança: todos os lançamentos têm o mesmo created_at
	svc, _ := ledgerSvc(t, clock)
	wID, pID := svcOpen(t, svc, "500.00")
	oID, oP := svcOpen(t, svc, "500.00")
	id := scoped()
	for i := 0; i < 9; i++ {
		mustSubmit(t, svc, op(wID, pID, "BET", id(fmt.Sprint("a", i)), "5.00", ""))
		mustSubmit(t, svc, op(oID, oP, "BET", id(fmt.Sprint("b", i)), "5.00", "")) // intercala: lacunas na seq da carteira
	}
	want := ledgerIDsInDB(t, wID)
	var seqs []int64
	rows, err := testPool.Query(ctx, `SELECT seq FROM wallet_ledger_entries WHERE wallet_id = $1 ORDER BY seq`, wID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var s int64
		_ = rows.Scan(&s)
		seqs = append(seqs, s)
	}
	rows.Close()
	gaps := 0
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			gaps++
		}
	}
	if gaps == 0 {
		t.Fatalf("setup: expected gaps in seq %v", seqs)
	}
	wid := uuid.MustParse(wID)
	var got []string
	var stamps = map[time.Time]int{}
	cursor := ""
	for pages := 0; ; pages++ {
		p, err := svc.ListLedger(ctx, wid, cursor, 4)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range p.Items {
			got = append(got, e.ID().String())
			stamps[e.CreatedAt()]++
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
		if pages > 50 {
			t.Fatal("does not terminate")
		}
	}
	if !sameIDs(got, want) {
		t.Fatalf("walk with equal timestamps lost or duplicated entries:\n got %v\nwant %v", got, want)
	}
	if len(stamps) != 1 {
		t.Fatalf("setup: expected a single created_at, got %d", len(stamps))
	}
}

// Lançamentos confirmados no meio da navegação aparecem nas páginas seguintes, sem duplicar nem perder o que já era
// navegável; nenhuma transação fica aberta entre as requisições.
func TestLedgerWalkSeesNewEntriesWithoutLosingOrDuplicating(t *testing.T) {
	a := startApp(t, testConfig())
	tok := token(t, "internal-service")
	w := openWallet(t, a, "1000.00")
	for i := 0; i < 5; i++ {
		submitBET(t, a, w, "1.00")
	}
	before := ledgerIDsInDB(t, w.ID) // 6
	p1 := pageOf(t, getLedger(t, a, tok, w.ID, url.Values{"limit": {"4"}}))
	if len(p1.ids) != 4 || p1.next == "" {
		t.Fatalf("page 1: %v %q", p1.ids, p1.next)
	}
	idle := queryInt(t, `SELECT count(*) FROM pg_stat_activity WHERE state LIKE 'idle in transaction%' AND datname = current_database()`)
	if idle != 0 {
		t.Errorf("%d session(s) idle in transaction between pages", idle)
	}
	// Movimentos confirmados entre as páginas.
	for i := 0; i < 3; i++ {
		submitBET(t, a, w, "1.00")
	}
	after := ledgerIDsInDB(t, w.ID) // 9
	rest := []string{}
	cursor := p1.next
	for cursor != "" {
		p := pageOf(t, getLedger(t, a, tok, w.ID, url.Values{"limit": {"4"}, "cursor": {cursor}}))
		rest = append(rest, p.ids...)
		cursor = p.next
	}
	got := append(append([]string{}, p1.ids...), rest...)
	if !sameIDs(got, after) {
		t.Fatalf("walk across new entries\n got %v\nwant %v", got, after)
	}
	if !sameIDs(got[:len(before)], before) {
		t.Fatal("the entries that were already navigable changed")
	}
}

// Escritores paralelos na mesma carteira enquanto o ledger é percorrido: a ordem por seq coincide com a ordem de
// confirmação (lançamentos são gravados sob o lock da carteira), então nenhum item já confirmado é pulado.
func TestLedgerWalkUnderParallelWritersNeverSkipsOrRepeats(t *testing.T) {
	a := startApp(t, testConfig())
	tok := token(t, "internal-service")
	w := openWallet(t, a, "5000.00")
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 15; n++ {
				select {
				case <-stop:
					return
				default:
				}
				submitBET(t, a, w, "1.00")
			}
		}()
	}
	var snapshots [][]string
	for walks := 0; walks < 8; walks++ {
		ids, _ := walkHTTP(t, a, tok, w.ID, 7)
		seen := map[string]bool{}
		for _, id := range ids {
			if seen[id] {
				t.Fatalf("walk %d repeated %s", walks, id)
			}
			seen[id] = true
		}
		snapshots = append(snapshots, ids)
	}
	close(stop)
	wg.Wait()
	final := ledgerIDsInDB(t, w.ID)
	full, _ := walkHTTP(t, a, tok, w.ID, 7)
	if !sameIDs(full, final) {
		t.Fatalf("final walk differs from the database order")
	}
	// Cada travessia parcial é um prefixo do estado final, na ordem: nada confirmado antes dela foi pulado.
	for i, s := range snapshots {
		if !sameIDs(s, final[:len(s)]) {
			t.Fatalf("walk %d is not a prefix of the final order (skipped or reordered entries)", i)
		}
	}
}
