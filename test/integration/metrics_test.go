//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"desafio-tecnico-go/internal/config"
)

// GET /metrics: somente o serviço interno; todas as categorias com observações REAIS (aplicação completa, PostgreSQL, SQS e SNS reais).

func getMetrics(t *testing.T, a *testApp, client string) response {
	t.Helper()
	tok := ""
	if client != "" {
		tok = token(t, client)
	}
	return call(t, a.URL, "GET", "/metrics", tok, nil, nil)
}

// metric devolve o valor da amostra cuja linha começa com prefix (ex.: `wager_inbox_total{outcome="duplicate"}`); -1 se ausente.
func metric(t *testing.T, raw, prefix string) float64 {
	t.Helper()
	for _, line := range strings.Split(raw, "\n") {
		if strings.HasPrefix(line, prefix+" ") {
			var v float64
			if _, err := fmt.Sscan(strings.TrimPrefix(line, prefix+" "), &v); err != nil {
				t.Fatalf("unreadable sample %q", line)
			}
			return v
		}
	}
	return -1
}

func TestMetricsAreInternalOnlyAndCoverEveryCategoryWithRealObservations(t *testing.T) {
	c := newConsEnv(t)
	a := startApp(t, c.consumerAppConfig(true, func(cfg *config.Config) {
		cfg.ReferenceWorkers = 1
	}))

	// Autorização: sem token 401, provedor 403; a saúde continua pública; só o serviço interno lê.
	if r := getMetrics(t, a, ""); r.Status != http.StatusUnauthorized {
		t.Fatalf("no token: %d", r.Status)
	}
	if r := getMetrics(t, a, "provider-a"); r.Status != http.StatusForbidden || strings.Contains(r.Raw, "wager_") {
		t.Fatalf("a provider must not read the metrics: %d %s", r.Status, r.Raw)
	}
	if r := call(t, a.URL, "GET", "/health/live", "", nil, nil); r.Status != http.StatusOK {
		t.Fatalf("health must stay public: %d", r.Status)
	}

	w := openWallet(t, a, "100.00")
	// HTTP: BET processada; BET rejeitada; replay do HTTP.
	if r := submit(t, a, "provider-a", "k-h1", betBody(w, "provider-a", "h1", "25.00")); r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	submit(t, a, "provider-a", "k-h1", betBody(w, "provider-a", "h1", "25.00")) // replay (HTTP)
	if r := submit(t, a, "provider-a", "k-h2", betBody(w, "provider-a", "h2", "9999.00")); r.Status != 422 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	// SQS: nova; a MESMA mensagem reentregue (duplicata da inbox); outro corpo sob o mesmo messageId (conflito → DLQ);
	// um envelope novo para a mesma operação (replay financeiro, diferente de duplicata).
	b1 := body("m-1", "provider-a", "k-s1", "s1", w.ID, w.PlayerID, "BET", "10.00", "")
	c.send(t, b1, w.ID, "m-1")
	eventually(t, "the first SQS message", 30*time.Second, func() bool { return c.inboxRows(w.ID) == 1 })
	c.adminSend(t, b1, w.ID, "again-1")
	c.adminSend(t, body("m-1", "provider-a", "k-s2", "s2", w.ID, w.PlayerID, "BET", "11.00", ""), w.ID, "conflict-1")
	c.send(t, body("m-2", "provider-a", "k-s1", "s1", w.ID, w.PlayerID, "BET", "10.00", ""), w.ID, "m-2")
	// Referência pendente com o worker ativo: aguarda (retry de referências) e depois é resolvida (resultado de origem references).
	c.send(t, body("m-3", "provider-a", "k-p", "p1", w.ID, w.PlayerID, "REFUND", "5.00", "late-bet"), w.ID, "m-3")
	eventually(t, "the pending reference to be retried at least once", 40*time.Second, func() bool {
		return metric(t, getMetrics(t, a, "internal-service").Raw, `wager_retries_total{component="references",reason="waiting"}`) >= 1
	})
	submit(t, a, "provider-a", "k-late", betBody(w, "provider-a", "late-bet", "5.00"))
	eventually(t, "the pending reference to be resolved", 40*time.Second, func() bool {
		return metric(t, getMetrics(t, a, "internal-service").Raw, `wager_transactions_total{origin="references",kind="REFUND",status="PROCESSED",replay="false"}`) >= 1
	})
	// Reconciliação consistente: nenhuma divergência.
	if r := call(t, a.URL, "POST", "/wallets/"+w.ID+"/reconciliation", token(t, "internal-service"), nil, nil); r.Status != 200 {
		t.Fatalf("reconciliation: %d %s", r.Status, r.Raw)
	}
	eventually(t, "the outbox to drain", 40*time.Second, func() bool { return c.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0 })
	eventually(t, "the DLQ to hold the conflict", 40*time.Second, func() bool {
		return metric(t, getMetrics(t, a, "internal-service").Raw, `wager_dlq_messages_approx{state="visible"}`) >= 1
	})

	r := getMetrics(t, a, "internal-service")
	if r.Status != http.StatusOK {
		t.Fatalf("internal: %d", r.Status)
	}
	raw := r.Raw
	want := map[string]float64{ // mínimos observados; cada categoria do enunciado tem uma observação real
		`wager_transactions_total{origin="http",kind="BET",status="PROCESSED",replay="false"}`: 1,
		`wager_transactions_total{origin="http",kind="BET",status="PROCESSED",replay="true"}`:  1,
		`wager_transactions_total{origin="http",kind="BET",status="REJECTED",replay="false"}`:  1,
		`wager_transactions_total{origin="sqs",kind="BET",status="PROCESSED",replay="false"}`:  1,
		`wager_transactions_total{origin="sqs",kind="BET",status="PROCESSED",replay="true"}`:   1,
		`wager_inbox_total{outcome="new"}`:                                                     2,
		`wager_inbox_total{outcome="duplicate"}`:                                               1,
		`wager_inbox_total{outcome="conflict"}`:                                                1,
		`wager_dlq_forwarded_total{code="MESSAGE_ID_CONFLICT"}`:                                1,
		`wager_outbox_published_total`:                                                         5,
		`wager_processing_seconds_count{component="http_submit"}`:                              3,
		`wager_processing_seconds_count{component="sqs_message"}`:                              4,
		`wager_processing_seconds_count{component="outbox_publish"}`:                           5,
		`wager_collector_up{source="outbox"}`:                                                  1,
		`wager_collector_up{source="dlq"}`:                                                     1,
	}
	for series, min := range want {
		if got := metric(t, raw, series); got < min {
			t.Errorf("%s = %v, want at least %v", series, got, min)
		}
	}
	if got := metric(t, raw, `wager_outbox_pending`); got != 0 {
		t.Errorf("wager_outbox_pending = %v after the drain", got)
	}
	if got := metric(t, raw, `wager_reconciliation_divergences_total`); got != 0 {
		t.Errorf("a consistent reconciliation counted %v divergence(s)", got)
	}
	// Rótulos limitados: nenhum identificador, hash ou texto livre.
	for _, leak := range []string{w.ID, w.PlayerID, "m-1", "m-2", "k-s1", "provider-a"} {
		if strings.Contains(raw, leak) {
			t.Errorf("metrics must not expose %q", leak)
		}
	}
	if re := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}`); re.MatchString(raw) {
		t.Error("a UUID leaked into the metrics")
	}
}

// Backlog da outbox (pendente, reservado e idade) acompanha o estado durável; a divergência REAL aumenta o contador e a falha de
// leitura da DLQ omite o gauge (nunca um zero fictício).
func TestMetricsBacklogDivergenceAndCollectionFailures(t *testing.T) {
	c := newConsEnv(t)
	a := startApp(t, c.consumerAppConfig(false, func(cfg *config.Config) { cfg.SQSDLQName, cfg.SQSConsumers = "", 0 }))
	w := openWallet(t, a, "100.00")
	if r := submit(t, a, "provider-a", "k-b", betBody(w, "provider-a", "b1", "30.00")); r.Status != 200 {
		t.Fatalf("%d %s", r.Status, r.Raw)
	}
	time.Sleep(1100 * time.Millisecond)
	raw := getMetrics(t, a, "internal-service").Raw
	if got := metric(t, raw, "wager_outbox_pending"); got != 4 { // abertura 2 + BET 2, sem publisher
		t.Fatalf("pending = %v, want 4", got)
	}
	if age := metric(t, raw, "wager_outbox_oldest_pending_age_seconds"); age < 1 {
		t.Fatalf("the oldest pending event is at least 1s old, got %v", age)
	}
	if got := metric(t, raw, "wager_outbox_reserved"); got != 0 {
		t.Fatalf("reserved = %v", got)
	}
	// Reservado por outro executor: o gauge acompanha a lease.
	r := mustReserve(t, c.store(), 30*time.Second)
	_ = r
	if got := metric(t, getMetrics(t, a, "internal-service").Raw, "wager_outbox_reserved"); got != 1 {
		t.Fatalf("reserved = %v, want 1", got)
	}
	// Falha da coleta da DLQ (nome ausente): amostras omitidas, fonte marcada como indisponível.
	if strings.Contains(raw, "wager_dlq_messages_approx{") || metric(t, raw, `wager_collector_up{source="dlq"}`) != 0 {
		t.Fatalf("a failed collection must omit the gauge and report the source down:\n%s", raw)
	}

	// Divergência real (fixture do dono, só neste banco exclusivo).
	for _, stmt := range []string{
		`ALTER TABLE wallets DISABLE TRIGGER ctrg_wallets_balance_matches_ledger`,
		`UPDATE wallets SET balance = balance + 500, version = version + 1 WHERE id = '` + w.ID + `'`,
	} {
		if _, err := c.owner.Exec(context.Background(), stmt); err != nil {
			t.Fatal(err)
		}
	}
	if res := call(t, a.URL, "POST", "/wallets/"+w.ID+"/reconciliation", token(t, "internal-service"), nil, nil); res.Status != 200 || res.Body["consistent"] != false {
		t.Fatalf("reconciliation: %d %s", res.Status, res.Raw)
	}
	if got := metric(t, getMetrics(t, a, "internal-service").Raw, "wager_reconciliation_divergences_total"); got != 1 {
		t.Fatalf("divergences = %v, want 1", got)
	}
}
