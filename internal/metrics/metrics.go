// Package metrics expõe as métricas operacionais em texto compatível com Prometheus, sem dependência externa.
//
// Semântica (documentada também no README): os contadores são LOCAIS ao processo e reiniciam com ele; são observações operacionais,
// NÃO contabilidade financeira (o ledger, a inbox e a outbox são a fonte de verdade) e podem perder uma observação entre o commit e
// a emissão. Os rótulos pertencem a conjuntos limitados e conhecidos: nunca walletId, transactionId, messageId, hash, correlationId,
// providerId arbitrário, URL concreta ou texto de erro. Um teto de cardinalidade por métrica agrupa o excedente em "other". Dinheiro
// nunca aparece aqui. Os gauges de backlog da outbox e da DLQ são coletados sob demanda, com prazo; se a coleta falhar, o valor
// é OMITIDO e wager_collector_up{source} = 0 (nunca um zero fictício).
package metrics

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

const maxSeriesPerMetric = 64

type metricDef struct {
	help string
}

var counterDefs = map[string]metricDef{
	"wager_transactions_total":               {"Resultados de operações financeiras observados DEPOIS do commit, por origem, tipo, status e se foi replay financeiro."},
	"wager_inbox_total":                      {"Mensagens SQS por resultado da inbox: new, duplicate (mesmo messageId e hash já concluídos) ou conflict (mesmo messageId, outro corpo)."},
	"wager_concurrency_conflicts_total":      {"Disputas de concorrência esperadas e resolvidas por nova tentativa (unicidades do banco, carteira ocupada, modificação concorrente)."},
	"wager_retries_total":                    {"Novas tentativas por componente e motivo (consumidor SQS, publisher da outbox, referências pendentes)."},
	"wager_dlq_forwarded_total":              {"Envios manuais à DLQ CONFIRMADOS pelo broker, por código de recusa permanente."},
	"wager_reconciliation_divergences_total": {"Divergências reais detectadas pela reconciliação (falha de leitura ou de cálculo NÃO conta como divergência)."},
	"wager_reconciliation_errors_total":      {"Reconciliações que não puderam ser calculadas."},
	"wager_outbox_published_total":           {"Eventos da outbox cuja publicação foi confirmada por este processo."},
}

// Registry guarda os contadores e as durações acumuladas.
type Registry struct {
	mu        sync.Mutex
	counters  map[string]map[string]uint64 // nome → série (rótulos) → valor
	durations map[string]*duration         // componente → soma e contagem
	collect   []Collector
}

type duration struct {
	count uint64
	nanos int64
}

// Sample é uma amostra de gauge coletada sob demanda.
type Sample struct {
	Name, Help, Labels string
	Value              float64
}

// Collector coleta gauges sob demanda (com o prazo do contexto). source nomeia a fonte em wager_collector_up.
type Collector struct {
	Source  string
	Collect func(ctx context.Context) ([]Sample, error)
}

func New() *Registry {
	return &Registry{counters: map[string]map[string]uint64{}, durations: map[string]*duration{}}
}

// AddCollector registra um coletor de gauges.
func (r *Registry) AddCollector(c Collector) {
	r.mu.Lock()
	r.collect = append(r.collect, c)
	r.mu.Unlock()
}

func clean(v string) string {
	if v == "" || len(v) > 48 {
		return "other"
	}
	for _, c := range v {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_') {
			return "other"
		}
	}
	return v
}

// inc soma 1 à série (name, pares chave/valor). Valores fora do formato limitado, ou além do teto de séries, viram "other".
func (r *Registry) inc(name string, kv ...string) {
	if r == nil {
		return
	}
	parts := make([]string, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		parts = append(parts, kv[i]+`="`+clean(kv[i+1])+`"`)
	}
	series := strings.Join(parts, ",")
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.counters[name]
	if m == nil {
		m = map[string]uint64{}
		r.counters[name] = m
	}
	if _, ok := m[series]; !ok && len(m) >= maxSeriesPerMetric {
		overflow := make([]string, 0, len(kv)/2)
		for i := 0; i+1 < len(kv); i += 2 {
			overflow = append(overflow, kv[i]+`="other"`)
		}
		series = strings.Join(overflow, ",")
	}
	m[series]++
}

// --- app.Observer -------------------------------------------------------------------------------------------------------

// Result: resultado de uma operação, DEPOIS do commit. origin: http|sqs; replay: o resultado original de uma operação já registrada.
func (r *Registry) Result(origin, kind, status string, replay bool) {
	r.inc("wager_transactions_total", "origin", origin, "kind", kind, "status", status, "replay", fmt.Sprint(replay))
}

// InboxOutcome: new, duplicate ou conflict.
func (r *Registry) InboxOutcome(outcome string) { r.inc("wager_inbox_total", "outcome", outcome) }

// Conflict: uma disputa de concorrência resolvida por nova tentativa.
func (r *Registry) Conflict(reason string) {
	r.inc("wager_concurrency_conflicts_total", "reason", reason)
}

// Retry: uma nova tentativa de um componente.
func (r *Registry) Retry(component, reason string) {
	r.inc("wager_retries_total", "component", component, "reason", reason)
}

// Reconciliation: result = error (a divergência real usa DivergenceDetected).
func (r *Registry) Reconciliation(result string) {
	if result == "error" {
		r.inc("wager_reconciliation_errors_total")
	}
}

// DivergenceDetected é o ponto ligado a app.WithReconciliationObserver: uma divergência REAL.
func (r *Registry) DivergenceDetected() { r.inc("wager_reconciliation_divergences_total") }

// DLQForwarded: envio manual à DLQ confirmado.
func (r *Registry) DLQForwarded(code string) { r.inc("wager_dlq_forwarded_total", "code", code) }

// OutboxPublished: publicação confirmada pelo publisher.
func (r *Registry) OutboxPublished() { r.inc("wager_outbox_published_total") }

// Duration acumula a duração observada de um componente (soma e contagem; sem percentis).
func (r *Registry) Duration(component string, d time.Duration) {
	if r == nil {
		return
	}
	component = clean(component)
	r.mu.Lock()
	defer r.mu.Unlock()
	x := r.durations[component]
	if x == nil {
		x = &duration{}
		r.durations[component] = x
	}
	x.count++
	x.nanos += d.Nanoseconds()
}

// Render escreve o texto de exposição. Coleta os gauges sob demanda, com o prazo de ctx; falha de coleta omite as amostras e marca
// a fonte como indisponível.
func (r *Registry) Render(ctx context.Context, w io.Writer) error {
	r.mu.Lock()
	names := make([]string, 0, len(counterDefs))
	for n := range counterDefs {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, n := range names {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n", n, counterDefs[n].help, n)
		series := make([]string, 0, len(r.counters[n]))
		for s := range r.counters[n] {
			series = append(series, s)
		}
		sort.Strings(series)
		for _, s := range series {
			if s == "" {
				fmt.Fprintf(&b, "%s %d\n", n, r.counters[n][s])
			} else {
				fmt.Fprintf(&b, "%s{%s} %d\n", n, s, r.counters[n][s])
			}
		}
		if len(series) == 0 && !strings.Contains(n, "_total{") {
			// série sem rótulo ainda não observada: expõe 0 apenas para contadores sem rótulos
			switch n {
			case "wager_reconciliation_divergences_total", "wager_reconciliation_errors_total", "wager_outbox_published_total":
				fmt.Fprintf(&b, "%s 0\n", n)
			}
		}
	}
	comps := make([]string, 0, len(r.durations))
	for c := range r.durations {
		comps = append(comps, c)
	}
	sort.Strings(comps)
	b.WriteString("# HELP wager_processing_seconds Duração acumulada (soma e contagem) do processamento por componente: http_submit, sqs_message, outbox_publish.\n# TYPE wager_processing_seconds summary\n")
	for _, c := range comps {
		d := r.durations[c]
		fmt.Fprintf(&b, "wager_processing_seconds_sum{component=%q} %.6f\nwager_processing_seconds_count{component=%q} %d\n", c, float64(d.nanos)/1e9, c, d.count)
	}
	collectors := append([]Collector(nil), r.collect...)
	r.mu.Unlock()

	b.WriteString("# HELP wager_collector_up 1 se a coleta sob demanda da fonte funcionou nesta leitura; 0 se falhou (as amostras da fonte são omitidas).\n# TYPE wager_collector_up gauge\n")
	help := map[string]string{}
	var samples []Sample
	for _, c := range collectors {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		s, err := c.Collect(cctx)
		cancel()
		if err != nil {
			fmt.Fprintf(&b, "wager_collector_up{source=%q} 0\n", c.Source)
			continue
		}
		fmt.Fprintf(&b, "wager_collector_up{source=%q} 1\n", c.Source)
		for _, x := range s {
			help[x.Name] = x.Help
			samples = append(samples, x)
		}
	}
	sort.SliceStable(samples, func(i, j int) bool { return samples[i].Name < samples[j].Name })
	last := ""
	for _, s := range samples {
		if s.Name != last {
			fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n", s.Name, help[s.Name], s.Name)
			last = s.Name
		}
		if s.Labels == "" {
			fmt.Fprintf(&b, "%s %g\n", s.Name, s.Value)
		} else {
			fmt.Fprintf(&b, "%s{%s} %g\n", s.Name, s.Labels, s.Value)
		}
	}
	_, err := io.WriteString(w, b.String())
	return err
}
