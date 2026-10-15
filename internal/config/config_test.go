package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

func valid() map[string]string {
	return map[string]string{
		"DATABASE_URL":  "postgres://u:p@localhost:5432/db",
		"OIDC_ISSUER":   "http://localhost:8081/realms/wager",
		"OIDC_JWKS_URL": "http://localhost:8081/realms/wager/protocol/openid-connect/certs",
		"OIDC_AUDIENCE": "wager-api",
		// O publisher da outbox vem ligado por padrão e exige destino e credenciais próprias.
		"SNS_EVENT_TOPIC_ARN":         "arn:aws:sns:us-east-1:000000000000:wager-events.fifo",
		"AWS_SHARED_CREDENTIALS_FILE": "/run/broker/credentials",
	}
}

func TestLoadDefaults(t *testing.T) {
	c, err := load(env(valid()))
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.DBMaxConns != 10 || c.RequestTimeout != 10*time.Second ||
		len(c.AllowedCurrencies) != 1 || c.AllowedCurrencies[0] != "BRL" || c.SQSWagerQueue != "wager-transactions.fifo" {
		t.Fatalf("unexpected defaults: %+v", c)
	}
}

func TestLoadReportsEveryProblem(t *testing.T) {
	_, err := load(env(map[string]string{
		"OIDC_ISSUER": "not a url", "DB_MAX_CONNS": "0", "ALLOWED_CURRENCIES": "BRL,usd", "REQUEST_TIMEOUT": "soon",
	}))
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL is required", "OIDC_JWKS_URL is required", "OIDC_AUDIENCE is required",
		"OIDC_ISSUER must be an http(s) URL", "DB_MAX_CONNS", "ALLOWED_CURRENCIES", "REQUEST_TIMEOUT"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
}

func TestLoadApprovedDefaultsForReferencesAndShutdown(t *testing.T) {
	c, err := load(env(valid()))
	if err != nil {
		t.Fatal(err)
	}
	if c.ReferenceTTL != 15*time.Minute || c.ReferenceBackoffInitial != time.Second || c.ReferenceBackoffMax != 30*time.Second ||
		c.ReferencePhaseTimeout != 10*time.Second || c.ReferenceWorkers != 1 ||
		c.ShutdownTimeout != 20*time.Second || c.ShutdownDrain != 12*time.Second || c.DBMaxConns != 10 {
		t.Fatalf("defaults: %+v", c)
	}
	if reserve := c.ShutdownTimeout - c.ShutdownDrain; reserve < 8*time.Second {
		t.Fatalf("shutdown reserve = %v, want >= 8s", reserve)
	}
}

func TestLoadValidatesTheRelationsBetweenDeadlines(t *testing.T) {
	for name, tc := range map[string]struct {
		set  map[string]string
		want string
	}{
		"drain not shorter than the global deadline": {map[string]string{"SHUTDOWN_DRAIN_TIMEOUT": "20s"}, "SHUTDOWN_DRAIN_TIMEOUT must be shorter than SHUTDOWN_TIMEOUT"},
		"backoff initial above its ceiling":          {map[string]string{"REFERENCE_BACKOFF_INITIAL": "40s"}, "REFERENCE_BACKOFF_INITIAL must not exceed REFERENCE_BACKOFF_MAX"},
		"backoff initial above the TTL":              {map[string]string{"REFERENCE_TTL": "500ms", "REFERENCE_BACKOFF_INITIAL": "1s", "REFERENCE_BACKOFF_MAX": "2s"}, "REFERENCE_BACKOFF_INITIAL must not exceed REFERENCE_TTL"},
		"phase timeout not shorter than shutdown":    {map[string]string{"REFERENCE_PHASE_TIMEOUT": "20s"}, "REFERENCE_PHASE_TIMEOUT must be shorter than SHUTDOWN_TIMEOUT"},
		"more than one executor":                     {map[string]string{"REFERENCE_WORKERS": "2"}, "REFERENCE_WORKERS must be 0 or 1"},
		"zero TTL":                                   {map[string]string{"REFERENCE_TTL": "0s"}, "REFERENCE_TTL must be greater than zero"},
	} {
		m := valid()
		for k, v := range tc.set {
			m[k] = v
		}
		_, err := load(env(m))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
}

func TestLoadApprovedOutboxDefaults(t *testing.T) {
	c, err := load(env(valid()))
	if err != nil {
		t.Fatal(err)
	}
	if c.OutboxPublishers != 1 || c.OutboxLease != 20*time.Second || c.OutboxPublishTimeout != 5*time.Second ||
		c.OutboxSQLTimeout != 2*time.Second || c.OutboxBackoffInitial != time.Second || c.OutboxBackoffMax != 30*time.Second ||
		c.OutboxAWSProfile != "wager-publisher" {
		t.Fatalf("outbox defaults: %+v", c)
	}
}

func TestLoadValidatesTheOutboxPublisher(t *testing.T) {
	for name, tc := range map[string]struct {
		set  map[string]string
		want string
	}{
		"no topic":               {map[string]string{"SNS_EVENT_TOPIC_ARN": ""}, "SNS_EVENT_TOPIC_ARN must be the ARN"},
		"not an arn":             {map[string]string{"SNS_EVENT_TOPIC_ARN": "wager-events.fifo"}, "SNS_EVENT_TOPIC_ARN must be the ARN"},
		"no credentials file":    {map[string]string{"AWS_SHARED_CREDENTIALS_FILE": ""}, "OUTBOX_CREDENTIALS_FILE (or AWS_SHARED_CREDENTIALS_FILE) and OUTBOX_AWS_PROFILE are required"},
		"too many publishers":    {map[string]string{"OUTBOX_PUBLISHERS": "9"}, "outbox workers must be between 0 and 8"},
		"not a number":           {map[string]string{"OUTBOX_PUBLISHERS": "many"}, "OUTBOX_PUBLISHERS must be an integer"},
		"lease too tight":        {map[string]string{"OUTBOX_LEASE": "9s"}, "outbox lease must exceed"},
		"zero publish timeout":   {map[string]string{"OUTBOX_PUBLISH_TIMEOUT": "0s"}, "outbox publish timeout must be greater than zero"},
		"backoff inverted":       {map[string]string{"OUTBOX_BACKOFF_INITIAL": "1m"}, "outbox backoff initial must not exceed backoff max"},
		"publish does not drain": {map[string]string{"OUTBOX_PUBLISH_TIMEOUT": "12s", "OUTBOX_LEASE": "60s"}, "OUTBOX_PUBLISH_TIMEOUT must be shorter than SHUTDOWN_DRAIN_TIMEOUT"},
		"sql beyond the reserve": {map[string]string{"OUTBOX_SQL_TIMEOUT": "8s", "OUTBOX_LEASE": "60s"}, "OUTBOX_SQL_TIMEOUT must be shorter than the shutdown reserve"},
	} {
		m := valid()
		for k, v := range tc.set {
			m[k] = v
		}
		_, err := load(env(m))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", name, err, tc.want)
		}
	}
}

func TestADisabledOutboxPublisherNeedsNoDestinationAndDirectConfigsStayValid(t *testing.T) {
	m := valid()
	m["OUTBOX_PUBLISHERS"] = "0"
	delete(m, "SNS_EVENT_TOPIC_ARN")
	delete(m, "AWS_SHARED_CREDENTIALS_FILE")
	m["OUTBOX_LEASE"] = "1s" // sem executores, os prazos do publisher não são exigidos
	if _, err := load(env(m)); err != nil {
		t.Fatalf("publisher disabled: %v", err)
	}
	direct := Config{ShutdownTimeout: 20 * time.Second, ShutdownDrain: 12 * time.Second, ReferenceTTL: time.Minute,
		ReferenceBackoffInitial: time.Second, ReferenceBackoffMax: time.Second, ReferencePollInterval: time.Second,
		ReferencePhaseTimeout: time.Second}
	if err := direct.Validate(); err != nil {
		t.Fatalf("a direct config without publisher fields must stay valid: %v", err)
	}
	direct.OutboxPublishers = 1
	if err := direct.Validate(); err == nil {
		t.Fatal("a direct config that enables the publisher is validated like Load")
	}
}
