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
