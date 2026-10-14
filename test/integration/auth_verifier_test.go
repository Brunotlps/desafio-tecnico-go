//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"desafio-tecnico-go/internal/infra/auth"
)

// O verificador contra o Keycloak real: identidade derivada do token, e recusa de
// credenciais inválidas, forjadas, de audiência errada e expiradas.
func TestVerifierAgainstKeycloak(t *testing.T) {
	newVerifier := func(mutate func(*auth.Config)) *auth.Verifier {
		cfg := auth.Config{Issuer: env.issuer, JWKSURL: env.jwksURL, Audience: "wager-api", ClockSkew: time.Second}
		if mutate != nil {
			mutate(&cfg)
		}
		v := auth.NewVerifier(cfg)
		if err := v.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(v.Stop)
		return v
	}
	v := newVerifier(nil)

	t.Run("identities come from the token", func(t *testing.T) {
		p, err := v.Verify(token(t, "provider-a"))
		if err != nil {
			t.Fatal(err)
		}
		if !p.IsProvider() || p.IsInternal() || p.ProviderID != "provider-a" || p.ClientID != "provider-a" {
			t.Fatalf("provider-a principal: %+v", p)
		}
		p, err = v.Verify(token(t, "provider-b"))
		if err != nil || p.ProviderID != "provider-b" {
			t.Fatalf("provider-b principal: %+v %v", p, err)
		}
		p, err = v.Verify(token(t, "internal-service"))
		if err != nil || !p.IsInternal() || p.IsProvider() || p.ProviderID != "" {
			t.Fatalf("internal principal: %+v %v", p, err)
		}
		// Autenticado, mas sem papéis: não é interno nem provedor.
		p, err = v.Verify(token(t, "no-roles-client"))
		if err != nil || p.IsInternal() || p.IsProvider() {
			t.Fatalf("no-roles principal: %+v %v", p, err)
		}
	})

	t.Run("invalid credentials are rejected", func(t *testing.T) {
		for name, tok := range forgedTokens(t) {
			if _, err := v.Verify(tok); err == nil {
				t.Errorf("%s: accepted", name)
			}
		}
		if _, err := v.Verify(""); err == nil {
			t.Error("empty token accepted")
		}
		if _, err := v.Verify(fetchToken(t, "wrong-audience-client")); err == nil {
			t.Error("token without the API audience accepted")
		}
		// Emissor diferente do esperado.
		other := newVerifier(func(c *auth.Config) { c.Issuer = "http://localhost:8081/realms/another" })
		if _, err := other.Verify(token(t, "provider-a")); err == nil {
			t.Error("token from another issuer accepted")
		}
		// Audiência diferente da exigida.
		aud := newVerifier(func(c *auth.Config) { c.Audience = "another-api" })
		if _, err := aud.Verify(token(t, "provider-a")); err == nil {
			t.Error("token for another audience accepted")
		}
	})

	t.Run("expired tokens are rejected", func(t *testing.T) {
		short := fetchToken(t, "provider-a-shortlived") // validade de 3 s no realm
		if _, err := v.Verify(short); err != nil {
			t.Fatalf("fresh token rejected: %v", err)
		}
		time.Sleep(5 * time.Second) // 3 s de validade + 1 s de tolerância
		if _, err := v.Verify(short); err == nil {
			t.Error("expired token accepted")
		}
	})

	t.Run("a verifier that was not started rejects everything", func(t *testing.T) {
		unstarted := auth.NewVerifier(auth.Config{Issuer: env.issuer, JWKSURL: env.jwksURL, Audience: "wager-api"})
		if _, err := unstarted.Verify(token(t, "provider-a")); err == nil {
			t.Error("unstarted verifier accepted a token")
		}
	})

	t.Run("start fails when the identity provider is unreachable", func(t *testing.T) {
		bad := auth.NewVerifier(auth.Config{Issuer: env.issuer, JWKSURL: "http://127.0.0.1:1/certs", Audience: "wager-api"})
		err := bad.Start(context.Background())
		if err == nil || !strings.Contains(err.Error(), "JWKS") {
			t.Fatalf("expected a JWKS error, got %v", err)
		}
	})
}
