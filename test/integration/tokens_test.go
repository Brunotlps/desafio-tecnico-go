//go:build integration

package integration

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var httpClient = &http.Client{Timeout: 30 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 100}}

// --- tokens reais do Keycloak ---------------------------------------------------------

var (
	tokenMu    sync.Mutex
	tokenCache = map[string]cachedToken{}
)

type cachedToken struct {
	value   string
	expires time.Time
}

var clientSecrets = map[string]string{
	"internal-service":      "internal-service-dev-secret",
	"provider-a":            "provider-a-dev-secret",
	"provider-b":            "provider-b-dev-secret",
	"provider-a-shortlived": "provider-a-shortlived-dev-secret",
	"no-roles-client":       "no-roles-dev-secret",
	"wrong-audience-client": "wrong-audience-dev-secret",
}

func fetchToken(t testing.TB, clientID string) string {
	t.Helper()
	resp, err := httpClient.PostForm(env.tokenURL, url.Values{
		"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {clientSecrets[clientID]},
	})
	if err != nil {
		t.Fatalf("token request (is Keycloak up?): %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
	}
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &body); err != nil || body.AccessToken == "" {
		t.Fatalf("no access token for %s: %s", clientID, raw)
	}
	return body.AccessToken
}

// token devolve um access token (reutilizado por 2 minutos).
func token(t testing.TB, clientID string) string {
	t.Helper()
	tokenMu.Lock()
	defer tokenMu.Unlock()
	if c, ok := tokenCache[clientID]; ok && time.Now().Before(c.expires) {
		return c.value
	}
	v := fetchToken(t, clientID)
	tokenCache[clientID] = cachedToken{value: v, expires: time.Now().Add(2 * time.Minute)}
	return v
}

func b64(v any) string {
	raw, _ := json.Marshal(v)
	return base64.RawURLEncoding.EncodeToString(raw)
}

// forgedTokens produz tokens que parecem legítimos, mas não são.
func forgedTokens(t *testing.T) map[string]string {
	real := token(t, "provider-a")
	parts := strings.Split(real, ".")
	var claims map[string]any
	raw, _ := base64.RawURLEncoding.DecodeString(parts[1])
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatal(err)
	}
	// Mesmas claims, mas trocando o provedor e mantendo a assinatura original.
	tampered := map[string]any{}
	for k, v := range claims {
		tampered[k] = v
	}
	tampered["provider_id"] = "provider-b"

	// Assinado com uma chave RSA que o IdP não conhece (kid inexistente e kid copiado).
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signed := func(method jwt.SigningMethod, k any, kid string) string {
		tok := jwt.NewWithClaims(method, jwt.MapClaims(claims))
		tok.Header["kid"] = kid
		s, err := tok.SignedString(k)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	var realHeader map[string]any
	rawH, _ := base64.RawURLEncoding.DecodeString(parts[0])
	_ = json.Unmarshal(rawH, &realHeader)
	realKid, _ := realHeader["kid"].(string)

	return map[string]string{
		"garbage":                  "not-a-jwt",
		"tampered payload":         parts[0] + "." + b64(tampered) + "." + parts[2],
		"signature removed":        parts[0] + "." + parts[1] + ".",
		"unknown signing key":      signed(jwt.SigningMethodRS256, key, "unknown-kid"),
		"copied kid, wrong key":    signed(jwt.SigningMethodRS256, key, realKid),
		"alg none":                 b64(map[string]any{"alg": "none", "typ": "JWT"}) + "." + parts[1] + ".",
		"HS256 with arbitrary key": signed(jwt.SigningMethodHS256, []byte("secret"), realKid),
	}
}
