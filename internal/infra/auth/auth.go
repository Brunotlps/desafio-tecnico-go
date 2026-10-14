// Package auth valida access tokens JWT emitidos por um IdP OAuth 2.0/OIDC
// (Keycloak) e extrai a identidade autenticada.
//
// Modelo de permissões:
//   - realm role "internal-service": serviço interno, autorizado a operar
//     carteiras e consultar qualquer transação;
//   - realm role "provider": provedor de jogos; o providerId autorizado vem
//     exclusivamente da claim "provider_id" do token (mapper do IdP), nunca do
//     corpo da requisição nem de headers.
package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/time/rate"
)

const (
	RoleInternal = "internal-service"
	RoleProvider = "provider"
)

// ErrUnauthenticated: token ausente, malformado, com assinatura, emissor,
// audiência ou validade inválidos.
var ErrUnauthenticated = errors.New("unauthenticated")

// Principal é a identidade autenticada de uma requisição.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	roles      map[string]struct{}
}

func (p Principal) HasRole(role string) bool { _, ok := p.roles[role]; return ok }

// IsInternal informa se a identidade é do serviço interno.
func (p Principal) IsInternal() bool { return p.HasRole(RoleInternal) }

// IsProvider informa se a identidade é de um provedor com providerId definido.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

type claims struct {
	jwt.RegisteredClaims
	AuthorizedParty string `json:"azp"`
	ProviderID      string `json:"provider_id"`
	RealmAccess     struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Verifier valida tokens contra o JWKS do IdP.
type Verifier struct {
	cfg    Config
	keys   keyfunc.Keyfunc
	cancel context.CancelFunc
}

// Config parametriza o verificador.
type Config struct {
	Issuer    string // valor esperado da claim iss
	JWKSURL   string // onde buscar as chaves públicas (pode diferir do issuer em redes de contêineres)
	Audience  string // valor exigido na claim aud
	ClockSkew time.Duration
}

// NewVerifier cria o verificador sem acessar a rede; Start carrega as chaves.
func NewVerifier(cfg Config) *Verifier { return &Verifier{cfg: cfg} }

// Start carrega o JWKS inicial (falha se o IdP estiver inacessível) e passa a
// atualizá-lo em segundo plano até Stop. Chaves desconhecidas disparam
// atualização limitada por taxa.
func (v *Verifier) Start(ctx context.Context) error {
	// O contexto de atualização vive até Stop, independentemente do prazo de ctx.
	refreshCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	failFirst := false
	keys, err := keyfunc.NewDefaultOverrideCtx(refreshCtx, []string{v.cfg.JWKSURL}, keyfunc.Override{
		NoErrorReturnFirstHTTPReq: &failFirst, // IdP inacessível na partida aborta a inicialização
		HTTPTimeout:               5 * time.Second,
		RefreshInterval:           15 * time.Minute,
		// Chave desconhecida (rotação) força uma releitura, no máximo uma a cada 30s,
		// para que tokens forjados com kids aleatórios não sobrecarreguem o IdP.
		// Acima do limite a verificação falha na hora (401), sem reter a requisição.
		RefreshUnknownKID: rate.NewLimiter(rate.Every(30*time.Second), 1),
		RateLimitWaitMax:  time.Millisecond,
	})
	if err != nil {
		cancel()
		return fmt.Errorf("load JWKS from %s: %w", v.cfg.JWKSURL, err)
	}
	v.keys, v.cancel = keys, cancel
	return nil
}

// Stop encerra a atualização em segundo plano.
func (v *Verifier) Stop() {
	if v.cancel != nil {
		v.cancel()
	}
}

// Verify valida assinatura (apenas RS256), emissor, audiência e expiração.
func (v *Verifier) Verify(raw string) (Principal, error) {
	if v.keys == nil {
		return Principal{}, fmt.Errorf("%w: verifier not started", ErrUnauthenticated)
	}
	var c claims
	tok, err := jwt.ParseWithClaims(raw, &c, v.keys.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(v.cfg.Issuer),
		jwt.WithAudience(v.cfg.Audience),
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(v.cfg.ClockSkew),
	)
	if err != nil || !tok.Valid {
		return Principal{}, fmt.Errorf("%w: %v", ErrUnauthenticated, err)
	}
	if c.Subject == "" {
		return Principal{}, fmt.Errorf("%w: missing subject", ErrUnauthenticated)
	}
	p := Principal{Subject: c.Subject, ClientID: c.AuthorizedParty, ProviderID: c.ProviderID,
		roles: make(map[string]struct{}, len(c.RealmAccess.Roles))}
	for _, r := range c.RealmAccess.Roles {
		p.roles[r] = struct{}{}
	}
	return p, nil
}

type ctxKey struct{}

// WithPrincipal anexa a identidade ao contexto.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// PrincipalFrom recupera a identidade do contexto.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
