// Package config carrega e valida a configuração a partir de variáveis de
// ambiente. Falhas de configuração impedem a inicialização.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"desafio-tecnico-go/internal/domain"
)

// Config é a configuração validada da aplicação.
type Config struct {
	HTTPAddr        string
	LogLevel        string
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration

	DatabaseURL string
	DBMaxConns  int32

	OIDCIssuer    string
	OIDCJWKSURL   string
	OIDCAudience  string
	OIDCClockSkew time.Duration

	AWSRegion         string
	AWSEndpointURL    string // vazio em AWS real; aponta para o LocalStack localmente
	SQSWagerQueue     string
	AllowedCurrencies []domain.Currency
}

// Load lê o ambiente. Devolve todos os problemas encontrados de uma vez.
func Load() (Config, error) { return load(os.LookupEnv) }

func load(lookup func(string) (string, bool)) (Config, error) {
	var errs []error
	get := func(key, def string) string {
		if v, ok := lookup(key); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return def
	}
	required := func(key string) string {
		v := get(key, "")
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", key))
		}
		return v
	}
	duration := func(key, def string) time.Duration {
		d, err := time.ParseDuration(get(key, def))
		if err != nil || d < 0 {
			errs = append(errs, fmt.Errorf("%s must be a non-negative duration (e.g. 10s)", key))
		}
		return d
	}
	httpURL := func(key, v string) {
		if v == "" {
			return
		}
		u, err := url.Parse(v)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("%s must be an http(s) URL", key))
		}
	}

	c := Config{
		HTTPAddr:        get("HTTP_ADDR", ":8080"),
		LogLevel:        strings.ToLower(get("LOG_LEVEL", "info")),
		RequestTimeout:  duration("REQUEST_TIMEOUT", "10s"),
		ShutdownTimeout: duration("SHUTDOWN_TIMEOUT", "20s"),
		DatabaseURL:     required("DATABASE_URL"),
		OIDCIssuer:      required("OIDC_ISSUER"),
		OIDCJWKSURL:     required("OIDC_JWKS_URL"),
		OIDCAudience:    required("OIDC_AUDIENCE"),
		OIDCClockSkew:   duration("OIDC_CLOCK_SKEW", "5s"),
		AWSRegion:       get("AWS_REGION", "us-east-1"),
		AWSEndpointURL:  get("AWS_ENDPOINT_URL", ""),
		SQSWagerQueue:   get("SQS_WAGER_QUEUE_NAME", "wager-transactions.fifo"),
	}
	httpURL("OIDC_ISSUER", c.OIDCIssuer)
	httpURL("OIDC_JWKS_URL", c.OIDCJWKSURL)
	httpURL("AWS_ENDPOINT_URL", c.AWSEndpointURL)

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, errors.New("LOG_LEVEL must be debug, info, warn or error"))
	}

	n, err := strconv.ParseInt(get("DB_MAX_CONNS", "10"), 10, 32)
	if err != nil || n < 1 || n > 500 {
		errs = append(errs, errors.New("DB_MAX_CONNS must be an integer between 1 and 500"))
	}
	c.DBMaxConns = int32(n)

	for _, code := range strings.Split(get("ALLOWED_CURRENCIES", "BRL"), ",") {
		cur, err := domain.ParseCurrency(strings.TrimSpace(code))
		if err != nil {
			errs = append(errs, fmt.Errorf("ALLOWED_CURRENCIES: %w", err))
			continue
		}
		c.AllowedCurrencies = append(c.AllowedCurrencies, cur)
	}
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return c, nil
}
