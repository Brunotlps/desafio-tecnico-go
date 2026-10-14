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
	ShutdownTimeout time.Duration // prazo global do encerramento (padrão 20s)
	// ShutdownDrain é o limite de drenagem dentro do prazo global (padrão 12s); o restante
	// (padrão 8s) é a reserva compartilhada para cancelamento, cleanup e fechamento das dependências.
	ShutdownDrain time.Duration

	// Referências pendentes e seu worker.
	ReferenceTTL            time.Duration // padrão 15m, desde a primeira persistência
	ReferenceBackoffInitial time.Duration // padrão 1s
	ReferenceBackoffMax     time.Duration // padrão 30s (teto, também do jitter)
	ReferencePollInterval   time.Duration // padrão 1s
	ReferencePhaseTimeout   time.Duration // padrão 10s: locks e commit de uma tentativa
	ReferenceWorkers        int           // executores por processo: 1 (padrão) ou 0 (desligado)

	DatabaseURL string
	DBMaxConns  int32

	OIDCIssuer    string
	OIDCJWKSURL   string
	OIDCAudience  string
	OIDCClockSkew time.Duration

	AWSRegion         string
	AWSEndpointURL    string // vazio em AWS real; aponta para o MiniStack localmente
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
		ShutdownDrain:   duration("SHUTDOWN_DRAIN_TIMEOUT", "12s"),

		ReferenceTTL:            duration("REFERENCE_TTL", "15m"),
		ReferenceBackoffInitial: duration("REFERENCE_BACKOFF_INITIAL", "1s"),
		ReferenceBackoffMax:     duration("REFERENCE_BACKOFF_MAX", "30s"),
		ReferencePollInterval:   duration("REFERENCE_POLL_INTERVAL", "1s"),
		ReferencePhaseTimeout:   duration("REFERENCE_PHASE_TIMEOUT", "10s"),
		DatabaseURL:             required("DATABASE_URL"),
		OIDCIssuer:              required("OIDC_ISSUER"),
		OIDCJWKSURL:             required("OIDC_JWKS_URL"),
		OIDCAudience:            required("OIDC_AUDIENCE"),
		OIDCClockSkew:           duration("OIDC_CLOCK_SKEW", "5s"),
		AWSRegion:               get("AWS_REGION", "us-east-1"),
		AWSEndpointURL:          get("AWS_ENDPOINT_URL", ""),
		SQSWagerQueue:           get("SQS_WAGER_QUEUE_NAME", "wager-transactions.fifo"),
	}
	httpURL("OIDC_ISSUER", c.OIDCIssuer)
	httpURL("OIDC_JWKS_URL", c.OIDCJWKSURL)
	httpURL("AWS_ENDPOINT_URL", c.AWSEndpointURL)

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, errors.New("LOG_LEVEL must be debug, info, warn or error"))
	}

	workers, err := strconv.ParseInt(get("REFERENCE_WORKERS", "1"), 10, 32)
	if err != nil {
		errs = append(errs, errors.New("REFERENCE_WORKERS must be 0 or 1"))
	}
	c.ReferenceWorkers = int(workers)

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
	if len(errs) == 0 {
		if err := c.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return c, nil
}

// Validate confere as relações entre os prazos. É chamada por Load e na composição da
// aplicação (bootstrap), de modo que configurações montadas diretamente também são validadas.
func (c Config) Validate() error {
	var errs []error
	positive := func(name string, d time.Duration) {
		if d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be greater than zero", name))
		}
	}
	positive("SHUTDOWN_TIMEOUT", c.ShutdownTimeout)
	positive("SHUTDOWN_DRAIN_TIMEOUT", c.ShutdownDrain)
	positive("REFERENCE_TTL", c.ReferenceTTL)
	positive("REFERENCE_BACKOFF_INITIAL", c.ReferenceBackoffInitial)
	positive("REFERENCE_BACKOFF_MAX", c.ReferenceBackoffMax)
	positive("REFERENCE_POLL_INTERVAL", c.ReferencePollInterval)
	positive("REFERENCE_PHASE_TIMEOUT", c.ReferencePhaseTimeout)
	if c.ShutdownDrain >= c.ShutdownTimeout {
		errs = append(errs, errors.New("SHUTDOWN_DRAIN_TIMEOUT must be shorter than SHUTDOWN_TIMEOUT: the difference is the reserve for cancellation, cleanup and closing dependencies"))
	}
	if c.ReferenceBackoffInitial > c.ReferenceBackoffMax {
		errs = append(errs, errors.New("REFERENCE_BACKOFF_INITIAL must not exceed REFERENCE_BACKOFF_MAX"))
	}
	if c.ReferenceBackoffInitial > c.ReferenceTTL {
		errs = append(errs, errors.New("REFERENCE_BACKOFF_INITIAL must not exceed REFERENCE_TTL"))
	}
	if c.ReferenceWorkers < 0 || c.ReferenceWorkers > 1 {
		errs = append(errs, errors.New("REFERENCE_WORKERS must be 0 or 1"))
	}
	if c.ReferenceWorkers > 0 && c.ReferencePhaseTimeout >= c.ShutdownTimeout {
		errs = append(errs, errors.New("REFERENCE_PHASE_TIMEOUT must be shorter than SHUTDOWN_TIMEOUT"))
	}
	return errors.Join(errs...)
}
