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
	"desafio-tecnico-go/internal/inbound"
	"desafio-tecnico-go/internal/outbox"
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

	// Publisher da outbox (incremento 3). OutboxPublishers = 0 desliga a publicação neste processo.
	OutboxPublishers      int           // executores por processo (padrão 1)
	OutboxLease           time.Duration // padrão 20s, sem renovação
	OutboxPublishTimeout  time.Duration // padrão 5s: prazo total de uma publicação (inclui as tentativas do SDK)
	OutboxSQLTimeout      time.Duration // padrão 2s: cada transação curta de reserva/confirmação
	OutboxPollInterval    time.Duration // padrão 500ms: espera sem evento elegível
	OutboxBackoffInitial  time.Duration // padrão 1s
	OutboxBackoffMax      time.Duration // padrão 30s
	OutboxAWSProfile      string        // perfil do principal publisher no arquivo de credenciais (padrão wager-publisher)
	SNSEventTopicARN      string        // ARN do tópico FIFO de saída
	OutboxCredentialsFile string        // arquivo de credenciais do perfil do publisher (padrão: AWS_SHARED_CREDENTIALS_FILE)

	// Consumidor SQS (incremento 4). SQSConsumers = 0 desliga a entrada por SQS neste processo.
	SQSConsumers           int           // executores por processo (padrão 4)
	ConsumerName           string        // estável entre instâncias e reinícios (padrão wager-consumer; nunca hostname/PID)
	SQSDLQName             string        // DLQ (encaminhamento manual de mensagens permanentes)
	ConsumerReceiveWait    time.Duration // long polling (padrão 10s), cancelável
	ConsumerPhaseTimeout   time.Duration // fase financeira de uma tentativa (padrão 10s)
	ConsumerIOTimeout      time.Duration // cada ação de transporte / diagnóstico (padrão 5s)
	ConsumerBackoffInitial time.Duration // padrão 5s
	ConsumerBackoffMax     time.Duration // padrão 60s
	ConsumerPauseMax       time.Duration // teto da pausa das buscas em falha global (padrão 30s)
	SQSCredentialsFile     string        // opcional: arquivo de credenciais do cliente SQS (padrão: cadeia padrão do SDK, perfil de runtime)
	SQSAWSProfile          string        // perfil nesse arquivo (padrão wager-runtime)

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
		OutboxLease:             duration("OUTBOX_LEASE", "20s"),
		OutboxPublishTimeout:    duration("OUTBOX_PUBLISH_TIMEOUT", "5s"),
		OutboxSQLTimeout:        duration("OUTBOX_SQL_TIMEOUT", "2s"),
		OutboxPollInterval:      duration("OUTBOX_POLL_INTERVAL", "500ms"),
		OutboxBackoffInitial:    duration("OUTBOX_BACKOFF_INITIAL", "1s"),
		OutboxBackoffMax:        duration("OUTBOX_BACKOFF_MAX", "30s"),
		OutboxAWSProfile:        get("OUTBOX_AWS_PROFILE", "wager-publisher"),
		SNSEventTopicARN:        get("SNS_EVENT_TOPIC_ARN", ""),
		OutboxCredentialsFile:   get("OUTBOX_CREDENTIALS_FILE", get("AWS_SHARED_CREDENTIALS_FILE", "")),
		ConsumerName:            get("CONSUMER_NAME", "wager-consumer"),
		SQSDLQName:              get("SQS_DLQ_NAME", "wager-transactions-dlq.fifo"),
		ConsumerReceiveWait:     duration("CONSUMER_RECEIVE_WAIT", "10s"),
		ConsumerPhaseTimeout:    duration("CONSUMER_PHASE_TIMEOUT", "10s"),
		ConsumerIOTimeout:       duration("CONSUMER_IO_TIMEOUT", "5s"),
		ConsumerBackoffInitial:  duration("CONSUMER_BACKOFF_INITIAL", "5s"),
		ConsumerBackoffMax:      duration("CONSUMER_BACKOFF_MAX", "60s"),
		ConsumerPauseMax:        duration("CONSUMER_PAUSE_MAX", "30s"),
		SQSCredentialsFile:      get("SQS_CREDENTIALS_FILE", ""),
		SQSAWSProfile:           get("SQS_AWS_PROFILE", "wager-runtime"),
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

	consumers, err := strconv.ParseInt(get("SQS_CONSUMERS", "4"), 10, 32)
	if err != nil {
		errs = append(errs, errors.New("SQS_CONSUMERS must be an integer between 0 and 16"))
	}
	c.SQSConsumers = int(consumers)

	publishers, err := strconv.ParseInt(get("OUTBOX_PUBLISHERS", "1"), 10, 32)
	if err != nil {
		errs = append(errs, errors.New("OUTBOX_PUBLISHERS must be an integer between 0 and 8"))
	}
	c.OutboxPublishers = int(publishers)

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
	errs = append(errs, c.validateOutbox()...)
	errs = append(errs, c.validateConsumer()...)
	return errors.Join(errs...)
}

// validateOutbox confere o publisher da outbox. Com OutboxPublishers = 0 nada é exigido (publicação desligada), de modo que
// configurações montadas diretamente sem os campos do publisher continuam válidas.
func (c Config) validateOutbox() []error {
	if c.OutboxPublishers == 0 {
		return nil
	}
	var errs []error
	pc := outbox.Config{
		Workers: c.OutboxPublishers, Lease: c.OutboxLease, PublishTimeout: c.OutboxPublishTimeout, SQLTimeout: c.OutboxSQLTimeout,
		PollInterval: c.OutboxPollInterval, BackoffInitial: c.OutboxBackoffInitial, BackoffMax: c.OutboxBackoffMax,
	}
	if err := pc.Validate(); err != nil {
		errs = append(errs, fmt.Errorf("outbox: %w", err))
	}
	if !strings.HasPrefix(c.SNSEventTopicARN, "arn:aws:sns:") {
		errs = append(errs, errors.New("SNS_EVENT_TOPIC_ARN must be the ARN of the event topic when OUTBOX_PUBLISHERS > 0"))
	}
	if c.OutboxCredentialsFile == "" || c.OutboxAWSProfile == "" {
		errs = append(errs, errors.New("OUTBOX_CREDENTIALS_FILE (or AWS_SHARED_CREDENTIALS_FILE) and OUTBOX_AWS_PROFILE are required when OUTBOX_PUBLISHERS > 0 (the publisher uses its own principal)"))
	}
	if c.OutboxPublishTimeout >= c.ShutdownDrain {
		errs = append(errs, errors.New("OUTBOX_PUBLISH_TIMEOUT must be shorter than SHUTDOWN_DRAIN_TIMEOUT so an admitted publication drains"))
	}
	if reserve := c.ShutdownTimeout - c.ShutdownDrain; c.OutboxSQLTimeout >= reserve {
		errs = append(errs, errors.New("OUTBOX_SQL_TIMEOUT must be shorter than the shutdown reserve (SHUTDOWN_TIMEOUT - SHUTDOWN_DRAIN_TIMEOUT)"))
	}
	return errs
}

// ConsumerConfig devolve a configuração do consumidor derivada desta configuração.
func (c Config) ConsumerConfig() inbound.Config {
	return inbound.Config{
		Workers: c.SQSConsumers, ConsumerName: c.ConsumerName, SourceQueue: c.SQSWagerQueue, ReceiveWait: c.ConsumerReceiveWait,
		PhaseTimeout: c.ConsumerPhaseTimeout, IOTimeout: c.ConsumerIOTimeout, BackoffInitial: c.ConsumerBackoffInitial,
		BackoffMax: c.ConsumerBackoffMax, PauseMax: c.ConsumerPauseMax, Visibility: QueueVisibility,
	}
}

// QueueVisibility é o visibility timeout provisionado da fila de entrada (broker.DefaultVisibility); só serve à validação.
const QueueVisibility = 60 * time.Second

// validateConsumer confere o consumidor SQS. Com SQSConsumers = 0 nada é exigido (entrada por SQS desligada).
func (c Config) validateConsumer() []error {
	if c.SQSConsumers == 0 {
		return nil
	}
	var errs []error
	if err := c.ConsumerConfig().Validate(); err != nil {
		errs = append(errs, fmt.Errorf("consumer: %w", err))
	}
	if c.SQSDLQName == "" {
		errs = append(errs, errors.New("SQS_DLQ_NAME is required when SQS_CONSUMERS > 0"))
	}
	if c.ConsumerPhaseTimeout >= c.ShutdownTimeout {
		errs = append(errs, errors.New("CONSUMER_PHASE_TIMEOUT must be shorter than SHUTDOWN_TIMEOUT"))
	}
	if reserve := c.ShutdownTimeout - c.ShutdownDrain; c.ConsumerIOTimeout >= reserve {
		errs = append(errs, errors.New("CONSUMER_IO_TIMEOUT must be shorter than the shutdown reserve (SHUTDOWN_TIMEOUT - SHUTDOWN_DRAIN_TIMEOUT)"))
	}
	return errs
}
