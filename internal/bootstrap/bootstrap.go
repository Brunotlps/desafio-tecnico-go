// Package bootstrap compõe a aplicação com Uber Fx. Cada fx.Module registra
// suas conexões e componentes no fx.Lifecycle; como o Fx encerra os hooks em
// ordem inversa à de início, o servidor HTTP é parado antes de o pool de
// conexões ser fechado.
package bootstrap

import (
	"context"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"desafio-tecnico-go/internal/app"
	"desafio-tecnico-go/internal/config"
	"desafio-tecnico-go/internal/inbound"
	"desafio-tecnico-go/internal/infra/auth"
	"desafio-tecnico-go/internal/infra/httpapi"
	"desafio-tecnico-go/internal/infra/postgres"
	"desafio-tecnico-go/internal/infra/sns"
	"desafio-tecnico-go/internal/infra/sqs"
	"desafio-tecnico-go/internal/lifecycle"
	"desafio-tecnico-go/internal/outbox"
)

// Options devolve a composição completa da aplicação para a configuração dada.
func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: l} }),
		fx.StopTimeout(cfg.ShutdownTimeout),
		// Falha rápido se as relações entre prazos forem incoerentes (também para
		// configurações montadas diretamente, que não passam por config.Load).
		fx.Invoke(func(cfg config.Config) error { return cfg.Validate() }),
		loggingModule,
		persistenceModule,
		authModule,
		messagingModule,
		outboxModule(cfg),
		consumerModule(cfg),
		applicationModule,
		httpModule,
		lifecycleModule,
	)
}

var loggingModule = fx.Module("logging",
	fx.Provide(func(cfg config.Config) *slog.Logger {
		var level slog.Level
		_ = level.UnmarshalText([]byte(cfg.LogLevel))
		return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	}),
)

var persistenceModule = fx.Module("persistence",
	fx.Provide(
		func(lc fx.Lifecycle, cfg config.Config) *postgres.Database {
			db := postgres.NewDatabase(postgres.PoolConfig{URL: cfg.DatabaseURL, MaxConns: cfg.DBMaxConns})
			// As conexões são adquiridas em Start, que se encarrega de liberá-las se
			// falhar (o Fx não chama OnStop de um hook cujo OnStart falhou).
			lc.Append(fx.Hook{OnStart: db.Start, OnStop: db.Stop})
			return db
		},
		func(db *postgres.Database) *postgres.Store { return postgres.NewStore(db) },
		func(s *postgres.Store) app.UnitOfWork { return s },
	),
)

var authModule = fx.Module("auth",
	fx.Provide(func(lc fx.Lifecycle, cfg config.Config) *auth.Verifier {
		v := auth.NewVerifier(auth.Config{
			Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience, ClockSkew: cfg.OIDCClockSkew,
		})
		lc.Append(fx.Hook{
			OnStart: v.Start,
			OnStop:  func(context.Context) error { v.Stop(); return nil },
		})
		return v
	}),
)

var messagingModule = fx.Module("messaging",
	fx.Provide(func(cfg config.Config) (*sqs.Client, error) {
		sc := sqs.Config{Region: cfg.AWSRegion, EndpointURL: cfg.AWSEndpointURL, QueueName: cfg.SQSWagerQueue,
			CredentialsFile: cfg.SQSCredentialsFile, Profile: cfg.SQSAWSProfile}
		if cfg.SQSConsumers > 0 {
			sc.DLQName = cfg.SQSDLQName // o consumidor encaminha à DLQ: a readiness também a confere
		}
		return sqs.NewClient(context.Background(), sc)
	}),
	// Valida a dependência no início, sem esperar o primeiro /health/ready.
	fx.Invoke(func(lc fx.Lifecycle, c *sqs.Client) {
		lc.Append(fx.Hook{OnStart: c.Ready})
	}),
)

// outboxModule compõe o publisher da outbox quando OUTBOX_PUBLISHERS > 0. O cliente SNS usa o perfil do principal wager-publisher
// (lido do arquivo de credenciais pelo construtor, sem alterar o ambiente do processo); o cliente SQS de readiness continua com
// o perfil de runtime. A readiness do tópico (existência, FIFO e permissão de GetTopicAttributes) é verificada na partida.
func outboxModule(cfg config.Config) fx.Option {
	if cfg.OutboxPublishers == 0 {
		return fx.Options()
	}
	return fx.Module("outbox",
		fx.Provide(
			func(cfg config.Config) (*sns.Publisher, error) {
				return sns.New(context.Background(), sns.Config{
					Region: cfg.AWSRegion, EndpointURL: cfg.AWSEndpointURL, TopicARN: cfg.SNSEventTopicARN,
					CredentialsFile: cfg.OutboxCredentialsFile, Profile: cfg.OutboxAWSProfile,
				})
			},
			func(db *postgres.Database) *postgres.OutboxStore { return postgres.NewOutboxStore(db) },
			func(cfg config.Config, store *postgres.OutboxStore, sink *sns.Publisher, log *slog.Logger) *outbox.Publisher {
				return outbox.New(store, sink, outbox.Config{
					Workers: cfg.OutboxPublishers, Lease: cfg.OutboxLease, PublishTimeout: cfg.OutboxPublishTimeout,
					SQLTimeout: cfg.OutboxSQLTimeout, PollInterval: cfg.OutboxPollInterval,
					BackoffInitial: cfg.OutboxBackoffInitial, BackoffMax: cfg.OutboxBackoffMax,
				}, log)
			},
		),
		fx.Invoke(func(lc fx.Lifecycle, p *sns.Publisher) {
			lc.Append(fx.Hook{OnStart: p.Ready})
		}),
	)
}

// consumerModule compõe o consumidor SQS quando SQS_CONSUMERS > 0. Usa o cliente SQS do runtime (receber, remover, visibilidade e
// envio à DLQ) e o mesmo Service do HTTP (inbox e finanças no mesmo commit).
func consumerModule(cfg config.Config) fx.Option {
	if cfg.SQSConsumers == 0 {
		return fx.Options()
	}
	return fx.Module("consumer", fx.Provide(func(cfg config.Config, svc *app.Service, q *sqs.Client, log *slog.Logger) *inbound.Consumer {
		return inbound.New(svc, q, cfg.ConsumerConfig(), log, inbound.Hooks{})
	}))
}

var applicationModule = fx.Module("application",
	fx.Provide(
		func(uow app.UnitOfWork, store *postgres.Store, cfg config.Config, log *slog.Logger) *app.Service {
			return app.NewService(uow, app.Config{
				AllowedCurrencies: cfg.AllowedCurrencies,
				Reference: app.ReferenceConfig{
					TTL: cfg.ReferenceTTL, BackoffInitial: cfg.ReferenceBackoffInitial, BackoffMax: cfg.ReferenceBackoffMax,
					PollInterval: cfg.ReferencePollInterval, PhaseTimeout: cfg.ReferencePhaseTimeout,
				},
			}, log, app.WithLedgerReader(store))
		},
		func(svc *app.Service, log *slog.Logger) *app.ReferenceWorker { return app.NewReferenceWorker(svc, log) },
	),
)

var httpModule = fx.Module("http",
	fx.Provide(func(cfg config.Config, svc *app.Service, v *auth.Verifier, db *postgres.Database, q *sqs.Client, log *slog.Logger) *httpapi.Server {
		checks := []httpapi.ReadinessCheck{
			{Name: "postgres", Check: db.Ping},
			{Name: "sqs", Check: q.Ready},
		}
		return httpapi.NewServer(cfg.HTTPAddr, httpapi.New(svc, v, checks, log, cfg.RequestTimeout), log)
	}),
	// O servidor é um componente do coordenador (ver lifecycleModule): não tem hook próprio.
)

// lifecycleModule coordena o início e o encerramento dos componentes que admitem trabalho
// (worker de referências e servidor HTTP). O hook do coordenador é registrado depois dos
// hooks das dependências (pool, verificador, SQS), porque o Fx executa os OnStop em ordem
// inversa: os componentes terminam (com confirmação) antes de as dependências serem fechadas.
var lifecycleModule = fx.Module("lifecycle",
	fx.Provide(func(cfg config.Config, log *slog.Logger, store *postgres.Store) *lifecycle.Coordinator {
		c := lifecycle.New(log, cfg.ShutdownDrain)
		c.OnShutdown(store.SetShutdownDeadline)
		return c
	}),
	fx.Invoke(func(p struct {
		fx.In
		Lifecycle fx.Lifecycle
		Config    config.Config
		Coord     *lifecycle.Coordinator
		Worker    *app.ReferenceWorker
		Server    *httpapi.Server
		Publisher *outbox.Publisher `optional:"true"` // só existe com OUTBOX_PUBLISHERS > 0
		Consumer  *inbound.Consumer `optional:"true"` // só existe com SQS_CONSUMERS > 0
	}) {
		lc, cfg, c, worker, server := p.Lifecycle, p.Config, p.Coord, p.Worker, p.Server
		// Ordem de início: os executores (referências e publisher) primeiro; a porta HTTP, que admite entradas, por último.
		if cfg.ReferenceWorkers > 0 {
			c.Add(worker)
		}
		if p.Publisher != nil {
			c.OnShutdown(p.Publisher.SetShutdownDeadline)
			c.Add(p.Publisher)
		}
		if p.Consumer != nil {
			c.OnShutdown(p.Consumer.SetShutdownDeadline)
			c.Add(p.Consumer)
		}
		c.Add(server)
		lc.Append(fx.Hook{OnStart: c.Start, OnStop: c.Stop})
	}),
)
