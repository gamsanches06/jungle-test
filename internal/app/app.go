// Package app composes the service with Uber Fx. Constructors are plain Go
// functions; Fx only wires them and manages their lifecycle. Lifecycle hooks
// run in dependency order on start and in reverse order on stop, so inputs
// (HTTP, SQS consumer) stop first, then workers, and the connection pool is
// closed last.
package app

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/auth"
	"github.com/gamsanches06/jungle-test/internal/config"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/infra/postgres"
	"github.com/gamsanches06/jungle-test/internal/infra/sqsx"
	"github.com/gamsanches06/jungle-test/internal/observability"
	"github.com/gamsanches06/jungle-test/internal/transport/httpapi"
	"github.com/gamsanches06/jungle-test/internal/worker"
)

// Options returns the whole application for a validated configuration.
func Options(cfg config.Config, extra ...fx.Option) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.WithLogger(func(l *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: l} }),
		ObservabilityModule,
		PostgresModule,
		SQSModule,
		AuthModule,
		ApplicationModule,
		WorkersModule,
		HTTPModule,
		fx.Options(extra...),
	)
}

// ObservabilityModule provides logging and metrics.
var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger { return observability.NewLogger(cfg.LogLevel, cfg.InstanceID) },
		observability.NewMetrics,
		func(m *observability.Metrics) application.Metrics { return m },
	),
)

// PostgresModule provides the pool (validated on start, closed on stop),
// the transaction manager and the outbox store.
var PostgresModule = fx.Module("postgres",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewDB, fx.As(new(application.TxManager))),
		postgres.NewOutboxStore,
	),
)

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(postgres.PoolConfig{
		URL: cfg.DatabaseURL, MaxConns: cfg.DBMaxConns,
		StatementTimeout: cfg.DBStatementTimeout, LockTimeout: cfg.DBLockTimeout,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error { return postgres.WaitReady(ctx, pool, log) },
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("postgres pool closed")
			return nil
		},
	})
	return pool, nil
}

// SQSModule provides the broker clients, resolved queues, consumer and publisher.
var SQSModule = fx.Module("sqs",
	fx.Provide(
		sqsx.NewClients,
		newQueues,
		sqsx.NewConsumer,
		fx.Annotate(sqsx.NewEventPublisher, fx.As(new(worker.Publisher))),
	),
)

func newQueues(lc fx.Lifecycle, c *sqsx.Clients, cfg config.Config, log *slog.Logger) *sqsx.Queues {
	q := &sqsx.Queues{}
	lc.Append(fx.Hook{OnStart: func(ctx context.Context) error { return q.Resolve(ctx, c, cfg, log) }})
	return q
}

// AuthModule provides the OIDC token verifier.
var AuthModule = fx.Module("auth",
	fx.Provide(
		newVerifier,
		func(v *auth.OIDCVerifier) auth.Verifier { return v },
	),
)

func newVerifier(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) *auth.OIDCVerifier {
	v := auth.NewOIDCVerifier(auth.OIDCConfig{Issuer: cfg.OIDCIssuer, JWKSURL: cfg.OIDCJWKSURL, Audience: cfg.OIDCAudience})
	if cfg.Roles.API {
		lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
			for {
				err := v.CheckReachable(ctx)
				if err == nil {
					return nil
				}
				log.Warn("identity provider not reachable", slog.String("error", err.Error()))
				select {
				case <-ctx.Done():
					return err
				case <-waitRetry():
				}
			}
		}})
	}
	return v
}

// ApplicationModule provides the use cases.
var ApplicationModule = fx.Module("application",
	fx.Provide(
		func() application.Clock { return application.SystemClock{} },
		func() wagering.IDGenerator { return application.UUIDv7{} },
		func(cfg config.Config) wagering.PendingPolicy {
			return wagering.PendingPolicy{
				BaseDelay: cfg.ReferenceBaseDelay, MaxDelay: cfg.ReferenceMaxDelay,
				MaxAttempts: cfg.ReferenceMaxAttempts, TTL: cfg.ReferenceTTL,
			}
		},
		application.NewProcessService,
		application.NewPendingService,
		application.NewWalletService,
		application.NewQueryService,
	),
)

// WorkersModule registers the background components enabled by APP_ROLES.
var WorkersModule = fx.Module("workers",
	fx.Provide(worker.NewOutboxRelay, worker.NewPendingResolver),
	fx.Invoke(registerWorkers),
)

func registerWorkers(lc fx.Lifecycle, cfg config.Config, c *sqsx.Consumer, o *worker.OutboxRelay, p *worker.PendingResolver) {
	if cfg.Roles.Outbox {
		lc.Append(fx.StartStopHook(o.Start, o.Stop))
	}
	if cfg.Roles.Pending {
		lc.Append(fx.StartStopHook(p.Start, p.Stop))
	}
	if cfg.Roles.Consumer {
		lc.Append(fx.StartStopHook(c.Start, c.Stop))
	}
}

// HTTPModule provides the API server. It is registered last, so it is the
// first component stopped.
var HTTPModule = fx.Module("http",
	fx.Provide(
		httpapi.NewHandlers,
		newReadiness,
		httpapi.NewRouter,
		httpapi.NewServer,
	),
	fx.Invoke(func(*httpapi.Server) {}),
)

func newReadiness(pool *pgxpool.Pool, clients *sqsx.Clients, queues *sqsx.Queues) *httpapi.Readiness {
	return httpapi.NewReadiness([]httpapi.Check{
		{Name: "postgres", Fn: func(ctx context.Context) error { return pool.Ping(ctx) }},
		{Name: "sqs", Fn: func(ctx context.Context) error { return sqsx.Ping(ctx, clients, queues) }},
	})
}
