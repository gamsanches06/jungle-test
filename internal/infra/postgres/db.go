// Package postgres implements the persistence ports with pgx and explicit
// SQL. Transactions, row locks and constraint handling are visible here.
package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gamsanches06/jungle-test/internal/application"
)

// PoolConfig configures the connection pool.
type PoolConfig struct {
	URL              string
	MaxConns         int32
	StatementTimeout time.Duration
	LockTimeout      time.Duration
	ConnectTimeout   time.Duration
}

// NewPool creates a lazily connected pool. Session settings bound how long a
// statement or a row-lock wait may take, and abort transactions left idle by
// a crashed client so their locks are released.
func NewPool(cfg PoolConfig) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if cfg.ConnectTimeout > 0 {
		pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	rp := pc.ConnConfig.RuntimeParams
	rp["application_name"] = "jungle-test"
	if cfg.StatementTimeout > 0 {
		rp["statement_timeout"] = fmt.Sprintf("%d", cfg.StatementTimeout.Milliseconds())
	}
	if cfg.LockTimeout > 0 {
		rp["lock_timeout"] = fmt.Sprintf("%d", cfg.LockTimeout.Milliseconds())
	}
	rp["idle_in_transaction_session_timeout"] = "30000"
	pc.HealthCheckPeriod = 5 * time.Second
	return pgxpool.NewWithConfig(context.Background(), pc)
}

// WaitReady pings the database until it answers or ctx expires.
func WaitReady(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	var last error
	for {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		last = pool.Ping(pctx)
		cancel()
		if last == nil {
			return nil
		}
		log.WarnContext(ctx, "postgres not ready", slog.String("error", last.Error()))
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres not ready: %w", last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// DB is the TxManager implementation.
type DB struct {
	pool       *pgxpool.Pool
	metrics    application.Metrics
	maxRetries int
}

// NewDB builds the transaction manager.
func NewDB(pool *pgxpool.Pool, metrics application.Metrics) *DB {
	return &DB{pool: pool, metrics: metrics, maxRetries: 5}
}

// Pool exposes the pool for health checks and the outbox store.
func (d *DB) Pool() *pgxpool.Pool { return d.pool }

var _ application.TxManager = (*DB)(nil)

// InTx runs fn in a READ COMMITTED transaction; wallet rows are serialized
// with SELECT ... FOR UPDATE inside fn. Serialization failures and
// deadlocks are retried with jittered backoff.
func (d *DB) InTx(ctx context.Context, fn func(context.Context, application.Repositories) error) error {
	return d.withRetry(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

// InSnapshot runs fn in a REPEATABLE READ READ ONLY transaction.
func (d *DB) InSnapshot(ctx context.Context, fn func(context.Context, application.Repositories) error) error {
	return d.withRetry(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (d *DB) withRetry(ctx context.Context, opts pgx.TxOptions, fn func(context.Context, application.Repositories) error) error {
	for attempt := 1; ; attempt++ {
		err := d.run(ctx, opts, fn)
		if err == nil {
			return nil
		}
		if isRetryableTx(err) && attempt < d.maxRetries {
			d.metrics.ConcurrencyConflict("sql")
			d.metrics.Retry("sql")
			backoff := time.Duration(attempt*attempt)*10*time.Millisecond + time.Duration(rand.IntN(10))*time.Millisecond
			select {
			case <-ctx.Done():
				return classify(ctx.Err())
			case <-time.After(backoff):
			}
			continue
		}
		return classify(err)
	}
}

func (d *DB) run(ctx context.Context, opts pgx.TxOptions, fn func(context.Context, application.Repositories) error) (err error) {
	tx, err := d.pool.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			// Rollback uses a fresh context so cancellation still releases locks.
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
			_ = tx.Rollback(rctx)
			cancel()
		}
	}()
	if err = fn(ctx, &repos{q: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// querier is the subset of pgx.Tx used by the repositories.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type repos struct{ q querier }

func (r *repos) Wallets() application.WalletRepository           { return walletRepo{r.q} }
func (r *repos) Transactions() application.TransactionRepository { return txRepo{r.q} }
func (r *repos) Ledger() application.LedgerRepository            { return ledgerRepo{r.q} }
func (r *repos) Outbox() application.OutboxRepository            { return outboxRepo{r.q} }
func (r *repos) Inbox() application.InboxRepository              { return inboxRepo{r.q} }
