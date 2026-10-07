// Package worker contains the background workers: the outbox relay and the
// pending transaction resolver. Both are safe to run in many instances.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/config"
	"github.com/gamsanches06/jungle-test/internal/infra/postgres"
	"github.com/gamsanches06/jungle-test/internal/infra/sqsx"
	"github.com/gamsanches06/jungle-test/internal/observability"
	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

// Publisher publishes events to the broker.
type Publisher interface {
	// PublishBatch sends up to sqsx.MaxBatch events; the map holds the
	// events rejected individually by the broker.
	PublishBatch(ctx context.Context, evs []sqsx.OutgoingEvent) (map[string]error, error)
}

// OutboxRelay publishes committed outbox events. Events are only visible to
// it after the SQL transaction that created them committed, so nothing is
// published before the commit.
type OutboxRelay struct {
	cfg       config.Config
	store     *postgres.OutboxStore
	publisher Publisher
	metrics   *observability.Metrics
	log       *slog.Logger
	loop      *Loop
	lastLag   time.Time
}

// NewOutboxRelay builds the relay.
func NewOutboxRelay(cfg config.Config, store *postgres.OutboxStore, p Publisher, m *observability.Metrics, log *slog.Logger) *OutboxRelay {
	return &OutboxRelay{cfg: cfg, store: store, publisher: p, metrics: m, log: log.With(slog.String("component", "outbox-relay"))}
}

// Start launches the loop.
func (r *OutboxRelay) Start() {
	r.loop = StartLoop("outbox-relay", r.run)
	r.log.Info("outbox relay started", slog.String("owner", r.cfg.InstanceID))
}

// Stop finishes the publication in progress, then releases the leases of
// claimed events that were not published so another instance takes them.
func (r *OutboxRelay) Stop(ctx context.Context) error {
	if r.loop == nil {
		return nil
	}
	err := r.loop.Stop(ctx)
	rctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if n, rerr := r.store.Release(rctx, r.cfg.InstanceID); rerr == nil && n > 0 {
		r.log.Info("released outbox leases", slog.Int64("events", n))
	}
	r.log.Info("outbox relay stopped")
	return err
}

// Done is closed when the loop has terminated.
func (r *OutboxRelay) Done() <-chan struct{} {
	if r.loop == nil {
		return closedChan
	}
	return r.loop.Done()
}

func (r *OutboxRelay) run(ctx context.Context) {
	for ctx.Err() == nil {
		n := r.tick(ctx)
		if n == 0 {
			sleep(ctx, r.cfg.OutboxPollInterval)
		}
	}
}

// RunOnce claims and publishes one batch (used by tests).
func (r *OutboxRelay) RunOnce(ctx context.Context) int { return r.tick(ctx) }

func (r *OutboxRelay) tick(ctx context.Context) int {
	if time.Since(r.lastLag) > 2*time.Second {
		if count, lag, err := r.store.Lag(ctx); err == nil {
			r.metrics.OutboxPending.Set(float64(count))
			r.metrics.OutboxLagSeconds.Set(lag.Seconds())
			r.lastLag = time.Now()
		}
	}
	recs, err := r.store.Claim(ctx, r.cfg.InstanceID, r.cfg.OutboxBatchSize, r.cfg.OutboxLease)
	if err != nil {
		if ctx.Err() == nil {
			r.log.Warn("outbox claim failed", slog.String("error", err.Error()))
			r.metrics.Retry("outbox-claim")
			sleep(ctx, time.Second)
		}
		return 0
	}
	for start := 0; start < len(recs); start += sqsx.MaxBatch {
		if ctx.Err() != nil {
			// Stop releases the leases of the records not sent yet.
			return start
		}
		chunk := recs[start:min(start+sqsx.MaxBatch, len(recs))]
		if err := failpoint.Inject(failpoint.OutboxAfterClaim); err != nil {
			r.failAll(chunk, err)
			continue
		}
		batch := make([]sqsx.OutgoingEvent, len(chunk))
		for i, rec := range chunk {
			batch[i] = sqsx.OutgoingEvent{EventID: rec.ID.String(), EventType: rec.EventType, GroupKey: rec.GroupKey, Payload: rec.Payload}
		}
		// The send itself is not interrupted by shutdown.
		pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		failed, err := r.publisher.PublishBatch(pctx, batch)
		cancel()
		if err != nil {
			r.failAll(chunk, err)
			continue
		}
		// A crash here leaves the events published but unconfirmed; once the
		// lease expires another publisher resends them with the same eventId.
		_ = failpoint.Inject(failpoint.OutboxAfterPublish)
		var ok []uuid.UUID
		for _, rec := range chunk {
			if ferr, bad := failed[rec.ID.String()]; bad {
				r.fail(rec, ferr)
				continue
			}
			ok = append(ok, rec.ID)
		}
		mctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if err := r.store.MarkPublished(mctx, ok...); err != nil {
			r.log.Warn("outbox confirmation failed; events will be republished with the same eventId",
				slog.Int("events", len(ok)), slog.String("error", err.Error()))
		} else {
			r.metrics.OutboxPublished.Add(float64(len(ok)))
			for _, rec := range chunk {
				if _, bad := failed[rec.ID.String()]; !bad {
					r.metrics.OutboxPublishLag.Observe(time.Since(rec.OccurredAt).Seconds())
				}
			}
		}
		cancel()
	}
	return len(recs)
}

func (r *OutboxRelay) failAll(recs []postgres.OutboxRecord, err error) {
	for _, rec := range recs {
		r.fail(rec, err)
	}
}

func (r *OutboxRelay) fail(rec postgres.OutboxRecord, err error) {
	r.metrics.OutboxFailures.Inc()
	r.metrics.Retry("outbox")
	delay := r.cfg.OutboxRetryBase
	for i := 1; i < rec.Attempts; i++ {
		delay *= 2
		if delay >= r.cfg.OutboxRetryMax {
			delay = r.cfg.OutboxRetryMax
			break
		}
	}
	r.log.Warn("outbox publish failed", slog.String("eventId", rec.ID.String()),
		slog.Int("attempts", rec.Attempts), slog.Duration("retryIn", delay), slog.String("error", err.Error()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if merr := r.store.MarkFailed(ctx, rec.ID, r.cfg.InstanceID, err.Error(), delay); merr != nil {
		r.log.Warn("outbox mark failed error; lease will expire", slog.String("error", merr.Error()))
	}
}
