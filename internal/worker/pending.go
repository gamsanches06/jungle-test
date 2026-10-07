package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/config"
)

// PendingResolver periodically retries PENDING and PENDING_REFERENCE
// transactions. State lives only in PostgreSQL, so after a restart (or on
// any other instance) pending work resumes from next_attempt_at.
type PendingResolver struct {
	cfg      config.Config
	svc      *application.PendingService
	log      *slog.Logger
	loop     *Loop
	failures map[uuid.UUID]int
}

// NewPendingResolver builds the worker.
func NewPendingResolver(cfg config.Config, svc *application.PendingService, log *slog.Logger) *PendingResolver {
	return &PendingResolver{cfg: cfg, svc: svc, log: log.With(slog.String("component", "pending-resolver")), failures: map[uuid.UUID]int{}}
}

// Start launches the loop.
func (p *PendingResolver) Start() {
	p.loop = StartLoop("pending-resolver", p.run)
	p.log.Info("pending resolver started")
}

// Stop finishes the attempt in progress and stops.
func (p *PendingResolver) Stop(ctx context.Context) error {
	if p.loop == nil {
		return nil
	}
	err := p.loop.Stop(ctx)
	p.log.Info("pending resolver stopped")
	return err
}

// Done is closed when the loop has terminated.
func (p *PendingResolver) Done() <-chan struct{} {
	if p.loop == nil {
		return closedChan
	}
	return p.loop.Done()
}

func (p *PendingResolver) run(ctx context.Context) {
	for ctx.Err() == nil {
		n := p.RunOnce(ctx)
		if n == 0 {
			sleep(ctx, p.cfg.PendingPollInterval)
		}
	}
}

// RunOnce makes one attempt for each due transaction; it returns how many
// transactions changed state.
func (p *PendingResolver) RunOnce(ctx context.Context) int {
	refs, err := p.svc.Due(ctx, p.cfg.PendingBatchSize)
	if err != nil {
		if ctx.Err() == nil {
			p.log.Warn("pending lookup failed", slog.String("error", err.Error()))
			sleep(ctx, time.Second)
		}
		return 0
	}
	changed := 0
	for _, ref := range refs {
		if ctx.Err() != nil {
			break
		}
		// An attempt that started is not interrupted by shutdown.
		actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		out, err := p.svc.Resolve(actx, ref)
		cancel()
		if err != nil {
			p.handleError(ctx, ref, err)
			continue
		}
		delete(p.failures, ref.ID)
		if out != application.ResolveSkipped {
			changed++
		}
	}
	return changed
}

func (p *PendingResolver) handleError(ctx context.Context, ref application.PendingRef, err error) {
	if errors.Is(err, application.ErrTransient) {
		p.log.Warn("pending attempt failed transiently", slog.String("transactionId", ref.ID.String()), slog.String("error", err.Error()))
		return
	}
	p.failures[ref.ID]++
	p.log.Error("pending attempt failed", slog.String("transactionId", ref.ID.String()),
		slog.Int("failures", p.failures[ref.ID]), slog.String("error", err.Error()))
	if p.failures[ref.ID] >= p.svc.MaxFailures() {
		if ferr := p.svc.MarkFailed(ctx, ref, err); ferr != nil {
			p.log.Error("mark failed error", slog.String("error", ferr.Error()))
			return
		}
		delete(p.failures, ref.ID)
	}
}
