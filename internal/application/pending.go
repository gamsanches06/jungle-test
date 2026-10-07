package application

import (
	"context"
	"errors"
	"log/slog"

	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

// PendingService resumes PENDING and PENDING_REFERENCE transactions. Any
// instance can resume any pending row: state is only in PostgreSQL.
type PendingService struct {
	tx          TxManager
	ids         wagering.IDGenerator
	clock       Clock
	policy      wagering.PendingPolicy
	metrics     Metrics
	log         *slog.Logger
	maxFailures int
}

// NewPendingService builds the service.
func NewPendingService(tx TxManager, ids wagering.IDGenerator, clock Clock, policy wagering.PendingPolicy, metrics Metrics, log *slog.Logger) *PendingService {
	return &PendingService{tx: tx, ids: ids, clock: clock, policy: policy, metrics: metrics, log: log, maxFailures: 5}
}

// Due lists transactions due for an attempt.
func (s *PendingService) Due(ctx context.Context, limit int) ([]PendingRef, error) {
	var refs []PendingRef
	err := s.tx.InTx(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		refs, err = r.Transactions().DuePending(ctx, s.clock.Now(), limit)
		return err
	})
	return refs, err
}

// ResolveOutcome describes what happened to one pending transaction.
type ResolveOutcome string

const (
	ResolveSkipped     ResolveOutcome = "skipped"     // wallet busy or not due anymore
	ResolveRescheduled ResolveOutcome = "rescheduled" // reference still unavailable
	ResolveFinished    ResolveOutcome = "finished"    // PROCESSED or REJECTED
)

// Resolve makes one attempt for a pending transaction. The wallet row is
// locked with SKIP LOCKED so concurrent workers never wait on each other,
// and the transaction row is re-read under the lock, so a row is never
// resolved twice.
func (s *PendingService) Resolve(ctx context.Context, ref PendingRef) (ResolveOutcome, error) {
	if err := failpoint.Inject(failpoint.PendingBeforeResolve); err != nil {
		return ResolveSkipped, Transient(err)
	}
	var (
		outcome = ResolveSkipped
		final   *wagering.Transaction
	)
	err := s.tx.InTx(ctx, func(ctx context.Context, r Repositories) error {
		outcome, final = ResolveSkipped, nil
		w, ok, err := r.Wallets().TryGetForUpdate(ctx, ref.WalletID)
		if err != nil || !ok {
			return err
		}
		tx, err := r.Transactions().GetForUpdate(ctx, ref.ID)
		if err != nil {
			return err
		}
		now := s.clock.Now()
		if tx.Status().IsTerminal() || tx.NextAttemptAt().After(now) {
			return nil
		}
		if err := applySettlement(ctx, r, settleArgs{
			tx: tx, wallet: w, isNew: false, policy: s.policy, ids: s.ids, causation: tx.ID().String(), now: now,
		}); err != nil {
			return err
		}
		final = tx
		if tx.Status().IsTerminal() {
			outcome = ResolveFinished
		} else {
			outcome = ResolveRescheduled
		}
		return nil
	})
	if err != nil {
		return ResolveSkipped, err
	}
	if final != nil {
		if outcome == ResolveFinished {
			s.metrics.TransactionOutcome("pending-worker", string(final.Kind()), string(final.Status()), false)
		} else {
			s.metrics.Retry("pending-reference")
		}
		s.log.InfoContext(ctx, "pending transaction attempt",
			slog.String("transactionId", final.ID().String()),
			slog.String("walletId", final.WalletID().String()),
			slog.String("providerId", final.ProviderID()),
			slog.String("correlationId", final.CorrelationID()),
			slog.String("status", string(final.Status())),
			slog.String("failureCode", string(final.FailureCode())),
			slog.Int("attempts", final.Attempts()))
	}
	return outcome, nil
}

// MarkFailed finalizes a pending transaction as FAILED when its resolution
// keeps failing with a non-transient error (broken data, programming error).
func (s *PendingService) MarkFailed(ctx context.Context, ref PendingRef, cause error) error {
	if errors.Is(cause, ErrTransient) {
		return nil
	}
	return s.tx.InTx(ctx, func(ctx context.Context, r Repositories) error {
		tx, err := r.Transactions().GetForUpdate(ctx, ref.ID)
		if err != nil {
			return err
		}
		if tx.Status().IsTerminal() {
			return nil
		}
		prev := tx.Status()
		out, err := wagering.FailPending(tx, cause.Error(), s.ids, tx.ID().String(), s.clock.Now())
		if err != nil {
			return err
		}
		if err := r.Transactions().Update(ctx, tx, prev); err != nil {
			return err
		}
		s.log.ErrorContext(ctx, "pending transaction failed permanently",
			slog.String("transactionId", tx.ID().String()), slog.String("error", cause.Error()))
		return r.Outbox().Append(ctx, out.Events)
	})
}

// MaxFailures is the number of consecutive permanent errors tolerated
// before MarkFailed is used.
func (s *PendingService) MaxFailures() int { return s.maxFailures }
