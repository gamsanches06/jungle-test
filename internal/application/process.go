package application

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

// Source of an operation.
const (
	SourceHTTP = "http"
	SourceSQS  = "sqs"
)

// ProcessCommand is the transport-independent input shared by HTTP and SQS.
type ProcessCommand struct {
	Request        wagering.Request
	IdempotencyKey string
	CorrelationID  string
	Source         string
	// Inbox is set by the SQS consumer; its registration and completion are
	// part of the same SQL transaction as the financial changes.
	Inbox *InboxMessage
}

// ProcessResult is the persisted outcome.
type ProcessResult struct {
	Transaction *wagering.Transaction
	// Replay is true when the operation already existed and was not reapplied.
	Replay bool
	// InboxDuplicate is true when the message id had already been handled.
	InboxDuplicate bool
}

// ProcessService is the single use case behind POST /wagering/transactions
// and the SQS consumer.
type ProcessService struct {
	tx      TxManager
	ids     wagering.IDGenerator
	clock   Clock
	policy  wagering.PendingPolicy
	metrics Metrics
	log     *slog.Logger
}

// NewProcessService builds the use case.
func NewProcessService(tx TxManager, ids wagering.IDGenerator, clock Clock, policy wagering.PendingPolicy, metrics Metrics, log *slog.Logger) (*ProcessService, error) {
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	return &ProcessService{tx: tx, ids: ids, clock: clock, policy: policy, metrics: metrics, log: log}, nil
}

const maxUniquenessRetries = 3

// Process validates idempotency and applies the operation exactly once.
func (s *ProcessService) Process(ctx context.Context, cmd ProcessCommand) (ProcessResult, error) {
	if err := wagering.ValidateIdempotencyKey(cmd.IdempotencyKey); err != nil {
		return ProcessResult{}, err
	}
	start := time.Now()
	defer func() { s.metrics.ProcessingDuration(cmd.Source, time.Since(start)) }()

	var (
		res ProcessResult
		err error
	)
	for attempt := 1; ; attempt++ {
		res, err = s.processOnce(ctx, cmd)
		if !errors.Is(err, ErrDuplicateTransaction) && !errors.Is(err, ErrConcurrentUpdate) {
			break
		}
		// A concurrent writer won a uniqueness race (same key or external id
		// on another wallet). Retrying finds its committed row.
		s.metrics.ConcurrencyConflict("process")
		if attempt >= maxUniquenessRetries {
			err = Transient(err)
			break
		}
		s.metrics.Retry("process")
	}
	if err != nil {
		return ProcessResult{}, err
	}
	if ferr := failpoint.Inject(failpoint.ProcessAfterCommit); ferr != nil {
		return ProcessResult{}, Transient(ferr)
	}
	t := res.Transaction
	if res.Replay {
		s.metrics.Duplicate(cmd.Source, string(t.Kind()))
	}
	s.metrics.TransactionOutcome(cmd.Source, string(t.Kind()), string(t.Status()), res.Replay)
	s.log.InfoContext(ctx, "wager transaction handled",
		slog.String("transactionId", t.ID().String()),
		slog.String("walletId", t.WalletID().String()),
		slog.String("providerId", t.ProviderID()),
		slog.String("kind", string(t.Kind())),
		slog.String("status", string(t.Status())),
		slog.String("failureCode", string(t.FailureCode())),
		slog.Bool("idempotentReplay", res.Replay),
		slog.Bool("inboxDuplicate", res.InboxDuplicate),
		slog.String("source", cmd.Source))
	return res, nil
}

func (s *ProcessService) processOnce(ctx context.Context, cmd ProcessCommand) (ProcessResult, error) {
	var res ProcessResult
	req := cmd.Request
	hash := req.PayloadHash()
	err := s.tx.InTx(ctx, func(ctx context.Context, r Repositories) error {
		res = ProcessResult{}
		if cmd.Inbox != nil {
			rec, inserted, err := r.Inbox().Register(ctx, *cmd.Inbox)
			if err != nil {
				return err
			}
			if !inserted {
				if rec.PayloadHash != cmd.Inbox.PayloadHash {
					return ErrMessageConflict
				}
				res.InboxDuplicate = true
				res.Replay = true
				existing, err := r.Transactions().Get(ctx, rec.TransactionID)
				if err != nil {
					return err
				}
				res.Transaction = existing
				return nil
			}
		}

		// Replays are answered before taking the wallet lock.
		found, err := s.findIdempotent(ctx, r, req, cmd.IdempotencyKey, hash)
		if err != nil {
			return err
		}
		if found == nil {
			// Serialize every writer of this wallet, then check again: a
			// concurrent request with the same key may have committed while
			// we were waiting for the lock.
			w, err := r.Wallets().GetForUpdate(ctx, req.WalletID())
			if err != nil {
				return err
			}
			found, err = s.findIdempotent(ctx, r, req, cmd.IdempotencyKey, hash)
			if err != nil {
				return err
			}
			if found == nil {
				now := s.clock.Now()
				tx, err := wagering.NewExternal(s.ids.NewID(), req, cmd.IdempotencyKey, cmd.CorrelationID, now)
				if err != nil {
					return err
				}
				causation := ""
				if cmd.Inbox != nil {
					causation = cmd.Inbox.MessageID
				}
				if err := applySettlement(ctx, r, settleArgs{
					tx: tx, wallet: w, isNew: true, policy: s.policy, ids: s.ids, causation: causation, now: now,
				}); err != nil {
					return err
				}
				res.Transaction = tx
			}
		}
		if found != nil {
			res.Transaction = found
			res.Replay = true
		}
		if cmd.Inbox != nil {
			outcome := string(res.Transaction.Status())
			if res.Replay {
				outcome = "REPLAY:" + outcome
			}
			if err := r.Inbox().Complete(ctx, cmd.Inbox.Consumer, cmd.Inbox.MessageID, res.Transaction.ID(), outcome, s.clock.Now()); err != nil {
				return err
			}
		}
		if err := failpoint.Inject(failpoint.ProcessBeforeCommit); err != nil {
			return Transient(err)
		}
		return nil
	})
	return res, err
}

// findIdempotent applies the idempotency rules:
//   - same key and same payload hash: replay of the stored transaction;
//   - same key and different hash: ErrIdempotencyConflict;
//   - same external id under another key: ErrExternalIDReused.
func (s *ProcessService) findIdempotent(ctx context.Context, r Repositories, req wagering.Request, key, hash string) (*wagering.Transaction, error) {
	rows, err := r.Transactions().FindIdempotent(ctx, req.ProviderID(), key, req.ExternalTransactionID())
	if err != nil {
		return nil, err
	}
	var byExternal *wagering.Transaction
	for _, t := range rows {
		ext, _ := t.External()
		if ext.IdempotencyKey == key {
			if ext.PayloadHash != hash {
				return nil, ErrIdempotencyConflict
			}
			return t, nil
		}
		if ext.ExternalTransactionID == req.ExternalTransactionID() {
			byExternal = t
		}
	}
	if byExternal != nil {
		return nil, ErrExternalIDReused
	}
	return nil, nil
}

type settleArgs struct {
	tx        *wagering.Transaction
	wallet    *wallet.Wallet
	isNew     bool
	policy    wagering.PendingPolicy
	ids       wagering.IDGenerator
	causation string
	now       time.Time
}

// applySettlement resolves the reference, runs the domain rules and
// persists the outcome (transaction, ledger, wallet, outbox) in the current
// SQL transaction. The wallet must be locked by the caller.
func applySettlement(ctx context.Context, r Repositories, a settleArgs) error {
	tx, w := a.tx, a.wallet
	prevVersion := w.Version()
	prevStatus := tx.Status()

	var lookup wagering.ReferenceLookup
	if tx.RequiresReference() {
		ref, err := r.Transactions().GetByExternalID(ctx, tx.ProviderID(), tx.ReferenceExternalID())
		switch {
		case errors.Is(err, ErrTransactionNotFound):
		case err != nil:
			return err
		default:
			lookup.Found = true
			lookup.Tx = ref
			if lookup.AlreadyReversed, err = r.Transactions().HasProcessedReversal(ctx, ref.ID()); err != nil {
				return err
			}
		}
	}

	out, err := wagering.Settle(wagering.SettleInput{
		Tx: tx, Wallet: w, Reference: lookup, Policy: a.policy, IDs: a.ids, CausationID: a.causation, Now: a.now,
	})
	if err != nil {
		return fmt.Errorf("settle %s: %w", tx.ID(), err)
	}
	if a.isNew {
		if err := r.Transactions().Insert(ctx, tx); err != nil {
			return err
		}
	} else if err := r.Transactions().Update(ctx, tx, prevStatus); err != nil {
		return err
	}
	if out.Entry != nil {
		if err := r.Ledger().Append(ctx, out.Entry); err != nil {
			return err
		}
		if err := r.Wallets().UpdateBalance(ctx, w, prevVersion); err != nil {
			return err
		}
	}
	if err := r.Outbox().Append(ctx, out.Events); err != nil {
		return err
	}
	// Operations that can be referenced wake up their waiting dependents.
	if tx.Status().IsTerminal() && tx.Kind() != wagering.KindLoss && tx.Kind() != wagering.KindRollback {
		if _, err := r.Transactions().WakeDependents(ctx, tx.ProviderID(), tx.ExternalTransactionID(), a.now); err != nil {
			return err
		}
	}
	return nil
}
