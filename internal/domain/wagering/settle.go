package wagering

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/events"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

// IDGenerator produces new identities (UUIDv7 in production).
type IDGenerator interface {
	NewID() uuid.UUID
}

// PendingPolicy controls how long an operation waits for its reference.
type PendingPolicy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
	TTL         time.Duration
}

// Validate checks the policy.
func (p PendingPolicy) Validate() error {
	if p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay || p.MaxAttempts < 1 || p.TTL <= 0 {
		return fmt.Errorf("wagering: invalid pending policy %+v", p)
	}
	return nil
}

// Delay returns the exponential backoff before attempt n (n >= 1):
// BaseDelay * 2^(n-1), capped at MaxDelay.
func (p PendingPolicy) Delay(n int) time.Duration {
	d := p.BaseDelay
	for i := 1; i < n; i++ {
		d *= 2
		if d >= p.MaxDelay || d <= 0 {
			return p.MaxDelay
		}
	}
	if d > p.MaxDelay {
		return p.MaxDelay
	}
	return d
}

// ReferenceLookup is the resolved state of (providerId, referenceExternalTransactionId).
type ReferenceLookup struct {
	Found bool
	Tx    *Transaction
	// AlreadyReversed is true when a PROCESSED REFUND or ROLLBACK already
	// references Tx.
	AlreadyReversed bool
}

// SettleInput is everything needed to decide an operation. Wallet must be
// the locked, current state of the transaction's wallet.
type SettleInput struct {
	Tx          *Transaction
	Wallet      *wallet.Wallet
	Reference   ReferenceLookup
	Policy      PendingPolicy
	IDs         IDGenerator
	CausationID string
	Now         time.Time
}

// Outcome is what must be persisted atomically after Settle.
type Outcome struct {
	// Entry is the ledger entry of the balance change, nil when there is none.
	Entry  *wallet.LedgerEntry
	Events []events.Event
}

// Settle applies the rules of the operation kind to the wallet and moves the
// transaction to its next state. Business rejections are not errors: they are
// recorded as REJECTED with a failure code. Returned errors indicate invalid
// usage or broken invariants.
func Settle(in SettleInput) (Outcome, error) {
	tx, w := in.Tx, in.Wallet
	if tx == nil || w == nil || in.IDs == nil || in.Now.IsZero() {
		return Outcome{}, fmt.Errorf("%w: incomplete settle input", ErrInvalidTransaction)
	}
	if tx.status.IsTerminal() {
		return Outcome{}, fmt.Errorf("%w: %s is terminal", ErrInvalidTransition, tx.status)
	}
	if tx.origin != OriginExternal {
		return Outcome{}, fmt.Errorf("%w: only external transactions are settled", ErrInvalidTransaction)
	}
	if tx.walletID != w.ID() {
		return Outcome{}, fmt.Errorf("%w: wallet %s does not match transaction wallet %s", ErrInvalidTransaction, w.ID(), tx.walletID)
	}
	s := settler{in: in, tx: tx, w: w, now: in.Now.UTC()}
	return s.run()
}

type settler struct {
	in  SettleInput
	tx  *Transaction
	w   *wallet.Wallet
	ref *Transaction // resolved reference, recorded even on rejection
	now time.Time
}

func (s *settler) observed() *Result {
	return &Result{Balance: s.w.Balance(), WalletVersion: s.w.Version()}
}

func (s *settler) run() (Outcome, error) {
	if s.w.PlayerID() != s.tx.playerID {
		return s.reject(CodeWalletPlayerMismatch, "wallet belongs to another player")
	}
	if s.tx.money.Currency() != s.w.Currency() {
		return s.reject(CodeCurrencyMismatch, fmt.Sprintf("wallet currency is %s", s.w.Currency()))
	}

	var ref *Transaction
	if s.tx.RequiresReference() {
		lookup := s.in.Reference
		if lookup.Found && lookup.Tx == nil {
			return Outcome{}, fmt.Errorf("%w: reference marked found without transaction", ErrInvalidTransaction)
		}
		if lookup.Found {
			ref = lookup.Tx
			s.ref = ref
			if code, detail := s.staticReferenceCheck(ref); code != "" {
				return s.reject(code, detail)
			}
		}
		if !lookup.Found || !ref.status.IsTerminal() {
			return s.pending(lookup.Found)
		}
		if ref.status != StatusProcessed {
			return s.reject(CodeReferenceNotProcessed, fmt.Sprintf("reference finished as %s", ref.status))
		}
		if s.tx.kind.RequiresReference() {
			if !ref.money.Equal(s.tx.money) {
				return s.reject(CodeReversalAmountMismatch, fmt.Sprintf("reference amount is %s", ref.money))
			}
			if lookup.AlreadyReversed {
				return s.reject(CodeReferenceAlreadyReversed, "reference already has a processed REFUND or ROLLBACK")
			}
		}
	}

	var (
		entry *wallet.LedgerEntry
		err   error
	)
	mv := wallet.MovementParams{EntryID: s.in.IDs.NewID(), TransactionID: s.tx.id, Amount: s.tx.money, Now: s.now}
	switch s.tx.kind {
	case KindBet:
		entry, err = s.w.Debit(mv)
		if errors.Is(err, wallet.ErrInsufficientFunds) {
			return s.reject(CodeInsufficientFunds, "balance lower than bet amount")
		}
	case KindWin, KindRefund:
		entry, err = s.w.Credit(mv)
	case KindLoss:
		// No balance movement, no ledger entry, no version change.
	case KindRollback:
		if refDirection(ref.kind) == wallet.Debit {
			entry, err = s.w.Credit(mv)
		} else {
			entry, err = s.w.Debit(mv)
			if errors.Is(err, wallet.ErrInsufficientFunds) {
				return s.reject(CodeReversalInsufficientFunds, "balance lower than the amount to be reversed")
			}
		}
	default:
		return Outcome{}, fmt.Errorf("%w: kind %s cannot be settled", ErrInvalidTransaction, s.tx.kind)
	}
	if errors.Is(err, money.ErrOverflow) {
		return s.reject(CodeBalanceOverflow, "balance would overflow")
	}
	if err != nil {
		return Outcome{}, err
	}

	refID := uuid.Nil
	if ref != nil {
		refID = ref.id
	}
	if err := s.tx.MarkProcessed(Result{Balance: s.w.Balance(), WalletVersion: s.w.Version()}, refID, s.now); err != nil {
		return Outcome{}, err
	}
	evs, err := processedEvents(s.tx, entry, s.meta, s.w.Balance(), s.w.Version())
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Entry: entry, Events: evs}, nil
}

// staticReferenceCheck validates fields that do not depend on the reference
// status, so mismatches are rejected immediately even if it is still pending.
func (s *settler) staticReferenceCheck(ref *Transaction) (FailureCode, string) {
	switch s.tx.kind {
	case KindWin, KindRefund:
		if ref.kind != KindBet {
			return CodeInvalidReferenceKind, fmt.Sprintf("%s must reference a BET, got %s", s.tx.kind, ref.kind)
		}
	case KindRollback:
		if ref.kind != KindBet && ref.kind != KindWin && ref.kind != KindRefund {
			return CodeInvalidReferenceKind, fmt.Sprintf("ROLLBACK must reference BET, WIN or REFUND, got %s", ref.kind)
		}
	}
	switch {
	case ref.ProviderID() != s.tx.ProviderID():
		return CodeReferenceMismatch, "provider differs"
	case ref.playerID != s.tx.playerID:
		return CodeReferenceMismatch, "player differs"
	case ref.walletID != s.tx.walletID:
		return CodeReferenceMismatch, "wallet differs"
	case ref.money.Currency() != s.tx.money.Currency():
		return CodeReferenceMismatch, "currency differs"
	case ref.external.RoundID != s.tx.external.RoundID:
		return CodeReferenceMismatch, "round differs"
	}
	return "", ""
}

func (s *settler) pending(found bool) (Outcome, error) {
	p := s.in.Policy
	if err := p.Validate(); err != nil {
		return Outcome{}, err
	}
	if s.tx.status == StatusPending {
		next := s.now.Add(p.Delay(1))
		expires := s.now.Add(p.TTL)
		if err := s.tx.MarkPendingReference(next, expires, s.now); err != nil {
			return Outcome{}, err
		}
		ext := s.tx.external
		ev, err := events.NewWagerTransactionPendingReference(s.meta(), events.WagerTransactionPendingReferenceData{
			TransactionID:                  s.tx.id,
			Kind:                           string(s.tx.kind),
			ProviderID:                     ext.ProviderID,
			ExternalTransactionID:          ext.ExternalTransactionID,
			ReferenceExternalTransactionID: ext.ReferenceExternalTransactionID,
			WalletID:                       s.tx.walletID,
			PlayerID:                       s.tx.playerID,
			Money:                          s.tx.money,
			NextAttemptAt:                  events.Timestamp(next),
			ExpiresAt:                      events.Timestamp(expires),
		})
		if err != nil {
			return Outcome{}, err
		}
		return Outcome{Events: []events.Event{ev}}, nil
	}
	if s.tx.attempts >= p.MaxAttempts || !s.now.Before(s.tx.expiresAt) {
		if found {
			return s.reject(CodeReferenceNotProcessed, "reference still pending when the waiting period expired")
		}
		return s.reject(CodeReferenceNotFound, "reference not received before the waiting period expired")
	}
	next := s.now.Add(p.Delay(s.tx.attempts + 1))
	if next.After(s.tx.expiresAt) {
		next = s.tx.expiresAt
	}
	if err := s.tx.MarkPendingReference(next, s.tx.expiresAt, s.now); err != nil {
		return Outcome{}, err
	}
	return Outcome{}, nil
}

func (s *settler) reject(code FailureCode, detail string) (Outcome, error) {
	if err := s.tx.Reject(code, detail, s.observed(), s.now); err != nil {
		return Outcome{}, err
	}
	if s.ref != nil {
		s.tx.referenceTxID = s.ref.id
	}
	ext := s.tx.external
	ev, err := events.NewWagerTransactionRejected(s.meta(), events.WagerTransactionRejectedData{
		TransactionID:         s.tx.id,
		Kind:                  string(s.tx.kind),
		ProviderID:            ext.ProviderID,
		ExternalTransactionID: ext.ExternalTransactionID,
		RoundID:               ext.RoundID,
		GameID:                ext.GameID,
		WalletID:              s.tx.walletID,
		PlayerID:              s.tx.playerID,
		Money:                 s.tx.money,
		FailureCode:           string(code),
		RejectedAt:            events.Timestamp(s.now),
	})
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Events: []events.Event{ev}}, nil
}

func (s *settler) meta() events.Metadata {
	return events.Metadata{
		EventID:       s.in.IDs.NewID(),
		CorrelationID: s.tx.correlationID,
		CausationID:   s.in.CausationID,
		OccurredAt:    s.now,
	}
}

// refDirection is the wallet movement originally produced by a kind.
func refDirection(k Kind) wallet.Direction {
	if k == KindBet {
		return wallet.Debit
	}
	return wallet.Credit
}

func processedEvents(tx *Transaction, entry *wallet.LedgerEntry, meta func() events.Metadata, balance money.Money, version int64) ([]events.Event, error) {
	data := events.WagerTransactionProcessedData{
		TransactionID: tx.id,
		Origin:        string(tx.origin),
		Kind:          string(tx.kind),
		WalletID:      tx.walletID,
		PlayerID:      tx.playerID,
		Money:         tx.money,
		BalanceAfter:  balance,
		WalletVersion: version,
		ProcessedAt:   events.Timestamp(tx.completedAt),
	}
	if tx.external != nil {
		data.ProviderID = tx.external.ProviderID
		data.ExternalTransactionID = tx.external.ExternalTransactionID
		data.RoundID = tx.external.RoundID
		data.GameID = tx.external.GameID
	}
	if tx.referenceTxID != uuid.Nil {
		id := tx.referenceTxID
		data.ReferenceTransactionID = &id
	}
	processed, err := events.NewWagerTransactionProcessed(meta(), data)
	if err != nil {
		return nil, err
	}
	out := []events.Event{processed}
	if entry != nil {
		changed, err := events.NewWalletBalanceChanged(meta(), events.WalletBalanceChangedData{
			WalletID:      entry.WalletID(),
			TransactionID: entry.TransactionID(),
			Direction:     string(entry.Direction()),
			Money:         entry.Amount(),
			BalanceBefore: entry.BalanceBefore(),
			BalanceAfter:  entry.BalanceAfter(),
			WalletVersion: entry.WalletVersion(),
		})
		if err != nil {
			return nil, err
		}
		out = append(out, changed)
	}
	return out, nil
}

// FailPending finalizes a pending transaction as FAILED after exhausting
// infrastructure retries, emitting WagerTransactionFailed.
func FailPending(tx *Transaction, detail string, ids IDGenerator, causationID string, now time.Time) (Outcome, error) {
	if err := tx.Fail(CodeProcessingFailed, detail, now); err != nil {
		return Outcome{}, err
	}
	ev, err := events.NewWagerTransactionFailed(events.Metadata{
		EventID: ids.NewID(), CorrelationID: tx.correlationID, CausationID: causationID, OccurredAt: now,
	}, events.WagerTransactionFailedData{
		TransactionID:         tx.id,
		Kind:                  string(tx.kind),
		ProviderID:            tx.ProviderID(),
		ExternalTransactionID: tx.ExternalTransactionID(),
		WalletID:              tx.walletID,
		Money:                 tx.money,
		FailureCode:           string(CodeProcessingFailed),
		FailedAt:              events.Timestamp(now),
	})
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{Events: []events.Event{ev}}, nil
}

// OpenWalletInput describes an internal wallet opening.
type OpenWalletInput struct {
	WalletID       uuid.UUID
	PlayerID       uuid.UUID
	InitialBalance money.Money
	IDs            IDGenerator
	CorrelationID  string
	Now            time.Time
}

// OpenWalletResult is persisted in a single SQL transaction.
type OpenWalletResult struct {
	Wallet  *wallet.Wallet
	Opening *Transaction        // nil for a zero initial balance
	Entry   *wallet.LedgerEntry // nil for a zero initial balance
	Events  []events.Event      // empty for a zero initial balance
}

// OpenWallet opens a wallet. A positive initial balance creates an OPENING
// transaction in PROCESSED, its CREDIT entry and the WagerTransactionProcessed
// and WalletBalanceChanged events, all at wallet version 1.
func OpenWallet(in OpenWalletInput) (OpenWalletResult, error) {
	if in.IDs == nil {
		return OpenWalletResult{}, fmt.Errorf("%w: id generator required", ErrInvalidTransaction)
	}
	if err := in.InitialBalance.Validate(); err != nil {
		return OpenWalletResult{}, err
	}
	if in.InitialBalance.IsZero() {
		op, err := wallet.Open(wallet.OpenParams{ID: in.WalletID, PlayerID: in.PlayerID, InitialBalance: in.InitialBalance, Now: in.Now})
		if err != nil {
			return OpenWalletResult{}, err
		}
		return OpenWalletResult{Wallet: op.Wallet}, nil
	}
	txID := in.IDs.NewID()
	tx, err := NewOpening(txID, in.WalletID, in.PlayerID, in.InitialBalance, in.CorrelationID, in.Now)
	if err != nil {
		return OpenWalletResult{}, err
	}
	op, err := wallet.Open(wallet.OpenParams{
		ID: in.WalletID, PlayerID: in.PlayerID, InitialBalance: in.InitialBalance,
		OpeningTransactionID: txID, EntryID: in.IDs.NewID(), Now: in.Now,
	})
	if err != nil {
		return OpenWalletResult{}, err
	}
	w := op.Wallet
	if err := tx.MarkProcessed(Result{Balance: w.Balance(), WalletVersion: w.Version()}, uuid.Nil, in.Now); err != nil {
		return OpenWalletResult{}, err
	}
	meta := func() events.Metadata {
		return events.Metadata{EventID: in.IDs.NewID(), CorrelationID: in.CorrelationID, OccurredAt: in.Now.UTC()}
	}
	evs, err := processedEvents(tx, op.Entry, meta, w.Balance(), w.Version())
	if err != nil {
		return OpenWalletResult{}, err
	}
	return OpenWalletResult{Wallet: w, Opening: tx, Entry: op.Entry, Events: evs}, nil
}
