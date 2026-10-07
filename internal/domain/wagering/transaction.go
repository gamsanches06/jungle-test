package wagering

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
)

// External holds the provider metadata that only exists for external
// operations. It is nil for the internal OPENING.
type External struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
}

// Result is the financial outcome returned to the provider and persisted so
// that replays return the balance observed at processing time.
type Result struct {
	Balance       money.Money
	WalletVersion int64
}

// Transaction is a wager transaction with an explicit state machine:
//
//	PENDING ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE
//	PENDING_REFERENCE ──► PROCESSED | REJECTED | FAILED | PENDING_REFERENCE (reschedule)
//
// PROCESSED, REJECTED and FAILED are terminal.
type Transaction struct {
	id            uuid.UUID
	origin        Origin
	kind          Kind
	status        Status
	walletID      uuid.UUID
	playerID      uuid.UUID
	money         money.Money
	external      *External
	referenceTxID uuid.UUID
	failureCode   FailureCode
	failureDetail string
	result        *Result
	attempts      int
	nextAttemptAt time.Time
	expiresAt     time.Time
	correlationID string
	createdAt     time.Time
	updatedAt     time.Time
	completedAt   time.Time
}

// NewExternal creates an external transaction in PENDING.
func NewExternal(id uuid.UUID, req Request, idempotencyKey, correlationID string, now time.Time) (*Transaction, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: id required", ErrInvalidTransaction)
	}
	if req.kind == "" || req.kind == KindOpening {
		return nil, fmt.Errorf("%w: invalid external request", ErrInvalidTransaction)
	}
	if err := ValidateIdempotencyKey(idempotencyKey); err != nil {
		return nil, err
	}
	if correlationID == "" || now.IsZero() {
		return nil, fmt.Errorf("%w: correlation id and timestamp required", ErrInvalidTransaction)
	}
	now = now.UTC()
	return &Transaction{
		id:       id,
		origin:   OriginExternal,
		kind:     req.kind,
		status:   StatusPending,
		walletID: req.walletID,
		playerID: req.playerID,
		money:    req.money,
		external: &External{
			ProviderID:                     req.providerID,
			ExternalTransactionID:          req.externalTransactionID,
			IdempotencyKey:                 idempotencyKey,
			PayloadHash:                    req.PayloadHash(),
			RoundID:                        req.roundID,
			GameID:                         req.gameID,
			ReferenceExternalTransactionID: req.referenceExternalTransactionID,
		},
		correlationID: correlationID,
		createdAt:     now,
		updatedAt:     now,
	}, nil
}

// NewOpening creates the internal OPENING transaction in PENDING. It carries
// no provider metadata; amount must be positive (a zero opening creates no
// transaction at all).
func NewOpening(id, walletID, playerID uuid.UUID, amount money.Money, correlationID string, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids required", ErrInvalidTransaction)
	}
	if err := amount.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: opening amount must be positive", ErrInvalidTransaction)
	}
	if correlationID == "" || now.IsZero() {
		return nil, fmt.Errorf("%w: correlation id and timestamp required", ErrInvalidTransaction)
	}
	now = now.UTC()
	return &Transaction{
		id:            id,
		origin:        OriginInternal,
		kind:          KindOpening,
		status:        StatusPending,
		walletID:      walletID,
		playerID:      playerID,
		money:         amount,
		correlationID: correlationID,
		createdAt:     now,
		updatedAt:     now,
	}, nil
}

// Snapshot is the persisted state used by Rehydrate.
type Snapshot struct {
	ID            uuid.UUID
	Origin        Origin
	Kind          Kind
	Status        Status
	WalletID      uuid.UUID
	PlayerID      uuid.UUID
	Money         money.Money
	External      *External
	ReferenceTxID uuid.UUID
	FailureCode   FailureCode
	FailureDetail string
	Result        *Result
	Attempts      int
	NextAttemptAt time.Time
	ExpiresAt     time.Time
	CorrelationID string
	CreatedAt     time.Time
	UpdatedAt     time.Time
	CompletedAt   time.Time
}

// Rehydrate rebuilds a transaction from storage. It validates the shape of
// the data but performs no transition and emits no event.
func Rehydrate(s Snapshot) (*Transaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids required", ErrInvalidTransaction)
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	if _, err := ParseStatus(string(s.Status)); err != nil {
		return nil, err
	}
	if err := s.Money.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTransaction, err)
	}
	switch s.Origin {
	case OriginInternal:
		if s.Kind != KindOpening || s.External != nil {
			return nil, fmt.Errorf("%w: internal transaction must be OPENING without external metadata", ErrInvalidTransaction)
		}
	case OriginExternal:
		if s.Kind == KindOpening || s.External == nil {
			return nil, fmt.Errorf("%w: external transaction requires provider metadata", ErrInvalidTransaction)
		}
	default:
		return nil, fmt.Errorf("%w: origin %q", ErrInvalidTransaction, s.Origin)
	}
	if s.Status == StatusProcessed && s.Result == nil {
		return nil, fmt.Errorf("%w: processed transaction without result", ErrInvalidTransaction)
	}
	if (s.Status == StatusRejected || s.Status == StatusFailed) && s.FailureCode == "" {
		return nil, fmt.Errorf("%w: %s without failure code", ErrInvalidTransaction, s.Status)
	}
	var ext *External
	if s.External != nil {
		e := *s.External
		ext = &e
	}
	var res *Result
	if s.Result != nil {
		r := *s.Result
		res = &r
	}
	return &Transaction{
		id:            s.ID,
		origin:        s.Origin,
		kind:          s.Kind,
		status:        s.Status,
		walletID:      s.WalletID,
		playerID:      s.PlayerID,
		money:         s.Money,
		external:      ext,
		referenceTxID: s.ReferenceTxID,
		failureCode:   s.FailureCode,
		failureDetail: s.FailureDetail,
		result:        res,
		attempts:      s.Attempts,
		nextAttemptAt: s.NextAttemptAt,
		expiresAt:     s.ExpiresAt,
		correlationID: s.CorrelationID,
		createdAt:     s.CreatedAt,
		updatedAt:     s.UpdatedAt,
		completedAt:   s.CompletedAt,
	}, nil
}

func (t *Transaction) ID() uuid.UUID            { return t.id }
func (t *Transaction) Origin() Origin           { return t.origin }
func (t *Transaction) Kind() Kind               { return t.kind }
func (t *Transaction) Status() Status           { return t.status }
func (t *Transaction) WalletID() uuid.UUID      { return t.walletID }
func (t *Transaction) PlayerID() uuid.UUID      { return t.playerID }
func (t *Transaction) Money() money.Money       { return t.money }
func (t *Transaction) ReferenceTxID() uuid.UUID { return t.referenceTxID }
func (t *Transaction) FailureCode() FailureCode { return t.failureCode }
func (t *Transaction) FailureDetail() string    { return t.failureDetail }
func (t *Transaction) Attempts() int            { return t.attempts }
func (t *Transaction) NextAttemptAt() time.Time { return t.nextAttemptAt }
func (t *Transaction) ExpiresAt() time.Time     { return t.expiresAt }
func (t *Transaction) CorrelationID() string    { return t.correlationID }
func (t *Transaction) CreatedAt() time.Time     { return t.createdAt }
func (t *Transaction) UpdatedAt() time.Time     { return t.updatedAt }
func (t *Transaction) CompletedAt() time.Time   { return t.completedAt }
func (t *Transaction) IsExternal() bool         { return t.origin == OriginExternal }
func (t *Transaction) RequiresReference() bool  { return t.ReferenceExternalID() != "" }
func (t *Transaction) Result() (Result, bool) {
	if t.result == nil {
		return Result{}, false
	}
	return *t.result, true
}

// External returns a copy of the provider metadata.
func (t *Transaction) External() (External, bool) {
	if t.external == nil {
		return External{}, false
	}
	return *t.external, true
}

func (t *Transaction) ProviderID() string {
	if t.external == nil {
		return ""
	}
	return t.external.ProviderID
}

func (t *Transaction) ExternalTransactionID() string {
	if t.external == nil {
		return ""
	}
	return t.external.ExternalTransactionID
}

func (t *Transaction) ReferenceExternalID() string {
	if t.external == nil {
		return ""
	}
	return t.external.ReferenceExternalTransactionID
}

func (t *Transaction) PayloadHash() string {
	if t.external == nil {
		return ""
	}
	return t.external.PayloadHash
}

func (t *Transaction) transition(to Status, now time.Time) error {
	if now.IsZero() {
		return fmt.Errorf("%w: timestamp required", ErrInvalidTransition)
	}
	if t.status.IsTerminal() {
		return fmt.Errorf("%w: %s is terminal (attempted %s)", ErrInvalidTransition, t.status, to)
	}
	if to == StatusPendingReference && t.origin != OriginExternal {
		return fmt.Errorf("%w: internal transactions have no references", ErrInvalidTransition)
	}
	if t.origin == OriginInternal && to != StatusProcessed {
		return fmt.Errorf("%w: OPENING can only be processed", ErrInvalidTransition)
	}
	t.status = to
	t.updatedAt = now.UTC()
	if to.IsTerminal() {
		t.completedAt = now.UTC()
		t.nextAttemptAt = time.Time{}
	}
	return nil
}

// MarkProcessed finalizes the transaction successfully.
func (t *Transaction) MarkProcessed(res Result, referenceTxID uuid.UUID, now time.Time) error {
	if err := res.Balance.Validate(); err != nil {
		return fmt.Errorf("%w: result balance: %v", ErrInvalidTransition, err)
	}
	if t.kind.AcceptsReference() && t.ReferenceExternalID() != "" && referenceTxID == uuid.Nil {
		return fmt.Errorf("%w: resolved reference required", ErrInvalidTransition)
	}
	if err := t.transition(StatusProcessed, now); err != nil {
		return err
	}
	r := res
	t.result = &r
	t.referenceTxID = referenceTxID
	return nil
}

// MarkPendingReference moves to PENDING_REFERENCE (from PENDING) or
// reschedules the next attempt (from PENDING_REFERENCE), incrementing the
// attempt counter.
func (t *Transaction) MarkPendingReference(nextAttemptAt, expiresAt time.Time, now time.Time) error {
	if t.ReferenceExternalID() == "" {
		return fmt.Errorf("%w: transaction has no reference", ErrInvalidTransition)
	}
	if nextAttemptAt.IsZero() || expiresAt.IsZero() {
		return fmt.Errorf("%w: schedule required", ErrInvalidTransition)
	}
	if err := t.transition(StatusPendingReference, now); err != nil {
		return err
	}
	t.attempts++
	t.nextAttemptAt = nextAttemptAt.UTC()
	t.expiresAt = expiresAt.UTC()
	return nil
}

// Reject finalizes the transaction with a business rejection. The balance
// observed at decision time is kept as the result returned to the provider.
func (t *Transaction) Reject(code FailureCode, detail string, observed *Result, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: failure code required", ErrInvalidTransition)
	}
	if err := t.transition(StatusRejected, now); err != nil {
		return err
	}
	t.failureCode = code
	t.failureDetail = detail
	if observed != nil {
		r := *observed
		t.result = &r
	}
	return nil
}

// Fail finalizes the transaction as a permanent infrastructure failure.
func (t *Transaction) Fail(code FailureCode, detail string, now time.Time) error {
	if code == "" {
		return fmt.Errorf("%w: failure code required", ErrInvalidTransition)
	}
	if err := t.transition(StatusFailed, now); err != nil {
		return err
	}
	t.failureCode = code
	t.failureDetail = detail
	return nil
}

// Snapshot exports the state for persistence adapters.
func (t *Transaction) Snapshot() Snapshot {
	s := Snapshot{
		ID: t.id, Origin: t.origin, Kind: t.kind, Status: t.status, WalletID: t.walletID, PlayerID: t.playerID,
		Money: t.money, ReferenceTxID: t.referenceTxID, FailureCode: t.failureCode, FailureDetail: t.failureDetail,
		Attempts: t.attempts, NextAttemptAt: t.nextAttemptAt, ExpiresAt: t.expiresAt, CorrelationID: t.correlationID,
		CreatedAt: t.createdAt, UpdatedAt: t.updatedAt, CompletedAt: t.completedAt,
	}
	if t.external != nil {
		e := *t.external
		s.External = &e
	}
	if t.result != nil {
		r := *t.result
		s.Result = &r
	}
	return s
}
