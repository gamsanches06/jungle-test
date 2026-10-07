// Package wagering models wager transactions, their state machine and the
// settlement rules of each operation kind. It is a pure domain package.
package wagering

import (
	"errors"
	"fmt"
)

// Kind of a wager transaction.
type Kind string

const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseKind validates any persisted kind, including OPENING.
func ParseKind(s string) (Kind, error) {
	switch Kind(s) {
	case KindOpening, KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return Kind(s), nil
	}
	return "", &ValidationError{Field: "kind", Reason: fmt.Sprintf("unknown kind %q", s)}
}

// ParseExternalKind validates a kind received by HTTP or SQS. OPENING is
// reserved to internal wallet opening and is rejected.
func ParseExternalKind(s string) (Kind, error) {
	k, err := ParseKind(s)
	if err != nil {
		return "", err
	}
	if k == KindOpening {
		return "", &ValidationError{Field: "kind", Reason: "OPENING is reserved for internal wallet opening", Code: CodeInternalKindNotAllowed}
	}
	return k, nil
}

// RequiresReference reports whether the kind needs referenceExternalTransactionId.
func (k Kind) RequiresReference() bool { return k == KindRefund || k == KindRollback }

// AcceptsReference reports whether the kind may carry a reference.
func (k Kind) AcceptsReference() bool { return k == KindWin || k.RequiresReference() }

// Status of a wager transaction.
type Status string

const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// ParseStatus validates a persisted status.
func ParseStatus(s string) (Status, error) {
	switch Status(s) {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return Status(s), nil
	}
	return "", fmt.Errorf("%w: status %q", ErrInvalidTransaction, s)
}

// IsTerminal reports whether no further transition is allowed.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Origin distinguishes internal (OPENING) and external (provider) operations.
type Origin string

const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)

// FailureCode is a stable, documented reason for REJECTED or FAILED.
type FailureCode string

// Rejection codes persisted with status REJECTED. "Correctable" codes mean
// the provider sent data that does not match the wallet/reference and may
// send a new operation (new externalTransactionId) with corrected data;
// "definitive" codes are outcomes of the current state.
const (
	// Definitive.
	CodeInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	CodeReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	CodeReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	CodeReferenceNotProcessed     FailureCode = "REFERENCE_NOT_PROCESSED"
	CodeReferenceAlreadyReversed  FailureCode = "REFERENCE_ALREADY_REVERSED"
	CodeBalanceOverflow           FailureCode = "BALANCE_OVERFLOW"
	// Correctable.
	CodeCurrencyMismatch       FailureCode = "CURRENCY_MISMATCH"
	CodeWalletPlayerMismatch   FailureCode = "WALLET_PLAYER_MISMATCH"
	CodeReferenceMismatch      FailureCode = "REFERENCE_MISMATCH"
	CodeInvalidReferenceKind   FailureCode = "INVALID_REFERENCE_KIND"
	CodeReversalAmountMismatch FailureCode = "REVERSAL_AMOUNT_MISMATCH"
	// FAILED (infrastructure).
	CodeProcessingFailed FailureCode = "PROCESSING_FAILED"
)

// Non-persisted error codes (the request never became a transaction).
const (
	CodeInvalidRequest            FailureCode = "INVALID_REQUEST"
	CodeInternalKindNotAllowed    FailureCode = "INTERNAL_KIND_NOT_ALLOWED"
	CodeWalletNotFound            FailureCode = "WALLET_NOT_FOUND"
	CodeIdempotencyKeyReused      FailureCode = "IDEMPOTENCY_KEY_REUSED"
	CodeExternalIDReused          FailureCode = "EXTERNAL_TRANSACTION_ID_REUSED"
	CodeMessageIDReused           FailureCode = "MESSAGE_ID_REUSED"
	CodeProviderForbidden         FailureCode = "PROVIDER_FORBIDDEN"
	CodeTransientFailure          FailureCode = "SERVICE_UNAVAILABLE"
	CodeConcurrencyLimitExhausted FailureCode = "CONCURRENCY_CONFLICT"
)

// Correctable reports whether a rejection code is caused by correctable input.
func (c FailureCode) Correctable() bool {
	switch c {
	case CodeCurrencyMismatch, CodeWalletPlayerMismatch, CodeReferenceMismatch,
		CodeInvalidReferenceKind, CodeReversalAmountMismatch:
		return true
	}
	return false
}

var (
	// ErrInvalidTransaction reports invalid construction/rehydration data.
	ErrInvalidTransaction = errors.New("wagering: invalid transaction")
	// ErrInvalidTransition reports a forbidden state transition.
	ErrInvalidTransition = errors.New("wagering: invalid state transition")
	// ErrValidation is matched by every *ValidationError.
	ErrValidation = errors.New("wagering: validation failed")
)

// ValidationError is a non-persisted rejection of malformed input.
type ValidationError struct {
	Field  string
	Reason string
	Code   FailureCode
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

// Is makes errors.Is(err, ErrValidation) true.
func (e *ValidationError) Is(target error) bool { return target == ErrValidation }

// FailureCodeOf returns the code of a validation error.
func (e *ValidationError) FailureCodeOf() FailureCode {
	if e.Code == "" {
		return CodeInvalidRequest
	}
	return e.Code
}
