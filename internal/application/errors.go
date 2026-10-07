package application

import (
	"errors"
	"fmt"

	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
)

var (
	// ErrWalletNotFound: the wallet does not exist (not persisted as a transaction).
	ErrWalletNotFound = errors.New("wallet not found")
	// ErrWalletAlreadyExists: (playerId, currency) already has a wallet.
	ErrWalletAlreadyExists = errors.New("wallet already exists for player and currency")
	// ErrTransactionNotFound: no transaction with that identity is visible.
	ErrTransactionNotFound = errors.New("transaction not found")
	// ErrIdempotencyConflict: the key was used with a different payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrExternalIDReused: (providerId, externalTransactionId) exists under another key.
	ErrExternalIDReused = errors.New("external transaction id already used with another idempotency key")
	// ErrMessageConflict: an inbox message id was redelivered with a different hash.
	ErrMessageConflict = errors.New("message id reused with a different payload")
	// ErrDuplicateTransaction: a uniqueness race; the operation is retried.
	ErrDuplicateTransaction = errors.New("duplicate transaction")
	// ErrConcurrentUpdate: the optimistic version guard matched no row.
	ErrConcurrentUpdate = errors.New("concurrent wallet update")
	// ErrTransient marks temporary infrastructure failures (database or
	// broker unavailable, timeouts, exhausted serialization retries). The
	// operation can safely be retried with the same idempotency key.
	ErrTransient = errors.New("transient failure")
)

// TransientError wraps a cause classified as transient.
type TransientError struct{ Cause error }

func (e *TransientError) Error() string        { return fmt.Sprintf("transient failure: %v", e.Cause) }
func (e *TransientError) Unwrap() error        { return e.Cause }
func (e *TransientError) Is(target error) bool { return target == ErrTransient }

// Transient wraps err as a transient failure.
func Transient(err error) error {
	if err == nil || errors.Is(err, ErrTransient) {
		return err
	}
	return &TransientError{Cause: err}
}

// IsPermanentInput reports errors caused by the request content that will
// never succeed if retried unchanged.
func IsPermanentInput(err error) bool {
	return errors.Is(err, wagering.ErrValidation) ||
		errors.Is(err, ErrWalletNotFound) ||
		errors.Is(err, ErrIdempotencyConflict) ||
		errors.Is(err, ErrExternalIDReused) ||
		errors.Is(err, ErrMessageConflict)
}

// CodeOf maps an error to its stable public code.
func CodeOf(err error) wagering.FailureCode {
	var ve *wagering.ValidationError
	switch {
	case errors.As(err, &ve):
		return ve.FailureCodeOf()
	case errors.Is(err, wagering.ErrValidation):
		return wagering.CodeInvalidRequest
	case errors.Is(err, ErrWalletNotFound):
		return wagering.CodeWalletNotFound
	case errors.Is(err, ErrIdempotencyConflict):
		return wagering.CodeIdempotencyKeyReused
	case errors.Is(err, ErrExternalIDReused):
		return wagering.CodeExternalIDReused
	case errors.Is(err, ErrMessageConflict):
		return wagering.CodeMessageIDReused
	case errors.Is(err, ErrTransient):
		return wagering.CodeTransientFailure
	}
	return "INTERNAL_ERROR"
}
