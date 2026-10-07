package postgres

import (
	"context"
	"errors"
	"net"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/gamsanches06/jungle-test/internal/application"
)

// SQLSTATE codes used for classification.
const (
	codeUniqueViolation      = "23505"
	codeSerializationFailure = "40001"
	codeDeadlockDetected     = "40P01"
	codeLockNotAvailable     = "55P03"
	codeAdminShutdown        = "57P01"
	codeCrashShutdown        = "57P02"
	codeCannotConnectNow     = "57P03"
	codeTooManyConnections   = "53300"
)

// isRetryableTx reports errors resolved by re-running the SQL transaction.
func isRetryableTx(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == codeSerializationFailure || pgErr.Code == codeDeadlockDetected
	}
	return false
}

// isTransient reports infrastructure failures that may succeed later:
// connection problems, server shutdown/startup, resource exhaustion,
// timeouts and exhausted serialization retries.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case strings.HasPrefix(pgErr.Code, "08"), // connection exception
			strings.HasPrefix(pgErr.Code, "53"), // insufficient resources
			pgErr.Code == codeAdminShutdown, pgErr.Code == codeCrashShutdown,
			pgErr.Code == codeCannotConnectNow, pgErr.Code == codeSerializationFailure,
			pgErr.Code == codeDeadlockDetected, pgErr.Code == codeLockNotAvailable,
			pgErr.Code == codeTooManyConnections:
			return true
		}
		return false
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "conn closed") || strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe") || strings.Contains(msg, "unexpected EOF") ||
		strings.Contains(msg, "closed pool")
}

func uniqueViolation(err error) (constraint string, ok bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == codeUniqueViolation {
		return pgErr.ConstraintName, true
	}
	return "", false
}

func notFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }

// classify wraps transient errors so callers can use errors.Is(err, application.ErrTransient).
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, application.ErrTransient) || errors.Is(err, context.Canceled) {
		return err
	}
	if isTransient(err) {
		return application.Transient(err)
	}
	return err
}
