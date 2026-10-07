// Package application contains the use cases. It orchestrates the domain
// inside explicit SQL transactions through ports implemented by adapters.
package application

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/events"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

// TxManager delimits SQL transactions. Every repository obtained from the
// Repositories passed to fn shares the same SQL transaction, so the wallet,
// ledger, transaction, inbox and outbox writes commit or roll back together.
type TxManager interface {
	// InTx runs fn in a READ COMMITTED transaction. Serialization failures
	// and deadlocks are retried a bounded number of times.
	InTx(ctx context.Context, fn func(ctx context.Context, r Repositories) error) error
	// InSnapshot runs fn in a REPEATABLE READ, READ ONLY transaction, giving
	// a consistent view across several queries.
	InSnapshot(ctx context.Context, fn func(ctx context.Context, r Repositories) error) error
}

// Repositories gives access to the repositories bound to one SQL transaction.
type Repositories interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// WalletRepository persists the wallet aggregate.
type WalletRepository interface {
	// Insert fails with ErrWalletAlreadyExists for a duplicated (player, currency).
	Insert(ctx context.Context, w *wallet.Wallet) error
	// Get fails with ErrWalletNotFound.
	Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// GetForUpdate locks the wallet row (SELECT ... FOR UPDATE) until commit.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// TryGetForUpdate locks the row with SKIP LOCKED; ok is false when another
	// transaction holds it.
	TryGetForUpdate(ctx context.Context, id uuid.UUID) (w *wallet.Wallet, ok bool, err error)
	// UpdateBalance writes balance and version guarded by the expected
	// previous version; it fails with ErrConcurrentUpdate when 0 rows match.
	UpdateBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

// TransactionRepository persists wager transactions.
type TransactionRepository interface {
	// Insert fails with ErrDuplicateTransaction on idempotency/uniqueness races.
	Insert(ctx context.Context, tx *wagering.Transaction) error
	// Update persists the state of a non-terminal transaction guarded by its
	// previous status.
	Update(ctx context.Context, tx *wagering.Transaction, expected wagering.Status) error
	Get(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error)
	GetByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error)
	// FindIdempotent returns the transactions of the provider matching the
	// idempotency key or the external id (at most two).
	FindIdempotent(ctx context.Context, providerID, idempotencyKey, externalID string) ([]*wagering.Transaction, error)
	// HasProcessedReversal reports whether a PROCESSED REFUND/ROLLBACK references id.
	HasProcessedReversal(ctx context.Context, id uuid.UUID) (bool, error)
	// WakeDependents makes pending operations waiting on (provider, externalID)
	// due immediately.
	WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) (int64, error)
	// DuePending lists PENDING/PENDING_REFERENCE transactions due for an attempt.
	DuePending(ctx context.Context, now time.Time, limit int) ([]PendingRef, error)
}

// PendingRef identifies a pending transaction and its wallet.
type PendingRef struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

// LedgerRepository appends and reads ledger entries.
type LedgerRepository interface {
	Append(ctx context.Context, e *wallet.LedgerEntry) error
	// List returns entries ordered by wallet version, after the given version.
	List(ctx context.Context, walletID uuid.UUID, afterVersion int64, limit int) ([]*wallet.LedgerEntry, error)
	// Summarize returns credits minus debits and the number of entries.
	Summarize(ctx context.Context, walletID uuid.UUID, currency money.Currency) (LedgerSummary, error)
}

// LedgerSummary is the ledger reconstruction of a wallet balance.
type LedgerSummary struct {
	Balance money.Money
	Entries int64
}

// OutboxRepository appends events in the caller's SQL transaction.
type OutboxRepository interface {
	Append(ctx context.Context, evs []events.Event) error
}

// InboxMessage identifies a consumed message.
type InboxMessage struct {
	Consumer    string
	MessageID   string
	PayloadHash string
	ReceivedAt  time.Time
}

// InboxRecord is the persisted state of a consumed message.
type InboxRecord struct {
	InboxMessage
	TransactionID uuid.UUID
	Outcome       string
	CompletedAt   time.Time
	Deliveries    int
}

// InboxRepository deduplicates consumed messages.
type InboxRepository interface {
	// Register inserts the message (INSERT ... ON CONFLICT DO NOTHING). When
	// it already exists the stored record is returned with inserted=false and
	// its delivery counter is incremented.
	Register(ctx context.Context, m InboxMessage) (rec InboxRecord, inserted bool, err error)
	Complete(ctx context.Context, consumer, messageID string, txID uuid.UUID, outcome string, now time.Time) error
}

// Clock returns the current time, truncated to microseconds (PostgreSQL precision).
type Clock interface{ Now() time.Time }

// SystemClock is the production clock.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// UUIDv7 generates time-ordered UUIDs.
type UUIDv7 struct{}

func (UUIDv7) NewID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

// Metrics is the observability port used by the use cases.
type Metrics interface {
	TransactionOutcome(source, kind, status string, replay bool)
	Duplicate(source, kind string)
	Retry(component string)
	ConcurrencyConflict(component string)
	ProcessingDuration(source string, d time.Duration)
	ReconciliationDivergence(walletID string)
}

// NopMetrics discards measurements.
type NopMetrics struct{}

func (NopMetrics) TransactionOutcome(string, string, string, bool) {}
func (NopMetrics) Duplicate(string, string)                        {}
func (NopMetrics) Retry(string)                                    {}
func (NopMetrics) ConcurrencyConflict(string)                      {}
func (NopMetrics) ProcessingDuration(string, time.Duration)        {}
func (NopMetrics) ReconciliationDivergence(string)                 {}
