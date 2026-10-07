// Package events defines the integration events emitted by the service.
// Each event has a concrete payload type; its type name and version are set
// by the constructor and cannot be chosen by callers.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
)

// ErrInvalidEvent reports missing envelope metadata.
var ErrInvalidEvent = errors.New("events: invalid event")

// Event type names.
const (
	TypeWagerTransactionProcessed        = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         = "WagerTransactionRejected"
	TypeWagerTransactionPendingReference = "WagerTransactionPendingReference"
	TypeWagerTransactionFailed           = "WagerTransactionFailed"
	TypeWalletBalanceChanged             = "WalletBalanceChanged"
)

// Aggregate type names.
const (
	AggregateWagerTransaction = "WagerTransaction"
	AggregateWallet           = "Wallet"
)

// Metadata is supplied by the caller of a constructor.
type Metadata struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   string // optional
	OccurredAt    time.Time
}

// Event is the read-only view of any integration event.
type Event interface {
	EventID() uuid.UUID
	EventType() string
	Version() int
	AggregateType() string
	AggregateID() uuid.UUID
	// GroupKey is the ordering key used by the broker (the wallet id).
	GroupKey() string
	CorrelationID() string
	CausationID() string
	OccurredAt() time.Time
	// Marshal returns the immutable JSON snapshot of the envelope.
	Marshal() ([]byte, error)
}

// payload is implemented by every concrete event payload.
type payload interface {
	eventType() string
	eventVersion() int
	aggregateType() string
	aggregateID() uuid.UUID
	groupKey() string
}

// Envelope is the generic, typed event envelope.
type Envelope[P payload] struct {
	meta Metadata
	data P
}

func newEnvelope[P payload](m Metadata, data P) (*Envelope[P], error) {
	if m.EventID == uuid.Nil {
		return nil, fmt.Errorf("%w: event id required", ErrInvalidEvent)
	}
	if m.CorrelationID == "" {
		return nil, fmt.Errorf("%w: correlation id required", ErrInvalidEvent)
	}
	if m.OccurredAt.IsZero() {
		return nil, fmt.Errorf("%w: occurredAt required", ErrInvalidEvent)
	}
	if data.aggregateID() == uuid.Nil {
		return nil, fmt.Errorf("%w: aggregate id required", ErrInvalidEvent)
	}
	m.OccurredAt = m.OccurredAt.UTC()
	return &Envelope[P]{meta: m, data: data}, nil
}

func (e *Envelope[P]) EventID() uuid.UUID     { return e.meta.EventID }
func (e *Envelope[P]) EventType() string      { return e.data.eventType() }
func (e *Envelope[P]) Version() int           { return e.data.eventVersion() }
func (e *Envelope[P]) AggregateType() string  { return e.data.aggregateType() }
func (e *Envelope[P]) AggregateID() uuid.UUID { return e.data.aggregateID() }
func (e *Envelope[P]) GroupKey() string       { return e.data.groupKey() }
func (e *Envelope[P]) CorrelationID() string  { return e.meta.CorrelationID }
func (e *Envelope[P]) CausationID() string    { return e.meta.CausationID }
func (e *Envelope[P]) OccurredAt() time.Time  { return e.meta.OccurredAt }
func (e *Envelope[P]) Data() P                { return e.data }

type wireEnvelope[P any] struct {
	EventID       string  `json:"eventId"`
	EventType     string  `json:"eventType"`
	AggregateType string  `json:"aggregateType"`
	AggregateID   string  `json:"aggregateId"`
	CorrelationID string  `json:"correlationId"`
	CausationID   *string `json:"causationId,omitempty"`
	OccurredAt    string  `json:"occurredAt"`
	Version       int     `json:"version"`
	Data          P       `json:"data"`
}

// Marshal renders the envelope as JSON.
func (e *Envelope[P]) Marshal() ([]byte, error) {
	w := wireEnvelope[P]{
		EventID:       e.meta.EventID.String(),
		EventType:     e.EventType(),
		AggregateType: e.AggregateType(),
		AggregateID:   e.AggregateID().String(),
		CorrelationID: e.meta.CorrelationID,
		OccurredAt:    FormatTime(e.meta.OccurredAt),
		Version:       e.Version(),
		Data:          e.data,
	}
	if e.meta.CausationID != "" {
		c := e.meta.CausationID
		w.CausationID = &c
	}
	return json.Marshal(w)
}

// FormatTime renders a UTC RFC 3339 timestamp with millisecond precision.
func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// Timestamp is a time rendered as UTC RFC 3339.
type Timestamp time.Time

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatTime(time.Time(t)))
}

// ---- WagerTransactionProcessed ----

// WagerTransactionProcessedData is emitted on every successful operation,
// including LOSS (no balance change) and the internal OPENING.
type WagerTransactionProcessedData struct {
	TransactionID          uuid.UUID   `json:"transactionId"`
	Origin                 string      `json:"origin"`
	Kind                   string      `json:"kind"`
	ProviderID             string      `json:"providerId,omitempty"`
	ExternalTransactionID  string      `json:"externalTransactionId,omitempty"`
	RoundID                string      `json:"roundId,omitempty"`
	GameID                 string      `json:"gameId,omitempty"`
	WalletID               uuid.UUID   `json:"walletId"`
	PlayerID               uuid.UUID   `json:"playerId"`
	Money                  money.Money `json:"money"`
	BalanceAfter           money.Money `json:"balanceAfter"`
	WalletVersion          int64       `json:"walletVersion"`
	ReferenceTransactionID *uuid.UUID  `json:"referenceTransactionId,omitempty"`
	ProcessedAt            Timestamp   `json:"processedAt"`
}

func (WagerTransactionProcessedData) eventType() string        { return TypeWagerTransactionProcessed }
func (WagerTransactionProcessedData) eventVersion() int        { return 1 }
func (WagerTransactionProcessedData) aggregateType() string    { return AggregateWagerTransaction }
func (d WagerTransactionProcessedData) aggregateID() uuid.UUID { return d.TransactionID }
func (d WagerTransactionProcessedData) groupKey() string       { return d.WalletID.String() }

// WagerTransactionProcessed is the concrete event type.
type WagerTransactionProcessed = Envelope[WagerTransactionProcessedData]

// NewWagerTransactionProcessed builds the event; type and version are fixed.
func NewWagerTransactionProcessed(m Metadata, d WagerTransactionProcessedData) (*WagerTransactionProcessed, error) {
	return newEnvelope(m, d)
}

// ---- WagerTransactionRejected ----

// WagerTransactionRejectedData is emitted on definitive business rejection.
type WagerTransactionRejectedData struct {
	TransactionID         uuid.UUID   `json:"transactionId"`
	Kind                  string      `json:"kind"`
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	RoundID               string      `json:"roundId"`
	GameID                string      `json:"gameId"`
	WalletID              uuid.UUID   `json:"walletId"`
	PlayerID              uuid.UUID   `json:"playerId"`
	Money                 money.Money `json:"money"`
	FailureCode           string      `json:"failureCode"`
	RejectedAt            Timestamp   `json:"rejectedAt"`
}

func (WagerTransactionRejectedData) eventType() string        { return TypeWagerTransactionRejected }
func (WagerTransactionRejectedData) eventVersion() int        { return 1 }
func (WagerTransactionRejectedData) aggregateType() string    { return AggregateWagerTransaction }
func (d WagerTransactionRejectedData) aggregateID() uuid.UUID { return d.TransactionID }
func (d WagerTransactionRejectedData) groupKey() string       { return d.WalletID.String() }

// WagerTransactionRejected is the concrete event type.
type WagerTransactionRejected = Envelope[WagerTransactionRejectedData]

// NewWagerTransactionRejected builds the event; type and version are fixed.
func NewWagerTransactionRejected(m Metadata, d WagerTransactionRejectedData) (*WagerTransactionRejected, error) {
	return newEnvelope(m, d)
}

// ---- WagerTransactionPendingReference ----

// WagerTransactionPendingReferenceData is emitted once, when an operation
// starts waiting for its reference.
type WagerTransactionPendingReferenceData struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	Kind                           string      `json:"kind"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	Money                          money.Money `json:"money"`
	NextAttemptAt                  Timestamp   `json:"nextAttemptAt"`
	ExpiresAt                      Timestamp   `json:"expiresAt"`
}

func (WagerTransactionPendingReferenceData) eventType() string {
	return TypeWagerTransactionPendingReference
}
func (WagerTransactionPendingReferenceData) eventVersion() int        { return 1 }
func (WagerTransactionPendingReferenceData) aggregateType() string    { return AggregateWagerTransaction }
func (d WagerTransactionPendingReferenceData) aggregateID() uuid.UUID { return d.TransactionID }
func (d WagerTransactionPendingReferenceData) groupKey() string       { return d.WalletID.String() }

// WagerTransactionPendingReference is the concrete event type.
type WagerTransactionPendingReference = Envelope[WagerTransactionPendingReferenceData]

// NewWagerTransactionPendingReference builds the event; type and version are fixed.
func NewWagerTransactionPendingReference(m Metadata, d WagerTransactionPendingReferenceData) (*WagerTransactionPendingReference, error) {
	return newEnvelope(m, d)
}

// ---- WagerTransactionFailed ----

// WagerTransactionFailedData is emitted when an operation is finalized as
// FAILED (permanent infrastructure failure recorded for audit).
type WagerTransactionFailedData struct {
	TransactionID         uuid.UUID   `json:"transactionId"`
	Kind                  string      `json:"kind"`
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	WalletID              uuid.UUID   `json:"walletId"`
	Money                 money.Money `json:"money"`
	FailureCode           string      `json:"failureCode"`
	FailedAt              Timestamp   `json:"failedAt"`
}

func (WagerTransactionFailedData) eventType() string        { return TypeWagerTransactionFailed }
func (WagerTransactionFailedData) eventVersion() int        { return 1 }
func (WagerTransactionFailedData) aggregateType() string    { return AggregateWagerTransaction }
func (d WagerTransactionFailedData) aggregateID() uuid.UUID { return d.TransactionID }
func (d WagerTransactionFailedData) groupKey() string       { return d.WalletID.String() }

// WagerTransactionFailed is the concrete event type.
type WagerTransactionFailed = Envelope[WagerTransactionFailedData]

// NewWagerTransactionFailed builds the event; type and version are fixed.
func NewWagerTransactionFailed(m Metadata, d WagerTransactionFailedData) (*WagerTransactionFailed, error) {
	return newEnvelope(m, d)
}

// ---- WalletBalanceChanged ----

// WalletBalanceChangedData is emitted for every effective balance change.
type WalletBalanceChangedData struct {
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     string      `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
}

func (WalletBalanceChangedData) eventType() string        { return TypeWalletBalanceChanged }
func (WalletBalanceChangedData) eventVersion() int        { return 1 }
func (WalletBalanceChangedData) aggregateType() string    { return AggregateWallet }
func (d WalletBalanceChangedData) aggregateID() uuid.UUID { return d.WalletID }
func (d WalletBalanceChangedData) groupKey() string       { return d.WalletID.String() }

// WalletBalanceChanged is the concrete event type.
type WalletBalanceChanged = Envelope[WalletBalanceChangedData]

// NewWalletBalanceChanged builds the event; type and version are fixed.
func NewWalletBalanceChanged(m Metadata, d WalletBalanceChangedData) (*WalletBalanceChanged, error) {
	return newEnvelope(m, d)
}
