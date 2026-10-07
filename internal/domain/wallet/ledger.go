package wallet

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
)

// Direction of a ledger entry.
type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

// ParseDirection validates a persisted direction.
func ParseDirection(s string) (Direction, error) {
	switch Direction(s) {
	case Debit, Credit:
		return Direction(s), nil
	}
	return "", fmt.Errorf("%w: direction %q", ErrInvalidLedgerEntry, s)
}

// Opposite returns the reverse direction.
func (d Direction) Opposite() Direction {
	if d == Debit {
		return Credit
	}
	return Debit
}

// LedgerEntry is an immutable, append-only record of a balance movement.
type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	walletVersion int64
	createdAt     time.Time
}

// LedgerEntryParams are the fields of a ledger entry.
type LedgerEntryParams struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
	CreatedAt     time.Time
}

// NewLedgerEntry validates balanceAfter = balanceBefore ± amount according to
// the direction. It is used both for new entries and for rehydration, since
// entries are immutable and carry no transitions.
func NewLedgerEntry(p LedgerEntryParams) (*LedgerEntry, error) {
	if p.ID == uuid.Nil || p.WalletID == uuid.Nil || p.TransactionID == uuid.Nil {
		return nil, fmt.Errorf("%w: ids are required", ErrInvalidLedgerEntry)
	}
	if _, err := ParseDirection(string(p.Direction)); err != nil {
		return nil, err
	}
	for _, m := range []money.Money{p.Amount, p.BalanceBefore, p.BalanceAfter} {
		if err := m.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidLedgerEntry, err)
		}
	}
	if !p.Amount.IsPositive() {
		return nil, fmt.Errorf("%w: amount must be positive", ErrInvalidLedgerEntry)
	}
	if p.BalanceBefore.IsNegative() || p.BalanceAfter.IsNegative() {
		return nil, fmt.Errorf("%w: balances must be non-negative", ErrInvalidLedgerEntry)
	}
	var expected money.Money
	var err error
	if p.Direction == Credit {
		expected, err = p.BalanceBefore.Add(p.Amount)
	} else {
		expected, err = p.BalanceBefore.Sub(p.Amount)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLedgerEntry, err)
	}
	if !expected.Equal(p.BalanceAfter) {
		return nil, fmt.Errorf("%w: balanceAfter %s != balanceBefore %s %s %s",
			ErrInvalidLedgerEntry, p.BalanceAfter, p.BalanceBefore, p.Direction, p.Amount)
	}
	if p.WalletVersion < InitialVersion {
		return nil, fmt.Errorf("%w: wallet version %d", ErrInvalidLedgerEntry, p.WalletVersion)
	}
	if p.CreatedAt.IsZero() {
		return nil, fmt.Errorf("%w: timestamp required", ErrInvalidLedgerEntry)
	}
	return &LedgerEntry{
		id:            p.ID,
		walletID:      p.WalletID,
		transactionID: p.TransactionID,
		direction:     p.Direction,
		amount:        p.Amount,
		balanceBefore: p.BalanceBefore,
		balanceAfter:  p.BalanceAfter,
		walletVersion: p.WalletVersion,
		createdAt:     p.CreatedAt.UTC(),
	}, nil
}

func (e *LedgerEntry) ID() uuid.UUID              { return e.id }
func (e *LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e *LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e *LedgerEntry) Direction() Direction       { return e.direction }
func (e *LedgerEntry) Amount() money.Money        { return e.amount }
func (e *LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e *LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e *LedgerEntry) WalletVersion() int64       { return e.walletVersion }
func (e *LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
