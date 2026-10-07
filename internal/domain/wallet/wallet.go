// Package wallet holds the wallet aggregate root and its immutable ledger
// entries. It is independent from transport, persistence and composition.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
)

var (
	// ErrInvalidWallet reports a construction or rehydration with invalid data.
	ErrInvalidWallet = errors.New("wallet: invalid wallet")
	// ErrInsufficientFunds reports a debit that would make the balance negative.
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	// ErrCurrencyMismatch reports a movement in a currency other than the wallet's.
	ErrCurrencyMismatch = errors.New("wallet: currency mismatch")
	// ErrInvalidMovement reports a non-positive movement amount.
	ErrInvalidMovement = errors.New("wallet: movement amount must be positive")
	// ErrInvalidLedgerEntry reports an entry whose balances are inconsistent.
	ErrInvalidLedgerEntry = errors.New("wallet: invalid ledger entry")
)

// InitialVersion is the version of a freshly opened wallet.
const InitialVersion int64 = 1

// Wallet is the financial aggregate root. Its balance can only change through
// Debit and Credit, which also produce the matching ledger entry.
type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	currency  money.Currency
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// Opening is the result of opening a wallet: the wallet itself and, when the
// initial balance is positive, the credit entry of the OPENING transaction.
type Opening struct {
	Wallet *Wallet
	Entry  *LedgerEntry
}

// OpenParams carries the identities chosen by the caller when opening a wallet.
type OpenParams struct {
	ID             uuid.UUID
	PlayerID       uuid.UUID
	InitialBalance money.Money
	// OpeningTransactionID and EntryID are only used for a positive initial balance.
	OpeningTransactionID uuid.UUID
	EntryID              uuid.UUID
	Now                  time.Time
}

// Open creates a new wallet at version 1. A positive initial balance produces
// a CREDIT ledger entry from 0 to the initial balance at version 1; a zero
// initial balance produces no entry.
func Open(p OpenParams) (Opening, error) {
	if p.ID == uuid.Nil || p.PlayerID == uuid.Nil {
		return Opening{}, fmt.Errorf("%w: id and player id are required", ErrInvalidWallet)
	}
	if err := p.InitialBalance.Validate(); err != nil {
		return Opening{}, fmt.Errorf("%w: %v", ErrInvalidWallet, err)
	}
	if p.InitialBalance.IsNegative() {
		return Opening{}, fmt.Errorf("%w: negative initial balance", ErrInvalidWallet)
	}
	if p.Now.IsZero() {
		return Opening{}, fmt.Errorf("%w: timestamp required", ErrInvalidWallet)
	}
	now := p.Now.UTC()
	w := &Wallet{
		id:        p.ID,
		playerID:  p.PlayerID,
		currency:  p.InitialBalance.Currency(),
		balance:   p.InitialBalance,
		version:   InitialVersion,
		createdAt: now,
		updatedAt: now,
	}
	if p.InitialBalance.IsZero() {
		return Opening{Wallet: w}, nil
	}
	zero, _ := money.Zero(w.currency)
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID:            p.EntryID,
		WalletID:      w.id,
		TransactionID: p.OpeningTransactionID,
		Direction:     Credit,
		Amount:        p.InitialBalance,
		BalanceBefore: zero,
		BalanceAfter:  p.InitialBalance,
		WalletVersion: InitialVersion,
		CreatedAt:     now,
	})
	if err != nil {
		return Opening{}, err
	}
	return Opening{Wallet: w, Entry: entry}, nil
}

// Snapshot is the persisted state used for rehydration.
type Snapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Rehydrate rebuilds a wallet from persisted state without applying any
// movement or producing ledger entries.
func Rehydrate(s Snapshot) (*Wallet, error) {
	if s.ID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: id and player id are required", ErrInvalidWallet)
	}
	if err := s.Balance.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidWallet, err)
	}
	if s.Balance.IsNegative() {
		return nil, fmt.Errorf("%w: negative balance", ErrInvalidWallet)
	}
	if s.Version < InitialVersion {
		return nil, fmt.Errorf("%w: version %d", ErrInvalidWallet, s.Version)
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() {
		return nil, fmt.Errorf("%w: timestamps required", ErrInvalidWallet)
	}
	return &Wallet{
		id:        s.ID,
		playerID:  s.PlayerID,
		currency:  s.Balance.Currency(),
		balance:   s.Balance,
		version:   s.Version,
		createdAt: s.CreatedAt.UTC(),
		updatedAt: s.UpdatedAt.UTC(),
	}, nil
}

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Currency() money.Currency { return w.currency }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

// MovementParams identifies a balance movement.
type MovementParams struct {
	EntryID       uuid.UUID
	TransactionID uuid.UUID
	Amount        money.Money
	Now           time.Time
}

// Debit subtracts the amount. It never lets the balance become negative.
func (w *Wallet) Debit(p MovementParams) (*LedgerEntry, error) {
	if err := w.checkMovement(p); err != nil {
		return nil, err
	}
	after, err := w.balance.Sub(p.Amount)
	if err != nil {
		return nil, err
	}
	if after.IsNegative() {
		return nil, ErrInsufficientFunds
	}
	return w.apply(Debit, p, after)
}

// Credit adds the amount.
func (w *Wallet) Credit(p MovementParams) (*LedgerEntry, error) {
	if err := w.checkMovement(p); err != nil {
		return nil, err
	}
	after, err := w.balance.Add(p.Amount)
	if err != nil {
		return nil, err
	}
	return w.apply(Credit, p, after)
}

// CanDebit reports whether a debit of amount would keep the balance >= 0.
func (w *Wallet) CanDebit(amount money.Money) (bool, error) {
	c, err := w.balance.Cmp(amount)
	if err != nil {
		return false, err
	}
	return c >= 0, nil
}

func (w *Wallet) checkMovement(p MovementParams) error {
	if err := p.Amount.Validate(); err != nil {
		return err
	}
	if p.Amount.Currency() != w.currency {
		return fmt.Errorf("%w: wallet %s, movement %s", ErrCurrencyMismatch, w.currency, p.Amount.Currency())
	}
	if !p.Amount.IsPositive() {
		return ErrInvalidMovement
	}
	if p.Now.IsZero() {
		return fmt.Errorf("%w: timestamp required", ErrInvalidMovement)
	}
	return nil
}

func (w *Wallet) apply(dir Direction, p MovementParams, after money.Money) (*LedgerEntry, error) {
	entry, err := NewLedgerEntry(LedgerEntryParams{
		ID:            p.EntryID,
		WalletID:      w.id,
		TransactionID: p.TransactionID,
		Direction:     dir,
		Amount:        p.Amount,
		BalanceBefore: w.balance,
		BalanceAfter:  after,
		WalletVersion: w.version + 1,
		CreatedAt:     p.Now.UTC(),
	})
	if err != nil {
		return nil, err
	}
	w.balance = after
	w.version++
	w.updatedAt = p.Now.UTC()
	return entry, nil
}

// Snapshot exports the state for persistence adapters.
func (w *Wallet) Snapshot() Snapshot {
	return Snapshot{ID: w.id, PlayerID: w.playerID, Balance: w.balance, Version: w.version, CreatedAt: w.createdAt, UpdatedAt: w.updatedAt}
}
