package wallet_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

var now = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func open(t *testing.T, initial string) wallet.Opening {
	t.Helper()
	op, err := wallet.Open(wallet.OpenParams{
		ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: brl(t, initial),
		OpeningTransactionID: uuid.New(), EntryID: uuid.New(), Now: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func mv(t *testing.T, amount string) wallet.MovementParams {
	return wallet.MovementParams{EntryID: uuid.New(), TransactionID: uuid.New(), Amount: brl(t, amount), Now: now.Add(time.Second)}
}

func TestOpenWithPositiveBalanceCreatesCreditAtVersionOne(t *testing.T) {
	op := open(t, "1000.00")
	w := op.Wallet
	if w.Version() != 1 || w.Balance().Amount() != "1000.00" || w.Currency().Code() != "BRL" {
		t.Fatalf("wallet = v%d %s", w.Version(), w.Balance())
	}
	e := op.Entry
	if e == nil || e.Direction() != wallet.Credit || e.BalanceBefore().Amount() != "0.00" ||
		e.BalanceAfter().Amount() != "1000.00" || e.WalletVersion() != 1 {
		t.Fatalf("opening entry = %+v", e)
	}
}

func TestOpenWithZeroBalanceHasNoEntry(t *testing.T) {
	op := open(t, "0.00")
	if op.Entry != nil || op.Wallet.Version() != 1 || !op.Wallet.Balance().IsZero() {
		t.Fatalf("zero opening = %+v", op)
	}
}

func TestOpenRejectsInvalidInput(t *testing.T) {
	neg, _ := money.Parse("-1.00", "BRL")
	cases := []wallet.OpenParams{
		{ID: uuid.Nil, PlayerID: uuid.New(), InitialBalance: brl(t, "1.00"), Now: now},
		{ID: uuid.New(), PlayerID: uuid.Nil, InitialBalance: brl(t, "1.00"), Now: now},
		{ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: money.Money{}, Now: now},
		{ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: neg, Now: now},
		{ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: brl(t, "1.00")},
	}
	for i, c := range cases {
		if _, err := wallet.Open(c); !errors.Is(err, wallet.ErrInvalidWallet) {
			t.Errorf("case %d: %v", i, err)
		}
	}
}

func TestDebitKeepsBalanceNonNegative(t *testing.T) {
	w := open(t, "100.00").Wallet
	e, err := w.Debit(mv(t, "80.00"))
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("after debit: %s v%d", w.Balance(), w.Version())
	}
	if e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "20.00" || e.WalletVersion() != 2 {
		t.Fatalf("entry = %s -> %s v%d", e.BalanceBefore(), e.BalanceAfter(), e.WalletVersion())
	}
	if _, err := w.Debit(mv(t, "80.00")); !errors.Is(err, wallet.ErrInsufficientFunds) {
		t.Fatalf("second debit = %v", err)
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("rejected debit changed state: %s v%d", w.Balance(), w.Version())
	}
	if _, err := w.Debit(mv(t, "20.00")); err != nil || !w.Balance().IsZero() {
		t.Fatalf("debit to zero: %v %s", err, w.Balance())
	}
}

func TestCreditIncrementsVersion(t *testing.T) {
	w := open(t, "0.00").Wallet
	e, err := w.Credit(mv(t, "10.50"))
	if err != nil || w.Balance().Amount() != "10.50" || w.Version() != 2 || e.Direction() != wallet.Credit {
		t.Fatalf("credit: %v %s v%d", err, w.Balance(), w.Version())
	}
}

func TestMovementValidation(t *testing.T) {
	w := open(t, "10.00").Wallet
	usd, _ := money.Parse("1.00", "USD")
	if _, err := w.Credit(wallet.MovementParams{EntryID: uuid.New(), TransactionID: uuid.New(), Amount: usd, Now: now}); !errors.Is(err, wallet.ErrCurrencyMismatch) {
		t.Errorf("currency mismatch: %v", err)
	}
	if _, err := w.Debit(mv(t, "0.00")); !errors.Is(err, wallet.ErrInvalidMovement) {
		t.Errorf("zero movement: %v", err)
	}
	if _, err := w.Credit(wallet.MovementParams{EntryID: uuid.New(), TransactionID: uuid.New(), Amount: money.Money{}, Now: now}); err == nil {
		t.Error("uninitialized money accepted")
	}
	if w.Version() != 1 || w.Balance().Amount() != "10.00" {
		t.Fatalf("invalid movements changed state")
	}
}

func TestCreditOverflow(t *testing.T) {
	big, _ := money.Parse("92233720368547758.07", "BRL")
	op, err := wallet.Open(wallet.OpenParams{ID: uuid.New(), PlayerID: uuid.New(), InitialBalance: big,
		OpeningTransactionID: uuid.New(), EntryID: uuid.New(), Now: now})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := op.Wallet.Credit(mv(t, "0.01")); !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("overflow credit = %v", err)
	}
}

func TestRehydrateDoesNotApplyMovements(t *testing.T) {
	id, player := uuid.New(), uuid.New()
	w, err := wallet.Rehydrate(wallet.Snapshot{ID: id, PlayerID: player, Balance: brl(t, "975.00"), Version: 2, CreatedAt: now, UpdatedAt: now})
	if err != nil || w.Version() != 2 || w.Balance().Amount() != "975.00" {
		t.Fatalf("rehydrate = %v %v", w, err)
	}
	neg, _ := money.Parse("-1.00", "BRL")
	for i, s := range []wallet.Snapshot{
		{ID: id, PlayerID: player, Balance: neg, Version: 1, CreatedAt: now, UpdatedAt: now},
		{ID: id, PlayerID: player, Balance: brl(t, "1.00"), Version: 0, CreatedAt: now, UpdatedAt: now},
		{ID: id, PlayerID: player, Balance: money.Money{}, Version: 1, CreatedAt: now, UpdatedAt: now},
		{ID: uuid.Nil, PlayerID: player, Balance: brl(t, "1.00"), Version: 1, CreatedAt: now, UpdatedAt: now},
	} {
		if _, err := wallet.Rehydrate(s); !errors.Is(err, wallet.ErrInvalidWallet) {
			t.Errorf("case %d: %v", i, err)
		}
	}
}

func TestLedgerEntryValidatesBalanceMath(t *testing.T) {
	base := wallet.LedgerEntryParams{
		ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), Direction: wallet.Debit,
		Amount: brl(t, "25.00"), BalanceBefore: brl(t, "100.00"), BalanceAfter: brl(t, "75.00"),
		WalletVersion: 2, CreatedAt: now,
	}
	if _, err := wallet.NewLedgerEntry(base); err != nil {
		t.Fatal(err)
	}
	bad := base
	bad.BalanceAfter = brl(t, "125.00")
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Errorf("debit with credit math: %v", err)
	}
	bad = base
	bad.Direction = wallet.Credit
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Errorf("credit with debit math: %v", err)
	}
	bad = base
	bad.Direction = "SIDEWAYS"
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Errorf("unknown direction: %v", err)
	}
	bad = base
	bad.Amount, bad.BalanceAfter = brl(t, "0.00"), brl(t, "100.00")
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Errorf("zero amount: %v", err)
	}
	bad = base
	bad.Amount, bad.BalanceAfter = brl(t, "150.00"), brl(t, "-50.00")
	if _, err := wallet.NewLedgerEntry(bad); !errors.Is(err, wallet.ErrInvalidLedgerEntry) {
		t.Errorf("negative after: %v", err)
	}
	if wallet.Debit.Opposite() != wallet.Credit || wallet.Credit.Opposite() != wallet.Debit {
		t.Error("Opposite")
	}
}
