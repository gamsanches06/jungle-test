package application

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

// WalletService handles internal wallet operations and read models.
type WalletService struct {
	tx      TxManager
	ids     wagering.IDGenerator
	clock   Clock
	metrics Metrics
	log     *slog.Logger
}

// NewWalletService builds the service.
func NewWalletService(tx TxManager, ids wagering.IDGenerator, clock Clock, metrics Metrics, log *slog.Logger) *WalletService {
	return &WalletService{tx: tx, ids: ids, clock: clock, metrics: metrics, log: log}
}

// OpenWallet creates the wallet and, for a positive initial balance, the
// OPENING transaction, its ledger entry and outbox events in one commit.
func (s *WalletService) OpenWallet(ctx context.Context, playerID uuid.UUID, initial money.Money, correlationID string) (*wallet.Wallet, error) {
	if playerID == uuid.Nil {
		return nil, &wagering.ValidationError{Field: "playerId", Reason: "required"}
	}
	var w *wallet.Wallet
	err := s.tx.InTx(ctx, func(ctx context.Context, r Repositories) error {
		res, err := wagering.OpenWallet(wagering.OpenWalletInput{
			WalletID: s.ids.NewID(), PlayerID: playerID, InitialBalance: initial,
			IDs: s.ids, CorrelationID: correlationID, Now: s.clock.Now(),
		})
		if err != nil {
			return err
		}
		if err := r.Wallets().Insert(ctx, res.Wallet); err != nil {
			return err
		}
		if res.Opening != nil {
			if err := r.Transactions().Insert(ctx, res.Opening); err != nil {
				return err
			}
			if err := r.Ledger().Append(ctx, res.Entry); err != nil {
				return err
			}
			if err := r.Outbox().Append(ctx, res.Events); err != nil {
				return err
			}
		}
		w = res.Wallet
		return nil
	})
	if err != nil {
		return nil, err
	}
	s.log.InfoContext(ctx, "wallet opened", slog.String("walletId", w.ID().String()), slog.Int64("version", w.Version()))
	return w, nil
}

// GetWallet returns the current wallet state.
func (s *WalletService) GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	var w *wallet.Wallet
	err := s.tx.InSnapshot(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		w, err = r.Wallets().Get(ctx, id)
		return err
	})
	return w, err
}

// LedgerPage is one page of ledger entries ordered by wallet version.
type LedgerPage struct {
	Entries     []*wallet.LedgerEntry
	NextVersion int64 // 0 when there is no further page
}

// ListLedger returns entries with wallet version greater than afterVersion.
func (s *WalletService) ListLedger(ctx context.Context, walletID uuid.UUID, afterVersion int64, limit int) (LedgerPage, error) {
	var page LedgerPage
	err := s.tx.InSnapshot(ctx, func(ctx context.Context, r Repositories) error {
		if _, err := r.Wallets().Get(ctx, walletID); err != nil {
			return err
		}
		entries, err := r.Ledger().List(ctx, walletID, afterVersion, limit+1)
		if err != nil {
			return err
		}
		if len(entries) > limit {
			entries = entries[:limit]
			page.NextVersion = entries[len(entries)-1].WalletVersion()
		}
		page.Entries = entries
		return nil
	})
	return page, err
}

// Reconciliation compares the stored balance with the ledger reconstruction.
type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

// Reconcile rebuilds the balance from the ledger (including the opening) in
// a REPEATABLE READ snapshot. It never modifies the balance.
func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var rec Reconciliation
	err := s.tx.InSnapshot(ctx, func(ctx context.Context, r Repositories) error {
		w, err := r.Wallets().Get(ctx, walletID)
		if err != nil {
			return err
		}
		sum, err := r.Ledger().Summarize(ctx, walletID, w.Currency())
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(sum.Balance)
		if err != nil {
			return err
		}
		rec = Reconciliation{
			WalletID:          walletID,
			StoredBalance:     w.Balance(),
			CalculatedBalance: sum.Balance,
			Difference:        diff,
			Consistent:        diff.IsZero(),
			CheckedEntries:    sum.Entries,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if !rec.Consistent {
		s.metrics.ReconciliationDivergence(walletID.String())
		s.log.ErrorContext(ctx, "wallet reconciliation divergence",
			slog.String("walletId", walletID.String()),
			slog.String("storedBalance", rec.StoredBalance.Amount()),
			slog.String("calculatedBalance", rec.CalculatedBalance.Amount()),
			slog.String("difference", rec.Difference.Amount()),
			slog.Int64("checkedEntries", rec.CheckedEntries))
	} else {
		s.log.InfoContext(ctx, "wallet reconciliation consistent",
			slog.String("walletId", walletID.String()), slog.Int64("checkedEntries", rec.CheckedEntries))
	}
	return rec, nil
}

// QueryService serves transaction lookups.
type QueryService struct{ tx TxManager }

// NewQueryService builds the service.
func NewQueryService(tx TxManager) *QueryService { return &QueryService{tx: tx} }

// GetTransaction returns a transaction by internal id.
func (s *QueryService) GetTransaction(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	var t *wagering.Transaction
	err := s.tx.InSnapshot(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		t, err = r.Transactions().Get(ctx, id)
		return err
	})
	return t, err
}

// GetByExternalID returns a transaction by (providerId, externalTransactionId).
func (s *QueryService) GetByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	var t *wagering.Transaction
	err := s.tx.InSnapshot(ctx, func(ctx context.Context, r Repositories) error {
		var err error
		t, err = r.Transactions().GetByExternalID(ctx, providerID, externalID)
		return err
	})
	return t, err
}
