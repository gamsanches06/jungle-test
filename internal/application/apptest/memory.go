// Package apptest provides an in-memory TxManager for unit tests of the use
// cases and HTTP handlers. Production guarantees are tested against
// PostgreSQL in test/integration.
package apptest

import (
	"context"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/domain/events"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

// MemStore is an in-memory TxManager for unit tests of the use cases. It
// serializes transactions with one mutex and restores a copy on rollback.
// The real guarantees are exercised against PostgreSQL in test/integration.
type MemStore struct {
	mu          sync.Mutex
	WalletsByID map[uuid.UUID]wallet.Snapshot
	Txs         map[uuid.UUID]wagering.Snapshot
	Ledger      []*wallet.LedgerEntry
	Outbox      []events.Event
	Inboxes     map[string]application.InboxRecord
}

// NewMemStore builds an empty store.
func NewMemStore() *MemStore {
	return &MemStore{WalletsByID: map[uuid.UUID]wallet.Snapshot{}, Txs: map[uuid.UUID]wagering.Snapshot{}, Inboxes: map[string]application.InboxRecord{}}
}

func (m *MemStore) InTx(ctx context.Context, fn func(context.Context, application.Repositories) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	w, t, l, o, i := maps.Clone(m.WalletsByID), maps.Clone(m.Txs), append([]*wallet.LedgerEntry(nil), m.Ledger...), append([]events.Event(nil), m.Outbox...), maps.Clone(m.Inboxes)
	if err := fn(ctx, memRepos{m}); err != nil {
		m.WalletsByID, m.Txs, m.Ledger, m.Outbox, m.Inboxes = w, t, l, o, i
		return err
	}
	return nil
}

func (m *MemStore) InSnapshot(ctx context.Context, fn func(context.Context, application.Repositories) error) error {
	return m.InTx(ctx, fn)
}

type memRepos struct{ m *MemStore }

func (r memRepos) Wallets() application.WalletRepository           { return memWallets(r) }
func (r memRepos) Transactions() application.TransactionRepository { return memTxs(r) }
func (r memRepos) Ledger() application.LedgerRepository            { return memLedger(r) }
func (r memRepos) Outbox() application.OutboxRepository            { return memOutbox(r) }
func (r memRepos) Inbox() application.InboxRepository              { return memInbox(r) }

type memWallets memRepos

func (r memWallets) Insert(_ context.Context, w *wallet.Wallet) error {
	for _, s := range r.m.WalletsByID {
		if s.PlayerID == w.PlayerID() && s.Balance.Currency() == w.Currency() {
			return application.ErrWalletAlreadyExists
		}
	}
	r.m.WalletsByID[w.ID()] = w.Snapshot()
	return nil
}

func (r memWallets) Get(_ context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	s, ok := r.m.WalletsByID[id]
	if !ok {
		return nil, application.ErrWalletNotFound
	}
	return wallet.Rehydrate(s)
}

func (r memWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return r.Get(ctx, id)
}

func (r memWallets) TryGetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, bool, error) {
	w, err := r.Get(ctx, id)
	return w, err == nil, err
}

func (r memWallets) UpdateBalance(_ context.Context, w *wallet.Wallet, expected int64) error {
	if r.m.WalletsByID[w.ID()].Version != expected {
		return application.ErrConcurrentUpdate
	}
	r.m.WalletsByID[w.ID()] = w.Snapshot()
	return nil
}

type memTxs memRepos

func (r memTxs) Insert(_ context.Context, t *wagering.Transaction) error {
	for _, s := range r.m.Txs {
		if s.External != nil && t.IsExternal() && s.External.ProviderID == t.ProviderID() {
			ext, _ := t.External()
			if s.External.ExternalTransactionID == ext.ExternalTransactionID || s.External.IdempotencyKey == ext.IdempotencyKey {
				return application.ErrDuplicateTransaction
			}
		}
	}
	r.m.Txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) Update(_ context.Context, t *wagering.Transaction, expected wagering.Status) error {
	if r.m.Txs[t.ID()].Status != expected {
		return application.ErrConcurrentUpdate
	}
	r.m.Txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) Get(_ context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	s, ok := r.m.Txs[id]
	if !ok {
		return nil, application.ErrTransactionNotFound
	}
	return wagering.Rehydrate(s)
}

func (r memTxs) GetForUpdate(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return r.Get(ctx, id)
}

func (r memTxs) GetByExternalID(_ context.Context, provider, ext string) (*wagering.Transaction, error) {
	for _, s := range r.m.Txs {
		if s.External != nil && s.External.ProviderID == provider && s.External.ExternalTransactionID == ext {
			return wagering.Rehydrate(s)
		}
	}
	return nil, application.ErrTransactionNotFound
}

func (r memTxs) FindIdempotent(_ context.Context, provider, key, ext string) ([]*wagering.Transaction, error) {
	var out []*wagering.Transaction
	for _, s := range r.m.Txs {
		if s.External != nil && s.External.ProviderID == provider && (s.External.IdempotencyKey == key || s.External.ExternalTransactionID == ext) {
			t, err := wagering.Rehydrate(s)
			if err != nil {
				return nil, err
			}
			out = append(out, t)
		}
	}
	return out, nil
}

func (r memTxs) HasProcessedReversal(_ context.Context, id uuid.UUID) (bool, error) {
	for _, s := range r.m.Txs {
		if s.ReferenceTxID == id && s.Status == wagering.StatusProcessed && (s.Kind == wagering.KindRefund || s.Kind == wagering.KindRollback) {
			return true, nil
		}
	}
	return false, nil
}

func (r memTxs) WakeDependents(context.Context, string, string, time.Time) (int64, error) {
	return 0, nil
}

func (r memTxs) DuePending(_ context.Context, now time.Time, limit int) ([]application.PendingRef, error) {
	var out []application.PendingRef
	for _, s := range r.m.Txs {
		if !s.Status.IsTerminal() && !s.NextAttemptAt.After(now) {
			out = append(out, application.PendingRef{ID: s.ID, WalletID: s.WalletID})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.String() < out[j].ID.String() })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type memLedger memRepos

func (r memLedger) Append(_ context.Context, e *wallet.LedgerEntry) error {
	r.m.Ledger = append(r.m.Ledger, e)
	return nil
}

func (r memLedger) List(_ context.Context, walletID uuid.UUID, after int64, limit int) ([]*wallet.LedgerEntry, error) {
	var out []*wallet.LedgerEntry
	for _, e := range r.m.Ledger {
		if e.WalletID() == walletID && e.WalletVersion() > after && len(out) < limit {
			out = append(out, e)
		}
	}
	return out, nil
}

func (r memLedger) Summarize(_ context.Context, walletID uuid.UUID, c money.Currency) (application.LedgerSummary, error) {
	total, _ := money.Zero(c)
	var n int64
	for _, e := range r.m.Ledger {
		if e.WalletID() != walletID {
			continue
		}
		var err error
		if e.Direction() == wallet.Credit {
			total, err = total.Add(e.Amount())
		} else {
			total, err = total.Sub(e.Amount())
		}
		if err != nil {
			return application.LedgerSummary{}, err
		}
		n++
	}
	return application.LedgerSummary{Balance: total, Entries: n}, nil
}

type memOutbox memRepos

func (r memOutbox) Append(_ context.Context, evs []events.Event) error {
	r.m.Outbox = append(r.m.Outbox, evs...)
	return nil
}

type memInbox memRepos

func (r memInbox) Register(_ context.Context, msg application.InboxMessage) (application.InboxRecord, bool, error) {
	k := msg.Consumer + "/" + msg.MessageID
	if rec, ok := r.m.Inboxes[k]; ok {
		rec.Deliveries++
		r.m.Inboxes[k] = rec
		return rec, false, nil
	}
	r.m.Inboxes[k] = application.InboxRecord{InboxMessage: msg, Deliveries: 1}
	return r.m.Inboxes[k], true, nil
}

func (r memInbox) Complete(_ context.Context, consumer, id string, txID uuid.UUID, outcome string, now time.Time) error {
	k := consumer + "/" + id
	rec := r.m.Inboxes[k]
	rec.TransactionID, rec.Outcome, rec.CompletedAt = txID, outcome, now
	r.m.Inboxes[k] = rec
	return nil
}
