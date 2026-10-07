package application_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/application/apptest"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
)

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

type env struct {
	store              *apptest.MemStore
	process            *application.ProcessService
	wallets            *application.WalletService
	pending            *application.PendingService
	walletID, playerID uuid.UUID
}

func setup(t *testing.T, initial string) *env {
	t.Helper()
	store := apptest.NewMemStore()
	policy := wagering.PendingPolicy{BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, MaxAttempts: 3, TTL: time.Hour}
	proc, err := application.NewProcessService(store, application.UUIDv7{}, application.SystemClock{}, policy, application.NopMetrics{}, discard)
	if err != nil {
		t.Fatal(err)
	}
	ws := application.NewWalletService(store, application.UUIDv7{}, application.SystemClock{}, application.NopMetrics{}, discard)
	player := uuid.New()
	m, _ := money.Parse(initial, "BRL")
	w, err := ws.OpenWallet(context.Background(), player, m, "corr")
	if err != nil {
		t.Fatal(err)
	}
	return &env{store: store, process: proc, wallets: ws, walletID: w.ID(), playerID: player,
		pending: application.NewPendingService(store, application.UUIDv7{}, application.SystemClock{}, policy, application.NopMetrics{}, discard)}
}

func (e *env) cmd(t *testing.T, kind, ext, amount, ref, key string) application.ProcessCommand {
	t.Helper()
	req, err := wagering.NewRequest(wagering.RawRequest{
		ProviderID: "provider-a", ExternalTransactionID: ext, PlayerID: e.playerID.String(), WalletID: e.walletID.String(),
		RoundID: "round-1", GameID: "game-1", Kind: kind, Amount: amount, Currency: "BRL", HasMoney: true,
		ReferenceExternalTransactionID: ref,
	})
	if err != nil {
		t.Fatal(err)
	}
	return application.ProcessCommand{Request: req, IdempotencyKey: key, CorrelationID: "corr", Source: application.SourceHTTP}
}

func (e *env) balance(t *testing.T) string {
	w, err := e.wallets.GetWallet(context.Background(), e.walletID)
	if err != nil {
		t.Fatal(err)
	}
	return w.Balance().Amount()
}

func TestReplayReturnsOriginalResult(t *testing.T) {
	e := setup(t, "1000.00")
	ctx := context.Background()
	first, err := e.process.Process(ctx, e.cmd(t, "BET", "tx-1", "25.00", "", "provider-a:tx-1"))
	if err != nil || first.Replay {
		t.Fatalf("first = %+v %v", first, err)
	}
	if _, err := e.process.Process(ctx, e.cmd(t, "BET", "tx-2", "100.00", "", "provider-a:tx-2")); err != nil {
		t.Fatal(err)
	}
	again, err := e.process.Process(ctx, e.cmd(t, "BET", "tx-1", "25.00", "", "provider-a:tx-1"))
	if err != nil || !again.Replay || again.Transaction.ID() != first.Transaction.ID() {
		t.Fatalf("replay = %+v %v", again, err)
	}
	res, _ := again.Transaction.Result()
	if res.Balance.Amount() != "975.00" {
		t.Fatalf("replay balance = %s, want the original 975.00", res.Balance)
	}
	if e.balance(t) != "875.00" || len(e.store.Ledger) != 3 {
		t.Fatalf("balance %s ledger %d", e.balance(t), len(e.store.Ledger))
	}
}

func TestSameKeyDifferentPayloadConflicts(t *testing.T) {
	e := setup(t, "1000.00")
	ctx := context.Background()
	if _, err := e.process.Process(ctx, e.cmd(t, "BET", "tx-1", "25.00", "", "k1")); err != nil {
		t.Fatal(err)
	}
	_, err := e.process.Process(ctx, e.cmd(t, "BET", "tx-1", "26.00", "", "k1"))
	if !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("different payload = %v", err)
	}
	_, err = e.process.Process(ctx, e.cmd(t, "BET", "tx-9", "25.00", "", "k1"))
	if !errors.Is(err, application.ErrIdempotencyConflict) {
		t.Fatalf("same key other external id = %v", err)
	}
	_, err = e.process.Process(ctx, e.cmd(t, "BET", "tx-1", "25.00", "", "k2"))
	if !errors.Is(err, application.ErrExternalIDReused) {
		t.Fatalf("same external id other key = %v", err)
	}
	if e.balance(t) != "975.00" || len(e.store.Ledger) != 2 {
		t.Fatalf("conflicts moved money: %s ledger=%d", e.balance(t), len(e.store.Ledger))
	}
}

func TestInboxDeduplicatesAndChecksHash(t *testing.T) {
	e := setup(t, "100.00")
	ctx := context.Background()
	c := e.cmd(t, "BET", "tx-1", "10.00", "", "k1")
	c.Source = application.SourceSQS
	c.Inbox = &application.InboxMessage{Consumer: "c", MessageID: "msg-1", PayloadHash: "h1", ReceivedAt: time.Now()}
	first, err := e.process.Process(ctx, c)
	if err != nil || first.InboxDuplicate {
		t.Fatal(first, err)
	}
	dup, err := e.process.Process(ctx, c)
	if err != nil || !dup.InboxDuplicate || dup.Transaction.ID() != first.Transaction.ID() {
		t.Fatalf("dup = %+v %v", dup, err)
	}
	c.Inbox.PayloadHash = "h2"
	if _, err := e.process.Process(ctx, c); !errors.Is(err, application.ErrMessageConflict) {
		t.Fatalf("hash change = %v", err)
	}
	c.Inbox = &application.InboxMessage{Consumer: "c", MessageID: "msg-2", PayloadHash: "h1", ReceivedAt: time.Now()}
	other, err := e.process.Process(ctx, c)
	if err != nil || !other.Replay || other.InboxDuplicate {
		t.Fatalf("new message id = %+v %v", other, err)
	}
	if e.balance(t) != "90.00" {
		t.Fatalf("balance = %s", e.balance(t))
	}
}

func TestUnknownWalletIsNotPersisted(t *testing.T) {
	e := setup(t, "100.00")
	c := e.cmd(t, "BET", "tx-1", "10.00", "", "k1")
	req, _ := wagering.NewRequest(wagering.RawRequest{
		ProviderID: "provider-a", ExternalTransactionID: "tx-1", PlayerID: e.playerID.String(), WalletID: uuid.NewString(),
		RoundID: "r", GameID: "g", Kind: "BET", Amount: "10.00", Currency: "BRL", HasMoney: true,
	})
	c.Request = req
	if _, err := e.process.Process(context.Background(), c); !errors.Is(err, application.ErrWalletNotFound) {
		t.Fatalf("unknown wallet = %v", err)
	}
	if len(e.store.Txs) != 1 { // only the OPENING
		t.Fatalf("transactions = %d", len(e.store.Txs))
	}
}

func TestPendingReferenceIsResolvedByWorker(t *testing.T) {
	e := setup(t, "100.00")
	ctx := context.Background()
	refund, err := e.process.Process(ctx, e.cmd(t, "REFUND", "r1", "25.00", "b1", "kr"))
	if err != nil || refund.Transaction.Status() != wagering.StatusPendingReference {
		t.Fatalf("refund = %v %v", refund.Transaction.Status(), err)
	}
	if _, err := e.process.Process(ctx, e.cmd(t, "BET", "b1", "25.00", "", "kb")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	refs, err := e.pending.Due(ctx, 10)
	if err != nil || len(refs) != 1 {
		t.Fatalf("due = %v %v", refs, err)
	}
	out, err := e.pending.Resolve(ctx, refs[0])
	if err != nil || out != application.ResolveFinished {
		t.Fatalf("resolve = %v %v", out, err)
	}
	if e.balance(t) != "100.00" {
		t.Fatalf("balance = %s", e.balance(t))
	}
	second, err := e.process.Process(ctx, e.cmd(t, "REFUND", "r2", "25.00", "b1", "kr2"))
	if err != nil || second.Transaction.FailureCode() != wagering.CodeReferenceAlreadyReversed {
		t.Fatalf("second refund = %v %v", second.Transaction.FailureCode(), err)
	}
	rb, err := e.process.Process(ctx, e.cmd(t, "ROLLBACK", "rb", "25.00", "b1", "krb"))
	if err != nil || rb.Transaction.FailureCode() != wagering.CodeReferenceAlreadyReversed {
		t.Fatalf("rollback after refund = %v %v", rb.Transaction.FailureCode(), err)
	}
}

func TestReconcile(t *testing.T) {
	e := setup(t, "1000.00")
	ctx := context.Background()
	if _, err := e.process.Process(ctx, e.cmd(t, "BET", "tx-1", "25.00", "", "k1")); err != nil {
		t.Fatal(err)
	}
	rec, err := e.wallets.Reconcile(ctx, e.walletID)
	if err != nil || !rec.Consistent || rec.CheckedEntries != 2 || rec.CalculatedBalance.Amount() != "975.00" || !rec.Difference.IsZero() {
		t.Fatalf("reconcile = %+v %v", rec, err)
	}
}

func TestOpenWalletConflict(t *testing.T) {
	e := setup(t, "0.00")
	m, _ := money.Parse("1.00", "BRL")
	if _, err := e.wallets.OpenWallet(context.Background(), e.playerID, m, "c"); !errors.Is(err, application.ErrWalletAlreadyExists) {
		t.Fatalf("duplicate wallet = %v", err)
	}
	if len(e.store.Txs) != 0 || len(e.store.Ledger) != 0 || len(e.store.Outbox) != 0 {
		t.Fatal("zero opening must not create OPENING, ledger or events")
	}
}
