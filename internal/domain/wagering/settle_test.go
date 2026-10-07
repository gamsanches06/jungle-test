package wagering_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/events"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

var t0 = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)

type ids struct{}

func (ids) NewID() uuid.UUID { return uuid.Must(uuid.NewV7()) }

var policy = wagering.PendingPolicy{BaseDelay: time.Second, MaxDelay: 8 * time.Second, MaxAttempts: 4, TTL: time.Minute}

type fixture struct {
	t      *testing.T
	wallet *wallet.Wallet
	player uuid.UUID
}

func newFixture(t *testing.T, balance string) *fixture {
	t.Helper()
	player := uuid.MustParse(playerID)
	res, err := wagering.OpenWallet(wagering.OpenWalletInput{
		WalletID: uuid.MustParse(walletID), PlayerID: player, InitialBalance: brl(t, balance),
		IDs: ids{}, CorrelationID: "corr", Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, wallet: res.Wallet, player: player}
}

func brl(t *testing.T, s string) money.Money {
	t.Helper()
	m, err := money.Parse(s, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (f *fixture) tx(kind, ext, amount, ref string, mutate ...func(*wagering.RawRequest)) *wagering.Transaction {
	f.t.Helper()
	r := raw(kind, amount)
	r.ExternalTransactionID = ext
	r.ReferenceExternalTransactionID = ref
	for _, m := range mutate {
		m(&r)
	}
	req, err := wagering.NewRequest(r)
	if err != nil {
		f.t.Fatal(err)
	}
	tx, err := wagering.NewExternal(uuid.Must(uuid.NewV7()), req, "key-"+ext, "corr", t0)
	if err != nil {
		f.t.Fatal(err)
	}
	return tx
}

func (f *fixture) settle(tx *wagering.Transaction, lookup wagering.ReferenceLookup, now time.Time) wagering.Outcome {
	f.t.Helper()
	out, err := wagering.Settle(wagering.SettleInput{Tx: tx, Wallet: f.wallet, Reference: lookup, Policy: policy, IDs: ids{}, Now: now})
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

func found(tx *wagering.Transaction) wagering.ReferenceLookup {
	return wagering.ReferenceLookup{Found: true, Tx: tx}
}

func eventTypes(out wagering.Outcome) []string {
	var s []string
	for _, e := range out.Events {
		s = append(s, e.EventType())
	}
	return s
}

func assertEvents(t *testing.T, out wagering.Outcome, want ...string) {
	t.Helper()
	got := eventTypes(out)
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

func TestBetDebitsAndEmitsEvents(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx("BET", "b1", "25.00", "")
	out := f.settle(bet, wagering.ReferenceLookup{}, t0)
	if bet.Status() != wagering.StatusProcessed || out.Entry == nil || out.Entry.Direction() != wallet.Debit {
		t.Fatalf("bet = %s entry=%v", bet.Status(), out.Entry)
	}
	res, _ := bet.Result()
	if res.Balance.Amount() != "75.00" || res.WalletVersion != 2 || f.wallet.Version() != 2 {
		t.Fatalf("result %s v%d", res.Balance, res.WalletVersion)
	}
	assertEvents(t, out, events.TypeWagerTransactionProcessed, events.TypeWalletBalanceChanged)
	b, _ := out.Events[1].Marshal()
	var env map[string]any
	_ = json.Unmarshal(b, &env)
	data := env["data"].(map[string]any)
	for _, k := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[k]; !ok {
			t.Errorf("WalletBalanceChanged missing %s: %s", k, b)
		}
	}
}

func TestBetWithoutFundsIsRejected(t *testing.T) {
	f := newFixture(t, "10.00")
	bet := f.tx("BET", "b1", "25.00", "")
	out := f.settle(bet, wagering.ReferenceLookup{}, t0)
	if bet.Status() != wagering.StatusRejected || bet.FailureCode() != wagering.CodeInsufficientFunds || out.Entry != nil {
		t.Fatalf("bet = %s %s", bet.Status(), bet.FailureCode())
	}
	if f.wallet.Version() != 1 || f.wallet.Balance().Amount() != "10.00" {
		t.Fatal("rejection changed the wallet")
	}
	assertEvents(t, out, events.TypeWagerTransactionRejected)
}

func TestWinCreditsAndOptionalReference(t *testing.T) {
	f := newFixture(t, "100.00")
	win := f.tx("WIN", "w1", "50.00", "")
	out := f.settle(win, wagering.ReferenceLookup{}, t0)
	if win.Status() != wagering.StatusProcessed || out.Entry.Direction() != wallet.Credit || f.wallet.Balance().Amount() != "150.00" {
		t.Fatalf("win = %s %s", win.Status(), f.wallet.Balance())
	}
	bet := f.tx("BET", "b1", "10.00", "")
	f.settle(bet, wagering.ReferenceLookup{}, t0)
	win2 := f.tx("WIN", "w2", "30.00", "b1")
	f.settle(win2, found(bet), t0)
	if win2.Status() != wagering.StatusProcessed || win2.ReferenceTxID() != bet.ID() {
		t.Fatalf("win with reference = %s ref=%s", win2.Status(), win2.ReferenceTxID())
	}
	otherRound := f.tx("WIN", "w3", "30.00", "b1", func(r *wagering.RawRequest) { r.RoundID = "round-x" })
	f.settle(otherRound, found(bet), t0)
	if otherRound.FailureCode() != wagering.CodeReferenceMismatch {
		t.Fatalf("win other round = %s", otherRound.FailureCode())
	}
}

func TestLossHasNoMovement(t *testing.T) {
	f := newFixture(t, "100.00")
	loss := f.tx("LOSS", "l1", "0.00", "")
	out := f.settle(loss, wagering.ReferenceLookup{}, t0)
	if loss.Status() != wagering.StatusProcessed || out.Entry != nil || f.wallet.Version() != 1 {
		t.Fatalf("loss = %s entry=%v v%d", loss.Status(), out.Entry, f.wallet.Version())
	}
	res, _ := loss.Result()
	if res.Balance.Amount() != "100.00" || res.WalletVersion != 1 {
		t.Fatalf("loss result %v", res)
	}
	assertEvents(t, out, events.TypeWagerTransactionProcessed)
}

func TestRefundReturnsTheBet(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx("BET", "b1", "25.00", "")
	f.settle(bet, wagering.ReferenceLookup{}, t0)
	refund := f.tx("REFUND", "r1", "25.00", "b1")
	out := f.settle(refund, found(bet), t0)
	if refund.Status() != wagering.StatusProcessed || out.Entry.Direction() != wallet.Credit || f.wallet.Balance().Amount() != "100.00" {
		t.Fatalf("refund = %s %s", refund.Status(), f.wallet.Balance())
	}
	if refund.ReferenceTxID() != bet.ID() {
		t.Fatal("reference not resolved")
	}
}

func TestReversalRules(t *testing.T) {
	cases := []struct {
		name   string
		build  func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup)
		status wagering.Status
		code   wagering.FailureCode
		after  string
	}{
		{"rollback of bet credits", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			bet := f.tx("BET", "b1", "25.00", "")
			f.settle(bet, wagering.ReferenceLookup{}, t0)
			return f.tx("ROLLBACK", "rb", "25.00", "b1"), found(bet)
		}, wagering.StatusProcessed, "", "100.00"},
		{"rollback of win debits", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			win := f.tx("WIN", "w1", "40.00", "")
			f.settle(win, wagering.ReferenceLookup{}, t0)
			return f.tx("ROLLBACK", "rb", "40.00", "w1"), found(win)
		}, wagering.StatusProcessed, "", "100.00"},
		{"rollback of refund debits", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			bet := f.tx("BET", "b1", "25.00", "")
			f.settle(bet, wagering.ReferenceLookup{}, t0)
			ref := f.tx("REFUND", "r1", "25.00", "b1")
			f.settle(ref, found(bet), t0)
			return f.tx("ROLLBACK", "rb", "25.00", "r1"), found(ref)
		}, wagering.StatusProcessed, "", "75.00"},
		{"rollback of win without funds", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			win := f.tx("WIN", "w1", "40.00", "")
			f.settle(win, wagering.ReferenceLookup{}, t0)
			drain := f.tx("BET", "b9", "130.00", "")
			f.settle(drain, wagering.ReferenceLookup{}, t0)
			return f.tx("ROLLBACK", "rb", "40.00", "w1"), found(win)
		}, wagering.StatusRejected, wagering.CodeReversalInsufficientFunds, "10.00"},
		{"refund of non-bet", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			win := f.tx("WIN", "w1", "40.00", "")
			f.settle(win, wagering.ReferenceLookup{}, t0)
			return f.tx("REFUND", "r1", "40.00", "w1"), found(win)
		}, wagering.StatusRejected, wagering.CodeInvalidReferenceKind, "140.00"},
		{"rollback of rollback", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			bet := f.tx("BET", "b1", "25.00", "")
			f.settle(bet, wagering.ReferenceLookup{}, t0)
			rb := f.tx("ROLLBACK", "rb1", "25.00", "b1")
			f.settle(rb, found(bet), t0)
			return f.tx("ROLLBACK", "rb2", "25.00", "rb1"), found(rb)
		}, wagering.StatusRejected, wagering.CodeInvalidReferenceKind, "100.00"},
		{"partial amount", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			bet := f.tx("BET", "b1", "25.00", "")
			f.settle(bet, wagering.ReferenceLookup{}, t0)
			return f.tx("REFUND", "r1", "10.00", "b1"), found(bet)
		}, wagering.StatusRejected, wagering.CodeReversalAmountMismatch, "75.00"},
		{"already reversed", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			bet := f.tx("BET", "b1", "25.00", "")
			f.settle(bet, wagering.ReferenceLookup{}, t0)
			return f.tx("ROLLBACK", "rb", "25.00", "b1"), wagering.ReferenceLookup{Found: true, Tx: bet, AlreadyReversed: true}
		}, wagering.StatusRejected, wagering.CodeReferenceAlreadyReversed, "75.00"},
		{"reference rejected", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			bet := f.tx("BET", "b1", "500.00", "")
			f.settle(bet, wagering.ReferenceLookup{}, t0)
			return f.tx("REFUND", "r1", "500.00", "b1"), found(bet)
		}, wagering.StatusRejected, wagering.CodeReferenceNotProcessed, "100.00"},
		{"reference of another wallet", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			other := f.tx("BET", "b1", "25.00", "", func(r *wagering.RawRequest) { r.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef99" })
			return f.tx("REFUND", "r1", "25.00", "b1"), found(other)
		}, wagering.StatusRejected, wagering.CodeReferenceMismatch, "100.00"},
		{"reference of another player", func(f *fixture) (*wagering.Transaction, wagering.ReferenceLookup) {
			other := f.tx("BET", "b1", "25.00", "", func(r *wagering.RawRequest) { r.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f499" })
			return f.tx("REFUND", "r1", "25.00", "b1"), found(other)
		}, wagering.StatusRejected, wagering.CodeReferenceMismatch, "100.00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFixture(t, "100.00")
			tx, lookup := c.build(f)
			out := f.settle(tx, lookup, t0)
			if tx.Status() != c.status || tx.FailureCode() != c.code {
				t.Fatalf("status = %s %s (%s), want %s %s", tx.Status(), tx.FailureCode(), tx.FailureDetail(), c.status, c.code)
			}
			if f.wallet.Balance().Amount() != c.after {
				t.Fatalf("balance = %s, want %s", f.wallet.Balance(), c.after)
			}
			if c.status == wagering.StatusRejected && out.Entry != nil {
				t.Fatal("rejection produced a ledger entry")
			}
			if lookup.Found && tx.ReferenceTxID() != lookup.Tx.ID() {
				t.Fatal("resolved reference not recorded")
			}
		})
	}
	if wagering.CodeReversalInsufficientFunds == wagering.CodeInsufficientFunds {
		t.Fatal("reversal and bet insufficient funds codes must differ")
	}
}

func TestWalletMismatches(t *testing.T) {
	f := newFixture(t, "100.00")
	wrongPlayer := f.tx("BET", "b1", "25.00", "", func(r *wagering.RawRequest) { r.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f499" })
	f.settle(wrongPlayer, wagering.ReferenceLookup{}, t0)
	if wrongPlayer.FailureCode() != wagering.CodeWalletPlayerMismatch {
		t.Fatalf("player mismatch = %s", wrongPlayer.FailureCode())
	}
	wrongCurrency := f.tx("BET", "b2", "25.00", "", func(r *wagering.RawRequest) { r.Currency = "USD" })
	f.settle(wrongCurrency, wagering.ReferenceLookup{}, t0)
	if wrongCurrency.FailureCode() != wagering.CodeCurrencyMismatch {
		t.Fatalf("currency mismatch = %s", wrongCurrency.FailureCode())
	}
	if f.wallet.Version() != 1 {
		t.Fatal("mismatch changed the wallet")
	}
}

func TestPendingReferenceLifecycle(t *testing.T) {
	f := newFixture(t, "100.00")
	refund := f.tx("REFUND", "r1", "25.00", "b1")
	out := f.settle(refund, wagering.ReferenceLookup{}, t0)
	if refund.Status() != wagering.StatusPendingReference || refund.Attempts() != 1 {
		t.Fatalf("refund = %s attempts=%d", refund.Status(), refund.Attempts())
	}
	if !refund.NextAttemptAt().Equal(t0.Add(time.Second)) || !refund.ExpiresAt().Equal(t0.Add(time.Minute)) {
		t.Fatalf("schedule next=%s expires=%s", refund.NextAttemptAt(), refund.ExpiresAt())
	}
	assertEvents(t, out, events.TypeWagerTransactionPendingReference)

	out = f.settle(refund, wagering.ReferenceLookup{}, t0.Add(time.Second))
	if refund.Attempts() != 2 || !refund.NextAttemptAt().Equal(t0.Add(3*time.Second)) || len(out.Events) != 0 {
		t.Fatalf("retry attempts=%d next=%s events=%v", refund.Attempts(), refund.NextAttemptAt(), eventTypes(out))
	}
	bet := f.tx("BET", "b1", "25.00", "")
	f.settle(bet, wagering.ReferenceLookup{}, t0.Add(2*time.Second))
	out = f.settle(refund, found(bet), t0.Add(3*time.Second))
	if refund.Status() != wagering.StatusProcessed || f.wallet.Balance().Amount() != "100.00" {
		t.Fatalf("resolved refund = %s %s", refund.Status(), f.wallet.Balance())
	}
	assertEvents(t, out, events.TypeWagerTransactionProcessed, events.TypeWalletBalanceChanged)
}

func TestPendingReferenceExpires(t *testing.T) {
	f := newFixture(t, "100.00")
	rb := f.tx("ROLLBACK", "rb", "25.00", "missing")
	f.settle(rb, wagering.ReferenceLookup{}, t0)
	now := t0
	for rb.Status() == wagering.StatusPendingReference {
		now = rb.NextAttemptAt()
		f.settle(rb, wagering.ReferenceLookup{}, now)
	}
	if rb.Status() != wagering.StatusRejected || rb.FailureCode() != wagering.CodeReferenceNotFound {
		t.Fatalf("expired = %s %s", rb.Status(), rb.FailureCode())
	}
	if rb.Attempts() != policy.MaxAttempts {
		t.Fatalf("attempts = %d, want %d", rb.Attempts(), policy.MaxAttempts)
	}
}

func TestPendingReferenceWaitsForPendingReference(t *testing.T) {
	f := newFixture(t, "100.00")
	refund := f.tx("REFUND", "r1", "25.00", "b1")
	f.settle(refund, wagering.ReferenceLookup{}, t0)
	rb := f.tx("ROLLBACK", "rb", "25.00", "r1")
	f.settle(rb, found(refund), t0)
	if rb.Status() != wagering.StatusPendingReference {
		t.Fatalf("rollback of pending refund = %s", rb.Status())
	}
	f.settle(rb, found(refund), t0.Add(2*time.Minute))
	if rb.FailureCode() != wagering.CodeReferenceNotProcessed {
		t.Fatalf("code = %s", rb.FailureCode())
	}
}

func TestStateMachine(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx("BET", "b1", "25.00", "")
	if bet.Status() != wagering.StatusPending {
		t.Fatalf("initial = %s", bet.Status())
	}
	f.settle(bet, wagering.ReferenceLookup{}, t0)
	res := wagering.Result{Balance: f.wallet.Balance(), WalletVersion: f.wallet.Version()}
	if err := bet.MarkProcessed(res, uuid.Nil, t0); !errors.Is(err, wagering.ErrInvalidTransition) {
		t.Errorf("processed -> processed: %v", err)
	}
	if err := bet.Reject(wagering.CodeInsufficientFunds, "", nil, t0); !errors.Is(err, wagering.ErrInvalidTransition) {
		t.Errorf("processed -> rejected: %v", err)
	}
	if err := bet.Fail(wagering.CodeProcessingFailed, "", t0); !errors.Is(err, wagering.ErrInvalidTransition) {
		t.Errorf("processed -> failed: %v", err)
	}
	if _, err := wagering.Settle(wagering.SettleInput{Tx: bet, Wallet: f.wallet, Policy: policy, IDs: ids{}, Now: t0}); !errors.Is(err, wagering.ErrInvalidTransition) {
		t.Errorf("settle terminal: %v", err)
	}
	other := f.tx("BET", "b2", "25.00", "")
	if err := other.MarkPendingReference(t0, t0, t0); !errors.Is(err, wagering.ErrInvalidTransition) {
		t.Errorf("bet without reference -> pending reference: %v", err)
	}
	if err := other.Reject("", "", nil, t0); !errors.Is(err, wagering.ErrInvalidTransition) {
		t.Errorf("reject without code: %v", err)
	}
	if err := other.Fail(wagering.CodeProcessingFailed, "db gone", t0); err != nil || other.Status() != wagering.StatusFailed {
		t.Errorf("pending -> failed: %v", err)
	}
}

func TestRehydrateDoesNotTransitionOrEmit(t *testing.T) {
	f := newFixture(t, "100.00")
	bet := f.tx("BET", "b1", "25.00", "")
	f.settle(bet, wagering.ReferenceLookup{}, t0)
	ext, _ := bet.External()
	res, _ := bet.Result()
	back, err := wagering.Rehydrate(wagering.Snapshot{
		ID: bet.ID(), Origin: bet.Origin(), Kind: bet.Kind(), Status: bet.Status(), WalletID: bet.WalletID(),
		PlayerID: bet.PlayerID(), Money: bet.Money(), External: &ext, Result: &res,
		CorrelationID: "c", CreatedAt: t0, UpdatedAt: t0, CompletedAt: t0,
	})
	if err != nil || back.Status() != wagering.StatusProcessed {
		t.Fatalf("rehydrate = %v %v", back, err)
	}
	if f.wallet.Balance().Amount() != "75.00" {
		t.Fatal("rehydration moved money")
	}
	if _, err := wagering.Rehydrate(wagering.Snapshot{ID: bet.ID(), Origin: wagering.OriginExternal, Kind: wagering.KindBet,
		Status: wagering.StatusProcessed, WalletID: bet.WalletID(), PlayerID: bet.PlayerID(), Money: bet.Money(), External: &ext}); err == nil {
		t.Fatal("processed snapshot without result accepted")
	}
	if _, err := wagering.Rehydrate(wagering.Snapshot{ID: bet.ID(), Origin: wagering.OriginInternal, Kind: wagering.KindBet,
		Status: wagering.StatusPending, WalletID: bet.WalletID(), PlayerID: bet.PlayerID(), Money: bet.Money()}); err == nil {
		t.Fatal("internal non-opening accepted")
	}
}

func TestOpenWalletInternalOpening(t *testing.T) {
	player := uuid.New()
	res, err := wagering.OpenWallet(wagering.OpenWalletInput{
		WalletID: uuid.New(), PlayerID: player, InitialBalance: brl(t, "1000.00"), IDs: ids{}, CorrelationID: "c", Now: t0,
	})
	if err != nil {
		t.Fatal(err)
	}
	op := res.Opening
	if op.Kind() != wagering.KindOpening || op.Origin() != wagering.OriginInternal || op.Status() != wagering.StatusProcessed {
		t.Fatalf("opening = %s %s %s", op.Kind(), op.Origin(), op.Status())
	}
	if _, ok := op.External(); ok || op.ProviderID() != "" || op.ExternalTransactionID() != "" || op.PayloadHash() != "" {
		t.Fatal("opening carries external metadata")
	}
	if res.Wallet.Version() != 1 || res.Entry.TransactionID() != op.ID() || res.Entry.WalletVersion() != 1 {
		t.Fatal("opening entry/version mismatch")
	}
	assertEvents(t, wagering.Outcome{Events: res.Events}, events.TypeWagerTransactionProcessed, events.TypeWalletBalanceChanged)
	b, _ := res.Events[0].Marshal()
	var env struct {
		Data map[string]any `json:"data"`
	}
	_ = json.Unmarshal(b, &env)
	if env.Data["origin"] != "INTERNAL" || env.Data["kind"] != "OPENING" {
		t.Fatalf("opening event = %s", b)
	}
	for _, k := range []string{"providerId", "externalTransactionId", "roundId", "gameId"} {
		if _, ok := env.Data[k]; ok {
			t.Errorf("opening event has %s", k)
		}
	}

	zero, err := wagering.OpenWallet(wagering.OpenWalletInput{
		WalletID: uuid.New(), PlayerID: player, InitialBalance: brl(t, "0.00"), IDs: ids{}, CorrelationID: "c", Now: t0,
	})
	if err != nil || zero.Opening != nil || zero.Entry != nil || len(zero.Events) != 0 || zero.Wallet.Version() != 1 {
		t.Fatalf("zero opening = %+v %v", zero, err)
	}
	if _, err := wagering.NewOpening(uuid.New(), uuid.New(), player, brl(t, "0.00"), "c", t0); err == nil {
		t.Fatal("zero OPENING transaction accepted")
	}
}

func TestPendingPolicyDelay(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}
	for i, w := range want {
		if got := policy.Delay(i + 1); got != w {
			t.Errorf("Delay(%d) = %s, want %s", i+1, got, w)
		}
	}
	if (wagering.PendingPolicy{}).Validate() == nil {
		t.Error("empty policy accepted")
	}
}

func TestFailPendingRecordsPermanentFailure(t *testing.T) {
	f := newFixture(t, "100.00")
	refund := f.tx("REFUND", "r1", "25.00", "b1")
	f.settle(refund, wagering.ReferenceLookup{}, t0)
	out, err := wagering.FailPending(refund, "broken reference data", ids{}, "cause", t0.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if refund.Status() != wagering.StatusFailed || refund.FailureCode() != wagering.CodeProcessingFailed {
		t.Fatalf("failed = %s %s", refund.Status(), refund.FailureCode())
	}
	assertEvents(t, out, events.TypeWagerTransactionFailed)
	if _, err := wagering.FailPending(refund, "again", ids{}, "", t0); !errors.Is(err, wagering.ErrInvalidTransition) {
		t.Fatalf("FAILED is terminal: %v", err)
	}
	if f.wallet.Version() != 1 {
		t.Fatal("failure moved money")
	}
}
