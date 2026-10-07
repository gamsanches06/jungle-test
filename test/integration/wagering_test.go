//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"
)

func TestWalletOpening(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api"))
	internal := Token(t, "wallet-service")

	wid, pid := OpenWallet(t, a.BaseURL, "1000.00")
	if n := db.Int(t, `SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1 AND kind = 'OPENING' AND status = 'PROCESSED'
		AND origin = 'INTERNAL' AND provider_id IS NULL`, wid); n != 1 {
		t.Fatalf("OPENING rows = %d", n)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'CREDIT' AND amount_minor = 100000 AND wallet_version = 1`, wid); n != 1 {
		t.Fatalf("opening credit entries = %d", n)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE group_key = $1 AND event_type IN ('WagerTransactionProcessed','WalletBalanceChanged')`, wid); n != 2 {
		t.Fatalf("opening events = %d", n)
	}
	r := Do(t, "GET", a.BaseURL+"/wallets/"+wid, internal, nil)
	if r.Code != 200 || r.Body["version"].(float64) != 1 || r.Str("balance", "amount") != "1000.00" {
		t.Fatalf("get wallet = %d %s", r.Code, r.Raw)
	}
	r = Do(t, "POST", a.BaseURL+"/wallets", internal, map[string]any{"playerId": pid,
		"initialBalance": map[string]string{"amount": "1.00", "currency": "BRL"}})
	if r.Code != 409 || r.ErrCode() != "WALLET_ALREADY_EXISTS" {
		t.Fatalf("duplicate wallet = %d %s", r.Code, r.Raw)
	}
	zw, _ := OpenWallet(t, a.BaseURL, "0.00")
	if n := db.Int(t, `SELECT (SELECT COUNT(*) FROM wager_transactions WHERE wallet_id = $1) +
		(SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1) + (SELECT COUNT(*) FROM outbox_events WHERE group_key = $1::text)`, zw); n != 0 {
		t.Fatalf("zero opening produced %d rows", n)
	}
	for _, bad := range []map[string]string{{"amount": "-1.00", "currency": "BRL"}, {"amount": "1.0", "currency": "BRL"}, {"amount": "1.00", "currency": "XYZ"}} {
		r = Do(t, "POST", a.BaseURL+"/wallets", internal, map[string]any{"playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", "initialBalance": bad})
		if r.Code != 400 {
			t.Errorf("opening with %v = %d", bad, r.Code)
		}
	}
}

func TestOperationsAndReplays(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api"))
	wid, pid := OpenWallet(t, a.BaseURL, "1000.00")
	sub := func(ext, kind, amount, ref string) Resp {
		return Submit(t, a.BaseURL, "provider-a", Op("provider-a", ext, pid, wid, kind, amount, ref))
	}
	expect := func(r Resp, code int, status, balance, failure string) {
		t.Helper()
		if r.Code != code || r.Str("status") != status || r.Str("balance", "amount") != balance || r.Str("failureCode") != failure {
			t.Fatalf("got %d %s, want %d %s %s %s", r.Code, r.Raw, code, status, balance, failure)
		}
	}
	expect(sub("bet-1", "BET", "25.00", ""), 200, "PROCESSED", "975.00", "")
	expect(sub("win-1", "WIN", "50.00", "bet-1"), 200, "PROCESSED", "1025.00", "")
	expect(sub("loss-1", "LOSS", "0.00", ""), 200, "PROCESSED", "1025.00", "")
	expect(sub("bet-2", "BET", "100.00", ""), 200, "PROCESSED", "925.00", "")
	expect(sub("refund-2", "REFUND", "100.00", "bet-2"), 200, "PROCESSED", "1025.00", "")
	expect(sub("rollback-2", "ROLLBACK", "100.00", "bet-2"), 422, "REJECTED", "1025.00", "REFERENCE_ALREADY_REVERSED")
	expect(sub("refund-2b", "REFUND", "100.00", "bet-2"), 422, "REJECTED", "1025.00", "REFERENCE_ALREADY_REVERSED")
	expect(sub("rb-refund-2", "ROLLBACK", "100.00", "refund-2"), 200, "PROCESSED", "925.00", "")
	expect(sub("rb-win-1", "ROLLBACK", "50.00", "win-1"), 200, "PROCESSED", "875.00", "")
	expect(sub("rb-win-1b", "ROLLBACK", "50.00", "win-1"), 422, "REJECTED", "875.00", "REFERENCE_ALREADY_REVERSED")
	expect(sub("bet-big", "BET", "5000.00", ""), 422, "REJECTED", "875.00", "INSUFFICIENT_FUNDS")
	expect(sub("win-2", "WIN", "10.00", ""), 200, "PROCESSED", "885.00", "")
	expect(sub("bet-drain", "BET", "880.00", ""), 200, "PROCESSED", "5.00", "")
	expect(sub("rb-win-2", "ROLLBACK", "10.00", "win-2"), 422, "REJECTED", "5.00", "REVERSAL_INSUFFICIENT_FUNDS")
	expect(sub("refund-partial", "REFUND", "1.00", "bet-1"), 422, "REJECTED", "5.00", "REVERSAL_AMOUNT_MISMATCH")
	expect(sub("refund-loss", "REFUND", "0.01", "loss-1"), 422, "REJECTED", "5.00", "INVALID_REFERENCE_KIND")

	r := sub("bet-1", "BET", "25.00", "")
	if r.Code != 200 || r.Body["idempotentReplay"] != true || r.Str("balance", "amount") != "975.00" {
		t.Fatalf("replay = %d %s", r.Code, r.Raw)
	}
	r = sub("rb-win-2", "ROLLBACK", "10.00", "win-2")
	if r.Code != 422 || r.Body["idempotentReplay"] != true || r.Str("failureCode") != "REVERSAL_INSUFFICIENT_FUNDS" {
		t.Fatalf("rejection replay = %d %s", r.Code, r.Raw)
	}
	r = Do(t, "POST", a.BaseURL+"/wagering/transactions", Token(t, "provider-a"), Op("provider-a", "bet-1", pid, wid, "BET", "26.00", ""), "Idempotency-Key", "provider-a:bet-1")
	if r.Code != 409 || r.ErrCode() != "IDEMPOTENCY_KEY_REUSED" {
		t.Fatalf("payload conflict = %d %s", r.Code, r.Raw)
	}
	r = Do(t, "POST", a.BaseURL+"/wagering/transactions", Token(t, "provider-a"), Op("provider-a", "bet-1", pid, wid, "BET", "25.00", ""), "Idempotency-Key", "other-key")
	if r.Code != 409 || r.ErrCode() != "EXTERNAL_TRANSACTION_ID_REUSED" {
		t.Fatalf("external id conflict = %d %s", r.Code, r.Raw)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id
		WHERE t.kind = 'LOSS' OR t.status = 'REJECTED'`); n != 0 {
		t.Fatalf("LOSS/REJECTED ledger entries = %d", n)
	}
	// Version 1 from the opening plus 8 balance movements.
	if v := db.Int(t, `SELECT version FROM wallets WHERE id = $1`, wid); v != 9 {
		t.Fatalf("version = %d", v)
	}
	// 7 rejections and 9 balance changes (opening included) were produced above.
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE event_type = 'WagerTransactionRejected'`); n != 7 {
		t.Fatalf("rejected events = %d", n)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE event_type = 'WalletBalanceChanged'`); n != 9 {
		t.Fatalf("balance changed events = %d", n)
	}
	var data map[string]any
	_ = json.Unmarshal([]byte(db.Str(t, `SELECT payload::text FROM outbox_events WHERE event_type='WalletBalanceChanged' ORDER BY seq DESC LIMIT 1`)), &data)
	d := data["data"].(map[string]any)
	if d["direction"] != "DEBIT" || d["balanceAfter"].(map[string]any)["amount"] != "5.00" || d["walletVersion"].(float64) != 9 {
		t.Fatalf("last WalletBalanceChanged = %v", d)
	}

	rec := Do(t, "POST", a.BaseURL+"/wallets/"+wid+"/reconciliation", Token(t, "wallet-service"), nil)
	if rec.Code != 200 || rec.Body["consistent"] != true || rec.Body["checkedEntries"].(float64) != 9 || rec.Str("storedBalance", "amount") != "5.00" {
		t.Fatalf("reconciliation = %s", rec.Raw)
	}
	db.AssertConsistent(t, wid)

	v := Do(t, "GET", a.BaseURL+"/providers/provider-a/wagering/transactions/rb-win-2", Token(t, "provider-a"), nil)
	if v.Code != 200 || v.Str("failureCode") != "REVERSAL_INSUFFICIENT_FUNDS" || v.Str("referenceTransactionId") == "" {
		t.Fatalf("query = %d %s", v.Code, v.Raw)
	}
}

func TestPendingReferenceResolution(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api,pending", "REFERENCE_TTL", "3s"))
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")

	r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "refund-1", pid, wid, "REFUND", "40.00", "bet-1"))
	if r.Code != 202 || r.Str("status") != "PENDING_REFERENCE" {
		t.Fatalf("refund = %d %s", r.Code, r.Raw)
	}
	refundID := r.Str("transactionId")
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE event_type = 'WagerTransactionPendingReference'`); n != 1 {
		t.Fatalf("pending events = %d", n)
	}
	if r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "refund-1", pid, wid, "REFUND", "40.00", "bet-1")); r.Code != 202 || r.Body["idempotentReplay"] != true {
		t.Fatalf("pending replay = %d %s", r.Code, r.Raw)
	}
	if r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "bet-1", pid, wid, "BET", "40.00", "")); r.Code != 200 || r.Str("balance", "amount") != "60.00" {
		t.Fatalf("bet = %d %s", r.Code, r.Raw)
	}
	Eventually(t, 10*time.Second, "refund resolved", func() bool {
		v := Do(t, "GET", a.BaseURL+"/wagering/transactions/"+refundID, Token(t, "provider-a"), nil)
		return v.Str("status") == "PROCESSED" && v.Str("balance", "amount") == "100.00"
	})

	r = Submit(t, a.BaseURL, "provider-a", Op("provider-a", "rb-ghost", pid, wid, "ROLLBACK", "10.00", "ghost-bet"))
	if r.Code != 202 {
		t.Fatalf("rollback = %d %s", r.Code, r.Raw)
	}
	ghost := r.Str("transactionId")
	Eventually(t, 15*time.Second, "rollback expired", func() bool {
		v := Do(t, "GET", a.BaseURL+"/wagering/transactions/"+ghost, Token(t, "provider-a"), nil)
		return v.Str("status") == "REJECTED" && v.Str("failureCode") == "REFERENCE_NOT_FOUND"
	})
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE event_type = 'WagerTransactionRejected' AND aggregate_id = $1`, ghost); n != 1 {
		t.Fatalf("expiry rejection events = %d", n)
	}
	if n := db.Int(t, `SELECT attempts FROM wager_transactions WHERE id = $1`, ghost); n < 2 {
		t.Fatalf("attempts = %d, expected retries with backoff", n)
	}
	db.AssertConsistent(t, wid)
}
