//go:build integration

package integration

import (
	"strings"
	"testing"
	"time"
)

func TestAuthenticationWithKeycloak(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q))
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")
	bet := Op("provider-a", "auth-bet", pid, wid, "BET", "10.00", "")

	expired, ttl := FreshToken(t, "provider-a-shortlived")
	if ttl > 10*time.Second {
		t.Fatalf("short-lived client issued a %s token", ttl)
	}
	valid := Token(t, "provider-a")
	tampered := valid[:len(valid)-4] + "AAAA"
	parts := strings.Split(valid, ".")
	unsigned := parts[0] + "." + parts[1] + "."

	cases := map[string]string{
		"missing":        "",
		"garbage":        "not-a-jwt",
		"bad signature":  tampered,
		"unsigned":       unsigned,
		"wrong audience": Token(t, "other-audience-client"),
	}
	for name, tok := range cases {
		r := Do(t, "POST", a.BaseURL+"/wagering/transactions", tok, bet, "Idempotency-Key", "auth-bet")
		if r.Code != 401 || r.ErrCode() != "UNAUTHENTICATED" {
			t.Errorf("%s token: %d %s", name, r.Code, r.Raw)
		}
	}
	time.Sleep(ttl + 2*time.Second)
	r := Do(t, "POST", a.BaseURL+"/wagering/transactions", expired, bet, "Idempotency-Key", "auth-bet")
	if r.Code != 401 {
		t.Errorf("expired token: %d %s", r.Code, r.Raw)
	}
	if r.Headers.Get("WWW-Authenticate") == "" {
		t.Error("401 without WWW-Authenticate")
	}

	if n := db.Int(t, `SELECT COUNT(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 0 {
		t.Fatalf("%d operations persisted by unauthenticated calls", n)
	}
	db.AssertConsistent(t, wid)
	if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wid); bal != 10000 {
		t.Fatalf("balance changed to %d", bal)
	}
}

func TestAuthorizationAndProviderIsolation(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q))
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")
	internal, provA, provB, noRole := Token(t, "wallet-service"), Token(t, "provider-a"), Token(t, "provider-b"), Token(t, "no-role-client")

	for _, tok := range []string{provA, provB, noRole} {
		for _, c := range []struct{ m, p string }{
			{"POST", "/wallets"}, {"GET", "/wallets/" + wid}, {"GET", "/wallets/" + wid + "/ledger"},
			{"POST", "/wallets/" + wid + "/reconciliation"},
		} {
			if r := Do(t, c.m, a.BaseURL+c.p, tok, map[string]any{"playerId": pid,
				"initialBalance": map[string]string{"amount": "5.00", "currency": "BRL"}}); r.Code != 403 {
				t.Errorf("%s %s: %d", c.m, c.p, r.Code)
			}
		}
	}
	for _, tok := range []string{internal, noRole} {
		r := Do(t, "POST", a.BaseURL+"/wagering/transactions", tok, Op("provider-a", "x", pid, wid, "BET", "1.00", ""), "Idempotency-Key", "x")
		if r.Code != 403 {
			t.Errorf("non-provider submit: %d", r.Code)
		}
	}

	r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "bet-a", pid, wid, "BET", "25.00", ""))
	if r.Code != 200 {
		t.Fatalf("provider-a bet: %d %s", r.Code, r.Raw)
	}
	txID := r.Str("transactionId")

	r = Do(t, "POST", a.BaseURL+"/wagering/transactions", provB, Op("provider-a", "bet-a", pid, wid, "BET", "25.00", ""), "Idempotency-Key", "provider-a:bet-a")
	if r.Code != 403 || strings.Contains(r.Raw, txID) {
		t.Fatalf("cross-provider replay: %d %s", r.Code, r.Raw)
	}
	r = Do(t, "POST", a.BaseURL+"/wagering/transactions", provB, Op("provider-b", "bet-b-own", pid, wid, "BET", "5.00", ""), "Idempotency-Key", "provider-a:bet-a")
	if r.Code != 200 || r.Body["idempotentReplay"] != false || r.Str("transactionId") == txID {
		t.Fatalf("provider-b own op with same key: %d %s", r.Code, r.Raw)
	}
	if r := Do(t, "GET", a.BaseURL+"/wagering/transactions/"+txID, provB, nil); r.Code != 404 || strings.Contains(r.Raw, pid) {
		t.Errorf("provider-b reading provider-a tx by id: %d %s", r.Code, r.Raw)
	}
	if r := Do(t, "GET", a.BaseURL+"/providers/provider-a/wagering/transactions/bet-a", provB, nil); r.Code != 403 || strings.Contains(r.Raw, pid) {
		t.Errorf("provider-b reading provider-a path: %d %s", r.Code, r.Raw)
	}
	if r := Do(t, "GET", a.BaseURL+"/providers/provider-a/wagering/transactions/bet-a", provA, nil); r.Code != 200 || r.Str("transactionId") != txID {
		t.Errorf("provider-a own read: %d %s", r.Code, r.Raw)
	}
	if r := Do(t, "GET", a.BaseURL+"/wagering/transactions/"+txID, internal, nil); r.Code != 200 {
		t.Errorf("internal read: %d", r.Code)
	}
	// A provider cannot reverse another provider's bet: references resolve
	// only inside the caller's provider, so it waits and never finds it.
	r = Submit(t, a.BaseURL, "provider-b", Op("provider-b", "steal", pid, wid, "REFUND", "25.00", "bet-a"))
	if r.Code != 202 || r.Str("status") != "PENDING_REFERENCE" {
		t.Fatalf("cross-provider refund: %d %s", r.Code, r.Raw)
	}
	if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wid); bal != 7000 {
		t.Fatalf("balance = %d, want 7000", bal)
	}
	db.AssertConsistent(t, wid)
}
