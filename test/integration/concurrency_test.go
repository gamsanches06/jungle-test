//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSameBetFiftyTimesInParallel sends one bet 50 times concurrently: one
// debit, 49 idempotent replays answered from the persisted result.
func TestSameBetFiftyTimesInParallel(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api"))
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")
	bet := Op("provider-a", "dup-bet", pid, wid, "BET", "10.00", "")
	Token(t, "provider-a")

	var wg sync.WaitGroup
	var processed, replays atomic.Int64
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := Submit(t, a.BaseURL, "provider-a", bet)
			if r.Code != 200 || r.Str("balance", "amount") != "90.00" {
				t.Errorf("response %d %s", r.Code, r.Raw)
				return
			}
			processed.Add(1)
			if r.Body["idempotentReplay"] == true {
				replays.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if processed.Load() != 50 || replays.Load() != 49 {
		t.Fatalf("processed=%d replays=%d, want 50/49", processed.Load(), replays.Load())
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wid); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wid); bal != 9000 {
		t.Fatalf("balance = %d", bal)
	}
	if v := Metric(t, a.BaseURL, "wagering_idempotent_replays_total"); v != 49 {
		t.Fatalf("replay metric = %v", v)
	}
	db.AssertConsistent(t, wid)
}

// raceTwoBets is the mandatory scenario: 100.00 BRL and two distinct 80.00
// bets at the same time, possibly through different instances.
func raceTwoBets(t *testing.T, db *Database, bases []string, round int) {
	t.Helper()
	wid, pid := OpenWallet(t, bases[0], "100.00")
	bets := []map[string]any{
		Op("provider-a", fmt.Sprintf("race-%d-a", round), pid, wid, "BET", "80.00", ""),
		Op("provider-a", fmt.Sprintf("race-%d-b", round), pid, wid, "BET", "80.00", ""),
	}
	results := make([]Resp, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range bets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i] = Submit(t, bases[i%len(bases)], "provider-a", bets[i])
		}()
	}
	close(start)
	wg.Wait()
	var ok, rejected int
	for _, r := range results {
		switch {
		case r.Code == 200 && r.Str("status") == "PROCESSED" && r.Str("balance", "amount") == "20.00":
			ok++
		case r.Code == 422 && r.Str("failureCode") == "INSUFFICIENT_FUNDS":
			rejected++
		default:
			t.Fatalf("unexpected result %d %s", r.Code, r.Raw)
		}
	}
	if ok != 1 || rejected != 1 {
		t.Fatalf("processed=%d rejected=%d", ok, rejected)
	}
	if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wid); bal != 2000 {
		t.Fatalf("balance = %d", bal)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wid); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	for i, b := range bets {
		r := Submit(t, bases[(i+1)%len(bases)], "provider-a", b)
		if r.Body["idempotentReplay"] != true || r.Str("status") != results[i].Str("status") {
			t.Fatalf("resend %d = %d %s", i, r.Code, r.Raw)
		}
	}
	if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wid); bal != 2000 {
		t.Fatalf("balance after resend = %d", bal)
	}
	db.AssertConsistent(t, wid)
}

func TestTwoConcurrentBetsOfEightyOnHundred(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api"))
	Token(t, "provider-a")
	for round := 0; round < 20; round++ {
		raceTwoBets(t, db, []string{a.BaseURL}, round)
	}
}

// TestIndependentWalletsProgressInParallel holds the row lock of wallet A in
// an external SQL transaction: operations on A wait, operations on B do not.
// A global lock would make B wait too.
func TestIndependentWalletsProgressInParallel(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 5*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api", "DB_LOCK_TIMEOUT", "8s"))
	wa, pa := OpenWallet(t, a.BaseURL, "100.00")
	wb, pb := OpenWallet(t, a.BaseURL, "100.00")
	Token(t, "provider-a")

	ctx := context.Background()
	lock, err := db.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, wa); err != nil {
		t.Fatal(err)
	}
	doneA := make(chan Resp, 1)
	go func() {
		doneA <- Submit(t, a.BaseURL, "provider-a", Op("provider-a", "lock-a", pa, wa, "BET", "10.00", ""))
	}()

	started := time.Now()
	rb := Submit(t, a.BaseURL, "provider-a", Op("provider-a", "free-b", pb, wb, "BET", "10.00", ""))
	if rb.Code != 200 || time.Since(started) > 2*time.Second {
		t.Fatalf("wallet B blocked: %d after %s", rb.Code, time.Since(started))
	}
	select {
	case r := <-doneA:
		t.Fatalf("wallet A did not wait for its lock: %d %s", r.Code, r.Raw)
	case <-time.After(700 * time.Millisecond):
	}
	_ = lock.Rollback(ctx)
	select {
	case r := <-doneA:
		if r.Code != 200 {
			t.Fatalf("wallet A after unlock: %d %s", r.Code, r.Raw)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("wallet A never completed")
	}

	type w struct{ id, player string }
	var wallets []w
	for i := 0; i < 10; i++ {
		id, p := OpenWallet(t, a.BaseURL, "100.00")
		wallets = append(wallets, w{id, p})
	}
	var wg sync.WaitGroup
	for i, wl := range wallets {
		for j := 0; j < 10; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := Submit(t, a.BaseURL, "provider-a", Op("provider-a", fmt.Sprintf("p-%d-%d", i, j), wl.player, wl.id, "BET", "5.00", ""))
				if r.Code != 200 {
					t.Errorf("parallel bet %d %s", r.Code, r.Raw)
				}
			}()
		}
	}
	wg.Wait()
	for _, wl := range wallets {
		if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wl.id); bal != 5000 {
			t.Fatalf("wallet %s balance = %d", wl.id, bal)
		}
		db.AssertConsistent(t, wl.id)
	}
}
