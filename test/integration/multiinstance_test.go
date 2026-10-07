//go:build integration

package integration

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

func startCluster(t *testing.T, s Settings, n int) []*Process {
	t.Helper()
	var ps []*Process
	for i := 0; i < n; i++ {
		ps = append(ps, StartProcess(t, s.With("INSTANCE_ID", fmt.Sprintf("proc-%d-%s", i, randSuffix()))))
	}
	return ps
}

func bases(ps []*Process) []string {
	var out []string
	for _, p := range ps {
		out = append(out, p.BaseURL)
	}
	return out
}

// TestThreeIndependentInstances repeats the critical scenarios with three
// OS processes, each with its own memory and connection pool.
func TestThreeIndependentInstances(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	cluster := startCluster(t, NewSettings(db, q), 3)
	urls := bases(cluster)
	Token(t, "provider-a")

	t.Run("two bets of 80 on 100 across instances", func(t *testing.T) {
		for round := 0; round < 10; round++ {
			raceTwoBets(t, db, urls, round)
		}
	})

	t.Run("same bet 50 times over HTTP on 3 instances plus SQS", func(t *testing.T) {
		wid, pid := OpenWallet(t, urls[0], "100.00")
		bet := Op("provider-a", "cluster-dup", pid, wid, "BET", "30.00", "")
		var wg sync.WaitGroup
		var replays atomic.Int64
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r := Submit(t, urls[i%3], "provider-a", bet)
				if r.Code != 200 || r.Str("balance", "amount") != "70.00" {
					t.Errorf("dup = %d %s", r.Code, r.Raw)
				}
				if r.Body["idempotentReplay"] == true {
					replays.Add(1)
				}
			}()
		}
		for i := 0; i < 10; i++ {
			q.Send(t, fmt.Sprintf("cluster-dup-%d", i), SQSData(bet, "provider-a:cluster-dup"))
		}
		wg.Wait()
		Eventually(t, 20*time.Second, "sqs duplicates consumed", func() bool {
			return db.Int(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id LIKE 'cluster-dup-%' AND completed_at IS NOT NULL`) == 10
		})
		if replays.Load() != 49 && replays.Load() != 50 {
			t.Fatalf("http replays = %d", replays.Load())
		}
		if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wid); n != 1 {
			t.Fatalf("debits = %d", n)
		}
		db.AssertConsistent(t, wid)
	})

	t.Run("distinct wallets in parallel across instances", func(t *testing.T) {
		var wg sync.WaitGroup
		type w struct{ id, player string }
		var ws []w
		for i := 0; i < 9; i++ {
			id, p := OpenWallet(t, urls[i%3], "50.00")
			ws = append(ws, w{id, p})
		}
		for i, wl := range ws {
			for j := 0; j < 5; j++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					body := Op("provider-a", fmt.Sprintf("dw-%d-%d", i, j), wl.player, wl.id, "BET", "10.00", "")
					if j%2 == 0 {
						Submit(t, urls[(i+j)%3], "provider-a", body)
					} else {
						q.Send(t, fmt.Sprintf("dw-%d-%d", i, j), SQSData(body, "provider-a:"+body["externalTransactionId"].(string)))
					}
				}()
			}
		}
		wg.Wait()
		Eventually(t, 30*time.Second, "all wallets drained", func() bool {
			return db.Int(t, `SELECT COUNT(*) FROM wallets WHERE id = ANY($1) AND balance_minor = 0`, ids(ws, func(x w) string { return x.id })) == 9
		})
		for _, wl := range ws {
			db.AssertConsistent(t, wl.id)
		}
	})

	Eventually(t, 20*time.Second, "outbox drained by the cluster", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
}

func ids[T any](xs []T, f func(T) string) []string {
	var out []string
	for _, x := range xs {
		out = append(out, f(x))
	}
	return out
}

// TestConsumerCrashAfterCommitBeforeDelete kills the consumer right after the
// SQL commit; the message is redelivered and deduplicated by the inbox.
func TestConsumerCrashAfterCommitBeforeDelete(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 5)
	s := NewSettings(db, q)
	api := StartApp(t, s.With("APP_ROLES", "api"))
	wid, pid := OpenWallet(t, api.BaseURL, "100.00")

	a := StartProcess(t, s.With("APP_ROLES", "consumer", "FAILPOINTS", failpoint.ConsumerAfterCommit+"=exit"))
	q.Send(t, "crash-msg", SQSData(Op("provider-a", "crash-bet", pid, wid, "BET", "15.00", ""), "provider-a:crash-bet"))
	if !a.WaitExit(20 * time.Second) {
		t.Fatal("consumer did not crash after commit")
	}
	if statusOf(t, db, "crash-bet") != "PROCESSED" {
		t.Fatal("operation was not committed before the crash")
	}
	if q.Approx(t, q.InputURL) != 1 {
		t.Fatal("message should still be in the queue")
	}
	b := StartProcess(t, s.With("APP_ROLES", "api,consumer"))
	Eventually(t, 20*time.Second, "redelivery deduplicated and deleted", func() bool {
		return q.Approx(t, q.InputURL) == 0 && db.Int(t, `SELECT deliveries FROM inbox_messages WHERE message_id = 'crash-msg'`) == 2
	})
	if v := Metric(t, b.BaseURL, "wagering_inbox_duplicates_total"); v != 1 {
		t.Fatalf("inbox duplicate metric on B = %v", v)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wid); n != 1 {
		t.Fatalf("debits = %d", n)
	}
	db.AssertConsistent(t, wid)
}

// TestCrashAroundCommit kills an instance before and after the commit of an
// HTTP operation; the client retries on another instance.
func TestCrashAroundCommit(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	s := NewSettings(db, q).With("APP_ROLES", "api")
	healthy := StartProcess(t, s)
	wid, pid := OpenWallet(t, healthy.BaseURL, "100.00")
	tok := Token(t, "provider-a")

	before := StartProcess(t, s.With("FAILPOINTS", failpoint.ProcessBeforeCommit+"=exit"))
	bet := Op("provider-a", "pre-commit", pid, wid, "BET", "10.00", "")
	if _, err := DoErr("POST", before.BaseURL+"/wagering/transactions", tok, bet, "Idempotency-Key", "provider-a:pre-commit"); err == nil {
		t.Fatal("expected the connection to drop")
	}
	before.WaitExit(5 * time.Second)
	if statusOf(t, db, "pre-commit") != "" {
		t.Fatal("uncommitted operation became visible")
	}
	if r := Submit(t, healthy.BaseURL, "provider-a", bet); r.Code != 200 || r.Body["idempotentReplay"] != false {
		t.Fatalf("retry after pre-commit crash = %d %s", r.Code, r.Raw)
	}

	after := StartProcess(t, s.With("FAILPOINTS", failpoint.ProcessAfterCommit+"=exit"))
	bet2 := Op("provider-a", "post-commit", pid, wid, "BET", "10.00", "")
	if _, err := DoErr("POST", after.BaseURL+"/wagering/transactions", tok, bet2, "Idempotency-Key", "provider-a:post-commit"); err == nil {
		t.Fatal("expected the connection to drop")
	}
	after.WaitExit(5 * time.Second)
	if statusOf(t, db, "post-commit") != "PROCESSED" {
		t.Fatal("committed operation lost")
	}
	r := Submit(t, healthy.BaseURL, "provider-a", bet2)
	if r.Code != 200 || r.Body["idempotentReplay"] != true || r.Str("balance", "amount") != "80.00" {
		t.Fatalf("retry after post-commit crash = %d %s", r.Code, r.Raw)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, wid); n != 2 {
		t.Fatalf("debits = %d", n)
	}
	db.AssertConsistent(t, wid)
}

// TestFullRestartPreservesIdempotencyPendingAndBalances kills every process
// and starts a new generation.
func TestFullRestartPreservesIdempotencyPendingAndBalances(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	s := NewSettings(db, q).With("REFERENCE_BASE_DELAY", "1s", "REFERENCE_MAX_DELAY", "2s")
	gen1 := startCluster(t, s, 3)
	urls := bases(gen1)
	wid, pid := OpenWallet(t, urls[0], "100.00")
	bet := Op("provider-a", "r-bet", pid, wid, "BET", "30.00", "")
	if r := Submit(t, urls[1], "provider-a", bet); r.Code != 200 {
		t.Fatalf("bet = %d", r.Code)
	}
	win := Op("provider-a", "r-win", pid, wid, "WIN", "5.00", "")
	if r := Submit(t, urls[2], "provider-a", win); r.Code != 200 {
		t.Fatalf("win = %d", r.Code)
	}
	refund := Op("provider-a", "r-refund", pid, wid, "REFUND", "20.00", "r-late-bet")
	if r := Submit(t, urls[0], "provider-a", refund); r.Code != 202 {
		t.Fatalf("refund = %d", r.Code)
	}
	for _, p := range gen1 {
		p.Kill()
	}

	gen2 := startCluster(t, s, 3)
	urls = bases(gen2)
	r := Submit(t, urls[2], "provider-a", bet)
	if r.Body["idempotentReplay"] != true || r.Str("balance", "amount") != "70.00" {
		t.Fatalf("bet replay after restart = %d %s", r.Code, r.Raw)
	}
	r = Submit(t, urls[0], "provider-a", win)
	if r.Body["idempotentReplay"] != true || r.Str("balance", "amount") != "75.00" {
		t.Fatalf("win replay after restart = %d %s", r.Code, r.Raw)
	}
	if r := Submit(t, urls[1], "provider-a", refund); r.Code != 202 || r.Body["idempotentReplay"] != true {
		t.Fatalf("pending replay after restart = %d %s", r.Code, r.Raw)
	}
	if r := Submit(t, urls[1], "provider-a", Op("provider-a", "r-late-bet", pid, wid, "BET", "20.00", "")); r.Code != 200 {
		t.Fatalf("late bet = %d %s", r.Code, r.Raw)
	}
	Eventually(t, 15*time.Second, "pending resumed by the new generation", func() bool {
		return statusOf(t, db, "r-refund") == "PROCESSED"
	})
	rec := Do(t, "POST", urls[2]+"/wallets/"+wid+"/reconciliation", Token(t, "wallet-service"), nil)
	if rec.Body["consistent"] != true || rec.Str("storedBalance", "amount") != "75.00" {
		t.Fatalf("reconciliation = %s", rec.Raw)
	}
	db.AssertConsistent(t, wid)
}

// TestGracefulShutdownDoesNotLoseMessages sends SIGTERM while messages are
// being consumed; work in progress finishes or is released, and a new
// instance completes the rest exactly once.
func TestGracefulShutdownDoesNotLoseMessages(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 5)
	s := NewSettings(db, q)
	api := StartApp(t, s.With("APP_ROLES", "api"))
	type w struct{ id, player string }
	var ws []w
	for i := 0; i < 5; i++ {
		id, p := OpenWallet(t, api.BaseURL, "100.00")
		ws = append(ws, w{id, p})
	}
	a := StartProcess(t, s.With("APP_ROLES", "api,consumer,outbox,pending"))
	for i := 0; i < 40; i++ {
		wl := ws[i%5]
		q.Send(t, fmt.Sprintf("term-%d", i), SQSData(Op("provider-a", fmt.Sprintf("term-%d", i), wl.player, wl.id, "BET", "1.00", ""), fmt.Sprintf("provider-a:term-%d", i)))
	}
	time.Sleep(300 * time.Millisecond)
	start := time.Now()
	if err := a.Terminate(15 * time.Second); err != nil {
		t.Fatalf("graceful exit: %v", err)
	}
	t.Logf("instance stopped in %s", time.Since(start))
	StartProcess(t, s.With("APP_ROLES", "api,consumer"))
	Eventually(t, 40*time.Second, "all 40 messages processed", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM wager_transactions WHERE external_transaction_id LIKE 'term-%' AND status = 'PROCESSED'`) == 40
	})
	Eventually(t, 20*time.Second, "queue drained", func() bool { return q.Approx(t, q.InputURL) == 0 })
	for _, wl := range ws {
		if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wl.id); bal != 9200 {
			t.Fatalf("wallet balance = %d, want 9200", bal)
		}
		db.AssertConsistent(t, wl.id)
	}
}
