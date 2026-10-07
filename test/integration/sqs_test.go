//go:build integration

package integration

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

func statusOf(t *testing.T, db *Database, ext string) string {
	t.Helper()
	var s string
	_ = db.Owner.QueryRow(t.Context(), `SELECT status FROM wager_transactions WHERE external_transaction_id = $1`, ext).Scan(&s)
	return s
}

func TestSQSConsumerDeduplicatesAndSharesIdempotencyWithHTTP(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api,consumer,pending"))
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")

	bet := Op("provider-a", "sqs-bet-1", pid, wid, "BET", "10.00", "")
	q.Send(t, "msg-1", SQSData(bet, "provider-a:sqs-bet-1"))
	Eventually(t, 10*time.Second, "sqs bet processed", func() bool { return statusOf(t, db, "sqs-bet-1") == "PROCESSED" })

	q.Send(t, "msg-1", SQSData(bet, "provider-a:sqs-bet-1"))
	Eventually(t, 10*time.Second, "redelivery deduplicated", func() bool {
		return db.Int(t, `SELECT deliveries FROM inbox_messages WHERE message_id = 'msg-1'`) == 2
	})
	q.Send(t, "msg-2", SQSData(bet, "provider-a:sqs-bet-1"))
	Eventually(t, 10*time.Second, "second message completed", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id = 'msg-2' AND outcome = 'REPLAY:PROCESSED'`) == 1
	})
	if r := Submit(t, a.BaseURL, "provider-a", bet); r.Code != 200 || r.Body["idempotentReplay"] != true || r.Str("balance", "amount") != "90.00" {
		t.Fatalf("http replay of sqs op = %d %s", r.Code, r.Raw)
	}
	bet2 := Op("provider-a", "http-bet-2", pid, wid, "BET", "5.00", "")
	if r := Submit(t, a.BaseURL, "provider-a", bet2); r.Code != 200 {
		t.Fatalf("http bet = %d", r.Code)
	}
	q.Send(t, "msg-3", SQSData(bet2, "provider-a:http-bet-2"))
	Eventually(t, 10*time.Second, "sqs replay of http op", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id = 'msg-3' AND completed_at IS NOT NULL`) == 1
	})
	bet3 := Op("provider-a", "both-bet-3", pid, wid, "BET", "1.00", "")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); q.Send(t, "msg-4", SQSData(bet3, "provider-a:both-bet-3")) }()
	go func() { defer wg.Done(); Submit(t, a.BaseURL, "provider-a", bet3) }()
	wg.Wait()
	Eventually(t, 10*time.Second, "concurrent sqs completed", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM inbox_messages WHERE message_id = 'msg-4' AND completed_at IS NOT NULL`) == 1
	})

	for _, ext := range []string{"sqs-bet-1", "http-bet-2", "both-bet-3"} {
		if n := db.Int(t, `SELECT COUNT(*) FROM wallet_ledger_entries l JOIN wager_transactions t ON t.id = l.transaction_id WHERE t.external_transaction_id = $1`, ext); n != 1 {
			t.Fatalf("%s ledger entries = %d", ext, n)
		}
	}
	if bal := db.Int(t, `SELECT balance_minor FROM wallets WHERE id = $1`, wid); bal != 8400 {
		t.Fatalf("balance = %d", bal)
	}
	if v := a.Metrics; v == nil {
		t.Fatal("metrics not populated")
	}
	if v := Metric(t, a.BaseURL, "wagering_inbox_duplicates_total"); v != 1 {
		t.Fatalf("inbox duplicate metric = %v", v)
	}
	Eventually(t, 10*time.Second, "input queue drained", func() bool { return q.Approx(t, q.InputURL) == 0 })
	if n := len(q.Drain(t, q.DLQURL, 2*time.Second)); n != 0 {
		t.Fatalf("%d unexpected DLQ messages", n)
	}

	q.Send(t, "msg-5", SQSData(Op("provider-a", "sqs-big", pid, wid, "BET", "999.00", ""), "provider-a:sqs-big"))
	Eventually(t, 10*time.Second, "rejection persisted", func() bool { return statusOf(t, db, "sqs-big") == "REJECTED" })
	q.Send(t, "msg-6", SQSData(Op("provider-a", "sqs-refund", pid, wid, "REFUND", "7.00", "late-bet"), "provider-a:sqs-refund"))
	Eventually(t, 10*time.Second, "pending persisted", func() bool { return statusOf(t, db, "sqs-refund") == "PENDING_REFERENCE" })
	q.Send(t, "msg-7", SQSData(Op("provider-a", "late-bet", pid, wid, "BET", "7.00", ""), "provider-a:late-bet"))
	Eventually(t, 10*time.Second, "refund resolved", func() bool { return statusOf(t, db, "sqs-refund") == "PROCESSED" })
	Eventually(t, 10*time.Second, "input queue drained", func() bool { return q.Approx(t, q.InputURL) == 0 })
	db.AssertConsistent(t, wid)
}

func TestSQSInvalidMessagesGoToDLQ(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api,consumer"))
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")
	bet := Op("provider-a", "dlq-bet", pid, wid, "BET", "10.00", "")
	q.Send(t, "ok-1", SQSData(bet, "provider-a:dlq-bet"))
	Eventually(t, 10*time.Second, "bet processed", func() bool { return statusOf(t, db, "dlq-bet") == "PROCESSED" })

	q.SendRaw(t, `{not json`, "g", "d1")
	q.Send(t, "opening", SQSData(Op("provider-a", "op-1", pid, wid, "OPENING", "10.00", ""), "provider-a:op-1"))
	q.Send(t, "no-wallet", SQSData(Op("provider-a", "nw-1", pid, "0192f291-27dd-7d3f-8071-5f8685deef37", "BET", "1.00", ""), "provider-a:nw-1"))
	conflict := SQSData(bet, "provider-a:dlq-bet")
	conflict["money"] = map[string]string{"amount": "11.00", "currency": "BRL"}
	q.Send(t, "conflict", conflict)
	reused := SQSData(Op("provider-a", "other", pid, wid, "BET", "1.00", ""), "provider-a:other")
	q.Send(t, "ok-1", reused)

	var got []string
	Eventually(t, 20*time.Second, "5 messages in the DLQ", func() bool {
		for _, m := range q.Drain(t, q.DLQURL, time.Second) {
			got = append(got, aws.ToString(m.MessageAttributes["failureCode"].StringValue))
		}
		return len(got) >= 5
	})
	joined := strings.Join(got, ",")
	for _, code := range []string{"INVALID_MESSAGE", "WALLET_NOT_FOUND", "IDEMPOTENCY_KEY_REUSED", "MESSAGE_ID_REUSED"} {
		if !strings.Contains(joined, code) {
			t.Errorf("DLQ codes %s missing %s", joined, code)
		}
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 1 {
		t.Fatalf("invalid messages persisted operations: %d", n)
	}
	Eventually(t, 10*time.Second, "input drained", func() bool { return q.Approx(t, q.InputURL) == 0 })
	if v := Metric(t, a.BaseURL, "wagering_dlq_messages_total"); v < 5 {
		t.Fatalf("dlq metric = %v", v)
	}
}

func TestSQSTransientFailuresAreRetriedThenDeadLettered(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	a := StartApp(t, NewSettings(db, q).With("APP_ROLES", "api,consumer"))
	wid, pid := OpenWallet(t, a.BaseURL, "100.00")
	t.Cleanup(failpoint.Reset)

	if err := failpoint.Enable(failpoint.ConsumerProcessTransiently, "once-error"); err != nil {
		t.Fatal(err)
	}
	q.Send(t, "retry-1", SQSData(Op("provider-a", "retry-bet", pid, wid, "BET", "10.00", ""), "provider-a:retry-bet"))
	Eventually(t, 15*time.Second, "processed after retry", func() bool { return statusOf(t, db, "retry-bet") == "PROCESSED" })
	if failpoint.Hits(failpoint.ConsumerProcessTransiently) < 2 {
		t.Fatal("message was not retried")
	}

	// With a failure on every attempt, the queue's redrive policy moves the
	// message to the DLQ after SQS_MAX_RECEIVES receptions.
	if err := failpoint.Enable(failpoint.ConsumerProcessTransiently, "error"); err != nil {
		t.Fatal(err)
	}
	q.Send(t, "doomed", SQSData(Op("provider-a", "doomed-bet", pid, wid, "BET", "10.00", ""), "provider-a:doomed-bet"))
	var dlq int
	Eventually(t, 40*time.Second, "redriven to DLQ", func() bool {
		dlq += len(q.Drain(t, q.DLQURL, time.Second))
		return dlq == 1
	})
	if hits := failpoint.Hits(failpoint.ConsumerProcessTransiently); hits < 3 {
		t.Fatalf("attempts before DLQ = %d", hits)
	}
	if statusOf(t, db, "doomed-bet") != "" {
		t.Fatal("dead-lettered message left an operation")
	}
	if v := Metric(t, a.BaseURL, "wagering_retries_total"); v < 3 {
		t.Fatalf("retry metric = %v", v)
	}
	db.AssertConsistent(t, wid)
}
