//go:build integration

package integration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/gamsanches06/jungle-test/internal/platform/failpoint"
)

func eventIDs(t *testing.T, q *Queues, wait time.Duration) map[string]int {
	t.Helper()
	seen := map[string]int{}
	for _, m := range q.Drain(t, q.EventsURL, wait) {
		var env struct {
			EventID string `json:"eventId"`
		}
		if err := json.Unmarshal([]byte(aws.ToString(m.Body)), &env); err != nil || env.EventID == "" {
			t.Fatalf("bad event %s", aws.ToString(m.Body))
		}
		if attr := aws.ToString(m.MessageAttributes["eventId"].StringValue); attr != env.EventID {
			t.Fatalf("eventId attribute %s != body %s", attr, env.EventID)
		}
		seen[env.EventID]++
	}
	return seen
}

func TestOutboxCompetingPublishers(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	s := NewSettings(db, q)
	api := StartApp(t, s.With("APP_ROLES", "api"))
	for i := 0; i < 30; i++ {
		OpenWallet(t, api.BaseURL, "10.00")
	}
	total := db.Int(t, `SELECT COUNT(*) FROM outbox_events`)
	if total != 60 || db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NOT NULL`) != 0 {
		t.Fatalf("outbox = %d events", total)
	}
	for i := 0; i < 3; i++ {
		StartApp(t, s.With("APP_ROLES", "outbox", "OUTBOX_BATCH_SIZE", "5"))
	}
	Eventually(t, 20*time.Second, "all events published", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	// SKIP LOCKED + leases: every event was claimed exactly once.
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE attempts <> 1`); n != 0 {
		t.Fatalf("%d events claimed more than once", n)
	}
	if n := db.Int(t, `SELECT COUNT(DISTINCT locked_by) FROM outbox_events`); n != 0 {
		t.Fatalf("leases left behind: %d", n)
	}
	seen := eventIDs(t, q, 3*time.Second)
	if len(seen) != 60 {
		t.Fatalf("received %d distinct events, want 60", len(seen))
	}
	for id := range seen {
		if db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE id = $1`, id) != 1 {
			t.Fatalf("event %s not in outbox", id)
		}
	}
}

func TestOutboxRecoversCrashBetweenClaimAndPublish(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	s := NewSettings(db, q)
	api := StartApp(t, s.With("APP_ROLES", "api"))
	OpenWallet(t, api.BaseURL, "10.00") // 2 committed, unpublished events

	a := StartProcess(t, s.With("APP_ROLES", "outbox", "FAILPOINTS", failpoint.OutboxAfterClaim+"=exit"))
	if !a.WaitExit(15 * time.Second) {
		t.Fatal("publisher A did not crash")
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE locked_by IS NOT NULL AND published_at IS NULL`); n == 0 {
		t.Fatal("expected abandoned leases")
	}
	StartProcess(t, s.With("APP_ROLES", "outbox"))
	Eventually(t, 20*time.Second, "events published by B", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	if n := db.Int(t, `SELECT MIN(attempts) FROM outbox_events`); n < 2 {
		t.Fatalf("attempts = %d, expected a second claim", n)
	}
	if seen := eventIDs(t, q, 3*time.Second); len(seen) != 2 {
		t.Fatalf("events = %v", seen)
	}
}

func TestOutboxRecoversCrashBetweenPublishAndConfirm(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	s := NewSettings(db, q)
	api := StartApp(t, s.With("APP_ROLES", "api"))
	OpenWallet(t, api.BaseURL, "10.00")
	first := db.Str(t, `SELECT id::text FROM outbox_events ORDER BY seq LIMIT 1`)

	a := StartProcess(t, s.With("APP_ROLES", "outbox", "FAILPOINTS", failpoint.OutboxAfterPublish+"=exit"))
	if !a.WaitExit(15 * time.Second) {
		t.Fatal("publisher A did not crash")
	}
	if db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE id = $1 AND published_at IS NULL`, first) != 1 {
		t.Fatal("first event should be published but unconfirmed")
	}
	StartProcess(t, s.With("APP_ROLES", "outbox"))
	Eventually(t, 20*time.Second, "all confirmed", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	if n := db.Int(t, `SELECT attempts FROM outbox_events WHERE id = $1`, first); n != 2 {
		t.Fatalf("first event attempts = %d (republished by B)", n)
	}
	// The republication kept the eventId: the FIFO queue deduplicates it by
	// MessageDeduplicationId=eventId and consumers can dedupe by eventId.
	seen := eventIDs(t, q, 3*time.Second)
	if len(seen) != 2 || seen[first] < 1 {
		t.Fatalf("events = %v", seen)
	}
}

func TestOutboxRetriesWithBackoff(t *testing.T) {
	db := NewDatabase(t)
	q := NewQueues(t, 4*time.Second, 3)
	s := NewSettings(db, q)
	t.Cleanup(failpoint.Reset)
	if err := failpoint.Enable(failpoint.PublisherSend, "error"); err != nil {
		t.Fatal(err)
	}
	StartApp(t, s.With("APP_ROLES", "api,outbox", "OUTBOX_RETRY_BASE", "300ms", "OUTBOX_RETRY_MAX", "1s"))
	api := StartApp(t, s.With("APP_ROLES", "api"))
	OpenWallet(t, api.BaseURL, "10.00")
	Eventually(t, 15*time.Second, "several failed attempts", func() bool {
		return db.Int(t, `SELECT MIN(attempts) FROM outbox_events`) >= 3
	})
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE last_error IS NULL OR published_at IS NOT NULL`); n != 0 {
		t.Fatalf("%d events without recorded error", n)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE next_attempt_at <= created_at`); n != 0 {
		t.Fatal("no backoff scheduled")
	}
	failpoint.Disable(failpoint.PublisherSend)
	Eventually(t, 15*time.Second, "published after recovery", func() bool {
		return db.Int(t, `SELECT COUNT(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	if seen := eventIDs(t, q, 3*time.Second); len(seen) != 2 {
		t.Fatalf("events = %v", seen)
	}
}
