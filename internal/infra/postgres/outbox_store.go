package postgres

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OutboxRecord is a claimed outbox event.
type OutboxRecord struct {
	ID         uuid.UUID
	Seq        int64
	EventType  string
	GroupKey   string
	Payload    string
	Attempts   int
	OccurredAt time.Time
}

// OutboxStore implements claim/ack for the outbox publisher. Claims use
// FOR UPDATE SKIP LOCKED plus a lease (locked_until) evaluated with the
// database clock, so several publishers never take the same event at the
// same time and an event abandoned by a crashed publisher is taken again
// once its lease expires.
type OutboxStore struct{ pool *pgxpool.Pool }

// NewOutboxStore builds the store.
func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

// Claim leases up to limit due events to owner.
func (s *OutboxStore) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]OutboxRecord, error) {
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT id FROM outbox_events
			 WHERE published_at IS NULL
			   AND next_attempt_at <= now()
			   AND (locked_until IS NULL OR locked_until < now())
			 ORDER BY seq
			 LIMIT $1
			 FOR UPDATE SKIP LOCKED)
		UPDATE outbox_events o
		   SET locked_by = $2,
		       locked_until = now() + ($3::bigint * interval '1 millisecond'),
		       attempts = o.attempts + 1
		  FROM due
		 WHERE o.id = due.id
		RETURNING o.id, o.seq, o.event_type, o.group_key, o.payload::text, o.attempts, o.occurred_at`,
		limit, owner, lease.Milliseconds())
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []OutboxRecord
	for rows.Next() {
		var r OutboxRecord
		if err := rows.Scan(&r.ID, &r.Seq, &r.EventType, &r.GroupKey, &r.Payload, &r.Attempts, &r.OccurredAt); err != nil {
			return nil, classify(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// MarkPublished confirms publication. It is idempotent: a republished event
// whose lease was taken over by another publisher is confirmed only once.
func (s *OutboxStore) MarkPublished(ctx context.Context, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events
		SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		WHERE id = ANY($1) AND published_at IS NULL`, ids)
	return classify(err)
}

// MarkFailed releases the lease and schedules the next attempt.
func (s *OutboxStore) MarkFailed(ctx context.Context, id uuid.UUID, owner, lastError string, delay time.Duration) error {
	_, err := s.pool.Exec(ctx, `UPDATE outbox_events
		SET locked_by = NULL, locked_until = NULL, last_error = $3,
		    next_attempt_at = now() + ($4::bigint * interval '1 millisecond')
		WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, truncate(lastError, 500), delay.Milliseconds())
	return classify(err)
}

// Release returns every unpublished lease held by owner (graceful shutdown).
func (s *OutboxStore) Release(ctx context.Context, owner string) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox_events SET locked_by = NULL, locked_until = NULL
		WHERE locked_by = $1 AND published_at IS NULL`, owner)
	if err != nil {
		return 0, classify(err)
	}
	return tag.RowsAffected(), nil
}

// Lag returns the number of unpublished events and the age of the oldest.
func (s *OutboxStore) Lag(ctx context.Context) (int64, time.Duration, error) {
	var (
		count int64
		ageMs int64
	)
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*),
		COALESCE((EXTRACT(EPOCH FROM (now() - MIN(occurred_at))) * 1000)::bigint, 0)
		FROM outbox_events WHERE published_at IS NULL`).Scan(&count, &ageMs)
	if err != nil {
		return 0, 0, classify(err)
	}
	return count, time.Duration(ageMs) * time.Millisecond, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
