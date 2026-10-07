BEGIN;

-- Inbox: durable deduplication of consumed messages. The row is inserted and
-- completed in the same SQL transaction as the domain changes it caused.
CREATE TABLE inbox_messages (
    consumer_name  TEXT        NOT NULL,
    message_id     TEXT        NOT NULL,
    payload_hash   TEXT        NOT NULL,
    transaction_id UUID REFERENCES wager_transactions (id),
    outcome        TEXT,
    received_at    TIMESTAMPTZ NOT NULL,
    completed_at   TIMESTAMPTZ,
    deliveries     INTEGER     NOT NULL DEFAULT 1 CHECK (deliveries >= 1),
    PRIMARY KEY (consumer_name, message_id)
);

-- Transactional outbox. payload uses the json type (not jsonb) so the exact
-- bytes of the immutable snapshot are kept and republished unchanged.
CREATE TABLE outbox_events (
    id              UUID        PRIMARY KEY,
    seq             BIGINT      GENERATED ALWAYS AS IDENTITY UNIQUE,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    UUID        NOT NULL,
    group_key       TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    event_version   INTEGER     NOT NULL CHECK (event_version >= 1),
    payload         JSON        NOT NULL,
    correlation_id  TEXT        NOT NULL,
    causation_id    TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts        INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX outbox_events_unpublished
    ON outbox_events (next_attempt_at, seq) WHERE published_at IS NULL;

COMMIT;
