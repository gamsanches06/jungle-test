BEGIN;

-- Wallets: one per (player_id, currency). Balance is stored in minor units
-- (cents) as BIGINT and can never be negative.
CREATE TABLE wallets (
    id            UUID        PRIMARY KEY,
    player_id     UUID        NOT NULL,
    currency      CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor BIGINT      NOT NULL CHECK (balance_minor >= 0),
    version       BIGINT      NOT NULL CHECK (version >= 1),
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency)
);

CREATE TABLE wager_transactions (
    id                                UUID        PRIMARY KEY,
    origin                            TEXT        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              TEXT        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            TEXT        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         UUID        NOT NULL REFERENCES wallets (id),
    player_id                         UUID        NOT NULL,
    amount_minor                      BIGINT      NOT NULL CHECK (amount_minor >= 0),
    currency                          CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    provider_id                       TEXT,
    external_transaction_id           TEXT,
    idempotency_key                   TEXT,
    payload_hash                      TEXT,
    round_id                          TEXT,
    game_id                           TEXT,
    reference_external_transaction_id TEXT,
    reference_transaction_id          UUID REFERENCES wager_transactions (id),
    failure_code                      TEXT,
    failure_detail                    TEXT,
    result_balance_minor              BIGINT CHECK (result_balance_minor >= 0),
    result_wallet_version             BIGINT CHECK (result_wallet_version >= 1),
    attempts                          INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   TIMESTAMPTZ,
    expires_at                        TIMESTAMPTZ,
    correlation_id                    TEXT        NOT NULL,
    created_at                        TIMESTAMPTZ NOT NULL,
    updated_at                        TIMESTAMPTZ NOT NULL,
    completed_at                      TIMESTAMPTZ,

    CONSTRAINT wager_transactions_id_wallet_key UNIQUE (id, wallet_id),

    -- Internal operations (OPENING) carry no provider metadata and are
    -- always written already PROCESSED with a positive amount.
    CONSTRAINT wager_transactions_origin_shape CHECK (
        (origin = 'INTERNAL'
            AND kind = 'OPENING'
            AND status = 'PROCESSED'
            AND amount_minor > 0
            AND provider_id IS NULL AND external_transaction_id IS NULL
            AND idempotency_key IS NULL AND payload_hash IS NULL
            AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL
            AND reference_transaction_id IS NULL)
        OR
        (origin = 'EXTERNAL'
            AND kind <> 'OPENING'
            AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL
            AND idempotency_key IS NOT NULL AND payload_hash IS NOT NULL
            AND round_id IS NOT NULL AND game_id IS NOT NULL)
    ),
    -- Zero policy: LOSS is exactly zero, every other external kind is positive.
    CONSTRAINT wager_transactions_amount_policy CHECK (
        (kind = 'LOSS' AND amount_minor = 0) OR (kind <> 'LOSS' AND amount_minor > 0)
    ),
    CONSTRAINT wager_transactions_reference_required CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL
    ),
    CONSTRAINT wager_transactions_reference_allowed CHECK (
        kind IN ('WIN', 'REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NULL
    ),
    CONSTRAINT wager_transactions_processed_shape CHECK (
        status <> 'PROCESSED' OR (
            result_balance_minor IS NOT NULL AND result_wallet_version IS NOT NULL
            AND completed_at IS NOT NULL AND failure_code IS NULL
            AND (reference_external_transaction_id IS NULL OR reference_transaction_id IS NOT NULL))
    ),
    CONSTRAINT wager_transactions_failure_shape CHECK (
        status NOT IN ('REJECTED', 'FAILED') OR (failure_code IS NOT NULL AND completed_at IS NOT NULL)
    ),
    CONSTRAINT wager_transactions_pending_shape CHECK (
        status <> 'PENDING_REFERENCE' OR (next_attempt_at IS NOT NULL AND expires_at IS NOT NULL)
    )
);

-- Persistent idempotency: a provider operation exists once, and a key is
-- bound to exactly one operation of that provider.
CREATE UNIQUE INDEX wager_transactions_provider_external_key
    ON wager_transactions (provider_id, external_transaction_id) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_transactions_provider_idempotency_key
    ON wager_transactions (provider_id, idempotency_key) WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX wager_transactions_single_opening
    ON wager_transactions (wallet_id) WHERE kind = 'OPENING';
-- A referenced operation receives at most one successful reversal
-- (REFUND or ROLLBACK, of any of the two kinds).
CREATE UNIQUE INDEX wager_transactions_single_reversal
    ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');
-- Durable resumption of pending work.
CREATE INDEX wager_transactions_pending_due
    ON wager_transactions (next_attempt_at)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');
CREATE INDEX wager_transactions_waiting_reference
    ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE status IN ('PENDING', 'PENDING_REFERENCE');

CREATE TABLE wallet_ledger_entries (
    id                   UUID        PRIMARY KEY,
    wallet_id            UUID        NOT NULL REFERENCES wallets (id),
    transaction_id       UUID        NOT NULL,
    direction            TEXT        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor         BIGINT      NOT NULL CHECK (amount_minor > 0),
    currency             CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_before_minor BIGINT      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  BIGINT      NOT NULL CHECK (balance_after_minor >= 0),
    wallet_version       BIGINT      NOT NULL CHECK (wallet_version >= 1),
    created_at           TIMESTAMPTZ NOT NULL,
    CONSTRAINT wallet_ledger_entries_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT wallet_ledger_entries_transaction_fk
        FOREIGN KEY (transaction_id, wallet_id) REFERENCES wager_transactions (id, wallet_id),
    CONSTRAINT wallet_ledger_entries_balance_math CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
    )
);

COMMIT;
