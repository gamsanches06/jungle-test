BEGIN;

-- ---------------------------------------------------------------------------
-- Ledger: append-only. UPDATE, DELETE and TRUNCATE are rejected for every
-- role (including the owner); the application role is also never granted
-- these privileges.
-- ---------------------------------------------------------------------------
CREATE FUNCTION ledger_forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only (% rejected)', TG_OP
        USING ERRCODE = 'integrity_constraint_violation';
END;
$$;

CREATE TRIGGER wallet_ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION ledger_forbid_mutation();
CREATE TRIGGER wallet_ledger_entries_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger_forbid_mutation();

-- A ledger entry must belong to a PROCESSED transaction that moves money
-- (LOSS and rejected operations never produce entries). Checked at commit.
CREATE FUNCTION ledger_check_transaction() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    t RECORD;
BEGIN
    SELECT status, kind, amount_minor, currency INTO t
      FROM wager_transactions WHERE id = NEW.transaction_id;
    IF t.status IS DISTINCT FROM 'PROCESSED' OR t.kind = 'LOSS'
       OR t.amount_minor <> NEW.amount_minor OR t.currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger entry % does not match a processed money-moving transaction', NEW.id
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallet_ledger_entries_transaction_check
    AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger_check_transaction();

-- ---------------------------------------------------------------------------
-- Wallets: identity is immutable, version moves by exactly one with every
-- balance change (and only then), and every balance change must be matched
-- by the ledger entry written in the same SQL transaction.
-- ---------------------------------------------------------------------------
CREATE FUNCTION wallets_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallets cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF TG_OP = 'INSERT' THEN
        IF NEW.version <> 1 THEN
            RAISE EXCEPTION 'a new wallet starts at version 1' USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'balance change must increment version by one' USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'version only changes with the balance' USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallets_guard_row
    BEFORE INSERT OR UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard();

CREATE FUNCTION wallets_require_ledger() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    before_minor BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.balance_minor = 0 THEN
            RETURN NULL;
        END IF;
        before_minor := 0;
    ELSE
        IF NEW.balance_minor = OLD.balance_minor THEN
            RETURN NULL;
        END IF;
        before_minor := OLD.balance_minor;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM wallet_ledger_entries
         WHERE wallet_id = NEW.id
           AND wallet_version = NEW.version
           AND balance_before_minor = before_minor
           AND balance_after_minor = NEW.balance_minor) THEN
        RAISE EXCEPTION 'balance change of wallet % (version %) has no matching ledger entry', NEW.id, NEW.version
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallets_balance_has_ledger
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_require_ledger();

-- ---------------------------------------------------------------------------
-- Wager transactions: no deletes, immutable business fields, and terminal
-- states (PROCESSED, REJECTED, FAILED) accept no further changes.
-- ---------------------------------------------------------------------------
CREATE FUNCTION wager_transactions_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wager transactions cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'wager transaction % is terminal (%)', OLD.id, OLD.status
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind
       OR NEW.wallet_id <> OLD.wallet_id OR NEW.player_id <> OLD.player_id
       OR NEW.amount_minor <> OLD.amount_minor OR NEW.currency <> OLD.currency
       OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
       OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
       OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
       OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
       OR NEW.round_id IS DISTINCT FROM OLD.round_id
       OR NEW.game_id IS DISTINCT FROM OLD.game_id
       OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
       OR NEW.correlation_id <> OLD.correlation_id
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'wager transaction business fields are immutable'
            USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_transactions_guard_row
    BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();

-- ---------------------------------------------------------------------------
-- Outbox: the event snapshot is immutable; publication cannot be undone.
-- ---------------------------------------------------------------------------
CREATE FUNCTION outbox_events_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.published_at IS NULL THEN
            RAISE EXCEPTION 'unpublished outbox events cannot be deleted'
                USING ERRCODE = 'integrity_constraint_violation';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.id <> OLD.id OR NEW.aggregate_type <> OLD.aggregate_type OR NEW.aggregate_id <> OLD.aggregate_id
       OR NEW.group_key <> OLD.group_key OR NEW.event_type <> OLD.event_type
       OR NEW.event_version <> OLD.event_version OR NEW.payload::text <> OLD.payload::text
       OR NEW.correlation_id <> OLD.correlation_id
       OR NEW.causation_id IS DISTINCT FROM OLD.causation_id
       OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event snapshot is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox publication cannot be undone' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_guard_row
    BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();

-- Inbox: the message identity and hash are immutable.
CREATE FUNCTION inbox_messages_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'inbox messages cannot be deleted' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    IF NEW.consumer_name <> OLD.consumer_name OR NEW.message_id <> OLD.message_id
       OR NEW.payload_hash <> OLD.payload_hash OR NEW.received_at <> OLD.received_at THEN
        RAISE EXCEPTION 'inbox message identity is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER inbox_messages_guard_row
    BEFORE UPDATE OR DELETE ON inbox_messages
    FOR EACH ROW EXECUTE FUNCTION inbox_messages_guard();

-- ---------------------------------------------------------------------------
-- Least privilege for the application role (created by the database
-- bootstrap, see deploy/postgres). The ledger is INSERT/SELECT only.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'jungle-app') THEN
        EXECUTE 'GRANT USAGE ON SCHEMA public TO "jungle-app"';
        EXECUTE 'GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, inbox_messages, outbox_events TO "jungle-app"';
        EXECUTE 'GRANT SELECT, INSERT ON wallet_ledger_entries TO "jungle-app"';
        EXECUTE 'REVOKE UPDATE, DELETE, TRUNCATE ON wallet_ledger_entries FROM "jungle-app"';
        EXECUTE 'REVOKE DELETE, TRUNCATE ON wallets, wager_transactions, inbox_messages, outbox_events FROM "jungle-app"';
    END IF;
END;
$$;

COMMIT;
