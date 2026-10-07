//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gamsanches06/jungle-test/migrations"
)

func TestMigrationsUpDownUp(t *testing.T) {
	db := NewDatabase(t)
	m, err := migrations.New(db.OwnerURL)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if v, dirty, _ := m.Version(); v != 3 || dirty {
		t.Fatalf("version after up = %d dirty=%v", v, dirty)
	}
	if err := m.Down(); err != nil {
		t.Fatalf("down: %v", err)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name <> 'schema_migrations'`); n != 0 {
		t.Fatalf("%d tables left after down", n)
	}
	if err := m.Up(); err != nil {
		t.Fatalf("up again: %v", err)
	}
	if err := m.Steps(-1); err != nil {
		t.Fatalf("down 1: %v", err)
	}
	if n := db.Int(t, `SELECT COUNT(*) FROM pg_trigger WHERE tgname = 'wallet_ledger_entries_no_update_delete'`); n != 0 {
		t.Fatal("trigger left after reverting 000003")
	}
	if err := m.Up(); err != nil {
		t.Fatal(err)
	}
}

// seed creates a wallet with balance 100.00 and one BET of 25.00 directly in
// SQL (as the owner), returning ids.
func seed(t *testing.T, db *Database) (walletID, betID uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	walletID, betID = uuid.New(), uuid.New()
	openID := uuid.New()
	tx, err := db.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	exec := func(sql string, args ...any) {
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	exec(`INSERT INTO wallets VALUES ($1, $2, 'BRL', 10000, 1, now(), now())`, walletID, uuid.New())
	exec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		result_balance_minor, result_wallet_version, correlation_id, created_at, updated_at, completed_at)
		SELECT $1, 'INTERNAL', 'OPENING', 'PROCESSED', $2, player_id, 10000, 'BRL', 10000, 1, 'c', now(), now(), now() FROM wallets WHERE id = $2`, openID, walletID)
	exec(`INSERT INTO wallet_ledger_entries VALUES ($1, $2, $3, 'CREDIT', 10000, 'BRL', 0, 10000, 1, now())`, uuid.New(), walletID, openID)
	exec(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
		result_balance_minor, result_wallet_version, correlation_id, created_at, updated_at, completed_at)
		SELECT $1, 'EXTERNAL', 'BET', 'PROCESSED', $2, player_id, 2500, 'BRL', 'p', 'bet-1', 'k1', 'h', 'r', 'g',
		7500, 2, 'c', now(), now(), now() FROM wallets WHERE id = $2`, betID, walletID)
	exec(`INSERT INTO wallet_ledger_entries VALUES ($1, $2, $3, 'DEBIT', 2500, 'BRL', 10000, 7500, 2, now())`, uuid.New(), walletID, betID)
	exec(`UPDATE wallets SET balance_minor = 7500, version = 2 WHERE id = $1`, walletID)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	return walletID, betID
}

// expectFailure runs sql in its own transaction and expects an error
// containing want (checked at commit for deferred constraints).
func expectFailure(t *testing.T, pool *pgxpool.Pool, want string, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		for _, s := range stmts {
			if _, err := tx.Exec(ctx, s); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		t.Fatalf("expected failure %q for %v", want, stmts)
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want it to contain %q", err, want)
	}
}

func TestSchemaEnforcesFinancialInvariants(t *testing.T) {
	db := NewDatabase(t)
	w, bet := seed(t, db)
	ws, bs := "'"+w.String()+"'", "'"+bet.String()+"'"

	t.Run("ledger is append-only even for the owner", func(t *testing.T) {
		expectFailure(t, db.Owner, "append-only", `UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = `+ws)
		expectFailure(t, db.Owner, "append-only", `DELETE FROM wallet_ledger_entries WHERE wallet_id = `+ws)
		expectFailure(t, db.Owner, "append-only", `TRUNCATE wallet_ledger_entries CASCADE`)
	})
	t.Run("balance cannot be negative", func(t *testing.T) {
		expectFailure(t, db.Owner, "balance_minor_check", `UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = `+ws)
	})
	t.Run("balance change requires ledger entry", func(t *testing.T) {
		expectFailure(t, db.Owner, "no matching ledger entry", `UPDATE wallets SET balance_minor = 1, version = version + 1 WHERE id = `+ws)
	})
	t.Run("version moves with balance", func(t *testing.T) {
		expectFailure(t, db.Owner, "increment version", `UPDATE wallets SET balance_minor = 1 WHERE id = `+ws)
		expectFailure(t, db.Owner, "version only changes", `UPDATE wallets SET version = version + 1 WHERE id = `+ws)
	})
	t.Run("ledger math is checked", func(t *testing.T) {
		expectFailure(t, db.Owner, "balance_math", `INSERT INTO wallet_ledger_entries
			VALUES (gen_random_uuid(), `+ws+`, `+bs+`, 'DEBIT', 100, 'BRL', 7500, 7500, 3, now())`)
	})
	t.Run("one entry per wallet and transaction", func(t *testing.T) {
		expectFailure(t, db.Owner, "wallet_ledger_entries_wallet_transaction_key", `INSERT INTO wallet_ledger_entries
			VALUES (gen_random_uuid(), `+ws+`, `+bs+`, 'DEBIT', 100, 'BRL', 7500, 7400, 3, now())`)
	})
	t.Run("entries only for processed money-moving transactions", func(t *testing.T) {
		expectFailure(t, db.Owner, "does not match a processed", `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id,
			amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			correlation_id, created_at, updated_at) SELECT '11111111-1111-1111-1111-111111111111', 'EXTERNAL', 'LOSS', 'PENDING', id, player_id,
			0, 'BRL', 'p', 'loss-1', 'kl', 'h', 'r', 'g', 'c', now(), now() FROM wallets WHERE id = `+ws,
			`INSERT INTO wallet_ledger_entries VALUES (gen_random_uuid(), `+ws+`, '11111111-1111-1111-1111-111111111111', 'CREDIT', 100, 'BRL', 7500, 7600, 3, now())`,
			`UPDATE wallets SET balance_minor = 7600, version = 3 WHERE id = `+ws)
	})
	t.Run("single opening per wallet", func(t *testing.T) {
		expectFailure(t, db.Owner, "wager_transactions_single_opening", `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id,
			player_id, amount_minor, currency, result_balance_minor, result_wallet_version, correlation_id, created_at, updated_at, completed_at)
			SELECT gen_random_uuid(), 'INTERNAL', 'OPENING', 'PROCESSED', id, player_id, 100, 'BRL', 100, 1, 'c', now(), now(), now() FROM wallets WHERE id = `+ws)
	})
	t.Run("opening carries no provider metadata", func(t *testing.T) {
		expectFailure(t, db.Owner, "origin_shape", `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id,
			player_id, amount_minor, currency, provider_id, result_balance_minor, result_wallet_version, correlation_id, created_at, updated_at, completed_at)
			SELECT gen_random_uuid(), 'INTERNAL', 'OPENING', 'PROCESSED', id, player_id, 100, 'BRL', 'p', 100, 1, 'c', now(), now(), now() FROM wallets WHERE id = `+ws)
	})
	t.Run("provider operation is unique", func(t *testing.T) {
		expectFailure(t, db.Owner, "wager_transactions_provider_external_key", `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id,
			player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			correlation_id, created_at, updated_at) SELECT gen_random_uuid(), 'EXTERNAL', 'BET', 'PENDING', id, player_id, 100, 'BRL',
			'p', 'bet-1', 'other-key', 'h', 'r', 'g', 'c', now(), now() FROM wallets WHERE id = `+ws)
		expectFailure(t, db.Owner, "wager_transactions_provider_idempotency_key", `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id,
			player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			correlation_id, created_at, updated_at) SELECT gen_random_uuid(), 'EXTERNAL', 'BET', 'PENDING', id, player_id, 100, 'BRL',
			'p', 'bet-2', 'k1', 'h', 'r', 'g', 'c', now(), now() FROM wallets WHERE id = `+ws)
	})
	t.Run("zero amount policy", func(t *testing.T) {
		expectFailure(t, db.Owner, "amount_policy", `INSERT INTO wager_transactions (id, origin, kind, status, wallet_id,
			player_id, amount_minor, currency, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			correlation_id, created_at, updated_at) SELECT gen_random_uuid(), 'EXTERNAL', 'BET', 'PENDING', id, player_id, 0, 'BRL',
			'p', 'bet-0', 'k0', 'h', 'r', 'g', 'c', now(), now() FROM wallets WHERE id = `+ws)
	})
	t.Run("terminal transactions are immutable", func(t *testing.T) {
		expectFailure(t, db.Owner, "is terminal", `UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'X' WHERE id = `+bs)
		expectFailure(t, db.Owner, "cannot be deleted", `DELETE FROM wager_transactions WHERE id = `+bs)
	})
	t.Run("application role has no destructive privileges", func(t *testing.T) {
		app, err := pgxpool.New(context.Background(), db.AppURL)
		if err != nil {
			t.Fatal(err)
		}
		defer app.Close()
		expectFailure(t, app, "permission denied", `DELETE FROM wallet_ledger_entries`)
		expectFailure(t, app, "permission denied", `UPDATE wallet_ledger_entries SET amount_minor = 1`)
		expectFailure(t, app, "permission denied", `DELETE FROM wallets`)
	})

	db.AssertConsistent(t, w.String())
}
