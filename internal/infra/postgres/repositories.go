package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/gamsanches06/jungle-test/internal/application"
	"github.com/gamsanches06/jungle-test/internal/domain/events"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/domain/wallet"
)

// ---------------------------------------------------------------- wallets

type walletRepo struct{ q querier }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func scanWallet(row pgx.Row) (*wallet.Wallet, error) {
	var (
		id, player uuid.UUID
		currency   string
		balance    int64
		version    int64
		created    time.Time
		updated    time.Time
	)
	if err := row.Scan(&id, &player, &currency, &balance, &version, &created, &updated); err != nil {
		if notFound(err) {
			return nil, application.ErrWalletNotFound
		}
		return nil, err
	}
	c, err := money.ParseCurrency(currency)
	if err != nil {
		return nil, err
	}
	b, err := money.FromMinor(balance, c)
	if err != nil {
		return nil, err
	}
	return wallet.Rehydrate(wallet.Snapshot{ID: id, PlayerID: player, Balance: b, Version: version, CreatedAt: created, UpdatedAt: updated})
}

func (r walletRepo) Insert(ctx context.Context, w *wallet.Wallet) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallets (`+walletColumns+`) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		w.ID(), w.PlayerID(), w.Currency().Code(), w.Balance().Minor(), w.Version(), w.CreatedAt(), w.UpdatedAt())
	if c, ok := uniqueViolation(err); ok && c == "wallets_player_currency_key" {
		return application.ErrWalletAlreadyExists
	}
	return err
}

func (r walletRepo) Get(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

func (r walletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	return scanWallet(r.q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

func (r walletRepo) TryGetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, bool, error) {
	w, err := scanWallet(r.q.QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE SKIP LOCKED`, id))
	if err == application.ErrWalletNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return w, true, nil
}

// UpdateBalance is a compare-and-set on the version: even though the row is
// locked by FOR UPDATE, the guard makes a lost update impossible if a caller
// ever forgets the lock.
func (r walletRepo) UpdateBalance(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error {
	tag, err := r.q.Exec(ctx,
		`UPDATE wallets SET balance_minor = $1, version = $2, updated_at = $3 WHERE id = $4 AND version = $5`,
		w.Balance().Minor(), w.Version(), w.UpdatedAt(), w.ID(), expectedVersion)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrConcurrentUpdate
	}
	return nil
}

// ----------------------------------------------------------- transactions

type txRepo struct{ q querier }

const txColumns = `id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, failure_detail,
	result_balance_minor, result_wallet_version, attempts, next_attempt_at, expires_at,
	correlation_id, created_at, updated_at, completed_at`

func scanTx(row pgx.Row) (*wagering.Transaction, error) {
	var (
		id, walletID, playerID                          uuid.UUID
		origin, kind, status, currency, correlation     string
		amount                                          int64
		provider, extID, key, hash, round, game, refExt *string
		refID                                           *uuid.UUID
		failureCode, failureDetail                      *string
		resultBalance, resultVersion                    *int64
		attempts                                        int
		nextAttempt, expires, completed                 *time.Time
		created, updated                                time.Time
	)
	err := row.Scan(&id, &origin, &kind, &status, &walletID, &playerID, &amount, &currency,
		&provider, &extID, &key, &hash, &round, &game, &refExt, &refID, &failureCode, &failureDetail,
		&resultBalance, &resultVersion, &attempts, &nextAttempt, &expires, &correlation, &created, &updated, &completed)
	if err != nil {
		if notFound(err) {
			return nil, application.ErrTransactionNotFound
		}
		return nil, err
	}
	c, err := money.ParseCurrency(currency)
	if err != nil {
		return nil, err
	}
	m, err := money.FromMinor(amount, c)
	if err != nil {
		return nil, err
	}
	s := wagering.Snapshot{
		ID: id, Origin: wagering.Origin(origin), Kind: wagering.Kind(kind), Status: wagering.Status(status),
		WalletID: walletID, PlayerID: playerID, Money: m, Attempts: attempts,
		CorrelationID: correlation, CreatedAt: created, UpdatedAt: updated,
	}
	if provider != nil {
		s.External = &wagering.External{
			ProviderID: *provider, ExternalTransactionID: deref(extID), IdempotencyKey: deref(key),
			PayloadHash: deref(hash), RoundID: deref(round), GameID: deref(game),
			ReferenceExternalTransactionID: deref(refExt),
		}
	}
	if refID != nil {
		s.ReferenceTxID = *refID
	}
	s.FailureCode = wagering.FailureCode(deref(failureCode))
	s.FailureDetail = deref(failureDetail)
	if resultBalance != nil {
		b, err := money.FromMinor(*resultBalance, c)
		if err != nil {
			return nil, err
		}
		s.Result = &wagering.Result{Balance: b, WalletVersion: derefInt(resultVersion)}
	}
	if nextAttempt != nil {
		s.NextAttemptAt = *nextAttempt
	}
	if expires != nil {
		s.ExpiresAt = *expires
	}
	if completed != nil {
		s.CompletedAt = *completed
	}
	return wagering.Rehydrate(s)
}

type txValues struct {
	provider, extID, key, hash, round, game, refExt *string
	refID                                           *uuid.UUID
	failureCode, failureDetail                      *string
	resultBalance, resultVersion                    *int64
	nextAttempt, expires, completed                 *time.Time
}

func valuesOf(t *wagering.Transaction) txValues {
	var v txValues
	if ext, ok := t.External(); ok {
		v.provider, v.extID, v.key, v.hash = &ext.ProviderID, &ext.ExternalTransactionID, &ext.IdempotencyKey, &ext.PayloadHash
		v.round, v.game = &ext.RoundID, &ext.GameID
		v.refExt = nullable(ext.ReferenceExternalTransactionID)
	}
	if id := t.ReferenceTxID(); id != uuid.Nil {
		v.refID = &id
	}
	v.failureCode = nullable(string(t.FailureCode()))
	v.failureDetail = nullable(t.FailureDetail())
	if res, ok := t.Result(); ok {
		b, ver := res.Balance.Minor(), res.WalletVersion
		v.resultBalance, v.resultVersion = &b, &ver
	}
	v.nextAttempt = nullableTime(t.NextAttemptAt())
	v.expires = nullableTime(t.ExpiresAt())
	v.completed = nullableTime(t.CompletedAt())
	return v
}

func (r txRepo) Insert(ctx context.Context, t *wagering.Transaction) error {
	v := valuesOf(t)
	_, err := r.q.Exec(ctx, `INSERT INTO wager_transactions (`+txColumns+`)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27)`,
		t.ID(), string(t.Origin()), string(t.Kind()), string(t.Status()), t.WalletID(), t.PlayerID(),
		t.Money().Minor(), t.Money().Currency().Code(),
		v.provider, v.extID, v.key, v.hash, v.round, v.game, v.refExt, v.refID, v.failureCode, v.failureDetail,
		v.resultBalance, v.resultVersion, t.Attempts(), v.nextAttempt, v.expires,
		t.CorrelationID(), t.CreatedAt(), t.UpdatedAt(), v.completed)
	if c, ok := uniqueViolation(err); ok {
		return fmt.Errorf("%w (%s)", application.ErrDuplicateTransaction, c)
	}
	return err
}

func (r txRepo) Update(ctx context.Context, t *wagering.Transaction, expected wagering.Status) error {
	v := valuesOf(t)
	tag, err := r.q.Exec(ctx, `UPDATE wager_transactions SET
			status = $2, reference_transaction_id = $3, failure_code = $4, failure_detail = $5,
			result_balance_minor = $6, result_wallet_version = $7, attempts = $8,
			next_attempt_at = $9, expires_at = $10, updated_at = $11, completed_at = $12
		WHERE id = $1 AND status = $13`,
		t.ID(), string(t.Status()), v.refID, v.failureCode, v.failureDetail, v.resultBalance, v.resultVersion,
		t.Attempts(), v.nextAttempt, v.expires, t.UpdatedAt(), v.completed, string(expected))
	if c, ok := uniqueViolation(err); ok {
		return fmt.Errorf("%w (%s)", application.ErrDuplicateTransaction, c)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrConcurrentUpdate
	}
	return nil
}

func (r txRepo) Get(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return scanTx(r.q.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (r txRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*wagering.Transaction, error) {
	return scanTx(r.q.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1 FOR UPDATE`, id))
}

func (r txRepo) GetByExternalID(ctx context.Context, providerID, externalID string) (*wagering.Transaction, error) {
	return scanTx(r.q.QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (r txRepo) FindIdempotent(ctx context.Context, providerID, key, externalID string) ([]*wagering.Transaction, error) {
	rows, err := r.q.Query(ctx, `SELECT `+txColumns+` FROM wager_transactions
		WHERE origin = 'EXTERNAL' AND provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`,
		providerID, key, externalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*wagering.Transaction
	for rows.Next() {
		t, err := scanTx(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r txRepo) HasProcessedReversal(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := r.q.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM wager_transactions
		WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`, id).Scan(&exists)
	return exists, err
}

func (r txRepo) WakeDependents(ctx context.Context, providerID, externalID string, now time.Time) (int64, error) {
	tag, err := r.q.Exec(ctx, `UPDATE wager_transactions SET next_attempt_at = $3
		WHERE provider_id = $1 AND reference_external_transaction_id = $2
		  AND status IN ('PENDING', 'PENDING_REFERENCE') AND next_attempt_at > $3`, providerID, externalID, now)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (r txRepo) DuePending(ctx context.Context, now time.Time, limit int) ([]application.PendingRef, error) {
	rows, err := r.q.Query(ctx, `SELECT id, wallet_id FROM wager_transactions
		WHERE status IN ('PENDING', 'PENDING_REFERENCE') AND next_attempt_at <= $1
		ORDER BY next_attempt_at LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []application.PendingRef
	for rows.Next() {
		var p application.PendingRef
		if err := rows.Scan(&p.ID, &p.WalletID); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ----------------------------------------------------------------- ledger

type ledgerRepo struct{ q querier }

const ledgerColumns = `id, wallet_id, transaction_id, direction, amount_minor, currency,
	balance_before_minor, balance_after_minor, wallet_version, created_at`

func (r ledgerRepo) Append(ctx context.Context, e *wallet.LedgerEntry) error {
	_, err := r.q.Exec(ctx, `INSERT INTO wallet_ledger_entries (`+ledgerColumns+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		e.ID(), e.WalletID(), e.TransactionID(), string(e.Direction()), e.Amount().Minor(), e.Amount().Currency().Code(),
		e.BalanceBefore().Minor(), e.BalanceAfter().Minor(), e.WalletVersion(), e.CreatedAt())
	if c, ok := uniqueViolation(err); ok {
		return fmt.Errorf("%w (%s)", application.ErrDuplicateTransaction, c)
	}
	return err
}

func (r ledgerRepo) List(ctx context.Context, walletID uuid.UUID, afterVersion int64, limit int) ([]*wallet.LedgerEntry, error) {
	rows, err := r.q.Query(ctx, `SELECT `+ledgerColumns+` FROM wallet_ledger_entries
		WHERE wallet_id = $1 AND wallet_version > $2 ORDER BY wallet_version LIMIT $3`, walletID, afterVersion, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*wallet.LedgerEntry
	for rows.Next() {
		var (
			id, wid, tid                uuid.UUID
			dir, currency               string
			amount, before, after, wver int64
			created                     time.Time
		)
		if err := rows.Scan(&id, &wid, &tid, &dir, &amount, &currency, &before, &after, &wver, &created); err != nil {
			return nil, err
		}
		c, err := money.ParseCurrency(currency)
		if err != nil {
			return nil, err
		}
		d, err := wallet.ParseDirection(dir)
		if err != nil {
			return nil, err
		}
		am, _ := money.FromMinor(amount, c)
		bb, _ := money.FromMinor(before, c)
		ba, _ := money.FromMinor(after, c)
		e, err := wallet.NewLedgerEntry(wallet.LedgerEntryParams{
			ID: id, WalletID: wid, TransactionID: tid, Direction: d, Amount: am,
			BalanceBefore: bb, BalanceAfter: ba, WalletVersion: wver, CreatedAt: created,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Summarize sums in NUMERIC (no overflow inside SQL) and parses the integer
// result exactly; a value outside int64 is reported as an error.
func (r ledgerRepo) Summarize(ctx context.Context, walletID uuid.UUID, currency money.Currency) (application.LedgerSummary, error) {
	var (
		total string
		count int64
	)
	err := r.q.QueryRow(ctx, `SELECT
			COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_minor::numeric ELSE -amount_minor::numeric END), 0)::text,
			COUNT(*)
		FROM wallet_ledger_entries WHERE wallet_id = $1 AND currency = $2`, walletID, currency.Code()).Scan(&total, &count)
	if err != nil {
		return application.LedgerSummary{}, err
	}
	minor, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return application.LedgerSummary{}, fmt.Errorf("ledger sum %q: %w", total, money.ErrOverflow)
	}
	m, err := money.FromMinor(minor, currency)
	if err != nil {
		return application.LedgerSummary{}, err
	}
	return application.LedgerSummary{Balance: m, Entries: count}, nil
}

// ----------------------------------------------------------------- outbox

type outboxRepo struct{ q querier }

func (r outboxRepo) Append(ctx context.Context, evs []events.Event) error {
	for _, e := range evs {
		payload, err := e.Marshal()
		if err != nil {
			return err
		}
		_, err = r.q.Exec(ctx, `INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, group_key, event_type, event_version, payload,
			 correlation_id, causation_id, occurred_at, next_attempt_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8, $9, $10, $10)`,
			e.EventID(), e.AggregateType(), e.AggregateID(), e.GroupKey(), e.EventType(), e.Version(), string(payload),
			e.CorrelationID(), nullable(e.CausationID()), e.OccurredAt())
		if err != nil {
			return err
		}
	}
	return nil
}

// ------------------------------------------------------------------ inbox

type inboxRepo struct{ q querier }

func (r inboxRepo) Register(ctx context.Context, m application.InboxMessage) (application.InboxRecord, bool, error) {
	tag, err := r.q.Exec(ctx, `INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4) ON CONFLICT (consumer_name, message_id) DO NOTHING`,
		m.Consumer, m.MessageID, m.PayloadHash, m.ReceivedAt)
	if err != nil {
		return application.InboxRecord{}, false, err
	}
	if tag.RowsAffected() == 1 {
		return application.InboxRecord{InboxMessage: m, Deliveries: 1}, true, nil
	}
	rec := application.InboxRecord{InboxMessage: application.InboxMessage{Consumer: m.Consumer, MessageID: m.MessageID}}
	var (
		txID      *uuid.UUID
		outcome   *string
		completed *time.Time
	)
	err = r.q.QueryRow(ctx, `UPDATE inbox_messages SET deliveries = deliveries + 1
		WHERE consumer_name = $1 AND message_id = $2
		RETURNING payload_hash, received_at, transaction_id, outcome, completed_at, deliveries`,
		m.Consumer, m.MessageID).Scan(&rec.PayloadHash, &rec.ReceivedAt, &txID, &outcome, &completed, &rec.Deliveries)
	if err != nil {
		return application.InboxRecord{}, false, err
	}
	if txID != nil {
		rec.TransactionID = *txID
	}
	rec.Outcome = deref(outcome)
	if completed != nil {
		rec.CompletedAt = *completed
	}
	return rec, false, nil
}

func (r inboxRepo) Complete(ctx context.Context, consumer, messageID string, txID uuid.UUID, outcome string, now time.Time) error {
	tag, err := r.q.Exec(ctx, `UPDATE inbox_messages SET transaction_id = $3, outcome = $4, completed_at = $5
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID, txID, outcome, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("inbox message %s/%s not registered", consumer, messageID)
	}
	return nil
}

// ---------------------------------------------------------------- helpers

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}
