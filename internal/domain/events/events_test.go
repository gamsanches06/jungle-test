package events_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/events"
	"github.com/gamsanches06/jungle-test/internal/domain/money"
)

func TestConstructorsFixTypeAndVersion(t *testing.T) {
	m, _ := money.Parse("25.00", "BRL")
	meta := events.Metadata{EventID: uuid.New(), CorrelationID: "corr", CausationID: "msg-1",
		OccurredAt: time.Date(2026, 9, 8, 9, 0, 0, 0, time.FixedZone("BRT", -3*3600))}
	wid, tid := uuid.New(), uuid.New()
	ev, err := events.NewWalletBalanceChanged(meta, events.WalletBalanceChangedData{
		WalletID: wid, TransactionID: tid, Direction: "DEBIT", Money: m, BalanceBefore: m, BalanceAfter: m, WalletVersion: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ev.EventType() != events.TypeWalletBalanceChanged || ev.Version() != 1 || ev.AggregateID() != wid || ev.GroupKey() != wid.String() {
		t.Fatalf("envelope = %s v%d %s", ev.EventType(), ev.Version(), ev.AggregateID())
	}
	b, err := ev.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var env map[string]any
	if err := json.Unmarshal(b, &env); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := env[k]; !ok {
			t.Errorf("envelope missing %s", k)
		}
	}
	if env["occurredAt"] != "2026-09-08T12:00:00.000Z" {
		t.Errorf("occurredAt must be UTC RFC 3339: %v", env["occurredAt"])
	}
	data := env["data"].(map[string]any)
	if data["money"].(map[string]any)["amount"] != "25.00" {
		t.Errorf("money must be a decimal string: %v", data["money"])
	}

	meta.CausationID = ""
	p, err := events.NewWagerTransactionProcessed(meta, events.WagerTransactionProcessedData{TransactionID: tid, WalletID: wid, Money: m, BalanceAfter: m})
	if err != nil || p.EventType() != events.TypeWagerTransactionProcessed || p.AggregateID() != tid {
		t.Fatalf("processed = %v %v", p, err)
	}
	b, _ = p.Marshal()
	env = map[string]any{}
	_ = json.Unmarshal(b, &env)
	if _, ok := env["causationId"]; ok {
		t.Error("causationId must be omitted when empty")
	}
}

func TestEnvelopeValidation(t *testing.T) {
	m, _ := money.Parse("1.00", "BRL")
	d := events.WagerTransactionRejectedData{TransactionID: uuid.New(), WalletID: uuid.New(), Money: m}
	cases := []events.Metadata{
		{CorrelationID: "c", OccurredAt: time.Now()},
		{EventID: uuid.New(), OccurredAt: time.Now()},
		{EventID: uuid.New(), CorrelationID: "c"},
	}
	for i, meta := range cases {
		if _, err := events.NewWagerTransactionRejected(meta, d); !errors.Is(err, events.ErrInvalidEvent) {
			t.Errorf("case %d: %v", i, err)
		}
	}
}
