package sqsx_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
	"github.com/gamsanches06/jungle-test/internal/infra/sqsx"
)

const valid = `{"messageId":"msg-123","type":"WagerTransactionRequested","occurredAt":"2026-09-08T12:00:00.000Z",
"data":{"providerId":"provider-a","externalTransactionId":"transaction-123","idempotencyKey":"provider-a:transaction-123",
"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37","roundId":"round-987",
"gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}}`

func TestParseValidMessage(t *testing.T) {
	m, err := sqsx.ParseMessage([]byte(valid))
	if err != nil {
		t.Fatal(err)
	}
	if m.MessageID != "msg-123" || m.IdempotencyKey != "provider-a:transaction-123" || m.Request.Kind() != wagering.KindBet {
		t.Fatalf("message = %+v", m)
	}
	req, _ := wagering.NewRequest(wagering.RawRequest{ProviderID: "provider-a", ExternalTransactionID: "transaction-123",
		PlayerID: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1", WalletID: "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID: "round-987", GameID: "fortune-chimp", Kind: "BET", Amount: "25.00", Currency: "BRL", HasMoney: true})
	if m.Request.PayloadHash() != req.PayloadHash() {
		t.Fatal("SQS and HTTP payload hashes differ")
	}
	other, _ := sqsx.ParseMessage([]byte(strings.Replace(valid, "12:00:00.000Z", "13:00:00.000Z", 1)))
	if other.InboxHash() != m.InboxHash() {
		t.Fatal("occurredAt changed the inbox hash")
	}
	changed, _ := sqsx.ParseMessage([]byte(strings.Replace(valid, `"25.00"`, `"26.00"`, 1)))
	if changed.InboxHash() == m.InboxHash() {
		t.Fatal("amount did not change the inbox hash")
	}
}

func TestParseInvalidMessages(t *testing.T) {
	cases := map[string]string{
		"not json":      `{`,
		"no message id": strings.Replace(valid, `"msg-123"`, `""`, 1),
		"wrong type":    strings.Replace(valid, "WagerTransactionRequested", "Other", 1),
		"bad time":      strings.Replace(valid, "2026-09-08T12:00:00.000Z", "yesterday", 1),
		"opening":       strings.Replace(valid, `"BET"`, `"OPENING"`, 1),
		"float amount":  strings.Replace(valid, `"25.00"`, `25.00`, 1),
		"no key":        strings.Replace(valid, `"provider-a:transaction-123"`, `""`, 1),
		"unknown field": strings.Replace(valid, `"type"`, `"extra":1,"type"`, 1),
		"trailing":      valid + `{}`,
	}
	for name, body := range cases {
		if _, err := sqsx.ParseMessage([]byte(body)); !errors.Is(err, sqsx.ErrInvalidMessage) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
