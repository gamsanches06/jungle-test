package wagering_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
)

const (
	playerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	walletID = "0192f291-27dd-7d3f-8071-5f8685deef37"
)

func raw(kind, amount string) wagering.RawRequest {
	return wagering.RawRequest{
		ProviderID: "provider-a", ExternalTransactionID: "transaction-123", PlayerID: playerID, WalletID: walletID,
		RoundID: "round-987", GameID: "fortune-chimp", Kind: kind, Amount: amount, Currency: "BRL", HasMoney: true,
	}
}

func TestZeroValuePolicyPerKind(t *testing.T) {
	cases := []struct {
		kind, amount, ref string
		ok                bool
	}{
		{"BET", "25.00", "", true},
		{"BET", "0.00", "", false},
		{"WIN", "10.00", "", true},
		{"WIN", "10.00", "bet-1", true},
		{"WIN", "0.00", "", false},
		{"LOSS", "0.00", "", true},
		{"LOSS", "0.01", "", false},
		{"REFUND", "25.00", "bet-1", true},
		{"REFUND", "0.00", "bet-1", false},
		{"ROLLBACK", "25.00", "bet-1", true},
		{"ROLLBACK", "0.00", "bet-1", false},
	}
	for _, c := range cases {
		r := raw(c.kind, c.amount)
		r.ReferenceExternalTransactionID = c.ref
		_, err := wagering.NewRequest(r)
		if (err == nil) != c.ok {
			t.Errorf("%s %s ref=%q: err=%v, want ok=%v", c.kind, c.amount, c.ref, err, c.ok)
		}
		if err != nil && !errors.Is(err, wagering.ErrValidation) {
			t.Errorf("%s: error not classified as validation: %v", c.kind, err)
		}
	}
}

func TestReferenceRules(t *testing.T) {
	for _, kind := range []string{"REFUND", "ROLLBACK"} {
		if _, err := wagering.NewRequest(raw(kind, "25.00")); err == nil {
			t.Errorf("%s without reference accepted", kind)
		}
	}
	for _, kind := range []string{"BET", "LOSS"} {
		amount := "25.00"
		if kind == "LOSS" {
			amount = "0.00"
		}
		r := raw(kind, amount)
		r.ReferenceExternalTransactionID = "x"
		if _, err := wagering.NewRequest(r); err == nil {
			t.Errorf("%s with reference accepted", kind)
		}
	}
	r := raw("REFUND", "25.00")
	r.ReferenceExternalTransactionID = r.ExternalTransactionID
	if _, err := wagering.NewRequest(r); err == nil {
		t.Error("self reference accepted")
	}
}

func TestOpeningIsRejectedFromExternalSources(t *testing.T) {
	_, err := wagering.NewRequest(raw("OPENING", "25.00"))
	var ve *wagering.ValidationError
	if !errors.As(err, &ve) || ve.FailureCodeOf() != wagering.CodeInternalKindNotAllowed {
		t.Fatalf("OPENING = %v", err)
	}
	if _, err := wagering.NewRequest(raw("JACKPOT", "1.00")); err == nil {
		t.Fatal("unknown kind accepted")
	}
}

func TestInvalidFieldsAreReported(t *testing.T) {
	r := raw("BET", "25.00")
	r.ProviderID, r.PlayerID, r.WalletID = "", "not-a-uuid", "00000000-0000-0000-0000-000000000000"
	r.RoundID, r.GameID = "round 1", strings.Repeat("g", 200)
	r.Amount = "-25.00"
	_, err := wagering.NewRequest(r)
	if err == nil {
		t.Fatal("invalid request accepted")
	}
	for _, f := range []string{"providerId", "playerId", "walletId", "roundId", "gameId", "money"} {
		if !strings.Contains(err.Error(), f) {
			t.Errorf("error does not mention %s: %v", f, err)
		}
	}
	r = raw("BET", "")
	r.HasMoney = false
	if _, err := wagering.NewRequest(r); err == nil || !strings.Contains(err.Error(), "money") {
		t.Errorf("missing money: %v", err)
	}
	for _, amount := range []string{"25", "25.000", "NaN", "Infinity", "1e2", ""} {
		if _, err := wagering.NewRequest(raw("BET", amount)); err == nil {
			t.Errorf("amount %q accepted", amount)
		}
	}
}

func TestPayloadHashIsCanonical(t *testing.T) {
	a, err := wagering.NewRequest(raw("BET", "25.00"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}`
	if got := string(a.CanonicalJSON()); got != want {
		t.Fatalf("canonical JSON\n got %s\nwant %s", got, want)
	}
	if !strings.HasPrefix(a.PayloadHash(), "sha256:") || len(a.PayloadHash()) != 7+64 {
		t.Fatalf("hash format %s", a.PayloadHash())
	}
	r := raw("BET", "25.00")
	r.PlayerID = strings.ToUpper(playerID)
	b, err := wagering.NewRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	if a.PayloadHash() != b.PayloadHash() {
		t.Error("UUID case changed the hash")
	}
}

func TestPayloadHashDetectsBusinessChanges(t *testing.T) {
	base, _ := wagering.NewRequest(raw("BET", "25.00"))
	mutations := map[string]func(*wagering.RawRequest){
		"amount":   func(r *wagering.RawRequest) { r.Amount = "25.01" },
		"currency": func(r *wagering.RawRequest) { r.Currency = "USD" },
		"round":    func(r *wagering.RawRequest) { r.RoundID = "round-988" },
		"game":     func(r *wagering.RawRequest) { r.GameID = "other" },
		"kind":     func(r *wagering.RawRequest) { r.Kind = "WIN" },
		"player":   func(r *wagering.RawRequest) { r.PlayerID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2" },
		"wallet":   func(r *wagering.RawRequest) { r.WalletID = "0192f291-27dd-7d3f-8071-5f8685deef38" },
		"external": func(r *wagering.RawRequest) { r.ExternalTransactionID = "transaction-124" },
		"provider": func(r *wagering.RawRequest) { r.ProviderID = "provider-b" },
	}
	for name, mut := range mutations {
		r := raw("BET", "25.00")
		mut(&r)
		other, err := wagering.NewRequest(r)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if other.PayloadHash() == base.PayloadHash() {
			t.Errorf("changing %s did not change the hash", name)
		}
	}
	withRef := raw("WIN", "25.00")
	noRef, _ := wagering.NewRequest(withRef)
	withRef.ReferenceExternalTransactionID = "bet-1"
	ref, _ := wagering.NewRequest(withRef)
	if noRef.PayloadHash() == ref.PayloadHash() {
		t.Error("reference does not affect the hash")
	}
}

func TestIdempotencyKeyValidation(t *testing.T) {
	for _, k := range []string{"", "has space", strings.Repeat("k", 256), "tab\tkey", "ção"} {
		if err := wagering.ValidateIdempotencyKey(k); err == nil {
			t.Errorf("key %q accepted", k)
		}
	}
	if err := wagering.ValidateIdempotencyKey("provider-a:transaction-123"); err != nil {
		t.Error(err)
	}
}
