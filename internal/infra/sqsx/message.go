package sqsx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gamsanches06/jungle-test/internal/domain/wagering"
)

// MessageTypeWagerTransactionRequested is the only accepted envelope type.
const MessageTypeWagerTransactionRequested = "WagerTransactionRequested"

// ErrInvalidMessage marks a message that can never be processed.
var ErrInvalidMessage = errors.New("invalid message")

type moneyWire struct {
	Amount   *string `json:"amount"`
	Currency *string `json:"currency"`
}

type dataWire struct {
	ProviderID                     string     `json:"providerId"`
	ExternalTransactionID          string     `json:"externalTransactionId"`
	IdempotencyKey                 string     `json:"idempotencyKey"`
	PlayerID                       string     `json:"playerId"`
	WalletID                       string     `json:"walletId"`
	RoundID                        string     `json:"roundId"`
	GameID                         string     `json:"gameId"`
	Kind                           string     `json:"kind"`
	Money                          *moneyWire `json:"money"`
	ReferenceExternalTransactionID string     `json:"referenceExternalTransactionId,omitempty"`
}

type envelopeWire struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt string    `json:"occurredAt"`
	Data       *dataWire `json:"data"`
}

// Message is a parsed and validated input message.
type Message struct {
	MessageID      string
	Type           string
	OccurredAt     time.Time
	IdempotencyKey string
	Request        wagering.Request
}

// InboxHash is the digest stored in the inbox for the message id: SHA-256
// over type, idempotency key and the canonical business payload (the same
// canonical JSON used for the transaction payload hash). Transport metadata
// such as occurredAt is excluded.
func (m Message) InboxHash() string {
	var b bytes.Buffer
	b.WriteString(m.Type)
	b.WriteByte('\n')
	b.WriteString(m.IdempotencyKey)
	b.WriteByte('\n')
	b.Write(m.Request.CanonicalJSON())
	return wagering.HashBytes(b.Bytes())
}

// ParseMessage validates the envelope and builds the domain request with the
// same validation used by HTTP. Errors wrap ErrInvalidMessage (and the
// domain validation errors when applicable).
func ParseMessage(body []byte) (Message, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var env envelopeWire
	if err := dec.Decode(&env); err != nil {
		return Message{}, fmt.Errorf("%w: malformed JSON: %v", ErrInvalidMessage, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Message{}, fmt.Errorf("%w: trailing data", ErrInvalidMessage)
	}
	if env.MessageID == "" || len(env.MessageID) > 128 {
		return Message{}, fmt.Errorf("%w: messageId is required (max 128 chars)", ErrInvalidMessage)
	}
	if env.Type != MessageTypeWagerTransactionRequested {
		return Message{}, fmt.Errorf("%w: unsupported type %q", ErrInvalidMessage, env.Type)
	}
	occurred, err := time.Parse(time.RFC3339Nano, env.OccurredAt)
	if err != nil {
		return Message{}, fmt.Errorf("%w: occurredAt must be RFC 3339", ErrInvalidMessage)
	}
	if env.Data == nil {
		return Message{}, fmt.Errorf("%w: data is required", ErrInvalidMessage)
	}
	d := env.Data
	if err := wagering.ValidateIdempotencyKey(d.IdempotencyKey); err != nil {
		return Message{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	raw := wagering.RawRequest{
		ProviderID: d.ProviderID, ExternalTransactionID: d.ExternalTransactionID, PlayerID: d.PlayerID,
		WalletID: d.WalletID, RoundID: d.RoundID, GameID: d.GameID, Kind: d.Kind,
		ReferenceExternalTransactionID: d.ReferenceExternalTransactionID,
	}
	if d.Money != nil && d.Money.Amount != nil && d.Money.Currency != nil {
		raw.HasMoney, raw.Amount, raw.Currency = true, *d.Money.Amount, *d.Money.Currency
	}
	req, err := wagering.NewRequest(raw)
	if err != nil {
		return Message{}, fmt.Errorf("%w: %w", ErrInvalidMessage, err)
	}
	return Message{
		MessageID: env.MessageID, Type: env.Type, OccurredAt: occurred.UTC(),
		IdempotencyKey: d.IdempotencyKey, Request: req,
	}, nil
}
