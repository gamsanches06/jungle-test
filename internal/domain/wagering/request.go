package wagering

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
)

const (
	maxIdentifierLen     = 128
	maxIdempotencyKeyLen = 255
)

// Request is a validated external operation, independent of transport.
// HTTP and SQS both build a Request, so validation and hashing are shared.
type Request struct {
	providerID                     string
	externalTransactionID          string
	playerID                       uuid.UUID
	walletID                       uuid.UUID
	roundID                        string
	gameID                         string
	kind                           Kind
	money                          money.Money
	referenceExternalTransactionID string
}

// RawRequest holds the untrusted strings received from a transport.
type RawRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	HasMoney                       bool
	ReferenceExternalTransactionID string
}

// NewRequest validates the raw input. It never normalizes strings except
// UUIDs, which are canonicalized to lower case (see ARCHITECTURE.md).
func NewRequest(r RawRequest) (Request, error) {
	var errs []error
	check := func(field, v string) {
		if err := validateIdentifier(field, v); err != nil {
			errs = append(errs, err)
		}
	}
	check("providerId", r.ProviderID)
	check("externalTransactionId", r.ExternalTransactionID)
	check("roundId", r.RoundID)
	check("gameId", r.GameID)

	playerID, err := parseUUID("playerId", r.PlayerID)
	if err != nil {
		errs = append(errs, err)
	}
	walletID, err := parseUUID("walletId", r.WalletID)
	if err != nil {
		errs = append(errs, err)
	}
	kind, err := ParseExternalKind(r.Kind)
	if err != nil {
		errs = append(errs, err)
	}

	var m money.Money
	moneyOK := false
	if !r.HasMoney {
		errs = append(errs, &ValidationError{Field: "money", Reason: "required"})
	} else if m, err = money.ParseNonNegative(r.Amount, r.Currency); err != nil {
		errs = append(errs, &ValidationError{Field: "money", Reason: err.Error()})
	} else {
		moneyOK = true
	}
	if moneyOK && kind != "" {
		switch {
		case kind == KindLoss && !m.IsZero():
			errs = append(errs, &ValidationError{Field: "money.amount", Reason: `LOSS requires amount "0.00"`})
		case kind != KindLoss && !m.IsPositive():
			errs = append(errs, &ValidationError{Field: "money.amount", Reason: string(kind) + " requires an amount greater than zero"})
		}
	}

	if kind != "" {
		switch {
		case kind.RequiresReference() && r.ReferenceExternalTransactionID == "":
			errs = append(errs, &ValidationError{Field: "referenceExternalTransactionId", Reason: "required for " + string(kind)})
		case !kind.AcceptsReference() && r.ReferenceExternalTransactionID != "":
			errs = append(errs, &ValidationError{Field: "referenceExternalTransactionId", Reason: "not allowed for " + string(kind)})
		case r.ReferenceExternalTransactionID != "":
			check("referenceExternalTransactionId", r.ReferenceExternalTransactionID)
			if r.ReferenceExternalTransactionID == r.ExternalTransactionID {
				errs = append(errs, &ValidationError{Field: "referenceExternalTransactionId", Reason: "cannot reference itself"})
			}
		}
	}
	if len(errs) > 0 {
		return Request{}, errors.Join(errs...)
	}
	return Request{
		providerID:                     r.ProviderID,
		externalTransactionID:          r.ExternalTransactionID,
		playerID:                       playerID,
		walletID:                       walletID,
		roundID:                        r.RoundID,
		gameID:                         r.GameID,
		kind:                           kind,
		money:                          m,
		referenceExternalTransactionID: r.ReferenceExternalTransactionID,
	}, nil
}

func (r Request) ProviderID() string                     { return r.providerID }
func (r Request) ExternalTransactionID() string          { return r.externalTransactionID }
func (r Request) PlayerID() uuid.UUID                    { return r.playerID }
func (r Request) WalletID() uuid.UUID                    { return r.walletID }
func (r Request) RoundID() string                        { return r.roundID }
func (r Request) GameID() string                         { return r.gameID }
func (r Request) Kind() Kind                             { return r.kind }
func (r Request) Money() money.Money                     { return r.money }
func (r Request) ReferenceExternalTransactionID() string { return r.referenceExternalTransactionID }

// CanonicalJSON returns the canonical JSON of the business fields: object
// keys sorted lexicographically, no insignificant whitespace, no HTML
// escaping, UUIDs in lower case, money as decimal string with scale 2.
// referenceExternalTransactionId is omitted when absent. The idempotency key
// and transport metadata (headers, messageId, occurredAt, correlationId) are
// never part of it.
func (r Request) CanonicalJSON() []byte {
	fields := map[string]any{
		"providerId":            r.providerID,
		"externalTransactionId": r.externalTransactionID,
		"playerId":              r.playerID.String(),
		"walletId":              r.walletID.String(),
		"roundId":               r.roundID,
		"gameId":                r.gameID,
		"kind":                  string(r.kind),
		"money": map[string]any{
			"amount":   r.money.Amount(),
			"currency": r.money.Currency().Code(),
		},
	}
	if r.referenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = r.referenceExternalTransactionID
	}
	return canonicalJSON(fields)
}

// PayloadHash is "sha256:" + hex(SHA-256(CanonicalJSON())).
func (r Request) PayloadHash() string { return HashBytes(r.CanonicalJSON()) }

// HashBytes returns the "sha256:<hex>" digest used for payload hashes.
func HashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// canonicalJSON relies on encoding/json sorting map keys.
func canonicalJSON(v map[string]any) []byte {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Only strings and nested maps of strings are encoded; this cannot fail.
		panic(err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// ValidateIdempotencyKey checks the client supplied key. It is never
// replaced by a computed key.
func ValidateIdempotencyKey(k string) error {
	if k == "" {
		return &ValidationError{Field: "idempotencyKey", Reason: "required"}
	}
	if len(k) > maxIdempotencyKeyLen {
		return &ValidationError{Field: "idempotencyKey", Reason: "too long"}
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return &ValidationError{Field: "idempotencyKey", Reason: "must be visible ASCII without spaces"}
		}
	}
	return nil
}

func validateIdentifier(field, v string) error {
	if v == "" {
		return &ValidationError{Field: field, Reason: "required"}
	}
	if len(v) > maxIdentifierLen {
		return &ValidationError{Field: field, Reason: "too long"}
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == ':'
		if !ok {
			return &ValidationError{Field: field, Reason: "allowed characters are [A-Za-z0-9._:-]"}
		}
	}
	return nil
}

func parseUUID(field, v string) (uuid.UUID, error) {
	if v == "" {
		return uuid.Nil, &ValidationError{Field: field, Reason: "required"}
	}
	// Only the canonical 36-character hyphenated form is accepted.
	if len(v) != 36 {
		return uuid.Nil, &ValidationError{Field: field, Reason: "must be a UUID"}
	}
	id, err := uuid.Parse(v)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, &ValidationError{Field: field, Reason: "must be a non-nil UUID"}
	}
	return id, nil
}

// ParseID parses a canonical UUID path or body parameter.
func ParseID(field, v string) (uuid.UUID, error) { return parseUUID(field, v) }
