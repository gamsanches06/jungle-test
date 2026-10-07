// Package money implements an immutable monetary value object backed by
// int64 minor units (cents). It never uses floating point: parsing,
// arithmetic, comparison and serialization are done on integers and strings.
//
// Representation: amount is stored as int64 minor units with a fixed scale of
// two decimal places. The supported range is therefore
// [-92233720368547758.08, 92233720368547758.07]. Every operation that could
// exceed this range returns ErrOverflow instead of wrapping around.
package money

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Scale is the fixed number of decimal places of every supported currency.
const Scale = 2

const scaleFactor int64 = 100

var (
	// ErrInvalidAmount reports a malformed decimal string.
	ErrInvalidAmount = errors.New("money: invalid amount")
	// ErrInvalidScale reports an amount whose scale differs from Scale.
	ErrInvalidScale = errors.New("money: amount must have exactly two decimal places")
	// ErrNegativeAmount reports a negative amount where it is not allowed.
	ErrNegativeAmount = errors.New("money: negative amount not allowed")
	// ErrOverflow reports an int64 overflow during parsing or arithmetic.
	ErrOverflow = errors.New("money: amount overflows int64 minor units")
	// ErrCurrencyMismatch reports an operation between different currencies.
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	// ErrInvalidCurrency reports an unsupported or malformed ISO 4217 code.
	ErrInvalidCurrency = errors.New("money: invalid currency")
	// ErrUninitialized reports usage of the zero value of Money.
	ErrUninitialized = errors.New("money: uninitialized value")
)

// Currency is an ISO 4217 alphabetic code supported by this service.
type Currency struct{ code string }

// supportedCurrencies lists ISO 4217 codes whose minor unit exponent is 2.
// Only currencies with exponent 2 are accepted because the external contract
// uses a fixed scale of two decimal places.
var supportedCurrencies = map[string]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "MXN": {},
	"CAD": {}, "AUD": {}, "CHF": {}, "COP": {}, "PEN": {}, "UYU": {},
}

// ParseCurrency validates a currency code. Codes must already be upper case;
// no normalization is applied.
func ParseCurrency(code string) (Currency, error) {
	if len(code) != 3 {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	for _, r := range code {
		if r < 'A' || r > 'Z' {
			return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
		}
	}
	if _, ok := supportedCurrencies[code]; !ok {
		return Currency{}, fmt.Errorf("%w: unsupported %q", ErrInvalidCurrency, code)
	}
	return Currency{code: code}, nil
}

// MustCurrency is intended for constants in tests and wiring code only.
func MustCurrency(code string) Currency {
	c, err := ParseCurrency(code)
	if err != nil {
		panic(err)
	}
	return c
}

// Code returns the ISO 4217 code.
func (c Currency) Code() string { return c.code }

// IsZero reports whether the currency is uninitialized.
func (c Currency) IsZero() bool { return c.code == "" }

func (c Currency) String() string { return c.code }

// Money is an immutable amount in a currency. The zero value is invalid and
// is rejected by every operation.
type Money struct {
	minor    int64
	currency Currency
}

// Zero returns zero in the given currency.
func Zero(c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrInvalidCurrency
	}
	return Money{minor: 0, currency: c}, nil
}

// FromMinor builds Money from minor units (e.g. cents). Used for rehydration
// from persistence, where values are stored as BIGINT minor units.
func FromMinor(minor int64, c Currency) (Money, error) {
	if c.IsZero() {
		return Money{}, ErrInvalidCurrency
	}
	return Money{minor: minor, currency: c}, nil
}

// Parse parses a signed decimal string with exactly two decimal places, for
// example "25.00" or "-0.50". It rejects empty strings, NaN, Infinity,
// exponents, thousands separators, surrounding whitespace, explicit '+',
// leading zeros and any scale other than two. Nothing is rounded.
func Parse(amount, currency string) (Money, error) {
	c, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: c}, nil
}

// ParseNonNegative is the parser for external financial inputs: it applies
// the same rules as Parse and additionally rejects negative values
// (including "-0.00").
func ParseNonNegative(amount, currency string) (Money, error) {
	if strings.HasPrefix(amount, "-") {
		return Money{}, ErrNegativeAmount
	}
	return Parse(amount, currency)
}

func parseMinor(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	neg := false
	body := s
	if body[0] == '-' {
		neg = true
		body = body[1:]
	}
	// Only digits and at most one '.' are allowed: this rejects NaN,
	// Infinity, exponents, signs other than a leading '-', separators and
	// whitespace before any scale consideration.
	for i := 0; i < len(body); i++ {
		if (body[i] < '0' || body[i] > '9') && body[i] != '.' {
			return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
		}
	}
	if strings.Count(body, ".") > 1 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	dot := strings.IndexByte(body, '.')
	if dot < 0 {
		if body == "" {
			return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
		}
		return 0, fmt.Errorf("%w: %q", ErrInvalidScale, s)
	}
	intPart, fracPart := body[:dot], body[dot+1:]
	if intPart == "" || !allDigits(intPart) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if !allDigits(fracPart) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if len(fracPart) != Scale {
		return 0, fmt.Errorf("%w: %q", ErrInvalidScale, s)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return 0, fmt.Errorf("%w: leading zeros in %q", ErrInvalidAmount, s)
	}
	// Accumulate as a negative number so that math.MinInt64 is representable.
	var acc int64
	for _, ch := range intPart + fracPart {
		d := int64(ch - '0')
		if acc < (math.MinInt64+d)/10 {
			return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
		}
		acc = acc*10 - d
	}
	if neg {
		return acc, nil
	}
	if acc == math.MinInt64 {
		return 0, fmt.Errorf("%w: %q", ErrOverflow, s)
	}
	return -acc, nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (m Money) valid() error {
	if m.currency.IsZero() {
		return ErrUninitialized
	}
	return nil
}

func (m Money) compatible(o Money) error {
	if err := m.valid(); err != nil {
		return err
	}
	if err := o.valid(); err != nil {
		return err
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Validate reports whether m is an initialized value.
func (m Money) Validate() error { return m.valid() }

// Currency returns the currency.
func (m Money) Currency() Currency { return m.currency }

// Minor returns the amount in minor units.
func (m Money) Minor() int64 { return m.minor }

// IsZero reports whether the amount is zero.
func (m Money) IsZero() bool { return m.minor == 0 }

// IsPositive reports whether the amount is greater than zero.
func (m Money) IsPositive() bool { return m.minor > 0 }

// IsNegative reports whether the amount is lower than zero.
func (m Money) IsNegative() bool { return m.minor < 0 }

// Add returns m + o.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub returns m - o.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) || (o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

// Neg returns -m.
func (m Money) Neg() (Money, error) {
	if err := m.valid(); err != nil {
		return Money{}, err
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp returns -1, 0 or 1 comparing m with o. Currencies must match.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

// Equal reports whether both values have the same amount and currency.
func (m Money) Equal(o Money) bool {
	return m.currency == o.currency && m.minor == o.minor
}

// Amount returns the canonical decimal string, e.g. "25.00" or "-0.50".
func (m Money) Amount() string {
	v := m.minor
	neg := v < 0
	var u uint64
	if neg {
		u = uint64(-(v + 1)) + 1 // safe for MinInt64
	} else {
		u = uint64(v)
	}
	intPart := u / uint64(scaleFactor)
	frac := u % uint64(scaleFactor)
	s := strconv.FormatUint(intPart, 10) + "." + fmt.Sprintf("%02d", frac)
	if neg {
		return "-" + s
	}
	return s
}

func (m Money) String() string { return m.Amount() + " " + m.currency.code }

type wire struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON renders {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.valid(); err != nil {
		return nil, err
	}
	return json.Marshal(wire{Amount: m.Amount(), Currency: m.currency.code})
}

// UnmarshalJSON accepts {"amount":"25.00","currency":"BRL"} where amount is
// a JSON string. JSON numbers are rejected so no float is ever involved.
// Signed values are accepted here; external inputs must use
// ParseNonNegative through their own DTOs.
func (m *Money) UnmarshalJSON(b []byte) error {
	var w struct {
		Amount   *string `json:"amount"`
		Currency *string `json:"currency"`
	}
	if err := json.Unmarshal(b, &w); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	if w.Amount == nil || w.Currency == nil {
		return fmt.Errorf("%w: amount and currency are required", ErrInvalidAmount)
	}
	v, err := Parse(*w.Amount, *w.Currency)
	if err != nil {
		return err
	}
	*m = v
	return nil
}
