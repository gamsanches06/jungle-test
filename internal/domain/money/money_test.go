package money_test

import (
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/gamsanches06/jungle-test/internal/domain/money"
)

func mustParse(t *testing.T, amount, currency string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, currency)
	if err != nil {
		t.Fatalf("Parse(%q, %q): %v", amount, currency, err)
	}
	return m
}

func TestParseAcceptsCanonicalTwoDecimalStrings(t *testing.T) {
	cases := map[string]int64{
		"0.00":                  0,
		"0.01":                  1,
		"25.00":                 2500,
		"1000.00":               100000,
		"-0.50":                 -50,
		"92233720368547758.07":  math.MaxInt64,
		"-92233720368547758.08": math.MinInt64,
		"12345678901234.56":     1234567890123456,
		"-12345678901234.56":    -1234567890123456,
		"9999999999999999.99":   999999999999999999,
	}
	for in, want := range cases {
		m := mustParse(t, in, "BRL")
		if m.Minor() != want {
			t.Errorf("Parse(%q).Minor() = %d, want %d", in, m.Minor(), want)
		}
		if got := m.Amount(); got != in {
			t.Errorf("Parse(%q).Amount() = %q (round trip)", in, got)
		}
	}
}

func TestParseRejectsInvalidInput(t *testing.T) {
	cases := []struct {
		in   string
		want error
	}{
		{"", money.ErrInvalidAmount},
		{"NaN", money.ErrInvalidAmount},
		{"nan", money.ErrInvalidAmount},
		{"Infinity", money.ErrInvalidAmount},
		{"-Infinity", money.ErrInvalidAmount},
		{"Inf.00", money.ErrInvalidAmount},
		{"1e3", money.ErrInvalidAmount},
		{"1.5e2", money.ErrInvalidAmount},
		{"1E+02", money.ErrInvalidAmount},
		{"25", money.ErrInvalidScale},
		{"25.0", money.ErrInvalidScale},
		{"25.000", money.ErrInvalidScale},
		{"25.001", money.ErrInvalidScale},
		{".50", money.ErrInvalidAmount},
		{"25.", money.ErrInvalidAmount},
		{"+25.00", money.ErrInvalidAmount},
		{" 25.00", money.ErrInvalidAmount},
		{"25.00 ", money.ErrInvalidAmount},
		{"25,00", money.ErrInvalidAmount},
		{"1,000.00", money.ErrInvalidAmount},
		{"025.00", money.ErrInvalidAmount},
		{"--1.00", money.ErrInvalidAmount},
		{"0x10.00", money.ErrInvalidAmount},
		{"92233720368547758.08", money.ErrOverflow},
		{"-92233720368547758.09", money.ErrOverflow},
		{"100000000000000000000.00", money.ErrOverflow},
	}
	for _, c := range cases {
		_, err := money.Parse(c.in, "BRL")
		if !errors.Is(err, c.want) {
			t.Errorf("Parse(%q) error = %v, want %v", c.in, err, c.want)
		}
	}
}

func TestParseNonNegativeRejectsNegatives(t *testing.T) {
	for _, in := range []string{"-0.01", "-0.00", "-25.00"} {
		if _, err := money.ParseNonNegative(in, "BRL"); !errors.Is(err, money.ErrNegativeAmount) {
			t.Errorf("ParseNonNegative(%q) = %v, want ErrNegativeAmount", in, err)
		}
	}
	if m, err := money.ParseNonNegative("0.00", "BRL"); err != nil || !m.IsZero() {
		t.Fatalf("ParseNonNegative(0.00) = %v, %v", m, err)
	}
}

func TestCurrencyValidation(t *testing.T) {
	for _, c := range []string{"", "BR", "BRLL", "brl", "Brl", "XXX", "JPY", "B1L"} {
		if _, err := money.Parse("1.00", c); !errors.Is(err, money.ErrInvalidCurrency) {
			t.Errorf("Parse with currency %q = %v, want ErrInvalidCurrency", c, err)
		}
	}
	if _, err := money.Zero(money.Currency{}); !errors.Is(err, money.ErrInvalidCurrency) {
		t.Errorf("Zero(uninitialized currency) = %v", err)
	}
	z, err := money.Zero(money.MustCurrency("USD"))
	if err != nil || !z.IsZero() || z.Currency().Code() != "USD" {
		t.Fatalf("Zero(USD) = %v, %v", z, err)
	}
}

func TestArithmetic(t *testing.T) {
	a, b := mustParse(t, "100.00", "BRL"), mustParse(t, "80.00", "BRL")
	sum, err := a.Add(b)
	if err != nil || sum.Amount() != "180.00" {
		t.Fatalf("Add = %v, %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.Amount() != "-20.00" || !diff.IsNegative() {
		t.Fatalf("Sub = %v, %v", diff, err)
	}
	neg, err := a.Neg()
	if err != nil || neg.Amount() != "-100.00" {
		t.Fatalf("Neg = %v, %v", neg, err)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Errorf("Cmp(100, 80) = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Errorf("Cmp(80, 100) = %d", c)
	}
	if c, _ := a.Cmp(mustParse(t, "100.00", "BRL")); c != 0 {
		t.Errorf("Cmp(100, 100) = %d", c)
	}
	if a.Amount() != "100.00" || b.Amount() != "80.00" {
		t.Fatalf("operands mutated: %v %v", a, b)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl, usd := mustParse(t, "1.00", "BRL"), mustParse(t, "1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Add mismatch = %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Sub mismatch = %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, money.ErrCurrencyMismatch) {
		t.Errorf("Cmp mismatch = %v", err)
	}
	if brl.Equal(usd) {
		t.Error("Equal across currencies")
	}
}

func TestOverflow(t *testing.T) {
	max := mustParse(t, "92233720368547758.07", "BRL")
	min := mustParse(t, "-92233720368547758.08", "BRL")
	one := mustParse(t, "0.01", "BRL")
	if _, err := max.Add(one); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("max+0.01 = %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("min-0.01 = %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("-min = %v", err)
	}
	negOne, _ := one.Neg()
	if _, err := max.Sub(negOne); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("max-(-0.01) = %v", err)
	}
	if _, err := min.Add(negOne); !errors.Is(err, money.ErrOverflow) {
		t.Errorf("min+(-0.01) = %v", err)
	}
	if v, err := max.Neg(); err != nil || v.Amount() != "-92233720368547758.07" {
		t.Errorf("-max = %v, %v", v, err)
	}
}

func TestUninitializedValuesAreRejected(t *testing.T) {
	var zero money.Money
	one := mustParse(t, "1.00", "BRL")
	if err := zero.Validate(); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Validate = %v", err)
	}
	if _, err := zero.Add(one); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Add = %v", err)
	}
	if _, err := one.Sub(zero); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Sub = %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("Neg = %v", err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Error("Marshal of uninitialized money succeeded")
	}
}

func TestJSON(t *testing.T) {
	m := mustParse(t, "25.00", "BRL")
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("Marshal = %s, %v", b, err)
	}
	var back money.Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("Unmarshal = %v, %v", back, err)
	}
	for _, in := range []string{
		`{"amount":25.00,"currency":"BRL"}`, // JSON number: never parsed as float
		`{"amount":"25","currency":"BRL"}`,
		`{"amount":"25.00"}`,
		`{"currency":"BRL"}`,
		`{"amount":"1e2","currency":"BRL"}`,
		`{"amount":"25.00","currency":"brl"}`,
	} {
		var v money.Money
		if err := json.Unmarshal([]byte(in), &v); err == nil {
			t.Errorf("Unmarshal(%s) accepted", in)
		}
	}
}

func TestFromMinorPreservesExactValue(t *testing.T) {
	m, err := money.FromMinor(123456789, money.MustCurrency("BRL"))
	if err != nil || m.Amount() != "1234567.89" {
		t.Fatalf("FromMinor = %v, %v", m, err)
	}
	if _, err := money.FromMinor(1, money.Currency{}); !errors.Is(err, money.ErrInvalidCurrency) {
		t.Fatalf("FromMinor without currency = %v", err)
	}
}
