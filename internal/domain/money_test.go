package domain

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func TestParseMoney(t *testing.T) {
	valid := map[string]string{
		"0": "0.00", "0.00": "0.00", "0.5": "0.50", "25": "25.00", "25.5": "25.50", "25.50": "25.50",
		"1000.00": "1000.00", "0.01": "0.01",
		"92233720368547758.07": "92233720368547758.07",
	}
	for in, want := range valid {
		m, err := ParseMoney(in, "BRL")
		if err != nil {
			t.Errorf("ParseMoney(%q): %v", in, err)
			continue
		}
		if m.Amount() != want {
			t.Errorf("ParseMoney(%q).Amount() = %q, want %q", in, m.Amount(), want)
		}
	}

	invalid := []struct {
		in   string
		want error
	}{
		{"", ErrInvalidAmount}, {" ", ErrInvalidAmount}, {" 1.00", ErrInvalidAmount}, {"1.00 ", ErrInvalidAmount},
		{"NaN", ErrInvalidAmount}, {"Infinity", ErrInvalidAmount}, {"-Infinity", ErrInvalidAmount},
		{"1e3", ErrInvalidAmount}, {"1E3", ErrInvalidAmount}, {"1.0e2", ErrInvalidAmount},
		{"+1.00", ErrInvalidAmount}, {".5", ErrInvalidAmount}, {"1.", ErrInvalidAmount}, {"1,00", ErrInvalidAmount},
		{"01.00", ErrInvalidAmount}, {"00", ErrInvalidAmount}, {"0x10", ErrInvalidAmount}, {"1_000", ErrInvalidAmount},
		{"١٢٣", ErrInvalidAmount}, {"1.2.3", ErrInvalidAmount}, {"--1", ErrInvalidAmount}, {"-", ErrInvalidAmount},
		{"1.001", ErrAmountScale}, {"0.999", ErrAmountScale}, {"25.000", ErrAmountScale},
		{"-1.00", ErrAmountNegative}, {"-0.01", ErrAmountNegative}, {"-0", ErrAmountNegative}, {"-1.234", ErrAmountNegative},
		{"92233720368547758.08", ErrAmountOverflow}, {"92233720368547759", ErrAmountOverflow},
		{"99999999999999999999999", ErrAmountOverflow}, {"100000000000000000", ErrAmountOverflow},
	}
	for _, c := range invalid {
		_, err := ParseMoney(c.in, "BRL")
		if !errors.Is(err, c.want) {
			t.Errorf("ParseMoney(%q) = %v, want %v", c.in, err, c.want)
		}
	}
}

func TestParseCurrency(t *testing.T) {
	for _, c := range []string{"BRL", "USD", "EUR"} {
		if _, err := ParseCurrency(c); err != nil {
			t.Errorf("ParseCurrency(%q): %v", c, err)
		}
	}
	for _, c := range []string{"", "BR", "BRLL", "brl", "B1L", "R$ ", "BRL "} {
		if _, err := ParseCurrency(c); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("ParseCurrency(%q) = %v, want ErrInvalidCurrency", c, err)
		}
	}
	if _, err := ParseMoney("1.00", "xyz"); !errors.Is(err, ErrInvalidCurrency) {
		t.Errorf("lowercase currency accepted: %v", err)
	}
}

func mustMoney(t testing.TB, amount, cur string) Money {
	t.Helper()
	m, err := ParseMoney(amount, cur)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestMoneyArithmetic(t *testing.T) {
	a, b := mustMoney(t, "100.00", "BRL"), mustMoney(t, "80.00", "BRL")
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
		t.Errorf("Cmp = %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Errorf("Cmp = %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Errorf("Cmp = %d", c)
	}
	// Soma de centavos não acumula erro de ponto flutuante.
	acc, _ := Zero("BRL")
	cent := mustMoney(t, "0.10", "BRL")
	for i := 0; i < 10; i++ {
		acc, _ = acc.Add(cent)
	}
	if acc.Amount() != "1.00" {
		t.Errorf("10 x 0.10 = %s", acc.Amount())
	}
}

func TestMoneyCurrencyMismatch(t *testing.T) {
	brl, usd := mustMoney(t, "1.00", "BRL"), mustMoney(t, "1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Cmp: %v", err)
	}
	if brl.Equal(usd) {
		t.Error("Equal across currencies")
	}
}

func TestMoneyUninitialized(t *testing.T) {
	var zero Money
	valid := mustMoney(t, "1.00", "BRL")
	if zero.IsInitialized() || zero.IsZero() {
		t.Error("zero struct must not look like a monetary zero")
	}
	if z, _ := Zero("BRL"); !z.IsInitialized() || !z.IsZero() {
		t.Error("Zero(BRL) must be an initialized zero")
	}
	if _, err := zero.Add(valid); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("Add: %v", err)
	}
	if _, err := valid.Add(zero); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("Add: %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("Neg: %v", err)
	}
	if _, err := zero.Cmp(valid); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("Cmp: %v", err)
	}
	if _, err := json.Marshal(zero); !errors.Is(err, ErrUninitializedMoney) {
		t.Errorf("Marshal: %v", err)
	}
	if zero.Equal(zero) {
		t.Error("uninitialized values must not be equal")
	}
}

func TestMoneyOverflow(t *testing.T) {
	max, err := FromMinor(math.MaxInt64, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	one := mustMoney(t, "0.01", "BRL")
	if _, err := max.Add(one); !errors.Is(err, ErrAmountOverflow) {
		t.Errorf("max+1: %v", err)
	}
	negMax, _ := max.Neg()
	if _, err := negMax.Sub(one); !errors.Is(err, ErrAmountOverflow) {
		t.Errorf("-max-1: %v", err)
	}
	if _, err := max.Sub(negMax); !errors.Is(err, ErrAmountOverflow) {
		t.Errorf("max-(-max): %v", err)
	}
	if got, err := max.Sub(one); err != nil || got.Minor() != math.MaxInt64-1 {
		t.Errorf("max-1: %v %v", got, err)
	}
	if _, err := FromMinor(math.MinInt64, "BRL"); !errors.Is(err, ErrAmountOverflow) {
		t.Errorf("MinInt64 accepted: %v", err)
	}
	if got := negMax.Amount(); got != "-92233720368547758.07" {
		t.Errorf("Amount() = %s", got)
	}
}

func TestMoneyJSON(t *testing.T) {
	m := mustMoney(t, "25", "BRL")
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("Marshal = %s, %v", b, err)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil || back != m {
		t.Fatalf("round trip: %v %v", back, err)
	}
	bad := []string{
		`{"amount":25.00,"currency":"BRL"}`,   // número JSON
		`{"amount":"-1.00","currency":"BRL"}`, // negativo
		`{"amount":"1.001","currency":"BRL"}`,
		`{"amount":"1.00","currency":"brl"}`,
		`{"amount":"1.00"}`,
		`{"amount":"1.00","currency":"BRL","extra":1}`,
		`"25.00"`, `null`, `[]`,
	}
	for _, in := range bad {
		var m Money
		if err := json.Unmarshal([]byte(in), &m); err == nil {
			t.Errorf("Unmarshal(%s) accepted: %v", in, m)
		}
	}
}

func FuzzParseMoney(f *testing.F) {
	for _, s := range []string{"0", "1.00", "-1", "1e5", "99999999999999999999", "0.001", "", "NaN"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		m, err := ParseMoney(s, "BRL")
		if err != nil {
			return
		}
		// Tudo que é aceito é canônico e reversível.
		if m.IsNegative() {
			t.Fatalf("negative accepted: %q", s)
		}
		back, err := ParseMoney(m.Amount(), "BRL")
		if err != nil || back != m {
			t.Fatalf("round trip %q -> %q: %v", s, m.Amount(), err)
		}
	})
}
