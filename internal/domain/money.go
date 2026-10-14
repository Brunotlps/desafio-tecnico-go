package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Scale é o número fixo de casas decimais de Money.
const Scale = 2

// Currency é um código de três letras maiúsculas no formato ISO 4217. O domínio
// valida o formato; o conjunto de moedas aceitas é política da aplicação.
type Currency string

// ParseCurrency valida o formato do código de moeda.
func ParseCurrency(code string) (Currency, error) {
	if len(code) != 3 {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	for i := 0; i < 3; i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
		}
	}
	return Currency(code), nil
}

func (c Currency) String() string { return string(c) }

// Money é um value object imutável: valor em unidades mínimas (centavos, int64)
// mais a moeda. O valor zero da struct (Money{}) representa "não inicializado"
// e é rejeitado por todas as operações; um zero monetário válido é criado com
// Zero(currency). Faixa: ±9.223.372.036.854.775.807 centavos, isto é, até
// 92.233.720.368.547.758,07 na unidade principal.
type Money struct {
	minor    int64
	currency Currency
}

// Zero devolve o zero monetário válido de uma moeda.
func Zero(currency Currency) (Money, error) {
	return FromMinor(0, currency)
}

// FromMinor cria um Money a partir de unidades mínimas. Aceita negativos, que
// só são legítimos em diferenças e cálculos internos; entradas externas devem
// usar ParseMoney.
func FromMinor(minor int64, currency Currency) (Money, error) {
	if _, err := ParseCurrency(string(currency)); err != nil {
		return Money{}, err
	}
	if minor == math.MinInt64 { // simetria: Neg(MinInt64) transbordaria
		return Money{}, fmt.Errorf("%w: %d", ErrAmountOverflow, minor)
	}
	return Money{minor: minor, currency: currency}, nil
}

// ParseMoney interpreta uma string decimal diretamente, sem passar por ponto
// flutuante. Gramática aceita: 0 | [1-9][0-9]* com fração opcional de 1 ou 2
// dígitos ("25", "25.5" e "25.50" são equivalentes e normalizam para
// "25.00"/"25.50"/"25.50"). Rejeita vazio, sinais, espaços, NaN, Infinity,
// notação científica, zeros à esquerda, separador sem dígitos, mais de duas
// casas decimais e negativos. Nunca arredonda.
func ParseMoney(amount string, currencyCode string) (Money, error) {
	cur, err := ParseCurrency(currencyCode)
	if err != nil {
		return Money{}, err
	}
	minor, err := parseMinor(amount)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur}, nil
}

func parseMinor(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("%w: empty", ErrInvalidAmount)
	}
	if s[0] == '-' {
		if _, err := parseMinor(s[1:]); err == nil || isScaleErr(err) {
			return 0, fmt.Errorf("%w: %q", ErrAmountNegative, s)
		}
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	if !allDigits(intPart) || (len(intPart) > 1 && intPart[0] == '0') {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if hasDot && (fracPart == "" || !allDigits(fracPart)) {
		return 0, fmt.Errorf("%w: %q", ErrInvalidAmount, s)
	}
	if len(fracPart) > Scale {
		return 0, fmt.Errorf("%w: %q", ErrAmountScale, s)
	}
	for len(fracPart) < Scale {
		fracPart += "0"
	}
	// Um int64 comporta no máximo 19 dígitos; mais que isso transborda sem
	// precisar converter.
	if len(intPart)+Scale > 19 {
		return 0, fmt.Errorf("%w: %q", ErrAmountOverflow, s)
	}
	v, err := strconv.ParseInt(intPart+fracPart, 10, 64)
	if err != nil || v == math.MinInt64 {
		return 0, fmt.Errorf("%w: %q", ErrAmountOverflow, s)
	}
	return v, nil
}

func isScaleErr(err error) bool { return errors.Is(err, ErrAmountScale) }

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

// IsInitialized informa se o valor foi criado por um construtor.
func (m Money) IsInitialized() bool { return m.currency != "" }

func (m Money) Currency() Currency { return m.currency }

// Minor devolve o valor em unidades mínimas.
func (m Money) Minor() int64 { return m.minor }

func (m Money) IsZero() bool     { return m.IsInitialized() && m.minor == 0 }
func (m Money) IsPositive() bool { return m.IsInitialized() && m.minor > 0 }
func (m Money) IsNegative() bool { return m.IsInitialized() && m.minor < 0 }

// Amount devolve o valor como string decimal de escala fixa, p.ex. "25.00".
func (m Money) Amount() string {
	neg := m.minor < 0
	u := uint64(m.minor)
	if neg {
		u = uint64(-m.minor)
	}
	s := fmt.Sprintf("%d.%02d", u/100, u%100)
	if neg {
		return "-" + s
	}
	return s
}

func (m Money) String() string {
	if !m.IsInitialized() {
		return "<uninitialized>"
	}
	return m.Amount() + " " + string(m.currency)
}

func (m Money) compatible(o Money) error {
	if !m.IsInitialized() || !o.IsInitialized() {
		return ErrUninitializedMoney
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

// Add soma dois valores da mesma moeda, detectando overflow.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor < -math.MaxInt64-o.minor) {
		return Money{}, ErrAmountOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub subtrai dois valores da mesma moeda; o resultado pode ser negativo.
func (m Money) Sub(o Money) (Money, error) {
	neg, err := o.Neg()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

// Neg inverte o sinal.
func (m Money) Neg() (Money, error) {
	if !m.IsInitialized() {
		return Money{}, ErrUninitializedMoney
	}
	// FromMinor impede MinInt64, portanto a negação nunca transborda; a
	// verificação protege valores construídos por outros caminhos.
	if m.minor == math.MinInt64 {
		return Money{}, ErrAmountOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp compara dois valores da mesma moeda: -1, 0 ou 1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

// Equal informa igualdade de valor e moeda; valores não inicializados nunca
// são iguais a nada.
func (m Money) Equal(o Money) bool {
	return m.IsInitialized() && o.IsInitialized() && m == o
}

type moneyJSON struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

// MarshalJSON produz {"amount":"25.00","currency":"BRL"}.
func (m Money) MarshalJSON() ([]byte, error) {
	if !m.IsInitialized() {
		return nil, ErrUninitializedMoney
	}
	return json.Marshal(moneyJSON{Amount: m.Amount(), Currency: string(m.currency)})
}

// UnmarshalJSON é estrito: amount precisa ser string JSON (números são
// recusados), campos desconhecidos são recusados e valem as regras de
// ParseMoney, inclusive a recusa de negativos.
func (m *Money) UnmarshalJSON(data []byte) error {
	var raw moneyJSON
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	parsed, err := ParseMoney(raw.Amount, raw.Currency)
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
