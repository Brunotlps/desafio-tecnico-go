package app

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"desafio-tecnico-go/internal/domain"
)

func TestCursorRoundTripAndRejections(t *testing.T) {
	w, other := uuid.New(), uuid.New()
	good := encodeCursor(w, 42)
	if seq, err := decodeCursor(good, w); err != nil || seq != 42 {
		t.Fatalf("round trip: %d %v", seq, err)
	}
	enc := func(s string) string { return base64.RawURLEncoding.EncodeToString([]byte(s)) }
	cases := map[string]string{
		"another wallet":     encodeCursor(other, 42),
		"not base64":         "***",
		"padded base64":      good + "==",
		"empty json":         enc(``),
		"not json":           enc(`not json`),
		"wrong version":      enc(`{"v":2,"w":"` + w.String() + `","s":42}`),
		"zero position":      enc(`{"v":1,"w":"` + w.String() + `","s":0}`),
		"negative position":  enc(`{"v":1,"w":"` + w.String() + `","s":-3}`),
		"unknown field":      enc(`{"v":1,"w":"` + w.String() + `","s":42,"x":1}`),
		"trailing data":      enc(`{"v":1,"w":"` + w.String() + `","s":42}{}`),
		"non canonical":      enc(`{"s":42,"v":1,"w":"` + w.String() + `"}`),
		"whitespace":         enc(` {"v":1,"w":"` + w.String() + `","s":42}`),
		"uppercase wallet":   enc(`{"v":1,"w":"` + strings.ToUpper(w.String()) + `","s":42}`),
		"position as string": enc(`{"v":1,"w":"` + w.String() + `","s":"42"}`),
		"too long":           strings.Repeat("A", maxCursorLength+1),
	}
	for name, raw := range cases {
		if _, err := decodeCursor(raw, w); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("%s: want ErrInvalidCursor, got %v", name, err)
		}
	}
}

type fakeLedger struct{ snap ReconciliationSnapshot }

func (f fakeLedger) LedgerPage(context.Context, uuid.UUID, int64, int) ([]LedgerRow, error) {
	return nil, nil
}
func (f fakeLedger) ReconciliationSnapshot(context.Context, uuid.UUID) (ReconciliationSnapshot, error) {
	return f.snap, nil
}

func reconcileWith(t *testing.T, stored int64, net *big.Int, foreign int64) (ReconciliationResult, error, int) {
	t.Helper()
	wallet, err := domain.RehydrateWallet(domain.WalletState{
		ID: uuid.New(), PlayerID: uuid.New(), Balance: mustMoney(t, stored), Version: 1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	divergences := 0
	s := NewService(nil, Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithLedgerReader(fakeLedger{ReconciliationSnapshot{Wallet: wallet, Entries: 3, ForeignCurrencyEntries: foreign, NetMinor: net}}),
		WithReconciliationObserver(func(ReconciliationResult) { divergences++ }))
	res, err := s.Reconcile(context.Background(), wallet.ID())
	return res, err, divergences
}

func mustMoney(t *testing.T, minor int64) domain.Money {
	t.Helper()
	m, err := domain.FromMinor(minor, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// A soma é exata: o saldo calculado e a diferença só são Money se couberem; nada transborda em silêncio.
func TestReconcileUsesExactArithmeticAndNeverInventsAResult(t *testing.T) {
	huge := new(big.Int).Lsh(big.NewInt(1), 70)
	t.Run("equal at the limit is consistent", func(t *testing.T) {
		res, err, div := reconcileWith(t, math.MaxInt64, big.NewInt(math.MaxInt64), 0)
		if err != nil || !res.Consistent || !res.Difference.IsZero() || res.CheckedEntries != 3 || div != 0 {
			t.Fatalf("%+v %v div=%d", res, err, div)
		}
	})
	t.Run("negative difference", func(t *testing.T) {
		res, err, div := reconcileWith(t, 1000, big.NewInt(1500), 0)
		if err != nil || res.Consistent || res.Difference.Minor() != -500 || res.Difference.Amount() != "-5.00" || div != 1 {
			t.Fatalf("%+v %v div=%d", res, err, div)
		}
	})
	t.Run("calculated balance outside Money is a failure, not a divergence", func(t *testing.T) {
		for name, net := range map[string]*big.Int{"2^70": huge, "-2^70": new(big.Int).Neg(huge), "MinInt64": big.NewInt(math.MinInt64)} {
			res, err, div := reconcileWith(t, 0, net, 0)
			if !errors.Is(err, ErrReconciliationFailed) || res.Consistent || div != 0 {
				t.Errorf("%s: %+v %v div=%d", name, res, err, div)
			}
		}
	})
	t.Run("difference outside Money is a failure", func(t *testing.T) {
		_, err, div := reconcileWith(t, math.MaxInt64, big.NewInt(-math.MaxInt64), 0)
		if !errors.Is(err, ErrReconciliationFailed) || div != 0 {
			t.Fatalf("%v div=%d", err, div)
		}
	})
	t.Run("foreign currency entries are a failure", func(t *testing.T) {
		_, err, _ := reconcileWith(t, 0, big.NewInt(0), 1)
		if !errors.Is(err, ErrReconciliationFailed) {
			t.Fatal(err)
		}
	})
	t.Run("empty ledger of a zero wallet", func(t *testing.T) {
		res, err, _ := reconcileWith(t, 0, big.NewInt(0), 0)
		if err != nil || !res.Consistent || !res.Calculated.IsZero() {
			t.Fatalf("%+v %v", res, err)
		}
	})
}
