package pmx

import (
	"testing"

	pmxv1 "github.com/kalshi-suite/kalshi-suite/internal/pmxgen/polymarket/v1"
)

func TestInstrumentQuantityNormalization(t *testing.T) {
	fractional := &pmxv1.Instrument{FractionalQtyScale: 100, MinimumTradeQty: 1}
	if got := InstrumentQuantityScale(fractional); got != 100 {
		t.Fatalf("scale=%d, want 100", got)
	}
	if got := InstrumentMinimumTradeQty(fractional); got != 0.01 {
		t.Fatalf("minimum=%v, want 0.01", got)
	}
	if got, err := DecimalQuantity(125, InstrumentQuantityScale(fractional)); err != nil || got != 1.25 {
		t.Fatalf("quantity=%v err=%v, want 1.25", got, err)
	}

	whole := &pmxv1.Instrument{MinimumTradeQty: 2}
	if got := InstrumentQuantityScale(whole); got != 1 {
		t.Fatalf("omitted scale=%d, want whole-contract divisor 1", got)
	}
	if got := InstrumentMinimumTradeQty(whole); got != 2 {
		t.Fatalf("whole minimum=%v, want 2", got)
	}
}

func TestDecimalQuantityRejectsInvalidScale(t *testing.T) {
	if _, err := DecimalQuantity(1, 0); err == nil {
		t.Fatal("non-positive scale must fail closed")
	}
}
