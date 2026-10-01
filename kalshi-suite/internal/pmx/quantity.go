package pmx

import (
	"fmt"

	pmxv1 "github.com/kalshi-suite/kalshi-suite/internal/pmxgen/polymarket/v1"
)

// InstrumentQuantityScale returns the venue divisor for raw integer quantities. Whole-contract
// instruments omit fractional_qty_scale, which is equivalent to a divisor of one; fractional
// instruments explicitly publish values such as 100 (raw quantity 1 = 0.01 contracts).
func InstrumentQuantityScale(in *pmxv1.Instrument) int64 {
	if in != nil && in.GetFractionalQtyScale() > 0 {
		return in.GetFractionalQtyScale()
	}
	return 1
}

// DecimalQuantity normalizes an institutional raw integer quantity using its instrument's scale.
// Callers must not guess a non-positive scale: use InstrumentQuantityScale for an Instrument.
func DecimalQuantity(raw, scale int64) (float64, error) {
	if scale <= 0 {
		return 0, fmt.Errorf("polymarket US quantity scale must be positive, got %d", scale)
	}
	return float64(raw) / float64(scale), nil
}

// InstrumentMinimumTradeQty returns the documented minimumTradeQty in decimal contracts, using
// the same fractionalQtyScale as positions, orders, and market-data quantities.
func InstrumentMinimumTradeQty(in *pmxv1.Instrument) float64 {
	if in == nil {
		return 0
	}
	qty, _ := DecimalQuantity(in.GetMinimumTradeQty(), InstrumentQuantityScale(in))
	return qty
}
