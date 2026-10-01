package kalshi

import (
	"math"
	"testing"
)

func TestR154BalanceSeparatesPositionValueFromAccountNAV(t *testing.T) {
	positionsCents := int64(584)
	b := Balance{Balance: 40331, PortfolioValue: &positionsCents}

	if positions, ok := b.PortfolioValueUSD(); !ok || math.Abs(positions-5.84) > 1e-9 {
		t.Fatalf("positions value = %.2f ok=%v, want $5.84", positions, ok)
	}
	if nav, ok := b.AccountNAVUSD(); !ok || math.Abs(nav-409.15) > 1e-9 {
		t.Fatalf("account NAV = %.2f ok=%v, want $403.31 cash + $5.84 positions = $409.15", nav, ok)
	}
}

func TestR154AccountNAVRequiresPortfolioValuePresence(t *testing.T) {
	b := Balance{Balance: 40331}
	if nav, ok := b.AccountNAVUSD(); ok || nav != 0 {
		t.Fatalf("missing portfolio_value produced account NAV %.2f ok=%v; want fail closed", nav, ok)
	}
}
