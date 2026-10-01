package signal

import (
	"math"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/ev"
)

func almost(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestNoArbWhenSumAtLeastOne(t *testing.T) {
	if ArbProfit(ev.General, 0.55, 0.50) != 0 {
		t.Error("expected no arb when yes+no >= 1")
	}
}

func TestTinyGapEatenByFees(t *testing.T) {
	// 0.48 + 0.49 = 0.97; two ~2c fees wipe out the 3c gap.
	if ArbProfit(ev.General, 0.48, 0.49) > 0 {
		t.Error("a 3c gap should not beat the fees")
	}
}

func TestRealArbIsPositive(t *testing.T) {
	// 0.40 + 0.50 = 0.90; fees ~0.04; ~6c locked profit per pair.
	if p := ArbProfit(ev.General, 0.40, 0.50); p <= 0 {
		t.Fatalf("expected positive arb, got %.4f", p)
	}
}

func TestMoveSkipsSettledMarkets(t *testing.T) {
	if MoveCents(0.99, 0.50) != 0 {
		t.Error("settled (>=98c) market should report no move")
	}
	if !almost(MoveCents(0.45, 0.40), 5) {
		t.Error("expected +5c move")
	}
	if !almost(MoveCents(0.30, 0.44), -14) {
		t.Error("expected -14c move")
	}
}

func TestSpread(t *testing.T) {
	if !almost(SpreadCents(0.40, 0.43), 3) {
		t.Error("expected 3c spread")
	}
	if SpreadCents(0.50, 0.40) != 0 {
		t.Error("crossed book should report 0 spread")
	}
}
