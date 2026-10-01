package ev

import (
	"math"
	"testing"
)

func almost(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// Values taken directly from Kalshi's published fee table (general, taker, 100 contracts).
func TestTakerFeeMatchesPublishedTable(t *testing.T) {
	table := map[int]float64{
		1: 0.07, 5: 0.34, 10: 0.63, 15: 0.90, 20: 1.12, 25: 1.32, 30: 1.47,
		35: 1.60, 40: 1.68, 45: 1.74, 50: 1.75, 55: 1.74, 60: 1.68, 65: 1.60,
		70: 1.47, 75: 1.32, 80: 1.12, 85: 0.90, 90: 0.63, 95: 0.34, 99: 0.07,
	}
	for cents, want := range table {
		if got := Fee(General, false, 100, float64(cents)/100); !almost(got, want) {
			t.Errorf("taker 100 @ %dc = %.2f, want %.2f", cents, got, want)
		}
	}
}

func TestMakerIsQuarterOfTaker(t *testing.T) {
	if got := Fee(General, true, 100, 0.50); !almost(got, 0.44) { // 0.0175*100*0.25 = 0.4375 -> 0.44
		t.Errorf("maker 100 @ 50c = %.2f, want 0.44", got)
	}
}

func TestIndexCoefficient(t *testing.T) {
	if got := Fee(IndexSP, false, 100, 0.50); !almost(got, 0.88) { // S&P table value
		t.Errorf("S&P taker 100 @ 50c = %.2f, want 0.88", got)
	}
}

func TestSingleContractFee(t *testing.T) {
	if got := Fee(General, false, 1, 0.50); !almost(got, 0.02) {
		t.Errorf("taker 1 @ 50c = %.2f, want 0.02", got)
	}
}

func TestZeroOnInvalidInputs(t *testing.T) {
	if Fee(General, false, 0, 0.50) != 0 || Fee(General, false, 100, 0) != 0 || Fee(General, false, 100, 1) != 0 {
		t.Error("expected zero fee for invalid inputs")
	}
}

func TestEvaluateEdgeAfterFee(t *testing.T) {
	// Buy YES at 0.40, believe true chance is 0.50, 100 contracts, taker.
	r := Evaluate(Trade{Class: General, Contracts: 100, Price: 0.40, FairValue: 0.50})
	if !almost(r.Fee, 1.68) {
		t.Fatalf("fee=%.4f want 1.68", r.Fee)
	}
	if !almost(r.EdgePerC, 0.10) {
		t.Fatalf("edge=%.4f want 0.10", r.EdgePerC)
	}
	if !almost(r.Breakeven, 0.40+1.68/100) {
		t.Fatalf("breakeven=%.4f want %.4f", r.Breakeven, 0.40+1.68/100)
	}
	if r.EVPerContract <= 0 {
		t.Fatalf("expected positive EV after fee, got %.4f", r.EVPerContract)
	}
}

func TestEvaluateNegativeWhenEdgeInsideFee(t *testing.T) {
	// 1 cent of edge at 50c is swallowed by the fee.
	r := Evaluate(Trade{Class: General, Contracts: 100, Price: 0.50, FairValue: 0.51})
	if r.EVPerContract >= 0 {
		t.Fatalf("expected negative EV (edge inside fee), got %.4f", r.EVPerContract)
	}
}
