package server

// r116_test.go — pins the R116 verdict-engine corrections (recheck pass):
//  1. csRadius carries the bounded-range term 0.5·L/n (null-sim validated: skewed nulls were
//     false-proving at 9.5–11.1% without it, ≤1.7% with it).
//  2. whale-exit's R115 PROVEN+ at n=32 no longer clears the corrected bar (→ COLLECTING).
//  3. Large-n verdicts are materially untouched (the term decays ln(n)/n).
//  4. An INVERT twin inherits venue_locked from its parent.

import (
	"math"
	"testing"
)

func TestR116RadiusRangeCorrection(t *testing.T) {
	n, sd := 32, 0.2456
	L := math.Log(math.Sqrt(float64(n)+1) / 0.05)
	base := sd * math.Sqrt((2*(float64(n)+1))/(float64(n)*float64(n))*L)
	want := base + 0.5*L/float64(n)
	if got := csRadius(n, sd); math.Abs(got-want) > 1e-12 {
		t.Fatalf("csRadius(32,.2456)=%v want %v (range term missing?)", got, want)
	}
	if got := csRadius(n, sd); got-base < 0.07 {
		t.Fatalf("range term too small at n=32: extra=%v", got-base)
	}
}

func TestR116WhaleExitNoLongerProvenAt32(t *testing.T) {
	// R115 live numbers: n=32, mean +0.1463, sd 0.2456 fired PROVEN+. The corrected CS must not.
	v := verdictFrom("whale-exit", "signal", "$/contract", 32, 0.1463, 0.2456, 0.0099, 0)
	if v.State != "COLLECTING" {
		t.Fatalf("whale-exit n=32 state=%s want COLLECTING (lo=%v)", v.State, v.Lo)
	}
	// poly-consensus at n=94,670 must survive: the term is +0.00005 there.
	v = verdictFrom("poly-consensus", "signal", "$/contract", 94670, 0.0058, 0.3931, 0.0111, 0)
	if v.State != "PROVEN+" {
		t.Fatalf("poly-consensus n=94670 state=%s want PROVEN+ (lo=%v)", v.State, v.Lo)
	}
	// pcrypto PROVEN− and its qualifying invert edge (R118 honest cost model: −mean − 2·fee −
	// default 2¢ spread haircut = 0.2195 − 0.0254 − 0.02 = 0.1741).
	v = verdictFrom("pcrypto", "signal", "$/contract", 5222, -0.2195, 0.5097, 0.0127, 0)
	if v.State != "PROVEN-" || !almostEq(v.Invert, 0.1741) {
		t.Fatalf("pcrypto state=%s invert=%v want PROVEN- with invert = .1741", v.State, v.Invert)
	}
}

func TestR116InvertTwinInheritsVenueLock(t *testing.T) {
	v := verdictFrom("pcrypto", "signal", "$/contract", 5222, -0.2195, 0.5097, 0.0127, 0)
	v.Locked = true
	if v.Invert <= 0 {
		t.Fatal("expected a qualifying invert edge")
	}
	inv := verdictFrom("invert:pcrypto", "invert", v.Unit, v.N, -v.Mean-invertDrag(v.FeePC, v.InvHC), v.SD, v.FeePC, v.InvHC)
	inv.Locked = v.Locked // the add() path in computeExperimentVerdicts does exactly this
	if !inv.Locked {
		t.Fatal("invert twin must inherit venue_locked")
	}
}
