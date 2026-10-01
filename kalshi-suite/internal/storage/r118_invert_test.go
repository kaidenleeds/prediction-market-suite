package storage

// r118_invert_test.go — pins FamilyEdgeStats' R118 invert cost inputs against hand-built rows:
//  - pc: side-adjusted settle − entry − the venue's whole-contract balance fee;
//  - InvHaircutPC: per-row half of the recorded spread, 2¢ documented default when unrecorded.

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestR118FamilyEdgeStatsInvertHaircut(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	ctx := context.Background()

	// Family A ("fade-a"): 20 YES rows @0.60 settling 0 (a pure deep loser). 10 rows carry a
	// recorded 10¢ spread (haircut 5¢ each), 10 carry none (2¢ default each) → mean haircut 3.5¢.
	for i := 0; i < 20; i++ {
		sc := 0.0
		if i < 10 {
			sc = 10
		}
		if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: fmt.Sprintf("A%d", i), Title: "a", Side: "YES",
			SignalType: "fade-a", EntryPrice: 0.60, SpreadCents: sc}); err != nil {
			t.Fatalf("insert A: %v", err)
		}
	}
	// Family B ("fade-b"): 20 NO rows @0.40 with a YES settle of 1 (side-adjusted settle 0).
	// Family C ("fade-c"): 20 YES rows @0.60 half-settling at 0.5.
	for i := 0; i < 20; i++ {
		if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: fmt.Sprintf("B%d", i), Title: "b", Side: "NO",
			SignalType: "fade-b", EntryPrice: 0.40}); err != nil {
			t.Fatalf("insert B: %v", err)
		}
		if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: fmt.Sprintf("C%d", i), Title: "c", Side: "YES",
			SignalType: "fade-c", EntryPrice: 0.60}); err != nil {
			t.Fatalf("insert C: %v", err)
		}
	}
	for fam, sv := range map[string]float64{"fade-a": 0, "fade-b": 1, "fade-c": 0.5} {
		if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1, settle_val=? WHERE signal_type=?`, sv, fam); err != nil {
			t.Fatalf("resolve %s: %v", fam, err)
		}
	}

	fams, err := st.FamilyEdgeStats(ctx, time.Time{})
	if err != nil {
		t.Fatalf("FamilyEdgeStats: %v", err)
	}
	got := map[string]FamilyEdge{}
	for _, f := range fams {
		got[f.Family] = f
	}
	const fee60 = 0.02 // Kalshi's $0.0168 trade fee settles as a 2¢ whole-contract balance charge
	checks := []struct {
		fam              string
		mean, fee, invHC float64
	}{
		{"fade-a", 0 - 0.60 - fee60, fee60, 0.035},  // pc = settle − entry − fee; haircut (10×5¢+10×2¢)/20
		{"fade-b", 0 - 0.40 - fee60, fee60, 0.02},   // NO side: settle flips (1−1=0); default haircut
		{"fade-c", 0.5 - 0.60 - fee60, fee60, 0.02}, // half-settle graded at 0.5
	}
	for _, c := range checks {
		f, ok := got[c.fam]
		if !ok || f.N != 20 {
			t.Fatalf("%s: missing or wrong N (%+v)", c.fam, f)
		}
		if math.Abs(f.Mean-c.mean) > 1e-9 || math.Abs(f.FeePC-c.fee) > 1e-9 || math.Abs(f.InvHaircutPC-c.invHC) > 1e-9 {
			t.Fatalf("%s: mean=%v fee=%v invHC=%v, want %v / %v / %v", c.fam, f.Mean, f.FeePC, f.InvHaircutPC, c.mean, c.fee, c.invHC)
		}
	}
}
