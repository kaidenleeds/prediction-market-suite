package storage

import (
	"context"
	"testing"
)

func TestR147CurrentComboAdmissionCountsExcludeButPreserveLegacyRows(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	rows := []PlabCand{
		{ID: "legacy-2l", At: 199, Bucket: "indep", Class: "ind:2leg", Legality: "synthetic_legs_only",
			Prod: .16, JointP: .25, EVSyn: .1, LegFees: "[0,0]", Legs: "[]", NLegs: 2,
			Cohort: ComboLabCohortAllEligible, RouteState: "synthetic-settlement-only",
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "OLD-A"}, {Platform: "kalshi", Ticker: "OLD-B"}}},
		{ID: "current-general-2l", At: 200, Bucket: "indep", Class: "ind:2leg", Legality: "synthetic_legs_only",
			Prod: .16, JointP: .25, EVSyn: .1, LegFees: "[0,0]", Legs: "[]", NLegs: 2,
			Cohort: ComboLabCohortAllEligible, RouteState: "synthetic-settlement-only",
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "NEW-A"}, {Platform: "kalshi", Ticker: "NEW-B"}}},
		{ID: "current-system-6l", At: 201, Bucket: "indep", Class: "ind:6leg", Legality: "synthetic_legs_only",
			Prod: .01, JointP: .02, EVSyn: .1, LegFees: "[0,0,0,0,0,0]", Legs: "[]", NLegs: 6,
			Cohort: ComboLabCohortRollingPositive, RouteState: "paper-only-rolling-positive",
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "SYS-A"}, {Platform: "kalshi", Ticker: "SYS-B"},
				{Platform: "kalshi", Ticker: "SYS-C"}, {Platform: "kalshi", Ticker: "SYS-D"},
				{Platform: "kalshi", Ticker: "SYS-E"}, {Platform: "kalshi", Ticker: "SYS-F"}}},
	}
	if n, err := st.PlabInsertBatch(ctx, rows); err != nil || n != int64(len(rows)) {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	if n, err := st.PlabOpenCurrentCount(ctx, 6); err != nil || n != 3 {
		t.Fatalf("broad compatibility count=%d err=%v", n, err)
	}
	if n, err := st.PlabOpenCurrentCountSince(ctx, 6, 200); err != nil || n != 2 {
		t.Fatalf("current admission count=%d err=%v, want 2", n, err)
	}
	counts, err := st.PlabOpenCurrentCohortLegCountsSince(ctx, 6, 200)
	if err != nil || counts[ComboLabCohortAllEligible][2] != 1 || counts[ComboLabCohortRollingPositive][6] != 1 {
		t.Fatalf("current admission cohorts=%+v err=%v", counts, err)
	}
	if n, err := st.PlabOpenCount(ctx); err != nil || n != 3 {
		t.Fatalf("legacy settlement row was lost: n=%d err=%v", n, err)
	}
}
