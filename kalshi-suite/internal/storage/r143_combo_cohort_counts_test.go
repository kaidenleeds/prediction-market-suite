package storage

import (
	"context"
	"testing"
)

func TestR143OpenComboCountsKeepCohortAndTwoThroughSixLengths(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	var rows []PlabCand
	for legs := 2; legs <= 6; legs++ {
		rows = append(rows, PlabCand{ID: string(rune('a' + legs)), At: 1, Bucket: "indep", Class: "test",
			Legality: "synthetic_legs_only", Prod: .25, JointP: .3, EVSyn: .1,
			LegFees: "[]", Legs: "[]", NLegs: legs, Cohort: ComboLabCohortRollingPositive,
			RouteState: "paper-only-rolling-positive"})
	}
	if n, err := st.PlabInsertBatch(ctx, rows); err != nil || n != 5 {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	counts, err := st.PlabOpenCurrentCohortLegCounts(ctx, 6)
	if err != nil {
		t.Fatal(err)
	}
	for legs := 2; legs <= 6; legs++ {
		if counts[ComboLabCohortRollingPositive][legs] != 1 {
			t.Fatalf("rolling %dL count=%d, want 1; all=%+v", legs, counts[ComboLabCohortRollingPositive][legs], counts)
		}
	}
}
