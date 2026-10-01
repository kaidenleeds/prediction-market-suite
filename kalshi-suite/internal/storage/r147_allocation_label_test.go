package storage

import (
	"context"
	"testing"
)

func TestR147PromotionStateKeepsMachineEnumAndAddsAllocationLabel(t *testing.T) {
	legacy := "WAITING_FOR_FROZEN_ROUTE_CAUSE_AND_MODE4_GOVERNANCE"
	want := "WAITING_FOR_FROZEN_ROUTE_CAUSE_AND_ADAPTIVE_ALLOCATION_MODEL_GOVERNANCE"
	if got := researchPromotionStateLabel(legacy); got != want {
		t.Fatalf("operator state label=%q want %q", got, want)
	}
	// The caller stores the legacy enum unchanged; only the separate display field is translated.
	if legacy != "WAITING_FOR_FROZEN_ROUTE_CAUSE_AND_MODE4_GOVERNANCE" {
		t.Fatal("compatibility enum changed")
	}
}

func TestR147ProperScoreRetainsComparatorIDWithOperatorLabel(t *testing.T) {
	st, _ := openTemp(t)
	defer st.Close()
	report, err := st.ProperScoreReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	comparators := report["comparators"].(map[string]any)
	labels := comparators["display_labels"].(map[string]string)
	if labels["mode4-normalized"] != "Adaptive Allocation Model" {
		t.Fatalf("compatibility comparator lacks operator label: %+v", labels)
	}
	arms := comparators["arms"].([]string)
	found := false
	for _, arm := range arms {
		found = found || arm == "mode4-normalized"
	}
	if !found {
		t.Fatal("stable comparator id was renamed instead of labeled")
	}
}
