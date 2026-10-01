package storage

import "testing"

func TestR145EveryChildHandoffHasExplicitTruthfulAdapterContract(t *testing.T) {
	wantState := map[string]string{
		"maker-salvage-matched-cohort": "CODE_PATH_CONNECTED_OVERLAY",
	}
	seen := map[string]bool{}
	for _, spec := range SystemExecutionSpecs() {
		status := SystemExecutionAdapterStatus(spec)
		if status.SystemID != spec.SystemID || status.AdapterID == "" || status.State == "" || status.Reason == "" {
			t.Fatalf("incomplete adapter contract for %s: %+v", spec.SystemID, status)
		}
		if spec.HandoffClass != SystemHandoffChild {
			continue
		}
		seen[spec.SystemID] = true
		if !status.Connected || !status.CodePathExists {
			t.Fatalf("child %s lacks its concrete no-duplicate overlay: %+v", spec.SystemID, status)
		}
		if status.State != wantState[spec.SystemID] {
			t.Fatalf("child %s state=%q want %q", spec.SystemID, status.State, wantState[spec.SystemID])
		}
	}
	if len(seen) != len(wantState) {
		t.Fatalf("explicit child contracts=%v want %v", seen, wantState)
	}
	if got, _ := SystemExecutionCapability("maker-salvage-matched-cohort"); !SystemExecutionAdapterStatus(got).Connected {
		t.Fatal("maker-salvage overlay code path was lost")
	}
}
