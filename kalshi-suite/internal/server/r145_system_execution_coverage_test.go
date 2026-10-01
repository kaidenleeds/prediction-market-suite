package server

import (
	"fmt"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR145NineteenSystemsExposeExactExecutionCellsWithoutAggregateDuplicates(t *testing.T) {
	stats := make([]storage.ResearchSystemCollectionStat, 0, len(storage.SystemIDs()))
	for _, id := range storage.SystemIDs() {
		stats = append(stats, storage.ResearchSystemCollectionStat{SystemID: id, State: "COLLECTING"})
	}
	coverage := r138SystemCoverageRows(stats)
	seen := map[string]leaderboardCoverageRow{}
	connected, waitingChild, needsAdapter := 0, 0, 0
	for _, row := range coverage {
		key := fmt.Sprintf("%s|%s|%s|%s", row.Family, row.Venue, row.Side, row.Route)
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate exact system cell %s", key)
		}
		seen[key] = row
		if row.ResearchOnly || row.LiveAuthorizes {
			t.Fatalf("system cell confused technical connection with current LIVE authority: %+v", row)
		}
		if row.ExecutionConnected {
			connected++
			if row.LiveHandoff == "" || row.ExecutionRole == "" {
				t.Fatalf("connected cell lacks handoff/role: %+v", row)
			}
		} else {
			switch row.State {
			case "WAITING_FOR_REGISTERED_CHILD_ADAPTER":
				waitingChild++
			case "NEEDS_EXECUTION_ADAPTER":
				needsAdapter++
			default:
				t.Fatalf("disconnected cell hides its exact implementation gap: %+v", row)
			}
			if row.LiveHandoff != "" || row.Note == "" {
				t.Fatalf("disconnected cell advertised LIVE or hid its blocker: %+v", row)
			}
		}
	}

	exactVariants := 0
	for _, spec := range storage.SystemExecutionSpecs() {
		variants := spec.ExactVariants()
		exactVariants += len(variants)
		for _, variant := range variants {
			key := fmt.Sprintf("%s|%s|%s|%s", variant.SystemID, variant.Venue, variant.Side, variant.Route)
			if _, ok := seen[key]; !ok {
				t.Fatalf("registered execution variant missing from Systems coverage: %s", key)
			}
		}
	}
	if exactVariants != 55 || connected != 55 || waitingChild != 0 || needsAdapter != 2 || len(coverage) != 57 {
		t.Fatalf("coverage=%d connected=%d waiting_child=%d needs_adapter=%d exact_variants=%d; want 57/55/0/2/55",
			len(coverage), connected, waitingChild, needsAdapter, exactVariants)
	}

	rows := appendLeaderboardCollectionPlaceholders(nil, coverage)
	variants, systems := trackedSystemCountRows(rows)
	if variants != 55 || systems != 19 {
		t.Fatalf("registry-only count=%d variants/%d systems, want 55/19", variants, systems)
	}
}
