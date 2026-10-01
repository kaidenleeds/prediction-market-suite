package server

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestR151ArmedCrossVenueSettlementKeepsOnlyFundedRows(t *testing.T) {
	rows := []xvLockOpp{
		{Key: "research-only", Staked: 0},
		{Key: "funded", Staked: 3},
	}
	all := xvlArmedSettlementSnapshot(append([]xvLockOpp(nil), rows...), false)
	if len(all) != 2 {
		t.Fatalf("unarmed settlement rows=%d want 2", len(all))
	}
	funded := xvlArmedSettlementSnapshot(append([]xvLockOpp(nil), rows...), true)
	if len(funded) != 1 || funded[0].Key != "funded" {
		t.Fatalf("armed settlement rows=%+v want funded row only", funded)
	}
}

func TestR151ResearchDigestDefersWhileLiveIsArmed(t *testing.T) {
	s := testServer(t)
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveMu.Unlock()

	s.kickResearchDigestRefresh()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.researchDigestMu.Lock()
		busy, next, payload := s.researchDigestBusy, s.researchDigestNext, len(s.researchDigestJSON)
		s.researchDigestMu.Unlock()
		if !busy {
			if payload != 0 {
				t.Fatal("armed digest unexpectedly published a newly scanned payload")
			}
			if !next.After(time.Now()) {
				t.Fatal("armed digest did not schedule a deferred retry")
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("armed digest did not release its busy state")
}

func TestR151BulkDatabaseLanesUseLiveAwareCoordinator(t *testing.T) {
	wants := map[string][]string{
		"server.go": {
			`tryRunHeavyResearch(ctx, "broad-signal-settlement"`,
			`tryRunHeavyResearch(c, "catalog"`,
		},
		"settlement_lanes.go": {
			`tryRunHeavyResearch(ctx, "settlement-research-ledgers"`,
			`tryRunHeavyResearch(ctx, "settlement-subcent-golf"`,
		},
		"pttbackfill.go": {
			`tryRunHeavyResearch(ctx, "ptt-backfill"`,
		},
		"researchdigest.go": {
			`tryRunHeavyResearch(ctx, "research-digest"`,
		},
		"markettree.go": {
			`tryRunHeavyResearch(ctx, "market-tree-catalog"`,
		},
		"xvident.go": {
			`tryRunHeavyResearch(ctx, "xvident"`,
		},
	}
	for file, needles := range wants {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		for _, needle := range needles {
			if !strings.Contains(text, needle) {
				t.Fatalf("%s missing live-aware bulk lane %q", file, needle)
			}
		}
	}
}
