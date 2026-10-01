package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestFrozenSelectorUsesPriorEventDisjointControlsThenEmitsOnlySelectedArm(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)
	insertControl := func(observed time.Time, eventID, ticker, side string, realized *float64) int64 {
		t.Helper()
		registerR138EvidenceInstrument(t, st, eventID, "kalshi", ticker)
		row := r138EvidenceFixture("control", false)
		row.Observed, row.SystemID = observed, "attention-spillover-graph"
		row.OpportunityID, row.Cohort = eventID+"|"+side, "selector-training-fixture"
		row.CanonicalEventID, row.EventVersion = eventID, 0
		row.CanonicalPayoffID, row.PayoffVersion = "", 0
		row.Ticker, row.Side, row.NetLower = ticker, side, -.41
		row.PayoutLower, row.PayoutUpper, row.NetUpper = 0, 1, .59
		row.Blocker = "training control"
		row.CapacityCurve = []CapacityPoint{{Size: 1, Cost: .40, Fee: .01, PayoutFloor: 0, NetFloor: -.41}}
		row.Inputs = map[string]any{"selector_training": true, "selector_scope": "venue=kalshi", "selector_cell": "child=spread",
			"selector_arm": side}
		id, inserted, insertErr := st.InsertResearchSystemObservation(ctx, row)
		if insertErr != nil || !inserted {
			t.Fatalf("insert %s/%s=%v err=%v", eventID, side, inserted, insertErr)
		}
		if realized != nil {
			payout := *realized + row.Cost + row.Fee
			if ok, updateErr := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{ObservationID: id,
				Observed: observed.Add(time.Hour), Status: "settled", PayoutLower: payout, PayoutUpper: payout,
				RealizedNet: realized, SourceArtifact: "fixture settlement",
				SourceHash: fmt.Sprintf("%s-%s", eventID, side)}); updateErr != nil || !ok {
				t.Fatalf("grade %s/%s=%v err=%v", eventID, side, ok, updateErr)
			}
		}
		return id
	}
	for i := 0; i < 20; i++ {
		observed := base.Add(time.Duration(i%7) * 24 * time.Hour)
		eventID, ticker := fmt.Sprintf("selector-event-%02d", i), fmt.Sprintf("KXSELECT%02d", i)
		yes, no := .20, -.10
		insertControl(observed, eventID, ticker, "YES", &yes)
		insertControl(observed, eventID, ticker, "NO", &no)
	}
	freezeAt := base.Add(8 * 24 * time.Hour)
	// The complete prior UTC day is an embargo. These extreme outcomes would reverse the choice if
	// either yesterday or the current partial day leaked into training.
	leakYes, leakNo := -100.0, 100.0
	insertControl(freezeAt.Add(-24*time.Hour), "selector-embargo-event", "KXSELECTEMB", "YES", &leakYes)
	insertControl(freezeAt.Add(-24*time.Hour), "selector-embargo-event", "KXSELECTEMB", "NO", &leakNo)
	insertControl(freezeAt.Add(-30*time.Minute), "selector-precreate-event", "KXSELECTPRE", "YES", nil)
	insertControl(freezeAt.Add(-30*time.Minute), "selector-precreate-event", "KXSELECTPRE", "NO", nil)
	frozen, err := st.FreezeEligibleResearchSelectors(ctx, freezeAt, 7, 20)
	if err != nil || frozen != 1 {
		t.Fatalf("frozen=%d err=%v", frozen, err)
	}
	newEvent, newTicker := "selector-event-new", "KXSELECTNEW"
	yesID := insertControl(freezeAt.Add(time.Minute), newEvent, newTicker, "YES", nil)
	noID := insertControl(freezeAt.Add(time.Minute), newEvent, newTicker, "NO", nil)
	emitted, err := st.EmitFrozenSelectorCandidates(ctx, freezeAt.Add(2*time.Minute), 10)
	if err != nil || emitted != 1 {
		t.Fatalf("emitted=%d err=%v", emitted, err)
	}
	var side, cohort string
	var sourceID, candidate, paper, live int
	if err := st.db.QueryRow(`SELECT o.side,o.cohort,e.source_observation_id,o.candidate,
o.paper_authority,o.live_authority FROM research_frozen_selector_emissions e
JOIN research_system_observations o ON o.id=e.candidate_observation_id`).Scan(
		&side, &cohort, &sourceID, &candidate, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if side != "YES" || sourceID != int(yesID) || sourceID == int(noID) || candidate != 1 ||
		paper != 0 || live != 0 || cohort == "selector-training-fixture" {
		t.Fatalf("emission side=%s cohort=%s source=%d yes/no=%d/%d candidate/auth=%d/%d/%d",
			side, cohort, sourceID, yesID, noID, candidate, paper, live)
	}
}

func TestFrozenSelectorEmissionPlanStartsFromSparseSelectorLedger(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	rows, err := st.db.Query(`EXPLAIN QUERY PLAN `+frozenSelectorEmissionSelect, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(plan) < 2 || !strings.Contains(plan[0], "SCAN f") ||
		!strings.Contains(plan[1], "SEARCH o USING INDEX idx_rsystem_obs_system") {
		t.Fatalf("selector emission can regress to a full observation scan: %v", plan)
	}
}
