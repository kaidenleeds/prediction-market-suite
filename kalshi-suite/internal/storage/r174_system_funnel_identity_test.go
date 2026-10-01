package storage

import (
	"context"
	"testing"
	"time"
)

func insertR174FunnelControls(t *testing.T, st *Store, system string, inputs []ResearchSystemFunnelInput) {
	t.Helper()
	ctx := context.Background()
	seen := map[string]bool{}
	for _, input := range inputs {
		if seen[input.OpportunityID] {
			t.Fatalf("%s reused opportunity id %q", system, input.OpportunityID)
		}
		seen[input.OpportunityID] = true
		observed, err := time.Parse(time.RFC3339Nano, input.Observed)
		if err != nil {
			t.Fatal(err)
		}
		_, inserted, err := st.InsertResearchSystemObservation(ctx, ResearchSystemObservation{
			Observed: observed, SystemID: system, ExperimentVersion: 1,
			OpportunityID: input.OpportunityID, Kind: "control", Cohort: "system-specific-input",
			CanonicalEventID: input.CanonicalEventID, Venue: input.Venue, Ticker: input.Ticker,
			Route: "observer", CertificateStatus: "not_applicable", SourceClockID: system + "-funnel",
			SourceArtifact: input.Source, OutcomeStatus: "open", Blocker: "fixture", Inputs: input.Inputs,
		})
		if err != nil || !inserted {
			t.Fatalf("%s input %q inserted=%v err=%v", system, input.OpportunityID, inserted, err)
		}
	}
}

func TestR174SystemFunnelUnitTrialIdentityDoesNotCollapseDistinctTrials(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	const closed = "2026-07-25T12:00:00.123456Z"
	for i, family := range []string{"family-a", "family-b"} {
		if _, err := st.db.Exec(`INSERT INTO unit_trials(
opened_ts,closed_ts,family,platform,origin_layer,ticker,side,episode,ask,fee_pc,settled,pnl_pc)
VALUES(?, ?, ?, 'kalshi', 'model', 'KX-SAME', 'YES', ?, .40, .01, 1, .59)`,
			"2026-07-25T11:00:00Z", closed, family, i+1); err != nil {
			t.Fatal(err)
		}
	}
	for _, system := range []string{"collateral-release-rotation", "settlement-latency-carry"} {
		inputs, err := st.ResearchSystemFunnelInputs(context.Background(), system, 20)
		if err != nil || len(inputs) != 2 {
			t.Fatalf("%s inputs=%d err=%v", system, len(inputs), err)
		}
		insertR174FunnelControls(t, st, system, inputs)
	}
}

func TestR174IdentityFunnelRetainsExactSourceRouteIdentity(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	for i, sourceID := range []string{"source-opportunity-a", "source-opportunity-b"} {
		inserted, err := st.InsertResearchRouteOpportunity(context.Background(), ResearchRouteOpportunity{
			OpportunityID: sourceID, RouteID: "source-route-" + sourceID, Observed: now,
			DecisionAt: now, SystemName: "identity-challenged-cross-venue-lock",
			IdentityStatus: "structural", Venue: "multi", Ticker: "shared-route", Side: "NONE",
			Route: "control", Action: "control", QuoteSource: "fixture-book",
			FeeAuthority: "fixture-fee", Decision: "control", DecisionReason: "fixture",
			EvidenceJSON: `{"variant":` + string(rune('1'+i)) + `}`,
		})
		if err != nil || !inserted {
			t.Fatalf("route %d inserted=%v err=%v", i, inserted, err)
		}
	}
	inputs, err := st.ResearchSystemFunnelInputs(context.Background(),
		"identity-challenged-cross-venue-lock", 20)
	if err != nil || len(inputs) != 2 {
		t.Fatalf("identity inputs=%d err=%v", len(inputs), err)
	}
	insertR174FunnelControls(t, st, "identity-challenged-cross-venue-lock", inputs)
}
