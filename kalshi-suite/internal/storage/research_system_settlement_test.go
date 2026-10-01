package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestR175PendingTakerSettlementPlanKeepsBothObservationLanesIndexed(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	rows, err := st.db.Query(`EXPLAIN QUERY PLAN `+researchSystemTakerSettlementSQL, 250)
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
	joined := strings.Join(plan, "\n")
	for _, want := range []string{
		"USING INDEX idx_rsystem_obs_pending_taker",
		"USING INDEX idx_rsystem_obs_pending_step7_structural_v3",
		"USING INDEX idx_rroute_system_observation",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("pending settlement plan lost %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "SCAN r") {
		t.Fatalf("pending settlement plan regressed to a full route-ledger scan:\n%s", joined)
	}
}

func TestR175PendingTakerSettlementsMergeLanesOldestFirstBeforeOneLimit(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 7, 25, 20, 0, 0, 0, time.UTC)

	type fixture struct {
		ticker     string
		kind       string
		identity   string
		cohort     string
		blocker    string
		candidate  bool
		observedAt time.Time
	}
	fixtures := []fixture{
		{ticker: "KX-R175-ORDER-A", kind: "candidate", identity: "verified",
			cohort: "ordinary-candidate", candidate: true, observedAt: base},
		{ticker: "KX-R175-ORDER-B", kind: "negative", identity: "structural",
			cohort:  Step7StructuralFlowCohort + "|structural-action|policy=" + Step7StructuralFlowSelector,
			blocker: Step7StructuralFlowBlocker, observedAt: base.Add(time.Second)},
		{ticker: "KX-R175-ORDER-C", kind: "control", identity: "verified",
			cohort: "ordinary-control", blocker: "matched exact control", observedAt: base.Add(2 * time.Second)},
		{ticker: "KX-R175-ORDER-D", kind: "negative", identity: "structural",
			cohort:  Step7StructuralFlowCohort + "|structural-action|policy=" + Step7StructuralFlowSelector,
			blocker: Step7StructuralFlowBlocker, observedAt: base.Add(3 * time.Second)},
	}

	events := make([]CanonicalEventSpec, 0, len(fixtures))
	payoffs := make([]CanonicalPayoffSpec, 0, len(fixtures))
	instruments := make([]CanonicalInstrumentSpec, 0, len(fixtures))
	for i, f := range fixtures {
		eventID := fmt.Sprintf("event:r175-order:%d", i)
		payoffID := fmt.Sprintf("payoff:r175-order:%d", i)
		events = append(events, CanonicalEventSpec{
			EventID: eventID, EventType: "venue-local-binary", Domain: "kalshi",
			Title: "R175 settlement ordering fixture", SourceArtifact: "test fixture",
			SourceClockID: "test-catalog-clock", OutcomeSetStatus: "unknown",
		})
		payoffs = append(payoffs, CanonicalPayoffSpec{
			PayoffID: payoffID, EventID: eventID, Label: "venue YES predicate",
			PredicateJSON: `{"venue_native_yes":true}`, PayoutFloor: 0, PayoutCeiling: 1,
			SourceArtifact: "test fixture", IdentityStatus: f.identity,
		})
		instruments = append(instruments, CanonicalInstrumentSpec{
			Venue: "kalshi", Ticker: f.ticker, EventID: eventID, PayoffID: payoffID,
			NativeSide: "YES", Orientation: "same", MarketKind: "binary",
			RulesArtifact: "test fixture", IdentityStatus: f.identity,
		})
	}
	if _, err := st.RegisterCanonicalBatch(ctx, events, payoffs, instruments); err != nil {
		t.Fatal(err)
	}

	insertedIDs := make(map[string]int64, len(fixtures))
	for i, f := range fixtures {
		inputs := map[string]any{}
		if f.kind == "negative" {
			inputs = map[string]any{
				"direction_selected":               true,
				"frozen_identity_status":           "structural",
				"identity_scope":                   "kalshi_exact_ticker_payoff_route",
				"flow_projection_semantics":        Step7StructuralFlowSemantics,
				"cross_venue_equivalence_verified": false,
				"profit_authority":                 false,
				"paper_authority":                  false,
				"live_authority":                   false,
			}
		}
		id, inserted, insertErr := st.InsertResearchSystemObservation(ctx, ResearchSystemObservation{
			Observed: f.observedAt, DecisionAt: f.observedAt.Add(time.Millisecond),
			SystemID: "flow-direction-integrity", ExperimentVersion: Step7StructuralFlowExperimentVersion,
			OpportunityID: fmt.Sprintf("r175-order-%d", i), Kind: f.kind, Cohort: f.cohort,
			CanonicalEventID: events[i].EventID, EventVersion: 1,
			CanonicalPayoffID: payoffs[i].PayoffID, PayoffVersion: 1,
			Venue: "kalshi", Ticker: f.ticker, Route: "taker", Side: "YES",
			CertificateStatus: f.identity, CertificateHash: fmt.Sprintf("certificate-%d", i),
			SourceClockID:  fmt.Sprintf("kalshi-book:test:%d", i),
			SourceArtifact: "R175 settlement ordering fixture",
			BookSource:     "kalshi_book_ws_full", FeeSource: "kalshi-fee-test",
			QuoteAgeMax: .001, TickMin: .01, Size: 1, Cost: .4, Fee: .01,
			PayoutLower: 0, PayoutUpper: 1, NetLower: -.41, NetUpper: .59,
			VisibleCapacity: 5, DecisionLatencyMS: 1,
			LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
			CapacityCurve: []CapacityPoint{{Size: 1, Cost: .4, Fee: .01, PayoutFloor: 0, NetFloor: -.41}},
			OutcomeStatus: "open", Blocker: f.blocker, Candidate: f.candidate, Inputs: inputs,
		})
		if insertErr != nil || !inserted {
			t.Fatalf("insert %s: inserted=%v err=%v", f.ticker, inserted, insertErr)
		}
		insertedIDs[f.ticker] = id
		if _, err := st.RecordVenueSettlement(ctx, "kalshi", f.ticker, 1,
			base.Add(24*time.Hour), "kalshi final settlement test fixture"); err != nil {
			t.Fatal(err)
		}
	}

	got, err := st.PendingResearchSystemTakerSettlements(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"KX-R175-ORDER-A", "KX-R175-ORDER-B", "KX-R175-ORDER-C"}
	if len(got) != len(want) {
		t.Fatalf("pending rows=%+v want tickers=%v", got, want)
	}
	for i := range want {
		if got[i].Ticker != want[i] || got[i].ObservationID != insertedIDs[want[i]] {
			t.Fatalf("pending[%d]=%+v want ticker=%s id=%d", i, got[i], want[i], insertedIDs[want[i]])
		}
	}
}
