package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func seedExactVenueSettlement(t *testing.T, st *storage.Store, platform, ticker string, yes float64, observed, resolved time.Time) {
	t.Helper()
	won := 0
	if yes >= .5 {
		won = 1
	}
	_, err := st.DBForTest().Exec(`INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,won,settle_val,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,1,?,?,?)`, observed.UTC().Format(time.RFC3339Nano),
		observed.UTC().Format("2006-01-02"), observed.UTC().Format("200601021504"), platform,
		ticker, "fixture", "YES", "fixture-settlement-"+platform+"-"+ticker, .5, won, yes,
		resolved.UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	if strings.EqualFold(platform, "polyus") {
		if inserted, err := st.RecordVenueSettlement(context.Background(), "polyus", ticker, yes,
			resolved, storage.PolyUSFinalEndpointSettlementV2); err != nil || !inserted {
			t.Fatalf("record trusted PolyUS terminal receipt: inserted=%v err=%v", inserted, err)
		}
	}
}

func TestR139NativeRouteControlsReceiveOnlyExactCounterfactualTerminalGrades(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	opened := time.Now().UTC().Add(-2 * time.Hour)
	resolved := opened.Add(time.Hour)
	for _, row := range []struct {
		venue, ticker string
		yes           float64
	}{{"kalshi", "TL", 0}, {"kalshi", "TR", 1}, {"polyus", "JA", 1},
		{"polyus", "JB", 0}, {"kalshi", "FEE", 1},
		// Same ticker, opposite venue/value: terminal grading must remain venue-scoped.
		{"kalshi", "JA", 0}} {
		seedExactVenueSettlement(t, s.store, row.venue, row.ticker, row.yes, opened.Add(-time.Hour), resolved)
	}
	if _, err := s.insertNativeAggregateRoute(ctx, nativeAggregateRoute{
		System: "time-nested-lock", Opportunity: "time", EventID: "time-event", Venue: "kalshi",
		Identity: "verified", SourceArtifact: "verified relation", QuoteSource: "kalshi_book_ws",
		FeeSource: "kalshi-fee-v1", Decision: "blocked", Reason: "non-atomic", Observed: opened,
		DecisionAt: opened, QuoteAge: .1, Tick: .01, Qty: 1, Depth: 3, Fees: .02,
		PayoutLow: 1, PayoutHigh: 2, NetLow: .28, NetHigh: 1.28, PartialWorst: -.4,
		Evidence: map[string]any{"left_ticker": "TL", "right_ticker": "TR",
			"all_leg_cost": .70, "exact_fee": .02, "atomic": false},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.insertNativeAggregateRoute(ctx, nativeAggregateRoute{
		System: "joint-marginal-lock", Opportunity: "joint", EventID: "joint-event", Venue: "polyus",
		Identity: "structural", SourceArtifact: "certified basket", QuoteSource: "polyus_market_ws_full",
		FeeSource: "polyus-fee-v1", Decision: "blocked", Reason: "no joint quote", Observed: opened,
		DecisionAt: opened, QuoteAge: .1, Tick: .01, Qty: 1, Depth: 3, Fees: .02,
		PayoutLow: 0, PayoutHigh: 2, NetLow: -.92, NetHigh: 1.08, PartialWorst: -.92,
		Evidence: map[string]any{"legs": []storage.BasketLeg{{Ticker: "JA", Side: "YES"},
			{Ticker: "JB", Side: "NO"}}, "all_leg_cost": .90, "exact_fee": .02, "atomic": false},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.InsertResearchRouteOpportunity(ctx, storage.ResearchRouteOpportunity{
		OpportunityID: "fee-opp", RouteID: "fee-route", Observed: opened, DecisionAt: opened,
		SystemName: "fee-rounding-batch", CanonicalEventID: "venue:kalshi:FEE",
		IdentityStatus: "verified", Venue: "kalshi", Ticker: "FEE", Side: "YES", Route: "taker",
		Action: "control", QuoteSource: "kalshi_book_ws", QuoteAgeSeconds: .1, QuoteAgeKnown: true,
		DecisionLatencyMS: 1, LatencyKnown: true, TickSize: .01, TickKnown: true,
		ExecutablePrice: .20, ExecutableDepth: 3, DepthKnown: true, RequestedQty: 3,
		FeeAmount: .03, FeeAuthority: "kalshi-fee-v1", FeeKnown: true,
		ExpectedPayoutLow: 0, ExpectedPayoutHigh: 3, ExpectedNetLow: -.63, ExpectedNetHigh: 2.37,
		PartialFillWorst: -.63, Decision: "control", DecisionReason: "fee rounding control",
		EvidenceJSON: `{"optimization_candidate":true,"actual_fill":false}`,
	}); err != nil {
		t.Fatal(err)
	}
	graded, invalid, waiting, err := s.settleNativeRouteControls(ctx, time.Now().UTC())
	if err != nil || graded != 3 || invalid != 0 || waiting != 0 {
		t.Fatalf("graded/invalid/waiting=%d/%d/%d err=%v", graded, invalid, waiting, err)
	}
	want := map[string]struct{ payout, net float64 }{
		"time-nested-lock":    {2, 1.28},
		"joint-marginal-lock": {2, 1.08},
		"fee-rounding-batch":  {3, 2.37},
	}
	rows, err := s.store.DBForTest().Query(`SELECT o.system_name,e.payout,e.realized_net,
e.evidence_json,e.funded,e.paper_authority,e.live_authority
FROM research_route_events e JOIN research_route_opportunities o
 ON o.opportunity_id=e.opportunity_id AND o.route_id=e.route_id
WHERE e.event_type='grade' AND o.system_name IN ('time-nested-lock','joint-marginal-lock','fee-rounding-batch')`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var system, evidence string
		var payout, net float64
		var funded, paper, live int
		if err := rows.Scan(&system, &payout, &net, &evidence, &funded, &paper, &live); err != nil {
			t.Fatal(err)
		}
		expect := want[system]
		if math.Abs(payout-expect.payout) > 1e-9 || math.Abs(net-expect.net) > 1e-9 ||
			funded != 0 || paper != 0 || live != 0 ||
			!containsAll(evidence, `"actual_fill_claimed":false`, `"atomic_fill_claimed":false`) {
			t.Fatalf("%s payout/net=%v/%v evidence=%s authority=%d/%d/%d", system, payout, net, evidence, funded, paper, live)
		}
		n++
	}
	if n != 3 {
		t.Fatalf("terminal grades=%d, want 3", n)
	}
	graded, _, _, err = s.settleNativeRouteControls(ctx, time.Now().UTC())
	if err != nil || graded != 0 {
		t.Fatalf("retry graded=%d err=%v", graded, err)
	}
}

func TestR139FrozenTakerObservationSettlesAndReachesInferenceWithoutGradingControls(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	opened, resolved := now.Add(-48*time.Hour), now.Add(-36*time.Hour)
	eventID := "venue:kalshi:KXTERMINAL"
	payoffID := "payoff:kalshi:KXTERMINAL:YES"
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{
		EventID: eventID, EventType: "fixture", Domain: "test", Title: "fixture",
		SettlementSource: "fixture settlement", SourceArtifact: "fixture", SourceClockID: "fixture-clock",
		OutcomeSetStatus: "complete", EvidenceJSON: `{}`,
	}}, []storage.CanonicalPayoffSpec{{PayoffID: payoffID, EventID: eventID, Label: "YES",
		PredicateJSON: `{}`, SettlementSource: "fixture settlement", SourceArtifact: "fixture",
		IdentityStatus: "verified", PayoutFloor: 0, PayoutCeiling: 1, EvidenceJSON: `{}`}},
		[]storage.CanonicalInstrumentSpec{{Venue: "kalshi", Ticker: "KXTERMINAL", EventID: eventID,
			PayoffID: payoffID, NativeSide: "YES", Orientation: "same", MarketKind: "binary",
			SettlementSource: "fixture settlement", RulesArtifact: "fixture", RulesHash: "fixture-rules",
			FeeAuthority: "kalshi-fee-v1", IdentityStatus: "verified", EvidenceJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	row := storage.ResearchSystemObservation{Observed: opened, SystemID: "deadline-hazard-surface",
		OpportunityID: "terminal-taker", Kind: "candidate", Cohort: "fixture",
		CanonicalEventID: eventID, EventVersion: 0, CanonicalPayoffID: payoffID, PayoffVersion: 0,
		Venue: "kalshi", Ticker: "KXTERMINAL",
		Route: "taker", Side: "YES", CertificateStatus: "verified", CertificateHash: "fixture-cert",
		SourceClockID: "kalshi-orderbook-ws", SourceArtifact: "fixture", BookSource: "kalshi_book_ws",
		FeeSource: "kalshi-fee-v1", QuoteAgeMax: .1, TickMin: .01, Size: 1, Cost: .40, Fee: .01,
		PayoutLower: .50, PayoutUpper: 1, NetLower: .09, NetUpper: .59, VisibleCapacity: 3,
		DecisionLatencyMS: 1, Candidate: true, LatencyKnown: true, QuoteAgeKnown: true,
		TickKnown: true, DepthKnown: true, FeeKnown: true,
		CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: .40, Fee: .01, PayoutFloor: .50, NetFloor: .09}},
	}
	id, inserted, err := s.store.InsertResearchSystemObservation(ctx, row)
	if err != nil || !inserted || id <= 0 {
		t.Fatalf("observation id=%d inserted=%v err=%v", id, inserted, err)
	}
	// A fully priced same-ticker taker control is useful counterfactual input, but it is not an
	// accepted system action. It must never enter terminal economic n or the sealed promotion path.
	control := row
	control.SystemID, control.OpportunityID, control.Kind, control.Candidate =
		"side-normalized-crowding-fade", "priced-taker-control", "control", false
	control.NetLower, control.Blocker = -.41, "matched no-action control"
	if _, ok, err := s.store.InsertResearchSystemObservation(ctx, control); err != nil || !ok {
		t.Fatalf("priced taker control inserted=%v err=%v", ok, err)
	}
	// These controls resolve on the same ticker, but the terminal worker must not turn either into
	// economic evidence because neither represents a complete immediate-taker route.
	row.SystemID, row.OpportunityID, row.Kind, row.Route, row.Candidate = "flow-direction-integrity", "observer", "control", "observer", false
	row.EventVersion, row.Cost, row.Fee, row.TickMin, row.Size, row.VisibleCapacity = 0, 0, 0, 0, 0, 0
	row.BookSource, row.FeeSource = "", ""
	row.LatencyKnown, row.QuoteAgeKnown, row.TickKnown, row.DepthKnown, row.FeeKnown = false, false, false, false, false
	if _, _, err := s.store.InsertResearchSystemObservation(ctx, row); err != nil {
		t.Fatal(err)
	}
	row.SystemID, row.OpportunityID, row.Route = "maker-salvage-matched-cohort", "maker-control", "maker-control"
	row.EventVersion, row.Cost, row.Fee, row.TickMin, row.Size, row.VisibleCapacity = 0, .39, 0, .01, 1, 0
	row.BookSource, row.FeeSource = "kalshi_book_ws", "kalshi-fee-v1"
	row.LatencyKnown, row.QuoteAgeKnown, row.TickKnown, row.DepthKnown, row.FeeKnown = true, true, true, true, true
	if _, _, err := s.store.InsertResearchSystemObservation(ctx, row); err != nil {
		t.Fatal(err)
	}
	seedExactVenueSettlement(t, s.store, "kalshi", "KXTERMINAL", 1, opened.Add(-time.Hour), resolved)
	pending, pendingErr := s.store.PendingResearchSystemTakerSettlements(ctx, 10)
	if pendingErr != nil || len(pending) != 2 {
		var eventVersion int
		_ = s.store.DBForTest().QueryRow(`SELECT event_version FROM research_system_observations WHERE id=?`, id).Scan(&eventVersion)
		t.Fatalf("pending=%+v err=%v event_version=%d opened=%s resolved=%s", pending, pendingErr, eventVersion,
			opened.Format(time.RFC3339Nano), resolved.Format(time.RFC3339Nano))
	}
	s.sweepResearchSystemTerminalGrades(ctx, now)
	var updates int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM research_system_payoff_updates`).Scan(&updates); err != nil {
		t.Fatal(err)
	}
	if updates != 2 {
		t.Fatalf("payoff updates=%d, want candidate plus priced training control", updates)
	}
	var payout, realized float64
	if err := s.store.DBForTest().QueryRow(`SELECT payout_lower,realized_net FROM research_system_payoff_updates WHERE observation_id=?`, id).Scan(&payout, &realized); err != nil {
		t.Fatal(err)
	}
	if payout != 1 || math.Abs(realized-.59) > 1e-9 {
		t.Fatalf("payout/realized=%v/%v", payout, realized)
	}
	report, _, err := s.store.RunR139ResearchInference(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if report.ExactTerminalRows != 1 {
		t.Fatalf("exact terminal rows=%d exclusions=%+v", report.ExactTerminalRows, report.Exclusions)
	}
	found := false
	for _, result := range report.Results {
		if result.SystemID == "deadline-hazard-surface" {
			found = result.TerminalRows == 1
		}
	}
	if !found || report.Funded || report.PaperAuthority || report.LiveAuthority {
		t.Fatalf("inference did not admit exact row or gained authority: %+v", report)
	}
}

func containsAll(v string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(v, needle) {
			return false
		}
	}
	return true
}
