package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func r146Observer(system, opportunity string) ResearchSystemObservation {
	return ResearchSystemObservation{
		Observed: time.Now().UTC(), SystemID: system, OpportunityID: opportunity,
		Kind: "control", Cohort: "kalshi-authoritative-aggressor-v1", Venue: "kalshi", Route: "observer", Side: "NONE",
		CertificateStatus: "not_applicable", SourceClockID: "fixture-clock",
		SourceArtifact: "compact native evidence owns the full observation", OutcomeStatus: "open",
		PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0,
		Blocker: "label_audit_only_no_trade_authority", Inputs: map[string]any{"native_row": opportunity},
	}
}

func r146LegacyObserverMirror(observationID int64, opportunity, routeID string) ResearchRouteOpportunity {
	return ResearchRouteOpportunity{
		OpportunityID: opportunity, RouteID: routeID, Observed: time.Now().UTC(),
		ExperimentID: "flow-direction-integrity", ExperimentVersion: 1,
		SystemName: "flow-direction-integrity", IdentityStatus: "unverified", Venue: "kalshi",
		Side: "NONE", Route: "abstain", Action: "control", QuoteSource: "research-observer",
		FeeAuthority: "not_applicable", Decision: "control", DecisionReason: "legacy observer mirror",
		ExpectedPayoutLow: 0, ExpectedPayoutHigh: 0, ExpectedNetLow: 0, ExpectedNetHigh: 0,
		EvidenceJSON: fmt.Sprintf(`{"system_observation_id":%d}`, observationID),
	}
}

func TestR146ObserverStaysCanonicalWithoutUnifiedRouteMirror(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	id, inserted, err := st.InsertResearchSystemObservation(ctx,
		r146Observer("flow-direction-integrity", "pair-1|authoritative"))
	if err != nil || !inserted || id <= 0 {
		t.Fatalf("observer id=%d inserted=%v err=%v", id, inserted, err)
	}
	var observations, routes int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_system_observations WHERE id=?`, id).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_route_opportunities`).Scan(&routes); err != nil {
		t.Fatal(err)
	}
	if observations != 1 || routes != 0 {
		t.Fatalf("canonical observations=%d route mirrors=%d", observations, routes)
	}
	settled, err := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{ObservationID: id,
		Observed: time.Now().UTC(), Status: "settled", PayoutLower: 0, PayoutUpper: 0,
		SourceArtifact: "fixture terminal label", SourceHash: "fixture-hash", Reason: "label complete"})
	if err != nil || !settled {
		t.Fatalf("observer payoff settled=%v err=%v", settled, err)
	}
	var updates, routeEvents int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_system_payoff_updates WHERE observation_id=?`, id).Scan(&updates); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_route_events`).Scan(&routeEvents); err != nil {
		t.Fatal(err)
	}
	if updates != 1 || routeEvents != 0 {
		t.Fatalf("payoff updates=%d fabricated route events=%d", updates, routeEvents)
	}
}

func TestR146ObserverRouteCompactionIsBoundedAndLossless(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	id1, ok, err := st.InsertResearchSystemObservation(ctx,
		r146Observer("flow-direction-integrity", "legacy-no-event"))
	if err != nil || !ok {
		t.Fatalf("observer one inserted=%v err=%v", ok, err)
	}
	id2, ok, err := st.InsertResearchSystemObservation(ctx,
		r146Observer("flow-direction-integrity", "legacy-with-event"))
	if err != nil || !ok {
		t.Fatalf("observer two inserted=%v err=%v", ok, err)
	}
	if ok, err := st.InsertResearchRouteOpportunity(ctx,
		r146LegacyObserverMirror(id1, "legacy-route-1", "observer-1")); err != nil || !ok {
		t.Fatalf("legacy mirror one inserted=%v err=%v", ok, err)
	}
	if ok, err := st.InsertResearchRouteOpportunity(ctx,
		r146LegacyObserverMirror(id2, "legacy-route-2", "observer-2")); err != nil || !ok {
		t.Fatalf("legacy mirror two inserted=%v err=%v", ok, err)
	}
	if _, err := st.AppendResearchRouteEvent(ctx, ResearchRouteEvent{OpportunityID: "legacy-route-2",
		RouteID: "observer-2", EventType: "note", Reason: "independent lifecycle evidence"}); err != nil {
		t.Fatal(err)
	}
	negative := routeFixture("negative-route", "negative-taker", "blocked")
	negative.SystemName, negative.ExperimentID = "flow-direction-integrity", "flow-direction-integrity"
	if ok, err := st.InsertResearchRouteOpportunity(ctx, negative); err != nil || !ok {
		t.Fatalf("negative route inserted=%v err=%v", ok, err)
	}

	scanned, removed, done, err := st.CompactRedundantObserverRouteMirrors(ctx, 1)
	if err != nil || scanned != 1 || removed != 1 || done {
		t.Fatalf("compaction scanned=%d removed=%d done=%v err=%v", scanned, removed, done, err)
	}
	scanned, removed, done, err = st.CompactRedundantObserverRouteMirrors(ctx, 1)
	if err != nil || scanned != 1 || removed != 0 || done {
		t.Fatalf("event-bearing page scanned=%d removed=%d done=%v err=%v", scanned, removed, done, err)
	}
	scanned, removed, done, err = st.CompactRedundantObserverRouteMirrors(ctx, 1)
	if err != nil || scanned != 0 || removed != 0 || !done {
		t.Fatalf("terminal page scanned=%d removed=%d done=%v err=%v", scanned, removed, done, err)
	}
	var observations, noEvent, withEvent, negativeRows int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_system_observations WHERE id IN (?,?)`, id1, id2).Scan(&observations)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_route_opportunities WHERE opportunity_id='legacy-route-1'`).Scan(&noEvent)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_route_opportunities WHERE opportunity_id='legacy-route-2'`).Scan(&withEvent)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_route_opportunities WHERE opportunity_id='negative-route'`).Scan(&negativeRows)
	if observations != 2 || noEvent != 0 || withEvent != 1 || negativeRows != 1 {
		t.Fatalf("observations=%d event-free=%d lifecycle=%d negative=%d",
			observations, noEvent, withEvent, negativeRows)
	}
	if _, err := st.db.Exec(`DELETE FROM research_route_opportunities WHERE opportunity_id='negative-route'`); err == nil {
		t.Fatal("public immutable delete trigger was not restored")
	}
}

func TestR146CrossVenueIdentityObserverRemainsRoutedAndFunnelVisible(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	row := r146Observer("identity-challenged-cross-venue-lock", "cv-resolution-reject")
	row.Cohort = "cross-venue-resolution-control"
	row.Blocker = "resolution terms are not equivalent"
	row.SourceArtifact = "accepted/rejected cross-venue resolution review"
	id, inserted, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil || !inserted || id <= 0 {
		t.Fatalf("identity observer id=%d inserted=%v err=%v", id, inserted, err)
	}
	var routes int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_route_opportunities
WHERE system_name='identity-challenged-cross-venue-lock'`).Scan(&routes); err != nil {
		t.Fatal(err)
	}
	if routes != 1 {
		t.Fatalf("cross-venue identity route rows=%d, want 1", routes)
	}
	inputs, err := st.ResearchSystemFunnelInputs(ctx, "identity-challenged-cross-venue-lock", 10)
	if err != nil || len(inputs) != 1 {
		t.Fatalf("identity funnel inputs=%d err=%v", len(inputs), err)
	}
	if _, removed, done, err := st.CompactRedundantObserverRouteMirrors(ctx, 10); err != nil || removed != 0 || !done {
		t.Fatalf("narrow compaction removed=%d done=%v err=%v", removed, done, err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_route_opportunities
WHERE system_name='identity-challenged-cross-venue-lock'`).Scan(&routes); err != nil || routes != 1 {
		t.Fatalf("identity route after compaction=%d err=%v", routes, err)
	}
}

func TestR146ObserverCompactionPlanSkipsLargeUnrelatedPrefix(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	// Production shape: a large prefix of unrelated observer controls precedes the one superseded
	// flow-label cohort. The cleanup page must enter through its exact partial index, never scan the
	// prefix while holding SQLite's single writer.
	const unrelated = 50000
	_, err = st.db.ExecContext(ctx, `WITH RECURSIVE n(x) AS (
 VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?
)
INSERT INTO research_system_observations(
observed_ts,decision_ts,observed_slot,system_id,experiment_version,opportunity_id,observation_kind,cohort,
venue,route,side,certificate_status,source_artifact,outcome_status,blocker,candidate,
funded,paper_authority,live_authority)
SELECT '2026-07-15T00:00:00Z','2026-07-15T00:00:00Z',printf('unrelated-%06d',x),'identity-challenged-cross-venue-lock',1,
printf('cv-control-%06d',x),'control','cross-venue-resolution-control','multi','observer','NONE',
'structural','cross-venue accept/reject fixture','open','resolution review retained',0,0,0,0 FROM n`, unrelated)
	if err != nil {
		t.Fatal(err)
	}
	id, inserted, err := st.InsertResearchSystemObservation(ctx,
		r146Observer("flow-direction-integrity", "indexed-cleanup-target"))
	if err != nil || !inserted {
		t.Fatalf("target id=%d inserted=%v err=%v", id, inserted, err)
	}
	if ok, err := st.InsertResearchRouteOpportunity(ctx,
		r146LegacyObserverMirror(id, "indexed-cleanup-route", "observer")); err != nil || !ok {
		t.Fatalf("target mirror inserted=%v err=%v", ok, err)
	}
	planRows, err := st.db.QueryContext(ctx, `EXPLAIN QUERY PLAN
SELECT id FROM research_system_observations INDEXED BY idx_rsystem_obs_flow_label_cleanup
WHERE id>0 AND system_id='flow-direction-integrity' AND route='observer'
 AND observation_kind='control' AND blocker='label_audit_only_no_trade_authority'
 AND cohort IN ('kalshi-authoritative-aggressor-v1','kalshi-inferred-aggressor-v1')
ORDER BY id LIMIT 500`)
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for planRows.Next() {
		var node, parent, aux int
		var detail string
		if err := planRows.Scan(&node, &parent, &aux, &detail); err != nil {
			_ = planRows.Close()
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := planRows.Close(); err != nil {
		t.Fatal(err)
	}
	planText := strings.Join(plan, " | ")
	if !strings.Contains(planText, "idx_rsystem_obs_flow_label_cleanup") {
		t.Fatalf("cleanup plan missed selective index: %s", planText)
	}
	started := time.Now()
	scanned, removed, done, err := st.CompactRedundantObserverRouteMirrors(ctx, 500)
	elapsed := time.Since(started)
	if err != nil || scanned != 1 || removed != 1 || !done {
		t.Fatalf("cleanup scanned=%d removed=%d done=%v elapsed=%s err=%v",
			scanned, removed, done, elapsed, err)
	}
	if elapsed >= time.Second {
		t.Fatalf("selective cleanup took %s with %d unrelated rows; plan=%s", elapsed, unrelated, planText)
	}
	t.Logf("cleanup plan=%s; unrelated=%d; elapsed=%s", planText, unrelated, elapsed)
	var crossVenue int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_system_observations
WHERE system_id='identity-challenged-cross-venue-lock'`).Scan(&crossVenue); err != nil || crossVenue != unrelated {
		t.Fatalf("cross-venue controls=%d err=%v", crossVenue, err)
	}
}

func TestR146ObserverCompactionWriterWaitIsHardBounded(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, inserted, err := st.InsertResearchSystemObservation(ctx,
		r146Observer("flow-direction-integrity", "writer-bound-target")); err != nil || !inserted {
		t.Fatalf("target inserted=%v err=%v", inserted, err)
	}
	locker, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer locker.Close()
	if _, err := locker.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
		t.Fatal(err)
	}
	defer locker.ExecContext(context.Background(), `ROLLBACK`)
	started := time.Now()
	_, _, _, err = st.CompactRedundantObserverRouteMirrors(ctx, 500)
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("compaction unexpectedly acquired a deliberately held writer")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("writer wait escaped two-second bound: elapsed=%s err=%v", elapsed, err)
	}
	t.Logf("writer contention returned in %s: %v", elapsed, err)
}
