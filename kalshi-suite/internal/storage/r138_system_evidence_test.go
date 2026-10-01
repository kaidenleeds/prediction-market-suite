package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func r138EvidenceFixture(kind string, candidate bool) ResearchSystemObservation {
	return ResearchSystemObservation{
		Observed: time.Now().UTC(), SystemID: "deadline-hazard-surface", OpportunityID: "opp-1",
		Kind: kind, Cohort: "certified", CanonicalEventID: "event:1", EventVersion: 1,
		Venue: "kalshi", Ticker: "KXTEST", Route: "taker", Side: "YES",
		CertificateStatus: "verified", CertificateHash: strings.Repeat("a", 64),
		SourceClockID: "kalshi-orderbook-ws", SourceArtifact: "immutable implication",
		BookSource: "kalshi_book_ws", FeeSource: "quadratic", QuoteAgeMax: .2, TickMin: .01,
		Size: 1, Cost: .40, Fee: .01, PayoutLower: .50, PayoutUpper: 1,
		NetLower: .09, NetUpper: .59, VisibleCapacity: 10, Candidate: candidate,
		DecisionLatencyMS: 12, LatencyKnown: true, QuoteAgeKnown: true,
		TickKnown: true, DepthKnown: true, FeeKnown: true,
		CapacityCurve: []CapacityPoint{{Size: 1, Cost: .40, Fee: .01, PayoutFloor: .50, NetFloor: .09}},
		Inputs:        map[string]any{"book_complete": true},
	}
}

func registerR138EvidenceInstrument(t *testing.T, st *Store, eventID, venue, ticker string) (int, string, int) {
	t.Helper()
	payoffID := eventID + "|fixture-payoff"
	_, err := st.RegisterCanonicalBatch(context.Background(), []CanonicalEventSpec{{
		EventID: eventID, EventType: "fixture", Domain: "test", Title: "fixture event",
		SourceArtifact: "test fixture", SourceClockID: "fixture-clock", OutcomeSetStatus: "unknown",
	}}, []CanonicalPayoffSpec{{PayoffID: payoffID, EventID: eventID, Label: "fixture YES",
		PredicateJSON: `{"fixture":true}`, PayoutFloor: 0, PayoutCeiling: 1,
		SourceArtifact: "test fixture", IdentityStatus: "verified"}}, []CanonicalInstrumentSpec{{
		Venue: venue, Ticker: ticker, EventID: eventID, PayoffID: payoffID, NativeSide: "YES",
		Orientation: "same", RulesArtifact: "test fixture", IdentityStatus: "verified",
	}})
	if err != nil {
		t.Fatal(err)
	}
	var eventVersion, payoffVersion int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM research_event_specs WHERE event_id=?`, eventID).Scan(&eventVersion); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT MAX(version) FROM research_payoff_specs WHERE payoff_id=?`, payoffID).Scan(&payoffVersion); err != nil {
		t.Fatal(err)
	}
	return eventVersion, payoffID, payoffVersion
}

func TestR138EvidenceCandidateGateAuthorityAndRouteMirror(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	row := r138EvidenceFixture("candidate", true)
	row.Observed = time.Date(2026, 7, 25, 12, 0, 0, 100_000_000, time.UTC)
	row.DecisionAt = row.Observed.Add(12 * time.Millisecond)
	row.EventVersion, row.CanonicalPayoffID, row.PayoffVersion = registerR138EvidenceInstrument(t, st,
		row.CanonicalEventID, row.Venue, row.Ticker)
	id, inserted, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil || !inserted || id <= 0 {
		t.Fatalf("id=%d inserted=%v err=%v", id, inserted, err)
	}
	funded, paper, live, err := st.R138ObservationAuthorityCounts(ctx)
	if err != nil || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("authority=%d/%d/%d err=%v", funded, paper, live, err)
	}
	var routes, candidates int
	if err := st.db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(decision='candidate'),0)
FROM research_route_opportunities WHERE system_name='deadline-hazard-surface'`).Scan(&routes, &candidates); err != nil {
		t.Fatal(err)
	}
	if routes != 1 || candidates != 1 {
		t.Fatalf("route mirror routes=%d candidates=%d", routes, candidates)
	}
	var observationObserved, observationDecision, routeObserved, routeDecision string
	var routeLatency float64
	if err := st.db.QueryRow(`SELECT o.observed_ts,o.decision_ts,r.observed_ts,r.decision_ts,
r.decision_latency_ms FROM research_system_observations o
JOIN research_route_opportunities r ON json_extract(r.evidence_json,'$.system_observation_id')=o.id
WHERE o.id=?`, id).Scan(&observationObserved, &observationDecision, &routeObserved,
		&routeDecision, &routeLatency); err != nil {
		t.Fatal(err)
	}
	if observationObserved != row.Observed.Format(time.RFC3339Nano) ||
		observationDecision != row.DecisionAt.Format(time.RFC3339Nano) ||
		routeObserved != observationObserved || routeDecision != observationDecision ||
		routeLatency != 12 {
		t.Fatalf("trigger/decision clocks diverged: observation=%s/%s route=%s/%s latency=%v",
			observationObserved, observationDecision, routeObserved, routeDecision, routeLatency)
	}
	var latencyMS float64
	var latencyKnown, quoteAgeKnown, tickKnown, depthKnown, feeKnown int
	if err := st.db.QueryRow(`SELECT decision_latency_ms,latency_known,quote_age_known,tick_known,depth_known,fee_known
FROM research_system_observations WHERE id=?`, id).Scan(&latencyMS, &latencyKnown, &quoteAgeKnown,
		&tickKnown, &depthKnown, &feeKnown); err != nil {
		t.Fatal(err)
	}
	if latencyMS != 12 || latencyKnown != 1 || quoteAgeKnown != 1 || tickKnown != 1 || depthKnown != 1 || feeKnown != 1 {
		t.Fatalf("system evidence lost money-truth flags: latency=%v flags=%d/%d/%d/%d/%d",
			latencyMS, latencyKnown, quoteAgeKnown, tickKnown, depthKnown, feeKnown)
	}
	bad := row
	bad.OpportunityID = "bad-unverified"
	bad.CertificateStatus = "unverified"
	if _, _, err := st.InsertResearchSystemObservation(ctx, bad); err == nil {
		t.Fatal("unverified candidate bypassed gate")
	}
	bad = row
	bad.OpportunityID = "bad-net"
	bad.NetLower = 1
	if _, _, err := st.InsertResearchSystemObservation(ctx, bad); err == nil {
		t.Fatal("rosier-than-payoff net envelope bypassed gate")
	}
	bad = row
	bad.OpportunityID = "bad-decision-clock"
	bad.DecisionAt = bad.Observed.Add(20 * time.Millisecond)
	if _, _, err := st.InsertResearchSystemObservation(ctx, bad); err == nil ||
		!strings.Contains(err.Error(), "contradicts measured latency") {
		t.Fatalf("contradictory decision clock accepted: %v", err)
	}
	for name, mutate := range map[string]func(*ResearchSystemObservation){
		"latency":        func(v *ResearchSystemObservation) { v.LatencyKnown = false },
		"quote-age-flag": func(v *ResearchSystemObservation) { v.QuoteAgeKnown = false },
		"tick-flag":      func(v *ResearchSystemObservation) { v.TickKnown = false },
		"depth-flag":     func(v *ResearchSystemObservation) { v.DepthKnown = false },
		"fee-flag":       func(v *ResearchSystemObservation) { v.FeeKnown = false },
		"clock":          func(v *ResearchSystemObservation) { v.SourceClockID = "" },
		"book":           func(v *ResearchSystemObservation) { v.BookSource = "" },
		"fee":            func(v *ResearchSystemObservation) { v.FeeSource = "" },
		"tick":           func(v *ResearchSystemObservation) { v.TickMin = 0 },
		"depth":          func(v *ResearchSystemObservation) { v.VisibleCapacity = .5 },
	} {
		bad = row
		bad.OpportunityID = "bad-" + name
		mutate(&bad)
		if _, _, err := st.InsertResearchSystemObservation(ctx, bad); err == nil {
			t.Fatalf("candidate with unknown %s bypassed gate", name)
		}
	}
	if _, err := st.db.Exec(`UPDATE research_system_observations SET candidate=0 WHERE id=?`, id); err == nil {
		t.Fatal("append-only observation allowed update")
	}
}

func TestR174LegacyBlankDecisionClockReplaysAsObservedWithoutBackfill(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	row := r138EvidenceFixture("negative", false)
	row.OpportunityID = "legacy-blank-decision"
	row.Observed = time.Date(2026, 7, 25, 12, 0, 0, 100_000_000, time.UTC)
	row.DecisionAt = time.Time{}
	row.EventVersion, row.CanonicalPayoffID, row.PayoffVersion = registerR138EvidenceInstrument(t,
		st, row.CanonicalEventID, row.Venue, row.Ticker)
	id, inserted, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil || !inserted {
		t.Fatalf("legacy fixture inserted=%v err=%v", inserted, err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER research_system_observations_no_update;
UPDATE research_system_observations SET decision_ts='' WHERE id=?;
CREATE TRIGGER research_system_observations_no_update
BEFORE UPDATE ON research_system_observations
BEGIN SELECT RAISE(ABORT,'append-only research system observation'); END;`, id); err != nil {
		t.Fatal(err)
	}
	replayedID, inserted, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil || inserted || replayedID != id {
		t.Fatalf("legacy blank clock replay id=%d/%d inserted=%v err=%v",
			id, replayedID, inserted, err)
	}
	var decision string
	if err := st.db.QueryRow(`SELECT decision_ts FROM research_system_observations
WHERE id=?`, id).Scan(&decision); err != nil || decision != "" {
		t.Fatalf("startup/write path rewrote legacy decision clock=%q err=%v", decision, err)
	}
}

func TestR144ResearchSystemCollectionTotalsTrackInsertsWithoutLedgerScan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	row := r138EvidenceFixture("candidate", true)
	row.EventVersion, row.CanonicalPayoffID, row.PayoffVersion = registerR138EvidenceInstrument(t, st,
		row.CanonicalEventID, row.Venue, row.Ticker)
	id, inserted, insertErr := st.InsertResearchSystemObservation(ctx, row)
	if insertErr != nil || !inserted {
		t.Fatalf("economic insert=%v err=%v", inserted, insertErr)
	}
	control := row
	control.OpportunityID = "observer-control"
	control.Kind, control.Candidate, control.Route = "control", false, "observer"
	control.CanonicalEventID, control.EventVersion = "", 0
	control.CanonicalPayoffID, control.PayoffVersion = "", 0
	control.Ticker, control.CertificateStatus, control.CertificateHash = "", "not_applicable", ""
	control.Size, control.Cost, control.Fee, control.VisibleCapacity = 0, 0, 0, 0
	control.SourceClockID, control.BookSource, control.FeeSource = "", "", ""
	control.LatencyKnown, control.QuoteAgeKnown, control.TickKnown = false, false, false
	control.DepthKnown, control.FeeKnown = false, false
	if _, inserted, insertErr := st.InsertResearchSystemObservation(ctx, control); insertErr != nil || !inserted {
		t.Fatalf("control insert=%v err=%v", inserted, insertErr)
	}
	var economics, candidates, controls, open int
	if err := st.db.QueryRow(`SELECT economic_observations,candidates,controls,open_economic
FROM research_system_collection_totals WHERE system_id=?`, row.SystemID).Scan(
		&economics, &candidates, &controls, &open); err != nil {
		t.Fatal(err)
	}
	if economics != 1 || candidates != 1 || controls != 1 || open != 1 {
		t.Fatalf("materialized totals=%d/%d/%d/%d want 1/1/1/1", economics, candidates, controls, open)
	}
	realized := .20
	update := ResearchPayoffUpdate{ObservationID: id, Observed: time.Now().UTC(), Status: "settled",
		PayoutLower: 1, PayoutUpper: 1, RealizedNet: &realized,
		SourceArtifact: "fixture settlement", SourceHash: strings.Repeat("b", 64)}
	if inserted, updateErr := st.AppendResearchPayoffUpdate(ctx, update); updateErr != nil || !inserted {
		t.Fatalf("terminal update=%v err=%v", inserted, updateErr)
	}
	if err := st.db.QueryRow(`SELECT open_economic FROM research_system_collection_totals
WHERE system_id=?`, row.SystemID).Scan(&open); err != nil {
		t.Fatal(err)
	}
	if open != 0 {
		t.Fatalf("terminal update left materialized open=%d", open)
	}
	if inserted, updateErr := st.AppendResearchPayoffUpdate(ctx, update); updateErr != nil || inserted {
		t.Fatalf("duplicate terminal update=%v err=%v", inserted, updateErr)
	}
	if err := st.db.QueryRow(`SELECT open_economic FROM research_system_collection_totals
WHERE system_id=?`, row.SystemID).Scan(&open); err != nil || open != 0 {
		t.Fatalf("duplicate terminal changed open=%d err=%v", open, err)
	}
	compact, err := st.ResearchSystemCollectionStatsCompact(ctx)
	if err != nil {
		t.Fatal(err)
	}
	compactFound := false
	for _, stat := range compact {
		if stat.SystemID == row.SystemID {
			compactFound = true
			if stat.EconomicObservations != 1 || stat.Candidates != 1 || stat.Controls != 1 || stat.Open != 0 {
				t.Fatalf("compact stat=%+v", stat)
			}
		}
	}
	if !compactFound {
		t.Fatalf("system %q missing from compact stats", row.SystemID)
	}
	stats, err := st.ResearchSystemCollectionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, stat := range stats {
		if stat.SystemID == row.SystemID {
			if stat.EconomicObservations != 1 || stat.Candidates != 1 || stat.Controls != 1 || stat.Open != 0 {
				t.Fatalf("public stat=%+v", stat)
			}
			return
		}
	}
	t.Fatalf("system %q missing from collection stats", row.SystemID)
}

func TestR144ResearchSystemCollectionTotalsCancellationResumeAndConcurrentInsert(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	eventID, venue, ticker := "event:bulk-bootstrap", "kalshi", "KXBULKBOOT"
	eventVersion, payoffID, payoffVersion := registerR138EvidenceInstrument(t, st, eventID, venue, ticker)

	// A production-shaped majority of observer controls plus a smaller executable subset makes
	// this large enough to catch the old CASE-over-every-row regression without making the unit
	// suite carry a multi-million-row fixture.
	if _, err := st.db.Exec(`WITH RECURSIVE n(x) AS (
  VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<60000
)
INSERT INTO research_system_observations(
 observed_ts,decision_ts,observed_slot,system_id,opportunity_id,observation_kind,canonical_event_id,event_version,
 canonical_payoff_id,payoff_version,instrument_version,venue,ticker,route,certificate_status,
 source_clock_id,book_source,fee_source,size_units,executable_cost,latency_known,quote_age_known,
 tick_known,depth_known,fee_known,candidate)
SELECT '2026-07-14T00:00:00Z','2026-07-14T00:00:00Z','slot-'||x,'bulk-'||(x%17),'opp-'||x,
 CASE WHEN x%5=0 THEN 'candidate' ELSE 'control' END,
 CASE WHEN x%5=0 THEN ? ELSE '' END,
 CASE WHEN x%5=0 THEN ? ELSE 0 END,
 CASE WHEN x%5=0 THEN ? ELSE NULL END,
 CASE WHEN x%5=0 THEN ? ELSE NULL END,
 CASE WHEN x%5=0 THEN 1 ELSE 0 END,
 CASE WHEN x%5=0 THEN ? ELSE '' END,
 CASE WHEN x%5=0 THEN ? ELSE '' END,
 CASE WHEN x%5=0 THEN 'taker' ELSE 'observer' END,
 'not_applicable',
 CASE WHEN x%5=0 THEN 'clock' ELSE '' END,
 CASE WHEN x%5=0 THEN 'book' ELSE '' END,
 CASE WHEN x%5=0 THEN 'fee' ELSE '' END,
 CASE WHEN x%5=0 THEN 1 ELSE 0 END,
 CASE WHEN x%5=0 THEN .4 ELSE 0 END,
 CASE WHEN x%5=0 THEN 1 ELSE 0 END,
 CASE WHEN x%5=0 THEN 1 ELSE 0 END,
 CASE WHEN x%5=0 THEN 1 ELSE 0 END,
 CASE WHEN x%5=0 THEN 1 ELSE 0 END,
 CASE WHEN x%5=0 THEN 1 ELSE 0 END,
 CASE WHEN x%10=0 THEN 1 ELSE 0 END
FROM n`, eventID, eventVersion, payoffID, payoffVersion, venue, ticker); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO research_system_payoff_updates(
 observation_id,observed_ts,status,payout_lower,payout_upper,realized_net,source_artifact,
 source_hash,reason,funded,paper_authority,live_authority)
SELECT id,'2026-07-14T01:00:00Z','settled',1,1,.6,'bulk fixture','hash-'||id,'',0,0,0
FROM research_system_observations WHERE route='taker' AND id%7=0`); err != nil {
		t.Fatal(err)
	}

	// Hold the writer lock past a short context deadline. The aborted recount must roll back in
	// full, leave the latch absent, and remain safe to retry on the same Store.
	blocker, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	cancelCtx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	err = st.ensureR144ResearchSystemCollectionTotals(cancelCtx)
	cancel()
	_ = blocker.Rollback()
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked bootstrap err=%v, want context cancellation", err)
	}
	var latched int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM kv WHERE k='r144_research_system_collection_totals_v1'`).Scan(&latched); err != nil {
		t.Fatal(err)
	}
	if latched != 0 {
		t.Fatal("canceled bootstrap committed its latch")
	}

	// Race ordinary trigger-maintained writes with the retry. Whether each insert lands just before
	// the recount lock or just after its commit, transaction serialization must count it once.
	insertErr := make(chan error, 1)
	go func() {
		for i := 0; i < 100; i++ {
			_, err := st.db.Exec(`INSERT INTO research_system_observations(
 observed_ts,decision_ts,observed_slot,system_id,opportunity_id,observation_kind,canonical_event_id,event_version,
 canonical_payoff_id,payoff_version,instrument_version,venue,ticker,route,certificate_status,
 source_clock_id,book_source,fee_source,size_units,executable_cost,latency_known,quote_age_known,
 tick_known,depth_known,fee_known,candidate)
VALUES('2026-07-14T02:00:00Z','2026-07-14T02:00:00Z',?,'concurrent','concurrent-'||?,'candidate',?,?,?,?,1,?,?,'taker',
 'not_applicable','clock','book','fee',1,.4,1,1,1,1,1,1)`, fmt.Sprintf("concurrent-%d", i), i,
				eventID, eventVersion, payoffID, payoffVersion, venue, ticker)
			if err != nil {
				insertErr <- err
				return
			}
		}
		insertErr <- nil
	}()
	start := time.Now()
	if err := st.ensureR144ResearchSystemCollectionTotals(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-insertErr; err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("selective 60k-row bootstrap took %s", elapsed)
	}

	type totals struct{ economics, candidates, controls, open int }
	want := map[string]totals{}
	const truth = `o.route!='observer' AND TRIM(o.source_clock_id)!='' AND
TRIM(o.book_source)!='' AND TRIM(o.fee_source)!='' AND o.size_units>0 AND o.executable_cost>0 AND
o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1`
	rows, err := st.db.Query(`SELECT o.system_id,
COUNT(CASE WHEN ` + truth + ` THEN 1 END),
COALESCE(SUM(o.candidate=1 AND ` + truth + `),0),
COALESCE(SUM(o.observation_kind='control'),0),
COALESCE(SUM(o.outcome_status='open' AND ` + truth + ` AND NOT EXISTS(
 SELECT 1 FROM research_system_payoff_updates u WHERE u.observation_id=o.id)),0)
FROM research_system_observations o GROUP BY o.system_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var systemID string
		var v totals
		if err := rows.Scan(&systemID, &v.economics, &v.candidates, &v.controls, &v.open); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		want[systemID] = v
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	got := map[string]totals{}
	rows, err = st.db.Query(`SELECT system_id,economic_observations,candidates,controls,open_economic
FROM research_system_collection_totals`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var systemID string
		var v totals
		if err := rows.Scan(&systemID, &v.economics, &v.candidates, &v.controls, &v.open); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		got[systemID] = v
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("materialized totals mismatch\ngot=%v\nwant=%v", got, want)
	}

	// The latch makes every later call a no-op; a second call cannot count either the baseline or
	// the concurrently inserted rows twice.
	if err := st.ensureR144ResearchSystemCollectionTotals(context.Background()); err != nil {
		t.Fatal(err)
	}
	var concurrent totals
	if err := st.db.QueryRow(`SELECT economic_observations,candidates,controls,open_economic
FROM research_system_collection_totals WHERE system_id='concurrent'`).Scan(
		&concurrent.economics, &concurrent.candidates, &concurrent.controls, &concurrent.open); err != nil {
		t.Fatal(err)
	}
	if concurrent != (totals{economics: 100, candidates: 100, open: 100}) {
		t.Fatalf("concurrent inserts counted incorrectly after no-op retry: %+v", concurrent)
	}
}

func TestR139VerifiedDeadlineFilterDoesNotScanUnrelatedRelationUniverse(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := st.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO research_event_specs(event_id,version,spec_hash,domain,event_type,
created_ts) VALUES('event:bulk',1,?,'test','binary',?)`, strings.Repeat("e", 64), now); err != nil {
		t.Fatal(err)
	}
	for _, payoff := range []string{"left", "right"} {
		if _, err = tx.Exec(`INSERT INTO research_payoff_specs(payoff_id,version,spec_hash,event_id,event_version,
label,identity_status,created_ts) VALUES(?,1,?,'event:bulk',1,?,'structural',?)`,
			"payoff:"+payoff, fmt.Sprintf("%064s", payoff), payoff, now); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(`INSERT INTO research_instrument_specs(venue,ticker,version,spec_hash,event_id,
event_version,payoff_id,payoff_version,identity_status,created_ts) VALUES('kalshi',?,1,?,
'event:bulk',1,?,1,'structural',?)`, "KX"+strings.ToUpper(payoff),
			fmt.Sprintf("%064s", "instrument-"+payoff), "payoff:"+payoff, now); err != nil {
			t.Fatal(err)
		}
	}
	stmt, err := tx.Prepare(`INSERT INTO research_payoff_relations(relation_id,version,spec_hash,event_id,
event_version,left_payoff_id,left_payoff_version,right_payoff_id,right_payoff_version,relation_type,
identity_status,created_ts) VALUES(?,1,?,'event:bulk',1,'payoff:left',1,'payoff:right',1,
'exhaustive_with','structural',?)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		if _, err = stmt.Exec(fmt.Sprintf("relation:%05d", i), fmt.Sprintf("%064d", i+1), now); err != nil {
			t.Fatal(err)
		}
	}
	_ = stmt.Close()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	rows, err := st.VerifiedDeadlineRelations(ctx, "kalshi", 100)
	if err != nil || len(rows) != 0 {
		t.Fatalf("unrelated structural universe delayed/contaminated deadline scan: rows=%d err=%v", len(rows), err)
	}
}

func TestR138CensorSafePayoffUpdateMirrorsRouteGrade(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	row := r138EvidenceFixture("negative", false)
	row.EventVersion, row.CanonicalPayoffID, row.PayoffVersion = registerR138EvidenceInstrument(t, st,
		row.CanonicalEventID, row.Venue, row.Ticker)
	row.OpportunityID = "censor"
	row.CertificateStatus = "structural"
	row.Blocker = "void payoff unresolved"
	row.PayoutLower, row.PayoutUpper = 0, 1
	row.NetLower, row.NetUpper = -.41, .59
	row.CapacityCurve[0].PayoutFloor, row.CapacityCurve[0].NetFloor = 0, -.41
	id, _, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{ObservationID: id,
		Status: "censored", PayoutLower: .5, PayoutUpper: .5, SourceArtifact: "timeout",
		SourceHash: strings.Repeat("b", 64)}); err == nil {
		t.Fatal("censored row collapsed to a fabricated point")
	}
	inserted, err := st.AppendResearchPayoffUpdate(ctx, ResearchPayoffUpdate{ObservationID: id,
		Status: "censored", PayoutLower: 0, PayoutUpper: 1, SourceArtifact: "authoritative source unavailable",
		SourceHash: strings.Repeat("c", 64), Reason: "still unresolved"})
	if err != nil || !inserted {
		t.Fatalf("payoff update inserted=%v err=%v", inserted, err)
	}
	var grades int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_route_events WHERE event_type='grade'`).Scan(&grades); err != nil || grades != 1 {
		t.Fatalf("route grades=%d err=%v", grades, err)
	}
}

func TestR138OutcomeMembershipStoresUnchangedAndChanges(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Minute)
	base := OutcomeSetFrame{Observed: now, Venue: "kalshi", EventID: "E", SourceArtifact: "official event",
		Members: []OutcomeSetMember{{Ticker: "A", Status: "active", Bid: .4, Ask: .42, Depth: 5},
			{Ticker: "B", Status: "active", Bid: .5, Ask: .52, Depth: 5}}, MutuallyExclusive: true}
	r, err := st.InsertOutcomeSetFrame(ctx, base)
	if err != nil || !r.Inserted || r.ChangeClass != "baseline" {
		t.Fatalf("baseline=%+v err=%v", r, err)
	}
	base.Observed = now.Add(5 * time.Minute)
	r, err = st.InsertOutcomeSetFrame(ctx, base)
	if err != nil || !r.Inserted || r.ChangeClass != "unchanged" {
		t.Fatalf("unchanged=%+v err=%v", r, err)
	}
	base.Observed = now.Add(10 * time.Minute)
	base.Members[1].Status = "withdrawn"
	r, err = st.InsertOutcomeSetFrame(ctx, base)
	if err != nil || !r.Inserted || r.ChangeClass != "status_change" {
		t.Fatalf("status change=%+v err=%v", r, err)
	}
}

func TestR138MicrostructureFramesPreserveExactFeesAndTransition(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	frame := ResearchMicrostructureFrame{Observed: now, Received: now.Add(10 * time.Millisecond),
		SourceChannel: "orderbook_delta", SourceGeneration: 1, SourceSubscriptionID: 7, SourceSequence: 10,
		Ticker: "KX", BookSource: "ws", FeeSource: "quadratic",
		QuoteAge: .1, TickSize: .001, YesBid: .40, YesAsk: .42, YesBidDepth: 10, YesAskDepth: 10,
		BidLevels: []ResearchBookLevel{{Price: .40, Size: 10}}, AskLevels: []ResearchBookLevel{{Price: .42, Size: 10}},
		YesTakerFee: .01, NoTakerFee: .01, YesMakerFee: 0, NoMakerFee: 0}
	_, transition, inserted, err := st.InsertResearchMicrostructureFrame(ctx, frame)
	if err != nil || !inserted || transition != "baseline" {
		t.Fatalf("baseline inserted=%v transition=%s err=%v", inserted, transition, err)
	}
	frame.Observed = now.Add(time.Second)
	frame.Received = frame.Observed.Add(10 * time.Millisecond)
	frame.SourceSequence++
	frame.YesAskDepth = 2
	frame.AskLevels[0].Size = 2
	_, transition, inserted, err = st.InsertResearchMicrostructureFrame(ctx, frame)
	if err != nil || !inserted || transition != "ask_depletion" {
		t.Fatalf("depletion inserted=%v transition=%s err=%v", inserted, transition, err)
	}
	var funded, paper, live int
	if err := st.db.QueryRow(`SELECT COALESCE(SUM(funded),0),COALESCE(SUM(paper_authority),0),COALESCE(SUM(live_authority),0)
FROM research_microstructure_frames`).Scan(&funded, &paper, &live); err != nil || funded+paper+live != 0 {
		t.Fatalf("micro authority=%d/%d/%d err=%v", funded, paper, live, err)
	}
	total, today, err := st.ResearchMicrostructureFrameCounts(ctx, now)
	if err != nil || total != 2 || today != 2 {
		t.Fatalf("durable counts total=%d today=%d err=%v", total, today, err)
	}
}

func TestR138Step8BlockedAdaptersAreFirstClassEvidence(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{"nws-hourly-gridpoint", "deribit-option-summary"} {
		if _, ok := R138Step8SourceClockSpec(source); !ok {
			t.Fatalf("missing source-clock contract %s", source)
		}
	}
	inserted, err := st.InsertOfficialReleaseFrame(ctx, OfficialReleaseFrame{SourceID: "noaa-nbm",
		ArtifactID: "adapter-status", SchemaVersion: "nbm-publication-v1", ClockStatus: "blocked",
		ArtifactHash: strings.Repeat("d", 64), Values: map[string]any{"required": "model_run"},
		Blocker: "validated GRIB2 decoder missing"})
	if err != nil || !inserted {
		t.Fatalf("blocked insert=%v err=%v", inserted, err)
	}
	var blocked, authority int
	if err := st.db.QueryRow(`SELECT COUNT(*),COALESCE(SUM(funded+paper_authority+live_authority),0)
FROM research_official_release_frames WHERE clock_status='blocked'`).Scan(&blocked, &authority); err != nil || blocked != 1 || authority != 0 {
		t.Fatalf("blocked=%d authority=%d err=%v", blocked, authority, err)
	}
}

func TestR139AdapterUpgradesPreserveExactR138ContractsOnFreshDatabase(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		collectorID string
		versions    int
		maxVersion  int
	}{{"polyus-incentive-economics", 2, 2}, {"official-release-adapters", 3, 3}} {
		var versions, minVersion, maxVersion int
		if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*),MIN(version),MAX(version)
FROM research_collector_specs WHERE collector_id=?`, tc.collectorID).Scan(&versions, &minVersion, &maxVersion); err != nil {
			t.Fatal(err)
		}
		if versions != tc.versions || minVersion != 1 || maxVersion != tc.maxVersion {
			t.Fatalf("collector %s versions=%d range=%d..%d", tc.collectorID, versions, minVersion, maxVersion)
		}
	}
	var versions, minVersion, maxVersion int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*),MIN(version),MAX(version)
FROM research_source_clock_specs WHERE source_id='deribit-option-summary'`).
		Scan(&versions, &minVersion, &maxVersion); err != nil {
		t.Fatal(err)
	}
	if versions != 3 || minVersion != 1 || maxVersion != 3 {
		t.Fatalf("Deribit source-clock versions=%d range=%d..%d", versions, minVersion, maxVersion)
	}
	var v1Schema, v2Schema, v3Schema string
	if err := st.db.QueryRowContext(ctx, `SELECT
MAX(CASE WHEN version=1 THEN schema_version ELSE '' END),
MAX(CASE WHEN version=2 THEN schema_version ELSE '' END),
MAX(CASE WHEN version=3 THEN schema_version ELSE '' END)
FROM research_source_clock_specs WHERE source_id='deribit-option-summary'`).Scan(&v1Schema, &v2Schema, &v3Schema); err != nil {
		t.Fatal(err)
	}
	if v1Schema != "deribit-option-summary-blocked-v1" || v2Schema != "deribit-option-summary-v2" ||
		v3Schema != "deribit-option-threshold-surface-v3" {
		t.Fatalf("Deribit schemas v1=%q v2=%q v3=%q", v1Schema, v2Schema, v3Schema)
	}
}

func TestR139SystemFunnelSpecsAreDedicatedAndTruthful(t *testing.T) {
	specs := R139SystemFunnelSpecs()
	if len(specs) != 12 {
		t.Fatalf("system-specific funnel specs=%d want 12", len(specs))
	}
	seenSystems := map[string]bool{}
	for _, spec := range specs {
		if spec.ExperimentID == "" || len(spec.Systems) != 1 || spec.Systems[0] != spec.ExperimentID {
			t.Fatalf("non-dedicated system funnel: %+v", spec)
		}
		if seenSystems[spec.ExperimentID] {
			t.Fatalf("duplicate system funnel %s", spec.ExperimentID)
		}
		seenSystems[spec.ExperimentID] = true
	}
	for _, id := range []string{"attention-spillover-graph", "clientele-clock-basis",
		"collateral-release-rotation", "deadline-hazard-surface", "proper-score-executor",
		"forecast-persona-router", "semantic-complexity-premium", "paired-bridge-inversion",
		"series-roll-anchor", "side-normalized-crowding-fade", "settlement-latency-carry",
		"identity-challenged-cross-venue-lock"} {
		if !seenSystems[id] {
			t.Fatalf("missing dedicated system funnel %s", id)
		}
	}
	for _, spec := range specs {
		if (spec.ExperimentID == "collateral-release-rotation" || spec.ExperimentID == "settlement-latency-carry") &&
			(!strings.Contains(spec.Source, "grade-close") || !strings.Contains(spec.Source, "credit")) {
			t.Fatalf("credit-time overclaim returned for %s: %s", spec.ExperimentID, spec.Source)
		}
		if spec.ExperimentID == "clientele-clock-basis" && !strings.Contains(spec.Source, "no venue-local") {
			t.Fatalf("clientele clock overclaim returned: %s", spec.Source)
		}
		if spec.ExperimentID == "attention-spillover-graph" && !strings.Contains(spec.Source, "no parent-child graph") {
			t.Fatalf("attention graph overclaim returned: %s", spec.Source)
		}
	}
}

func TestR139SystemFunnelQueriesAreSchemaValidOnFreshDatabase(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for _, spec := range R139SystemFunnelSpecs() {
		rows, err := st.ResearchSystemFunnelInputs(ctx, spec.ExperimentID, 20)
		if err != nil {
			t.Fatalf("%s source query: %v", spec.ExperimentID, err)
		}
		if len(rows) != 0 {
			t.Fatalf("fresh database returned %d %s inputs", len(rows), spec.ExperimentID)
		}
	}
}

func TestR139CollectionStatsDoNotDoubleCountDedicatedObservationRows(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	spec := R139SystemFunnelSpecs()[0]
	now := time.Now().UTC()
	if _, err := st.InsertResearchSystemRuntimeReceipt(ctx, ResearchSystemRuntimeReceipt{Observed: now,
		SystemID: spec.ExperimentID, State: "COLLECTING_PARTIAL", Reason: "source-native control only",
		CollectorIDs: []string{spec.CollectorID}}); err != nil {
		t.Fatal(err)
	}
	if _, inserted, err := st.InsertResearchSystemObservation(ctx, ResearchSystemObservation{Observed: now,
		SystemID: spec.ExperimentID, OpportunityID: "one", Kind: "control", Cohort: "input",
		Venue: "kalshi", Route: "observer", CertificateStatus: "not_applicable",
		SourceClockID: spec.CollectorID, SourceArtifact: "fixture", OutcomeStatus: "open",
		Blocker: "no graph"}); err != nil || !inserted {
		t.Fatalf("observation inserted=%v err=%v", inserted, err)
	}
	economicEventVersion, economicPayoffID, economicPayoffVersion := registerR138EvidenceInstrument(t, st,
		"event", "kalshi", "KX-ECONOMIC")
	if _, inserted, err := st.InsertResearchSystemObservation(ctx, ResearchSystemObservation{Observed: now,
		SystemID: spec.ExperimentID, OpportunityID: "economic", Kind: "negative", Cohort: "route",
		CanonicalEventID: "event", EventVersion: economicEventVersion, CanonicalPayoffID: economicPayoffID,
		PayoffVersion: economicPayoffVersion, Venue: "kalshi", Ticker: "KX-ECONOMIC",
		Route: "taker", Side: "YES", CertificateStatus: "verified", SourceClockID: "kalshi-book-clock",
		SourceArtifact: "fixture", BookSource: "kalshi-book-ws", FeeSource: "kalshi-fee-v1",
		QuoteAgeMax: .1, TickMin: .01, Size: 1, Cost: .40, Fee: .01,
		PayoutLower: 0, PayoutUpper: 1, NetLower: -.41, NetUpper: .59, VisibleCapacity: 2,
		DecisionLatencyMS: 1, LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true,
		DepthKnown: true, FeeKnown: true, OutcomeStatus: "open", Blocker: "negative control"}); err != nil || !inserted {
		t.Fatalf("economic observation inserted=%v err=%v", inserted, err)
	}
	if _, err := st.InsertCollectorReceipt(ctx, CollectorReceipt{CollectorID: spec.CollectorID,
		CycleID: "fixture", ExperimentID: spec.ExperimentID, ExperimentVersion: spec.ExperimentVersion,
		Status: "healthy", Started: now, Completed: now.Add(time.Second), Eligible: 1, Attempted: 1,
		Inserted: 1, Exclusions: map[string]int{}, Source: spec.Source, SchemaVersion: spec.SchemaVersion,
		ExpectedCadence: spec.ExpectedCadence, Systems: spec.Systems}); err != nil {
		t.Fatal(err)
	}
	stats, err := st.ResearchSystemCollectionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, stat := range stats {
		if stat.SystemID == spec.ExperimentID {
			if stat.InputRows != 1 || stat.EconomicObservations != 1 || stat.InputCycles != 1 ||
				stat.Controls != 1 || stat.Open != 1 {
				t.Fatalf("double-counted collection stat: %+v", stat)
			}
			return
		}
	}
	t.Fatalf("missing system stat %s", spec.ExperimentID)
}

func TestR139EconomicObservationPinsCurrentCanonicalEventVersionOrFailsClosed(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	row := r138EvidenceFixture("negative", false)
	row.EventVersion = 0
	if _, _, err := st.InsertResearchSystemObservation(ctx, row); err == nil ||
		!strings.Contains(err.Error(), "no current canonical instrument/event/payoff version") {
		t.Fatalf("unregistered economic event did not fail closed: %v", err)
	}
	wantEvent, wantPayoff, wantPayoffVersion := registerR138EvidenceInstrument(t, st,
		row.CanonicalEventID, row.Venue, row.Ticker)
	id, inserted, err := st.InsertResearchSystemObservation(ctx, row)
	if err != nil || !inserted || id <= 0 {
		t.Fatalf("current event version insert=%v id=%d err=%v", inserted, id, err)
	}
	var gotEvent, gotPayoffVersion int
	var gotPayoff, gotTicker string
	if err := st.db.QueryRow(`SELECT event_version,canonical_payoff_id,payoff_version,ticker
FROM research_system_observations WHERE id=?`, id).Scan(&gotEvent, &gotPayoff, &gotPayoffVersion, &gotTicker); err != nil {
		t.Fatal(err)
	}
	if gotEvent != wantEvent || gotPayoff != wantPayoff || gotPayoffVersion != wantPayoffVersion || gotTicker != row.Ticker {
		t.Fatalf("identity=%d/%q/%d/%q, want %d/%q/%d/%q", gotEvent, gotPayoff, gotPayoffVersion,
			gotTicker, wantEvent, wantPayoff, wantPayoffVersion, row.Ticker)
	}
	row.OpportunityID = "missing-explicit"
	row.EventVersion = wantEvent + 100
	if _, _, err := st.InsertResearchSystemObservation(ctx, row); err == nil ||
		!strings.Contains(err.Error(), "conflicts with current canonical") {
		t.Fatalf("conflicting explicit event version did not fail closed: %v", err)
	}
}

func TestR139RuntimeAndCollectionViewsDowngradeStaleCollectorsAtReadTime(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	spec := R139SystemFunnelSpecs()[0]
	old := time.Now().UTC().Add(-20 * time.Minute)
	if _, err := st.InsertResearchSystemRuntimeReceipt(ctx, ResearchSystemRuntimeReceipt{Observed: old,
		SystemID: spec.ExperimentID, State: "COLLECTING", Reason: "fresh at observation time",
		CollectorIDs: []string{spec.CollectorID}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertCollectorReceipt(ctx, CollectorReceipt{CollectorID: spec.CollectorID,
		CycleID: "stale-fixture", ExperimentID: spec.ExperimentID, ExperimentVersion: spec.ExperimentVersion,
		Status: "healthy_empty", Started: old, Completed: old.Add(time.Second), ExpectedZero: true,
		ZeroReason: "no eligible source row", Exclusions: map[string]int{}, Source: spec.Source,
		SchemaVersion: spec.SchemaVersion, ExpectedCadence: spec.ExpectedCadence, Systems: spec.Systems}); err != nil {
		t.Fatal(err)
	}
	report, err := st.ResearchSystemRuntimeReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rawSystems, err := json.Marshal(report["systems"])
	if err != nil {
		t.Fatal(err)
	}
	var systems []struct {
		SystemID, State, Reason, Observed           string
		CollectorIDs, EvidenceTables, Prerequisites []string
		Funded, PaperAuthority, LiveAuthority       bool
	}
	if err := json.Unmarshal(rawSystems, &systems); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range systems {
		if row.SystemID == spec.ExperimentID {
			found = true
			if row.State != "BLOCKED" || !strings.Contains(row.Reason, "three-cadence limit") {
				t.Fatalf("stale runtime stayed green: %+v", row)
			}
		}
	}
	if !found {
		t.Fatalf("missing runtime system %s", spec.ExperimentID)
	}
	stats, err := st.ResearchSystemCollectionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range stats {
		if row.SystemID == spec.ExperimentID &&
			(row.State != "BLOCKED" || row.CollectorAlerts != 1 || !strings.Contains(row.Reason, "three-cadence limit")) {
			t.Fatalf("stale collection view stayed green: %+v", row)
		}
	}
}

func TestR144CompactCollectionUsesLatestCollectorLiveness(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	spec := R139SystemFunnelSpecs()[0]
	old := time.Now().UTC().Add(-20 * time.Minute)
	if _, err := st.InsertResearchSystemRuntimeReceipt(ctx, ResearchSystemRuntimeReceipt{Observed: old,
		SystemID: spec.ExperimentID, State: "COLLECTING", Reason: "fresh at observation time",
		CollectorIDs: []string{spec.CollectorID}}); err != nil {
		t.Fatal(err)
	}
	insertReceipt := func(cycle string, completed time.Time) {
		t.Helper()
		if _, err := st.InsertCollectorReceipt(ctx, CollectorReceipt{CollectorID: spec.CollectorID,
			CycleID: cycle, ExperimentID: spec.ExperimentID, ExperimentVersion: spec.ExperimentVersion,
			Status: "healthy_empty", Started: completed.Add(-time.Second), Completed: completed,
			ExpectedZero: true, ZeroReason: "no eligible source row", Exclusions: map[string]int{},
			Source: spec.Source, SchemaVersion: spec.SchemaVersion,
			ExpectedCadence: spec.ExpectedCadence, Systems: spec.Systems}); err != nil {
			t.Fatal(err)
		}
	}
	insertReceipt("compact-stale", old)

	find := func() ResearchSystemCollectionStat {
		t.Helper()
		stats, err := st.ResearchSystemCollectionStatsCompact(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, stat := range stats {
			if stat.SystemID == spec.ExperimentID {
				return stat
			}
		}
		t.Fatalf("missing compact system %s", spec.ExperimentID)
		return ResearchSystemCollectionStat{}
	}
	stale := find()
	if stale.State != "BLOCKED" || stale.CollectorAlerts != 1 ||
		!strings.Contains(stale.Reason, "three-cadence limit") {
		t.Fatalf("compact view hid stale latest receipt: %+v", stale)
	}

	insertReceipt("compact-fresh", time.Now().UTC())
	fresh := find()
	if fresh.State != "COLLECTING" || fresh.CollectorAlerts != 0 || fresh.Reason != "fresh at observation time" {
		t.Fatalf("compact view ignored fresh latest receipt: %+v", fresh)
	}
}

func TestR138RuntimeReceiptRejectsRegistryOnlyAndPinsAuthorityZero(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.InsertResearchSystemRuntimeReceipt(ctx, ResearchSystemRuntimeReceipt{
		SystemID: "settlement-latency-carry", State: "BLOCKED", Reason: "not implemented"}); err == nil {
		t.Fatal("blocked receipt without exact prerequisites was accepted")
	}
	inserted, err := st.InsertResearchSystemRuntimeReceipt(ctx, ResearchSystemRuntimeReceipt{
		SystemID: "settlement-latency-carry", State: "BLOCKED", Reason: "credit clock unavailable",
		Prerequisites: []string{"authoritative determination and credit timestamps"}})
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	var funded, paper, live int
	if err := st.db.QueryRow(`SELECT funded,paper_authority,live_authority FROM research_system_runtime_receipts`).
		Scan(&funded, &paper, &live); err != nil || funded+paper+live != 0 {
		t.Fatalf("authority=%d/%d/%d err=%v", funded, paper, live, err)
	}
	if _, err := st.db.Exec(`UPDATE research_system_runtime_receipts SET state='COLLECTING'`); err == nil {
		t.Fatal("runtime coverage receipt was mutable")
	}
}
