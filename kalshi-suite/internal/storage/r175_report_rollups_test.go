package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func explainPlanText(t *testing.T, st *Store, query string, args ...any) string {
	t.Helper()
	rows, err := st.db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail)
		plan.WriteByte('\n')
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan.String()
}

func directRouteCounts(t *testing.T, st *Store) map[string]int {
	t.Helper()
	keys := []string{"routes", "candidates", "blocked", "abstains", "controls",
		"fee_authority_rows", "nonpositive_lower_bound", "identity_unverified_or_rejected",
		"complete_money_truth", "lifecycle_events", "fill_events", "cancel_events", "grade_events"}
	values := make([]int, len(keys))
	if err := st.db.QueryRow(`SELECT COUNT(*),
COALESCE(SUM(decision='candidate'),0),COALESCE(SUM(decision='blocked'),0),
COALESCE(SUM(decision='abstain'),0),COALESCE(SUM(decision='control'),0),
COALESCE(SUM(fee_authority!=''),0),COALESCE(SUM(expected_net_low<=0),0),
COALESCE(SUM(identity_status NOT IN ('verified','structural')),0),
COALESCE(SUM(quote_age_known=1 AND latency_known=1 AND tick_known=1
 AND depth_known=1 AND fee_known=1),0)
FROM research_route_opportunities`).Scan(
		&values[0], &values[1], &values[2], &values[3], &values[4], &values[5],
		&values[6], &values[7], &values[8]); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*),
COALESCE(SUM(event_type IN ('fill','partial_fill')),0),
COALESCE(SUM(event_type='cancel'),0),COALESCE(SUM(event_type='grade'),0)
FROM research_route_events`).Scan(&values[9], &values[10], &values[11], &values[12]); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]int, len(keys))
	for i, key := range keys {
		out[key] = values[i]
	}
	return out
}

func assertRouteReportMatchesLedger(t *testing.T, st *Store) {
	t.Helper()
	report, err := st.ResearchRouteReport(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	got := report["counts"].(map[string]int)
	want := directRouteCounts(t, st)
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("route report %s=%d want %d; report=%+v", key, got[key], value, report)
		}
	}
	directBuckets := map[string]map[string]int{}
	rows, err := st.db.Query(`SELECT route,decision,COUNT(*)
FROM research_route_opportunities GROUP BY route,decision ORDER BY route,decision`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var route, decision string
		var count int
		if err := rows.Scan(&route, &decision, &count); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if directBuckets[route] == nil {
			directBuckets[route] = map[string]int{}
		}
		directBuckets[route][decision] = count
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	gotBuckets := report["route_decisions"].(map[string]map[string]int)
	if !reflect.DeepEqual(gotBuckets, directBuckets) {
		t.Fatalf("route decision totals=%v want %v", gotBuckets, directBuckets)
	}
}

func TestR175ResearchRouteReportRollupBackfillAndDeleteParity(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 7, 25, 15, 0, 0, 0, time.UTC)
	rows := []ResearchRouteOpportunity{
		routeFixture("route-candidate", "taker", "candidate"),
		routeFixture("route-blocked", "maker", "blocked"),
		routeFixture("route-control", "control", "control"),
		routeFixture("route-abstain", "abstain", "abstain"),
	}
	rows[1].IdentityStatus = "unverified"
	for i := range rows {
		rows[i].Observed = base.Add(time.Duration(i) * time.Second)
		if inserted, err := st.InsertResearchRouteOpportunity(ctx, rows[i]); err != nil || !inserted {
			t.Fatalf("insert route %d=%v err=%v", i, inserted, err)
		}
	}
	for _, event := range []ResearchRouteEvent{
		{OpportunityID: "route-candidate", RouteID: "taker", EventType: "fill"},
		{OpportunityID: "route-blocked", RouteID: "maker", EventType: "cancel"},
		{OpportunityID: "route-control", RouteID: "control", EventType: "grade"},
	} {
		if _, err := st.AppendResearchRouteEvent(ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	assertRouteReportMatchesLedger(t, st)

	// Rebuild from deliberately blank summaries. The latch and both summaries commit together;
	// calling it twice cannot add the historical rows twice.
	if _, err := st.db.Exec(`DELETE FROM kv WHERE k=?`, r175RouteReportLatch); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`DELETE FROM research_route_report_totals`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`DELETE FROM research_route_decision_totals`); err != nil {
		t.Fatal(err)
	}
	if err := ensureR175ResearchRouteReportTotals(ctx, st.db); err != nil {
		t.Fatal(err)
	}
	if err := ensureR175ResearchRouteReportTotals(ctx, st.db); err != nil {
		t.Fatal(err)
	}
	assertRouteReportMatchesLedger(t, st)

	// The two sanctioned cleanup shapes temporarily remove only the public immutability guards.
	// Their AFTER DELETE mirrors must keep both opportunity and event totals exact.
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`DROP TRIGGER research_route_events_no_delete`,
		`DELETE FROM research_route_events
		  WHERE opportunity_id='route-blocked' AND route_id='maker' AND event_type='cancel'`,
		`CREATE TRIGGER research_route_events_no_delete BEFORE DELETE ON research_route_events
		  BEGIN SELECT RAISE(ABORT,'append-only research route event'); END`,
		`DROP TRIGGER research_route_opportunities_no_delete`,
		`DELETE FROM research_route_opportunities
		  WHERE opportunity_id='route-blocked' AND route_id='maker'`,
		`CREATE TRIGGER research_route_opportunities_no_delete
		  BEFORE DELETE ON research_route_opportunities BEGIN
		  SELECT RAISE(ABORT,'immutable research route opportunity'); END`,
	} {
		if _, err := tx.Exec(statement); err != nil {
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	assertRouteReportMatchesLedger(t, st)
}

func TestR175ResearchRouteBackfillSerializesConcurrentTriggerWrites(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for i := 0; i < 40; i++ {
		row := routeFixture(fmt.Sprintf("baseline-%03d", i), "taker", "candidate")
		if inserted, err := st.InsertResearchRouteOpportunity(ctx, row); err != nil || !inserted {
			t.Fatalf("baseline %d=%v err=%v", i, inserted, err)
		}
	}
	// The triggers are installed before the recount begins. Clearing the latch/tables recreates the
	// upgrade state while a normal writer races the immediate recount transaction.
	if _, err := st.db.Exec(`DELETE FROM kv WHERE k=?`, r175RouteReportLatch); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`DELETE FROM research_route_report_totals`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`DELETE FROM research_route_decision_totals`); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	insertErr := make(chan error, 1)
	go func() {
		<-start
		for i := 0; i < 40; i++ {
			row := routeFixture(fmt.Sprintf("concurrent-%03d", i), "maker", "blocked")
			if inserted, err := st.InsertResearchRouteOpportunity(ctx, row); err != nil {
				insertErr <- err
				return
			} else if !inserted {
				insertErr <- fmt.Errorf("concurrent route %d was not inserted", i)
				return
			}
		}
		insertErr <- nil
	}()
	close(start)
	if err := ensureR175ResearchRouteReportTotals(ctx, st.db); err != nil {
		t.Fatal(err)
	}
	if err := <-insertErr; err != nil {
		t.Fatal(err)
	}
	assertRouteReportMatchesLedger(t, st)
	var triggers, latch int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'
AND name IN ('research_route_report_opportunity_insert','research_route_report_event_insert')`).
		Scan(&triggers); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM kv WHERE k=?`, r175RouteReportLatch).
		Scan(&latch); err != nil {
		t.Fatal(err)
	}
	if triggers != 2 || latch != 1 {
		t.Fatalf("rollup installation/latch triggers=%d latch=%d", triggers, latch)
	}
}

func TestR175ResearchReportPlansStayOffHistoricalLedgers(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	recentPlan := explainPlanText(t, st, researchRouteRecentSQL, 100)
	if !strings.Contains(recentPlan, "idx_rroute_report_recent") ||
		strings.Contains(strings.ToUpper(recentPlan), "USE TEMP B-TREE") {
		t.Fatalf("recent route report lost exact ordered index:\n%s", recentPlan)
	}
	totalsPlan := explainPlanText(t, st, researchRouteReportTotalsSQL)
	if strings.Contains(totalsPlan, "research_route_opportunities") ||
		strings.Contains(totalsPlan, "research_route_events") ||
		!strings.Contains(totalsPlan, "research_route_report_totals") {
		t.Fatalf("route totals returned to historical ledgers:\n%s", totalsPlan)
	}
	systemPlan := explainPlanText(t, st, researchSystemEvidenceRawTotalsSQL)
	if strings.Contains(systemPlan, "research_system_observations") ||
		!strings.Contains(systemPlan, "research_system_collection_totals") {
		t.Fatalf("system evidence returned to historical observation ledger:\n%s", systemPlan)
	}
}

func TestR175SystemEvidenceRawTotalsPreserveLegacySemantics(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := r138EvidenceFixture("candidate", true)
	base.EventVersion, base.CanonicalPayoffID, base.PayoffVersion = registerR138EvidenceInstrument(
		t, st, base.CanonicalEventID, base.Venue, base.Ticker)
	fixtures := []ResearchSystemObservation{base, base, base}
	fixtures[0].OpportunityID = "raw-candidate-open"
	fixtures[1].OpportunityID, fixtures[1].Kind = "raw-negative-open", "negative"
	fixtures[1].Candidate, fixtures[1].Blocker = false, "negative research action"
	fixtures[2].OpportunityID, fixtures[2].Kind = "raw-negative-settled", "negative"
	fixtures[2].Candidate, fixtures[2].Blocker = false, "settled negative research action"
	fixtures[2].OutcomeStatus = "settled"
	for i := range fixtures {
		fixtures[i].Observed = base.Observed.Add(time.Duration(i) * time.Second)
		if _, inserted, err := st.InsertResearchSystemObservation(ctx, fixtures[i]); err != nil || !inserted {
			t.Fatalf("insert system fixture %d=%v err=%v", i, inserted, err)
		}
	}
	control := base
	control.Observed = base.Observed.Add(4 * time.Second)
	control.OpportunityID, control.Kind, control.Route = "raw-observer-control", "control", "observer"
	control.Candidate, control.Blocker = false, "observer control"
	control.CanonicalEventID, control.EventVersion = "", 0
	control.CanonicalPayoffID, control.PayoffVersion, control.InstrumentVersion = "", 0, 0
	control.Venue, control.Ticker, control.Side = "", "", ""
	control.CertificateStatus, control.CertificateHash = "not_applicable", ""
	control.SourceClockID, control.SourceArtifact, control.BookSource, control.FeeSource = "", "", "", ""
	control.QuoteAgeMax, control.TickMin, control.Size, control.Cost, control.Fee = 0, 0, 0, 0, 0
	control.VisibleCapacity, control.DecisionLatencyMS = 0, 0
	control.LatencyKnown, control.QuoteAgeKnown, control.TickKnown = false, false, false
	control.DepthKnown, control.FeeKnown = false, false
	control.CapacityCurve = nil
	control.OutcomeStatus = "open"
	if _, inserted, err := st.InsertResearchSystemObservation(ctx, control); err != nil || !inserted {
		t.Fatalf("insert observer control=%v err=%v", inserted, err)
	}

	assertSystemEvidence := func() {
		t.Helper()
		report, err := st.ResearchSystemEvidenceReport(ctx)
		if err != nil {
			t.Fatal(err)
		}
		counts := report["counts"].(map[string]int64)
		if counts["observations"] != 4 || counts["candidates"] != 1 ||
			counts["negative_controls"] != 3 || counts["open_envelopes"] != 3 {
			t.Fatalf("raw global counts=%v", counts)
		}
		wire, err := json.Marshal(report["systems"])
		if err != nil {
			t.Fatal(err)
		}
		var systems map[string]struct {
			Rows, Candidates, Negative, Controls int64
		}
		if err := json.Unmarshal(wire, &systems); err != nil {
			t.Fatal(err)
		}
		got := systems[base.SystemID]
		if got.Rows != 4 || got.Candidates != 1 || got.Negative != 2 || got.Controls != 1 {
			t.Fatalf("per-system raw totals=%+v", got)
		}
	}
	assertSystemEvidence()

	// Force the one-time migration path, then run the older R144 bootstrap that starts with DELETE.
	// Both must restore the same raw semantics and leave the newer counters intact.
	if _, err := st.db.Exec(`DELETE FROM kv WHERE k=?`, r175SystemRawTotalsLatch); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE research_system_collection_totals SET controls=0,
raw_observations=0,raw_candidates=0,raw_negative=0,raw_open_envelopes=0`); err != nil {
		t.Fatal(err)
	}
	if err := ensureR175ResearchSystemRawTotals(ctx, st.db); err != nil {
		t.Fatal(err)
	}
	assertSystemEvidence()
	if _, err := st.db.Exec(`DELETE FROM kv WHERE k='r144_research_system_collection_totals_v1'`); err != nil {
		t.Fatal(err)
	}
	if err := st.ensureR144ResearchSystemCollectionTotals(ctx); err != nil {
		t.Fatal(err)
	}
	assertSystemEvidence()
}
