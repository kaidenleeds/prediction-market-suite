package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR139NativeFeeBatchUsesActualAggregateVersusSeparateMath(t *testing.T) {
	rows, ok := nativeFeeBatchPoints(5, .011, func(q float64) (float64, string, bool) {
		// Fixture models a venue that rounds once for the aggregate request. This is an injected
		// authority result, not a local approximation of the production fee formula.
		return q*.011 - .003, "fixture-fee-authority", true
	})
	if !ok || len(rows) != 4 {
		t.Fatalf("rows=%+v ok=%v", rows, ok)
	}
	best, ok := bestNativeFeeBatchPoint(rows)
	if !ok || best.Quantity != 2 || math.Abs(best.SeparateFee-.022) > 1e-12 ||
		math.Abs(best.AggregateFee-.019) > 1e-12 || math.Abs(best.Savings-.003) > 1e-12 {
		t.Fatalf("best=%+v ok=%v", best, ok)
	}
	if _, ok := nativeFeeBatchPoints(1, .01, func(float64) (float64, string, bool) { return 0, "x", true }); ok {
		t.Fatal("one unit cannot create an aggregate-versus-separate cohort")
	}
}

func TestR139NativeLockCollectorsEmitHonestHealthyEmptyReceipts(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{store: st}
	now := time.Now().UTC()
	s.sweepNativeTimeNestedLock(context.Background(), now)
	s.sweepNativeJointMarginalLock(context.Background(), now)
	s.sweepNativeFeeRoundingBatch(context.Background(), now)
	for _, id := range []string{"native-time-nested-lock", "native-joint-marginal-lock", "native-fee-rounding-batch"} {
		var status, zeroReason string
		var expectedZero, funded, paper, live int
		if err := st.DBForTest().QueryRow(`SELECT status,expected_zero,zero_reason,funded,paper_authority,live_authority
FROM research_collector_receipts WHERE collector_id=? ORDER BY id DESC LIMIT 1`, id).
			Scan(&status, &expectedZero, &zeroReason, &funded, &paper, &live); err != nil {
			t.Fatal(err)
		}
		if status != "healthy_empty" || expectedZero != 1 || strings.TrimSpace(zeroReason) == "" ||
			funded != 0 || paper != 0 || live != 0 {
			t.Fatalf("%s status=%q expected=%d reason=%q authority=%d/%d/%d",
				id, status, expectedZero, zeroReason, funded, paper, live)
		}
	}
	views := s.nativeLockCollectionViews(context.Background())
	if len(views) != 3 {
		t.Fatalf("views=%+v", views)
	}
	for _, view := range views {
		if view.State != "RESEARCH_ONLY_HEALTHY_EMPTY" || view.Cycles != 1 || view.Alerts != 0 {
			t.Fatalf("view=%+v", view)
		}
	}
}

func TestR139NativeAggregatePositiveControlNeverBecomesTradeCandidate(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{store: st}
	now := time.Now().UTC()
	inserted, err := s.insertNativeAggregateRoute(context.Background(), nativeAggregateRoute{
		System: "time-nested-lock", Opportunity: "rel", EventID: "event", Venue: "kalshi",
		Identity: "verified", SourceArtifact: "immutable relation", QuoteSource: "kalshi_book_ws",
		FeeSource: "kalshi-fee-v1", Decision: "blocked", Reason: "non-atomic",
		Observed: now, DecisionAt: now, QuoteAge: .1, Tick: .01, Qty: 1, Depth: 3, Fees: .01,
		PayoutLow: 1, PayoutHigh: 2, NetLow: .10, NetHigh: 1.10, PartialWorst: -.45,
		Evidence: map[string]any{"conditional_positive": true, "atomic": false},
	})
	if err != nil || !inserted {
		t.Fatalf("insert=%v err=%v", inserted, err)
	}
	var decision string
	var funded, paper, live int
	if err := st.DBForTest().QueryRow(`SELECT decision,funded,paper_authority,live_authority
FROM research_route_opportunities WHERE system_name='time-nested-lock'`).Scan(&decision, &funded, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if decision != "blocked" || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("positive non-atomic control decision=%q authority=%d/%d/%d", decision, funded, paper, live)
	}
}

func TestR139NativePlaceholderSystemsNoLongerReportUnimplemented(t *testing.T) {
	want := map[string]bool{"time-nested-lock": false, "joint-marginal-lock": false, "fee-rounding-batch": false}
	for _, row := range researchSystemCoverageRows() {
		if _, ok := want[row.Family]; ok {
			want[row.Family] = !strings.Contains(row.State, "NOT_IMPLEMENTED") && row.ResearchOnly && !row.LiveAuthorizes
		}
	}
	for system, ok := range want {
		if !ok {
			t.Fatalf("%s still missing or unimplemented", system)
		}
	}
	s := &Server{}
	reasons := map[string]bool{}
	for _, system := range nativeLockSystemIDs {
		state, reason := s.nativeProducerState(system)
		if state != "RESEARCH_ONLY_CURRENT" || strings.Contains(state, "NOT_IMPLEMENTED") ||
			strings.Contains(strings.ToLower(reason), "generic") || strings.TrimSpace(reason) == "" {
			t.Fatalf("%s producer state=%q reason=%q", system, state, reason)
		}
		reasons[reason] = true
	}
	if len(reasons) != len(nativeLockSystemIDs) {
		t.Fatalf("native systems must have system-specific producer explanations: %+v", reasons)
	}
}

func TestR139NativeResearchSystemsAppearCompactlyInBriefing(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	s.sweepNativeTimeNestedLock(context.Background(), now)
	s.sweepNativeJointMarginalLock(context.Background(), now)
	s.sweepNativeFeeRoundingBatch(context.Background(), now)
	text := s.briefScoreboard(context.Background())
	if !strings.Contains(text, "⚙ ") || !strings.Contains(text, "n = unique settled venue+ticker contracts") ||
		(!strings.Contains(text, "302 typed variants") && !strings.Contains(text, "system count refreshing")) ||
		(!strings.Contains(text, "producer-capable") && !strings.Contains(text, "system count refreshing")) {
		t.Fatalf("briefing missing the compact cached/warming system registry receipt:\n%s", text)
	}
	// R141: one delayed native report must never become three alarming per-system report errors.
	for _, system := range nativeLockSystemIDs {
		if strings.Contains(text, famPlainName(system)+" · cycles") ||
			strings.Contains(text, famPlainName(system)+" · collector report error") {
			t.Fatalf("%s leaked a verbose or fabricated briefing alert:\n%s", system, text)
		}
	}
}
