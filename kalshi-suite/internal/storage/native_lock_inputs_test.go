package storage

import (
	"context"
	"testing"
	"time"
)

func nativeLockTestStore(t *testing.T) *Store {
	t.Helper()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestR139NativeLockInputsAreProspectiveBookNativeRows(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	inserted, err := st.InsertEventBasketResult(ctx, EventBasketObservation{Observed: now,
		Venue: "kalshi", EventID: "E", Route: "buy-no-all", IdentitySource: "official fixture",
		IdentityComplete: true, MutuallyExclusive: true, PayoutLowerBound: 1,
		Legs: []BasketLeg{{Ticker: "A", Side: "NO", Ask: .40, Depth: 3, Fee: .01},
			{Ticker: "B", Side: "NO", Ask: .45, Depth: 2, Fee: .01}}})
	if err != nil || !inserted {
		t.Fatalf("basket insert=%v err=%v", inserted, err)
	}
	baskets, err := st.RecentNativeBasketInputs(ctx, now.Add(-time.Minute), 10)
	if err != nil || len(baskets) != 1 || len(baskets[0].Legs) != 2 || !baskets[0].IdentityComplete {
		t.Fatalf("baskets=%+v err=%v", baskets, err)
	}
	inserted, err = st.InsertUnitTrial(ctx, UnitTrial{OpenedTS: now, Family: "xvgap", Platform: "kalshi",
		OriginLayer: "strategy", Ticker: "A", Side: "YES", Ask: .30, FeePC: .01,
		FeeKnown: true, FeeSource: "kalshi-fee-v1", Depth: 4, QuoteSource: "kalshi_book_ws", ResolveHours: 2})
	if err != nil || !inserted {
		t.Fatalf("unit insert=%v err=%v", inserted, err)
	}
	units, err := st.RecentNativeUnitCandidates(ctx, now.Add(-time.Minute), 10)
	if err != nil || len(units) != 1 || units[0].Family != "xvgap" || units[0].Depth != 4 || units[0].FeeSource == "" {
		t.Fatalf("units=%+v err=%v", units, err)
	}
}

func TestR139NativeRouteStatsKeepOptimizationCandidatesSeparateFromTradeAuthority(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	inserted, err := st.InsertResearchRouteOpportunity(ctx, ResearchRouteOpportunity{
		OpportunityID: "fee-opp", RouteID: "fee-route", Observed: now, DecisionAt: now,
		SystemName: "fee-rounding-batch", IdentityStatus: "verified", Venue: "kalshi",
		Ticker: "T", Side: "YES", Route: "taker", Action: "control", QuoteSource: "kalshi_book_ws",
		QuoteAgeSeconds: .1, QuoteAgeKnown: true, DecisionLatencyMS: 1, LatencyKnown: true,
		TickSize: .01, TickKnown: true, ExecutablePrice: .30, ExecutableDepth: 5, DepthKnown: true,
		RequestedQty: 5, FeeAmount: .04, FeeAuthority: "kalshi-fee-v1", FeeKnown: true,
		ExpectedPayoutLow: 0, ExpectedPayoutHigh: 5, ExpectedNetLow: -1.54, ExpectedNetHigh: 3.46,
		PartialFillWorst: -1.54, Decision: "control", DecisionReason: "fee savings only",
		EvidenceJSON: `{"optimization_candidate":true,"fee_savings":0.01}`,
	})
	if err != nil || !inserted {
		t.Fatalf("route insert=%v err=%v", inserted, err)
	}
	stats, err := st.NativeRouteSystemStats(ctx, []string{"fee-rounding-batch", "time-nested-lock"})
	if err != nil {
		t.Fatal(err)
	}
	if got := stats["fee-rounding-batch"]; got.Rows != 1 || got.Candidates != 1 || got.Controls != 1 {
		t.Fatalf("fee stats=%+v", got)
	}
	var funded, paper, live, decisionCandidate int
	if err := st.DBForTest().QueryRow(`SELECT funded,paper_authority,live_authority,decision='candidate'
FROM research_route_opportunities WHERE system_name='fee-rounding-batch'`).Scan(&funded, &paper, &live, &decisionCandidate); err != nil {
		t.Fatal(err)
	}
	if funded != 0 || paper != 0 || live != 0 || decisionCandidate != 0 {
		t.Fatalf("optimization control gained authority funded/paper/live/candidate=%d/%d/%d/%d", funded, paper, live, decisionCandidate)
	}
}

func TestR139NativeCollectorBlueprintsAreDedicatedAndImmutable(t *testing.T) {
	st := nativeLockTestStore(t)
	ctx := context.Background()
	for _, id := range []string{"native-time-nested-lock", "native-joint-marginal-lock", "native-fee-rounding-batch"} {
		spec, err := st.currentCollectorSpec(ctx, id)
		if err != nil || spec.Version != 1 || len(spec.Systems) != 1 || spec.ExpectedCadence != 5*time.Minute {
			t.Fatalf("%s spec=%+v err=%v", id, spec, err)
		}
		drift := spec
		drift.Source += " drift"
		if _, err := st.RegisterCollectorSpec(ctx, drift); err == nil {
			t.Fatalf("%s immutable drift accepted", id)
		}
	}
}
