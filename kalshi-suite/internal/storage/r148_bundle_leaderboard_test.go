package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func r148LeaderboardBundle(id, system, opportunity, event string, observed time.Time, venues ...string) ResearchRouteBundle {
	if len(venues) != 2 {
		venues = []string{"kalshi", "kalshi"}
	}
	level := []ResearchRouteBundleLevel{{Price: .40, Quantity: 2,
		FeeQuotes: []ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	unwind := []ResearchRouteBundleLevel{{Price: .39, Quantity: 2,
		FeeQuotes: []ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	legs := make([]ResearchRouteBundleLeg, 2)
	for i := range legs {
		legs[i] = ResearchRouteBundleLeg{Index: i, LegID: id + "-leg-" + string(rune('a'+i)),
			Venue: venues[i], Ticker: id + "-ticker-" + string(rune('a'+i)), Side: "YES",
			PayoffID: id + "-payoff-" + string(rune('a'+i)), Quantity: 1, IntegratedCost: .40,
			ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1, BookSource: "test-book",
			SourceClockID: "test-clock", FeeSource: "test-fee", Levels: level,
			Payoff: []float64{float64(1 - i), float64(i)}, UnwindKnown: true,
			UnwindBookSource: "test-unwind", UnwindFeeSource: "test-fee", UnwindLevels: unwind}
	}
	return ResearchRouteBundle{BundleID: id, SystemID: system, Cohort: "r148-test",
		OpportunityID: opportunity, CanonicalEventID: event, CertificateHash: id + "-certificate",
		CertificateStatus: "verified", RouteKind: "all_leg_taker", StateVectorHash: id + "-states",
		Blocker: "staged route", Observed: observed, ExperimentVersion: 1, EventVersion: 1,
		UnwindKnown: true, LatencyKnown: true, Size: 1, Cost: .80, Fee: .02, PayoutFloor: 1,
		NetFloor: .18, PartialFillWorst: -.40, UnwindWorst: -.04, DecisionLatencyMS: 5,
		Legs: legs, States: []ResearchRouteBundleState{{Index: 0, StateID: "a", Payout: 1},
			{Index: 1, StateID: "b", Payout: 1}}, Evidence: map[string]any{"test": true}}
}

func TestR148BundleLeaderboardUsesDistinctFirstOpportunityAndExactVenueShape(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	insert := func(b ResearchRouteBundle, net *float64) {
		t.Helper()
		if ok, err := st.InsertResearchRouteBundle(ctx, b); err != nil || !ok {
			t.Fatalf("insert %s ok=%v err=%v", b.BundleID, ok, err)
		}
		if net != nil {
			if ok, err := st.AppendResearchRouteBundleGrade(ctx, ResearchRouteBundleGrade{
				BundleID: b.BundleID, OutcomeStatus: "resolved", Reason: "test settlement",
				EvidenceJSON: `{}`, Observed: b.Observed.Add(time.Hour), Payout: b.Cost + b.Fee + *net,
				RealizedNet: *net, CapitalSeconds: 3600,
			}); err != nil || !ok {
				t.Fatalf("grade %s ok=%v err=%v", b.BundleID, ok, err)
			}
		}
	}
	plus20, minus50, plus10 := .20, -.50, .10
	insert(r148LeaderboardBundle("b1", "event-basket-lock", "opp-1", "event-1", now.Add(-4*time.Hour), "kalshi", "kalshi"), &plus20)
	// A later solver snapshot for the same opportunity must not inflate n or replace the first row.
	insert(r148LeaderboardBundle("b1-later", "event-basket-lock", "opp-1", "event-1", now.Add(-3*time.Hour), "kalshi", "kalshi"), &minus50)
	insert(r148LeaderboardBundle("b2", "event-basket-lock", "opp-2", "event-2", now.Add(-2*time.Hour), "kalshi", "kalshi"), &plus10)
	insert(r148LeaderboardBundle("b3", "event-basket-lock", "opp-3", "event-3", now.Add(-time.Hour), "kalshi", "kalshi"), nil)
	// The same upstream opportunity on a different executable venue is a different route cell.
	insert(r148LeaderboardBundle("p1", "event-basket-lock", "opp-1", "event-1", now.Add(-3*time.Hour), "polyus", "polyus"), &plus10)
	insert(r148LeaderboardBundle("x1", "xvlock", "xopp-1", "xevent-1", now.Add(-2*time.Hour), "kalshi", "polyus"), &plus10)
	structural := r148LeaderboardBundle("structural", "nested-ladder-lock", "sopp", "sevent", now.Add(-2*time.Hour), "kalshi", "kalshi")
	structural.CertificateStatus = "structural"
	insert(structural, &plus10)

	stats, err := st.ResearchRouteBundleLeaderboard(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 3 {
		t.Fatalf("rows=%d want 3: %+v", len(stats), stats)
	}
	bySystem := make(map[string]UnitTrialLeaderboardStat)
	for _, row := range stats {
		bySystem[row.Family+"@"+row.Platform] = row
	}
	k := bySystem["event-basket-lock@kalshi"]
	if k.Platform != "kalshi" || k.Side != "BUNDLE" || k.OriginLayer != "strategy" ||
		k.N != 2 || k.SettledMarkets != 2 || k.Total != 3 || k.Open != 1 || k.UniqueMarkets != 3 ||
		math.Abs(k.MeanPC-.15) > 1e-12 || math.Abs(k.TotalPnL-.30) > 1e-12 || k.SettledEventClusters != 2 {
		t.Fatalf("Kalshi bundle aggregate=%+v", k)
	}
	p := bySystem["event-basket-lock@polyus"]
	if p.N != 1 || math.Abs(p.MeanPC-.10) > 1e-12 {
		t.Fatalf("PolyUS bundle aggregate=%+v", p)
	}
	x := bySystem["xvlock@crossvenue"]
	if x.Platform != "crossvenue" || x.N != 1 || math.Abs(x.MeanPC-.10) > 1e-12 {
		t.Fatalf("cross-venue bundle aggregate=%+v", x)
	}
}
