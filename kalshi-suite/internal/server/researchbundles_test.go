package server

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/payoffsolver"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestTypedBundlePaperOnlyCannotDispatchLiveAndQuoteRecheckIsExact(t *testing.T) {
	if researchBundleLiveAcceptAllowed(true, true, true) || researchBundleLiveAcceptAllowed(false, false, true) ||
		researchBundleLiveAcceptAllowed(false, true, false) || !researchBundleLiveAcceptAllowed(false, true, true) {
		t.Fatal("Paper-only/ARM/LIVE AUTO accept gate is not fail closed")
	}
	if autoComboRequiresDistinctEvents(true) || !autoComboRequiresDistinctEvents(false) {
		t.Fatal("same-event exemption escaped the exact certified-conjunction path")
	}
	good := kalshi.Quote{ID: "q", RFQID: "r", MarketTicker: "m", Status: "open",
		YesBidDollars: ".40", YesContractsFp: "1.00"}
	if _, ok := sameOpenResearchBundleQuote([]kalshi.Quote{good}, "r", "q", "m", .4); !ok {
		t.Fatal("identical open one-contract quote was not recognized")
	}
	for name, mutate := range map[string]func(*kalshi.Quote){
		"rfq":       func(q *kalshi.Quote) { q.RFQID = "other" },
		"quote":     func(q *kalshi.Quote) { q.ID = "other" },
		"market":    func(q *kalshi.Quote) { q.MarketTicker = "other" },
		"status":    func(q *kalshi.Quote) { q.Status = "accepted" },
		"price":     func(q *kalshi.Quote) { q.YesBidDollars = ".41" },
		"contracts": func(q *kalshi.Quote) { q.YesContractsFp = "1.01" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := good
			mutate(&changed)
			if _, ok := sameOpenResearchBundleQuote([]kalshi.Quote{changed}, "r", "q", "m", .4); ok {
				t.Fatalf("changed %s quote revalidated", name)
			}
		})
	}
}

func typedSolverProblem() payoffsolver.Problem {
	legs := []payoffsolver.Leg{}
	for i, ticker := range []string{"A", "B"} {
		payoff := []float64{float64(1 - i), float64(i)}
		levels := []payoffsolver.Level{{Price: .4, Quantity: 2,
			FeeQuotes: []payoffsolver.FeeQuote{{Quantity: 1, Total: .01}}}}
		legs = append(legs, payoffsolver.Leg{ID: ticker + "|YES", Venue: "kalshi", Ticker: ticker,
			Side: "YES", PayoffID: "p:" + ticker, Payoff: payoff, Levels: levels, UnwindLevels: levels,
			QuoteAgeSeconds: .1, TickSize: .01, DecisionLatencyMS: 2,
			BookSource: "kalshi_ws", SourceClockID: "g1:s1:q1:" + ticker, FeeSource: "kalshi-fee",
			UnwindBookSource: "kalshi_ws", UnwindFeeSource: "kalshi-fee"})
	}
	return payoffsolver.Problem{Certificate: payoffsolver.Certificate{CanonicalEventID: "event", EventVersion: 1,
		States: []string{"A", "B"}, RulesHash: "rules", RelationsHash: "relations", Verified: true, Complete: true},
		Legs: legs, Sizes: []float64{1}, MaxLegs: 2, MaxQuoteAgeSeconds: 1}
}

func TestRecordSolverRowsPersistsExactTypedBundleAndSurfacesMissingTruth(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	problem := typedSolverProblem()
	rows, err := payoffsolver.Evaluate(problem)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	ins, dup, err := s.recordR138SolverRows(ctx, time.Now().UTC(), "payoff-constraint-solver", "opp",
		"event", "kalshi", "fixture", "verified", "cert", "non-atomic", "fixture",
		"kalshi_ws", "kalshi-fee", problem, rows, .01, .1, 2, map[string]any{"fixture": true})
	if err != nil || ins != 1 || dup != 0 {
		t.Fatalf("inserted/duplicates=%d/%d err=%v", ins, dup, err)
	}
	bundles, legs, states, _, err := s.store.ResearchRouteBundleCounts(ctx)
	if err != nil || bundles != 1 || legs != 2 || states != 2 {
		t.Fatalf("bundle counts=%d/%d/%d err=%v", bundles, legs, states, err)
	}
	problem.Legs[1].SourceClockID = ""
	rows, _ = payoffsolver.Evaluate(problem)
	ins, dup, err = s.recordR138SolverRows(ctx, time.Now().UTC().Add(time.Second), "payoff-constraint-solver", "bad",
		"event", "kalshi", "fixture", "verified", "cert", "non-atomic", "fixture",
		"kalshi_ws", "kalshi-fee", problem, rows, .01, .1, 2, nil)
	if err == nil || ins != 0 || dup != 0 {
		t.Fatalf("missing source clock was swallowed inserted/duplicates=%d/%d err=%v", ins, dup, err)
	}
	bundles, _, _, _, _ = s.store.ResearchRouteBundleCounts(ctx)
	if bundles != 1 {
		t.Fatalf("partial bad bundle persisted; bundles=%d", bundles)
	}
}

func TestTypedBundleGradesEveryVenueLegWithoutFillClaim(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	opened := time.Now().UTC().Add(-2 * time.Hour)
	resolved := opened.Add(time.Hour)
	seedExactVenueSettlement(t, s.store, "kalshi", "KA", 1, opened.Add(-time.Hour), resolved)
	seedExactVenueSettlement(t, s.store, "polyus", "PB", 0, opened.Add(-time.Hour), resolved.Add(time.Minute))
	level := []storage.ResearchRouteBundleLevel{{Price: .40, Quantity: 2,
		FeeQuotes: []storage.ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	b := storage.ResearchRouteBundle{BundleID: "cross-bundle", SystemID: "payoff-constraint-solver", Cohort: "fixture",
		ExperimentVersion: 1, OpportunityID: "cross", CanonicalEventID: "objective:cross", EventVersion: 1,
		CertificateHash: "cert", CertificateStatus: "verified", RouteKind: "cross_venue_non_atomic", Observed: opened, Size: 1,
		Cost: .80, Fee: .02, PayoutFloor: 1, NetFloor: .18, PartialFillWorst: -.82,
		UnwindWorst: -.82, UnwindKnown: false, DecisionLatencyMS: 1, LatencyKnown: true,
		StateVectorHash: "state", Blocker: "non-atomic", Evidence: map[string]any{"actual_fill_claimed": false},
		States: []storage.ResearchRouteBundleState{{Index: 0, StateID: "A", Payout: 1}},
		Legs: []storage.ResearchRouteBundleLeg{
			{Index: 0, LegID: "KA|YES", Venue: "kalshi", Ticker: "KA", Side: "YES", PayoffID: "p:ka",
				Quantity: 1, IntegratedCost: .4, ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1,
				BookSource: "kalshi_ws", SourceClockID: "k:g1:s1:q1", FeeSource: "kalshi-fee", Levels: level,
				Payoff: []float64{1}},
			{Index: 1, LegID: "PB|NO", Venue: "polyus", Ticker: "PB", Side: "NO", PayoffID: "p:pb",
				Quantity: 1, IntegratedCost: .4, ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1,
				BookSource: "polyus_ws", SourceClockID: "p:q1", FeeSource: "polyus-fee", Levels: level,
				Payoff: []float64{1}},
		}}
	if inserted, err := s.store.InsertResearchRouteBundle(ctx, b); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	graded, invalid, waiting, err := s.settleTypedResearchBundles(ctx, time.Now().UTC())
	if err != nil || graded != 1 || invalid != 0 || waiting != 0 {
		t.Fatalf("graded/invalid/waiting=%d/%d/%d err=%v", graded, invalid, waiting, err)
	}
	var payout, net float64
	var actual, atomic, funded, paper, live int
	var evidence string
	if err := s.store.DBForTest().QueryRow(`SELECT payout,realized_net,actual_fill,atomic_fill,evidence_json,
funded,paper_authority,live_authority FROM research_route_bundle_events WHERE bundle_id='cross-bundle' AND event_type='grade'`).Scan(
		&payout, &net, &actual, &atomic, &evidence, &funded, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if math.Abs(payout-2) > 1e-9 || math.Abs(net-1.18) > 1e-9 || actual != 0 || atomic != 0 || funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("payout/net=%v/%v fills=%d/%d authority=%d/%d/%d", payout, net, actual, atomic, funded, paper, live)
	}
	var decoded map[string]any
	if json.Unmarshal([]byte(evidence), &decoded) != nil || decoded["actual_fill_claimed"] != false || decoded["atomic_fill_claimed"] != false {
		t.Fatalf("unsafe grade evidence=%s", evidence)
	}
}

func TestR143ComboLabSurfacesPositiveTypedBundleAsPaperProvisionalOnly(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	level := []storage.ResearchRouteBundleLevel{{Price: .40, Quantity: 2,
		FeeQuotes: []storage.ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}}
	b := storage.ResearchRouteBundle{BundleID: "r143-positive-bundle", SystemID: "payoff-constraint-solver", Cohort: "r143-paper",
		ExperimentVersion: 1, OpportunityID: "r143-positive", CanonicalEventID: "objective:r143", EventVersion: 1,
		CertificateHash: "cert-r143", CertificateStatus: "structural", RouteKind: "all_leg_taker", Observed: now, Size: 1,
		Cost: .80, Fee: .02, PayoutFloor: 1, NetFloor: .18, PartialFillWorst: -.82,
		UnwindWorst: -.82, UnwindKnown: false, DecisionLatencyMS: 1, LatencyKnown: true,
		StateVectorHash: "state-r143", Blocker: "void payoff not yet certified; non-atomic", Evidence: map[string]any{"actual_fill_claimed": false},
		States: []storage.ResearchRouteBundleState{{Index: 0, StateID: "A", Payout: 1}, {Index: 1, StateID: "B", Payout: 1}},
		Legs: []storage.ResearchRouteBundleLeg{
			{Index: 0, LegID: "KA|YES", Venue: "kalshi", Ticker: "KA", Side: "YES", PayoffID: "p:ka",
				Quantity: 1, IntegratedCost: .4, ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1,
				BookSource: "kalshi_ws", SourceClockID: "k:g1:s1:q1", FeeSource: "kalshi-fee", Levels: level,
				Payoff: []float64{1, 0}},
			{Index: 1, LegID: "PB|NO", Venue: "polyus", Ticker: "PB", Side: "NO", PayoffID: "p:pb",
				Quantity: 1, IntegratedCost: .4, ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1,
				BookSource: "polyus_ws", SourceClockID: "p:q1", FeeSource: "polyus-fee", Levels: level,
				Payoff: []float64{0, 1}},
		}}
	if inserted, err := s.store.InsertResearchRouteBundle(ctx, b); err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	refs := s.plabTypedPositiveBundleRefs(ctx, now.Add(time.Second))
	if len(refs) != 1 {
		t.Fatalf("typed positive refs=%+v", refs)
	}
	ref := refs[0]
	if ref.SystemID != b.SystemID || ref.Authority != "PAPER_RESEARCH_ONLY" || ref.LiveAuthority ||
		ref.CertificateStatus != "structural" || ref.Blocker == "" || ref.NetFloor <= 0 || len(ref.Legs) != 2 ||
		ref.Legs[0].BookSource == "" || ref.Legs[1].Fee <= 0 {
		t.Fatalf("unsafe or incomplete typed positive ref=%+v", ref)
	}
}
