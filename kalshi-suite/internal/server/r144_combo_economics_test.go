package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR144ComboAllInReturnCannotLoseMoreThanEntry(t *testing.T) {
	if got, ok := plabAllInReturn(0, .000001, 234398); !ok || got != -1 {
		t.Fatalf("full loss got=%v ok=%v, want exactly -1", got, ok)
	}
	got, ok := plabAllInReturn(.4, .2, 1)
	if !ok || math.Abs(got) > 1e-12 { // proceeds 2, all-in capital 2
		t.Fatalf("break-even all-in return got=%v ok=%v", got, ok)
	}
	for _, bad := range [][3]float64{{0, 0, 0}, {0, .2, -1}, {1.1, .2, 0}, {math.NaN(), .2, 0}} {
		if _, ok := plabAllInReturn(bad[0], bad[1], bad[2]); ok {
			t.Fatalf("invalid economics accepted: %v", bad)
		}
	}
}

func TestR144ComboSampleBucketUsesUniqueMarketCount(t *testing.T) {
	for _, tc := range []struct {
		n     int
		color string
	}{{0, "red"}, {10, "red"}, {11, "orange"}, {40, "orange"}, {41, "yellow"},
		{120, "yellow"}, {121, "green"}, {500, "green"}, {501, "blue"},
		{1000, "blue"}, {1001, "purple"}} {
		if color, _ := plabSampleBucket(tc.n); color != tc.color {
			t.Fatalf("n=%d color=%s want %s", tc.n, color, tc.color)
		}
	}
}

func TestR144ComboCIUsesOneMeanPerConnectedComponent(t *testing.T) {
	a := &plabAgg{
		N2: 4, Sum2: 2, Sq2: 4,
		Outcomes:           []float64{1, 1, 1, -1},
		IndependenceGroups: [][]string{{"shared"}, {"shared", "a"}, {"shared", "b"}, {"independent"}},
	}
	mean, _, _, n, _ := plabCellCI(a)
	if n != 2 || math.Abs(mean) > 1e-12 {
		t.Fatalf("connected-component CI used raw-row mean: mean=%v n=%d want mean=0 n=2", mean, n)
	}
	if _, _, _, _, ok := plabCellCI(&plabAgg{N2: 100, Sum2: 50, Sq2: 30}); ok {
		t.Fatal("aggregate-only rows fabricated a dependence-safe CI")
	}
}

func TestR144ComboMarketIdentityKeepsVenueSeparateFromResolutionDependence(t *testing.T) {
	legs := []plabLeg{
		{Platform: "kalshi", Ticker: "SAME", EventKey: "objective-final"},
		{Platform: "polyus", Ticker: "SAME", EventKey: "objective-final"},
	}
	if markets, blocks := plabMarketKeys(legs), plabIndependenceKeys(legs); len(markets) != 2 || len(blocks) != 1 {
		t.Fatalf("market/dependence identities were conflated: markets=%v blocks=%v", markets, blocks)
	}
}

func TestR144FundedComboQuantityUsesCommonVenueGrid(t *testing.T) {
	quotes := []fundedComboLegQuote{{QtyStep: .25, MinQty: .25}, {QtyStep: .10, MinQty: .10}}
	got, ok := fundedComboRoundQuantity(3.8, quotes)
	if !ok || math.Abs(got-3.5) > 1e-12 {
		t.Fatalf("common quantity grid got=%v ok=%v, want 3.5", got, ok)
	}
	if got, ok := fundedComboRoundQuantity(.2, quotes); ok || got != 0 {
		t.Fatalf("below-minimum package quantity accepted: got=%v ok=%v", got, ok)
	}
}

func TestR144FundedComboUsesFinalExecutableTouchDepthAndQuantityFee(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	quoteCalls := map[string]int{}
	s.fundedComboQuoteFn = func(_ context.Context, leg paper.Leg) (fundedComboLegQuote, bool) {
		quoteCalls[leg.Ticker]++
		ask, depth := .60, 20.0
		if leg.Ticker == "B" {
			ask = .70
		}
		if quoteCalls[leg.Ticker] > 1 { // final decision sees less common touch
			if leg.Ticker == "A" {
				depth = 3.8 // whole-package quantity authority rounds this down to 3
			} else {
				depth = 4
			}
		}
		return fundedComboLegQuote{Bid: ask - .01, Ask: ask, Depth: depth, Tick: .01,
			QuoteAge: .02, QtyStep: 1, MinQty: 1, BookSource: "fixture-full-book",
			FeeSource: "fixture-fee", QuantitySource: "fixture-whole-package"}, true
	}
	lastFeeQty := map[string]float64{}
	s.fundedComboFeeFn = func(leg paper.Leg, contracts, _ float64) (float64, string, bool) {
		lastFeeQty[leg.Ticker] = contracts
		return contracts * .001, "fixture-quantity-fee", true
	}
	// A contradictory cached mark proves the funded route cannot read parlayLiveLegPrice.
	s.pusBookMu.Lock()
	s.pusBookPx = map[string]pusBook{"A": {px: .20, at: time.Now()}, "B": {px: .20, at: time.Now()}}
	s.pusBookMu.Unlock()
	contract := &fundedComboContract{RouteSource: positiveSystemComboRouteSource,
		Cohort: storage.ComboLabCohortRollingPositive, SystemIDs: []string{"sys-a", "sys-b"},
		JointP: .50, ForceTaker: true}
	id, err := s.placeParlayLegsWithContract(context.Background(), []paper.Leg{
		{Platform: "polyus", Ticker: "A", Side: "YES", Entry: .60},
		{Platform: "polyus", Ticker: "B", Side: "YES", Entry: .70},
	}, 10, "positive-systems:sys-a,sys-b", contract)
	if err != nil || id == 0 {
		t.Fatalf("funded combo placement id=%d err=%v", id, err)
	}
	rows, err := s.store.ListParlays(context.Background(), "open")
	if err != nil || len(rows) != 1 {
		t.Fatalf("funded combo rows=%d err=%v", len(rows), err)
	}
	p := rows[0]
	if math.Abs(p.Price-.42) > 1e-12 || math.Abs(p.Contracts-3) > 1e-12 ||
		math.Abs(p.Stake-1.26) > 1e-12 || math.Abs(p.Fees-.006) > 1e-12 {
		t.Fatalf("final price/depth/fee not used: %+v", p)
	}
	for _, leg := range p.Legs {
		if leg.TouchDepth < 3 || leg.TickSize != .01 || leg.QuantityStep != 1 || leg.MinimumQty != 1 || leg.QuoteAgeS != .02 ||
			leg.BookSource != "fixture-full-book" || leg.FeeSource != "fixture-quantity-fee" ||
			leg.QuantitySource != "fixture-whole-package" ||
			math.Abs(leg.EntryFee-.003) > 1e-12 || math.Abs(lastFeeQty[leg.Ticker]-3) > 1e-12 {
			t.Fatalf("incomplete final executable receipt: %+v feeQty=%v", leg, lastFeeQty)
		}
	}
}

func TestR144FundedComboFailsClosedOnStaleTouch(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	s.fundedComboQuoteFn = func(context.Context, paper.Leg) (fundedComboLegQuote, bool) {
		return fundedComboLegQuote{Bid: .49, Ask: .50, Depth: 10, Tick: .01,
			QuoteAge: fundedComboMaxQuoteAge.Seconds() + .001, QtyStep: 1, MinQty: 1,
			BookSource: "stale", FeeSource: "fee", QuantitySource: "fixture-whole-package"}, true
	}
	s.fundedComboFeeFn = func(paper.Leg, float64, float64) (float64, string, bool) {
		return 0, "fee", true
	}
	contract := &fundedComboContract{RouteSource: positiveSystemComboRouteSource,
		Cohort: storage.ComboLabCohortRollingPositive, SystemIDs: []string{"sys"}, JointP: .30, ForceTaker: true}
	_, err := s.placeParlayLegsWithContract(context.Background(), []paper.Leg{
		{Platform: "kalshi", Ticker: "A", Side: "YES", Entry: .50},
		{Platform: "kalshi", Ticker: "B", Side: "YES", Entry: .50},
	}, 5, "positive-systems:sys", contract)
	if err == nil || !strings.Contains(err.Error(), "fresh complete") {
		t.Fatalf("stale touch did not fail closed: %v", err)
	}
	if rows, e := s.store.ListParlays(context.Background(), ""); e != nil || len(rows) != 0 {
		t.Fatalf("stale funded combo persisted rows=%d err=%v", len(rows), e)
	}
}

func TestR144ComboLegacyPoisonAggregateIsExcluded(t *testing.T) {
	s := testServer(t)
	poison := `{"indep|6leg|ind:6leg|synthetic_legs_only":{"n":9,"sum_real":-2109591}}`
	if err := s.store.KVSet(context.Background(), "parlaylab_stats", poison); err != nil {
		t.Fatal(err)
	}
	s.plabMu.Lock()
	s.plabState(context.Background())
	n, graded := len(s.plabStats), s.plabGraded
	s.plabMu.Unlock()
	if n != 0 || graded != 0 {
		t.Fatalf("legacy poisoned aggregate entered all-in-v2 epoch: cells=%d graded=%d", n, graded)
	}
}

func r144GradeFixture(id string, eventA, eventB string, fees []float64) plabOpenEnt {
	return plabOpenEnt{ID: id, At: time.Now().Add(-time.Hour), Legs: []plabLeg{
		{Platform: "kalshi", Ticker: id + "-A", Side: "YES", Price: .5, PWin: .6, EventKey: eventA},
		{Platform: "kalshi", Ticker: id + "-B", Side: "YES", Price: .5, PWin: .6, EventKey: eventB},
	}, Bucket: "indep", Class: "ind:2leg", Prod: .25, JointP: .36, FeeMVE: -1,
		LegFees: fees, Legality: "synthetic_legs_only", Cohort: storage.ComboLabCohortAllEligible,
		RouteState: "synthetic-settlement-only"}
}

func TestR144ComboGradeReceiptIsIdempotentAndBounded(t *testing.T) {
	s := testServer(t)
	s.plabLoaded = true
	s.plabStats, s.plabCorr = map[string]*plabAgg{}, map[string]*plabCorrC{}
	e := r144GradeFixture("huge-fee", "event-a", "event-b", []float64{100000, 134399})
	inserted, err := s.plabGradeOne(context.Background(), e, []float64{0, 1})
	if err != nil || !inserted {
		t.Fatalf("first grade inserted=%v err=%v", inserted, err)
	}
	if inserted, err = s.plabGradeOne(context.Background(), e, []float64{0, 1}); err != nil || inserted {
		t.Fatalf("duplicate grade inserted=%v err=%v", inserted, err)
	}
	key := "indep|2leg|ind:2leg|synthetic_legs_only|all-eligible"
	if got := s.plabStats[key]; got == nil || got.N != 1 || got.SumReal != -1 || got.SumReal < -1 {
		t.Fatalf("grade was duplicated or unbounded: %+v", got)
	}
	receipts, err := s.store.PlabGradeReceipts(context.Background())
	if err != nil || len(receipts) != 1 || receipts[0].RealizedReturn != -1 {
		t.Fatalf("durable receipt truth=%+v err=%v", receipts, err)
	}
}

func TestR144ComboCountsRowsUniqueMarketsAndDependenceBlocks(t *testing.T) {
	s := testServer(t)
	s.plabLoaded = true
	s.plabStats, s.plabCorr = map[string]*plabAgg{}, map[string]*plabCorrC{}
	fixtures := []plabOpenEnt{
		r144GradeFixture("one", "shared", "a", []float64{0, 0}),
		r144GradeFixture("two", "shared", "b", []float64{0, 0}),
		r144GradeFixture("three", "c", "d", []float64{0, 0}),
	}
	for _, e := range fixtures {
		if inserted, err := s.plabGradeOne(context.Background(), e, []float64{1, 1}); err != nil || !inserted {
			t.Fatalf("grade %s inserted=%v err=%v", e.ID, inserted, err)
		}
	}
	// Aggregate caches are disposable. Rebuild from receipts to prove a restart/crash keeps the
	// same duplicate-safe denominator and never needs the poisoned legacy KV blob.
	s.plabMu.Lock()
	s.plabStats, s.plabCorr, s.plabGraded = nil, nil, 0
	if err := s.plabRebuildV2StatsLocked(context.Background()); err != nil {
		s.plabMu.Unlock()
		t.Fatal(err)
	}
	s.plabMu.Unlock()
	key := "indep|2leg|ind:2leg|synthetic_legs_only|all-eligible"
	a := s.plabStats[key]
	// Six venue+ticker instruments are distinct, even though the first two combo rows share one
	// semantic resolution key and therefore contribute only one dependence component together.
	if a == nil || a.N != 3 || a.UniqueMarkets != 6 || a.IndependentN != 2 {
		t.Fatalf("row/market/block counts wrong: %+v", a)
	}
	rows := s.plabSummary()
	if len(rows) != 1 || rows[0]["settled_rows"] != 3 || rows[0]["unique_independent_markets"] != 6 ||
		rows[0]["independent_resolution_blocks"] != 2 || rows[0]["sample_bucket"] != "red" {
		t.Fatalf("summary did not expose duplicate-safe counts/bucket: %+v", rows)
	}
}
