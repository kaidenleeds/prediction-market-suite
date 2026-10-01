package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r143PositiveComboPool(n int, platform string) []plabLeg {
	out := make([]plabLeg, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, plabLeg{Platform: platform, Ticker: fmt.Sprintf("R143-POS-%02d", i),
			Side: "YES", Price: .70, PWin: .85, EVNet: .15, Depth: 100,
			EventKey: fmt.Sprintf("event-%02d", i), RollingPositive: true,
			PositiveSystems: []string{fmt.Sprintf("system-%02d", i%4)}, SystemRoute: "taker"})
	}
	return out
}

func r144EnableFundedComboExecutionFixture(s *Server) {
	r166AllowUnverifiedMultiLegPaperForTest(s)
	s.fundedComboQuoteFn = func(_ context.Context, leg paper.Leg) (fundedComboLegQuote, bool) {
		s.pusBookMu.Lock()
		book, ok := s.pusBookPx[leg.Ticker]
		s.pusBookMu.Unlock()
		if !ok || book.px <= .02 || book.px >= .98 {
			return fundedComboLegQuote{}, false
		}
		ask := book.px
		if strings.EqualFold(leg.Side, "NO") {
			ask = 1 - book.px
		}
		return fundedComboLegQuote{Bid: ask - .01, Ask: ask, Depth: 100, Tick: .01,
			QuoteAge: .01, QtyStep: 1, MinQty: 1, BookSource: "fixture-full-book",
			FeeSource: "fixture-exact-fee", QuantitySource: "fixture-whole-package"}, true
	}
	s.fundedComboFeeFn = func(leg paper.Leg, contracts, price float64) (float64, string, bool) {
		if s.plabSampleFeeFn != nil {
			fee, ok := s.plabSampleFeeFn(plabLeg{Platform: leg.Platform, Ticker: leg.Ticker,
				Side: leg.Side, Price: price}, contracts)
			return fee, "fixture-exact-fee", ok
		}
		return 0, "fixture-exact-fee", true
	}
}

func TestR143BriefAndAPIExposePositiveSystemLegsTwoThroughSix(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if _, err := s.plabCurrentAdmissionEpoch(ctx, time.Now().UTC(), true); err != nil {
		t.Fatal(err)
	}
	s.plabLoaded = true
	s.plabStats = map[string]*plabAgg{}
	var open []storage.PlabCand
	for legs := 2; legs <= 6; legs++ {
		s.plabStats[fmt.Sprintf("indep|%dleg|ind:%dleg|synthetic_legs_only|%s", legs, legs,
			storage.ComboLabCohortRollingPositive)] = &plabAgg{N: 1, SumReal: .1 * float64(legs)}
		open = append(open, storage.PlabCand{ID: fmt.Sprintf("r143-brief-%d", legs), At: time.Now().Unix(),
			Bucket: "indep", Class: fmt.Sprintf("ind:%dleg", legs), Legality: "synthetic_legs_only",
			Prod: .25, JointP: .30, EVSyn: .10, LegFees: "[]", Legs: "[]", NLegs: legs,
			Cohort: storage.ComboLabCohortRollingPositive, RouteState: "paper-only-rolling-positive"})
	}
	if n, err := s.store.PlabInsertBatch(ctx, open); err != nil || n != 5 {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	brief := s.briefScoreboard(ctx)
	if strings.Contains(brief, "Combo Paper") || strings.Contains(brief, "Combo Lab") {
		t.Fatalf("duplicate Combo section leaked below the portfolio header:\n%s", brief)
	}

	rr := httptest.NewRecorder()
	s.handleParlayLab(rr, httptest.NewRequest("GET", "/api/combolab", nil))
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	progress, ok := body["positive_system_leg_progress"].([]any)
	if !ok || len(progress) != 5 {
		t.Fatalf("positive-system 2-6 progress missing: %#v", body["positive_system_leg_progress"])
	}
	for i, raw := range progress {
		row := raw.(map[string]any)
		if row["legs"] != float64(i+2) || row["settled_n"] != float64(1) || row["open_n"] != float64(1) {
			t.Fatalf("positive-system progress[%d]=%#v", i, row)
		}
	}
}

func TestR143BoundedComboSampleCannotStarvePositiveSystemsTwoThroughSix(t *testing.T) {
	pool := r133SamplePool(30)
	for i := range pool {
		pool[i].RollingPositive = true
		pool[i].PositiveSystems = []string{fmt.Sprintf("positive-%02d", i%5)}
		pool[i].SystemRoute = "taker"
	}
	rows, _ := plabStableProspectiveSample(pool, 6, 48)
	seen := map[int]bool{}
	for _, row := range rows {
		if row.Cohort == storage.ComboLabCohortRollingPositive {
			seen[len(row.Legs)] = true
		}
	}
	for legs := 2; legs <= 6; legs++ {
		if !seen[legs] {
			t.Fatalf("48-row bounded sample starved positive-system %dL rows: %+v", legs, seen)
		}
	}
}

func TestR143SparseExactRouteGetsFairTwoThroughSixCoverageReceipt(t *testing.T) {
	pool := r143PositiveComboPool(18, "polyus")
	for i := range pool {
		pool[i].PositiveSystems = []string{"prolific-route"}
	}
	// A single current leg is enough for this route to participate in combinations with the
	// venue's other exact positive legs. It must not disappear behind the prolific route.
	pool[len(pool)-1].PositiveSystems = []string{"sparse-route"}
	rows, _ := plabStableProspectiveSample(pool, 6, 48)
	sparse := "polyus|sparse-route|taker"
	seen := map[int]bool{}
	for _, row := range rows {
		for _, route := range row.Routes {
			if route == sparse {
				seen[len(row.Legs)] = true
			}
		}
	}
	for legs := 2; legs <= 6; legs++ {
		if !seen[legs] {
			t.Fatalf("sparse route was starved at %dL: %+v", legs, seen)
		}
	}
	coverage := plabBuildPositiveRouteCoverage(pool, rows, 6)
	found := false
	for _, row := range coverage {
		if row.RouteID != sparse {
			continue
		}
		found = true
		if !row.Complete || len(row.MissingFeasible) != 0 || len(row.Infeasible) != 0 {
			t.Fatalf("sparse route receipt was not complete/honest: %+v", row)
		}
		for legs := 2; legs <= 6; legs++ {
			if !row.FeasibleByLegs[legs] || row.ProposedByLegs[legs] == 0 {
				t.Fatalf("coverage receipt omitted sparse %dL: %+v", legs, row)
			}
		}
	}
	if !found {
		t.Fatalf("coverage receipt omitted sparse route: %+v", coverage)
	}
}

func TestR143FundedPositiveSystemCombosCollectTwoThroughSixWithoutGenericML(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.polyUS = polymarketus.NewPublicClient(time.Millisecond)
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	r144EnableFundedComboExecutionFixture(s)
	pool := r143PositiveComboPool(8, "polyus")
	s.pusBookMu.Lock()
	s.pusBookPx = map[string]pusBook{}
	for _, leg := range pool {
		s.pusBookPx[leg.Ticker] = pusBook{px: leg.Price, at: time.Now()}
	}
	s.pusBookMu.Unlock()
	s.autoMu.Lock()
	s.autoOn = true
	s.autoMu.Unlock()

	s.autoPlacePositiveSystemCombos(ctx, pool)
	rows, err := s.store.ListParlays(ctx, "open")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	for _, row := range rows {
		if len(row.Legs) < 2 || len(row.Legs) > 6 {
			t.Fatalf("funded positive-system route emitted invalid leg count: %d", len(row.Legs))
		}
		seen[len(row.Legs)] = true
		for _, leg := range row.Legs {
			if leg.Platform != "polyus" {
				t.Fatalf("cross-venue/generic leg entered funded route: %+v", leg)
			}
		}
	}
	for legs := 2; legs <= 6; legs++ {
		if !seen[legs] {
			t.Fatalf("funded positive-system route did not collect %dL: rows=%d seen=%+v", legs, len(rows), seen)
		}
	}
	st := s.positiveSystemComboStatus(ctx)
	if st.State != "collecting" || st.Placed < 5 || st.LiveAuthority || len(st.Systems) != 4 {
		t.Fatalf("funded positive-system status=%+v", st)
	}
}

func TestR143PositiveSystemComboRouteRequiresAutoAndExplainsZero(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	pool := r143PositiveComboPool(6, "polyus")
	s.autoPlacePositiveSystemCombos(ctx, pool)
	st := s.positiveSystemComboStatus(ctx)
	if st.State != "paused" || st.Reasons["auto_off"] != 1 || st.ExactLegs != 6 || st.LiveAuthority {
		t.Fatalf("AUTO-off status did not explain zero funded combos: %+v", st)
	}
	if rows, err := s.store.ListParlays(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("AUTO-off route placed rows=%d err=%v", len(rows), err)
	}
}

func TestR143FundedPositiveComboProvenanceExcludesLegacyFromStatusAndCooldown(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.polyUS = polymarketus.NewPublicClient(time.Millisecond)
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	r144EnableFundedComboExecutionFixture(s)
	pool := r143PositiveComboPool(2, "polyus")
	s.pusBookMu.Lock()
	s.pusBookPx = map[string]pusBook{}
	legacyLegs := make([]paper.Leg, 0, len(pool))
	for _, leg := range pool {
		s.pusBookPx[leg.Ticker] = pusBook{px: leg.Price, at: time.Now()}
		legacyLegs = append(legacyLegs, paper.Leg{Platform: "polyus", Ticker: leg.Ticker, Side: leg.Side, Entry: leg.Price})
	}
	s.pusBookMu.Unlock()
	legacyID, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 3, Price: .49, Contracts: 3 / .49, Legs: legacyLegs})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SettleParlay(ctx, legacyID, 1, 999); err != nil {
		t.Fatal(err)
	}
	s.autoMu.Lock()
	s.autoOn = true
	s.autoMu.Unlock()

	s.autoPlacePositiveSystemCombos(ctx, pool)
	rows, err := s.store.ListParlays(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	funded := 0
	for _, row := range rows {
		if !isFundedPositiveSystemCombo(row) {
			continue
		}
		funded++
		if len(row.SystemIDs) == 0 || row.JointP <= 0 || row.ExpectedNetPerDollar <= 0 {
			t.Fatalf("funded provenance/economics were not persisted: %+v", row)
		}
	}
	if funded != 1 {
		t.Fatalf("legacy identical signature incorrectly blocked funded route: funded=%d rows=%+v", funded, rows)
	}
	st := s.positiveSystemComboStatus(ctx)
	if st.Open != 1 || st.Closed != 0 || st.RealizedNet != 0 {
		t.Fatalf("legacy row leaked into funded status/performance: %+v", st)
	}
}

func TestR143PaperResetStartsFreshFundedComboEpochWithoutDeletingHistory(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	pool := r143PositiveComboPool(2, "polyus")
	legs := make([]paper.Leg, 0, len(pool))
	for _, leg := range pool {
		legs = append(legs, paper.Leg{Platform: "polyus", Ticker: leg.Ticker, Side: leg.Side, Entry: leg.Price})
	}
	old := paper.Parlay{Stake: 5, Price: .49, Contracts: 5 / .49, Legs: legs,
		RouteSource: positiveSystemComboRouteSource, Cohort: storage.ComboLabCohortRollingPositive,
		SystemIDs: []string{"system-00", "system-01"}, JointP: .70, ExpectedNetPerDollar: .10}
	settledID, err := s.store.InsertParlay(ctx, old)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SettleParlay(ctx, settledID, 0, -5); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.InsertParlay(ctx, old); err != nil {
		t.Fatal(err)
	}

	closed, busy := s.doPaperReset(ctx)
	if busy || closed != 1 {
		t.Fatalf("reset closed=%d busy=%v, want one open combo closed", closed, busy)
	}
	all, err := s.store.ListParlays(ctx, "")
	if err != nil || len(all) != 2 {
		t.Fatalf("reset deleted research history: rows=%d err=%v", len(all), err)
	}
	st := s.positiveSystemComboStatus(ctx)
	if st.Epoch == "" || st.Open != 0 || st.Closed != 0 || st.RealizedNet != 0 || st.Placed != 0 ||
		st.State != "warming" || st.Reasons["post_reset_warmup"] != 1 {
		t.Fatalf("reset did not clear current funded-combo receipt: %+v", st)
	}
	staleReceipt := positiveSystemComboPaperStatus{Epoch: "2026-01-01T00:00:00Z", State: "collecting",
		ExactLegs: 999, Placed: 99, Closed: 100, RealizedNet: -184, Reasons: map[string]int{"already_open_or_cooldown": 1}}
	staleJSON, _ := json.Marshal(staleReceipt)
	if err := s.store.KVSet(ctx, positiveSystemComboStatusKV, string(staleJSON)); err != nil {
		t.Fatal(err)
	}
	st = s.positiveSystemComboStatus(ctx)
	if st.Placed != 0 || st.ExactLegs != 0 || st.Closed != 0 || st.RealizedNet != 0 ||
		st.Reasons["post_reset_warmup"] != 1 {
		t.Fatalf("stale pre-reset receipt survived durable epoch mismatch: %+v", st)
	}

	// The exact same signature may collect immediately in the new Paper epoch. The reset-close
	// timestamp is deliberately recent; if pre-reset cooldown state leaks, this stays at zero.
	s.polyUS = polymarketus.NewPublicClient(time.Millisecond)
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	r144EnableFundedComboExecutionFixture(s)
	s.pusBookMu.Lock()
	s.pusBookPx = map[string]pusBook{}
	for _, leg := range pool {
		s.pusBookPx[leg.Ticker] = pusBook{px: leg.Price, at: time.Now()}
	}
	s.pusBookMu.Unlock()
	s.autoMu.Lock()
	s.autoOn = true
	s.autoMu.Unlock()
	s.autoPlacePositiveSystemCombos(ctx, pool)

	current := s.positiveSystemComboStatus(ctx)
	if current.Open != 1 || current.Closed != 0 || current.RealizedNet != 0 || current.Placed != 1 ||
		current.Reasons["already_open_or_cooldown"] != 0 {
		t.Fatalf("pre-reset rows still throttled or polluted current combo epoch: %+v", current)
	}
	open, err := s.store.ListParlays(ctx, "open")
	if err != nil || len(open) != 1 {
		t.Fatalf("same signature did not resume after reset: open=%d err=%v", len(open), err)
	}
	// The reset epoch is durable across a later process boot. A rebuild must not make the current
	// funded position vanish from the compact Combos portfolio merely because its TS is pre-boot.
	s.bootAt = time.Now().Add(time.Hour)
	if m, ok := s.bookSessionClosedMetrics(ctx, vbCombos); !ok || m.Open != 1 || m.Bets != 0 || m.NetUSD != 0 {
		t.Fatalf("briefing portfolio did not expose current funded open after reset: %+v ok=%v", m, ok)
	}
	if err := s.store.SettleParlay(ctx, open[0].ID, 1, 2); err != nil {
		t.Fatal(err)
	}
	current = s.positiveSystemComboStatus(ctx)
	if current.Open != 0 || current.Closed != 1 || math.Abs(current.RealizedNet-2) > 1e-12 {
		t.Fatalf("current combo status did not report since-reset P&L: %+v", current)
	}
	if m, ok := s.bookSessionClosedMetrics(ctx, vbCombos); !ok || m.Open != 0 || m.Bets != 1 ||
		math.Abs(m.NetUSD-2) > 1e-12 || math.Abs(m.Units-open[0].Contracts) > 1e-12 {
		t.Fatalf("briefing portfolio did not expose current funded settlement: %+v ok=%v", m, ok)
	}

	rr := httptest.NewRecorder()
	s.handleParlay(rr, httptest.NewRequest("GET", "/api/parlay", nil))
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	summary := body["summary"].(map[string]any)
	if got := summary["realized"].(float64); math.Abs(got-2) > 1e-12 {
		t.Fatalf("current combo portfolio P&L includes pre-reset history: %v", got)
	}
}

func TestR143FundedPositiveComboRefusesAdverseFinalRepriceEV(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.polyUS = polymarketus.NewPublicClient(time.Millisecond)
	r144EnableFundedComboExecutionFixture(s)
	tickers := []string{"R143-FINAL-A", "R143-FINAL-B"}
	s.pusBookMu.Lock()
	s.pusBookPx = map[string]pusBook{}
	for _, ticker := range tickers {
		s.pusBookPx[ticker] = pusBook{px: .70, at: time.Now()}
	}
	s.pusBookMu.Unlock()
	horizonCalls := 0
	s.paperHorizonFn = func(context.Context, string, string, string) float64 {
		horizonCalls++
		if horizonCalls == 3 { // after both initial reads, just before the final funded reprice
			s.pusBookMu.Lock()
			for _, ticker := range tickers {
				s.pusBookPx[ticker] = pusBook{px: .72, at: time.Now()}
			}
			s.pusBookMu.Unlock()
		}
		return 1
	}
	legs := []paper.Leg{
		{Platform: "polyus", Ticker: tickers[0], Side: "YES", Entry: .70},
		{Platform: "polyus", Ticker: tickers[1], Side: "YES", Entry: .70},
	}
	contract := &fundedComboContract{RouteSource: positiveSystemComboRouteSource,
		Cohort: storage.ComboLabCohortRollingPositive, SystemIDs: []string{"system-a"}, JointP: .50, ForceTaker: true}
	if _, err := s.placeParlayLegsWithContract(ctx, legs, 5, "positive-systems:system-a", contract); err == nil ||
		!strings.Contains(strings.ToLower(err.Error()), "expected value") {
		t.Fatalf("adverse final reprice did not refuse non-positive EV: %v", err)
	}
	if rows, err := s.store.ListParlays(ctx, ""); err != nil || len(rows) != 0 {
		t.Fatalf("rejected final-EV combo was inserted: rows=%d err=%v", len(rows), err)
	}
}

func TestR143GenericMLCannotDonateEconomicsToExactPositiveSystemLeg(t *testing.T) {
	generic := plabLeg{Platform: "polyus", Ticker: "same", Side: "YES", Price: .40,
		PWin: .95, EVNet: .55, Src: "generic-ml"}
	exact := plabLeg{Platform: "polyus", Ticker: "same", Side: "YES", Price: .60,
		PWin: .63, EVNet: .03, Src: "exact-unit-trial/model", RollingPositive: true,
		PositiveSystems: []string{"taker:exact@polyus [YES]"}, SystemRoute: "taker"}
	if plabPoolKey(generic) == plabPoolKey(exact) {
		t.Fatal("generic ML and exact positive route still share a candidate key")
	}
	for _, got := range []plabLeg{mergePlabLeg(generic, exact), mergePlabLeg(exact, generic)} {
		if !got.RollingPositive || got.PWin != exact.PWin || got.Price != exact.Price || got.EVNet != exact.EVNet ||
			len(got.PositiveSystems) != 1 || got.PositiveSystems[0] != exact.PositiveSystems[0] {
			t.Fatalf("generic ML economics leaked into exact positive route: %+v", got)
		}
	}
}

func TestR143ComboPrescreenChargesEveryRolledLegFee(t *testing.T) {
	s := testServer(t)
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return .015, true }
	legs := []plabLeg{
		{Platform: "polyus", Ticker: "fee-a", Side: "YES", Price: .70, PWin: .709, EVNet: .009,
			EventKey: "a", Depth: 10, RollingPositive: true, PositiveSystems: []string{"sys-a"}, SystemRoute: "taker"},
		{Platform: "polyus", Ticker: "fee-b", Side: "YES", Price: .70, PWin: .709, EVNet: .009,
			EventKey: "b", Depth: 10, RollingPositive: true, PositiveSystems: []string{"sys-b"}, SystemRoute: "taker"},
	}
	fee, perLeg, ok := s.plabExactSyntheticFees(legs)
	if !ok || math.Abs(fee-.030) > 1e-12 || len(perLeg) != 2 {
		t.Fatalf("all-leg fee convention not applied: fee=%v legs=%v ok=%v", fee, perLeg, ok)
	}
	// Gross EV is ~2.62¢. The old probability-discounted fee was 2.58¢ and falsely passed;
	// funded placement charges the true 3.00¢, so the prospective candidate must now fail.
	oldDiscounted := .015 + .709*.015
	gross := (.709*.709)/(.70*.70) - 1
	if !(gross > oldDiscounted && gross < fee) {
		t.Fatalf("adversarial fixture does not straddle old/new fee gates: gross=%v old=%v all=%v", gross, oldDiscounted, fee)
	}
	if got, _ := s.positiveSystemComboCandidates(context.Background(), legs, 2); len(got) != 0 {
		t.Fatalf("old discounted-fee false positive survived true all-fee prescreen: %+v", got)
	}
}

func TestR147FundedComboRejectsUnknownOrRelatedPayoffButResearchCanStillObserveIt(t *testing.T) {
	independent := []plabLeg{{Platform: "kalshi", Ticker: "K-A", Side: "YES", EventKey: "event-a"},
		{Platform: "kalshi", Ticker: "K-B", Side: "NO", EventKey: "event-b"}}
	if ok, reason := fundedComboRelationGate(independent); !ok || reason != "" {
		t.Fatalf("independent identified legs rejected: ok=%v reason=%q", ok, reason)
	}
	related := append([]plabLeg(nil), independent...)
	related[1].EventKey = "event-a"
	if ok, reason := fundedComboRelationGate(related); ok || reason != "related_without_exact_joint_quote" {
		t.Fatalf("related synthetic payoff funded: ok=%v reason=%q", ok, reason)
	}
	unknown := append([]plabLeg(nil), independent...)
	unknown[1].EventKey = ""
	if ok, reason := fundedComboRelationGate(unknown); ok || reason != "missing_relation_identity" {
		t.Fatalf("unknown relation funded: ok=%v reason=%q", ok, reason)
	}
	// The research lab keeps these shapes available; this gate belongs only to funded Paper/Live-
	// transfer candidates and must not erase correlation evidence.
	if err := plabValidatePricedLegSet([]plabLeg{
		{Platform: "kalshi", Ticker: "K-A", Side: "YES", EventKey: "event-a", Price: .4, PWin: .5},
		{Platform: "kalshi", Ticker: "K-B", Side: "YES", EventKey: "event-a", Price: .4, PWin: .5},
	}); err != nil {
		t.Fatalf("research-only related observation erased: %v", err)
	}
}

func TestR143ComboGradeChargesAllEntryFeesAfterEarlyLoss(t *testing.T) {
	s := testServer(t)
	s.plabLoaded = true
	s.plabStats = map[string]*plabAgg{}
	s.plabCorr = map[string]*plabCorrC{}
	e := plabOpenEnt{ID: "r143-early-loss", Legs: []plabLeg{
		{Platform: "polyus", Ticker: "loss-first", Side: "YES", EventKey: "one"},
		{Platform: "polyus", Ticker: "win-second", Side: "YES", EventKey: "two"},
	}, Bucket: "indep", Class: "ind:test", Prod: .25, JointP: .30, FeeMVE: -1,
		LegFees: []float64{.01, .02}, Legality: "synthetic_legs_only",
		Cohort: storage.ComboLabCohortRollingPositive, RouteState: "paper-only-rolling-positive"}
	if inserted, err := s.plabGradeOne(context.Background(), e, []float64{0, 1}); err != nil || !inserted {
		t.Fatalf("grade inserted=%v err=%v", inserted, err)
	}
	key := "indep|2leg|ind:test|synthetic_legs_only|" + storage.ComboLabCohortRollingPositive
	got := s.plabStats[key]
	if got == nil || got.N != 1 || math.Abs(got.SumReal-(-1)) > 1e-12 {
		t.Fatalf("early loss did not charge complete entry fee chain: %+v", got)
	}
}

func TestR143LegacyGenericCombosRemainLogOnly(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	legacyID, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 100, Fees: 10, Price: .4, Contracts: 250})
	if err != nil {
		t.Fatal(err)
	}
	fundedID, err := s.store.InsertParlay(ctx, paper.Parlay{Stake: 4, Fees: .25, Price: .4, Contracts: 10,
		RouteSource: positiveSystemComboRouteSource, Cohort: storage.ComboLabCohortRollingPositive,
		SystemIDs: []string{"exact-system"}, JointP: .5, ExpectedNetPerDollar: .1})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := s.store.ListParlays(ctx, "")
	s.autoPlaceParlays(ctx)
	after, _ := s.store.ListParlays(ctx, "")
	if len(after) != len(before) {
		t.Fatalf("retired generic auto funder inserted a row: before=%d after=%d", len(before), len(after))
	}
	if got := s.parlayOpenCost(ctx); math.Abs(got-4.25) > 1e-12 {
		t.Fatalf("legacy open row leaked into current portfolio exposure: %v", got)
	}
	if err := s.store.SettleParlay(ctx, legacyID, 1, 999); err != nil {
		t.Fatal(err)
	}
	if err := s.store.SettleParlay(ctx, fundedID, 1, 3); err != nil {
		t.Fatal(err)
	}
	if got, err := s.store.ParlaysRealized(ctx); err != nil || math.Abs(got-3) > 1e-12 {
		t.Fatalf("legacy realized row leaked into current portfolio P&L: got=%v err=%v", got, err)
	}
}
