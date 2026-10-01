package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r147EnableProspectiveAllocation(s *Server) {
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveAllocationMinMarkets = liveAllocationMinMarketsFloor
	cfg.Risk.LiveAllocationMinEdge = .005
	s.cfgP.Store(&cfg)
}

func r147InsertAllocationHistory(t *testing.T, s *Server, family, side string, markets, events int) liveMirrorCandidate {
	t.Helper()
	return r147InsertAllocationHistoryOrigin(t, s, family, side, "strategy", markets, events)
}

func r147InsertAllocationHistoryOrigin(t *testing.T, s *Server, family, side, origin string, markets, events int) liveMirrorCandidate {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXCANARY-NOW", Title: "current canary fixture",
		Side: side, Source: "auto-cons-" + family, Price: .40, At: time.Now()}
	bound, _, why := r147BindSignalContract(c, "taker")
	if why != "" {
		t.Fatalf("bind canonical allocation contract: %s", why)
	}
	c = bound
	economicFamily := family
	catalog := make([]storage.CatalogRow, 0, markets+1)
	for i := 0; i < markets; i++ {
		event := i
		if events > 0 {
			event = i % events
		}
		catalog = append(catalog, storage.CatalogRow{Venue: "kalshi", Ticker: fmt.Sprintf("KXCANARY-HIST-%03d", i),
			EventKey: fmt.Sprintf("KXCANARY-EVENT-%03d", event), Kind: "winner", Title: "canary fixture"})
	}
	currentTicker := "KXCANARY-NOW"
	catalog = append(catalog, storage.CatalogRow{Venue: "kalshi", Ticker: currentTicker,
		EventKey: "KXCANARY-NOW-EVENT", Kind: "winner", Title: "current canary fixture"})
	if err := s.store.UpsertMarketCatalog(ctx, catalog); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < markets; i++ {
		opened := now.Add(-time.Duration(2+i%48) * time.Hour)
		episode := 0
		quoteSource := "kalshi-rest-full-book"
		if origin == "strategy" {
			episode = livePriorityProofGenerationEpisode
			quoteSource = livePriorityProofQuoteSource("kalshi-rest-full-book", "r147-test-contract")
		}
		inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{OpenedTS: opened,
			Family: economicFamily, Platform: "kalshi", OriginLayer: origin, Ticker: fmt.Sprintf("KXCANARY-HIST-%03d", i),
			Side: side, Episode: episode, Ask: .40, FeePC: .01, FeeKnown: true,
			FeeSource: "kalshi:test-schedule", Depth: 10, QuoteSource: quoteSource})
		if err != nil || !inserted {
			t.Fatalf("history insert %d=%v err=%v", i, inserted, err)
		}
		pnl := .18
		if i%2 == 0 {
			pnl = .22
		}
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,settle_val=1,pnl_pc=?,return_per_dollar=?
WHERE family=? AND ticker=? AND side=?`, opened.Add(time.Minute).Format(time.RFC3339Nano), pnl,
			pnl/.41, economicFamily, fmt.Sprintf("KXCANARY-HIST-%03d", i), side); err != nil {
			t.Fatal(err)
		}
	}
	inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{OpenedTS: time.Now().UTC(),
		Family: economicFamily, Platform: "kalshi", OriginLayer: "strategy", Ticker: currentTicker, Side: side,
		Episode: livePriorityProofGenerationEpisode, Ask: .40, FeePC: .01, FeeKnown: true,
		FeeSource: "kalshi:test-schedule", Depth: 10,
		QuoteSource: livePriorityProofQuoteSource("kalshi-rest-full-book", "r147-test-contract")})
	if err != nil || !inserted {
		t.Fatalf("fresh strategy receipt=%v err=%v", inserted, err)
	}
	s.liveAllocationSignal = map[string]time.Time{liveAllocationSignalKey(c): time.Now()}
	return c
}

func r147AllocationServer(t *testing.T) (*Server, liveMirrorCandidate) {
	t.Helper()
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	s.kalFees = map[string]kalFeeInfo{
		"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistory(t, s, "kflow", "YES", liveAllocationMinMarketsFloor, 1)
	return s, c
}

func TestR147ProspectiveAllocationNeedsOptInFreshExactRouteAndPositiveLowerBound(t *testing.T) {
	s, c := r147AllocationServer(t)
	ok, basis, mean, lower, fee :=
		s.liveProspectiveDiagnosticProof(context.Background(), c, .40, false)
	if !ok || !strings.HasPrefix(basis, "prospective-allocation:kflow@kalshi[YES]/taker:input=K:contract=") ||
		mean <= 0 || lower < s.liveAllocationEdgeFloor() || fee < 0 {
		t.Fatalf("valid diagnostic route rejected: ok=%v basis=%q mean=%v lower=%v fee=%v",
			ok, basis, mean, lower, fee)
	}
	ok, basis, mean, lower, fee =
		s.livePolicyMirrorIsolatedDiagnosticProof(context.Background(), c, .40)
	if !ok || !strings.HasPrefix(
		basis, "mirror-isolated-prospective-allocation:kflow@kalshi[YES]/taker:input=K:contract=") ||
		!strings.Contains(basis, ":strategy:n") || mean <= 0 ||
		lower < s.liveAllocationEdgeFloor() || fee < 0 {
		t.Fatalf("valid isolated diagnostic route rejected: ok=%v basis=%q mean=%v lower=%v fee=%v",
			ok, basis, mean, lower, fee)
	}
	if ok, why, _, _, _ := s.liveMirrorProof(
		context.Background(), c, .40, false); ok ||
		why != liveFillConditionedProofUnavailableReason {
		t.Fatalf("hypothetical route authorized LIVE: ok=%v why=%q", ok, why)
	}
	if ok, why, _, _, _ := s.livePolicyMirrorIsolatedProof(
		context.Background(), c, .40); ok ||
		why != liveFillConditionedProofUnavailableReason {
		t.Fatalf("hypothetical route authorized isolated mirror: ok=%v why=%q", ok, why)
	}

	// A different side is a different route and cannot borrow either the current receipt or history.
	wrongSide := c
	wrongSide.Side = "NO"
	wrongSide.InputTopology, wrongSide.SignalContractID = "", ""
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), wrongSide, .40, false); ok || why != "prospective-allocation-no-fresh-exact-signal-receipt" {
		t.Fatalf("YES route leaked into NO: ok=%v why=%q", ok, why)
	}

	// Current all-in deterioration is fully charged to both contract and event confidence bounds.
	if ok, why, _, lower, _ := s.liveMirrorProof(context.Background(), c, .90, false); ok ||
		why != "prospective-allocation-current-cost-erases-confidence-lower-bound" || lower >= s.liveAllocationEdgeFloor() {
		t.Fatalf("expensive current book did not erase allocation: ok=%v why=%q lower=%v", ok, why, lower)
	}

	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = false
	s.cfgP.Store(&cfg)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, false); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("disabled allocation weakened sealed default: ok=%v why=%q", ok, why)
	}
}

func TestR148ProspectiveAllocationUsesDistinctContractsNotEventClusterCount(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	s.kalFees = map[string]kalFeeInfo{"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistory(t, s, "kflow", "YES", liveAllocationMinMarketsFloor-1, 1)
	wantThin := fmt.Sprintf("needs-%d-distinct-settled-exact-strategy-contracts", liveAllocationMinMarketsFloor)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, false); ok || !strings.Contains(why, wantThin) {
		t.Fatalf("thin route authorized: ok=%v why=%q", ok, why)
	}
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, true); ok || why != "prospective-allocation-kalshi-or-polyus-taker-singles-only" {
		t.Fatalf("maker route authorized: ok=%v why=%q", ok, why)
	}
	poly := c
	poly.Platform = "polyus"
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), poly, .40, false); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("PolyUS allocation authorized: ok=%v why=%q", ok, why)
	}
	c.At = time.Now().Add(-liveMirrorTTL - time.Second)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, false); ok || why != "prospective-allocation-signal-receipt-stale" {
		t.Fatalf("stale candidate authorized: ok=%v why=%q", ok, why)
	}
}

func TestR147ProspectiveAllocationCrossInputs(t *testing.T) {
	for family, want := range map[string][]string{
		"invert:xmatch": {"K-PINT"},
		"xvlag":         {"K-PUS"},
		"confluence":    {"K-PINT", "K-PUS", "K-PUS-PINT"},
	} {
		got := r147ExactSignalContracts(family, "kalshi", "YES", "taker")
		if len(got) != len(want) {
			t.Fatalf("%s contract count=%d want=%d (%v)", family, len(got), len(want), got)
		}
		for i := range want {
			if got[i].InputTopology != want[i] {
				t.Fatalf("%s topology[%d]=%q want=%q", family, i, got[i].InputTopology, want[i])
			}
		}
	}
}

func TestR147ProspectiveAllocationUsesMode4AndCurrentCapacityRails(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	cfg := *s.cfg()
	cfg.Risk.LiveMaxOrderPct = .05
	cfg.Risk.LiveMaxOrderUSD = -1
	cfg.Risk.LiveExposureCapPct = .50
	cfg.Risk.LiveExposureCapUSD = -1
	cfg.Risk.LiveClusterCapPct = .10
	cfg.Risk.LiveKellyMaxFrac = .50
	s.cfgP.Store(&cfg)
	s.liveBankArm = map[string]float64{"kalshi": 1000}
	s.maxContracts = 1000

	count, usd, frac, capacity, why := s.liveProspectiveAllocationSize("kalshi", 1000, .40, .20, .10, .01, 200, 1, true)
	if why != "" || count <= 1 || math.Abs(usd-50) > 1e-9 || frac <= 0 || capacity != 200 || count != 121 {
		t.Fatalf("Adaptive Allocation Model count/usd/frac/cap/why = %.0f/%.4f/%.4f/%.0f/%q", count, usd, frac, capacity, why)
	}
	if exposure, ok := liveRailLimit(1000, cfg.Risk.LiveExposureCapPct, cfg.Risk.LiveExposureCapUSD); !ok || exposure != 500 {
		t.Fatalf("50%% exposure rail=%v ok=%v", exposure, ok)
	}
	if cluster, ok := liveRailLimit(1000, cfg.Risk.LiveClusterCapPct, -1); !ok || cluster != 100 {
		t.Fatalf("10%% cluster rail=%v ok=%v", cluster, ok)
	}

	// Same proof and balance can exceed one, but exact touch depth and the global quantity rail
	// each independently tighten it without inventing capacity beyond the displayed ask.
	if got, _, _, cap, why := s.liveProspectiveAllocationSize("kalshi", 1000, .40, .20, .10, .01, 7, 1, true); why != "" || got != 7 || cap != 7 {
		t.Fatalf("depth cap count/cap/why=%v/%v/%q", got, cap, why)
	}
	s.maxContracts = 3
	if got, _, _, cap, why := s.liveProspectiveAllocationSize("kalshi", 1000, .40, .20, .10, .01, 200, 1, true); why != "" || got != 3 || cap != 3 {
		t.Fatalf("global cap count/cap/why=%v/%v/%q", got, cap, why)
	}
}

func TestR147ProspectiveAllocationKeepsItsExplicitFloorAtDispatch(t *testing.T) {
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	cfg := *s.cfg()
	cfg.Auto.MinEVPerContract = .03
	cfg.Risk.LiveAllocationMinEdge = .005
	s.cfgP.Store(&cfg)
	if got := s.liveMirrorDispatchProofFloor(true); got != .005 {
		t.Fatalf("allocation dispatch floor=%v want .005", got)
	}
	if got := s.liveMirrorDispatchProofFloor(false); got != .03 {
		t.Fatalf("normal AUTO dispatch floor=%v want .03", got)
	}
	if usd, _ := s.liveProofOrderUSD("kalshi", 1000, .40, .02, .01); usd != 0 {
		t.Fatalf("normal AUTO sizing silently accepted sub-3c proof: %v", usd)
	}
	s.liveBankArm = map[string]float64{"kalshi": 1000}
	s.maxContracts = 100
	if count, usd, _, _, why := s.liveProspectiveAllocationSize("kalshi", 1000, .40, .02, .01, .01, 100, 1, true); why != "" || usd <= 0 || count <= 1 {
		t.Fatalf("authorization-specific 0.5c floor did not reach allocation sizing: count=%v usd=%v why=%q", count, usd, why)
	}
}

func TestR147ProspectiveAllocationHandlerCannotLoosenDerivedAllInCap(t *testing.T) {
	derived := .415
	if got, ok := liveAutoTightenAllInCap(.90, derived); !ok || got != derived {
		t.Fatalf("loose caller cap escaped handler truth: got=%v ok=%v", got, ok)
	}
	if got, ok := liveAutoTightenAllInCap(.412, derived); !ok || got != .412 {
		t.Fatalf("stricter caller cap was not retained: got=%v ok=%v", got, ok)
	}
	for _, invalid := range []float64{0, -1, 1, math.NaN(), math.Inf(1)} {
		if got, ok := liveAutoTightenAllInCap(.40, invalid); ok || got != 0 {
			t.Fatalf("invalid handler-derived cap did not fail closed: derived=%v got=%v ok=%v", invalid, got, ok)
		}
	}
}

func TestR147ProspectiveAllocationRepricesProofAtFinalWireCost(t *testing.T) {
	mean, lower := liveAllocationWireAdjusted(.08, .03, .40, .01, .42, .015)
	if math.Abs(mean-.055) > 1e-12 || math.Abs(lower-.005) > 1e-12 {
		t.Fatalf("worse wire all-in did not reduce proof: mean=%.6f lower=%.6f", mean, lower)
	}
	mean, lower = liveAllocationWireAdjusted(.08, .03, .40, .01, .39, .005)
	if mean != .08 || lower != .03 {
		t.Fatalf("cheaper wire quote received optimistic proof credit: mean=%.6f lower=%.6f", mean, lower)
	}
}

func TestR147ProspectiveAllocationSettingsDiscloseIndependentEdgeFloor(t *testing.T) {
	s := testServer(t)
	body := strings.NewReader(`{"live_prospective_allocation":1,"live_allocation_min_markets":120,"live_allocation_min_edge":0.007}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/settings", body)
	req.Header.Set("Content-Type", "application/json")
	s.handleSettings(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("settings POST = %d: %s", rec.Code, rec.Body.String())
	}
	if !s.cfg().Risk.LiveProspectiveAllocation || math.Abs(s.liveAllocationEdgeFloor()-.007) > 1e-12 {
		t.Fatalf("allocation policy did not apply: enabled=%v floor=%v", s.cfg().Risk.LiveProspectiveAllocation, s.liveAllocationEdgeFloor())
	}
	rec = httptest.NewRecorder()
	s.handleSettings(rec, httptest.NewRequest(http.MethodGet, "/api/settings", nil))
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("settings GET JSON: %v (%s)", err, rec.Body.String())
	}
	if got["live_prospective_allocation"] != float64(1) || math.Abs(got["live_allocation_min_edge"].(float64)-.007) > 1e-12 {
		t.Fatalf("allocation disclosure=%v", got)
	}
}

func TestR147CanaryConfigurationKeepsLiveAutoSinglesOnly(t *testing.T) {
	s := testServer(t)
	if s.liveAutoCombosEnabled() {
		t.Fatal("R147 canary must keep Combo AUTO disabled; LIVE AUTO is singles-only")
	}
	cfg := *s.cfg()
	cfg.Auto.ParlayEnabled = true
	s.cfgP.Store(&cfg)
	if !s.liveAutoCombosEnabled() {
		t.Fatal("explicitly enabling Combos must be the only way to enable Combo AUTO")
	}
}
