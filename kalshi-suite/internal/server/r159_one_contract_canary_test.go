package server

import (
	"context"
	"strings"
	"testing"
	"time"
)

const r159FourCanaryLanes = "kalshi|spotlag|YES|taker,kalshi|spotlag|NO|taker," +
	"kalshi|kalshi-flow|YES|taker,kalshi|kalshi-flow|NO|taker"

func TestR159CanaryAllowlistIsExactSupportedSubset(t *testing.T) {
	allowed, canonical, why := parseLiveSystemCanaryAllowlist(
		" Kalshi|spotlag|yes|TAKER ; kalshi|kalshi-flow|NO|taker ",
		r159FourCanaryLanes)
	if why != "" || len(allowed) != 2 || len(canonical) != 2 ||
		canonical[0] != "kalshi|spotlag|YES|taker" ||
		canonical[1] != "kalshi|kalshi-flow|NO|taker" {
		t.Fatalf("supported subset rejected: allowed=%v canonical=%v why=%q",
			allowed, canonical, why)
	}
	if _, _, why := parseLiveSystemCanaryAllowlist(
		"kalshi|xmatch|YES|taker",
		r159FourCanaryLanes+",kalshi|xmatch|YES|taker"); why !=
		"live-system-canary-identity-not-supported" {
		t.Fatalf("unsupported family canary reason=%q", why)
	}
	if _, _, why := parseLiveSystemCanaryAllowlist(
		"kalshi|spotlag|YES|taker",
		"kalshi|spotlag|NO|taker"); why !=
		"live-system-canary-identity-not-in-live-system-allowlist" {
		t.Fatalf("canary escaped parent allowlist: %q", why)
	}
	if _, _, why := parseLiveSystemCanaryAllowlist(
		"kalshi|spotlag|YES|maker",
		r159FourCanaryLanes+",kalshi|spotlag|YES|maker"); why !=
		"live-system-canary-identity-not-supported" {
		t.Fatalf("maker canary reason=%q", why)
	}
	if _, _, why := parseLiveSystemCanaryAllowlist(
		"polyus|polyus-flow|NO|taker",
		r159FourCanaryLanes+",polyus|polyus-flow|NO|taker"); why !=
		"live-system-canary-identity-not-supported" {
		t.Fatalf("PolyUS canary reason=%q", why)
	}
}

func r159CanaryPlanFixture(t *testing.T, settled int) (*Server, liveMirrorCandidate,
	liveMirrorQuote, verdictEnt) {
	t.Helper()
	s := testServer(t)
	r147EnableProspectiveAllocation(s)
	s.kalFees = map[string]kalFeeInfo{
		"KXCANARY": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := r147InsertAllocationHistory(t, s, "spotlag", "YES", settled, max(1, settled))
	c.InputObservedAt = time.Now()
	cfg := *s.cfg()
	cfg.Risk.LiveSystemAllowlist = r159FourCanaryLanes
	cfg.Risk.LiveSystemCanaryAllowlist = r159FourCanaryLanes
	cfg.Risk.LiveMaxOrderPct = .05
	cfg.Risk.LiveMaxOrderUSD = -1
	cfg.Risk.LiveKellyMaxFrac = .50
	s.cfgP.Store(&cfg)
	s.liveBankArm = map[string]float64{"kalshi": 400}
	s.maxContracts = 10
	q := liveMirrorQuote{Price: .40, Depth: 10, Tick: .01,
		MinQty: 1, MinQtyKnown: true, BookSource: "r159-canary-current-ws"}
	cell := verdictEnt{SourceFamily: "spotlag", Platform: "kalshi", Side: "YES",
		OriginLayer: "model", Route: "taker", Group: "taker",
		N: 1, Markets: 1, Mean: .10, Lo: -.03, MeanAsk: .40, FeePC: .01}
	s.verdMu.Lock()
	s.verdCache, s.verdAt = []verdictEnt{cell}, time.Now()
	s.verdMu.Unlock()
	return s, c, q, cell
}

func TestR163CanaryIsAlwaysOneIOC(t *testing.T) {
	s, c, q, cell := r159CanaryPlanFixture(t, 1)
	cash, cashWhy := s.liveProspectivePlan(context.Background(), c, q, 400, cell, false)
	if cash.valid() || cashWhy != liveOneContractCanaryCashRetiredReason {
		t.Fatalf("model-point canary retained cash authority: plan=%+v why=%q", cash, cashWhy)
	}
	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c, q, 400, cell, false)
	if why != "" || !plan.valid() || !plan.Canary ||
		plan.Qty != 1 || plan.TimeInForce != liveProspectiveIOC ||
		plan.Lower >= s.liveAllocationEdgeFloor() ||
		!strings.HasPrefix(plan.Basis, "one-contract-canary:UNPROVEN:kalshi|spotlag|YES|taker:") {
		t.Fatalf("one-contract fallback plan=%+v why=%q", plan, why)
	}

	cfg := *s.cfg()
	cfg.Risk.LiveSystemCanaryAllowlist = ""
	s.cfgP.Store(&cfg)
	if disabled, disabledWhy := s.liveProspectivePlan(
		context.Background(), c, q, 400, cell, false); disabledWhy == "" || disabled.valid() {
		t.Fatalf("empty canary belt still granted plan=%+v why=%q", disabled, disabledWhy)
	}
}

func TestR163CanaryAllowlistForcesQ1EvenWhenNormalProofExists(t *testing.T) {
	s, c, q, cell := r159CanaryPlanFixture(t, liveAllocationMinMarketsFloor)
	cash, cashWhy := s.liveProspectivePlan(context.Background(), c, q, 400, cell, false)
	if cash.valid() || cashWhy != liveOneContractCanaryCashRetiredReason {
		t.Fatalf("mature model-point canary retained cash authority: plan=%+v why=%q", cash, cashWhy)
	}
	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c, q, 400, cell, false)
	if why != "" || !plan.valid() || !plan.Canary || plan.Qty != 1 ||
		plan.TimeInForce != liveProspectiveIOC ||
		!strings.Contains(plan.Basis, "normal-proof-refused=operator-selected-canary-only-route") {
		t.Fatalf("mature canary identity escaped q1 IOC: plan=%+v why=%q", plan, why)
	}

	// Removing only canary membership must not turn opportunity-only rows into normal cash proof.
	// The diagnostic plan stays measurable, while the real plan requires the separate
	// authoritative LIVE-fill cohort.
	cfg := *s.cfg()
	cfg.Risk.LiveSystemCanaryAllowlist = ""
	s.cfgP.Store(&cfg)
	normal, normalWhy := s.liveProspectivePlan(context.Background(), c, q, 400, cell, false)
	if normal.valid() || normalWhy != liveFillConditionedProofUnavailableReason {
		t.Fatalf("non-canary route escaped fill-conditioned fence: plan=%+v why=%q",
			normal, normalWhy)
	}
	diagnostic, diagnosticWhy := s.liveProspectiveDiagnosticPlan(
		context.Background(), c, q, 400, cell, false)
	if diagnosticWhy != "" || !diagnostic.valid() || diagnostic.Canary ||
		!strings.HasPrefix(
			diagnostic.Basis, "prospective-allocation:spotlag@kalshi[YES]/taker:") {
		t.Fatalf("normal diagnostic calculation disappeared: plan=%+v why=%q",
			diagnostic, diagnosticWhy)
	}
}

func TestR159CanaryDispatchFieldsNeverPretendLowerBoundIsPositive(t *testing.T) {
	q := liveMirrorQuote{Price: .40, Depth: 1, Tick: .01}
	basis := "one-contract-canary:UNPROVEN:kalshi|spotlag|YES|taker:" +
		"point-source=exact-paper-model-cell:input=K+SPOT:contract=x"
	fields, ok := liveCanaryDispatchFields(q, basis, .03, -.07, .01, .005, time.Now())
	if !ok || fields["canary_state"] != "UNPROVEN_CANARY" ||
		fields["canary_observed_lower"] != -.07 ||
		fields["canary_authority"] != true {
		t.Fatalf("canary telemetry=%v ok=%v", fields, ok)
	}
	if _, normalOK := liveMirrorProofDispatchFields(q, basis, .03, -.07, .01,
		.005, time.Now()); normalOK {
		t.Fatal("negative canary lower bound passed the normal proof contract")
	}
}

func TestR159CanaryWireContractIsExactlyOneTakerIOC(t *testing.T) {
	if why := liveOneContractCanaryWireReason(
		true, true, false, false, true, false, 1); why != "" {
		t.Fatalf("exact canary refused: %q", why)
	}
	for _, tc := range []struct {
		auto, sealed, ml, taker, fok bool
		count                        int
	}{
		{false, false, false, true, false, 1},
		{true, true, false, true, false, 1},
		{true, false, true, true, false, 1},
		{true, false, false, false, false, 1},
		{true, false, false, true, true, 1},
		{true, false, false, true, false, 2},
	} {
		if why := liveOneContractCanaryWireReason(true, tc.auto, tc.sealed, tc.ml,
			tc.taker, tc.fok, tc.count); why == "" {
			t.Fatalf("invalid canary wire accepted: %+v", tc)
		}
	}
}

func TestR163CurrentCanaryConfigStopsInFlightNormalOrderAtWire(t *testing.T) {
	s, c, _, _ := r159CanaryPlanFixture(t, liveAllocationMinMarketsFloor)
	cfg := *s.cfg()
	cfg.Risk.LiveSystemCanaryAllowlist = ""
	s.cfgP.Store(&cfg)
	if why := s.liveConfiguredCanaryWireReason(
		c, "taker", false, true, false, false, true, false, 7); why != "" {
		t.Fatalf("normal route was blocked before canary membership changed: %q", why)
	}

	cfg = *s.cfg()
	cfg.Risk.LiveSystemCanaryAllowlist = "kalshi|spotlag|YES|taker"
	s.cfgP.Store(&cfg)
	if why := s.liveConfiguredCanaryWireReason(
		c, "taker", false, true, false, false, true, false, 7); !strings.Contains(
		why, "current exact System identity is canary-only") {
		t.Fatalf("in-flight normal order escaped new canary ceiling: %q", why)
	}
	if why := s.liveConfiguredCanaryWireReason(
		c, "taker", true, true, false, false, true, false, 2); !strings.Contains(
		why, "count=1") {
		t.Fatalf("multi-contract claimed canary escaped current wire ceiling: %q", why)
	}
	if why := s.liveConfiguredCanaryWireReason(
		c, "taker", true, true, false, false, true, false, 1); why != "" {
		t.Fatalf("exact q1 IOC canary was refused at current wire: %q", why)
	}

	cfg = *s.cfg()
	cfg.Risk.LiveSystemCanaryAllowlist = ""
	s.cfgP.Store(&cfg)
	if why := s.liveConfiguredCanaryWireReason(
		c, "taker", true, true, false, false, true, false, 1); !strings.Contains(
		why, "no longer in the current canary allowlist") {
		t.Fatalf("removed canary authority survived a current-config wire check: %q", why)
	}
}

func TestR159HandlerReproofRetiresModelPointCashAuthority(t *testing.T) {
	s, c, q, _ := r159CanaryPlanFixture(t, 1)
	c.CanaryBasis = "one-contract-canary:UNPROVEN:kalshi|some-other-system|NO|taker"
	plan, why := s.liveOneContractCanaryReproof(context.Background(), c, q)
	if plan.valid() || why != liveOneContractCanaryCashRetiredReason {
		t.Fatalf("handler model-point canary retained cash authority=%+v why=%q", plan, why)
	}
}

func r159PreparedCanaryCandidate(t *testing.T) (*Server, liveMirrorCandidate) {
	t.Helper()
	s, c, q, cell := r159CanaryPlanFixture(t, 1)
	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c, q, 400, cell, false)
	if why != "" || !plan.valid() || !plan.Canary {
		t.Fatalf("prepare canary plan=%+v why=%q", plan, why)
	}
	applyLiveProspectivePlan(&c, plan)
	c.ArbitrationQuote = q
	c.ArbitrationBasis = plan.Basis
	c.ArbitrationMean = plan.Mean
	c.ArbitrationLower = plan.Lower
	c.ArbitrationFee = plan.ProofFeePC
	c.ArbitrationAt = time.Now()
	c.LiveFirstUnitPersisted = true
	s.liveMirrorMu.Lock()
	s.liveAllocationSignal[liveAllocationSignalKey(c)] = c.ArbitrationAt
	s.liveMirrorMu.Unlock()
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	return s, c
}

func TestR159CanaryNegativeLowerSurvivesLiveFirstAndArbitrationReuse(t *testing.T) {
	s, c := r159PreparedCanaryCandidate(t)
	if c.ArbitrationLower >= s.liveAllocationEdgeFloor() {
		t.Fatalf("fixture unexpectedly has proved lower=%v", c.ArbitrationLower)
	}
	now := time.Now()
	if why := s.liveOneContractCanaryReceiptReason(c); why != "" {
		t.Fatalf("valid internal canary receipt refused: %q", why)
	}
	if !s.liveMirrorArbitrationReceiptReusable(c, now, time.Second) {
		t.Fatal("negative-lower UNPROVEN canary was not reusable")
	}
	if !s.liveFirstMirrorReceiptFresh(c, now) {
		t.Fatal("negative-lower UNPROVEN canary fell into the legacy observe path")
	}
	ranked, why := s.preflightLiveMirrorRank(context.Background(), c)
	if why != "" || !ranked.Canary || !ranked.Candidate.Canary ||
		ranked.Lower != c.ArbitrationLower || ranked.Lower >= 0 ||
		ranked.Mean < s.liveAllocationEdgeFloor() {
		t.Fatalf("canary arbitration row=%+v why=%q", ranked, why)
	}
}

func TestR159NegativeLowerWithoutExactCanaryShapeRemainsRejected(t *testing.T) {
	s, c := r159PreparedCanaryCandidate(t)
	normal := c
	normal.Canary, normal.CanaryBasis = false, ""
	if s.liveMirrorArbitrationReceiptReusable(normal, time.Now(), time.Second) {
		t.Fatal("negative lower was reused as normal proof")
	}
	bulk := c
	bulk.ProspectiveQty = 2
	if s.liveMirrorArbitrationReceiptReusable(bulk, time.Now(), time.Second) {
		t.Fatal("multi-contract canary receipt entered arbitration")
	}
	wrongBasis := c
	wrongBasis.CanaryBasis = "one-contract-canary:UNPROVEN:forged"
	if s.liveMirrorArbitrationReceiptReusable(wrongBasis, time.Now(), time.Second) {
		t.Fatal("inconsistent canary basis entered arbitration")
	}
}

func TestR159ArbitrationAlwaysRanksNormalProofAheadOfCanary(t *testing.T) {
	normal := liveMirrorRankedCandidate{Canary: false, LowerPerDay: .01, MeanPerDay: .02}
	canary := liveMirrorRankedCandidate{Canary: true, LowerPerDay: 10, MeanPerDay: 20}
	if !strongerLiveMirrorRank(normal, canary) || strongerLiveMirrorRank(canary, normal) {
		t.Fatal("UNPROVEN canary outranked a positive-lower-bound candidate")
	}
	weakPoint := liveMirrorRankedCandidate{Canary: true, LowerPerDay: 100, MeanPerDay: .01}
	strongPoint := liveMirrorRankedCandidate{Canary: true, LowerPerDay: -100, MeanPerDay: .02}
	if !strongerLiveMirrorRank(strongPoint, weakPoint) {
		t.Fatal("canaries were ranked by diagnostic lower instead of current point profit-rate")
	}
}
