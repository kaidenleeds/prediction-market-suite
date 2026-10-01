package server

import (
	"context"
	"math"
	"testing"
)

func r159QuantityPlanFixture(t *testing.T, depth float64) (*Server, liveMirrorCandidate,
	liveMirrorQuote, verdictEnt) {
	t.Helper()
	s, c := r147AllocationServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveMaxOrderPct = .05
	cfg.Risk.LiveMaxOrderUSD = -1
	cfg.Risk.LiveKellyMaxFrac = .50
	s.cfgP.Store(&cfg)
	s.liveBankArm = map[string]float64{"kalshi": 400}
	s.maxContracts = 100
	q := liveMirrorQuote{Price: .40, Depth: depth, MinQty: 1, MinQtyKnown: true,
		BookSource: "r159-exact-book"}
	cell := verdictEnt{Mean: .10, MeanAsk: .40, FeePC: .01}
	return s, c, q, cell
}

func r159SetAllocationFloorBetween(t *testing.T, s *Server, c liveMirrorCandidate,
	price float64, cheapQty, expensiveQty float64) {
	t.Helper()
	ctx := context.Background()
	_, _, _, cheapLower, _ := s.liveProspectiveDiagnosticProofAtQuantity(
		ctx, c, price, false, cheapQty)
	_, _, _, expensiveLower, _ := s.liveProspectiveDiagnosticProofAtQuantity(
		ctx, c, price, false, expensiveQty)
	if cheapLower <= expensiveLower {
		t.Fatalf("fixture fee did not make q%.0f cheaper than q%.0f: lower %.9f <= %.9f",
			cheapQty, expensiveQty, cheapLower, expensiveLower)
	}
	cfg := *s.cfg()
	cfg.Risk.LiveAllocationMinEdge = (cheapLower + expensiveLower) / 2
	s.cfgP.Store(&cfg)
}

func TestR159OneContractFailBulkExactFeeUsesFOK(t *testing.T) {
	s, c, q, cell := r159QuantityPlanFixture(t, 4)
	r159SetAllocationFloorBetween(t, s, c, q.Price, 4, 1)

	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c, q, 400, cell, false)
	if why != "" || !plan.valid() || plan.Qty != 4 ||
		plan.TimeInForce != liveProspectiveFOK {
		t.Fatalf("bulk-safe plan=%+v why=%q, want q4 FOK", plan, why)
	}
	one, _, _, oneKnown := s.liveExactAggregateBuyFee(c, 1, q.Price)
	if !oneKnown || !(plan.RequestedFeePC < one) {
		t.Fatalf("aggregate fee did not improve exact per-contract economics: q1=%v plan=%+v",
			one, plan)
	}
}

func TestR159OneContractSafeKeepsIOCForAnyPartial(t *testing.T) {
	s, c, q, cell := r159QuantityPlanFixture(t, 20)
	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c, q, 400, cell, false)
	if why != "" || !plan.valid() || plan.TimeInForce != liveProspectiveIOC ||
		plan.Qty < 1 {
		t.Fatalf("one-contract-safe plan=%+v why=%q, want IOC", plan, why)
	}
	oneFee := s.liveMirrorFeePCAtQuantity(c, false, 1, q.Price)
	if math.Abs(plan.ProofFeePC-oneFee) > 1e-12 {
		t.Fatalf("IOC proof fee=%v want one-contract worst case %v", plan.ProofFeePC, oneFee)
	}
}

func TestR159OneContractOperatorCapForcesCanaryIOC(t *testing.T) {
	s, c, q, cell := r159QuantityPlanFixture(t, 20)
	s.maxContracts = 1
	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c, q, 400, cell, false)
	if why != "" || !plan.valid() || plan.Qty != 1 ||
		plan.TimeInForce != liveProspectiveIOC {
		t.Fatalf("one-contract canary plan=%+v why=%q, want q1 IOC", plan, why)
	}
}

func TestR159CanaryAllowlistNamesOnlySpotLagAndRawKalshiFlowSides(t *testing.T) {
	raw := "kalshi|spotlag|YES|taker,kalshi|spotlag|NO|taker," +
		"kalshi|kalshi-flow|YES|taker,kalshi|kalshi-flow|NO|taker"
	allowed, canonical, why := parseLiveSystemAllowlist(raw)
	if why != "" || len(allowed) != 4 || len(canonical) != 4 {
		t.Fatalf("four exact canary lanes rejected: allowed=%v canonical=%v why=%q",
			allowed, canonical, why)
	}
	if _, wrongFamily := allowed["kalshi|kflow|YES|taker"]; wrongFamily {
		t.Fatal("the separate kflow family borrowed raw Kalshi-flow canary authority")
	}
}

func TestR159QuantitySearchHonorsNonMonotoneExactRounding(t *testing.T) {
	s, c, q, cell := r159QuantityPlanFixture(t, 5)
	fee4, pc4, _, ok4 := s.liveExactAggregateBuyFee(c, 4, q.Price)
	fee5, pc5, _, ok5 := s.liveExactAggregateBuyFee(c, 5, q.Price)
	if !ok4 || !ok5 || !(pc4 < pc5) || fee4 <= 0 || fee5 <= 0 {
		t.Fatalf("fixture lost non-monotone Kalshi rounding: q4 %.6f/%.6f q5 %.6f/%.6f",
			fee4, pc4, fee5, pc5)
	}
	r159SetAllocationFloorBetween(t, s, c, q.Price, 4, 5)
	plan, why := s.liveProspectiveDiagnosticPlan(context.Background(), c, q, 400, cell, false)
	if why != "" || plan.Qty != 4 || plan.TimeInForce != liveProspectiveFOK {
		t.Fatalf("descending exact search skipped cheaper q4: plan=%+v why=%q", plan, why)
	}
}

func TestR159FOKMirrorIsFullOrZeroAndPartialReceiptHalts(t *testing.T) {
	final := liveMirrorQuote{Price: .40, Depth: 3}
	if filled, why := livePolicyMirrorExecute(liveProspectiveFOK, .40, 4, final); filled != 0 || why != "mirror-fok-full-quantity-not-visible-after-wire-delay" {
		t.Fatalf("thin FOK mirror filled=%v why=%q", filled, why)
	}
	final.Depth = 4
	if filled, why := livePolicyMirrorExecute(liveProspectiveFOK, .40, 4, final); filled != 4 || why != "" {
		t.Fatalf("complete FOK mirror filled=%v why=%q", filled, why)
	}
	if why := liveProspectiveFOKPartialReason(liveProspectiveFOK, "partial", 1, 4); why != "fill-or-kill returned a partial fill" {
		t.Fatalf("partial FOK receipt was not treated as contract violation: %q", why)
	}
	if why := liveProspectiveFOKPartialReason(liveProspectiveIOC, "partial", 1, 4); why != "" {
		t.Fatalf("normal IOC partial was rejected: %q", why)
	}
}

func TestR159LiveAndIsolatedMirrorChooseSameExactPlanWithoutVenueIO(t *testing.T) {
	s, c, q, cell := r159QuantityPlanFixture(t, 4)
	r159SetAllocationFloorBetween(t, s, c, q.Price, 4, 1)
	// The fixture deliberately has no Kalshi client. A successful plan proves the quantity/fee
	// work uses the loaded fee registry and stored proof, not an extra REST call.
	if s.kal != nil {
		t.Fatal("fixture unexpectedly owns a venue client")
	}
	livePlan, liveWhy := s.liveProspectiveDiagnosticPlan(
		context.Background(), c, q, 400, cell, false)
	mirrorPlan, mirrorWhy := s.liveProspectiveDiagnosticPlan(
		context.Background(), c, q, 400, cell, true)
	if liveWhy != "" || mirrorWhy != "" || livePlan.Qty != mirrorPlan.Qty ||
		livePlan.TimeInForce != mirrorPlan.TimeInForce ||
		math.Abs(livePlan.RequestedFee-mirrorPlan.RequestedFee) > 1e-12 ||
		math.Abs(livePlan.ProofFeePC-mirrorPlan.ProofFeePC) > 1e-12 {
		t.Fatalf("LIVE/mirror plan mismatch:\nlive=%+v why=%q\nmirror=%+v why=%q",
			livePlan, liveWhy, mirrorPlan, mirrorWhy)
	}
}

func TestR159FinalWorsePriceCannotKeepPriorBulkPlan(t *testing.T) {
	s, c, q, cell := r159QuantityPlanFixture(t, 4)
	r159SetAllocationFloorBetween(t, s, c, q.Price, 4, 1)
	if plan, why := s.liveProspectiveDiagnosticPlan(
		context.Background(), c, q, 400, cell, false); why != "" ||
		plan.TimeInForce != liveProspectiveFOK {
		t.Fatalf("initial bulk plan unavailable: %+v why=%q", plan, why)
	}
	q.Price = .65
	if plan, why := s.liveProspectiveDiagnosticPlan(
		context.Background(), c, q, 400, cell, false); why == "" || plan.valid() {
		t.Fatalf("worse final price retained prior authorization: %+v why=%q", plan, why)
	}
}
