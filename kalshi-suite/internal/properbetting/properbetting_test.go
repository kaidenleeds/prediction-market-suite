package properbetting

import (
	"math"
	"testing"
)

func closeTo(a, b float64) bool { return math.Abs(a-b) < 1e-10 }

func TestPaperPositionVectorsAndRealizedDecomposition(t *testing.T) {
	p := []float64{.50, .30, .20}
	q := []float64{.40, .35, .25}
	for _, transform := range []Transform{Brier, Log, Spherical} {
		pos, err := NewPosition(transform, p, q)
		if err != nil {
			t.Fatal(err)
		}
		for i := range p {
			var want float64
			switch transform {
			case Brier:
				want = 2 * (p[i] - q[i])
			case Log:
				want = math.Log(p[i]) - math.Log(q[i])
			case Spherical:
				want = p[i]/math.Sqrt(.38) - q[i]/math.Sqrt(.345)
			}
			if !closeTo(pos.Raw[i], want) {
				t.Fatalf("%s raw[%d]=%.12f want %.12f", transform, i, pos.Raw[i], want)
			}
		}
		for outcome := range p {
			profit := 0.0
			for i, s := range pos.Raw {
				y := 0.0
				if i == outcome {
					y = 1
				}
				profit += s * (y - q[i])
			}
			sp, _ := Score(transform, p, outcome)
			sq, _ := Score(transform, q, outcome)
			d, _ := Bregman(transform, q, p)
			if !closeTo(profit, sp-sq+d) {
				t.Fatalf("%s outcome %d decomposition %.12f != %.12f", transform, outcome, profit, sp-sq+d)
			}
		}
	}
}

func TestConstantShiftAndPositiveRescaleAreOnlyFrictionlessSimplexInvariant(t *testing.T) {
	p, q := []float64{.6, .25, .15}, []float64{.4, .35, .25}
	pos, err := NewPosition(Brier, p, q)
	if err != nil {
		t.Fatal(err)
	}
	rescaled, err := pos.Rescaled(7.5)
	if err != nil {
		t.Fatal(err)
	}
	for outcome := range p {
		raw, shifted, scaled := 0.0, 0.0, 0.0
		for i := range p {
			y := 0.0
			if i == outcome {
				y = 1
			}
			raw += pos.Raw[i] * (y - q[i])
			shifted += pos.Canonical[i] * (y - q[i])
			scaled += rescaled.Canonical[i] * (y - q[i])
		}
		if !closeTo(raw, shifted) || !closeTo(scaled, 7.5*raw) {
			t.Fatalf("outcome %d raw=%v shifted=%v scaled=%v", outcome, raw, shifted, scaled)
		}
	}
	// Crossing every shifted outcome at asks pays spread/fees and is not a free representation
	// change. A 2-outcome +c shift costs c*(.55+.55)>c before fees.
	shift := .4
	clobCost, guaranteedPayout := shift*(.55+.55), shift
	if clobCost <= guaranteedPayout {
		t.Fatalf("test fixture did not expose complete-set CLOB friction: cost=%v payout=%v", clobCost, guaranteedPayout)
	}
}

func TestBinaryBrierCanonicalScaleIsNormalizedAway(t *testing.T) {
	p, q := .70, .50
	pos, err := NewPosition(Brier, []float64{p, 1 - p}, []float64{q, 1 - q})
	if err != nil {
		t.Fatal(err)
	}
	if !closeTo(pos.Raw[0], 2*(p-q)) || !closeTo(pos.Raw[1], -2*(p-q)) ||
		!closeTo(pos.Canonical[0], 4*(p-q)) || pos.Canonical[1] != 0 {
		t.Fatalf("binary Brier raw/canonical=%+v", pos)
	}
	// The common factor four in the one-sided representation cannot flatter portfolio results:
	// L1 normalization produces the same weights as the underlying absolute margins.
	canonical, _ := NormalizeWeights([]float64{4 * .20, 4 * .10}, 1)
	margins, _ := NormalizeWeights([]float64{.20, .10}, 1)
	for i := range canonical {
		if !closeTo(canonical[i], margins[i]) {
			t.Fatalf("canonical representation scale leaked into weights: %v vs %v", canonical, margins)
		}
	}
}

func TestBidAskUsesSideSpecificQtildeAndNoTradeZone(t *testing.T) {
	actions, err := BidAskActions(Brier, []float64{.70, .47, .30},
		[]float64{.55, .50, .55}, []float64{.50, .55, .55})
	if err != nil {
		t.Fatal(err)
	}
	if actions[0].Side != "YES" || actions[0].Qtilde != .55 || !closeTo(actions[0].Target, .60) {
		t.Fatalf("YES action=%+v", actions[0])
	}
	if !actions[1].NoTrade || actions[1].Side != "NONE" || actions[1].Target != 0 {
		t.Fatalf("dead-zone action=%+v", actions[1])
	}
	if actions[2].Side != "NO" || !closeTo(actions[2].Qtilde, .45) || !closeTo(actions[2].ForecastSide, .70) ||
		!closeTo(actions[2].Target, .60) {
		t.Fatalf("NO action=%+v", actions[2])
	}
}

func TestFullDepthWalkComputesSlippagePerLevelFeesLotsAndCancellation(t *testing.T) {
	levels := []Level{{Price: .40, Quantity: 2}, {Price: .42, Quantity: 3}, {Price: .45, Quantity: 10}}
	fee := func(fills []Fill) (float64, bool) {
		total := 0.0
		for _, fill := range fills {
			total += fill.Quantity * fill.Price * (1 - fill.Price) * .05
		}
		return total, true
	}
	r, err := WalkDepth(levels, 6.8, .65, .01, 1, fee)
	if err != nil {
		t.Fatal(err)
	}
	wantCost := 2*.40 + 3*.42 + 1*.45
	wantFee := 2*.40*.60*.05 + 3*.42*.58*.05 + .45*.55*.05
	if r.RoundedRequest != 6 || r.Filled != 6 || !r.FullFill || r.LevelsUsed != 3 ||
		!closeTo(r.IntegratedCost, wantCost) || !closeTo(r.SpotCost, 6*.40) ||
		!closeTo(r.LiquidityLoss, wantCost-6*.40) || !closeTo(r.Fee, wantFee) ||
		!closeTo(r.ExpectedNet, 6*.65-wantCost-wantFee) {
		t.Fatalf("depth evaluation=%+v", r)
	}
	partial, err := WalkDepth(levels[:1], 5, .65, .01, 1, fee)
	if err != nil || partial.FullFill || partial.Filled != 2 || partial.Cancelled != 3 {
		t.Fatalf("partial=%+v err=%v", partial, err)
	}
}

func TestSaleDepthWalkUsesBidsAndActualSimulatedFills(t *testing.T) {
	levels := []Level{{Price: .55, Quantity: 1, Tick: .01}, {Price: .53, Quantity: 2, Tick: .01}}
	fee := func(fills []Fill) (float64, bool) {
		raw := 0.0
		for _, fill := range fills {
			raw += .07 * fill.Quantity * fill.Price * (1 - fill.Price)
		}
		return raw, true
	}
	r, err := WalkSaleDepth(levels, 4, .40, .01, 1, fee)
	if err != nil || r.Filled != 3 || r.Cancelled != 1 || r.FullFill ||
		!closeTo(r.IntegratedValue, .55+2*.53) || !closeTo(r.SpotValue, 3*.55) ||
		!closeTo(r.LiquidityLoss, 3*.55-(.55+2*.53)) ||
		!closeTo(r.ExpectedNet, r.IntegratedValue-r.Fee-3*.40) {
		t.Fatalf("sale depth=%+v err=%v", r, err)
	}
}

func TestDepthWalkRejectsOffTickAndUnknownNonQuadraticFee(t *testing.T) {
	if _, err := WalkDepth([]Level{{Price: .405, Quantity: 2}}, 1, .6, .01, 1,
		func([]Fill) (float64, bool) { return 0, true }); err == nil {
		t.Fatal("off-tick depth admitted")
	}
	if _, err := WalkDepth([]Level{{Price: .40, Quantity: 2}}, 1, .6, .01, 1,
		func([]Fill) (float64, bool) { return 0, false }); err == nil {
		t.Fatal("unknown fee admitted")
	}
}

func TestMomentumRebalanceUsesActualFillsAndCancelledOrdersMoveNothing(t *testing.T) {
	first, err := ApplyRebalance(0, 8, 3)
	if err != nil || first.PostActual != 3 || first.Cancelled != 5 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := ApplyRebalance(first.PostActual, 5, 0)
	if err != nil || second.Requested != 2 || second.PostActual != 3 || second.Cancelled != 2 {
		t.Fatalf("cancelled second=%+v err=%v", second, err)
	}
	if _, err := ApplyRebalance(second.PostActual, -2, -5); err == nil {
		t.Fatal("one signed fill hid a two-route side crossing")
	}
	legs, err := PlanRebalance(second.PostActual, -2)
	if err != nil || len(legs) != 2 || legs[0] != (RebalanceLeg{Action: "SELL", Side: "YES", Quantity: 3}) ||
		legs[1] != (RebalanceLeg{Action: "BUY", Side: "NO", Quantity: 2}) {
		t.Fatalf("side crossing plan=%+v err=%v", legs, err)
	}
	if _, err := ApplyRebalance(3, 5, -1); err == nil {
		t.Fatal("opposite-direction fill admitted")
	}
}

func TestBoundaryForecastsSupportedExceptLog(t *testing.T) {
	if _, err := BidAskActions(Brier, []float64{1}, []float64{.60}, []float64{.45}); err != nil {
		t.Fatalf("Brier boundary rejected: %v", err)
	}
	if _, err := BidAskActions(Spherical, []float64{0}, []float64{.60}, []float64{.45}); err != nil {
		t.Fatalf("spherical boundary rejected: %v", err)
	}
	if _, err := BidAskActions(Log, []float64{1}, []float64{.60}, []float64{.45}); err == nil {
		t.Fatal("log boundary admitted without finite log odds")
	}
}

func TestFeeAdapterControlsAggregateRounding(t *testing.T) {
	aggregateRounded := func(fills []Fill) (float64, bool) {
		raw := 0.0
		for _, f := range fills {
			raw += f.Quantity * f.Price * (1 - f.Price) * .07
		}
		return math.Ceil(raw*100) / 100, true
	}
	r, err := WalkDepth([]Level{{Price: .40, Quantity: 1}, {Price: .41, Quantity: 1}}, 2, .7, .01, 1, aggregateRounded)
	if err != nil || r.Fee != .04 {
		t.Fatalf("aggregate venue rounding fee=%v err=%v", r.Fee, err)
	}
}

func TestDepthWalkUsesEachDynamicPriceBandTick(t *testing.T) {
	levels := []Level{{Price: .095, Quantity: 1, Tick: .005}, {Price: .10, Quantity: 1, Tick: .01}}
	r, err := WalkDepth(levels, 2, .4, .01, 1, func([]Fill) (float64, bool) { return 0, true })
	if err != nil || r.MinimumTick != .005 || r.Filled != 2 {
		t.Fatalf("dynamic tick walk=%+v err=%v", r, err)
	}
}

func TestDepthWalkPreservesAuthoritativeMakerRebate(t *testing.T) {
	r, err := WalkDepth([]Level{{Price: .4, Quantity: 2}}, 2, .6, .01, 1,
		func([]Fill) (float64, bool) { return -.01, true })
	if err != nil || r.Fee != -.01 || !closeTo(r.ExpectedNet, .41) {
		t.Fatalf("maker rebate=%+v err=%v", r, err)
	}
}

func TestPortfolioNormalizationIsShareBasedAndDirectionPreserving(t *testing.T) {
	w, err := NormalizeWeights([]float64{2, -1, .5}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !closeTo(math.Abs(w[0])+math.Abs(w[1])+math.Abs(w[2]), 1) || w[0] <= 0 || w[1] >= 0 || w[2] <= 0 {
		t.Fatalf("weights=%v", w)
	}
}
