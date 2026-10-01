package server

import (
	"math"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func polyUSPendingFixture(side, route string) storage.PendingResearchLiveExecution {
	in := storage.ResearchPromotionIntent{
		Candidate: storage.ResearchPromotionCandidate{Venue: "polyus", Ticker: "market-1", Side: side},
		Proof:     storage.ResearchPromotionProof{Route: route},
	}
	return storage.PendingResearchLiveExecution{Intent: in, Receipt: storage.ResearchLiveExecutionReceipt{
		Venue: "polyus", Ticker: "market-1", Side: side, Route: route, OrderID: "order-1",
		State: "pending", RequestedQty: 1, RemainingQty: 1, AveragePrice: .40,
	}}
}

func TestR140PolyUSPromotionOrderReceiptExactMoneyTruth(t *testing.T) {
	taker := polyUSPendingFixture("YES", "taker")
	full := polymarketus.OrderState{ID: "order-1", State: "ORDER_STATE_FILLED", Cum: 1, Leaves: 0,
		AvgPx: .41, Intent: "ORDER_INTENT_BUY_LONG", TIF: "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
		CommissionTotalKnown: true}
	r := polyUSPromotionOrderReceipt(taker, full)
	if r.State != "full" || !r.Authoritative || !r.FeeKnown || r.FilledQty != 1 || r.RemainingQty != 0 || r.AveragePrice != .41 {
		t.Fatalf("exact full receipt=%+v", r)
	}

	makerNO := polyUSPendingFixture("NO", "maker")
	full.Intent, full.TIF, full.AvgPx, full.CommissionTotalUSD = "ORDER_INTENT_BUY_SHORT", "TIME_IN_FORCE_GOOD_TILL_CANCEL", .37, -.0125
	r = polyUSPromotionOrderReceipt(makerNO, full)
	if r.State != "full" || r.FeeTotal != 0 || math.Abs(r.RebateTotal-.0125) > 1e-12 {
		t.Fatalf("signed maker rebate was not preserved: %+v", r)
	}
}

func TestR140PolyUSPromotionOrderReceiptFailsClosedOnIdentityOrEconomicsDrift(t *testing.T) {
	p := polyUSPendingFixture("YES", "taker")
	base := polymarketus.OrderState{ID: "order-1", State: "ORDER_STATE_FILLED", Cum: 1, AvgPx: .42,
		Intent: "ORDER_INTENT_BUY_LONG", TIF: "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", CommissionTotalKnown: true}
	tests := map[string]polymarketus.OrderState{
		"order id":    func() polymarketus.OrderState { x := base; x.ID = "other"; return x }(),
		"side":        func() polymarketus.OrderState { x := base; x.Intent = "ORDER_INTENT_BUY_SHORT"; return x }(),
		"route":       func() polymarketus.OrderState { x := base; x.TIF = "TIME_IN_FORCE_GOOD_TILL_CANCEL"; return x }(),
		"average":     func() polymarketus.OrderState { x := base; x.AvgPx = 0; return x }(),
		"commission":  func() polymarketus.OrderState { x := base; x.CommissionTotalKnown = false; return x }(),
		"overfill":    func() polymarketus.OrderState { x := base; x.Cum = 2; return x }(),
		"working qty": func() polymarketus.OrderState { x := base; x.Leaves = .25; return x }(),
	}
	for name, st := range tests {
		r := polyUSPromotionOrderReceipt(p, st)
		if r.State != "ambiguous" || r.Authoritative {
			t.Fatalf("%s drift did not fail closed: %+v", name, r)
		}
	}
}

func TestR140PolyUSPromotionOrderReceiptLifecycle(t *testing.T) {
	p := polyUSPendingFixture("YES", "maker")
	base := polymarketus.OrderState{ID: "order-1", Intent: "ORDER_INTENT_BUY_LONG",
		TIF: "TIME_IN_FORCE_GOOD_TILL_CANCEL", CommissionTotalKnown: true}

	dead := base
	dead.State = "ORDER_STATE_CANCELED"
	r := polyUSPromotionOrderReceipt(p, dead)
	if r.State != "unfilled" || !r.Authoritative || r.RemainingQty != 0 || !r.FeeKnown {
		t.Fatalf("dead unfilled=%+v", r)
	}

	terminalPartial := base
	terminalPartial.State, terminalPartial.Cum, terminalPartial.AvgPx = "ORDER_STATE_CANCELED", .4, .39
	r = polyUSPromotionOrderReceipt(p, terminalPartial)
	if r.State != "partial" || !r.Authoritative || r.RemainingQty != 0 {
		t.Fatalf("terminal partial=%+v", r)
	}

	openPartial := base
	openPartial.State, openPartial.Cum, openPartial.Leaves, openPartial.AvgPx = "ORDER_STATE_PARTIALLY_FILLED", .4, .6, .39
	r = polyUSPromotionOrderReceipt(p, openPartial)
	if r.State != "partial" || !r.Authoritative || math.Abs(r.RemainingQty-.6) > 1e-12 {
		t.Fatalf("open partial=%+v", r)
	}
}

func TestR140LiveAutoPromotionContractBindsBothSingleVenues(t *testing.T) {
	in := storage.ResearchPromotionIntent{Candidate: storage.ResearchPromotionCandidate{
		Venue: "polyus", Ticker: "market-1", Side: "NO"}, Proof: storage.ResearchPromotionProof{Route: "maker"}}
	if why := liveAutoPromotionContractReason(in, "polyus", "market-1", "no", "maker"); why != "" {
		t.Fatalf("identical PolyUS single contract rejected: %s", why)
	}
	for name, args := range map[string][4]string{
		"venue":  {"kalshi", "market-1", "NO", "maker"},
		"market": {"polyus", "market-2", "NO", "maker"},
		"side":   {"polyus", "market-1", "YES", "maker"},
		"route":  {"polyus", "market-1", "NO", "taker"},
	} {
		if why := liveAutoPromotionContractReason(in, args[0], args[1], args[2], args[3]); why == "" {
			t.Fatalf("%s drift bypassed sealed Paper contract", name)
		}
	}
}
