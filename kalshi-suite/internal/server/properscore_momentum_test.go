package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestProperMomentumExecutionRequiresConfiguredDelayAndNewerPrepriceClock(t *testing.T) {
	now := time.Now().UTC()
	in := momentumIntentFixture("YES", 1)
	in.Preprice = storage.ProperMomentumSellBookReceipt{SourceClockID: "preprice-clock",
		SourceClockAt: now}
	in.ExecutionDelayMS, in.ExecuteAfter = 750, now.Add(750*time.Millisecond)
	if properMomentumExecutionBookIsNewer(in, "preprice-clock", now.Add(time.Second)) {
		t.Fatal("same preprice frame became execution")
	}
	if properMomentumExecutionBookIsNewer(in, "different-id-same-time", now) {
		t.Fatal("different id without a later source clock became execution")
	}
	if !properMomentumExecutionBookIsNewer(in, "execution-clock", now.Add(time.Millisecond)) {
		t.Fatal("strictly newer complete execution frame was rejected")
	}
	legacy := in
	legacy.Preprice.SourceClockID = ""
	if properMomentumExecutionBookIsNewer(legacy, "execution-clock", now.Add(time.Second)) {
		t.Fatal("legacy intent without persisted preprice provenance became executable")
	}
	s := testServer(t)
	// Server setup can take longer than the production delay under a busy full-suite run.
	// Anchor the delay assertion after setup so this test proves the gate, not test speed.
	in.ExecuteAfter = time.Now().UTC().Add(time.Minute)
	if placed, why, execution := s.executeProperMomentumPaperSell(context.Background(), in); placed ||
		why != "paper-sell-configured-delay-not-elapsed" || execution != nil {
		t.Fatalf("configured delay bypassed placed=%v why=%q receipt=%+v", placed, why, execution)
	}
}

func TestProperMomentumSingleSellRequiresOneFullNonCrossLeg(t *testing.T) {
	valid := properScoreSimResult{prior: 1, target: 0, requested: -1, filled: -1,
		cancelled: 0, post: 0, expected: .08, status: "filled", legs: []map[string]any{{
			"action": "SELL", "side": "YES", "requested": 1.0, "filled": 1.0,
			"cancelled": 0.0, "integrated_proceeds": .62, "fee": .01,
			"fee_source": "kalshi:test", "status": "filled",
		}}}
	leg, ok := properMomentumSingleSell(valid)
	if !ok || leg.OwnedSide != "YES" || leg.SyntheticSide != "NO" || leg.Proceeds != .62 || leg.Fee != .01 {
		t.Fatalf("valid typed SELL rejected: %+v ok=%v", leg, ok)
	}
	partial := valid
	partial.status, partial.filled, partial.cancelled = "partial", -.5, -.5
	partial.legs = []map[string]any{{"action": "SELL", "side": "YES", "requested": 1.0,
		"filled": .5, "cancelled": .5, "integrated_proceeds": .31, "fee": .01,
		"fee_source": "kalshi:test", "status": "partial"}}
	if _, ok := properMomentumSingleSell(partial); ok {
		t.Fatal("partial SELL became promotable")
	}
	partialReduction := valid
	partialReduction.prior, partialReduction.target, partialReduction.post = 2, 1, 1
	if _, ok := properMomentumSingleSell(partialReduction); ok {
		t.Fatal("multi-share lot reduction bypassed exact one-promoted-lot contract")
	}
	cross := valid
	cross.prior, cross.target, cross.requested, cross.filled, cross.post = 1, -1, -2, -2, -1
	cross.legs = append(cross.legs, map[string]any{"action": "BUY", "side": "NO",
		"requested": 1.0, "filled": 1.0, "cancelled": 0.0, "status": "filled"})
	if _, ok := properMomentumSingleSell(cross); ok {
		t.Fatal("non-atomic close+open cross became a single SELL")
	}
}

func momentumIntentFixture(side string, prior float64) storage.ProperMomentumSellIntent {
	return storage.ProperMomentumSellIntent{IntentID: "intent", Candidate: storage.ProperMomentumSellCandidate{
		Ticker: "KXTEST", OwnedSide: side, RequiredPrior: prior}}
}

func TestProperMomentumKalshiRequestIsReduceOnlyFOK(t *testing.T) {
	yes, err := properMomentumKalshiFOKRequest(momentumIntentFixture("YES", 1), .61)
	if err != nil || yes.Side != "ask" || yes.Price != "0.61" || yes.Count != "1" ||
		yes.TimeInForce != "fill_or_kill" || !yes.ReduceOnly || yes.PostOnly ||
		yes.SelfTradePreventionType != "taker_at_cross" || !yes.CancelOrderOnPause {
		t.Fatalf("unsafe YES SELL request: %+v err=%v", yes, err)
	}
	no, err := properMomentumKalshiFOKRequest(momentumIntentFixture("NO", -1), .61)
	if err != nil || no.Side != "bid" || no.Price != "0.39" || !no.ReduceOnly {
		t.Fatalf("unsafe NO SELL request: %+v err=%v", no, err)
	}
	if _, err := properMomentumKalshiFOKRequest(momentumIntentFixture("BAD", 1), .5); err == nil {
		t.Fatal("invalid owned side reached wire")
	}
	if _, err := properMomentumKalshiFOKRequest(momentumIntentFixture("YES", 2), .5); err == nil {
		t.Fatal("multi-share prior reached one-lot reduce-only wire")
	}
}

func TestProperMomentumLiveRequiresExactPositionAndNoOrder(t *testing.T) {
	in := momentumIntentFixture("YES", 1)
	positions := []kalshi.MarketPosition{{Ticker: "KXTEST", Position: 1}}
	if why, qty := properMomentumKalshiPositionBlocker(in, positions, nil); why != "" || qty != 1 {
		t.Fatalf("exact position blocked: why=%q qty=%v", why, qty)
	}
	positions[0].Position = 2
	if why, _ := properMomentumKalshiPositionBlocker(in, positions, nil); why != "live-position-does-not-exactly-match-typed-prior" {
		t.Fatalf("wrong position blocker=%q", why)
	}
	positions[0].Position = 1
	if why, _ := properMomentumKalshiPositionBlocker(in, positions,
		[]kalshi.Order{{Ticker: "KXTEST", OrderID: "resting"}}); why != "existing-kalshi-resting-order" {
		t.Fatalf("resting order blocker=%q", why)
	}
}

func TestProperMomentumFOKReceiptRejectsPartialOrUnknown(t *testing.T) {
	if !properMomentumFullFOKReceipt(&kalshi.CreateOrderResult{FillCount: "1.00", RemainingCount: "0.00"}) {
		t.Fatal("full fixed-point FOK receipt rejected")
	}
	for _, row := range []*kalshi.CreateOrderResult{nil,
		{FillCount: ".5", RemainingCount: ".5"}, {FillCount: "1", RemainingCount: "bad"},
		{FillCount: "0", RemainingCount: "0"}} {
		if properMomentumFullFOKReceipt(row) {
			t.Fatalf("non-full receipt accepted: %+v", row)
		}
	}
}

func TestProperMomentumExpectedPostReducesOnlyOwnedSide(t *testing.T) {
	if got := properMomentumExpectedPost(1, "YES"); got != 0 {
		t.Fatalf("YES reduce post=%v", got)
	}
	if got := properMomentumExpectedPost(-1, "NO"); got != 0 {
		t.Fatalf("NO reduce post=%v", got)
	}
}
