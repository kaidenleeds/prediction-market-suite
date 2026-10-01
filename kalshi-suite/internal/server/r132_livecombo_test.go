package server

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/ev"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR132LiveComboRequiresEveryExecutableAskAndModelScore(t *testing.T) {
	legs := []liveComboLegQuote{
		{Ticker: "A", Side: "YES", Ask: .40, Depth: 3, PWin: .55},
		{Ticker: "B", Side: "NO", Ask: .50, Depth: 2, PWin: .60},
	}
	exec, joint, ok := liveComboProducts(legs)
	if !ok || math.Abs(exec-.20) > 1e-12 || math.Abs(joint-(.55*.60*parlayCorrHaircut)) > 1e-12 {
		t.Fatalf("products=(%.8f, %.8f, %v)", exec, joint, ok)
	}
	for _, mutate := range []func([]liveComboLegQuote){
		func(v []liveComboLegQuote) { v[0].Depth = 0 },
		func(v []liveComboLegQuote) { v[0].Ask = 0 },
		func(v []liveComboLegQuote) { v[1].PWin = 0 },
	} {
		bad := append([]liveComboLegQuote(nil), legs...)
		mutate(bad)
		if _, _, ok := liveComboProducts(bad); ok {
			t.Fatal("incomplete executable/model leg must fail closed")
		}
	}
}

func TestR132StrategyAutoComboUsesFrechetJointLowerBound(t *testing.T) {
	legs := []liveComboLegQuote{
		{Ticker: "A", Side: "YES", Ask: .40, Depth: 3, PWin: .60},
		{Ticker: "B", Side: "YES", Ask: .50, Depth: 2, PWin: .60},
	}
	exec, joint, ok := liveStrategyComboProducts(legs)
	if !ok || math.Abs(exec-.20) > 1e-12 || math.Abs(joint-.20) > 1e-12 {
		t.Fatalf("strategy products=(%.8f, %.8f, %v), want exec .20 / Frechet .20", exec, joint, ok)
	}
	if _, _, _, reject := liveComboQuoteDecision(.25, 1, 0, exec, joint, 2, 2); reject == "" {
		t.Fatal("25¢ quote must reject against a 20¢ joint lower bound")
	}
	legs[0].PWin, legs[1].PWin = .90, .80
	exec, joint, ok = liveStrategyComboProducts(legs)
	if !ok || math.Abs(joint-.70) > 1e-12 {
		t.Fatalf("Frechet high-confidence joint=(%.8f,%v), want .70", joint, ok)
	}
	if _, _, _, reject := liveComboQuoteDecision(.15, 1, 0, exec, joint, 2, 2); reject != "" {
		t.Fatalf("sufficiently cheap quote should pass the strict joint bound: %s", reject)
	}
}

func TestR132LiveComboQuoteUsesFeeInAcceptanceAndRisk(t *testing.T) {
	cost, allIn, _, reject := liveComboQuoteDecision(.20, 2, .04, .30, .36, 2, 2)
	if reject != "" || math.Abs(cost-.44) > 1e-12 || math.Abs(allIn-.22) > 1e-12 {
		t.Fatalf("accepted decision cost=%.4f allIn=%.4f reject=%q", cost, allIn, reject)
	}
	_, _, _, reject = liveComboQuoteDecision(.251, 2, .04, .30, .36, 2, 2)
	if reject == "" {
		t.Fatal("fee-inclusive quote above the conservative ceiling must be rejected")
	}
}

func TestR132AutoComboSpendIsHandlerOwnedActualCost(t *testing.T) {
	s := &Server{}
	if got := s.recordAutoComboCost(false, .44); got != 0 {
		t.Fatalf("manual combo changed AUTO budget: %.4f", got)
	}
	if got := s.recordAutoComboCost(true, .44); math.Abs(got-.44) > 1e-12 {
		t.Fatalf("accepted AUTO actual cost not booked: %.4f", got)
	}
	if got := s.recordAutoComboCost(true, .61); math.Abs(got-1.05) > 1e-12 {
		t.Fatalf("ambiguous/second actual cost not accumulated exactly: %.4f", got)
	}
}

func TestR165AutoComboPaperProofCannotAuthorizeCash(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.ParlayEnabled = true
	s.cfgP.Store(&cfg)
	s.liveArmed, s.liveAuto = true, true
	code, out := s.selfPOST(s.handleLiveComboPlace, "/api/live/combo/place", map[string]any{
		"collection_ticker": "KXMVE-COLLECTION", "auto": true,
		"legs": []map[string]any{
			{"ticker": "KXA", "event": "KXEA", "side": "yes"},
			{"ticker": "KXB", "event": "KXEB", "side": "no"},
		},
	})
	if code != http.StatusForbidden || out["error"] != liveAutoComboCashRetiredReason {
		t.Fatalf("Paper/research combo proof reached LIVE AUTO: code=%d out=%v", code, out)
	}
	// This counter is telemetry, not the retired fixed session choke. Going past five dollars
	// remains observable and leaves authorization to the ordinary exact-cost risk rails.
	if got := s.recordAutoComboCost(true, 6.25); got != 6.25 {
		t.Fatalf("combo telemetry unexpectedly capped at %.2f", got)
	}
}

func TestR139ComboAcceptIsNotFillAndAuthoritativeFillsAggregateExactly(t *testing.T) {
	var rows []kalshi.Fill
	if err := json.Unmarshal([]byte(`[
 {"fill_id":"prior","order_id":"old-order","ticker":"KXMVE-NEW","outcome_side":"yes","count_fp":"5","yes_price_dollars":"0.10","fee_cost":"1"},
 {"fill_id":"f1","order_id":"rfq-order","ticker":"KXMVE-NEW","outcome_side":"yes","count_fp":"0.40","yes_price_dollars":"0.25","fee_cost":"0.003"},
 {"fill_id":"f2","order_id":"rfq-order","ticker":"KXMVE-NEW","outcome_side":"yes","count_fp":"0.60","yes_price_dollars":"0.30","fee_cost":"0.004"},
 {"fill_id":"other","order_id":"rfq-order","ticker":"OTHER","outcome_side":"yes","count_fp":"9","yes_price_dollars":"0.10","fee_cost":"1"}
]`), &rows); err != nil {
		t.Fatal(err)
	}
	qty, avg, fee, feeKnown, ids := liveComboFillAggregate(rows, "KXMVE-NEW", "rfq-order")
	if math.Abs(qty-1) > 1e-12 || math.Abs(avg-.28) > 1e-12 || math.Abs(fee-.007) > 1e-12 || !feeKnown || len(ids) != 2 {
		t.Fatalf("authoritative combo fill aggregate qty=%v avg=%v fee=%v ids=%v", qty, avg, fee, ids)
	}
}

func TestR139SealedSingleReconciliationRequiresExactOrderAndSide(t *testing.T) {
	var rows []kalshi.Fill
	if err := json.Unmarshal([]byte(`[
 {"fill_id":"wrong-order","order_id":"old","ticker":"KXONE","outcome_side":"yes","count_fp":"9","yes_price_dollars":"0.10","fee_cost":"1"},
 {"fill_id":"wrong-side","order_id":"new","ticker":"KXONE","outcome_side":"no","count_fp":"2","no_price_dollars":"0.60","fee_cost":".02"},
 {"fill_id":"right","order_id":"new","ticker":"KXONE","outcome_side":"yes","count_fp":"1","yes_price_dollars":"0.40","fee_cost":".01"}
]`), &rows); err != nil {
		t.Fatal(err)
	}
	qty, avg, fee, feeKnown, ids := kalshiOrderFillAggregate(rows, "new", "YES")
	if qty != 1 || avg != .4 || fee != .01 || !feeKnown || len(ids) != 1 || ids[0] != "right" {
		t.Fatalf("sealed single fill join admitted a different order/side: qty=%v avg=%v fee=%v ids=%v", qty, avg, fee, ids)
	}
}

func TestR140AuthoritativeFillFeeMustBePresentEvenWhenScheduleCouldRoundToZero(t *testing.T) {
	var rows []kalshi.Fill
	if err := json.Unmarshal([]byte(`[
 {"fill_id":"missing","order_id":"new","ticker":"KXONE","outcome_side":"yes","count_fp":"1","yes_price_dollars":"0.40"}
]`), &rows); err != nil {
		t.Fatal(err)
	}
	if _, _, _, known, _ := kalshiOrderFillAggregate(rows, "new", "YES"); known {
		t.Fatal("missing fee_cost masqueraded as an explicit zero fee")
	}
	if _, _, _, known, _ := liveComboFillAggregate(rows, "KXONE", "new"); known {
		t.Fatal("combo fill accepted missing fee_cost")
	}
	if err := json.Unmarshal([]byte(`[
 {"fill_id":"zero","order_id":"new","ticker":"KXONE","outcome_side":"yes","count_fp":"1","yes_price_dollars":"0.40","fee_cost":"0"}
]`), &rows); err != nil {
		t.Fatal(err)
	}
	if _, _, _, known, _ := kalshiOrderFillAggregate(rows, "new", "YES"); !known {
		t.Fatal("explicit venue zero fee was mistaken for a missing fee")
	}
}

func TestR132AutoComboAttemptCooldownDoesNotStarveDistinctPairs(t *testing.T) {
	s := &Server{}
	legsA := []map[string]any{{"ticker": "A", "side": "yes"}, {"ticker": "B", "side": "no"}}
	legsB := []map[string]any{{"ticker": "C", "side": "yes"}, {"ticker": "D", "side": "yes"}}
	now := time.Now()
	if !s.claimLiveComboAttempt("COL", legsA, now) {
		t.Fatal("first pair must claim")
	}
	if s.claimLiveComboAttempt("COL", legsA, now.Add(time.Second)) {
		t.Fatal("immediate duplicate RFQ attempt must cool down")
	}
	if !s.claimLiveComboAttempt("COL", legsB, now.Add(time.Second)) {
		t.Fatal("a distinct pair must remain eligible instead of being starved")
	}
	if !s.claimLiveComboAttempt("COL", legsA, now.Add(liveMirrorComboTTL)) {
		t.Fatal("attempt cooldown must expire")
	}
}

func TestR132KalshiExactFeeEventPrecedenceAndUnsupportedTypes(t *testing.T) {
	s := testServer(t)
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{
		"KXTEST": {taker: .07, typ: "quadratic", multiplier: 1},
		"KXPERP": {typ: "margin_market_maker_program_fees", multiplier: 0},
	}
	s.kalEventFees = map[string]kalFeeInfo{
		"KXTEST-EVENT": {taker: .035, maker: .00875, typ: "quadratic_with_maker_fees", multiplier: .5},
	}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()

	fee, known, _ := s.kalFeeExact("KXTEST-EVENT-MARKET", false, 2.5, .40)
	want := ev.FeeBreakdownCoeff(.035, 2.5, .40, true).Net
	if !known || fee != want || fee <= 0 {
		t.Fatalf("event override fee=(%.6f,%v), want known positive", fee, known)
	}
	if _, known, _ := s.kalFeeExact("KXPERP-X", false, 1, .50); known {
		t.Fatal("margin fee type with multiplier zero must not be misread as fee-free")
	}

	// A scheduled change that is effective now invalidates the pre-change map until the
	// authoritative refresh succeeds; live money must never race on yesterday's fee.
	s.kalFeeMu.Lock()
	s.kalFeesNext = time.Now().Add(-time.Second)
	s.kalFeesRetryAt = time.Now().Add(time.Hour) // keep the test deterministic: refresh is backed off
	s.kalFeeMu.Unlock()
	if _, known, why := s.kalFeeExact("KXTEST-EVENT-MARKET", false, 1, .40); known || !strings.Contains(why, "activation is due") {
		t.Fatalf("due fee activation must fail closed, known=%v why=%q", known, why)
	}
	s.kalFeeMu.Lock()
	s.kalFeesNext = time.Now().Add(time.Hour)
	s.kalFeesAt = time.Now().Add(-7 * time.Hour)
	s.kalFeeMu.Unlock()
	if _, known, why := s.kalFeeExact("KXTEST-EVENT-MARKET", false, 1, .40); known || !strings.Contains(why, "registry is stale") {
		t.Fatalf("stale exact-fee registry must fail closed, known=%v why=%q", known, why)
	}
}
