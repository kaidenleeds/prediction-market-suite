package server

// r118_invert_test.go — R118 invert-path correctness audit, five regression pins:
//  (a) side flip correct for every market shape the chain sees (plain YES/NO, above/below
//      ladder strikes, R106 two-sided winner/advance short-leg expression);
//  (b) the invert twin charges REAL costs both ways: 2×taker fee (0.07·p·(1−p) — symmetric in
//      p↔1−p, so the inverted side's fee equals the original's) PLUS a spread haircut (family
//      mean half-spread from signal_log; documented 2¢ default when unrecorded) — the R116
//      "spread-free upper bound" is retired;
//  (c) twin grading applies the settle flip and the price flip each EXACTLY once (no double
//      negation), including the settle_val=0.5 half-settle;
//  (d) promotion of a twin creates a book placing the INVERTED side end-to-end (promoDecide →
//      freshinv executor side selection, NO at 1−px);
//  (e) the deep-loser spawn threshold under the corrected cost model is loss > 2×fee + haircut,
//      and the live invert:pcrypto / invert:freshlist edges still clear it.

import (
	"math"
	"testing"
	"time"
)

// ---- (a) side flip per market shape --------------------------------------------------------

func TestR118SideFlipPerShape(t *testing.T) {
	// Every shape in the invert chain is a single-slug binary instrument quoted as a YES price:
	//  - plain YES/NO markets: fade = NO at 1−yes;
	//  - above/below ladder strikes (Kalshi KX*-style): each strike is its own binary — the
	//    "above" leg is that strike's YES, so the invert is NO at 1−yes, per strike;
	//  - two-sided winner/advance instruments (R106 bug-255): the short leg is only ever
	//    expressed as side NO at 1−yes (never a synthetic YES) — inverting a row that already
	//    sits on the short leg (side NO) lands back on YES at yes, exactly once.
	cases := []struct {
		name      string
		origSide  string // the losing family's logged side
		yesMark   float64
		wantSide  string
		wantEntry float64
	}{
		{"plain YES/NO", "YES", 0.70, "NO", 0.30},
		{"ladder above-strike leg (binary per strike)", "YES", 0.12, "NO", 0.88},
		{"ladder below-strike leg", "NO", 0.12, "YES", 0.12},
		{"two-sided advance, long leg", "Up", 0.64, "NO", 0.36},
		{"two-sided advance, short leg (R106: expressed as NO)", "Down", 0.64, "YES", 0.64},
	}
	for _, c := range cases {
		side, entry, ok := xinvDerive(c.origSide, c.yesMark)
		if !ok || side != c.wantSide || !almostEq(entry, c.wantEntry) {
			t.Errorf("%s: xinvDerive(%q, %v) = (%q, %v, %v), want (%q, %v)",
				c.name, c.origSide, c.yesMark, side, entry, ok, c.wantSide, c.wantEntry)
		}
	}
}

// ---- (b) real costs both ways ---------------------------------------------------------------

func TestR118InvertDragChargesFeesAndSpread(t *testing.T) {
	// Taker fee symmetry: the inverted side's fee at 1−p equals the original's at p, so 2×feePC
	// really is fee-both-ways with no inverted-price recompute needed.
	fee := func(p float64) float64 { return 0.07 * p * (1 - p) }
	for _, p := range []float64{0.05, 0.30, 0.60, 0.95} {
		if !almostEq(fee(p), fee(1-p)) {
			t.Fatalf("taker fee not symmetric at p=%v", p)
		}
	}
	// invertDrag = 2×fee + spread haircut; haircut ≤ 0 falls to the documented 2¢ default.
	if d := invertDrag(0.01, 0.03); !almostEq(d, 0.05) {
		t.Fatalf("invertDrag(0.01, 0.03) = %v, want 0.05", d)
	}
	if d := invertDrag(0.01, 0); !almostEq(d, 0.02+invDefaultSpreadHaircutPC) {
		t.Fatalf("invertDrag default haircut = %v, want %v", d, 0.02+invDefaultSpreadHaircutPC)
	}
	// The twin's mean is the honest net edge: verdictFrom must subtract the FULL drag.
	v := verdictFrom("loser", "signal", "$/contract", 5000, -0.10, 0.05, 0.01, 0.03)
	if v.State != "PROVEN-" {
		t.Fatalf("deep loser must be PROVEN-, got %s", v.State)
	}
	if !almostEq(v.Invert, 0.10-0.05) { // −mean − (2·fee + haircut)
		t.Fatalf("invert edge = %v, want 0.05 (loss minus 2×fee minus spread haircut)", v.Invert)
	}
}

// ---- (c) grading: settle flip + price flip each exactly once ---------------------------------

func TestR118TwinGradingNoDoubleNegation(t *testing.T) {
	const p, spread, hc = 0.60, 0.04, 0.02 // original YES entry 60¢; 4¢ book → 2¢ half-spread haircut
	fee := 0.07 * p * (1 - p)              // 0.0168; identical on the inverted price (symmetry)
	twinRow := func(settle float64) float64 {
		origPC := settle - p - fee            // the verdict engine's per-row pc for the original YES row
		return -origPC - 2*fee - hc           // the twin ledger's per-row value (sign flip + full drag)
	}
	direct := func(settle float64) float64 {
		// The honest inverted trade computed from scratch: buy NO at the other side's ask
		// (mid + half-spread), pay the inverted side's own taker fee, settle at 1−settle.
		entryNO := (1 - p) + hc
		feeInv := 0.07 * (1 - p) * p
		return (1 - settle) - entryNO - feeInv
	}
	for _, settle := range []float64{0, 1, 0.5} { // loss, win, half-settle
		if !almostEq(twinRow(settle), direct(settle)) {
			t.Fatalf("settle=%v: twin ledger %v != direct inverted computation %v (double negation?)",
				settle, twinRow(settle), direct(settle))
		}
	}
	// Hand numbers: original loses (YES@0.60 settles 0) → twin WINS (1−0.60) − 2¢ spread − fee
	// on 40¢ = 0.60 − 0.02 − 0.0168 net of the original's fee refund in the ledger identity:
	if !almostEq(twinRow(0), 0.60+fee-2*fee-hc) || twinRow(0) < 0.56 {
		t.Fatalf("twin win on original loss = %v, want 0.5632", twinRow(0))
	}
	if !almostEq(twinRow(0.5), 0.0632) { // half-settle: twin nets +6.32¢
		t.Fatalf("half-settle twin row = %v, want 0.0632", twinRow(0.5))
	}

	// Executor-book grading (fiSettlePnL): the side flip lives ONLY in the payout; entry already
	// carried the price flip. NO bought at 0.40 (1−0.60), market settles NO (yv=0): payout 1.
	if payout, pnl, won := fiSettlePnL("NO", 0, 0.40, 1, 0.0168); payout != 1 || !won || !almostEq(pnl, 0.5832) {
		t.Fatalf("NO settle yv=0: payout=%v pnl=%v won=%v, want 1 / 0.5832 / true", payout, pnl, won)
	}
	if payout, pnl, won := fiSettlePnL("NO", 1, 0.40, 1, 0.0168); payout != 0 || won || !almostEq(pnl, -0.4168) {
		t.Fatalf("NO settle yv=1: payout=%v pnl=%v won=%v, want 0 / -0.4168 / false", payout, pnl, won)
	}
	if payout, pnl, won := fiSettlePnL("NO", 0.5, 0.40, 1, 0.0168); payout != 0.5 || !won || !almostEq(pnl, 0.0832) {
		t.Fatalf("NO half-settle: payout=%v pnl=%v won=%v, want 0.5 / 0.0832 / true", payout, pnl, won)
	}
	if payout, pnl, won := fiSettlePnL("YES", 1, 0.60, 1, 0.0168); payout != 1 || !won || !almostEq(pnl, 0.3832) {
		t.Fatalf("YES side must not flip: payout=%v pnl=%v won=%v", payout, pnl, won)
	}
}

// ---- (d) twin promotion places the inverted side end-to-end ---------------------------------

func TestR118TwinPromotionPlacesInvertedSide(t *testing.T) {
	// A PROVEN+ invert:freshlist snapshot promotes under the R118 significance-only gate…
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"invert:freshlist": provenPlus("invert:freshlist", 483)},
		Records:  map[string]promoRecord{}, Reserve: 500}
	p := findTrans(promoDecide(sn), "invert:freshlist", promoStatePromoted)
	if p == nil {
		t.Fatal("PROVEN+ invert:freshlist must promote")
	}
	if ex := promoTestExecs()["invert:freshlist"]; ex.Name != "freshinv" || ex.BookFile != "freshinv_book.json" {
		t.Fatalf("twin must map to the freshinv executor, got %+v", ex)
	}
	// …and the freshinv executor's side selection places the INVERTED side: NO at 1−yesPx.
	if side, price, ok := fiEntrySide(0.70, 5); !ok || side != "NO" || !almostEq(price, 0.30) {
		t.Fatalf("fiEntrySide(0.70) = (%q, %v, %v), want NO @ 0.30", side, price, ok)
	}
	// Ladder-strike shaped listing (deep YES): still NO at 1−yes, entry band enforced.
	if side, price, ok := fiEntrySide(0.10, 5); !ok || side != "NO" || !almostEq(price, 0.90) {
		t.Fatalf("fiEntrySide(0.10) = (%q, %v, %v), want NO @ 0.90", side, price, ok)
	}
	// Screens: detection band, fade-side entry band, spread cap.
	for _, c := range []struct{ yes, sc float64 }{{0.02, 5}, {0.98, 5}, {0.96, 5}, {0.04, 5}, {0.50, 11}} {
		if _, _, ok := fiEntrySide(c.yes, c.sc); ok {
			t.Errorf("fiEntrySide(%v, spread %v) must refuse", c.yes, c.sc)
		}
	}
}

// ---- (e) spawn threshold under the corrected cost model + live edges survive ------------------

func TestR118SpawnThresholdIsFeePlusSpread(t *testing.T) {
	// drag = 2×0.01 + 0.03 = 0.05: a loss of 4.9¢ must NOT spawn a twin; 5.1¢ must.
	v := verdictFrom("shallow", "signal", "$/contract", 10000, -0.049, 0.05, 0.01, 0.03)
	if v.State != "PROVEN-" || v.Invert != 0 {
		t.Fatalf("loss inside the drag must not spawn: state=%s invert=%v", v.State, v.Invert)
	}
	v = verdictFrom("deep", "signal", "$/contract", 10000, -0.051, 0.05, 0.01, 0.03)
	if v.State != "PROVEN-" || !almostEq(v.Invert, 0.001) {
		t.Fatalf("loss beyond the drag must spawn: state=%s invert=%v want 0.001", v.State, v.Invert)
	}
}

func TestR118LiveTwinEdgesSurviveHonestCosts(t *testing.T) {
	// Live 2026-07-08 numbers (data/verdicts.json + signal_log spread audit):
	// freshlist: n=484, mean −0.1125, sd 0.3485, fee 0.0088; family mean per-row haircut 0.0245
	// (18/484 rows carry a recorded spread, mean 28¢ → half; the rest charge the 2¢ default).
	v := verdictFrom("freshlist", "signal", "$/contract", 484, -0.1125, 0.3485, 0.0088, 0.0245)
	if v.State != "PROVEN-" || !almostEq(v.Invert, 0.1125-2*0.0088-0.0245) { // 0.0704
		t.Fatalf("freshlist: state=%s invert=%v, want PROVEN- with 0.0704", v.State, v.Invert)
	}
	inv := verdictFrom("invert:freshlist", "invert", "$/contract", 484, v.Invert, 0.3485, 0.0088, 0.0245)
	if inv.State != "PROVEN+" || inv.Lo <= 0 {
		t.Fatalf("invert:freshlist at the honest +7.0¢ must STILL be PROVEN+ (lo=%v state=%s)", inv.Lo, inv.State)
	}
	// pcrypto: n=5222, mean −0.2195, sd 0.5097, fee 0.0127; no recorded spreads → 2¢ default.
	v = verdictFrom("pcrypto", "signal", "$/contract", 5222, -0.2195, 0.5097, 0.0127, 0)
	if !almostEq(v.Invert, 0.1741) {
		t.Fatalf("pcrypto honest invert edge = %v, want 0.1741", v.Invert)
	}
	inv = verdictFrom("invert:pcrypto", "invert", "$/contract", 5222, v.Invert, 0.5097, 0.0127, 0.02)
	if inv.State != "PROVEN+" {
		t.Fatalf("invert:pcrypto at the honest +17.4¢ must still be PROVEN+ (lo=%v)", inv.Lo)
	}
	if math.Abs(inv.Lo-0.1465) > 0.002 {
		t.Fatalf("invert:pcrypto CS lower bound drifted: lo=%v want ≈0.1465", inv.Lo)
	}
}
