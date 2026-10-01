package server

// R105 pins — the ML-tab root fix (evLive scope) + per-section isolation; live sizing with the
// $/bet knob REMOVED (engine-only, no flat fallback); fail-closed risk-cap zeros (auditor 270);
// the option-(c) maker-posting policy; the RawFlow /api/ml payload; A4 live-peak preservation.

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

// THE R105 ROOT-CAUSE PIN (operator screenshot 06:24 "Could not load ML data."): renderMLBody —
// a top-level function — called evLive(), which existed only as a loadML() LOCAL. JS lexical
// scoping made that a ReferenceError on the FIRST +EV row of the Top-scored table, and loadML's
// tab-wide catch blanked the whole tab. Zero-pick boards never executed the row loop, which is
// why quiet overnight boards masked it and the first busy morning tripped it on every repaint.
// Pins: ONE top-level evLive, no local shadow, per-section isolation with named error lines, a
// bounded retry in the fetch catch, and the RawFlow panel/widget wiring.
func TestR105MLTabEvLiveScopeAndSectionIsolation(t *testing.T) {
	h := dashboardHTML
	if !strings.Contains(h, "function evLive(x)") {
		t.Fatalf("top-level evLive definition missing from the dashboard JS — the R105 root fix regressed")
	}
	if strings.Contains(h, "var evLive=function") {
		t.Fatalf("a LOCAL evLive shadow is back in the dashboard JS — this is the exact scope bug that blanked the ML tab")
	}
	if n := strings.Count(h, "mlSecErr("); n < 6 {
		t.Fatalf("mlSecErr appears %d times, want >=6 (definition + 5 isolated sections) — section isolation regressed", n)
	}
	for _, sec := range []string{"'ML paper book'", "'Shadow book'", "'Kflow twin books'", "'RawFlow book'", "'Top scored open markets'"} {
		if !strings.Contains(h, "mlSecErr("+sec) && !strings.Contains(h, ","+sec[1:]) && !strings.Contains(h, sec) {
			t.Fatalf("named section %s missing from the ML tab isolation wiring", sec)
		}
	}
	if !strings.Contains(h, "_mlRetryT") || !strings.Contains(h, "auto-retry in 5s") {
		t.Fatalf("loadML bounded-retry wiring missing — a transient fetch failure would park the tab on an error again")
	}
	// RawFlow visibility (operator): panel reads d.rawflow; widget registered + rendered.
	for _, want := range []string{"d.rawflow", "rawflow-book", "renderRawflowBook", "rfEqChart"} {
		if !strings.Contains(h, want) {
			t.Fatalf("RawFlow ML-tab wiring missing %q", want)
		}
	}
	// R105 (operator): the live $/bet chip is GONE — the exposure-cap chip remains.
	if strings.Contains(h, "echip('live_order_usd'") {
		t.Fatalf("the live $/bet chip (live_order_usd) is back in the dashboard — it was removed on operator order (live sizes like paper)")
	}
	if strings.Contains(h, "echip('live_exposure_cap_usd'") || !strings.Contains(h, "proof-Kelly · bankroll-scaled cap") {
		t.Fatalf("Adaptive Allocation Model must show the bankroll-scaled cap without resurrecting an editable fixed-dollar choke")
	}
}

// R105/R133: missing fixed rails never fail open. Percentage rails additionally require the
// operator's ARM-bankroll receipt; -1 disables only the legacy dollar cap, not all risk controls.
func TestR105LiveRiskCheckFailClosedZeros(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) { c.Risk.LiveMaxOrderUSD = 0; c.Risk.LiveMaxDailyLossUSD = 10 })
	if msg := s.liveRiskCheck(0.50, "kalshi"); !strings.Contains(msg, "fail-closed") {
		t.Fatalf("maxOrd=0 must fail closed, got %q", msg)
	}
	s.liveBankArm = map[string]float64{"kalshi": 100, "polyus": 100}
	s.mutateCfg(func(c *config.Config) { c.Risk.LiveMaxOrderUSD = 2; c.Risk.LiveMaxDailyLossUSD = 0 })
	if msg := s.liveRiskCheck(0.50, "kalshi"); msg != "" {
		t.Fatalf("the legacy accepted-turnover value must not act as a second loss gate, got %q", msg)
	}
	s.mutateCfg(func(c *config.Config) { c.Risk.LiveMaxOrderUSD = -1; c.Risk.LiveMaxDailyLossUSD = -1 })
	if msg := s.liveRiskCheck(0.50, "kalshi"); msg != "" {
		t.Fatalf("percentage rails with an ARM receipt should pass, got refusal %q", msg)
	}
	s.mutateCfg(func(c *config.Config) { c.Risk.LiveMaxOrderUSD = 2; c.Risk.LiveMaxDailyLossUSD = 10 })
	if msg := s.liveRiskCheck(0.50, "kalshi"); msg != "" {
		t.Fatalf("normal caps small order must pass, got %q", msg)
	}
}

// R105 (operator): live sizing has NO flat fallback — an unknown/zero bankroll refuses (usd=0)
// and proposal callers drop the candidate. Config defaults keep both caps fail-tight.
func TestR105EngineRefusesUnknownBank(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	usd, src := s.liveEngineOrderUSDWith(ctx, "kalshi", "KXR105-X", "yes", "auto-ml", 0.50, 0, "unknown")
	if usd != 0 || src != "unknown" {
		t.Fatalf("bank 0 must refuse to size: got $%v (%s), want $0 (unknown)", usd, src)
	}
	def := config.Default()
	if def.Risk.LiveMaxOrderPct <= 0 || def.Risk.LiveExposureCapPct <= 0 || def.Risk.LiveCryptoCapPct <= 0 ||
		def.Risk.LiveDailyLossPct <= 0 || def.Risk.LiveClusterCapPct <= 0 ||
		def.Risk.LiveKellyMaxFrac <= 0 || def.Risk.LiveKellyMaxFrac > 0.50 {
		t.Fatalf("defaults must keep proportional live rails armed: %+v", def.Risk)
	}
}

// R105 MAKER-POSTING POLICY (option c): posts require visible depth above maker_min_depth; thin
// or invisible books are unsafe (divert to taker). Momentum needs live ring data (not seedable
// hermetically) — its logic is unchanged from the R101-proven producer, so depth is what we pin.
func TestR105MakerPolicyDepthGate(t *testing.T) {
	s := testServer(t)
	s.kobImbMu.Lock()
	s.kobImb = map[string]kobImbEntry{
		"KXR105-THIN": {depth: 5, at: time.Now()},
		"KXR105-DEEP": {depth: 50, at: time.Now()},
	}
	s.kobImbMu.Unlock()
	if reason, dep, src, _, _ := s.makerPostUnsafe("kalshi", "KXR105-NOBOOK", "YES"); reason != "depth0" || dep != 0 || src != "none" {
		t.Fatalf("invisible book must be depth0/none: got %q dep=%v src=%q", reason, dep, src)
	}
	if reason, dep, src, _, _ := s.makerPostUnsafe("kalshi", "KXR105-THIN", "YES"); reason != "depthfloor" || dep != 5 || src != "kob" {
		t.Fatalf("thin book (5 < default floor 10) must be depthfloor: got %q dep=%v src=%q", reason, dep, src)
	}
	if reason, _, _, _, _ := s.makerPostUnsafe("kalshi", "KXR105-DEEP", "YES"); reason != "" {
		t.Fatalf("deep book (50 >= floor) with no momentum data must be SAFE: got %q", reason)
	}
	// The floor is a config tunable: raise it above the deep book and the post becomes unsafe.
	s.mutateCfg(func(c *config.Config) { c.Auto.MakerMinDepth = 100 })
	if reason, _, _, _, _ := s.makerPostUnsafe("kalshi", "KXR105-DEEP", "YES"); reason != "depthfloor" {
		t.Fatalf("floor raised to 100 must gate the 50-deep book: got %q", reason)
	}
}

// R105 (auditor A4): a paper reset keeps ONLY the live|<venue> drawdown-breaker peaks.
func TestR105PreserveLivePeaks(t *testing.T) {
	in := map[string]float64{"kalshi": 500, "polyus": 250, "live|kalshi": 25.5, "live|polyus": 36.87}
	out := preserveLivePeaks(in)
	if len(out) != 2 || out["live|kalshi"] != 25.5 || out["live|polyus"] != 36.87 {
		t.Fatalf("live peaks must survive a paper reset: got %v", out)
	}
	if _, hasPaper := out["kalshi"]; hasPaper {
		t.Fatalf("paper peaks must clear on reset: got %v", out)
	}
}

// R105 (operator): the RawFlow /api/ml payload — kflowBooksPayload-shaped, with open lots and
// the band/rules the panel + widget render.
func TestR105RawFlowMLPayload(t *testing.T) {
	s := testServer(t)
	// Disabled (legacy flat bankrolls) ⇒ {enabled:false}.
	if p := s.rawFlowMLPayload(); p["enabled"] != false {
		t.Fatalf("legacy-flat suite must report rawflow disabled, got %v", p)
	}
	s.mutateCfg(func(c *config.Config) {
		c.Auto.PaperTotalStart = 1000
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML, c.Auto.AllocRawFlow = 0.35, 0.10, 0.35, 0.20
	})
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	b.Open = append(b.Open, kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: "KXR105-RF",
		Title: "rf lot", Side: "YES", Price: 0.30, Contracts: 10, Fee: 0.05})
	bank := b.Bank
	s.rfBookMu.Unlock()
	p := s.rawFlowMLPayload()
	if p["enabled"] != true {
		t.Fatalf("alloc_rawflow>0 must enable the payload, got %v", p["enabled"])
	}
	if p["open"] != 1 {
		t.Fatalf("open lot count = %v, want 1", p["open"])
	}
	lots, ok := p["open_lots"].([]map[string]any)
	if !ok || len(lots) != 1 || lots[0]["ticker"] != "KXR105-RF" {
		t.Fatalf("open_lots malformed: %v", p["open_lots"])
	}
	if got := p["open_cost"].(float64); math.Abs(got-3.05) > 0.001 {
		t.Fatalf("open_cost = %v, want 3.05 (10×0.30 + 0.05 fee)", got)
	}
	if got := p["equity"].(float64); math.Abs(got-bank) > 0.001 {
		t.Fatalf("equity = %v, want bank %v (net 0)", got, bank)
	}
	if p["band"] != "10–50¢" {
		t.Fatalf("band = %v", p["band"])
	}
	if !strings.Contains(p["rules"].(string), "no ML") {
		t.Fatalf("rules one-liner must state the no-ML design: %v", p["rules"])
	}
}

// R105 (auditor A8): the cross-book hedge guard is symmetric now — an auto-* source must refuse
// the OPPOSITE side of an open RawFlow lot (auto-arb stays exempt).
func TestR105RawFlowHedgeSymmetry(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Auto.PaperTotalStart = 1000
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML, c.Auto.AllocRawFlow = 0.35, 0.10, 0.35, 0.20
	})
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	b.Open = append(b.Open, kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: "KXR105-HEDGE",
		Title: "rf lot", Side: "YES", Price: 0.30, Contracts: 10})
	s.rfBookMu.Unlock()
	if r := s.betConflictReason("kalshi", "KXR105-HEDGE", "NO", "auto-cons-kalshi", nil); r != "rawflow-hedge" {
		t.Fatalf("auto source opposing an open RawFlow lot must refuse (rawflow-hedge), got %q", r)
	}
	if r := s.betConflictReason("kalshi", "KXR105-HEDGE", "YES", "auto-cons-kalshi", nil); r != "" {
		t.Fatalf("SAME side as the RawFlow lot is not a hedge, got %q", r)
	}
	if r := s.betConflictReason("kalshi", "KXR105-HEDGE", "NO", "auto-arb", nil); r != "" {
		t.Fatalf("auto-arb is exempt from the hedge guard by design, got %q", r)
	}
}
