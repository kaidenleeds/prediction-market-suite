package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR133Mode4ProofLowerSizesFractionalKellyAndCompounds(t *testing.T) {
	s := testServer(t)
	s.liveBankArm = map[string]float64{"kalshi": 1000}
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveKellyMaxFrac = 0.50
		c.Auto.MinEVPerContract = 0.03
	})
	// mean=6¢, lower=3¢ at a 3¢ admission floor: stability=.5, strength=.5, so the
	// live-only half-Kelly ceiling self-tunes to .50×.875×.875=.3828125.
	base, frac := s.liveProofOrderUSD("kalshi", 1000, 0.40, 0.06, 0.03)
	if math.Abs(frac-0.3828125) > 1e-12 || math.Abs(base-19.140625) > 1e-9 {
		t.Fatalf("adaptive proof Kelly stake=%v frac=%v want 19.140625 / 0.3828125", base, frac)
	}
	s.liveBankArm = map[string]float64{"kalshi": 500, "polyus": 500}
	if split, _ := s.liveProofOrderUSD("kalshi", 500, 0.40, 0.06, 0.03); math.Abs(split-9.5703125) > 1e-9 {
		t.Fatalf("$1k split-book example stake=%v want 9.5703125", split)
	}
	s.liveBankArm = map[string]float64{"kalshi": 1000}
	won, _ := s.liveProofOrderUSD("kalshi", 1100, 0.40, 0.06, 0.03)
	lost, _ := s.liveProofOrderUSD("kalshi", 800, 0.40, 0.06, 0.03)
	if won <= base || lost >= base {
		t.Fatalf("sizing must compound/de-risk with venue NAV: won=%v base=%v lost=%v", won, base, lost)
	}
	// Strong/stable executable proof reaches half-Kelly, but the 5%-NAV order rail still wins.
	if got := adaptiveProofKellyFrac(0.50, 0.06, 0.06, 0.03, 1); math.Abs(got-0.50) > 1e-12 {
		t.Fatalf("strong stable proof fraction=%v want ceiling .50", got)
	}
	s.liveBankArm = map[string]float64{"kalshi": 500}
	if got, _ := s.liveProofOrderUSD("kalshi", 500, 0.40, 0.06, 0.06); math.Abs(got-25) > 1e-9 {
		t.Fatalf("strong proof stake=%v want 5%% order cap $25", got)
	}
	// Never round a sub-Kelly stake upward just to manufacture a minimum lot.
	if got, _ := s.liveProofOrderUSD("kalshi", 10, 0.80, 0.06, 0.03); got != 0 {
		t.Fatalf("sub-contract proof stake must skip, got %v", got)
	}
}

func TestR133Mode4AdaptiveKellyShrinksForUncertaintyAndDrawdown(t *testing.T) {
	stable := adaptiveProofKellyFrac(0.50, 0.06, 0.03, 0.03, 1)
	uncertain := adaptiveProofKellyFrac(0.50, 0.12, 0.03, 0.03, 1)
	drawn := adaptiveProofKellyFrac(0.50, 0.06, 0.03, 0.03, 0.94)
	strong := adaptiveProofKellyFrac(0.50, 0.06, 0.06, 0.03, 1)
	if !(strong > stable && stable > uncertain && stable > drawn && drawn > 0) {
		t.Fatalf("adaptive ordering wrong: strong=%v stable=%v uncertain=%v drawdown=%v", strong, stable, uncertain, drawn)
	}
	for _, bad := range []float64{
		adaptiveProofKellyFrac(0.50, 0.06, 0.029, 0.03, 1), // below required live edge
		adaptiveProofKellyFrac(0.50, 0.02, 0.03, 0.03, 1),  // impossible lower > mean
		adaptiveProofKellyFrac(0, 0.06, 0.03, 0.03, 1),     // no configured ceiling
		adaptiveProofKellyFrac(0.50, 0.06, 0.03, 0.03, 0),  // no valid bankroll/drawdown state
	} {
		if bad != 0 {
			t.Fatalf("invalid proof state must fail closed, got %v", bad)
		}
	}
	if got := adaptiveProofKellyFrac(0.90, 0.06, 0.06, 0.03, 1); got != 0.50 {
		t.Fatalf("live Kelly ceiling must clamp to half-Kelly, got %v", got)
	}
	for ratio, want := range map[float64]float64{1.10: 1, 1.00: 1, 0.97: 0.75, 0.95: 0.50, 0.92: 0} {
		if got := livePnLRiskMultiplier(ratio); math.Abs(got-want) > 1e-9 {
			t.Fatalf("P&L throttle at equity ratio %.2f = %.6f, want %.6f", ratio, got, want)
		}
	}
}

func TestR133Mode4PortfolioRailsScaleFromArmBankroll(t *testing.T) {
	s := testServer(t)
	s.liveBankArm = map[string]float64{"kalshi": 500, "polyus": 500}
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveExposureCapPct = 0.25
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveMaxDailyLossUSD = -1
		c.Risk.LiveDailyLossPct = 0.05
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemPolyUS = true
	})
	if got := s.liveCap(); got != 250 {
		t.Fatalf("combined exposure cap=%v want $250 (25%% of $1k), not legacy $10", got)
	}
	if got := s.liveVenueCap("kalshi"); got != 125 {
		t.Fatalf("Kalshi segregated cap=%v want $125", got)
	}
	if got, ok := liveRailLimit(s.liveArmBankroll("kalshi"), s.cfg().Risk.LiveMaxOrderPct, s.cfg().Risk.LiveMaxOrderUSD); !ok || got != 25 {
		t.Fatalf("venue order rail=%v ok=%v want $25", got, ok)
	}
	if msg := s.liveRiskCheck(25, "kalshi"); msg != "" {
		t.Fatalf("5%% boundary should pass: %s", msg)
	}
	if msg := s.liveRiskCheck(25.01, "kalshi"); msg == "" {
		t.Fatal("order above 5% venue rail must fail")
	}
	rail := s.liveRailsView()
	for key, want := range map[string]float64{
		"order_kalshi": 25, "order_polyus": 25, "exposure_total": 250,
		"cluster_kalshi": 15, "cluster_polyus": 15, "daily_loss": 50,
		"kelly_max_frac": 0.25,
	} {
		if got, _ := rail[key].(float64); got != want {
			t.Fatalf("rails[%s]=%v want %v (view must match money-path formula)", key, got, want)
		}
	}
}

func TestR144LiveExposureCompoundsIntradayByVenue(t *testing.T) {
	s := testServer(t)
	s.liveBankArm = map[string]float64{"kalshi": 500, "polyus": 500}
	s.liveLossDay = time.Now().UTC().Format("2006-01-02")
	s.liveLossHasBase = true
	s.liveLossDeltaV = map[string]int64{"kalshi": liveLossUSDToUnits(100), "polyus": liveLossUSDToUnits(-50)}
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveExposureCapPct = 0.5 // operator's starting hard portfolio wall; Allocation may use less
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveMaxDailyLossUSD = -1
		c.Risk.LiveDailyLossPct = 0.10
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemPolyUS = true
	})
	if got := s.liveRiskBankroll("kalshi"); math.Abs(got-600) > 1e-9 {
		t.Fatalf("Kalshi intraday profit did not expand only its risk equity: %v", got)
	}
	if got := s.liveRiskBankroll("polyus"); math.Abs(got-450) > 1e-9 {
		t.Fatalf("PolyUS intraday loss did not contract only its risk equity: %v", got)
	}
	if got := s.liveCap(); math.Abs(got-525) > 1e-9 {
		t.Fatalf("combined dynamic exposure cap=%v want $525", got)
	}
	if got := s.liveVenueCap("kalshi"); math.Abs(got-300) > 1e-9 {
		t.Fatalf("Kalshi dynamic venue cap=%v want $300", got)
	}
	if got := s.liveVenueCap("polyus"); math.Abs(got-225) > 1e-9 {
		t.Fatalf("PolyUS dynamic venue cap=%v want $225", got)
	}
	if msg := s.liveRiskCheck(30, "kalshi"); msg != "" { // 5% of current $600
		t.Fatalf("dynamic Kalshi order boundary should pass: %s", msg)
	}
	if msg := s.liveRiskCheck(30.01, "kalshi"); msg == "" {
		t.Fatal("order above 5% of current intraday Kalshi equity must fail")
	}
	if got := s.liveRailsView()["daily_loss"].(float64); math.Abs(got-100) > 1e-9 {
		t.Fatalf("10%% ARM-baseline hard-loss rail view=%v want $100", got)
	}
}

func TestR133Mode4ClusterKeysCollapseOneGameNotWholeVenue(t *testing.T) {
	s := &Server{}
	a := s.liveMirrorClusterKey("kalshi", "KXMLBTOTAL-26JUL011507DETNYY-11", "")
	b := s.liveMirrorClusterKey("kalshi", "KXMLBSPREAD-26JUL011507DETNYY-2", "")
	if a != b || a == "" {
		t.Fatalf("same Kalshi game must share a cluster: %q vs %q", a, b)
	}
	p1 := s.liveMirrorClusterKey("polyus", "unknown-market-one", "")
	p2 := s.liveMirrorClusterKey("polyus", "unknown-market-two", "")
	if p1 == p2 || p1 == "polyus:" || p2 == "polyus:" {
		t.Fatalf("unknown PolyUS markets must not collapse the whole venue: %q %q", p1, p2)
	}
}

func TestR133Mode4ClusterGuardCountsOnlyExistingRiskAndDoesNotClaim(t *testing.T) {
	s := testServer(t)
	s.liveBankArm = map[string]float64{"polyus": 100}
	s.mutateCfg(func(c *config.Config) { c.Risk.LiveClusterCapPct = 0.10 })
	s.pusLiveAt = time.Now()
	s.pusPosC = []polymarketus.PUSPosition{{
		Slug: "aec-mlb-det-nyy-20260710-total", Cost: 9, Net: 10,
	}}
	c := liveMirrorCandidate{
		Platform: "polyus", Ticker: "aec-mlb-det-nyy-20260710-moneyline", Side: "YES",
		Source: "xvgap", Price: 0.50, At: time.Now(),
	}
	if why := s.liveMirrorClusterGuard(context.Background(), c, 1); why != "" {
		t.Fatalf("exact 10%% boundary should pass without double-counting proposed cost: %s", why)
	}
	if why := s.liveMirrorClusterGuard(context.Background(), c, 1.01); why == "" {
		t.Fatal("same-game exposure above the 10% venue rail must fail")
	}
	if len(s.liveMirrorClaim) != 0 {
		t.Fatalf("cluster guard is read-only and must not claim intent: %#v", s.liveMirrorClaim)
	}
}

func TestR133Mode4InsufficientPathsResetToSafeRide(t *testing.T) {
	s := testServer(t)
	s.autoMu.Lock()
	s.autoSLTPMode, s.autoSLTPRatio = "auto", 0.5
	s.autoMu.Unlock()
	s.mutateCfg(func(c *config.Config) { c.Auto.SLTPAuto = true; c.Auto.SLTPRatio = 0.5 })
	s.sltpTuneAt = time.Time{}
	s.autoTuneSLTP(context.Background())
	s.autoMu.Lock()
	got := s.autoSLTPRatio
	s.autoMu.Unlock()
	if got != 0 || s.cfg().Auto.SLTPRatio != 0 {
		t.Fatalf("insufficient eligible evidence must retain safe r=0, runtime=%v cfg=%v", got, s.cfg().Auto.SLTPRatio)
	}
}
