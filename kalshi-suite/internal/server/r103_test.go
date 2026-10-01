package server

// R103 pins — per-venue live bankrolls + the SHARED sizing engine + the RawFlow book rules.

import (
	"context"
	"math"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

// The R103 contract: live sizing runs the SAME engine as paper — same bankroll in, same stake out.
// (autoPlace passes allocMult=true; with no allocator table built allocStakeMult is 1.0, so the
// paper-shaped call and the live-shaped call must be equal to the cent. The Kelly-free base path
// is pinned numerically: pct base → <$1 → $1 floor → min-stake floor clamped to 25% of bank.)
func TestR103LiveSizingMatchesPaper(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// config.Default(): stake_pct 0.005, min_stake_usd 10, kelly_frac from defaults; no signal
	// stats exist in the fresh store, so betEdge finds no edge and the flat/pct base rules.
	s.autoMu.Lock()
	s.autoStake, s.autoKellyFrac, s.autoKellyEdge = 2, 0.25, "realized"
	s.autoMu.Unlock()
	for _, bank := range []float64{15.78, 100, 20, 350} {
		paperStake := s.engineStake(ctx, "kalshi", "KXR103-A", "yes", "auto-ml", 0.50, 0, bank, true, "")
		liveStake := s.engineStake(ctx, "kalshi", "KXR103-A", "yes", "auto-ml", 0.50, 0, bank, false, "")
		if math.Abs(paperStake-liveStake) > 1e-9 {
			t.Fatalf("bank $%.2f: paper stake %v != live stake %v — the engine forked", bank, paperStake, liveStake)
		}
	}
	// Numeric pin at bank=$100, price 50¢: base = 100×0.005 = $0.50 → $1 floor → min-stake $10
	// (lim 25%×100=$25 ≥ 10). The exact pre-R103 autoPlace arithmetic.
	if got := s.engineStake(ctx, "kalshi", "KXR103-A", "yes", "auto-ml", 0.50, 0, 100, true, ""); math.Abs(got-10) > 1e-9 {
		t.Fatalf("engine stake at bank=100 = %v, want 10 (min-stake floor)", got)
	}
	// Small bankroll: the min-stake floor clamps to 25% of bank (bank=20 → floor $5, never $10).
	if got := s.engineStake(ctx, "kalshi", "KXR103-A", "yes", "auto-ml", 0.50, 0, 20, true, ""); math.Abs(got-5) > 1e-9 {
		t.Fatalf("engine stake at bank=20 = %v, want 5 (floor clamped to 25%% of bank)", got)
	}
	// Explicit stakeIn keeps autoPlace's semantics (skips base/Kelly, still floored).
	if got := s.engineStake(ctx, "kalshi", "KXR103-A", "yes", "auto-ml", 0.50, 3, 100, true, ""); math.Abs(got-10) > 1e-9 {
		t.Fatalf("explicit $3 stake = %v, want 10 (min-stake floor still applies)", got)
	}
}

// liveEngineOrderUSD: engine result bounded by live_max_order_usd; R105 — an UNKNOWN bankroll
// REFUSES to size (usd=0, callers drop the candidate): the flat $/bet fallback died with the knob.
// R147: config bankroll values are ceilings only; an authenticated venue receipt is mandatory.
func TestR103LiveEngineOrderRails(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveMaxOrderUSD = 2
		c.Risk.KalshiLiveBankroll = 100
	})
	s.liveBankCashRead = func(_ context.Context, venue string) (float64, bool) {
		return 100, venue == "kalshi"
	}
	usd, src := s.liveEngineOrderUSD(ctx, "kalshi", "KXR103-B", "yes", "auto-ml", 0.50)
	if src != "venue" || math.Abs(usd-2) > 1e-9 {
		t.Fatalf("engine order = $%v (%s), want $2 (authenticated venue) — the per-order rail must win over the $10 engine stake", usd, src)
	}
	// R105: PolyUS with no pin and no private WS = UNKNOWN → usd 0 (refuse to size, no fallback).
	usd, src = s.liveEngineOrderUSD(ctx, "polyus", "slug-x", "yes", "auto-ml", 0.50)
	if src != "unknown" || usd != 0 {
		t.Fatalf("PUS unknown bankroll must REFUSE to size: got $%v (%s), want $0 (unknown)", usd, src)
	}
}

// Per-venue bankroll sources + caps: authenticated venue truth is mandatory and config pins can
// only tighten it; unset venue cap = the combined cap; the throttle halves sizing equity >20%
// under the session live peak (and never floors UP).
func TestR103VenueBankrollAndCaps(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) {
		c.Risk.KalshiLiveBankroll = 25
		c.Risk.PolyusLiveBankroll = 7
		c.Risk.LiveExposureCapUSD = 10
		c.Risk.LivePolyusCapUSD = 3
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemPolyUS = true
	})
	s.liveBankArm = map[string]float64{"kalshi": 25, "polyus": 7}
	cash := map[string]float64{"kalshi": 25, "polyus": 7}
	valid := map[string]bool{"kalshi": true, "polyus": true}
	s.liveBankCashRead = func(_ context.Context, venue string) (float64, bool) {
		return cash[venue], valid[venue]
	}
	if b, src := s.liveVenueBankroll(ctx, "kalshi"); src != "venue" || b != 25 {
		t.Fatalf("kalshi authenticated bankroll = %v (%s), want 25 (venue)", b, src)
	}
	if b, src := s.liveVenueBankroll(ctx, "polyus"); src != "venue" || b != 7 {
		t.Fatalf("polyus authenticated bankroll = %v (%s), want 7 (venue)", b, src)
	}
	if got := s.liveVenueCap("kalshi"); got != 5 {
		t.Fatalf("kalshi cap = %v, want min(20%% of its $25 ARM bankroll, $10 fixed cap)", got)
	}
	if got := s.liveVenueCap("polyus"); math.Abs(got-1.4) > 1e-9 {
		t.Fatalf("polyus cap = %v, want min(20%% of its $7 ARM bankroll, $3 venue cap)", got)
	}
	// Throttle: ratchet a $25 peak, then tighten the ceiling to $18 (28%% under) → halved to $9.
	if eq, _ := s.liveSizingBankroll(ctx, "kalshi"); eq != 25 {
		t.Fatalf("throttled eq at peak = %v, want 25", eq)
	}
	s.mutateCfg(func(c *config.Config) { c.Risk.KalshiLiveBankroll = 18 })
	if eq, _ := s.liveSizingBankroll(ctx, "kalshi"); math.Abs(eq-9) > 1e-9 {
		t.Fatalf("throttled eq 28%% under peak = %v, want 9 (breaker halves, no floor-up)", eq)
	}
	// Unpinned PolyUS with no authenticated receipt: honestly unknown.
	s.mutateCfg(func(c *config.Config) { c.Risk.PolyusLiveBankroll = 0 })
	valid["polyus"] = false
	if b, src := s.liveVenueBankroll(ctx, "polyus"); src != "unknown" || b != 0 {
		t.Fatalf("polyus unpinned = %v (%s), want 0 (unknown)", b, src)
	}
}

// RawFlow fixed rules: 10–50¢ band, one lot per market, ML-hedge block, epoch reset re-seeds.
func TestR103RawFlowBookRules(t *testing.T) {
	s := testServer(t)
	r107kal(s)
	ctx := context.Background()
	s.mutateCfg(func(c *config.Config) {
		c.Auto.PaperTotalStart = 1000
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML, c.Auto.AllocRawFlow = 0.35, 0.10, 0.35, 0.20
	})
	if !s.rawFlowEnabled() {
		t.Fatal("alloc_rawflow=0.2 must enable the book")
	}
	if seed := s.rawFlowSeedBank(); math.Abs(seed-200) > 1e-9 {
		t.Fatalf("seed bank = %v, want 200 (0.20 × 1000)", seed)
	}
	openN := func() int {
		s.rfBookMu.Lock()
		defer s.rfBookMu.Unlock()
		return len(s.rfLoadLocked().Open)
	}
	// The old JSON portfolio is settlement/history-only. Even an in-band candidate cannot append
	// a modeled lot; it is handed to the delayed generic Paper worker instead.
	s.rawFlowPlace(ctx, "KXRF-A", "T", "YES", 0.60, 1)
	s.rawFlowPlace(ctx, "KXRF-A", "T", "YES", 0.08, 1)
	s.rawFlowPlace(ctx, "KXRF-A", "T", "YES", 0.30, 1)
	if openN() != 0 {
		t.Fatalf("retired RawFlow JSON path opened %d modeled lot(s)", openN())
	}
	// Old lots still settle correctly and remain auditable.
	s.rfBookMu.Lock()
	b := s.rfLoadLocked()
	b.Open = []kfPos{
		{TS: "2026-07-20T00:00:00Z", Ticker: "KXRF-A", Title: "T", Side: "YES", Price: .30, Contracts: 2, SL: .24},
		{TS: "2026-07-20T00:00:00Z", Ticker: "KXRF-B", Title: "T", Side: "YES", Price: .30, Contracts: 2},
	}
	s.rfBookMu.Unlock()
	s.topMarksMu.Lock()
	if s.topMarks == nil {
		s.topMarks = map[string]float64{}
	}
	s.topMarks["KXRF-A"] = 0.20 // YES mark 20¢ ≤ SL 24¢ → stop fires
	s.topMarksMu.Unlock()
	s.settleRawFlowBook(ctx)
	if openN() != 1 {
		t.Fatalf("stop-loss must close the marked-through lot (open=%d)", openN())
	}
	s.rfBookMu.Lock()
	closedN := len(s.rfLoadLocked().Closed)
	reason := ""
	if closedN > 0 {
		reason = s.rfLoadLocked().Closed[closedN-1].Reason
	}
	lossNet := s.rfLoadLocked().Net
	s.rfBookMu.Unlock()
	if closedN != 1 || reason != "stop-loss" || lossNet >= 0 {
		t.Fatalf("stop close: n=%d reason=%q net=%v — want 1 lot, stop-loss, negative net", closedN, reason, lossNet)
	}
	// Reset starts flat: bases stamp, bank re-seeds, sparkline restarts, old opens archive.
	s.resetRawFlowBook(ctx)
	s.rfBookMu.Lock()
	b = s.rfLoadLocked()
	if b.NetBase != b.Net || b.Bank != 200 || len(b.Equity) != 0 || len(b.Open) != 0 {
		t.Fatalf("reset: netBase=%v net=%v bank=%v equity=%d open=%d — want stamped/200/0/0", b.NetBase, b.Net, b.Bank, len(b.Equity), len(b.Open))
	}
	s.rfBookMu.Unlock()
	// NAV component: after the reset the epoch net is 0 again.
	if got := s.rawFlowNetSinceEpoch(); got != 0 {
		t.Fatalf("post-reset epoch net = %v, want 0", got)
	}
}
