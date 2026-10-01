package server

// R78 tests — EQUITY-FRACTION BANKROLLS (venue budgets = frac × live total paper equity,
// compounding across venues + the ML book; ml_alloc.json sidecar handshake; Reset P&L seeds a
// fresh epoch back to paper_total_start) and the EV-SHARE AUTO-ALLOCATION (95% LOWER-bound
// weights at n≥30, exploration floor for unproven families, 50% hard bound on multi-family
// venues, neutral when nothing is proven). Hermetic: temp SQLite + no venue clients.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

// bustEquityCaches clears the NAV + realized-net caches so a test's freshly-inserted fills apply
// on the very next read (production refreshes ride the 30s sweep / 20s cache instead).
func bustEquityCaches(s *Server) {
	s.invalidatePortfolioEquityCache()
	s.navMu.Lock()
	s.navAt = time.Time{}
	s.navMu.Unlock()
	s.eqMu.Lock()
	s.eqRealized, s.eqAt = nil, time.Time{}
	s.eqMu.Unlock()
}

func TestAllocFracsNormalizeAndLegacyFallback(t *testing.T) {
	s := testServer(t)
	// Defaults: paper_total_start 1000 + ¼/¼/½ → active, exact fractions.
	k, pu, ml, rf, wx, active := s.allocFracs()
	if !active || math.Abs(k-0.25) > 1e-9 || math.Abs(pu-0.25) > 1e-9 || math.Abs(ml-0.5) > 1e-9 || rf != 0 || wx != 0 {
		t.Fatalf("default fracs = %v/%v/%v/%v/%v active=%v, want 0.25/0.25/0.5/0/0 active", k, pu, ml, rf, wx, active)
	}
	// Non-normalized input normalizes by the sum ("must sum ≈ 1" enforced on read).
	s.mutateCfg(func(c *config.Config) { c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML = 0.3, 0.3, 0.3 })
	k, pu, ml, _, _, _ = s.allocFracs()
	if math.Abs(k-1.0/3) > 1e-9 || math.Abs(pu-1.0/3) > 1e-9 || math.Abs(ml-1.0/3) > 1e-9 {
		t.Fatalf("0.3/0.3/0.3 should normalize to thirds, got %v/%v/%v", k, pu, ml)
	}
	// R103: alloc_rawflow joins the normalization (the epoch-3 split: 0.35/0.10/0.35/0.20).
	s.mutateCfg(func(c *config.Config) {
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML, c.Auto.AllocRawFlow = 0.35, 0.10, 0.35, 0.20
	})
	k, pu, ml, rf, _, _ = s.allocFracs()
	if math.Abs(k-0.35) > 1e-9 || math.Abs(pu-0.10) > 1e-9 || math.Abs(ml-0.35) > 1e-9 || math.Abs(rf-0.20) > 1e-9 {
		t.Fatalf("epoch-3 fracs should read 0.35/0.10/0.35/0.20, got %v/%v/%v/%v", k, pu, ml, rf)
	}
	// R106: alloc_weather joins identically — the operator's $1,300 split (350/100/350/200/300)
	// reads back as exact dollar-preserving fractions, and 0/unset weather keeps pre-R106 math.
	s.mutateCfg(func(c *config.Config) {
		c.Auto.PaperTotalStart = 1300
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML = 350.0/1300, 100.0/1300, 350.0/1300
		c.Auto.AllocRawFlow, c.Auto.AllocWeather = 200.0/1300, 300.0/1300
	})
	k, pu, ml, rf, wx, _ = s.allocFracs()
	for name, got := range map[string]float64{"kalshi": k * 1300, "polyus": pu * 1300, "ml": ml * 1300, "rawflow": rf * 1300, "weather": wx * 1300} {
		want := map[string]float64{"kalshi": 350, "polyus": 100, "ml": 350, "rawflow": 200, "weather": 300}[name]
		if math.Abs(got-want) > 1e-6 {
			t.Fatalf("R106 split: %s share = $%v, want $%v", name, got, want)
		}
	}
	s.mutateCfg(func(c *config.Config) {
		c.Auto.PaperTotalStart = 1000
		c.Auto.AllocKalshi, c.Auto.AllocPolyus, c.Auto.AllocML, c.Auto.AllocRawFlow, c.Auto.AllocWeather = 0.3, 0.3, 0.3, 0, 0
	})
	// Explicit paper_total_start:0 = the documented legacy escape hatch.
	s.mutateCfg(func(c *config.Config) { c.Auto.PaperTotalStart = 0 })
	if _, _, _, _, _, active := s.allocFracs(); active {
		t.Fatal("paper_total_start=0 must deactivate equity-fraction mode (legacy flat bankrolls)")
	}
}

// TestPlatformBankrollEquityFractionCompounds — R127 REVISION: the frac × NAV compounding split
// is SUPERSEDED by the four fixed portfolio banks. R143 sets every portfolio to $600 at all
// times (a polyus win must NOT move the kalshi budget anymore), the ML handshake hands over the
// fixed $600, and NAV keeps computing for its informational surfaces. Legacy flat mode
// (paper_total_start=0) is unchanged.
func TestPlatformBankrollEquityFractionCompounds(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if got := s.platformBankroll("kalshi"); math.Abs(got-600) > 0.01 {
		t.Fatalf("kalshi budget = %v, want the fixed $600 portfolio bank (R143)", got)
	}
	// A POLYUS win must NOT move the kalshi budget (the R78 cross-compounding is superseded).
	roundTrip(t, s, "polyus", "PU-WIN", "auto-cons-pusflow", 0.50, 0.60, 1000, 0) // +$100 net
	bustEquityCaches(s)
	if got := s.platformBankroll("kalshi"); math.Abs(got-600) > 0.01 {
		t.Fatalf("kalshi budget after +$100 polyus net = %v, want the fixed $600 (no cross-compounding)", got)
	}
	// ML book net still joins the NAV (informational) via ml_paper.json (lifetime.net − net_base).
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"lifetime": map[string]any{"net": 260.0, "net_base": 60.0}, // +200 since its epoch
	})
	bustEquityCaches(s)
	// The handshake: ml_bank_usd = the FIXED $600 book; total_equity_usd = the live NAV (1300).
	s.writeMLAllocFile(context.Background())
	var hs struct {
		Bank float64 `json:"ml_bank_usd"`
		NAV  float64 `json:"total_equity_usd"`
	}
	b, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "ml_alloc.json"))
	if err != nil || json.Unmarshal(b, &hs) != nil {
		t.Fatalf("ml_alloc.json handshake unreadable: %v", err)
	}
	if math.Abs(hs.Bank-600) > 0.01 || math.Abs(hs.NAV-1300) > 0.01 {
		t.Fatalf("handshake bank/nav = %v/%v, want the fixed 600 / NAV 1300", hs.Bank, hs.NAV)
	}
	// Legacy mode: flat bankroll + own realized net only (poly-int semantics unchanged either way).
	s.mutateCfg(func(c *config.Config) { c.Auto.PaperTotalStart = 0 })
	bustEquityCaches(s)
	if got := s.platformBankroll("kalshi"); math.Abs(got-600) > 0.01 {
		t.Fatalf("compatibility-mode kalshi equity = %v, want its unchanged $600 funded grant", got)
	}
	_ = ctx
}

func TestResetSeedsFreshEquityEpoch(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	roundTrip(t, s, "polyus", "PU-EPOCH", "auto-cons-pusflow", 0.50, 0.70, 500, 0) // +$100
	bustEquityCaches(s)
	if nav := s.totalPaperEquityUSD(); math.Abs(nav-1100) > 0.01 {
		t.Fatalf("pre-reset NAV = %v, want 1100", nav)
	}
	if _, busy := s.doPaperReset(ctx); busy {
		t.Fatal("temp dir: ML book lock must not read busy")
	}
	// paper_total_start seeds the fresh epoch: every net component re-baselined → NAV = seed again.
	if nav := s.totalPaperEquityUSD(); math.Abs(nav-1000) > 0.01 {
		t.Fatalf("post-reset NAV = %v, want the 1000 seed (fresh epoch)", nav)
	}
	var hs struct {
		Bank float64 `json:"ml_bank_usd"`
	}
	b, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "ml_alloc.json"))
	if err != nil || json.Unmarshal(b, &hs) != nil {
		t.Fatalf("reset must push a fresh handshake: %v", err)
	}
	if math.Abs(hs.Bank-600) > 0.01 {
		t.Fatalf("post-reset handshake bank = %v, want the fixed 600 ML book (R143)", hs.Bank)
	}
}

// roundTrip books one BUY+SELL round for `source` on `platform`: contracts ct at entry→exit,
// zero fees, so net/contract = exit − entry exactly (keeps the LB math hand-checkable).
func roundTrip(t *testing.T, s *Server, platform, ticker, source string, entry, exit float64, ct float64, fee float64) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: platform, Ticker: ticker, Title: "T " + ticker,
		Side: "Yes", Action: "BUY", Price: entry, Contracts: ct, Fee: fee, Source: source}); err != nil {
		t.Fatalf("buy: %v", err)
	}
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: platform, Ticker: ticker, Title: "T " + ticker,
		Side: "Yes", Action: "SELL", Price: exit, Contracts: ct, Fee: fee, Source: "settled"}); err != nil {
		t.Fatalf("sell: %v", err)
	}
}

func TestRebuildAllocationsEVShare(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// Exactly TWO placement-enabled families on kalshi: kcrypto + xmatch (ML routing off so the
	// directional families own their placement; everything else explicitly off).
	s.mutateCfg(func(c *config.Config) {
		a := &c.Auto
		a.MLDrivenDirectional = false
		a.ConsensusEnabled = true
		a.ConsensusArb, a.ConsensusKalshi, a.ConsensusCross = false, false, false
		a.ConsensusCrypto, a.ConsensusXMatch = true, true
		a.ConsensusKThresh, a.ConsensusPMatch, a.ConsensusPCrypto = false, false, false
		a.ConsensusFavLong, a.ConsensusConfluence, a.ConsensusXVLag = false, false, false
		a.ConsensusPolyUSFlow = false
		a.ConsensusKalshiFlow = false // R79: kflow is a placement family now — off to keep this venue at exactly two
	})
	// kcrypto: 40 identical +10¢/ct wins → zero variance → LB = +0.10 exactly (proven).
	for i := 0; i < 40; i++ {
		roundTrip(t, s, "kalshi", fmt.Sprintf("KC-%d", i), "auto-cons-kcrypto", 0.50, 0.60, 1, 0)
	}
	// xmatch: 40 alternating +30¢/−20¢ → mean +5¢ but sd 25¢ → 95% LB < 0 (NOT significant): the
	// "n=5 +25¢ gets ~0" principle — a positive mean without significance earns no weight.
	for i := 0; i < 40; i++ {
		exit := 0.80
		if i%2 == 1 {
			exit = 0.30
		}
		roundTrip(t, s, "kalshi", fmt.Sprintf("XM-%d", i), "auto-cons-xmatch", 0.50, exit, 1, 0)
	}
	s.rebuildAllocations(ctx)
	s.allocMu.Lock()
	rows := append([]allocRow(nil), s.allocRows...)
	s.allocMu.Unlock()
	byKey := map[string]allocRow{}
	for _, r := range rows {
		byKey[r.Source+"|"+r.Venue] = r
	}
	kc, okKC := byKey["auto-cons-kcrypto|kalshi"]
	xm, okXM := byKey["auto-cons-xmatch|kalshi"]
	if !okKC || !okXM {
		t.Fatalf("allocation rows missing: kc=%v xm=%v (rows=%v)", okKC, okXM, rows)
	}
	// kcrypto proven: LB +0.10; share capped at the 50% HARD BOUND (2 enabled families on the
	// venue) → mult = 0.5 × 2 = 1.0. xmatch below the floor → exploration stake (default 0.10).
	if kc.Explore || kc.Neutral || math.Abs(kc.EVLBPC-0.10) > 0.005 {
		t.Fatalf("kcrypto row = %+v, want proven with EV LB ≈ +0.10", kc)
	}
	if math.Abs(kc.Share-0.5) > 1e-6 || math.Abs(kc.Mult-1.0) > 0.001 {
		t.Fatalf("kcrypto share/mult = %v/%v, want 0.5 (hard bound) / ×1.0", kc.Share, kc.Mult)
	}
	if !xm.Explore || math.Abs(xm.Mult-0.10) > 0.001 {
		t.Fatalf("xmatch row = %+v, want exploration stake ×0.10 (LB below 0 at n=40)", xm)
	}
	if xm.EVLBPC >= 0 {
		t.Fatalf("xmatch LB = %v, want < 0 (mean +5¢ is not significant at sd 25¢, n=40)", xm.EVLBPC)
	}
	// autoPlace consumes the same table through allocStakeMult.
	if m := s.allocStakeMult("auto-cons-kcrypto", "kalshi"); math.Abs(m-1.0) > 0.001 {
		t.Fatalf("stake mult kcrypto = %v, want 1.0", m)
	}
	if m := s.allocStakeMult("auto-cons-xmatch", "kalshi"); math.Abs(m-0.10) > 0.001 {
		t.Fatalf("stake mult xmatch = %v, want 0.10", m)
	}
	// Non-auto sources and unknown families stay unscaled — the allocator only touches paper auto.
	if s.allocStakeMult("gate", "kalshi") != 1 || s.allocStakeMult("manual", "kalshi") != 1 ||
		s.allocStakeMult("auto-cons-kcrypto", "polymarket") != 1 || s.allocStakeMult("auto-never-seen", "kalshi") != 1 {
		t.Fatal("gate/manual/poly-int/unknown must all stay ×1.0")
	}
}

// TestHandleAllocShape — the /api/alloc payload the Research → PERFORMANCE panel renders and the
// export captures must carry the full shape (active/budgets/alloc/rows) with sane numbers.
func TestHandleAllocShape(t *testing.T) {
	s := testServer(t)
	s.rebuildAllocations(context.Background())
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/alloc", nil)
	s.handleAlloc(rec, req)
	var d struct {
		Active  bool               `json:"active"`
		NAV     float64            `json:"total_equity_usd"`
		Alloc   map[string]float64 `json:"alloc"`
		Budgets map[string]float64 `json:"budgets"`
		Rows    []allocRow         `json:"rows"`
		MinN    int                `json:"min_n"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &d); err != nil {
		t.Fatalf("alloc payload unparseable: %v (%s)", err, rec.Body.String())
	}
	if !d.Active || math.Abs(d.NAV-1000) > 0.01 || d.MinN != 30 {
		t.Fatalf("alloc payload active/nav/min_n = %v/%v/%v, want true/1000/30", d.Active, d.NAV, d.MinN)
	}
	// R143: all four paper portfolios have fixed $600 banks.
	if math.Abs(d.Budgets["kalshi"]-600) > 0.01 || math.Abs(d.Budgets["polyus"]-600) > 0.01 ||
		d.Budgets["ml"] != 0 || math.Abs(d.Budgets["combos"]-600) > 0.01 {
		t.Fatalf("alloc budgets = %v, want K/PUS/Combo grants and fail-closed cold ML", d.Budgets)
	}
	if len(d.Rows) == 0 {
		t.Fatal("alloc payload must carry the (neutral) rows for enabled families — the export/UI shape check")
	}
}

func TestRebuildAllocationsNeutralWithoutEvidence(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// Default config: MLDrivenDirectional on → auto-ml is the (lone-ish) placement-enabled family,
	// and there are ZERO closed trades — the allocator must go NEUTRAL (×1.0), never all-explorers
	// (which would silently 10× down every stake with no evidence backing the cut).
	s.rebuildAllocations(ctx)
	if m := s.allocStakeMult("auto-ml", "kalshi"); m != 1 {
		t.Fatalf("no-evidence mult = %v, want neutral 1.0", m)
	}
	s.allocMu.Lock()
	rows := append([]allocRow(nil), s.allocRows...)
	s.allocMu.Unlock()
	if len(rows) == 0 {
		t.Fatal("expected neutral rows for the enabled families")
	}
	for _, r := range rows {
		if !r.Neutral || r.Mult != 1 {
			t.Fatalf("row %+v, want neutral ×1.0", r)
		}
	}
}
