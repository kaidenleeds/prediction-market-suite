package server

// R126 tests — both-side gap scanning (Part 1), episode re-entry (Part 2), the wait-vs-now
// formula (Part 4.2), the offline placement gate (Part 6.2) and the ROI-band weights (Part 5).
// The flip-twin PRICE MAPPING is pinned via the pure helpers (xvBothSides/xvBestExpr) — building
// a full gameident structural registry in a hermetic test would re-implement the matcher, and the
// same-side paths above already pin the emission shape end-to-end (r93_test.go).

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

// TestR126BothSides — the unified-gap → physical-side mapping (dedup honesty: one signed gap per
// pair; the four (direction, flip) cells each buy exactly one PUS side and one Kalshi side).
func TestR126BothSides(t *testing.T) {
	cases := []struct {
		gap                  float64
		flip                 bool
		wantPus, wantK       string
	}{
		{+0.05, false, "YES", "NO"}, // PUS K-YES-equiv cheap, same-side → PUS YES (the proven cohort)
		{+0.05, true, "NO", "NO"},   // flip twin: the K-YES-equiv on PUS is its NO side
		{-0.05, false, "NO", "YES"}, // Kalshi cheap, same-side → PUS expression = PUS NO (new cohort)
		{-0.05, true, "YES", "YES"}, // Kalshi cheap, flip → PUS YES
	}
	for _, c := range cases {
		p, k := xvBothSides(c.gap, c.flip)
		if p != c.wantPus || k != c.wantK {
			t.Fatalf("xvBothSides(%v, flip=%v) = (%s,%s), want (%s,%s)", c.gap, c.flip, p, k, c.wantPus, c.wantK)
		}
	}
}

// TestR126BestExpr — expression choice by executable edge; a visibly EMPTY touch (depth 0) can
// never win, unknown depth (−1) stays comparable.
func TestR126BestExpr(t *testing.T) {
	pus := xvExprEdge{Expr: "pus_yes", EdgeC: 4.0, DepthC: 50}
	kal := xvExprEdge{Expr: "k_no", EdgeC: 5.5, DepthC: -1}
	best, alt := xvBestExpr(pus, kal)
	if best.Expr != "k_no" || alt.Expr != "pus_yes" {
		t.Fatalf("higher edge with unknown depth must win: best=%s", best.Expr)
	}
	kal.DepthC = 0 // visibly empty book — unfillable, loses regardless of edge
	if best, _ = xvBestExpr(pus, kal); best.Expr != "pus_yes" {
		t.Fatalf("depth-0 expression must lose: best=%s", best.Expr)
	}
}

// TestR126EpisodeTracker — open → close (≤1¢) → reopen = a NEW episode; same-episode readings
// don't increment; a direction flip through zero while active starts a new episode.
func TestR126EpisodeTracker(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	key := "slug|TWIN"
	n, isNew := s.xvEpObserve(ctx, key, "ml", 5.0, 3.0, 0)
	if n != 1 || !isNew {
		t.Fatalf("first open: want (1,true), got (%d,%v)", n, isNew)
	}
	if n, isNew = s.xvEpObserve(ctx, key, "ml", 4.0, 3.0, 0); n != 1 || isNew {
		t.Fatalf("still open: want (1,false), got (%d,%v)", n, isNew)
	}
	if n, isNew = s.xvEpObserve(ctx, key, "ml", 0.5, 3.0, 0); n != 1 || isNew {
		t.Fatalf("close at ≤1¢: want (1,false), got (%d,%v)", n, isNew)
	}
	// sub-reopen wiggle (reopen threshold 4¢ via config override) must NOT reopen
	if n, isNew = s.xvEpObserve(ctx, key, "ml", 3.5, 3.0, 4.0); n != 1 || isNew {
		t.Fatalf("sub-reopen wiggle: want (1,false), got (%d,%v)", n, isNew)
	}
	if n, isNew = s.xvEpObserve(ctx, key, "ml", 4.5, 3.0, 4.0); n != 2 || !isNew {
		t.Fatalf("reopen ≥ threshold: want (2,true), got (%d,%v)", n, isNew)
	}
	// direction flip through zero while active → new episode
	if n, isNew = s.xvEpObserve(ctx, key, "ml", -5.0, 3.0, 4.0); n != 3 || !isNew {
		t.Fatalf("direction flip: want (3,true), got (%d,%v)", n, isNew)
	}
}

// TestR126EpisodeGate — the executor entry rule that replaced one-lot-per-market: same episode
// blocks, opposite side blocks (self-hedge), per-market cap holds, new episodes re-enter.
func TestR126EpisodeGate(t *testing.T) {
	open := []kfPos{{Ticker: "s1", Side: "YES", EpisodeN: 1}}
	if why := xvgEpisodeGate(open, "s2", "YES", 1, 3); why != "" {
		t.Fatalf("different market must pass, got %q", why)
	}
	if why := xvgEpisodeGate(open, "s1", "YES", 1, 3); why != "same-episode" {
		t.Fatalf("same episode must block, got %q", why)
	}
	if why := xvgEpisodeGate(open, "s1", "NO", 2, 3); why != "self-hedge" {
		t.Fatalf("opposite side must block (self-hedge), got %q", why)
	}
	if why := xvgEpisodeGate(open, "s1", "YES", 2, 3); why != "" {
		t.Fatalf("new episode same direction must re-enter, got %q", why)
	}
	open = append(open, kfPos{Ticker: "s1", Side: "YES", EpisodeN: 2}, kfPos{Ticker: "s1", Side: "YES", EpisodeN: 3})
	if why := xvgEpisodeGate(open, "s1", "YES", 4, 3); why != "episode-cap" {
		t.Fatalf("per-market cap (3) must block the 4th lot, got %q", why)
	}
	// pre-R126 lots (EpisodeN 0) block conservatively — no stacking on untracked history
	if why := xvgEpisodeGate([]kfPos{{Ticker: "s1", Side: "YES", EpisodeN: 0}}, "s1", "YES", 2, 3); why != "same-episode" {
		t.Fatalf("untracked legacy lot must block, got %q", why)
	}
}

// TestR126WaitCalc — the operator's E[wait] formula, boundary behavior: no measured arrivals ⇒
// P(arrive)=0 ⇒ decision would be "now" (E[wait] = −gap/2 < 0); a strong measured arrival rate
// with real correlation can flip it positive.
func TestR126WaitCalc(t *testing.T) {
	pArr, bonus, decay, ew := xvWaitCalc(0, 10, 0.5, 0.4, 0.5, 5)
	if pArr != 0 || bonus <= 0 || decay != 2.5 || ew >= 0 {
		t.Fatalf("zero λ must yield pArr=0 and negative E[wait]: pArr=%v bonus=%v decay=%v ew=%v", pArr, bonus, decay, ew)
	}
	// λ=6/hr over a 30-min half-life ⇒ pArr≈0.95; ρ=0.5 at p1=p2=0.5 ⇒ bonus 12.5¢ > decay 1¢
	pArr, bonus, _, ew = xvWaitCalc(6, 30, 0.5, 0.5, 0.5, 2)
	if pArr < 0.9 || ew <= 0 {
		t.Fatalf("measured λ + real ρ must flip the decision: pArr=%v bonus=%v ew=%v", pArr, bonus, ew)
	}
	// negative/zero correlation never mints a bonus
	if _, bonus, _, _ = xvWaitCalc(6, 30, -0.4, 0.5, 0.5, 2); bonus != 0 {
		t.Fatalf("negative ρ must not bonus: %v", bonus)
	}
}

// TestR126PxOfflineGate — Part 6.2: fresh venue data passes; a provably-dead pipeline (stale
// meta + no WS) refuses with the px-offline reason; feeds healing (fresh stamp) re-admits.
func TestR126PxOfflineGate(t *testing.T) {
	s := testServer(t)
	tk := "KXMLBGAME-26JUL06PHIKC-PHI"
	seedKalMeta(s, kalshi.Market{Ticker: tk, LastPrice: 0.60})
	if r := s.pxOfflineReason("kalshi", tk); r != "" {
		t.Fatalf("fresh kalshi meta must pass, got %q", r)
	}
	s.metaMu.Lock()
	s.kmktsAt[tk] = time.Now().Add(-10 * time.Minute)
	s.metaMu.Unlock()
	if r := s.pxOfflineReason("kalshi", tk); r != "px-offline:kalshi" {
		t.Fatalf("stale kalshi pipeline must refuse, got %q", r)
	}
	s.metaMu.Lock()
	s.kmktsAt[tk] = time.Now() // watchdogs healed the feed → placements resume on their own
	s.metaMu.Unlock()
	if r := s.pxOfflineReason("kalshi", tk); r != "" {
		t.Fatalf("healed kalshi pipeline must pass again, got %q", r)
	}
	// polyus: zero-value snapshot age = dead; a fresh snapshot stamp passes
	if r := s.pxOfflineReason("polyus", "some-slug"); r != "px-offline:polyus" {
		t.Fatalf("dead polyus snapshot must refuse, got %q", r)
	}
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now()
	s.polyUSMu.Unlock()
	if r := s.pxOfflineReason("polyus", "some-slug"); r != "" {
		t.Fatalf("fresh polyus snapshot must pass, got %q", r)
	}
	// poly-int has no cheap age signal here — never gated (documented)
	if r := s.pxOfflineReason("polymarket", "0xabc"); r != "" {
		t.Fatalf("poly-int must not gate, got %q", r)
	}
}

// TestR126RoiBandWeight — Part 5 loader: weights serve per venue+band, the measured polymarket
// "10-15" concentration artifact is excluded by name, missing file = zero weights (never invent).
func TestR126RoiBandWeight(t *testing.T) {
	s := testServer(t)
	if w := s.roiBandWeight("kalshi", 0.07); w != 0 {
		t.Fatalf("no file → no weight, got %v", w)
	}
	blob := `{"generated":"x","views":{},"preferred_bands":{
		"kalshi":[["0-10",1.0],["10-15",0.54]],
		"polymarket":[["10-15",1.0],["55-60",0.25]],
		"polyus":[["5-10",1.0]]}}`
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "roi_bands.json"), []byte(blob), 0o644); err != nil {
		t.Fatal(err)
	}
	s.roiMu.Lock() // force reload past the 6h cache
	s.roiBands, s.roiLoadAt = nil, time.Time{}
	s.roiMu.Unlock()
	if w := s.roiBandWeight("kalshi", 0.07); math.Abs(w-1.0) > 1e-9 {
		t.Fatalf("kalshi 7¢ must weigh 1.0, got %v", w)
	}
	if w := s.roiBandWeight("kalshi", 0.12); math.Abs(w-0.54) > 1e-9 {
		t.Fatalf("kalshi 12¢ must weigh 0.54, got %v", w)
	}
	if w := s.roiBandWeight("kalshi", 0.30); w != 0 {
		t.Fatalf("unlisted band must weigh 0, got %v", w)
	}
	if w := s.roiBandWeight("polymarket", 0.12); w != 0 {
		t.Fatalf("the polymarket 10-15 artifact must be EXCLUDED, got %v", w)
	}
	if w := s.roiBandWeight("polymarket", 0.57); math.Abs(w-0.25) > 1e-9 {
		t.Fatalf("polymarket 57¢ must weigh 0.25, got %v", w)
	}
	if w := s.roiBandWeight("polyus", 0.06); math.Abs(w-1.0) > 1e-9 {
		t.Fatalf("polyus 6¢ must weigh 1.0, got %v", w)
	}
}

// TestR126EpisodePersistence — the tracker survives a restart via kv (the per-market cap can't
// be reset by bouncing the suite).
func TestR126EpisodePersistence(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if n, _ := s.xvEpObserve(ctx, "p|T", "total", 6.0, 3.0, 0); n != 1 {
		t.Fatalf("open failed")
	}
	s.xvEpFlush(ctx)
	// simulate restart: drop the in-memory tracker, reload from kv
	s.xvEpMu.Lock()
	s.xvEpSt = nil
	s.xvEpMu.Unlock()
	if n, isNew := s.xvEpObserve(ctx, "p|T", "total", 6.0, 3.0, 0); n != 1 || isNew {
		t.Fatalf("reloaded tracker must remember the open episode: (%d,%v)", n, isNew)
	}
}
