package server

// R77 tests — item 1 (operator screenshot: "after RESET, ML book still shows Net -$101.87 /
// Closed 28, shadow -$160.25 / Closed 44"): RESET must re-epoch every DISPLAYED book stat
// (net / closed / win-rate / ROI / equity) while lifetime history keeps accruing internally,
// exactly like the paper book's net_base pattern. The fixtures mirror the real ml_shadow.json
// shape (R64 $800 gross book + R67h stored entry fee); TestR77ResetOnRealBookCopies additionally
// replays the reset on temp COPIES of the real data files when R77_BOOKS_DIR points at them.
// Also: item 8's fee-mode single-source-of-truth derivation.

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func writeBookJSON(t *testing.T, dir, name string, v map[string]any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), b, 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
}

func TestResetMLBookReEpochsDisplayedStats(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	// One open lot (R67h shadow shape: entry fee stored at open) + a lifetime that matches the
	// operator's stuck screenshot numbers. bank0 800 = the R64 shadow bank.
	writeBookJSON(t, dir, "ml_shadow.json", map[string]any{
		"bank0": 800.0,
		"open": []any{map[string]any{
			"ticker": "T-A", "side": "YES", "signal_type": "r77-test", "platform": "kalshi",
			"title": "A", "price": 0.40, "contracts": 25.0, "fee": 0.11, "opened": 1000.0,
		}},
		"closed":   []any{map[string]any{"ticker": "T-B", "pnl": -1.00, "won": 0.0}},
		"lifetime": map[string]any{"net": -160.25, "closed": 44.0, "wins": 18.0, "net_base": 0.0, "bank_ver": 3.0, "shadow_bank_ver": 4.0},
		"equity":   []any{map[string]any{"t": 1.0, "eq": 639.75}},
		// the sidecar ships extra stats keys the dashboard renders — the Go re-mark must PRESERVE them
		"stats": map[string]any{"ev_pred_mean": 0.05, "ev_real_mean": -0.01},
	})
	if n := s.resetMLBook(ctx, "ml_shadow.json"); n != 1 {
		t.Fatalf("resetMLBook closed %d, want 1", n)
	}
	pf, ok := s.liveMarkBook(ctx, dir, "ml_shadow.json").(map[string]any)
	if !ok || pf == nil {
		t.Fatal("liveMarkBook returned no book")
	}
	st, ok := pf["stats"].(map[string]any)
	if !ok {
		t.Fatal("no stats block")
	}
	gf := func(k string) float64 { v, _ := st[k].(float64); return v }
	// The dashboard header reads THESE numbers — every one must restart at the reset epoch.
	// (No live mark available in tests → the open closes flat-at-entry, pnl = −stored fee.)
	if gf("net") != 0 {
		t.Fatalf("post-reset displayed net = %v, want 0 (the screenshot bug: lifetime leaked through)", st["net"])
	}
	if gf("closed") != 0 {
		t.Fatalf("post-reset displayed closed = %v, want 0", st["closed"])
	}
	if gf("win_rate") != 0 || gf("roi") != 0 {
		t.Fatalf("post-reset win_rate/roi = %v/%v, want 0/0", st["win_rate"], st["roi"])
	}
	if gf("equity") != 800 {
		t.Fatalf("post-reset equity = %v, want the $800 bank0 anchor", st["equity"])
	}
	// History preserved INTERNALLY: lifetime carries the pre-reset book + the force-close.
	// pnl of the force-close = 25×(0.40−0.40) − 0.11 stored entry fee (NOT a taker exit fee at the
	// close price — the R67h ledger charges one entry-leg fee, so must the reset).
	if got, want := gf("net_lifetime"), -160.36; math.Abs(got-want) > 0.005 {
		t.Fatalf("lifetime net = %v, want %v (stored-fee close)", got, want)
	}
	if gf("closed_lifetime") != 45 {
		t.Fatalf("lifetime closed = %v, want 45", st["closed_lifetime"])
	}
	if _, ok := st["ev_pred_mean"]; !ok {
		t.Fatal("liveMarkBook dropped the sidecar's ev_pred_mean — stats must MERGE, not replace")
	}
	if eq, _ := pf["equity"].([]any); len(eq) != 0 {
		t.Fatalf("equity series must restart empty at reset, got %d points", len(eq))
	}
	// Idempotence: a second reset re-rebases to the same $0 with nothing left to close.
	if n := s.resetMLBook(ctx, "ml_shadow.json"); n != 0 {
		t.Fatalf("second reset closed %d, want 0", n)
	}
}

func TestResetKflowBooksReEpochsDisplay(t *testing.T) {
	s := testServer(t)
	s.kfBookMu.Lock()
	b := s.kfLoadLocked()
	b.Pre.Net, b.Pre.Wins, b.Pre.Losses = -12.5, 3, 7
	b.Pre.Equity = [][2]float64{{1, -12.5}}
	s.kfDirty = true
	s.kfBookMu.Unlock()
	s.resetKflowBooks(context.Background())
	pay := s.kflowBooksPayload()
	pre, ok := pay["pre"].(map[string]any)
	if !ok {
		t.Fatal("no pre book in payload")
	}
	// R166: headline stats describe only the clean current epoch. Lifetime history remains
	// separately labeled and immutable.
	if v, _ := pre["net"].(float64); v != 0 {
		t.Fatalf("post-reset kflow current net = %v, want 0", pre["net"])
	}
	if v, _ := pre["closed"].(int); v != 0 {
		t.Fatalf("post-reset kflow current closed = %v, want 0", pre["closed"])
	}
	if v, _ := pre["net_lifetime"].(float64); v != -12.5 {
		t.Fatalf("kflow lifetime net = %v, want -12.5 (history preserved)", pre["net_lifetime"])
	}
	if v, _ := pre["closed_lifetime"].(int); v != 10 {
		t.Fatalf("kflow lifetime closed = %v, want 10", pre["closed_lifetime"])
	}
	if eq, _ := pre["equity"].([][2]float64); len(eq) != 0 {
		t.Fatalf("kflow equity sparkline must restart at reset, got %d points", len(eq))
	}
	// The reset stamps the epoch bases used by the current-only display.
	s.kfBookMu.Lock()
	if nb := s.kfLoadLocked().Pre.NetBase; nb != -12.5 {
		s.kfBookMu.Unlock()
		t.Fatalf("reset must stamp NetBase = -12.5, got %v", nb)
	}
	s.kfBookMu.Unlock()
}

// TestFeeModeSingleSourceOfTruth (item 8): fee_maker_share is THE fee-mode key — a diverged
// legacy maker_first (the exact live-config state: maker_first=false + share=1) must not flip
// the effective execution/fee intent to taker.
func TestFeeModeSingleSourceOfTruth(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) { c.Auto.FeeMakerShare = 1.0; c.Auto.MakerFirst = false })
	if !s.makerIntent() {
		t.Fatal("share=1.0 with stale maker_first=false must still read as maker intent")
	}
	s.mutateCfg(func(c *config.Config) { c.Auto.FeeMakerShare = 0 })
	if s.makerIntent() {
		t.Fatal("share=0 must read as taker")
	}
}

// TestR77ResetOnRealBookCopies replays the reset on temp COPIES of the real book JSONs
// (item 1 verification: "simulate a reset on a temp copy of the book JSONs"). Point
// R77_BOOKS_DIR at a folder holding copies of ml_paper.json / ml_shadow.json; skipped otherwise
// so CI/regular runs stay hermetic. The live data dir is never touched.
func TestR77ResetOnRealBookCopies(t *testing.T) {
	src := os.Getenv("R77_BOOKS_DIR")
	if src == "" {
		t.Skip("R77_BOOKS_DIR not set — the fixture test above covers the logic")
	}
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	for _, f := range []string{"ml_paper.json", "ml_shadow.json"} {
		b, err := os.ReadFile(filepath.Join(src, f))
		if err != nil {
			t.Logf("%s: copy not present (%v) — skipped", f, err)
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, f), b, 0o644); err != nil {
			t.Fatalf("stage copy: %v", err)
		}
		if n := s.resetMLBook(ctx, f); n < 0 {
			t.Fatalf("%s: book lock busy on a private temp copy?!", f)
		}
		pf, _ := s.liveMarkBook(ctx, dir, f).(map[string]any)
		if pf == nil {
			t.Fatalf("%s: unreadable after reset", f)
		}
		st, _ := pf["stats"].(map[string]any)
		if st == nil {
			t.Fatalf("%s: no stats after reset", f)
		}
		net, _ := st["net"].(float64)
		closed, _ := st["closed"].(float64)
		if net != 0 || closed != 0 {
			t.Fatalf("%s: post-reset displayed net/closed = %v/%v, want 0/0", f, net, closed)
		}
		t.Logf("%s: displayed stats re-epoched (net $0, closed 0) · lifetime net %v · lifetime closed %v",
			f, st["net_lifetime"], st["closed_lifetime"])
	}
}
