package server

// R92 tests — operator: "make sure that reset and reset on start actually resets equity and
// profit" + the Telegram equity header.
//
// What R92 fixed (each test pins one class):
//  1. resetMLBook rebased the lifetime bases but left the FILE's sidecar-owned stats block at
//     pre-reset values — briefings/Telegram (which read the file directly, not liveMarkBook)
//     kept quoting the old net/equity until the sidecar's next cycle (forever, if it was down).
//  2. The RESET button path tried the ML/shadow books ONCE — a click during a sidecar cycle
//     (cross-process book lock held) silently skipped them; the UI ignored ml_lock_busy.
//  3. The briefing's once-per-run session baselines survived the reset, so "since start" deltas
//     kept measuring against pre-reset nets.
//  4. The new equity header: 💰 equity = cash + open-position value per money book.

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// readBookFileRaw reads a book file EXACTLY like the briefing/Telegram consumers do (raw
// json.Unmarshal of the file bytes — NOT liveMarkBook, which recomputes from bases and would
// mask a stale stats block).
func readBookFileRaw(t *testing.T, dir, name string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	var pf map[string]any
	if err := json.Unmarshal(b, &pf); err != nil {
		t.Fatalf("parse %s: %v", name, err)
	}
	return pf
}

// TestR92ResetWritesFreshStatsBlock — fix #1 (shadow book: bank0 comes from the FILE, $800).
func TestR92ResetWritesFreshStatsBlock(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	writeBookJSON(t, dir, "ml_shadow.json", map[string]any{
		"bank0": 800.0,
		"open": []any{map[string]any{
			"ticker": "T-A", "side": "YES", "signal_type": "r92-test", "platform": "kalshi",
			"title": "A", "price": 0.40, "contracts": 25.0, "fee": 0.11, "opened": 1000.0,
		}},
		"closed":   []any{map[string]any{"ticker": "T-B", "pnl": -1.00, "won": 0.0}},
		"lifetime": map[string]any{"net": -440.31, "closed": 669.0, "wins": 310.0, "net_base": 0.0, "bank_ver": 3.0},
		"equity":   []any{map[string]any{"t": 1.0, "eq": 359.69}},
		// the STALE pre-reset stats the sidecar wrote last cycle — the exact numbers the phone
		// kept showing after a reset. ev_* keys are sidecar-owned and must SURVIVE (it re-derives
		// them since-epoch next cycle).
		"stats": map[string]any{"net": -440.31, "equity": 359.69, "open_n": 138.0, "closed": 669.0,
			"win_rate": 0.463, "ev_pred_mean": 0.05, "ev_real_mean": -0.0158},
	})
	if n := s.resetMLBook(ctx, "ml_shadow.json"); n != 1 {
		t.Fatalf("resetMLBook closed %d, want 1", n)
	}
	pf := readBookFileRaw(t, dir, "ml_shadow.json")
	st, _ := pf["stats"].(map[string]any)
	if st == nil {
		t.Fatal("stats block missing after reset")
	}
	gf := func(k string) float64 { v, _ := st[k].(float64); return v }
	if gf("net") != 0 || gf("closed") != 0 || gf("wins") != 0 || gf("win_rate") != 0 || gf("open_n") != 0 {
		t.Fatalf("FILE stats not zeroed after reset (net=%v closed=%v wins=%v wr=%v open_n=%v) — briefings would keep quoting pre-reset profit",
			st["net"], st["closed"], st["wins"], st["win_rate"], st["open_n"])
	}
	if gf("equity") != 800 || gf("bank0") != 800 {
		t.Fatalf("FILE stats equity/bank0 = %v/%v, want 800/800 (equity must equal the bank the instant the reset lands)", st["equity"], st["bank0"])
	}
	if _, ok := st["ev_pred_mean"]; !ok {
		t.Fatal("reset dropped the sidecar-owned ev_pred_mean — stats must MERGE, not replace")
	}
	if eq, _ := pf["equity"].([]any); len(eq) != 0 {
		t.Fatalf("equity curve must restart EMPTY at reset, got %d points (pre-reset points would blend into the new epoch)", len(eq))
	}
	if opens, _ := pf["open"].([]any); len(opens) != 0 {
		t.Fatalf("open positions must be force-closed at reset, %d still open", len(opens))
	}
}

// TestR92PaperBookResetAnchorsToAlloc — fix #1 for ml_paper.json. R127 REVISION: the fresh-epoch
// bank is the FIXED ML book bank (book_ml_usd, $1,000) — the same number the sidecar's next
// ml_alloc.json handshake now hands over, NOT the stale mid-epoch topped-up bank0 in the file.
func TestR92PaperBookResetAnchorsToAlloc(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	writeBookJSON(t, dir, "ml_paper.json", map[string]any{
		"bank0":    474.96, // mid-epoch equity-alloc top-ups moved it off the configured start (auditor r22)
		"open":     []any{},
		"closed":   []any{},
		"lifetime": map[string]any{"net": -41.03, "closed": 156.0, "wins": 89.0, "net_base": 0.0, "bank_ver": 3.0},
		"equity":   []any{map[string]any{"t": 1.0, "eq": 433.93}},
		"stats":    map[string]any{"net": -41.03, "equity": 433.93},
	})
	if n := s.resetMLBook(ctx, "ml_paper.json"); n != 0 {
		t.Fatalf("resetMLBook closed %d, want 0 (no opens)", n)
	}
	pf := readBookFileRaw(t, dir, "ml_paper.json")
	st, _ := pf["stats"].(map[string]any)
	gf := func(m map[string]any, k string) float64 { v, _ := m[k].(float64); return v }
	want := s.bookBankUSD(vbML) // R127: the fixed $1,000 ML book bank is the fresh-epoch anchor
	if gf(st, "equity") != want || gf(pf, "bank0") != want {
		t.Fatalf("post-reset equity/bank0 = %v/%v, want %v (book_ml_usd — the fresh-epoch anchor)",
			st["equity"], pf["bank0"], want)
	}
	if gf(st, "net") != 0 {
		t.Fatalf("post-reset displayed net = %v, want 0", st["net"])
	}
}

// TestR92ResetRetriesWhenLockBusy — fix #2: the button path must RETRY the sidecar lock instead
// of silently skipping the books, and must report busy only when the lock outlasts every retry.
func TestR92ResetRetriesWhenLockBusy(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 400.0, "open": []any{}, "closed": []any{},
		"lifetime": map[string]any{"net": 5.0, "closed": 1.0, "wins": 1.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	oldR, oldS := resetLockRetries, resetLockRetrySleep
	oldW, oldStale := bookLockWait, bookLockStale
	// Shrink the per-attempt lock wait so the test runs in ms, and push the stale-steal window out
	// so the deliberately-held lock is never broken as "crashed holder" (the steal is correct
	// production behavior — the first run of this test proved it works at 15s — but here we are
	// simulating a LIVE holder).
	resetLockRetries, resetLockRetrySleep = 2, time.Millisecond
	bookLockWait, bookLockStale = 30*time.Millisecond, 10*time.Minute
	defer func() {
		resetLockRetries, resetLockRetrySleep = oldR, oldS
		bookLockWait, bookLockStale = oldW, oldStale
	}()
	release, ok := acquireBookLock(s.cfg().DataDir)
	if !ok {
		t.Fatal("could not take the book lock for the test")
	}
	if _, busy := s.doPaperReset(ctx); !busy {
		release()
		t.Fatal("lock held through every retry — doPaperReset must report ml_lock_busy=true")
	}
	release()
	if _, busy := s.doPaperReset(ctx); busy {
		t.Fatal("lock free — doPaperReset must not report busy (retry path must not wedge the lock)")
	}
	// and the released-lock pass actually re-based the book:
	pf := readBookFileRaw(t, s.cfg().DataDir, "ml_paper.json")
	life, _ := pf["lifetime"].(map[string]any)
	if nb, _ := life["net_base"].(float64); nb != 5.0 {
		t.Fatalf("net_base = %v, want 5.0 (rebase landed once the lock freed)", life["net_base"])
	}
}

// TestR92ResetRecapturesBriefingBaselines — fix #3: doPaperReset must clear the once-per-run
// briefing session/day baselines so the next briefing measures from the fresh epoch.
func TestR92ResetRecapturesBriefingBaselines(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.briefMu.Lock()
	s.briefSessSet, s.briefDayDate = true, "2026-07-06"
	s.briefSessMLNet = -41.03 // a pre-reset baseline that must not survive
	s.briefMu.Unlock()
	if _, busy := s.doPaperReset(ctx); busy {
		t.Fatal("unexpected lock busy in a temp dir")
	}
	s.briefMu.Lock()
	sessSet, day := s.briefSessSet, s.briefDayDate
	s.briefMu.Unlock()
	if sessSet || day != "" {
		t.Fatalf("briefing baselines survived the reset (sessSet=%v day=%q) — 'since start' would keep blending pre-reset profit", sessSet, day)
	}
	// pnl curve restarts (samples cleared; persisted series wiped is covered by R70's test).
	s.pnlMu.Lock()
	nSamples := len(s.pnlSamples)
	s.pnlMu.Unlock()
	if nSamples != 0 {
		t.Fatalf("pnlSamples = %d, want 0 (graph must restart from the reset baseline)", nSamples)
	}
	// kflow twins: equity sparkline restarted.
	s.kfBookMu.Lock()
	kb := s.kfLoadLocked()
	preEq, liveEq := len(kb.Pre.Equity), len(kb.Live.Equity)
	s.kfBookMu.Unlock()
	if preEq != 0 || liveEq != 0 {
		t.Fatalf("kflow equity sparklines = %d/%d points, want 0/0", preEq, liveEq)
	}
}

// TestR92EquityHeaderBlock — R127 REVISION (operator's four-book restructure): the header is the
// compact FOUR-BOOK run block now (venue, process-epoch closed net, cents/round), then mini +
// the current active-system summary. Bankroll/open/roster detail does not belong in the briefing.
func TestR92EquityHeaderBlock(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 1000.0,
		"open": []any{map[string]any{
			"ticker": "T-H", "side": "YES", "signal_type": "r92-test", "platform": "kalshi",
			"title": "H", "price": 0.50, "contracts": 20.0, "opened": time.Now().Unix(),
			"model_cohort": currentMLCohort,
		}},
		"closed":   []any{},
		"lifetime": map[string]any{"net": 10.0, "closed": 3.0, "wins": 2.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	hdr := s.equityHeaderBlock(ctx)
	lines := strings.Split(strings.TrimSpace(hdr), "\n")
	if !strings.HasPrefix(lines[0], "⚪ Kalshi ") {
		t.Fatalf("header must open directly with Kalshi, got %q:\n%s", lines[0], hdr)
	}
	idx := func(sub string) int {
		for i, ln := range lines {
			if strings.Contains(ln, sub) {
				return i
			}
		}
		return -1
	}
	k, pu, cb, ml := idx("Kalshi "), idx("PolyUS "), idx("Combos "), idx("ML ")
	if k < 0 || pu < k || cb < pu || ml < cb {
		t.Fatalf("the four books must render in the operator's order (k=%d pu=%d cb=%d ml=%d):\n%s", k, pu, cb, ml, hdr)
	}
	if !strings.Contains(hdr, "⚪ ML +$0.00 · n/a/bet · n/a¢/u") {
		t.Fatalf("lifetime ML data without a close this process must not leak into the run line:\n%s", hdr)
	}
	if !strings.Contains(hdr, "🅾️1©️0") {
		t.Fatalf("the ML line must show one process-epoch open and zero process-epoch closes:\n%s", hdr)
	}
	if strings.Contains(hdr, "mini: off") {
		t.Fatalf("an inactive mini must be silent in the compact briefing:\n%s", hdr)
	}
	if !strings.Contains(hdr, "🚀 Portfolio systems · 0 positive authenticated exchange-profit routes · avg n/a") {
		t.Fatalf("the current active-system line must render (empty verdict cache reads zero):\n%s", hdr)
	}
	if strings.Contains(hdr, "$1000") || strings.Contains(hdr, " open") || strings.Contains(hdr, "lifetime net") {
		t.Fatalf("compact book lines must omit bank/open/roster wording:\n%s", hdr)
	}
}

func TestR133BriefingShowsMiniOnlyWhenOn(t *testing.T) {
	s := testServer(t)
	s.miniProbe = func() bool { return true }
	if hdr := s.equityHeaderBlock(context.Background()); !strings.Contains(hdr, "🟢 mini: on") {
		t.Fatalf("active mini state must remain visible:\n%s", hdr)
	}
}

// TestR92ResetThenHeaderShowsBankroll — the operator's literal ask, end to end: after a reset the
// header's ML book equity equals its fresh bankroll and profit reads $0 everywhere that serves it.
func TestR92ResetThenHeaderShowsBankroll(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	writeBookJSON(t, dir, "ml_paper.json", map[string]any{
		"bank0": 474.96,
		"open": []any{map[string]any{
			"ticker": "T-O", "side": "YES", "signal_type": "r92-test", "platform": "kalshi",
			"title": "O", "price": 0.30, "contracts": 10.0,
		}},
		"closed":   []any{},
		"lifetime": map[string]any{"net": -41.03, "closed": 156.0, "wins": 89.0, "net_base": 0.0, "bank_ver": 3.0},
		"stats":    map[string]any{"net": -41.03, "equity": 433.93},
	})
	_ = s.equityHeaderBlock(ctx)
	if _, busy := s.doPaperReset(ctx); busy {
		t.Fatal("unexpected lock busy")
	}
	want := s.bookBankUSD(vbML) // R127: $1,000 — the fixed ML book bank is the fresh-epoch anchor
	hdr := s.equityHeaderBlock(ctx)
	// The briefing is now process-epoch closed P&L only; lifetime history remains in the API/book.
	if !strings.Contains(hdr, "⚪ ML +$0.00 · n/a/bet · n/a¢/u") {
		t.Fatalf("post-reset run line must stay flat until a position closes this process:\n%s", hdr)
	}
	// the API/UI view agrees (liveMarkBook is what /api/ml serves):
	pf, _ := s.liveMarkBook(ctx, dir, "ml_paper.json").(map[string]any)
	st, _ := pf["stats"].(map[string]any)
	if n, _ := st["net"].(float64); n != 0 {
		t.Fatalf("served net = %v, want 0 post-reset", st["net"])
	}
	if eq, _ := st["equity"].(float64); math.Abs(eq-want) > 0.005 {
		t.Fatalf("served equity = %v, want %v (== fresh bankroll)", st["equity"], want)
	}
}
