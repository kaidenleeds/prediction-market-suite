// r99_test.go — R99 regression pins (auditor r24/r25 URGENT paper-wedge + both-sides redesign).
//
//  1. TestR99ResetReanchorsEqPeak (bug 180): resetMLBook must stamp eq_peak = the fresh bank.
//     A carried-over epoch-1 peak (564 vs bank0 400) read as a permanent ~29% pseudo-drawdown and
//     the sidecar's D5 brake half-sized every lot of the new epoch — the sidecar itself can only
//     ratchet the peak UP (live_ml.py: peak = max(file, equity)), so the reset is the one place
//     the peak can legitimately come down.
//  2. TestR99MLBookHedgeBlock (both-sides redesign): the cross-book guard autoPlace consults must
//     block the OPPOSITE side of a market the sidecar's ML book is riding, allow the SAME side
//     (doubling is a sizing question, not a hedge), and stay silent on unheld markets.
//  3. TestR99LegacyAllocKeepsShadowRetired (bug 184): flipping to legacy bankrolls must NOT delete
//     the ml_alloc.json handshake while the shadow book is retired — an absent file reads as
//     shadow_enabled=true on the sidecar and silently un-retires the frozen control record.
package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestR99ResetReanchorsEqPeak(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dir := s.cfg().DataDir
	writeBookJSON(t, dir, "ml_paper.json", map[string]any{
		"bank0":    400.0,
		"eq_peak":  564.06, // epoch-1 high-water mark — must NOT survive the reset
		"open":     []any{},
		"closed":   []any{},
		"lifetime": map[string]any{"net": 164.06, "closed": 68.0, "wins": 46.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	if n := s.resetMLBook(ctx, "ml_paper.json"); n != 0 {
		t.Fatalf("resetMLBook closed %d, want 0 (no opens)", n)
	}
	pf := readBookFileRaw(t, dir, "ml_paper.json")
	gf := func(m map[string]any, k string) float64 { v, _ := m[k].(float64); return v }
	bank := gf(pf, "bank0")
	if bank <= 0 {
		t.Fatalf("post-reset bank0 = %v, want > 0", pf["bank0"])
	}
	if got := gf(pf, "eq_peak"); got != bank {
		t.Fatalf("post-reset eq_peak = %v, want %v (the fresh bank — bug 180: a stale peak half-sizes the whole epoch)", got, bank)
	}
}

func TestR99MLBookHedgeBlock(t *testing.T) {
	s := testServer(t)
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 400.0, "closed": []any{},
		"open": []any{map[string]any{"ticker": "KXNFLGAME-X", "side": "yes", "platform": "kalshi",
			"price": 0.42, "contracts": 20.0}},
		"lifetime": map[string]any{"net": 0.0, "closed": 0.0, "wins": 0.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	held := s.mlBookOpenSides()
	if got := held["kalshi|KXNFLGAME-X"]; got != "YES" {
		t.Fatalf("mlBookOpenSides = %q, want YES (side upper-normalized)", got)
	}
	// The autoPlace guard condition, verbatim: opposite side blocks, same side and unheld don't.
	block := func(platform, ticker, side string) bool {
		h := s.mlBookOpenSides()[platform+"|"+ticker]
		return h != "" && !strings.EqualFold(h, side)
	}
	if !block("kalshi", "KXNFLGAME-X", "NO") {
		t.Fatalf("opposite side of an ML-book lot must be blocked (cross-book hedge)")
	}
	if block("kalshi", "KXNFLGAME-X", "yes") {
		t.Fatalf("same side must NOT be blocked (case-insensitive)")
	}
	if block("kalshi", "KXOTHER", "NO") {
		t.Fatalf("unheld market must not be blocked")
	}
}

func TestR99LegacyAllocKeepsShadowRetired(t *testing.T) {
	s := testServer(t)
	tcfg := *s.cfg()
	tcfg.Auto.ShadowBookRetired = true
	tcfg.Auto.PaperTotalStart = 0 // legacy flat-bankroll mode (allocFracs → active=false)
	s.cfgP.Store(&tcfg)
	path := filepath.Join(s.cfg().DataDir, "ml_alloc.json")
	if err := os.WriteFile(path, []byte(`{"ml_bank_usd":400,"shadow_enabled":false}`), 0o644); err != nil {
		t.Fatalf("seed handshake: %v", err)
	}
	s.writeMLAllocFile(context.Background())
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("legacy mode deleted ml_alloc.json while the shadow is retired (bug 184): %v", err)
	}
	var m map[string]any
	if jerr := json.Unmarshal(b, &m); jerr != nil {
		t.Fatalf("handshake unreadable: %v", jerr)
	}
	if v, ok := m["shadow_enabled"].(bool); !ok || v {
		t.Fatalf("legacy-mode handshake must carry shadow_enabled=false, got %v", m["shadow_enabled"])
	}
	if _, hasBank := m["ml_bank_usd"]; hasBank {
		t.Fatalf("legacy-mode handshake must NOT carry bank keys (sidecar keeps its legacy anchor)")
	}
}
