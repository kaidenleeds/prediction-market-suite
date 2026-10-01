package server

// R95 tests — operator mockup: the Telegram briefing header is ONE fixed list (exact order,
// SIGNED SESSION-DELTA dollars in parens — R94's total-equity parens rejected — zeros shown,
// "mini:" state line last) and the words "since start" never render in any Telegram text again
// (operator: "its still saying x since start remove that shit" — the numbers stay, the label goes).

import (
	"context"
	"strings"
	"testing"
)

// TestR95BriefingOpensWithMockupHeader — R144: BriefingText opens with the stable animal build
// name, then the compact four-book process-epoch block in order.
func TestR95BriefingOpensWithMockupHeader(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	txt := s.BriefingText(ctx)
	if strings.Contains(txt, "Parlays") {
		t.Fatalf("R101: archived flat parlay book must not render anywhere in the briefing:\n%s", txt)
	}
	lines := strings.Split(txt, "\n")
	if !strings.HasPrefix(lines[0], "🧬 build ") {
		t.Fatalf("briefing must open with the animal build name, got %q\nfull:\n%s", lines[0], txt)
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
	if k != 1 || pu < k || cb < pu || ml < cb {
		t.Fatalf("the four books must open the briefing in order (k=%d pu=%d cb=%d ml=%d):\n%s", k, pu, cb, ml, txt)
	}
	if systems := idx("Portfolio systems ·"); systems < ml || strings.Contains(txt, "mini: off") {
		t.Fatalf("the active portfolio-system summary must close the header and inactive mini must be silent (systems=%d ml=%d):\n%s", systems, ml, txt)
	}
}

func TestR144DedicatedComboBriefingOpensWithAnimalBuild(t *testing.T) {
	s := testServer(t)
	nc := *s.cfg()
	nc.Auto.ParlayEnabled = true
	s.cfgP.Store(&nc)
	txt := s.ParlayBriefingText(context.Background())
	if lines := strings.Split(txt, "\n"); len(lines) < 2 || !strings.HasPrefix(lines[0], "🧬 build ") ||
		!strings.HasPrefix(lines[1], "🎰 Combos · 2-6 legs") {
		t.Fatalf("dedicated Combo briefing must open build then 2-6L scope:\n%s", txt)
	}
}

// TestR95NoSinceStartWording — the phrase is banned from every Telegram/ntfy text; the numbers
// (which ARE measured since session start) stay.
func TestR95NoSinceStartWording(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	_ = s.BriefingText(ctx) // arm every baseline path first
	for _, txt := range []string{s.BriefingText(ctx), s.ParlayBriefingText(ctx)} {
		low := strings.ToLower(txt)
		if strings.Contains(low, "since start") || strings.Contains(low, "since session start") {
			t.Fatalf("telegram text still says 'since start':\n%s", txt)
		}
	}
}

// TestR95PortfolioSumsBookDeltas — R127 REVISION: the Portfolio sum line is GONE (the four-book
// header shows each book's own all-time truth; there is no cross-book session sum to game). The
// surviving pin: the Shadow CONTROL book must not surface in the four-book header at all — its
// numbers can never paint a book line green (the R97 bug-3 class, structurally closed).
func TestR95PortfolioSumsBookDeltas(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	writeBookJSON(t, s.cfg().DataDir, "ml_shadow.json", map[string]any{
		"bank0": 800.0, "open": []any{}, "closed": []any{},
		"lifetime": map[string]any{"net": 337.85, "closed": 500.0, "wins": 300.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 1000.0, "open": []any{}, "closed": []any{},
		"lifetime": map[string]any{"net": -5.0, "closed": 2.0, "wins": 0.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	hdr := s.equityHeaderBlock(ctx)
	if strings.Contains(hdr, "Shadow") || strings.Contains(hdr, "Portfolio $") {
		t.Fatalf("the header must carry neither a Shadow nor a cross-book Portfolio money line anymore:\n%s", hdr)
	}
	if !strings.Contains(hdr, "⚪ ML +$0.00 · n/a/bet · n/a¢/u") {
		t.Fatalf("lifetime ML/Shadow history must not leak into this-process closed P&L:\n%s", hdr)
	}
}
