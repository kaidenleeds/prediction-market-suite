package server

// Naming regression locks (operator: "verify friendly naming is working correctly").
// These tests pin the ntfy naming convention (emoji + readable market titles) against the three
// regressions it has already had:
//
//	R47a  "🪙 Crypto · 🪙 Crypto · …" — friendlyName applied twice through layered paths
//	      (idempotency guard: HasPrefix check).            → TestFriendlyNameIdempotent
//	R47b  tennis combos labeled "🎮 Esports" — KXMVE… tickers substring-matched the gaming
//	      rule; the KXMVE → "🎟 Combo" case must stay FIRST. → TestLegSportComboFirst
//	R49   pre-registry combo positions had no per-leg emojis — venueComboTitle /
//	      legEmojiFromName recover them from the venue title. → TestVenueComboTitle / TestLegEmojiFromName
//
// No network, no sleeps: Server values are constructed directly with a seeded kmkts cache
// (legEmojiFromName / venueComboTitle take metaMu themselves — same convention as kmkt — so a
// zero-value mutex is all they need). The name-fallback ring is package-level state and is reset
// at the start of the test that inspects it, so ordering with other tests doesn't matter.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// namingTestServer returns a minimal *Server whose kmkts cache (map[ticker]kalshi.Market, the
// exact live types) holds one tennis market — enough for the token-overlap match in
// legEmojiFromName to resolve "Tommy Paul" to a KXATP ticker and thus the 🎾 emoji.
func namingTestServer() *Server {
	return &Server{kmkts: map[string]kalshi.Market{
		"KXATPMATCH-25JUL04-PAUL": {Ticker: "KXATPMATCH-25JUL04-PAUL", Title: "Tommy Paul wins the match"},
	}}
}

// TestFriendlyNameIdempotent — R47a: applying friendlyName to its own output must be a no-op
// (the "🪙 Crypto · 🪙 Crypto · ETH…" double-prefix regression).
func TestFriendlyNameIdempotent(t *testing.T) {
	cases := []struct {
		name, ticker, title, outcome string
		wantLabel                    string // legSport label expected in the prefix; "" = unmapped → raw fallback
	}{
		{"crypto", "KXBTCD-25JUL03-T62000", "Bitcoin price today at 2pm EDT?", "", "🪙 Crypto"},
		{"crypto with outcome", "KXBTCD-25JUL03-T62000", "Bitcoin price today at 2pm EDT?", "$62,000 or above", "🪙 Crypto"},
		{"tennis", "KXATPMATCH-25JUL04-PAUL", "Tommy Paul wins the match?", "", "🎾 Tennis"},
		// R63 2a: weather is MAPPED now (🌡) — idempotency must hold through the new prefix too.
		{"weather mapped (R63 2a)", "KXHIGHNY-25JUL03-B90", "Highest temperature in NYC today?", "", "🌡 Weather"},
		// A ticker with no rule at all still falls back raw + idempotent.
		{"unmapped falls back raw", "KXZQXW-25JUL03-B90", "Some unmapped series?", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			once := friendlyName(c.ticker, c.title, c.outcome)
			twice := friendlyName(c.ticker, once, c.outcome)
			if twice != once {
				t.Fatalf("friendlyName is not idempotent:\n once=%q\ntwice=%q", once, twice)
			}
			if c.wantLabel != "" {
				if !strings.HasPrefix(once, c.wantLabel+" · ") {
					t.Fatalf("want prefix %q, got %q", c.wantLabel+" · ", once)
				}
				if n := strings.Count(twice, c.wantLabel); n != 1 {
					t.Fatalf("label %q must appear exactly once, appears %d times in %q", c.wantLabel, n, twice)
				}
			} else if once != c.title {
				t.Fatalf("unmapped ticker must fall back to the raw title, got %q", once)
			}
			if c.outcome != "" && !strings.Contains(strings.ToLower(twice), strings.ToLower(c.outcome)) {
				t.Fatalf("outcome %q lost from %q", c.outcome, twice)
			}
		})
	}
}

// TestLegSportComboFirst — R47b: a KXMVE combined-market ticker CONTAINS the esports trigger
// substring ("mvESPORTsmultigame"), so the "🎟 Combo" case must stay ahead of the gaming rule.
func TestLegSportComboFirst(t *testing.T) {
	for _, tk := range []string{
		"KXMVESPORTSMULTIGAME-26JUL03-TPAUL61700",
		"KXMVE-26JUL03-ABC",
	} {
		if got := legSport(tk); got != "🎟 Combo" {
			t.Fatalf("legSport(%q) = %q, want \"🎟 Combo\" (regression: combos labeled %q)", tk, got, "🎮 Esports")
		}
	}
	// Prove the test still exercises the ordering hazard: the realistic ticker really does carry
	// the esports substring, so if the KXMVE case ever moves below the gaming rule this breaks.
	if !strings.Contains(strings.ToUpper("KXMVESPORTSMULTIGAME-26JUL03-TPAUL61700"), "ESPORT") {
		t.Fatal("test input no longer contains the ESPORT substring — pick a ticker that does")
	}
}

// TestLegSportSeries locks the series-prefix → emoji-label table as currently implemented.
// R63 2a: weather (🌡), politics (🏛), econ (📈), entertainment/collectibles (🏆) and FIBA (🏀)
// are mapped now — the old ""-fallback locks for weather/FIBA were updated deliberately.
func TestLegSportSeries(t *testing.T) {
	cases := []struct{ ticker, want string }{
		{"KXATPMATCH-25JUL04-PAUL", "🎾 Tennis"},
		{"KXWTAMATCH-25JUL04-GAUFF", "🎾 Tennis"},
		{"KXBTCD-25JUL03-T62000", "🪙 Crypto"},
		{"KXETH-25JUL03-T2500", "🪙 Crypto"},
		{"KXCS2GAME-25JUL03-NAVI", "🎮 Esports"},
		{"KXNBAFINALS-26-OKC", "🏀 Basketball"},
		{"KXMLBGAME-25JUL03-NYY", "⚾ Baseball"},
		{"KXWORLDCUPWINNER-26", "⚽ World Cup"},
		{"astatc-fwc-esp-bel-2026-07-10-sot-fwclamyam-gte2", "⚽ World Cup"}, // PolyUS World Cup player prop
		{"fifwc-par-fra-2026-07-04-spread-away-1pt5", "⚽ World Cup"},        // Poly-int/legacy FWC family
		{"tec-fifa-wc-2026-07-19-gba-fwcmikoya", "⚽ World Cup"},             // PolyUS World Cup award family
		{"tec-f-wc-2026-07-19-topscorer-alerlo", "⚽ World Cup"},
		{"tec-mls-winner-2026-11-07-atl", "⚽ Soccer"},
		{"tec-uefa-bdor-2026-10-26-w-decric", "⚽ Soccer"},
		{"tec-cycling-letour-2026-07-26-w-mikteu", "🚴 Cycling"},
		{"tec-fide-wcc-2026-12-17-w-andesi", "♟️ Chess"},
		{"KXNHLGAME-26JAN01-BOS", "🏒 Hockey"},
		{"aec-mlb-nyy-bos-2025-07-03", "⚾ Baseball"}, // Poly US slug path
		// R63 2a — non-sport categories:
		{"KXHIGHNY-25JUL03-B90", "🌡 Weather"},      // daily-high ladder (was "")
		{"KXHIGHTNOLA-26JUL04-B93.5", "🌡 Weather"}, // KXHIGHT* station variant
		{"KXLOWCHI-26JAN10-B10", "🌡 Weather"},      // daily-low ladder
		{"KXFIBA-26-USA", "🏀 Basketball"},          // FIBA mapped (was "")
		{"KXCAELECTION-26-DEM", "🏛 Politics"},      // election series
		{"KXSENATENC-26-R", "🏛 Politics"},          // senate race
		{"KXPRES-28-JD", "🏛 Politics"},             // presidential
		{"KXGOVTX-26-ABB", "🏛 Politics"},           // governor
		{"KXCPI-26JUL-B0.3", "📈 Econ"},             // CPI print
		{"KXFEDDECISION-26SEP-CUT", "📈 Econ"},      // Fed decision
		{"KXGDP-26Q2-B2.0", "📈 Econ"},              // GDP print
		{"KXJOBSREPORT-26JUL-B150", "📈 Econ"},      // jobs report
		{"KXESPYS-26-BESTTEAM", "🏆 Entertainment"}, // awards show
		{"KXLOTSALE-26-CARD1", "🏆 Entertainment"},  // collectible lot-sale series
	}
	for _, c := range cases {
		if got := legSport(c.ticker); got != c.want {
			t.Errorf("legSport(%q) = %q, want %q", c.ticker, got, c.want)
		}
	}
}

func TestR133PolyUSFutureNameKeepsTheSelection(t *testing.T) {
	m := polymarketus.Market{Question: "Tour de France Winner", Title: "Mike Teunissen"}
	if got := pusQuestionSelectionName(m); got != "Tour de France Winner — Mike Teunissen" {
		t.Fatalf("future label=%q", got)
	}
	m = polymarketus.Market{Question: "Will Mike Teunissen win?", Title: "Mike Teunissen"}
	if got := pusQuestionSelectionName(m); got != m.Question {
		t.Fatalf("selection already in question should not duplicate: %q", got)
	}
}

// TestLegEmojiFromName — R49: per-leg emoji recovery from a bare leg NAME (no ticker).
func TestLegEmojiFromName(t *testing.T) {
	s := namingTestServer()
	cases := []struct{ name, in, want string }{
		{"price text ($)", "$61,700 or above", "🪙"},
		{"price text (word)", "eth price at 2pm", "🪙"},
		{"tennis via 2-token overlap", "Tommy Paul", "🎾"},
		{"tennis via single distinctive token", "Tommy", "🎾"},
		{"unknown name", "zzz unknown name", "•"},
		{"empty", "", "•"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := s.legEmojiFromName(c.in); got != c.want {
				t.Fatalf("legEmojiFromName(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
	// A best match that is itself a combo market must NOT yield the 🎟 emoji for a leg
	// (the !HasPrefix(sp, "🎟") guard): fall to "•" instead.
	sc := &Server{kmkts: map[string]kalshi.Market{
		"KXMVEX-1": {Ticker: "KXMVEX-1", Title: "Quantum Wager Special"},
	}}
	if got := sc.legEmojiFromName("Quantum Wager"); got != "•" {
		t.Fatalf("combo-market match must be excluded from leg emojis, got %q", got)
	}
}

// TestVenueComboTitle — R49: parse the venue's combined-market title into per-leg emoji form.
// The canonical input (from the R49 comment itself) carries a comma INSIDE the price
// ("$61,700"), which must survive as one leg — not split into "$61" + an orphan "700 or above".
func TestVenueComboTitle(t *testing.T) {
	s := namingTestServer()
	const want = "🎟 Combo · 🎾 Tommy Paul YES + 🪙 $61,700 or above NO"
	for _, in := range []string{
		"yes Tommy Paul,no $61,700 or above",  // venue format (no space after leg comma)
		"yes Tommy Paul, no $61,700 or above", // spaced variant
	} {
		got, ok := s.venueComboTitle(in)
		if !ok {
			t.Fatalf("venueComboTitle(%q) not ok", in)
		}
		if got != want {
			t.Fatalf("venueComboTitle(%q)\n got %q\nwant %q", in, got, want)
		}
		for _, frag := range []string{"🎟 Combo", "🎾", "Tommy Paul", "YES", "🪙", "$61,700 or above", "NO"} {
			if !strings.Contains(got, frag) {
				t.Fatalf("venueComboTitle(%q) = %q missing %q", in, got, frag)
			}
		}
	}
	// Single leg with a side prefix: still rendered (one-leg combo), no panic.
	if got, ok := s.venueComboTitle("yes Tommy Paul"); !ok || got != "🎟 Combo · 🎾 Tommy Paul YES" {
		t.Fatalf("single-leg: got %q (ok=%v)", got, ok)
	}
	// Not a leg list (no comma, no yes/no prefix) and empty input: ok=false, caller falls back.
	if got, ok := s.venueComboTitle("Tommy Paul"); ok || got != "" {
		t.Fatalf("bare title must not parse as combo: got %q (ok=%v)", got, ok)
	}
	if got, ok := s.venueComboTitle(""); ok || got != "" {
		t.Fatalf("empty title must not parse as combo: got %q (ok=%v)", got, ok)
	}
}

// TestNameFallbackRingDedup locks the fallback-observability semantics: the RING dedups per
// ticker (one row per ticker, capped at 200), and — R76 (auditor nag: the per-render count hit
// 4.3M of noise) — the COUNTER (nameFallbackN) now counts UNIQUE tickers only: friendlyName
// increments it exactly when the ring admits a new ticker, never on re-renders.
func TestNameFallbackRingDedup(t *testing.T) {
	// Package-level state — reset for hermeticity (other tests route through friendlyName).
	nameFbMu.Lock()
	nameFbSeen = map[string]bool{}
	nameFbRing = nil
	nameFbMu.Unlock()
	nameFallbackN.Store(0)

	ring := func() []map[string]string {
		nameFbMu.Lock()
		defer nameFbMu.Unlock()
		return append([]map[string]string(nil), nameFbRing...)
	}

	nameFallbackLog("KXZZZFAKE-1", "first title")
	if r := ring(); len(r) != 1 || r[0]["ticker"] != "KXZZZFAKE-1" || r[0]["title"] != "first title" {
		t.Fatalf("after first log: %v", r)
	}
	nameFallbackLog("KXZZZFAKE-1", "second title") // same ticker → deduped, first title kept
	if r := ring(); len(r) != 1 || r[0]["title"] != "first title" {
		t.Fatalf("dedup by ticker failed: %v", r)
	}
	nameFallbackLog("KXZZZFAKE-2", "other title")
	if r := ring(); len(r) != 2 || r[1]["ticker"] != "KXZZZFAKE-2" {
		t.Fatalf("distinct ticker must append: %v", r)
	}

	// Counter (R76): UNIQUE tickers only — a re-render of an already-seen ticker must not move it.
	base := nameFallbackN.Load()
	_ = friendlyName("KXZZZFAKE-1", "KXZZZFAKE-1", "") // already in the ring → +0
	_ = friendlyName("KXZZZFAKE-1", "KXZZZFAKE-1", "") // still +0
	if got := nameFallbackN.Load() - base; got != 0 {
		t.Fatalf("counter must ignore re-renders of a seen ticker: grew %d, want 0", got)
	}
	_ = friendlyName("KXZZZFAKE-3", "KXZZZFAKE-3", "") // brand-new ticker → +1
	if got := nameFallbackN.Load() - base; got != 1 {
		t.Fatalf("counter must count a NEW fallback ticker once: grew %d, want 1", got)
	}
	if r := ring(); len(r) != 3 {
		t.Fatalf("ring must stay deduped at 3 rows, got %d", len(r))
	}

	// Cap: the ring keeps only the newest 200 rows (FIFO trim).
	nameFbMu.Lock()
	nameFbSeen = map[string]bool{}
	nameFbRing = nil
	nameFbMu.Unlock()
	for i := 0; i < 205; i++ {
		nameFallbackLog(fmt.Sprintf("KXCAP-%03d", i), "cap")
	}
	r := ring()
	if len(r) != 200 {
		t.Fatalf("ring cap: len %d, want 200", len(r))
	}
	if r[0]["ticker"] != "KXCAP-005" || r[199]["ticker"] != "KXCAP-204" {
		t.Fatalf("ring must trim oldest: first %q last %q", r[0]["ticker"], r[199]["ticker"])
	}
}

func TestR133NaturalLanguageTitleIsAlreadyFriendly(t *testing.T) {
	t.Cleanup(func() {
		nameFbMu.Lock()
		nameFbSeen = map[string]bool{}
		nameMappedSeen = map[string]bool{}
		nameFbRing = nil
		nameFbMu.Unlock()
		nameFallbackN.Store(0)
	})
	nameFbMu.Lock()
	nameFbSeen = map[string]bool{}
	nameMappedSeen = map[string]bool{}
	nameFbRing = nil
	nameFbMu.Unlock()
	nameFallbackN.Store(0)

	got := friendlyName("openai-ipo-by", "Will OpenAI IPO by December 31 2026?", "")
	if got != "Will OpenAI IPO by December 31 2026?" {
		t.Fatalf("natural title changed: %q", got)
	}
	v := namingVitalityNow()
	if v.Mapped != 1 || v.Fallback != 0 || nameFallbackN.Load() != 0 {
		t.Fatalf("natural title must count mapped, not raw fallback: %+v counter=%d", v, nameFallbackN.Load())
	}
	_ = friendlyName("KXRAWCODE-1", "KXRAWCODE-1", "")
	v = namingVitalityNow()
	if v.Fallback != 1 || nameFallbackN.Load() != 1 {
		t.Fatalf("actual raw identifier must remain observable: %+v counter=%d", v, nameFallbackN.Load())
	}
}
