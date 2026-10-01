package server

// R72-A tests: the deterministic build-name mapping (item 2) and the adaptive price-band
// aggregation (item 6). Pure functions — no store, no feeds.

import (
	"strings"
	"testing"
)

// ── buildNameOf: sha → color+animal must be stable, well-formed and list-bound ──────────────────

func TestBuildNameDeterministic(t *testing.T) {
	inputs := []string{"dev", "61a398e@2026-07-03T22:11", "66c9c42@2026-07-01T09:00", "abc1234@x", ""}
	for _, in := range inputs {
		n1, c1 := buildNameOf(in)
		for i := 0; i < 5; i++ { // same input, same process → identical every call
			n2, c2 := buildNameOf(in)
			if n1 != n2 || c1 != c2 {
				t.Fatalf("buildNameOf(%q) unstable: %q/%q vs %q/%q", in, n1, c1, n2, c2)
			}
		}
		// name must be exactly color+animal with both parts from the fixed lists
		if !strings.HasPrefix(n1, c1) {
			t.Fatalf("buildNameOf(%q) = %q — does not start with its color %q", in, n1, c1)
		}
		animal := strings.TrimPrefix(n1, c1)
		colorOK, animalOK := false, false
		for _, c := range buildNameColors {
			if c == c1 {
				colorOK = true
			}
		}
		for _, a := range buildNameAnimals {
			if a == animal {
				animalOK = true
			}
		}
		if !colorOK || animalOK == false {
			t.Fatalf("buildNameOf(%q) = %q (color %q, animal %q) — parts must come from the fixed lists", in, n1, c1, animal)
		}
	}
	// the two stamps of different builds should (for these known inputs) differ — the chip's whole point
	a, _ := buildNameOf("61a398e@2026-07-03T22:11")
	b, _ := buildNameOf("66c9c42@2026-07-01T09:00")
	if a == b {
		t.Fatalf("known-distinct build stamps mapped to the same name %q — hash mixing broken?", a)
	}
}

func TestBuildNameStableAcrossRebuildTime(t *testing.T) {
	a, _ := buildNameOf("722579a@260714-1442")
	b, _ := buildNameOf("722579a@260714-1517")
	if a != b {
		t.Fatalf("same source rebuilt at a new time changed animal: %q vs %q", a, b)
	}
}

func TestResolvedBuildNameUsesCleanCommitAnimal(t *testing.T) {
	for _, version := range []string{"722579a@260714-1442", "722579a@260714-1517"} {
		name, color := resolvedBuildName(version, "limecobra")
		if name != "limecobra" || color != "lime" {
			t.Fatalf("resolvedBuildName(%q, limecobra) = %q/%q", version, name, color)
		}
	}
}

func TestResolvedBuildNameDirtyNeverMasqueradesAsCommit(t *testing.T) {
	versions := []string{
		"722579a-dirty.0123456789ab@260714-1517",
		"722579a-dirty.0123456789ab@260714-1601",
	}
	var first string
	for _, version := range versions {
		name, _ := resolvedBuildName(version, "limecobra")
		if name == "limecobra" {
			t.Fatalf("dirty build %q inherited the clean commit animal", version)
		}
		if first == "" {
			first = name
		} else if name != first {
			t.Fatalf("same dirty source fingerprint changed across rebuild times: %q vs %q", first, name)
		}
	}
}

func TestResolvedBuildNameRejectsInvalidCommitPrefix(t *testing.T) {
	want, wantColor := buildNameOf("722579a@260714-1517")
	got, gotColor := resolvedBuildName("722579a@260714-1517", "R144:")
	if got != want || gotColor != wantColor {
		t.Fatalf("invalid commit prefix became release name: got %q/%q want %q/%q", got, gotColor, want, wantColor)
	}
}

// ── adaptiveBands: 5¢ default · 3¢ split at n≥1000 · ≤10¢ merge at n<100 · full coverage ────────

func bandsContiguous(t *testing.T, bands [][2]int) {
	t.Helper()
	if len(bands) == 0 || bands[0][0] != 0 || bands[len(bands)-1][1] != 100 {
		t.Fatalf("bands must cover (0,100): %v", bands)
	}
	for i := 1; i < len(bands); i++ {
		if bands[i][0] != bands[i-1][1] {
			t.Fatalf("gap/overlap at %d: %v", i, bands)
		}
	}
	for _, b := range bands {
		if w := b[1] - b[0]; w < 2 || w > 10 {
			t.Fatalf("band width %d outside [2,10]: %v (all %v)", w, b, bands)
		}
	}
}

func TestAdaptiveBandsSparseMergesTo10c(t *testing.T) {
	var cnt [100]int // all-zero: every 5¢ cell is sparse → pairs merge at the 10¢ ceiling
	bands := adaptiveBands(&cnt)
	bandsContiguous(t, bands)
	if len(bands) != 10 {
		t.Fatalf("empty histogram should merge to ten 10¢ bands, got %d: %v", len(bands), bands)
	}
	for _, b := range bands {
		if b[1]-b[0] != 10 {
			t.Fatalf("expected uniform 10¢ bands, got %v", bands)
		}
	}
}

func TestAdaptiveBandsDenseSplitsTo3c(t *testing.T) {
	var cnt [100]int
	for c := 20; c < 30; c++ {
		cnt[c] = 500 // cells [20,25) and [25,30) each hold 2500 ≥ 1000 → one dense run, 3¢ recut
	}
	for c := 0; c < 100; c++ {
		if cnt[c] == 0 {
			cnt[c] = 30 // every 5¢ cell holds 150 ≥ 100 elsewhere → no merging outside the run
		}
	}
	bands := adaptiveBands(&cnt)
	bandsContiguous(t, bands)
	want := [][2]int{{20, 23}, {23, 26}, {26, 29}} // 3¢ steps inside the dense run ({29,30} tail folds → last piece {26,30})
	_ = want
	got3 := 0
	for _, b := range bands {
		if b[0] >= 20 && b[1] <= 30 && b[1]-b[0] <= 4 {
			got3++
		}
		if b[0] < 20 && b[1] > 20 || b[0] < 30 && b[1] > 30 {
			t.Fatalf("dense run [20,30) must not blur across its edges: %v", bands)
		}
	}
	if got3 < 3 {
		t.Fatalf("dense run [20,30) should recut into ≥3 fine bands, got %v", bands)
	}
	if lbl := bandLabel(20, 23); lbl != "20–23¢" {
		t.Fatalf("bandLabel(20,23) = %q", lbl)
	}
	if lbl := bandLabel(0, 10); lbl != "<10¢" {
		t.Fatalf("bandLabel(0,10) = %q", lbl)
	}
	if lbl := bandLabel(90, 100); lbl != "90¢+" {
		t.Fatalf("bandLabel(90,100) = %q", lbl)
	}
}

func TestAdaptiveBandsDefault5c(t *testing.T) {
	var cnt [100]int
	for c := 0; c < 100; c++ {
		cnt[c] = 40 // every 5¢ cell holds 200: ≥100 (no merge) and <1000 (no split) → pure 5¢ grid
	}
	bands := adaptiveBands(&cnt)
	bandsContiguous(t, bands)
	if len(bands) != 20 {
		t.Fatalf("mid-density histogram should stay on the 5¢ default, got %d: %v", len(bands), bands)
	}
	for _, b := range bands {
		if b[1]-b[0] != 5 {
			t.Fatalf("expected uniform 5¢ bands, got %v", bands)
		}
	}
}

func TestBuildNameListSizes(t *testing.T) {
	if len(buildNameColors) != 16 || len(buildNameAnimals) != 24 {
		t.Fatalf("list sizes changed (%d colors, %d animals) — changes REMAP every deployed build name; do it deliberately",
			len(buildNameColors), len(buildNameAnimals))
	}
	seen := map[string]bool{}
	for _, c := range buildNameColors {
		if seen[c] {
			t.Fatalf("duplicate color %q", c)
		}
		seen[c] = true
	}
	for _, a := range buildNameAnimals {
		if seen[a] {
			t.Fatalf("duplicate animal %q", a)
		}
		seen[a] = true
	}
}

// ── R72-B signalKindOf: the 5-value kind vocabulary must be stable — the sidecar one-hots it ────

func TestSignalKindOf(t *testing.T) {
	cases := []struct {
		platform, ticker, title, want string
	}{
		// Kalshi series tickers are authoritative (probed active 2026-07-04):
		{"kalshi", "KXMLBGAME-25JUL0419BOSSEA-BOS", "Boston vs Seattle winner", "winner"},
		{"kalshi", "KXMLBSPREAD-25JUL0419BOSSEA-BOS1.5", "Boston wins by 2+", "spread"},
		{"kalshi", "KXMLBTOTAL-25JUL0419BOSSEA-T8.5", "8.5 or more runs", "total"},
		{"kalshi", "KXMLBTEAMTOTAL-25JUL0419BOSSEA-BOS4.5", "Boston team total", "total"},
		{"kalshi", "KXWC1HTOTAL-26JUL07SUICOL-T1.5", "1H goals", "total"}, // 1st Half Total stays a total
		{"kalshi", "KXWCGOAL-26JUL07SUICOL-EMB", "Embolo scores a goal", "prop"},
		{"kalshi", "KXWCCORNERS-26JUL07SUICOL-T9.5", "corners", "prop"},
		{"kalshi", "KXWCSCORE-26JUL07SUICOL-21", "correct score 2-1", "prop"},
		{"kalshi", "KXMLBRFI-25JUL0419BOSSEA", "run scored in the 1st inning", "prop"},
		{"kalshi", "KXWCGAME-26JUL07SUICOL-TIE", "Switzerland vs Colombia", "winner"},
		// Crypto binaries/thresholds ARE the event outcome, not sub-bets:
		{"kalshi", "KXBTC15M-26JUL041715-U", "BTC up at 5:15pm?", "winner"},
		{"kalshi", "KXBTCD-26JUL0417-T62999.99", "BTC at $63k or above?", "winner"},
		// R73 full-universe breadth: Mentions / awards / round H2Hs are sub-bets → prop:
		{"kalshi", "KXWCMENTION-26JUL07-VAR", "Will VAR be mentioned during the broadcast?", "prop"},
		{"kalshi", "KXPGATOURH2H-26JUL06-SCHMCI", "Scheffler vs McIlroy — Round 4 head-to-head", "prop"},
		{"kalshi", "KXAWARDMVP-26-JUDGE", "Will Judge win the MVP award?", "prop"},
		{"polymarket", "0x777", "Will Trump mention Bitcoin in the speech?", "prop"},
		// Title evidence for venues without series tickers:
		{"polyus", "aec-mlb-pit-phi-2026-07-02", "Pirates @ Phillies", "winner"},
		{"polymarket", "0xabc", "Spread: France -2.5", "spread"},
		{"polymarket", "0xdef", "Will over 2.5 goals be scored?", "total"},
		{"polymarket", "0x123", "France vs Senegal — France wins", "winner"},
		// Blank input stays unknown (never guess a kind from nothing):
		{"polymarket", "", "", "unknown"},
	}
	for _, c := range cases {
		if got := signalKindOf(c.platform, c.ticker, c.title); got != c.want {
			t.Fatalf("signalKindOf(%q,%q,%q) = %q, want %q", c.platform, c.ticker, c.title, got, c.want)
		}
	}
}
