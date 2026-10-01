// r100_test.go — R100 regression pins (auditor r26 slice-5 findings 205–213 + DO-THIS 3/4/7).
//
//  1. Same-city side gate (bug 205): the matcher must never attach a signal to the WRONG team's
//     market on a shared city token (CHC/CHW, NYY/NYM, LAD/LAA class), including when only ONE
//     row of the game survives the snapshot filters.
//  2. Doubleheader anchor (bug 206): two games, same teams, same ET day — a start-time-carrying
//     ref anchors to the right game; a dateless/timeless ref REFUSES instead of letting snapshot
//     order pick (reason "ambiguous").
//  3. Type-class split (bug 207): spread ≠ total ≠ prop — see also polyusmatch_test.go.
//  4. Shared conflict guard (bug 211 + 212): every betting path is covered by ONE guard
//     (betConflictReason); auto-arb and gate are the two documented exemptions; UP/DOWN sides
//     normalize (a DOWN lot must not block BOTH sides). A source-census test walks the autoPlace
//     call sites in server.go so a NEW betting path cannot ship without joining this test.
//  5. kmkts cache (bug 208): existing keys refresh at cap; new keys evict instead of vanishing.
package server

import (
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

// chiSnapshot: one same-city game (Cubs vs White Sox), both rows listed, EventID-linked.
func chiSnapshot() []polyUSMarket {
	game := "Chicago Cubs vs Chicago White Sox"
	return []polyUSMarket{
		{League: "mlb", EventID: "ev-chi", Game: game, Team: "CHC", TeamName: "Chicago Cubs",
			Slug: "mlb-chc-chw-cubs", Yes: 0.55, Bid: 0.54, Ask: 0.56},
		{League: "mlb", EventID: "ev-chi", Game: game, Team: "CHW", TeamName: "Chicago White Sox",
			Slug: "mlb-chc-chw-sox", Yes: 0.45, Bid: 0.44, Ask: 0.46},
	}
}

func TestR100SameCitySideGate(t *testing.T) {
	const q = "Will the Chicago Cubs beat the Chicago White Sox?"
	s := &Server{polyUSMkts: chiSnapshot()}
	cases := []struct {
		side, wantSlug string // wantSlug "" = must refuse
	}{
		{"Chicago Cubs", "mlb-chc-chw-cubs"},
		{"Chicago White Sox", "mlb-chc-chw-sox"},
		{"Cubs", "mlb-chc-chw-cubs"},
		{"White Sox", "mlb-chc-chw-sox"},
		{"Chicago", ""}, // bare city fits BOTH teams — ambiguous, never a guess
	}
	for _, c := range cases {
		slug, _, _, ok := s.polyUSMatchForSide("arb", q, c.side)
		if c.wantSlug == "" {
			if ok {
				t.Fatalf("side %q must refuse (ambiguous), matched %q", c.side, slug)
			}
			continue
		}
		if !ok || slug != c.wantSlug {
			t.Fatalf("side %q = (%q, %v), want %q", c.side, slug, ok, c.wantSlug)
		}
	}

	// LONE-ROW wrong-side (the poisonous case: the sibling was liquidity-filtered out, so no
	// tie-break exists — the side's own tokens must carry the refusal): a White Sox side must not
	// ride into the only-listed CUBS row on the shared "Chicago".
	sLone := &Server{polyUSMkts: chiSnapshot()[:1]} // Cubs row only
	if slug, _, _, ok := sLone.polyUSMatchForSide("arb", q, "Chicago White Sox"); ok {
		t.Fatalf("White Sox side must not match the lone Cubs row, matched %q", slug)
	}
	if _, _, _, ok := sLone.polyUSMatchForSide("arb", q, "Chicago Cubs"); !ok {
		t.Fatal("Cubs side must still match the lone Cubs row")
	}

	// NYY/NYM + LAD/LAA class cases via the evidence core directly.
	for _, c := range []struct {
		side, target, opp string
		want              bool
	}{
		{"New York Yankees", "New York Yankees", "New York Mets", true},
		{"New York Yankees", "New York Mets", "New York Yankees", false}, // shared "york" is not evidence; "yankees" hits the opponent
		{"New York Mets", "New York Yankees", "New York Mets", false},
		{"Los Angeles Dodgers", "Los Angeles Angels", "Los Angeles Dodgers", false},
		{"Los Angeles Dodgers", "Los Angeles Dodgers", "Los Angeles Angels", true},
		{"New York Yankees", "NYY", "New York Mets", true},  // abbrev target: initials evidence, opponent-guarded
		{"New York Mets", "NYY", "New York Yankees", false}, // wrong team vs abbrev target
		{"CHC", "Chicago Cubs", "Chicago White Sox", false}, // city-subsequence fits both cities' word — refuse
		{"Cubs", "Chicago Cubs", "Chicago White Sox", true}, // distinct nickname
		{"White Sox", "Chicago Cubs", "Chicago White Sox", false},
	} {
		if got := sideEvidenceDistinct(c.side, c.target, c.opp); got != c.want {
			t.Fatalf("sideEvidenceDistinct(%q,%q,%q) = %v, want %v", c.side, c.target, c.opp, got, c.want)
		}
	}
}

func TestR100DoubleheaderAnchor(t *testing.T) {
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load America/New_York: %v", err)
	}
	// Two games, same teams, same day: G1 ≈ now+2h, G2 ≈ now+8h (both inside the ±1-day date
	// gate even across midnight). Built against the live clock so kalshiTickerStart's
	// plausibility gate passes (same convention as TestPolyUSMatchDateGuard).
	g1 := time.Now().In(et).Add(2 * time.Hour)
	g2 := g1.Add(6 * time.Hour)
	game := "Chicago Cubs vs Chicago White Sox"
	row := func(slug string, start time.Time) polyUSMarket {
		return polyUSMarket{League: "mlb", Game: game, Team: "CHC", TeamName: "Chicago Cubs",
			Slug: slug, Yes: 0.55, Bid: 0.54, Ask: 0.56, Start: start.UTC().Format(time.RFC3339)}
	}
	s := &Server{polyUSMkts: []polyUSMarket{row("mlb-chc-chw-g1", g1), row("mlb-chc-chw-g2", g2)}}
	const q, side = "Will the Chicago Cubs beat the Chicago White Sox?", "Chicago Cubs"

	tick := func(at time.Time) string { // Kalshi sports-style ticker embedding date+HHMM
		return "KXMLBGAME-" + strings.ToUpper(at.Format("06Jan021504")) + "CHCCHW-CHC"
	}
	if slug, _, _, ok := s.polyUSMatchForSide("arb", q, side, tick(g1)); !ok || slug != "mlb-chc-chw-g1" {
		t.Fatalf("game-1 ticker must anchor to game 1: (%q, %v)", slug, ok)
	}
	if slug, _, _, ok := s.polyUSMatchForSide("arb", q, side, tick(g2)); !ok || slug != "mlb-chc-chw-g2" {
		t.Fatalf("game-2 ticker must anchor to game 2: (%q, %v)", slug, ok)
	}
	// Date-only ref (slug, no time-of-day) → can't tell the games apart → REFUSE (was: snapshot
	// order silently picked one — bug 206).
	if slug, _, _, ok := s.polyUSMatchForSide("arb", q, side, "mlb-chc-chw-"+g1.Format("2006-01-02")); ok {
		t.Fatalf("timeless ref on a doubleheader must refuse, matched %q", slug)
	}
	// No ref at all → same refusal (two indistinguishable survivors).
	if slug, _, _, ok := s.polyUSMatchForSide("arb", q, side); ok {
		t.Fatalf("refless doubleheader must refuse, matched %q", slug)
	}
}

func TestR100TypeClassSplit(t *testing.T) {
	for _, c := range []struct{ text, ref, want string }{
		{"Yankees vs Red Sox: total runs O/U 8.5", "", "total"},
		{"New York Yankees over 4.5 runs vs Boston", "", "total"},
		{"Spread: New York Yankees -1.5 vs Red Sox", "", "spread"},
		{"Corners handicap: New York Yankees vs Boston", "", "prop"}, // first classified word wins
		{"New York Yankees to win by a set score", "", "prop"},
		{"Will the New York Yankees beat the Boston Red Sox?", "", "winner"},
		{"Yankees game", "kxmlbtotal-26jul06", "total"}, // ticker hint, split
		{"Yankees game", "mlb-nyy-spread-26jul06", "spread"},
		{"", "", ""},
	} {
		if got := matchTypeOf(c.text, c.ref); got != c.want {
			t.Fatalf("matchTypeOf(%q,%q) = %q, want %q", c.text, c.ref, got, c.want)
		}
	}
	// Venue Kind outranks text (matchTypeOfMkt): a winner-looking Game with Kind="total" is a total.
	m := polyUSMarket{Game: "Chicago Cubs vs Chicago White Sox", Kind: "total"}
	if got := matchTypeOfMkt(m); got != "total" {
		t.Fatalf("Kind must outrank text: got %q", got)
	}
	m.Kind = ""
	if got := matchTypeOfMkt(m); got != "winner" {
		t.Fatalf("kindless winner-looking row = %q, want winner", got)
	}
}

// r100GuardedSources: every source whose shared conflict behavior remains pinned. auto-ml is
// included because old positions can still settle and conflict checks remain defense in depth,
// even though R166 retired its synthetic Paper entry call site. The census below therefore
// compares only the sources that still call autoPlace.
var r100GuardedSources = []string{
	"auto-ml",
	"auto-cons-kalshi", "auto-cons-kflow", "auto-cons-poly", "auto-cons-x", "auto-cons-pflow",
	"auto-cons-pusflow", "auto-cons-pcrypto", "auto-cons-xmatch", "auto-cons-kcrypto",
	"auto-cons-pbridge", "auto-cons-favlong", "auto-cons-kthresh", "auto-cons-pmatch",
	"auto-cons-xvlag", "auto-cons-confluence",
}

func (s *Server) r100ResetMLOpenCache() {
	s.mlOpenMu.Lock()
	s.mlOpenSides = nil
	s.mlOpenAt = time.Time{}
	s.mlOpenMu.Unlock()
}

func TestR100HedgeGuardAllPaths(t *testing.T) {
	s := testServer(t)
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 400.0, "closed": []any{},
		"open": []any{
			map[string]any{"ticker": "KXR100HEDGE-A", "side": "yes", "platform": "kalshi", "price": 0.42, "contracts": 20.0},
			map[string]any{"ticker": "KXR100HEDGE-B", "side": "DOWN", "platform": "kalshi", "price": 0.38, "contracts": 10.0},
		},
		"lifetime": map[string]any{"net": 0.0, "closed": 0.0, "wins": 0.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	s.r100ResetMLOpenCache()

	// 1) Every guarded path blocks the OPPOSITE side, allows the SAME side and unheld markets.
	for _, src := range r100GuardedSources {
		if r := s.betConflictReason("kalshi", "KXR100HEDGE-A", "NO", src, nil); r != "ml-book-hedge" {
			t.Fatalf("source %q: opposite side must be blocked, got %q", src, r)
		}
		if r := s.betConflictReason("kalshi", "KXR100HEDGE-A", "yes", src, nil); r != "" {
			t.Fatalf("source %q: same side must pass, got %q", src, r)
		}
		if r := s.betConflictReason("kalshi", "KXR100UNHELD", "NO", src, nil); r != "" {
			t.Fatalf("source %q: unheld market must pass, got %q", src, r)
		}
	}
	// 2) Documented exemptions: arb legs pair opposite sides BY DESIGN; gate = operator territory.
	for _, src := range []string{"auto-arb", "gate"} {
		if r := s.betConflictReason("kalshi", "KXR100HEDGE-A", "NO", src, nil); r != "" {
			t.Fatalf("exempt source %q must pass the cross-book check, got %q", src, r)
		}
	}
	// 3) Bug 212: a held DOWN lot is a NO — candidate NO passes (same side), YES blocks. Before
	// normalization the raw "DOWN" blocked BOTH sides.
	if r := s.betConflictReason("kalshi", "KXR100HEDGE-B", "no", "auto-cons-kcrypto", nil); r != "" {
		t.Fatalf("DOWN-held: NO candidate is the SAME side and must pass, got %q", r)
	}
	if r := s.betConflictReason("kalshi", "KXR100HEDGE-B", "YES", "auto-cons-kcrypto", nil); r != "ml-book-hedge" {
		t.Fatalf("DOWN-held: YES candidate must block, got %q", r)
	}
	// 4) One position per market (either side) is unchanged and universal (incl. gate/arb).
	// R111: the blanket one-per-Kalshi-EVENT block ("dup-event") is RETIRED — same-event strike
	// ladders are correlated-DISTINCT markets (operator correction); they now flow through the
	// correlated-cluster exposure cap instead (TestR111DupVsCorrelatedGuard pins both directions).
	pos := []paper.Position{{Platform: "kalshi", Ticker: "KXR100EVT-A", Contracts: 2}}
	if r := s.betConflictReason("kalshi", "KXR100EVT-A", "YES", "gate", pos); r != "dup-ticker" {
		t.Fatalf("dup-ticker must fire for every source, got %q", r)
	}
	if r := s.betConflictReason("kalshi", "KXR100EVT-B", "YES", "auto-arb", pos); r != "" {
		t.Fatalf("a second market of one event is correlated-distinct and must pass (R111), got %q", r)
	}
	if r := s.betConflictReason("polyus", "KXR100EVT-A", "YES", "auto-cons-pusflow", pos); r != "" {
		t.Fatalf("other-platform position must not collide, got %q", r)
	}
}

// TestR100SourceCensus scans server.go's autoPlace call sites: every string-literal source must be
// in the known set, and every known source must appear — a NEW betting path cannot ship without
// updating r100GuardedSources (and therefore joining the guard walk above).
func TestR100SourceCensus(t *testing.T) {
	b, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatalf("read server.go: %v", err)
	}
	// Topology-aware paths carry the same guarded source immediately before their explicit
	// venue-input topology. Scan both call shapes so the wrapper cannot hide a money path.
	re := regexp.MustCompile(`s\.autoPlace\(ctx,[^\n]*?,\s*(?:"([a-z-]+)"|([A-Za-z_][A-Za-z0-9_]*))\)`)
	topologyRe := regexp.MustCompile(`s\.autoPlaceWithTopology\(ctx,[^\n]*?,\s*(?:"([a-z-]+)"|([A-Za-z_][A-Za-z0-9_]*)),\s*(?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\)`)
	found := map[string]bool{}
	varArgs := 0
	matches := append(re.FindAllStringSubmatch(string(b), -1), topologyRe.FindAllStringSubmatch(string(b), -1)...)
	for _, m := range matches {
		if m[1] != "" {
			found[m[1]] = true
		} else {
			varArgs++
		}
	}
	// The kalshi-flow site passes its source via the `src` variable — pin its literal separately.
	if !strings.Contains(string(b), `src := "auto-cons-kalshi"`) {
		t.Fatal("kalshi-flow source literal moved — update the census")
	}
	if varArgs != 1 {
		t.Fatalf("expected exactly 1 variable-source autoPlace call site (kalshi-flow src), found %d", varArgs)
	}
	found["auto-cons-kalshi"] = true
	want := []string{"auto-arb", "gate"}
	for _, source := range r100GuardedSources {
		if source != "auto-ml" { // R166: book-native ML is prediction/log-only, not an autoPlace caller.
			want = append(want, source)
		}
	}
	sort.Strings(want)
	var got []string
	for k := range found {
		got = append(got, k)
	}
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("autoPlace source census drifted:\n got  %v\n want %v\n(new betting path? add it to r100GuardedSources so the guard walk covers it)", got, want)
	}
}

func TestR100NormBinSide(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"UP", "YES"}, {"up", "YES"}, {"DOWN", "NO"}, {" down ", "NO"},
		{"yes", "YES"}, {"NO", "NO"}, {"Over 4.5", "OVER 4.5"}, // exotic labels pass through
		{"OVER", "YES"}, {"under", "NO"}, {"BUY", "YES"}, {"SELL", "NO"}, // r27: the outcomeIdx settle-class set
	} {
		if got := normBinSide(c.in); got != c.want {
			t.Fatalf("normBinSide(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestR100KmktsPutRefreshAndEvict(t *testing.T) {
	s := testServer(t)
	s.kmkts = make(map[string]kalshi.Market, 24000)
	s.kmktsAt = make(map[string]time.Time, 24000)
	old := time.Now().Add(-time.Hour)
	for i := 0; i < 24000; i++ {
		tk := "T-xxx-" + string(rune('a'+i%26)) + "-" + strconv.Itoa(i)
		s.kmkts[tk] = kalshi.Market{Ticker: tk}
		s.kmktsAt[tk] = old
	}
	if len(s.kmkts) != 24000 {
		t.Fatalf("fixture: %d entries", len(s.kmkts))
	}
	// EXISTING key at cap: must refresh in place (bug 208: the old len<cap guard skipped it and
	// the R98b 2-min cache stayed permanently stale).
	existing := "T-xxx-a-0" // i=0 → 'a'
	s.metaMu.Lock()
	s.kmktsPutLocked(existing, kalshi.Market{Ticker: existing, Title: "fresh"})
	s.metaMu.Unlock()
	if s.kmkts[existing].Title != "fresh" || !s.kmktsAt[existing].After(old) {
		t.Fatal("existing key must refresh at cap")
	}
	if len(s.kmkts) != 24000 {
		t.Fatalf("refresh must not evict: %d", len(s.kmkts))
	}
	// NEW key at cap: must land (evicting a slice) instead of being silently dropped.
	s.metaMu.Lock()
	s.kmktsPutLocked("T-NEW", kalshi.Market{Ticker: "T-NEW"})
	s.metaMu.Unlock()
	if _, ok := s.kmkts["T-NEW"]; !ok {
		t.Fatal("new key must land at cap (was: silently dropped)")
	}
	if len(s.kmkts) > 24000 {
		t.Fatalf("cap must hold: %d", len(s.kmkts))
	}
	if len(s.kmkts) < 24000-1600 {
		t.Fatalf("eviction slice too large: %d", len(s.kmkts))
	}
}
