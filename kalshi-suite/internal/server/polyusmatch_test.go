package server

// Cross-venue Poly-US matcher regression locks. The /api/arb third-venue enrichment (handleArb)
// and logArbXV both route through polyUSPriceForSide — the per-market Team/abbrev side match plus
// a shared DISTINCTIVE (>=6-char) game token. These tests pin that behavior as currently
// implemented so the arb panel's polyus_* fields can't silently drift:
//
//	(a) word-initials abbrev   NYY → "New York Yankees"       (abbrevMatchesSide path a)
//	(b) first-word subsequence WSH → "Washington Nationals"   (abbrevMatchesSide path b)
//	(c) side gate: the OTHER team of the same game must NOT match
//	(d) game gate: a shared short token ("york", 4 chars) is NOT distinctive — >=6 required
//	(e) price gate: Yes<=0.02 / >=0.98 (resolved-ish) entries are skipped outright
//
// No network, no fixtures: Server is constructed directly with a seeded polyUSMkts cache
// (polyUSPriceForSide takes polyUSMu itself via polyUSSnapshot — same convention as kmkt —
// so a zero-value mutex is all it needs).
//
// R68 adds locks on the SHARED entry point polyUSMatchForSide (polyUSPriceForSide is now its
// family-less wrapper — same join, same results):
//
//	② DATE GUARD  — both-sides-dated mismatches reject (±1 ET calendar day tolerated for
//	                midnight-spanning games); a dateless side NEVER rejects (pre-R68 behavior).
//	③ TYPE GUARD  — winner/moneyline never pairs with totals/spreads/props; the scan continues
//	                past a guard rejection so a same-type twin later in the snapshot still matches.
//	① TELEMETRY   — per-family attempt/hit counters + the deduped misses ring (/api/matchlog),
//	                guard rejections logged distinctly (reason date-mismatch / type-mismatch).

import (
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

func TestPolyUSPriceForSide(t *testing.T) {
	s := &Server{polyUSMkts: []polyUSMarket{
		{ // (e) name-matches its question below, but Yes>=0.98 must be filtered before any matching
			League: "mlb", Game: "Los Angeles Dodgers vs San Francisco Giants",
			Team: "LAD", TeamName: "Los Angeles Dodgers",
			Slug: "aec-mlb-lad-sf-2026-07-03", Yes: 0.99, Bid: 0.98, Ask: 1.0,
		},
		{
			League: "mlb", Game: "New York Yankees vs Boston Red Sox",
			Team: "NYY", TeamName: "New York Yankees",
			Slug: "aec-mlb-nyy-bos-2026-07-03", Yes: 0.61, Bid: 0.60, Ask: 0.62,
		},
		{
			League: "mlb", Game: "Washington Nationals vs New York Mets",
			Team: "WSH", TeamName: "Washington Nationals",
			Slug: "aec-mlb-wsh-nym-2026-07-03", Yes: 0.34, Bid: 0.33, Ask: 0.35,
		},
	}}
	cases := []struct {
		name, question, side string
		wantSlug             string  // "" = expect no match
		wantYes              float64 // checked only when wantSlug != ""
	}{
		{"abbrev initials (NYY)", "Will the New York Yankees beat the Boston Red Sox?", "New York Yankees",
			"aec-mlb-nyy-bos-2026-07-03", 0.61},
		{"abbrev first-word subsequence (WSH)", "Will the Washington Nationals beat the New York Mets?", "Washington Nationals",
			"aec-mlb-wsh-nym-2026-07-03", 0.34},
		// R63 2e: Poly-int outcome labels are usually NICKNAMES — the TeamName join must catch them
		// (this was why the cross-venue PUS column was always "—": "Yankees" never matched "NYY").
		{"nickname side matches via TeamName (R63 2e)", "Will the New York Yankees beat the Boston Red Sox?", "Yankees",
			"aec-mlb-nyy-bos-2026-07-03", 0.61},
		{"same game, other team must not match", "Will the New York Yankees beat the Boston Red Sox?", "Boston Red Sox",
			"", 0},
		{"shared short token is not distinctive", "Will New York cover the run line at home?", "New York Yankees",
			"", 0},
		{"resolved-price entry is filtered", "Will the Los Angeles Dodgers beat the San Francisco Giants?", "Los Angeles Dodgers",
			"", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			slug, yes, ok := s.polyUSPriceForSide(c.question, c.side)
			if c.wantSlug == "" {
				if ok {
					t.Fatalf("polyUSPriceForSide(%q, %q) matched %q (yes=%v), want no match", c.question, c.side, slug, yes)
				}
				return
			}
			if !ok || slug != c.wantSlug || yes != c.wantYes {
				t.Fatalf("polyUSPriceForSide(%q, %q) = (%q, %v, %v), want (%q, %v, true)",
					c.question, c.side, slug, yes, ok, c.wantSlug, c.wantYes)
			}
		})
	}
}

// nyySnapshot seeds the canonical Yankees/Red Sox moneyline used by the R68 guard tests.
// slugDate ("2026-07-03" or "" for a dateless slug) and start (RFC3339 or "") control exactly
// which date evidence the PolyUS side carries.
func nyySnapshot(slugDate, start string) []polyUSMarket {
	slug := "aec-mlb-nyy-bos"
	if slugDate != "" {
		slug += "-" + slugDate
	}
	return []polyUSMarket{{
		League: "mlb", Game: "New York Yankees vs Boston Red Sox",
		Team: "NYY", TeamName: "New York Yankees",
		Slug: slug, Yes: 0.61, Bid: 0.60, Ask: 0.62, Start: start,
	}}
}

// TestPolyUSMatchDateGuard — R68 ②: same teams + a distinctive token pair ALL SEASON, so when both
// sides carry a game date they must sit on the same ET calendar day (±1 for midnight spans). A
// dateless side must never reject (exact pre-R68 behavior — no false rejections).
func TestPolyUSMatchDateGuard(t *testing.T) {
	const q, side = "Will the New York Yankees beat the Boston Red Sox?", "New York Yankees"
	s := &Server{polyUSMkts: nyySnapshot("2026-07-03", "")} // PolyUS date from the slug suffix

	if slug, _, _, ok := s.polyUSMatchForSide("arb", q, side, "mlb-nyy-bos-2026-07-03"); !ok || slug != "aec-mlb-nyy-bos-2026-07-03" {
		t.Fatalf("same date must accept: (%q, %v)", slug, ok)
	}
	for _, d := range []string{"2026-07-04", "2026-07-02"} { // midnight-spanning tolerance
		if _, _, _, ok := s.polyUSMatchForSide("arb", q, side, "mlb-nyy-bos-"+d); !ok {
			t.Fatalf("±1 day (%s) must accept", d)
		}
	}
	if slug, _, _, ok := s.polyUSMatchForSide("arb", q, side, "mlb-nyy-bos-2026-07-10"); ok {
		t.Fatalf("same teams a week apart must reject, matched %q", slug)
	}
	// No input date (empty ref / no-date shapes) → unchanged behavior: accept.
	if _, _, _, ok := s.polyUSMatchForSide("arb", q, side); !ok {
		t.Fatal("no ref at all must keep matching")
	}
	if _, _, _, ok := s.polyUSMatchForSide("arb", q, side, "", "KXMLBGAME-NOTIME-NYY"); !ok {
		t.Fatal("dateless refs must keep matching")
	}
	// No PolyUS date (dateless slug, no Start) → dated input still accepts.
	s2 := &Server{polyUSMkts: nyySnapshot("", "")}
	if _, _, _, ok := s2.polyUSMatchForSide("arb", q, side, "mlb-nyy-bos-2026-07-10"); !ok {
		t.Fatal("dateless PolyUS side must keep matching")
	}
	// Snapshot Start OUTRANKS the slug date, converted to the ET wall date: 2026-07-11T00:30Z is
	// still Jul 10 in ET, so a Jul 10 ref accepts even though the slug says Jul 3 — and Jul 3 rejects.
	s3 := &Server{polyUSMkts: nyySnapshot("2026-07-03", "2026-07-11T00:30:00Z")}
	if _, _, _, ok := s3.polyUSMatchForSide("arb", q, side, "mlb-nyy-bos-2026-07-10"); !ok {
		t.Fatal("Start (UTC→ET Jul 10) must outrank the slug date and accept a Jul 10 ref")
	}
	if _, _, _, ok := s3.polyUSMatchForSide("arb", q, side, "mlb-nyy-bos-2026-07-03"); ok {
		t.Fatal("Start (UTC→ET Jul 10) must reject a Jul 3 ref despite the Jul 3 slug")
	}
	// Kalshi TICKER-embedded dates count as evidence too. Built against the live clock so
	// kalshiTickerStart's plausibility gate passes (same convention as kalshi_start_test.go).
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load America/New_York: %v", err)
	}
	near := time.Now().In(et).Add(2 * time.Hour)
	sNow := &Server{polyUSMkts: nyySnapshot("", near.UTC().Format(time.RFC3339))}
	tick := "KXMLBGAME-" + strings.ToUpper(near.Format("06Jan021504")) + "NYYBOS-NYY"
	if _, _, _, ok := sNow.polyUSMatchForSide("arb", q, side, tick); !ok {
		t.Fatalf("ticker-embedded date matching the Start day must accept (%s)", tick)
	}
	far := near.Add(7 * 24 * time.Hour) // +7d2h: inside the ticker plausibility window, wrong game day
	tickFar := "KXMLBGAME-" + strings.ToUpper(far.Format("06Jan021504")) + "NYYBOS-NYY"
	if _, _, _, ok := sNow.polyUSMatchForSide("arb", q, side, tickFar); ok {
		t.Fatalf("ticker-embedded date a week out must reject (%s)", tickFar)
	}
}

// TestPolyUSMatchTypeGuard — R68 ③: a winner/moneyline side must never pair with a totals/
// spreads/props side. Same-type passes, and a guard rejection does NOT end the scan — the
// same-game same-type twin later in the snapshot still matches.
func TestPolyUSMatchTypeGuard(t *testing.T) {
	const side = "New York Yankees"
	ml := nyySnapshot("2026-07-03", "")[0]
	tot := ml // a totals sibling: same teams, "prop"-classified Game text
	tot.Game, tot.Slug, tot.Yes = "New York Yankees vs Boston Red Sox total runs", "aec-mlb-nyy-bos-total", 0.44
	s := &Server{polyUSMkts: []polyUSMarket{ml, tot}}

	if slug, _, _, ok := s.polyUSMatchForSide("bridge", "Will the New York Yankees beat the Boston Red Sox?", side); !ok || slug != ml.Slug {
		t.Fatalf("winner vs winner must accept the MONEYLINE twin: (%q, %v)", slug, ok)
	}
	for _, q := range []string{
		"Yankees vs Red Sox: total runs O/U 8.5",   // O/U + "total" → class "total"
		"New York Yankees over 4.5 runs vs Boston", // over/under words → "total"
	} {
		slug, _, _, ok := s.polyUSMatchForSide("bridge", q, side)
		if !ok || slug != tot.Slug {
			t.Fatalf("total-typed %q must skip the moneyline and land on the totals twin: (%q, %v)", q, slug, ok)
		}
	}
	// R100 (auditor bug 207): the classes are SPLIT — a spread/prop question must no longer land
	// on the same team's TOTALS row (that exact pairing was the bug the lumped class allowed; the
	// pre-R100 version of this test pinned the buggy behavior).
	for _, q := range []string{
		"Spread: New York Yankees -1.5 vs Red Sox",     // spread ≠ total
		"New York Yankees to win by a set score",       // prop ≠ total
		"Corners handicap: New York Yankees vs Boston", // corners → prop ≠ total
	} {
		if slug, _, _, ok := s.polyUSMatchForSide("bridge", q, side); ok {
			t.Fatalf("%q must not land on a totals row after the R100 class split, matched %q", q, slug)
		}
	}
	// …and a spread sibling attracts the spread question (positive coverage of the split).
	spr := ml
	spr.Game, spr.Slug, spr.Yes = "New York Yankees vs Boston Red Sox spread", "aec-mlb-nyy-bos-spread", 0.52
	sSpr := &Server{polyUSMkts: []polyUSMarket{ml, tot, spr}}
	if slug, _, _, ok := sSpr.polyUSMatchForSide("bridge", "Spread: New York Yankees -1.5 vs Red Sox", side); !ok || slug != spr.Slug {
		t.Fatalf("spread question must land on the spread twin: (%q, %v)", slug, ok)
	}
	// With ONLY the moneyline listed, a prop-typed question must reject outright…
	sML := &Server{polyUSMkts: []polyUSMarket{ml}}
	if slug, _, _, ok := sML.polyUSMatchForSide("bridge", "Yankees vs Red Sox: total runs O/U 8.5", side); ok {
		t.Fatalf("winner-only snapshot must reject an O/U question, matched %q", slug)
	}
	// …and with ONLY the totals row listed, a winner question must reject too (both directions).
	sTot := &Server{polyUSMkts: []polyUSMarket{tot}}
	if slug, _, _, ok := sTot.polyUSMatchForSide("bridge", "Will the New York Yankees beat the Boston Red Sox?", side); ok {
		t.Fatalf("prop-only snapshot must reject a winner question, matched %q", slug)
	}
}

// TestPolyUSMatchTelemetry — R68 ①: counters count EVERY attempt per caller family (no dedup),
// the misses ring dedups by input key, and guard rejections land distinctly (reason + the
// rejected candidate's slug). Mirrors TestNameFallbackRingDedup's reset-then-assert convention.
func TestPolyUSMatchTelemetry(t *testing.T) {
	matchLogMu.Lock() // package-level state — reset for hermeticity (other tests route through the matcher)
	matchAttempts = map[string]int64{}
	matchHits = map[string]int64{}
	matchStructHits = map[string]int64{}
	matchFuzzyHits = map[string]int64{}
	matchMissSeen = map[string]bool{}
	matchMissRing = nil
	matchLogMu.Unlock()
	ring := func() []map[string]string {
		matchLogMu.Lock()
		defer matchLogMu.Unlock()
		return append([]map[string]string(nil), matchMissRing...)
	}

	const q, side = "Will the New York Yankees beat the Boston Red Sox?", "New York Yankees"
	s := &Server{polyUSMkts: nyySnapshot("2026-07-03", "")}

	if _, _, _, ok := s.polyUSMatchForSide("arb", q, side); !ok {
		t.Fatal("seeded twin must match")
	}
	miss := func() { // same unmatched input twice: counters grow each time, the ring rows dedup
		s.polyUSMatchForSide("arb", "Will the Chicago Bulls beat the Miami Heat?", "Chicago Bulls")
	}
	miss()
	miss()
	if _, _, _, ok := s.polyUSMatchForSide("startborrow", q, side); !ok {
		t.Fatal("second family must match independently")
	}

	matchLogMu.Lock()
	arbA, arbH := matchAttempts["arb"], matchHits["arb"]
	sbA, sbH := matchAttempts["startborrow"], matchHits["startborrow"]
	matchLogMu.Unlock()
	if arbA != 3 || arbH != 1 {
		t.Fatalf("arb counters = %d hits / %d attempts, want 1/3 (attempts never dedup)", arbH, arbA)
	}
	if sbA != 1 || sbH != 1 {
		t.Fatalf("startborrow counters = %d/%d, want 1/1 (families count separately)", sbH, sbA)
	}
	r := ring()
	if len(r) != 1 || r[0]["family"] != "arb" || r[0]["reason"] != "no-match" || r[0]["slug"] != "" {
		t.Fatalf("ring must hold ONE deduped no-match row: %v", r)
	}

	// Guard rejections: distinct reasons + the rejected candidate's slug in the ring.
	if _, _, _, ok := s.polyUSMatchForSide("bridge", "Yankees vs Red Sox total runs O/U 8.5", side); ok {
		t.Fatal("type guard must reject O/U vs the moneyline")
	}
	if _, _, _, ok := s.polyUSMatchForSide("bridge", q, side, "mlb-nyy-bos-2026-08-11"); ok {
		t.Fatal("date guard must reject a different game date")
	}
	r = ring()
	if len(r) != 3 {
		t.Fatalf("guard rejections must append their own rows: %v", r)
	}
	if r[1]["reason"] != "type-mismatch" || r[1]["slug"] != "aec-mlb-nyy-bos-2026-07-03" || r[1]["family"] != "bridge" {
		t.Fatalf("type-mismatch row wrong: %v", r[1])
	}
	if r[2]["reason"] != "date-mismatch" || r[2]["slug"] != "aec-mlb-nyy-bos-2026-07-03" {
		t.Fatalf("date-mismatch row wrong: %v", r[2])
	}
}

// ————— R69: telemetry locks for the matchers R68 left dark —————
// One test per new family, feed-free (constructed inputs only). The kthresh LADDER pick
// (kalshiThresholdMispricing: MarketsBySeries), the handleWhales payload plumbing (live tapes) and
// sweepSharpline end-to-end (Odds API) need live feeds and are NOT exercised here — their pure cores
// (strikeParseTelem, smartMoneyPUSJoin, crossMatchKalshi) are what these tests pin.

// resetMatchTelemetry clears the package-level R68 telemetry state (counters + ring) so each R69
// family test starts hermetic — same convention as TestPolyUSMatchTelemetry's inline reset.
func resetMatchTelemetry() {
	matchLogMu.Lock()
	matchAttempts = map[string]int64{}
	matchHits = map[string]int64{}
	matchStructHits = map[string]int64{}
	matchFuzzyHits = map[string]int64{}
	matchMissSeen = map[string]bool{}
	matchMissRing = nil
	matchLogMu.Unlock()
}

func matchFamilyCounts(family string) (attempts, hits int64) {
	matchLogMu.Lock()
	defer matchLogMu.Unlock()
	return matchAttempts[family], matchHits[family]
}

func matchRingRows() []map[string]string {
	matchLogMu.Lock()
	defer matchLogMu.Unlock()
	return append([]map[string]string(nil), matchMissRing...)
}

// TestIntBridgeTelemetry — R69 family "int-bridge": bestKalshiMatch (the kalshi↔poly-int matcher
// core that handleArb and consensusKalshiMatch/pmatch both route through) counts every attempt, and
// a miss records the FIRST gate that rejected an accept-worthy candidate: "date-gate" (the 14h
// window), "kind-gate" (line/kind/segment), else "no-candidate".
func TestIntBridgeTelemetry(t *testing.T) {
	resetMatchTelemetry()
	kmkts := []kalshi.Market{{
		Ticker: "KXMLBGAME-YANKS", EventTicker: "KXMLBGAME",
		Title: "Yankees vs Red Sox Winner", YesSubTitle: "New York Yankees",
		ExpectedExpiration: "2026-07-03T22:00:00Z",
	}}
	pm := func(q, end string) polymarket.Market { return polymarket.Market{Question: q, EndDate: end} }

	if _, ok := bestKalshiMatch(pm("Will the New York Yankees beat the Boston Red Sox?", "2026-07-03T23:00:00Z"), kmkts); !ok {
		t.Fatal("same-day Yankees twin must match")
	}
	if _, ok := bestKalshiMatch(pm("Will the New York Yankees beat the Boston Red Sox?", "2026-07-10T23:00:00Z"), kmkts); ok {
		t.Fatal("a week-apart date must reject (14h gate)")
	}
	if _, ok := bestKalshiMatch(pm("Yankees vs Red Sox: total runs O/U 8.5", "2026-07-03T23:00:00Z"), kmkts); ok {
		t.Fatal("a lined totals question must not pair with the moneyline (line/kind gate)")
	}
	if _, ok := bestKalshiMatch(pm("Will the Chicago Bulls beat the Miami Heat?", "2026-07-03T23:00:00Z"), kmkts); ok {
		t.Fatal("token-disjoint markets must not match")
	}

	a, h := matchFamilyCounts("int-bridge")
	if a != 4 || h != 1 {
		t.Fatalf("int-bridge counters = %d/%d, want 1 hit / 4 attempts", h, a)
	}
	r := matchRingRows()
	if len(r) != 3 {
		t.Fatalf("want 3 deduped miss rows, got %v", r)
	}
	if r[0]["reason"] != "date-gate" || r[0]["slug"] != "KXMLBGAME-YANKS" || r[0]["family"] != "int-bridge" {
		t.Fatalf("date-gate row wrong: %v", r[0])
	}
	if r[1]["reason"] != "kind-gate" || r[1]["slug"] != "KXMLBGAME-YANKS" {
		t.Fatalf("kind-gate row wrong: %v", r[1])
	}
	if r[2]["reason"] != "no-candidate" || r[2]["slug"] != "" {
		t.Fatalf("no-candidate row wrong: %v", r[2])
	}
}

// TestSharplineTelemetryAndDateGuard — R69 family "sharpline": crossMatchKalshi counts attempts/hits,
// and the NEW date guard (the one the R68 audit flagged as missing vs its sibling bestKalshiMatch)
// rejects a dated event against a Kalshi expiry outside ±14h — while a dateless input (evDateOK
// false) keeps the exact pre-R69 behavior and still matches.
func TestSharplineTelemetryAndDateGuard(t *testing.T) {
	resetMatchTelemetry()
	kmkts := []kalshi.Market{{
		Ticker: "KXMLBGAME-YANKS", EventTicker: "KXMLBGAME",
		Title: "Yankees vs Red Sox Winner", YesSubTitle: "New York Yankees",
		ExpectedExpiration: "2026-07-03T22:00:00Z",
	}}
	const question = "New York Yankees vs Boston Red Sox New York Yankees wins" // sweepSharpline's phrasing

	sameDay, ok1 := parseTime("2026-07-03T20:00:00Z")
	weekOut, ok2 := parseTime("2026-07-10T20:00:00Z")
	if !ok1 || !ok2 {
		t.Fatal("test dates must parse")
	}
	if _, _, _, ok := crossMatchKalshi(question, kmkts, sameDay, true); !ok {
		t.Fatal("same-day commence_time must match")
	}
	if _, _, _, ok := crossMatchKalshi(question, kmkts, weekOut, true); ok {
		t.Fatal("a week-out commence_time must reject (new ±14h date guard)")
	}
	if _, _, _, ok := crossMatchKalshi(question, kmkts, weekOut, false); !ok {
		t.Fatal("dateless input must keep matching (guard silent — pre-R69 behavior)")
	}

	a, h := matchFamilyCounts("sharpline")
	if a != 3 || h != 2 {
		t.Fatalf("sharpline counters = %d/%d, want 2 hits / 3 attempts", h, a)
	}
	r := matchRingRows()
	if len(r) != 1 || r[0]["reason"] != "date-gate" || r[0]["slug"] != "KXMLBGAME-YANKS" || r[0]["family"] != "sharpline" {
		t.Fatalf("want ONE sharpline date-gate row with the rejected ticker: %v", r)
	}
}

// TestSmartMoneyPUSJoin — R69 family "smartmoney": the server-side replacement for the dashboard's
// client canonTok join. A YES-side Kalshi row label ("Title — YesSubTitle") joins to the Poly US twin
// via the shared matcher and picks up that market's recent YES taker notional; the join is counted.
func TestSmartMoneyPUSJoin(t *testing.T) {
	resetMatchTelemetry()
	s := &Server{polyUSMkts: nyySnapshot("2026-07-03", "")}
	pusYes := map[string]float64{"aec-mlb-nyy-bos-2026-07-03": 1234}

	slug, n := s.smartMoneyPUSJoin("Yankees vs Red Sox Winner — New York Yankees", "KXMLBGAME-YANKS", pusYes)
	if slug != "aec-mlb-nyy-bos-2026-07-03" || n != 1234 {
		t.Fatalf("join = (%q, %v), want the seeded twin + its YES notional", slug, n)
	}
	if slug, n = s.smartMoneyPUSJoin("Bulls vs Heat Winner — Chicago Bulls", "KXNBAGAME-BULLS", pusYes); slug != "" || n != 0 {
		t.Fatalf("unrelated row must not join: (%q, %v)", slug, n)
	}
	// Matched market with NO recent YES prints → the matcher hit still counts, the row stays empty.
	if slug, n = s.smartMoneyPUSJoin("Yankees vs Red Sox Winner — New York Yankees", "KXMLBGAME-YANKS", map[string]float64{}); slug != "" || n != 0 {
		t.Fatalf("no-print join must return empty: (%q, %v)", slug, n)
	}

	a, h := matchFamilyCounts("smartmoney")
	if a != 3 || h != 2 {
		t.Fatalf("smartmoney counters = %d/%d, want 2 hits / 3 attempts", h, a)
	}
}

// TestResolverTelemetry — R69 family "resolver": polyUSResolveSignal (the live_prod v1 token matcher
// + price-match guard) counts every call; hits are matched AND guard-passed, guard rejections land
// under static reasons with the matched slug.
func TestResolverTelemetry(t *testing.T) {
	resetMatchTelemetry()
	s := &Server{polyUSMkts: nyySnapshot("2026-07-03", "")}

	if slug, _, ok, reason := s.polyUSResolveSignal("New York Yankees moneyline", "NYY", 0.61); !ok || slug == "" {
		t.Fatalf("agreeing signal must pass the guard: (%q, %v, %q)", slug, ok, reason)
	}
	if _, _, ok, _ := s.polyUSResolveSignal("Chicago Bulls moneyline", "Bulls", 0.61); ok {
		t.Fatal("token-disjoint signal must not resolve")
	}
	if _, _, ok, _ := s.polyUSResolveSignal("New York Yankees moneyline", "NYY", 0.20); ok {
		t.Fatal("a 41¢ divergence must fail the price-match guard")
	}
	if _, _, ok, _ := s.polyUSResolveSignal("", "", 0.5); ok {
		t.Fatal("empty signal must not resolve")
	}

	a, h := matchFamilyCounts("resolver")
	if a != 4 || h != 1 {
		t.Fatalf("resolver counters = %d/%d, want 1 hit / 4 attempts", h, a)
	}
	r := matchRingRows()
	if len(r) != 3 {
		t.Fatalf("want 3 resolver miss rows, got %v", r)
	}
	if r[0]["reason"] != "no-match" || r[1]["reason"] != "price-diverge" || r[2]["reason"] != "empty-signal" {
		t.Fatalf("resolver miss reasons wrong: %v", r)
	}
	if r[1]["slug"] != "aec-mlb-nyy-bos-2026-07-03" {
		t.Fatalf("guard rejection must carry the matched slug: %v", r[1])
	}
}

// TestStrikeTelemetry — R69 family "strike": the submarket↔quantity pairing sites. strikeParseTelem
// (kthresh KX{COIN}D rung parse — T-floor and B-range suffixes) and wxFindBin (temperature-ladder
// bin pick) count attempts/hits; unparseable inputs and uncovered ladders ring with the raw ticker.
func TestStrikeTelemetry(t *testing.T) {
	resetMatchTelemetry()
	if v := strikeParseTelem("KXBTCD-26JUL0313-T61999.99"); v != 61999.99 {
		t.Fatalf("T-suffix strike = %v, want 61999.99", v)
	}
	if v := strikeParseTelem("KXBTCD-26JUL0313-B63125"); v != 63125 {
		t.Fatalf("B-suffix strike = %v, want 63125", v)
	}
	if v := strikeParseTelem("KXHIGHNY-26JUL03"); v != 0 {
		t.Fatalf("suffix-less ticker must parse to 0, got %v", v)
	}

	mkts := []kalshi.Market{
		{Ticker: "KXHIGHNY-26JUL03-B98.5", YesSubTitle: "98° to 99°"},
		{Ticker: "KXHIGHNY-26JUL03-T97", YesSubTitle: "97° or below"},
		{Ticker: "KXHIGHNY-26JUL03-BAD", YesSubTitle: "weird"},
	}
	// Unparseable bin first (rings), covering bin second (hit) — same scan order as wxScanSeries.
	if bi, lo, hi, ok := wxFindBin("KXHIGHNY", "2026-07-03", mkts, []int{2, 0, 1}, 98); !ok || bi != 0 || lo != 98 || hi != 99 {
		t.Fatalf("wxFindBin = (%d, %v, %v, %v), want the 98–99 bin", bi, lo, hi, ok)
	}
	// A forecast no bin covers → the ladder-level miss.
	if _, _, _, ok := wxFindBin("KXHIGHNY", "2026-07-03", mkts, []int{2, 0, 1}, 150); ok {
		t.Fatal("150° must not land in any bin")
	}

	// R100 (auditor bug 213): the per-rung strike parse and the wx bin pick count under their own
	// families now — "strike" tracked whichever pipeline was busiest before.
	a, h := matchFamilyCounts("strike")
	if a != 3 || h != 2 {
		t.Fatalf("strike counters = %d/%d, want 2 hits / 3 attempts", h, a)
	}
	wa, wh := matchFamilyCounts("strike-wxbin")
	if wa != 4 || wh != 1 {
		t.Fatalf("strike-wxbin counters = %d/%d, want 1 hit / 4 attempts", wh, wa)
	}
	r := matchRingRows()
	if len(r) != 3 {
		t.Fatalf("want 3 strike miss rows, got %v", r)
	}
	if r[0]["reason"] != "unparseable-suffix" || r[0]["slug"] != "KXHIGHNY-26JUL03" {
		t.Fatalf("unparseable-suffix row wrong: %v", r[0])
	}
	if r[1]["reason"] != "unparseable-bin" || r[1]["slug"] != "KXHIGHNY-26JUL03-BAD" {
		t.Fatalf("unparseable-bin row wrong: %v", r[1])
	}
	if r[2]["reason"] != "no-bin-covering-value" || r[2]["slug"] != "high=150" || r[2]["title"] != "KXHIGHNY 2026-07-03" {
		t.Fatalf("no-bin row wrong: %v", r[2])
	}
}
