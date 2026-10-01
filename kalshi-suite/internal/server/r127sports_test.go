package server

// R127 pins — poly-int sports anchoring (pintsports.go): (1) the sportsMarketType ALLOW-LIST
// (full-game only; every period/prop spelling refuses — the R125 period-scope lesson applied to
// pint's own vocabulary); (2) signed-spread orientation (venue line rides outcome-0 and is
// ALREADY Kalshi-YES oriented — twins must land on the correct side, incl. the negated-line flip);
// (3) doubleheader disambiguation by start-time window + ambiguous-refusal without time evidence;
// (4) same-city distinctive-evidence resolution; (5) type-mismatch refusal; (6) the consume gate:
// gate OFF ⇒ consumers see NOTHING even with joins anchored.

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

func TestR127PintClassifyAllowList(t *testing.T) {
	for _, tc := range []struct {
		smt  string
		typ  string
		want bool
	}{
		{"moneyline", "winner", true},
		{"spreads", "spread", true},
		{"totals", "total", true},
		{"soccer_team_to_advance", "advance", true},
		// live-probed period/prop spellings — ALL must refuse (allow-list, never deny-list)
		{"baseball_team_first_five_spread", "", false},
		{"baseball_team_first_five_total", "", false},
		{"first_half_totals", "", false},
		{"second_half_totals", "", false},
		{"soccer_team_totals", "", false},
		{"soccer_first_half_team_totals", "", false},
		{"both_teams_to_score", "", false},
		{"nrfi", "", false},
		{"points", "", false},
		{"rebounds", "", false},
		{"baseball_game_extra_innings", "", false},
		{"soccer_extra_time", "", false},
		{"soccer_penalty_shootout", "", false},
		{"", "", false},                            // absent metadata: never guess
		{"basketball_first_quarter_ml", "", false}, // unknown NEW spelling: refuse-by-default
	} {
		typ, ok := pintClassify(tc.smt)
		if ok != tc.want || (ok && typ != tc.typ) {
			t.Errorf("pintClassify(%q) = (%q,%v), want (%q,%v)", tc.smt, typ, ok, tc.typ, tc.want)
		}
	}
}

func TestR127PintEventGrammar(t *testing.T) {
	for _, tc := range []struct {
		key      string
		league   string
		gameSlug string
		ok       bool
	}{
		{"mlb-sea-mia-2026-07-09", "mlb", "mlb-sea-mia-2026-07-09", true},
		{"wnba-sea-atl-2026-07-09", "wnba", "wnba-sea-atl-2026-07-09", true},
		{"fifwc-arg-che-2026-07-11", "fwc", "fifwc-arg-che-2026-07-11", true},
		{"fifwc-arg-che-2026-07-11-more-markets", "fwc", "fifwc-arg-che-2026-07-11", true},
		{"fifwc-arg-che-2026-07-11-exact-score", "", "", false}, // exact-score satellite ≠ game lines
		{"mlb-2026-al-central-champion", "", "", false},         // season future
		{"nba-lebron-james-next-team", "", "", false},
		{"mlb-highest-abs-challenge-success-rate-team-20260702220030937", "", "", false},
		{"bra2-abc-def-2026-07-09", "", "", false}, // league without gameident vocabulary — skipped
		{"bitcoin-above-56k-on-july-10-2026", "", "", false},
		{"", "", "", false},
	} {
		lg, gs, _, _, _, ok := pintParseEventKey(tc.key)
		if ok != tc.ok || lg != tc.league || gs != tc.gameSlug {
			t.Errorf("pintParseEventKey(%q) = (%q,%q,%v), want (%q,%q,%v)", tc.key, lg, gs, ok, tc.league, tc.gameSlug, tc.ok)
		}
	}
}

// The real anchor path over a wire-shaped gamma event: full-game lines anchor with the venue's
// SIGNED line in Kalshi-YES orientation (sameSide + flip twins land correctly); period/prop
// markets refuse; totals twin same-side on the exact line.
func TestR127PintAnchorScopeAndSpreadOrientation(t *testing.T) {
	pintReg.reset()
	s := &Server{}
	st := s.gi()
	s.giMu.Lock()
	sea := st.learnTeam("wnba", "Seattle Storm", "SEA")
	atl := st.learnTeam("wnba", "Atlanta Dream", "ATL")
	start := time.Date(2026, 7, 9, 23, 0, 0, 0, time.UTC)
	g := st.getGame("wnba", "2026-07-09", sea, atl, start)
	if g == nil {
		t.Fatal("getGame returned nil")
	}
	kSEA := "KXWNBASPREAD-26JUL09SEAATL-SEA17"
	kATL := "KXWNBASPREAD-26JUL09SEAATL-ATL17"
	kTOT := "KXWNBATOTAL-26JUL09SEAATL-160"
	st.attachMkt2(g, "kalshi", kSEA, "spread", sea.abbr, "", -16.5) // SEA wins by 17+
	st.attachMkt2(g, "kalshi", kATL, "spread", atl.abbr, "", -16.5) // ATL wins by 17+
	st.attachMkt2(g, "kalshi", kTOT, "total", "over", "", 159.5)
	s.giMu.Unlock()

	var ev polymarket.SportsEvent
	if err := json.Unmarshal([]byte(`{
	  "slug": "wnba-sea-atl-2026-07-09",
	  "startTime": "2026-07-09T23:00:00Z",
	  "teams": [
	    {"name":"Seattle Storm","league":"wnba","abbreviation":"sea","ordering":"away"},
	    {"name":"Atlanta Dream","league":"wnba","abbreviation":"atl","ordering":"home"}],
	  "markets": [
	    {"conditionId":"0xml","slug":"wnba-sea-atl-2026-07-09","question":"Seattle Storm vs. Atlanta Dream","sportsMarketType":"moneyline","outcomes":"[\"Seattle Storm\", \"Atlanta Dream\"]"},
	    {"conditionId":"0xspa","slug":"wnba-sea-atl-2026-07-09-spread-away-16pt5","question":"Spread: Seattle Storm (-16.5)","sportsMarketType":"spreads","line":-16.5,"outcomes":"[\"Seattle Storm\", \"Atlanta Dream\"]"},
	    {"conditionId":"0xsph","slug":"wnba-sea-atl-2026-07-09-spread-home-16pt5","question":"Spread: Atlanta Dream (+16.5)","sportsMarketType":"spreads","line":16.5,"outcomes":"[\"Atlanta Dream\", \"Seattle Storm\"]"},
	    {"conditionId":"0xtot","slug":"wnba-sea-atl-2026-07-09-total-159pt5","question":"O/U 159.5","sportsMarketType":"totals","line":159.5,"outcomes":"[\"Over\", \"Under\"]"},
	    {"conditionId":"0xf5","slug":"wnba-sea-atl-2026-07-09-f5-spread-away-2pt5","question":"1st 5 Spread","sportsMarketType":"baseball_team_first_five_spread","line":-2.5,"outcomes":"[\"Seattle Storm\", \"Atlanta Dream\"]"},
	    {"conditionId":"0xpts","slug":"wnba-sea-atl-2026-07-09-points-star-18pt5","question":"18.5 points?","sportsMarketType":"points","line":18.5,"outcomes":"[\"Yes\", \"No\"]"}
	  ]}`), &ev); err != nil {
		t.Fatalf("fixture decode: %v", err)
	}
	rows := s.pintAnchorEvent("wnba", "wnba-sea-atl-2026-07-09", ev)
	if len(rows) != 4 { // ml + 2 spreads + total; f5 + points refused
		t.Fatalf("anchored rows = %d, want 4 (%+v)", len(rows), rows)
	}
	pintReg.mu.Lock()
	scopeRefused := pintReg.refused["scope"]
	ml, spa, sph := pintReg.refs["0xml"], pintReg.refs["0xspa"], pintReg.refs["0xsph"]
	pintReg.mu.Unlock()
	if scopeRefused != 2 {
		t.Fatalf("scope refusals = %d, want 2 (f5 spread + points prop)", scopeRefused)
	}
	if ml == nil || ml.mktType != "winner" || ml.yesTeam != "SEA" {
		t.Fatalf("moneyline ref wrong: %+v", ml)
	}
	if spa == nil || spa.yesTeam != "SEA" || spa.line != -16.5 {
		t.Fatalf("away spread ref wrong: %+v (venue signed line must NOT be flipped — R125)", spa)
	}
	if sph == nil || sph.yesTeam != "ATL" || sph.line != 16.5 {
		t.Fatalf("home spread ref wrong: %+v", sph)
	}
	// twins via the SAME twinLocked semantics consumers use
	twins := map[string]pintTwinPair{}
	for _, p := range s.pintTwinPairs() {
		twins[p.ref.condID] = p
	}
	if p, ok := twins["0xspa"]; !ok || p.kalshi != kSEA || !p.kalshiSame {
		t.Fatalf("SEA -16.5 twin = %+v, want %s sameSide", twins["0xspa"], kSEA)
	}
	// ATL +16.5 ≡ the complement of SEA -16.5 (opp team, negated line) ⇒ FLIP twin of kSEA —
	// the exact class the R125 double-flip bug used to mis-side.
	if p, ok := twins["0xsph"]; !ok || p.kalshi != kSEA || p.kalshiSame {
		t.Fatalf("ATL +16.5 twin = %+v, want %s flip", twins["0xsph"], kSEA)
	}
	if p, ok := twins["0xtot"]; !ok || p.kalshi != kTOT || !p.kalshiSame {
		t.Fatalf("total twin = %+v, want %s sameSide", twins["0xtot"], kTOT)
	}
	if _, ok := twins["0xf5"]; ok {
		t.Fatal("first-five spread must never twin a full-game market")
	}
}

// Doubleheader: a timed pint start joins ONLY the game inside the ±75min window (#2 here);
// a timeless pint event over a two-game key REFUSES (bug-206 doctrine).
func TestR127PintDoubleheader(t *testing.T) {
	pintReg.reset()
	s := &Server{}
	st := s.gi()
	s.giMu.Lock()
	col := st.teamByAbbr("mlb", "COL")
	lad := st.teamByAbbr("mlb", "LAD")
	g1 := st.getGame("mlb", "2026-07-09", col, lad, time.Date(2026, 7, 9, 17, 10, 0, 0, time.UTC))
	g2 := st.getGame("mlb", "2026-07-09", col, lad, time.Date(2026, 7, 9, 23, 10, 0, 0, time.UTC))
	s.giMu.Unlock()
	if g1 == nil || g2 == nil || g1 == g2 || g2.id != g1.id+"#2" {
		t.Fatalf("doubleheader setup wrong: %v / %v", g1, g2)
	}
	mkts := `"markets":[{"conditionId":"0xdh","slug":"mlb-col-lad-2026-07-09","question":"COL vs LAD","sportsMarketType":"moneyline","outcomes":"[\"Colorado Rockies\", \"Los Angeles Dodgers\"]"}]`
	teams := `"teams":[{"name":"Colorado Rockies","abbreviation":"col","ordering":"away"},{"name":"Los Angeles Dodgers","abbreviation":"lad","ordering":"home"}]`
	var evTimed polymarket.SportsEvent
	if err := json.Unmarshal([]byte(`{"slug":"mlb-col-lad-2026-07-09","startTime":"2026-07-09T23:05:00Z",`+teams+`,`+mkts+`}`), &evTimed); err != nil {
		t.Fatal(err)
	}
	if rows := s.pintAnchorEvent("mlb", "mlb-col-lad-2026-07-09", evTimed); len(rows) != 1 || rows[0].GameID != g2.id {
		t.Fatalf("timed DH anchor = %+v, want game %s", rows, g2.id)
	}
	pintReg.reset()
	var evNoTime polymarket.SportsEvent
	if err := json.Unmarshal([]byte(`{"slug":"mlb-col-lad-2026-07-09",`+teams+`,`+mkts+`}`), &evNoTime); err != nil {
		t.Fatal(err)
	}
	if rows := s.pintAnchorEvent("mlb", "mlb-col-lad-2026-07-09", evNoTime); len(rows) != 0 {
		t.Fatalf("timeless DH must refuse, got %+v", rows)
	}
	pintReg.mu.Lock()
	amb := pintReg.refused["ambiguous_game"]
	pintReg.mu.Unlock()
	if amb != 1 {
		t.Fatalf("ambiguous_game refusals = %d, want 1", amb)
	}
}

// Same-city (bug-205 doctrine): full team names resolve on distinctive tokens; a city-only
// label shared by both teams refuses instead of guessing.
func TestR127PintSameCity(t *testing.T) {
	pintReg.reset()
	s := &Server{}
	st := s.gi()
	s.giMu.Lock()
	lal := st.teamByAbbr("nba", "LAL")
	lac := st.teamByAbbr("nba", "LAC")
	g := st.getGame("nba", "2026-07-09", lal, lac, time.Date(2026, 7, 10, 2, 30, 0, 0, time.UTC))
	s.giMu.Unlock()
	if g == nil {
		t.Fatal("getGame nil")
	}
	var ev polymarket.SportsEvent
	if err := json.Unmarshal([]byte(`{
	  "slug":"nba-lal-lac-2026-07-09","startTime":"2026-07-10T02:30:00Z",
	  "teams":[{"name":"Los Angeles Lakers","abbreviation":"lal","ordering":"away"},
	           {"name":"Los Angeles Clippers","abbreviation":"lac","ordering":"home"}],
	  "markets":[
	    {"conditionId":"0xgood","slug":"nba-lal-lac-2026-07-09","question":"ml","sportsMarketType":"moneyline","outcomes":"[\"Los Angeles Lakers\", \"Los Angeles Clippers\"]"},
	    {"conditionId":"0xbad","slug":"nba-lal-lac-2026-07-09-spread-away-5pt5","question":"sp","sportsMarketType":"spreads","line":-5.5,"outcomes":"[\"Los Angeles\", \"Los Angeles\"]"}
	  ]}`), &ev); err != nil {
		t.Fatal(err)
	}
	rows := s.pintAnchorEvent("nba", "nba-lal-lac-2026-07-09", ev)
	if len(rows) != 1 || rows[0].YesTeam != "LAL" {
		t.Fatalf("same-city anchor = %+v, want only the LAL moneyline", rows)
	}
	pintReg.mu.Lock()
	unresolved := pintReg.refused["side_unresolved"]
	pintReg.mu.Unlock()
	if unresolved != 1 {
		t.Fatalf("side_unresolved = %d, want 1 (city-only label must refuse)", unresolved)
	}
}

// Soccer shapes: per-side Yes/No moneylines resolve from the slug's team-abbrev suffix against
// the venue's own vocabulary (draw included); advance = two-sided single instrument with the
// R106 complement (twins the Kalshi per-team advance on EITHER side); venue home-first slug
// order (fifwc, live-probed) still lands on the Kalshi-ordered game via the reversed-key fallback.
func TestR127PintSoccerSides(t *testing.T) {
	pintReg.reset()
	s := &Server{}
	st := s.gi()
	s.giMu.Lock()
	fra := st.teamByAbbr("fwc", "FRA")
	mar := st.teamByAbbr("fwc", "MAR")
	// Kalshi discovered the game first, FRAMAR order (away=FRA per its ticker pair)
	g := st.getGame("fwc", "2026-07-11", fra, mar, time.Date(2026, 7, 12, 1, 0, 0, 0, time.UTC))
	kFRA := "KXWCGAME-26JUL11FRAMAR-FRA"
	kTIE := "KXWCGAME-26JUL11FRAMAR-TIE"
	kADV := "KXWCADVANCE-26JUL11FRAMAR-FRA"
	st.attachMkt2(g, "kalshi", kFRA, "winner", "FRA", "", 0)
	st.attachMkt2(g, "kalshi", kTIE, "winner", "draw", "", 0)
	st.attachMkt2(g, "kalshi", kADV, "advance", "FRA", "MAR", 0)
	s.giMu.Unlock()
	var ev polymarket.SportsEvent
	if err := json.Unmarshal([]byte(`{
	  "slug":"fifwc-fra-mar-2026-07-11","startTime":"2026-07-12T01:00:00Z",
	  "teams":[{"name":"France","abbreviation":"fra","ordering":"home"},
	           {"name":"Morocco","abbreviation":"mar","ordering":"away"}],
	  "markets":[
	    {"conditionId":"0xfra","slug":"fifwc-fra-mar-2026-07-11-fra","question":"Will France win?","sportsMarketType":"moneyline","outcomes":"[\"Yes\", \"No\"]"},
	    {"conditionId":"0xdraw","slug":"fifwc-fra-mar-2026-07-11-draw","question":"Draw?","sportsMarketType":"moneyline","outcomes":"[\"Yes\", \"No\"]"},
	    {"conditionId":"0xadv","slug":"fifwc-fra-mar-2026-07-11-team-to-advance","question":"Team to Advance","sportsMarketType":"soccer_team_to_advance","outcomes":"[\"Morocco\", \"France\"]"}
	  ]}`), &ev); err != nil {
		t.Fatal(err)
	}
	rows := s.pintAnchorEvent("fwc", "fifwc-fra-mar-2026-07-11", ev)
	if len(rows) != 3 {
		t.Fatalf("anchored = %d, want 3 (%+v)", len(rows), rows)
	}
	twins := map[string]pintTwinPair{}
	for _, p := range s.pintTwinPairs() {
		twins[p.ref.condID] = p
	}
	if p := twins["0xfra"]; p.kalshi != kFRA || !p.kalshiSame {
		t.Fatalf("FRA moneyline twin = %+v, want %s sameSide", p, kFRA)
	}
	if p := twins["0xdraw"]; p.kalshi != kTIE || !p.kalshiSame {
		t.Fatalf("draw twin = %+v, want %s sameSide", p, kTIE)
	}
	// pint advance YES = Morocco advances ≡ NO of Kalshi "France advances" (R106 complement)
	if p := twins["0xadv"]; p.kalshi != kADV || p.kalshiSame {
		t.Fatalf("advance twin = %+v, want %s FLIP", p, kADV)
	}
}

// Type semantics must match exactly: a pint total never twins a Kalshi spread (and vice versa).
func TestR127PintTypeMismatchRefusal(t *testing.T) {
	pintReg.reset()
	s := &Server{}
	st := s.gi()
	s.giMu.Lock()
	sea := st.learnTeam("wnba", "Seattle Storm", "SEA")
	atl := st.learnTeam("wnba", "Atlanta Dream", "ATL")
	g := st.getGame("wnba", "2026-07-09", sea, atl, time.Date(2026, 7, 9, 23, 0, 0, 0, time.UTC))
	st.attachMkt2(g, "kalshi", "KXWNBASPREAD-26JUL09SEAATL-SEA17", "spread", "SEA", "", -16.5)
	s.giMu.Unlock()
	var ev polymarket.SportsEvent
	if err := json.Unmarshal([]byte(`{
	  "slug":"wnba-sea-atl-2026-07-09","startTime":"2026-07-09T23:00:00Z",
	  "teams":[{"name":"Seattle Storm","abbreviation":"sea","ordering":"away"},
	           {"name":"Atlanta Dream","abbreviation":"atl","ordering":"home"}],
	  "markets":[{"conditionId":"0xtot","slug":"wnba-sea-atl-2026-07-09-total-159pt5","question":"O/U","sportsMarketType":"totals","line":159.5,"outcomes":"[\"Over\", \"Under\"]"}]}`), &ev); err != nil {
		t.Fatal(err)
	}
	if rows := s.pintAnchorEvent("wnba", "wnba-sea-atl-2026-07-09", ev); len(rows) != 1 {
		t.Fatalf("total should anchor (grouping), got %+v", rows)
	}
	if pairs := s.pintTwinPairs(); len(pairs) != 0 {
		t.Fatalf("total must NOT twin a spread — got %+v", pairs)
	}
}

// THE GATE: with joins anchored and twins live, consumers see NOTHING until
// auto.pint_sports_consume is armed (and even armed, a nil poly client yields nothing —
// prices are never fabricated).
func TestR127PintGateOffConsumersSeeNothing(t *testing.T) {
	pintReg.reset()
	s := &Server{}
	st := s.gi()
	s.giMu.Lock()
	sea := st.learnTeam("wnba", "Seattle Storm", "SEA")
	atl := st.learnTeam("wnba", "Atlanta Dream", "ATL")
	g := st.getGame("wnba", "2026-07-09", sea, atl, time.Date(2026, 7, 9, 23, 0, 0, 0, time.UTC))
	st.attachMkt2(g, "kalshi", "KXWNBATOTAL-26JUL09SEAATL-160", "total", "over", "", 159.5)
	s.giMu.Unlock()
	var ev polymarket.SportsEvent
	if err := json.Unmarshal([]byte(`{
	  "slug":"wnba-sea-atl-2026-07-09","startTime":"2026-07-09T23:00:00Z",
	  "teams":[{"name":"Seattle Storm","abbreviation":"sea","ordering":"away"},
	           {"name":"Atlanta Dream","abbreviation":"atl","ordering":"home"}],
	  "markets":[{"conditionId":"0xtot","slug":"wnba-sea-atl-2026-07-09-total-159pt5","question":"O/U","sportsMarketType":"totals","line":159.5,"outcomes":"[\"Over\", \"Under\"]"}]}`), &ev); err != nil {
		t.Fatal(err)
	}
	if rows := s.pintAnchorEvent("wnba", "wnba-sea-atl-2026-07-09", ev); len(rows) != 1 {
		t.Fatalf("anchor failed: %+v", rows)
	}
	if pairs := s.pintTwinPairs(); len(pairs) != 1 {
		t.Fatalf("twin should exist pre-gate: %+v", pairs) // the JOIN exists…
	}
	// …but the consumer surface is empty: default config (zero value) = gate OFF.
	if c := s.pintLockCandidates(); c != nil {
		t.Fatalf("gate OFF must yield nil candidates, got %+v", c)
	}
	// Armed but with no poly client/prices: still nothing — never fabricate a price.
	cfg := config.Default()
	cfg.Auto.PintSportsConsume = true
	s.cfgP.Store(&cfg)
	if c := s.pintLockCandidates(); len(c) != 0 {
		t.Fatalf("armed with no prices must yield 0 candidates, got %+v", c)
	}
	// Default() itself must ship the gate OFF and the anchor ON.
	d := config.Default()
	if d.Auto.PintSportsConsume {
		t.Fatal("pint_sports_consume must default FALSE")
	}
	if !d.Auto.PintSportsAnchorOn() {
		t.Fatal("pint_sports_anchor must default TRUE (nil = on)")
	}
}

// TestR127OfflineCoverage — the R127 DRY RUN against a READ-ONLY copy of the live DB (not part
// of the normal suite: set PINT_DRYRUN=1; needs data/kalshi.db). Hydrates the game/team registry
// from game_identity + market_game, walks every OPEN poly-int catalog event key through the real
// grammar, and (optionally, PINT_DRYRUN_LIVE=n) runs the REAL anchor path against n live gamma
// events with 150ms spacing. Prints the before→after coverage estimate.
func TestR127OfflineCoverage(t *testing.T) {
	if os.Getenv("PINT_DRYRUN") == "" {
		t.Skip("set PINT_DRYRUN=1 (needs a data/kalshi.db copy)")
	}
	runPintDryRun(t)
}
