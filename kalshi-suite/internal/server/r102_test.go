// r102_test.go — R102 STRUCTURAL GAME MATCHING pins.
//
//  1. Kalshi event parsing: series → (league, type); ET date+HHMM+pair → canonical identity with
//     the venue-probed timezone math (2145 ET == 01:45Z next day).
//  2. Same-city (bug-205 class): CHC/CWS anchor as DISTINCT identities — a struct join can never
//     cross them; bare "Chicago" side text refuses.
//  3. Doubleheader (bug-206 class): same teams, same ET day, two start times = two canonical
//     games; start-time proximity anchors each venue row; a timeless ref against two games REFUSES.
//  4. Abbrev collision: a pair that splits two ways refuses (never guess a game identity).
//  5. Twin equivalence: winner (same side), total (line match), spread (opposite-team listing =
//     YES↔NO with negated line).
//  6. polyUSMatchForSide struct-first: an anchored ref resolves via the join (no fuzz); the fuzzy
//     path still serves unanchored questions and is counted as fallback.
//  7. px-age aggregator sanity.
package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// kalMkt builds a kalshi.Market through the JSON wire path (flexFloat fields are unexported).
func kalMkt(t *testing.T, raw string) kalshi.Market {
	t.Helper()
	var m kalshi.Market
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	return m
}

func TestR102KalEventParse(t *testing.T) {
	st := newGiState()
	league, mktType, away, home, start, etDate, ok := st.giParseKalEvent("KXMLBGAME-26JUL092145COLSF")
	if !ok || league != "mlb" || mktType != "winner" {
		t.Fatalf("parse: league=%q type=%q ok=%v", league, mktType, ok)
	}
	if away.abbr != "COL" || home.abbr != "SF" {
		t.Fatalf("pair: %s@%s, want COL@SF", away.abbr, home.abbr)
	}
	if etDate != "2026-07-09" {
		t.Fatalf("etDate=%q", etDate)
	}
	// 21:45 ET on Jul 9 (EDT, UTC−4) == 2026-07-10T01:45Z — the exact PUS startTime convention
	if want := time.Date(2026, 7, 10, 1, 45, 0, 0, time.UTC); !start.Equal(want) {
		t.Fatalf("start=%v, want %v", start, want)
	}
	// series typing: spread/total/prop split (bug-207 class stays split on the structural path)
	if _, mt, _, _, _, _, ok := st.giParseKalEvent("KXMLBSPREAD-26JUL092145COLSF"); !ok || mt != "spread" {
		t.Fatalf("KXMLBSPREAD type=%q ok=%v", mt, ok)
	}
	if _, mt, _, _, _, _, ok := st.giParseKalEvent("KXMLBTOTAL-26JUL092145COLSF"); !ok || mt != "total" {
		t.Fatalf("KXMLBTOTAL type=%q ok=%v", mt, ok)
	}
	if _, mt, _, _, _, _, ok := st.giParseKalEvent("KXMLBTEAMTOTAL-26JUL092145COLSF"); !ok || mt != "prop" {
		t.Fatalf("KXMLBTEAMTOTAL type=%q ok=%v (team totals must never pair with game totals)", mt, ok)
	}
	if _, _, _, _, _, _, ok := st.giParseKalEvent("KXBTCD-26JUL0913"); ok {
		t.Fatal("crypto daily parsed as a sports game")
	}
}

func TestR102SplitPairRefusesAmbiguity(t *testing.T) {
	st := newGiState()
	// craft a league where "AABBCC" splits two valid ways — identity must refuse
	st.learnTeam("xlg", "Alpha Alpha", "AA")
	st.learnTeam("xlg", "Bravo Bravo Charlie Charlie", "BBCC")
	st.learnTeam("xlg", "Alpha Bravo", "AABB")
	st.learnTeam("xlg", "Charlie", "CC")
	if _, _, ok := st.giSplitPair("xlg", "AABBCC"); ok {
		t.Fatal("two valid splits must refuse (never guess a game identity)")
	}
	// digit-leading abbrev (esports "100T" class) splits fine when unique
	st.learnTeam("xlg", "Hundred Thieves", "100T")
	st.learnTeam("xlg", "Evil Geniuses", "EG")
	if a, h, ok := st.giSplitPair("xlg", "100TEG"); !ok || a.abbr != "100T" || h.abbr != "EG" {
		t.Fatalf("digit-leading split: %v %v %v", a, h, ok)
	}
}

// anchorChiGame anchors the same-city fixture on both venues: Cubs @ White Sox, 19:10 ET Jul 9.
func anchorChiGame(t *testing.T, s *Server) (cubsSlug, soxSlug string) {
	t.Helper()
	cubsSlug, soxSlug = "atc-mlb-chc-cws-2026-07-09-chc", "atc-mlb-chc-cws-2026-07-09-cws"
	for _, tk := range []string{"CHC", "CWS"} {
		m := kalMkt(t, `{"ticker":"KXMLBGAME-26JUL091910CHCCWS-`+tk+`","event_ticker":"KXMLBGAME-26JUL091910CHCCWS","title":"Chicago Cubs vs Chicago White Sox Winner?","yes_sub_title":"`+map[string]string{"CHC": "Chicago Cubs", "CWS": "Chicago White Sox"}[tk]+`"}`)
		if !s.giAnchorKalshi(m) {
			t.Fatalf("kalshi anchor failed for %s", tk)
		}
	}
	team := func(name, ab, ord string) polymarketus.MarketSide {
		return polymarketus.MarketSide{Long: ord == "long", Team: polymarketus.Team{Name: name, Abbreviation: ab, League: "mlb", Ordering: map[string]string{"long": "away", "short": "home"}[ord]}}
	}
	ev := polymarketus.Event{ID: "ev-chi", Slug: "mlb-chc-cws-2026-07-09", GameID: 777, StartTime: "2026-07-09T23:10:00Z", // 19:10 EDT
		Markets: []polymarketus.Market{
			{Slug: cubsSlug, SportsType: "baseball_team_full_game_winner",
				Sides: []polymarketus.MarketSide{team("Chicago Cubs", "chc", "long"), team("Chicago White Sox", "cws", "short")}},
			{Slug: soxSlug, SportsType: "baseball_team_full_game_winner",
				Sides: []polymarketus.MarketSide{{Long: true, Team: polymarketus.Team{Name: "Chicago White Sox", Abbreviation: "cws", League: "mlb", Ordering: "home"}}, {Long: false, Team: polymarketus.Team{Name: "Chicago Cubs", Abbreviation: "chc", League: "mlb", Ordering: "away"}}}},
		}}
	s.giAnchorPUSEvent("mlb", ev)
	return cubsSlug, soxSlug
}

func TestR102SameCityStructurallyImpossible(t *testing.T) {
	s := &Server{}
	cubsSlug, soxSlug := anchorChiGame(t, s)
	// the struct join can NEVER cross same-city teams: CHC market → cubs slug, CWS → sox slug
	if slug, sameSide, ok := s.giPUSTwin("KXMLBGAME-26JUL091910CHCCWS-CHC"); !ok || slug != cubsSlug || !sameSide {
		t.Fatalf("CHC twin = (%q,%v,%v), want %q same-side", slug, sameSide, ok, cubsSlug)
	}
	if slug, _, ok := s.giPUSTwin("KXMLBGAME-26JUL091910CHCCWS-CWS"); !ok || slug != soxSlug {
		t.Fatalf("CWS twin = (%q,%v), want %q", slug, ok, soxSlug)
	}
	// side text: distinctive nickname resolves; bare shared city REFUSES
	gid, ok := s.giGameOfRef("KXMLBGAME-26JUL091910CHCCWS-CHC")
	if !ok {
		t.Fatal("game ref lookup failed")
	}
	if slug, flip, ok := s.giPUSSideMarket(gid, "Cubs"); !ok || slug != cubsSlug || flip {
		t.Fatalf("side 'Cubs' = (%q,flip=%v,%v), want %q flip=false", slug, flip, ok, cubsSlug)
	}
	if _, _, ok := s.giPUSSideMarket(gid, "Chicago"); ok {
		t.Fatal("bare 'Chicago' fits both teams — must refuse")
	}
}

func TestR102StructFirstMatchForSide(t *testing.T) {
	s := &Server{}
	cubsSlug, soxSlug := anchorChiGame(t, s)
	s.polyUSMkts = []polyUSMarket{
		{League: "mlb", EventID: "ev-chi", Game: "Chicago Cubs vs Chicago White Sox", Team: "CHC", TeamName: "Chicago Cubs",
			Slug: cubsSlug, Yes: 0.55, Bid: 0.54, Ask: 0.56},
		{League: "mlb", EventID: "ev-chi", Game: "Chicago Cubs vs Chicago White Sox", Team: "CWS", TeamName: "Chicago White Sox",
			Slug: soxSlug, Yes: 0.45, Bid: 0.44, Ask: 0.46},
	}
	const q = "Will the Chicago Cubs beat the Chicago White Sox?"
	// anchored ref ⇒ the STRUCTURAL path serves the match (counted as a struct hit, zero fuzz)
	slug, px, flip, ok := s.polyUSMatchForSide("arb", q, "Chicago Cubs", "KXMLBGAME-26JUL091910CHCCWS-CHC")
	if !ok || slug != cubsSlug || px != 0.55 || flip {
		t.Fatalf("struct-first = (%q,%v,flip=%v,%v), want %q@0.55 flip=false", slug, px, flip, ok, cubsSlug)
	}
	s.giMu.Lock()
	structN, fuzzN := s.gi().structHits["arb"], s.gi().fuzzHits["arb"]
	s.giMu.Unlock()
	if structN != 1 || fuzzN != 0 {
		t.Fatalf("counters struct=%d fuzz=%d, want 1/0", structN, fuzzN)
	}
	// no ref (unanchored question) ⇒ the guarded fuzzy path still works and is counted as fallback
	slug, _, _, ok = s.polyUSMatchForSide("arb", q, "Chicago White Sox")
	if !ok || slug != soxSlug {
		t.Fatalf("fuzzy fallback = (%q,%v), want %q", slug, ok, soxSlug)
	}
	s.giMu.Lock()
	fuzzN = s.gi().fuzzHits["arb"]
	s.giMu.Unlock()
	if fuzzN != 1 {
		t.Fatalf("fuzz counter=%d, want 1 (the auditor's fallback-rate numerator)", fuzzN)
	}
}

func TestR102Doubleheader(t *testing.T) {
	s := &Server{}
	// two games, same teams, same ET day: 13:05 and 18:30
	for _, ev := range []string{"KXMLBGAME-26JUL091305BOSNYY", "KXMLBGAME-26JUL091830BOSNYY"} {
		m := kalMkt(t, `{"ticker":"`+ev+`-BOS","event_ticker":"`+ev+`","title":"Boston vs New York Winner?","yes_sub_title":"Boston"}`)
		if !s.giAnchorKalshi(m) {
			t.Fatalf("anchor %s failed", ev)
		}
	}
	g1, ok1 := s.giGameOfRef("KXMLBGAME-26JUL091305BOSNYY")
	g2, ok2 := s.giGameOfRef("KXMLBGAME-26JUL091830BOSNYY")
	if !ok1 || !ok2 || g1 == g2 {
		t.Fatalf("doubleheader must be TWO canonical games: %q vs %q", g1, g2)
	}
	// the PUS evening game (18:30 ET = 22:30Z) anchors to game 2 by start proximity
	ev := polymarketus.Event{ID: "ev-dh2", Slug: "mlb-bos-nyy-2026-07-09", GameID: 888, StartTime: "2026-07-09T22:30:00Z",
		Markets: []polymarketus.Market{{Slug: "atc-mlb-bos-nyy-2026-07-09-g2-bos", SportsType: "baseball_team_full_game_winner",
			Sides: []polymarketus.MarketSide{
				{Long: true, Team: polymarketus.Team{Name: "Boston Red Sox", Abbreviation: "bos", League: "mlb", Ordering: "away"}},
				{Long: false, Team: polymarketus.Team{Name: "New York Yankees", Abbreviation: "nyy", League: "mlb", Ordering: "home"}}}}}}
	s.giAnchorPUSEvent("mlb", ev)
	if slug, _, ok := s.giPUSTwin("KXMLBGAME-26JUL091830BOSNYY-BOS"); !ok || slug != "atc-mlb-bos-nyy-2026-07-09-g2-bos" {
		t.Fatalf("evening kalshi row must twin the evening PUS row, got (%q,%v)", slug, ok)
	}
	if _, _, ok := s.giPUSTwin("KXMLBGAME-26JUL091305BOSNYY-BOS"); ok {
		t.Fatal("the 13:05 game has no PUS surface — a twin claim would be a fabricated wrong-game join")
	}
	// a TIMELESS event ticker against two same-day games must refuse to anchor (bug-206 doctrine)
	m := kalMkt(t, `{"ticker":"KXMLBGAME-26JUL09BOSNYY-BOS","event_ticker":"KXMLBGAME-26JUL09BOSNYY","yes_sub_title":"Boston"}`)
	if s.giAnchorKalshi(m) {
		t.Fatal("timeless ref + doubleheader day = ambiguous — must refuse")
	}
}

func TestR102TwinEquivalence(t *testing.T) {
	s := &Server{}
	anchorChiGame(t, s)
	// TOTAL: Kalshi floor_strike 8.5 (YES = over) ↔ PUS O/U 8.5 (long = Over) — same side
	mt := kalMkt(t, `{"ticker":"KXMLBTOTAL-26JUL091910CHCCWS-9","event_ticker":"KXMLBTOTAL-26JUL091910CHCCWS","yes_sub_title":"Over 8.5 runs scored","floor_strike":8.5,"strike_type":"greater"}`)
	if !s.giAnchorKalshi(mt) {
		t.Fatal("total anchor failed")
	}
	evT := polymarketus.Event{ID: "ev-chi", Slug: "mlb-chc-cws-2026-07-09", StartTime: "2026-07-09T23:10:00Z",
		Markets: []polymarketus.Market{{Slug: "tsc-mlb-chc-cws-2026-07-09-8pt5", SportsType: "baseball_team_full_game_total", Line: 8.5,
			Sides: []polymarketus.MarketSide{{Long: true, Description: "Over"}, {Long: false, Description: "Under"}}}}}
	s.giAnchorPUSEvent("mlb", evT)
	if slug, sameSide, ok := s.giPUSTwin("KXMLBTOTAL-26JUL091910CHCCWS-9"); !ok || slug != "tsc-mlb-chc-cws-2026-07-09-8pt5" || !sameSide {
		t.Fatalf("total twin = (%q,%v,%v)", slug, sameSide, ok)
	}
	// SPREAD: Kalshi "CWS wins by over 7.5" (yes_team CWS line −7.5) ↔ PUS "CHC +7.5" (long CHC) —
	// the SAME handicap listed from the other team ⇒ twin with FLIPPED sides (kal YES == pus NO)
	msp := kalMkt(t, `{"ticker":"KXMLBSPREAD-26JUL091910CHCCWS-CWS8","event_ticker":"KXMLBSPREAD-26JUL091910CHCCWS","yes_sub_title":"White Sox wins by over 7.5 runs","floor_strike":7.5,"strike_type":"greater"}`)
	if !s.giAnchorKalshi(msp) {
		t.Fatal("spread anchor failed")
	}
	evS := polymarketus.Event{ID: "ev-chi", Slug: "mlb-chc-cws-2026-07-09", StartTime: "2026-07-09T23:10:00Z",
		Markets: []polymarketus.Market{{Slug: "asc-mlb-chc-cws-2026-07-09-pos-7pt5", SportsType: "baseball_team_full_game_spread", Line: 7.5,
			Sides: []polymarketus.MarketSide{
				{Long: true, Description: "+7.50", Team: polymarketus.Team{Name: "Chicago Cubs", Abbreviation: "chc", League: "mlb", Ordering: "away"}},
				{Long: false, Description: "-7.50", Team: polymarketus.Team{Name: "Chicago White Sox", Abbreviation: "cws", League: "mlb", Ordering: "home"}}}}}}
	s.giAnchorPUSEvent("mlb", evS)
	slug, sameSide, ok := s.giPUSTwin("KXMLBSPREAD-26JUL091910CHCCWS-CWS8")
	if !ok || slug != "asc-mlb-chc-cws-2026-07-09-pos-7pt5" || sameSide {
		t.Fatalf("spread twin = (%q, sameSide=%v, %v), want flipped-side twin", slug, sameSide, ok)
	}
}

func TestR102PxAgeAggregator(t *testing.T) {
	s := &Server{}
	s.notePxAge("proposal", "ws", 40*time.Millisecond, 5*time.Second)
	s.notePxAge("proposal", "ws", 60*time.Millisecond, 7*time.Second)
	snap := s.pxAgeSnapshot()
	wsB, ok := snap["proposal|ws"].(map[string]any)
	if !ok || wsB["n"].(int64) != 2 {
		t.Fatalf("ws bucket missing/short: %+v", snap)
	}
	if med := wsB["median_ms"].(float64); med < 40 || med > 60 {
		t.Fatalf("ws median %v", med)
	}
	if _, ok := snap["proposal|rest_era"]; !ok {
		t.Fatal("rest-era baseline bucket missing (the before/after comparison depends on it)")
	}
}
