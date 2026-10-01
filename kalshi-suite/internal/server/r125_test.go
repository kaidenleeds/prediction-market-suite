package server

// R125 pins: (1) the PUS period-scope guard (partial-game markets must never twin full-game
// Kalshi markets — audit: 11/50 sampled joins were f5/fh/sh scope mismatches); (2) the signed
// spread line fix (venue now sends line=-16.5 on neg slugs; the legacy description sign-flip
// double-flipped them into the OPPOSITE handicap — audit: every neg-slug spread join was wrong);
// (3) lock-orientation margin math (both orientations, flip semantics, post-fee); (4) briefing
// plain-name fallbacks.

import (
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR125PusFullScope(t *testing.T) {
	for _, tc := range []struct {
		v1   string
		want bool
	}{
		{"baseball_team_full_game_total", true},
		{"basketball_team_full_game_spread", true},
		{"soccer_team_full_game_total", true},
		{"baseball_team_full_game_winner", true},
		{"soccer_game_to_advance", true},
		{"", false}, // current schema omission is fail-closed; removed V2 cannot restore scope
		{"baseball_team_first_five_total", false},
		{"baseball_team_first_five_spread", false},
		{"soccer_team_first_half_total", false},
		{"soccer_team_second_half_total", false},
		{"hockey_team_first_period_total", false}, // unknown partial scope: refused by allow-list
	} {
		if got := pusFullScope(tc.v1); got != tc.want {
			t.Errorf("pusFullScope(%q) = %v, want %v", tc.v1, got, tc.want)
		}
	}
}

// The real anchor path: neg/pos spreads twin the CORRECT Kalshi side; partial-scope markets
// anchor as prop and never twin; full-game totals still twin.
func TestR125PUSAnchorScopeAndSignedSpread(t *testing.T) {
	s := &Server{}
	st := s.gi()
	s.giMu.Lock()
	sea := st.learnTeam("wnba", "Seattle Storm", "SEA")
	atl := st.learnTeam("wnba", "Atlanta Dream", "ATL")
	start := time.Date(2026, 7, 9, 23, 0, 0, 0, time.UTC) // 19:00 EDT Jul 9
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

	side := func(long bool, desc, name, ab, ord string) polymarketus.MarketSide {
		return polymarketus.MarketSide{Long: long, Description: desc,
			Team: polymarketus.Team{Name: name, Abbreviation: ab, Ordering: ord}}
	}
	ev := polymarketus.Event{ID: "ev-sa", Slug: "wnba-sea-atl-2026-07-09", StartTime: "2026-07-09T23:00:00Z",
		Markets: []polymarketus.Market{
			{ // neg slug: SIGNED venue line (live-probed shape) — long side = SEA covering −16.5
				Slug:         "asc-wnba-sea-atl-2026-07-09-neg-16pt5",
				SportsType:   "basketball_team_full_game_spread",
				SportsTypeV2: "SPORTS_MARKET_TYPE_SPREAD", Line: -16.5,
				Sides: []polymarketus.MarketSide{
					side(true, "-16.50", "Seattle Storm", "SEA", "away"),
					side(false, "+16.50", "Atlanta Dream", "ATL", "home")}},
			{ // pos slug: underdog long +16.5 (ATL)
				Slug:         "asc-wnba-sea-atl-2026-07-09-pos-16pt5",
				SportsType:   "basketball_team_full_game_spread",
				SportsTypeV2: "SPORTS_MARKET_TYPE_SPREAD", Line: 16.5,
				Sides: []polymarketus.MarketSide{
					side(true, "+16.50", "Atlanta Dream", "ATL", "home"),
					side(false, "-16.50", "Seattle Storm", "SEA", "away")}},
			{ // partial-scope total (fh) — MUST anchor as prop, never twin
				Slug:         "tsc-wnba-sea-atl-2026-07-09-fh-79pt5",
				SportsType:   "basketball_team_first_half_total",
				SportsTypeV2: "SPORTS_MARKET_TYPE_TOTAL", Line: 79.5,
				Sides: []polymarketus.MarketSide{
					{Long: true, Description: "Over"}, {Long: false, Description: "Under"}}},
			{ // full-game total control — must still twin same-side
				Slug:         "tsc-wnba-sea-atl-2026-07-09-159pt5",
				SportsType:   "basketball_team_full_game_total",
				SportsTypeV2: "SPORTS_MARKET_TYPE_TOTAL", Line: 159.5,
				Sides: []polymarketus.MarketSide{
					{Long: true, Description: "Over"}, {Long: false, Description: "Under"}}},
		}}
	s.giAnchorPUSEvent("wnba", ev)

	// neg spread: P YES = SEA −16.5 ⇒ SAME SIDE as Kalshi "SEA by 17+" (pre-R125 this
	// double-flipped to ATL17 as a flip twin — the audit's phantom-edge class).
	if tk, sameSide, ok := s.giKalshiTwin("asc-wnba-sea-atl-2026-07-09-neg-16pt5"); !ok || tk != kSEA || !sameSide {
		t.Fatalf("neg spread twin = (%q, sameSide=%v, ok=%v), want (%q, true, true)", tk, sameSide, ok, kSEA)
	}
	// pos spread: P YES = ATL +16.5 ⇒ the COMPLEMENT (flip) of Kalshi "SEA by 17+".
	if tk, sameSide, ok := s.giKalshiTwin("asc-wnba-sea-atl-2026-07-09-pos-16pt5"); !ok || tk != kSEA || sameSide {
		t.Fatalf("pos spread twin = (%q, sameSide=%v, ok=%v), want (%q, false, true)", tk, sameSide, ok, kSEA)
	}
	// partial-scope market: refused from twinning entirely.
	if tk, _, ok := s.giKalshiTwin("tsc-wnba-sea-atl-2026-07-09-fh-79pt5"); ok {
		t.Fatalf("first-half total must NOT twin a full-game market, got %q", tk)
	}
	// full-game total control: still twins, same side.
	if tk, sameSide, ok := s.giKalshiTwin("tsc-wnba-sea-atl-2026-07-09-159pt5"); !ok || tk != kTOT || !sameSide {
		t.Fatalf("full-game total twin = (%q, sameSide=%v, ok=%v), want (%q, true, true)", tk, sameSide, ok, kTOT)
	}
}

// Lock orientation math: complement legs per side-semantics, post-fee margins exact (polyus fee
// model is pure arithmetic: taker 0.06·p·(1−p) banker's-rounded to the cent).
func TestR125LockOrientations(t *testing.T) {
	s := &Server{}
	near := func(a, b float64) bool { d := a - b; return d < 0.01 && d > -0.01 }

	// same-side pair: lock = YES_A+NO_B / NO_A+YES_B
	c := xvlCandidate{pair: "K-PUS", aVenue: "polyus", aID: "a", bVenue: "polyus", bID: "b",
		sameSide: true, aBid: 0.40, aAsk: 0.42, bBid: 0.50, bAsk: 0.52,
		aAskSz: 100, aBidSz: 90, bAskSz: 80, bBidSz: 70}
	outs := s.xvlOrientations(c)
	if len(outs) != 2 {
		t.Fatalf("want 2 orientations, got %d", len(outs))
	}
	// YESa(0.42)+NOb(1−0.50=0.50): fees 0.01+0.02 ⇒ margin (1−0.42−0.50−0.03)·100 = 5.0¢
	o := outs[0]
	if o.orient != "YESa+NOb" || o.aSide != "YES" || o.bSide != "NO" || !near(o.marginC, 5.0) {
		t.Fatalf("orientation 1 wrong: %+v", o)
	}
	if o.aDepth != 100 || o.bDepth != 70 { // YES ask size on A, NO liquidity = bid size on B
		t.Fatalf("orientation 1 depths wrong: %+v", o)
	}
	// NOa(0.60)+YESb(0.52): fees 0.01+0.01 ⇒ margin −14.0¢ (still computed, scanner filters)
	if o2 := outs[1]; o2.orient != "NOa+YESb" || !near(o2.marginC, -14.0) {
		t.Fatalf("orientation 2 wrong: %+v", o2)
	}

	// flip pair (R106: A YES ≡ B NO): lock = YES_A+YES_B / NO_A+NO_B
	c.sameSide = false
	outs = s.xvlOrientations(c)
	if len(outs) != 2 {
		t.Fatalf("flip: want 2 orientations, got %d", len(outs))
	}
	// YESa(0.42)+YESb(0.52): fees 0.01+0.01 ⇒ margin 4.0¢
	if o := outs[0]; o.orient != "YESa+YESb" || !near(o.marginC, 4.0) {
		t.Fatalf("flip orientation 1 wrong: %+v", o)
	}
	if o := outs[1]; o.orient != "NOa+NOb" || !near(o.marginC, -13.0) {
		t.Fatalf("flip orientation 2 wrong: %+v", o)
	}
}

func TestR125FamPlainName(t *testing.T) {
	for k, want := range map[string]string{
		"xvgap":          "cross-venue gap",
		"invert:pcrypto": "fade of poly crypto momentum",
		"book:xvgap":     "cross-venue gap book",
		"parlay-3leg":    "3-leg combos",
		"unknown-fam":    "unknown-fam",
	} {
		if got := famPlainName(k); got != want {
			t.Errorf("famPlainName(%q) = %q, want %q", k, got, want)
		}
	}
}
