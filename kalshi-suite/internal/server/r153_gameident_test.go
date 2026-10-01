package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR153KalshiQualifiedSeriesTyping(t *testing.T) {
	tests := []struct {
		series, league, marketType string
		ok                         bool
	}{
		{series: "KXNBASUMMERGAME", league: "nba", marketType: "winner", ok: true},
		{series: "KXNBASUMMERSPREAD", league: "nba", marketType: "spread", ok: true},
		{series: "KXNBASUMMERTOTAL", league: "nba", marketType: "total", ok: true},
		{series: "KXNBASUMMERTEAMTOTAL", league: "nba", marketType: "prop", ok: true},
		{series: "KXNBASUMMER1HWINNER", league: "nba", marketType: "prop", ok: true},
		{series: "KXWNBA2QSPREAD", league: "wnba", marketType: "prop", ok: true},
		{series: "KXCS2MAP", league: "cs2", marketType: "prop", ok: true},
		{series: "KXATPSETWINNER", league: "atp", marketType: "prop", ok: true},
		{series: "KXMLBF3", league: "mlb", marketType: "prop", ok: true},
		{series: "KXMLBF5", league: "mlb", marketType: "prop", ok: true},
		{series: "KXMLBF7", league: "mlb", marketType: "prop", ok: true},
		{series: "KXNBAPRESEASONGAME", league: "nba", marketType: "prop", ok: true},
		{series: "KXNBASUMMERUNKNOWN", ok: false},
		{series: "KXUNKNOWNGAME", ok: false},
	}
	for _, tc := range tests {
		t.Run(tc.series, func(t *testing.T) {
			league, marketType, ok := giKalSeries(tc.series)
			if ok != tc.ok || league != tc.league || marketType != tc.marketType {
				t.Fatalf("giKalSeries(%q) = (%q,%q,%v), want (%q,%q,%v)",
					tc.series, league, marketType, ok, tc.league, tc.marketType, tc.ok)
			}
		})
	}
}

func TestR153QualifiedSeriesAnchorToFixtureButRemainConservative(t *testing.T) {
	s := &Server{}
	s.giMu.Lock()
	st := s.gi()
	st.learnTeam("cs2", "Alpha", "AAA")
	st.learnTeam("cs2", "Bravo", "BBB")
	st.learnTeam("atp", "Player Alpha", "AAA")
	st.learnTeam("atp", "Player Bravo", "BBB")
	s.giMu.Unlock()

	type fixtureCase struct {
		name    string
		markets []kalshi.Market
		types   []string
	}
	cases := []fixtureCase{
		{
			name: "NBA Summer full game and period",
			markets: []kalshi.Market{
				{Ticker: "KXNBASUMMERGAME-26JUL16BKNHOU-BKN", EventTicker: "KXNBASUMMERGAME-26JUL16BKNHOU", YesSubTitle: "Brooklyn"},
				{Ticker: "KXNBASUMMERSPREAD-26JUL16BKNHOU-HOU4", EventTicker: "KXNBASUMMERSPREAD-26JUL16BKNHOU", YesSubTitle: "Houston wins by over 3.5 points"},
				{Ticker: "KXNBASUMMERTOTAL-26JUL16BKNHOU-183", EventTicker: "KXNBASUMMERTOTAL-26JUL16BKNHOU", YesSubTitle: "Over 182.5 points"},
				{Ticker: "KXNBASUMMER1HWINNER-26JUL16BKNHOU-BKN", EventTicker: "KXNBASUMMER1HWINNER-26JUL16BKNHOU", YesSubTitle: "Brooklyn wins 1st half"},
			},
			types: []string{"winner", "spread", "total", "prop"},
		},
		{
			name: "period",
			markets: []kalshi.Market{
				{Ticker: "KXWNBAGAME-26JUL16NYDAL-NY", EventTicker: "KXWNBAGAME-26JUL16NYDAL", YesSubTitle: "New York"},
				{Ticker: "KXWNBA2QSPREAD-26JUL16NYDAL-NY2", EventTicker: "KXWNBA2QSPREAD-26JUL16NYDAL", YesSubTitle: "New York wins 2Q"},
			},
			types: []string{"winner", "prop"},
		},
		{
			name: "map",
			markets: []kalshi.Market{
				{Ticker: "KXCS2GAME-26JUL161500AAABBB-AAA", EventTicker: "KXCS2GAME-26JUL161500AAABBB", YesSubTitle: "Alpha"},
				{Ticker: "KXCS2MAP-26JUL161500AAABBB-AAA", EventTicker: "KXCS2MAP-26JUL161500AAABBB", YesSubTitle: "Alpha"},
			},
			types: []string{"winner", "prop"},
		},
		{
			name: "set",
			markets: []kalshi.Market{
				{Ticker: "KXATPMATCH-26JUL161100AAABBB-AAA", EventTicker: "KXATPMATCH-26JUL161100AAABBB", YesSubTitle: "Player Alpha"},
				{Ticker: "KXATPSETWINNER-26JUL161100AAABBB-AAA", EventTicker: "KXATPSETWINNER-26JUL161100AAABBB", YesSubTitle: "Player Alpha"},
			},
			types: []string{"winner", "prop"},
		},
		{
			name: "baseball F3 F5 F7",
			markets: []kalshi.Market{
				{Ticker: "KXMLBGAME-26JUL161200BOSNYY-BOS", EventTicker: "KXMLBGAME-26JUL161200BOSNYY", YesSubTitle: "Boston"},
				{Ticker: "KXMLBF3-26JUL161200BOSNYY-BOS", EventTicker: "KXMLBF3-26JUL161200BOSNYY", YesSubTitle: "Boston"},
				{Ticker: "KXMLBF5-26JUL161200BOSNYY-BOS", EventTicker: "KXMLBF5-26JUL161200BOSNYY", YesSubTitle: "Boston"},
				{Ticker: "KXMLBF7-26JUL161200BOSNYY-BOS", EventTicker: "KXMLBF7-26JUL161200BOSNYY", YesSubTitle: "Boston"},
			},
			types: []string{"winner", "prop", "prop", "prop"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i, market := range tc.markets {
				if !s.giAnchorKalshi(market) {
					t.Fatalf("market %s did not anchor", market.Ticker)
				}
				s.giMu.Lock()
				ref := s.gi().byMkt["kalshi|"+market.Ticker]
				s.giMu.Unlock()
				if ref == nil || ref.mktType != tc.types[i] {
					t.Fatalf("market %s ref = %+v, want type %s", market.Ticker, ref, tc.types[i])
				}
			}
			firstGame, ok := s.giGameOfRef(tc.markets[0].Ticker)
			if !ok {
				t.Fatalf("base market %s has no game", tc.markets[0].Ticker)
			}
			for _, market := range tc.markets[1:] {
				game, found := s.giGameOfRef(market.Ticker)
				if !found || game != firstGame {
					t.Fatalf("market %s game = %q/%v, want shared fixture %q", market.Ticker, game, found, firstGame)
				}
			}
		})
	}
}
