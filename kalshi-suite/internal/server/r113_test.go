package server

// r113_test.go — R113 live-board verification of the Jul-9 two-sided flip fuse (auditor 293/299,
// fixed R106/R108). The three fixtures below are REAL venue payloads captured 2026-07-08 from
// GET https://gateway.polymarket.us/v1/market/slug/{slug} (trimmed to the fields our decoder
// reads, prices as served): the FRA-MAR World Cup QF to-advance instrument that goes live Jul-9
// plus two already-live two-sided singles (WNBA IND-LA tipping 02:00Z Jul-9, MLB NYY-TB). Each
// walks the full pipeline — venue decode → structural anchor → shared matcher → logArbSignal —
// and then GRADES the row via storage.ResolveSignals, asserting the flipped short leg lands as
// side NO at the 1−YES price and pays out on the SHORT team's win, not the long team's.

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// Live payloads (gateway.polymarket.us, captured 2026-07-08; decoder-relevant fields only).
const (
	r113AdvJSON = `{
  "id":"188395",
  "question":"Which team will advance from France vs Morocco on 2026-07-09 4:00PM ET?",
  "slug":"aadc-fwc-fra-mar-2026-07-09-to-advance",
  "category":"sports","marketType":"moneyline",
  "sportsMarketType":"soccer_game_to_advance",
  "sportsMarketTypeV2":"SPORTS_MARKET_TYPE_MONEYLINE",
  "gameStartTime":"2026-07-09T20:00:00Z",
  "marketSides":[
    {"identifier":"aadc-fwc-fra-mar-2026-07-09-to-advance","description":"France","price":"0.7900","long":true,
     "team":{"name":"France","abbreviation":"fra","league":"fwc","ordering":"home","displayAbbreviation":"FRA"}},
    {"identifier":"aadc-fwc-fra-mar-2026-07-09-to-advance","description":"Morocco","price":"0.22","long":false,
     "team":{"name":"Morocco","abbreviation":"mar","league":"fwc","ordering":"away","displayAbbreviation":"MAR"}}
  ]
}`
	r113WnbaJSON = `{
  "id":"184027",
  "question":"Who will win in the upcoming basketball event Indiana vs Los Angeles scheduled for July 9, 2026 at 2:00 AM UTC?",
  "slug":"aec-wnba-ind-la-2026-07-08",
  "category":"sports","marketType":"moneyline",
  "sportsMarketType":"basketball_team_full_game_winner",
  "sportsMarketTypeV2":"SPORTS_MARKET_TYPE_MONEYLINE",
  "gameStartTime":"2026-07-09T02:00:00Z",
  "marketSides":[
    {"identifier":"aec-wnba-ind-la-2026-07-08","description":"Fever","price":"0.7000","long":true,
     "team":{"name":"Indiana","abbreviation":"ind","league":"wnba","ordering":"away","displayAbbreviation":"IND"}},
    {"identifier":"aec-wnba-ind-la-2026-07-08","description":"Sparks","price":"0.31","long":false,
     "team":{"name":"Los Angeles","abbreviation":"la","league":"wnba","ordering":"home","displayAbbreviation":"LA"}}
  ]
}`
	r113MlbJSON = `{
  "id":"192293",
  "question":"New York Yankees vs. Tampa Bay Rays",
  "slug":"aec-mlb-nyy-tb-2026-07-08",
  "category":"sports","marketType":"moneyline",
  "sportsMarketType":"baseball_team_full_game_winner",
  "sportsMarketTypeV2":"SPORTS_MARKET_TYPE_MONEYLINE",
  "gameStartTime":"2026-07-08T22:40:00Z",
  "marketSides":[
    {"identifier":"aec-mlb-nyy-tb-2026-07-08","description":"New York Yankees","price":"0.4650","long":true,
     "team":{"name":"New York Yankees","abbreviation":"nyy","league":"mlb","ordering":"away","displayAbbreviation":"NYY"}},
    {"identifier":"aec-mlb-nyy-tb-2026-07-08","description":"Tampa Bay Rays","price":"0.54","long":false,
     "team":{"name":"Tampa Bay Rays","abbreviation":"tb","league":"mlb","ordering":"home","displayAbbreviation":"TB"}}
  ]
}`
)

// TestR113LiveJul9FlipMatcherSignalGrade — matcher→signal→grade on the live Jul-9 board shapes.
func TestR113LiveJul9FlipMatcherSignalGrade(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		league    string
		evID      string
		gameID    int
		kalTicker string  // ref/label only (arb row's kalshi twin)
		wantLong  string  // long team name (its match must be flip=false at YES)
		wantShort string  // short team name (its match must be flip=true at 1−YES)
		toAdvance bool
		settleYes float64 // graded settle of the slug's YES (1 = long team won, 0 = short team won)
		wantWon   int     // the flipped NO row's expected won flag after grading
	}{
		{"fwc-fra-mar-advance", r113AdvJSON, "fwc", "ev-r113-framar", 91301,
			"KXWCADVANCE-26JUL09FRAMAR-MAR", "France", "Morocco", true,
			1.0, 0}, // France advances → the Morocco NO row LOSES
		{"wnba-ind-la", r113WnbaJSON, "wnba", "ev-r113-indla", 91302,
			"KXWNBAGAME-26JUL08INDLA-LA", "Indiana", "Los Angeles", false,
			0.0, 1}, // Sparks win → the flipped NO row WINS
		{"mlb-nyy-tb", r113MlbJSON, "mlb", "ev-r113-nyytb", 91303,
			"KXMLBGAME-26JUL08NYYTB-TB", "New York Yankees", "Tampa Bay Rays", false,
			0.0, 1}, // Rays win → the flipped NO row WINS
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var m polymarketus.Market
			if err := json.Unmarshal([]byte(c.raw), &m); err != nil {
				t.Fatalf("live payload decode: %v", err)
			}
			// VENUE SHAPE — the live board really is the two-sided single-slug instrument.
			if !m.TwoSidedSingle() {
				t.Fatal("live payload must classify TwoSidedSingle (both sides carry the market slug)")
			}
			if c.toAdvance != m.IsToAdvance() {
				t.Fatalf("IsToAdvance = %v, want %v", m.IsToAdvance(), c.toAdvance)
			}
			if lt, sh := m.LongTeam(), m.ShortTeam(); lt.Name != c.wantLong || sh.Name != c.wantShort {
				t.Fatalf("teams = long %q / short %q, want %q / %q", lt.Name, sh.Name, c.wantLong, c.wantShort)
			}
			yes := m.YesPrice()
			if yes <= 0.02 || yes >= 0.98 {
				t.Fatalf("live YES price %v out of band — refetch fixtures", yes)
			}

			// ANCHOR + SNAPSHOT — exactly how the refresh ingests this market (kind from venue
			// metadata, long+short teams stamped, price = the one shared book's YES).
			s := testServer(t)
			ev := polymarketus.Event{ID: c.evID, Slug: m.Slug, GameID: c.gameID,
				StartTime: m.GameStart, Markets: []polymarketus.Market{m}}
			s.giAnchorPUSEvent(c.league, ev)
			kind, q := "winner", m.Question
			if m.IsToAdvance() {
				kind, q = "advance", "To Advance"
			}
			s.polyUSMkts = []polyUSMarket{{
				League: c.league, EventID: c.evID, Game: m.Question,
				Team: m.LongTeam().Abbreviation, TeamName: m.LongTeam().Name,
				ShortTeam: m.ShortTeam().Abbreviation, ShortTeamName: m.ShortTeam().Name, TwoSided: true,
				Slug: m.Slug, Kind: kind, Question: q,
				Yes: yes, Bid: yes - 0.01, Ask: yes, Start: m.GameStart,
			}}

			// MATCHER — long side plain, short side flipped at 1−YES on the SAME slug.
			slug, px, flip, ok := s.polyUSMatchForSide("arb", m.Question, c.wantLong, m.Slug)
			if !ok || slug != m.Slug || flip || math.Abs(px-yes) > 1e-9 {
				t.Fatalf("long side = (%q,%v,flip=%v,%v), want %q@%v flip=false", slug, px, flip, ok, m.Slug, yes)
			}
			slug, px, flip, ok = s.polyUSMatchForSide("arb", m.Question, c.wantShort, m.Slug)
			if !ok || slug != m.Slug || !flip || math.Abs(px-(1-yes)) > 1e-9 {
				t.Fatalf("short side = (%q,%v,flip=%v,%v), want %q@%v FLIPPED", slug, px, flip, ok, m.Slug, 1-yes)
			}

			// SIGNAL — thread the matcher's flip exactly like the fixed sweepArbForward call site:
			// the flipped short leg must land as side NO at the side-adjusted price.
			ctx := context.Background()
			s.logArbSignal(ctx, "polyus", c.kalTicker, slug, m.Question, c.wantShort, px, math.Min(px+0.05, 0.97), 0.01, flip)
			if side, gotPx := r108FindSig(t, s, slug, "arb"); side != "NO" || math.Abs(gotPx-(1-yes)) > 1e-9 {
				t.Fatalf("stored row = side %q @ %v, want NO @ %v", side, gotPx, 1-yes)
			}

			// GRADE — settle the slug's YES; the NO row must pay on the SHORT team's win only.
			if err := s.store.ResolveSignals(ctx, slug, c.settleYes); err != nil {
				t.Fatalf("ResolveSignals: %v", err)
			}
			rows, err := s.store.ListSignals(ctx, 50)
			if err != nil {
				t.Fatalf("ListSignals: %v", err)
			}
			found := false
			for _, r := range rows {
				if r.Ticker != slug || r.SignalType != "arb" {
					continue
				}
				found = true
				if r.Resolved != 1 {
					t.Fatalf("row not resolved (resolved=%d)", r.Resolved)
				}
				if r.Won != c.wantWon {
					t.Fatalf("flipped NO row won=%d after settle YES=%v, want %d (side inversion!)", r.Won, c.settleYes, c.wantWon)
				}
			}
			if !found {
				t.Fatal("graded signal row not found")
			}
		})
	}
}
