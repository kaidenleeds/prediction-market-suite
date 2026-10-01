package polymarketus

import "testing"

func TestCurrentSportsMarketTypeWinsDeprecatedV2(t *testing.T) {
	cases := []struct {
		current, legacy, want string
	}{
		{"basketball_team_full_game_winner", "SPORTS_MARKET_TYPE_TOTAL", "winner"},
		{"baseball_team_first_five_total", "SPORTS_MARKET_TYPE_MONEYLINE", "total"},
		{"soccer_team_full_game_spread", "SPORTS_MARKET_TYPE_PROP", "spread"},
		{"soccer_game_to_advance", "SPORTS_MARKET_TYPE_MONEYLINE", "advance"},
		{"soccer_game_first_half_btts", "SPORTS_MARKET_TYPE_MONEYLINE", "prop"},
		{"", "SPORTS_MARKET_TYPE_TOTAL", ""}, // removed field cannot classify a live payload
	}
	for _, tc := range cases {
		m := Market{SportsType: tc.current, SportsTypeV2: tc.legacy}
		if got := m.SportsKind(); got != tc.want {
			t.Errorf("current=%q legacy=%q got=%q want=%q", tc.current, tc.legacy, got, tc.want)
		}
	}
}
