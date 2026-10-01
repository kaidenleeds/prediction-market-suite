package server

import "testing"

func r132PUSDutchRow(eventID, league, kind, team, slug string) polyUSMarket {
	return polyUSMarket{
		EventID: eventID, League: league, Kind: kind, Team: team, TeamName: team,
		Slug: slug, Game: "A vs B", SportsScope: league + "_team_full_game_winner",
		Bid: 0.54, Ask: 0.55, BidSz: 3, AskSz: 4,
	}
}

func TestR132PolyUSDutchUsesOnlyCompleteWinnerSets(t *testing.T) {
	rows := []polyUSMarket{
		r132PUSDutchRow("game-1", "mlb", "winner", "A", "a-win"),
		r132PUSDutchRow("game-1", "mlb", "winner", "B", "b-win"),
		// These share EventID but are not mutually-exclusive outcomes. The pre-R132
		// scanner included all of them and manufactured multi-dollar "locks".
		r132PUSDutchRow("game-1", "mlb", "spread", "A -1.5", "a-spread"),
		r132PUSDutchRow("game-1", "mlb", "total", "Over 8.5", "over-total"),
		r132PUSDutchRow("game-1", "mlb", "prop", "Player hit", "player-prop"),
	}
	sets := completePolyUSDutchSets(rows)
	if len(sets) != 1 || sets[0].eventID != "game-1" || len(sets[0].legs) != 2 {
		t.Fatalf("complete winner set = %+v, want one two-outcome set with non-winners excluded", sets)
	}

	for name, mutate := range map[string]func([]polyUSMarket) []polyUSMarket{
		"missing outcome":                func(in []polyUSMarket) []polyUSMarket { return in[1:] },
		"unknown league":                 func(in []polyUSMarket) []polyUSMarket { in[0].League, in[1].League = "unknown", "unknown"; return in },
		"two-sided singleton vocabulary": func(in []polyUSMarket) []polyUSMarket { in[0].TwoSided = true; return in },
		"missing depth":                  func(in []polyUSMarket) []polyUSMarket { in[1].AskSz = 0; return in },
		"duplicate outcome":              func(in []polyUSMarket) []polyUSMarket { in[1].Team, in[1].TeamName = "A", "A"; return in },
		"mixed payoff scope": func(in []polyUSMarket) []polyUSMarket {
			in[1].SportsScope = "baseball_team_first_five_winner"
			return in
		},
	} {
		t.Run(name, func(t *testing.T) {
			base := append([]polyUSMarket(nil), rows[:2]...)
			if got := completePolyUSDutchSets(mutate(base)); len(got) != 0 {
				t.Fatalf("invalid/partial set passed: %+v", got)
			}
		})
	}
}

func TestR132PolyUSDutchThreeWayRequiresExplicitDraw(t *testing.T) {
	rows := []polyUSMarket{
		r132PUSDutchRow("soccer-1", "soccer", "winner", "Home", "home"),
		r132PUSDutchRow("soccer-1", "soccer", "winner", "Away", "away"),
		r132PUSDutchRow("soccer-1", "soccer", "winner", "Draw", "draw"),
	}
	if got := completePolyUSDutchSets(rows); len(got) != 1 || len(got[0].legs) != 3 {
		t.Fatalf("explicit three-way set rejected: %+v", got)
	}
	rows[2].Team, rows[2].TeamName, rows[2].Question = "Other", "Other", "Other"
	if got := completePolyUSDutchSets(rows); len(got) != 0 {
		t.Fatalf("three-way set without explicit draw/tie must fail closed: %+v", got)
	}
}
