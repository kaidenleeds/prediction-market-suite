package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR139ScorePredicateCanonicalizesWinnerSpreadAndTotal(t *testing.T) {
	game := storage.GameIdentityRow{GameID: "g", Home: "home", Away: "away"}
	tests := []struct {
		row       storage.MarketGameRow
		axis      string
		threshold float64
		greater   bool
	}{
		{storage.MarketGameRow{MktType: "winner", YesTeam: "home"}, "margin", 0, true},
		{storage.MarketGameRow{MktType: "winner", YesTeam: "away"}, "margin", 0, false},
		{storage.MarketGameRow{MktType: "spread", YesTeam: "home", Line: -3.5}, "margin", 3.5, true},
		{storage.MarketGameRow{MktType: "spread", YesTeam: "away", Line: 3.5}, "margin", 3.5, false},
		{storage.MarketGameRow{MktType: "total", YesTeam: "over", Line: 220.5}, "total", 220.5, true},
		{storage.MarketGameRow{MktType: "total", YesTeam: "under", Line: 220.5}, "total", 220.5, false},
	}
	for _, tt := range tests {
		got, err := r139ScorePredicate(tt.row, game)
		if err != nil || got.Axis != tt.axis || got.Threshold != tt.threshold || got.Greater != tt.greater {
			t.Fatalf("row=%+v got=%+v err=%v", tt.row, got, err)
		}
	}
	if _, err := r139ScorePredicate(storage.MarketGameRow{MktType: "prop", YesTeam: "home"}, game); err == nil {
		t.Fatal("props must stay blocked until a correlation model is frozen")
	}
}

func TestR139FairGamePageWrapsWithoutStarvation(t *testing.T) {
	rows := func(id string) []storage.MarketGameRow {
		return []storage.MarketGameRow{{GameID: id, MktType: "winner"}, {GameID: id, MktType: "spread"}}
	}
	groups := map[string][]storage.MarketGameRow{"a": rows("a"), "b": rows("b"), "c": rows("c")}
	first, cursor := r139FairGamePage(groups, "", 2)
	if len(first) != 2 || first[0] != "a" || first[1] != "b" || cursor != "b" {
		t.Fatalf("first page=%v cursor=%q", first, cursor)
	}
	second, cursor := r139FairGamePage(groups, cursor, 2)
	if len(second) != 2 || second[0] != "c" || second[1] != "a" || cursor != "a" {
		t.Fatalf("wrapped page=%v cursor=%q", second, cursor)
	}
}
