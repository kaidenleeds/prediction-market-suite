package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR146SoccerFullTimeScopeAndOutcomeCardinality(t *testing.T) {
	if !pusFullScope("soccer_team_full_time_winner") {
		t.Fatal("current PolyUS soccer full-time winner scope must anchor as a full-game winner")
	}
	if pusFullScope("soccer_team_first_half_winner") {
		t.Fatal("partial soccer winner must remain fail-closed")
	}
	if got := objectiveSportsPeriod("soccer_team_full_time_winner", "winner"); got != "full_game" {
		t.Fatalf("full-time winner period = %q, want full_game", got)
	}
	for _, lg := range []string{"soccer", "mls", "ucl", "epl", "fwc", "bun"} {
		if got := objectiveSportsOutcomeCardinality(lg, "soccer_team_full_time_winner", "winner"); got != 3 {
			t.Errorf("%s outcome cardinality = %d, want 3", lg, got)
		}
	}
	if got := objectiveSportsOutcomeCardinality("fwc", "soccer_game_to_advance", "advance"); got != 2 {
		t.Fatalf("to-advance cardinality = %d, want 2", got)
	}
}

func TestR146ObjectiveAdapterDoesNotInvertDifferentThreeWaySelections(t *testing.T) {
	pair := storage.StructuralRulePair{
		PairID: "k-pus|K-SOCCER-HOME|pus-away", PairType: "K-PUS",
		GameID: "mls:2026-07-15:SEA@POR", MarketType: "winner", League: "mls",
		Away: "SEA", Home: "POR", LeftVenue: "kalshi", LeftInstrumentID: "K-SOCCER-HOME",
		RightVenue: "polyus", RightInstrumentID: "pus-away", LeftYesTeam: "POR", RightYesTeam: "SEA",
		CanonicalEventID: "sports:mls:2026-07-15:SEA@POR",
	}
	artifact := objectiveTestArtifact("official")
	artifact.SportsType = "soccer_team_full_time_winner"
	artifact.TiePolicy = "draw-is-third-outcome"
	left := objectivePairContract(pair, true, artifact)
	right := objectivePairContract(pair, false, artifact)
	if left.OutcomeCardinality != 3 || right.OutcomeCardinality != 3 || left.Opposite != "" || right.Opposite != "" {
		t.Fatalf("three-way contracts carry false complement metadata: left=%+v right=%+v", left, right)
	}
	if v := objectiveidentity.Evaluate(left, right); v.Compatible || v.ReasonCode != objectiveidentity.RejectSelection {
		t.Fatalf("YES(Home) and YES(Away) were incorrectly treated as inverse: %+v", v)
	}
	pair.RightYesTeam = "POR"
	right = objectivePairContract(pair, false, artifact)
	if v := objectiveidentity.Evaluate(left, right); !v.Compatible || v.Orientation != "same" {
		t.Fatalf("same soccer outcome must still cross-match: %+v", v)
	}
}

func TestR146PolyUSSoccerDrawAnchorsAsDistinctWinner(t *testing.T) {
	s := &Server{}
	scope := "soccer_team_full_time_winner"
	market := func(slug, name, abbr, ordering string) polymarketus.Market {
		sides := []polymarketus.MarketSide{{Long: true, Description: "Yes"}}
		if name != "" || abbr != "" {
			sides[0].Team = polymarketus.Team{Name: name, Abbreviation: abbr, DisplayAbbr: abbr, Ordering: ordering}
		}
		return polymarketus.Market{Slug: slug, SportsType: scope, Sides: sides}
	}
	homeSlug := "atc-mls-sea-por-2026-07-15-por"
	awaySlug := "atc-mls-sea-por-2026-07-15-sea"
	drawSlug := "atc-mls-sea-por-2026-07-15-draw"
	ev := polymarketus.Event{ID: "mls-sea-por", Slug: "mls-sea-por-2026-07-15", StartTime: "2026-07-16T02:30:00Z",
		Markets: []polymarketus.Market{
			market(awaySlug, "Seattle Sounders", "SEA", "away"),
			market(homeSlug, "Portland Timbers", "POR", "home"),
			market(drawSlug, "", "", ""),
		}}
	s.giAnchorPUSEvent("mls", ev)

	s.giMu.Lock()
	st := s.gi()
	drawRef := st.byMkt["polyus|"+drawSlug]
	homeRef := st.byMkt["polyus|"+homeSlug]
	awayRef := st.byMkt["polyus|"+awaySlug]
	s.giMu.Unlock()
	if drawRef == nil || homeRef == nil || awayRef == nil {
		t.Fatalf("three distinct contracts were not all anchored: home=%+v draw=%+v away=%+v", homeRef, drawRef, awayRef)
	}
	if drawRef.yesTeam != "draw" || drawRef.mktType != "winner" ||
		drawRef.gameID != homeRef.gameID || drawRef.gameID != awayRef.gameID {
		t.Fatalf("Home/Draw/Away must be distinct winner contracts under one event: home=%+v draw=%+v away=%+v", homeRef, drawRef, awayRef)
	}
	if slug, flip, ok := s.giPUSSideMarket(drawRef.gameID, "Draw"); !ok || slug != drawSlug || flip {
		t.Fatalf("Draw side lookup = (%q, flip=%v, ok=%v), want exact Draw YES contract", slug, flip, ok)
	}
	if slug, flip, ok := s.giPUSSideMarket(homeRef.gameID, "Portland Timbers"); !ok || slug != homeSlug || flip {
		t.Fatalf("Home side lookup = (%q, flip=%v, ok=%v), want exact Home YES contract", slug, flip, ok)
	}
	// One match contributes three distinct venue+ticker contracts to contract-level n, while the
	// shared gameID keeps all three in one dependency/exposure cluster.
	if homeSlug == awaySlug || homeSlug == drawSlug || awaySlug == drawSlug {
		t.Fatal("fixture must preserve three distinct contract identities")
	}
}

func TestR146PolyUSDutchAcceptsStructuredBlankTeamDrawOnly(t *testing.T) {
	rows := []polyUSMarket{
		r132PUSDutchRow("soccer-blank-draw", "fwc", "winner", "FRA", "atc-fwc-fra-mar-2026-07-15-fra"),
		r132PUSDutchRow("soccer-blank-draw", "fwc", "winner", "MAR", "atc-fwc-fra-mar-2026-07-15-mar"),
		r132PUSDutchRow("soccer-blank-draw", "fwc", "winner", "", "atc-fwc-fra-mar-2026-07-15-draw"),
	}
	for i := range rows {
		rows[i].SportsScope = "soccer_team_full_time_winner"
	}
	if got := completePolyUSDutchSets(rows); len(got) != 1 || len(got[0].legs) != 3 {
		t.Fatalf("authoritative blank-team Draw row was dropped: %+v", got)
	}
	rows[2].SportsScope = "baseball_team_full_game_winner"
	if polyUSStructuredDraw(rows[2].SportsScope, rows[2].Slug) {
		t.Fatal("a -draw slug outside an exact soccer winner scope must not become Draw evidence")
	}
}

func TestR146SoccerRestartFoldPreservesTickerAndSideSettlementIdentity(t *testing.T) {
	// A three-way match is three distinct binary contracts. Persisted Paper fills are replayed
	// after every restart, so this pins the exact fold key used by that reconstruction:
	// venue+ticker+side. In particular, NO(Home) is not YES(Away), and Draw is its own ticker.
	const (
		home = "K-SOCCER-HOME"
		draw = "K-SOCCER-DRAW"
		away = "K-SOCCER-AWAY"
	)
	buy := func(ts, ticker, side string) paper.Fill {
		return paper.Fill{TS: ts, Platform: "kalshi", Ticker: ticker, Side: side,
			Action: "BUY", Price: .40, Contracts: 1, Source: "soccer-restart-test"}
	}
	fills := []paper.Fill{
		buy("2026-07-15T00:00:01Z", home, "YES"),
		buy("2026-07-15T00:00:02Z", home, "NO"),
		buy("2026-07-15T00:00:03Z", draw, "YES"),
		buy("2026-07-15T00:00:04Z", away, "YES"),
	}
	positions, _ := paper.Aggregate(append([]paper.Fill(nil), fills...))
	if len(positions) != 4 {
		t.Fatalf("restart reconstruction merged soccer contract sides: positions=%+v", positions)
	}

	// The game draws. Settle NO(Home) first using the Home ticker's own YES value (0 -> NO pays 1).
	// Its sibling YES contracts must remain open until each exact ticker receives its own settlement.
	settleHomeNO := paper.Fill{TS: "2026-07-15T01:00:00Z", Platform: "kalshi", Ticker: home,
		Side: "NO", Action: "SELL", Price: 1, Contracts: 1, Source: "settled-win"}
	fills = append(fills, settleHomeNO)
	positions, _ = paper.Aggregate(append([]paper.Fill(nil), fills...))
	if len(positions) != 3 {
		t.Fatalf("settling NO(Home) closed a sibling outcome: positions=%+v", positions)
	}
	for _, p := range positions {
		if p.Ticker == home && p.Side == "NO" {
			t.Fatalf("NO(Home) remained open after its exact settlement: %+v", p)
		}
	}

	// A duplicate settlement replay after a crash cannot mint another closed trade.
	dup := settleHomeNO
	dup.TS = "2026-07-15T01:00:01Z"
	withDuplicate := append(append([]paper.Fill(nil), fills...), dup)
	if got := paper.Stats(withDuplicate).ClosedTrades; got != 1 {
		t.Fatalf("duplicate restart settlement produced %d closed trades, want 1", got)
	}

	// Finish every sibling against its own ticker. A draw pays Home YES=0, Draw YES=1, Away YES=0.
	for i, v := range []struct {
		ticker string
		price  float64
	}{{home, 0}, {draw, 1}, {away, 0}} {
		fills = append(fills, paper.Fill{TS: "2026-07-15T01:00:1" + string(rune('0'+i)) + "Z",
			Platform: "kalshi", Ticker: v.ticker, Side: "YES", Action: "SELL",
			Price: v.price, Contracts: 1, Source: "settled"})
	}
	positions, _ = paper.Aggregate(append([]paper.Fill(nil), fills...))
	if len(positions) != 0 {
		t.Fatalf("exact sibling settlements left positions open: %+v", positions)
	}
	if got := paper.Stats(fills).ClosedTrades; got != 4 {
		t.Fatalf("settled trade count = %d, want four exact ticker+side lots", got)
	}
}
