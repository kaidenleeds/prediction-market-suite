package storage

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestComboAttributionRoundTripLeaderboardAndNoDuplicateGrade(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	legs := []paper.Leg{{Platform: "kalshi", Ticker: "A", Side: "YES", Entry: .4}, {Platform: "kalshi", Ticker: "B", Side: "NO", Entry: .5}}
	id, err := st.InsertParlay(ctx, paper.Parlay{Stake: 2, Price: .2, Contracts: 10, Fees: .1, Legs: legs,
		CanonicalSystemID: "parlay-2leg", ComboVenue: "kalshi", LegCount: 2, RelationClass: "independent",
		ProducerFamily: "positive-system", ComboRoute: "paper-combo-taker", ExperimentEpoch: "test-paper", ComboKey: "security-paper"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SettleParlay(ctx, id, 1, 7.9); err != nil {
		t.Fatal(err)
	}

	legsJSON := `[{"platform":"kalshi","ticker":"C","side":"YES"},{"platform":"kalshi","ticker":"D","side":"YES"}]`
	grade := PlabGradeReceipt{ComboID: "grade-1", CandidateAt: time.Now().Add(-time.Hour).Unix(),
		GradedTS: time.Now().UTC().Format(time.RFC3339Nano), Cell: "indep|2leg|x|legal|all-eligible",
		Bucket: "indep", Class: "x", Legality: "legal", Cohort: ComboLabCohortAllEligible,
		RouteState: "synthetic-settlement-only", NLegs: 2, Prod: .2, JointP: .3, Fees: .1, EntryCapital: 1.1,
		JointPayout: 1, RealizedReturn: 3.545454545, PredictedReturn: .363636, MVEValid: false,
		IndependenceKeys: []string{"c", "d"}, MarketKeys: []string{"kalshi|C", "kalshi|D"},
		LegsJSON: legsJSON, PayoutsJSON: `[1,1]`, CanonicalSystemID: "parlay-2leg", ComboVenue: "kalshi",
		RelationClass: "independent", ProducerFamily: "generic", ComboRoute: "synthetic-taker-chain",
		ExperimentEpoch: "test-lab", ComboKey: "security-lab"}
	if inserted, err := st.InsertPlabGradeReceipt(ctx, grade); err != nil || !inserted {
		t.Fatalf("first grade inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.InsertPlabGradeReceipt(ctx, grade); err != nil || inserted {
		t.Fatalf("duplicate grade inserted=%v err=%v", inserted, err)
	}

	stats, err := st.ComboSystemLeaderboard(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(stats) != 2 {
		t.Fatalf("got %d exact variants: %+v", len(stats), stats)
	}
	seen := map[string]UnitTrialLeaderboardStat{}
	for _, row := range stats {
		seen[row.EconomicSource] = row
	}
	if got := seen["combo-paper"]; got.N != 1 || got.SettledMarkets != 1 || got.ProducerFamily != "positive-system" || got.MeanPC != .79 {
		t.Fatalf("paper attribution=%+v", got)
	}
	if got := seen["combo-lab"]; got.N != 1 || got.SettledMarkets != 1 || got.ProducerFamily != "generic" || got.Route != "synthetic-taker-chain" {
		t.Fatalf("lab attribution=%+v", got)
	}
}

func TestPaperComboBackfillRefusesAmbiguousLegacy(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	exact, err := st.InsertParlay(ctx, paper.Parlay{Stake: 2, Price: .2, Contracts: 10, Legs: []paper.Leg{{Platform: "kalshi", Ticker: "A", Side: "YES"}, {Platform: "kalshi", Ticker: "B", Side: "NO"}}, RouteSource: FundedPositiveSystemComboRoute, Cohort: ComboLabCohortRollingPositive})
	if err != nil {
		t.Fatal(err)
	}
	ambiguous, err := st.InsertParlay(ctx, paper.Parlay{Stake: 2, Price: .2, Contracts: 10, Legs: []paper.Leg{{Platform: "kalshi", Ticker: "C", Side: "YES"}, {Platform: "kalshi", Ticker: "D", Side: "NO"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var exactSystem, ambiguousSystem string
	if err := st.db.QueryRow(`SELECT canonical_system_id FROM paper_parlays WHERE id=?`, exact).Scan(&exactSystem); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT canonical_system_id FROM paper_parlays WHERE id=?`, ambiguous).Scan(&ambiguousSystem); err != nil {
		t.Fatal(err)
	}
	if exactSystem != "parlay-2leg" || ambiguousSystem != "" {
		t.Fatalf("exact=%q ambiguous=%q", exactSystem, ambiguousSystem)
	}
}
