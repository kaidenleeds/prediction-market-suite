package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

func TestVenueSettlementSurvivesRestartWithoutSignalRow(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	when := time.Now().UTC().Add(-time.Minute)
	if inserted, err := st.RecordVenueSettlement(ctx, "polyus", "NO-SIGNAL-SLUG", 1, when,
		PolyUSFinalBookSettlementV2); err != nil || !inserted {
		t.Fatalf("first receipt inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.RecordVenueSettlement(ctx, "polyus", "NO-SIGNAL-SLUG", 1, when,
		PolyUSFinalEndpointSettlementV2); err != nil || inserted {
		t.Fatalf("idempotent replay inserted=%v err=%v", inserted, err)
	}
	if _, err := st.RecordVenueSettlement(ctx, "polyus", "NO-SIGNAL-SLUG", 0, when,
		PolyUSFinalEndpointSettlementV2); err == nil {
		t.Fatal("contradictory terminal receipt silently rewrote final P&L truth")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got, ok := st.ResolvedYesForVenue(ctx, "polyus", "NO-SIGNAL-SLUG"); !ok || got != 1 {
		t.Fatalf("restart lost signal-independent settlement: got=%v ok=%v", got, ok)
	}
	if _, ok := st.ResolvedYesForVenue(ctx, "kalshi", "NO-SIGNAL-SLUG"); ok {
		t.Fatal("same ticker string leaked across venues")
	}
}

func TestUnitTrialClosesFromDurableVenueReceipt(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Now().UTC().Add(-time.Hour)
	inserted, err := st.InsertUnitTrial(ctx, UnitTrial{OpenedTS: opened, Family: "restart-test",
		Platform: "polyus", OriginLayer: "strategy", Ticker: "ROUTED-WITHOUT-SIGNAL", Side: "YES",
		Ask: .4, FeePC: .01, FeeKnown: true, FeeSource: "test exact fee", Depth: 5,
		QuoteSource: "test book", ResolveHours: 1})
	if err != nil || !inserted {
		t.Fatalf("unit inserted=%v err=%v", inserted, err)
	}
	if _, err := st.RecordVenueSettlement(ctx, "polyus", "ROUTED-WITHOUT-SIGNAL", 1,
		opened.Add(time.Hour), PolyUSFinalEndpointSettlementV2); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ResolveUnitTrialsFromSignals(ctx, 10); err != nil || n != 1 {
		t.Fatalf("durable receipt did not close unit trial: n=%d err=%v", n, err)
	}
}

func TestProperScoreResolvedTailCannotBeStarvedByOldOpenKeys(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	trials := make([]ProperScoreTrial, 0, 252)
	for i := 0; i < 252; i++ {
		trial := properStorageFixture("brier")
		trial.Ticker = fmt.Sprintf("PROPER-RESTART-%03d", i)
		trial.ForecastSignal = fmt.Sprintf("restart-%03d", i)
		trials = append(trials, trial)
	}
	freezeProperStorageManifests(t, st, trials...)
	for _, trial := range trials {
		if inserted, err := st.InsertProperScoreTrial(ctx, trial); err != nil || !inserted {
			t.Fatalf("insert %s inserted=%v err=%v", trial.Ticker, inserted, err)
		}
	}
	last := trials[len(trials)-1]
	if _, err := st.RecordVenueSettlement(ctx, "kalshi", last.Ticker, 1,
		time.Now().UTC().Add(time.Hour), "test tail settlement"); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ResolveProperScoreTrials(ctx, 1); err != nil || n != 1 {
		t.Fatalf("resolved tail starved behind 251 unresolved keys: n=%d err=%v", n, err)
	}
	var settled int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT settled FROM research_proper_score_trials
WHERE ticker=?`, last.Ticker).Scan(&settled); err != nil || settled != 1 {
		t.Fatalf("tail row settled=%d err=%v", settled, err)
	}
}

func TestSixLegComboLedgersSurviveRestartWithExactLegIdentity(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	legs := make([]paper.Leg, 0, 6)
	keys := make([]PlabLegKey, 0, 6)
	for i := 0; i < 6; i++ {
		ticker := fmt.Sprintf("KX-RESTART-COMBO-%d", i)
		side := "YES"
		if i%2 == 1 {
			side = "NO"
		}
		legs = append(legs, paper.Leg{Platform: "kalshi", Ticker: ticker, Side: side, Entry: .6})
		keys = append(keys, PlabLegKey{Platform: "kalshi", Ticker: ticker})
	}
	parlayID, err := st.InsertParlay(ctx, paper.Parlay{Stake: 3, Price: .6, Contracts: 5,
		Legs: legs, RouteSource: "r146-restart-test"})
	if err != nil {
		t.Fatal(err)
	}
	legsJSON, _ := json.Marshal(legs)
	feeJSON, _ := json.Marshal(make([]float64, 6))
	if n, err := st.PlabInsertBatch(ctx, []PlabCand{{ID: "r146-six-leg-restart", At: time.Now().Unix(),
		Bucket: "indep", Class: "restart", Legality: "paper", Prod: .6, JointP: .7,
		FeeSyn: .01, FeeMVE: -1, EVSyn: .1, EVMVE: -1000, LegFees: string(feeJSON),
		Legs: string(legsJSON), NLegs: 6, LegKeys: keys}}); err != nil || n != 1 {
		t.Fatalf("insert six-leg Combo Lab row: n=%d err=%v", n, err)
	}
	for i, key := range keys {
		if _, err := st.RecordVenueSettlement(ctx, key.Platform, key.Ticker, float64(i%2),
			time.Now().UTC(), "test exact combo leg"); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	open, err := st.ListParlays(ctx, "open")
	if err != nil || len(open) != 1 || open[0].ID != parlayID || len(open[0].Legs) != 6 {
		t.Fatalf("funded six-leg identity lost across restart: rows=%d err=%v row=%+v", len(open), err, open)
	}
	for i, key := range keys {
		yes, ok := st.ResolvedYesForVenue(ctx, key.Platform, key.Ticker)
		if !ok || yes != float64(i%2) {
			t.Fatalf("durable leg %d lost: yes=%g ok=%v", i, yes, ok)
		}
		if _, err := st.PlabResolveTicker(ctx, key.Platform, key.Ticker, yes); err != nil {
			t.Fatal(err)
		}
	}
	gradable, err := st.PlabGradable(ctx, time.Now().Add(time.Minute).Unix(), 10)
	if err != nil || len(gradable) != 1 || gradable[0].ID != "r146-six-leg-restart" {
		t.Fatalf("Combo Lab six-leg row did not recover: rows=%+v err=%v", gradable, err)
	}
	vals, err := st.PlabLegVals(ctx, []string{"r146-six-leg-restart"})
	if err != nil || len(vals["r146-six-leg-restart"]) != 6 {
		t.Fatalf("Combo Lab exact legs lost: vals=%+v err=%v", vals, err)
	}
}
