package storage

import (
	"context"
	"testing"
	"time"
)

func registerUnitTrialEventFixture(t *testing.T, st *Store, eventID string, tickers ...string) {
	t.Helper()
	payoffs := make([]CanonicalPayoffSpec, 0, len(tickers))
	instruments := make([]CanonicalInstrumentSpec, 0, len(tickers))
	for _, ticker := range tickers {
		payoffID := eventID + "|" + ticker
		payoffs = append(payoffs, CanonicalPayoffSpec{
			PayoffID: payoffID, EventID: eventID, Label: ticker, PredicateJSON: `{"fixture":true}`,
			PayoutFloor: 0, PayoutCeiling: 1, SourceArtifact: "test fixture", IdentityStatus: "structural",
		})
		instruments = append(instruments, CanonicalInstrumentSpec{
			Venue: "kalshi", Ticker: ticker, EventID: eventID, PayoffID: payoffID,
			NativeSide: "YES", Orientation: "same", MarketKind: "fixture", Scope: "full_game",
			RulesArtifact: "test fixture", IdentityStatus: "structural",
		})
	}
	if _, err := st.RegisterCanonicalBatch(context.Background(), []CanonicalEventSpec{{
		EventID: eventID, EventType: "sports-game", Domain: "test", Title: "same game",
		SourceArtifact: "test fixture", SourceClockID: "fixture", OutcomeSetStatus: "incomplete",
	}}, payoffs, instruments); err != nil {
		t.Fatal(err)
	}
}

func TestR144UnitTrialsFreezeCanonicalEventAndNeverGuessMissingHistory(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	eventID := "sports:test:2026-07-14:AWAY@HOME"
	registerUnitTrialEventFixture(t, st, eventID, "SPREAD", "TOTAL")
	now := unitTrialUTCDay(time.Now().UTC().Add(-48 * time.Hour))
	for _, ticker := range []string{"SPREAD", "TOTAL", "UNKNOWN-PROP"} {
		inserted, insertErr := st.InsertUnitTrial(ctx, UnitTrial{
			OpenedTS: now, Family: "same-game", Platform: "kalshi", Ticker: ticker, Side: "YES",
			Ask: .40, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws",
		})
		if insertErr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", ticker, inserted, insertErr)
		}
	}
	var spreadID, totalID, unknownID string
	var spreadV, totalV, unknownV int
	if err := st.db.QueryRow(`SELECT canonical_event_id,event_version FROM unit_trials WHERE ticker='SPREAD'`).Scan(&spreadID, &spreadV); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT canonical_event_id,event_version FROM unit_trials WHERE ticker='TOTAL'`).Scan(&totalID, &totalV); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT canonical_event_id,event_version FROM unit_trials WHERE ticker='UNKNOWN-PROP'`).Scan(&unknownID, &unknownV); err != nil {
		t.Fatal(err)
	}
	if spreadID != eventID || totalID != eventID || spreadV <= 0 || totalV != spreadV || unknownID != "" || unknownV != 0 {
		t.Fatalf("frozen identity spread=%q/v%d total=%q/v%d unknown=%q/v%d",
			spreadID, spreadV, totalID, totalV, unknownID, unknownV)
	}
	closed := now.Add(time.Hour).Format(time.RFC3339Nano)
	for ticker, pnl := range map[string]float64{"SPREAD": .10, "TOTAL": .30, "UNKNOWN-PROP": -.90} {
		if _, err := st.db.Exec(`UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=?,return_per_dollar=? WHERE ticker=?`,
			closed, pnl, pnl/.4, ticker); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	if got := stats[0]; got.SettledMarkets != 3 || got.SettledEventClusters != 1 || got.UnclusteredSettledMarkets != 1 {
		t.Fatalf("related contracts were counted as independent events or unknown identity was guessed: %+v", got)
	}
	if got := stats[0]; got.ClusteredSettledMarkets != 2 || got.EventClusterMeanPC < .1999 || got.EventClusterMeanPC > .2001 {
		t.Fatalf("event cell did not average its related contracts or included the unknown row: %+v", got)
	}
	if got := stats[0]; got.EventClusterMeanAsk != .40 || got.OpenCanonicalEventClusters != 0 {
		t.Fatalf("event proof costs/open count do not match the frozen cohort: %+v", got)
	}

	// Learning an identity later must not rewrite a settled observation. Historical dependence is
	// unknowable without its entry-time registry version, so it remains excluded prospectively.
	registerUnitTrialEventFixture(t, st, eventID, "UNKNOWN-PROP")
	if err := st.db.QueryRow(`SELECT canonical_event_id,event_version FROM unit_trials WHERE ticker='UNKNOWN-PROP'`).Scan(&unknownID, &unknownV); err != nil {
		t.Fatal(err)
	}
	if unknownID != "" || unknownV != 0 {
		t.Fatalf("later catalog knowledge backfilled history: %q/v%d", unknownID, unknownV)
	}
	leaders, err := st.UnitTrialLeaderboard(ctx, now.Add(72*time.Hour))
	if err != nil || len(leaders) != 1 {
		t.Fatalf("leaders=%+v err=%v", leaders, err)
	}
	if leaders[0].SettledEventClusters != 1 || leaders[0].ClusteredSettledMarkets != 2 ||
		leaders[0].UnclusteredSettledMarkets != 1 || leaders[0].EventClusterMeanPC < .1999 ||
		leaders[0].EventClusterMeanPC > .2001 || leaders[0].EventDayClusters != 1 ||
		leaders[0].ProofMeanAsk != .40 {
		t.Fatalf("leaderboard dependence telemetry disagrees with frozen rows: %+v", leaders[0])
	}
}
