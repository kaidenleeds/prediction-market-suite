package storage

import (
	"context"
	"testing"
	"time"
)

func TestR146ExecutionStatsCountDistinctContractsWithoutClusterScan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Add(-2 * time.Hour)
	registerUnitTrialEventFixture(t, st, "soccer:test:home-away", "HOME", "DRAW", "AWAY")
	for _, ticker := range []string{"HOME", "DRAW", "AWAY"} {
		inserted, insertErr := st.InsertUnitTrial(ctx, UnitTrial{OpenedTS: now, Family: "soccer-three-way",
			Platform: "kalshi", Ticker: ticker, Side: "YES", Ask: .30, FeeKnown: true,
			FeeSource: "test", Depth: 1, QuoteSource: "test-book"})
		if insertErr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", ticker, inserted, insertErr)
		}
	}
	closed := now.Add(time.Hour).Format(time.RFC3339Nano)
	if _, err := st.db.Exec(`UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=.10,return_per_dollar=.333`, closed); err != nil {
		t.Fatal(err)
	}
	rows, err := st.UnitTrialExecutionStatsAt(ctx, time.Now().UTC())
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if rows[0].SettledMarkets != 3 || rows[0].N != 3 {
		t.Fatalf("Home/Draw/Away did not remain three distinct contract samples: %+v", rows[0])
	}
	if rows[0].SettledEventClusters != 0 || rows[0].ClusteredSettledMarkets != 0 {
		t.Fatalf("decision-path stats unexpectedly ran cluster diagnostics: %+v", rows[0])
	}
}
