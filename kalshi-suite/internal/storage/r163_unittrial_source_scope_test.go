package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR163UnitTrialSourceScopeKeepsHistoryVisibleButSeparatesProofGeneration(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ctx := context.Background()
	now := time.Now().UTC()
	trials := []UnitTrial{
		{OpenedTS: now.Add(-2 * time.Hour), Family: "scope", Platform: "kalshi",
			OriginLayer: "strategy", Ticker: "KXSCOPE-OLD", Side: "YES", Episode: 0,
			Ask: .40, FeePC: .01, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1,
			QuoteSource: "strategy-decision/legacy-paper/kalshi-ws"},
		{OpenedTS: now.Add(-time.Hour), Family: "scope", Platform: "kalshi",
			OriginLayer: "strategy", Ticker: "KXSCOPE-CURRENT", Side: "YES", Episode: 1_630_001,
			Ask: .40, FeePC: .01, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1,
			QuoteSource: "strategy-decision/live-priority/cash-live-priority-v1/kalshi-ws/contract"},
	}
	for _, trial := range trials {
		if inserted, insertErr := st.InsertUnitTrial(ctx, trial); insertErr != nil || !inserted {
			t.Fatalf("insert %+v = %v, %v", trial, inserted, insertErr)
		}
	}
	closed := now.Format(time.RFC3339Nano)
	if _, err = st.db.ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,pnl_pc=CASE ticker
 WHEN 'KXSCOPE-OLD' THEN .20 ELSE .50 END,return_per_dollar=1`,
		closed); err != nil {
		t.Fatal(err)
	}

	all, found, err := st.UnitTrialRouteLeaderboard(
		ctx, now.Add(time.Minute), "scope", "kalshi", "strategy", "YES")
	if err != nil || !found || all.SettledMarkets != 2 || math.Abs(all.MeanPC-.35) > 1e-12 {
		t.Fatalf("general history disappeared or changed: found=%v row=%+v err=%v", found, all, err)
	}
	current, found, err := st.UnitTrialRouteLeaderboardSourcePrefix(ctx, now.Add(time.Minute),
		"scope", "kalshi", "strategy", "YES",
		"strategy-decision/live-priority/cash-live-priority-v1/")
	if err != nil || !found || current.SettledMarkets != 1 || math.Abs(current.MeanPC-.50) > 1e-12 {
		t.Fatalf("current proof generation pooled legacy row: found=%v row=%+v err=%v",
			found, current, err)
	}
}
