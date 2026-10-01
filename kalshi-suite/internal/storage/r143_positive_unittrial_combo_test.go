package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestR143RecentPositiveUnitTrialLegsCoversExactRoutesOnly(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	insert := func(family, origin, ticker, side string, episode int) {
		t.Helper()
		ok, err := st.InsertUnitTrial(ctx, UnitTrial{OpenedTS: now.Add(-time.Minute), Family: family,
			Platform: "polyus", OriginLayer: origin, Ticker: ticker, Side: side, Episode: episode,
			Ask: .40, FeePC: .01, FeeKnown: true, FeeSource: "polyus:test", Depth: 5, QuoteSource: "polyus-ws"})
		if err != nil || !ok {
			t.Fatalf("insert %s/%s/%s: ok=%v err=%v", family, origin, ticker, ok, err)
		}
	}
	// One settled positive cell + one current opportunity for each origin.
	insert("model-edge", "model", "model-settled", "YES", 0)
	for i := 0; i < 6; i++ { // prolific route must not consume the sparse route's global result set
		insert("model-edge", "model", fmt.Sprintf("model-open-%d", i), "YES", 0)
	}
	insert("strategy-edge", "strategy", "strategy-settled", "NO", 0)
	insert("strategy-edge", "strategy", "strategy-open", "NO", 0)
	// A negative route with a current row must not enter.
	insert("loser", "model", "loser-settled", "YES", 0)
	insert("loser", "model", "loser-open", "YES", 0)
	for ticker, pnl := range map[string]float64{"model-settled": .03, "strategy-settled": .02, "loser-settled": -.10} {
		if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,pnl_pc=?,closed_ts=? WHERE ticker=?`,
			pnl, now.Format(time.RFC3339Nano), ticker); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := st.RecentPositiveUnitTrialLegs(ctx, now.Add(-20*time.Minute), 2)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]PositiveUnitTrialLeg{}
	for _, row := range rows {
		seen[row.OriginLayer+"|"+row.Family+"|"+row.Side] = row
	}
	byRoute := map[string]int{}
	for _, row := range rows {
		byRoute[row.OriginLayer+"|"+row.Family+"|"+row.Side]++
	}
	if len(rows) != 3 || byRoute["model|model-edge|YES"] != 2 ||
		byRoute["strategy|strategy-edge|NO"] != 1 || seen["strategy|strategy-edge|NO"].Ticker != "strategy-open" {
		t.Fatalf("exact positive route breadth/identity wrong: %+v", rows)
	}
	if _, exists := seen["model|loser|YES"]; exists {
		t.Fatalf("negative exact route entered Combo Lab discovery: %+v", rows)
	}
}
