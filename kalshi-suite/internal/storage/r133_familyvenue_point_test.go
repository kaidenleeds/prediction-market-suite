package storage

import (
	"context"
	"testing"
	"time"
)

func TestR133FamilyVenuePointPublishesSingleResolvedMarket(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "KXN1POINT",
		Side: "YES", SignalType: "n1-point", EntryPrice: 0.40}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1,settle_val=1,resolved_at=? WHERE ticker='KXN1POINT'`,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	rows, err := st.FamilyVenueEdgeStats(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Family != "n1-point" || rows[0].Platform != "kalshi" || rows[0].N != 1 {
		t.Fatalf("single-market venue point was hidden by an n floor: %+v", rows)
	}
	if rows[0].Mean <= 0 {
		t.Fatalf("single-market venue point mean=%v want positive", rows[0].Mean)
	}
}
