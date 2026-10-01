package storage

import (
	"context"
	"testing"
	"time"
)

func TestR133RecentOpenSignalTickersAreBoundedAndNewestFirst(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	for _, sig := range []Signal{
		{Platform: "polymarket", Ticker: "old", Title: "old", Side: "YES", SignalType: "pflow", EntryPrice: .4},
		{Platform: "polymarket", Ticker: "newer", Title: "newer", Side: "YES", SignalType: "pflow", EntryPrice: .4},
		{Platform: "polymarket", Ticker: "newest", Title: "newest", Side: "NO", SignalType: "pcrypto", EntryPrice: .6},
		{Platform: "kalshi", Ticker: "wrong-venue", Title: "wrong", Side: "YES", SignalType: "cross", EntryPrice: .5},
	} {
		if err := st.InsertSignal(ctx, sig); err != nil {
			t.Fatal(err)
		}
	}
	for ticker, ts := range map[string]time.Time{
		"old": now.Add(-25 * time.Hour), "newer": now.Add(-time.Minute),
		"newest": now, "wrong-venue": now,
	} {
		if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET ts=? WHERE ticker=?`, ts.Format(time.RFC3339), ticker); err != nil {
			t.Fatal(err)
		}
	}
	got, err := st.ListRecentOpenSignalTickersByPlatform(ctx, "polymarket", now.Add(-24*time.Hour), 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "newest" || got[1] != "newer" {
		t.Fatalf("recent poly-int markets=%v, want newest/current only", got)
	}
}
