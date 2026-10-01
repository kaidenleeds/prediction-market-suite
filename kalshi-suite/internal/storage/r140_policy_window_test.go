package storage

import (
	"context"
	"testing"
	"time"
)

func TestListResolvedPolicySignalsSinceFiltersThenDeduplicates(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	insert := func(ts, slot, platform, ticker, side, family string, entry, fill float64, won any, resolved int) {
		t.Helper()
		_, err := st.db.ExecContext(ctx, `
INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,fill_price,category,resolved,won)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, ts, ts[:10], slot, platform, ticker, ticker, side, family, entry, fill, "Sports", resolved, won)
		if err != nil {
			t.Fatalf("insert %s/%s: %v", ticker, slot, err)
		}
	}
	old := now.AddDate(0, 0, -45).Format(time.RFC3339)
	recent1 := now.Add(-2 * time.Hour).Format(time.RFC3339)
	recent2 := now.Add(-time.Hour).Format(time.RFC3339)
	insert(old, "old", "kalshi", "REPEAT", "YES", "xmatch", .11, 0, 0, 1)
	insert(recent1, "recent-1", "kalshi", "REPEAT", "YES", "xmatch", .31, .32, 1, 1)
	insert(recent2, "recent-2", "kalshi", "REPEAT", "YES", "xmatch", .41, .42, 1, 1)
	insert(recent1, "polyus", "polyus", "PUS", "NO", "pbridge", .25, 0, 0, 1)
	insert(recent1, "fill-fallback", "polyus", "FILL-FALLBACK", "YES", "pbridge", 0, .44, 1, 1)
	insert(recent1, "research", "polymarket", "PINT", "YES", "pflow", .25, 0, 1, 1)
	insert(recent1, "bad-side", "kalshi", "BAD", "MAYBE", "xmatch", .25, 0, 1, 1)
	insert(recent1, "open", "kalshi", "OPEN", "YES", "xmatch", .25, 0, nil, 0)

	rows, err := st.ListResolvedPolicySignalsSince(ctx, now.AddDate(0, 0, -30))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want only current canonical Kalshi and PolyUS rows: %+v", len(rows), rows)
	}
	byTicker := map[string]PolicySignalRow{}
	for _, r := range rows {
		byTicker[r.Ticker] = r
	}
	if got := byTicker["REPEAT"]; got.EntryPrice != .31 || got.FillPrice != .32 || got.Won != 1 {
		t.Fatalf("window must deduplicate to earliest current entry, got %+v", got)
	}
	if got := byTicker["PUS"]; got.Platform != "polyus" || got.Side != "NO" || got.FillPrice != 0 || got.Won != 0 {
		t.Fatalf("PolyUS canonical single missing or malformed: %+v", got)
	}
	if got := byTicker["FILL-FALLBACK"]; got.EntryPrice != 0 || got.FillPrice != .44 {
		t.Fatalf("valid executable fill must survive an invalid signal entry: %+v", got)
	}
}
