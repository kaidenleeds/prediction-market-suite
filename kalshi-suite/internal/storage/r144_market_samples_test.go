package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR144CLVAveragesRowsWithinTickerBeforeConfidenceSample(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insert := func(sourceTicker, clusteredTicker, path string, slotBump int) {
		t.Helper()
		if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: sourceTicker, Side: "YES",
			SignalType: "clustered-clv", EntryPrice: .50}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET ticker=?,slot=slot+?,resolved=1,settle_val=1,post_path=? WHERE id=(SELECT MAX(id) FROM signal_log)`, clusteredTicker, slotBump, path); err != nil {
			t.Fatal(err)
		}
	}
	// SAME has two contradictory rows whose market-level mean is zero. The other four markets
	// each contribute +20c. Independent-market mean is therefore +16c; raw-row mean is +13.33c.
	insert("SAME-1", "SAME", "0.900", 0)
	insert("SAME-2", "SAME", "0.100", 1)
	for _, ticker := range []string{"B", "C", "D", "E"} {
		insert(ticker, ticker, "0.700", 0)
	}
	rows, err := st.FamilyCLVStats(ctx, time.Now().Add(-time.Hour))
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	got := rows[0]
	if got.N != 6 || got.Markets != 5 || math.Abs(got.Mean-.16) > 1e-12 {
		t.Fatalf("CLV rows pseudo-replicated instead of ticker-clustered: %+v", got)
	}
}

func TestR144MakerSettledEdgeAveragesWithinVenueTicker(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, row := range []struct {
		ticker string
		settle float64
	}{{"A", 1}, {"A", 0}, {"B", .7}} {
		if _, err := st.db.ExecContext(ctx, `INSERT INTO maker_fill_stats
			(ts,platform,ticker,side,post_px,filled,settle_val)
			VALUES(?,?,?,?,?,1,?)`, now, "kalshi", row.ticker, "YES", .5, row.settle); err != nil {
			t.Fatal(err)
		}
	}
	markets, rows, mean, _, err := st.MakerSettledMarketEdge(ctx)
	if err != nil || markets != 2 || rows != 3 || math.Abs(mean-.1) > 1e-12 {
		t.Fatalf("maker evidence markets=%d rows=%d mean=%v err=%v", markets, rows, mean, err)
	}
}

func TestR144FamilySpecificFillAndPathCannotStampAnotherSystem(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for _, family := range []string{"other-system", "invert:edge"} {
		if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "KX-R144-FILL",
			Side: "NO", SignalType: family, EntryPrice: .40}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.UpdateSignalFamilyFill(ctx, "kalshi", "KX-R144-FILL", "NO", "invert:edge", .41); err != nil {
		t.Fatal(err)
	}
	if err := st.AppendSignalFamilyPostPath(ctx, "kalshi", "KX-R144-FILL", "NO", "invert:edge", .44); err != nil {
		t.Fatal(err)
	}
	rows, err := st.db.QueryContext(ctx, `SELECT signal_type,fill_price,post_path FROM signal_log
WHERE ticker='KX-R144-FILL' ORDER BY signal_type`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	got := map[string]struct {
		fill float64
		path string
	}{}
	for rows.Next() {
		var family, path string
		var fill float64
		if err := rows.Scan(&family, &fill, &path); err != nil {
			t.Fatal(err)
		}
		got[family] = struct {
			fill float64
			path string
		}{fill: fill, path: path}
	}
	if got["other-system"].fill != 0 || got["other-system"].path != "" ||
		math.Abs(got["invert:edge"].fill-.41) > 1e-12 || got["invert:edge"].path != "0.440" {
		t.Fatalf("family-specific label leaked: %+v", got)
	}
}
