package storage

import (
	"context"
	"math"
	"testing"
)

func TestR135WeatherResearchSignalsSettleWithoutPortfolioRows(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	ptr := func(v float64) *float64 { return &v }
	for _, fam := range []string{"weather-curve-residual", "weather-curve-revision"} {
		inserted, err := st.InsertSignalResult(ctx, Signal{Platform: "kalshi", Ticker: "WX-" + fam,
			Title: fam, Side: "YES", SignalType: fam, EntryPrice: .40, FeePC: ptr(.01),
			BookFeatureVer: 1, BookBid: ptr(.39), BookAsk: ptr(.40), BookBidDepth: ptr(10), BookAskDepth: ptr(8),
			BookQuoteAgeS: ptr(.2), BookMakerTick: ptr(.01), BookTakerTick: ptr(.01),
			BookMakerFeePC: ptr(.002), BookTakerFeePC: ptr(.01), BookSource: "kalshi-ws-depth"})
		if err != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", fam, inserted, err)
		}
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1,won=1,settle_val=1,resolved_at=datetime('now')`); err != nil {
		t.Fatal(err)
	}
	stats, err := st.WeatherResearchSignalStats(ctx)
	if err != nil || len(stats) != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	for _, row := range stats {
		if row.Logged != 1 || row.Resolved != 1 || row.Won != 1 || math.Abs(row.NetPC-.59) > 1e-9 {
			t.Fatalf("bad settlement stat: %+v", row)
		}
	}
	for _, table := range []string{"paper_fills", "unit_trials", "maker_fill_stats"} {
		var n int
		if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Fatalf("%s rows=%d err=%v; research signal must not dispatch", table, n, err)
		}
	}
}
