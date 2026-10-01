package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR132MakerDeployedCalib(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	for i := 0; i < 100; i++ {
		y := 0.0
		if i < 70 {
			y = 1
		}
		if _, err := st.db.ExecContext(ctx, `INSERT INTO maker_fill_stats
(ts,platform,ticker,side,source,post_px,filled,fill_pwin,settle_val)
VALUES(?,?,?,?,?,?,?,?,?)`, now, "kalshi", "KXCAL", "YES", "ml-book", .50, 1, .80, y); err != nil {
			t.Fatal(err)
		}
	}
	ece, n, bins, err := st.MakerDeployedCalib(ctx, time.Now().Add(-14*24*time.Hour))
	if err != nil || n != 100 || len(bins) != 1 || math.Abs(ece-.10) > 1e-9 {
		t.Fatalf("ece=%v n=%d bins=%+v err=%v", ece, n, bins, err)
	}
}
