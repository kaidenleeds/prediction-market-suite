package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR145FamilyCLVCountsSameTickerOnDifferentVenuesSeparately(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	sequence := 0
	insert := func(platform, ticker, path string) {
		t.Helper()
		sequence++
		if err := st.InsertSignal(ctx, Signal{Platform: platform, Ticker: ticker, Side: "YES",
			SignalType: "venue-contract-clv", EntryPrice: .50}); err != nil {
			t.Fatal(err)
		}
		// Move each fixture to a distinct stored slot after insert. The production dedupe key is
		// intentionally a signal hot-loop concern; this test isolates the historical CLV grouping.
		if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET slot=slot||?,resolved=1,settle_val=1,post_path=?
WHERE id=(SELECT MAX(id) FROM signal_log)`, sequence, path); err != nil {
			t.Fatal(err)
		}
	}
	// The identical text ticker is two different executable contracts on two venues. They carry
	// opposite CLV and must remain two samples, followed by three other +20c contracts.
	insert("kalshi", "SAME", "0.900")
	insert("polyus", "SAME", "0.100")
	for _, ticker := range []string{"B", "C", "D"} {
		insert("kalshi", ticker, "0.700")
	}
	rows, err := st.FamilyCLVStats(ctx, time.Now().Add(-time.Hour))
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	got := rows[0]
	if got.N != 5 || got.Markets != 5 || math.Abs(got.Mean-.12) > 1e-12 {
		t.Fatalf("venue+ticker CLV contracts were pooled: %+v", got)
	}
}
