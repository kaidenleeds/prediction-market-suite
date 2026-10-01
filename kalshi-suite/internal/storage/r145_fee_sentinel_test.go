package storage

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestR145FamilyEdgesExcludeOperationalFeeSentinels(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	fee := 0.01
	for i := 0; i < 20; i++ {
		if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: fmt.Sprintf("GOOD-%02d", i),
			Side: "YES", SignalType: "sentinel-guard", EntryPrice: 0.40, FeePC: &fee}); err != nil {
			t.Fatal(err)
		}
	}
	badFee := 1e9 // server-side fail-closed execution sentinel, never an economic fee receipt
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "BAD-FEE",
		Side: "YES", SignalType: "sentinel-guard", EntryPrice: 0.40, FeePC: &badFee}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "BAD-SETTLE",
		Side: "YES", SignalType: "sentinel-guard", EntryPrice: 0.40, FeePC: &fee}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1,settle_val=1,resolved_at=?
WHERE signal_type='sentinel-guard'`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET settle_val=2 WHERE ticker='BAD-SETTLE'`); err != nil {
		t.Fatal(err)
	}

	families, err := st.FamilyEdgeStats(ctx, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(families) != 1 {
		t.Fatalf("family rows=%+v", families)
	}
	got := families[0]
	if got.Family != "sentinel-guard" || got.N != 20 || got.Rows != 20 ||
		math.Abs(got.Mean-0.59) > 1e-12 || math.Abs(got.FeePC-fee) > 1e-12 {
		t.Fatalf("invalid fee/outcome entered family evidence: %+v", got)
	}

	venues, err := st.FamilyVenueEdgeStats(ctx, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(venues) != 1 {
		t.Fatalf("venue rows=%+v", venues)
	}
	venue := venues[0]
	if venue.Family != "sentinel-guard" || venue.Platform != "kalshi" || venue.N != 20 ||
		venue.Rows != 20 || math.Abs(venue.Mean-0.59) > 1e-12 || math.Abs(venue.FeePC-fee) > 1e-12 {
		t.Fatalf("invalid fee/outcome entered venue evidence: %+v", venue)
	}
}
