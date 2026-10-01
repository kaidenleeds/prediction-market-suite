package storage

import (
	"context"
	"testing"
	"time"
)

func TestR139NativeSystemFunnelReceiptsSeparateInputsFromEconomicN(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	rows := []NativeSystemFunnelReceipt{{Observed: now, CycleID: "c1", Family: "edge",
		Platform: "kalshi", OriginLayer: "model", ProducerState: "CURRENT",
		ProducerReason: "fixture producer", Eligible: 4, InputRows: 3, QuoteAttempts: 3,
		UnitRows: 2, MoneyTruthAttempts: 1, Exclusions: map[string]int{"missing_book": 1},
		FirstInput: now.Add(-time.Minute), LastInput: now, LastEconomic: now}}
	if err := st.InsertNativeSystemFunnelReceipts(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertNativeSystemFunnelReceipts(ctx, rows); err != nil {
		t.Fatal(err)
	}
	views, err := st.NativeSystemFunnelViews(ctx, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 {
		t.Fatalf("views=%+v", views)
	}
	v := views[0]
	if v.CycleEligible != 4 || v.CycleInputRows != 3 || v.CycleUnitRows != 2 ||
		v.CycleMoneyTruthAttempts != 1 || v.LifetimeEligible != 4 ||
		v.Exclusions["missing_book"] != 1 || v.LastInputTS == "" || v.LastEconomicTS == "" {
		t.Fatalf("funnel view lost separate collection/economic truth: %+v", v)
	}
}

func TestR139NativeSystemFunnelRejectsImpossibleCountsAndAuthorityColumns(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	bad := NativeSystemFunnelReceipt{CycleID: "bad", Family: "edge", Platform: "kalshi",
		ProducerState: "CURRENT", ProducerReason: "fixture", Eligible: 1, InputRows: 2}
	if err := st.InsertNativeSystemFunnelReceipts(context.Background(), []NativeSystemFunnelReceipt{bad}); err == nil {
		t.Fatal("impossible funnel counts were accepted")
	}
	var funded, paper, live int
	if err := st.db.QueryRow(`SELECT COALESCE(MAX(funded),0),COALESCE(MAX(paper_authority),0),
COALESCE(MAX(live_authority),0) FROM native_system_funnel_receipts`).Scan(&funded, &paper, &live); err != nil {
		t.Fatal(err)
	}
	if funded != 0 || paper != 0 || live != 0 {
		t.Fatalf("funnel acquired authority %d/%d/%d", funded, paper, live)
	}
}
