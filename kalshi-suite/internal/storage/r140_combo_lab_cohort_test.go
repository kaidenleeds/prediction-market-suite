package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestR140ComboLabCohortSchemaAndRoundTrip(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	rows, err := st.db.Query(`PRAGMA table_info(plab_open)`)
	if err != nil {
		t.Fatal(err)
	}
	cols := map[string]bool{}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var dflt any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			t.Fatal(err)
		}
		cols[name] = true
	}
	rows.Close()
	if !cols["cohort"] || !cols["route_state"] {
		t.Fatalf("Combo Lab migration columns missing: %+v", cols)
	}
	now := time.Now().UTC()
	c := PlabCand{ID: "r140-promoted-fixture", At: now.Unix(), Bucket: "indep", Class: "ind:2leg",
		Legality: "unprobed", LegFees: "[0,0]", Legs: `[{"platform":"kalshi","ticker":"A","side":"YES"},{"platform":"kalshi","ticker":"B","side":"YES"}]`,
		NLegs: 2, Cohort: ComboLabCohortPromotedSystem, RouteState: "awaiting-rfq-identical-proof",
		LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "A"}, {Platform: "kalshi", Ticker: "B"}}}
	if n, err := st.PlabInsertBatch(ctx, []PlabCand{c}); err != nil || n != 1 {
		t.Fatalf("insert cohort row n=%d err=%v", n, err)
	}
	counts, err := st.PlabOpenCohortCounts(ctx)
	if err != nil || counts[ComboLabCohortPromotedSystem] != 1 {
		t.Fatalf("cohort counts=%+v err=%v", counts, err)
	}
	if _, err := st.PlabResolveTicker(ctx, "kalshi", "A", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.PlabResolveTicker(ctx, "kalshi", "B", 1); err != nil {
		t.Fatal(err)
	}
	gradable, err := st.PlabGradable(ctx, now.Add(time.Minute).Unix(), 10)
	if err != nil || len(gradable) != 1 || gradable[0].Cohort != ComboLabCohortPromotedSystem ||
		gradable[0].RouteState != "awaiting-rfq-identical-proof" {
		t.Fatalf("cohort grade round trip=%+v err=%v", gradable, err)
	}
}

func TestR142ComboLabUnresolvedTickerKeysetWrapReachesPastPage(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Unix()
	batch := make([]PlabCand, 0, 260)
	for i := 0; i < 260; i++ {
		a, b := fmt.Sprintf("K%03dA", i), fmt.Sprintf("K%03dB", i)
		batch = append(batch, PlabCand{ID: fmt.Sprintf("keyset-%03d", i), At: now,
			Bucket: "indep", Class: "ind:2leg", LegFees: "[0,0]", Legs: "[]", NLegs: 2,
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: a}, {Platform: "kalshi", Ticker: b}}})
	}
	if n, err := st.PlabInsertBatch(ctx, batch); err != nil || n != int64(len(batch)) {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	first, err := st.PlabUnresolvedTickersAfter(ctx, PlabLegKey{}, 512)
	if err != nil || len(first) != 512 {
		t.Fatalf("first page len=%d err=%v", len(first), err)
	}
	second, err := st.PlabUnresolvedTickersAfter(ctx, first[len(first)-1], 512)
	if err != nil || len(second) != 512 {
		t.Fatalf("wrapped page len=%d err=%v", len(second), err)
	}
	seen := map[string]bool{}
	for _, page := range [][]PlabLegKey{first, second} {
		for _, k := range page {
			seen[k.Platform+"|"+k.Ticker] = true
		}
	}
	if len(seen) != 520 {
		t.Fatalf("keyset rotation covered %d/520 distinct unresolved tickers", len(seen))
	}
}

func TestR142ComboLabOpenLegCountsAndLegacyPreservation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Unix()
	rows := []PlabCand{
		{ID: "r142-2leg", At: now, Bucket: "indep", Class: "ind:2leg", LegFees: "[0,0]", Legs: "[]", NLegs: 2,
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "A"}, {Platform: "kalshi", Ticker: "B"}}},
		{ID: "r142-6leg", At: now, Bucket: "indep", Class: "ind:6leg", LegFees: "[]", Legs: "[]", NLegs: 6,
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "C1"}, {Platform: "kalshi", Ticker: "C2"},
				{Platform: "kalshi", Ticker: "C3"}, {Platform: "kalshi", Ticker: "C4"},
				{Platform: "kalshi", Ticker: "C5"}, {Platform: "kalshi", Ticker: "C6"}}},
		{ID: "r142-legacy-8leg", At: now, Bucket: "indep", Class: "ind:8leg", LegFees: "[]", Legs: "[]", NLegs: 8,
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "D"}}},
		{ID: "r142-settled-8leg", At: now, Bucket: "indep", Class: "ind:8leg", LegFees: "[]", Legs: "[]", NLegs: 8,
			LegKeys: []PlabLegKey{{Platform: "kalshi", Ticker: "E"}}},
	}
	if n, err := st.PlabInsertBatch(ctx, rows); err != nil || n != int64(len(rows)) {
		t.Fatalf("insert rows n=%d err=%v", n, err)
	}
	counts, err := st.PlabOpenLegCounts(ctx)
	if err != nil || counts[2] != 1 || counts[6] != 1 || counts[8] != 2 {
		t.Fatalf("leg counts=%+v err=%v", counts, err)
	}
	if _, err := st.PlabResolveTicker(ctx, "kalshi", "E", 1); err != nil {
		t.Fatal(err)
	}
	pending, err := st.PlabPendingHorizonRows(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range pending {
		if row.ID == "r142-settled-8leg" {
			t.Fatal("settled row appeared in pending horizon cleanup")
		}
	}
	if n, err := st.PlabPruneAboveLegLimit(ctx, 6, 100); err != nil || n != 0 {
		t.Fatalf("valid legacy row must not be erased: n=%d err=%v", n, err)
	}
	counts, err = st.PlabOpenLegCounts(ctx)
	if err != nil || counts[2] != 1 || counts[6] != 1 || counts[8] != 2 {
		t.Fatalf("post-policy leg counts=%+v err=%v", counts, err)
	}
	if n, err := st.PlabOpenCount(ctx); err != nil || n != 4 {
		t.Fatalf("post-policy open n=%d err=%v", n, err)
	}
	if n, err := st.PlabOpenCurrentCount(ctx, 6); err != nil || n != 2 {
		t.Fatalf("current 2-6 sample denominator=%d err=%v, want legacy rows excluded", n, err)
	}
	if n, err := st.PlabExpire(ctx, now+1, 100); err != nil || n != 0 {
		t.Fatalf("age alone must not erase unresolved truth: n=%d err=%v", n, err)
	}
	if n, err := st.PlabOpenCount(ctx); err != nil || n != 4 {
		t.Fatalf("all valid rows must survive policy cleanup: n=%d err=%v", n, err)
	}
}
