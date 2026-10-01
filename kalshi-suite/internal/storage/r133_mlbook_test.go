package storage

import (
	"context"
	"testing"
)

func TestR133SignalBookSnapshotIsVersionedAndLegacyStaysNull(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	ptr := func(v float64) *float64 { return &v }
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "BOOK-V1", Title: "book",
		Side: "YES", SignalType: "edge", EntryPrice: .40, BookFeatureVer: 1,
		BookBid: ptr(.39), BookAsk: ptr(.41), BookBidDepth: ptr(12), BookAskDepth: ptr(7),
		BookQuoteAgeS: ptr(.25), BookMakerTick: ptr(.001), BookTakerTick: ptr(.01),
		BookMakerFeePC: ptr(.002), BookTakerFeePC: ptr(.008), BookSource: "kalshi-ws-depth"}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "LEGACY", Title: "legacy",
		Side: "YES", SignalType: "edge", EntryPrice: .40}); err != nil {
		t.Fatal(err)
	}
	rows, err := st.db.QueryContext(ctx, `SELECT ticker,book_feature_ver,book_bid,book_ask,book_source FROM signal_log ORDER BY ticker`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var ticker, source string
		var ver int
		var bid, ask any
		if err := rows.Scan(&ticker, &ver, &bid, &ask, &source); err != nil {
			t.Fatal(err)
		}
		seen++
		if ticker == "BOOK-V1" && (ver != 1 || bid == nil || ask == nil || source != "kalshi-ws-depth") {
			t.Fatalf("book row ver=%d bid=%v ask=%v source=%q", ver, bid, ask, source)
		}
		if ticker == "LEGACY" && (ver != 0 || bid != nil || ask != nil || source != "") {
			t.Fatalf("legacy row was reinterpreted: ver=%d bid=%v ask=%v source=%q", ver, bid, ask, source)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("rows=%d, want 2", seen)
	}
}
