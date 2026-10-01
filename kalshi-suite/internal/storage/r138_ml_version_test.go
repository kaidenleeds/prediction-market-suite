package storage

import (
	"context"
	"testing"
)

func TestR138SignalPersistsLabelAndPricingVersions(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	sig := Signal{Platform: "kalshi", Ticker: "KXV2", Title: "v2", Side: "YES",
		SignalType: "test", EntryPrice: .4, BookFeatureVer: 1,
		LabelVersion: "kalshi_start_clock_v2", PricingVersion: "book-native-v2"}
	if err := st.InsertSignal(context.Background(), sig); err != nil {
		t.Fatal(err)
	}
	var label, pricing string
	if err := st.db.QueryRow(`SELECT label_version,pricing_version FROM signal_log WHERE ticker='KXV2'`).Scan(&label, &pricing); err != nil {
		t.Fatal(err)
	}
	if label != sig.LabelVersion || pricing != sig.PricingVersion {
		t.Fatalf("versions=%q/%q want %q/%q", label, pricing, sig.LabelVersion, sig.PricingVersion)
	}
}
