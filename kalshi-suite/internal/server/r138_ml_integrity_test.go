package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR138MLRowVersionsRequireCompleteV2BookReceipt(t *testing.T) {
	visibleOnly := storage.Signal{Platform: "kalshi", Ticker: "VISIBLE", EntryPrice: .42, IsLive: 1}
	finalizeMLSignalProvenance(&visibleOnly)
	if visibleOnly.LabelVersion != "" || visibleOnly.PricingVersion != "" || visibleOnly.BookFeatureVer != 0 {
		t.Fatalf("visible-price classification entered v2 cohort: %+v", visibleOnly)
	}

	partial := storage.Signal{Platform: "kalshi", Ticker: "PARTIAL",
		BookFeatureVer: mlBookFeatureVersion, PricingVersion: "book-native-v1"}
	finalizeMLSignalProvenance(&partial)
	if partial.LabelVersion != "" {
		t.Fatalf("mismatched pricing contract entered v2 label cohort: %+v", partial)
	}

	pre := int64(3600)
	complete := storage.Signal{Platform: "kalshi", Ticker: "COMPLETE", SecsToStart: &pre, IsLive: 0,
		BookFeatureVer: mlBookFeatureVersion, PricingVersion: mlBookFeatureSchema}
	finalizeMLSignalProvenance(&complete)
	if complete.LabelVersion != sigLiveLabelVersion {
		t.Fatalf("complete v2 receipt label=%q, want %q", complete.LabelVersion, sigLiveLabelVersion)
	}

	impossible := complete
	impossible.LabelVersion, impossible.IsLive = "", 1
	finalizeMLSignalProvenance(&impossible)
	if impossible.LabelVersion != "" {
		t.Fatalf("future Kalshi row labeled live entered v2 cohort: %+v", impossible)
	}
}
