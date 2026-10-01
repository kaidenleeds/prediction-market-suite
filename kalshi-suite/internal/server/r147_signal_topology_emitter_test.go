package server

import (
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR147SignalInputReceiptRoundTrip(t *testing.T) {
	at := time.Date(2026, 7, 15, 12, 34, 56, 789, time.UTC)
	for _, topology := range []string{"K", "K-PINT", "K-PUS", "PUS-PINT", "K-PUS-PINT",
		"K-SPOT", "K-OKX", "K-SPORTSBOOK", "K-NOAA"} {
		sig := storage.Signal{ExecExpr: "detector/" + r147InputReceiptExpr(topology, at) + "/book=current"}
		if got := r147SignalInputTopology(sig); got != topology {
			t.Fatalf("topology %s round-tripped as %s", topology, got)
		}
		if got := r147SignalInputObservedAt(sig); !got.Equal(at) {
			t.Fatalf("%s source time=%s want=%s", topology, got, at)
		}
	}
}

func TestR147IndependentInversePreservesInputReceipt(t *testing.T) {
	at := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXTEST", Side: "YES", SignalType: "spotlag",
		EntryPrice: .40, ExecExpr: r147InputReceiptExpr("K-SPOT", at)}
	out, ok := independentInverseSignalIdentity(sig)
	if !ok || out.SignalType != "invert:spotlag" || out.Side != "NO" {
		t.Fatalf("inverse identity=%+v ok=%v", out, ok)
	}
	if got := r147SignalInputTopology(out); got != "K-SPOT" {
		t.Fatalf("inverse topology=%q", got)
	}
	if got := r147SignalInputObservedAt(out); !got.Equal(at) {
		t.Fatalf("inverse source time=%s want=%s", got, at)
	}
}

func TestR147CheapbandContractsPreserveLayeredTopologies(t *testing.T) {
	wantK := map[string]bool{"K": true, "K-PINT": true, "K-PUS": true, "K-PUS-PINT": true,
		"K-SPOT": true, "K-OKX": true, "K-SPORTSBOOK": true, "K-NOAA": true}
	for _, row := range r147ExactSignalContracts("cheapband", "kalshi", "YES", "taker") {
		delete(wantK, row.InputTopology)
	}
	if len(wantK) != 0 {
		t.Fatalf("missing Kalshi cheapband source topologies: %v", wantK)
	}
	wantPUS := map[string]bool{"PUS": true, "PUS-PINT": true, "K-PUS": true, "K-PUS-PINT": true}
	for _, row := range r147ExactSignalContracts("cheapband", "polyus", "YES", "taker") {
		delete(wantPUS, row.InputTopology)
	}
	if len(wantPUS) != 0 {
		t.Fatalf("missing PolyUS cheapband source topologies: %v", wantPUS)
	}
}
