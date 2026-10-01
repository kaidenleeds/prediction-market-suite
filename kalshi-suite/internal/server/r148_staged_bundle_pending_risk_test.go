package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR148StagedPendingRiskReservesWholePackageBeforeAttempts(t *testing.T) {
	ctx := context.Background()
	s := testServer(t)
	b := newR148ServerStagedBroker(s)
	now := time.Date(2026, 7, 15, 20, 0, 0, 0, time.UTC)
	b.now = func() time.Time { return now }
	bundle := storage.ResearchRouteBundle{BundleID: "risk-package", SystemID: "nested-ladder-lock",
		CertificateHash: r148Hash("certificate"), EventVersion: 2,
		Legs: []storage.ResearchRouteBundleLeg{
			{Index: 0, Venue: "kalshi", Ticker: "KXRISK-A", Side: "YES", Quantity: 2},
			{Index: 1, Venue: "kalshi", Ticker: "KXRISK-B", Side: "NO", Quantity: 2},
		}}
	b.riskBaselineFn = func(context.Context, storage.ResearchRouteBundle) (map[string]r148LiveRiskBaseline, error) {
		return map[string]r148LiveRiskBaseline{
			"kalshi\x00KXRISK-A": {Observed: now, PositionQty: 1, RestingRiskUSD: .2, ReceiptJSON: `{"ticker":"KXRISK-A"}`},
			"kalshi\x00KXRISK-B": {Observed: now, PositionQty: 0, RestingRiskUSD: .1, ReceiptJSON: `{"ticker":"KXRISK-B"}`},
		}, nil
	}
	quotes := map[int]r148StagedQuote{
		0: {Price: .30, Available: 10, Fee: .02, Tick: .01, Source: "book+fee"},
		1: {Price: .40, Available: 10, Fee: .03, Tick: .01, Source: "book+fee"},
	}
	if err := b.ReserveStagedPackage(ctx, "execution-risk-package", bundle, quotes); err != nil {
		t.Fatal(err)
	}
	// A crash/retry after the atomic insert must reload the exact reservation, not collect a new
	// baseline or create per-leg reservations.
	b.riskBaselineFn = func(context.Context, storage.ResearchRouteBundle) (map[string]r148LiveRiskBaseline, error) {
		t.Fatal("idempotent reserve recollected a mutable baseline")
		return nil, nil
	}
	if err := b.ReserveStagedPackage(ctx, "execution-risk-package", bundle, quotes); err != nil {
		t.Fatal(err)
	}
	row, ok, err := s.store.LivePendingRiskByID(ctx, r148StagedRiskID("execution-risk-package"))
	if err != nil || !ok {
		t.Fatalf("risk row ok=%v err=%v", ok, err)
	}
	if row.Intent.Product != "staged" || row.Intent.Route != "staged_fok" || len(row.Legs) != 2 || len(row.Clusters) == 0 {
		t.Fatalf("reservation=%+v", row)
	}
	if math.Abs(row.Intent.CostUSD-1.45) > 1e-9 || row.Legs[0].ExpectedPositionQty != 3 ||
		row.Legs[1].ExpectedPositionQty != -2 || row.Legs[0].ClientOrderID == "" || row.Legs[1].ClientOrderID == "" {
		t.Fatalf("intent=%+v legs=%+v", row.Intent, row.Legs)
	}
	for _, cluster := range row.Clusters {
		if math.Abs(cluster.ReservedUSD-row.Intent.CostUSD) > 1e-9 {
			t.Fatalf("cluster=%+v cost=%v", cluster, row.Intent.CostUSD)
		}
	}
}

func TestR148StagedPendingRiskAttemptIsAppendOnlyAndReleasableWhenUnfilled(t *testing.T) {
	ctx := context.Background()
	s := testServer(t)
	b := newR148ServerStagedBroker(s)
	now := time.Date(2026, 7, 15, 20, 1, 0, 0, time.UTC)
	b.now = func() time.Time { return now }
	leg := storage.ResearchRouteBundleLeg{Index: 0, Venue: "kalshi", Ticker: "KXRISK-C", Side: "YES", Quantity: 1}
	// Storage requires staged packages to contain 2+ legs. The second leg never submits in this
	// clean first-leg no-fill case, and therefore owns no attempt that must be reconciled.
	bundle := storage.ResearchRouteBundle{BundleID: "risk-unfilled", SystemID: "event-basket-lock",
		CertificateHash: r148Hash("certificate-2"), EventVersion: 1,
		Legs: []storage.ResearchRouteBundleLeg{leg,
			{Index: 1, Venue: "kalshi", Ticker: "KXRISK-D", Side: "NO", Quantity: 1}}}
	b.riskBaselineFn = func(context.Context, storage.ResearchRouteBundle) (map[string]r148LiveRiskBaseline, error) {
		return map[string]r148LiveRiskBaseline{
			"kalshi\x00KXRISK-C": {Observed: now, ReceiptJSON: `{"ticker":"KXRISK-C"}`},
			"kalshi\x00KXRISK-D": {Observed: now, ReceiptJSON: `{"ticker":"KXRISK-D"}`},
		}, nil
	}
	quotes := map[int]r148StagedQuote{
		0: {Price: .30, Available: 2, Fee: .01, Tick: .01, Source: "book+fee"},
		1: {Price: .40, Available: 2, Fee: .01, Tick: .01, Source: "book+fee"},
	}
	executionID := "execution-risk-unfilled"
	if err := b.ReserveStagedPackage(ctx, executionID, bundle, quotes); err != nil {
		t.Fatal(err)
	}
	clientID := r148StagedRiskClientID(executionID, 0, "BUY")
	if err := b.appendStagedRiskAttempt(ctx, executionID, leg, "BUY", storage.LivePendingRiskSubmitStarted,
		clientID, "", r148StagedReceipt{Source: "pre-send"}, "before venue mutation", time.Time{}); err != nil {
		t.Fatal(err)
	}
	receipt := r148StagedReceipt{OrderID: "order-unfilled", State: r148ReceiptUnfilled,
		Authoritative: true, Source: "kalshi-get-order+fills", Reason: "terminal FOK no-fill"}
	if err := b.RecordStagedRiskReceipt(ctx, executionID, leg, "BUY", receipt); err != nil {
		t.Fatal(err)
	}
	if err := b.ReleaseStagedRisk(ctx, executionID, "flat rejection", map[string]any{"flat": true}); err != nil {
		t.Fatal(err)
	}
	row, ok, err := s.store.LivePendingRiskByID(ctx, r148StagedRiskID(executionID))
	if err != nil || !ok {
		t.Fatalf("risk row ok=%v err=%v", ok, err)
	}
	want := []string{storage.LivePendingRiskReserved, storage.LivePendingRiskSubmitStarted,
		storage.LivePendingRiskAck, storage.LivePendingRiskTerminalUnfilled, storage.LivePendingRiskReleased}
	if len(row.Events) != len(want) {
		t.Fatalf("events=%+v", row.Events)
	}
	for i, event := range row.Events {
		if event.EventType != want[i] || event.Sequence != i+1 {
			t.Fatalf("event[%d]=%+v want=%s", i, event, want[i])
		}
	}
}
