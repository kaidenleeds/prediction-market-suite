package storage

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func insertLiveDispatchShadowRisk(t *testing.T, st *Store, reservationID, clientOrderID,
	route, hashChar string, at time.Time) {
	t.Helper()
	price, principal, fee := 0.42, 0.84, 0.04
	if route == "maker" {
		price, principal, fee = 0.40, 0.80, 0.01
	}
	intent := LivePendingRiskIntent{
		ReservationID: reservationID, Created: at, BaselineObserved: at.Add(-time.Second),
		Product: "single", DispatchSource: "live-auto", SystemID: "shadow-system", Route: route,
		PrincipalUSD: principal, FeeUSD: fee, CostUSD: principal + fee, SourceIntentID: "shadow-source",
		RequestHash: strings.Repeat(hashChar, 64), ProofJSON: `{"shadow":true}`,
		BaselineReceiptJSON: `{"position":0,"orders":[]}`,
	}
	legs := []LivePendingRiskLeg{{
		Index: 0, Venue: "kalshi", Ticker: "KXSHADOW-" + reservationID, Side: "YES",
		Action: "BUY", ClientOrderID: clientOrderID, Quantity: 2, LimitPrice: price,
		BaselinePositionQty: 0, ExpectedPositionQty: 2, BaselineRestingRiskUSD: 0,
	}}
	clusters := []LivePendingRiskCluster{{
		Index: 0, Venue: "kalshi", ClusterKey: "shadow:" + reservationID,
		MappingVersion: "canonical-v1", ReservedUSD: intent.CostUSD,
	}}
	if inserted, err := st.InsertLivePendingRisk(context.Background(), intent, legs, clusters); err != nil || !inserted {
		t.Fatalf("insert pending-risk prerequisite inserted=%v err=%v", inserted, err)
	}
}

func takerLiveDispatchShadow(reservationID, clientOrderID string, at time.Time) LiveDispatchShadow {
	return LiveDispatchShadow{
		ReservationID: reservationID, ClientOrderID: clientOrderID, Captured: at,
		Venue: "kalshi", Ticker: "KXSHADOW-" + reservationID, Title: "Shadow fixture",
		Side: "YES", Action: "BUY", SystemID: "shadow-system", Route: "taker",
		RequestedQty: 2, LimitPrice: 0.42, TouchDepth: 7, DepthKnown: true,
		TickSize: 0.01, BookSource: "kalshi-book-ws:seq-123",
		QuotedFee: 0.04, QuoteFeeKnown: true, QuoteFeeSource: "kalshi:exact-registry",
		ModelVersion: "paper-execution-r154-v1", State: LiveDispatchShadowFilled,
		FilledQty: 2, FillPrice: 0.42, FillFee: 0.04, FillFeeKnown: true,
		FillFeeSource: "kalshi:exact-registry", FillAt: at, FillRule: "taker-at-dispatch-book",
	}
}

func makerLiveDispatchShadow(reservationID, clientOrderID string, at time.Time,
	queueKnown bool) LiveDispatchShadow {
	row := LiveDispatchShadow{
		ReservationID: reservationID, ClientOrderID: clientOrderID, Captured: at,
		Venue: "kalshi", Ticker: "KXSHADOW-" + reservationID, Title: "Shadow maker fixture",
		Side: "YES", Action: "BUY", SystemID: "shadow-system", Route: "maker",
		RequestedQty: 2, LimitPrice: 0.40, TouchDepth: 10, DepthKnown: true,
		TickSize: 0.01, BookSource: "kalshi-book-ws:seq-456",
		QuotedFee: 0.01, QuoteFeeKnown: true, QuoteFeeSource: "kalshi:maker-registry",
		ModelVersion: "paper-execution-r154-v1", State: LiveDispatchShadowResting,
		QueueKnown: queueKnown,
	}
	if queueKnown {
		row.QueueAhead, row.QueueLeft, row.QueueFilled = 10, 10, 0
		row.TapeCutoff = at
	}
	return row
}

func TestLiveDispatchShadowTakerIsDurableIdempotentAndPaperIsolated(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Microsecond)
	insertLiveDispatchShadowRisk(t, st, "shadow-taker-1", "shadow-client-1", "taker", "a", at)
	in := takerLiveDispatchShadow("shadow-taker-1", "shadow-client-1", at)

	id, inserted, err := st.InsertLiveDispatchShadow(ctx, in)
	if err != nil || !inserted || id <= 0 {
		t.Fatalf("insert id=%d inserted=%v err=%v", id, inserted, err)
	}
	retryID, inserted, err := st.InsertLiveDispatchShadow(ctx, in)
	if err != nil || inserted || retryID != id {
		t.Fatalf("exact retry id=%d inserted=%v err=%v", retryID, inserted, err)
	}
	changed := in
	changed.FillPrice += 0.01
	if _, _, err := st.InsertLiveDispatchShadow(ctx, changed); err == nil {
		t.Fatal("changed economics reused the durable dispatch keys")
	}

	var paperRows int
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM paper_fills`).Scan(&paperRows); err != nil {
		t.Fatal(err)
	}
	if paperRows != 0 {
		t.Fatalf("isolated shadow polluted normal Paper: rows=%d", paperRows)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE live_dispatch_shadow SET limit_price=.43 WHERE id=?`, id); err == nil {
		t.Fatal("raw SQL rewrote immutable dispatch economics")
	}
	if _, err := st.db.ExecContext(ctx, `DELETE FROM live_dispatch_shadow WHERE id=?`, id); err == nil {
		t.Fatal("raw SQL deleted durable shadow evidence")
	}

	settledAt := at.Add(time.Hour)
	if changed, err := st.SettleLiveDispatchShadow(ctx, id, 1, settledAt,
		"venue-scoped canonical settlement", "settlement-hash-1"); err != nil || !changed {
		t.Fatalf("settle changed=%v err=%v", changed, err)
	}
	if changed, err := st.SettleLiveDispatchShadow(ctx, id, 1, settledAt,
		"venue-scoped canonical settlement", "settlement-hash-1"); err != nil || changed {
		t.Fatalf("settle retry changed=%v err=%v", changed, err)
	}
	if _, err := st.SettleLiveDispatchShadow(ctx, id, 0, settledAt,
		"venue-scoped canonical settlement", "settlement-hash-1"); err == nil {
		t.Fatal("settlement truth was rewritten")
	}
	got, found, err := st.LiveDispatchShadowByReservation(ctx, in.ReservationID)
	if err != nil || !found || got.State != LiveDispatchShadowSettled || !got.RealizedNetKnown {
		t.Fatalf("settled row=%+v found=%v err=%v", got, found, err)
	}
	if want := 2*(1.0-0.42) - 0.04; math.Abs(got.RealizedNet-want) > 1e-9 {
		t.Fatalf("realized net=%.9f want=%.9f", got.RealizedNet, want)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	got, found, err = st.LiveDispatchShadowByID(ctx, id)
	if err != nil || !found || got.State != LiveDispatchShadowSettled ||
		!got.SettledAt.Equal(settledAt) || !got.RealizedNetKnown {
		t.Fatalf("reopened row=%+v found=%v err=%v", got, found, err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM paper_fills`).Scan(&paperRows); err != nil ||
		paperRows != 0 {
		t.Fatalf("reopened Paper rows=%d err=%v", paperRows, err)
	}
}

func TestLiveDispatchShadowMakerQueueFillSettlementAndLateFee(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Microsecond)
	insertLiveDispatchShadowRisk(t, st, "shadow-maker-1", "shadow-maker-client-1", "maker", "b", at)

	id, inserted, err := st.InsertLiveDispatchShadow(ctx,
		makerLiveDispatchShadow("shadow-maker-1", "shadow-maker-client-1", at, true))
	if err != nil || !inserted {
		t.Fatalf("insert id=%d inserted=%v err=%v", id, inserted, err)
	}
	if changed, err := st.UpdateLiveDispatchShadowMakerProgress(ctx, id, 3, 0,
		at.Add(time.Second)); err != nil || !changed {
		t.Fatalf("queue chew changed=%v err=%v", changed, err)
	}
	if changed, err := st.UpdateLiveDispatchShadowMakerProgress(ctx, id, 3, 0,
		at.Add(time.Second)); err != nil || changed {
		t.Fatalf("queue retry changed=%v err=%v", changed, err)
	}
	if _, err := st.UpdateLiveDispatchShadowMakerProgress(ctx, id, 4, 0,
		at.Add(2*time.Second)); err == nil {
		t.Fatal("queue position moved backwards")
	}
	if changed, err := st.UpdateLiveDispatchShadowMakerProgress(ctx, id, 0, 1.5,
		at.Add(2*time.Second)); err != nil || !changed {
		t.Fatalf("partial queue fill changed=%v err=%v", changed, err)
	}
	fillAt := at.Add(3 * time.Second)
	if changed, err := st.FillLiveDispatchShadow(ctx, id, 1, 0.40, 0, false, "",
		fillAt, "queue-partial:moved-1tick"); err != nil || !changed {
		t.Fatalf("fill changed=%v err=%v", changed, err)
	}
	if changed, err := st.FillLiveDispatchShadow(ctx, id, 1, 0.40, 0, false, "",
		fillAt, "queue-partial:moved-1tick"); err != nil || changed {
		t.Fatalf("fill retry changed=%v err=%v", changed, err)
	}
	if _, err := st.CancelLiveDispatchShadow(ctx, id, at.Add(4*time.Second), "moved-1tick"); err == nil {
		t.Fatal("filled maker was rewritten as a cancellation")
	}

	settledAt := at.Add(time.Hour)
	if changed, err := st.SettleLiveDispatchShadow(ctx, id, 0, settledAt,
		"venue-scoped canonical settlement", "settlement-hash-2"); err != nil || !changed {
		t.Fatalf("settle changed=%v err=%v", changed, err)
	}
	got, found, err := st.LiveDispatchShadowByID(ctx, id)
	if err != nil || !found || got.State != LiveDispatchShadowSettled || got.RealizedNetKnown {
		t.Fatalf("fee-incomplete settlement row=%+v found=%v err=%v", got, found, err)
	}
	if changed, err := st.SetLiveDispatchShadowFillFee(ctx, id, 0.01,
		"kalshi:maker-registry"); err != nil || !changed {
		t.Fatalf("late fee changed=%v err=%v", changed, err)
	}
	if changed, err := st.SetLiveDispatchShadowFillFee(ctx, id, 0.01,
		"kalshi:maker-registry"); err != nil || changed {
		t.Fatalf("late fee retry changed=%v err=%v", changed, err)
	}
	if _, err := st.SetLiveDispatchShadowFillFee(ctx, id, 0.02,
		"kalshi:maker-registry"); err == nil {
		t.Fatal("exact fee truth was rewritten")
	}
	got, found, err = st.LiveDispatchShadowByID(ctx, id)
	if err != nil || !found || !got.RealizedNetKnown || math.Abs(got.RealizedNet-(-0.41)) > 1e-9 {
		t.Fatalf("completed net row=%+v found=%v err=%v", got, found, err)
	}
}

func TestLiveDispatchShadowMakerCancelKeysAndOpenList(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Microsecond)
	insertLiveDispatchShadowRisk(t, st, "shadow-cancel-1", "shadow-cancel-client-1", "maker", "c", at)
	in := makerLiveDispatchShadow("shadow-cancel-1", "shadow-cancel-client-1", at, false)
	id, inserted, err := st.InsertLiveDispatchShadow(ctx, in)
	if err != nil || !inserted {
		t.Fatalf("insert id=%d inserted=%v err=%v", id, inserted, err)
	}
	open, err := st.ListOpenLiveDispatchShadows(ctx, 20)
	if err != nil || len(open) != 1 || open[0].ID != id {
		t.Fatalf("open=%+v err=%v", open, err)
	}
	cancelAt := at.Add(time.Minute)
	if changed, err := st.CancelLiveDispatchShadow(ctx, id, cancelAt,
		"expire-10m"); err != nil || !changed {
		t.Fatalf("cancel changed=%v err=%v", changed, err)
	}
	if changed, err := st.CancelLiveDispatchShadow(ctx, id, cancelAt,
		"expire-10m"); err != nil || changed {
		t.Fatalf("cancel retry changed=%v err=%v", changed, err)
	}
	if _, err := st.FillLiveDispatchShadow(ctx, id, 2, 0.40, 0.01, true,
		"kalshi:maker-registry", at.Add(2*time.Minute), "trade-through"); err == nil {
		t.Fatal("cancelled maker was rewritten as a fill")
	}
	open, err = st.ListOpenLiveDispatchShadows(ctx, 20)
	if err != nil || len(open) != 0 {
		t.Fatalf("cancelled row remained open: %+v err=%v", open, err)
	}
	all, err := st.ListLiveDispatchShadows(ctx, 20)
	if err != nil || len(all) != 1 || all[0].State != LiveDispatchShadowCancelled {
		t.Fatalf("all=%+v err=%v", all, err)
	}

	// The ledger cannot invent a dispatch with no matching durable pre-send reservation.
	orphan := takerLiveDispatchShadow("missing-risk", "missing-client", at.Add(time.Second))
	if _, _, err := st.InsertLiveDispatchShadow(ctx, orphan); err == nil {
		t.Fatal("orphan live-dispatch shadow bypassed pending-risk authority")
	}

	// Each durable key names exactly one captured dispatch, even when the other key is new.
	insertLiveDispatchShadowRisk(t, st, "shadow-cancel-2", "shadow-cancel-client-2", "maker", "d", at.Add(2*time.Second))
	collision := makerLiveDispatchShadow("shadow-cancel-2", in.ClientOrderID, at.Add(2*time.Second), false)
	if _, _, err := st.InsertLiveDispatchShadow(ctx, collision); err == nil {
		t.Fatal("client-order key was reused across reservations")
	}
}

func TestLiveDispatchShadowBatchReadersAvoidPerRowRiskAndSettlementQueries(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Microsecond)
	insertLiveDispatchShadowRisk(t, st, "shadow-batch-1", "shadow-batch-client-1", "taker", "e", at)
	insertLiveDispatchShadowRisk(t, st, "shadow-batch-2", "shadow-batch-client-2", "taker", "f", at.Add(time.Second))
	firstID, _, err := st.InsertLiveDispatchShadow(ctx,
		takerLiveDispatchShadow("shadow-batch-1", "shadow-batch-client-1", at))
	if err != nil {
		t.Fatal(err)
	}
	secondID, _, err := st.InsertLiveDispatchShadow(ctx,
		takerLiveDispatchShadow("shadow-batch-2", "shadow-batch-client-2", at.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	risks, err := st.LivePendingRiskExecutionsByID(ctx,
		[]string{"shadow-batch-1", "shadow-batch-2", "shadow-batch-1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"shadow-batch-1", "shadow-batch-2"} {
		if len(risks[id].Legs) != 1 || len(risks[id].Events) != 1 ||
			risks[id].Events[0].EventType != LivePendingRiskReserved {
			t.Fatalf("batched pending risk %s = %+v", id, risks[id])
		}
	}
	open, err := st.ListOpenLiveDispatchShadowsAfter(ctx, firstID, 10)
	if err != nil || len(open) != 1 || open[0].ID != secondID {
		t.Fatalf("cursor open rows=%+v err=%v", open, err)
	}
	if _, err := st.RecordVenueSettlement(ctx, "kalshi",
		"KXSHADOW-shadow-batch-1", 1, at.Add(time.Hour), "fixture-final"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RecordVenueSettlement(ctx, "kalshi",
		"KXSHADOW-shadow-batch-2", 0, at.Add(time.Hour), "fixture-final"); err != nil {
		t.Fatal(err)
	}
	settlements, err := st.VenueSettlementsForTickers(ctx, "kalshi",
		[]string{"KXSHADOW-shadow-batch-1", "KXSHADOW-shadow-batch-2"})
	if err != nil || len(settlements) != 2 ||
		settlements["KXSHADOW-shadow-batch-1"].YesValue != 1 ||
		settlements["KXSHADOW-shadow-batch-2"].YesValue != 0 {
		t.Fatalf("batched settlements=%+v err=%v", settlements, err)
	}
}
