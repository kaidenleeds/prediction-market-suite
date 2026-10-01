package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

func testKalshiTradeReconciliationEvent(eventID, status, reason string,
	public bool) KalshiTradeReconciliationEvent {
	at := time.Date(2026, 7, 24, 16, 0, 0, 123000000, time.UTC)
	event := KalshiTradeReconciliationEvent{
		EventID: eventID, TradeID: "trade-1", Status: status, Reason: reason,
		ObservedAt: at, ReservationID: "reservation-1", AttemptID: "attempt-1",
		PrivatePresent: true, PrivateOrderID: "order-1", PrivateTicker: "KXTEST-YES",
		PrivateSide: "YES", PrivateAction: "BUY", PrivateQty: 3,
		PrivatePrice: 0.42, PrivateFee: 0.03, PrivateFeeKnown: true,
		PrivateIsTaker: true, PrivateSourceAt: at.Add(-time.Millisecond),
		PrivateReceivedAt: at, PublicPresent: public,
		Evidence: map[string]any{"identity_basis": "kalshi_trade_id"},
	}
	if public {
		event.PublicTicker, event.PublicQty = "KXTEST-YES", 3
		event.PublicYesPrice, event.PublicNoPrice = 0.42, 0.58
		event.PublicTakerOutcomeSide, event.PublicTakerBookSide = "yes", "bid"
		event.PublicAggressor = "yes"
		event.PublicSourceAt, event.PublicReceivedAt = at.Add(-time.Millisecond), at
	}
	return event
}

func TestKalshiTradeReconciliationAppendOnlyIdempotentAndTerminalDoesNotRegress(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	pending := testKalshiTradeReconciliationEvent(
		"event-pending", "PENDING", "public-trade-not-observed-yet", false)
	inserted, err := st.AppendKalshiTradeReconciliationEvent(ctx, pending)
	if err != nil || !inserted {
		t.Fatalf("append pending inserted=%t err=%v", inserted, err)
	}
	inserted, err = st.AppendKalshiTradeReconciliationEvent(ctx, pending)
	if err != nil || inserted {
		t.Fatalf("idempotent replay inserted=%t err=%v", inserted, err)
	}

	reused := pending
	reused.Reason = "different evidence under same id"
	if _, err = st.AppendKalshiTradeReconciliationEvent(ctx, reused); err == nil ||
		!strings.Contains(err.Error(), "reused") {
		t.Fatalf("event-id evidence reuse err=%v", err)
	}

	matched := testKalshiTradeReconciliationEvent(
		"event-matched", "MATCHED", "trade-id-ticker-quantity-price-direction-match", true)
	matched.ObservedAt = matched.ObservedAt.Add(time.Second)
	inserted, err = st.AppendKalshiTradeReconciliationEvent(ctx, matched)
	if err != nil || !inserted {
		t.Fatalf("append matched inserted=%t err=%v", inserted, err)
	}

	replayedPending := pending
	replayedPending.EventID = "event-replayed-pending"
	replayedPending.ObservedAt = matched.ObservedAt.Add(time.Second)
	inserted, err = st.AppendKalshiTradeReconciliationEvent(ctx, replayedPending)
	if err != nil || inserted {
		t.Fatalf("terminal->pending regression inserted=%t err=%v", inserted, err)
	}
	latest, err := st.ListLatestKalshiTradeReconciliations(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 1 || latest[0].Status != "MATCHED" ||
		latest[0].EventID != "event-matched" {
		t.Fatalf("latest=%+v", latest)
	}

	db, err := st.executionShadowHandle()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE kalshi_trade_reconciliation_events
SET reason='mutated' WHERE event_id='event-matched'`); err == nil ||
		!strings.Contains(err.Error(), "immutable") {
		t.Fatalf("immutable update err=%v", err)
	}
	if _, err = db.ExecContext(ctx, `DELETE FROM kalshi_trade_reconciliation_events
WHERE event_id='event-matched'`); err == nil ||
		!strings.Contains(err.Error(), "append-preserved") {
		t.Fatalf("append-preserved delete err=%v", err)
	}
}

func TestKalshiTradeReconciliationLatestPendingIsRestartSeed(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	pending := testKalshiTradeReconciliationEvent(
		"event-pending", "PENDING", "public-rest-lookup-complete-trade-id-not-found", false)
	if inserted, err := st.AppendKalshiTradeReconciliationEvent(ctx, pending); err != nil || !inserted {
		t.Fatalf("append inserted=%t err=%v", inserted, err)
	}
	rows, err := st.ListPendingKalshiTradeReconciliations(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].TradeID != pending.TradeID ||
		rows[0].PrivateFee != pending.PrivateFee || !rows[0].PrivateFeeKnown {
		t.Fatalf("pending restart seed=%+v", rows)
	}
}

func TestKalshiTradeReconciliationStorageKeepsMismatchTerminalSticky(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	mismatch := testKalshiTradeReconciliationEvent(
		"event-mismatch", "MISMATCH", "same-trade-id-disagrees:quantity", true)
	if inserted, err := st.AppendKalshiTradeReconciliationEvent(ctx, mismatch); err != nil || !inserted {
		t.Fatalf("append mismatch inserted=%t err=%v", inserted, err)
	}
	matched := testKalshiTradeReconciliationEvent(
		"event-later-match", "MATCHED", "trade-id-ticker-quantity-price-direction-match", true)
	matched.ObservedAt = mismatch.ObservedAt.Add(time.Second)
	if inserted, err := st.AppendKalshiTradeReconciliationEvent(ctx, matched); err != nil || inserted {
		t.Fatalf("mismatch->matched regression inserted=%t err=%v", inserted, err)
	}
	latest, err := st.ListLatestKalshiTradeReconciliations(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 1 || latest[0].Status != "MISMATCH" ||
		latest[0].EventID != mismatch.EventID {
		t.Fatalf("latest mismatch terminal regressed: %+v", latest)
	}
}
