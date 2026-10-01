package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR163LiveDispatchShadowCaptureIsRetiredAndHistoricalAPIExcludesPaperPnL(t *testing.T) {
	s := testServer(t)
	quote := liveMirrorQuote{Price: .30, Depth: 9, Tick: .01, BookSource: "final-book"}
	if got := s.liveDispatchShadowCapture(false, "risk", ".300000", "client", "T", "Title", "YES",
		"flow", "taker", 4, .12, quote); got != nil {
		t.Fatal("manual order entered the AUTO shadow ledger")
	}
	if got := s.liveDispatchShadowCapture(true, "risk-y", ".300000", "client-y",
		"T-Y", "Title Y", "YES", "flow", "taker", 4, .12, quote); got != nil {
		t.Fatalf("retired pre-wire fantasy-fill lane returned %+v", got)
	}

	rows, summary, err := s.liveDispatchShadowRows(context.Background(), 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("historical forecast read rows=%d err=%v", len(rows), err)
	}
	if summary["legacy_historical_only"] != true || summary["new_rows_enabled"] != false ||
		summary["paper_pnl_eligible"] != false || summary["system_verdict_eligible"] != false ||
		summary["shadow_net_usd"] != nil {
		t.Fatalf("legacy forecast API remained eligible as Paper/verdict P&L: %+v", summary)
	}
}

func TestR154LiveDispatchShadowWorkerIsBoundedSingleAndNonblocking(t *testing.T) {
	s := testServer(t)
	s.liveDispatchShadowWorkerCapacity = 1
	s.liveDispatchShadowQuietDelay = -1
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var active, maximum, writes atomic.Int64
	s.liveDispatchShadowWrite = func(ctx context.Context, _ storage.LiveDispatchShadow) error {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			writes.Add(1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	row := &storage.LiveDispatchShadow{ReservationID: "queued-1"}
	if !s.enqueueLiveDispatchShadow(row) {
		t.Fatal("first shadow did not enter the worker")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("shadow worker did not start")
	}
	row.ReservationID = "queued-2"
	if !s.enqueueLiveDispatchShadow(row) {
		t.Fatal("second shadow did not enter the bounded queue")
	}
	row.ReservationID = "queued-3"
	if s.enqueueLiveDispatchShadow(row) {
		t.Fatal("full shadow queue blocked or accepted beyond its bound")
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopLiveDispatchShadowWorker(ctx); err != nil {
		t.Fatalf("shadow worker drain: %v", err)
	}
	if writes.Load() != 2 || maximum.Load() != 1 {
		t.Fatalf("writes=%d maximum_concurrent=%d", writes.Load(), maximum.Load())
	}
}

func TestR154LiveDispatchShadowMarkIsCachedOnly(t *testing.T) {
	s := testServer(t)
	s.kobCache = map[string]kobEntry{
		"CACHED": {yesBid: .41, noBid: .57, bookKnown: true, at: time.Now()},
	}
	if got, ok := s.liveDispatchShadowCachedExitMark("CACHED", "YES"); !ok || got != .41 {
		t.Fatalf("cached YES mark=%.2f ok=%v", got, ok)
	}
	if got, ok := s.liveDispatchShadowCachedExitMark("CACHED", "NO"); !ok || got != .57 {
		t.Fatalf("cached NO mark=%.2f ok=%v", got, ok)
	}
	if _, ok := s.liveDispatchShadowCachedExitMark("MISSING", "YES"); ok {
		t.Fatal("missing cache invented a mark or attempted a fallback")
	}
}

func TestR154LivePendingExecutionRequiresExactScopedReceipt(t *testing.T) {
	leg := 0
	row := storage.LivePendingRiskReservation{
		Legs: []storage.LivePendingRiskLeg{{Venue: "kalshi", ClientOrderID: "client"}},
		Events: []storage.LivePendingRiskEvent{
			{EventType: storage.LivePendingRiskSubmitStarted, AttemptKey: "entry", Action: "BUY",
				ClientOrderID: "client", LegIndex: &leg},
			{EventType: storage.LivePendingRiskAck, AttemptKey: "entry", Action: "BUY",
				ClientOrderID: "client", OrderID: "order", LegIndex: &leg},
			{EventType: storage.LivePendingRiskFillSeen, AttemptKey: "entry", OrderID: "order",
				ReceiptSource: "kalshi-create-ack+order-scoped-fills", FilledQty: 4,
				AveragePrice: .30, FeeTotal: .12, Observed: time.Now()},
		},
	}
	state, qty, price, fee, known := livePendingExecution(row)
	if !known || state != "filled" || qty != 4 || price != .30 || fee != .12 {
		t.Fatalf("exact live execution = %q %.2f %.2f %.2f known=%v", state, qty, price, fee, known)
	}
	row.Events[2].ReceiptSource = "estimated"
	if _, _, _, _, known = livePendingExecution(row); known {
		t.Fatal("unscoped/estimated LIVE fill was treated as exact")
	}
	row.Events = append(row.Events, storage.LivePendingRiskEvent{
		EventType: storage.LivePendingRiskTerminalUnfilled, AttemptKey: "entry", OrderID: "order"})
	state, qty, _, _, known = livePendingExecution(row)
	if !known || state != "terminal_unfilled" || qty != 0 {
		t.Fatalf("terminal zero fill = %q %.2f known=%v", state, qty, known)
	}
	row.Events = append(row.Events[:2], storage.LivePendingRiskEvent{
		EventType: storage.LivePendingRiskCleanRejected, AttemptKey: "entry"})
	state, qty, _, _, known = livePendingExecution(row)
	if !known || state != "clean_rejected" || qty != 0 {
		t.Fatalf("clean rejection = %q %.2f known=%v", state, qty, known)
	}
}
