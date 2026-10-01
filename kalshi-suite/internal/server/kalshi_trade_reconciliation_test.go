package server

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func testKalshiReconcileFill() kalshi.Fill {
	return kalshi.Fill{
		FillID: "trade-1", OrderID: "order-1", Ticker: "KXTEST-YES",
		Side: "yes", Action: "buy", Count: 3, YesPrice: 42, NoPrice: 58,
		IsTaker: true, CreatedTime: "2026-07-24T16:00:00.123Z",
	}
}

func testKalshiReconcileTrade(t *testing.T, extra string) kalshi.Trade {
	t.Helper()
	raw := `{"trade_id":"trade-1","ticker":"KXTEST-YES","count_fp":"3",` +
		`"yes_price_dollars":"0.42","no_price_dollars":"0.58",` +
		`"created_time":"2026-07-24T16:00:00.123Z"` + extra + `}`
	var trade kalshi.Trade
	if err := json.Unmarshal([]byte(raw), &trade); err != nil {
		t.Fatal(err)
	}
	return trade
}

func newTestKalshiTradeRuntime() *kalshiTradeReconcileRuntime {
	return &kalshiTradeReconcileRuntime{
		cacheMax: 16, private: map[string]kalshiPrivateTradeEvidence{},
		public:   map[string]kalshiPublicTradeEvidence{},
		terminal: map[string]string{}, conflict: map[string]string{},
	}
}

func drainTestKalshiReconciliation(t *testing.T, s *Server) []storage.KalshiTradeReconciliationEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.FenceExecutionShadowForBackup(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListKalshiTradeReconciliations(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestKalshiTradeReconciliationAcceptsEitherArrivalOrder(t *testing.T) {
	for _, publicFirst := range []bool{false, true} {
		name := "private-first"
		if publicFirst {
			name = "public-first"
		}
		t.Run(name, func(t *testing.T) {
			s := testServer(t)
			rt := newTestKalshiTradeRuntime()
			at := time.Date(2026, 7, 24, 16, 0, 1, 0, time.UTC)
			private := kalshiTradeReconcileObservation{
				kind: kalshiTradePrivateObserved, fill: testKalshiReconcileFill(), receivedAt: at,
			}
			public := kalshiTradeReconcileObservation{
				kind:       kalshiTradePublicObserved,
				trade:      testKalshiReconcileTrade(t, `,"taker_outcome_side":"yes"`),
				receivedAt: at.Add(time.Millisecond),
			}
			if publicFirst {
				s.processKalshiTradeObservation(rt, public)
				s.processKalshiTradeObservation(rt, private)
			} else {
				s.processKalshiTradeObservation(rt, private)
				s.processKalshiTradeObservation(rt, public)
			}
			// Exact WS/REST replays remain one logical transition.
			s.processKalshiTradeObservation(rt, private)
			s.processKalshiTradeObservation(rt, public)
			rows := drainTestKalshiReconciliation(t, s)
			if len(rows) == 0 || rows[0].Status != "MATCHED" ||
				rows[0].TradeID != "trade-1" || !rows[0].PrivatePresent ||
				!rows[0].PublicPresent {
				t.Fatalf("rows=%+v", rows)
			}
			if publicFirst && len(rows) != 1 {
				t.Fatalf("public-first wrote public-only or duplicate evidence: %+v", rows)
			}
			if !publicFirst && (len(rows) != 2 || rows[1].Status != "PENDING") {
				t.Fatalf("private-first transition=%+v", rows)
			}
		})
	}
}

func TestKalshiTradeReconciliationKeepsPartialFillTradeIDsSeparateAndCachesBounded(t *testing.T) {
	s := testServer(t)
	rt := newTestKalshiTradeRuntime()
	rt.cacheMax = 2
	at := time.Now().UTC()
	for i := 1; i <= 3; i++ {
		fill := testKalshiReconcileFill()
		fill.FillID = "partial-" + string(rune('0'+i))
		trade := testKalshiReconcileTrade(t, `,"taker_outcome_side":"yes"`)
		trade.TradeID = fill.FillID
		s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
			kind: kalshiTradePrivateObserved, fill: fill, receivedAt: at.Add(time.Duration(i) * time.Millisecond),
		})
		s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
			kind: kalshiTradePublicObserved, trade: trade,
			receivedAt: at.Add(time.Duration(i)*time.Millisecond + time.Microsecond),
		})
	}
	rows := drainTestKalshiReconciliation(t, s)
	matched := map[string]bool{}
	for _, row := range rows {
		if row.Status == "MATCHED" {
			matched[row.TradeID] = true
		}
	}
	if len(matched) != 3 {
		t.Fatalf("partial fills were merged: matched=%v rows=%+v", matched, rows)
	}
	if len(rt.private) > 2 || len(rt.public) > 2 || len(rt.terminal) > 2 {
		t.Fatalf("unbounded caches private=%d public=%d terminal=%d",
			len(rt.private), len(rt.public), len(rt.terminal))
	}
}

func TestKalshiTradeReconciliationMismatchReasonsAreExplicit(t *testing.T) {
	s := testServer(t)
	rt := newTestKalshiTradeRuntime()
	at := time.Now().UTC()
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind: kalshiTradePrivateObserved, fill: testKalshiReconcileFill(), receivedAt: at,
	})
	trade := testKalshiReconcileTrade(t, `,"taker_outcome_side":"no"`)
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind: kalshiTradePublicObserved, trade: trade, receivedAt: at.Add(time.Millisecond),
	})
	rows := drainTestKalshiReconciliation(t, s)
	if len(rows) == 0 || rows[0].Status != "MISMATCH" ||
		rows[0].Reason != "same-trade-id-disagrees:aggressor" {
		t.Fatalf("rows=%+v", rows)
	}
}

func TestKalshiTradeReconciliationMismatchCannotRegressAfterEnrichment(t *testing.T) {
	s := testServer(t)
	rt := newTestKalshiTradeRuntime()
	at := time.Now().UTC()
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind: kalshiTradePrivateObserved, fill: testKalshiReconcileFill(), receivedAt: at,
	})
	// The first public receipt has exact identity/economics but lacks direction, so the comparable
	// legacy private fill conservatively produces a mismatch.
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind: kalshiTradePublicObserved, trade: testKalshiReconcileTrade(t, ""),
		receivedAt: at.Add(time.Millisecond),
	})
	// A later compatible receipt enriches the optional direction. It may be appended as evidence,
	// but it must not erase the mismatch terminal already observed.
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind:       kalshiTradePublicObserved,
		trade:      testKalshiReconcileTrade(t, `,"taker_outcome_side":"yes"`),
		receivedAt: at.Add(2 * time.Millisecond),
	})
	rows := drainTestKalshiReconciliation(t, s)
	if len(rows) < 2 || rows[0].Status != "MISMATCH" ||
		rows[0].Reason != "same-trade-id-disagrees:public-aggressor-missing" {
		t.Fatalf("mismatch regressed after enrichment: %+v", rows)
	}
	for _, row := range rows {
		if row.Status == "MATCHED" {
			t.Fatalf("mismatch was followed by MATCHED: %+v", rows)
		}
	}
}

func TestKalshiTradeReconciliationDuplicateConflictStaysSticky(t *testing.T) {
	s := testServer(t)
	rt := newTestKalshiTradeRuntime()
	at := time.Now().UTC()
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind: kalshiTradePrivateObserved, fill: testKalshiReconcileFill(), receivedAt: at,
	})
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind:       kalshiTradePublicObserved,
		trade:      testKalshiReconcileTrade(t, `,"taker_outcome_side":"yes"`),
		receivedAt: at.Add(time.Millisecond),
	})
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind:       kalshiTradePublicObserved,
		trade:      testKalshiReconcileTrade(t, `,"taker_outcome_side":"no"`),
		receivedAt: at.Add(2 * time.Millisecond),
	})
	// Enrich the original compatible receipt after the contradiction. This exercises the duplicate
	// merge branch which previously ignored rt.conflict and appended MATCHED after MISMATCH.
	s.processKalshiTradeObservation(rt, kalshiTradeReconcileObservation{
		kind: kalshiTradePublicObserved,
		trade: testKalshiReconcileTrade(t,
			`,"taker_outcome_side":"yes","taker_book_side":"bid"`),
		receivedAt: at.Add(3 * time.Millisecond),
	})
	rows := drainTestKalshiReconciliation(t, s)
	if len(rows) == 0 || rows[0].Status != "MISMATCH" ||
		rows[0].Reason != "conflicting-public-trade-duplicate" {
		t.Fatalf("duplicate conflict regressed: %+v", rows)
	}
}

func TestKalshiTradeReconciliationEquivalentDuplicatesEnrich(t *testing.T) {
	at := time.Now().UTC()
	privateA := kalshiPrivateTradeEvidence{
		tradeID: "t", orderID: "o", ticker: "K", side: "YES", qty: 2, price: .4,
		receivedAt: at,
	}
	privateB := privateA
	privateB.action, privateB.fee, privateB.feeKnown = "BUY", .02, true
	mergedPrivate, ok := mergeKalshiPrivateTradeEvidence(privateA, privateB)
	if !ok || mergedPrivate.action != "BUY" || !mergedPrivate.feeKnown ||
		mergedPrivate.fee != .02 {
		t.Fatalf("private enrichment ok=%t merged=%+v", ok, mergedPrivate)
	}

	publicA := kalshiPublicTradeEvidence{
		tradeID: "t", ticker: "K", qty: 2, yesPrice: .4, noPrice: .6,
		aggressor: "yes", receivedAt: at,
	}
	publicB := publicA
	publicB.takerOutcomeSide, publicB.takerBookSide = "yes", "bid"
	mergedPublic, ok := mergeKalshiPublicTradeEvidence(publicA, publicB)
	if !ok || mergedPublic.takerOutcomeSide != "yes" || mergedPublic.takerBookSide != "bid" {
		t.Fatalf("public enrichment ok=%t merged=%+v", ok, mergedPublic)
	}
	publicB.aggressor = "no"
	if _, ok = mergeKalshiPublicTradeEvidence(publicA, publicB); ok {
		t.Fatal("contradictory normalized aggressor was accepted as enrichment")
	}
}

func TestKalshiTradeReconciliationQueueFullDropIsVisible(t *testing.T) {
	s := testServer(t)
	ch := make(chan kalshiTradeReconcileObservation, 1)
	ch <- kalshiTradeReconcileObservation{kind: kalshiTradePublicObserved}
	s.kalshiTradeReconcileHandle.Store(&kalshiTradeReconcileHandle{ch: ch})
	if s.offerKalshiPublicTrade(testKalshiReconcileTrade(t,
		`,"taker_outcome_side":"yes"`)) {
		t.Fatal("full queue accepted an observation")
	}
	if s.kalshiTradeReconcileDropped.Load() != 1 ||
		s.executionShadowDropped.Load() != 1 ||
		s.executionShadowDropCapacity.Load() != 1 {
		t.Fatalf("drop counters reconcile=%d shadow=%d capacity=%d",
			s.kalshiTradeReconcileDropped.Load(), s.executionShadowDropped.Load(),
			s.executionShadowDropCapacity.Load())
	}
}

func TestKalshiTradeReconciliationShutdownAdmissionRefusesWithoutBlocking(t *testing.T) {
	s := testServer(t)
	handle := &kalshiTradeReconcileHandle{ch: make(chan kalshiTradeReconcileObservation, 1)}
	s.kalshiTradeReconcileHandle.Store(handle)
	handle.admission.Lock() // deterministic stand-in for the stop writer fence
	done := make(chan bool, 1)
	go func() {
		done <- s.offerKalshiPrivateFill(testKalshiReconcileFill())
	}()
	select {
	case accepted := <-done:
		if accepted {
			t.Fatal("callback entered while shutdown owned admission")
		}
	case <-time.After(time.Second):
		t.Fatal("callback waited behind shutdown admission")
	}
	handle.closed = true
	handle.admission.Unlock()
	if s.kalshiTradeReconcileDropped.Load() != 1 ||
		s.executionShadowDropped.Load() != 1 ||
		s.executionShadowDropStopping.Load() != 1 {
		t.Fatalf("stopping counters reconcile=%d shadow=%d stopping=%d",
			s.kalshiTradeReconcileDropped.Load(), s.executionShadowDropped.Load(),
			s.executionShadowDropStopping.Load())
	}
}

func TestKalshiRESTFillOffersAreDeduplicatedBoundedAndRetryRefusals(t *testing.T) {
	s := testServer(t)
	s.kalshiTradeReconcileCacheMax = 2
	ch := make(chan kalshiTradeReconcileObservation, 4)
	s.kalshiTradeReconcileHandle.Store(&kalshiTradeReconcileHandle{ch: ch})

	fill := testKalshiReconcileFill()
	fill.Action = ""
	if got := s.offerKalshiRESTFills([]kalshi.Fill{fill, fill}); got != 1 || len(ch) != 1 {
		t.Fatalf("exact REST replay accepted=%d queued=%d want 1/1", got, len(ch))
	}
	fill.Action = "buy" // compatible REST enrichment must not be hidden by exact dedupe
	if got := s.offerKalshiRESTFills([]kalshi.Fill{fill}); got != 1 || len(ch) != 2 {
		t.Fatalf("REST enrichment accepted=%d queued=%d want 1/2", got, len(ch))
	}
	for i := 2; i <= 3; i++ {
		next := fill
		next.FillID = fmt.Sprintf("trade-%d", i)
		if got := s.offerKalshiRESTFills([]kalshi.Fill{next}); got != 1 {
			t.Fatalf("REST fill %d accepted=%d want 1", i, got)
		}
	}
	if len(s.kalshiTradeRESTSeen) > 2 || len(s.kalshiTradeRESTFIFO) > 2 {
		t.Fatalf("REST dedupe unbounded seen=%d fifo=%d",
			len(s.kalshiTradeRESTSeen), len(s.kalshiTradeRESTFIFO))
	}

	blocked := testServer(t)
	full := make(chan kalshiTradeReconcileObservation, 1)
	full <- kalshiTradeReconcileObservation{kind: kalshiTradePublicObserved}
	blocked.kalshiTradeReconcileHandle.Store(&kalshiTradeReconcileHandle{ch: full})
	if got := blocked.offerKalshiRESTFills([]kalshi.Fill{testKalshiReconcileFill()}); got != 0 {
		t.Fatalf("full queue accepted %d REST fills", got)
	}
	<-full
	if got := blocked.offerKalshiRESTFills([]kalshi.Fill{testKalshiReconcileFill()}); got != 1 {
		t.Fatalf("refused REST fill was incorrectly deduped; retry accepted=%d", got)
	}
}

func TestKalshiTradeReconciliationRestartReplayCannotRegressTerminal(t *testing.T) {
	s := testServer(t)
	at := time.Date(2026, 7, 24, 16, 0, 1, 0, time.UTC)
	terminal := storage.KalshiTradeReconciliationEvent{
		EventID: "terminal", TradeID: "trade-1", Status: "MATCHED",
		Reason: "trade-id-ticker-quantity-price-direction-match", ObservedAt: at,
		PrivatePresent: true, PrivateOrderID: "order-1", PrivateTicker: "KXTEST-YES",
		PrivateSide: "YES", PrivateAction: "BUY", PrivateQty: 3, PrivatePrice: .42,
		PrivateIsTaker: true, PrivateReceivedAt: at,
		PublicPresent: true, PublicTicker: "KXTEST-YES", PublicQty: 3,
		PublicYesPrice: .42, PublicNoPrice: .58, PublicAggressor: "yes",
		PublicReceivedAt: at,
	}
	if inserted, err := s.store.AppendKalshiTradeReconciliationEvent(
		context.Background(), terminal); err != nil || !inserted {
		t.Fatalf("seed terminal inserted=%t err=%v", inserted, err)
	}
	s.startKalshiTradeReconciler()
	if !s.offerKalshiPrivateFill(testKalshiReconcileFill()) {
		t.Fatal("restart replay offer refused")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopKalshiTradeReconciler(ctx); err != nil {
		t.Fatal(err)
	}
	rows := drainTestKalshiReconciliation(t, s)
	if len(rows) != 1 || rows[0].Status != "MATCHED" {
		t.Fatalf("terminal regressed after replay: %+v", rows)
	}
}

func TestKalshiTradeReconciliationRecoveryNotFoundStaysPending(t *testing.T) {
	s := testServer(t)
	at := time.Now().UTC()
	pending := storage.KalshiTradeReconciliationEvent{
		EventID: "pending", TradeID: "trade-1", Status: "PENDING",
		Reason: "public-trade-not-observed-yet", ObservedAt: at,
		PrivatePresent: true, PrivateOrderID: "order-1", PrivateTicker: "KXTEST-YES",
		PrivateSide: "YES", PrivateAction: "BUY", PrivateQty: 3, PrivatePrice: .42,
		PrivateIsTaker: true, PrivateReceivedAt: at,
	}
	if inserted, err := s.store.AppendKalshiTradeReconciliationEvent(
		context.Background(), pending); err != nil || !inserted {
		t.Fatalf("seed pending inserted=%t err=%v", inserted, err)
	}
	s.kalshiTradePublicLookup = func(context.Context, string, int) ([]kalshi.Trade, error) {
		return nil, nil
	}
	s.startKalshiTradeReconciler()
	deadline := time.Now().Add(3 * time.Second)
	for {
		rows, err := s.store.ListKalshiTradeReconciliations(context.Background(), 10)
		if err == nil && len(rows) >= 2 &&
			rows[0].Reason == "public-rest-lookup-complete-trade-id-not-found" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not-found recovery not persisted; rows=%+v err=%v", rows, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopKalshiTradeReconciler(ctx); err != nil {
		t.Fatal(err)
	}
}
