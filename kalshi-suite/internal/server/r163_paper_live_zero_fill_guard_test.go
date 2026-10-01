package server

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func waitR163PaperCorrection(t *testing.T, s *Server) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for s.fundedPaperBookTruthReconcileRunning.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.fundedPaperBookTruthReconcileRunning.Load() {
		t.Fatal("funded-Paper zero-fill correction worker did not finish")
	}
}

func r163InjectFundedPaperLot(t *testing.T, s *Server, key, attemptID, ticker string,
	requireDurableTerminal bool) kfPos {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	lot := kfPos{TS: now, SignalTS: now, DecisionTS: now, FillTS: now,
		Platform: "kalshi", Ticker: ticker, Title: "R163 race fixture", Side: "YES",
		Price: .40, Contracts: 2, Fee: .02, FeeKnown: true, FeeSource: "r163-test",
		FillKind: "taker", FillRule: "genfollow",
		ExecutionShadowAttemptID: attemptID,
		ExecutionBookSource:      "kalshi_ws_full_orderbook:g1:s1:q1"}
	if requireDurableTerminal {
		lot.ExecutionTruthContract = fundedPaperLiveTruthContractV1
		// Deliberately leave the terminal fields empty: this models settlement obtaining its
		// linearization turn before the same attempt has a durable LIVE terminal.
	}
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	if book == nil {
		book = &kfBook{}
		s.gfLoadLocked().Subs[key] = book
	}
	book.Open = append(book.Open, lot)
	s.gfBookDirty = true
	s.gfBookMu.Unlock()
	if err := s.genfollowFlush(); err != nil {
		t.Fatal(err)
	}
	return lot
}

func TestR163FundedPaperDurableLiveTerminalClassification(t *testing.T) {
	qty := 2.0
	cases := []struct {
		name string
		in   storage.ExecutionShadowEvent
		kind string
	}{
		{name: "local no-send remains labelled counterfactual",
			in:   storage.ExecutionShadowEvent{EventID: "no-send", LiveState: "not-sent"},
			kind: fundedPaperLiveTerminalNoSend},
		{name: "positive fill is comparable",
			in: storage.ExecutionShadowEvent{EventID: "fill", LiveState: "full",
				VenueAttempted: true, LiveAuthoritative: true, LiveFilledQty: &qty},
			kind: fundedPaperLiveTerminalFill},
		{name: "authoritative empty IOC is zero-fill",
			in: storage.ExecutionShadowEvent{EventID: "zero", LiveState: "unfilled",
				VenueAttempted: true, LiveAuthoritative: true},
			kind: fundedPaperLiveTerminalZeroFill},
		{name: "authoritative reconciled empty IOC is zero-fill",
			in: storage.ExecutionShadowEvent{EventID: "terminal-zero",
				LiveState: "terminal_unfilled", VenueAttempted: true, LiveAuthoritative: true},
			kind: fundedPaperLiveTerminalZeroFill},
		{name: "clean venue rejection is zero-fill",
			in: storage.ExecutionShadowEvent{EventID: "clean-rejected",
				LiveState: "clean_rejected", VenueAttempted: true, LiveAuthoritative: true},
			kind: fundedPaperLiveTerminalZeroFill},
		{name: "non-authoritative reconciled empty result fails closed",
			in: storage.ExecutionShadowEvent{EventID: "terminal-zero-ambiguous",
				LiveState: "terminal_unfilled", VenueAttempted: true},
			kind: fundedPaperLiveTerminalAmbiguous},
		{name: "local clean rejection label fails closed",
			in: storage.ExecutionShadowEvent{EventID: "local-clean-rejected",
				LiveState: "clean_rejected"},
			kind: fundedPaperLiveTerminalAmbiguous},
		{name: "ambiguous venue result fails closed",
			in: storage.ExecutionShadowEvent{EventID: "ambiguous", LiveState: "ambiguous",
				VenueAttempted: true, LiveFilledQty: &qty},
			kind: fundedPaperLiveTerminalAmbiguous},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fundedPaperLiveTerminalReceiptFromEvent(tc.in); got.Kind != tc.kind {
				t.Fatalf("terminal kind=%q want %q receipt=%+v", got.Kind, tc.kind, got)
			}
		})
	}
}

func TestR163AuthoritativeLiveZeroFillBlocksOnlyDisprovedKalshiBookReceipt(t *testing.T) {
	const attemptID = "r163-authoritative-zero"
	s := testServer(t)
	observedAt := time.Now().UTC()
	disproved := liveMirrorQuote{
		Price: .40, Depth: 20,
		BookSource: "kalshi_ws_full_orderbook:g3:s7:q19",
		ObservedAt: observedAt,
	}
	if !s.rememberFundedPaperLiveZeroFillBook(
		attemptID, "KX-R163", "YES", liveProspectiveIOC,
		"unfilled", true, 0, 1, .40, disproved) {
		t.Fatal("valid direct IOC zero-fill receipt was not published")
	}
	if s.rememberFundedPaperLiveZeroFillBook(
		"r163-nonauthoritative-zero", "KX-R163-NONAUTH", "YES", liveProspectiveIOC,
		"unfilled", false, 0, 1, .40, disproved) {
		t.Fatal("non-authoritative empty acknowledgement published venue truth")
	}
	if why := s.fundedPaperLiveZeroFillReason(
		attemptID, "KX-R163", "YES", .40, disproved); why == "" {
		t.Fatal("funded Paper accepted the exact venue-disproved book receipt")
	}
	older := disproved
	older.BookSource = "kalshi_ws_full_orderbook:g3:s7:q18"
	if why := s.fundedPaperLiveZeroFillReason(
		attemptID, "KX-R163", "YES", .40, older); why == "" {
		t.Fatal("funded Paper accepted a sequence older than the venue-disproved receipt")
	}
	newer := disproved
	newer.BookSource = "kalshi_ws_full_orderbook:g3:s7:q20"
	newer.ObservedAt = time.Now().UTC().Add(time.Second)
	if why := s.fundedPaperLiveZeroFillReason(
		attemptID, "KX-R163", "YES", .40, newer); why != "" {
		t.Fatalf("newer per-ticker sequence stayed blocked: %q", why)
	}
	reconnected := disproved
	reconnected.BookSource = "kalshi_ws_full_orderbook:g4:s1:q1"
	reconnected.ObservedAt = time.Now().UTC().Add(2 * time.Second)
	if why := s.fundedPaperLiveZeroFillReason(
		attemptID, "KX-R163", "YES", .40, reconnected); why != "" {
		t.Fatalf("new socket generation stayed blocked: %q", why)
	}
	if why := s.fundedPaperLiveZeroFillReason(
		attemptID, "KX-R163", "NO", .40, disproved); why != "" {
		t.Fatalf("different outcome was blocked: %q", why)
	}
	if why := s.fundedPaperLiveZeroFillReason(
		attemptID, "KX-R163", "YES", .41, disproved); why != "" {
		t.Fatalf("different limit price was blocked: %q", why)
	}
	waitR163PaperCorrection(t, s)
}

func TestR163LiveZeroFillPublicationNeverWaitsForPaperBookLock(t *testing.T) {
	s := testServer(t)
	s.gfBookMu.Lock()
	returned := make(chan bool, 1)
	go func() {
		returned <- s.rememberFundedPaperLiveZeroFillBook(
			"r163-live-hot-path", "KX-R163-HOT", "YES", liveProspectiveIOC,
			"unfilled", true, 0, 1, .40, liveMirrorQuote{
				Price: .40, Depth: 1,
				BookSource: "kalshi_ws_full_orderbook:g1:s1:q1",
				ObservedAt: time.Now().UTC(),
			})
	}()
	select {
	case published := <-returned:
		if !published {
			s.gfBookMu.Unlock()
			t.Fatal("valid zero-fill receipt was not published")
		}
	case <-time.After(250 * time.Millisecond):
		s.gfBookMu.Unlock()
		t.Fatal("LIVE zero-fill publication waited for funded-Paper book lock")
	}
	s.gfBookMu.Unlock()
	waitR163PaperCorrection(t, s)
}

func TestR163FundedPaperDoesNotFillAuthoritativelyDisprovedDisplayedDepth(t *testing.T) {
	const ticker = "R163PAPER-LIVE-ZERO"
	const attemptID = "r163-live-first-paper-after"
	s, key, quoteFor := r154PaperTakerFixture(t, ticker)
	disproved := liveMirrorQuote{
		Price: .40, Depth: 20,
		BookSource: "kalshi_ws_full_orderbook:g1:s1:q1",
		ObservedAt: time.Now().UTC(),
	}
	// This test targets Paper JSON + main-DB correction truth; suppress the independent shadow
	// writer because the synthetic attempt deliberately has no parent detector row.
	s.executionShadowStoppingAtomic.Store(true)
	if !s.rememberFundedPaperLiveZeroFillBook(
		attemptID, ticker, "YES", liveProspectiveIOC,
		"unfilled", true, 0, 1, .40, disproved) {
		t.Fatal("fixture LIVE zero-fill receipt was not published")
	}
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	s.genfollowConsiderWithShadow(context.Background(), storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R163 same receipt",
		Side: "YES", SignalType: "r154-paper", EntryPrice: .40, ResolveHours: 1,
	}, attemptID)
	s.gfBookMu.Lock()
	open := 0
	if book := s.gfLoadLocked().Subs[key]; book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("venue-disproved displayed depth created %d funded Paper lot(s)", open)
	}
	var terminal string
	if err := s.store.DBForTest().QueryRow(`SELECT detail FROM audit_log
WHERE category='system-route' AND message LIKE ? ORDER BY id DESC LIMIT 1`,
		"PAPER-ZERO-FILL%"+ticker+"%").Scan(&terminal); err != nil {
		t.Fatal(err)
	}
	var receipt map[string]any
	if err := json.Unmarshal([]byte(terminal), &receipt); err != nil {
		t.Fatal(err)
	}
	if got := receipt["reason"]; got !=
		"execution-book-disproved-by-authoritative-live-zero-fill" {
		t.Fatalf("Paper zero-fill reason=%v", got)
	}
	waitR163PaperCorrection(t, s)
}

func TestR163FundedPaperDoesNotReuseZeroFilledAttemptAfterBookSequenceAdvances(t *testing.T) {
	const ticker = "R163PAPER-LIVE-NEWER"
	const attemptID = "r163-newer-sequence"
	s, key, quoteFor := r154PaperTakerFixture(t, ticker)
	if !s.rememberFundedPaperLiveZeroFillBook(attemptID, ticker, "YES", liveProspectiveFOK,
		"unfilled", true, 0, 1, .40,
		liveMirrorQuote{
			Price: .40, Depth: 20,
			BookSource: "kalshi_ws_full_orderbook:g1:s1:q1",
			ObservedAt: time.Now().UTC(),
		}) {
		t.Fatal("fixture LIVE FOK zero-fill receipt was not published")
	}
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		out, source, ok := quoteFor(in, .38, .40, 20)
		out.BookSource = "kalshi_ws_full_orderbook:g1:s1:q2"
		age := 0.0
		out.BookQuoteAgeS = &age
		return out, source, ok
	}
	s.genfollowConsiderWithShadow(context.Background(), storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R163 newer receipt",
		Side: "YES", SignalType: "r154-paper", EntryPrice: .40, ResolveHours: 1,
	}, attemptID)
	s.gfBookMu.Lock()
	open := 0
	if book := s.gfLoadLocked().Subs[key]; book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("same zero-filled LIVE attempt created %d Paper lots after sequence advance", open)
	}
	waitR163PaperCorrection(t, s)
}

func TestR163PaperFirstThenAuthoritativeLiveZeroFillRemovesExactLot(t *testing.T) {
	const (
		ticker    = "R163PAPER-FIRST"
		attemptID = "r163-paper-first-live-zero-after"
	)
	s, key, _ := r154PaperTakerFixture(t, ticker)
	sig := storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R163 Paper wins append boundary",
		Side: "YES", SignalType: "r154-paper", EntryPrice: .40, ResolveHours: 1,
	}
	r163InjectFundedPaperLot(t, s, key, attemptID, ticker, false)
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	if book == nil || len(book.Open) != 1 {
		s.gfBookMu.Unlock()
		t.Fatalf("Paper-first setup did not persist one lot: %+v", book)
	}
	if got := book.Open[0]; got.ExecutionShadowAttemptID != attemptID ||
		got.ExecutionBookSource != "kalshi_ws_full_orderbook:g1:s1:q1" {
		s.gfBookMu.Unlock()
		t.Fatalf("Paper lot lacks exact execution lineage: %+v", got)
	}
	s.gfBookMu.Unlock()

	disproved := liveMirrorQuote{
		Price: .40, Depth: 20,
		BookSource: "kalshi_ws_full_orderbook:g1:s1:q1",
		ObservedAt: time.Now().UTC(),
	}
	s.executionShadowStoppingAtomic.Store(true)
	if !s.rememberFundedPaperLiveZeroFillBook("different-attempt", ticker, "YES",
		liveProspectiveIOC, "unfilled", true, 0, 1, .40, disproved) {
		t.Fatal("different-attempt zero-fill receipt was not accepted")
	}
	waitR163PaperCorrection(t, s)
	s.gfBookMu.Lock()
	openAfterWrongAttempt := len(s.gfLoadLocked().Subs[key].Open)
	s.gfBookMu.Unlock()
	if openAfterWrongAttempt != 1 {
		t.Fatalf("loose match removed Paper lot for a different attempt: open=%d",
			openAfterWrongAttempt)
	}

	if !s.rememberFundedPaperLiveZeroFillBook(attemptID, ticker, "YES",
		liveProspectiveIOC, "unfilled", true, 0, 1, .40, disproved) {
		t.Fatal("matching authoritative zero-fill receipt was not accepted")
	}
	waitR163PaperCorrection(t, s)
	s.gfBookMu.Lock()
	open := len(s.gfLoadLocked().Subs[key].Open)
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("Paper-first lot survived authoritative same-receipt zero-fill: open=%d", open)
	}

	raw, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "genfollow_book.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted gfBookState
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Subs[key] == nil || len(persisted.Subs[key].Open) != 0 {
		t.Fatalf("corrected Paper lot survived on disk: %+v", persisted.Subs[key])
	}
	var corrections int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM audit_log
WHERE category='genfollow' AND message LIKE 'PAPER-CORRECTION%'`).Scan(&corrections); err != nil {
		t.Fatal(err)
	}
	if corrections != 1 {
		t.Fatalf("Paper correction audit count=%d, want 1", corrections)
	}

	// The tombstone stays resident after cleanup, so delayed Paper work cannot resurrect the lot.
	s.genfollowConsiderWithShadow(context.Background(), sig, attemptID)
	s.gfBookMu.Lock()
	open = len(s.gfLoadLocked().Subs[key].Open)
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("delayed Paper replay resurrected corrected lot: open=%d", open)
	}
}

func TestR163SettlementFirstDefersUnknownAttemptThenZeroFillRemovesWithoutPnL(t *testing.T) {
	const (
		ticker    = "R163-SETTLE-FIRST-ZERO"
		attemptID = "r163-settlement-first-live-zero-after"
	)
	s, key, _ := r154PaperTakerFixture(t, ticker)
	r163InjectFundedPaperLot(t, s, key, attemptID, ticker, true)
	mustInsertResolvedSignal(t, s, "kalshi", ticker, "YES", .40, 1)

	// Settlement obtains its complete probe and apply turn first. Contract-v1 says the same
	// attempt's durable LIVE terminal is still unknown, so no open->closed/P&L/relation transition
	// is permitted.
	s.settleGenfollowBook(context.Background())
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	openBeforeZero, closedBeforeZero := len(book.Open), len(book.Closed)
	netBeforeZero, winsBeforeZero, lossesBeforeZero := book.Net, book.Wins, book.Losses
	s.gfBookMu.Unlock()
	if openBeforeZero != 1 || closedBeforeZero != 0 || netBeforeZero != 0 ||
		winsBeforeZero != 0 || lossesBeforeZero != 0 {
		t.Fatalf("unknown LIVE terminal crossed settlement: open=%d closed=%d net=%v W/L=%d/%d",
			openBeforeZero, closedBeforeZero, netBeforeZero, winsBeforeZero, lossesBeforeZero)
	}
	var relationOutcomes int
	if err := s.store.DBForTest().QueryRow(
		`SELECT COUNT(*) FROM funded_relation_outcomes`).Scan(&relationOutcomes); err != nil {
		t.Fatal(err)
	}
	if relationOutcomes != 0 {
		t.Fatalf("deferred false lot created %d funded relation outcome(s)", relationOutcomes)
	}

	if !s.rememberFundedPaperLiveZeroFillBook(attemptID, ticker, "YES",
		liveProspectiveIOC, "unfilled", true, 0, 2, .40, liveMirrorQuote{
			Price: .40, Depth: 20,
			BookSource: "kalshi_ws_full_orderbook:g1:s1:q1",
			ObservedAt: time.Now().UTC(),
		}) {
		t.Fatal("settlement-first fixture zero-fill was not published")
	}
	waitR163PaperCorrection(t, s)
	s.gfBookMu.Lock()
	book = s.gfLoadLocked().Subs[key]
	openAfterZero, closedAfterZero := len(book.Open), len(book.Closed)
	netAfterZero, winsAfterZero, lossesAfterZero := book.Net, book.Wins, book.Losses
	s.gfBookMu.Unlock()
	if openAfterZero != 0 || closedAfterZero != 0 || netAfterZero != 0 ||
		winsAfterZero != 0 || lossesAfterZero != 0 {
		t.Fatalf("zero-fill correction left false economics: open=%d closed=%d net=%v W/L=%d/%d",
			openAfterZero, closedAfterZero, netAfterZero, winsAfterZero, lossesAfterZero)
	}
	if err := s.store.DBForTest().QueryRow(
		`SELECT COUNT(*) FROM funded_relation_outcomes`).Scan(&relationOutcomes); err != nil {
		t.Fatal(err)
	}
	if relationOutcomes != 0 {
		t.Fatalf("settlement-first zero-fill left %d funded relation outcome(s)", relationOutcomes)
	}
}

func TestR163LegacyAttemptBoundLotWithoutTruthContractIsNotStranded(t *testing.T) {
	const (
		ticker    = "R163-LEGACY-SETTLES"
		attemptID = "r163-legacy-before-terminal-contract"
	)
	s, key, _ := r154PaperTakerFixture(t, ticker)
	r163InjectFundedPaperLot(t, s, key, attemptID, ticker, false)
	mustInsertResolvedSignal(t, s, "kalshi", ticker, "YES", .40, 1)
	s.settleGenfollowBook(context.Background())
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	open, closed := len(book.Open), len(book.Closed)
	s.gfBookMu.Unlock()
	if open != 0 || closed != 1 {
		t.Fatalf("legacy attempt-bound lot was stranded: open=%d closed=%d", open, closed)
	}
}

func TestR163BootRepairsPaperLotFromDurableReleasedZeroFill(t *testing.T) {
	const (
		ticker        = "R163BOOT-ZERO"
		attemptID     = "r163-durable-zero-fill-attempt"
		reservationID = "risk-r163-durable-zero-fill"
		orderID       = "order-r163-zero"
		clientOrderID = "client-r163-zero"
	)
	s, key, _ := r154PaperTakerFixture(t, ticker)
	r163InjectFundedPaperLot(t, s, key, attemptID, ticker, false)

	now := time.Now().UTC()
	hash := sha256.Sum256([]byte(reservationID))
	inserted, err := s.store.InsertLivePendingRisk(context.Background(),
		storage.LivePendingRiskIntent{
			ReservationID: reservationID, Created: now, BaselineObserved: now.Add(-time.Second),
			Product: "single", DispatchSource: "r163-test", SystemID: "r154-paper", Route: "taker",
			PrincipalUSD: .40, CostUSD: .40, RequestHash: fmt.Sprintf("%x", hash),
			ProofJSON: `{}`, BaselineReceiptJSON: `{}`,
		}, []storage.LivePendingRiskLeg{{
			ReservationID: reservationID, Index: 0, Venue: "kalshi", Ticker: ticker,
			Side: "YES", Action: "BUY", ClientOrderID: clientOrderID,
			Quantity: 1, LimitPrice: .40, ExpectedPositionQty: 1,
		}}, []storage.LivePendingRiskCluster{{
			ReservationID: reservationID, Index: 0, Venue: "kalshi",
			ClusterKey: "kalshi:r163boot", MappingVersion: "r163-test", ReservedUSD: .40,
		}})
	if err != nil || !inserted {
		t.Fatalf("insert durable risk fixture: inserted=%v err=%v", inserted, err)
	}
	legIndex := 0
	if err := s.r148AppendRiskEvent(context.Background(), reservationID,
		storage.LivePendingRiskSubmitStarted, "entry", "BUY", clientOrderID, "",
		"kalshi-create-order", "network mutation begins", &legIndex, 0, .40, 0,
		map[string]any{"request": "r163"}); err != nil {
		t.Fatal(err)
	}
	evidence := map[string]any{
		"execution_shadow_attempt_id": attemptID,
		"execution_book_source":       "kalshi_ws_full_orderbook:g1:s1:q1",
		"execution_book_observed_at":  now.Format(time.RFC3339Nano),
		"execution_limit_price":       .40,
		"time_in_force":               liveProspectiveIOC,
		"response": map[string]any{
			"order_id": orderID, "fill_count": "0.00", "remaining_count": "1.00",
		},
	}
	if err := s.r148AppendRiskEvent(context.Background(), reservationID,
		storage.LivePendingRiskAck, "entry", "BUY", clientOrderID, orderID,
		"kalshi-create-order-receipt", "exact acknowledgement", &legIndex, 0, .40, 0,
		evidence); err != nil {
		t.Fatal(err)
	}
	if err := s.r148RiskTerminalUnfilled(context.Background(), reservationID, "entry", orderID,
		"kalshi-create-order-receipt", "authoritative immediate zero fill", evidence); err != nil {
		t.Fatal(err)
	}

	// Simulate a process dying before the async correction: discard memory truth and force the
	// follower to reload its still-false lot from disk.
	s.gfBookMu.Lock()
	s.gfBook = nil
	s.gfBookDirty = false
	s.gfBookMu.Unlock()
	s.fundedPaperBookTruthMu.Lock()
	s.fundedPaperLiveZeroFillBook = nil
	s.fundedPaperBookTruthMu.Unlock()
	s.executionShadowStoppingAtomic.Store(true)
	corrected, err := s.recoverFundedPaperLiveZeroFillsAtBoot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if corrected != 1 {
		t.Fatalf("boot corrected=%d, want 1", corrected)
	}
	s.gfBookMu.Lock()
	open := len(s.gfLoadLocked().Subs[key].Open)
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("durable boot proof did not remove false Paper lot: open=%d", open)
	}
}
