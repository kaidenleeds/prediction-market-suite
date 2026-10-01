package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR164SelectedFundedPaperIgnoresPendingQueuesButWaitsForActiveCash(t *testing.T) {
	s := testServer(t)

	// Model continuous AUTO traffic: pending work never drains while the selected attempt's
	// funded-Paper comparison is due.
	s.liveSignalIntentChannel() <- liveSignalIntent{}
	s.liveMirrorMu.Lock()
	s.liveMirrorQ = append(s.liveMirrorQ, liveMirrorCandidate{Ticker: "R164-PENDING"})
	s.liveMirrorMu.Unlock()

	s.liveDispatchShadowLiveActive.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan bool, 1)
	go func() { result <- s.waitForFundedPaperCashSection(ctx) }()

	select {
	case <-result:
		t.Fatal("selected funded Paper overlapped an active real-money section")
	case <-time.After(40 * time.Millisecond):
	}

	s.liveDispatchShadowLiveActive.Store(0)
	select {
	case ok := <-result:
		if !ok {
			t.Fatal("selected funded Paper did not resume after active cash completed")
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("continuous pending queues starved selected funded Paper")
	}

	// The general diagnostic gate deliberately remains stricter; this verifies the scheduling
	// exception is scoped only to selected same-attempt funded Paper.
	strictCtx, strictCancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer strictCancel()
	if s.waitForLiveDispatchShadowQuiet(strictCtx) {
		t.Fatal("general diagnostic quiet gate ignored pending LIVE work")
	}
}

func TestR164SelectedFundedPaperWorkerRunsWithContinuouslyPendingSignals(t *testing.T) {
	s, _ := newExecutionShadowTestServer(t)
	s.genfollowPaperQuietTimeout = time.Second
	triggerAt := time.Now().UTC().Add(-genfollowPaperLiveFirstGrace)
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "KXR164-PAPER-NO-STARVE",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40,
	}
	attemptID := s.executionShadowBeginSignal(sig, .05, triggerAt)
	if attemptID == "" {
		t.Fatal("could not begin selected Paper attempt")
	}
	candidate := liveMirrorCandidate{
		Platform: sig.Platform, Ticker: sig.Ticker, Side: sig.Side,
		Family: sig.SignalType, At: triggerAt, ShadowAttemptID: attemptID,
		ProspectiveQty: 1, ProspectiveTimeInForce: liveProspectiveIOC,
	}
	quote := liveMirrorQuote{
		Price: .40, Depth: 5, Tick: .01,
		BookSource: "kalshi_ws_full_orderbook:g1:s2:q3",
		ObservedAt: time.Now().UTC(),
	}
	s.executionShadowRecordLiveResult(candidate, quote, "", "", "not-sent",
		"local-final-guard", 1, 0, 0, 0, true, false, false,
		"test selected attempt terminal", nil)

	terminalCtx, terminalCancel := context.WithTimeout(context.Background(), time.Second)
	defer terminalCancel()
	if _, found, err := s.waitForFundedPaperLiveTerminal(terminalCtx, attemptID); err != nil || !found {
		t.Fatalf("selected attempt terminal unavailable: found=%v err=%v", found, err)
	}
	canonical := s.executionShadowEvent(attemptID, "counterfactual-execution-terminal",
		livePolicyMirrorModeledFill, "", time.Now().UTC())
	canonical.EventID = "r170-canonical-priority-fixture"
	canonical.BookSource = quote.BookSource
	canonical.ShadowState = livePolicyMirrorModeledFill
	canonical.ShadowFilledQty = floatPtrShadow(1)
	canonical.ShadowFillPrice = executionShadowPricePtr(.40)
	canonical.ShadowFee = executionShadowNonnegativePtr(.01)
	canonical.ShadowFeeSource = "fixture-exact-fee"
	if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &canonical}) {
		t.Fatal("could not enqueue canonical execution fixture")
	}
	s.wakeExecutionShadowWriter()
	if _, found, err := s.waitForCanonicalSystemExecutionTerminal(
		terminalCtx, attemptID); err != nil || !found {
		t.Fatalf("canonical execution terminal unavailable: found=%v err=%v", found, err)
	}

	// Keep unrelated AUTO work pending throughout the selected Paper run.
	s.liveSignalIntentChannel() <- liveSignalIntent{}
	s.liveDispatchShadowLiveActive.Store(1)
	started := make(chan struct{}, 1)
	in := make(chan genfollowPaperIntent, 1)
	in <- genfollowPaperIntent{
		EnqueuedAt: triggerAt,
		Signals: []genfollowPaperSignal{{
			Signal: sig, SignalAt: triggerAt, ShadowAttemptID: attemptID,
			testRun: func(context.Context) { started <- struct{}{} },
		}},
	}
	close(in)
	done := make(chan struct{})
	go s.runGenfollowPaperWorkers(context.Background(), in, done)

	select {
	case <-started:
		t.Fatal("selected Paper worker overlapped active cash")
	case <-time.After(40 * time.Millisecond):
	}
	s.liveDispatchShadowLiveActive.Store(0)
	select {
	case <-started:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("pending signal queue starved selected Paper worker")
	}
	// The worker's shutdown audit deliberately uses the unchanged strict quiet gate. Drain the
	// synthetic pending item after proving the selected comparison itself was not starved.
	select {
	case <-s.liveSignalIntentChannel():
	default:
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("selected Paper worker did not finish")
	}
}
