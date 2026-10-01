package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r163ArmKalshiFlowYES(t *testing.T, s *Server, ticker string) {
	t.Helper()
	fee, _, ok := s.fillFeeReceipt("kalshi", ticker, false, 1, .40)
	if !ok {
		t.Fatal("kalshi-flow fixture exact fee unavailable")
	}
	injectVerdicts(s, []verdictEnt{{
		Family: "taker:kalshi-flow@kalshi", SourceFamily: "kalshi-flow",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 150, Mean: .08, MeanAsk: .40, FeePC: fee,
	}})
	key := gfRosterKey("kalshi-flow", "kalshi", "YES", "taker")
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), SubShare: map[string]float64{key: 100}}
	s.swMu.Unlock()
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
		c.Risk.LiveSystemCanaryAllowlist = "kalshi|kalshi-flow|YES|taker"
	})
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	r147InsertAllocationHistory(t, s, "kalshi-flow", "YES",
		s.liveAllocationMinMarkets(), s.liveAllocationMinMarkets())
}

func r163InstallPaperCapture(s *Server, capacity int,
	prefill bool) chan genfollowPaperIntent {
	ch := make(chan genfollowPaperIntent, capacity)
	if prefill {
		ch <- genfollowPaperIntent{Signals: []genfollowPaperSignal{{
			Signal: storage.Signal{Platform: "kalshi", Ticker: "R163-PREFILL",
				Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40},
		}}}
	}
	s.genfollowPaperWorkerMu.Lock()
	s.genfollowPaperIntentCh = ch
	s.genfollowPaperWorkerMu.Unlock()
	return ch
}

func r163RemovePaperCapture(s *Server) {
	s.genfollowPaperWorkerMu.Lock()
	s.genfollowPaperIntentCh = nil
	s.genfollowPaperPriorityCh = nil
	s.genfollowPaperWorkerDone = nil
	s.genfollowPaperWorkerCancel = nil
	s.genfollowPaperWorkerStopping = false
	s.genfollowPaperWorkerMu.Unlock()
}

func TestR165RawEventCashRetirementStillFansSameIDToFundedPaper(t *testing.T) {
	const ticker = "R163EVENT-PASS"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	r163ArmKalshiFlowYES(t, s, ticker)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	paperCh := r163InstallPaperCapture(s, 2, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })

	signalAt := time.Now().Add(-25 * time.Millisecond)
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "R163 event pass-only fanout", Side: "YES", SignalType: "kalshi-flow",
		EntryPrice: .40, ResolveHours: 1, ExecExpr: r147InputReceiptExpr("K", signalAt)}
	if !s.queueLiveSignalIntentAt(sig, .08, signalAt) {
		t.Fatal("raw event LIVE intent was not published")
	}
	intent := <-s.liveSignalIntentChannel()
	if intent.ShadowAttemptID == "" {
		t.Fatal("raw event LIVE intent lost its attempt id")
	}
	if intent.FundedPaperPreScheduled {
		t.Fatal("raw event falsely claimed that funded Paper was already scheduled")
	}
	if len(paperCh) != 0 {
		t.Fatal("raw event reached funded Paper before cash preflight")
	}

	s.executionShadowMu.Lock()
	pendingBeforePass := append([]executionShadowWrite(nil), s.executionShadowPending...)
	s.executionShadowMu.Unlock()
	if len(pendingBeforePass) != 2 || pendingBeforePass[1].event == nil {
		t.Fatalf("raw event detector group=%+v want attempt+event", pendingBeforePass)
	}
	detector := pendingBeforePass[1].event
	if !strings.Contains(strings.ToLower(detector.Reason), "identical live/paper opportunity") ||
		detector.Evidence["shared_opportunity_receipt"] != true ||
		detector.Evidence["live_detector_opportunity_recorded"] != true ||
		detector.Evidence["paper_detector_opportunity_recorded"] != true ||
		detector.Evidence["paper_fanout"] != false ||
		detector.Evidence["paper_execution_scheduled"] != false ||
		detector.Evidence["paper_execution_boundary"] != "after-live-preflight" ||
		detector.Evidence["paper_branch"] !=
			"opportunity-recorded; funded execution waits for LIVE preflight" {
		t.Fatalf("raw detector lost shared opportunity or falsely claimed Paper execution: %+v",
			detector)
	}

	started := time.Now()
	if s.processLiveAllowlistedSignalIntentJob(context.Background(), intent) {
		t.Fatal("R165 cash-retired preflight unexpectedly admitted the event")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("pass-only Paper offer delayed cash preflight by %v", elapsed)
	}
	var paperIntent genfollowPaperIntent
	select {
	case paperIntent = <-paperCh:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cash-preflight pass did not offer the signal to funded Paper")
	}
	if len(paperIntent.Signals) != 1 ||
		paperIntent.Signals[0].ShadowAttemptID != intent.ShadowAttemptID {
		t.Fatalf("LIVE/Paper lineage differs: live=%q paper=%+v",
			intent.ShadowAttemptID, paperIntent.Signals)
	}
	if got := paperIntent.Signals[0].Signal; got.Ticker != sig.Ticker ||
		got.Side != sig.Side || got.EntryPrice != sig.EntryPrice || got.ExecExpr != sig.ExecExpr {
		t.Fatalf("Paper did not receive the identical event signal: got=%+v want=%+v", got, sig)
	}

	r163RemovePaperCapture(s)
	releaseShadow()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestR163RejectedRawEventNeverFansFundedPaper(t *testing.T) {
	const ticker = "R163EVENT-REJECT"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	r163ArmKalshiFlowYES(t, s, ticker)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .98, .99, 20)
	}
	paperCh := r163InstallPaperCapture(s, 1, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })

	sig := storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "R163 rejected event", Side: "YES", SignalType: "kalshi-flow",
		EntryPrice: .40, ResolveHours: 1}
	if !s.queueLiveSignalIntentAt(sig, .08, time.Now()) {
		t.Fatal("raw event did not reach the LIVE preflight queue")
	}
	intent := <-s.liveSignalIntentChannel()
	if s.processLiveAllowlistedSignalIntentJob(context.Background(), intent) {
		t.Fatal("out-of-range executable quote passed cash preflight")
	}
	if len(paperCh) != 0 {
		t.Fatal("rejected cash candidate reached funded Paper")
	}
	if s.genfollowPaperWorkerDropped.Load() != 0 {
		t.Fatal("rejected cash candidate touched the Paper worker")
	}
}

func TestR165PeriodicSelectedPaperCollectsAfterCashRetirement(t *testing.T) {
	const ticker = "R163PERIODIC-NO-DUP"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	r163ArmKalshiFlowYES(t, s, ticker)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	paperCh := r163InstallPaperCapture(s, 2, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })
	signalAt := time.Now()
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "R163 periodic pre-scheduled Paper", Side: "YES", SignalType: "kalshi-flow",
		EntryPrice: .40, ResolveHours: 1, ExecExpr: r147InputReceiptExpr("K", signalAt)}

	s.genfollowTriggeredSystems(context.Background(), sig)
	intent := <-s.liveSignalIntentChannel()
	if !intent.FundedPaperPreScheduled {
		t.Fatal("periodic LIVE intent did not reserve its terminal-gated Paper comparison")
	}
	if len(paperCh) != 0 {
		t.Fatal("selected periodic signal reached funded Paper before cash preflight")
	}
	if s.processLiveAllowlistedSignalIntentJob(context.Background(), intent) {
		t.Fatal("R165 cash-retired periodic candidate unexpectedly entered the money queue")
	}
	var firstPaper genfollowPaperIntent
	select {
	case firstPaper = <-paperCh:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cash-retired periodic opportunity did not fan out to corrected Paper")
	}
	if len(firstPaper.Signals) != 1 ||
		firstPaper.Signals[0].ShadowAttemptID != intent.ShadowAttemptID {
		t.Fatalf("periodic LIVE/Paper lineage differs: live=%q paper=%+v",
			intent.ShadowAttemptID, firstPaper.Signals)
	}
	if len(paperCh) != 0 {
		t.Fatal("cash retirement duplicated the periodic Paper branch")
	}
}

func TestR164PeriodicSelectedPaperStillCollectsWhileAutoOff(t *testing.T) {
	const ticker = "R164PERIODIC-AUTO-OFF"
	s, _, _ := r154PaperTakerFixture(t, ticker)
	r163ArmKalshiFlowYES(t, s, ticker)
	s.liveMu.Lock()
	s.liveAuto = false
	s.liveMu.Unlock()
	paperCh := r163InstallPaperCapture(s, 1, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })

	sig := storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "R164 periodic AUTO-off Paper", Side: "YES", SignalType: "kalshi-flow",
		EntryPrice: .40, ResolveHours: 1}
	s.genfollowTriggeredSystems(context.Background(), sig)
	select {
	case paperIntent := <-paperCh:
		direct := 0
		for _, queued := range paperIntent.Signals {
			if queued.Signal.SignalType == sig.SignalType && queued.Signal.Side == sig.Side &&
				queued.Signal.Ticker == ticker {
				direct++
			}
		}
		if direct != 1 {
			t.Fatalf("AUTO-off periodic Paper signal=%+v", paperIntent.Signals)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("AUTO-off periodic signal stopped collecting in funded Paper")
	}
	if len(s.liveSignalIntentChannel()) != 0 {
		t.Fatal("AUTO-off periodic signal entered the cash preflight queue")
	}
}

func TestR165CashRetiredPaperQueueFullLeavesRetryableReceipt(t *testing.T) {
	const ticker = "R163EVENT-PAPER-FULL"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	r163ArmKalshiFlowYES(t, s, ticker)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	_ = r163InstallPaperCapture(s, 1, true)
	t.Cleanup(func() { r163RemovePaperCapture(s) })

	signalAt := time.Now()
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker,
		Title: "R163 event Paper queue full", Side: "YES", SignalType: "kalshi-flow",
		EntryPrice: .40, ResolveHours: 1, ExecExpr: r147InputReceiptExpr("K", signalAt)}
	if !s.queueLiveSignalIntentAt(sig, .08, signalAt) {
		t.Fatal("raw event LIVE intent was not published")
	}
	intent := <-s.liveSignalIntentChannel()
	started := time.Now()
	if s.processLiveAllowlistedSignalIntentJob(context.Background(), intent) {
		t.Fatal("R165 cash-retired candidate unexpectedly entered the money queue")
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("full Paper queue delayed the cash-retirement terminal by %v", elapsed)
	}
	s.liveMirrorMu.Lock()
	liveCandidates := len(s.liveMirrorQ)
	s.liveMirrorMu.Unlock()
	if liveCandidates != 0 {
		t.Fatalf("cash-retired candidate reached the money queue: count=%d", liveCandidates)
	}

	r163RemovePaperCapture(s)
	releaseShadow()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 ||
		rows[0].Attempt.AttemptID != intent.ShadowAttemptID {
		t.Fatalf("queue-full joined rows=%+v err=%v", rows, err)
	}
	foundDrop := false
	for _, event := range rows[0].Events {
		foundDrop = foundDrop || event.Stage == "funded-paper-scheduler" &&
			event.PaperState == "PAPER-DEFERRED" &&
			event.Reason == "bounded-paper-worker-queue-full"
	}
	if !foundDrop {
		t.Fatalf("full Paper queue did not leave an exact same-ID retry receipt: %+v",
			rows[0].Events)
	}
}
