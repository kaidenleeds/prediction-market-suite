package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r158FlowTrade(t *testing.T, ticker, side string, count, yesPrice float64, at time.Time) kalshi.Trade {
	t.Helper()
	noPrice := 1 - yesPrice
	raw := fmt.Sprintf(`{"ticker":%q,"count_fp":%q,"yes_price_dollars":%q,`+
		`"no_price_dollars":%q,"taker_outcome_side":%q,"created_time":%q}`,
		ticker, fmt.Sprintf("%.8f", count), fmt.Sprintf("%.8f", yesPrice),
		fmt.Sprintf("%.8f", noPrice), side, at.UTC().Format(time.RFC3339Nano))
	var trade kalshi.Trade
	if err := json.Unmarshal([]byte(raw), &trade); err != nil {
		t.Fatal(err)
	}
	return trade
}

func TestR158KalshiFlowEventUsesExactSevenMinuteRuleForOneTicker(t *testing.T) {
	now := time.Now().UTC()
	tape := []kalshi.Trade{
		r158FlowTrade(t, "KX-FLOW", "yes", 400, .40, now.Add(-time.Second)), // $160 YES
		r158FlowTrade(t, "OTHER", "no", 1000, .60, now.Add(-time.Second)),
		r158FlowTrade(t, "KX-FLOW", "no", 150, .40, now.Add(-2*time.Second)), // $90 NO
		r158FlowTrade(t, "KX-FLOW", "no", 1000, .40, now.Add(-8*time.Minute)),
	}
	decision, ok := kalshiFlowEventDecisionFromTape(tape, "KX-FLOW", now, 0)
	if !ok {
		t.Fatalf("exact $250, 64%% one-sided seven-minute flow did not qualify: yes=%v/%v/%s no=%v/%v/%s",
			tape[0].Count.Float(), tape[0].YesPrice.Float(), tape[0].Aggressor(),
			tape[2].Count.Float(), tape[2].NoPrice.Float(), tape[2].Aggressor())
	}
	if decision.Side != "YES" || decision.Notional != 250 || decision.Strength != .64 ||
		decision.YesPrice != .40 || decision.SidePrice != .40 || decision.TradeCount != 2 {
		t.Fatalf("decision=%+v", decision)
	}

	noTape := []kalshi.Trade{
		r158FlowTrade(t, "KX-NO", "no", 500, .40, now.Add(-time.Second)), // $300 NO
	}
	decision, ok = kalshiFlowEventDecisionFromTape(noTape, "KX-NO", now, .35)
	if !ok || decision.Side != "NO" || decision.SidePrice != .65 {
		t.Fatalf("NO-side price was not expressed as 1-YES: %+v ok=%v", decision, ok)
	}
	if _, ok := kalshiFlowEventDecisionFromTape(tape, "KX-FLOW", now, .04); ok {
		t.Fatal("5-95c YES-space band admitted a 4c market")
	}
	if _, ok := kalshiFlowEventDecisionFromTape(tape, "KXMVE-COMBO", now, .40); ok {
		t.Fatal("combo market entered the one-market Flow evaluator")
	}
}

func TestR158KalshiFlowEventCoalescesWakeAndUsesOnlyInFlightPublicationGuard(t *testing.T) {
	now := time.Now()
	runtime := newKalshiFlowEventRuntime()
	defer runtime.cancel()
	first := kalshiFlowEventWake{Ticker: "KX-FLOW", ObservedAt: now, QueuedAt: now}
	second := kalshiFlowEventWake{Ticker: "KX-FLOW", ObservedAt: now.Add(time.Millisecond), QueuedAt: now.Add(time.Millisecond)}
	if !runtime.offer(first) || !runtime.offer(second) || len(runtime.wake) != 1 {
		t.Fatalf("same-ticker trade burst was not nonblockingly coalesced: wake=%d queued=%d",
			len(runtime.wake), len(runtime.queued))
	}
	if wake := runtime.queued["KX-FLOW"]; !wake.ObservedAt.Equal(second.ObservedAt) ||
		!wake.QueuedAt.Equal(first.QueuedAt) {
		t.Fatalf("coalesced clocks=%+v", wake)
	}

	sig := storage.Signal{Ticker: "KX-FLOW", Side: "YES", EntryPrice: .40}
	if !runtime.claimPublication(sig, now) || runtime.claimPublication(sig, now.Add(time.Second)) {
		t.Fatal("same ticker entered the evaluator twice while its first publication was in flight")
	}
	runtime.rollbackPublication(sig, now)
	if !runtime.claimPublication(sig, now.Add(time.Nanosecond)) {
		t.Fatal("completed queue offer suppressed an immediate retry despite having no downstream outcome")
	}
	runtime.rollbackPublication(sig, now.Add(time.Nanosecond))
}

func TestR158KalshiFlowEventQueueOfferNeverWaitsForPublicationGuard(t *testing.T) {
	runtime := newKalshiFlowEventRuntime()
	defer runtime.cancel()
	runtime.publicationMu.Lock()
	offered := make(chan bool, 1)
	go func() {
		offered <- runtime.offer(kalshiFlowEventWake{Ticker: "KX-FLOW", ObservedAt: time.Now()})
	}()
	select {
	case ok := <-offered:
		runtime.publicationMu.Unlock()
		if !ok {
			t.Fatal("nonblocking queue offer was refused while publication state was independently locked")
		}
	case <-time.After(250 * time.Millisecond):
		runtime.publicationMu.Unlock()
		t.Fatal("WS callback queue offer blocked behind evaluator publication state")
	}
}

func TestR158KalshiFlowEventExistingRuntimeFastPathNeverWaitsForCreationLock(t *testing.T) {
	s := &Server{}
	runtime := kalshiFlowEventRuntimeFor(s)
	if runtime == nil {
		t.Fatal("initial runtime was not created")
	}
	kalshiFlowEventRuntimesMu.Lock()
	loaded := make(chan *kalshiFlowEventRuntime, 1)
	go func() { loaded <- kalshiFlowEventRuntimeFor(s) }()
	select {
	case got := <-loaded:
		kalshiFlowEventRuntimesMu.Unlock()
		if got != runtime {
			t.Fatal("fast path returned a different runtime")
		}
	case <-time.After(250 * time.Millisecond):
		kalshiFlowEventRuntimesMu.Unlock()
		stopKalshiFlowEventRuntime(s, context.Background())
		t.Fatal("existing-runtime WS callback blocked behind the cold creation/shutdown mutex")
	}
	stopKalshiFlowEventRuntime(s, context.Background())
}

func TestR158KalshiFlowEventExactAllowlistPrecheckAllocatesNothing(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want bool
	}{
		{"kalshi|kalshi-flow|YES|taker", true},
		{"polyus|spotlag|YES|taker; KALSHI|KALSHI-FLOW|no|TAKER", true},
		{"kalshi|invert:kalshi-flow|NO|taker", false},
		{"kalshi|kalshi-flow-extra|YES|taker", false},
		{"kalshi|kalshi-flow|YES|maker", false},
		{"polyus|kalshi-flow|YES|taker", false},
	} {
		if got := kalshiFlowEventAllowlisted(tc.raw); got != tc.want {
			t.Fatalf("allowlist %q got=%v want=%v", tc.raw, got, tc.want)
		}
	}
	if got := testing.AllocsPerRun(1000, func() {
		_ = kalshiFlowEventAllowlisted("kalshi|spotlag|YES|taker,kalshi|kalshi-flow|NO|taker")
	}); got != 0 {
		t.Fatalf("high-rate exact allowlist precheck allocated %.2f objects per trade", got)
	}
}

func TestR158KalshiFlowEventStopAndRuntimeCreationCannotRaceAReplacement(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := &Server{}
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = kalshiFlowEventRuntimeFor(s)
		}()
		go func() {
			defer wg.Done()
			<-start
			stopKalshiFlowEventRuntime(s, context.Background())
		}()
		close(start)
		wg.Wait()
		if !s.kalshiFlowEventStopping.Load() {
			t.Fatalf("iteration %d: shutdown latch was not set", i)
		}
		if _, loaded := kalshiFlowEventRuntimes.Load(s); loaded {
			t.Fatalf("iteration %d: runtime survived/reappeared after shutdown", i)
		}
		if runtime := kalshiFlowEventRuntimeFor(s); runtime != nil {
			t.Fatalf("iteration %d: callback recreated a runtime after shutdown", i)
		}
	}
}

func TestR158KalshiFlowEventFailsSilentWhenSpotlagOnlyAndPreservesSourceClock(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	s.cfgP.Store(&cfg)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KX-FLOW", Side: "YES",
		SignalType: "kalshi-flow", EntryPrice: .40}
	if s.kalshiFlowEventSelected(sig) {
		t.Fatal("Spotlag-only exact allowlist selected event-driven Kalshi Flow")
	}
	s.queueKalshiFlowTradeWake(kalshi.ResearchReplayEvent{
		Kind: "trade", EntityID: "KX-FLOW", ObservedAt: time.Now(),
		Trade: &kalshi.ResearchReplayTrade{},
	})
	if _, loaded := kalshiFlowEventRuntimes.Load(s); loaded {
		t.Fatal("Spotlag-only AUTO created an unselected Flow worker")
	}
	if len(s.liveSignalIntentChannel()) != 0 {
		t.Fatal("unselected event created a LIVE intent")
	}

	cfg.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	s.cfgP.Store(&cfg)
	if !s.kalshiFlowEventSelected(sig) {
		t.Fatal("exact Flow identity was not selected")
	}
	observed := time.Now().Add(-123 * time.Millisecond)
	sig.ExecExpr = r147InputReceiptExpr("K", observed)
	if !s.queueLiveSignalIntentAt(sig, .05, observed) {
		t.Fatal("selected Flow source-clock intent was not queued")
	}
	var intent liveSignalIntent
	select {
	case intent = <-s.liveSignalIntentChannel():
	case <-time.After(time.Second):
		t.Fatal("selected Flow source-clock intent did not reach the bounded queue")
	}
	if !intent.At.Equal(observed) {
		t.Fatalf("source clock reset at worker: got=%s want=%s", intent.At, observed)
	}
}
