package server

import (
	"context"
	"encoding/json"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR164FundedPaperExactReconcileRecoveryCompletesOnce(t *testing.T) {
	for _, liveState := range []string{"terminal_unfilled", "clean_rejected"} {
		t.Run(liveState, func(t *testing.T) {
			s, st := newExecutionShadowTestServer(t)
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
			t.Cleanup(func() {
				defer stopCancel()
				_ = s.stopGenfollowPaperWorkers(stopCtx)
			})

			triggerAt := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
			attemptID := "r164-exact-recovery-" + liveState
			ticker := "KXR164-" + liveState
			sig := storage.Signal{
				Platform: "kalshi", Ticker: ticker, Title: "exact recovered zero fill",
				Side: "YES", SignalType: "spotlag", EntryPrice: .40,
			}
			payload, err := json.Marshal(sig)
			if err != nil {
				t.Fatal(err)
			}
			attempt := storage.ExecutionShadowAttempt{
				AttemptID: attemptID, SignalDecisionID: "decision-" + liveState,
				ObservedAt: triggerAt, TriggerUnixMS: triggerAt.UnixMilli(),
				Venue: "kalshi", Ticker: ticker, Title: sig.Title, Side: sig.Side,
				Action: "BUY", SystemID: sig.SignalType, Route: "taker",
				SignalSource: "r164-recovery-test", SignalPrice: sig.EntryPrice,
				QualificationBasis: "selected funded-Paper comparison", CreatedAt: triggerAt,
			}
			if inserted, insertErr := st.InsertExecutionShadowAttempt(
				context.Background(), attempt); insertErr != nil || !inserted {
				t.Fatalf("insert attempt inserted=%v err=%v", inserted, insertErr)
			}
			appendEvent := func(event storage.ExecutionShadowEvent) {
				t.Helper()
				event.AttemptID = attemptID
				event.ElapsedFromTriggerMS =
					event.At.UnixMilli() - triggerAt.UnixMilli()
				if inserted, appendErr := st.AppendExecutionShadowEvent(
					context.Background(), event); appendErr != nil || !inserted {
					t.Fatalf("append %s inserted=%v err=%v", event.EventID, inserted, appendErr)
				}
			}
			appendEvent(storage.ExecutionShadowEvent{
				EventID: "offered-" + liveState, At: triggerAt.Add(time.Millisecond),
				Stage: "funded-paper-branch", Outcome: "offered",
				Evidence: map[string]any{"paper_signal_json": string(payload)},
			})
			appendEvent(storage.ExecutionShadowEvent{
				EventID: "ambiguous-" + liveState, At: triggerAt.Add(2 * time.Millisecond),
				Stage: "live-terminal", Outcome: "ambiguous", LiveState: "ambiguous",
				VenueAttempted: true,
			})
			appendEvent(storage.ExecutionShadowEvent{
				EventID: "exact-" + liveState, At: triggerAt.Add(3 * time.Millisecond),
				Stage: "live-exact-reconcile", Outcome: liveState, LiveState: liveState,
				VenueAttempted: true, LiveAuthoritative: true,
			})

			s.recoverFundedPaperGaps(context.Background())
			deadline := time.Now().Add(2 * time.Second)
			for {
				rows, listErr := st.ListExecutionShadowAttempts(context.Background(), 10)
				if listErr != nil {
					t.Fatal(listErr)
				}
				terminalCount := 0
				terminalState := ""
				for _, row := range rows {
					if row.Attempt.AttemptID != attemptID {
						continue
					}
					for _, event := range row.Events {
						if event.Stage == "funded-paper-terminal" {
							terminalCount++
							terminalState = event.PaperState
						}
					}
				}
				if terminalCount == 1 && terminalState == "PAPER-ZERO-FILL" {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("recovery terminal count=%d state=%q", terminalCount, terminalState)
				}
				s.wakeExecutionShadowWriter()
				time.Sleep(5 * time.Millisecond)
			}
			if gaps, gapErr := st.ListExecutionShadowFundedPaperGaps(
				context.Background(), 10); gapErr != nil || len(gaps) != 0 {
				t.Fatalf("completed recovery gaps=%d err=%v", len(gaps), gapErr)
			}

			// A second sweep must see the stable terminal and do nothing.
			s.recoverFundedPaperGaps(context.Background())
			time.Sleep(25 * time.Millisecond)
			if err = s.stopGenfollowPaperWorkers(stopCtx); err != nil {
				t.Fatal(err)
			}
			if err = s.stopExecutionShadowWriter(stopCtx); err != nil {
				t.Fatal(err)
			}
			rows, err := st.ListExecutionShadowAttempts(context.Background(), 10)
			if err != nil {
				t.Fatal(err)
			}
			terminalCount := 0
			for _, row := range rows {
				if row.Attempt.AttemptID != attemptID {
					continue
				}
				for _, event := range row.Events {
					if event.Stage == "funded-paper-terminal" {
						terminalCount++
					}
				}
			}
			if terminalCount != 1 {
				t.Fatalf("recovery wrote %d durable terminals; want exactly one", terminalCount)
			}
		})
	}
}

func r164InsertFundedPaperRecoveryGap(t *testing.T, st *storage.Store, attemptID string,
	triggerAt time.Time) {
	t.Helper()
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "KXR164-" + attemptID, Title: "recovery gap",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40,
	}
	payload, err := json.Marshal(sig)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.ExecutionShadowAttempt{
		AttemptID: attemptID, SignalDecisionID: "decision-" + attemptID,
		ObservedAt: triggerAt, TriggerUnixMS: triggerAt.UnixMilli(),
		Venue: "kalshi", Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
		Action: "BUY", SystemID: sig.SignalType, Route: "taker",
		SignalSource: "r164-recovery-test", SignalPrice: sig.EntryPrice,
		QualificationBasis: "selected funded-Paper comparison", CreatedAt: triggerAt,
	}
	if inserted, insertErr := st.InsertExecutionShadowAttempt(
		context.Background(), attempt); insertErr != nil || !inserted {
		t.Fatalf("insert attempt inserted=%v err=%v", inserted, insertErr)
	}
	for _, event := range []storage.ExecutionShadowEvent{
		{
			EventID: "offered-" + attemptID, AttemptID: attemptID,
			At: triggerAt.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
			Stage: "funded-paper-branch", Outcome: "offered",
			Evidence: map[string]any{"paper_signal_json": string(payload)},
		},
		{
			EventID: "live-" + attemptID, AttemptID: attemptID,
			At: triggerAt.Add(2 * time.Millisecond), ElapsedFromTriggerMS: 2,
			Stage: "live-terminal", Outcome: "clean_rejected",
			LiveState: "clean_rejected", LiveAuthoritative: true,
		},
	} {
		if inserted, appendErr := st.AppendExecutionShadowEvent(
			context.Background(), event); appendErr != nil || !inserted {
			t.Fatalf("append %s inserted=%v err=%v", event.EventID, inserted, appendErr)
		}
	}
}

func TestR164FundedPaperRecoverySkipsHistoricalBacklogWhileLiveAutoOn(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	attemptID := "auto-on-backlog"
	r164InsertFundedPaperRecoveryGap(t, st, attemptID,
		time.Now().UTC().Add(-2*genfollowPaperSignalMaxAge))

	s.liveMu.Lock()
	s.liveAuto = true
	s.liveMu.Unlock()
	s.recoverFundedPaperGaps(context.Background())

	gaps, err := st.ListExecutionShadowFundedPaperGaps(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 1 || gaps[0].Attempt.AttemptID != attemptID {
		t.Fatalf("AUTO-on recovery changed durable backlog: %+v", gaps)
	}
	s.genfollowPaperWorkerMu.Lock()
	workerStarted := s.genfollowPaperIntentCh != nil || s.genfollowPaperPriorityCh != nil
	s.genfollowPaperWorkerMu.Unlock()
	if workerStarted {
		t.Fatal("AUTO-on historical recovery started the funded-Paper worker")
	}
}

func TestR164FundedPaperRecoveryClosesExpiredGapWithoutWorker(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	attemptID := "expired-direct-terminal"
	r164InsertFundedPaperRecoveryGap(t, st, attemptID,
		time.Now().UTC().Add(-genfollowPaperSignalMaxAge-time.Second))

	s.recoverFundedPaperGaps(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}

	rows, err := st.ListExecutionShadowAttempts(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, row := range rows {
		if row.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range row.Events {
			if event.Stage != "funded-paper-terminal" {
				continue
			}
			found = true
			if event.PaperState != "PAPER-NOT-OBSERVED" ||
				event.Reason != "paper-original-signal-expired-before-recovery" {
				t.Fatalf("expired recovery terminal=%+v", event)
			}
		}
	}
	if !found {
		t.Fatal("expired recovery gap did not receive a direct terminal")
	}
	s.genfollowPaperWorkerMu.Lock()
	workerStarted := s.genfollowPaperIntentCh != nil || s.genfollowPaperPriorityCh != nil
	s.genfollowPaperWorkerMu.Unlock()
	if workerStarted {
		t.Fatal("expired recovery gap entered the funded-Paper worker")
	}
}

func TestR164ExecutionOnlyWaitsForCashThenRunsBeforePortfolio(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = -1
	s.liveDispatchShadowLiveActive.Store(1)
	executionStarted := make(chan struct{}, 1)
	releaseExecution := make(chan struct{})
	portfolioStarted := make(chan struct{}, 1)
	due := time.Now()
	if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		DueAt: due, Preflighted: true,
		Execution: livePolicyMirrorExecutionMeta{LiveReceiptSeen: true,
			LiveTerminalAt: due, ScheduledAt: due},
		testExecutionRun: func(context.Context) {
			executionStarted <- struct{}{}
			<-releaseExecution
		},
		testRun: func(context.Context) {
			portfolioStarted <- struct{}{}
		},
	}) {
		t.Fatal("LIVE-passed comparison work was not admitted")
	}
	select {
	case <-executionStarted:
		t.Fatal("execution-only comparison overlapped the active cash lane")
	case <-time.After(75 * time.Millisecond):
	}
	select {
	case <-portfolioStarted:
		t.Fatal("fake portfolio began while the real cash lane was active")
	default:
	}
	s.liveDispatchShadowLiveActive.Store(0)
	select {
	case <-executionStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("execution-only comparison did not resume after cash terminal")
	}
	select {
	case <-portfolioStarted:
		t.Fatal("fake portfolio began before execution-only comparison completed")
	default:
	}
	close(releaseExecution)
	select {
	case <-portfolioStarted:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("fake portfolio did not resume after execution-only comparison")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestR164ExecutionOnlyDoesNotWaitForUnrelatedQueuedCandidate(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = -1
	s.liveMirrorMu.Lock()
	s.liveMirrorQ = append(s.liveMirrorQ, liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR164-UNRELATED", Side: "YES",
	})
	s.liveMirrorMu.Unlock()

	executionStarted := make(chan struct{}, 1)
	if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		DueAt: time.Now(), Preflighted: true, ExecutionOnly: true,
		Execution:        livePolicyMirrorExecutionMeta{LiveReceiptSeen: true},
		testExecutionRun: func(context.Context) { executionStarted <- struct{}{} },
	}) {
		t.Fatal("execution-only comparison work was not admitted")
	}
	select {
	case <-executionStarted:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("unrelated queued candidate erased the execution-only comparison window")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestR164ExecutionOnlyReceiptIsDurableAndExplicitlyNotPortfolioAuthority(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR164-EXEC",
		Title: "R164 execution-only", Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(sig, .04, now)
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: sig.Ticker,
		Side: "YES", Family: "spotlag", At: now, ShadowAttemptID: attemptID,
		ProspectiveQty: 1, ProspectiveTimeInForce: liveProspectiveIOC}
	initial := liveMirrorQuote{Price: .40, Depth: 8, Tick: .01,
		BookSource: "kalshi_ws_full_orderbook:g4:s12:q90", ObservedAt: now}
	finalQ := liveMirrorQuote{Price: .39, Depth: 2, Tick: .01,
		BookSource: "kalshi_ws_full_orderbook:g4:s12:q91", ObservedAt: now.Add(time.Millisecond)}
	meta := livePolicyMirrorExecutionMeta{ScheduledAt: now, LiveTerminalAt: now,
		LiveState: "filled", LiveLimit: .40, VenueAttempted: true, LiveReceiptSeen: true}
	s.recordLivePolicyMirrorExecution(candidate, initial, finalQ,
		meta, now, .40, 1, 1, .39, .01, true, "exact-test-fee",
		livePolicyMirrorModeledFill, "")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range row.Events {
			if event.Stage != "counterfactual-execution-terminal" {
				continue
			}
			if event.ShadowFilledQty == nil || *event.ShadowFilledQty != 1 ||
				event.ShadowFillPrice == nil || math.Abs(*event.ShadowFillPrice-.39) > 1e-12 ||
				event.ShadowFee == nil || math.Abs(*event.ShadowFee-.01) > 1e-12 {
				t.Fatalf("execution-only economics incomplete: %+v", event)
			}
			if event.Evidence["fake_portfolio_admission"] != false ||
				event.Evidence["cash_path_blocking"] != false ||
				event.Evidence["live_terminal_observed"] != true {
				t.Fatalf("execution-only scope became portfolio/cash authority: %+v", event.Evidence)
			}
			return
		}
	}
	t.Fatal("execution-only terminal was not durably joined to the shared attempt")
}

func TestR164ExecutionComparisonRequiresProvenLiveTerminal(t *testing.T) {
	ptr := func(v float64) *float64 { return &v }
	cases := []struct {
		name string
		row  storage.ExecutionShadowEvent
		want bool
	}{
		{name: "local no-send", want: true,
			row: storage.ExecutionShadowEvent{LiveState: "not-sent"}},
		{name: "pending", row: storage.ExecutionShadowEvent{
			LiveState: "pending", VenueAttempted: true}},
		{name: "ambiguous even with quantity", row: storage.ExecutionShadowEvent{
			LiveState: "ambiguous", VenueAttempted: true, LiveFilledQty: ptr(1)}},
		{name: "fill before exact fee", want: true, row: storage.ExecutionShadowEvent{
			LiveState: "full", VenueAttempted: true, LiveFilledQty: ptr(1)}},
		{name: "partial immediate", want: true, row: storage.ExecutionShadowEvent{
			LiveState: "partial", VenueAttempted: true, LiveFilledQty: ptr(1),
			Evidence: map[string]any{"time_in_force": liveProspectiveIOC}}},
		{name: "partial non-immediate", row: storage.ExecutionShadowEvent{
			LiveState: "partial", VenueAttempted: true, LiveFilledQty: ptr(1),
			Evidence: map[string]any{"time_in_force": "good_till_canceled"}}},
		{name: "unfilled not authoritative", row: storage.ExecutionShadowEvent{
			LiveState: "unfilled", VenueAttempted: true}},
		{name: "unfilled authoritative", want: true, row: storage.ExecutionShadowEvent{
			LiveState: "unfilled", VenueAttempted: true, LiveAuthoritative: true}},
		{name: "durable terminal unfilled", want: true, row: storage.ExecutionShadowEvent{
			LiveState: "terminal_unfilled", VenueAttempted: true, LiveAuthoritative: true}},
		{name: "clean venue rejection", want: true, row: storage.ExecutionShadowEvent{
			LiveState: "rejected", VenueAttempted: true}},
		{name: "durable clean venue rejection", want: true, row: storage.ExecutionShadowEvent{
			LiveState: "clean_rejected", VenueAttempted: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := executionShadowLiveTerminalComparable(tc.row); got != tc.want {
				t.Fatalf("comparable=%v want=%v row=%+v", got, tc.want, tc.row)
			}
		})
	}
}

func TestR164ExecutionGapSkipsAmbiguousUntilLaterExactReconcile(t *testing.T) {
	now := time.Now().UTC()
	qty, limit := 1.0, .40
	row := storage.ExecutionShadowAttemptView{
		Attempt: storage.ExecutionShadowAttempt{
			AttemptID: "r164-live-terminal-gate", Venue: "kalshi",
			Ticker: "KXR164-GATE", Side: "YES", Action: "BUY", Route: "taker",
			SystemID: "spotlag", ObservedAt: now, TriggerUnixMS: now.UnixMilli(),
		},
		Events: []storage.ExecutionShadowEvent{
			{Stage: "live-first-preflight", At: now, Outcome: "passed",
				RequestedQty: &qty, Evidence: map[string]any{
					"time_in_force": liveProspectiveIOC}},
			{Stage: "live-terminal", At: now.Add(time.Millisecond),
				LiveState: "ambiguous", VenueAttempted: true, RequestedQty: &qty,
				OriginalLimit: &limit, Evidence: map[string]any{
					"time_in_force": liveProspectiveIOC}},
			{Stage: "live-terminal", At: now.Add(2 * time.Millisecond),
				LiveState: "pending", VenueAttempted: true, RequestedQty: &qty,
				OriginalLimit: &limit, Evidence: map[string]any{
					"time_in_force": liveProspectiveIOC}},
		},
	}
	if _, _, _, ok := livePolicyMirrorExecutionGap(row); ok {
		t.Fatal("pending/ambiguous LIVE evidence scheduled a permanent comparison result")
	}
	row.Events = append(row.Events, storage.ExecutionShadowEvent{
		Stage: "live-terminal", At: now.Add(3 * time.Millisecond),
		LiveState: "full", VenueAttempted: true, RequestedQty: &qty,
		LiveFilledQty: &qty, OriginalLimit: &limit,
		Evidence: map[string]any{"time_in_force": liveProspectiveIOC},
	})
	candidate, quote, meta, ok := livePolicyMirrorExecutionGap(row)
	if !ok || meta.LiveState != "full" || !meta.VenueAttempted ||
		candidate.ProspectiveQty != 1 ||
		candidate.ProspectiveTimeInForce != liveProspectiveIOC ||
		math.Abs(quote.Price-limit) > 1e-12 ||
		math.Abs(candidate.ArbitrationQuote.Tick-quote.Tick) > 1e-12 {
		t.Fatalf("later exact terminal was not reconstructed: ok=%v c=%+v q=%+v meta=%+v",
			ok, candidate, quote, meta)
	}
}

func TestR164ImmediateComparisonWaitsForExactLiveReconcile(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = -1
	// Keep the scheduled comparison in the past so shutdown does not wait on a future test timer.
	now := time.Now().UTC().Add(-3 * time.Second)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR164-IMMEDIATE-GATE",
		Title: "exact reconcile gate", Side: "YES", SignalType: "spotlag",
		EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(sig, .04, now)
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
		Action: "BUY", Family: sig.SignalType, At: now, ShadowAttemptID: attemptID,
		ProspectiveQty: 1, ProspectiveTimeInForce: liveProspectiveIOC,
	}
	quote := liveMirrorQuote{Price: .40, Depth: 2, Tick: .01,
		BookSource: "kalshi_ws_full_orderbook:g1:s1:q1", ObservedAt: now}
	s.executionShadowRecordLiveResult(candidate, quote, "risk-gate", "", "ambiguous",
		"transport-timeout", 1, 0, .40, 0, false, false, true,
		"transport result unknown", map[string]any{"time_in_force": liveProspectiveIOC})
	if !s.markLivePolicyMirrorExecutionScheduled(attemptID) {
		t.Fatal("ambiguous receipt claimed the one comparison terminal")
	}
	s.forgetLivePolicyMirrorExecutionScheduled(attemptID)

	s.executionShadowRecordLiveResult(candidate, quote, "risk-gate", "order-gate", "full",
		"exact-order-reconcile", 1, 1, .40, 0, false, false, true, "",
		map[string]any{"time_in_force": liveProspectiveIOC})
	if s.markLivePolicyMirrorExecutionScheduled(attemptID) {
		t.Fatal("later exact LIVE fill did not schedule the comparison terminal")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestR164LocalNoSendPersistsFrozenQuantityAndTIF(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = -1
	// Keep the scheduled comparison in the past so shutdown does not wait on a future test timer.
	now := time.Now().UTC().Add(-3 * time.Second)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR164-NOSEND-PLAN",
		Title: "durable no-send plan", Side: "NO", SignalType: "spotlag",
		EntryPrice: .41}
	attemptID := s.executionShadowBeginSignal(sig, .04, now)
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
		Action: "BUY", Family: sig.SignalType, At: now, ShadowAttemptID: attemptID,
		ProspectiveQty: 1, ProspectiveTimeInForce: liveProspectiveIOC,
		ProspectivePlanReason: "one-contract canary",
		ArbitrationAt:         now, ArbitrationQuote: liveMirrorQuote{
			Price: .41, Depth: 3, Tick: .01,
			BookSource: "kalshi_ws_full_orderbook:g2:s3:q4", ObservedAt: now,
		},
	}
	s.executionShadowRecordDrop(candidate, "live-local-gate", "known no-send",
		now.Add(time.Millisecond), map[string]any{"existing": "kept"})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, stored := range rows {
		if stored.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range stored.Events {
			if event.Stage != "live-local-gate" {
				continue
			}
			if event.RequestedQty == nil || *event.RequestedQty != 1 ||
				event.Evidence["time_in_force"] != liveProspectiveIOC ||
				event.Evidence["time_in_force_reason"] != "one-contract canary" ||
				event.Evidence["existing"] != "kept" {
				t.Fatalf("local no-send lost frozen plan: %+v", event)
			}
			recovered, _, _, ok := livePolicyMirrorExecutionGap(
				storage.ExecutionShadowAttemptView{
					Attempt: stored.Attempt, Events: stored.Events,
				})
			if !ok || recovered.ProspectiveQty != 1 ||
				recovered.ProspectiveTimeInForce != liveProspectiveIOC {
				t.Fatalf("durable no-send plan did not recover: ok=%v candidate=%+v",
					ok, recovered)
			}
			return
		}
	}
	t.Fatal("durable local no-send event was not stored")
}

func TestR164ExecutionOnlyFillWinsShadowEconomicsOverLaterPortfolioRow(t *testing.T) {
	events := []storage.ExecutionShadowEvent{
		{Stage: "counterfactual-execution-terminal",
			ShadowFilledQty: floatPtrShadow(1), ShadowFillPrice: floatPtrShadow(.40),
			ShadowFee: floatPtrShadow(.01)},
		{Stage: "counterfactual-terminal",
			ShadowFilledQty: floatPtrShadow(2), ShadowFillPrice: floatPtrShadow(.30),
			ShadowFee: floatPtrShadow(.02)},
	}
	_, _, _, _, _, _, _, _, quantity, price, fee, known :=
		executionShadowLatestFills(events)
	if !known || quantity != 1 || math.Abs(price-.40) > 1e-12 ||
		math.Abs(fee-.01) > 1e-12 {
		t.Fatalf("latest shadow economics=%v @ %v fee=%v known=%v; want execution-only lane",
			quantity, price, fee, known)
	}

	// Once an execution-only terminal exists, a later fake-portfolio fill cannot make an
	// execution zero-fill look filled in the joined summary.
	rows := []storage.ExecutionShadowAttemptView{{Events: []storage.ExecutionShadowEvent{
		{Stage: "counterfactual-execution-terminal", ShadowState: "zero_fill"},
		{Stage: "counterfactual-terminal",
			ShadowFilledQty: floatPtrShadow(1), ShadowFillPrice: floatPtrShadow(.40),
			ShadowFee: floatPtrShadow(.01)},
	}}}
	summary := executionShadowSummary(rows)
	if got := summary["counterfactual_filled"]; got != 0 {
		t.Fatalf("portfolio fill overwrote execution-only zero-fill in summary: %v", got)
	}
}

func TestR164ExecutionOnlyConcurrencyIsBounded(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = -1
	var current, peak atomic.Int64
	release := make(chan struct{})
	done := make(chan struct{}, 12)
	for i := 0; i < 12; i++ {
		if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
			DueAt: time.Now(), Preflighted: true,
			Execution: livePolicyMirrorExecutionMeta{LiveReceiptSeen: true},
			testExecutionRun: func(context.Context) {
				n := current.Add(1)
				for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				<-release
				current.Add(-1)
				done <- struct{}{}
			},
			testRun: func(context.Context) {},
		}) {
			t.Fatal("priority work was not admitted")
		}
	}
	deadline := time.Now().Add(time.Second)
	for peak.Load() < livePolicyMirrorExecutionWorkerCount && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := peak.Load(); got != livePolicyMirrorExecutionWorkerCount {
		t.Fatalf("peak execution concurrency=%d want=%d", got,
			livePolicyMirrorExecutionWorkerCount)
	}
	time.Sleep(25 * time.Millisecond)
	if got := peak.Load(); got > livePolicyMirrorExecutionWorkerCount {
		t.Fatalf("execution concurrency exceeded bound: %d", got)
	}
	close(release)
	for i := 0; i < 12; i++ {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("bounded execution work did not drain")
		}
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestR164BusyOptionalPortfolioCannotBackUpExecutionLane(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = -1
	firstPortfolioStarted := make(chan struct{}, 1)
	releaseFirstPortfolio := make(chan struct{})
	if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		DueAt: time.Now(), Preflighted: true,
		Execution:        livePolicyMirrorExecutionMeta{LiveReceiptSeen: true},
		testExecutionRun: func(context.Context) {},
		testRun: func(context.Context) {
			firstPortfolioStarted <- struct{}{}
			<-releaseFirstPortfolio
		},
	}) {
		t.Fatal("first comparison work was not admitted")
	}
	select {
	case <-firstPortfolioStarted:
	case <-time.After(time.Second):
		t.Fatal("first optional portfolio job did not occupy its serial slot")
	}

	now := time.Now().UTC()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR164-PORTFOLIO-BUSY",
		Title: "optional portfolio busy", Side: "YES", SignalType: "spotlag",
		EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(sig, .04, now)
	secondExecutionRan := make(chan struct{}, 1)
	if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		DueAt: time.Now(), Preflighted: true,
		Candidate: liveMirrorCandidate{Platform: "kalshi", Ticker: sig.Ticker,
			Title: sig.Title, Side: sig.Side, Family: sig.SignalType, At: now,
			ShadowAttemptID: attemptID},
		Execution:        livePolicyMirrorExecutionMeta{LiveReceiptSeen: true},
		testExecutionRun: func(context.Context) { secondExecutionRan <- struct{}{} },
	}) {
		t.Fatal("second comparison work was not admitted")
	}
	select {
	case <-secondExecutionRan:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("busy optional portfolio delayed the next execution comparison")
	}
	close(releaseFirstPortfolio)

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := s.stopExecutionShadowWriter(stopCtx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(stopCtx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range row.Events {
			if event.Stage == "counterfactual-terminal" &&
				event.ShadowState == "not_observed" &&
				event.Reason == "optional-portfolio-worker-busy" {
				return
			}
		}
	}
	t.Fatal("busy optional portfolio was not recorded as isolated not-observed evidence")
}

func TestR164ExecutionClaimsAndSchedulerNoticesStayBounded(t *testing.T) {
	s := testServer(t)
	if !s.markLivePolicyMirrorExecutionScheduled("attempt-one") {
		t.Fatal("initial execution claim failed")
	}
	if len(s.livePolicyMirrorSeen) != 0 {
		t.Fatalf("execution claim leaked into expiring signal dedup map: %+v",
			s.livePolicyMirrorSeen)
	}
	s.forgetLivePolicyMirrorExecutionScheduled("attempt-one")
	if !s.markLivePolicyMirrorExecutionScheduled("attempt-one") {
		t.Fatal("released execution claim could not be reacquired")
	}

	now := time.Now().UTC()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR164-SCHEDULER",
		Title: "scheduler coalescing", Side: "NO", SignalType: "kalshi-flow",
		EntryPrice: .60}
	attemptID := s.executionShadowBeginSignal(sig, .03, now)
	s.executionShadowRecordPaperScheduler(attemptID, "queue-full", now.Add(time.Millisecond))
	s.executionShadowRecordPaperScheduler(attemptID, "queue-full", now.Add(2*time.Millisecond))
	s.executionShadowRecordPaperScheduler(attemptID, "worker-stopping", now.Add(3*time.Millisecond))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	notices := 0
	for _, row := range rows {
		if row.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range row.Events {
			if event.Stage == "funded-paper-scheduler" {
				notices++
			}
		}
	}
	if notices != 2 {
		t.Fatalf("scheduler notices=%d want=2 unique attempt/reason receipts", notices)
	}
}

func TestR164FundedPaperCannotFillExpiredOriginalSignal(t *testing.T) {
	s := testServer(t)
	ran := make(chan struct{}, 1)
	old := time.Now().Add(-genfollowPaperSignalMaxAge - time.Second)
	in := make(chan genfollowPaperIntent, 1)
	done := make(chan struct{})
	in <- genfollowPaperIntent{EnqueuedAt: old, Signals: []genfollowPaperSignal{{
		SignalAt: old,
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR164-EXPIRED",
			Side: "YES", SignalType: "spotlag", EntryPrice: .40},
		testRun: func(context.Context) { ran <- struct{}{} },
	}}}
	close(in)
	go s.runGenfollowPaperWorkers(context.Background(), in, done)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expired Paper signal did not terminalize promptly")
	}
	select {
	case <-ran:
		t.Fatal("expired Paper signal reached fake execution")
	default:
	}
}

func TestR164PaperFanoutPreencodesNonFiniteSignalWithoutPoisoningWriter(t *testing.T) {
	s := testServer(t)
	release := holdExecutionShadowWriterStart(t, s)
	now := time.Now().UTC()
	valid := storage.Signal{Platform: "kalshi", Ticker: "KXR164-NONFINITE",
		Title: "non-finite replay payload", Side: "YES", SignalType: "spotlag",
		EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(valid, .04, now)
	invalid := valid
	invalid.EntryPrice = math.NaN()
	s.executionShadowRecordPaperFanout(attemptID, now.Add(time.Millisecond), now, invalid)
	release()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range row.Events {
			if event.Stage == "funded-paper-branch" {
				if event.Evidence["paper_signal_available"] != false ||
					event.Evidence["paper_signal_error"] != "signal-payload-not-finite-json" {
					t.Fatalf("unsafe replay payload evidence=%+v", event.Evidence)
				}
				if _, leaked := event.Evidence["paper_signal"]; leaked {
					t.Fatal("non-finite signal struct reached retained writer evidence")
				}
				return
			}
		}
	}
	t.Fatal("safe fanout receipt did not persist")
}

func TestR164StableTerminalClaimsRemainHeldWhileWriterIsDelayed(t *testing.T) {
	s := testServer(t)
	release := holdExecutionShadowWriterStart(t, s)
	now := time.Now().UTC()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR164-STABLE",
		Title: "stable terminal claim", Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(sig, .04, now)

	if !s.claimGenfollowPaperScheduled(attemptID) {
		t.Fatal("initial Paper recovery claim failed")
	}
	s.executionShadowRecordPaperAttempt(attemptID, "PAPER-NOT-OBSERVED",
		"test-terminal", "", now.Add(time.Millisecond), 0, 0, 0, 0, 0, "", sig)
	if s.claimGenfollowPaperScheduled(attemptID) {
		t.Fatal("Paper stable terminal claim was released before durable visibility")
	}

	if !s.markLivePolicyMirrorExecutionScheduled(attemptID) {
		t.Fatal("initial execution comparison claim failed")
	}
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: sig.Ticker,
		Side: sig.Side, Family: sig.SignalType, At: now, ShadowAttemptID: attemptID,
		ProspectiveQty: 1, ProspectiveTimeInForce: liveProspectiveIOC}
	s.recordLivePolicyMirrorExecutionMiss(candidate, livePolicyMirrorExecutionMeta{
		LiveReceiptSeen: true, LiveTerminalAt: now, ScheduledAt: now,
	}, "test-terminal")
	if s.markLivePolicyMirrorExecutionScheduled(attemptID) {
		t.Fatal("execution stable terminal claim was released before durable visibility")
	}

	release()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	paperTerminals, executionTerminals := 0, 0
	for _, row := range rows {
		if row.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range row.Events {
			if event.Stage == "funded-paper-terminal" {
				paperTerminals++
			}
			if event.Stage == "counterfactual-execution-terminal" {
				executionTerminals++
			}
		}
	}
	if paperTerminals != 1 || executionTerminals != 1 {
		t.Fatalf("stable terminals paper=%d execution=%d; want one each",
			paperTerminals, executionTerminals)
	}
}

func TestR164ComparisonSettlementWaitsForExecutionTerminal(t *testing.T) {
	row := storage.ExecutionShadowAttemptView{
		Attempt: storage.ExecutionShadowAttempt{
			Venue: "kalshi", Action: "BUY", Route: "taker",
		},
		Events: []storage.ExecutionShadowEvent{
			{Stage: "live-first-preflight", Outcome: "passed"},
			{Stage: "live-terminal", LiveState: "filled"},
		},
	}
	if !executionShadowComparisonPending(row) {
		t.Fatal("selected LIVE terminal was allowed to settle before execution comparison")
	}
	olderShadowNet := .25
	events := append(row.Events,
		storage.ExecutionShadowEvent{Stage: "settlement", SettlementKnown: true,
			ShadowNet: &olderShadowNet},
		storage.ExecutionShadowEvent{Stage: "counterfactual-execution-terminal"})
	_, _, _, shadowCovered := executionShadowSettlementCoverage(events)
	if shadowCovered {
		t.Fatal("older fake-portfolio settlement covered the later execution-only lane")
	}
	newShadowNet := .20
	events = append(events, storage.ExecutionShadowEvent{
		Stage: "settlement", SettlementKnown: true, ShadowNet: &newShadowNet})
	_, _, _, shadowCovered = executionShadowSettlementCoverage(events)
	if !shadowCovered {
		t.Fatal("post-execution terminal settlement did not cover comparison economics")
	}
}

func TestR164RecoveredPaperLotRequiresExactTruthAndEconomics(t *testing.T) {
	attempt := storage.ExecutionShadowAttempt{
		Venue: "kalshi", Ticker: "KXR164-LOT", Side: "NO",
	}
	valid := kfPos{
		Platform: "kalshi", Ticker: attempt.Ticker, Side: attempt.Side,
		Price: .40, Contracts: 1, Fee: .01, FeeKnown: true, FeeSource: "exact-fee",
		ExecutionTruthContract: fundedPaperLiveTruthContractV1,
		ExecutionLiveTerminal:  "filled", ExecutionLiveTerminalID: "terminal-1",
	}
	if reason := fundedPaperRecoveryLotReason(valid, attempt); reason != "" {
		t.Fatalf("valid durable lot refused: %s", reason)
	}
	bad := valid
	bad.FeeKnown = false
	if reason := fundedPaperRecoveryLotReason(bad, attempt); reason !=
		"existing-funded-paper-lot-economics-incomplete" {
		t.Fatalf("unknown fee reason=%q", reason)
	}
	bad = valid
	bad.ExecutionTruthContract = ""
	if reason := fundedPaperRecoveryLotReason(bad, attempt); reason !=
		"existing-funded-paper-lot-missing-live-truth-contract" {
		t.Fatalf("missing truth reason=%q", reason)
	}
}

func TestR164RecoveredPaperSignalMustMatchImmutableAttemptIdentity(t *testing.T) {
	attempt := storage.ExecutionShadowAttempt{
		Venue: "kalshi", Ticker: "KXR164-PAYLOAD", Side: "YES", SystemID: "spotlag",
	}
	valid := storage.Signal{
		Platform: "KALSHI", Ticker: attempt.Ticker, Side: "yes", SignalType: "SPOTLAG",
	}
	if reason := fundedPaperRecoverySignalReason(valid, attempt); reason != "" {
		t.Fatalf("valid recovered signal refused: %s", reason)
	}
	tests := []struct {
		name, want string
		edit       func(*storage.Signal)
	}{
		{"venue", "durable-paper-signal-identity-mismatch:venue",
			func(sig *storage.Signal) { sig.Platform = "polyus" }},
		{"ticker", "durable-paper-signal-identity-mismatch:ticker",
			func(sig *storage.Signal) { sig.Ticker = "KXR164-OTHER" }},
		{"side", "durable-paper-signal-identity-mismatch:side",
			func(sig *storage.Signal) { sig.Side = "NO" }},
		{"system", "durable-paper-signal-identity-mismatch:system",
			func(sig *storage.Signal) { sig.SignalType = "kalshi-flow" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := valid
			tc.edit(&changed)
			if reason := fundedPaperRecoverySignalReason(changed, attempt); reason != tc.want {
				t.Fatalf("reason=%q want %q", reason, tc.want)
			}
		})
	}
}

func TestR164PaperPriorityQueueDrainsSelectedBeforeQueuedBackground(t *testing.T) {
	s := testServer(t)
	priority := make(chan genfollowPaperIntent, 1)
	background := make(chan genfollowPaperIntent, 1)
	done := make(chan struct{})
	order := make(chan string, 2)
	signalAt := time.Now().Add(-2 * time.Second)
	background <- genfollowPaperIntent{EnqueuedAt: signalAt, Signals: []genfollowPaperSignal{{
		SignalAt: signalAt, testRun: func(context.Context) { order <- "background" },
	}}}
	priority <- genfollowPaperIntent{EnqueuedAt: signalAt, Signals: []genfollowPaperSignal{{
		SignalAt: signalAt, testRun: func(context.Context) { order <- "priority" },
	}}}
	close(priority)
	close(background)
	go s.runGenfollowPaperPriorityWorkers(context.Background(), priority, background, done)
	select {
	case first := <-order:
		if first != "priority" {
			t.Fatalf("first Paper lane=%q want priority", first)
		}
	case <-time.After(time.Second):
		t.Fatal("priority Paper work did not run")
	}
	select {
	case second := <-order:
		if second != "background" {
			t.Fatalf("second Paper lane=%q want background", second)
		}
	case <-time.After(time.Second):
		t.Fatal("background Paper work did not run")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Paper priority worker did not stop")
	}
}

func TestR164FundedPaperWaitsForTerminalAndPrioritizesVenueAttempt(t *testing.T) {
	s := testServer(t)
	priority := make(chan genfollowPaperIntent, 2)
	background := make(chan genfollowPaperIntent, 2)
	s.genfollowPaperWorkerMu.Lock()
	s.genfollowPaperPriorityCh = priority
	s.genfollowPaperIntentCh = background
	s.genfollowPaperWorkerMu.Unlock()

	now := time.Now()
	venueID, localID := "r164-paper-venue-ready", "r164-paper-local-ready"
	s.rememberGenfollowPaperAfterLiveTerminal(genfollowPaperSignal{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR164-VENUE",
			Side: "YES", SignalType: "spotlag"},
		SignalAt: now, ShadowAttemptID: venueID,
	})
	s.rememberGenfollowPaperAfterLiveTerminal(genfollowPaperSignal{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR164-LOCAL",
			Side: "NO", SignalType: "spotlag"},
		SignalAt: now, ShadowAttemptID: localID,
	})
	if len(priority) != 0 || len(background) != 0 {
		t.Fatal("preflight-passed Paper work entered a worker before LIVE terminal")
	}
	s.enqueueGenfollowPaperAfterLiveTerminal(storage.ExecutionShadowEvent{
		EventID: "local-terminal", AttemptID: localID, LiveState: "not-sent",
	})
	s.enqueueGenfollowPaperAfterLiveTerminal(storage.ExecutionShadowEvent{
		EventID: "venue-terminal", AttemptID: venueID, LiveState: "full",
		VenueAttempted: true, LiveAuthoritative: true, LiveFilledQty: floatPtrShadow(1),
	})
	if len(priority) != 1 || len(background) != 1 {
		t.Fatalf("terminal lanes priority=%d background=%d want 1/1",
			len(priority), len(background))
	}
	venue := <-priority
	local := <-background
	if venue.Signals[0].ShadowAttemptID != venueID ||
		!venue.Signals[0].VenuePriority ||
		venue.Signals[0].LiveTerminal.Kind != fundedPaperLiveTerminalFill {
		t.Fatalf("venue terminal was not priority-ready: %+v", venue.Signals)
	}
	if local.Signals[0].ShadowAttemptID != localID ||
		local.Signals[0].VenuePriority ||
		local.Signals[0].LiveTerminal.Kind != fundedPaperLiveTerminalNoSend {
		t.Fatalf("local no-send was not background-ready: %+v", local.Signals)
	}
}

func TestR164PaperWorkerUsesOriginalAbsoluteDeadline(t *testing.T) {
	s := testServer(t)
	in := make(chan genfollowPaperIntent, 1)
	done := make(chan struct{})
	observed := make(chan time.Duration, 1)
	signalAt := time.Now().Add(-genfollowPaperSignalMaxAge + 150*time.Millisecond)
	started := time.Now()
	in <- genfollowPaperIntent{EnqueuedAt: signalAt, Signals: []genfollowPaperSignal{{
		SignalAt: signalAt,
		testRun: func(ctx context.Context) {
			<-ctx.Done()
			observed <- time.Since(started)
		},
	}}}
	close(in)
	go s.runGenfollowPaperWorkers(context.Background(), in, done)
	select {
	case elapsed := <-observed:
		if elapsed > 500*time.Millisecond {
			t.Fatalf("Paper work received a fresh timeout instead of original deadline: %v", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("original Paper deadline did not cancel active work")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deadline-canceled Paper worker did not stop")
	}
}
