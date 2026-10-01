package server

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/ev"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR156LivePolicyMirrorUsesDelayedTwoTouchIOCAndExactFee(t *testing.T) {
	s := testServer(t)
	if got := s.livePolicyMirrorExecutionDelay(); got != 750*time.Millisecond {
		t.Fatalf("first-to-second touch delay=%v want 750ms", got)
	}
	// The shared hermetic fixture disables the production wire wait so unrelated tests do not pay
	// it thousands of times. Restore the zero-value production mode for this explicit default test.
	s.livePolicyMirrorWireDelay = 0
	if got := s.livePolicyMirrorFinalWireDelay(); got != 100*time.Millisecond {
		t.Fatalf("final wire delay=%v want 100ms", got)
	}
	s.livePolicyMirrorWireDelay = -1

	if filled, why := livePolicyMirrorIOC(.40, 4, liveMirrorQuote{Price: .41, Depth: 10}); filled != 0 ||
		why != "mirror-ioc-limit-missed-after-wire-delay" {
		t.Fatalf("worse second touch filled=%v why=%q", filled, why)
	}
	if filled, why := livePolicyMirrorIOC(.40, 4, liveMirrorQuote{Price: .40, Depth: 0}); filled != 0 ||
		why != "mirror-ioc-visible-depth-zero-after-wire-delay" {
		t.Fatalf("empty second touch filled=%v why=%q", filled, why)
	}
	filled, why := livePolicyMirrorIOC(.40, 4, liveMirrorQuote{Price: .39, Depth: 1.5})
	if why != "" || math.Abs(filled-1.5) > 1e-12 {
		t.Fatalf("better partial second touch filled=%v why=%q, want 1.5", filled, why)
	}
	now := time.Now().UTC()
	first := liveMirrorQuote{BookSource: "kalshi_ws_full_orderbook:g4:s12:q90",
		ObservedAt: now.Add(-200 * time.Millisecond)}
	same := first
	if !livePolicyMirrorBookUsableAtWire(first, same) {
		t.Fatal("the same still-fresh WS frame must remain usable at simulated wire time")
	}
	sameSequenceLaterClock := first
	sameSequenceLaterClock.ObservedAt = first.ObservedAt.Add(time.Millisecond)
	if !livePolicyMirrorBookUsableAtWire(first, sameSequenceLaterClock) {
		t.Fatal("an unchanged sequence must remain usable after the wire delay")
	}
	lowerSequenceLaterClock := liveMirrorQuote{
		BookSource: "kalshi_ws_full_orderbook:g4:s12:q89",
		ObservedAt: first.ObservedAt.Add(time.Millisecond),
	}
	if livePolicyMirrorBookUsableAtWire(first, lowerSequenceLaterClock) {
		t.Fatal("a lower sequence in the same WS domain is not newer provenance")
	}
	nextSequence := liveMirrorQuote{BookSource: "kalshi_ws_full_orderbook:g4:s12:q91",
		ObservedAt: first.ObservedAt}
	if !livePolicyMirrorBookUsableAtWire(first, nextSequence) {
		t.Fatal("a strictly higher sequence in the same WS domain must be newer")
	}
	nextReceipt := liveMirrorQuote{BookSource: "kalshi_ws_full_orderbook:g5:s2:q1",
		ObservedAt: first.ObservedAt.Add(time.Millisecond)}
	if !livePolicyMirrorBookUsableAtWire(first, nextReceipt) {
		t.Fatal("a later receipt after reconnect must be newer")
	}

	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{
		"KXR156": {taker: .07, typ: "quadratic", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	fee, known, source := s.kalFeeExact("KXR156-MARKET", false, filled, .39)
	wantFee := ev.FeeBreakdownCoeff(.07, filled, .39, true).Net
	if !known || source != "quadratic" || math.Abs(fee-wantFee) > 1e-12 {
		t.Fatalf("exact partial-fill fee=%v known=%v source=%q want=%v/quadratic",
			fee, known, source, wantFee)
	}

	now = time.Now().UTC()
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR156-MARKET",
		Title: "R156 market", Side: "YES", Family: "kalshi-flow",
		Source: "auto-cons-kalshi-flow", Price: .40, At: now}
	row := s.livePolicyMirrorBaseRow(candidate)
	row.State, row.LimitPrice, row.RequestedQty = storage.LivePolicyMirrorFilled, .40, 4
	row.FilledQty, row.FillPrice, row.TouchDepth = filled, .39, 1.5
	row.FeeUSD, row.FeeSource, row.CostUSD = fee,
		"kalshi-exact-current-route-fee", filled*.39+fee
	row.ProofMean, row.ProofLower, row.ProofFeePC = .08, .04, fee/filled
	row.SizingBankroll, row.SizingTargetUSD = 400, 20
	row.EventKey, row.ClusterKey = "KXR156", "kalshi:event:KXR156"
	row.BookSource = "first-full-book -> second-full-book"
	if _, err := s.store.InsertLivePolicyMirror(context.Background(), row); err != nil {
		t.Fatalf("persist exact-fee partial fill: %v", err)
	}
	rows, err := s.store.ListLivePolicyMirror(context.Background(), 1)
	if err != nil || len(rows) != 1 {
		t.Fatalf("read exact-fee fill rows=%d err=%v", len(rows), err)
	}
	if math.Abs(rows[0].FeeUSD-wantFee) > 1e-12 ||
		math.Abs(rows[0].CostUSD-(filled*.39+wantFee)) > 1e-12 ||
		rows[0].BookSource != "first-full-book -> second-full-book" {
		t.Fatalf("stored partial fill fee=%v cost=%v books=%q",
			rows[0].FeeUSD, rows[0].CostUSD, rows[0].BookSource)
	}
}

func TestR156ActualPublicationAlwaysCompletesBeforeMirror(t *testing.T) {
	order := make([]string, 0, 2)
	accepted := publishActualBeforeMirror(func() bool {
		order = append(order, "actual")
		return true
	}, func() {
		order = append(order, "mirror")
	})
	if !accepted || strings.Join(order, ",") != "actual,mirror" {
		t.Fatalf("accepted=%v order=%v, want actual then mirror", accepted, order)
	}
	order = order[:0]
	accepted = publishActualBeforeMirror(func() bool {
		order = append(order, "actual-rejection-log")
		return false
	}, func() {
		order = append(order, "mirror-rejection")
	})
	if accepted || strings.Join(order, ",") != "actual-rejection-log,mirror-rejection" {
		t.Fatalf("rejection accepted=%v order=%v", accepted, order)
	}
}

func TestR156MirrorOnlyProofReceiptCannotMutateLiveEvidence(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	var before int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM unit_trials`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR156-ISOLATED",
		Side: "YES", Family: "kalshi-flow", SignalContractID: "contract",
		InputTopology: "K", At: time.Now()}
	if err := s.publishLivePriorityProofReceipt(ctx, candidate,
		liveMirrorQuote{Price: .40, Depth: 5, BookSource: "test"}, .01, "test",
		time.Now(), true); err != nil {
		t.Fatal(err)
	}
	var after int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM unit_trials`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("mirror-only receipt changed unit_trials %d -> %d", before, after)
	}
	if len(s.liveAllocationSignal) != 0 {
		t.Fatalf("mirror-only receipt mutated LIVE signal evidence: %+v", s.liveAllocationSignal)
	}
}

func TestR164MirrorWorkersSerializePortfolioAndRejectLateStart(t *testing.T) {
	s := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan livePolicyMirrorWork, 8)
	done := make(chan struct{})
	go s.runLivePolicyMirrorWorker(ctx, in, done)

	started := make(chan struct{}, 3)
	release := make(chan struct{})
	due := time.Now().Add(40 * time.Millisecond)
	for i := 0; i < 3; i++ {
		in <- livePolicyMirrorWork{DueAt: due, testRun: func(context.Context) {
			started <- struct{}{}
			<-release
		}}
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first fake-portfolio job did not start")
	}
	select {
	case <-started:
		t.Fatal("shared-ledger fake-portfolio work was not serialized")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	for i := 1; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatalf("only %d/3 serialized portfolio jobs drained", i)
		}
	}

	var lateRan atomic.Bool
	in <- livePolicyMirrorWork{DueAt: time.Now().Add(-livePolicyMirrorStartLateMax - time.Millisecond),
		testRun: func(context.Context) { lateRan.Store(true) }}
	close(in)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded mirror workers did not drain")
	}
	if lateRan.Load() {
		t.Fatal("late mirror task ran as though it were timely")
	}
}

func TestR157MirrorEarlierDueIntentIsNotBlockedByLaterTimerWaiters(t *testing.T) {
	s := testServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := make(chan livePolicyMirrorWork, 8)
	done := make(chan struct{})
	go s.runLivePolicyMirrorWorker(ctx, in, done)

	releaseLater := make(chan struct{})
	now := time.Now()
	for i := 0; i < livePolicyMirrorPriorityWorkerCount; i++ {
		in <- livePolicyMirrorWork{DueAt: now.Add(250 * time.Millisecond),
			testRun: func(context.Context) { <-releaseLater }}
	}
	urgentStarted := make(chan time.Time, 1)
	urgentDue := now.Add(40 * time.Millisecond)
	in <- livePolicyMirrorWork{DueAt: urgentDue, testRun: func(context.Context) {
		urgentStarted <- time.Now()
	}}
	close(in)

	select {
	case started := <-urgentStarted:
		if late := started.Sub(urgentDue); late < 0 || late > 100*time.Millisecond {
			t.Fatalf("earlier-due intent started %v after its own due time", late)
		}
		close(releaseLater)
	case <-time.After(180 * time.Millisecond):
		close(releaseLater)
		t.Fatal("later timer waiters occupied execution slots ahead of an earlier-due intent")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("per-intent scheduler did not drain")
	}
}

func TestR163PriorityMirrorClockIncludesCashFirstGrace(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = 125 * time.Millisecond
	at := time.Now().UTC().Add(-time.Second)
	candidate := liveMirrorCandidate{At: at}

	priorityDue := s.livePolicyMirrorWorkDueAt(livePolicyMirrorWork{
		SignalAt: at, Candidate: candidate, Preflighted: true,
	})
	wantPriority := at.Add(genfollowPaperLiveFirstGrace + 125*time.Millisecond)
	if !priorityDue.Equal(wantPriority) {
		t.Fatalf("priority due=%s want cash-first due=%s", priorityDue, wantPriority)
	}

	disarmedDue := s.livePolicyMirrorWorkDueAt(livePolicyMirrorWork{
		SignalAt: at, Candidate: candidate,
	})
	if want := at.Add(125 * time.Millisecond); !disarmedDue.Equal(want) {
		t.Fatalf("disarmed due=%s want=%s", disarmedDue, want)
	}

	rejectionDue := s.livePolicyMirrorWorkDueAt(livePolicyMirrorWork{
		SignalAt: at, Candidate: candidate, Preflighted: true, Reason: "preflight-rejected",
	})
	if !rejectionDue.Equal(at) {
		t.Fatalf("rejection due=%s want immediate=%s", rejectionDue, at)
	}
}

func TestR163ActiveLiveRejectionsDoNotFloodMirrorPortfolioQueue(t *testing.T) {
	s := testServer(t)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	signal := storage.Signal{Platform: "kalshi", Ticker: "KXR163-LIVE-REJECT",
		Title: "Active LIVE rejection", Side: "YES", SignalType: "kalshi-flow",
		EntryPrice: .40}
	if s.enqueueLivePolicyMirrorRejectionPrepared(signal, "current-book-unavailable",
		time.Now().UTC(), "shadow-attempt") {
		t.Fatal("active LIVE rejection entered the fake portfolio queue")
	}
	s.livePolicyMirrorMu.Lock()
	background, priority := s.livePolicyMirrorCh, s.livePolicyMirrorPriorityCh
	s.livePolicyMirrorMu.Unlock()
	if background != nil || priority != nil {
		t.Fatal("active LIVE rejection started mirror portfolio workers")
	}
}

func TestR163LivePassedMirrorStartsOnTimeThroughBackgroundFlood(t *testing.T) {
	s := testServer(t)
	// Hold two background workers and leave another two thousand durable rejection jobs queued.
	// This deterministically recreates the production failure where the old shared FIFO delayed a
	// valid candidate for minutes.
	s.livePolicyMirrorCapacity = 4096
	s.livePolicyMirrorPriorityCapacity = 8
	backgroundRelease := make(chan struct{})
	backgroundStarted := make(chan struct{}, livePolicyMirrorBackgroundWorkerCount)
	backgroundRun := func(ctx context.Context) {
		select {
		case backgroundStarted <- struct{}{}:
		default:
		}
		select {
		case <-backgroundRelease:
		case <-ctx.Done():
		}
	}
	now := time.Now()
	for i := 0; i < 2000; i++ {
		if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
			DueAt: now, Reason: "deterministic-rejection-flood", testRun: backgroundRun,
		}) {
			t.Fatalf("background rejection %d was not admitted to its bounded lane", i)
		}
	}
	for i := 0; i < livePolicyMirrorBackgroundWorkerCount; i++ {
		select {
		case <-backgroundStarted:
		case <-time.After(time.Second):
			t.Fatalf("only %d/%d background workers started", i, livePolicyMirrorBackgroundWorkerCount)
		}
	}
	// The channel alone is not queue truth: the scheduler has already removed hundreds of jobs
	// into its bounded waiter set. Pin all three states while the flood is held.
	telemetryDeadline := time.Now().Add(time.Second)
	for {
		s.livePolicyMirrorMu.Lock()
		channelDepth := len(s.livePolicyMirrorCh)
		s.livePolicyMirrorMu.Unlock()
		outstanding := int64(channelDepth) + s.livePolicyMirrorBackgroundWaiting.Load() +
			s.livePolicyMirrorBackgroundActive.Load()
		if outstanding == 2000 && s.livePolicyMirrorBackgroundWaiting.Load() > 0 {
			break
		}
		if time.Now().After(telemetryDeadline) {
			t.Fatalf("background telemetry never stabilized: outstanding=%d channel=%d waiting=%d active=%d",
				outstanding, channelDepth, s.livePolicyMirrorBackgroundWaiting.Load(),
				s.livePolicyMirrorBackgroundActive.Load())
		}
		time.Sleep(time.Millisecond)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/live-policy-mirror", nil)
	rec := httptest.NewRecorder()
	s.handleLivePolicyMirror(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("mirror telemetry status=%d body=%s", rec.Code, rec.Body.String())
	}
	var telemetry map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &telemetry); err != nil {
		t.Fatal(err)
	}
	outstandingByLane, ok := telemetry["work_outstanding"].(map[string]any)
	if !ok || outstandingByLane["rejection_or_disarmed"] != float64(2000) {
		t.Fatalf("API hid scheduler-owned background work: %+v", telemetry)
	}
	if telemetry["candidates"] != float64(0) || telemetry["queue_dropped"] != float64(0) {
		t.Fatalf("queued work was misreported as terminal or dropped: %+v", telemetry)
	}
	waitingByLane, ok := telemetry["work_waiting"].(map[string]any)
	if !ok || waitingByLane["rejection_or_disarmed"].(float64) <= 0 {
		t.Fatalf("API did not expose scheduler waiters: %+v", telemetry)
	}

	priorityStarted := make(chan time.Time, 1)
	due := time.Now().Add(60 * time.Millisecond)
	if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		DueAt: due, Preflighted: true,
		testRun: func(context.Context) { priorityStarted <- time.Now() },
	}) {
		t.Fatal("actual LIVE-passed candidate was not admitted to its independent priority lane")
	}
	select {
	case started := <-priorityStarted:
		if late := started.Sub(due); late < 0 || late > livePolicyMirrorStartLateMax {
			t.Fatalf("LIVE-passed candidate started %v after scheduled touch; want <=%v",
				late, livePolicyMirrorStartLateMax)
		}
	case <-time.After(time.Second):
		t.Fatal("background rejection flood blocked the LIVE-passed candidate")
	}

	close(backgroundRelease)
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatalf("priority/background worker did not drain after saturation: %v", err)
	}
	if got := s.livePolicyMirrorPriorityDropped.Load(); got != 0 {
		t.Fatalf("priority queue dropped %d LIVE-passed candidates under background-only saturation", got)
	}
	if got := s.livePolicyMirrorPriorityWaiting.Load() + s.livePolicyMirrorPriorityActive.Load() +
		s.livePolicyMirrorBackgroundWaiting.Load() + s.livePolicyMirrorBackgroundActive.Load(); got != 0 {
		t.Fatalf("mirror worker telemetry leaked %d jobs after drain", got)
	}
	if got := s.livePolicyMirrorPriorityOutstanding.Load() +
		s.livePolicyMirrorBackgroundOutstanding.Load(); got != 0 {
		t.Fatalf("authoritative mirror work gauge leaked %d jobs after drain", got)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/live-policy-mirror", nil)
	rec = httptest.NewRecorder()
	s.handleLivePolicyMirror(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("drained mirror telemetry status=%d body=%s", rec.Code, rec.Body.String())
	}
	telemetry = nil
	if err := json.Unmarshal(rec.Body.Bytes(), &telemetry); err != nil {
		t.Fatal(err)
	}
	outstandingByLane, ok = telemetry["work_outstanding"].(map[string]any)
	if !ok || outstandingByLane["live_passed"] != float64(0) ||
		outstandingByLane["rejection_or_disarmed"] != float64(0) ||
		telemetry["candidates"] != float64(0) || telemetry["queue_dropped"] != float64(0) {
		t.Fatalf("drained API retained phantom work or terminals: %+v", telemetry)
	}
}

func TestR163LivePassedMirrorIgnoresRawObservationsAndLaterActivityTimestamps(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorDelay = -1
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 0 }
	now := time.Now().UTC()
	signal := storage.Signal{Platform: "kalshi", Ticker: "KXR163-PRIORITY-QUIET",
		Title: "Priority mirror quiet gate", Side: "YES", SignalType: "spotlag",
		EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(signal, .04, now)
	candidate := liveCandidateFromSignalIntent(liveSignalIntent{
		Signal: signal, Point: .04, At: now, ShadowAttemptID: attemptID,
	})
	// Raw detector observations have no cash authority. Leave one queued and continually reset the
	// old global timestamp to prove neither can starve the actual LIVE-passed mirror lane.
	s.liveSignalIntentChannel() <- liveSignalIntent{Signal: signal, At: now}
	stopNoise := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.liveDispatchShadowLivePriorityAt.Store(time.Now().UnixNano())
			case <-stopNoise:
				return
			}
		}
	}()
	defer func() {
		close(stopNoise)
		<-s.liveSignalIntentChannel()
	}()

	started := time.Now()
	if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		Candidate: candidate, SignalAt: now, DueAt: now, Preflighted: true,
	}) {
		t.Fatal("LIVE-passed mirror work was not admitted")
	}
	deadline := time.Now().Add(time.Second)
	var row storage.LivePolicyMirrorRow
	for {
		rows, err := s.store.ListLivePolicyMirror(context.Background(), 10)
		if err != nil {
			t.Fatal(err)
		}
		for _, candidateRow := range rows {
			if candidateRow.Ticker == signal.Ticker {
				row = candidateRow
				break
			}
		}
		if row.ID > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("raw observations or later activity timestamps starved the priority mirror")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if elapsed := time.Since(started); elapsed > livePolicyMirrorStartLateMax {
		t.Fatalf("priority mirror took %v despite no cash-active/candidate-dispatch work", elapsed)
	}
	if row.State != storage.LivePolicyMirrorRejected ||
		row.Reason != "mirror-current-funded-entry-horizon-unavailable-or-exceeded" {
		t.Fatalf("real priority simulation terminal=%s/%q", row.State, row.Reason)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := s.stopExecutionShadowWriter(stopCtx); err != nil {
		t.Fatal(err)
	}
	views, err := s.store.ListExecutionShadowAttempts(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, view := range views {
		if view.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range view.Events {
			if event.Stage == "counterfactual-terminal" &&
				event.Reason == row.Reason {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("real priority path did not append its execution-shadow terminal")
	}
}

func TestR163MissedPriorityTouchSurvivesUnavailableMainMirrorDatabase(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	signal := storage.Signal{Platform: "kalshi", Ticker: "KXR163-MISSED-TOUCH",
		Title: "Missed mirror touch", Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(signal, .04, now)
	candidate := liveCandidateFromSignalIntent(liveSignalIntent{
		Signal: signal, Point: .04, At: now, ShadowAttemptID: attemptID,
	})
	// Close only the shared cash DB. The isolated execution-shadow DB remains available and must
	// receive the terminal after the priority deadline expires.
	if err := s.store.DBForTest().Close(); err != nil {
		t.Fatal(err)
	}
	s.liveDispatchShadowLiveActive.Store(1)
	if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		Candidate: candidate, SignalAt: now, DueAt: now, Preflighted: true,
	}) {
		t.Fatal("LIVE-passed mirror work was not admitted")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatalf("bounded priority deadline did not complete: %v", err)
	}
	s.liveDispatchShadowLiveActive.Store(0)
	if err := s.stopExecutionShadowWriter(stopCtx); err != nil {
		t.Fatal(err)
	}
	views, err := s.store.ListExecutionShadowAttempts(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, view := range views {
		if view.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range view.Events {
			if event.Stage == "counterfactual-terminal" &&
				event.ShadowState == storage.LivePolicyMirrorZeroFill &&
				event.Reason == "mirror-scheduled-touch-started-over-500ms-late" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("missed scheduled touch vanished when its main mirror row could not write")
	}
}

func TestR164LivePolicyMirrorShutdownDrainsExecutionAndBoundsOptionalPortfolio(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorCapacity = 16
	s.livePolicyMirrorPriorityCapacity = 16
	var priorityExecutionRan, priorityPortfolioRan, backgroundRan atomic.Int64
	now := time.Now()
	for i := 0; i < 7; i++ {
		if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
			DueAt: now, Preflighted: true,
			Execution:        livePolicyMirrorExecutionMeta{LiveReceiptSeen: true},
			testExecutionRun: func(context.Context) { priorityExecutionRan.Add(1) },
			testRun:          func(context.Context) { priorityPortfolioRan.Add(1) },
		}) {
			t.Fatalf("priority work %d was not admitted", i)
		}
		if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
			DueAt: now, Reason: "durable-rejection",
			testRun: func(context.Context) { backgroundRan.Add(1) },
		}) {
			t.Fatalf("background work %d was not admitted", i)
		}
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatalf("two-lane mirror shutdown did not drain: %v", err)
	}
	if got := priorityExecutionRan.Load(); got != 7 {
		t.Fatalf("priority execution drain ran %d/7 jobs", got)
	}
	if got := priorityPortfolioRan.Load(); got < 1 || got > 7 {
		t.Fatalf("optional portfolio drain ran %d jobs; want bounded subset of 1..7", got)
	}
	if got := backgroundRan.Load(); got != 7 {
		t.Fatalf("background drain ran %d/7 jobs", got)
	}
	if got := s.livePolicyMirrorPriorityWaiting.Load() + s.livePolicyMirrorPriorityActive.Load() +
		s.livePolicyMirrorBackgroundWaiting.Load() + s.livePolicyMirrorBackgroundActive.Load(); got != 0 {
		t.Fatalf("shutdown left %d mirror jobs in scheduler telemetry", got)
	}
	if s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		DueAt: time.Now(), Preflighted: true, testRun: func(context.Context) {},
	}) {
		t.Fatal("stopped mirror accepted new priority work")
	}
}

func TestR163LivePolicyMirrorShutdownPersistsQueuedRejections(t *testing.T) {
	s := testServer(t)
	s.liveDispatchShadowQuietDelay = -1
	s.livePolicyMirrorCapacity = 16
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	})
	now := time.Now().UTC()
	for _, ticker := range []string{"KXR163-R1", "KXR163-R2", "KXR163-R3", "KXR163-R4", "KXR163-R5"} {
		signal := storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "R163 rejection",
			Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40}
		candidate := liveCandidateFromSignalIntent(liveSignalIntent{Signal: signal, At: now})
		if !s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
			Signal: signal, SignalAt: now, Candidate: candidate,
			DueAt: now, Reason: "durable-bounded-rejection",
		}) {
			t.Fatalf("rejection %s was not admitted", ticker)
		}
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(stopCtx); err != nil {
		t.Fatalf("rejection lane did not drain durably: %v", err)
	}
	rows, err := s.store.ListLivePolicyMirror(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	var persisted int
	for _, row := range rows {
		if row.State == storage.LivePolicyMirrorRejected &&
			row.Reason == "durable-bounded-rejection" {
			persisted++
		}
	}
	if persisted != 5 {
		t.Fatalf("durable rejection rows=%d want 5", persisted)
	}
}

func TestR156LivePolicyMirrorRailsScaleFromExact400Ledger(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveMaxDailyLossUSD = -1
		c.Risk.LiveMaxOrderPct = .05
		c.Risk.LiveExposureCapPct = .20
		c.Risk.LiveCryptoCapPct = .10
		c.Risk.LiveClusterCapPct = .03
		c.Risk.LiveDailyLossPct = .075
	})
	for name, tc := range map[string]struct {
		pct  float64
		want float64
	}{
		"order":           {.05, 20},
		"total exposure":  {.20, 80},
		"crypto exposure": {.10, 40},
		"event cluster":   {.03, 12},
		"loss":            {.075, 30},
	} {
		got, ok := liveRailLimit(storage.LivePolicyMirrorSeedUSD, tc.pct, -1)
		if !ok || math.Abs(got-tc.want) > 1e-12 {
			t.Fatalf("%s rail=%v ok=%v want=%v from $400", name, got, ok, tc.want)
		}
	}

	normal := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXTENNIS-R156",
		Title: "Player A vs Player B", Side: "YES", Family: "kalshi-flow"}
	base := livePolicyMirrorPortfolio{
		Meta: storage.LivePolicyMirrorMeta{SeedUSD: 400, PeakNAVUSD: 400,
			DayKey: time.Now().UTC().Format("2006-01-02")},
		NAV: 400, NAVKnown: true, Cash: 400, ClusterCosts: map[string]float64{},
	}
	if got := s.livePolicyMirrorRiskReason(base, normal, 20.01); got != "mirror-5pct-order-rail" {
		t.Fatalf("order rail reason=%q", got)
	}
	total := base
	total.OpenCost = 71
	if got := s.livePolicyMirrorRiskReason(total, normal, 10); got != "mirror-20pct-total-exposure-rail" {
		t.Fatalf("total exposure rail reason=%q", got)
	}
	cryptoCandidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXBTC-R156",
		Title: "BTC price", Side: "YES", Family: "spotlag"}
	crypto := base
	crypto.OpenCost, crypto.CryptoCost = 31, 31
	if got := s.livePolicyMirrorRiskReason(crypto, cryptoCandidate, 10); got != "mirror-10pct-crypto-exposure-rail" {
		t.Fatalf("crypto rail reason=%q", got)
	}
	cluster := base
	clusterKey := s.liveMirrorClusterKey("kalshi", normal.Ticker, normal.Title)
	cluster.ClusterCosts = map[string]float64{clusterKey: 2.01}
	if got := s.livePolicyMirrorRiskReason(cluster, normal, 10); got != "mirror-3pct-event-cluster-rail" {
		t.Fatalf("cluster rail reason=%q", got)
	}
	loss := base
	loss.NAV = 369.99
	if got := s.livePolicyMirrorRiskReason(loss, normal, 1); got != "mirror-7.5pct-session-loss-stop" {
		t.Fatalf("loss-stop reason=%q", got)
	}
}

func TestR156MirrorPersistsEveryKnownPeakBeforeRisk(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	initialMeta, err := s.store.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := livePolicyMirrorPortfolio{Meta: initialMeta, NAVKnown: true, NAV: 412.25}
	if err := s.persistLivePolicyMirrorPeak(ctx, &p); err != nil {
		t.Fatal(err)
	}
	meta, err := s.store.LivePolicyMirrorMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(meta.PeakNAVUSD-412.25) > 1e-12 || math.Abs(p.Meta.PeakNAVUSD-412.25) > 1e-12 {
		t.Fatalf("persisted/in-memory peak=%v/%v want 412.25", meta.PeakNAVUSD, p.Meta.PeakNAVUSD)
	}
	p.NAV = 405
	if err = s.persistLivePolicyMirrorPeak(ctx, &p); err != nil {
		t.Fatal(err)
	}
	meta, err = s.store.LivePolicyMirrorMeta(ctx)
	if err != nil || math.Abs(meta.PeakNAVUSD-412.25) > 1e-12 {
		t.Fatalf("lower known NAV changed peak: meta=%+v err=%v", meta, err)
	}
}

func TestR156MirrorAPINullsTotalPnLWhenAnyOpenMarkUnknown(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	row := storage.LivePolicyMirrorRow{
		CandidateID: "api-unknown-mark", IntentKey: "api-unknown-intent",
		ObservedAt: now, ProcessedAt: now, Venue: "kalshi",
		Ticker: "KXR156-NOMARK", Title: "No executable exit", Side: "YES",
		SystemID: "kalshi-flow", Route: "taker", ModelVersion: livePolicyMirrorModelVersion,
		State: storage.LivePolicyMirrorFilled, SignalPrice: .40, LimitPrice: .40,
		RequestedQty: 2, FilledQty: 2, FillPrice: .40, TouchDepth: 2,
		FeeUSD: .10, FeeSource: "kalshi:test", CostUSD: .90,
		ProofMean: .08, ProofLower: .04, ProofFeePC: .05,
		SizingBankroll: 400, SizingTargetUSD: 5, EventKey: "KXR156",
		ClusterKey: "kalshi:event:KXR156", DelayMS: 750, WireDelayMS: 100,
		BookSource: "kalshi_ws_full_orderbook:g1:s1:q1",
	}
	if _, err := s.store.InsertLivePolicyMirror(context.Background(), row); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/live-policy-mirror", nil)
	rec := httptest.NewRecorder()
	s.handleLivePolicyMirror(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"nav_usd", "open_mark_net_usd", "net_pnl_usd"} {
		if body[key] != nil {
			t.Fatalf("%s=%v, want JSON null while an open mark is unknown", key, body[key])
		}
	}
	if known, _ := body["nav_known"].(bool); known {
		t.Fatal("nav_known=true with an unmarked open position")
	}
}

func TestR156LivePolicyMirrorCollectsWhileDisarmedWithoutMoneyOrPaperQueue(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemAllowlist = "kalshi|kalshi-flow|YES|taker"
	})
	// Supply a bounded channel directly so this test can inspect publication without starting the
	// worker or touching a book/proof source.
	s.livePolicyMirrorCh = make(chan livePolicyMirrorWork, 2)
	signal := storage.Signal{Platform: "kalshi", Ticker: "KXR156-DISARMED",
		Title: "Disarmed mirror", Side: "YES", SignalType: "kalshi-flow", EntryPrice: .40}
	if !s.queueLivePolicyMirrorSignal(signal, .04) {
		c := liveMirrorCandidate{Platform: signal.Platform, Ticker: signal.Ticker,
			Side: signal.Side, Family: signal.SignalType, Source: "auto-cons-" + signal.SignalType}
		t.Fatalf("disarmed exact allowlisted signal was not published: selection=%q on=%v mirror=%v venue=%v cfg=%q",
			s.liveSystemSelectionReason(c, "taker"), s.liveOperatorAutoOn(), s.liveMirrorEnabled(),
			s.cfg().Risk.LiveSystemKalshi, s.cfg().Risk.LiveSystemAllowlist)
	}
	if got := len(s.livePolicyMirrorCh); got != 1 {
		t.Fatalf("mirror queue len=%d want 1", got)
	}
	if s.liveSignalIntentCh != nil {
		t.Fatal("mirror publication must not create or write the actual LIVE intent queue")
	}

	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	signal.Ticker = "KXR156-ARMED"
	if s.queueLivePolicyMirrorSignal(signal, .04) {
		t.Fatal("raw mirror lane must stand down while actual LIVE owns the exact preflight")
	}
	if got := len(s.livePolicyMirrorCh); got != 1 {
		t.Fatalf("armed raw publication changed mirror queue len to %d", got)
	}
}

func TestR156LivePolicyMirrorBriefingUsesOnlyLastGoodSnapshot(t *testing.T) {
	s := testServer(t)
	s.livePolicyMirrorBrief.Store(&livePolicyMirrorBriefSnapshot{
		At: time.Now(), Cash: 376.25, NAV: 405.50, Net: 5.50, NAVKnown: true,
		Filled: 7, Open: 2, Settled: 5, ZeroFill: 3, Rejected: 11,
	})
	s.livePolicyMirrorDropped.Store(4)
	line := s.livePolicyMirrorBriefLine()
	for _, want := range []string{
		"$400 LIVE-policy mirror", "cash $376.25", "NAV $405.50", "net +5.50",
		"filled 7", "open 2", "settled 5", "zero 3", "rejected 11", "dropped 4",
	} {
		if !strings.Contains(line, want) {
			t.Fatalf("last-good mirror line missing %q: %s", want, line)
		}
	}
	if got := s.BriefingText(context.Background()); !strings.Contains(got, line) {
		t.Fatalf("recurring briefing omitted cached LIVE-policy mirror line:\n%s", got)
	}
}
