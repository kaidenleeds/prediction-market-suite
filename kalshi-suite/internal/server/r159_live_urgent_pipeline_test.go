package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r159AdmittedCandidate(s *Server, ticker string) liveMirrorCandidate {
	return liveMirrorCandidate{
		Platform: "kalshi", Ticker: ticker, Side: "YES", Action: "BUY",
		Family: "spotlag", Source: "auto-cons-spotlag", Price: .40, At: time.Now(),
		LiveIntentGeneration: s.liveSignalIntentGeneration.Load(), LiveIntentGenerationBound: true,
	}
}

func r159WaitFor(t *testing.T, timeout time.Duration, condition func() bool, detail string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", detail)
}

func TestR159UrgentSignalIntakeSubmitsNextBurstWhilePriorPreflightRuns(t *testing.T) {
	if liveMirrorArbitrationWindow > 10*time.Millisecond {
		t.Fatalf("raw arbitration window=%s exceeds 10ms", liveMirrorArbitrationWindow)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	runStarted := make(chan struct{}, 1)
	scheduler := newLiveSignalPreflightScheduler(nil, ctx, 1, 8,
		func(context.Context, liveSignalIntent) bool {
			select {
			case runStarted <- struct{}{}:
			default:
			}
			<-release
			return false
		}, nil)
	defer func() {
		close(release)
		scheduler.stop()
	}()

	intents := make(chan liveSignalIntent, 4)
	submitted := make(chan struct{}, 4)
	go runLiveUrgentSignalIntakeLoop(ctx, liveUrgentIntakeHooks{
		Intents: intents,
		Collect: func(collectCtx context.Context, first *liveSignalIntent) []liveSignalIntent {
			timer := time.NewTimer(liveMirrorArbitrationWindow)
			defer timer.Stop()
			select {
			case <-collectCtx.Done():
				return nil
			case <-timer.C:
				return []liveSignalIntent{*first}
			}
		},
		Submit: func(submitCtx context.Context, batch []liveSignalIntent) {
			_ = scheduler.submit(submitCtx, batch)
			submitted <- struct{}{}
		},
	})
	now := time.Now()
	intents <- liveSignalIntent{Point: .04, At: now}
	select {
	case <-submitted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("first arbitration window did not publish to asynchronous preflight")
	}
	select {
	case <-runStarted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("first preflight did not start")
	}
	// The sole preflight worker is deliberately blocked. Raw intake must still spend only its
	// arbitration window and enqueue this next burst instead of waiting for that worker.
	intents <- liveSignalIntent{Point: .03, At: now}
	select {
	case <-submitted:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("raw intake waited for the prior preflight result")
	}
}

func TestR159AutoGenerationRejectsOldPreflightAfterFastOffOn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &Server{liveArmed: true, liveAuto: true}
	oldStarted := make(chan liveSignalIntent, 1)
	releaseOld := make(chan struct{})
	candidateFor := func(intent liveSignalIntent) liveMirrorCandidate {
		return liveMirrorCandidate{
			Platform: "kalshi", Ticker: intent.Signal.Ticker, Side: "YES", Action: "BUY",
			Family: "spotlag", Source: "auto-cons-spotlag", Price: .40, At: time.Now(),
			LiveIntentGeneration: intent.Generation, LiveIntentGenerationBound: intent.GenerationBound,
		}
	}
	oldScheduler := newLiveSignalPreflightScheduler(s, ctx, 1, 8,
		func(_ context.Context, intent liveSignalIntent) bool {
			oldStarted <- intent
			<-releaseOld // Deliberately ignores cancellation, matching the former boundary race.
			return s.enqueueLiveMirrorCandidate(candidateFor(intent))
		}, nil)
	if _, loaded := liveSignalPreflightSchedulers.LoadOrStore(s, oldScheduler); loaded {
		oldScheduler.stop()
		t.Fatal("test server unexpectedly already had a preflight scheduler")
	}
	defer func() {
		liveSignalPreflightSchedulers.CompareAndDelete(s, oldScheduler)
		oldScheduler.stop()
	}()
	oldBatch := oldScheduler.submit(ctx, []liveSignalIntent{{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR159-OLD-EPOCH", Side: "YES",
			SignalType: "spotlag", EntryPrice: .40},
		Point: .08, At: time.Now(),
	}})
	var oldIntent liveSignalIntent
	select {
	case oldIntent = <-oldStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("old-generation preflight did not start")
	}
	if !oldIntent.GenerationBound {
		t.Fatal("production Server scheduler did not bind the preflight job to an AUTO generation")
	}

	// Match the real OFF then ON handlers: both clear boundaries occur while AUTO is false, and
	// neither waits for the active worker that is still deliberately blocked above.
	s.liveMu.Lock()
	s.liveAuto = false
	s.liveMu.Unlock()
	clearReturned := make(chan struct{})
	go func() {
		s.clearLiveMirrorCandidates()
		close(clearReturned)
	}()
	select {
	case <-clearReturned:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("AUTO OFF boundary joined a running preflight worker")
	}
	s.clearLiveMirrorCandidates()
	s.liveMu.Lock()
	s.liveAuto = true
	s.liveMu.Unlock()

	close(releaseOld)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if accepted := oldBatch.wait(waitCtx); accepted != 0 {
		t.Fatalf("old-generation worker accepted=%d after OFF/ON, want 0", accepted)
	}
	s.liveMirrorMu.Lock()
	queuedAfterOld := len(s.liveMirrorQ)
	s.liveMirrorMu.Unlock()
	if queuedAfterOld != 0 {
		t.Fatalf("old-generation worker published %d candidates into the new AUTO session", queuedAfterOld)
	}

	// A new scheduler binds to the new generation and can publish normally; the boundary is not a
	// permanent pause.
	newScheduler := newLiveSignalPreflightScheduler(s, ctx, 1, 8,
		func(_ context.Context, intent liveSignalIntent) bool {
			return s.enqueueLiveMirrorCandidate(candidateFor(intent))
		}, nil)
	if _, loaded := liveSignalPreflightSchedulers.LoadOrStore(s, newScheduler); loaded {
		newScheduler.stop()
		t.Fatal("stopped old scheduler remained registered after clear")
	}
	defer func() {
		liveSignalPreflightSchedulers.CompareAndDelete(s, newScheduler)
		newScheduler.stop()
	}()
	newBatch := newScheduler.submit(ctx, []liveSignalIntent{{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR159-NEW-EPOCH", Side: "YES",
			SignalType: "spotlag", EntryPrice: .40},
		Point: .08, At: time.Now(),
	}})
	if accepted := newBatch.wait(waitCtx); accepted != 1 {
		t.Fatalf("new-generation worker accepted=%d, want 1", accepted)
	}
	s.liveMirrorMu.Lock()
	defer s.liveMirrorMu.Unlock()
	if len(s.liveMirrorQ) != 1 || s.liveMirrorQ[0].Ticker != "KXR159-NEW-EPOCH" {
		t.Fatalf("new AUTO session queue=%+v", s.liveMirrorQ)
	}
}

func TestR159PoppedCandidateCannotDispatchAfterFastOffOn(t *testing.T) {
	s := &Server{liveArmed: true, liveAuto: true}
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR159-POPPED-OLD-EPOCH", Side: "YES", Action: "BUY",
		Family: "spotlag", Source: "auto-cons-spotlag", Price: .40, At: time.Now(),
		Canary: true, LiveIntentGeneration: s.liveSignalIntentGeneration.Load(),
		LiveIntentGenerationBound: true,
	}
	if !s.enqueueLiveMirrorCandidate(candidate) {
		t.Fatal("current-generation fixture did not enter the candidate queue")
	}
	popped, ok := s.popLiveMirrorCandidate()
	if !ok {
		t.Fatal("fixture was not popped before the AUTO boundary")
	}

	s.liveMu.Lock()
	s.liveAuto = false
	s.liveMu.Unlock()
	s.clearLiveMirrorCandidates() // OFF boundary
	s.clearLiveMirrorCandidates() // ON clean-start boundary
	s.liveMu.Lock()
	s.liveAuto = true
	s.liveMu.Unlock()

	if dispatched, why := s.dispatchLiveMirror(context.Background(), popped); dispatched ||
		why != liveIntentGenerationChangedReason {
		t.Fatalf("popped old-generation candidate dispatch=%v why=%q", dispatched, why)
	}
}

func TestR159GenerationGateAllowsUnboundParsingOnlyForManualRequests(t *testing.T) {
	s := &Server{}
	current := s.liveSignalIntentGeneration.Load()
	if why := s.liveIntentGenerationGateReason(false, 0, false, true); why != "" {
		t.Fatalf("manual unbound request was rejected: %q", why)
	}
	if why := s.liveIntentGenerationGateReason(true, 0, false, true); why != liveIntentGenerationRequiredReason {
		t.Fatalf("AUTO unbound reason=%q", why)
	}
	if why := s.liveIntentGenerationGateReason(true, current, true, true); why != "" {
		t.Fatalf("current bound request was rejected: %q", why)
	}
	s.liveSignalIntentGeneration.Add(1)
	if why := s.liveIntentGenerationGateReason(true, current, true, true); why != liveIntentGenerationChangedReason {
		t.Fatalf("stale bound AUTO request reason=%q", why)
	}
}

func TestR159PoppedSealedCandidateIsBoundAndCannotCrossFastOffOn(t *testing.T) {
	s := &Server{liveArmed: true, liveAuto: true}
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR159-SEALED-OLD-EPOCH", Side: "YES", Action: "BUY",
		Family: "sealed-fixture", Source: "r139p:sealed-fixture", Price: .40, At: time.Now(),
		// Deliberately unbound, matching legacy sealed candidate construction.
	}
	if !s.enqueueLiveMirrorCandidate(candidate) {
		t.Fatal("legacy sealed fixture did not enter the candidate queue")
	}
	popped, ok := s.popLiveMirrorCandidate()
	if !ok {
		t.Fatal("sealed fixture was not popped before the AUTO boundary")
	}
	if !popped.LiveIntentGenerationBound ||
		popped.LiveIntentGeneration != s.liveSignalIntentGeneration.Load() {
		t.Fatalf("sealed candidate left queue admission unbound: %+v", popped)
	}

	s.liveMu.Lock()
	s.liveAuto = false
	s.liveMu.Unlock()
	s.clearLiveMirrorCandidates()
	s.clearLiveMirrorCandidates()
	s.liveMu.Lock()
	s.liveAuto = true
	s.liveMu.Unlock()

	if dispatched, why := s.dispatchLiveMirror(context.Background(), popped); dispatched ||
		why != liveIntentGenerationChangedReason {
		t.Fatalf("popped sealed candidate dispatch=%v why=%q", dispatched, why)
	}
}

func TestR159GenerationReachesBothFinalVenueWriteFences(t *testing.T) {
	functionBlock := func(path, signature string) string {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		src := string(raw)
		start := strings.Index(src, signature)
		if start < 0 {
			t.Fatalf("%s missing %s", path, signature)
		}
		tail := src[start+len(signature):]
		end := strings.Index(tail, "\nfunc ")
		if end < 0 {
			return src[start:]
		}
		return src[start : start+len(signature)+end]
	}

	dispatch := functionBlock("liveautomirror.go",
		"func (s *Server) dispatchLiveMirror")
	if strings.Count(dispatch, `"live_intent_generation"`) < 2 ||
		strings.Count(dispatch, `"live_intent_generation_bound"`) < 2 {
		t.Fatal("internal Kalshi and PolyUS requests do not both carry the bound AUTO generation")
	}

	for _, tc := range []struct {
		name, signature, signalMarker, generationMarker, venueCall string
	}{
		{"kalshi", "func (s *Server) handleLivePlace",
			`"wire-signal-deadline"`, `"wire-auto-generation"`, "s.kal.CreateOrder(postCtx, req)"},
		{"polyus", "func (s *Server) handlePolyUSLiveOrder",
			`"polyus-wire-signal-deadline"`, `"polyus-wire-auto-generation"`,
			"s.polyUSAuth.PlaceLiveOrder(ctx, polymarketus.LiveOrder{"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			block := functionBlock("server.go", tc.signature)
			lockAt := strings.Index(block, "s.liveWriteFence.RLock()")
			signalAt := strings.Index(block, tc.signalMarker)
			generationAt := strings.Index(block, tc.generationMarker)
			callAt := strings.Index(block, tc.venueCall)
			unlockAfterCall := -1
			if callAt >= 0 {
				unlockAfterCall = strings.Index(block[callAt:], "s.liveWriteFence.RUnlock()")
			}
			if lockAt < 0 || signalAt < lockAt || generationAt < signalAt ||
				callAt < generationAt || unlockAfterCall < 0 {
				t.Fatalf("final generation gate is not ordered inside the write fence: lock=%d signal=%d generation=%d call=%d unlock_after=%d",
					lockAt, signalAt, generationAt, callAt, unlockAfterCall)
			}
		})
	}
}

func TestR159LiveAutoOffWaitsForFinalVenueFenceBeforePublishingBoundary(t *testing.T) {
	s := testServer(t)
	r153ArmAuto(s)
	oldGeneration := s.liveSignalIntentGeneration.Load()
	candidate := r159AdmittedCandidate(s, "KXR159-OFF-FENCE")
	s.liveMirrorMu.Lock()
	s.liveMirrorQ = append(s.liveMirrorQ, candidate)
	s.liveMirrorComboQ = append(s.liveMirrorComboQ, candidate)
	s.liveMirrorMu.Unlock()

	// Model a handler that already owns its final venue fence. The operator OFF handler must wait
	// for that call to finish; it may not publish AUTO=false first and leave the old call running
	// outside the transition boundary.
	s.liveWriteFence.RLock()
	fenceHeld := true
	defer func() {
		if fenceHeld {
			s.liveWriteFence.RUnlock()
		}
	}()
	started := make(chan struct{})
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		close(started)
		request := httptest.NewRequest(http.MethodPost, "/api/live/auto",
			strings.NewReader(`{"on":0}`))
		response := httptest.NewRecorder()
		s.handleLiveAuto(response, request)
		done <- response
	}()
	<-started
	select {
	case <-done:
		t.Fatal("AUTO OFF returned while a final venue read fence was still held")
	case <-time.After(30 * time.Millisecond):
	}
	s.liveMu.Lock()
	autoWhileCallInFlight := s.liveAuto
	s.liveMu.Unlock()
	if !autoWhileCallInFlight {
		t.Fatal("AUTO OFF became visible before the prior final venue call finished")
	}
	if got := s.liveSignalIntentGeneration.Load(); got != oldGeneration {
		t.Fatalf("generation advanced behind an in-flight venue call: got=%d want=%d", got, oldGeneration)
	}
	s.liveMirrorMu.Lock()
	queuedWhileCallInFlight := len(s.liveMirrorQ)
	s.liveMirrorMu.Unlock()
	if queuedWhileCallInFlight != 1 {
		t.Fatalf("queue cleared before the final venue call finished: remaining=%d", queuedWhileCallInFlight)
	}

	s.liveWriteFence.RUnlock()
	fenceHeld = false
	var response *httptest.ResponseRecorder
	select {
	case response = <-done:
	case <-time.After(time.Second):
		t.Fatal("AUTO OFF did not finish after the final venue fence was released")
	}
	if response.Code != http.StatusOK {
		t.Fatalf("AUTO OFF status=%d body=%s", response.Code, response.Body.String())
	}
	s.liveMu.Lock()
	autoAfterBoundary := s.liveAuto
	s.liveMu.Unlock()
	if autoAfterBoundary {
		t.Fatal("AUTO remained enabled after the atomic OFF boundary")
	}
	if got := s.liveSignalIntentGeneration.Load(); got == oldGeneration {
		t.Fatal("AUTO OFF did not publish a new generation")
	}
	s.liveMirrorMu.Lock()
	defer s.liveMirrorMu.Unlock()
	if len(s.liveMirrorQ) != 0 || len(s.liveMirrorComboQ) != 0 {
		t.Fatalf("AUTO OFF left old candidates queued: singles=%d combos=%d",
			len(s.liveMirrorQ), len(s.liveMirrorComboQ))
	}
}

func TestR159DisabledDrainIsUngatedDurableAndCannotReplayAfterKillReset(t *testing.T) {
	s := testServer(t)
	r153ArmAuto(s)
	oldGeneration := s.liveSignalIntentGeneration.Load()
	candidate := r159AdmittedCandidate(s, "KXR159-KILL-DRAIN")
	s.liveMirrorMu.Lock()
	s.liveMirrorQ = append(s.liveMirrorQ, candidate)
	s.liveMirrorComboQ = append(s.liveMirrorComboQ, candidate)
	s.liveMirrorSeen = map[string]time.Time{candidate.key(): time.Now()}
	s.liveAllocationSignal = map[string]time.Time{"fixture": time.Now()}
	s.liveSignalIntentSeen = map[string]time.Time{"fixture": time.Now()}
	s.liveMirrorMu.Unlock()
	s.liveSignalIntentChannel() <- liveSignalIntent{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR159-KILL-INTENT",
			Side: "NO", SignalType: "kalshi-flow", EntryPrice: .44},
		Point: .05, At: time.Now(), Generation: oldGeneration, GenerationBound: true,
	}

	s.ks.Trip("r159 deterministic kill fixture")
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = false, false
	s.liveMu.Unlock()
	s.discardLiveUrgentCandidatesWhileDisabled()
	if got := s.liveSignalIntentGeneration.Load(); got == oldGeneration {
		t.Fatal("disabled drain did not invalidate the admitted generation")
	}
	s.liveMirrorMu.Lock()
	if len(s.liveMirrorQ) != 0 || len(s.liveMirrorComboQ) != 0 ||
		s.liveMirrorSeen != nil || s.liveAllocationSignal != nil || s.liveSignalIntentSeen != nil {
		t.Fatalf("disabled drain left queue/session state: singles=%d combos=%d seen=%v allocation=%v intents=%v",
			len(s.liveMirrorQ), len(s.liveMirrorComboQ), s.liveMirrorSeen,
			s.liveAllocationSignal, s.liveSignalIntentSeen)
	}
	s.liveMirrorMu.Unlock()
	if len(s.liveSignalIntentChannel()) != 0 {
		t.Fatal("disabled drain left a raw intent queued")
	}

	s.ks.Reset()
	r153ArmAuto(s)
	if replayed, ok := s.popLiveMirrorCandidate(); ok {
		t.Fatalf("kill/reset/re-arm replayed an old candidate: %+v", replayed)
	}
	rows := r153CandidateDropRows(t, s)
	branches := map[string]bool{}
	for _, row := range rows {
		if row["reason"] == "live-runtime-gate-disabled-before-urgent-dispatch" {
			branches[row["queue_branch"].(string)] = true
		}
	}
	for _, branch := range []string{"live-mirror-single", "live-mirror-combo", "live-signal-intent"} {
		if !branches[branch] {
			t.Fatalf("disabled drain lost durable %s receipt: %v", branch, rows)
		}
	}
}

func TestR159ArbitrationRequeueDropsOldGenerationAfterFastOffOn(t *testing.T) {
	s := testServer(t)
	r153ArmAuto(s)
	old := r159AdmittedCandidate(s, "KXR159-REQUEUE-OLD-EPOCH")

	if _, why := s.transitionLiveAuto(false); why != "" {
		t.Fatalf("AUTO OFF transition failed: %s", why)
	}
	if _, why := s.transitionLiveAuto(true); why != "" {
		t.Fatalf("AUTO ON transition failed: %s", why)
	}
	s.requeueLiveMirrorBatch([]liveMirrorCandidate{old})
	s.liveMirrorMu.Lock()
	queued := len(s.liveMirrorQ)
	s.liveMirrorMu.Unlock()
	if queued != 0 {
		t.Fatalf("old arbitration batch re-entered the new generation: queued=%d", queued)
	}
	rows := r153CandidateDropRows(t, s)
	found := false
	for _, row := range rows {
		if row["ticker"] == old.Ticker && row["stage"] == "arbitration-requeue" &&
			row["reason"] == liveIntentGenerationChangedReason {
			found = true
		}
	}
	if !found {
		t.Fatalf("old requeue generation rejection was not durable: %v", rows)
	}
}

func TestR159UrgentCandidateDispatcherIsSerialAndRearmsPendingWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	wake <- struct{}{}
	var pending atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	var calls atomic.Int32
	pending.Store(3)
	rearm := func() {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	go runLiveUrgentCandidateDispatchLoop(ctx, liveUrgentDispatchHooks{
		Wake:    wake,
		Pending: func() bool { return pending.Load() > 0 },
		Gate:    func(time.Time) liveAutoRuntimeDecision { return liveAutoRuntimeDecision{Dispatch: true} },
		Dispatch: func(context.Context) {
			now := active.Add(1)
			for {
				seen := maxActive.Load()
				if now <= seen || maxActive.CompareAndSwap(seen, now) {
					break
				}
			}
			time.Sleep(4 * time.Millisecond)
			active.Add(-1)
			pending.Add(-1)
			calls.Add(1)
		},
		Rearm: rearm,
	}, 50*time.Millisecond, time.Second)

	r159WaitFor(t, 250*time.Millisecond, func() bool { return pending.Load() == 0 },
		"all pending candidates to be rearmed")
	if calls.Load() != 3 {
		t.Fatalf("dispatch calls=%d want=3", calls.Load())
	}
	if maxActive.Load() != 1 {
		t.Fatalf("concurrent venue dispatches=%d want exactly one", maxActive.Load())
	}
}

func TestR159UrgentCandidateDispatcherFailsafeRetainsSafetyPausedWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1) // deliberately never signaled: exercise pending-queue failsafe
	var pending atomic.Bool
	var paused atomic.Bool
	var calls atomic.Int32
	var disabled atomic.Int32
	pending.Store(true)
	paused.Store(true)
	go runLiveUrgentCandidateDispatchLoop(ctx, liveUrgentDispatchHooks{
		Wake:    wake,
		Pending: pending.Load,
		Gate: func(time.Time) liveAutoRuntimeDecision {
			if paused.Load() {
				return liveAutoRuntimeDecision{Paused: true, Reason: "test safety pause"}
			}
			return liveAutoRuntimeDecision{Dispatch: true}
		},
		Dispatch: func(context.Context) {
			calls.Add(1)
			pending.Store(false)
		},
		Disabled: func() {
			disabled.Add(1)
			pending.Store(false)
		},
	}, 5*time.Millisecond, time.Second)
	time.Sleep(25 * time.Millisecond)
	if calls.Load() != 0 || disabled.Load() != 0 || !pending.Load() {
		t.Fatalf("paused work changed: calls=%d disabled=%d pending=%v",
			calls.Load(), disabled.Load(), pending.Load())
	}
	paused.Store(false)
	r159WaitFor(t, 100*time.Millisecond, func() bool { return calls.Load() == 1 },
		"lost-wake failsafe dispatch after safety recovery")
}

func TestR159UrgentCandidateDispatcherDiscardsDisabledWorkWithoutDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	wake <- struct{}{}
	var pending atomic.Bool
	var calls atomic.Int32
	var disabled atomic.Int32
	pending.Store(true)
	go runLiveUrgentCandidateDispatchLoop(ctx, liveUrgentDispatchHooks{
		Wake:     wake,
		Pending:  pending.Load,
		Gate:     func(time.Time) liveAutoRuntimeDecision { return liveAutoRuntimeDecision{} },
		Dispatch: func(context.Context) { calls.Add(1) },
		Disabled: func() {
			disabled.Add(1)
			pending.Store(false)
		},
	}, 5*time.Millisecond, time.Second)
	r159WaitFor(t, 100*time.Millisecond, func() bool { return disabled.Load() == 1 },
		"disabled queue discard")
	if calls.Load() != 0 {
		t.Fatalf("disabled gate reached dispatch %d times", calls.Load())
	}
}

func TestR159UrgentLoopRegistryAllowsOnlyOneOwner(t *testing.T) {
	var registry sync.Map
	server := &Server{}
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 1)
	run := func(runCtx context.Context) {
		started <- struct{}{}
		<-runCtx.Done()
	}
	if !startLiveUrgentLoop(&registry, server, ctx, "r159-registry-test", run) {
		t.Fatal("first urgent loop owner was refused")
	}
	if startLiveUrgentLoop(&registry, server, ctx, "r159-registry-test-duplicate", run) {
		t.Fatal("second urgent loop owner was allowed")
	}
	select {
	case <-started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("registered urgent loop did not start")
	}
	loaded, ok := registry.Load(server)
	if !ok {
		t.Fatal("urgent loop registration disappeared while running")
	}
	cancel()
	select {
	case <-loaded.(*liveUrgentLoopRegistration).done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("urgent loop did not stop with its lifecycle context")
	}
	if _, stillLoaded := registry.Load(server); stillLoaded {
		t.Fatal("stopped urgent loop kept its registry seat")
	}
}
