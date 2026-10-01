package server

import (
	"context"
	"sync"
	"time"
)

const (
	// The wake channel remains the normal path. This timer is only a lost/coalesced-wake belt over
	// the already-bounded mirror queue, replacing the seven-second maintenance tick as recovery.
	liveUrgentDispatchFailsafe = 100 * time.Millisecond
	liveUrgentDispatchTimeout  = 12 * time.Second
	liveUrgentDispatchBatch    = 2
)

// liveUrgentIntakeHooks keeps the raw signal reader mechanically separate from book, proof,
// account and venue work. Collect owns only the existing <=10ms arbitration window; Submit must
// publish into the persistent asynchronous preflight scheduler and return without waiting for a
// worker or candidate result.
type liveUrgentIntakeHooks struct {
	Intents <-chan liveSignalIntent
	Collect func(context.Context, *liveSignalIntent) []liveSignalIntent
	Submit  func(context.Context, []liveSignalIntent)
}

func runLiveUrgentSignalIntakeLoop(ctx context.Context, hooks liveUrgentIntakeHooks) {
	if ctx == nil || hooks.Intents == nil || hooks.Collect == nil || hooks.Submit == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case intent, open := <-hooks.Intents:
			if !open {
				return
			}
			batch := hooks.Collect(ctx, &intent)
			if len(batch) > 0 {
				hooks.Submit(ctx, batch)
			}
		}
	}
}

// liveUrgentDispatchHooks defines one serial money-ready consumer. Pending is a resident-memory
// queue check. Gate must include ARM, AUTO, kill-switch and runtime safety-pause authority.
// Dispatch stays synchronous on this goroutine: a timeout cancels its context but never starts an
// overlapping venue attempt. Rearm republishes the coalesced wake whenever bounded work remains.
type liveUrgentDispatchHooks struct {
	Wake     <-chan struct{}
	Pending  func() bool
	Gate     func(time.Time) liveAutoRuntimeDecision
	Dispatch func(context.Context)
	Rearm    func()
	Disabled func()
}

func drainLiveUrgentWake(wake <-chan struct{}) {
	for wake != nil {
		select {
		case _, open := <-wake:
			if !open {
				return
			}
		default:
			return
		}
	}
}

func runLiveUrgentCandidateDispatchLoop(ctx context.Context, hooks liveUrgentDispatchHooks,
	failsafe, dispatchTimeout time.Duration) {
	if ctx == nil || hooks.Pending == nil || hooks.Gate == nil || hooks.Dispatch == nil {
		return
	}
	if failsafe <= 0 {
		failsafe = liveUrgentDispatchFailsafe
	}
	if dispatchTimeout <= 0 {
		dispatchTimeout = liveUrgentDispatchTimeout
	}
	ticker := time.NewTicker(failsafe)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case _, open := <-hooks.Wake:
			if hooks.Wake != nil && !open {
				return
			}
			// One token represents all currently pending work. Coalesce redundant tokens before
			// the serial call; Rearm below publishes exactly one more when the bounded pass leaves
			// candidates queued.
			drainLiveUrgentWake(hooks.Wake)
		case <-ticker.C:
			// A small in-memory queue poll is the only failsafe. It performs no venue, account or
			// database call and is skipped entirely while the queue is empty.
		}
		if !hooks.Pending() {
			continue
		}
		decision := hooks.Gate(time.Now())
		if !decision.Dispatch {
			// A transient safety pause preserves the fresh bounded queue for the next failsafe
			// check. Operator disarm/AUTO-off/kill is different: old candidates must not replay
			// after a later re-arm, so the integration discards them without a venue call.
			if !decision.Paused && hooks.Disabled != nil {
				hooks.Disabled()
			}
			continue
		}
		dispatchCtx, cancel := context.WithTimeout(ctx, dispatchTimeout)
		hooks.Dispatch(dispatchCtx)
		cancel()
		if hooks.Pending() && hooks.Rearm != nil {
			hooks.Rearm()
		}
	}
}

func (s *Server) liveUrgentCandidatePending() bool {
	if s == nil {
		return false
	}
	s.liveMirrorMu.Lock()
	pending := len(s.liveMirrorQ) > 0
	s.liveMirrorMu.Unlock()
	return pending
}

func (s *Server) discardLiveUrgentCandidatesWhileDisabled() {
	if s == nil {
		return
	}
	// popLiveMirrorCandidate intentionally refuses to pop while ARM/AUTO/kill is disabled, so a
	// gated pop cannot implement this transition. Advance the session and remove admitted work
	// directly under the queue lock while holding the same writer fence as final venue calls.
	s.liveWriteFence.Lock()
	cleared := s.advanceAndClearLiveMirrorCandidatesWhileWriteLocked()
	s.liveWriteFence.Unlock()
	now := time.Now()
	const reason = "live-runtime-gate-disabled-before-urgent-dispatch"
	for _, candidate := range cleared.singles {
		s.recordAdmittedLiveCandidateDrop(candidate, "AUTO-LIVE-MIRROR-DROP",
			"urgent-dispatch-gate", reason, now, map[string]any{"queue_branch": "live-mirror-single"})
	}
	for _, candidate := range cleared.combos {
		s.recordAdmittedLiveCandidateHousekeeping(candidate, "AUTO-LIVE-COMBO-CANDIDATE-DROP",
			"urgent-dispatch-gate", reason, now, map[string]any{"queue_branch": "live-mirror-combo"})
	}
	for _, intent := range cleared.intents {
		s.recordAdmittedLiveCandidateDrop(liveCandidateFromSignalIntent(intent),
			"AUTO-LIVE-SIGNAL-DROP", "urgent-dispatch-gate", reason, now,
			map[string]any{"queue_branch": "live-signal-intent"})
	}
}

func (s *Server) runLiveUrgentSignalIntake(ctx context.Context) {
	runLiveUrgentSignalIntakeLoop(ctx, liveUrgentIntakeHooks{
		Intents: s.liveSignalIntentChannel(),
		Collect: s.collectLiveSignalIntentBurst,
		Submit: func(submitCtx context.Context, batch []liveSignalIntent) {
			_ = s.submitLiveSignalIntentBatch(submitCtx, batch)
		},
	})
}

func (s *Server) runLiveUrgentCandidateDispatcher(ctx context.Context) {
	runLiveUrgentCandidateDispatchLoop(ctx, liveUrgentDispatchHooks{
		Wake:    s.liveMirrorWakeChannel(),
		Pending: s.liveUrgentCandidatePending,
		Gate:    s.refreshLiveAutoRuntimeDecision,
		Dispatch: func(dispatchCtx context.Context) {
			endCashPriority := s.beginLiveCashPriority()
			defer endCashPriority()
			// Raw intake already paid the shared arbitration window. The complete bounded mirror
			// queue is still preflighted, ranked and compatibility-checked before these serial
			// dispatches; only the duplicate sleep is removed.
			_ = s.consumeRankedLiveMirrorCandidatesReady(dispatchCtx, liveUrgentDispatchBatch)
		},
		Rearm:    func() { s.rearmLiveMirrorDispatchIfPending() },
		Disabled: s.discardLiveUrgentCandidatesWhileDisabled,
	}, liveUrgentDispatchFailsafe, liveUrgentDispatchTimeout)
}

type liveUrgentLoopRegistration struct {
	done chan struct{}
}

var (
	liveUrgentSignalIntakes sync.Map // *Server -> *liveUrgentLoopRegistration
	liveUrgentDispatchers   sync.Map // *Server -> *liveUrgentLoopRegistration
)

func startLiveUrgentLoop(registry *sync.Map, s *Server, ctx context.Context,
	name string, run func(context.Context)) bool {
	if registry == nil || s == nil || ctx == nil || ctx.Err() != nil || run == nil {
		return false
	}
	registration := &liveUrgentLoopRegistration{done: make(chan struct{})}
	if _, loaded := registry.LoadOrStore(s, registration); loaded {
		return false
	}
	go func() {
		defer close(registration.done)
		defer registry.CompareAndDelete(s, registration)
		s.runGuarded(name, func() { run(ctx) })
	}()
	return true
}

// startLiveUrgentSignalIntake is safe to call before startup reconciliation. It performs no money
// mutation and prevents the raw channel from aging while the maintenance owner restores state.
func startLiveUrgentSignalIntake(s *Server, ctx context.Context) bool {
	return startLiveUrgentLoop(&liveUrgentSignalIntakes, s, ctx,
		"live-urgent-signal-intake", s.runLiveUrgentSignalIntake)
}

// startLiveUrgentCandidateDispatcher must be called after startup pending-risk/staged recovery.
// The registry guarantees a single candidate consumer even if lifecycle wiring calls it twice.
func startLiveUrgentCandidateDispatcher(s *Server, ctx context.Context) bool {
	return startLiveUrgentLoop(&liveUrgentDispatchers, s, ctx,
		"live-urgent-candidate-dispatch", s.runLiveUrgentCandidateDispatcher)
}

func waitLiveUrgentLoop(ctx context.Context, registry *sync.Map, s *Server) error {
	if registry == nil || s == nil {
		return nil
	}
	value, ok := registry.Load(s)
	if !ok {
		return nil
	}
	registration, ok := value.(*liveUrgentLoopRegistration)
	if !ok || registration == nil || registration.done == nil {
		return nil
	}
	select {
	case <-registration.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitLiveUrgentLoops joins the two cash schedulers after their parent run context has been
// canceled. Final-snapshot code uses this receipt before fencing comparison telemetry, so a
// dequeued preflight cannot publish one last row after the immutable backup boundary.
func waitLiveUrgentLoops(ctx context.Context, s *Server) error {
	if err := waitLiveUrgentLoop(ctx, &liveUrgentSignalIntakes, s); err != nil {
		return err
	}
	return waitLiveUrgentLoop(ctx, &liveUrgentDispatchers, s)
}
