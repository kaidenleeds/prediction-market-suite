package server

import (
	"context"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

const (
	researchSystemTimeoutRetryDelay = 30 * time.Second
	// Collector liveness allows a six-minute cold-start grace. A retry backoff longer than that can
	// turn one cooperative timeout into a false NEVER_RAN alert even though the scheduler is alive.
	researchSystemTimeoutRetryMax = time.Minute
	// A system-funnel cycle contains twelve independently durable collectors. Letting that cycle
	// own every systems admission made the later book-native collectors wait for all twelve under
	// cold-start WAL/gate contention. One funnel admission may run consecutively; the next systems
	// admission must be offered to another due lane before the funnel resumes.
	researchSystemFunnelMaxBurst = 1
)

func researchSystemRetryDelay(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	delay := researchSystemTimeoutRetryDelay
	for i := 1; i < failures && delay < researchSystemTimeoutRetryMax; i++ {
		delay *= 2
		if delay > researchSystemTimeoutRetryMax {
			delay = researchSystemTimeoutRetryMax
		}
	}
	return delay
}

// Each entry is one admission to the shared heavy-research lane. Keeping the list explicit makes
// cold-start order, cooperative work deadline and round-robin coverage reviewable. A final durable
// receipt may use only the common two-second grace beyond that deadline. Latent registration is
// first and is never handed the remainder of another collector's deadline. With healthy sub-second
// jobs, one full cold rotation completes in about one minute.
type researchSystemHeavyLaneSpec struct {
	name    string
	timeout time.Duration
}

var researchSystemHeavyLaneSpecs = []researchSystemHeavyLaneSpec{
	{name: "latent-register", timeout: 15 * time.Second},
	{name: "latent-outcome", timeout: 25 * time.Second},
	// This one sweep also feeds nested-ladder, outcome-set-surface, payoff-envelope-capacity, and
	// the next outcome-expansion pass. Keep it ahead of the slow score/funnel lanes at cold start.
	{name: "event-basket", timeout: 35 * time.Second},
	{name: "latent-score", timeout: 25 * time.Second},
	{name: "system-funnels", timeout: 25 * time.Second},
	{name: "native-time-nested", timeout: 25 * time.Second},
	{name: "native-joint-marginal", timeout: 25 * time.Second},
	{name: "native-fee-rounding", timeout: 25 * time.Second},
	{name: "concrete-paired", timeout: 35 * time.Second},
	{name: "proper-score", timeout: 35 * time.Second},
	{name: "incentive-economics", timeout: 55 * time.Second},
	// Coverage consumes the collector receipts above. Keep it last so the first cold snapshot does
	// not freeze transient NEVER_RAN blockers for five minutes after those collectors finish.
	{name: "system-coverage", timeout: 20 * time.Second},
}

type researchSystemsRotationRuntime struct {
	sync.Mutex
	next                         int
	funnelBurst                  int
	initialRotationComplete      bool
	retryAt                      map[string]time.Time
	failures                     map[string]int
	completeBoardSnapshotForTest func() ([]kalshi.Market, time.Time)
}

var researchSystemsRotationRuntimes sync.Map // map[*Server]*researchSystemsRotationRuntime

func (s *Server) researchSystemsRotationRuntime() *researchSystemsRotationRuntime {
	value, _ := researchSystemsRotationRuntimes.LoadOrStore(s, &researchSystemsRotationRuntime{})
	return value.(*researchSystemsRotationRuntime)
}

func nextDueResearchSystemLane(start int, due func(string) bool) (int, bool) {
	n := len(researchSystemHeavyLaneSpecs)
	if n == 0 {
		return 0, false
	}
	start %= n
	if start < 0 {
		start += n
	}
	for offset := 0; offset < n; offset++ {
		idx := (start + offset) % n
		if due(researchSystemHeavyLaneSpecs[idx].name) {
			return idx, true
		}
	}
	return 0, false
}

// A cooperative deadline means the collector did not finish inside its ordinary cadence slot.
// Restore only that lane's due stamp so the scheduler can make one short, explicit retry. Source
// and semantic errors still retain the normal collector cadence because the legacy collectors do
// not return typed results; their durable error receipts remain the honest alert contract.
func (s *Server) resetResearchSystemHeavyLaneAfterTimeout(name string) {
	switch name {
	case "latent-outcome", "latent-score":
		schedule := s.r139LatentSchedule()
		schedule.Lock()
		if name == "latent-outcome" {
			schedule.lastOutcome = time.Time{}
		} else {
			schedule.lastScore = time.Time{}
		}
		schedule.Unlock()
	case "system-funnels", "system-coverage":
		state := s.r138CollectorState()
		state.mu.Lock()
		if name == "system-funnels" {
			state.lastSystemFunnels = time.Time{}
		} else {
			state.lastCoverage = time.Time{}
		}
		state.mu.Unlock()
	case "native-time-nested", "native-joint-marginal", "native-fee-rounding":
		state := s.nativeLockScheduler()
		state.Lock()
		switch name {
		case "native-time-nested":
			state.lastTimeNested = time.Time{}
		case "native-joint-marginal":
			state.lastJointMarginal = time.Time{}
		case "native-fee-rounding":
			state.lastFeeRounding = time.Time{}
		}
		state.Unlock()
	case "concrete-paired":
		state := s.concreteSchedule()
		state.Lock()
		state.last = time.Time{}
		state.Unlock()
	case "event-basket", "proper-score", "incentive-economics":
		s.researchMu.Lock()
		switch name {
		case "event-basket":
			s.lastBasketResearchAt = time.Time{}
		case "proper-score":
			s.lastProperScoreAt = time.Time{}
		case "incentive-economics":
			s.lastIncentiveResearchAt = time.Time{}
		}
		s.researchMu.Unlock()
	}
}

func (s *Server) researchSystemsPrimaryLaneDue(name string, now time.Time) bool {
	ready := s.kal != nil && s.kal.TickerCount() > 0
	eventBasketReady := s.researchEventBasketReady()
	s.researchMu.Lock()
	defer s.researchMu.Unlock()
	switch name {
	case "event-basket":
		return eventBasketReady && (s.lastBasketResearchAt.IsZero() || now.Sub(s.lastBasketResearchAt) >= 2*time.Minute)
	case "proper-score":
		return ready && (s.lastProperScoreAt.IsZero() || now.Sub(s.lastProperScoreAt) >= 5*time.Minute)
	case "incentive-economics":
		return ready && (s.lastIncentiveResearchAt.IsZero() || now.Sub(s.lastIncentiveResearchAt) >= 15*time.Minute)
	default:
		return false
	}
}

// Event-basket's derived collectors require one fresh, successfully published complete-board
// generation. The general kmkts map also contains targeted REST and lifecycle-patched rows, so its
// size or per-row timestamp can never prove a complete crawl. The returned immutable board is the
// exact generation the sweep must enumerate.
func (s *Server) researchEventBasketCompleteBoard(now time.Time) ([]kalshi.Market, time.Time, bool) {
	if s.kal == nil {
		return nil, time.Time{}, false
	}
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	testSnapshot := rotation.completeBoardSnapshotForTest
	rotation.Unlock()
	var markets []kalshi.Market
	var observedAt time.Time
	if testSnapshot != nil {
		markets, observedAt = testSnapshot()
	} else {
		markets, observedAt = s.kal.CompleteBoardSnapshot()
	}
	age := now.Sub(observedAt)
	if len(markets) == 0 || observedAt.IsZero() || age < 0 || age > r159KalshiMarketCacheMaxAge {
		return nil, observedAt, false
	}
	return markets, observedAt, true
}

func (s *Server) researchEventBasketReady() bool {
	if s.kal == nil {
		return true // run once to persist the explicit unavailable-client receipt
	}
	_, _, ready := s.researchEventBasketCompleteBoard(time.Now())
	return ready
}

func (s *Server) runResearchSystemsPrimaryLaneIfDue(ctx context.Context, name string, now time.Time) {
	ready := s.kal != nil && s.kal.TickerCount() > 0
	eventBasketReady := s.researchEventBasketReady()
	s.researchMu.Lock()
	due := false
	switch name {
	case "event-basket":
		due = eventBasketReady && (s.lastBasketResearchAt.IsZero() || now.Sub(s.lastBasketResearchAt) >= 2*time.Minute)
		if due {
			s.lastBasketResearchAt = now
		}
	case "proper-score":
		due = ready && (s.lastProperScoreAt.IsZero() || now.Sub(s.lastProperScoreAt) >= 5*time.Minute)
		if due {
			s.lastProperScoreAt = now
		}
	case "incentive-economics":
		due = ready && (s.lastIncentiveResearchAt.IsZero() || now.Sub(s.lastIncentiveResearchAt) >= 15*time.Minute)
		if due {
			s.lastIncentiveResearchAt = now
		}
	}
	s.researchMu.Unlock()
	if !due {
		return
	}
	switch name {
	case "event-basket":
		s.sweepEventBasketLock(ctx)
	case "proper-score":
		s.sweepProperScore(ctx)
	case "incentive-economics":
		s.sweepR138IncentiveEconomics(ctx)
	}
}

// researchSystemHeavyLaneDue is read-only. Every mutable due stamp remains in the corresponding
// run function, which is called only after the shared heavy gate admits this lane.
func (s *Server) researchSystemHeavyLaneDue(name string, now time.Time) bool {
	switch name {
	case "latent-register":
		return s.r139LatentLaneDue("register", now)
	case "latent-outcome":
		return s.r139LatentLaneDue("outcome", now)
	case "latent-score":
		return s.r139LatentLaneDue("score", now)
	case "system-funnels":
		state := s.r138CollectorState()
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.systemFunnelCycleActive || state.lastSystemFunnels.IsZero() || now.Sub(state.lastSystemFunnels) >= 5*time.Minute
	case "native-time-nested":
		return s.nativeLockLaneDue("time-nested", now)
	case "native-joint-marginal":
		return s.nativeLockLaneDue("joint-marginal", now)
	case "native-fee-rounding":
		return s.nativeLockLaneDue("fee-rounding", now)
	case "concrete-paired":
		state := s.concreteSchedule()
		state.Lock()
		defer state.Unlock()
		return state.last.IsZero() || now.Sub(state.last) >= 5*time.Minute
	case "system-coverage":
		state := s.r138CollectorState()
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.lastCoverage.IsZero() || now.Sub(state.lastCoverage) >= 5*time.Minute
	case "event-basket", "proper-score", "incentive-economics":
		return s.researchSystemsPrimaryLaneDue(name, now)
	default:
		return false
	}
}

func (s *Server) runResearchSystemHeavyLane(ctx context.Context, name string, now time.Time) {
	switch name {
	case "latent-register":
		s.sweepR139LatentLaneIfDue(ctx, "register", now)
	case "latent-outcome":
		s.sweepR139LatentLaneIfDue(ctx, "outcome", now)
	case "latent-score":
		s.sweepR139LatentLaneIfDue(ctx, "score", now)
	case "system-funnels":
		s.sweepR139SystemSpecificFunnels(ctx)
	case "native-time-nested":
		s.sweepNativeLockLaneIfDue(ctx, "time-nested", now)
	case "native-joint-marginal":
		s.sweepNativeLockLaneIfDue(ctx, "joint-marginal", now)
	case "native-fee-rounding":
		s.sweepNativeLockLaneIfDue(ctx, "fee-rounding", now)
	case "concrete-paired":
		s.sweepConcreteResearchSystems(ctx, now)
	case "system-coverage":
		s.sweepR138SystemCoverageIfDue(ctx, now)
	case "event-basket", "proper-score", "incentive-economics":
		s.runResearchSystemsPrimaryLaneIfDue(ctx, name, now)
	}
}

// runNextResearchSystemHeavy admits at most one collector lane per five-second systems tick. The
// cursor advances only after admission; a busy shared gate therefore defers work without consuming
// its due clock. All systems lanes use the common gate name so global gate fairness still yields to
// evidence, replay, portfolio and inference between systems admissions.
func (s *Server) runNextResearchSystemHeavy(ctx context.Context, now time.Time) bool {
	if now.IsZero() {
		now = time.Now()
	}
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	start := rotation.next
	funnelBurst := rotation.funnelBurst
	initialRotationComplete := rotation.initialRotationComplete
	retries := make(map[string]time.Time, len(rotation.retryAt))
	for lane, at := range rotation.retryAt {
		retries[lane] = at
	}
	rotation.Unlock()
	idx, ok := 0, false
	// Keep an active system-funnel cycle moving, but never let its twelve independent specs starve
	// the other systems lanes. After one funnel admission, the fair cursor must admit another due
	// lane once; if no peer is due, the funnel may continue immediately.
	collectorState := s.r138CollectorState()
	collectorState.mu.Lock()
	funnelCycleActive := collectorState.systemFunnelCycleActive
	collectorState.mu.Unlock()
	yieldFunnel := funnelCycleActive && funnelBurst >= researchSystemFunnelMaxBurst
	// During the first boot rotation, every other due lane gets one bounded admission before the
	// twelve-part funnel may repeat. Without this guard, the active funnel's special priority could
	// revisit its early children while later collectors remained NEVER_RAN.
	if initialRotationComplete && funnelCycleActive && !yieldFunnel {
		for candidateIdx, candidate := range researchSystemHeavyLaneSpecs {
			if candidate.name != "system-funnels" {
				continue
			}
			if retryAt, retrying := retries[candidate.name]; retrying && now.Before(retryAt) {
				break
			}
			if s.researchSystemHeavyLaneDue(candidate.name, now) {
				idx, ok = candidateIdx, true
			}
			break
		}
	}
	// An expired retry becomes normally eligible at its fair cursor position; it never jumps ahead
	// of ordinary due peers. The old retry-first pass let one WAL-canceled score lane reclaim every
	// turn after each 30-second backoff even though the cursor had advanced.
	for name, at := range retries {
		if now.Before(at) || s.researchSystemHeavyLaneDue(name, now) {
			continue
		}
		rotation.Lock()
		if current, exists := rotation.retryAt[name]; exists && current.Equal(at) {
			delete(rotation.retryAt, name)
		}
		rotation.Unlock()
		delete(retries, name)
	}
	if !ok {
		idx, ok = nextDueResearchSystemLane(start, func(name string) bool {
			if yieldFunnel && name == "system-funnels" {
				return false
			}
			if retryAt, retrying := retries[name]; retrying && now.Before(retryAt) {
				return false
			}
			return s.researchSystemHeavyLaneDue(name, now)
		})
	}
	// Do not idle when the funnel is the only due lane. The burst bound is a fairness yield, not a
	// cadence throttle.
	if !ok && yieldFunnel {
		idx, ok = nextDueResearchSystemLane(start, func(name string) bool {
			if retryAt, retrying := retries[name]; retrying && now.Before(retryAt) {
				return false
			}
			return s.researchSystemHeavyLaneDue(name, now)
		})
	}
	if !ok {
		// A restarted suite can restore recent receipts for every lane. In that state there is no
		// first-boot systems work to protect, so do not hold broad settlement/catalog maintenance
		// behind the eight-minute fallback merely because the cursor never admitted a lane.
		if !initialRotationComplete && !funnelCycleActive {
			rotation.Lock()
			rotation.initialRotationComplete = true
			rotation.Unlock()
		}
		return false
	}
	spec := researchSystemHeavyLaneSpecs[idx]
	timedOut, laneStarted, laneCompleted := false, false, false
	admitted := s.tryRunHeavyResearch(ctx, "systems", 0, func(gateCtx context.Context) {
		laneStarted = true
		timedOut = runCooperativelyBoundedResearchLane(gateCtx, spec.timeout, func(laneCtx context.Context) {
			s.runResearchSystemHeavyLane(laneCtx, spec.name, time.Now())
		})
		laneCompleted = !timedOut && gateCtx.Err() == nil
		if timedOut && s.log != nil {
			s.log.Warn("research systems lane deadline", "lane", spec.name, "limit", spec.timeout)
		}
	})
	if !admitted {
		if !laneStarted || laneCompleted {
			// A busy shared gate did not consume this lane. Keep the cursor and due clock unchanged
			// so the same work remains eligible at the next systems tick.
			return false
		}
		// Admission can begin while the WAL is safe and then be cooperatively canceled when the WAL
		// crosses its guard (or LIVE arms). Restore this lane's due clock, but treat the interrupted
		// admission like a bounded failure below: move the fair cursor forward and back this lane
		// off. Leaving the cursor pinned here made one write-heavy collector retry forever while
		// every later system remained NEVER_RAN.
		timedOut = true
	}
	if timedOut {
		s.resetResearchSystemHeavyLaneAfterTimeout(spec.name)
	}
	rotation.Lock()
	next := (idx + 1) % len(researchSystemHeavyLaneSpecs)
	rotation.next = next
	if !rotation.initialRotationComplete && (next == 0 || idx < start) {
		rotation.initialRotationComplete = true
	}
	if spec.name == "system-funnels" {
		if funnelCycleActive {
			rotation.funnelBurst++
		} else {
			rotation.funnelBurst = 1
		}
	} else {
		rotation.funnelBurst = 0
	}
	if rotation.retryAt == nil {
		rotation.retryAt = make(map[string]time.Time)
	}
	if rotation.failures == nil {
		rotation.failures = make(map[string]int)
	}
	if timedOut {
		rotation.failures[spec.name]++
		rotation.retryAt[spec.name] = time.Now().Add(researchSystemRetryDelay(rotation.failures[spec.name]))
	} else {
		delete(rotation.retryAt, spec.name)
		delete(rotation.failures, spec.name)
	}
	rotation.Unlock()
	return admitted
}
