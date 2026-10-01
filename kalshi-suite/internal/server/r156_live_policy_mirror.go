package server

// R156 independent LIVE-policy mirror.
//
// This is not the older post-reservation dispatch shadow. It owns a separate fake $400 portfolio
// and keeps running while real LIVE is disarmed. It consumes only the exact Kalshi System
// allowlist, reuses the real preflight, then simulates a delayed IOC from fresh full WS books.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	livePolicyMirrorModelVersion = "live-policy-mirror-r164-execution-before-portfolio-ioc-v5"
	// Actual LIVE-passed candidates have their own small admission lane. Rejection and disarmed
	// research traffic can be much burstier, so it has a larger but still hard-bounded queue.
	// The two lanes never share execution slots.
	livePolicyMirrorPriorityQueueCapacity   = 512
	livePolicyMirrorBackgroundQueueCapacity = 32768
	livePolicyMirrorWaiterCapacity          = 512
	livePolicyMirrorWorkTimeout             = 35 * time.Second
	livePolicyMirrorWireDefault             = 100 * time.Millisecond
	livePolicyMirrorBookMaxAge              = time.Second
	// The optional $400 fake portfolio writes the shared Paper database and is serial. Execution
	// mechanics have their own four-slot, cached-book-only semaphore below.
	livePolicyMirrorPriorityWorkerCount   = 1
	livePolicyMirrorExecutionWorkerCount  = 4
	livePolicyMirrorBackgroundWorkerCount = 2
	livePolicyMirrorStartLateMax          = 500 * time.Millisecond

	livePolicyMirrorModeledFill     = "modeled_fill"
	livePolicyMirrorModeledZeroFill = "modeled_zero_fill"
	livePolicyMirrorNotObserved     = "not_observed"
)

type livePolicyMirrorExecutionMeta struct {
	ScheduledAt     time.Time
	LiveTerminalAt  time.Time
	LiveState       string
	LiveLimit       float64
	VenueAttempted  bool
	LiveReceiptSeen bool
}

type livePolicyMirrorWork struct {
	Signal    storage.Signal
	Point     float64
	SignalAt  time.Time
	DueAt     time.Time
	Candidate liveMirrorCandidate
	Execution livePolicyMirrorExecutionMeta
	// ExperimentKind is an explicitly money-free execution-shadow lane. It never enters the
	// optional $400 portfolio, cash preflight, LIVE/canary selection, or venue POST path.
	ExperimentKind string
	// ExecutionOnly is set by crash/restart recovery. Historical gaps can be honestly closed from
	// the isolated ledger, but must never create a late fake-portfolio trade.
	ExecutionOnly bool
	Reason        string
	Preflighted   bool
	testRun       func(context.Context) // hermetic bounded-concurrency portfolio regression seam
	// testExecutionRun proves that the delayed execution-only receipt runs before the fake
	// portfolio waits for a cash-quiet turn. Production leaves this nil.
	testExecutionRun func(context.Context)
}

type livePolicyMirrorPriorityContextKey struct{}

func withLivePolicyMirrorPriority(ctx context.Context) context.Context {
	return context.WithValue(ctx, livePolicyMirrorPriorityContextKey{}, true)
}

func isLivePolicyMirrorPriority(ctx context.Context) bool {
	priority, _ := ctx.Value(livePolicyMirrorPriorityContextKey{}).(bool)
	return priority
}

// Only an actual LIVE-passed candidate is latency-critical. Rejections remain durable comparison
// evidence and disarmed signals still run the complete isolated preflight, but neither may occupy
// a scheduled-touch slot needed by a candidate that already passed the real money branch.
func livePolicyMirrorPriorityWork(work livePolicyMirrorWork) bool {
	return work.ExperimentKind == "" && work.Reason == "" && work.Preflighted
}

func livePolicyMirrorIntentKey(c liveMirrorCandidate) string {
	sum := sha256.Sum256([]byte(c.key()))
	return "lpm:" + hex.EncodeToString(sum[:16])
}

func livePolicyMirrorCandidateID(c liveMirrorCandidate, at time.Time) string {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", c.key(), at.UnixNano())))
	return fmt.Sprintf("lpm-%d-%s", at.UnixNano(), hex.EncodeToString(sum[:6]))
}

func (s *Server) livePolicyMirrorExecutionDelay() time.Duration {
	if s.livePolicyMirrorDelay < 0 {
		return 0
	}
	if s.livePolicyMirrorDelay > 0 {
		return s.livePolicyMirrorDelay
	}
	return s.genfollowPaperTakerDelay()
}

func (s *Server) livePolicyMirrorFinalWireDelay() time.Duration {
	if s.livePolicyMirrorWireDelay < 0 {
		return 0
	}
	if s.livePolicyMirrorWireDelay > 0 {
		return s.livePolicyMirrorWireDelay
	}
	return livePolicyMirrorWireDefault
}

func (s *Server) livePolicyMirrorWorkDueAt(work livePolicyMirrorWork) time.Time {
	base := work.SignalAt
	if base.IsZero() {
		base = work.Candidate.At
	}
	if base.IsZero() {
		base = time.Now()
	}
	if work.Reason != "" {
		return base
	}
	if work.ExperimentKind != "" {
		return base
	}
	// A priority comparison exists only after the cash preflight has accepted the exact signal.
	// Give cash the same fixed head start used by funded Paper, then apply the configured delayed
	// execution touch. Starting the fake clock at the raw detector timestamp made normal 1-2s
	// cash preflight work look hundreds of milliseconds late and erased otherwise joinable
	// counterfactual receipts.
	if work.Preflighted {
		base = base.Add(genfollowPaperLiveFirstGrace)
	}
	return base.Add(s.livePolicyMirrorExecutionDelay())
}

func (s *Server) enqueueLivePolicyMirrorWork(work livePolicyMirrorWork) bool {
	if s == nil || s.store == nil {
		return false
	}
	s.livePolicyMirrorMu.Lock()
	defer s.livePolicyMirrorMu.Unlock()
	if s.livePolicyMirrorStopping {
		return false
	}
	if work.DueAt.IsZero() {
		work.DueAt = s.livePolicyMirrorWorkDueAt(work)
	}
	if s.livePolicyMirrorCh == nil {
		backgroundCapacity := s.livePolicyMirrorCapacity
		if backgroundCapacity <= 0 {
			backgroundCapacity = livePolicyMirrorBackgroundQueueCapacity
		}
		priorityCapacity := s.livePolicyMirrorPriorityCapacity
		if priorityCapacity <= 0 {
			priorityCapacity = livePolicyMirrorPriorityQueueCapacity
		}
		s.livePolicyMirrorCh = make(chan livePolicyMirrorWork, backgroundCapacity)
		s.livePolicyMirrorPriorityCh = make(chan livePolicyMirrorWork, priorityCapacity)
		s.livePolicyMirrorDone = make(chan struct{})
		workerCtx, cancel := context.WithCancel(context.Background())
		s.livePolicyMirrorCancel = cancel
		priority, background, done := s.livePolicyMirrorPriorityCh,
			s.livePolicyMirrorCh, s.livePolicyMirrorDone
		go s.runGuarded("live-policy-mirror", func() {
			s.runLivePolicyMirrorWorkers(workerCtx, priority, background, done)
		})
	}
	target := s.livePolicyMirrorCh
	priority := livePolicyMirrorPriorityWork(work)
	outstanding := &s.livePolicyMirrorBackgroundOutstanding
	if priority && s.livePolicyMirrorPriorityCh != nil {
		target = s.livePolicyMirrorPriorityCh
		outstanding = &s.livePolicyMirrorPriorityOutstanding
	}
	// Reserve the work gauge before the nonblocking send. A fast consumer may finish immediately
	// after the send, so incrementing afterward would briefly hide that job. A full queue rolls the
	// reservation back under livePolicyMirrorMu before API readers can observe it.
	outstanding.Add(1)
	select {
	case target <- work:
		return true
	default:
		outstanding.Add(-1)
		s.livePolicyMirrorDropped.Add(1)
		if priority {
			s.livePolicyMirrorPriorityDropped.Add(1)
		}
		return false
	}
}

// queueLivePolicyMirrorSignal owns only the disarmed lane. When actual LIVE is on, the normal
// preflight publishes its accepted/rejected output into this mirror, avoiding duplicate REST/proof
// work and guaranteeing identical candidate admission.
func (s *Server) queueLivePolicyMirrorSignal(sig storage.Signal, measuredPoint float64) bool {
	now := time.Now()
	shadowAttemptID := s.executionShadowAttemptForSignal(sig)
	if shadowAttemptID == "" {
		shadowAttemptID = s.executionShadowBeginSignal(sig, measuredPoint, now)
	}
	accepted := s.queueLivePolicyMirrorSignalPrepared(sig, measuredPoint, now, shadowAttemptID)
	s.wakeExecutionShadowWriter()
	return accepted
}

func (s *Server) queueLivePolicyMirrorSignalPrepared(sig storage.Signal, measuredPoint float64,
	signalAt time.Time, shadowAttemptID string) bool {
	if s == nil || s.store == nil || (s.liveOperatorAutoOn() && s.liveMirrorEnabled()) {
		return false
	}
	c := liveMirrorCandidate{Platform: strings.ToLower(strings.TrimSpace(sig.Platform)),
		Ticker: strings.TrimSpace(sig.Ticker), Title: sig.Title,
		Side: strings.ToUpper(strings.TrimSpace(sig.Side)), Family: strings.TrimSpace(sig.SignalType),
		Source: "auto-cons-" + strings.TrimSpace(sig.SignalType), Price: sig.EntryPrice,
		At: signalAt, InputTopology: r147SignalInputTopology(sig),
		InputObservedAt: r147SignalInputObservedAt(sig), ShadowAttemptID: shadowAttemptID}
	if bound, why, declared := bindStaticTakerSignalCandidate(c, sig); declared {
		if why != "" {
			return false
		}
		c = bound
	}
	// This experiment is deliberately the current Kalshi singles canary, not "all positive Paper".
	if c.Platform != "kalshi" || measuredPoint <= 0 || s.liveSystemSelectionReason(c, "taker") != "" {
		return false
	}
	now := time.Now()
	key := c.key()
	s.livePolicyMirrorMu.Lock()
	if s.livePolicyMirrorSeen == nil {
		s.livePolicyMirrorSeen = make(map[string]time.Time)
	}
	if seen, ok := s.livePolicyMirrorSeen[key]; ok && now.Sub(seen) >= 0 &&
		now.Sub(seen) <= liveSignalIntentBurstDedup {
		s.livePolicyMirrorMu.Unlock()
		return false
	}
	s.livePolicyMirrorSeen[key] = now
	for oldKey, seen := range s.livePolicyMirrorSeen {
		if now.Sub(seen) > liveMirrorClaimTTL {
			delete(s.livePolicyMirrorSeen, oldKey)
		}
	}
	s.livePolicyMirrorMu.Unlock()
	return s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		Signal: sig, Point: measuredPoint, SignalAt: signalAt, Candidate: c,
	})
}

func (s *Server) enqueueLivePolicyMirrorCandidateAfterTerminal(c liveMirrorCandidate,
	q liveMirrorQuote, terminalAt time.Time, state string, venueAttempted bool) bool {
	// Attempt-time preflight already froze the selected authority. A later allowlist edit may stop
	// new cash attempts, but it must not erase the comparison terminal for one already completed.
	if !strings.EqualFold(c.Platform, "kalshi") {
		return false
	}
	return s.enqueueLivePolicyMirrorTerminalWork(c, q, terminalAt, state, venueAttempted)
}

// executionShadowLiveTerminalComparable accepts only outcomes that cannot later change whether
// the cash attempt filled. Full fills are usable before exact fee reconciliation because this
// comparison models execution mechanics, not LIVE P&L. Ambiguous/pending receipts deliberately
// remain gaps until a later exact venue reconciliation is appended.
func executionShadowLiveTerminalComparable(event storage.ExecutionShadowEvent) bool {
	state := strings.ToLower(strings.TrimSpace(event.LiveState))
	if state == "" || state == "ambiguous" || state == "pending" || state == "unknown" {
		return false
	}
	filled := 0.0
	if event.LiveFilledQty != nil {
		filled = *event.LiveFilledQty
	}
	switch {
	case !event.VenueAttempted &&
		(state == "not-sent" || state == "rejected" || state == "rejected-before-venue"):
		return true
	case event.VenueAttempted && filled > 0 && (state == "full" || state == "filled"):
		return true
	case event.VenueAttempted && filled > 0 && state == "partial":
		tif, _ := event.Evidence["time_in_force"].(string)
		return liveKalshiImmediateTimeInForce(strings.TrimSpace(tif))
	case event.VenueAttempted && state == "rejected":
		// A clean venue rejection is final even when the handler marks fee/fill economics
		// non-authoritative. Transport-ambiguous errors use the distinct "ambiguous" state above.
		return true
	case event.VenueAttempted && state == "clean_rejected":
		// The restart-safe pending-risk reconciler uses this explicit spelling for the same
		// final clean venue refusal.
		return true
	case event.VenueAttempted && event.LiveAuthoritative &&
		(state == "unfilled" || state == "terminal_unfilled" ||
			state == "canceled" || state == "cancelled" || state == "expired"):
		return true
	default:
		return false
	}
}

func (s *Server) enqueueLivePolicyMirrorTerminalWork(c liveMirrorCandidate,
	q liveMirrorQuote, terminalAt time.Time, state string, venueAttempted bool) bool {
	attemptID := strings.TrimSpace(c.ShadowAttemptID)
	if attemptID == "" || !s.markLivePolicyMirrorExecutionScheduled(attemptID) {
		return false
	}
	work := livePolicyMirrorWork{
		Candidate: c, SignalAt: c.At, Preflighted: true,
		Execution: livePolicyMirrorExecutionMeta{
			LiveTerminalAt: terminalAt, LiveState: strings.TrimSpace(state),
			LiveLimit: q.Price, VenueAttempted: venueAttempted, LiveReceiptSeen: true,
		},
	}
	work.DueAt = s.livePolicyMirrorWorkDueAt(work)
	work.Execution.ScheduledAt = work.DueAt
	if s.enqueueLivePolicyMirrorWork(work) {
		return true
	}
	s.forgetLivePolicyMirrorExecutionScheduled(attemptID)
	return false
}

// recoverLivePolicyMirrorExecutionGaps turns the isolated execution-shadow database into a durable
// spool. Normal work is scheduled directly after the LIVE terminal; this bounded sweep covers a
// full queue, crash, or restart. Old work is honestly terminaled as not observed rather than
// pretending a current book was available at its historical clock.
func (s *Server) recoverLivePolicyMirrorExecutionGaps(ctx context.Context) {
	if s == nil || s.store == nil || ctx.Err() != nil {
		return
	}
	rows, err := s.store.ListExecutionShadowCounterfactualExecutionPending(ctx, 64)
	if err != nil {
		if s.log != nil && s.gfRouteAuditDue(
			"r164-execution-comparison-gap-recovery", err.Error(), time.Now()) {
			s.log.Warn("execution comparison recovery delayed; durable gaps retained",
				"err", err)
		}
		return
	}
	for _, row := range rows {
		s.executionShadowRememberTrigger(row.Attempt.AttemptID, row.Attempt.TriggerUnixMS)
		c, _, meta, ok := livePolicyMirrorExecutionGap(row)
		if !ok || !s.markLivePolicyMirrorExecutionScheduled(row.Attempt.AttemptID) {
			continue
		}
		work := livePolicyMirrorWork{
			Candidate: c, SignalAt: c.At, Preflighted: true, Execution: meta,
			ExecutionOnly: true,
		}
		work.DueAt = s.livePolicyMirrorWorkDueAt(work)
		work.Execution.ScheduledAt = work.DueAt
		if !s.enqueueLivePolicyMirrorWork(work) {
			s.forgetLivePolicyMirrorExecutionScheduled(row.Attempt.AttemptID)
		}
	}
}

func livePolicyMirrorExecutionGap(row storage.ExecutionShadowAttemptView) (
	liveMirrorCandidate, liveMirrorQuote, livePolicyMirrorExecutionMeta, bool) {
	attempt := row.Attempt
	var terminal storage.ExecutionShadowEvent
	found := false
	for _, event := range row.Events {
		// Joined events are in append order. Keep the newest comparable LIVE terminal, skipping
		// pending/ambiguous receipts rather than permanently modeling them as a zero-fill.
		if executionShadowLiveTerminalComparable(event) {
			terminal, found = event, true
		}
	}
	if !found || strings.TrimSpace(attempt.AttemptID) == "" {
		return liveMirrorCandidate{}, liveMirrorQuote{}, livePolicyMirrorExecutionMeta{}, false
	}
	triggerAt := attempt.ObservedAt
	if attempt.TriggerUnixMS > 0 {
		triggerAt = time.UnixMilli(attempt.TriggerUnixMS).UTC()
	}
	c := liveMirrorCandidate{
		Platform: attempt.Venue, Ticker: attempt.Ticker, Title: attempt.Title,
		Side: attempt.Side, Action: attempt.Action, Family: attempt.SystemID,
		Source: "auto-cons-" + attempt.SystemID, At: triggerAt,
		ShadowAttemptID: attempt.AttemptID,
	}
	q := liveMirrorQuote{BookSource: terminal.BookSource, ObservedAt: terminal.BookReceivedAt}
	if terminal.OriginalLimit != nil {
		q.Price = *terminal.OriginalLimit
	}
	if terminal.VisibleDepth != nil {
		q.Depth = *terminal.VisibleDepth
	}
	if terminal.TickSize != nil {
		q.Tick = *terminal.TickSize
	}
	if terminal.RequestedQty != nil {
		c.ProspectiveQty = *terminal.RequestedQty
	}
	// The exact LIVE terminal carries the tick that already passed the money-lane lifecycle
	// and market-rule checks. Recovery must preserve it because the execution-only comparison
	// intentionally does not re-select the market from the later, volatile catalog cache.
	c.ArbitrationQuote = q
	if tif, _ := terminal.Evidence["time_in_force"].(string); strings.TrimSpace(tif) != "" {
		c.ProspectiveTimeInForce = strings.TrimSpace(tif)
	}
	for _, event := range row.Events {
		if event.Stage != "live-first-preflight" {
			continue
		}
		c.ArbitrationAt = event.At
		if c.ProspectiveQty <= 0 && event.RequestedQty != nil {
			c.ProspectiveQty = *event.RequestedQty
		}
		if c.ProspectiveTimeInForce == "" {
			if tif, _ := event.Evidence["time_in_force"].(string); strings.TrimSpace(tif) != "" {
				c.ProspectiveTimeInForce = strings.TrimSpace(tif)
			}
		}
	}
	meta := livePolicyMirrorExecutionMeta{
		LiveTerminalAt: terminal.At, LiveState: terminal.LiveState,
		LiveLimit: q.Price, VenueAttempted: terminal.VenueAttempted, LiveReceiptSeen: true,
	}
	return c, q, meta, true
}

func (s *Server) markLivePolicyMirrorExecutionScheduled(attemptID string) bool {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return false
	}
	// An execution terminal uses one stable event ID. Keep this claim for the process lifetime:
	// the retained writer already retries a delayed database, and a second recovery result with a
	// later timestamp would be different evidence under the same stable ID.
	_, alreadyScheduled := s.livePolicyMirrorExecutionClaims.LoadOrStore(attemptID, struct{}{})
	return !alreadyScheduled
}

func (s *Server) forgetLivePolicyMirrorExecutionScheduled(attemptID string) {
	s.livePolicyMirrorExecutionClaims.Delete(strings.TrimSpace(attemptID))
}

func (s *Server) enqueueLivePolicyMirrorRejection(sig storage.Signal, reason string,
	signalAt time.Time) bool {
	return s.enqueueLivePolicyMirrorRejectionPrepared(sig, reason, signalAt,
		s.executionShadowAttemptForSignal(sig))
}

func (s *Server) enqueueLivePolicyMirrorRejectionPrepared(sig storage.Signal, reason string,
	signalAt time.Time, shadowAttemptID string) bool {
	// During actual LIVE, the isolated execution-shadow already owns the exact preflight rejection
	// and attempt join. Writing the same high-rate negative observation into the fake portfolio
	// database added no P&L information and could outgrow its workers, delaying API reads and the
	// much rarer executable comparison. Disarmed research still retains this background lane.
	if s.liveOperatorAutoOn() {
		return false
	}
	c := liveCandidateFromSignalIntent(liveSignalIntent{Signal: sig, At: signalAt,
		ShadowAttemptID: shadowAttemptID})
	if !strings.EqualFold(c.Platform, "kalshi") ||
		s.liveSystemSelectionReason(c, "taker") != "" {
		return false
	}
	return s.enqueueLivePolicyMirrorWork(livePolicyMirrorWork{
		Signal: sig, SignalAt: signalAt, Candidate: c, Reason: reason, Preflighted: true,
	})
}

// runLivePolicyMirrorWorkers gives actual LIVE-passed candidates their own scheduler and slots.
// The background lane can hold thousands of durable rejection/preflight jobs without delaying the
// scheduled 750ms touch of a candidate from the real branch.
func (s *Server) runLivePolicyMirrorWorkers(ctx context.Context,
	priority, background <-chan livePolicyMirrorWork, done chan<- struct{}) {
	defer close(done)
	var lanes sync.WaitGroup
	lanes.Add(2)
	go func() {
		defer lanes.Done()
		s.runLivePolicyMirrorLane(ctx, priority, livePolicyMirrorPriorityWorkerCount,
			&s.livePolicyMirrorPriorityWaiting, &s.livePolicyMirrorPriorityActive,
			&s.livePolicyMirrorPriorityOutstanding)
	}()
	go func() {
		defer lanes.Done()
		s.runLivePolicyMirrorLane(ctx, background, livePolicyMirrorBackgroundWorkerCount,
			&s.livePolicyMirrorBackgroundWaiting, &s.livePolicyMirrorBackgroundActive,
			&s.livePolicyMirrorBackgroundOutstanding)
	}()
	lanes.Wait()
}

// runLivePolicyMirrorWorker is retained as a single-lane test helper. Production uses
// runLivePolicyMirrorWorkers so background traffic cannot consume candidate execution slots.
func (s *Server) runLivePolicyMirrorWorker(ctx context.Context, in <-chan livePolicyMirrorWork,
	done chan<- struct{}) {
	defer close(done)
	s.runLivePolicyMirrorLane(ctx, in, livePolicyMirrorPriorityWorkerCount, nil, nil, nil)
}

func (s *Server) runLivePolicyMirrorLane(ctx context.Context, in <-chan livePolicyMirrorWork,
	workerCount int, waiting, active, outstanding *atomic.Int64) {
	if workerCount < 1 {
		workerCount = 1
	}
	var workers sync.WaitGroup
	slots := make(chan struct{}, workerCount)
	executionSlots := make(chan struct{}, livePolicyMirrorExecutionWorkerCount)
	pending := make(chan struct{}, livePolicyMirrorWaiterCapacity)
	defer workers.Wait()
dispatch:
	for {
		select {
		case <-ctx.Done():
			break dispatch
		case work, ok := <-in:
			if !ok {
				break dispatch
			}
			if waiting != nil {
				waiting.Add(1)
			}
			// Timer waiters are bounded separately from executing work. Acquiring an execution
			// slot here would let four later-due signals occupy every slot while an earlier-due
			// signal sits unread in the queue.
			select {
			case pending <- struct{}{}:
			case <-ctx.Done():
				if waiting != nil {
					waiting.Add(-1)
				}
				if outstanding != nil {
					outstanding.Add(-1)
				}
				break dispatch
			}
			workers.Add(1)
			go func(work livePolicyMirrorWork) {
				defer workers.Done()
				if outstanding != nil {
					defer outstanding.Add(-1)
				}
				defer func() { <-pending }()
				waitingOwned := waiting != nil
				defer func() {
					if waitingOwned {
						waiting.Add(-1)
					}
				}()
				if !waitLivePolicyMirrorUntil(ctx, work.DueAt) {
					return
				}
				priority := livePolicyMirrorPriorityWork(work)
				workCtx, cancel := context.WithTimeout(ctx, livePolicyMirrorWorkTimeout)
				defer cancel()
				if work.ExperimentKind == inverseKflowShadowExperimentKind {
					select {
					case executionSlots <- struct{}{}:
					case <-ctx.Done():
						return
					}
					s.runGuarded("inverse-kalshi-flow-shadow", func() {
						s.runInverseKflowCanonicalFanout(workCtx, work)
					})
					<-executionSlots
					return
				}
				if work.ExperimentKind == canonicalSystemShadowExperimentKind {
					select {
					case executionSlots <- struct{}{}:
					case <-ctx.Done():
						return
					}
					s.runGuarded("canonical-system-execution-shadow", func() {
						s.simulateCanonicalSystemExecutionShadow(workCtx, work)
					})
					<-executionSlots
					return
				}
				// A LIVE-passed candidate records a delayed two-touch IOC/FOK result from cached
				// full books only after the exact cash attempt has terminaled and the shared cash
				// lane is quiet. A dedicated semaphore bounds the comparison reads without holding
				// a fake-portfolio slot. No balance, position, identity, history, REST, or
				// main-ledger dependency can erase this execution receipt.
				if priority {
					select {
					case executionSlots <- struct{}{}:
					case <-ctx.Done():
						return
					}
					executionCompleted := false
					s.runGuarded("live-policy-mirror-execution", func() {
						switch {
						case !work.Execution.LiveReceiptSeen:
							if work.testExecutionRun == nil && work.testRun == nil {
								s.recordLivePolicyMirrorExecutionMiss(work.Candidate,
									work.Execution, "exact-live-terminal-not-observed")
							}
						case !s.waitForLivePolicyMirrorExecutionTurn(workCtx, work.DueAt):
							if work.testExecutionRun == nil && work.testRun == nil {
								s.recordLivePolicyMirrorExecutionMiss(work.Candidate,
									work.Execution, "cash-priority-or-schedule-window-unavailable")
							}
						case time.Since(work.DueAt) > livePolicyMirrorStartLateMax:
							if work.testExecutionRun == nil && work.testRun == nil {
								s.recordLivePolicyMirrorExecutionMiss(work.Candidate,
									work.Execution, "counterfactual-execution-started-over-500ms-late")
							}
						case work.testExecutionRun != nil:
							work.testExecutionRun(workCtx)
						case work.testRun == nil:
							s.simulateLivePolicyMirrorExecutionOnly(workCtx, work.Candidate,
								work.Execution)
						}
						executionCompleted = true
					})
					<-executionSlots
					if !executionCompleted && work.testExecutionRun == nil && work.testRun == nil {
						s.recordLivePolicyMirrorExecutionMiss(work.Candidate, work.Execution,
							"counterfactual-execution-panicked")
					}
					if work.ExecutionOnly {
						return
					}
				}
				if priority {
					// Optional fake-portfolio admission must never queue behind its single ledger
					// slot after the higher-value execution receipt is complete. If the slot is
					// busy, preserve an honest isolated not-observed receipt and release this
					// scheduler reservation immediately.
					select {
					case slots <- struct{}{}:
					default:
						if work.testRun == nil {
							s.recordSkippedLivePolicyMirrorPortfolio(work,
								"optional-portfolio-worker-busy")
						}
						return
					}
				} else {
					select {
					case slots <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
				defer func() { <-slots }()
				if waitingOwned {
					waiting.Add(-1)
					waitingOwned = false
				}
				if active != nil {
					active.Add(1)
					defer active.Add(-1)
				}
				s.runGuarded("live-policy-mirror-task", func() {
					if priority {
						workCtx = withLivePolicyMirrorPriority(workCtx)
						if !s.waitForLivePolicyMirrorPriorityTurn(workCtx, work.DueAt) {
							reason := "mirror-scheduled-touch-started-over-500ms-late"
							if workCtx.Err() != nil {
								reason = "mirror-priority-canceled-before-scheduled-touch"
							}
							s.recordMissedLivePolicyMirrorWork(workCtx, work, reason)
							return
						}
					}
					// The priority-only wait can consume the whole lateness budget. Recheck at the
					// exact simulation boundary instead of relying on the earlier scheduler time.
					if work.Reason == "" &&
						time.Since(work.DueAt) > livePolicyMirrorStartLateMax {
						s.recordLateLivePolicyMirrorWork(workCtx, work)
						return
					}
					if work.testRun != nil {
						work.testRun(workCtx)
						return
					}
					switch {
					case work.Reason != "":
						s.recordLivePolicyMirrorRejection(workCtx, work.Signal, work.Reason,
							work.SignalAt, work.Candidate.ShadowAttemptID)
					case work.Preflighted:
						s.simulateLivePolicyMirror(workCtx, work.Candidate)
					default:
						// Mirror-only book/proof/storage work always yields behind real LIVE.
						if s.waitForLiveDispatchShadowQuiet(workCtx) {
							s.processLiveAllowlistedSignalIntentAt(workCtx, work.Signal, work.Point,
								work.SignalAt, true, work.Candidate.ShadowAttemptID)
						}
					}
				})
			}(work)
		}
	}
	// A bounded shutdown normally closes and drains both lanes. If its context is canceled first,
	// retire every still-buffered reservation so the public work ledger cannot strand phantom jobs.
	if outstanding != nil {
		for {
			select {
			case _, ok := <-in:
				if !ok {
					return
				}
				outstanding.Add(-1)
			default:
				return
			}
		}
	}
}

func (s *Server) recordSkippedLivePolicyMirrorPortfolio(work livePolicyMirrorWork, reason string) {
	candidate := work.Candidate
	if strings.TrimSpace(candidate.Ticker) == "" {
		candidate = liveCandidateFromSignalIntent(liveSignalIntent{
			Signal: work.Signal, Point: work.Point, At: work.SignalAt,
			ShadowAttemptID: s.executionShadowAttemptForSignal(work.Signal),
		})
	}
	if strings.TrimSpace(candidate.ShadowAttemptID) == "" {
		return
	}
	row := s.livePolicyMirrorBaseRow(candidate)
	row.State = "not_observed"
	row.Reason = strings.TrimSpace(reason)
	row.ProcessedAt = time.Now().UTC()
	// This retained isolated writer cannot contend with the cash/main-ledger path. The optional
	// $400 portfolio row is intentionally absent; the canonical execution-only result already
	// exists and remains the evidence used for parity and settlement.
	s.executionShadowRecordMirror(candidate, row)
}

// The actual-candidate mirror yields only while cash work that can really reach a venue is active
// or queued. Raw observations and the global "last activity" timestamp are deliberately ignored:
// neither is authority to spend, and either can reset forever during a busy public feed.
func (s *Server) waitForLivePolicyMirrorPriorityTurn(ctx context.Context, dueAt time.Time) bool {
	if dueAt.IsZero() {
		dueAt = time.Now()
	}
	deadline := dueAt.Add(livePolicyMirrorStartLateMax)
	for {
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		s.liveMirrorMu.Lock()
		candidateDispatchPending := len(s.liveMirrorQ) > 0
		s.liveMirrorMu.Unlock()
		if s.liveDispatchShadowLiveActive.Load() <= 0 && !candidateDispatchPending {
			return true
		}
		wait := min(10*time.Millisecond, time.Until(deadline))
		if wait <= 0 {
			return false
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		}
	}
}

// The execution-only comparison is scheduled only after its exact LIVE terminal. It performs
// cached full-book reads and writes only to the isolated shadow ledger, so unrelated candidates
// waiting for their own dispatch must not erase it. An actually executing cash section still owns
// strict priority through the same bounded scheduled-touch window.
func (s *Server) waitForLivePolicyMirrorExecutionTurn(ctx context.Context, dueAt time.Time) bool {
	if dueAt.IsZero() {
		dueAt = time.Now()
	}
	deadline := dueAt.Add(livePolicyMirrorStartLateMax)
	for {
		if ctx.Err() != nil || time.Now().After(deadline) {
			return false
		}
		if s.liveDispatchShadowLiveActive.Load() <= 0 {
			return true
		}
		wait := min(10*time.Millisecond, time.Until(deadline))
		if wait <= 0 {
			return false
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		}
	}
}

func (s *Server) recordLateLivePolicyMirrorWork(ctx context.Context, work livePolicyMirrorWork) {
	s.recordMissedLivePolicyMirrorWork(ctx, work,
		"mirror-scheduled-touch-started-over-500ms-late")
}

func (s *Server) recordMissedLivePolicyMirrorWork(ctx context.Context, work livePolicyMirrorWork,
	reason string) {
	candidate := work.Candidate
	if strings.TrimSpace(candidate.Ticker) == "" {
		candidate = liveCandidateFromSignalIntent(liveSignalIntent{
			Signal: work.Signal, Point: work.Point, At: work.SignalAt,
			ShadowAttemptID: s.executionShadowAttemptForSignal(work.Signal),
		})
	}
	// A candidate that already passed LIVE must keep its terminal receipt even if the operator
	// changes the allowlist before this delayed touch. Re-running current policy here would erase
	// the exact evidence needed to explain the missed comparison.
	if !strings.EqualFold(candidate.Platform, "kalshi") ||
		strings.TrimSpace(candidate.Ticker) == "" ||
		(strings.ToUpper(candidate.Side) != "YES" && strings.ToUpper(candidate.Side) != "NO") {
		return
	}
	row := s.livePolicyMirrorBaseRow(candidate)
	row.State = storage.LivePolicyMirrorZeroFill
	row.Reason = strings.TrimSpace(reason)
	row.ProcessedAt = time.Now().UTC()
	s.insertLivePolicyMirrorRow(ctx, candidate, row)
}

func (s *Server) stopLivePolicyMirrorWorker(ctx context.Context) error {
	s.livePolicyMirrorMu.Lock()
	if s.livePolicyMirrorCh == nil {
		s.livePolicyMirrorMu.Unlock()
		return nil
	}
	if !s.livePolicyMirrorStopping {
		s.livePolicyMirrorStopping = true
		close(s.livePolicyMirrorCh)
		if s.livePolicyMirrorPriorityCh != nil {
			close(s.livePolicyMirrorPriorityCh)
		}
	}
	done, cancel := s.livePolicyMirrorDone, s.livePolicyMirrorCancel
	s.livePolicyMirrorMu.Unlock()
	select {
	case <-done:
		if cancel != nil {
			cancel()
		}
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

func waitLivePolicyMirror(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func waitLivePolicyMirrorUntil(ctx context.Context, due time.Time) bool {
	if due.IsZero() {
		return ctx.Err() == nil
	}
	delay := time.Until(due)
	if delay <= 0 {
		return ctx.Err() == nil
	}
	return waitLivePolicyMirror(ctx, delay)
}

// livePolicyMirrorIOC is the deterministic second-touch fill rule. The first touch is the IOC
// limit; the later full-book touch may improve that price, but it cannot chase a worse ask and
// cannot fill more than the visible size at the executable touch.
func livePolicyMirrorIOC(limitPrice, requestedQty float64, finalQ liveMirrorQuote) (float64, string) {
	if limitPrice <= 0 || limitPrice >= 1 || requestedQty <= 0 {
		return 0, "mirror-ioc-invalid-request"
	}
	if finalQ.Price <= 0 || finalQ.Price >= 1 || finalQ.Price > limitPrice+1e-9 {
		return 0, "mirror-ioc-limit-missed-after-wire-delay"
	}
	filled := math.Min(requestedQty, math.Max(0, finalQ.Depth))
	if filled <= 0 {
		return 0, "mirror-ioc-visible-depth-zero-after-wire-delay"
	}
	return filled, ""
}

func livePolicyMirrorExecute(timeInForce string, limitPrice, requestedQty float64,
	finalQ liveMirrorQuote) (float64, string) {
	if timeInForce != liveProspectiveFOK {
		return livePolicyMirrorIOC(limitPrice, requestedQty, finalQ)
	}
	if limitPrice <= 0 || limitPrice >= 1 || requestedQty <= 0 {
		return 0, "mirror-fok-invalid-request"
	}
	if finalQ.Price <= 0 || finalQ.Price >= 1 || finalQ.Price > limitPrice+1e-9 {
		return 0, "mirror-fok-limit-missed-after-wire-delay"
	}
	if finalQ.Depth+1e-9 < requestedQty {
		return 0, "mirror-fok-full-quantity-not-visible-after-wire-delay"
	}
	return requestedQty, ""
}

// simulateLivePolicyMirrorExecutionOnly isolates execution from portfolio selection. The exact
// LIVE-passed candidate supplies quantity and TIF; the delayed first full-book touch supplies the
// IOC/FOK limit; the second full-book touch after the configured wire delay decides fill. No fake
// holdings, semantic conflicts, balance, proof-history read, or shared cash database can erase
// this receipt. It is comparison evidence only and grants neither fake nor real money authority.
func (s *Server) simulateLivePolicyMirrorExecutionOnly(ctx context.Context,
	c liveMirrorCandidate, meta livePolicyMirrorExecutionMeta) {
	startedAt := time.Now().UTC()
	quantity := c.ProspectiveQty
	timeInForce := strings.TrimSpace(c.ProspectiveTimeInForce)
	if quantity < 1 || quantity != math.Floor(quantity) ||
		(timeInForce != liveProspectiveIOC && timeInForce != liveProspectiveFOK) {
		s.recordLivePolicyMirrorExecutionMiss(c, meta,
			"counterfactual-execution-frozen-plan-unavailable")
		return
	}
	initial, why := s.livePolicyMirrorExecutionCachedQuote(c)
	if why != "" {
		s.recordLivePolicyMirrorExecution(c, liveMirrorQuote{}, liveMirrorQuote{},
			meta, startedAt, 0, quantity, 0, 0, 0, false, "",
			livePolicyMirrorNotObserved, why)
		return
	}
	limit := initial.Price
	if !waitLivePolicyMirror(ctx, s.livePolicyMirrorFinalWireDelay()) {
		s.recordLivePolicyMirrorExecution(c, initial, liveMirrorQuote{},
			meta, startedAt, limit, quantity, 0, 0, 0, false, "",
			livePolicyMirrorNotObserved,
			"counterfactual-execution-wire-delay-canceled")
		return
	}
	finalQ, finalWhy := s.livePolicyMirrorExecutionCachedQuote(c)
	if finalWhy != "" {
		s.recordLivePolicyMirrorExecution(c, initial, liveMirrorQuote{},
			meta, startedAt, limit, quantity, 0, 0, 0, false, "",
			livePolicyMirrorNotObserved, finalWhy)
		return
	}
	if !livePolicyMirrorBookUsableAtWire(initial, finalQ) {
		s.recordLivePolicyMirrorExecution(c, initial, finalQ,
			meta, startedAt, limit, quantity, 0, 0, 0, false, "",
			livePolicyMirrorNotObserved,
			"counterfactual-execution-wire-book-provenance-regressed")
		return
	}
	filled, executionWhy := livePolicyMirrorExecute(timeInForce, limit, quantity, finalQ)
	if executionWhy != "" {
		s.recordLivePolicyMirrorExecution(c, initial, finalQ,
			meta, startedAt, limit, quantity, 0, 0, 0, false, "",
			livePolicyMirrorModeledZeroFill,
			"counterfactual-execution:"+executionWhy)
		return
	}
	fee, known, feeWhy := s.kalFeeExact(c.Ticker, false, filled, finalQ.Price)
	if !known {
		s.recordLivePolicyMirrorExecution(c, initial, finalQ,
			meta, startedAt, limit, quantity, filled, finalQ.Price, 0, false, "",
			livePolicyMirrorModeledFill,
			"counterfactual-execution-final-fee-unavailable:"+feeWhy)
		return
	}
	s.recordLivePolicyMirrorExecution(c, initial, finalQ,
		meta, startedAt, limit, quantity, filled, finalQ.Price, fee, true,
		"kalshi-exact-current-route-fee", livePolicyMirrorModeledFill, "")
}

// livePolicyMirrorExecutionCachedQuote keeps the comparison on the same market-rule decision as
// the accepted LIVE attempt. The frozen tick is immutable preflight evidence; the price and depth
// still come from a fresh, complete, sequence-proven WebSocket book at each simulated touch.
// Re-reading kmkt here previously introduced a second lifecycle selector after LIVE had terminaled.
func (s *Server) livePolicyMirrorExecutionCachedQuote(c liveMirrorCandidate) (liveMirrorQuote, string) {
	if s == nil || s.kal == nil || !strings.EqualFold(c.Platform, "kalshi") {
		return liveMirrorQuote{}, "mirror-kalshi-client-unavailable"
	}
	tick := c.ArbitrationQuote.Tick
	if tick <= 0 || tick > 1 {
		return liveMirrorQuote{}, "counterfactual-execution-frozen-tick-unavailable"
	}
	book, _, provenance, ok := s.kal.LiveBookWithProvenance(c.Ticker, livePolicyMirrorBookMaxAge)
	if !ok || book == nil {
		return liveMirrorQuote{}, "mirror-fresh-full-ws-book-unavailable"
	}
	maker, _, taker, depth, sidesOK := kalshiFullBookSides(book, c.Side)
	if !sidesOK || taker <= 0 || taker >= 1 || depth <= 0 {
		return liveMirrorQuote{}, "mirror-fresh-executable-touch-unavailable"
	}
	observed := provenance.ReceivedAt
	if observed.IsZero() {
		return liveMirrorQuote{}, "mirror-book-receipt-time-unavailable"
	}
	source := fmt.Sprintf("kalshi_ws_full_orderbook:g%d:s%d:q%d",
		provenance.Generation, provenance.SubscriptionID, provenance.Sequence)
	return liveMirrorQuote{Price: taker, Depth: depth, Tick: tick, MinQty: 1,
		MinQtyKnown: true, SpreadCents: math.Max(0, (taker-maker)*100),
		Maker: false, Route: "taker:live-policy-mirror", BookSource: source,
		SourceAt: provenance.SourceAt, ObservedAt: observed}, ""
}

func (s *Server) recordLivePolicyMirrorExecutionMiss(c liveMirrorCandidate,
	meta livePolicyMirrorExecutionMeta, reason string) {
	s.recordLivePolicyMirrorExecution(c, liveMirrorQuote{}, liveMirrorQuote{},
		meta, time.Now().UTC(), 0, c.ProspectiveQty, 0, 0, 0, false, "",
		livePolicyMirrorNotObserved, reason)
}

type livePolicyMirrorBookReceipt struct {
	generation uint64
	subID      int64
	sequence   int64
	ok         bool
}

func livePolicyMirrorBookReceiptFrom(source string) livePolicyMirrorBookReceipt {
	var out livePolicyMirrorBookReceipt
	n, err := fmt.Sscanf(strings.TrimSpace(source),
		"kalshi_ws_full_orderbook:g%d:s%d:q%d", &out.generation, &out.subID, &out.sequence)
	out.ok = err == nil && n == 3 && out.generation > 0 && out.subID > 0 && out.sequence > 0
	return out
}

// The final quote is re-read after the simulated wire delay, and livePolicyMirrorCachedQuote has
// already proved that it is a fresh full-depth WS book. An unchanged sequence therefore means the
// observed book stayed available at wire time; requiring a new market-data event would falsely
// turn quiet, fillable books into zero fills. Reject only provenance that moved backwards. Sequence
// is comparable only inside one generation/subscription domain; ReceivedAt is the reconnect
// fallback.
func livePolicyMirrorBookUsableAtWire(initial, final liveMirrorQuote) bool {
	first, second := livePolicyMirrorBookReceiptFrom(initial.BookSource),
		livePolicyMirrorBookReceiptFrom(final.BookSource)
	if !first.ok || !second.ok {
		return false
	}
	if first.generation == second.generation && first.subID == second.subID {
		return second.sequence >= first.sequence
	}
	return !initial.ObservedAt.IsZero() && final.ObservedAt.After(initial.ObservedAt)
}

func (s *Server) livePolicyMirrorBaseRow(c liveMirrorCandidate) storage.LivePolicyMirrorRow {
	at := c.At
	if at.IsZero() {
		at = time.Now().UTC()
	}
	return storage.LivePolicyMirrorRow{
		CandidateID: livePolicyMirrorCandidateID(c, time.Now().UTC()),
		IntentKey:   livePolicyMirrorIntentKey(c), ObservedAt: at.UTC(),
		ProcessedAt: time.Now().UTC(), Venue: "kalshi", Ticker: c.Ticker, Title: c.Title,
		Side: strings.ToUpper(c.Side), SystemID: strings.ToLower(liveMirrorFamily(c)),
		Route: "taker", SignalPrice: c.Price,
		DelayMS:      s.livePolicyMirrorExecutionDelay().Milliseconds(),
		WireDelayMS:  s.livePolicyMirrorFinalWireDelay().Milliseconds(),
		ModelVersion: livePolicyMirrorModelVersion,
	}
}

func (s *Server) insertLivePolicyMirrorRow(ctx context.Context, candidate liveMirrorCandidate,
	row storage.LivePolicyMirrorRow) {
	if s == nil {
		return
	}
	priority := isLivePolicyMirrorPriority(ctx)
	// For an actual LIVE-passed candidate, the isolated execution-shadow terminal is the
	// authoritative comparison receipt. Queue it exactly once even when the shared cash database
	// is locked, shutting down, or rejects the companion portfolio row. Background writers retain
	// their existing strict cash-quiet ordering below.
	if priority {
		defer s.executionShadowRecordMirror(candidate, row)
	}
	if s.store == nil {
		return
	}
	// Actual LIVE-passed work already completed its bounded priority-only admission wait at the
	// scheduled touch. Do not re-enter the globally resetting background quiet gate here.
	if !priority && !s.waitForLiveDispatchShadowQuiet(ctx) {
		return
	}
	writeCtx, cancel := context.WithTimeout(ctx, liveDispatchShadowWriteTimeout)
	defer cancel()
	if _, err := s.store.InsertLivePolicyMirror(writeCtx, row); err != nil && s.log != nil {
		s.log.Warn("LIVE-policy mirror row write failed; real LIVE was unaffected",
			"ticker", row.Ticker, "state", row.State, "err", err)
		return
	}
	if !priority {
		s.executionShadowRecordMirror(candidate, row)
	}
}

func (s *Server) recordLivePolicyMirrorRejection(ctx context.Context, sig storage.Signal,
	reason string, signalAt time.Time, explicitAttemptID ...string) {
	shadowAttemptID := ""
	if len(explicitAttemptID) > 0 {
		shadowAttemptID = strings.TrimSpace(explicitAttemptID[0])
	}
	if shadowAttemptID == "" {
		shadowAttemptID = s.executionShadowAttemptForSignal(sig)
	}
	c := liveCandidateFromSignalIntent(liveSignalIntent{Signal: sig, At: signalAt,
		ShadowAttemptID: shadowAttemptID})
	if !strings.EqualFold(c.Platform, "kalshi") ||
		s.liveSystemSelectionReason(c, "taker") != "" {
		return
	}
	row := s.livePolicyMirrorBaseRow(c)
	row.State, row.Reason = storage.LivePolicyMirrorRejected, strings.TrimSpace(reason)
	s.insertLivePolicyMirrorRow(ctx, c, row)
}

func (s *Server) livePolicyMirrorCachedQuote(c liveMirrorCandidate) (liveMirrorQuote, string) {
	if s == nil || s.kal == nil || !strings.EqualFold(c.Platform, "kalshi") {
		return liveMirrorQuote{}, "mirror-kalshi-client-unavailable"
	}
	market, ok := s.kmkt(c.Ticker)
	if !ok || market.Result != "" ||
		(!strings.EqualFold(market.Status, "active") && !strings.EqualFold(market.Status, "open")) {
		return liveMirrorQuote{}, "mirror-current-market-lifecycle-unavailable"
	}
	book, _, provenance, ok := s.kal.LiveBookWithProvenance(c.Ticker, livePolicyMirrorBookMaxAge)
	if !ok || book == nil {
		return liveMirrorQuote{}, "mirror-fresh-full-ws-book-unavailable"
	}
	maker, _, taker, depth, sidesOK := kalshiFullBookSides(book, c.Side)
	if !sidesOK || taker <= 0 || taker >= 1 || depth <= 0 {
		return liveMirrorQuote{}, "mirror-fresh-executable-touch-unavailable"
	}
	tick, tickKnown := market.TickForKnown(taker)
	if !tickKnown || tick <= 0 {
		return liveMirrorQuote{}, "mirror-current-market-tick-unavailable"
	}
	observed := provenance.ReceivedAt
	if observed.IsZero() {
		return liveMirrorQuote{}, "mirror-book-receipt-time-unavailable"
	}
	source := fmt.Sprintf("kalshi_ws_full_orderbook:g%d:s%d:q%d",
		provenance.Generation, provenance.SubscriptionID, provenance.Sequence)
	return liveMirrorQuote{Price: taker, Depth: depth, Tick: tick, MinQty: 1,
		MinQtyKnown: true, SpreadCents: math.Max(0, (taker-maker)*100),
		Maker: false, Route: "taker:live-policy-mirror", BookSource: source,
		SourceAt: provenance.SourceAt, ObservedAt: observed}, ""
}

// livePolicyMirrorIsolatedProof is the normal simulated-money boundary. It preserves the
// independent opportunity calculation as diagnostics, but applies the same fill-conditioned
// promotion fence as LIVE without inserting a shared unit_trial, touching the LIVE signal-receipt
// map, or joining LIVE's proof-cache singleflight.
func (s *Server) livePolicyMirrorIsolatedProof(ctx context.Context, c liveMirrorCandidate,
	price float64) (ok bool, basis string, meanNow, loNow, feePC float64) {
	return s.livePolicyMirrorIsolatedProofAtQuantity(ctx, c, price, 1)
}

func (s *Server) livePolicyMirrorIsolatedProofAtQuantity(ctx context.Context,
	c liveMirrorCandidate, price, quantity float64) (
	ok bool, basis string, meanNow, loNow, feePC float64) {
	ok, basis, meanNow, loNow, feePC =
		s.livePolicyMirrorIsolatedDiagnosticProofAtQuantity(ctx, c, price, quantity)
	if !ok {
		return ok, basis, meanNow, loNow, feePC
	}
	return false, liveFillConditionedProofUnavailableReason, meanNow, loNow, feePC
}

func (s *Server) livePolicyMirrorIsolatedDiagnosticProof(ctx context.Context,
	c liveMirrorCandidate, price float64) (
	ok bool, basis string, meanNow, loNow, feePC float64) {
	return s.livePolicyMirrorIsolatedDiagnosticProofAtQuantity(ctx, c, price, 1)
}

// livePolicyMirrorIsolatedDiagnosticProofAtQuantity keeps the independent mirror's
// current-generation opportunity statistics observable. It cannot authorize a simulated normal
// allocation until the same fill-conditioned LIVE cohort required by the cash path exists.
func (s *Server) livePolicyMirrorIsolatedDiagnosticProofAtQuantity(ctx context.Context,
	c liveMirrorCandidate, price, quantity float64) (
	ok bool, basis string, meanNow, loNow, feePC float64) {
	if !s.liveSystemVenueEnabled(c.Platform) {
		return false, "prospective-live-system-disabled-for-venue", 0, 0, 0
	}
	if quantity < 1 || quantity != math.Floor(quantity) {
		return false, "prospective-allocation-exact-whole-quantity-required", 0, 0, 0
	}
	if !strings.EqualFold(c.Platform, "kalshi") {
		return false, "prospective-allocation-kalshi-taker-singles-only", 0, 0, 0
	}
	action := strings.ToUpper(strings.TrimSpace(c.Action))
	if action == "" {
		action = "BUY"
	}
	if action != "BUY" {
		return false, "prospective-allocation-buy-singles-only", 0, 0, 0
	}
	age := time.Since(c.At)
	if c.At.IsZero() || age < -2*time.Second || age > liveMirrorTTL {
		return false, "prospective-allocation-signal-receipt-stale", 0, 0, 0
	}
	family := liveMirrorFamily(c)
	if family == "" || strings.HasPrefix(family, "counterfactual:") ||
		strings.HasPrefix(family, "side-control:") {
		return false, "prospective-allocation-exact-system-family-unavailable", 0, 0, 0
	}
	bound, contract, why := s.liveAllocationBindSignalContract(c, "taker", false)
	if why != "" {
		return false, why, 0, 0, 0
	}
	c = bound
	if why = liveAllocationEconomicHistoryTopologyReason(family, c.Platform, c.Side, "taker"); why != "" {
		return false, why, 0, 0, 0
	}
	if why = s.liveAllocationInputFreshReason(contract, c); why != "" {
		return false, why, 0, 0, 0
	}
	if s.store == nil {
		return false, "prospective-allocation-ledger-unavailable", 0, 0, 0
	}
	feePC = s.liveMirrorFeePCAtQuantity(c, false, quantity, price)
	if math.IsNaN(feePC) || math.IsInf(feePC, 0) || feePC < 0 {
		return false, "prospective-allocation-current-fee-unavailable", 0, 0, feePC
	}
	minMarkets := s.liveAllocationMinMarkets()
	floor := s.liveAllocationEdgeFloor()
	// The isolated mirror uses the same evidence contract as LIVE: only settled history from this
	// exact strategy stream and execution generation can authorize and size the simulated order.
	// Model-origin and older strategy rows remain discovery/history evidence only.
	best, found, readErr := s.livePriorityUnitTrialRouteLeaderboard(
		ctx, time.Now().UTC(), family, c.Platform, "strategy", c.Side)
	if readErr != nil {
		return false, "prospective-allocation-route-history-read-failed", 0, 0, feePC
	}
	if !found || best.SettledMarkets < minMarkets {
		return false, fmt.Sprintf(
			"prospective-allocation-needs-%d-distinct-settled-exact-strategy-contracts",
			minMarkets), 0, 0, feePC
	}
	_, meanNow, loNow = liveMirrorUnitAdjusted(best, price, feePC)
	if math.IsNaN(meanNow) || math.IsInf(meanNow, 0) ||
		math.IsNaN(loNow) || math.IsInf(loNow, 0) || loNow < floor {
		return false, "prospective-allocation-current-cost-erases-confidence-lower-bound",
			meanNow, loNow, feePC
	}
	quantityBasis := ""
	if quantity > 1 {
		quantityBasis = fmt.Sprintf("q%.0f:", quantity)
	}
	basis = fmt.Sprintf("mirror-isolated-prospective-allocation:%s@%s[%s]/taker:%s"+
		"input=%s:contract=%s:%s:n%d:m%d-diagnostic:generation=%s",
		family, c.Platform, strings.ToUpper(c.Side), quantityBasis, c.InputTopology, c.SignalContractID,
		"strategy", best.SettledMarkets, best.SettledEventClusters, livePriorityProofGeneration)
	return true, basis, meanNow, loNow, feePC
}

type livePolicyMirrorPortfolio struct {
	Meta         storage.LivePolicyMirrorMeta
	Rows         []storage.LivePolicyMirrorRow
	Open         []storage.LivePolicyMirrorRow
	Positions    []paper.Position
	SettledNet   float64
	OpenMarkNet  float64
	Fees         float64
	OpenCost     float64
	OpenValue    float64
	Cash         float64
	NAV          float64
	NAVKnown     bool
	CryptoCost   float64
	ClusterCosts map[string]float64
	ReasonCounts map[string]int
	StateCounts  map[string]int
}

type livePolicyMirrorBriefSnapshot struct {
	At       time.Time
	Cash     float64
	NAV      float64
	Net      float64
	NAVKnown bool
	Filled   int
	Open     int
	Settled  int
	ZeroFill int
	Rejected int
}

func (s *Server) publishLivePolicyMirrorBrief(p livePolicyMirrorPortfolio) {
	if s == nil {
		return
	}
	open := p.StateCounts[storage.LivePolicyMirrorFilled]
	settled := p.StateCounts[storage.LivePolicyMirrorSettled]
	s.livePolicyMirrorBrief.Store(&livePolicyMirrorBriefSnapshot{
		At: time.Now().UTC(), Cash: p.Cash, NAV: p.NAV,
		Net: p.SettledNet + p.OpenMarkNet, NAVKnown: p.NAVKnown,
		Filled: open + settled, Open: open, Settled: settled,
		ZeroFill: p.StateCounts[storage.LivePolicyMirrorZeroFill],
		Rejected: p.StateCounts[storage.LivePolicyMirrorRejected],
	})
}

// livePolicyMirrorBriefLine is read by BriefingText/Telegram only from the last-good atomic
// snapshot. It performs no SQLite or venue work and therefore cannot slow the recurring briefing.
func (s *Server) livePolicyMirrorBriefLine() string {
	if s == nil {
		return ""
	}
	snap := s.livePolicyMirrorBrief.Load()
	dropped := s.livePolicyMirrorDropped.Load()
	if snap == nil {
		return fmt.Sprintf("🪞 $400 LIVE-policy mirror: warming · dropped %d · no real-money authority", dropped)
	}
	nav, net := "n/a", "n/a"
	if snap.NAVKnown {
		nav = fmt.Sprintf("$%.2f", snap.NAV)
		net = fmt.Sprintf("%+.2f", snap.Net)
	}
	return fmt.Sprintf("🪞 $400 LIVE-policy mirror: cash $%.2f · NAV %s · net %s · filled %d · open %d · settled %d · zero %d · rejected %d · dropped %d",
		snap.Cash, nav, net, snap.Filled, snap.Open, snap.Settled, snap.ZeroFill,
		snap.Rejected, dropped)
}

func (s *Server) livePolicyMirrorPortfolio(ctx context.Context) (livePolicyMirrorPortfolio, error) {
	var out livePolicyMirrorPortfolio
	out.ClusterCosts = make(map[string]float64)
	out.ReasonCounts = make(map[string]int)
	out.StateCounts = make(map[string]int)
	meta, err := s.store.LivePolicyMirrorMeta(ctx)
	if err != nil {
		return out, err
	}
	rollup, err := s.store.LivePolicyMirrorRollup(ctx)
	if err != nil {
		return out, err
	}
	open, err := s.store.OpenLivePolicyMirrorCurrent(ctx)
	if err != nil {
		return out, err
	}
	recent, err := s.store.ListLivePolicyMirrorCurrent(ctx, 100)
	if err != nil {
		return out, err
	}
	out.Meta, out.Rows, out.Open, out.NAVKnown = meta, recent, open, true
	out.StateCounts, out.ReasonCounts = rollup.StateCounts, rollup.ReasonCounts
	out.SettledNet, out.Fees = rollup.SettledNet, rollup.FeesUSD
	for _, row := range open {
		out.OpenCost += row.CostUSD
		if row.Crypto {
			out.CryptoCost += row.CostUSD
		}
		out.ClusterCosts[row.ClusterKey] += row.CostUSD
		out.Positions = append(out.Positions, paper.Position{Platform: "kalshi",
			Ticker: row.Ticker, Title: row.Title, Side: row.Side,
			Contracts: row.FilledQty, AvgPrice: row.FillPrice, CostBasis: row.CostUSD})
		mark, known := s.liveDispatchShadowCachedExitMark(row.Ticker, row.Side)
		if !known {
			out.NAVKnown = false
			continue
		}
		out.OpenValue += row.FilledQty * mark
		out.OpenMarkNet += row.FilledQty*(mark-row.FillPrice) - row.FeeUSD
	}
	out.Cash = meta.SeedUSD + out.SettledNet - out.OpenCost
	if out.NAVKnown {
		out.NAV = meta.SeedUSD + out.SettledNet + out.OpenMarkNet
	}
	s.publishLivePolicyMirrorBrief(out)
	return out, nil
}

func (s *Server) livePolicyMirrorRiskReason(p livePolicyMirrorPortfolio,
	c liveMirrorCandidate, cost float64) string {
	if !p.NAVKnown || p.NAV <= 0 {
		return "mirror-current-nav-or-open-marks-unavailable"
	}
	risk := s.cfg().Risk
	lossPct := risk.LiveDailyLossPct
	if lossPct <= 0 {
		lossPct = .075
	}
	if p.Meta.SeedUSD-p.NAV > p.Meta.SeedUSD*lossPct+1e-9 {
		return "mirror-7.5pct-session-loss-stop"
	}
	if p.Meta.PeakNAVUSD-p.NAV > p.Meta.PeakNAVUSD*lossPct+1e-9 {
		return "mirror-7.5pct-peak-drawdown-stop"
	}
	maxOrder, ok := liveRailLimit(p.NAV, risk.LiveMaxOrderPct, risk.LiveMaxOrderUSD)
	if !ok || cost > maxOrder+1e-9 {
		return "mirror-5pct-order-rail"
	}
	if cost > p.Cash+1e-9 {
		return "mirror-insufficient-fake-cash"
	}
	totalCap, ok := liveRailLimit(p.NAV, risk.LiveExposureCapPct, -1)
	if !ok || p.OpenCost+cost > totalCap+1e-9 {
		return "mirror-20pct-total-exposure-rail"
	}
	class := liveCryptoCandidateClass(c)
	if class == liveCryptoUnknown {
		return "mirror-crypto-classification-unavailable"
	}
	if class == liveCryptoYes {
		cryptoCap, capOK := liveRailLimit(p.NAV, risk.LiveCryptoCapPct, -1)
		if !capOK || p.CryptoCost+cost > cryptoCap+1e-9 {
			return "mirror-10pct-crypto-exposure-rail"
		}
	}
	cluster := s.liveMirrorClusterKey("kalshi", c.Ticker, c.Title)
	clusterCap, capOK := liveRailLimit(p.Meta.SeedUSD, risk.LiveClusterCapPct, -1)
	if !capOK || p.ClusterCosts[cluster]+cost > clusterCap+1e-9 {
		return "mirror-3pct-event-cluster-rail"
	}
	return ""
}

func (s *Server) persistLivePolicyMirrorPeak(ctx context.Context,
	p *livePolicyMirrorPortfolio) error {
	if p == nil || !p.NAVKnown || p.NAV <= 0 {
		return nil
	}
	if err := s.store.UpdateLivePolicyMirrorPeak(ctx, p.NAV, time.Now().UTC(), p.Meta.EpochAt); err != nil {
		return err
	}
	if p.NAV > p.Meta.PeakNAVUSD {
		p.Meta.PeakNAVUSD = p.NAV
	}
	return nil
}

func (s *Server) livePolicyMirrorAdmissionReason(ctx context.Context,
	p livePolicyMirrorPortfolio, c liveMirrorCandidate) string {
	market, cached := s.kmkt(c.Ticker)
	eventKey := strings.ToUpper(strings.TrimSpace(market.EventTicker))
	if !cached || eventKey == "" {
		return "mirror-kalshi-event-identity-cache-unavailable"
	}
	for _, row := range p.Open {
		if strings.EqualFold(row.Ticker, c.Ticker) {
			return "mirror-existing-ticker-position"
		}
		if strings.EqualFold(row.EventKey, eventKey) {
			return "mirror-existing-kalshi-event-position"
		}
	}
	if why := s.livePolicyMirrorCachedSemanticConflict(c, p.Open); why != "" {
		return why
	}
	exists, err := s.store.LivePolicyMirrorIntentExistsSince(ctx,
		livePolicyMirrorIntentKey(c), time.Now().Add(-liveMirrorClaimTTL))
	if err != nil {
		return "mirror-durable-intent-dedup-unavailable"
	}
	if exists {
		return "mirror-duplicate-intent-within-6h"
	}
	return ""
}

func (s *Server) livePolicyMirrorCachedSemanticDescriptor(ticker, side string) (
	kalshiSemanticDescriptor, bool) {
	ticker = strings.TrimSpace(ticker)
	market, ok := s.kmkt(ticker)
	eventTicker := strings.ToUpper(strings.TrimSpace(market.EventTicker))
	if !ok || eventTicker == "" {
		return kalshiSemanticDescriptor{}, false
	}
	if s.allowSyntheticKalshiEventIdentity {
		return kalshiSemanticDescriptor{Ticker: ticker, EventTicker: eventTicker}, true
	}
	now := time.Now()
	s.kalSemMu.Lock()
	eventEntry, eventOK := s.kalSemEvents[eventTicker]
	milestoneEntry, milestoneOK := s.kalSemMilestone[eventTicker]
	s.kalSemMu.Unlock()
	if !eventOK || eventEntry.Err != "" || now.Sub(eventEntry.At) < 0 ||
		now.Sub(eventEntry.At) > kalshiSemanticSuccessTTL {
		return kalshiSemanticDescriptor{}, false
	}
	out := kalshiSemanticDescriptor{Ticker: ticker, EventTicker: eventTicker,
		Event:  eventEntry.Event,
		Sports: strings.EqualFold(strings.TrimSpace(eventEntry.Event.Category), "sports")}
	if !out.Sports {
		return out, true
	}
	if !milestoneOK || !milestoneEntry.Found || milestoneEntry.Err != "" ||
		now.Sub(milestoneEntry.At) < 0 || now.Sub(milestoneEntry.At) > kalshiSemanticSuccessTTL {
		return kalshiSemanticDescriptor{}, false
	}
	out.Milestone = milestoneEntry.Milestone
	out.Predicate = semanticPurchasedPredicate(market, out.Event, out.Milestone, side)
	return out, true
}

// The mirror may consume only semantic facts already warmed by the real-money path. It never
// invokes the priority Event/Milestone endpoints itself.
func (s *Server) livePolicyMirrorCachedSemanticConflict(c liveMirrorCandidate,
	open []storage.LivePolicyMirrorRow) string {
	candidate, ok := s.livePolicyMirrorCachedSemanticDescriptor(c.Ticker, c.Side)
	if !ok {
		return "mirror-kalshi-payoff-identity-cache-unavailable"
	}
	if !candidate.Sports {
		return ""
	}
	predicates := []objectiveidentity.PurchasedPredicate{candidate.Predicate}
	for _, row := range open {
		held, heldOK := s.livePolicyMirrorCachedSemanticDescriptor(row.Ticker, row.Side)
		if !heldOK {
			return "mirror-kalshi-held-payoff-identity-cache-unavailable"
		}
		if held.Sports && held.Milestone.ID == candidate.Milestone.ID {
			predicates = append(predicates, held.Predicate)
		}
	}
	if len(predicates) < 2 {
		return ""
	}
	if why := semanticConflictReason(objectiveidentity.EvaluatePurchasedSet(predicates)); why != "" {
		return "mirror-" + why
	}
	return ""
}

func (s *Server) rejectLivePolicyMirrorCandidate(ctx context.Context, c liveMirrorCandidate,
	row storage.LivePolicyMirrorRow, reason string) {
	row.State, row.Reason, row.ProcessedAt = storage.LivePolicyMirrorRejected,
		strings.TrimSpace(reason), time.Now().UTC()
	s.insertLivePolicyMirrorRow(ctx, c, row)
}

func (s *Server) simulateLivePolicyMirror(ctx context.Context, c liveMirrorCandidate) {
	row := s.livePolicyMirrorBaseRow(c)
	if c.At.IsZero() || time.Since(c.At) < 0 || time.Since(c.At) > liveMirrorTTL {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-candidate-expired-before-delay")
		return
	}
	// The scheduler gives every signal its own due time. This guard keeps direct/internal callers
	// honest without adding another full delay after queue time.
	if !waitLivePolicyMirrorUntil(ctx, c.At.Add(s.livePolicyMirrorExecutionDelay())) {
		return
	}
	// Priority work already yielded behind actual cash-active/candidate-dispatch state at DueAt.
	// Background/disarmed callers retain the global yield discipline.
	if !isLivePolicyMirrorPriority(ctx) && !s.waitForLiveDispatchShadowQuiet(ctx) {
		return
	}
	if time.Since(c.At) > liveMirrorTTL {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-candidate-expired-during-delay")
		return
	}
	if !s.paperEntryHorizonNow(ctx, "kalshi", c.Ticker, c.Title) {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-current-funded-entry-horizon-unavailable-or-exceeded")
		return
	}
	q, why := s.livePolicyMirrorCachedQuote(c)
	if why != "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, why)
		return
	}
	row.LimitPrice, row.TouchDepth, row.BookSource = q.Price, q.Depth, q.BookSource
	if q.Price < gfPriceLo || q.Price > gfPriceHi {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-current-price-outside-system-range")
		return
	}
	if q.SpreadCents > gfSpreadCapC {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-current-spread-too-wide")
		return
	}
	if offline := s.pxOfflineReason("kalshi", c.Ticker); offline != "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-current-price-source-offline:"+offline)
		return
	}
	if why := s.liveSystemPostProofReason(c, "taker"); why != "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, why)
		return
	}
	portfolio, err := s.livePolicyMirrorPortfolio(ctx)
	if err != nil {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-portfolio-read-unavailable")
		return
	}
	if err = s.persistLivePolicyMirrorPeak(ctx, &portfolio); err != nil {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-peak-nav-write-unavailable")
		return
	}
	if why := s.livePolicyMirrorAdmissionReason(ctx, portfolio, c); why != "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, why)
		return
	}
	cell, cellOK := s.gfModeCached(liveMirrorFamily(c), "kalshi", c.Side)
	if !cellOK {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"current-exact-system-verdict-unavailable-or-stale")
		return
	}
	plan, planWhy := s.liveProspectivePlan(ctx, c, q, portfolio.NAV, cell, true)
	if planWhy != "" || !plan.valid() {
		if planWhy == "" {
			planWhy = "mirror-current-exact-route-proof-lower-bound-nonpositive"
		}
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, planWhy)
		return
	}
	applyLiveProspectivePlan(&c, plan)
	proofMean, proofLower, proofFee := plan.Mean, plan.Lower, plan.ProofFeePC
	count, fee := plan.Qty, plan.RequestedFee
	cost := count*q.Price + fee
	row.ProofMean, row.ProofLower, row.ProofFeePC = proofMean, proofLower, proofFee
	row.SizingBankroll, row.SizingTargetUSD = portfolio.NAV, plan.TargetUSD
	if why := s.livePolicyMirrorRiskReason(portfolio, c, cost); why != "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, why)
		return
	}
	row.RequestedQty = count
	market, eventKnown := s.kmkt(c.Ticker)
	eventKey := strings.ToUpper(strings.TrimSpace(market.EventTicker))
	if !eventKnown || eventKey == "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-canonical-event-identity-cache-unavailable")
		return
	}
	row.EventKey = eventKey
	row.ClusterKey = s.liveMirrorClusterKey("kalshi", c.Ticker, c.Title)
	row.Crypto = liveCryptoCandidateClass(c) == liveCryptoYes
	s.executionShadowRecordQuote(c, "counterfactual-decision-book", "captured",
		plan.TimeInForce+": "+plan.Reason, q, fee, plan.FeeSource, count)
	if !waitLivePolicyMirror(ctx, s.livePolicyMirrorFinalWireDelay()) {
		return
	}
	if !s.paperEntryHorizonNow(ctx, "kalshi", c.Ticker, c.Title) {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-market-left-funded-horizon-before-ioc")
		return
	}
	finalQ, finalWhy := s.livePolicyMirrorCachedQuote(c)
	if finalWhy != "" {
		row.State, row.Reason, row.ProcessedAt = storage.LivePolicyMirrorZeroFill,
			finalWhy, time.Now().UTC()
		s.insertLivePolicyMirrorRow(ctx, c, row)
		return
	}
	if !livePolicyMirrorBookUsableAtWire(q, finalQ) {
		s.executionShadowRecordQuoteWithLimit(c, "counterfactual-final-book", "zero-fill",
			"mirror-ioc-wire-book-provenance-regressed", finalQ, q.Price, -1, "", count)
		row.State, row.Reason, row.ProcessedAt = storage.LivePolicyMirrorZeroFill,
			"mirror-ioc-wire-book-provenance-regressed", time.Now().UTC()
		s.insertLivePolicyMirrorRow(ctx, c, row)
		return
	}
	filled, executionWhy := livePolicyMirrorExecute(plan.TimeInForce, q.Price, count, finalQ)
	if executionWhy != "" {
		s.executionShadowRecordQuoteWithLimit(c, "counterfactual-final-book", "zero-fill",
			executionWhy, finalQ, q.Price, -1, "", count)
		row.State, row.Reason, row.ProcessedAt = storage.LivePolicyMirrorZeroFill,
			executionWhy, time.Now().UTC()
		s.insertLivePolicyMirrorRow(ctx, c, row)
		return
	}
	finalFee, known, finalFeeWhy := s.kalFeeExact(c.Ticker, false, filled, finalQ.Price)
	if !known {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-final-route-fee-unavailable:"+finalFeeWhy)
		return
	}
	s.executionShadowRecordQuoteWithLimit(c, "counterfactual-final-book", "executable",
		"same captured original limit; final book passed realistic "+plan.TimeInForce, finalQ, q.Price,
		finalFee, "kalshi-exact-current-route-fee", filled)
	finalMean, finalLower := liveAllocationWireAdjusted(proofMean, proofLower,
		q.Price, proofFee, finalQ.Price, finalFee/filled)
	if finalLower < s.liveAllocationEdgeFloor() {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-final-wire-cost-erased-fee-net-lower-bound")
		return
	}
	finalPointEdge, finalPointKnown := gfCurrentRouteEdge(cell.Mean, cell.MeanAsk,
		finalQ.Price, cell.FeePC, finalFee, filled)
	if !finalPointKnown || finalPointEdge <= 0 {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-final-executable-price-or-exact-fee-erased-point-edge")
		return
	}
	finalCost := filled*finalQ.Price + finalFee
	// The final admission/read/write is one mirror-ledger transaction boundary. Concurrent delayed
	// candidates may do independent book/proof work, but cannot jointly spend the same fake cash or
	// pass the same exposure cap from one stale portfolio.
	s.livePolicyMirrorSettleMu.Lock()
	defer s.livePolicyMirrorSettleMu.Unlock()
	portfolio, err = s.livePolicyMirrorPortfolio(ctx)
	if err != nil {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-final-portfolio-read-unavailable")
		return
	}
	if err = s.persistLivePolicyMirrorPeak(ctx, &portfolio); err != nil {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-final-peak-nav-write-unavailable")
		return
	}
	finalPlan, finalPlanWhy := s.liveProspectivePlan(ctx, c, finalQ, portfolio.NAV,
		cell, true)
	if finalPlanWhy != "" || !finalPlan.valid() {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-final-exact-plan:"+finalPlanWhy)
		return
	}
	if plan.TimeInForce == liveProspectiveIOC &&
		finalPlan.TimeInForce == liveProspectiveFOK {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-final-ioc-no-longer-safe-for-partial-fill")
		return
	}
	if filled > finalPlan.Qty+1e-9 {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			"mirror-final-filled-quantity-exceeds-refreshed-exact-plan")
		return
	}
	finalExactCount, _, _, _, finalExactWhy := s.liveProspectiveAllocationSize("kalshi",
		portfolio.NAV, finalQ.Price, finalMean, finalLower, finalFee/filled,
		finalQ.Depth, finalQ.MinQty, finalQ.MinQtyKnown)
	if finalExactWhy != "" || filled > finalExactCount+1e-9 {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row,
			fmt.Sprintf("mirror-final-filled-quantity-exceeds-exact-fee-capacity:%.0f:%s",
				finalExactCount, finalExactWhy))
		return
	}
	if why := s.livePolicyMirrorAdmissionReason(ctx, portfolio, c); why != "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-final-"+why)
		return
	}
	if why := s.livePolicyMirrorRiskReason(portfolio, c, finalCost); why != "" {
		s.rejectLivePolicyMirrorCandidate(ctx, c, row, "mirror-final-"+why)
		return
	}
	row.State, row.Reason, row.ProcessedAt = storage.LivePolicyMirrorFilled,
		"", time.Now().UTC()
	row.FilledQty, row.FillPrice, row.TouchDepth = filled, finalQ.Price, finalQ.Depth
	row.FeeUSD, row.FeeSource, row.CostUSD = finalFee, "kalshi-exact-current-route-fee", finalCost
	row.ProofMean, row.ProofLower = finalMean, finalLower
	row.BookSource = q.BookSource + " -> " + finalQ.BookSource
	s.insertLivePolicyMirrorRow(ctx, c, row)
}

func (s *Server) settleLivePolicyMirror(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	s.livePolicyMirrorSettleMu.Lock()
	defer s.livePolicyMirrorSettleMu.Unlock()
	if !s.waitForLiveDispatchShadowQuiet(ctx) {
		return
	}
	brief := s.livePolicyMirrorBrief.Load()
	if brief == nil || time.Since(brief.At) >= time.Minute {
		// Warm/refresh the recurring Telegram line on this background lane. BriefingText itself
		// stays an atomic last-good read and never waits for SQLite.
		_, _ = s.livePolicyMirrorPortfolio(ctx)
	}
	open, err := s.store.OpenLivePolicyMirror(ctx)
	if err != nil || len(open) == 0 {
		return
	}
	tickers := make([]string, 0, len(open))
	for _, row := range open {
		tickers = append(tickers, row.Ticker)
	}
	receipts, err := s.store.VenueSettlementsForTickers(ctx, "kalshi", tickers)
	if err != nil {
		return
	}
	for _, row := range open {
		receipt, ok := receipts[row.Ticker]
		if !ok {
			continue
		}
		_, _ = s.store.SettleLivePolicyMirror(ctx, row.ID, receipt.YesValue,
			receipt.ResolvedAt, receipt.SourceArtifact, receipt.Hash)
	}
	if portfolio, readErr := s.livePolicyMirrorPortfolio(ctx); readErr == nil && portfolio.NAVKnown {
		_ = s.store.UpdateLivePolicyMirrorPeak(ctx, portfolio.NAV, time.Now().UTC(), portfolio.Meta.EpochAt)
	}
}

func (s *Server) handleLivePolicyMirror(w http.ResponseWriter, r *http.Request) {
	if s == nil || s.store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "mirror store unavailable"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	portfolio, err := s.livePolicyMirrorPortfolio(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	recent := portfolio.Rows
	if len(recent) > 100 {
		recent = recent[:100]
	}
	sort.Slice(recent, func(i, j int) bool { return recent[i].ID > recent[j].ID })
	rows := make([]map[string]any, 0, len(recent))
	for _, row := range recent {
		rows = append(rows, map[string]any{
			"id": row.ID, "observed_at": row.ObservedAt, "processed_at": row.ProcessedAt,
			"ticker": row.Ticker, "title": row.Title, "side": row.Side,
			"system": row.SystemID, "state": row.State, "reason": row.Reason,
			"requested_qty": row.RequestedQty, "filled_qty": row.FilledQty,
			"limit_price": row.LimitPrice, "fill_price": row.FillPrice,
			"fee_usd": row.FeeUSD, "cost_usd": row.CostUSD,
			"proof_mean": row.ProofMean, "proof_lower": row.ProofLower,
			"book_source": row.BookSource, "model_version": row.ModelVersion,
			"realized_net_usd": row.RealizedNet,
		})
	}
	dropped := s.livePolicyMirrorDropped.Load()
	priorityDropped := s.livePolicyMirrorPriorityDropped.Load()
	s.livePolicyMirrorMu.Lock()
	backgroundChannelDepth, priorityChannelDepth :=
		len(s.livePolicyMirrorCh), len(s.livePolicyMirrorPriorityCh)
	s.livePolicyMirrorMu.Unlock()
	priorityWaiting, priorityActive := s.livePolicyMirrorPriorityWaiting.Load(),
		s.livePolicyMirrorPriorityActive.Load()
	backgroundWaiting, backgroundActive := s.livePolicyMirrorBackgroundWaiting.Load(),
		s.livePolicyMirrorBackgroundActive.Load()
	priorityOutstanding := s.livePolicyMirrorPriorityOutstanding.Load()
	backgroundOutstanding := s.livePolicyMirrorBackgroundOutstanding.Load()
	var navUSD, openMarkNetUSD, totalPnLUSD any
	if portfolio.NAVKnown {
		navUSD = portfolio.NAV
		openMarkNetUSD = portfolio.OpenMarkNet
		totalPnLUSD = portfolio.SettledNet + portfolio.OpenMarkNet
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"model_version": livePolicyMirrorModelVersion,
		"scope":         "current exact Kalshi LIVE System allowlist only: " + s.cfg().Risk.LiveSystemAllowlist,
		"independent":   true, "real_live_authority": false,
		"execution":        "same LIVE preflight; fixed 1.5s cash-first grace; configured 750ms-style delay; fresh full WS book; 100ms wire delay; IOC limit; visible-depth partial fills",
		"known_difference": "no real venue acknowledgement or hidden queue; zero/fill is inferred from the second full-book touch",
		"seed_usd":         portfolio.Meta.SeedUSD, "epoch_at": portfolio.Meta.EpochAt,
		"peak_nav_usd": portfolio.Meta.PeakNAVUSD,
		"cash_usd":     portfolio.Cash, "nav_usd": navUSD,
		"nav_known": portfolio.NAVKnown, "open_cost_usd": portfolio.OpenCost,
		"settled_net_usd": portfolio.SettledNet, "open_mark_net_usd": openMarkNetUSD,
		"net_pnl_usd": totalPnLUSD,
		"fees_usd":    portfolio.Fees, "day_turnover_usd": portfolio.Meta.DayTurnover,
		"candidates": portfolio.StateCounts[storage.LivePolicyMirrorRejected] +
			portfolio.StateCounts[storage.LivePolicyMirrorZeroFill] +
			portfolio.StateCounts[storage.LivePolicyMirrorFilled] +
			portfolio.StateCounts[storage.LivePolicyMirrorSettled],
		"states":           portfolio.StateCounts,
		"terminal_reasons": portfolio.ReasonCounts, "queue_dropped": dropped,
		"priority_queue_dropped": priorityDropped,
		"queue_channel_depth": map[string]int{
			"live_passed": priorityChannelDepth, "rejection_or_disarmed": backgroundChannelDepth,
		},
		"work_waiting": map[string]int64{
			"live_passed": priorityWaiting, "rejection_or_disarmed": backgroundWaiting,
		},
		"work_active": map[string]int64{
			"live_passed": priorityActive, "rejection_or_disarmed": backgroundActive,
		},
		"work_outstanding": map[string]int64{
			"live_passed": priorityOutstanding, "rejection_or_disarmed": backgroundOutstanding,
		},
		"recent": rows,
	})
}
