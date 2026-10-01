package server

// R153 same-cycle LIVE arbitration.
//
// A producer burst can contain mutually impossible contracts from one official occurrence. Queue
// order is not economic merit. We briefly coalesce the already-arriving burst, preflights each
// candidate against the current book/proof contract, ranks it by conservative fee-net return per
// capital-day, and dispatches the strongest compatible set. A successfully reserved/submitted bet
// wins permanently; later contradictory candidates are refused by the final account guard.

import (
	"context"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
)

// Ten milliseconds is enough to coalesce signals emitted by one producer pass without making a
// real-money taker sit behind a human-scale batching delay. The final handler still rejects
// incompatible account/event exposure, so this window ranks a burst; it is not a safety timeout.
const liveMirrorArbitrationWindow = 10 * time.Millisecond

const (
	// A LIVE-first candidate normally carries a complete current quote/proof receipt and finishes
	// ranking immediately. Slow legacy/Paper candidates may use this bounded background window, but
	// they cannot hold a ready real-money candidate behind the slowest book or database read.
	liveMirrorRankPreflightBudget = 150 * time.Millisecond
	liveMirrorRankReadyGrace      = 5 * time.Millisecond
	// A weaker exact-event sibling waits a little longer for a stronger preflight already in
	// flight. If that bound expires, the weaker row is withheld instead of winning by completion
	// order. Every withheld or unfinished row is requeued only after the canceled worker set has
	// fully joined, so the next pass can never overlap a duplicate preflight.
	liveMirrorRankSiblingGrace = 25 * time.Millisecond
)

type liveMirrorRankedCandidate struct {
	Candidate    liveMirrorCandidate
	Quote        liveMirrorQuote
	Descriptor   kalshiSemanticDescriptor
	Canary       bool
	Mean         float64
	Lower        float64
	FeePC        float64
	CapitalHours float64
	LowerPerDay  float64
	MeanPerDay   float64
}

func (s *Server) liveMirrorCapitalHours(c liveMirrorCandidate) float64 {
	if c.Platform == "kalshi" {
		if market, ok := s.kmkt(c.Ticker); ok {
			if end, parsed := fundedKalshiResultAt(market); parsed {
				hours := time.Until(end).Hours()
				if hours > 0 {
					return hours
				}
			}
		}
	}
	// Unknown time can never earn a speed bonus. The funded horizon independently fails closed
	// before dispatch; four hours is the most conservative ordinary-lane fallback inside that cap.
	return 4
}

func (s *Server) preflightLiveMirrorRank(ctx context.Context,
	c liveMirrorCandidate) (liveMirrorRankedCandidate, string) {
	if strings.EqualFold(strings.TrimSpace(c.Platform), "kalshi") {
		ctx = r159KalshiResidentAdmissionContext(ctx)
	}
	if !s.liveMirrorEnabled() {
		return liveMirrorRankedCandidate{}, "auto-live-off"
	}
	if c.At.IsZero() || time.Since(c.At) < -2*time.Second || time.Since(c.At) > liveMirrorTTL {
		return liveMirrorRankedCandidate{}, "stale-candidate"
	}
	q, basis, mean, lower, fee := c.ArbitrationQuote, c.ArbitrationBasis,
		c.ArbitrationMean, c.ArbitrationLower, c.ArbitrationFee
	reuse := s.liveMirrorArbitrationReceiptReusable(c, time.Now(), time.Second)
	if !reuse {
		if !s.paperEntryHorizonNow(ctx, c.Platform, c.Ticker, c.Title) {
			return liveMirrorRankedCandidate{}, "current-funded-entry-horizon-unavailable-or-exceeded"
		}
		var why string
		q, why = s.liveMirrorExecutable(ctx, c)
		if why != "" {
			return liveMirrorRankedCandidate{}, why
		}
		if c.Canary {
			plan, planWhy := s.liveOneContractCanaryReproof(ctx, c, q)
			if planWhy != "" || !plan.valid() || !plan.Canary {
				if planWhy == "" {
					planWhy = "one-contract-canary-current-reproof-invalid"
				}
				return liveMirrorRankedCandidate{}, planWhy
			}
			applyLiveProspectivePlan(&c, plan)
			basis, mean, lower, fee = plan.Basis, plan.Mean, plan.Lower, plan.ProofFeePC
		} else {
			ok, proofBasis, proofMean, proofLower, proofFee :=
				s.liveMirrorProof(ctx, c, q.Price, q.Maker)
			if !ok {
				return liveMirrorRankedCandidate{}, proofBasis
			}
			basis, mean, lower, fee = proofBasis, proofMean, proofLower, proofFee
		}
	}
	if _, newML := s.newMLV2Capability(c, ""); !newML {
		route := "taker"
		if q.Maker {
			route = "maker"
		}
		if why := s.liveSystemPostProofReason(c, route); why != "" {
			return liveMirrorRankedCandidate{}, why
		}
	}
	unitRisk := q.Price + math.Max(fee, 0)
	if unitRisk <= 0 || q.Depth <= 0 {
		return liveMirrorRankedCandidate{}, "arbitration-nonpositive-current-proof-or-depth"
	}
	if c.Canary {
		if mean+1e-12 < s.liveAllocationEdgeFloor() {
			return liveMirrorRankedCandidate{},
				"arbitration-one-contract-canary-point-below-live-floor"
		}
	} else if lower <= 0 {
		return liveMirrorRankedCandidate{}, "arbitration-nonpositive-current-proof-or-depth"
	}
	hours := math.Max(.25, s.liveMirrorCapitalHours(c))
	// Dispatch can reuse this just-computed read-only receipt for sizing. Kalshi's final handler
	// still owns the one fresh pre-wire quote/proof/account pass, so reusing it here removes a
	// duplicate network cycle without letting a stale price authorize an order.
	c.ArbitrationQuote = q
	c.ArbitrationBasis = basis
	c.ArbitrationMean = mean
	c.ArbitrationLower = lower
	c.ArbitrationFee = fee
	c.ArbitrationAt = time.Now()
	if c.Canary {
		if why := s.liveOneContractCanaryReceiptReason(c); why != "" {
			return liveMirrorRankedCandidate{}, why
		}
	}
	ranked := liveMirrorRankedCandidate{Candidate: c, Quote: q, Canary: c.Canary,
		Mean: mean, Lower: lower,
		FeePC: fee, CapitalHours: hours,
		LowerPerDay: lower / unitRisk * 24 / hours,
		MeanPerDay:  mean / unitRisk * 24 / hours}
	return ranked, ""
}

func strongerLiveMirrorRank(a, b liveMirrorRankedCandidate) bool {
	// Positive-lower-bound candidates always outrank experimental canaries. Among canaries, the
	// actual lower remains diagnostic and may be negative; current point profit-rate ranks them.
	if a.Canary != b.Canary {
		return !a.Canary
	}
	if a.Canary {
		if math.Abs(a.MeanPerDay-b.MeanPerDay) > 1e-12 {
			return a.MeanPerDay > b.MeanPerDay
		}
	} else {
		if math.Abs(a.LowerPerDay-b.LowerPerDay) > 1e-12 {
			return a.LowerPerDay > b.LowerPerDay
		}
		if math.Abs(a.MeanPerDay-b.MeanPerDay) > 1e-12 {
			return a.MeanPerDay > b.MeanPerDay
		}
	}
	// Cross-match is preferred only after conservative and mean fee-net profit rates are tied.
	// It never displaces a materially stronger executable candidate merely because of its label.
	if preferA, decided := liveMirrorXMatchPreferred(a, b); decided {
		return preferA
	}
	if math.Abs(a.Quote.Depth-b.Quote.Depth) > 1e-9 {
		return a.Quote.Depth > b.Quote.Depth
	}
	if !a.Candidate.At.Equal(b.Candidate.At) {
		return a.Candidate.At.Before(b.Candidate.At) // first observed is the true tie-breaker
	}
	return a.Candidate.key() < b.Candidate.key()
}

func liveMirrorRankSetCompatible(already []liveMirrorRankedCandidate,
	candidate liveMirrorRankedCandidate) (bool, string) {
	if candidate.Candidate.Platform != "kalshi" {
		return true, ""
	}
	group := []objectiveidentity.PurchasedPredicate{candidate.Descriptor.Predicate}
	for _, accepted := range already {
		if accepted.Candidate.Platform != "kalshi" {
			continue
		}
		if strings.EqualFold(accepted.Descriptor.EventTicker, candidate.Descriptor.EventTicker) {
			return false, "same-cycle-native-event-conflict"
		}
		if accepted.Descriptor.Milestone.ID == "" ||
			accepted.Descriptor.Milestone.ID != candidate.Descriptor.Milestone.ID {
			continue
		}
		group = append(group, accepted.Descriptor.Predicate)
	}
	if len(group) < 2 {
		return true, ""
	}
	verdict := objectiveidentity.EvaluatePurchasedSet(group)
	if verdict.Contradictory {
		return false, "same-cycle-logical-payoff-conflict"
	}
	if verdict.ReasonCode == objectiveidentity.IntersectionUnclassified {
		return false, "same-cycle-related-payoff-unclassified"
	}
	return true, ""
}

func (s *Server) popLiveMirrorBatch(limit int) []liveMirrorCandidate {
	if limit <= 0 {
		return nil
	}
	out := make([]liveMirrorCandidate, 0, limit)
	for len(out) < limit {
		candidate, ok := s.popLiveMirrorCandidate()
		if !ok {
			break
		}
		out = append(out, candidate)
	}
	return out
}

func (s *Server) requeueLiveMirrorBatch(candidates []liveMirrorCandidate) {
	if len(candidates) == 0 {
		return
	}
	now := time.Now()
	recordGenerationDrop := func(candidate liveMirrorCandidate, at time.Time) {
		why := s.liveIntentGenerationGateReason(true, candidate.LiveIntentGeneration,
			candidate.LiveIntentGenerationBound, true)
		if why == "" {
			why = liveIntentGenerationChangedReason
		}
		s.recordAdmittedLiveCandidateDrop(candidate, "AUTO-LIVE-MIRROR-DROP",
			"arbitration-requeue", why, at, nil)
	}
	if !s.liveMirrorEnabled() {
		for _, candidate := range candidates {
			s.recordAdmittedLiveCandidateDrop(candidate, "AUTO-LIVE-MIRROR-DROP", "arbitration-requeue",
				"live-runtime-gate-disabled-before-requeue", now, nil)
		}
		return
	}
	kept := make([]liveMirrorCandidate, 0, len(candidates))
	var staleGeneration []liveMirrorCandidate
	for _, candidate := range candidates {
		if why := s.liveIntentGenerationGateReason(true, candidate.LiveIntentGeneration,
			candidate.LiveIntentGenerationBound, true); why != "" {
			staleGeneration = append(staleGeneration, candidate)
		} else if now.Sub(candidate.At) <= liveMirrorTTL {
			kept = append(kept, candidate)
		} else {
			s.logLiveMirrorExpiry(candidate, now, "arbitration-requeue")
		}
	}
	for _, candidate := range staleGeneration {
		recordGenerationDrop(candidate, now)
	}
	if len(kept) == 0 {
		return
	}
	s.liveMirrorMu.Lock()
	// OFF/ON advances the generation before taking this same queue lock. Recheck here so a batch
	// that was popped into arbitration cannot prepend itself after the clean-session boundary.
	current := kept[:0]
	staleGeneration = staleGeneration[:0]
	for _, candidate := range kept {
		if why := s.liveIntentGenerationGateReason(true, candidate.LiveIntentGeneration,
			candidate.LiveIntentGenerationBound, true); why != "" {
			staleGeneration = append(staleGeneration, candidate)
			continue
		}
		current = append(current, candidate)
	}
	kept = current
	combined := append(kept, s.liveMirrorQ...)
	var evicted []liveMirrorCandidate
	if len(combined) > liveMirrorQueueCap {
		evicted = append(evicted, combined[liveMirrorQueueCap:]...)
		combined = combined[:liveMirrorQueueCap]
	}
	s.liveMirrorQ = combined
	s.liveMirrorMu.Unlock()
	if len(kept) > 0 {
		s.wakeLiveMirrorDispatch()
	}
	for _, candidate := range staleGeneration {
		recordGenerationDrop(candidate, time.Now())
	}
	for _, candidate := range evicted {
		s.recordLiveCandidateDrop(candidate, "AUTO-LIVE-MIRROR-DROP", "arbitration-requeue",
			"queue-cap-evicted", now, map[string]any{"queue_capacity": liveMirrorQueueCap})
	}
}

func (s *Server) logLiveMirrorArbitrationDrop(c liveMirrorCandidate, stage, reason string) {
	reason = strings.TrimSpace(reason)
	handlerObservation := reason == "duplicate-or-unpersisted-claim" ||
		reason == "authoritative-zero-fill" ||
		strings.HasPrefix(reason, "live-handler:")
	if stage == "final-dispatch" && handlerObservation &&
		s.executionShadowHandlerTerminalSeen(c.ShadowAttemptID) {
		s.recordLiveCandidateHousekeeping(c, "AUTO-LIVE-MIRROR-DROP",
			stage, reason, time.Now(), map[string]any{
				"duplicate_of_handler_terminal": true,
			})
		return
	}
	s.recordLiveCandidateDrop(c, "AUTO-LIVE-MIRROR-DROP", stage, reason, time.Now(), nil)
}

type liveMirrorPreflightResult struct {
	row       liveMirrorRankedCandidate
	candidate liveMirrorCandidate
	why       string
	index     int
}

func (s *Server) liveMirrorArbitrationEventKey(candidate liveMirrorCandidate) string {
	if !strings.EqualFold(strings.TrimSpace(candidate.Platform), "kalshi") {
		return ""
	}
	event := ""
	if market, ok := s.kmkt(candidate.Ticker); ok {
		event = market.EventTicker
	}
	if strings.TrimSpace(event) == "" {
		// This is only a conservative wait-group proxy; final money authority still requires the
		// venue's exact event_ticker. Over-grouping can delay 25ms but can never authorize a bet.
		event = eventTickerOf(candidate.Ticker)
	}
	return strings.ToUpper(strings.TrimSpace(event))
}

func strongerLiveMirrorPreflightProxy(a, b liveMirrorCandidate) bool {
	aRisk := a.ArbitrationQuote.Price + math.Max(a.ArbitrationFee, 0)
	bRisk := b.ArbitrationQuote.Price + math.Max(b.ArbitrationFee, 0)
	if a.Canary && b.Canary && aRisk > 0 && bRisk > 0 &&
		a.ArbitrationMean > 0 && b.ArbitrationMean > 0 {
		return a.ArbitrationMean/aRisk > b.ArbitrationMean/bRisk
	}
	if a.ArbitrationLower > 0 && b.ArbitrationLower > 0 && aRisk > 0 && bRisk > 0 {
		return a.ArbitrationLower/aRisk > b.ArbitrationLower/bRisk
	}
	if a.ArbitrationLower > 0 || b.ArbitrationLower > 0 {
		return a.ArbitrationLower > 0
	}
	if a.Canary != b.Canary {
		return !a.Canary
	}
	return a.At.Before(b.At)
}

func liveMirrorPendingStrongerSibling(result liveMirrorPreflightResult, done []bool,
	priority []int, eventKeys []string) bool {
	if result.index < 0 || result.index >= len(done) || eventKeys[result.index] == "" {
		return false
	}
	for index := range done {
		if !done[index] && eventKeys[index] == eventKeys[result.index] &&
			priority[index] < priority[result.index] {
			return true
		}
	}
	return false
}

func (s *Server) preflightLiveMirrorBatch(ctx context.Context,
	batch []liveMirrorCandidate) ([]liveMirrorPreflightResult, []liveMirrorCandidate) {
	return s.preflightLiveMirrorBatchWith(ctx, batch, s.preflightLiveMirrorRank)
}

type liveMirrorRankPreflightFunc func(context.Context,
	liveMirrorCandidate) (liveMirrorRankedCandidate, string)

func (s *Server) preflightLiveMirrorBatchWith(ctx context.Context, batch []liveMirrorCandidate,
	preflight liveMirrorRankPreflightFunc) ([]liveMirrorPreflightResult, []liveMirrorCandidate) {
	if len(batch) == 0 {
		return nil, nil
	}
	if preflight == nil {
		return nil, append([]liveMirrorCandidate(nil), batch...)
	}
	type indexedCandidate struct {
		index     int
		candidate liveMirrorCandidate
	}
	// Start candidates carrying the strongest already-proved lower return first. This is only
	// worker priority; the completed set is still ranked by the canonical fee-net profit rate.
	indexed := make([]indexedCandidate, 0, len(batch))
	for i, candidate := range batch {
		indexed = append(indexed, indexedCandidate{index: i, candidate: candidate})
	}
	sort.SliceStable(indexed, func(i, j int) bool {
		return strongerLiveMirrorPreflightProxy(indexed[i].candidate, indexed[j].candidate)
	})
	priority := make([]int, len(batch))
	eventKeys := make([]string, len(batch))
	for rank, candidate := range indexed {
		priority[candidate.index] = rank
		eventKeys[candidate.index] = s.liveMirrorArbitrationEventKey(candidate.candidate)
	}
	preflightCtx, cancel := context.WithCancel(ctx)
	jobs := make(chan indexedCandidate)
	results := make(chan liveMirrorPreflightResult, len(batch))
	workers := liveSignalPreflightWorkers
	if workers > len(batch) {
		workers = len(batch)
	}
	var pipeline sync.WaitGroup
	pipeline.Add(workers + 1)
	for i := 0; i < workers; i++ {
		go func() {
			defer pipeline.Done()
			for job := range jobs {
				row, why := preflight(preflightCtx, job.candidate)
				results <- liveMirrorPreflightResult{row: row, candidate: job.candidate, why: why, index: job.index}
			}
		}()
	}
	go func() {
		defer pipeline.Done()
		defer close(jobs)
		for _, job := range indexed {
			select {
			case <-preflightCtx.Done():
				return
			case jobs <- job:
			}
		}
	}()
	out := make([]liveMirrorPreflightResult, 0, len(batch))
	done := make([]bool, len(batch))
	budget := time.NewTimer(liveMirrorRankPreflightBudget)
	defer budget.Stop()
	var readyTimer *time.Timer
	var ready <-chan time.Time
	var readyDeadline time.Time
	extendReady := func(delay time.Duration) {
		deadline := time.Now().Add(delay)
		if readyTimer != nil && !deadline.After(readyDeadline) {
			return
		}
		if readyTimer == nil {
			readyTimer = time.NewTimer(delay)
		} else {
			if !readyTimer.Stop() {
				select {
				case <-readyTimer.C:
				default:
				}
			}
			readyTimer.Reset(time.Until(deadline))
		}
		readyDeadline, ready = deadline, readyTimer.C
	}
	stop := func() {
		cancel()
		if readyTimer != nil && !readyTimer.Stop() {
			select {
			case <-readyTimer.C:
			default:
			}
		}
	}
	defer stop()
	for len(out) < len(batch) {
		select {
		case result := <-results:
			if result.index >= 0 && result.index < len(done) {
				done[result.index] = true
			}
			out = append(out, result)
			if result.why == "" {
				grace := liveMirrorRankReadyGrace
				if liveMirrorPendingStrongerSibling(result, done, priority, eventKeys) {
					grace = liveMirrorRankSiblingGrace
				}
				extendReady(grace)
			}
		case <-ready:
			goto finished
		case <-budget.C:
			goto finished
		case <-ctx.Done():
			goto finished
		}
	}
finished:
	// Once the ready/budget boundary owns a result, stop admitting more work and join every
	// in-flight worker before handing any unfinished candidate back to the queue. Results that
	// complete after this boundary are intentionally retried from a fresh batch rather than being
	// treated as terminal context-cancellation failures.
	cancel()
	blocked := make(map[int]bool)
	for _, result := range out {
		if result.why == "" &&
			liveMirrorPendingStrongerSibling(result, done, priority, eventKeys) {
			blocked[result.index] = true
		}
	}
	pipeline.Wait()

	filtered := out[:0]
	completed := make([]bool, len(batch))
	for _, result := range out {
		if blocked[result.index] {
			continue
		}
		filtered = append(filtered, result)
		if result.index >= 0 && result.index < len(completed) {
			completed[result.index] = true
		}
	}
	requeue := make([]liveMirrorCandidate, 0, len(batch)-len(filtered))
	for i, candidate := range batch {
		if !completed[i] {
			requeue = append(requeue, candidate)
		}
	}
	return filtered, requeue
}

func (s *Server) consumeRankedLiveMirrorCandidates(ctx context.Context, maxPlaced int) int {
	return s.consumeRankedLiveMirrorCandidatesWindow(ctx, maxPlaced, true)
}

// consumeRankedLiveMirrorCandidatesReady is used only after MonitorLiveAuto already spent the one
// shared 10ms window draining raw signal intents. Sleeping again would add latency without
// admitting another bounded upstream batch into this ranking decision.
func (s *Server) consumeRankedLiveMirrorCandidatesReady(ctx context.Context, maxPlaced int) int {
	return s.consumeRankedLiveMirrorCandidatesWindow(ctx, maxPlaced, false)
}

func liveMirrorShouldRequeueTransient(candidate liveMirrorCandidate, reason string, now time.Time) bool {
	age := now.Sub(candidate.At)
	return age >= 0 && age <= liveMirrorTTL &&
		strings.Contains(reason, "live-handler:automatic safety pause before venue submission:")
}

func (s *Server) consumeRankedLiveMirrorCandidatesWindow(ctx context.Context, maxPlaced int, wait bool) int {
	if maxPlaced <= 0 {
		return 0
	}
	if wait {
		select {
		case <-ctx.Done():
			return 0
		case <-time.After(liveMirrorArbitrationWindow):
		}
	}
	// The mirror queue is itself bounded. Rank the complete decision-window set, not the first
	// eight arrivals; otherwise a stronger ninth same-game candidate can be permanently blocked by
	// a weaker order that happened to enter the channel first. Preflight uses a bounded worker pool.
	batch := s.popLiveMirrorBatch(liveMirrorQueueCap)
	ranked := make([]liveMirrorRankedCandidate, 0, len(batch))
	results, unfinished := s.preflightLiveMirrorBatch(ctx, batch)
	s.requeueLiveMirrorBatch(unfinished)
	for _, result := range results {
		if result.why != "" {
			s.logLiveMirrorArbitrationDrop(result.candidate, "arbitration-preflight", result.why)
			continue
		}
		ranked = append(ranked, result.row)
	}
	sort.SliceStable(ranked, func(i, j int) bool { return strongerLiveMirrorRank(ranked[i], ranked[j]) })
	placed := 0
	accepted := make([]liveMirrorRankedCandidate, 0, maxPlaced)
	deferred := make([]liveMirrorCandidate, 0, len(ranked))
	for _, candidate := range ranked {
		if placed >= maxPlaced {
			deferred = append(deferred, candidate.Candidate)
			continue
		}
		if candidate.Candidate.Platform == "kalshi" {
			// Exact event identity is always required from the current complete board. Broader
			// Event/Milestone semantics enrich same-cycle compatibility only when already resident;
			// the serial final guard fails closed on any later cross-event exposure whose semantic
			// cache is unavailable, so a cold cache cannot block the first standalone pick.
			if descriptor, ok := s.r159KalshiResidentSemanticDescriptor(
				candidate.Candidate.Ticker, candidate.Candidate.Side); ok {
				candidate.Descriptor = descriptor
			} else if market, ok := s.r159KalshiResidentMarket(candidate.Candidate.Ticker); ok {
				candidate.Descriptor = kalshiSemanticDescriptor{
					Ticker:      candidate.Candidate.Ticker,
					EventTicker: strings.ToUpper(strings.TrimSpace(market.EventTicker)),
				}
			}
			if candidate.Descriptor.EventTicker == "" {
				s.logLiveMirrorArbitrationDrop(candidate.Candidate, "arbitration-compatibility",
					"kalshi-payoff-identity-unavailable")
				continue
			}
		}
		if compatible, why := liveMirrorRankSetCompatible(accepted, candidate); !compatible {
			s.logLiveMirrorArbitrationDrop(candidate.Candidate, "arbitration-compatibility",
				why+":stronger-current-candidate-selected")
			continue
		}
		if yes, why := s.dispatchLiveMirror(ctx, candidate.Candidate); yes {
			placed++
			accepted = append(accepted, candidate)
		} else if liveMirrorShouldRequeueTransient(candidate.Candidate, why, time.Now()) {
			// The previous order briefly owned the global reconciliation belt. This candidate made
			// no venue call and remains economically fresh, so preserve it for the recovery pass
			// already scheduled by MonitorLiveAuto instead of converting a millisecond race into a
			// permanent missed bet.
			s.requeueLiveMirrorBatch([]liveMirrorCandidate{candidate.Candidate})
		} else {
			// A terminal IOC zero-fill belongs to this immutable detector attempt and is never
			// retried under the same id. A later detector opportunity may emit a fresh attempt.
			// Other duplicate/unpersisted claims remain actual final-dispatch refusals. The accepted
			// money path has its own durable risk/fill receipt; this records only housekeeping when
			// the handler already wrote the terminal.
			s.logLiveMirrorArbitrationDrop(candidate.Candidate, "final-dispatch", why)
		}
	}
	s.requeueLiveMirrorBatch(deferred)
	return placed
}
