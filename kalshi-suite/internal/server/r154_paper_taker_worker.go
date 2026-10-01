package server

// R154 Paper taker realism. LIVE receives every measured-positive signal first. The funded Paper
// follower then runs on one bounded serial executor, so its delayed book reads and main-ledger
// writes cannot multiply into resource contention with the real-money lane.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	genfollowPaperIntentCapacity = 512
	// Funded Paper owns one main-ledger writer. Raw event traffic never enters this worker:
	// event-driven signals are fanned out only after the cash candidate passes preflight.
	genfollowPaperWorkerCount      = 1
	genfollowPaperWorkTimeout      = 30 * time.Second
	genfollowPaperLiveFirstGrace   = 1500 * time.Millisecond
	genfollowPaperLiveQuietTimeout = 5 * time.Second
	genfollowPaperTerminalTimeout  = 5 * time.Second
	genfollowPaperSignalMaxAge     = 25 * time.Second

	defaultPaperTakerDelay = 750 * time.Millisecond
	minPaperTakerDelay     = 25 * time.Millisecond
	maxPaperTakerDelay     = 5 * time.Second
)

type genfollowPaperIntent struct {
	Signals    []genfollowPaperSignal
	EnqueuedAt time.Time
}

type genfollowPaperSignal struct {
	Signal          storage.Signal
	SignalAt        time.Time
	ShadowAttemptID string
	LiveTerminal    fundedPaperLiveTerminalReceipt
	VenuePriority   bool
	// onTerminal advances durable research-candidate state after the delayed Paper branch records
	// its result under the shared LIVE/Paper/Shadow attempt. It runs exactly once after the delayed
	// executor reaches a market terminal (fill, zero-fill, rejection, or not-observed). Queue
	// admission failure remains the caller's responsibility because enqueueGenfollowPaperIntent
	// returns it synchronously.
	preflight  func(context.Context) string
	onTerminal func(context.Context, genfollowPaperTerminalResult)
	testRun    func(context.Context) // hermetic bounded-throughput regression seam
}

// genfollowPaperTerminalResult is the executor's actual delayed-book outcome. In particular,
// PAPER-FILLED is the only accepted simulated order; an attempted order that later loses its book
// is a zero-fill, never an assumed fill.
type genfollowPaperTerminalResult struct {
	State, Reason, AttemptID, ShadowAttemptID, FeeSource string
	AttemptedContracts, LimitPrice                       float64
	FilledContracts, FillPrice, Fee                      float64
}

func completeQueuedGenfollowPaperSignal(queued genfollowPaperSignal,
	result genfollowPaperTerminalResult) {
	if queued.onTerminal == nil {
		return
	}
	if strings.TrimSpace(result.ShadowAttemptID) == "" {
		result.ShadowAttemptID = strings.TrimSpace(queued.ShadowAttemptID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queued.onTerminal(ctx, result)
}

func queuedGenfollowPaperTerminalDetail(queued genfollowPaperSignal,
	result genfollowPaperTerminalResult, at time.Time) map[string]any {
	signalAt := queued.SignalAt
	if signalAt.IsZero() {
		signalAt = at
	}
	topology := r147SignalInputTopology(queued.Signal)
	contractID := ""
	if contract, _, declared := gfSignalContractBinding(queued.Signal); declared {
		contractID = r147SignalContractIdentity(contract)
		if topology == "" {
			topology = contract.InputTopology
		}
	}
	return map[string]any{
		"system": queued.Signal.SignalType, "venue": strings.ToLower(strings.TrimSpace(queued.Signal.Platform)),
		"ticker": queued.Signal.Ticker, "side": strings.ToUpper(strings.TrimSpace(queued.Signal.Side)),
		"route": "taker", "input_topology": topology, "signal_contract": contractID,
		"state": result.State, "reason": result.Reason, "order_result": result.State,
		"signal_at":   signalAt.UTC().Format(time.RFC3339Nano),
		"terminal_at": at.UTC().Format(time.RFC3339Nano),
	}
}

// completeQueuedGenfollowPaperBeforeExecution gives every signal that leaves before the normal
// delayed executor one exact runtime terminal and one durable receipt. The normal executor already
// writes its own full system-route audit, so callers use this helper only for scheduler, expiry,
// shutdown, and final-preflight exits.
func (s *Server) completeQueuedGenfollowPaperBeforeExecution(queued genfollowPaperSignal,
	result genfollowPaperTerminalResult) {
	at := time.Now().UTC()
	s.noteR147PaperRouteOutcome(queued.Signal, "taker", result.State, result.Reason, at)
	if s.store != nil {
		detail, _ := json.Marshal(queuedGenfollowPaperTerminalDetail(queued, result, at))
		auditCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = s.store.Audit(auditCtx, "info", "system-route",
			fmt.Sprintf("%s %s %s %s: %s", result.State, queued.Signal.SignalType,
				strings.ToLower(strings.TrimSpace(queued.Signal.Platform)), queued.Signal.Ticker,
				result.Reason), string(detail))
		cancel()
	}
	completeQueuedGenfollowPaperSignal(queued, result)
}

// recordGenfollowPaperAdmissionFailure preserves every member of a dropped batch in one bounded
// durable receipt after LIVE publication. It deliberately does not invoke onTerminal: callers of
// enqueueGenfollowPaperIntent already own synchronous admission-failure cleanup.
func (s *Server) recordGenfollowPaperAdmissionFailure(batch []genfollowPaperSignal,
	reason string, at time.Time) {
	if len(batch) == 0 {
		return
	}
	state := "PAPER-NOT-OBSERVED"
	rows := make([]map[string]any, 0, len(batch))
	for _, queued := range batch {
		result := genfollowPaperTerminalResult{State: state, Reason: reason}
		s.noteR147PaperRouteOutcome(queued.Signal, "taker", state, reason, at)
		rows = append(rows, queuedGenfollowPaperTerminalDetail(queued, result, at))
	}
	if s.store == nil {
		return
	}
	detail, _ := json.Marshal(map[string]any{
		"reason": reason, "order_result": state, "signals": rows,
		"signal_count": len(rows), "live_impact": "none; LIVE intents were published first",
	})
	auditCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_ = s.store.Audit(auditCtx, "warn", "paper-taker-attempt",
		fmt.Sprintf("Paper taker batch not observed: %s (%d exact signal(s))", reason, len(rows)),
		string(detail))
	cancel()
}

// genfollowPaperTakerDelay is file-configurable but bounded. Zero keeps the conservative default;
// a tiny positive value remains useful for hermetic tests without allowing production to collapse
// the second-book read back into an effectively instant fantasy fill.
func (s *Server) genfollowPaperTakerDelay() time.Duration {
	ms := s.cfg().Auto.PaperTakerDelayMS
	if ms <= 0 {
		return defaultPaperTakerDelay
	}
	delay := time.Duration(ms) * time.Millisecond
	if delay < minPaperTakerDelay {
		return minPaperTakerDelay
	}
	if delay > maxPaperTakerDelay {
		return maxPaperTakerDelay
	}
	return delay
}

// enqueueGenfollowPaperIntent is deliberately nonblocking. The caller has already published every
// LIVE intent in this batch. Candidate order is preserved inside the batch so the stronger exact
// route still gets first claim on the Paper market-level duplicate guard.
func (s *Server) enqueueGenfollowPaperIntent(signals []genfollowPaperSignal) bool {
	if len(signals) == 0 {
		return false
	}
	copied := append([]genfollowPaperSignal(nil), signals...)
	now := time.Now().UTC()
	for i := range copied {
		if copied[i].SignalAt.IsZero() {
			copied[i].SignalAt = now
		}
	}

	s.genfollowPaperWorkerMu.Lock()
	defer s.genfollowPaperWorkerMu.Unlock()
	if s.genfollowPaperScheduled == nil {
		s.genfollowPaperScheduled = make(map[string]time.Time)
	}
	if s.genfollowPaperCompleted == nil {
		s.genfollowPaperCompleted = make(map[string]struct{})
	}
	for attemptID, seen := range s.genfollowPaperScheduled {
		if now.Sub(seen) > liveMirrorClaimTTL {
			delete(s.genfollowPaperScheduled, attemptID)
		}
	}
	filtered := copied[:0]
	for _, queued := range copied {
		attemptID := strings.TrimSpace(queued.ShadowAttemptID)
		if attemptID != "" {
			if _, completed := s.genfollowPaperCompleted[attemptID]; completed {
				continue
			}
			if seen, ok := s.genfollowPaperScheduled[attemptID]; ok &&
				now.Sub(seen) >= 0 && now.Sub(seen) <= liveMirrorClaimTTL {
				continue
			}
			s.genfollowPaperScheduled[attemptID] = now
		}
		filtered = append(filtered, queued)
	}
	copied = filtered
	if len(copied) == 0 {
		return false
	}
	if s.genfollowPaperWorkerStopping {
		for _, queued := range copied {
			delete(s.genfollowPaperScheduled, strings.TrimSpace(queued.ShadowAttemptID))
			s.executionShadowRecordPaperScheduler(queued.ShadowAttemptID,
				"paper-worker-stopping", time.Now().UTC())
		}
		s.recordGenfollowPaperAdmissionFailure(copied, "paper-worker-stopping", time.Now().UTC())
		return false
	}
	if s.genfollowPaperIntentCh == nil {
		capacity := s.genfollowPaperWorkerCapacity
		if capacity <= 0 {
			capacity = genfollowPaperIntentCapacity
		}
		s.genfollowPaperIntentCh = make(chan genfollowPaperIntent, capacity)
		s.genfollowPaperPriorityCh = make(chan genfollowPaperIntent, capacity)
		s.genfollowPaperWorkerDone = make(chan struct{})
		workerCtx, cancel := context.WithCancel(context.Background())
		s.genfollowPaperWorkerCancel = cancel
		go s.runGenfollowPaperPriorityWorkers(workerCtx, s.genfollowPaperPriorityCh,
			s.genfollowPaperIntentCh, s.genfollowPaperWorkerDone)
	}
	prioritySignals := make([]genfollowPaperSignal, 0, len(copied))
	backgroundSignals := make([]genfollowPaperSignal, 0, len(copied))
	for _, queued := range copied {
		if queued.VenuePriority ||
			queued.LiveTerminal.Kind == fundedPaperLiveTerminalFill ||
			queued.LiveTerminal.Kind == fundedPaperLiveTerminalZeroFill {
			prioritySignals = append(prioritySignals, queued)
		} else {
			backgroundSignals = append(backgroundSignals, queued)
		}
	}
	send := func(batch []genfollowPaperSignal, target chan genfollowPaperIntent) bool {
		if len(batch) == 0 {
			return true
		}
		select {
		case target <- genfollowPaperIntent{Signals: batch, EnqueuedAt: now}:
			return true
		default:
			dropped := s.genfollowPaperWorkerDropped.Add(1)
			for _, queued := range batch {
				delete(s.genfollowPaperScheduled, strings.TrimSpace(queued.ShadowAttemptID))
				s.executionShadowRecordPaperScheduler(queued.ShadowAttemptID,
					"bounded-paper-worker-queue-full", time.Now().UTC())
			}
			s.recordGenfollowPaperAdmissionFailure(batch, "bounded-paper-worker-queue-full", time.Now().UTC())
			if dropped == 1 && s.log != nil {
				s.log.Warn("Paper taker worker queue full; LIVE stayed unblocked and the dropped Paper batch will be audited",
					"ticker", batch[0].Signal.Ticker, "signals", len(batch))
			}
			return false
		}
	}
	priorityTarget := s.genfollowPaperPriorityCh
	if priorityTarget == nil {
		priorityTarget = s.genfollowPaperIntentCh
	}
	priorityOK := send(prioritySignals, priorityTarget)
	backgroundOK := send(backgroundSignals, s.genfollowPaperIntentCh)
	return priorityOK && backgroundOK
}

func (s *Server) releaseGenfollowPaperScheduled(attemptID string) {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return
	}
	s.genfollowPaperWorkerMu.Lock()
	delete(s.genfollowPaperScheduled, attemptID)
	s.genfollowPaperWorkerMu.Unlock()
}

func (s *Server) claimGenfollowPaperScheduled(attemptID string) bool {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return false
	}
	now := time.Now().UTC()
	s.genfollowPaperWorkerMu.Lock()
	defer s.genfollowPaperWorkerMu.Unlock()
	if s.genfollowPaperScheduled == nil {
		s.genfollowPaperScheduled = make(map[string]time.Time)
	}
	if s.genfollowPaperCompleted == nil {
		s.genfollowPaperCompleted = make(map[string]struct{})
	}
	if _, completed := s.genfollowPaperCompleted[attemptID]; completed {
		return false
	}
	if seen, ok := s.genfollowPaperScheduled[attemptID]; ok &&
		now.Sub(seen) >= 0 && now.Sub(seen) <= liveMirrorClaimTTL {
		return false
	}
	s.genfollowPaperScheduled[attemptID] = now
	return true
}

func (s *Server) completeGenfollowPaperScheduled(attemptID string) {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return
	}
	s.genfollowPaperWorkerMu.Lock()
	if s.genfollowPaperCompleted == nil {
		s.genfollowPaperCompleted = make(map[string]struct{})
	}
	delete(s.genfollowPaperScheduled, attemptID)
	s.genfollowPaperCompleted[attemptID] = struct{}{}
	s.genfollowPaperWorkerMu.Unlock()
}

// recoverFundedPaperGaps replays only durable LIVE-preflight offers. The original trigger clock is
// retained, so a restart can produce an honest not-observed terminal but can never turn a stale
// historical quote into a new fake fill.
func (s *Server) recoverFundedPaperGaps(ctx context.Context) {
	if s == nil || s.store == nil || ctx.Err() != nil {
		return
	}
	// Historical replay must never compete with the real-money lane. Fresh same-attempt Paper
	// work is scheduled directly by the detector/preflight path and does not pass through here.
	s.liveMu.Lock()
	liveAuto := s.liveAuto
	s.liveMu.Unlock()
	if liveAuto {
		return
	}
	rows, err := s.store.ListExecutionShadowFundedPaperGaps(ctx, 64)
	if err != nil {
		if s.log != nil && s.gfRouteAuditDue(
			"r164-funded-paper-gap-recovery", err.Error(), time.Now()) {
			s.log.Warn("funded Paper comparison recovery delayed; durable gaps retained",
				"err", err)
		}
		return
	}
	for _, row := range rows {
		s.liveMu.Lock()
		liveAuto = s.liveAuto
		s.liveMu.Unlock()
		if liveAuto {
			return
		}
		s.executionShadowRememberTrigger(row.Attempt.AttemptID, row.Attempt.TriggerUnixMS)
		if lot, found := s.fundedPaperLotForExecutionAttempt(row.Attempt.AttemptID); found {
			if !s.claimGenfollowPaperScheduled(row.Attempt.AttemptID) {
				continue
			}
			if reason := fundedPaperRecoveryLotReason(lot, row.Attempt); reason != "" {
				s.executionShadowRecordPaperAttempt(row.Attempt.AttemptID,
					"PAPER-NOT-OBSERVED", reason, "", time.Now().UTC(),
					0, 0, 0, 0, 0, "", storage.Signal{})
				continue
			}
			book := storage.Signal{
				Platform: firstNonEmpty(lot.Platform, row.Attempt.Venue),
				Ticker:   lot.Ticker, Title: lot.Title, Side: lot.Side,
				SignalType: row.Attempt.SystemID, EntryPrice: lot.Price,
				BookSource: lot.ExecutionBookSource,
			}
			s.executionShadowRecordPaperAttempt(row.Attempt.AttemptID,
				"PAPER-FILLED", "recovered-existing-durable-funded-paper-lot",
				"recovered-lot:"+lot.TS, time.Now().UTC(), lot.Contracts, lot.Price,
				lot.Contracts, lot.Price, lot.Fee, lot.FeeSource, book)
			continue
		}
		signalAt := row.Attempt.ObservedAt
		if row.Attempt.TriggerUnixMS > 0 {
			signalAt = time.UnixMilli(row.Attempt.TriggerUnixMS).UTC()
		}
		if age := time.Since(signalAt); age > genfollowPaperSignalMaxAge {
			if !s.claimGenfollowPaperScheduled(row.Attempt.AttemptID) {
				continue
			}
			s.executionShadowRecordPaperAttempt(row.Attempt.AttemptID,
				"PAPER-NOT-OBSERVED", "paper-original-signal-expired-before-recovery", "",
				time.Now().UTC(), 0, 0, 0, 0, 0, "", storage.Signal{})
			continue
		}
		var sig storage.Signal
		haveSignal := false
		for _, event := range row.Events {
			if event.Stage != "funded-paper-branch" || event.Outcome != "offered" {
				continue
			}
			if payload, ok := event.Evidence["paper_signal_json"].(string); ok {
				if json.Unmarshal([]byte(payload), &sig) == nil &&
					strings.TrimSpace(sig.Ticker) != "" {
					haveSignal = true
				}
				continue
			}
			// Backward-compatible replay for R163/R164 rows written before the payload was
			// pre-encoded. New rows never place the struct itself in retained evidence.
			raw, ok := event.Evidence["paper_signal"]
			if !ok {
				continue
			}
			encoded, marshalErr := json.Marshal(raw)
			if marshalErr == nil && json.Unmarshal(encoded, &sig) == nil &&
				strings.TrimSpace(sig.Ticker) != "" {
				haveSignal = true
			}
		}
		if !haveSignal {
			if !s.claimGenfollowPaperScheduled(row.Attempt.AttemptID) {
				continue
			}
			s.executionShadowRecordPaperAttempt(row.Attempt.AttemptID,
				"PAPER-NOT-OBSERVED", "durable-paper-signal-unavailable", "",
				time.Now().UTC(), 0, 0, 0, 0, 0, "", storage.Signal{})
			continue
		}
		if reason := fundedPaperRecoverySignalReason(sig, row.Attempt); reason != "" {
			if !s.claimGenfollowPaperScheduled(row.Attempt.AttemptID) {
				continue
			}
			s.executionShadowRecordPaperAttempt(row.Attempt.AttemptID,
				"PAPER-NOT-OBSERVED", reason, "", time.Now().UTC(),
				0, 0, 0, 0, 0, "", storage.Signal{})
			continue
		}
		s.enqueueGenfollowPaperIntent([]genfollowPaperSignal{{
			Signal: sig, SignalAt: signalAt, ShadowAttemptID: row.Attempt.AttemptID,
		}})
	}
}

func fundedPaperRecoverySignalReason(sig storage.Signal,
	attempt storage.ExecutionShadowAttempt) string {
	if !strings.EqualFold(strings.TrimSpace(sig.Platform), strings.TrimSpace(attempt.Venue)) {
		return "durable-paper-signal-identity-mismatch:venue"
	}
	if strings.TrimSpace(sig.Ticker) != strings.TrimSpace(attempt.Ticker) {
		return "durable-paper-signal-identity-mismatch:ticker"
	}
	if !strings.EqualFold(strings.TrimSpace(sig.Side), strings.TrimSpace(attempt.Side)) {
		return "durable-paper-signal-identity-mismatch:side"
	}
	if !strings.EqualFold(strings.TrimSpace(sig.SignalType), strings.TrimSpace(attempt.SystemID)) {
		return "durable-paper-signal-identity-mismatch:system"
	}
	return ""
}

func fundedPaperRecoveryLotReason(lot kfPos, attempt storage.ExecutionShadowAttempt) string {
	if lot.ExecutionTruthContract != fundedPaperLiveTruthContractV1 ||
		strings.TrimSpace(lot.ExecutionLiveTerminalID) == "" ||
		strings.TrimSpace(lot.ExecutionLiveTerminal) == "" {
		return "existing-funded-paper-lot-missing-live-truth-contract"
	}
	if !strings.EqualFold(firstNonEmpty(lot.Platform, attempt.Venue), attempt.Venue) ||
		strings.TrimSpace(lot.Ticker) != strings.TrimSpace(attempt.Ticker) ||
		!strings.EqualFold(lot.Side, attempt.Side) {
		return "existing-funded-paper-lot-identity-mismatch"
	}
	if !lot.FeeKnown || strings.TrimSpace(lot.FeeSource) == "" ||
		math.IsNaN(lot.Fee) || math.IsInf(lot.Fee, 0) || lot.Fee < 0 ||
		math.IsNaN(lot.Contracts) || math.IsInf(lot.Contracts, 0) || lot.Contracts <= 0 ||
		math.IsNaN(lot.Price) || math.IsInf(lot.Price, 0) || lot.Price <= 0 || lot.Price >= 1 {
		return "existing-funded-paper-lot-economics-incomplete"
	}
	return ""
}

func (s *Server) fundedPaperLotForExecutionAttempt(attemptID string) (kfPos, bool) {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return kfPos{}, false
	}
	s.gfBookMu.Lock()
	defer s.gfBookMu.Unlock()
	for _, book := range s.gfLoadLocked().Subs {
		if book == nil {
			continue
		}
		for _, lot := range book.Open {
			if strings.TrimSpace(lot.ExecutionShadowAttemptID) == attemptID {
				return lot, true
			}
		}
		for _, closed := range book.Closed {
			if strings.TrimSpace(closed.ExecutionShadowAttemptID) == attemptID {
				return closed.kfPos, true
			}
		}
	}
	return kfPos{}, false
}

// runGenfollowPaperPriorityWorkers keeps one serial main-ledger executor while giving selected,
// same-attempt comparison work a separate admission lane. The arbiter takes queued selected work
// first; at most one ordinary batch can already be handed off when a new priority batch arrives.
func (s *Server) runGenfollowPaperPriorityWorkers(ctx context.Context,
	priority, background <-chan genfollowPaperIntent, done chan<- struct{}) {
	merged := make(chan genfollowPaperIntent)
	go func() {
		defer close(merged)
		for priority != nil || background != nil {
			var (
				intent genfollowPaperIntent
				ok     bool
			)
			select {
			case intent, ok = <-priority:
				if !ok {
					priority = nil
					continue
				}
			default:
				select {
				case intent, ok = <-priority:
					if !ok {
						priority = nil
						continue
					}
				case intent, ok = <-background:
					if !ok {
						background = nil
						continue
					}
				case <-ctx.Done():
					return
				}
			}
			select {
			case merged <- intent:
			case <-ctx.Done():
				return
			}
		}
	}()
	s.runGenfollowPaperWorkers(ctx, merged, done)
}

func (s *Server) runGenfollowPaperWorkers(ctx context.Context, in <-chan genfollowPaperIntent,
	done chan<- struct{}) {
	for intent := range in {
		if len(intent.Signals) == 0 {
			continue
		}
		// Preserve detector-batch and queue order. One executor is deliberate: funded Paper shares
		// the main SQLite ledger, so simulations must not fan into parallel DB/REST work.
		for i, queued := range intent.Signals {
			if ctx.Err() != nil {
				for _, pending := range intent.Signals[i:] {
					s.releaseGenfollowPaperScheduled(pending.ShadowAttemptID)
					s.executionShadowRecordPaperScheduler(pending.ShadowAttemptID,
						"paper-worker-shutdown", time.Now().UTC())
					s.completeQueuedGenfollowPaperBeforeExecution(pending, genfollowPaperTerminalResult{
						State: "PAPER-NOT-OBSERVED", Reason: "paper-worker-shutdown",
					})
				}
				break
			}
			signalAt := queued.SignalAt
			if signalAt.IsZero() {
				signalAt = intent.EnqueuedAt
			}
			if signalAt.IsZero() {
				signalAt = time.Now().UTC()
			}
			if !waitForGenfollowPaperDue(ctx, signalAt.Add(genfollowPaperLiveFirstGrace)) {
				s.releaseGenfollowPaperScheduled(queued.ShadowAttemptID)
				s.executionShadowRecordPaperScheduler(queued.ShadowAttemptID,
					"paper-worker-shutdown", time.Now().UTC())
				s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
					State: "PAPER-NOT-OBSERVED", Reason: "paper-worker-shutdown",
				})
				continue
			}
			// Resolve the exact LIVE terminal from the isolated ledger before touching the shared
			// cash database. Venue zero-fills terminalize immediately, so they cannot spend five
			// seconds each waiting for a global quiet period and clog the funded-Paper queue.
			if queued.ShadowAttemptID != "" && queued.LiveTerminal.EventID == "" {
				terminalTimeout := s.genfollowPaperTerminalTimeout
				if terminalTimeout <= 0 {
					terminalTimeout = genfollowPaperTerminalTimeout
				}
				terminalCtx, terminalCancel := context.WithTimeout(ctx, terminalTimeout)
				terminal, found, terminalErr := s.waitForFundedPaperLiveTerminal(
					terminalCtx, queued.ShadowAttemptID)
				terminalCancel()
				if terminalErr != nil || !found ||
					terminal.Kind == fundedPaperLiveTerminalAmbiguous {
					reason := "durable-live-terminal-unavailable"
					if terminalErr != nil {
						reason += ":" + terminalErr.Error()
					} else if found {
						reason = "durable-live-terminal-ambiguous:" + terminal.State
					}
					s.releaseGenfollowPaperScheduled(queued.ShadowAttemptID)
					s.executionShadowRecordPaperScheduler(queued.ShadowAttemptID,
						reason, time.Now().UTC())
					s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
						State: "PAPER-NOT-OBSERVED", Reason: reason,
					})
					continue
				}
				if terminal.Kind == fundedPaperLiveTerminalZeroFill {
					s.executionShadowRecordPaperAttempt(queued.ShadowAttemptID,
						"PAPER-ZERO-FILL", "durable-live-terminal-proved-zero-fill:"+terminal.State,
						"", time.Now().UTC(), 0, 0, 0, 0, 0, "", queued.Signal)
					s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
						State:  "PAPER-ZERO-FILL",
						Reason: "durable-live-terminal-proved-zero-fill:" + terminal.State,
					})
					continue
				}
				queued.LiveTerminal = terminal
			}
			if queued.LiveTerminal.Kind == fundedPaperLiveTerminalZeroFill {
				s.executionShadowRecordPaperAttempt(queued.ShadowAttemptID,
					"PAPER-ZERO-FILL", "durable-live-terminal-proved-zero-fill:"+queued.LiveTerminal.State,
					"", time.Now().UTC(), 0, 0, 0, 0, 0, "", queued.Signal)
				s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
					State:  "PAPER-ZERO-FILL",
					Reason: "durable-live-terminal-proved-zero-fill:" + queued.LiveTerminal.State,
				})
				continue
			}
			if age := time.Since(signalAt); age < 0 || age > genfollowPaperSignalMaxAge {
				s.executionShadowRecordPaperAttempt(queued.ShadowAttemptID,
					"PAPER-NOT-OBSERVED", "paper-original-signal-expired", "",
					time.Now().UTC(), 0, 0, 0, 0, 0, "", queued.Signal)
				s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
					State: "PAPER-NOT-OBSERVED", Reason: "paper-original-signal-expired",
				})
				continue
			}
			workDeadline := time.Now().Add(genfollowPaperWorkTimeout)
			if signalDeadline := signalAt.Add(genfollowPaperSignalMaxAge); signalDeadline.Before(workDeadline) {
				workDeadline = signalDeadline
			}
			workCtx, cancel := context.WithDeadline(ctx, workDeadline)
			quietTimeout := s.genfollowPaperQuietTimeout
			if quietTimeout <= 0 {
				quietTimeout = genfollowPaperLiveQuietTimeout
			}
			quietCtx, quietCancel := context.WithTimeout(workCtx, quietTimeout)
			quiet := false
			if strings.TrimSpace(queued.ShadowAttemptID) != "" {
				// The exact LIVE terminal above proves this selected attempt is no longer on the
				// cash path. Unrelated queued signals must not starve its same-attempt Paper
				// comparison; yield only while another real cash section is actively executing.
				quiet = s.waitForFundedPaperCashSection(quietCtx)
			} else {
				quiet = s.waitForLiveDispatchShadowQuiet(quietCtx)
			}
			quietCancel()
			if !quiet {
				if ctx.Err() == nil && workCtx.Err() == context.DeadlineExceeded {
					s.executionShadowRecordPaperAttempt(queued.ShadowAttemptID,
						"PAPER-NOT-OBSERVED", "paper-original-signal-expired-during-quiet",
						"", time.Now().UTC(), 0, 0, 0, 0, 0, "", queued.Signal)
					s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
						State:  "PAPER-NOT-OBSERVED",
						Reason: "paper-original-signal-expired-during-quiet",
					})
					cancel()
					continue
				}
				reason := "paper-worker-live-priority-timeout"
				if ctx.Err() != nil {
					reason = "paper-worker-shutdown"
				}
				s.releaseGenfollowPaperScheduled(queued.ShadowAttemptID)
				s.executionShadowRecordPaperScheduler(queued.ShadowAttemptID,
					reason, time.Now().UTC())
				s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
					State: "PAPER-NOT-OBSERVED", Reason: reason,
				})
				cancel()
				continue
			}
			// Every main-ledger/horizon/audit/book operation begins only after this point.
			s.auditDroppedGenfollowPaperIntents(workCtx, intent.EnqueuedAt)
			if queued.preflight != nil {
				if reason := strings.TrimSpace(queued.preflight(workCtx)); reason != "" {
					s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
						State: "REJECTED", Reason: "delayed-paper-final-preflight:" + reason,
					})
					cancel()
					continue
				}
			}
			var canonicalExecution *storage.ExecutionShadowEvent
			if strings.TrimSpace(queued.ShadowAttemptID) != "" {
				canonicalCtx, canonicalCancel := context.WithTimeout(workCtx, 2*time.Second)
				terminal, found, terminalErr := s.waitForCanonicalSystemExecutionTerminal(
					canonicalCtx, queued.ShadowAttemptID)
				canonicalCancel()
				if terminalErr != nil || !found {
					reason := "canonical-execution-terminal-unavailable"
					if terminalErr != nil {
						reason += ":" + terminalErr.Error()
					}
					s.executionShadowRecordPaperAttempt(queued.ShadowAttemptID,
						"PAPER-NOT-OBSERVED", reason, "", time.Now().UTC(),
						0, 0, 0, 0, 0, "", queued.Signal)
					s.completeQueuedGenfollowPaperBeforeExecution(queued,
						genfollowPaperTerminalResult{State: "PAPER-NOT-OBSERVED", Reason: reason})
					cancel()
					continue
				}
				state := strings.ToLower(strings.TrimSpace(firstNonEmpty(
					terminal.ShadowState, terminal.Outcome)))
				reason := firstNonEmpty(terminal.ShadowReason, terminal.Reason)
				switch state {
				case livePolicyMirrorModeledFill:
					if terminal.ShadowFilledQty == nil || terminal.ShadowFillPrice == nil ||
						terminal.ShadowFee == nil || strings.TrimSpace(terminal.ShadowFeeSource) == "" ||
						strings.TrimSpace(terminal.BookSource) == "" {
						state = livePolicyMirrorNotObserved
						reason = "canonical-modeled-fill-economics-incomplete"
					} else {
						canonicalExecution = &terminal
					}
				case livePolicyMirrorModeledZeroFill:
					reason = "canonical-execution-zero-fill:" + firstNonEmpty(reason, "no-fill")
					s.executionShadowRecordPaperAttempt(queued.ShadowAttemptID,
						"PAPER-ZERO-FILL", reason, "", time.Now().UTC(),
						0, 0, 0, 0, 0, "", queued.Signal)
					s.completeQueuedGenfollowPaperBeforeExecution(queued,
						genfollowPaperTerminalResult{State: "PAPER-ZERO-FILL", Reason: reason})
					cancel()
					continue
				default:
					reason = "canonical-execution-not-observed:" + firstNonEmpty(reason, state)
				}
				if canonicalExecution == nil {
					s.executionShadowRecordPaperAttempt(queued.ShadowAttemptID,
						"PAPER-NOT-OBSERVED", reason, "", time.Now().UTC(),
						0, 0, 0, 0, 0, "", queued.Signal)
					s.completeQueuedGenfollowPaperBeforeExecution(queued,
						genfollowPaperTerminalResult{State: "PAPER-NOT-OBSERVED", Reason: reason})
					cancel()
					continue
				}
			}
			if queued.testRun != nil {
				queued.testRun(workCtx)
				s.releaseGenfollowPaperScheduled(queued.ShadowAttemptID)
				s.completeQueuedGenfollowPaperBeforeExecution(queued, genfollowPaperTerminalResult{
					State: "PAPER-NOT-OBSERVED", Reason: "hermetic-test-run-no-market-terminal",
				})
			} else {
				result := s.genfollowConsiderWithShadowTerminalCanonicalAt(workCtx, queued.Signal,
					queued.ShadowAttemptID, queued.LiveTerminal, canonicalExecution, queued.SignalAt)
				completeQueuedGenfollowPaperSignal(queued, result)
			}
			cancel()
		}
	}
	// The final aggregate audit is best-effort and obeys the same bounded LIVE-priority gate.
	auditTimeout := s.genfollowPaperQuietTimeout
	if auditTimeout <= 0 {
		auditTimeout = genfollowPaperLiveQuietTimeout
	}
	auditCtx, auditCancel := context.WithTimeout(ctx, auditTimeout)
	if s.waitForLiveDispatchShadowQuiet(auditCtx) {
		s.auditDroppedGenfollowPaperIntents(auditCtx, time.Now().UTC())
	}
	auditCancel()
	close(done)
}

// waitForFundedPaperCashSection gives an actively executing real-money section strict priority
// without requiring the continuously-fed signal/preflight queues to become globally empty. It is
// used only after the selected attempt's exact LIVE terminal has been durably read.
func (s *Server) waitForFundedPaperCashSection(ctx context.Context) bool {
	for s.liveDispatchShadowLiveActive.Load() > 0 {
		timer := time.NewTimer(5 * time.Millisecond)
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
	return ctx.Err() == nil
}

func (s *Server) waitForFundedPaperLiveTerminal(ctx context.Context,
	attemptID string) (fundedPaperLiveTerminalReceipt, bool, error) {
	if strings.TrimSpace(attemptID) == "" {
		return fundedPaperLiveTerminalReceipt{}, false, nil
	}
	s.wakeExecutionShadowWriter()
	timer := time.NewTicker(5 * time.Millisecond)
	defer timer.Stop()
	for {
		terminal, found, err := s.fundedPaperLiveTerminalForAttempt(ctx, attemptID)
		if err != nil {
			return fundedPaperLiveTerminalReceipt{}, false, err
		}
		if found {
			return terminal, true, nil
		}
		select {
		case <-timer.C:
			s.wakeExecutionShadowWriter()
		case <-ctx.Done():
			return fundedPaperLiveTerminalReceipt{}, false, ctx.Err()
		}
	}
}

func canonicalSystemExecutionTerminal(
	view storage.ExecutionShadowAttemptView) (storage.ExecutionShadowEvent, bool, error) {
	var terminal storage.ExecutionShadowEvent
	found := false
	for _, event := range view.Events {
		if event.Stage != "counterfactual-execution-terminal" {
			continue
		}
		if found {
			return storage.ExecutionShadowEvent{}, false,
				errors.New("multiple canonical execution terminals")
		}
		terminal, found = event, true
	}
	return terminal, found, nil
}

func (s *Server) waitForCanonicalSystemExecutionTerminal(ctx context.Context,
	attemptID string) (storage.ExecutionShadowEvent, bool, error) {
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return storage.ExecutionShadowEvent{}, false, nil
	}
	s.wakeExecutionShadowWriter()
	timer := time.NewTicker(10 * time.Millisecond)
	defer timer.Stop()
	for {
		views, err := s.store.ExecutionShadowAttemptsByIDs(ctx, []string{attemptID})
		if err != nil {
			return storage.ExecutionShadowEvent{}, false, err
		}
		if view, ok := views[attemptID]; ok {
			if terminal, found, err := canonicalSystemExecutionTerminal(view); err != nil || found {
				return terminal, found, err
			}
		}
		select {
		case <-timer.C:
			s.wakeExecutionShadowWriter()
		case <-ctx.Done():
			return storage.ExecutionShadowEvent{}, false, ctx.Err()
		}
	}
}

// waitForGenfollowPaperDue gives the already-published LIVE intent one fixed, per-attempt head
// start. Unrelated later LIVE signals cannot move this deadline and starve Paper indefinitely.
func waitForGenfollowPaperDue(ctx context.Context, dueAt time.Time) bool {
	wait := time.Until(dueAt)
	if wait <= 0 {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Server) auditDroppedGenfollowPaperIntents(parent context.Context, observedAt time.Time) {
	dropped := s.genfollowPaperWorkerDropped.Swap(0)
	if dropped == 0 || s.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	detail, _ := json.Marshal(map[string]any{
		"dropped_batches": dropped,
		"observed_at":     observedAt.Format(time.RFC3339Nano),
		"reason":          "bounded-paper-worker-queue-full",
		"live_impact":     "none",
	})
	_ = s.store.Audit(ctx, "warn", "paper-taker-attempt",
		fmt.Sprintf("Paper taker skipped %d queued batch(es); LIVE stayed nonblocking", dropped), string(detail))
}

// stopGenfollowPaperWorkers drains all normally queued Paper decisions before the store closes.
// If the caller's shutdown budget expires, cancellation wins and no unfinished simulation is
// promoted into a fill.
func (s *Server) stopGenfollowPaperWorkers(ctx context.Context) error {
	s.genfollowPaperWorkerMu.Lock()
	if s.genfollowPaperIntentCh == nil {
		s.genfollowPaperWorkerMu.Unlock()
		return nil
	}
	if !s.genfollowPaperWorkerStopping {
		s.genfollowPaperWorkerStopping = true
		close(s.genfollowPaperIntentCh)
		if s.genfollowPaperPriorityCh != nil {
			close(s.genfollowPaperPriorityCh)
		}
	}
	done := s.genfollowPaperWorkerDone
	cancel := s.genfollowPaperWorkerCancel
	s.genfollowPaperWorkerMu.Unlock()

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

// recordGenfollowPaperTakerAttempt is the durable "order was attempted" boundary. A process crash
// after this row but before the delayed check leaves an honest pending attempt and no funded lot.
func (s *Server) recordGenfollowPaperTakerAttempt(ctx context.Context, sig storage.Signal,
	signalAt time.Time, attemptID, opportunityID string, attemptAt time.Time, delay time.Duration, limitPrice, requestedContracts,
	estimatedFee float64, feeSource string, quote storage.Signal) error {
	if s.store == nil {
		return fmt.Errorf("paper taker attempt store unavailable")
	}
	detail, err := json.Marshal(map[string]any{
		"attempt_id":                    attemptID,
		"opportunity_id":                strings.TrimSpace(opportunityID),
		"execution_shadow_attempt_id":   strings.TrimSpace(opportunityID),
		"attempt_at":                    attemptAt.Format(time.RFC3339Nano),
		"trigger_unix_ms":               signalAt.UnixMilli(),
		"execution_generation":          fundedPaperCorrectedExecutionGenerationV1,
		"system":                        sig.SignalType,
		"venue":                         stringsLowerTrim(sig.Platform),
		"ticker":                        sig.Ticker,
		"side":                          stringsUpperTrim(sig.Side),
		"state":                         "ATTEMPTED",
		"order_result":                  "PENDING_DELAYED_IOC_SIMULATION",
		"limit_price":                   limitPrice,
		"attempted_contracts":           requestedContracts,
		"estimated_fee":                 estimatedFee,
		"fee_source":                    feeSource,
		"configured_execution_delay_ms": float64(delay) / float64(time.Millisecond),
		"configured_wire_delay_ms":      float64(s.livePolicyMirrorFinalWireDelay()) / float64(time.Millisecond),
		"book_source":                   quote.BookSource,
		"ask":                           quote.BookAsk,
		"bid":                           quote.BookBid,
		"ask_depth":                     quote.BookAskDepth,
		"bid_depth":                     quote.BookBidDepth,
		"quote_age_s":                   quote.BookQuoteAgeS,
		"simulation":                    "delayed-two-touch-wire-ioc-v2",
	})
	if err != nil {
		return err
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 4*time.Second)
	defer cancel()
	return s.store.Audit(auditCtx, "info", "paper-taker-attempt",
		fmt.Sprintf("Paper taker ATTEMPT %s %s %s x%.0f limit %.1f¢",
			stringsLowerTrim(sig.Platform), sig.Ticker, stringsUpperTrim(sig.Side),
			requestedContracts, limitPrice*100), string(detail))
}

func stringsLowerTrim(v string) string { return strings.ToLower(strings.TrimSpace(v)) }
func stringsUpperTrim(v string) string { return strings.ToUpper(strings.TrimSpace(v)) }
