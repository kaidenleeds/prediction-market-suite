package server

// Runtime admission and restart recovery for staged additive bundles. This loop is intentionally
// separate from singles and Kalshi RFQ combos: the dedicated config belt defaults OFF, and recovery
// still runs read-only while disarmed so an acknowledged order can never be forgotten on restart.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var r148StagedRuntimeMu sync.Mutex

var errR165StagedBundleCashRetired = errors.New(
	"staged-bundle-positive-settled-authenticated-live-profit-cohort-unavailable")

func r165StagedBundleNewCashAuthorityReady() bool { return false }

func (s *Server) r148StagedRuntimeEnabled() bool {
	return s != nil && s.cfg().Risk.LiveStagedBundles
}

func (s *Server) r148StagedCanAdvanceMoney() bool {
	if !s.r148StagedRuntimeEnabled() || s.ksBlocked() || s.liveAutoDispatchPaused() ||
		s.r148StagedRecoveryPauseReason() != "" {
		return false
	}
	s.liveMu.Lock()
	armed, auto := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	return armed && auto
}

func r148TransientRecoveryError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	v := strings.ToLower(err.Error())
	return strings.Contains(v, "context deadline") || strings.Contains(v, "context canceled") ||
		strings.Contains(v, "database is locked") || strings.Contains(v, "sqlite_busy") ||
		strings.Contains(v, "database connection is busy")
}

func (s *Server) r148StagedRecoveryPauseReason() string {
	if s == nil {
		return "staged recovery unavailable"
	}
	s.liveMu.Lock()
	paused, reason := s.r148StagedRecoveryPaused, s.r148StagedRecoveryReason
	s.liveMu.Unlock()
	if !paused {
		return ""
	}
	if strings.TrimSpace(reason) == "" {
		return "staged recovery is waiting for a clean durable-ledger read"
	}
	return reason
}

func (s *Server) setR148StagedRecoveryPaused(paused bool, reason string) {
	reason = strings.TrimSpace(reason)
	s.liveMu.Lock()
	wasPaused, oldReason := s.r148StagedRecoveryPaused, s.r148StagedRecoveryReason
	s.r148StagedRecoveryPaused = paused
	if paused {
		s.r148StagedRecoveryReason = reason
	} else {
		s.r148StagedRecoveryReason = ""
	}
	s.liveMu.Unlock()
	if paused && (!wasPaused || oldReason != reason) {
		s.liveLogAdd(map[string]any{"event": "STAGED-RECOVERY-PAUSED", "reason": reason,
			"money_action": "none; retrying automatically"})
	} else if !paused && wasPaused {
		s.liveLogAdd(map[string]any{"event": "STAGED-RECOVERY-RESUMED",
			"reason": "durable staged ledgers read cleanly"})
	}
}

// A transient storage failure pauses all new dispatch and retries on the next monitor cycle. It
// never rewrites ARM/AUTO intent or permanently trips the kill switch. Affirmative corruption,
// identity mismatch, and other non-transient failures still freeze globally.
func (s *Server) r148StagedRecoveryReadFailure(ctx context.Context, broker *r148ServerStagedBroker,
	scope string, err error, durableStateKnown bool) {
	detail := strings.TrimSpace(scope) + ": " + err.Error()
	if r148TransientRecoveryError(err) {
		reason := "staged recovery paused for a transient durable-ledger read; retrying automatically"
		if durableStateKnown {
			reason = "staged exposure recovery paused for a transient durable-ledger read; no new orders until retry succeeds"
		}
		s.setR148StagedRecoveryPaused(true, reason)
		auditCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		s.auditR148StagedOnce(auditCtx, scope, "recovery-deferred", detail)
		cancel()
		return
	}
	_ = broker.Freeze(ctx, "durable staged recovery failed: "+detail)
	s.auditR148StagedOnce(ctx, scope, "recovery-frozen", err.Error())
}

func r148StagedRuntimeSupported(system string) bool {
	switch strings.ToLower(strings.TrimSpace(system)) {
	case "event-basket-lock", "nested-ladder-lock", "time-nested-lock", "xvlock":
		return true
	default:
		return false
	}
}

func (s *Server) auditR148StagedOnce(ctx context.Context, bundleID, class, detail string) {
	if s == nil || s.store == nil {
		return
	}
	key := "r148_staged_runtime_" + r148Hash(map[string]string{"bundle": bundleID, "class": class, "detail": detail})[:20]
	if _, exists := s.store.KVGet(ctx, key); exists {
		return
	}
	_ = s.store.KVSet(ctx, key, time.Now().UTC().Format(time.RFC3339Nano))
	_ = s.store.Audit(ctx, "warn", "live", "staged bundle "+class, detail)
}

// r148RunUntilPause advances already-authorized FOK legs immediately. It stops on terminal state
// or the first nonterminal order id, whose next transition must come from authoritative recovery.
func (s *Server) r148RunUntilPause(ctx context.Context, c *r148StagedBundleCoordinator,
	executionID string, max int) error {
	if max <= 0 || max > 20 {
		max = 12
	}
	for i := 0; i < max; i++ {
		before, err := s.store.StagedBundleExecutionEvents(ctx, executionID)
		if err != nil || r148Terminal(before) {
			return err
		}
		if _, pending := r148Outstanding(before); pending {
			return nil
		}
		if err := c.Resume(ctx, executionID); err != nil {
			return err
		}
		after, err := s.store.StagedBundleExecutionEvents(ctx, executionID)
		if err != nil || r148Terminal(after) {
			return err
		}
		if _, pending := r148Outstanding(after); pending || len(after) == len(before) {
			return nil
		}
	}
	return errors.New("staged bundle exceeded bounded transition count")
}

func (s *Server) recoverR148StagedBundleExecutions(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	r148StagedRuntimeMu.Lock()
	defer r148StagedRuntimeMu.Unlock()
	broker := newR148ServerStagedBroker(s)
	c := &r148StagedBundleCoordinator{store: s.store, broker: broker}
	hasRisk, err := s.store.HasActiveLivePendingRiskProduct(ctx, "staged")
	if err != nil {
		s.r148StagedRecoveryReadFailure(ctx, broker, "pending-risk-presence", err, false)
		return
	}
	if hasRisk {
		if err := broker.reconcileActiveStagedRisks(ctx); err != nil {
			s.r148StagedRecoveryReadFailure(ctx, broker, "pending-risk-reconcile", err, true)
			return
		}
	}
	hasExecutions, err := s.store.HasPendingStagedBundleExecutions(ctx)
	if err != nil {
		s.r148StagedRecoveryReadFailure(ctx, broker, "journal-presence", err, hasRisk)
		return
	}
	if !hasExecutions {
		s.setR148StagedRecoveryPaused(false, "")
		s.clearLiveAutoSafetyPause("staged-risk",
			"no unresolved staged execution or pending staged venue mutation remains")
		return
	}
	ids, err := s.store.AllPendingStagedBundleExecutionIDs(ctx)
	if err != nil {
		s.r148StagedRecoveryReadFailure(ctx, broker, "journal-enumeration", err, true)
		return
	}
	for _, id := range ids {
		in, ok, readErr := s.store.StagedBundleExecutionIntentByID(ctx, id)
		if readErr != nil {
			s.r148StagedRecoveryReadFailure(ctx, broker, "intent-"+id, readErr, true)
			return
		}
		if !ok {
			_ = broker.Freeze(ctx, "pending staged intent is unreadable: "+id)
			s.auditR148StagedOnce(ctx, id, "recovery-frozen", "immutable intent missing")
			continue
		}
		bundle, found, readErr := s.store.ResearchRouteBundleByID(ctx, in.BundleID)
		if readErr != nil {
			s.r148StagedRecoveryReadFailure(ctx, broker, "bundle-"+id, readErr, true)
			return
		}
		if !found {
			_ = c.freeze(ctx, id, "immutable staged source bundle is unavailable during recovery", fmt.Sprint(readErr))
			continue
		}
		events, readErr := s.store.StagedBundleExecutionEvents(ctx, id)
		if readErr != nil {
			if r148TransientRecoveryError(readErr) {
				s.r148StagedRecoveryReadFailure(ctx, broker, "events-"+id, readErr, true)
				return
			}
			_ = c.freeze(ctx, id, "pending staged event journal is unreadable during recovery", readErr.Error())
			continue
		}
		if r148Terminal(events) {
			continue
		}
		if _, orphan := r148IntentWithoutReceipt(events); orphan {
			if err := c.Resume(ctx, id); err != nil {
				if r148TransientRecoveryError(err) {
					s.r148StagedRecoveryReadFailure(ctx, broker, "intent-recovery-"+id, err, true)
					return
				}
				s.auditR148StagedOnce(ctx, id, "intent-recovery-failed", err.Error())
			}
			continue
		}
		if _, outstanding := r148Outstanding(events); outstanding {
			// Resume performs only an authenticated read/reconcile while an order is outstanding.
			if err := c.Resume(ctx, id); err != nil {
				if r148TransientRecoveryError(err) {
					s.r148StagedRecoveryReadFailure(ctx, broker, "order-recovery-"+id, err, true)
					return
				}
				s.auditR148StagedOnce(ctx, id, "reconcile-failed", err.Error())
				continue
			}
			events, readErr = s.store.StagedBundleExecutionEvents(ctx, id)
			if readErr != nil {
				if r148TransientRecoveryError(readErr) {
					s.r148StagedRecoveryReadFailure(ctx, broker, "reconciled-events-"+id, readErr, true)
					return
				}
				_ = c.freeze(ctx, id, "reconciled staged event journal is unreadable", readErr.Error())
				continue
			}
		}
		if r148Terminal(events) {
			continue
		}
		// A reconciliation may still leave an acknowledged order pending. Never turn that into a
		// flat no-send retirement, and never originate a replacement order while its venue truth is
		// unresolved.
		if _, outstanding := r148Outstanding(events); outstanding {
			continue
		}
		if _, orphan := r148IntentWithoutReceipt(events); orphan {
			continue
		}
		filled, failed, _ := r148LegState(events)
		if len(filled) == len(bundle.Legs) {
			_ = c.Resume(ctx, id) // journal completion; no venue write.
			continue
		}
		if len(filled) == 0 && !failed {
			// Pre-R165 admitted-only intents inherited simulated research/Paper authority. With no
			// venue attempt and no exposure, recovery must close them rather than place leg one.
			if err := c.retireFlatBeforeFirstVenueWrite(ctx, id,
				"R165 retired staged research/Paper cash authority before first venue write"); err != nil {
				s.auditR148StagedOnce(ctx, id, "flat-legacy-retirement-failed", err.Error())
			}
			continue
		}
		// A transient AUTO pause is not a broken execution or a disabled product belt. Reconcile
		// acknowledged state above, then leave the next unsubmitted leg durable for automatic resume.
		if s.liveAutoDispatchPaused() {
			continue
		}
		if !s.r148StagedCanAdvanceMoney() {
			if failed && len(filled) == 0 {
				_ = c.Resume(ctx, id) // flat terminal rejection is journal-only; no money permission is needed.
				continue
			}
			if len(filled) > 0 {
				_ = c.freeze(ctx, id, "recovered staged exposure cannot advance while its explicit LIVE belt is off", map[string]any{"filled_legs": len(filled)})
			}
			continue
		}
		// Serialize the final budget/cluster re-check through every staged write against the same
		// mutex used by Kalshi and PolyUS singles. No concurrent single can consume the exposure
		// headroom between this bundle's Gate and FOK submission.
		s.livePlaceMu.Lock()
		if err := s.r148RunUntilPause(ctx, c, id, 2*len(bundle.Legs)+4); err != nil {
			after, stateErr := s.store.StagedBundleExecutionEvents(ctx, id)
			if r148TransientRecoveryError(err) || r148TransientRecoveryError(stateErr) {
				s.livePlaceMu.Unlock()
				s.r148StagedRecoveryReadFailure(ctx, broker, "transition-"+id,
					firstNonNilError(stateErr, err), true)
				return
			}
			if stateErr != nil || len(after) == 0 || !r148Terminal(after) {
				_ = c.freeze(ctx, id, "staged transition failed and durable state cannot be re-read",
					map[string]any{"transition_error": err.Error(), "state_error": fmt.Sprint(stateErr)})
			}
			s.auditR148StagedOnce(ctx, id, "recovery-transition-failed", err.Error())
		}
		s.livePlaceMu.Unlock()
	}
	s.setR148StagedRecoveryPaused(false, "")
}

// sweepR148StagedBundleAdmissions is called only from the armed AUTO loop. It consumes at most one
// new current positive named bundle per pass; bundle_id uniqueness and the immutable intent make
// restarts and repeated collector rows no-ops rather than duplicate orders.
func (s *Server) sweepR148StagedBundleAdmissions(ctx context.Context) {
	if !s.r148StagedCanAdvanceMoney() || s.store == nil {
		return
	}
	r148StagedRuntimeMu.Lock()
	defer r148StagedRuntimeMu.Unlock()
	rows, err := s.store.RecentPositiveResearchRouteBundles(ctx, time.Now().UTC().Add(-45*time.Second), 40)
	if err != nil {
		s.auditR148StagedOnce(ctx, "admission", "candidate-read-failed", err.Error())
		return
	}
	broker := newR148ServerStagedBroker(s)
	c := &r148StagedBundleCoordinator{store: s.store, broker: broker}
	for _, bundle := range rows {
		if !r148StagedRuntimeSupported(bundle.SystemID) || bundle.CertificateStatus != "verified" ||
			bundle.NetFloor <= 0 || !bundle.UnwindKnown || bundle.AtomicRoute {
			continue
		}
		polyUSLeg := false
		for _, leg := range bundle.Legs {
			polyUSLeg = polyUSLeg || leg.Venue == "polyus"
		}
		if polyUSLeg {
			_ = broker.durableBlock(ctx, bundle, r148PUSStagedBlockReason)
			continue
		}
		// Hold the normal placement mutex from the whole-bundle exposure/cluster reservation check
		// through every immediate FOK leg/unwind transition.
		s.livePlaceMu.Lock()
		executionID, admitErr := c.AdmitCurrent(ctx, bundle)
		if errors.Is(admitErr, errR148StagedNoProof) {
			s.livePlaceMu.Unlock()
			continue
		}
		if admitErr != nil {
			s.livePlaceMu.Unlock()
			s.auditR148StagedOnce(ctx, bundle.BundleID, "admission-refused", admitErr.Error())
			continue
		}
		if err := s.r148RunUntilPause(ctx, c, executionID, 2*len(bundle.Legs)+4); err != nil {
			s.auditR148StagedOnce(ctx, bundle.BundleID, "dispatch-failed", err.Error())
		}
		s.livePlaceMu.Unlock()
		return
	}
}
