package server

// R149 keeps transient transport trouble separate from operator intent. Once ARM + AUTO are ON,
// feed/book/database/producer readiness may temporarily stop NEW dispatches, but it never flips
// either control or trips the kill switch. The next healthy cycle resumes automatically.

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

const liveSafetyPausePendingRisk = "pending-risk"

// setLiveAutoSafetyPause records an automatic, non-latching stop on new money. It never changes
// the operator's ARM/AUTO choices and never touches the manual kill switch. Callers must clear the
// same stable key only after their own authoritative recovery condition is true.
func (s *Server) setLiveAutoSafetyPause(key, reason string) {
	if s == nil {
		return
	}
	key, reason = strings.TrimSpace(key), strings.TrimSpace(reason)
	if key == "" {
		key = "runtime-safety"
	}
	if reason == "" {
		reason = "automatic safety reconciliation is incomplete"
	}
	s.liveMu.Lock()
	if s.liveAutoSafetyPauses == nil {
		s.liveAutoSafetyPauses = make(map[string]string)
	}
	old, existed := s.liveAutoSafetyPauses[key]
	s.liveAutoSafetyPauses[key] = reason
	s.liveMu.Unlock()
	if !existed || old != reason {
		s.liveLogAdd(map[string]any{"event": "LIVE-SAFETY-PAUSED", "key": key,
			"reason": reason, "manual_kill_switch": "unchanged", "arm_auto": "preserved"})
	}
}

func (s *Server) clearLiveAutoSafetyPause(key, proof string) {
	if s == nil {
		return
	}
	key = strings.TrimSpace(key)
	s.liveMu.Lock()
	_, existed := s.liveAutoSafetyPauses[key]
	delete(s.liveAutoSafetyPauses, key)
	s.liveMu.Unlock()
	if existed {
		s.liveLogAdd(map[string]any{"event": "LIVE-SAFETY-RESUMED", "key": key,
			"reason": strings.TrimSpace(proof), "manual_kill_switch": "unchanged"})
	}
}

// snapshotLiveAutoSafetyPauses is the canonical in-memory view shared by the runtime dispatcher
// and the operator readiness card. It deliberately performs no venue, network, or database read:
// an ARM refusal retains its fail-closed reason in this map for the current process only. The map
// is not persisted across a clean restart, so disarmed readiness separately requires current
// in-memory daily-loss truth before it may advertise SAFE_TO_ARM.
func (s *Server) snapshotLiveAutoSafetyPauses() (reason, dailyLossReason string) {
	if s == nil {
		return "runtime unavailable", ""
	}
	s.liveMu.Lock()
	keys := make([]string, 0, len(s.liveAutoSafetyPauses))
	for key := range s.liveAutoSafetyPauses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	reasons := make([]string, 0, len(keys))
	for _, key := range keys {
		reasons = append(reasons, key+": "+s.liveAutoSafetyPauses[key])
		if key == "daily-loss" {
			dailyLossReason = s.liveAutoSafetyPauses[key]
		}
	}
	s.liveMu.Unlock()
	return strings.Join(reasons, "; "), dailyLossReason
}

func (s *Server) liveAutoSafetyPauseReason() string {
	reason, _ := s.snapshotLiveAutoSafetyPauses()
	return reason
}

type liveAutoRuntimeDecision struct {
	Dispatch bool
	Paused   bool
	Reason   string
}

func liveVenueRequired(venues []string, target string) bool {
	target = strings.ToLower(strings.TrimSpace(target))
	for _, venue := range venues {
		if strings.EqualFold(strings.TrimSpace(venue), target) {
			return true
		}
	}
	return false
}

func decideLiveAutoRuntime(armed, auto, killClear bool, pauseReason string) liveAutoRuntimeDecision {
	if !armed || !auto || !killClear {
		return liveAutoRuntimeDecision{}
	}
	pauseReason = strings.TrimSpace(pauseReason)
	if pauseReason != "" {
		return liveAutoRuntimeDecision{Paused: true, Reason: pauseReason}
	}
	return liveAutoRuntimeDecision{Dispatch: true}
}

func (s *Server) refreshLiveAutoRuntimeDecision(now time.Time) liveAutoRuntimeDecision {
	s.liveMu.Lock()
	armed, auto := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	killClear := !s.ksBlocked()
	decision := decideLiveAutoRuntime(armed, auto, killClear, "")
	if armed && auto && killClear {
		decision = decideLiveAutoRuntime(armed, auto, true, s.liveAutoRuntimePauseReason(now))
		if decision.Paused {
			s.setLiveAutoPaused(true, decision.Reason)
		} else if decision.Dispatch {
			s.setLiveAutoPaused(false, "")
		}
		return decision
	}
	s.resetLiveAutoPauseTracking()
	return decision
}

// livePolyUSDestinationEnabled is money authority, unlike mere credential/config presence.
func (s *Server) livePolyUSDestinationEnabled() bool {
	return s != nil && (s.liveSystemVenueEnabled("polyus") || s.liveNewMLVenueEnabled("polyus"))
}

// liveAutoPolyUSBooks is shared by the operator card and runtime gate so the two cannot disagree
// about an enabled PolyUS destination's current executable transport.
func (s *Server) liveAutoPolyUSBooks(now time.Time) (bool, string) {
	if s.polyUSWS == nil {
		return false, "markets websocket unavailable"
	}
	requested, frames, _ := s.polyUSWS.SubPlan()
	full, lite, trade := s.polyUSWS.SubCoverage()
	proofs, proofAt := s.polyUSWS.OpenSlugStats()
	proofAge := time.Duration(-1)
	if !proofAt.IsZero() {
		proofAge = now.Sub(proofAt)
	}
	dataAge, primaryAge, haveData, havePrimary := s.polyUSWS.PrimaryDataAge()
	transportAge, haveTransport := s.polyUSWS.PrimaryFrameAge()
	plan := s.depthBookPlanView("polyus")
	class := polyUSBooksClassify(polyUSBooksReadyInput{
		Proofs: proofs, Requested: requested, Frames: frames, Full: full, Lite: lite, Trade: trade,
		FullCap: s.polyUSWS.FullBookCap(), Fresh: s.polyUSWS.Count(), Executable: s.polyUSWS.ExecutableCount(),
		PriorityShortfall: plan.RequiredShortfall, ProofAge: proofAge, DataAge: dataAge,
		TransportAge: transportAge, PrimaryAge: primaryAge, HavePrimary: havePrimary,
		HaveData: haveData, HaveTransport: haveTransport,
		MaxProofAge:     polymarketus.MarketsRESTLifecycleMaxAge,
		MaxTransportAge: polymarketus.MarketsWSPrimaryTransportMaxAge,
	}, false)
	return class.TransportCoverageOK && class.ExecutableOK, class.Detail
}

// liveAutoRuntimePauseReason checks only runtime-wide prerequisites. It deliberately does not
// require a current opportunity and does not replace exact per-order market/book/account/risk
// checks. Disabled destinations are absent, so a Kalshi-only canary never reads PolyUS here.
func (s *Server) liveAutoRuntimePauseReason(now time.Time) string {
	if s == nil {
		return "runtime unavailable"
	}
	if reason := s.liveAutoSafetyPauseReason(); reason != "" {
		return reason
	}
	if reason := s.r148StagedRecoveryPauseReason(); reason != "" {
		return reason
	}
	authorityOK, _, venues, authorityDetail := s.liveAutoGoAuthority(now)
	if !authorityOK {
		return "LIVE authority: " + authorityDetail
	}
	required := map[string]bool{}
	for _, venue := range venues {
		required[venue] = true
	}
	if !s.feedsReady() {
		return "live feeds are warming or unavailable"
	}
	if !s.latchesOK.Load() {
		return "restart/dedup safety latches are unavailable"
	}
	for venue := range required {
		if why := s.liveLossVenueGate(venue); why != "" {
			return why
		}
	}
	if required["kalshi"] {
		if s.kal == nil || !s.kal.HasCredentials() || strings.EqualFold(s.authState, "locked") {
			return "Kalshi authentication is unavailable"
		}
		subs, fresh, gaps, lastErr := s.kal.BookStats()
		plan := s.depthBookPlanView("kalshi")
		ok, detail, _ := kalshiLiveBooksClassify(subs, fresh, gaps, plan.RequiredShortfall, lastErr)
		if !ok {
			return "Kalshi executable books: " + detail
		}
	}
	if required["polyus"] {
		if !s.livePolyUSDestinationEnabled() || s.polyUSAuth == nil || !strings.EqualFold(s.pusAuthState, "ok") {
			return "PolyUS authentication or destination authority is unavailable"
		}
		if ok, detail := s.liveAutoPolyUSBooks(now); !ok {
			return "PolyUS executable books: " + detail
		}
	}
	requiredProducers := make([]string, 0, len(required))
	for _, venue := range []string{"kalshi", "polyus"} {
		if required[venue] {
			requiredProducers = append(requiredProducers, venue)
		}
	}
	requiredProducers = s.requiredLiveSignalProducers(requiredProducers)
	producerReceipts, producerNow := s.signalProducerSnapshotAt()
	if ok, detail := signalProducersClassify(producerNow, 3*time.Minute,
		requiredProducers, producerReceipts); !ok {
		return "signal producer: " + detail
	}
	last, _, age, known := s.liveAutoGoDB(now)
	threshold := s.cfg().Auto.LatWarnDBP95Ms
	if threshold <= 0 {
		threshold = 500
	}
	if !known || age < 0 || age > liveGoDBSampleMaxAge || last >= threshold {
		return fmt.Sprintf("database canary is slow or stale (current %.0fms, age %s, limit %.0fms)", last, age.Truncate(time.Second), threshold)
	}
	return ""
}

func (s *Server) resetLiveAutoPauseTracking() {
	s.liveMu.Lock()
	s.liveAutoPauseKnown = false
	s.liveAutoPaused = false
	s.liveAutoPauseReason = ""
	s.liveMu.Unlock()
}

func (s *Server) liveAutoDispatchPaused() bool {
	if s == nil {
		return true
	}
	s.liveMu.Lock()
	paused := s.liveAutoPaused
	s.liveMu.Unlock()
	return paused
}

func (s *Server) setLiveAutoPaused(paused bool, reason string) {
	reason = strings.TrimSpace(reason)
	s.liveMu.Lock()
	known, wasPaused := s.liveAutoPauseKnown, s.liveAutoPaused
	s.liveAutoPauseKnown = true
	s.liveAutoPaused = paused
	s.liveAutoPauseReason = reason
	s.liveMu.Unlock()
	if known && wasPaused == paused {
		return
	}
	if paused {
		s.liveLogAdd(map[string]any{"event": "LIVE-AUTO-PAUSED", "reason": reason})
		return
	}
	if known && wasPaused {
		s.liveLogAdd(map[string]any{"event": "LIVE-AUTO-RESUMED", "reason": "runtime checks recovered"})
	}
}
