package server

// R167 canonical money-free execution shadow.
//
// Every exact taker detector opportunity that is not selected for LIVE still receives the same
// delayed q1 IOC observation used to judge execution mechanics. This lane reads only resident
// complete books and exact local fee schedules, writes only the isolated execution-shadow ledger,
// and never reads an account, reserves cash, or calls a venue order endpoint.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	canonicalSystemShadowExperimentKind = "canonical-system-q1-execution-shadow"
	canonicalSystemShadowModelVersion   = "canonical-system-delayed-two-touch-q1-ioc-v2"
	// This is a conservative safety rail inherited from the existing immutable Paper review
	// contract. It is not a causal or validated fill predictor: prospective pre-POST calibration
	// still has to measure whether a quote this old is executable at the exchange.
	canonicalKalshiBookMutationAgeMaxS = 5.0
	canonicalKalshiMutationAgePolicy   = "conservative-5s-per-market-mutation-age-safety-rail; not-a-causal-or-validated-fill-predictor"
)

func canonicalKalshiBookMutationAgeReason(sig storage.Signal, stage string) string {
	if !strings.EqualFold(strings.TrimSpace(sig.Platform), "kalshi") {
		return ""
	}
	stage = strings.ToLower(strings.TrimSpace(stage))
	if stage == "" {
		stage = "unknown"
	}
	if sig.BookQuoteAgeS == nil || math.IsNaN(*sig.BookQuoteAgeS) ||
		math.IsInf(*sig.BookQuoteAgeS, 0) || *sig.BookQuoteAgeS < 0 {
		return "canonical-" + stage + "-kalshi-book-mutation-age-unavailable"
	}
	if *sig.BookQuoteAgeS > canonicalKalshiBookMutationAgeMaxS {
		return "canonical-" + stage + "-kalshi-book-mutation-age-exceeded"
	}
	return ""
}

func canonicalKalshiSequenceRelation(platform string, initial, finalQ liveMirrorQuote) (
	relation string, identical, advanced bool) {
	if !strings.EqualFold(strings.TrimSpace(platform), "kalshi") {
		return "non_kalshi", false, false
	}
	first, second := livePolicyMirrorBookReceiptFrom(initial.BookSource),
		livePolicyMirrorBookReceiptFrom(finalQ.BookSource)
	if !first.ok || !second.ok {
		return "unavailable", false, false
	}
	if first.generation != second.generation || first.subID != second.subID {
		return "domain_changed", false, false
	}
	switch {
	case second.sequence == first.sequence:
		return "identical", true, false
	case second.sequence > first.sequence:
		return "advanced", false, true
	default:
		return "regressed", false, false
	}
}

func canonicalBookMutationAgeMS(q liveMirrorQuote) float64 {
	if q.ObservedAt.IsZero() {
		return -1
	}
	checkedAt := q.CheckedAt
	if checkedAt.IsZero() {
		return -1
	}
	age := float64(checkedAt.Sub(q.ObservedAt)) / float64(time.Millisecond)
	if math.IsNaN(age) || math.IsInf(age, 0) || age < 0 {
		return -1
	}
	return age
}

func (s *Server) enqueueCanonicalSystemExecutionShadow(sig storage.Signal, signalAt time.Time,
	attemptID string) bool {
	if s == nil || s.store == nil || strings.TrimSpace(attemptID) == "" {
		return false
	}
	venue := strings.ToLower(strings.TrimSpace(sig.Platform))
	if venue != "kalshi" && venue != "polyus" {
		return false
	}
	if signalAt.IsZero() {
		signalAt = time.Now().UTC()
	}
	c := liveCandidateFromSignalIntent(liveSignalIntent{
		Signal: sig, At: signalAt, ShadowAttemptID: attemptID,
	})
	if strings.TrimSpace(c.SignalContractID) == "" || strings.TrimSpace(c.InputTopology) == "" {
		s.recordCanonicalSystemExecution(c, liveMirrorQuote{}, liveMirrorQuote{},
			time.Now().UTC(), 0, 1, 0, 0, 0, false, "",
			livePolicyMirrorNotObserved, "canonical-signal-contract-unavailable")
		return false
	}
	c.ProspectiveQty = 1
	c.ProspectiveTimeInForce = liveProspectiveIOC
	if !s.markLivePolicyMirrorExecutionScheduled(attemptID) {
		return false
	}
	work := livePolicyMirrorWork{
		Signal: sig, SignalAt: signalAt, Candidate: c,
		ExperimentKind: canonicalSystemShadowExperimentKind,
		ExecutionOnly:  true,
	}
	work.DueAt = signalAt.Add(s.genfollowPaperTakerDelay())
	if s.enqueueLivePolicyMirrorWork(work) {
		return true
	}
	s.forgetLivePolicyMirrorExecutionScheduled(attemptID)
	s.recordCanonicalSystemExecution(c, liveMirrorQuote{}, liveMirrorQuote{},
		time.Now().UTC(), 0, 1, 0, 0, 0, false, "",
		livePolicyMirrorNotObserved, "canonical-execution-shadow-queue-full")
	return false
}

func (s *Server) simulateCanonicalSystemExecutionShadow(ctx context.Context,
	work livePolicyMirrorWork) {
	c := work.Candidate
	startedAt := time.Now().UTC()
	if work.DueAt.IsZero() || startedAt.Sub(work.DueAt) > livePolicyMirrorStartLateMax {
		s.recordCanonicalSystemExecution(c, liveMirrorQuote{}, liveMirrorQuote{}, startedAt,
			0, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
			"canonical-execution-shadow-started-over-500ms-late")
		return
	}
	initialSignal, initialFeeSource, initialOK := s.completeBookSignalCached(work.Signal)
	if !initialOK {
		s.recordCanonicalSystemExecution(c, liveMirrorQuote{}, liveMirrorQuote{}, startedAt,
			0, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
			"canonical-initial-complete-book-unavailable")
		return
	}
	initial, initialWhy := s.genfollowPaperWireQuote(initialSignal)
	if initialWhy != "" {
		s.recordCanonicalSystemExecution(c, liveMirrorQuote{}, liveMirrorQuote{}, startedAt,
			0, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
			"canonical-initial-book:"+initialWhy)
		return
	}
	if ageWhy := canonicalKalshiBookMutationAgeReason(initialSignal, "initial"); ageWhy != "" {
		s.recordCanonicalSystemExecution(c, initial, liveMirrorQuote{}, startedAt,
			0, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved, ageWhy)
		return
	}
	initialFee := 0.0
	if initialSignal.FeePC != nil {
		initialFee = *initialSignal.FeePC
	}
	s.executionShadowRecordQuote(c, "counterfactual-execution-book-initial", "observed",
		"first delayed complete book", initial, initialFee, initialFeeSource, 1)
	limit := initial.Price
	if !waitLivePolicyMirror(ctx, s.livePolicyMirrorFinalWireDelay()) {
		s.recordCanonicalSystemExecution(c, initial, liveMirrorQuote{}, startedAt,
			limit, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
			"canonical-execution-wire-delay-canceled")
		return
	}
	finalSignal, finalFeeSource, finalOK := s.completeBookSignalCached(work.Signal)
	if !finalOK {
		s.recordCanonicalSystemExecution(c, initial, liveMirrorQuote{}, startedAt,
			limit, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
			"canonical-final-complete-book-unavailable")
		return
	}
	finalQ, finalWhy := s.genfollowPaperWireQuote(finalSignal)
	if finalWhy != "" {
		s.recordCanonicalSystemExecution(c, initial, liveMirrorQuote{}, startedAt,
			limit, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
			"canonical-final-book:"+finalWhy)
		return
	}
	if ageWhy := canonicalKalshiBookMutationAgeReason(finalSignal, "final"); ageWhy != "" {
		s.recordCanonicalSystemExecution(c, initial, finalQ, startedAt,
			limit, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved, ageWhy)
		return
	}
	if strings.EqualFold(c.Platform, "kalshi") {
		if !livePolicyMirrorBookUsableAtWire(initial, finalQ) {
			s.recordCanonicalSystemExecution(c, initial, finalQ, startedAt,
				limit, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
				"canonical-final-wire-book-provenance-regressed")
			return
		}
	} else if initial.ObservedAt.IsZero() || finalQ.ObservedAt.IsZero() ||
		finalQ.ObservedAt.Before(initial.ObservedAt) {
		s.recordCanonicalSystemExecution(c, initial, finalQ, startedAt,
			limit, 1, 0, 0, 0, false, "", livePolicyMirrorNotObserved,
			"canonical-final-wire-book-clock-regressed")
		return
	}
	finalFee := 0.0
	if finalSignal.FeePC != nil {
		finalFee = *finalSignal.FeePC
	}
	s.executionShadowRecordQuote(c, "counterfactual-execution-book-final", "observed",
		"final wire complete book", finalQ, finalFee, finalFeeSource, 1)
	filled, executionWhy := livePolicyMirrorIOC(limit, 1, finalQ)
	if executionWhy != "" {
		s.recordCanonicalSystemExecution(c, initial, finalQ, startedAt,
			limit, 1, 0, 0, 0, false, "", livePolicyMirrorModeledZeroFill,
			"canonical-execution:"+executionWhy)
		return
	}
	fee, feeSource, feeKnown := s.fillFeeReceipt(c.Platform, c.Ticker, false, filled, finalQ.Price)
	if !feeKnown || strings.TrimSpace(feeSource) == "" || math.IsNaN(fee) ||
		math.IsInf(fee, 0) || fee < 0 {
		s.recordCanonicalSystemExecution(c, initial, finalQ, startedAt,
			limit, 1, filled, finalQ.Price, 0, false, "", livePolicyMirrorModeledFill,
			"canonical-final-exact-fee-unavailable")
		return
	}
	s.recordCanonicalSystemExecution(c, initial, finalQ, startedAt,
		limit, 1, filled, finalQ.Price, fee, true, feeSource,
		livePolicyMirrorModeledFill, "")
}

func (s *Server) recordCanonicalSystemExecution(c liveMirrorCandidate,
	initial, finalQ liveMirrorQuote, startedAt time.Time,
	limit, requested, filled, fillPrice, fee float64, feeKnown bool,
	feeSource, state, reason string) {
	if strings.TrimSpace(c.ShadowAttemptID) == "" {
		return
	}
	now := time.Now().UTC()
	outcome := strings.TrimSpace(state)
	if outcome == "" {
		outcome = "unknown"
	}
	event := s.executionShadowEvent(c.ShadowAttemptID,
		"counterfactual-execution-terminal", outcome, strings.TrimSpace(reason), now)
	stable := sha256.Sum256([]byte(strings.Join([]string{
		c.ShadowAttemptID, "counterfactual-execution-terminal", canonicalSystemShadowModelVersion,
	}, "|")))
	event.EventID = "execev-stable-" + hex.EncodeToString(stable[:16])
	if finalQ.Price > 0 {
		executionShadowApplyQuote(&event, finalQ, now)
	} else if initial.Price > 0 {
		executionShadowApplyQuote(&event, initial, now)
	}
	event.OriginalLimit = executionShadowPricePtr(limit)
	if requested > 0 {
		event.RequestedQty = floatPtrShadow(requested)
	}
	event.ShadowState, event.ShadowReason = outcome, strings.TrimSpace(reason)
	if filled > 0 {
		event.ShadowFilledQty = floatPtrShadow(filled)
		event.ShadowFillPrice = executionShadowPricePtr(fillPrice)
		if feeValue := executionShadowNonnegativePtr(fee); feeKnown && feeValue != nil &&
			strings.TrimSpace(feeSource) != "" {
			event.ShadowFee, event.ShadowFeeSource = feeValue, strings.TrimSpace(feeSource)
		}
	}
	sequenceRelation, sequenceIdentical, sequenceAdvanced :=
		canonicalKalshiSequenceRelation(c.Platform, initial, finalQ)
	fillEvidenceClass := ""
	if outcome == livePolicyMirrorModeledFill && filled > 0 {
		// This describes only visible-depth model acceptance. It is deliberately not named or
		// treated as an exchange fill and grants no fee-net profit authority.
		fillEvidenceClass = "modeled_visible_fill"
	}
	event.Evidence = map[string]any{
		"model_version":                           canonicalSystemShadowModelVersion,
		"experiment_kind":                         canonicalSystemShadowExperimentKind,
		"scope":                                   "money-free delayed q1 IOC execution mechanics before portfolio admission",
		"real_money_authority":                    false,
		"profit_authority":                        false,
		"cash_path_blocking":                      false,
		"fake_portfolio_admission":                false,
		"live_terminal_observed":                  false,
		"exchange_fill_observed":                  false,
		"fill_evidence_class":                     fillEvidenceClass,
		"requested_quantity":                      requested,
		"time_in_force":                           liveProspectiveIOC,
		"fee_known":                               feeKnown,
		"wire_delay_ms":                           s.livePolicyMirrorFinalWireDelay().Milliseconds(),
		"trigger_at":                              optionalExecutionShadowTimeText(c.At),
		"started_at":                              optionalExecutionShadowTimeText(startedAt),
		"completed_at":                            optionalExecutionShadowTimeText(now),
		"initial_quote":                           liveMirrorQuoteEvidence(initial, startedAt),
		"final_quote":                             liveMirrorQuoteEvidence(finalQ, now),
		"kalshi_book_mutation_age_max_ms":         canonicalKalshiBookMutationAgeMaxS * 1000,
		"kalshi_mutation_age_policy":              canonicalKalshiMutationAgePolicy,
		"initial_book_mutation_age_ms":            canonicalBookMutationAgeMS(initial),
		"final_book_mutation_age_ms":              canonicalBookMutationAgeMS(finalQ),
		"kalshi_initial_final_sequence_relation":  sequenceRelation,
		"kalshi_initial_final_sequence_identical": sequenceIdentical,
		"kalshi_initial_final_sequence_advanced":  sequenceAdvanced,
	}
	if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &event}) {
		s.forgetLivePolicyMirrorExecutionScheduled(c.ShadowAttemptID)
	}
}
