package server

// Generic system execution dispatcher. It cannot create evidence or promote from a rolling
// leaderboard. A fresh exact-contract action goes through the delayed two-complete-book Paper IOC
// executor first. Only an immutable sealed untouched PASS plus that identical filled Paper
// decision can enter the ARM+LIVE AUTO mirror, which independently refreshes price, fee, account
// state and risk.

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type researchPromotionSchedule struct {
	sync.Mutex
	last time.Time
}

var researchPromotionSchedules sync.Map

const (
	sealedPromotionNoLiveCohortReason                = "sealed-promotion-no-prior-authenticated-exchange-attempt-cohort"
	sealedPromotionIncompleteLiveCohortReason        = "sealed-promotion-authenticated-exchange-attempt-cohort-incomplete"
	sealedPromotionNoPositiveSettledLiveCohortReason = "sealed-promotion-positive-settled-live-profit-cohort-unavailable"
	sealedPromotionLiveCohortReadReason              = "sealed-promotion-authenticated-exchange-cohort-read-unavailable"
)

func (s *Server) researchPromotionSchedule() *researchPromotionSchedule {
	v, _ := researchPromotionSchedules.LoadOrStore(s, &researchPromotionSchedule{})
	return v.(*researchPromotionSchedule)
}

func (s *Server) researchPromotionRouteForSource(ctx context.Context, source string) (string, bool) {
	if s.store == nil || storage.ResearchPromotionIntentFromSource(source) == "" {
		return "", false
	}
	route, ok, err := s.store.ResearchPromotionIntentRoute(ctx, source)
	return route, ok && err == nil && (route == "maker" || route == "taker")
}

func (s *Server) researchPromotionAccepted(ctx context.Context, source string) (storage.ResearchPromotionIntent, bool) {
	if s.store == nil {
		return storage.ResearchPromotionIntent{}, false
	}
	in, ok, err := s.store.AcceptedResearchPromotionIntent(ctx, source)
	return in, ok && err == nil
}

// researchPromotionCashAuthorityReason is deliberately stricter than Paper acceptance. A sealed
// result may continue collecting, but it cannot authorize money until a prior identical promotion
// cohort has at least one authenticated exchange attempt and every such attempt has terminal
// fill/zero-fill truth.
func (s *Server) researchPromotionCashAuthorityReason(ctx context.Context,
	in storage.ResearchPromotionIntent) string {
	if s == nil || s.store == nil {
		return sealedPromotionLiveCohortReadReason
	}
	cohort, err := s.store.ResearchPromotionAuthoritativeLiveCohort(ctx, in)
	if err != nil {
		return sealedPromotionLiveCohortReadReason
	}
	return researchPromotionCashAuthorityCohortReason(cohort)
}

func researchPromotionCashAuthorityCohortReason(cohort storage.ResearchLiveExecutionCohort) string {
	if cohort.Attempts == 0 {
		return sealedPromotionNoLiveCohortReason
	}
	if !cohort.Ready() {
		return sealedPromotionIncompleteLiveCohortReason
	}
	// Terminal exchange attempts prove execution behavior, not profitability. Until this bridge
	// has a preregistered, settled, fee-net LIVE cohort with a positive conservative profit-rate
	// bound, simulated sealed-Paper economics cannot authorize the next cash order. A lone fill or
	// zero-fill must never bootstrap its own money authority.
	return sealedPromotionNoPositiveSettledLiveCohortReason
}

func (s *Server) reconcileResearchPromotionIntent(ctx context.Context, now time.Time,
	intent storage.ResearchPromotionIntent, receipt *storage.CollectorReceipt) {
	state, err := s.store.ResearchPromotionPaperRouteState(ctx, intent.SourceID, intent.Proof.Route,
		intent.Created.Add(-time.Second))
	if err != nil {
		receipt.Exclusions["paper_receipt_error"]++
		return
	}
	switch state.State {
	case "pending":
		receipt.Metrics["maker_pending"] = metricInt(receipt.Metrics["maker_pending"]) + 1
	case "filled":
		if state.Price <= 0 || state.Price >= 1 || math.IsNaN(state.FeePC) || math.IsInf(state.FeePC, 0) {
			receipt.Exclusions["paper_fill_invalid"]++
			return
		}
		if why := s.currentResearchCandidateIdentityReason(ctx, intent.Candidate); why != "" {
			_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), intent, "paper_rejected",
				intent.Proof.Route, state.Price, state.FeePC,
				"Paper filled, but current semantic identity changed before acceptance: "+why)
			receipt.Exclusions["paper_fill_identity_changed"]++
			return
		}
		if err := s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), intent, "paper_accepted",
			intent.Proof.Route, state.Price, state.FeePC,
			"pre-R166 Paper route completed identical order with exact fee/rebate authority"); err != nil {
			receipt.Exclusions["paper_accept_ceiling_or_identity_rejected"]++
			return
		}
		s.enqueueLiveMirrorCandidate(liveMirrorCandidate{Platform: intent.Candidate.Venue,
			Ticker: intent.Candidate.Ticker, Side: strings.ToUpper(intent.Candidate.Side),
			Family: intent.Candidate.StrategyFamily, SelectorID: intent.Candidate.SelectorID,
			FiredSide: strings.ToUpper(intent.Candidate.FiredSide),
			Source:    intent.SourceID, Price: state.Price, At: now,
			Inverted: strings.HasPrefix(strings.ToLower(intent.Candidate.StrategyFamily), "invert:")})
		receipt.Inserted++
	case "terminal_rejected":
		_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), intent, "paper_rejected",
			intent.Proof.Route, math.Max(0, state.Price), state.FeePC, state.Reason)
		receipt.Exclusions["paper_terminal_rejected"]++
	case "missing":
		if now.Sub(intent.Created) < 30*time.Second {
			receipt.Metrics["paper_receipt_warming"] = metricInt(receipt.Metrics["paper_receipt_warming"]) + 1
			return
		}
		_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), intent, "paper_rejected",
			intent.Proof.Route, 0, 0, "pre-R166 Paper route produced no durable route receipt")
		receipt.Exclusions["paper_receipt_missing_terminal"]++
	}
}

func metricInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

func researchPaperExplorationSource(c storage.ResearchPromotionCandidate) string {
	family := researchPaperCandidateFamily(c)
	return fmt.Sprintf("r142x:%s:%d:%s", strings.ToLower(c.Route), c.ObservationID,
		family)
}

func researchPaperCandidateFamily(c storage.ResearchPromotionCandidate) string {
	family := strings.ToLower(strings.TrimSpace(c.StrategyFamily))
	systemID := strings.ToLower(strings.TrimSpace(c.SystemID))
	fired := strings.ToUpper(strings.TrimSpace(c.FiredSide))
	side := strings.ToUpper(strings.TrimSpace(c.Side))
	if systemID == "paired-bridge-inversion" && strings.HasPrefix(family, "invert:") &&
		!strings.HasPrefix(family, "invert:invert:") &&
		(fired == "YES" || fired == "NO") && concreteOppositeSide(fired) == side {
		return family
	}
	return strings.TrimSpace(c.SystemID)
}

func researchPaperExplorationRoute(source string) (string, bool) {
	parts := strings.SplitN(strings.TrimSpace(source), ":", 4)
	if len(parts) != 4 || parts[0] != "r142x" || (parts[1] != "maker" && parts[1] != "taker") {
		return "", false
	}
	return parts[1], true
}

// researchPaperExplorationSystem attributes current r142x attempts to the canonical System while
// preserving route-only parsing for old hash-suffixed rows. An unregistered legacy suffix remains
// readable as an r142x route but must not manufacture a System family.
func researchPaperExplorationSystem(source string) (string, bool) {
	parts := strings.SplitN(strings.TrimSpace(source), ":", 4)
	if len(parts) != 4 {
		return "", false
	}
	if _, ok := researchPaperExplorationRoute(source); !ok {
		return "", false
	}
	systemID := strings.ToLower(strings.TrimSpace(parts[3]))
	if strings.HasPrefix(systemID, "invert:") && !strings.HasPrefix(systemID, "invert:invert:") &&
		len(strings.TrimPrefix(systemID, "invert:")) > 0 {
		return systemID, true
	}
	if _, ok := storage.SystemExecutionCapability(systemID); !ok {
		return "", false
	}
	return systemID, true
}

func researchPaperExplorationObservationID(source string) (int64, bool) {
	parts := strings.SplitN(strings.TrimSpace(source), ":", 4)
	if len(parts) != 4 || parts[0] != "r142x" {
		return 0, false
	}
	id, err := strconv.ParseInt(parts[2], 10, 64)
	return id, err == nil && id > 0
}

// researchPaperExactPoint returns a positive independently measured exact-route Paper estimate.
// It never falls back to a visible midpoint, a pooled signal verdict, or an algebraic inverse.
func (s *Server) researchPaperExactPoint(candidate storage.ResearchPromotionCandidate) (
	point, baselineAllIn float64, samples int, ok bool) {
	family := strings.ToLower(strings.TrimSpace(researchPaperCandidateFamily(candidate)))
	venue := strings.ToLower(strings.TrimSpace(candidate.Venue))
	side := strings.ToUpper(strings.TrimSpace(candidate.Side))
	route := strings.ToLower(strings.TrimSpace(candidate.Route))
	if family == "" || (venue != "kalshi" && venue != "polyus") ||
		(side != "YES" && side != "NO") || (route != "maker" && route != "taker") {
		return 0, 0, 0, false
	}
	s.verdMu.Lock()
	cache, at := append([]verdictEnt(nil), s.verdCache...), s.verdAt
	s.verdMu.Unlock()
	if cache == nil || at.IsZero() || time.Since(at) > swStale {
		return 0, 0, 0, false
	}
	bestPriority := -1
	for _, v := range cache {
		vf := strings.ToLower(strings.TrimSpace(v.SourceFamily))
		vv := strings.ToLower(strings.TrimSpace(v.Platform))
		vs := strings.ToUpper(strings.TrimSpace(v.Side))
		vr := strings.ToLower(strings.TrimSpace(v.Route))
		if vr == "" && v.Group == "taker" {
			vr = "taker"
		}
		if vf != family || vv != venue || vs != side || vr != route ||
			(v.Group != route && v.Group != "strategy") {
			continue
		}
		mean, n, pointOK := gfPaperPoint(v)
		ask, fee := gfPaperPointCosts(v)
		if !pointOK || mean <= 0 || n <= 0 || ask <= 0 || ask >= 1 || ask+fee <= 0 ||
			math.IsNaN(mean) || math.IsInf(mean, 0) || math.IsNaN(fee) || math.IsInf(fee, 0) {
			continue
		}
		priority := 0
		if v.Group == route {
			priority = 1 // the native maker/taker cell wins over a strategy display aggregate
		}
		// Deterministic, sample-first precedence prevents a tiny lucky duplicate from authorizing
		// over the canonical exact route. Equal-priority/equal-n duplicates choose the lower mean.
		if !ok || priority > bestPriority ||
			(priority == bestPriority && (n > samples || (n == samples && mean < point))) {
			point, baselineAllIn, samples, ok = mean, ask+fee, n, true
			bestPriority = priority
		}
	}
	return
}

func (s *Server) researchPaperExplorationCostAllowed(ctx context.Context, source string,
	contracts, price, feeTotal float64) bool {
	id, parsed := researchPaperExplorationObservationID(source)
	if !parsed || contracts != 1 || price <= 0 || price >= 1 || price+feeTotal <= 0 ||
		math.IsNaN(feeTotal) || math.IsInf(feeTotal, 0) {
		return false
	}
	candidate, found, err := s.store.ResearchPaperCandidateByID(ctx, id)
	if err != nil || !found || researchPaperExplorationSource(candidate) != source {
		return false
	}
	if researchPaperCandidateFreshReason(candidate, time.Now()) != "" ||
		s.currentResearchCandidateIdentityReason(ctx, candidate) != "" {
		return false
	}
	point, baseline, _, pointOK := s.researchPaperExactPoint(candidate)
	if !pointOK {
		return false
	}
	adjusted := point - ((price + feeTotal) - baseline)
	floor := s.cfg().Auto.MinEVPerContract
	if floor < 0 {
		floor = 0
	}
	return adjusted > floor+1e-9
}

// researchPaperRouteFee is the route-exact fee boundary for both unsealed exploration and sealed
// Paper validation. It deliberately preserves a signed PolyUS maker rebate as economic truth. A
// later capital/risk check may not spend that not-yet-credited rebate, but repricing, receipts and
// settlement must retain it. Missing or stale venue fee authority always fails closed.
func (s *Server) researchPaperRouteFee(platform, ticker, route string,
	contracts, price float64) (fee float64, source string, ok bool) {
	route = strings.ToLower(strings.TrimSpace(route))
	if route != "maker" && route != "taker" {
		return 0, "", false
	}
	fee, source, ok = s.fillFeeReceipt(platform, ticker, route == "maker", contracts, price)
	if !ok || strings.TrimSpace(source) == "" || math.IsNaN(fee) || math.IsInf(fee, 0) ||
		price*contracts+fee <= 0 {
		return 0, "", false
	}
	return fee, source, true
}

type researchSystemDispatchAdapter string

const (
	researchSystemDispatchSingle      researchSystemDispatchAdapter = "shared-single"
	researchSystemDispatchBundle      researchSystemDispatchAdapter = "native-bundle"
	researchSystemDispatchComposite   researchSystemDispatchAdapter = "composite-child-required"
	researchSystemDispatchDataControl researchSystemDispatchAdapter = "data-control-no-order"
	researchSystemDispatchUnsupported researchSystemDispatchAdapter = "unsupported"
)

// researchSystemDispatchPlan is the execution boundary between a System's collector and the
// shared order engines. A System is not allowed to become a one-leg order merely because it has a
// positive observation: the registry must say that this exact action shape owns the shared single
// route. Bundle, composite-without-child, data-control and unsupported shapes abstain here and keep
// their purpose-built evidence/route lanes.
func researchSystemDispatchPlan(systemID string) (researchSystemDispatchAdapter, string) {
	spec, ok := storage.SystemExecutionCapability(strings.TrimSpace(systemID))
	if !ok {
		return researchSystemDispatchUnsupported, "system-action-class-unregistered"
	}
	switch spec.HandoffClass {
	case storage.SystemHandoffSingle:
		return researchSystemDispatchSingle, "registered-single-action"
	case storage.SystemHandoffBundle:
		return researchSystemDispatchBundle, "registered-native-bundle-handoff"
	case storage.SystemHandoffChild:
		return researchSystemDispatchComposite, "registered-concrete-child-handoff"
	case storage.SystemHandoffUnsupported:
		if spec.ActionClass == storage.SystemActionData {
			return researchSystemDispatchDataControl, "registered-data-control-no-order"
		}
		if len(spec.UnsupportedReasons) > 0 {
			return researchSystemDispatchUnsupported, strings.Join(spec.UnsupportedReasons, "+")
		}
		return researchSystemDispatchUnsupported, "registered-product-route-unsupported"
	default:
		return researchSystemDispatchUnsupported, "system-handoff-class-unregistered"
	}
}

type researchSystemCandidatePlacer func(context.Context, string, string, string, string,
	float64, float64, int, int, string) bool

// dispatchResearchSystemCandidateWith is retained as an injectable compatibility contract for
// focused tests and old rows. Current research singles do not call it: production queues the
// delayed two-complete-book Paper IOC path below, so no immediate synthetic taker fill can enter
// current evidence.
func dispatchResearchSystemCandidateWith(ctx context.Context, candidate storage.ResearchPromotionCandidate,
	source string, place researchSystemCandidatePlacer) (bool, string) {
	adapter, reason := researchSystemDispatchPlan(candidate.SystemID)
	if adapter != researchSystemDispatchSingle {
		return false, string(adapter) + ":" + reason
	}
	spec, ok := storage.SystemExecutionCapability(candidate.SystemID)
	if !ok {
		return false, "unsupported:system-action-class-unregistered"
	}
	if !spec.SingleOrderEligible(candidate.Venue, candidate.Side, candidate.Route) {
		return false, "unsupported:single-action-identity-outside-registered-capability"
	}
	if candidate.Ticker == "" || (candidate.Side != "YES" && candidate.Side != "NO") ||
		candidate.ObservedPrice <= 0 || candidate.ObservedPrice >= 1 {
		return false, "unsupported:incomplete-single-action-identity"
	}
	if place == nil {
		return false, "unsupported:shared-single-executor-unavailable"
	}
	if strings.TrimSpace(source) == "" {
		return false, "unsupported:missing-execution-source"
	}
	if !place(ctx, candidate.Venue, candidate.Ticker, candidate.Title, candidate.Side,
		candidate.ObservedPrice, candidate.ObservedPrice, 0, 0, source) {
		return false, "shared-single:normal-executor-rejected"
	}
	return true, "shared-single:accepted"
}

func (s *Server) noteResearchSystemDispatchAbstain(ctx context.Context,
	candidate storage.ResearchPromotionCandidate, reason string) {
	if s.store == nil {
		return
	}
	detail := fmt.Sprintf("system=%s observation=%d venue=%s ticker=%s side=%s route=%s reason=%s",
		candidate.SystemID, candidate.ObservationID, candidate.Venue, candidate.Ticker,
		candidate.Side, candidate.Route, reason)
	_ = s.store.Audit(context.WithoutCancel(ctx), "info", "system-dispatch",
		"System action abstained before shared execution", detail)
}

func (s *Server) completeResearchPaperExploration(ctx context.Context,
	receipt *storage.CollectorReceipt, candidate storage.ResearchPromotionCandidate,
	accepted bool, reason string) {
	if err := s.store.CompleteResearchPaperCandidate(context.WithoutCancel(ctx),
		candidate.ObservationID, accepted, reason); err != nil {
		receipt.Exclusions["paper_exploration_completion_error"]++
		detail := fmt.Sprintf("system=%s observation=%d accepted=%t error=%v",
			candidate.SystemID, candidate.ObservationID, accepted, err)
		_ = s.store.Audit(context.WithoutCancel(ctx), "error", "system-dispatch",
			"Paper exploration claim completion failed", detail)
	}
}

func properScoreTransformFromCohort(cohort string) (string, bool) {
	transform := ""
	for _, token := range strings.Split(cohort, "|") {
		value, ok := strings.CutPrefix(strings.TrimSpace(token), "transform=")
		if !ok {
			continue
		}
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "brier" && value != "log" && value != "spherical" {
			continue
		}
		if transform != "" && transform != value {
			return "", false
		}
		transform = value
	}
	return transform, transform != ""
}

func researchCandidateDelayedPaperSignal(candidate storage.ResearchPromotionCandidate) (
	storage.Signal, string) {
	adapter, reason := researchSystemDispatchPlan(candidate.SystemID)
	if adapter != researchSystemDispatchSingle {
		return storage.Signal{}, string(adapter) + ":" + reason
	}
	spec, ok := storage.SystemExecutionCapability(candidate.SystemID)
	if !ok || !spec.SingleOrderEligible(candidate.Venue, candidate.Side, candidate.Route) {
		return storage.Signal{}, "unsupported:single-action-identity-outside-registered-capability"
	}
	if !strings.EqualFold(candidate.Route, "taker") {
		return storage.Signal{}, "unsupported:delayed-single-order-route-is-not-taker"
	}
	contracts := r147ExactSignalContracts(candidate.SystemID, candidate.Venue,
		candidate.Side, candidate.Route)
	if strings.EqualFold(candidate.SystemID, "proper-score-executor") && len(contracts) > 1 {
		transform, transformOK := properScoreTransformFromCohort(candidate.Cohort)
		if !transformOK {
			return storage.Signal{}, "unsupported:proper-score-transform-provenance-ambiguous"
		}
		suffix := "-PROPER-" + strings.ToUpper(transform)
		filtered := contracts[:0]
		for _, contract := range contracts {
			if strings.HasSuffix(strings.ToUpper(contract.InputTopology), suffix) {
				filtered = append(filtered, contract)
			}
		}
		contracts = filtered
	}
	if len(contracts) != 1 {
		if len(contracts) == 0 {
			return storage.Signal{}, "unsupported:exact-static-signal-contract-absent"
		}
		return storage.Signal{}, "unsupported:exact-static-signal-topology-ambiguous"
	}
	feePC := candidate.ObservedFeePC
	return storage.Signal{Platform: strings.ToLower(strings.TrimSpace(candidate.Venue)),
		Ticker: candidate.Ticker, Title: candidate.Title,
		Side: strings.ToUpper(strings.TrimSpace(candidate.Side)), SignalType: candidate.SystemID,
		EntryPrice: candidate.ObservedPrice, FeePC: &feePC,
		ExecExpr: r147InputReceiptExpr(contracts[0].InputTopology, candidate.Observed) +
			fmt.Sprintf("/research_observation=%d", candidate.ObservationID),
		LabelVersion:   fundedPaperCorrectedExecutionGenerationV1,
		PricingVersion: fundedPaperCorrectedExecutionGenerationV1}, ""
}

func researchDelayedPaperTerminalReason(result genfollowPaperTerminalResult) string {
	reason := strings.TrimSpace(result.Reason)
	if reason == "" {
		reason = "delayed-paper-terminal-without-reason"
	}
	if result.AttemptID != "" {
		return fmt.Sprintf("delayed-paper %s: %s; attempt=%s", result.State, reason, result.AttemptID)
	}
	return fmt.Sprintf("delayed-paper %s: %s", result.State, reason)
}

func (s *Server) completeResearchPromotionDelayedPaper(ctx context.Context,
	intent storage.ResearchPromotionIntent, signal storage.Signal,
	result genfollowPaperTerminalResult) {
	reject := func(reason string) {
		_ = s.store.AppendResearchPromotionEvent(ctx, intent, "paper_rejected", intent.Proof.Route,
			math.Max(0, result.FillPrice), 0, reason)
	}
	if result.State != "PAPER-FILLED" || result.FilledContracts != 1 ||
		result.FillPrice <= 0 || result.FillPrice >= 1 || result.Fee < 0 ||
		strings.TrimSpace(result.FeeSource) == "" {
		reject(researchDelayedPaperTerminalReason(result))
		return
	}
	if !researchPromotionPaperCostAllowed(intent, result.FilledContracts,
		result.FillPrice, result.Fee) {
		reject("delayed Paper fill exceeded the sealed one-share all-in ceiling")
		return
	}
	if why := s.currentResearchCandidateIdentityReason(ctx, intent.Candidate); why != "" {
		reject("delayed Paper filled, but current semantic identity changed before acceptance: " + why)
		return
	}
	feePC := result.Fee / result.FilledContracts
	if err := s.store.AppendResearchPromotionEvent(ctx, intent, "paper_accepted", intent.Proof.Route,
		result.FillPrice, feePC,
		"delayed two-complete-book Paper IOC filled one share with exact fee authority"); err != nil {
		return
	}
	candidate := liveMirrorCandidate{Platform: intent.Candidate.Venue,
		Ticker: intent.Candidate.Ticker, Title: intent.Candidate.Title,
		Side: strings.ToUpper(intent.Candidate.Side), Family: intent.Candidate.StrategyFamily,
		SelectorID: intent.Candidate.SelectorID, FiredSide: strings.ToUpper(intent.Candidate.FiredSide),
		Source: intent.SourceID, Price: result.FillPrice, At: time.Now().UTC(),
		ShadowAttemptID: result.ShadowAttemptID,
		InputTopology:   r147SignalInputTopology(signal),
		Inverted:        strings.HasPrefix(strings.ToLower(intent.Candidate.StrategyFamily), "invert:")}
	if bound, why, declared := bindStaticTakerSignalCandidate(candidate, signal); declared && why == "" {
		candidate = bound
	}
	s.enqueueLiveMirrorCandidate(candidate)
}

func (s *Server) enqueueResearchCandidateDelayedPaper(candidate storage.ResearchPromotionCandidate,
	intent *storage.ResearchPromotionIntent) (bool, string) {
	signal, why := researchCandidateDelayedPaperSignal(candidate)
	if why != "" {
		return false, why
	}
	signalAt := candidate.Observed
	if signalAt.IsZero() {
		signalAt = time.Now().UTC()
	}
	// The research producer is another view of the same exact detector opportunity, not a private
	// Paper-only sample. Claim before id minting so a simultaneous genfollow producer coalesces into
	// one row and distinct input topologies retain independent rows.
	if !s.claimCanonicalSignalOpportunity(signal, signalAt) {
		return false, "shared-single:duplicate-coalesced-canonical-opportunity"
	}
	shadowAttemptID := s.executionShadowSignalAttemptID(signal, 0, signalAt)
	if strings.TrimSpace(shadowAttemptID) == "" {
		return false, "shared-single:canonical-opportunity-id-unavailable"
	}
	queued := genfollowPaperSignal{Signal: signal, SignalAt: signalAt,
		ShadowAttemptID: shadowAttemptID,
		preflight: func(ctx context.Context) string {
			if why := researchPaperCandidateFreshReason(candidate, time.Now().UTC()); why != "" {
				return why
			}
			return s.currentResearchCandidateIdentityReason(ctx, candidate)
		},
		onTerminal: func(ctx context.Context, result genfollowPaperTerminalResult) {
			if intent != nil {
				s.completeResearchPromotionDelayedPaper(ctx, *intent, signal, result)
				return
			}
			accepted := result.State == "PAPER-FILLED" && result.FilledContracts == 1 &&
				result.FillPrice > 0 && result.FillPrice < 1 && result.Fee >= 0 &&
				strings.TrimSpace(result.FeeSource) != ""
			_ = s.store.CompleteResearchPaperCandidate(ctx, candidate.ObservationID,
				accepted, researchDelayedPaperTerminalReason(result))
		}}
	canonical := liveCandidateFromSignalIntent(liveSignalIntent{
		Signal: signal, At: signalAt, ShadowAttemptID: shadowAttemptID,
	})
	selectionReason := "research-paper-first-route-has-no-live-order-at-detector-boundary"
	s.executionShadowPublishSignalDisposition(signal, 0, signalAt, shadowAttemptID,
		false, false, selectionReason)
	s.executionShadowRecordDrop(canonical, "live-selection",
		"route-not-selected-for-live:"+selectionReason, time.Now().UTC(), map[string]any{
			"cash_authority": false, "selection_reason": selectionReason,
			"research_observation_id": candidate.ObservationID,
		})
	// Persist the exact Paper offer before nonblocking admission. A full/stopping queue therefore
	// retains same-id replay/scheduler evidence instead of leaving an orphan detector row.
	s.executionShadowRecordPaperFanout(shadowAttemptID, time.Now().UTC(), signalAt, signal)
	s.enqueueCanonicalSystemExecutionShadow(signal, signalAt, shadowAttemptID)
	if !s.enqueueGenfollowPaperIntent([]genfollowPaperSignal{queued}) {
		return false, "shared-single:delayed-paper-worker-queue-unavailable"
	}
	return true, "shared-single:queued-delayed-two-complete-book-paper-ioc"
}

func (s *Server) dispatchResearchPaperExploration(ctx context.Context, now time.Time,
	receipt *storage.CollectorReceipt, candidates []storage.ResearchPromotionCandidate) {
	for _, candidate := range candidates {
		candidate.Venue = strings.ToLower(strings.TrimSpace(candidate.Venue))
		candidate.Route = strings.ToLower(strings.TrimSpace(candidate.Route))
		candidate.Side = strings.ToUpper(strings.TrimSpace(candidate.Side))
		claimed, claimErr := s.store.ClaimResearchPaperCandidate(ctx, candidate,
			researchPaperExplorationSource(candidate))
		if claimErr != nil {
			receipt.Exclusions["paper_exploration_claim_error"]++
			continue
		}
		if !claimed {
			receipt.Duplicates++
			continue
		}
		receipt.Attempted++
		adapter, adapterReason := researchSystemDispatchPlan(candidate.SystemID)
		if adapter != researchSystemDispatchSingle {
			reason := string(adapter) + ":" + adapterReason
			s.completeResearchPaperExploration(ctx, receipt, candidate, false, reason)
			key := "system_action_" + strings.NewReplacer(":", "_", "-", "_").Replace(reason)
			receipt.Exclusions[key]++
			s.noteResearchSystemDispatchAbstain(ctx, candidate, reason)
			continue
		}
		if why := researchPaperCandidateFreshReason(candidate, now); why != "" {
			s.completeResearchPaperExploration(ctx, receipt, candidate, false, why)
			receipt.Exclusions["paper_exploration_"+why]++
			continue
		}
		if why := s.currentResearchCandidateIdentityReason(ctx, candidate); why != "" {
			s.completeResearchPaperExploration(ctx, receipt, candidate, false, why)
			receipt.Exclusions["paper_exploration_"+why]++
			continue
		}
		family := researchPaperCandidateFamily(candidate)
		if point, baseline, samples, pointOK := s.researchPaperExactPoint(candidate); pointOK {
			receipt.Metrics["legacy_positive_nomination_only"] =
				metricInt(receipt.Metrics["legacy_positive_nomination_only"]) + 1
			receipt.Metrics["last_legacy_exact_route_family"] = family
			receipt.Metrics["last_legacy_exact_route_point"] = point
			receipt.Metrics["last_legacy_exact_route_baseline_all_in"] = baseline
			receipt.Metrics["last_legacy_exact_route_samples"] = samples
		}
		if why := researchPaperCandidateFreshReason(candidate, time.Now()); why != "" {
			s.completeResearchPaperExploration(ctx, receipt, candidate, false, why)
			receipt.Exclusions["paper_exploration_recheck_"+why]++
			continue
		}
		if why := s.currentResearchCandidateIdentityReason(ctx, candidate); why != "" {
			s.completeResearchPaperExploration(ctx, receipt, candidate, false, why)
			receipt.Exclusions["paper_exploration_recheck_"+why]++
			continue
		}
		if queued, reason := s.enqueueResearchCandidateDelayedPaper(candidate, nil); queued {
			receipt.Metrics["delayed_paper_queued"] =
				metricInt(receipt.Metrics["delayed_paper_queued"]) + 1
		} else {
			s.completeResearchPaperExploration(ctx, receipt, candidate, false, reason)
			key := "system_action_" + strings.NewReplacer(":", "_", "-", "_").Replace(reason)
			receipt.Exclusions[key]++
			s.noteResearchSystemDispatchAbstain(ctx, candidate, reason)
		}
	}
}

func researchPromotionPaperCostAllowed(intent storage.ResearchPromotionIntent,
	contracts, price, feeTotal float64) bool {
	if contracts != 1 || price <= 0 || price >= 1 || price+feeTotal <= 0 ||
		math.IsNaN(feeTotal) || math.IsInf(feeTotal, 0) {
		return false
	}
	return price+feeTotal <= intent.MaxAllInUnit+1e-9
}

func (s *Server) sweepResearchPromotionDispatch(ctx context.Context, now time.Time) {
	// Multi-leg conjunction systems have their own exact RFQ/Paper bridge. It runs even while LIVE
	// is disarmed; only the later quote accept remains behind ARM + LIVE AUTO.
	s.sweepResearchBundlePaperDispatch(ctx, now)
	st := s.researchPromotionSchedule()
	st.Lock()
	due := st.last.IsZero() || now.Sub(st.last) >= 5*time.Second
	if due {
		st.last = now
	}
	st.Unlock()
	if !due || s.store == nil {
		return
	}
	receipt := storage.CollectorReceipt{CollectorID: "sealed-paper-promotion", CycleID: collectorCycleID(now),
		Status: "healthy", Started: now,
		Source:        storage.R139SealedPaperPromotionSource,
		SchemaVersion: "sealed-paper-live-bridge-r139-v1", ExpectedCadence: 5 * time.Second,
		Systems: storage.SystemIDs(), Exclusions: map[string]int{}, Metrics: map[string]any{
			"input_or_control_rows_can_promote": false, "paper_first": true,
			"live_requires_accepted_paper_arm_and_live_auto": true,
		}}
	defer func() {
		receipt.Completed = time.Now().UTC()
		_, _ = s.store.InsertCollectorReceipt(context.WithoutCancel(ctx), receipt)
	}()
	if recovered, recoverErr := s.store.CompleteStaleResearchPaperCandidates(ctx,
		now.Add(-2*time.Minute)); recoverErr != nil {
		receipt.Exclusions["stale_delayed_paper_claim_recovery_error"]++
	} else if recovered > 0 {
		receipt.Metrics["stale_delayed_paper_claims_terminalized"] = recovered
	}
	if unresolved, err := s.store.UnresolvedResearchPromotionIntents(ctx, 100); err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	} else {
		receipt.Metrics["unresolved_intents_checked"] = len(unresolved)
		for _, intent := range unresolved {
			s.reconcileResearchPromotionIntent(ctx, now, intent, &receipt)
		}
	}
	// Momentum reductions own a distinct immutable SELL contract. The generic lane remains BUY-only.
	s.reconcileProperMomentumSellIntents(ctx, now, &receipt)

	// Runtime wiring truth must not depend on Paper AUTO being enabled. Read the same bounded,
	// indexed exact-candidate page once per dispatch sweep and stamp it before the money gate; the
	// executor below reuses this page rather than issuing a second database query. A fresh candidate
	// is observability only and does not bypass economics, Paper, ARM, or LIVE authority.
	candidates, candidateErr := s.store.RecentResearchPaperCandidates(ctx, now.Add(-45*time.Second), 100)
	if candidateErr != nil {
		receipt.Exclusions["paper_exploration_read_error"]++
	} else {
		receipt.Metrics["fresh_paper_exploration_candidates"] = len(candidates)
		for _, candidate := range candidates {
			candidate.Venue = strings.ToLower(strings.TrimSpace(candidate.Venue))
			candidate.Route = strings.ToLower(strings.TrimSpace(candidate.Route))
			candidate.Side = strings.ToUpper(strings.TrimSpace(candidate.Side))
			s.noteR147ResearchCandidateRuntime(candidate)
		}
	}

	s.autoMu.Lock()
	paperAuto, globalInvert := s.autoOn, s.autoInvert
	s.autoMu.Unlock()
	s.sugMu.Lock()
	collecting := s.collecting
	s.sugMu.Unlock()
	if !paperAuto || globalInvert || collecting || s.ksBlocked() || s.liveProdActive() {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "Paper AUTO must be on, collection mode/global invert/live-prod must be off, and kill switch clear"
		return
	}
	// Fresh exact-route System actions may run one share through the same normal book/fee/depth/fill
	// executor before statistical proof exists. The unsealed source is not LIVE authority; the same
	// System is technically connected to LIVE through a later sealed identical-route intent, so no
	// Paper-only execution fork is created here.
	if candidateErr == nil {
		s.dispatchResearchPaperExploration(ctx, now, &receipt, candidates)
	}
	proofs, err := s.store.CurrentResearchPromotionProofs(ctx)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	receipt.Eligible = len(proofs)
	if len(proofs) == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no sealed untouched single-instrument System PASS exists"
		return
	}
	floor := s.liveMirrorEdgeFloor()
	for _, proof := range proofs {
		if proof.SystemID == "proper-score-executor" &&
			strings.Contains(proof.Cohort, "|action=sell-reduce-only|") {
			s.tryProperMomentumSellPromotion(ctx, now, proof, floor, &receipt)
			continue
		}
		adapter, reason := researchSystemDispatchPlan(proof.SystemID)
		if adapter != researchSystemDispatchSingle {
			key := "system_action_" + strings.NewReplacer(":", "_", "-", "_").Replace(string(adapter)+"_"+reason)
			receipt.Exclusions[key]++
			continue
		}
		candidate, found, candidateErr := s.store.LatestResearchPromotionCandidate(ctx, proof, now.Add(-20*time.Second))
		if candidateErr != nil {
			receipt.Exclusions["candidate_storage_error"]++
			continue
		}
		if !found {
			receipt.Exclusions["no_fresh_exact_contract_candidate"]++
			continue
		}
		candidate.Venue = strings.ToLower(strings.TrimSpace(candidate.Venue))
		candidate.Route = strings.ToLower(strings.TrimSpace(candidate.Route))
		candidate.Side = strings.ToUpper(strings.TrimSpace(candidate.Side))
		spec, registered := storage.SystemExecutionCapability(candidate.SystemID)
		if !registered || !spec.SingleOrderEligible(candidate.Venue, candidate.Side, candidate.Route) {
			receipt.Exclusions["candidate_outside_registered_execution_capability"]++
			continue
		}
		if why := researchPaperCandidateFreshReason(candidate, now); why != "" {
			receipt.Exclusions["sealed_candidate_"+why]++
			continue
		}
		if why := s.currentResearchCandidateIdentityReason(ctx, candidate); why != "" {
			receipt.Exclusions["sealed_candidate_"+why]++
			continue
		}
		receipt.Attempted++
		intent, inserted, intentErr := s.store.InsertResearchPromotionIntent(ctx, proof, candidate,
			candidate.ObservedPrice, candidate.ObservedFeePC, floor)
		if intentErr != nil {
			receipt.Exclusions["sealed_all_in_or_contract_rejected"]++
			continue
		}
		if !inserted {
			receipt.Duplicates++
			continue
		}
		if why := researchPaperCandidateFreshReason(candidate, time.Now()); why != "" {
			_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), intent, "paper_rejected",
				candidate.Route, 0, 0, "sealed candidate changed before Paper dispatch: "+why)
			receipt.Exclusions["sealed_candidate_recheck_"+why]++
			continue
		}
		if why := s.currentResearchCandidateIdentityReason(ctx, candidate); why != "" {
			_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), intent, "paper_rejected",
				candidate.Route, 0, 0, "sealed candidate changed before Paper dispatch: "+why)
			receipt.Exclusions["sealed_candidate_recheck_"+why]++
			continue
		}
		intentCopy := intent
		queued, rejectReason := s.enqueueResearchCandidateDelayedPaper(candidate, &intentCopy)
		if !queued {
			_ = s.store.AppendResearchPromotionEvent(context.WithoutCancel(ctx), intent, "paper_rejected",
				candidate.Route, 0, 0, "delayed Paper executor rejected the identical sealed route before queueing: "+rejectReason)
			receipt.Exclusions["paper_not_accepted_on_identical_route"]++
			continue
		}
		receipt.Metrics["sealed_delayed_paper_queued"] =
			metricInt(receipt.Metrics["sealed_delayed_paper_queued"]) + 1
	}
}
