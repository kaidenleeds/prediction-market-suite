package server

// R174 freezes every Step-7 projection before the raw SQLite transaction. The durable outbox
// consumer may validate and insert these bytes, but it must never ask a later book, fee schedule,
// identity version, lifecycle, or wall clock what the old decision should have been.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	step7ProjectionPayloadVersion = 1
	step7FlowExperimentVersion    = storage.Step7StructuralFlowExperimentVersion
	step7FlowAtomicCohort         = storage.Step7StructuralFlowCohort
	step7FlowAtomicSelector       = storage.Step7StructuralFlowSelector
	step7FlowProjectionSemantics  = storage.Step7StructuralFlowSemantics
	step7FlowStructuralBlocker    = storage.Step7StructuralFlowBlocker
)

func step7ProjectionNow(now func() time.Time) time.Time {
	if now == nil {
		return time.Now().UTC()
	}
	at := now().UTC()
	if at.IsZero() {
		return time.Now().UTC()
	}
	return at
}

// step7ValidateFrozenKalshiMarket validates a market snapshot against the exact per-job decision
// clock captured after every mutable input read. A lingering book on a closed, settled, missing,
// or ambiguously timed market is not an executable opportunity.
func step7ValidateFrozenKalshiMarket(market kalshi.Market, decisionAt time.Time) (kalshi.Market, string) {
	if market.Result != "" || (!strings.EqualFold(market.Status, "active") &&
		!strings.EqualFold(market.Status, "open")) {
		return kalshi.Market{}, "frozen_market_not_open"
	}
	closeRaw := strings.TrimSpace(firstNonEmpty(market.ExpectedExpiration, market.CloseTime))
	if closeRaw == "" {
		return kalshi.Market{}, "frozen_market_close_time_unavailable"
	}
	closeAt, parsed := parseTime(closeRaw)
	if !parsed || !closeAt.After(decisionAt) {
		return kalshi.Market{}, "frozen_market_close_time_invalid_or_past"
	}
	return market, ""
}

// step7FrozenKalshiMarket is retained as the direct lifecycle probe used by focused tests.
func (s *Server) step7FrozenKalshiMarket(ticker string, decisionAt time.Time) (kalshi.Market, string) {
	market, ok := s.kmkt(strings.TrimSpace(ticker))
	if !ok {
		return kalshi.Market{}, "frozen_market_lifecycle_unavailable"
	}
	return step7ValidateFrozenKalshiMarket(market, decisionAt)
}

func step7ProjectionJobSeen(jobs []storage.Step7ProjectionJob) map[string]bool {
	out := make(map[string]bool, len(jobs))
	for _, job := range jobs {
		if strings.TrimSpace(job.JobID) != "" {
			out[job.JobID] = true
		}
	}
	return out
}

func (s *Server) freezeStep7ProjectionJobs(ctx context.Context, drain *step7Drain,
	now func() time.Time) (map[string]int, error) {
	if s == nil || s.store == nil || drain == nil {
		return nil, errors.New("Step-7 projection freezer unavailable")
	}
	seen := step7ProjectionJobSeen(drain.projections)
	terminals := map[string]int{}
	appendJob := func(job storage.Step7ProjectionJob, err error) error {
		if err != nil {
			return err
		}
		if seen[job.JobID] {
			return nil
		}
		seen[job.JobID] = true
		drain.projections = append(drain.projections, job)
		if strings.TrimSpace(job.TerminalReason) != "" {
			terminals[job.TerminalReason]++
		}
		return nil
	}
	for _, episode := range drain.episodes {
		jobID := storage.Step7ProjectionJobKey(storage.Step7ProjectionEpisode,
			episode.EpisodeID)
		if seen[jobID] {
			continue
		}
		decisionAt := step7ProjectionNow(now)
		if decisionAt.Before(episode.Observed) {
			if err := appendJob(storage.NewStep7EpisodeProjectionJob(episode,
				step7ProjectionPayloadVersion, "raw_episode_clock_ahead_of_projection", nil)); err != nil {
				return terminals, err
			}
			continue
		}
		obs := storage.ResearchSystemObservation{
			Observed: episode.Observed, DecisionAt: decisionAt, SystemID: "replenishment-fingerprint",
			ExperimentVersion: step7TimingExperimentVersion, OpportunityID: "step7-v3|" + episode.EpisodeID,
			Kind: "control", Cohort: "event-driven-" + strings.ToLower(episode.Side) + "-" + step7TimingCohort,
			Venue: "kalshi", Route: "observer", CertificateStatus: "not_applicable",
			SourceClockID:  "kalshi-book-ws-event",
			SourceArtifact: "every subscribed top-touch delta; cancel is a book-minus-trade proxy",
			PayoutLower:    0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "open",
			Blocker: "fixed_horizon_and_settlement_evidence_pending",
			Inputs: map[string]any{"raw_episode": episode.Evidence, "projection_frozen_at": decisionAt,
				"projection_contract": "step7-frozen-outbox-v1"},
		}
		if err := appendJob(storage.NewStep7EpisodeProjectionJob(episode, step7ProjectionPayloadVersion,
			"", []storage.Step7FrozenObservation{{Observation: obs}})); err != nil {
			return terminals, err
		}
	}
	for _, mark := range drain.marks {
		jobID := storage.Step7ProjectionJobKey(storage.Step7ProjectionMark,
			storage.Step7MarkProjectionRawID(mark.EpisodeID, mark.EventType, mark.HorizonMS))
		if seen[jobID] {
			continue
		}
		outputs, terminal, err := s.freezeStep7MarkOutputs(ctx, mark, now)
		if err != nil {
			return terminals, err
		}
		if err := appendJob(storage.NewStep7MarkProjectionJob(mark, step7ProjectionPayloadVersion,
			terminal, outputs)); err != nil {
			return terminals, err
		}
	}
	for _, pair := range drain.flows {
		jobID := storage.Step7ProjectionJobKey(storage.Step7ProjectionFlow,
			pair.PairID)
		if seen[jobID] {
			continue
		}
		outputs, terminal, err := s.freezeStep7FlowOutputs(ctx, pair, now)
		if err != nil {
			return terminals, err
		}
		if err := appendJob(storage.NewStep7FlowProjectionJob(pair, step7ProjectionPayloadVersion,
			terminal, outputs)); err != nil {
			return terminals, err
		}
	}
	rawJobIDs := make(map[string]struct{}, len(drain.episodes)+len(drain.marks)+len(drain.flows))
	for _, episode := range drain.episodes {
		rawJobIDs[storage.Step7ProjectionJobKey(storage.Step7ProjectionEpisode, episode.EpisodeID)] = struct{}{}
	}
	for _, mark := range drain.marks {
		rawID := storage.Step7MarkProjectionRawID(mark.EpisodeID, mark.EventType, mark.HorizonMS)
		rawJobIDs[storage.Step7ProjectionJobKey(storage.Step7ProjectionMark, rawID)] = struct{}{}
	}
	for _, flow := range drain.flows {
		rawJobIDs[storage.Step7ProjectionJobKey(storage.Step7ProjectionFlow, flow.PairID)] = struct{}{}
	}
	if len(drain.projections) != len(rawJobIDs) {
		return terminals, fmt.Errorf("Step-7 projection coverage jobs=%d unique_raw=%d",
			len(drain.projections), len(rawJobIDs))
	}
	return terminals, nil
}

func (s *Server) freezeStep7MarkOutputs(ctx context.Context, mark storage.Step7ReplenishmentMark,
	now func() time.Time) ([]storage.Step7FrozenObservation, string, error) {
	if mark.EventType != "horizon" || mark.Ticker == "" || mark.Side == "" {
		return nil, "raw_mark_has_no_economic_projection", nil
	}
	directional, reason := step7ReplenishmentActionRule(mark)
	switch {
	case mark.TimingVersion != step7TimingExperimentVersion:
		directional, reason = false, "legacy_or_wrong_timing_version"
	case mark.Target.IsZero() || mark.Observed.Before(mark.Target) ||
		mark.Observed.Sub(mark.Target) > step7TimingMaxLateness:
		directional, reason = false, "fixed_horizon_sample_outside_lateness_bound"
	}
	var candidate storage.Step7FrozenObservation
	var decisionAt time.Time
	if directional {
		var err error
		candidate, decisionAt, reason, err = s.freezeStep7ReplenishmentCandidate(ctx, mark, now)
		if err != nil {
			return nil, "", err
		}
		directional = reason == ""
	}
	if decisionAt.IsZero() {
		decisionAt = step7ProjectionNow(now)
	}
	if decisionAt.Before(mark.Observed) {
		return nil, "raw_mark_clock_ahead_of_projection", nil
	}
	controlBlocker := "candidate_not_projected:" + reason
	if directional {
		controlBlocker = ""
	}
	control := storage.ResearchSystemObservation{
		Observed: mark.Observed, DecisionAt: decisionAt, SystemID: "replenishment-fingerprint",
		ExperimentVersion: step7TimingExperimentVersion,
		OpportunityID:     "step7-v3|" + mark.EpisodeID + "|" + strconv.FormatInt(mark.HorizonMS, 10) + "|no-trade",
		Kind:              "control", Cohort: "event-driven-matched-no-trade-" + step7TimingCohort,
		Venue: "kalshi", Route: "observer", CertificateStatus: "not_applicable",
		SourceClockID:  "kalshi-book-ws-event",
		SourceArtifact: "RAM-clocked fixed-horizon depletion persistence matched no-trade control",
		PayoutLower:    0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "settled",
		Blocker: controlBlocker,
		Inputs: map[string]any{"ticker": mark.Ticker, "side": mark.Side,
			"horizon_ms": mark.HorizonMS, "authoritative_trade_units": mark.TradeUnits,
			"unexplained_removal_units": mark.UnexplainedUnits, "refilled": mark.Refilled,
			"control_realized_net": 0, "frozen_rule_outcome": reason,
			"timing_cohort": step7TimingCohort, "target_ts": mark.Target,
			"sampled_at": mark.Observed, "sample_lateness_ms": mark.Observed.Sub(mark.Target).Seconds() * 1000,
			"projection_frozen_at": decisionAt, "projection_contract": "step7-frozen-outbox-v1"},
	}
	outputs := []storage.Step7FrozenObservation{{Observation: control}}
	if directional {
		outputs = append(outputs, candidate)
		return outputs, "", nil
	}
	return outputs, reason, nil
}

func (s *Server) freezeStep7ReplenishmentCandidate(ctx context.Context,
	mark storage.Step7ReplenishmentMark, now func() time.Time) (storage.Step7FrozenObservation, time.Time, string, error) {
	side, ask, depth := "YES", mark.BookPrice, mark.BookDepth
	if mark.Side == "YES_BID" {
		side, ask = "NO", 1-mark.BookPrice
	}
	if ask <= 0 || ask >= 1 || depth < 1 || mark.BookGeneration == 0 ||
		mark.BookSubscriptionID <= 0 || mark.BookSourceSequence <= 0 || mark.BookObserved.IsZero() {
		return storage.Step7FrozenObservation{}, step7ProjectionNow(now),
			"frozen_horizon_book_depth_or_clock_unavailable", nil
	}
	m, marketOK := s.kmkt(strings.TrimSpace(mark.Ticker))
	if !marketOK {
		return storage.Step7FrozenObservation{}, step7ProjectionNow(now),
			"frozen_market_lifecycle_unavailable", nil
	}
	tick, tickOK := m.TickForKnown(ask)
	if !tickOK || tick <= 0 {
		return storage.Step7FrozenObservation{}, step7ProjectionNow(now),
			"frozen_horizon_tick_unknown", nil
	}
	fee, feeOK, feeSource := s.kalFeeExact(mark.Ticker, false, 1, ask)
	if !feeOK || strings.TrimSpace(feeSource) == "" {
		return storage.Step7FrozenObservation{}, step7ProjectionNow(now),
			"frozen_horizon_exact_taker_fee_unknown", nil
	}
	identity, found, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", mark.Ticker)
	if err != nil {
		// The first decision-time lookup is the evidence. Retrying this raw event against a later
		// identity would rewrite history, so preserve an explicit blocked terminal instead.
		return storage.Step7FrozenObservation{}, step7ProjectionNow(now),
			"frozen_horizon_canonical_identity_lookup_error", nil
	}
	if !found || identity.IdentityStatus != "verified" || identity.InstrumentVersion <= 0 ||
		identity.EventID == "" || identity.EventVersion <= 0 || identity.PayoffID == "" ||
		identity.PayoffVersion <= 0 {
		return storage.Step7FrozenObservation{}, step7ProjectionNow(now),
			"frozen_horizon_canonical_identity_unverified", nil
	}
	decisionAt := step7ProjectionNow(now)
	m, lifecycleReason := step7ValidateFrozenKalshiMarket(m, decisionAt)
	if lifecycleReason != "" {
		return storage.Step7FrozenObservation{}, decisionAt, lifecycleReason, nil
	}
	delay := decisionAt.Sub(mark.Observed)
	if delay < 0 || delay > step7TimingMaxLateness {
		return storage.Step7FrozenObservation{}, decisionAt,
			"projection_freeze_outside_lateness_bound", nil
	}
	quoteAt := mark.BookSourceAt
	if quoteAt.IsZero() {
		quoteAt = mark.BookObserved
	}
	age := decisionAt.Sub(quoteAt)
	if age < 0 || age > 3*time.Second {
		return storage.Step7FrozenObservation{}, decisionAt,
			"frozen_horizon_quote_clock_stale_or_ahead", nil
	}
	latency := delay
	clockID := fmt.Sprintf("kalshi-book:g%d:sid%d:seq%d", mark.BookGeneration,
		mark.BookSubscriptionID, mark.BookSourceSequence)
	certificate := step7Hash("replenishment-rule-v3", step7TimingCohort, "persistent-no-refill",
		"authoritative-trade>=75pct-removal", strconv.FormatInt(mark.HorizonMS, 10))
	obs := storage.ResearchSystemObservation{
		Observed: mark.Observed, DecisionAt: decisionAt, SystemID: "replenishment-fingerprint",
		ExperimentVersion: step7TimingExperimentVersion,
		OpportunityID:     "step7-v3|" + mark.EpisodeID + "|" + strconv.FormatInt(mark.HorizonMS, 10) + "|action",
		Kind:              "candidate",
		Cohort: "persistent-authoritative-depletion-" + strconv.FormatInt(mark.HorizonMS, 10) +
			"ms-" + step7TimingCohort,
		CanonicalEventID: identity.EventID, EventVersion: identity.EventVersion,
		CanonicalPayoffID: identity.PayoffID, PayoffVersion: identity.PayoffVersion,
		Venue: "kalshi", Ticker: mark.Ticker, Route: "taker", Side: side,
		CertificateStatus: "verified", CertificateHash: certificate, SourceClockID: clockID,
		SourceArtifact: "frozen v3: RAM-clocked horizon touch plus atomic fee/tick/identity projection",
		BookSource:     "kalshi_book_ws_frozen_top_touch", FeeSource: feeSource,
		QuoteAgeMax: age.Seconds(), TickMin: tick, Size: 1, Cost: ask, Fee: fee,
		PayoutLower: 0, PayoutUpper: 1, NetLower: -ask - fee, NetUpper: 1 - ask - fee,
		VisibleCapacity: depth, DecisionLatencyMS: latency.Seconds() * 1000,
		LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
		Candidate: true, OutcomeStatus: "open",
		CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: ask, Fee: fee, PayoutFloor: 0,
			NetFloor: -ask - fee}},
		Inputs: map[string]any{"episode_id": mark.EpisodeID, "horizon_ms": mark.HorizonMS,
			"authoritative_trade_units": mark.TradeUnits, "unexplained_removal_units": mark.UnexplainedUnits,
			"refilled": false, "maker_route_used": false, "top_touch_capacity_only": true,
			"timing_cohort": step7TimingCohort, "target_ts": mark.Target, "sampled_at": mark.Observed,
			"sample_lateness_ms":   mark.Observed.Sub(mark.Target).Seconds() * 1000,
			"projection_frozen_at": decisionAt, "trigger_to_pricing_latency_ms": latency.Seconds() * 1000,
			"frozen_instrument_version": identity.InstrumentVersion,
			"frozen_market_status":      m.Status, "frozen_market_close_time": firstNonEmpty(m.ExpectedExpiration, m.CloseTime),
			"persistence_repriced_book": false, "projection_contract": "step7-frozen-outbox-v1"},
	}
	return storage.Step7FrozenObservation{Observation: obs, InstrumentVersion: identity.InstrumentVersion},
		decisionAt, "", nil
}

func (s *Server) freezeStep7FlowOutputs(ctx context.Context, pair storage.Step7FlowDirectionPair,
	now func() time.Time) ([]storage.Step7FrozenObservation, string, error) {
	selected := strings.ToUpper(strings.TrimSpace(pair.AuthoritativeSide))
	if selected != "YES" && selected != "NO" {
		return nil, "authoritative_flow_side_conflict_or_unknown", nil
	}
	identity, found, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", pair.Ticker)
	if err != nil {
		return nil, "authoritative_flow_identity_lookup_error", nil
	}
	if !found {
		return nil, "authoritative_flow_identity_unverified", nil
	}
	if _, ok := step7FlowVenueLocalIdentityStatus(identity, pair.Ticker); !ok {
		return nil, "authoritative_flow_identity_unverified", nil
	}
	legs, ok := s.concreteExactPair("kalshi", pair.Ticker, nil)
	if !ok {
		return nil, "authoritative_flow_current_exact_book_unavailable", nil
	}
	market, marketOK := s.kmkt(strings.TrimSpace(pair.Ticker))
	if !marketOK {
		return nil, "frozen_market_lifecycle_unavailable", nil
	}
	decisionAt := step7ProjectionNow(now)
	market, lifecycleReason := step7ValidateFrozenKalshiMarket(market, decisionAt)
	if lifecycleReason != "" {
		return nil, lifecycleReason, nil
	}
	for i := range legs {
		if legs[i].QuoteObserved.IsZero() {
			return nil, "authoritative_flow_quote_observation_clock_unavailable", nil
		}
		age := decisionAt.Sub(legs[i].QuoteObserved)
		if age < 0 || age > 3*time.Second {
			return nil, "authoritative_flow_quote_clock_stale_or_ahead", nil
		}
		legs[i].QuoteAge = age.Seconds()
	}
	in, inputOK := step7FlowIntegrityAtomicPairInput(pair, identity, legs, decisionAt)
	if !inputOK {
		return nil, "authoritative_flow_candidate_contract_invalid", nil
	}
	in.Inputs["frozen_market_status"] = market.Status
	in.Inputs["frozen_market_close_time"] = firstNonEmpty(market.ExpectedExpiration, market.CloseTime)
	outputs, err := step7FrozenConcretePairOutputs(in, pair.Observed, step7FlowExperimentVersion)
	if err != nil {
		return nil, "", err
	}
	return outputs, "", nil
}

func step7FlowIntegrityAtomicPairInput(pair storage.Step7FlowDirectionPair,
	identity storage.CurrentCanonicalInstrument, legs []nativeExactLeg, decisionAt time.Time) (concretePairInput, bool) {
	selected := strings.ToUpper(strings.TrimSpace(pair.AuthoritativeSide))
	identityStatus, identityOK := step7FlowVenueLocalIdentityStatus(identity, pair.Ticker)
	if (selected != "YES" && selected != "NO") || pair.PairID == "" || pair.Ticker == "" ||
		!identityOK || len(legs) != 2 || decisionAt.IsZero() ||
		decisionAt.Before(pair.Observed) {
		return concretePairInput{}, false
	}
	legs = append([]nativeExactLeg(nil), legs...)
	seenSides := map[string]bool{}
	clockID := ""
	for i := range legs {
		leg := &legs[i]
		side := strings.ToUpper(strings.TrimSpace(leg.Side))
		if strings.ToLower(strings.TrimSpace(leg.Venue)) != "kalshi" ||
			strings.TrimSpace(leg.Ticker) != strings.TrimSpace(pair.Ticker) ||
			(side != "YES" && side != "NO") || seenSides[side] ||
			leg.QuoteObserved.IsZero() || decisionAt.Before(leg.QuoteObserved) ||
			strings.TrimSpace(leg.BookSource) == "" || strings.TrimSpace(leg.FeeSource) == "" ||
			strings.TrimSpace(leg.SourceClockID) == "" || leg.Ask <= 0 || leg.Ask >= 1 ||
			leg.Depth < 1 || leg.Tick <= 0 || leg.Fee < 0 || leg.QuoteAge < 0 ||
			math.IsNaN(leg.Ask) || math.IsInf(leg.Ask, 0) ||
			math.IsNaN(leg.Depth) || math.IsInf(leg.Depth, 0) ||
			math.IsNaN(leg.Tick) || math.IsInf(leg.Tick, 0) ||
			math.IsNaN(leg.Fee) || math.IsInf(leg.Fee, 0) ||
			math.IsNaN(leg.QuoteAge) || math.IsInf(leg.QuoteAge, 0) {
			return concretePairInput{}, false
		}
		if clockID == "" {
			clockID = leg.SourceClockID
		} else if clockID != leg.SourceClockID {
			return concretePairInput{}, false
		}
		leg.Side = side
		seenSides[side] = true
	}
	if !seenSides["YES"] || !seenSides["NO"] {
		return concretePairInput{}, false
	}
	certificate := storage.R138HashJSON(map[string]any{
		"system": "flow-direction-integrity", "selector": step7FlowAtomicSelector,
		"trade_id": pair.TradeID, "ticker": pair.Ticker, "selected_side": selected,
		"canonical_event_id": identity.EventID, "event_version": identity.EventVersion,
		"canonical_payoff_id": identity.PayoffID, "payoff_version": identity.PayoffVersion,
		"instrument_version": identity.InstrumentVersion, "identity_status": identityStatus,
		"projection_contract":       "step7-frozen-outbox-v1",
		"flow_projection_semantics": step7FlowProjectionSemantics,
	})
	return concretePairInput{
		System: "flow-direction-integrity", Opportunity: "authoritative-flow-v3|" + pair.PairID,
		Cohort:          step7FlowAtomicCohort,
		SourceArtifact:  "Kalshi venue-local public-trade side plus one frozen complete two-sided book",
		CertificateHash: certificate, Blocker: "same-clock opposite-side matched no-order control",
		SelectedSide: selected, SelectorID: step7FlowAtomicSelector, Identity: identity,
		Observed: pair.Observed, DecisionAt: decisionAt, Legs: legs,
		Inputs: map[string]any{
			"authoritative_side": selected,
			"inferred_side":      strings.ToUpper(strings.TrimSpace(pair.InferredSide)),
			"comparison_status":  pair.ComparisonStatus, "trade_id": pair.TradeID,
			"trade_units": pair.Count, "trade_yes_price": pair.YesPrice,
			"trade_observed_ts":         pair.Observed.UTC().Format(time.RFC3339Nano),
			"projection_frozen_at":      decisionAt.UTC().Format(time.RFC3339Nano),
			"authoritative_fields_only": true, "inferred_label_never_selects_action": true,
			"frozen_instrument_version":        identity.InstrumentVersion,
			"frozen_identity_status":           identityStatus,
			"identity_scope":                   "kalshi_exact_ticker_payoff_route",
			"cross_venue_equivalence_verified": false,
			"settlement_observed":              false,
			"profit_authority":                 false,
			"paper_authority":                  false,
			"live_authority":                   false,
			"projection_contract":              "step7-frozen-outbox-v1",
			"flow_projection_semantics":        step7FlowProjectionSemantics,
		},
	}, true
}

// step7FlowVenueLocalIdentityStatus admits only an exact, versioned Kalshi ticker -> event/payoff
// mapping. A structural mapping is enough to retain a blocked venue-local route observation, but
// it is deliberately not upgraded to verified and cannot become a Paper/LIVE candidate.
func step7FlowVenueLocalIdentityStatus(identity storage.CurrentCanonicalInstrument,
	ticker string) (string, bool) {
	status := strings.ToLower(strings.TrimSpace(identity.IdentityStatus))
	if (status != "verified" && status != "structural") ||
		strings.ToLower(strings.TrimSpace(identity.Venue)) != "kalshi" ||
		strings.TrimSpace(identity.Ticker) != strings.TrimSpace(ticker) ||
		identity.InstrumentVersion <= 0 || strings.TrimSpace(identity.EventID) == "" ||
		identity.EventVersion <= 0 || strings.TrimSpace(identity.PayoffID) == "" ||
		identity.PayoffVersion <= 0 {
		return "", false
	}
	return status, true
}

func step7FrozenConcretePairOutputs(in concretePairInput, triggerAt time.Time,
	experimentVersion int) ([]storage.Step7FrozenObservation, error) {
	identityStatus, identityOK := step7FlowVenueLocalIdentityStatus(in.Identity, in.Identity.Ticker)
	if len(in.Legs) != 2 || !identityOK {
		return nil, errors.New("frozen concrete pair lacks exact canonical identity")
	}
	decisionAt := in.DecisionAt
	if decisionAt.IsZero() {
		decisionAt = in.Observed
	}
	latencyMS := math.Max(0, decisionAt.Sub(triggerAt).Seconds()*1000)
	outputs := make([]storage.Step7FrozenObservation, 0, 2)
	for _, leg := range in.Legs {
		selected := (in.SelectedSide == "YES" || in.SelectedSide == "NO") && leg.Side == in.SelectedSide
		inputs := map[string]any{"paired_same_clock": true, "pair_system": in.System,
			"pair_opportunity":   in.Opportunity,
			"direction_selected": selected, "economic_candidate": selected && identityStatus == "verified",
			"frozen_identity_status":           identityStatus,
			"identity_scope":                   "kalshi_exact_ticker_payoff_route",
			"cross_venue_equivalence_verified": false,
			"settlement_observed":              false, "profit_authority": false,
			"paper_authority": false, "live_authority": false,
			"trigger_to_pricing_latency_ms": latencyMS}
		for key, value := range in.Inputs {
			inputs[key] = value
		}
		kind, cohort, blocker := "control", in.Cohort, in.Blocker
		inputs["selector_id"], inputs["selector_frozen_before_observation"] = in.SelectorID, true
		inputs["selected_side"], inputs["selection_role"] = in.SelectedSide, "matched-control"
		cohort += "|matched-control|policy=" + in.SelectorID
		if selected {
			if identityStatus == "verified" {
				kind, blocker, inputs["selection_role"] = "candidate", "", "selected-action"
				cohort = in.Cohort + "|candidate|policy=" + in.SelectorID
			} else {
				kind, blocker, inputs["selection_role"] = "negative", step7FlowStructuralBlocker,
					"structural-action-observation"
				cohort = in.Cohort + "|structural-action|policy=" + in.SelectorID
			}
		}
		obs := storage.ResearchSystemObservation{
			Observed: in.Observed, DecisionAt: decisionAt, SystemID: in.System, ExperimentVersion: experimentVersion,
			OpportunityID: in.Opportunity + "|" + leg.Side, Kind: kind, Cohort: cohort,
			CanonicalEventID: in.Identity.EventID, EventVersion: in.Identity.EventVersion,
			CanonicalPayoffID: in.Identity.PayoffID, PayoffVersion: in.Identity.PayoffVersion,
			Venue: leg.Venue, Ticker: leg.Ticker, Route: "taker", Side: leg.Side,
			CertificateStatus: identityStatus, CertificateHash: in.CertificateHash,
			SourceClockID:  concreteLegClockID(leg, decisionAt),
			SourceArtifact: in.SourceArtifact, BookSource: leg.BookSource, FeeSource: leg.FeeSource,
			QuoteAgeMax: leg.QuoteAge, TickMin: leg.Tick, Size: 1, Cost: leg.Ask, Fee: leg.Fee,
			PayoutLower: 0, PayoutUpper: 1, NetLower: -leg.Ask - leg.Fee,
			NetUpper: 1 - leg.Ask - leg.Fee, VisibleCapacity: leg.Depth,
			DecisionLatencyMS: latencyMS, LatencyKnown: true, QuoteAgeKnown: true,
			TickKnown: true, DepthKnown: true, FeeKnown: true,
			CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: leg.Ask, Fee: leg.Fee,
				PayoutFloor: 0, NetFloor: -leg.Ask - leg.Fee}},
			OutcomeStatus: "open", Blocker: blocker,
			Candidate: selected && identityStatus == "verified", Inputs: inputs,
		}
		outputs = append(outputs, storage.Step7FrozenObservation{
			Observation: obs, InstrumentVersion: in.Identity.InstrumentVersion,
		})
	}
	return outputs, nil
}
