package server

// Concrete, zero-authority paired research cohorts for R139. Every economic row is repriced from
// one current side-specific book snapshot with exact depth/tick/fee. Discovery signals and rule
// artifacts choose cohorts; they never supply the money price. Paired controls later grade through
// the common authoritative settlement pipeline.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

var concreteSystemIDs = []string{"paired-bridge-inversion", "side-normalized-crowding-fade",
	"clientele-clock-basis", "attention-spillover-graph", "series-roll-anchor",
	"semantic-complexity-premium", "forecast-persona-router", "collateral-release-rotation",
	"settlement-latency-carry"}

const (
	concreteSignalPairsConsumer = "paired-and-attention"
	concreteSignalPageLimit     = 500
)

type concreteResearchSchedule struct {
	sync.Mutex
	last time.Time
}

var concreteResearchSchedules sync.Map

func (s *Server) concreteSchedule() *concreteResearchSchedule {
	v, _ := concreteResearchSchedules.LoadOrStore(s, &concreteResearchSchedule{})
	return v.(*concreteResearchSchedule)
}

func concreteBridgeFamily(v string) bool {
	v = strings.ToLower(strings.TrimSpace(v))
	return strings.Contains(v, "bridge") || strings.HasPrefix(v, "xvgap") || v == "xmatch" || v == "pmatch"
}

func concreteOppositeSide(side string) string {
	switch strings.ToUpper(strings.TrimSpace(side)) {
	case "YES":
		return "NO"
	case "NO":
		return "YES"
	default:
		return ""
	}
}

// concreteNativeInverseIdentity freezes every independently named inverse into its own immutable
// selector/cohort.  paired-bridge-inversion is the common experiment machinery, not a pooled
// economic family: the selector hash includes the originating family, venue, emitted side,
// opposite bought side and route, so two strategies can never share an untouched test cell.
func concreteNativeInverseIdentity(family, venue, firedSide string) (strategyFamily, selectorID, cohort string, ok bool) {
	family = strings.ToLower(strings.TrimSpace(family))
	venue = strings.ToLower(strings.TrimSpace(venue))
	firedSide = strings.ToUpper(strings.TrimSpace(firedSide))
	invertedSide := concreteOppositeSide(firedSide)
	if family == "" || (venue != "kalshi" && venue != "polyus") || invertedSide == "" ||
		strings.HasPrefix(family, "invert:") || strings.HasPrefix(family, "side-control:") ||
		strings.HasPrefix(family, "counterfactual:") {
		return "", "", "", false
	}
	strategyFamily = "invert:" + family
	selectorHash := strings.TrimPrefix(storage.R138HashJSON(map[string]any{
		"contract": "native-inverse-v1", "family": strategyFamily, "venue": venue,
		"fired_side": firedSide, "inverted_side": invertedSide, "route": "taker",
	}), "sha256:")
	selectorID = "native-inverse-v1:" + selectorHash
	cohort = "native-inverse-v1|selector=" + selectorID
	return strategyFamily, selectorID, cohort, true
}

func concreteCrowdingTrigger(v storage.ConcreteSignalTrigger) bool {
	f := strings.ToLower(v.Family)
	return v.FlowN > 0 || v.HoldersHHI > 0 || v.Concentration > 0 || v.Notional > 0 ||
		strings.Contains(f, "whale") || strings.Contains(f, "flow") || strings.Contains(f, "consensus")
}

func concreteAttentionTrigger(v storage.ConcreteSignalTrigger) bool {
	return v.Notional >= 1000 || v.Strength >= .80 || math.Abs(v.FlowRatio) >= .80 ||
		math.Abs(v.Momentum) >= .05
}

func (s *Server) concreteExactPair(venue, ticker string, poly map[string]r138LinkedBook) ([]nativeExactLeg, bool) {
	legs := make([]nativeExactLeg, 0, 2)
	clockID := ""
	for _, side := range []string{"YES", "NO"} {
		leg, ok := s.nativeCurrentBasketLeg(venue, storage.BasketLeg{Ticker: ticker, Side: side}, poly)
		if !ok || leg.QuoteAge < 0 || leg.Depth < 1 || leg.Tick <= 0 || leg.FeeSource == "" || leg.SourceClockID == "" {
			return nil, false
		}
		if clockID == "" {
			clockID = leg.SourceClockID
		} else if leg.SourceClockID != clockID {
			// A delta between side reads means this is not one internally consistent two-sided frame.
			return nil, false
		}
		legs = append(legs, leg)
	}
	return legs, true
}

func concreteLegClockID(leg nativeExactLeg, observed time.Time) string {
	if strings.TrimSpace(leg.SourceClockID) != "" {
		return leg.SourceClockID
	}
	// Legacy stored-signal rows predate exact per-frame clocks. They remain research-only and are
	// explicitly distinguishable from venue-source receipts; current exact collectors never use it.
	return "legacy-receipt-clock:" + leg.BookSource + "|" + observed.UTC().Format(time.RFC3339Nano)
}

type concretePairInput struct {
	System, Opportunity, Cohort, SourceArtifact, CertificateHash, Blocker string
	// SelectedSide is set only when a deterministic policy was frozen before this observation.
	// The selected executable lane becomes a prospective candidate; its same-clock opposite is a
	// matched control in a different cohort. An empty value deliberately records controls only.
	SelectedSide, SelectorID string
	Identity                 storage.CurrentCanonicalInstrument
	Observed                 time.Time
	// DecisionAt is optional for legacy/current collectors. Frozen outbox projections set it
	// explicitly so the economic row stays keyed to the raw trigger window while full
	// trigger-to-pricing latency remains exact.
	DecisionAt time.Time
	Legs       []nativeExactLeg
	Inputs     map[string]any
}

func (s *Server) insertConcretePair(ctx context.Context, in concretePairInput) (inserted, duplicates int, err error) {
	if len(in.Legs) != 2 || in.Identity.IdentityStatus != "verified" || in.Identity.EventID == "" ||
		in.Identity.PayoffID == "" {
		return 0, 0, fmt.Errorf("concrete pair lacks verified canonical instrument identity")
	}
	for _, leg := range in.Legs {
		selected := (in.SelectedSide == "YES" || in.SelectedSide == "NO") && leg.Side == in.SelectedSide
		inputs := map[string]any{"paired_same_clock": true, "pair_system": in.System,
			"pair_opportunity": in.Opportunity, "economic_candidate": selected,
			"paper_authority": false, "live_authority": false}
		for k, v := range in.Inputs {
			inputs[k] = v
		}
		if training, _ := inputs["selector_training"].(bool); training {
			if _, ok := inputs["selector_arm"]; !ok {
				inputs["selector_arm"] = leg.Side
			}
			if _, ok := inputs["selector_scope"]; !ok {
				inputs["selector_scope"] = "venue=" + leg.Venue
			}
		}
		kind, cohort, blocker := "control", in.Cohort, in.Blocker
		if in.SelectedSide == "YES" || in.SelectedSide == "NO" {
			inputs["selector_id"], inputs["selector_frozen_before_observation"] = in.SelectorID, true
			inputs["selected_side"], inputs["selection_role"] = in.SelectedSide, "matched-control"
			cohort += "|matched-control|policy=" + in.SelectorID
			if selected {
				kind, blocker, inputs["selection_role"] = "candidate", "", "selected-action"
				cohort = in.Cohort + "|candidate|policy=" + in.SelectorID
			}
		}
		opportunityID := in.Opportunity + "|" + leg.Side
		exists, existsErr := s.store.ConcreteResearchObservationExists(ctx, in.System, opportunityID,
			"taker", kind, 1)
		if existsErr != nil {
			return inserted, duplicates, existsErr
		}
		if exists {
			duplicates++
			continue
		}
		_, ok, insertErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: in.Observed, SystemID: in.System,
			OpportunityID: opportunityID, Kind: kind, Cohort: cohort,
			CanonicalEventID: in.Identity.EventID, EventVersion: in.Identity.EventVersion,
			CanonicalPayoffID: in.Identity.PayoffID, PayoffVersion: in.Identity.PayoffVersion,
			Venue: leg.Venue, Ticker: leg.Ticker, Route: "taker", Side: leg.Side,
			CertificateStatus: "verified", CertificateHash: in.CertificateHash,
			SourceClockID:  concreteLegClockID(leg, in.Observed),
			SourceArtifact: in.SourceArtifact, BookSource: leg.BookSource, FeeSource: leg.FeeSource,
			QuoteAgeMax: leg.QuoteAge, TickMin: leg.Tick, Size: 1, Cost: leg.Ask, Fee: leg.Fee,
			PayoutLower: 0, PayoutUpper: 1, NetLower: -leg.Ask - leg.Fee,
			NetUpper: 1 - leg.Ask - leg.Fee, VisibleCapacity: leg.Depth,
			DecisionLatencyMS: math.Max(0, time.Since(in.Observed).Seconds()*1000),
			LatencyKnown:      true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
			CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: leg.Ask, Fee: leg.Fee,
				PayoutFloor: 0, NetFloor: -leg.Ask - leg.Fee}},
			OutcomeStatus: "open", Blocker: blocker, Candidate: selected, Inputs: inputs,
		})
		if insertErr != nil {
			return inserted, duplicates, insertErr
		}
		if ok {
			inserted++
		} else {
			duplicates++
		}
	}
	return inserted, duplicates, nil
}

func (s *Server) insertConcreteOne(ctx context.Context, system, opportunity, cohort, source,
	certificate, blocker string, identity storage.CurrentCanonicalInstrument, observed time.Time,
	leg nativeExactLeg, inputs map[string]any, candidate bool) (bool, error) {
	if identity.IdentityStatus != "verified" || identity.EventID == "" || identity.PayoffID == "" {
		return false, fmt.Errorf("concrete route lacks verified canonical identity")
	}
	if inputs == nil {
		inputs = map[string]any{}
	}
	inputs["economic_candidate"] = candidate
	inputs["paper_authority"], inputs["live_authority"] = false, false
	kind := "control"
	if candidate {
		kind, blocker = "candidate", ""
	}
	exists, existsErr := s.store.ConcreteResearchObservationExists(ctx, system, opportunity, "taker", kind, 1)
	if existsErr != nil {
		return false, existsErr
	}
	if exists {
		return false, nil
	}
	latencyMS := math.Max(0, time.Since(observed).Seconds()*1000)
	if stored, ok := inputs["stored_decision_latency_ms"].(float64); ok && stored >= 0 {
		latencyMS = stored
	}
	_, inserted, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: observed, SystemID: system, OpportunityID: opportunity, Kind: kind, Cohort: cohort,
		CanonicalEventID: identity.EventID, EventVersion: identity.EventVersion,
		CanonicalPayoffID: identity.PayoffID, PayoffVersion: identity.PayoffVersion,
		Venue: leg.Venue, Ticker: leg.Ticker, Route: "taker", Side: leg.Side,
		CertificateStatus: "verified", CertificateHash: certificate,
		SourceClockID:  concreteLegClockID(leg, observed),
		SourceArtifact: source, BookSource: leg.BookSource, FeeSource: leg.FeeSource,
		QuoteAgeMax: leg.QuoteAge, TickMin: leg.Tick, Size: 1, Cost: leg.Ask, Fee: leg.Fee,
		PayoutLower: 0, PayoutUpper: 1, NetLower: -leg.Ask - leg.Fee,
		NetUpper: 1 - leg.Ask - leg.Fee, VisibleCapacity: leg.Depth,
		DecisionLatencyMS: latencyMS,
		LatencyKnown:      true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
		CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: leg.Ask, Fee: leg.Fee,
			PayoutFloor: 0, NetFloor: -leg.Ask - leg.Fee}}, OutcomeStatus: "open",
		Blocker: blocker, Candidate: candidate, Inputs: inputs,
	})
	return inserted, err
}

func (s *Server) insertConcreteStoredSignal(ctx context.Context, system, opportunity, cohort,
	source, certificate, blocker string, identity storage.CurrentCanonicalInstrument,
	trigger storage.ConcreteSignalTrigger, inputs map[string]any, candidate bool) (bool, error) {
	if strings.TrimSpace(trigger.FeeSource) == "" {
		return false, fmt.Errorf("stored book-native signal lacks persisted exact fee authority")
	}
	leg := nativeExactLeg{Venue: trigger.Platform, Ticker: trigger.Ticker, Side: trigger.Side,
		BookSource: trigger.BookSource, FeeSource: trigger.FeeSource,
		Ask: trigger.BookAsk, Depth: trigger.AskDepth, Tick: trigger.TakerTick,
		Fee: trigger.TakerFee, QuoteAge: trigger.QuoteAge}
	// Preserve the actual measured decision latency instead of replacing it with insertion delay.
	if inputs == nil {
		inputs = map[string]any{}
	}
	inputs["stored_decision_latency_ms"] = trigger.LatencyMS
	inputs["source_signal_id"] = trigger.ID
	inserted, err := s.insertConcreteOne(ctx, system, opportunity, cohort, source, certificate,
		blocker, identity, trigger.Observed, leg, inputs, candidate)
	return inserted, err
}

func concreteRuleFeatures(rawRules, rawSources string) map[string]any {
	text := strings.ToLower(rawRules)
	clauses := 1 + strings.Count(text, ";") + strings.Count(text, " if ") +
		strings.Count(text, " unless ") + strings.Count(text, " except ")
	var sources []any
	_ = json.Unmarshal([]byte(rawSources), &sources)
	features := map[string]any{"utf8_bytes": len([]byte(rawRules)), "clause_count": clauses,
		"settlement_source_count": len(sources), "has_early_close": strings.Contains(text, "early_close"),
		"has_void_or_cancel":         strings.Contains(text, "void") || strings.Contains(text, "cancel"),
		"has_revision_or_discretion": strings.Contains(text, "revise") || strings.Contains(text, "discretion"),
		"has_timezone_boundary":      strings.Contains(text, "timezone") || strings.Contains(text, " et") || strings.Contains(text, "utc")}
	features["complexity_score_v1"] = math.Log1p(float64(len([]byte(rawRules)))) + float64(clauses) +
		2*float64(len(sources))
	return features
}

func concreteComplexityBin(score float64) string {
	switch {
	case score < 8:
		return "low_lt8"
	case score < 16:
		return "medium_8_15"
	default:
		return "high_16plus"
	}
}

func (s *Server) collectConcreteSignalPairs(ctx context.Context, now time.Time, poly map[string]r138LinkedBook,
	metrics map[string]int) {
	cursor, err := s.store.ConcreteSignalCursor(ctx, concreteSignalPairsConsumer)
	if err != nil {
		metrics["signal_cursor_read_error"]++
		return
	}
	metrics["signal_cursor_before"] = int(cursor)
	triggers, err := s.store.ConcreteSignalTriggersAfter(ctx, now.Add(-10*time.Minute), cursor, concreteSignalPageLimit)
	if err != nil {
		metrics["signal_source_error"]++
		return
	}
	metrics["signal_page_rows"] = len(triggers)
	pageFailed := false
	gameRows := map[string][]storage.MarketGameRow{}
	for _, venue := range []string{"kalshi", "polyus"} {
		rows, rowsErr := s.store.MarketGameRows(ctx, venue)
		if rowsErr != nil {
			metrics["signal_game_rows_error"]++
			pageFailed = true
			continue
		}
		for _, row := range rows {
			gameRows[venue+"|"+row.GameID] = append(gameRows[venue+"|"+row.GameID], row)
		}
	}
	for _, trigger := range triggers {
		identity, ok, identityErr := s.store.CurrentCanonicalInstrument(ctx, trigger.Platform, trigger.Ticker)
		if identityErr != nil {
			metrics["canonical_identity_read_error"]++
			pageFailed = true
			continue
		}
		if !ok || identity.IdentityStatus != "verified" {
			metrics["canonical_identity_unavailable"]++
			continue
		}
		legs, pairOK := s.concreteExactPair(trigger.Platform, trigger.Ticker, poly)
		if !pairOK {
			metrics["current_two_side_money_truth_unavailable"]++
			continue
		}
		baseInputs := map[string]any{"source_signal_id": trigger.ID, "source_family": trigger.Family,
			"source_side": trigger.Side, "source_observed": trigger.Observed,
			"book_repriced_at_decision": true, "discovery_price_used": false}
		// Every eligible one-leg detector gets a prospective, exact opposite-book candidate under
		// the shared paired-bridge machinery. Economics remain isolated by the frozen selector and
		// cohort; an already inverted/control family is rejected so inversion never recurses.
		if inverse, eligible := independentInverseSignalIdentity(storage.Signal{
			SignalType: trigger.Family, Platform: trigger.Platform, Ticker: trigger.Ticker, Side: trigger.Side,
		}); eligible {
			strategyFamily, selectorID, cohort, identityOK := concreteNativeInverseIdentity(
				trigger.Family, trigger.Platform, trigger.Side)
			if identityOK && inverse.SignalType == strategyFamily {
				inverseInputs := map[string]any{}
				for k, v := range baseInputs {
					inverseInputs[k] = v
				}
				inverseInputs["strategy_family"] = strategyFamily
				inverseInputs["fired_side"] = strings.ToUpper(trigger.Side)
				inverseInputs["inverted_side"] = inverse.Side
				inverseInputs["execution_route"] = "taker"
				in := concretePairInput{System: "paired-bridge-inversion",
					Opportunity: fmt.Sprintf("native-inverse|%s|%s|%d", trigger.Platform, trigger.Ticker, trigger.ID),
					Cohort:      cohort, SourceArtifact: "book-native signal repriced on its independently executable opposite side",
					CertificateHash: storage.R138HashJSON(map[string]any{"identity": identity,
						"trigger": trigger.ID, "strategy_family": strategyFamily, "selector_id": selectorID}),
					Blocker:      "same-clock emitted-side matched control; zero trading authority",
					SelectedSide: inverse.Side, SelectorID: selectorID,
					Identity: identity, Observed: now, Legs: legs, Inputs: inverseInputs}
				i, d, e := s.insertConcretePair(ctx, in)
				metrics["native_inverse_inserted"] += i
				metrics["native_inverse_duplicates"] += d
				if e != nil {
					metrics["native_inverse_insert_error"]++
					pageFailed = true
				}
			}
		}
		if concreteBridgeFamily(trigger.Family) {
			direct, inverse := strings.ToUpper(trigger.Side), concreteOppositeSide(trigger.Side)
			if inverse == "" {
				metrics["paired_bridge_direction_unavailable"]++
			} else {
				for _, arm := range []struct{ name, side string }{{"direct", direct}, {"inverse", inverse}} {
					in := concretePairInput{System: "paired-bridge-inversion",
						Opportunity: fmt.Sprintf("bridge|%s|%s|%d|%s", trigger.Platform, trigger.Ticker, trigger.ID, arm.name),
						Cohort:      "same-clock-bridge-" + arm.name, SourceArtifact: "book-native bridge trigger repriced at one current two-sided book",
						CertificateHash: storage.R138HashJSON(map[string]any{"identity": identity, "trigger": trigger.ID, "arm": arm.name}),
						Blocker:         "same-clock matched opposite control; zero trading authority",
						SelectedSide:    arm.side, SelectorID: "bridge-" + arm.name + "-frozen-v1",
						Identity: identity, Observed: now, Legs: legs, Inputs: baseInputs}
					i, d, e := s.insertConcretePair(ctx, in)
					metrics["paired_bridge_inserted"] += i
					metrics["paired_bridge_duplicates"] += d
					if e != nil {
						metrics["paired_bridge_insert_error"]++
						pageFailed = true
					}
				}
			}
		}
		if concreteCrowdingTrigger(trigger) {
			crowd := map[string]any{}
			for k, v := range baseInputs {
				crowd[k] = v
			}
			crowd["bought_side_normalized"] = true
			crowd["strength"], crowd["notional"] = trigger.Strength, trigger.Notional
			crowd["concentration"], crowd["imbalance"] = trigger.Concentration, trigger.Imbalance
			crowd["flow_ratio_15m"], crowd["flow_n_15m"] = trigger.FlowRatio, trigger.FlowN
			crowd["holders_hhi"], crowd["holders_skill"] = trigger.HoldersHHI, trigger.HoldersSkill
			in := concretePairInput{System: "side-normalized-crowding-fade",
				Opportunity: fmt.Sprintf("crowd|%s|%s|%d", trigger.Platform, trigger.Ticker, trigger.ID),
				Cohort:      "same-frame-direct-fade", SourceArtifact: "book-native crowding trigger repriced at one current two-sided book",
				CertificateHash: storage.R138HashJSON(map[string]any{"identity": identity, "trigger": trigger.ID, "coordinate": "bought-side"}),
				Blocker:         "same-clock direct-side matched control; zero trading authority",
				SelectedSide:    concreteOppositeSide(trigger.Side), SelectorID: "crowding-fade-opposite-source-v1",
				Identity: identity, Observed: now, Legs: legs, Inputs: crowd}
			if in.SelectedSide == "" {
				in.SelectorID = ""
			}
			i, d, e := s.insertConcretePair(ctx, in)
			metrics["crowding_inserted"] += i
			metrics["crowding_duplicates"] += d
			if e != nil {
				metrics["crowding_insert_error"]++
				pageFailed = true
			}
		}
		if !concreteAttentionTrigger(trigger) {
			continue
		}
		var parent storage.MarketGameRow
		found := false
		for _, rows := range gameRows {
			for _, row := range rows {
				if row.Venue == trigger.Platform && row.Ticker == trigger.Ticker && row.Src == "struct" {
					parent, found = row, true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			continue
		}
		children := gameRows[parent.Venue+"|"+parent.GameID]
		childN := 0
		for _, child := range children {
			if child.Ticker == parent.Ticker || child.Src != "struct" || child.MktType == parent.MktType || childN >= 2 {
				continue
			}
			childIdentity, childOK, childIdentityErr := s.store.CurrentCanonicalInstrument(ctx, child.Venue, child.Ticker)
			if childIdentityErr != nil {
				metrics["attention_identity_read_error"]++
				pageFailed = true
				continue
			}
			childLegs, bookOK := s.concreteExactPair(child.Venue, child.Ticker, poly)
			if !childOK || childIdentity.IdentityStatus != "verified" || !bookOK {
				continue
			}
			edge := storage.R138HashJSON(map[string]any{"game": parent.GameID, "parent": parent,
				"child": child, "horizons_s": []int{60, 300}})
			in := concretePairInput{System: "attention-spillover-graph",
				Opportunity: fmt.Sprintf("attention|%s|%s|%s|%d", parent.GameID, parent.Ticker, child.Ticker, trigger.ID),
				Cohort:      "structural-parent-child-60s-300s", SourceArtifact: "immutable market_game structural edge + book-native parent shock",
				CertificateHash: edge, Blocker: "paired child-side control; exact 60s/300s executable marks collect prospectively; untouched graph lower bound pending",
				Identity: childIdentity, Observed: now, Legs: childLegs,
				Inputs: map[string]any{"edge_hash": edge, "parent_ticker": parent.Ticker,
					"parent_market_type": parent.MktType, "child_market_type": child.MktType,
					"horizons_s": []int{60, 300}, "source_signal_id": trigger.ID,
					"selector_training": true,
					"selector_cell":     "parent=" + parent.MktType + "|child=" + child.MktType}}
			i, d, e := s.insertConcretePair(ctx, in)
			metrics["attention_inserted"] += i
			metrics["attention_duplicates"] += d
			if e != nil {
				metrics["attention_insert_error"]++
				pageFailed = true
			}
			childN++
		}
	}
	if len(triggers) == 0 || pageFailed || ctx.Err() != nil {
		return
	}
	durableCtx, cancel := researchDurabilityContext(ctx)
	defer cancel()
	if err := s.store.AdvanceConcreteSignalCursor(durableCtx, concreteSignalPairsConsumer,
		triggers[len(triggers)-1].ID); err != nil {
		metrics["signal_cursor_advance_error"]++
		return
	}
	metrics["signal_cursor_advanced"] += len(triggers)
	metrics["signal_cursor_after"] = int(triggers[len(triggers)-1].ID)
}

func (s *Server) collectConcreteRuleAndPersona(ctx context.Context, now time.Time, poly map[string]r138LinkedBook,
	metrics map[string]int) {
	rules, err := s.store.RecentConcreteRuleInputs(ctx, 20)
	if err == nil {
		for _, rule := range rules {
			identity, ok, _ := s.store.CurrentCanonicalInstrument(ctx, rule.Venue, rule.InstrumentID)
			legs, bookOK := s.concreteExactPair(rule.Venue, rule.InstrumentID, poly)
			if !ok || identity.IdentityStatus != "verified" || !bookOK {
				metrics["semantic_current_money_truth_unavailable"]++
				continue
			}
			features := concreteRuleFeatures(rule.RawRules, rule.SettlementSources)
			score, _ := features["complexity_score_v1"].(float64)
			features["selector_training"] = true
			features["selector_cell"] = "complexity=" + concreteComplexityBin(score)
			in := concretePairInput{System: "semantic-complexity-premium",
				Opportunity: "semantic|" + rule.Venue + "|" + rule.InstrumentID + "|" + rule.ArtifactHash,
				Cohort:      "deterministic-rule-complexity-v1", SourceArtifact: rule.Source,
				CertificateHash: rule.ArtifactHash,
				Blocker:         "paired rule-complexity control; frozen complexity-bin calibration and untouched lower bound pending",
				Identity:        identity, Observed: now, Legs: legs, Inputs: features}
			i, d, e := s.insertConcretePair(ctx, in)
			metrics["semantic_inserted"] += i
			metrics["semantic_duplicates"] += d
			if e != nil {
				metrics["semantic_insert_error"]++
			}
		}
	}
	personas, err := s.store.FrozenPersonaCandidates(ctx, now, 20)
	if err == nil {
		for _, persona := range personas {
			identity, ok, _ := s.store.CurrentCanonicalInstrument(ctx, persona.Platform, persona.Ticker)
			legs, bookOK := s.concreteExactPair(persona.Platform, persona.Ticker, poly)
			if !ok || identity.IdentityStatus != "verified" || !bookOK {
				metrics["persona_current_money_truth_unavailable"]++
				continue
			}
			selected := strings.ToUpper(persona.SelectedSide)
			if selected != "YES" && selected != "NO" {
				continue
			}
			// Keep the selected action beside its same-clock opposite-side control. This tests the
			// frozen router without allowing its prior-window point mean to authorize money.
			in := concretePairInput{System: "forecast-persona-router",
				Opportunity: "persona|" + persona.Platform + "|" + persona.Ticker + "|" + persona.StrategyMode + "|" + persona.SelectedTransform + "|" + persona.FreezeEnd.Format("20060102"),
				Cohort:      "frozen-prior-window-router", SourceArtifact: "research_proper_score_trials prior-only selector",
				CertificateHash: storage.R138HashJSON(persona),
				Blocker:         "same-clock opposite-side matched control; zero trading authority",
				SelectedSide:    selected, SelectorID: "persona-prior-window-router-v1",
				Identity: identity, Observed: now, Legs: legs, Inputs: map[string]any{
					"selected_transform": persona.SelectedTransform, "selected_side": selected,
					"strategy_mode": persona.StrategyMode,
					"freeze_start":  persona.FreezeStart, "freeze_end": persona.FreezeEnd,
					"brier_n": persona.BrierN, "log_n": persona.LogN, "spherical_n": persona.SphericalN,
					"brier_mean": persona.BrierMean, "log_mean": persona.LogMean,
					"spherical_mean":                 persona.SphericalMean,
					"selection_uses_evaluation_rows": false, "tie_break": "brier"}}
			i, d, e := s.insertConcretePair(ctx, in)
			metrics["persona_inserted"] += i
			metrics["persona_duplicates"] += d
			if e != nil {
				metrics["persona_insert_error"]++
			}
		}
	}
}

func concreteCreditHash(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return "sha256:" + hex.EncodeToString(h[:])
}

func (s *Server) collectResearchCreditEvents(ctx context.Context, metrics map[string]int) {
	if s.kal != nil {
		if rows, err := s.kal.GetSettlements(ctx, 100); err == nil {
			for _, row := range rows {
				at, parseErr := time.Parse(time.RFC3339Nano, row.SettledTime)
				if parseErr != nil {
					at, parseErr = time.Parse(time.RFC3339, row.SettledTime)
				}
				if parseErr != nil || row.Ticker == "" {
					metrics["kalshi_credit_malformed"]++
					continue
				}
				qty := row.YesCount.Float() + row.NoCount.Float()
				amount := row.RevenueUSD()
				inserted, insertErr := s.store.InsertResearchCreditEvent(ctx, storage.ResearchCreditEvent{
					Venue: "kalshi", InstrumentID: row.Ticker, CreditAt: at, Quantity: qty,
					CreditedAmount: amount, AmountKnown: true, SettlePx: -1,
					SourceArtifact: "Kalshi GET /portfolio/settlements settled_time+revenue",
					SourceHash:     concreteCreditHash(row.Ticker, row.SettledTime, fmt.Sprint(qty), fmt.Sprint(amount)),
				})
				if insertErr != nil {
					metrics["kalshi_credit_insert_error"]++
				} else if inserted {
					metrics["kalshi_credit_inserted"]++
				}
			}
		} else {
			metrics["kalshi_credit_source_error"]++
		}
	}
	if s.polyUSAuth != nil {
		if rows, err := s.polyUSAuth.Activities(ctx); err == nil {
			for _, row := range rows {
				if row.Type != "POSITION_RESOLUTION" || row.Slug == "" {
					continue
				}
				at, parseErr := time.Parse(time.RFC3339Nano, row.Time)
				if parseErr != nil {
					at, parseErr = time.Parse(time.RFC3339, row.Time)
				}
				if parseErr != nil {
					metrics["polyus_credit_malformed"]++
					continue
				}
				inserted, insertErr := s.store.InsertResearchCreditEvent(ctx, storage.ResearchCreditEvent{
					Venue: "polyus", InstrumentID: row.Slug, CreditAt: at, Quantity: row.Qty,
					RealizedPnL: row.PnL, SettlePx: row.SettlePx, AmountKnown: false,
					SourceArtifact: "PolyUS GET /v1/portfolio/activities positionResolution.updateTime; credited cash amount absent",
					SourceHash:     concreteCreditHash(row.Slug, row.Time, fmt.Sprint(row.Qty), fmt.Sprint(row.PnL), fmt.Sprint(row.SettlePx)),
				})
				if insertErr != nil {
					metrics["polyus_credit_insert_error"]++
				} else if inserted {
					metrics["polyus_credit_inserted"]++
				}
			}
		} else {
			metrics["polyus_credit_source_error"]++
		}
	}
}

func concreteLiquidityBin(depth float64) string {
	switch {
	case depth < 5:
		return "thin_lt5"
	case depth < 25:
		return "medium_5_24"
	default:
		return "deep_25plus"
	}
}

func concreteGamePhase(now time.Time, startRaw string) string {
	start, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(startRaw))
	if err != nil {
		start, err = time.Parse(time.RFC3339, strings.TrimSpace(startRaw))
	}
	if err != nil {
		return "start_unknown"
	}
	if now.Before(start) {
		return "pre"
	}
	if now.Before(start.Add(6 * time.Hour)) {
		return "live_window"
	}
	return "post"
}

func (s *Server) collectConcreteClienteleSeriesAndCredit(ctx context.Context, now time.Time,
	poly map[string]r138LinkedBook, metrics map[string]int) {
	games, _ := s.store.GameIdentityRows(ctx)
	gameStart := map[string]string{}
	gameLeague := map[string]string{}
	for _, game := range games {
		gameStart[game.GameID] = game.StartUTC
		gameLeague[game.GameID] = strings.ToLower(strings.TrimSpace(game.League))
	}
	et, _ := time.LoadLocation("America/New_York")
	if et == nil {
		et = time.FixedZone("ET", -5*3600)
	}
	pairs, pairErr := s.store.StructuralKPolyRulePairs(ctx, now.Add(-20*time.Minute), 30)
	if pairErr != nil {
		metrics["clientele_pair_source_error"]++
	}
	for _, pair := range pairs {
		cert, current, certErr := s.store.CompatibleRulePairCertificate(ctx, pair.KalshiTicker, pair.PolyUSSlug)
		if certErr != nil || !current {
			metrics["clientele_rule_certificate_unavailable"]++
			continue
		}
		kIdentity, kok, _ := s.store.CurrentCanonicalInstrument(ctx, "kalshi", pair.KalshiTicker)
		pIdentity, pok, _ := s.store.CurrentCanonicalInstrument(ctx, "polyus", pair.PolyUSSlug)
		kLegs, kb := s.concreteExactPair("kalshi", pair.KalshiTicker, poly)
		pLegs, pb := s.concreteExactPair("polyus", pair.PolyUSSlug, poly)
		if !kok || !pok || kIdentity.IdentityStatus != "verified" || pIdentity.IdentityStatus != "verified" || !kb || !pb {
			metrics["clientele_current_money_truth_unavailable"]++
			continue
		}
		arms := concreteClienteleExactArms(pair.Orientation, kLegs, pLegs)
		if len(arms) != 4 {
			metrics["clientele_exact_side_set_unavailable"]++
			continue
		}
		inputs := map[string]any{"pair_id": pair.PairID, "rule_certificate": cert.SpecHash,
			"venue_local_hour_et": now.In(et).Hour(), "phase": concreteGamePhase(now, gameStart[pair.GameID]),
			"orientation": pair.Orientation, "same_clock": true,
			"selector_training": true, "selector_scope": "crossvenue"}
		opportunity := "clientele|" + pair.PairID + "|" + now.UTC().Truncate(5*time.Minute).Format(time.RFC3339)
		blocker := "paired certified venue/hour/phase/liquidity control; untouched clock-cell lower bound pending"
		for _, arm := range arms {
			armInputs := map[string]any{}
			for key, value := range inputs {
				armInputs[key] = value
			}
			armInputs["selector_arm"] = arm.SelectorArm
			armInputs["canonical_side"] = arm.CanonicalSide
			armInputs["native_side"] = arm.NativeSide
			armInputs["left_side"] = arm.CanonicalSide
			pairDepth := arm.Leg.Depth
			for _, counterpart := range arms {
				if counterpart.Venue != arm.Venue && counterpart.CanonicalSide == arm.CanonicalSide {
					pairDepth = math.Min(pairDepth, counterpart.Leg.Depth)
					if counterpart.Venue == "polyus" {
						armInputs["right_side"] = counterpart.NativeSide
					} else if arm.Venue == "polyus" {
						armInputs["right_side"] = arm.NativeSide
					}
					break
				}
			}
			armInputs["liquidity_bin"] = concreteLiquidityBin(pairDepth)
			armInputs["selector_cell"] = fmt.Sprintf(
				"hour=%d|phase=%s|liquidity=%s|orientation=%s|canonical=%s",
				armInputs["venue_local_hour_et"], armInputs["phase"], armInputs["liquidity_bin"],
				pair.Orientation, arm.CanonicalSide)
			identity := kIdentity
			if arm.Venue == "polyus" {
				identity = pIdentity
			}
			if ok, err := s.insertConcreteOne(ctx, "clientele-clock-basis",
				opportunity+"|"+arm.Venue+"|"+arm.NativeSide,
				"certified-cross-venue-clock-cell", "current certified K-PUS books", cert.SpecHash,
				blocker, identity, now, arm.Leg, armInputs, false); err != nil {
				metrics["clientele_insert_error"]++
			} else if ok {
				metrics["clientele_inserted"]++
			}
		}
	}

	seriesRows, seriesErr := s.store.SeriesRollInputs(ctx, now, 20)
	if seriesErr != nil {
		metrics["series_source_error"]++
	}
	for _, row := range seriesRows {
		identity, ok, _ := s.store.CurrentCanonicalInstrument(ctx, "kalshi", row.CurrentTicker)
		legs, bookOK := s.concreteExactPair("kalshi", row.CurrentTicker, poly)
		if !ok || identity.IdentityStatus != "verified" || !bookOK {
			metrics["series_current_money_truth_unavailable"]++
			continue
		}
		anchorSide := "NO"
		if row.PriorSettle >= .5 {
			anchorSide = "YES"
		}
		inputs := map[string]any{"official_series_ticker": row.Series, "current_event": row.CurrentEvent,
			"prior_event": row.PriorEvent, "prior_ticker": row.PriorTicker, "prior_kind": row.Kind,
			"prior_open_ask": row.PriorAsk, "prior_open_fee": row.PriorFee,
			"prior_settle_yes": row.PriorSettle, "anchor_side": anchorSide,
			"ticker_prefix_inference": false, "current_book_repriced": true}
		in := concretePairInput{System: "series-roll-anchor",
			Opportunity: "series|" + row.Series + "|" + row.CurrentTicker + "|" + row.PriorTicker,
			Cohort:      "official-series-prior-issue-anchor", SourceArtifact: "official Kalshi series/event mapping + prior exact book settlement",
			CertificateHash: storage.R138HashJSON(inputs),
			Blocker:         "same-clock opposite-side matched control; zero trading authority",
			SelectedSide:    anchorSide, SelectorID: "series-prior-settlement-anchor-v1",
			Identity: identity, Observed: now, Legs: legs, Inputs: inputs}
		i, d, e := s.insertConcretePair(ctx, in)
		metrics["series_inserted"] += i
		metrics["series_duplicates"] += d
		if e != nil {
			metrics["series_insert_error"]++
		}
	}

	credits, creditErr := s.store.RecentResearchCreditEvents(ctx, now.Add(-30*time.Minute), 30)
	if creditErr != nil {
		metrics["credit_read_error"]++
	}
	marketRows := map[string]storage.MarketGameRow{}
	gameChildren := map[string][]storage.MarketGameRow{}
	for _, venue := range []string{"kalshi", "polyus"} {
		rows, _ := s.store.MarketGameRows(ctx, venue)
		for _, row := range rows {
			marketRows[venue+"|"+row.Ticker] = row
			gameChildren[venue+"|"+row.GameID] = append(gameChildren[venue+"|"+row.GameID], row)
		}
	}
	creditedGames := map[string]bool{}
	for _, credit := range credits {
		if row, ok := marketRows[credit.Venue+"|"+credit.InstrumentID]; ok {
			creditedGames[row.GameID] = true
		}
	}
	exactPairCache := map[string][]nativeExactLeg{}
	exactPairKnown := map[string]bool{}
	exactPair := func(row storage.MarketGameRow) ([]nativeExactLeg, bool) {
		key := row.Venue + "|" + row.Ticker
		if exactPairKnown[key] {
			legs, ok := exactPairCache[key]
			return legs, ok
		}
		exactPairKnown[key] = true
		legs, ok := s.concreteExactPair(row.Venue, row.Ticker, poly)
		if ok {
			exactPairCache[key] = legs
		}
		return legs, ok
	}
	for _, credit := range credits {
		if !credit.AmountKnown {
			metrics["collateral_credit_amount_unavailable"]++
			continue
		}
		parent, ok := marketRows[credit.Venue+"|"+credit.InstrumentID]
		if !ok {
			metrics["collateral_structural_link_unavailable"]++
			continue
		}
		childN := 0
		for _, child := range gameChildren[credit.Venue+"|"+parent.GameID] {
			if child.Ticker == parent.Ticker || childN >= 2 {
				continue
			}
			identity, identityOK, _ := s.store.CurrentCanonicalInstrument(ctx, child.Venue, child.Ticker)
			legs, bookOK := exactPair(child)
			if !identityOK || identity.IdentityStatus != "verified" || !bookOK {
				continue
			}
			inputs := map[string]any{"credit_venue": credit.Venue, "credit_instrument": credit.InstrumentID,
				"credit_ts": credit.CreditAt, "credited_amount": credit.CreditedAmount,
				"credit_source": credit.SourceArtifact, "credit_hash": credit.SourceHash,
				"parent_game": parent.GameID, "child_market_type": child.MktType,
				"post_credit_seconds": math.Max(0, now.Sub(credit.CreditAt).Seconds()),
				"selector_training":   true, "selector_cell": "child=" + child.MktType}
			in := concretePairInput{System: "collateral-release-rotation",
				Opportunity: "credit|" + credit.Venue + "|" + credit.InstrumentID + "|" + child.Ticker + "|" + credit.CreditAt.Format(time.RFC3339Nano),
				Cohort:      "authoritative-credit-linked-child", SourceArtifact: credit.SourceArtifact,
				CertificateHash: credit.SourceHash,
				Blocker:         "post-credit child-side pair; untouched rotation horizon lower bound pending",
				Identity:        identity, Observed: now, Legs: legs, Inputs: inputs}
			i, d, e := s.insertConcretePair(ctx, in)
			metrics["collateral_inserted"] += i
			metrics["collateral_duplicates"] += d
			if e != nil {
				metrics["collateral_insert_error"]++
			}

			targetLeague := gameLeague[child.GameID]
			targetPhase := concreteGamePhase(now, gameStart[child.GameID])
			targetLiquidity := concreteLiquidityBin(math.Min(legs[0].Depth, legs[1].Depth))
			comparables := make([]collateralComparable, 0, 16)
			for _, candidate := range marketRows {
				if candidate.Venue != child.Venue || candidate.GameID == child.GameID ||
					candidate.MktType != child.MktType || candidate.Src != "struct" ||
					creditedGames[candidate.GameID] || gameLeague[candidate.GameID] != targetLeague ||
					concreteGamePhase(now, gameStart[candidate.GameID]) != targetPhase ||
					((child.MktType == "spread" || child.MktType == "total") && math.Abs(candidate.Line-child.Line) > 1e-9) {
					continue
				}
				candidateLegs, candidateBookOK := exactPair(candidate)
				if !candidateBookOK {
					continue
				}
				liquidity := concreteLiquidityBin(math.Min(candidateLegs[0].Depth, candidateLegs[1].Depth))
				if liquidity != targetLiquidity {
					continue
				}
				comparables = append(comparables, collateralComparable{Market: candidate, League: targetLeague,
					Phase: targetPhase, Liquidity: liquidity})
			}
			matched, matchedOK := chooseCollateralNoReleaseControl(child, targetLeague, targetPhase,
				targetLiquidity, credit.SourceHash, comparables, creditedGames)
			if !matchedOK {
				metrics["collateral_no_release_match_unavailable"]++
			} else {
				matchedIdentity, matchedIdentityOK, _ := s.store.CurrentCanonicalInstrument(ctx, matched.Venue, matched.Ticker)
				matchedLegs, matchedBookOK := exactPair(matched)
				if !matchedIdentityOK || matchedIdentity.IdentityStatus != "verified" || !matchedBookOK {
					metrics["collateral_no_release_money_truth_unavailable"]++
				} else {
					controlInputs := map[string]any{"matched_release_opportunity": in.Opportunity,
						"release_game": child.GameID, "control_game": matched.GameID,
						"credit_hash": credit.SourceHash, "credit_time_cell_start": now.UTC().Truncate(5 * time.Minute),
						"no_release_in_recent_30m_cell": true, "market_type": child.MktType,
						"line": child.Line, "league": targetLeague, "phase": targetPhase,
						"liquidity_bin": targetLiquidity, "deterministic_match_hash_order": true,
						// A no-release market is the causal matched control, not an action trigger. It must
						// remain gradeable but can never train/emit a frozen order candidate of its own.
						"selector_training": false, "matched_control_only": true,
						"selector_cell": "no-release|child=" + child.MktType}
					certificate := storage.R138HashJSON(map[string]any{"release": child, "control": matched,
						"credit_hash": credit.SourceHash, "league": targetLeague, "phase": targetPhase,
						"liquidity": targetLiquidity})
					control := concretePairInput{System: "collateral-release-rotation",
						Opportunity: fmt.Sprintf("no-release|%s|%s|%s|%s", credit.SourceHash, child.Ticker,
							matched.Ticker, credit.CreditAt.UTC().Format(time.RFC3339Nano)),
						Cohort: "deterministic-structurally-matched-no-release", SourceArtifact: "current exact book + immutable market_game no-release time-cell match",
						CertificateHash: certificate,
						Blocker:         "deterministic structurally matched no-release control; no qualifying credit in the matched recent time cell",
						Identity:        matchedIdentity, Observed: now, Legs: matchedLegs, Inputs: controlInputs}
					ci, cd, ce := s.insertConcretePair(ctx, control)
					metrics["collateral_no_release_control_inserted"] += ci
					metrics["collateral_no_release_control_duplicates"] += cd
					if ce != nil {
						metrics["collateral_no_release_control_insert_error"]++
					}
				}
			}
			childN++
		}
	}
}

func (s *Server) sweepConcreteResearchSystems(ctx context.Context, now time.Time) {
	st := s.concreteSchedule()
	st.Lock()
	due := st.last.IsZero() || now.Sub(st.last) >= 5*time.Minute
	if due {
		st.last = now
	}
	st.Unlock()
	if !due {
		return
	}
	receipt := storage.CollectorReceipt{CollectorID: "concrete-paired-systems", CycleID: collectorCycleID(now),
		Status: "healthy", Started: now,
		Source:        "source-native triggers repriced from one current two-sided executable book; official credit receipts read separately",
		SchemaVersion: "concrete-paired-systems-r139-v1", ExpectedCadence: 5 * time.Minute,
		Systems: concreteSystemIDs, Exclusions: map[string]int{}, Metrics: map[string]any{}}
	metrics := map[string]int{}
	poly, _ := s.r138PolyUSLinkedBooks()
	s.collectConcreteSignalPairs(ctx, now, poly, metrics)
	s.collectConcreteRuleAndPersona(ctx, now, poly, metrics)
	s.collectResearchCreditEvents(ctx, metrics)
	s.collectConcreteClienteleSeriesAndCredit(ctx, now, poly, metrics)
	s.collectConcreteCarryProspective(ctx, now, poly, metrics)
	if frozen, err := s.store.FreezeEligibleResearchSelectors(ctx, now, 7, 20); err != nil {
		metrics["selector_freeze_error"]++
	} else {
		metrics["selectors_frozen"] += frozen
	}
	if emitted, err := s.store.EmitFrozenSelectorCandidates(ctx, now, 100); err != nil {
		metrics["selector_emit_error"]++
	} else {
		metrics["selector_candidates_emitted"] += emitted
	}
	finalizeConcreteResearchReceipt(&receipt, metrics, ctx.Err())
	// A suite shutdown cancels the shared work context. Do not append that cooperative stop as the
	// newest runtime failure: no collector contract failed, and the prior completed receipt remains
	// the truthful last cycle. Real per-cycle deadlines are still persisted as errors below.
	if errors.Is(ctx.Err(), context.Canceled) {
		return
	}
	receipt.Metrics["economic_n_definition"] = "terminal exact-money payoff updates only; input/control counts are not n"
	receipt.Metrics["paper_authority"], receipt.Metrics["live_authority"] = false, false
	receipt.Completed = time.Now().UTC()
	durableCtx, cancelDurability := researchDurabilityContext(ctx)
	defer cancelDurability()
	_, _ = s.store.InsertCollectorReceipt(durableCtx, receipt)
}

// finalizeConcreteResearchReceipt keeps a timed-out or partially failed collection pass from
// masquerading as a healthy empty cycle. A zero row count is healthy only when every source and
// storage operation completed without an explicit error and the caller's work context survived.
func finalizeConcreteResearchReceipt(receipt *storage.CollectorReceipt, metrics map[string]int, ctxErr error) {
	if receipt.Metrics == nil {
		receipt.Metrics = map[string]any{}
	}
	if receipt.Exclusions == nil {
		receipt.Exclusions = map[string]int{}
	}
	receipt.Status = "healthy"
	receipt.ExpectedZero = false
	receipt.ZeroReason, receipt.ErrorClass, receipt.ErrorText = "", "", ""
	inserted := 0
	errorMetrics := make([]string, 0)
	for key, value := range metrics {
		receipt.Metrics[key] = value
		if strings.HasSuffix(key, "_inserted") && !strings.Contains(key, "credit_") {
			inserted += value
		}
		if strings.Contains(strings.ToLower(key), "error") {
			receipt.Exclusions[key] = value
			if value > 0 {
				errorMetrics = append(errorMetrics, fmt.Sprintf("%s=%d", key, value))
			}
		}
	}
	receipt.Inserted, receipt.Attempted, receipt.Eligible = inserted, inserted, inserted
	switch {
	case ctxErr != nil:
		receipt.Status = "error"
		switch {
		case errors.Is(ctxErr, context.DeadlineExceeded):
			receipt.ErrorClass = "timeout"
		case errors.Is(ctxErr, context.Canceled):
			receipt.ErrorClass = "canceled"
		default:
			receipt.ErrorClass = collectorErrorClass(ctxErr)
		}
		receipt.ErrorText = ctxErr.Error()
	case len(errorMetrics) > 0:
		sort.Strings(errorMetrics)
		receipt.Status, receipt.ErrorClass = "error", "collector_metrics"
		receipt.ErrorText = "collector operations reported errors: " + strings.Join(errorMetrics, ", ")
	case inserted == 0:
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no source trigger had verified canonical identity plus one current complete two-sided book; exclusions are explicit"
	}
}
