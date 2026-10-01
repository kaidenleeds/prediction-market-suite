package server

// Typed momentum SELL evidence. A SELL is graded as the economically equivalent purchase of the
// opposite binary side at synthetic cost (1 - actual proceeds), while preserving the owned side,
// required signed position, action-aware fee, and actual sale fills in immutable inputs. This lets
// the common inference engine grade the opportunity without ever relabeling the executable route
// as a BUY. Paper/LIVE dispatch is handled by the separate reduce-only contract.

import (
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type properMomentumSellLeg struct {
	OwnedSide, SyntheticSide, FeeSource string
	Quantity, Proceeds, Fee             float64
	Fills                               []properbetting.Fill
}

func numericMapValue(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, !math.IsNaN(n) && !math.IsInf(n, 0)
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

func properMomentumSingleSell(sim properScoreSimResult) (properMomentumSellLeg, bool) {
	if sim.status != "filled" || len(sim.legs) != 1 || math.Abs(math.Abs(sim.requested)-1) > 1e-8 ||
		math.Abs(sim.cancelled) > 1e-8 || math.Abs(math.Abs(sim.prior)-1) > 1e-8 ||
		math.Abs(sim.target) > 1e-8 || math.Abs(sim.post) > 1e-8 {
		return properMomentumSellLeg{}, false
	}
	leg := sim.legs[0]
	action, _ := leg["action"].(string)
	owned, _ := leg["side"].(string)
	status, _ := leg["status"].(string)
	feeSource, _ := leg["fee_source"].(string)
	requested, rok := numericMapValue(leg["requested"])
	filled, fok := numericMapValue(leg["filled"])
	cancelled, cok := numericMapValue(leg["cancelled"])
	proceeds, pok := numericMapValue(leg["integrated_proceeds"])
	fee, feeOK := numericMapValue(leg["fee"])
	owned = strings.ToUpper(strings.TrimSpace(owned))
	if action != "SELL" || status != "filled" || !rok || !fok || !cok || !pok || !feeOK ||
		math.Abs(requested-1) > 1e-8 || math.Abs(filled-1) > 1e-8 || math.Abs(cancelled) > 1e-8 ||
		(owned != "YES" && owned != "NO") || proceeds <= 0 || proceeds >= 1 || fee < 0 || feeSource == "" {
		return properMomentumSellLeg{}, false
	}
	synthetic := "NO"
	if owned == "NO" {
		synthetic = "YES"
	}
	fills := []properbetting.Fill{}
	if raw, ok := leg["fills"].([]properbetting.Fill); ok {
		fills = append(fills, raw...)
	}
	return properMomentumSellLeg{OwnedSide: owned, SyntheticSide: synthetic,
		FeeSource: feeSource, Quantity: 1, Proceeds: proceeds, Fee: fee, Fills: fills}, true
}

func properMomentumSellCohort(base string) string {
	return base + "|action=sell-reduce-only|promotion=kalshi-fok-v1"
}

func (s *Server) properScoreMomentumSellObservation(ctx context.Context, now time.Time, slot,
	transform string, row properScorePrepared, sim properScoreSimResult, baseCohort string) (int64, error) {
	leg, ok := properMomentumSingleSell(sim)
	if !ok || row.platform != "kalshi" || sim.expected <= 0 {
		return 0, errors.New("proper-score momentum SELL is not a positive exact one-share Kalshi route")
	}
	identity, identityOK, err := s.store.CurrentCanonicalInstrument(ctx, row.platform, row.forecast.Ticker)
	if err != nil || !identityOK || !strings.EqualFold(identity.IdentityStatus, "verified") {
		return 0, errors.New("proper-score momentum SELL canonical identity is not verified")
	}
	levels := row.book.yesBids
	if leg.OwnedSide == "NO" {
		levels = row.book.noBids
	}
	capacity := 0.0
	for _, level := range levels {
		capacity += level.Quantity
	}
	if capacity < 1 || row.book.tick <= 0 || strings.TrimSpace(row.book.sourceClock) == "" {
		return 0, errors.New("proper-score momentum SELL book capacity/tick/clock is incomplete")
	}
	syntheticCost := 1 - leg.Proceeds
	cohort := properMomentumSellCohort(baseCohort)
	opportunityID := storage.ResearchRouteStableID("proper-score-momentum-sell", slot, transform,
		row.platform, row.forecast.Ticker, leg.OwnedSide, row.book.sourceClock)
	id, _, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: now, SystemID: "proper-score-executor", ExperimentVersion: 1,
		OpportunityID: opportunityID, Kind: "candidate", Cohort: cohort,
		CanonicalEventID: identity.EventID, EventVersion: identity.EventVersion,
		CanonicalPayoffID: identity.PayoffID, PayoffVersion: identity.PayoffVersion,
		Venue: "kalshi", Ticker: row.forecast.Ticker, Route: "taker", Side: leg.SyntheticSide,
		CertificateStatus: "verified", SourceClockID: row.book.sourceClock,
		SourceArtifact: "proper-score momentum one-share SELL simulation; synthetic opposite-side grade",
		BookSource:     row.book.source, FeeSource: leg.FeeSource,
		QuoteAgeMax: row.book.age, TickMin: row.book.tick, Size: 1, Cost: syntheticCost, Fee: leg.Fee,
		PayoutLower: 0, PayoutUpper: 1, NetLower: -syntheticCost - leg.Fee,
		NetUpper: 1 - syntheticCost - leg.Fee, VisibleCapacity: capacity,
		CapitalSeconds: math.Max(0, row.forecast.ResolveHours*3600), DecisionLatencyMS: row.latencyMS,
		Candidate: true, LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true,
		DepthKnown: true, FeeKnown: true,
		CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: syntheticCost, Fee: leg.Fee,
			PayoutFloor: 0, NetFloor: -syntheticCost - leg.Fee}},
		Inputs: map[string]any{"promotion_action": "SELL", "owned_side": leg.OwnedSide,
			"synthetic_grade_side": leg.SyntheticSide, "required_prior_position": sim.prior,
			"target_position": sim.target, "paper_position_must_match_exactly": true,
			"live_position_must_match_exactly": true, "reduce_only": true,
			"fill_or_kill": true, "cross_route": false, "actual_sale_proceeds": leg.Proceeds,
			"actual_sell_fee": leg.Fee, "actual_sell_fills": leg.Fills,
			"synthetic_cost_identity": "1-actual_sale_proceeds",
			"one_share_expected_net":  sim.expected, "promotion_expected_net_lower": sim.expected,
			"polyus_sell_promotion": "blocked_no_verified_fill_or_kill_receipt",
			"cross_promotion":       "blocked_non_atomic_multi_action_route",
			"paper_authority":       false, "live_authority": false},
	})
	return id, err
}
