package server

// Resolution-driven grading for the three native R139 route controls. A grade answers only:
// "what would the frozen all-leg executable quote have paid if every quoted leg filled?" It does
// not claim that a non-atomic basket filled, and it cannot grant Paper or LIVE authority.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/payoffsolver"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type nativeSettlementLeg struct {
	Venue, Ticker, Side string
	Quantity            float64
}

type nativeAggregateEvidence struct {
	Aggregate struct {
		LeftTicker  string              `json:"left_ticker"`
		RightTicker string              `json:"right_ticker"`
		AllLegCost  float64             `json:"all_leg_cost"`
		ExactFee    float64             `json:"exact_fee"`
		Legs        []storage.BasketLeg `json:"legs"`
	} `json:"aggregate_route"`
}

func nativePendingRouteShape(v storage.NativeRoutePendingGrade) ([]nativeSettlementLeg, float64, error) {
	venue := strings.ToLower(strings.TrimSpace(v.Venue))
	switch v.SystemID {
	case "time-nested-lock":
		var evidence nativeAggregateEvidence
		if json.Unmarshal([]byte(v.EvidenceJSON), &evidence) != nil || venue != "kalshi" ||
			evidence.Aggregate.LeftTicker == "" || evidence.Aggregate.RightTicker == "" ||
			evidence.Aggregate.AllLegCost <= 0 || evidence.Aggregate.ExactFee < 0 ||
			math.Abs(evidence.Aggregate.ExactFee-v.FeeAmount) > 1e-9 {
			return nil, 0, fmt.Errorf("invalid time-nested terminal evidence")
		}
		return []nativeSettlementLeg{{Venue: "kalshi", Ticker: evidence.Aggregate.LeftTicker, Side: "NO", Quantity: 1},
				{Venue: "kalshi", Ticker: evidence.Aggregate.RightTicker, Side: "YES", Quantity: 1}},
			evidence.Aggregate.AllLegCost, nil

	case "joint-marginal-lock":
		var evidence nativeAggregateEvidence
		if json.Unmarshal([]byte(v.EvidenceJSON), &evidence) != nil ||
			(venue != "kalshi" && venue != "polyus") || len(evidence.Aggregate.Legs) < 2 ||
			len(evidence.Aggregate.Legs) > parlayLegLimit || evidence.Aggregate.AllLegCost <= 0 ||
			evidence.Aggregate.ExactFee < 0 || math.Abs(evidence.Aggregate.ExactFee-v.FeeAmount) > 1e-9 {
			return nil, 0, fmt.Errorf("invalid joint-marginal terminal evidence")
		}
		legs := make([]nativeSettlementLeg, 0, len(evidence.Aggregate.Legs))
		for _, leg := range evidence.Aggregate.Legs {
			side := strings.ToUpper(strings.TrimSpace(leg.Side))
			if leg.Ticker == "" || (side != "YES" && side != "NO") {
				return nil, 0, fmt.Errorf("invalid joint-marginal terminal leg")
			}
			legs = append(legs, nativeSettlementLeg{Venue: venue, Ticker: leg.Ticker, Side: side, Quantity: 1})
		}
		return legs, evidence.Aggregate.AllLegCost, nil

	case "fee-rounding-batch":
		side := strings.ToUpper(strings.TrimSpace(v.Side))
		if venue != "kalshi" || v.Ticker == "" || (side != "YES" && side != "NO") ||
			v.Quantity < 2 || v.ExecutablePrice <= 0 || v.ExecutablePrice >= 1 || v.FeeAmount < 0 {
			return nil, 0, fmt.Errorf("invalid fee-rounding terminal evidence")
		}
		return []nativeSettlementLeg{{Venue: venue, Ticker: v.Ticker, Side: side, Quantity: v.Quantity}},
			v.Quantity * v.ExecutablePrice, nil
	default:
		return nil, 0, fmt.Errorf("unsupported native terminal system %q", v.SystemID)
	}
}

func nativeSidePayout(yesValue float64, side string) (float64, bool) {
	if yesValue < 0 || yesValue > 1 || math.IsNaN(yesValue) || math.IsInf(yesValue, 0) {
		return 0, false
	}
	switch strings.ToUpper(strings.TrimSpace(side)) {
	case "YES":
		return yesValue, true
	case "NO":
		return 1 - yesValue, true
	default:
		return 0, false
	}
}

func (s *Server) settleNativeRouteControls(ctx context.Context, now time.Time) (graded, invalid, waiting int, err error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rows, err := s.store.PendingNativeRouteGrades(ctx, 250)
	if err != nil {
		return 0, 0, 0, err
	}
	for _, row := range rows {
		legs, cost, shapeErr := nativePendingRouteShape(row)
		if shapeErr != nil {
			invalid++
			continue
		}
		payout, settledQty := 0.0, 0.0
		closedAt := row.Observed
		settlementEvidence := make([]map[string]any, 0, len(legs))
		complete := true
		for _, leg := range legs {
			receipt, ok, settleErr := s.store.VenueSettlementForTicker(ctx, leg.Venue, leg.Ticker)
			if settleErr != nil {
				return graded, invalid, waiting, settleErr
			}
			if !ok || receipt.ResolvedAt.Before(row.Observed) {
				complete = false
				break
			}
			legPayout, ok := nativeSidePayout(receipt.YesValue, leg.Side)
			if !ok {
				complete = false
				break
			}
			payout += leg.Quantity * legPayout
			settledQty += leg.Quantity
			if receipt.ResolvedAt.After(closedAt) {
				closedAt = receipt.ResolvedAt
			}
			settlementEvidence = append(settlementEvidence, map[string]any{
				"venue": leg.Venue, "ticker": leg.Ticker, "side": leg.Side, "quantity": leg.Quantity,
				"side_payout": legPayout, "yes_settle": receipt.YesValue,
				"resolved_at":     receipt.ResolvedAt.UTC().Format(time.RFC3339Nano),
				"source_artifact": receipt.SourceArtifact, "source_hash": receipt.Hash,
			})
		}
		if !complete {
			waiting++
			continue
		}
		realized := payout - cost - row.FeeAmount + row.RebateAmount
		evidence, _ := json.Marshal(map[string]any{
			"grade_kind":          "all-legs-frozen-quote-counterfactual",
			"actual_fill_claimed": false, "atomic_fill_claimed": false,
			"paper_authority": false, "live_authority": false,
			"entry_cost": cost, "entry_fee": row.FeeAmount, "entry_rebate": row.RebateAmount,
			"payout": payout, "settlement_receipts": settlementEvidence,
		})
		inserted, insertErr := s.store.AppendNativeRouteTerminalGrade(ctx, storage.NativeRouteTerminalGrade{
			OpportunityID: row.OpportunityID, RouteID: row.RouteID, Observed: closedAt,
			Quantity: settledQty, Payout: payout, RealizedNet: realized,
			CapitalSeconds: math.Max(0, closedAt.Sub(row.Observed).Seconds()),
			OutcomeStatus:  "counterfactual_settled", Reason: "authoritative venue settlement; all quoted legs assumed filled only for research grading",
			EvidenceJSON: string(evidence),
		})
		if insertErr != nil {
			return graded, invalid, waiting, insertErr
		}
		if inserted {
			graded++
		}
	}
	return graded, invalid, waiting, nil
}

// settleTypedResearchBundles grades the immutable ordered legs from each venue's own authoritative
// settlement receipt. It is explicitly counterfactual: neither an entry fill nor atomic execution
// is claimed, and no result is copied into Paper/LIVE ledgers.
func (s *Server) settleTypedResearchBundles(ctx context.Context, now time.Time) (graded, invalid, waiting int, err error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rows, err := s.store.PendingResearchRouteBundleGrades(ctx, 100)
	if err != nil {
		return 0, 0, 0, err
	}
	for _, row := range rows {
		if len(row.Legs) < 2 || len(row.Legs) > payoffsolver.MaxSupportedLegs {
			invalid++
			continue
		}
		payout, closedAt, complete := 0.0, row.Observed, true
		settlements := make([]map[string]any, 0, len(row.Legs))
		for _, leg := range row.Legs {
			receipt, ok, settleErr := s.store.VenueSettlementForTicker(ctx, leg.Venue, leg.Ticker)
			if settleErr != nil {
				return graded, invalid, waiting, settleErr
			}
			if !ok || receipt.ResolvedAt.Before(row.Observed) {
				complete = false
				break
			}
			perUnit, payoutOK := nativeSidePayout(receipt.YesValue, leg.Side)
			if !payoutOK {
				complete = false
				break
			}
			legPayout := leg.Quantity * perUnit
			payout += legPayout
			if receipt.ResolvedAt.After(closedAt) {
				closedAt = receipt.ResolvedAt
			}
			settlements = append(settlements, map[string]any{"leg_index": leg.Index,
				"leg_id": leg.LegID, "venue": leg.Venue, "ticker": leg.Ticker, "side": leg.Side,
				"quantity": leg.Quantity, "yes_settle": receipt.YesValue, "leg_payout": legPayout,
				"resolved_at":     receipt.ResolvedAt.UTC().Format(time.RFC3339Nano),
				"source_artifact": receipt.SourceArtifact, "source_hash": receipt.Hash})
		}
		if !complete {
			waiting++
			continue
		}
		realized := payout - row.Cost - row.Fee
		evidence, _ := json.Marshal(map[string]any{"grade_kind": "typed-all-leg-counterfactual",
			"actual_fill_claimed": false, "atomic_fill_claimed": false,
			"paper_authority": false, "live_authority": false, "ordered_settlements": settlements})
		inserted, insertErr := s.store.AppendResearchRouteBundleGrade(ctx, storage.ResearchRouteBundleGrade{
			BundleID: row.BundleID, Observed: closedAt, OutcomeStatus: "counterfactual_settled",
			Payout: payout, RealizedNet: realized, CapitalSeconds: math.Max(0, closedAt.Sub(row.Observed).Seconds()),
			Reason:       "authoritative per-leg venue settlements; frozen all-leg entry assumed only for research grading",
			EvidenceJSON: string(evidence)})
		if insertErr != nil {
			return graded, invalid, waiting, insertErr
		}
		if inserted {
			graded++
		}
	}
	return graded, invalid, waiting, nil
}

func (s *Server) sweepNativeRouteTerminalGrades(ctx context.Context, now time.Time) {
	receipt := storage.CollectorReceipt{CollectorID: "native-route-terminal-grades",
		CycleID: collectorCycleID(now), Status: "healthy", Started: now,
		Source:        "venue-scoped canonical signal settlements with exact settle_val joined to frozen native route controls",
		SchemaVersion: "native-route-terminal-grades-r139-v1", ExpectedCadence: 5 * time.Minute,
		Systems: append([]string(nil), nativeLockSystemIDs...), Exclusions: map[string]int{}, Metrics: map[string]any{
			"grade_kind": "all-legs-frozen-quote-counterfactual", "actual_fill_claimed": false,
			"atomic_fill_claimed": false, "paper_authority": false, "live_authority": false,
		}}
	graded, invalid, waiting, err := s.settleNativeRouteControls(ctx, now)
	bundleGraded, bundleInvalid, bundleWaiting, bundleErr := s.settleTypedResearchBundles(ctx, now)
	graded, invalid, waiting = graded+bundleGraded, invalid+bundleInvalid, waiting+bundleWaiting
	if err == nil {
		err = bundleErr
	}
	if promotion, promotionErr := s.store.ResearchBundlePromotionStatus(ctx); promotionErr == nil {
		receipt.Metrics["multi_leg_promotion_state"] = promotion.State
		receipt.Metrics["multi_leg_sealed_eligible"] = promotion.SealedEligible
		receipt.Metrics["current_atomic_rfq_quotes"] = promotion.CurrentAtomicRFQ
		receipt.Metrics["multi_leg_promotion_truth"] = promotion.Truth
	} else if err == nil {
		err = promotionErr
	}
	receipt.Eligible, receipt.Attempted, receipt.Inserted = graded+invalid+waiting, graded+invalid+waiting, graded
	if invalid > 0 {
		receipt.Exclusions["invalid_frozen_route_evidence"] = invalid
	}
	if waiting > 0 {
		receipt.Exclusions["settlement_precedes_quote_or_exact_receipt_unavailable"] = waiting
	}
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage_or_reconciliation", err.Error()
	} else if receipt.Eligible == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no ungraded native route has complete exact venue-scoped settlements"
	}
	receipt.Completed = time.Now().UTC()
	if _, receiptErr := s.store.InsertCollectorReceipt(context.WithoutCancel(ctx), receipt); receiptErr != nil && s.log != nil {
		s.log.Warn("native route terminal receipt failed", "err", receiptErr)
	}
}

func (s *Server) sweepResearchSystemTerminalGrades(ctx context.Context, now time.Time) {
	receipt := storage.CollectorReceipt{CollectorID: "research-system-terminal-grades",
		CycleID: collectorCycleID(now), Status: "healthy", Started: now,
		Source:        "complete frozen taker observations + venue-scoped canonical exact settle_val",
		SchemaVersion: "research-system-terminal-grades-r139-v1", ExpectedCadence: 5 * time.Minute,
		Systems: storage.ResearchExperimentIDs(), Exclusions: map[string]int{}, Metrics: map[string]any{
			"observer_controls_graded": false, "maker_no_fill_controls_graded": false,
			"actual_order_claimed": false, "paper_authority": false, "live_authority": false,
		}}
	rows, err := s.store.PendingResearchSystemTakerSettlements(ctx, 250)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
	} else {
		receipt.Eligible, receipt.Attempted = len(rows), len(rows)
		candidateGrades, controlGrades, blockedStructuralGrades := 0, 0, 0
		for _, row := range rows {
			settlement, ok, settleErr := s.store.VenueSettlementForTicker(ctx, row.Venue, row.Ticker)
			if settleErr != nil {
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "settlement_read", settleErr.Error()
				break
			}
			if !ok || settlement.ResolvedAt.Before(row.Observed) {
				receipt.Exclusions["exact_platform_settlement_unavailable_or_precedes_quote"]++
				continue
			}
			perUnit, payoutOK := nativeSidePayout(settlement.YesValue, row.Side)
			if !payoutOK {
				receipt.Exclusions["invalid_bought_side_or_settlement"]++
				continue
			}
			payout := row.Size * perUnit
			realized := payout - row.Cost - row.Fee
			inserted, updateErr := s.store.AppendResearchPayoffUpdate(ctx, storage.ResearchPayoffUpdate{
				ObservationID: row.ObservationID, Observed: settlement.ResolvedAt, Status: "settled",
				PayoutLower: payout, PayoutUpper: payout, RealizedNet: &realized,
				SourceArtifact: settlement.SourceArtifact, SourceHash: settlement.Hash,
				Reason: "authoritative venue settlement applied to frozen immediate-taker research quote; no actual order claimed",
			})
			if updateErr != nil {
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "payoff_update", updateErr.Error()
				break
			}
			if inserted {
				receipt.Inserted++
				switch row.ObservationKind {
				case "candidate":
					candidateGrades++
				case "control":
					controlGrades++
				case "negative":
					blockedStructuralGrades++
				}
			} else {
				receipt.Duplicates++
			}
		}
		receipt.Metrics["candidate_terminal_grades"] = candidateGrades
		receipt.Metrics["training_control_terminal_grades"] = controlGrades
		receipt.Metrics["blocked_structural_terminal_grades"] = blockedStructuralGrades
	}
	if receipt.Status == "healthy" && receipt.Eligible == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no ungraded complete taker observation has an exact post-quote venue settlement"
	}
	receipt.Completed = time.Now().UTC()
	if _, receiptErr := s.store.InsertCollectorReceipt(context.WithoutCancel(ctx), receipt); receiptErr != nil && s.log != nil {
		s.log.Warn("research system terminal receipt failed", "err", receiptErr)
	}
}
