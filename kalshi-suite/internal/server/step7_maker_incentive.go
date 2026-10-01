package server

// Natural maker lifecycle and incentive-lock evaluation. Both are observation-only: maker rows
// originate in the normal honest-fill harness, and incentive rows join official programs to
// current immutable route bundles. Missing attribution is a blocker with a zero reward floor.

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func (s *Server) sweepStep7MakerSalvage(ctx context.Context, now time.Time) {
	if !step7RuntimeFor(s).makerDue(now) {
		return
	}
	started := time.Now()
	receipt := storage.CollectorReceipt{CollectorID: "step7-maker-salvage", CycleID: collectorCycleID(started),
		ExperimentID: "maker-salvage-matched-cohort", ExperimentVersion: 1, Status: "healthy", Started: started,
		Source:        storage.R139Step7MakerSalvageSource,
		SchemaVersion: "step7-maker-salvage-v1", ExpectedCadence: time.Minute, Exclusions: map[string]int{},
		Systems: []string{"maker-salvage-matched-cohort"}, Metrics: map[string]any{"research_orders_created": 0}}
	defer func() {
		receipt.Completed = time.Now()
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
	}()
	rows, err := s.store.NaturalMakerSalvageSnapshots(ctx, 250)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	receipt.Attempted, receipt.Eligible = len(rows), len(rows)
	if len(rows) == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no natural maker attempts exist"
		return
	}
	matched, canceled, filled, settled := 0, 0, 0, 0
	for _, row := range rows {
		n, mirrorErr := s.store.MirrorNaturalMakerSalvage(ctx, row)
		if mirrorErr != nil {
			receipt.Exclusions["lifecycle_storage_error"]++
			continue
		}
		if n == 0 {
			receipt.Duplicates++
		} else {
			receipt.Inserted += n
		}
		isMatched := row.DecisionID != "" && row.MakerNetLower != nil && *row.MakerNetLower > 0 &&
			row.TakerCost != nil && row.TakerFee != nil && row.TakerNetLower != nil &&
			*row.TakerNetLower <= 0 && row.TakerRejection != "" && row.Family != "" && row.Tick != nil &&
			row.QuoteAge != nil && row.DecisionLatency != nil && row.VisibleCapacity != nil && *row.VisibleCapacity >= 1 &&
			row.BookSource != "" && row.SourceClockID != "" && row.CertificateHash != "" && row.QueueAhead != nil
		if isMatched {
			matched++
		} else {
			receipt.Exclusions["frozen_same_model_taker_rejection_missing"]++
		}
		var makerLowerAtDecision any
		if row.MakerNetLower != nil {
			makerLowerAtDecision = *row.MakerNetLower
		}
		if row.Filled == 0 {
			canceled++
		}
		if row.Filled == 1 {
			filled++
		}
		if row.Settlement != nil {
			settled++
		}
		status := "open"
		if row.Filled == 0 {
			status = "settled"
		} else if row.Filled == 1 && row.Settlement != nil && row.Fee != nil && row.Rebate != nil && row.FeeSource != "" {
			status = "settled"
		} else if row.Filled == 1 && row.Settlement != nil {
			status = "censored"
		}
		blocker := "frozen_same_model_taker_rejection_missing"
		if isMatched {
			blocker = "economic lifecycle lives in the exact maker ledger; executable promotion still requires untouched replication and a current book"
		}
		_, inserted, obsErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: row.Opened, SystemID: "maker-salvage-matched-cohort", OpportunityID: fmt.Sprintf("natural-maker:%d", row.AttemptID),
			Kind: "control", Cohort: map[bool]string{true: "matched-natural-maker", false: "unmatched-natural-maker"}[isMatched],
			Venue: strings.ToLower(row.Platform), Route: "observer", CertificateStatus: "not_applicable",
			SourceClockID: "maker-fill-stats-natural-lifecycle", SourceArtifact: "normal paper maker attempt; no research order",
			PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: status, Blocker: blocker,
			Inputs: map[string]any{"attempt_id": row.AttemptID, "ticker": row.Ticker, "side": row.Side,
				"filled_state": row.Filled, "cancel_realized_net": 0, "matched_taker_decision": isMatched,
				"maker_net_lower_at_decision": makerLowerAtDecision,
				"fee_receipt_known":           row.Fee != nil && row.Rebate != nil && row.FeeSource != "", "settlement_known": row.Settlement != nil}})
		if obsErr != nil {
			receipt.Exclusions["observation_storage_error"]++
		} else if inserted {
			receipt.Inserted++
		}
		if isMatched && row.Filled == 1 && row.Settlement != nil && row.Fee != nil && row.Rebate != nil &&
			row.FeeSource != "" && !row.SettlementAt.IsZero() && row.QueueLeft != nil && row.FillUnits != nil &&
			*row.FillUnits > 0 && row.Markout != nil && !row.MarkoutAt.IsZero() {
			settleSide := *row.Settlement
			if strings.EqualFold(row.Side, "NO") {
				settleSide = 1 - settleSide
			}
			feeNet := *row.Fee - *row.Rebate
			realized := settleSide - row.PostPrice - feeNet
			capitalSeconds := row.SettlementAt.Sub(row.Opened).Seconds()
			if capitalSeconds < 0 {
				capitalSeconds = 0
			}
			_, candidateInserted, candidateErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
				Observed: row.Opened, SystemID: "maker-salvage-matched-cohort",
				OpportunityID: fmt.Sprintf("natural-maker:%d:terminal", row.AttemptID), Kind: "candidate",
				Cohort: "matched-natural-maker-terminal", Venue: strings.ToLower(row.Platform), Ticker: row.Ticker,
				Route: "maker", Side: strings.ToUpper(row.Side), CertificateStatus: "verified",
				CertificateHash: row.CertificateHash, SourceClockID: row.SourceClockID,
				SourceArtifact: "frozen same-model taker rejection + natural queue/fill + exact fee/rebate + authoritative settlement",
				BookSource:     row.BookSource, FeeSource: row.FeeSource, QuoteAgeMax: *row.QuoteAge, TickMin: *row.Tick,
				Size: 1, Cost: row.PostPrice, Fee: feeNet, PayoutLower: settleSide, PayoutUpper: settleSide,
				NetLower: realized, NetUpper: realized, VisibleCapacity: *row.VisibleCapacity, CapitalSeconds: capitalSeconds,
				DecisionLatencyMS: *row.DecisionLatency, LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true,
				DepthKnown: true, FeeKnown: true, Candidate: true, OutcomeStatus: "settled",
				CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: row.PostPrice, Fee: feeNet, PayoutFloor: settleSide, NetFloor: realized}},
				Inputs: map[string]any{"attempt_id": row.AttemptID, "decision_id": row.DecisionID,
					"maker_net_lower_at_decision": *row.MakerNetLower,
					"taker_cost":                  *row.TakerCost, "taker_fee": *row.TakerFee, "taker_net_lower": *row.TakerNetLower,
					"taker_rejection": row.TakerRejection, "queue_ahead": *row.QueueAhead, "queue_left": *row.QueueLeft,
					"fill_units": *row.FillUnits, "actual_natural_fill": true, "one_share_economics": true}})
			if candidateErr != nil {
				receipt.Exclusions["terminal_candidate_storage_or_identity_error"]++
			} else if candidateInserted {
				receipt.Inserted++
			}
		}
	}
	receipt.Metrics["matched_trials"], receipt.Metrics["unmatched_controls"] = matched, len(rows)-matched
	receipt.Metrics["canceled_exact_zero"], receipt.Metrics["natural_fills"], receipt.Metrics["settled"] = canceled, filled, settled
}

// stampMakerSalvageTruth attaches the maker-salvage overlay to an order that the shared executor
// has already admitted as an exact maker route. The current maker lower bound must remain positive
// while the independently refreshed taker route is non-positive. This records no extra order and
// therefore cannot duplicate the source System's position.
func (s *Server) stampMakerSalvageTruth(ctx context.Context, attemptID int64,
	candidate storage.ResearchPromotionCandidate, decisionID, certificateHash, platform, ticker, side string,
	makerCost, makerFee float64) {
	if attemptID <= 0 || candidate.Route != "maker" || candidate.SystemID == "maker-salvage-matched-cohort" ||
		decisionID == "" || certificateHash == "" || candidate.ExpectedNetLower <= 0 ||
		candidate.Venue != strings.ToLower(platform) || candidate.Ticker != ticker ||
		!strings.EqualFold(candidate.Side, side) || makerCost <= 0 || makerCost >= 1 {
		return
	}
	decisionAt := time.Time{}
	ask, depth, bookSource := 0.0, 0.0, ""
	tick, quoteAge, latency := 0.0, 0.0, 0.0
	clockID := ""
	if strings.EqualFold(platform, "polyus") {
		if s.polyUSWS == nil {
			return
		}
		bids, asks, sourceAt, receivedAt, bookOK := s.polyUSWS.FullBookLevelsAt(ticker)
		if !bookOK {
			return
		}
		decisionAt = time.Now().UTC()
		var quoteOK bool
		ask, depth, quoteOK = makerSalvagePolyUSSideQuote(bids, asks, side)
		if !quoteOK {
			return
		}
		quoteAge, latency, clockID, quoteOK = makerSalvagePolyUSClocks(decisionAt, sourceAt, receivedAt)
		if !quoteOK {
			return
		}
		bookSource = "polyus-ws-full"
	} else {
		var quoteOK bool
		ask, depth, bookSource, quoteOK = s.executableAsk(ctx, platform, ticker, side, true)
		if !quoteOK || depth < 1 || ask <= 0 || ask >= 1 {
			return
		}
		decisionAt = time.Now().UTC()
	}
	takerFee, _, feeKnown := s.researchPaperRouteFee(platform, ticker, "taker", 1, ask)
	if !feeKnown || takerFee < 0 {
		return
	}
	makerLow, takerLow, economicMatch := step7MakerSalvageRouteBounds(candidate,
		makerCost, makerFee, ask, takerFee)
	if !economicMatch {
		return
	} // same frozen executable lower bound still permits taker; this is not maker salvage
	switch strings.ToLower(platform) {
	case "kalshi":
		m, mok := s.kmkt(ticker)
		if !mok {
			return
		}
		var tickOK bool
		tick, tickOK = m.TickForKnown(ask)
		if !tickOK {
			return
		}
		prov, provOK := s.kal.LiveBookProvenance(ticker, 3*time.Second)
		if !provOK || prov.Generation == 0 || prov.SubscriptionID <= 0 || prov.Sequence <= 0 {
			return
		}
		quoteAt := prov.SourceAt
		if quoteAt.IsZero() {
			quoteAt = prov.ReceivedAt
		}
		age := decisionAt.Sub(quoteAt)
		if age < 0 || age > 3*time.Second {
			return
		}
		quoteAge = age.Seconds()
		lat := decisionAt.Sub(prov.ReceivedAt)
		if lat < 0 {
			lat = 0
		}
		latency = lat.Seconds() * 1000
		clockID = fmt.Sprintf("kalshi-book:g%d:sid%d:seq%d", prov.Generation, prov.SubscriptionID, prov.Sequence)
	case "polyus":
		_, candidateTick, authority := s.polyUSFreshFeeAuthority(ticker)
		if !authority || candidateTick <= 0 {
			return
		}
		tick = candidateTick
	default:
		return
	}
	_ = s.store.SetMakerSalvageDecisionTruth(ctx, attemptID, storage.MakerSalvageDecisionTruth{
		DecisionID: decisionID + "|maker-vs-taker", Rejection: "frozen_executable_lower_bound_rejects_current_taker",
		BookSource: bookSource, SourceClockID: clockID, CertificateHash: certificateHash,
		MakerNetLower: makerLow, TakerCost: ask, TakerFee: takerFee, TakerNetLower: takerLow, Tick: tick, QuoteAge: quoteAge,
		DecisionLatencyMS: latency, VisibleCapacity: depth})
}

// makerSalvagePolyUSSideQuote derives the bought outcome's taker ask and capacity from one full
// MARKET_DATA frame. A NO buy consumes the best YES bid; it is never algebraically paired with
// depth from a different LITE or REST snapshot.
func makerSalvagePolyUSSideQuote(bids, asks []polymarketus.BookLevel, side string) (price, depth float64, ok bool) {
	side = strings.ToUpper(strings.TrimSpace(side))
	if side == "YES" {
		if len(asks) == 0 || asks[0].Price <= 0 || asks[0].Price >= 1 || asks[0].Quantity < 1 {
			return 0, 0, false
		}
		return asks[0].Price, asks[0].Quantity, true
	}
	if side == "NO" {
		if len(bids) == 0 || bids[0].Price <= 0 || bids[0].Price >= 1 || bids[0].Quantity < 1 {
			return 0, 0, false
		}
		return 1 - bids[0].Price, bids[0].Quantity, true
	}
	return 0, 0, false
}

// makerSalvagePolyUSClocks records the venue source clock and local receipt clock from the exact
// full-book snapshot used above. Quiet, still-authoritative books keep their truthful age; socket
// generation/lifecycle vitality is already enforced by FullBookLevelsAt.
func makerSalvagePolyUSClocks(decisionAt, sourceAt, receivedAt time.Time) (
	quoteAgeSeconds, decisionLatencyMS float64, clockID string, ok bool) {
	decisionAt, sourceAt, receivedAt = decisionAt.UTC(), sourceAt.UTC(), receivedAt.UTC()
	if decisionAt.IsZero() || receivedAt.IsZero() {
		return 0, 0, "", false
	}
	quoteAt := sourceAt
	if quoteAt.IsZero() {
		quoteAt = receivedAt
	}
	quoteAge, decisionLag := decisionAt.Sub(quoteAt), decisionAt.Sub(receivedAt)
	if quoteAge < 0 || decisionLag < 0 {
		return 0, 0, "", false
	}
	sourceClock := "missing"
	if !sourceAt.IsZero() {
		sourceClock = sourceAt.Format(time.RFC3339Nano)
	}
	return quoteAge.Seconds(), decisionLag.Seconds() * 1000,
		"polyus-full-book:source=" + sourceClock + ":received=" + receivedAt.Format(time.RFC3339Nano), true
}

func step7MakerSalvageRouteBounds(candidate storage.ResearchPromotionCandidate,
	makerCost, makerFee, takerCost, takerFee float64) (makerLower, takerLower float64, matched bool) {
	if candidate.ObservedPrice <= 0 || candidate.ObservedPrice >= 1 || candidate.ExpectedNetLower <= 0 ||
		makerCost <= 0 || makerCost >= 1 || takerCost <= 0 || takerCost >= 1 || takerFee < 0 ||
		math.IsNaN(candidate.ObservedFeePC) || math.IsInf(candidate.ObservedFeePC, 0) ||
		math.IsNaN(makerFee) || math.IsInf(makerFee, 0) {
		return 0, 0, false
	}
	baseAllIn := candidate.ObservedPrice + candidate.ObservedFeePC
	makerLower = candidate.ExpectedNetLower - (makerCost + makerFee - baseAllIn)
	takerLower = makerLower - (takerCost + takerFee - makerCost - makerFee)
	if math.IsNaN(makerLower) || math.IsInf(makerLower, 0) ||
		math.IsNaN(takerLower) || math.IsInf(takerLower, 0) {
		return 0, 0, false
	}
	return makerLower, takerLower, makerLower > 0 && takerLower <= 0
}

func (s *Server) stampPromotedMakerSalvageTruth(ctx context.Context, attemptID int64,
	intent storage.ResearchPromotionIntent, platform, ticker, side string, makerCost, makerFee float64) {
	if intent.Proof.Route != "maker" || intent.Proof.ResultHash == "" {
		return
	}
	s.stampMakerSalvageTruth(ctx, attemptID, intent.Candidate, intent.IntentID,
		intent.Proof.ResultHash, platform, ticker, side, makerCost, makerFee)
}

func (s *Server) stampExplorationMakerSalvageTruth(ctx context.Context, attemptID int64,
	candidate storage.ResearchPromotionCandidate, platform, ticker, side string, makerCost, makerFee float64) {
	s.stampMakerSalvageTruth(ctx, attemptID, candidate,
		fmt.Sprintf("paper-exploration-%d", candidate.ObservationID), candidate.CertificateHash,
		platform, ticker, side, makerCost, makerFee)
}

// stampNativeMakerSalvageTruth connects the overlay to established native System maker routes.
// The source order already exists when this runs. A separate settled exact maker cell supplies the
// current positive lower estimate, while stampMakerSalvageTruth independently refreshes the taker
// ask/depth/fee and stamps only when that alternative is non-positive. No second order is created.
func (s *Server) stampNativeMakerSalvageTruth(ctx context.Context, attemptID int64,
	source, platform, ticker, side string, makerCost, makerFeeTotal, contracts float64) {
	makerFeePC, feeOK := makerSalvagePerContractFee(makerFeeTotal, contracts)
	if !feeOK {
		return
	}
	point, ok := s.nativeMakerSystemRouteCurrentPoint(source, platform, side,
		makerCost, makerFeeTotal, contracts)
	if !ok {
		return
	}
	identity, found, err := s.store.CurrentCanonicalInstrument(ctx, platform, ticker)
	if err != nil || !found || identity.IdentityStatus != "verified" {
		return
	}
	family := strings.ToLower(strings.TrimSpace(policySourceFamily(source)))
	if family == "" || family == "maker-salvage-matched-cohort" {
		return
	}
	certificate := storage.R138HashJSON(map[string]any{
		"overlay": "maker-salvage-matched-cohort-v1", "source_system": family,
		"venue": strings.ToLower(platform), "ticker": ticker, "side": strings.ToUpper(side),
		"event_id": identity.EventID, "event_version": identity.EventVersion,
		"payoff_id": identity.PayoffID, "payoff_version": identity.PayoffVersion,
	})
	candidate := storage.ResearchPromotionCandidate{SystemID: family, StrategyFamily: family,
		Venue: strings.ToLower(platform), Ticker: ticker, Side: strings.ToUpper(side), Route: "maker",
		ObservedPrice: makerCost, ObservedFeePC: makerFeePC, ExpectedNetLower: point,
		CertificateHash: certificate}
	s.stampMakerSalvageTruth(ctx, attemptID, candidate,
		fmt.Sprintf("native-maker-route-%d-%s", attemptID, family), certificate,
		platform, ticker, side, makerCost, makerFeePC)
}

func makerSalvagePerContractFee(total, contracts float64) (float64, bool) {
	if contracts <= 0 || math.IsNaN(total) || math.IsInf(total, 0) ||
		math.IsNaN(contracts) || math.IsInf(contracts, 0) {
		return 0, false
	}
	return total / contracts, true
}

func step7ConservativeIncentiveLower(bundle storage.CurrentIncentiveBundle, rewardLower, capitalReserve float64) float64 {
	return math.Min(bundle.NetFloor, math.Min(bundle.PartialFillWorst, bundle.UnwindWorst)) + rewardLower - capitalReserve
}

func step7IncentiveBlocker(bundle *storage.CurrentIncentiveBundle, eligibility, competition, queue, fill, credit bool, total float64) string {
	switch {
	case bundle == nil:
		return "no_current_certified_typed_bundle"
	case !eligibility:
		return "program_order_eligibility_not_exactly_proven"
	case !competition:
		return "current_competition_not_observed"
	case !queue:
		return "bundle_attributed_natural_queue_not_observed"
	case !fill:
		return "bundle_attributed_natural_fill_not_observed"
	case !credit:
		return "exact_order_attributed_credited_reward_unavailable; reward_floor_zero"
	case total <= 0:
		return "conservative_total_lower_not_positive"
	default:
		return ""
	}
}

func (s *Server) insertStep7IncentiveEvaluation(ctx context.Context, now time.Time, venue, programID, ticker string,
	competition float64, competitionKnown bool, advertised any, creditedMarketTotal float64, creditedRows int) (bool, string, error) {
	bundles, err := s.store.CurrentCertifiedIncentiveBundles(ctx, venue, ticker, now.Add(-5*time.Minute), 4)
	if err != nil {
		return false, "bundle_lookup_failed", err
	}
	var bundle *storage.CurrentIncentiveBundle
	if len(bundles) > 0 {
		bundle = &bundles[0]
	}
	// Active-program membership proves only the program/ticker join. It does not prove that an
	// arbitrary route bundle, size, account, or order qualified under the program's exact terms.
	eligibilityKnown, queueKnown, fillKnown, creditKnown := false, false, false, false
	rewardLower, actualCredit := 0.0, 0.0
	base, partial, unwind, total := 0.0, 0.0, 0.0, 0.0
	bundleID, certificate := "", ""
	if bundle != nil {
		bundleID, certificate = bundle.BundleID, bundle.CertificateHash
		base = bundle.NetFloor
		partial = bundle.PartialFillWorst
		unwind = bundle.UnwindWorst
		total = step7ConservativeIncentiveLower(*bundle, rewardLower, 0)
	}
	blocker := step7IncentiveBlocker(bundle, eligibilityKnown, competitionKnown, queueKnown, fillKnown, creditKnown, total)
	evalID := step7Hash("incentive-lock", venue, programID, ticker, bundleID, now.UTC().Truncate(15*time.Minute).Format(time.RFC3339Nano))
	inserted, err := s.store.InsertStep7IncentiveEvaluation(ctx, storage.Step7IncentiveEvaluation{
		EvaluationID: evalID, Observed: now, Venue: venue, ProgramID: programID, Ticker: ticker, BundleID: bundleID,
		CertificateHash: certificate, BundleCurrent: bundle != nil, EligibilityKnown: eligibilityKnown,
		CompetitionKnown: competitionKnown, QueueKnown: queueKnown, FillKnown: fillKnown, RewardCreditKnown: creditKnown,
		CompetitionUnits: math.Max(0, competition), ActualCredit: actualCredit, RewardLower: rewardLower,
		BaseNetFloor: base, PartialFillWorst: partial, UnwindWorst: unwind, ConservativeLower: total,
		Candidate: false, Blocker: blocker, Evidence: map[string]any{"advertised_program": advertised,
			"advertised_reward_used_as_pnl": false, "market_level_credited_total": creditedMarketTotal,
			"market_level_credited_rows": creditedRows, "market_credit_attributed_to_bundle": false,
			"reward_lower_bound": 0, "natural_order_submitted": false, "bundle_lookback_seconds": 300}})
	return inserted, blocker, err
}

func (s *Server) recordStep7KalshiIncentive(ctx context.Context, now time.Time, p kalshi.IncentiveProgram, b storage.LifecycleBook) (bool, string, error) {
	competition := math.Max(0, b.BidDepth) + math.Max(0, b.AskDepth)
	return s.insertStep7IncentiveEvaluation(ctx, now, "kalshi", p.ID, p.MarketTicker, competition, true, p, 0, 0)
}

func (s *Server) recordStep7PolyUSIncentive(ctx context.Context, now time.Time, p r138PolyUSIncentivePeriod,
	b r138LinkedBook, earnings []polymarketus.IncentiveEarning) (bool, string, error) {
	creditTotal, creditRows := 0.0, 0
	for _, e := range earnings {
		if strings.EqualFold(strings.TrimSpace(e.MarketSlug), strings.TrimSpace(p.slug)) {
			creditTotal += e.Reward
			creditRows++
		}
	}
	competition := math.Max(0, b.BidDepth) + math.Max(0, b.AskDepth)
	return s.insertStep7IncentiveEvaluation(ctx, now, "polyus", p.row.ProgramID, p.slug, competition, true, p.row, creditTotal, creditRows)
}
