package server

// Staged additive bundles retain their own immutable package decision and settlement ledgers.
// R166 records new candidates as not observed instead of turning one frozen snapshot into a
// simulated fill. Previously accepted packages continue to settle from authoritative outcomes.

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

const r148StagedPaperFreshFor = 5 * time.Second

// New verified xvlock opportunities are owned by the exact staged package ledger below. The old
// lockstack JSON rows remain immutable history and continue settling, but funding both paths would
// spend the Combo portfolio twice on the same two legs.
func r148LegacyLockstackFundingAllowed() bool { return false }

func r148StagedPaperSystem(system string) bool {
	switch strings.ToLower(strings.TrimSpace(system)) {
	case "event-basket-lock", "nested-ladder-lock", "time-nested-lock", "xvlock":
		return true
	default:
		return false
	}
}

func r148StagedPaperVenueShape(b storage.ResearchRouteBundle) (string, error) {
	seen := map[string]bool{}
	for _, leg := range b.Legs {
		venue := strings.ToLower(strings.TrimSpace(leg.Venue))
		if venue != "kalshi" && venue != "polyus" {
			return "", fmt.Errorf("leg %s uses research-only or unknown venue %s", leg.LegID, venue)
		}
		seen[venue] = true
	}
	if len(seen) == 1 && seen["kalshi"] {
		return "kalshi", nil
	}
	if len(seen) == 1 && seen["polyus"] {
		return "polyus", nil
	}
	if len(seen) == 2 {
		return "crossvenue", nil
	}
	return "", errors.New("bundle has no executable Paper venue shape")
}

func r148StagedPaperLevelFee(level storage.ResearchRouteBundleLevel, quantity float64) (float64, bool) {
	for _, quote := range level.FeeQuotes {
		if math.Abs(quote.Quantity-quantity) <= 1e-9 && quote.Total >= 0 &&
			!math.IsNaN(quote.Total) && !math.IsInf(quote.Total, 0) {
			return quote.Total, true
		}
	}
	return 0, false
}

func r148StagedPaperLegAllIn(leg storage.ResearchRouteBundleLeg) (cost, fee float64, err error) {
	if leg.Quantity <= 0 || leg.VisibleDepth+1e-9 < leg.Quantity || len(leg.Levels) == 0 ||
		strings.TrimSpace(leg.BookSource) == "" || strings.TrimSpace(leg.SourceClockID) == "" ||
		strings.TrimSpace(leg.FeeSource) == "" {
		return 0, 0, errors.New("incomplete frozen FOK book/depth/fee receipt")
	}
	remaining := leg.Quantity
	for _, level := range leg.Levels {
		if level.Price <= 0 || level.Price >= 1 || level.Quantity <= 0 ||
			math.IsNaN(level.Price) || math.IsInf(level.Price, 0) ||
			math.IsNaN(level.Quantity) || math.IsInf(level.Quantity, 0) {
			return 0, 0, errors.New("invalid frozen FOK book level")
		}
		take := math.Min(remaining, level.Quantity)
		if take <= 1e-12 {
			continue
		}
		levelFee, known := r148StagedPaperLevelFee(level, take)
		if !known {
			return 0, 0, fmt.Errorf("frozen level lacks exact fee at quantity %.9f", take)
		}
		cost += take * level.Price
		fee += levelFee
		remaining -= take
		if remaining <= 1e-12 {
			break
		}
	}
	if remaining > 1e-9 {
		return 0, 0, errors.New("frozen FOK book lacks full-package depth")
	}
	return cost, fee, nil
}

func r148StagedPaperLiveState(system string) string {
	if strings.EqualFold(system, "xvlock") {
		return "PAPER_ONLY_EXTERNAL_LIVE_BLOCK: PolyUS Retail has no durable client-order recovery identity"
	}
	return "PAPER_ACTIVE; LIVE_STAGED_DISABLED_UNTIL_ARM_ALLOWLIST_AND_SEALED_PROOF"
}

type r148StagedPaperPreflight struct {
	VenueShape, LiveState, Reason string
	Cost, Fee, NetFloor           float64
	Accept                        bool
	Evidence                      map[string]any
}

// r148StagedPaperCheck recomputes the all-in package from the immutable full books. It does not
// accept header totals on faith, use a midpoint, or fill only the cheap legs. Quote age includes
// time elapsed between capture and this simulated FOK decision.
func r148StagedPaperCheck(b storage.ResearchRouteBundle, now time.Time, budget float64) r148StagedPaperPreflight {
	out := r148StagedPaperPreflight{LiveState: r148StagedPaperLiveState(b.SystemID),
		Evidence: map[string]any{"paper_route": "full-package-simulated-fok-v1", "flat_on_reject": true}}
	reject := func(reason string) r148StagedPaperPreflight {
		out.Reason = reason
		return out
	}
	if !r148StagedPaperSystem(b.SystemID) {
		return reject("system is not one of the four exact staged Paper products")
	}
	if b.CertificateStatus != "verified" {
		return reject("resolution/payoff certificate is not verified")
	}
	if len(b.Legs) < 2 || len(b.Legs) > 6 || b.Size <= 0 || b.PayoutFloor < 0 || b.AtomicRoute ||
		math.IsNaN(b.Size) || math.IsInf(b.Size, 0) || math.IsNaN(b.PayoutFloor) || math.IsInf(b.PayoutFloor, 0) {
		return reject("bundle shape is not a supported non-atomic 2-6 leg package")
	}
	venueShape, err := r148StagedPaperVenueShape(b)
	if err != nil {
		return reject(err.Error())
	}
	out.VenueShape = venueShape
	if now.IsZero() {
		now = time.Now().UTC()
	}
	elapsed := now.Sub(b.Observed)
	if b.Observed.IsZero() || elapsed < 0 || elapsed > r148StagedPaperFreshFor {
		return reject("frozen package quote is no longer current")
	}
	for _, leg := range b.Legs {
		ageAtDecision := leg.Age + elapsed.Seconds()
		if leg.Age < 0 || math.IsNaN(ageAtDecision) || math.IsInf(ageAtDecision, 0) ||
			ageAtDecision > r148StagedPaperFreshFor.Seconds() {
			return reject("at least one frozen leg quote is stale at package admission")
		}
		cost, fee, legErr := r148StagedPaperLegAllIn(leg)
		if legErr != nil {
			return reject("leg " + leg.LegID + ": " + legErr.Error())
		}
		if math.Abs(cost-leg.IntegratedCost) > 1e-7 || math.Abs(fee-leg.ExactFee) > 1e-7 {
			return reject("frozen leg all-in does not match its immutable receipt")
		}
		out.Cost += cost
		out.Fee += fee
	}
	out.NetFloor = b.PayoutFloor - out.Cost - out.Fee
	if math.Abs(out.Cost-b.Cost) > 1e-7 || math.Abs(out.Fee-b.Fee) > 1e-7 ||
		math.Abs(out.NetFloor-b.NetFloor) > 1e-7 {
		return reject("full-package economics do not match the immutable bundle header")
	}
	if out.NetFloor <= 0 {
		return reject("full-package fee-net payout floor is not positive")
	}
	allIn := out.Cost + out.Fee
	if budget <= 0 || math.IsNaN(budget) || math.IsInf(budget, 0) || allIn > budget+1e-9 {
		return reject(fmt.Sprintf("Combo portfolio capacity is $%.2f but this exact package needs $%.2f", math.Max(0, budget), allIn))
	}
	out.Accept = true
	out.Reason = "accepted: complete verified package filled simultaneously at frozen executable books"
	out.Evidence["venue_shape"] = venueShape
	out.Evidence["all_in_usd"] = allIn
	out.Evidence["fee_net_floor_usd"] = out.NetFloor
	out.Evidence["size_units"] = b.Size
	out.Evidence["quote_age_limit_seconds"] = r148StagedPaperFreshFor.Seconds()
	out.Evidence["live_state"] = out.LiveState
	return out
}

// r148StagedPaperBudget applies the shared independently compounded Combo equity, the operator's
// 50% deployed-capital ceiling, and a 5% single-package ceiling. No contract-count cap exists:
// larger exact capacity rows become admissible automatically as this portfolio compounds.
func (s *Server) r148StagedPaperBudget(ctx context.Context) float64 {
	equity := s.parlayBankroll()
	available := s.parlayAvailableUSD(ctx)
	if equity <= 0 || available <= 0 || math.IsNaN(equity) || math.IsInf(equity, 0) ||
		math.IsNaN(available) || math.IsInf(available, 0) {
		return 0
	}
	deployed := math.Max(0, equity-available)
	portfolioHeadroom := math.Max(0, .50*equity-deployed)
	return math.Max(0, math.Min(available, math.Min(portfolioHeadroom, .05*equity)))
}

func (s *Server) r148StagedPaperLegacyDuplicate(ctx context.Context, b storage.ResearchRouteBundle) bool {
	if !strings.EqualFold(b.SystemID, "xvlock") || s == nil {
		return false
	}
	if !s.xvlLockContext(ctx, 0) {
		return true // fail closed rather than risk funding the same K-PUS lock twice
	}
	defer s.xvlMu.Unlock()
	book := s.xvlLoadLocked()
	for _, row := range book.Open {
		if row.Key == b.OpportunityID && row.Staked > 0 {
			return true
		}
	}
	for _, row := range book.Closed {
		if row.Key == b.OpportunityID && row.Staked > 0 {
			return true
		}
	}
	return false
}

// sweepR148StagedPaperBundles settles previously accepted packages first, then writes at most one
// durable flat decision for a fresh candidate. The old acceptance branch stays behind the R166
// fail-closed gate for settlement tests.
func (s *Server) sweepR148StagedPaperBundles(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	changed := false
	pending, err := s.store.PendingStagedPaperBundleSettlements(ctx, 100)
	if err != nil {
		_ = s.store.Audit(ctx, "error", "paper", "staged Paper bundle settlement read failed", err.Error())
		return
	}
	for _, row := range pending {
		inserted, settleErr := s.store.AppendStagedPaperBundleSettlement(ctx, row)
		if settleErr != nil {
			_ = s.store.Audit(ctx, "error", "paper", "staged Paper bundle settlement failed", settleErr.Error())
			continue
		}
		changed = changed || inserted
	}
	if changed {
		s.invalidatePortfolioEquityCache()
	}
	if !s.researchBundlePaperAutoReady() {
		return
	}
	now := time.Now().UTC()
	rows, err := s.store.RecentNamedResearchRouteBundles(ctx, now.Add(-2*r148StagedPaperFreshFor), 200)
	if err != nil {
		_ = s.store.Audit(ctx, "error", "paper", "staged Paper bundle candidate read failed", err.Error())
		return
	}
	budget := s.r148StagedPaperBudget(ctx)
	equity := s.parlayBankroll()
	for _, bundle := range rows {
		check := r148StagedPaperCheck(bundle, now, budget)
		if check.Accept && !s.r166CanBookVerifiedMultiLegPaper() {
			check.Accept = false
			check.Reason = r166UnverifiedMultiLegPaperReason
			if check.Evidence == nil {
				check.Evidence = map[string]any{}
			}
			check.Evidence["paper_execution_state"] = "not_observed"
			check.Evidence["new_pnl_booked"] = false
		}
		if check.Accept && s.r148StagedPaperLegacyDuplicate(ctx, bundle) {
			check.Accept = false
			check.Reason = "flat duplicate: the legacy lockstack ledger already funded this xvlock opportunity"
		}
		evidence := check.Evidence
		if evidence == nil {
			evidence = map[string]any{}
		}
		evidence["bundle_id"] = bundle.BundleID
		evidence["system_id"] = bundle.SystemID
		evidence["opportunity_id"] = bundle.OpportunityID
		evidence["certificate_hash"] = bundle.CertificateHash
		evidence["state_vector_hash"] = bundle.StateVectorHash
		evidence["portfolio_equity_usd"] = equity
		evidence["admission_budget_usd"] = budget
		evidenceJSON, marshalErr := json.Marshal(evidence)
		if marshalErr != nil {
			continue
		}
		venueShape := check.VenueShape
		if venueShape == "" {
			venueShape, _ = r148StagedPaperVenueShape(bundle)
			if venueShape == "" {
				venueShape = "unsupported" // schema-safe rejected receipt; evidence carries the exact reason
			}
		}
		inserted, accepted, writeErr := s.store.AppendStagedPaperBundleDecision(ctx, storage.StagedPaperBundleDecision{
			PackageID: "staged-paper:" + bundle.BundleID, BundleID: bundle.BundleID,
			SystemID: bundle.SystemID, OpportunityID: bundle.OpportunityID,
			CanonicalEventID: bundle.CanonicalEventID, Cohort: bundle.Cohort,
			VenueShape: venueShape, LiveState: check.LiveState, Reason: check.Reason,
			EvidenceJSON: string(evidenceJSON), Candidate: bundle.Observed, Decided: now,
			Size: bundle.Size, Cost: bundle.Cost, Fee: bundle.Fee,
			PayoutFloor: bundle.PayoutFloor, NetFloor: bundle.NetFloor,
			LegCount: len(bundle.Legs), Accept: check.Accept,
		})
		if writeErr != nil {
			_ = s.store.Audit(ctx, "error", "paper", "staged Paper bundle decision failed", writeErr.Error())
			continue
		}
		if !inserted {
			continue
		}
		if accepted {
			s.invalidatePortfolioEquityCache()
			_ = s.store.Audit(ctx, "info", "paper", fmt.Sprintf(
				"staged Paper %s accepted: %s · %.4f units · $%.2f all-in · floor +$%.2f",
				bundle.SystemID, bundle.OpportunityID, bundle.Size, bundle.Cost+bundle.Fee, bundle.NetFloor), "")
			return // bounded: one new funded package per pass
		}
	}
}
