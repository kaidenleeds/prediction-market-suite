package server

import (
	"context"
	"math"
	"strings"
	"time"
)

const (
	liveProspectiveIOC = "immediate_or_cancel"
	liveProspectiveFOK = "fill_or_kill"
	// A model-point canary is useful research telemetry, but it is not exchange execution
	// evidence and therefore cannot authorize a real order.
	liveOneContractCanaryCashRetiredReason = "one-contract-canary-retired-model-point-is-not-authenticated-execution-evidence"
)

// liveProspectiveQuantityPlan is one self-consistent order: its current proof, exact requested
// aggregate BUY fee, visible depth, proof-sized dollars, and percentage rail all agree on Qty.
// ProofFeePC may be more conservative than RequestedFeePC for IOC because any positive partial
// fill must remain valid down to one contract.
type liveProspectiveQuantityPlan struct {
	Qty                            float64
	RequestedFee, RequestedFeePC   float64
	ProofFeePC                     float64
	FeeSource, TimeInForce, Reason string
	Basis                          string
	Mean, Lower                    float64
	TargetUSD, EffectiveFrac       float64
	Capacity                       float64
	Canary                         bool
}

func (p liveProspectiveQuantityPlan) valid() bool {
	base := p.Qty >= 1 && p.RequestedFee >= 0 && p.RequestedFeePC >= 0 &&
		p.ProofFeePC >= 0 && strings.TrimSpace(p.FeeSource) != "" &&
		(p.TimeInForce == liveProspectiveIOC || p.TimeInForce == liveProspectiveFOK) &&
		p.Mean > 0
	if !base {
		return false
	}
	if p.Canary {
		return p.Qty == 1 && p.TimeInForce == liveProspectiveIOC &&
			strings.HasPrefix(p.Basis, "one-contract-canary:UNPROVEN:")
	}
	return p.Lower > 0
}

// liveOneContractCanaryPlan is a deliberately separate evidence-collection authority. It does
// not turn a negative/immature confidence bound into proof. Its point comes only from the exact
// family+venue+side taker cell used by Paper/model exploration; exact strategy-origin history
// remains the separate normal confidence proof. The canary asks whether that explicitly unproven
// point survives the current one-contract taker fee, then caps collection at one IOC contract.
// The handler repeats this check at the final wire.
func (s *Server) liveOneContractCanaryPlan(ctx context.Context, c liveMirrorCandidate,
	q liveMirrorQuote, cell verdictEnt, normalProofWhy string) (liveProspectiveQuantityPlan, string) {
	if why := s.liveSystemCanaryReason(c, "taker"); why != "" {
		return liveProspectiveQuantityPlan{}, why
	}
	if q.Maker || q.Depth < 1 || q.Price <= 0 || q.Price >= 1 {
		return liveProspectiveQuantityPlan{}, "one-contract-canary-current-taker-touch-unavailable"
	}
	if c.At.IsZero() || time.Since(c.At) < -2*time.Second || time.Since(c.At) > liveMirrorTTL {
		return liveProspectiveQuantityPlan{}, "one-contract-canary-signal-receipt-stale"
	}
	action := strings.ToUpper(strings.TrimSpace(c.Action))
	if action == "" {
		action = "BUY"
	}
	if action != "BUY" {
		return liveProspectiveQuantityPlan{}, "one-contract-canary-buy-singles-only"
	}
	bound, contract, why := s.liveAllocationBindSignalContract(c, "taker", true)
	if why != "" {
		return liveProspectiveQuantityPlan{}, why
	}
	c = bound
	if why := s.liveAllocationInputFreshReason(contract, c); why != "" {
		return liveProspectiveQuantityPlan{}, why
	}
	if s.store == nil {
		return liveProspectiveQuantityPlan{}, "one-contract-canary-ledger-unavailable"
	}
	if !s.liveAllocationSignalFresh(c, time.Now().UTC().Add(-liveMirrorTTL)) {
		return liveProspectiveQuantityPlan{}, "one-contract-canary-no-fresh-exact-signal-receipt"
	}
	cellFamily, cellPlatform, cellSide, cellOrigin, cellRoute, cellOK := gfExactCell(cell)
	if !cellOK || cellFamily != liveMirrorFamily(c) ||
		cellPlatform != strings.ToLower(strings.TrimSpace(c.Platform)) ||
		cellSide != strings.ToUpper(strings.TrimSpace(c.Side)) ||
		cellOrigin != "model" || cellRoute != "taker" {
		return liveProspectiveQuantityPlan{},
			"one-contract-canary-exact-paper-model-cell-mismatch"
	}
	totalFee, feePC, source, feeKnown := s.liveExactAggregateBuyFee(c, 1, q.Price)
	if !feeKnown {
		return liveProspectiveQuantityPlan{}, "one-contract-canary-current-fee-unavailable"
	}
	point, pointKnown := gfCurrentRouteEdge(cell.Mean, cell.MeanAsk, q.Price,
		cell.FeePC, totalFee, 1)
	if !pointKnown || point+1e-12 < s.liveAllocationEdgeFloor() {
		return liveProspectiveQuantityPlan{},
			"one-contract-canary-current-point-edge-below-live-floor"
	}
	observedLower, lowerKnown := gfCurrentRouteEdge(cell.Lo, cell.MeanAsk, q.Price,
		cell.FeePC, totalFee, 1)
	if !lowerKnown {
		observedLower = 0
	}
	identity := liveSystemIdentityKey(c, "taker")
	basis := "one-contract-canary:UNPROVEN:" + identity +
		":point-source=exact-paper-model-cell:input=" + c.InputTopology +
		":contract=" + c.SignalContractID
	if normalProofWhy != "" {
		basis += ":normal-proof-refused=" + normalProofWhy
	}
	return liveProspectiveQuantityPlan{
		Qty: 1, RequestedFee: totalFee, RequestedFeePC: feePC,
		ProofFeePC: feePC, FeeSource: source, TimeInForce: liveProspectiveIOC,
		Reason: "operator-selected UNPROVEN canary; exactly one IOC contract",
		Basis:  basis, Mean: point, Lower: observedLower,
		TargetUSD: q.Price + totalFee, EffectiveFrac: 0, Capacity: 1,
		Canary: true,
	}, ""
}

// liveOneContractCanaryReproof is the final money boundary. The former implementation rebuilt a
// one-contract order from a Paper/model point estimate; that still was not authenticated exchange
// evidence. Keep the diagnostic calculator above for research, but retire its cash authority.
func (s *Server) liveOneContractCanaryReproof(ctx context.Context, c liveMirrorCandidate,
	q liveMirrorQuote) (liveProspectiveQuantityPlan, string) {
	return liveProspectiveQuantityPlan{}, liveOneContractCanaryCashRetiredReason
}

func (s *Server) liveExactAggregateBuyFee(c liveMirrorCandidate, quantity, price float64) (
	total, perContract float64, source string, ok bool) {
	if quantity < 1 || quantity != math.Floor(quantity) || price <= 0 || price >= 1 {
		return 0, 0, "", false
	}
	total, source, ok = s.fillFeeReceipt(c.Platform, c.Ticker, false, quantity, price)
	if !ok || total < 0 || strings.TrimSpace(source) == "" ||
		math.IsNaN(total) || math.IsInf(total, 0) {
		return 0, 0, "", false
	}
	return total, total / quantity, source, true
}

func (s *Server) liveProspectiveHardQuantity(bank float64, q liveMirrorQuote) int {
	if bank <= 0 || q.Price <= 0 || q.Price >= 1 || q.Depth < 1 {
		return 0
	}
	maxOrder, ok := liveRailLimit(bank, s.cfg().Risk.LiveMaxOrderPct,
		s.cfg().Risk.LiveMaxOrderUSD)
	if !ok || maxOrder <= 0 {
		return 0
	}
	hard := int(math.Floor(math.Min(q.Depth, maxOrder/q.Price)))
	s.riskMu.Lock()
	maxContracts := s.maxContracts
	s.riskMu.Unlock()
	if maxContracts > 0 && hard > maxContracts {
		hard = maxContracts
	}
	if q.MinQtyKnown && q.MinQty > 0 && float64(hard)+1e-9 < q.MinQty {
		return 0
	}
	return hard
}

// liveProspectivePointQuantityAvailable is the pre-proof model-point gate. It checks every exact
// whole quantity because Kalshi's cent-alignment fee per contract is intentionally non-monotone.
// This performs no network read; fillFeeReceipt consumes the already-loaded fee authority.
func (s *Server) liveProspectivePointQuantityAvailable(c liveMirrorCandidate, q liveMirrorQuote,
	bank float64, cell verdictEnt) bool {
	hard := s.liveProspectiveHardQuantity(bank, q)
	for quantity := hard; quantity >= 1; quantity-- {
		fee, _, _, known := s.liveExactAggregateBuyFee(c, float64(quantity), q.Price)
		if !known {
			continue
		}
		edge, edgeKnown := gfCurrentRouteEdge(cell.Mean, cell.MeanAsk, q.Price,
			cell.FeePC, fee, float64(quantity))
		if edgeKnown && edge > 0 {
			return true
		}
	}
	return false
}

func (s *Server) liveProspectiveDiagnosticProofForPlan(ctx context.Context,
	c liveMirrorCandidate, price, quantity float64, isolated bool) (
	bool, string, float64, float64, float64) {
	if isolated {
		return s.livePolicyMirrorIsolatedDiagnosticProofAtQuantity(ctx, c, price, quantity)
	}
	return s.liveProspectiveDiagnosticProofAtQuantity(ctx, c, price, false, quantity)
}

// liveProspectiveQuantityPlan evaluates the real wire contract:
//   - an exact identity in the operator's canary allowlist is always one IOC contract, even when
//     its normal confidence proof would otherwise authorize a larger order;
//   - every non-canary identity fails closed until the authoritative LIVE-fill proof cohort exists.
//
// The exact IOC/FOK quantity search remains in liveProspectiveDiagnosticPlan so research can
// measure what the opportunity-only policy would have requested without granting cash authority.
func (s *Server) liveProspectivePlan(ctx context.Context, c liveMirrorCandidate, q liveMirrorQuote,
	bank float64, cell verdictEnt, isolated bool) (liveProspectiveQuantityPlan, string) {
	return s.liveProspectivePlanMode(ctx, c, q, bank, cell, isolated, false)
}

// liveProspectiveDiagnosticPlan preserves the aggregate-fee/capacity calculation as research
// output. It must never be sent to a venue or simulated as an authorized normal mirror order.
func (s *Server) liveProspectiveDiagnosticPlan(ctx context.Context, c liveMirrorCandidate,
	q liveMirrorQuote, bank float64, cell verdictEnt, isolated bool) (
	liveProspectiveQuantityPlan, string) {
	return s.liveProspectivePlanMode(ctx, c, q, bank, cell, isolated, true)
}

func (s *Server) liveProspectivePlanMode(ctx context.Context, c liveMirrorCandidate,
	q liveMirrorQuote, bank float64, cell verdictEnt, isolated, diagnostic bool) (
	liveProspectiveQuantityPlan, string) {
	hard := s.liveProspectiveHardQuantity(bank, q)
	if hard < 1 {
		return liveProspectiveQuantityPlan{}, "current-depth-or-5pct-sizing-rail-below-one-contract"
	}
	// Canary membership is an operator sizing ceiling, not merely a fallback around weak evidence.
	// Letting a canary identity take the normal proof branch could silently turn the approved q1
	// experiment into a multi-contract IOC/FOK order as its sample matured.
	if s.liveSystemCanaryReason(c, "taker") == "" {
		if !diagnostic {
			return liveProspectiveQuantityPlan{}, liveOneContractCanaryCashRetiredReason
		}
		plan, why := s.liveOneContractCanaryPlan(
			ctx, c, q, cell, "operator-selected-canary-only-route")
		if why != "" {
			return liveProspectiveQuantityPlan{}, why
		}
		if !plan.valid() {
			return liveProspectiveQuantityPlan{},
				"one-contract-canary-plan-failed-its-exact-wire-contract"
		}
		return plan, ""
	}
	if !diagnostic {
		// Current-generation UnitTrial rows are pre-IOC opportunities. Normal multi-contract
		// promotion remains closed until a separate authoritative fill-conditioned cohort exists.
		return liveProspectiveQuantityPlan{}, liveFillConditionedProofUnavailableReason
	}
	oneFee, _, _, oneKnown :=
		s.liveExactAggregateBuyFee(c, 1, q.Price)
	onePoint := false
	if oneKnown {
		edge, known := gfCurrentRouteEdge(cell.Mean, cell.MeanAsk, q.Price,
			cell.FeePC, oneFee, 1)
		onePoint = known && edge > 0
	}
	oneProof, oneBasis, oneMean, oneLower, oneProofFee :=
		s.liveProspectiveDiagnosticProofForPlan(ctx, c, q.Price, 1, isolated)
	if oneKnown && onePoint && oneProof && oneLower >= s.liveAllocationEdgeFloor() {
		count, usd, frac, capacity, why := s.liveProspectiveAllocationSize(c.Platform, bank,
			q.Price, oneMean, oneLower, oneProofFee, q.Depth, q.MinQty, q.MinQtyKnown)
		if why == "" && count >= 1 {
			requestedFee, requestedPC, requestedSource, feeOK :=
				s.liveExactAggregateBuyFee(c, count, q.Price)
			if feeOK {
				return liveProspectiveQuantityPlan{
					Qty: count, RequestedFee: requestedFee, RequestedFeePC: requestedPC,
					ProofFeePC: oneProofFee, FeeSource: requestedSource,
					TimeInForce: liveProspectiveIOC,
					Reason:      "one-contract fee and proof cover every possible IOC partial fill",
					Basis:       oneBasis, Mean: oneMean, Lower: oneLower,
					TargetUSD: usd, EffectiveFrac: frac, Capacity: capacity,
				}, ""
			}
		}
	}

	anyPoint := onePoint
	lastProofReason := oneBasis
	// PolyUS currently has no proved full-fill-or-zero order contract on this path. Keep its old
	// one-contract-worst-case IOC rule; aggregate-fee reliance is Kalshi-only until the destination
	// wire can enforce the same all-or-none semantics.
	if strings.ToLower(strings.TrimSpace(c.Platform)) != "kalshi" {
		if !anyPoint {
			return liveProspectiveQuantityPlan{},
				"current-executable-price-or-fee-erases-point-edge"
		}
		if strings.TrimSpace(lastProofReason) != "" {
			return liveProspectiveQuantityPlan{}, lastProofReason
		}
		return liveProspectiveQuantityPlan{},
			"prospective-allocation-no-self-consistent-one-contract-safe-quantity"
	}
	for quantity := hard; quantity >= 2; quantity-- {
		qty := float64(quantity)
		totalFee, feePC, source, known := s.liveExactAggregateBuyFee(c, qty, q.Price)
		if !known {
			continue
		}
		pointEdge, pointKnown := gfCurrentRouteEdge(cell.Mean, cell.MeanAsk, q.Price,
			cell.FeePC, totalFee, qty)
		if !pointKnown || pointEdge <= 0 {
			continue
		}
		anyPoint = true
		proofOK, basis, mean, lower, proofFee :=
			s.liveProspectiveDiagnosticProofForPlan(ctx, c, q.Price, qty, isolated)
		if !proofOK || lower < s.liveAllocationEdgeFloor() {
			if strings.TrimSpace(basis) != "" {
				lastProofReason = basis
			}
			continue
		}
		maxCount, usd, frac, capacity, why := s.liveProspectiveAllocationSize(c.Platform, bank,
			q.Price, mean, lower, proofFee, q.Depth, q.MinQty, q.MinQtyKnown)
		if why != "" || qty > maxCount+1e-9 {
			continue
		}
		return liveProspectiveQuantityPlan{
			Qty: qty, RequestedFee: totalFee, RequestedFeePC: feePC,
			ProofFeePC: proofFee, FeeSource: source, TimeInForce: liveProspectiveFOK,
			Reason: "aggregate fee is safe only when the complete proved quantity fills",
			Basis:  basis, Mean: mean, Lower: lower,
			TargetUSD: usd, EffectiveFrac: frac, Capacity: capacity,
		}, ""
	}
	if !anyPoint {
		return liveProspectiveQuantityPlan{},
			"current-executable-price-or-fee-erases-point-edge"
	}
	if strings.TrimSpace(lastProofReason) != "" {
		if canary, canaryWhy := s.liveOneContractCanaryPlan(
			ctx, c, q, cell, lastProofReason); canaryWhy == "" && canary.valid() {
			return canary, ""
		}
		return liveProspectiveQuantityPlan{}, lastProofReason
	}
	const noExactQuantity = "prospective-allocation-no-self-consistent-exact-quantity"
	if canary, canaryWhy := s.liveOneContractCanaryPlan(
		ctx, c, q, cell, noExactQuantity); canaryWhy == "" && canary.valid() {
		return canary, ""
	}
	return liveProspectiveQuantityPlan{}, noExactQuantity
}

func applyLiveProspectivePlan(c *liveMirrorCandidate, p liveProspectiveQuantityPlan) {
	if c == nil {
		return
	}
	c.ProspectiveQty = p.Qty
	c.ProspectiveRequestedFee = p.RequestedFee
	c.ProspectiveRequestedFeePC = p.RequestedFeePC
	c.ProspectiveFeeSource = p.FeeSource
	c.ProspectiveTimeInForce = p.TimeInForce
	c.ProspectivePlanReason = p.Reason
	c.Canary = p.Canary
	c.CanaryBasis = ""
	if p.Canary {
		c.CanaryBasis = p.Basis
	}
}

func liveProspectiveFOKPartialReason(timeInForce, state string, filled, requested float64) string {
	if timeInForce == liveProspectiveFOK && filled > 0 && filled+1e-9 < requested {
		return "fill-or-kill returned a partial fill"
	}
	return ""
}
