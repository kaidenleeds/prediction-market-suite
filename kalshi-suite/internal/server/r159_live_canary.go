package server

import (
	"math"
	"strings"
	"time"
)

func liveOneContractCanaryWireReason(requested, auto, sealed, newML, taker, fillOrKill bool,
	count int) string {
	if !requested {
		return ""
	}
	if !auto || sealed || newML || !taker || fillOrKill || count != 1 {
		return "one-contract canary requires an unsealed System taker IOC with count=1"
	}
	return ""
}

// liveConfiguredCanaryWireReason applies the current operator config as a final sizing ceiling.
// Planning already gives canary identities q1 precedence, but an in-flight normal candidate must
// not escape if the operator moves its exact identity into the canary allowlist before submit.
func (s *Server) liveConfiguredCanaryWireReason(c liveMirrorCandidate, route string,
	requested, auto, sealed, newML, taker, fillOrKill bool, count int) string {
	risk := s.cfg().Risk
	allowed, _, why := parseLiveSystemCanaryAllowlist(
		risk.LiveSystemCanaryAllowlist, risk.LiveSystemAllowlist)
	if why != "" {
		return "one-contract canary configuration is invalid: " + why
	}
	_, canaryOnly := allowed[liveSystemIdentityKey(c, route)]
	if requested && !canaryOnly {
		return "claimed one-contract canary is no longer in the current canary allowlist"
	}
	if !canaryOnly {
		return ""
	}
	if !requested {
		return "current exact System identity is canary-only and requires explicit one-contract canary authority"
	}
	return liveOneContractCanaryWireReason(
		requested, auto, sealed, newML, taker, fillOrKill, count)
}

// liveConfiguredCanaryAdmissionReason is the queue-side provenance belt for the four temporary
// q=1 experiments. A Paper fill is allowed to keep its own portfolio/logging path, but it may not
// manufacture the exact-signal receipt that grants a canary access to the LIVE queue. Only the
// canonical LIVE-first worker may arrive here with all of these immutable facts already bound:
// source family, input topology, original detector clock, exact signal contract, q1 IOC plan, and
// the freshly persisted one-unit receipt.
func (s *Server) liveConfiguredCanaryAdmissionReason(c liveMirrorCandidate, route string,
	now time.Time) string {
	identity := liveSystemIdentityKey(c, route)
	risk := s.cfg().Risk
	allowed, _, why := parseLiveSystemCanaryAllowlist(
		risk.LiveSystemCanaryAllowlist, risk.LiveSystemAllowlist)
	if why != "" {
		// An invalid canary configuration must fail closed for every identity that could receive
		// experimental authority, without parking unrelated sealed/non-System routes.
		if liveSystemCanaryIdentitySupported(identity) {
			return "one-contract-canary-configuration-invalid:" + why
		}
		return ""
	}
	if _, canary := allowed[identity]; !canary {
		return ""
	}

	family := strings.ToLower(strings.TrimSpace(liveMirrorFamily(c)))
	expectedTopology := ""
	switch family {
	case "kalshi-flow":
		expectedTopology = "K"
	case "spotlag":
		expectedTopology = "K-SPOT"
	default:
		return "one-contract-canary-family-is-not-supported"
	}
	if strings.TrimSpace(c.Source) != "auto-cons-"+family {
		return "one-contract-canary-requires-canonical-live-first-source"
	}
	if !c.LiveFirstUnitPersisted {
		return "one-contract-canary-requires-completed-live-first-preflight"
	}
	if strings.ToUpper(strings.TrimSpace(c.InputTopology)) != expectedTopology {
		return "one-contract-canary-input-topology-mismatch"
	}
	if now.IsZero() {
		now = time.Now()
	}
	originAge := now.Sub(c.InputObservedAt)
	if c.InputObservedAt.IsZero() || originAge < -2*time.Second || originAge > liveMirrorTTL {
		return "one-contract-canary-origin-clock-missing-or-stale"
	}
	bound, contract, bindWhy := r147BindSignalContract(c, route)
	if bindWhy != "" || strings.TrimSpace(c.SignalContractID) == "" ||
		bound.SignalContractID != strings.TrimSpace(c.SignalContractID) ||
		!strings.EqualFold(bound.InputTopology, expectedTopology) {
		return "one-contract-canary-exact-signal-contract-mismatch"
	}
	// K-SPOT's external receipt has a stricter 20-second source limit than the queue's 25-second
	// lifetime. Reuse the canonical input-clock contract so this belt cannot accidentally widen it.
	if inputWhy := liveAllocationInputClockReason(contract, r147LiveInputClockSnapshot{
		ExternalObservedAt: c.InputObservedAt,
		Now:                now,
	}); inputWhy != "" {
		return inputWhy
	}
	if !s.liveFirstMirrorReceiptFresh(c, now) {
		return "one-contract-canary-live-first-receipt-is-not-fresh-or-exact-q1-ioc"
	}
	return ""
}

// liveOneContractCanaryReceiptReason validates only the short-lived internal receipt shape used
// between LIVE-first preflight, arbitration and dispatch. A negative/immature observed lower is
// deliberately allowed and preserved, but it never becomes normal proof: the explicit Canary bit,
// UNPROVEN basis, exact q1 IOC plan and positive exact-cell point must all agree. The venue handler
// still reconstructs and re-proves the point/fee independently.
func (s *Server) liveOneContractCanaryReceiptReason(c liveMirrorCandidate) string {
	if !c.Canary {
		return "one-contract canary marker is absent"
	}
	if why := s.liveSystemCanaryReason(c, "taker"); why != "" {
		return why
	}
	if c.ProspectiveQty != 1 || c.ProspectiveTimeInForce != liveProspectiveIOC ||
		c.ArbitrationQuote.Maker || c.ArbitrationQuote.Depth < 1 {
		return "one-contract canary receipt is not an exact q1 taker IOC"
	}
	if c.ArbitrationMean+1e-12 < s.liveAllocationEdgeFloor() ||
		math.IsNaN(c.ArbitrationMean) || math.IsInf(c.ArbitrationMean, 0) ||
		math.IsNaN(c.ArbitrationLower) || math.IsInf(c.ArbitrationLower, 0) ||
		c.ArbitrationFee < 0 || math.IsNaN(c.ArbitrationFee) ||
		math.IsInf(c.ArbitrationFee, 0) {
		return "one-contract canary point, observed lower, or fee receipt is invalid"
	}
	if c.ProspectiveRequestedFee < 0 || c.ProspectiveRequestedFeePC < 0 ||
		math.IsNaN(c.ProspectiveRequestedFee) ||
		math.IsInf(c.ProspectiveRequestedFee, 0) ||
		math.IsNaN(c.ProspectiveRequestedFeePC) ||
		math.IsInf(c.ProspectiveRequestedFeePC, 0) ||
		math.Abs(c.ProspectiveRequestedFee-c.ProspectiveRequestedFeePC) > 1e-12 ||
		math.Abs(c.ArbitrationFee-c.ProspectiveRequestedFeePC) > 1e-12 ||
		strings.TrimSpace(c.ProspectiveFeeSource) == "" {
		return "one-contract canary exact q1 fee receipt is inconsistent"
	}
	basis := strings.TrimSpace(c.ArbitrationBasis)
	if basis == "" || basis != strings.TrimSpace(c.CanaryBasis) ||
		!strings.HasPrefix(basis, "one-contract-canary:UNPROVEN:") ||
		!strings.Contains(basis, ":point-source=exact-paper-model-cell:") {
		return "one-contract canary UNPROVEN basis is missing or inconsistent"
	}
	return ""
}

// liveMirrorArbitrationReceiptReusable is the sole proof-shape decision for short-lived internal
// quote reuse. Normal candidates still require lower>0. Canary candidates take their separate
// validator above, which preserves a negative lower strictly as diagnostic evidence.
func (s *Server) liveMirrorArbitrationReceiptReusable(c liveMirrorCandidate, now time.Time,
	maxAge time.Duration) bool {
	if maxAge <= 0 || c.ArbitrationAt.IsZero() {
		return false
	}
	age := now.Sub(c.ArbitrationAt)
	if age < 0 || age > maxAge || c.ArbitrationQuote.Price <= 0 ||
		c.ArbitrationQuote.Depth <= 0 || strings.TrimSpace(c.ArbitrationBasis) == "" {
		return false
	}
	if c.Canary {
		return s.liveOneContractCanaryReceiptReason(c) == ""
	}
	return c.ArbitrationLower > 0
}
