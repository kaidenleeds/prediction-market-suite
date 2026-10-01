package payoffsolver

import "testing"

func verifiedSemantic(payoff, venue string) SemanticInstrument {
	return SemanticInstrument{Venue: venue, Ticker: venue + "-ticker", EventID: "event-1",
		PayoffID: payoff, NativeSide: "YES", Orientation: "same", SettlementSource: "official-source",
		RulesHash: "rules-hash", CrossVenueBasis: "basis-1", FeeAuthority: venue + "-fee",
		IdentityStatus: "verified", EventVersion: 2, PayoffVersion: 3}
}

func TestRouteSemanticBasisSamePayoffVerified(t *testing.T) {
	left, right := verifiedSemantic("payoff-a", "kalshi"), verifiedSemantic("payoff-a", "polyus")
	right.Orientation = "inverse"
	route := RouteSemanticBasis(left, right, nil)
	if !route.SolverEligible || route.State != "VERIFIED_SEMANTIC_BASIS" ||
		route.Kind != "same-payoff" || route.LeftSide != "YES" || route.RightSide != "NO" {
		t.Fatalf("route=%+v", route)
	}
}

func TestRouteSemanticBasisFailsClosed(t *testing.T) {
	base, twin := verifiedSemantic("payoff-a", "kalshi"), verifiedSemantic("payoff-a", "polyus")
	tests := []struct {
		name string
		mut  func(*SemanticInstrument, *SemanticInstrument)
	}{
		{"structural identity", func(a, _ *SemanticInstrument) { a.IdentityStatus = "structural" }},
		{"source mismatch", func(_, b *SemanticInstrument) { b.SettlementSource = "other" }},
		{"rules mismatch", func(_, b *SemanticInstrument) { b.RulesHash = "other" }},
		{"basis missing", func(_, b *SemanticInstrument) { b.CrossVenueBasis = "" }},
		{"fee authority missing", func(a, _ *SemanticInstrument) { a.FeeAuthority = "" }},
		{"event version drift", func(_, b *SemanticInstrument) { b.EventVersion++ }},
		{"payoff version drift", func(_, b *SemanticInstrument) { b.PayoffVersion++ }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, b := base, twin
			tc.mut(&a, &b)
			if route := RouteSemanticBasis(a, b, nil); route.SolverEligible || route.State != "BLOCKED" || route.Blocker == "" {
				t.Fatalf("route=%+v", route)
			}
		})
	}
}

func TestRouteSemanticBasisRequiresVerifiedRelation(t *testing.T) {
	left, right := verifiedSemantic("payoff-a", "kalshi"), verifiedSemantic("payoff-b", "polyus")
	floor := 1.0
	verified := SemanticRelation{EventID: "event-1", LeftPayoffID: "payoff-a", RightPayoffID: "payoff-b",
		EventVersion: 2, LeftPayoffVersion: 3, RightPayoffVersion: 3,
		RelationType: "exhaustive_with", IdentityStatus: "verified", PayoutFloor: &floor}
	if route := RouteSemanticBasis(left, right, []SemanticRelation{verified}); !route.SolverEligible || route.Kind != "exhaustive_with" {
		t.Fatalf("verified relation route=%+v", route)
	}
	correlated := verified
	correlated.RelationType = "correlated"
	if route := RouteSemanticBasis(left, right, []SemanticRelation{correlated}); route.SolverEligible || route.Blocker == "" {
		t.Fatalf("correlation must not certify payoff identity: %+v", route)
	}
	verified.PayoutFloor = nil
	if route := RouteSemanticBasis(left, right, []SemanticRelation{verified}); route.SolverEligible {
		t.Fatalf("missing relation payout floor passed: %+v", route)
	}
	verified.PayoutFloor = &floor
	verified.RightPayoffVersion++
	if route := RouteSemanticBasis(left, right, []SemanticRelation{verified}); route.SolverEligible {
		t.Fatalf("stale relation payoff version passed: %+v", route)
	}
}
