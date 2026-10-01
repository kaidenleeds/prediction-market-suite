package payoffsolver

import "strings"

// SemanticInstrument is the minimum immutable identity certificate needed before two venue
// instruments may be compared. It deliberately contains no title: text similarity is not proof.
type SemanticInstrument struct {
	Venue, Ticker, EventID, PayoffID             string
	NativeSide, Orientation                      string
	SettlementSource, RulesHash, CrossVenueBasis string
	FeeAuthority, IdentityStatus                 string
	EventVersion, PayoffVersion                  int
}

type SemanticRelation struct {
	EventID, LeftPayoffID, RightPayoffID  string
	RelationType, IdentityStatus          string
	EventVersion                          int
	LeftPayoffVersion, RightPayoffVersion int
	PayoutFloor                           *float64
}

// SemanticRoute never authorizes an order. SolverEligible means only that the immutable semantic
// certificate can be handed to the payoff solver; books, fees, clocks, capacity, partial fills,
// untouched replication and portfolio risk remain separate mandatory gates.
type SemanticRoute struct {
	State, Kind, LeftSide, RightSide, Blocker string
	SolverEligible                            bool
}

func normalizedSide(side, orientation string) (string, bool) {
	side = strings.ToUpper(strings.TrimSpace(side))
	if side != "YES" && side != "NO" {
		return "", false
	}
	switch strings.ToLower(strings.TrimSpace(orientation)) {
	case "same", "basis":
		return side, true
	case "inverse":
		if side == "YES" {
			return "NO", true
		}
		return "YES", true
	default:
		return "", false
	}
}

func sameNonEmpty(a, b string) bool {
	a, b = strings.TrimSpace(a), strings.TrimSpace(b)
	return a != "" && b != "" && strings.EqualFold(a, b)
}

func relationFor(a, b SemanticInstrument, relations []SemanticRelation) (SemanticRelation, bool) {
	for _, relation := range relations {
		if relation.EventID != a.EventID || !strings.EqualFold(relation.IdentityStatus, "verified") {
			continue
		}
		if relation.EventVersion != a.EventVersion || relation.EventVersion != b.EventVersion {
			continue
		}
		direct := relation.LeftPayoffID == a.PayoffID && relation.LeftPayoffVersion == a.PayoffVersion &&
			relation.RightPayoffID == b.PayoffID && relation.RightPayoffVersion == b.PayoffVersion
		reverse := relation.LeftPayoffID == b.PayoffID && relation.LeftPayoffVersion == b.PayoffVersion &&
			relation.RightPayoffID == a.PayoffID && relation.RightPayoffVersion == a.PayoffVersion
		if direct || reverse {
			return relation, true
		}
	}
	return SemanticRelation{}, false
}

func blockedSemantic(reason string) SemanticRoute {
	return SemanticRoute{State: "BLOCKED", Blocker: reason}
}

// RouteSemanticBasis is the fail-closed Step-5 semantic-basis router. It compares immutable
// event/payoff/rules/source versions, never titles or visible prices.
func RouteSemanticBasis(left, right SemanticInstrument, relations []SemanticRelation) SemanticRoute {
	if left.Venue == "" || right.Venue == "" || left.Ticker == "" || right.Ticker == "" ||
		left.EventID == "" || right.EventID == "" || left.PayoffID == "" || right.PayoffID == "" {
		return blockedSemantic("incomplete immutable instrument identity")
	}
	if left.EventID != right.EventID || left.EventVersion <= 0 || left.EventVersion != right.EventVersion {
		return blockedSemantic("event identity/version mismatch")
	}
	if left.PayoffVersion <= 0 || right.PayoffVersion <= 0 {
		return blockedSemantic("payoff version missing")
	}
	if strings.EqualFold(left.IdentityStatus, "rejected") || strings.EqualFold(right.IdentityStatus, "rejected") {
		return blockedSemantic("identity was rejected")
	}
	if !strings.EqualFold(left.IdentityStatus, "verified") || !strings.EqualFold(right.IdentityStatus, "verified") {
		return blockedSemantic("both instruments need verified identity; structural/title-only identity is research-only")
	}
	if !sameNonEmpty(left.SettlementSource, right.SettlementSource) {
		return blockedSemantic("settlement source mismatch or missing")
	}
	if !sameNonEmpty(left.RulesHash, right.RulesHash) {
		return blockedSemantic("rules hash mismatch or missing")
	}
	if !sameNonEmpty(left.CrossVenueBasis, right.CrossVenueBasis) {
		return blockedSemantic("cross-venue basis mismatch or missing")
	}
	if strings.TrimSpace(left.FeeAuthority) == "" || strings.TrimSpace(right.FeeAuthority) == "" {
		return blockedSemantic("fee authority missing")
	}
	leftSide, lok := normalizedSide(left.NativeSide, left.Orientation)
	rightSide, rok := normalizedSide(right.NativeSide, right.Orientation)
	if !lok || !rok {
		return blockedSemantic("side orientation is unknown")
	}
	if left.PayoffID == right.PayoffID {
		if left.PayoffVersion != right.PayoffVersion {
			return blockedSemantic("same payoff has different immutable versions")
		}
		return SemanticRoute{State: "VERIFIED_SEMANTIC_BASIS", Kind: "same-payoff",
			LeftSide: leftSide, RightSide: rightSide, SolverEligible: true}
	}
	relation, ok := relationFor(left, right, relations)
	if !ok {
		return blockedSemantic("no verified payoff relation connects the instruments")
	}
	switch relation.RelationType {
	case "implies", "excludes":
		return SemanticRoute{State: "VERIFIED_SEMANTIC_BASIS", Kind: relation.RelationType,
			LeftSide: leftSide, RightSide: rightSide, SolverEligible: true}
	case "exhaustive_with", "basis":
		if relation.PayoutFloor == nil {
			return blockedSemantic("verified relation has no payout floor")
		}
		return SemanticRoute{State: "VERIFIED_SEMANTIC_BASIS", Kind: relation.RelationType,
			LeftSide: leftSide, RightSide: rightSide, SolverEligible: true}
	case "correlated":
		return blockedSemantic("correlation is not a payoff identity certificate")
	default:
		return blockedSemantic("unsupported payoff relation")
	}
}
