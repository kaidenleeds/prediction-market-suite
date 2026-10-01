package storage

import (
	"sort"
	"strings"
)

// SystemActionClass describes what makes the trading decision. It is deliberately separate from
// route: a composite selector or data guard may feed a named executable child, but is never
// allowed to masquerade as a standalone order source.
type SystemActionClass string

const (
	SystemActionSingle    SystemActionClass = "single"
	SystemActionBundle    SystemActionClass = "bundle"
	SystemActionComposite SystemActionClass = "composite"
	SystemActionData      SystemActionClass = "data"
)

// SystemHandoffClass is the only supported transition from a system decision to execution.
// Unsupported means the code currently has no honest order handoff; it is not a placeholder
// permission. Child means the system may select/veto a named executable child but cannot submit an
// order under its own observation alone.
type SystemHandoffClass string

const (
	SystemHandoffSingle      SystemHandoffClass = "single"
	SystemHandoffBundle      SystemHandoffClass = "bundle"
	SystemHandoffChild       SystemHandoffClass = "child"
	SystemHandoffUnsupported SystemHandoffClass = "unsupported"
)

// SystemExecutionVariant is one exact venue/side/order-route cell. BOTH is intentionally absent:
// a system that can trade both outcomes owns separate YES and NO variants with separate books,
// fees, fills, positions and settlements.
type SystemExecutionVariant struct {
	SystemID string `json:"system_id"`
	Venue    string `json:"venue"`
	Side     string `json:"side"`
	Route    string `json:"route"`
}

// SystemExecutionSpec is the canonical capability contract for the 19 named systems. Venues,
// Sides and Routes contain executable children only (kalshi/polyus, YES/NO, maker/taker/rfq).
// Research-only Polymarket inputs and observer/maker-control rows are recorded as unsupported
// reasons instead of being exposed as order variants.
type SystemExecutionSpec struct {
	SystemID           string             `json:"system_id"`
	ActionClass        SystemActionClass  `json:"action_class"`
	HandoffClass       SystemHandoffClass `json:"handoff_class"`
	Venues             []string           `json:"venues"`
	Sides              []string           `json:"sides"`
	Routes             []string           `json:"routes"`
	TriggerSource      string             `json:"trigger_source"`
	SignalAdapter      string             `json:"signal_adapter"`
	PaperHandoff       string             `json:"paper_handoff"`
	LiveHandoff        string             `json:"live_handoff"`
	SettlementPath     string             `json:"settlement_path"`
	UnsupportedReasons []string           `json:"unsupported_reasons"`
}

func systemSpec(id string, action SystemActionClass, handoff SystemHandoffClass,
	venues, sides, routes []string, trigger, adapter, paper, live, settlement string,
	unsupported ...string) SystemExecutionSpec {
	return SystemExecutionSpec{SystemID: id, ActionClass: action, HandoffClass: handoff,
		Venues: venues, Sides: sides, Routes: routes, TriggerSource: trigger,
		SignalAdapter: adapter, PaperHandoff: paper, LiveHandoff: live,
		SettlementPath: settlement, UnsupportedReasons: unsupported}
}

var canonicalSystemExecutionSpecs = []SystemExecutionSpec{
	systemSpec("attention-spillover-graph", SystemActionComposite, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"same-game parent shock and structurally linked child books",
		"prior-window event-disjoint frozen arm selects a post-freeze exact child-book action",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"no order exists until 7 complete UTC days and 20 independent events per arm freeze a selector"),
	systemSpec("clientele-clock-basis", SystemActionComposite, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"rule-certified K-PUS pair by local hour, phase and liquidity",
		"prior-window event-disjoint frozen venue/side arm selects a post-freeze exact action",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"no order exists until 7 complete UTC days and 20 independent events per arm freeze a selector"),
	systemSpec("collateral-release-rotation", SystemActionComposite, SystemHandoffSingle,
		[]string{"kalshi"}, []string{"YES", "NO"}, []string{"taker"},
		"authoritative collateral credit joined to linked child markets",
		"prior-window event-disjoint frozen arm selects a post-credit exact child-book action",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"no order exists until 7 complete UTC days and 20 independent events per arm freeze a selector",
		"PolyUS lacks an authoritative credited cash amount"),
	systemSpec("deadline-hazard-surface", SystemActionComposite, SystemHandoffSingle,
		[]string{"kalshi"}, []string{"YES", "NO"}, []string{"taker"},
		"verified deadline implication or settlement-compatible NOAA NBM forecast",
		"positive NOAA lower-bound candidate to exact one-share taker observation",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"multi-leg deadline implications require a separate typed bundle handoff"),
	systemSpec("flow-direction-integrity", SystemActionComposite, SystemHandoffSingle,
		[]string{"kalshi"}, []string{"YES", "NO"}, []string{"taker"},
		"authoritative Kalshi public-trade side joined to the prior sequenced book",
		"authoritative bought side selects one fresh exact-book action while the same-clock opposite is retained as a matched control",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"authoritative conflict or missing canonical identity/book truth always abstains"),
	systemSpec("forecast-persona-router", SystemActionComposite, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"frozen prior-window proper-score persona selection",
		"selected persona side to exact one-share taker candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"maker execution is not implemented", "an unfrozen or abstaining persona cannot order"),
	systemSpec("identity-challenged-cross-venue-lock", SystemActionBundle, SystemHandoffUnsupported,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, nil,
		"current cross-venue rule and orientation certificate with simultaneous books",
		"resolution-certificate observer; no typed executable bundle is emitted",
		"unsupported", "unsupported", "legacy cross-venue ledgers only",
		"the named system emits observer controls only", "Polymarket is research-only",
		"cross-venue legs are non-atomic"),
	systemSpec("incentive-subsidized-structural-lock", SystemActionBundle, SystemHandoffUnsupported,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, nil,
		"official active incentive program joined to current exact maker books",
		"multi-leg maker-control and reward-attribution collector; no executable bundle candidate",
		"unsupported", "unsupported", "maker incentive and queue control ledgers",
		"no exact order-attributed earned-credit receipt exists", "unearned advertised rewards have a zero lower bound"),
	systemSpec("maker-salvage-matched-cohort", SystemActionComposite, SystemHandoffChild,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"maker"},
		"same frozen model rejects taker while a post-only maker route remains viable",
		"fresh matched maker overlay stamps a source system's normal post-only order; it never creates a duplicate order",
		"source system's exact maker Paper executor", "accepted source-system Paper route owns the armed LIVE maker mirror",
		"natural maker queue/fill/cancel/fee/settlement ledger",
		"existing historical candidates are retrospective and cannot substitute for a fresh signal"),
	systemSpec("outcome-set-expansion-shock", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"immutable outcome-set membership change with a prior-only survivor projection",
		"positive conservative projection to exact one-share side candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"maker execution is not implemented", "void-uncertified outcome sets remain blocked"),
	systemSpec("paired-bridge-inversion", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"directional bridge signal repriced on direct and independently quoted opposite books",
		"frozen direct/inverse selector to exact one-share taker candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"maker execution is not implemented", "nested inversion is forbidden"),
	systemSpec("payoff-constraint-solver", SystemActionBundle, SystemHandoffBundle,
		[]string{"kalshi"}, []string{"YES"}, []string{"rfq"},
		"verified complete payoff state vector and exact 2-6-leg cost envelope",
		"positive atomic Kalshi conjunction to typed route bundle",
		"authenticated no-money Kalshi RFQ then Combo Paper", "fresh armed Kalshi RFQ acceptance",
		"research route-bundle terminal grades",
		"the venue-created conjunction is bought as YES; constituent NO legs do not create a NO-side RFQ security",
		"PolyUS has no combo execution route", "cross-venue and additive bundles are non-atomic",
		"all-leg taker bundles are not exposed as atomic orders"),
	systemSpec("proper-score-executor", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"book-native calibrated forecast transformed by Brier, log or spherical scoring",
		"positive exact one-share proper-score action candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"proper-score ledger plus research system terminal grades",
		"maker execution is not implemented", "abstentions are not order variants"),
	systemSpec("replenishment-fingerprint", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi"}, []string{"YES", "NO"}, []string{"taker"},
		"persistent sequenced touch depletion attributed to authoritative trades",
		"no-refill horizon to exact one-share taker candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"maker refill execution is not implemented", "public cancels remain an inferred proxy"),
	systemSpec("score-state-surface", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"leave-one-contract-out sports state surface over structural winner/spread/total books",
		"positive conservative residual to exact one-share side candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"joint props and multi-leg correlation routes are not implemented", "maker execution is not implemented"),
	systemSpec("semantic-complexity-premium", SystemActionComposite, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"versioned rules complexity features and frozen complexity-bin policy",
		"prior event-disjoint UTC-day selector emits only its post-freeze selected exact side",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"unfrozen complexity observations remain controls and cannot originate an order", "Polymarket is research-only"),
	systemSpec("series-roll-anchor", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi"}, []string{"YES", "NO"}, []string{"taker"},
		"official series/event mapping plus prior issue settlement and current opening book",
		"prior-settlement anchor side to exact one-share taker candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"maker execution is not implemented", "ticker-prefix inference is forbidden"),
	systemSpec("settlement-latency-carry", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"near-certain current-book signal with prior-frozen void and capital-time reserves",
		"reserve-complete carry input to exact one-share taker candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"patient maker execution is not implemented", "PolyUS credited cash amount can be unavailable"),
	systemSpec("side-normalized-crowding-fade", SystemActionSingle, SystemHandoffSingle,
		[]string{"kalshi", "polyus"}, []string{"YES", "NO"}, []string{"taker"},
		"book-native flow, whale, concentration and imbalance in bought-side coordinates",
		"frozen direct/fade selector to exact one-share taker candidate",
		"generic exact single-order Paper executor", "accepted-Paper armed LIVE mirror",
		"research system terminal grades",
		"maker execution is not implemented"),
}

func cloneSystemExecutionSpec(in SystemExecutionSpec) SystemExecutionSpec {
	out := in
	out.Venues = append([]string(nil), in.Venues...)
	out.Sides = append([]string(nil), in.Sides...)
	out.Routes = append([]string(nil), in.Routes...)
	out.UnsupportedReasons = append([]string(nil), in.UnsupportedReasons...)
	return out
}

// SystemExecutionSpecs returns a stable defensive copy of every canonical system capability.
func SystemExecutionSpecs() []SystemExecutionSpec {
	out := make([]SystemExecutionSpec, len(canonicalSystemExecutionSpecs))
	for i := range canonicalSystemExecutionSpecs {
		out[i] = cloneSystemExecutionSpec(canonicalSystemExecutionSpecs[i])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SystemID < out[j].SystemID })
	return out
}

// SystemIDs is the canonical public system registry. Internal controls and experiment arms are
// deliberately absent.
func SystemIDs() []string {
	specs := SystemExecutionSpecs()
	out := make([]string, len(specs))
	for i := range specs {
		out[i] = specs[i].SystemID
	}
	return out
}

// SystemExecutionCapability returns one defensive-copy capability by exact system ID.
func SystemExecutionCapability(systemID string) (SystemExecutionSpec, bool) {
	systemID = strings.TrimSpace(systemID)
	for _, spec := range canonicalSystemExecutionSpecs {
		if spec.SystemID == systemID {
			return cloneSystemExecutionSpec(spec), true
		}
	}
	return SystemExecutionSpec{}, false
}

func containsSystemCapability(rows []string, value string, upper bool) bool {
	value = strings.TrimSpace(value)
	if upper {
		value = strings.ToUpper(value)
	} else {
		value = strings.ToLower(value)
	}
	for _, row := range rows {
		candidate := strings.TrimSpace(row)
		if upper {
			candidate = strings.ToUpper(candidate)
		} else {
			candidate = strings.ToLower(candidate)
		}
		if candidate == value {
			return true
		}
	}
	return false
}

func (s SystemExecutionSpec) SupportsVenue(venue string) bool {
	return containsSystemCapability(s.Venues, venue, false)
}

func (s SystemExecutionSpec) SupportsSide(side string) bool {
	return containsSystemCapability(s.Sides, side, true)
}

func (s SystemExecutionSpec) SupportsRoute(route string) bool {
	return containsSystemCapability(s.Routes, route, false)
}

// SingleOrderEligible is true only for a system that owns the generic exact single-order handoff.
// Child guards and unsupported systems cannot use it even when their named child supports the same
// venue/side/route cell.
func (s SystemExecutionSpec) SingleOrderEligible(venue, side, route string) bool {
	return s.HandoffClass == SystemHandoffSingle &&
		(s.ActionClass == SystemActionSingle || s.ActionClass == SystemActionComposite) &&
		s.SupportsVenue(venue) && s.SupportsSide(side) && s.SupportsRoute(route)
}

// ExactVariants returns deterministic executable cells. Unsupported systems return no variants;
// child handoffs return the child cells for inspection but remain ineligible for direct orders.
func (s SystemExecutionSpec) ExactVariants() []SystemExecutionVariant {
	if s.HandoffClass == SystemHandoffUnsupported {
		return nil
	}
	out := make([]SystemExecutionVariant, 0, len(s.Venues)*len(s.Sides)*len(s.Routes))
	for _, venue := range s.Venues {
		for _, side := range s.Sides {
			for _, route := range s.Routes {
				out = append(out, SystemExecutionVariant{SystemID: s.SystemID,
					Venue: venue, Side: side, Route: route})
			}
		}
	}
	return out
}
