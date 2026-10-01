package server

import "strings"

// r147BundleProductContract separates real multi-leg payoff policies from the ledgers and
// controls that describe them.  None of these rows may inherit a single-leg or Kalshi-MVE route
// merely because every constituent leg can be named: a Kalshi MVE YES contract is a conjunction,
// while the lock products below own additive payoffs, and a cross-venue lock has no atomic venue.
type r147BundleProductContract struct {
	SystemID          string
	Kind              string
	DecisionSystem    bool
	TriggerSource     string
	InputVenues       []string
	ExecutionVenues   []string
	ConsumedBy        string
	PaperPath         string
	LivePath          string
	Blocker           string
	RequiredPrimitive string
	Variants          []r147BundleProductVariant
}

type r147BundleProductVariant struct {
	VariantID string   `json:"variant_id"`
	Venues    []string `json:"venues"`
	Legs      []string `json:"legs"`
	Route     string   `json:"route"`
	Atomic    bool     `json:"atomic"`
	State     string   `json:"state"`
}

func r147BlockedProductVariant(id, route string, venues, legs []string) r147BundleProductVariant {
	return r147BundleProductVariant{VariantID: id, Venues: venues, Legs: legs, Route: route,
		Atomic: false, State: "BLOCKED_EXTERNAL_PRODUCT"}
}

func r147WiredStagedProductVariant(id, route string, venues, legs []string) r147BundleProductVariant {
	return r147BundleProductVariant{VariantID: id, Venues: venues, Legs: legs, Route: route,
		Atomic: false, State: "WIRED_DISABLED_EXACT_PROOF"}
}

var r147BundleProductContracts = map[string]r147BundleProductContract{
	"event-basket-lock": {
		SystemID: "event-basket-lock", Kind: "additive_lock_product", DecisionSystem: true,
		TriggerSource: "complete current official event membership plus side-specific exact books, depth and fees",
		InputVenues:   []string{"kalshi", "polyus"}, ExecutionVenues: []string{"kalshi", "polyus"},
		ConsumedBy: "payoff-constraint-solver",
		PaperPath:  "counterfactual all-leg basket grade plus exact named typed-bundle candidates",
		LivePath:   "Kalshi staged_fok_v1 broker/recovery is wired behind a separate OFF-by-default exact allowlist; PolyUS remains fail-closed",
		Blocker: "a complete-set lock pays the sum of its filled legs; Kalshi MVE/RFQ pays only an all-win conjunction, " +
			"PolyUS has no atomic basket order, and sequential singles would expose an unhedged partial fill",
		RequiredPrimitive: "verified void/payoff certificate plus exact sealed untouched proof; PolyUS additionally needs idempotent intent-only recovery",
		Variants: []r147BundleProductVariant{
			r147WiredStagedProductVariant("kalshi-buy-yes-all", "additive-all-leg-taker", []string{"kalshi"}, []string{"YES(each mutually-exclusive and exhaustive outcome)"}),
			r147WiredStagedProductVariant("kalshi-buy-no-all", "additive-all-leg-taker", []string{"kalshi"}, []string{"NO(each mutually-exclusive outcome)"}),
			r147BlockedProductVariant("polyus-buy-yes-all", "additive-all-leg-taker", []string{"polyus"}, []string{"YES(each mutually-exclusive and exhaustive outcome)"}),
			r147BlockedProductVariant("polyus-buy-no-all", "additive-all-leg-taker", []string{"polyus"}, []string{"NO(each mutually-exclusive outcome)"}),
		},
	},
	"nested-ladder-lock": {
		SystemID: "nested-ladder-lock", Kind: "additive_two_leg_lock", DecisionSystem: true,
		TriggerSource: "official Kalshi same-event greater-than ladder snapshot plus current exact books, depth and fees",
		InputVenues:   []string{"kalshi"}, ExecutionVenues: []string{"kalshi"},
		ConsumedBy: "payoff-constraint-solver and deadline-hazard-surface",
		PaperPath:  "counterfactual two-leg exact-book grade plus exact named typed-bundle candidates",
		LivePath:   "Kalshi staged_fok_v1 broker/recovery is wired behind a separate OFF-by-default exact allowlist",
		Blocker: "YES(low strike)+NO(high strike) has a one-dollar floor and sometimes pays two dollars; " +
			"a Kalshi MVE conjunction has a zero-or-one-dollar payoff and is not the same security, while separate orders are non-atomic",
		RequiredPrimitive: "verified void/payoff certificate plus exact sealed untouched proof",
		Variants: []r147BundleProductVariant{
			r147WiredStagedProductVariant("kalshi-low-yes-high-no", "additive-two-leg-taker", []string{"kalshi"}, []string{"YES(low strike)", "NO(high strike)"}),
		},
	},
	"time-nested-lock": {
		SystemID: "time-nested-lock", Kind: "additive_two_leg_lock", DecisionSystem: true,
		TriggerSource: "immutable verified ordered-deadline relation plus current Kalshi exact books, depth and fees",
		InputVenues:   []string{"kalshi"}, ExecutionVenues: []string{"kalshi"},
		ConsumedBy: "native time-nested collector",
		PaperPath:  "counterfactual two-leg exact-book grade plus exact named typed-bundle candidates",
		LivePath:   "Kalshi staged_fok_v1 broker/recovery is wired behind a separate OFF-by-default exact allowlist",
		Blocker: "NO(earlier deadline)+YES(later deadline) is an additive floor position, not an all-win conjunction; " +
			"Kalshi exposes no atomic order for that payoff and uncertified void treatment can remove the floor",
		RequiredPrimitive: "certified void-equivalent payoff plus exact sealed untouched proof",
		Variants: []r147BundleProductVariant{
			r147WiredStagedProductVariant("kalshi-early-no-late-yes", "additive-two-leg-taker", []string{"kalshi"}, []string{"NO(earlier deadline)", "YES(later deadline)"}),
		},
	},
	"xvlock": {
		SystemID: "xvlock", Kind: "cross_venue_two_leg_lock", DecisionSystem: true,
		TriggerSource: "payoff-certified K-PUS identity pair plus simultaneous current side books, depth and exact fees",
		InputVenues:   []string{"kalshi", "polyus", "polymarket"}, ExecutionVenues: []string{"kalshi", "polyus"},
		ConsumedBy: "xvlock scanner",
		PaperPath:  "lockstack records the depth- and fee-checked K-PUS stake; verified pairs emit exact named typed bundles",
		LivePath:   "staged_fok_v1 engine exists, but every xvlock contains PolyUS and is hard-blocked before intent",
		Blocker: "the two offsetting legs live on different venues, so no venue can fill them atomically; " +
			"the existing auto-arb two-leg path is Paper-only and Polymarket Global remains research-only",
		RequiredPrimitive: "PolyUS idempotent client-order identity plus authoritative intent-only recovery",
		Variants: []r147BundleProductVariant{
			r147BlockedProductVariant("k-yes-pus-no", "cross-venue-two-leg-taker", []string{"kalshi", "polyus"}, []string{"Kalshi YES", "PolyUS NO"}),
			r147BlockedProductVariant("k-no-pus-yes", "cross-venue-two-leg-taker", []string{"kalshi", "polyus"}, []string{"Kalshi NO", "PolyUS YES"}),
			r147BlockedProductVariant("k-yes-pus-yes-flipped-identity", "cross-venue-two-leg-taker", []string{"kalshi", "polyus"}, []string{"Kalshi YES", "PolyUS YES (certificate says opposite outcome)"}),
			r147BlockedProductVariant("k-no-pus-no-flipped-identity", "cross-venue-two-leg-taker", []string{"kalshi", "polyus"}, []string{"Kalshi NO", "PolyUS NO (certificate says opposite outcome)"}),
		},
	},
	"identity-challenged-cross-venue-lock": {
		SystemID: "identity-challenged-cross-venue-lock", Kind: "identity_and_route_control",
		TriggerSource: "cross-venue rule artifacts, canonical identity and resolution certificates",
		InputVenues:   []string{"kalshi", "polyus", "polymarket"}, ExecutionVenues: []string{"kalshi", "polyus"},
		ConsumedBy: "xvlock and payoff-constraint-solver",
		PaperPath:  "observer/certificate rows only", LivePath: "child route only",
		Blocker: "this name owns resolution certificates and accepted/rejected matcher evidence; it emits no independent order. " +
			"The certified child must execute as xvlock or payoff-constraint-solver without duplicating the trade",
		RequiredPrimitive: "none; this control must remain attached to its executable child",
	},
	"incentive-subsidized-structural-lock": {
		SystemID: "incentive-subsidized-structural-lock", Kind: "reward_attribution_control",
		TriggerSource: "official incentive program plus matched queue, fill and earned-credit receipts",
		InputVenues:   []string{"kalshi", "polyus"}, ExecutionVenues: []string{"kalshi", "polyus"},
		ConsumedBy: "a separately named structural-lock child",
		PaperPath:  "matched incentive and queue controls only", LivePath: "child route only",
		Blocker: "advertised rewards are not guaranteed proceeds; until an order-attributed earned-credit lower bound exists, " +
			"this overlay can rank or veto a child but cannot create a second order",
		RequiredPrimitive: "order-attributed earned-credit lower bound joined to the existing child route",
	},
	"joint-marginal-lock": {
		SystemID: "joint-marginal-lock", Kind: "joint_quote_comparator_control",
		TriggerSource: "certified basket plus current marginal books; exact joint quote is absent",
		InputVenues:   []string{"kalshi", "polyus"}, ExecutionVenues: []string{"kalshi"},
		ConsumedBy: "payoff-constraint-solver",
		PaperPath:  "current marginal-cost control only", LivePath: "child route only",
		Blocker: "the collector compares a certified basket with marginal books but intentionally has no current joint quote; " +
			"only a separately typed, payoff-equivalent Kalshi conjunction may enter the existing RFQ handoff",
		RequiredPrimitive: "a current payoff-equivalent joint quote; when it is a Kalshi conjunction, payoff-constraint-solver already owns the RFQ route",
	},
	"lockstack": {
		SystemID: "lockstack", Kind: "paper_execution_ledger",
		TriggerSource: "xvlock first-sight candidate after exact aggregate fee, depth and portfolio checks",
		InputVenues:   []string{"kalshi", "polyus"}, ExecutionVenues: []string{"kalshi", "polyus"},
		ConsumedBy: "xvlock",
		PaperPath:  "depth- and fee-checked xvlock Paper stake", LivePath: "xvlock route only",
		Blocker: "lockstack is the funded Paper ledger for xvlock, not another signal or payoff policy; " +
			"counting it as a standalone LIVE system would duplicate the same two legs",
		RequiredPrimitive: "none; execute the xvlock child exactly once and attribute its Paper/LIVE receipts to this ledger",
	},
}

func r147BundleProductContractFor(systemID string) (r147BundleProductContract, bool) {
	row, ok := r147BundleProductContracts[strings.ToLower(strings.TrimSpace(systemID))]
	return row, ok
}

func r147BundleProductVariantCounts() (systems, declared, connected, blocked int) {
	for _, product := range r147BundleProductContracts {
		if !product.DecisionSystem || len(product.Variants) == 0 {
			continue
		}
		systems++
		for _, variant := range product.Variants {
			declared++
			switch variant.State {
			case "WIRED_DISABLED_EXACT_PROOF":
				connected++
			case "BLOCKED_EXTERNAL_PRODUCT":
				blocked++
			}
		}
	}
	return
}

func r147BundleProductHasConnectedVariant(systemID string) bool {
	product, ok := r147BundleProductContractFor(systemID)
	if !ok || !product.DecisionSystem {
		return false
	}
	for _, variant := range product.Variants {
		if variant.State == "WIRED_DISABLED_EXACT_PROOF" {
			return true
		}
	}
	return false
}

func r147BundleCatalogClassification(systemID string) (r145CatalogExecutionRole, bool, string, bool) {
	row, ok := r147BundleProductContractFor(systemID)
	if !ok {
		return "", false, "", false
	}
	reason := row.Kind + ": " + row.Blocker
	switch row.Kind {
	case "identity_and_route_control", "reward_attribution_control", "joint_quote_comparator_control":
		return r145ExecutionDataControl, false, reason, true
	case "paper_execution_ledger":
		return r145ExecutionPortfolioEvidence, false, reason, true
	default:
		if row.SystemID == "event-basket-lock" || row.SystemID == "nested-ladder-lock" || row.SystemID == "time-nested-lock" {
			return r145ExecutionOrderSystem, true, reason, true
		}
		return r145ExecutionUnsupported, false, reason, true
	}
}
