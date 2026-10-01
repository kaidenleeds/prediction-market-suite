package server

// R145 canonical system-count catalog.
//
// This file deliberately does not read SQLite or any runtime cache. A system count must not change
// merely because a verdict row warmed up, expired, or settled. It records three different things:
//
//   - a base system: one independently named decision/research policy;
//   - an alias: another ledger, route, or display name for an existing base system;
//   - a component: a diagnostic, control, method sub-ledger, or Combo cohort that is not a system.
//
// Known variants are a conservative lower bound. They include every exact venue/side/route cell
// already materialized by the native roster, every declared R138 capability cell, and the two
// dedicated book systems whose execution shapes are explicit in code. Bases without a typed
// declaration remain visible in MissingVariantSystems; no cartesian product is fabricated for them.

import (
	"sort"
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type r145CatalogRole string

const (
	r145CatalogBase      r145CatalogRole = "base_system"
	r145CatalogAlias     r145CatalogRole = "alias"
	r145CatalogComponent r145CatalogRole = "component"
)

// r145CatalogExecutionRole says whether a catalog name can originate money, or is instead an
// input/control/cohort/product shape that must never be made to look executable by inventing a
// venue/side/route cartesian product.  This is static code capability, not runtime freshness,
// economic merit, Paper allocation, or LIVE authority.
type r145CatalogExecutionRole string

const (
	r145ExecutionOrderSystem       r145CatalogExecutionRole = "order_originating_system"
	r145ExecutionChildOverlay      r145CatalogExecutionRole = "executable_child_overlay"
	r145ExecutionResearchVenue     r145CatalogExecutionRole = "research_only_venue_system"
	r145ExecutionDataControl       r145CatalogExecutionRole = "data_or_routing_control"
	r145ExecutionResearchCohort    r145CatalogExecutionRole = "research_cohort"
	r145ExecutionUnsupported       r145CatalogExecutionRole = "unsupported_product_shape"
	r145ExecutionFrozen            r145CatalogExecutionRole = "frozen_order_system"
	r145ExecutionPortfolioEvidence r145CatalogExecutionRole = "portfolio_or_evidence_ledger"
)

type r145SystemCatalogEntry struct {
	ID                 string                     `json:"id"`
	Role               r145CatalogRole            `json:"role"`
	Class              string                     `json:"class"`
	ExecutionRole      r145CatalogExecutionRole   `json:"execution_role,omitempty"`
	RequiresOrderRoute bool                       `json:"requires_order_route,omitempty"`
	Blocked            bool                       `json:"blocked,omitempty"`
	BaseID             string                     `json:"base_id,omitempty"`
	Venue              string                     `json:"venue,omitempty"`
	Side               string                     `json:"side,omitempty"`
	Route              string                     `json:"route,omitempty"`
	Rationale          string                     `json:"rationale,omitempty"`
	TriggerSource      string                     `json:"trigger_source,omitempty"`
	InputVenues        []string                   `json:"input_venues,omitempty"`
	ExecutionVenues    []string                   `json:"execution_venues,omitempty"`
	ConsumedBy         string                     `json:"consumed_by,omitempty"`
	PaperPath          string                     `json:"paper_path,omitempty"`
	LivePath           string                     `json:"live_path,omitempty"`
	RequiredPrimitive  string                     `json:"required_primitive,omitempty"`
	ProductVariants    []r147BundleProductVariant `json:"product_variants,omitempty"`
}

type r145SystemCatalogVariant struct {
	SystemID string `json:"system_id"`
	Venue    string `json:"venue"`
	Side     string `json:"side"`
	Route    string `json:"route"`
	Handoff  string `json:"handoff"`
}

type r145SystemCatalogCounts struct {
	BaseSystems                      int      `json:"base_systems"`
	DeclaredTypedVariants            int      `json:"declared_typed_variants"`
	CodePathConnectedVariants        int      `json:"code_path_connected_variants"`
	DeclaredWithoutCodePathVariants  int      `json:"declared_without_code_path_variants"`
	ProducerCapableTypedVariants     int      `json:"producer_capable_typed_variants"`
	ProducerBlockedTypedVariants     int      `json:"producer_blocked_typed_variants"`
	CrossVenueInputVariants          int      `json:"cross_venue_input_variants"`
	SignalContractVariants           int      `json:"signal_contract_variants"`
	ExecutionDerivativeContracts     int      `json:"execution_derivative_contracts"`
	KnownExactExecutionVariants      int      `json:"known_exact_execution_variants"` // Compatibility alias for CodePathConnectedVariants.
	BundleProductSystems             int      `json:"bundle_product_systems"`
	DeclaredBundleProductVariants    int      `json:"declared_bundle_product_variants"`
	ConnectedStagedBundleVariants    int      `json:"connected_staged_bundle_variants"`
	ExternallyBlockedBundleVariants  int      `json:"externally_blocked_bundle_variants"`
	MissingTypedVariantSystems       int      `json:"missing_typed_variant_systems"`
	MissingVariantSystemIDs          []string `json:"missing_variant_system_ids"`
	OrderOriginatingBaseSystems      int      `json:"order_originating_base_systems"`
	ChildOverlayBaseSystems          int      `json:"child_overlay_base_systems"`
	ClassifiedNonOrderBaseSystems    int      `json:"classified_non_order_base_systems"`
	ResearchVenueBaseSystems         int      `json:"research_only_venue_base_systems"`
	DataControlBaseSystems           int      `json:"data_or_routing_control_base_systems"`
	ResearchCohortBaseSystems        int      `json:"research_cohort_base_systems"`
	UnsupportedProductBaseSystems    int      `json:"unsupported_product_base_systems"`
	FrozenBaseSystems                int      `json:"frozen_order_systems"`
	PortfolioEvidenceBaseSystems     int      `json:"portfolio_or_evidence_ledger_base_systems"`
	MissingExecutableVariantSystems  int      `json:"missing_executable_variant_systems"`
	MissingExecutableSystemIDs       []string `json:"missing_executable_system_ids"`
	ExternallyBlockedDecisionSystems int      `json:"externally_blocked_decision_systems"`
	ExternallyBlockedDecisionIDs     []string `json:"externally_blocked_decision_system_ids"`
	Aliases                          int      `json:"aliases"`
	Components                       int      `json:"components"`
	VariantCountContract             string   `json:"variant_count_contract"`
}

var r145PolicyBaseSystems = []string{
	"arb", "basket", "bookskew", "confluence", "cross", "divergence", "fade", "favlong",
	"fbridge", "freshfade", "freshlist", "fundtilt", "independent-probabilistic-weather",
	"insider", "kalshi-flow", "kalshi-whale", "kcrypto", "kflow", "kthresh", "meanrev",
	"notail", "pbridge", "pcrypto", "pfbridge", "pflow", "pmatch", "poly-consensus",
	"poly-pred-kalshi", "poly-whale", "polyus-consensus", "polyus-flow", "polyus-whale",
	"sharpline", "skillbuy", "spotlag", "whale-exit", "whale-exit-hold-bridge", "wxedge",
	"xinv-pcrypto", "xmatch", "xvgap", "xvgap2", "xvgapk", "xvlag",
}

var r145NativeBaseSystems = []string{
	"combo-overlay", "combo-rfq", "combo-synth", "kflow-live", "kflow-pre", "lockstack",
	"ml-book", "rawflow", "weather", "xvlock",
}

var r145AdditionalResearchBaseSystems = []string{
	"event-basket-lock", "fee-rounding-batch", "incentive-maker", "joint-marginal-lock",
	"lifecycle-reopen", "nested-ladder-lock", "queue-priority", "subcent-golf", "time-nested-lock",
}

// r145IndependentInverseBaseSystems is derived from the same direct K/PUS taker cells that can
// reach insertSignal. The central signal choke creates `invert:<family>` on the actual opposite
// book for every such detector, so a hand-maintained shortlist necessarily loses real systems.
func r145IndependentInverseBaseSystems() []string {
	seen := map[string]bool{}
	for _, cell := range r145NativeKnownVariantCells {
		parts := strings.Split(cell, "|")
		if len(parts) != 4 || strings.HasPrefix(parts[0], "invert:") || parts[3] != "taker" ||
			!r147NativeSignalProducerCapable(parts[0], parts[1], parts[2]) {
			continue
		}
		seen["invert:"+parts[0]] = true
	}
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// These are operator-requested systems even though their current implementations still lack a
// complete order handoff. They count as bases and remain explicitly missing from the variant map.
var r145BlockedBaseSystems = []string{
	"adaptive-whale-scorer", "behavioral-bias-regime",
}

var r145DedicatedBookBaseSystems = []string{"cheapband", "favlong80"}

var r145ResearchVenueSystemSet = map[string]bool{
	"basket": true, "divergence": true, "fade": true, "insider": true,
	"pcrypto": true, "pflow": true, "poly-consensus": true, "poly-whale": true,
	"skillbuy": true, "whale-exit": true,
}

var r145DataControlSystemSet = map[string]bool{
	"adaptive-whale-scorer": true, "behavioral-bias-regime": true,
	"fee-rounding-batch": true, "incentive-maker": true,
	"lifecycle-reopen": true, "queue-priority": true,
}

var r145ResearchCohortSystemSet = map[string]bool{
	"independent-probabilistic-weather": true, "subcent-golf": true,
}

var r145PortfolioEvidenceSystemSet = map[string]bool{
	"combo-overlay": true, "combo-rfq": true, "combo-synth": true,
	"kflow-live": true, "kflow-pre": true,
}

func r145CatalogListContains(rows []string, id string) bool {
	for _, row := range rows {
		if strings.EqualFold(strings.TrimSpace(row), id) {
			return true
		}
	}
	return false
}

// r145CatalogExecutionClassification is exhaustive over base-system names. Policy families,
// independently named inverses, dedicated books, and registered System specs opt in explicitly;
// every other name fails closed. This prevents a control, Poly-int signal, cohort, frozen strategy,
// or unsupported product from being counted as an unimplemented single-order system.
func r145CatalogExecutionClassification(id string) (r145CatalogExecutionRole, bool, string) {
	id = strings.ToLower(strings.TrimSpace(id))
	// Multi-leg names need payoff-shape-specific truth before the generic 19-system registry is
	// consulted. Some are actual but externally unsupported additive products; others are controls
	// or ledgers consumed by an executable child and must not masquerade as duplicate systems.
	if role, requires, reason, ok := r147BundleCatalogClassification(id); ok {
		return role, requires, reason
	}
	if spec, ok := storage.SystemExecutionCapability(id); ok {
		switch spec.HandoffClass {
		case storage.SystemHandoffUnsupported:
			return r145ExecutionUnsupported, false, strings.Join(spec.UnsupportedReasons, "; ")
		case storage.SystemHandoffChild:
			return r145ExecutionChildOverlay, false,
				"stamps or selects a separately named executable child; it never originates a duplicate order"
		default:
			return r145ExecutionOrderSystem, true,
				"owns an explicit exact single-order or typed bundle adapter"
		}
	}
	switch {
	case r145ResearchVenueSystemSet[id]:
		return r145ExecutionResearchVenue, false,
			"signal is sourced from Polymarket Global, which is research-only and has no K/PUS money route"
	case r145DataControlSystemSet[id]:
		return r145ExecutionDataControl, false,
			"ranks, validates, or optimizes another system and cannot originate an order"
	case r145ResearchCohortSystemSet[id]:
		return r145ExecutionResearchCohort, false,
			"prospective research cohort has no Paper/LIVE order authority"
	case r145PortfolioEvidenceSystemSet[id]:
		return r145ExecutionPortfolioEvidence, false,
			"portfolio/result cohort or evidence ledger; the originating decision system owns execution"
	case id == "weather":
		return r145ExecutionFrozen, false,
			"legacy point-forecast order system is hard-frozen; wxedge continues as the separately named book-native signal"
	case id == "ml-book" || id == "rawflow" ||
		r145CatalogListContains(r145PolicyBaseSystems, id) ||
		r145CatalogListContains(r145IndependentInverseBaseSystems(), id) ||
		r145CatalogListContains(r145DedicatedBookBaseSystems, id):
		return r145ExecutionOrderSystem, true,
			"owns an explicit native, generic-follower, or dedicated-book execution route"
	default:
		return r145ExecutionUnsupported, false,
			"catalog base has no registered execution classification; fail closed until explicitly classified"
	}
}

func r145CanonicalBaseSystems() []r145SystemCatalogEntry {
	byID := make(map[string]r145SystemCatalogEntry, 128)
	add := func(ids []string, class string, blocked bool) {
		for _, id := range ids {
			id = strings.ToLower(strings.TrimSpace(id))
			if id == "" {
				continue
			}
			byID[id] = r145SystemCatalogEntry{ID: id, Role: r145CatalogBase, Class: class, Blocked: blocked}
		}
	}
	add(r145PolicyBaseSystems, "policy_family", false)
	add(r145NativeBaseSystems, "native_dedicated", false)
	add(r145AdditionalResearchBaseSystems, "research_system", false)
	add(storage.SystemIDs(), "r138_system", false)
	add(r145IndependentInverseBaseSystems(), "independent_inverse", false)
	add(r145DedicatedBookBaseSystems, "dedicated_book", false)
	add(r145BlockedBaseSystems, "operator_system_missing_handoff", true)

	out := make([]r145SystemCatalogEntry, 0, len(byID))
	for _, row := range byID {
		row.ExecutionRole, row.RequiresOrderRoute, row.Rationale = r145CatalogExecutionClassification(row.ID)
		if product, ok := r147BundleProductContractFor(row.ID); ok {
			row.TriggerSource, row.ConsumedBy = product.TriggerSource, product.ConsumedBy
			row.InputVenues = append([]string(nil), product.InputVenues...)
			row.ExecutionVenues = append([]string(nil), product.ExecutionVenues...)
			row.PaperPath, row.LivePath = product.PaperPath, product.LivePath
			row.RequiredPrimitive = product.RequiredPrimitive
			row.ProductVariants = append([]r147BundleProductVariant(nil), product.Variants...)
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Explicit aliases are intentionally narrow. Unknown book:* names fail closed instead of being
// silently stripped into a possibly unrelated system.
func r145CanonicalSystemAliases() []r145SystemCatalogEntry {
	rows := []r145SystemCatalogEntry{
		{ID: "book:freshinv", Role: r145CatalogAlias, Class: "book_ledger", BaseID: "invert:freshlist"},
		{ID: "book:freshinv-k", Role: r145CatalogAlias, Class: "book_ledger", BaseID: "invert:freshlist", Venue: "kalshi"},
		{ID: "book:freshlist-p", Role: r145CatalogAlias, Class: "book_ledger", BaseID: "freshlist", Venue: "polyus"},
		{ID: "book:xvgap", Role: r145CatalogAlias, Class: "book_ledger", BaseID: "xvgap", Venue: "polyus"},
		{ID: "book:favlong80", Role: r145CatalogAlias, Class: "book_ledger", BaseID: "favlong80", Venue: "kalshi"},
		{ID: "book:cheapband-k", Role: r145CatalogAlias, Class: "book_ledger", BaseID: "cheapband", Venue: "kalshi"},
		{ID: "book:cheapband-p", Role: r145CatalogAlias, Class: "book_ledger", BaseID: "cheapband", Venue: "polyus"},
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	return rows
}

func r145CanonicalSystemComponents() []r145SystemCatalogEntry {
	ids := []string{
		"auto-ml", "maker-fills", "manual", "parlay-2leg", "parlay-3leg", "parlay-4leg",
		"parlay-5leg", "parlay-6leg", "proper-score-brier", "proper-score-log",
		"proper-score-spherical", "raw-flow-observer", "rfq-sim", "systems-regimes",
		"weather-curve-residual", "weather-curve-revision",
	}
	out := make([]r145SystemCatalogEntry, 0, len(ids))
	for _, id := range ids {
		class := "diagnostic_or_control"
		switch {
		case strings.HasPrefix(id, "parlay-"):
			class = "combo_result_cohort"
		case strings.HasPrefix(id, "proper-score-"):
			class = "proper_score_method"
		case id == "auto-ml" || id == "manual":
			class = "archived_source"
		}
		out = append(out, r145SystemCatalogEntry{ID: id, Role: r145CatalogComponent, Class: class})
	}
	return out
}

func r145CatalogBaseSet() map[string]struct{} {
	out := make(map[string]struct{}, 128)
	for _, row := range r145CanonicalBaseSystems() {
		out[row.ID] = struct{}{}
	}
	return out
}

func r145CatalogAliasMap() map[string]r145SystemCatalogEntry {
	out := make(map[string]r145SystemCatalogEntry)
	for _, row := range r145CanonicalSystemAliases() {
		out[row.ID] = row
	}
	return out
}

func r145CatalogIsComponent(id string) bool {
	id = strings.ToLower(strings.TrimSpace(id))
	if strings.HasPrefix(id, "counterfactual:") || strings.HasPrefix(id, "side-control:") ||
		strings.HasPrefix(id, "control:") || strings.HasPrefix(id, "maker-control") ||
		strings.HasPrefix(id, "invert:invert:") {
		return true
	}
	for _, row := range r145CanonicalSystemComponents() {
		if row.ID == id {
			return true
		}
	}
	return false
}

func r145CatalogMergeIdentityField(current, embedded string, valid func(string) bool) (string, bool) {
	current, embedded = strings.TrimSpace(current), strings.TrimSpace(embedded)
	if embedded == "" {
		return current, true
	}
	if !valid(embedded) {
		return current, false
	}
	if current != "" && valid(current) && current != embedded {
		return current, false
	}
	return embedded, true
}

// r145CanonicalSystemIdentity normalizes an alias into one base plus any exact identity embedded
// by the alias. Conflicting embedded and row fields fail closed rather than creating a false cell.
func r145CanonicalSystemIdentity(name, venue, side, route string) (r145SystemCatalogVariant, bool) {
	family := strings.ToLower(strings.TrimSpace(name))
	venue = strings.ToLower(strings.TrimSpace(venue))
	side = strings.ToUpper(strings.TrimSpace(side))
	route = strings.ToLower(strings.TrimSpace(route))
	if family == "" {
		return r145SystemCatalogVariant{}, false
	}
	validVenue := func(v string) bool { return v == "kalshi" || v == "polyus" || v == "polymarket" }
	validSide := func(v string) bool { return v == "YES" || v == "NO" || v == "BOTH" }
	validRoute := func(v string) bool { return v == "maker" || v == "taker" || v == "rfq" }

	if strings.HasPrefix(family, "gf:") {
		gf := strings.TrimPrefix(family, "gf:")
		embeddedVenue := ""
		switch {
		case strings.HasSuffix(gf, "-k"):
			gf, embeddedVenue = strings.TrimSuffix(gf, "-k"), "kalshi"
		case strings.HasSuffix(gf, "-p"):
			gf, embeddedVenue = strings.TrimSuffix(gf, "-p"), "polyus"
		}
		parts := strings.Split(gf, ":")
		if len(parts) >= 3 {
			embeddedSide := strings.ToUpper(parts[len(parts)-2])
			embeddedRoute := strings.ToLower(parts[len(parts)-1])
			if validSide(embeddedSide) && validRoute(embeddedRoute) {
				family = strings.Join(parts[:len(parts)-2], ":")
				var ok bool
				side, ok = r145CatalogMergeIdentityField(side, embeddedSide, validSide)
				if !ok {
					return r145SystemCatalogVariant{}, false
				}
				route, ok = r145CatalogMergeIdentityField(route, embeddedRoute, validRoute)
				if !ok {
					return r145SystemCatalogVariant{}, false
				}
			} else {
				family = gf
			}
		} else {
			family = gf
		}
		var ok bool
		venue, ok = r145CatalogMergeIdentityField(venue, embeddedVenue, validVenue)
		if !ok {
			return r145SystemCatalogVariant{}, false
		}
	} else {
		for _, prefix := range []string{"taker:", "maker:", "unit:", "strategy:"} {
			if !strings.HasPrefix(family, prefix) {
				continue
			}
			family = strings.TrimPrefix(family, prefix)
			embeddedRoute := ""
			if prefix == "taker:" {
				embeddedRoute = "taker"
			} else if prefix == "maker:" {
				embeddedRoute = "maker"
			}
			var ok bool
			route, ok = r145CatalogMergeIdentityField(route, embeddedRoute, validRoute)
			if !ok {
				return r145SystemCatalogVariant{}, false
			}
			break
		}
	}

	if i := strings.LastIndex(family, " ["); i > 0 && strings.HasSuffix(family, "]") {
		embeddedSide := strings.ToUpper(strings.TrimSuffix(family[i+2:], "]"))
		var ok bool
		side, ok = r145CatalogMergeIdentityField(side, embeddedSide, validSide)
		if !ok {
			return r145SystemCatalogVariant{}, false
		}
		family = family[:i]
	}
	if i := strings.LastIndex(family, "@"); i > 0 && i+1 < len(family) {
		embeddedVenue := strings.ToLower(strings.TrimSpace(family[i+1:]))
		var ok bool
		venue, ok = r145CatalogMergeIdentityField(venue, embeddedVenue, validVenue)
		if !ok {
			return r145SystemCatalogVariant{}, false
		}
		family = family[:i]
	}
	family = strings.ToLower(strings.TrimSpace(family))
	if r145CatalogIsComponent(family) {
		return r145SystemCatalogVariant{}, false
	}
	if alias, ok := r145CatalogAliasMap()[family]; ok {
		family = alias.BaseID
		var merged bool
		venue, merged = r145CatalogMergeIdentityField(venue, alias.Venue, validVenue)
		if !merged {
			return r145SystemCatalogVariant{}, false
		}
		side, merged = r145CatalogMergeIdentityField(side, alias.Side, validSide)
		if !merged {
			return r145SystemCatalogVariant{}, false
		}
		route, merged = r145CatalogMergeIdentityField(route, alias.Route, validRoute)
		if !merged {
			return r145SystemCatalogVariant{}, false
		}
	} else if strings.HasPrefix(family, "book:") {
		return r145SystemCatalogVariant{}, false
	}
	if _, ok := r145CatalogBaseSet()[family]; !ok {
		return r145SystemCatalogVariant{}, false
	}
	return r145SystemCatalogVariant{SystemID: family, Venue: venue, Side: side, Route: route}, true
}

var r145NativeKnownVariantCells = []string{
	"arb|kalshi|YES|taker", "arb|polyus|NO|taker", "arb|polyus|YES|taker",
	"bookskew|kalshi|NO|taker", "bookskew|kalshi|YES|taker",
	"confluence|kalshi|YES|taker", "confluence|polyus|NO|taker", "confluence|polyus|YES|taker",
	"cross|kalshi|YES|taker",
	"favlong|kalshi|NO|maker", "favlong|kalshi|NO|taker", "favlong|kalshi|YES|taker", "favlong|polyus|YES|taker",
	"fbridge|kalshi|NO|taker", "fbridge|kalshi|YES|taker", "fbridge|polyus|NO|taker", "fbridge|polyus|YES|taker",
	"freshfade|kalshi|NO|taker", "freshfade|polyus|NO|taker", "freshlist|kalshi|YES|taker", "freshlist|polyus|YES|taker",
	"fundtilt|kalshi|NO|taker", "fundtilt|kalshi|YES|taker",
	"invert:arb|polyus|NO|taker", "invert:favlong|polyus|NO|taker", "invert:freshfade|polyus|YES|taker",
	"invert:freshlist|kalshi|NO|taker", "invert:freshlist|polyus|NO|taker", "invert:kalshi-flow|kalshi|NO|taker", "invert:kalshi-flow|kalshi|YES|taker",
	"invert:kcrypto|kalshi|NO|taker", "invert:kcrypto|kalshi|YES|taker",
	"invert:kthresh|kalshi|NO|taker", "invert:kthresh|kalshi|YES|taker", "invert:pbridge|kalshi|NO|taker",
	"invert:pbridge|kalshi|YES|taker", "invert:pfbridge|polyus|NO|taker", "invert:pmatch|polyus|NO|taker", "invert:poly-pred-kalshi|kalshi|NO|taker",
	"invert:poly-pred-kalshi|kalshi|YES|taker", "invert:polyus-consensus|polyus|NO|taker",
	"invert:polyus-consensus|polyus|YES|taker", "invert:polyus-flow|polyus|NO|taker",
	"invert:polyus-flow|polyus|YES|taker", "invert:polyus-whale|polyus|NO|taker",
	"invert:polyus-whale|polyus|YES|taker", "invert:spotlag|kalshi|NO|taker", "invert:spotlag|kalshi|YES|taker",
	"invert:xmatch|kalshi|NO|taker", "invert:xmatch|kalshi|YES|taker",
	"kalshi-flow|kalshi|NO|maker", "kalshi-flow|kalshi|NO|taker", "kalshi-flow|kalshi|YES|maker",
	"kalshi-flow|kalshi|YES|taker", "kalshi-whale|kalshi|NO|taker", "kalshi-whale|kalshi|YES|taker",
	"kcrypto|kalshi|NO|maker", "kcrypto|kalshi|NO|taker", "kcrypto|kalshi|YES|maker", "kcrypto|kalshi|YES|taker",
	"kflow|kalshi|NO|maker", "kflow|kalshi|NO|taker", "kflow|kalshi|YES|maker", "kflow|kalshi|YES|taker",
	"kthresh|kalshi|NO|taker", "kthresh|kalshi|YES|taker", "notail|kalshi|NO|taker", "notail|polyus|NO|taker",
	"meanrev|kalshi|YES|taker", "meanrev|polyus|NO|taker", "meanrev|polyus|YES|taker",
	"ml-book|kalshi|NO|maker", "ml-book|kalshi|NO|taker", "ml-book|kalshi|YES|maker", "ml-book|kalshi|YES|taker",
	"ml-book|polyus|NO|maker", "ml-book|polyus|NO|taker", "ml-book|polyus|YES|maker", "ml-book|polyus|YES|taker",
	"pbridge|kalshi|NO|maker", "pbridge|kalshi|NO|taker", "pbridge|kalshi|YES|maker", "pbridge|kalshi|YES|taker",
	"pfbridge|kalshi|NO|taker", "pfbridge|kalshi|YES|taker", "pfbridge|polyus|NO|taker", "pfbridge|polyus|YES|taker",
	"pmatch|kalshi|NO|taker", "pmatch|kalshi|YES|taker", "pmatch|polyus|NO|taker", "pmatch|polyus|YES|taker", "poly-pred-kalshi|kalshi|NO|taker",
	"poly-pred-kalshi|kalshi|YES|taker", "polyus-consensus|polyus|NO|taker", "polyus-consensus|polyus|YES|taker",
	"polyus-flow|polyus|NO|taker", "polyus-flow|polyus|YES|taker", "polyus-whale|polyus|NO|taker",
	"polyus-whale|polyus|YES|taker", "spotlag|kalshi|NO|taker", "spotlag|kalshi|YES|taker",
	"rawflow|kalshi|NO|maker", "rawflow|kalshi|NO|taker", "rawflow|kalshi|YES|maker", "rawflow|kalshi|YES|taker",
	"sharpline|kalshi|NO|taker", "sharpline|kalshi|YES|taker",
	"whale-exit-hold-bridge|kalshi|NO|taker", "whale-exit-hold-bridge|kalshi|YES|taker",
	"whale-exit-hold-bridge|polyus|NO|taker", "whale-exit-hold-bridge|polyus|YES|taker",
	"wxedge|kalshi|YES|taker",
	"xinv-pcrypto|kalshi|NO|taker", "xinv-pcrypto|kalshi|YES|taker",
	"xmatch|kalshi|NO|maker", "xmatch|kalshi|NO|taker", "xmatch|kalshi|YES|maker", "xmatch|kalshi|YES|taker",
	"xvgap2|polyus|NO|taker", "xvgap2|polyus|YES|taker", "xvgapk|kalshi|NO|taker",
	"xvgapk|kalshi|YES|taker", "xvgap|polyus|YES|taker", "xvlag|kalshi|YES|taker", "xvlag|polyus|NO|taker", "xvlag|polyus|YES|taker",
}

var r145DedicatedBookKnownVariantCells = []string{
	"favlong80|kalshi|NO|maker", "favlong80|kalshi|NO|taker",
	"favlong80|kalshi|YES|maker", "favlong80|kalshi|YES|taker",
	"cheapband|kalshi|NO|taker", "cheapband|kalshi|YES|taker",
	"cheapband|polyus|NO|taker", "cheapband|polyus|YES|taker",
}

const (
	r145HandoffGenericTaker  = "generic_exact_taker"
	r145HandoffNativeMaker   = "native_auto_place_maker"
	r145HandoffFreshInv      = "freshinv_book"
	r145HandoffXVGap         = "xvgap_book"
	r145HandoffRawFlow       = "rawflow_book"
	r145HandoffMLBook        = "book_native_ml"
	r145HandoffDedicatedBook = "dedicated_book"
	r145HandoffUnregistered  = "unregistered"
)

var r145NativeMakerSystemSet = map[string]bool{
	"favlong": true, "kalshi-flow": true, "kcrypto": true,
	"kflow": true, "pbridge": true, "xmatch": true,
}

func r145PolicySignalProducer(systemID string) bool {
	base := strings.ToLower(strings.TrimSpace(systemID))
	base = strings.TrimPrefix(base, "invert:")
	for _, row := range policyFamilies {
		if row.fam == base {
			return true
		}
	}
	return false
}

// r145NativeVariantHandoff traces a native declaration to the concrete executor family that owns
// it.  Returning unregistered is intentional: a row may remain declared for audit visibility
// without being counted as code-connected.  In particular, independent inverses are taker-only;
// no maker route is inferred from the original system.
func r145NativeVariantHandoff(v r145SystemCatalogVariant) string {
	systemID := strings.ToLower(strings.TrimSpace(v.SystemID))
	venue := strings.ToLower(strings.TrimSpace(v.Venue))
	route := strings.ToLower(strings.TrimSpace(v.Route))
	switch {
	case systemID == "ml-book":
		return r145HandoffMLBook
	case systemID == "rawflow":
		return r145HandoffRawFlow
	case systemID == "xvgap" && venue == "polyus" && route == "taker":
		return r145HandoffXVGap
	case systemID == "freshlist" && venue == "polyus" && route == "taker":
		return r145HandoffFreshInv
	case systemID == "invert:freshlist" && venue == "kalshi" && route == "taker":
		return r145HandoffFreshInv
	case route == "maker" && r145NativeMakerSystemSet[systemID] &&
		r147NativeSignalProducerCapable(systemID, venue, v.Side):
		return r145HandoffNativeMaker
	case route == "taker" && r147NativeSignalProducerCapable(systemID, venue, v.Side):
		return r145HandoffGenericTaker
	default:
		return r145HandoffUnregistered
	}
}

func r145DedicatedBookVariantHandoff(v r145SystemCatalogVariant) string {
	validSide := v.Side == "YES" || v.Side == "NO"
	if validSide && ((v.SystemID == "favlong80" && v.Venue == "kalshi" &&
		(v.Route == "maker" || v.Route == "taker")) ||
		(v.SystemID == "cheapband" && (v.Venue == "kalshi" || v.Venue == "polyus") &&
			v.Route == "taker")) {
		return r145HandoffDedicatedBook
	}
	return r145HandoffUnregistered
}

func r145ParseCatalogVariantCell(cell, handoff string) (r145SystemCatalogVariant, bool) {
	parts := strings.Split(cell, "|")
	if len(parts) != 4 {
		return r145SystemCatalogVariant{}, false
	}
	return r145SystemCatalogVariant{SystemID: parts[0], Venue: parts[1], Side: parts[2], Route: parts[3], Handoff: handoff}, true
}

func r145KnownSystemVariants() []r145SystemCatalogVariant {
	byCell := make(map[string]r145SystemCatalogVariant, 256)
	add := func(v r145SystemCatalogVariant) {
		key := strings.Join([]string{v.SystemID, v.Venue, v.Side, v.Route}, "\x00")
		byCell[key] = v
	}
	for _, cell := range r145NativeKnownVariantCells {
		if v, ok := r145ParseCatalogVariantCell(cell, ""); ok {
			v.Handoff = r145NativeVariantHandoff(v)
			add(v)
			// This is a real independently named system, not an algebraic score row: insertSignal
			// flips the emitted side, clears copied price/model fields, then collects the actual
			// opposite ask/depth/fee through the same generic taker and settlement path.
			if v.Route == "taker" && !strings.HasPrefix(v.SystemID, "invert:") &&
				r147NativeSignalProducerCapable(v.SystemID, v.Venue, v.Side) {
				inverse := v
				inverse.SystemID = "invert:" + v.SystemID
				if v.Side == "YES" {
					inverse.Side = "NO"
				} else if v.Side == "NO" {
					inverse.Side = "YES"
				} else {
					continue
				}
				inverse.Handoff = r145NativeVariantHandoff(inverse)
				add(inverse)
			}
		}
	}
	// R148: portable single-instrument signals gain the opposite execution venue only through the
	// current normalized-certificate adapter. Both destination sides are declared because exact
	// venue instruments can be same- or inverse-oriented; runtime side selection belongs solely to
	// the certificate. This does not claim that every source ticker currently has a matched twin.
	for _, cell := range r145NativeKnownVariantCells {
		source, ok := r145ParseCatalogVariantCell(cell, "")
		if !ok || source.Route != "taker" || strings.HasPrefix(source.SystemID, "invert:") ||
			!r148CrossVenueSignalPortable(source.SystemID) ||
			!r147NativeSignalProducerCapable(source.SystemID, source.Venue, source.Side) {
			continue
		}
		destinationVenue := r148OtherExecutionVenue(source.Venue)
		for _, destinationSide := range []string{"YES", "NO"} {
			destination := r145SystemCatalogVariant{SystemID: source.SystemID, Venue: destinationVenue,
				Side: destinationSide, Route: "taker", Handoff: r145HandoffGenericTaker}
			add(destination)
			inverse := destination
			inverse.SystemID = "invert:" + source.SystemID
			inverse.Side = concreteOppositeSide(destinationSide)
			add(inverse)
		}
	}
	for _, spec := range storage.SystemExecutionSpecs() {
		handoff := string(spec.HandoffClass)
		for _, cell := range spec.ExactVariants() {
			add(r145SystemCatalogVariant{SystemID: cell.SystemID, Venue: cell.Venue, Side: cell.Side,
				Route: cell.Route, Handoff: handoff})
		}
	}
	for _, cell := range r145DedicatedBookKnownVariantCells {
		if v, ok := r145ParseCatalogVariantCell(cell, ""); ok {
			v.Handoff = r145DedicatedBookVariantHandoff(v)
			add(v)
		}
	}
	out := make([]r145SystemCatalogVariant, 0, len(byCell))
	for _, row := range byCell {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return a.SystemID+"\x00"+a.Venue+"\x00"+a.Side+"\x00"+a.Route <
			b.SystemID+"\x00"+b.Venue+"\x00"+b.Side+"\x00"+b.Route
	})
	return out
}

// r145CatalogVariantHasCodePath separates a declared venue/side/route cell from an order handoff
// that exists in code. Native and dedicated cells must name their concrete adapter; the old
// `standalone => true` shortcut is deliberately gone. Cells from the 19-system registry use the
// machine-readable adapter contract; in particular, a child declaration is not connected merely
// because the registry can name its venue, side, and route.
// Collector freshness, an eligible signal, economic proof, Paper authority, and LIVE authority are
// intentionally separate runtime facts and are not claimed here.
func r145CatalogVariantHasCodePath(row r145SystemCatalogVariant) bool {
	systemID := strings.ToLower(strings.TrimSpace(row.SystemID))
	venue := strings.ToLower(strings.TrimSpace(row.Venue))
	side := strings.ToUpper(strings.TrimSpace(row.Side))
	route := strings.ToLower(strings.TrimSpace(row.Route))
	switch strings.ToLower(strings.TrimSpace(row.Handoff)) {
	case r145HandoffGenericTaker:
		return route == "taker" && (venue == "kalshi" || venue == "polyus") &&
			(side == "YES" || side == "NO") && (r147NativeSignalProducerCapable(systemID, venue, side) ||
			r148CrossVenueProducerCapable(systemID, venue, side))
	case r145HandoffNativeMaker:
		return route == "maker" && venue == "kalshi" && (side == "YES" || side == "NO") &&
			r145NativeMakerSystemSet[systemID] && r147NativeSignalProducerCapable(systemID, venue, side)
	case r145HandoffFreshInv:
		return route == "taker" && (side == "YES" || side == "NO") &&
			((systemID == "freshlist" && venue == "polyus") ||
				(systemID == "invert:freshlist" && venue == "kalshi")) &&
			r147NativeSignalProducerCapable(systemID, venue, side)
	case r145HandoffXVGap:
		return systemID == "xvgap" && venue == "polyus" && route == "taker" &&
			(side == "YES" || side == "NO") && r147NativeSignalProducerCapable(systemID, venue, side)
	case r145HandoffRawFlow:
		return systemID == "rawflow" && venue == "kalshi" &&
			(side == "YES" || side == "NO") && (route == "maker" || route == "taker")
	case r145HandoffMLBook:
		return systemID == "ml-book" && (venue == "kalshi" || venue == "polyus") &&
			(side == "YES" || side == "NO") && (route == "maker" || route == "taker")
	case r145HandoffDedicatedBook:
		return (side == "YES" || side == "NO") &&
			((systemID == "favlong80" && venue == "kalshi" &&
				(route == "maker" || route == "taker")) ||
				(systemID == "cheapband" && (venue == "kalshi" || venue == "polyus") &&
					route == "taker"))
	}
	spec, ok := storage.SystemExecutionCapability(systemID)
	if !ok {
		return false
	}
	return spec.SupportsVenue(venue) && spec.SupportsSide(side) && spec.SupportsRoute(route) &&
		storage.SystemExecutionAdapterStatus(spec).Connected
}

func r145CanonicalSystemCatalogCounts() r145SystemCatalogCounts {
	bases := r145CanonicalBaseSystems()
	variants := r145KnownSystemVariants()
	bundleSystems, bundleVariants, connectedBundles, blockedBundles := r147BundleProductVariantCounts()
	signalContracts := r147SystemVariantSignalContractCounts()
	hasVariant := make(map[string]bool, len(bases))
	connected := 0
	for _, row := range variants {
		hasVariant[row.SystemID] = true
		if r145CatalogVariantHasCodePath(row) {
			connected++
		}
	}
	missing := make([]string, 0)
	missingExecutable := make([]string, 0)
	externallyBlockedDecisions := make([]string, 0)
	orderSystems, childOverlays, researchVenue, dataControls := 0, 0, 0, 0
	researchCohorts, unsupported, frozen, portfolioEvidence := 0, 0, 0, 0
	for _, row := range bases {
		if product, ok := r147BundleProductContractFor(row.ID); ok && len(product.Variants) > 0 {
			hasVariant[row.ID] = true
		}
		if product, ok := r147BundleProductContractFor(row.ID); ok && product.DecisionSystem &&
			row.ExecutionRole == r145ExecutionUnsupported {
			externallyBlockedDecisions = append(externallyBlockedDecisions, row.ID)
		}
		switch row.ExecutionRole {
		case r145ExecutionOrderSystem:
			orderSystems++
		case r145ExecutionChildOverlay:
			childOverlays++
		case r145ExecutionResearchVenue:
			researchVenue++
		case r145ExecutionDataControl:
			dataControls++
		case r145ExecutionResearchCohort:
			researchCohorts++
		case r145ExecutionUnsupported:
			unsupported++
		case r145ExecutionFrozen:
			frozen++
		case r145ExecutionPortfolioEvidence:
			portfolioEvidence++
		}
		if !hasVariant[row.ID] {
			missing = append(missing, row.ID)
			if row.RequiresOrderRoute {
				missingExecutable = append(missingExecutable, row.ID)
			}
		}
	}
	classifiedNonOrder := childOverlays + researchVenue + dataControls + researchCohorts + unsupported + frozen + portfolioEvidence
	return r145SystemCatalogCounts{
		BaseSystems:                      len(bases),
		DeclaredTypedVariants:            len(variants),
		CodePathConnectedVariants:        connected,
		DeclaredWithoutCodePathVariants:  len(variants) - connected,
		ProducerCapableTypedVariants:     signalContracts.ProducerCapable,
		ProducerBlockedTypedVariants:     signalContracts.ProducerBlocked,
		CrossVenueInputVariants:          signalContracts.CrossVenueInputs,
		SignalContractVariants:           signalContracts.SignalContracts,
		ExecutionDerivativeContracts:     signalContracts.DerivativeContracts,
		KnownExactExecutionVariants:      connected,
		BundleProductSystems:             bundleSystems,
		DeclaredBundleProductVariants:    bundleVariants,
		ConnectedStagedBundleVariants:    connectedBundles,
		ExternallyBlockedBundleVariants:  blockedBundles,
		MissingTypedVariantSystems:       len(missing),
		MissingVariantSystemIDs:          missing,
		OrderOriginatingBaseSystems:      orderSystems,
		ChildOverlayBaseSystems:          childOverlays,
		ClassifiedNonOrderBaseSystems:    classifiedNonOrder,
		ResearchVenueBaseSystems:         researchVenue,
		DataControlBaseSystems:           dataControls,
		ResearchCohortBaseSystems:        researchCohorts,
		UnsupportedProductBaseSystems:    unsupported,
		FrozenBaseSystems:                frozen,
		PortfolioEvidenceBaseSystems:     portfolioEvidence,
		MissingExecutableVariantSystems:  len(missingExecutable),
		MissingExecutableSystemIDs:       missingExecutable,
		ExternallyBlockedDecisionSystems: len(externallyBlockedDecisions),
		ExternallyBlockedDecisionIDs:     externallyBlockedDecisions,
		Aliases:                          len(r145CanonicalSystemAliases()),
		Components:                       len(r145CanonicalSystemComponents()),
		VariantCountContract:             "declared typed single-contract variants are a lower bound over explicitly traced K/PUS x YES/NO x maker/taker/RFQ cells; additive multi-leg products are counted separately as exact bundle variants so they never masquerade as Y/N singles; code-path-connected counts require a named adapter; neither count claims a fresh collector, eligible signal, economic proof, Paper authority, or LIVE authority",
	}
}
