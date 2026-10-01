package server

import (
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// r147SystemVariantSignalContract separates three identities that the old four-column catalog
// collapsed: the upstream venue topology, the venue where an order can execute, and the exact
// side/route. A K-PINT signal that executes on Kalshi is not a fictitious `cross` venue; PINT is a
// research input and Kalshi is the money venue. One execution cell may have multiple contracts
// when genuinely different upstream topologies can fire it (for example confluence).
type r147SystemVariantSignalContract struct {
	SystemID            string   `json:"system_id"`
	ParentSystemID      string   `json:"parent_system_id,omitempty"`
	ExecutionVenue      string   `json:"execution_venue"`
	Side                string   `json:"side"`
	Action              string   `json:"action"`
	Route               string   `json:"route"`
	TimeInForce         string   `json:"time_in_force,omitempty"`
	ReduceOnly          bool     `json:"reduce_only,omitempty"`
	ExecutionDerivative bool     `json:"execution_derivative,omitempty"`
	LiveAuthority       bool     `json:"live_authority"`
	Handoff             string   `json:"handoff"`
	InputTopology       string   `json:"input_topology"`
	RequiredInputs      []string `json:"required_inputs"`
	CrossVenue          bool     `json:"cross_venue"`
	PINTResearchInput   bool     `json:"pint_research_input_only,omitempty"`
	ProducerCapable     bool     `json:"producer_capable"`
	FreshFedRuntime     string   `json:"fresh_fed_runtime"`
	Blocker             string   `json:"blocker,omitempty"`
}

type r147SystemVariantSignalContractSummary struct {
	SignalContracts        int            `json:"signal_contracts"`
	TypedExecutionVariants int            `json:"typed_execution_variants"`
	DerivativeContracts    int            `json:"derivative_contracts"`
	ProducerCapable        int            `json:"producer_capable_typed_variants"`
	ProducerBlocked        int            `json:"producer_blocked_typed_variants"`
	CrossVenueInputs       int            `json:"cross_venue_input_variants"`
	FreshFedRuntime        string         `json:"fresh_fed_runtime"`
	ByInputTopology        map[string]int `json:"by_input_topology"`
}

type r147InputTopology struct {
	name     string
	required []string
}

func r147Topology(name string, inputs ...string) r147InputTopology {
	return r147InputTopology{name: name, required: append([]string(nil), inputs...)}
}

// r147SignalInputTopology carries the exact upstream venue/source identity from a detector row
// into dedicated/follower execution paths.  It is intentionally parsed from the durable ExecExpr
// receipt rather than inferred from a family name: arb, confluence and kcrypto can each fire from
// more than one input set, and guessing would pool unlike evidence.
func r147SignalInputTopology(sig storage.Signal) string {
	for _, token := range strings.FieldsFunc(sig.ExecExpr, func(r rune) bool {
		return r == '/' || r == ':' || r == ';' || r == ' ' || r == '\t' || r == '\r' || r == '\n'
	}) {
		value, ok := strings.CutPrefix(strings.TrimSpace(token), "input=")
		if !ok {
			continue
		}
		value = strings.ToUpper(strings.TrimSpace(value))
		switch value {
		case "K", "PUS", "K-PINT", "K-PUS", "PUS-PINT", "K-PUS-PINT",
			"K-SPOT", "K-OKX", "K-SPORTSBOOK", "K-NOAA", "K-PUS-SPOT",
			"K-PUS-OKX", "K-PUS-SPORTSBOOK", "K-PUS-NOAA", "SOURCE-K", "SOURCE-PUS":
			return value
		case "K-PROPER-BRIER", "K-PROPER-LOG", "K-PROPER-SPHERICAL",
			"PUS-PROPER-BRIER", "PUS-PROPER-LOG", "PUS-PROPER-SPHERICAL":
			return value
		}
	}
	return ""
}

func r147SignalInputObservedAt(sig storage.Signal) time.Time {
	for _, token := range strings.FieldsFunc(sig.ExecExpr, func(r rune) bool {
		return r == '/' || r == ';' || r == ' ' || r == '\t' || r == '\r' || r == '\n'
	}) {
		value, ok := strings.CutPrefix(strings.TrimSpace(token), "input_at=")
		if !ok {
			continue
		}
		if at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value)); err == nil {
			return at
		}
	}
	return time.Time{}
}

func r147InputReceiptExpr(topology string, observedAt time.Time) string {
	topology = strings.ToUpper(strings.TrimSpace(topology))
	if topology == "" {
		return ""
	}
	if observedAt.IsZero() {
		return "input=" + topology
	}
	return "input=" + topology + "/input_at=" + observedAt.UTC().Format(time.RFC3339Nano)
}

func r147VenueCode(venue string) string {
	switch strings.ToLower(strings.TrimSpace(venue)) {
	case "kalshi":
		return "K"
	case "polyus":
		return "PUS"
	case "polymarket":
		return "PINT"
	default:
		return strings.ToUpper(strings.TrimSpace(venue))
	}
}

func r147TopologyIsCrossVenue(t r147InputTopology) bool {
	n := 0
	for _, in := range t.required {
		switch in {
		case "K", "PUS", "PINT":
			n++
		}
	}
	return n >= 2
}

func r147TopologyHas(t r147InputTopology, input string) bool {
	for _, candidate := range t.required {
		if candidate == input {
			return true
		}
	}
	return false
}

// r147NativeSignalProducerCells is intentionally independent from the declaration roster. Every
// cell below is traced to an actual current insertSignal emitter. Adding a catalog row cannot make
// it connected; a different venue/side must first gain a real producer and a regression test.
var r147NativeSignalProducerCells = func() map[string]bool {
	rows := []string{
		"arb|kalshi|YES", "arb|polyus|NO", "arb|polyus|YES",
		"bookskew|kalshi|NO", "bookskew|kalshi|YES",
		"confluence|kalshi|YES", "confluence|polyus|NO", "confluence|polyus|YES",
		"cross|kalshi|YES",
		"favlong|kalshi|NO", "favlong|kalshi|YES", "favlong|polyus|YES",
		"fbridge|kalshi|NO", "fbridge|kalshi|YES", "fbridge|polyus|NO", "fbridge|polyus|YES",
		"freshfade|kalshi|NO", "freshfade|polyus|NO",
		"freshlist|kalshi|YES", "freshlist|polyus|YES",
		"fundtilt|kalshi|NO", "fundtilt|kalshi|YES",
		"kalshi-flow|kalshi|NO", "kalshi-flow|kalshi|YES",
		"kalshi-whale|kalshi|NO", "kalshi-whale|kalshi|YES",
		"kcrypto|kalshi|NO", "kcrypto|kalshi|YES",
		"kflow|kalshi|NO", "kflow|kalshi|YES",
		"kthresh|kalshi|NO", "kthresh|kalshi|YES",
		"meanrev|kalshi|YES", "meanrev|polyus|NO", "meanrev|polyus|YES",
		"notail|kalshi|NO", "notail|polyus|NO",
		"pbridge|kalshi|NO", "pbridge|kalshi|YES",
		"pfbridge|kalshi|NO", "pfbridge|kalshi|YES", "pfbridge|polyus|NO", "pfbridge|polyus|YES",
		"pmatch|kalshi|NO", "pmatch|kalshi|YES", "pmatch|polyus|NO", "pmatch|polyus|YES",
		"poly-pred-kalshi|kalshi|NO", "poly-pred-kalshi|kalshi|YES",
		"polyus-consensus|polyus|NO", "polyus-consensus|polyus|YES",
		"polyus-flow|polyus|NO", "polyus-flow|polyus|YES",
		"polyus-whale|polyus|NO", "polyus-whale|polyus|YES",
		"sharpline|kalshi|NO", "sharpline|kalshi|YES",
		"spotlag|kalshi|NO", "spotlag|kalshi|YES",
		"whale-exit-hold-bridge|kalshi|NO", "whale-exit-hold-bridge|kalshi|YES",
		"whale-exit-hold-bridge|polyus|NO", "whale-exit-hold-bridge|polyus|YES",
		"wxedge|kalshi|YES",
		"xinv-pcrypto|kalshi|NO", "xinv-pcrypto|kalshi|YES",
		"xmatch|kalshi|NO", "xmatch|kalshi|YES",
		"xvgap|polyus|YES", "xvgap2|polyus|NO", "xvgap2|polyus|YES",
		"xvgapk|kalshi|NO", "xvgapk|kalshi|YES",
		"xvlag|kalshi|YES", "xvlag|polyus|NO", "xvlag|polyus|YES",
	}
	out := make(map[string]bool, len(rows))
	for _, row := range rows {
		out[row] = true
	}
	return out
}()

func r147NativeSignalProducerCapable(systemID, venue, side string) bool {
	systemID = strings.ToLower(strings.TrimSpace(systemID))
	venue = strings.ToLower(strings.TrimSpace(venue))
	side = strings.ToUpper(strings.TrimSpace(side))
	if strings.HasPrefix(systemID, "invert:") {
		base := strings.TrimPrefix(systemID, "invert:")
		opposite := ""
		if side == "YES" {
			opposite = "NO"
		} else if side == "NO" {
			opposite = "YES"
		}
		return opposite != "" && r147NativeSignalProducerCells[base+"|"+venue+"|"+opposite]
	}
	return r147NativeSignalProducerCells[systemID+"|"+venue+"|"+side]
}

func r147NativeInputTopologies(systemID, executionVenue string) []r147InputTopology {
	base := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(systemID)), "invert:")
	target := r147VenueCode(executionVenue)
	switch base {
	case "arb":
		// The arb producer emits one signal per compared pair.  It never combines the K-PINT
		// and K-PUS observations into a three-input signal.  A Kalshi buy can therefore be
		// K-PINT or K-PUS and must carry the exact pair; a PolyUS buy can only be K-PUS.
		if target == "K" {
			return []r147InputTopology{r147Topology("K-PINT", "K", "PINT"),
				r147Topology("K-PUS", "K", "PUS")}
		}
		return []r147InputTopology{r147Topology("K-PUS", "K", "PUS")}
	case "confluence":
		if target == "K" {
			return []r147InputTopology{r147Topology("K-PINT", "K", "PINT"),
				r147Topology("K-PUS", "K", "PUS"), r147Topology("K-PUS-PINT", "K", "PUS", "PINT")}
		}
		return []r147InputTopology{r147Topology("PUS-PINT", "PUS", "PINT"),
			r147Topology("K-PUS", "K", "PUS"), r147Topology("K-PUS-PINT", "K", "PUS", "PINT")}
	case "cross", "pbridge", "poly-pred-kalshi", "xinv-pcrypto", "xmatch":
		return []r147InputTopology{r147Topology("K-PINT", "K", "PINT")}
	case "fbridge", "pfbridge", "pmatch", "whale-exit-hold-bridge":
		if target == "K" {
			return []r147InputTopology{r147Topology("K-PINT", "K", "PINT")}
		}
		return []r147InputTopology{r147Topology("PUS-PINT", "PUS", "PINT")}
	case "xvgap", "xvgap2", "xvgapk", "xvlag":
		return []r147InputTopology{r147Topology("K-PUS", "K", "PUS")}
	case "fundtilt":
		return []r147InputTopology{r147Topology("K-OKX", "K", "OKX")}
	case "kcrypto":
		// Coinbase spot is recorded as a model feature, not the trading trigger.  The order is
		// either a Kalshi-only mild favorite or a size-boosted K-PINT agreement.  Until the
		// producer carries which branch fired, these two contracts remain deliberately ambiguous.
		return []r147InputTopology{r147Topology("K", "K"), r147Topology("K-PINT", "K", "PINT")}
	case "spotlag":
		return []r147InputTopology{r147Topology("K-SPOT", "K", "SPOT")}
	case "sharpline":
		return []r147InputTopology{r147Topology("K-SPORTSBOOK", "K", "SPORTSBOOK")}
	case "wxedge":
		return []r147InputTopology{r147Topology("K-NOAA", "K", "NOAA")}
	case "cheapband":
		// Cheapband is a distinct price/category/spread system layered on a qualifying source.
		// Preserve that source's exact input topology instead of pooling every cheap quote into
		// one same-venue row.  The execution destination remains the venue carrying the cheap ask.
		if target == "K" {
			return []r147InputTopology{r147Topology("K", "K"),
				r147Topology("K-PINT", "K", "PINT"), r147Topology("K-PUS", "K", "PUS"),
				r147Topology("K-PUS-PINT", "K", "PUS", "PINT"),
				r147Topology("K-SPOT", "K", "SPOT"), r147Topology("K-OKX", "K", "OKX"),
				r147Topology("K-SPORTSBOOK", "K", "SPORTSBOOK"), r147Topology("K-NOAA", "K", "NOAA")}
		}
		return []r147InputTopology{r147Topology("PUS", "PUS"),
			r147Topology("PUS-PINT", "PUS", "PINT"), r147Topology("K-PUS", "K", "PUS"),
			r147Topology("K-PUS-PINT", "K", "PUS", "PINT")}
	default:
		return []r147InputTopology{r147Topology(target, target)}
	}
}

const r147SignalContractVersion = "r147sc-v1"

// r147SignalContractIdentity is the stable prospective-money identity.  It deliberately names
// the upstream input topology as well as the order destination, side and route: K-PINT economics
// can never be looked up under a K-PUS or K-PUS-PINT receipt.
func r147SignalContractIdentity(row r147SystemVariantSignalContract) string {
	parts := []string{r147SignalContractVersion,
		strings.ToLower(strings.TrimSpace(row.SystemID)),
		strings.ToLower(strings.TrimSpace(row.ExecutionVenue)),
		strings.ToUpper(strings.TrimSpace(row.Side)),
		strings.ToLower(strings.TrimSpace(row.Route)),
		strings.ToUpper(strings.TrimSpace(row.InputTopology))}
	if row.ExecutionDerivative {
		parts = append(parts, strings.ToUpper(strings.TrimSpace(row.Action)), "reduce_only_fok")
	}
	return strings.Join(parts, "|")
}

func r147ExecutionDerivativeSignalContracts() []r147SystemVariantSignalContract {
	rows := make([]r147SystemVariantSignalContract, 0, 6)
	for _, side := range []string{"NO", "YES"} {
		for _, transform := range []string{"BRIER", "LOG", "SPHERICAL"} {
			rows = append(rows, r147SystemVariantSignalContract{
				SystemID: "proper-score-momentum", ParentSystemID: "proper-score-executor",
				ExecutionVenue: "kalshi", Side: side, Action: "SELL", Route: "taker",
				TimeInForce: "fill_or_kill", ReduceOnly: true, ExecutionDerivative: true,
				LiveAuthority: false, Handoff: "proper-score-momentum-delayed-sell-fok",
				InputTopology: "K-PROPER-" + transform, RequiredInputs: []string{"K"},
				ProducerCapable: true, FreshFedRuntime: "not_measured",
			})
		}
	}
	return rows
}

func r147ProperMomentumSignalContract(side, topology string) (r147SystemVariantSignalContract, bool) {
	for _, row := range r147ExecutionDerivativeSignalContracts() {
		if strings.EqualFold(row.Side, side) && strings.EqualFold(row.InputTopology, topology) {
			return row, true
		}
	}
	return r147SystemVariantSignalContract{}, false
}

// r147ExactSignalContracts returns only code-connected contracts for one exact executable cell.
// PINT may appear in RequiredInputs but can never be an execution venue.
func r147ExactSignalContracts(systemID, venue, side, route string) []r147SystemVariantSignalContract {
	systemID = strings.ToLower(strings.TrimSpace(systemID))
	venue = strings.ToLower(strings.TrimSpace(venue))
	side = strings.ToUpper(strings.TrimSpace(side))
	route = strings.ToLower(strings.TrimSpace(route))
	out := make([]r147SystemVariantSignalContract, 0, 3)
	if venue != "kalshi" && venue != "polyus" {
		return out
	}
	for _, row := range r147SystemVariantSignalContracts() {
		if !row.ProducerCapable || strings.ToLower(row.SystemID) != systemID ||
			strings.ToLower(row.ExecutionVenue) != venue || strings.ToUpper(row.Side) != side ||
			strings.ToLower(row.Route) != route {
			continue
		}
		out = append(out, row)
	}
	return out
}

func r147SpecInputTopologies(systemID, executionVenue string) []r147InputTopology {
	target := r147VenueCode(executionVenue)
	switch strings.ToLower(strings.TrimSpace(systemID)) {
	case "clientele-clock-basis":
		return []r147InputTopology{r147Topology("K-PUS", "K", "PUS")}
	case "deadline-hazard-surface":
		return []r147InputTopology{r147Topology("K-NOAA", "K", "NOAA")}
	case "paired-bridge-inversion", "maker-salvage-matched-cohort":
		return []r147InputTopology{r147Topology("SOURCE-"+target, "SOURCE_SIGNAL", target)}
	case "proper-score-executor":
		// The three funded scoring transforms are independent decision policies over the same
		// venue book.  Keeping the transform in the exact input contract prevents Brier, log,
		// and spherical coordinates on one ticker from collapsing into one economic sample.
		return []r147InputTopology{
			r147Topology(target+"-PROPER-BRIER", target),
			r147Topology(target+"-PROPER-LOG", target),
			r147Topology(target+"-PROPER-SPHERICAL", target),
		}
	default:
		return []r147InputTopology{r147Topology(target, target)}
	}
}

func r147VariantProducerCapable(v r145SystemCatalogVariant) bool {
	if spec, ok := storage.SystemExecutionCapability(v.SystemID); ok {
		adapter := storage.SystemExecutionAdapterStatus(spec)
		return adapter.Connected && spec.SupportsVenue(v.Venue) && spec.SupportsSide(v.Side) &&
			spec.SupportsRoute(v.Route)
	}
	switch strings.ToLower(strings.TrimSpace(v.Handoff)) {
	case r145HandoffGenericTaker, r145HandoffNativeMaker, r145HandoffFreshInv, r145HandoffXVGap:
		return r147NativeSignalProducerCapable(v.SystemID, v.Venue, v.Side) ||
			(strings.EqualFold(v.Handoff, r145HandoffGenericTaker) &&
				r148CrossVenueProducerCapable(v.SystemID, v.Venue, v.Side))
	case r145HandoffRawFlow, r145HandoffMLBook, r145HandoffDedicatedBook:
		return r145CatalogVariantHasCodePath(v)
	default:
		return false
	}
}

func r147VariantInputTopologies(v r145SystemCatalogVariant) []r147InputTopology {
	if _, ok := storage.SystemExecutionCapability(v.SystemID); ok {
		return r147SpecInputTopologies(v.SystemID, v.Venue)
	}
	var out []r147InputTopology
	seen := map[string]bool{}
	add := func(rows []r147InputTopology) {
		for _, row := range rows {
			if row.name == "" || seen[row.name] {
				continue
			}
			seen[row.name] = true
			out = append(out, row)
		}
	}
	if r147NativeSignalProducerCapable(v.SystemID, v.Venue, v.Side) {
		add(r147NativeInputTopologies(v.SystemID, v.Venue))
	}
	// Dedicated, RawFlow, and ML-book executors are real producers but intentionally do not live
	// in the generic signal-producer cell map. They still require their actual venue topology;
	// UNDECLARED would make a validated runtime candidate impossible to track exactly.
	if (strings.EqualFold(v.Handoff, r145HandoffDedicatedBook) ||
		strings.EqualFold(v.Handoff, r145HandoffRawFlow) ||
		strings.EqualFold(v.Handoff, r145HandoffMLBook)) && r145CatalogVariantHasCodePath(v) {
		add(r147NativeInputTopologies(v.SystemID, v.Venue))
	}
	if r148CrossVenueProducerCapable(v.SystemID, v.Venue, v.Side) {
		add(r148CrossVenueInputTopologies(v.SystemID, v.Venue))
	}
	return out
}

func r147SystemVariantSignalContracts() []r147SystemVariantSignalContract {
	variants := r145KnownSystemVariants()
	out := make([]r147SystemVariantSignalContract, 0, len(variants))
	for _, variant := range variants {
		capable := r147VariantProducerCapable(variant)
		topologies := r147VariantInputTopologies(variant)
		if len(topologies) == 0 {
			topologies = []r147InputTopology{r147Topology("UNDECLARED")}
		}
		for _, topology := range topologies {
			row := r147SystemVariantSignalContract{SystemID: variant.SystemID,
				ExecutionVenue: variant.Venue, Side: variant.Side, Action: "BUY", Route: variant.Route,
				Handoff: variant.Handoff, InputTopology: topology.name,
				RequiredInputs: append([]string(nil), topology.required...),
				CrossVenue:     r147TopologyIsCrossVenue(topology), PINTResearchInput: r147TopologyHas(topology, "PINT"),
				ProducerCapable: capable, FreshFedRuntime: "not_measured"}
			if !capable {
				row.Blocker = "no exact current producer for this family+execution-venue+side+route cell"
			}
			out = append(out, row)
		}
	}
	out = append(out, r147ExecutionDerivativeSignalContracts()...)
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return strings.Join([]string{a.SystemID, a.ExecutionVenue, a.Side, a.Action, a.Route, a.InputTopology}, "\x00") <
			strings.Join([]string{b.SystemID, b.ExecutionVenue, b.Side, b.Action, b.Route, b.InputTopology}, "\x00")
	})
	return out
}

func r147SystemVariantSignalContractCounts() r147SystemVariantSignalContractSummary {
	contracts := r147SystemVariantSignalContracts()
	variants := r145KnownSystemVariants()
	capableCells, crossCells := map[string]bool{}, map[string]bool{}
	byTopology := map[string]int{}
	derivatives := 0
	for _, row := range contracts {
		if row.ExecutionDerivative {
			derivatives++
			byTopology[row.InputTopology]++
			continue
		}
		cell := strings.Join([]string{row.SystemID, row.ExecutionVenue, row.Side, row.Route}, "\x00")
		if row.ProducerCapable {
			capableCells[cell] = true
		}
		if row.CrossVenue {
			crossCells[cell] = true
		}
		byTopology[row.InputTopology]++
	}
	return r147SystemVariantSignalContractSummary{SignalContracts: len(contracts),
		TypedExecutionVariants: len(variants), DerivativeContracts: derivatives,
		ProducerCapable: len(capableCells),
		ProducerBlocked: len(variants) - len(capableCells), CrossVenueInputs: len(crossCells),
		FreshFedRuntime: "not_measured", ByInputTopology: byTopology}
}
