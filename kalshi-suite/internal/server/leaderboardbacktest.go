package server

// R135 historical one-share diagnostics. UnitTrial rows saw a book but never sent an order, so
// their assumed-fill cents/share are retained for audit only and are VOID as profit evidence.

import (
	"context"
	"math"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type leaderboardBacktestRow struct {
	storage.UnitTrialLeaderboardStat
	SystemID                    string                      `json:"system_id"`
	Route                       string                      `json:"route"`
	EconomicUnit                string                      `json:"economic_unit"`
	Counterfactual              bool                        `json:"counterfactual"`
	NativeCollection            *nativeSystemCollectionView `json:"native_collection,omitempty"`
	Layer                       string                      `json:"layer"`
	MeanLoPC                    float64                     `json:"mean_lo_pc"`
	MeanHiPC                    float64                     `json:"mean_hi_pc"`
	NetPerCalendarDayLo         float64                     `json:"net_per_calendar_day_lo"`
	NetPerCalendarDayHi         float64                     `json:"net_per_calendar_day_hi"`
	Proof                       string                      `json:"proof"`
	BookNative                  bool                        `json:"book_native"`
	ConfidenceReady             bool                        `json:"confidence_ready"`
	VenueLocked                 bool                        `json:"venue_locked"`
	ResearchOnly                bool                        `json:"research_only"`
	EvidenceTier                string                      `json:"evidence_tier"`
	FillConditioned             bool                        `json:"fill_conditioned"`
	ProfitEvidence              bool                        `json:"profit_evidence"`
	SimulationState             string                      `json:"simulation_state,omitempty"`
	VoidReason                  string                      `json:"void_reason,omitempty"`
	LiveAuthorizes              bool                        `json:"live_authorizes"`
	EconomicsReady              bool                        `json:"economics_ready"`
	CollectionState             string                      `json:"collection_state"`
	CollectionN                 int                         `json:"collection_n"`
	CollectionCycles            int                         `json:"collection_cycles"`
	CollectionAlerts            int                         `json:"collection_alerts"`
	CollectionOpen              int                         `json:"collection_open"`
	CollectionCandidates        int                         `json:"collection_candidates"`
	CollectionCurrentCycle      int                         `json:"collection_current_cycle"`
	CollectionCurrentCandidates int                         `json:"collection_current_candidates"`
	CollectionSource            string                      `json:"collection_source"`
	CollectionReason            string                      `json:"collection_reason"`
	PromotionClass              string                      `json:"promotion_class,omitempty"`
	PromotionHandoff            string                      `json:"promotion_handoff,omitempty"`
	StandaloneOrder             bool                        `json:"standalone_order"`
	ExecutionConnected          bool                        `json:"execution_connected"`
	ExecutionRole               string                      `json:"execution_role,omitempty"`
	LiveHandoff                 string                      `json:"live_handoff,omitempty"`
}

type leaderboardCoverageRow struct {
	Family                      string `json:"family"`
	Side                        string `json:"side,omitempty"`
	Route                       string `json:"route"`
	Group                       string `json:"group"`
	Layer                       string `json:"layer"`
	State                       string `json:"state"`
	Venue                       string `json:"venue,omitempty"`
	N                           int    `json:"n"`
	VenueLocked                 bool   `json:"venue_locked"`
	Timed                       bool   `json:"timed_one_share_ledger"`
	ResearchOnly                bool   `json:"research_only"`
	LiveAuthorizes              bool   `json:"live_authorizes"`
	Note                        string `json:"note"`
	CollectionN                 int    `json:"collection_n,omitempty"`
	CollectionCycles            int    `json:"collection_cycles,omitempty"`
	CollectionAlerts            int    `json:"collection_alerts,omitempty"`
	CollectionOpen              int    `json:"collection_open,omitempty"`
	CollectionCandidates        int    `json:"collection_candidates,omitempty"`
	CollectionCurrentCycle      int    `json:"collection_current_cycle,omitempty"`
	CollectionCurrentCandidates int    `json:"collection_current_candidates,omitempty"`
	CollectionSource            string `json:"collection_source,omitempty"`
	PromotionClass              string `json:"promotion_class,omitempty"`
	PromotionHandoff            string `json:"promotion_handoff,omitempty"`
	StandaloneOrder             bool   `json:"standalone_order"`
	ExecutionConnected          bool   `json:"execution_connected"`
	ExecutionRole               string `json:"execution_role,omitempty"`
	LiveHandoff                 string `json:"live_handoff,omitempty"`
}

// researchPromotionApplicability prevents a Systems row from implying that every research
// mechanism is a standalone BUY button. A sealed result may promote either an exact order route,
// a multi-leg/RFQ plan, a frozen router/filter consumed by another executable policy, or a data
// guard. Only the first class is eligible for the generic single-instrument Paper->LIVE handoff.
func researchPromotionApplicability(system string) (class, handoff string, standalone bool) {
	if product, ok := r147BundleProductContractFor(system); ok {
		if product.DecisionSystem {
			if r147BundleProductHasConnectedVariant(system) {
				return "wired_staged_multi_leg_product", product.PaperPath + " -> " + product.LivePath, false
			}
			return "externally_blocked_multi_leg_product", product.Blocker + "; required: " + product.RequiredPrimitive, false
		}
		class = "router_or_filter"
		if product.Kind == "paper_execution_ledger" {
			class = "portfolio_or_evidence_ledger"
		}
		return class, product.PaperPath + " -> " + product.LivePath + "; child: " + product.ConsumedBy, false
	}
	spec, ok := storage.SystemExecutionCapability(strings.TrimSpace(system))
	if !ok {
		return "unclassified", "no system execution capability is registered; fail closed", false
	}
	switch spec.HandoffClass {
	case storage.SystemHandoffSingle:
		return "single_instrument", spec.PaperHandoff + " -> " + spec.LiveHandoff, true
	case storage.SystemHandoffBundle:
		return "multi_leg_or_rfq", spec.PaperHandoff + " -> " + spec.LiveHandoff, false
	case storage.SystemHandoffChild:
		class = "router_or_filter"
		if spec.ActionClass == storage.SystemActionData {
			class = "data_control"
		}
		adapter := storage.SystemExecutionAdapterStatus(spec)
		if !adapter.Connected {
			return class, adapter.State + ": " + adapter.Reason, false
		}
		return class, spec.PaperHandoff + " -> " + spec.LiveHandoff, false
	default:
		return "needs_execution_adapter", strings.Join(spec.UnsupportedReasons, "; "), false
	}
}

func leaderboardBacktestRows(stats []storage.UnitTrialLeaderboardStat) []leaderboardBacktestRow {
	out := make([]leaderboardBacktestRow, 0, len(stats))
	for _, st := range stats {
		if strings.TrimSpace(st.EconomicsScope) == "" {
			// Unit trials measure one hypothetical executable share per opportunity. They are
			// deliberately separate from actual sized Paper portfolio profit, which is exposed
			// by the funded-system ledger and must never be inferred from this point estimate.
			st.EconomicsScope = "historical-assumed-fill-simulation"
		}
		locked := st.Platform == "polymarket"
		route, systemID, economicUnit := "taker", takerSystemID(st.Family, st.Platform, st.Side), "share"
		if strings.TrimSpace(st.Route) != "" {
			route = strings.TrimSpace(st.Route)
		}
		if strings.TrimSpace(st.EconomicUnit) != "" {
			economicUnit = strings.TrimSpace(st.EconomicUnit)
		}
		if st.Side == "BUNDLE" {
			if strings.TrimSpace(st.Route) == "" {
				route = "staged"
			}
			systemID = "staged:" + strings.TrimSpace(st.Family) + "@" + strings.ToLower(strings.TrimSpace(st.Platform)) + " [BUNDLE]"
			if st.ProducerFamily != "" || st.RelationClass != "" || st.ExperimentEpoch != "" {
				systemID = "bundle:" + strings.TrimSpace(st.Family) + "@" + strings.ToLower(strings.TrimSpace(st.Platform)) +
					" [" + strings.Join([]string{st.ProducerFamily, st.RelationClass, route, st.ExperimentEpoch}, "/") + "]"
			}
			if strings.TrimSpace(st.EconomicUnit) == "" {
				economicUnit = "package_unit"
			}
		}
		r := leaderboardBacktestRow{UnitTrialLeaderboardStat: st, SystemID: systemID,
			Route: route, EconomicUnit: economicUnit, Counterfactual: true, Layer: "observed_book", Proof: "VOID ASSUMED-FILL HISTORY", BookNative: true,
			VenueLocked: locked, ResearchOnly: true, EvidenceTier: "historical_assumed_fill_simulation_void", FillConditioned: false,
			ProfitEvidence: false, SimulationState: "COLLECTING", VoidReason: "no exchange order, acknowledgement, or fill was observed", LiveAuthorizes: false, EconomicsReady: true,
			CollectionState: "ECONOMICS_COLLECTING", CollectionN: st.Total, CollectionOpen: st.Open,
			CollectionSource: "unit_trials", CollectionReason: "observed-book one-share simulation is collecting; no exchange order, acknowledgement, or fill is observed"}
		// R145 operator rule: the visible/ranking sample is one distinct settled venue+ticker.
		// Canonical event IDs continue to be collected but do not affect Paper proof or ranking.
		// This is explicitly a contract-cell PAPER interval, not a claim of independence and not
		// LIVE authority (LiveAuthorizes remains false; LIVE requires a sealed untouched holdout).
		rad := csRadius(st.SettledMarkets, st.SDPC)
		timeReady := st.TrackedSeconds >= 24*60*60
		if st.SettledMarkets > 0 && !math.IsInf(rad, 1) {
			r.MeanLoPC, r.MeanHiPC = st.MeanPC-rad, st.MeanPC+rad
			// Keep the observed quiet-time denominator, but replace the point cents/share with
			// its contract-sample confidence bounds. This is the conservative Net/d used for
			// ranking; it never borrows canonical-event m or repeated receipt rows.
			trackedDays := st.TrackedDays
			if trackedDays <= 0 && st.TrackedSeconds > 0 {
				trackedDays = st.TrackedSeconds / (24 * 60 * 60)
			}
			throughput := 0.0
			if trackedDays > 0 {
				throughput = float64(st.SettledMarkets) / trackedDays
				// The point Net/d must use the same one-contract-per-venue+ticker denominator as
				// n and its interval. Raw repeat receipts remain diagnostics only.
				r.NetPerCalendarDay = st.MeanPC * throughput
			}
			if timeReady && throughput > 0 {
				r.NetPerCalendarDayLo = r.MeanLoPC * throughput
				r.NetPerCalendarDayHi = r.MeanHiPC * throughput
			}
			if timeReady && (r.MeanLoPC > 0 || r.MeanHiPC < 0) {
				r.SimulationState = "PRELIMINARY"
			}
		}
		if st.SettledMarkets >= 20 && timeReady && st.Open == 0 && !math.IsInf(rad, 1) {
			r.ConfidenceReady = true
			switch {
			case r.MeanLoPC > 0:
				r.SimulationState = "LOWER-BOUND+"
			case r.MeanHiPC < 0:
				r.SimulationState = "LOWER-BOUND-"
			default:
				r.SimulationState = "COLLECTING"
			}
		}
		if st.Side == "BUNDLE" && st.EconomicSource != "combo-paper" {
			r.ResearchOnly = true
			r.LiveAuthorizes = false
			r.Counterfactual = true
			r.Layer = "research_quote"
			r.EvidenceTier = "counterfactual_bundle_simulation"
			r.Proof = "VOID COUNTERFACTUAL BUNDLE"
			r.VoidReason = "complete-package fill was not observed at an exchange"
			r.CollectionSource = firstNonEmpty(st.EconomicSource, "research_route_bundles")
			r.CollectionReason = "frozen exact-book complete-package evidence; not LIVE authority"
		} else if st.Side == "BUNDLE" {
			r.ResearchOnly, r.Counterfactual = true, true
			r.Layer = "paper_simulation"
			r.EvidenceTier = "funded_paper_simulation"
			r.Proof = "PAPER SIMULATION — NOT EXCHANGE PROFIT EVIDENCE"
			r.VoidReason = "Paper placement and fill are simulated"
			r.CollectionSource = "combo-paper"
			r.CollectionReason = "settled attributed funded Paper simulation; not an exchange fill"
		}
		out = append(out, r)
	}
	// Conservative fee-net profit per calendar day is primary. Cents/share is secondary. A young
	// burst with no defensible Net/d lower bound therefore cannot outrank a slower proven system.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].ProfitEvidence != out[j].ProfitEvidence {
			return out[i].ProfitEvidence
		}
		if out[i].VenueLocked != out[j].VenueLocked {
			return !out[i].VenueLocked // tradeable venues lead; poly-int remains visible below them
		}
		if out[i].NetPerCalendarDayLo != out[j].NetPerCalendarDayLo {
			return out[i].NetPerCalendarDayLo > out[j].NetPerCalendarDayLo
		}
		if out[i].MeanPC != out[j].MeanPC {
			return out[i].MeanPC > out[j].MeanPC
		}
		if out[i].SettledMarkets != out[j].SettledMarkets {
			return out[i].SettledMarkets > out[j].SettledMarkets
		}
		return out[i].Family+out[i].Platform < out[j].Family+out[j].Platform
	})
	return out
}

func takerSystemID(family, platform, side string) string {
	id := "taker:" + strings.TrimSpace(family) + "@" + strings.ToLower(strings.TrimSpace(platform))
	if s := strings.ToUpper(strings.TrimSpace(side)); s == "YES" || s == "NO" {
		id += " [" + s + "]"
	}
	return id
}

func leaderboardCoverageIdentity(family, platform, side, route string) string {
	family = sideSelectionBaseFamily(family)
	return family + "\x00" + strings.ToLower(strings.TrimSpace(platform)) + "\x00" +
		strings.ToUpper(strings.TrimSpace(side)) + "\x00" + strings.ToLower(strings.TrimSpace(route))
}

func r138SystemVenue(id string) string {
	spec, ok := storage.SystemExecutionCapability(id)
	if !ok || len(spec.Venues) == 0 {
		return "unsupported"
	}
	if len(spec.Venues) == 1 {
		return spec.Venues[0]
	}
	return "multi"
}

// r138SystemExecutionShape makes every registered n=0 row explicit. Side is the outcome shape the
// mechanism can hand to execution; route names either its own executable lane or the exact child
// that must consume a router/filter/control. No router or data guard is mislabeled as an order.
func r138SystemExecutionShape(id string) (side, route string) {
	spec, ok := storage.SystemExecutionCapability(id)
	if !ok {
		return "unknown", "fail-closed"
	}
	side = strings.Join(spec.Sides, "/")
	if side == "" {
		side = "no executable side"
	}
	route = strings.Join(spec.Routes, "+")
	if route == "" {
		route = "needs execution adapter"
	}
	return side, route
}

func r138SystemCoverageRows(stats []storage.ResearchSystemCollectionStat) []leaderboardCoverageRow {
	out := make([]leaderboardCoverageRow, 0, len(stats)*4)
	for _, st := range stats {
		spec, registered := storage.SystemExecutionCapability(st.SystemID)
		class, handoff, standalone := researchPromotionApplicability(st.SystemID)
		role, connected, liveHandoff := "unregistered", false, ""
		adapterState, adapterReason := "NEEDS_EXECUTION_ADAPTER", "no system execution capability is registered"
		variants := []storage.SystemExecutionVariant(nil)
		if registered {
			role = string(spec.ActionClass) + "/" + string(spec.HandoffClass)
			adapter := storage.SystemExecutionAdapterStatus(spec)
			connected, adapterState, adapterReason = adapter.Connected, adapter.State, adapter.Reason
			if connected {
				liveHandoff = spec.LiveHandoff
			}
			variants = spec.ExactVariants()
		}
		if len(variants) == 0 {
			side, route := r138SystemExecutionShape(st.SystemID)
			variants = []storage.SystemExecutionVariant{{SystemID: st.SystemID,
				Venue: r138SystemVenue(st.SystemID), Side: side, Route: route}}
		}
		for _, variant := range variants {
			state, note := st.State, st.Reason
			if !connected {
				state, note = adapterState, adapterReason
			} else if strings.TrimSpace(note) == "" {
				// A static order handoff and a live collector are different facts. Keep State as
				// the current collector truth; use the note to describe code-path capability.
				note = adapterReason
			}
			out = append(out, leaderboardCoverageRow{Family: st.SystemID, Group: "system", Layer: "system",
				State: state, Venue: variant.Venue, Side: variant.Side, Route: variant.Route, N: 0,
				Timed: false, ResearchOnly: false, LiveAuthorizes: false,
				CollectionN: st.InputRows, CollectionOpen: st.Open, CollectionCandidates: st.Candidates,
				CollectionCycles: st.InputCycles, CollectionAlerts: st.CollectorAlerts,
				CollectionCurrentCycle:      st.CurrentCycleMatches,
				CollectionCurrentCandidates: st.CurrentCycleCandidates,
				CollectionSource:            strings.Join(st.CollectorIDs, ","),
				PromotionClass:              class, PromotionHandoff: handoff, StandaloneOrder: standalone,
				ExecutionConnected: connected, ExecutionRole: role, LiveHandoff: liveHandoff,
				Note: note})
		}
	}
	return out
}

func r138RegisteredCoverageFallback() []leaderboardCoverageRow {
	ids := storage.SystemIDs()
	stats := make([]storage.ResearchSystemCollectionStat, 0, len(ids))
	for _, id := range ids {
		stats = append(stats, storage.ResearchSystemCollectionStat{SystemID: id, State: "COLLECTING",
			Reason: "compact collector summary is warming; exact candidates, economic rows, and alerts are not loaded yet"})
	}
	return r138SystemCoverageRows(stats)
}

func appendLeaderboardCollectionPlaceholders(rows []leaderboardBacktestRow, coverage []leaderboardCoverageRow) []leaderboardBacktestRow {
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		seen[leaderboardCoverageIdentity(row.Family, row.Platform, row.Side, row.Route)] = true
	}
	for _, meta := range coverage {
		origin := "strategy"
		if meta.Layer == "model" || meta.Group == "taker" {
			origin = "model"
		}
		platform := strings.TrimSpace(meta.Venue)
		if platform == "" {
			platform = "multi"
		}
		identity := leaderboardCoverageIdentity(meta.Family, platform, meta.Side, meta.Route)
		// Exact venue/side/route variants remain visible independently. A settled taker cell replaces
		// only its own placeholder; it must not hide the same system's opposite side or maker/RFQ cell.
		if meta.Family == "" || seen[identity] {
			continue
		}
		rows = append(rows, leaderboardBacktestRow{
			UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: meta.Family, Platform: platform, OriginLayer: origin, Side: meta.Side},
			SystemID:                 meta.Family, Route: meta.Route,
			Layer: meta.Layer, Proof: meta.State, BookNative: false, EconomicsReady: false,
			VenueLocked: meta.VenueLocked, ResearchOnly: meta.ResearchOnly,
			EvidenceTier: "collection_only", FillConditioned: false, ProfitEvidence: false,
			VoidReason: "no settled execution-backed profit evidence", LiveAuthorizes: false,
			CollectionState: meta.State, CollectionN: meta.CollectionN, CollectionOpen: meta.CollectionOpen,
			CollectionCycles: meta.CollectionCycles, CollectionAlerts: meta.CollectionAlerts,
			CollectionCandidates: meta.CollectionCandidates, CollectionSource: meta.CollectionSource,
			CollectionCurrentCycle:      meta.CollectionCurrentCycle,
			CollectionCurrentCandidates: meta.CollectionCurrentCandidates,
			CollectionReason:            meta.Note, PromotionClass: meta.PromotionClass,
			PromotionHandoff: meta.PromotionHandoff, StandaloneOrder: meta.StandaloneOrder,
			ExecutionConnected: meta.ExecutionConnected, ExecutionRole: meta.ExecutionRole,
			LiveHandoff: meta.LiveHandoff,
		})
		seen[identity] = true
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].EconomicsReady != rows[j].EconomicsReady {
			return rows[i].EconomicsReady
		}
		if !rows[i].EconomicsReady {
			return rows[i].Family < rows[j].Family
		}
		if rows[i].VenueLocked != rows[j].VenueLocked {
			return !rows[i].VenueLocked
		}
		return rows[i].NetPerCalendarDay > rows[j].NetPerCalendarDay
	})
	return rows
}

func verdictLayer(v verdictEnt) string {
	switch v.Group {
	case "signal", "invert":
		return "model"
	case "taker", "maker":
		return "execution_route"
	default:
		return "strategy"
	}
}

func verdictSystemID(v verdictEnt) string {
	id := strings.TrimSpace(v.Family)
	if s := strings.ToUpper(strings.TrimSpace(v.Side)); (s == "YES" || s == "NO") &&
		(v.Group == "taker" || v.Group == "maker") {
		id += " [" + s + "]"
	}
	return id
}

func verdictRouteVenue(v verdictEnt) string {
	if v.Group != "taker" && v.Group != "maker" {
		return ""
	}
	if i := strings.LastIndex(v.Family, "@"); i >= 0 && i+1 < len(v.Family) {
		return strings.ToLower(strings.TrimSpace(v.Family[i+1:]))
	}
	return ""
}

// researchSystemCoverageRows keeps every implemented collector/control and every concrete
// specified system visible on the canonical Systems Leaderboard even before it owns a compatible
// timed one-share ledger. Visibility never implies proof: all rows here are non-authorizing.
func researchSystemCoverageRows() []leaderboardCoverageRow {
	row := func(family, venue, layer, state, note string) leaderboardCoverageRow {
		class, handoff, standalone := researchPromotionApplicability(family)
		return leaderboardCoverageRow{Family: family, Group: "research", Layer: layer, State: state,
			Venue: venue, VenueLocked: venue == "polymarket", ResearchOnly: true,
			LiveAuthorizes: false, Timed: false, Note: note, PromotionClass: class,
			PromotionHandoff: handoff, StandaloneOrder: standalone}
	}
	return []leaderboardCoverageRow{
		row("subcent-golf", "kalshi", "strategy", "RESEARCH_ONLY_COLLECTING", "prospective maker/taker golf cohort; no verdict or money authority"),
		row("weather-curve-residual", "kalshi", "model", "RESEARCH_ONLY_NONPROMOTABLE", "market-derived weather-curve diagnostic; permanently nonpromotable"),
		row("weather-curve-revision", "kalshi", "model", "RESEARCH_ONLY_NONPROMOTABLE", "market-derived curve-revision diagnostic; permanently nonpromotable"),
		row("queue-priority", "kalshi", "execution_route", "RESEARCH_ONLY_COLLECTING", "true queue-position and natural fill/cancel execution cohort"),
		row("incentive-maker", "kalshi", "strategy", "RESEARCH_ONLY_COLLECTING", "active incentive terms joined to exact maker books; unearned rewards count as zero EV"),
		row("lifecycle-reopen", "kalshi", "model", "RESEARCH_ONLY_COLLECTING", "fixed-horizon book response after a genuine venue lifecycle transition"),
		row("event-basket-lock@kalshi", "kalshi", "strategy", "RESEARCH_ONLY_COLLECTING", "complete rule-proven Kalshi outcome baskets; non-atomic execution risk remains"),
		row("event-basket-lock@polyus", "polyus", "strategy", "RESEARCH_ONLY_COLLECTING", "complete same-scope PolyUS outcome baskets; non-atomic execution risk remains"),
		row("nested-ladder-lock", "kalshi", "strategy", "RESEARCH_ONLY_COLLECTING", "rule-proven threshold implication; both-leg and unwind risk remain"),
		row("systems-regimes", "", "system", "ANALYTICAL_CONTROL", "7d/30d/90d/all native-ledger decay and proof controller"),
		row("behavioral-bias-regime", "", "system", "RESEARCH_ONLY_NONPROMOTABLE", "complete-day book-v1 behavioral cohorts; discovery only and no order path"),
		row("proper-score-brier", "", "strategy", "RESEARCH_ONLY_COLLECTING", "Brier proper-position transform over fresh unselected forecasts and current executable books; dedicated research ledger only"),
		row("proper-score-log", "", "strategy", "RESEARCH_ONLY_COLLECTING", "log proper-position transform over fresh unselected forecasts and current executable books; dedicated research ledger only"),
		row("proper-score-spherical", "", "strategy", "RESEARCH_ONLY_COLLECTING", "spherical proper-position transform over fresh unselected forecasts and current executable books; dedicated research ledger only"),
		row("independent-probabilistic-weather", "kalshi", "model", "RESEARCH_ONLY_COLLECTING", "official NOAA NBM station/run daily-maximum quantile envelopes mapped to exact NWS-CLI settlement station/date and executable ladders; no money authority"),
		row("time-nested-lock", "kalshi", "strategy", "RESEARCH_ONLY_COLLECTING", "verified immutable time implications + current exact books/fees; non-atomic and void-uncertain routes remain controls"),
		row("joint-marginal-lock", "multi", "strategy", "RESEARCH_ONLY_COLLECTING", "certified baskets + revalidated marginal books/fees; no RFQ is created to manufacture a joint quote"),
		row("fee-rounding-batch", "kalshi", "strategy", "RESEARCH_ONLY_COLLECTING", "actual aggregate-versus-separate fee curves on recent real book-native candidates; optimization research only"),
	}
}

// nativeRegistryCoverageRows makes the current native registry visible before the first terminal
// outcome. These are liveness/ownership rows only: an n=0 placeholder never borrows signal price,
// never becomes PROVEN, and never gains Paper or LIVE authority.
func (s *Server) nativeRegistryCoverageRows(verdicts []verdictEnt) []leaderboardCoverageRow {
	rows := make([]leaderboardCoverageRow, 0, len(policyFamilies)+32)
	for _, pf := range policyFamilies {
		state, reason := s.nativeProducerState(pf.fam)
		venue := "multi"
		if pf.research {
			venue = "polymarket"
		}
		rows = append(rows, leaderboardCoverageRow{Family: pf.fam, Group: "signal", Layer: "model",
			State: state, Venue: venue, VenueLocked: pf.research, ResearchOnly: pf.research,
			Note: reason, CollectionSource: "native signal registry"})
	}
	for _, archived := range []string{"auto-ml", "manual"} {
		state, reason := s.nativeProducerState(archived)
		rows = append(rows, leaderboardCoverageRow{Family: archived, Group: "legacy", Layer: "strategy",
			State: state, Venue: "multi", ResearchOnly: true, Note: reason,
			CollectionSource: "archived native registry"})
	}
	for _, sub := range s.venueBookSubsAll(verdicts) {
		if strings.TrimSpace(sub.Family) == "" {
			continue
		}
		state, reason := s.nativeProducerState(sub.Family)
		research := sub.Book == vbML
		rows = append(rows, leaderboardCoverageRow{Family: sub.Family, Group: "book", Layer: "strategy",
			State: state, Venue: sub.Book, ResearchOnly: research, Note: reason,
			CollectionSource: "current Paper portfolio roster"})
	}
	for _, family := range []string{"rawflow", "xvlock", "maker-fills", "rfq-sim",
		"parlay-2leg", "parlay-3leg", "parlay-4leg", "parlay-5leg", "parlay-6leg"} {
		state, reason := s.nativeProducerState(family)
		rows = append(rows, leaderboardCoverageRow{Family: family, Group: "native", Layer: "strategy",
			State: state, Venue: "multi", ResearchOnly: strings.HasPrefix(family, "parlay-") || family == "rfq-sim",
			Note: reason, CollectionSource: "native system registry"})
	}
	return rows
}

func appendUniqueSystemCoverage(base []leaderboardCoverageRow, extra ...[]leaderboardCoverageRow) []leaderboardCoverageRow {
	seen := make(map[string]bool, len(base))
	for _, r := range base {
		seen[leaderboardCoverageIdentity(r.Family, r.Venue, r.Side, r.Route)] = true
	}
	for _, rows := range extra {
		for _, r := range rows {
			key := leaderboardCoverageIdentity(r.Family, r.Venue, r.Side, r.Route)
			if r.Family == "" || seen[key] {
				continue
			}
			seen[key] = true
			base = append(base, r)
		}
	}
	return base
}

func markTimedResearchCoverage(rows []leaderboardCoverageRow, timedFamily map[string]bool) []leaderboardCoverageRow {
	for i := range rows {
		if !timedFamily[rows[i].Family] {
			continue
		}
		rows[i].Timed = true
		rows[i].LiveAuthorizes = false
		rows[i].Note = "observed-book one-share simulation only; hypothesis may enter corrected forward Paper collection, but this row is not fill-conditioned profit evidence and has no LIVE authority"
	}
	return rows
}

// leaderboardVerdictSnapshot is deliberately non-blocking. The canonical verdict refresh scans
// the large signal ledger and is owned by sweepVerdicts; making a dashboard request synchronously
// refresh an expired 60-second cache made /api/systems-leaderboard inherit that 10-15 second scan.
// A slightly stale canonical snapshot changes no economics or verdict math, and the source/as-of
// receipt below makes the freshness explicit until the regular sweep replaces it.
func (s *Server) leaderboardVerdictSnapshot() (rows []verdictEnt, source, asOf string) {
	s.verdMu.Lock()
	if s.verdCache != nil {
		rows = append([]verdictEnt(nil), s.verdCache...)
		at := s.verdAt
		s.verdMu.Unlock()
		if !at.IsZero() {
			asOf = at.UTC().Format(time.RFC3339)
		}
		return rows, "memory", asOf
	}
	s.verdMu.Unlock()

	var disk struct {
		TS          string       `json:"ts"`
		Experiments []verdictEnt `json:"experiments"`
	}
	if s.readJSONLoose(filepath.Join(s.cfg().DataDir, "verdicts.json"), &disk) && disk.Experiments != nil {
		return disk.Experiments, "persisted", disk.TS
	}
	return nil, "warming", ""
}

// buildSystemLeaderboardRows retains the full historical/funnel report for explicit offline
// diagnostics. Operator request paths use cachedSystemLeaderboardRows below; they must never run
// these multi-table reports synchronously or contend with the feed writers.
func (s *Server) buildSystemLeaderboardRows(ctx context.Context, verdicts []verdictEnt) (
	rows []leaderboardBacktestRow, coverage []leaderboardCoverageRow, timedN int,
	researchErr error, err error,
) {
	stats, err := s.store.UnitTrialLeaderboard(ctx, time.Now().UTC())
	if err != nil {
		return nil, nil, 0, nil, err
	}
	bundleStats, err := s.store.ResearchRouteBundleLeaderboard(ctx, time.Now().UTC())
	if err != nil {
		return nil, nil, 0, nil, err
	}
	stats = append(stats, bundleStats...)
	comboStats, err := s.store.ComboSystemLeaderboard(ctx, time.Now().UTC())
	if err != nil {
		return nil, nil, 0, nil, err
	}
	stats = append(stats, comboStats...)
	timed := make(map[string]bool, len(stats))
	timedFamily := make(map[string]bool, len(stats))
	for _, st := range stats {
		timed[takerSystemID(st.Family, st.Platform, st.Side)] = true
		timedFamily[st.Family] = true
	}
	researchRows := researchSystemCoverageRows()
	r138Stats, r138QueryErr := s.store.ResearchSystemCollectionStats(ctx)
	if r138QueryErr == nil {
		researchRows = appendUniqueSystemCoverage(researchRows, r138SystemCoverageRows(r138Stats))
	}
	if nativeStats, nativeErr := s.queryNativeLockCollectionViews(ctx); nativeErr == nil {
		researchRows = overlayNativeLockCoverageRows(researchRows, nativeStats)
	}
	// Registry fallbacks carry identity only. They never invent economic n or authority.
	researchRows = appendUniqueSystemCoverage(researchRows, r138RegisteredCoverageFallback())
	researchRows = markTimedResearchCoverage(researchRows, timedFamily)
	researchMeta := make(map[string]leaderboardCoverageRow, len(researchRows))
	for _, meta := range researchRows {
		researchMeta[meta.Family] = meta
	}
	coverage = make([]leaderboardCoverageRow, 0, len(verdicts)+len(researchRows)+64)
	for _, v := range verdicts {
		c := leaderboardCoverageRow{Family: v.Family, Side: v.Side, Group: v.Group,
			Layer: verdictLayer(v), State: v.State, N: v.N, Venue: verdictRouteVenue(v),
			VenueLocked: v.Locked, Timed: timed[verdictSystemID(v)], CollectionN: v.N,
			CollectionSource: "prospective signal/verdict ledger"}
		if meta, research := researchMeta[v.Family]; research {
			c.Layer, c.State, c.Venue = meta.Layer, meta.State, meta.Venue
			c.VenueLocked, c.ResearchOnly, c.LiveAuthorizes = meta.VenueLocked, meta.ResearchOnly, false
			c.ExecutionConnected, c.ExecutionRole, c.LiveHandoff = meta.ExecutionConnected, meta.ExecutionRole, meta.LiveHandoff
			c.Timed = c.Timed || meta.Timed
			if c.Timed {
				c.Note = "research system has prospective executable one-share economics; rankable but non-authorizing until sealed promotion"
			} else {
				c.Note = meta.Note
			}
		} else if c.Timed {
			c.Note = "prospective executable one-share timing available"
		} else {
			c.Note = "model/route/strategy is visible, but has no complete timed native-unit ledger yet; excluded rather than proxied with signal prices"
		}
		coverage = append(coverage, c)
	}
	coverage = appendUniqueSystemCoverage(coverage, s.nativeRegistryCoverageRows(verdicts),
		researchRows, adaptiveWhaleCoverageRows())
	for _, row := range coverage {
		if row.Timed {
			timedN++
		}
	}
	rows = leaderboardBacktestRows(stats)
	for i := range rows {
		if meta, ok := researchMeta[rows[i].Family]; ok {
			rows[i].ResearchOnly = meta.ResearchOnly
			rows[i].LiveAuthorizes = false
			rows[i].ExecutionConnected, rows[i].ExecutionRole, rows[i].LiveHandoff =
				meta.ExecutionConnected, meta.ExecutionRole, meta.LiveHandoff
			rows[i].PromotionClass = meta.PromotionClass
			rows[i].PromotionHandoff = meta.PromotionHandoff
			rows[i].StandaloneOrder = meta.StandaloneOrder
		}
	}
	rows = appendLeaderboardCollectionPlaceholders(rows, coverage)
	rows = s.annotateNativeSystemFunnels(ctx, rows, time.Now().UTC())
	return rows, coverage, timedN, r138QueryErr, nil
}

// cachedSystemLeaderboardRows assembles the operator-facing Systems table exclusively from the
// compact research digest's last-good unit-trial aggregate, the immutable verdict snapshot, and
// in-process/static registries. It performs no SQLite report itself. refresh=true merely schedules
// the digest's bounded singleflight worker; the current request always receives the cached snapshot.
func (s *Server) cachedSystemLeaderboardRows(verdicts []verdictEnt, refresh bool) (
	rows []leaderboardBacktestRow, coverage []leaderboardCoverageRow, timedN int,
	digestReady bool, digestErr string,
) {
	var stats []storage.UnitTrialLeaderboardStat
	var researchStats []storage.ResearchSystemCollectionStat
	var nativeStats []nativeLockCollectionView
	if refresh {
		stats, researchStats, nativeStats, digestReady, digestErr = s.researchDigestRows()
	} else {
		stats, researchStats, nativeStats, digestReady, digestErr = s.researchDigestRowsCached()
	}

	timed := make(map[string]bool, len(stats))
	timedFamily := make(map[string]bool, len(stats))
	for _, st := range stats {
		timed[takerSystemID(st.Family, st.Platform, st.Side)] = true
		timedFamily[st.Family] = true
	}
	researchRows := researchSystemCoverageRows()
	if len(researchStats) > 0 {
		researchRows = appendUniqueSystemCoverage(researchRows, r138SystemCoverageRows(researchStats))
	}
	if len(nativeStats) > 0 {
		researchRows = overlayNativeLockCoverageRows(researchRows, nativeStats)
	}
	researchRows = appendUniqueSystemCoverage(researchRows, r138RegisteredCoverageFallback())
	researchRows = markTimedResearchCoverage(researchRows, timedFamily)
	researchMeta := make(map[string]leaderboardCoverageRow, len(researchRows))
	for _, meta := range researchRows {
		researchMeta[meta.Family] = meta
	}

	coverage = make([]leaderboardCoverageRow, 0, len(verdicts)+len(researchRows)+64)
	for _, v := range verdicts {
		c := leaderboardCoverageRow{Family: v.Family, Side: v.Side, Route: v.Route, Group: v.Group,
			Layer: verdictLayer(v), State: v.State, N: v.N, Venue: verdictRouteVenue(v),
			VenueLocked: v.Locked, Timed: timed[verdictSystemID(v)], CollectionN: v.N,
			CollectionSource: "cached prospective signal/verdict ledger"}
		if meta, research := researchMeta[v.Family]; research {
			c.Layer, c.State, c.Venue = meta.Layer, meta.State, meta.Venue
			c.VenueLocked, c.ResearchOnly, c.LiveAuthorizes = meta.VenueLocked, meta.ResearchOnly, false
			c.ExecutionConnected, c.ExecutionRole, c.LiveHandoff = meta.ExecutionConnected, meta.ExecutionRole, meta.LiveHandoff
			c.Timed = c.Timed || meta.Timed
			if c.Route == "" {
				c.Route = meta.Route
			}
			if c.Timed {
				c.Note = "research system has prospective executable one-share economics; rankable but non-authorizing until sealed promotion"
			} else {
				c.Note = meta.Note
			}
		} else if c.Timed {
			c.Note = "prospective executable one-share timing available"
		} else {
			c.Note = "model/route/strategy is visible, but has no complete timed native-unit ledger yet; excluded rather than proxied with signal prices"
		}
		coverage = append(coverage, c)
	}
	coverage = appendUniqueSystemCoverage(coverage, s.nativeRegistryCoverageRows(verdicts),
		researchRows, adaptiveWhaleCoverageRows())
	for _, row := range coverage {
		if row.Timed {
			timedN++
		}
	}

	rows = leaderboardBacktestRows(stats)
	for i := range rows {
		if meta, ok := researchMeta[rows[i].Family]; ok {
			rows[i].ResearchOnly = meta.ResearchOnly
			rows[i].LiveAuthorizes = false
			rows[i].ExecutionConnected, rows[i].ExecutionRole, rows[i].LiveHandoff =
				meta.ExecutionConnected, meta.ExecutionRole, meta.LiveHandoff
			rows[i].PromotionClass = meta.PromotionClass
			rows[i].PromotionHandoff = meta.PromotionHandoff
			rows[i].StandaloneOrder = meta.StandaloneOrder
		}
	}
	rows = appendLeaderboardCollectionPlaceholders(rows, coverage)
	return rows, coverage, timedN, digestReady, digestErr
}

// trackedSystemCountIdentity returns the one trading-system definition and, when available, its
// exact order cell. The leaderboard deliberately contains several views of the same thing:
// model/strategy evidence, strategy:<family>@<venue> aliases, and gf:* funded-portfolio mirrors.
// Those views stay visible, but none of them creates another system or another executable route.
func trackedSystemCountIdentity(row leaderboardBacktestRow) (family, venue, side, route string, count bool) {
	name := strings.TrimSpace(row.Family)
	if name == "" {
		name = strings.TrimSpace(row.SystemID)
	}
	if name == "" {
		return "", "", "", "", false
	}

	venue = strings.ToLower(strings.TrimSpace(row.Platform))
	side = strings.ToUpper(strings.TrimSpace(row.Side))
	route = strings.ToLower(strings.TrimSpace(row.Route))
	family = strings.ToLower(strings.TrimSpace(name))

	// gf:<family>:<side>:<route>-k/-p is the funded Paper mirror of an existing exact
	// system cell, not a new system. Parse the identity from the key so a warming placeholder with
	// blank row fields still de-duplicates against the strategy/model view.
	if strings.HasPrefix(family, "gf:") {
		gf := strings.TrimPrefix(family, "gf:")
		suffixVenue := ""
		switch {
		case strings.HasSuffix(gf, "-k"):
			gf, suffixVenue = strings.TrimSuffix(gf, "-k"), "kalshi"
		case strings.HasSuffix(gf, "-p"):
			gf, suffixVenue = strings.TrimSuffix(gf, "-p"), "polyus"
		}
		parts := strings.Split(gf, ":")
		if len(parts) >= 3 {
			candidateSide := strings.ToUpper(parts[len(parts)-2])
			candidateRoute := strings.ToLower(parts[len(parts)-1])
			if (candidateSide == "YES" || candidateSide == "NO") &&
				(candidateRoute == "maker" || candidateRoute == "taker" || candidateRoute == "rfq") {
				family = strings.Join(parts[:len(parts)-2], ":")
				if side != "YES" && side != "NO" {
					side = candidateSide
				}
				if route != "maker" && route != "taker" && route != "rfq" {
					route = strings.ToLower(candidateRoute)
				}
			} else {
				family = gf
			}
		} else {
			family = gf
		}
		if venue != "kalshi" && venue != "polyus" && suffixVenue != "" {
			venue = suffixVenue
		}
	} else {
		// Portfolio ledger summaries are not trading-system definitions. Their underlying system
		// already appears through its model/strategy/execution identity.
		if strings.HasPrefix(family, "book:") {
			return "", "", "", "", false
		}
		for _, prefix := range []string{"taker:", "maker:", "unit:", "strategy:"} {
			if !strings.HasPrefix(family, prefix) {
				continue
			}
			family = strings.TrimPrefix(family, prefix)
			if route != "maker" && route != "taker" && route != "rfq" {
				switch prefix {
				case "maker:":
					route = "maker"
				case "taker:":
					route = "taker"
				}
			}
			break
		}
	}

	// The alias often carries more exact identity than a warming placeholder's row fields.
	if i := strings.LastIndex(family, " ["); i > 0 && strings.HasSuffix(family, "]") {
		candidate := strings.ToUpper(strings.TrimSuffix(family[i+2:], "]"))
		if side != "YES" && side != "NO" && (candidate == "YES" || candidate == "NO") {
			side = candidate
		}
		family = family[:i]
	}
	if i := strings.LastIndex(family, "@"); i > 0 && i+1 < len(family) {
		candidate := strings.ToLower(strings.TrimSpace(family[i+1:]))
		if venue != "kalshi" && venue != "polyus" && (candidate == "kalshi" || candidate == "polyus") {
			venue = candidate
		}
		family = family[:i]
	}
	family = strings.ToLower(strings.TrimSpace(family))

	// These rows are controls, observers, score-method subledgers, archived sources, or route
	// diagnostics. They remain inspectable on the dashboard but do not own an order decision and
	// therefore cannot inflate either count.
	switch family {
	case "adaptive-whale-scorer", "raw-flow-observer", "systems-regimes",
		"behavioral-bias-regime", "weather-curve-residual", "weather-curve-revision",
		"proper-score-brier", "proper-score-log", "proper-score-spherical",
		"auto-ml", "manual", "maker-fills", "rfq-sim":
		return "", "", "", "", false
	}
	if strings.HasPrefix(family, "counterfactual:") || strings.HasPrefix(family, "side-control:") ||
		(strings.HasPrefix(family, "parlay-") && strings.HasSuffix(family, "leg")) {
		return "", "", "", "", false
	}
	return family, venue, side, route, family != ""
}

func trackedSystemCountRows(rows []leaderboardBacktestRow) (variants, families int) {
	base := make(map[string]struct{}, len(rows))
	distinct := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		family, venue, side, route, count := trackedSystemCountIdentity(row)
		if !count {
			continue
		}
		base[family] = struct{}{}
		// A variant is an exact execution identity, not another view of its evidence. BOTH summaries,
		// unsupported adapters, observers and child labels never manufacture additional order cells.
		if (venue == "kalshi" || venue == "polyus") && (side == "YES" || side == "NO") &&
			(route == "maker" || route == "taker" || route == "rfq") {
			key := strings.Join([]string{family, venue, side, route}, "\x00")
			distinct[key] = struct{}{}
		}
	}
	return len(distinct), len(base)
}

func (s *Server) trackedSystemCounts(ctx context.Context, verdicts []verdictEnt) (variants, families int, ok bool) {
	// The operator-facing total is catalog truth, not "whatever currently has a cached row".
	// Verdicts can warm, expire, or be temporarily absent without creating/deleting a System.
	// Keep trackedSystemCountRows for row-level diagnostics/tests, but never use it as the roster.
	_, _ = ctx, verdicts
	counts := r145CanonicalSystemCatalogCounts()
	return counts.DeclaredTypedVariants, counts.BaseSystems, true
}

func (s *Server) handleLeaderboardBacktest(w http.ResponseWriter, r *http.Request) {
	// Never run the million-row verdict refresh or full unit-trial/funnel reports on this request
	// path. The endpoint serves the compact digest's last-good aggregate and merely schedules its
	// bounded singleflight refresh. Core feed writers cannot be starved by a human opening a tab.
	verdicts, verdictSource, verdictAsOf := s.leaderboardVerdictSnapshot()
	gradedRows, coverage, timedN, digestReady, digestErr := s.cachedSystemLeaderboardRows(verdicts, true)
	promotionBridge, promotionReady := s.researchDigestPromotionStatus()
	catalog := r145CanonicalSystemCatalogCounts()
	writeJSON(w, http.StatusOK, map[string]any{
		"name":                     "Systems Leaderboard",
		"canonical_endpoint":       "/api/systems-leaderboard",
		"umbrella":                 "system is the whole trading stack; each row identifies its measured layer plus origin_layer as model or strategy; models emit signals, strategies decide what action to take, and routes determine whether and how that action fills",
		"basis":                    "one simulated taker share per origin x venue x market x side x episode; observed side ask and visible depth; stored exact fee; actual settlement; actual open-to-close seconds; no exchange order, acknowledgement, or fill was observed",
		"ranked_by":                "execution-backed profit evidence first; historical assumed-fill simulations remain visible only as void diagnostics and never rank as current profit evidence",
		"speed":                    "net_per_occupied_share_day = sum(pnl) / sum(exact held seconds) x 86400; return_per_dollar_day = sum(pnl) / sum((ask+fee)*held seconds) x 86400; both are ratio-of-sums",
		"proof":                    "legacy UnitTrial cents/share and dollars/day are VOID as profit evidence: they assumed a fill from visible depth without an exchange order, acknowledgement, or fill; raw values remain only for historical audit",
		"profit_evidence_contract": "only rows with profit_evidence=true may populate current profit metrics; legacy assumed-fill rows are always false regardless of sample size or confidence interval",
		"rate_interval":            "net_per_calendar_day_lo/hi preserve the observed quiet-time Net/d denominator and replace point cents/share with its contract-sample confidence bounds; canonical-event clusters remain dashboard research only",
		"live_gate":                "table proof is research evidence only and never authorizes a live order; the canonical same-family, same-venue maker/taker route proof and every live risk/freshness gate must separately pass",
		"legacy":                   "pre-book signal-price rows remain available in the legacy Backtest for discovery diagnostics only; they are not blended into this ranking and cannot authorize live execution",
		"rows":                     gradedRows,
		"system_catalog":           r145CanonicalBaseSystems(),
		"variant_signal_contracts": r147SystemVariantSignalContractCounts(),
		"variant_signal_matrix":    r147SystemVariantSignalContracts(),
		"variant_signal_runtime":   s.r147SystemVariantRuntimeSnapshot(r.Context(), time.Now()),
		"count_semantics": map[string]string{
			"base_system":          "one distinct named decision system, control, cohort, or evidence ledger in the canonical catalog; execution roles are reported separately",
			"order_system":         "a base system that can originate an order through a named adapter; controls, cohorts, ledgers, and unsupported product shapes are excluded",
			"typed_variant":        "one exact system + execution venue + YES/NO side + maker/taker/RFQ route; changing venue is a different variant",
			"signal_contract":      "one typed variant or explicitly declared execution derivative plus one exact upstream input topology; this is the runtime tracking denominator",
			"execution_derivative": "an action/TIF contract owned by a parent system but excluded from the 302 base BUY/maker/taker/RFQ typed-variant count; current examples are reduce-only SELL/FOK",
			"bundle_variant":       "one exact additive multi-leg payoff and route; it is counted separately and never fabricated as a single YES/NO contract",
			"producer_capable":     "the exact variant has a named input-to-signal handoff in code; this is not runtime freshness, economics, Paper authority, or LIVE authority",
			"fresh_fed":            "requires a current exact runtime signal receipt and is never inferred from static code connectivity",
			"runtime_state":        "every exact signal contract is current_signal, terminal, excluded, or not_applicable inside the current process freshness window",
			"cross_venue":          "input topology is separate from execution venue; PINT is input-only and only Kalshi or PolyUS may be money destinations",
		},
		"counts": map[string]any{
			"base_systems":                        catalog.BaseSystems,
			"order_originating_base_systems":      catalog.OrderOriginatingBaseSystems,
			"executable_child_overlays":           catalog.ChildOverlayBaseSystems,
			"classified_non_order_bases":          catalog.ClassifiedNonOrderBaseSystems,
			"research_only_venue_bases":           catalog.ResearchVenueBaseSystems,
			"data_or_routing_control_bases":       catalog.DataControlBaseSystems,
			"research_cohort_bases":               catalog.ResearchCohortBaseSystems,
			"unsupported_product_bases":           catalog.UnsupportedProductBaseSystems,
			"frozen_order_systems":                catalog.FrozenBaseSystems,
			"portfolio_or_evidence_ledger_bases":  catalog.PortfolioEvidenceBaseSystems,
			"declared_typed_variants":             catalog.DeclaredTypedVariants,
			"code_path_connected_variants":        catalog.CodePathConnectedVariants,
			"declared_without_code_path_variants": catalog.DeclaredWithoutCodePathVariants,
			"signal_contract_variants":            catalog.SignalContractVariants,
			"execution_derivative_contracts":      catalog.ExecutionDerivativeContracts,
			"producer_capable_typed_variants":     catalog.ProducerCapableTypedVariants,
			"producer_blocked_typed_variants":     catalog.ProducerBlockedTypedVariants,
			"cross_venue_input_variants":          catalog.CrossVenueInputVariants,
			"bundle_product_systems":              catalog.BundleProductSystems,
			"declared_bundle_product_variants":    catalog.DeclaredBundleProductVariants,
			"connected_staged_bundle_variants":    catalog.ConnectedStagedBundleVariants,
			"externally_blocked_bundle_variants":  catalog.ExternallyBlockedBundleVariants,
			// Compatibility aliases both use the narrower code-path-connected denominator.
			"known_exact_execution_variants": catalog.CodePathConnectedVariants,
			// Compatibility alias now reports the narrower code-path-connected denominator.
			// Runtime collector, candidate, economic, Paper and LIVE gates remain separate.
			"exact_execution_variants":               catalog.CodePathConnectedVariants,
			"variant_count_is_lower_bound":           true,
			"missing_typed_variant_systems":          catalog.MissingTypedVariantSystems,
			"missing_typed_variant_system_ids":       catalog.MissingVariantSystemIDs,
			"missing_executable_variants":            catalog.MissingExecutableVariantSystems,
			"missing_executable_system_ids":          catalog.MissingExecutableSystemIDs,
			"externally_blocked_decision_systems":    catalog.ExternallyBlockedDecisionSystems,
			"externally_blocked_decision_system_ids": catalog.ExternallyBlockedDecisionIDs,
			"aliases_excluded":                       catalog.Aliases,
			"components_excluded":                    catalog.Components,
			"visible_leaderboard_rows":               len(gradedRows),
			"contract":                               catalog.VariantCountContract + "; aliases, funded mirrors, diagnostics, Combo result cohorts, and method sub-ledgers are excluded; retained control/cohort/product names are explicitly classified and do not count as order-originating systems",
		},
		"side_selection": sideSelectionReceipt(coverage),
		"promotion_bridge": map[string]any{
			"status": promotionBridge, "cached": promotionReady,
			"single_instrument_dispatch": "sealed matching contract -> normal Paper executor -> accepted Paper receipt -> armed LIVE AUTO with the same route and all-in ceiling",
			"non_order_handoffs":         "multi-leg/RFQ systems use native payoff-certificate routes; routers/filters feed an executable child policy; data controls can promote only a guard",
		},
		"coverage": map[string]any{
			// Coverage rows can include multiple venue/side/route evidence cells for one base
			// System. Never expose this row count under a field named "systems".
			"coverage_rows": len(coverage), "timed": timedN,
			"missing": len(coverage) - timedN, "components": coverage, "strategies": coverage,
			"verdict_source": verdictSource, "verdict_as_of": verdictAsOf,
			"digest_ready": digestReady, "digest_refreshing": !digestReady, "digest_error": digestErr,
		},
	})
}
