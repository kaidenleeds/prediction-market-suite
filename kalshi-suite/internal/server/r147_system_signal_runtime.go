package server

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const r147SignalRuntimeFreshAge = 3 * time.Minute

// r147SignalRuntimeReceipt is process-local liveness, not economic evidence. One row belongs to
// one exact signal contract (system + order venue + bought side + route + input topology). The
// five-part identity is deliberately stricter than the old four-part row: K-PINT cannot make a
// K-PUS path look alive and a base-side signal cannot make its independently quoted inverse live.
type r147SignalRuntimeReceipt struct {
	LastChecked time.Time
	LastEvent   time.Time
	LastSignal  time.Time
	Stage       string
	Outcome     string
	Reason      string
	LastError   string
	Ticker      string
	Source      string
	Seen        uint64
}

type r147SystemVariantRuntimeRow struct {
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
	InputTopology       string   `json:"input_topology"`
	InputTopologies     []string `json:"input_topologies"` // compatibility: exactly one entry
	SignalContractID    string   `json:"signal_contract_id"`
	ProducerCapable     bool     `json:"producer_capable"`
	PaperExecution      string   `json:"paper_execution"`
	RealisticPaper      bool     `json:"realistic_paper"`
	State               string   `json:"state"`
	FreshFed            bool     `json:"fresh_fed"`
	CoverageCheckedAt   string   `json:"coverage_checked_at,omitempty"`
	LastEventAt         string   `json:"last_event_at,omitempty"`
	LastSignalAt        string   `json:"last_signal_at,omitempty"`
	SignalAgeSec        int64    `json:"signal_age_seconds,omitempty"`
	LastTicker          string   `json:"last_ticker,omitempty"`
	ReceiptSource       string   `json:"receipt_source,omitempty"`
	Outcome             string   `json:"outcome,omitempty"`
	Observed            bool     `json:"observed_this_process"`
	ObservedEvents      uint64   `json:"observed_events_this_process"`
	Blocker             string   `json:"blocker,omitempty"`
}

type r147SystemVariantRuntimeSnapshot struct {
	AsOf                    string                        `json:"as_of"`
	FreshnessSeconds        int64                         `json:"freshness_seconds"`
	RefreshCurrent          bool                          `json:"refresh_current"`
	LastRefreshAt           string                        `json:"last_refresh_at,omitempty"`
	RefreshAgeSeconds       int64                         `json:"refresh_age_seconds,omitempty"`
	TypedVariants           int                           `json:"typed_variants"`
	SignalContracts         int                           `json:"signal_contracts"`
	DerivativeContracts     int                           `json:"derivative_contracts"`
	CurrentSignal           int                           `json:"current_signal"`
	Terminal                int                           `json:"terminal"`
	Excluded                int                           `json:"excluded"`
	NotApplicable           int                           `json:"not_applicable"`
	FreshFed                int                           `json:"fresh_fed"`
	Stale                   int                           `json:"stale"`      // compatibility subset of not_applicable
	NeverSeen               int                           `json:"never_seen"` // compatibility subset of not_applicable
	ProducerBlocked         int                           `json:"producer_blocked"`
	ObservedContracts       int                           `json:"observed_contracts_this_process"`
	UnobservedContracts     int                           `json:"unobserved_contracts_this_process"`
	ObservedEvents          uint64                        `json:"observed_events_this_process"`
	RealisticPaperContracts int                           `json:"realistic_paper_contracts"`
	LegacyOrOtherContracts  int                           `json:"legacy_or_other_paper_contracts"`
	ByPaperExecution        map[string]int                `json:"by_paper_execution"`
	Rows                    []r147SystemVariantRuntimeRow `json:"rows"`
}

func r147SignalRuntimeKey(systemID, venue, side, route, topology string) string {
	return strings.Join([]string{strings.ToLower(strings.TrimSpace(systemID)),
		strings.ToLower(strings.TrimSpace(venue)), strings.ToUpper(strings.TrimSpace(side)),
		strings.ToLower(strings.TrimSpace(route)), strings.ToUpper(strings.TrimSpace(topology))}, "\x00")
}

func r147ContractPaperExecution(c r147SystemVariantSignalContract) (string, bool) {
	// Current taker routes reach the delayed two-touch, final-wire IOC simulator. Maker and
	// book-native ML shapes remain visible but log-only until they gain an equally honest route.
	if c.ExecutionDerivative && strings.EqualFold(c.SystemID, "proper-score-momentum") &&
		strings.EqualFold(c.Action, "SELL") && strings.EqualFold(c.Route, "taker") &&
		strings.EqualFold(c.TimeInForce, "fill_or_kill") && c.ReduceOnly {
		return "delayed_newer_bid_sell_fok", true
	}
	if strings.EqualFold(c.Handoff, r145HandoffGenericTaker) && strings.EqualFold(c.Route, "taker") {
		return "delayed_two_touch", true
	}
	// R165 permanently retired the FreshInv and XVGap specialist placement branches. Their static
	// handoff names remain for historical audit, but current taker opportunities are deliberately
	// routed through the generic delayed two-touch executor.
	if (strings.EqualFold(c.Handoff, r145HandoffFreshInv) ||
		strings.EqualFold(c.Handoff, r145HandoffXVGap)) && strings.EqualFold(c.Route, "taker") {
		return "delayed_two_touch", true
	}
	// A payoff-constraint candidate names the YES side of one venue-created conjunction. Its
	// current Paper path can see the first authenticated RFQ quote, but it has no delayed/newer
	// execution check yet. R166 therefore keeps it visible and explicitly log-only; a first quote
	// must never be described as realistic execution.
	if strings.EqualFold(c.SystemID, "payoff-constraint-solver") &&
		strings.EqualFold(c.Route, "rfq") && strings.EqualFold(c.Side, "YES") {
		return "log_only_rfq_not_observed", false
	}
	if _, registered := storage.SystemExecutionCapability(c.SystemID); registered {
		if strings.EqualFold(c.Route, "taker") {
			return "delayed_two_touch", true
		}
		return "log_only_registered_non_taker", false
	}
	switch strings.ToLower(strings.TrimSpace(c.Handoff)) {
	case r145HandoffNativeMaker:
		return "log_only_maker", false
	case r145HandoffRawFlow, r145HandoffDedicatedBook:
		if strings.EqualFold(c.Route, "taker") {
			return "delayed_two_touch", true
		}
		return "log_only_maker", false
	case r145HandoffMLBook:
		return "log_only_book_native_ml", false
	default:
		return "log_only_not_applicable", false
	}
}

func r147ContractMatches(c r147SystemVariantSignalContract, systemID, venue, side, route, topology string) bool {
	if !c.ProducerCapable || !strings.EqualFold(c.SystemID, systemID) ||
		!strings.EqualFold(c.ExecutionVenue, venue) || !strings.EqualFold(c.Side, side) ||
		!strings.EqualFold(c.InputTopology, topology) {
		return false
	}
	return route == "*" || strings.EqualFold(c.Route, route)
}

func r147ContractsForRuntimeEvent(systemID, venue, side, route, topology string) []r147SystemVariantSignalContract {
	systemID = strings.ToLower(strings.TrimSpace(systemID))
	venue = strings.ToLower(strings.TrimSpace(venue))
	side = strings.ToUpper(strings.TrimSpace(side))
	route = strings.ToLower(strings.TrimSpace(route))
	topology = strings.ToUpper(strings.TrimSpace(topology))
	if systemID == "" || (venue != "kalshi" && venue != "polyus") ||
		(side != "YES" && side != "NO") || (route != "*" && route != "maker" && route != "taker" && route != "rfq") {
		return nil
	}
	all := r147SystemVariantSignalContracts()
	if route == "*" {
		// A route-less detector receipt is exact only when the matching contract has one unique
		// order route. In particular, a generic signal must never make both the maker and taker
		// rows look live. Callers at a real maker/taker/RFQ boundary must name that route.
		uniqueRoute := ""
		for _, c := range all {
			if !c.ProducerCapable || !strings.EqualFold(c.SystemID, systemID) ||
				!strings.EqualFold(c.ExecutionVenue, venue) || !strings.EqualFold(c.Side, side) {
				continue
			}
			if topology != "" && !strings.EqualFold(c.InputTopology, topology) {
				continue
			}
			candidate := strings.ToLower(strings.TrimSpace(c.Route))
			if uniqueRoute == "" {
				uniqueRoute = candidate
			} else if uniqueRoute != candidate {
				return nil
			}
		}
		if uniqueRoute == "" {
			return nil
		}
		route = uniqueRoute
	}
	if topology == "" {
		// Missing topology is safe to recover only when every matching exact route has one and the
		// same topology. Multi-input systems stay unmeasured instead of being pooled.
		unique := ""
		for _, c := range all {
			if !c.ProducerCapable || !strings.EqualFold(c.SystemID, systemID) ||
				!strings.EqualFold(c.ExecutionVenue, venue) || !strings.EqualFold(c.Side, side) ||
				!strings.EqualFold(c.Route, route) {
				continue
			}
			if unique == "" {
				unique = strings.ToUpper(c.InputTopology)
			} else if unique != strings.ToUpper(c.InputTopology) {
				return nil
			}
		}
		topology = unique
	}
	if topology == "" {
		return nil
	}
	out := make([]r147SystemVariantSignalContract, 0, 2)
	for _, c := range all {
		if r147ContractMatches(c, systemID, venue, side, route, topology) {
			out = append(out, c)
		}
	}
	return out
}

func (s *Server) noteR147SignalContractRuntime(systemID, venue, side, route, topology,
	ticker, source, stage, outcome, reason string, at time.Time, durable bool, lastErr string) {
	if s == nil || at.IsZero() {
		return
	}
	contracts := r147ContractsForRuntimeEvent(systemID, venue, side, route, topology)
	if len(contracts) == 0 {
		return
	}
	stage = strings.ToLower(strings.TrimSpace(stage))
	if stage != "signal" && stage != "terminal" && stage != "excluded" {
		return
	}
	at = at.UTC()
	s.r147SignalRuntimeMu.Lock()
	if s.r147SignalRuntime == nil {
		s.r147SignalRuntime = map[string]r147SignalRuntimeReceipt{}
	}
	for _, c := range contracts {
		key := r147SignalRuntimeKey(c.SystemID, c.ExecutionVenue, c.Side, c.Route, c.InputTopology)
		r := s.r147SignalRuntime[key]
		// A delayed terminal may race a repeated signal scan. Never let an older event replace a
		// newer state, but do retain the newest exact signal clock independently.
		if stage == "signal" && at.After(r.LastSignal) {
			r.LastSignal = at
		}
		if r.LastEvent.IsZero() || !at.Before(r.LastEvent) {
			r.LastEvent, r.Stage, r.Outcome, r.Reason = at, stage,
				strings.TrimSpace(outcome), strings.TrimSpace(reason)
			r.LastError, r.Ticker, r.Source = strings.TrimSpace(lastErr),
				strings.TrimSpace(ticker), strings.TrimSpace(source)
		}
		if durable {
			r.Seen++
		}
		s.r147SignalRuntime[key] = r
	}
	s.r147SignalRuntimeMu.Unlock()
}

// noteR147SignalRuntime retains the old call shape for dedicated producers with one unambiguous
// input topology. It refuses a multi-topology family; those callers must use the Signal helper.
func (s *Server) noteR147SignalRuntime(systemID, venue, side, route, ticker, source string,
	at time.Time, durable bool, lastErr string) {
	s.noteR147SignalContractRuntime(systemID, venue, side, route, "", ticker, source,
		"signal", "", "", at, durable, lastErr)
}

func (s *Server) noteR147SignalRuntimeForSignal(systemID string, sig storage.Signal, route,
	source string, at time.Time, durable bool, lastErr string) {
	s.noteR147SignalContractRuntime(systemID, sig.Platform, sig.Side, route,
		r147SignalInputTopology(sig), sig.Ticker, source, "signal", "", "", at, durable, lastErr)
}

func (s *Server) noteR147NativeSignalRuntime(sig storage.Signal, at time.Time, durable bool, lastErr string) {
	// Current native detector rows feed the canonical delayed taker observer. A maker route needs
	// its own post/queue receipt and cannot inherit liveness from the same raw signal.
	s.noteR147SignalRuntimeForSignal(sig.SignalType, sig, "taker", "insertSignal", at, durable, lastErr)
}

func (s *Server) noteR147ResearchCandidateRuntime(c storage.ResearchPromotionCandidate) {
	topology := ""
	if strings.EqualFold(c.SystemID, "proper-score-executor") {
		transform, ok := properScoreTransformFromCohort(c.Cohort)
		if !ok {
			return
		}
		topology = properScorePaperInputTopology(c.Venue, transform)
		if topology == "" {
			return
		}
	}
	s.noteR147SignalContractRuntime(c.SystemID, c.Venue, c.Side, c.Route, topology, c.Ticker,
		"research_system_observations:candidate", "signal", "", "", c.Observed, true, "")
}

// noteR147PaperRouteOutcome is observability only. It is called from the generic executor's
// existing deferred audit after all routing decisions have already been made.
func (s *Server) noteR147PaperRouteOutcome(sig storage.Signal, route, state, reason string, at time.Time) {
	stage := "terminal"
	if strings.EqualFold(strings.TrimSpace(state), "REJECTED") {
		stage = "excluded"
	}
	s.noteR147SignalContractRuntime(sig.SignalType, sig.Platform, sig.Side, route,
		r147SignalInputTopology(sig), sig.Ticker, "realistic-paper-route", stage,
		strings.TrimSpace(state), strings.TrimSpace(reason), at, true, "")
}

func (s *Server) noteR147InverseOpportunity(sig storage.Signal, result unitTrialFunnelResult, at time.Time) {
	stage, outcome := "excluded", "complete_book_unavailable"
	if result.MoneyTruth {
		stage, outcome = "signal", "complete_book_current"
	}
	s.noteR147SignalContractRuntime(sig.SignalType, sig.Platform, sig.Side, "taker",
		r147SignalInputTopology(sig), sig.Ticker, "independent-inverse-complete-book", stage,
		outcome, result.Reason, at, true, "")
}

func (s *Server) r147SignalRuntimeSnapshotMap() map[string]r147SignalRuntimeReceipt {
	out := map[string]r147SignalRuntimeReceipt{}
	if s == nil {
		return out
	}
	s.r147SignalRuntimeMu.Lock()
	for key, value := range s.r147SignalRuntime {
		out[key] = value
	}
	s.r147SignalRuntimeMu.Unlock()
	return out
}

// refreshR147SystemContractCoverage is intentionally a five-second, cache-only pass. It performs
// no database or network work. Creating a checked row for every exact contract makes absence an
// explicit not_applicable state instead of an invented "connected" signal.
func (s *Server) refreshR147SystemContractCoverage(now time.Time) {
	if s == nil {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	contracts := r147SystemVariantSignalContracts()
	s.r147SignalRuntimeMu.Lock()
	if s.r147SignalRuntime == nil {
		s.r147SignalRuntime = map[string]r147SignalRuntimeReceipt{}
	}
	for _, c := range contracts {
		key := r147SignalRuntimeKey(c.SystemID, c.ExecutionVenue, c.Side, c.Route, c.InputTopology)
		r := s.r147SignalRuntime[key]
		r.LastChecked = now
		s.r147SignalRuntime[key] = r
	}
	s.r147SignalRuntimeMu.Unlock()
}

func (s *Server) r147SystemVariantRuntimeSnapshot(_ context.Context, now time.Time) r147SystemVariantRuntimeSnapshot {
	if now.IsZero() {
		now = time.Now()
	}
	now = now.UTC()
	receipts := s.r147SignalRuntimeSnapshotMap()
	contracts := r147SystemVariantSignalContracts()
	out := r147SystemVariantRuntimeSnapshot{AsOf: now.Format(time.RFC3339Nano),
		FreshnessSeconds: int64(r147SignalRuntimeFreshAge.Seconds()),
		TypedVariants:    len(r145KnownSystemVariants()), SignalContracts: len(contracts),
		ByPaperExecution: make(map[string]int),
		Rows:             make([]r147SystemVariantRuntimeRow, 0, len(contracts))}
	var newestRefresh time.Time
	for _, c := range contracts {
		key := r147SignalRuntimeKey(c.SystemID, c.ExecutionVenue, c.Side, c.Route, c.InputTopology)
		r := receipts[key]
		paperExecution, realistic := r147ContractPaperExecution(c)
		out.ByPaperExecution[paperExecution]++
		row := r147SystemVariantRuntimeRow{SystemID: c.SystemID, ParentSystemID: c.ParentSystemID,
			ExecutionVenue: c.ExecutionVenue, Side: c.Side, Action: c.Action, Route: c.Route,
			TimeInForce: c.TimeInForce, ReduceOnly: c.ReduceOnly,
			ExecutionDerivative: c.ExecutionDerivative, LiveAuthority: c.LiveAuthority,
			InputTopology:   c.InputTopology,
			InputTopologies: []string{c.InputTopology}, SignalContractID: r147SignalContractIdentity(c),
			ProducerCapable: c.ProducerCapable, PaperExecution: paperExecution, RealisticPaper: realistic}
		if c.ExecutionDerivative {
			out.DerivativeContracts++
		}
		if realistic {
			out.RealisticPaperContracts++
		} else {
			out.LegacyOrOtherContracts++
		}
		if r.LastChecked.After(newestRefresh) {
			newestRefresh = r.LastChecked
		}
		if !r.LastChecked.IsZero() {
			row.CoverageCheckedAt = r.LastChecked.Format(time.RFC3339Nano)
		}
		if !r.LastEvent.IsZero() {
			row.LastEventAt = r.LastEvent.Format(time.RFC3339Nano)
			row.LastTicker, row.ReceiptSource, row.Outcome = r.Ticker, r.Source, r.Outcome
		}
		row.ObservedEvents = r.Seen
		row.Observed = r.Seen > 0
		out.ObservedEvents += r.Seen
		if row.Observed {
			out.ObservedContracts++
		} else {
			out.UnobservedContracts++
		}
		if !r.LastSignal.IsZero() {
			row.LastSignalAt = r.LastSignal.Format(time.RFC3339Nano)
			row.SignalAgeSec = int64(now.Sub(r.LastSignal).Seconds())
			row.FreshFed = now.Sub(r.LastSignal) >= 0 && now.Sub(r.LastSignal) <= r147SignalRuntimeFreshAge
			if row.FreshFed {
				out.FreshFed++
			}
		}
		freshEvent := !r.LastEvent.IsZero() && now.Sub(r.LastEvent) >= 0 &&
			now.Sub(r.LastEvent) <= r147SignalRuntimeFreshAge
		switch {
		case !c.ProducerCapable:
			row.State, row.Blocker = "not_applicable", "no exact code-level producer for this contract"
			out.NotApplicable++
			out.ProducerBlocked++
		case freshEvent && r.LastError != "":
			row.State, row.Blocker = "excluded", r.LastError
			out.Excluded++
		case freshEvent && r.Stage == "terminal":
			row.State, row.Blocker = "terminal", r.Reason
			out.Terminal++
		case freshEvent && r.Stage == "excluded":
			row.State, row.Blocker = "excluded", r.Reason
			out.Excluded++
		case freshEvent && r.Stage == "signal":
			row.State = "current_signal"
			out.CurrentSignal++
		default:
			row.State = "not_applicable"
			out.NotApplicable++
			if r.LastEvent.IsZero() {
				row.Blocker = "no exact matching opportunity observed in this process"
				out.NeverSeen++
			} else {
				row.Blocker = "no exact matching opportunity inside the current freshness window"
				out.Stale++
			}
		}
		out.Rows = append(out.Rows, row)
	}
	if !newestRefresh.IsZero() {
		age := now.Sub(newestRefresh)
		out.LastRefreshAt = newestRefresh.Format(time.RFC3339Nano)
		out.RefreshAgeSeconds = int64(age.Seconds())
		out.RefreshCurrent = age >= 0 && age <= 15*time.Second
	}
	sort.Slice(out.Rows, func(i, j int) bool {
		a, b := out.Rows[i], out.Rows[j]
		return strings.Join([]string{a.SystemID, a.ExecutionVenue, a.Side, a.Route, a.InputTopology}, "\x00") <
			strings.Join([]string{b.SystemID, b.ExecutionVenue, b.Side, b.Route, b.InputTopology}, "\x00")
	})
	return out
}
