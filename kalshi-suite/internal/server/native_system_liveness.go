package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// unitTrialFunnelResult describes what happened after one newly durable signal reached the
// book-native one-share boundary. It is operational telemetry, not statistical evidence.
type unitTrialFunnelResult struct {
	QuoteAttempted, UnitRow, MoneyTruth bool
	UnitRows, MoneyTruthRows            int
	Reason                              string
}

type nativeSignalFunnelEvent struct {
	Eligible, InputRows, QuoteAttempts, UnitRows, MoneyTruth int
	Reason                                                   string
}

type nativeFunnelKey struct{ family, platform, origin string }

type nativeFunnelAggregate struct {
	first, last, lastEconomic                   time.Time
	eligible, inputs, quotes, units, moneyTruth int
	exclusions                                  map[string]int
}

type nativeFunnelState struct {
	sync.Mutex
	lastFlush time.Time
	rows      map[nativeFunnelKey]*nativeFunnelAggregate
}

var nativeFunnelStates sync.Map // map[*Server]*nativeFunnelState

func (s *Server) nativeFunnelState() *nativeFunnelState {
	if v, ok := nativeFunnelStates.Load(s); ok {
		return v.(*nativeFunnelState)
	}
	v, _ := nativeFunnelStates.LoadOrStore(s, &nativeFunnelState{rows: map[nativeFunnelKey]*nativeFunnelAggregate{}})
	return v.(*nativeFunnelState)
}

func nativeFunnelOrigin(sig storage.Signal) string {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sig.ExecExpr)), "strategy-decision/") {
		return "strategy"
	}
	return "model"
}

func nativeFunnelReason(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "no_exclusion"
	}
	v = strings.NewReplacer(" ", "_", ":", "_", "/", "_", "\\", "_", "|", "_").Replace(v)
	if len(v) > 96 {
		v = v[:96]
	}
	return v
}

func mergeNativeFunnelAggregate(dst, src *nativeFunnelAggregate) {
	if dst.first.IsZero() || (!src.first.IsZero() && src.first.Before(dst.first)) {
		dst.first = src.first
	}
	if src.last.After(dst.last) {
		dst.last = src.last
	}
	if src.lastEconomic.After(dst.lastEconomic) {
		dst.lastEconomic = src.lastEconomic
	}
	dst.eligible += src.eligible
	dst.inputs += src.inputs
	dst.quotes += src.quotes
	dst.units += src.units
	dst.moneyTruth += src.moneyTruth
	if dst.exclusions == nil {
		dst.exclusions = map[string]int{}
	}
	for reason, n := range src.exclusions {
		dst.exclusions[reason] += n
	}
}

func (s *Server) noteNativeSystemFunnel(ctx context.Context, sig storage.Signal, ev nativeSignalFunnelEvent) {
	_ = ctx // hot-path aggregation is memory-only; the independent research tick owns durable flushes
	family := strings.TrimSpace(sig.SignalType)
	platform := strings.ToLower(strings.TrimSpace(sig.Platform))
	if family == "" || platform == "" {
		return
	}
	now := time.Now().UTC()
	key := nativeFunnelKey{family: family, platform: platform, origin: nativeFunnelOrigin(sig)}
	st := s.nativeFunnelState()
	st.Lock()
	if st.lastFlush.IsZero() {
		st.lastFlush = now
	}
	a := st.rows[key]
	if a == nil {
		a = &nativeFunnelAggregate{exclusions: map[string]int{}}
		st.rows[key] = a
	}
	if a.first.IsZero() {
		a.first = now
	}
	a.last = now
	a.eligible += ev.Eligible
	a.inputs += ev.InputRows
	a.quotes += ev.QuoteAttempts
	a.units += ev.UnitRows
	a.moneyTruth += ev.MoneyTruth
	if ev.UnitRows > 0 {
		a.lastEconomic = now
	}
	if ev.Reason != "" {
		a.exclusions[nativeFunnelReason(ev.Reason)]++
	}
	st.Unlock()
}

func (s *Server) nativeProducerState(family string) (state, reason string) {
	family = strings.TrimSpace(family)
	base := nativeFunnelBaseFamily(family)
	for _, row := range policyFamilies {
		if row.fam != base {
			continue
		}
		if base == "sharpline" {
			if !s.cfg().Auto.SharplineEnabled {
				return "CONFIG_DISABLED", "sharp-line collector is disabled in current settings"
			}
			if strings.TrimSpace(s.cfg().Auto.OddsAPIKey) == "" {
				return "NEEDS_KEY", "sharp-line collector requires the configured sportsbook API key"
			}
		}
		if row.research {
			return "RESEARCH_ONLY_CURRENT", "current model producer; research venue or research-only route"
		}
		return "CURRENT", "current signal producer feeds the book-native one-share funnel"
	}
	if n, ok := nativeParlayLegs(family); ok {
		if n > parlayLegLimit {
			return "ARCHIVED_BY_6_LEG_LIMIT", fmt.Sprintf("historical %d-leg results remain visible; new combinations are hard-capped at %d legs", n, parlayLegLimit)
		}
		if !s.cfg().Auto.ParlayLabEnabled {
			return "CONFIG_DISABLED", "Combo Lab collection is disabled; historical results remain visible"
		}
		return "RESEARCH_ONLY_CURRENT", "Combo Lab currently collects combinations through the six-leg ceiling"
	}
	switch base {
	case "manual":
		return "ARCHIVED_POLICY", "manual paper order entry was removed; historical rows remain readable"
	case "auto-ml":
		return "ARCHIVED_POLICY", "ML-originated order authority was retired; book-native ML remains research-only"
	case "xcrypto", "kalshi":
		return "ARCHIVED_POLICY", "legacy producer was replaced; historical evidence remains readable"
	case "ml-book", "proper-score-brier", "proper-score-log", "proper-score-spherical", "subcent-golf",
		"weather-curve-residual", "weather-curve-revision", "independent-probabilistic-weather",
		"queue-priority", "incentive-maker", "lifecycle-reopen", "event-basket-lock",
		"nested-ladder-lock", "systems-regimes", "behavioral-bias-regime",
		"raw-flow-observer", "adaptive-whale-scorer":
		return "RESEARCH_ONLY_CURRENT", "current research collector; no Paper or LIVE authority"
	case "rfq-sim":
		return "RESEARCH_ONLY_CURRENT", "current RFQ quote simulator; quote observations never imply a fill or money authority"
	case "time-nested-lock":
		return "RESEARCH_ONLY_CURRENT", "verified ordered-deadline relations are repriced at current side books and exact fees; non-atomic/void-uncertain routes stay controls"
	case "joint-marginal-lock":
		return "RESEARCH_ONLY_CURRENT", "certified event baskets are repriced at current marginal books and fees; the collector never creates an RFQ to fabricate a joint quote"
	case "fee-rounding-batch":
		return "RESEARCH_ONLY_CURRENT", "recent executable unit candidates receive actual aggregate-versus-separate fee curves; savings are optimization controls, not alpha"
	case "rawflow", "weather", "kflow-pre", "kflow-live", "freshinv", "freshinv-k",
		"freshlist-p", "favlong80", "cheapband-k", "cheapband-p", "lockstack", "xvlock",
		"maker-fills":
		return "CURRENT", "current book or execution observer; one-share activity is reported separately"
	case "combo-overlay", "combo-synth", "combo-rfq":
		return "CURRENT", "current Combo portfolio or product-study ledger; RFQ and additive expressions remain separate"
	}
	if strings.HasPrefix(family, "gf:") || strings.HasPrefix(strings.ToLower(family), "follow:") {
		return "CURRENT", "current generalized follower strategy"
	}
	for _, id := range storage.ResearchExperimentIDs() {
		if family == id {
			return "R138_RUNTIME_RECEIPT", "collection truth comes from the system-specific immutable collector receipts"
		}
	}
	return "HISTORICAL_UNMAPPED", "historical result exists, but no current producer contract was found; not called collecting"
}

func nativeParlayLegs(family string) (int, bool) {
	v := strings.ToLower(strings.TrimSpace(family))
	if !strings.HasPrefix(v, "parlay-") || !strings.HasSuffix(v, "leg") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(v, "parlay-"), "leg"))
	return n, err == nil && n > 0
}

func nativeFunnelBaseFamily(family string) string {
	v := strings.TrimSpace(family)
	for _, prefix := range []string{"taker:", "maker:", "unit:"} {
		if strings.HasPrefix(strings.ToLower(v), prefix) {
			v = v[len(prefix):]
			if i := strings.LastIndex(v, "@"); i > 0 {
				v = v[:i]
			}
			break
		}
	}
	for strings.HasPrefix(strings.ToLower(v), "invert:") {
		v = v[len("invert:"):]
	}
	if strings.HasPrefix(strings.ToLower(v), "book:") {
		v = v[len("book:"):]
	}
	for _, suffix := range []string{"@kalshi", "@polyus", "@polymarket"} {
		if strings.HasSuffix(strings.ToLower(v), suffix) {
			v = v[:len(v)-len(suffix)]
			break
		}
	}
	return v
}

func (s *Server) flushNativeSystemFunnels(ctx context.Context, now time.Time, force bool) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	st := s.nativeFunnelState()
	st.Lock()
	if !force && (st.lastFlush.IsZero() || now.Sub(st.lastFlush) < time.Minute) {
		st.Unlock()
		return
	}
	pending := st.rows
	st.rows = map[nativeFunnelKey]*nativeFunnelAggregate{}
	st.lastFlush = now
	st.Unlock()
	if len(pending) == 0 {
		return
	}
	rows := make([]storage.NativeSystemFunnelReceipt, 0, len(pending))
	cycle := "native-system-funnel-" + now.UTC().Format(time.RFC3339Nano)
	for key, a := range pending {
		producerState, producerReason := s.nativeProducerState(key.family)
		if producerState == "HISTORICAL_UNMAPPED" {
			producerState = "CURRENT_UNREGISTERED"
			producerReason = "a current producer reached the central signal funnel, but the system registry is missing this family"
		}
		rows = append(rows, storage.NativeSystemFunnelReceipt{Observed: now, CycleID: cycle,
			Family: key.family, Platform: key.platform, OriginLayer: key.origin,
			ProducerState: producerState, ProducerReason: producerReason,
			Eligible: a.eligible, InputRows: a.inputs, QuoteAttempts: a.quotes,
			UnitRows: a.units, MoneyTruthAttempts: a.moneyTruth,
			Exclusions: a.exclusions, FirstInput: a.first, LastInput: a.last, LastEconomic: a.lastEconomic})
	}
	if err := s.store.InsertNativeSystemFunnelReceipts(ctx, rows); err == nil {
		return
	}
	// A transient busy/deadline must not erase the only liveness receipt. Merge it back into the
	// next bounded batch; the existing signal-write alarm separately reports database health.
	st.Lock()
	for key, src := range pending {
		dst := st.rows[key]
		if dst == nil {
			dst = &nativeFunnelAggregate{exclusions: map[string]int{}}
			st.rows[key] = dst
		}
		mergeNativeFunnelAggregate(dst, src)
	}
	st.Unlock()
}

type nativeSystemCollectionView struct {
	ProducerState      string         `json:"producer_state"`
	CollectionState    string         `json:"collection_state"`
	Reason             string         `json:"reason"`
	LastInputTS        string         `json:"last_input_ts"`
	LastEconomicTS     string         `json:"last_economic_ts"`
	CycleEligible      int            `json:"cycle_eligible"`
	CycleInputRows     int            `json:"cycle_input_rows"`
	CycleQuoteAttempts int            `json:"cycle_quote_attempts"`
	CycleUnitRows      int            `json:"cycle_unit_rows"`
	MoneyTruthAttempts int            `json:"money_truth_attempts"`
	EconomicRows24H    int            `json:"economic_rows_24h"`
	Exclusions         map[string]int `json:"exclusions,omitempty"`
}

func nativeFunnelViewKey(family, platform, origin string) string {
	return family + "\x00" + strings.ToLower(platform) + "\x00" + strings.ToLower(origin)
}

func nativeFunnelRecent(raw string, now time.Time) bool {
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	return err == nil && !t.After(now.Add(time.Minute)) && now.Sub(t) <= 2*time.Hour
}

func (s *Server) annotateNativeSystemFunnels(ctx context.Context, rows []leaderboardBacktestRow, now time.Time) []leaderboardBacktestRow {
	views, err := s.store.NativeSystemFunnelViews(ctx, now)
	if err != nil {
		return rows
	}
	byKey := map[string]storage.NativeSystemFunnelView{}
	for _, v := range views {
		byKey[nativeFunnelViewKey(v.Family, v.Platform, v.OriginLayer)] = v
	}
	for i := range rows {
		state, reason := s.nativeProducerState(rows[i].Family)
		if state == "R138_RUNTIME_RECEIPT" {
			continue
		}
		base := nativeFunnelBaseFamily(rows[i].Family)
		origin := strings.ToLower(strings.TrimSpace(rows[i].OriginLayer))
		if origin != "strategy" {
			origin = "model"
		}
		platform := strings.ToLower(strings.TrimSpace(rows[i].Platform))
		v, ok := byKey[nativeFunnelViewKey(rows[i].Family, platform, origin)]
		if !ok {
			v, ok = byKey[nativeFunnelViewKey(base, platform, origin)]
		}
		if ok && state == "HISTORICAL_UNMAPPED" && v.ProducerState == "CURRENT_UNREGISTERED" {
			state, reason = v.ProducerState, v.ProducerReason
		}
		receiptEconomicFresh := ok && nativeFunnelRecent(v.LastEconomicTS, now)
		if strings.TrimSpace(v.LastEconomicTS) == "" {
			v.LastEconomicTS = rows[i].LastActivity
		}
		collection := state
		if state == "CURRENT" || state == "RESEARCH_ONLY_CURRENT" {
			collection = "READY_NO_RECENT_CANDIDATE"
			if ok && (nativeFunnelRecent(v.LastInputTS, now) || receiptEconomicFresh) {
				collection = "COLLECTING"
			}
		}
		view := &nativeSystemCollectionView{ProducerState: state, CollectionState: collection,
			Reason: reason, LastEconomicTS: v.LastEconomicTS, Exclusions: map[string]int{}}
		if ok {
			view.LastInputTS, view.LastEconomicTS = v.LastInputTS, v.LastEconomicTS
			view.CycleEligible, view.CycleInputRows = v.CycleEligible, v.CycleInputRows
			view.CycleQuoteAttempts, view.CycleUnitRows = v.CycleQuoteAttempts, v.CycleUnitRows
			view.MoneyTruthAttempts, view.EconomicRows24H = v.CycleMoneyTruthAttempts, v.EconomicRows24H
			view.Exclusions = v.Exclusions
		}
		rows[i].NativeCollection = view
	}
	return rows
}
