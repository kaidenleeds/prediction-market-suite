package server

// Poly-int is a research quote source, not a live execution venue.  Gamma/event discovery stays
// broad and lightweight, while this planner bounds the expensive CLOB market-channel working set
// to the conditions most likely to affect research decisions.  A condition consumes one seat and
// contributes every known outcome token (normally the distinct YES and NO/complement tokens).

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

const (
	// Broad discovery stays separate from this bounded executable-depth tier. The official channel
	// is uncapped; 600 priority conditions / up to 1,200 outcome tokens maximizes useful cross-venue
	// coverage without recreating the prior full-catalog subscription flood. Startup reconnect churn
	// is observed through connection receipts and must be judged only after a full warm rotation.
	pintCLOBConditionCap = 600
	pintSignalSeatTTL    = 24 * time.Hour
	pintSignalSeatLimit  = 2400
)

type pintBookCandidate struct {
	ConditionID string
	Slug        string
	Tokens      []string
	ResolveAt   time.Time
	Volume      float64
	Held        bool
	Arb         bool
	Signal      bool
	Near        bool
	MatchK      bool
	MatchPUS    bool
	Retained    bool // last selected seat carried through a non-authoritative partial refresh
}

type pintBookPlanStats struct {
	Cap                    int       `json:"cap_conditions"`
	Universe               int       `json:"gamma_universe"`
	Eligible               int       `json:"eligible_conditions"`
	Selected               int       `json:"selected_conditions"`
	RequestedAssets        int       `json:"requested_token_assets"`
	Held                   int       `json:"held"`
	HeldSelected           int       `json:"held_selected"`
	ActiveArb              int       `json:"active_arb"`
	ActiveArbSelected      int       `json:"active_arb_selected"`
	Matched                int       `json:"matched"`
	MatchedSelected        int       `json:"matched_selected"`
	ActiveSignal           int       `json:"active_signal"`
	ActiveSignalSelected   int       `json:"active_signal_selected"`
	NearHorizon            int       `json:"near_horizon"`
	NearHorizonSelected    int       `json:"near_horizon_selected"`
	KPair                  int       `json:"k_pint"`
	KPairSelected          int       `json:"k_pint_selected"`
	PUSPair                int       `json:"pus_pint"`
	PUSPairSelected        int       `json:"pus_pint_selected"`
	ThreeWay               int       `json:"three_way"`
	ThreeWaySelected       int       `json:"three_way_selected"`
	VolumeFillSelected     int       `json:"volume_fill_selected"`
	Tokenless              int       `json:"tokenless"`
	IncompleteOutcomeToken int       `json:"incomplete_outcome_tokens"`
	RequiredShortfall      int       `json:"required_shortfall"`
	RetainedSelected       int       `json:"retained_selected"`
	HeadRows               int       `json:"volume_head_rows"`
	HeadAgeSec             float64   `json:"volume_head_age_sec"`
	HeadRefreshing         bool      `json:"volume_head_refreshing"`
	CompleteCatalogRows    int       `json:"complete_catalog_rows"`
	CompleteCatalogAgeSec  float64   `json:"complete_catalog_age_sec"`
	CompleteCatalogAt      time.Time `json:"complete_catalog_at,omitempty"`
	CatalogRefreshing      bool      `json:"complete_catalog_refreshing"`
	CatalogCheckpointRows  int       `json:"catalog_checkpoint_rows"`
	CatalogCheckpointPages int       `json:"catalog_checkpoint_pages"`
	CatalogCheckpointLimit int       `json:"catalog_checkpoint_limit"`
	CatalogLastError       string    `json:"catalog_last_error,omitempty"`
	PlanSource             string    `json:"plan_source"`
	RegularHorizonHours    float64   `json:"regular_horizon_hours"`
	CryptoHorizonHours     float64   `json:"crypto_horizon_hours"`
	FullCatalogDiscovery   bool      `json:"full_gamma_catalog_discovery"`
	ResearchOnly           bool      `json:"research_only"`
	LiveAuthorizing        bool      `json:"live_authorizing"`
	At                     time.Time `json:"at"`
}

type pintBookPlan struct {
	Conditions []string
	Assets     []string
	Selected   []pintBookCandidate
	Stats      pintBookPlanStats
}

func pintCandidateRank(c pintBookCandidate) int {
	switch {
	case c.Held:
		return 0 // settlement/mark continuity cannot be evicted by discovery
	case c.Arb:
		return 1 // a currently open cross-venue lock observation
	case c.MatchK || c.MatchPUS:
		if c.Near {
			return 2
		}
		return 3
	case c.Signal:
		if c.Near {
			return 4
		}
		return 5
	case c.Near:
		return 6 // cover every active execution horizon before volume-only discovery
	case c.Retained:
		return 7 // partial refresh: preserve last-good seats before rotating volume fillers
	default:
		return 8
	}
}

func cleanPintTokens(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, id := range in {
		if id = strings.TrimSpace(id); id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func planPintCLOBBooks(max, universe int, in []pintBookCandidate, regularH, cryptoH float64, now time.Time) pintBookPlan {
	p := pintBookPlan{Stats: pintBookPlanStats{Cap: max, Universe: universe,
		RegularHorizonHours: regularH, CryptoHorizonHours: cryptoH,
		ResearchOnly: true, LiveAuthorizing: false, At: now}}
	cs := append([]pintBookCandidate(nil), in...)
	for i := range cs {
		cs[i].ConditionID = strings.TrimSpace(cs[i].ConditionID)
		cs[i].Tokens = cleanPintTokens(cs[i].Tokens)
	}
	sort.SliceStable(cs, func(i, j int) bool {
		ri, rj := pintCandidateRank(cs[i]), pintCandidateRank(cs[j])
		if ri != rj {
			return ri < rj
		}
		if !cs[i].ResolveAt.IsZero() || !cs[j].ResolveAt.IsZero() {
			if cs[i].ResolveAt.IsZero() != cs[j].ResolveAt.IsZero() {
				return !cs[i].ResolveAt.IsZero()
			}
			if !cs[i].ResolveAt.Equal(cs[j].ResolveAt) {
				return cs[i].ResolveAt.Before(cs[j].ResolveAt)
			}
		}
		if cs[i].Volume != cs[j].Volume {
			return cs[i].Volume > cs[j].Volume
		}
		return cs[i].ConditionID < cs[j].ConditionID
	})
	selected := map[string]bool{}
	required := map[string]bool{}
	for _, c := range cs {
		if c.ConditionID == "" {
			continue
		}
		p.Stats.Eligible++
		matched := c.MatchK || c.MatchPUS
		if c.Held {
			p.Stats.Held++
		}
		if c.Arb {
			p.Stats.ActiveArb++
		}
		if matched {
			p.Stats.Matched++
		}
		if c.Signal {
			p.Stats.ActiveSignal++
		}
		if c.Near {
			p.Stats.NearHorizon++
		}
		if c.MatchK {
			p.Stats.KPair++
		}
		if c.MatchPUS {
			p.Stats.PUSPair++
		}
		if c.MatchK && c.MatchPUS {
			p.Stats.ThreeWay++
		}
		if c.Held || c.Arb || matched || c.Signal || c.Near {
			required[c.ConditionID] = true
		}
		if len(c.Tokens) == 0 {
			p.Stats.Tokenless++
			continue
		}
		if len(c.Tokens) < 2 {
			p.Stats.IncompleteOutcomeToken++
		}
		if len(p.Conditions) >= max || selected[c.ConditionID] {
			continue
		}
		selected[c.ConditionID] = true
		p.Conditions = append(p.Conditions, c.ConditionID)
		p.Assets = append(p.Assets, c.Tokens...)
		p.Selected = append(p.Selected, c)
		if c.Held {
			p.Stats.HeldSelected++
		}
		if c.Arb {
			p.Stats.ActiveArbSelected++
		}
		if matched {
			p.Stats.MatchedSelected++
		}
		if c.Signal {
			p.Stats.ActiveSignalSelected++
		}
		if c.Near {
			p.Stats.NearHorizonSelected++
		}
		if c.MatchK {
			p.Stats.KPairSelected++
		}
		if c.MatchPUS {
			p.Stats.PUSPairSelected++
		}
		if c.MatchK && c.MatchPUS {
			p.Stats.ThreeWaySelected++
		}
		if c.Retained {
			p.Stats.RetainedSelected++
		}
		if pintCandidateRank(c) == 8 {
			p.Stats.VolumeFillSelected++
		}
	}
	p.Stats.Selected, p.Stats.RequestedAssets = len(p.Conditions), len(p.Assets)
	for id := range required {
		if !selected[id] {
			p.Stats.RequiredShortfall++
		}
	}
	return p
}

func stringSetHas(set map[string]bool, m polymarket.Market) bool {
	return set[m.ConditionID] || set[m.Slug] || set[strings.ToLower(m.Slug)]
}

func marketOutcomeTokens(raw string) []string {
	var ids []string
	_ = json.Unmarshal([]byte(raw), &ids)
	return cleanPintTokens(ids)
}

func pintSnapshotAgeSec(now, at time.Time) float64 {
	if at.IsZero() {
		return -1
	}
	age := now.Sub(at).Seconds()
	if age < 0 {
		return 0
	}
	return age
}

func stampPintCatalogTelemetry(plan *pintBookPlan, head, complete polymarket.CatalogSnapshot, now time.Time, source string) {
	plan.Stats.At = now
	plan.Stats.HeadRows = len(head.Markets)
	plan.Stats.HeadAgeSec = pintSnapshotAgeSec(now, head.At)
	plan.Stats.HeadRefreshing = head.Refreshing
	plan.Stats.CompleteCatalogRows = len(complete.Markets)
	plan.Stats.CompleteCatalogAgeSec = pintSnapshotAgeSec(now, complete.At)
	plan.Stats.CompleteCatalogAt = complete.At
	plan.Stats.CatalogRefreshing = complete.Refreshing
	plan.Stats.CatalogCheckpointRows = complete.CheckpointRows
	plan.Stats.CatalogCheckpointPages = complete.CheckpointPage
	plan.Stats.CatalogCheckpointLimit = complete.CheckpointLimit
	plan.Stats.CatalogLastError = complete.LastError
	plan.Stats.FullCatalogDiscovery = complete.Complete
	plan.Stats.PlanSource = source
}

// mergePintCatalog keeps the completed keyset snapshot as the sole breadth authority and uses a
// newer volume head only to overlay rows already present there. Head-only rows wait for the next
// complete crawl, so neither a partial refresh nor a stale cache can resurrect removed markets.
func mergePintCatalog(head, complete polymarket.CatalogSnapshot) []polymarket.Market {
	base := head.Markets
	if complete.Complete {
		base = complete.Markets
	}
	byID := make(map[string]polymarket.Market, len(base)+len(head.Markets))
	order := make([]string, 0, len(base)+len(head.Markets))
	put := func(m polymarket.Market, add bool) {
		id := strings.TrimSpace(m.ConditionID)
		if id == "" {
			return
		}
		if _, exists := byID[id]; !exists {
			if !add {
				return
			}
			order = append(order, id)
		}
		byID[id] = m
	}
	for _, m := range base {
		put(m, true)
	}
	if complete.Complete && head.At.After(complete.At) {
		for _, m := range head.Markets {
			id := strings.TrimSpace(m.ConditionID)
			if _, exists := byID[id]; exists {
				byID[id] = m // fresher lifecycle/volume overlay; never breadth authority
			}
		}
	}
	out := make([]polymarket.Market, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Volume24hr != out[j].Volume24hr {
			return out[i].Volume24hr > out[j].Volume24hr
		}
		return out[i].ConditionID < out[j].ConditionID
	})
	return out
}

// retainPintCandidates makes a partial refresh expansion-only. Current classifications win, while
// last-selected rows absent from the partial head retain their exact condition/token identity.
// Held/arb/matched/near rows still outrank retained fillers through pintCandidateRank.
func retainPintCandidates(current, previous []pintBookCandidate) []pintBookCandidate {
	byID := make(map[string]pintBookCandidate, len(current)+len(previous))
	for _, c := range current {
		if id := strings.TrimSpace(c.ConditionID); id != "" {
			c.ConditionID = id
			byID[id] = c
		}
	}
	for _, old := range previous {
		id := strings.TrimSpace(old.ConditionID)
		if id == "" {
			continue
		}
		if cur, ok := byID[id]; ok {
			cur.Retained = true
			if len(cur.Tokens) < len(old.Tokens) {
				cur.Tokens = append([]string(nil), old.Tokens...)
			}
			byID[id] = cur
			continue
		}
		old.Retained = true
		byID[id] = old
	}
	out := make([]pintBookCandidate, 0, len(byID))
	for _, c := range byID {
		out = append(out, c)
	}
	return out
}

func pintCandidateSetHas(set map[string]bool, c pintBookCandidate) bool {
	return set[c.ConditionID] || set[c.Slug] || set[strings.ToLower(c.Slug)]
}

// filterRetainedPintCandidates refreshes the two money-safety flags before carrying an old seat.
// A partial catalog keeps every last-good seat. A completed catalog is authoritative for ordinary
// discovery breadth, but it still cannot evict a currently held position or an active lock merely
// because Gamma omitted or halted that market. If either local dependency read fails, preserve its
// prior true flag (fail closed) until a later successful read can prove the dependency is gone.
func filterRetainedPintCandidates(retained []pintBookCandidate, held, arb map[string]bool, heldKnown, arbKnown, retainAll bool) []pintBookCandidate {
	out := make([]pintBookCandidate, 0, len(retained))
	for _, old := range retained {
		if heldKnown {
			old.Held = pintCandidateSetHas(held, old)
		}
		if arbKnown {
			old.Arb = pintCandidateSetHas(arb, old)
		}
		if !retainAll && !old.Held && !old.Arb {
			continue
		}
		out = append(out, old)
	}
	return out
}

// pintDynamicXVVenues is xvTwin's all-destinations view.  xvTwin returns one other venue, which is
// sufficient for a lookup but would hide a future three-way registry entry behind map iteration.
// The same member-level safety rules apply: only kinds whose settlement authorities are verified
// equivalent can become matches; weather/econ/politics remain coverage-only. An ambiguous
// destination with multiple ids is never called a match.
func pintDynamicXVTwins(ids ...string) (kalshiID, polyusID string) {
	xvReg.mu.Lock()
	defer xvReg.mu.Unlock()
	key := ""
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		key = xvReg.keyOf["polymarket|"+id]
		if key == "" {
			key = xvReg.keyOf["polymarket|"+strings.ToLower(id)]
		}
		if key != "" {
			break
		}
	}
	e := xvReg.byKey[key]
	if e == nil || !xvKeyRulesEquivalent(e.kind, key) {
		return "", ""
	}
	if len(e.ids["kalshi"]) == 1 {
		kalshiID = e.ids["kalshi"][0]
	}
	if len(e.ids["polyus"]) == 1 {
		polyusID = e.ids["polyus"][0]
	}
	return kalshiID, polyusID
}

func pintDynamicXVVenues(ids ...string) (matchK, matchPUS bool) {
	k, p := pintDynamicXVTwins(ids...)
	return k != "", p != ""
}

func (s *Server) buildPintCLOBPlan(ctx context.Context, markets []polymarket.Market, retained []pintBookCandidate, retainAll bool) pintBookPlan {
	now := time.Now()
	held, signal, arb := map[string]bool{}, map[string]bool{}, map[string]bool{}
	heldKnown := false
	if s.store != nil {
		if fills, err := s.store.ListPaperFills(ctx); err == nil {
			heldKnown = true
			positions, _ := paper.Aggregate(fills)
			for _, pos := range positions {
				if pos.Platform == "polymarket" && pos.Contracts > 0 && strings.TrimSpace(pos.Ticker) != "" {
					held[pos.Ticker] = true
				}
			}
		}
		if ids, err := s.store.ListRecentOpenSignalTickersByPlatform(ctx, "polymarket", now.Add(-pintSignalSeatTTL), pintSignalSeatLimit); err == nil {
			for _, id := range ids {
				signal[id] = true
			}
		}
	}
	s.xvlMu.Lock()
	arbKnown := false
	if b := s.xvlLoadLocked(); b != nil {
		arbKnown = true
		for _, op := range b.Open {
			if op.AVenue == "polymarket" {
				arb[op.AID] = true
			}
			if op.BVenue == "polymarket" {
				arb[op.BID] = true
			}
		}
	}
	s.xvlMu.Unlock()
	retained = filterRetainedPintCandidates(retained, held, arb, heldKnown, arbKnown, retainAll)

	matchK, matchPUS := map[string]bool{}, map[string]bool{}
	for _, pair := range s.pintTwinPairs() {
		if pair.kalshi != "" {
			matchK[pair.ref.condID] = true
		}
		if pair.polyus != "" {
			matchPUS[pair.ref.condID] = true
		}
	}
	regularH, cryptoH := directionalBookHours(s.cfg().Auto)
	byCond := map[string]pintBookCandidate{}
	for _, m := range markets {
		if m.ConditionID == "" || m.Closed || (!stringSetHas(held, m) && (!m.Active || !m.AcceptingOrders)) {
			continue
		}
		dynK, dynPUS := pintDynamicXVVenues(m.ConditionID, m.Slug)
		mk, mp := matchK[m.ConditionID] || dynK, matchPUS[m.ConditionID] || dynPUS
		resolveAt, _ := parsePolyTime(strings.TrimSpace(m.EndDate))
		crypto := isCryptoTicker(m.Slug + " " + m.Question + " " + m.Category())
		c := pintBookCandidate{ConditionID: m.ConditionID, Slug: m.Slug,
			Tokens: marketOutcomeTokens(m.TokensRaw), ResolveAt: resolveAt, Volume: m.Volume24hr,
			Held: stringSetHas(held, m), Arb: stringSetHas(arb, m), Signal: stringSetHas(signal, m),
			Near:   withinDirectionalBookHorizon(now, resolveAt, crypto, regularH, cryptoH),
			MatchK: mk, MatchPUS: mp}
		if old, ok := byCond[m.ConditionID]; !ok || len(c.Tokens) > len(old.Tokens) {
			byCond[m.ConditionID] = c
		}
	}
	cs := make([]pintBookCandidate, 0, len(byCond))
	for _, c := range byCond {
		cs = append(cs, c)
	}
	if len(retained) > 0 {
		cs = retainPintCandidates(cs, retained)
	}
	return planPintCLOBBooks(pintCLOBConditionCap, len(markets), cs, regularH, cryptoH, now)
}

func clonePintBookPlan(in pintBookPlan) pintBookPlan {
	out := in
	out.Conditions = append([]string(nil), in.Conditions...)
	out.Assets = append([]string(nil), in.Assets...)
	out.Selected = append([]pintBookCandidate(nil), in.Selected...)
	for i := range out.Selected {
		out.Selected[i].Tokens = append([]string(nil), in.Selected[i].Tokens...)
	}
	return out
}

func (s *Server) recordPintCLOBPlan(plan pintBookPlan) {
	s.pintBookMu.Lock()
	old := s.pintBookPlan
	s.pintBookPlan = plan.Stats
	s.pintBookLast = clonePintBookPlan(plan)
	s.pintBookMu.Unlock()
	if plan.Stats.RequiredShortfall > 0 && old.RequiredShortfall == 0 && s.log != nil {
		s.log.Warn("poly-int CLOB priority band exceeds bounded condition cap",
			"cap", plan.Stats.Cap, "required_shortfall", plan.Stats.RequiredShortfall,
			"matched", plan.Stats.Matched, "held", plan.Stats.Held, "active_signals", plan.Stats.ActiveSignal)
	}
}

func (s *Server) pintCLOBPlanView() pintBookPlanStats {
	s.pintBookMu.Lock()
	defer s.pintBookMu.Unlock()
	return s.pintBookPlan
}

func (s *Server) pintCLOBLastPlan() pintBookPlan {
	s.pintBookMu.Lock()
	defer s.pintBookMu.Unlock()
	return clonePintBookPlan(s.pintBookLast)
}

func (s *Server) pintCatalogMarkets() []polymarket.Market {
	if s.poly == nil {
		return nil
	}
	return mergePintCatalog(s.poly.CachedMarketSnapshot(), s.poly.CompleteCatalogSnapshot())
}

// pintCLOBAssets is deliberately cache-only. A CLOB reconnect/rotation must never wait 15s for a
// partial Gamma volume crawl and then shrink/dial-flap on whatever pages arrived before timeout.
func (s *Server) pintCLOBAssets(ctx context.Context) []string {
	if s.poly == nil {
		return nil
	}
	now := time.Now()
	head := s.poly.CachedMarketSnapshot()
	complete := s.poly.CompleteCatalogSnapshot()
	markets := mergePintCatalog(head, complete)
	last := s.pintCLOBLastPlan()
	if len(markets) == 0 {
		if len(last.Assets) == 0 {
			last = pintBookPlan{Stats: pintBookPlanStats{Cap: pintCLOBConditionCap,
				ResearchOnly: true, LiveAuthorizing: false}}
		}
		stampPintCatalogTelemetry(&last, head, complete, now, "last-good-no-cache")
		s.recordPintCLOBPlan(last)
		return append([]string(nil), last.Assets...)
	}

	// SQLite bookkeeping is bounded independently from the socket loop. There is no venue REST in
	// buildPintCLOBPlan; a busy local DB simply yields fewer fresh priority flags this rotation.
	bctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	// Always pass the prior plan. A completed keyset may retire ordinary discovery rows, while
	// buildPintCLOBPlan filters this list down to live held/active-lock dependencies only.
	retained := last.Selected
	plan := s.buildPintCLOBPlan(bctx, markets, retained, !complete.Complete)
	source := "partial-head-cold-start"
	if complete.Complete {
		source = "complete-keyset+head-overlay"
	} else if len(last.Selected) > 0 {
		source = "partial-head+last-good-seats"
	}
	stampPintCatalogTelemetry(&plan, head, complete, now, source)
	s.recordPintCLOBPlan(plan)
	return append([]string(nil), plan.Assets...)
}

// StartPolyCLOBWS keeps full Gamma/event discovery separate from the bounded executable-quote
// cache. There is no full-universe depth fallback: discovery can stay broad without subscribing
// every token book.
func (s *Server) StartPolyCLOBWS(ctx context.Context) {
	s.poly.SetCatalogCachePath(filepath.Join(s.cfg().DataDir, "polyint_gamma_catalog.json"))
	s.poly.StartCLOBMarketWS(ctx, func() []string { return s.pintCLOBAssets(ctx) }, s.onPolyResolved)
}

type xvlPintQuote struct {
	ConditionID                    string
	BestBid, BestAsk               float64
	BestBidDepth, BestAskDepth     float64
	NoBestBid, NoBestAsk           float64
	NoBestBidDepth, NoBestAskDepth float64
}

// xvlPintMarketQuote reads BOTH outcome-token books. Poly-int's NO/opposite token is a distinct
// CLOB asset, so a derived 1-YES-bid quote is not executable evidence there.
func (s *Server) xvlPintMarketQuote(gm polymarket.Market) (q xvlPintQuote, ok bool) {
	if s.poly == nil || gm.ConditionID == "" || gm.Closed || !gm.Active || !gm.AcceptingOrders || gm.Halted() {
		return q, false
	}
	yb, ya, ybd, yad, _, yok := s.poly.CLOBOutcomeBBO(gm.ConditionID, 0)
	nb, na, nbd, nad, _, nok := s.poly.CLOBOutcomeBBO(gm.ConditionID, 1)
	if !yok || !nok {
		return q, false
	}
	return xvlPintQuote{ConditionID: gm.ConditionID, BestBid: yb, BestAsk: ya,
		BestBidDepth: ybd, BestAskDepth: yad, NoBestBid: nb, NoBestAsk: na,
		NoBestBidDepth: nbd, NoBestAskDepth: nad}, true
}

// xvlPintCached is a zero-API lookup by slug OR condition id from the full Gamma cache, followed
// by an exact two-token CLOB read.
func (s *Server) xvlPintCached(id string) (q xvlPintQuote, ok bool) {
	if s.poly == nil {
		return q, false
	}
	for _, gm := range s.pintCatalogMarkets() {
		if gm.Slug == id || gm.ConditionID == id {
			return s.xvlPintMarketQuote(gm)
		}
	}
	return q, false
}
