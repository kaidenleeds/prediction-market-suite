package server

// R133 bookpriority.go keeps expensive full-depth subscriptions on the markets that can affect
// money first.  The broad lightweight ticker/BBO/trade feeds remain separate and uncapped.

import (
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type depthBookCandidate struct {
	ID        string
	Live      bool
	Near      bool
	ResolveAt time.Time
	Value     float64
}

// depthBookStrategyGroup is one independently named, side-specific executable-system queue.
// IDs stay producer-ranked inside the group; the planner round-robins groups so a prolific system
// cannot consume every full-depth seat before another positive system receives one current book.
type depthBookStrategyGroup struct {
	System   string
	Side     string
	Platform string
	IDs      []string
	// Positive is reserved for exact, currently fee-net-positive executable system routes. These
	// queues run immediately after funded dependencies and before generic live/horizon markets;
	// research/proposal queues run after the horizon.
	Positive bool
}

const (
	liveSignalDepthBookTTL         = 3 * time.Minute
	liveSignalDepthBookCap         = 256
	liveSignalDepthBookPerRouteCap = 64
)

// liveSignalDepthBookHint closes the signal->book circularity for an exact LIVE-allowlisted
// system. It is only a short-lived WS subscription hint: it does not grant Paper/LIVE authority,
// does not fetch REST, and every eventual order still repeats book, fee, proof and risk checks.
type liveSignalDepthBookHint struct {
	Platform string
	System   string
	Side     string
	Ticker   string
	At       time.Time
}

func (s *Server) noteLiveSignalDepthBookHint(sig storage.Signal) {
	platform := strings.ToLower(strings.TrimSpace(sig.Platform))
	if platform != "kalshi" && platform != "polyus" {
		return
	}
	system := strings.ToLower(strings.TrimSpace(sig.SignalType))
	side := strings.ToUpper(strings.TrimSpace(sig.Side))
	ticker := strings.TrimSpace(sig.Ticker)
	if system == "" || ticker == "" || (side != "YES" && side != "NO") {
		return
	}
	c := liveMirrorCandidate{Platform: platform, Ticker: ticker, Side: side, Family: system,
		Source: "auto-cons-" + system}
	if why := s.liveSystemSelectionReason(c, "taker"); why != "" {
		return
	}
	if platform == "polyus" {
		// PolyUS keeps prioritized LITE+TRADE coverage, but full depth is capped. Give this exact
		// allowlisted signal one immediate expiring seat in that same cap; no Kalshi planner state,
		// REST request, or money authority crosses this branch.
		if s.polyUSWS != nil {
			s.polyUSWS.PromoteFullBook(ticker)
		}
		return
	}
	now := time.Now()
	key := platform + "\x00" + system + "\x00" + side + "\x00" + ticker
	s.bookSetMu.Lock()
	if s.bookLiveSignals == nil {
		s.bookLiveSignals = map[string]liveSignalDepthBookHint{}
	}
	for k, hint := range s.bookLiveSignals {
		if now.Sub(hint.At) > liveSignalDepthBookTTL || now.Before(hint.At) {
			delete(s.bookLiveSignals, k)
		}
	}
	s.bookLiveSignals[key] = liveSignalDepthBookHint{Platform: platform, System: system,
		Side: side, Ticker: ticker, At: now}
	if len(s.bookLiveSignals) > liveSignalDepthBookCap {
		oldestKey, oldestAt := "", now
		for k, hint := range s.bookLiveSignals {
			if oldestKey == "" || hint.At.Before(oldestAt) {
				oldestKey, oldestAt = k, hint.At
			}
		}
		delete(s.bookLiveSignals, oldestKey)
	}
	s.bookSetMu.Unlock()
	// Wake one coalesced book worker. It immediately moves every fresh LIVE hint to the front
	// without increasing the 1,000-book ceiling or rereading account REST state.
	select {
	case s.liveBookWakeChannel() <- struct{}{}:
	default:
	}
}

func (s *Server) liveBookWakeChannel() chan struct{} {
	s.liveBookWakeOnce.Do(func() { s.liveBookWakeCh = make(chan struct{}, 1) })
	return s.liveBookWakeCh
}

func (s *Server) prioritizeLiveSignalBooks() {
	if s.kal == nil {
		return
	}
	now := time.Now()
	s.bookSetMu.Lock()
	hints := make([]liveSignalDepthBookHint, 0, len(s.bookLiveSignals))
	for key, hint := range s.bookLiveSignals {
		if now.Sub(hint.At) > liveSignalDepthBookTTL || now.Before(hint.At) {
			delete(s.bookLiveSignals, key)
			continue
		}
		hints = append(hints, hint)
	}
	s.bookSetMu.Unlock()
	sort.Slice(hints, func(i, j int) bool { return hints[i].At.After(hints[j].At) })
	tickers := make([]string, 0, len(hints))
	for _, hint := range hints {
		tickers = append(tickers, hint.Ticker)
	}
	s.kal.PrioritizeBookSubscriptions(tickers)
}

type depthBookStrategyStats struct {
	System    string `json:"system"`
	Side      string `json:"side,omitempty"`
	Eligible  int    `json:"eligible"`
	Selected  int    `json:"selected"`
	Shortfall int    `json:"shortfall"`
}

// depthBookPlanStats is deliberately surfaced in /api/live (Kalshi) and /api/pxage (PolyUS).
// RequiredShortfall is not an error by itself: it says the bounded full-depth cap could not hold
// every dependency/positive-system/live/near-horizon market, while the lightweight full-board
// feed still covers it.
type depthBookPlanStats struct {
	Cap                  int                      `json:"cap"`
	Universe             int                      `json:"universe"`
	Selected             int                      `json:"selected"`
	Dependency           int                      `json:"dependency"`
	DependencySelected   int                      `json:"dependency_selected"`
	Live                 int                      `json:"live"`
	LiveSelected         int                      `json:"live_selected"`
	NearHorizon          int                      `json:"near_horizon"`
	NearHorizonSelected  int                      `json:"near_horizon_selected"`
	Strategy             int                      `json:"strategy"`
	StrategySelected     int                      `json:"strategy_selected"`
	StrategyShortfall    int                      `json:"strategy_shortfall"`
	StrategySystems      []depthBookStrategyStats `json:"strategy_systems,omitempty"`
	TailSelected         int                      `json:"tail_selected"`
	RequiredShortfall    int                      `json:"required_shortfall"`
	RegularHorizonHours  float64                  `json:"regular_horizon_hours"`
	CryptoHorizonHours   float64                  `json:"crypto_horizon_hours"`
	LightweightFullBoard bool                     `json:"lightweight_full_board"`
}

type depthBookPlan struct {
	IDs   []string
	Stats depthBookPlanStats
}

// cachePositiveDepthBookGroups publishes current exact positive route CANDIDATES before quote
// refresh. This is a bounded subscription hint, not Paper/LIVE authority: the candidate needs the
// hint precisely so PolyUS Book3 can become fresh, after which Combo Lab repeats depth/fee gates.
func (s *Server) cachePositiveDepthBookGroups(legs []plabLeg) {
	const perRouteCap = 64
	legs = append([]plabLeg(nil), legs...)
	sort.SliceStable(legs, func(i, j int) bool {
		if legs[i].EVNet != legs[j].EVNet {
			return legs[i].EVNet > legs[j].EVNet
		}
		return legs[i].Ticker < legs[j].Ticker
	})
	type key struct{ platform, system, side string }
	byKey := map[key][]string{}
	seen := map[key]map[string]bool{}
	for _, leg := range legs {
		if !leg.RollingPositive || leg.EVNet <= 0 || !strings.EqualFold(leg.SystemRoute, "taker") {
			continue
		}
		platform := strings.ToLower(strings.TrimSpace(leg.Platform))
		if platform != "kalshi" && platform != "polyus" {
			continue
		}
		side := strings.ToUpper(strings.TrimSpace(leg.Side))
		if side != "YES" && side != "NO" {
			continue
		}
		for _, system := range leg.PositiveSystems {
			system = strings.TrimSpace(system)
			if system == "" {
				continue
			}
			k := key{platform: platform, system: system, side: side}
			if seen[k] == nil {
				seen[k] = map[string]bool{}
			}
			if !seen[k][leg.Ticker] && len(byKey[k]) < perRouteCap {
				seen[k][leg.Ticker] = true
				byKey[k] = append(byKey[k], leg.Ticker)
			}
		}
	}
	keys := make([]key, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].platform != keys[j].platform {
			return keys[i].platform < keys[j].platform
		}
		if keys[i].system != keys[j].system {
			return keys[i].system < keys[j].system
		}
		return keys[i].side < keys[j].side
	})
	groups := make([]depthBookStrategyGroup, 0, len(keys))
	for _, k := range keys {
		groups = append(groups, depthBookStrategyGroup{Platform: k.platform, System: k.system,
			Side: k.side, IDs: append([]string(nil), byKey[k]...), Positive: true})
	}
	s.bookSetMu.Lock()
	s.bookSystemGroups = groups
	s.bookSetMu.Unlock()
}

func (s *Server) positiveDepthBookGroups(platform string) []depthBookStrategyGroup {
	platform = strings.ToLower(strings.TrimSpace(platform))
	s.bookSetMu.Lock()
	defer s.bookSetMu.Unlock()
	now := time.Now()
	type routeKey struct{ system, side string }
	byRoute := map[routeKey][]string{}
	seen := map[routeKey]map[string]bool{}
	add := func(k routeKey, ticker string) {
		ticker = strings.TrimSpace(ticker)
		if ticker == "" {
			return
		}
		if seen[k] == nil {
			seen[k] = map[string]bool{}
		}
		if !seen[k][ticker] {
			seen[k][ticker] = true
			byRoute[k] = append(byRoute[k], ticker)
		}
	}
	// Fresh allowlisted signals go first inside their route so the next 30-second subscription
	// reconciliation can make their retry executable. Newest first; each route remains bounded.
	hints := make([]liveSignalDepthBookHint, 0, len(s.bookLiveSignals))
	for key, hint := range s.bookLiveSignals {
		if now.Sub(hint.At) > liveSignalDepthBookTTL || now.Before(hint.At) {
			delete(s.bookLiveSignals, key)
			continue
		}
		if hint.Platform == platform {
			hints = append(hints, hint)
		}
	}
	sort.Slice(hints, func(i, j int) bool { return hints[i].At.After(hints[j].At) })
	for _, hint := range hints {
		k := routeKey{system: hint.System, side: hint.Side}
		if len(byRoute[k]) < liveSignalDepthBookPerRouteCap {
			add(k, hint.Ticker)
		}
	}
	for _, group := range s.bookSystemGroups {
		if group.Platform != platform {
			continue
		}
		k := routeKey{system: group.System, side: group.Side}
		for _, ticker := range group.IDs {
			add(k, ticker)
		}
	}
	keys := make([]routeKey, 0, len(byRoute))
	for k := range byRoute {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].system != keys[j].system {
			return keys[i].system < keys[j].system
		}
		return keys[i].side < keys[j].side
	})
	out := make([]depthBookStrategyGroup, 0, len(keys))
	for _, k := range keys {
		out = append(out, depthBookStrategyGroup{Platform: platform, System: k.system,
			Side: k.side, IDs: byRoute[k], Positive: true})
	}
	return out
}

func directionalBookHours(a config.AutoConfig) (regular, crypto float64) {
	regular = a.ConsensusMaxHoursOut
	if regular <= 0 {
		regular = 6
	}
	crypto = a.ConsensusCryptoMaxHoursOut
	if crypto <= 0 {
		crypto = 2
	}
	return regular, crypto
}

func withinDirectionalBookHorizon(now, resolveAt time.Time, crypto bool, regularH, cryptoH float64) bool {
	if resolveAt.IsZero() || !resolveAt.After(now) {
		return false
	}
	lim := regularH
	if crypto {
		lim = cryptoH
	}
	return lim > 0 && resolveAt.Sub(now).Hours() <= lim
}

func resolveSooner(a, b depthBookCandidate) bool {
	switch {
	case a.ResolveAt.IsZero() != b.ResolveAt.IsZero():
		return !a.ResolveAt.IsZero()
	case !a.ResolveAt.Equal(b.ResolveAt):
		return a.ResolveAt.Before(b.ResolveAt)
	case a.Value != b.Value:
		return a.Value > b.Value
	default:
		return a.ID < b.ID
	}
}

// planDepthBooks orders one bounded full-depth working set:
//
//  1. held/resting money dependencies;
//  2. explicit executable positive-system inputs, fair across systems and YES/NO sides;
//  3. genuinely live markets;
//  4. every market inside the active directional AUTO horizon;
//  5. other explicit research/strategy inputs (directional proposals, then combo legs inside the shared
//     regular <=4h / crypto <=2h entry contract);
//  6. remaining discovery/logging candidates by value/volume.
//
// Live and near-horizon bands are deterministic: soonest resolution, then value, then ID.  This
// matters when a large game exposes hundreds of child markets and the required band exceeds cap.
func planDepthBooks(max int, dependencies [][]string, candidates []depthBookCandidate, strategy []depthBookStrategyGroup, regularH, cryptoH float64, lightweightFullBoard bool) depthBookPlan {
	p := depthBookPlan{Stats: depthBookPlanStats{Cap: max, Universe: len(candidates),
		RegularHorizonHours: regularH, CryptoHorizonHours: cryptoH,
		LightweightFullBoard: lightweightFullBoard}}
	if max <= 0 {
		return p
	}
	p.IDs = make([]string, 0, max)
	seen := make(map[string]bool, len(candidates)+max)
	add := func(id string) bool {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			return false
		}
		seen[id] = true
		if len(p.IDs) >= max {
			return false
		}
		p.IDs = append(p.IDs, id)
		return true
	}
	countBand := func(ids []string) {
		bandSeen := map[string]bool{}
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" || bandSeen[id] {
				continue
			}
			bandSeen[id] = true
			add(id)
		}
	}

	// Source APIs do not promise order. Sort inside each dependency class so cap behavior is stable,
	// while preserving the class priority (held positions before resting orders).
	for _, group := range dependencies {
		g := append([]string(nil), group...)
		sort.Strings(g)
		countBand(g)
	}

	live, near := make([]depthBookCandidate, 0), make([]depthBookCandidate, 0)
	for _, c := range candidates {
		if strings.TrimSpace(c.ID) == "" {
			continue
		}
		if c.Live {
			live = append(live, c)
		} else if c.Near {
			near = append(near, c)
		}
	}
	sort.Slice(live, func(i, j int) bool { return resolveSooner(live[i], live[j]) })
	sort.Slice(near, func(i, j int) bool { return resolveSooner(near[i], near[j]) })
	ids := func(cs []depthBookCandidate) []string {
		out := make([]string, len(cs))
		for i := range cs {
			out[i] = cs[i].ID
		}
		return out
	}
	// Normalize and deterministically order the system/side queues, then take one still-unselected
	// market from each queue per round. Producer order inside a queue remains its value ranking.
	groups := append([]depthBookStrategyGroup(nil), strategy...)
	sort.SliceStable(groups, func(i, j int) bool {
		if groups[i].Positive != groups[j].Positive {
			return groups[i].Positive
		}
		ki := strings.ToLower(strings.TrimSpace(groups[i].System)) + "|" + strings.ToUpper(strings.TrimSpace(groups[i].Side))
		kj := strings.ToLower(strings.TrimSpace(groups[j].System)) + "|" + strings.ToUpper(strings.TrimSpace(groups[j].Side))
		return ki < kj
	})
	normalized := make([][]string, len(groups))
	for i := range groups {
		local := map[string]bool{}
		for _, id := range groups[i].IDs {
			id = strings.TrimSpace(id)
			if id != "" && !local[id] {
				local[id] = true
				normalized[i] = append(normalized[i], id)
			}
		}
	}
	pos := make([]int, len(groups))
	addGroupClass := func(positive bool) {
		for len(p.IDs) < max {
			progress := false
			for i := range groups {
				if groups[i].Positive != positive {
					continue
				}
				for pos[i] < len(normalized[i]) {
					id := normalized[i][pos[i]]
					pos[i]++
					if seen[id] {
						continue
					}
					progress = true
					add(id)
					break
				}
				if len(p.IDs) >= max {
					break
				}
			}
			if !progress {
				break
			}
		}
	}
	addGroupClass(true)
	countBand(ids(live))
	countBand(ids(near))
	addGroupClass(false)

	// The tail is never discarded by the PolyUS lightweight planner; this is only the ordering of
	// the bounded full-depth prefix. Value first, then sooner resolution and ID for stable ties.
	tail := append([]depthBookCandidate(nil), candidates...)
	sort.Slice(tail, func(i, j int) bool {
		if tail[i].Value != tail[j].Value {
			return tail[i].Value > tail[j].Value
		}
		return resolveSooner(tail[i], tail[j])
	})
	for _, c := range tail {
		if add(c.ID) {
			p.Stats.TailSelected++
		}
	}
	p.Stats.Selected = len(p.IDs)
	selected := make(map[string]bool, len(p.IDs))
	for _, id := range p.IDs {
		selected[id] = true
	}
	countCoverage := func(ids []string) (total, selectedN int, unique map[string]bool) {
		unique = map[string]bool{}
		for _, id := range ids {
			if id = strings.TrimSpace(id); id != "" {
				unique[id] = true
			}
		}
		for id := range unique {
			total++
			if selected[id] {
				selectedN++
			}
		}
		return
	}
	var dependencyIDs []string
	for _, group := range dependencies {
		dependencyIDs = append(dependencyIDs, group...)
	}
	p.Stats.Dependency, p.Stats.DependencySelected, _ = countCoverage(dependencyIDs)
	p.Stats.Live, p.Stats.LiveSelected, _ = countCoverage(ids(live))
	p.Stats.NearHorizon, p.Stats.NearHorizonSelected, _ = countCoverage(ids(near))
	strategyUnion := map[string]bool{}
	for i, group := range groups {
		total, selectedN, idsForGroup := countCoverage(normalized[i])
		name := strings.TrimSpace(group.System)
		if name == "" {
			name = "unnamed"
		}
		p.Stats.StrategySystems = append(p.Stats.StrategySystems, depthBookStrategyStats{
			System: name, Side: strings.ToUpper(strings.TrimSpace(group.Side)), Eligible: total,
			Selected: selectedN, Shortfall: total - selectedN,
		})
		for id := range idsForGroup {
			strategyUnion[id] = true
		}
	}
	for id := range strategyUnion {
		p.Stats.Strategy++
		if selected[id] {
			p.Stats.StrategySelected++
		}
	}
	p.Stats.StrategyShortfall = p.Stats.Strategy - p.Stats.StrategySelected
	required := map[string]bool{}
	for _, ids := range [][]string{dependencyIDs, ids(live), ids(near)} {
		for _, id := range ids {
			if id = strings.TrimSpace(id); id != "" {
				required[id] = true
			}
		}
	}
	for id := range strategyUnion {
		required[id] = true
	}
	for id := range required {
		if !selected[id] {
			p.Stats.RequiredShortfall++
		}
	}
	return p
}

func kalshiDepthCandidates(markets map[string]kalshi.Market, now time.Time, regularH, cryptoH float64) []depthBookCandidate {
	out := make([]depthBookCandidate, 0, len(markets))
	for ticker, m := range markets {
		if ticker == "" || (m.Status != "" && !strings.EqualFold(m.Status, "active") && !strings.EqualFold(m.Status, "open")) || m.Result != "" {
			continue
		}
		resolveAt, _ := parseTime(firstNonEmpty(m.ExpectedExpiration, m.CloseTime))
		crypto := isCryptoTicker(ticker)
		live := false
		if start, ok := kalshiTickerStartAt(ticker, now); ok && !start.After(now) && (resolveAt.IsZero() || resolveAt.After(now)) {
			live = true
		}
		out = append(out, depthBookCandidate{ID: ticker, Live: live,
			Near:      withinDirectionalBookHorizon(now, resolveAt, crypto, regularH, cryptoH),
			ResolveAt: resolveAt, Value: m.Volume24h.Float()})
	}
	return out
}

// completeKalshiBookMarkets unions the complete non-blocking board cache used by /api/markets with
// the smaller, fresher tape/warm cache.  The warm copy may improve metadata but can never define
// planner breadth by itself (the live soak caught that old 319-vs-20,302 mismatch).
func completeKalshiBookMarkets(board []kalshi.Market, warm map[string]kalshi.Market) map[string]kalshi.Market {
	out := make(map[string]kalshi.Market, len(board)+len(warm))
	for _, m := range board {
		if m.Ticker != "" {
			out[m.Ticker] = m
		}
	}
	for ticker, m := range warm {
		if ticker == "" {
			ticker = m.Ticker
		}
		if ticker != "" {
			out[ticker] = m
		}
	}
	return out
}

// mergePolyUSPlannerMarkets overlays the rich league snapshot on the last complete strict-open
// /v1 crawl. League rows retain live/score/sub-bet metadata; crawl-only props/futures/non-sports
// rows still enter the depth planner immediately with lifecycle, horizon and volume truth.
func mergePolyUSPlannerMarkets(snapshot []polyUSMarket, crawl []polymarketus.Market, now time.Time) []polyUSMarket {
	out := append([]polyUSMarket(nil), snapshot...)
	idx := make(map[string]int, len(snapshot)+len(crawl))
	for i, m := range out {
		if m.Slug != "" {
			idx[m.Slug] = i
		}
	}
	for _, m := range crawl {
		if m.Slug == "" || !m.Open() { // belt: the stored sweep is strict-open, and the helper pins it too
			continue
		}
		if i, ok := idx[m.Slug]; ok {
			r := &out[i]
			if r.Resolve == "" {
				r.Resolve = m.EndDate
			}
			if r.Start == "" {
				r.Start = m.GameStart
			}
			if r.Volume24h == 0 {
				r.Volume24h = m.Volume24hr
			}
			if r.Kind == "" {
				r.Kind = pusKindOf(m)
			}
			if r.SportsScope == "" {
				r.SportsScope = strings.TrimSpace(m.SportsType)
			}
			continue
		}
		name := pusQuestionSelectionName(m)
		league := strings.ToLower(strings.TrimSpace(m.Category))
		if league == "" {
			league = "other"
		}
		live := false
		if start, err := parsePolyTime(strings.TrimSpace(m.GameStart)); err == nil {
			age := now.Sub(start)
			live = age >= 0 && age <= 5*time.Hour
		}
		row := polyUSMarket{
			League: league, EventID: m.ID, Game: name, Slug: m.Slug,
			Kind: pusKindOf(m), SportsScope: strings.TrimSpace(m.SportsType), Question: name, Line: m.Line,
			Yes: m.YesPrice(), Bid: m.BestBid, Ask: m.BestAsk,
			Volume24h: m.Volume24hr, MinimumQty: m.MinimumQty, TickSize: m.TickSize,
			FeeCoeff: m.FeeCoeff, Start: m.GameStart, Resolve: m.EndDate, Live: live,
		}
		if m.IsToAdvance() || m.TwoSidedSingle() {
			lt, sh := m.LongTeam(), m.ShortTeam()
			row.Team, row.TeamName = lt.Abbreviation, lt.Name
			row.ShortTeam, row.ShortTeamName = sh.Abbreviation, sh.Name
			row.TwoSided = m.TwoSidedSingle()
		}
		idx[m.Slug] = len(out)
		out = append(out, row)
	}
	return out
}

func polyUSDepthCandidates(markets []polyUSMarket, now time.Time, regularH, cryptoH float64) []depthBookCandidate {
	out := make([]depthBookCandidate, 0, len(markets))
	for _, m := range markets {
		if m.Slug == "" {
			continue
		}
		// Sports endDate is the scheduled event boundary, not a dependable settlement clock.
		// Prefer the same start+3.5h estimate used by signal collection and the funded gate so an
		// in-progress game is not evicted from the full-depth live/near bands before it can trade.
		// Crawl-only futures/non-sports rows often have no game start; retain their venue endDate.
		resolveAt := time.Time{}
		if start, err := parsePolyTime(strings.TrimSpace(m.Start)); err == nil {
			resolveAt = start.Add(3*time.Hour + 30*time.Minute)
		} else {
			resolveAt, _ = parsePolyTime(strings.TrimSpace(m.Resolve))
		}
		crypto := isCryptoTicker(m.Slug + " " + m.Game)
		// A stale schedule flag is not "genuinely live" after its known/fallback resolution
		// horizon. PolyUS can leave already-finished games strict-open while settlement catches up;
		// letting those rows fill the entire 1,000-seat live band starved active strategy/liquidity
		// books and produced zero executable quotes. Lightweight all-board coverage remains.
		live := m.Live && (resolveAt.IsZero() || resolveAt.After(now))
		out = append(out, depthBookCandidate{ID: m.Slug, Live: live,
			Near:      withinDirectionalBookHorizon(now, resolveAt, crypto, regularH, cryptoH),
			ResolveAt: resolveAt, Value: m.Volume24h})
	}
	return out
}

func (s *Server) recordDepthBookPlan(platform string, stats depthBookPlanStats) {
	s.bookSetMu.Lock()
	old := s.bookKalPlan
	if platform == "polyus" {
		old = s.bookPUSPlan
		s.bookPUSPlan = stats
	} else {
		s.bookKalPlan = stats
	}
	s.bookSetMu.Unlock()
	if stats.RequiredShortfall > 0 && old.RequiredShortfall == 0 && s.log != nil {
		s.log.Warn("full-depth priority band exceeds bounded cap",
			"venue", platform, "cap", stats.Cap, "dependencies", stats.Dependency,
			"live", stats.Live, "strategy", stats.Strategy, "strategy_shortfall", stats.StrategyShortfall,
			"near_horizon", stats.NearHorizon,
			"required_shortfall", stats.RequiredShortfall,
			"lightweight_full_board", stats.LightweightFullBoard)
	}
}

func (s *Server) depthBookPlanView(platform string) depthBookPlanStats {
	s.bookSetMu.Lock()
	defer s.bookSetMu.Unlock()
	if platform == "polyus" {
		return s.bookPUSPlan
	}
	return s.bookKalPlan
}

// polyUSBookWSCap returns the effective socket ceiling. Once the markets WS exists it is the source
// of truth: re-reading mutable config could otherwise make planning/readiness claim 1,000 while an
// already-created socket is still requesting 600. Before socket construction, use the same shared
// clamp on config so cold planner tests and boot diagnostics agree with the eventual wire value.
func (s *Server) polyUSBookWSCap() int {
	if s != nil && s.polyUSWS != nil {
		return s.polyUSWS.FullBookCap()
	}
	if s == nil {
		return polymarketus.DefaultMarketsFullBookCap
	}
	return polymarketus.ClampMarketsFullBookCap(s.cfg().PolyUSBookWSCap)
}
