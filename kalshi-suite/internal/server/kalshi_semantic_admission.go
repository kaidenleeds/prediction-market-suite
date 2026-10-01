package server

// R153 authoritative sports-payoff admission.
//
// Kalshi's event_ticker is only the native market header. Winner, spread, total, period and prop
// headers for one real game are related by the official Milestones API. This file converts the
// purchased YES/NO side of the structured core families into constraints on the same score and
// checks the candidate together with every already-funded leg. Unknown same-occurrence semantics
// fail closed; different official occurrences remain independent.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/objectiveidentity"
)

const (
	kalshiSemanticSuccessTTL = 10 * time.Minute
	kalshiSemanticErrorTTL   = 15 * time.Second
)

type kalshiSemanticEventCache struct {
	Event kalshi.Event
	At    time.Time
	Err   string
}

type kalshiSemanticMilestoneCache struct {
	Milestone kalshi.Milestone
	Found     bool
	At        time.Time
	Err       string
}

type r159KalshiResidentAdmissionContextKey struct{}

// r159KalshiResidentAdmissionContext marks the exact urgent AUTO admission lane. Public semantic
// recovery remains available to Paper, research, background warming and manual/package paths, but
// the cash lane may consume only facts that were already resident before the decision began.
func r159KalshiResidentAdmissionContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, r159KalshiResidentAdmissionContextKey{}, true)
}

func r159KalshiResidentAdmissionOnly(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	residentOnly, _ := ctx.Value(r159KalshiResidentAdmissionContextKey{}).(bool)
	return residentOnly
}

type kalshiSemanticFlight struct {
	mu   sync.Mutex
	refs int
}

// lockKalshiSemanticFlight singleflights one official semantic resource without serializing
// unrelated games. The entry is reference-counted and removed after the last waiter, so a moving
// sports board cannot grow an unbounded lock registry.
func (s *Server) lockKalshiSemanticFlight(key string) func() {
	s.kalSemFlightMu.Lock()
	if s.kalSemFlights == nil {
		s.kalSemFlights = map[string]*kalshiSemanticFlight{}
	}
	flight := s.kalSemFlights[key]
	if flight == nil {
		flight = &kalshiSemanticFlight{}
		s.kalSemFlights[key] = flight
	}
	flight.refs++
	s.kalSemFlightMu.Unlock()

	flight.mu.Lock()
	return func() {
		flight.mu.Unlock()
		s.kalSemFlightMu.Lock()
		flight.refs--
		if flight.refs == 0 && s.kalSemFlights[key] == flight {
			delete(s.kalSemFlights, key)
		}
		s.kalSemFlightMu.Unlock()
	}
}

type kalshiSemanticDescriptor struct {
	Ticker      string
	EventTicker string
	Event       kalshi.Event
	Milestone   kalshi.Milestone
	Sports      bool
	Predicate   objectiveidentity.PurchasedPredicate
}

// r159KalshiResidentMarket returns current complete-board metadata without starting venue I/O.
// kmktsAt measures the last price refresh and can age on a quiet market, so it is deliberately not
// lifecycle authority here. The complete-board receipt is the same <=5m metadata proof consumed by
// the final WS executable-book boundary.
func (s *Server) r159KalshiResidentMarket(ticker string) (kalshi.Market, bool) {
	ticker = strings.TrimSpace(ticker)
	if ticker == "" {
		return kalshi.Market{}, false
	}
	if s.allowSyntheticKalshiEventIdentity {
		market, ok := s.kmkt(ticker)
		return market, ok && strings.TrimSpace(market.EventTicker) != ""
	}
	if s.kal == nil {
		return kalshi.Market{}, false
	}
	markets, observedAt := s.kal.CompleteBoardSnapshot()
	age := time.Since(observedAt)
	if observedAt.IsZero() || age < 0 || age > r159KalshiMarketCacheMaxAge {
		return kalshi.Market{}, false
	}
	for i := range markets {
		if strings.EqualFold(strings.TrimSpace(markets[i].Ticker), ticker) &&
			strings.TrimSpace(markets[i].EventTicker) != "" {
			return markets[i], true
		}
	}
	return kalshi.Market{}, false
}

// r159KalshiResidentSemanticDescriptor is the complete cache-only semantic receipt used by urgent
// cash arbitration and event-exposure admission. Sports requires both a fresh official Event and
// its fresh, unambiguous Milestone relationship; non-sports requires the Event. No cache miss can
// fall through to a public endpoint.
func (s *Server) r159KalshiResidentSemanticDescriptor(ticker, side string) (
	kalshiSemanticDescriptor, bool) {
	ticker = strings.TrimSpace(ticker)
	market, ok := s.r159KalshiResidentMarket(ticker)
	if !ok {
		return kalshiSemanticDescriptor{}, false
	}
	eventTicker := strings.ToUpper(strings.TrimSpace(market.EventTicker))
	if s.allowSyntheticKalshiEventIdentity {
		return kalshiSemanticDescriptor{Ticker: ticker, EventTicker: eventTicker}, true
	}
	now := time.Now()
	s.kalSemMu.Lock()
	eventEntry, eventOK := s.kalSemEvents[eventTicker]
	milestoneEntry, milestoneOK := s.kalSemMilestone[eventTicker]
	s.kalSemMu.Unlock()
	if !eventOK || eventEntry.Err != "" || now.Sub(eventEntry.At) < 0 ||
		now.Sub(eventEntry.At) > kalshiSemanticSuccessTTL {
		return kalshiSemanticDescriptor{}, false
	}
	out := kalshiSemanticDescriptor{Ticker: ticker, EventTicker: eventTicker,
		Event:  eventEntry.Event,
		Sports: strings.EqualFold(strings.TrimSpace(eventEntry.Event.Category), "sports")}
	if !out.Sports {
		return out, true
	}
	if !milestoneOK || !milestoneEntry.Found || milestoneEntry.Err != "" ||
		now.Sub(milestoneEntry.At) < 0 || now.Sub(milestoneEntry.At) > kalshiSemanticSuccessTTL {
		return kalshiSemanticDescriptor{}, false
	}
	out.Milestone = milestoneEntry.Milestone
	out.Predicate = semanticPurchasedPredicate(market, out.Event, out.Milestone, side)
	return out, true
}

func semanticToken(v string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(v)) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func semanticRawString(raw json.RawMessage) string {
	var out string
	if len(raw) == 0 || json.Unmarshal(raw, &out) != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func semanticCustomStrikeID(m kalshi.Market) string {
	keys := make([]string, 0, len(m.CustomStrike))
	for key := range m.CustomStrike {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if value := semanticRawString(m.CustomStrike[key]); value != "" {
			return value
		}
	}
	return ""
}

func (s *Server) kalshiSemanticEvent(ctx context.Context, eventTicker string) (kalshi.Event, error) {
	return s.kalshiSemanticEventMode(ctx, eventTicker, true)
}

func (s *Server) kalshiSemanticEventMode(ctx context.Context, eventTicker string,
	priority bool) (kalshi.Event, error) {
	eventTicker = strings.ToUpper(strings.TrimSpace(eventTicker))
	if eventTicker == "" || s.kal == nil {
		return kalshi.Event{}, errors.New("Kalshi semantic event client unavailable")
	}
	now := time.Now()
	s.kalSemMu.Lock()
	if cached, ok := s.kalSemEvents[eventTicker]; ok {
		ttl := kalshiSemanticSuccessTTL
		if cached.Err != "" {
			ttl = kalshiSemanticErrorTTL
		}
		if now.Sub(cached.At) >= 0 && now.Sub(cached.At) <= ttl {
			s.kalSemMu.Unlock()
			if cached.Err != "" {
				return kalshi.Event{}, errors.New(cached.Err)
			}
			return cached.Event, nil
		}
	}
	s.kalSemMu.Unlock()

	if r159KalshiResidentAdmissionOnly(ctx) {
		return kalshi.Event{}, errors.New("resident Kalshi semantic event cache unavailable or stale")
	}

	// Same-resource callers singleflight; unrelated games remain parallel. Refresh the clock after
	// waiting, otherwise the new cache row can appear to be from the future and trigger a duplicate
	// REST fetch for every waiter.
	release := s.lockKalshiSemanticFlight("event:" + eventTicker)
	defer release()
	now = time.Now()
	s.kalSemMu.Lock()
	if cached, ok := s.kalSemEvents[eventTicker]; ok && now.Sub(cached.At) >= 0 &&
		now.Sub(cached.At) <= kalshiSemanticSuccessTTL && cached.Err == "" {
		s.kalSemMu.Unlock()
		return cached.Event, nil
	}
	s.kalSemMu.Unlock()

	fetchCtx := ctx
	if priority {
		fetchCtx = kalshi.WithPriority(ctx)
	}
	event, err := s.kal.GetEvent(fetchCtx, eventTicker)
	entry := kalshiSemanticEventCache{Event: event, At: time.Now()}
	if err != nil {
		entry.Err = err.Error()
	}
	s.kalSemMu.Lock()
	if s.kalSemEvents == nil {
		s.kalSemEvents = map[string]kalshiSemanticEventCache{}
	}
	s.kalSemEvents[eventTicker] = entry
	s.kalSemMu.Unlock()
	if err != nil {
		return kalshi.Event{}, err
	}
	return event, nil
}

func milestoneSemanticScore(m kalshi.Milestone) int {
	score := 0
	if strings.EqualFold(strings.TrimSpace(m.Category), "sports") {
		score += 10
	}
	for _, key := range []string{"main_game_event_ticker", "home_team_id", "away_team_id",
		"first_competitor_id", "second_competitor_id", "tie_id"} {
		if m.DetailString(key) != "" {
			score += 2
		}
	}
	if strings.Contains(strings.ToLower(m.Type), "game") || strings.Contains(strings.ToLower(m.Type), "match") ||
		strings.Contains(strings.ToLower(m.Type), "tournament") {
		score++
	}
	return score
}

func chooseSemanticMilestone(eventTicker string, rows []kalshi.Milestone) (kalshi.Milestone, error) {
	candidates := make([]kalshi.Milestone, 0, len(rows))
	for _, row := range rows {
		if row.ContainsEvent(eventTicker) && strings.EqualFold(strings.TrimSpace(row.Category), "sports") {
			candidates = append(candidates, row)
		}
	}
	if len(candidates) == 0 {
		return kalshi.Milestone{}, errors.New("official Sports milestone unavailable")
	}
	sort.Slice(candidates, func(i, j int) bool {
		si, sj := milestoneSemanticScore(candidates[i]), milestoneSemanticScore(candidates[j])
		if si != sj {
			return si > sj
		}
		if len(candidates[i].RelatedEventTickers) != len(candidates[j].RelatedEventTickers) {
			return len(candidates[i].RelatedEventTickers) < len(candidates[j].RelatedEventTickers)
		}
		return candidates[i].ID < candidates[j].ID
	})
	if len(candidates) > 1 && milestoneSemanticScore(candidates[0]) == milestoneSemanticScore(candidates[1]) &&
		candidates[0].ID != candidates[1].ID &&
		len(candidates[0].RelatedEventTickers) == len(candidates[1].RelatedEventTickers) {
		return kalshi.Milestone{}, errors.New("official Sports milestone relationship is ambiguous")
	}
	return candidates[0], nil
}

func (s *Server) kalshiSemanticMilestone(ctx context.Context, eventTicker string) (kalshi.Milestone, error) {
	return s.kalshiSemanticMilestoneMode(ctx, eventTicker, true)
}

func (s *Server) kalshiSemanticMilestoneMode(ctx context.Context, eventTicker string,
	priority bool) (kalshi.Milestone, error) {
	eventTicker = strings.ToUpper(strings.TrimSpace(eventTicker))
	if eventTicker == "" || s.kal == nil {
		return kalshi.Milestone{}, errors.New("Kalshi semantic milestone client unavailable")
	}
	now := time.Now()
	s.kalSemMu.Lock()
	if cached, ok := s.kalSemMilestone[eventTicker]; ok {
		ttl := kalshiSemanticSuccessTTL
		if cached.Err != "" {
			ttl = kalshiSemanticErrorTTL
		}
		if now.Sub(cached.At) >= 0 && now.Sub(cached.At) <= ttl {
			s.kalSemMu.Unlock()
			if cached.Err != "" {
				return kalshi.Milestone{}, errors.New(cached.Err)
			}
			if !cached.Found {
				return kalshi.Milestone{}, errors.New("official Sports milestone unavailable")
			}
			return cached.Milestone, nil
		}
	}
	s.kalSemMu.Unlock()

	if r159KalshiResidentAdmissionOnly(ctx) {
		return kalshi.Milestone{}, errors.New("resident Kalshi semantic milestone cache unavailable or stale")
	}

	release := s.lockKalshiSemanticFlight("milestone:" + eventTicker)
	defer release()
	now = time.Now()
	s.kalSemMu.Lock()
	if cached, ok := s.kalSemMilestone[eventTicker]; ok && now.Sub(cached.At) >= 0 &&
		now.Sub(cached.At) <= kalshiSemanticSuccessTTL && cached.Err == "" && cached.Found {
		s.kalSemMu.Unlock()
		return cached.Milestone, nil
	}
	s.kalSemMu.Unlock()

	fetchCtx := ctx
	if priority {
		fetchCtx = kalshi.WithPriority(ctx)
	}
	rows, err := s.kal.GetMilestonesForEvent(fetchCtx, eventTicker)
	var milestone kalshi.Milestone
	if err == nil {
		milestone, err = chooseSemanticMilestone(eventTicker, rows)
	}
	entry := kalshiSemanticMilestoneCache{Milestone: milestone, Found: err == nil, At: time.Now()}
	if err != nil {
		entry.Err = err.Error()
	}
	s.kalSemMu.Lock()
	if s.kalSemMilestone == nil {
		s.kalSemMilestone = map[string]kalshiSemanticMilestoneCache{}
	}
	for _, related := range append(append([]string(nil), milestone.RelatedEventTickers...), milestone.PrimaryEventTickers...) {
		related = strings.ToUpper(strings.TrimSpace(related))
		if related != "" {
			s.kalSemMilestone[related] = entry
		}
	}
	s.kalSemMilestone[eventTicker] = entry
	s.kalSemMu.Unlock()
	if err != nil {
		return kalshi.Milestone{}, err
	}
	return milestone, nil
}

func semanticPeriod(event kalshi.Event) string {
	series := strings.ToUpper(strings.TrimSpace(event.SeriesTicker))
	scope := semanticToken(event.ProductString("competition_scope"))
	joined := series + "|" + strings.ToUpper(scope)
	periods := []struct {
		Tokens []string
		Value  string
	}{
		{[]string{"FIRSTHALF", "1STHALF", "1H"}, "half:1"},
		{[]string{"SECONDHALF", "2NDHALF", "2H"}, "half:2"},
		{[]string{"FIRSTQUARTER", "1STQUARTER", "Q1", "1Q"}, "quarter:1"},
		{[]string{"SECONDQUARTER", "2NDQUARTER", "Q2", "2Q"}, "quarter:2"},
		{[]string{"THIRDQUARTER", "3RDQUARTER", "Q3", "3Q"}, "quarter:3"},
		{[]string{"FOURTHQUARTER", "4THQUARTER", "Q4", "4Q"}, "quarter:4"},
		{[]string{"FIRST3INNINGS", "F3"}, "innings:1-3"},
		{[]string{"FIRST5INNINGS", "F5"}, "innings:1-5"},
		{[]string{"FIRST7INNINGS", "F7"}, "innings:1-7"},
	}
	for _, period := range periods {
		for _, token := range period.Tokens {
			if strings.Contains(joined, token) {
				return period.Value
			}
		}
	}
	if strings.Contains(joined, "SET") || strings.Contains(joined, "MAP") ||
		strings.Contains(joined, "ROUND") || strings.Contains(joined, "INNING") {
		// Indexed unit propositions are safe only when the official scope names the index.
		for i := 1; i <= 15; i++ {
			n := strconv.Itoa(i)
			if strings.Contains(strings.ToUpper(scope), n) {
				kind := "unit"
				if strings.Contains(joined, "SET") {
					kind = "set"
				} else if strings.Contains(joined, "MAP") {
					kind = "map"
				} else if strings.Contains(joined, "ROUND") {
					kind = "round"
				} else if strings.Contains(joined, "INNING") {
					kind = "inning"
				}
				return kind + ":" + n
			}
		}
		return ""
	}
	return "full_game"
}

func semanticFamily(event kalshi.Event) string {
	series := semanticToken(event.SeriesTicker)
	scope := semanticToken(event.ProductString("competition_scope"))
	joined := series + scope
	switch {
	case strings.Contains(joined, "btts") || strings.Contains(joined, "bothteamstoscore"):
		return "btts"
	case strings.Contains(joined, "teamtotal"):
		return "team-total"
	case strings.Contains(joined, "spread") || strings.Contains(joined, "handicap"):
		return "spread"
	case strings.Contains(joined, "total") || strings.Contains(joined, "pointtotal"):
		return "total"
	case strings.Contains(joined, "exactscore") || strings.Contains(joined, "correctscore"):
		return "exact-score"
	case strings.Contains(joined, "advance") || strings.Contains(joined, "qualif"):
		return "advance"
	case strings.Contains(joined, "game") || strings.Contains(joined, "match") ||
		strings.Contains(joined, "winner"):
		return "winner"
	default:
		return ""
	}
}

func semanticTargetRole(m kalshi.Market, milestone kalshi.Milestone) string {
	target := semanticCustomStrikeID(m)
	if target == "" {
		return ""
	}
	for _, key := range []string{"home_team_id", "first_competitor_id"} {
		if id := milestone.DetailString(key); id != "" && strings.EqualFold(id, target) {
			return "home"
		}
	}
	for _, key := range []string{"away_team_id", "second_competitor_id"} {
		if id := milestone.DetailString(key); id != "" && strings.EqualFold(id, target) {
			return "away"
		}
	}
	if id := milestone.DetailString("tie_id"); id != "" && strings.EqualFold(id, target) {
		return "tie"
	}
	return ""
}

func scoreConstraint(home, away float64, comparator string, threshold float64) objectiveidentity.ScoreConstraint {
	return objectiveidentity.ScoreConstraint{HomeCoeff: home, AwayCoeff: away,
		Comparator: comparator, Threshold: threshold}
}

func semanticPurchasedPredicate(m kalshi.Market, event kalshi.Event, milestone kalshi.Milestone,
	side string) objectiveidentity.PurchasedPredicate {
	p := objectiveidentity.PurchasedPredicate{EventID: milestone.ID, InstrumentID: m.Ticker,
		Period: semanticPeriod(event)}
	side = strings.ToUpper(strings.TrimSpace(side))
	if side != "YES" && side != "NO" || p.EventID == "" || p.Period == "" {
		return p
	}
	family := semanticFamily(event)
	role := semanticTargetRole(m, milestone)
	line := m.FloorStrike.Float()
	var alternatives [][]objectiveidentity.ScoreConstraint
	switch family {
	case "winner":
		if role == "tie" {
			if side == "YES" {
				alternatives = [][]objectiveidentity.ScoreConstraint{{scoreConstraint(1, -1, "eq", 0)}}
			} else {
				alternatives = [][]objectiveidentity.ScoreConstraint{
					{scoreConstraint(1, -1, "gt", 0)}, {scoreConstraint(1, -1, "lt", 0)},
				}
			}
		} else if role == "home" || role == "away" {
			h, a := 1.0, -1.0
			if role == "away" {
				h, a = -1, 1
			}
			cmp := "gt"
			if side == "NO" {
				cmp = "lte"
			}
			alternatives = [][]objectiveidentity.ScoreConstraint{{scoreConstraint(h, a, cmp, 0)}}
		}
	case "spread":
		if role == "home" || role == "away" {
			h, a := 1.0, -1.0
			if role == "away" {
				h, a = -1, 1
			}
			cmp := "gt"
			if side == "NO" {
				cmp = "lte"
			}
			alternatives = [][]objectiveidentity.ScoreConstraint{{scoreConstraint(h, a, cmp, line)}}
		}
	case "total":
		cmp := "gt"
		if side == "NO" {
			cmp = "lte"
		}
		alternatives = [][]objectiveidentity.ScoreConstraint{{scoreConstraint(1, 1, cmp, line)}}
	case "team-total":
		if role == "home" || role == "away" {
			h, a := 1.0, 0.0
			if role == "away" {
				h, a = 0, 1
			}
			cmp := "gt"
			if side == "NO" {
				cmp = "lte"
			}
			alternatives = [][]objectiveidentity.ScoreConstraint{{scoreConstraint(h, a, cmp, line)}}
		}
	case "btts":
		if side == "YES" {
			alternatives = [][]objectiveidentity.ScoreConstraint{{
				scoreConstraint(1, 0, "gte", 1), scoreConstraint(0, 1, "gte", 1),
			}}
		} else {
			alternatives = [][]objectiveidentity.ScoreConstraint{
				{scoreConstraint(1, 0, "lte", 0)}, {scoreConstraint(0, 1, "lte", 0)},
			}
		}
	}
	if len(alternatives) > 0 {
		p.Structured = true
		p.ScoreAlternatives = alternatives
	}
	return p
}

func (s *Server) kalshiSemanticDescriptor(ctx context.Context, ticker, side string) (kalshiSemanticDescriptor, error) {
	ticker = strings.TrimSpace(ticker)
	if ticker == "" || (s.kal == nil && !s.allowSyntheticKalshiEventIdentity) {
		return kalshiSemanticDescriptor{}, errors.New("Kalshi semantic market client unavailable")
	}
	if r159KalshiResidentAdmissionOnly(ctx) {
		if descriptor, ok := s.r159KalshiResidentSemanticDescriptor(ticker, side); ok {
			return descriptor, nil
		}
		return kalshiSemanticDescriptor{}, errors.New("resident Kalshi semantic descriptor unavailable or stale")
	}
	m, cached := s.kmkt(ticker)
	if s.allowSyntheticKalshiEventIdentity && cached && strings.TrimSpace(m.EventTicker) != "" {
		// Hermetic tests deliberately use fake Kalshi tickers and no official Event/Milestone
		// endpoint. They continue to exercise exact native-header ownership; only production may
		// invoke the broader Sports authority below.
		return kalshiSemanticDescriptor{Ticker: ticker,
			EventTicker: strings.ToUpper(strings.TrimSpace(m.EventTicker))}, nil
	}
	if !cached || strings.TrimSpace(m.EventTicker) == "" {
		var err error
		m, err = s.kal.GetMarket(kalshi.WithPriority(ctx), ticker)
		if err != nil {
			return kalshiSemanticDescriptor{}, fmt.Errorf("Kalshi semantic market read failed: %w", err)
		}
		s.metaMu.Lock()
		s.kmktsPutLocked(ticker, m)
		s.metaMu.Unlock()
	}
	eventTicker := strings.ToUpper(strings.TrimSpace(m.EventTicker))
	if eventTicker == "" {
		return kalshiSemanticDescriptor{}, errors.New("Kalshi semantic market omitted event_ticker")
	}
	event, err := s.kalshiSemanticEvent(ctx, eventTicker)
	if err != nil {
		return kalshiSemanticDescriptor{}, err
	}
	d := kalshiSemanticDescriptor{Ticker: ticker, EventTicker: eventTicker, Event: event,
		Sports: strings.EqualFold(strings.TrimSpace(event.Category), "sports")}
	if !d.Sports {
		return d, nil
	}
	d.Milestone, err = s.kalshiSemanticMilestone(ctx, eventTicker)
	if err != nil {
		return kalshiSemanticDescriptor{}, err
	}
	d.Predicate = semanticPurchasedPredicate(m, event, d.Milestone, side)
	return d, nil
}

func semanticConflictReason(verdict objectiveidentity.IntersectionVerdict) string {
	if verdict.Contradictory {
		return "kalshi-logical-payoff-conflict"
	}
	if verdict.ReasonCode == objectiveidentity.IntersectionUnclassified {
		return "kalshi-related-payoff-unclassified"
	}
	return ""
}

// kalshiSemanticExposureConflict checks the candidate against the full funded constraint set for
// each official occurrence. It is intentionally set-wise: a three-leg contradiction can exist
// even when every pair appears compatible.
func (s *Server) kalshiSemanticExposureConflict(ctx context.Context, candidateTicker, candidateSide string,
	held []kalshiEventExposure) string {
	candidate, err := s.kalshiSemanticDescriptor(ctx, candidateTicker, candidateSide)
	if err != nil {
		if r159KalshiResidentAdmissionOnly(ctx) {
			s.scheduleKalshiSemanticWarm(candidateTicker)
			for _, exposure := range held {
				s.scheduleKalshiSemanticWarm(exposure.Ticker)
			}
		}
		return "kalshi-payoff-identity-unavailable"
	}
	if !candidate.Sports {
		return ""
	}
	groups := map[string][]objectiveidentity.PurchasedPredicate{
		candidate.Milestone.ID: {candidate.Predicate},
	}
	for _, exposure := range held {
		d, descErr := s.kalshiSemanticDescriptor(ctx, exposure.Ticker, exposure.Side)
		if descErr != nil {
			if r159KalshiResidentAdmissionOnly(ctx) {
				s.scheduleKalshiSemanticWarm(exposure.Ticker)
			}
			return "kalshi-held-payoff-identity-unavailable"
		}
		if !d.Sports || d.Milestone.ID != candidate.Milestone.ID {
			continue
		}
		groups[d.Milestone.ID] = append(groups[d.Milestone.ID], d.Predicate)
	}
	if len(groups[candidate.Milestone.ID]) < 2 {
		return ""
	}
	return semanticConflictReason(objectiveidentity.EvaluatePurchasedSet(groups[candidate.Milestone.ID]))
}
