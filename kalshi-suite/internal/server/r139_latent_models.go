package server

// Frozen v1 score-state and outcome-expansion research collectors. Discovery may use other current
// books, but every tested action is independently repriced from the target's current complete
// side-specific full-depth book with exact one-share fee/tick/depth/source-clock truth. These paths
// can only write zero-authority research observations and collector receipts.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/outcomeexpansion"
	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
	"github.com/kalshi-suite/kalshi-suite/internal/scorestate"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	// One game can fan out into dozens of markets and hundreds of immutable observer writes.
	// Twenty-five games repeatedly grew the live WAL to the research-pressure rail before the
	// cursor could commit, so the same page retried forever. Five retains fair cursor coverage
	// while fitting one durable cycle inside the bounded research lane.
	r139ScoreStatePage       = 5
	r139OutcomeExpansionPage = 25
	r139ScoreStateClockSkew  = 5 * time.Second
)

type r139LatentSchedule struct {
	sync.Mutex
	lastScore, lastOutcome time.Time
	registered             bool
}

var r139LatentSchedules sync.Map // *Server -> *r139LatentSchedule

func (s *Server) insertR139LatentCollectorReceipt(ctx context.Context, receipt storage.CollectorReceipt) (bool, error) {
	durableCtx, cancelDurability := researchDurabilityContext(ctx)
	defer cancelDurability()
	return s.store.InsertCollectorReceipt(durableCtx, receipt)
}

func (s *Server) r139LatentSchedule() *r139LatentSchedule {
	v, _ := r139LatentSchedules.LoadOrStore(s, &r139LatentSchedule{})
	return v.(*r139LatentSchedule)
}

func (s *Server) r139LatentRegistered() bool {
	schedule := s.r139LatentSchedule()
	schedule.Lock()
	defer schedule.Unlock()
	return schedule.registered
}

// ensureR139LatentCollectorBlueprints owns registration as a separate bounded lane. Registration
// used to be the last operation in a multi-minute systems callback, so it inherited an already
// expired context and could never make the two latent collectors visible.
func (s *Server) ensureR139LatentCollectorBlueprints(ctx context.Context) bool {
	if s == nil || s.store == nil {
		return false
	}
	schedule := s.r139LatentSchedule()
	schedule.Lock()
	registered := schedule.registered
	schedule.Unlock()
	if registered {
		return true
	}
	if err := s.store.EnsureR139LatentCollectorBlueprints(ctx); err != nil {
		if s.log != nil {
			s.log.Error("research latent collector registration failed", "err", err)
		}
		return false
	}
	schedule.Lock()
	schedule.registered = true
	schedule.Unlock()
	return true
}

func (s *Server) r139LatentLaneDue(lane string, now time.Time) bool {
	schedule := s.r139LatentSchedule()
	schedule.Lock()
	defer schedule.Unlock()
	switch lane {
	case "register":
		return !schedule.registered
	case "outcome":
		return schedule.registered && (schedule.lastOutcome.IsZero() || now.Sub(schedule.lastOutcome) >= 2*time.Minute)
	case "score":
		return schedule.registered && (schedule.lastScore.IsZero() || now.Sub(schedule.lastScore) >= 5*time.Minute)
	default:
		return false
	}
}

func (s *Server) sweepR139LatentLaneIfDue(ctx context.Context, lane string, now time.Time) {
	if lane == "register" {
		s.ensureR139LatentCollectorBlueprints(ctx)
		return
	}
	if !s.ensureR139LatentCollectorBlueprints(ctx) {
		return
	}
	schedule := s.r139LatentSchedule()
	schedule.Lock()
	due := false
	switch lane {
	case "outcome":
		due = schedule.lastOutcome.IsZero() || now.Sub(schedule.lastOutcome) >= 2*time.Minute
		if due {
			schedule.lastOutcome = now
		}
	case "score":
		due = schedule.lastScore.IsZero() || now.Sub(schedule.lastScore) >= 5*time.Minute
		if due {
			schedule.lastScore = now
		}
	}
	schedule.Unlock()
	if !due {
		return
	}
	switch lane {
	case "outcome":
		s.sweepR139OutcomeExpansion(ctx, now)
	case "score":
		s.sweepR139ScoreState(ctx, now)
	}
}

// Compatibility entry point for direct tests/tools. Runtime scheduling uses the three fair lanes
// above, so registration, outcome expansion and score-state never share one admission.
func (s *Server) sweepR139LatentModels(ctx context.Context, now time.Time) {
	if !s.ensureR139LatentCollectorBlueprints(ctx) {
		return
	}
	s.sweepR139LatentLaneIfDue(ctx, "outcome", now)
	s.sweepR139LatentLaneIfDue(ctx, "score", now)
}

type r139OneShareAction struct {
	Side, BookSource, FeeSource, SourceClock string
	Cost, Fee, Depth, Tick, QuoteAge         float64
	Fills                                    []properbetting.Fill
}

func (a r139OneShareAction) exact() bool {
	return (a.Side == "YES" || a.Side == "NO") && a.Cost > 0 && a.Cost < 1 && a.Fee >= 0 &&
		a.Depth >= 1 && a.Tick > 0 && a.QuoteAge >= 0 && strings.TrimSpace(a.BookSource) != "" &&
		strings.TrimSpace(a.FeeSource) != "" && strings.TrimSpace(a.SourceClock) != ""
}

func r139SideDepth(levels []properbetting.Level) float64 {
	total := 0.0
	for _, level := range levels {
		if level.Quantity > 0 {
			total += level.Quantity
		}
	}
	return total
}

func (s *Server) r139OneShareBook(ctx context.Context, venue, ticker string) (properScoreBook,
	map[string]r139OneShareAction, error) {
	book, ok := s.properScoreCurrentBook(ctx, venue, ticker)
	if !ok || !book.completeBinarySet() {
		return properScoreBook{}, nil, errors.New("current complete binary full-depth book unavailable")
	}
	if strings.TrimSpace(book.sourceClock) == "" {
		return book, nil, errors.New("current book lacks immutable venue source clock")
	}
	out := map[string]r139OneShareAction{}
	for _, side := range []string{"YES", "NO"} {
		levels, probe := book.yesAsks, book.yesAsk
		if side == "NO" {
			levels, probe = book.noAsks, book.noAsk
		}
		feeFn, feeSource, known := s.properScoreCurveFee(venue, ticker, "taker", probe)
		if !known {
			return book, nil, fmt.Errorf("%s exact one-share fee unavailable", side)
		}
		walk, err := properbetting.WalkDepth(levels, 1, .5, book.tick, book.lot, feeFn)
		if err != nil || !walk.FullFill || walk.Filled < 1-1e-9 || walk.Cancelled > 1e-9 {
			return book, nil, fmt.Errorf("%s exact one-share depth unavailable: %w", side, err)
		}
		action := r139OneShareAction{Side: side, BookSource: book.source, FeeSource: feeSource,
			SourceClock: book.sourceClock, Cost: walk.IntegratedCost, Fee: walk.Fee,
			Depth: r139SideDepth(levels), Tick: walk.MinimumTick, QuoteAge: book.age,
			Fills: append([]properbetting.Fill(nil), walk.Fills...)}
		if !action.exact() {
			return book, nil, fmt.Errorf("%s one-share action failed exact route contract", side)
		}
		out[side] = action
	}
	return book, out, nil
}

func r139CertificateStatus(identity storage.CurrentCanonicalInstrument) string {
	switch strings.ToLower(strings.TrimSpace(identity.IdentityStatus)) {
	case "verified", "structural", "unverified", "rejected":
		return strings.ToLower(strings.TrimSpace(identity.IdentityStatus))
	default:
		return "unverified"
	}
}

func r139CapitalSeconds(store *storage.Store, ctx context.Context, venue, ticker string, now time.Time) float64 {
	closeTS, _, ok := store.CatalogCloseTS(ctx, venue, ticker)
	if !ok {
		return 0
	}
	closeAt, ok := parseTime(closeTS)
	if !ok {
		return 0
	}
	return math.Max(0, closeAt.Sub(now).Seconds())
}

type r139ActionObservation struct {
	SystemID, OpportunityID, Cohort, Venue, Ticker, Side string
	SourceArtifact, Blocker                              string
	Observed                                             time.Time
	Identity                                             storage.CurrentCanonicalInstrument
	Action                                               r139OneShareAction
	Candidate                                            bool
	ExpectedFair, ExpectedFairLower, ExpectedNetLower    float64
	CapitalSeconds, DecisionLatencyMS                    float64
	Inputs                                               map[string]any
}

func (s *Server) insertR139ActionObservation(ctx context.Context, row r139ActionObservation) (bool, error) {
	if !row.Action.exact() || row.Side != row.Action.Side {
		return false, errors.New("R139 action observation lacks exact one-share route")
	}
	inputs := map[string]any{}
	for k, v := range row.Inputs {
		inputs[k] = v
	}
	inputs["expected_fair"] = row.ExpectedFair
	inputs["expected_fair_lower"] = row.ExpectedFairLower
	inputs["promotion_expected_net_lower"] = row.ExpectedNetLower
	inputs["literal_open_payoff_envelope"] = []float64{0, 1}
	inputs["one_share_fills"] = row.Action.Fills
	inputs["authority"] = "zero"
	kind := "control"
	if row.Candidate {
		kind = "candidate"
	}
	status := r139CertificateStatus(row.Identity)
	if row.Candidate && status != "verified" {
		return false, errors.New("unverified R139 identity cannot become candidate")
	}
	id, inserted, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: row.Observed, SystemID: row.SystemID, ExperimentVersion: 1,
		OpportunityID: row.OpportunityID, Kind: kind, Cohort: row.Cohort,
		CanonicalEventID: row.Identity.EventID, EventVersion: row.Identity.EventVersion,
		CanonicalPayoffID: row.Identity.PayoffID, PayoffVersion: row.Identity.PayoffVersion,
		Venue: row.Venue, Ticker: row.Ticker, Route: "taker", Side: row.Side,
		CertificateStatus: status, CertificateHash: storage.R138HashJSON(inputs),
		SourceClockID: row.Action.SourceClock, SourceArtifact: row.SourceArtifact,
		BookSource: row.Action.BookSource, FeeSource: row.Action.FeeSource, Blocker: row.Blocker,
		QuoteAgeMax: row.Action.QuoteAge, TickMin: row.Action.Tick, Size: 1,
		Cost: row.Action.Cost, Fee: row.Action.Fee, PayoutLower: 0, PayoutUpper: 1,
		NetLower:        -row.Action.Cost - row.Action.Fee,
		NetUpper:        1 - row.Action.Cost - row.Action.Fee,
		VisibleCapacity: row.Action.Depth, CapitalSeconds: row.CapitalSeconds,
		DecisionLatencyMS: row.DecisionLatencyMS, Candidate: row.Candidate,
		LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
		CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: row.Action.Cost, Fee: row.Action.Fee,
			PayoutFloor: 0, NetFloor: -row.Action.Cost - row.Action.Fee}}, Inputs: inputs,
	})
	_ = id
	return inserted, err
}

func (s *Server) insertR139BlockedObserver(ctx context.Context, system, opportunity, cohort,
	venue, ticker, eventID, sourceClock, sourceArtifact, blocker string, observed time.Time,
	inputs map[string]any) (bool, error) {
	_, inserted, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: observed, SystemID: system, ExperimentVersion: 1, OpportunityID: opportunity,
		Kind: "control", Cohort: cohort, CanonicalEventID: eventID, Venue: venue, Ticker: ticker,
		Route: "observer", Side: "NONE", CertificateStatus: "structural",
		CertificateHash: storage.R138HashJSON(inputs), SourceClockID: sourceClock,
		SourceArtifact: sourceArtifact, PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0,
		OutcomeStatus: "open", Blocker: blocker, Candidate: false, Inputs: inputs,
	})
	return inserted, err
}

func r139TeamEqual(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b)) && strings.TrimSpace(a) != ""
}

func r139ScorePredicate(row storage.MarketGameRow, game storage.GameIdentityRow) (scorestate.Predicate, error) {
	kind := strings.ToLower(strings.TrimSpace(row.MktType))
	switch kind {
	case "winner":
		if r139TeamEqual(row.YesTeam, game.Home) {
			return scorestate.Predicate{Axis: "margin", Threshold: 0, Greater: true}, nil
		}
		if r139TeamEqual(row.YesTeam, game.Away) {
			return scorestate.Predicate{Axis: "margin", Threshold: 0, Greater: false}, nil
		}
	case "spread":
		if r139TeamEqual(row.YesTeam, game.Home) {
			return scorestate.Predicate{Axis: "margin", Threshold: -row.Line, Greater: true}, nil
		}
		if r139TeamEqual(row.YesTeam, game.Away) {
			return scorestate.Predicate{Axis: "margin", Threshold: row.Line, Greater: false}, nil
		}
	case "total":
		if strings.EqualFold(strings.TrimSpace(row.YesTeam), "over") {
			return scorestate.Predicate{Axis: "total", Threshold: row.Line, Greater: true}, nil
		}
		if strings.EqualFold(strings.TrimSpace(row.YesTeam), "under") {
			return scorestate.Predicate{Axis: "total", Threshold: row.Line, Greater: false}, nil
		}
	}
	return scorestate.Predicate{}, fmt.Errorf("unsupported or structurally ambiguous %s predicate", kind)
}

func r139FairGamePage(groups map[string][]storage.MarketGameRow, after string, limit int) ([]string, string) {
	ids := make([]string, 0, len(groups))
	for id, rows := range groups {
		supported := 0
		for _, row := range rows {
			switch strings.ToLower(strings.TrimSpace(row.MktType)) {
			case "winner", "spread", "total":
				supported++
			}
		}
		if supported >= 2 {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 || limit <= 0 {
		return nil, after
	}
	start := sort.SearchStrings(ids, after)
	for start < len(ids) && ids[start] <= after {
		start++
	}
	if start >= len(ids) {
		start = 0
	}
	n := min(limit, len(ids))
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, ids[(start+i)%len(ids)])
	}
	return out, out[len(out)-1]
}

type r139ScoreInput struct {
	row             storage.MarketGameRow
	predicate       scorestate.Predicate
	book            properScoreBook
	actions         map[string]r139OneShareAction
	discoveryMid    float64
	decisionStarted time.Time
	identity        storage.CurrentCanonicalInstrument
	identityOK      bool
}

func (s *Server) sweepR139ScoreState(ctx context.Context, now time.Time) {
	receipt := storage.CollectorReceipt{CollectorID: storage.R139ScoreStateCollectorID,
		CycleID: collectorCycleID(now), ExperimentID: "score-state-surface", ExperimentVersion: 1,
		Status: "healthy", Started: now, Source: storage.R139ScoreStateCollectorSource,
		SchemaVersion: storage.R139ScoreStateCollectorSchema, ExpectedCadence: 5 * time.Minute,
		Systems: []string{"score-state-surface"}, Exclusions: map[string]int{}, Metrics: map[string]any{}}
	defer func() {
		receipt.Completed = time.Now()
		if receipt.Eligible == 0 && receipt.Status == "healthy" {
			receipt.Status, receipt.ExpectedZero = "healthy_empty", true
			receipt.ZeroReason = "no fair-page structural game currently has at least two winner/spread/total markets"
		}
		_, _ = s.insertR139LatentCollectorReceipt(ctx, receipt)
	}()
	groups := map[string][]storage.MarketGameRow{}
	for _, venue := range []string{"kalshi", "polyus"} {
		rows, err := s.store.MarketGameRows(ctx, venue)
		if err != nil {
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
			return
		}
		for _, row := range rows {
			if row.GameID != "" && row.Src == "struct" {
				groups[row.GameID] = append(groups[row.GameID], row)
			}
		}
	}
	gameRows, err := s.store.GameIdentityRows(ctx)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	games := map[string]storage.GameIdentityRow{}
	for _, game := range gameRows {
		games[game.GameID] = game
	}
	after, cursorOK := s.store.KVGet(ctx, "r139:score-state:last-game")
	if !cursorOK {
		after = ""
	}
	selected, next := r139FairGamePage(groups, after, r139ScoreStatePage)
	receipt.Eligible = len(selected)
	receipt.Metrics["cursor_before"] = after
	receipt.Metrics["cursor_after"] = next
	receipt.Metrics["fair_page_limit"] = r139ScoreStatePage
	for _, gameID := range selected {
		game, ok := games[gameID]
		if !ok {
			receipt.Exclusions["canonical_game_identity_missing"]++
			if inserted, err := s.insertR139BlockedObserver(ctx, "score-state-surface",
				storage.ResearchRouteStableID("score-state", gameID, collectorCycleID(now), "missing-game"),
				"score-state-v1|blocked", "multi", "", "sports:"+gameID, "",
				"market_game structural joins", "canonical game identity missing", now,
				map[string]any{"game_id": gameID, "model_version": scorestate.ModelVersion}); err != nil {
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
				return
			} else if inserted {
				receipt.Inserted++
			} else {
				receipt.Duplicates++
			}
			continue
		}
		var inputs []r139ScoreInput
		for _, row := range groups[gameID] {
			receipt.Attempted++
			predicate, predErr := r139ScorePredicate(row, game)
			if predErr != nil {
				receipt.Exclusions["unsupported_or_ambiguous_predicate"]++
				inserted, insertErr := s.insertR139BlockedObserver(ctx, "score-state-surface",
					storage.ResearchRouteStableID("score-state", gameID, row.Venue, row.Ticker, collectorCycleID(now), "predicate"),
					"score-state-v1|unsupported", row.Venue, row.Ticker, "sports:"+gameID, "",
					"market_game structural joins", predErr.Error()+"; no prop or correlation claim", now,
					map[string]any{"game_id": gameID, "market_kind": row.MktType, "joint_claim": false})
				if insertErr != nil {
					receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", insertErr.Error()
					return
				}
				if inserted {
					receipt.Inserted++
				} else {
					receipt.Duplicates++
				}
				continue
			}
			decisionStarted := time.Now()
			book, actions, bookErr := s.r139OneShareBook(ctx, row.Venue, row.Ticker)
			if bookErr != nil {
				receipt.Exclusions["current_exact_book_fee_clock_unavailable"]++
				inserted, insertErr := s.insertR139BlockedObserver(ctx, "score-state-surface",
					storage.ResearchRouteStableID("score-state", gameID, row.Venue, row.Ticker, collectorCycleID(now), "book"),
					"score-state-v1|blocked-book", row.Venue, row.Ticker, "sports:"+gameID, "",
					"current target full-depth book", bookErr.Error(), now,
					map[string]any{"game_id": gameID, "market_kind": row.MktType, "target_quote_used_in_fit": false})
				if insertErr != nil {
					receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", insertErr.Error()
					return
				}
				if inserted {
					receipt.Inserted++
				} else {
					receipt.Duplicates++
				}
				continue
			}
			identity, identityOK, identityErr := s.store.CurrentCanonicalInstrument(ctx, row.Venue, row.Ticker)
			if identityErr != nil {
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "identity", identityErr.Error()
				return
			}
			inputs = append(inputs, r139ScoreInput{row: row, predicate: predicate, book: book,
				actions: actions, discoveryMid: (book.yesBid + book.yesAsk) / 2,
				decisionStarted: decisionStarted, identity: identity, identityOK: identityOK})
		}
		if len(inputs) == 0 {
			continue
		}
		modelRows := make([]scorestate.Market, len(inputs))
		minAge, maxAge := inputs[0].book.age, inputs[0].book.age
		clocks := map[string]string{}
		for i, input := range inputs {
			id := strings.ToLower(input.row.Venue) + "|" + input.row.Ticker
			modelRows[i] = scorestate.Market{ID: id, Kind: input.row.MktType,
				Mid: input.discoveryMid, Predicate: input.predicate}
			minAge, maxAge = math.Min(minAge, input.book.age), math.Max(maxAge, input.book.age)
			clocks[id] = input.book.sourceClock
		}
		clockSkew := time.Duration(math.Max(0, maxAge-minAge) * float64(time.Second))
		for i, input := range inputs {
			estimate, modelErr := scorestate.FitLeaveOneOut(modelRows, modelRows[i].ID)
			if modelErr != nil {
				receipt.Exclusions["leave_one_out_surface_unidentified"]++
				if inserted, err := s.insertR139BlockedObserver(ctx, "score-state-surface",
					storage.ResearchRouteStableID("score-state", gameID, modelRows[i].ID, collectorCycleID(now), "unidentified"),
					"score-state-v1|axis="+input.predicate.Axis+"|blocked", input.row.Venue, input.row.Ticker,
					"sports:"+gameID, input.book.sourceClock, "frozen leave-one-out current-book surface",
					modelErr.Error(), now, map[string]any{"game_id": gameID, "model_version": scorestate.ModelVersion,
						"target_quote_used_in_fit": false, "training_book_clocks": clocks, "joint_claim": false}); err != nil {
					receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
					return
				} else if inserted {
					receipt.Inserted++
				} else {
					receipt.Duplicates++
				}
				continue
			}
			if !input.identityOK {
				receipt.Exclusions["canonical_instrument_identity_missing"]++
				if inserted, err := s.insertR139BlockedObserver(ctx, "score-state-surface",
					storage.ResearchRouteStableID("score-state", gameID, modelRows[i].ID, collectorCycleID(now), "identity"),
					"score-state-v1|axis="+estimate.Axis+"|blocked", input.row.Venue, input.row.Ticker,
					"sports:"+gameID, input.book.sourceClock, "frozen leave-one-out current-book surface",
					"canonical instrument identity missing", now, map[string]any{"estimate": estimate,
						"target_quote_used_in_fit": false, "training_book_clocks": clocks}); err != nil {
					receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
					return
				} else if inserted {
					receipt.Inserted++
				} else {
					receipt.Duplicates++
				}
				continue
			}
			yesNet := estimate.Lower - input.actions["YES"].Cost - input.actions["YES"].Fee
			noLower := 1 - estimate.Upper
			noNet := noLower - input.actions["NO"].Cost - input.actions["NO"].Fee
			selectedSide := ""
			if yesNet > 0 || noNet > 0 {
				selectedSide = "YES"
				if noNet > yesNet {
					selectedSide = "NO"
				}
			}
			baseBlocker := ""
			if clockSkew > r139ScoreStateClockSkew {
				baseBlocker = "linked discovery books exceed frozen five-second source-clock skew"
				receipt.Exclusions["linked_book_clock_skew"]++
			} else if r139CertificateStatus(input.identity) != "verified" {
				baseBlocker = "canonical instrument identity is not verified"
				receipt.Exclusions["identity_not_verified"]++
			}
			for _, side := range []string{"YES", "NO"} {
				fair, lower, net := estimate.Fair, estimate.Lower, yesNet
				if side == "NO" {
					fair, lower, net = 1-estimate.Fair, noLower, noNet
				}
				candidate := side == selectedSide && baseBlocker == ""
				blocker := baseBlocker
				if blocker == "" && !candidate {
					if selectedSide == "" {
						blocker = "conservative model lower bound does not clear current all-in one-share cost"
					} else {
						blocker = "same-clock opposite-side control"
					}
				}
				cohort := strings.Join([]string{"score-state-v1", "axis=" + estimate.Axis,
					"league=" + strings.ToLower(game.League), "venue=" + input.row.Venue,
					"kind=" + strings.ToLower(input.row.MktType), "side=" + side,
					"route=taker", "event_day=" + now.UTC().Format("2006-01-02")}, "|")
				opp := storage.ResearchRouteStableID("score-state-surface", gameID, input.row.Venue,
					input.row.Ticker, side, input.book.sourceClock, collectorCycleID(now))
				inserted, err := s.insertR139ActionObservation(ctx, r139ActionObservation{
					SystemID: "score-state-surface", OpportunityID: opp, Cohort: cohort,
					Venue: input.row.Venue, Ticker: input.row.Ticker, Side: side, Observed: now,
					SourceArtifact: "market_game structural joins + frozen leave-one-out current-book surface",
					Blocker:        blocker, Identity: input.identity, Action: input.actions[side], Candidate: candidate,
					ExpectedFair: fair, ExpectedFairLower: lower, ExpectedNetLower: net,
					CapitalSeconds:    r139CapitalSeconds(s.store, ctx, input.row.Venue, input.row.Ticker, now),
					DecisionLatencyMS: math.Max(0, time.Since(input.decisionStarted).Seconds()*1000),
					Inputs: map[string]any{"model": estimate, "game_id": gameID,
						"target_quote_used_in_fit": false, "equivalent_target_predicates_excluded": true,
						"training_book_clocks": clocks, "source_clock_skew_s": clockSkew.Seconds(),
						"joint_claim": false, "correlation_supported": false, "props_supported": false,
						"current_target_book_only_for_execution": true, "paper_authority": false, "live_authority": false},
				})
				if err != nil {
					receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
					return
				}
				if inserted {
					receipt.Inserted++
				} else {
					receipt.Duplicates++
				}
			}
		}
	}
	if next != "" && receipt.Status == "healthy" {
		durableCtx, cancelDurability := researchDurabilityContext(ctx)
		err := s.store.KVSet(durableCtx, "r139:score-state:last-game", next)
		cancelDurability()
		if err != nil {
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "cursor", err.Error()
		}
	}
}

func r139OutcomeFrame(v storage.OutcomeSetFrameRecord) outcomeexpansion.Frame {
	members := make([]outcomeexpansion.Member, 0, len(v.Members))
	for _, member := range v.Members {
		members = append(members, outcomeexpansion.Member{ID: member.Ticker, Status: member.Status,
			Bid: member.Bid, Ask: member.Ask})
	}
	return outcomeexpansion.Frame{FrameID: v.ID, Observed: v.Observed, Venue: v.Venue,
		EventID: v.EventID, MembershipHash: v.MembershipHash, Members: members,
		MutuallyExclusive: v.MutuallyExclusive, Exhaustive: v.Exhaustive, VoidPolicy: v.VoidVerified}
}

func (s *Server) processR139OutcomePair(ctx context.Context, pair storage.OutcomeSetFramePair,
	now time.Time, exclusions map[string]int) (inserted, duplicates, attempted int, err error) {
	prior, current := r139OutcomeFrame(pair.Prior), r139OutcomeFrame(pair.Current)
	result, modelErr := outcomeexpansion.Analyze(prior, current)
	baseInputs := map[string]any{"model_version": outcomeexpansion.ModelVersion,
		"prior_frame_id": prior.FrameID, "current_frame_id": current.FrameID,
		"prior_membership_hash": prior.MembershipHash, "current_membership_hash": current.MembershipHash,
		"prior_model_input_digest":         outcomeexpansion.MembershipDigest(prior.Members),
		"current_membership_digest":        outcomeexpansion.MembershipDigest(current.Members),
		"target_current_quote_in_fair_fit": false, "event_day_separation": true,
		"paper_authority": false, "live_authority": false}
	if modelErr != nil {
		exclusions["immutable_frame_or_probability_contract"]++
		ok, insertErr := s.insertR139BlockedObserver(ctx, "outcome-set-expansion-shock",
			storage.ResearchRouteStableID("outcome-expansion", fmt.Sprint(current.FrameID), "model-block"),
			"outcome-expansion-v1|blocked", current.Venue, "", current.EventID,
			fmt.Sprintf("outcome-frame:%d", current.FrameID),
			pair.Current.SourceArtifact, modelErr.Error(), now, baseInputs)
		if insertErr != nil {
			return 0, 0, 0, insertErr
		}
		if ok {
			inserted++
		} else {
			duplicates++
		}
		return inserted, duplicates, 1, nil
	}
	baseInputs["model"] = result
	if len(result.Projections) == 0 {
		exclusions["model_explicit_blocker"]++
		blocker := firstNonEmpty(result.Blocker, "membership change has no identified survivor redistribution")
		ok, insertErr := s.insertR139BlockedObserver(ctx, "outcome-set-expansion-shock",
			storage.ResearchRouteStableID("outcome-expansion", fmt.Sprint(current.FrameID), "no-projection"),
			"outcome-expansion-v1|change="+result.Change.Class+"|blocked", current.Venue, "", current.EventID,
			fmt.Sprintf("outcome-frame:%d", current.FrameID), pair.Current.SourceArtifact, blocker, now, baseInputs)
		if insertErr != nil {
			return 0, 0, 0, insertErr
		}
		if ok {
			inserted++
		} else {
			duplicates++
		}
		return inserted, duplicates, 1, nil
	}
	for _, projection := range result.Projections {
		attempted++
		decisionStarted := time.Now()
		book, actions, bookErr := s.r139OneShareBook(ctx, current.Venue, projection.MemberID)
		if bookErr != nil {
			exclusions["current_exact_book_fee_clock_unavailable"]++
			inputs := map[string]any{}
			for k, v := range baseInputs {
				inputs[k] = v
			}
			inputs["projection"] = projection
			ok, insertErr := s.insertR139BlockedObserver(ctx, "outcome-set-expansion-shock",
				storage.ResearchRouteStableID("outcome-expansion", fmt.Sprint(current.FrameID), projection.MemberID, "book"),
				"outcome-expansion-v1|change="+result.Change.Class+"|blocked-book", current.Venue,
				projection.MemberID, current.EventID, fmt.Sprintf("outcome-frame:%d", current.FrameID),
				pair.Current.SourceArtifact, bookErr.Error(), now, inputs)
			if insertErr != nil {
				return inserted, duplicates, attempted, insertErr
			}
			if ok {
				inserted++
			} else {
				duplicates++
			}
			continue
		}
		identity, identityOK, identityErr := s.store.CurrentCanonicalInstrument(ctx, current.Venue, projection.MemberID)
		if identityErr != nil {
			return inserted, duplicates, attempted, identityErr
		}
		if !identityOK {
			exclusions["canonical_instrument_identity_missing"]++
			inputs := map[string]any{}
			for k, v := range baseInputs {
				inputs[k] = v
			}
			inputs["projection"] = projection
			ok, insertErr := s.insertR139BlockedObserver(ctx, "outcome-set-expansion-shock",
				storage.ResearchRouteStableID("outcome-expansion", fmt.Sprint(current.FrameID), projection.MemberID, "identity"),
				"outcome-expansion-v1|change="+result.Change.Class+"|blocked-identity", current.Venue,
				projection.MemberID, current.EventID, book.sourceClock, pair.Current.SourceArtifact,
				"canonical instrument identity missing", now, inputs)
			if insertErr != nil {
				return inserted, duplicates, attempted, insertErr
			}
			if ok {
				inserted++
			} else {
				duplicates++
			}
			continue
		}
		decision := outcomeexpansion.SelectAction(projection,
			outcomeexpansion.OneShareEconomics{Side: "YES", Cost: actions["YES"].Cost,
				Fee: actions["YES"].Fee, Depth: actions["YES"].Depth, Tick: actions["YES"].Tick,
				QuoteAge: actions["YES"].QuoteAge, BookClock: actions["YES"].SourceClock},
			outcomeexpansion.OneShareEconomics{Side: "NO", Cost: actions["NO"].Cost,
				Fee: actions["NO"].Fee, Depth: actions["NO"].Depth, Tick: actions["NO"].Tick,
				QuoteAge: actions["NO"].QuoteAge, BookClock: actions["NO"].SourceClock})
		for _, side := range []string{"YES", "NO"} {
			fair, lower := projection.Fair, projection.Lower
			if side == "NO" {
				fair, lower = 1-projection.Fair, 1-projection.Upper
			}
			netLower := lower - actions[side].Cost - actions[side].Fee
			candidate := result.Blocker == "" && decision.Decision == "candidate" &&
				decision.Side == side && r139CertificateStatus(identity) == "verified"
			blocker := result.Blocker
			if blocker == "" && r139CertificateStatus(identity) != "verified" {
				blocker = "canonical instrument identity is not verified"
				exclusions["identity_not_verified"]++
			} else if blocker == "" && !candidate {
				if decision.Decision != "candidate" {
					blocker = decision.Blocker
				} else {
					blocker = "same-clock opposite-side control"
				}
			}
			inputs := map[string]any{}
			for k, v := range baseInputs {
				inputs[k] = v
			}
			inputs["projection"] = projection
			inputs["route_decision"] = decision
			inputs["current_target_book_only_for_execution"] = true
			opp := storage.ResearchRouteStableID("outcome-expansion", fmt.Sprint(current.FrameID),
				projection.MemberID, side, actions[side].SourceClock)
			cohort := strings.Join([]string{"outcome-expansion-v1", "change=" + result.Change.Class,
				"venue=" + current.Venue, "side=" + side, "route=taker",
				"event_day=" + result.EventDayCluster}, "|")
			ok, insertErr := s.insertR139ActionObservation(ctx, r139ActionObservation{
				SystemID: "outcome-set-expansion-shock", OpportunityID: opp, Cohort: cohort,
				Venue: current.Venue, Ticker: projection.MemberID, Side: side, Observed: now,
				SourceArtifact: pair.Prior.SourceArtifact + " -> " + pair.Current.SourceArtifact,
				Blocker:        blocker, Identity: identity, Action: actions[side], Candidate: candidate,
				ExpectedFair: fair, ExpectedFairLower: lower, ExpectedNetLower: netLower,
				CapitalSeconds:    r139CapitalSeconds(s.store, ctx, current.Venue, projection.MemberID, now),
				DecisionLatencyMS: math.Max(0, time.Since(decisionStarted).Seconds()*1000), Inputs: inputs,
			})
			if insertErr != nil {
				return inserted, duplicates, attempted, insertErr
			}
			if ok {
				inserted++
			} else {
				duplicates++
			}
		}
	}
	return inserted, duplicates, attempted, nil
}

func (s *Server) sweepR139OutcomeExpansion(ctx context.Context, now time.Time) {
	cursor, err := s.store.R139LatentCursor(ctx, "outcome-expansion")
	if err != nil {
		_, _ = s.insertR139LatentCollectorReceipt(ctx, storage.CollectorReceipt{
			CollectorID: storage.R139OutcomeShockCollectorID, CycleID: collectorCycleID(now) + "|cursor-read",
			ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
			Status: "error", ErrorClass: "cursor", ErrorText: err.Error(), Started: now, Completed: time.Now(),
			Source: storage.R139OutcomeShockCollectorSource, SchemaVersion: storage.R139OutcomeShockCollectorSchema,
			ExpectedCadence: 2 * time.Minute, Systems: []string{"outcome-set-expansion-shock"},
			Exclusions: map[string]int{"cursor_read_error": 1}})
		return
	}
	pairs, err := s.store.OutcomeSetFrameChangesAfter(ctx, cursor, r139OutcomeExpansionPage)
	if err != nil {
		receipt := storage.CollectorReceipt{CollectorID: storage.R139OutcomeShockCollectorID,
			CycleID: collectorCycleID(now), ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
			Status: "error", Started: now, Completed: time.Now(), ErrorClass: "storage", ErrorText: err.Error(),
			Source: storage.R139OutcomeShockCollectorSource, SchemaVersion: storage.R139OutcomeShockCollectorSchema,
			ExpectedCadence: 2 * time.Minute, Systems: []string{"outcome-set-expansion-shock"},
			Exclusions: map[string]int{"frame_pair_read_error": 1}}
		_, _ = s.insertR139LatentCollectorReceipt(ctx, receipt)
		return
	}
	if len(pairs) == 0 {
		_, _ = s.insertR139LatentCollectorReceipt(ctx, storage.CollectorReceipt{
			CollectorID: storage.R139OutcomeShockCollectorID, CycleID: collectorCycleID(now),
			ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
			Status: "healthy_empty", ExpectedZero: true,
			ZeroReason: "no unseen immutable outcome-set membership change after the fair cursor",
			Started:    now, Completed: time.Now(), Source: storage.R139OutcomeShockCollectorSource,
			SchemaVersion: storage.R139OutcomeShockCollectorSchema, ExpectedCadence: 2 * time.Minute,
			Systems: []string{"outcome-set-expansion-shock"}, Exclusions: map[string]int{},
			Metrics: map[string]any{"cursor": cursor, "page_limit": r139OutcomeExpansionPage},
		})
		return
	}
	for _, pair := range pairs {
		exclusions := map[string]int{}
		inserted, duplicates, attempted, processErr := s.processR139OutcomePair(ctx, pair, now, exclusions)
		status := "healthy"
		errorClass, errorText := "", ""
		if processErr != nil {
			status, errorClass, errorText = "error", "model_or_storage", processErr.Error()
			exclusions["frame_processing_error"]++
		}
		receipt := storage.CollectorReceipt{CollectorID: storage.R139OutcomeShockCollectorID,
			CycleID:      collectorCycleID(now) + fmt.Sprintf("|frame=%d", pair.Current.ID),
			ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
			Status: status, ErrorClass: errorClass, ErrorText: errorText, Started: now, Completed: time.Now(),
			Eligible: 1, Attempted: attempted, Inserted: inserted, Duplicates: duplicates,
			Source: storage.R139OutcomeShockCollectorSource, SchemaVersion: storage.R139OutcomeShockCollectorSchema,
			ExpectedCadence: 2 * time.Minute, Systems: []string{"outcome-set-expansion-shock"},
			Exclusions: exclusions, Metrics: map[string]any{"cursor_before": cursor,
				"current_frame_id": pair.Current.ID, "prior_frame_id": pair.Prior.ID,
				"change_class": pair.Current.ChangeClass, "page_limit": r139OutcomeExpansionPage}}
		if _, receiptErr := s.insertR139LatentCollectorReceipt(ctx, receipt); receiptErr != nil {
			return
		}
		if processErr != nil {
			return // do not skip a failed frame
		}
		durableCtx, cancelDurability := researchDurabilityContext(ctx)
		cursorErr := s.store.AdvanceR139LatentCursor(durableCtx, "outcome-expansion", pair.Current.ID)
		cancelDurability()
		if cursorErr != nil {
			_, _ = s.insertR139LatentCollectorReceipt(ctx, storage.CollectorReceipt{
				CollectorID:  storage.R139OutcomeShockCollectorID,
				CycleID:      collectorCycleID(now) + fmt.Sprintf("|frame=%d|cursor-write", pair.Current.ID),
				ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
				Status: "error", ErrorClass: "cursor", ErrorText: cursorErr.Error(), Started: now, Completed: time.Now(),
				Source: storage.R139OutcomeShockCollectorSource, SchemaVersion: storage.R139OutcomeShockCollectorSchema,
				ExpectedCadence: 2 * time.Minute, Systems: []string{"outcome-set-expansion-shock"},
				Exclusions: map[string]int{"cursor_write_error": 1}, Metrics: map[string]any{"frame_id": pair.Current.ID}})
			return // safe replay: observations/receipt are immutable and deduplicate
		}
		cursor = pair.Current.ID
	}
}
