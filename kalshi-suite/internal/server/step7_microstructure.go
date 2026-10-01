package server

// Event-driven Step-7 microstructure observer. It runs before the selected-replay sampler, uses
// only already-subscribed Kalshi public books/trades, and performs no I/O in the callback. Durable
// output is limited to depletion episodes, 5/30/300s marks, and paired direction labels.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	step7BookStateCap   = 2048
	step7EpisodeCap     = 512
	step7EpisodeQueue   = 256
	step7MarkQueue      = 2048
	step7FlowQueue      = 1024
	step7EpisodeRateMin = 120
	step7FlowRateMin    = 600

	// Version 1 horizon rows were sampled only when the shared heavy-research gate admitted the
	// SQLite writer. A busy database could therefore turn a 5s mark into a minutes-late mark.
	// Version 2 advances the clock in bounded RAM on the independent 5s scheduler. Version 3 also
	// commits one byte-frozen projection disposition with every raw row and retries only those
	// bytes. Never merge gate-delayed or non-atomic history with the complete v3 cohort.
	step7TimingExperimentVersion = 3
	step7TimingCohort            = "ram-fixed-horizon-atomic-v3"
	step7TimingMaxLateness       = 6 * time.Second
)

type step7Touch struct {
	Observed, SourceAt time.Time
	Generation         uint64
	SubscriptionID     int64
	Sequence           int64
	YesBid, YesAsk     float64
	YesBidDepth        float64
	YesAskDepth        float64
	TradeYes, TradeNo  float64
}

type step7TrackedEpisode struct {
	row                            storage.Step7ReplenishmentEpisode
	tradeUnits, unexplained, churn float64
	refilled                       bool
	refillAt                       time.Time
	refillLatencyMS                float64
	marks                          map[int64]bool
}

type step7Runtime struct {
	mu                          sync.Mutex
	books                       map[string]step7Touch
	active                      map[string]string // ticker|side -> episode id
	tracked                     map[string]*step7TrackedEpisode
	episodes                    []storage.Step7ReplenishmentEpisode
	marks                       []storage.Step7ReplenishmentMark
	flows                       []storage.Step7FlowDirectionPair
	projections                 []storage.Step7ProjectionJob
	seenFlow                    map[string]time.Time
	recentEpisode, recentFlow   []time.Time
	attemptBooks, attemptTrades int
	exclusions                  map[string]int
	lastPrune, lastMaker        time.Time
}

func (st *step7Runtime) makerDue(now time.Time) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.lastMaker.IsZero() && now.Sub(st.lastMaker) < time.Minute {
		return false
	}
	st.lastMaker = now
	return true
}

var step7Runtimes sync.Map // map[*Server]*step7Runtime

func step7RuntimeFor(s *Server) *step7Runtime {
	if v, ok := step7Runtimes.Load(s); ok {
		return v.(*step7Runtime)
	}
	created := &step7Runtime{books: map[string]step7Touch{}, active: map[string]string{},
		tracked: map[string]*step7TrackedEpisode{}, seenFlow: map[string]time.Time{},
		exclusions: map[string]int{}}
	v, _ := step7Runtimes.LoadOrStore(s, created)
	return v.(*step7Runtime)
}

func step7Hash(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(h[:])
}

func step7Seq(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func step7NormalizeOutcome(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "yes":
		return "yes"
	case "no":
		return "no"
	}
	return ""
}

func step7NormalizeBookSide(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "bid":
		return "yes"
	case "ask":
		return "no"
	}
	return ""
}

func step7AuthoritativeDirection(t *kalshi.ResearchReplayTrade) string {
	if t == nil {
		return "unknown"
	}
	outcome, book := step7NormalizeOutcome(t.TakerOutcomeSide), step7NormalizeBookSide(t.TakerBookSide)
	if outcome != "" && book != "" && outcome != book {
		return "conflict"
	}
	if outcome != "" {
		return outcome
	}
	if book != "" {
		return book
	}
	return "unknown"
}

func step7InferredDirection(t *kalshi.ResearchReplayTrade, book step7Touch, at time.Time) (string, float64, string) {
	if t == nil || book.Observed.IsZero() || at.Before(book.Observed) {
		return "unknown", 0, "book_clock_unavailable"
	}
	age := at.Sub(book.Observed)
	if age > 2*time.Second || book.YesBid <= 0 || book.YesAsk <= book.YesBid || book.YesAsk >= 1 {
		return "unknown", age.Seconds() * 1000, "book_not_same_clock_fresh"
	}
	const tolerance = 1e-7
	if t.YesPrice >= book.YesAsk-tolerance {
		return "yes", age.Seconds() * 1000, "trade_at_or_above_yes_ask"
	}
	if t.YesPrice <= book.YesBid+tolerance {
		return "no", age.Seconds() * 1000, "trade_at_or_below_yes_bid"
	}
	return "unknown", age.Seconds() * 1000, "trade_inside_spread_or_price_unmatched"
}

func step7Comparison(authoritative, inferred string) string {
	if authoritative == "conflict" {
		return "authoritative_conflict"
	}
	if authoritative == "unknown" {
		return "authoritative_unknown"
	}
	if inferred == "unknown" {
		return "inferred_unknown"
	}
	if authoritative == inferred {
		return "agreement"
	}
	return "mismatch"
}

func step7PruneRate(in []time.Time, now time.Time) []time.Time {
	cut := now.Add(-time.Minute)
	i := 0
	for i < len(in) && !in[i].After(cut) {
		i++
	}
	return in[i:]
}

func (st *step7Runtime) appendMark(mark storage.Step7ReplenishmentMark) {
	if len(st.marks) >= step7MarkQueue {
		st.exclusions["mark_queue_full"]++
		return
	}
	st.marks = append(st.marks, mark)
}

func (st *step7Runtime) resetTicker(ticker string, now time.Time, gap int64, reason string) {
	for _, side := range []string{"YES_ASK", "YES_BID"} {
		key := ticker + "|" + side
		id := st.active[key]
		if id == "" {
			continue
		}
		if tracked := st.tracked[id]; tracked != nil {
			st.appendMark(storage.Step7ReplenishmentMark{EpisodeID: id, Observed: now, EventType: "sequence_reset",
				Ticker: tracked.row.Ticker, Side: tracked.row.Side,
				SequenceGap: gap, TradeUnits: tracked.tradeUnits, UnexplainedUnits: tracked.unexplained,
				QueueChurnUnits: tracked.churn, Evidence: map[string]any{"reason": reason}})
		}
		delete(st.active, key)
	}
}

func step7SideTouch(book step7Touch, side string) (float64, float64) {
	if side == "YES_ASK" {
		return book.YesAsk, book.YesAskDepth
	}
	return book.YesBid, book.YesBidDepth
}

func (st *step7Runtime) startEpisode(ticker, side string, prior, current step7Touch, ev kalshi.ResearchReplayEvent,
	tradeUnits float64) {
	now := ev.ObservedAt
	st.recentEpisode = step7PruneRate(st.recentEpisode, now)
	if len(st.recentEpisode) >= step7EpisodeRateMin || len(st.episodes) >= step7EpisodeQueue || len(st.tracked) >= step7EpisodeCap {
		st.exclusions["episode_budget_or_capacity"]++
		return
	}
	startPrice, startDepth := step7SideTouch(prior, side)
	depletedPrice, depletedDepth := step7SideTouch(current, side)
	removed := math.Max(0, startDepth-depletedDepth)
	if math.Abs(startPrice-depletedPrice) > 1e-9 {
		removed = startDepth
	}
	tradeUnits = math.Min(math.Max(0, tradeUnits), removed)
	unexplained := math.Max(0, removed-tradeUnits)
	seq, priorSeq := step7Seq(ev.SourceSequence), step7Seq(ev.PriorSourceSequence)
	id := step7Hash("replenishment", ticker, side, time.Unix(0, now.UnixNano()).UTC().Format(time.RFC3339Nano),
		strings.TrimSpace(ev.Channel), strconv.FormatUint(ev.SourceGeneration, 10), strconv.FormatInt(seq, 10))
	sourceClock := "sequenced_arrival"
	if !ev.SourceAt.IsZero() {
		sourceClock = "venue_source_time"
	}
	row := storage.Step7ReplenishmentEpisode{EpisodeID: id, Ticker: ticker, Side: side, Observed: now,
		Generation: ev.SourceGeneration, SubscriptionID: ev.SubscriptionID, Sequence: seq, PriorSequence: priorSeq,
		SequenceGap: ev.SequenceGap, StartPrice: startPrice, StartDepth: startDepth, DepletedPrice: depletedPrice,
		DepletedDepth: depletedDepth, DepletionUnits: removed, TradeUnits: tradeUnits, UnexplainedUnits: unexplained,
		QueueChurnUnits: removed, SourceClock: sourceClock, Evidence: map[string]any{
			"detection": "top_price_or_25pct_touch_depth_depletion", "cancel_is_proxy": true,
			"all_existing_subscribed_deltas_observed": true, "new_subscriptions": 0}}
	st.episodes = append(st.episodes, row)
	st.recentEpisode = append(st.recentEpisode, now)
	st.tracked[id] = &step7TrackedEpisode{row: row, tradeUnits: tradeUnits, unexplained: unexplained, churn: removed, marks: map[int64]bool{}}
	st.active[ticker+"|"+side] = id
}

func (st *step7Runtime) updateActive(ticker, side string, prior, current step7Touch, now time.Time) {
	id := st.active[ticker+"|"+side]
	if id == "" {
		return
	}
	tracked := st.tracked[id]
	if tracked == nil {
		return
	}
	priorPrice, priorDepth := step7SideTouch(prior, side)
	price, depth := step7SideTouch(current, side)
	if math.Abs(priorPrice-price) <= 1e-9 {
		tracked.churn += math.Abs(depth - priorDepth)
	}
	if side == "YES_ASK" {
		tracked.tradeUnits += current.TradeYes
	} else {
		tracked.tradeUnits += current.TradeNo
	}
	tracked.unexplained = math.Max(tracked.unexplained, tracked.row.DepletionUnits-math.Min(tracked.tradeUnits, tracked.row.DepletionUnits))
	priceRecovered := price > 0
	if side == "YES_ASK" {
		priceRecovered = priceRecovered && price <= tracked.row.StartPrice+1e-9
	} else {
		priceRecovered = priceRecovered && price >= tracked.row.StartPrice-1e-9
	}
	if !tracked.refilled && priceRecovered && depth >= 0.75*tracked.row.StartDepth {
		tracked.refilled = true
		tracked.refillAt = now
		tracked.refillLatencyMS = now.Sub(tracked.row.Observed).Seconds() * 1000
		st.appendMark(storage.Step7ReplenishmentMark{EpisodeID: id, Observed: now, EventType: "refill",
			Ticker: tracked.row.Ticker, Side: tracked.row.Side, Refilled: true,
			BookPrice: price, BookDepth: depth, RefillLatencyMS: tracked.refillLatencyMS,
			RefillFraction: depth / tracked.row.StartDepth, TradeUnits: tracked.tradeUnits,
			UnexplainedUnits: tracked.unexplained, QueueChurnUnits: tracked.churn,
			Evidence: map[string]any{"refill_threshold_fraction": 0.75, "cancel_is_proxy": true}})
		delete(st.active, ticker+"|"+side)
	}
}

func (s *Server) observeStep7Replay(ev kalshi.ResearchReplayEvent) {
	if s == nil || ev.EntityID == "" || ev.ObservedAt.IsZero() {
		return
	}
	st := step7RuntimeFor(s)
	st.mu.Lock()
	defer st.mu.Unlock()
	switch ev.Kind {
	case "trade":
		if ev.Trade == nil || ev.Trade.Count <= 0 || ev.Trade.YesPrice <= 0 || ev.Trade.YesPrice >= 1 {
			return
		}
		st.attemptTrades++
		tradeSeq := step7Seq(ev.SourceSequence)
		tradeID := strings.TrimSpace(ev.Trade.TradeID)
		book := st.books[ev.EntityID]
		if tradeID == "" {
			// Arrival time, socket generation, and sequence are transport receipts, not a durable
			// venue trade identity. Using them as a fallback would count a reconnect replay as a
			// second economic observation. Keep the trade in depletion accounting, but exclude it
			// from the flow candidate/control cohort.
			st.exclusions["flow_trade_id_missing"]++
			authoritative := step7AuthoritativeDirection(ev.Trade)
			if authoritative == "yes" {
				book.TradeYes += ev.Trade.Count
			} else if authoritative == "no" {
				book.TradeNo += ev.Trade.Count
			}
			if !book.Observed.IsZero() {
				st.books[ev.EntityID] = book
			}
			return
		}
		dedupeKey := ev.EntityID + "|" + tradeID
		if st.seenFlow == nil {
			st.seenFlow = map[string]time.Time{}
		}
		if priorSeen, duplicate := st.seenFlow[dedupeKey]; duplicate {
			st.exclusions["flow_trade_replay"]++
			if ev.ObservedAt.After(priorSeen) {
				st.seenFlow[dedupeKey] = ev.ObservedAt
			}
			return
		}
		st.seenFlow[dedupeKey] = ev.ObservedAt
		// Keep reconnect replay protection well beyond a normal socket handoff while bounding RAM.
		if st.attemptTrades%128 == 0 || len(st.seenFlow) > 8192 {
			cut := ev.ObservedAt.Add(-15 * time.Minute)
			for key, seenAt := range st.seenFlow {
				if seenAt.Before(cut) {
					delete(st.seenFlow, key)
				}
			}
			if len(st.seenFlow) > 8192 {
				// This should require a sustained rate above the collector contract. Fail visibly
				// and bound memory; storage's durable source/job identity remains the final replay
				// barrier across a restart.
				st.seenFlow = map[string]time.Time{dedupeKey: ev.ObservedAt}
				st.exclusions["flow_replay_window_capacity_reset"]++
			}
		}
		authoritative := step7AuthoritativeDirection(ev.Trade)
		inferred, ageMS, inferenceBasis := step7InferredDirection(ev.Trade, book, ev.ObservedAt)
		status := step7Comparison(authoritative, inferred)
		st.recentFlow = step7PruneRate(st.recentFlow, ev.ObservedAt)
		if len(st.recentFlow) >= step7FlowRateMin || len(st.flows) >= step7FlowQueue {
			st.exclusions["flow_durable_budget"]++
		} else {
			pairID := step7Hash("flow-direction-v2", ev.EntityID, tradeID)
			clockStatus := "arrival_clock_pair"
			if !ev.SourceAt.IsZero() && !book.SourceAt.IsZero() {
				clockStatus = "venue_times_retained_arrival_order_pair"
			}
			st.flows = append(st.flows, storage.Step7FlowDirectionPair{PairID: pairID, Ticker: ev.EntityID,
				TradeID: tradeID, TakerOutcomeSide: ev.Trade.TakerOutcomeSide, TakerBookSide: ev.Trade.TakerBookSide,
				LegacyTakerSide: ev.Trade.LegacyTakerSide, LegacyOutcomeSide: ev.Trade.LegacyOutcomeSide,
				AuthoritativeSide: authoritative, InferredSide: inferred, ComparisonStatus: status,
				SourceClockStatus: clockStatus, Observed: ev.ObservedAt, SourceAt: ev.SourceAt, BookObserved: book.Observed,
				Count: ev.Trade.Count, YesPrice: ev.Trade.YesPrice, BookAgeMS: math.Max(0, ageMS), YesBid: book.YesBid,
				YesAsk: book.YesAsk, BookGeneration: book.Generation, BookSequence: book.Sequence,
				TradeSequence: tradeSeq, Evidence: map[string]any{"inference_basis": inferenceBasis,
					"canonical_fields_only_define_authoritative": true, "legacy_fields_not_promoted": true}})
			st.recentFlow = append(st.recentFlow, ev.ObservedAt)
		}
		if authoritative == "yes" {
			book.TradeYes += ev.Trade.Count
		} else if authoritative == "no" {
			book.TradeNo += ev.Trade.Count
		}
		if !book.Observed.IsZero() {
			st.books[ev.EntityID] = book
		}
	case "book":
		if ev.Book == nil {
			return
		}
		st.attemptBooks++
		prior, hasPrior := st.books[ev.EntityID]
		seq := step7Seq(ev.SourceSequence)
		current := step7Touch{Observed: ev.ObservedAt, SourceAt: ev.SourceAt, Generation: ev.SourceGeneration,
			SubscriptionID: ev.SubscriptionID, Sequence: seq, YesBid: ev.Book.YesBid, YesAsk: ev.Book.YesAsk,
			YesBidDepth: ev.Book.YesBidSize, YesAskDepth: ev.Book.YesAskSize}
		if !ev.Book.Valid || current.YesBid <= 0 || current.YesAsk <= current.YesBid || current.YesAsk >= 1 ||
			current.YesBidDepth <= 0 || current.YesAskDepth <= 0 {
			st.resetTicker(ev.EntityID, ev.ObservedAt, ev.SequenceGap, "invalid_or_one_sided_book")
			delete(st.books, ev.EntityID)
			return
		}
		if hasPrior && (prior.Generation != current.Generation || ev.SequenceGap > 0 || current.Sequence <= prior.Sequence) {
			st.resetTicker(ev.EntityID, ev.ObservedAt, ev.SequenceGap, "source_generation_or_sequence_chain_reset")
			hasPrior = false
		}
		if hasPrior {
			current.TradeYes, current.TradeNo = prior.TradeYes, prior.TradeNo
			st.updateActive(ev.EntityID, "YES_ASK", prior, current, ev.ObservedAt)
			st.updateActive(ev.EntityID, "YES_BID", prior, current, ev.ObservedAt)
			askDepleted := current.YesAsk > prior.YesAsk+1e-9 || (math.Abs(current.YesAsk-prior.YesAsk) <= 1e-9 &&
				prior.YesAskDepth-current.YesAskDepth >= math.Max(1, 0.25*prior.YesAskDepth))
			bidDepleted := current.YesBid < prior.YesBid-1e-9 || (math.Abs(current.YesBid-prior.YesBid) <= 1e-9 &&
				prior.YesBidDepth-current.YesBidDepth >= math.Max(1, 0.25*prior.YesBidDepth))
			if askDepleted && st.active[ev.EntityID+"|YES_ASK"] == "" {
				st.startEpisode(ev.EntityID, "YES_ASK", prior, current, ev, current.TradeYes)
			}
			if bidDepleted && st.active[ev.EntityID+"|YES_BID"] == "" {
				st.startEpisode(ev.EntityID, "YES_BID", prior, current, ev, current.TradeNo)
			}
		}
		current.TradeYes, current.TradeNo = 0, 0
		st.books[ev.EntityID] = current
		if len(st.books) > step7BookStateCap {
			for ticker, b := range st.books {
				if ev.ObservedAt.Sub(b.Observed) > 10*time.Minute {
					st.resetTicker(ticker, ev.ObservedAt, 0, "bounded_state_eviction")
					delete(st.books, ticker)
				}
			}
			// A burst can contain >cap entirely fresh tickers, so age pruning alone is not a
			// bound. Evict oldest deterministically (ticker tie-break) until the hard cap holds.
			if len(st.books) > step7BookStateCap {
				type rankedBook struct {
					ticker   string
					observed time.Time
				}
				ranked := make([]rankedBook, 0, len(st.books))
				for ticker, b := range st.books {
					ranked = append(ranked, rankedBook{ticker, b.Observed})
				}
				sort.Slice(ranked, func(i, j int) bool {
					if ranked[i].observed.Equal(ranked[j].observed) {
						return ranked[i].ticker < ranked[j].ticker
					}
					return ranked[i].observed.Before(ranked[j].observed)
				})
				for i := 0; i < len(ranked)-step7BookStateCap; i++ {
					st.resetTicker(ranked[i].ticker, ev.ObservedAt, 0, "bounded_state_capacity_eviction")
					delete(st.books, ranked[i].ticker)
					st.exclusions["book_state_capacity_eviction"]++
				}
			}
		}
	}
}

type step7Drain struct {
	episodes                    []storage.Step7ReplenishmentEpisode
	marks                       []storage.Step7ReplenishmentMark
	flows                       []storage.Step7FlowDirectionPair
	projections                 []storage.Step7ProjectionJob
	attemptBooks, attemptTrades int
	exclusions                  map[string]int
	prune                       bool
}

func (st *step7Runtime) advanceTimedEvidenceLocked(now time.Time) {
	for id, tracked := range st.tracked {
		book, ok := st.books[tracked.row.Ticker]
		for _, h := range []int64{5000, 30000, 300000} {
			if tracked.marks[h] || now.Sub(tracked.row.Observed) < time.Duration(h)*time.Millisecond {
				continue
			}
			price, depth := 0.0, 0.0
			if ok {
				price, depth = step7SideTouch(book, tracked.row.Side)
			}
			fraction := 0.0
			if tracked.row.StartDepth > 0 {
				fraction = depth / tracked.row.StartDepth
			}
			target := tracked.row.Observed.Add(time.Duration(h) * time.Millisecond)
			lateness := now.Sub(target)
			if lateness < 0 {
				lateness = 0
			}
			evidence := map[string]any{
				"book_available":          ok,
				"refilled_before_mark":    tracked.refilled,
				"timing_cohort":           step7TimingCohort,
				"timing_version":          step7TimingExperimentVersion,
				"target_ts":               target.UTC().Format(time.RFC3339Nano),
				"sampled_at":              now.UTC().Format(time.RFC3339Nano),
				"sample_lateness_ms":      lateness.Seconds() * 1000,
				"sampled_before_storage":  true,
				"persistence_may_be_late": true,
			}
			if ok {
				evidence["book_observed_ts"] = book.Observed.UTC().Format(time.RFC3339Nano)
				evidence["book_source_ts"] = book.SourceAt.UTC().Format(time.RFC3339Nano)
				evidence["book_source_generation"] = book.Generation
				evidence["book_subscription_id"] = book.SubscriptionID
				evidence["book_source_sequence"] = book.Sequence
				evidence["yes_bid"] = book.YesBid
				evidence["yes_ask"] = book.YesAsk
				evidence["yes_bid_depth"] = book.YesBidDepth
				evidence["yes_ask_depth"] = book.YesAskDepth
			}
			st.appendMark(storage.Step7ReplenishmentMark{EpisodeID: id, Observed: now, EventType: "horizon", HorizonMS: h,
				Ticker: tracked.row.Ticker, Side: tracked.row.Side, Refilled: tracked.refilled,
				BookPrice: price, BookDepth: depth, RefillFraction: fraction, TradeUnits: tracked.tradeUnits,
				UnexplainedUnits: tracked.unexplained, QueueChurnUnits: tracked.churn,
				Evidence: evidence, Target: target, BookObserved: book.Observed, BookSourceAt: book.SourceAt,
				BookGeneration: book.Generation, BookSubscriptionID: book.SubscriptionID,
				BookSourceSequence: book.Sequence, TimingVersion: step7TimingExperimentVersion})
			tracked.marks[h] = true
		}
		if tracked.marks[300000] {
			delete(st.tracked, id)
		}
	}
}

// advanceTimedEvidence is intentionally memory-only. The independent five-second scheduler calls
// it before trying the shared heavy-research gate, so a slow analytic reader or WAL checkpoint can
// delay persistence but cannot move the horizon's sampling clock. The hard tracked/queue caps make
// the mutex hold bounded and the mutex itself prevents overlapping advances.
func (st *step7Runtime) advanceTimedEvidence(now time.Time) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.advanceTimedEvidenceLocked(now)
}

func (st *step7Runtime) drain(now time.Time) step7Drain {
	st.mu.Lock()
	defer st.mu.Unlock()
	// Keep direct/manual callers correct. The normal runtime already advanced on the fast clock;
	// the per-horizon flags make this call idempotent rather than sampling a second time.
	st.advanceTimedEvidenceLocked(now)
	out := step7Drain{episodes: append([]storage.Step7ReplenishmentEpisode(nil), st.episodes...),
		marks: append([]storage.Step7ReplenishmentMark(nil), st.marks...), flows: append([]storage.Step7FlowDirectionPair(nil), st.flows...),
		projections:  append([]storage.Step7ProjectionJob(nil), st.projections...),
		attemptBooks: st.attemptBooks, attemptTrades: st.attemptTrades, exclusions: map[string]int{}}
	for k, v := range st.exclusions {
		out.exclusions[k] = v
	}
	st.episodes = st.episodes[:0]
	st.marks = st.marks[:0]
	st.flows = st.flows[:0]
	st.projections = st.projections[:0]
	st.attemptBooks = 0
	st.attemptTrades = 0
	st.exclusions = map[string]int{}
	if st.lastPrune.IsZero() || now.Sub(st.lastPrune) >= time.Hour {
		st.lastPrune = now
		out.prune = true
	}
	return out
}

// restoreDrain makes a failed durable batch lossless. The drained rows are prepended ahead of any
// callbacks that arrived while SQLite was busy, preserving source order. A repeated failure drains
// and restores the same identities once; it never appends a second copy. INSERT OR IGNORE remains
// the final restart/partial-commit deduplicator.
func (st *step7Runtime) restoreDrain(drain step7Drain) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.exclusions == nil {
		st.exclusions = map[string]int{}
	}
	// The failed batch owns priority because it had already survived the admission caps. Callbacks
	// may fill the now-empty queues while SQLite is busy; retain only the earliest rows that fit
	// behind the failed batch and count every later overflow. This keeps all three hard caps true
	// across arbitrarily many failures.
	episodeRoom := max(0, step7EpisodeQueue-len(drain.episodes))
	if episodeRoom > len(st.episodes) {
		episodeRoom = len(st.episodes)
	}
	droppedEpisodes := append([]storage.Step7ReplenishmentEpisode(nil), st.episodes[episodeRoom:]...)
	st.episodes = append(append(make([]storage.Step7ReplenishmentEpisode, 0, len(drain.episodes)+episodeRoom),
		drain.episodes...), st.episodes[:episodeRoom]...)
	droppedEpisodeIDs := make(map[string]bool, len(droppedEpisodes))
	for _, episode := range droppedEpisodes {
		droppedEpisodeIDs[episode.EpisodeID] = true
		delete(st.tracked, episode.EpisodeID)
		key := episode.Ticker + "|" + episode.Side
		if st.active[key] == episode.EpisodeID {
			delete(st.active, key)
		}
	}
	if len(droppedEpisodes) > 0 {
		st.exclusions["restore_episode_concurrent_overflow"] += len(droppedEpisodes)
	}

	// A mark for a concurrently dropped, never-durable episode would poison the next atomic batch
	// through its foreign key. Remove those first, then apply the independent mark queue cap.
	currentMarks := st.marks[:0]
	orphanMarks := 0
	for _, mark := range st.marks {
		if droppedEpisodeIDs[mark.EpisodeID] {
			orphanMarks++
			continue
		}
		currentMarks = append(currentMarks, mark)
	}
	if orphanMarks > 0 {
		st.exclusions["restore_mark_orphaned_by_episode_overflow"] += orphanMarks
	}
	markRoom := max(0, step7MarkQueue-len(drain.marks))
	if markRoom > len(currentMarks) {
		markRoom = len(currentMarks)
	}
	if dropped := len(currentMarks) - markRoom; dropped > 0 {
		st.exclusions["restore_mark_concurrent_overflow"] += dropped
	}
	st.marks = append(append(make([]storage.Step7ReplenishmentMark, 0, len(drain.marks)+markRoom),
		drain.marks...), currentMarks[:markRoom]...)

	flowRoom := max(0, step7FlowQueue-len(drain.flows))
	if flowRoom > len(st.flows) {
		flowRoom = len(st.flows)
	}
	if dropped := len(st.flows) - flowRoom; dropped > 0 {
		st.exclusions["restore_flow_concurrent_overflow"] += dropped
	}
	st.flows = append(append(make([]storage.Step7FlowDirectionPair, 0, len(drain.flows)+flowRoom),
		drain.flows...), st.flows[:flowRoom]...)
	// A projection payload is frozen before the first raw transaction attempt. Failed batches keep
	// those exact bytes ahead of later unprepared callbacks; retries must not reread a newer book,
	// fee, identity, lifecycle, or wall clock.
	st.projections = append(append(make([]storage.Step7ProjectionJob, 0,
		len(drain.projections)+len(st.projections)), drain.projections...), st.projections...)
	st.attemptBooks += drain.attemptBooks
	st.attemptTrades += drain.attemptTrades
	for reason, count := range drain.exclusions {
		st.exclusions[reason] += count
	}
	if drain.prune {
		// The failed batch never reached its retention pass. Make the next successful drain retry
		// it rather than suppressing pruning for an hour.
		st.lastPrune = time.Time{}
	}
}

func (s *Server) advanceStep7TimedEvidence(now time.Time) {
	if s == nil {
		return
	}
	step7RuntimeFor(s).advanceTimedEvidence(now)
}

func (s *Server) flushStep7Microstructure(ctx context.Context, now time.Time) {
	started := time.Now()
	drain := step7RuntimeFor(s).drain(now)
	receipt := storage.CollectorReceipt{CollectorID: "step7-event-microstructure", CycleID: collectorCycleID(started),
		ExperimentID: "replenishment-fingerprint", ExperimentVersion: step7TimingExperimentVersion,
		Status: "healthy", Started: started,
		Source:        storage.R139Step7EventMicrostructureSource,
		SchemaVersion: "step7-event-microstructure-v3", ExpectedCadence: 5 * time.Second, Exclusions: drain.exclusions,
		Systems: []string{"replenishment-fingerprint", "flow-direction-integrity"}, Attempted: drain.attemptBooks + drain.attemptTrades,
		Eligible: len(drain.episodes) + len(drain.flows), Metrics: map[string]any{
			"book_delta_callbacks": drain.attemptBooks, "trade_callbacks": drain.attemptTrades,
			"episode_queue": len(drain.episodes), "mark_queue": len(drain.marks), "flow_pair_queue": len(drain.flows),
			"new_subscriptions": 0, "raw_tick_archive": false, "cancel_label_authority": "proxy_only",
			"flow_label_evidence": "research_flow_direction_pairs (90d exact pairs) + research_flow_direction_daily (durable day/ticker/status/side totals); no duplicate observer/route mirrors"}}
	defer func() {
		receipt.Completed = time.Now()
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
	}()
	terminalReasons, freezeErr := s.freezeStep7ProjectionJobs(ctx, &drain, time.Now)
	if freezeErr != nil {
		step7RuntimeFor(s).restoreDrain(drain)
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "projection_freeze", freezeErr.Error()
		return
	}
	for reason, count := range terminalReasons {
		receipt.Exclusions["frozen_projection:"+reason] += count
	}
	receipt.Metrics["projection_jobs_prepared"] = len(drain.projections)
	if len(drain.episodes) == 0 && len(drain.marks) == 0 && len(drain.flows) == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no bounded event summaries due this cycle"
	} else {
		epN, markN, flowN, jobN, err := s.store.InsertStep7MicrostructureBatch(ctx,
			drain.episodes, drain.marks, drain.flows, drain.projections)
		if err != nil {
			step7RuntimeFor(s).restoreDrain(drain)
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
			return
		}
		receipt.Inserted = epN + markN + flowN
		receipt.Duplicates = len(drain.episodes) + len(drain.marks) + len(drain.flows) - receipt.Inserted
		receipt.Metrics["episodes_inserted"], receipt.Metrics["marks_inserted"], receipt.Metrics["flow_pairs_inserted"] = epN, markN, flowN
		receipt.Metrics["projection_jobs_inserted"] = jobN
		receipt.Metrics["projection_jobs_duplicate"] = len(drain.projections) - jobN
	}
	projectionResult, projectionErr := s.store.ApplyStep7ProjectionOutbox(ctx, 256)
	receipt.Metrics["projection_jobs_selected"] = projectionResult.Selected
	receipt.Metrics["projection_jobs_completed"] = projectionResult.Completed
	receipt.Metrics["projection_outputs_inserted"] = projectionResult.OutputsInserted
	receipt.Metrics["projection_outputs_duplicate"] = projectionResult.OutputsDuplicate
	receipt.Metrics["projection_jobs_failed"] = projectionResult.Failed
	receipt.Metrics["projection_jobs_pending"] = projectionResult.Pending
	receipt.Metrics["projection_oldest_pending_ms"] = projectionResult.OldestPendingAge.Milliseconds()
	if projectionResult.LastError != "" {
		receipt.Metrics["projection_last_error"] = projectionResult.LastError
	}
	if projectionErr != nil {
		// Raw evidence and its exact payload are already durable. Never restore or rebuild them from
		// current state; a later bounded pass retries only the stored bytes.
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "projection_outbox", projectionErr.Error()
		receipt.Exclusions["projection_outbox_retry_pending"]++
	} else if projectionResult.Pending > 0 {
		receipt.Status, receipt.ExpectedZero, receipt.ZeroReason = "starved", false, ""
		receipt.Exclusions["projection_outbox_backlog"] += projectionResult.Pending
	}
	if drain.prune {
		if n, err := s.store.PruneStep7Evidence(ctx, now.AddDate(0, 0, -90)); err != nil {
			receipt.Exclusions["retention_prune_error"]++
		} else {
			receipt.Metrics["retention_rows_pruned"] = n
		}
	}
}

func step7ReplenishmentActionRule(mark storage.Step7ReplenishmentMark) (bool, string) {
	depleted := mark.TradeUnits + mark.UnexplainedUnits
	switch {
	case mark.Refilled:
		return false, "refilled_before_horizon"
	case mark.TradeUnits <= 0 || depleted <= 0:
		return false, "depletion_has_no_authoritative_trade_direction"
	case mark.UnexplainedUnits > .25*depleted:
		return false, "cancel_or_nontrade_proxy_dominates_depletion"
	default:
		return true, "frozen_persistent_authoritative_depletion_rule_met"
	}
}

// Stable deterministic order for focused tests and receipt inspection.
func step7SortedEpisodeIDs(in map[string]*step7TrackedEpisode) []string {
	out := make([]string, 0, len(in))
	for id := range in {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
