package server

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/researchreplay"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	selectedReplayMaxFrames = 96
	selectedReplayMaxBytes  = 512 << 10
)

type selectedReplayRuntime struct {
	mu        sync.Mutex
	queue     *researchreplay.SelectionQueue
	tradeBest map[string]float64
	bookAt    map[string]time.Time
}

var selectedReplayRuntimes sync.Map // map[*Server]*selectedReplayRuntime

func selectedReplayRuntimeFor(s *Server) *selectedReplayRuntime {
	if value, ok := selectedReplayRuntimes.Load(s); ok {
		return value.(*selectedReplayRuntime)
	}
	created := &selectedReplayRuntime{queue: researchreplay.NewSelectionQueue(
		selectedReplayMaxFrames, selectedReplayMaxBytes), tradeBest: map[string]float64{},
		bookAt: map[string]time.Time{}}
	value, _ := selectedReplayRuntimes.LoadOrStore(s, created)
	return value.(*selectedReplayRuntime)
}

func (state *selectedReplayRuntime) admitBook(ticker string, at time.Time, critical bool) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !critical && !state.bookAt[ticker].IsZero() && at.Sub(state.bookAt[ticker]) < 500*time.Millisecond {
		return false
	}
	if len(state.bookAt) >= 2048 {
		for key, seen := range state.bookAt {
			if at.Sub(seen) > 10*time.Minute {
				delete(state.bookAt, key)
			}
		}
	}
	state.bookAt[ticker] = at
	return true
}

func (state *selectedReplayRuntime) admitTrade(ticker string, score float64, block bool) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if prior, ok := state.tradeBest[ticker]; ok && !block && score <= prior {
		return false
	}
	if len(state.tradeBest) >= 256 {
		weakKey, weakScore := "", math.Inf(1)
		for key, value := range state.tradeBest {
			if value < weakScore {
				weakKey, weakScore = key, value
			}
		}
		if !block && score <= weakScore {
			return false
		}
		delete(state.tradeBest, weakKey)
	}
	if score > state.tradeBest[ticker] {
		state.tradeBest[ticker] = score
	}
	return true
}

func (state *selectedReplayRuntime) resetAdmission() {
	state.mu.Lock()
	state.tradeBest, state.bookAt = map[string]float64{}, map[string]time.Time{}
	state.mu.Unlock()
}

func replaySequenceState(current, prior *int64) (string, int64) {
	return researchreplay.SequenceTruth(current, prior)
}

func selectedReplayFrame(source, schema, kind, entity, selection string, observed, sourceAt time.Time,
	current, prior *int64, gap int64, payload any) (researchreplay.Frame, error) {
	frame, err := researchreplay.NewFrame(source, schema, kind, entity, selection, payload)
	if err != nil {
		return researchreplay.Frame{}, err
	}
	frame.ObservedAt = observed.UTC()
	frame.SourceAt = sourceAt.UTC()
	if sourceAt.IsZero() {
		frame.SourceClock = "missing"
	} else {
		frame.SourceClock = "venue"
	}
	frame.SourceSequence, frame.PriorSourceSequence = current, prior
	frame.SequenceState, frame.SequenceGap = replaySequenceState(current, prior)
	if frame.SequenceGap != gap {
		return researchreplay.Frame{}, fmt.Errorf("selected replay gap=%d disagrees with adjacent source values=%d", gap, frame.SequenceGap)
	}
	return frame, nil
}

func replayBookLevels(levels []kalshi.OrderbookLevel) []map[string]float64 {
	if len(levels) > 10 {
		levels = levels[:10]
	}
	out := make([]map[string]float64, 0, len(levels))
	for _, level := range levels {
		if level.Price > 0 && level.Price < 1 && level.Size > 0 &&
			!math.IsNaN(level.Price) && !math.IsNaN(level.Size) &&
			!math.IsInf(level.Price, 0) && !math.IsInf(level.Size, 0) {
			out = append(out, map[string]float64{"price": level.Price, "size": level.Size})
		}
	}
	return out
}

// queueKalshiResearchReplay runs on the venue reader and does only bounded canonicalization plus a
// small mutex-protected coalescing offer. It never performs disk, SQLite, HTTP, order, or Paper work.
func (s *Server) queueKalshiResearchReplay(ev kalshi.ResearchReplayEvent) {
	if s == nil || ev.EntityID == "" || ev.ObservedAt.IsZero() {
		return
	}
	// This hook sees every existing subscribed book delta and public trade before the durable
	// replay sampler coalesces it. Both observers are strictly memory-only and bounded. Flow gets
	// first offer so research selection can never add discovery latency to a real-money candidate.
	s.queueKalshiFlowTradeWake(ev)
	s.observeStep7Replay(ev)
	state := selectedReplayRuntimeFor(s)
	common := map[string]any{
		"channel": ev.Channel, "subscription_id": ev.SubscriptionID,
		"source_generation": ev.SourceGeneration, "frame_type": ev.FrameType,
		"research_only": true, "funded": false, "paper_authority": false, "live_authority": false,
	}
	var frame researchreplay.Frame
	var err error
	key, priority, score := "", 0, float64(ev.ObservedAt.UnixNano())
	if ev.SourceSequence != nil {
		score = float64(*ev.SourceSequence)
	}
	switch ev.Kind {
	case "book":
		if ev.Book == nil {
			return
		}
		critical := ev.SequenceGap > 0 || ev.SourceSequence != nil && ev.PriorSourceSequence != nil && *ev.SourceSequence <= *ev.PriorSourceSequence
		if !state.admitBook(ev.EntityID, ev.ObservedAt, critical) {
			return
		}
		common["valid"] = ev.Book.Valid
		common["yes_bid"], common["yes_ask"] = ev.Book.YesBid, ev.Book.YesAsk
		common["yes_bid_size"], common["yes_ask_size"] = ev.Book.YesBidSize, ev.Book.YesAskSize
		common["yes_bids"], common["yes_asks"] = replayBookLevels(ev.Book.YesBids), replayBookLevels(ev.Book.YesAsks)
		selection := "existing sequenced book snapshot/top-price change selected without adding a subscription"
		key, priority = "book:"+ev.EntityID, 50
		if ev.FrameType == "orderbook_snapshot" {
			priority = 55
		}
		if ev.SequenceGap > 0 || (ev.SourceSequence != nil && ev.PriorSourceSequence != nil && *ev.SourceSequence <= *ev.PriorSourceSequence) {
			selection = "exact sequence break/replay invalidated the subscribed book chain"
			key = fmt.Sprintf("book-gap:%d:%d:%d", ev.SourceGeneration, ev.SubscriptionID, *ev.SourceSequence)
			priority = 100
		}
		frame, err = selectedReplayFrame("kalshi-orderbook-ws", "kalshi-orderbook-normalized-v2",
			"book", ev.EntityID, selection, ev.ObservedAt, ev.SourceAt,
			ev.SourceSequence, ev.PriorSourceSequence, ev.SequenceGap, common)
	case "trade":
		if ev.Trade == nil {
			return
		}
		common["trade_id"], common["aggressor"] = ev.Trade.TradeID, ev.Trade.Aggressor
		common["taker_outcome_side"], common["taker_book_side"] = ev.Trade.TakerOutcomeSide, ev.Trade.TakerBookSide
		common["legacy_taker_side"], common["legacy_outcome_side"] = ev.Trade.LegacyTakerSide, ev.Trade.LegacyOutcomeSide
		common["count"], common["yes_price"], common["no_price"] = ev.Trade.Count, ev.Trade.YesPrice, ev.Trade.NoPrice
		common["is_block_trade"] = ev.Trade.Block
		key, priority = "trade:"+ev.EntityID, 30
		notional := ev.Trade.Count * ev.Trade.YesPrice
		if ev.Trade.Aggressor == "no" {
			notional = ev.Trade.Count * ev.Trade.NoPrice
		}
		if !state.admitTrade(ev.EntityID, notional, ev.Trade.Block) {
			return
		}
		score = notional
		if ev.Trade.Block {
			priority = 40
		}
		frame, err = selectedReplayFrame("kalshi-trade-ws", "kalshi-trade-normalized-v2",
			"trade", ev.EntityID, "largest public print retained per market between bounded replay flushes",
			ev.ObservedAt, ev.SourceAt, ev.SourceSequence, ev.PriorSourceSequence, ev.SequenceGap, common)
	case "lifecycle":
		if ev.Lifecycle == nil {
			return
		}
		common["event_type"], common["close_time"] = ev.Lifecycle.EventType, ev.Lifecycle.CloseTime
		common["settled_result"] = ev.Lifecycle.SettledResult
		key = fmt.Sprintf("lifecycle:%s:%s:%d", ev.Channel, ev.EntityID, ev.ObservedAt.UnixNano())
		priority = 80
		frame, err = selectedReplayFrame("kalshi-lifecycle-ws", "kalshi-lifecycle-normalized-v2",
			"lifecycle", ev.EntityID, "rare normalized lifecycle transition retained without raw account/header data",
			ev.ObservedAt, ev.SourceAt, ev.SourceSequence, ev.PriorSourceSequence, ev.SequenceGap, common)
	default:
		return
	}
	if err != nil {
		return
	}
	_, _ = state.queue.Offer(key, frame, priority, score)
}

func (s *Server) queueSourceClockReplay(in storage.SourceClockReceipt, result storage.SourceClockReceiptResult,
	prior *int64, exactGap int64, sourceMeta map[string]any) {
	current := in.SequenceEnd
	sequenceState, derivedGap := replaySequenceState(current, prior)
	if exactGap != derivedGap {
		return // a source adapter may not write contradictory replay truth
	}
	payload := map[string]any{
		"source_id": in.SourceID, "spec_version": result.SpecVersion, "schema_version": in.SchemaVersion,
		"schema_hash": in.SchemaHash, "status": result.Status, "rows_seen": in.RowsSeen,
		"gap_seconds": result.GapSeconds, "regression_seconds": result.RegressionSeconds,
		"sequence_gap": exactGap, "lag_seconds": result.LagSeconds,
		"research_only": true, "funded": false, "paper_authority": false, "live_authority": false,
		"source_meta": sourceMeta, "sequence_state": sequenceState,
	}
	clock := "missing"
	if !in.Watermark.IsZero() {
		clock = "venue"
	} else if kind, _ := sourceMeta["clock_kind"].(string); strings.Contains(kind, "arrival") {
		clock = "arrival_only"
	}
	frame, err := researchreplay.NewFrame(in.SourceID, in.SchemaVersion, "source-clock", in.SourceID,
		"changed source receipt retained with explicit clock and adjacent sequence truth", payload)
	if err != nil {
		return
	}
	frame.ObservedAt, frame.SourceAt, frame.SourceClock = in.Received.UTC(), in.Watermark.UTC(), clock
	frame.SourceSequence, frame.PriorSourceSequence = current, prior
	frame.SequenceState, frame.SequenceGap = sequenceState, exactGap
	key := "source:" + in.SourceID
	priority, score := 70, float64(in.Received.UnixNano())
	if exactGap > 0 || result.RegressionSeconds > 0 || result.SchemaDrift {
		priority = 95
		key = fmt.Sprintf("source-fault:%s:%d", in.SourceID, result.ID)
	}
	_, _ = selectedReplayRuntimeFor(s).queue.Offer(key, frame, priority, score)
}

func (s *Server) drainSelectedResearchReplay() []researchreplay.Frame {
	state := selectedReplayRuntimeFor(s)
	frames := state.queue.Drain()
	state.resetAdmission()
	return frames
}

func (s *Server) requeueSelectedResearchReplay(frames []researchreplay.Frame) {
	queue := selectedReplayRuntimeFor(s).queue
	for i, frame := range frames {
		key := fmt.Sprintf("retry:%s:%s:%d:%d", frame.Source, frame.EntityID, frame.ObservedAt.UnixNano(), i)
		_, _ = queue.Offer(key, frame, 90, float64(frame.ObservedAt.UnixNano()))
	}
}

func (s *Server) selectedResearchReplayStats() researchreplay.SelectionStats {
	return selectedReplayRuntimeFor(s).queue.Stats()
}
