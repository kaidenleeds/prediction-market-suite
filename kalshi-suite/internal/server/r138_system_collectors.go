package server

// R138 build-order Steps 6-9.  Every path here is prospective research only: it reads existing
// venue caches/public metadata, writes append-only research_* ledgers, and has no paper, LIVE,
// arming, sizing, promotion, mirror or order function in its call graph.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/deribit"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/nbm"
	"github.com/kalshi-suite/kalshi-suite/internal/payoffsolver"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type r138PendingBookFrame struct {
	frame      storage.ResearchMicrostructureFrame
	capturedAt time.Time
}

const (
	// Research must not turn the trading DB into a tick archive. The callback considers each book
	// at most once/minute, stores transitions plus a sparse unchanged heartbeat, and admits at most
	// 60 frames/minute globally. Durable caps below stop collection before unbounded growth; a real
	// archive must be built before either cap is raised.
	r138BookSampleInterval         = time.Minute
	r138BookHeartbeat              = 15 * time.Minute
	r138BookBudgetWindow           = time.Minute
	r138BookSampleBudget           = 60
	r138BookPendingLimit           = 120
	r138BookPendingRetention       = 2 * time.Minute
	r138BookDailyRowCap            = 5000
	r138BookLifetimeRowCap         = 100000
	r138IncentiveProgramCap        = 20
	r138MaxPortfolioLegs           = 6
	r144PolyUSIncentiveBudget      = 25 * time.Second
	r144PolyUSIncentiveLastGoodAge = 45 * time.Minute
)

type r138CollectorRuntime struct {
	mu                                      sync.Mutex
	lastCapture                             map[string]time.Time
	lastBookHash                            map[string]string
	lastBookStored                          map[string]time.Time
	pending                                 []r138PendingBookFrame
	recentHeavyAttempts                     []time.Time
	recentAdmissions                        []time.Time
	captureAttempts, captureAdmitted        int
	captureEnqueued                         int
	captureExclusions                       map[string]int
	lastFlush, lastFallback, lastScoreState time.Time
	lastCoverage, lastDeadline, lastRelease time.Time
	lastSystemFunnels                       time.Time
	systemFunnelNext                        int
	systemFunnelCycleActive                 bool
	incentiveCursor                         int
	polyUSIncentiveLastGood                 []polymarketus.IncentiveMarket
	polyUSIncentiveLastGoodAt               time.Time
}

type r138CaptureAccounting struct {
	Attempts, Admitted, Enqueued int
	Exclusions                   map[string]int
}

var r138CollectorStates = struct {
	sync.Mutex
	byServer map[*Server]*r138CollectorRuntime
}{byServer: map[*Server]*r138CollectorRuntime{}}

func (s *Server) r138CollectorState() *r138CollectorRuntime {
	r138CollectorStates.Lock()
	defer r138CollectorStates.Unlock()
	st := r138CollectorStates.byServer[s]
	if st == nil {
		st = &r138CollectorRuntime{lastCapture: map[string]time.Time{}, lastBookHash: map[string]string{},
			lastBookStored: map[string]time.Time{}, captureExclusions: map[string]int{}}
		r138CollectorStates.byServer[s] = st
	}
	return st
}

func (st *r138CollectorRuntime) addCaptureExclusionLocked(reason string, count int) {
	if count <= 0 {
		return
	}
	if st.captureExclusions == nil {
		st.captureExclusions = map[string]int{}
	}
	st.captureExclusions[reason] += count
}

func (st *r138CollectorRuntime) prunePendingLocked(now time.Time) {
	cutoff := now.Add(-r138BookPendingRetention)
	kept := st.pending[:0]
	expired := 0
	for _, pending := range st.pending {
		if pending.capturedAt.Before(cutoff) {
			expired++
			continue
		}
		kept = append(kept, pending)
	}
	st.pending = kept
	st.addCaptureExclusionLocked("pending_retention_expired", expired)
}

// admitR138BookSample is the single sampler budget gate. Every rejected callback is accounted for
// and later copied into the immutable collector receipt; there is no silent slice truncation.
func (st *r138CollectorRuntime) admitR138BookSample(ticker string, now time.Time) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.captureAttempts++
	st.prunePendingLocked(now)
	if last := st.lastCapture[ticker]; !last.IsZero() && now.Sub(last) >= 0 && now.Sub(last) < r138BookSampleInterval {
		st.addCaptureExclusionLocked("sample_interval_coalesced", 1)
		return false
	}
	cutoff := now.Add(-r138BookBudgetWindow)
	keep := 0
	for keep < len(st.recentHeavyAttempts) && !st.recentHeavyAttempts[keep].After(cutoff) {
		keep++
	}
	st.recentHeavyAttempts = st.recentHeavyAttempts[keep:]
	if len(st.recentHeavyAttempts) >= r138BookSampleBudget {
		st.lastCapture[ticker] = now
		st.addCaptureExclusionLocked("heavy_path_budget_exhausted", 1)
		return false
	}
	st.recentHeavyAttempts = append(st.recentHeavyAttempts, now)
	st.lastCapture[ticker] = now
	if len(st.lastCapture) > 5000 {
		for key, at := range st.lastCapture {
			if now.Sub(at) > 15*time.Minute {
				delete(st.lastCapture, key)
				delete(st.lastBookHash, key)
				delete(st.lastBookStored, key)
			}
		}
	}
	return true
}

func (st *r138CollectorRuntime) rejectR138BookSample(reason string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.addCaptureExclusionLocked(reason, 1)
}

func (st *r138CollectorRuntime) enqueueR138BookFrame(frame storage.ResearchMicrostructureFrame, capturedAt time.Time) bool {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.prunePendingLocked(capturedAt)
	hash := r138MicroBookHash(frame)
	if st.lastBookHash[frame.Ticker] == hash && capturedAt.Sub(st.lastBookStored[frame.Ticker]) >= 0 &&
		capturedAt.Sub(st.lastBookStored[frame.Ticker]) < r138BookHeartbeat {
		st.addCaptureExclusionLocked("unchanged_between_heartbeats", 1)
		return false
	}
	cutoff := capturedAt.Add(-r138BookBudgetWindow)
	keep := 0
	for keep < len(st.recentAdmissions) && !st.recentAdmissions[keep].After(cutoff) {
		keep++
	}
	st.recentAdmissions = st.recentAdmissions[keep:]
	if len(st.recentAdmissions) >= r138BookSampleBudget {
		st.addCaptureExclusionLocked("sample_window_budget_exhausted", 1)
		return false
	}
	if len(st.pending) >= r138BookPendingLimit {
		st.addCaptureExclusionLocked("pending_capacity_rejected", 1)
		return false
	}
	st.pending = append(st.pending, r138PendingBookFrame{frame: frame, capturedAt: capturedAt})
	st.lastBookHash[frame.Ticker], st.lastBookStored[frame.Ticker] = hash, capturedAt
	st.recentAdmissions = append(st.recentAdmissions, capturedAt)
	st.captureAdmitted++
	st.captureEnqueued++
	return true
}

func (st *r138CollectorRuntime) drainR138BookFrames(now time.Time) ([]r138PendingBookFrame, r138CaptureAccounting) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.prunePendingLocked(now)
	pending := append([]r138PendingBookFrame(nil), st.pending...)
	st.pending = st.pending[:0]
	exclusions := make(map[string]int, len(st.captureExclusions))
	for reason, count := range st.captureExclusions {
		exclusions[reason] = count
	}
	accounting := r138CaptureAccounting{Attempts: st.captureAttempts, Admitted: st.captureAdmitted,
		Enqueued: st.captureEnqueued, Exclusions: exclusions}
	st.captureAttempts, st.captureAdmitted, st.captureEnqueued = 0, 0, 0
	st.captureExclusions = map[string]int{}
	return pending, accounting
}

func r138BookLevels(in []kalshi.OrderbookLevel) []storage.ResearchBookLevel {
	if len(in) > 10 {
		in = in[:10]
	}
	out := make([]storage.ResearchBookLevel, 0, len(in))
	for _, level := range in {
		if level.Price > 0 && level.Price < 1 && level.Size > 0 {
			out = append(out, storage.ResearchBookLevel{Price: level.Price, Size: level.Size})
		}
	}
	return out
}

func r138JoinAuthorities(values ...string) string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return strings.Join(out, "+")
}

func r138MarketClose(m kalshi.Market) time.Time {
	if t, ok := parseTime(strings.TrimSpace(m.CloseTime)); ok {
		return t
	}
	return time.Time{}
}

// captureR138KalshiBookTick is intentionally memory-only and coalesced.  The one-line hook in
// onKalshiTick never writes SQLite or issues REST, so research cannot delay cancel/risk handling.
func (s *Server) captureR138KalshiBookTick(ticker string) {
	if s == nil || s.kal == nil || strings.TrimSpace(ticker) == "" {
		return
	}
	now := time.Now()
	st := s.r138CollectorState()
	if !st.admitR138BookSample(ticker, now) {
		return
	}
	book, _, ok := s.kal.LiveBook(ticker, 3*time.Second)
	if !ok || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 {
		st.rejectR138BookSample("book_unavailable_or_one_sided")
		return
	}
	provenance, ok := s.kal.LiveBookProvenance(ticker, 3*time.Second)
	if !ok || provenance.ReceivedAt.IsZero() || provenance.Generation == 0 ||
		provenance.SubscriptionID <= 0 || provenance.Sequence <= 0 {
		st.rejectR138BookSample("venue_source_clock_or_sequence_unknown")
		return
	}
	// Kalshi orderbook snapshots/deltas do not always include ts/ts_ms. The WS book builder already
	// rejects sequence regressions within generation+subscription, so a positive sequence plus the
	// fresh ReceivedAt is authoritative ordering/liveness even though it is not venue event-time.
	// Preserve that distinction in the channel/source labels; never relabel arrival as SourceAt.
	observed, sourceChannel, bookSource := provenance.SourceAt, provenance.Channel, "kalshi_book_ws_source_time"
	if observed.IsZero() {
		observed = provenance.ReceivedAt
		sourceChannel += ":sequenced-arrival"
		bookSource = "kalshi_book_ws_sequenced_arrival"
	}
	quoteAge := now.Sub(observed)
	if quoteAge < 0 || quoteAge > 30*time.Second ||
		(!provenance.SourceAt.IsZero() && provenance.ReceivedAt.Before(provenance.SourceAt)) {
		st.rejectR138BookSample("venue_source_clock_ahead_of_local_receipt")
		return
	}
	m, ok := s.kmkt(ticker)
	if !ok || (m.Status != "" && !strings.EqualFold(m.Status, "active")) || m.Result != "" {
		st.rejectR138BookSample("market_not_active")
		return
	}
	yesBid, yesAsk := book.YesBids[0], book.YesAsks[0]
	if yesBid.Price <= 0 || yesAsk.Price <= yesBid.Price || yesAsk.Price >= 1 || yesBid.Size <= 0 || yesAsk.Size <= 0 {
		st.rejectR138BookSample("invalid_top_of_book")
		return
	}
	ticks := []float64{}
	for _, px := range []float64{yesBid.Price, yesAsk.Price, 1 - yesAsk.Price, 1 - yesBid.Price} {
		if tick, known := m.TickForKnown(px); known && tick > 0 {
			ticks = append(ticks, tick)
		}
	}
	if len(ticks) != 4 {
		st.rejectR138BookSample("tick_schema_unknown")
		return
	}
	tick := ticks[0]
	for _, candidate := range ticks[1:] {
		if candidate < tick {
			tick = candidate
		}
	}
	yesTF, ytok, yts := s.kalFeeExact(ticker, false, 1, yesAsk.Price)
	noTF, ntok, nts := s.kalFeeExact(ticker, false, 1, 1-yesBid.Price)
	yesMF, ymok, yms := s.kalFeeExact(ticker, true, 1, yesBid.Price)
	noMF, nmok, nms := s.kalFeeExact(ticker, true, 1, 1-yesAsk.Price)
	if !ytok || !ntok || !ymok || !nmok {
		st.rejectR138BookSample("route_fee_unknown")
		return
	}
	// Book and trade source times now share the venue clock. Local ReceivedAt is persisted only for
	// latency measurement and is never substituted when the venue omits ts_ms/ts.
	f := storage.ResearchMicrostructureFrame{
		Observed: observed, Received: provenance.ReceivedAt, CloseTime: r138MarketClose(m), Ticker: ticker,
		SourceChannel: sourceChannel, SourceGeneration: int64(provenance.Generation),
		SourceSubscriptionID: provenance.SubscriptionID, SourceSequence: provenance.Sequence,
		CanonicalEventID: "venue:kalshi:" + firstNonEmpty(m.EventTicker, ticker),
		MarketStatus:     m.Status, BookSource: bookSource, FeeSource: r138JoinAuthorities(yts, nts, yms, nms),
		QuoteAge: quoteAge.Seconds(), TickSize: tick, YesBid: yesBid.Price, YesAsk: yesAsk.Price,
		YesBidDepth: yesBid.Size, YesAskDepth: yesAsk.Size, BidLevels: r138BookLevels(book.YesBids),
		AskLevels: r138BookLevels(book.YesAsks), YesTakerFee: yesTF, NoTakerFee: noTF,
		YesMakerFee: yesMF, NoMakerFee: noMF,
	}
	_ = st.enqueueR138BookFrame(f, now)
}

func r138TradeTime(t kalshi.Trade) (time.Time, bool) {
	when, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(t.CreatedTime))
	return when, err == nil
}

func r138AttachFlow(frames []r138PendingBookFrame, tape []kalshi.Trade) {
	byTicker := map[string][]kalshi.Trade{}
	for _, trade := range tape {
		if trade.Ticker != "" {
			byTicker[trade.Ticker] = append(byTicker[trade.Ticker], trade)
		}
	}
	for i := range frames {
		f := &frames[i].frame
		for _, trade := range byTicker[f.Ticker] {
			when, ok := r138TradeTime(trade)
			if !ok || when.After(f.Observed) || when.Before(f.Observed.Add(-60*time.Second)) {
				continue
			}
			units, sign := trade.Count.Float(), 0.0
			switch trade.Aggressor() {
			case "yes":
				sign = 1
			case "no":
				sign = -1
			default:
				continue
			}
			f.SignedFlow60 += sign * units
			f.FlowUnits60 += units
			if !when.Before(f.Observed.Add(-10 * time.Second)) {
				f.SignedFlow10 += sign * units
				f.FlowUnits10 += units
			}
		}
	}
}

func r138CapacitySizes(depth float64) []float64 {
	if depth < 1 || math.IsNaN(depth) || math.IsInf(depth, 0) {
		return nil
	}
	cap := math.Min(math.Floor(depth+1e-9), 1000)
	base := []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000}
	var out []float64
	for _, size := range base {
		if size <= cap {
			out = append(out, size)
		}
	}
	if len(out) == 0 || math.Abs(out[len(out)-1]-cap) > 1e-9 {
		out = append(out, cap)
	}
	return out
}

func r138DepthCostFee(sizes []float64, levels []storage.ResearchBookLevel,
	fee func(float64, float64) (float64, bool)) ([]storage.CapacityPoint, bool) {
	points := make([]storage.CapacityPoint, 0, len(sizes))
	for _, size := range sizes {
		remaining, cost, fees := size, 0.0, 0.0
		for _, level := range levels {
			take := math.Min(remaining, level.Size)
			if take <= 0 {
				continue
			}
			f, ok := fee(take, level.Price)
			if !ok {
				return nil, false
			}
			cost += take * level.Price
			fees += f
			remaining -= take
			if remaining <= 1e-9 {
				break
			}
		}
		if remaining > 1e-9 {
			continue
		}
		points = append(points, storage.CapacityPoint{Size: size, Cost: cost, Fee: fees,
			PayoutFloor: 0, NetFloor: -cost - fees})
	}
	return points, len(points) > 0
}

func r138NoAskLevels(bids []storage.ResearchBookLevel) []storage.ResearchBookLevel {
	out := make([]storage.ResearchBookLevel, 0, len(bids))
	for _, level := range bids {
		out = append(out, storage.ResearchBookLevel{Price: 1 - level.Price, Size: level.Size})
	}
	return out
}

func r138FrameClockID(f storage.ResearchMicrostructureFrame) string {
	return fmt.Sprintf("%s:g%d:sid%d:seq%d", f.SourceChannel, f.SourceGeneration,
		f.SourceSubscriptionID, f.SourceSequence)
}

func r138FrameDecisionLatencyMS(f storage.ResearchMicrostructureFrame) float64 {
	// quoteAge=(decision-source); receiveDelay=(receipt-source), so their difference is the
	// measured receipt-to-decision latency without mixing venue and local clock offsets.
	latency := (f.QuoteAge - f.Received.Sub(f.Observed).Seconds()) * 1000
	return math.Max(0, latency)
}

func (s *Server) r138DirectionalObservation(ctx context.Context, id int64, transition string,
	f storage.ResearchMicrostructureFrame, bookHash string) (int, int) {
	inserted, duplicates := 0, 0
	_, _, _, _ = id, transition, f, bookHash
	// Authoritative flow sign is a data-control row even when no directional system exists.
	if _, ok, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: f.Observed, SystemID: "flow-direction-integrity", OpportunityID: f.Ticker + "|" + bookHash,
		Kind: "control", Cohort: "kalshi-authoritative-aggressor", CanonicalEventID: f.CanonicalEventID,
		Venue: "kalshi", Route: "observer", CertificateStatus: "not_applicable",
		SourceClockID: r138FrameClockID(f), SourceArtifact: "Kalshi WS trade taker_outcome_side/taker_book_side",
		PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "open",
		Blocker: "markout_and_settlement_evidence_pending", Inputs: map[string]any{
			"ticker": f.Ticker, "transition": transition, "signed_flow_10s": f.SignedFlow10,
			"flow_units_10s": f.FlowUnits10, "signed_flow_60s": f.SignedFlow60, "flow_units_60s": f.FlowUnits60,
			"source_generation": f.SourceGeneration, "source_subscription_id": f.SourceSubscriptionID,
			"source_sequence": f.SourceSequence,
		},
	}); err == nil && ok {
		inserted++
	} else if err == nil {
		duplicates++
	}
	if transition == "baseline" || transition == "unchanged" {
		return inserted, duplicates
	}
	side := ""
	switch {
	case transition == "ask_depletion" && f.SignedFlow10 > 0:
		side = "YES"
	case transition == "bid_depletion" && f.SignedFlow10 < 0:
		side = "NO"
	case transition == "ask_refill":
		side = "NO"
	case transition == "bid_refill":
		side = "YES"
	}
	if side == "" {
		return inserted, duplicates
	}
	levels, ask, feeOne := f.AskLevels, f.YesAsk, f.YesTakerFee
	feeAt := func(q, px float64) (float64, bool) {
		fee, known, _ := s.kalFeeExact(f.Ticker, false, q, px)
		return fee, known
	}
	if side == "NO" {
		levels, ask, feeOne = r138NoAskLevels(f.BidLevels), 1-f.YesBid, f.NoTakerFee
	}
	depth := 0.0
	for _, l := range levels {
		depth += l.Size
	}
	curve, curveOK := r138DepthCostFee(r138CapacitySizes(depth), levels, feeAt)
	if !curveOK {
		return inserted, duplicates
	}
	upper := 1 - ask - feeOne
	if _, ok, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: f.Observed, SystemID: "replenishment-fingerprint", OpportunityID: f.Ticker + "|" + bookHash,
		Kind: "control", Cohort: transition, CanonicalEventID: f.CanonicalEventID, Venue: "kalshi",
		Ticker: f.Ticker, Route: "taker", Side: side, CertificateStatus: "unverified", SourceClockID: r138FrameClockID(f),
		SourceArtifact: "sequenced current book + authoritative aggressor tape", BookSource: f.BookSource,
		FeeSource: f.FeeSource, QuoteAgeMax: f.QuoteAge, TickMin: f.TickSize, Size: 1, Cost: ask, Fee: feeOne,
		PayoutLower: 0, PayoutUpper: 1, NetLower: -ask - feeOne, NetUpper: upper,
		VisibleCapacity: depth, CapacityCurve: curve, OutcomeStatus: "open",
		DecisionLatencyMS: r138FrameDecisionLatencyMS(f), LatencyKnown: true,
		QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
		Blocker: "directional_and_untouched_holdout_evidence_pending", Inputs: map[string]any{
			"transition": transition, "signed_flow_10s": f.SignedFlow10, "flow_units_10s": f.FlowUnits10,
			"maker_and_taker_are_separate": true,
		},
	}); err == nil && ok {
		inserted++
	} else if err == nil {
		duplicates++
	}
	// A matched no-order maker control cannot earn a hypothetical fill. Its payoff interval is the
	// would-fill economics, while the blocker and control label keep realized P&L at zero.
	makerPrice, makerFee := f.YesBid, f.YesMakerFee
	if side == "NO" {
		makerPrice, makerFee = 1-f.YesAsk, f.NoMakerFee
	}
	makerCurve := []storage.CapacityPoint{{Size: 1, Cost: makerPrice, Fee: makerFee,
		PayoutFloor: 0, NetFloor: -makerPrice - makerFee}}
	if _, ok, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: f.Observed, SystemID: "maker-salvage-matched-cohort", OpportunityID: f.Ticker + "|" + bookHash,
		Kind: "control", Cohort: "matched-no-order", CanonicalEventID: f.CanonicalEventID, Venue: "kalshi",
		Ticker: f.Ticker, Route: "maker-control", Side: side, CertificateStatus: "unverified", SourceClockID: r138FrameClockID(f),
		SourceArtifact: "current post price; no order submitted", BookSource: f.BookSource, FeeSource: f.FeeSource,
		QuoteAgeMax: f.QuoteAge, TickMin: f.TickSize, Size: 1, Cost: makerPrice, Fee: makerFee,
		PayoutLower: 0, PayoutUpper: 1, NetLower: -makerPrice - makerFee, NetUpper: 1 - makerPrice - makerFee,
		VisibleCapacity: 0, CapacityCurve: makerCurve, OutcomeStatus: "open",
		DecisionLatencyMS: r138FrameDecisionLatencyMS(f), LatencyKnown: true,
		QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
		Blocker: "no_natural_fill_or_queue_receipt; matched_control_realized_pnl_is_zero",
		Inputs:  map[string]any{"transition": transition, "control_realized_net": 0, "hypothetical_fill_not_counted": true},
	}); err == nil && ok {
		inserted++
	} else if err == nil {
		duplicates++
	}
	return inserted, duplicates
}

func r138MicroBookHash(f storage.ResearchMicrostructureFrame) string {
	return storage.R138HashJSON(struct {
		Tick float64                     `json:"tick"`
		Bids []storage.ResearchBookLevel `json:"bids"`
		Asks []storage.ResearchBookLevel `json:"asks"`
	}{f.TickSize, f.BidLevels, f.AskLevels})
}

func (s *Server) flushR138BookFrames(ctx context.Context) {
	started := time.Now()
	receipt := storage.CollectorReceipt{
		CollectorID: "replenishment-toxicity", CycleID: collectorCycleID(started),
		ExperimentID: "replenishment-fingerprint", ExperimentVersion: 1, Status: "healthy",
		Source:        "existing Kalshi sequenced WS books + authoritative aggressor tape; no added subscriptions",
		SchemaVersion: storage.R138EvidenceSchemaVersion(), Started: started, ExpectedCadence: 5 * time.Second,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"replenishment-fingerprint", "flow-direction-integrity", "maker-salvage-matched-cohort"},
	}
	defer func() {
		selectionDrops := 0
		for _, reason := range []string{"sample_interval_coalesced", "unchanged_between_heartbeats",
			"heavy_path_budget_exhausted", "sample_window_budget_exhausted", "pending_capacity_rejected", "pending_retention_expired",
			"durable_row_cap_rejected"} {
			selectionDrops += receipt.Exclusions[reason]
		}
		receipt.Metrics["selection_drop_count"] = selectionDrops
		receipt.Metrics["source_sequence_gaps"] = receipt.ReplayGaps
		receipt.Completed = time.Now()
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
	}()
	st := s.r138CollectorState()
	pending, accounting := st.drainR138BookFrames(started)
	receipt.Attempted, receipt.Eligible = accounting.Attempts, accounting.Admitted
	for reason, count := range accounting.Exclusions {
		receipt.Exclusions[reason] += count
	}
	receipt.Metrics["sampling"] = map[string]any{
		"per_book_interval_ms":   r138BookSampleInterval.Milliseconds(),
		"budget_window_ms":       r138BookBudgetWindow.Milliseconds(),
		"budget_per_window":      r138BookSampleBudget,
		"heavy_paths_per_window": r138BookSampleBudget,
		"pending_limit":          r138BookPendingLimit,
		"pending_retention_ms":   r138BookPendingRetention.Milliseconds(),
		"callbacks":              accounting.Attempts,
		"admitted":               accounting.Admitted,
		"enqueued":               accounting.Enqueued,
		"drained":                len(pending),
	}
	if len(pending) == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no explicitly sampled fresh complete subscribed book survived this cycle; exclusions and sampler budget are receipted"
		return
	}
	totalRows, todayRows, err := s.store.ResearchMicrostructureFrameCounts(ctx, started)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage_budget", err.Error()
		return
	}
	// The hot table is a bounded working set, not a lifetime fuse.  Before it reaches the
	// durable cap, move completed >7d raw frames and all of their horizon receipts to the
	// two-phase archive. Derived route observations remain in the main inference ledger.
	archivedFrames, archivedHorizons := int64(0), int64(0)
	if totalRows >= r138BookLifetimeRowCap-r138BookDailyRowCap {
		archivedFrames, archivedHorizons, err = s.store.ArchiveResearchMicrostructure(ctx, 0, 0)
		if err != nil {
			receipt.Status, receipt.ErrorClass = "blocked", "retention_archive_failed"
			receipt.ErrorText = err.Error()
			receipt.Exclusions["durable_archive_failed"] += len(pending)
			return
		}
		if archivedFrames > 0 {
			totalRows, todayRows, err = s.store.ResearchMicrostructureFrameCounts(ctx, started)
			if err != nil {
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage_budget", err.Error()
				return
			}
		}
	}
	remaining := min(r138BookLifetimeRowCap-totalRows, r138BookDailyRowCap-todayRows)
	receipt.Metrics["durable_budget"] = map[string]any{
		"lifetime_rows": totalRows, "lifetime_cap": r138BookLifetimeRowCap,
		"utc_day_rows": todayRows, "utc_day_cap": r138BookDailyRowCap,
		"archive_implemented": true, "archive_after_days": storage.MicrostructureArchiveAfterDays,
		"archived_frames_this_cycle": archivedFrames, "archived_horizons_this_cycle": archivedHorizons,
	}
	if remaining <= 0 {
		if todayRows >= r138BookDailyRowCap {
			receipt.Status, receipt.ExpectedZero = "healthy_empty", true
			receipt.ZeroReason = "the explicit UTC-day raw-frame budget is complete; collection resumes in the next UTC day"
		} else {
			receipt.Status, receipt.ErrorClass = "blocked", "hot_window_budget_reached"
			receipt.ErrorText = "the seven-day hot working set still exceeds its lifetime guard after a successful archive pass"
		}
		receipt.Exclusions["durable_row_cap_rejected"] += len(pending)
		return
	}
	if len(pending) > remaining {
		dropped := len(pending) - remaining
		pending = pending[:remaining]
		receipt.Exclusions["durable_row_cap_rejected"] += dropped
	}
	tape, freshTape := s.kal.LiveTape()
	if freshTape {
		r138AttachFlow(pending, tape)
	} else {
		receipt.Exclusions["authoritative_trade_tape_stale"] = len(pending)
	}
	transitionCounts := map[string]int{}
	derived, derivedDup := 0, 0
	frames := make([]storage.ResearchMicrostructureFrame, len(pending))
	for i := range pending {
		frames[i] = pending[i].frame
	}
	results, err := s.store.InsertResearchMicrostructureFrames(ctx, frames)
	if err != nil {
		receipt.Exclusions["frame_batch_storage_error"] += len(frames)
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	for i, result := range results {
		f := pending[i].frame
		transitionCounts[result.Transition]++
		if result.Inserted {
			receipt.Inserted++
			inserted, duplicates := s.r138DirectionalObservation(ctx, result.ID, result.Transition, f, r138MicroBookHash(f))
			derived, derivedDup = derived+inserted, derivedDup+duplicates
		} else {
			receipt.Duplicates++
		}
	}
	receipt.Metrics["transition_counts"] = transitionCounts
	receipt.Metrics["derived_system_rows"] = derived
	receipt.Metrics["derived_duplicates"] = derivedDup
	receipt.Metrics["trade_tape_fresh"] = freshTape
}

func r138HorizonTolerance(h int64) time.Duration {
	switch h {
	case 5:
		return 4 * time.Second
	case 30:
		return 6 * time.Second
	default:
		return 15 * time.Second
	}
}

func (s *Server) settleR138BookHorizons(ctx context.Context) {
	now := time.Now()
	due, err := s.store.ResearchMicrostructureHorizonsDue(ctx, now, 300)
	if err != nil {
		return
	}
	for _, row := range due {
		target := row.Observed.Add(time.Duration(row.Horizon) * time.Second)
		if now.Before(target) {
			continue
		}
		if now.After(target.Add(r138HorizonTolerance(row.Horizon))) {
			_, _ = s.store.InsertResearchMicrostructureHorizon(ctx, storage.ResearchMicrostructureHorizon{
				FrameID: row.FrameID, Horizon: row.Horizon, Captured: now, Status: "missed",
				Reason: "fixed horizon passed before this bounded sweep captured a fresh complete book",
			})
			continue
		}
		book, age, ok := s.kal.LiveBook(row.Ticker, 3*time.Second)
		if !ok || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 {
			continue
		}
		yesBid, yesAsk := book.YesBids[0].Price, book.YesAsks[0].Price
		if yesBid <= 0 || yesAsk <= yesBid || yesAsk >= 1 {
			continue
		}
		noBid, noAsk := 1-yesAsk, 1-yesBid
		yesExit, yok, _ := s.kalFeeExactAction(row.Ticker, false, 1, yesBid, false)
		noExit, nok, _ := s.kalFeeExactAction(row.Ticker, false, 1, noBid, false)
		if !yok || !nok {
			continue
		}
		quoteAge := age.Seconds()
		_, _ = s.store.InsertResearchMicrostructureHorizon(ctx, storage.ResearchMicrostructureHorizon{
			FrameID: row.FrameID, Horizon: row.Horizon, Captured: now, Status: "captured",
			YesBid: &yesBid, YesAsk: &yesAsk, NoBid: &noBid, NoAsk: &noAsk,
			YesExitFee: &yesExit, NoExitFee: &noExit, QuoteAge: &quoteAge,
		})
	}
}

// fallbackR138BookCapture is a no-network liveness backstop for a cold ticker callback hook. It
// samples only books the existing WS already tracks and runs at most once per minute.
func (s *Server) fallbackR138BookCapture() {
	if s == nil || s.kal == nil {
		return
	}
	now := time.Now()
	type candidate struct {
		ticker string
		rank   string
	}
	var rows []candidate
	s.metaMu.Lock()
	for ticker, m := range s.kmkts {
		if len(rows) >= 5000 {
			break
		}
		if m.Result != "" || (m.Status != "" && !strings.EqualFold(m.Status, "active")) {
			continue
		}
		h := sha256.Sum256([]byte(now.UTC().Format("200601021504") + "|" + ticker))
		rows = append(rows, candidate{ticker: ticker, rank: hex.EncodeToString(h[:8])})
	}
	s.metaMu.Unlock()
	sort.Slice(rows, func(i, j int) bool { return rows[i].rank < rows[j].rank })
	if len(rows) > 64 {
		rows = rows[:64]
	}
	for _, row := range rows {
		s.captureR138KalshiBookTick(row.ticker)
	}
}

func r138SolverSizes(depth float64) []float64 { return r138CapacitySizes(depth) }

func r138SolverFeeQuotes(sizes []float64, levelIndex int, levels []kalshi.OrderbookLevel,
	fee func(float64, float64) (float64, bool)) ([]payoffsolver.FeeQuote, bool) {
	quantities := map[float64]bool{}
	priorDepth := 0.0
	for i := 0; i < levelIndex; i++ {
		priorDepth += levels[i].Size
	}
	for _, size := range sizes {
		q := math.Min(math.Max(0, size-priorDepth), levels[levelIndex].Size)
		if q > 1e-9 {
			quantities[q] = true
		}
	}
	qs := make([]float64, 0, len(quantities))
	for q := range quantities {
		qs = append(qs, q)
	}
	sort.Float64s(qs)
	quotes := make([]payoffsolver.FeeQuote, 0, len(qs))
	for _, q := range qs {
		f, ok := fee(q, levels[levelIndex].Price)
		if !ok || f < 0 {
			return nil, false
		}
		quotes = append(quotes, payoffsolver.FeeQuote{Quantity: q, Total: f})
	}
	return quotes, len(quotes) > 0
}

func (s *Server) r138KalshiSolverLeg(ticker, side, payoffID string, payoff []float64,
	sizes []float64) (payoffsolver.Leg, string, bool) {
	decisionStarted := time.Now()
	book, age, ok := s.kal.LiveBook(ticker, 3*time.Second)
	if !ok || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 {
		return payoffsolver.Leg{}, "", false
	}
	m, ok := s.kmkt(ticker)
	if !ok {
		return payoffsolver.Leg{}, "", false
	}
	raw := append([]kalshi.OrderbookLevel(nil), book.YesAsks...)
	if side == "NO" {
		raw = make([]kalshi.OrderbookLevel, 0, len(book.YesBids))
		for _, level := range book.YesBids {
			raw = append(raw, kalshi.OrderbookLevel{Price: 1 - level.Price, Size: level.Size})
		}
	}
	if len(raw) == 0 {
		return payoffsolver.Leg{}, "", false
	}
	tick, known := m.TickForKnown(raw[0].Price)
	if !known || tick <= 0 {
		return payoffsolver.Leg{}, "", false
	}
	levels := make([]payoffsolver.Level, 0, len(raw))
	fullLevels := make([]payoffsolver.Level, 0, len(raw))
	feeSources := []string{}
	feeFn := func(q, px float64) (float64, bool) {
		fee, known, source := s.kalFeeExact(ticker, false, q, px)
		if known {
			feeSources = append(feeSources, source)
		}
		return fee, known
	}
	for i, level := range raw {
		quotes, ok := r138SolverFeeQuotes(sizes, i, raw, feeFn)
		full := payoffsolver.Level{Price: level.Price, Quantity: level.Size}
		if ok {
			full.FeeQuotes = quotes
			levels = append(levels, full)
		}
		fullLevels = append(fullLevels, full)
	}
	if len(levels) == 0 {
		return payoffsolver.Leg{}, "", false
	}
	// Capture the opposite executable book at the same sequenced snapshot. It is not assumed to
	// remain available: it only defines the quote-time unwind envelope for a non-atomic partial fill.
	unwindRaw := append([]kalshi.OrderbookLevel(nil), book.YesBids...)
	if side == "NO" {
		unwindRaw = make([]kalshi.OrderbookLevel, 0, len(book.YesAsks))
		for _, level := range book.YesAsks {
			unwindRaw = append(unwindRaw, kalshi.OrderbookLevel{Price: 1 - level.Price, Size: level.Size})
		}
	}
	unwindLevels := make([]payoffsolver.Level, 0, len(unwindRaw))
	fullUnwindLevels := make([]payoffsolver.Level, 0, len(unwindRaw))
	for i, level := range unwindRaw {
		quotes, quoteOK := r138SolverFeeQuotes(sizes, i, unwindRaw, feeFn)
		full := payoffsolver.Level{Price: level.Price, Quantity: level.Size}
		if quoteOK {
			full.FeeQuotes = quotes
			unwindLevels = append(unwindLevels, full)
		}
		fullUnwindLevels = append(fullUnwindLevels, full)
	}
	provenance, provenanceOK := s.kal.LiveBookProvenance(ticker, 3*time.Second)
	if !provenanceOK {
		return payoffsolver.Leg{}, "", false
	}
	sourceClock, sourceAt, clockOK := properScoreKalshiSourceClock(provenance.Generation,
		provenance.SubscriptionID, provenance.Sequence, provenance.SourceAt, provenance.ReceivedAt)
	if !clockOK {
		return payoffsolver.Leg{}, "", false
	}
	if sourceAge := time.Since(sourceAt).Seconds(); sourceAge >= 0 && sourceAge > age.Seconds() {
		age = time.Duration(sourceAge * float64(time.Second))
	}
	feeSource := r138JoinAuthorities(feeSources...)
	return payoffsolver.Leg{ID: ticker + "|" + side, Venue: "kalshi", Ticker: ticker, Side: side,
			PayoffID: payoffID, Payoff: payoff, Levels: levels, FullBookLevels: fullLevels,
			QuoteAgeSeconds: age.Seconds(), TickSize: tick,
			BookSource: "kalshi_book_ws_full", SourceClockID: sourceClock, FeeSource: feeSource,
			DecisionLatencyMS: time.Since(decisionStarted).Seconds() * 1000,
			UnwindLevels:      unwindLevels, FullUnwindLevels: fullUnwindLevels,
			UnwindBookSource: "kalshi_book_ws_full", UnwindFeeSource: feeSource},
		feeSource, true
}

func r138AllLegSolutions(rows []payoffsolver.Solution, legCount int) []payoffsolver.Solution {
	out := make([]payoffsolver.Solution, 0, len(rows))
	for _, row := range rows {
		if len(row.LegIDs) == legCount {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Size < out[j].Size })
	return out
}

func r138SolutionsCurve(rows []payoffsolver.Solution) []storage.CapacityPoint {
	out := make([]storage.CapacityPoint, 0, len(rows))
	for _, row := range rows {
		out = append(out, storage.CapacityPoint{Size: row.Size, Cost: row.Cost, Fee: row.Fees,
			PayoutFloor: row.PayoutFloor, NetFloor: row.NetFloor})
	}
	return out
}

func r138ConditionalCertificateHash(snap kalshi.EventSnapshot, kind string) string {
	return storage.R138HashJSON(map[string]any{
		"event": snap.EventTicker, "kind": kind, "mutually_exclusive": snap.MutuallyExclusive,
		"collateral_return_type": snap.CollateralReturn, "last_updated_ts": snap.LastUpdatedTS,
	})
}

func r138BundleLevelFee(level payoffsolver.Level, quantity float64) (float64, bool) {
	if len(level.FeeQuotes) == 0 {
		return quantity * level.FeePerShare, true
	}
	for _, quote := range level.FeeQuotes {
		if math.Abs(quote.Quantity-quantity) <= 1e-9 {
			return quote.Total, true
		}
	}
	return 0, false
}

func r138BundleLegAt(leg payoffsolver.Leg, quantity float64) (storage.ResearchRouteBundleLeg, error) {
	remaining, cost, fee, depth := quantity, 0.0, 0.0, 0.0
	convert := func(rows []payoffsolver.Level) []storage.ResearchRouteBundleLevel {
		out := make([]storage.ResearchRouteBundleLevel, 0, len(rows))
		for _, level := range rows {
			quotes := make([]storage.ResearchRouteBundleFeeQuote, 0, len(level.FeeQuotes))
			for _, quote := range level.FeeQuotes {
				quotes = append(quotes, storage.ResearchRouteBundleFeeQuote{Quantity: quote.Quantity, Total: quote.Total})
			}
			out = append(out, storage.ResearchRouteBundleLevel{Price: level.Price, Quantity: level.Quantity, FeeQuotes: quotes})
		}
		return out
	}
	for _, level := range leg.Levels {
		depth += level.Quantity
		take := math.Min(remaining, level.Quantity)
		if take > 1e-12 {
			qfee, ok := r138BundleLevelFee(level, take)
			if !ok {
				return storage.ResearchRouteBundleLeg{}, fmt.Errorf("leg %s lacks exact fee quote for %.9f", leg.ID, take)
			}
			cost, fee, remaining = cost+take*level.Price, fee+qfee, remaining-take
		}
	}
	if remaining > 1e-9 {
		return storage.ResearchRouteBundleLeg{}, fmt.Errorf("leg %s lacks requested depth", leg.ID)
	}
	frozenLevels := leg.FullBookLevels
	if len(frozenLevels) == 0 {
		frozenLevels = leg.Levels
	}
	depth = 0
	for _, level := range frozenLevels {
		depth += level.Quantity
	}
	frozenUnwind := leg.FullUnwindLevels
	if len(frozenUnwind) == 0 {
		frozenUnwind = leg.UnwindLevels
	}
	return storage.ResearchRouteBundleLeg{LegID: leg.ID, Venue: leg.Venue, Ticker: leg.Ticker,
		Side: leg.Side, PayoffID: leg.PayoffID, Quantity: quantity, IntegratedCost: cost, ExactFee: fee,
		VisibleDepth: depth, Tick: leg.TickSize, Age: leg.QuoteAgeSeconds, BookSource: leg.BookSource,
		SourceClockID: leg.SourceClockID, FeeSource: leg.FeeSource, Levels: convert(frozenLevels),
		Payoff: append([]float64(nil), leg.Payoff...), UnwindKnown: len(leg.UnwindLevels) > 0,
		UnwindBookSource: leg.UnwindBookSource, UnwindFeeSource: leg.UnwindFeeSource,
		UnwindLevels: convert(frozenUnwind)}, nil
}

func r138TypedBundle(systemID, cohort, opportunity, certHash, certStatus, blocker string, observed time.Time,
	problem payoffsolver.Problem, row payoffsolver.Solution, inputs map[string]any) (storage.ResearchRouteBundle, error) {
	byID := make(map[string]payoffsolver.Leg, len(problem.Legs))
	for _, leg := range problem.Legs {
		byID[leg.ID] = leg
	}
	legs := make([]storage.ResearchRouteBundleLeg, 0, len(row.LegIDs))
	latency := 0.0
	venues := map[string]bool{}
	for i, id := range row.LegIDs {
		leg, ok := byID[id]
		if !ok {
			return storage.ResearchRouteBundle{}, fmt.Errorf("solver solution references unknown leg %s", id)
		}
		frozen, err := r138BundleLegAt(leg, row.Size)
		if err != nil {
			return storage.ResearchRouteBundle{}, err
		}
		frozen.Index = i
		legs = append(legs, frozen)
		latency = math.Max(latency, leg.DecisionLatencyMS)
		venues[leg.Venue] = true
	}
	if len(legs) < 2 || len(legs) > payoffsolver.MaxSupportedLegs {
		return storage.ResearchRouteBundle{}, fmt.Errorf("solver bundle leg count %d exceeds hard 2..6 contract", len(legs))
	}
	states := make([]storage.ResearchRouteBundleState, len(problem.Certificate.States))
	for i, id := range problem.Certificate.States {
		if i >= len(row.StatePayouts) {
			return storage.ResearchRouteBundle{}, errors.New("solver state payout vector is incomplete")
		}
		states[i] = storage.ResearchRouteBundleState{Index: i, StateID: id, Payout: row.StatePayouts[i]}
	}
	routeKind := "all_leg_taker"
	if len(venues) > 1 {
		routeKind = "cross_venue_non_atomic"
	}
	if row.AtomicRoute {
		routeKind = "kalshi_rfq"
	}
	stateHash := storage.R138HashJSON(map[string]any{"states": problem.Certificate.States,
		"payouts": row.StatePayouts, "legs": row.LegIDs, "size": row.Size})
	bundleID := storage.ResearchRouteStableID(systemID, opportunity, certHash, stateHash,
		observed.UTC().Format(time.RFC3339Nano))
	evidence := map[string]any{"inputs": inputs, "rules_hash": problem.Certificate.RulesHash,
		"relations_hash": problem.Certificate.RelationsHash, "actual_fill_claimed": false,
		"atomic_fill_claimed": false, "paper_authority": false, "live_authority": false}
	return storage.ResearchRouteBundle{BundleID: bundleID, SystemID: systemID, Cohort: cohort, ExperimentVersion: 1,
		OpportunityID: opportunity, CanonicalEventID: problem.Certificate.CanonicalEventID,
		EventVersion: problem.Certificate.EventVersion, CertificateHash: certHash,
		CertificateStatus: certStatus, RouteKind: routeKind,
		Observed: observed, AtomicRoute: row.AtomicRoute, Size: row.Size, Cost: row.Cost, Fee: row.Fees,
		PayoutFloor: row.PayoutFloor, NetFloor: row.NetFloor, PartialFillWorst: row.PartialFillWorstLoss,
		UnwindWorst: row.UnwindWorstLoss, UnwindKnown: row.UnwindKnown, DecisionLatencyMS: latency,
		LatencyKnown: latency >= 0, StateVectorHash: stateHash, Blocker: blocker, Legs: legs, States: states,
		Evidence: evidence}, nil
}

// recordR148NamedRouteBundles gives a concrete multi-leg system its own typed candidate stream
// without duplicating the underlying R139 inference observation. The solver/deadline systems keep
// owning statistical proof; the named product owns execution identity and can borrow only the
// exact sealed proof mapped by CurrentStagedBundleExecutionProof.
func (s *Server) recordR148NamedRouteBundles(ctx context.Context, observed time.Time, systemID,
	cohort, opportunity, certHash, certStatus, blocker string, problem payoffsolver.Problem,
	rows []payoffsolver.Solution, inputs map[string]any) (inserted, duplicates int, err error) {
	for _, row := range rows {
		aliasedInputs := make(map[string]any, len(inputs)+2)
		for key, value := range inputs {
			aliasedInputs[key] = value
		}
		aliasedInputs["named_execution_system"] = systemID
		aliasedInputs["candidate_authority_only"] = true
		bundle, bundleErr := r138TypedBundle(systemID, cohort, opportunity, certHash, certStatus,
			blocker, observed, problem, row, aliasedInputs)
		if bundleErr != nil {
			return inserted, duplicates, bundleErr
		}
		ok, bundleErr := s.store.InsertResearchRouteBundle(ctx, bundle)
		if bundleErr != nil {
			return inserted, duplicates, fmt.Errorf("persist named route bundle %s: %w", bundle.BundleID, bundleErr)
		}
		if ok {
			inserted++
		} else {
			duplicates++
		}
	}
	return inserted, duplicates, nil
}

func (s *Server) recordR138SolverRows(ctx context.Context, observed time.Time, systemID, opportunity,
	eventID, venue, cohort, certStatus, certHash, blocker, sourceArtifact, bookSource, feeSource string,
	problem payoffsolver.Problem, rows []payoffsolver.Solution, tick, quoteAge, capacity float64,
	inputs map[string]any) (inserted, duplicates int, err error) {
	if len(rows) == 0 {
		return 0, 0, nil
	}
	for _, row := range rows {
		bundle, bundleErr := r138TypedBundle(systemID, cohort, opportunity, certHash, certStatus, blocker, observed, problem, row, inputs)
		if bundleErr != nil {
			return inserted, duplicates, bundleErr
		}
		bundleInserted, bundleErr := s.store.InsertResearchRouteBundle(ctx, bundle)
		if bundleErr != nil {
			return inserted, duplicates, fmt.Errorf("persist typed solver bundle %s: %w", bundle.BundleID, bundleErr)
		}
		rowInputs := map[string]any{}
		for key, value := range inputs {
			rowInputs[key] = value
		}
		rowInputs["conditional_positive"] = row.NetFloor > 0
		rowInputs["partial_fill_worst_loss"] = row.PartialFillWorstLoss
		rowInputs["state_payouts"] = row.StatePayouts
		rowInputs["atomic_route"] = row.AtomicRoute
		rowInputs["typed_bundle_id"] = bundle.BundleID
		rowInputs["ordered_leg_ids"] = row.LegIDs
		rowInputs["unwind_known"] = row.UnwindKnown
		rowInputs["unwind_worst_loss"] = row.UnwindWorstLoss
		kind := "negative"
		if row.NetFloor > 0 {
			kind = "control"
		}
		_, ok, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: observed, SystemID: systemID, OpportunityID: opportunity + "|bundle=" + bundle.BundleID, Kind: kind, Cohort: cohort,
			CanonicalEventID: eventID, Venue: venue, Route: "observer", CertificateStatus: certStatus,
			CertificateHash: certHash, SourceClockID: "typed-bundle:" + bundle.BundleID,
			SourceArtifact: sourceArtifact, PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0,
			OutcomeStatus: "open", Blocker: blocker, Candidate: false, Inputs: rowInputs,
		})
		if err != nil {
			return inserted, duplicates, fmt.Errorf("persist solver bundle observer %s: %w", bundle.BundleID, err)
		}
		if bundleInserted && ok {
			inserted++
		} else {
			duplicates++
		}
	}
	return inserted, duplicates, nil
}

func r138SnapshotMembers(snap kalshi.EventSnapshot) []storage.OutcomeSetMember {
	rows := make([]storage.OutcomeSetMember, 0, len(snap.Markets))
	for _, m := range snap.Markets {
		depth := math.Min(m.YesBidSize.Float(), m.YesAskSize.Float())
		rows = append(rows, storage.OutcomeSetMember{Ticker: m.Ticker, Status: strings.ToLower(m.Status),
			Result: strings.ToLower(m.Result), Bid: m.YesBid.Float(), Ask: m.YesAsk.Float(), Depth: math.Max(0, depth)})
	}
	return rows
}

// captureR138EventSnapshot reuses the official response already fetched by event-basket-lock. It
// stores unchanged controls, membership changes, and conditional solver rows without another REST
// request. Unknown void economics keep every conditional lock unfunded and non-candidate.
func (s *Server) captureR138EventSnapshot(ctx context.Context, snap kalshi.EventSnapshot, observed time.Time) {
	if snap.EventTicker == "" {
		return
	}
	if snap.SeriesTicker != "" {
		_, _ = s.store.InsertSeriesEventMap(ctx, snap.SeriesTicker, snap.EventTicker, observed,
			"Kalshi GET /events/{event_ticker}?with_nested_markets=true")
	}
	if len(snap.Markets) < 2 {
		return
	}
	// Reuse this already-fetched official response for the K-PUS rule-artifact collector. This
	// caches exact settlement source identifiers/URLs and performs no additional venue request.
	s.cacheResearchEventRuleSources(snap)
	updated, _ := parseTime(strings.TrimSpace(snap.LastUpdatedTS))
	settlementSources := make([]map[string]string, 0, len(snap.SettlementSources))
	for _, source := range snap.SettlementSources {
		settlementSources = append(settlementSources, map[string]string{"name": source.Name, "url": source.URL})
	}
	marketRules := make(map[string]map[string]string, len(snap.Markets))
	for _, market := range snap.Markets {
		marketRules[market.Ticker] = map[string]string{
			"rules_primary": market.RulesPrimary, "rules_secondary": market.RulesSecondary,
			"early_close_condition": market.EarlyCloseCondition,
			"artifact_hash": storage.R138HashJSON(map[string]string{"primary": market.RulesPrimary,
				"secondary": market.RulesSecondary, "early_close": market.EarlyCloseCondition}),
		}
	}
	blocker := "event exhaustiveness and void payoff are not both authoritatively certified"
	result, err := s.store.InsertOutcomeSetFrame(ctx, storage.OutcomeSetFrame{
		Observed: observed, SourceUpdated: updated, Venue: "kalshi", EventID: snap.EventTicker,
		Title: snap.Title, SourceArtifact: "GET /events/{event_ticker}?with_nested_markets=true",
		Members: r138SnapshotMembers(snap), MutuallyExclusive: snap.MutuallyExclusive,
		Exhaustive: false, VoidVerified: false, Blocker: blocker,
	})
	status := "healthy"
	errorClass, errorText := "", ""
	exclusions := map[string]int{}
	inserted := 0
	if err != nil {
		status, errorClass, errorText = "error", "storage", err.Error()
		exclusions["outcome_frame_storage_error"] = 1
	} else if result.Inserted {
		inserted++
	}
	if status == "healthy" {
		// The outer bounded event-basket sweep owns whole-cycle truth. A per-event receipt is only a
		// durable pending detail until that sweep writes its aggregate terminal. If a later request is
		// canceled, this cannot falsely label the incomplete cycle healthy.
		status, errorClass, errorText = "blocked", "cycle_pending", "event snapshot recorded; bounded source cycle has not terminaled"
	}
	if err == nil {
		kind := "control"
		if result.ChangeClass != "baseline" && result.ChangeClass != "unchanged" {
			kind = "negative"
		}
		_, _, _ = s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: observed, SystemID: "outcome-set-expansion-shock",
			OpportunityID: snap.EventTicker + "|membership=" + result.ChangeClass, Kind: kind,
			Cohort: result.ChangeClass, CanonicalEventID: "venue:kalshi:" + snap.EventTicker,
			Venue: "kalshi", Route: "observer", CertificateStatus: "structural",
			CertificateHash: r138ConditionalCertificateHash(snap, "membership"),
			SourceClockID:   "kalshi-market-lifecycle-ws", SourceArtifact: "official event membership snapshot",
			PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "open",
			Blocker: "no frozen survivor fair-value residual; " + blocker,
			Inputs: map[string]any{"change_class": result.ChangeClass, "members": result.Total,
				"executable_members": result.Executable, "source_updated_ts": snap.LastUpdatedTS,
				"settlement_sources": settlementSources, "market_rule_artifacts": marketRules},
		})
	}
	durableCtx, cancelDurability := researchDurabilityContext(ctx)
	_, _ = s.store.InsertCollectorReceipt(durableCtx, storage.CollectorReceipt{
		CollectorID: "outcome-set-surface", CycleID: collectorCycleID(observed) + "|" + snap.EventTicker,
		ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1, Status: status,
		ErrorClass: errorClass, ErrorText: errorText,
		Started: observed, Completed: time.Now(), Eligible: 1, Attempted: 1, Inserted: inserted,
		Duplicates: map[bool]int{true: 0, false: 1}[result.Inserted], Exclusions: exclusions,
		Source:        "official venue event membership + current side-specific books",
		SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 2 * time.Minute,
		Systems: []string{"outcome-set-expansion-shock", "payoff-constraint-solver"},
	})
	cancelDurability()
	s.solveR138KalshiEvent(ctx, snap, observed)
}

func (s *Server) r138KalshiTakerDepth(ticker, side string) (float64, bool) {
	book, _, ok := s.kal.LiveBook(ticker, 3*time.Second)
	if !ok || book == nil {
		return 0, false
	}
	levels := book.YesAsks
	if side == "NO" {
		levels = book.YesBids
	}
	if len(levels) == 0 {
		return 0, false
	}
	depth := 0.0
	for i, level := range levels {
		if i >= r138MaxPortfolioLegs {
			break
		}
		if level.Size > 0 {
			depth += level.Size
		}
	}
	return depth, depth >= 1
}

func r138LegEnvelope(legs []payoffsolver.Leg) (tick, age, capacity float64) {
	tick, capacity = math.Inf(1), math.Inf(1)
	for _, leg := range legs {
		if leg.TickSize < tick {
			tick = leg.TickSize
		}
		if leg.QuoteAgeSeconds > age {
			age = leg.QuoteAgeSeconds
		}
		depth := 0.0
		for _, level := range leg.Levels {
			depth += level.Quantity
		}
		if depth < capacity {
			capacity = depth
		}
	}
	if math.IsInf(tick, 0) {
		tick = 0
	}
	if math.IsInf(capacity, 0) {
		capacity = 0
	}
	return
}

func (s *Server) solveR138KalshiEvent(ctx context.Context, snap kalshi.EventSnapshot, observed time.Time) {
	started := time.Now()
	receipt := storage.CollectorReceipt{
		CollectorID: "payoff-envelope-capacity", CycleID: collectorCycleID(observed) + "|" + snap.EventTicker,
		ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1, Status: "healthy",
		Started: started, Source: "verified payoff certificate + actual depth levels + exact per-level route fees",
		SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 2 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"payoff-constraint-solver", "deadline-hazard-surface", "incentive-subsidized-structural-lock"},
	}
	defer func() {
		durableCtx, cancelDurability := researchDurabilityContext(ctx)
		defer cancelDurability()
		receipt.Completed = time.Now()
		if receipt.Inserted == 0 && receipt.Status == "healthy" {
			receipt.Status, receipt.ExpectedZero = "healthy_empty", true
			receipt.ZeroReason = "no complete conditional state vector had fresh exact-fee depth"
		}
		if receipt.Status == "healthy" || receipt.Status == "healthy_empty" {
			receipt.Status, receipt.ErrorClass = "blocked", "cycle_pending"
			receipt.ErrorText = "event payoff details recorded; bounded source cycle has not terminaled"
			receipt.ExpectedZero, receipt.ZeroReason = false, ""
		}
		_, _ = s.store.InsertCollectorReceipt(durableCtx, receipt)
	}()
	// Official mutually-exclusive child markets define a complete normal-settlement state list
	// once a NONE state is included. Unknown void economics deliberately widen the stored envelope.
	if snap.MutuallyExclusive && len(snap.Markets) >= 2 && len(snap.Markets) <= r138MaxPortfolioLegs {
		receipt.Eligible++
		capacity := math.Inf(1)
		for _, m := range snap.Markets {
			depth, ok := s.r138KalshiTakerDepth(m.Ticker, "NO")
			if !ok {
				capacity = 0
				break
			}
			capacity = math.Min(capacity, depth)
		}
		sizes := r138SolverSizes(capacity)
		states := []string{"none-listed-yes"}
		for _, m := range snap.Markets {
			states = append(states, "yes:"+m.Ticker)
		}
		legs, feeSources, complete := []payoffsolver.Leg{}, []string{}, len(sizes) > 0
		for i, m := range snap.Markets {
			payoff := make([]float64, len(states))
			for state := range payoff {
				payoff[state] = 1
			}
			payoff[i+1] = 0
			leg, feeSource, ok := s.r138KalshiSolverLeg(m.Ticker, "NO", m.Ticker+"|NO", payoff, sizes)
			if !ok {
				complete = false
				break
			}
			legs, feeSources = append(legs, leg), append(feeSources, feeSource)
		}
		if complete {
			receipt.Attempted++
			certHash := r138ConditionalCertificateHash(snap, "at-most-one-normal-settlement")
			problem := payoffsolver.Problem{Certificate: payoffsolver.Certificate{
				CanonicalEventID: "venue:kalshi:" + snap.EventTicker, EventVersion: 1, States: states,
				RulesHash: certHash, RelationsHash: certHash, Verified: true, Complete: true,
			}, Legs: legs, Sizes: sizes, MaxLegs: len(legs), MaxQuoteAgeSeconds: 3, AtomicRoute: false}
			all, err := payoffsolver.Evaluate(problem)
			if err != nil {
				receipt.Exclusions["solver_rejected_problem"]++
			} else {
				all = r138AllLegSolutions(all, len(legs))
				tick, age, depth := r138LegEnvelope(legs)
				i, d, recordErr := s.recordR138SolverRows(ctx, observed, "payoff-constraint-solver",
					snap.EventTicker+"|buy-no-all", "venue:kalshi:"+snap.EventTicker, "kalshi",
					"mutually-exclusive-normal-settlement", "structural", certHash,
					"void payoff is not certified; multi-leg route is non-atomic; untouched replication pending",
					"official nested event snapshot", "kalshi_book_ws", r138JoinAuthorities(feeSources...),
					problem, all, tick, age, depth, map[string]any{"mutually_exclusive": true, "exhaustive": false,
						"normal_states_include_none": true, "route": "buy-no-all"})
				receipt.Inserted += i
				receipt.Duplicates += d
				if recordErr != nil {
					receipt.Exclusions["typed_bundle_storage_error"]++
					receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "typed_bundle", recordErr.Error()
				}
				namedI, namedD, namedErr := s.recordR148NamedRouteBundles(ctx, observed, "event-basket-lock",
					"mutually-exclusive-normal-settlement", snap.EventTicker+"|buy-no-all", certHash,
					"structural", "void payoff is not certified; staged execution and untouched replication are required",
					problem, all, map[string]any{"venue": "kalshi", "route": "buy-no-all"})
				receipt.Metrics["event_basket_named_bundles_inserted"] = namedI
				receipt.Metrics["event_basket_named_bundles_duplicate"] = namedD
				if namedErr != nil {
					receipt.Exclusions["named_bundle_storage_error"]++
					receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "typed_bundle", namedErr.Error()
				}
				receipt.Metrics["basket_sizes_evaluated"] = len(all)
			}
		} else {
			receipt.Exclusions["basket_fresh_exact_fee_depth_incomplete"]++
		}
	}
	// Greater-than rungs provide an exact normal-state implication. Every adjacent pair is retained,
	// even when its fee-net floor is negative; unknown void economics keeps the outer envelope open.
	rungs := nestedLadderRungs(snap)
	sort.Slice(rungs, func(i, j int) bool { return rungs[i].strike < rungs[j].strike })
	for i := 0; i+1 < len(rungs); i++ {
		lo, hi := rungs[i], rungs[i+1]
		receipt.Eligible++
		lowDepth, lok := s.r138KalshiTakerDepth(lo.ticker, "YES")
		highDepth, hok := s.r138KalshiTakerDepth(hi.ticker, "NO")
		sizes := r138SolverSizes(math.Min(lowDepth, highDepth))
		if !lok || !hok || len(sizes) == 0 {
			receipt.Exclusions["nested_fresh_exact_fee_depth_incomplete"]++
			continue
		}
		states := []string{"x<=low", "low<x<=high", "x>high"}
		lowLeg, lfs, lok := s.r138KalshiSolverLeg(lo.ticker, "YES", lo.ticker+"|YES", []float64{0, 1, 1}, sizes)
		highLeg, hfs, hok := s.r138KalshiSolverLeg(hi.ticker, "NO", hi.ticker+"|NO", []float64{1, 1, 0}, sizes)
		if !lok || !hok {
			receipt.Exclusions["nested_fee_curve_unavailable"]++
			continue
		}
		receipt.Attempted++
		certHash := storage.R138HashJSON(map[string]any{"event": snap.EventTicker, "low": lo.strike,
			"high": hi.strike, "strike_type": "greater", "conditional": "normal-settlement"})
		problem := payoffsolver.Problem{Certificate: payoffsolver.Certificate{
			CanonicalEventID: "venue:kalshi:" + snap.EventTicker, EventVersion: 1, States: states,
			RulesHash: certHash, RelationsHash: certHash, Verified: true, Complete: true,
		}, Legs: []payoffsolver.Leg{lowLeg, highLeg}, Sizes: sizes, MaxLegs: 2,
			MaxQuoteAgeSeconds: 3, AtomicRoute: false}
		all, err := payoffsolver.Evaluate(problem)
		if err != nil {
			receipt.Exclusions["nested_solver_rejected_problem"]++
			continue
		}
		all = r138AllLegSolutions(all, 2)
		tick, age, depth := r138LegEnvelope([]payoffsolver.Leg{lowLeg, highLeg})
		ins, dup, recordErr := s.recordR138SolverRows(ctx, observed, "payoff-constraint-solver",
			fmt.Sprintf("%s|nested|%s|%s", snap.EventTicker, lo.ticker, hi.ticker),
			"venue:kalshi:"+snap.EventTicker, "kalshi", "nested-greater-normal-settlement", "structural",
			certHash, "void payoff is not certified; two-leg route is non-atomic; untouched replication pending",
			"official nested greater-strike event", "kalshi_book_ws", r138JoinAuthorities(lfs, hfs),
			problem, all, tick, age, depth, map[string]any{"low_strike": lo.strike, "high_strike": hi.strike})
		receipt.Inserted += ins
		receipt.Duplicates += dup
		if recordErr != nil {
			receipt.Exclusions["typed_bundle_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "typed_bundle", recordErr.Error()
		}
		namedI, namedD, namedErr := s.recordR148NamedRouteBundles(ctx, observed, "nested-ladder-lock",
			"nested-greater-normal-settlement", fmt.Sprintf("%s|nested|%s|%s", snap.EventTicker, lo.ticker, hi.ticker),
			certHash, "structural", "void payoff is not certified; staged execution and untouched replication are required",
			problem, all, map[string]any{"low_strike": lo.strike, "high_strike": hi.strike})
		receipt.Metrics["nested_named_bundles_inserted"] = systemsMapInt(receipt.Metrics, "nested_named_bundles_inserted") + namedI
		receipt.Metrics["nested_named_bundles_duplicate"] = systemsMapInt(receipt.Metrics, "nested_named_bundles_duplicate") + namedD
		if namedErr != nil {
			receipt.Exclusions["named_bundle_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "typed_bundle", namedErr.Error()
		}
	}
}

func (s *Server) r138PolySolverLeg(ticker, side, payoffID string, payoff []float64, price, depth, tick,
	quoteAge float64, sizes []float64) (payoffsolver.Leg, string, bool) {
	decisionStarted := time.Now()
	if ticker == "" || price <= 0 || price >= 1 || depth < 1 || tick <= 0 || quoteAge < 0 {
		return payoffsolver.Leg{}, "", false
	}
	feeSources := []string{}
	bids, asks, sourceAt, receivedAt, bookOK := s.polyUSWS.FullBookLevelsAt(ticker)
	if !bookOK || len(bids) == 0 || len(asks) == 0 || receivedAt.IsZero() {
		return payoffsolver.Leg{}, "", false
	}
	entryRaw, unwindRaw := asks, bids
	if side == "NO" {
		entryRaw = make([]polymarketus.BookLevel, 0, len(bids))
		for _, level := range bids {
			entryRaw = append(entryRaw, polymarketus.BookLevel{Price: 1 - level.Price, Quantity: level.Quantity})
		}
		unwindRaw = make([]polymarketus.BookLevel, 0, len(asks))
		for _, level := range asks {
			unwindRaw = append(unwindRaw, polymarketus.BookLevel{Price: 1 - level.Price, Quantity: level.Quantity})
		}
	}
	build := func(raw []polymarketus.BookLevel) (solver, full []payoffsolver.Level, ok bool) {
		out := make([]payoffsolver.Level, 0, len(raw))
		all := make([]payoffsolver.Level, 0, len(raw))
		prior := 0.0
		for _, level := range raw {
			quantities := map[float64]bool{}
			for _, size := range sizes {
				q := math.Min(math.Max(0, size-prior), level.Quantity)
				if q > 1e-9 {
					quantities[q] = true
				}
			}
			qs := make([]float64, 0, len(quantities))
			for q := range quantities {
				qs = append(qs, q)
			}
			sort.Float64s(qs)
			feeQuotes := make([]payoffsolver.FeeQuote, 0, len(qs))
			for _, q := range qs {
				fee, source, known := s.polyUSFeeExactAuthority(ticker, false, q, level.Price)
				if !known || fee < 0 {
					return nil, nil, false
				}
				feeQuotes = append(feeQuotes, payoffsolver.FeeQuote{Quantity: q, Total: fee})
				feeSources = append(feeSources, source)
			}
			frozen := payoffsolver.Level{Price: level.Price, Quantity: level.Quantity, FeeQuotes: feeQuotes}
			all = append(all, frozen)
			if len(feeQuotes) > 0 {
				out = append(out, frozen)
			}
			prior += level.Quantity
		}
		return out, all, len(out) > 0
	}
	entryLevels, fullEntryLevels, entryOK := build(entryRaw)
	unwindLevels, fullUnwindLevels, unwindOK := build(unwindRaw)
	if !entryOK {
		return payoffsolver.Leg{}, "", false
	}
	clockAt, clockKind := sourceAt, "source"
	if clockAt.IsZero() {
		clockAt, clockKind = receivedAt, "received-venue-time-omitted"
	}
	if clockAt.IsZero() {
		return payoffsolver.Leg{}, "", false
	}
	feeSource := r138JoinAuthorities(feeSources...)
	leg := payoffsolver.Leg{ID: ticker + "|" + side, Venue: "polyus", Ticker: ticker, Side: side,
		PayoffID: payoffID, Payoff: payoff, QuoteAgeSeconds: quoteAge, TickSize: tick,
		Levels: entryLevels, FullBookLevels: fullEntryLevels, BookSource: "polyus_ws_full_depth",
		SourceClockID: "polyus-market-data:" + clockKind + ":" + clockAt.UTC().Format(time.RFC3339Nano),
		FeeSource:     feeSource, DecisionLatencyMS: time.Since(decisionStarted).Seconds() * 1000}
	if unwindOK {
		leg.UnwindLevels, leg.FullUnwindLevels = unwindLevels, fullUnwindLevels
		leg.UnwindBookSource, leg.UnwindFeeSource = "polyus_ws_full_depth", feeSource
	}
	return leg, feeSource, true
}

// captureR138PolyUSOutcomeSet reuses the complete winner set already accepted by
// completePolyUSDutchSets. It adds no request and never treats a broad EventID as a payoff set.
func (s *Server) captureR138PolyUSOutcomeSet(ctx context.Context, set polyUSDutchSet, observed time.Time) (inserted, duplicates, recordErrors int) {
	if len(set.legs) < 2 || set.eventID == "" || set.scope == "" {
		return 0, 0, 0
	}
	members := make([]storage.OutcomeSetMember, 0, len(set.legs))
	for _, leg := range set.legs {
		members = append(members, storage.OutcomeSetMember{Ticker: leg.ticker, Status: "open",
			Bid: leg.bid, Ask: leg.ask, Depth: math.Min(leg.bidSz, leg.askSz)})
	}
	eventID := set.eventID + "|scope=" + set.scope
	frame, err := s.store.InsertOutcomeSetFrame(ctx, storage.OutcomeSetFrame{
		Observed: observed, Venue: "polyus", EventID: eventID, Title: set.name,
		SourceArtifact: "authenticated product event + current exact sportsMarketType + explicit league cardinality",
		Members:        members, MutuallyExclusive: true, Exhaustive: true, VoidVerified: false,
		Blocker: "normal winner cardinality is complete but void/cancel payout treatment is not attached to the certificate",
	})
	if err == nil {
		_, _, _ = s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: observed, SystemID: "outcome-set-expansion-shock", OpportunityID: eventID + "|membership=" + frame.ChangeClass,
			Kind: "control", Cohort: frame.ChangeClass, CanonicalEventID: "venue:polyus:" + eventID,
			Venue: "polyus", Route: "observer", CertificateStatus: "structural",
			CertificateHash: storage.R138HashJSON(map[string]any{"event": eventID, "members": members}),
			SourceClockID:   "polyus-market-ws-full", SourceArtifact: "complete authenticated winner set",
			PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "open",
			Blocker: "no membership change fair-value model and no void payoff certificate",
			Inputs: map[string]any{"change_class": frame.ChangeClass, "members": frame.Total,
				"mutually_exclusive": true, "exhaustive_normal_settlement": true},
		})
	}
	capacity := math.Inf(1)
	for _, leg := range set.legs {
		capacity = math.Min(capacity, math.Min(leg.bidSz, leg.askSz))
	}
	sizes := r138SolverSizes(capacity)
	if len(sizes) == 0 {
		return 0, 0, 0
	}
	quoteAge := 0.0
	s.polyUSMu.Lock()
	if !s.polyUSAt.IsZero() {
		quoteAge = time.Since(s.polyUSAt).Seconds()
	}
	s.polyUSMu.Unlock()
	if quoteAge < 0 || quoteAge > 45 {
		return 0, 0, 0
	}
	if len(set.legs) > r138MaxPortfolioLegs {
		return 0, 0, 0
	}
	states := make([]string, len(set.legs))
	for i, leg := range set.legs {
		states[i] = "yes:" + leg.ticker
	}
	for _, route := range []string{"buy-yes-all", "buy-no-all"} {
		legs := make([]payoffsolver.Leg, 0, len(set.legs))
		feeSources, valid := []string{}, true
		for i, raw := range set.legs {
			payoff := make([]float64, len(states))
			side, price, depth := "YES", raw.ask, raw.askSz
			if route == "buy-yes-all" {
				payoff[i] = 1
			} else {
				side, price, depth = "NO", 1-raw.bid, raw.bidSz
				for j := range payoff {
					if j != i {
						payoff[j] = 1
					}
				}
			}
			_, tick, tickKnown := s.polyUSFreshFeeAuthority(raw.ticker)
			leg, feeSource, ok := s.r138PolySolverLeg(raw.ticker, side, raw.ticker+"|"+side,
				payoff, price, depth, tick, quoteAge, sizes)
			if !tickKnown || !ok {
				valid = false
				break
			}
			legs, feeSources = append(legs, leg), append(feeSources, feeSource)
		}
		if !valid {
			continue
		}
		certHash := storage.R138HashJSON(map[string]any{"event": eventID, "route": route,
			"members": members, "conditional": "normal-settlement"})
		problem := payoffsolver.Problem{Certificate: payoffsolver.Certificate{
			CanonicalEventID: "venue:polyus:" + eventID, EventVersion: 1, States: states,
			RulesHash: certHash, RelationsHash: certHash, Verified: true, Complete: true,
		}, Legs: legs, Sizes: sizes, MaxLegs: len(legs), MaxQuoteAgeSeconds: 45, AtomicRoute: false}
		all, err := payoffsolver.Evaluate(problem)
		if err != nil {
			continue
		}
		all = r138AllLegSolutions(all, len(legs))
		tick, age, depth := r138LegEnvelope(legs)
		ins, dup, recordErr := s.recordR138SolverRows(ctx, observed, "payoff-constraint-solver", eventID+"|"+route,
			"venue:polyus:"+eventID, "polyus", "complete-winner-set-normal-settlement", "structural",
			certHash, "void payoff is not certified; route is non-atomic; partial-fill loss is not protectable",
			"authenticated product event + exact sportsMarketType/cardinality", "polyus_market_ws",
			r138JoinAuthorities(feeSources...), problem, all, tick, age, depth,
			map[string]any{"route": route, "mutually_exclusive": true, "exhaustive_normal_settlement": true})
		inserted, duplicates = inserted+ins, duplicates+dup
		if recordErr != nil {
			recordErrors++
		}
		namedI, namedD, namedErr := s.recordR148NamedRouteBundles(ctx, observed, "event-basket-lock",
			"complete-winner-set-normal-settlement", eventID+"|"+route, certHash, "structural",
			"void payoff is not certified; staged execution and untouched replication are required", problem, all,
			map[string]any{"venue": "polyus", "route": route, "mutually_exclusive": true, "exhaustive_normal_settlement": true})
		inserted, duplicates = inserted+namedI, duplicates+namedD
		if namedErr != nil {
			recordErrors++
		}
	}
	return inserted, duplicates, recordErrors
}

func r138DeadlineVoidVerified(row storage.VerifiedDeadlineRelation) bool {
	if strings.TrimSpace(row.VoidPolicy) == "" || strings.Contains(strings.ToLower(row.VoidPolicy), "unknown") ||
		strings.Contains(strings.ToLower(row.VoidPolicy), "unverified") {
		return false
	}
	for _, raw := range []string{row.EvidenceJSON, row.EventEvidenceJSON} {
		var evidence map[string]any
		if json.Unmarshal([]byte(raw), &evidence) == nil {
			if yes, ok := evidence["void_payoff_verified"].(bool); ok && yes {
				return true
			}
		}
	}
	return false
}

func (s *Server) scanR138CertifiedDeadlines(ctx context.Context) {
	started := time.Now()
	receipt := storage.CollectorReceipt{
		CollectorID: "deadline-hazard-certified", CycleID: collectorCycleID(started),
		ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1, Status: "healthy",
		Started: started, Source: "verified canonical implication relations + simultaneous current books",
		SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 5 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"deadline-hazard-surface", "payoff-constraint-solver"},
	}
	defer func() {
		receipt.Completed = time.Now()
		if receipt.Eligible == 0 && receipt.Status == "healthy" {
			receipt.Status, receipt.ExpectedZero = "healthy_empty", true
			receipt.ZeroReason = "immutable graph contains no fully verified implication pair"
		}
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
	}()
	rows, err := s.store.VerifiedDeadlineRelations(ctx, "kalshi", 100)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		return
	}
	receipt.Eligible = len(rows)
	for _, row := range rows {
		leftDeadline, lok := storage.R138DeadlineFromPredicate(row.LeftPredicate)
		rightDeadline, rok := storage.R138DeadlineFromPredicate(row.RightPredicate)
		if !lok || !rok || !leftDeadline.Before(rightDeadline) {
			receipt.Exclusions["deadline_axis_missing_or_inconsistent"]++
			continue
		}
		leftDepth, ldok := s.r138KalshiTakerDepth(row.LeftTicker, "NO")
		rightDepth, rdok := s.r138KalshiTakerDepth(row.RightTicker, "YES")
		sizes := r138SolverSizes(math.Min(leftDepth, rightDepth))
		if !ldok || !rdok || len(sizes) == 0 {
			receipt.Exclusions["fresh_exact_fee_depth_incomplete"]++
			continue
		}
		states := []string{"neither-by-later", "later-only", "by-earlier"}
		left, lfs, lok := s.r138KalshiSolverLeg(row.LeftTicker, "NO", row.LeftPayoffID+"|NOT", []float64{1, 1, 0}, sizes)
		right, rfs, rok := s.r138KalshiSolverLeg(row.RightTicker, "YES", row.RightPayoffID, []float64{0, 1, 1}, sizes)
		if !lok || !rok {
			receipt.Exclusions["exact_fee_curve_unavailable"]++
			continue
		}
		receipt.Attempted++
		certHash := storage.R138HashJSON(map[string]any{"relation": row.RelationID,
			"relation_version": row.RelationVersion, "event_version": row.EventVersion,
			"evidence": row.EvidenceJSON, "void_policy": row.VoidPolicy})
		problem := payoffsolver.Problem{Certificate: payoffsolver.Certificate{
			CanonicalEventID: row.EventID, EventVersion: row.EventVersion, States: states,
			RulesHash: certHash, RelationsHash: certHash, Verified: true, Complete: true,
		}, Legs: []payoffsolver.Leg{left, right}, Sizes: sizes, MaxLegs: 2,
			MaxQuoteAgeSeconds: 3, AtomicRoute: false}
		all, err := payoffsolver.Evaluate(problem)
		if err != nil {
			receipt.Exclusions["solver_rejected_problem"]++
			continue
		}
		all = r138AllLegSolutions(all, 2)
		status, blocker := "structural", "void payoff is not certified; two-leg route is non-atomic"
		if r138DeadlineVoidVerified(row) {
			status, blocker = "verified", "two-leg route is non-atomic; partial-fill loss is not protectable"
		}
		tick, age, depth := r138LegEnvelope([]payoffsolver.Leg{left, right})
		ins, dup, recordErr := s.recordR138SolverRows(ctx, started, "deadline-hazard-surface",
			row.RelationID+"|no-earlier+yes-later", row.EventID, "kalshi", "certified-time-implication",
			status, certHash, blocker, "immutable verified implication relation", "kalshi_book_ws",
			r138JoinAuthorities(lfs, rfs), problem, all, tick, age, depth, map[string]any{
				"left_deadline":                  leftDeadline.UTC().Format(time.RFC3339Nano),
				"right_deadline":                 rightDeadline.UTC().Format(time.RFC3339Nano),
				"normal_state_relation_verified": true,
			})
		receipt.Inserted += ins
		receipt.Duplicates += dup
		if recordErr != nil {
			receipt.Exclusions["typed_bundle_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "typed_bundle", recordErr.Error()
		}
		namedI, namedD, namedErr := s.recordR148NamedRouteBundles(ctx, started, "time-nested-lock",
			"certified-time-implication", row.RelationID+"|no-earlier+yes-later", certHash, status,
			blocker+"; staged execution and untouched replication are required", problem, all, map[string]any{
				"left_deadline": leftDeadline.UTC().Format(time.RFC3339Nano), "right_deadline": rightDeadline.UTC().Format(time.RFC3339Nano),
			})
		receipt.Metrics["time_nested_named_bundles_inserted"] = systemsMapInt(receipt.Metrics, "time_nested_named_bundles_inserted") + namedI
		receipt.Metrics["time_nested_named_bundles_duplicate"] = systemsMapInt(receipt.Metrics, "time_nested_named_bundles_duplicate") + namedD
		if namedErr != nil {
			receipt.Exclusions["named_bundle_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "typed_bundle", namedErr.Error()
		}
	}
}

type r138LinkedBook struct {
	Venue, Ticker, Kind, YesTeam, NoTeam, BookSource, FeeSource string
	Line, Bid, Ask, BidDepth, AskDepth, QuoteAge, Tick          float64
	YesTakerFee, NoTakerFee, YesMakerFee, NoMakerFee            float64
}

func (s *Server) r138KalshiLinkedBook(row storage.MarketGameRow) (r138LinkedBook, bool) {
	book, age, ok := s.kal.LiveBook(row.Ticker, 3*time.Second)
	if !ok || book == nil || len(book.YesBids) == 0 || len(book.YesAsks) == 0 {
		return r138LinkedBook{}, false
	}
	m, ok := s.kmkt(row.Ticker)
	if !ok {
		return r138LinkedBook{}, false
	}
	bid, ask := book.YesBids[0], book.YesAsks[0]
	tick, known := m.TickForKnown(ask.Price)
	if !known || bid.Price <= 0 || ask.Price <= bid.Price || ask.Price >= 1 || bid.Size <= 0 || ask.Size <= 0 {
		return r138LinkedBook{}, false
	}
	ytf, yok, yts := s.kalFeeExact(row.Ticker, false, 1, ask.Price)
	ntf, nok, nts := s.kalFeeExact(row.Ticker, false, 1, 1-bid.Price)
	ymf, ymok, yms := s.kalFeeExact(row.Ticker, true, 1, bid.Price)
	nmf, nmok, nms := s.kalFeeExact(row.Ticker, true, 1, 1-ask.Price)
	if !yok || !nok || !ymok || !nmok {
		return r138LinkedBook{}, false
	}
	return r138LinkedBook{Venue: "kalshi", Ticker: row.Ticker, Kind: row.MktType,
		YesTeam: row.YesTeam, NoTeam: row.NoTeam, Line: row.Line, Bid: bid.Price, Ask: ask.Price,
		BidDepth: bid.Size, AskDepth: ask.Size, QuoteAge: age.Seconds(), Tick: tick,
		BookSource: "kalshi_book_ws", FeeSource: r138JoinAuthorities(yts, nts, yms, nms),
		YesTakerFee: ytf, NoTakerFee: ntf, YesMakerFee: ymf, NoMakerFee: nmf}, true
}

func (s *Server) r138PolyUSLinkedBooks() (map[string]r138LinkedBook, bool) {
	s.polyUSMu.Lock()
	rows := append([]polyUSMarket(nil), s.polyUSMkts...)
	at := s.polyUSAt
	s.polyUSMu.Unlock()
	if at.IsZero() || time.Since(at) < 0 || time.Since(at) > 45*time.Second {
		return nil, false
	}
	out := make(map[string]r138LinkedBook, len(rows))
	for _, m := range rows {
		if m.Slug == "" || m.Bid <= 0 || m.Ask <= m.Bid || m.Ask >= 1 || m.BidSz <= 0 || m.AskSz <= 0 {
			continue
		}
		ytf, yts, yok := s.polyUSFeeExactAuthority(m.Slug, false, 1, m.Ask)
		ntf, nts, nok := s.polyUSFeeExactAuthority(m.Slug, false, 1, 1-m.Bid)
		ymf, yms, ymok := s.polyUSFeeExactAuthority(m.Slug, true, 1, m.Bid)
		nmf, nms, nmok := s.polyUSFeeExactAuthority(m.Slug, true, 1, 1-m.Ask)
		theta, tick, authority := s.polyUSFreshFeeAuthority(m.Slug)
		_ = theta
		if !yok || !nok || !ymok || !nmok || !authority || tick <= 0 {
			continue
		}
		out[m.Slug] = r138LinkedBook{Venue: "polyus", Ticker: m.Slug, Kind: m.Kind,
			YesTeam: m.Team, NoTeam: m.ShortTeam, Line: m.Line, Bid: m.Bid, Ask: m.Ask,
			BidDepth: m.BidSz, AskDepth: m.AskSz, QuoteAge: time.Since(at).Seconds(), Tick: tick,
			BookSource: "polyus_market_ws", FeeSource: r138JoinAuthorities(yts, nts, yms, nms),
			YesTakerFee: ytf, NoTakerFee: ntf, YesMakerFee: ymf, NoMakerFee: nmf}
	}
	return out, true
}

func r138SelectGameGroups(groups map[string][]storage.MarketGameRow, slot string, limit int) []string {
	type ranked struct{ id, rank string }
	var rows []ranked
	for gameID, markets := range groups {
		kinds := map[string]bool{}
		for _, market := range markets {
			kinds[strings.ToLower(market.MktType)] = true
		}
		if len(kinds) < 2 {
			continue
		}
		h := sha256.Sum256([]byte(slot + "|" + gameID))
		rows = append(rows, ranked{id: gameID, rank: hex.EncodeToString(h[:8])})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].rank < rows[j].rank })
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.id)
	}
	return out
}

func (s *Server) scanR138ScoreState(ctx context.Context) {
	started := time.Now()
	receipt := storage.CollectorReceipt{
		CollectorID: "score-state-surface", CycleID: collectorCycleID(started),
		ExperimentID: "score-state-surface", ExperimentVersion: 1, Status: "healthy",
		Started: started, Source: "structural market-to-game joins + current executable books",
		SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 5 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{}, Systems: []string{"score-state-surface"},
	}
	defer func() {
		receipt.Completed = time.Now()
		if receipt.Eligible == 0 && receipt.Status == "healthy" {
			receipt.Status, receipt.ExpectedZero = "healthy_empty", true
			receipt.ZeroReason = "no structurally joined game currently has two distinct payoff families"
		}
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		_, _ = s.store.InsertCollectorReceipt(dctx, receipt)
	}()
	groups := map[string][]storage.MarketGameRow{}
	for _, venue := range []string{"kalshi", "polyus"} {
		rows, err := s.store.MarketGameRows(ctx, venue)
		if err != nil {
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
			return
		}
		for _, row := range rows {
			if row.Src == "struct" && row.GameID != "" {
				groups[row.GameID] = append(groups[row.GameID], row)
			}
		}
	}
	selected := r138SelectGameGroups(groups, researchFiveMinuteCycle(started), 50)
	receipt.Eligible = len(selected)
	polyBooks, polyFresh := s.r138PolyUSLinkedBooks()
	for _, gameID := range selected {
		var books []r138LinkedBook
		missing := 0
		for _, row := range groups[gameID] {
			switch row.Venue {
			case "kalshi":
				if book, ok := s.r138KalshiLinkedBook(row); ok {
					books = append(books, book)
				} else {
					missing++
				}
			case "polyus":
				if book, ok := polyBooks[row.Ticker]; ok && polyFresh {
					books = append(books, book)
				} else {
					missing++
				}
			}
		}
		receipt.Attempted++
		if len(books) < 2 {
			receipt.Exclusions["fewer_than_two_fresh_exact_fee_books"]++
			continue
		}
		kinds := map[string]bool{}
		for _, book := range books {
			kinds[book.Kind] = true
		}
		if len(kinds) < 2 {
			receipt.Exclusions["fresh_books_cover_only_one_payoff_family"]++
			continue
		}
		certificate := storage.R138HashJSON(map[string]any{"game_id": gameID, "books": books})
		_, inserted, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: started, SystemID: "score-state-surface", OpportunityID: gameID,
			Kind: "control", Cohort: "linked-current-books", CanonicalEventID: "sports:" + gameID,
			Venue: "multi", Route: "observer", CertificateStatus: "structural", CertificateHash: certificate,
			SourceClockID:  "kalshi-orderbook-ws+polyus-market-ws-full",
			SourceArtifact: "market_game structural joins + current venue books",
			PayoutLower:    0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "open",
			Blocker: "latent score-state distribution and joint correlation model are not frozen or independently replicated",
			Inputs: map[string]any{"books": books, "missing_joined_books": missing,
				"book_pricing_only": true, "midpoint_used": false, "maker_taker_fees_separate": true},
		})
		if err != nil {
			receipt.Exclusions["observation_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		} else if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
	}
	receipt.Metrics["selected_game_cap"] = 50
	receipt.Metrics["polyus_books_fresh"] = polyFresh
}

func r138RotateIncentivePrograms(programs []kalshi.IncentiveProgram, start, limit int) ([]kalshi.IncentiveProgram, int) {
	rows := append([]kalshi.IncentiveProgram(nil), programs...)
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i].MarketTicker+"|"+rows[i].ID, rows[j].MarketTicker+"|"+rows[j].ID
		return left < right
	})
	if len(rows) == 0 || limit <= 0 {
		return nil, 0
	}
	start %= len(rows)
	if start < 0 {
		start += len(rows)
	}
	n := min(limit, len(rows))
	out := make([]kalshi.IncentiveProgram, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, rows[(start+i)%len(rows)])
	}
	return out, (start + n) % len(rows)
}

type r138PolyUSIncentivePeriod struct {
	slug string
	row  polymarketus.IncentivePeriod
}

func cloneR144PolyUSIncentiveMarkets(rows []polymarketus.IncentiveMarket) []polymarketus.IncentiveMarket {
	out := make([]polymarketus.IncentiveMarket, len(rows))
	for i, row := range rows {
		out[i] = row
		out[i].TimePeriods = append([]polymarketus.IncentivePeriod(nil), row.TimePeriods...)
	}
	return out
}

// r144PolyUSIncentiveSnapshot never turns a failed refresh green. It only lets the research
// economics join continue from a recent complete executable-universe crawl while the receipt keeps
// the current source error. Expired last-good data is refused.
func (s *Server) r144PolyUSIncentiveSnapshot(rows []polymarketus.IncentiveMarket, observed time.Time,
	refreshOK bool) (snapshot []polymarketus.IncentiveMarket, lastGoodAt time.Time, fallback bool) {
	st := s.r138CollectorState()
	st.mu.Lock()
	defer st.mu.Unlock()
	if refreshOK {
		st.polyUSIncentiveLastGood = cloneR144PolyUSIncentiveMarkets(rows)
		st.polyUSIncentiveLastGoodAt = observed
		return cloneR144PolyUSIncentiveMarkets(rows), observed, false
	}
	age := observed.Sub(st.polyUSIncentiveLastGoodAt)
	if st.polyUSIncentiveLastGoodAt.IsZero() || age < 0 || age > r144PolyUSIncentiveLastGoodAge {
		return nil, st.polyUSIncentiveLastGoodAt, false
	}
	return cloneR144PolyUSIncentiveMarkets(st.polyUSIncentiveLastGood), st.polyUSIncentiveLastGoodAt, true
}

func r143MissingCanonicalInstrument(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no current canonical instrument/event/payoff version")
}

func r138RotatePolyUSIncentives(markets []polymarketus.IncentiveMarket, now time.Time, limit int) ([]r138PolyUSIncentivePeriod, int) {
	var all []r138PolyUSIncentivePeriod
	for _, market := range markets {
		for _, period := range market.TimePeriods {
			if period.ActiveAt(now) {
				all = append(all, r138PolyUSIncentivePeriod{slug: market.MarketSlug, row: period})
			}
		}
	}
	sort.Slice(all, func(i, j int) bool {
		left := all[i].slug + "|" + all[i].row.ProgramID + "|" + all[i].row.Start
		right := all[j].slug + "|" + all[j].row.ProgramID + "|" + all[j].row.Start
		return left < right
	})
	if len(all) <= limit || limit <= 0 {
		return all, len(all)
	}
	cycle := int(now.UTC().Unix() / int64((15 * time.Minute).Seconds()))
	start := (cycle * limit) % len(all)
	out := make([]r138PolyUSIncentivePeriod, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, all[(start+i)%len(all)])
	}
	return out, len(all)
}

func (s *Server) sweepR138PolyUSIncentiveEconomics(ctx context.Context, started time.Time) (receipt storage.CollectorReceipt) {
	receipt = storage.CollectorReceipt{CollectorID: "polyus-incentive-economics",
		CycleID: collectorCycleID(started), ExperimentID: "incentive-subsidized-structural-lock",
		ExperimentVersion: 1, Status: "healthy", Started: started,
		Source:        "official public PolyUS GET /v1/incentives + current WS books + exact maker fees; credited earnings read separately",
		SchemaVersion: "polyus-incentives-v0.0.69", ExpectedCadence: 15 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"incentive-subsidized-structural-lock", "maker-salvage-matched-cohort"}}
	defer func() { receipt.Completed = time.Now() }()
	if s.polyUS == nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "blocked", "client", "PolyUS public client unavailable"
		return receipt
	}
	books, booksFresh := s.r138PolyUSLinkedBooks()
	receipt.Metrics["books_fresh_before_crawl"] = booksFresh
	receipt.Metrics["executable_book_universe"] = len(books)
	if !booksFresh || len(books) == 0 {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "blocked", "source_not_ready", "fresh complete PolyUS exact-fee book universe unavailable"
		return receipt
	}
	symbols := make([]string, 0, len(books))
	for slug := range books {
		symbols = append(symbols, slug)
	}
	sort.Strings(symbols)
	crawlCtx, crawlCancel := context.WithTimeout(ctx, r144PolyUSIncentiveBudget)
	markets, crawlStats, crawlErr := s.polyUS.ActiveIncentiveProgramsForSymbols(crawlCtx, symbols)
	crawlCancel()
	markets, lastGoodAt, fallback := s.r144PolyUSIncentiveSnapshot(markets, started, crawlErr == nil && crawlStats.Complete)
	receipt.Metrics["crawl"] = crawlStats
	receipt.Metrics["crawl_scope"] = "complete fresh exact-fee PolyUS book universe via official symbols filter"
	receipt.Metrics["last_good_used"] = fallback
	if !lastGoodAt.IsZero() {
		receipt.Metrics["last_good_completed_at"] = lastGoodAt
		receipt.Metrics["last_good_age_seconds"] = math.Max(0, started.Sub(lastGoodAt).Seconds())
	}
	if crawlErr != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", collectorErrorClass(crawlErr), crawlErr.Error()
		receipt.Exclusions["official_symbol_crawl_failed"]++
		if fallback {
			receipt.Exclusions["recent_complete_last_good_used"]++
		} else {
			return receipt
		}
	}
	// The scoped API crawl can take several seconds. Re-read the executable books so the economic
	// controls never inherit the pre-crawl quote age, depth, or fee receipt.
	currentBooks, currentBooksFresh := s.r138PolyUSLinkedBooks()
	receipt.Metrics["books_fresh_after_crawl"] = currentBooksFresh
	receipt.Metrics["revalidated_executable_book_universe"] = len(currentBooks)
	if !currentBooksFresh || len(currentBooks) == 0 {
		if crawlErr == nil {
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "blocked", "source_not_ready", "PolyUS exact-fee books became stale during incentive crawl"
		}
		return receipt
	}
	books = currentBooks
	// A complete last-good snapshot can contain a book that has since left the tracked executable
	// universe. Filter before rotating so stale symbols cannot consume the 20-program work budget.
	filtered := markets[:0]
	for _, market := range markets {
		if _, ok := books[strings.ToLower(strings.TrimSpace(market.MarketSlug))]; ok {
			filtered = append(filtered, market)
		}
	}
	markets = filtered
	selected, universe := r138RotatePolyUSIncentives(markets, started, r138IncentiveProgramCap)
	receipt.Eligible = universe
	receipt.Metrics["program_market_groups"] = len(markets)
	receipt.Metrics["active_periods"] = universe
	receipt.Metrics["period_cap"] = r138IncentiveProgramCap
	if deferred := universe - len(selected); deferred > 0 {
		receipt.Exclusions["rotation_budget_deferred"] = deferred
	}
	if universe == 0 {
		if crawlErr != nil {
			return receipt
		}
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "complete official symbol-scoped crawl returned no active periods in the fresh executable book universe"
		return receipt
	}
	creditedRows, creditedTotal := 0, 0.0
	var incentiveEarnings []polymarketus.IncentiveEarning
	if s.polyUSAuth != nil {
		earnings, earningsErr := s.polyUSAuth.IncentiveEarnings(ctx, started.AddDate(0, 0, -30), started)
		if earningsErr != nil {
			receipt.Exclusions["credited_earnings_read_failed"]++
			receipt.Metrics["credited_earnings_error"] = earningsErr.Error()
		} else {
			incentiveEarnings = earnings
			creditedRows = len(earnings)
			for _, earning := range earnings {
				creditedTotal += earning.Reward
			}
		}
	} else {
		receipt.Exclusions["authenticated_earnings_client_unavailable"]++
	}
	receipt.Metrics["credited_earnings_rows_30d"] = creditedRows
	receipt.Metrics["credited_earnings_total_30d"] = creditedTotal
	receipt.Metrics["advertised_reward_in_net_lower_bound"] = false
	for _, program := range selected {
		book, ok := books[program.slug]
		if !booksFresh || !ok {
			receipt.Exclusions["fresh_exact_fee_book_unavailable"]++
			marketCredit, marketCreditRows := 0.0, 0
			for _, earning := range incentiveEarnings {
				if strings.EqualFold(earning.MarketSlug, program.slug) {
					marketCredit += earning.Reward
					marketCreditRows++
				}
			}
			if inserted, blocker, evalErr := s.insertStep7IncentiveEvaluation(ctx, started, "polyus", program.row.ProgramID,
				program.slug, 0, false, program.row, marketCredit, marketCreditRows); evalErr != nil {
				receipt.Exclusions["certified_bundle_evaluation_error"]++
			} else if inserted {
				receipt.Inserted++
				if blocker != "" {
					receipt.Exclusions[blocker]++
				}
			}
			continue
		}
		if inserted, blocker, evalErr := s.recordStep7PolyUSIncentive(ctx, started, program, book, incentiveEarnings); evalErr != nil {
			receipt.Exclusions["certified_bundle_evaluation_error"]++
		} else if inserted {
			receipt.Inserted++
			if blocker != "" {
				receipt.Exclusions[blocker]++
			}
		} else {
			receipt.Duplicates++
		}
		for _, side := range []string{"YES", "NO"} {
			receipt.Attempted++
			price, fee := book.Bid, book.YesMakerFee
			if side == "NO" {
				price, fee = 1-book.Ask, book.NoMakerFee
			}
			if price <= 0 || price >= 1 || book.Tick <= 0 {
				receipt.Exclusions["invalid_maker_quote"]++
				continue
			}
			_, inserted, insertErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
				Observed: started, SystemID: "incentive-subsidized-structural-lock",
				OpportunityID: "polyus|" + program.row.ProgramID + "|" + program.slug + "|" + side,
				// Leave canonical identity blank here. InsertResearchSystemObservation resolves the
				// newest immutable instrument/event/payoff version atomically. The old venue-local
				// guess conflicted as soon as a stronger structural identity superseded it.
				Kind: "control", Cohort: "polyus-active-program-uncredited-reward",
				Venue: "polyus", Ticker: program.slug, Route: "maker-control", Side: side,
				CertificateStatus: "unverified", SourceClockID: "polyus-market-ws-full",
				SourceArtifact: "GET /v1/incentives?statuses=active", BookSource: book.BookSource,
				FeeSource: book.FeeSource, QuoteAgeMax: book.QuoteAge, TickMin: book.Tick,
				Size: 1, Cost: price, Fee: fee, PayoutLower: 0, PayoutUpper: 1,
				NetLower: -price - fee, NetUpper: 1 - price - fee, VisibleCapacity: 0,
				CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: price, Fee: fee, PayoutFloor: 0, NetFloor: -price - fee}},
				OutcomeStatus: "open", QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
				Blocker: "natural queue fill, adverse selection, allocation and credited reward are not yet observed; advertised reward lower bound is zero",
				Inputs: map[string]any{"program_type": program.row.ProgramType, "start": program.row.Start,
					"end": program.row.End, "reward_pool": program.row.RewardPool,
					"discount_factor": program.row.DiscountFactor, "target_size": program.row.TargetSize,
					"period": program.row.Period, "status": program.row.Status,
					"credited_reward_lower_bound": 0, "natural_order_submitted": false},
			})
			if insertErr != nil {
				// An advertised program can lead the slower immutable identity crawler. The
				// program/book receipt remains useful, but cannot enter economic grading until
				// that exact instrument version lands. This is an explicit join-lag exclusion,
				// not a storage failure that should turn the entire collector red.
				if r143MissingCanonicalInstrument(insertErr) {
					receipt.Exclusions["canonical_instrument_version_pending"]++
					continue
				}
				receipt.Exclusions["observation_storage_error"]++
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", insertErr.Error()
			} else if inserted {
				receipt.Inserted++
			} else {
				receipt.Duplicates++
			}
		}
	}
	return receipt
}

func (s *Server) sweepR138IncentiveEconomics(ctx context.Context) {
	started := time.Now()
	polyUSReceipt := s.sweepR138PolyUSIncentiveEconomics(ctx, started)
	receipt := storage.CollectorReceipt{
		CollectorID: "incentive-economics", CycleID: collectorCycleID(started),
		ExperimentID: "incentive-subsidized-structural-lock", ExperimentVersion: 1, Status: "healthy",
		Started: started, Source: "official Kalshi incentive programs + current book + exact maker fee; uncredited reward lower bound is zero",
		SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 15 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"incentive-subsidized-structural-lock", "maker-salvage-matched-cohort"},
	}
	defer func() {
		durableCtx, cancelDurability := researchDurabilityContext(ctx)
		defer cancelDurability()
		receipt.Completed = time.Now()
		_, _ = s.store.InsertCollectorReceipt(durableCtx, receipt)
		legacy := receipt
		legacy.CollectorID = "incentive-maker"
		legacy.Source = "Kalshi official incentive programs + current books"
		legacy.SchemaVersion = "kalshi-incentives-v1"
		legacy.Systems = []string{"incentive-subsidized-structural-lock"}
		_, _ = s.store.InsertCollectorReceipt(durableCtx, legacy)
		_, _ = s.store.InsertCollectorReceipt(durableCtx, polyUSReceipt)
	}()
	if s.kal == nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "blocked", "client", "Kalshi client unavailable"
		return
	}
	programs, err := s.kal.ActiveIncentivePrograms(ctx)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", collectorErrorClass(err), err.Error()
		return
	}
	receipt.Eligible = len(programs)
	if len(programs) == 0 {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "official active liquidity-program crawl returned no programs"
		return
	}
	st := s.r138CollectorState()
	st.mu.Lock()
	selected, nextCursor := r138RotateIncentivePrograms(programs, st.incentiveCursor, r138IncentiveProgramCap)
	st.incentiveCursor = nextCursor
	st.mu.Unlock()
	if deferred := len(programs) - len(selected); deferred > 0 {
		receipt.Exclusions["rotation_budget_deferred"] = deferred
	}
	receipt.Metrics["program_universe"] = len(programs)
	receipt.Metrics["program_cap"] = r138IncentiveProgramCap
	receipt.Metrics["rotation_cursor_next"] = nextCursor
	receipt.Metrics["projected_full_rotation_cycles"] = int(math.Ceil(float64(len(programs)) / r138IncentiveProgramCap))
	programs = selected
	for _, program := range programs {
		if program.ID == "" || program.MarketTicker == "" {
			receipt.Exclusions["malformed_program_identity"]++
			continue
		}
		book, ok := s.researchKalshiBBO(ctx, program.MarketTicker, true)
		if !ok {
			receipt.Exclusions["fresh_two_sided_book_unavailable"]++
			if inserted, blocker, evalErr := s.insertStep7IncentiveEvaluation(ctx, started, "kalshi", program.ID,
				program.MarketTicker, 0, false, program, 0, 0); evalErr != nil {
				receipt.Exclusions["certified_bundle_evaluation_error"]++
			} else if inserted {
				receipt.Inserted++
				if blocker != "" {
					receipt.Exclusions[blocker]++
				}
			}
			continue
		}
		if inserted, blocker, evalErr := s.recordStep7KalshiIncentive(ctx, started, program, book); evalErr != nil {
			receipt.Exclusions["certified_bundle_evaluation_error"]++
		} else if inserted {
			receipt.Inserted++
			if blocker != "" {
				receipt.Exclusions[blocker]++
			}
		} else {
			receipt.Duplicates++
		}
		m, mok := s.kmkt(program.MarketTicker)
		for _, side := range []string{"YES", "NO"} {
			receipt.Attempted++
			b := book
			if side == "NO" {
				b = noSideBook(book)
			}
			tick, knownTick := m.TickForKnown(b.Bid)
			fee, knownFee, feeSource := s.kalFeeExact(program.MarketTicker, true, 1, b.Bid)
			if !mok || !knownTick || !knownFee {
				receipt.Exclusions["tick_or_exact_maker_fee_unavailable"]++
				continue
			}
			legacyErr := s.store.InsertIncentiveMaker(ctx, storage.IncentiveMakerObservation{
				Observed: started, ProgramID: program.ID, MarketTicker: program.MarketTicker,
				Side: side, IncentiveType: "liquidity", Description: program.IncentiveDescription,
				StartDate: program.StartDate, EndDate: program.EndDate, PeriodReward: program.PeriodReward,
				DiscountBPS: program.DiscountFactorBPS, TargetSize: program.TargetSize(), Bid: b.Bid, Ask: b.Ask,
				BidDepth: b.BidDepth, AskDepth: b.AskDepth, MakerFee: fee,
				Capacity: math.Min(b.BidDepth, b.AskDepth), BookSource: b.Source, QuoteAge: b.Age,
			})
			if legacyErr != nil {
				receipt.Exclusions["legacy_observation_storage_error"]++
			}
			curve := []storage.CapacityPoint{{Size: 1, Cost: b.Bid, Fee: fee,
				PayoutFloor: 0, NetFloor: -b.Bid - fee}}
			_, inserted, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
				Observed: started, SystemID: "incentive-subsidized-structural-lock",
				OpportunityID: program.ID + "|" + program.MarketTicker + "|" + side,
				Kind:          "control", Cohort: "active-program-uncredited-reward", CanonicalEventID: "venue:kalshi:" + firstNonEmpty(m.EventTicker, program.MarketTicker),
				Venue: "kalshi", Ticker: program.MarketTicker, Route: "maker-control", Side: side, CertificateStatus: "unverified",
				SourceClockID: "venue-notice-watcher", SourceArtifact: "GET /incentive_programs?status=active&type=liquidity",
				BookSource: b.Source, FeeSource: feeSource, QuoteAgeMax: b.Age, TickMin: tick,
				Size: 1, Cost: b.Bid, Fee: fee, PayoutLower: 0, PayoutUpper: 1,
				NetLower: -b.Bid - fee, NetUpper: 1 - b.Bid - fee, VisibleCapacity: 0,
				CapacityCurve: curve, OutcomeStatus: "open",
				QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
				Blocker: "reward lower bound is zero until competition, allocation, eligibility, queue fill, adverse selection and credited reward are observed",
				Inputs: map[string]any{"period_reward_raw": program.PeriodReward,
					"discount_bps": program.DiscountFactorBPS, "target_size": program.TargetSize(),
					"competition_touch_depth": b.BidDepth, "credited_reward_lower_bound": 0,
					"natural_order_submitted": false, "control_realized_net": 0},
			})
			if err != nil {
				if r143MissingCanonicalInstrument(err) {
					receipt.Exclusions["canonical_instrument_version_pending"]++
					continue
				}
				receipt.Exclusions["observation_storage_error"]++
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
			} else if inserted {
				receipt.Inserted++
			} else {
				receipt.Duplicates++
			}
		}
	}
	receipt.Metrics["reward_in_net_lower_bound"] = false
	receipt.Metrics["maker_controls_submit_orders"] = false
}

type r138NWSRelease struct {
	Series, Station, ForecastURL string
	Observed                     time.Time
	Periods                      []wxPeriod
}

func (s *Server) r138CachedNWSReleases() []r138NWSRelease {
	s.wxMu.Lock()
	defer s.wxMu.Unlock()
	var out []r138NWSRelease
	for _, station := range s.wxStations {
		if station == nil || station.Series == "" || station.StationID == "" || station.periodsAt.IsZero() ||
			len(station.periods) == 0 || time.Since(station.periodsAt) > wxForecastMaxStale {
			continue
		}
		out = append(out, r138NWSRelease{Series: station.Series, Station: station.StationID,
			ForecastURL: station.FcstURL, Observed: station.periodsAt,
			Periods: append([]wxPeriod(nil), station.periods...)})
	}
	return out
}

func (s *Server) r138NBMStationIDs() []string {
	s.wxMu.Lock()
	defer s.wxMu.Unlock()
	seen := map[string]bool{}
	for _, station := range s.wxStations {
		if station != nil && strings.TrimSpace(station.StationID) != "" {
			seen[strings.ToUpper(strings.TrimSpace(station.StationID))] = true
		}
	}
	out := make([]string, 0, len(seen))
	for station := range seen {
		out = append(out, station)
	}
	sort.Strings(out)
	return out
}

func (s *Server) r144NBMLastGoodStatus(now time.Time) map[string]any {
	rt := s.wxNBMRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	latest := time.Time{}
	for _, forecast := range rt.forecasts {
		if forecast.Run.After(latest) {
			latest = forecast.Run
		}
	}
	age := 0.0
	usable := false
	if !latest.IsZero() {
		age = math.Max(0, now.Sub(latest).Seconds())
		usable = now.Sub(latest) >= 0 && now.Sub(latest) <= wxNBMMaxRunAge
	}
	return map[string]any{"forecasts": len(rt.forecasts), "run": latest,
		"age_seconds": age, "usable_under_12h_research_guard": usable}
}

func (s *Server) sweepR138OfficialReleases(ctx context.Context) {
	started := time.Now()
	receipt := storage.CollectorReceipt{
		CollectorID: "official-release-adapters", CycleID: collectorCycleID(started),
		ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1, Status: "healthy",
		Started: started, Source: "NOAA NBM station bulletin + Deribit BTC/ETH summaries joined to exact option metadata and frozen risk-neutral threshold surfaces + cached NWS gridpoint forecast, each with explicit source-clock provenance",
		SchemaVersion: "r139-official-release-v3", ExpectedCadence: 30 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"deadline-hazard-surface", "score-state-surface"},
	}
	defer func() {
		receipt.Completed = time.Now()
		dctx, cancel := researchDurabilityContext(ctx)
		defer cancel()
		if _, err := s.store.InsertCollectorReceipt(dctx, receipt); err != nil && s.log != nil {
			s.log.Warn("official release collector receipt failed", "err", err)
		}
	}()
	setOperationalError := func(class, text string) {
		receipt.Status = "error"
		if receipt.ErrorClass == "" {
			receipt.ErrorClass, receipt.ErrorText = class, text
		}
	}

	// NOAA's operational probabilistic station bulletin supplies an authoritative model-run clock
	// and the TXN exceedance ladder. It is independent forecast evidence, never settlement truth.
	stations := s.r138NBMStationIDs()
	nbmAttempts := 0
	receipt.Eligible += len(stations)
	receipt.Attempted += len(stations)
	if len(stations) == 0 {
		receipt.Exclusions["nbm_station_registry_empty"]++
	} else {
		// The bulletin is ~27 MB. One slow NOAA request previously consumed 45 seconds, then older
		// fallback cycles exhausted the shared 90-second lane and starved Deribit/NWS plus the failure
		// receipt itself. Keep a dedicated source budget and leave the parent lane alive for the other
		// independent adapters. FetchLatest now stops immediately when this sub-context expires.
		nbmCtx, nbmCancel := context.WithTimeout(ctx, 25*time.Second)
		nbmReceipt, err := nbm.NewClient(15*time.Second).FetchLatest(nbmCtx, stations, started)
		nbmCancel()
		var fetchErr *nbm.FetchError
		if errors.As(err, &fetchErr) {
			nbmAttempts = fetchErr.Attempts
		} else if err == nil {
			nbmAttempts = nbmReceipt.Attempts
		}
		if err != nil {
			receipt.Exclusions["noaa_nbm_fetch_error"]++
			lastGood := s.r144NBMLastGoodStatus(started)
			lastGoodUsable, _ := lastGood["usable_under_12h_research_guard"].(bool)
			// A transient refresh timeout must not erase a still-current, previously verified
			// station/run envelope or turn the independent Deribit/NWS adapters red. The failed
			// fetch and source-clock error remain durable; only a missing/stale last-good blocks
			// the collector as a whole.
			if lastGoodUsable {
				receipt.Exclusions["noaa_nbm_refresh_failed_using_fresh_last_good"]++
			} else {
				setOperationalError("noaa_nbm", err.Error())
			}
			receipt.Metrics["nbm"] = map[string]any{"refresh_status": "error",
				"refresh_error": err.Error(), "attempts": nbmAttempts, "requested": len(stations),
				"source_budget_seconds": 25, "last_good": lastGood}
			_ = s.recordR138SourceClock(ctx, "noaa-nbm",
				"error:"+collectorCycleID(started), time.Time{}, 0, nil, nil, nil, nil,
				map[string]any{"requested_stations": len(stations), "attempts": nbmAttempts,
					"source_budget_seconds": 25, "last_good": lastGood}, "source_api", err.Error())
		} else {
			s.cacheWeatherNBMForecasts(nbmReceipt.Forecasts)
			for _, forecast := range nbmReceipt.Forecasts {
				frame := storage.OfficialReleaseFrame{Observed: started, SourceTime: forecast.Run,
					SourceID: "noaa-nbm", ArtifactID: forecast.Station + "|" + forecast.Run.Format(time.RFC3339),
					SchemaVersion: "nbm-probabilistic-text-v2", ClockStatus: "exact",
					ArtifactHash: storage.R138HashJSON(forecast), Values: forecast,
					SettlementCompatible: false}
				if inserted, insertErr := s.store.InsertOfficialReleaseFrame(ctx, frame); insertErr != nil {
					receipt.Exclusions["nbm_frame_storage_error"]++
					setOperationalError("storage", insertErr.Error())
				} else if inserted {
					receipt.Inserted++
				} else {
					receipt.Duplicates++
				}
			}
			if !s.recordR138SourceClock(ctx, "noaa-nbm",
				nbmReceipt.ArtifactURL+"|"+nbmReceipt.Run.Format(time.RFC3339Nano), nbmReceipt.Run,
				len(nbmReceipt.Forecasts), nil, nil, nil, nil,
				map[string]any{"artifact_url": nbmReceipt.ArtifactURL, "bytes": nbmReceipt.BytesRead,
					"requested": nbmReceipt.Requested, "missing": nbmReceipt.Missing,
					"settlement_compatible": false}, "", "") {
				receipt.Exclusions["nbm_source_clock_error"]++
				setOperationalError("source_clock", "NOAA NBM source-clock receipt could not be persisted")
			}
			receipt.Exclusions["nbm_station_missing"] += len(nbmReceipt.Missing)
			receipt.Metrics["nbm"] = map[string]any{"run": nbmReceipt.Run, "bytes": nbmReceipt.BytesRead,
				"attempts": nbmReceipt.Attempts, "requested": nbmReceipt.Requested,
				"forecasts": len(nbmReceipt.Forecasts), "missing": nbmReceipt.Missing}
		}
	}

	// Deribit's JSON-RPC usOut is the exact source clock. Retain the top 100 open-interest rows per
	// currency; the mapping to a prediction-market payoff remains a separately frozen model input.
	var deribitTokens []string
	latestDeribit, deribitRows, deribitSurfaceRows, deribitMetadataRows, deribitRequests, deribitCurrencies := time.Time{}, 0, 0, 0, 0, 0
	for _, currency := range []string{"BTC", "ETH"} {
		receipt.Attempted++
		client := deribit.NewClient(20 * time.Second)
		snapshot, err := client.OptionSummary(ctx, currency)
		deribitRequests++
		if err != nil {
			receipt.Exclusions["deribit_"+strings.ToLower(currency)+"_fetch_error"]++
			setOperationalError("deribit", err.Error())
			continue
		}
		deribitCurrencies++
		rows := snapshot.Rows
		if len(rows) > 100 {
			receipt.Exclusions["deribit_open_interest_cap_deferred"] += len(rows) - 100
			rows = rows[:100]
		}
		snapshot.Rows = rows
		metadata, metadataErr := client.OptionInstruments(ctx, currency)
		deribitRequests++
		if metadataErr != nil {
			receipt.Exclusions["deribit_"+strings.ToLower(currency)+"_metadata_error"]++
			setOperationalError("deribit", metadataErr.Error())
		} else {
			deribitMetadataRows += len(metadata.Rows)
			surface, surfaceErr := deribit.ThresholdSurface(snapshot, metadata)
			if surfaceErr != nil {
				receipt.Exclusions["deribit_"+strings.ToLower(currency)+"_surface_error"]++
				setOperationalError("deribit", surfaceErr.Error())
			} else {
				for _, point := range surface {
					frame := storage.OfficialReleaseFrame{Observed: started, SourceTime: point.SourceTime,
						ValidTime: point.Expiry, SourceID: "deribit-option-summary",
						ArtifactID: fmt.Sprintf("%s|threshold|%s|%.9f|%s", currency,
							point.Expiry.Format(time.RFC3339Nano), point.Strike, point.PriceIndex),
						SchemaVersion: "deribit-option-threshold-surface-v3", ClockStatus: "exact",
						ArtifactHash: storage.R138HashJSON(point), Values: point,
						SettlementCompatible: false,
						Blocker:              "risk-neutral distribution input only; exact prediction-market underlying, threshold, deadline, index and resolution-source mapping plus untouched testing remain mandatory"}
					if inserted, insertErr := s.store.InsertOfficialReleaseFrame(ctx, frame); insertErr != nil {
						receipt.Exclusions["deribit_surface_storage_error"]++
						setOperationalError("storage", insertErr.Error())
					} else if inserted {
						receipt.Inserted++
					} else {
						receipt.Duplicates++
					}
				}
				deribitSurfaceRows += len(surface)
				deribitTokens = append(deribitTokens, metadata.URL+"|"+metadata.ServerTime.Format(time.RFC3339Nano)+
					fmt.Sprintf("|surface=%d", len(surface)))
			}
		}
		for _, row := range rows {
			frame := storage.OfficialReleaseFrame{Observed: started, SourceTime: snapshot.ServerTime,
				SourceID: "deribit-option-summary", ArtifactID: currency + "|" + row.InstrumentName,
				SchemaVersion: "deribit-option-summary-v2", ClockStatus: "exact",
				ArtifactHash: storage.R138HashJSON(map[string]any{"server_time": snapshot.ServerTime, "row": row}),
				Values:       row, SettlementCompatible: false}
			if inserted, insertErr := s.store.InsertOfficialReleaseFrame(ctx, frame); insertErr != nil {
				receipt.Exclusions["deribit_frame_storage_error"]++
				setOperationalError("storage", insertErr.Error())
			} else if inserted {
				receipt.Inserted++
			} else {
				receipt.Duplicates++
			}
		}
		deribitRows += len(rows)
		deribitTokens = append(deribitTokens, snapshot.URL+"|"+snapshot.ServerTime.Format(time.RFC3339Nano))
		if snapshot.ServerTime.After(latestDeribit) {
			latestDeribit = snapshot.ServerTime
		}
	}
	if len(deribitTokens) > 0 && !s.recordR138SourceClock(ctx, "deribit-option-summary",
		storage.R138HashJSON(deribitTokens), latestDeribit, deribitRows, nil, nil, nil, nil,
		map[string]any{"currencies": deribitCurrencies, "retained_rows": deribitRows,
			"metadata_rows": deribitMetadataRows, "threshold_surface_rows": deribitSurfaceRows,
			"model":                 "Black-Scholes risk-neutral N(d2), zero rate; contributor min/max retained",
			"settlement_compatible": false}, "", "") {
		receipt.Exclusions["deribit_source_clock_error"]++
		setOperationalError("source_clock", "Deribit source-clock receipt could not be persisted")
	}
	receipt.Metrics["deribit"] = map[string]any{"currencies": deribitCurrencies, "retained_rows": deribitRows,
		"metadata_rows": deribitMetadataRows, "threshold_surface_rows": deribitSurfaceRows,
		"latest_source_time": latestDeribit, "requests": deribitRequests,
		"mapping_state": "risk-neutral surface complete; prediction-market contract identity remains fail-closed"}

	// Preserve the existing cached NWS point forecast as explicitly arrival-only secondary input.
	nwsRows := s.r138CachedNWSReleases()
	latestNWS, nwsPeriods := time.Time{}, 0
	for _, row := range nwsRows {
		receipt.Attempted++
		values := make([]map[string]any, 0, len(row.Periods))
		for _, period := range row.Periods {
			values = append(values, map[string]any{"valid_time": period.start.UTC().Format(time.RFC3339Nano), "temperature_f": period.tempF})
		}
		nwsPeriods += len(row.Periods)
		if row.Observed.After(latestNWS) {
			latestNWS = row.Observed
		}
		frame := storage.OfficialReleaseFrame{Observed: row.Observed, SourceID: "nws-hourly-gridpoint",
			ArtifactID: row.Series + "|" + row.Station, SchemaVersion: "nws-gridpoint-hourly-v1",
			ClockStatus: "arrival_only", ArtifactHash: storage.R138HashJSON(values),
			Values:               map[string]any{"series": row.Series, "station": row.Station, "forecast_url": row.ForecastURL, "periods": values},
			SettlementCompatible: false,
			Blocker:              "arrival-only point forecast; NOAA NBM is the independent probabilistic source and venue CLI observation decides settlement"}
		if inserted, err := s.store.InsertOfficialReleaseFrame(ctx, frame); err != nil {
			receipt.Exclusions["nws_frame_storage_error"]++
			setOperationalError("storage", err.Error())
		} else if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
	}
	nwsToken := fmt.Sprintf("%s:%d:%d", latestNWS.UTC().Format(time.RFC3339Nano), len(nwsRows), nwsPeriods)
	if len(nwsRows) == 0 {
		nwsToken = "empty:" + started.UTC().Truncate(30*time.Minute).Format(time.RFC3339)
		receipt.Exclusions["nws_cache_empty"]++
	}
	if !s.recordR138SourceClock(ctx, "nws-hourly-gridpoint", nwsToken,
		time.Time{}, nwsPeriods, nil, nil, nil, nil,
		map[string]any{"clock_status": "arrival_only", "cached_series": len(nwsRows),
			"periods": nwsPeriods, "latest_cache_arrival": latestNWS, "external_request_added": false,
			"settlement_compatible": false}, "", "") {
		receipt.Exclusions["nws_source_clock_error"]++
		setOperationalError("source_clock", "NWS source-clock receipt could not be persisted")
	}
	receipt.Metrics["nws_arrival_only_frames"] = len(nwsRows)
	receipt.Metrics["external_requests"] = nbmAttempts + deribitRequests
}

func r138RuntimeCoverageContracts() []storage.ResearchSystemRuntimeReceipt {
	rows := []storage.ResearchSystemRuntimeReceipt{
		{SystemID: "attention-spillover-graph", State: "COLLECTING",
			Reason:       "structural same-game parent-child observations now receive immutable exact-book, exact-fee and source-clock 60s/300s executable markouts; missed targets retain explicit immutable reasons",
			CollectorIDs: []string{"concrete-paired-systems", "concrete-attention-horizons"}, EvidenceTables: []string{"market_game", "signal_log", "research_system_observations", "research_attention_markouts", "research_attention_markout_misses", "research_system_payoff_updates"},
			Prerequisites: []string{"untouched event-day replication", "positive route-specific lower bound"}},
		{SystemID: "clientele-clock-basis", State: "COLLECTING",
			Reason:       "current rule-certified K-PUS books collect as paired venue-local-hour, game-phase and liquidity cells with exact side books/fees",
			CollectorIDs: []string{"concrete-paired-systems"}, EvidenceTables: []string{"market_game", "research_rule_pair_certificates", "research_system_observations", "research_system_payoff_updates"},
			Prerequisites: []string{"event-disjoint untouched day holdout", "positive route-specific lower bound"}},
		{SystemID: "collateral-release-rotation", State: "COLLECTING_PARTIAL",
			Reason:       "Kalshi settled_time+revenue credit receipts trigger linked child books plus deterministic same-venue, same-league/type/line/phase/liquidity no-release market controls; PolyUS exposes updateTime but no unambiguous credited cash amount",
			CollectorIDs: []string{"concrete-paired-systems"}, EvidenceTables: []string{"research_credit_events", "market_game", "research_system_observations", "research_system_payoff_updates"},
			Prerequisites: []string{"PolyUS authoritative credited cash amount", "untouched post-credit horizon replication"}},
		{SystemID: "deadline-hazard-surface", State: "COLLECTING_PARTIAL",
			Reason:         "verified implication scans and independent NOAA NBM station/run candidates now join current exact-fee books; non-weather relation coverage and untouched replication remain incomplete",
			CollectorIDs:   []string{"deadline-hazard-certified", "payoff-envelope-capacity", "weather-research", "deadline-source-funnel"},
			EvidenceTables: []string{"research_system_observations", "research_payoff_relations", "weather_research_trials", "audit_log"},
			Prerequisites:  []string{"broader verified implication producers", "untouched event-day lower-bound replication"}},
		{SystemID: "flow-direction-integrity", State: "COLLECTING",
			Reason:       "every existing subscribed Kalshi public trade preserves canonical and inferred labels separately; a valid authoritative bought side is now repriced from one current complete book as an exact prospective action beside its same-clock opposite-side control",
			CollectorIDs: []string{"step7-event-microstructure", "replenishment-toxicity"}, EvidenceTables: []string{"research_flow_direction_pairs", "research_microstructure_frames", "research_microstructure_horizons", "research_system_observations"},
			Prerequisites: []string{"untouched event-day replication", "positive route-specific lower bound"}},
		{SystemID: "forecast-persona-router", State: "COLLECTING",
			Reason:       "a frozen 30-day prior window with 24-hour embargo selects Brier/log per persona; later same-clock selected/opposite controls collect without authority",
			CollectorIDs: []string{"proper-score", "concrete-paired-systems"}, EvidenceTables: []string{"research_proper_score_trials", "research_system_observations", "research_system_payoff_updates"}},
		{SystemID: "identity-challenged-cross-venue-lock", State: "COLLECTING_PARTIAL",
			Reason:       "exact raw K-PUS, K-PINT, and PUS-PINT rule artifacts plus per-pair review blockers collect; each economic pair requires a current independently normalized orientation certificate, and three-way status requires the complete pairwise triangle",
			CollectorIDs: []string{"semantic-basis-router", "event-basket-lock", "identity-lock-funnel", "cross-venue-rule-artifacts"}, EvidenceTables: []string{"research_route_opportunities", "research_event_baskets", "research_instrument_specs", "research_payoff_relations", "research_rule_artifacts", "research_rule_pair_reviews", "research_rule_pair_certificates", "research_crossvenue_match_decisions", "research_system_observations"},
			Prerequisites: []string{"current raw rules for both venue instruments", "official settlement-source identifiers/URLs", "independent normalized predicate/timing/void/scalar/unknown review including same/inverse orientation", "all three pairwise current certificates for a three-venue claim", "simultaneous sequence-stamped books", "both venue fee receipts", "partial-fill/unwind envelope"}},
		{SystemID: "incentive-subsidized-structural-lock", State: "COLLECTING_PARTIAL",
			Reason:       "official Kalshi and PolyUS active programs rotate fairly and join only to current certified typed bundles; competition is measured, but reward lower stays zero unless an exact natural order's earned credit can be attributed",
			CollectorIDs: []string{"incentive-economics", "polyus-incentive-economics"}, EvidenceTables: []string{"research_incentive_maker", "research_incentive_lock_evaluations", "research_route_bundles", "research_system_observations"},
			Prerequisites: []string{"exact program/order eligibility", "bundle-attributed natural queue and fill", "order-attributed credited reward; market/day aggregate credit is context only"}},
		{SystemID: "maker-salvage-matched-cohort", State: "COLLECTING_PARTIAL",
			Reason:         "an already admitted exact maker order can now receive the no-duplicate maker-salvage overlay; natural attempts mirror queue, fill/cancel, exact fee/rebate, zero-aware 5m markout and settlement, while cancels and controls remain exactly zero",
			CollectorIDs:   []string{"queue-priority", "step7-maker-salvage", "step7-event-microstructure"},
			EvidenceTables: []string{"research_queue_orders", "research_queue_samples", "research_maker_salvage_trials", "research_maker_salvage_events", "research_system_observations"},
			Prerequisites:  []string{"new attempts must freeze the same model's executable taker rejection before matched-candidate status", "untouched replication and current-book recheck"}},
		{SystemID: "outcome-set-expansion-shock", State: "COLLECTING",
			Reason:         "official event membership snapshots retain baselines, unchanged controls, additions, removals and status changes",
			CollectorIDs:   []string{"outcome-set-surface", "subcent-golf", "lifecycle-reopen"},
			EvidenceTables: []string{"research_outcome_set_frames", "subcent_golf_trials", "research_system_observations"}},
		{SystemID: "paired-bridge-inversion", State: "COLLECTING",
			Reason:       "bridge/gap triggers now freeze direct and inverse asks from one current payoff-certified book with exact tick/depth/fees and common terminal settlement",
			CollectorIDs: []string{"concrete-paired-systems"}, EvidenceTables: []string{"signal_log", "research_system_observations", "research_system_payoff_updates"},
			Prerequisites: []string{"untouched paired lower-bound replication"}},
		{SystemID: "payoff-constraint-solver", State: "COLLECTING_PARTIAL",
			Reason:         "bounded solver and exact nonlinear fee envelopes run, but semantic cross-venue output is not yet a solver certificate and decision latency is not measured",
			CollectorIDs:   []string{"payoff-envelope-capacity", "semantic-basis-router", "event-basket-lock", "nested-ladder-lock"},
			EvidenceTables: []string{"research_system_observations", "research_event_baskets", "research_nested_ladders"}},
		{SystemID: "proper-score-executor", State: "COLLECTING",
			Reason:       "unselected book-native forecasts, abstentions, exact fees and binary settlements collect in a zero-authority ledger",
			CollectorIDs: []string{"proper-score", "proper-score-system-funnel"}, EvidenceTables: []string{"research_proper_score_trials", "research_system_observations"}},
		{SystemID: "replenishment-fingerprint", State: "COLLECTING_PARTIAL",
			Reason:         "every existing subscribed top-touch delta is observed in memory; durable depletion episodes retain refill latency plus 5/30/300s marks, trade-attributed removals and bounded queue-churn proxies without creating a raw tick archive",
			CollectorIDs:   []string{"step7-event-microstructure", "replenishment-toxicity", "queue-priority"},
			EvidenceTables: []string{"research_replenishment_episodes", "research_replenishment_marks", "research_microstructure_frames", "research_microstructure_horizons", "research_queue_samples"},
			Prerequisites:  []string{"book-minus-trade removals remain a cancel proxy because the public feed has no authoritative cancel message", "route-specific untouched settlement replication"}},
		{SystemID: "score-state-surface", State: "COLLECTING_PARTIAL",
			Reason:       "structurally linked moneyline/spread/total books collect; no frozen latent game-state or joint correlation model exists",
			CollectorIDs: []string{"score-state-surface"}, EvidenceTables: []string{"research_system_observations", "market_game"}},
		{SystemID: "semantic-complexity-premium", State: "COLLECTING",
			Reason:       "current raw rule artifacts receive deterministic clause/source/void/revision/time-boundary features and exact side pairs; the existing prior event-disjoint freezer emits only post-freeze selected actions to the shared single-order route",
			CollectorIDs: []string{"venue-notices", "concrete-paired-systems"}, EvidenceTables: []string{"research_rule_artifacts", "research_system_observations", "research_system_payoff_updates"},
			Prerequisites: []string{"event-day untouched complexity-bin replication"}},
		{SystemID: "series-roll-anchor", State: "COLLECTING",
			Reason:       "official series-to-event mappings now join a prior issue's prospective book+settlement to current same-kind two-sided books; ticker prefixes are never used",
			CollectorIDs: []string{"event-basket-lock", "concrete-paired-systems"}, EvidenceTables: []string{"research_series_event_map", "market_catalog", "signal_log", "research_system_observations", "research_system_payoff_updates"}},
		{SystemID: "settlement-latency-carry", State: "COLLECTING_PARTIAL",
			Reason:       "near-certain current-book entries now freeze a prior-only Wilson void/dispute reserve and the best pre-existing sealed route lower bound, or a formally justified nominal-cash floor; unknown capital time is immutably blocked rather than backfilled",
			CollectorIDs: []string{"concrete-paired-systems"}, EvidenceTables: []string{"research_carry_frozen_inputs", "research_credit_events", "signal_log", "research_system_observations", "research_system_payoff_updates"},
			Prerequisites: []string{"PolyUS authoritative credited cash amount for direct cash-release attribution", "untouched carry replication", "positive reserve-net route-specific lower bound"}},
		{SystemID: "side-normalized-crowding-fade", State: "COLLECTING",
			Reason:       "book-native flow/whale/concentration triggers now freeze direct and fade sides from one current book with exact tick/depth/fees and common terminal settlement",
			CollectorIDs: []string{"concrete-paired-systems"}, EvidenceTables: []string{"signal_log", "research_system_observations", "research_system_payoff_updates"},
			Prerequisites: []string{"untouched bought-side agreement-bin lower bound"}},
	}
	return rows
}

func r138RuntimeContractsWithLiveness(contracts []storage.ResearchSystemRuntimeReceipt,
	views []storage.CollectorLivenessView) []storage.ResearchSystemRuntimeReceipt {
	byID := make(map[string]storage.CollectorLivenessView, len(views))
	for _, view := range views {
		byID[view.CollectorID] = view
	}
	out := make([]storage.ResearchSystemRuntimeReceipt, 0, len(contracts))
	for _, contract := range contracts {
		if contract.State == "BLOCKED" || len(contract.CollectorIDs) == 0 {
			out = append(out, contract)
			continue
		}
		healthy := 0
		var blockers []string
		for _, collectorID := range contract.CollectorIDs {
			view, ok := byID[collectorID]
			if ok && !view.NeverRan && !view.Alert && (view.Status == "healthy" || view.Status == "healthy_empty") {
				healthy++
				continue
			}
			status := "NEVER_RAN"
			if ok {
				status = strings.ToUpper(firstNonEmpty(view.Status, "NEVER_RAN"))
				if view.Alert && status == "HEALTHY" {
					status = "STALE"
				}
			}
			blockers = append(blockers, collectorID+"="+status)
			contract.Prerequisites = append(contract.Prerequisites,
				"fresh healthy or healthy_empty receipt from "+collectorID)
		}
		if len(blockers) > 0 {
			if healthy == 0 {
				contract.State = "BLOCKED"
			} else {
				contract.State = "COLLECTING_PARTIAL"
			}
			contract.Reason += "; effective runtime collector blockers: " + strings.Join(blockers, ", ")
		}
		out = append(out, contract)
	}
	return out
}

func (s *Server) sweepR138SystemCoverage(ctx context.Context) {
	started := time.Now()
	contracts := r138RuntimeCoverageContracts()
	liveness, livenessErr := s.store.CollectorLivenessReport(ctx)
	if livenessErr == nil {
		if views, ok := liveness["collectors"].([]storage.CollectorLivenessView); ok {
			contracts = r138RuntimeContractsWithLiveness(contracts, views)
		} else {
			livenessErr = errors.New("collector liveness report has unexpected shape")
		}
	}
	if livenessErr != nil {
		contracts = r138RuntimeContractsWithLiveness(contracts, nil)
	}
	receipt := storage.CollectorReceipt{CollectorID: "system-contract-coverage", CycleID: collectorCycleID(started),
		Status: "healthy", Started: started, Source: "runtime collector/evidence contract audit for every immutable system ID",
		SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 5 * time.Minute,
		Eligible: len(contracts), Attempted: len(contracts), Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: storage.ResearchExperimentIDs()}
	if livenessErr != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "collector_liveness", livenessErr.Error()
	}
	states := map[string]int{}
	for _, contract := range contracts {
		contract.Observed = started
		inserted, err := s.store.InsertResearchSystemRuntimeReceipt(ctx, contract)
		if err != nil {
			receipt.Exclusions["runtime_contract_storage_error"]++
			receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
		} else if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
		states[contract.State]++
	}
	receipt.Completed = time.Now()
	receipt.Metrics["states"] = states
	receipt.Metrics["expected_systems"] = 19
	receipt.Metrics["reported_systems"] = len(contracts)
	durableCtx, cancelDurability := researchDurabilityContext(ctx)
	defer cancelDurability()
	_, _ = s.store.InsertCollectorReceipt(durableCtx, receipt)
}

func (s *Server) sweepR138SystemCoverageIfDue(ctx context.Context, now time.Time) {
	st := s.r138CollectorState()
	st.mu.Lock()
	due := st.lastCoverage.IsZero() || now.Sub(st.lastCoverage) >= 5*time.Minute
	if due {
		st.lastCoverage = now
	}
	st.mu.Unlock()
	if due {
		s.sweepR138SystemCoverage(ctx)
	}
}

func (s *Server) sweepR138Step6To9(ctx context.Context) {
	now := time.Now()
	st := s.r138CollectorState()
	st.mu.Lock()
	doFlush := now.Sub(st.lastFlush) >= 5*time.Second
	doFallback := now.Sub(st.lastFallback) >= time.Minute
	doScore := now.Sub(st.lastScoreState) >= 5*time.Minute
	doDeadline := now.Sub(st.lastDeadline) >= 5*time.Minute
	doRelease := now.Sub(st.lastRelease) >= 30*time.Minute
	if doFlush {
		st.lastFlush = now
	}
	if doFallback {
		st.lastFallback = now
	}
	if doScore {
		st.lastScoreState = now
	}
	if doDeadline {
		st.lastDeadline = now
	}
	if doRelease {
		st.lastRelease = now
	}
	st.mu.Unlock()
	if doRelease {
		// Unsupported external adapters and arrival-only NWS cache truth must become explicit source
		// receipts before heavier score/deadline scans; NEVER_RAN is not a valid steady-state blocker.
		s.sweepR138OfficialReleases(ctx)
	}
	if doFallback {
		s.fallbackR138BookCapture()
	}
	if doFlush {
		s.flushR138BookFrames(ctx)
		s.settleR138BookHorizons(ctx)
		s.flushStep7Microstructure(ctx, now)
		s.sweepStep7MakerSalvage(ctx, now)
		s.sweepConcreteTimedEvidence(ctx, now)
	}
	if doScore {
		s.scanR138ScoreState(ctx)
		s.sweepSemanticBasis(ctx)
	}
	if doDeadline {
		s.scanR138CertifiedDeadlines(ctx)
	}
}

func (s *Server) handleR138SystemEvidence(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.ResearchSystemEvidenceReport(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}
