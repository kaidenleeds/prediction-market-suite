package server

import (
	"context"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	kalshiFlowEventWindow      = 7 * time.Minute
	kalshiFlowEventMinNotional = 250.0
	kalshiFlowEventMinStrength = 0.60
	kalshiFlowEventMinPrice    = 0.05
	kalshiFlowEventMaxPrice    = 0.95
	kalshiFlowEventQueueCap    = 2048
)

type kalshiFlowEventDecision struct {
	Side       string
	Strength   float64
	Notional   float64
	YesPrice   float64
	SidePrice  float64
	TradeCount int
}

// kalshiFlowEventDecisionFromTape is the one-ticker form of handleWhales' exact seven-minute
// Kalshi-flow detector. The periodic producer remains the durable DB/Paper backstop; this helper
// only removes its one-minute discovery delay from the real-money lane.
func kalshiFlowEventDecisionFromTape(tape []kalshi.Trade, ticker string, now time.Time,
	implied float64) (kalshiFlowEventDecision, bool) {
	ticker = strings.TrimSpace(ticker)
	up := strings.ToUpper(ticker)
	if ticker == "" || strings.Contains(up, "KXMVE") || strings.Contains(up, "MULTIGAME") ||
		strings.Contains(up, "CROSSCATEGORY") {
		return kalshiFlowEventDecision{}, false
	}
	cutoff := now.Add(-kalshiFlowEventWindow)
	yesNotional, noNotional := 0.0, 0.0
	lastYes, trades := 0.0, 0
	for _, trade := range tape {
		if trade.Ticker != ticker {
			continue
		}
		if lastYes <= 0 {
			lastYes = trade.YesPrice.Float() // LiveTape is newest-first, matching handleWhales.
		}
		if created, err := time.Parse(time.RFC3339, trade.CreatedTime); err == nil && !created.After(cutoff) {
			continue
		}
		count := trade.Count.Float()
		sidePrice := trade.YesPrice.Float()
		if strings.EqualFold(trade.Aggressor(), "no") {
			sidePrice = trade.NoPrice.Float()
			noNotional += count * sidePrice
		} else {
			yesNotional += count * sidePrice
		}
		trades++
	}
	total := yesNotional + noNotional
	// Venue prices/counts arrive as decimal strings but live as float64 in the shared tape.
	// Preserve the stated >=$250 boundary across an otherwise sub-nanocent representation wobble.
	if total+1e-9 < kalshiFlowEventMinNotional {
		return kalshiFlowEventDecision{}, false
	}
	side, strength := "YES", yesNotional/total
	if noNotional > yesNotional {
		side, strength = "NO", noNotional/total
	}
	if strength < kalshiFlowEventMinStrength {
		return kalshiFlowEventDecision{}, false
	}
	if implied <= 0 {
		implied = lastYes
	}
	if implied < kalshiFlowEventMinPrice || implied > kalshiFlowEventMaxPrice {
		return kalshiFlowEventDecision{}, false
	}
	sidePrice := implied
	if side == "NO" {
		sidePrice = 1 - implied
	}
	return kalshiFlowEventDecision{Side: side, Strength: strength, Notional: total,
		YesPrice: implied, SidePrice: sidePrice, TradeCount: trades}, true
}

type kalshiFlowEventWake struct {
	Ticker     string
	ObservedAt time.Time
	QueuedAt   time.Time
}

type kalshiFlowEventPublication struct {
	Side  string
	Price float64
	At    time.Time
}

type kalshiFlowEventRuntime struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	wake   chan string

	queueMu sync.Mutex
	queued  map[string]kalshiFlowEventWake

	lifecycleMu   sync.Mutex
	publicationMu sync.Mutex
	published     map[string]kalshiFlowEventPublication
}

var (
	// Runtime creation and shutdown share one lock. The stop latch is set while this lock is held,
	// so no callback can pass the latch and publish a replacement runtime after LoadAndDelete.
	kalshiFlowEventRuntimesMu sync.Mutex
	kalshiFlowEventRuntimes   sync.Map // map[*Server]*kalshiFlowEventRuntime
)

func newKalshiFlowEventRuntime() *kalshiFlowEventRuntime {
	ctx, cancel := context.WithCancel(context.Background())
	return &kalshiFlowEventRuntime{ctx: ctx, cancel: cancel, done: make(chan struct{}),
		wake:   make(chan string, kalshiFlowEventQueueCap),
		queued: map[string]kalshiFlowEventWake{}, published: map[string]kalshiFlowEventPublication{}}
}

func kalshiFlowEventRuntimeFor(s *Server) *kalshiFlowEventRuntime {
	if s == nil || s.kalshiFlowEventStopping.Load() {
		return nil
	}
	// Existing-runtime callbacks are the hot path. They never wait on the creation/shutdown mutex;
	// the latch and per-runtime cancellation/publish gate make a raced stale pointer harmless.
	if value, ok := kalshiFlowEventRuntimes.Load(s); ok {
		if s.kalshiFlowEventStopping.Load() {
			return nil
		}
		return value.(*kalshiFlowEventRuntime)
	}
	kalshiFlowEventRuntimesMu.Lock()
	defer kalshiFlowEventRuntimesMu.Unlock()
	if s.kalshiFlowEventStopping.Load() {
		return nil
	}
	if value, ok := kalshiFlowEventRuntimes.Load(s); ok {
		return value.(*kalshiFlowEventRuntime)
	}
	created := newKalshiFlowEventRuntime()
	kalshiFlowEventRuntimes.Store(s, created)
	go created.run(s)
	return created
}

// offer is the only work added to the Kalshi WS reader. It coalesces repeated trades for the same
// ticker and never waits for the evaluator, SQLite, Paper, venue I/O, or an order path.
func (runtime *kalshiFlowEventRuntime) offer(wake kalshiFlowEventWake) bool {
	if runtime == nil || wake.Ticker == "" {
		return false
	}
	if runtime.ctx != nil {
		select {
		case <-runtime.ctx.Done():
			return false
		default:
		}
	}
	runtime.queueMu.Lock()
	if prior, exists := runtime.queued[wake.Ticker]; exists {
		if wake.ObservedAt.After(prior.ObservedAt) {
			prior.ObservedAt = wake.ObservedAt
		}
		// Preserve the first enqueue clock so worker queue time includes coalescing delay.
		runtime.queued[wake.Ticker] = prior
		runtime.queueMu.Unlock()
		return true
	}
	if len(runtime.queued) >= kalshiFlowEventQueueCap {
		runtime.queueMu.Unlock()
		return false
	}
	runtime.queued[wake.Ticker] = wake
	select {
	case runtime.wake <- wake.Ticker:
		runtime.queueMu.Unlock()
		return true
	default:
		delete(runtime.queued, wake.Ticker)
		runtime.queueMu.Unlock()
		return false
	}
}

func (runtime *kalshiFlowEventRuntime) take(ticker string) (kalshiFlowEventWake, bool) {
	runtime.queueMu.Lock()
	defer runtime.queueMu.Unlock()
	wake, ok := runtime.queued[ticker]
	if ok {
		delete(runtime.queued, ticker)
	}
	return wake, ok
}

func sameKalshiFlowPublication(publication kalshiFlowEventPublication, sig storage.Signal) bool {
	return strings.EqualFold(publication.Side, strings.TrimSpace(sig.Side)) &&
		math.Abs(publication.Price-sig.EntryPrice) <= 1e-9
}

// claimPublication is only an in-flight evaluator guard. It is always released when the worker
// finishes its nonblocking LIVE queue offer; it is not evidence that downstream preflight reached
// Kalshi and therefore must never suppress a later periodic retry.
func (runtime *kalshiFlowEventRuntime) claimPublication(sig storage.Signal, now time.Time) bool {
	runtime.publicationMu.Lock()
	defer runtime.publicationMu.Unlock()
	if _, ok := runtime.published[sig.Ticker]; ok {
		return false
	}
	runtime.published[sig.Ticker] = kalshiFlowEventPublication{
		Side: strings.ToUpper(strings.TrimSpace(sig.Side)), Price: sig.EntryPrice, At: now}
	return true
}

func (runtime *kalshiFlowEventRuntime) rollbackPublication(sig storage.Signal, claimedAt time.Time) {
	runtime.publicationMu.Lock()
	if publication, ok := runtime.published[sig.Ticker]; ok && publication.At.Equal(claimedAt) &&
		sameKalshiFlowPublication(publication, sig) {
		delete(runtime.published, sig.Ticker)
	}
	runtime.publicationMu.Unlock()
}

func (runtime *kalshiFlowEventRuntime) run(s *Server) {
	defer close(runtime.done)
	for {
		select {
		case <-runtime.ctx.Done():
			return
		case ticker := <-runtime.wake:
			wake, ok := runtime.take(ticker)
			if !ok {
				continue
			}
			s.evaluateKalshiFlowEvent(runtime, wake)
		}
	}
}

func kalshiFlowEventAllowlisted(raw string) bool {
	const (
		yes = "kalshi|kalshi-flow|YES|taker"
		no  = "kalshi|kalshi-flow|NO|taker"
	)
	for start := 0; start <= len(raw); {
		end := start
		for end < len(raw) {
			switch raw[end] {
			case ',', ';', '\n', '\r':
				goto compare
			}
			end++
		}
	compare:
		entry := strings.TrimSpace(raw[start:end])
		if strings.EqualFold(entry, yes) || strings.EqualFold(entry, no) {
			return true
		}
		if end == len(raw) {
			break
		}
		start = end + 1
	}
	return false
}

func (s *Server) queueKalshiFlowTradeWake(ev kalshi.ResearchReplayEvent) {
	if s == nil || s.kalshiFlowEventStopping.Load() || ev.Kind != "trade" || ev.Trade == nil ||
		strings.TrimSpace(ev.EntityID) == "" {
		return
	}
	// Do not create or feed the Flow worker while real LIVE is off or the exact operator
	// allowlist contains no Flow family at all. The final side-specific selection check remains
	// authoritative in the worker; this cheap family precheck keeps Spotlag-only operation from
	// spending CPU on an unselected high-rate trade stream.
	directEnabled := s.liveOperatorAutoOn() &&
		kalshiFlowEventAllowlisted(s.cfg().Risk.LiveSystemAllowlist)
	if !directEnabled && !s.inverseKflowShadowEnabled() {
		return
	}
	observed := ev.ObservedAt
	if observed.IsZero() {
		observed = time.Now()
	}
	runtime := kalshiFlowEventRuntimeFor(s)
	if runtime == nil {
		return
	}
	_ = runtime.offer(kalshiFlowEventWake{
		Ticker: ev.EntityID, ObservedAt: observed, QueuedAt: time.Now()})
}

func (s *Server) cachedKalshiFlowMetadata(ticker string) (name string, implied float64) {
	name = ticker
	s.metaMu.Lock()
	if meta, ok := s.meta[ticker]; ok {
		if meta.name != "" {
			name = meta.name
		}
		implied = meta.implied
	} else if market, ok := s.kmkts[ticker]; ok {
		name = market.Title
		if market.YesSubTitle != "" {
			name += " — " + market.YesSubTitle
		}
		implied = market.ImpliedProbability()
	}
	s.metaMu.Unlock()
	return
}

// evaluateKalshiFlowEvent runs off the WS reader. It performs only resident-memory reads and the
// same nonblocking LIVE queue offer used by the periodic producer; no SQLite or Paper function is
// reachable from this raw event path. A later cash-preflight pass owns any funded-Paper fanout.
func (s *Server) evaluateKalshiFlowEvent(runtime *kalshiFlowEventRuntime, wake kalshiFlowEventWake) {
	if s == nil || runtime == nil || s.kalshiFlowEventStopping.Load() ||
		runtime.ctx.Err() != nil || s.kal == nil {
		return
	}
	tape, fresh := s.kal.LiveTape()
	if !fresh {
		return
	}
	name, implied := s.cachedKalshiFlowMetadata(wake.Ticker)
	evaluatedAt := time.Now().UTC()
	decision, ok := kalshiFlowEventDecisionFromTape(tape, wake.Ticker, evaluatedAt, implied)
	if !ok {
		return
	}
	spread, resolveHours, depth := s.kalSigMetaCached(wake.Ticker)
	imbalance, _ := s.kalImbalanceCached(wake.Ticker)
	momentum, _ := s.kal.Momentum(wake.Ticker)
	sig := storage.Signal{Platform: "kalshi", Ticker: wake.Ticker,
		Title: friendlyName(wake.Ticker, name, ""), Side: decision.Side,
		SignalType: "kalshi-flow", EntryPrice: decision.SidePrice,
		Strength: decision.Strength, Notional: decision.Notional,
		Momentum: momentum, Imbalance: imbalance, SpreadCents: spread,
		ResolveHours: resolveHours, BookDepth: depth,
		Confidence: s.signalConfidence("kalshi-flow", decision.SidePrice),
		ExecExpr:   r147InputReceiptExpr("K", wake.ObservedAt)}
	// LiveTape was read and the decision recomputed above. Use the actual emission boundary for
	// downstream pipeline latency; the coalesced source-trade receipt remains independently
	// preserved in ExecExpr/InputObservedAt and shadow evidence.
	decisionAt := time.Now().UTC()
	point, liveSelected := 0.0, false
	if s.liveOperatorAutoOn() {
		point, liveSelected = s.kalshiFlowEventLivePoint(sig)
	}
	shadowSelected := s.inverseKflowShadowEnabled() && strings.EqualFold(sig.Side, "YES")
	if !liveSelected && !shadowSelected {
		// Fail silently here. The periodic producer remains responsible for the durable
		// unselected-candidate receipt; a high-rate public trade feed must not flood that audit.
		return
	}
	// Cash keeps first publication. The shadow fanout below is a later nonblocking queue write and
	// has no proof, balance, position, REST, SQLite, or venue work on this evaluator.
	if liveSelected {
		claimedAt := time.Now()
		if runtime.claimPublication(sig, claimedAt) {
			s.noteLiveSignalDepthBookHint(sig)
			runtime.lifecycleMu.Lock()
			if !s.kalshiFlowEventStopping.Load() && runtime.ctx.Err() == nil {
				_ = s.queueLiveSignalIntentAt(sig, point, decisionAt)
			}
			runtime.lifecycleMu.Unlock()
			runtime.rollbackPublication(sig, claimedAt)
		}
	}
	if shadowSelected && !s.kalshiFlowEventStopping.Load() && runtime.ctx.Err() == nil {
		_ = s.queueInverseKflowShadow(sig, decisionAt)
	}
}

func (s *Server) kalshiFlowEventSelected(sig storage.Signal) bool {
	candidate := liveMirrorCandidate{Platform: strings.ToLower(strings.TrimSpace(sig.Platform)),
		Ticker: strings.TrimSpace(sig.Ticker), Side: strings.ToUpper(strings.TrimSpace(sig.Side)),
		Source: "auto-cons-" + strings.TrimSpace(sig.SignalType), Family: strings.TrimSpace(sig.SignalType),
		Price: sig.EntryPrice, At: time.Now(), InputTopology: r147SignalInputTopology(sig),
		InputObservedAt: r147SignalInputObservedAt(sig)}
	return s.liveMirrorEnabled() && s.liveSystemSelectionReason(candidate, "taker") == ""
}

func (s *Server) kalshiFlowEventLivePoint(sig storage.Signal) (float64, bool) {
	// Selection is checked before the event worker claims its brief in-flight publication guard.
	// This is intentionally fail-silent: the periodic producer owns durable receipts for unselected
	// systems, while Spotlag-only operation must not emit one Flow rejection per public trade.
	if !s.kalshiFlowEventSelected(sig) {
		return 0, false
	}
	cell, ok := s.gfModeCached(sig.SignalType, sig.Platform, sig.Side)
	if !ok {
		return 0, false
	}
	return cell.Mean, true
}

func stopKalshiFlowEventRuntime(s *Server, ctx context.Context) {
	if s == nil {
		return
	}
	kalshiFlowEventRuntimesMu.Lock()
	s.kalshiFlowEventStopping.Store(true)
	value, ok := kalshiFlowEventRuntimes.LoadAndDelete(s)
	kalshiFlowEventRuntimesMu.Unlock()
	if !ok {
		return
	}
	runtime := value.(*kalshiFlowEventRuntime)
	runtime.lifecycleMu.Lock()
	runtime.cancel()
	runtime.lifecycleMu.Unlock()
	if ctx == nil {
		return
	}
	select {
	case <-runtime.done:
	case <-ctx.Done():
	}
}
