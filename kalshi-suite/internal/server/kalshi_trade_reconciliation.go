package server

// Kalshi publishes the two sides of one execution on independent channels:
//   - the authenticated fill stream proves what this account received;
//   - the public trade stream proves what the market printed.
//
// Both messages carry trade_id. The callback path below performs only a bounded, nonblocking
// channel send. One background owner does all matching, lineage reads, REST recovery, and durable
// execution-shadow writes, so this evidence can never delay or authorize an order.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	kalshiTradeReconcileQueueDefault = 8192
	kalshiTradeReconcileCacheDefault = 4096
	kalshiTradeReconcileRecoveryMax  = 4096
	kalshiTradeReconcileLookupLimit  = 1000
)

type kalshiTradeReconcileKind uint8

const (
	kalshiTradePrivateObserved kalshiTradeReconcileKind = iota + 1
	kalshiTradePublicObserved
	kalshiTradePublicNotFound
	kalshiTradePublicLookupFailed
)

type kalshiTradeReconcileObservation struct {
	kind       kalshiTradeReconcileKind
	fill       kalshi.Fill
	trade      kalshi.Trade
	tradeID    string
	ticker     string
	receivedAt time.Time
	detail     string
}

type kalshiTradeReconcileHandle struct {
	// admission is the shutdown fence for callback publishers. TryRLock keeps the callback
	// nonblocking while guaranteeing that stop cannot finish draining and then receive a late
	// observation from a goroutine which loaded the old atomic handle.
	admission sync.RWMutex
	closed    bool
	ch        chan kalshiTradeReconcileObservation
}

type kalshiTradeReconcileRuntime struct {
	ch       chan kalshiTradeReconcileObservation
	handle   *kalshiTradeReconcileHandle
	stop     chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
	stopOnce sync.Once
	recovery sync.WaitGroup

	cacheMax int
	private  map[string]kalshiPrivateTradeEvidence
	public   map[string]kalshiPublicTradeEvidence
	terminal map[string]string
	conflict map[string]string
	privFIFO []kalshiTradeCacheRef
	pubFIFO  []kalshiTradeCacheRef
	termFIFO []kalshiTradeCacheRef
}

type kalshiTradeCacheRef struct {
	tradeID string
	stamp   time.Time
}

type kalshiPrivateTradeEvidence struct {
	tradeID, orderID, ticker, side, action string
	qty, price, fee                        float64
	feeKnown, isTaker                      bool
	sourceAt, receivedAt                   time.Time
	reservationID, attemptID               string
}

type kalshiPublicTradeEvidence struct {
	tradeID, ticker                 string
	qty, yesPrice, noPrice          float64
	takerOutcomeSide, takerBookSide string
	aggressor                       string
	isBlock                         bool
	sourceAt, receivedAt            time.Time
}

func (s *Server) startKalshiTradeReconciler() {
	if s == nil || s.store == nil {
		return
	}
	s.kalshiTradeReconcileMu.Lock()
	defer s.kalshiTradeReconcileMu.Unlock()
	if s.kalshiTradeReconcileRuntime != nil {
		return
	}
	capacity := s.kalshiTradeReconcileCapacity
	if capacity <= 0 {
		capacity = kalshiTradeReconcileQueueDefault
	}
	cacheMax := s.kalshiTradeReconcileCacheMax
	if cacheMax <= 0 {
		cacheMax = kalshiTradeReconcileCacheDefault
	}
	ctx, cancel := context.WithCancel(context.Background())
	handle := &kalshiTradeReconcileHandle{
		ch: make(chan kalshiTradeReconcileObservation, capacity),
	}
	rt := &kalshiTradeReconcileRuntime{
		ch:       handle.ch,
		handle:   handle,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
		cancel:   cancel,
		cacheMax: cacheMax,
		private:  make(map[string]kalshiPrivateTradeEvidence),
		public:   make(map[string]kalshiPublicTradeEvidence),
		terminal: make(map[string]string),
		conflict: make(map[string]string),
	}
	s.kalshiTradeReconcileRuntime = rt
	s.kalshiTradeReconcileHandle.Store(handle)
	go s.runGuarded("kalshi-trade-reconciliation", func() {
		s.runKalshiTradeReconciler(ctx, rt)
	})
}

// offerKalshiTradeObservation is the one bounded callback admission path. TryRLock never waits:
// during shutdown it refuses immediately, and otherwise it prevents the drain from completing
// until this single nonblocking channel-send attempt has finished.
func (s *Server) offerKalshiTradeObservation(obs kalshiTradeReconcileObservation) bool {
	if s == nil {
		return false
	}
	handle := s.kalshiTradeReconcileHandle.Load()
	if handle == nil {
		return false
	}
	if !handle.admission.TryRLock() {
		s.noteKalshiTradeReconcileStoppingDrop()
		return false
	}
	defer handle.admission.RUnlock()
	if handle.closed {
		s.noteKalshiTradeReconcileStoppingDrop()
		return false
	}
	select {
	case handle.ch <- obs:
		return true
	default:
		s.kalshiTradeReconcileDropped.Add(1)
		s.executionShadowDropped.Add(1)
		s.executionShadowDropCapacity.Add(1)
		return false
	}
}

func (s *Server) noteKalshiTradeReconcileStoppingDrop() {
	s.kalshiTradeReconcileDropped.Add(1)
	s.executionShadowDropped.Add(1)
	s.executionShadowDropStopping.Add(1)
}

// offerKalshiPrivateFill is safe on the authenticated WebSocket reader and REST fill pollers.
// It never touches SQLite, performs a network request, or waits for queue space or a mutex.
func (s *Server) offerKalshiPrivateFill(fill kalshi.Fill) bool {
	return s.offerKalshiTradeObservation(kalshiTradeReconcileObservation{
		kind: kalshiTradePrivateObserved, fill: fill, receivedAt: time.Now().UTC(),
	})
}

// offerKalshiPublicTrade is safe on the public WebSocket reader. Public-only observations remain
// memory-only until an authenticated fill with the same trade_id exists.
func (s *Server) offerKalshiPublicTrade(trade kalshi.Trade) bool {
	return s.offerKalshiTradeObservation(kalshiTradeReconcileObservation{
		kind: kalshiTradePublicObserved, trade: trade, receivedAt: time.Now().UTC(),
	})
}

// getKalshiFillsObserved makes every normal authenticated account-history read a second private
// evidence source. The venue request was already happening off the order path; publishing each
// returned fill is a deduplicated, bounded, nonblocking queue offer after the response arrives.
func (s *Server) getKalshiFillsObserved(ctx context.Context) ([]kalshi.Fill, error) {
	fills, err := s.kal.GetFills(ctx)
	if err == nil {
		s.offerKalshiRESTFills(fills)
	}
	return fills, err
}

func (s *Server) offerKalshiRESTFills(fills []kalshi.Fill) int {
	if s == nil || len(fills) == 0 {
		return 0
	}
	maxSeen := s.kalshiTradeReconcileCacheMax
	if maxSeen <= 0 {
		maxSeen = kalshiTradeReconcileCacheDefault
	}
	accepted := 0
	for _, fill := range fills {
		fingerprint := kalshiRESTFillFingerprint(fill)
		if fingerprint == "" {
			continue
		}
		// Serializing only REST dedupe avoids duplicate publication when dashboard and loss-ledger
		// snapshots overlap. The nested queue offer is nonblocking; a refusal is deliberately not
		// remembered so the next authenticated snapshot can retry it.
		s.kalshiTradeRESTSeenMu.Lock()
		if _, seen := s.kalshiTradeRESTSeen[fingerprint]; seen {
			s.kalshiTradeRESTSeenMu.Unlock()
			continue
		}
		if !s.offerKalshiPrivateFill(fill) {
			s.kalshiTradeRESTSeenMu.Unlock()
			continue
		}
		if s.kalshiTradeRESTSeen == nil {
			s.kalshiTradeRESTSeen = make(map[string]struct{})
		}
		s.kalshiTradeRESTSeen[fingerprint] = struct{}{}
		s.kalshiTradeRESTFIFO = append(s.kalshiTradeRESTFIFO, fingerprint)
		for len(s.kalshiTradeRESTFIFO) > maxSeen {
			oldest := s.kalshiTradeRESTFIFO[0]
			s.kalshiTradeRESTFIFO = s.kalshiTradeRESTFIFO[1:]
			delete(s.kalshiTradeRESTSeen, oldest)
		}
		s.kalshiTradeRESTSeenMu.Unlock()
		accepted++
	}
	return accepted
}

func kalshiRESTFillFingerprint(fill kalshi.Fill) string {
	tradeID := strings.TrimSpace(fill.FillID)
	if tradeID == "" {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s|%.9f|%.9f|%.9f|%.9f|%t|%t|%s",
		tradeID, strings.TrimSpace(fill.OrderID), strings.TrimSpace(fill.Ticker),
		strings.ToUpper(strings.TrimSpace(fill.SideYesNo())),
		strings.ToUpper(strings.TrimSpace(fill.Action)), fill.Qty(), fill.YesPriceUSD(),
		fill.NoPriceUSD(), fill.FeeUSD(), fill.FeeKnown(), fill.IsTaker,
		strings.TrimSpace(fill.CreatedTime))
}

func (s *Server) stopKalshiTradeReconciler(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.kalshiTradeReconcileMu.Lock()
	rt := s.kalshiTradeReconcileRuntime
	if rt == nil {
		s.kalshiTradeReconcileMu.Unlock()
		return nil
	}
	// This atomic removal is the admission boundary. Every callback which loaded the old handle
	// either completes under its read admission or loses a nonblocking race. Closing admission
	// before stopping the worker guarantees that its final drain includes every accepted callback.
	s.kalshiTradeReconcileHandle.Store(nil)
	if rt.handle != nil {
		rt.handle.admission.Lock()
		rt.handle.closed = true
		rt.handle.admission.Unlock()
	}
	rt.stopOnce.Do(func() {
		rt.cancel()
		close(rt.stop)
	})
	s.kalshiTradeReconcileMu.Unlock()

	select {
	case <-rt.done:
		s.kalshiTradeReconcileMu.Lock()
		if s.kalshiTradeReconcileRuntime == rt {
			s.kalshiTradeReconcileRuntime = nil
		}
		s.kalshiTradeReconcileMu.Unlock()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("Kalshi trade reconciliation did not drain: %w", ctx.Err())
	}
}

func (s *Server) runKalshiTradeReconciler(ctx context.Context, rt *kalshiTradeReconcileRuntime) {
	defer close(rt.done)
	pending := s.loadKalshiTradeReconcilePending(ctx, rt)
	if len(pending) > 0 {
		rt.recovery.Add(1)
		go func() {
			defer rt.recovery.Done()
			s.recoverKalshiPublicTrades(ctx, rt, pending)
		}()
	}
	for {
		select {
		case obs := <-rt.ch:
			s.processKalshiTradeObservation(rt, obs)
		case <-rt.stop:
			// Stop the sole asynchronous producer first, then consume every observation already
			// accepted before the admission boundary. The execution-shadow fence runs after this.
			rt.recovery.Wait()
			for {
				select {
				case obs := <-rt.ch:
					s.processKalshiTradeObservation(rt, obs)
				default:
					return
				}
			}
		}
	}
}

func (s *Server) loadKalshiTradeReconcilePending(ctx context.Context,
	rt *kalshiTradeReconcileRuntime) []kalshiPrivateTradeEvidence {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.store.ListLatestKalshiTradeReconciliations(
		readCtx, kalshiTradeReconcileRecoveryMax)
	if err != nil {
		if s.log != nil && ctx.Err() == nil {
			s.log.Warn("Kalshi public/private trade reconciliation recovery read failed", "err", err)
		}
		return nil
	}
	out := make([]kalshiPrivateTradeEvidence, 0, len(rows))
	for _, row := range rows {
		p := kalshiPrivateTradeEvidence{
			tradeID: row.TradeID, orderID: row.PrivateOrderID, ticker: row.PrivateTicker,
			side: row.PrivateSide, action: row.PrivateAction, qty: row.PrivateQty,
			price: row.PrivatePrice, fee: row.PrivateFee, feeKnown: row.PrivateFeeKnown,
			isTaker: row.PrivateIsTaker, sourceAt: row.PrivateSourceAt,
			receivedAt: row.PrivateReceivedAt, reservationID: row.ReservationID,
			attemptID: row.AttemptID,
		}
		if !validKalshiPrivateTradeEvidence(p) {
			continue
		}
		rt.cachePrivate(p)
		if row.PublicPresent {
			pub := kalshiPublicTradeEvidence{
				tradeID: row.TradeID, ticker: row.PublicTicker, qty: row.PublicQty,
				yesPrice: row.PublicYesPrice, noPrice: row.PublicNoPrice,
				takerOutcomeSide: row.PublicTakerOutcomeSide,
				takerBookSide:    row.PublicTakerBookSide, aggressor: row.PublicAggressor,
				isBlock: row.PublicIsBlock, sourceAt: row.PublicSourceAt,
				receivedAt: row.PublicReceivedAt,
			}
			rt.cachePublic(pub)
		}
		if row.Status == "MATCHED" || row.Status == "MISMATCH" {
			rt.cacheTerminal(row.TradeID, row.Status+":"+row.EventID, row.ObservedAt)
		} else {
			out = append(out, p)
		}
	}
	return out
}

func (s *Server) recoverKalshiPublicTrades(ctx context.Context,
	rt *kalshiTradeReconcileRuntime, pending []kalshiPrivateTradeEvidence) {
	lookup := s.kalshiTradePublicLookup
	if lookup == nil && s.kal != nil {
		lookup = s.kal.GetTrades
	}
	if lookup == nil {
		return
	}
	byTicker := make(map[string][]kalshiPrivateTradeEvidence)
	for _, p := range pending {
		byTicker[p.ticker] = append(byTicker[p.ticker], p)
	}
	tickers := make([]string, 0, len(byTicker))
	for ticker := range byTicker {
		tickers = append(tickers, ticker)
	}
	sort.Strings(tickers)
	for _, ticker := range tickers {
		if ctx.Err() != nil {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
		trades, err := lookup(callCtx, ticker, kalshiTradeReconcileLookupLimit)
		cancel()
		if err != nil {
			for _, p := range byTicker[ticker] {
				rt.offerRecovery(kalshiTradeReconcileObservation{
					kind: kalshiTradePublicLookupFailed, tradeID: p.tradeID, ticker: ticker,
					receivedAt: time.Now().UTC(), detail: compactKalshiReconcileError(err),
				})
			}
			continue
		}
		found := make(map[string]kalshi.Trade, len(trades))
		for _, trade := range trades {
			found[strings.TrimSpace(trade.TradeID)] = trade
		}
		for _, p := range byTicker[ticker] {
			if trade, ok := found[p.tradeID]; ok {
				rt.offerRecovery(kalshiTradeReconcileObservation{
					kind: kalshiTradePublicObserved, trade: trade, receivedAt: time.Now().UTC(),
				})
			} else {
				rt.offerRecovery(kalshiTradeReconcileObservation{
					kind: kalshiTradePublicNotFound, tradeID: p.tradeID, ticker: ticker,
					receivedAt: time.Now().UTC(),
				})
			}
		}
	}
}

func (rt *kalshiTradeReconcileRuntime) offerRecovery(obs kalshiTradeReconcileObservation) {
	select {
	case rt.ch <- obs:
	case <-rt.stop:
	default:
		// Recovery evidence is optional refinement of a PENDING row already durable on disk. A
		// full bounded queue never turns "not observed" into a fabricated match.
	}
}

func (s *Server) processKalshiTradeObservation(rt *kalshiTradeReconcileRuntime,
	obs kalshiTradeReconcileObservation) {
	switch obs.kind {
	case kalshiTradePrivateObserved:
		p, ok := s.kalshiPrivateEvidence(obs.fill, obs.receivedAt)
		if !ok {
			return
		}
		if prior, exists := rt.private[p.tradeID]; exists {
			merged, compatible := mergeKalshiPrivateTradeEvidence(prior, p)
			if !compatible {
				rt.conflict[p.tradeID] = "conflicting-private-fill-duplicate"
				pub, publicPresent := rt.public[p.tradeID]
				s.persistKalshiTradeComparison(rt, prior, pub, publicPresent,
					"MISMATCH", rt.conflict[p.tradeID], obs.receivedAt)
				return
			}
			if kalshiPrivateTradeFingerprint(prior) == kalshiPrivateTradeFingerprint(merged) {
				return
			}
			rt.cachePrivate(merged)
			if pub, found := rt.public[p.tradeID]; found {
				status, reason := compareKalshiTradeEvidence(merged, pub)
				if conflict := rt.conflict[p.tradeID]; conflict != "" {
					status, reason = "MISMATCH", conflict
				}
				s.persistKalshiTradeComparison(rt, merged, pub, true, status, reason, obs.receivedAt)
			} else {
				s.persistKalshiTradeComparison(rt, merged, kalshiPublicTradeEvidence{}, false,
					"PENDING", "private-fill-evidence-enriched", obs.receivedAt)
			}
			return
		}
		if lineage, found := s.lookupKalshiTradeLineage(p.orderID); found {
			p.reservationID, p.attemptID = lineage.ReservationID, lineage.AttemptID
		}
		rt.cachePrivate(p)
		if pub, found := rt.public[p.tradeID]; found {
			status, reason := compareKalshiTradeEvidence(p, pub)
			if conflict := rt.conflict[p.tradeID]; conflict != "" {
				status, reason = "MISMATCH", conflict
			}
			s.persistKalshiTradeComparison(rt, p, pub, true, status, reason, obs.receivedAt)
			return
		}
		s.persistKalshiTradeComparison(rt, p, kalshiPublicTradeEvidence{}, false,
			"PENDING", "public-trade-not-observed-yet", obs.receivedAt)

	case kalshiTradePublicObserved:
		pub, ok := kalshiPublicEvidence(obs.trade, obs.receivedAt)
		if !ok {
			return
		}
		if prior, exists := rt.public[pub.tradeID]; exists {
			merged, compatible := mergeKalshiPublicTradeEvidence(prior, pub)
			if !compatible {
				rt.conflict[pub.tradeID] = "conflicting-public-trade-duplicate"
				if p, found := rt.private[pub.tradeID]; found {
					s.persistKalshiTradeComparison(rt, p, prior, true,
						"MISMATCH", rt.conflict[pub.tradeID], obs.receivedAt)
				}
				return
			}
			if kalshiPublicTradeFingerprint(prior) == kalshiPublicTradeFingerprint(merged) {
				return
			}
			rt.cachePublic(merged)
			if p, found := rt.private[pub.tradeID]; found {
				status, reason := compareKalshiTradeEvidence(p, merged)
				if conflict := rt.conflict[pub.tradeID]; conflict != "" {
					status, reason = "MISMATCH", conflict
				}
				s.persistKalshiTradeComparison(rt, p, merged, true, status, reason, obs.receivedAt)
			}
			return
		}
		rt.cachePublic(pub)
		if p, found := rt.private[pub.tradeID]; found {
			status, reason := compareKalshiTradeEvidence(p, pub)
			if conflict := rt.conflict[pub.tradeID]; conflict != "" {
				status, reason = "MISMATCH", conflict
			}
			s.persistKalshiTradeComparison(rt, p, pub, true, status, reason, obs.receivedAt)
		}

	case kalshiTradePublicNotFound, kalshiTradePublicLookupFailed:
		tradeID := strings.TrimSpace(obs.tradeID)
		if strings.HasPrefix(rt.terminal[tradeID], "MATCHED:") ||
			strings.HasPrefix(rt.terminal[tradeID], "MISMATCH:") {
			return
		}
		p, found := rt.private[tradeID]
		if !found {
			return
		}
		reason := "public-rest-lookup-complete-trade-id-not-found"
		if obs.kind == kalshiTradePublicLookupFailed {
			reason = "public-rest-lookup-failed"
			if obs.detail != "" {
				reason += ":" + obs.detail
			}
		}
		s.persistKalshiTradeComparison(rt, p, kalshiPublicTradeEvidence{}, false,
			"PENDING", reason, obs.receivedAt)
	}
}

func (s *Server) kalshiPrivateEvidence(fill kalshi.Fill,
	receivedAt time.Time) (kalshiPrivateTradeEvidence, bool) {
	side := strings.ToUpper(strings.TrimSpace(fill.SideYesNo()))
	price := fill.YesPriceUSD()
	if side == "NO" {
		price = fill.NoPriceUSD()
	}
	p := kalshiPrivateTradeEvidence{
		tradeID: strings.TrimSpace(fill.FillID), orderID: strings.TrimSpace(fill.OrderID),
		ticker: strings.TrimSpace(fill.Ticker), side: side,
		action: strings.ToUpper(strings.TrimSpace(fill.Action)), qty: fill.Qty(), price: price,
		fee: fill.FeeUSD(), feeKnown: fill.FeeKnown(), isTaker: fill.IsTaker,
		sourceAt: parseKalshiReconcileTime(fill.CreatedTime), receivedAt: receivedAt.UTC(),
	}
	if !validKalshiPrivateTradeEvidence(p) {
		if s.log != nil {
			s.log.Warn("Kalshi private fill cannot enter trade-id reconciliation",
				"trade_id_present", p.tradeID != "", "order_id_present", p.orderID != "",
				"ticker", p.ticker, "side", p.side, "qty", p.qty, "price", p.price)
		}
		return p, false
	}
	return p, true
}

func validKalshiPrivateTradeEvidence(p kalshiPrivateTradeEvidence) bool {
	return p.tradeID != "" && p.orderID != "" && p.ticker != "" &&
		(p.side == "YES" || p.side == "NO") &&
		(p.action == "" || p.action == "BUY" || p.action == "SELL") &&
		p.qty > 0 && finiteKalshiReconcile(p.qty) &&
		p.price > 0 && p.price < 1 && finiteKalshiReconcile(p.price) &&
		p.fee >= 0 && finiteKalshiReconcile(p.fee) && !p.receivedAt.IsZero()
}

func kalshiPublicEvidence(trade kalshi.Trade,
	receivedAt time.Time) (kalshiPublicTradeEvidence, bool) {
	yesPrice, noPrice := trade.YesPrice.Float(), trade.NoPrice.Float()
	if yesPrice > 0 && yesPrice < 1 && (noPrice <= 0 || noPrice >= 1) {
		noPrice = 1 - yesPrice
	}
	if noPrice > 0 && noPrice < 1 && (yesPrice <= 0 || yesPrice >= 1) {
		yesPrice = 1 - noPrice
	}
	pub := kalshiPublicTradeEvidence{
		tradeID: strings.TrimSpace(trade.TradeID), ticker: strings.TrimSpace(trade.Ticker),
		qty: trade.Count.Float(), yesPrice: yesPrice, noPrice: noPrice,
		takerOutcomeSide: strings.ToLower(strings.TrimSpace(trade.TakerOutcomeSide)),
		takerBookSide:    strings.ToLower(strings.TrimSpace(trade.TakerBookSide)),
		aggressor:        strings.ToLower(strings.TrimSpace(trade.Aggressor())),
		isBlock:          trade.IsBlock, sourceAt: parseKalshiReconcileTime(trade.CreatedTime),
		receivedAt: receivedAt.UTC(),
	}
	ok := pub.tradeID != "" && pub.ticker != "" && pub.qty > 0 &&
		finiteKalshiReconcile(pub.qty) && pub.yesPrice > 0 && pub.yesPrice < 1 &&
		pub.noPrice > 0 && pub.noPrice < 1 && finiteKalshiReconcile(pub.yesPrice) &&
		finiteKalshiReconcile(pub.noPrice) && !pub.receivedAt.IsZero()
	return pub, ok
}

func (s *Server) lookupKalshiTradeLineage(orderID string) (storage.KalshiTradeLineage, bool) {
	var zero storage.KalshiTradeLineage
	if s == nil || s.store == nil || strings.TrimSpace(orderID) == "" {
		return zero, false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 750*time.Millisecond)
	defer cancel()
	lineage, found, err := s.store.KalshiTradeLineageForOrder(ctx, orderID)
	if err != nil {
		if s.log != nil {
			s.log.Warn("Kalshi fill lineage lookup failed", "order_id", orderID, "err", err)
		}
		return zero, false
	}
	return lineage, found
}

func compareKalshiTradeEvidence(p kalshiPrivateTradeEvidence,
	pub kalshiPublicTradeEvidence) (string, string) {
	var mismatches []string
	if !strings.EqualFold(p.ticker, pub.ticker) {
		mismatches = append(mismatches, "ticker")
	}
	if !kalshiReconcileEqual(p.qty, pub.qty) {
		mismatches = append(mismatches, "quantity")
	}
	publicPrice := pub.yesPrice
	if p.side == "NO" {
		publicPrice = pub.noPrice
	}
	if !kalshiReconcileEqual(p.price, publicPrice) {
		mismatches = append(mismatches, "price")
	}
	directionComparable := p.action != ""
	if directionComparable {
		expected := p.side
		if (p.action == "BUY" && !p.isTaker) || (p.action == "SELL" && p.isTaker) {
			expected = oppositeKalshiReconcileSide(p.side)
		}
		if pub.aggressor == "" {
			mismatches = append(mismatches, "public-aggressor-missing")
		} else if !strings.EqualFold(expected, pub.aggressor) {
			mismatches = append(mismatches, "aggressor")
		}
	}
	if len(mismatches) > 0 {
		return "MISMATCH", "same-trade-id-disagrees:" + strings.Join(mismatches, ",")
	}
	if directionComparable {
		return "MATCHED", "trade-id-ticker-quantity-price-direction-match"
	}
	return "MATCHED", "trade-id-ticker-quantity-price-match;direction-not-comparable"
}

func (s *Server) persistKalshiTradeComparison(rt *kalshiTradeReconcileRuntime,
	p kalshiPrivateTradeEvidence, pub kalshiPublicTradeEvidence, publicPresent bool,
	status, reason string, observedAt time.Time) {
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	event := storage.KalshiTradeReconciliationEvent{
		TradeID: p.tradeID, Status: status, Reason: reason, ObservedAt: observedAt.UTC(),
		ReservationID: p.reservationID, AttemptID: p.attemptID,
		PrivatePresent: true, PrivateOrderID: p.orderID, PrivateTicker: p.ticker,
		PrivateSide: p.side, PrivateAction: p.action, PrivateQty: p.qty,
		PrivatePrice: p.price, PrivateFee: p.fee, PrivateFeeKnown: p.feeKnown,
		PrivateIsTaker: p.isTaker, PrivateSourceAt: p.sourceAt,
		PrivateReceivedAt: p.receivedAt, PublicPresent: publicPresent,
		Evidence: map[string]any{
			"identity_basis": "kalshi_trade_id",
			"public_lookup":  publicPresent,
		},
	}
	if publicPresent {
		event.PublicTicker, event.PublicQty = pub.ticker, pub.qty
		event.PublicYesPrice, event.PublicNoPrice = pub.yesPrice, pub.noPrice
		event.PublicTakerOutcomeSide = pub.takerOutcomeSide
		event.PublicTakerBookSide, event.PublicAggressor = pub.takerBookSide, pub.aggressor
		event.PublicIsBlock, event.PublicSourceAt = pub.isBlock, pub.sourceAt
		event.PublicReceivedAt = pub.receivedAt
	}
	event.EventID = kalshiTradeReconcileEventID(event)
	fingerprint := status + ":" + event.EventID
	// Contradictory evidence is terminal and conservative. A later optional-field enrichment may
	// add useful facts, but it cannot erase a previously observed mismatch in this runtime.
	if status == "MATCHED" && strings.HasPrefix(rt.terminal[p.tradeID], "MISMATCH:") {
		return
	}
	if rt.terminal[p.tradeID] == fingerprint {
		return
	}
	if s.enqueueExecutionShadowWrite(executionShadowWrite{kalshiTrade: &event}) {
		if status == "MATCHED" || status == "MISMATCH" {
			rt.cacheTerminal(p.tradeID, fingerprint, observedAt)
		}
		return
	}
	if s.log != nil {
		s.log.Warn("Kalshi trade reconciliation evidence queue refused",
			"trade_id", p.tradeID, "status", status, "reason", reason)
	}
}

func kalshiTradeReconcileEventID(event storage.KalshiTradeReconciliationEvent) string {
	parts := []string{
		event.TradeID, event.Status, event.Reason,
		event.ObservedAt.UTC().Format(time.RFC3339Nano),
		event.ReservationID, event.AttemptID, event.PrivateOrderID, event.PrivateTicker,
		event.PrivateSide, event.PrivateAction,
		fmt.Sprintf("%.9f|%.9f|%.9f|%t|%t", event.PrivateQty, event.PrivatePrice,
			event.PrivateFee, event.PrivateFeeKnown, event.PrivateIsTaker),
		event.PrivateSourceAt.UTC().Format(time.RFC3339Nano),
		event.PrivateReceivedAt.UTC().Format(time.RFC3339Nano),
		fmt.Sprintf("%t|%s|%.9f|%.9f|%.9f|%s|%s|%s|%t", event.PublicPresent,
			event.PublicTicker, event.PublicQty, event.PublicYesPrice, event.PublicNoPrice,
			event.PublicTakerOutcomeSide, event.PublicTakerBookSide, event.PublicAggressor,
			event.PublicIsBlock),
		event.PublicSourceAt.UTC().Format(time.RFC3339Nano),
		event.PublicReceivedAt.UTC().Format(time.RFC3339Nano),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "ktr-" + hex.EncodeToString(sum[:16])
}

// Private WS and REST schemas overlap imperfectly. A later row may add fee/action/source time
// without changing the economic fill; only contradictory facts are a duplicate mismatch.
func mergeKalshiPrivateTradeEvidence(a, b kalshiPrivateTradeEvidence) (
	kalshiPrivateTradeEvidence, bool) {
	if a.tradeID != b.tradeID || a.orderID != b.orderID ||
		!strings.EqualFold(a.ticker, b.ticker) || a.side != b.side ||
		!kalshiReconcileEqual(a.qty, b.qty) || !kalshiReconcileEqual(a.price, b.price) ||
		a.isTaker != b.isTaker ||
		(a.action != "" && b.action != "" && a.action != b.action) ||
		(a.feeKnown && b.feeKnown && !kalshiReconcileEqual(a.fee, b.fee)) ||
		(!a.sourceAt.IsZero() && !b.sourceAt.IsZero() && !a.sourceAt.Equal(b.sourceAt)) {
		return a, false
	}
	out := a
	if out.action == "" {
		out.action = b.action
	}
	if !out.feeKnown && b.feeKnown {
		out.fee, out.feeKnown = b.fee, true
	}
	if out.sourceAt.IsZero() {
		out.sourceAt = b.sourceAt
	}
	if out.receivedAt.IsZero() || (!b.receivedAt.IsZero() && b.receivedAt.Before(out.receivedAt)) {
		out.receivedAt = b.receivedAt
	}
	if out.reservationID == "" {
		out.reservationID = b.reservationID
	}
	if out.attemptID == "" {
		out.attemptID = b.attemptID
	}
	return out, true
}

// Direction field spellings are transport detail. REST may supply only taker_outcome_side while
// WS also carries taker_book_side; equal normalized ticker/qty/prices/aggressor is one trade.
func mergeKalshiPublicTradeEvidence(a, b kalshiPublicTradeEvidence) (
	kalshiPublicTradeEvidence, bool) {
	if a.tradeID != b.tradeID || !strings.EqualFold(a.ticker, b.ticker) ||
		!kalshiReconcileEqual(a.qty, b.qty) ||
		!kalshiReconcileEqual(a.yesPrice, b.yesPrice) ||
		!kalshiReconcileEqual(a.noPrice, b.noPrice) ||
		(a.aggressor != "" && b.aggressor != "" && a.aggressor != b.aggressor) ||
		(!a.sourceAt.IsZero() && !b.sourceAt.IsZero() && !a.sourceAt.Equal(b.sourceAt)) {
		return a, false
	}
	out := a
	if out.takerOutcomeSide == "" {
		out.takerOutcomeSide = b.takerOutcomeSide
	}
	if out.takerBookSide == "" {
		out.takerBookSide = b.takerBookSide
	}
	if out.aggressor == "" {
		out.aggressor = b.aggressor
	}
	out.isBlock = out.isBlock || b.isBlock
	if out.sourceAt.IsZero() {
		out.sourceAt = b.sourceAt
	}
	if out.receivedAt.IsZero() || (!b.receivedAt.IsZero() && b.receivedAt.Before(out.receivedAt)) {
		out.receivedAt = b.receivedAt
	}
	return out, true
}

func kalshiPrivateTradeFingerprint(p kalshiPrivateTradeEvidence) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%.9f|%.9f|%.9f|%t|%t|%s",
		p.tradeID, p.orderID, p.ticker, p.side, p.action, p.qty, p.price, p.fee,
		p.feeKnown, p.isTaker, p.sourceAt.UTC().Format(time.RFC3339Nano))
}

func kalshiPublicTradeFingerprint(p kalshiPublicTradeEvidence) string {
	return fmt.Sprintf("%s|%s|%.9f|%.9f|%.9f|%s|%s|%s|%t|%s",
		p.tradeID, p.ticker, p.qty, p.yesPrice, p.noPrice, p.takerOutcomeSide,
		p.takerBookSide, p.aggressor, p.isBlock, p.sourceAt.UTC().Format(time.RFC3339Nano))
}

func (rt *kalshiTradeReconcileRuntime) cachePrivate(p kalshiPrivateTradeEvidence) {
	rt.private[p.tradeID] = p
	rt.privFIFO = append(rt.privFIFO, kalshiTradeCacheRef{p.tradeID, p.receivedAt})
	rt.prunePrivate()
}

func (rt *kalshiTradeReconcileRuntime) cachePublic(p kalshiPublicTradeEvidence) {
	rt.public[p.tradeID] = p
	rt.pubFIFO = append(rt.pubFIFO, kalshiTradeCacheRef{p.tradeID, p.receivedAt})
	rt.prunePublic()
}

func (rt *kalshiTradeReconcileRuntime) cacheTerminal(tradeID, fingerprint string, at time.Time) {
	rt.terminal[tradeID] = fingerprint
	rt.termFIFO = append(rt.termFIFO, kalshiTradeCacheRef{tradeID, at})
	for len(rt.terminal) > rt.cacheMax && len(rt.termFIFO) > 0 {
		ref := rt.termFIFO[0]
		rt.termFIFO = rt.termFIFO[1:]
		if _, found := rt.terminal[ref.tradeID]; found {
			delete(rt.terminal, ref.tradeID)
		}
	}
}

func (rt *kalshiTradeReconcileRuntime) prunePrivate() {
	for len(rt.private) > rt.cacheMax && len(rt.privFIFO) > 0 {
		ref := rt.privFIFO[0]
		rt.privFIFO = rt.privFIFO[1:]
		if current, found := rt.private[ref.tradeID]; found &&
			current.receivedAt.Equal(ref.stamp) {
			delete(rt.private, ref.tradeID)
			delete(rt.conflict, ref.tradeID)
		}
	}
}

func (rt *kalshiTradeReconcileRuntime) prunePublic() {
	for len(rt.public) > rt.cacheMax && len(rt.pubFIFO) > 0 {
		ref := rt.pubFIFO[0]
		rt.pubFIFO = rt.pubFIFO[1:]
		if current, found := rt.public[ref.tradeID]; found &&
			current.receivedAt.Equal(ref.stamp) {
			delete(rt.public, ref.tradeID)
			delete(rt.conflict, ref.tradeID)
		}
	}
}

func finiteKalshiReconcile(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func kalshiReconcileEqual(a, b float64) bool {
	return math.Abs(a-b) <= 0.0000005
}

func oppositeKalshiReconcileSide(side string) string {
	if strings.EqualFold(side, "YES") {
		return "NO"
	}
	return "YES"
}

func parseKalshiReconcileTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func compactKalshiReconcileError(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	text = strings.Join(strings.Fields(text), "-")
	if len(text) > 96 {
		text = text[:96]
	}
	return text
}
