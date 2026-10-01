package kalshi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// tickerPx is one market's last live YES mid-price from the ticker WebSocket, plus a rolling ~3-min
// window (first/hi/lo) for live volatility — momentum/volatility computed off the WS, no REST candles.
type tickerPx struct {
	yes float64
	// bid/ask are the actual YES top of book carried by the all-market ticker
	// channel.  Keeping them separate matters: the midpoint is useful as a mark,
	// but it is not a price at which a strategy can buy or post.
	bid, ask      float64
	at            time.Time // venue source time for WS; local seed time for REST
	sourceAt      time.Time // venue ticker timestamp; zero for REST seed or a missing clock
	ws            bool      // true only for a decoded ticker WebSocket frame
	winStart      time.Time
	first, hi, lo float64
}

// tickerExecutionFresh is deliberately stricter than a generic "recent map write" check. A WS
// sample must still own a current venue timestamp at read time; otherwise a buffered frame could
// gain a second freshness lease merely because it arrived now. REST cold-start seeds have no
// venue clock, so their explicitly local seed time is the only bounded fallback.
func tickerExecutionFresh(s tickerPx, now time.Time, maxAge time.Duration) bool {
	arrivalOrSeedAge := now.Sub(s.at)
	if s.at.IsZero() || arrivalOrSeedAge < -5*time.Second || arrivalOrSeedAge > maxAge {
		return false
	}
	if !s.ws {
		return true
	}
	sourceAge := now.Sub(s.sourceAt)
	return !s.sourceAt.IsZero() && sourceAge >= -5*time.Second && sourceAge <= maxAge
}

// WSURLFor returns the Trade-API WebSocket base URL for an environment. The signed path is always
// /trade-api/ws/v2 regardless of host.
func WSURLFor(env string) string {
	if strings.EqualFold(env, "prod") {
		return "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
	}
	return "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
}

// StartTickerStream keeps TWO authenticated WebSockets open (R41 WARM-STANDBY, operator: "latency
// will bite us"): both fully subscribed to the same channels, but only the PRIMARY ingests — the
// standby drains (discards) while staying connected + subscribed server-side. When the primary
// dies, the standby is PROMOTED by flipping a role flag on its own read loop (no connection
// handoff), so the feed gap on a drop is ~zero instead of the 1–2s redial+resubscribe blindness.
// A replacement standby dials in the background. NO-OP when the client has no signer.
func (c *Client) StartTickerStream(ctx context.Context, wsURL string) {
	if c.signer == nil {
		return // public-only client: the WS handshake requires auth, so there's nothing to do
	}
	var mu sync.Mutex
	var cur, next *wsSession
	stopping := false
	var spawn func(asPrimary bool)
	onDead := func(s *wsSession) {
		mu.Lock()
		defer mu.Unlock()
		if stopping {
			return // cancellation detached both slots; a dying read loop must never refill either one
		}
		switch s {
		case cur:
			if next != nil { // PROMOTE the warm standby — zero-blindness failover
				cur = next
				next = nil
				cur.primary.Store(true)
				// R74 §4: books stream on the PRIMARY only — the promoted socket has no book sub and
				// the dead one's seq chain is gone, so invalidate everything and resubscribe the
				// working set here (fresh snapshots re-baseline; correctness over continuity).
				go c.bookPrimaryChanged(cur)
				go spawn(false) // refill the standby slot
			} else {
				cur = nil
				go spawn(true) // both down → dial a fresh primary (with backoff)
			}
		case next:
			next = nil
			go spawn(false)
		}
	}
	spawn = func(asPrimary bool) {
		// EXPONENTIAL BACKOFF + JITTER (audit §6) per slot; healthy dials reset naturally because
		// each spawn starts fresh at 2s.
		backoff := 2 * time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			s, err := c.dialTickerSession(ctx, wsURL)
			if err == nil {
				mu.Lock()
				replaced := false
				if stopping || ctx.Err() != nil {
					// Cancellation can win after DialContext connects but before this socket owns a
					// role. Never publish that late session; close it below as an orphan.
				} else if asPrimary && cur == nil {
					cur = s
					s.primary.Store(true)
					replaced = true
				} else if !asPrimary && next == nil {
					next = s
					replaced = true
				} else if asPrimary && cur != nil { // someone else recovered first → offer as standby
					if next == nil {
						next = s
						replaced = true
					}
				}
				mu.Unlock()
				if !replaced {
					_ = s.conn.Close() // slot already filled — drop the extra socket
					return
				}
				go c.runTickerSession(ctx, s, onDead)
				if s.primary.Load() {
					c.bookPrimaryChanged(s) // R74: fresh primary (boot / both-down recovery) carries the book subscription
				}
				return
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			jitter := time.Duration(time.Now().UnixNano() % int64(backoff/2))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff/2 + jitter):
			}
		}
	}
	// R108 staleness watchdog hook: closing the primary errors its read loop → onDead promotes
	// the standby (bookPrimaryChanged re-baselines the book subs) → spawn refills. The existing
	// failure path, triggered deliberately when data goes stale while pongs keep flowing.
	c.kickMu.Lock()
	c.kickFn = func() bool {
		mu.Lock()
		defer mu.Unlock()
		if stopping {
			return false
		}
		s := cur
		if s == nil || s.conn == nil {
			return false
		}
		_ = s.conn.Close()
		return true
	}
	c.kickMu.Unlock()
	spawn(true)     // primary (synchronous first dial keeps boot ordering)
	go spawn(false) // warm standby
	<-ctx.Done()
	// Cancellation must tear down the sockets immediately. Previously the supervisor returned but
	// both read loops could remain blocked until their 60s deadlines, leaking a stale authenticated
	// session across fast stop/restart cycles.
	mu.Lock()
	stopping = true
	oldCur, oldNext := cur, next
	cur, next = nil, nil
	mu.Unlock()
	for _, session := range []*wsSession{oldCur, oldNext} {
		if session != nil && session.conn != nil {
			_ = session.conn.Close()
		}
	}
	c.kickMu.Lock()
	c.kickFn = nil
	c.kickMu.Unlock()
}

// wsSession is one live socket + its role. primary=true → its read loop ingests; false → drains.
type wsSession struct {
	conn       *websocket.Conn
	primary    atomic.Bool
	generation uint64 // unique source domain; channel sequences may restart on a new socket
	// frameAt is application liveness for this multiplexed socket. Order books are event streams:
	// a quiet market emits no periodic deltas, so an unchanged valid snapshot remains authoritative
	// while the owning socket is receiving other channel frames.
	frameAt atomic.Int64
	wmu     sync.Mutex // R74: serializes post-dial WriteJSON (book subscribe/update commands); gorilla allows ONE writer
}

// writeJSON sends one JSON command on the socket (R74 book subscriptions). The dial-time subscribe
// happens before the session is shared, and WriteControl pings are concurrency-safe by contract, so
// this mutex only has to serialize the book manager's commands against each other.
func (s *wsSession) writeJSON(v any) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.conn == nil {
		return errors.New("ws session has no connection")
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(15 * time.Second)) // a wedged TCP write must error, not strand the book manager
	return s.conn.WriteJSON(v)
}

// dialTickerSession dials + authenticates + subscribes one socket (shared by primary and standby).
func (c *Client) dialTickerSession(ctx context.Context, wsURL string) (*wsSession, error) {
	headers, err := c.signer.Headers("GET", "/trade-api/ws/v2") // same RSA-PSS signing as REST
	if err != nil {
		return nil, err
	}
	h := http.Header{}
	for k, v := range headers {
		h.Set(k, v)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, wsURL, h)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close() // R102 (auditor bug 248, P4): handshake-failure response body never closed — fd leak per failed dial
		}
		return nil, err
	}
	// Subscribe to the ticker channel for ALL markets (omit market_tickers => everything), plus
	// market_lifecycle_v2 (audit §8/§9: Kalshi PUSHES authoritative settlement values), the
	// trade channel (audit §4/#12: the live tape over WS replaces the GetRecentTrades REST poll),
	// multivariate_market_lifecycle (R18: KXMVE* combos push here since Mar 2026), and
	// communications (R18 RFQ-pulse probe: venue-wide rfq_created/rfq_deleted broadcasts).
	sub := map[string]any{"id": 1, "cmd": "subscribe", "params": map[string]any{"channels": []string{
		"ticker", "market_lifecycle_v2", "trade", "multivariate_market_lifecycle", "communications",
		// R132: authenticated account pushes. They remove the 45s fill-label delay and
		// invalidate account views immediately; paginated REST remains reconnect truth.
		"user_orders", "fill", "market_positions",
	}}}
	if err := conn.WriteJSON(sub); err != nil {
		_ = conn.Close()
		return nil, err
	}
	conn.SetReadLimit(1 << 21) // 2MB frame cap (audit §6: no read limit = an OOM lever)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	return &wsSession{conn: conn, generation: c.wsSessionGeneration.Add(1)}, nil
}

// runTickerSession reads one socket forever. Ingests only while it holds the PRIMARY role — a
// standby discards frames (staying subscribed) until promoted by the supervisor.
func (c *Client) runTickerSession(ctx context.Context, s *wsSession, onDead func(*wsSession)) {
	defer s.conn.Close()
	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-pingDone:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					return
				}
			}
		}
	}()
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				onDead(s)
			}
			return
		}
		s.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		s.frameAt.Store(time.Now().UnixNano())
		if !s.primary.Load() {
			continue // warm standby: connected + subscribed, but silent
		}
		c.ingestBook(data, s)
		c.ingestTicker(data)
		c.ingestLifecycleSession(data, s.generation)
		c.ingestTradeSession(data, s.generation)
		c.ingestRFQPulse(data)
		c.ingestPrivateAccount(data)
	}
}

// SetPrivateAccountHandlers wires the authenticated order/fill/position push channels. onFill and
// onEvent run from the WS read loop and therefore must return quickly (the server hands work off).
func (c *Client) SetPrivateAccountHandlers(onFill func(Fill), onEvent func()) {
	c.txMu.Lock()
	c.userFillFn, c.userEventFn = onFill, onEvent
	c.txMu.Unlock()
}

// SetPublicTradeHandler wires normalized public trade prints to a bounded asynchronous consumer.
// The callback runs on the WS read loop and therefore must only perform a nonblocking enqueue.
func (c *Client) SetPublicTradeHandler(fn func(Trade)) {
	c.txMu.Lock()
	c.publicTradeFn = fn
	c.txMu.Unlock()
}

// ingestPrivateAccount decodes only account-scoped push events. User-order and position frames are
// change notifications (REST supplies the complete paginated snapshot); fill frames carry enough
// canonical fixed-point truth to label execution immediately.
func (c *Client) ingestPrivateAccount(data []byte) {
	var env struct {
		Type string `json:"type"`
		Msg  struct {
			TradeID       string            `json:"trade_id"`
			OrderID       string            `json:"order_id"`
			Ticker        string            `json:"market_ticker"`
			TickerLegacy  string            `json:"ticker"`
			OutcomeSide   string            `json:"outcome_side"`
			BookSide      string            `json:"book_side"`
			PurchasedSide string            `json:"purchased_side"`
			Side          string            `json:"side"`
			Action        string            `json:"action"`
			YesPrice      flexFloat         `json:"yes_price_dollars"`
			Count         flexFloat         `json:"count_fp"`
			Fee           optionalFlexFloat `json:"fee_cost"`
			IsTaker       bool              `json:"is_taker"`
			TS            int64             `json:"ts"`
			TSMS          int64             `json:"ts_ms"`
		} `json:"msg"`
	}
	if json.Unmarshal(data, &env) != nil {
		return
	}
	if env.Type != "fill" && env.Type != "user_order" && env.Type != "market_position" {
		return
	}
	c.txMu.Lock()
	fillFn, eventFn := c.userFillFn, c.userEventFn
	c.txMu.Unlock()
	if env.Type == "fill" && fillFn != nil {
		ticker := env.Msg.Ticker
		if ticker == "" {
			ticker = env.Msg.TickerLegacy
		}
		outcome := strings.ToLower(strings.TrimSpace(env.Msg.OutcomeSide))
		if outcome == "" {
			outcome = strings.ToLower(strings.TrimSpace(env.Msg.PurchasedSide))
		}
		when := time.Now()
		if env.Msg.TSMS > 0 {
			when = time.UnixMilli(env.Msg.TSMS)
		} else if env.Msg.TS > 0 {
			when = time.Unix(env.Msg.TS, 0)
		}
		yp := env.Msg.YesPrice.Float()
		fillFn(Fill{
			FillID: env.Msg.TradeID, OrderID: env.Msg.OrderID, Ticker: ticker,
			Side: env.Msg.Side, Action: env.Msg.Action, OutcomeSide: outcome,
			BookSide: env.Msg.BookSide, CountFP: env.Msg.Count, YesPriceD: env.Msg.YesPrice,
			NoPriceD: flexFloat(1 - yp), FeeCost: env.Msg.Fee, IsTaker: env.Msg.IsTaker,
			CreatedTime: when.UTC().Format(time.RFC3339Nano),
		})
	}
	if eventFn != nil {
		eventFn()
	}
}

// RFQPulseEvent is one venue-wide RFQ lifecycle broadcast (rfq_created / rfq_deleted) — the raw
// material for "is anyone's RFQ flow getting handled, and at what size?" (R18 probe).
type RFQPulseEvent struct {
	Type       string // created | deleted
	RFQID      string
	Ticker     string
	Collection string  // mve_collection_ticker when it's a combo RFQ
	TargetUSD  float64 // target_cost_dollars (0 when contracts-denominated)
	Contracts  float64
	At         time.Time
}

// SetRFQPulseHandler wires the venue-wide RFQ broadcast into the server's pulse tracker.
func (c *Client) SetRFQPulseHandler(fn func(RFQPulseEvent)) { c.rfqPulseFn = fn }

// ingestRFQPulse parses communications-channel broadcasts (schema verified against the AsyncAPI
// 2026-07-02: rfq_created/rfq_deleted always sent to every subscriber; quote events only to
// involved parties — so this measures VENUE RFQ traffic, not quotes).
func (c *Client) ingestRFQPulse(data []byte) {
	fn := c.rfqPulseFn
	if fn == nil {
		return
	}
	var env struct {
		Type string `json:"type"`
		Msg  struct {
			ID            string    `json:"id"`
			MarketTicker  string    `json:"market_ticker"`
			MVECollection string    `json:"mve_collection_ticker"`
			TargetCost    flexFloat `json:"target_cost_dollars"`
			Contracts     flexFloat `json:"contracts_fp"`
		} `json:"msg"`
	}
	if json.Unmarshal(data, &env) != nil {
		return
	}
	if env.Type != "rfq_created" && env.Type != "rfq_deleted" {
		return
	}
	fn(RFQPulseEvent{
		Type: strings.TrimPrefix(env.Type, "rfq_"), RFQID: env.Msg.ID,
		Ticker: env.Msg.MarketTicker, Collection: env.Msg.MVECollection,
		TargetUSD: env.Msg.TargetCost.Float(), Contracts: env.Msg.Contracts.Float(),
		At: time.Now(),
	})
}

// ingestTrade parses one trade-channel message into the rolling live tape (audit §4/#12).
// Field names mirror the REST /markets/trades shape; integer-cent fallbacks included.
func (c *Client) ingestTrade(data []byte) {
	c.ingestTradeSession(data, 0)
}

func (c *Client) ingestTradeSession(data []byte, generation uint64) {
	var env struct {
		Type string `json:"type"`
		SID  int64  `json:"sid"`
		Seq  int64  `json:"seq"`
		Msg  struct {
			TradeID          string    `json:"trade_id"`
			MarketTicker     string    `json:"market_ticker"`
			Count            flexFloat `json:"count_fp"`
			CountInt         float64   `json:"count"`
			YesPrice         flexFloat `json:"yes_price_dollars"`
			NoPrice          flexFloat `json:"no_price_dollars"`
			YesPriceC        float64   `json:"yes_price"`
			NoPriceC         float64   `json:"no_price"`
			TakerSide        string    `json:"taker_side"`         // deprecated
			TakerOutcomeSide string    `json:"taker_outcome_side"` // canonical public-trade field
			TakerBookSide    string    `json:"taker_book_side"`    // canonical equivalent: bid=yes, ask=no
			OutcomeSide      string    `json:"outcome_side"`       // defensive old-capture fallback
			IsBlock          bool      `json:"is_block_trade"`
			TS               int64     `json:"ts"`
			TSMS             int64     `json:"ts_ms"`
		} `json:"msg"`
	}
	if json.Unmarshal(data, &env) != nil || env.Type != "trade" || env.Msg.MarketTicker == "" {
		return
	}
	t := Trade{
		Ticker: env.Msg.MarketTicker, TradeID: env.Msg.TradeID,
		Count: env.Msg.Count, YesPrice: env.Msg.YesPrice, NoPrice: env.Msg.NoPrice,
		TakerSide: env.Msg.TakerSide, TakerOutcomeSide: env.Msg.TakerOutcomeSide,
		TakerBookSide: env.Msg.TakerBookSide, OutcomeSide: env.Msg.OutcomeSide, IsBlock: env.Msg.IsBlock,
	}
	t.TakerSide = t.Aggressor() // compatibility for consumers that still read the legacy field directly
	if t.Count.Float() == 0 && env.Msg.CountInt > 0 {
		t.Count = flexFloat(env.Msg.CountInt)
	}
	if t.YesPrice.Float() == 0 && env.Msg.YesPriceC > 0 {
		t.YesPrice = flexFloat(env.Msg.YesPriceC / 100)
	}
	if t.NoPrice.Float() == 0 && env.Msg.NoPriceC > 0 {
		t.NoPrice = flexFloat(env.Msg.NoPriceC / 100)
	}
	observed := time.Now().UTC()
	when := observed
	var sourceAt time.Time
	if env.Msg.TSMS > 0 {
		sourceAt = time.UnixMilli(env.Msg.TSMS).UTC()
		when = sourceAt
	} else if env.Msg.TS > 0 {
		sourceAt = time.Unix(env.Msg.TS, 0).UTC()
		when = sourceAt
	}
	t.CreatedTime = when.UTC().Format(time.RFC3339Nano)
	// The raw-flow observer keeps every valid positive print, including sub-dollar trades. Invalid
	// zero/negative/NaN rows are rejected here so downstream relative-size baselines cannot be
	// poisoned. There is deliberately no dollar-notional threshold on this tape.
	side := t.Aggressor()
	sidePx := t.YesPrice.Float()
	if side == "no" {
		sidePx = t.NoPrice.Float()
	}
	if (side != "yes" && side != "no") || t.Count.Float() <= 0 || math.IsNaN(t.Count.Float()) ||
		math.IsInf(t.Count.Float(), 0) || sidePx <= 0 || sidePx > 1 || math.IsNaN(sidePx) || math.IsInf(sidePx, 0) {
		return
	}
	c.txMu.Lock()
	c.tape = append(c.tape, t)
	if len(c.tape) > 4000 {
		c.tape = c.tape[len(c.tape)-4000:]
	}
	c.tapeAt = time.Now()
	publicTradeFn := c.publicTradeFn
	c.txMu.Unlock()
	if publicTradeFn != nil {
		publicTradeFn(t)
	}
	sequenceDomain := fmt.Sprintf("trade:%d:%d", generation, env.SID)
	current, prior, gap := c.researchReplaySequence(sequenceDomain, env.Seq)
	c.emitResearchReplay(ResearchReplayEvent{Kind: "trade", EntityID: t.Ticker,
		FrameType: "trade", Channel: "trade", SubscriptionID: env.SID, SourceGeneration: generation,
		ObservedAt: observed, SourceAt: sourceAt, SourceSequence: current,
		PriorSourceSequence: prior, SequenceGap: gap, Trade: &ResearchReplayTrade{
			TradeID: t.TradeID, Aggressor: side, TakerOutcomeSide: t.TakerOutcomeSide,
			TakerBookSide: t.TakerBookSide, LegacyTakerSide: env.Msg.TakerSide,
			LegacyOutcomeSide: env.Msg.OutcomeSide, Count: t.Count.Float(),
			YesPrice: t.YesPrice.Float(), NoPrice: t.NoPrice.Float(), Block: t.IsBlock,
		}})
}

// LiveTape returns the WS trade tape NEWEST-FIRST (matching GetRecentTrades' order) and whether it
// is fresh enough to replace the REST pull. Empty/stale ⇒ callers fall back to REST.
func (c *Client) LiveTape() ([]Trade, bool) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if len(c.tape) == 0 || time.Since(c.tapeAt) > 2*time.Minute {
		return nil, false
	}
	out := make([]Trade, len(c.tape))
	for i, t := range c.tape {
		out[len(c.tape)-1-i] = t // stored oldest→newest; served newest-first
	}
	return out, true
}

// SetLifecycleHandler registers the callback for PUSHED settlements (market_lifecycle_v2).
// yesVal is the authoritative settled YES value in [0,1]. Called from the WS read loop.
func (c *Client) SetLifecycleHandler(fn func(ticker string, yesVal float64)) {
	c.txMu.Lock()
	c.lifecycleFn = fn
	c.txMu.Unlock()
}

// SetListingHandler registers the callback for market-lifecycle `created` events (R27 freshlist).
// isNew is true only when the message carried an open_ts inside the last hour; the server still
// verifies novelty via GET /markets/{ticker} before treating it as fresh.
func (c *Client) SetListingHandler(fn func(ticker string, isNew bool)) {
	c.txMu.Lock()
	c.listingFn = fn
	c.txMu.Unlock()
}

// SetStructureHandler registers the R124 D6 price-structure event handler. The lifecycle channel's
// outer type is always market_lifecycle_v2; the actual kind is msg.event_type.
func (c *Client) SetStructureHandler(fn func(evType string, raw []byte)) {
	c.txMu.Lock()
	c.structFn = fn
	c.txMu.Unlock()
}

// SetPriceStructureHandler registers a parsed price-grid update callback. The client applies the
// update to its own immutable market snapshot before invoking this hook, so downstream caches can
// patch the exact same receipt without reparsing an evolving venue payload. The callback runs on
// the WebSocket reader and must return immediately.
func (c *Client) SetPriceStructureHandler(fn func(PriceStructureUpdate)) {
	c.txMu.Lock()
	c.structureUpdateFn = fn
	c.txMu.Unlock()
}

func decodeStructureFloat(raw json.RawMessage) (float64, bool) {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return 0, false
	}
	if strings.HasPrefix(s, "{") {
		var obj struct {
			Value json.RawMessage `json:"value"`
		}
		if json.Unmarshal(raw, &obj) != nil || len(obj.Value) == 0 {
			return 0, false
		}
		return decodeStructureFloat(obj.Value)
	}
	if strings.HasPrefix(s, `"`) {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return 0, false
		}
		s = strings.TrimSpace(text)
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v > 1 {
		return 0, false
	}
	return v, true
}

// MarketLifecycleEvent is the non-settlement transition slice used by the prospective
// lifecycle-reopen cohort. Raw is copied before the callback and must be treated read-only.
type MarketLifecycleEvent struct {
	Ticker    string
	EventType string
	CloseTime string
	Observed  time.Time
	Raw       []byte
}

// SetMarketLifecycleHandler registers a fast callback for activated, deactivated and
// close_date_updated frames. The callback runs on the WS reader and therefore must enqueue or
// return immediately; it is informational and cannot alter normal settlement/listing handling.
func (c *Client) SetMarketLifecycleHandler(fn func(MarketLifecycleEvent)) {
	c.txMu.Lock()
	c.marketLifecycleFn = fn
	c.txMu.Unlock()
}

// ingestLifecycle parses a market_lifecycle_v2 message and, on a SETTLEMENT, hands the
// authoritative settled YES value to the registered handler (audit §8: this replaces much of the
// polling settlement guesswork with the venue's own push). Defensive parsing: the settled result
// may arrive as settled_result yes/no or as a settlement_value dollar string.
func (c *Client) ingestLifecycle(data []byte) {
	c.ingestLifecycleSession(data, 0)
}

func (c *Client) ingestLifecycleSession(data []byte, generation uint64) {
	var env struct {
		Type string `json:"type"`
		SID  int64  `json:"sid"`
		Seq  int64  `json:"seq"`
		Msg  struct {
			EventType             string          `json:"event_type"`
			MarketTicker          string          `json:"market_ticker"`
			PriceLevelStructure   string          `json:"price_level_structure"`
			PriceRanges           json.RawMessage `json:"price_ranges"`
			TickSize              json.RawMessage `json:"tick_size"`
			TickSizeDollars       json.RawMessage `json:"tick_size_dollars"`
			SettledResult         string          `json:"settled_result"`           // legacy/defensive
			SettlementValue       string          `json:"settlement_value"`         // canonical lifecycle value
			SettlementValueDollar string          `json:"settlement_value_dollars"` // legacy/defensive
			Result                string          `json:"result"`
			OpenTS                int64           `json:"open_ts"` // created events
			CloseTime             string          `json:"close_time"`
			CloseTS               int64           `json:"close_ts"`
			TS                    int64           `json:"ts"`
			TSMS                  int64           `json:"ts_ms"`
		} `json:"msg"`
	}
	// R18: KXMVE* combo markets were EXCLUDED from market_lifecycle_v2 in Mar 2026 and now push on
	// the multivariate_market_lifecycle channel (same schema) — accept both so combo settlements
	// resolve our signals too.
	if json.Unmarshal(data, &env) != nil {
		return
	}
	if (env.Type != "market_lifecycle_v2" && env.Type != "multivariate_market_lifecycle") || env.Msg.MarketTicker == "" {
		return
	}
	eventType := strings.ToLower(strings.TrimSpace(env.Msg.EventType))
	var lifecycleSourceAt time.Time
	if env.Msg.TSMS > 0 {
		lifecycleSourceAt = time.UnixMilli(env.Msg.TSMS).UTC()
	} else if env.Msg.TS > 0 {
		lifecycleSourceAt = time.Unix(env.Msg.TS, 0).UTC()
	}
	receivedAt := time.Now().UTC()
	closeTime := strings.TrimSpace(env.Msg.CloseTime)
	if closeTime == "" && env.Msg.CloseTS > 0 {
		closeTime = time.Unix(env.Msg.CloseTS, 0).UTC().Format(time.RFC3339Nano)
	}
	settled := ""
	for _, value := range []string{env.Msg.SettledResult, env.Msg.SettlementValue,
		env.Msg.SettlementValueDollar, env.Msg.Result} {
		if strings.TrimSpace(value) != "" {
			settled = strings.TrimSpace(value)
			break
		}
	}
	sequenceDomain := fmt.Sprintf("%s:%d:%d", env.Type, generation, env.SID)
	current, prior, gap := c.researchReplaySequence(sequenceDomain, env.Seq)
	c.emitResearchReplay(ResearchReplayEvent{Kind: "lifecycle", EntityID: env.Msg.MarketTicker,
		FrameType: env.Type, Channel: env.Type, SubscriptionID: env.SID, SourceGeneration: generation,
		ObservedAt: receivedAt, SourceAt: lifecycleSourceAt, SourceSequence: current,
		PriorSourceSequence: prior, SequenceGap: gap, Lifecycle: &ResearchReplayLifecycle{
			EventType: eventType, CloseTime: closeTime, SettledResult: settled,
		}})
	c.txMu.Lock()
	c.lifecycleFrames++
	if current != nil {
		c.lifecycleGeneration = generation
		c.lifecycleSID = env.SID
		c.lifecycleSequence = *current
		if prior != nil {
			c.lifecyclePrior = *prior
		} else {
			c.lifecyclePrior = 0
		}
		c.lifecycleLastGap = gap
		c.lifecycleGapTotal += gap
	}
	if !lifecycleSourceAt.IsZero() && lifecycleSourceAt.After(c.lifecycleSourceAt) {
		c.lifecycleSourceAt = lifecycleSourceAt
	}
	c.txMu.Unlock()
	if eventType == "activated" || eventType == "deactivated" || eventType == "close_date_updated" ||
		eventType == "determined" || eventType == "settled" {
		// Patch the already-complete market cache before notifying research consumers. This is a
		// local copy-on-write update; the WebSocket reader never performs REST or storage work.
		c.applyMarketLifecycleUpdate(env.Msg.MarketTicker, eventType, closeTime, receivedAt)
	}
	if eventType == "activated" || eventType == "deactivated" || eventType == "close_date_updated" {
		observed := time.Now().UTC()
		if !lifecycleSourceAt.IsZero() {
			observed = lifecycleSourceAt
		}
		c.txMu.Lock()
		lf := c.marketLifecycleFn
		c.txMu.Unlock()
		if lf != nil {
			lf(MarketLifecycleEvent{Ticker: env.Msg.MarketTicker, EventType: eventType,
				CloseTime: closeTime, Observed: observed, Raw: append([]byte(nil), data...)})
		}
	}
	// R124 D6: msg.event_type, not the outer channel name, identifies structure changes.
	if eventType == "price_level_structure_updated" || strings.Contains(eventType, "tick_size") {
		observed := time.Now().UTC()
		if env.Msg.TSMS > 0 {
			observed = time.UnixMilli(env.Msg.TSMS).UTC()
		} else if env.Msg.TS > 0 {
			observed = time.Unix(env.Msg.TS, 0).UTC()
		}
		u := PriceStructureUpdate{Ticker: env.Msg.MarketTicker, EventType: eventType,
			PriceLevelStructure: strings.TrimSpace(env.Msg.PriceLevelStructure), Observed: observed}
		valid := true
		if raw := env.Msg.PriceRanges; len(raw) > 0 && string(raw) != "null" {
			u.RangesPresent = true
			if err := json.Unmarshal(raw, &u.PriceRanges); err != nil {
				valid = false
			}
		}
		tickRaw := env.Msg.TickSize
		if len(tickRaw) == 0 || string(tickRaw) == "null" {
			tickRaw = env.Msg.TickSizeDollars
		}
		if len(tickRaw) > 0 && string(tickRaw) != "null" {
			if tick, ok := decodeStructureFloat(tickRaw); ok {
				u.TickSize, u.TickSizePresent = tick, true
			} else {
				valid = false
			}
		}
		if valid {
			c.applyPriceStructureUpdate(u)
		}
		c.txMu.Lock()
		fn := c.structFn
		parsedFn := c.structureUpdateFn
		c.txMu.Unlock()
		if valid && parsedFn != nil {
			u.PriceRanges = append([]PriceRange(nil), u.PriceRanges...)
			parsedFn(u)
		}
		if fn != nil {
			fn(eventType, append([]byte(nil), data...))
		}
		return
	}
	val, have := -1.0, false
	settlementValue := env.Msg.SettlementValue
	if settlementValue == "" {
		settlementValue = env.Msg.SettlementValueDollar
	}
	if v, err := strconv.ParseFloat(strings.TrimSpace(settlementValue), 64); err == nil && v >= 0 && v <= 1 {
		val, have = v, true
	}
	if !have {
		result := env.Msg.Result
		if result == "" {
			result = env.Msg.SettledResult
		}
		switch strings.ToLower(strings.TrimSpace(result)) {
		case "yes":
			val, have = 1, true
		case "no":
			val, have = 0, true
		}
	}
	if !have {
		// Only `created` is a listing event. Deactivation, close-date and metadata updates are not new
		// markets and must not flood the fresh-list verifier.
		if eventType != "created" {
			return
		}
		c.txMu.Lock()
		lf := c.listingFn
		c.txMu.Unlock()
		if lf != nil {
			age := time.Since(time.Unix(env.Msg.OpenTS, 0))
			isNew := env.Msg.OpenTS > 0 && age >= 0 && age < time.Hour
			lf(env.Msg.MarketTicker, isNew)
		}
		return
	}
	c.txMu.Lock()
	fn := c.lifecycleFn
	c.txMu.Unlock()
	if fn != nil {
		fn(env.Msg.MarketTicker, val)
	}
}

// ingestTicker parses one ticker message and records the market's live YES mid-price (the
// tradeable mid of yes_bid/yes_ask, falling back to last price).
// INTEGER-CENT FALLBACK (audit §6): only the `*_dollars` string fields were parsed — a message
// subtype carrying only the integer-cent fields silently ZEROED the live price feed.
func (c *Client) ingestTicker(data []byte) {
	var env struct {
		Type string `json:"type"`
		Msg  struct {
			MarketTicker string  `json:"market_ticker"`
			YesBid       string  `json:"yes_bid_dollars"`
			YesAsk       string  `json:"yes_ask_dollars"`
			Price        string  `json:"price_dollars"`
			YesBidC      float64 `json:"yes_bid"` // integer-cent fallbacks
			YesAskC      float64 `json:"yes_ask"`
			PriceC       float64 `json:"price"`
			Time         string  `json:"time"`
			TS           int64   `json:"ts"`
			TSMS         int64   `json:"ts_ms"`
			// R70-B SCHEMA_AUDIT #7 (AsyncAPI-verified 2026-07-04): the ticker payload carries
			// ABSOLUTE totals — open_interest_fp (fixed-point contracts string) + dollar_open_interest
			// (int) — NOT the "volume_delta/open_interest_delta" names the audit flagged UNVERIFIED
			// (those fields do not exist). Deltas are computed server-side by ringing the absolutes.
			OpenInterest string  `json:"open_interest_fp"`
			DollarOI     float64 `json:"dollar_open_interest"`
		} `json:"msg"`
	}
	if json.Unmarshal(data, &env) != nil || env.Type != "ticker" || env.Msg.MarketTicker == "" {
		return
	}
	now := time.Now().UTC()
	var sourceAt time.Time
	if env.Msg.TSMS > 0 {
		sourceAt = time.UnixMilli(env.Msg.TSMS).UTC()
	} else if env.Msg.TS > 0 {
		sourceAt = time.Unix(env.Msg.TS, 0).UTC()
	} else if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(env.Msg.Time)); err == nil {
		sourceAt = parsed.UTC()
	}
	age := now.Sub(sourceAt)
	c.txMu.Lock()
	c.tickerFrames++
	if sourceAt.IsZero() || age < -5*time.Second || age > 30*time.Second {
		c.tickerClockRejects++
		c.tickerClockRejectAt = now
		c.txMu.Unlock()
		return
	}
	c.txMu.Unlock()
	if of := c.oiHook(); of != nil {
		oi, _ := strconv.ParseFloat(env.Msg.OpenInterest, 64)
		if oi <= 0 && env.Msg.DollarOI > 0 {
			oi = env.Msg.DollarOI // dollar OI stands in when the fp field is absent (still an accumulation gauge)
		}
		if oi > 0 {
			of(env.Msg.MarketTicker, oi)
		}
	}
	bid, _ := strconv.ParseFloat(env.Msg.YesBid, 64)
	ask, _ := strconv.ParseFloat(env.Msg.YesAsk, 64)
	last, _ := strconv.ParseFloat(env.Msg.Price, 64)
	if bid == 0 && env.Msg.YesBidC > 0 {
		bid = env.Msg.YesBidC / 100
	}
	if ask == 0 && env.Msg.YesAskC > 0 {
		ask = env.Msg.YesAskC / 100
	}
	if last == 0 && env.Msg.PriceC > 0 {
		last = env.Msg.PriceC / 100
	}
	yes := 0.0
	switch {
	case bid > 0 && ask > 0:
		yes = (bid + ask) / 2
	case last > 0:
		yes = last
	case bid > 0:
		yes = bid
	case ask > 0:
		yes = ask
	}
	if yes <= 0 || yes >= 1 {
		return
	}
	c.txMu.Lock()
	if c.txPx == nil {
		c.txPx = map[string]tickerPx{}
	}
	e := c.txPx[env.Msg.MarketTicker]
	if e.winStart.IsZero() || sourceAt.Sub(e.winStart) > 3*time.Minute || sourceAt.Before(e.winStart) {
		e.winStart, e.first, e.hi, e.lo = sourceAt, yes, yes, yes
	} else {
		if yes > e.hi {
			e.hi = yes
		}
		if yes < e.lo {
			e.lo = yes
		}
	}
	e.yes, e.bid, e.ask, e.at, e.sourceAt, e.ws = yes, bid, ask, sourceAt, sourceAt, true
	c.txPx[env.Msg.MarketTicker] = e
	tf := c.tickFn
	c.txMu.Unlock()
	// R40 (operator: "latency will bite us"): EVENT-DRIVEN hook — every ticker update is handed to
	// the server the moment it arrives, so a resting live order that the 1¢ rule condemns is
	// canceled off THIS tick, not on the next 200ms sweep pass. The handler must be O(µs): it scans
	// a tiny in-memory order map and only spawns work on a hit.
	if tf != nil {
		tf(env.Msg.MarketTicker, yes)
	}
}

// SetTickHandler registers the per-tick callback (R40 event-driven cancels). Called from the WS
// read loop — the handler MUST be near-instant and never block.
func (c *Client) SetTickHandler(fn func(ticker string, yes float64)) {
	c.txMu.Lock()
	c.tickFn = fn
	c.txMu.Unlock()
}

// SetOIHandler registers the per-tick open-interest callback (R70-B SCHEMA_AUDIT #7). Same WS
// read-loop contract as SetTickHandler: the handler MUST be near-instant and never block.
func (c *Client) SetOIHandler(fn func(ticker string, oi float64)) {
	c.txMu.Lock()
	c.oiFn = fn
	c.txMu.Unlock()
}

func (c *Client) oiHook() func(string, float64) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	return c.oiFn
}

// LivePrice returns a market's live YES mid-price from the ticker stream and whether it is fresh
// (updated within 30s). Callers prefer this real-time WS price over REST-cached prices.
func (c *Client) LivePrice(ticker string) (float64, bool) {
	if ticker == "" {
		return 0, false
	}
	c.txMu.Lock()
	defer c.txMu.Unlock()
	s, ok := c.txPx[ticker]
	if !ok || !tickerExecutionFresh(s, time.Now(), 30*time.Second) || s.yes <= 0 || s.yes >= 1 {
		return 0, false
	}
	return s.yes, true
}

// LivePriceAt is LivePrice plus the price's last-update time and WITHOUT the 30s freshness gate —
// the R102 decision-time pricing surface: callers judge the age themselves (px_src/px_age telemetry).
// NOTE: a SeedTickers REST seed also lands here with its seed time, so "age" is honest either way.
func (c *Client) LivePriceAt(ticker string) (px float64, at time.Time, ok bool) {
	if ticker == "" {
		return 0, time.Time{}, false
	}
	c.txMu.Lock()
	defer c.txMu.Unlock()
	s, exists := c.txPx[ticker]
	at = s.at
	if s.ws {
		// Keep the venue clock authoritative even if a future refactor stores local receipt time in
		// tickerPx.at. LivePriceAt deliberately leaves the age decision to its caller, but it must
		// never hand a cancel/mark path a fresh local timestamp for an old WebSocket observation.
		at = s.sourceAt
	}
	if !exists || s.yes <= 0 || s.yes >= 1 || at.IsZero() || time.Until(at) > 5*time.Second {
		return 0, time.Time{}, false
	}
	return s.yes, at, true
}

// LiveBidAsk returns the fresh, executable YES top of book from Kalshi's
// all-market ticker WebSocket.  Unlike LivePrice, it never substitutes a last
// trade, one-sided quote, or midpoint: callers asking what they can buy/sell
// need both sides of the book.  The orderbook WS remains the richer source for
// depth on its priority subset; this surface gives BBO coverage to the full
// ticker universe without one REST request per signal.
func (c *Client) LiveBidAsk(ticker string) (bid, ask float64, ok bool) {
	if ticker == "" {
		return 0, 0, false
	}
	c.txMu.Lock()
	defer c.txMu.Unlock()
	s, exists := c.txPx[ticker]
	if !exists || !tickerExecutionFresh(s, time.Now(), 30*time.Second) || s.bid <= 0 || s.ask <= s.bid || s.ask >= 1 {
		return 0, 0, false
	}
	return s.bid, s.ask, true
}

// TickerCount returns how many markets currently have a fresh (<=30s) live ticker price — a quick
// health signal for the WS feed.
func (c *Client) TickerCount() int {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	n := 0
	for _, s := range c.txPx {
		// R19: 30s freshness made this gauge lie twice — it read 0 for minutes at boot (the ticker
		// channel only pushes on price CHANGES, so 3000 markets trickle in) and crashed to 0 during
		// any >30s stall. 3 minutes matches the feed's real cadence; genuine outages still go red.
		age := time.Since(s.at)
		if age >= -5*time.Second && age <= 3*time.Minute {
			n++
		}
	}
	return n
}

// TickerSourceClock reports only real ticker-WebSocket observations. REST warm-start seeds are
// excluded so a healthy catalog pull cannot disguise a dead or timestamp-less ticker feed.
func (c *Client) TickerSourceClock() (watermark time.Time, rows int, frames int64, rejects int64, lastReject time.Time) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	for _, sample := range c.txPx {
		if !sample.ws {
			continue
		}
		rows++
		if sample.sourceAt.After(watermark) {
			watermark = sample.sourceAt
		}
	}
	return watermark, rows, c.tickerFrames, c.tickerClockRejects, c.tickerClockRejectAt
}

// LifecycleSourceClock preserves the venue timestamp separately from local callback arrival.
func (c *Client) LifecycleSourceClock() (watermark time.Time, frames int64) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	return c.lifecycleSourceAt, c.lifecycleFrames
}

// LifecycleSourceTruth keeps the channel's actual sequence domain even when Kalshi omits an
// optional ts/ts_ms field. Sequence plus local receipt proves ordering and liveness; it does not
// pretend to measure venue-to-receipt latency.
type LifecycleSourceTruth struct {
	Watermark  time.Time
	Generation uint64
	SID        int64
	Sequence   int64
	Prior      int64
	LastGap    int64
	GapTotal   int64
	Frames     int64
}

func (c *Client) LifecycleSequenceTruth() LifecycleSourceTruth {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	return LifecycleSourceTruth{Watermark: c.lifecycleSourceAt, Generation: c.lifecycleGeneration,
		SID: c.lifecycleSID, Sequence: c.lifecycleSequence, Prior: c.lifecyclePrior,
		LastGap: c.lifecycleLastGap, GapTotal: c.lifecycleGapTotal, Frames: c.lifecycleFrames}
}

// SeedTickers primes the live-price map from a REST market snapshot (R19 cold-start fix): the
// ticker WS only pushes when a price CHANGES, so a fresh boot showed "0 tickers" for ~3 minutes
// while 3000 markets each waited for their first tick. REST implied probabilities are REAL current
// prices — seed them, let the WS take over per market as pushes arrive.
func (c *Client) SeedTickers(mkts []Market) int {
	now := time.Now()
	n := 0
	c.txMu.Lock()
	defer c.txMu.Unlock()
	if c.txPx == nil {
		c.txPx = map[string]tickerPx{}
	}
	for _, m := range mkts {
		if m.Ticker == "" {
			continue
		}
		if _, exists := c.txPx[m.Ticker]; exists {
			continue // never clobber a live WS price with a REST snapshot
		}
		ip := m.ImpliedProbability()
		if ip <= 0 || ip >= 1 {
			continue
		}
		bid, ask := m.YesBid.Float(), m.YesAsk.Float()
		c.txPx[m.Ticker] = tickerPx{yes: ip, bid: bid, ask: ask, at: now, winStart: now, first: ip, hi: ip, lo: ip}
		n++
	}
	return n
}

// Volatility returns a market's live intraday range (hi-lo) over the ticker WS's ~3-min window — a
// "moving fast" read computed off the live feed. ok=false if there's no fresh window.
func (c *Client) Volatility(ticker string) (float64, bool) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	e, ok := c.txPx[ticker]
	age := time.Since(e.at)
	if !ok || e.first <= 0 || age < -5*time.Second || age > 60*time.Second {
		return 0, false
	}
	return e.hi - e.lo, true
}

// Momentum returns a market's SIGNED YES-price move over the ticker WS's ~3-min window (current −
// window-open). ok=false if there's no fresh window. Pairs with Volatility for the consensus panel.
func (c *Client) Momentum(ticker string) (float64, bool) {
	c.txMu.Lock()
	defer c.txMu.Unlock()
	e, ok := c.txPx[ticker]
	age := time.Since(e.at)
	if !ok || e.first <= 0 || age < -5*time.Second || age > 60*time.Second {
		return 0, false
	}
	return e.yes - e.first, true
}
