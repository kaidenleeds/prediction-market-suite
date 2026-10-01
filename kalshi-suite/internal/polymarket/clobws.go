package polymarket

// CLOB market-channel WebSocket (R70-B SCHEMA_AUDIT #10): wss://ws-subscriptions-clob.polymarket.com
// /ws/market, subscribed with custom_feature_enabled:true. `book`, `price_change`, and
// `best_bid_ask` maintain executable outcome-token BBOs; `market_resolved` settles immediately.
// Read-only, public, no auth.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	clobWSURL             = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	clobSubChunk          = 40  // fallback only if a future caller exceeds the proven initial tier
	clobInitialSafeAssets = 100 // last empirically stable single-socket initial tier
	clobPlanRefreshEvery  = 30 * time.Second
)

func clobAssetChunks(assets []string) [][]string {
	var out [][]string
	for i := 0; i < len(assets); i += clobSubChunk {
		end := i + clobSubChunk
		if end > len(assets) {
			end = len(assets)
		}
		out = append(out, assets[i:end])
	}
	return out
}

func normalizeCLOBAssets(raw []string) []string {
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, id := range raw {
		id = strings.TrimSpace(id)
		if id != "" && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

func clobAssetDiff(current, desired []string) (add, remove []string) {
	current = normalizeCLOBAssets(current)
	desired = normalizeCLOBAssets(desired)
	have := make(map[string]bool, len(current))
	want := make(map[string]bool, len(desired))
	for _, id := range current {
		have[id] = true
	}
	for _, id := range desired {
		want[id] = true
		if !have[id] {
			add = append(add, id)
		}
	}
	for _, id := range current {
		if !want[id] {
			remove = append(remove, id)
		}
	}
	return add, remove
}

// ResolvedEvent is one CLOB `market_resolved` push: the condition id, its outcome labels and the
// winning outcome/asset (docs shape: market/outcomes/winning_outcome/winning_asset_id + slug).
type ResolvedEvent struct {
	ConditionID    string
	Slug           string
	Outcomes       []string
	WinningOutcome string
}

type clobFloat float64

func (f *clobFloat) UnmarshalJSON(b []byte) error {
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}
	*f = clobFloat(v)
	return nil
}

// clobTimestamp preserves the difference between an omitted timestamp (legacy-compatible arrival
// time) and an explicitly malformed/null source timestamp (event is unusable for quote freshness).
// The official market channel sends quoted Unix milliseconds.
type clobTimestamp struct {
	millis  int64
	present bool
	valid   bool
}

func (t *clobTimestamp) UnmarshalJSON(b []byte) error {
	t.present = true
	s := strings.Trim(strings.TrimSpace(string(b)), `"`)
	if s == "" || s == "null" {
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return nil
	}
	t.millis, t.valid = v, true
	return nil
}

func clobSourceTime(ts clobTimestamp, received time.Time) (time.Time, bool) {
	if !ts.present {
		return received, true
	}
	if !ts.valid {
		return time.Time{}, false
	}
	at := time.UnixMilli(ts.millis)
	// A venue clock a little ahead must not create a negative age and extend freshness.
	if at.After(received) {
		at = received
	}
	return at, true
}

type clobLevel struct {
	Price clobFloat `json:"price"`
	Size  clobFloat `json:"size"`
}

type clobPriceChange struct {
	AssetID string    `json:"asset_id"`
	Price   clobFloat `json:"price"`
	Size    clobFloat `json:"size"`
	Side    string    `json:"side"`
	BestBid clobFloat `json:"best_bid"`
	BestAsk clobFloat `json:"best_ask"`
}

type clobWireEvent struct {
	EventType      string            `json:"event_type"`
	Market         string            `json:"market"` // condition id
	AssetID        string            `json:"asset_id"`
	Slug           string            `json:"slug"`
	Outcomes       []string          `json:"outcomes"`
	WinningOutcome string            `json:"winning_outcome"`
	Bids           []clobLevel       `json:"bids"`
	Asks           []clobLevel       `json:"asks"`
	BestBid        clobFloat         `json:"best_bid"`
	BestAsk        clobFloat         `json:"best_ask"`
	Price          clobFloat         `json:"price"` // legacy single-change compatibility
	Size           clobFloat         `json:"size"`
	Side           string            `json:"side"`
	PriceChanges   []clobPriceChange `json:"price_changes"`
	Timestamp      clobTimestamp     `json:"timestamp"`
	OldTickSize    clobFloat         `json:"old_tick_size"`
	NewTickSize    clobFloat         `json:"new_tick_size"`
}

type clobTokenRef struct {
	condition string
	outcome   int
}

type clobBookState struct {
	condition          string
	bids, asks         map[float64]float64
	bestBid, bestAsk   float64
	bidDepth, askDepth float64
	at                 time.Time // execution freshness (source timestamp when present, receive time otherwise)
	sourceAt           time.Time // non-zero only when the frame carried a valid venue timestamp
	generation         uint64
	tickSize           float64
	tickAt             time.Time
	tickGeneration     uint64
}

func (c *Client) clobSetConnected(v bool) {
	c.clobMu.Lock()
	c.clobConnected = v
	if v {
		c.clobFrameAt = time.Now()
	}
	c.clobMu.Unlock()
}

func (c *Client) clobBeginConnection(conn *websocket.Conn, now time.Time) {
	c.clobMu.Lock()
	c.clobGeneration++
	if c.clobGeneration == 0 { // theoretical uint64 wrap: zero remains reserved for direct tests
		c.clobGeneration = 1
	}
	c.clobConn = conn
	c.clobConnected = true
	c.clobFrameAt = now
	c.clobReconnects++
	c.clobMu.Unlock()
}

func (c *Client) clobRecordError(err error) {
	if err == nil {
		return
	}
	c.clobMu.Lock()
	c.clobLastErr, c.clobLastErrAt = err.Error(), time.Now()
	c.clobMu.Unlock()
}

func (c *Client) clobTouch() {
	c.clobMu.Lock()
	c.clobConnected = true
	c.clobFrameAt = time.Now()
	c.clobMu.Unlock()
}

// registerMarketTokens records gamma's authoritative outcome order. Every market read path calls
// this before the token can enter a CLOB subscription, so token-specific BBOs stay side-correct.
func (c *Client) registerMarketTokens(m Market) {
	if strings.TrimSpace(m.ConditionID) == "" {
		return
	}
	// Fee discovery is condition-based and does not depend on token parsing. Seed it even if a
	// partial Gamma row temporarily omits clobTokenIds; absent fee fields intentionally do nothing.
	seedGammaFeeSchedule(m)
	var ids []string
	if json.Unmarshal([]byte(m.TokensRaw), &ids) != nil || len(ids) == 0 {
		return
	}
	c.clobMu.Lock()
	if c.clobTokenRef == nil {
		c.clobTokenRef = map[string]clobTokenRef{}
	}
	if c.clobAssets == nil {
		c.clobAssets = map[string][]string{}
	}
	cp := make([]string, 0, len(ids))
	for i, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		cp = append(cp, id)
		c.clobTokenRef[id] = clobTokenRef{condition: m.ConditionID, outcome: i}
		if b := c.clobBooks[id]; b != nil && b.condition == "" {
			b.condition = m.ConditionID
		}
	}
	c.clobAssets[m.ConditionID] = cp
	c.clobMu.Unlock()
}

func (c *Client) registerMarkets(ms []Market) {
	for _, m := range ms {
		c.registerMarketTokens(m)
	}
}

func clobRecompute(b *clobBookState) {
	b.bestBid, b.bestAsk, b.bidDepth, b.askDepth = 0, 0, 0, 0
	for px, sz := range b.bids {
		if sz <= 0 || px <= 0 || px >= 1 {
			continue
		}
		if px > b.bestBid {
			b.bestBid, b.bidDepth = px, sz
		} else if px == b.bestBid {
			b.bidDepth += sz
		}
	}
	for px, sz := range b.asks {
		if sz <= 0 || px <= 0 || px >= 1 {
			continue
		}
		if b.bestAsk == 0 || px < b.bestAsk {
			b.bestAsk, b.askDepth = px, sz
		} else if px == b.bestAsk {
			b.askDepth += sz
		}
	}
}

func (c *Client) clobBook(asset, condition string) *clobBookState {
	b := c.clobBooks[asset]
	if b == nil {
		b = &clobBookState{condition: condition, bids: map[float64]float64{}, asks: map[float64]float64{}}
		c.clobBooks[asset] = b
	} else if b.condition == "" {
		b.condition = condition
	}
	return b
}

func clobResetQuote(b *clobBookState) {
	b.bids, b.asks = map[float64]float64{}, map[float64]float64{}
	b.bestBid, b.bestAsk, b.bidDepth, b.askDepth = 0, 0, 0, 0
}

func (c *Client) ingestCLOBBook(e clobWireEvent, now time.Time, sourceProven bool) {
	if e.AssetID == "" {
		return
	}
	c.clobMu.Lock()
	b := c.clobBook(e.AssetID, e.Market)
	clobResetQuote(b)
	for _, l := range e.Bids {
		if px, sz := float64(l.Price), float64(l.Size); px > 0 && px < 1 && sz > 0 {
			b.bids[px] += sz
		}
	}
	for _, l := range e.Asks {
		if px, sz := float64(l.Price), float64(l.Size); px > 0 && px < 1 && sz > 0 {
			b.asks[px] += sz
		}
	}
	clobRecompute(b)
	b.at = now
	if sourceProven {
		b.sourceAt = now
	} else {
		b.sourceAt = time.Time{}
	}
	b.generation = c.clobGeneration
	c.clobMu.Unlock()
}

func (c *Client) ingestCLOBChanges(e clobWireEvent, now time.Time, sourceProven bool) {
	changes := e.PriceChanges
	if len(changes) == 0 && e.AssetID != "" { // pre-array wire compatibility
		changes = []clobPriceChange{{AssetID: e.AssetID, Price: e.Price, Size: e.Size, Side: e.Side, BestBid: e.BestBid, BestAsk: e.BestAsk}}
	}
	c.clobMu.Lock()
	defer c.clobMu.Unlock()
	for _, ch := range changes {
		if ch.AssetID == "" {
			continue
		}
		var levelsKind string
		switch strings.ToUpper(strings.TrimSpace(ch.Side)) {
		case "BUY":
			levelsKind = "bid"
		case "SELL":
			levelsKind = "ask"
		default:
			// Unknown/missing side used to default to ASK and could fabricate executable offers.
			continue
		}
		b := c.clobBook(ch.AssetID, e.Market)
		if b.generation != c.clobGeneration {
			// A delta from the replacement socket may arrive before its full snapshot. Start from
			// empty state and use only this event's authoritative BBO; never merge prior-generation
			// levels/depth into a quote that is about to be marked current.
			clobResetQuote(b)
		}
		px, sz := float64(ch.Price), float64(ch.Size)
		levels := b.asks
		if levelsKind == "bid" {
			levels = b.bids
		}
		if px > 0 && px < 1 {
			if sz <= 0 {
				delete(levels, px)
			} else {
				levels[px] = sz
			}
		}
		clobRecompute(b)
		// The push's best fields are authoritative even if this process joined between snapshots.
		if bid, ask := float64(ch.BestBid), float64(ch.BestAsk); bid > 0 && ask > bid && ask < 1 {
			if b.bestBid != bid {
				b.bestBid, b.bidDepth = bid, b.bids[bid]
			}
			if b.bestAsk != ask {
				b.bestAsk, b.askDepth = ask, b.asks[ask]
			}
		}
		b.at = now
		if sourceProven {
			b.sourceAt = now
		} else {
			b.sourceAt = time.Time{}
		}
		b.generation = c.clobGeneration
	}
}

func (c *Client) ingestCLOBBest(e clobWireEvent, now time.Time, sourceProven bool) {
	if e.AssetID == "" {
		return
	}
	bid, ask := float64(e.BestBid), float64(e.BestAsk)
	if bid <= 0 || ask <= bid || ask >= 1 {
		return
	}
	c.clobMu.Lock()
	b := c.clobBook(e.AssetID, e.Market)
	if b.generation != c.clobGeneration {
		// best_bid_ask proves top prices, not the old socket's quantities/levels.
		clobResetQuote(b)
	}
	if b.bestBid != bid {
		b.bestBid, b.bidDepth = bid, b.bids[bid]
	}
	if b.bestAsk != ask {
		b.bestAsk, b.askDepth = ask, b.asks[ask]
	}
	b.at = now
	if sourceProven {
		b.sourceAt = now
	} else {
		b.sourceAt = time.Time{}
	}
	b.generation = c.clobGeneration
	c.clobMu.Unlock()
}

func (c *Client) ingestCLOBTick(e clobWireEvent, now time.Time, _ bool) {
	newTick := float64(e.NewTickSize)
	if e.AssetID == "" || newTick <= 0 || newTick >= 1 {
		return
	}
	c.clobMu.Lock()
	b := c.clobBook(e.AssetID, e.Market)
	b.tickSize = newTick
	b.tickAt = now
	b.tickGeneration = c.clobGeneration
	c.clobMu.Unlock()
}

// CLOBOutcomeBBO returns the executable token book for one outcome index. It never complements
// the other token unless that is all the caller has; YES and NO asks therefore come from their
// own venue books. A quiet book remains valid through a connected socket while in-place priority
// updates avoid recurring reconnect gaps; a disconnected/stale socket fails closed.
func (c *Client) CLOBOutcomeBBO(conditionID string, outcome int) (bid, ask, bidDepth, askDepth float64, at time.Time, ok bool) {
	c.clobMu.RLock()
	defer c.clobMu.RUnlock()
	if !c.clobConnected || time.Since(c.clobFrameAt) > 45*time.Second || outcome < 0 {
		return
	}
	assets := c.clobAssets[conditionID]
	if outcome >= len(assets) {
		return
	}
	b := c.clobBooks[assets[outcome]]
	if b == nil || b.generation != c.clobGeneration || time.Since(b.at) > 6*time.Minute || b.bestBid <= 0 || b.bestAsk <= b.bestBid || b.bestAsk >= 1 {
		return
	}
	return b.bestBid, b.bestAsk, b.bidDepth, b.askDepth, b.at, true
}

// CLOBOutcomeTick returns a tick-size change proven on the current socket generation. Gamma's
// OrderPriceMinTickSize remains the REST snapshot fallback; a pre-reconnect WS tick is never
// promoted merely because a replacement socket connected.
func (c *Client) CLOBOutcomeTick(conditionID string, outcome int) (tick float64, at time.Time, ok bool) {
	c.clobMu.RLock()
	defer c.clobMu.RUnlock()
	if !c.clobConnected || time.Since(c.clobFrameAt) > 45*time.Second || outcome < 0 {
		return
	}
	assets := c.clobAssets[conditionID]
	if outcome >= len(assets) {
		return
	}
	b := c.clobBooks[assets[outcome]]
	if b == nil || b.tickGeneration != c.clobGeneration || b.tickSize <= 0 || b.tickSize >= 1 {
		return
	}
	return b.tickSize, b.tickAt, true
}

func (c *Client) overlayCLOBBBO(m Market) Market {
	if bid, ask, _, _, _, ok := c.CLOBOutcomeBBO(m.ConditionID, 0); ok {
		m.BestBid, m.BestAsk = bid, ask
	}
	return m
}

// StartCLOBMarketWS keeps the CLOB market channel connected forever. assetsFn returns the CLOB
// token ids to subscribe (both outcome tokens of the server's bounded priority conditions); it is
// re-read on every (re)connect and refreshed in place while the socket stays healthy. onResolved
// fires per market_resolved push — the
// handler must be quick (spawn its own work). Auto-reconnects with backoff; returns on ctx done.
func (c *Client) StartCLOBMarketWS(ctx context.Context, assetsFn func() []string, onResolved func(ResolvedEvent)) {
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		err := c.runCLOBMarketWS(ctx, assetsFn, onResolved)
		if ctx.Err() != nil {
			return
		}
		c.clobRecordError(err)
		if time.Since(started) > time.Minute {
			backoff = 2 * time.Second
		} else if backoff < 60*time.Second {
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

func (c *Client) runCLOBMarketWS(ctx context.Context, assetsFn func() []string, onResolved func(ResolvedEvent)) error {
	assets := normalizeCLOBAssets(assetsFn())
	if len(assets) == 0 {
		return fmt.Errorf("subscription plan is empty") // reconnect loop retries after discovery warms
	}
	seenAssets := make(map[string]bool, len(assets))
	for _, id := range assets {
		seenAssets[id] = true
	}
	// Initial pruning plus successful in-place unsubscriptions bound resolved/delisted token books
	// without throwing away the current socket generation on a timer.
	c.clobMu.Lock()
	for id := range c.clobBooks {
		if !seenAssets[id] {
			delete(c.clobBooks, id)
		}
	}
	c.clobMu.Unlock()
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, clobWSURL, http.Header{})
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close() // R102 (auditor bug 248, P4): handshake-failure response body never closed — fd leak per failed dial
		}
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()
	var writeMu sync.Mutex // Gorilla permits one concurrent reader and exactly one writer.
	c.clobBeginConnection(conn, time.Now())
	defer func() {
		c.clobMu.Lock()
		if c.clobConn == conn {
			c.clobConn = nil
			c.clobConnected = false
		}
		c.clobMu.Unlock()
	}()
	// Subscribe the proven bounded tier in one official initial frame. The venue's dynamic update
	// path repeatedly closed otherwise-valid 400-token sockets during R141 soak. A future caller
	// above this tier falls back to 40-token dynamic chunks; 100 initial books remain below the
	// raised 32 MiB read ceiling and avoid recreating the historical 1,000-token giant snapshot.
	chunks := [][]string{assets}
	if len(assets) > clobInitialSafeAssets {
		chunks = clobAssetChunks(assets)
	}
	writeMu.Lock()
	err = conn.WriteJSON(map[string]any{"assets_ids": chunks[0], "type": "market", "custom_feature_enabled": true})
	writeMu.Unlock()
	if err != nil {
		return fmt.Errorf("initial subscription: %w", err)
	}
	// The venue removed its token cap, but the caller deliberately bounds the working set so an
	// initial snapshot cannot recreate the old full-catalog memory/CPU flood.
	conn.SetReadLimit(32 << 20)
	conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	conn.SetPongHandler(func(string) error {
		c.clobTouch()
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		return nil
	})
	done := make(chan struct{})
	defer close(done)
	terminal := make(chan error, 1)
	closeWith := func(err error) {
		select {
		case terminal <- err:
		default:
		}
		_ = conn.Close()
	}
	// Read the first snapshots while adding the remaining chunks, then refresh the bounded
	// priority set in place. Re-dialing a healthy socket every five minutes made the entire depth
	// tier cold for several seconds and discarded usable current-generation books.
	go func() {
		current := append([]string(nil), assets...)
		writeOperation := func(operation string, ids []string) bool {
			for _, chunk := range clobAssetChunks(ids) {
				writeMu.Lock()
				writeErr := conn.WriteJSON(map[string]any{"assets_ids": chunk,
					"operation": operation, "custom_feature_enabled": true})
				writeMu.Unlock()
				if writeErr != nil {
					closeWith(fmt.Errorf("dynamic %s: %w", operation, writeErr))
					return false
				}
				select {
				case <-done:
					return false
				case <-ctx.Done():
					closeWith(ctx.Err())
					return false
				case <-time.After(250 * time.Millisecond):
				}
			}
			return true
		}
		for _, chunk := range chunks[1:] {
			if !writeOperation("subscribe", chunk) {
				return
			}
		}
		refresh := time.NewTicker(clobPlanRefreshEvery)
		defer refresh.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				closeWith(ctx.Err())
				return
			case <-refresh.C:
			}
			desired := normalizeCLOBAssets(assetsFn())
			if len(desired) == 0 {
				continue // retain last-good subscriptions through a cold/partial catalog refresh
			}
			add, remove := clobAssetDiff(current, desired)
			// Subscribe replacements before removing old seats so plan changes create no gap.
			if len(add) > 0 && !writeOperation("subscribe", add) {
				return
			}
			if len(remove) > 0 && !writeOperation("unsubscribe", remove) {
				return
			}
			if len(remove) > 0 {
				c.clobMu.Lock()
				for _, id := range remove {
					delete(c.clobBooks, id)
				}
				c.clobMu.Unlock()
			}
			current = desired
		}
	}()
	go func() { // official market-channel heartbeat is the text frame PING every ~10s
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				closeWith(ctx.Err())
				return
			case <-t.C:
				writeMu.Lock()
				err := conn.WriteMessage(websocket.TextMessage, []byte("PING"))
				writeMu.Unlock()
				if err != nil {
					closeWith(fmt.Errorf("heartbeat write: %w", err))
					return
				}
			}
		}
	}()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			select {
			case reason := <-terminal:
				return reason
			default:
				return fmt.Errorf("read: %w", err)
			}
		}
		c.clobTouch() // includes text PONG heartbeat replies
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		c.ingestCLOBMarket(data, onResolved)
	}
}

// ingestCLOBMarket decodes one market-channel frame. Frames may be a single event object or an
// ARRAY of events, and keep-alive replies ("PONG") are not JSON. Book snapshots, level changes,
// and direct BBO pushes feed the executable quote cache; resolution remains an immediate callback.
func (c *Client) ingestCLOBMarket(data []byte, onResolved func(ResolvedEvent)) {
	c.clobTouch() // direct unit tests and any future transport adapter still stamp arrival freshness
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || (!strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[")) {
		return // PONG / non-JSON keep-alive chatter
	}
	var evs []clobWireEvent
	if json.Unmarshal(data, &evs) != nil {
		var one clobWireEvent
		if json.Unmarshal(data, &one) != nil {
			return
		}
		evs = []clobWireEvent{one}
	}
	now := time.Now()
	for _, e := range evs {
		sourceAt, sourceOK := clobSourceTime(e.Timestamp, now)
		sourceProven := e.Timestamp.present && e.Timestamp.valid
		switch e.EventType {
		case "book":
			if sourceOK {
				c.ingestCLOBBook(e, sourceAt, sourceProven)
			}
		case "price_change":
			if sourceOK {
				c.ingestCLOBChanges(e, sourceAt, sourceProven)
			}
		case "best_bid_ask":
			if sourceOK {
				c.ingestCLOBBest(e, sourceAt, sourceProven)
			}
		case "tick_size_change":
			if sourceOK {
				c.ingestCLOBTick(e, sourceAt, sourceProven)
			}
		case "market_resolved":
			if e.Market != "" && onResolved != nil {
				onResolved(ResolvedEvent{ConditionID: e.Market, Slug: e.Slug, Outcomes: e.Outcomes, WinningOutcome: e.WinningOutcome})
			}
		}
	}
}

// CLOBStats is the market-channel vitality receipt used by readiness/latency surfaces.
func (c *Client) CLOBStats() (connected bool, books, fresh int, frameAgeSec float64) {
	now := time.Now()
	c.clobMu.RLock()
	defer c.clobMu.RUnlock()
	connected = c.clobConnected
	frameFresh := connected && now.Sub(c.clobFrameAt) <= 45*time.Second
	if c.clobFrameAt.IsZero() {
		frameAgeSec = -1
	} else {
		frameAgeSec = now.Sub(c.clobFrameAt).Seconds()
	}
	for _, b := range c.clobBooks {
		if b == nil {
			continue
		}
		books++
		if frameFresh && b.generation == c.clobGeneration && now.Sub(b.at) <= 6*time.Minute && b.bestBid > 0 && b.bestAsk > b.bestBid && b.bestAsk < 1 {
			fresh++
		}
	}
	return
}

// CLOBConnectionReceipt explains reconnect churn without logging credentials or every retry.
func (c *Client) CLOBConnectionReceipt() (lastErr string, lastErrAt time.Time, reconnects uint64) {
	c.clobMu.RLock()
	defer c.clobMu.RUnlock()
	return c.clobLastErr, c.clobLastErrAt, c.clobReconnects
}

// CLOBSourceClock returns the newest validated venue timestamp on the current socket generation.
// Heartbeats advance transport freshness but never advance this source watermark.
func (c *Client) CLOBSourceClock() (watermark time.Time, rows int, connected bool) {
	c.clobMu.RLock()
	defer c.clobMu.RUnlock()
	connected = c.clobConnected
	for _, book := range c.clobBooks {
		if book == nil || book.generation != c.clobGeneration || book.sourceAt.IsZero() {
			continue
		}
		rows++
		if book.sourceAt.After(watermark) {
			watermark = book.sourceAt
		}
	}
	return watermark, rows, connected
}

// ForceReconnectCLOB closes the current market-channel socket. The owning loop redials with its
// normal backoff and obtains a new full snapshot; false means the reconnect loop is already idle.
func (c *Client) ForceReconnectCLOB() bool {
	c.clobMu.Lock()
	conn := c.clobConn
	c.clobConn = nil
	c.clobConnected = false
	c.clobMu.Unlock()
	if conn == nil {
		return false
	}
	_ = conn.Close()
	return true
}
