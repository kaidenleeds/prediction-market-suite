package polymarket

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// rtdsHost is Polymarket's Real-Time Data Socket — the live activity stream the website
// itself consumes. Trades arrive within milliseconds, with no meaningful rate limit. This
// is the fix for the chronically-stale whale feed: the REST /trades endpoint is a
// batch/eventual-consistency endpoint that lags minutes behind, no matter how it's polled.
const rtdsHost = "wss://ws-live-data.polymarket.com"

// StartLiveTrades keeps a WebSocket connection to the RTDS open forever, subscribed to the
// live trade stream, feeding a rolling in-memory buffer. It auto-reconnects on any drop.
// Call once in a goroutine at startup.
func (c *Client) StartLiveTrades(ctx context.Context) {
	backoff := 2 * time.Second // exponential backoff + jitter (audit §6: fixed 2s hammered a struggling endpoint)
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		c.runLiveTrades(ctx)
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

func (c *Client) runLiveTrades(ctx context.Context) {
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, rtdsHost, http.Header{})
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close() // R102 (auditor bug 248, P4): handshake-failure response body never closed — fd leak per failed dial
		}
		return
	}
	defer conn.Close()
	// R108 staleness watchdog: publish the live socket so ForceReconnect can kill a wedged-but-
	// ponging connection from outside; cleared on exit so a kick can never hit a dead handle twice.
	c.wsMu.Lock()
	c.wsConn = conn
	c.wsMu.Unlock()
	defer func() {
		c.wsMu.Lock()
		if c.wsConn == conn {
			c.wsConn = nil
		}
		c.wsMu.Unlock()
	}()

	// Subscribe to the live trades activity stream (exact wire format from Polymarket's
	// own real-time-data-client: {"action":"subscribe","subscriptions":[...]}).
	sub := map[string]any{
		"action":        "subscribe",
		"subscriptions": []map[string]string{{"topic": "activity", "type": "trades"}},
	}
	if err := conn.WriteJSON(sub); err != nil {
		return
	}

	// Keep-alive: the RTDS expects a text "ping" every ~5s. A pong (control frame) refreshes
	// the read deadline so a quiet market doesn't look like a dead connection.
	conn.SetReadLimit(1 << 21) // 2MB frame cap (audit §6)
	conn.SetReadDeadline(time.Now().Add(45 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		return nil
	})
	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-pingDone:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
					return
				}
			}
		}
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return // triggers reconnect in StartLiveTrades
		}
		conn.SetReadDeadline(time.Now().Add(45 * time.Second))
		c.ingestLiveMessage(data)
	}
}

// ingestLiveMessage parses one RTDS message ({topic,type,payload}) and, if it's an activity
// trade, appends it to the rolling buffer (keeping ~13 min, capped). The payload may be a
// single trade object or an array.
func (c *Client) ingestLiveMessage(data []byte) {
	var env struct {
		Topic   string          `json:"topic"`
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(data, &env) != nil || env.Topic != "activity" || len(env.Payload) == 0 {
		return
	}
	var batch []PolyTrade
	if json.Unmarshal(env.Payload, &batch) != nil { // not an array -> try a single object
		var one PolyTrade
		if json.Unmarshal(env.Payload, &one) != nil {
			return
		}
		batch = []PolyTrade{one}
	}
	if len(batch) == 0 {
		return
	}
	now := time.Now()
	c.ltMu.Lock()
	for _, t := range batch {
		// Keep every valid positive print, even when its notional is pennies. Whale/significance
		// thresholds belong to later research consumers, never to the raw RTDS observation layer.
		if (t.ProxyWallet == "" && t.Title == "") || t.Size <= 0 || t.Price <= 0 || t.Price > 1 ||
			math.IsNaN(t.Size) || math.IsInf(t.Size, 0) || math.IsNaN(t.Price) || math.IsInf(t.Price, 0) {
			continue
		}
		c.liveTr = append(c.liveTr, t)
		// Record the live YES (outcome-0) price for this market so the cross-platform arb
		// can show a price that moves the instant a trade prints, instead of the laggy
		// gamma outcomePrices summary field.
		if t.ConditionID != "" && t.Price > 0 && t.Price < 1 {
			yes := t.Price
			if t.OutcomeIndex != 0 {
				yes = 1 - t.Price
			}
			c.lastPx[t.ConditionID] = pxSample{px: yes, at: now}
		}
	}
	c.liveAt = now
	// Trim to the last ~13 min by trade timestamp, and hard-cap the slice length.
	cutoff := now.Add(-13 * time.Minute).Unix()
	kept := c.liveTr[:0]
	for _, t := range c.liveTr {
		if t.Timestamp >= cutoff {
			kept = append(kept, t)
		}
	}
	c.liveTr = kept
	if len(c.liveTr) > 5000 {
		c.liveTr = c.liveTr[len(c.liveTr)-5000:]
	}
	c.ltMu.Unlock() // unlock BEFORE the callback (which re-reads the buffer) to avoid deadlock
	// Push: notify the server the instant new trades land so they're folded into the displayed
	// whale log immediately — no polling/ticker delay. The handler throttles bursts.
	if c.OnTrades != nil {
		c.OnTrades()
	}
}

// LiveTrades returns a copy of the current live-trade buffer (newest trades included as they
// stream in). Empty if the WebSocket hasn't connected/received yet — callers fall back to the
// REST tape in that case.
func (c *Client) LiveTrades() []PolyTrade {
	c.ltMu.Lock()
	defer c.ltMu.Unlock()
	out := make([]PolyTrade, len(c.liveTr))
	copy(out, c.liveTr)
	return out
}

// LivePrice returns the most recent live YES (outcome-0) price for a market from the trade
// stream, and whether it's fresh (traded within the last 2 min). Lets the arb show a price
// that updates the moment a trade prints.
func (c *Client) LivePrice(conditionID string) (float64, bool) {
	if conditionID == "" {
		return 0, false
	}
	// Executable CLOB BBO is a stronger mark than the last trade and updates on order changes.
	if bid, ask, _, _, _, ok := c.CLOBOutcomeBBO(conditionID, 0); ok {
		return (bid + ask) / 2, true
	}
	c.ltMu.Lock()
	defer c.ltMu.Unlock()
	s, ok := c.lastPx[conditionID]
	if !ok || time.Since(s.at) > 2*time.Minute || s.px <= 0 || s.px >= 1 {
		return 0, false
	}
	return s.px, true
}

// ForceReconnect (R108 staleness watchdog) closes the CURRENT live-trades socket, if any —
// the read loop errors out and StartLiveTrades redials with its normal backoff. Returns true
// when there was a socket to kill (false = not connected; the redial loop is already working).
func (c *Client) ForceReconnect() bool {
	c.wsMu.Lock()
	conn := c.wsConn
	c.wsConn = nil // one kick per connection — the next handle is published by the next dial
	c.wsMu.Unlock()
	if conn == nil {
		return false
	}
	_ = conn.Close()
	return true
}

// LiveTradesAge returns seconds since the last live trade message (-1 if none yet) — powers
// the feed-health badge.
func (c *Client) LiveTradesAge() float64 {
	c.ltMu.Lock()
	defer c.ltMu.Unlock()
	if c.liveAt.IsZero() {
		return -1
	}
	return time.Since(c.liveAt).Seconds()
}
