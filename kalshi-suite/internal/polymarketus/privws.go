// privws.go — Polymarket US PRIVATE WebSocket (wss://api.polymarket.us/v1/ws/private): real-time
// orders, executions, positions and balance for OUR account. This is the docs' own answer to the
// REST 429s ("use WebSocket instead of polling"): one persistent authenticated connection replaces
// the orders/positions/balance polling entirely; REST remains as reconciliation.
//
// Wire shapes verified against docs.polymarket.us/api-reference/websocket/private (2026-07-02):
//
//	subscribe: {"subscribe":{"requestId":..,"subscriptionType":"SUBSCRIPTION_TYPE_ORDER",...}}
//	inbound:   orderSubscriptionSnapshot{orders[]} · orderSubscriptionUpdate{execution{order,lastPx,
//	           lastShares,type,tradeId}} · positionSubscription{before/afterPosition,...} ·
//	           accountBalancesSnapshot{balances[]} / accountBalancesUpdate{balanceChange{...}}
package polymarketus

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

// PUSFill is one of OUR executions, pushed in real time (feeds fill-price labels + the order log).
type PUSFill struct {
	OrderID         string  `json:"order_id"`
	Slug            string  `json:"slug"`
	Outcome         string  `json:"outcome"`   // YES | NO (from order intent)
	Action          string  `json:"action"`    // BUY | SELL
	Px              float64 `json:"px"`        // traded outcome's price (NO is 1-YesPrice)
	YesPrice        float64 `json:"yes_price"` // raw venue wire price; PolyUS always denominates in YES
	Qty             float64 `json:"qty"`
	Type            string  `json:"type"` // PARTIAL_FILL | FILL
	TradeID         string  `json:"trade_id"`
	Time            string  `json:"time"`
	CommissionUSD   float64 `json:"commission_usd,omitempty"` // this execution only; never order cumulative
	CommissionKnown bool    `json:"commission_known,omitempty"`
	Aggressor       bool    `json:"aggressor,omitempty"` // venue truth: true=taker, false=maker
	AggressorKnown  bool    `json:"aggressor_known,omitempty"`
}

// pusOrderWire is the Order JSON shape shared by REST /v1/orders/open and the WS snapshot/updates.
type pusOrderWire struct {
	ID                   string         `json:"id"`
	MarketSlug           string         `json:"marketSlug"`
	Price                pusAmt         `json:"price"`
	Quantity             float64        `json:"quantity"`
	LeavesQuantity       float64        `json:"leavesQuantity"`
	TIF                  string         `json:"tif"`
	OutcomeSide          string         `json:"outcomeSide"`
	Intent               string         `json:"intent"`
	Action               string         `json:"action"`
	State                string         `json:"state"`
	InsertTime           string         `json:"insertTime"`
	CreateTime           string         `json:"createTime"`
	CommissionTotal      pusOptionalAmt `json:"commissionNotionalTotalCollected"`
	CommissionTotalSnake pusOptionalAmt `json:"commission_notional_total_collected"`
	MarketMetadata       struct {
		Title string `json:"title"`
	} `json:"marketMetadata"`
}

func intentSides(intent string) (outcome, action string) {
	switch intent {
	case "ORDER_INTENT_BUY_LONG":
		return "YES", "BUY"
	case "ORDER_INTENT_SELL_LONG":
		return "YES", "SELL"
	case "ORDER_INTENT_BUY_SHORT":
		return "NO", "BUY"
	case "ORDER_INTENT_SELL_SHORT":
		return "NO", "SELL"
	}
	return "", ""
}

// orderSides treats the current Retail response fields as authority and keeps intent only as the
// documented backward-compatible fallback.  Some synchronous execution responses omit intent;
// older order receipts omit outcomeSide/action.
func orderSides(outcomeSide, action, intent string) (outcome, orderAction string) {
	rawOutcome, rawAction := strings.TrimSpace(outcomeSide), strings.TrimSpace(action)
	if rawOutcome == "" && rawAction == "" {
		return intentSides(intent)
	}
	// The current fields are one atomic pair. A half-present or malformed pair must not be
	// hybridized with one component of legacy intent; doing so can silently reverse exposure.
	if rawOutcome == "" || rawAction == "" {
		return "", ""
	}
	outcome = strings.ToUpper(rawOutcome)
	outcome = strings.TrimPrefix(outcome, "OUTCOME_SIDE_")
	if outcome != "YES" && outcome != "NO" {
		outcome = ""
	}
	orderAction = strings.ToUpper(rawAction)
	orderAction = strings.TrimPrefix(orderAction, "ORDER_ACTION_")
	if orderAction != "BUY" && orderAction != "SELL" {
		orderAction = ""
	}
	if outcome == "" || orderAction == "" {
		return "", ""
	}
	return outcome, orderAction
}

// outcomePrice converts the venue's wire price into the price paid/received for the named
// outcome. PolyUS denominates every order and execution price in YES space, including both
// BUY_SHORT (buy NO) and SELL_SHORT (sell NO). Keeping that conversion at this boundary prevents
// downstream exposure, fill labels, and stale-order comparisons from silently treating a NO order
// as a YES order. Unknown outcomes remain in raw YES space rather than guessing.
func outcomePrice(yesPrice float64, outcome string) float64 {
	if strings.EqualFold(strings.TrimSpace(outcome), "NO") {
		return 1 - yesPrice
	}
	return yesPrice
}

func (o pusOrderWire) flat() PUSOrder {
	yesPrice := o.Price.f()
	commission := o.CommissionTotal
	if !commission.Present {
		commission = o.CommissionTotalSnake
	}
	out := PUSOrder{
		ID: o.ID, Slug: o.MarketSlug, Title: o.MarketMetadata.Title,
		Outcome:  strings.TrimPrefix(o.OutcomeSide, "OUTCOME_SIDE_"),
		Action:   strings.TrimPrefix(o.Action, "ORDER_ACTION_"),
		YesPrice: yesPrice, Qty: o.Quantity, Leaves: o.LeavesQuantity,
		State: strings.TrimPrefix(o.State, "ORDER_STATE_"), TIF: strings.TrimPrefix(o.TIF, "TIME_IN_FORCE_"),
		Created: o.InsertTime, CommissionTotalUSD: commission.Value, CommissionTotalKnown: commission.Present,
	}
	if out.Created == "" {
		out.Created = o.CreateTime
	}
	out.Outcome, out.Action = orderSides(o.OutcomeSide, o.Action, o.Intent)
	out.Price = outcomePrice(yesPrice, out.Outcome)
	return out
}

// PrivateWS maintains the live account state pushed by the venue.
type PrivateWS struct {
	c          *Client
	wsURL      string              // blank = production; package tests override this with a local websocket peer
	snapOrders map[string]PUSOrder // chunked snapshot staging; published only at eof
	snapReady  bool                // false on reconnect/incomplete snapshot; REST reconciles

	mu        sync.Mutex
	orders    map[string]PUSOrder // open orders by id (snapshot ∪ updates; terminal states delete)
	fills     []PUSFill           // rolling, drained by the server (caps at 200)
	bal       float64
	buyPow    float64
	balOK     bool
	posDirty  bool // a position changed → server refetches positions REST once
	lastMsg   time.Time
	connected bool

	onEvent        func()                                          // server callback: invalidate live snapshot + SSE push (may be nil)
	onState        func(state, detail string)                      // observability: CONNECT / SUBSCRIBED / CLOSED(reason) into the order log
	onDead         func(slug, outcome, state, reason, text string) // terminal-unfilled order → order log (silent-death visibility)
	onFillOverflow func([]PUSFill)                                 // R76 (auditor bug 10): evicted ring overflow → server persists to disk (real-money records must never silently vanish)
}

const privateWSFreshMaxAge = 2 * time.Minute

// healthyLocked is the one freshness definition for account-scoped push truth. A cached balance
// is safe to use only while the private socket that would deliver subsequent balance changes is
// still connected and demonstrably alive. Disconnects and stale traffic fail closed immediately.
func (w *PrivateWS) healthyLocked(now time.Time) bool {
	age := now.Sub(w.lastMsg)
	return w.connected && !w.lastMsg.IsZero() && age >= 0 && age < privateWSFreshMaxAge
}

func (w *PrivateWS) beginSessionLocked(now time.Time) {
	w.connected, w.lastMsg = true, now
	w.snapOrders, w.snapReady = nil, false
	// A new socket must earn a balance snapshot of its own. Carrying balOK across a reconnect would
	// briefly relabel the prior session's dollars as fresh merely because SUBSCRIBED or a pong landed.
	w.bal, w.buyPow, w.balOK = 0, 0, false
}

// SetFillOverflowHandler wires the fill-ring overflow sink (R76, auditor bug 10). When the server
// stalls (the exact time fills pile up fastest) and the ring passes its cap, the EVICTED records go
// here — the server appends them to a JSONL sidecar — instead of being silently discarded.
func (w *PrivateWS) SetFillOverflowHandler(fn func([]PUSFill)) {
	w.mu.Lock()
	w.onFillOverflow = fn
	w.mu.Unlock()
}

// SetOnDead wires the terminal-unfilled order callback (REJECTED / EXPIRED / CANCELED with no fill).
func (w *PrivateWS) SetOnDead(fn func(slug, outcome, state, reason, text string)) {
	w.mu.Lock()
	w.onDead = fn
	w.mu.Unlock()
}

// NewPrivateWS builds the private stream handle (call Stream in a goroutine).
func (c *Client) NewPrivateWS() *PrivateWS {
	return &PrivateWS{c: c, orders: map[string]PUSOrder{}}
}

// SetOnEvent registers the push callback (fired on any meaningful account event).
func (w *PrivateWS) SetOnEvent(fn func()) { w.mu.Lock(); w.onEvent = fn; w.mu.Unlock() }

// SetOnState registers the connection-state callback (CONNECT/SUBSCRIBED/CLOSED + detail) — the
// observability that was missing when the stream silently failed and the UI just said "poll".
func (w *PrivateWS) SetOnState(fn func(state, detail string)) {
	w.mu.Lock()
	w.onState = fn
	w.mu.Unlock()
}

func (w *PrivateWS) state(st, detail string) {
	w.mu.Lock()
	fn := w.onState
	w.mu.Unlock()
	if fn != nil {
		fn(st, detail)
	}
}

// Healthy reports a live connection with recent traffic (2-min freshness window).
func (w *PrivateWS) Healthy() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.healthyLocked(time.Now())
}

// Orders returns the current open-order set (ok=false when the stream isn't healthy — fall back to REST).
func (w *PrivateWS) Orders() ([]PUSOrder, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.snapReady || !w.healthyLocked(time.Now()) {
		return nil, false
	}
	out := make([]PUSOrder, 0, len(w.orders))
	for _, o := range w.orders {
		out = append(out, o)
	}
	return out, true
}

// DrainFills returns + clears the accumulated executions.
func (w *PrivateWS) DrainFills() []PUSFill {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.fills
	w.fills = nil
	return out
}

// Balance returns the pushed account balance. Cached dollars are venue truth only while the
// private stream remains connected and recent; stale/disconnected state must size live orders at
// zero rather than carrying the last balOK receipt indefinitely.
func (w *PrivateWS) Balance() (bal, buyingPower float64, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.balOK || !w.healthyLocked(time.Now()) {
		return 0, 0, false
	}
	return w.bal, w.buyPow, true
}

// PositionsDirty reports-and-clears the "a position changed" flag.
func (w *PrivateWS) PositionsDirty() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	d := w.posDirty
	w.posDirty = false
	return d
}

// Stream connects + subscribes forever (exp backoff + jitter, same discipline as the Kalshi WS).
func (w *PrivateWS) Stream(ctx context.Context) {
	backoff := 2 * time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		started := time.Now()
		w.run(ctx)
		w.mu.Lock()
		w.connected = false
		w.mu.Unlock()
		if time.Since(started) > time.Minute {
			backoff = 2 * time.Second
		} else if backoff < 60*time.Second {
			backoff *= 2
		}
		jitter := time.Duration(time.Now().UnixNano() % int64(backoff/2+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff/2 + jitter):
		}
	}
}

func (w *PrivateWS) run(ctx context.Context) {
	h := http.Header{}
	for k, v := range w.c.authHeaders("GET", "/v1/ws/private") {
		h.Set(k, v)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	wsURL := strings.TrimSpace(w.wsURL)
	if wsURL == "" {
		wsURL = "wss://api.polymarket.us/v1/ws/private"
	}
	conn, resp, err := dialer.DialContext(ctx, wsURL, h)
	if err != nil {
		detail := err.Error()
		if resp != nil {
			detail = fmt.Sprintf("%s (HTTP %d)", detail, resp.StatusCode)
			if resp.Body != nil {
				_ = resp.Body.Close() // R102 (auditor bug 248, P4): handshake-failure response body never closed — fd leak per failed dial
			}
		}
		w.state("DIAL-FAIL", detail)
		return
	}
	defer conn.Close()
	w.state("CONNECT", "")
	// marketSlugs is deliberately OMITTED (absent = all markets): the docs' balance example has no
	// such field, and an empty-array rejection is exactly the silent close we couldn't see before.
	sub := func(id, typ string) error {
		return conn.WriteJSON(map[string]any{"subscribe": map[string]any{
			"requestId": id, "subscriptionType": typ,
		}})
	}
	if err := func() error {
		for _, s := range [][2]string{{"ord-1", "SUBSCRIPTION_TYPE_ORDER"}, {"pos-1", "SUBSCRIPTION_TYPE_POSITION"}, {"bal-1", "SUBSCRIPTION_TYPE_ACCOUNT_BALANCE"}} {
			if e := sub(s[0], s[1]); e != nil {
				return e
			}
		}
		return nil
	}(); err != nil {
		w.state("SUBSCRIBE-FAIL", err.Error())
		return
	}
	w.state("SUBSCRIBED", "orders+positions+balance")
	w.mu.Lock()
	w.beginSessionLocked(time.Now())
	w.mu.Unlock()
	conn.SetReadLimit(1 << 21)
	_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))
		// LIVENESS TRUTH (probe 2026-07-02): a quiet account sends NO data frames — only our
		// ping/pong keeps the socket warm. Health must count pongs, else a perfectly healthy
		// idle stream gets declared dead after 2min and the UI falls back to "poll" for no reason.
		w.mu.Lock()
		w.lastMsg = time.Now()
		w.mu.Unlock()
		return nil
	})
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				// Unblock ReadMessage immediately on shutdown; waiting for the 75s read deadline
				// kept an authenticated session alive across quick suite restarts.
				_ = conn.Close()
				return
			case <-t.C:
				if conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)) != nil {
					return
				}
			}
		}
	}()
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			w.state("CLOSED", err.Error())
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(75 * time.Second))
		w.ingest(data)
	}
}

// WSProbe dials the private stream once, subscribes, and returns a transcript of the handshake +
// first frames (or the precise failure) — the truth-probe for "why is it in poll mode?".
func (c *Client) WSProbe(ctx context.Context) string {
	var b strings.Builder
	h := http.Header{}
	for k, v := range c.authHeaders("GET", "/v1/ws/private") {
		h.Set(k, v)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 12 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, "wss://api.polymarket.us/v1/ws/private", h)
	if err != nil {
		code := 0
		body := ""
		if resp != nil {
			code = resp.StatusCode
			buf := make([]byte, 300)
			n, _ := resp.Body.Read(buf)
			body = string(buf[:n])
			_ = resp.Body.Close() // R102 (auditor bug 248, P4)
		}
		fmt.Fprintf(&b, "DIAL FAILED: %v (HTTP %d) body=%s", err, code, body)
		return b.String()
	}
	defer conn.Close()
	b.WriteString("DIAL OK\n")
	for _, s := range [][2]string{{"ord-1", "SUBSCRIPTION_TYPE_ORDER"}, {"bal-1", "SUBSCRIPTION_TYPE_ACCOUNT_BALANCE"}} {
		if e := conn.WriteJSON(map[string]any{"subscribe": map[string]any{"requestId": s[0], "subscriptionType": s[1]}}); e != nil {
			fmt.Fprintf(&b, "SUBSCRIBE %s write err: %v\n", s[1], e)
			return b.String()
		}
	}
	b.WriteString("SUBSCRIBES SENT\n")
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for i := 0; i < 3; i++ {
		_, data, e := conn.ReadMessage()
		if e != nil {
			fmt.Fprintf(&b, "READ %d err: %v\n", i, e)
			return b.String()
		}
		fmt.Fprintf(&b, "FRAME %d: %s\n", i, truncate(string(data), 400))
	}
	return b.String()
}

// RawGET returns an authenticated GET's raw body — shape-truth probe (mirrors kalshi.RawGET).
func (c *Client) RawGET(ctx context.Context, path string) (string, int, error) {
	raw, code, err := c.do(ctx, http.MethodGet, path, nil)
	return string(raw), code, err
}

func (w *PrivateWS) ingest(data []byte) {
	var msg struct {
		OrderSnapshot *struct {
			Orders []pusOrderWire `json:"orders"`
			EOF    *bool          `json:"eof"`
		} `json:"orderSubscriptionSnapshot"`
		OrderUpdate *struct {
			Execution struct {
				Order            pusOrderWire    `json:"order"`
				LastShares       string          `json:"lastShares"`
				LastPx           pusAmt          `json:"lastPx"`
				Type             string          `json:"type"`
				TradeID          string          `json:"tradeId"`
				Transact         string          `json:"transactTime"`
				RejectReason     string          `json:"orderRejectReason"` // ORD_REJECT_REASON_* — the reject reason lives on the EXECUTION, not the order (docs)
				Text             string          `json:"text"`
				UnsolCancel      string          `json:"unsolicitedCancelReason"`
				Commission       pusOptionalAmt  `json:"commissionNotionalCollected"`
				CommissionSnake  pusOptionalAmt  `json:"commission_notional_collected"`
				Aggressor        pusOptionalBool `json:"aggressor"`
				IsAggressor      pusOptionalBool `json:"isAggressor"`
				IsAggressorSnake pusOptionalBool `json:"is_aggressor"`
			} `json:"execution"`
		} `json:"orderSubscriptionUpdate"`
		Position *json.RawMessage `json:"positionSubscription"`
		BalSnap  *struct {
			Balances []struct {
				CurrentBalance float64 `json:"currentBalance"`
				BuyingPower    float64 `json:"buyingPower"`
			} `json:"balances"`
		} `json:"accountBalancesSnapshot"`
		BalUpd *struct {
			BalanceChange struct {
				AfterBalance struct {
					CurrentBalance float64 `json:"currentBalance"`
					BuyingPower    float64 `json:"buyingPower"`
				} `json:"afterBalance"`
			} `json:"balanceChange"`
		} `json:"accountBalancesUpdate"`
	}
	if json.Unmarshal(data, &msg) != nil {
		return
	}
	notify := false
	w.mu.Lock()
	w.lastMsg = time.Now()
	switch {
	case msg.OrderSnapshot != nil:
		if w.snapOrders == nil {
			w.snapOrders = map[string]PUSOrder{}
		}
		for _, o := range msg.OrderSnapshot.Orders {
			f := o.flat()
			if f.ID != "" {
				w.snapOrders[f.ID] = f
			}
		}
		// The documented snapshot can arrive in chunks. Missing eof is accepted as
		// a legacy single-frame snapshot; explicit false keeps the staged map private.
		complete := msg.OrderSnapshot.EOF == nil || *msg.OrderSnapshot.EOF
		if complete {
			w.orders = w.snapOrders
			w.snapOrders = nil
			w.snapReady = true
			notify = true
		}
	case msg.OrderUpdate != nil:
		ex := msg.OrderUpdate.Execution
		f := ex.Order.flat()
		t := strings.TrimPrefix(ex.Type, "EXECUTION_TYPE_")
		switch t {
		case "CANCELED", "FILL", "REJECTED", "EXPIRED", "DONE_FOR_DAY":
			delete(w.orders, f.ID)
		default: // NEW / PARTIAL_FILL / REPLACE → upsert current view
			if f.ID != "" {
				w.orders[f.ID] = f
			}
		}
		// SILENT-DEATH VISIBILITY: terminal states with zero fill used to vanish without a trace —
		// the venue rejects/expires asynchronously after the 200 (docs + probe). Push the truth (with
		// the venue's own reason fields) to the order log via onDead. REJECTED always reports;
		// EXPIRED/CANCELED report when the terminal execution carried no fill.
		if t == "REJECTED" || ((t == "EXPIRED" || t == "CANCELED") && func() bool { q, _ := strconv.ParseFloat(ex.LastShares, 64); return q <= 0 }()) {
			if cb := w.onDead; cb != nil {
				reason := strings.TrimPrefix(ex.RejectReason, "ORD_REJECT_REASON_")
				if reason == "" {
					reason = strings.TrimPrefix(ex.UnsolCancel, "UNSOLICITED_CXL_REASON_")
				}
				go cb(f.Slug, f.Outcome, t, reason, ex.Text)
			}
		}
		if t == "FILL" || t == "PARTIAL_FILL" {
			qty, _ := strconv.ParseFloat(ex.LastShares, 64)
			yesPrice := ex.LastPx.f()
			commission := ex.Commission
			if !commission.Present {
				commission = ex.CommissionSnake
			}
			// The explicit user-relative aliases are authoritative. `aggressor` may
			// instead be the institutional Execution object, whose nested role is not
			// allowed to override an explicit retail isAggressor=false receipt. Keep
			// this precedence identical to Activities (REST).
			aggressor := ex.IsAggressor
			if !aggressor.Present {
				aggressor = ex.IsAggressorSnake
			}
			if !aggressor.Present {
				aggressor = ex.Aggressor
			}
			isAggressor, aggressorKnown := aggressor.Value, aggressor.Present
			w.fills = append(w.fills, PUSFill{
				OrderID: f.ID, Slug: f.Slug, Outcome: f.Outcome, Action: f.Action,
				Px: outcomePrice(yesPrice, f.Outcome), YesPrice: yesPrice,
				Qty: qty, Type: t, TradeID: ex.TradeID, Time: ex.Transact,
				CommissionUSD: commission.Value, CommissionKnown: commission.Present,
				Aggressor: isAggressor, AggressorKnown: aggressorKnown,
			})
			if len(w.fills) > 200 {
				// R76 (auditor bug 10): overflow used to be a SILENT drop of the oldest records —
				// real-money executions vanishing precisely when the drainer is wedged. Hand the
				// evicted slice to the overflow sink (server → pus_fills_overflow.jsonl, off this
				// goroutine); only without a handler does the old trim (documented loss) apply.
				if cb := w.onFillOverflow; cb != nil {
					evicted := append([]PUSFill(nil), w.fills[:len(w.fills)-200]...)
					go cb(evicted)
				}
				w.fills = append(w.fills[:0:0], w.fills[len(w.fills)-200:]...)
			}
			w.posDirty = true
		}
		notify = true
	case msg.Position != nil:
		w.posDirty = true
		notify = true
	case msg.BalSnap != nil:
		for _, b := range msg.BalSnap.Balances {
			w.bal, w.buyPow, w.balOK = b.CurrentBalance, b.BuyingPower, true
		}
		notify = true
	case msg.BalUpd != nil:
		ab := msg.BalUpd.BalanceChange.AfterBalance
		w.bal, w.buyPow, w.balOK = ab.CurrentBalance, ab.BuyingPower, true
		notify = true
	}
	fn := w.onEvent
	w.mu.Unlock()
	if notify && fn != nil {
		fn()
	}
}
