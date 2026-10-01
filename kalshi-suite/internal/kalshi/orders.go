package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// Portfolio list endpoints all support 1,000 rows/page. Keep one shared, explicit ceiling so a
	// malformed venue cursor can neither loop forever nor turn a safety read into an unbounded pull.
	portfolioPageLimit = 1000
	portfolioMaxPages  = 100
)

// portfolioPagePath builds a signed GET path. Signer.Headers deliberately strips the query string,
// while url.Values makes opaque cursors safe to carry between pages.
func portfolioPagePath(endpoint string, values url.Values, cursor string) string {
	q := make(url.Values, len(values)+1)
	for k, vs := range values {
		q[k] = append([]string(nil), vs...)
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	return endpoint + "?" + q.Encode()
}

// portfolioNextCursor validates venue pagination. A repeated cursor is an upstream schema/protocol
// failure, not end-of-data: return an error so account-risk callers fail closed instead of accepting
// a silently partial or duplicated snapshot.
func portfolioNextCursor(endpoint, current, next string, seen map[string]struct{}) (string, bool, error) {
	next = strings.TrimSpace(next)
	if next == "" {
		return "", true, nil
	}
	if next == current {
		return "", false, fmt.Errorf("kalshi %s repeated pagination cursor %q", endpoint, next)
	}
	if _, ok := seen[next]; ok {
		return "", false, fmt.Errorf("kalshi %s cycled pagination cursor %q", endpoint, next)
	}
	seen[next] = struct{}{}
	return next, false, nil
}

// OrderRequest is the body for POST /trade-api/v2/portfolio/events/orders (Kalshi V2).
// V2 quotes everything from the YES leg via a single book Side:
//
//	Side "bid" = BUY YES, "ask" = SELL YES (selling YES ≈ buying NO at 1-price).
//
// Count and Price are fixed-point STRINGS: Count in contracts ("1", "10.00"); Price in
// DOLLARS ("0.02" = 2¢, "0.56" = 56¢). TimeInForce and SelfTradePreventionType are required.
//
//	TimeInForce:             fill_or_kill | good_till_canceled | immediate_or_cancel
//	SelfTradePreventionType: taker_at_cross | maker
//
// PostOnly=true makes a limit order maker-only (rejected rather than crossing).
type OrderRequest struct {
	Ticker                  string `json:"ticker"`
	ClientOrderID           string `json:"client_order_id"`
	Side                    string `json:"side"`
	Count                   string `json:"count"`
	Price                   string `json:"price"`
	TimeInForce             string `json:"time_in_force"`
	SelfTradePreventionType string `json:"self_trade_prevention_type"`
	PostOnly                bool   `json:"post_only,omitempty"`
	ExpirationTime          int64  `json:"expiration_time,omitempty"`
	ReduceOnly              bool   `json:"reduce_only,omitempty"`
	CancelOrderOnPause      bool   `json:"cancel_order_on_pause,omitempty"` // V2 spec: auto-cancel open orders if the exchange pauses — free tail-risk protection for resting maker orders
}

// CreateOrderResult is the V2 create response (returned directly, not wrapped in {"order":…}).
// Fill/price fields are fixed-point strings; AverageFillPrice/AverageFeePaid are present only
// when FillCount > 0.
type CreateOrderResult struct {
	OrderID          string `json:"order_id"`
	ClientOrderID    string `json:"client_order_id"`
	FillCount        string `json:"fill_count"`
	RemainingCount   string `json:"remaining_count"`
	AverageFillPrice string `json:"average_fill_price"`
	AverageFeePaid   string `json:"average_fee_paid"`
	TsMs             int64  `json:"ts_ms"`
}

// Order is a row from GET /portfolio/orders (resting/recent orders list). String fields plus
// flexFloat price/size (fixed-point "_dollars"/"_fp" strings AND legacy ints both decode) so the
// resting-orders table can show price + remaining size, not bare tickers.
type Order struct {
	OrderID       string    `json:"order_id"`
	ClientOrderID string    `json:"client_order_id"`
	Ticker        string    `json:"ticker"`
	Status        string    `json:"status"`
	Type          string    `json:"type"`
	Side          string    `json:"side"`
	Action        string    `json:"action"`
	OutcomeSide   string    `json:"outcome_side"`
	BookSide      string    `json:"book_side"`
	YesPriceD     flexFloat `json:"yes_price_dollars"`
	NoPriceD      flexFloat `json:"no_price_dollars"`
	FillFP        flexFloat `json:"fill_count_fp"`
	RemainFP      flexFloat `json:"remaining_count_fp"`
	InitialFP     flexFloat `json:"initial_count_fp"`
	CreatedTime   string    `json:"created_time"`
	UpdatedTime   string    `json:"last_update_time"`
	fillKnown     bool
	initialKnown  bool
	outcomeKnown  bool
	bookKnown     bool
	statusKnown   bool
	typeKnown     bool
}

func strictOrderNumber(raw json.RawMessage, positive bool) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return false
	}
	var number float64
	switch v := value.(type) {
	case string:
		var err error
		number, err = strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return false
		}
	case float64:
		number = v
	default:
		return false
	}
	if math.IsNaN(number) || math.IsInf(number, 0) || number < 0 {
		return false
	}
	return !positive || number > 0
}

func strictOrderEnum(raw json.RawMessage, allowed ...string) bool {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return false
	}
	value = strings.ToLower(strings.TrimSpace(value))
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func (o *Order) UnmarshalJSON(data []byte) error {
	type wireOrder Order
	var wire wireOrder
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*o = Order(wire)
	o.fillKnown = strictOrderNumber(fields["fill_count_fp"], false)
	o.initialKnown = strictOrderNumber(fields["initial_count_fp"], true)
	o.outcomeKnown = strictOrderEnum(fields["outcome_side"], "yes", "no")
	o.bookKnown = strictOrderEnum(fields["book_side"], "bid", "ask")
	o.statusKnown = strictOrderEnum(fields["status"], "resting", "canceled", "executed")
	o.typeKnown = strictOrderEnum(fields["type"], "limit")
	return nil
}

func (o Order) YesPrice() float64  { return o.YesPriceD.Float() }
func (o Order) NoPrice() float64   { return o.NoPriceD.Float() }
func (o Order) Remaining() float64 { return o.RemainFP.Float() }
func (o Order) Filled() float64    { return o.FillFP.Float() }
func (o Order) Initial() float64   { return o.InitialFP.Float() }

// CurrentExecutionSchemaKnown reports whether the required directional/count fields used for
// crash-safe execution recovery were actually present on the wire. Zero is a valid fill count,
// so value-only checks cannot distinguish an unfilled FOK from schema drift.
func (o Order) CurrentExecutionSchemaKnown() bool {
	return o.fillKnown && o.initialKnown && o.outcomeKnown && o.bookKnown && o.statusKnown && o.typeKnown
}

// CurrentRestingOutcomeSide returns the directional side of a current-schema resting order.
// Kalshi's successor order schema carries the same direction twice: outcome_side=yes/no and
// book_side=bid/ask (bid == yes, ask == no). Both fields must be present and agree. Callers that
// meter real-money risk must not fall back to the deprecated side field because a missing or
// contradictory successor direction is schema uncertainty, not zero exposure.
func (o Order) CurrentRestingOutcomeSide() (string, bool) {
	if !o.outcomeKnown || !o.bookKnown {
		return "", false
	}
	outcome := strings.ToLower(strings.TrimSpace(o.OutcomeSide))
	book := strings.ToLower(strings.TrimSpace(o.BookSide))
	if (outcome == "yes" && book == "bid") || (outcome == "no" && book == "ask") {
		return outcome, true
	}
	return "", false
}

type ordersResp struct {
	Orders []Order `json:"orders"`
	Cursor string  `json:"cursor"`
}

// UnmarshalJSON makes the required envelope field part of schema truth. A successful HTTP 200
// containing `{}` used to decode as an empty account and could make queue liveness claim there
// were naturally no resting orders. An explicit empty array remains a valid healthy-empty read.
func (r *ordersResp) UnmarshalJSON(data []byte) error {
	var raw struct {
		Orders json.RawMessage `json:"orders"`
		Cursor string          `json:"cursor"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.Orders == nil || string(raw.Orders) == "null" {
		return fmt.Errorf("kalshi orders schema: missing required orders array")
	}
	if err := json.Unmarshal(raw.Orders, &r.Orders); err != nil {
		return fmt.Errorf("kalshi orders schema: %w", err)
	}
	r.Cursor = raw.Cursor
	return nil
}

// ArmProdWrites permits order writes on a PROD client. Off by default; only a client explicitly armed
// (under the KALSHI_LIVE_ARMED gate) may place REAL-MONEY orders.
func (c *Client) ArmProdWrites() { c.writesArmed = true }

// DisarmProdWrites revokes real-money ORDER-CREATE permission (kill switch / manual disarm).
// R90 bug 149 (auditor DO-THIS 4): cancels are deliberately NO LONGER blocked by disarm —
// canceling can only reduce exposure, and the old gate stranded resting real-money orders
// exactly during the outage that tripped the kill switch (the sweep's GetOrders failed → all
// cancels skipped → disarm then made every later cancel impossible until a manual re-arm).
func (c *Client) DisarmProdWrites() { c.writesArmed = false }

// writeAllowed gates every order WRITE: always allowed on the demo sandbox (mock funds); allowed on
// prod ONLY when the client has been explicitly armed (real money). Anything else is refused, so a
// prod client can never place a real order by accident.
func (c *Client) writeAllowed() error {
	if c.baseURL == BaseDemo {
		return nil
	}
	if c.baseURL == BaseProd && c.writesArmed {
		return nil
	}
	return fmt.Errorf("refusing order write: allowed on demo, or prod only when explicitly armed (baseURL=%s armed=%v)", c.baseURL, c.writesArmed)
}

// CreateOrder submits a V2 order and returns the create result. Demo always; prod only when armed.
func (c *Client) CreateOrder(ctx context.Context, req OrderRequest) (*CreateOrderResult, error) {
	if err := c.writeAllowed(); err != nil {
		return nil, err
	}
	var resp CreateOrderResult
	if err := c.doWithBody(ctx, http.MethodPost, "/portfolio/events/orders", req, &resp, true); err != nil {
		return nil, err
	}
	return &resp, nil
}

// NOTE (audit §8): the legacy V3 order path (POST /portfolio/orders with action/side/yes_price)
// was REMOVED — Kalshi deprecated that shape May 2026 in favor of /portfolio/events/orders (the V2
// shape above), which is the endpoint the live path actually uses. Keeping a dead deprecated writer
// around invited someone to call it; the order-preview console now renders the V2 shape too.

// Armed reports whether this client is armed for prod writes (real-money orders).
func (c *Client) Armed() bool { return c.writesArmed }

// cancelAllowed gates order CANCELS: demo always; prod ALWAYS — a cancel can only reduce
// exposure, and blocking it during incidents (kill-switch trips, disarms) is exactly how resting
// orders get stranded (R90 bug 149). Order CREATION stays behind writeAllowed's explicit arming.
func (c *Client) cancelAllowed() error {
	if c.baseURL == BaseDemo || c.baseURL == BaseProd {
		return nil
	}
	return fmt.Errorf("refusing cancel on unrecognized baseURL %s", c.baseURL)
}

// CancelOrder cancels a resting order by ID (V2 path). Demo always; prod always (risk-reducing —
// R90 bug 149: cancels are exempt from the arm/disarm gate).
func (c *Client) CancelOrder(ctx context.Context, orderID string) error {
	if err := c.cancelAllowed(); err != nil {
		return err
	}
	return c.do(ctx, http.MethodDelete, "/portfolio/events/orders/"+orderID, nil, true)
}

// BatchCancelOrders cancels many resting orders in ONE request via the V2 batch endpoint
// (R90 — auditor bugs 135/149, edge 28 execution modernization).
//
//	DELETE /trade-api/v2/portfolio/events/orders/batched  body {"orders":[{"order_id":…},…]}
//
// The response is PER-ITEM (each entry carries the canceled order or its own error), so one bad
// id can never fail the whole sweep. Rate note: batch endpoints are billed per item (a cancel
// costs 2 tokens each; docs: "no token savings") — the win is ONE HTTP round trip instead of N
// during incident sweeps, when the venue is slowest. Batch size scales with the account's write
// budget; callers here sweep well under any plausible cap.
// Docs (fetched 2026-07-06): https://docs.kalshi.com/api-reference/orders/batch-cancel-orders-v2
// (V2 family added Apr 22 2026; legacy DELETE /portfolio/orders/batched remains supported, its
// top-level ids:[] shape is deprecated) and https://docs.kalshi.com/getting_started/rate_limits.
// Returns a per-id failure map (nil when everything canceled) plus any transport-level error.
func (c *Client) BatchCancelOrders(ctx context.Context, ids []string) (map[string]string, error) {
	if err := c.cancelAllowed(); err != nil {
		return nil, err
	}
	type item struct {
		OrderID string `json:"order_id"`
	}
	req := struct {
		Orders []item `json:"orders"`
	}{}
	for _, id := range ids {
		if strings.TrimSpace(id) != "" {
			req.Orders = append(req.Orders, item{OrderID: id})
		}
	}
	if len(req.Orders) == 0 {
		return nil, nil
	}
	var resp struct {
		Orders []struct {
			OrderID string `json:"order_id"`
			Error   *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"orders"`
	}
	if err := c.doWithBody(ctx, http.MethodDelete, "/portfolio/events/orders/batched", req, &resp, true); err != nil {
		return nil, err
	}
	var failed map[string]string
	for _, o := range resp.Orders {
		if o.Error != nil {
			if failed == nil {
				failed = map[string]string{}
			}
			failed[o.OrderID] = strings.TrimSpace(o.Error.Code + " " + o.Error.Message)
		}
	}
	return failed, nil
}

// GetOrders lists ALL resting orders across every cursor page (read-only). Every current caller uses
// this as venue-truth for exposure/cancel safety; status=resting avoids re-downloading canceled and
// executed history once per second while armed.
func (c *Client) GetOrders(ctx context.Context) ([]Order, error) {
	const endpoint = "/portfolio/orders"
	values := url.Values{"limit": {strconv.Itoa(portfolioPageLimit)}, "status": {"resting"}}
	out := make([]Order, 0)
	cursor := ""
	seen := map[string]struct{}{}
	for page := 0; page < portfolioMaxPages; page++ {
		var resp ordersResp
		if err := c.do(ctx, http.MethodGet, portfolioPagePath(endpoint, values, cursor), &resp, true); err != nil {
			return nil, err
		}
		out = append(out, resp.Orders...)
		next, done, err := portfolioNextCursor(endpoint, cursor, resp.Cursor, seen)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("kalshi %s pagination exceeded %d pages", endpoint, portfolioMaxPages)
}

// GetOrdersByClientOrderID is the crash-recovery reader for a durable pre-send order intent.
// Unlike GetOrders, it deliberately omits status=resting: a fill-or-kill order normally vanishes
// from the resting set immediately. Kalshi does not expose client_order_id as a server-side query,
// so this method fully paginates the ticker/time-bounded history and filters locally. A caller can
// therefore distinguish one exact venue receipt from an incomplete or ambiguous lookup without
// downloading unrelated account history or treating an empty first page as final.
func (c *Client) GetOrdersByClientOrderID(ctx context.Context, ticker, clientOrderID string,
	minTS time.Time) ([]Order, error) {
	ticker = strings.TrimSpace(ticker)
	clientOrderID = strings.TrimSpace(clientOrderID)
	if ticker == "" || clientOrderID == "" || minTS.IsZero() {
		return nil, fmt.Errorf("kalshi client-order recovery requires ticker, client_order_id, and min_ts")
	}
	const endpoint = "/portfolio/orders"
	values := url.Values{
		"limit":  {strconv.Itoa(portfolioPageLimit)},
		"ticker": {ticker},
		"min_ts": {strconv.FormatInt(minTS.UTC().Unix(), 10)},
	}
	out := make([]Order, 0)
	cursor := ""
	seen := map[string]struct{}{}
	for page := 0; page < portfolioMaxPages; page++ {
		var resp ordersResp
		if err := c.do(ctx, http.MethodGet, portfolioPagePath(endpoint, values, cursor), &resp, true); err != nil {
			return nil, err
		}
		for _, order := range resp.Orders {
			if strings.TrimSpace(order.Ticker) != ticker {
				return nil, fmt.Errorf("kalshi orders schema: ticker filter %q returned ticker %q", ticker, order.Ticker)
			}
			if strings.TrimSpace(order.ClientOrderID) == clientOrderID {
				out = append(out, order)
			}
		}
		next, done, err := portfolioNextCursor(endpoint+"?ticker="+ticker, cursor, resp.Cursor, seen)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("kalshi %s ticker=%s client_order_id=%s pagination exceeded %d pages",
		endpoint, ticker, clientOrderID, portfolioMaxPages)
}

// MarketPosition — FIXED-POINT TRUTH (probed live 2026-07-02): Kalshi's positions endpoint now
// returns decimal STRINGS (position_fp, market_exposure_dollars, realized_pnl_dollars, …); the
// legacy integer-cent fields are GONE from responses. That silent shape change zeroed the live
// portfolio, exposure and the already-held dedup all at once. This struct parses BOTH shapes and
// consumers use ONLY the normalized accessors below — the next rename breaks one place, loudly.
type MarketPosition struct {
	Ticker        string `json:"ticker"`
	LastUpdatedTS string `json:"last_updated_ts"`
	// current fixed-point shape (decimal strings)
	PositionFP   flexFloat         `json:"position_fp"`
	ExposureUSDs flexFloat         `json:"market_exposure_dollars"`
	RealizedUSDs flexFloat         `json:"realized_pnl_dollars"`
	FeesUSDs     optionalFlexFloat `json:"fees_paid_dollars"`
	TradedUSDs   flexFloat         `json:"total_traded_dollars"`
	// legacy integer-cent shape (fallback only)
	Position       int   `json:"position"`
	MarketExposure int64 `json:"market_exposure"`
	RealizedPnl    int64 `json:"realized_pnl"`
}

// PositionQty returns net contracts (fixed-point first, legacy fallback).
func (p MarketPosition) PositionQty() float64 {
	if v := p.PositionFP.Float(); v != 0 {
		return v
	}
	return float64(p.Position)
}

// ExposureUSD returns market exposure in dollars.
func (p MarketPosition) ExposureUSD() float64 {
	if v := p.ExposureUSDs.Float(); v != 0 {
		return v
	}
	return float64(p.MarketExposure) / 100
}

// RealizedUSD returns realized P&L in dollars.
func (p MarketPosition) RealizedUSD() float64 {
	if v := p.RealizedUSDs.Float(); v != 0 {
		return v
	}
	return float64(p.RealizedPnl) / 100
}

// FeesUSD returns cumulative fees paid for the market.
func (p MarketPosition) FeesUSD() float64 { return p.FeesUSDs.Float() }
func (p MarketPosition) FeesKnown() bool  { return p.FeesUSDs.Present() }

type positionsResp struct {
	MarketPositions []MarketPosition `json:"market_positions"`
	Cursor          string           `json:"cursor"`
}

// GetPositions returns every non-flat per-market position across all cursor pages (read-only).
// count_filter=position is the venue-supported open-position filter; flat lifetime-history rows do
// not belong in exposure, held-market dedup, or the live account view.
func (c *Client) GetPositions(ctx context.Context) ([]MarketPosition, error) {
	return c.getPositionsByFilter(ctx, "position")
}

// GetTradedPositions returns the durable cumulative account ledger: every market with non-zero
// total_traded, including flat/closed positions. Unlike GetPositions, closing a market does not
// remove its cumulative realized_pnl_dollars and fees_paid_dollars from this view.
func (c *Client) GetTradedPositions(ctx context.Context) ([]MarketPosition, error) {
	return c.getPositionsByFilter(ctx, "total_traded")
}

func (c *Client) getPositionsByFilter(ctx context.Context, countFilter string) ([]MarketPosition, error) {
	const endpoint = "/portfolio/positions"
	values := url.Values{"limit": {strconv.Itoa(portfolioPageLimit)}, "count_filter": {countFilter}}
	var out []MarketPosition
	cursor := ""
	seen := map[string]struct{}{}
	for page := 0; page < portfolioMaxPages; page++ {
		var resp positionsResp
		if err := c.do(ctx, http.MethodGet, portfolioPagePath(endpoint, values, cursor), &resp, true); err != nil {
			return nil, err
		}
		out = append(out, resp.MarketPositions...)
		next, done, err := portfolioNextCursor(endpoint, cursor, resp.Cursor, seen)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("kalshi %s pagination exceeded %d pages", endpoint, portfolioMaxPages)
}

// Fill is one row of GET /portfolio/fills — an EXECUTED trade (real fills).
//
// BOTH-SHAPE PARSER (SCHEMA_AUDIT F1/F9, docs+probe verified 2026-07-04): the legacy integer-cent
// fields (side/action/count/yes_price/no_price) are DEPRECATED ("will not be removed before May 14,
// 2026" per the OpenAPI spec) in favor of the fixed-point successor shape:
//
//	count_fp / yes_price_dollars / no_price_dollars — decimal STRINGS
//	fee_cost                                        — per-fill fee in fixed-point DOLLARS
//	outcome_side (yes|no)                           — directional exposure: buy-yes/sell-no ⇒ "yes",
//	                                                  buy-no/sell-yes ⇒ "no" (replaces side+action)
//	book_side (bid|ask)                             — same bit in book vocabulary (bid ≡ yes)
//
// Positions flipped to the fp shape in Apr 2026 and zeroed our decode silently; this struct parses
// BOTH shapes and consumers use ONLY the normalized accessors below — so the legacy removal breaks
// one place, loudly, instead of zeroing fill stamping + fill labels.
type Fill struct {
	FillID  string `json:"fill_id"`
	Ticker  string `json:"ticker"`
	OrderID string `json:"order_id"`
	// legacy shape (deprecated; fallback only)
	Side     string `json:"side"`
	Action   string `json:"action"`
	Count    int    `json:"count"`
	YesPrice int    `json:"yes_price"`
	NoPrice  int    `json:"no_price"`
	// successor fixed-point shape (preferred when present)
	OutcomeSide string            `json:"outcome_side"`
	BookSide    string            `json:"book_side"`
	CountFP     flexFloat         `json:"count_fp"`
	YesPriceD   flexFloat         `json:"yes_price_dollars"`
	NoPriceD    flexFloat         `json:"no_price_dollars"`
	FeeCost     optionalFlexFloat `json:"fee_cost"`
	IsTaker     bool              `json:"is_taker"`
	CreatedTime string            `json:"created_time"`
}

// optionalFlexFloat preserves the difference between a genuine zero and an absent/null field.
// That distinction is mandatory for fee-net LIVE reconciliation: a missing fee may never be
// silently booked as a real $0 fee.
type optionalFlexFloat struct {
	value   float64
	present bool
}

func (f *optionalFlexFloat) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	f.value, f.present = 0, false
	if s == "" || s == "null" || s == `""` {
		return nil
	}
	v, err := strconv.ParseFloat(strings.Trim(s, `"`), 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	f.value, f.present = v, true
	return nil
}

func (f optionalFlexFloat) Float() float64 { return f.value }
func (f optionalFlexFloat) Present() bool  { return f.present }

// Qty returns contracts filled (fixed-point first, legacy int fallback).
func (f Fill) Qty() float64 {
	if v := f.CountFP.Float(); v != 0 {
		return v
	}
	return float64(f.Count)
}

// YesPriceUSD returns the YES fill price in dollars (fp first, legacy cents fallback).
func (f Fill) YesPriceUSD() float64 {
	if v := f.YesPriceD.Float(); v != 0 {
		return v
	}
	return float64(f.YesPrice) / 100
}

// NoPriceUSD returns the NO fill price in dollars (fp first, legacy cents fallback).
func (f Fill) NoPriceUSD() float64 {
	if v := f.NoPriceD.Float(); v != 0 {
		return v
	}
	return float64(f.NoPrice) / 100
}

// SideYesNo returns the fill's directional side "yes"/"no", preferring the canonical
// outcome_side, then book_side (bid ≡ yes / ask ≡ no), then the legacy side field.
func (f Fill) SideYesNo() string {
	if s := strings.ToLower(strings.TrimSpace(f.OutcomeSide)); s == "yes" || s == "no" {
		return s
	}
	switch strings.ToLower(strings.TrimSpace(f.BookSide)) {
	case "bid":
		return "yes"
	case "ask":
		return "no"
	}
	return strings.ToLower(strings.TrimSpace(f.Side))
}

// IsEntry reports whether this fill should stamp a signal's ENTRY price. With the legacy action
// field present it means exactly "buy". The successor shape has NO buy/sell bit — outcome_side
// folds buy-yes and sell-no into "yes" — so once legacy action disappears every fill counts as an
// entry on its outcome side (the traded price for that side at that moment; UpdateSignalFill's
// open-row + fill_price=0 guards bound any mislabeling).
func (f Fill) IsEntry() bool {
	if a := strings.TrimSpace(f.Action); a != "" {
		return strings.EqualFold(a, "buy")
	}
	// outcome_side is directional exposure, not a buy/sell bit. Without action (or
	// caller-owned order provenance) a closing sell cannot safely be labeled an entry.
	return false
}

// FeeUSD returns the per-fill fee in dollars (successor fee_cost only; 0 when absent).
func (f Fill) FeeUSD() float64 { return f.FeeCost.Float() }

// FeeKnown distinguishes an explicitly supplied zero/rebate-free fill fee from a missing field.
func (f Fill) FeeKnown() bool { return f.FeeCost.Present() }

type fillsResp struct {
	Fills  []Fill `json:"fills"`
	Cursor string `json:"cursor"`
}

// GetFills returns the account's recent executed fills across all cursor pages (read-only). The
// rolling 24-hour filter covers the 10-minute fill-label poll and the live dashboard while avoiding
// a full account-history download every 45 seconds; older fills live in the suite's durable ledger.
func (c *Client) GetFills(ctx context.Context) ([]Fill, error) {
	const endpoint = "/portfolio/fills"
	values := url.Values{
		"limit":  {strconv.Itoa(portfolioPageLimit)},
		"min_ts": {strconv.FormatInt(time.Now().Add(-24*time.Hour).Unix(), 10)},
	}
	var out []Fill
	cursor := ""
	seen := map[string]struct{}{}
	for page := 0; page < portfolioMaxPages; page++ {
		var resp fillsResp
		if err := c.do(ctx, http.MethodGet, portfolioPagePath(endpoint, values, cursor), &resp, true); err != nil {
			return nil, err
		}
		out = append(out, resp.Fills...)
		next, done, err := portfolioNextCursor(endpoint, cursor, resp.Cursor, seen)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("kalshi %s pagination exceeded %d pages", endpoint, portfolioMaxPages)
}

// GetFillsForOrder is the restart-safe execution reader. Unlike GetFills' rolling 24-hour UI/feed
// window, this order-scoped query has no time cutoff and follows every cursor page. Every returned
// row must carry the requested immutable order id or the response is rejected as schema drift.
func (c *Client) GetFillsForOrder(ctx context.Context, orderID string) ([]Fill, error) {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return nil, fmt.Errorf("empty fill order id")
	}
	const endpoint = "/portfolio/fills"
	values := url.Values{"limit": {strconv.Itoa(portfolioPageLimit)}, "order_id": {orderID}}
	var out []Fill
	cursor := ""
	seen := map[string]struct{}{}
	for page := 0; page < portfolioMaxPages; page++ {
		var resp fillsResp
		if err := c.do(ctx, http.MethodGet, portfolioPagePath(endpoint, values, cursor), &resp, true); err != nil {
			return nil, err
		}
		for _, fill := range resp.Fills {
			if strings.TrimSpace(fill.OrderID) != orderID {
				return nil, fmt.Errorf("kalshi fills schema: order filter %q returned order %q", orderID, fill.OrderID)
			}
			out = append(out, fill)
		}
		next, done, err := portfolioNextCursor(endpoint+"?order_id="+orderID, cursor, resp.Cursor, seen)
		if err != nil {
			return nil, err
		}
		if done {
			return out, nil
		}
		cursor = next
	}
	return nil, fmt.Errorf("kalshi %s order_id=%s pagination exceeded %d pages", endpoint, orderID, portfolioMaxPages)
}

// Settlement is one row of GET /portfolio/settlements (fixed-point era — the legacy cent fields
// were REMOVED Apr 2026; docs/API-RESEARCH-2026-07.md §1.5). R106 (auditor bug 284): live SINGLE
// positions used to settle with ZERO suite-side persistence — this feed is the venue-truth ledger
// the live-settlement journal polls. `revenue` is documented in CENTS; consumers use RevenueUSD.
type Settlement struct {
	Ticker       string            `json:"ticker"`
	MarketResult string            `json:"market_result"`
	YesCount     flexFloat         `json:"yes_count_fp"`
	NoCount      flexFloat         `json:"no_count_fp"`
	YesTotalCost flexFloat         `json:"yes_total_cost_dollars"`
	NoTotalCost  flexFloat         `json:"no_total_cost_dollars"`
	Revenue      flexFloat         `json:"revenue"`
	FeeCost      optionalFlexFloat `json:"fee_cost"`
	SettledTime  string            `json:"settled_time"`
}

// RevenueUSD converts the documented cents field to dollars.
func (st Settlement) RevenueUSD() float64 { return st.Revenue.Float() / 100 }

// FeeUSD/FeeKnown expose the settlement's authoritative fee receipt. A missing fee may not be
// treated as a real zero in daily-loss or settlement P&L accounting.
func (st Settlement) FeeUSD() float64 { return st.FeeCost.Float() }
func (st Settlement) FeeKnown() bool  { return st.FeeCost.Present() }

// GetSettlements fetches the newest settlement rows (portfolio priority lane via c.do).
func (c *Client) GetSettlements(ctx context.Context, limit int) ([]Settlement, error) {
	rows, _, err := c.GetSettlementsWindow(ctx, limit)
	return rows, err
}

// GetSettlementsWindow additionally reports whether the authenticated cursor reached the end of
// the venue ledger. Callers rebuilding a durable loss rail need that distinction: receiving the
// requested cap is not proof that older same-day settlements do not exist.
func (c *Client) GetSettlementsWindow(ctx context.Context, limit int) ([]Settlement, bool, error) {
	if limit <= 0 {
		limit = 100
	}
	maxRows := portfolioPageLimit * portfolioMaxPages
	if limit > maxRows {
		limit = maxRows
	}
	const endpoint = "/portfolio/settlements"
	var out []Settlement
	cursor := ""
	seen := map[string]struct{}{}
	for page := 0; page < portfolioMaxPages && len(out) < limit; page++ {
		pageLimit := limit - len(out)
		if pageLimit > portfolioPageLimit {
			pageLimit = portfolioPageLimit
		}
		values := url.Values{"limit": {strconv.Itoa(pageLimit)}}
		var resp struct {
			Settlements []Settlement `json:"settlements"`
			Cursor      string       `json:"cursor"`
		}
		if err := c.do(ctx, http.MethodGet, portfolioPagePath(endpoint, values, cursor), &resp, true); err != nil {
			return nil, false, err
		}
		out = append(out, resp.Settlements...)
		if len(out) >= limit {
			return out[:limit], strings.TrimSpace(resp.Cursor) == "", nil
		}
		next, done, err := portfolioNextCursor(endpoint, cursor, resp.Cursor, seen)
		if err != nil {
			return nil, false, err
		}
		if done {
			return out, true, nil
		}
		cursor = next
	}
	return nil, false, fmt.Errorf("kalshi %s pagination exceeded %d pages", endpoint, portfolioMaxPages)
}
