// Package polymarketus is a thin client for the Polymarket US RETAIL developer API
// (https://api.polymarket.us). Authentication is an Ed25519 request signature:
//
//	message   = <unix_ms> + <HTTP METHOD> + <path>     e.g. "1719000000000GET/v1/portfolio/positions"
//	signature = base64( Ed25519_sign(seed, message) )
//
// sent as headers X-PM-Access-Key (Key ID), X-PM-Timestamp (ms), X-PM-Signature (base64). The Secret
// Key from polymarket.us/developer is base64; its first 32 bytes are the Ed25519 seed. Timestamps must
// be within 30s of server time, so keep the clock NTP-synced.
//
// SCOPE OF THIS FILE: read + VERIFY only (positions). Order entry is a deliberate PLACEHOLDER
// (PlaceLiveOrder) until the key is verified with the `polyus` command and a signal->market-slug
// mapping is wired.
package polymarketus

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// BaseURL is the authenticated retail API (trading / portfolio / account). Public market data lives on
// gateway.polymarket.us and needs no key.
const BaseURL = "https://api.polymarket.us"

// Client signs Polymarket US retail API requests with Ed25519. Safe for concurrent use (stateless).
type Client struct {
	baseURL string
	keyID   string
	priv    ed25519.PrivateKey
	http    *http.Client
}

// NewClient builds a client from the Key ID and the base64 Secret Key shown in the developer portal.
// Matches the reference impl: base64-decode the secret and use its first 32 bytes as the Ed25519 seed.
func NewClient(keyID, secretB64 string, timeout time.Duration) (*Client, error) {
	keyID = strings.TrimSpace(keyID)
	if keyID == "" {
		return nil, errors.New("empty key id")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(secretB64))
	if err != nil {
		if raw, err = base64.RawStdEncoding.DecodeString(strings.TrimSpace(secretB64)); err != nil {
			return nil, fmt.Errorf("decode secret key (expected base64 from the developer portal): %w", err)
		}
	}
	if len(raw) < ed25519.SeedSize {
		return nil, fmt.Errorf("secret key decodes to %d bytes; need >= %d", len(raw), ed25519.SeedSize)
	}
	return &Client{
		baseURL: BaseURL,
		keyID:   keyID,
		priv:    ed25519.NewKeyFromSeed(raw[:ed25519.SeedSize]),
		// LATENCY (R17): warm conn pool + TLS session cache — see kalshi.NewClient for the numbers.
		// Extra relevant here: reusing connections keeps us pinned to ONE Cloudflare colo, so the
		// 20 req/s edge counter we pace against is the one actually counting us.
		http: &http.Client{Timeout: timeout, Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 8,
			IdleConnTimeout:     80 * time.Second,
			TLSHandshakeTimeout: 5 * time.Second,
			ForceAttemptHTTP2:   true,
			TLSClientConfig:     &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(32)},
		}},
	}, nil
}

func (c *Client) KeyID() string { return c.keyID }

// signaturePath returns the URL path covered by the PolyUS Ed25519 signature. The official retail
// authentication contract is timestamp + method + path; query parameters are transmitted but are
// not part of that path. Keeping the split here prevents a paginated/account-filtered request from
// accidentally signing its request-target (path + query), which production rejects with HTTP 401.
func signaturePath(requestTarget string) string {
	if i := strings.IndexByte(requestTarget, '?'); i >= 0 {
		return requestTarget[:i]
	}
	return requestTarget
}

// authHeaders signs "<ts_ms><METHOD><path>" with Ed25519 and returns the X-PM-* headers.
func (c *Client) authHeaders(method, requestTarget string) map[string]string {
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	sig := ed25519.Sign(c.priv, []byte(ts+method+signaturePath(requestTarget)))
	return map[string]string{
		"X-PM-Access-Key": c.keyID,
		"X-PM-Timestamp":  ts,
		"X-PM-Signature":  base64.StdEncoding.EncodeToString(sig),
		"Content-Type":    "application/json",
	}
}

// RESTObs — R107 latency monitor: optional per-request duration observer (server wires it; nil = no-op).
var RESTObs func(time.Duration)

// do performs a signed request. requestTarget is the exact path + optional query sent on the wire;
// authHeaders signs only its path component, per the official PolyUS authentication contract.
func (c *Client) do(ctx context.Context, method, requestTarget string, body []byte) ([]byte, int, error) {
	if RESTObs != nil { // R107: passive RTT observer
		t0 := time.Now()
		defer func() { RESTObs(time.Since(t0)) }()
	}
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+requestTarget, r)
	if err != nil {
		return nil, 0, err
	}
	for k, v := range c.authHeaders(method, requestTarget) {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return raw, resp.StatusCode, nil
}

// Positions returns the account's open positions (GET /v1/portfolio/positions). A 200 here proves the
// Key ID + Ed25519 signing work end-to-end (authenticated, account-scoped read — no money moves).
func (c *Client) Positions(ctx context.Context) ([]byte, int, error) {
	return c.do(ctx, http.MethodGet, "/v1/portfolio/positions", nil)
}

// LiveOrder is the retail order request for POST /v1/orders. Retail markets are addressed by
// slug + outcome (YES/NO), not the institutional tec-* symbology.
type LiveOrder struct {
	MarketSlug          string  `json:"market_slug"`
	Outcome             string  `json:"outcome"` // YES | NO
	Side                string  `json:"side"`    // BUY | SELL
	Price               float64 `json:"price"`   // 0..1 (the outcome side's price)
	Size                float64 `json:"size"`    // contracts
	PostOnly            bool    `json:"post_only"`
	FillOrKill          bool    `json:"fill_or_kill"` // require the complete quantity immediately or cancel all
	Synchronous         bool    `json:"synchronous_execution,omitempty"`
	MaxBlockTimeSeconds int     `json:"max_block_time_seconds,omitempty"` // official Retail limit: <=10s
}

// LiveOrderExecution is the exact synchronous Retail execution receipt. Prices are normalized to
// the named YES/NO outcome while Yes* preserves the exchange's universal YES denomination.
type LiveOrderExecution struct {
	ID, OrderID, TradeID, MarketSlug, Intent, Outcome, Action, Type, State, TIF, TransactTime string
	OrderQty, CumQty, LeavesQty, LastQty                                                      float64
	OrderPrice, YesOrderPrice, AveragePrice, YesAveragePrice, LastPrice, YesLastPrice         float64
	CommissionUSD                                                                             float64
	CommissionKnown                                                                           bool
}

type LiveOrderResult struct {
	OrderID    string
	Executions []LiveOrderExecution
}

// PlaceLiveOrder places a REAL retail order: POST /v1/orders (docs.polymarket.us, verified
// 2026-07-02). The open question that kept this a placeholder is now answered by the official
// OpenAPI: X-PM-Signature signs `timestamp + method + path` for POST exactly like GET — the BODY
// IS NOT part of the signed message — so the existing authHeaders works unchanged.
//
// Shape notes (from the spec): LIMIT orders require price as a DECIMAL-STRING Amount
// {value,currency}; side is expressed as outcomeSide (OUTCOME_SIDE_YES/NO) + action
// (ORDER_ACTION_BUY/SELL); participateDontInitiate=true = post-only (maker; rejected if it would
// cross); there is NO client-order-id field, so idempotency/dedup is the CALLER's job.
// Returns the exchange order id.
func (c *Client) PlaceLiveOrder(ctx context.Context, o LiveOrder) (string, error) {
	result, err := c.PlaceLiveOrderDetailed(ctx, o)
	return result.OrderID, err
}

// PlaceLiveOrderDetailed preserves the exchange's synchronous executions. Staged multi-leg FOK
// routing uses this form so the HTTP response itself can become an authoritative fill/fee receipt;
// ordinary callers retain PlaceLiveOrder's order-id compatibility wrapper.
func (c *Client) PlaceLiveOrderDetailed(ctx context.Context, o LiveOrder) (LiveOrderResult, error) {
	if o.MarketSlug == "" || o.Price <= 0 || o.Price >= 1 || math.IsNaN(o.Price) || math.IsInf(o.Price, 0) ||
		o.Size < 0.01 || math.IsNaN(o.Size) || math.IsInf(o.Size, 0) {
		return LiveOrderResult{}, errors.New("polyus live order: need slug, price in (0,1), size >= 0.01")
	}
	if o.PostOnly && o.FillOrKill {
		return LiveOrderResult{}, errors.New("polyus live order: post-only and fill-or-kill are mutually exclusive")
	}
	if o.Synchronous && (o.MaxBlockTimeSeconds < 1 || o.MaxBlockTimeSeconds > 10) {
		return LiveOrderResult{}, errors.New("polyus live order: synchronous max block time must be 1..10 seconds")
	}
	if !strings.EqualFold(strings.TrimSpace(o.Outcome), "YES") && !strings.EqualFold(strings.TrimSpace(o.Outcome), "NO") {
		return LiveOrderResult{}, errors.New("polyus live order: outcome must be YES or NO")
	}
	if !strings.EqualFold(strings.TrimSpace(o.Side), "BUY") && !strings.EqualFold(strings.TrimSpace(o.Side), "SELL") {
		return LiveOrderResult{}, errors.New("polyus live order: side must be BUY or SELL")
	}
	outcome := "OUTCOME_SIDE_YES"
	px := o.Price
	if strings.EqualFold(o.Outcome, "NO") {
		outcome = "OUTCOME_SIDE_NO"
		// PRICE IS ALWAYS YES-DENOMINATED (probed 2026-07-02 + docs order overview): the venue models
		// "BUY NO" as ORDER_INTENT_BUY_SHORT — a SELL of the YES — and price.value is the YES price.
		// We used to send the NO price raw: "NO @ 79¢" became "sell YES at 0.79" while YES traded at
		// 0.21 → IOC expired untouched. EVERY no-side order died silently this way (probed orders
		// B0DH0J0JC5Z8, B0DGB8C0R5YZ: ORDER_STATE_EXPIRED, cum 0). Convert at the wire so no caller
		// can repeat the mistake.
		px = 1 - o.Price
	}
	action := "ORDER_ACTION_BUY"
	if strings.EqualFold(o.Side, "SELL") {
		action = "ORDER_ACTION_SELL"
	}
	tif := "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL" // taker default: fill what's there, cancel the rest
	if o.FillOrKill {
		tif = "TIME_IN_FORCE_FILL_OR_KILL" // taker: fill the complete requested size now or fill nothing
	} else if o.PostOnly {
		tif = "TIME_IN_FORCE_GOOD_TILL_CANCEL" // maker: rest on the book at our price
	}
	// Preserve the caller's market-specific snapped price. Live markets already use 0.001 ticks and
	// the venue may publish finer grids; a fixed 2- or 3-decimal formatter silently changes a valid
	// order. Six fixed-point places bound float noise while retaining every currently documented grid.
	if rounded := math.Round(px*1e6) / 1e6; rounded <= 0 || rounded >= 1 {
		return LiveOrderResult{}, errors.New("polyus live order: price is outside six-decimal wire precision")
	}
	priceValue := formatLiveOrderPrice(px)
	req := map[string]any{
		"marketSlug":              o.MarketSlug,
		"type":                    "ORDER_TYPE_LIMIT",
		"price":                   map[string]string{"value": priceValue, "currency": "USD"},
		"quantity":                o.Size,
		"tif":                     tif,
		"outcomeSide":             outcome,
		"action":                  action,
		"participateDontInitiate": o.PostOnly,
		"manualOrderIndicator":    "MANUAL_ORDER_INDICATOR_AUTOMATIC",
	}
	if o.Synchronous {
		req["synchronousExecution"] = true
		req["maxBlockTime"] = strconv.Itoa(o.MaxBlockTimeSeconds)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return LiveOrderResult{}, err
	}
	raw, code, err := c.do(ctx, http.MethodPost, "/v1/orders", body)
	if err != nil {
		return LiveOrderResult{}, err
	}
	if code != 200 {
		return LiveOrderResult{}, fmt.Errorf("polyus create order: status %d: %s", code, truncate(string(raw), 300))
	}
	var resp struct {
		ID         string `json:"id"`
		Executions []struct {
			ID      string         `json:"id"`
			TradeID string         `json:"tradeId"`
			Type    string         `json:"type"`
			Time    string         `json:"transactTime"`
			LastQty pusOptionalAmt `json:"lastShares"`
			LastPx  pusAmt         `json:"lastPx"`
			Fee     pusOptionalAmt `json:"commissionNotionalCollected"`
			Order   struct {
				ID          string  `json:"id"`
				MarketSlug  string  `json:"marketSlug"`
				Intent      string  `json:"intent"`
				OutcomeSide string  `json:"outcomeSide"`
				Action      string  `json:"action"`
				State       string  `json:"state"`
				TIF         string  `json:"tif"`
				Price       pusAmt  `json:"price"`
				AvgPx       pusAmt  `json:"avgPx"`
				Quantity    float64 `json:"quantity"`
				Cum         float64 `json:"cumQuantity"`
				Leaves      float64 `json:"leavesQuantity"`
			} `json:"order"`
		} `json:"executions"`
	}
	if json.Unmarshal(raw, &resp) != nil || resp.ID == "" {
		return LiveOrderResult{}, fmt.Errorf("polyus create order: 200 but unparseable body: %s", truncate(string(raw), 300))
	}
	result := LiveOrderResult{OrderID: resp.ID, Executions: make([]LiveOrderExecution, 0, len(resp.Executions))}
	for _, row := range resp.Executions {
		outcomeSide, actionSide := orderSides(row.Order.OutcomeSide, row.Order.Action, row.Order.Intent)
		lastQty := row.LastQty.Value
		yesOrder, yesAvg, yesLast := row.Order.Price.f(), row.Order.AvgPx.f(), row.LastPx.f()
		orderPrice, averagePrice, lastPrice := 0.0, 0.0, 0.0
		if yesOrder > 0 && yesOrder < 1 {
			orderPrice = outcomePrice(yesOrder, outcomeSide)
		}
		if yesAvg > 0 && yesAvg < 1 {
			averagePrice = outcomePrice(yesAvg, outcomeSide)
		}
		if yesLast > 0 && yesLast < 1 {
			lastPrice = outcomePrice(yesLast, outcomeSide)
		}
		orderID := strings.TrimSpace(row.Order.ID)
		if orderID == "" {
			orderID = resp.ID
		}
		result.Executions = append(result.Executions, LiveOrderExecution{
			ID: row.ID, OrderID: orderID, TradeID: row.TradeID, MarketSlug: row.Order.MarketSlug,
			Intent: row.Order.Intent, Outcome: outcomeSide, Action: actionSide, Type: row.Type,
			State: row.Order.State, TIF: row.Order.TIF, TransactTime: row.Time, OrderQty: row.Order.Quantity,
			CumQty: row.Order.Cum, LeavesQty: row.Order.Leaves, LastQty: lastQty,
			OrderPrice: orderPrice, YesOrderPrice: yesOrder,
			AveragePrice: averagePrice, YesAveragePrice: yesAvg,
			LastPrice: lastPrice, YesLastPrice: yesLast,
			CommissionUSD: row.Fee.Value, CommissionKnown: row.Fee.Present,
		})
	}
	return result, nil
}

func formatLiveOrderPrice(px float64) string {
	px = math.Round(px*1e6) / 1e6
	s := strconv.FormatFloat(px, 'f', 6, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" || s == "-0" {
		return "0"
	}
	return s
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// MarketMeta is the per-market order-constraint and lifecycle receipt. Tick/minimum fields have
// appeared as numbers, strings, and Amount objects; ep3Status is the current detail endpoint's
// lifecycle field. Read this receipt immediately before quoting; never assume a tick or OPEN state.
type MarketMeta struct {
	Slug      string
	TickUSD   float64 // orderPriceMinTickSize (dollars); 0 = unknown, not a default
	MinQty    float64 // minimumTradeQty (contracts); 0 = unknown, not a default
	Active    *bool
	Closed    *bool
	Archived  *bool
	State     string // legacy MARKET_STATE_*
	Status    string
	EP3Status string // current v1 detail lifecycle field, e.g. OPEN/PREOPEN/HALTED
}

func (m MarketMeta) LifecycleKnown() bool {
	return m.Active != nil || m.Closed != nil || m.Archived != nil ||
		strings.TrimSpace(m.State) != "" || strings.TrimSpace(m.Status) != "" ||
		strings.TrimSpace(m.EP3Status) != ""
}

func (m MarketMeta) Terminal() bool {
	if m.Archived != nil && *m.Archived || m.Closed != nil && *m.Closed || m.Active != nil && !*m.Active {
		return true
	}
	for _, raw := range []string{m.State, m.Status, m.EP3Status} {
		if state := lifecycleState(raw); state != "" && state != "OPEN" && state != "ACTIVE" {
			return true
		}
	}
	return false
}

// Open requires affirmative lifecycle evidence and rejects if any supplied source disagrees.
func (m MarketMeta) Open() bool {
	if m.Archived != nil && *m.Archived || m.Closed != nil && *m.Closed {
		return false
	}
	affirmed := false
	for _, raw := range []string{m.State, m.Status, m.EP3Status} {
		if state := lifecycleState(raw); state != "" {
			if state != "OPEN" && state != "ACTIVE" {
				return false
			}
			affirmed = true
		}
	}
	if m.Active != nil {
		if !*m.Active {
			return false
		}
		affirmed = true
	}
	return affirmed
}

// GetMarketMeta fetches one market's order constraints (GET /v1/market/slug/{slug}).
func (c *Client) GetMarketMeta(ctx context.Context, slug string) (MarketMeta, error) {
	raw, code, err := c.do(ctx, http.MethodGet, "/v1/market/slug/"+strings.TrimSpace(slug), nil)
	if err != nil {
		return MarketMeta{}, err
	}
	if code != 200 {
		return MarketMeta{}, fmt.Errorf("polyus market meta %s: status %d: %s", slug, code, truncate(string(raw), 160))
	}
	type mm struct {
		Tick      flexAmt `json:"orderPriceMinTickSize"` // number, string, or {value}
		MinQty    flexAmt `json:"minimumTradeQty"`
		Active    *bool   `json:"active"`
		Closed    *bool   `json:"closed"`
		Archived  *bool   `json:"archived"`
		State     string  `json:"state"`
		Status    string  `json:"status"`
		EP3Status string  `json:"ep3Status"`
		EP3StateS string  `json:"ep3_status"`
	}
	var resp struct {
		Market *mm `json:"market"`
		mm
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return MarketMeta{}, fmt.Errorf("polyus market meta %s: unparseable: %s", slug, truncate(string(raw), 160))
	}
	src := resp.mm
	if resp.Market != nil {
		src = *resp.Market
	}
	if src.EP3Status == "" {
		src.EP3Status = src.EP3StateS
	}
	return MarketMeta{
		Slug: slug, TickUSD: src.Tick.f(), MinQty: src.MinQty.f(), Active: src.Active,
		Closed: src.Closed, Archived: src.Archived, State: src.State, Status: src.Status,
		EP3Status: src.EP3Status,
	}, nil
}

// flexAmt decodes a money/number field that the venue serializes inconsistently: bare number,
// decimal string, or {value,currency}.
type flexAmt float64

func (a *flexAmt) UnmarshalJSON(b []byte) error {
	v, present, err := decodeFlexAmount(b)
	if err != nil {
		return err
	}
	if !present {
		v = 0
	}
	*a = flexAmt(v)
	return nil
}

func (a flexAmt) f() float64 { return float64(a) }

// decodeFlexAmount normalizes every numeric shape observed on list/detail/book schemas: a JSON
// number, decimal string, or Amount object {"value": number|string}. Null/absent is not present;
// malformed supplied values return an error so a schema drift cannot silently become zero.
func decodeFlexAmount(b []byte) (value float64, present bool, err error) {
	s := strings.TrimSpace(string(b))
	if s == "" || s == "null" {
		return 0, false, nil
	}
	if strings.HasPrefix(s, "{") {
		var w struct {
			Value json.RawMessage `json:"value"`
		}
		if err := json.Unmarshal(b, &w); err != nil {
			return 0, true, err
		}
		if len(w.Value) == 0 {
			return 0, true, errors.New("amount object missing value")
		}
		v, ok, err := decodeFlexAmount(w.Value)
		if err != nil {
			return 0, true, err
		}
		if !ok {
			return 0, true, errors.New("amount value is null")
		}
		return v, true, nil
	}
	if strings.HasPrefix(s, `"`) {
		var text string
		if err := json.Unmarshal(b, &text); err != nil {
			return 0, true, err
		}
		s = strings.TrimSpace(text)
		if s == "" {
			return 0, false, nil
		}
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		if err == nil {
			err = errors.New("non-finite amount")
		}
		return 0, true, err
	}
	return v, true, nil
}

// OrderState is the venue's answer to "what actually happened to this order id" — the docs are
// explicit that a 200 + order id does NOT mean accepted; rejection/expiry is asynchronous. This is
// the post-place truth check (GET /v1/order/{id}, ~100ms).
type OrderState struct {
	ID                   string
	MarketSlug           string
	Outcome              string
	Action               string
	OrderQty             float64
	State                string  // ORDER_STATE_* (NEW/PARTIALLY_FILLED/FILLED/CANCELED/REJECTED/EXPIRED/...)
	Cum                  float64 // contracts filled
	Leaves               float64 // contracts still resting
	AvgPx                float64 // filled outcome's price (NO is 1-YesAvgPx)
	YesAvgPx             float64 // raw venue YES-denominated average fill
	YesPx                float64 // the order's YES-denominated limit price as the venue holds it
	Intent               string  // ORDER_INTENT_BUY_LONG / BUY_SHORT / ...
	TIF                  string
	CommissionTotalUSD   float64 // venue-reported cumulative commission for the whole order
	CommissionTotalKnown bool    // distinguishes absent schema from an explicit $0 receipt
}

// Terminal reports whether the order is done moving (nothing resting, nothing more coming).
func (o OrderState) Terminal() bool {
	switch o.State {
	case "ORDER_STATE_FILLED", "ORDER_STATE_CANCELED", "ORDER_STATE_REJECTED", "ORDER_STATE_EXPIRED", "ORDER_STATE_REPLACED":
		return true
	}
	return false
}

// DeadUnfilled = terminal with zero fill — the silent-death shape the dashboard must surface.
func (o OrderState) DeadUnfilled() bool { return o.Terminal() && o.Cum <= 0 }

// GetOrderState fetches one order's current state (GET /v1/order/{id}).
func (c *Client) GetOrderState(ctx context.Context, id string) (OrderState, error) {
	raw, code, err := c.do(ctx, http.MethodGet, "/v1/order/"+id, nil)
	if err != nil {
		return OrderState{}, err
	}
	if code != 200 {
		return OrderState{}, fmt.Errorf("polyus get order %s: status %d: %s", id, code, truncate(string(raw), 200))
	}
	var resp struct {
		Order struct {
			ID                   string         `json:"id"`
			MarketSlug           string         `json:"marketSlug"`
			Quantity             float64        `json:"quantity"`
			State                string         `json:"state"`
			Cum                  float64        `json:"cumQuantity"`
			Leaves               float64        `json:"leavesQuantity"`
			AvgPx                pusAmt         `json:"avgPx"`
			Price                pusAmt         `json:"price"`
			Intent               string         `json:"intent"`
			OutcomeSide          string         `json:"outcomeSide"`
			Action               string         `json:"action"`
			TIF                  string         `json:"tif"`
			CommissionTotal      pusOptionalAmt `json:"commissionNotionalTotalCollected"`
			CommissionTotalSnake pusOptionalAmt `json:"commission_notional_total_collected"`
		} `json:"order"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil || resp.Order.ID == "" {
		return OrderState{}, fmt.Errorf("polyus get order %s: unparseable: %s", id, truncate(string(raw), 200))
	}
	rawAvg := resp.Order.AvgPx.f()
	outcome, action := orderSides(resp.Order.OutcomeSide, resp.Order.Action, resp.Order.Intent)
	avg := 0.0
	// A missing average is zero in the wire decoder. Complementing that sentinel for a NO order
	// would manufacture a $1.00 fill, so only side-convert a genuine price.
	if rawAvg > 0 && rawAvg < 1 {
		avg = outcomePrice(rawAvg, outcome)
	}
	commission := resp.Order.CommissionTotal
	if !commission.Present {
		commission = resp.Order.CommissionTotalSnake
	}
	return OrderState{
		ID: resp.Order.ID, MarketSlug: resp.Order.MarketSlug, Outcome: outcome, Action: action,
		OrderQty: resp.Order.Quantity, State: resp.Order.State,
		Cum: resp.Order.Cum, Leaves: resp.Order.Leaves,
		AvgPx: avg, YesAvgPx: rawAvg, YesPx: resp.Order.Price.f(),
		Intent: resp.Order.Intent, TIF: resp.Order.TIF,
		CommissionTotalUSD: commission.Value, CommissionTotalKnown: commission.Present,
	}, nil
}

// pusAmt is the API's {value:"0.55",currency:"USD"} decimal-string money shape.
type pusAmt struct {
	Value string `json:"value"`
}

func (a pusAmt) f() float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(a.Value), 64)
	return v
}

// pusOptionalAmt decodes both retail Amount objects and the institutional string/number shape.
// Present is essential for fee receipts: a missing field means "venue did not send a receipt",
// while an explicit zero proves this execution/order paid no commission.
type pusOptionalAmt struct {
	Value   float64
	Present bool
}

func (a *pusOptionalAmt) UnmarshalJSON(b []byte) error {
	v, present, err := decodeFlexAmount(b)
	if err != nil {
		return err
	}
	a.Value, a.Present = v, present
	return nil
}

// pusOptionalBool accepts the documented retail boolean while refusing to let an unexpected
// object poison the whole account-activity response.  The live venue has emitted both shapes for
// trade.aggressor: retail activities document isAggressor:boolean, while the institutional Trade
// schema uses aggressor:Execution.  An Execution object is not, by itself, proof that the user's
// order was the taker; only an explicit nested boolean is treated as known.
type pusOptionalBool struct {
	Value   bool
	Present bool
}

func (v *pusOptionalBool) UnmarshalJSON(b []byte) error {
	var scalar bool
	if err := json.Unmarshal(b, &scalar); err == nil {
		v.Value, v.Present = scalar, true
		return nil
	}
	var text string
	if err := json.Unmarshal(b, &text); err == nil {
		switch strings.ToLower(strings.TrimSpace(text)) {
		case "true", "1":
			v.Value, v.Present = true, true
		case "false", "0":
			v.Value, v.Present = false, true
		}
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(b, &obj); err == nil {
		for _, key := range []string{"isAggressor", "is_aggressor", "aggressor"} {
			raw, ok := obj[key]
			if !ok {
				continue
			}
			var nested bool
			if err := json.Unmarshal(raw, &nested); err == nil {
				v.Value, v.Present = nested, true
				return nil
			}
		}
		// A structurally valid Execution object without an explicit user-aggressor flag is
		// intentionally unknown, not false.
		return nil
	}
	if strings.TrimSpace(string(b)) == "null" {
		return nil
	}
	return fmt.Errorf("unsupported boolean shape: %s", truncate(string(b), 80))
}

// PUSOrder is one open retail order, flattened to what the dashboard renders.
type PUSOrder struct {
	ID                   string  `json:"id"`
	Slug                 string  `json:"slug"`
	Title                string  `json:"title"`
	Outcome              string  `json:"outcome"`   // YES | NO
	Action               string  `json:"action"`    // BUY | SELL
	Price                float64 `json:"price"`     // traded outcome's price (NO is 1-YesPrice)
	YesPrice             float64 `json:"yes_price"` // venue wire price; PolyUS always denominates this in YES
	Qty                  float64 `json:"qty"`
	Leaves               float64 `json:"leaves"`
	State                string  `json:"state"`
	TIF                  string  `json:"tif"`
	Created              string  `json:"created,omitempty"` // insert/create time (drives the stale-order sweep)
	CommissionTotalUSD   float64 `json:"commission_total_usd,omitempty"`
	CommissionTotalKnown bool    `json:"commission_total_known,omitempty"`
}

// OpenOrders returns the account's working orders (GET /v1/orders/open). Shares the wire shape +
// flattener with the private WS (pusOrderWire.flat), so REST and push views can never diverge.
func (c *Client) OpenOrders(ctx context.Context) ([]PUSOrder, error) {
	raw, code, err := c.do(ctx, http.MethodGet, "/v1/orders/open", nil)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("polyus open orders: status %d: %s", code, truncate(string(raw), 200))
	}
	var resp struct {
		Orders []pusOrderWire `json:"orders"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	out := make([]PUSOrder, 0, len(resp.Orders))
	for _, o := range resp.Orders {
		out = append(out, o.flat())
	}
	return out, nil
}

// CancelAllLiveOrders cancels every open order (POST /v1/orders/open/cancel; empty slugs = all).
// Returns the canceled order ids.
func (c *Client) CancelAllLiveOrders(ctx context.Context, slugs []string) ([]string, error) {
	if slugs == nil {
		slugs = []string{}
	}
	body, _ := json.Marshal(map[string]any{"slugs": slugs})
	raw, code, err := c.do(ctx, http.MethodPost, "/v1/orders/open/cancel", body)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("polyus cancel-all: status %d: %s", code, truncate(string(raw), 200))
	}
	var resp struct {
		CanceledOrderIDs []string `json:"canceledOrderIds"`
	}
	_ = json.Unmarshal(raw, &resp)
	return resp.CanceledOrderIDs, nil
}

// CancelLiveOrder cancels one working order (POST /v1/order/{orderId}/cancel, body {marketSlug}).
func (c *Client) CancelLiveOrder(ctx context.Context, orderID, marketSlug string) error {
	body, _ := json.Marshal(map[string]string{"marketSlug": marketSlug})
	raw, code, err := c.do(ctx, http.MethodPost, "/v1/order/"+strings.TrimSpace(orderID)+"/cancel", body)
	if err != nil {
		return err
	}
	if code != 200 {
		return fmt.Errorf("polyus cancel: status %d: %s", code, truncate(string(raw), 200))
	}
	return nil
}

// PUSPosition is one account position (GET /v1/portfolio/positions), flattened.
type PUSPosition struct {
	Slug      string  `json:"slug"`
	Title     string  `json:"title"`
	QtyBought float64 `json:"qty_bought,omitempty"`
	QtySold   float64 `json:"qty_sold,omitempty"`
	Net       float64 `json:"net"` // net contracts (+long YES / −short)
	Cost      float64 `json:"cost"`
	Realized  float64 `json:"realized"`
	CashVal   float64 `json:"cash_value"`
	// CashValKnown distinguishes an authoritative venue-sent $0 mark from an omitted cashValue.
	// An open losing position can genuinely be worth zero; treating that as "unmarked" makes LIVE
	// P&L disappear precisely when the loss is largest.
	CashValKnown bool   `json:"cash_value_known"`
	Expired      bool   `json:"expired"`
	Updated      string `json:"updated,omitempty"`
}

type pusPositionWire struct {
	NetPositionDecimal pusOptionalAmt `json:"netPositionDecimal"`
	NetPosition        string         `json:"netPosition"`
	QtyBoughtDecimal   pusOptionalAmt `json:"qtyBoughtDecimal"`
	QtyBought          string         `json:"qtyBought"`
	QtySoldDecimal     pusOptionalAmt `json:"qtySoldDecimal"`
	QtySold            string         `json:"qtySold"`
	Cost               pusAmt         `json:"cost"`
	Realized           pusAmt         `json:"realized"`
	CashValue          pusOptionalAmt `json:"cashValue"`
	Expired            bool           `json:"expired"`
	UpdateTime         string         `json:"updateTime"`
	MarketMetadata     struct {
		Slug  string `json:"slug"`
		Title string `json:"title"`
	} `json:"marketMetadata"`
}

type pusPositionsPage struct {
	Positions  map[string]pusPositionWire `json:"positions"`
	NextCursor string                     `json:"nextCursor"`
	EOF        *bool                      `json:"eof"`
}

// LivePositions returns the account's positions, flattened for display. NOTE the response shape
// (verified against the OpenAPI after a live unmarshal error): positions is a MAP of market slug →
// UserPosition (additionalProperties), NOT an array.
func (c *Client) LivePositions(ctx context.Context) ([]PUSPosition, error) {
	const (
		pageLimit = 100
		maxPages  = 100
	)
	out := make([]PUSPosition, 0, pageLimit)
	seenSlugs := make(map[string]struct{})
	seenCursors := map[string]struct{}{"": {}}
	cursor := ""
	for page := 0; page < maxPages; page++ {
		path := "/v1/portfolio/positions?limit=" + strconv.Itoa(pageLimit)
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		raw, code, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		if code != http.StatusOK {
			return nil, fmt.Errorf("polyus positions page %d: status %d: %s", page+1, code, truncate(string(raw), 200))
		}
		var resp pusPositionsPage
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("polyus positions page %d: %w", page+1, err)
		}
		if resp.Positions == nil {
			return nil, fmt.Errorf("polyus positions page %d omitted positions", page+1)
		}
		if resp.EOF == nil {
			return nil, fmt.Errorf("polyus positions page %d omitted eof", page+1)
		}
		for mapSlug, p := range resp.Positions {
			slug := strings.TrimSpace(mapSlug)
			if strings.TrimSpace(p.MarketMetadata.Slug) != "" {
				slug = strings.TrimSpace(p.MarketMetadata.Slug)
			}
			if slug == "" {
				return nil, fmt.Errorf("polyus positions page %d contained an empty market slug", page+1)
			}
			if _, duplicate := seenSlugs[slug]; duplicate {
				return nil, fmt.Errorf("polyus positions pagination repeated market slug %q", slug)
			}
			seenSlugs[slug] = struct{}{}
			net := p.NetPositionDecimal.Value
			if !p.NetPositionDecimal.Present {
				net, _ = strconv.ParseFloat(p.NetPosition, 64)
			}
			bought := p.QtyBoughtDecimal.Value
			if !p.QtyBoughtDecimal.Present {
				bought, _ = strconv.ParseFloat(p.QtyBought, 64)
			}
			sold := p.QtySoldDecimal.Value
			if !p.QtySoldDecimal.Present {
				sold, _ = strconv.ParseFloat(p.QtySold, 64)
			}
			out = append(out, PUSPosition{
				Slug: slug, Title: p.MarketMetadata.Title, Net: net, QtyBought: bought, QtySold: sold,
				Cost: p.Cost.f(), Realized: p.Realized.f(), CashVal: p.CashValue.Value,
				CashValKnown: p.CashValue.Present, Expired: p.Expired,
				Updated: p.UpdateTime,
			})
		}
		if *resp.EOF {
			return out, nil
		}
		next := strings.TrimSpace(resp.NextCursor)
		if next == "" {
			return nil, fmt.Errorf("polyus positions page %d reported eof=false without nextCursor", page+1)
		}
		if _, repeated := seenCursors[next]; repeated {
			return nil, fmt.Errorf("polyus positions pagination repeated cursor %q", next)
		}
		seenCursors[next] = struct{}{}
		cursor = next
	}
	return nil, fmt.Errorf("polyus positions pagination exceeded %d pages", maxPages)
}

// PUSActivity is one history row (trade / resolution), flattened from GET /v1/portfolio/activities.
type PUSActivity struct {
	ID              string  `json:"id,omitempty"`
	Type            string  `json:"type"` // TRADE | POSITION_RESOLUTION | …
	Slug            string  `json:"slug"`
	Title           string  `json:"title,omitempty"`     // human name, enriched server-side from orders/positions/gateway
	SettlePx        float64 `json:"settle_px,omitempty"` // resolutions: derived settlement $/contract = (cost basis + pnl)/qty, when it lands in [0,1]
	Price           float64 `json:"price"`
	Qty             float64 `json:"qty"`
	PnL             float64 `json:"pnl"`
	Taker           bool    `json:"taker"`
	TakerKnown      bool    `json:"taker_known,omitempty"`
	CommissionUSD   float64 `json:"commission_usd,omitempty"` // per-trade receipt when supplied
	CommissionKnown bool    `json:"commission_known,omitempty"`
	Time            string  `json:"time"`
	Updated         string  `json:"updated,omitempty"`
	State           string  `json:"state,omitempty"`
}

type pusActivityWire struct {
	Type  string `json:"type"`
	Trade struct {
		ID               string          `json:"id"`
		MarketSlug       string          `json:"marketSlug"`
		Price            pusAmt          `json:"price"`
		QtyDecimal       pusOptionalAmt  `json:"qtyDecimal"`
		Qty              string          `json:"qty"`
		Aggressor        pusOptionalBool `json:"aggressor"`
		IsAggressor      pusOptionalBool `json:"isAggressor"`
		IsAggressorSnake pusOptionalBool `json:"is_aggressor"`
		Commission       pusOptionalAmt  `json:"commissionNotionalCollected"`
		CommissionSnake  pusOptionalAmt  `json:"commission_notional_collected"`
		RealizedPnl      *pusAmt         `json:"realizedPnl"`
		State            string          `json:"state"`
		CreateTime       string          `json:"createTime"`
		UpdateTime       string          `json:"updateTime"`
	} `json:"trade"`
	PositionResolution struct {
		MarketSlug string `json:"marketSlug"`
		UpdateTime string `json:"updateTime"`
		TradeID    string `json:"tradeId"`
		Side       string `json:"side"` // POSITION_RESOLUTION_SIDE_LONG | _SHORT
		Before     struct {
			NetPositionDecimal pusOptionalAmt `json:"netPositionDecimal"`
			NetPosition        string         `json:"netPosition"`
			Cost               pusAmt         `json:"cost"`
			AvgPx              *pusAmt        `json:"avgPx"`
		} `json:"beforePosition"`
		After struct {
			Realized pusAmt `json:"realized"`
		} `json:"afterPosition"`
	} `json:"positionResolution"`
	AccountBalanceChange struct {
		TransactionID string `json:"transactionId"`
	} `json:"accountBalanceChange"`
}

type pusActivitiesPage struct {
	Activities []json.RawMessage `json:"activities"`
	NextCursor string            `json:"nextCursor"`
	EOF        *bool             `json:"eof"`
}

// pusActivityStableIdentity uses only venue-assigned identifiers documented as stable. We do not
// invent a timestamp/price/quantity fallback: two legitimate partial fills may share all three.
func pusActivityStableIdentity(a pusActivityWire) (string, bool) {
	t := strings.TrimPrefix(strings.TrimSpace(a.Type), "ACTIVITY_TYPE_")
	switch t {
	case "TRADE":
		if id := strings.TrimSpace(a.Trade.ID); id != "" {
			return "trade:" + id, true
		}
	case "POSITION_RESOLUTION":
		if id := strings.TrimSpace(a.PositionResolution.TradeID); id != "" {
			return "resolution:" + strings.TrimSpace(a.PositionResolution.MarketSlug) + ":" +
				strings.TrimSpace(a.PositionResolution.Side) + ":" + id, true
		}
	default:
		if id := strings.TrimSpace(a.AccountBalanceChange.TransactionID); id != "" {
			return "balance:" + id, true
		}
	}
	return "", false
}

func (c *Client) liveActivityPages(ctx context.Context) ([]json.RawMessage, error) {
	const (
		pageLimit = 100
		maxPages  = 100
	)
	all := make([]json.RawMessage, 0, pageLimit)
	seenCursors := map[string]struct{}{"": {}}
	seenStable := make(map[string]int)
	cursor := ""
	for page := 0; page < maxPages; page++ {
		path := "/v1/portfolio/activities?limit=" + strconv.Itoa(pageLimit)
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		raw, code, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		if code != http.StatusOK {
			return nil, fmt.Errorf("polyus activities page %d: status %d: %s", page+1, code, truncate(string(raw), 200))
		}
		var resp pusActivitiesPage
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, fmt.Errorf("polyus activities page %d: %w", page+1, err)
		}
		if resp.Activities == nil {
			return nil, fmt.Errorf("polyus activities page %d omitted activities", page+1)
		}
		if resp.EOF == nil {
			return nil, fmt.Errorf("polyus activities page %d omitted eof", page+1)
		}
		for _, rawActivity := range resp.Activities {
			var activity pusActivityWire
			if err := json.Unmarshal(rawActivity, &activity); err != nil {
				return nil, fmt.Errorf("polyus activities page %d contained invalid activity: %w", page+1, err)
			}
			if identity, stable := pusActivityStableIdentity(activity); stable {
				if priorPage, duplicate := seenStable[identity]; duplicate && priorPage != page {
					return nil, fmt.Errorf("polyus activities pagination repeated stable activity %q", identity)
				}
				seenStable[identity] = page
			}
			// RawMessage preserves decimal-field presence through the existing union parser below.
			all = append(all, append(json.RawMessage(nil), rawActivity...))
		}
		if *resp.EOF {
			return all, nil
		}
		next := strings.TrimSpace(resp.NextCursor)
		if next == "" {
			return nil, fmt.Errorf("polyus activities page %d reported eof=false without nextCursor", page+1)
		}
		if _, repeated := seenCursors[next]; repeated {
			return nil, fmt.Errorf("polyus activities pagination repeated cursor %q", next)
		}
		seenCursors[next] = struct{}{}
		cursor = next
	}
	return nil, fmt.Errorf("polyus activities pagination exceeded %d pages", maxPages)
}

// Activities returns complete account history, newest first. The venue caps each cursor page at
// 100; liveActivityPages transmits each exact cursor query while signing the bare endpoint path,
// and fails closed rather than returning a partial ledger when pagination truth is malformed.
func (c *Client) Activities(ctx context.Context) ([]PUSActivity, error) {
	activities, err := c.liveActivityPages(ctx)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(struct {
		Activities []json.RawMessage `json:"activities"`
	}{Activities: activities})
	if err != nil {
		return nil, fmt.Errorf("polyus activities combine pages: %w", err)
	}
	var resp struct {
		Activities []struct {
			Type  string `json:"type"`
			Trade struct {
				ID               string          `json:"id"`
				MarketSlug       string          `json:"marketSlug"`
				Price            pusAmt          `json:"price"`
				QtyDecimal       pusOptionalAmt  `json:"qtyDecimal"`
				Qty              string          `json:"qty"`
				Aggressor        pusOptionalBool `json:"aggressor"`
				IsAggressor      pusOptionalBool `json:"isAggressor"`
				IsAggressorSnake pusOptionalBool `json:"is_aggressor"`
				Commission       pusOptionalAmt  `json:"commissionNotionalCollected"`
				CommissionSnake  pusOptionalAmt  `json:"commission_notional_collected"`
				RealizedPnl      *pusAmt         `json:"realizedPnl"`
				State            string          `json:"state"`
				CreateTime       string          `json:"createTime"`
				UpdateTime       string          `json:"updateTime"`
			} `json:"trade"`
			PositionResolution struct {
				MarketSlug string `json:"marketSlug"`
				UpdateTime string `json:"updateTime"`
				Side       string `json:"side"` // POSITION_RESOLUTION_SIDE_LONG | _SHORT
				Before     struct {
					NetPositionDecimal pusOptionalAmt `json:"netPositionDecimal"`
					NetPosition        string         `json:"netPosition"`
					Cost               pusAmt         `json:"cost"`
					AvgPx              *pusAmt        `json:"avgPx"` // probe truth: avg entry per contract — the exact settle basis
				} `json:"beforePosition"`
				After struct {
					Realized pusAmt `json:"realized"`
				} `json:"afterPosition"`
			} `json:"positionResolution"`
		} `json:"activities"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, err
	}
	out := make([]PUSActivity, 0, len(resp.Activities))
	for _, a := range resp.Activities {
		t := strings.TrimPrefix(a.Type, "ACTIVITY_TYPE_")
		switch t {
		case "TRADE":
			if strings.Contains(strings.ToUpper(strings.TrimSpace(a.Trade.State)), "BUST") {
				// A busted trade is a reversal, not durable execution evidence or realized volume.
				continue
			}
			q := a.Trade.QtyDecimal.Value
			if !a.Trade.QtyDecimal.Present {
				q, _ = strconv.ParseFloat(a.Trade.Qty, 64)
			}
			pnl := 0.0
			if a.Trade.RealizedPnl != nil {
				pnl = a.Trade.RealizedPnl.f()
			}
			// The retail fields are the user-relative authority. trade.aggressor is a legacy
			// boolean on some responses but an institutional Execution object on others.
			taker := a.Trade.IsAggressor
			if !taker.Present {
				taker = a.Trade.IsAggressorSnake
			}
			if !taker.Present {
				taker = a.Trade.Aggressor
			}
			isTaker, takerKnown := taker.Value, taker.Present
			commission := a.Trade.Commission
			if !commission.Present {
				commission = a.Trade.CommissionSnake
			}
			out = append(out, PUSActivity{ID: a.Trade.ID, Type: t, Slug: a.Trade.MarketSlug, Price: a.Trade.Price.f(),
				Qty: q, PnL: pnl, Taker: isTaker, TakerKnown: takerKnown,
				CommissionUSD: commission.Value, CommissionKnown: commission.Present, Time: a.Trade.CreateTime,
				Updated: a.Trade.UpdateTime, State: a.Trade.State})
		case "POSITION_RESOLUTION":
			bq := a.PositionResolution.Before.NetPositionDecimal.Value
			if !a.PositionResolution.Before.NetPositionDecimal.Present {
				bq, _ = strconv.ParseFloat(a.PositionResolution.Before.NetPosition, 64)
			}
			pnl := a.PositionResolution.After.Realized.f()
			// SETTLE DERIVATION (probe truth 2026-07-02): use avgPx ± pnl/qty. LONG: settle =
			// avg + pnl/q. SHORT (net<0 / SIDE_SHORT): settle = avg − pnl/q (a short profits as
			// the side price falls). The old cost-based formula broke on shorts because `cost`
			// includes fees and inverts sign conventions.
			settle := 0.0
			if q := math.Abs(bq); q > 0 && a.PositionResolution.Before.AvgPx != nil {
				avg := a.PositionResolution.Before.AvgPx.f()
				sp := avg + pnl/q
				if bq < 0 || strings.HasSuffix(a.PositionResolution.Side, "_SHORT") {
					sp = avg - pnl/q
				}
				if sp >= 0 && sp <= 1 {
					settle = sp
				}
			}
			out = append(out, PUSActivity{Type: t, Slug: a.PositionResolution.MarketSlug,
				Qty: math.Abs(bq), PnL: pnl, SettlePx: settle, Time: a.PositionResolution.UpdateTime})
		}
	}
	return out, nil
}
