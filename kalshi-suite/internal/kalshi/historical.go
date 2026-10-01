// historical.go — Kalshi's archived-data tier (R69 item 7, operator lead VERIFIED 2026-07-04).
//
// Kalshi partitions exchange data into live vs historical at rolling cutoff timestamps
// (docs.kalshi.com/getting_started/historical_data; live window target ~3 months). Verified by
// probe against prod, UNAUTHENTICATED (public market data):
//
//	GET /historical/cutoff                       → 200 {"market_settled_ts":"2026-03-08T00:00:00Z",
//	                                                    "orders_updated_ts":"...","trades_created_ts":"..."}
//	GET /historical/markets?series_ticker=KXBTCD → 200 {cursor, markets:[full Market shapes + settlement_ts]}
//	GET /historical/trades?ticker=...&limit=3    → 200 {cursor, trades:[trade_id, count_fp,
//	                                                    yes/no_price_dollars, taker_side, created_time]}
//
// Only cmd/kalshi-backfill consumes these today (never auto-run by the server) — read-only, and
// /historical paths take BACKGROUND rate-limit tokens in do(), so a backfill can't starve orders.
package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// HistoricalCutoff is the live/historical partition boundary (all RFC3339). Records older than
// the relevant cutoff are only served by the /historical/* endpoints, newer only by live ones.
type HistoricalCutoff struct {
	MarketSettledTS string `json:"market_settled_ts"`
	OrdersUpdatedTS string `json:"orders_updated_ts"`
	TradesCreatedTS string `json:"trades_created_ts"`
}

// GetHistoricalCutoff fetches the current cutoff timestamps (public, cheap).
func (c *Client) GetHistoricalCutoff(ctx context.Context) (*HistoricalCutoff, error) {
	var out HistoricalCutoff
	if err := c.do(ctx, http.MethodGet, "/historical/cutoff", &out, false); err != nil {
		return nil, err
	}
	return &out, nil
}

// HistoricalMarketsPage returns one page of archived (settled-before-cutoff) markets for a series.
// Filters on /historical/markets are mutually exclusive per the spec — series_ticker only here.
// limit is clamped to the documented 1..1000; cursor "" = first page.
func (c *Client) HistoricalMarketsPage(ctx context.Context, seriesTicker, cursor string, limit int) ([]Market, string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	q := url.Values{}
	q.Set("series_ticker", seriesTicker)
	q.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	var resp marketsResponse
	if err := c.do(ctx, http.MethodGet, "/historical/markets?"+q.Encode(), &resp, false); err != nil {
		return nil, "", err
	}
	return resp.Markets, resp.Cursor, nil
}

// HistoricalTrade is one archived public trade (fixed-point era shape only). Current responses use
// canonical public-trade direction fields; legacy taker_side is accepted only as a fallback.
type HistoricalTrade struct {
	TradeID          string    `json:"trade_id"`
	Ticker           string    `json:"ticker"`
	CountFP          flexFloat `json:"count_fp"`
	YesPriceD        flexFloat `json:"yes_price_dollars"`
	NoPriceD         flexFloat `json:"no_price_dollars"`
	TakerSide        string    `json:"taker_side"`         // deprecated upstream; normalized after decode
	TakerOutcomeSide string    `json:"taker_outcome_side"` // canonical public-trade outcome direction
	TakerBookSide    string    `json:"taker_book_side"`    // canonical equivalent: bid=yes, ask=no
	CreatedTime      string    `json:"created_time"`
}

// Aggressor returns the taker's YES/NO outcome side using the same precedence as the live tape:
// canonical outcome side, canonical book side, then the legacy taker_side fallback.
func (t HistoricalTrade) Aggressor() string {
	if s := strings.ToLower(strings.TrimSpace(t.TakerOutcomeSide)); s == "yes" || s == "no" {
		return s
	}
	switch strings.ToLower(strings.TrimSpace(t.TakerBookSide)) {
	case "bid":
		return "yes"
	case "ask":
		return "no"
	}
	return strings.ToLower(strings.TrimSpace(t.TakerSide))
}

// UnmarshalJSON keeps legacy backfill consumers correct when the historical API supplies only the
// canonical direction fields.
func (t *HistoricalTrade) UnmarshalJSON(data []byte) error {
	type wire HistoricalTrade
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*t = HistoricalTrade(w)
	t.TakerSide = t.Aggressor()
	return nil
}

// HistoricalTradesPage returns one page of archived trades for a market ticker. min/maxTs are
// optional unix-second bounds (0 = unset); cursor "" = first page. Returns the next cursor
// ("" = done).
func (c *Client) HistoricalTradesPage(ctx context.Context, ticker, cursor string, limit int, minTs, maxTs int64) ([]HistoricalTrade, string, error) {
	if ticker == "" {
		return nil, "", fmt.Errorf("historical trades: ticker required")
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	q := url.Values{}
	q.Set("ticker", ticker)
	q.Set("limit", strconv.Itoa(limit))
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	if minTs > 0 {
		q.Set("min_ts", strconv.FormatInt(minTs, 10))
	}
	if maxTs > 0 {
		q.Set("max_ts", strconv.FormatInt(maxTs, 10))
	}
	var resp struct {
		Trades []HistoricalTrade `json:"trades"`
		Cursor string            `json:"cursor"`
	}
	if err := c.do(ctx, http.MethodGet, "/historical/trades?"+q.Encode(), &resp, false); err != nil {
		return nil, "", err
	}
	return resp.Trades, resp.Cursor, nil
}
