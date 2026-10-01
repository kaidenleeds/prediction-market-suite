package kalshi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Trade is one executed print from Kalshi's public trade feed. taker_side is the
// aggressor's outcome side: "yes" = someone aggressively BOUGHT YES (bullish),
// "no" = aggressively bought NO (bearish on YES). count_fp is the size.
//
// DEPRECATION (audit §5): public trades use taker_outcome_side/taker_book_side (not the
// unprefixed Order/Fill names). taker_side is retained only as a legacy fallback.
type Trade struct {
	Ticker           string    `json:"ticker"`
	TradeID          string    `json:"trade_id"`
	Count            flexFloat `json:"count_fp"`
	YesPrice         flexFloat `json:"yes_price_dollars"`
	NoPrice          flexFloat `json:"no_price_dollars"`
	TakerSide        string    `json:"taker_side"`         // deprecated upstream; normalized after decode
	TakerOutcomeSide string    `json:"taker_outcome_side"` // canonical public-trade direction
	TakerBookSide    string    `json:"taker_book_side"`    // bid == yes, ask == no
	OutcomeSide      string    `json:"outcome_side"`       // defensive fallback for old/noncanonical captures
	IsBlock          bool      `json:"is_block_trade"`
	CreatedTime      string    `json:"created_time"`
}

// Aggressor returns the taker's outcome side ("yes"/"no"), preferring the canonical public
// taker_outcome_side, then its book-side equivalent, then legacy spellings.
func (t Trade) Aggressor() string {
	if s := strings.ToLower(strings.TrimSpace(t.TakerOutcomeSide)); s == "yes" || s == "no" {
		return s
	}
	switch strings.ToLower(strings.TrimSpace(t.TakerBookSide)) {
	case "bid":
		return "yes"
	case "ask":
		return "no"
	}
	if t.OutcomeSide != "" {
		return strings.ToLower(strings.TrimSpace(t.OutcomeSide))
	}
	return strings.ToLower(strings.TrimSpace(t.TakerSide))
}

// UnmarshalJSON normalizes the legacy TakerSide field as well. A few older consumers read that
// field directly, so canonical-only REST responses must not silently lose trade direction.
func (t *Trade) UnmarshalJSON(data []byte) error {
	type wire Trade
	var w wire
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*t = Trade(w)
	t.TakerSide = t.Aggressor()
	return nil
}

type tradesResponse struct {
	Trades []Trade `json:"trades"`
	Cursor string  `json:"cursor"`
}

// GetTrades returns recent public trades for a market (no credentials required).
func (c *Client) GetTrades(ctx context.Context, ticker string, limit int) ([]Trade, error) {
	if limit <= 0 {
		limit = 100
	}
	path := "/markets/trades?ticker=" + url.QueryEscape(ticker) + "&limit=" + strconv.Itoa(limit)
	var resp tradesResponse
	if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
		return nil, err
	}
	return resp.Trades, nil
}

// GetRecentTrades returns recent public trades across ALL markets (the live tape),
// newest first — one call to surface whales market-wide rather than scanning markets
// one at a time. Each Trade carries its own Ticker.
func (c *Client) GetRecentTrades(ctx context.Context, limit int) ([]Trade, error) {
	if limit <= 0 {
		limit = 1000
	}
	path := "/markets/trades?limit=" + strconv.Itoa(limit)
	var resp tradesResponse
	if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
		return nil, err
	}
	return resp.Trades, nil
}
