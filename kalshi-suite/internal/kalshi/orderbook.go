package kalshi

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// OrderbookLevel is one price level. Price is in dollars (0..1 = 0..100%).
type OrderbookLevel struct {
	Price float64 `json:"price"`
	Size  float64 `json:"size"`
}

// Orderbook is a YES-centric view of a market's book:
//   - YesBids  = resting bids to BUY YES (from the API's yes_dollars side)
//   - YesAsks  = effective offers to SELL YES, derived from resting NO bids
//     (a NO bid at price q is equivalent to a YES ask at 1-q)
type Orderbook struct {
	Ticker  string           `json:"ticker"`
	YesBids []OrderbookLevel `json:"yes_bids"`
	YesAsks []OrderbookLevel `json:"yes_asks"`
}

// rawOrderbook matches Kalshi's current schema:
// {"orderbook_fp":{"yes_dollars":[["0.61","12.0"],...],"no_dollars":[...]}}
type rawOrderbook struct {
	Ticker      string `json:"ticker"`
	OrderbookFP struct {
		YesDollars [][]string `json:"yes_dollars"`
		NoDollars  [][]string `json:"no_dollars"`
	} `json:"orderbook_fp"`
}

func normalizeOrderbook(ticker string, raw rawOrderbook) *Orderbook {
	if strings.TrimSpace(raw.Ticker) != "" {
		ticker = raw.Ticker
	}
	bids := parseLevels(raw.OrderbookFP.YesDollars)
	asks := make([]OrderbookLevel, 0, len(raw.OrderbookFP.NoDollars))
	for _, lvl := range parseLevels(raw.OrderbookFP.NoDollars) {
		asks = append(asks, OrderbookLevel{Price: math.Round((1-lvl.Price)*10000) / 10000, Size: lvl.Size})
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	return &Orderbook{Ticker: ticker, YesBids: bids, YesAsks: asks}
}

func parseLevels(raw [][]string) []OrderbookLevel {
	out := make([]OrderbookLevel, 0, len(raw))
	for _, pair := range raw {
		if len(pair) < 2 {
			continue
		}
		p, err1 := strconv.ParseFloat(pair[0], 64)
		s, err2 := strconv.ParseFloat(pair[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		out = append(out, OrderbookLevel{Price: p, Size: s})
	}
	return out
}

// GetOrderbook fetches and normalizes a market's order book (public; no auth).
func (c *Client) GetOrderbook(ctx context.Context, ticker string) (*Orderbook, error) {
	var raw rawOrderbook
	path := "/markets/" + url.PathEscape(ticker) + "/orderbook"
	if err := c.do(ctx, http.MethodGet, path, &raw, false); err != nil {
		return nil, err
	}

	return normalizeOrderbook(ticker, raw), nil
}

func multipleOrderbooksPath(tickers []string) (string, error) {
	if len(tickers) == 0 || len(tickers) > 100 {
		return "", fmt.Errorf("multiple orderbooks requires 1..100 tickers")
	}
	q := url.Values{}
	seen := make(map[string]bool, len(tickers))
	for _, ticker := range tickers {
		ticker = strings.TrimSpace(ticker)
		if ticker == "" {
			return "", fmt.Errorf("multiple orderbooks contains an empty ticker")
		}
		if seen[ticker] {
			continue
		}
		seen[ticker] = true
		q.Add("tickers", ticker)
	}
	if len(seen) == 0 {
		return "", fmt.Errorf("multiple orderbooks contains no tickers")
	}
	return "/markets/orderbooks?" + q.Encode(), nil
}

// GetOrderbooks uses Kalshi's authenticated batch depth endpoint (1..100 tickers). It is the
// bounded fallback for research collectors that cannot displace live and near-horizon markets
// from the capped WebSocket depth set. Every returned book is a current venue depth snapshot;
// callers still own quote-age, exact-fee, and route-specific admission checks.
func (c *Client) GetOrderbooks(ctx context.Context, tickers []string) (map[string]*Orderbook, error) {
	path, err := multipleOrderbooksPath(tickers)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Orderbooks []rawOrderbook `json:"orderbooks"`
	}
	if err := c.do(ctx, http.MethodGet, path, &resp, true); err != nil {
		return nil, err
	}
	out := make(map[string]*Orderbook, len(resp.Orderbooks))
	for _, raw := range resp.Orderbooks {
		if strings.TrimSpace(raw.Ticker) == "" {
			continue
		}
		out[raw.Ticker] = normalizeOrderbook(raw.Ticker, raw)
	}
	return out, nil
}
