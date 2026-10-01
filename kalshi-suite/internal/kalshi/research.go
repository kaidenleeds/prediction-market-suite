package kalshi

// Read-only venue research surfaces.  Nothing in this file can create, amend, or cancel an
// order: the callers use naturally occurring account orders and public market metadata only.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// QueuePosition is Kalshi's true zero-indexed price-time position for one naturally resting
// account order. PositionFP is the number of contracts ahead (the first order is 0.00).
type QueuePosition struct {
	OrderID      string    `json:"order_id"`
	MarketTicker string    `json:"market_ticker"`
	PositionFP   flexFloat `json:"queue_position_fp"`
}

func (q QueuePosition) Position() float64 { return q.PositionFP.Float() }

func strictQueuePosition(raw json.RawMessage) (float64, error) {
	s := strings.TrimSpace(string(raw))
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return 0, err
		}
		s = strings.TrimSpace(text)
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, fmt.Errorf("invalid queue_position_fp %q", s)
	}
	return v, nil
}

// GetQueuePositions reads official queue positions for existing resting orders.  Kalshi's
// original rollout required market_tickers or event_ticker, so callers deliberately provide a
// non-empty, de-duplicated ticker set rather than relying on the later optional-filter behavior.
func (c *Client) GetQueuePositions(ctx context.Context, marketTickers []string) ([]QueuePosition, error) {
	seen := map[string]bool{}
	clean := make([]string, 0, len(marketTickers))
	for _, ticker := range marketTickers {
		ticker = strings.TrimSpace(ticker)
		if ticker != "" && !seen[ticker] {
			seen[ticker] = true
			clean = append(clean, ticker)
		}
	}
	if len(clean) == 0 {
		return nil, nil
	}
	sort.Strings(clean)
	var out []QueuePosition
	seenOrderIDs := make(map[string]bool)
	// Keep signed URLs and response sizes bounded.  The endpoint has no cursor; every chunk is a
	// complete answer for exactly the supplied market set.
	for start := 0; start < len(clean); start += 100 {
		end := start + 100
		if end > len(clean) {
			end = len(clean)
		}
		q := url.Values{"market_tickers": {strings.Join(clean[start:end], ",")}}
		var resp struct {
			Rows json.RawMessage `json:"queue_positions"`
		}
		if err := c.do(ctx, http.MethodGet, "/portfolio/orders/queue_positions?"+q.Encode(), &resp, true); err != nil {
			return nil, err
		}
		if resp.Rows == nil || string(resp.Rows) == "null" {
			return nil, fmt.Errorf("kalshi queue-position schema: missing required queue_positions array")
		}
		var rawRows []json.RawMessage
		if err := json.Unmarshal(resp.Rows, &rawRows); err != nil {
			return nil, fmt.Errorf("kalshi queue-position schema: %w", err)
		}
		for i, raw := range rawRows {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				return nil, fmt.Errorf("kalshi queue-position schema row %d: %w", i, err)
			}
			if fields["queue_position_fp"] == nil || string(fields["queue_position_fp"]) == "null" {
				return nil, fmt.Errorf("kalshi queue-position schema row %d: missing queue_position_fp", i)
			}
			position, err := strictQueuePosition(fields["queue_position_fp"])
			if err != nil {
				return nil, fmt.Errorf("kalshi queue-position schema row %d: %w", i, err)
			}
			var row QueuePosition
			if err := json.Unmarshal(raw, &row); err != nil {
				return nil, fmt.Errorf("kalshi queue-position schema row %d: %w", i, err)
			}
			row.PositionFP = flexFloat(position)
			if strings.TrimSpace(row.OrderID) == "" || strings.TrimSpace(row.MarketTicker) == "" {
				return nil, fmt.Errorf("kalshi queue-position schema row %d: invalid required identity/position", i)
			}
			if seenOrderIDs[row.OrderID] {
				return nil, fmt.Errorf("kalshi queue-position schema: duplicate order_id %q", row.OrderID)
			}
			seenOrderIDs[row.OrderID] = true
			out = append(out, row)
		}
	}
	return out, nil
}

// GetOrder reads one order's terminal state after it disappears from the resting-order list.
func (c *Client) GetOrder(ctx context.Context, orderID string) (Order, error) {
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return Order{}, fmt.Errorf("empty order id")
	}
	var resp struct {
		Order Order `json:"order"`
	}
	if err := c.do(ctx, http.MethodGet, "/portfolio/orders/"+url.PathEscape(orderID), &resp, true); err != nil {
		return Order{}, err
	}
	return resp.Order, nil
}

// IncentiveProgram mirrors the documented public /incentive_programs response schema. PeriodReward is
// retained in venue units: the system never treats a headline reward as per-order EV because
// participant count, qualification and eventual allocation are unknown at observation time.
type IncentiveProgram struct {
	ID                   string    `json:"id"`
	MarketID             string    `json:"market_id"`
	MarketTicker         string    `json:"market_ticker"`
	IncentiveType        string    `json:"incentive_type"`
	IncentiveDescription string    `json:"incentive_description"`
	StartDate            string    `json:"start_date"`
	EndDate              string    `json:"end_date"`
	PeriodReward         float64   `json:"period_reward"`
	PaidOut              bool      `json:"paid_out"`
	DiscountFactorBPS    float64   `json:"discount_factor_bps"`
	TargetSizeFP         flexFloat `json:"target_size_fp"`
}

func (p IncentiveProgram) TargetSize() float64 { return p.TargetSizeFP.Float() }

// ActiveIncentivePrograms follows every next_cursor and rejects repeated/cyclic cursors, so a
// partial page can never masquerade as the complete active-program universe.
func (c *Client) ActiveIncentivePrograms(ctx context.Context) ([]IncentiveProgram, error) {
	const endpoint = "/incentive_programs"
	cursor := ""
	seen := map[string]bool{}
	var out []IncentiveProgram
	for page := 0; page < 100; page++ {
		// `type` is a request filter, not a documented response property. Filtering here is
		// essential: decoding a nonexistent incentive_type field and filtering afterward would
		// silently discard every real program.
		q := url.Values{"status": {"active"}, "type": {"liquidity"}, "limit": {"10000"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var resp struct {
			Programs   []IncentiveProgram `json:"incentive_programs"`
			NextCursor string             `json:"next_cursor"`
		}
		if err := c.do(ctx, http.MethodGet, endpoint+"?"+q.Encode(), &resp, false); err != nil {
			return nil, err
		}
		out = append(out, resp.Programs...)
		next := strings.TrimSpace(resp.NextCursor)
		if next == "" {
			return out, nil
		}
		if next == cursor || seen[next] {
			return nil, fmt.Errorf("kalshi %s repeated pagination cursor %q", endpoint, next)
		}
		seen[next] = true
		cursor = next
	}
	return nil, fmt.Errorf("kalshi %s pagination exceeded 100 pages", endpoint)
}

// EventSnapshot is the official event identity plus its complete current child-market list.
// with_nested_markets=true is required explicitly; callers reject non-mutually-exclusive or
// partially executable sets rather than inferring children from ticker strings.
type EventSnapshot struct {
	EventTicker       string `json:"event_ticker"`
	SeriesTicker      string `json:"series_ticker"`
	Title             string `json:"title"`
	SubTitle          string `json:"sub_title"`
	Category          string `json:"category"`
	CollateralReturn  string `json:"collateral_return_type"`
	MutuallyExclusive bool   `json:"mutually_exclusive"`
	LastUpdatedTS     string `json:"last_updated_ts"`
	SettlementSources []struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"settlement_sources"`
	Markets []Market `json:"markets"`
}

func (c *Client) GetEventSnapshot(ctx context.Context, eventTicker string) (EventSnapshot, error) {
	eventTicker = strings.TrimSpace(eventTicker)
	if eventTicker == "" {
		return EventSnapshot{}, fmt.Errorf("empty event ticker")
	}
	var resp struct {
		Event   EventSnapshot `json:"event"`
		Markets []Market      `json:"markets"` // compatibility with the deprecated top-level shape
	}
	path := "/events/" + url.PathEscape(eventTicker) + "?with_nested_markets=true"
	if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
		return EventSnapshot{}, err
	}
	if len(resp.Event.Markets) == 0 {
		resp.Event.Markets = resp.Markets
	}
	return resp.Event, nil
}
