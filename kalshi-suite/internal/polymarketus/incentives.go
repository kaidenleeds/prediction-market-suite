package polymarketus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// IncentivePeriod is the current public /v1/incentives period schema. Status is accepted because
// the live gateway currently emits it even though the reference schema documents status primarily
// as a query filter. Unknown additions remain harmless.
type IncentivePeriod struct {
	ProgramID      string  `json:"programId"`
	ProgramType    string  `json:"programType"`
	Start          string  `json:"start"`
	End            string  `json:"end"`
	RewardPool     float64 `json:"rewardPool"`
	DiscountFactor float64 `json:"discountFactor"`
	TargetSize     float64 `json:"targetSize"`
	Period         string  `json:"period"`
	CreatedAt      string  `json:"createdAt"`
	Status         string  `json:"status"`
}

type IncentiveMarket struct {
	MarketSlug  string            `json:"marketSlug"`
	TimePeriods []IncentivePeriod `json:"timePeriods"`
}

type IncentiveProgramsPage struct {
	Programs      []IncentiveMarket `json:"programs"`
	NextPageToken string            `json:"nextPageToken"`
}

const (
	incentivePageSize        = 100 // production hard cap observed 2026-07-14, even when 500/1000 is requested
	incentiveFullPageLimit   = 100 // legacy unfiltered caller fuse; runtime economics uses symbol-scoped batches below
	incentiveSymbolBatchSize = 50  // keeps repeated-symbol query strings comfortably below common proxy limits
	incentiveBatchPageLimit  = 2   // <=50 grouped symbols must fit one 100-row page; one extra page tolerates boundary churn
)

// IncentiveCrawlStats proves what the symbol-scoped crawl covered. Complete is never true unless
// every requested symbol batch reached an empty nextPageToken. On error no partial Programs escape.
type IncentiveCrawlStats struct {
	RequestedSymbols int  `json:"requested_symbols"`
	Batches          int  `json:"batches"`
	Pages            int  `json:"pages"`
	Programs         int  `json:"programs"`
	Complete         bool `json:"complete"`
}

func normalizeIncentivePrograms(rows []IncentiveMarket) []IncentiveMarket {
	byMarket := map[string]IncentiveMarket{}
	for _, market := range rows {
		market.MarketSlug = strings.ToLower(strings.TrimSpace(market.MarketSlug))
		if market.MarketSlug == "" {
			continue
		}
		existing := byMarket[market.MarketSlug]
		existing.MarketSlug = market.MarketSlug
		existing.TimePeriods = append(existing.TimePeriods, market.TimePeriods...)
		byMarket[market.MarketSlug] = existing
	}
	out := make([]IncentiveMarket, 0, len(byMarket))
	for _, market := range byMarket {
		seen := map[string]bool{}
		periods := market.TimePeriods[:0]
		for _, period := range market.TimePeriods {
			period.ProgramID = strings.TrimSpace(period.ProgramID)
			key := period.ProgramID + "|" + period.Start + "|" + period.End + "|" +
				strconv.FormatFloat(period.RewardPool, 'g', -1, 64)
			if period.ProgramID == "" || seen[key] {
				continue
			}
			seen[key] = true
			periods = append(periods, period)
		}
		market.TimePeriods = periods
		if len(periods) > 0 {
			out = append(out, market)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].MarketSlug < out[j].MarketSlug })
	return out
}

func (p *PublicClient) incentivePage(ctx context.Context, q url.Values) (IncentiveProgramsPage, error) {
	attempts := p.pageRetryAttempts
	if attempts <= 0 {
		attempts = marketsPageRetryAttempts
	}
	base := p.pageRetryBase
	if base <= 0 {
		base = 250 * time.Millisecond
	}
	path := "/v1/incentives?" + q.Encode()
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		raw, code, header, err := p.getWithHeaders(ctx, path)
		var decoded IncentiveProgramsPage
		if err == nil && code == http.StatusOK {
			switch {
			case len(raw) == 0 || len(raw) > 2<<20:
				err = fmt.Errorf("invalid response: status=%d bytes=%d", code, len(raw))
			default:
				err = json.Unmarshal(raw, &decoded)
				if err != nil {
					err = fmt.Errorf("schema: %w", err)
				}
			}
			if err == nil {
				return decoded, nil
			}
		}
		lastErr = err
		if ctx.Err() != nil {
			return IncentiveProgramsPage{}, ctx.Err()
		}
		transient := code == http.StatusTooManyRequests || code >= 500 && code <= 599 ||
			code == http.StatusOK && (len(raw) == 0 || truncatedMarketsJSON(err)) || transientMarketsTransport(err)
		if !transient || attempt+1 >= attempts {
			break
		}
		delay := base << attempt
		if header != nil {
			if retryAfter, ok := retryAfterDelay(header.Get("Retry-After"), time.Now()); ok {
				delay = retryAfter
			}
		}
		if err := waitMarketsRetry(ctx, delay); err != nil {
			return IncentiveProgramsPage{}, err
		}
	}
	if lastErr == nil {
		lastErr = errors.New("empty incentive page failure")
	}
	return IncentiveProgramsPage{}, lastErr
}

func (p *PublicClient) activeIncentiveProgramsQuery(ctx context.Context, symbols []string, maxPages int) ([]IncentiveMarket, int, error) {
	if maxPages <= 0 {
		return nil, 0, errors.New("PolyUS incentives page bound must be positive")
	}
	allowed := map[string]bool{}
	for _, symbol := range symbols {
		if symbol = strings.ToLower(strings.TrimSpace(symbol)); symbol != "" {
			allowed[symbol] = true
		}
	}
	token := ""
	seenToken := map[string]bool{}
	var rows []IncentiveMarket
	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, page, err
		}
		q := url.Values{}
		q.Set("pageSize", strconv.Itoa(incentivePageSize))
		q.Add("statuses", "active")
		if token != "" {
			q.Set("pageToken", token)
		}
		for _, symbol := range symbols {
			if symbol = strings.ToLower(strings.TrimSpace(symbol)); symbol != "" {
				q.Add("symbols", symbol)
			}
		}
		decoded, err := p.incentivePage(ctx, q)
		if err != nil {
			return nil, page, fmt.Errorf("PolyUS incentives page %d: %w", page+1, err)
		}
		for _, market := range decoded.Programs {
			slug := strings.ToLower(strings.TrimSpace(market.MarketSlug))
			if len(allowed) > 0 && !allowed[slug] {
				return nil, page + 1, fmt.Errorf("PolyUS incentives symbol filter leaked unrequested market %q", slug)
			}
			rows = append(rows, market)
		}
		next := strings.TrimSpace(decoded.NextPageToken)
		if next == "" {
			return normalizeIncentivePrograms(rows), page + 1, nil
		}
		if next == token || seenToken[next] {
			return nil, page + 1, fmt.Errorf("PolyUS incentives pagination repeated token %q", next)
		}
		seenToken[next], token = true, next
	}
	return nil, maxPages, fmt.Errorf("PolyUS incentives exceeded %d-page safety bound", maxPages)
}

// ActiveIncentivePrograms performs the public, bounded, paginated production crawl. The endpoint
// is research input only: rewardPool is advertised capacity, never earned P&L.
func (p *PublicClient) ActiveIncentivePrograms(ctx context.Context) ([]IncentiveMarket, error) {
	if p == nil {
		return nil, fmt.Errorf("nil PolyUS public client")
	}
	rows, _, err := p.activeIncentiveProgramsQuery(ctx, nil, incentiveFullPageLimit)
	return rows, err
}

// ActiveIncentiveProgramsForSymbols is the runtime economics crawl. The production endpoint
// hard-caps pages at 100 groups regardless of a requested 500/1000, while the global active board
// exceeds 10,000 groups. The economics collector can only act on markets with a current exact book,
// so this official symbols filter is both the complete executable universe and 5x fewer requests.
// Every batch must finish; a timeout or ignored filter returns no partial snapshot.
func (p *PublicClient) ActiveIncentiveProgramsForSymbols(ctx context.Context, symbols []string) ([]IncentiveMarket, IncentiveCrawlStats, error) {
	stats := IncentiveCrawlStats{}
	if p == nil {
		return nil, stats, fmt.Errorf("nil PolyUS public client")
	}
	seen := map[string]bool{}
	clean := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		symbol = strings.ToLower(strings.TrimSpace(symbol))
		if symbol == "" || seen[symbol] {
			continue
		}
		seen[symbol] = true
		clean = append(clean, symbol)
	}
	sort.Strings(clean)
	stats.RequestedSymbols = len(clean)
	if len(clean) == 0 {
		stats.Complete = true
		return nil, stats, nil
	}
	var all []IncentiveMarket
	for start := 0; start < len(clean); start += incentiveSymbolBatchSize {
		end := start + incentiveSymbolBatchSize
		if end > len(clean) {
			end = len(clean)
		}
		rows, pages, err := p.activeIncentiveProgramsQuery(ctx, clean[start:end], incentiveBatchPageLimit)
		stats.Pages += pages
		if err != nil {
			return nil, stats, fmt.Errorf("PolyUS incentives symbol batch %d: %w", stats.Batches+1, err)
		}
		stats.Batches++
		all = append(all, rows...)
	}
	out := normalizeIncentivePrograms(all)
	stats.Programs, stats.Complete = len(out), true
	return out, stats, nil
}

func (p IncentivePeriod) ActiveAt(now time.Time) bool {
	if strings.EqualFold(strings.TrimSpace(p.Status), "closed") {
		return false
	}
	if start, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(p.Start)); err == nil && now.Before(start) {
		return false
	}
	if end, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(p.End)); err == nil && !now.Before(end) {
		return false
	}
	return true
}

type IncentiveEarning struct {
	Reward      float64 `json:"reward"`
	ProgramType string  `json:"program_type"`
	MarketSlug  string  `json:"market_slug"`
	Date        string  `json:"date"`
}

// IncentiveEarnings reads account-credited rewards. It is the only incentive value that may later
// enter realized economics; advertised reward pools remain capacity context. This method never
// places or modifies an order.
func (c *Client) IncentiveEarnings(ctx context.Context, startDate, endDate time.Time) ([]IncentiveEarning, error) {
	if c == nil {
		return nil, fmt.Errorf("nil PolyUS authenticated client")
	}
	q := url.Values{}
	if !startDate.IsZero() {
		q.Set("startDate", startDate.UTC().Format("2006-01-02"))
	}
	if !endDate.IsZero() {
		q.Set("endDate", endDate.UTC().Format("2006-01-02"))
	}
	path := "/v1/incentives/earnings"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	raw, code, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK || len(raw) == 0 || len(raw) > 2<<20 {
		return nil, fmt.Errorf("PolyUS incentive earnings invalid response: status=%d bytes=%d", code, len(raw))
	}
	var decoded struct {
		Rewards []struct {
			Reward      json.RawMessage `json:"reward"`
			ProgramType string          `json:"programType"`
			MarketSlug  string          `json:"marketSlug"`
			Date        string          `json:"date"`
		} `json:"rewards"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("PolyUS incentive earnings schema: %w", err)
	}
	out := make([]IncentiveEarning, 0, len(decoded.Rewards))
	for _, row := range decoded.Rewards {
		reward := rawFloat(row.Reward)
		slug := strings.ToLower(strings.TrimSpace(row.MarketSlug))
		if slug == "" || reward < 0 {
			continue
		}
		out = append(out, IncentiveEarning{Reward: reward, ProgramType: strings.TrimSpace(row.ProgramType),
			MarketSlug: slug, Date: strings.TrimSpace(row.Date)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Date == out[j].Date {
			return out[i].MarketSlug < out[j].MarketSlug
		}
		return out[i].Date < out[j].Date
	})
	return out, nil
}
