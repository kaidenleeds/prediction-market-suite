// Public market-data client for Polymarket US (https://gateway.polymarket.us). NO auth required.
// This is the READ side for the "trade Poly US off its own data" plan: list markets, fetch top-of-book
// (BBO) and full books, and settlement. The international Polymarket leaderboard / whale-flow signals
// are a SEPARATE system (lb-api / data-api on polymarket.com) and are intentionally untouched by this
// client -- Poly US has no leaderboard / public-wallet data, so those signals stay on international.
package polymarketus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ErrUnknownMarketLifecycle means a list page contained at least one market without the venue
// lifecycle fields needed to prove whether it is currently tradeable. Silently dropping that row
// would make a supposedly complete universe incomplete, so the crawler preserves last-known-good.
var ErrUnknownMarketLifecycle = errors.New("polyus market lifecycle fields absent")

// GatewayURL is the PUBLIC Poly US data API (markets, books, events, search) -- no key needed.
const GatewayURL = "https://gateway.polymarket.us"

// PublicClient reads Poly US public market data. Safe for concurrent use.
type PublicClient struct {
	baseURL           string
	http              *http.Client
	pageRetryBase     time.Duration // OpenMarketsAll only; package tests shorten this deterministically.
	pageRetryAttempts int           // total attempts for one offset, including the first request.
}

func NewPublicClient(timeout time.Duration) *PublicClient {
	return &PublicClient{
		baseURL:           GatewayURL,
		http:              &http.Client{Timeout: timeout},
		pageRetryBase:     250 * time.Millisecond,
		pageRetryAttempts: 3,
	}
}

func (p *PublicClient) get(ctx context.Context, path string) ([]byte, int, error) {
	raw, code, _, err := p.getWithHeaders(ctx, path)
	return raw, code, err
}

// getWithHeaders is the header-preserving form used by the complete-market crawler so a 429/5xx
// retry can obey Retry-After. Public one-shot methods keep the original get signature above.
func (p *PublicClient) getWithHeaders(ctx context.Context, path string) ([]byte, int, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path, nil)
	if err != nil {
		return nil, 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.http.Do(req)
	if err != nil {
		return nil, 0, nil, err
	}
	defer resp.Body.Close()
	raw, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return raw, resp.StatusCode, resp.Header.Clone(), fmt.Errorf("polyus GET %s body: %w", path, readErr)
	}
	if resp.StatusCode >= 400 {
		// NON-200 IS AN ERROR (audit §6): the exported wrappers used to return err==nil on a
		// 500/429, so an outage read as "empty book"/"no markets" and consumers silently cached
		// emptiness. The status code is still returned for callers that branch on it.
		return raw, resp.StatusCode, resp.Header.Clone(), fmt.Errorf("polyus GET %s: status %d", path, resp.StatusCode)
	}
	return raw, resp.StatusCode, resp.Header.Clone(), nil
}

// Markets fetches GET /v1/markets with a raw query string, e.g.
// "active=true&closed=false&limit=15&orderBy=volumeNum&orderDirection=desc". Returns the raw body so
// the caller can inspect the (not-fully-documented) list shape before we lock in a typed decoder.
func (p *PublicClient) Markets(ctx context.Context, query string) ([]byte, int, error) {
	path := "/v1/markets"
	if query != "" {
		path += "?" + query
	}
	return p.get(ctx, path)
}

// Market is the subset of /v1/markets fields we use (field names confirmed against a live response).
// Unknown fields are ignored, so this stays robust to schema additions.
type Market struct {
	ID           string       `json:"id"`
	Slug         string       `json:"slug"`
	Question     string       `json:"question"`
	Title        string       `json:"title"`
	Description  string       `json:"description"` // market-specific resolution/void terms; never infer them from the title
	EndDate      string       `json:"endDate"`
	Category     string       `json:"category"`
	MarketType   string       `json:"marketType"`         // deprecated compatibility; never sports authority
	SportsType   string       `json:"sportsMarketType"`   // current fine-grained retail structural truth
	SportsTypeV2 string       `json:"sportsMarketTypeV2"` // removed/deprecated Jun-29; legacy capture only
	GameStart    string       `json:"gameStartTime"`
	BestBid      float64      `json:"bestBid"` // nullable in JSON; null decodes to 0
	BestAsk      float64      `json:"bestAsk"`
	Line         float64      `json:"line"`
	Volume       float64      `json:"volume"`     // lifetime matched notional from the venue
	Volume24hr   float64      `json:"volume24hr"` // rolling 24h matched notional
	MinimumQty   float64      `json:"minimumTradeQty"`
	TickSize     float64      `json:"orderPriceMinTickSize"`
	FeeCoeff     *float64     `json:"feeCoefficient"` // taker theta; pointer distinguishes absent from an explicit zero
	Sides        []MarketSide `json:"marketSides"`
	// Lifecycle fields are pointers so an omitted field is distinguishable from an explicit false.
	// The retail API has exposed both the active/closed flags and the MARKET_STATE_* enum across
	// list/detail versions. A market is usable only when at least one affirmative source says open,
	// and every lifecycle source present agrees (see Open).
	Active   *bool  `json:"active"`
	Closed   *bool  `json:"closed"`
	Archived *bool  `json:"archived"`
	State    string `json:"state"`
	Status   string `json:"status"`
	// EP3Status is the lifecycle field currently served by both /v1/markets and market detail.
	// It is independent evidence: active=true never overrides PREOPEN/HALTED here.
	EP3Status string `json:"ep3Status"`
}

// UnmarshalJSON keeps Market's public numeric fields normalized as float64 while accepting every
// numeric representation the retail gateway has used: number, decimal string, or {value}. The
// current list BBO is bestBidQuote/bestAskQuote Amount objects; older snapshots used bare bestBid/
// bestAsk. Explicit malformed values fail the whole decode instead of quietly becoming zero.
func (m *Market) UnmarshalJSON(b []byte) error {
	type marketAlias Market
	aux := struct {
		*marketAlias
		BestBidRaw      json.RawMessage `json:"bestBid"`
		BestAskRaw      json.RawMessage `json:"bestAsk"`
		BestBidQuoteRaw json.RawMessage `json:"bestBidQuote"`
		BestAskQuoteRaw json.RawMessage `json:"bestAskQuote"`
		LineRaw         json.RawMessage `json:"line"`
		VolumeRaw       json.RawMessage `json:"volume"`
		Volume24hrRaw   json.RawMessage `json:"volume24hr"`
		MinimumQtyRaw   json.RawMessage `json:"minimumTradeQty"`
		TickSizeRaw     json.RawMessage `json:"orderPriceMinTickSize"`
		FeeCoeffRaw     json.RawMessage `json:"feeCoefficient"`
		EP3StatusSnake  string          `json:"ep3_status"`
	}{marketAlias: (*marketAlias)(m)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	if m.EP3Status == "" {
		m.EP3Status = aux.EP3StatusSnake
	}
	decode := func(name string, raw json.RawMessage, dst *float64) (bool, error) {
		v, present, err := decodeFlexAmount(raw)
		if err != nil {
			return present, fmt.Errorf("polyus market %s: %w", name, err)
		}
		if present {
			*dst = v
		}
		return present, nil
	}
	if _, err := decode("line", aux.LineRaw, &m.Line); err != nil {
		return err
	}
	if _, err := decode("volume", aux.VolumeRaw, &m.Volume); err != nil {
		return err
	}
	if _, err := decode("volume24hr", aux.Volume24hrRaw, &m.Volume24hr); err != nil {
		return err
	}
	if _, err := decode("minimumTradeQty", aux.MinimumQtyRaw, &m.MinimumQty); err != nil {
		return err
	}
	if _, err := decode("orderPriceMinTickSize", aux.TickSizeRaw, &m.TickSize); err != nil {
		return err
	}
	if _, err := decode("bestBid", aux.BestBidRaw, &m.BestBid); err != nil {
		return err
	}
	if _, err := decode("bestAsk", aux.BestAskRaw, &m.BestAsk); err != nil {
		return err
	}
	// Amount wrappers are the current source and win when supplied; bare fields are compatibility.
	if _, err := decode("bestBidQuote", aux.BestBidQuoteRaw, &m.BestBid); err != nil {
		return err
	}
	if _, err := decode("bestAskQuote", aux.BestAskQuoteRaw, &m.BestAsk); err != nil {
		return err
	}
	if v, present, err := decodeFlexAmount(aux.FeeCoeffRaw); err != nil {
		return fmt.Errorf("polyus market feeCoefficient: %w", err)
	} else if present {
		m.FeeCoeff = new(float64)
		*m.FeeCoeff = v
	} else {
		m.FeeCoeff = nil
	}
	return nil
}

// LifecycleKnown reports whether the venue supplied enough lifecycle metadata to make an
// evidence-based open/closed decision. Missing fields are not interpreted as zero/false.
func (m Market) LifecycleKnown() bool {
	return m.Active != nil || m.Closed != nil || m.Archived != nil ||
		strings.TrimSpace(m.State) != "" || strings.TrimSpace(m.Status) != "" ||
		strings.TrimSpace(m.EP3Status) != ""
}

// Terminal reports explicit venue evidence that a market is not currently tradeable. Unlike Open,
// a lone closed:false flag is not enough to return true or false about readiness; this distinction
// lets the league live-feed path reject known dead children without treating a partial schema as a
// reason to erase the whole event tree.
func (m Market) Terminal() bool {
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

func lifecycleState(s string) string {
	s = strings.ToUpper(strings.TrimSpace(s))
	for _, prefix := range []string{"MARKET_STATE_", "INSTRUMENT_STATE_", "MARKET_STATUS_"} {
		s = strings.TrimPrefix(s, prefix)
	}
	return s
}

// Open is deliberately strict: terminal/archive flags always win; any supplied state must be
// OPEN; any supplied active flag must be true; and at least one field must affirm liveness. This
// prevents PREOPEN/SUSPENDED/HALTED/EXPIRED/TERMINATED markets and schema-unknown rows from being
// admitted merely because a query string requested active=true.
func (m Market) Open() bool {
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

func firstNonBlank(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

// SportsKind maps only the current fine-grained sportsMarketType. SportsTypeV2 remains decoded so
// old stored payloads are readable, but the removed field may never classify a live market.
func (m Market) SportsKind() string {
	current := strings.ToLower(strings.TrimSpace(m.SportsType))
	if current != "" {
		switch {
		case strings.HasSuffix(current, "_to_advance"):
			return "advance"
		case strings.Contains(current, "spread"):
			return "spread"
		case strings.Contains(current, "total"):
			return "total"
		case strings.Contains(current, "winner") || strings.Contains(current, "moneyline") ||
			strings.Contains(current, "drawable_outcome"):
			return "winner"
		case strings.Contains(current, "future"):
			return "future"
		default:
			return "prop"
		}
	}
	return ""
}

func (m Market) IsMoneyline() bool { k := m.SportsKind(); return k == "winner" || k == "advance" }
func (m Market) IsSpread() bool    { return m.SportsKind() == "spread" }
func (m Market) IsTotal() bool     { return m.SportsKind() == "total" }

// IsToAdvance identifies the venue's knockout "to advance" class (R106 bug 255, live Jul-9 with the
// World Cup quarter-finals): sportsMarketType "soccer_game_to_advance" (live-probed 2026-07-07 on
// aadc-fwc-fra-mar-2026-07-09-to-advance). sportsMarketTypeV2 still reports MONEYLINE for these, so
// the v1 string is the ONLY venue metadata that distinguishes "advance (incl. ET/pens)" from a
// regulation-time winner — classifying by it prevents the advance↔90-min-moneyline wrong-twin.
func (m Market) IsToAdvance() bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(m.SportsType)), "_to_advance")
}

// TwoSidedSingle reports the R106 single-slug-both-teams instrument shape (bug 255): exactly two
// sides, BOTH carrying the market's own slug as their identifier (in the legacy shape each side's
// identifier is that side's own per-team market slug), with two distinct teams. Structural venue
// truth — no title/slug guessing.
func (m Market) TwoSidedSingle() bool {
	if len(m.Sides) != 2 || m.Slug == "" {
		return false
	}
	for _, sd := range m.Sides {
		if !strings.EqualFold(strings.TrimSpace(sd.Identifier), m.Slug) {
			return false
		}
	}
	a, b := m.Sides[0].Team, m.Sides[1].Team
	if a.Name == "" && a.Abbreviation == "" || b.Name == "" && b.Abbreviation == "" {
		return false
	}
	return !strings.EqualFold(a.Abbreviation, b.Abbreviation) || !strings.EqualFold(a.Name, b.Name)
}

// MarketSide is one outcome of a market. For sports it carries the team. price is a decimal string.
type MarketSide struct {
	Identifier  string `json:"identifier"`
	Description string `json:"description"` // "Yes" / "No"
	Price       string `json:"price"`       // e.g. "0.0210"
	Long        bool   `json:"long"`
	Team        Team   `json:"team"`
}

func (s *MarketSide) UnmarshalJSON(b []byte) error {
	type sideAlias MarketSide
	aux := struct {
		*sideAlias
		PriceRaw json.RawMessage `json:"price"`
		QuoteRaw json.RawMessage `json:"quote"`
	}{sideAlias: (*sideAlias)(s)}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	price, present, err := decodeMarketSidePrice(aux.PriceRaw)
	if err != nil {
		return fmt.Errorf("polyus market side price: %w", err)
	}
	if !present {
		price, present, err = decodeMarketSidePrice(aux.QuoteRaw)
		if err != nil {
			return fmt.Errorf("polyus market side quote: %w", err)
		}
	}
	if present {
		s.Price = strconv.FormatFloat(price, 'f', -1, 64)
	}
	return nil
}

// decodeMarketSidePrice is decodeFlexAmount with one venue-specific schema guard. The live list
// feed has emitted the Amount currency token ("USD") through the side `price` slot on isolated
// rows. Currency metadata is not a price and must not abort the otherwise complete REST-open crawl;
// treat it as absent so the sibling `quote` Amount can supply the value. Other malformed supplied
// strings still fail loudly.
func decodeMarketSidePrice(raw json.RawMessage) (float64, bool, error) {
	var text string
	if len(raw) > 0 && json.Unmarshal(raw, &text) == nil && strings.EqualFold(strings.TrimSpace(text), "USD") {
		return 0, false, nil
	}
	return decodeFlexAmount(raw)
}

// Team is the embedded team info on a sports market side (the key for whale-signal -> Poly US mapping).
// R102 STRUCTURAL MATCHING adds the venue's own home/away tag (market-side team objects carry
// "ordering":"away"/"home" — live-probed 2026-07-07) and the display abbreviation ("COL" vs the
// lowercase "col" in abbreviation) — both additive decodes.
type Team struct {
	Name         string `json:"name"`
	Abbreviation string `json:"abbreviation"`
	League       string `json:"league"`
	Ordering     string `json:"ordering"`            // "away" / "home" on market-side team objects; "" elsewhere
	DisplayAbbr  string `json:"displayAbbreviation"` // upper-case venue display abbrev
}

// YesPrice returns the long/YES side price as a float (0 if absent).
func (m Market) YesPrice() float64 {
	for _, s := range m.Sides {
		if s.Long || strings.EqualFold(s.Description, "Yes") {
			f, _ := strconv.ParseFloat(strings.TrimSpace(s.Price), 64)
			return f
		}
	}
	return 0
}

// LongTeam returns the long/YES side's team (best-effort).
func (m Market) LongTeam() Team {
	for _, s := range m.Sides {
		if s.Long || strings.EqualFold(s.Description, "Yes") {
			return s.Team
		}
	}
	if len(m.Sides) > 0 {
		return m.Sides[0].Team
	}
	return Team{}
}

// ShortTeam returns the short/NO side's team of a two-sided instrument (zero Team when the market
// has no distinct short side). R106 bug 255: on single-slug-both-teams markets the short side IS
// the second team — its price = 1 − the long/YES price on the one shared book.
func (m Market) ShortTeam() Team {
	for _, s := range m.Sides {
		if !s.Long && !strings.EqualFold(s.Description, "Yes") &&
			(s.Team.Name != "" || s.Team.Abbreviation != "") {
			return s.Team
		}
	}
	return Team{}
}

// MarketsList fetches GET /v1/markets and decodes the {"markets":[...]} wrapper into Markets (string
// fields only). Returns the raw body too so callers can inspect/lock the full shape.
func (p *PublicClient) MarketsList(ctx context.Context, query string) ([]Market, []byte, int, error) {
	markets, raw, code, _, err := p.marketsListOnce(ctx, query)
	return markets, raw, code, err
}

func (p *PublicClient) marketsListOnce(ctx context.Context, query string) ([]Market, []byte, int, http.Header, error) {
	path := "/v1/markets"
	if query != "" {
		path += "?" + query
	}
	raw, code, header, err := p.getWithHeaders(ctx, path)
	if err != nil || code != 200 {
		return nil, raw, code, header, err
	}
	markets, err := decodeMarketPage(raw)
	if err != nil {
		return nil, raw, code, header, err
	}
	return markets, raw, code, header, nil
}

// decodeMarketPage accepts the three list envelopes the retail gateway has used: {markets:[...]},
// {data:[...]}, and a bare array. Keeping this decoder shared ensures a wrapper migration cannot
// silently turn the crawler into an empty universe.
func decodeMarketPage(raw []byte) ([]Market, error) {
	var w struct {
		Markets json.RawMessage `json:"markets"`
		Data    json.RawMessage `json:"data"`
	}
	wrapperErr := json.Unmarshal(raw, &w)
	if wrapperErr == nil {
		for _, body := range []json.RawMessage{w.Markets, w.Data} {
			if len(body) == 0 || string(body) == "null" {
				continue
			}
			var markets []Market
			if err := json.Unmarshal(body, &markets); err != nil {
				return nil, err
			}
			return markets, nil
		}
	} else if trimmed := bytes.TrimSpace(raw); len(trimmed) > 0 && trimmed[0] == '{' {
		// Preserve "unexpected end of JSON input" for a truncated wrapper. Falling through to the
		// bare-array decoder masks it as an object/type error and would make a safe page retry
		// impossible to distinguish from a permanent schema failure.
		return nil, wrapperErr
	}
	var markets []Market
	if err := json.Unmarshal(raw, &markets); err != nil {
		return nil, err
	}
	return markets, nil
}

// MarketCrawlStats is the completeness receipt for OpenMarketsAll. Seen is the raw number of rows
// received (including duplicates/closed rows); Unique is the distinct market count; Open is what
// survived the local lifecycle proof. UnknownLifecycle rows are refused, never guessed open.
type MarketCrawlStats struct {
	Pages            int
	Seen             int
	Unique           int
	Open             int
	UnknownLifecycle int
}

// The gateway currently caps list responses at 500 rows even when a larger limit is requested.
// Request that full page so the 67k-row board needs about 135 calls instead of about 269.
const marketsPageSize = 500

// A full crawl remains strictly sequential. At most two retries are spent on the current offset;
// successful prior pages are never fetched again and no partial universe escapes on exhaustion.
const marketsPageRetryAttempts = 3

func retryAfterDelay(raw string, now time.Time) (time.Duration, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(raw)
	if err != nil {
		return 0, false
	}
	if delay := when.Sub(now); delay > 0 {
		return delay, true
	}
	return 0, true
}

func waitMarketsRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func transientMarketsTransport(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	// Go's HTTP/2 GOAWAY error is not guaranteed to implement net.Error. A graceful server-side
	// connection rotation can therefore arrive while reading a perfectly retryable page body as
	// "server sent GOAWAY and closed the connection". Keep this deliberately narrow: schema and
	// ordinary HTTP 4xx errors must still fail immediately.
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "server sent goaway") ||
		strings.Contains(text, "http2: goaway") ||
		strings.Contains(text, "unexpected eof")
}

// RetryableMarketsCrawlError reports whether a failed complete-universe crawl should be retried
// promptly by its owner. It never makes a partial crawl authoritative. Transport interruptions,
// an exhausted crawl deadline, and a moving offset boundary are recoverable only by another full,
// lifecycle-validated crawl; schema/lifecycle failures retain the ordinary slower cadence.
func RetryableMarketsCrawlError(err error) bool {
	if err == nil {
		return false
	}
	if transientMarketsTransport(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "pagination made no progress")
}

func truncatedMarketsJSON(err error) bool {
	if err == nil {
		return false
	}
	var syntaxErr *json.SyntaxError
	return errors.As(err, &syntaxErr) && strings.Contains(strings.ToLower(syntaxErr.Error()), "unexpected end")
}

func (p *PublicClient) marketsPage(ctx context.Context, query string) ([]Market, int, error) {
	attempts := p.pageRetryAttempts
	if attempts <= 0 {
		attempts = marketsPageRetryAttempts
	}
	base := p.pageRetryBase
	if base <= 0 {
		base = 250 * time.Millisecond
	}
	var lastErr error
	var lastCode int
	for attempt := 0; attempt < attempts; attempt++ {
		page, _, code, header, err := p.marketsListOnce(ctx, query)
		if err == nil && code == http.StatusOK {
			return page, code, nil
		}
		lastErr, lastCode = err, code
		if ctx.Err() != nil {
			return nil, code, ctx.Err()
		}
		transient := code == http.StatusTooManyRequests || code >= 500 && code <= 599 ||
			code == http.StatusOK && truncatedMarketsJSON(err) || transientMarketsTransport(err)
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
			return nil, code, err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("status %d", lastCode)
	}
	return nil, lastCode, lastErr
}

// OpenMarketsAll crawls the complete /v1/markets result set with offset pagination and locally
// re-validates lifecycle truth. It intentionally does not impose a row/page ceiling: completion is
// an empty page, while the caller's context is the time/request budget. Advancing by the ACTUAL
// page length handles gateways that silently cap below the requested 250 rows. A repeated page is
// an error rather than a silently truncated "full" universe.
//
// query may carry category/date/order filters; active/closed/limit/offset are owned by this method
// and overwritten. On any incomplete/erroring crawl, no partial rows are returned so consumers can
// retain their last-known-good snapshot.
func (p *PublicClient) OpenMarketsAll(ctx context.Context, query string) ([]Market, MarketCrawlStats, error) {
	q, err := url.ParseQuery(query)
	if err != nil {
		return nil, MarketCrawlStats{}, fmt.Errorf("polyus markets query: %w", err)
	}
	q.Set("active", "true")
	q.Set("closed", "false")
	q.Set("limit", strconv.Itoa(marketsPageSize))
	q.Del("offset")

	stats := MarketCrawlStats{}
	seen := make(map[string]struct{})
	open := make([]Market, 0, marketsPageSize)
	offset := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil, stats, err
		}
		q.Set("offset", strconv.Itoa(offset))
		page, code, err := p.marketsPage(ctx, q.Encode())
		if err != nil || code != http.StatusOK {
			if err == nil {
				err = fmt.Errorf("status %d", code)
			}
			return nil, stats, fmt.Errorf("polyus markets crawl offset %d: %w", offset, err)
		}
		stats.Pages++
		stats.Seen += len(page)
		if len(page) == 0 {
			break
		}

		progress := 0
		for _, m := range page {
			key := strings.TrimSpace(m.Slug)
			if key == "" {
				key = strings.TrimSpace(m.ID)
			}
			if key == "" {
				continue
			}
			key = strings.ToLower(key)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			progress++
			stats.Unique++
			if !m.LifecycleKnown() {
				stats.UnknownLifecycle++
				continue
			}
			if m.Open() {
				open = append(open, m)
			}
		}
		if progress == 0 {
			return nil, stats, fmt.Errorf("polyus markets pagination made no progress at offset %d", offset)
		}
		offset += len(page)
	}
	if stats.UnknownLifecycle > 0 {
		return nil, stats, ErrUnknownMarketLifecycle
	}
	stats.Open = len(open)
	return open, stats, nil
}

// Event is a game/event from /v2/leagues/{league}/events. It embeds the event's markets (with prices)
// + live game state (score/period/live) + participants (teams) — the authoritative way to find
// today's games and identify them by team/league/start-time rather than parsing slugs.
type Event struct {
	ID           string        `json:"id"`
	Slug         string        `json:"slug"`
	Title        string        `json:"title"`
	StartTime    string        `json:"startTime"`
	EventDate    string        `json:"eventDate"`
	EndDate      string        `json:"endDate"`
	Live         bool          `json:"live"`
	Ended        bool          `json:"ended"`
	Score        string        `json:"score"`
	Period       string        `json:"period"`
	GameID       int           `json:"gameId"`
	Markets      []Market      `json:"markets"`
	Participants []Participant `json:"participants"`
	Teams        []Team        `json:"teams"`
	Active       *bool         `json:"active"`
	Closed       *bool         `json:"closed"`
	Archived     *bool         `json:"archived"`
	State        string        `json:"state"`
	Status       string        `json:"status"`
	EP3Status    string        `json:"ep3Status"`
}

// LifecycleKnown reports whether the event payload carried any explicit lifecycle field. League
// event responses have historically omitted these fields even while their nested market rows are
// usable, so callers that already have a live-feed guarantee can distinguish "unknown" from an
// explicit terminal state instead of treating an absent field as false.
func (e Event) LifecycleKnown() bool {
	return e.Ended || e.Active != nil || e.Closed != nil || e.Archived != nil ||
		strings.TrimSpace(e.State) != "" || strings.TrimSpace(e.Status) != "" ||
		strings.TrimSpace(e.EP3Status) != ""
}

// Terminal reports explicit venue evidence that an event is not currently tradeable. See
// Market.Terminal for why this is intentionally distinct from the stricter Open proof.
func (e Event) Terminal() bool {
	if e.Ended || e.Archived != nil && *e.Archived || e.Closed != nil && *e.Closed || e.Active != nil && !*e.Active {
		return true
	}
	for _, raw := range []string{e.State, e.Status, e.EP3Status} {
		if state := lifecycleState(raw); state != "" && state != "OPEN" && state != "ACTIVE" {
			return true
		}
	}
	return false
}

// Open reports whether event-level lifecycle metadata proves the event is still open. Ended,
// archived, closed, and non-OPEN states are terminal even if another stale flag says active.
func (e Event) Open() bool {
	if e.Ended || e.Archived != nil && *e.Archived || e.Closed != nil && *e.Closed {
		return false
	}
	affirmed := false
	for _, raw := range []string{e.State, e.Status, e.EP3Status} {
		if state := lifecycleState(raw); state != "" {
			if state != "OPEN" && state != "ACTIVE" {
				return false
			}
			affirmed = true
		}
	}
	if e.Active != nil {
		if !*e.Active {
			return false
		}
		affirmed = true
	}
	return affirmed
}

// OpenMarkets returns every locally-proven open child; there is deliberately no per-event child
// cap. Large games can carry dozens of spreads, totals, alternate lines, and player props, and the
// whole tree is part of the venue universe. The returned slice is detached from e.Markets.
func (e Event) OpenMarkets() []Market {
	if !e.Open() {
		return nil
	}
	out := make([]Market, 0, len(e.Markets))
	for _, m := range e.Markets {
		if m.Open() {
			out = append(out, m)
		}
	}
	return out
}

// OpenEvents filters a league response at both event and child-market lifecycle levels. Events
// with no proven-open children are dropped; every retained event owns a copied, uncapped child
// slice so callers cannot accidentally fall back to the raw mixed-lifecycle list.
func OpenEvents(events []Event) []Event {
	out := make([]Event, 0, len(events))
	for _, event := range events {
		markets := event.OpenMarkets()
		if len(markets) == 0 {
			continue
		}
		event.Markets = markets
		out = append(out, event)
	}
	return out
}

// Participant is an event participant (team for sports). The team {name,abbreviation,league} is the
// key for mapping an international whale signal to this Poly US event.
type Participant struct {
	ID   string `json:"id"`
	Team Team   `json:"team"`
}

// Moneylines returns the event's full-game moneyline markets (one per team side).
func (e Event) Moneylines() []Market {
	var out []Market
	for _, m := range e.Markets {
		if m.IsMoneyline() {
			out = append(out, m)
		}
	}
	return out
}

// LeagueEvents fetches GET /v2/leagues/{league}/events. evType "sport" = games, "futures" = futures.
// Returns the decoded events plus the raw body.
func (p *PublicClient) LeagueEvents(ctx context.Context, league, evType string) ([]Event, []byte, int, error) {
	path := "/v2/leagues/" + url.PathEscape(league) + "/events"
	if evType != "" {
		path += "?type=" + url.QueryEscape(evType)
	}
	raw, code, err := p.get(ctx, path)
	if err != nil || code != 200 {
		return nil, raw, code, err
	}
	var w struct {
		Events []Event `json:"events"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, raw, code, err
	}
	return w.Events, raw, code, nil
}

// MarketRaw fetches GET /v1/market/slug/{slug} (single market detail) as raw JSON.
func (p *PublicClient) MarketRaw(ctx context.Context, slug string) ([]byte, int, error) {
	return p.get(ctx, "/v1/market/slug/"+url.PathEscape(slug))
}

// MarketBySlug fetches + decodes GET /v1/market/slug/{slug} — the auditor's bug-255 read: the
// single-market detail carries the full marketSides array (live-probed 2026-07-07: the response
// wrapper is {"market": {...}}), which is the canonical source for a two-sided instrument's sides
// when the market didn't arrive through an event feed.
func (p *PublicClient) MarketBySlug(ctx context.Context, slug string) (Market, []byte, int, error) {
	raw, code, err := p.MarketRaw(ctx, slug)
	if err != nil || code != 200 {
		return Market{}, raw, code, err
	}
	var w struct {
		Market Market `json:"market"`
	}
	if err := json.Unmarshal(raw, &w); err != nil {
		return Market{}, raw, code, err
	}
	return w.Market, raw, code, nil
}

// BBO is the parsed top-of-book for a Poly US market (prices in dollars, 0..1).
type BBO struct {
	Slug        string
	BestBid     float64
	BestAsk     float64
	CurrentPx   float64
	LastTradePx float64
}

// amt pulls a v1Amount {"value":"0.55","currency":"USD"} field out of a raw object map as a float.
func amt(m map[string]json.RawMessage, key string) float64 {
	raw, ok := m[key]
	if !ok {
		return 0
	}
	var a struct {
		Value string `json:"value"`
	}
	if json.Unmarshal(raw, &a) != nil {
		return 0
	}
	f, _ := strconv.ParseFloat(strings.TrimSpace(a.Value), 64)
	return f
}

// MarketBBO fetches + parses GET /v1/markets/{slug}/bbo (documented shape: marketData.{bestBid,
// bestAsk,currentPx,lastTradePx} as v1Amount). Returns the parsed BBO plus the raw body.
func (p *PublicClient) MarketBBO(ctx context.Context, slug string) (*BBO, []byte, int, error) {
	raw, code, err := p.get(ctx, "/v1/markets/"+url.PathEscape(slug)+"/bbo")
	if err != nil || code != 200 {
		return nil, raw, code, err
	}
	var wrap struct {
		MarketData map[string]json.RawMessage `json:"marketData"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return nil, raw, code, err
	}
	md := wrap.MarketData
	b := &BBO{
		BestBid:     amt(md, "bestBid"),
		BestAsk:     amt(md, "bestAsk"),
		CurrentPx:   amt(md, "currentPx"),
		LastTradePx: amt(md, "lastTradePx"),
	}
	if s, ok := md["marketSlug"]; ok {
		_ = json.Unmarshal(s, &b.Slug)
	}
	return b, raw, code, nil
}

// Book fetches GET /v1/markets/{slug}/book (full depth) as raw JSON.
func (p *PublicClient) Book(ctx context.Context, slug string) ([]byte, int, error) {
	return p.get(ctx, "/v1/markets/"+url.PathEscape(slug)+"/book")
}

// rawFloatOK parses a JSON value that may be a number, a string, or a v1Amount
// {"value":"0.5"}, while preserving whether decoding actually succeeded. Settlement uses the
// boolean because malformed venue data must never silently become the valid final value zero.
func rawFloatOK(r json.RawMessage) (float64, bool) {
	if len(r) == 0 {
		return 0, false
	}
	var f float64
	if json.Unmarshal(r, &f) == nil {
		return f, !math.IsNaN(f) && !math.IsInf(f, 0)
	}
	var s string
	if json.Unmarshal(r, &s) == nil {
		v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		return v, err == nil && !math.IsNaN(v) && !math.IsInf(v, 0)
	}
	var a struct {
		Value string `json:"value"`
	}
	if json.Unmarshal(r, &a) == nil {
		v, err := strconv.ParseFloat(strings.TrimSpace(a.Value), 64)
		return v, err == nil && !math.IsNaN(v) && !math.IsInf(v, 0)
	}
	return 0, false
}

func rawFloat(r json.RawMessage) float64 {
	v, _ := rawFloatOK(r)
	return v
}

func fieldFloat(o map[string]json.RawMessage, keys ...string) float64 {
	for _, k := range keys {
		if r, ok := o[k]; ok {
			if v := rawFloat(r); v != 0 {
				return v
			}
		}
	}
	return 0
}

// sumLevels totals the resting SIZE across order-book levels in any of the common encodings:
// [{price,size}], [{px,qty}], or [[price,size]] pairs (size as number/string/v1Amount).
func sumLevels(raw json.RawMessage) float64 {
	if len(raw) == 0 {
		return 0
	}
	var objs []map[string]json.RawMessage
	if json.Unmarshal(raw, &objs) == nil && len(objs) > 0 {
		total := 0.0
		for _, o := range objs {
			total += fieldFloat(o, "size", "quantity", "qty", "amount")
		}
		return total
	}
	var pairs [][]json.RawMessage
	if json.Unmarshal(raw, &pairs) == nil && len(pairs) > 0 {
		total := 0.0
		for _, p := range pairs {
			if len(p) >= 2 {
				total += rawFloat(p[1])
			}
		}
		return total
	}
	return 0
}

// BookDepth best-effort parses a /v1/markets/{slug}/book response into total resting bid vs ask size
// — the anonymous order-flow / pressure the Poly US book exposes (like Kalshi's book). The book shape
// isn't fully documented, so it tries the common wrappers ({book:{bids,asks}}, {marketData:{...}},
// bare {bids,asks}) and the level encodings above. Returns ok=false if no book is found, so the
// caller degrades gracefully (falls back to price-move momentum).
func BookDepth(raw []byte) (bidQty, askQty float64, ok bool) {
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil {
		return 0, 0, false
	}
	for _, key := range []string{"book", "marketData", "data", "orderBook", "orderbook"} {
		if inner, has := top[key]; has {
			var m map[string]json.RawMessage
			if json.Unmarshal(inner, &m) == nil {
				if _, hb := m["bids"]; hb {
					top = m
					break
				}
				if _, hb := m["buy"]; hb {
					top = m
					break
				}
			}
		}
	}
	bidQty = sumLevels(top["bids"])
	askQty = sumLevels(top["asks"])
	if askQty == 0 {
		askQty = sumLevels(top["offers"]) // Poly US names the ask side "offers", not "asks"
	}
	if bidQty == 0 && askQty == 0 {
		bidQty, askQty = sumLevels(top["buy"]), sumLevels(top["sell"])
	}
	return bidQty, askQty, bidQty > 0 || askQty > 0
}

// BookSample reveals the raw order-book shape for the probe: market state, level counts, and the first
// raw bid/offer level — so on a LIVE/open market we can confirm the per-level encoding (price/size
// field names) before wiring the order-flow signal.
func BookSample(raw []byte) (state, firstBid, firstOffer string, nBids, nOffers int) {
	var wrap struct {
		MarketData struct {
			Bids   []json.RawMessage `json:"bids"`
			Offers []json.RawMessage `json:"offers"`
			State  string            `json:"state"`
		} `json:"marketData"`
	}
	if json.Unmarshal(raw, &wrap) != nil {
		return "", "", "", 0, 0
	}
	state = wrap.MarketData.State
	nBids, nOffers = len(wrap.MarketData.Bids), len(wrap.MarketData.Offers)
	if nBids > 0 {
		firstBid = string(wrap.MarketData.Bids[0])
	}
	if nOffers > 0 {
		firstOffer = string(wrap.MarketData.Offers[0])
	}
	return
}

// BookData is the parsed /v1/markets/{slug}/book: best bid/ask + total resting depth per side (→
// imbalance) from the order book, plus the stats block (traded volume, last/current/open price). This
// is the Poly US order-flow signal source — anonymous order + trade pressure, like Kalshi's book/tape.
const FinalSettlementEndpointAuthority = "GET /v1/markets/{slug}/settlement"

type BookData struct {
	Slug         string
	State        string
	BestBid      float64
	BestAsk      float64
	BestBidQty   float64 // executable contracts resting exactly at BestBid
	BestAskQty   float64 // executable contracts resting exactly at BestAsk
	BidQty       float64 // total resting size on the bid side
	AskQty       float64 // total resting size on the offer side
	Imbalance    float64 // (BidQty-AskQty)/(BidQty+AskQty), -1..1 (>0 = buy pressure)
	Volume       float64 // notionalTraded (USD) — like a 24h volume
	SharesTraded float64
	LastTradePx  float64
	CurrentPx    float64
	OpenPx       float64
	SettlementPx float64
	// HasSettlement reports whether the venue actually SENT a settlementPx (audit #7): the JSON
	// zero-value made "absent" indistinguishable from a real 0.00 (NO wins), so a CLOSED-but-ungraded
	// book used to settle every YES signal as a loss. Consumers must check this before trusting 0.
	HasSettlement bool
	// Final-settlement authority is separate from the numeric settlement field. PolyUS publishes
	// intraday/preliminary marks in settlementPx too; treating the mere presence of that number as
	// a binary result fabricated settlements (for example 0.53 and 0.61 payouts). A final event
	// settlement must explicitly say preliminary=false and identify the tier-1 event method.
	SettlementPreliminary    bool
	HasSettlementPreliminary bool
	SettlementMethod         string
	SettlementText           string
	SettlementSetTime        string
	SettlementAuthority      string
	OpenInterest             float64
}

// SettledYes returns exact final binary truth only after the lifecycle is terminal. A terminal
// state plus settlementPx is insufficient: the book must carry explicit non-preliminary tier-1
// authority, or BookFull must have verified the separate settlement endpoint. Keep this contract
// beside the decoder so a live/preliminary mark can never grade a position again.
func (b BookData) SettledYes() (float64, bool) {
	bookAuthority := b.HasSettlementPreliminary && !b.SettlementPreliminary &&
		polyUSFinalEventSettlementMethod(b.SettlementMethod)
	dedicatedAuthority := b.SettlementAuthority == FinalSettlementEndpointAuthority
	if !b.HasSettlement || (!bookAuthority && !dedicatedAuthority) ||
		(b.SettlementPx != 0 && b.SettlementPx != 1) ||
		math.IsNaN(b.SettlementPx) || math.IsInf(b.SettlementPx, 0) {
		return 0, false
	}
	state := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(b.State)), "MARKET_STATE_")
	switch state {
	case "RESOLVED", "SETTLED", "EXPIRED", "TERMINATED", "GRADED", "CLOSED":
		return b.SettlementPx, true
	}
	return 0, false
}

func polyUSSettlementLifecycleTerminal(state string) bool {
	state = strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(state)), "MARKET_STATE_")
	switch state {
	case "RESOLVED", "SETTLED", "EXPIRED", "TERMINATED", "GRADED", "CLOSED":
		return true
	default:
		return false
	}
}

func polyUSFinalEventSettlementMethod(method string) bool {
	switch strings.ToUpper(strings.TrimSpace(method)) {
	case "SETTLEMENT_PRICE_CALCULATION_METHOD_EVENT_TIER_1", "EVENT_TIER_1":
		return true
	default:
		return false
	}
}

func rawBoolOK(r json.RawMessage) (bool, bool) {
	if len(r) == 0 {
		return false, false
	}
	var b bool
	if json.Unmarshal(r, &b) == nil {
		return b, true
	}
	var s string
	if json.Unmarshal(r, &s) == nil {
		v, err := strconv.ParseBool(strings.TrimSpace(s))
		return v, err == nil
	}
	return false, false
}

func rawString(r json.RawMessage) string {
	var s string
	if json.Unmarshal(r, &s) == nil {
		return strings.TrimSpace(s)
	}
	var a struct {
		Value string `json:"value"`
	}
	if json.Unmarshal(r, &a) == nil {
		return strings.TrimSpace(a.Value)
	}
	return ""
}

// ParseBook parses the confirmed book shape: marketData.{bids,offers}[] with levels
// {"px":{"value":..},"qty":"<size>"}, plus marketData.stats.{notionalTraded,lastTradePx,...}.
func ParseBook(raw []byte, slug string) *BookData {
	var wrap struct {
		MarketData struct {
			Slug  string `json:"marketSlug"`
			State string `json:"state"`
			Bids  []struct {
				Px struct {
					Value string `json:"value"`
				} `json:"px"`
				Qty string `json:"qty"`
			} `json:"bids"`
			Offers []struct {
				Px struct {
					Value string `json:"value"`
				} `json:"px"`
				Qty string `json:"qty"`
			} `json:"offers"`
			Stats map[string]json.RawMessage `json:"stats"`
		} `json:"marketData"`
	}
	bd := &BookData{Slug: slug}
	if json.Unmarshal(raw, &wrap) != nil {
		return bd
	}
	md := wrap.MarketData
	if md.Slug != "" {
		bd.Slug = md.Slug
	}
	bd.State = md.State
	pf := func(s string) float64 { f, _ := strconv.ParseFloat(strings.TrimSpace(s), 64); return f }
	for _, b := range md.Bids {
		bd.BidQty += pf(b.Qty)
	}
	for _, o := range md.Offers {
		bd.AskQty += pf(o.Qty)
	}
	if len(md.Bids) > 0 {
		bd.BestBid = pf(md.Bids[0].Px.Value)
		bd.BestBidQty = pf(md.Bids[0].Qty)
	}
	if len(md.Offers) > 0 {
		bd.BestAsk = pf(md.Offers[0].Px.Value)
		bd.BestAskQty = pf(md.Offers[0].Qty)
	}
	if bd.BidQty+bd.AskQty > 0 {
		bd.Imbalance = (bd.BidQty - bd.AskQty) / (bd.BidQty + bd.AskQty)
	}
	bd.Volume = fieldFloat(md.Stats, "notionalTraded")
	bd.SharesTraded = fieldFloat(md.Stats, "sharesTraded")
	bd.LastTradePx = fieldFloat(md.Stats, "lastTradePx")
	bd.CurrentPx = fieldFloat(md.Stats, "currentPx")
	bd.OpenPx = fieldFloat(md.Stats, "openPx")
	if r, ok := md.Stats["settlementPx"]; ok { // present ≠ zero-value (audit #7)
		if t := strings.TrimSpace(string(r)); t != "" && t != "null" && t != `""` {
			if settlement, parsed := rawFloatOK(r); parsed {
				bd.SettlementPx = settlement
				bd.HasSettlement = true
			}
		}
	}
	if r, ok := md.Stats["settlementPreliminary"]; ok {
		if preliminary, parsed := rawBoolOK(r); parsed {
			bd.SettlementPreliminary = preliminary
			bd.HasSettlementPreliminary = true
		}
	}
	if !bd.HasSettlementPreliminary {
		if r, ok := md.Stats["settlementPreliminaryFlag"]; ok {
			if preliminary, parsed := rawBoolOK(r); parsed {
				bd.SettlementPreliminary = preliminary
				bd.HasSettlementPreliminary = true
			}
		}
	}
	bd.SettlementMethod = rawString(md.Stats["settlementPriceCalculationMethod"])
	bd.SettlementText = rawString(md.Stats["settlementPriceCalculationText"])
	bd.SettlementSetTime = rawString(md.Stats["settlementSetTime"])
	bd.OpenInterest = fieldFloat(md.Stats, "openInterest")
	return bd
}

// Settlement fetches the dedicated retail settlement endpoint. Unlike settlementPx in a book,
// this endpoint exists specifically to return a resolved market's payout. We still require exact
// identity and an exact binary 0/1 value before exposing it as authority.
func (p *PublicClient) Settlement(ctx context.Context, slug string) (float64, []byte, int, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return 0, nil, 0, errors.New("empty PolyUS settlement slug")
	}
	raw, code, err := p.get(ctx, "/v1/markets/"+url.PathEscape(slug)+"/settlement")
	if err != nil {
		return 0, raw, code, err
	}
	if code != http.StatusOK {
		return 0, raw, code, fmt.Errorf("PolyUS settlement %s: status %d", slug, code)
	}
	var payload struct {
		Slug       string          `json:"slug"`
		Settlement json.RawMessage `json:"settlement"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, raw, code, fmt.Errorf("decode PolyUS settlement: %w", err)
	}
	if strings.TrimSpace(payload.Slug) != slug {
		return 0, raw, code, fmt.Errorf("PolyUS settlement identity mismatch: requested=%q returned=%q", slug, payload.Slug)
	}
	value, ok := rawFloatOK(payload.Settlement)
	if !ok || (value != 0 && value != 1) {
		return 0, raw, code, fmt.Errorf("PolyUS settlement is not final binary truth: %v", value)
	}
	return value, raw, code, nil
}

// BookFull fetches + parses the full order book for a market (depth + stats). A terminal book that
// lacks the institutional finality metadata falls back to the official dedicated settlement
// endpoint, so the strict book guard cannot starve genuine final resolutions.
func (p *PublicClient) BookFull(ctx context.Context, slug string) (*BookData, []byte, int, error) {
	raw, code, err := p.Book(ctx, slug)
	if err != nil || code != 200 {
		return nil, raw, code, err
	}
	book := ParseBook(raw, slug)
	if _, final := book.SettledYes(); !final && polyUSSettlementLifecycleTerminal(book.State) {
		if value, _, _, settleErr := p.Settlement(ctx, slug); settleErr == nil {
			book.SettlementPx = value
			book.HasSettlement = true
			book.SettlementAuthority = FinalSettlementEndpointAuthority
		}
	}
	return book, raw, code, nil
}

// Slugs is a best-effort extraction of market slugs from a /v1/markets list response. The list
// wrapper shape isn't fully documented, so it tries the common shapes ({"markets":[...]},
// {"data":[...]}, or a bare array) and returns whatever it finds.
func Slugs(raw []byte) []string {
	var out []string
	var w1 struct {
		Markets []struct {
			Slug string `json:"slug"`
		} `json:"markets"`
	}
	if json.Unmarshal(raw, &w1) == nil {
		for _, m := range w1.Markets {
			if m.Slug != "" {
				out = append(out, m.Slug)
			}
		}
	}
	if len(out) > 0 {
		return out
	}
	var w2 struct {
		Data []struct {
			Slug string `json:"slug"`
		} `json:"data"`
	}
	if json.Unmarshal(raw, &w2) == nil {
		for _, m := range w2.Data {
			if m.Slug != "" {
				out = append(out, m.Slug)
			}
		}
	}
	if len(out) > 0 {
		return out
	}
	var w3 []struct {
		Slug string `json:"slug"`
	}
	if json.Unmarshal(raw, &w3) == nil {
		for _, m := range w3 {
			if m.Slug != "" {
				out = append(out, m.Slug)
			}
		}
	}
	return out
}
