// Package kalshi is a thin client for the Kalshi Trade API: RSA-PSS request
// signing, a token-bucket rate limiter, and 429 backoff. Phase 0 implements only
// the endpoints needed to prove connectivity and validate credentials.
package kalshi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/ratelimit"
)

const (
	// BaseProd is Kalshi's dedicated external Trade API host (May 2026). The old
	// api.elections host remains compatible, but this is the documented low-latency surface.
	BaseProd = "https://external-api.kalshi.com/trade-api/v2"
	// BaseDemo is the demo/sandbox base URL. Verified against current Kalshi docs
	// (getting_started/api_keys sample uses external-api.demo.kalshi.co).
	BaseDemo                       = "https://external-api.demo.kalshi.co/trade-api/v2"
	defaultPriorityReserveFraction = 0.30
	livePriorityReserveFraction    = 0.50 // WaitBackground's safe maximum
)

// BaseURLFor returns the base URL for an environment ("prod" or anything else => demo).
func BaseURLFor(env string) string {
	if strings.EqualFold(env, "prod") {
		return BaseProd
	}
	return BaseDemo
}

type Client struct {
	baseURL        string
	basePath       string // URL path prefix, e.g. "/trade-api/v2" (used when signing)
	http           *http.Client
	signer         *Signer           // nil => only public (unauthenticated) endpoints work
	limiter        *ratelimit.Bucket // official READ token bucket (GET)
	writeLimiter   *ratelimit.Bucket // official WRITE token bucket (orders/amends/cancels/RFQ mutations)
	reserve        float64           // tokens held back from BACKGROUND reads so the order path always has headroom (audit #12)
	reserveFrac    float64           // normal 30%; raised to 50% while LIVE AUTO owns the money lane
	configuredRate float64           // guarded with reserve fields; keeps concurrent AUTO/settings updates coherent

	writesArmed bool // prod order writes permitted ONLY when explicitly armed (real money); demo always allowed

	mktMu         sync.Mutex
	mktCache      []Market
	mktCacheAt    time.Time
	mktRefreshing bool            // a refresh is in flight; concurrent callers serve stale cache
	mktMaxPull    int             // R72-B/R73: windowed-pull row budget (markets_max_pull; 0 = default 20000 = the full universe)
	mktRefreshS   float64         // R73: board refresh cadence seconds (markets_refresh_s; 0 = default 4)
	mktSeeded     bool            // R73: curated liquidSeries warm-boot seed already merged (it no longer re-pulls per refresh)
	mktBgCtx      context.Context // R75: lifetime ctx for BACKGROUND pulls (set by StartBoardRefresher) — request ctxs must never own a pull
	mktPullDur    time.Duration   // R75: duration of the last completed full-universe pull (observability: the abort root cause was pulls > client timeouts)
	mktPullRows   int             // R75: rows the last pull kept (catalog-count verification)
	mktPullAt     time.Time       // R75: when the last pull finished
	// Price-structure lifecycle frames hot-patch mktCache immediately and retain a bounded receipt
	// here. This keeps a newly changed tick grid from waiting for the next full-board REST crawl.
	mktStructureUpdates map[string]PriceStructureUpdate
	// Market lifecycle receipts protect a deactivated/determined/settled/close-time update that
	// arrives while the multi-page REST board crawl is still in flight. The completed crawl
	// reapplies only receipts newer than its start before publishing its immutable snapshot.
	mktLifecycleUpdates map[string]marketLifecycleUpdate

	txMu sync.Mutex // ticker WebSocket: live YES mid-prices keyed by market ticker
	txPx map[string]tickerPx
	// Source-clock receipts stay separate from execution arrival freshness. REST ticker seeds never
	// advance them, and missing venue timestamps remain missing instead of becoming local time.
	lifecycleSourceAt   time.Time
	lifecycleFrames     int64
	lifecycleGeneration uint64
	lifecycleSID        int64
	lifecycleSequence   int64
	lifecyclePrior      int64
	lifecycleLastGap    int64
	lifecycleGapTotal   int64
	tickerFrames        int64
	tickerClockRejects  int64
	tickerClockRejectAt time.Time

	lifecycleFn         func(ticker string, yesVal float64) // market_lifecycle_v2 push-settlement handler (audit §8)
	rfqPulseFn          func(RFQPulseEvent)                 // communications-channel venue-wide RFQ broadcast handler (R18 probe)
	listingFn           func(ticker string, isNew bool)     // R27 freshlist: non-settlement lifecycle events (listing candidates)
	tickFn              func(ticker string, yes float64)    // R40: per-tick hook for event-driven 1¢ cancels (must be near-instant)
	oiFn                func(ticker string, oi float64)     // R70-B SCHEMA_AUDIT #7: per-tick OPEN-INTEREST hook (ticker WS open_interest_fp) → server OI-delta ring
	structFn            func(evType string, raw []byte)     // R124 D6: price-structure lifecycle frames, raw (sub-cent rollout watch)
	structureUpdateFn   func(PriceStructureUpdate)          // parsed structure update; keeps downstream warm caches on the same grid
	marketLifecycleFn   func(MarketLifecycleEvent)          // research-only activated/deactivated/close-date transition hook
	researchReplayFn    func(ResearchReplayEvent)           // selected normalized public frames; callback must only enqueue
	researchReplaySeq   map[string]int64                    // immediate source sequence by public channel
	wsSessionGeneration atomic.Uint64                       // unique local identity for each real WS source session
	publicTradeFn       func(Trade)                         // public trade push; handler must only enqueue
	userFillFn          func(Fill)                          // private fill push; handler must return quickly
	userEventFn         func()                              // private order/position/fill changed; invalidate REST snapshot

	tape   []Trade   // WS trade channel: rolling live tape (audit §4/#12 — replaces GetRecentTrades polling)
	tapeAt time.Time // last trade received (freshness gate for LiveTape)

	books bookWS // R74: orderbook_delta subscription manager + seq-checked live book store (bookws.go)

	healthMu  sync.Mutex // R85 REST-health watchdog: when did REST last TRY / last get an HTTP response?
	restTryAt time.Time  // last do/doWithBody attempt entering the limiter (any endpoint)
	restOKAt  time.Time  // last HTTP response RECEIVED (any status — a 4xx still proves limiter+transport alive)

	// R108 WS staleness watchdog: closure set by StartTickerStream that force-closes the CURRENT
	// primary ticker socket (onDead then promotes the warm standby + re-carries the book subs).
	kickMu sync.Mutex
	kickFn func() bool
}

// ForceReconnectWS (R108) kills the current primary WS session so a wedged-but-ponging socket is
// replaced through the standard onDead/promote path (books resubscribe via bookPrimaryChanged).
// Returns false when the stream isn't running.
func (c *Client) ForceReconnectWS() bool {
	c.kickMu.Lock()
	f := c.kickFn
	c.kickMu.Unlock()
	if f == nil {
		return false
	}
	return f()
}

func NewClient(baseURL string, signer *Signer, ratePerSec float64, timeout time.Duration) *Client {
	basePath := ""
	if u, err := url.Parse(baseURL); err == nil {
		basePath = strings.TrimRight(u.Path, "/")
	}
	// LATENCY (R17 research, measured wins): Go's default Transport keeps only 2 idle conns/host —
	// our parallel snapshot fans out 4-8 concurrent calls, so bursts paid fresh TCP+TLS handshakes
	// (~2×RTT, 30-80ms from home). A warm pool of 8 + an explicit TLS session cache (Go does NOT set
	// one by default; resumed TLS1.3 handshakes skip cert-chain verification) removes that tail.
	// IdleConnTimeout stays under Cloudflare's ~100s idle cutoff so WE close cleanly, not CF.
	tr := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     80 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		ForceAttemptHTTP2:   true,
		TLSClientConfig:     &tls.Config{ClientSessionCache: tls.NewLRUClientSessionCache(32)},
	}
	return &Client{
		baseURL:        strings.TrimRight(baseURL, "/"),
		basePath:       basePath,
		http:           &http.Client{Timeout: timeout, Transport: tr},
		signer:         signer,
		limiter:        ratelimit.New(ratePerSec),
		writeLimiter:   ratelimit.New(ratePerSec),
		reserve:        ratePerSec * defaultPriorityReserveFraction,
		reserveFrac:    defaultPriorityReserveFraction,
		configuredRate: ratePerSec,
	}
}

// HasCredentials reports whether authenticated calls are possible.
func (c *Client) HasCredentials() bool { return c.signer != nil }

// markRESTTry stamps a REST attempt (called at the top of every do/doWithBody attempt, BEFORE the
// limiter wait — so a fleet of waiters parked in a starved limiter still refreshes it).
func (c *Client) markRESTTry() {
	c.healthMu.Lock()
	c.restTryAt = time.Now()
	c.healthMu.Unlock()
}

// markRESTOk stamps a completed HTTP round-trip (any status code: a venue 4xx/5xx still proves the
// limiter granted a token and the transport works — the wedge class this watches for never reaches
// http.Do at all).
func (c *Client) markRESTOk() {
	c.healthMu.Lock()
	c.restOKAt = time.Now()
	c.healthMu.Unlock()
}

// RESTHealth returns (lastAttempt, lastResponse) for the R85 REST-health watchdog: attempts recent
// + responses ancient + WS alive ⇔ the REST path is wedged in the limiter, not the network.
func (c *Client) RESTHealth() (try, ok time.Time) {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	return c.restTryAt, c.restOKAt
}

// ResetLimiter safely rebuilds the token bucket (full burst, configured rate, penalties cleared).
// R85 SELF-HEAL: called by the server's REST-health watchdog when REST is wedged >120s while the
// WS is alive — the suite must never sit starved for hours again.
func (c *Client) ResetLimiter() {
	c.limiter.Reset()
	c.writeLimiter.Reset()
}

// LimiterRate reports the limiter's current effective refill rate (observability for the watchdog
// audit line — a value pinned far below rate_limit_per_sec means sustained venue 429 pushback).
func (c *Client) LimiterRate() float64 { return c.limiter.EffectiveRate() }

// WriteLimiterRate exposes the independent official write-bucket rate for health receipts.
func (c *Client) WriteLimiterRate() float64 { return c.writeLimiter.EffectiveRate() }

// SetRateLimit — R106 (auditor bug 272): hot-apply a new kalshi_rate_limit_per_sec to the RUNNING
// client (the knob was settings-accepted + persisted + echoed but the limiter was built once at
// boot — a silent no-op until restart). Also recomputes the current normal/LIVE priority reserve
// so the order/cancel lane keeps its share of the new budget.
func (c *Client) SetRateLimit(ratePerSec float64) {
	if ratePerSec <= 0 {
		return
	}
	c.limiter.SetRate(ratePerSec)
	c.writeLimiter.SetRate(ratePerSec)
	c.healthMu.Lock() // reserve is read on the request hot path — keep the update race-free
	c.configuredRate = ratePerSec
	frac := c.reserveFrac
	if frac <= 0 || frac > livePriorityReserveFraction {
		frac = defaultPriorityReserveFraction
		c.reserveFrac = frac
	}
	c.reserve = ratePerSec * frac
	c.healthMu.Unlock()
}

// SetLivePriority raises the background hold-back to the limiter's safe 50% maximum while AUTO is
// on. Priority market/account reads and writes still use the full token bucket; discovery consumes
// refill surplus, but broad/background work cannot drain the burst that fresh money needs. Turning
// AUTO off restores the normal 30% reserve. This changes scheduling only, never venue authority.
func (c *Client) SetLivePriority(active bool) {
	frac := defaultPriorityReserveFraction
	if active {
		frac = livePriorityReserveFraction
	}
	c.healthMu.Lock()
	c.reserveFrac = frac
	c.reserve = c.configuredRate * frac
	c.healthMu.Unlock()
}

// PriorityReserveTokens exposes the current background hold-back for health receipts and tests.
func (c *Client) PriorityReserveTokens() float64 { return c.reserveTokens() }

// reserveTokens reads the background hold-back race-free (written only by SetRateLimit).
func (c *Client) reserveTokens() float64 {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	return c.reserve
}

// RESTObs — R107 latency monitor: optional per-request duration observer (server wires it at
// boot; nil = no-op). Measures the FULL do() incl. limiter wait and retries — the latency our
// traffic actually experiences, vs the monitor's clean-room public probes.
var RESTObs func(time.Duration)

// do performs a request with rate limiting, optional signing, and 429 backoff.
// PRIORITY TIER (audit #12: background polling starved the live order path): /portfolio reads and
// every write take tokens at FULL priority; research/marking GETs may only spend the surplus
// (WaitBackground holds back ~30% of the budget), so a burst of board/tape polling can never queue
// an order, cancel, or position read behind it.
func (c *Client) do(ctx context.Context, method, path string, out any, authed bool) error {
	const maxAttempts = 5
	backoff := time.Second
	if RESTObs != nil { // R107 latency monitor: passive per-request RTT observer (incl. limiter wait + retries — what our traffic EXPERIENCES)
		t0 := time.Now()
		defer func() { RESTObs(time.Since(t0)) }()
	}
	readRequest := method == http.MethodGet
	background := readRequest && !strings.HasPrefix(path, "/portfolio") && !hasPriority(ctx)
	limiter := c.writeLimiter
	if readRequest {
		limiter = c.limiter
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c.markRESTTry() // R85 watchdog: stamped BEFORE the limiter so parked waiters keep the attempt clock fresh
		var lerr error
		if background {
			lerr = limiter.WaitBackground(ctx, c.reserveTokens())
		} else {
			lerr = limiter.Wait(ctx)
		}
		if lerr != nil {
			return lerr
		}
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		if authed {
			if c.signer == nil {
				return fmt.Errorf("authenticated request to %s but no credentials are loaded", path)
			}
			headers, err := c.signer.Headers(method, c.basePath+path)
			if err != nil {
				return err
			}
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}

		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		c.markRESTOk() // R85 watchdog: an HTTP response landed — REST path (limiter+transport) is alive
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			// SPLIT BACKOFF (R18 research): a Kalshi 429 is its own token bucket — docs say "no
			// enforced cooldown; your next request is allowed as soon as the bucket refills"
			// (~hundreds of ms), so exponential seconds just wasted time. A Cloudflare 1015/edge
			// block is the opposite: repeated hits EXTEND the block — freeze for 30s and don't
			// hammer. Tell them apart by the response body/server.
			// R85: EVERY 429 also halves the shared limiter's effective refill (AIMD, Penalize429)
			// — a configured rate above the account's real cap self-tunes down instead of feeding
			// the retry→429→retry amplification that starved the background tier for hours.
			limiter.Penalize429()
			wait := 300 * time.Millisecond
			if isCloudflareBlock(resp, body) {
				wait = 30 * time.Second
			} else {
				wait += backoff / 4 // gentle growth if Kalshi keeps saying no
				backoff *= 2
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		case resp.StatusCode >= 400:
			return fmt.Errorf("kalshi %s %s: status %d: %s", method, path, resp.StatusCode, string(body))
		}

		if out != nil && len(bytes.TrimSpace(body)) > 0 { // some 2xx endpoints return an EMPTY body (e.g. the tier upgrade) — that's success, not a decode error
			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("decode response from %s: %w", path, err)
			}
		}
		return nil
	}
	return fmt.Errorf("kalshi %s %s: retries exhausted (persistently rate limited)", method, path)
}

// prioCtxKey marks a context as PRIORITY-lane (R85 limiter split): market-data reads that guard
// REAL positions (held-ticker settle detection) ride the reserved orders/cancels/portfolio lane
// instead of queueing in the starvable background tier. Use sparingly — the lane's whole value is
// that discovery-class traffic can never enter it.
type prioCtxKey struct{}

// WithPriority returns a ctx whose Kalshi client calls take tokens at FULL priority (the
// orders/cancels/portfolio lane) even for market-data GETs.
func WithPriority(ctx context.Context) context.Context {
	return context.WithValue(ctx, prioCtxKey{}, true)
}

func hasPriority(ctx context.Context) bool {
	v, _ := ctx.Value(prioCtxKey{}).(bool)
	return v
}

// isCloudflareBlock distinguishes an edge (Cloudflare) rate-limit page from Kalshi's own JSON 429.
func isCloudflareBlock(resp *http.Response, body []byte) bool {
	if strings.Contains(strings.ToLower(resp.Header.Get("Server")), "cloudflare") && !bytes.HasPrefix(bytes.TrimSpace(body), []byte("{")) {
		return true
	}
	lb := bytes.ToLower(body)
	return bytes.Contains(lb, []byte("cloudflare")) || bytes.Contains(lb, []byte("error 1015"))
}

// doWithBody is do() for requests that carry a JSON body (e.g. POST /portfolio/orders).
// Kalshi signs only timestamp+method+path — the body is NOT part of the signature — so the
// signing path is identical to do(). Retries are safe for order POSTs because Kalshi dedups
// by client_order_id (the same id won't place a second order).
func (c *Client) doWithBody(ctx context.Context, method, path string, body any, out any, authed bool) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	const maxAttempts = 5
	backoff := time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		c.markRESTTry() // R85 watchdog: attempt clock (see do)
		if err := c.writeLimiter.Wait(ctx); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Content-Type", "application/json")
		if authed {
			if c.signer == nil {
				return fmt.Errorf("authenticated request to %s but no credentials are loaded", path)
			}
			headers, err := c.signer.Headers(method, c.basePath+path)
			if err != nil {
				return err
			}
			for k, v := range headers {
				req.Header.Set(k, v)
			}
		}

		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		c.markRESTOk() // R85 watchdog: HTTP response landed
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			// split backoff (R18): Kalshi token bucket → short retry; Cloudflare block → 30s freeze
			c.writeLimiter.Penalize429() // official write-bucket pushback is independent from reads
			wait := 300 * time.Millisecond
			if isCloudflareBlock(resp, respBody) {
				wait = 30 * time.Second
			} else {
				wait += backoff / 4
				backoff *= 2
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		case resp.StatusCode >= 400:
			return fmt.Errorf("kalshi %s %s: status %d: %s", method, path, resp.StatusCode, string(respBody))
		}

		if out != nil && len(bytes.TrimSpace(respBody)) > 0 { // empty 2xx body = success with nothing to decode (tier upgrade does this)
			if err := json.Unmarshal(respBody, out); err != nil {
				return fmt.Errorf("decode response from %s: %w", path, err)
			}
		}
		return nil
	}
	return fmt.Errorf("kalshi %s %s: retries exhausted (persistently rate limited)", method, path)
}

// ExchangeStatus is a public, unauthenticated health endpoint.
type ExchangeStatus struct {
	ExchangeActive bool `json:"exchange_active"`
	TradingActive  bool `json:"trading_active"`
}

func (c *Client) GetExchangeStatus(ctx context.Context) (*ExchangeStatus, error) {
	var s ExchangeStatus
	if err := c.do(ctx, http.MethodGet, "/exchange/status", &s, false); err != nil {
		return nil, err
	}
	return &s, nil
}

// Balance is an authenticated endpoint, used to validate credentials. Values are
// reported by Kalshi in cents. PortfolioValue is the current value of positions,
// not total account value. It is presence-aware because money controls must
// distinguish a legitimate zero-dollar position portfolio from an incomplete
// response that omitted a required account-NAV component.
type Balance struct {
	Balance        int64  `json:"balance"`
	PortfolioValue *int64 `json:"portfolio_value"`
	UpdatedTS      int64  `json:"updated_ts,omitempty"`
}

// PortfolioValueUSD returns the authenticated current value of positions.
func (b Balance) PortfolioValueUSD() (float64, bool) {
	if b.PortfolioValue == nil || *b.PortfolioValue < 0 {
		return 0, false
	}
	return float64(*b.PortfolioValue) / 100, true
}

// AccountNAVUSD returns Kalshi account NAV: available cash balance plus the
// authenticated current value of positions. Missing portfolio_value fails closed.
func (b Balance) AccountNAVUSD() (float64, bool) {
	positions, ok := b.PortfolioValueUSD()
	if !ok || b.Balance < 0 {
		return 0, false
	}
	return float64(b.Balance)/100 + positions, true
}

func (c *Client) GetBalance(ctx context.Context) (*Balance, error) {
	var b Balance
	if err := c.do(ctx, http.MethodGet, "/portfolio/balance", &b, true); err != nil {
		return nil, err
	}
	return &b, nil
}
