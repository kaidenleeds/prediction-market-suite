// Package polymarket reads Polymarket's PUBLIC data (no key): top markets by
// volume from the gamma API, and per-market top holders from the data API.
// Because Polymarket settles on-chain, holder positions are public and
// attributable — which is what makes smart-money "consensus" detectable.
package polymarket

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kalshi-suite/kalshi-suite/internal/polyid"
	"github.com/kalshi-suite/kalshi-suite/internal/ratelimit"
)

// ErrNotFound — R125: a definitive venue-side 404 (the resource does not exist), distinguishable
// from transient transport/5xx errors. The ptt backfill worker needs this to mark deleted/voided
// markets unresolvable (resolved=-2) instead of retrying them forever.
var (
	ErrNotFound           = errors.New("polymarket: not found (404)")
	ErrInvalidConditionID = errors.New("polymarket: invalid condition id")
)

const (
	gammaBase = "https://gamma-api.polymarket.com"
	dataBase  = "https://data-api.polymarket.com"
	clobBase  = "https://clob.polymarket.com"
)

type Client struct {
	http *http.Client
	// posLimiter throttles data-api /positions calls. The consensus panel fans out one
	// /positions per leaderboard wallet; unthrottled that burst blows past Polymarket's
	// 15/s /positions cap, trips Cloudflare's general data-api throttle, and that's what
	// makes /trades (the whale feed) return STALE cached data. Capped under 15/s, the burst
	// spreads out, never trips the throttle, and /trades stays live.
	posLimiter *ratelimit.Bucket
	posMu      sync.Mutex
	posCache   map[string]posEntry

	// R112 adaptive slow mode: rolling RTT ring over this client's own requests.
	rttMu   sync.Mutex
	rttRing [rttWindow]float64
	rttI    int
	rttN    int

	lbMu     sync.Mutex
	lbInfo   map[string]*LBInfo
	lbRoster []Trader
	lbAt     time.Time

	tpwMu sync.Mutex // dedicated all-time-profit roster for the smart-money DB (own fetch, no signal drift)
	tpw   []ProfitWallet
	tpwAt time.Time

	profMu    sync.Mutex
	profCache map[string]profEntry

	pnMu    sync.Mutex // wallet → public display name (gamma /public-profile), bounded + 24h TTL
	pnCache map[string]nameEntry

	// R132: data-api activity occasionally emits a truncated, token-derived value in
	// conditionId. Cache immutable token-to-condition recovery and throttle cache misses.
	ctMu      sync.Mutex
	ctCache   map[string]conditionTokenEntry
	ctLimiter *ratelimit.Bucket

	hdMu    sync.Mutex // condID → top-holders (identity + lean), fed by the consensus render's own fetches (R70-B #4)
	hdCache map[string]holdersEntry

	cuMu    sync.Mutex
	cuCache map[string]cryptoUp // coin -> current 15-min "Up" probability (cross-confirm crypto)

	mktMu         sync.Mutex
	mktCache      []Market
	mktAt         time.Time
	mktRefreshing bool // R73: a (now 20-page) refresh is in flight — concurrent stale callers serve the last cache instead of stampeding gamma

	// R75 SUB-MARKETS: flat /markets pagination by volume drops the low-volume sub-outcomes of
	// big EVENTS (a World Cup match's lines/props sit under one event but each trades under the
	// ~$2k/24h flat-pull floor). A SLOW (~2 min) background events-with-nested-markets crawl
	// fills them in; GetTopMarkets merges this cache into every refresh.
	evtMu         sync.Mutex
	evtCache      []Market
	evtAt         time.Time
	evtRefreshing bool
	// A slow/failed keyset page never restarts the multi-minute crawl from cursor zero. The
	// in-progress rows/cursor checkpoint across background attempts; the last completed catalog
	// may additionally persist across process restarts (catalogsnapshot.go).
	evtPartial   []Market
	evtCursor    string
	evtPages     int
	evtPageLimit int
	evtLastErr   string
	evtCachePath string

	// Live trade feed from the RTDS WebSocket (wss://ws-live-data.polymarket.com) — the only
	// truly real-time trade source. The REST /trades endpoint lags minutes by design.
	ltMu   sync.Mutex
	liveTr []PolyTrade
	liveAt time.Time           // last time a live trade message arrived
	lastPx map[string]pxSample // conditionId -> last YES (outcome-0) price from the live stream
	// OnTrades fires (off-lock) the instant new live trades arrive, so the server can fold
	// them into the whale log immediately instead of waiting for a ticker. Optional.
	OnTrades func()
	// R108 WS staleness watchdog: handle on the CURRENT live-trades socket so a wedged-but-
	// ponging connection (R107: tape age grew second-for-second while pongs kept the read
	// deadline alive) can be force-closed; StartLiveTrades' loop then redials with backoff.
	wsMu   sync.Mutex
	wsConn *websocket.Conn

	// CLOB market-channel level-2 cache. Unlike RTDS lastPx (last trade), this is executable
	// outcome-token bid/ask state from book/price_change/best_bid_ask pushes. Token order comes
	// from gamma clobTokenIds, so YES and NO can each use their own real ask without complement
	// guesses. clobws.go owns all access under clobMu.
	clobMu         sync.RWMutex
	clobBooks      map[string]*clobBookState // asset token id -> live book/BBO
	clobTokenRef   map[string]clobTokenRef   // asset token id -> condition + outcome index
	clobAssets     map[string][]string       // condition id -> ordered outcome token ids
	clobConnected  bool
	clobFrameAt    time.Time
	clobConn       *websocket.Conn // current market-channel socket; watchdog may force a redial
	clobLastErr    string          // last transport/subscription failure; operator-readable, no secrets
	clobLastErrAt  time.Time
	clobReconnects uint64
	// Each successful dial advances the generation. Quotes from a prior socket are never
	// executable merely because the replacement socket connected and started ponging.
	clobGeneration uint64
}

// pxSample is the most recent live trade price for a market's outcome-0 (YES) side.
type pxSample struct {
	px float64
	at time.Time
}

type posEntry struct {
	ps []Position
	at time.Time
}

func NewClient(timeout time.Duration) *Client {
	return &Client{
		http:         &http.Client{Timeout: timeout},
		posLimiter:   ratelimit.New(14), // just under Polymarket's 15/s /positions cap (max before 429s)
		posCache:     map[string]posEntry{},
		profCache:    map[string]profEntry{},
		pnCache:      map[string]nameEntry{},
		ctCache:      map[string]conditionTokenEntry{},
		ctLimiter:    ratelimit.New(2),
		lastPx:       map[string]pxSample{},
		clobBooks:    map[string]*clobBookState{},
		clobTokenRef: map[string]clobTokenRef{},
		clobAssets:   map[string][]string{},
	}
}

// Trader is a known wallet from Polymarket's public profit leaderboard.
type Trader struct {
	Wallet string
	Name   string
}

// LeaderboardTraders is the top of Polymarket's all-time profit leaderboard
// (public, on-chain). Refresh from https://polymarket.com/leaderboard as needed.
var LeaderboardTraders = []Trader{
	{"0x56687bf447db6ffa42ffe2204a05edaa20f55839", "Theo4"},
	{"0x1f2dd6d473f3e824cd2f8a89d9c69fb96f6ad0cf", "Fredi9999"},
	{"0x6a72f61820b26b1fe4d956e17b6dc2a1ea3033ee", "kch123"},
	{"0x2005d16a84ceefa912d4e380cd32e7ff827875ea", "RN1"},
	{"0x204f72f35326db932158cba6adff0b9a1da95e14", "swisstony"},
	{"0x96cfcb0c30942cfcd1cdf76c7d408794d66b1acb", "mintblade"},
	{"0xed64a7bf029040aa331abc87902434d815ef217d", "fishalive"},
	{"0xbc11a64ab34a03a043fbe80598fa065ee87eeec6", "frostrizz"},
	{"0x78b9ac44a6d7d7a076c14e0ad518b301b63c6b76", "Len9311238"},
	{"0xd235973291b2b75ff4070e9c0b01728c520b0f29", "zxgngl"},
	{"0x3f87d51f27ba6e19ec52aaeebb68559a839c742c", "GRIMDRIP"},
	{"0x863134d00841b2e200492805a01e1e2f5defaa53", "RepTrump"},
	{"0x5e4c3b5b81171e2ca4ab776ac0d6bba787f9dba2", "endlessFate"},
	{"0x8119010a6e589062aa03583bb3f39ca632d9f887", "PrincessCaro"},
	{"0xe9ad918c7678cd38b12603a762e638a5d1ee7091", "walletmobile"},
	{"0x94f199fb7789f1aef7fff6b758d6b375100f4c7a", "KeyTransporter"},
	{"0x885783760858e1bd5dd09a3c3f916cfa251ac270", "BetTom42"},
	{"0x23786fdad0073692157c6d7dc81f281843a35fcb", "mikatrade77"},
	{"0xe90bec87d9ef430f27f9dcfe72c34b76967d5da2", "gmanas"},
	{"0xd0c042c08f755ff940249f62745e82d356345565", "alexmulti"},
	{"0xf8831548531d56ad6a4331493243c447a827cd1f", "Inaccuratestake"},
	{"0x9d36d18057ac9a61308b22128839e903bea30e68", "supersob"},
	{"0x26437896ed9dfeb2f69765edcafe8fdceaab39ae", "Latina"},
	{"0xcfb69f94b3764e75228515ad9d169570d5f906da", "athelstan"},
	{"0xf0318c32136c2db7fec88b84869aee6a1106c80c", "BreakTheBank"},
	{"0x97cb27132b9dd66a2ef49390893cbeb26c3fe4d0", "MrYuYa777"},
	{"0xde7be6d489bce070a959e0cb813128ae659b5f4b", "wan123"},
	{"0x4761ecf3578e388a9b16c43f874efe32ee855ae8", "Grenderen"},
	{"0xbaa2bcb5439e985ce4ccf815b4700027d1b92c73", "denizz"},
	{"0x0b89e6c79decff0365855c828c73caa1ccd0d710", "Slickvenom"},
	{"0x53bed12209c83a8fe4d4d247aa91caba7277456a", "nutmegger"},
}

// leaderboardEntry is one row of Polymarket's public profit leaderboard
// (lb-api.polymarket.com/profit). amount is the trader's profit in USD.
type leaderboardEntry struct {
	ProxyWallet string  `json:"proxyWallet"`
	Name        string  `json:"name"`
	Pseudonym   string  `json:"pseudonym"`
	Amount      float64 `json:"amount"`
}

// LBInfo is what we know about a leaderboard trader: display name, the windows they
// rank in (e.g. "wk #2 · all #14"), and their profit (all-time when available).
type LBInfo struct {
	Name   string  `json:"name"`
	Ranks  string  `json:"ranks"`
	Best   int     `json:"best"` // best (lowest) rank across all windows
	Profit float64 `json:"profit"`
}

// lbWindows are the leaderboard time windows we union, their short display label,
// and how many of each window's top to include in the live consensus roster.
var lbWindows = []struct {
	window string
	label  string
	top    int
}{
	// Pool: top-100 on daily (noisier window → stricter), top-500 on weekly/monthly/all-time.
	// R82 PROBE (2026-07-05): lb-api caps every window at its top 50 (limit>50 → 50 rows, offset
	// ignored), so these tops are UPPER BOUNDS — each window actually contributes ≤50 wallets and
	// ranks only ever reach #50. Kept as-is so a future API un-capping restores depth for free.
	{"1d", "day", 100}, {"7d", "wk", 500}, {"30d", "mo", 500}, {"all", "all", 500},
}

// ensureLeaderboard fetches Polymarket's profit leaderboard across all windows and
// caches (~30min): wallet -> LBInfo (name, ranks, profit) for the top ~500 per window
// (used to flag whale trades), plus a deduped roster of the very top wallets (scanned
// for the live-position consensus). Falls back to the built-in roster on failure.
func (c *Client) ensureLeaderboard(ctx context.Context) (map[string]*LBInfo, []Trader) {
	c.lbMu.Lock()
	defer c.lbMu.Unlock()
	if c.lbInfo != nil && time.Since(c.lbAt) < c.ttl(30*time.Minute) {
		return c.lbInfo, c.lbRoster
	}

	info := map[string]*LBInfo{}
	var roster []Trader
	inRoster := map[string]bool{}
	for _, win := range lbWindows {
		var entries []leaderboardEntry
		if err := c.get(ctx, "https://lb-api.polymarket.com/profit?window="+win.window+"&limit=500", &entries); err != nil {
			continue
		}
		for i, e := range entries {
			w := strings.ToLower(strings.TrimSpace(e.ProxyWallet))
			if w == "" {
				continue
			}
			name := cleanLBName(e.Name, e.Pseudonym, e.ProxyWallet)
			it := info[w]
			if it == nil {
				it = &LBInfo{Name: name}
				info[w] = it
			}
			rank := win.label + " #" + strconv.Itoa(i+1)
			if it.Ranks == "" {
				it.Ranks = rank
			} else {
				it.Ranks += " · " + rank
			}
			if it.Best == 0 || i+1 < it.Best {
				it.Best = i + 1
			}
			if win.window == "all" || it.Profit == 0 {
				it.Profit = e.Amount
			}
			if i < win.top && !inRoster[w] {
				inRoster[w] = true
				roster = append(roster, Trader{Wallet: e.ProxyWallet, Name: name})
			}
		}
	}

	if len(info) > 0 {
		c.lbInfo, c.lbRoster, c.lbAt = info, roster, time.Now()
		return c.lbInfo, c.lbRoster
	}
	// Fetch failed — fall back to the built-in roster so the panels still work.
	if c.lbInfo == nil {
		c.lbInfo = map[string]*LBInfo{}
		for _, t := range LeaderboardTraders {
			c.lbInfo[strings.ToLower(t.Wallet)] = &LBInfo{Name: t.Name}
		}
		c.lbRoster = append([]Trader(nil), LeaderboardTraders...)
		c.lbAt = time.Now()
	}
	return c.lbInfo, c.lbRoster
}

// LeaderboardInfo returns wallet(lowercased) -> LBInfo for top-profit traders.
func (c *Client) LeaderboardInfo(ctx context.Context) map[string]*LBInfo {
	m, _ := c.ensureLeaderboard(ctx)
	return m
}

// ConsensusRoster returns the leaderboard wallets to scan for live-position consensus:
// window-qualified (top-500 all/wk/mo, top-100 day) AND each with all-time profit ≥ minPnL
// (a proven track record, not a one-week fluke). Sorted by profit and capped at 120 so the
// per-wallet /positions fanout stays under Polymarket's data-api limit (12/s).
func (c *Client) ConsensusRoster(ctx context.Context, minPnL float64) []Trader {
	info, r := c.ensureLeaderboard(ctx)
	out := make([]Trader, 0, len(r))
	for _, t := range r {
		if i := info[strings.ToLower(t.Wallet)]; i != nil && i.Profit >= minPnL {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(a, b int) bool {
		return info[strings.ToLower(out[a].Wallet)].Profit > info[strings.ToLower(out[b].Wallet)].Profit
	})
	if len(out) > 120 {
		out = out[:120]
	}
	return out
}

type profEntry struct {
	v  float64
	at time.Time
}

// WalletProfit returns a wallet's all-time profit/loss in USD (lb-api profit scoped by
// address), cached ~10min. Works for ANY wallet, not just leaderboard members, so we
// can show whether a big bettor is actually up or down. Negative = net loser.
func (c *Client) WalletProfit(ctx context.Context, wallet string) float64 {
	key := strings.ToLower(wallet)
	c.profMu.Lock()
	if e, ok := c.profCache[key]; ok && time.Since(e.at) < c.ttl(10*time.Minute) {
		c.profMu.Unlock()
		return e.v
	}
	c.profMu.Unlock()

	var entries []leaderboardEntry
	_ = c.get(ctx, "https://lb-api.polymarket.com/profit?window=all&address="+url.QueryEscape(wallet), &entries)
	var v float64
	if len(entries) > 0 {
		v = entries[0].Amount
	}
	c.profMu.Lock()
	c.profCache[key] = profEntry{v: v, at: time.Now()}
	c.profMu.Unlock()
	return v
}

type nameEntry struct {
	name string // "" = looked up, no public name (negative result cached too)
	at   time.Time
}

// ProfileName resolves a wallet's PUBLIC display name via the documented gamma profiles endpoint
// (R69 item 6; docs + live probe verified 2026-07-04):
//
//	GET https://gamma-api.polymarket.com/public-profile?address=0x…
//	→ 200 {"name":"Theo4","pseudonym":"Ironclad-Tenement","proxyWallet":"0x…",…} · 404 unknown wallet
//
// (The operator-suggested data-api.polymarket.com/profile pattern returns an empty body — the
// documented host is gamma.) Bounded cache: ~200 wallets, 24h TTL, negative results cached so
// unknown wallets don't re-fetch every render. Returns "" when the wallet has no usable handle.
func (c *Client) ProfileName(ctx context.Context, wallet string) string {
	key := strings.ToLower(strings.TrimSpace(wallet))
	if len(key) < 10 {
		return ""
	}
	c.pnMu.Lock()
	if e, ok := c.pnCache[key]; ok && time.Since(e.at) < 24*time.Hour {
		c.pnMu.Unlock()
		return e.name
	}
	c.pnMu.Unlock()

	var prof struct {
		Name      string `json:"name"`
		Pseudonym string `json:"pseudonym"`
	}
	_ = c.get(ctx, gammaBase+"/public-profile?address="+url.QueryEscape(key), &prof) // 404/error → both fields empty → negative-cached
	name := ""
	for _, n := range []string{strings.TrimSpace(prof.Name), strings.TrimSpace(prof.Pseudonym)} {
		if n != "" && !strings.HasPrefix(strings.ToLower(n), "0x") && len(n) <= 24 {
			name = n
			break
		}
	}
	c.pnMu.Lock()
	if len(c.pnCache) >= 200 { // hard bound (~200 tracked whale wallets); evict the oldest half
		type kv struct {
			k string
			t time.Time
		}
		all := make([]kv, 0, len(c.pnCache))
		for k, e := range c.pnCache {
			all = append(all, kv{k, e.at})
		}
		sort.Slice(all, func(i, j int) bool { return all[i].t.Before(all[j].t) })
		for _, e := range all[:len(all)/2] {
			delete(c.pnCache, e.k)
		}
	}
	c.pnCache[key] = nameEntry{name: name, at: time.Now()}
	c.pnMu.Unlock()
	return name
}

// ProfileNameCached is the no-network variant: the cached name only ("" = unknown/not cached).
// Render paths use this so a popup never blocks on a profile fetch.
func (c *Client) ProfileNameCached(wallet string) string {
	key := strings.ToLower(strings.TrimSpace(wallet))
	c.pnMu.Lock()
	defer c.pnMu.Unlock()
	if e, ok := c.pnCache[key]; ok && time.Since(e.at) < 24*time.Hour {
		return e.name
	}
	return ""
}

// cleanLBName prefers a real handle; Polymarket's default pseudonyms are raw wallet
// strings, so anything empty/address-like is shortened to a 10-char wallet stub.
func cleanLBName(name, pseudonym, wallet string) string {
	for _, n := range []string{strings.TrimSpace(name), strings.TrimSpace(pseudonym)} {
		if n != "" && !strings.HasPrefix(strings.ToLower(n), "0x") && len(n) <= 24 {
			return n
		}
	}
	if len(wallet) >= 10 {
		return wallet[:10]
	}
	return "top trader"
}

// RESTObs — R107 latency monitor: optional per-request duration observer (server wires it; nil = no-op).
var RESTObs func(time.Duration)

// ── R112 ADAPTIVE SLOW MODE ──────────────────────────────────────────────────────────────────
// R112 measured the polyint REST slowness as VENUE-SIDE (gamma-api RTT p50 ~3.3s / p95 timeout
// while kalshi+polyus sit ~100ms from the same host, and our own rate limiter idle). We can't
// fix their edge; we can stop hammering it: the client tracks its own recent request RTTs and,
// when the rolling median degrades past slowRTTMs, every cache TTL stretches ×slowTTLFactor —
// lean on cache/WS, poll gracefully slower, recover automatically when the venue heals.
const (
	slowRTTMs     = 1500.0
	slowTTLFactor = 3
	rttWindow     = 24
)

func (c *Client) noteRTT(d time.Duration) {
	c.rttMu.Lock()
	c.rttRing[c.rttI%rttWindow] = float64(d.Milliseconds())
	c.rttI++
	if c.rttN < rttWindow {
		c.rttN++
	}
	c.rttMu.Unlock()
}

// SlowMode reports whether the venue is currently degraded (rolling median RTT past threshold).
func (c *Client) SlowMode() bool {
	c.rttMu.Lock()
	defer c.rttMu.Unlock()
	if c.rttN < 8 {
		return false
	}
	v := make([]float64, c.rttN)
	copy(v, c.rttRing[:c.rttN])
	sort.Float64s(v)
	return v[len(v)/2] >= slowRTTMs
}

// ttl stretches a cache TTL when the venue is slow (information stays served, just staler).
func (c *Client) ttl(base time.Duration) time.Duration {
	if c.SlowMode() {
		return base * slowTTLFactor
	}
	return base
}

func (c *Client) get(ctx context.Context, urlStr string, out any) error {
	t0 := time.Now()
	defer func() {
		d := time.Since(t0)
		c.noteRTT(d) // R112 adaptive slow mode input
		if RESTObs != nil {
			RESTObs(d) // R107: passive RTT observer
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	// A real browser UA. The old "(compatible; KalshiSuite/1.0)" is a classic bot signature
	// that Polymarket's Cloudflare intermittently challenges (HTML, not JSON) under load —
	// which silently failed the trade fetch and froze the whale feed for minutes at a time.
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	// Intentionally NO Cache-Control/Pragma no-cache: we WANT Polymarket's CDN edge-cached
	// response (a few seconds fresh — exactly what the website reads). Forcing no-cache (and
	// the old &_= cache-buster) bypassed that fresh edge and hit a ~5-min-lagging origin,
	// which is what made the whale feed read minutes stale.
	// 429/5xx BACKOFF (audit §6): the Kalshi client retries rate limits; this one didn't — a
	// Cloudflare throttle read as a hard error and poisoned caches downstream. One retry after
	// Retry-After (or 1.5s), honoring ctx.
	var resp *http.Response
	for attempt := 0; ; attempt++ {
		resp, err = c.http.Do(req)
		if err != nil {
			return err
		}
		if (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500) && attempt == 0 {
			_ = resp.Body.Close()
			wait := 1500 * time.Millisecond
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, e := strconv.Atoi(strings.TrimSpace(ra)); e == nil && secs > 0 && secs <= 30 {
					wait = time.Duration(secs) * time.Second
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
			continue
		}
		break
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("polymarket GET %s: %w", urlStr, ErrNotFound) // R125: typed 404 (backfill's deleted-market signal)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("polymarket GET %s: status %d", urlStr, resp.StatusCode)
	}
	return json.Unmarshal(body, out)
}

// Market is a subset of Polymarket's gamma market object. outcomes/outcomePrices
// arrive as JSON-encoded strings, e.g. "[\"Yes\",\"No\"]" / "[\"0.54\",\"0.46\"]".
type Market struct {
	Question         string  `json:"question"`
	ConditionID      string  `json:"conditionId"`
	Slug             string  `json:"slug"`
	Description      string  `json:"description"`
	ResolutionSource string  `json:"resolutionSource"`
	Volume24hr       float64 `json:"volume24hr"`
	PricesRaw        string  `json:"outcomePrices"`
	OutcomesRaw      string  `json:"outcomes"`
	TokensRaw        string  `json:"clobTokenIds"` // JSON-encoded ["<upTokenId>","<downTokenId>"] — for prices-history
	EndDate          string  `json:"endDate"`
	GameStart        string  `json:"gameStartTime"` // sports markets only: when the game starts (live = now ≥ this)
	Closed           bool    `json:"closed"`
	OneDayChange     float64 `json:"oneDayPriceChange"` // YES-price move over 24h (probability units)
	OneHourChange    float64 `json:"oneHourPriceChange"`
	NegRisk          bool    `json:"negRisk"` // R69 SCHEMA_AUDIT #3: multi-outcome neg-risk event — NO-side economics are NOT independent-binary
	// R70-B SCHEMA_AUDIT #10 / F3 — true liveness. Gotcha (AR §3): resolved markets can still
	// show active:true, and a `closed` test alone lets halted/suspended-but-unclosed markets be
	// marked/arbed as live. Live = Active && !Closed && AcceptingOrders (see Halted below).
	Active          bool             `json:"active"`
	AcceptingOrders bool             `json:"acceptingOrders"`
	UmaResolution   string           `json:"umaResolutionStatus"` // ""/"proposed"/"resolved"/… — resolution pipeline state
	BestBid         float64          `json:"bestBid"`             // top of book for outcome-0 (YES) — for the TRUE tradeable arb
	BestAsk         float64          `json:"bestAsk"`
	Events          []MarketEventRef `json:"events"` // R75: named type so the events-crawl can inject the parent event slug
	Tags            []struct {
		Label string `json:"label"`
		Slug  string `json:"slug"`
	} `json:"tags"` // populated only when the fetch passes &include_tag=true
	// R127 poly-int sports metadata (live-probed 2026-07-09 on mlb-sea-mia / wnba-sea-atl /
	// fifwc-arg-che): the venue's OWN market-type string — and unlike the PUS V2 enum it is
	// SCOPE-AWARE ("moneyline"/"spreads"/"totals" = full game; period/prop variants carry their
	// own spellings like "baseball_team_first_five_spread", "first_half_totals", "points",
	// "nrfi") — plus the SIGNED line riding outcome-0 (a "spread-home-11pt5" market decodes
	// line=-11.5 with outcomes[0] = the home team). Line stays a RawMessage so a venue-side type
	// change can never poison the shared bulk /markets decode; read it via SportsLine().
	SportsMarketType string          `json:"sportsMarketType,omitempty"`
	SportsLineRaw    json.RawMessage `json:"line,omitempty"`
	// R132 fee/tick schema: Gamma is the authoritative discovery surface for whether a market
	// participates in the current fee program. Pointer fields preserve absent/null versus an
	// explicit fee-free false/zero; callers must not silently convert unknown into free.
	OrderPriceMinTickSize float64      `json:"orderPriceMinTickSize,omitempty"`
	FeesEnabled           *bool        `json:"feesEnabled"`
	FeeSchedule           *FeeSchedule `json:"feeSchedule"`
}

// FeeSchedule is Gamma's complete per-market taker-fee curve: fee = contracts * rate *
// [price*(1-price)]^exponent. TakerOnly is retained as schema-drift evidence; on the current V2
// engine makers are never charged a match-time fee. RebateRate is separate program metadata rather
// than a negative fee at match time.
type FeeSchedule struct {
	Exponent       float64 `json:"exponent"`
	Rate           float64 `json:"rate"`
	TakerOnly      bool    `json:"takerOnly"`
	RebateRate     float64 `json:"rebateRate"`
	curveKnown     bool
	takerModeKnown bool
}

// UnmarshalJSON keeps the public fields ergonomic while retaining child-field presence. A schema
// regression that drops exponent/rate/takerOnly therefore fails closed instead of turning into a
// zero/default fee curve.
func (s *FeeSchedule) UnmarshalJSON(data []byte) error {
	var wire struct {
		Exponent   *float64 `json:"exponent"`
		Rate       *float64 `json:"rate"`
		TakerOnly  *bool    `json:"takerOnly"`
		RebateRate *float64 `json:"rebateRate"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*s = FeeSchedule{}
	if wire.Exponent != nil && wire.Rate != nil {
		s.Exponent, s.Rate, s.curveKnown = *wire.Exponent, *wire.Rate, true
	}
	if wire.TakerOnly != nil {
		s.TakerOnly, s.takerModeKnown = *wire.TakerOnly, true
	}
	if wire.RebateRate != nil {
		s.RebateRate = *wire.RebateRate
	}
	return nil
}

func newFeeSchedule(rate, exponent float64, takerOnly bool) FeeSchedule {
	return FeeSchedule{Rate: rate, Exponent: exponent, TakerOnly: takerOnly, curveKnown: true, takerModeKnown: true}
}

func (s FeeSchedule) valid() bool {
	return s.curveKnown && s.takerModeKnown && s.Exponent > 0 && !math.IsNaN(s.Exponent) && !math.IsInf(s.Exponent, 0) &&
		s.Rate >= 0 && !math.IsNaN(s.Rate) && !math.IsInf(s.Rate, 0) &&
		s.RebateRate >= 0 && s.RebateRate <= 1 && !math.IsNaN(s.RebateRate) && !math.IsInf(s.RebateRate, 0)
}

// FeeUSD calculates the venue fee for one matched quantity using the complete current curve:
// C * r * [p(1-p)]^e, rounded to the venue's documented 5-decimal pUSD precision. Current V2 makers
// always pay zero at matching; a false legacy takerOnly field cannot reintroduce an obsolete maker
// charge. RebateRate is retained as program metadata and is never subtracted here: maker rebates
// are distributed separately, not an instant negative fill fee.
func (s FeeSchedule) FeeUSD(contracts, price float64, taker bool) (float64, bool) {
	if !s.valid() || contracts < 0 || math.IsNaN(contracts) || math.IsInf(contracts, 0) ||
		price < 0 || price > 1 || math.IsNaN(price) || math.IsInf(price, 0) {
		return 0, false
	}
	if !taker {
		return 0, true
	}
	fee := contracts * s.Rate * math.Pow(price*(1-price), s.Exponent)
	return math.Round(fee*1e5) / 1e5, true
}

// SportsLine decodes the venue's sports line (signed spread handicap riding outcome-0, or the
// O/U total). ok=false = absent/null/non-numeric — callers REFUSE rather than guess.
func (m Market) SportsLine() (float64, bool) {
	raw := strings.Trim(strings.TrimSpace(string(m.SportsLineRaw)), `"`)
	if raw == "" || raw == "null" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// MarketEventRef is the slim parent-event reference carried on a gamma market (events[].slug).
type MarketEventRef struct {
	Slug string `json:"slug"`
}

// SportsTeam — R127: one structured team object on a gamma sports EVENT (the venue hands over
// name + abbreviation + ordering "away"/"home" first-class; ordering is REQUIRED identity — the
// slug token order disagrees between leagues: fifwc lists home first, wnba away first).
type SportsTeam struct {
	Name         string `json:"name"`
	League       string `json:"league"`
	Abbreviation string `json:"abbreviation"`
	Ordering     string `json:"ordering"`
}

// SportsEvent — R127: one gamma EVENT with its structured sports metadata + nested markets
// (a single fetch serves every line/prop of a game — the budget-efficient anchor unit).
type SportsEvent struct {
	Slug      string       `json:"slug"`
	StartTime string       `json:"startTime"`
	Live      bool         `json:"live"`
	Teams     []SportsTeam `json:"teams"`
	Markets   []Market     `json:"markets"`
}

// EventBySlug fetches one gamma event by its EXACT slug (nested markets + teams + startTime).
// Two-pass on miss with &closed=true — the R125 gamma lesson (bare lookups exclude closed rows)
// applied preemptively so already-settled games still serve their metadata.
func (c *Client) EventBySlug(ctx context.Context, slug string) (SportsEvent, bool) {
	slug = strings.ToLower(strings.TrimSpace(slug))
	if slug == "" {
		return SportsEvent{}, false
	}
	base := gammaBase + "/events?slug=" + url.QueryEscape(slug)
	for _, u := range []string{base, base + "&closed=true"} {
		var evs []SportsEvent
		if err := c.get(ctx, u, &evs); err == nil && len(evs) > 0 && len(evs[0].Markets) > 0 {
			c.registerMarkets(evs[0].Markets)
			for i := range evs[0].Markets {
				evs[0].Markets[i] = c.overlayCLOBBBO(evs[0].Markets[i])
			}
			return evs[0], true
		}
	}
	return SportsEvent{}, false
}

// CachedMarketByCondition returns the cached gamma market for a conditionId — CACHE-ONLY, no
// network (R69: the insertSignal momentum-fallback stamping path must never add API calls).
// The cache is the TopMarkets list, so misses just mean "not a top-volume market right now".
func (c *Client) CachedMarketByCondition(condID string) (Market, bool) {
	if strings.TrimSpace(condID) == "" {
		return Market{}, false
	}
	c.mktMu.Lock()
	for i := range c.mktCache {
		if c.mktCache[i].ConditionID == condID {
			m := c.mktCache[i]
			c.mktMu.Unlock()
			return c.overlayCLOBBBO(m), true
		}
	}
	c.mktMu.Unlock()
	return Market{}, false
}

// CachedMarkets returns a snapshot COPY of the cached active universe, with fresh CLOB BBO
// overlays. After the first keyset crawl it includes every nested active market, not just the
// volume head. CACHE-ONLY: no network.
func (c *Client) CachedMarkets() []Market {
	c.mktMu.Lock()
	out := make([]Market, len(c.mktCache))
	copy(out, c.mktCache)
	c.mktMu.Unlock()
	for i := range out {
		out[i] = c.overlayCLOBBBO(out[i])
	}
	return out
}

func (c *Client) marketView(in []Market, limit int) []Market {
	n := len(in)
	if limit > 0 && limit < n {
		n = limit
	}
	out := make([]Market, n)
	copy(out, in[:n])
	for i := range out {
		out[i] = c.overlayCLOBBBO(out[i])
	}
	return out
}

// Halted reports a NOT-closed market that is nonetheless not tradeable right now (R70-B #10/F3:
// suspended, resolution proposed/pending, or otherwise not accepting orders). Its book is frozen —
// prices from it are NOT live and proposals/bridges must not quote off it. Only meaningful on
// markets decoded from gamma (fields default false → a closed market reports Halted()==false;
// check Closed separately).
func (m Market) Halted() bool {
	return !m.Closed && (!m.Active || !m.AcceptingOrders || strings.EqualFold(m.UmaResolution, "proposed"))
}

// Category returns the market's broad category label (its first tag, e.g. "Sports"/"Crypto"/
// "Politics"), or "" if no tags are attached. The first tag is the top-level bucket.
func (m Market) Category() string {
	if len(m.Tags) == 0 {
		return ""
	}
	return m.Tags[0].Label
}

func (m Market) YesPrice() float64 {
	var prices []string
	if json.Unmarshal([]byte(m.PricesRaw), &prices) != nil || len(prices) == 0 {
		return 0
	}
	v, _ := strconv.ParseFloat(prices[0], 64)
	return v
}

// Outcomes returns the market's outcome labels, e.g. ["Yes","No"] or team names
// like ["Astros","Guardians"]. Aligned index-for-index with Prices().
func (m Market) Outcomes() []string {
	var out []string
	_ = json.Unmarshal([]byte(m.OutcomesRaw), &out)
	return out
}

// Prices returns the outcome prices (0..1), aligned index-for-index with Outcomes().
func (m Market) Prices() []float64 {
	var raw []string
	if json.Unmarshal([]byte(m.PricesRaw), &raw) != nil {
		return nil
	}
	out := make([]float64, len(raw))
	for i, s := range raw {
		out[i], _ = strconv.ParseFloat(s, 64)
	}
	return out
}

// UpTokenID returns the CLOB token id for outcome-0 ("Up"/"Yes") — the id prices-history needs.
func (m Market) UpTokenID() string {
	var ids []string
	if json.Unmarshal([]byte(m.TokensRaw), &ids) != nil || len(ids) == 0 {
		return ""
	}
	return ids[0]
}

// PricesHistory returns a token's recent price path (oldest→newest), capped to the last `max` points,
// from the CLOB /prices-history endpoint. Best-effort: nil on any error.
func (c *Client) PricesHistory(ctx context.Context, tokenID string, fidelity, max int) []float64 {
	if tokenID == "" {
		return nil
	}
	var resp struct {
		History []struct {
			P float64 `json:"p"`
		} `json:"history"`
	}
	u := fmt.Sprintf("%s/prices-history?market=%s&interval=1h&fidelity=%d", clobBase, url.QueryEscape(tokenID), fidelity)
	if err := c.get(ctx, u, &resp); err != nil || len(resp.History) == 0 {
		return nil
	}
	out := make([]float64, 0, len(resp.History))
	for _, h := range resp.History {
		out = append(out, h.P)
	}
	if max > 0 && len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

type feeScheduleEntry struct {
	schedule FeeSchedule
	at       time.Time
}

var (
	feeScheduleMu    sync.Mutex
	feeScheduleCache = map[string]feeScheduleEntry{}
)

// gammaFeeSchedule returns only a complete fee curve that Gamma proves. In particular, an
// omitted/null feesEnabled field is UNKNOWN rather than fee-free. A contradictory positive
// schedule paired with feesEnabled:false is also unknown.
func gammaFeeSchedule(m Market) (FeeSchedule, bool) {
	if m.FeesEnabled == nil {
		return FeeSchedule{}, false
	}
	if !*m.FeesEnabled {
		if m.FeeSchedule != nil && m.FeeSchedule.Rate > 0 {
			return FeeSchedule{}, false
		}
		return newFeeSchedule(0, 1, true), true
	}
	if m.FeeSchedule == nil || !m.FeeSchedule.valid() {
		return FeeSchedule{}, false
	}
	return *m.FeeSchedule, true
}

func seedGammaFeeSchedule(m Market) {
	if strings.TrimSpace(m.ConditionID) == "" {
		return
	}
	schedule, ok := gammaFeeSchedule(m)
	if !ok {
		return
	}
	feeScheduleMu.Lock()
	feeScheduleCache[m.ConditionID] = feeScheduleEntry{schedule: schedule, at: time.Now()}
	feeScheduleMu.Unlock()
}

// ClobFeeSchedule returns the complete proven per-market fee curve. CLOB fee-details `fd.r/e/to`
// or Gamma's explicit feesEnabled+feeSchedule are accepted. `tbf` is deliberately NOT a fallback:
// in CLOB V2 it is an order/base-fee parameter (commonly 1000 even when fd.r=0.05), not the
// platform curve coefficient. Missing/invalid details remain unknown, never silently free.
func (c *Client) ClobFeeSchedule(condID string) (FeeSchedule, bool) {
	condID = strings.TrimSpace(condID)
	if condID == "" {
		return FeeSchedule{}, false
	}
	feeScheduleMu.Lock()
	if entry, found := feeScheduleCache[condID]; found && time.Since(entry.at) < time.Hour {
		feeScheduleMu.Unlock()
		return entry.schedule, true
	}
	feeScheduleMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var info struct {
		Fd *struct {
			R  *float64 `json:"r"`
			E  *float64 `json:"e"`
			To *bool    `json:"to"`
		} `json:"fd"`
	}
	if err := c.get(ctx, clobBase+"/clob-markets/"+url.QueryEscape(condID), &info); err != nil {
		return FeeSchedule{}, false
	}
	if info.Fd == nil || info.Fd.R == nil || info.Fd.E == nil || info.Fd.To == nil {
		return FeeSchedule{}, false
	}
	schedule := newFeeSchedule(*info.Fd.R, *info.Fd.E, *info.Fd.To)
	if !schedule.valid() {
		return FeeSchedule{}, false
	}
	feeScheduleMu.Lock()
	feeScheduleCache[condID] = feeScheduleEntry{schedule: schedule, at: time.Now()}
	feeScheduleMu.Unlock()
	return schedule, true
}

// ClobFeeUSD resolves the schedule and applies its full exponent/taker-mode semantics.
func (c *Client) ClobFeeUSD(condID string, contracts, price float64, taker bool) (float64, bool) {
	schedule, ok := c.ClobFeeSchedule(condID)
	if !ok {
		return 0, false
	}
	return schedule.FeeUSD(contracts, price, taker)
}

// ClobFeeRate is the legacy coefficient-only view. It is exact only for exponent=1; callers that
// need all current market types must use ClobFeeSchedule/ClobFeeUSD rather than reapplying the old
// fixed p(1-p) curve themselves.
func (c *Client) ClobFeeRate(condID string) (float64, bool) {
	schedule, ok := c.ClobFeeSchedule(condID)
	if !ok || schedule.Exponent != 1 {
		return 0, false
	}
	return schedule.Rate, true
}

var (
	catMu    sync.Mutex
	catCache = map[string]struct {
		cat string
		at  time.Time
	}{}
)

// MarketCategory returns a market's broad category (first tag label) by conditionId, fetched with
// &include_tag=true and cached ~6h (tags rarely change). ok=false on error; "" if untagged. Lets us
// log the category as a feature without a per-signal network hit after the first lookup.
func (c *Client) MarketCategory(condID string) (string, bool) {
	if condID == "" {
		return "", false
	}
	catMu.Lock()
	if e, f := catCache[condID]; f && time.Since(e.at) < 6*time.Hour {
		catMu.Unlock()
		return e.cat, true
	}
	catMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var ms []Market
	u := gammaBase + "/markets?condition_ids=" + url.QueryEscape(condID) + "&include_tag=true&limit=1"
	if err := c.get(ctx, u, &ms); err != nil || len(ms) == 0 {
		return "", false
	}
	cat := ms[0].Category()
	catMu.Lock()
	catCache[condID] = struct {
		cat string
		at  time.Time
	}{cat: cat, at: time.Now()}
	catMu.Unlock()
	return cat, true
}

func (m Market) EventURL() string {
	slug := m.Slug
	if len(m.Events) > 0 && m.Events[0].Slug != "" {
		slug = m.Events[0].Slug
	}
	return "https://polymarket.com/event/" + slug
}

// Resolved returns the winning outcome label and true once the market has settled: the venue
// must mark it CLOSED **and** show a decisive price (one outcome at ~$1). Used to score
// Polymarket backtest signals.
//
// The closed check is load-bearing (audit F3/§5): a bare ≥0.99 print let 15-minute crypto
// windows "settle" up to 7.6 minutes EARLY off a confident-but-live price — self-referential
// grading that inflated the pcrypto edge (a live 0.99 that reverts is a loss the old code
// booked as a win).
func (m Market) Resolved() (string, bool) {
	if !m.Closed {
		return "", false
	}
	outs, prices := m.Outcomes(), m.Prices()
	if len(outs) == 0 || len(outs) != len(prices) {
		return "", false
	}
	for i, p := range prices {
		if p >= 0.99 {
			return outs[i], true
		}
	}
	return "", false
}

// MarketByCondition fetches a single market by conditionId (open OR closed), so settled
// signals can be scored. Returns false if not found.
//
// ⚠ R125 ROOT-CAUSE FIX (the 4.2M-ungraded-rows bug): gamma changed behavior — a bare
// condition_ids lookup now EXCLUDES closed markets entirely (live-verified 2026-07-09: the
// settled Trump-2024 id returns 0 rows plain, 1 row with &closed=true; an OPEN id returns the
// exact opposite). Every settled market therefore looked "gone" to the resolution sweeps, which
// is why ptt sat 89% unresolved and poly-int signal grading limped along on the tiny CLOB
// fallback budget. The fix is a two-pass lookup: plain (open) first, &closed=true on miss.
func (c *Client) MarketByCondition(ctx context.Context, conditionID string) (Market, bool) {
	if conditionID == "" {
		return Market{}, false
	}
	var ms []Market
	u := gammaBase + "/markets?condition_ids=" + url.QueryEscape(conditionID) + "&limit=1"
	if err := c.get(ctx, u, &ms); err == nil && len(ms) > 0 {
		c.registerMarketTokens(ms[0])
		return c.overlayCLOBBBO(ms[0]), true
	}
	ms = nil
	if err := c.get(ctx, u+"&closed=true", &ms); err != nil || len(ms) == 0 {
		return Market{}, false
	}
	c.registerMarketTokens(ms[0])
	return c.overlayCLOBBBO(ms[0]), true
}

// MarketBySlug fetches a single market by its slug (open OR closed). Lets us settle/resolve
// positions that were keyed by slug (e.g. arb legs) without knowing their conditionId.
func (c *Client) MarketBySlug(ctx context.Context, slug string) (Market, bool) {
	if slug == "" {
		return Market{}, false
	}
	var ms []Market
	u := gammaBase + "/markets?slug=" + url.QueryEscape(slug) + "&limit=1"
	if err := c.get(ctx, u, &ms); err == nil && len(ms) > 0 {
		c.registerMarketTokens(ms[0])
		return c.overlayCLOBBBO(ms[0]), true
	}
	// Gamma's default now excludes closed rows. Mirror MarketByCondition's lifecycle-safe
	// two-pass lookup so slug-keyed settlement/mark consumers do not treat a resolved bet as gone.
	ms = nil
	if err := c.get(ctx, u+"&closed=true", &ms); err != nil || len(ms) == 0 {
		return Market{}, false
	}
	c.registerMarketTokens(ms[0])
	return c.overlayCLOBBBO(ms[0]), true
}

// MarketsByConditions batch-fetches markets for many conditionIds in one (chunked) gamma call —
// far more reliable than title-matching against the top-N-by-volume set, which silently misses
// lower-volume game markets (the reason Portfolio P&L showed "—"). Returns conditionId -> Market.
//
// This compatibility wrapper intentionally preserves the historical partial-result contract.
// Callers making deletion/settlement decisions must use MarketsByConditionsStrict: an omitted
// market means "absent" only when every chunk in both lifecycle passes completed successfully.
func (c *Client) MarketsByConditions(ctx context.Context, ids []string) map[string]Market {
	out, _ := c.MarketsByConditionsStrict(ctx, ids)
	return out
}

// MarketsByConditionsStrict is the lifecycle-complete batch lookup with transport failures
// surfaced. It still returns any successful chunks alongside the error so read-only display
// callers can choose to use partial data, but mutation callers must discard results on err.
func (c *Client) MarketsByConditionsStrict(ctx context.Context, ids []string) (map[string]Market, error) {
	out := map[string]Market{}
	seen := map[string]bool{}
	uniq := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" && !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	var fetchErrs []error
	fetch := func(ids []string, closed bool) {
		for i := 0; i < len(ids); i += 20 {
			end := i + 20
			if end > len(ids) {
				end = len(ids)
			}
			u := gammaBase + "/markets?limit=" + strconv.Itoa(end-i)
			for _, id := range ids[i:end] {
				u += "&condition_ids=" + url.QueryEscape(id)
			}
			if closed {
				u += "&closed=true"
			}
			var ms []Market
			if err := c.get(ctx, u, &ms); err != nil {
				phase := "open"
				if closed {
					phase = "closed"
				}
				fetchErrs = append(fetchErrs, fmt.Errorf("gamma conditions %s chunk %d-%d: %w", phase, i, end, err))
				continue
			}
			for _, m := range ms {
				if m.ConditionID != "" {
					c.registerMarketTokens(m)
					out[m.ConditionID] = c.overlayCLOBBBO(m)
				}
			}
		}
	}
	fetch(uniq, false)
	// R125 (same root cause as MarketByCondition): gamma now EXCLUDES closed markets from bare
	// condition_ids lookups — the misses are re-queried with &closed=true, so settled markets
	// (the resolution sweeps' whole purpose here) are served again.
	var missing []string
	for _, id := range uniq {
		if _, ok := out[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		fetch(missing, true)
	}
	return out, errors.Join(fetchErrs...)
}

// MarketBySearch finds a market via gamma's public-search (q=query) and returns the one whose
// conditionId matches — a fallback for markets gamma WON'T serve by condition_ids (it returns []
// for the crypto up/down micro-markets), so stuck positions can still be settled by title.
func (c *Client) MarketBySearch(ctx context.Context, query, condID string) (Market, bool) {
	if strings.TrimSpace(query) == "" {
		return Market{}, false
	}
	var resp struct {
		Events []struct {
			Markets []Market `json:"markets"`
		} `json:"events"`
	}
	u := gammaBase + "/public-search?limit_per_type=20&q=" + url.QueryEscape(query)
	if err := c.get(ctx, u, &resp); err != nil {
		return Market{}, false
	}
	// Prefer an exact conditionId match, but FALL BACK to an exact (normalized) title match.
	// Sports sub-markets (O/U corners, BTTS, spreads, 2nd-half lines) return [] from gamma's
	// condition_ids endpoint, so conditionId matching alone left them stuck at "—" with a 404
	// conditionId link. Matching on the question text resolves them (and yields the real slug URL).
	want := strings.ToLower(strings.TrimSpace(query))
	var titleHit *Market
	for ei := range resp.Events {
		for mi := range resp.Events[ei].Markets {
			m := resp.Events[ei].Markets[mi]
			if condID != "" && strings.EqualFold(strings.TrimSpace(m.ConditionID), strings.TrimSpace(condID)) {
				return m, true
			}
			if titleHit == nil && strings.ToLower(strings.TrimSpace(m.Question)) == want {
				mm := m
				titleHit = &mm
			}
		}
	}
	if titleHit != nil {
		return *titleHit, true
	}
	return Market{}, false
}

// MarketByCLOB resolves a market straight from the CLOB API by conditionId. This is the ONLY
// reliable resolver for sports sub-markets (O/U corners, BTTS, spreads, 2nd-half lines) — gamma's
// /markets returns [] for them by both condition_ids AND slug, which is what left those positions
// stuck at "—" with a 404 link. CLOB returns the live token prices + market slug, so it fixes the
// mark, the P&L, and the link. Outcome prices come from tokens[].price.
func (c *Client) MarketByCLOB(ctx context.Context, conditionID string) (Market, bool) {
	m, ok, _ := c.MarketByCLOBStrict(ctx, conditionID)
	return m, ok
}

type conditionTokenEntry struct {
	conditionID string
	found       bool
	at          time.Time
}

// ConditionByToken recovers the canonical parent condition id for one outcome token through
// CLOB's official /markets-by-token/{token_id} endpoint. Token mappings are immutable, so hits
// remain cached for 24 hours; definitive misses are cached briefly to prevent repeated lookups.
// Every response is validated as exactly 0x + 64 hex before it can enter the signal ledger.
func (c *Client) ConditionByToken(ctx context.Context, tokenID string) (string, bool, error) {
	rawTokenID := tokenID
	tokenID, ok := normalizeTokenID(tokenID)
	if !ok {
		return "", false, fmt.Errorf("polymarket token id %q: invalid decimal token", rawTokenID)
	}
	if conditionID, found, known := c.cachedConditionByToken(tokenID, time.Now()); known {
		return conditionID, found, nil
	}

	if c.ctLimiter != nil {
		if err := c.ctLimiter.Wait(ctx); err != nil {
			return "", false, err
		}
	}
	var r struct {
		ConditionID      string `json:"condition_id"`
		ConditionIDCamel string `json:"conditionId"`
	}
	err := c.get(ctx, clobBase+"/markets-by-token/"+url.PathEscape(tokenID), &r)
	if errors.Is(err, ErrNotFound) {
		c.ctMu.Lock()
		if c.ctCache == nil {
			c.ctCache = make(map[string]conditionTokenEntry)
		}
		c.ctCache[tokenID] = conditionTokenEntry{at: time.Now()}
		c.ctMu.Unlock()
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	raw := r.ConditionID
	if raw == "" {
		raw = r.ConditionIDCamel
	}
	conditionID, ok := polyid.NormalizeConditionID(raw)
	if !ok {
		return "", false, fmt.Errorf("%w from /markets-by-token for token %s: %q", ErrInvalidConditionID, tokenID, raw)
	}
	c.ctMu.Lock()
	if c.ctCache == nil {
		c.ctCache = make(map[string]conditionTokenEntry)
	}
	c.ctCache[tokenID] = conditionTokenEntry{conditionID: conditionID, found: true, at: time.Now()}
	c.ctMu.Unlock()
	return conditionID, true, nil
}

// CachedConditionByToken reports the tri-state token cache without doing network I/O.
// known=true/found=false is a recently confirmed 404. The ingest budget uses this so immutable
// cache hits never consume one of its bounded external-recovery slots.
func (c *Client) CachedConditionByToken(tokenID string) (conditionID string, found, known bool) {
	tokenID, ok := normalizeTokenID(tokenID)
	if !ok {
		return "", false, false
	}
	return c.cachedConditionByToken(tokenID, time.Now())
}

func (c *Client) cachedConditionByToken(tokenID string, now time.Time) (conditionID string, found, known bool) {
	c.ctMu.Lock()
	defer c.ctMu.Unlock()
	if c.ctCache == nil {
		return "", false, false
	}
	ce, cached := c.ctCache[tokenID]
	if !cached {
		return "", false, false
	}
	ttl := 24 * time.Hour
	if !ce.found {
		ttl = 10 * time.Minute
	}
	if now.Sub(ce.at) >= ttl {
		delete(c.ctCache, tokenID)
		return "", false, false
	}
	return ce.conditionID, ce.found, true
}

// normalizeTokenID accepts one positive uint256-style decimal token identifier and strips
// redundant leading zeroes so the same immutable mapping has one cache key.
func normalizeTokenID(tokenID string) (string, bool) {
	tokenID = strings.TrimSpace(tokenID)
	if len(tokenID) == 0 || len(tokenID) > 78 { // max uint256 is 78 decimal digits
		return "", false
	}
	for i := range tokenID {
		if tokenID[i] < '0' || tokenID[i] > '9' {
			return "", false
		}
	}
	tokenID = strings.TrimLeft(tokenID, "0")
	if tokenID == "" {
		return "", false
	}
	return tokenID, true
}

// MarketByCLOBStrict — R125: MarketByCLOB with the error surfaced, so callers can tell a
// definitive venue 404 from a transient failure. It also decodes CLOB's authoritative winner.
func (c *Client) MarketByCLOBStrict(ctx context.Context, conditionID string) (Market, bool, error) {
	if strings.TrimSpace(conditionID) == "" {
		return Market{}, false, nil
	}
	var r struct {
		ConditionID string `json:"condition_id"`
		Question    string `json:"question"`
		MarketSlug  string `json:"market_slug"`
		Closed      bool   `json:"closed"`
		Tokens      []struct {
			Outcome string  `json:"outcome"`
			Price   float64 `json:"price"`
			Winner  bool    `json:"winner"`
		} `json:"tokens"`
	}
	if err := c.get(ctx, clobBase+"/markets/"+url.PathEscape(conditionID), &r); err != nil {
		return Market{}, false, err
	}
	if r.ConditionID == "" || len(r.Tokens) == 0 {
		return Market{}, false, nil
	}
	winners := 0
	for _, t := range r.Tokens {
		if t.Winner {
			winners++
		}
	}
	outs := make([]string, len(r.Tokens))
	prices := make([]string, len(r.Tokens))
	for i, t := range r.Tokens {
		outs[i] = t.Outcome
		px := t.Price
		if r.Closed && winners == 1 { // exactly one winner: the venue's own settlement is decisive
			if t.Winner {
				px = 1
			} else {
				px = 0
			}
		}
		prices[i] = strconv.FormatFloat(px, 'f', -1, 64)
	}
	ob, _ := json.Marshal(outs)
	pb, _ := json.Marshal(prices)
	return Market{
		Question:    r.Question,
		ConditionID: r.ConditionID,
		Slug:        r.MarketSlug,
		OutcomesRaw: string(ob),
		PricesRaw:   string(pb),
		Closed:      r.Closed,
	}, true, nil
}

type cryptoUp struct {
	up float64
	at time.Time
}

// coinFull maps our short coin codes to Polymarket's full names used in human-readable slugs.
var coinFull = map[string]string{"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "xrp", "doge": "dogecoin"}

// etNow returns the current time in US/Eastern (Polymarket's hourly/daily up/down slugs are
// labeled in ET). Falls back to a fixed EDT (UTC−4) offset if the tz database isn't available.
func etNow() time.Time {
	if loc, err := time.LoadLocation("America/New_York"); err == nil {
		return time.Now().In(loc)
	}
	return time.Now().UTC().Add(-4 * time.Hour)
}

// CryptoUpDownMarket returns the CURRENT active Polymarket up/down market for a coin + duration.
// Confirmed live (gamma, June 2026): 5m/15m/4h use the timestamp slug `{coin}-updown-{dur}-{ts}`
// (ts = duration-aligned window start); hourly + daily use ET-labeled human-readable slugs
// (hourly: `bitcoin-up-or-down-june-26-2026-12pm-et`; daily: `bitcoin-up-or-down-on-june-26-2026`).
// Tries the current window then ±1 so a boundary tick still resolves. Outcomes are ["Up","Down"].
func (c *Client) CryptoUpDownMarket(ctx context.Context, coin, dur string) (Market, bool) {
	coin = strings.ToLower(strings.TrimSpace(coin))
	try := func(slug string) (Market, bool) {
		if m, ok := c.MarketBySlug(ctx, slug); ok && !m.Closed && m.ConditionID != "" {
			return m, true
		}
		return Market{}, false
	}
	switch dur {
	case "5m", "15m", "4h":
		step := int64(900)
		if dur == "4h" {
			step = 14400
		} else if dur == "5m" {
			step = 300
		}
		win := (time.Now().Unix() / step) * step
		for _, ts := range []int64{win, win + step, win - step} {
			if m, ok := try(fmt.Sprintf("%s-updown-%s-%d", coin, dur, ts)); ok {
				return m, true
			}
		}
	case "1h":
		name := coinFull[coin]
		if name == "" {
			return Market{}, false
		}
		for _, off := range []int{0, -1, 1} {
			t := etNow().Add(time.Duration(off) * time.Hour)
			h12, ap := t.Hour(), "am"
			if h12 >= 12 {
				ap = "pm"
			}
			if h12 > 12 {
				h12 -= 12
			}
			if h12 == 0 {
				h12 = 12
			}
			slug := fmt.Sprintf("%s-up-or-down-%s-%d-%d-%d%s-et", name, strings.ToLower(t.Month().String()), t.Day(), t.Year(), h12, ap)
			if m, ok := try(slug); ok {
				return m, true
			}
		}
	case "1d":
		name := coinFull[coin]
		if name == "" {
			return Market{}, false
		}
		base := etNow()
		if base.Hour() < 12 { // daily window is noon-ET→noon-ET: before noon, the live one started yesterday
			base = base.AddDate(0, 0, -1)
		}
		for _, off := range []int{0, -1, 1} {
			t := base.AddDate(0, 0, off)
			slug := fmt.Sprintf("%s-up-or-down-on-%s-%d-%d", name, strings.ToLower(t.Month().String()), t.Day(), t.Year())
			if m, ok := try(slug); ok {
				return m, true
			}
		}
	}
	return Market{}, false
}

// PolyCryptoUpProb returns Polymarket's CURRENT 15-minute "Up" probability for a coin
// (btc/eth/sol/…), read from the btc-updown-15m series — the SAME 15-min up/down bet Kalshi
// runs (KXBTC15M), resolved off the Chainlink price. Outcomes are ["Up","Down"], so the YES
// (outcome-0) price IS the Up probability. The window-start slug is btc-updown-15m-<unix>, unix
// aligned to 15-min UTC boundaries; we try the current window then ±1 to cover the boundary.
// Cached ~20s. ok=false means no live Poly market (caller falls back to Kalshi flow alone).
func (c *Client) PolyCryptoUpProb(ctx context.Context, coin string) (float64, bool) {
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return 0, false
	}
	c.cuMu.Lock()
	if e, ok := c.cuCache[coin]; ok && time.Since(e.at) < c.ttl(20*time.Second) {
		c.cuMu.Unlock()
		return e.up, e.up > 0
	}
	c.cuMu.Unlock()
	win := (time.Now().Unix() / 900) * 900
	up := 0.0
	for _, ts := range []int64{win, win + 900, win - 900} {
		slug := coin + "-updown-15m-" + strconv.FormatInt(ts, 10)
		if m, ok := c.MarketBySlug(ctx, slug); ok && !m.Closed {
			if p := m.YesPrice(); p > 0 && p < 1 {
				up = p
				break
			}
		}
	}
	c.cuMu.Lock()
	if c.cuCache == nil {
		c.cuCache = map[string]cryptoUp{}
	}
	c.cuCache[coin] = cryptoUp{up: up, at: time.Now()}
	c.cuMu.Unlock()
	return up, up > 0
}

// GetTopMarkets returns active markets sorted by 24h volume. The fast flat pull warms the high-
// volume head; the complete keyset event crawl merges every nested active market in the background.
// CLOB WS overlays BBOs, so the 15s gamma cache is metadata/lifecycle rather than a price poll.
func (c *Client) GetTopMarkets(ctx context.Context, limit int) ([]Market, error) {
	if limit <= 0 {
		limit = 12
	}
	c.mktMu.Lock()
	// R132: executable prices moved to CLOB WS. Gamma now refreshes metadata/lifecycle every 15s,
	// cutting the old 20-page/5s REST churn by 3× without slowing quotes.
	// R73 STAMPEDE GUARD (same shape as the Kalshi board client): a widened refresh takes several
	// seconds of sequential page fetches, during which every poller used to see a stale cache and
	// fire its OWN full pull — 20 pages × N callers would blow gamma's 30/s. Only the first stale
	// caller refreshes; the rest serve the last good cache (or briefly wait on a cold boot).
	if (c.mktCache != nil && time.Since(c.mktAt) < c.ttl(15*time.Second)) || (c.mktCache != nil && c.mktRefreshing) {
		out := c.marketView(c.mktCache, limit)
		c.mktMu.Unlock()
		return out, nil
	}
	if c.mktRefreshing { // cold cache + refresh in flight: wait for the first puller (≤10s)
		c.mktMu.Unlock()
		for i := 0; i < 200; i++ {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
			}
			c.mktMu.Lock()
			done, cur := !c.mktRefreshing, c.mktCache
			c.mktMu.Unlock()
			if cur != nil || done {
				return c.marketView(cur, limit), nil
			}
		}
		return nil, fmt.Errorf("polymarket markets refresh timed out waiting for the in-flight pull")
	}
	c.mktRefreshing = true
	c.mktMu.Unlock()
	defer func() {
		c.mktMu.Lock()
		c.mktRefreshing = false
		c.mktMu.Unlock()
	}()

	// R72-B ingestion-breadth AUDIT FINDING (probed 2026-07-04): gamma /markets silently caps
	// limit at 100 rows per request, so pagination by offset is the only way to breadth.
	// The 20-page volume head is a fast cold-start scaffold, not the universe boundary. The keyset
	// event crawl below is the complete active universe and preserves every nested sub-market.
	var markets []Market
	seen := map[string]bool{}
	for off := 0; off < 2000; off += 100 {
		// R106 (universal market tree): include_tag=true rides the SAME call so Market.Category()
		// (first tag label — venue metadata) works on the bulk cache; the tree's poly-int genre
		// derivation was Unknown-only without it. Payload grows a little; parse unchanged.
		u := gammaBase + "/markets?active=true&closed=false&order=volume24hr&ascending=false&limit=100&include_tag=true"
		if off > 0 {
			u += "&offset=" + strconv.Itoa(off)
		}
		var page []Market
		if err := c.get(ctx, u, &page); err != nil {
			if len(markets) > 0 {
				break // keep the pages we have
			}
			return nil, err
		}
		for _, m := range page {
			if m.ConditionID == "" || seen[m.ConditionID] {
				continue
			}
			seen[m.ConditionID] = true
			markets = append(markets, m)
		}
		if len(page) < 100 {
			break // universe smaller than the budget — done
		}
	}

	// Merge the complete keyset event cache. A World Cup match's low-volume spreads/totals/props,
	// for example, remain present even when absent from the volume head.
	c.maybeRefreshEventCrawl()
	c.evtMu.Lock()
	nested := c.evtCache
	c.evtMu.Unlock()
	added := 0
	for _, m := range nested {
		if m.ConditionID == "" || seen[m.ConditionID] {
			continue
		}
		seen[m.ConditionID] = true
		markets = append(markets, m)
		added++
	}
	if added > 0 { // keep the volume ordering contract for limit-slicing callers
		sort.Slice(markets, func(i, j int) bool { return markets[i].Volume24hr > markets[j].Volume24hr })
	}

	c.mktMu.Lock()
	c.mktCache = markets
	c.mktAt = time.Now()
	c.mktMu.Unlock()
	c.registerMarkets(markets)
	return c.marketView(markets, limit), nil
}

// maybeRefreshEventCrawl starts the background EVENTS-with-nested-markets crawl. R132 uses the
// official keyset cursor endpoint to completion: every active event and every nested market lands,
// not merely the former top-800-event volume slice. A partial/error crawl never replaces the last
// complete snapshot. It never blocks callers; the next GetTopMarkets refresh merges the result.
func (c *Client) maybeRefreshEventCrawl() {
	c.evtMu.Lock()
	start := time.Since(c.evtAt) > 5*time.Minute && !c.evtRefreshing
	if start {
		c.evtRefreshing = true
	}
	c.evtMu.Unlock()
	if !start {
		return
	}
	go func() {
		defer func() {
			c.evtMu.Lock()
			c.evtRefreshing = false
			// Cold discovery must not depend on an unrelated volume-head request succeeding before
			// it can resume. Gamma's flat head can itself time out, which previously left a valid
			// keyset checkpoint idle forever. A single delayed retry resumes the exact cursor; the
			// start latch prevents overlap when another caller already restarted it.
			retryCold := c.evtAt.IsZero() && (len(c.evtPartial) > 0 || c.evtLastErr != "")
			c.evtMu.Unlock()
			if retryCold {
				time.AfterFunc(15*time.Second, c.maybeRefreshEventCrawl)
			}
		}()
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		type gammaEvent struct {
			Slug   string `json:"slug"`
			Active bool   `json:"active"`
			Closed bool   `json:"closed"`
			Tags   []struct {
				Label string `json:"label"`
				Slug  string `json:"slug"`
			} `json:"tags"`
			Markets []Market `json:"markets"`
		}
		type gammaEventPage struct {
			Events     []gammaEvent `json:"events"`
			NextCursor string       `json:"next_cursor"`
		}
		c.evtMu.Lock()
		out := append([]Market(nil), c.evtPartial...)
		cursor := c.evtCursor
		pages := c.evtPages
		pageLimit := c.evtPageLimit
		if pageLimit < 2 {
			pageLimit = 20
		}
		c.evtMu.Unlock()
		seen := make(map[string]bool, len(out))
		for _, m := range out {
			if m.ConditionID != "" {
				seen[m.ConditionID] = true
			}
		}
		seenCursor := map[string]bool{}
		complete := false
		for {
			// Nested events are far heavier than flat /markets rows. Live R133 probe: limit=2 took
			// 4.4s while a flat 100-row page timed out after 25s with ~380KB truncated. The former
			// limit=100 plus the client's 15s timeout made an all-or-nothing crawl practically
			// impossible. Twenty keeps payloads bounded; getCatalogPage supplies a 45s retry lane.
			u := gammaBase + "/events/keyset?active=true&closed=false&limit=" + strconv.Itoa(pageLimit) + "&ascending=true"
			if cursor != "" {
				u += "&after_cursor=" + url.QueryEscape(cursor)
			}
			var page gammaEventPage
			if err := c.getCatalogPage(ctx, u, &page); err != nil {
				// A single event can contain hundreds of nested sub-markets. If the exact cursor
				// repeatedly exceeds even the dedicated request lane, resume it next cycle with a
				// smaller event page; keyset semantics make the change lossless.
				if pageLimit > 2 {
					pageLimit = max(2, pageLimit/2)
				}
				c.evtMu.Lock()
				c.evtPartial, c.evtCursor, c.evtPages = out, cursor, pages
				c.evtPageLimit = pageLimit
				c.evtLastErr = err.Error()
				c.evtMu.Unlock()
				return // resume this exact page next background attempt; last complete stays authoritative
			}
			for _, ev := range page.Events {
				if ev.Closed || !ev.Active {
					continue
				}
				for _, m := range ev.Markets {
					if m.ConditionID == "" || seen[m.ConditionID] || m.Closed || !m.Active {
						continue
					}
					seen[m.ConditionID] = true
					if len(m.Events) == 0 && ev.Slug != "" {
						m.Events = []MarketEventRef{{Slug: ev.Slug}} // nested markets carry no parent ref — EventURL needs it
					}
					if len(m.Tags) == 0 && len(ev.Tags) > 0 {
						m.Tags = ev.Tags
					}
					out = append(out, m)
				}
			}
			next := strings.TrimSpace(page.NextCursor)
			if next == "" {
				complete = true
				break
			}
			if next == cursor || seenCursor[next] {
				c.evtMu.Lock()
				c.evtPartial, c.evtCursor, c.evtPages = out, cursor, pages
				c.evtLastErr = "keyset cursor loop/replay"
				c.evtMu.Unlock()
				return // cursor loop/replay: never publish an incomplete or duplicated universe
			}
			seenCursor[next] = true
			cursor = next
			pages++
			c.evtMu.Lock()
			c.evtPartial, c.evtCursor, c.evtPages = out, cursor, pages
			c.evtPageLimit = pageLimit
			c.evtLastErr = ""
			c.evtMu.Unlock()
		}
		if !complete || len(out) == 0 {
			return
		}
		c.registerMarkets(out)
		c.evtMu.Lock()
		c.evtCache, c.evtAt = out, time.Now()
		at, path := c.evtAt, c.evtCachePath
		c.evtPartial, c.evtCursor, c.evtPages, c.evtPageLimit, c.evtLastErr = nil, "", 0, 0, ""
		c.evtMu.Unlock()
		_ = persistCatalog(path, at, out) // restart-safe last-good; a disk error cannot poison RAM truth
	}()
}

type holdersResponse []struct {
	Holders []struct {
		ProxyWallet  string  `json:"proxyWallet"` // R70-B SCHEMA_AUDIT #4: holder IDENTITY (was undecoded) — skill-weighting + HHI
		Amount       float64 `json:"amount"`
		OutcomeIndex int     `json:"outcomeIndex"`
	} `json:"holders"`
}

// Holder is one top holder of a market's outcome tokens (data-api /holders, cap 20/token):
// the wallet, its token amount and which outcome it holds (0 = YES-side token).
type Holder struct {
	Wallet       string
	Amount       float64
	OutcomeIndex int
}

type holdersEntry struct {
	holders  []Holder
	side     string
	strength float64
	at       time.Time
}

// holdersFetch fetches + caches a market's top holders (~5 min TTL, bounded map). ONE fetch path
// for both the lean (HoldersSide) and the identity features (HoldersCached) — R70-B #4's rule:
// the feature computation must ride the EXISTING fetch volume, never add its own.
func (c *Client) holdersFetch(ctx context.Context, condID string) (holdersEntry, bool) {
	condID = strings.TrimSpace(condID)
	if condID == "" {
		return holdersEntry{}, false
	}
	c.hdMu.Lock()
	if e, ok := c.hdCache[condID]; ok && time.Since(e.at) < c.ttl(5*time.Minute) {
		c.hdMu.Unlock()
		return e, true
	}
	c.hdMu.Unlock()
	var groups holdersResponse
	if err := c.get(ctx, dataBase+"/holders?market="+url.QueryEscape(condID)+"&limit=20", &groups); err != nil {
		return holdersEntry{}, false
	}
	e := holdersEntry{at: time.Now()}
	var yesSize, noSize float64
	for _, g := range groups {
		for _, h := range g.Holders {
			switch h.OutcomeIndex {
			case 0:
				yesSize += h.Amount
			case 1:
				noSize += h.Amount
			}
			if h.Amount > 0 {
				e.holders = append(e.holders, Holder{Wallet: strings.ToLower(strings.TrimSpace(h.ProxyWallet)), Amount: h.Amount, OutcomeIndex: h.OutcomeIndex})
			}
		}
	}
	if total := yesSize + noSize; total > 0 {
		e.side, e.strength = "YES", yesSize/total
		if noSize > yesSize {
			e.side, e.strength = "NO", noSize/total
		}
	}
	c.hdMu.Lock()
	if c.hdCache == nil {
		c.hdCache = map[string]holdersEntry{}
	}
	if len(c.hdCache) >= 400 { // bounded: evict the oldest half (same pattern as pnCache)
		type kv struct {
			k string
			t time.Time
		}
		all := make([]kv, 0, len(c.hdCache))
		for k, en := range c.hdCache {
			all = append(all, kv{k, en.at})
		}
		sort.Slice(all, func(i, j int) bool { return all[i].t.Before(all[j].t) })
		for _, en := range all[:len(all)/2] {
			delete(c.hdCache, en.k)
		}
	}
	c.hdCache[condID] = e
	c.hdMu.Unlock()
	return e, true
}

// HoldersCached returns the CACHED top-holder list for a market — no network (R70-B #4: the
// insertSignal feature path must never fetch; a cold cache just logs NULL). Fresh ≤ 30 min: the
// holders mix drifts slowly, and the cache is refreshed by the consensus render's own fetches.
func (c *Client) HoldersCached(condID string) ([]Holder, bool) {
	condID = strings.TrimSpace(condID)
	if condID == "" {
		return nil, false
	}
	c.hdMu.Lock()
	defer c.hdMu.Unlock()
	e, ok := c.hdCache[condID]
	if !ok || time.Since(e.at) > 30*time.Minute || len(e.holders) == 0 {
		return nil, false
	}
	return append([]Holder(nil), e.holders...), true
}

// Consensus summarizes where concentrated big money sits in a market: the side
// (YES/NO) that the largest holders cluster on, and how strongly (0..1 share of
// top-holder size). Compare Strength to YesPrice to spot smart-money divergence.
type Consensus struct {
	Question  string  `json:"question"`
	Side      string  `json:"side"`
	Strength  float64 `json:"strength"`
	YesPrice  float64 `json:"yes_price"`
	Volume24h float64 `json:"volume_24h"`
	URL       string  `json:"url"`
}

func (c *Client) ConsensusFor(ctx context.Context, m Market) (Consensus, bool) {
	e, ok := c.holdersFetch(ctx, m.ConditionID) // R70-B #4: shared fetch+cache path
	if !ok || e.side == "" {
		return Consensus{}, false
	}
	return Consensus{
		Question: m.Question, Side: e.side, Strength: e.strength,
		YesPrice: m.YesPrice(), Volume24h: m.Volume24hr, URL: m.EventURL(),
	}, true
}

// HoldersSide returns the market-wide top-holder lean by conditionId (public /holders, no auth):
// which outcome the largest holders cluster on (YES/NO) and how strongly (0..1 share of top-holder
// size). Independent of our leaderboard set — it's where the biggest money in the WHOLE market sits,
// a second conviction read on the consensus.
func (c *Client) HoldersSide(ctx context.Context, condID string) (side string, strength float64, ok bool) {
	e, got := c.holdersFetch(ctx, condID) // R70-B #4: one fetch path — also warms HoldersCached for the signal features
	if !got || e.side == "" {
		return "", 0, false
	}
	return e.side, e.strength, true
}

// TopConsensus fetches the most active markets and returns those whose big-money
// holders are strongly one-sided (Strength >= minStrength), sorted by volume.
func (c *Client) TopConsensus(ctx context.Context, scan int, minStrength float64) ([]Consensus, error) {
	markets, err := c.GetTopMarkets(ctx, scan)
	if err != nil {
		return nil, err
	}
	out := make([]Consensus, 0, len(markets))
	for _, m := range markets {
		if m.Closed {
			continue
		}
		if cons, ok := c.ConsensusFor(ctx, m); ok && cons.Strength >= minStrength {
			out = append(out, cons)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Volume24h > out[j].Volume24h })
	return out, nil
}

// PolyTrade is one recent public taker trade (data API). It is attributable to a
// wallet (Polymarket is on-chain). Notional in dollars = Size * Price.
type PolyTrade struct {
	ProxyWallet  string  `json:"proxyWallet"`
	ConditionID  string  `json:"conditionId"`
	Name         string  `json:"name"`
	Pseudonym    string  `json:"pseudonym"`
	Side         string  `json:"side"` // BUY | SELL
	Size         float64 `json:"size"`
	Price        float64 `json:"price"`
	Timestamp    int64   `json:"timestamp"`
	Title        string  `json:"title"`
	EventSlug    string  `json:"eventSlug"`
	Outcome      string  `json:"outcome"`
	OutcomeIndex int     `json:"outcomeIndex"`
}

// GetRecentTrades returns recent public taker trades across ALL markets, newest
// first. Filter/sort by notional (Size*Price) to find whales.
func (c *Client) GetRecentTrades(ctx context.Context, limit int) ([]PolyTrade, error) {
	if limit <= 0 {
		limit = 500
	}
	// NO cache-buster. A unique &_= URL MISSES Polymarket's CDN edge cache (a few seconds
	// fresh — what the website reads) and falls through to a ~5-min-lagging origin replica.
	// That buster was on this request from day one, which is why the whale feed was stale
	// "since we started." Hitting the plain cacheable URL returns live trades.
	u := dataBase + "/trades?takerOnly=true&filterType=CASH&filterAmount=100&limit=" + strconv.Itoa(limit)
	var trades []PolyTrade
	if err := c.get(ctx, u, &trades); err != nil {
		if err2 := c.get(ctx, u, &trades); err2 != nil { // one retry on a transient blip
			return nil, err2
		}
	}
	return trades, nil
}

// Position is one of a wallet's current holdings (data API).
type Position struct {
	ConditionID  string  `json:"conditionId"`
	Outcome      string  `json:"outcome"`
	Title        string  `json:"title"`
	EventSlug    string  `json:"eventSlug"`
	Size         float64 `json:"size"`
	CurrentValue float64 `json:"currentValue"`
	CurPrice     float64 `json:"curPrice"`
	AvgPrice     float64 `json:"avgPrice"` // the whale's ENTRY price (audit Q2: never captured — consensus copied stale entries at today's price, buying the whale's exit liquidity)
	CashPnl      float64 `json:"cashPnl"`
	RealizedPnl  float64 `json:"realizedPnl"`
}

// GetPositions returns a wallet's current positions (cached ~5min). On-chain data.
func (c *Client) GetPositions(ctx context.Context, wallet string) []Position {
	c.posMu.Lock()
	if e, ok := c.posCache[wallet]; ok && time.Since(e.at) < c.ttl(5*time.Minute) {
		c.posMu.Unlock()
		return e.ps
	}
	c.posMu.Unlock()

	// Throttle so the consensus fanout never bursts past Polymarket's /positions cap (15/s)
	// and trips the data-api throttle that was staling the whale /trades feed.
	if c.posLimiter != nil {
		if err := c.posLimiter.Wait(ctx); err != nil {
			// R85 limiter-usage sweep: a dead/starved ctx must NOT fall through to an
			// UNTHROTTLED venue call (the ignored error let expired-ctx bursts hammer the
			// data-api past its cap). Serve stale cache like any other fetch failure.
			c.posMu.Lock()
			prev, had := c.posCache[wallet]
			c.posMu.Unlock()
			if had {
				return prev.ps
			}
			return nil
		}
	}
	var ps []Position
	err := c.get(ctx, dataBase+"/positions?limit=200&sortBy=CURRENT&user="+url.QueryEscape(wallet), &ps)
	if err != nil {
		// ERROR ≠ EMPTY (audit §6): caching a failed fetch as "no positions" for 5 minutes poisoned
		// the consensus feed — a transient 429 read as "every whale exited". Keep any previous entry
		// (stale beats wrong) and retry on the next call.
		c.posMu.Lock()
		prev, had := c.posCache[wallet]
		c.posMu.Unlock()
		if had {
			return prev.ps
		}
		return nil
	}
	c.posMu.Lock()
	c.posCache[wallet] = posEntry{ps: ps, at: time.Now()}
	c.posMu.Unlock()
	return ps
}

// WalletPnL sums P&L across a wallet's current positions.
func (c *Client) WalletPnL(ctx context.Context, wallet string) float64 {
	var pnl float64
	for _, p := range c.GetPositions(ctx, wallet) {
		pnl += p.CashPnl + p.RealizedPnl
	}
	return pnl
}

// Activity is one on-chain action from a wallet's Polymarket history (public data-api
// /activity, no auth). Verified live June 2026: flat JSON array, newest first. We filter
// to type=TRADE — the directional bets whose outcomes we can score into a real hit rate.
type Activity struct {
	ProxyWallet     string  `json:"proxyWallet"`
	Timestamp       int64   `json:"timestamp"` // unix seconds
	ConditionID     string  `json:"conditionId"`
	Type            string  `json:"type"`     // TRADE | SPLIT | MERGE | REDEEM | REWARD | CONVERSION
	Size            float64 `json:"size"`     // shares
	UsdcSize        float64 `json:"usdcSize"` // legacy wire name; current V2 collateral is pUSD
	TransactionHash string  `json:"transactionHash"`
	Price           float64 `json:"price"`
	Asset           string  `json:"asset"` // ERC-1155 token id (the specific outcome)
	Side            string  `json:"side"`  // BUY | SELL
	OutcomeIndex    int     `json:"outcomeIndex"`
	Title           string  `json:"title"`
	Slug            string  `json:"slug"`
	EventSlug       string  `json:"eventSlug"`
	Outcome         string  `json:"outcome"` // outcome label (e.g. "Yes"/"No"/team)
	Name            string  `json:"name"`
}

// ProfitWallet is one all-time-profit leaderboard wallet (for the smart-money DB roster).
type ProfitWallet struct {
	Wallet string
	Name   string
	Profit float64
	Rank   int // 1-based position in the all-time profit ranking
}

// TopProfitWallets returns all-time leaderboard wallets with profit >= minProfit, up to max
// (Rank = position in the all-time ranking). Own fetch + ~30min cache so it never perturbs the
// shared consensus roster. R82 PROBE (2026-07-05): lb-api now serves EXACTLY the top 50 per
// window — limit>50 silently caps at 50 rows and offset is IGNORED (every page repeats the same
// head), so ~50/window is the hard ceiling; the newW==0 guard below stops the loop after page 1.
func (c *Client) TopProfitWallets(ctx context.Context, minProfit float64, max int) []ProfitWallet {
	c.tpwMu.Lock()
	cached := c.tpw
	fresh := cached != nil && time.Since(c.tpwAt) < c.ttl(30*time.Minute)
	c.tpwMu.Unlock()
	if fresh {
		return filterProfit(cached, minProfit, max)
	}
	// SCOPE FIX (audit Q2): lb-api silently returns ~50 rows regardless of limit — "top 1000" was
	// a no-op and the roster collapsed to ~50 mega-whales (the exact population the 2026 preprint
	// says UNDERPERFORMS). Paginate with offset until the API stops giving new wallets, and UNION
	// the month window (different, more-active wallets than all-time) for broader coverage.
	// R82: the month window token is "30d" — "1m" started returning 400 Bad Request (probed
	// 2026-07-05), which made the month-union contribute ZERO wallets silently. Offset is also
	// ignored server-side now (same head every page), so each window yields its top ~50 and the
	// newW==0 guard exits after one page — the union of the two windows is the whole visible pool.
	var out []ProfitWallet
	seen := map[string]bool{}
	for _, window := range []string{"all", "30d"} {
		for offset := 0; offset < 1000; {
			var entries []leaderboardEntry
			u := "https://lb-api.polymarket.com/profit?window=" + window + "&limit=100&offset=" + strconv.Itoa(offset)
			if err := c.get(ctx, u, &entries); err != nil || len(entries) == 0 {
				break
			}
			newW := 0
			for i, e := range entries {
				w := strings.ToLower(strings.TrimSpace(e.ProxyWallet))
				if w == "" || seen[w] {
					continue
				}
				seen[w] = true
				newW++
				out = append(out, ProfitWallet{Wallet: w, Name: cleanLBName(e.Name, e.Pseudonym, e.ProxyWallet), Profit: e.Amount, Rank: offset + i + 1})
			}
			offset += len(entries)
			if newW == 0 || len(entries) < 20 {
				break // offset unsupported (same head repeated) or the window is exhausted
			}
		}
	}
	if len(out) == 0 {
		return filterProfit(cached, minProfit, max) // keep last good roster on a transient fetch error
	}
	c.tpwMu.Lock()
	c.tpw, c.tpwAt = out, time.Now()
	c.tpwMu.Unlock()
	return filterProfit(out, minProfit, max)
}

func filterProfit(in []ProfitWallet, minProfit float64, max int) []ProfitWallet {
	out := make([]ProfitWallet, 0, len(in))
	for _, p := range in {
		if p.Profit >= minProfit {
			out = append(out, p)
		}
	}
	if max > 0 && len(out) > max {
		out = out[:max]
	}
	return out
}

// WalletActivity pulls a wallet's recent TRADE activity (newest first). start>0 limits to
// events at/after that unix-second timestamp (incremental pulls). Shares the data-api rate
// limiter so the per-wallet fanout never trips Polymarket's throttle. On-chain, public data.
// WalletActivity pulls a wallet's TRADE activity. start (>0) is a lower time bound for INCREMENTAL
// top-ups (only trades after the cursor); end (>0) is an upper time bound used to page BACKWARD
// through history (fetch trades older than `end`) so a wallet's full record can be back-filled.
func (c *Client) WalletActivity(ctx context.Context, wallet string, limit int, start, end int64) ([]Activity, error) {
	if limit <= 0 {
		limit = 200
	}
	if c.posLimiter != nil {
		if err := c.posLimiter.Wait(ctx); err != nil {
			return nil, err // R85 sweep: never fall through to an unthrottled venue call on a dead ctx
		}
	}
	u := dataBase + "/activity?type=TRADE&limit=" + strconv.Itoa(limit) + "&user=" + url.QueryEscape(wallet)
	if start > 0 {
		u += "&start=" + strconv.FormatInt(start, 10)
	}
	if end > 0 {
		u += "&end=" + strconv.FormatInt(end, 10)
	}
	var acts []Activity
	if err := c.get(ctx, u, &acts); err != nil {
		return nil, err
	}
	return acts, nil
}
