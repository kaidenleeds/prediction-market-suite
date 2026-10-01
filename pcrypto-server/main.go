// Command pcrypto-server is a SELF-CONTAINED, standalone runner for the proven Polymarket-crypto
// edge ("pcrypto"): Polymarket's BTC/ETH/SOL/XRP/DOGE Up/Down markets across durations
// (15m/1h/4h/1d), betting the mid-priced FAVORITE, cross-confirmed by Kalshi's matching 15-minute
// market. It paper-trades the signal, settles positions when the market resolves, scores each bet
// with a live CONFIDENCE (session win-rate × entry mid-price quality), and serves a tiny live
// dashboard. It depends on NOTHING from the main suite — pure stdlib — so you can copy this folder
// anywhere and `go run .` it.
//
// PAPER by default. The -live flag runs the identical engine on live data but does NOT place real
// orders (real Polymarket order entry needs wallet signing and is intentionally a logged placeholder);
// it prints the orders it WOULD place, so the same binary is ready to wire to a real executor.
//
//	go run .                 # paper, dashboard on http://127.0.0.1:8799
//	go run . -port 9000      # custom port
//	go run . -coins btc,eth -durations 15m,1h
//	go run . -min-conf 0.5   # only take bets above 0.50 confidence
//	go run . -live           # live data, order placement is a logged placeholder
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata" // embed the IANA tz database so ET slug labels work on any OS (incl. Windows)
)

const (
	gammaBase  = "https://gamma-api.polymarket.com"
	kalshiBase = "https://api.elections.kalshi.com/trade-api/v2"
)

var coinFull = map[string]string{"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "xrp", "doge": "dogecoin"}
var kalshi15mSeries = map[string]string{"btc": "KXBTC15M", "eth": "KXETH15M", "sol": "KXSOL15M", "xrp": "KXXRP15M", "doge": "KXDOGE15M"}

// ---- config (flags) ----

type Config struct {
	Port          string
	Coins         []string
	Durations     []string
	Interval      time.Duration
	Bankroll      float64
	StakePct      float64
	MinEntry      float64 // skip bets on the favored side priced below this (too close to a coinflip)
	MaxEntry      float64 // skip bets above this (extreme favorite — no edge, adverse payout)
	SoloMinEntry  float64 // SOLO (Kalshi-unconfirmed) bets must clear THIS favorite bar — a ~50¢ solo bet is a coinflip (max fees, no edge). 0 = no solo bar. Cross-confirmed bets still use MinEntry.
	MinConf       float64 // confidence gate: skip bets scoring below this (0 = take all)
	MakerShare    float64 // fee blend: fraction of fills modeled as maker ($0) vs taker
	CrossConfirmX float64 // size multiplier when Kalshi's 15m market agrees on direction
	Kelly         bool    // size by fractional Kelly (growth-optimal) instead of flat stake-pct
	KellyMult     float64 // fractional-Kelly multiplier (0.5 = half-Kelly; lower = safer, less variance)
	KellyCap      float64 // hard cap: max fraction of bankroll on any SINGLE bet
	MaxExposure   float64 // cap on TOTAL open cost basis as a fraction of bankroll (stops piling many bets at once)
	NoEdgeFloor   bool    // when a band shows no Kelly edge, bet a small flat floor instead of skipping (keeps it trading)
	Live          bool
	StatePath     string
}

// ---- Polymarket gamma market ----

// Market is the subset of Polymarket's gamma market object we need. outcomes/outcomePrices arrive
// as JSON-ENCODED strings, e.g. "[\"Up\",\"Down\"]" / "[\"0.54\",\"0.46\"]".
type Market struct {
	Question    string `json:"question"`
	ConditionID string `json:"conditionId"`
	Slug        string `json:"slug"`
	PricesRaw   string `json:"outcomePrices"`
	OutcomesRaw string `json:"outcomes"`
	TokensRaw   string `json:"clobTokenIds"` // JSON-encoded ["<upTokenId>","<downTokenId>"]
	EndDate     string `json:"endDate"`
	Closed      bool   `json:"closed"`
}

// upTokenID returns the CLOB token id for the "Up" (outcome-0) side — the id prices-history needs.
func (m Market) upTokenID() string {
	var ids []string
	if json.Unmarshal([]byte(m.TokensRaw), &ids) != nil || len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func (m Market) prices() []float64 {
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

func (m Market) outcomes() []string {
	var out []string
	_ = json.Unmarshal([]byte(m.OutcomesRaw), &out)
	return out
}

// upProb returns the "Up" probability (outcome-0 price), 0 if unavailable.
func (m Market) upProb() float64 {
	p := m.prices()
	if len(p) == 0 {
		return 0
	}
	return p[0]
}

// resolvedWinner returns the decisively-winning outcome label ("Up"/"Down") once one side prices at
// ~$1, else ("", false). Used to settle paper positions.
func (m Market) resolvedWinner() (string, bool) {
	outs, ps := m.outcomes(), m.prices()
	if len(outs) == 0 || len(outs) != len(ps) {
		return "", false
	}
	for i, p := range ps {
		if p >= 0.99 {
			return outs[i], true
		}
	}
	return "", false
}

// ---- HTTP helper ----

var httpClient = &http.Client{Timeout: 12 * time.Second}

func getJSON(ctx context.Context, urlStr string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	// A real-browser UA: Polymarket's Cloudflare intermittently challenges obvious bot UAs.
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("GET %s: status %d", urlStr, resp.StatusCode)
	}
	return json.Unmarshal(body, out)
}

// ---- Polymarket fetchers ----

func polyMarketBySlug(ctx context.Context, slug string) (Market, bool) {
	if slug == "" {
		return Market{}, false
	}
	var ms []Market
	u := gammaBase + "/markets?slug=" + url.QueryEscape(slug) + "&limit=1"
	if err := getJSON(ctx, u, &ms); err != nil || len(ms) == 0 {
		return Market{}, false
	}
	return ms[0], true
}

func polyMarketByCondition(ctx context.Context, condID string) (Market, bool) {
	if condID == "" {
		return Market{}, false
	}
	var ms []Market
	u := gammaBase + "/markets?condition_ids=" + url.QueryEscape(condID) + "&limit=1"
	if err := getJSON(ctx, u, &ms); err != nil || len(ms) == 0 {
		return Market{}, false
	}
	return ms[0], true
}

// etNow returns current time in US/Eastern (Polymarket's hourly/daily up/down slugs are ET-labeled).
func etNow() time.Time {
	if loc, err := time.LoadLocation("America/New_York"); err == nil {
		return time.Now().In(loc)
	}
	return time.Now().UTC().Add(-4 * time.Hour)
}

// cryptoUpDownMarket returns the CURRENT active Polymarket up/down market for a coin + duration.
// 5m/15m/4h use the timestamp slug `{coin}-updown-{dur}-{ts}`; hourly + daily use ET-labeled
// human-readable slugs. Tries the current window then ±1 so a boundary tick still resolves.
func cryptoUpDownMarket(ctx context.Context, coin, dur string) (Market, bool) {
	coin = strings.ToLower(strings.TrimSpace(coin))
	try := func(slug string) (Market, bool) {
		if m, ok := polyMarketBySlug(ctx, slug); ok && !m.Closed && m.ConditionID != "" {
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
		if base.Hour() < 12 { // daily window is noon-ET→noon-ET
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

// ---- Kalshi 15m cross-confirm ----

type kalshiMarket struct {
	Ticker        string `json:"ticker"`
	YesAskDollars string `json:"yes_ask_dollars"`
	LastDollars   string `json:"last_price_dollars"`
}

func parseDollars(s string) float64 {
	s = strings.Trim(strings.TrimSpace(s), `"`)
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// kalshi15mUp returns Kalshi's implied "Up" (YES) probability for a coin's 15-minute up/down market,
// read from the public market-data endpoint (no auth needed). ok=false → no live Kalshi market.
func kalshi15mUp(ctx context.Context, coin string) (float64, bool) {
	series := kalshi15mSeries[strings.ToLower(coin)]
	if series == "" {
		return 0, false
	}
	var resp struct {
		Markets []kalshiMarket `json:"markets"`
	}
	u := kalshiBase + "/markets?status=open&limit=1000&series_ticker=" + url.QueryEscape(series)
	if err := getJSON(ctx, u, &resp); err != nil || len(resp.Markets) == 0 {
		return 0, false
	}
	for _, m := range resp.Markets {
		if p := parseDollars(m.YesAskDollars); p > 0 {
			return p, true
		}
		if p := parseDollars(m.LastDollars); p > 0 {
			return p, true
		}
	}
	return 0, false
}

// ---- Polymarket CLOB: prices-history + exact fee rate ----

const clobBase = "https://clob.polymarket.com"

// pricesHistory returns a token's recent price path (oldest→newest), capped to the last `max` points.
// Best-effort: empty slice on any error. Used to draw a sparkline of where the market has been.
func pricesHistory(ctx context.Context, tokenID string, fidelity, max int) []float64 {
	if tokenID == "" {
		return nil
	}
	var resp struct {
		History []struct {
			P float64 `json:"p"`
		} `json:"history"`
	}
	u := fmt.Sprintf("%s/prices-history?market=%s&interval=1h&fidelity=%d", clobBase, url.QueryEscape(tokenID), fidelity)
	if err := getJSON(ctx, u, &resp); err != nil || len(resp.History) == 0 {
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

// clobMidpoint returns a token's LIVE order-book midpoint — the price Polymarket itself shows. This is the
// RELIABLE current price for marking open positions: gamma's market-object outcomePrices runs stale/empty
// for the fast crypto up/down markets (which is why marks were blank), but the CLOB book is always live.
// ok=false if the token has no book / no price.
func clobMidpoint(ctx context.Context, tokenID string) (float64, bool) {
	if tokenID == "" {
		return 0, false
	}
	var resp struct {
		Mid string `json:"mid"`
	}
	if err := getJSON(ctx, clobBase+"/midpoint?token_id="+url.QueryEscape(tokenID), &resp); err != nil {
		return 0, false
	}
	v, err := strconv.ParseFloat(resp.Mid, 64)
	if err != nil || v <= 0 || v >= 1 {
		return 0, false
	}
	return v, true
}

// clobResolve fetches a market's per-outcome prices from the CLOB /markets/<conditionId> endpoint — the
// ONE source that returns a market in EVERY state (active AND resolved). gamma /markets drops closed
// crypto markets (returns [] for them), which is why positions went unpriceable and piled up at "—".
// Returns outcome→price (0..1; for a resolved market the winner is 1, loser 0), whether it's closed, ok.
func clobResolve(ctx context.Context, condID string) (map[string]float64, bool, bool) {
	if condID == "" {
		return nil, false, false
	}
	var info struct {
		Closed bool `json:"closed"`
		Tokens []struct {
			Outcome string  `json:"outcome"`
			Price   float64 `json:"price"`
			Winner  bool    `json:"winner"`
		} `json:"tokens"`
	}
	if err := getJSON(ctx, clobBase+"/markets/"+url.QueryEscape(condID), &info); err != nil {
		return nil, false, false
	}
	out := map[string]float64{}
	for _, t := range info.Tokens {
		if t.Outcome == "" {
			continue
		}
		p := t.Price
		if info.Closed { // resolved → use the decisive 0/1 from the winner flag (price can lag)
			if t.Winner {
				p = 1
			} else {
				p = 0
			}
		}
		out[t.Outcome] = p
	}
	if len(out) == 0 {
		return nil, info.Closed, false
	}
	return out, info.Closed, true
}

// clobFeeRate returns a market's EXACT taker fee coefficient from getClobMarketInfo (fd.r, else
// tbf basis-points/10000). ok=false → fall back to the static crypto rate (0.07).
func clobFeeRate(ctx context.Context, condID string) (float64, bool) {
	if condID == "" {
		return 0, false
	}
	var info struct {
		Tbf int `json:"tbf"`
		Fd  struct {
			R *float64 `json:"r"`
		} `json:"fd"`
	}
	if err := getJSON(ctx, clobBase+"/clob-markets/"+url.QueryEscape(condID), &info); err != nil {
		return 0, false
	}
	if info.Fd.R != nil && *info.Fd.R > 0 {
		return *info.Fd.R, true
	}
	if info.Tbf > 0 {
		return float64(info.Tbf) / 10000, true
	}
	return 0, false
}

// ---- paper book ----

type Position struct {
	Coin       string    `json:"coin"`
	Dur        string    `json:"dur"`
	Side       string    `json:"side"`
	Slug       string    `json:"slug"`
	CondID     string    `json:"cond_id"`
	Question   string    `json:"question"`
	Entry      float64   `json:"entry"`
	Contracts  float64   `json:"contracts"`
	Fee        float64   `json:"fee"`
	Confidence float64   `json:"confidence"`
	Confirmed  bool      `json:"confirmed"` // Kalshi 15m agreed → 2x size
	TokenID    string    `json:"token_id"`  // CLOB Up-token id (for prices-history)
	Path       []float64 `json:"path"`      // recent price path (sparkline), refreshed each tick
	OpenedAt   time.Time `json:"opened_at"`
	LastUp     float64   `json:"last_up"`   // most recent observed "Up" price — lets us settle even if the micro-market is purged before it formally resolves
	LastSeen   time.Time `json:"last_seen"` // when LastUp was captured
}

type Closed struct {
	Coin       string    `json:"coin"`
	Dur        string    `json:"dur"`
	Side       string    `json:"side"`
	Question   string    `json:"question"`
	Entry      float64   `json:"entry"`
	Contracts  float64   `json:"contracts"`
	Fee        float64   `json:"fee"`
	Pnl        float64   `json:"pnl"`
	Confidence float64   `json:"confidence"`
	Won        bool      `json:"won"`
	OpenedAt   time.Time `json:"opened_at"`
	ClosedAt   time.Time `json:"closed_at"`
}

// Book is the in-memory paper ledger (persisted to disk between runs).
type Book struct {
	mu        sync.Mutex
	Open      []*Position `json:"open"`
	History   []Closed    `json:"history"`
	Realized  float64     `json:"realized"`
	Fees      float64     `json:"fees"`
	StartedAt time.Time   `json:"started_at"`
}

func (b *Book) hasOpen(condID string) bool {
	for _, p := range b.Open {
		if p.CondID == condID {
			return true
		}
	}
	return false
}

// winStats returns the session win-rate (0–1) and the number of settled trades. Lock-safe: it takes
// the book mutex itself, so callers must NOT already hold it.
func (b *Book) winStats() (float64, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(b.History)
	if n == 0 {
		return 0, 0
	}
	wins := 0
	for _, c := range b.History {
		if c.Won {
			wins++
		}
	}
	return float64(wins) / float64(n), n
}

// winProbNear returns the win rate (and sample count) of past settled trades entered NEAR a given
// price (±0.08). It's the empirical p(win | price) that Kelly sizing needs — so the engine sizes by
// the edge actually observed at THIS price band, not by an inflated all-bets average. Lock-safe.
func (b *Book) winProbNear(price float64) (float64, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var wins, n int
	for _, c := range b.History {
		if math.Abs(c.Entry-price) <= 0.08 {
			n++
			if c.Won {
				wins++
			}
		}
	}
	if n == 0 {
		return 0, 0
	}
	return float64(wins) / float64(n), n
}

// openCost returns the total cost basis ($) of all open paper positions. Used to cap TOTAL exposure
// so the engine can't deploy far more than the bankroll across many simultaneous bets. Lock-safe.
func (b *Book) openCost() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	var c float64
	for _, p := range b.Open {
		c += p.Entry * p.Contracts
	}
	return c
}

// ---- confidence + fees (mirror of the main suite) ----

// confidence scores a candidate bet in [0,1] = session win-rate (0.55 for <8 settled — benefit of the
// doubt) × a mid-price quality factor (1.0 in the 0.45–0.65 pocket, tapering to 0.2 at the extremes).
func confidence(winRate float64, settled int, price float64) float64 {
	wrq := 0.55
	if settled >= 8 {
		wrq = winRate
	}
	d := math.Abs(price - 0.55)
	pq := 1.0
	if d > 0.10 {
		pq = 1.0 - (d-0.10)/0.35
	}
	pq = math.Max(0.2, math.Min(1.0, pq))
	return wrq * pq
}

// polyFee models Polymarket's taker fee (docs.polymarket.com/trading/fees, 2026): maker $0, taker =
// C·rate·p·(1−p), rounded to 5 decimals (USDC). `rate` is the market's exact coefficient when we can
// read it (getClobMarketInfo), else the crypto default 0.07. The blend charges only the taker portion.
func polyFee(rate, makerShare, contracts, price float64) float64 {
	if rate <= 0 {
		rate = 0.07
	}
	taker := math.Round(rate*contracts*price*(1-price)*1e5) / 1e5
	return (1 - makerShare) * taker
}

// ---- application ----

type App struct {
	cfg        Config
	cfgMu      sync.RWMutex // guards cfg so the Settings panel can tune it live without racing the tick loop
	book       *Book
	log        []string  // recent activity lines (newest last), capped — served to the dashboard
	logM       sync.Mutex
	logw       io.Writer // console + persistent log file (set in main); nil → stdout only
	pollCursor int       // round-robin start index for settle() so a big open book is polled a few per tick (tick goroutine only)
	pathCursor int       // round-robin start index for refreshPaths() — same idea for the cosmetic sparkline fetches
}

// getCfg returns a race-safe snapshot of the current config (read by the tick loop + handlers).
func (a *App) getCfg() Config {
	a.cfgMu.RLock()
	defer a.cfgMu.RUnlock()
	return a.cfg
}

func (a *App) logf(format string, args ...any) {
	now := time.Now()
	msg := fmt.Sprintf(format, args...)
	// Persistent log (console + file) gets a full datetime so a multi-day run is readable on disk.
	if a.logw != nil {
		fmt.Fprintln(a.logw, now.Format("2006-01-02 15:04:05")+"  "+msg)
	} else {
		fmt.Println(now.Format("15:04:05") + "  " + msg)
	}
	short := now.Format("15:04:05") + "  " + msg // dashboard activity feed (time only, as before)
	a.logM.Lock()
	a.log = append(a.log, short)
	if len(a.log) > 200 {
		a.log = a.log[len(a.log)-200:]
	}
	a.logM.Unlock()
}

// durWindow is the expected time-to-resolution for a duration label — used to skip polling a position
// that can't have resolved yet (a 3-min-old 15m market) and to detect stale/stuck ones.
func durWindow(dur string) time.Duration {
	switch dur {
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "1h":
		return time.Hour
	case "4h":
		return 4 * time.Hour
	case "1d":
		return 24 * time.Hour
	}
	return time.Hour
}

// settle closes resolved positions via the CLOB /markets endpoint (clobResolve) — the ONE source that
// returns RESOLVED markets (gamma hides them, which is why hundreds of expired positions piled up open and
// unpriceable). Concurrent + a generous per-tick cap so the backlog actually drains. Falls back to the last
// observed price, and only force-closes a never-observed, very-stale leg. PAPER only.
func (a *App) settle(ctx context.Context) {
	a.book.mu.Lock()
	open := append([]*Position(nil), a.book.Open...)
	a.book.mu.Unlock()
	if len(open) == 0 {
		return
	}
	type ripeP struct {
		p        *Position
		age, win time.Duration
	}
	var ripe []ripeP
	for _, p := range open {
		if win := durWindow(p.Dur); time.Since(p.OpenedAt) >= win {
			ripe = append(ripe, ripeP{p, time.Since(p.OpenedAt), win})
		}
	}
	if len(ripe) == 0 {
		return
	}
	sort.Slice(ripe, func(i, j int) bool { return ripe[i].age > ripe[j].age }) // most-overdue first
	const maxPoll = 80 // drain aggressively — CLOB is a separate host + one light call each
	if len(ripe) > maxPoll {
		ripe = ripe[:maxPoll]
	}
	settled := map[*Position]bool{}
	var smu sync.Mutex
	const workers = 8
	jobs := make(chan ripeP, len(ripe))
	for _, rp := range ripe {
		jobs <- rp
	}
	close(jobs)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rp := range jobs {
				p, age, win := rp.p, rp.age, rp.win
				pctx, cancel := context.WithTimeout(ctx, 6*time.Second)
				prices, closed, ok := clobResolve(pctx, p.CondID)
				cancel()
				var winner string
				if ok && !closed {
					if up, h := prices["Up"]; h { // still active → refresh LastUp, not resolved yet
						a.book.mu.Lock()
						p.LastUp, p.LastSeen = up, time.Now()
						a.book.mu.Unlock()
					}
					continue
				}
				if ok && closed {
					for out, pr := range prices { // resolved → the outcome priced 1 is the winner
						if pr >= 0.99 {
							winner = out
							break
						}
					}
					if winner == "" {
						continue // closed but not decisively priced yet → retry next tick
					}
				} else {
					// clobResolve failed. AUDIT F3/§5: the old fallback graded the bet off the LAST
					// OBSERVED PRICE (≥0.5 ⇒ "Up" wins) — self-referential settlement that inflated the
					// book — and the FORCE-CLOSE silently DROPPED unresolvable legs with zero P&L
					// (survivorship: ~25% of an era's legs vanished). Now: no price-print grading, ever —
					// keep retrying CLOB for a real resolution; only a VERY stale leg closes, and it
					// books as a LOSS of its cost basis (conservative), never a silent scratch.
					if age > 4*win+30*time.Minute {
						winner = "" // no venue resolution: won=false below → pnl = −cost basis
						a.logf("STALE-CLOSE %s %s %s — venue never resolved; booked as LOSS (was: silent drop)", strings.ToUpper(p.Coin), p.Dur, p.Side)
					} else {
						continue // retry CLOB next tick until the venue actually resolves it
					}
				}
				won := strings.EqualFold(winner, p.Side)
				var pnl float64 // GROSS realized; fees tracked separately so NET = Realized − Fees
				if won {
					pnl = p.Contracts * (1 - p.Entry)
				} else {
					pnl = -p.Contracts * p.Entry
				}
				a.book.mu.Lock()
				a.book.History = append(a.book.History, Closed{
					Coin: p.Coin, Dur: p.Dur, Side: p.Side, Question: p.Question,
					Entry: p.Entry, Contracts: p.Contracts, Fee: p.Fee, Pnl: pnl, Confidence: p.Confidence,
					Won: won, OpenedAt: p.OpenedAt, ClosedAt: time.Now(),
				})
				a.book.Realized += pnl
				a.book.Fees += p.Fee
				a.book.mu.Unlock()
				smu.Lock()
				settled[p] = true
				smu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(settled) > 0 {
		a.book.mu.Lock()
		var stillOpen []*Position
		for _, p := range a.book.Open {
			if !settled[p] {
				stillOpen = append(stillOpen, p)
			}
		}
		a.book.Open = stillOpen
		a.book.mu.Unlock()
		a.logf("settled %d positions this pass (CLOB)", len(settled))
	}
}

// scan finds pcrypto signals across every coin × duration, RANKS them by EV, and funds the highest-EV
// (and cross-confirmed) bets FIRST — so the exposure cap can never starve the best signals by spending
// the room on mediocre first-in-loop ones (MINIBEST). Max-EV allocation: best edges get capital, the
// rest wait for settlements to free room.
func (a *App) scan(ctx context.Context) {
	cfg := a.getCfg() // race-safe snapshot of the live-editable tunables
	if a.book.openCost() >= cfg.MaxExposure*cfg.Bankroll {
		return // fully deployed — don't hit the APIs until settlements free room (stops API hammering + stalls)
	}
	wr, nset := a.book.winStats()
	// Phase 1 — gather every candidate with an EV estimate (no betting yet).
	type cand struct {
		coin, dur, side    string
		m                  Market
		price, conf, ev, f float64
		mult               float64
		confirmed, measured bool
	}
	var cands []cand
	for _, dur := range cfg.Durations {
		for _, coin := range cfg.Coins {
			m, ok := cryptoUpDownMarket(ctx, coin, dur)
			if !ok || m.ConditionID == "" {
				continue
			}
			up := m.upProb()
			if up <= 0.05 || up >= 0.95 {
				continue // no edge / too extreme
			}
			side, price := "Up", up
			if up < 0.5 {
				side, price = "Down", 1-up
			}
			if price < cfg.MinEntry || price > cfg.MaxEntry {
				continue // outside the mid-favorite pocket where the edge lives
			}
			a.book.mu.Lock()
			dup := a.book.hasOpen(m.ConditionID)
			a.book.mu.Unlock()
			if dup {
				continue // already hold this market
			}
			// CROSS-CONFIRM: only 15m has a matching Kalshi market. Agreement → upsize + rank up.
			mult, confirmed := 1.0, false
			if dur == "15m" {
				if kp, ok := kalshi15mUp(ctx, coin); ok && ((side == "Up" && kp >= 0.5) || (side == "Down" && kp < 0.5)) {
					mult, confirmed = cfg.CrossConfirmX, true
				}
			}
			// SOLO BAR: an UNCONFIRMED bet near 50¢ is a coinflip — max fees (fee ∝ p(1−p) peaks at 0.5),
			// no real edge. That's why the mini bled while the suite's pcrypto wins: the suite only takes
			// solo bets on a STRONG favorite and otherwise requires Kalshi confirmation. Mirror that here —
			// solo bets must clear a favorite bar; cross-confirmed bets can stay at MinEntry (both venues agree).
			if !confirmed && cfg.SoloMinEntry > 0 && price < cfg.SoloMinEntry {
				continue
			}
			conf := confidence(wr, nset, price)
			if cfg.MinConf > 0 && conf < cfg.MinConf {
				continue // below the confidence gate
			}
			// EV/contract from the measured win-prob near this price (shrunk toward the market price so a thin
			// band can't fake an edge) + the Kelly fraction. Cross-confirmed bets carry their own +EV edge, so
			// nudge their rank EV up to fund before an equal solo bet.
			ev, f, measured := 0.0, 0.0, false
			if w, ns := a.book.winProbNear(price); ns >= 6 {
				measured = true
				p := (float64(ns)*w + 6.0*price) / (float64(ns) + 6.0)
				ev = p - price
				f = ev / (1 - price) * cfg.KellyMult
				if confirmed {
					f *= cfg.CrossConfirmX
				}
				if f > cfg.KellyCap {
					f = cfg.KellyCap
				}
			}
			rankEV := ev
			if confirmed {
				rankEV += 0.01
			}
			cands = append(cands, cand{coin, dur, side, m, price, conf, rankEV, f, mult, confirmed, measured})
		}
	}
	// Phase 2 — rank: cross-confirmed first, then highest EV/contract.
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].confirmed != cands[j].confirmed {
			return cands[i].confirmed
		}
		return cands[i].ev > cands[j].ev
	})
	// Phase 3 — fund best-first until the exposure cap is hit (lower-EV ones wait for room to free up).
	for _, c := range cands {
		room := cfg.MaxExposure*cfg.Bankroll - a.book.openCost()
		if room <= 0 {
			break
		}
		var stake float64
		if cfg.Kelly {
			if c.f > 0 {
				stake = cfg.Bankroll * c.f // measured edge → size UP by Kelly
			} else if c.measured {
				if !cfg.NoEdgeFloor {
					continue // measured but no edge → skip (strict growth-optimal)
				}
				stake = cfg.Bankroll * cfg.StakePct * c.mult * 0.5 // half flat to keep gathering data
			} else {
				stake = cfg.Bankroll * cfg.StakePct * c.mult // cold-start band → flat stake to gather data
			}
		} else {
			stake = cfg.Bankroll * cfg.StakePct * c.mult
		}
		if stake > room {
			stake = room
		}
		contracts := math.Floor(stake / c.price)
		if contracts < 1 {
			continue
		}
		rate, _ := clobFeeRate(ctx, c.m.ConditionID) // exact per-market rate; 0 → polyFee falls back to 0.07
		fee := polyFee(rate, cfg.MakerShare, contracts, c.price)
		if cfg.Live {
			a.logf("[LIVE] would place: BUY %s %s %s x%.0f @%.2f (placeholder — no real order)", strings.ToUpper(c.coin), c.dur, c.side, contracts, c.price)
		}
		pos := &Position{
			Coin: c.coin, Dur: c.dur, Side: c.side, Slug: c.m.Slug, CondID: c.m.ConditionID, Question: c.m.Question,
			Entry: c.price, Contracts: contracts, Fee: fee, Confidence: c.conf, Confirmed: c.confirmed,
			TokenID: c.m.upTokenID(), OpenedAt: time.Now(),
			LastUp: c.m.upProb(), LastSeen: time.Now(),
		}
		a.book.mu.Lock()
		a.book.Open = append(a.book.Open, pos)
		a.book.mu.Unlock()
		tag := "solo"
		if c.confirmed {
			tag = "Kalshi-confirmed 2x"
		}
		a.logf("OPEN  %s %s %s x%.0f @%.2f  conf %.2f  EV %.3f  (%s)", strings.ToUpper(c.coin), c.dur, c.side, contracts, c.price, c.conf, c.ev, tag)
	}
}

func (a *App) tick(ctx context.Context) {
	a.settle(ctx)
	a.scan(ctx)
	a.refreshPaths(ctx)
	a.save()
}

// polyMarketsByConditions batches MANY conditionIds into ONE gamma call (≤20 per request) — the SUITE'S
// proven price source (the suite's markPoly uses exactly this and prices Poly fine). One request for the
// whole open book instead of 40 separate ones → no rate trouble, reliable. Returns conditionId → Market.
func polyMarketsByConditions(ctx context.Context, ids []string) map[string]Market {
	out := map[string]Market{}
	seen := map[string]bool{}
	uniq := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != "" && !seen[id] {
			seen[id] = true
			uniq = append(uniq, id)
		}
	}
	for i := 0; i < len(uniq); i += 20 {
		end := i + 20
		if end > len(uniq) {
			end = len(uniq)
		}
		u := gammaBase + "/markets?limit=" + strconv.Itoa(end-i)
		for _, id := range uniq[i:end] {
			u += "&condition_ids=" + url.QueryEscape(id)
		}
		var ms []Market
		if err := getJSON(ctx, u, &ms); err != nil {
			continue
		}
		for _, m := range ms {
			if m.ConditionID != "" {
				out[m.ConditionID] = m
			}
		}
	}
	return out
}

// refreshPaths MARKS every open position each tick via the CLOB /markets/<conditionId> endpoint — the ONE
// source that prices a market in EVERY state (live AND resolved). gamma drops expired crypto markets (it
// returns [] for them), which is why older positions went unpriceable and piled up at "—". clobResolve
// gives the live price while active and the decisive 0/1 once resolved — which also lets settle() drain the
// backlog through its last-price path. Worker pool bounds concurrency; logs marked/total so gaps are visible.
func (a *App) refreshPaths(ctx context.Context) {
	a.book.mu.Lock()
	open := append([]*Position(nil), a.book.Open...)
	a.book.mu.Unlock()
	if len(open) == 0 {
		return
	}
	const workers = 8
	jobs := make(chan *Position, len(open))
	for _, p := range open {
		jobs <- p
	}
	close(jobs)
	var wg sync.WaitGroup
	marked := 0
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for p := range jobs {
				pctx, cancel := context.WithTimeout(ctx, 6*time.Second)
				prices, _, ok := clobResolve(pctx, p.CondID)
				if !ok && p.TokenID != "" { // last-ditch fallback: live order-book midpoint of the up-token
					if u, o := clobMidpoint(pctx, p.TokenID); o {
						prices, ok = map[string]float64{"Up": u, "Down": 1 - u}, true
					}
				}
				cancel()
				if !ok {
					continue
				}
				cs, hasSide := prices[p.Side] // CLOB gives each outcome's price directly (Up / Down)
				if !hasSide {
					continue
				}
				up := prices["Up"] // LastUp drives settle()'s last-price path
				a.book.mu.Lock()
				p.LastUp, p.LastSeen = up, time.Now()
				p.Path = append(p.Path, cs)
				if len(p.Path) > 40 {
					p.Path = p.Path[len(p.Path)-40:]
				}
				marked++
				a.book.mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if marked < len(open) {
		a.logf("mark: %d/%d open positions priced this pass (CLOB /markets)", marked, len(open))
	}
}

// ---- persistence ----

func (a *App) save() {
	if a.cfg.StatePath == "" {
		return
	}
	a.book.mu.Lock()
	b, err := json.MarshalIndent(a.book, "", "  ")
	a.book.mu.Unlock()
	if err == nil {
		_ = os.WriteFile(a.cfg.StatePath, b, 0o644)
	}
}

func (a *App) load() {
	if a.cfg.StatePath == "" {
		return
	}
	b, err := os.ReadFile(a.cfg.StatePath)
	if err != nil {
		return
	}
	var bk Book
	if json.Unmarshal(b, &bk) == nil {
		a.book.Open, a.book.History = bk.Open, bk.History
		a.book.Realized, a.book.Fees = bk.Realized, bk.Fees
		if !bk.StartedAt.IsZero() {
			a.book.StartedAt = bk.StartedAt
		}
	}
}

// ---- HTTP dashboard ----

func (a *App) handleState(w http.ResponseWriter, _ *http.Request) {
	a.book.mu.Lock()
	open := append([]*Position(nil), a.book.Open...)
	hist := append([]Closed(nil), a.book.History...)
	realized, fees := a.book.Realized, a.book.Fees
	started := a.book.StartedAt
	a.book.mu.Unlock()
	wr, n := a.book.winStats()
	// newest history first, cap to 100 for the page
	sort.Slice(hist, func(i, j int) bool { return hist[i].ClosedAt.After(hist[j].ClosedAt) })
	if len(hist) > 100 {
		hist = hist[:100]
	}
	a.logM.Lock()
	logs := append([]string(nil), a.log...)
	a.logM.Unlock()
	for i, j := 0, len(logs)-1; i < j; i, j = i+1, j-1 {
		logs[i], logs[j] = logs[j], logs[i] // newest first
	}
	if len(logs) > 60 {
		logs = logs[:60]
	}
	cfg := a.getCfg()
	// Portfolio marks: current side-price → unrealized P&L + open cost basis → live equity (like the main book).
	unreal, openCost := 0.0, 0.0
	for _, p := range open {
		openCost += p.Contracts * p.Entry
		if p.LastUp > 0 {
			cs := p.LastUp
			if p.Side == "Down" {
				cs = 1 - p.LastUp
			}
			unreal += p.Contracts * (cs - p.Entry)
		}
	}
	equity := cfg.Bankroll + realized - fees + unreal
	writeJSON(w, map[string]any{
		"mode":      map[bool]string{true: "LIVE (placeholder orders)", false: "PAPER"}[cfg.Live],
		"coins":     cfg.Coins,
		"durations": cfg.Durations,
		"bankroll":  cfg.Bankroll,
		"stake_pct": cfg.StakePct,
		"min_conf":   cfg.MinConf,
		"sizing":     map[bool]string{true: "kelly", false: "fixed"}[cfg.Kelly],
		"kelly_mult":   cfg.KellyMult,
		"kelly_cap":    cfg.KellyCap,
		"max_exposure": cfg.MaxExposure,
		"min_entry":    cfg.MinEntry,
		"max_entry":    cfg.MaxEntry,
		"started":   started,
		"win_rate":  wr,
		"settled":   n,
		"realized":  realized,
		"fees":      fees,
		"net":       realized - fees,
		"unrealized": unreal,
		"open_cost":  openCost,
		"equity":     equity,
		"open":      open,
		"history":   hist,
		"log":       logs,
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

// handleBacktest replays the closed-trade history compounding a $1,000 bankroll at several Kelly
// fractions (flat 2% / ¼ / ½ / full + optional ?kelly=N) so you can see which sizing would have grown
// most. Per trade, p = the win rate of past trades near its entry price (in-sample, sequential bets).
func (a *App) handleBacktest(w http.ResponseWriter, r *http.Request) {
	a.book.mu.Lock()
	hist := append([]Closed(nil), a.book.History...) // appended at settle time → oldest-first
	a.book.mu.Unlock()
	cfg := a.getCfg()
	const start = 1000.0
	const capFrac = 0.25 // sane per-bet cap for the sim
	// run sizes each settled trade by a Kelly fraction of a FIXED $1,000 base (NON-compounding — bets
	// overlap, so you can't reinvest each one into the next), using a WALK-FORWARD band win rate (only
	// trades entered near this price BEFORE it — no lookahead). Cumulative equity, return ×, max drawdown.
	run := func(kf float64) (final, growth, maxDD float64, bets int) {
		equity, peak := start, start
		for i := range hist {
			price := hist[i].Entry
			var frac float64
			if kf == 0 {
				frac = 0.02 // flat baseline: 2% of the $1,000 base
			} else {
				var wins, n int
				for j := 0; j < i; j++ {
					if math.Abs(hist[j].Entry-price) <= 0.08 {
						n++
						if hist[j].Won {
							wins++
						}
					}
				}
				if n >= 6 {
					p := (float64(n)*(float64(wins)/float64(n)) + 6.0*price) / (float64(n) + 6.0)
					if fstar := (p - price) / (1 - price); fstar > 0 {
						frac = kf * fstar
						if frac > capFrac {
							frac = capFrac
						}
					}
				}
			}
			if price <= 0.02 || price >= 0.98 || frac <= 0 {
				continue
			}
			contracts := math.Floor(start * frac / price) // fixed base — non-compounding
			if contracts < 1 {
				continue
			}
			fee := polyFee(0, cfg.MakerShare, contracts, price)
			pnl := -contracts*price - fee
			if hist[i].Won {
				pnl = contracts*(1-price) - fee
			}
			equity += pnl
			bets++
			if equity > peak {
				peak = equity
			}
			if peak > 0 {
				if dd := (peak - equity) / peak; dd > maxDD {
					maxDD = dd
				}
			}
		}
		return equity, equity / start, maxDD, bets
	}
	kc := 0.0
	if q := r.URL.Query().Get("kelly"); q != "" {
		if v, e := strconv.ParseFloat(q, 64); e == nil && v > 0 {
			kc = v
		}
	}
	type row struct {
		Label  string  `json:"label"`
		Final  float64 `json:"final"`
		Growth float64 `json:"growth"`
		MaxDD  float64 `json:"max_dd"`
		Bets   int     `json:"bets"`
	}
	fracs := []struct {
		lab string
		f   float64
	}{{"flat 2%", 0}, {"¼-Kelly", 0.25}, {"½-Kelly", 0.5}, {"full Kelly", 1.0}}
	if kc > 0 {
		fracs = append(fracs, struct {
			lab string
			f   float64
		}{fmt.Sprintf("%.2g× Kelly", kc), kc})
	}
	out := []row{}
	for _, fr := range fracs {
		fb, g, dd, n := run(fr.f)
		out = append(out, row{Label: fr.lab, Final: fb, Growth: g, MaxDD: dd, Bets: n})
	}
	writeJSON(w, map[string]any{"start": start, "settled": len(hist), "sweep": out,
		"note": "cumulative P&L sizing each trade as a Kelly fraction of a fixed $1,000 (non-compounding, walk-forward band win-rate, 25% cap)."})
}

// handleSettings GETs the live tunables and POSTs updates (live — no restart). Not persisted across
// restarts; set permanent defaults via flags / run.bat.
// settingsFile is where the dashboard-editable tunables PERSIST so changes survive a restart.
const settingsFile = "pcrypto-settings.json"

// tunables is the persistable subset of Config (the dashboard knobs). Run-flags like port/coins/interval
// are NOT saved. Written on every settings POST, loaded at startup OVER the flag defaults.
type tunables struct {
	Bankroll      float64 `json:"bankroll"`
	StakePct      float64 `json:"stake_pct"`
	MinEntry      float64 `json:"min_entry"`
	MaxEntry      float64 `json:"max_entry"`
	SoloMinEntry  float64 `json:"solo_min_entry"`
	MinConf       float64 `json:"min_conf"`
	MakerShare    float64 `json:"maker_share"`
	CrossConfirmX float64 `json:"cross_confirm_x"`
	Kelly         bool    `json:"kelly"`
	KellyMult     float64 `json:"kelly_mult"`
	KellyCap      float64 `json:"kelly_cap"`
	MaxExposure   float64 `json:"max_exposure"`
	NoEdgeFloor   bool    `json:"no_edge_floor"`
	Saved         bool    `json:"saved"` // marker: a real save (so loadCfg ignores an empty/zero file)
}

// saveCfg writes the live tunables to disk so dashboard edits survive a restart.
func (a *App) saveCfg() {
	a.cfgMu.Lock()
	c := a.cfg
	a.cfgMu.Unlock()
	t := tunables{c.Bankroll, c.StakePct, c.MinEntry, c.MaxEntry, c.SoloMinEntry, c.MinConf, c.MakerShare, c.CrossConfirmX, c.Kelly, c.KellyMult, c.KellyCap, c.MaxExposure, c.NoEdgeFloor, true}
	if b, err := json.MarshalIndent(t, "", "  "); err == nil {
		_ = os.WriteFile(settingsFile, b, 0o644)
	}
}

// loadCfg applies a previously-saved settings file OVER the flag defaults at startup.
func (a *App) loadCfg() {
	b, err := os.ReadFile(settingsFile)
	if err != nil {
		return
	}
	var t tunables
	if json.Unmarshal(b, &t) != nil || !t.Saved {
		return
	}
	a.cfgMu.Lock()
	defer a.cfgMu.Unlock()
	if t.Bankroll > 0 {
		a.cfg.Bankroll = t.Bankroll
	}
	if t.StakePct >= 0 {
		a.cfg.StakePct = t.StakePct
	}
	a.cfg.MinEntry = t.MinEntry
	a.cfg.MaxEntry = t.MaxEntry
	a.cfg.SoloMinEntry = t.SoloMinEntry
	a.cfg.MinConf = t.MinConf
	if t.MakerShare >= 0 {
		a.cfg.MakerShare = t.MakerShare
	}
	if t.CrossConfirmX > 0 {
		a.cfg.CrossConfirmX = t.CrossConfirmX
	}
	a.cfg.Kelly = t.Kelly
	if t.KellyMult >= 0 {
		a.cfg.KellyMult = t.KellyMult
	}
	if t.KellyCap > 0 {
		a.cfg.KellyCap = t.KellyCap
	}
	if t.MaxExposure > 0 {
		a.cfg.MaxExposure = t.MaxExposure
	}
	a.cfg.NoEdgeFloor = t.NoEdgeFloor
	a.logf("settings loaded from %s (persisted dashboard tunables)", settingsFile)
}

func (a *App) handleSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var in struct {
			Bankroll    *float64 `json:"bankroll"`
			StakePct    *float64 `json:"stake_pct"`
			MinEntry      *float64 `json:"min_entry"`
			MaxEntry      *float64 `json:"max_entry"`
			SoloMinEntry  *float64 `json:"solo_min_entry"`
			CrossConfirmX *float64 `json:"cross_confirm_x"`
			MinConf       *float64 `json:"min_conf"`
			Kelly       *bool    `json:"kelly"`
			KellyMult   *float64 `json:"kelly_mult"`
			KellyCap    *float64 `json:"kelly_cap"`
			MaxExposure *float64 `json:"max_exposure"`
			NoEdgeFloor *bool    `json:"no_edge_floor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			writeJSON(w, map[string]any{"error": "invalid JSON"})
			return
		}
		a.cfgMu.Lock()
		if in.Bankroll != nil && *in.Bankroll > 0 {
			a.cfg.Bankroll = *in.Bankroll
		}
		if in.StakePct != nil && *in.StakePct >= 0 {
			a.cfg.StakePct = *in.StakePct
		}
		if in.MinEntry != nil {
			a.cfg.MinEntry = *in.MinEntry
		}
		if in.MaxEntry != nil {
			a.cfg.MaxEntry = *in.MaxEntry
		}
		if in.SoloMinEntry != nil {
			a.cfg.SoloMinEntry = *in.SoloMinEntry
		}
		if in.CrossConfirmX != nil && *in.CrossConfirmX > 0 {
			a.cfg.CrossConfirmX = *in.CrossConfirmX
		}
		if in.MinConf != nil {
			a.cfg.MinConf = *in.MinConf
		}
		if in.Kelly != nil {
			a.cfg.Kelly = *in.Kelly
		}
		if in.KellyMult != nil && *in.KellyMult >= 0 {
			a.cfg.KellyMult = *in.KellyMult
		}
		if in.KellyCap != nil && *in.KellyCap > 0 {
			a.cfg.KellyCap = *in.KellyCap
		}
		if in.MaxExposure != nil && *in.MaxExposure > 0 {
			a.cfg.MaxExposure = *in.MaxExposure
		}
		if in.NoEdgeFloor != nil {
			a.cfg.NoEdgeFloor = *in.NoEdgeFloor
		}
		a.cfgMu.Unlock()
		a.saveCfg() // SETTINGS PERSISTENCE: write tunables to disk so they survive a restart
		a.logf("settings updated via dashboard (saved to %s)", settingsFile)
	}
	cfg := a.getCfg()
	writeJSON(w, map[string]any{
		"bankroll": cfg.Bankroll, "stake_pct": cfg.StakePct, "min_entry": cfg.MinEntry, "max_entry": cfg.MaxEntry,
		"solo_min_entry": cfg.SoloMinEntry, "cross_confirm_x": cfg.CrossConfirmX,
		"min_conf": cfg.MinConf, "kelly": cfg.Kelly, "kelly_mult": cfg.KellyMult, "kelly_cap": cfg.KellyCap,
		"max_exposure": cfg.MaxExposure, "no_edge_floor": cfg.NoEdgeFloor,
	})
}

func (a *App) serve(openWindow bool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/state", a.handleState)
	mux.HandleFunc("/api/backtest", a.handleBacktest)
	mux.HandleFunc("/api/settings", a.handleSettings)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, dashboardHTML)
	})
	go func() {
		if err := http.ListenAndServe(a.cfg.Port, mux); err != nil {
			a.logf("http server error: %v", err)
		}
	}()
	if openWindow {
		go openAppWindow("http://" + a.cfg.Port) // interactive standalone launch only
	}
}

// openBrowser best-effort opens the dashboard URL in the default browser (so launching the mini-server
// pops its portfolio tab). Waits briefly for the listener to bind; no-op on failure.
func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

// openAppWindow opens url in a NORMAL Google Chrome window (chrome --new-window), per preference (not
// the chromeless --app window). A normal tab can't force-close itself, so on stop the heartbeat shows
// a "stopped" banner instead. Falls back to the default browser if Chrome isn't found. Waits briefly
// for the listener to bind first.
func openAppWindow(url string) {
	time.Sleep(900 * time.Millisecond)
	if runtime.GOOS == "windows" {
		if chrome := chromeExe(); chrome != "" {
			// NO --new-window: open as a TAB in the suite's existing Chrome window (one window, two tabs).
			if err := exec.Command(chrome, url).Start(); err == nil {
				return
			}
		}
	}
	openBrowser(url)
}

// chromeExe returns the Google Chrome executable path on Windows, or "" if not found.
func chromeExe() string {
	if runtime.GOOS != "windows" {
		return ""
	}
	for _, env := range []string{"ProgramFiles", "ProgramFiles(x86)", "LocalAppData"} {
		if b := os.Getenv(env); b != "" {
			p := filepath.Join(b, `Google\Chrome\Application\chrome.exe`)
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}
	return ""
}

// ---- console summary ----

func (a *App) printSummary() {
	a.book.mu.Lock()
	open := len(a.book.Open)
	realized, fees := a.book.Realized, a.book.Fees
	a.book.mu.Unlock()
	wr, n := a.book.winStats()
	fmt.Printf("---- pcrypto %s | open %d | settled %d | win %.0f%% | realized $%.2f | fees $%.2f | NET $%.2f ----\n",
		map[bool]string{true: "LIVE", false: "PAPER"}[a.cfg.Live], open, n, wr*100, realized, fees, realized-fees)
}

// ---- main ----

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.ToLower(strings.TrimSpace(p)); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func main() {
	disableQuickEdit() // Windows: stop the cmd window from PAUSING the engine when it's clicked/selected
	port := flag.String("port", "127.0.0.1:8799", "dashboard listen address")
	coins := flag.String("coins", "btc,eth,sol,xrp,doge", "comma-separated coins")
	durations := flag.String("durations", "15m", "comma-separated durations (5m,15m,1h,4h,1d). Default is 15m only: it's the one Kalshi-cross-confirmable band. NOTE (audit F3): the old '+$82' claim here was STALE/FALSE — the 15m bucket measured −$267.89 after fees over n=532 honest-fill bets (58.6% WR). Treat this engine as the falsification harness for the pcrypto edge, not as a proven strategy.")
	interval := flag.Duration("interval", 30*time.Second, "scan interval")
	bankroll := flag.Float64("bankroll", 1000, "paper bankroll $")
	stakePct := flag.Float64("stake-pct", 0.02, "flat stake fraction of bankroll per bet (cold-start / non-Kelly)")
	minEntry := flag.Float64("min-entry", 0.50, "skip favored-side bets priced below this (50¢ = start of the measured +EV band; 50-80¢ is the pcrypto sweet spot)")
	maxEntry := flag.Float64("max-entry", 0.80, "skip favored-side bets priced above this (80¢+ favorites are ≈ -EV: tiny payout eaten by fees)")
	soloMinEntry := flag.Float64("solo-min-entry", 0.65, "SOLO (Kalshi-unconfirmed) bets must clear this favorite bar (a ~50¢ solo bet is a coinflip — max fees, no edge; this is why the mini bled vs the suite). 0 = off. Cross-confirmed bets use min-entry.")
	minConf := flag.Float64("min-conf", 0, "confidence gate (0 = take all)")
	makerShare := flag.Float64("maker-share", 0.5, "fraction of fills modeled as maker ($0 fee)")
	crossX := flag.Float64("cross-x", 2.0, "size multiplier when Kalshi 15m confirms")
	kelly := flag.Bool("kelly", true, "size bets by fractional Kelly for max growth (false = flat stake-pct)")
	kellyMult := flag.Float64("kelly-mult", 0.5, "fractional-Kelly multiplier (0.5 = half-Kelly; lower = safer)")
	kellyCap := flag.Float64("kelly-cap", 0.03, "hard cap: max fraction of bankroll on a SINGLE bet")
	maxExposure := flag.Float64("max-exposure", 0.6, "cap on TOTAL open cost basis as a fraction of bankroll")
	noEdgeFloor := flag.Bool("no-edge-floor", true, "bet a small flat floor on no-edge bands instead of skipping (false = strict Kelly skip)")
	live := flag.Bool("live", false, "live data; order placement is a logged placeholder (no real orders)")
	headless := flag.Bool("headless", false, "do not open a browser or desktop window")
	statePath := flag.String("state", "pcrypto-state.json", "paper-state file (empty = no persistence)")
	logPath := flag.String("log", "pcrypto.log", "append activity to this log file (empty = console only)")
	flag.Parse()

	cfg := Config{
		Port: *port, Coins: splitCSV(*coins), Durations: splitCSV(*durations), Interval: *interval,
		Bankroll: *bankroll, StakePct: *stakePct, MinEntry: *minEntry, MaxEntry: *maxEntry, SoloMinEntry: *soloMinEntry,
		MinConf: *minConf, MakerShare: *makerShare, CrossConfirmX: *crossX,
		Kelly: *kelly, KellyMult: *kellyMult, KellyCap: *kellyCap, MaxExposure: *maxExposure, NoEdgeFloor: *noEdgeFloor, Live: *live, StatePath: *statePath,
	}
	app := &App{cfg: cfg, book: &Book{StartedAt: time.Now()}}
	// Persist activity to a log file (in addition to the console) so the run is captured even when the
	// window is closed/minimized/unfocused. Falls back to console-only if it can't be opened.
	if *logPath != "" {
		if lf, err := os.OpenFile(*logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
			app.logw = io.MultiWriter(os.Stdout, lf) // OS reclaims the handle on exit; no defer-close (runs until killed)
		} else {
			fmt.Println("log file open failed, console only:", err)
		}
	}
	app.loadCfg() // SETTINGS PERSISTENCE: apply saved dashboard tunables over the flag defaults
	app.load()
	app.serve(!*headless)

	fmt.Printf("pcrypto-server %s — dashboard http://%s\n", map[bool]string{true: "LIVE (placeholder orders)", false: "PAPER"}[cfg.Live], cfg.Port)
	fmt.Printf("coins=%v durations=%v bankroll=$%.0f stake=%.0f%% min-conf=%.2f maker-share=%.2f\n",
		cfg.Coins, cfg.Durations, cfg.Bankroll, cfg.StakePct*100, cfg.MinConf, cfg.MakerShare)
	if cfg.Kelly {
		fmt.Printf("sizing=KELLY  mult=%.2f  cap=%.0f%%/bet  max-exposure=%.0f%% of bankroll  (-EV bands skipped)\n", cfg.KellyMult, cfg.KellyCap*100, cfg.MaxExposure*100)
	} else {
		fmt.Printf("sizing=FIXED  %.0f%% of bankroll per bet\n", cfg.StakePct*100)
	}
	if cfg.Live {
		fmt.Println("** LIVE MODE: running on live data, but real order placement is a placeholder — NO real orders are sent. **")
	}

	ctx := context.Background()
	app.tick(ctx)
	app.printSummary()
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	// MINIFAST: mark open positions (cur price + path) on a fast 3s sub-cadence so the UI updates ASAP,
	// independent of the 30s scan tick. Safe to run concurrently — refreshPaths writes every position
	// field under a.book.mu; settle/scan stay on the slower main tick.
	fast := time.NewTicker(3 * time.Second)
	defer fast.Stop()
	go func() {
		for range fast.C {
			app.refreshPaths(ctx)
		}
	}()
	for range ticker.C {
		app.tick(ctx)
		app.printSummary()
	}
}
