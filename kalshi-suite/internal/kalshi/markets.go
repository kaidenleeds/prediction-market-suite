package kalshi

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
	"time"
)

// Candlesticks fetches a market's historical candle series between startTs and endTs (unix seconds),
// returning the mid-price path (0..1, oldest→newest). period is minutes (1/60/1440). BEST-EFFORT: returns
// nil on ANY error — used only to BACKFILL post-entry paths for the SL/TP backtest, never on a hot path,
// so a wrong endpoint/shape simply yields no data (no harm). Series ticker = the prefix before the first "-".
func (c *Client) Candlesticks(ctx context.Context, ticker string, startTs, endTs int64, periodMin int) []float64 {
	series := ticker
	if i := strings.Index(ticker, "-"); i > 0 {
		series = ticker[:i]
	}
	if series == "" || ticker == "" || startTs <= 0 || endTs <= startTs {
		return nil
	}
	switch periodMin {
	case 1, 60, 1440:
	default:
		periodMin = 60
	}
	var resp struct {
		Candlesticks []struct {
			Price struct {
				Open  flexFloat `json:"open"`
				Close flexFloat `json:"close"`
				High  flexFloat `json:"high"`
				Low   flexFloat `json:"low"`
				Mean  flexFloat `json:"mean"`
			} `json:"price"`
		} `json:"candlesticks"`
	}
	p := fmt.Sprintf("/series/%s/markets/%s/candlesticks?start_ts=%d&end_ts=%d&period_interval=%d",
		url.QueryEscape(series), url.QueryEscape(ticker), startTs, endTs, periodMin)
	if err := c.do(ctx, http.MethodGet, p, &resp, true); err != nil {
		return nil
	}
	out := make([]float64, 0, len(resp.Candlesticks))
	for _, cs := range resp.Candlesticks {
		v := cs.Price.Mean.Float() // Kalshi candle prices are in CENTS (0..100)
		if v <= 0 {
			if h, l := cs.Price.High.Float(), cs.Price.Low.Float(); h > 0 || l > 0 {
				v = (h + l) / 2
			} else {
				v = cs.Price.Close.Float()
			}
		}
		if v > 0 && v <= 100 {
			out = append(out, v/100.0)
		}
	}
	return out
}

// flexFloat parses a JSON value that may be a number, a numeric string, or null.
// Kalshi's current market schema sometimes returns prices/sizes as strings, so we
// stay lenient and never fail the whole decode over one odd field.
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	if s == "null" || s == `""` || s == "" {
		*f = 0
		return nil
	}
	s = strings.Trim(s, `"`)
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		*f = 0
		return nil
	}
	*f = flexFloat(v)
	return nil
}

func (f flexFloat) Float() float64 { return float64(f) }

// Market is a parsed Kalshi market using the CURRENT schema. Note the field names:
// prices end in "_dollars" (value already in dollars, e.g. 0.52 = 52%) and
// volumes/sizes end in "_fp". The older names (yes_bid, volume_24h) no longer exist.
type Market struct {
	Ticker      string `json:"ticker"`
	EventTicker string `json:"event_ticker"`
	Title       string `json:"title"`
	YesSubTitle string `json:"yes_sub_title"`
	// R102 STRUCTURAL MATCHING: the venue's own strike/side metadata (live-probed 2026-07-07 on
	// KXMLBGAME/KXMLBTOTAL/KXMLBSPREAD). no_sub_title carries the OTHER side's identity on
	// moneylines ("San Francisco" / "Colorado"); floor_strike carries the total/spread line (8.5,
	// 7.5); strike_type is "structured" (team markets) / "greater" (over-threshold); custom_strike
	// holds venue entity ids (e.g. {"baseball_team":"<uuid>"} = the team the YES side backs).
	// All additive decodes — absent fields stay zero-valued.
	NoSubTitle         string                     `json:"no_sub_title"`
	StrikeType         string                     `json:"strike_type"`
	FloorStrike        flexFloat                  `json:"floor_strike"`
	CapStrike          flexFloat                  `json:"cap_strike"`
	CustomStrike       map[string]json.RawMessage `json:"custom_strike"`
	Status             string                     `json:"status"`
	Result             string                     `json:"result"`    // "yes"/"no" once the market settles, else ""
	OpenTime           string                     `json:"open_time"` // R27 freshlist: listing time — "is this market actually NEW?"
	CloseTime          string                     `json:"close_time"`
	ExpectedExpiration string                     `json:"expected_expiration_time"`
	// Authoritative market-specific settlement text. Cross-venue identity and semantic-complexity
	// research must retain these exact artifacts; titles and structural sports fields are not rule
	// certificates. Empty remains unknown and fails closed.
	RulesPrimary        string     `json:"rules_primary"`
	RulesSecondary      string     `json:"rules_secondary"`
	EarlyCloseCondition string     `json:"early_close_condition"`
	YesBid              flexFloat  `json:"yes_bid_dollars"`
	YesBidSize          flexFloat  `json:"yes_bid_size_fp"`
	YesAsk              flexFloat  `json:"yes_ask_dollars"`
	YesAskSize          flexFloat  `json:"yes_ask_size_fp"`
	NoBid               flexFloat  `json:"no_bid_dollars"`
	NoAsk               flexFloat  `json:"no_ask_dollars"`
	LastPrice           flexFloat  `json:"last_price_dollars"`
	PreviousPrice       flexFloat  `json:"previous_price_dollars"`
	Volume24h           flexFloat  `json:"volume_24h_fp"`
	Volume              flexFloat  `json:"volume_fp"`
	OpenInterest        flexFloat  `json:"open_interest_fp"`
	Liquidity           flexFloat  `json:"liquidity_dollars"`
	SettlementValue     *flexFloat `json:"settlement_value_dollars"` // authoritative settled YES value for FINALIZED markets (handles scalar/structured, e.g. goalscorer). POINTER (R76, auditor bug 11a): absent must decode as ABSENT (nil), never as 0 — 0 means "YES lost"
	// MVESelectedLegs — R101 (auditor r28 bug 241 / edge 36): an MVE combo market's constituent
	// legs (market ticker + selected side each), served by GET /markets/{ticker} for KXMVE* combo
	// tickers (verified live 2026-07-07). Empty for ordinary markets. This is what lets the RFQ
	// would-quote logger price a bookless combo off its underlying leg books.
	MVESelectedLegs []MVELeg `json:"mve_selected_legs"`
	// R106 (auditor bug 254): per-market tick metadata ahead of the venue's SUB-CENT tick rollout
	// (~Jul 23). Decoded defensively (the final schema isn't published; both bare and _price/_size
	// field spellings accepted) — absent everything ⇒ TickFor serves the historical 1¢.
	TickSize    flexFloat    `json:"tick_size"`
	PriceRanges []PriceRange `json:"price_ranges"`
	// R124 D6: the venue already serves "price_level_structure":"linear_cent" beside price_ranges
	// (r113 raw captures); sub-cent structures become visible ~Jul 23 (pilots wk of Jul 27). Decoded
	// so the tick observatory (server tickobs.go) can latch + log pilot tickers the day they change.
	PriceLevelStructure string `json:"price_level_structure"`
	FeeWaiverExpiration string `json:"fee_waiver_expiration_time"` // when present, venue fees are zero until this instant
}

// PriceRange is one banded tick rule (R106 bug 254): [lo,hi] priced at `tick`. Field-name variants
// decoded side-by-side because the venue schema for the sub-cent rollout was unpublished when this
// shipped. R124: the REAL schema went live 2026-07-09 on 1,040 golf tickers (price_level_structure
// "tapered_deci_cent") and spells the fields `start`/`end`/`step` — venue-verified payload:
// [{"start":"0.0000","end":"0.1000","step":"0.0010"}, {"start":"0.1000","end":"0.9000","step":"0.0100"},
//
//	{"start":"0.9000","end":"1.0000","step":"0.0010"}]. Without the R124 decode TickFor silently fell
//
// back to the 1¢ default on these markets (orders stayed VALID — every 1¢ price sits on the 0.1¢
// grid — but the write path was blind to the fine tail ticks). Pinned in r124 tick tests.
type PriceRange struct {
	Min      flexFloat `json:"min"`
	Max      flexFloat `json:"max"`
	Tick     flexFloat `json:"tick"`
	MinPrice flexFloat `json:"min_price"`
	MaxPrice flexFloat `json:"max_price"`
	TickSize flexFloat `json:"tick_size"`
	Start    flexFloat `json:"start"` // R124: the venue's LIVE spelling (tapered_deci_cent rollout)
	End      flexFloat `json:"end"`
	Step     flexFloat `json:"step"`
}

// PriceStructureUpdate is the parsed, bounded receipt from a
// price_level_structure_updated/tick-size lifecycle frame. Presence booleans distinguish an
// omitted field from an explicit empty/zero value; callers must not infer missing ranges.
type PriceStructureUpdate struct {
	Ticker              string       `json:"ticker"`
	EventType           string       `json:"event_type"`
	PriceLevelStructure string       `json:"price_level_structure,omitempty"`
	PriceRanges         []PriceRange `json:"price_ranges,omitempty"`
	RangesPresent       bool         `json:"ranges_present"`
	TickSize            float64      `json:"tick_size,omitempty"`
	TickSizePresent     bool         `json:"tick_size_present"`
	Observed            time.Time    `json:"observed_at"`
}

const priceStructureReceiptCap = 4096

type marketLifecycleUpdate struct {
	EventType string
	CloseTime string
	Observed  time.Time
}

const marketLifecycleReceiptCap = 4096

func applyLifecycleToMarket(m *Market, update marketLifecycleUpdate) {
	if m == nil {
		return
	}
	switch update.EventType {
	case "activated":
		m.Status = "active"
	case "deactivated":
		m.Status = "inactive"
	case "determined", "settled":
		m.Status = update.EventType
	case "close_date_updated":
		if update.CloseTime != "" {
			m.CloseTime = update.CloseTime
			m.ExpectedExpiration = update.CloseTime
		}
	}
}

// applyRecentMarketUpdatesLocked merges socket facts that arrived after a multi-page REST crawl
// began. c.mktMu must be held. This prevents an early page in that crawl from overwriting newer
// price-grid or terminal-lifecycle truth at the final atomic publish.
func (c *Client) applyRecentMarketUpdatesLocked(markets []Market, started time.Time) {
	for i := range markets {
		key := strings.ToUpper(strings.TrimSpace(markets[i].Ticker))
		if u, ok := c.mktStructureUpdates[key]; ok && u.Observed.After(started) {
			applyStructureToMarket(&markets[i], u)
		}
		if u, ok := c.mktLifecycleUpdates[key]; ok && u.Observed.After(started) {
			applyLifecycleToMarket(&markets[i], u)
		}
	}
}

// LatestPriceStructure returns the newest parsed WS price-grid receipt for ticker. The receipt map
// is bounded; Market cache entries are patched separately and survive receipt eviction.
func (c *Client) LatestPriceStructure(ticker string) (PriceStructureUpdate, bool) {
	c.mktMu.Lock()
	defer c.mktMu.Unlock()
	u, ok := c.mktStructureUpdates[strings.ToUpper(strings.TrimSpace(ticker))]
	if ok {
		u.PriceRanges = append([]PriceRange(nil), u.PriceRanges...)
	}
	return u, ok
}

// applyPriceStructureUpdate immediately patches the cached market's exact supplied grid. When a
// structure name changes without ranges/tick metadata, stale old metadata is cleared so routing
// falls back conservatively until the next authoritative REST market refresh.
func (c *Client) applyPriceStructureUpdate(u PriceStructureUpdate) {
	u.Ticker = strings.ToUpper(strings.TrimSpace(u.Ticker))
	if u.Ticker == "" {
		return
	}
	u.PriceRanges = append([]PriceRange(nil), u.PriceRanges...)
	c.mktMu.Lock()
	defer c.mktMu.Unlock()
	if c.mktStructureUpdates == nil {
		c.mktStructureUpdates = make(map[string]PriceStructureUpdate)
	}
	if _, exists := c.mktStructureUpdates[u.Ticker]; !exists && len(c.mktStructureUpdates) >= priceStructureReceiptCap {
		oldestTicker := ""
		var oldest time.Time
		for ticker, receipt := range c.mktStructureUpdates {
			if oldestTicker == "" || receipt.Observed.Before(oldest) {
				oldestTicker, oldest = ticker, receipt.Observed
			}
		}
		delete(c.mktStructureUpdates, oldestTicker)
	}
	c.mktStructureUpdates[u.Ticker] = u
	for i := range c.mktCache {
		if !strings.EqualFold(strings.TrimSpace(c.mktCache[i].Ticker), u.Ticker) {
			continue
		}
		// Readers receive mktCache slices after the lock is released. Preserve that snapshot contract
		// with copy-on-write instead of mutating a backing array a caller may be traversing.
		patched := append([]Market(nil), c.mktCache...)
		applyStructureToMarket(&patched[i], u)
		c.mktCache = patched
		break
	}
}

func applyStructureToMarket(m *Market, u PriceStructureUpdate) {
	structureChanged := u.PriceLevelStructure != "" && !strings.EqualFold(m.PriceLevelStructure, u.PriceLevelStructure)
	if u.PriceLevelStructure != "" {
		m.PriceLevelStructure = u.PriceLevelStructure
	}
	if u.RangesPresent {
		m.PriceRanges = append([]PriceRange(nil), u.PriceRanges...)
	} else if structureChanged {
		m.PriceRanges = nil
	}
	if u.TickSizePresent {
		m.TickSize = flexFloat(u.TickSize)
	} else if structureChanged {
		m.TickSize = 0
	}
}

// ApplyPriceStructureUpdate returns a copy of m with the exact parsed lifecycle grid applied.
// It is exported so other in-process market snapshots can stay coherent without recreating the
// venue's private fixed-point decoder or reparsing raw WebSocket JSON.
func ApplyPriceStructureUpdate(m Market, u PriceStructureUpdate) Market {
	u.PriceRanges = append([]PriceRange(nil), u.PriceRanges...)
	applyStructureToMarket(&m, u)
	return m
}

func (r PriceRange) lo() float64 {
	if v := r.MinPrice.Float(); v > 0 {
		return v
	}
	if v := r.Min.Float(); v > 0 {
		return v
	}
	return r.Start.Float()
}
func (r PriceRange) hi() float64 {
	if v := r.MaxPrice.Float(); v > 0 {
		return v
	}
	if v := r.Max.Float(); v > 0 {
		return v
	}
	return r.End.Float()
}
func (r PriceRange) tickV() float64 {
	if v := r.TickSize.Float(); v > 0 {
		return v
	}
	if v := r.Tick.Float(); v > 0 {
		return v
	}
	return r.Step.Float()
}

// TickFor returns the market's tick size (dollars) at a given price: the covering price_ranges
// band wins, else the market-level tick_size, else the venue-historical 1¢. R106 (bug 254): every
// price WRITE (order placement / maker repricing) must step and snap by THIS, never a hardcoded
// 0.01 — the day the venue serves finer ticks the write path follows automatically.
func (m Market) TickFor(price float64) float64 {
	if tick, ok := m.TickForKnown(price); ok {
		return tick
	}
	return 0.01
}

// TickForKnown is TickFor without the historical 1-cent compatibility fallback. Execution-proof
// and book-native datasets use this form: absent tick metadata is missing schema, not evidence
// that the market is linear-cent.
func (m Market) TickForKnown(price float64) (float64, bool) {
	for _, r := range m.PriceRanges {
		if t := r.tickV(); t > 0 && price >= r.lo() && (r.hi() <= 0 || price <= r.hi()) {
			return t, true
		}
	}
	if t := m.TickSize.Float(); t > 0 {
		return t, true
	}
	if strings.EqualFold(strings.TrimSpace(m.PriceLevelStructure), "linear_cent") {
		return 0.01, true
	}
	return 0, false
}

// TickAbove/TickBelow disambiguate shared range boundaries for directional price moves. At 10 cents
// in a tapered market, moving up uses the 1-cent middle band while moving down uses the 0.1-cent
// lower tail; the inverse applies at 90 cents. TickFor remains the observational/current-band API.
func (m Market) TickAbove(price float64) float64 {
	for _, r := range m.PriceRanges {
		if t := r.tickV(); t > 0 && price >= r.lo() && (r.hi() <= 0 || price < r.hi()) {
			return t
		}
	}
	return m.TickFor(price)
}

func (m Market) TickBelow(price float64) float64 {
	for _, r := range m.PriceRanges {
		if t := r.tickV(); t > 0 && price > r.lo() && (r.hi() <= 0 || price <= r.hi()) {
			return t
		}
	}
	return m.TickFor(price)
}

func (m Market) NextPrice(price float64) float64 {
	t := m.TickAbove(price)
	return SnapPx(price+t, t)
}

func (m Market) PrevPrice(price float64) float64 {
	t := m.TickBelow(price)
	return SnapPx(price-t, t)
}

// SnapPx snaps a dollar price onto the tick grid (nearest; clamped inside (tick, 1−tick)) with the
// FP dust rounded off so FmtPx renders cleanly. R106 (bug 254).
func SnapPx(px, tick float64) float64 {
	if tick <= 0 {
		tick = 0.01
	}
	v := math.Round(px/tick) * tick
	if v < tick {
		v = tick
	}
	if v > 1-tick {
		v = 1 - tick
	}
	return math.Round(v*1e6) / 1e6
}

// FmtPx renders a dollar price for the wire: two to four decimals, matching Kalshi's fixed-point
// dollar schema. Quantizing before formatting removes binary float dust such as
// 0.050000000000000044 without changing any valid 0.0001-dollar price.
func FmtPx(px float64) string {
	px = math.Round(px*1e4) / 1e4
	s := strconv.FormatFloat(px, 'f', 4, 64)
	i := strings.IndexByte(s, '.')
	if i < 0 {
		return s + ".00"
	}
	for len(s)-i-1 > 2 && strings.HasSuffix(s, "0") {
		s = strings.TrimSuffix(s, "0")
	}
	return s
}

// ImpliedProbability returns the market's implied probability (0..1) from the best
// available price: the ask if present, otherwise the last trade price.
func (m Market) ImpliedProbability() float64 {
	// A degenerate ask of 1.00 (or 0) means a dead/thin book — NOT a real 100% price.
	// Prefer it only when it's a genuine in-range quote; otherwise fall to the last trade,
	// then the bid. This stops thin live markets (e.g. a far-out BTC daily) marking at 0/100.
	if a := m.YesAsk.Float(); a > 0 && a < 1 {
		return a
	}
	if lp := m.LastPrice.Float(); lp > 0 && lp < 1 {
		return lp
	}
	if b := m.YesBid.Float(); b > 0 && b < 1 {
		return b
	}
	return 0
}

// SettledYes returns the authoritative settled YES value in [0,1] for a FINALIZED market, or -1 if it
// isn't settled yet. Handles binary (result yes/no) AND scalar/structured markets (settlement_value —
// e.g. goalscorer markets that settle to a partial value like 0.10). Mark resolved positions off THIS,
// not a stale/degenerate book (a dead market quotes yes_ask=1.00 → would mark a NO position at 0¢).
func (m Market) SettledYes() float64 {
	switch m.Result {
	case "yes":
		return 1
	case "no":
		return 0
	}
	if m.Status == "finalized" || m.Status == "settled" || m.Status == "determined" {
		// R76 (auditor bug 11a): only trust a venue-SENT settlement value. The zero-value decode
		// used to read an ABSENT settlement_value_dollars as 0 — i.e. "YES lost" — mass-settling
		// YES rows to fake losses / NO rows to fake wins on any payload variant that omits the
		// field (the exact zero-settle corruption signature that forced the polyus v4 quarantine).
		// Absent ⇒ -1 (not yet knowable): the poll sweep simply re-checks next pass.
		if m.SettlementValue != nil {
			return m.SettlementValue.Float()
		}
	}
	return -1
}

type marketsResponse struct {
	Markets []Market `json:"markets"`
	Cursor  string   `json:"cursor"`
}

// SeriesInfo is the slice of series metadata the weather-edge sweep needs: the ticker plus the
// venue's OWN settlement sources. R27 operator directive: settlement sources DIFFER per market
// (NYC settles on the Central Park NWS CLI report; KXHIGHNYD settles on AccuWeather METAR; the
// same city can have multiple series on different stations) — a forecast model must read the
// settling source per series, never assume it.
type SeriesInfo struct {
	Ticker            string  `json:"ticker"`
	Title             string  `json:"title"`
	Frequency         string  `json:"frequency"`
	FeeType           string  `json:"fee_type"`
	FeeMultiplier     float64 `json:"fee_multiplier"`
	SettlementSources []struct {
		Name string `json:"name"`
		URL  string `json:"url"`
	} `json:"settlement_sources"`
}

// AllSeries returns the venue's current series registry. This is the authoritative baseline fee
// schema; /series/fee_changes is only a change log and therefore cannot identify every series that
// currently charges maker fees.
func (c *Client) AllSeries(ctx context.Context) ([]SeriesInfo, error) {
	var resp struct {
		Series []SeriesInfo `json:"series"`
	}
	if err := c.do(ctx, http.MethodGet, "/series", &resp, false); err != nil {
		return nil, err
	}
	return resp.Series, nil
}

// SeriesByCategory lists all series in a category (e.g. "Climate and Weather") with their
// settlement sources. Public market data.
func (c *Client) SeriesByCategory(ctx context.Context, category string) ([]SeriesInfo, error) {
	var resp struct {
		Series []SeriesInfo `json:"series"`
	}
	if err := c.do(ctx, http.MethodGet, "/series?category="+url.QueryEscape(category), &resp, false); err != nil {
		return nil, err
	}
	return resp.Series, nil
}

// MarketsBySeries returns all open markets in one series (e.g. the KXBTCD threshold ladder — every
// "$X or above" strike for the current crypto events). Public market data, no auth.
func (c *Client) MarketsBySeries(ctx context.Context, seriesTicker string) ([]Market, error) {
	var resp marketsResponse
	if err := c.do(ctx, http.MethodGet, "/markets?status=open&limit=1000&series_ticker="+url.QueryEscape(seriesTicker), &resp, false); err != nil {
		return nil, err
	}
	return resp.Markets, nil
}

// liquidSeries is a broad set of active Kalshi series (stable tickers) spanning
// crypto, tennis, esports, cricket, baseball, basketball, golf and tech.
//
// R73 FULL UNIVERSE: this list is now ONLY a WARM-BOOT SEED. R72-B pulled it on EVERY refresh
// (63 requests/tick) because the windowed pull truncated at a few pages; the windowed pull now
// paginates the WHOLE 18-day universe (markets_max_pull default 20k rows), so per-series pulls
// buy nothing after the first full board lands. The seed still runs ONCE at boot so the
// in-season series (crypto 15m, live sports) are warm seconds after start instead of after the
// first ~20-page windowed pull completes.
//
// R72-B SUB-MARKETS history (operator: "all subbets — game lines, player props, totals, goals/pts
// scored, passes — in markets AND logged as data"): the original list was MONEYLINES-ONLY; the
// blocks below added the probe-verified active sub-market series (MLB props/lines, WNBA, the
// 2026 World Cup complex). Under R73 there is no per-refresh cost to this list at all.
var liquidSeries = []string{
	"KXBTC15M", "KXETH15M", "KXSOL15M", "KXDOGE15M", "KXXRP15M", "KXBNB15M", "KXHYPE15M",
	"KXBTCD", "KXETHD", "KXSOLD",
	"KXATPMATCH", "KXWTAMATCH", "KXATPCHALLENGERMATCH", "KXWTACHALLENGERMATCH", "KXITFMATCH", "KXITFWMATCH",
	"KXCS2GAME", "KXVALORANTGAME", "KXDOTA2GAME", "KXLOLGAME",
	"KXT20MATCH", "KXTESTMATCH", "KXWT20MATCH", "KXCOUNTYCHAMPMATCH", "KXMLC", "KXIPLMATCH",
	"KXBSLGAME", "KXMLBGAME", "KXPGATOUR", "KXLPGATOUR",
	"KXWNBAGAME", "KXNBAGAME", "KXNHLGAME", "KXMLSGAME", "KXUFCFIGHT", "KXBOXING",
	"KXH200WS", "KXB200WS", "KXRTX5090WS",
	// R72-B MLB sub-markets (probed active 2026-07-04): game lines (run-line spread, game/team/F5
	// totals), 1st-inning-run, and the player-prop ladders (HR / total bases / hits / strikeouts).
	"KXMLBSPREAD", "KXMLBTOTAL", "KXMLBTEAMTOTAL", "KXMLBF5SPREAD", "KXMLBF5TOTAL",
	"KXMLBRFI", "KXMLBHR", "KXMLBTB", "KXMLBHIT", "KXMLBKS",
	// R72-B WNBA sub-markets: spreads/totals + player points/rebounds props.
	"KXWNBASPREAD", "KXWNBATOTAL", "KXWNBAPTS", "KXWNBAREB",
	// R72-B 2026 World Cup (in progress): moneylines + spreads/totals/1H-totals/BTTS/corners/
	// correct-score/goalscorer/shots-on-target/assists (goals scored + passes per the directive).
	"KXWCGAME", "KXWCSPREAD", "KXWCTOTAL", "KXWC1HTOTAL", "KXWCBTTS", "KXWCCORNERS",
	"KXWCSCORE", "KXWCGOAL", "KXWCSOA", "KXWCAST",
}

// GetTopLiquidMarkets returns EVERY open, bettable Kalshi market whose close lands inside the
// ~18-day pull window — the FULL catalog universe (R73, operator: "every single thing that can
// be bet on needs to be included": 188-rung BTC hourlies, MLB player props, World Cup
// mentions/props, golf R4 H2Hs — most of which the old volume filter + 96h horizon dropped) —
// minus dead parlay/MVE combos, ranked by 24h volume so the most active markets lead. It
// filters on expected_expiration_time (the real game/resolution time), NOT close_time:
// live-sport markets set close_time up to ~2 weeks past the game. The windowed pull paginates
// until the cursor ends (markets_max_pull budget, default 20k rows ≈ 20 pages); the curated
// series list is merged ONCE as a warm-boot seed only. Cached markets_refresh_s+0.5s; the cache
// lock is NOT held during the multi-request refresh — concurrent callers serve the last good
// cache. Public data. (Doc applies to GetTopLiquidMarkets below; TopLiquidCached is its
// non-blocking head.)
//
// TopLiquidCached is the NON-BLOCKING read of the markets board (R85 wedge fix, item "/api/markets
// must NEVER block"): it returns the current cache (nil while cold) plus whether a background
// refresh is in flight, kicking the stale-while-revalidate refresher exactly like
// GetTopLiquidMarkets — but it never waits, not even on an ice-cold boot. Request paths
// (/api/markets → marketRowsSnapshot) use THIS so they serve their cache/building state instantly;
// the 2026-07-05 wedge proved the cold-cache wait below was reachable from the handler (each
// request dwelled its full 10s while the board pull was starved, stacking goroutines into what the
// operator observed as handler timeouts).
func (c *Client) TopLiquidCached() (mkts []Market, refreshing bool) {
	c.mktMu.Lock()
	cached := c.mktCache
	// R85 ROOT CAUSE (2026-07-05, THE 12h universal wedge, build tealcobra): this expression used
	// to call c.refreshEvery() — WHICH LOCKS mktMu — while mktMu was ALREADY HELD here. Go mutexes
	// are non-reentrant, so the goroutine parked forever HOLDING mktMu, and every board/REST path
	// in the suite parked behind it: the board refresher itself, all 15 GetTopLiquidMarkets call
	// sites (autoTick, briefing, catalog, /api/markets), BoardPullStats (the mkts chip), and the
	// config setters. Crucially the `cached != nil` short-circuit meant COLD boards never hit it —
	// the deadlock armed on the FIRST WARM READ, ~4s after the first successful full pull, which
	// is why every earlier build "worked" exactly as long as its board pull kept failing, and why
	// ctx deadlines/watchdogs were powerless (mutex waits ignore ctx). refreshEveryLocked reads
	// the knob without re-locking.
	fresh := cached != nil && time.Since(c.mktCacheAt) < c.refreshEveryLocked()+500*time.Millisecond
	start := !fresh && !c.mktRefreshing
	if start {
		c.mktRefreshing = true
	}
	busy := c.mktRefreshing
	c.mktMu.Unlock()
	if start {
		go c.refreshBoard() // background; clears mktRefreshing when done (deferred inside)
	}
	return cached, busy
}

// CompleteBoardSnapshot returns the most recent successfully published complete-board cache and
// the exact local receipt time for that snapshot. It never starts a refresh or performs venue I/O.
// Money-authorizing cached producers use the receipt independently from per-market REST/price
// timestamps: immutable market lifecycle/catalog freshness and live WS price freshness are
// different proofs and must not silently stand in for each other.
//
// The returned slice is the immutable published cache. Callers must not modify it.
func (c *Client) CompleteBoardSnapshot() (mkts []Market, observedAt time.Time) {
	if c == nil {
		return nil, time.Time{}
	}
	c.mktMu.Lock()
	mkts, observedAt = c.mktCache, c.mktCacheAt
	c.mktMu.Unlock()
	return mkts, observedAt
}

// MarketCached returns one market from the last complete full-board snapshot without starting a
// refresh or performing venue I/O. The board slice is immutable after publication (lifecycle
// updates and refreshes publish a copied/replacement slice), so it is safe to scan after releasing
// mktMu. This is intentionally distinct from GetMarket: execution hot paths use it only to recover
// exact lifecycle/tick metadata for a ticker whose live WS book is already resident.
func (c *Client) MarketCached(ticker string) (Market, bool) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	if ticker == "" {
		return Market{}, false
	}
	c.mktMu.Lock()
	cached := c.mktCache
	c.mktMu.Unlock()
	for i := range cached {
		if strings.EqualFold(strings.TrimSpace(cached[i].Ticker), ticker) {
			return cached[i], true
		}
	}
	return Market{}, false
}

// applyMarketLifecycleUpdate keeps the immutable complete-board metadata cache aligned with the
// authoritative lifecycle stream. Execution may then pair a current sequence-proven order book
// with already-resident lifecycle/tick metadata instead of making a blocking GET immediately
// before every order. Copy-on-write preserves CompleteBoardSnapshot's published-slice contract.
func (c *Client) applyMarketLifecycleUpdate(ticker, eventType, closeTime string, observed time.Time) {
	ticker = strings.ToUpper(strings.TrimSpace(ticker))
	eventType = strings.ToLower(strings.TrimSpace(eventType))
	closeTime = strings.TrimSpace(closeTime)
	if ticker == "" || observed.IsZero() {
		return
	}
	update := marketLifecycleUpdate{
		EventType: eventType, CloseTime: closeTime, Observed: observed.UTC(),
	}
	switch eventType {
	case "activated", "deactivated", "determined", "settled":
	case "close_date_updated":
		if closeTime == "" {
			return
		}
	default:
		return
	}
	c.mktMu.Lock()
	defer c.mktMu.Unlock()
	if c.mktLifecycleUpdates == nil {
		c.mktLifecycleUpdates = make(map[string]marketLifecycleUpdate)
	}
	if _, exists := c.mktLifecycleUpdates[ticker]; !exists &&
		len(c.mktLifecycleUpdates) >= marketLifecycleReceiptCap {
		oldestTicker := ""
		var oldest time.Time
		for candidate, receipt := range c.mktLifecycleUpdates {
			if oldestTicker == "" || receipt.Observed.Before(oldest) {
				oldestTicker, oldest = candidate, receipt.Observed
			}
		}
		delete(c.mktLifecycleUpdates, oldestTicker)
	}
	c.mktLifecycleUpdates[ticker] = update
	for i := range c.mktCache {
		if !strings.EqualFold(strings.TrimSpace(c.mktCache[i].Ticker), ticker) {
			continue
		}
		patched := append([]Market(nil), c.mktCache...)
		applyLifecycleToMarket(&patched[i], update)
		c.mktCache = patched
		return
	}
}

func (c *Client) GetTopLiquidMarkets(ctx context.Context) ([]Market, error) {
	// R75 STALE-WHILE-REVALIDATE (root cause of "Could not load markets (signal is aborted)"):
	// the R73 full-universe pull is ~15-20 sequential 1000-row pages (~2MB each, 1-5s apiece
	// measured 2026-07-04) — 25-60s+ end to end, far past every request deadline. The old flow
	// let the FIRST caller to find the cache stale-and-idle run that pull INLINE, so once per
	// pull cycle a dashboard request (/api/markets, /api/arb, …) blocked for the whole pull and
	// the browser aborted at 20s. Now the pull ALWAYS runs in a background goroutine on the
	// refresher's lifetime ctx: warm callers get the cache instantly (stale is fine — live marks
	// ride the ticker WS), and only a genuinely COLD boot briefly waits for the first pull.
	// (R85: request paths use TopLiquidCached instead — even the cold-boot wait below must never
	// be reachable from an HTTP handler.)
	cached, _ := c.TopLiquidCached()
	if cached != nil {
		return cached, nil // never block a request path behind the multi-request pull
	}
	// COLD-CACHE STAMPEDE (audit §4): no cache yet — wait briefly for the first background
	// pull to land instead of every caller firing its own.
	for i := 0; i < 200; i++ { // ≤10s
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
		c.mktMu.Lock()
		busy, cur := c.mktRefreshing, c.mktCache
		c.mktMu.Unlock()
		if cur != nil {
			return cur, nil
		}
		if !busy {
			return nil, fmt.Errorf("markets board pull failed (no cache yet); retrying on the next tick")
		}
	}
	return nil, fmt.Errorf("markets board still warming (first full-universe pull in flight)")
}

// refreshBoard runs ONE full windowed pull in the background and swaps the cache when done.
// It must never run on a request ctx (a canceled request would tear the pull down mid-page),
// so it derives from the refresher's lifetime ctx with a generous per-pull ceiling.
func (c *Client) refreshBoard() {
	defer func() {
		c.mktMu.Lock()
		c.mktRefreshing = false
		c.mktMu.Unlock()
	}()
	c.mktMu.Lock()
	base := c.mktBgCtx
	c.mktMu.Unlock()
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, 5*time.Minute) // complete cursor crawl; context is the only ceiling
	defer cancel()
	started := time.Now()

	nowUnix := time.Now().Unix()
	// Wide close-time window: live-sport markets set close_time up to ~2 weeks PAST the
	// game (postponement buffer), so a tight close window silently drops every in-play
	// cricket/tennis/etc. We pull wide here, then keep only markets whose real resolution
	// time (expected_expiration_time) is within ~96h via expiresSoon below.
	//
	// R70-B F5 VERIFIED (docs + live probes 2026-07-04): the Get Markets doc table says
	// min/max_close_ts is only "compatible" with status=closed/empty — but a live probe of
	// status=open + a narrow far-future window returned 1000/1000 rows INSIDE the window, all
	// status=active, so the server applies BOTH filters today (the combo is NOT a silent no-op;
	// the doc table is stricter than the implementation). Kept as-is because it works AND the
	// documented alternative (empty status) front-loads thousands of `initialized` rows (probed)
	// that would eat the 5-page budget. Two backstops make a future doc-enforced no-op harmless:
	// expiresSoon() re-filters client-side and the curated-series pull below always runs.
	// mve_filter=exclude added (documented, probe-verified with status=open: 0 KXMVE rows) so
	// combo markets stop wasting page budget before the title-heuristic drop.
	const pullWindow = 18 * 24 * time.Hour
	soon := time.Now().Add(pullWindow).Unix()
	seen := make(map[string]Market)
	cursor := ""
	// R73 FULL UNIVERSE: the page budget (markets_max_pull, default 20000 = 20 pages) now covers
	// the ENTIRE windowed universe — probe 2026-07-04: 14,218+ open rows in the 18-day window, so
	// R72-B's 8-page default still truncated ~44% and the operator's app showed bettable markets
	// (188-rung BTC hourlies, player-prop ladders) the suite never ingested. The cursor ends the
	// loop as soon as the universe is exhausted, so the budget is a CEILING, not a per-refresh
	// cost: ~1 request per 1000 open rows. The kept horizon is the FULL pull window (was 96h) —
	// "catalog everything"; near-term relevance is downstream ranking/gating, not an ingest filter.
	cursorSeen := map[string]struct{}{}
	var pullErr error
	for page := 0; ; page++ {
		path := "/markets?status=open&limit=1000&mve_filter=exclude" +
			"&min_close_ts=" + strconv.FormatInt(nowUnix, 10) +
			"&max_close_ts=" + strconv.FormatInt(soon, 10)
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		var resp marketsResponse
		if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
			pullErr = fmt.Errorf("kalshi markets page %d: %w", page, err)
			break
		}
		for _, m := range resp.Markets {
			if isBettable(m) && expiresSoon(m, pullWindow) {
				seen[m.Ticker] = m
			}
		}
		if resp.Cursor == "" || len(resp.Markets) == 0 {
			break
		}
		if _, repeated := cursorSeen[resp.Cursor]; repeated {
			pullErr = fmt.Errorf("kalshi markets pagination repeated cursor at page %d", page)
			break
		}
		cursorSeen[resp.Cursor] = struct{}{}
		cursor = resp.Cursor
	}
	if pullErr != nil {
		// Never publish a partial universe. The last complete snapshot remains live.
		return
	}

	// WARM-BOOT SEED (R73): merge the curated live-sport / crypto series ONCE, on the first
	// successful refresh only. The fully-paginated windowed pull above now owns steady-state
	// coverage (R72-B re-pulled all ~63 series EVERY refresh — most of its request budget);
	// the seed just makes the in-season series live seconds after boot.
	c.mktMu.Lock()
	seeded := c.mktSeeded
	c.mktMu.Unlock()
	if !seeded {
		for _, series := range liquidSeries {
			var resp marketsResponse
			if err := c.do(ctx, http.MethodGet, "/markets?status=open&limit=1000&series_ticker="+url.QueryEscape(series), &resp, false); err != nil {
				continue
			}
			for _, m := range resp.Markets {
				if isBettable(m) && expiresSoon(m, pullWindow) {
					seen[m.Ticker] = m
				}
			}
		}
		if len(seen) > 0 { // seed sticks only once it actually returned data (a dead-network boot retries)
			c.mktMu.Lock()
			c.mktSeeded = true
			c.mktMu.Unlock()
		}
	}

	out := make([]Market, 0, len(seen))
	for _, m := range seen {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Volume24h.Float() > out[j].Volume24h.Float() })

	c.mktMu.Lock()
	// A structure lifecycle frame may have arrived while this multi-page crawl was in flight. Its
	// event-time grid is newer than some REST pages, so reapply only those newer receipts before the
	// atomic cache swap. Older receipts yield to the just-completed authoritative REST crawl.
	// A page fetched before a concurrent terminal lifecycle frame can still say active. Reapply
	// newer socket receipts so the completed crawl cannot resurrect the market.
	c.applyRecentMarketUpdatesLocked(out, started)
	// Don't clobber a good cache with an empty result (a transient all-requests-failed
	// refresh would otherwise blank the panel for a full TTL); just retry next call.
	if len(out) > 0 || c.mktCache == nil {
		c.mktCache = out
		c.mktCacheAt = time.Now()
	}
	c.mktPullDur = time.Since(started) // R75 observability: pull duration + kept rows (BoardPullStats)
	c.mktPullRows = len(out)
	c.mktPullAt = time.Now()
	c.mktMu.Unlock()
}

// BoardPullStats reports the last completed full-universe pull: rows kept, how long the pull
// took, and when it finished (zerolike before the first pull lands). R75: the server logs this
// once per material change so "is the whole universe actually landing, and how slow is the
// pull?" is answerable from the log instead of a debugger.
func (c *Client) BoardPullStats() (rows int, dur time.Duration, at time.Time) {
	c.mktMu.Lock()
	defer c.mktMu.Unlock()
	return c.mktPullRows, c.mktPullDur, c.mktPullAt
}

// expiresSoon reports whether the market's real resolution time (expected_expiration_time,
// falling back to close_time) lands within [now-6h, now+within]. The small look-back keeps
// just-finished, still-settling live games in view. A market with an unparseable time is
// kept (lenient) rather than silently dropped.
func expiresSoon(m Market, within time.Duration) bool {
	ts := m.ExpectedExpiration
	if strings.TrimSpace(ts) == "" {
		ts = m.CloseTime
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return true
	}
	now := time.Now()
	return t.After(now.Add(-6*time.Hour)) && t.Before(now.Add(within))
}

type singleMarketResponse struct {
	Market Market `json:"market"`
}

// GetMarket fetches one market by ticker (public, no credentials). Used to resolve
// human names + event tickers for markets seen on the live trade tape but not in the
// windowed liquid set.
func (c *Client) GetMarket(ctx context.Context, ticker string) (Market, error) {
	var resp singleMarketResponse
	if err := c.do(ctx, http.MethodGet, "/markets/"+url.PathEscape(ticker), &resp, false); err != nil {
		return Market{}, err
	}
	return resp.Market, nil
}

// GetMarketsByTickers bulk-fetches specific markets (GET /markets?tickers=a,b,c — verified against
// the July 2026 docs: comma-separated tickers filter, limit max 1000). Batches of 100 keep URLs sane.
// This is the COLD-START killer: one call warms ~100 markets' close times/prices where per-ticker
// GetMarket needed 100 round-trips — combos and horizons are live within seconds of boot.
func (c *Client) GetMarketsByTickers(ctx context.Context, tickers []string) ([]Market, error) {
	out := make([]Market, 0, len(tickers))
	for start := 0; start < len(tickers); start += 100 {
		end := start + 100
		if end > len(tickers) {
			end = len(tickers)
		}
		var resp struct {
			Markets []Market `json:"markets"`
		}
		path := "/markets?limit=1000&tickers=" + url.QueryEscape(strings.Join(tickers[start:end], ","))
		if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
			if len(out) > 0 {
				return out, nil // best-effort: keep what we warmed
			}
			return nil, err
		}
		out = append(out, resp.Markets...)
	}
	return out, nil
}

// ComboEligibleEvents returns the set of EVENT tickers currently eligible for Kalshi COMBOS (from the
// multivariate event collections + their associated_event_tickers). A market is combo-able iff its
// event_ticker is in this set. The set is ROLLING/dynamic (Kalshi adds events closer to start time), so
// callers should refresh it periodically. Best-effort paginated; returns what it has on a mid-page error.
func (c *Client) ComboEligibleEvents(ctx context.Context) (map[string]bool, error) {
	out := map[string]bool{}
	cursor := ""
	for i := 0; i < 15; i++ { // bounded pagination
		var resp struct {
			Contracts []struct {
				AssociatedEventTickers []string `json:"associated_event_tickers"`
			} `json:"multivariate_contracts"`
			Cursor string `json:"cursor"`
		}
		path := "/multivariate_event_collections?status=open&limit=200"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
			if len(out) > 0 {
				return out, nil
			}
			return nil, err
		}
		for _, mc := range resp.Contracts {
			for _, ev := range mc.AssociatedEventTickers {
				out[ev] = true
			}
		}
		if resp.Cursor == "" || resp.Cursor == cursor {
			break
		}
		cursor = resp.Cursor
	}
	return out, nil
}

// MVEventCfg is the PER-EVENT rulebook inside a collection (probed live 2026-07-02 on
// KXMVESPORTSMULTIGAMEEXTENDED-R + docs "MVE extension"): moneyline/props events are is_yes_only
// (a side:"no" leg → invalid_parameters), and most events cap the number of markets you may take
// from them (size_max, usually 1 — two total-lines from one game → invalid_parameters). These two
// rules were the invisible 400s behind "every parlay keeps refusing".
type MVEventCfg struct {
	IsYesOnly bool
	SizeMax   int // 0 = unlimited
	Quoters   int // active market-maker quoters RIGHT NOW (0 → expect zero RFQ quotes)
}

// MVCollection is one multivariate (combo) collection: its ticker (needed to create combined markets), the
// leg-count bounds, and the set of associated EVENT tickers with whether each has active market-maker
// quoters (an event with no active quoters won't get a combo quote → no fill).
type MVCollection struct {
	CollectionTicker string
	SizeMin, SizeMax int
	Events           map[string]bool       // event ticker -> has ≥1 active quoter
	EventCfg         map[string]MVEventCfg // event ticker -> per-event constraints (empty for flat-list collections)
}

// LegOK checks one (event, side) leg against this collection's per-event rules. Returns "" when the
// leg is allowed, else a human reason. Membership must be checked by the caller (Events).
func (c MVCollection) LegOK(eventTicker, side string) string {
	cfg, ok := c.EventCfg[eventTicker]
	if !ok {
		return "" // flat-list collection — no per-event config to enforce
	}
	if cfg.IsYesOnly && strings.EqualFold(side, "no") {
		return "event " + eventTicker + " is YES-only in this collection (venue refuses NO legs)"
	}
	return ""
}

// ComboCollections returns the OPEN multivariate collections with their collection ticker, size bounds, and
// per-event quoter presence — everything needed to (a) find a collection that contains a chosen set of legs
// and (b) judge whether a maker is likely to quote. Best-effort paginated.
func (c *Client) ComboCollections(ctx context.Context) ([]MVCollection, error) {
	var out []MVCollection
	cursor := ""
	for i := 0; i < 15; i++ {
		var resp struct {
			Contracts []struct {
				CollectionTicker string `json:"collection_ticker"`
				SizeMin          int    `json:"size_min"`
				SizeMax          int    `json:"size_max"`
				AssociatedEvents []struct {
					Ticker        string   `json:"ticker"`
					ActiveQuoters []string `json:"active_quoters"`
					IsYesOnly     bool     `json:"is_yes_only"`
					EvSizeMax     *int     `json:"size_max"` // null = no per-event cap
				} `json:"associated_events"`
				AssociatedEventTickers []string `json:"associated_event_tickers"`
			} `json:"multivariate_contracts"`
			Cursor string `json:"cursor"`
		}
		path := "/multivariate_event_collections?status=open&limit=200"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
			if len(out) > 0 {
				return out, nil
			}
			return nil, err
		}
		for _, mc := range resp.Contracts {
			col := MVCollection{CollectionTicker: mc.CollectionTicker, SizeMin: mc.SizeMin, SizeMax: mc.SizeMax,
				Events: map[string]bool{}, EventCfg: map[string]MVEventCfg{}}
			for _, ae := range mc.AssociatedEvents {
				col.Events[ae.Ticker] = len(ae.ActiveQuoters) > 0
				cfg := MVEventCfg{IsYesOnly: ae.IsYesOnly, Quoters: len(ae.ActiveQuoters)}
				if ae.EvSizeMax != nil {
					cfg.SizeMax = *ae.EvSizeMax
				}
				col.EventCfg[ae.Ticker] = cfg
			}
			for _, ev := range mc.AssociatedEventTickers { // fallback for the deprecated flat list
				if _, ok := col.Events[ev]; !ok {
					col.Events[ev] = false
				}
			}
			out = append(out, col)
		}
		if resp.Cursor == "" || resp.Cursor == cursor {
			break
		}
		cursor = resp.Cursor
	}
	return out, nil
}

// GetEventTitle returns the human matchup/context for an event (e.g. "Belgium vs. Senegal") — the game
// a market belongs to, which the per-market title often omits (e.g. "Will over 2.5 goals be scored?").
// Best-effort: prefers sub_title (usually the matchup), falls back to title; "" on any error.
func (c *Client) GetEventTitle(ctx context.Context, eventTicker string) string {
	if eventTicker == "" {
		return ""
	}
	var resp struct {
		Event struct {
			Title    string `json:"title"`
			SubTitle string `json:"sub_title"`
		} `json:"event"`
	}
	if err := c.do(ctx, http.MethodGet, "/events/"+url.PathEscape(eventTicker), &resp, false); err != nil {
		return ""
	}
	if strings.TrimSpace(resp.Event.SubTitle) != "" {
		return resp.Event.SubTitle
	}
	return resp.Event.Title
}

// StartBoardRefresher keeps the liquid-market board warm on the CONFIGURED cadence (audit §4: a
// persistent background refresher means request paths NEVER pull the board themselves — they
// always hit a fresh cache). Call once in a goroutine at startup.
//
// R73 BUDGET MATH (default markets_refresh_s=4): the full-universe pull is ~15-20 windowed pages
// per refresh (1 request per 1000 open rows; the curated seed runs once at boot), so steady state
// is ~4-5 req/s — 25% of the 20/s Basic budget, 17% of 30/s Advanced. That is LESS than R72-B's
// 71-request/4s (~17.8 req/s ≈ 59%) because the per-refresh curated re-pull is gone. Live marks
// come from the ticker WS (R70-B), so REST board staleness stays cheap; the ticker re-arms each
// pass so a live markets_refresh_s edit applies within one tick.
func (c *Client) StartBoardRefresher(ctx context.Context) {
	// R75: background pulls (refreshBoard) run on THIS lifetime ctx, never on a request ctx —
	// see GetTopLiquidMarkets. Recorded before the first tick so even a request-path trigger
	// that races boot picks it up on the next cycle.
	c.mktMu.Lock()
	c.mktBgCtx = ctx
	c.mktMu.Unlock()
	_, _ = c.GetTopLiquidMarkets(ctx) // kick the first pull immediately (don't wait one tick)
	every := c.refreshEvery()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = c.GetTopLiquidMarkets(ctx)
			if e := c.refreshEvery(); e != every {
				every = e
				t.Reset(every)
			}
		}
	}
}

// SetMarketsMaxPull wires the markets_max_pull config knob (max rows the WINDOWED pull may
// fetch per refresh, 1000 per page). 0 = default 20000 (R73 full universe; probe 2026-07-04:
// 14,218+ open rows in-window and growing); clamped to [5000, 40000] so a typo can neither
// regress below the historical 5-page floor nor run the cursor unbounded.
func (c *Client) SetMarketsMaxPull(rows int) {
	c.mktMu.Lock()
	c.mktMaxPull = rows
	c.mktMu.Unlock()
}

// marketsMaxPull returns the effective windowed-pull row budget (see SetMarketsMaxPull).
func (c *Client) marketsMaxPull() int {
	c.mktMu.Lock()
	v := c.mktMaxPull
	c.mktMu.Unlock()
	if v <= 0 {
		return 20000
	}
	if v < 5000 {
		return 5000
	}
	if v > 40000 {
		return 40000
	}
	return v
}

// SetMarketsRefreshS wires the markets_refresh_s config knob (board refresh cadence, seconds).
// 0 = default 4s; clamped to [2, 60]. The GetTopLiquidMarkets cache TTL rides this at +0.5s.
func (c *Client) SetMarketsRefreshS(secs float64) {
	c.mktMu.Lock()
	c.mktRefreshS = secs
	c.mktMu.Unlock()
}

// refreshEvery returns the effective board refresh cadence (see SetMarketsRefreshS).
// LOCKING (R85 root cause): this LOCKS mktMu — callers already holding mktMu MUST use
// refreshEveryLocked instead (calling this under the lock self-deadlocked the whole client:
// non-reentrant mutex parked forever holding mktMu — the 2026-07-05 universal REST wedge).
func (c *Client) refreshEvery() time.Duration {
	c.mktMu.Lock()
	v := c.mktRefreshS
	c.mktMu.Unlock()
	return clampRefreshS(v)
}

// refreshEveryLocked is refreshEvery for callers that ALREADY HOLD c.mktMu.
func (c *Client) refreshEveryLocked() time.Duration { return clampRefreshS(c.mktRefreshS) }

func clampRefreshS(v float64) time.Duration {
	if v <= 0 {
		return 4 * time.Second
	}
	if v < 2 {
		v = 2
	}
	if v > 60 {
		v = 60
	}
	return time.Duration(v * float64(time.Second))
}

// EventMeta is the slice of GET /events/{ticker} the dutch/sum-to-1 scanner needs (audit Q3):
// mutually_exclusive means AT MOST ONE of the event's markets settles YES — the precondition for
// the sell-all-outcomes lock (Σ yes_bid > 1 + fees) and, with exhaustiveness, the buy-all lock.
type EventMeta struct {
	Title                 string   `json:"title"`
	SubTitle              string   `json:"sub_title"`
	MutuallyExclusive     bool     `json:"mutually_exclusive"`
	FeeTypeOverride       string   `json:"fee_type_override"`
	FeeMultiplierOverride *float64 `json:"fee_multiplier_override"`
}

// GetEventMeta fetches an event's metadata (cache at the caller — this is a REST hit).
func (c *Client) GetEventMeta(ctx context.Context, eventTicker string) (EventMeta, error) {
	var resp struct {
		Event EventMeta `json:"event"`
	}
	if err := c.do(ctx, http.MethodGet, "/events/"+url.PathEscape(eventTicker), &resp, false); err != nil {
		return EventMeta{}, err
	}
	return resp.Event, nil
}

// SeriesFeeChange is one row of GET /series/fee_changes — the venue's own per-series fee
// schedule (R70-B SCHEMA_AUDIT #2; docs + live probe verified 2026-07-04; the AR §1.4
// "/margin/fee_tiers" endpoint does NOT exist in the docs — this is the documented one).
// Semantics (probed): fee_multiplier scales the BASE quadratic coefficient (taker 0.07,
// maker 0.0175), so multiplier 1 = standard fees and 0 = fee-free (the *PERP market-maker
// program series). fee_type "quadratic" charges takers only; "quadratic_with_maker_fees"
// charges makers too; "flat"/"margin_market_maker_program_fees" also appear. Rows are a
// CHANGE LOG — the latest row with scheduled_ts <= now is the effective schedule; a series
// with no row uses the defaults. Notably KXINX/KXNASDAQ100 flipped to multiplier 1 on
// 2026-07-03T17:00Z, ending the 0.035 index-halving era the old hardcoded class assumed.
type SeriesFeeChange struct {
	SeriesTicker  string  `json:"series_ticker"`
	FeeType       string  `json:"fee_type"`
	FeeMultiplier float64 `json:"fee_multiplier"`
	ScheduledTS   string  `json:"scheduled_ts"`
}

// EventFeeChange is one row of GET /events/fee_changes. Event overrides have higher precedence
// than the parent series. Nil override fields clear the event override and restore the series.
type EventFeeChange struct {
	EventTicker           string   `json:"event_ticker"`
	SeriesTicker          string   `json:"series_ticker"`
	FeeTypeOverride       *string  `json:"fee_type_override"`
	FeeMultiplierOverride *float64 `json:"fee_multiplier_override"`
	ScheduledTS           string   `json:"scheduled_ts"`
}

// EventFeeChanges fetches every event-level fee change, following the documented cursor. The
// endpoint currently returns a small set, but pagination is correctness—not an optimization.
func (c *Client) EventFeeChanges(ctx context.Context) ([]EventFeeChange, error) {
	var out []EventFeeChange
	cursor := ""
	for {
		path := "/events/fee_changes?limit=1000"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		var resp struct {
			Changes []EventFeeChange `json:"event_fee_changes"`
			Cursor  string           `json:"cursor"`
		}
		if err := c.do(ctx, http.MethodGet, path, &resp, false); err != nil {
			return nil, err
		}
		out = append(out, resp.Changes...)
		if resp.Cursor == "" || resp.Cursor == cursor {
			return out, nil
		}
		cursor = resp.Cursor
	}
}

// SeriesFeeChanges fetches the full per-series fee change log (public, no auth). Includes
// historical rows so callers can compute the CURRENTLY effective multiplier per series.
func (c *Client) SeriesFeeChanges(ctx context.Context) ([]SeriesFeeChange, error) {
	var resp struct {
		Changes []SeriesFeeChange `json:"series_fee_change_arr"`
	}
	if err := c.do(ctx, http.MethodGet, "/series/fee_changes?show_historical=true", &resp, false); err != nil {
		return nil, err
	}
	return resp.Changes, nil
}

// isBettable (R73, was isLiquidTradable) keeps every OPEN single-outcome market and drops only
// multi-leg "parlay"/combo markets (concatenated titles or KXMVE series). The old volume/OI
// requirement is GONE: a zero-volume rung of a 188-strike BTC hourly ladder or an unbet player
// prop is still something the operator can bet (post an order into) — "every single thing that
// can be bet on needs to be included". Volume ranking downstream keeps the active markets on
// top; auto-trading liquidity gates (LIQGATE spread/depth) still protect the bet paths.
func isBettable(m Market) bool {
	if strings.Count(m.Title, ",") >= 3 {
		return false
	}
	up := strings.ToUpper(m.EventTicker)
	tickerUp := strings.ToUpper(m.Ticker)
	if strings.Contains(up, "PERP") || strings.Contains(tickerUp, "PERP") {
		// Perpetual futures use margin/account-tier notional fees and non-binary payout rules.
		// Never let them enter the prediction-market universe or binary EV/settlement math.
		return false
	}
	if strings.HasPrefix(up, "KXMVE") || strings.Contains(up, "MULTIGAME") {
		return false
	}
	return true
}
