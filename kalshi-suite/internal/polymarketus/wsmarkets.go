package polymarketus

// Poly US markets WebSocket (wss://api.polymarket.us/v1/ws/markets): real-time book + executed trades.
// Auth = the SAME Ed25519 headers as REST, signing "<ts>GET/v1/ws/markets" (reuses authHeaders). This
// is the "no REST rate limit" live feed: MARKET_DATA_LITE gives live mid prices, TRADE gives the taker
// (aggressor) intent — BUY_LONG = aggressive buy YES, BUY_SHORT = aggressive buy NO = true flow.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const marketsWSURL = "wss://api.polymarket.us/v1/ws/markets"
const marketsWSPath = "/v1/ws/markets"

var (
	errNoSlugs                    = errors.New("no slugs to subscribe yet")
	errSubscriptionPlanSuperseded = errors.New("markets subscription plan superseded during reconciliation")
)

type pusLive struct {
	quoteAt         time.Time // last BBO update (full MARKET_DATA or MARKET_DATA_LITE)
	sourceAt        time.Time // source clock for the latest BBO (zero for LITE)
	bookSourceAt    time.Time // venue transactTime paired with bookAt/full depth
	fullLifecycleAt time.Time // last full MARKET_DATA frame carrying explicit lifecycle truth
	quoteGen        uint64    // socket generation that supplied bid/ask
	bookGen         uint64    // socket generation that supplied full depth
	priceGen        uint64    // socket generation that supplied the last price/trade observation
	stateGen        uint64    // socket generation that supplied explicit lifecycle state
	marketOpen      bool      // last explicit lifecycle state was OPEN
	stateKnown      bool      // absent lifecycle metadata never implies OPEN
	yes             float64
	buyYes          float64   // DECAYED taker BUY_LONG notional (aggressive buy YES) — see flowDecay
	buyNo           float64   // DECAYED taker BUY_SHORT notional (aggressive buy NO)
	flowAt          time.Time // last flow update (anchors the exponential decay)
	at              time.Time
	// rolling ~3-min price window for momentum (yes-first) + volatility (hi-lo), candlestick-style
	// but computed live off the WS price stream (no REST candle fetch).
	winStart      time.Time
	first, hi, lo float64
	bid, ask      float64 // latest live best bid/ask (full MARKET_DATA or LITE)
	// R70-B SCHEMA_AUDIT #1: per-level SIZES off the same MARKET_DATA frames (they were on the
	// wire since day 1, undecoded). Top-of-book resting size per side (maker fill-odds) and the
	// top-3 book shape (imb3 = bid share of top-3 depth, 0..1; depth3 = summed top-3 depth both
	// sides) — the polyus twin of Kalshi's kobImb cache, so book_imb3/book_depth3 stop logging
	// NULL for every polyus signal.
	bookBid, bookAsk float64 // BBO from the SAME full frame as bidSz/askSz (LITE must not relabel old depth)
	bidSz, askSz     float64 // resting size at the touch (best bid / best offer)
	bidLevels        []BookLevel
	askLevels        []BookLevel
	imb3             float64   // top-3-level bid share of (bid+ask) top-3 depth; -1 = top-3 empty/unknown
	depth3           float64   // summed top-3 resting depth, both sides
	bookAt           time.Time // local receipt time of the current generation's full MARKET_DATA snapshot
}

// BookLevel is one immutable price/quantity level from a full MARKET_DATA snapshot. The public
// accessor copies these slices so callers can integrate real depth without racing the WS writer.
type BookLevel struct {
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

// flowDecay applies the exponential flow decay (5-minute half-life) up to `now` (audit §6:
// buyYes/buyNo accumulated FOREVER, so FlowImbalance presented hours-old flow as a live signal —
// after decay, flow from 15 minutes ago carries ~12% of its original weight).
func (e *pusLive) flowDecay(now time.Time) {
	if e.flowAt.IsZero() {
		e.flowAt = now
		return
	}
	if dt := now.Sub(e.flowAt); dt > 0 {
		k := math.Pow(0.5, dt.Minutes()/5)
		e.buyYes *= k
		e.buyNo *= k
	}
	e.flowAt = now
}

// notePx records a price into the rolling momentum/volatility window (resets every ~3 min).
func (e *pusLive) notePx(px float64, now time.Time) {
	if px <= 0 {
		return
	}
	if e.winStart.IsZero() || now.Sub(e.winStart) > 3*time.Minute {
		e.winStart, e.first, e.hi, e.lo = now, px, px, px
	} else {
		if px > e.hi {
			e.hi = px
		}
		if px < e.lo {
			e.lo = px
		}
	}
	e.yes = px
	e.at = now
}

// pusTrade is one executed taker print (the aggressor side), kept in a rolling buffer so the
// dashboard can show recent aggressive-money totals + a Poly US "whales" panel (large prints).
// Poly US is a regulated exchange (anonymous, like Kalshi) — so these are sizes, not wallets.
type pusTrade struct {
	slug     string
	yesSide  bool // true = aggressive BUY YES (bullish), false = aggressive BUY NO (bearish)
	notional float64
	px       float64 // raw venue print (legacy display)
	yesPx    float64 // normalized YES-space execution price
	at       time.Time
}

// WhalePrint is a single large Poly US taker print surfaced to the dashboard whales panel.
type WhalePrint struct {
	Slug     string  `json:"slug"`
	Side     string  `json:"side"` // YES / NO (the aggressor's direction)
	Notional float64 `json:"notional"`
	Price    float64 `json:"price"`
	At       int64   `json:"at"` // unix seconds
}

// RawFlowPrint is one valid positive taker print from the full rolling PolyUS tape. Unlike Whales,
// RawFlow applies no dollar threshold and exists for relative-size research/ML observation only.
// The underlying buffer remains bounded, so this cannot recreate the former subscription flood.
type RawFlowPrint struct {
	Slug     string    `json:"slug"`
	Side     string    `json:"side"`
	Action   string    `json:"action"`
	Notional float64   `json:"notional"`
	Price    float64   `json:"price"`
	YesPrice float64   `json:"yes_price"`
	At       time.Time `json:"at"`
}

// Trade-print denomination modes (R102, auditor bug 243): whether the venue's TRADE px for a
// NO-side print is the traded OUTCOME's price (a NO buy @97¢ arrives as 0.97) or always YES-space.
// One of momentum (notePx) or NO-flow notional is wrong depending on the answer, so the mode is
// decided by LIVE EVIDENCE (denomVote below) and latches once conclusive. Unknown = legacy behavior.
const (
	denomUnknown = iota
	denomOutcome // px is outcome-denominated: NO prints normalize px→1−px for the YES momentum window; px·qty already = real dollars
	denomYes     // px is YES-denominated: momentum was fine; NO-flow notional becomes (1−px)·qty (the dollars the NO buyer paid)
)

// MarketsWS maintains a live cache (mid price + taker-flow) from the Poly US markets WebSocket.
type MarketsWS struct {
	c    *Client
	mu   sync.Mutex
	live map[string]*pusLive
	// Complete REST-universe replacements prune terminal/removed cache entries. Without this,
	// each expired slug lived forever and a long-running process grew monotonically.
	cachePruned atomic.Int64
	// Immutable per-process ceiling for expensive MARKET_DATA subscriptions. The server planner
	// reads this same value through FullBookCap so its priority/readiness receipts cannot drift
	// from the already-created socket after a config reload.
	fullBookCap int
	// The last COMPLETE strict /v1/markets crawl. MARKET_DATA_LITE usually omits lifecycle, so its
	// fresh BBO is executable only while the independent REST snapshot proves the slug open.
	// Replacement is authoritative: a missing slug immediately invalidates any cached quote.
	restOpen   map[string]bool
	restOpenAt time.Time
	trades     []pusTrade                     // rolling recent taker prints (aggressive-money + whales panels)
	onMsg      func([]byte)                   // optional raw-message hook (used by the verify probe); guarded by mu (R102, bug 250)
	tickFn     func(slug string, yes float64) // R40: per-update hook for event-driven 1¢ cancels
	// R102 (bug 242) observability: what the last subscribe actually requested (slugs + frames),
	// so the server can prove the full prioritized set went out (the old code silently sent 100).
	subSlugs  atomic.Int64
	subFrames atomic.Int64
	subAt     atomic.Int64 // unix seconds
	subFull   atomic.Int64 // priority slugs receiving full depth + lifecycle snapshots
	subLite   atomic.Int64 // complete universe receiving lightweight BBO updates
	subTrade  atomic.Int64 // complete universe receiving taker-trade updates
	// Exact membership authority for the active generation. Counts alone cannot distinguish two
	// equal-sized universes, so promotion/readiness stays fail-closed until the active session's
	// successfully installed request map hashes to this exact expected value. Guarded by mu.
	expectedSubGen  uint64
	expectedSubHash string
	// Exact full MARKET_DATA membership installed on the authoritative primary. A cached ladder is
	// never execution authority merely because its socket generation is still alive: hot-priority
	// rotation can unsubscribe that slug while LITE+TRADE remain active on the same connection.
	activeFullGen uint64
	activeFull    map[string]bool
	// Primary connection receipts. frameAt includes heartbeats, pongs and subscription responses;
	// dataAt is only MARKET_DATA/LITE/TRADE. The watchdog uses dataAt (a heartbeat-alive dark
	// subscription is still broken). Each connected session receives a monotonic generation. Cached
	// snapshots are usable only while their generation is the healthy active primary generation:
	// an unchanged complete snapshot may remain quiet at runtime, but reconnect/promotion invalidates
	// it at once.
	primaryFrameAt atomic.Int64 // unix nanoseconds
	primaryDataAt  atomic.Int64 // unix nanoseconds
	primarySince   atomic.Int64 // unix nanoseconds; initial-data grace for a new primary
	sessionSeq     atomic.Uint64
	activeGen      atomic.Uint64
	primaryStarts  atomic.Int64
	primarySwitch  atomic.Int64
	disconnects    atomic.Int64
	lastCloseAt    time.Time // guarded by mu
	lastCloseErr   string    // guarded by mu
	// Full MARKET_DATA has a venue transactTime; LITE is arrival-clock only. Keep the clocks
	// separate so an all-board LITE stream cannot make the source-timestamp contract look healthy.
	fullSourceAt atomic.Int64
	fullFrames   atomic.Int64
	liteAt       atomic.Int64
	liteFrames   atomic.Int64
	// R102 (bug 243) denomination evidence: votes accumulate on NO-side prints that land while the
	// full-book mid is fresh; the mode latches at ≥25 votes with ≥80% agreement.
	denomOutV, denomYesV int
	denomMode            int
	// R106 (bug 255): single-slug two-sided instruments (World Cup "to advance", live Jul-9). Their
	// short-team prints are a different shape from legacy NO prints — they are EXCLUDED from the
	// global denomination vote (a WC-heavy boot must not latch the venue-wide mode off them) and
	// disambiguated PER PRINT against their own fresh book mid instead.
	twoSided map[string]bool
	// R108 WS staleness watchdog: closure set by Stream that force-closes the CURRENT primary
	// socket (onDead then promotes the freshly-resubscribed standby — the normal healing path).
	kickMu  sync.Mutex
	kickFn  func() bool
	kickGen uint64 // identifies the Stream instance that owns kickFn; teardown cannot clear a newer owner
	// A complete REST crawl can discover newly-open markets between normal five-minute in-place
	// reconciliations. addFn appends only those proven-open slugs to both live sessions immediately.
	addMu  sync.Mutex
	addFn  func([]string) (int, bool)
	addGen uint64
	// Fresh LIVE signals may land outside the bounded full-depth prefix even though the prioritized
	// LITE stream already sees them. A small expiring hot set replaces only the tail of the existing
	// full-depth band; it never increases the configured full-book ceiling. promoteFn wakes the
	// Stream owner so both primary and standby reconcile immediately without blocking the signal
	// producer on socket I/O.
	hotFull    map[string]time.Time
	promoteMu  sync.Mutex
	promoteFn  func() bool
	promoteGen uint64
	// dialFn is a focused test seam for Stream lifecycle coverage. Production always leaves it nil
	// and uses dialSession; keeping the seam here lets cancellation prove both sockets close without
	// reaching the real venue.
	dialFn        func(context.Context, []string) (*pusSession, error)
	rotationEvery time.Duration // focused test seam; production reconciles both sessions every five minutes
}

// ForceReconnect (R108) kills the current primary socket so a wedged-but-ponging session is
// replaced through the standard onDead/promote/resub path. Returns false when not streaming.
func (ws *MarketsWS) ForceReconnect() bool {
	ws.kickMu.Lock()
	f := ws.kickFn
	ws.kickMu.Unlock()
	if f == nil {
		return false
	}
	return f()
}

// SubscribeNewOpenSlugs immediately adds independently REST-proven open slugs to the current
// primary and warm standby's lightweight-book and trade subscriptions. It never adds full depth;
// the bounded priority band is refreshed by the normal in-place reconciliation. The per-session fixed
// chunk IDs replace a partial tail chunk, so an incremental refresh cannot duplicate trade feeds.
func (ws *MarketsWS) SubscribeNewOpenSlugs(slugs []string) (int, bool) {
	if ws == nil {
		return 0, false
	}
	ws.mu.Lock()
	proven := make([]string, 0, len(slugs))
	seen := make(map[string]bool, len(slugs))
	for _, slug := range slugs {
		slug = strings.ToLower(strings.TrimSpace(slug))
		if slug != "" && ws.restOpen[slug] && !seen[slug] {
			seen[slug] = true
			proven = append(proven, slug)
		}
	}
	ws.mu.Unlock()
	if len(proven) == 0 {
		return 0, false
	}
	ws.addMu.Lock()
	f := ws.addFn
	ws.addMu.Unlock()
	if f == nil {
		return 0, false
	}
	return f(proven)
}

const (
	marketsHotFullBookTTL = 3 * time.Minute
	marketsHotFullBookCap = 64
)

// PromoteFullBook gives one independently REST-proven OPEN market an immediate, bounded seat in
// the full MARKET_DATA band. It is intentionally nonblocking: the caller only updates an in-memory
// hint and wakes the Stream owner; the socket writer remains single-owner and coalesced.
func (ws *MarketsWS) PromoteFullBook(slug string) bool {
	if ws == nil {
		return false
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	if slug == "" {
		return false
	}
	now := time.Now()
	ws.mu.Lock()
	restAge := now.Sub(ws.restOpenAt)
	if ws.restOpenAt.IsZero() || restAge < 0 || restAge > marketsRESTLifecycleMaxAge ||
		!ws.restOpen[slug] {
		ws.mu.Unlock()
		return false
	}
	if ws.hotFull == nil {
		ws.hotFull = make(map[string]time.Time, marketsHotFullBookCap)
	}
	for id, at := range ws.hotFull {
		if now.Before(at) || now.Sub(at) > marketsHotFullBookTTL || !ws.restOpen[id] {
			delete(ws.hotFull, id)
		}
	}
	if _, exists := ws.hotFull[slug]; !exists && len(ws.hotFull) >= marketsHotFullBookCap {
		oldestSlug, oldestAt := "", now
		for id, at := range ws.hotFull {
			if oldestSlug == "" || at.Before(oldestAt) ||
				(at.Equal(oldestAt) && id < oldestSlug) {
				oldestSlug, oldestAt = id, at
			}
		}
		delete(ws.hotFull, oldestSlug)
	}
	ws.hotFull[slug] = now
	ws.mu.Unlock()

	ws.promoteMu.Lock()
	f := ws.promoteFn
	ws.promoteMu.Unlock()
	return f != nil && f()
}

// NewMarketsWS retains the conservative default for probes/tests and callers without suite config.
func (c *Client) NewMarketsWS() *MarketsWS {
	return c.NewMarketsWSWithFullBookCap(DefaultMarketsFullBookCap)
}

// NewMarketsWSWithFullBookCap creates a markets socket with one immutable, safely clamped
// full-depth ceiling. Lightweight BBO and TRADE subscriptions cover the prioritized live set.
func (c *Client) NewMarketsWSWithFullBookCap(cap int) *MarketsWS {
	return &MarketsWS{c: c, live: map[string]*pusLive{}, fullBookCap: ClampMarketsFullBookCap(cap)}
}

// FullBookCap is the effective wire ceiling and the source of truth for server planning/readiness.
func (ws *MarketsWS) FullBookCap() int {
	if ws == nil {
		return DefaultMarketsFullBookCap
	}
	return ClampMarketsFullBookCap(ws.fullBookCap)
}

// SetOnMsg registers a raw-message hook (used by the verify probe). Optional.
// R102 (auditor bug 250, P4): the write is now mu-guarded (it used to race the runSession read —
// safe only by call order); the read side snapshots under the same lock.
func (ws *MarketsWS) SetOnMsg(f func([]byte)) {
	ws.mu.Lock()
	ws.onMsg = f
	ws.mu.Unlock()
}

// SetTwoSidedSlugs replaces the set of single-slug two-sided instruments (R106 bug 255). Called
// from the server's PolyUS refresh with venue-metadata truth (Market.TwoSidedSingle/IsToAdvance) —
// never inferred from slugs/titles here.
func (ws *MarketsWS) SetTwoSidedSlugs(slugs []string) {
	m := make(map[string]bool, len(slugs))
	for _, s := range slugs {
		if s = strings.ToLower(strings.TrimSpace(s)); s != "" {
			m[s] = true
		}
	}
	ws.mu.Lock()
	ws.twoSided = m
	ws.mu.Unlock()
}

const (
	MarketsRESTLifecycleMaxAge = 5 * time.Minute // shared with server readiness; must match executable lite-quote proof
	marketsRESTLifecycleMaxAge = MarketsRESTLifecycleMaxAge
	// Official docs define each MARKET_DATA message as a full order book. Runtime receipts show that
	// unchanged books may remain quiet, so a snapshot does not become false after an arbitrary quote
	// TTL. A broad primary must still deliver some real market payload within a bounded global window:
	// heartbeats alone cannot keep a dark subscription authoritative forever. Six minutes allows the
	// five-minute same-session reconciliation one full minute to produce a payload before books fail
	// closed. Three missed 15s transport frames remains the faster disconnected-socket ceiling; the
	// order path still performs its independent final check.
	MarketsWSPrimaryDataMaxAge      = 6 * time.Minute
	MarketsWSPrimaryTransportMaxAge = 45 * time.Second
	marketsWSPrimaryDataMaxAge      = MarketsWSPrimaryDataMaxAge
	marketsWSPrimaryTransportMaxAge = MarketsWSPrimaryTransportMaxAge
)

// SetOpenSlugs replaces the independently REST-proven open universe. The caller invokes this only
// after a complete, lifecycle-validated crawl; partial/error crawls must never reach this method.
// An empty but authoritative set is allowed and invalidates every lite-only executable quote.
func (ws *MarketsWS) SetOpenSlugs(slugs []string) {
	open := make(map[string]bool, len(slugs))
	for _, slug := range slugs {
		if slug = strings.ToLower(strings.TrimSpace(slug)); slug != "" {
			open[slug] = true
		}
	}
	ws.mu.Lock()
	ws.restOpen = open
	ws.restOpenAt = time.Now()
	for slug := range ws.live {
		if !open[slug] {
			delete(ws.live, slug)
			ws.cachePruned.Add(1)
		}
	}
	for slug := range ws.hotFull {
		if !open[slug] {
			delete(ws.hotFull, slug)
		}
	}
	ws.mu.Unlock()
}

// OpenSlugStats is the lifecycle-proof receipt for observability/tests.
func (ws *MarketsWS) OpenSlugStats() (count int, at time.Time) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return len(ws.restOpen), ws.restOpenAt
}

// CacheStats reports bounded-cache evidence. A completed lifecycle replacement prunes terminal
// entries immediately; late frames for removed slugs remain non-authoritative.
func (ws *MarketsWS) CacheStats() (live, pruned int64) {
	ws.mu.Lock()
	live = int64(len(ws.live))
	ws.mu.Unlock()
	return live, ws.cachePruned.Load()
}

// MarketsWSConnectionReceipt makes otherwise-silent connection handoffs auditable without logging
// credentials or raw payloads. QuietExecutable counts valid current-generation BBOs whose last
// change was over 30s ago: these are the books the former wall-clock gate incorrectly discarded.
type MarketsWSConnectionReceipt struct {
	ActiveGeneration uint64
	Sessions         uint64
	PrimarySwitches  int64
	Disconnects      int64
	QuietExecutable  int
	DataSeen         bool
	LastCloseAt      time.Time
	LastCloseError   string
}

func (ws *MarketsWS) ConnectionReceipt() MarketsWSConnectionReceipt {
	if ws == nil {
		return MarketsWSConnectionReceipt{}
	}
	now := time.Now()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	r := MarketsWSConnectionReceipt{
		ActiveGeneration: ws.activeGen.Load(), Sessions: ws.sessionSeq.Load(),
		PrimarySwitches: ws.primarySwitch.Load(), Disconnects: ws.disconnects.Load(),
		DataSeen: ws.primaryDataAt.Load() > 0, LastCloseAt: ws.lastCloseAt,
		LastCloseError: ws.lastCloseErr,
	}
	for slug, e := range ws.live {
		if e != nil && !e.quoteAt.IsZero() && now.Sub(e.quoteAt) > 30*time.Second &&
			ws.executableQuoteFreshLocked(slug, e, now) {
			r.QuietExecutable++
		}
	}
	return r
}

// TwoSidedStats reports the current two-sided set size (observability for /api/pxage + tests).
func (ws *MarketsWS) TwoSidedStats() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return len(ws.twoSided)
}

// Stream keeps TWO markets sockets open (R42 WARM-STANDBY, same pattern as the Kalshi WS): both
// stay subscribed, only the PRIMARY ingests. A healthy primary retains its generation and cached
// executable books while both sessions reconcile the exact current universe in place. Only a real
// primary failure promotes the standby and invalidates the failed generation.
func (ws *MarketsWS) Stream(ctx context.Context, slugsFn func() []string) {
	var mu sync.Mutex
	var addCallMu sync.Mutex
	var reconcileWG sync.WaitGroup
	var cur, next *pusSession
	stopping := false
	promoteCh := make(chan struct{}, 1)
	var spawn func(asPrimary bool)
	// Reconciliation never holds Stream's role lock across network I/O. Repeated ticks for one
	// session coalesce behind one bounded writer; a failed write closes that socket and enters the
	// normal replacement path. An empty transient plan keeps the last proven subscription.
	resub := func(s *pusSession, plan []string) {
		if s == nil {
			return
		}
		plan = append([]string(nil), plan...)
		if s.primary.Load() {
			ws.expectPrimarySubscriptionPlan(s, plan)
		}
		if !s.reconciling.CompareAndSwap(false, true) {
			s.reconcilePending.Store(true)
			return
		}
		reconcileWG.Add(1)
		go func() {
			defer reconcileWG.Done()
			currentPlan := plan
			for {
				if ctx.Err() != nil {
					s.reconciling.Store(false)
					return
				}
				if s.primary.Load() {
					ws.expectPrimarySubscriptionPlan(s, currentPlan)
				}
				if err := ws.subscribeMarkets(s, currentPlan); err != nil && !errors.Is(err, errNoSlugs) {
					s.reconciling.Store(false)
					if s.conn != nil {
						_ = s.conn.Close()
					}
					return
				}
				if s.reconcilePending.Swap(false) {
					currentPlan = slugsFn()
					continue
				}
				s.reconciling.Store(false)
				// Close the narrow release race: a caller that observed reconciling=true just before
				// the Store above left a pending bit. Reacquire this same bounded worker when no
				// newer worker already won the slot.
				if !s.reconcilePending.Swap(false) ||
					!s.reconciling.CompareAndSwap(false, true) {
					return
				}
				currentPlan = slugsFn()
			}
		}()
	}
	onDead := func(s *pusSession) {
		mu.Lock()
		defer mu.Unlock()
		if stopping {
			return // cancellation owns teardown; never refill a slot after it detached the sessions
		}
		switch s {
		case cur:
			s.primary.Store(false)
			if next != nil { // promote the warm standby
				cur = next
				next = nil
				ws.notePrimaryStart(cur.gen)
				cur.primary.Store(true)
				resub(cur, slugsFn()) // promoted socket must be provably on the current exact plan
				go spawn(false)
			} else {
				cur = nil
				go spawn(true)
			}
		case next:
			next = nil
			go spawn(false)
		}
		// A detached/dead socket is neither cur nor next, so duplicate close notification is a no-op.
	}
	spawn = func(asPrimary bool) {
		backoff := 2 * time.Second
		dial := ws.dialSession
		if ws.dialFn != nil {
			dial = ws.dialFn
		}
		for {
			if ctx.Err() != nil {
				return
			}
			plan := slugsFn()
			s, err := dial(ctx, plan)
			if err == nil {
				if s.gen == 0 { // production and focused dial seams share one monotonic authority
					s.gen = ws.sessionSeq.Add(1)
				}
				mu.Lock()
				placed := false
				if stopping || ctx.Err() != nil {
					// Cancellation can win after DialContext succeeds but before slot placement.
					// Refusing the placement closes that narrow orphan-session race.
				} else if asPrimary && cur == nil {
					cur = s
					ws.notePrimaryStart(s.gen)
					s.primary.Store(true)
					ws.expectPrimarySubscriptionPlan(s, plan)
					ws.publishSessionCoverage(s)
					placed = true
				} else if next == nil && (!asPrimary || cur != nil) {
					next = s
					placed = true
				}
				mu.Unlock()
				if !placed {
					_ = s.conn.Close()
					return
				}
				go ws.runSession(ctx, s, onDead)
				return
			}
			if backoff < 60*time.Second {
				backoff *= 2
			}
			jitter := time.Duration(time.Now().UnixNano() % int64(backoff/2))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff/2 + jitter):
			}
		}
	}
	// R108 staleness watchdog hook: closing the primary makes its read loop error → onDead
	// promotes the standby (resub re-proves the subscription) → spawn refills. Exactly the
	// existing failure path, triggered deliberately.
	ws.kickMu.Lock()
	ws.kickGen++
	streamKickGen := ws.kickGen
	ws.kickFn = func() bool {
		mu.Lock()
		defer mu.Unlock()
		if stopping {
			return false
		}
		s := cur
		if s == nil || s.conn == nil {
			return false
		}
		// Close while the role lock still pins `cur`: otherwise a simultaneous real socket death
		// can promote `next` between the snapshot and Close and make this kick report false success.
		_ = s.conn.Close()
		return true
	}
	ws.kickMu.Unlock()
	ws.addMu.Lock()
	ws.addGen++
	streamAddGen := ws.addGen
	ws.addFn = func(slugs []string) (int, bool) {
		addCallMu.Lock()
		defer addCallMu.Unlock()
		mu.Lock()
		if stopping || cur == nil {
			mu.Unlock()
			return 0, false
		}
		sessions := []*pusSession{cur}
		if next != nil && next != cur {
			sessions = append(sessions, next)
		}
		mu.Unlock()
		type result struct {
			added, frames int
			err           error
		}
		results := make(map[*pusSession]result, len(sessions))
		for _, s := range sessions {
			a, f, err := ws.subscribeNewMarkets(s, slugs)
			results[s] = result{a, f, err}
			if err != nil && s.conn != nil {
				_ = s.conn.Close()
			}
		}
		mu.Lock()
		primary := cur
		active := !stopping && primary != nil
		mu.Unlock()
		if !active {
			return 0, false
		}
		r, wrote := results[primary]
		if !wrote || r.err != nil {
			return 0, false // a newly-dialed replacement already received slugsFn's complete set
		}
		ws.publishSessionCoverage(primary)
		return r.added, true
	}
	ws.addMu.Unlock()
	ws.promoteMu.Lock()
	ws.promoteGen++
	streamPromoteGen := ws.promoteGen
	ws.promoteFn = func() bool {
		select {
		case promoteCh <- struct{}{}:
		default:
		}
		return true
	}
	ws.promoteMu.Unlock()
	defer func() {
		// Mark stopping and detach BOTH role slots under the same lock used by spawn/onDead. This
		// prevents a successful in-flight dial or a read-loop death from repopulating after cancel.
		mu.Lock()
		stopping = true
		oldCur, oldNext := cur, next
		cur, next = nil, nil
		if oldCur != nil {
			oldCur.primary.Store(false)
		}
		if oldNext != nil {
			oldNext.primary.Store(false)
		}
		mu.Unlock()
		ws.mu.Lock()
		if oldCur != nil && ws.activeGen.Load() == oldCur.gen {
			ws.activeGen.Store(0)
			ws.expectedSubGen = 0
			ws.expectedSubHash = ""
			ws.activeFullGen = 0
			ws.activeFull = nil
			ws.primarySince.Store(0)
			ws.primaryFrameAt.Store(0)
			ws.primaryDataAt.Store(0)
			ws.fullSourceAt.Store(0)
			ws.liteAt.Store(0)
		}
		ws.mu.Unlock()
		if oldCur != nil && oldCur.conn != nil {
			_ = oldCur.conn.Close()
		}
		if oldNext != nil && oldNext != oldCur && oldNext.conn != nil {
			_ = oldNext.conn.Close()
		}
		ws.clearPublishedSubscriptionCoverage()
		// Every reconcile is write-deadline bounded and coalesced, so teardown can safely wait for
		// the small tracked set instead of leaking subscription writers into a later Stream.
		reconcileWG.Wait()

		// Clear only this Stream's watchdog closure. A later Stream (defensive restart/test case)
		// may already own a newer generation.
		ws.kickMu.Lock()
		if ws.kickGen == streamKickGen {
			ws.kickFn = nil
		}
		ws.kickMu.Unlock()
		ws.addMu.Lock()
		if ws.addGen == streamAddGen {
			ws.addFn = nil
		}
		ws.addMu.Unlock()
		ws.promoteMu.Lock()
		if ws.promoteGen == streamPromoteGen {
			ws.promoteFn = nil
		}
		ws.promoteMu.Unlock()
	}()
	spawn(true)
	go spawn(false)
	rotationEvery := ws.rotationEvery
	if rotationEvery <= 0 {
		rotationEvery = 5 * time.Minute
	}
	rot := time.NewTicker(rotationEvery) // same-generation subscription reconciliation
	defer rot.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-promoteCh:
			mu.Lock()
			if stopping {
				mu.Unlock()
				return
			}
			primary, standby := cur, next
			mu.Unlock()
			plan := slugsFn()
			if primary != nil {
				resub(primary, plan)
			}
			if standby != nil && standby != primary {
				resub(standby, plan)
			}
		case <-rot.C:
			mu.Lock()
			if stopping {
				mu.Unlock()
				return
			}
			primary, standby := cur, next
			mu.Unlock()
			plan := slugsFn()
			// R145: a healthy primary keeps its generation. The former five-minute role swap
			// invalidated every otherwise-current cached BBO while the promoted standby (whose
			// frames are deliberately drained, not ingested) waited for a market to change. That
			// produced recurring red PolyUS Books intervals with healthy transports. Refresh the
			// complete subscription plan on both sockets in place instead. The standby is promoted
			// only after a real primary failure, where generation invalidation is required.
			if primary != nil {
				resub(primary, plan)
			}
			if standby != nil && standby != primary {
				resub(standby, plan)
			}
		}
	}
}

// pusSession is one live markets socket + its role (primary ingests; standby drains).
type pusSession struct {
	conn             *websocket.Conn
	gen              uint64 // immutable connection generation; assigned before the read loop starts
	primary          atomic.Bool
	reconciling      atomic.Bool // one bounded in-place reconcile per session; later ticks coalesce
	reconcilePending atomic.Bool // a hot promotion arriving mid-reconcile gets one immediate replay
	writeMu          sync.Mutex  // gorilla permits only one concurrent data writer
	wide             []string
	wideSet          map[string]bool
	requests         map[string]marketSubscriptionPlan // exact active request-ID membership; guarded by writeMu
	planHash         string                            // hash of requests after the last complete successful mutation
	fullSet          map[string]bool                   // exact active MARKET_DATA slugs; guarded by writeMu
	full             int                               // active full-depth slug count; guarded by writeMu
	frames           int                               // active subscription request count; guarded by writeMu
	subAt            time.Time                         // last successful plan mutation; guarded by writeMu
	writeBy          time.Time                         // whole-plan deadline; guarded by writeMu
	writeJSON        func(any) error                   // focused test seam; production writes conn.WriteJSON
}

// A dead peer must not hold a session's single data-writer lock forever and accumulate a new
// reconcile goroutine every five minutes. Gorilla's control ping has its own five-second deadline;
// all JSON subscription/heartbeat writes use this independent bounded deadline.
const (
	marketsWSDataWriteTimeout = 10 * time.Second
	marketsWSPlanWriteTimeout = 20 * time.Second
)

// dialSession dials + authenticates + subscribes one markets socket (primary and standby alike).
func (ws *MarketsWS) dialSession(ctx context.Context, slugs []string) (*pusSession, error) {
	if len(slugs) == 0 {
		return nil, errNoSlugs // nothing to subscribe yet — spawn's backoff will retry
	}
	h := http.Header{}
	for k, v := range ws.c.authHeaders("GET", marketsWSPath) { // Ed25519 handshake auth
		h.Set(k, v)
	}
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, resp, err := dialer.DialContext(ctx, marketsWSURL, h)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close() // R102 (auditor bug 248, P4): gorilla hands back the handshake response on failure — close it or leak the fd (bounded by backoff, but real)
		}
		return nil, err
	}
	s := &pusSession{conn: conn}
	if err := ws.subscribeMarkets(s, slugs); err != nil {
		// A dropped subscribe write is NOT survivable: the socket stays up and PONGS (so the read
		// loop never dies) but the server was never told to send anything — a connected-but-dark
		// session the redial path can't see. Treat it exactly like a failed dial so spawn's backoff
		// retries with a fresh socket.
		_ = conn.Close()
		return nil, err
	}
	// KEEPALIVE (audit §6): no ping = up to a minute of dead feed on a silent NAT/proxy drop.
	conn.SetReadLimit(1 << 21) // 2MB frame cap (audit §6)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		if s.primary.Load() {
			ws.notePrimaryTransport(s.gen)
		}
		return nil
	})
	return s, nil
}

// marketsSubChunk is the venue's documented per-SUBSCRIPTION FRAME slug ceiling (≤100
// market_slugs). Full depth uses a configured priority prefix (600 by default, safely capped at
// 1,000): requesting full depth for ~9k markets causes a huge initial snapshot flood and socket
// churn. Lightweight BBO and trade subscriptions still cover the complete universe in fixed chunks.
const (
	marketsSubChunk           = 100
	MinMarketsFullBookCap     = 50
	DefaultMarketsFullBookCap = 600
	MaxMarketsFullBookCap     = 1000
	// Keep the real-time socket focused on the markets the planner ranks highest. The open
	// catalog can be much larger and still remains available through the REST snapshot.
	DefaultMarketsWideBookCap = 10000
	// Compatibility alias for older tools/tests. New code must use FullBookCap on the actual
	// MarketsWS instance so configured wire and planner caps cannot drift.
	MarketsFullBookCap = DefaultMarketsFullBookCap
	marketsFullBookCap = MarketsFullBookCap
)

// ClampMarketsFullBookCap is shared by socket construction, priority planning and readiness.
// Non-positive values select the conservative default.
func ClampMarketsFullBookCap(v int) int {
	if v <= 0 {
		v = DefaultMarketsFullBookCap
	}
	if v < MinMarketsFullBookCap {
		return MinMarketsFullBookCap
	}
	if v > MaxMarketsFullBookCap {
		return MaxMarketsFullBookCap
	}
	return v
}

// marketSubFrames builds the chunked subscription frames for a slug set — R102 (auditor bug 242,
// P2): the old code sent ONE frame silently truncated to slugs[:100], so ~92% of the server's
// 1200-slug prioritized set got no ticks (no flow/momentum/book3 features; the event-driven
// 1¢-cancel hook fell back to the 429-constrained REST sweep) and the 5-min reconcile re-subscribed
// the same first-100 forever. The per-frame limit is handled with fixed per-chunk request IDs:
// md-* for priority full depth, mdl-* for full-universe BBO, and trade-* for full-universe tape.
// A promotion/resub therefore re-addresses the same server-side subscriptions.
// Pure function so tests can pin the chunking without a socket.
func marketSubFrames(slugs []string) []map[string]any {
	return marketSubFramesForCap(slugs, DefaultMarketsFullBookCap)
}

// marketSubFramesForCap is the configurable pure form used by the live socket and capacity tests.
func marketSubFramesForCap(slugs []string, fullBookCap int) []map[string]any {
	return marketSubFramesForSets(slugs, slugs, fullBookCap)
}

// marketSubFramesForSets builds the exact current request-ID plan. Reconciliation replaces reused
// chunks and explicitly unsubscribes request IDs beyond the new tail, so terminal markets cannot
// accumulate forever on a healthy long-lived session. The wide set is bounded before frames are
// built; a 70,000-market catalog otherwise leaves the connection alive but unable to deliver data.
func marketSubFramesForSets(priority, wide []string, fullBookCap int) []map[string]any {
	fullBookCap = ClampMarketsFullBookCap(fullBookCap)
	if len(wide) > DefaultMarketsWideBookCap {
		wide = wide[:DefaultMarketsWideBookCap]
	}
	fullN := len(priority)
	if fullN > fullBookCap {
		fullN = fullBookCap
	}
	wideFrames := (len(wide) + marketsSubChunk - 1) / marketsSubChunk
	fullFrames := (fullN + marketsSubChunk - 1) / marketsSubChunk
	frames := make([]map[string]any, 0, 2*wideFrames+fullFrames)
	add := func(prefix string, subType int, set []string) {
		for i := 0; i*marketsSubChunk < len(set); i++ {
			lo, hi := i*marketsSubChunk, (i+1)*marketsSubChunk
			if hi > len(set) {
				hi = len(set)
			}
			frames = append(frames, map[string]any{"subscribe": map[string]any{
				"request_id": fmt.Sprintf("%s-%d", prefix, i), "subscription_type": subType,
				"market_slugs": set[lo:hi],
			}})
		}
	}
	// Full depth for the priority band first. Lightweight BBO and trades cover the first 10,000
	// planner-ranked markets, while REST keeps the complete open catalog available.
	add("md", 1, priority[:fullN])
	add("mdl", 2, wide)
	add("trade", 3, wide)
	return frames
}

func normalizeMarketSlugs(slugs []string) []string {
	out := make([]string, 0, len(slugs))
	seen := make(map[string]bool, len(slugs))
	for _, slug := range slugs {
		slug = strings.ToLower(strings.TrimSpace(slug))
		if slug != "" && !seen[slug] {
			seen[slug] = true
			out = append(out, slug)
		}
	}
	return out
}

// fullPriorityWithHot preserves the stable base prefix and replaces only its tail with hot markets
// that would otherwise sit outside the configured full-depth band. The complete LITE/TRADE order is
// unchanged. Keeping replacements at the tail limits an ordinary one-signal promotion to one
// md-* unsubscribe/subscribe pair instead of shifting every 100-market chunk.
func fullPriorityWithHot(priority []string, fullBookCap int, hot []string) []string {
	priority = normalizeMarketSlugs(priority)
	fullBookCap = ClampMarketsFullBookCap(fullBookCap)
	if len(priority) <= fullBookCap {
		return append([]string(nil), priority...)
	}
	base := append([]string(nil), priority[:fullBookCap]...)
	inBase := make(map[string]bool, len(base))
	inWide := make(map[string]bool, len(priority))
	for _, slug := range priority {
		inWide[slug] = true
	}
	for _, slug := range base {
		inBase[slug] = true
	}
	replacements := make([]string, 0, len(hot))
	seen := make(map[string]bool, len(hot))
	for _, slug := range hot {
		slug = strings.ToLower(strings.TrimSpace(slug))
		if slug == "" || seen[slug] || inBase[slug] || !inWide[slug] {
			continue
		}
		seen[slug] = true
		replacements = append(replacements, slug)
		if len(replacements) >= fullBookCap {
			break
		}
	}
	if len(replacements) == 0 {
		return base
	}
	keep := fullBookCap - len(replacements)
	out := append([]string(nil), base[:keep]...)
	return append(out, replacements...)
}

func (ws *MarketsWS) fullPriority(priority []string, now time.Time) []string {
	priority = normalizeMarketSlugs(priority)
	if ws == nil {
		return fullPriorityWithHot(priority, DefaultMarketsFullBookCap, nil)
	}
	type hint struct {
		slug string
		at   time.Time
	}
	ws.mu.Lock()
	hints := make([]hint, 0, len(ws.hotFull))
	restAge := now.Sub(ws.restOpenAt)
	restFresh := !ws.restOpenAt.IsZero() && restAge >= 0 &&
		restAge <= marketsRESTLifecycleMaxAge
	for slug, at := range ws.hotFull {
		if !restFresh || now.Before(at) || now.Sub(at) > marketsHotFullBookTTL ||
			!ws.restOpen[slug] {
			delete(ws.hotFull, slug)
			continue
		}
		hints = append(hints, hint{slug: slug, at: at})
	}
	ws.mu.Unlock()
	sort.Slice(hints, func(i, j int) bool {
		if hints[i].at.Equal(hints[j].at) {
			return hints[i].slug < hints[j].slug
		}
		return hints[i].at.After(hints[j].at)
	})
	hot := make([]string, 0, len(hints))
	for _, h := range hints {
		hot = append(hot, h.slug)
	}
	return fullPriorityWithHot(priority, ws.FullBookCap(), hot)
}

func (s *pusSession) write(v any) error {
	if s.writeJSON != nil {
		return s.writeJSON(v)
	}
	if s.conn == nil {
		return errors.New("nil markets websocket")
	}
	deadline := time.Now().Add(marketsWSDataWriteTimeout)
	if !s.writeBy.IsZero() && s.writeBy.Before(deadline) {
		deadline = s.writeBy
	}
	if err := s.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return s.conn.WriteJSON(v)
}

func (s *pusSession) wideCount() int {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return len(s.wide)
}

// marketSubscriptionPlan is the exact server-side meaning of one fixed request ID. Request IDs
// are reused across reconciles, so the ID alone cannot prove that a retained chunk still contains
// the same markets. Keeping the type and ordered members lets reconciliation explicitly remove a
// changed request before installing its replacement.
type marketSubscriptionPlan struct {
	subscriptionType int
	marketSlugs      string
}

func marketSubscriptionFramePlan(frame map[string]any) (string, marketSubscriptionPlan, bool) {
	sub, _ := frame["subscribe"].(map[string]any)
	id, _ := sub["request_id"].(string)
	id = strings.TrimSpace(id)
	typ, _ := sub["subscription_type"].(int)
	slugs, _ := sub["market_slugs"].([]string)
	if id == "" || typ == 0 || len(slugs) == 0 {
		return "", marketSubscriptionPlan{}, false
	}
	return id, marketSubscriptionPlan{
		subscriptionType: typ,
		marketSlugs:      strings.Join(slugs, "\x00"),
	}, true
}

func marketSubscriptionPlanHash(requests map[string]marketSubscriptionPlan) string {
	ids := make([]string, 0, len(requests))
	for id := range requests {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	h := sha256.New()
	for _, id := range ids {
		plan := requests[id]
		_, _ = fmt.Fprintf(h, "%s\x00%d\x00%s\x00", id, plan.subscriptionType, plan.marketSlugs)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func marketSubscriptionHashForSlugs(slugs []string, fullBookCap int) string {
	priority := normalizeMarketSlugs(slugs)
	if len(priority) == 0 {
		return ""
	}
	frames := marketSubFramesForSets(priority, priority, fullBookCap)
	requests := make(map[string]marketSubscriptionPlan, len(frames))
	for _, frame := range frames {
		if id, plan, ok := marketSubscriptionFramePlan(frame); ok {
			requests[id] = plan
		}
	}
	return marketSubscriptionPlanHash(requests)
}

// marketUnsubscribeFrame is the current official Polymarket US WS envelope:
// https://docs.polymarket.us/api-reference/websocket/overview (verified 2026-07-14).
func marketUnsubscribeFrame(requestID string) map[string]any {
	return map[string]any{"unsubscribe": map[string]any{"request_id": requestID}}
}

func (ws *MarketsWS) clearPublishedSubscriptionCoverage() {
	ws.subSlugs.Store(0)
	ws.subFrames.Store(0)
	ws.subAt.Store(0)
	ws.subFull.Store(0)
	ws.subLite.Store(0)
	ws.subTrade.Store(0)
}

// expectPrimarySubscriptionPlan changes the exact membership contract before reconciliation
// starts. Clearing the count receipt here prevents an old equal-sized standby plan from looking
// current while the required unsubscribe/subscribe writes are still in flight.
func (ws *MarketsWS) expectPrimarySubscriptionPlan(s *pusSession, slugs []string) {
	if ws == nil {
		return
	}
	priority := normalizeMarketSlugs(slugs)
	if len(priority) == 0 {
		return
	}
	fullPriority := ws.fullPriority(priority, time.Now())
	frames := marketSubFramesForSets(fullPriority, priority, ws.FullBookCap())
	requests := make(map[string]marketSubscriptionPlan, len(frames))
	for _, frame := range frames {
		if id, plan, ok := marketSubscriptionFramePlan(frame); ok {
			requests[id] = plan
		}
	}
	ws.expectPrimarySubscriptionHash(s, marketSubscriptionPlanHash(requests))
}

func (ws *MarketsWS) expectPrimarySubscriptionHash(s *pusSession, hash string) {
	if ws == nil || s == nil || !s.primary.Load() || hash == "" {
		return
	}
	ws.mu.Lock()
	if s.gen != 0 && ws.activeGen.Load() == s.gen {
		ws.expectedSubGen = s.gen
		ws.expectedSubHash = hash
		// Membership is changing (or being re-proved). Fail closed until the complete unsubscribe /
		// subscribe mutation succeeds and publishSessionCoverageLocked installs the exact new set.
		ws.activeFullGen = 0
		ws.activeFull = nil
		ws.clearPublishedSubscriptionCoverage()
	}
	ws.mu.Unlock()
}

func (ws *MarketsWS) primarySubscriptionPlanMismatch(s *pusSession) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return s != nil && s.primary.Load() && s.gen != 0 && ws.activeGen.Load() == s.gen &&
		(ws.expectedSubGen != s.gen || ws.expectedSubHash == "" || s.planHash != ws.expectedSubHash)
}

// publishSessionCoverageLocked publishes only the currently authoritative primary. A standby can
// carry a different historical request set while it is being reconciled and must never overwrite
// readiness receipts merely because its writes happened to finish last. Caller holds s.writeMu.
func (ws *MarketsWS) publishSessionCoverageLocked(s *pusSession) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if s == nil || !s.primary.Load() || s.gen == 0 || ws.activeGen.Load() != s.gen ||
		ws.expectedSubGen != s.gen || ws.expectedSubHash == "" || s.planHash != ws.expectedSubHash {
		return
	}
	nextFull := make(map[string]bool, len(s.fullSet))
	for slug := range s.fullSet {
		nextFull[slug] = true
	}
	// A same-generation hot rotation used to leave the evicted slug's bookGen and ladder intact,
	// allowing CurrentOpenFullBook to reuse stale depth indefinitely. Remove only full-frame state;
	// the prioritized LITE/TRADE BBO and tape stay intact.
	for slug, e := range ws.live {
		if e == nil || e.bookGen != s.gen || nextFull[slug] {
			continue
		}
		e.bookBid, e.bookAsk, e.bidSz, e.askSz = 0, 0, 0, 0
		e.bidLevels, e.askLevels = nil, nil
		e.imb3, e.depth3 = -1, 0
		e.bookAt, e.bookSourceAt = time.Time{}, time.Time{}
		e.bookGen = 0
		e.fullLifecycleAt = time.Time{}
		if e.stateGen == s.gen {
			e.stateKnown, e.marketOpen, e.stateGen = false, false, 0
		}
	}
	ws.activeFullGen = s.gen
	ws.activeFull = nextFull
	ws.subSlugs.Store(int64(len(s.wide)))
	ws.subFrames.Store(int64(s.frames))
	if s.subAt.IsZero() {
		ws.subAt.Store(0)
	} else {
		ws.subAt.Store(s.subAt.Unix())
	}
	ws.subFull.Store(int64(s.full))
	ws.subLite.Store(int64(len(s.wide)))
	ws.subTrade.Store(int64(len(s.wide)))
}

func (ws *MarketsWS) publishSessionCoverage(s *pusSession) {
	if s == nil {
		return
	}
	s.writeMu.Lock()
	ws.publishSessionCoverageLocked(s)
	s.writeMu.Unlock()
}

// subscribeMarkets sends the chunked subscription frames for a slug set — shared by dialSession,
// promotion, and in-place reconciliation. Type 1 = full depth, 2 = lightweight BBO,
// and 3 = taker trades. Snake/numeric requests and camelCase responses are both confirmed live.
func (ws *MarketsWS) subscribeMarkets(s *pusSession, slugs []string) error {
	priority := normalizeMarketSlugs(slugs)
	if len(priority) == 0 {
		return errNoSlugs
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.writeBy = time.Now().Add(marketsWSPlanWriteTimeout)
	defer func() { s.writeBy = time.Time{} }()
	// slugsFn is backed by the latest complete lifecycle-validated crawl. Reconcile to that exact
	// universe instead of retaining closed slugs forever. Reused fixed IDs replace their chunk;
	// request IDs beyond the new tail are explicitly unsubscribed using the official envelope.
	wide := append([]string(nil), priority...)
	if len(wide) > DefaultMarketsWideBookCap {
		wide = wide[:DefaultMarketsWideBookCap]
	}
	fullBookCap := ws.FullBookCap()
	fullPriority := ws.fullPriority(priority, time.Now())
	frames := marketSubFramesForSets(fullPriority, wide, fullBookCap)
	nextRequests := make(map[string]marketSubscriptionPlan, len(frames))
	for _, f := range frames {
		if id, plan, ok := marketSubscriptionFramePlan(f); ok {
			nextRequests[id] = plan
		}
	}
	nextPlanHash := marketSubscriptionPlanHash(nextRequests)
	if s.primary.Load() {
		ws.expectPrimarySubscriptionHash(s, nextPlanHash)
	}
	// A fixed ID whose membership changes is not an in-place replacement on the venue. Remove all
	// retired or changed requests first, then subscribe the exact new plan. This prevents retained
	// tail chunks from accumulating stale markets or duplicate tape/book subscriptions.
	retiredOrChanged := make([]string, 0)
	for id, oldPlan := range s.requests {
		if nextPlan, ok := nextRequests[id]; !ok || nextPlan != oldPlan {
			retiredOrChanged = append(retiredOrChanged, id)
		}
	}
	sort.Strings(retiredOrChanged)
	for _, id := range retiredOrChanged {
		if err := s.write(marketUnsubscribeFrame(id)); err != nil {
			return err
		}
	}
	for _, f := range frames {
		id, nextPlan, ok := marketSubscriptionFramePlan(f)
		if !ok {
			continue
		}
		if oldPlan, exists := s.requests[id]; exists && oldPlan == nextPlan {
			continue
		}
		if err := s.write(f); err != nil {
			return err
		}
	}
	s.wide = wide
	s.wideSet = make(map[string]bool, len(wide))
	for _, slug := range wide {
		s.wideSet[slug] = true
	}
	s.requests = nextRequests
	s.planHash = nextPlanHash
	fullN := len(fullPriority)
	if fullN > fullBookCap {
		fullN = fullBookCap
	}
	s.fullSet = make(map[string]bool, fullN)
	for _, slug := range fullPriority[:fullN] {
		s.fullSet[slug] = true
	}
	s.full = fullN
	s.frames = len(nextRequests)
	s.subAt = time.Now()
	if ws.primarySubscriptionPlanMismatch(s) {
		return errSubscriptionPlanSuperseded
	}
	ws.publishSessionCoverageLocked(s)
	return nil
}

// subscribeNewMarkets appends only missing wide slugs. If the old tail chunk was partial it is
// explicitly removed before the same request ID is installed with old+new members; brand-new
// chunks receive the next fixed IDs. Thus each slug occupies exactly one LITE and one TRADE
// subscription on a session.
func (ws *MarketsWS) subscribeNewMarkets(s *pusSession, slugs []string) (added, frames int, err error) {
	requested := normalizeMarketSlugs(slugs)
	if len(requested) == 0 {
		return 0, 0, nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	remaining := DefaultMarketsWideBookCap - len(s.wide)
	if remaining <= 0 {
		return 0, 0, nil
	}
	s.writeBy = time.Now().Add(marketsWSPlanWriteTimeout)
	defer func() { s.writeBy = time.Time{} }()
	if s.wideSet == nil {
		s.wideSet = make(map[string]bool, len(s.wide)+len(requested))
		for _, slug := range s.wide {
			s.wideSet[slug] = true
		}
	}
	wide := append([]string(nil), s.wide...)
	for _, slug := range requested {
		if !s.wideSet[slug] {
			wide = append(wide, slug)
			added++
			if added == remaining {
				break
			}
		}
	}
	if added == 0 {
		return 0, 0, nil
	}
	startChunk := len(s.wide) / marketsSubChunk
	batch := make([]map[string]any, 0, 2*((len(wide)+marketsSubChunk-1)/marketsSubChunk-startChunk))
	for _, spec := range []struct {
		prefix string
		typ    int
	}{{"mdl", 2}, {"trade", 3}} {
		for i := startChunk; i*marketsSubChunk < len(wide); i++ {
			lo, hi := i*marketsSubChunk, (i+1)*marketsSubChunk
			if hi > len(wide) {
				hi = len(wide)
			}
			batch = append(batch, map[string]any{"subscribe": map[string]any{
				"request_id": fmt.Sprintf("%s-%d", spec.prefix, i), "subscription_type": spec.typ,
				"market_slugs": wide[lo:hi],
			}})
		}
	}
	if s.requests == nil {
		s.requests = make(map[string]marketSubscriptionPlan, len(batch))
	}
	batchPlans := make(map[string]marketSubscriptionPlan, len(batch))
	changed := make([]string, 0, len(batch))
	for _, f := range batch {
		id, nextPlan, ok := marketSubscriptionFramePlan(f)
		if !ok {
			continue
		}
		batchPlans[id] = nextPlan
		if oldPlan, exists := s.requests[id]; exists && oldPlan != nextPlan {
			changed = append(changed, id)
		}
	}
	nextRequests := make(map[string]marketSubscriptionPlan, len(s.requests)+len(batchPlans))
	for id, plan := range s.requests {
		nextRequests[id] = plan
	}
	for id, plan := range batchPlans {
		nextRequests[id] = plan
	}
	nextPlanHash := marketSubscriptionPlanHash(nextRequests)
	if s.primary.Load() {
		ws.expectPrimarySubscriptionHash(s, nextPlanHash)
	}
	sort.Strings(changed)
	for _, id := range changed {
		if err := s.write(marketUnsubscribeFrame(id)); err != nil {
			return 0, 0, err
		}
	}
	for _, f := range batch {
		id, nextPlan, ok := marketSubscriptionFramePlan(f)
		if !ok {
			continue
		}
		if oldPlan, exists := s.requests[id]; exists && oldPlan == nextPlan {
			continue
		}
		if err := s.write(f); err != nil {
			return 0, 0, err
		}
	}
	for id, plan := range batchPlans {
		s.requests[id] = plan
	}
	s.planHash = nextPlanHash
	s.wide = wide
	for _, slug := range wide[len(wide)-added:] {
		s.wideSet[slug] = true
	}
	s.frames = len(s.requests)
	s.subAt = time.Now()
	if ws.primarySubscriptionPlanMismatch(s) {
		return 0, 0, errSubscriptionPlanSuperseded
	}
	ws.publishSessionCoverageLocked(s)
	return added, len(batch), nil
}

// SubPlan reports the last subscribe actually sent (slugs requested, frames written, when) — the
// R102 bug-242 verification surface: requested should track the entire PolyUSSlugs set, and Count()
// (fresh slugs) should converge toward it on liquid days.
func (ws *MarketsWS) SubPlan() (slugs, frames int, at time.Time) {
	return int(ws.subSlugs.Load()), int(ws.subFrames.Load()), time.Unix(ws.subAt.Load(), 0)
}

// SubCoverage proves the tiered subscription split. Counts are requested slugs, not the transient
// number that happened to receive a price change in the current connection generation.
func (ws *MarketsWS) SubCoverage() (full, lite, trade int) {
	return int(ws.subFull.Load()), int(ws.subLite.Load()), int(ws.subTrade.Load())
}

func (ws *MarketsWS) notePrimaryStart(gen uint64) {
	if gen == 0 {
		return
	}
	now := time.Now().UnixNano()
	ws.mu.Lock()
	if ws.primaryStarts.Add(1) > 1 {
		ws.primarySwitch.Add(1)
	}
	ws.activeGen.Store(gen)
	ws.expectedSubGen = gen
	ws.expectedSubHash = ""
	ws.activeFullGen = 0
	ws.activeFull = nil
	ws.clearPublishedSubscriptionCoverage()
	ws.primarySince.Store(now)
	ws.primaryFrameAt.Store(now)
	ws.primaryDataAt.Store(0)
	ws.fullSourceAt.Store(0)
	ws.liteAt.Store(0)
	ws.mu.Unlock()
}

func (ws *MarketsWS) notePrimaryTransport(gen uint64) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if gen == 0 || ws.activeGen.Load() != gen {
		return false
	}
	ws.primaryFrameAt.Store(time.Now().UnixNano())
	return true
}

func (ws *MarketsWS) notePrimaryData(gen uint64) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if gen == 0 || ws.activeGen.Load() != gen {
		return false
	}
	now := time.Now().UnixNano()
	ws.primaryFrameAt.Store(now)
	ws.primaryDataAt.Store(now)
	return true
}

func (ws *MarketsWS) noteDisconnect(s *pusSession, err error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.disconnects.Add(1)
	ws.lastCloseAt = time.Now()
	if err != nil {
		ws.lastCloseErr = err.Error()
	} else {
		ws.lastCloseErr = "connection closed"
	}
	// Invalidate before the role manager promotes or redials. No reader may use a snapshot from a
	// connection after its read loop has reported death, even during the tiny handoff interval.
	if s != nil && s.primary.Load() && ws.activeGen.Load() == s.gen {
		ws.activeGen.Store(0)
		ws.expectedSubGen = 0
		ws.expectedSubHash = ""
		ws.activeFullGen = 0
		ws.activeFull = nil
		ws.clearPublishedSubscriptionCoverage()
		ws.primarySince.Store(0)
		ws.primaryFrameAt.Store(0)
		ws.primaryDataAt.Store(0)
		ws.fullSourceAt.Store(0)
		ws.liteAt.Store(0)
	}
}

// PrimaryFrameAge is active-generation transport vitality, including venue heartbeats and pongs.
// Book authority uses this continuity receipt plus generation identity, not an arbitrary quote TTL.
func (ws *MarketsWS) PrimaryFrameAge() (time.Duration, bool) {
	ns := ws.primaryFrameAt.Load()
	if ns <= 0 {
		return 0, false
	}
	return time.Since(time.Unix(0, ns)), true
}

// PrimaryDataAge excludes heartbeats and subscription responses. primaryAge is returned even
// before the first data payload so the watchdog grants one normal warm-up window, then heals a
// heartbeat-alive subscription that never begins delivering market data.
func (ws *MarketsWS) PrimaryDataAge() (dataAge, primaryAge time.Duration, haveData, havePrimary bool) {
	now := time.Now()
	if ns := ws.primarySince.Load(); ns > 0 {
		primaryAge, havePrimary = now.Sub(time.Unix(0, ns)), true
	}
	if ns := ws.primaryDataAt.Load(); ns > 0 {
		dataAge, haveData = now.Sub(time.Unix(0, ns)), true
	}
	return
}

func isMarketsDataFrame(data []byte) bool {
	var env struct {
		Trade json.RawMessage `json:"trade"`
		Full  json.RawMessage `json:"marketData"`
		FullS json.RawMessage `json:"market_data"`
		Lite  json.RawMessage `json:"marketDataLite"`
		LiteS json.RawMessage `json:"market_data_lite"`
	}
	return json.Unmarshal(data, &env) == nil &&
		(len(env.Trade) > 0 || len(env.Full) > 0 || len(env.FullS) > 0 || len(env.Lite) > 0 || len(env.LiteS) > 0)
}

func isMarketsHeartbeatFrame(data []byte) bool {
	var env struct {
		Heartbeat json.RawMessage `json:"heartbeat"`
	}
	return json.Unmarshal(data, &env) == nil && len(env.Heartbeat) > 0
}

// marketsHeartbeatReply is kept pure so the exact documented response envelope stays pinned by a
// schema test. The venue heartbeat has no nonce or payload to mirror.
func marketsHeartbeatReply() map[string]any {
	return map[string]any{"heartbeat": map[string]any{}}
}

// runSession reads one socket forever; ingests only while PRIMARY (standby stays silent).
func (ws *MarketsWS) runSession(ctx context.Context, s *pusSession, onDead func(*pusSession)) {
	defer s.conn.Close()
	pingDone := make(chan struct{})
	defer close(pingDone)
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-pingDone:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
					_ = s.conn.Close() // unblock ReadMessage; a dead ping writer must enter the fail-closed handoff
					return
				}
			}
		}
	}()
	for {
		_, data, err := s.conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				ws.noteDisconnect(s, err)
				onDead(s)
			}
			return
		}
		s.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		if isMarketsHeartbeatFrame(data) {
			// A standby now lives indefinitely instead of being recycled every five minutes. Keep it
			// protocol-healthy too; it must be ready for a real primary failure, not merely connected.
			s.writeMu.Lock()
			err = s.write(marketsHeartbeatReply())
			s.writeMu.Unlock()
			if err != nil {
				_ = s.conn.Close()
				continue
			}
		}
		if !s.primary.Load() {
			continue // warm standby: connected + subscribed, but silent
		}
		if !ws.notePrimaryTransport(s.gen) {
			continue // the role changed after ReadMessage; never ingest an old-generation tail frame
		}
		ws.mu.Lock() // R102 (bug 250): snapshot the hook under mu — SetOnMsg may race the read loop
		om := ws.onMsg
		ws.mu.Unlock()
		if om != nil {
			om(data)
		}
		if ws.ingestGeneration(data, s.gen) {
			ws.notePrimaryData(s.gen)
		}
	}
}

// ingest parses a trade (taker intent → flow) or a market-data-lite (price) message. Lenient on
// snake_case vs camelCase since the two doc pages disagree; whichever the server sends, we catch it.
func (ws *MarketsWS) ingest(data []byte) {
	gen := ws.activeGen.Load()
	if gen == 0 {
		gen = ws.sessionSeq.Add(1)
		ws.notePrimaryStart(gen)
	}
	if ws.ingestGeneration(data, gen) {
		ws.notePrimaryData(gen)
	}
}

// ingestGeneration accepts only frames belonging to the active primary. Official docs define each
// MARKET_DATA message as a full order book, while runtime receipts show unchanged books may be
// quiet; generation, rather than elapsed wall time since the last change, is therefore the book's
// continuity boundary.
func (ws *MarketsWS) ingestGeneration(data []byte, gen uint64) bool {
	accepted := false
	var env struct {
		Trade json.RawMessage `json:"trade"`
		Full  json.RawMessage `json:"marketData"`
		FullS json.RawMessage `json:"market_data"`
		Lite  json.RawMessage `json:"market_data_lite"`
		LiteC json.RawMessage `json:"marketDataLite"`
	}
	if json.Unmarshal(data, &env) != nil {
		return false
	}
	full := env.Full
	if len(full) == 0 {
		full = env.FullS
	}
	if len(full) > 0 { // full MARKET_DATA: live order book → best bid/ask + mid (replaces 15s REST book)
		var f struct {
			Slug          string `json:"marketSlug"`
			SlugS         string `json:"market_slug"`
			State         string `json:"state"`
			Status        string `json:"status"`
			EP3Status     string `json:"ep3Status"`
			EP3StatusS    string `json:"ep3_status"`
			TransactTime  string `json:"transactTime"`
			TransactTimeS string `json:"transact_time"`
			Bids          []struct {
				Px  json.RawMessage `json:"px"`
				Qty json.RawMessage `json:"qty"` // R70-B #1: per-level size — same {px:{value},qty} encoding as the REST book (ParseBook)
			} `json:"bids"`
			Offers []struct {
				Px  json.RawMessage `json:"px"`
				Qty json.RawMessage `json:"qty"`
			} `json:"offers"`
		}
		if json.Unmarshal(full, &f) == nil {
			slug := firstNonBlank(f.Slug, f.SlugS)
			if slug == "" {
				return false
			}
			var bestBid, bestAsk, bidSz, askSz, bid3, ask3 float64
			bidLevels := make([]BookLevel, 0, len(f.Bids))
			askLevels := make([]BookLevel, 0, len(f.Offers))
			if len(f.Bids) > 0 {
				bestBid = rawFloat(f.Bids[0].Px)
				bidSz = rawFloat(f.Bids[0].Qty)
			}
			if len(f.Offers) > 0 {
				bestAsk = rawFloat(f.Offers[0].Px)
				askSz = rawFloat(f.Offers[0].Qty)
			}
			for i := 0; i < len(f.Bids) && i < 3; i++ { // levels arrive best-first (bids[0] is the touch)
				bid3 += rawFloat(f.Bids[i].Qty)
			}
			for i := 0; i < len(f.Offers) && i < 3; i++ {
				ask3 += rawFloat(f.Offers[i].Qty)
			}
			for _, level := range f.Bids {
				if px, qty := rawFloat(level.Px), rawFloat(level.Qty); px > 0 && px < 1 && qty > 0 {
					bidLevels = append(bidLevels, BookLevel{Price: px, Quantity: qty})
				}
			}
			for _, level := range f.Offers {
				if px, qty := rawFloat(level.Px), rawFloat(level.Qty); px > 0 && px < 1 && qty > 0 {
					askLevels = append(askLevels, BookLevel{Price: px, Quantity: qty})
				}
			}
			sort.SliceStable(bidLevels, func(i, j int) bool { return bidLevels[i].Price > bidLevels[j].Price })
			sort.SliceStable(askLevels, func(i, j int) bool { return askLevels[i].Price < askLevels[j].Price })
			now := time.Now()
			transactTime := firstNonBlank(f.TransactTime, f.TransactTimeS)
			sourceAt, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(transactTime))
			stateKnown, marketOpen := wsLifecycleOpen(f.State, f.Status, f.EP3Status, f.EP3StatusS)
			ws.mu.Lock()
			if gen == 0 || ws.activeGen.Load() != gen {
				ws.mu.Unlock()
				return false
			}
			accepted = true
			ws.fullFrames.Add(1)
			if !sourceAt.IsZero() {
				for {
					previous := ws.fullSourceAt.Load()
					if previous >= sourceAt.UnixNano() || ws.fullSourceAt.CompareAndSwap(previous, sourceAt.UnixNano()) {
						break
					}
				}
			}
			e := ws.live[slug]
			if e == nil {
				e = &pusLive{}
				ws.live[slug] = e
			}
			e.stateKnown, e.marketOpen = stateKnown, marketOpen
			if stateKnown {
				e.fullLifecycleAt = now
				e.stateGen = gen
			} else {
				e.fullLifecycleAt = time.Time{}
				e.stateGen = 0
			}
			e.sourceAt, e.bookSourceAt = sourceAt, sourceAt
			// MARKET_DATA is a complete snapshot. Non-open or schema-unknown state
			// invalidates the old executable quote instead of carrying it forward.
			if stateKnown && marketOpen {
				e.bid, e.ask = bestBid, bestAsk
				e.bookBid, e.bookAsk = bestBid, bestAsk
				e.bidSz, e.askSz = bidSz, askSz
				e.bidLevels, e.askLevels = bidLevels, askLevels
				e.imb3, e.depth3 = -1, bid3+ask3
				if bid3+ask3 > 0 {
					e.imb3 = bid3 / (bid3 + ask3)
				}
				e.quoteAt, e.bookAt = now, now
				e.quoteGen, e.bookGen = gen, gen
			} else {
				e.bid, e.ask, e.bookBid, e.bookAsk, e.bidSz, e.askSz = 0, 0, 0, 0, 0, 0
				e.bidLevels, e.askLevels = nil, nil
				e.imb3, e.depth3 = -1, 0
				e.quoteAt, e.bookAt = time.Time{}, time.Time{}
				e.quoteGen, e.bookGen = 0, 0
			}
			mid := 0.0
			if stateKnown && marketOpen && bestBid > 0 && bestAsk > 0 {
				mid = (bestBid + bestAsk) / 2
				if e.priceGen != gen {
					e.winStart = time.Time{}
				}
				e.notePx(mid, now)
				e.priceGen = gen
			}
			e.at = now
			tf := ws.tickFn
			ws.mu.Unlock()
			if tf != nil && mid > 0 { // R40: event-driven 1¢ cancel hook — fired on THIS book update
				tf(slug, mid)
			}
		}
	}
	if len(env.Trade) > 0 {
		var t struct {
			Slug  string          `json:"market_slug"`
			SlugC string          `json:"marketSlug"`
			Price json.RawMessage `json:"price"`
			Qty   json.RawMessage `json:"quantity"`
			Taker struct {
				Intent      string `json:"intent"`
				Side        string `json:"side"`
				OutcomeSide string `json:"outcomeSide"`
			} `json:"taker"`
		}
		if json.Unmarshal(env.Trade, &t) == nil {
			slug := t.Slug
			if slug == "" {
				slug = t.SlugC
			}
			if slug != "" {
				px, qty := rawFloat(t.Price), rawFloat(t.Qty)
				bull, ok := takerBullish(t.Taker.Intent, t.Taker.Side, t.Taker.OutcomeSide)
				ws.mu.Lock()
				if gen == 0 || ws.activeGen.Load() != gen {
					ws.mu.Unlock()
					return false
				}
				accepted = true
				e := ws.live[slug]
				if e == nil {
					e = &pusLive{}
					ws.live[slug] = e
				}
				// R102 (auditor bug 243, P3): trade-print denomination. If the venue px is the traded
				// OUTCOME's price, a NO print @0.97 used to poison the YES momentum window (notePx) —
				// if it is YES-space, the NO-flow notional (px·qty) was under/over-stated instead. One
				// IS wrong today; which one is decided by live evidence: a NO-side print landing while
				// the full-book mid is fresh votes for whichever interpretation sits nearer that mid
				// (≥5¢ separation required — prints near 50¢ abstain). ≥25 votes at ≥80% agreement
				// latches the mode; until then behavior is EXACTLY the legacy one (zero-risk default).
				twoSided := ws.twoSided[slug] // R106 (bug 255): single-slug two-sided instruments get their own print handling
				if ok && !bull && !twoSided && px > 0 && px < 1 && ws.bookSnapshotCurrentLocked(e, time.Now()) {
					mid := (e.bookBid + e.bookAsk) / 2
					dYes, dOut := math.Abs(px-mid), math.Abs((1-px)-mid)
					if dOut < dYes-0.05 {
						ws.denomOutV++
					} else if dYes < dOut-0.05 {
						ws.denomYesV++
					}
					if ws.denomMode == denomUnknown && ws.denomOutV+ws.denomYesV >= 25 {
						tot := ws.denomOutV + ws.denomYesV
						if ws.denomOutV*5 >= tot*4 {
							ws.denomMode = denomOutcome
						} else if ws.denomYesV*5 >= tot*4 {
							ws.denomMode = denomYes
						}
					}
				}
				pxYes, notional := px, px*qty // legacy interpretation (denomUnknown): px is YES-space for momentum, outcome-dollars for notional
				if ok && !bull {
					if twoSided {
						// R106 (bug 255): short-team print on a two-sided instrument. Whether the venue
						// reports it in the short side's own price or in long/YES space is disambiguated
						// PER PRINT against this market's fresh long-denominated book mid (≥5¢ separation;
						// ambiguous prints near 50¢ inherit the venue-wide latched mode, where the two
						// interpretations converge anyway). These prints NEVER vote on the global latch.
						decided := false
						if px > 0 && px < 1 && ws.bookSnapshotCurrentLocked(e, time.Now()) {
							mid := (e.bookBid + e.bookAsk) / 2
							dYes, dOut := math.Abs(px-mid), math.Abs((1-px)-mid)
							if dOut < dYes-0.05 { // px is the short side's own price: normalize to YES-space, dollars are px·qty
								pxYes, decided = 1-px, true
							} else if dYes < dOut-0.05 { // px arrived long/YES-space: the short buyer actually paid (1−px)·qty
								notional, decided = (1-px)*qty, true
							}
						}
						if !decided {
							switch ws.denomMode {
							case denomOutcome:
								pxYes = 1 - px
							case denomYes:
								notional = (1 - px) * qty
							}
						}
					} else {
						switch ws.denomMode {
						case denomOutcome:
							pxYes = 1 - px // outcome px → YES-space for the momentum window; px·qty already = real dollars spent
						case denomYes:
							notional = (1 - px) * qty // YES px on a NO print → the NO buyer actually paid (1−px)·qty
						}
					}
				}
				validPrint := ok && qty > 0 && px > 0 && px <= 1 && !math.IsNaN(qty) && !math.IsInf(qty, 0) && !math.IsNaN(px) && !math.IsInf(px, 0)
				if validPrint {
					e.flowDecay(time.Now()) // decay the old flow before adding the fresh print
					if bull {
						e.buyYes += notional
					} else {
						e.buyNo += notional
					}
					if notional > 0 { // keep raw display px plus normalized YES-space execution truth
						ws.trades = append(ws.trades, pusTrade{slug: slug, yesSide: bull, notional: notional, px: px, yesPx: pxYes, at: time.Now()})
						if len(ws.trades) > 600 {
							ws.trades = ws.trades[len(ws.trades)-600:]
						}
					}
					if e.priceGen != gen {
						e.winStart = time.Time{}
					}
					e.notePx(pxYes, time.Now())
					e.at = time.Now()
					e.priceGen = gen
				}
				ws.mu.Unlock()
			}
		}
	}
	lite := env.Lite
	if len(lite) == 0 {
		lite = env.LiteC
	}
	if len(lite) > 0 {
		var l struct {
			Slug      string          `json:"market_slug"`
			SlugC     string          `json:"marketSlug"`
			BestBid   json.RawMessage `json:"best_bid"`
			BestBidC  json.RawMessage `json:"bestBid"`
			BestAsk   json.RawMessage `json:"best_ask"`
			BestAskC  json.RawMessage `json:"bestAsk"`
			CurPx     json.RawMessage `json:"current_px"`
			CurPxC    json.RawMessage `json:"currentPx"`
			LastPx    json.RawMessage `json:"last_trade_px"`
			LastPxC   json.RawMessage `json:"lastTradePx"`
			State     string          `json:"state"`
			Status    string          `json:"status"`
			EP3State  string          `json:"ep3Status"`
			EP3StateS string          `json:"ep3_status"`
		}
		if json.Unmarshal(lite, &l) == nil {
			slug := l.Slug
			if slug == "" {
				slug = l.SlugC
			}
			bid := rawFloat(l.BestBid)
			if bid == 0 {
				bid = rawFloat(l.BestBidC)
			}
			ask := rawFloat(l.BestAsk)
			if ask == 0 {
				ask = rawFloat(l.BestAskC)
			}
			yes := 0.0
			switch {
			case bid > 0 && ask > 0:
				yes = (bid + ask) / 2
			default:
				for _, r := range []json.RawMessage{l.CurPx, l.CurPxC, l.LastPx, l.LastPxC} {
					if v := rawFloat(r); v > 0 {
						yes = v
						break
					}
				}
			}
			if slug != "" {
				ws.mu.Lock()
				if gen == 0 || ws.activeGen.Load() != gen {
					ws.mu.Unlock()
					return false
				}
				accepted = true
				ws.liteFrames.Add(1)
				ws.liteAt.Store(time.Now().UTC().UnixNano())
				e := ws.live[slug]
				if e == nil {
					e = &pusLive{}
					ws.live[slug] = e
				}
				now := time.Now()
				if stateKnown, marketOpen := wsLifecycleOpen(l.State, l.Status, l.EP3State, l.EP3StateS); stateKnown {
					// LITE lifecycle is useful terminal evidence, but it is not the full-frame
					// lifecycle authority. Any explicit LITE state supersedes (and therefore
					// invalidates) a prior full-frame receipt; OPEN still needs fresh REST proof.
					e.fullLifecycleAt = time.Time{}
					e.stateKnown = true
					e.marketOpen = marketOpen
					e.stateGen = gen
				}
				if yes > 0 {
					if e.priceGen != gen {
						e.winStart = time.Time{}
					}
					e.notePx(yes, now)
					e.at = now
					e.priceGen = gen
				}
				// Lite BBO is executable quote data but carries no depth. The documented
				// payload normally has no lifecycle, so retain the quote and let
				// executableQuoteFreshLocked require the independent REST-open proof.
				if bid > 0 && ask > bid && ask < 1 {
					e.bid, e.ask, e.quoteAt = bid, ask, now
					e.quoteGen = gen
					e.sourceAt = time.Time{} // lite has no source clock; quoteAt is its arrival-time receipt
				} else {
					e.bid, e.ask, e.quoteAt, e.quoteGen = 0, 0, time.Time{}, 0
				}
				if e.stateKnown && e.stateGen == gen && !e.marketOpen {
					e.bid, e.ask, e.quoteAt = 0, 0, time.Time{}
					e.quoteGen = 0
				}
				tf := ws.tickFn
				ws.mu.Unlock()
				if tf != nil && yes > 0 { // R40: event-driven 1¢ cancel hook (lite feed path)
					tf(slug, yes)
				}
			}
		}
	}
	return accepted
}

// wsLifecycleOpen requires all supplied lifecycle sources to agree. PREOPEN contains the word OPEN
// but is not OPEN; exact normalized enum equality is mandatory on an execution feed.
func wsLifecycleOpen(states ...string) (known, open bool) {
	for _, raw := range states {
		state := lifecycleState(raw)
		if state == "" {
			continue
		}
		known = true
		if state != "OPEN" && state != "ACTIVE" {
			return true, false
		}
		open = true
	}
	return known, open
}

// SetTickHandler registers the per-update callback (R40 event-driven cancels). Called from the WS
// read loop — the handler MUST be near-instant and never block.
func (ws *MarketsWS) SetTickHandler(fn func(slug string, yes float64)) {
	ws.mu.Lock()
	ws.tickFn = fn
	ws.mu.Unlock()
}

// takerBullish decides whether the aggressor (taker) was bullish on YES — from the explicit intent
// (BUY_LONG=YES, BUY_SHORT=NO) when present, else from side+outcomeSide: buying YES or selling NO is
// bullish; buying NO or selling YES is bearish. ok=false if it can't be determined.
func takerBullish(intent, side, outcomeSide string) (bull, ok bool) {
	switch intent {
	case "ORDER_INTENT_BUY_LONG":
		return true, true
	case "ORDER_INTENT_BUY_SHORT":
		return false, true
	case "ORDER_INTENT_SELL_LONG":
		return false, true
	case "ORDER_INTENT_SELL_SHORT":
		return true, true
	}
	su, ou := strings.ToUpper(side), strings.ToUpper(outcomeSide)
	if su == "" || ou == "" {
		return false, false
	}
	buy := strings.Contains(su, "BUY")
	yesTok := strings.Contains(ou, "YES")
	return buy == yesTok, true // buy+YES or sell+NO => bullish; buy+NO or sell+YES => bearish
}

// LiveYes returns the latest price from the healthy active connection generation. Runtime shows an
// unchanged complete snapshot may be quiet, so unchanged does not mean stale; a handoff does.
func (ws *MarketsWS) LiveYes(slug string) (float64, bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if !ws.priceSnapshotCurrentLocked(e, time.Now()) {
		return 0, false
	}
	return e.yes, true
}

// LiveYesAt is LiveYes plus the actual update time for telemetry. Generation and transport
// continuity remain mandatory even though elapsed time since an unchanged snapshot is not a gate.
func (ws *MarketsWS) LiveYesAt(slug string) (px float64, at time.Time, ok bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if !ws.priceSnapshotCurrentLocked(e, time.Now()) {
		return 0, time.Time{}, false
	}
	return e.yes, e.at, true
}

// DenomStats reports the R102 bug-243 denomination evidence: the latched mode ("unknown"/
// "outcome"/"yes") and the vote tallies. The server WARN-logs the verdict once when it latches.
func (ws *MarketsWS) DenomStats() (mode string, outVotes, yesVotes int) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	switch ws.denomMode {
	case denomOutcome:
		mode = "outcome"
	case denomYes:
		mode = "yes"
	default:
		mode = "unknown"
	}
	return mode, ws.denomOutV, ws.denomYesV
}

// FlowImbalance returns a slug's taker-flow imbalance (buyYes-buyNo)/(buyYes+buyNo) in -1..1 (>0 = net
// aggressive BUY YES), and whether any MEANINGFUL flow remains after the 5-min-half-life decay
// (audit §6: the undecayed version presented hours-old flow as a live signal).
func (ws *MarketsWS) FlowImbalance(slug string) (float64, bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if e == nil {
		return 0, false
	}
	e.flowDecay(time.Now())
	tot := e.buyYes + e.buyNo
	if tot < 1 { // decayed below $1 of notional — stale/no flow, not a signal
		return 0, false
	}
	return (e.buyYes - e.buyNo) / tot, true
}

// LiveBidAsk returns a current-generation best bid/ask from MARKET_DATA or MARKET_DATA_LITE.
// Lite normally omits lifecycle, so it additionally needs a fresh independent REST-open proof.
func (ws *MarketsWS) LiveBidAsk(slug string) (bid, ask float64, ok bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if !ws.executableQuoteFreshLocked(slug, e, time.Now()) {
		return 0, 0, false
	}
	return e.bid, e.ask, true
}

// LiveBidAskAt is the book-native ML/audit form of LiveBidAsk. It returns the timestamp of the
// actual BBO frame (not the last trade/mid update) while preserving the exact same lifecycle and
// freshness gates. Callers can record quote age without treating a trade tick as a book refresh.
func (ws *MarketsWS) LiveBidAskAt(slug string) (bid, ask float64, at time.Time, ok bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if !ws.executableQuoteFreshLocked(slug, e, time.Now()) {
		return 0, 0, time.Time{}, false
	}
	return e.bid, e.ask, e.quoteAt, true
}

// FullBookTouchAt returns BBO and touch depth from one internally consistent full MARKET_DATA
// frame. LITE may update the display BBO between full frames, so it is deliberately excluded:
// pairing a new LITE price with old full-book depth would create a fabricated ML observation.
func (ws *MarketsWS) FullBookTouchAt(slug string) (bid, ask, bidSz, askSz float64, at time.Time, ok bool) {
	now := time.Now()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if !ws.bookSnapshotCurrentLocked(e, now) ||
		e.bidSz <= 0 || e.askSz <= 0 {
		return 0, 0, 0, 0, time.Time{}, false
	}
	// Lifecycle authority is shared with the executable BBO gate. A complete REST crawl may
	// invalidate a removed market even if a full book remains within its BBO age window.
	if !ws.lifecycleOpenFreshLocked(slug, e, now) {
		return 0, 0, 0, 0, time.Time{}, false
	}
	return e.bookBid, e.bookAsk, e.bidSz, e.askSz, e.bookAt, true
}

// FullBookLevelsAt returns the complete bounded full-frame ladders together with venue and local
// clocks. Bid levels are highest-first; ask levels are lowest-first. It never mixes LITE prices
// with older depth and never exposes internal mutable slices.
func (ws *MarketsWS) FullBookLevelsAt(slug string) (bids, asks []BookLevel, sourceAt, receivedAt time.Time, ok bool) {
	now := time.Now()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if !ws.bookSnapshotCurrentLocked(e, now) ||
		len(e.bidLevels) == 0 || len(e.askLevels) == 0 || !ws.lifecycleOpenFreshLocked(slug, e, now) {
		return nil, nil, time.Time{}, time.Time{}, false
	}
	return append([]BookLevel(nil), e.bidLevels...), append([]BookLevel(nil), e.askLevels...),
		e.bookSourceAt.UTC(), e.bookAt.UTC(), true
}

// CurrentOpenFullBookReceipt is execution-grade provenance from one complete MARKET_DATA frame.
// Unlike FullBookLevelsAt, it never borrows lifecycle authority from the periodic REST crawl: OPEN,
// full depth, and the active healthy socket generation must all be the same generation.
type CurrentOpenFullBookReceipt struct {
	Bids           []BookLevel
	Asks           []BookLevel
	SourceAt       time.Time
	ReceivedAt     time.Time
	ExplicitOpenAt time.Time
	Generation     uint64
}

// CurrentOpenFullBook returns a copied, current-generation, explicitly OPEN full book. This is the
// money-path accessor; broad research and Paper may continue using FullBookLevelsAt.
func (ws *MarketsWS) CurrentOpenFullBook(slug string) (CurrentOpenFullBookReceipt, bool) {
	if ws == nil {
		return CurrentOpenFullBookReceipt{}, false
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	now := time.Now()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	gen, healthy := ws.primaryGenerationHealthyLocked(now)
	e := ws.live[slug]
	if !healthy || !ws.bookSnapshotCurrentLocked(e, now) || len(e.bidLevels) == 0 ||
		len(e.askLevels) == 0 || !e.stateKnown || e.stateGen != gen || !e.marketOpen ||
		e.fullLifecycleAt.IsZero() || ws.activeFullGen != gen || !ws.activeFull[slug] {
		return CurrentOpenFullBookReceipt{}, false
	}
	// A completed crawl's absence is explicit terminal/removal evidence even if an out-of-order
	// socket frame remains cached. A stale crawl cannot affirm OPEN, but the current WS frame can.
	if !ws.restOpenAt.IsZero() && !ws.restOpen[slug] {
		return CurrentOpenFullBookReceipt{}, false
	}
	return CurrentOpenFullBookReceipt{
		Bids:     append([]BookLevel(nil), e.bidLevels...),
		Asks:     append([]BookLevel(nil), e.askLevels...),
		SourceAt: e.bookSourceAt.UTC(), ReceivedAt: e.bookAt.UTC(),
		ExplicitOpenAt: e.fullLifecycleAt.UTC(), Generation: gen,
	}, true
}

func (ws *MarketsWS) primaryGenerationHealthyLocked(now time.Time) (uint64, bool) {
	gen := ws.activeGen.Load()
	frameNS := ws.primaryFrameAt.Load()
	dataNS := ws.primaryDataAt.Load()
	if gen == 0 || frameNS <= 0 || dataNS <= 0 {
		return 0, false
	}
	frameAge := now.Sub(time.Unix(0, frameNS))
	dataAge := now.Sub(time.Unix(0, dataNS))
	return gen, frameAge >= 0 && frameAge <= marketsWSPrimaryTransportMaxAge &&
		dataAge >= 0 && dataAge <= marketsWSPrimaryDataMaxAge
}

func (ws *MarketsWS) priceSnapshotCurrentLocked(e *pusLive, now time.Time) bool {
	gen, healthy := ws.primaryGenerationHealthyLocked(now)
	return healthy && e != nil && e.priceGen == gen && e.yes > 0 && e.yes < 1 && !e.at.IsZero()
}

func (ws *MarketsWS) bookSnapshotCurrentLocked(e *pusLive, now time.Time) bool {
	gen, healthy := ws.primaryGenerationHealthyLocked(now)
	return healthy && e != nil && e.bookGen == gen && !e.bookAt.IsZero() &&
		e.bookBid > 0 && e.bookAsk > e.bookBid && e.bookAsk < 1 &&
		(e.bookSourceAt.IsZero() || e.bookSourceAt.Sub(now) <= 5*time.Second)
}

func (ws *MarketsWS) lifecycleOpenFreshLocked(slug string, e *pusLive, now time.Time) bool {
	if e == nil {
		return false
	}
	slug = strings.ToLower(strings.TrimSpace(slug))
	// Once a complete REST universe exists, absence is authoritative terminal/removal evidence.
	if !ws.restOpenAt.IsZero() && !ws.restOpen[slug] {
		return false
	}
	gen, transportOK := ws.primaryGenerationHealthyLocked(now)
	if !transportOK {
		return false
	}
	// Explicit terminal evidence from this connection always wins. An old connection's state is
	// not allowed to poison a newly promoted generation before its own snapshot arrives.
	if e.stateKnown && e.stateGen == gen && !e.marketOpen {
		return false
	}
	restAge := now.Sub(ws.restOpenAt)
	restOpenFresh := !ws.restOpenAt.IsZero() && restAge >= 0 && restAge <= marketsRESTLifecycleMaxAge && ws.restOpen[slug]
	fullOpenFresh := e.stateKnown && e.stateGen == gen && e.marketOpen && !e.fullLifecycleAt.IsZero()
	return restOpenFresh || fullOpenFresh
}

func (ws *MarketsWS) executableQuoteFreshLocked(slug string, e *pusLive, now time.Time) bool {
	if e == nil || e.bid <= 0 || e.ask <= e.bid || e.ask >= 1 ||
		e.quoteAt.IsZero() {
		return false
	}
	gen, healthy := ws.primaryGenerationHealthyLocked(now)
	if !healthy || e.quoteGen != gen || (!e.sourceAt.IsZero() && e.sourceAt.Sub(now) > 5*time.Second) {
		return false
	}
	// MARKET_DATA_LITE normally has no lifecycle. It may borrow only a fresh complete REST-open
	// proof or an explicit OPEN receipt from the same healthy connection generation; a prior
	// generation's stateKnown bit is never execution authority.
	return ws.lifecycleOpenFreshLocked(slug, e, now)
}

// ExecutableCount is the readiness receipt for full/lite BBO execution data. Count intentionally
// remains the broader signal-vitality count; trade-only traffic must not keep execution readiness
// green when full books are stale, halted, one-sided, or lifecycle-unknown.
func (ws *MarketsWS) ExecutableCount() int {
	now := time.Now()
	ws.mu.Lock()
	defer ws.mu.Unlock()
	n := 0
	for slug, e := range ws.live {
		if ws.executableQuoteFreshLocked(slug, e, now) {
			n++
		}
	}
	return n
}

// Book3 returns a slug's live book SHAPE from the full MARKET_DATA stream (R70-B SCHEMA_AUDIT #1):
// top-3 bid share of top-3 depth (0..1), summed top-3 depth (both sides), and the resting size at
// the touch per side (maker fill-odds). The complete snapshot remains current while its connection
// generation is healthy; a reconnect invalidates it until the replacement snapshot lands.
func (ws *MarketsWS) Book3(slug string) (imb3, depth3, bidSz, askSz float64, ok bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	now := time.Now()
	if !ws.bookSnapshotCurrentLocked(e, now) || e.imb3 < 0 || !ws.lifecycleOpenFreshLocked(slug, e, now) {
		return 0, 0, 0, 0, false
	}
	return e.imb3, e.depth3, e.bidSz, e.askSz, true
}

// MomVol returns a slug's short-window momentum (yes - first) and volatility (hi - lo) over the live
// ~3-min price window — a candlestick-style "is this market moving fast" read, computed off the WS.
func (ws *MarketsWS) MomVol(slug string) (mom, vol float64, ok bool) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	e := ws.live[slug]
	if e == nil || e.first <= 0 || time.Since(e.at) > 60*time.Second {
		return 0, 0, false
	}
	return e.yes - e.first, e.hi - e.lo, true
}

// Aggro returns the RECENT aggressive-money lean for a slug from the executed taker tape over the
// last `window` (e.g. 15m): the dominant side (YES/NO), its share of recent taker $ (0..1), and the
// total recent taker $ on the market. ok=false if there's no recent flow. This is the Poly US
// equivalent of the Kalshi whale-consensus "aggressive money" column — real aggressor flow, anonymous.
func (ws *MarketsWS) Aggro(slug string, window time.Duration) (side string, strength, notional float64, ok bool) {
	cut := time.Now().Add(-window)
	var yes, no float64
	ws.mu.Lock()
	for i := range ws.trades {
		t := ws.trades[i]
		if t.slug != slug || t.at.Before(cut) {
			continue
		}
		if t.yesSide {
			yes += t.notional
		} else {
			no += t.notional
		}
	}
	ws.mu.Unlock()
	tot := yes + no
	if tot <= 0 {
		return "", 0, 0, false
	}
	if yes >= no {
		return "YES", yes / tot, tot, true
	}
	return "NO", no / tot, tot, true
}

// TakerStats returns a slug's raw taker tape aggregates over the last `window`: BUY-YES notional,
// BUY-NO notional and the trade count. R67l: feeds the flow_ratio_15m / flow_n_15m ML feature
// family (Aggro folds these into side/strength; the ML wants the raw ratio + n).
func (ws *MarketsWS) TakerStats(slug string, window time.Duration) (yesUSD, noUSD float64, n int) {
	cut := time.Now().Add(-window)
	ws.mu.Lock()
	for i := range ws.trades {
		t := ws.trades[i]
		if t.slug != slug || t.at.Before(cut) {
			continue
		}
		n++
		if t.yesSide {
			yesUSD += t.notional
		} else {
			noUSD += t.notional
		}
	}
	ws.mu.Unlock()
	return yesUSD, noUSD, n
}

// RawFlow returns every valid positive print retained in the bounded rolling buffer within maxAge.
// It has intentionally no notional minimum. Consumers may score relative significance later, but
// observation itself must never erase the small-flow distribution used as that baseline.
func (ws *MarketsWS) RawFlow(maxAge time.Duration) []RawFlowPrint {
	cut := time.Time{}
	if maxAge > 0 {
		cut = time.Now().Add(-maxAge)
	}
	ws.mu.Lock()
	out := make([]RawFlowPrint, 0, len(ws.trades))
	for _, t := range ws.trades {
		if !cut.IsZero() && t.at.Before(cut) {
			continue
		}
		side := "NO"
		if t.yesSide {
			side = "YES"
		}
		out = append(out, RawFlowPrint{Slug: t.slug, Side: side, Action: "BUY", Notional: t.notional,
			Price: t.px, YesPrice: t.yesPx, At: t.at})
	}
	ws.mu.Unlock()
	return out
}

// TradeThrough reports whether an opposite-side aggressive print since `since` proves that a
// resting BUY of `side` traded strictly THROUGH postPx. A print exactly at our level cannot prove
// our queue-unknown order filled; it may have consumed older orders ahead of us. Midpoint movement
// is not evidence either. PolyUS currently exposes no stable public sequence/queue identifier.
func (ws *MarketsWS) TradeThrough(slug, side string, postPx float64, since time.Time) bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	wantYes := strings.EqualFold(side, "YES")
	for i := len(ws.trades) - 1; i >= 0; i-- {
		t := ws.trades[i]
		if t.at.Before(since) {
			break
		}
		if t.slug != slug || t.yesPx <= 0 || t.yesPx >= 1 || t.yesSide == wantYes {
			continue // our resting bid needs the opposite outcome's aggressor
		}
		outcomePx := t.yesPx
		if !wantYes {
			outcomePx = 1 - t.yesPx
		}
		if outcomePx < postPx-1e-9 {
			return true
		}
	}
	return false
}

// Whales returns recent large Poly US taker prints (>= minNotional within maxAge), newest-biggest
// first — the feed behind the Poly US whales panel. Slug-keyed; the caller joins game/team metadata.
func (ws *MarketsWS) Whales(minNotional float64, maxAge time.Duration) []WhalePrint {
	cut := time.Now().Add(-maxAge)
	out := make([]WhalePrint, 0, 32)
	ws.mu.Lock()
	for i := range ws.trades {
		t := ws.trades[i]
		if t.notional < minNotional || t.at.Before(cut) {
			continue
		}
		sd := "NO"
		if t.yesSide {
			sd = "YES"
		}
		out = append(out, WhalePrint{Slug: t.slug, Side: sd, Notional: t.notional, Price: t.px, At: t.at.Unix()})
	}
	ws.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].At > out[j].At }) // newest first (recency)
	if len(out) > 60 {
		out = out[:60]
	}
	return out
}

// Count returns how many slugs have a price observation from the healthy active generation.
func (ws *MarketsWS) Count() int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	n := 0
	now := time.Now()
	for _, e := range ws.live {
		if ws.priceSnapshotCurrentLocked(e, now) {
			n++
		}
	}
	return n
}

// FullSourceClock and LiteArrivalClock expose the two documented clock contracts independently.
func (ws *MarketsWS) FullSourceClock() (watermark time.Time, frames int64) {
	if ns := ws.fullSourceAt.Load(); ns > 0 {
		watermark = time.Unix(0, ns).UTC()
	}
	return watermark, ws.fullFrames.Load()
}

func (ws *MarketsWS) LiteArrivalClock() (received time.Time, frames int64) {
	if ns := ws.liteAt.Load(); ns > 0 {
		received = time.Unix(0, ns).UTC()
	}
	return received, ws.liteFrames.Load()
}
