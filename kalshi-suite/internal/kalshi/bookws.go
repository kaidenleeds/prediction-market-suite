package kalshi

// R74 LIVE ORDER BOOKS — real-time books over the orderbook_delta WebSocket channel, replacing the
// REST /orderbook poll as the primary source for a dynamic WORKING SET of markets (never the whole
// 14k universe). Contract verified against the venue's AsyncAPI (docs.kalshi.com/asyncapi.yaml via
// docs.kalshi.com/websockets/orderbook-updates.md + websocket-connection.md, fetched 2026-07-04):
//
//	subscribe            {"id":N,"cmd":"subscribe","params":{"channels":["orderbook_delta"],
//	                      "market_tickers":[...],"use_yes_price":false}} — market specification REQUIRED (market_ticker or
//	                      market_tickers; market_id/market_ids NOT supported on this channel). There
//	                      is no "all markets" mode. Auth required (rides our signed WS handshake).
//	                      The explicit false pins the two-scale convention this decoder uses (NO
//	                      levels are NO-leg prices); Kalshi plans to flip the omitted default.
//	→ subscribed         {"id":N,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":S}}
//	→ orderbook_snapshot {"type":"orderbook_snapshot","sid":S,"seq":Q,"msg":{"market_ticker":"…",
//	                      "yes_dollars_fp":[["0.0800","300.00"],…],"no_dollars_fp":[[…]]}}
//	                      Levels are [price_dollars, contract_count_fp] STRING pairs; a side's key is
//	                      ABSENT when it has no resting offers. Both sides are BIDS (yes bids + no
//	                      bids) — a NO bid at q is a YES ask at 1−q, exactly like the REST orderbook.
//	→ orderbook_delta    {"type":"orderbook_delta","sid":S,"seq":Q,"msg":{"market_ticker":"…",
//	                      "price_dollars":"0.9600","delta_fp":"-54.00","side":"yes"|"no",
//	                      "client_order_id":"…" (present ONLY when OUR order caused the change),
//	                      "ts_ms":1669149841000}} — delta_fp is SIGNED; a level at ≤0 is gone.
//	seq                  per-sid sequence: "should be checked if you want to guarantee you received
//	                      all the messages. Used for snapshot/delta consistency." The counter is per
//	                      SUBSCRIPTION (shared by every market on the sid), so ONE missed message
//	                      makes EVERY book on the sid unprovable → invalidate all, re-baseline.
//	update_subscription  {"id":N,"cmd":"update_subscription","params":{"sid":S,"market_tickers":[…],
//	                      "action":"add_markets"|"delete_markets"|"get_snapshot"}}. get_snapshot
//	                      pushes fresh orderbook_snapshot frames for the named tickers WITHOUT
//	                      changing the subscription — the documented resync path after a seq gap.
//	                      The "ok" response carries the FULL post-update ticker list (authoritative).
//	limits               the AsyncAPI documents NO per-connection market cap for this channel (the
//	                      error table, codes 1–22, has no "too many markets" code either). We
//	                      self-cap via kalshi_book_ws_cap (default 600, clamp 50–1000).
//
// ROLE PATTERN (R41 warm-standby parity): the book subscription lives on the PRIMARY session only.
// The standby stays ticker-subscribed-but-draining as before; on promotion the book state is
// INVALIDATED and the working set is re-subscribed on the promoted socket (fresh snapshots re-baseline
// every book) — correctness over continuity, since a drained standby has no provable seq chain.

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bookSubChunk    = 100  // tickers in the initial subscribe command
	bookAddChunk    = 10   // added books each push one snapshot back; small bursts avoid venue code-25 buffer overflow
	bookDepthCap    = 10   // exported ladder depth per side (LiveBook view cap)
	bookLevelCap    = 1024 // per-side stored-level guard (real books run <200 levels; this only stops pathology)
	bookWantCap     = 1000 // hard client-side cap on the working set (mirrors the config clamp ceiling)
	bookResyncWait  = 2 * time.Second
	bookUpdateWait  = 30 * time.Second // one update must ACK + drain its requested snapshots inside this bound
	bookUpdateRetry = 2 * time.Second  // healthy transport + missing control ACK: retry only the exact diff
	bookDarkWait    = 60 * time.Second // forced-redial guard: books wanted but ZERO provable for this long ⇒ kill the socket
)

// wsBook is one market's live book: FULL ladders of resting YES bids and NO bids, keyed by price in
// 1/100¢ ticks (dollars×10000 — covers the venue's subpenny pricing). The store keeps the whole
// ladder because a delta can hit any level and deep levels become the touch as the top is consumed;
// only the EXPORTED view (LiveBook) is capped at bookDepthCap levels/side. valid=false between a seq
// gap and the healing snapshot — readers get nothing rather than a book we can't prove.
//
// AUTHORITY is snapshot + unbroken per-sid sequence + owning-transport liveness. This channel is
// event-driven, so no orderbook frame is expected while prices remain unchanged; recent application
// traffic on the same ordered socket keeps that snapshot current. Terminal channel errors and seq
// gaps still invalidate immediately. bk.at (this individual book's last change) is display-only.
type wsBook struct {
	yes, no         map[int]float64 // price key -> resting contracts
	bestYes, bestNo int             // cached best (highest) bid key per side; 0 = side empty
	valid           bool
	sourceAt        time.Time // venue timestamp of latest applied frame; zero remains unknown
	sequence        int64     // actual source sequence of latest applied frame
	at              time.Time // last snapshot/delta applied (display/debug — NOT the freshness gate)
}

// bookWS is the client's orderbook_delta subscription manager + book store. One instance per Client;
// one subscription (sid) on the current PRIMARY wsSession.
type bookWS struct {
	frameAt      atomic.Int64 // unix-nano of the last accepted orderbook snapshot/delta (observability only)
	mu           sync.Mutex
	sess         *wsSession // session that owns the subscription (the primary); nil until first primary
	sid          int64      // server subscription id; 0 = not subscribed
	generation   uint64     // increments whenever a new primary owns the channel
	lastSeq      int64      // last seq seen on sid (0 = chain not started)
	lastPriorSeq int64      // actual prior frame sequence for lastSeq (never a synthetic receipt range)
	lastGapPrior int64      // exact endpoints of the latest detected break/replay
	lastGapSeq   int64
	cmdID        int64                // client command id counter (per docs: unique per WS session; monotonic is fine)
	pendingSubID int64                // id of the in-flight subscribe command (0 = none)
	subSent      []string             // tickers named in the in-flight subscribe (adopted as subbed on ack)
	initialLeft  map[string]bool      // first subscribe snapshots still owed before add chunks may start
	subbed       map[string]bool      // tickers the server has on the sid (optimistic on send; ok responses overwrite)
	want         []string             // latest desired working set from the server (priority-ordered)
	books        map[string]*wsBook   // ticker -> live book
	resyncAt     time.Time            // last get_snapshot storm (throttle)
	resyncPend   bool                 // a deferred bookResync retry is armed (throttled/failed resyncs re-arm; never stacks)
	resyncNeeded bool                 // a seq gap is still being healed; normal add/delete reconciliation waits
	gaps         int64                // seq gaps seen (health counter)
	lastErr      string               // last WS command error (health; cleared when the feed provably recovers)
	pendingUpd   map[int64]bookUpdCmd // at most ONE in-flight update_subscription command (backpressure + rollback)
	darkSince    time.Time            // start of the current zero-provable-books stretch (zero = not dark)
	errRedialAt  time.Time            // last terminal channel-error socket kill (10/17/25; throttles kills to 1/min)
}

// bookUpdCmd is one in-flight update_subscription command. syncBooks marks subbed OPTIMISTICALLY at
// send time (so back-to-back passes don't resend the same deltas) — which means a venue ERROR frame
// (or a write that never reached the wire) must be able to UNDO exactly that command's marks, or the
// 30s reconcile diff forever believes the venue took the change and never repairs it (production
// incident: error-frame'd adds left permanently dark books the health surface called subscribed).
type bookUpdCmd struct {
	action    string // add_markets | delete_markets | get_snapshot
	tickers   []string
	acked     bool            // delete completes here; add/snapshot also wait for every requested snapshot
	remaining map[string]bool // snapshots still owed by add_markets/get_snapshot (nil for delete)
}

func bookPriceKey(s string) (int, bool) {
	p, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	k := int(math.Round(p * 10000))
	if k <= 0 || k >= 10000 {
		return 0, false
	}
	return k, true
}

func bookMaxKey(m map[int]float64) int {
	best := 0
	for k := range m {
		if k > best {
			best = k
		}
	}
	return best
}

// bookFrameFresh reports whether the orderbook channel delivered a snapshot/delta within maxAge.
// It is a useful receipt, but not execution authority by itself: orderbook_delta is event-driven,
// so a perfectly current unchanged snapshot can be quiet for much longer than five seconds.
func (b *bookWS) bookFrameFresh(maxAge time.Duration) bool {
	n := b.frameAt.Load()
	return n > 0 && time.Since(time.Unix(0, n)) <= maxAge
}

// transportFreshLocked proves that the acknowledged book subscription still belongs to a live
// multiplexed socket. Kalshi sends ticker/trade/lifecycle/account and book frames over the same
// ordered WebSocket; recent application traffic keeps an unchanged valid snapshot authoritative.
// Pongs deliberately do not count, so an application-dark but ponging connection still fails shut.
// Caller holds b.mu.
func (b *bookWS) transportFreshLocked(maxAge time.Duration) bool {
	if b.sess == nil || b.sid <= 0 {
		return false
	}
	n := b.sess.frameAt.Load()
	return n > 0 && time.Since(time.Unix(0, n)) <= maxAge
}

func parseBookLadder(pairs [][]string) map[int]float64 {
	out := make(map[int]float64, len(pairs))
	for _, pair := range pairs {
		if len(pair) < 2 {
			continue
		}
		k, ok := bookPriceKey(pair[0])
		sz, err := strconv.ParseFloat(pair[1], 64)
		if !ok || err != nil || sz <= 0 {
			continue
		}
		out[k] = sz
	}
	return out
}

// SyncBookSubscriptions replaces the desired book working set (priority-ordered tickers; the server
// computes it — positions/resting/proposals/combo legs + top-volume fill) and reconciles the live
// subscription with unsub/sub DELTAS only. Safe to call before any WS session exists: the set is
// remembered and subscribed when a primary comes up.
func (c *Client) SyncBookSubscriptions(tickers []string) {
	seen := make(map[string]bool, len(tickers))
	want := make([]string, 0, len(tickers))
	for _, t := range tickers {
		if t == "" || seen[t] {
			continue
		}
		seen[t] = true
		want = append(want, t)
		if len(want) >= bookWantCap {
			break
		}
	}
	c.books.mu.Lock()
	c.books.want = want
	c.books.mu.Unlock()
	c.syncBooks()
}

// PrioritizeBookSubscription moves one real-money candidate to the front of the existing bounded
// desired set and reconciles immediately. It never increases the 1,000-book ceiling: at capacity
// the lowest-priority tail market yields its seat. The server's next normal plan retains the ticker
// through its short-lived LIVE-signal hint, so this is a fast wake rather than a competing planner.
func (c *Client) PrioritizeBookSubscription(ticker string) bool {
	return c.PrioritizeBookSubscriptions([]string{ticker})
}

// PrioritizeBookSubscriptions coalesces a burst of real-money hints into one subscription update.
// The caller supplies newest/most-urgent first; duplicates and blanks are removed.
func (c *Client) PrioritizeBookSubscriptions(tickers []string) bool {
	priority := make([]string, 0, len(tickers))
	seen := make(map[string]bool, len(tickers))
	for _, ticker := range tickers {
		ticker = strings.TrimSpace(ticker)
		if ticker != "" && !seen[ticker] && len(priority) < bookWantCap {
			seen[ticker] = true
			priority = append(priority, ticker)
		}
	}
	if len(priority) == 0 {
		return false
	}
	b := &c.books
	b.mu.Lock()
	want := make([]string, 0, len(b.want)+len(priority))
	want = append(want, priority...)
	for _, existing := range b.want {
		if existing != "" && !seen[existing] && len(want) < bookWantCap {
			want = append(want, existing)
		}
	}
	changed := len(want) != len(b.want)
	if !changed {
		for i := range want {
			if want[i] != b.want[i] {
				changed = true
				break
			}
		}
	}
	b.want = want
	b.mu.Unlock()
	if changed {
		c.syncBooks()
	}
	return changed
}

// bookPrimaryChanged is called whenever a session becomes the PRIMARY (first dial, promotion,
// both-down recovery). Books stream on the primary only, so all state tied to the old socket is
// discarded — sid, seq chain, and every book — and the working set is re-subscribed on the new
// socket. The server then pushes fresh snapshots, re-baselining each book (R74 §4: books are
// invalidated on failover; correctness over continuity).
func (c *Client) bookPrimaryChanged(s *wsSession) {
	b := &c.books
	b.mu.Lock()
	b.sess = s
	if s != nil && s.generation == 0 {
		s.generation = c.wsSessionGeneration.Add(1)
	}
	if s != nil {
		b.generation = s.generation
	}
	b.sid = 0
	b.lastSeq = 0
	b.lastPriorSeq = 0
	b.pendingSubID = 0
	b.subSent = nil
	b.initialLeft = nil
	b.subbed = nil
	b.pendingUpd = nil        // in-flight commands died with the old socket (their late replies are sess-gated off anyway)
	b.resyncNeeded = false    // fresh snapshots on the new sid replace any old-socket gap recovery
	b.darkSince = time.Time{} // fresh socket ⇒ restart the forced-redial dark clock
	b.books = map[string]*wsBook{}
	b.frameAt.Store(0)
	if b.cmdID < 1000 {
		b.cmdID = 1000 // dialTickerSession's channel subscribe used id 1 on this socket — keep book command ids disjoint
	}
	n := len(b.want)
	b.mu.Unlock()
	if n > 0 {
		c.syncBooks()
	}
}

// bookSubscribeCommand is kept in one constructor so the wire convention cannot drift. This book
// store interprets NO levels as NO-leg prices and converts them to YES asks (1-price), so
// use_yes_price MUST remain explicitly false while that decoder is in service. Depending on today's
// default would invert asks when Kalshi flips the omitted default to unified YES pricing.
func bookSubscribeCommand(id int64, tickers []string) map[string]any {
	return map[string]any{
		"id":  id,
		"cmd": "subscribe",
		"params": map[string]any{
			"channels":        []string{"orderbook_delta"},
			"market_tickers":  tickers,
			"skip_ticker_ack": true, // omit the full (up to 1,000 ticker) list from every update OK
			"use_yes_price":   false,
		},
	}
}

func bookTerminalError(code int) bool {
	switch code {
	case 10, 17, 25: // channel error, server internal error, subscription buffer overflow
		return true
	default:
		return false
	}
}

// syncBooks reconciles desired vs subscribed on the current primary: the initial subscribe carries
// the first chunk (one subscription per channel per connection — error 6 guards duplicates); the
// remainder and every later change flow as update_subscription add_markets/delete_markets chunks.
// Commands are written on a goroutine so WS read loops and server tickers never block on a send.
func (c *Client) syncBooks() {
	b := &c.books
	b.mu.Lock()
	s := b.sess
	if s == nil {
		b.mu.Unlock()
		return
	}
	// FORCED-REDIAL GUARD (stale-over-dead incident): "subscribed" can silently rot — a subscribe the
	// venue never answered (pendingSubID latched forever), or a subscription it dropped without an
	// error frame. The reconcile can't repair what the venue no longer acknowledges, so if we WANT
	// books and the primary is believed connected yet ZERO books have been provable for >bookDarkWait,
	// kill the socket: the supervisor promotes the warm standby (~zero ticker gap) and
	// bookPrimaryChanged re-subscribes the working set from scratch. A quiet-but-healthy book never
	// trips this: the channel is event-driven, so a valid snapshot stays provable while the
	// acknowledged owning socket has recent application traffic. Requiring a recent BOOK delta here
	// redialed healthy quiet markets roughly once per dark window.
	// Runs on the reconcile's 30s cadence, so detection lands within bookDarkWait+30s.
	if len(b.want) > 0 {
		dark := true
		if b.transportFreshLocked(bookDarkWait) {
			for _, bk := range b.books {
				if bk.valid {
					dark = false
					break
				}
			}
		}
		switch {
		case !dark:
			b.darkSince = time.Time{}
		case b.darkSince.IsZero():
			b.darkSince = time.Now()
		case time.Since(b.darkSince) > bookDarkWait:
			b.darkSince = time.Now() // re-arm: if this close is slow to take effect, re-fire only after another full window
			b.mu.Unlock()
			if s.conn != nil {
				_ = s.conn.Close() // read loop errors out → onDead → promotion/redial → bookPrimaryChanged
			}
			return // no point writing commands on a socket we just condemned
		}
	} else {
		b.darkSince = time.Time{} // nothing wanted ⇒ nothing can be dark
	}
	if b.resyncNeeded {
		b.mu.Unlock()
		go c.bookResync()
		return
	}
	var cmds []map[string]any
	if b.sid == 0 {
		if b.pendingSubID != 0 || len(b.want) == 0 {
			b.mu.Unlock()
			return
		}
		n := len(b.want)
		if n > bookSubChunk {
			n = bookSubChunk
		}
		first := append([]string(nil), b.want[:n]...)
		b.cmdID++
		b.pendingSubID = b.cmdID
		b.subSent = first
		cmds = append(cmds, bookSubscribeCommand(b.cmdID, first))
	} else {
		// Kalshi code 25 means the subscription's outbound buffer overflowed. An add produces one
		// full order-book snapshot per ticker, so queue exactly one chunk and wait for its receipt.
		if len(b.initialLeft) > 0 || len(b.pendingUpd) > 0 {
			b.mu.Unlock()
			return
		}
		wantSet := make(map[string]bool, len(b.want))
		for _, t := range b.want {
			wantSet[t] = true
		}
		var adds, dels []string
		for _, t := range b.want {
			if !b.subbed[t] {
				adds = append(adds, t)
			}
		}
		for t := range b.subbed {
			if !wantSet[t] {
				dels = append(dels, t)
			}
		}
		sort.Strings(dels) // deterministic frames (map iteration order otherwise)
		if b.pendingUpd == nil {
			b.pendingUpd = map[int64]bookUpdCmd{}
		}
		// Replacements at the bounded working-set ceiling must free their seats before adding the
		// newly prioritized LIVE tickers. Sending the add first can transiently ask the venue for
		// book #1001 and reject the exact real-money market this reconciliation is meant to admit.
		// Deletes therefore precede adds; optimistic bookkeeping below still describes the same
		// final desired set, and each command retains its independent rollback receipt.
		var action string
		var chunk []string
		switch {
		case len(dels) > 0:
			action = "delete_markets"
			n := len(dels)
			if n > bookSubChunk {
				n = bookSubChunk
			}
			chunk = append([]string(nil), dels[:n]...)
		case len(adds) > 0:
			action = "add_markets"
			n := len(adds)
			if n > bookAddChunk {
				n = bookAddChunk
			}
			chunk = append([]string(nil), adds[:n]...)
		}
		if len(chunk) > 0 {
			b.cmdID++
			upd := bookUpdCmd{action: action, tickers: chunk}
			if action == "add_markets" {
				upd.remaining = make(map[string]bool, len(chunk))
				for _, t := range chunk {
					upd.remaining[t] = true
				}
			}
			b.pendingUpd[b.cmdID] = upd
			cmds = append(cmds, map[string]any{"id": b.cmdID, "cmd": "update_subscription",
				"params": map[string]any{"sids": []int64{b.sid}, "market_tickers": chunk, "action": action}})
		}
		// Optimistic bookkeeping covers ONLY the one command put on the wire. A rejection/write
		// failure rolls this exact chunk back; later diffs discover and queue the remaining chunks.
		if b.subbed == nil {
			b.subbed = map[string]bool{}
		}
		switch action {
		case "add_markets":
			for _, t := range chunk {
				b.subbed[t] = true
			}
		case "delete_markets":
			for _, t := range chunk {
				delete(b.subbed, t)
			}
		}
	}
	b.mu.Unlock()
	if len(cmds) > 0 {
		go func() {
			for i, cmd := range cmds {
				if err := s.writeJSON(cmd); err != nil {
					b.mu.Lock()
					b.lastErr = err.Error()
					if b.sess == s {
						// A failed WRITE gets no server response, so an in-flight subscribe latch
						// would never clear — release it so the next sync pass can retry. (If the
						// bytes actually reached the wire, the retry draws error 6 "Already
						// subscribed" — logged, harmless, and healed by the next failover.)
						b.pendingSubID = 0
						b.subSent = nil
						// Same reasoning for update commands: the failed one and everything after it
						// never reached the venue, so their optimistic subbed marks must be undone or
						// the reconcile diff would believe the venue took them and never re-send.
						for _, cc := range cmds[i:] {
							if id, ok := cc["id"].(int64); ok {
								c.bookRollbackUpd(id)
							}
						}
					}
					b.mu.Unlock()
					return // socket is likely dying; the supervisor will hand us a new primary
				}
				if id, ok := cmd["id"].(int64); ok {
					c.bookArmUpdateTimeout(s, id)
				}
			}
		}()
	}
}

// bookResync heals a seq gap: one update_subscription get_snapshot for EVERY subscribed ticker (the
// documented path — fresh snapshots re-baseline without touching the subscription). Throttled so a
// gap burst can't storm the server; books stay invalid (unreadable) until their snapshot lands.
// A resync that can't run RIGHT NOW is never dropped: throttled and write-failed attempts re-arm a
// single deferred retry (bookResyncRetry) — a silently dropped resync used to leave every
// invalidated book permanently unreadable unless an unrelated later gap happened to fire a new one.
func (c *Client) bookResync() {
	c.bookResyncChunk(true)
}

// bookResyncContinue advances after the previous chunk's snapshots drained. Only the first chunk
// needs the gap-storm throttle; proven-drained continuation chunks can move immediately.
func (c *Client) bookResyncContinue() {
	c.bookResyncChunk(false)
}

func (c *Client) bookResyncChunk(throttle bool) {
	b := &c.books
	b.mu.Lock()
	s := b.sess
	if s == nil || b.sid == 0 || !b.resyncNeeded {
		b.mu.Unlock()
		return // no live subscription — the next (re)subscribe re-baselines every book anyway
	}
	if len(b.initialLeft) > 0 || len(b.pendingUpd) > 0 {
		b.mu.Unlock()
		return // completion of the one in-flight command resumes this repair
	}
	if wait := bookResyncWait - time.Since(b.resyncAt); throttle && wait > 0 {
		c.bookResyncRetry(wait) // throttled — retry once the window opens instead of vanishing
		b.mu.Unlock()
		return
	}
	all := make([]string, 0, len(b.subbed))
	for ticker := range b.subbed {
		if bk := b.books[ticker]; bk == nil || !bk.valid {
			all = append(all, ticker)
		}
	}
	if len(all) == 0 {
		b.resyncNeeded = false
		b.mu.Unlock()
		c.syncBooks()
		return
	}
	if throttle {
		b.resyncAt = time.Now()
	}
	if b.pendingUpd == nil {
		b.pendingUpd = map[int64]bookUpdCmd{}
	}
	sort.Strings(all)
	n := len(all)
	if n > bookSubChunk {
		n = bookSubChunk
	}
	b.cmdID++
	id := b.cmdID
	chunk := append([]string(nil), all[:n]...)
	remaining := make(map[string]bool, len(chunk))
	for _, ticker := range chunk {
		remaining[ticker] = true
	}
	b.pendingUpd[id] = bookUpdCmd{
		action: "get_snapshot", tickers: chunk, remaining: remaining,
	}
	cmd := map[string]any{"id": id, "cmd": "update_subscription",
		"params": map[string]any{"sids": []int64{b.sid}, "market_tickers": chunk, "action": "get_snapshot"}}
	b.mu.Unlock()
	go func() {
		if err := s.writeJSON(cmd); err != nil {
			b.mu.Lock()
			b.lastErr = err.Error()
			if b.sess == s {
				c.bookRollbackUpd(id)
			}
			b.mu.Unlock()
			return
		}
		c.bookArmUpdateTimeout(s, id)
	}()
}

// bookResyncRetry arms ONE deferred bookResync attempt after `wait` (caller holds books.mu). The
// timer never stacks (resyncPend), so a gap burst arms at most one retry, and bookResync re-arms
// itself while still throttled — the chain is bounded at one attempt per bookResyncWait, no busy
// loop. The epsilon lands the retry just past the throttle window that blocked it. A retry that
// fires after a failover no-ops in bookResync (sid gone / books re-baselined).
func (c *Client) bookResyncRetry(wait time.Duration) {
	b := &c.books
	if b.resyncPend {
		return
	}
	b.resyncPend = true
	time.AfterFunc(wait+50*time.Millisecond, func() {
		b.mu.Lock()
		b.resyncPend = false
		b.mu.Unlock()
		c.bookResync()
	})
}

// bookArmUpdateTimeout makes a command that stops producing receipts fail closed. The timer starts
// only after the websocket write succeeds; a completed/old-session command makes the callback a
// no-op. Closing the socket forces a clean sid + snapshot baseline instead of leaving optimistic
// subscription state wedged forever.
func (c *Client) bookArmUpdateTimeout(s *wsSession, id int64) {
	b := &c.books
	b.mu.Lock()
	_, pending := b.pendingUpd[id]
	pending = pending && b.sess == s
	b.mu.Unlock()
	if !pending {
		return
	}
	time.AfterFunc(bookUpdateWait, func() {
		c.bookUpdateTimedOut(s, id)
	})
}

func (c *Client) bookUpdateTimedOut(s *wsSession, id int64) {
	b := &c.books
	b.mu.Lock()
	upd, pending := b.pendingUpd[id]
	if !pending || b.sess != s {
		b.mu.Unlock()
		return
	}
	// A delete changes only the named markets. If this same multiplexed socket is still carrying
	// application frames, its other books retain an unbroken sequence chain: roll back only the
	// optimistic deletion and retry it. This is not a global book-health failure: latching lastErr
	// here used to pause every LIVE route despite hundreds of current, sequence-valid books.
	//
	// A stale transport, add timeout, or get_snapshot timeout is different. The requested market
	// or sequence proof is unavailable, so those cases still invalidate every book and force a
	// clean re-subscription.
	if upd.action == "delete_markets" && b.transportFreshLocked(5*time.Second) {
		c.bookRollbackUpd(id)
		b.lastErr = ""
		b.mu.Unlock()
		time.AfterFunc(bookUpdateRetry, c.syncBooks)
		return
	}
	b.lastErr = "ws update timeout: " + upd.action + " (" + strconv.Itoa(len(upd.tickers)) + " markets)"
	for _, bk := range b.books {
		bk.valid = false
	}
	b.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
}

func (c *Client) bookArmInitialTimeout(s *wsSession, sid int64) {
	time.AfterFunc(bookUpdateWait, func() {
		b := &c.books
		b.mu.Lock()
		if b.sess != s || b.sid != sid || len(b.initialLeft) == 0 {
			b.mu.Unlock()
			return
		}
		b.lastErr = "ws subscribe snapshot timeout (" + strconv.Itoa(len(b.initialLeft)) + " markets remaining)"
		for _, bk := range b.books {
			bk.valid = false
		}
		b.mu.Unlock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
	})
}

// bookAdvance chooses the next state-machine lane after the sole pending command completes.
// Snapshot recovery outranks normal working-set churn; a recovery chunk can continue immediately
// because receiving every requested snapshot proves the venue's outbound queue drained that chunk.
func (c *Client) bookAdvance(action string) {
	if action == "get_snapshot" {
		c.bookResyncContinue()
		return
	}
	b := &c.books
	b.mu.Lock()
	resync := b.resyncNeeded
	b.mu.Unlock()
	if resync {
		c.bookResync()
		return
	}
	c.syncBooks()
}

// bookRollbackUpd undoes ONE in-flight update_subscription command that the venue rejected (error
// frame) or that never reached the wire (failed write). Caller holds books.mu. Rolling back the
// optimistic marks is what lets the 30s reconcile actually repair the subscription: a rejected add
// is un-marked (else the diff believes the venue streams it and never re-sends), a rejected delete
// is re-marked (the venue still streams it, so it must be re-deleted), and a rejected get_snapshot
// re-arms the resync (the books it was meant to heal are still invalid). Idempotent against the ok
// path: an authoritative post-update ticker list may land first and turn these edits into no-ops.
func (c *Client) bookRollbackUpd(id int64) {
	b := &c.books
	upd, ok := b.pendingUpd[id]
	if !ok {
		return
	}
	delete(b.pendingUpd, id)
	switch upd.action {
	case "add_markets":
		for _, t := range upd.tickers {
			delete(b.subbed, t)
			delete(b.books, t)
		}
	case "delete_markets":
		if b.subbed == nil {
			b.subbed = map[string]bool{}
		}
		for _, t := range upd.tickers {
			b.subbed[t] = true
		}
	case "get_snapshot":
		c.bookResyncRetry(bookResyncWait)
	}
}

var (
	bookTagOrderbook = []byte("orderbook_")     // matches "type":"orderbook_snapshot" / "orderbook_delta" (and the subscribed ack's channel name)
	bookTagSubbed    = []byte(`"subscribed"`)   //
	bookTagOK        = []byte(`"type":"ok"`)    // update_subscription confirmations (compact server JSON)
	bookTagErr       = []byte(`"type":"error"`) //
)

// ingestBook routes one WS frame into the book engine. Called from the session read loop AFTER the
// primary gate (a standby never subscribes to books, so it never sees these frames anyway). The
// byte-scan fast path keeps the 3k-market ticker torrent from paying a JSON decode here.
func (c *Client) ingestBook(data []byte, s *wsSession) {
	if !bytes.Contains(data, bookTagOrderbook) && !bytes.Contains(data, bookTagSubbed) &&
		!bytes.Contains(data, bookTagOK) && !bytes.Contains(data, bookTagErr) {
		return
	}
	var env struct {
		ID   int64           `json:"id"`
		Type string          `json:"type"`
		SID  int64           `json:"sid"`
		Seq  int64           `json:"seq"`
		Msg  json.RawMessage `json:"msg"`
	}
	if json.Unmarshal(data, &env) != nil {
		return
	}
	switch env.Type {
	case "subscribed":
		var m struct {
			Channel string `json:"channel"`
			SID     int64  `json:"sid"`
		}
		if json.Unmarshal(env.Msg, &m) != nil || m.Channel != "orderbook_delta" || m.SID <= 0 {
			return
		}
		b := &c.books
		b.mu.Lock()
		if s != b.sess {
			b.mu.Unlock()
			return // ack from a session that is no longer the book owner
		}
		b.sid = m.SID
		b.lastSeq = 0
		b.pendingSubID = 0
		b.lastErr = "" // the subscribe round-tripped — don't latch a pre-recovery error over a healthy feed
		b.subbed = make(map[string]bool, len(b.subSent))
		b.initialLeft = make(map[string]bool, len(b.subSent))
		for _, t := range b.subSent {
			b.subbed[t] = true
			b.initialLeft[t] = true
		}
		b.subSent = nil
		waiting := len(b.initialLeft)
		b.mu.Unlock()
		if waiting == 0 {
			c.syncBooks()
		} else {
			c.bookArmInitialTimeout(s, m.SID)
		}
	case "ok":
		var m struct {
			MarketTickers []string `json:"market_tickers"`
		}
		_ = json.Unmarshal(env.Msg, &m)
		b := &c.books
		completed := ""
		b.mu.Lock()
		if s == b.sess && b.sid != 0 && env.SID == b.sid {
			b.lastErr = "" // a command round-tripped OK — unlatch the health error (stale-error-over-healed incident)
			if env.ID != 0 {
				if upd, pending := b.pendingUpd[env.ID]; pending {
					upd.acked = true
					if upd.action == "delete_markets" || len(upd.remaining) == 0 {
						delete(b.pendingUpd, env.ID)
						completed = upd.action
						if upd.action == "delete_markets" {
							for _, ticker := range upd.tickers {
								delete(b.books, ticker)
							}
						}
					} else {
						b.pendingUpd[env.ID] = upd
					}
				}
			}
			// The ok's seq shares the snapshot/delta schema. Adopt it ONLY when it's the exact next
			// link — if the server didn't consume a slot this is a no-op; if it did and we skipped
			// it, the next delta triggers ONE gap-resync and heals. Never a false chain-reset.
			if env.Seq == b.lastSeq+1 {
				b.lastSeq = env.Seq
			}
			if len(m.MarketTickers) > 0 { // full post-update list = authoritative subscription state
				b.subbed = make(map[string]bool, len(m.MarketTickers))
				for _, t := range m.MarketTickers {
					b.subbed[t] = true
				}
				for t := range b.books {
					if !b.subbed[t] {
						delete(b.books, t)
					}
				}
			}
		}
		b.mu.Unlock()
		if completed != "" {
			go c.bookAdvance(completed)
		}
	case "error":
		var m struct {
			Code int    `json:"code"`
			Msg  string `json:"msg"`
		}
		_ = json.Unmarshal(env.Msg, &m)
		b := &c.books
		kill := false
		b.mu.Lock()
		if s == b.sess {
			b.lastErr = "ws error " + strconv.Itoa(m.Code) + ": " + m.Msg
			if env.ID != 0 && env.ID == b.pendingSubID {
				b.pendingSubID = 0 // failed subscribe — a later sync pass retries
				b.subSent = nil
				b.initialLeft = nil
			}
			if env.ID != 0 {
				// Failed update_subscription: undo its optimistic marks (or re-arm the resync it
				// carried) so the reconcile actually re-sends — an error frame used to leave the
				// diff believing the venue took the change, permanently.
				c.bookRollbackUpd(env.ID)
			}
			// Codes 10 (channel error), 17 (server internal error), and 25 (subscription buffer
			// overflow) make the channel's state unprovable. Invalidate first, then condemn the
			// socket: the supervisor promotes/redials and bookPrimaryChanged re-subscribes the full
			// working set from fresh snapshots. A command rollback alone cannot heal id-less errors
			// or partial buffer drops. Socket kills are throttled to one/minute; invalid books fail
			// closed during that window and the dark-feed guard forces another redial if necessary.
			if bookTerminalError(m.Code) {
				for _, bk := range b.books {
					bk.valid = false
				}
			}
			if bookTerminalError(m.Code) && time.Since(b.errRedialAt) > time.Minute {
				b.errRedialAt = time.Now()
				kill = true
			}
		}
		b.mu.Unlock()
		if kill && s.conn != nil {
			_ = s.conn.Close() // read loop errors out → onDead → promotion/redial → bookPrimaryChanged
		}
	case "orderbook_snapshot", "orderbook_delta":
		c.ingestBookData(env.Type, env.SID, env.Seq, env.Msg, s)
	}
}

// ingestBookData applies one snapshot/delta with the seq-gap rule: any break in the per-sid chain
// invalidates EVERY book on the sid (the counter is shared, so we can't know which market's update
// vanished), adopts the observed seq, and requests a get_snapshot re-baseline. A snapshot always
// re-validates its own market; deltas apply only to books that are currently provable.
func (c *Client) ingestBookData(typ string, sid, seq int64, raw json.RawMessage, s *wsSession) {
	b := &c.books
	b.mu.Lock()
	if s != b.sess || b.sid == 0 || sid != b.sid {
		b.mu.Unlock()
		return
	}
	priorSeq := b.lastSeq
	gap := priorSeq > 0 && seq != priorSeq+1
	if seq > 0 {
		b.lastPriorSeq = priorSeq
		b.lastSeq = seq
	}
	if gap {
		b.gaps++
		b.lastGapPrior, b.lastGapSeq = priorSeq, seq
		b.resyncNeeded = true
		for _, bk := range b.books {
			bk.valid = false
		}
	}
	fireTicker, fireMid := "", 0.0
	replayTicker := ""
	initialDone, completed := false, ""
	var replaySourceAt time.Time
	acceptedFrame := false
	now := time.Now().UTC()
	if typ == "orderbook_snapshot" {
		var m struct {
			MarketTicker string     `json:"market_ticker"`
			Yes          [][]string `json:"yes_dollars_fp"`
			No           [][]string `json:"no_dollars_fp"`
			TS           int64      `json:"ts"`
			TSMS         int64      `json:"ts_ms"`
		}
		if json.Unmarshal(raw, &m) == nil && m.MarketTicker != "" {
			acceptedFrame = true
			replayTicker = m.MarketTicker
			if m.TSMS > 0 {
				replaySourceAt = time.UnixMilli(m.TSMS).UTC()
			} else if m.TS > 0 {
				replaySourceAt = time.Unix(m.TS, 0).UTC()
			}
			bk := &wsBook{yes: parseBookLadder(m.Yes), no: parseBookLadder(m.No), valid: true, at: now,
				sourceAt: replaySourceAt, sequence: seq}
			bk.bestYes, bk.bestNo = bookMaxKey(bk.yes), bookMaxKey(bk.no)
			prev := b.books[m.MarketTicker]
			b.books[m.MarketTicker] = bk
			if b.initialLeft[m.MarketTicker] {
				delete(b.initialLeft, m.MarketTicker)
				if len(b.initialLeft) == 0 {
					b.initialLeft = nil
					initialDone = true
				}
			}
			for id, upd := range b.pendingUpd { // invariant: at most one command is in flight
				if !upd.remaining[m.MarketTicker] {
					continue
				}
				delete(upd.remaining, m.MarketTicker)
				if upd.acked && len(upd.remaining) == 0 {
					delete(b.pendingUpd, id)
					completed = upd.action
				} else {
					b.pendingUpd[id] = upd
				}
				break
			}
			// A snapshot landing on our sid is end-to-end proof the subscription serves us (snapshots
			// only flow from a command the venue accepted) — unlatch the health error here too, so a
			// feed that healed without a visible ok frame doesn't stay red forever.
			b.lastErr = ""
			if prev == nil || !prev.valid || prev.bestYes != bk.bestYes || prev.bestNo != bk.bestNo {
				fireTicker, fireMid = bookTopMid(m.MarketTicker, bk)
			}
		}
	} else { // orderbook_delta
		var m struct {
			MarketTicker string `json:"market_ticker"`
			Price        string `json:"price_dollars"`
			Delta        string `json:"delta_fp"`
			Side         string `json:"side"`
			TS           int64  `json:"ts"`
			TSMS         int64  `json:"ts_ms"`
		}
		if json.Unmarshal(raw, &m) == nil && m.MarketTicker != "" {
			acceptedFrame = true
			replayTicker = m.MarketTicker
			if m.TSMS > 0 {
				replaySourceAt = time.UnixMilli(m.TSMS).UTC()
			} else if m.TS > 0 {
				replaySourceAt = time.Unix(m.TS, 0).UTC()
			}
			if bk := b.books[m.MarketTicker]; bk != nil && bk.valid {
				k, okK := bookPriceKey(m.Price)
				d, errD := strconv.ParseFloat(m.Delta, 64)
				lad := bk.yes
				if m.Side == "no" {
					lad = bk.no
				} else if m.Side != "yes" {
					okK = false // unknown side value — never guess a ladder
				}
				if okK && errD == nil {
					cur, exists := lad[k]
					next := cur + d
					switch {
					case next <= 1e-9:
						delete(lad, k)
					case exists || len(lad) < bookLevelCap:
						lad[k] = next
					}
					prevYes, prevNo := bk.bestYes, bk.bestNo
					if m.Side == "no" {
						if _, still := lad[k]; k > bk.bestNo && still {
							bk.bestNo = k
						} else if k == bk.bestNo && !still {
							bk.bestNo = bookMaxKey(lad)
						}
					} else {
						if _, still := lad[k]; k > bk.bestYes && still {
							bk.bestYes = k
						} else if k == bk.bestYes && !still {
							bk.bestYes = bookMaxKey(lad)
						}
					}
					bk.at = now
					bk.sourceAt, bk.sequence = replaySourceAt, seq
					if bk.bestYes != prevYes || bk.bestNo != prevNo {
						fireTicker, fireMid = bookTopMid(m.MarketTicker, bk)
					}
				}
			}
		}
	}
	if acceptedFrame {
		b.frameAt.Store(now.UnixNano())
	}
	var replayEvent *ResearchReplayEvent
	// Every already-subscribed book delta reaches the memory-only research observer. Ordinary
	// deltas carry only the executable touch (constant work); snapshots, sequence faults and
	// top-price changes retain the ten-level copy. The downstream durable replay selector still
	// coalesces at 500ms, so this does not become a raw-tick archive or add subscriptions.
	if acceptedFrame && replayTicker != "" {
		current, prior, sequenceGap := researchSequenceTruth(priorSeq, seq)
		replayBook := researchReplayTopBookLocked(b.books[replayTicker])
		if typ == "orderbook_snapshot" || gap || fireTicker != "" {
			replayBook = researchReplayBookLocked(b.books[replayTicker])
		}
		replayEvent = &ResearchReplayEvent{Kind: "book", EntityID: replayTicker, FrameType: typ,
			ObservedAt: now, SourceAt: replaySourceAt, SourceSequence: current,
			PriorSourceSequence: prior, SequenceGap: sequenceGap,
			Channel: "orderbook_delta", SubscriptionID: sid, SourceGeneration: b.generation,
			Book: replayBook}
	}
	b.mu.Unlock()
	if initialDone {
		go c.syncBooks()
	}
	if completed != "" {
		go c.bookAdvance(completed)
	}
	if replayEvent != nil {
		c.emitResearchReplay(*replayEvent)
	}
	if gap {
		go c.bookResync()
	}
	// R74 §3 tick-cancel: a top-of-book PRICE change feeds the SAME event-driven handler the ticker
	// WS drives (R40 1¢ cancels). Semantics verified identical to the ticker path's two-sided case:
	// the ticker channel's yes_bid/yes_ask ARE the venue top-of-book and onKalshiTick uses their mid
	// (bid+ask)/2 — this computes the same mid from the same levels. It is ADDITIVE only (fires
	// when the mid is two-sided and the book provable): the ticker path keeps full-universe coverage
	// and its last-price fallback, so cancel latency can only improve, never regress.
	if fireTicker != "" && fireMid > 0 && fireMid < 1 {
		c.txMu.Lock()
		tf := c.tickFn
		c.txMu.Unlock()
		if tf != nil {
			tf(fireTicker, fireMid)
		}
	}
}

// bookTopMid returns (ticker, mid) when the book is two-sided, else ("",0). Caller holds books.mu.
func bookTopMid(ticker string, bk *wsBook) (string, float64) {
	if bk.bestYes <= 0 || bk.bestNo <= 0 {
		return "", 0
	}
	bid := float64(bk.bestYes) / 10000
	ask := 1 - float64(bk.bestNo)/10000
	if ask <= 0 || ask >= 1 || bid <= 0 {
		return "", 0
	}
	return ticker, (bid + ask) / 2
}

// LiveBook returns the WS-maintained book as the same YES-centric view GetOrderbook serves (YesBids
// best-first; YesAsks derived from NO bids, best-first), capped at bookDepthCap (10) levels/side,
// plus the time since the book last CHANGED (display). ok=false when the ticker isn't subscribed,
// the seq chain is broken (awaiting resnapshot), or the acknowledged owning socket has not delivered
// any application frame within maxAge. A quiet unchanged book remains valid while the multiplexed
// transport is live; callers still fall back to REST when transport proof expires.
func (c *Client) LiveBook(ticker string, maxAge time.Duration) (*Orderbook, time.Duration, bool) {
	b := &c.books
	b.mu.Lock()
	defer b.mu.Unlock()
	bk := b.books[ticker]
	if bk == nil || !bk.valid || !b.transportFreshLocked(maxAge) {
		return nil, 0, false
	}
	yesKeys := make([]int, 0, len(bk.yes))
	for k := range bk.yes {
		yesKeys = append(yesKeys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(yesKeys))) // best (highest) YES bid first
	noKeys := make([]int, 0, len(bk.no))
	for k := range bk.no {
		noKeys = append(noKeys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(noKeys))) // highest NO bid = lowest YES ask first
	ob := &Orderbook{Ticker: ticker}
	for i, k := range yesKeys {
		if i >= bookDepthCap {
			break
		}
		ob.YesBids = append(ob.YesBids, OrderbookLevel{Price: float64(k) / 10000, Size: bk.yes[k]})
	}
	for i, k := range noKeys {
		if i >= bookDepthCap {
			break
		}
		ob.YesAsks = append(ob.YesAsks, OrderbookLevel{Price: 1 - float64(k)/10000, Size: bk.no[k]})
	}
	return ob, time.Since(bk.at), true
}

// BookProvenance is the exact public-source envelope for one currently valid WS book. SourceAt is
// zero when the venue omitted ts_ms/ts; callers must not replace it with ReceivedAt. Generation and
// SubscriptionID distinguish reconnect/re-subscribe sequence domains.
type BookProvenance struct {
	Channel        string    `json:"channel"`
	Generation     uint64    `json:"generation"`
	SubscriptionID int64     `json:"subscription_id"`
	Sequence       int64     `json:"sequence"`
	SourceAt       time.Time `json:"source_at,omitempty"`
	ReceivedAt     time.Time `json:"received_at"`
}

func (c *Client) LiveBookProvenance(ticker string, maxAge time.Duration) (BookProvenance, bool) {
	b := &c.books
	b.mu.Lock()
	defer b.mu.Unlock()
	bk := b.books[ticker]
	if bk == nil || !bk.valid || !b.transportFreshLocked(maxAge) || bk.sequence <= 0 {
		return BookProvenance{}, false
	}
	return BookProvenance{Channel: "orderbook_delta", Generation: b.generation,
		SubscriptionID: b.sid, Sequence: bk.sequence, SourceAt: bk.sourceAt.UTC(),
		ReceivedAt: bk.at.UTC()}, true
}

// LiveBookWithProvenance returns price/depth and its exact source receipt under one lock. Calling
// LiveBook and LiveBookProvenance separately can pair a pre-delta ladder with a post-delta sequence;
// research observations that claim a sequence-stamped executable frame must use this accessor.
func (c *Client) LiveBookWithProvenance(ticker string, maxAge time.Duration) (*Orderbook, time.Duration, BookProvenance, bool) {
	b := &c.books
	b.mu.Lock()
	defer b.mu.Unlock()
	bk := b.books[ticker]
	if bk == nil || !bk.valid || !b.transportFreshLocked(maxAge) || bk.sequence <= 0 {
		return nil, 0, BookProvenance{}, false
	}
	yesKeys := make([]int, 0, len(bk.yes))
	for k := range bk.yes {
		yesKeys = append(yesKeys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(yesKeys)))
	noKeys := make([]int, 0, len(bk.no))
	for k := range bk.no {
		noKeys = append(noKeys, k)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(noKeys)))
	ob := &Orderbook{Ticker: ticker}
	for i, k := range yesKeys {
		if i >= bookDepthCap {
			break
		}
		ob.YesBids = append(ob.YesBids, OrderbookLevel{Price: float64(k) / 10000, Size: bk.yes[k]})
	}
	for i, k := range noKeys {
		if i >= bookDepthCap {
			break
		}
		ob.YesAsks = append(ob.YesAsks, OrderbookLevel{Price: 1 - float64(k)/10000, Size: bk.no[k]})
	}
	provenance := BookProvenance{Channel: "orderbook_delta", Generation: b.generation,
		SubscriptionID: b.sid, Sequence: bk.sequence, SourceAt: bk.sourceAt.UTC(), ReceivedAt: bk.at.UTC()}
	return ob, time.Since(bk.at), provenance, true
}

// BookDepthTotals returns FULL-ladder depth sums plus top-3 sums per side from the WS book — the
// exact inputs kalImbalance/kalBook3 compute from a REST fetch (full-depth parity keeps the
// book_depth/book_imb3/book_depth3 ML features on the same scale regardless of source).
func (c *Client) BookDepthTotals(ticker string, maxAge time.Duration) (yesDepth, noDepth, yes3, no3 float64, ok bool) {
	b := &c.books
	b.mu.Lock()
	defer b.mu.Unlock()
	bk := b.books[ticker]
	if bk == nil || !bk.valid || !b.transportFreshLocked(maxAge) {
		return 0, 0, 0, 0, false
	}
	top3 := func(m map[int]float64) (total, t3 float64) {
		keys := make([]int, 0, len(m))
		for k := range m {
			total += m[k]
			keys = append(keys, k)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(keys))) // best bid first on either ladder
		for i, k := range keys {
			if i >= 3 {
				break
			}
			t3 += m[k]
		}
		return total, t3
	}
	yesDepth, yes3 = top3(bk.yes)
	noDepth, no3 = top3(bk.no)
	return yesDepth, noDepth, yes3, no3, true
}

// BookLevelSize returns the resting contracts at ONE exact price level (noSide=false → the YES-bid
// ladder, true → the NO-bid ladder). This is the maker queue-position input: the size at OUR resting
// order's level minus our own remainder approximates the money ahead of us (price-time priority
// means some of it may actually be behind us — treat it as an upper bound).
func (c *Client) BookLevelSize(ticker string, noSide bool, price float64, maxAge time.Duration) (float64, bool) {
	k := int(math.Round(price * 10000))
	if k <= 0 || k >= 10000 {
		return 0, false
	}
	b := &c.books
	b.mu.Lock()
	defer b.mu.Unlock()
	bk := b.books[ticker]
	if bk == nil || !bk.valid || !b.transportFreshLocked(maxAge) {
		return 0, false
	}
	if noSide {
		return bk.no[k], true
	}
	return bk.yes[k], true
}

// BookStats reports book-WS health: subscribed tickers, how many books are currently provable on an
// acknowledged live transport, seq gaps seen, and the last command error ("" when clean). The
// 60-second window matches the socket read deadline and does not mistake quiet books for dead ones.
func (c *Client) BookStats() (subscribed, fresh int, gaps int64, lastErr string) {
	b := &c.books
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.transportFreshLocked(bookDarkWait) {
		for _, bk := range b.books {
			if bk.valid {
				fresh++
			}
		}
	}
	return len(b.subbed), fresh, b.gaps, b.lastErr
}

// BookSourceClock exposes the current subscription sequence without pretending local arrival time
// is a venue clock. The caller appends a receipt only when this sequence advances.
func (c *Client) BookSourceClock() (sequence int64, rows int, received time.Time, ok bool) {
	truth := c.BookSourceClockTruth()
	return truth.Sequence, truth.Rows, truth.Received, truth.OK
}

// BookSourceTruth exposes actual channel/session and immediate-frame sequence truth. Callers must
// not synthesize a contiguous range between periodic samples: the immediate prior sequence and
// exact latest break endpoints are retained by the ingest path itself.
type BookSourceTruth struct {
	Channel                       string
	Generation                    uint64
	SubscriptionID                int64
	Sequence, PriorSequence       int64
	Rows                          int
	Received                      time.Time
	Gaps                          int64
	LastGapPrior, LastGapSequence int64
	OK                            bool
}

func (c *Client) BookSourceClockTruth() BookSourceTruth {
	b := &c.books
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sid <= 0 || b.lastSeq <= 0 {
		return BookSourceTruth{Channel: "orderbook_delta", Generation: b.generation,
			SubscriptionID: b.sid, Rows: len(b.books)}
	}
	var received time.Time
	ns := b.frameAt.Load()
	if ns > 0 {
		received = time.Unix(0, ns).UTC()
	}
	return BookSourceTruth{Channel: "orderbook_delta", Generation: b.generation,
		SubscriptionID: b.sid, Sequence: b.lastSeq, PriorSequence: b.lastPriorSeq,
		Rows: len(b.books), Received: received, Gaps: b.gaps,
		LastGapPrior: b.lastGapPrior, LastGapSequence: b.lastGapSeq, OK: true}
}
