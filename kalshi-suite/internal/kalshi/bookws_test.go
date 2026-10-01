package kalshi

// R74 decoder tests — message shapes copied from the venue AsyncAPI examples
// (docs.kalshi.com/websockets/orderbook-updates.md, fetched 2026-07-04): orderbook_snapshot,
// orderbook_delta (signed delta_fp, side yes|no, level ≤0 vanishes), the per-sid seq chain with
// gap ⇒ invalidate-all + resnapshot semantics, and the subscribed/ok bookkeeping frames.

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// newBookTestClient wires a Client with a book-owning primary session (no real socket: command
// writes fail harmlessly on the nil conn — exactly the "send failed, supervisor will redial" path).
func newBookTestClient(t *testing.T) (*Client, *wsSession) {
	t.Helper()
	c := &Client{}
	s := &wsSession{}
	s.primary.Store(true)
	s.frameAt.Store(time.Now().UnixNano())
	c.bookPrimaryChanged(s)
	// The documented ack for {"cmd":"subscribe","params":{"channels":["orderbook_delta"],…}}.
	c.ingestBook([]byte(`{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}`), s)
	if got := c.books.sid; got != 7 {
		t.Fatalf("subscribed ack not adopted: sid=%d want 7", got)
	}
	return c, s
}

// docSnapshot is the AsyncAPI example payload (sid rebased onto the test subscription).
const docSnapshot = `{"type":"orderbook_snapshot","sid":7,"seq":2,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1","yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],"no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}`

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func newBookCommandWire(t *testing.T) (*websocket.Conn, chan map[string]any, chan error) {
	t.Helper()
	commands := make(chan map[string]any, 32)
	errs := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			errs <- err
			return
		}
		defer conn.Close()
		for {
			var cmd map[string]any
			if err := conn.ReadJSON(&cmd); err != nil {
				select {
				case errs <- err:
				default:
				}
				return
			}
			commands <- cmd
		}
	}))
	t.Cleanup(server.Close)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, commands, errs
}

func bookTestCommandParts(t *testing.T, cmd map[string]any) (int64, string, []string) {
	t.Helper()
	rawID, ok := cmd["id"].(float64)
	if !ok {
		t.Fatalf("book command id missing: %v", cmd)
	}
	params, ok := cmd["params"].(map[string]any)
	if !ok {
		t.Fatalf("book command params missing: %v", cmd)
	}
	action, _ := params["action"].(string)
	rawTickers, ok := params["market_tickers"].([]any)
	if !ok {
		t.Fatalf("book command tickers missing: %v", cmd)
	}
	tickers := make([]string, 0, len(rawTickers))
	for _, raw := range rawTickers {
		ticker, ok := raw.(string)
		if !ok {
			t.Fatalf("non-string book ticker: %T %v", raw, raw)
		}
		tickers = append(tickers, ticker)
	}
	return int64(rawID), action, tickers
}

func TestBookWSSubscribePinsLegacyPriceConvention(t *testing.T) {
	// The decoder turns a NO bid at q into a YES ask at 1-q, so the outbound subscription must
	// explicitly request Kalshi's legacy/two-scale NO prices. Omission is unsafe because the venue
	// has announced that the default will flip to unified YES pricing.
	raw, err := json.Marshal(bookSubscribeCommand(42, []string{"KXTEST-1"}))
	if err != nil {
		t.Fatal(err)
	}
	var cmd struct {
		ID     int64  `json:"id"`
		Cmd    string `json:"cmd"`
		Params struct {
			Channels      []string `json:"channels"`
			Tickers       []string `json:"market_tickers"`
			SkipTickerAck *bool    `json:"skip_ticker_ack"`
			UseYesPrice   *bool    `json:"use_yes_price"`
		} `json:"params"`
	}
	if err := json.Unmarshal(raw, &cmd); err != nil {
		t.Fatal(err)
	}
	if cmd.ID != 42 || cmd.Cmd != "subscribe" || len(cmd.Params.Channels) != 1 || cmd.Params.Channels[0] != "orderbook_delta" ||
		len(cmd.Params.Tickers) != 1 || cmd.Params.Tickers[0] != "KXTEST-1" {
		t.Fatalf("wrong subscribe command: %s", raw)
	}
	if cmd.Params.UseYesPrice == nil || *cmd.Params.UseYesPrice {
		t.Fatalf("use_yes_price must be present and false: %s", raw)
	}
	if cmd.Params.SkipTickerAck == nil || !*cmd.Params.SkipTickerAck {
		t.Fatalf("skip_ticker_ack must be present and true: %s", raw)
	}
}

func TestBookWSSnapshotDecode(t *testing.T) {
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(docSnapshot), s)
	ob, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second)
	if !ok || ob == nil {
		t.Fatalf("LiveBook not ok after snapshot")
	}
	// YES bids best-first; YES asks derived from NO bids (ask = 1 − no_price), best-first.
	if len(ob.YesBids) != 2 || !near(ob.YesBids[0].Price, 0.22) || !near(ob.YesBids[0].Size, 333) ||
		!near(ob.YesBids[1].Price, 0.08) || !near(ob.YesBids[1].Size, 300) {
		t.Fatalf("bids wrong: %+v", ob.YesBids)
	}
	if len(ob.YesAsks) != 2 || !near(ob.YesAsks[0].Price, 0.44) || !near(ob.YesAsks[0].Size, 146) ||
		!near(ob.YesAsks[1].Price, 0.46) || !near(ob.YesAsks[1].Size, 20) {
		t.Fatalf("asks wrong: %+v", ob.YesAsks)
	}
	if yd, nd, y3, n3, ok := c.BookDepthTotals("FED-23DEC-T3.00", 5*time.Second); !ok ||
		!near(yd, 633) || !near(nd, 166) || !near(y3, 633) || !near(n3, 166) {
		t.Fatalf("depth totals wrong: %v %v %v %v ok=%v", yd, nd, y3, n3, ok)
	}
}

// R91 bug 137: venue error 25 (subscription message-buffer overflow) must condemn the socket —
// a PARTIAL venue-side drop is invisible to both existing heals (surviving markets keep the
// shared seq chain unbroken, and the dark clock needs ZERO provable books) — and the kill must
// throttle so an error storm can't become a redial storm. Nil test conn ⇒ the observable is the
// errRedialAt stamp; the rollback bookkeeping for id-carrying frames must still run first.
func TestBookWSErr25CondemnsSocketThrottled(t *testing.T) {
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(`{"type":"error","msg":{"code":6,"msg":"Already subscribed"}}`), s)
	if !c.books.errRedialAt.IsZero() {
		t.Fatalf("non-25 error must not arm the redial")
	}
	c.ingestBook([]byte(`{"type":"error","msg":{"code":25,"msg":"buffer overflow"}}`), s)
	first := c.books.errRedialAt
	if first.IsZero() {
		t.Fatalf("err-25 must stamp the redial throttle")
	}
	c.ingestBook([]byte(`{"type":"error","msg":{"code":25,"msg":"buffer overflow"}}`), s)
	if !c.books.errRedialAt.Equal(first) {
		t.Fatalf("second err-25 inside the window must throttle (one kill per minute)")
	}
	// id-carrying err-25 past the window: rolls back its command AND re-arms the kill.
	c.books.errRedialAt = time.Now().Add(-2 * time.Minute)
	c.books.pendingUpd = map[int64]bookUpdCmd{42: {action: "add_markets", tickers: []string{"T1"}}}
	c.books.subbed = map[string]bool{"T1": true}
	c.ingestBook([]byte(`{"id":42,"type":"error","msg":{"code":25,"msg":"buffer overflow"}}`), s)
	if c.books.subbed["T1"] {
		t.Fatalf("id-carrying err-25 must still roll back the optimistic add")
	}
	// (fresh-stamp check, not Equal(first) — two time.Now() calls in one fast test can land on
	// the same Windows clock tick and compare Equal)
	if time.Since(c.books.errRedialAt) > 30*time.Second {
		t.Fatalf("err-25 past the window must re-arm the kill (stamp stayed old: %v)", c.books.errRedialAt)
	}
}

func TestBookWSTerminalErrorsInvalidateAndRedial(t *testing.T) {
	for _, code := range []int{10, 17, 25} {
		t.Run(fmt.Sprintf("code-%d", code), func(t *testing.T) {
			c, s := newBookTestClient(t)
			c.ingestBook([]byte(docSnapshot), s)
			if _, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second); !ok {
				t.Fatal("fixture book never became valid")
			}
			c.books.errRedialAt = time.Time{}
			frame := fmt.Sprintf(`{"type":"error","msg":{"code":%d,"msg":"terminal"}}`, code)
			c.ingestBook([]byte(frame), s)
			if _, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second); ok {
				t.Fatalf("terminal error %d left an unprovable book readable", code)
			}
			if c.books.errRedialAt.IsZero() {
				t.Fatalf("terminal error %d did not request socket replacement", code)
			}
		})
	}
}

func TestBookWSDeltaTable(t *testing.T) {
	// Documented delta shape applied step-by-step on top of the snapshot; the seq chain advances 3,4,5,6.
	steps := []struct {
		frame     string
		wantYes22 float64 // size at yes 0.22 (-1 = level gone)
		wantNo54  float64 // size at no 0.54 (-1 = level gone)
	}{
		{`{"type":"orderbook_delta","sid":7,"seq":3,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"x","price_dollars":"0.2200","delta_fp":"-54.00","side":"yes","ts_ms":1669149841000}}`, 279, 20},
		{`{"type":"orderbook_delta","sid":7,"seq":4,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.5400","delta_fp":"-20.00","side":"no"}}`, 279, -1}, // level consumed to zero ⇒ gone
		{`{"type":"orderbook_delta","sid":7,"seq":5,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.1000","delta_fp":"25.00","side":"yes"}}`, 279, -1}, // fresh level inserted
		{`{"type":"orderbook_delta","sid":7,"seq":6,"msg":{"market_ticker":"OTHER-TICKER","price_dollars":"0.2200","delta_fp":"99.00","side":"yes"}}`, 279, -1},    // unknown market: seq consumed, book untouched
	}
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(docSnapshot), s)
	for i, st := range steps {
		c.ingestBook([]byte(st.frame), s)
		gotYes, okYes := c.BookLevelSize("FED-23DEC-T3.00", false, 0.22, 5*time.Second)
		gotNo, okNo := c.BookLevelSize("FED-23DEC-T3.00", true, 0.54, 5*time.Second)
		if !okYes || !okNo {
			t.Fatalf("step %d: level reads not ok", i)
		}
		wy, wn := st.wantYes22, st.wantNo54
		if wy < 0 {
			wy = 0 // absent level reads as 0 size
		}
		if wn < 0 {
			wn = 0
		}
		if !near(gotYes, wy) || !near(gotNo, wn) {
			t.Fatalf("step %d: yes22=%v no54=%v want %v %v", i, gotYes, gotNo, wy, wn)
		}
	}
	if _, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second); !ok {
		t.Fatalf("book should still be valid after in-order deltas")
	}
	if got := c.books.lastSeq; got != 6 {
		t.Fatalf("lastSeq=%d want 6", got)
	}
}

func TestBookWSSeqGapInvalidatesUntilResnapshot(t *testing.T) {
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(docSnapshot), s)
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":3,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.2200","delta_fp":"-54.00","side":"yes"}}`), s)
	// seq 4 lost in transit → the per-sid chain is broken: EVERY book on the sid is unprovable.
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":5,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.0800","delta_fp":"1.00","side":"yes"}}`), s)
	if _, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second); ok {
		t.Fatalf("book must be invalid after a seq gap")
	}
	if _, _, _, _, ok := c.BookDepthTotals("FED-23DEC-T3.00", 5*time.Second); ok {
		t.Fatalf("depth totals must refuse an invalid book")
	}
	if subs, fresh, gaps, _ := c.BookStats(); gaps != 1 || fresh != 0 {
		t.Fatalf("stats after gap: subs=%d fresh=%d gaps=%d", subs, fresh, gaps)
	}
	// Deltas KEEP consuming seq while invalid (chain re-anchored at the gap) but must not apply.
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":6,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.0800","delta_fp":"500.00","side":"yes"}}`), s)
	// The get_snapshot re-baseline lands in-stream and re-validates ONLY its market.
	c.ingestBook([]byte(`{"type":"orderbook_snapshot","sid":7,"seq":7,"msg":{"market_ticker":"FED-23DEC-T3.00","yes_dollars_fp":[["0.1500","10.00"]]}}`), s)
	ob, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second)
	if !ok || len(ob.YesBids) != 1 || !near(ob.YesBids[0].Price, 0.15) || !near(ob.YesBids[0].Size, 10) || len(ob.YesAsks) != 0 {
		t.Fatalf("resnapshot did not re-baseline: ok=%v ob=%+v", ok, ob)
	}
}

func TestBookWSIgnoresForeignSidAndStaleFeed(t *testing.T) {
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(docSnapshot), s)
	// A frame from some other subscription id must not touch the chain or the books.
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":9,"seq":99,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.2200","delta_fp":"-333.00","side":"yes"}}`), s)
	if got, _ := c.BookLevelSize("FED-23DEC-T3.00", false, 0.22, 5*time.Second); !near(got, 333) {
		t.Fatalf("foreign-sid delta applied: %v", got)
	}
	if c.books.lastSeq != 2 {
		t.Fatalf("foreign-sid seq adopted: %d", c.books.lastSeq)
	}
	// Transport liveness gate: a dead socket (no application frames) makes every read fall back,
	// while unrelated traffic on the same ordered socket restores authority without changing levels.
	s.frameAt.Store(time.Now().Add(-time.Minute).UnixNano())
	if _, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second); ok {
		t.Fatalf("stale transport must not authorize a cached book")
	}
	s.frameAt.Store(time.Now().UnixNano())
	if _, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second); !ok {
		t.Fatalf("live multiplexed transport must preserve a quiet valid snapshot")
	}
}

// Auditor r63 P2: orderbook_delta is change-driven. A valid snapshot can legitimately sit quiet
// while ticker/trade traffic on the same ordered socket proves transport liveness; that must not
// arm the dark-feed redial or make the snapshot unreadable. Conversely, a transport with no usable
// application frames still re-arms only once per full dark window (no reconnect storm).
func TestBookWSQuietSnapshotUsesTransportLiveness(t *testing.T) {
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(docSnapshot), s)

	c.books.mu.Lock()
	c.books.want = []string{"FED-23DEC-T3.00"}
	c.books.darkSince = time.Now().Add(-2 * bookDarkWait)
	c.books.mu.Unlock()
	// The last BOOK frame is old, but another channel on the same socket is current.
	c.books.frameAt.Store(time.Now().Add(-10 * time.Minute).UnixNano())
	s.frameAt.Store(time.Now().UnixNano())
	c.syncBooks()

	c.books.mu.Lock()
	darkSince := c.books.darkSince
	c.books.mu.Unlock()
	if !darkSince.IsZero() {
		t.Fatalf("quiet valid snapshot armed a redial: dark_since=%v", darkSince)
	}
	if _, _, ok := c.LiveBook("FED-23DEC-T3.00", 5*time.Second); !ok {
		t.Fatal("quiet valid snapshot became unreadable despite a live acknowledged transport")
	}
}

func TestBookWSDeadTransportRedialIsWindowThrottled(t *testing.T) {
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(docSnapshot), s)
	c.books.mu.Lock()
	c.books.want = []string{"FED-23DEC-T3.00"}
	c.books.darkSince = time.Now().Add(-2 * bookDarkWait)
	c.books.mu.Unlock()
	s.frameAt.Store(time.Now().Add(-2 * bookDarkWait).UnixNano())

	c.syncBooks()
	c.books.mu.Lock()
	first := c.books.darkSince
	c.books.mu.Unlock()
	if first.IsZero() || time.Since(first) > 5*time.Second {
		t.Fatalf("dead transport did not re-arm redial throttle: %v", first)
	}
	c.syncBooks()
	c.books.mu.Lock()
	second := c.books.darkSince
	c.books.mu.Unlock()
	if !second.Equal(first) {
		t.Fatalf("redial re-fired inside dark window: first=%v second=%v", first, second)
	}
}

func TestBookWSTopOfBookFiresTickHandler(t *testing.T) {
	c, s := newBookTestClient(t)
	var mu sync.Mutex
	var ticks []float64
	c.SetTickHandler(func(ticker string, yes float64) {
		mu.Lock()
		defer mu.Unlock()
		if ticker == "FED-23DEC-T3.00" {
			ticks = append(ticks, yes)
		}
	})
	c.ingestBook([]byte(docSnapshot), s)                                                                                                                                      // two-sided top: bid 0.22 / ask 0.44 → mid 0.33
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":3,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.3000","delta_fp":"10.00","side":"yes"}}`), s)  // best bid 0.22→0.30 → mid 0.37
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":4,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.0500","delta_fp":"10.00","side":"yes"}}`), s)  // deep level: top unchanged → NO tick
	c.ingestBook([]byte(`{"type":"orderbook_delta","sid":7,"seq":5,"msg":{"market_ticker":"FED-23DEC-T3.00","price_dollars":"0.5600","delta_fp":"-146.00","side":"no"}}`), s) // best no gone → ask 0.46 → mid 0.38
	mu.Lock()
	defer mu.Unlock()
	want := []float64{0.33, 0.37, 0.38}
	if len(ticks) != len(want) {
		t.Fatalf("ticks=%v want %v", ticks, want)
	}
	for i := range want {
		if !near(ticks[i], want[i]) {
			t.Fatalf("tick %d = %v want %v (all %v)", i, ticks[i], want[i], ticks)
		}
	}
}

func TestBookWSOkAdoptsAuthoritativeTickerList(t *testing.T) {
	c, s := newBookTestClient(t)
	c.ingestBook([]byte(docSnapshot), s)
	// update_subscription confirmation carries the FULL post-update list — adopt it and prune books.
	c.ingestBook([]byte(`{"id":4,"sid":7,"seq":3,"type":"ok","msg":{"market_tickers":["AAA-1","BBB-2"]}}`), s)
	c.books.mu.Lock()
	subbed := len(c.books.subbed)
	_, fedKept := c.books.books["FED-23DEC-T3.00"]
	lastSeq := c.books.lastSeq
	c.books.mu.Unlock()
	if subbed != 2 || fedKept {
		t.Fatalf("ok not adopted: subbed=%d fedKept=%v", subbed, fedKept)
	}
	if lastSeq != 3 { // ok.seq was the exact next link → adopted, no false gap on the next delta
		t.Fatalf("ok seq not chained: %d", lastSeq)
	}
	if _, _, gaps, _ := c.BookStats(); gaps != 0 {
		t.Fatalf("ok must not count as a gap: %d", gaps)
	}
}

func TestBookWSWorkingSetBookkeepingWithoutSocket(t *testing.T) {
	// No primary at all: the desired set is remembered; nothing panics, nothing subscribes.
	c := &Client{}
	c.SyncBookSubscriptions([]string{"A", "B", "B", ""})
	c.books.mu.Lock()
	want := append([]string(nil), c.books.want...)
	c.books.mu.Unlock()
	if len(want) != 2 || want[0] != "A" || want[1] != "B" {
		t.Fatalf("want set wrong: %v", want)
	}
	if !c.PrioritizeBookSubscription("B") {
		t.Fatal("existing non-front LIVE book was not promoted")
	}
	c.books.mu.Lock()
	want = append([]string(nil), c.books.want...)
	c.books.mu.Unlock()
	if len(want) != 2 || want[0] != "B" || want[1] != "A" {
		t.Fatalf("priority order wrong: %v", want)
	}
	if c.PrioritizeBookSubscription("B") {
		t.Fatal("already-front LIVE book reported a false subscription change")
	}
	// A primary with a dead socket: the subscribe attempt errors on write and stays retryable.
	s := &wsSession{}
	s.primary.Store(true)
	c.bookPrimaryChanged(s)
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.books.mu.Lock()
		errStr := c.books.lastErr
		c.books.mu.Unlock()
		if errStr != "" || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Hard cap: the client never asks the venue for more than bookWantCap tickers.
	big := make([]string, 0, bookWantCap+50)
	for i := 0; i < bookWantCap+50; i++ {
		big = append(big, fmt.Sprintf("T-%d", i))
	}
	c.SyncBookSubscriptions(big)
	c.books.mu.Lock()
	n := len(c.books.want)
	c.books.mu.Unlock()
	if n != bookWantCap {
		t.Fatalf("want cap: %d want %d", n, bookWantCap)
	}
	if !c.PrioritizeBookSubscription("LIVE-NOW") {
		t.Fatal("new LIVE book did not displace the bounded tail")
	}
	c.books.mu.Lock()
	first, n := c.books.want[0], len(c.books.want)
	c.books.mu.Unlock()
	if first != "LIVE-NOW" || n != bookWantCap {
		t.Fatalf("priority escaped cap: first=%q n=%d", first, n)
	}
}

func TestBookWSPriorityReplacementDeletesBeforeAdd(t *testing.T) {
	commands := make(chan map[string]any, 2)
	errs := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			errs <- err
			return
		}
		defer conn.Close()
		for i := 0; i < 2; i++ {
			var cmd map[string]any
			if err := conn.ReadJSON(&cmd); err != nil {
				errs <- err
				return
			}
			commands <- cmd
		}
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	c := &Client{}
	s := &wsSession{conn: conn}
	s.primary.Store(true)
	s.frameAt.Store(time.Now().UnixNano())
	c.books.mu.Lock()
	c.books.sess = s
	c.books.sid = 7
	c.books.want = []string{"LIVE-NOW", "KEEP"}
	c.books.subbed = map[string]bool{"KEEP": true, "TAIL": true}
	c.books.books = map[string]*wsBook{"KEEP": {valid: true}, "TAIL": {valid: true}}
	c.books.mu.Unlock()
	c.syncBooks()

	next := func() map[string]any {
		t.Helper()
		select {
		case err := <-errs:
			t.Fatal(err)
		case cmd := <-commands:
			return cmd
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for book reconciliation command")
		}
		return nil
	}
	action := func(cmd map[string]any) string {
		t.Helper()
		params, _ := cmd["params"].(map[string]any)
		value, _ := params["action"].(string)
		return value
	}
	assertSID := func(cmd map[string]any) {
		t.Helper()
		params, _ := cmd["params"].(map[string]any)
		if _, legacy := params["sid"]; legacy {
			t.Fatalf("book update used legacy single sid: %v", cmd)
		}
		sids, ok := params["sids"].([]any)
		if !ok || len(sids) != 1 || sids[0] != float64(7) {
			t.Fatalf("book update did not use canonical one-element sids: %v", cmd)
		}
	}
	first := next()
	assertSID(first)
	if got := action(first); got != "delete_markets" {
		t.Fatalf("bounded replacement must free a seat first: first action=%q command=%v", got, first)
	}
	select {
	case second := <-commands:
		t.Fatalf("R156 sent a second update before the delete ACK: %v", second)
	case <-time.After(75 * time.Millisecond):
	}
	id, ok := first["id"].(float64)
	if !ok {
		t.Fatalf("delete command id missing: %v", first)
	}
	c.ingestBook([]byte(fmt.Sprintf(`{"id":%d,"sid":7,"type":"ok","msg":{}}`, int64(id))), s)
	c.books.mu.Lock()
	_, tailRetained := c.books.books["TAIL"]
	c.books.mu.Unlock()
	if tailRetained {
		t.Fatal("ACKed delete retained the evicted local book")
	}
	second := next()
	assertSID(second)
	if got := action(second); got != "add_markets" {
		t.Fatalf("LIVE priority add did not follow the eviction: second action=%q command=%v", got, second)
	}
}

func TestR156BookWSInitialSnapshotsDrainBeforeFirstAdd(t *testing.T) {
	conn, commands, errs := newBookCommandWire(t)
	c := &Client{}
	s := &wsSession{conn: conn}
	s.primary.Store(true)
	s.frameAt.Store(time.Now().UnixNano())
	want := make([]string, 205)
	for i := range want {
		want[i] = fmt.Sprintf("BOOT-%03d", i)
	}
	c.books.mu.Lock()
	c.books.sess = s
	c.books.want = append([]string(nil), want...)
	c.books.books = map[string]*wsBook{}
	c.books.mu.Unlock()
	c.syncBooks()

	next := func() map[string]any {
		t.Helper()
		select {
		case err := <-errs:
			t.Fatal(err)
		case cmd := <-commands:
			return cmd
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for R156 bootstrap command")
		}
		return nil
	}
	none := func(stage string) {
		t.Helper()
		select {
		case err := <-errs:
			t.Fatal(err)
		case cmd := <-commands:
			t.Fatalf("%s emitted add before initial snapshots drained: %v", stage, cmd)
		case <-time.After(40 * time.Millisecond):
		}
	}

	subscribe := next()
	id, action, tickers := bookTestCommandParts(t, subscribe)
	if action != "" || len(tickers) != bookSubChunk {
		t.Fatalf("bootstrap command = action %q n=%d: %v", action, len(tickers), subscribe)
	}
	c.ingestBook([]byte(fmt.Sprintf(`{"id":%d,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}`, id)), s)
	none("subscribe ACK")
	for i, ticker := range tickers {
		if i == len(tickers)-1 {
			none("all but final initial snapshot")
		}
		frame := fmt.Sprintf(`{"type":"orderbook_snapshot","sid":7,"seq":%d,"msg":{"market_ticker":%q}}`, i+1, ticker)
		c.ingestBook([]byte(frame), s)
	}
	_, action, tickers = bookTestCommandParts(t, next())
	if action != "add_markets" || len(tickers) != bookAddChunk {
		t.Fatalf("first post-bootstrap command = action %q n=%d, want add_markets/%d", action, len(tickers), bookAddChunk)
	}
}

func TestR156BookWSAddChunksWaitForAckAndSnapshotDrain(t *testing.T) {
	conn, commands, errs := newBookCommandWire(t)
	c := &Client{}
	s := &wsSession{conn: conn}
	s.primary.Store(true)
	s.frameAt.Store(time.Now().UnixNano())
	want := make([]string, 205)
	for i := range want {
		want[i] = fmt.Sprintf("ADD-%03d", i)
	}
	c.books.mu.Lock()
	c.books.sess = s
	c.books.sid = 7
	c.books.want = append([]string(nil), want...)
	c.books.subbed = map[string]bool{}
	c.books.books = map[string]*wsBook{}
	c.books.mu.Unlock()

	next := func() map[string]any {
		t.Helper()
		select {
		case err := <-errs:
			t.Fatal(err)
		case cmd := <-commands:
			return cmd
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for R156 book command")
		}
		return nil
	}
	none := func(stage string) {
		t.Helper()
		select {
		case err := <-errs:
			t.Fatal(err)
		case cmd := <-commands:
			t.Fatalf("%s emitted another command before the prior chunk drained: %v", stage, cmd)
		case <-time.After(40 * time.Millisecond):
		}
	}

	c.syncBooks()
	seq := int64(0)
	remaining := len(want)
	for chunkN := 0; remaining > 0; chunkN++ {
		wantN := bookAddChunk
		if remaining < wantN {
			wantN = remaining
		}
		cmd := next()
		id, action, tickers := bookTestCommandParts(t, cmd)
		if action != "add_markets" || len(tickers) != wantN {
			t.Fatalf("chunk %d = action %q n=%d, want add_markets/%d: %v", chunkN, action, len(tickers), wantN, cmd)
		}
		c.syncBooks()
		none("repeated sync")
		ack := func() {
			c.ingestBook([]byte(fmt.Sprintf(`{"id":%d,"sid":7,"type":"ok","msg":{}}`, id)), s)
		}
		if chunkN%2 == 0 {
			ack()
			none("ACK without snapshots")
		}
		for i, ticker := range tickers {
			if i == len(tickers)-1 {
				none("all but final snapshot")
			}
			seq++
			frame := fmt.Sprintf(`{"type":"orderbook_snapshot","sid":7,"seq":%d,"msg":{"market_ticker":%q}}`, seq, ticker)
			c.ingestBook([]byte(frame), s)
		}
		if chunkN%2 != 0 {
			none("snapshots without ACK")
			ack()
		}
		remaining -= wantN
	}
	none("completed working set")
	c.books.mu.Lock()
	pending, subbed := len(c.books.pendingUpd), len(c.books.subbed)
	c.books.mu.Unlock()
	if pending != 0 || subbed != len(want) {
		t.Fatalf("R156 add state did not converge: pending=%d subbed=%d want=%d", pending, subbed, len(want))
	}
}

func TestR166BookWSAddPressureBudget(t *testing.T) {
	if bookAddChunk != 10 {
		t.Fatalf("add_markets burst = %d, want 10 to stay below the observed venue buffer limit", bookAddChunk)
	}
	if bookUpdateWait != 30*time.Second {
		t.Fatalf("update receipt window = %s, want 30s for ACK plus requested snapshots", bookUpdateWait)
	}
}

func TestR156BookWSResyncChunksWaitForAckAndSnapshotDrain(t *testing.T) {
	conn, commands, errs := newBookCommandWire(t)
	c := &Client{}
	s := &wsSession{conn: conn}
	s.primary.Store(true)
	s.frameAt.Store(time.Now().UnixNano())
	want := make([]string, 205)
	subbed := make(map[string]bool, len(want))
	books := make(map[string]*wsBook, len(want))
	for i := range want {
		want[i] = fmt.Sprintf("RESYNC-%03d", i)
		subbed[want[i]] = true
		books[want[i]] = &wsBook{valid: false}
	}
	c.books.mu.Lock()
	c.books.sess = s
	c.books.sid = 7
	c.books.want = append([]string(nil), want...)
	c.books.subbed = subbed
	c.books.books = books
	c.books.resyncNeeded = true
	c.books.mu.Unlock()

	next := func() map[string]any {
		t.Helper()
		select {
		case err := <-errs:
			t.Fatal(err)
		case cmd := <-commands:
			return cmd
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for R156 resync command")
		}
		return nil
	}
	none := func(stage string) {
		t.Helper()
		select {
		case err := <-errs:
			t.Fatal(err)
		case cmd := <-commands:
			t.Fatalf("%s emitted another resync before the prior chunk drained: %v", stage, cmd)
		case <-time.After(40 * time.Millisecond):
		}
	}

	c.bookResync()
	seq := int64(0)
	for chunkN, wantN := range []int{100, 100, 5} {
		cmd := next()
		id, action, tickers := bookTestCommandParts(t, cmd)
		if action != "get_snapshot" || len(tickers) != wantN {
			t.Fatalf("resync chunk %d = action %q n=%d, want get_snapshot/%d: %v", chunkN, action, len(tickers), wantN, cmd)
		}
		c.bookResyncContinue()
		none("repeated resync")
		ack := func() {
			c.ingestBook([]byte(fmt.Sprintf(`{"id":%d,"sid":7,"type":"ok","msg":{}}`, id)), s)
		}
		if chunkN%2 == 0 {
			ack()
			none("resync ACK without snapshots")
		}
		for i, ticker := range tickers {
			if i == len(tickers)-1 {
				none("all but final resync snapshot")
			}
			seq++
			frame := fmt.Sprintf(`{"type":"orderbook_snapshot","sid":7,"seq":%d,"msg":{"market_ticker":%q}}`, seq, ticker)
			c.ingestBook([]byte(frame), s)
		}
		if chunkN%2 != 0 {
			none("resync snapshots without ACK")
			ack()
		}
	}
	none("completed resync")
	c.books.mu.Lock()
	pending, needed := len(c.books.pendingUpd), c.books.resyncNeeded
	valid := 0
	for _, bk := range c.books.books {
		if bk.valid {
			valid++
		}
	}
	c.books.mu.Unlock()
	if pending != 0 || needed || valid != len(want) {
		t.Fatalf("R156 resync did not converge: pending=%d needed=%v valid=%d/%d", pending, needed, valid, len(want))
	}
}

func TestR157BookWSDeleteTimeoutPreservesProvenBooks(t *testing.T) {
	conn, _, _ := newBookCommandWire(t)
	c := &Client{}
	s := &wsSession{conn: conn}
	s.primary.Store(true)
	s.frameAt.Store(time.Now().UnixNano())
	c.books.mu.Lock()
	c.books.sess = s
	c.books.sid = 7
	c.books.want = []string{"KEEP"}
	c.books.subbed = map[string]bool{"KEEP": true} // DROP was removed optimistically
	c.books.books = map[string]*wsBook{
		"KEEP": {valid: true},
		"DROP": {valid: true}, // retained until the venue confirms deletion
	}
	c.books.pendingUpd = map[int64]bookUpdCmd{
		42: {action: "delete_markets", tickers: []string{"DROP"}},
	}
	c.books.mu.Unlock()

	c.bookUpdateTimedOut(s, 42)
	c.books.mu.Lock()
	keepValid := c.books.books["KEEP"].valid
	dropValid := c.books.books["DROP"].valid
	dropSubbed := c.books.subbed["DROP"]
	lastErr := c.books.lastErr
	_, pending := c.books.pendingUpd[42]
	c.books.mu.Unlock()
	if !keepValid || !dropValid || !dropSubbed || pending || lastErr != "" {
		t.Fatalf("delete timeout damaged proven books: keep=%v drop=%v subbed=%v pending=%v err=%q",
			keepValid, dropValid, dropSubbed, pending, lastErr)
	}
	if err := s.writeJSON(map[string]any{"id": 99}); err != nil {
		t.Fatalf("delete timeout closed a healthy transport: %v", err)
	}
}

func TestR157BookWSAddAndResyncTimeoutsStillFailClosed(t *testing.T) {
	for _, action := range []string{"add_markets", "get_snapshot"} {
		t.Run(action, func(t *testing.T) {
			conn, _, _ := newBookCommandWire(t)
			c := &Client{}
			s := &wsSession{conn: conn}
			s.primary.Store(true)
			s.frameAt.Store(time.Now().UnixNano())
			c.books.mu.Lock()
			c.books.sess = s
			c.books.sid = 7
			c.books.subbed = map[string]bool{"TIMEOUT-1": true}
			c.books.books = map[string]*wsBook{"TIMEOUT-1": {valid: true}}
			c.books.pendingUpd = map[int64]bookUpdCmd{
				42: {action: action, tickers: []string{"TIMEOUT-2"}, remaining: map[string]bool{"TIMEOUT-2": true}},
			}
			c.books.mu.Unlock()

			c.bookUpdateTimedOut(s, 42)
			c.books.mu.Lock()
			valid := c.books.books["TIMEOUT-1"].valid
			lastErr := c.books.lastErr
			_, pending := c.books.pendingUpd[42]
			c.books.mu.Unlock()
			if valid || !pending || !strings.Contains(lastErr, "ws update timeout: "+action) {
				t.Fatalf("%s timeout was not fail-closed: valid=%v pending=%v err=%q",
					action, valid, pending, lastErr)
			}
			if err := s.writeJSON(map[string]any{"id": 99}); err == nil {
				t.Fatalf("%s timeout did not close the condemned websocket", action)
			}
		})
	}
}
