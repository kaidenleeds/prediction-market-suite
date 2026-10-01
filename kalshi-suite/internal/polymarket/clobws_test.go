package polymarket

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestR132CLOBBookAndOutcomeSpecificBBO(t *testing.T) {
	c := NewClient(time.Second)
	const condition = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	c.registerMarketTokens(Market{ConditionID: condition, TokensRaw: `["yes-token","no-token"]`})

	// Official book shape uses string prices/sizes; levels are deliberately unsorted and include
	// duplicate best levels so the cache must compute and aggregate rather than trust array order.
	c.ingestCLOBMarket([]byte(`[
		{"event_type":"book","market":"`+condition+`","asset_id":"yes-token",
		 "bids":[{"price":"0.48","size":"30"},{"price":"0.50","size":"15"},{"price":"0.50","size":"5"}],
		 "asks":[{"price":"0.54","size":"10"},{"price":"0.52","size":"25"}]},
		{"event_type":"book","market":"`+condition+`","asset_id":"no-token",
		 "bids":[{"price":"0.39","size":"12"}],"asks":[{"price":"0.42","size":"18"}]}
	]`), nil)

	bid, ask, bd, ad, _, ok := c.CLOBOutcomeBBO(condition, 0)
	if !ok || bid != 0.50 || ask != 0.52 || bd != 20 || ad != 25 {
		t.Fatalf("YES BBO = %.3f/%.3f depth %.1f/%.1f ok=%v", bid, ask, bd, ad, ok)
	}
	bid, ask, bd, ad, _, ok = c.CLOBOutcomeBBO(condition, 1)
	if !ok || bid != 0.39 || ask != 0.42 || bd != 12 || ad != 18 {
		t.Fatalf("NO must use its own token book, got %.3f/%.3f depth %.1f/%.1f ok=%v", bid, ask, bd, ad, ok)
	}
	if px, live := c.LivePrice(condition); !live || px != 0.51 {
		t.Fatalf("LivePrice must prefer CLOB midpoint 0.51, got %.4f live=%v", px, live)
	}
	if connected, books, fresh, age := c.CLOBStats(); !connected || books != 2 || fresh != 2 || age < 0 {
		t.Fatalf("CLOB vitality = connected=%v books=%d fresh=%d age=%v", connected, books, fresh, age)
	}
	m := c.overlayCLOBBBO(Market{ConditionID: condition, BestBid: 0.10, BestAsk: 0.90})
	if m.BestBid != 0.50 || m.BestAsk != 0.52 {
		t.Fatalf("gamma market did not receive live BBO overlay: %.3f/%.3f", m.BestBid, m.BestAsk)
	}

	c.clobSetConnected(false)
	if _, _, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); ok {
		t.Fatal("disconnected socket must fail closed instead of serving a stale executable quote")
	}
}

func TestR132CLOBPriceChangeAndBestBidAskSchemas(t *testing.T) {
	c := NewClient(time.Second)
	const condition = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	c.registerMarketTokens(Market{ConditionID: condition, TokensRaw: `["yes-token","no-token"]`})
	c.ingestCLOBMarket([]byte(`{"event_type":"book","market":"`+condition+`","asset_id":"yes-token",
		"bids":[{"price":"0.50","size":"10"}],"asks":[{"price":"0.52","size":"8"},{"price":"0.54","size":"20"}]}`), nil)

	// Official current shape: one outer event with price_changes[]. A zero size removes a level;
	// best_bid/best_ask are authoritative when this process joined between full snapshots.
	c.ingestCLOBMarket([]byte(`{"event_type":"price_change","market":"`+condition+`","price_changes":[
		{"asset_id":"yes-token","price":"0.51","size":"7","side":"BUY","best_bid":"0.51","best_ask":"0.52"},
		{"asset_id":"yes-token","price":"0.52","size":"0","side":"SELL","best_bid":"0.51","best_ask":"0.53"}
	]}`), nil)
	bid, ask, bd, ad, _, ok := c.CLOBOutcomeBBO(condition, 0)
	if !ok || bid != 0.51 || ask != 0.53 || bd != 7 || ad != 0 {
		t.Fatalf("price_change BBO = %.3f/%.3f depth %.1f/%.1f ok=%v", bid, ask, bd, ad, ok)
	}

	c.ingestCLOBMarket([]byte(`{"event_type":"best_bid_ask","market":"`+condition+`","asset_id":"yes-token","best_bid":"0.515","best_ask":"0.535"}`), nil)
	bid, ask, bd, ad, _, ok = c.CLOBOutcomeBBO(condition, 0)
	if !ok || bid != 0.515 || ask != 0.535 || bd != 0 || ad != 0 {
		t.Fatalf("best_bid_ask BBO = %.3f/%.3f depth %.1f/%.1f ok=%v", bid, ask, bd, ad, ok)
	}

	var resolved ResolvedEvent
	c.ingestCLOBMarket([]byte(`{"event_type":"market_resolved","market":"`+condition+`","slug":"done","outcomes":["Yes","No"],"winning_outcome":"Yes"}`), func(e ResolvedEvent) { resolved = e })
	if resolved.ConditionID != condition || resolved.WinningOutcome != "Yes" {
		t.Fatalf("resolution callback regressed: %+v", resolved)
	}
	c.ingestCLOBMarket([]byte("PONG"), nil) // heartbeat is valid non-JSON, must not panic
}

func TestR132CLOBReconnectRequiresCurrentGenerationQuote(t *testing.T) {
	c := NewClient(time.Second)
	const condition = "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	c.registerMarketTokens(Market{ConditionID: condition, TokensRaw: `["yes-token","no-token"]`})
	c.ingestCLOBMarket([]byte(`{"event_type":"book","market":"`+condition+`","asset_id":"yes-token",
		"bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.44","size":"12"}]}`), nil)
	if _, _, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); !ok {
		t.Fatal("initial book should be executable")
	}

	c.clobBeginConnection(nil, time.Now())
	if _, _, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); ok {
		t.Fatal("a reconnect must not promote the prior socket's book")
	}
	c.ingestCLOBMarket([]byte(`{"event_type":"best_bid_ask","market":"`+condition+`","asset_id":"yes-token",
		"best_bid":"0.41","best_ask":"0.45"}`), nil)
	if bid, ask, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); !ok || bid != 0.41 || ask != 0.45 {
		t.Fatalf("current-generation BBO = %.3f/%.3f ok=%v", bid, ask, ok)
	}

	c.clobBeginConnection(nil, time.Now())
	if _, _, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); ok {
		t.Fatal("second reconnect reused a first-generation quote")
	}
	// A delta may beat the replacement connection's full snapshot. Its authoritative BBO is
	// usable, but old-generation levels/depth must not be mixed into the new quote.
	c.ingestCLOBMarket([]byte(`{"event_type":"price_change","market":"`+condition+`","price_changes":[
		{"asset_id":"yes-token","price":"0.42","size":"3","side":"BUY","best_bid":"0.42","best_ask":"0.46"}]}`), nil)
	if bid, ask, bd, ad, _, ok := c.CLOBOutcomeBBO(condition, 0); !ok || bid != 0.42 || ask != 0.46 || bd != 3 || ad != 0 {
		t.Fatalf("post-reconnect delta mixed stale state: %.3f/%.3f depth %.1f/%.1f ok=%v", bid, ask, bd, ad, ok)
	}
}

func TestR132CLOBUnknownSideCannotFabricateAsk(t *testing.T) {
	c := NewClient(time.Second)
	const condition = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	c.registerMarketTokens(Market{ConditionID: condition, TokensRaw: `["yes-token","no-token"]`})
	c.ingestCLOBMarket([]byte(`{"event_type":"book","market":"`+condition+`","asset_id":"yes-token",
		"bids":[{"price":"0.48","size":"10"}],"asks":[{"price":"0.52","size":"10"}]}`), nil)
	c.ingestCLOBMarket([]byte(`{"event_type":"price_change","market":"`+condition+`","price_changes":[
		{"asset_id":"yes-token","price":"0.49","size":"999","side":"UNKNOWN","best_bid":"0.49","best_ask":"0.50"}]}`), nil)
	bid, ask, bd, ad, _, ok := c.CLOBOutcomeBBO(condition, 0)
	if !ok || bid != 0.48 || ask != 0.52 || bd != 10 || ad != 10 {
		t.Fatalf("unknown side mutated book: %.3f/%.3f depth %.1f/%.1f ok=%v", bid, ask, bd, ad, ok)
	}
}

func TestR132CLOBUsesSourceTimestampAndTickSchema(t *testing.T) {
	c := NewClient(time.Second)
	const condition = "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	c.registerMarketTokens(Market{ConditionID: condition, TokensRaw: `["yes-token","no-token"]`})
	staleMillis := time.Now().Add(-7 * time.Minute).UnixMilli()
	c.ingestCLOBMarket([]byte(fmt.Sprintf(`{"event_type":"book","market":"%s","asset_id":"yes-token",
		"timestamp":"%d","bids":[{"price":"0.48","size":"10"}],"asks":[{"price":"0.52","size":"10"}]}`,
		condition, staleMillis)), nil)
	if _, _, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); ok {
		t.Fatal("a delayed seven-minute-old source frame appeared executable at arrival time")
	}

	nowMillis := time.Now().UnixMilli()
	c.ingestCLOBMarket([]byte(fmt.Sprintf(`{"event_type":"book","market":"%s","asset_id":"yes-token",
		"timestamp":"%d","bids":[{"price":"0.49","size":"11"}],"asks":[{"price":"0.51","size":"13"}]}`,
		condition, nowMillis)), nil)
	if _, _, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); !ok {
		t.Fatal("current source-timestamped book was rejected")
	}
	c.ingestCLOBMarket([]byte(fmt.Sprintf(`{"event_type":"tick_size_change","market":"%s","asset_id":"yes-token",
		"old_tick_size":"0.01","new_tick_size":"0.001","timestamp":"%d"}`, condition, nowMillis)), nil)
	if tick, _, ok := c.CLOBOutcomeTick(condition, 0); !ok || tick != 0.001 {
		t.Fatalf("tick_size_change = %.4f ok=%v, want .001 true", tick, ok)
	}

	c.clobBeginConnection(nil, time.Now())
	if _, _, ok := c.CLOBOutcomeTick(condition, 0); ok {
		t.Fatal("reconnect promoted a prior-generation tick update")
	}
	// An explicitly malformed timestamp is not allowed to freshen the new generation.
	c.ingestCLOBMarket([]byte(`{"event_type":"best_bid_ask","market":"`+condition+`","asset_id":"yes-token",
		"timestamp":"not-a-time","best_bid":"0.40","best_ask":"0.60"}`), nil)
	if _, _, _, _, _, ok := c.CLOBOutcomeBBO(condition, 0); ok {
		t.Fatal("malformed source timestamp freshened a stale book")
	}
}

type r132RoundTripper struct {
	mu    sync.Mutex
	pages map[string][]byte
	seen  []string
}

type r132RoundTripFunc func(*http.Request) (*http.Response, error)

func (f r132RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func (rt *r132RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	cursor := req.URL.Query().Get("after_cursor")
	rt.seen = append(rt.seen, cursor)
	body, ok := rt.pages[cursor]
	if !ok {
		return nil, fmt.Errorf("unexpected cursor %q", cursor)
	}
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: req}, nil
}

func TestR132EventKeysetCrawlKeepsEveryNestedMarket(t *testing.T) {
	mkMarket := func(i int) Market {
		return Market{Question: fmt.Sprintf("M%d", i), ConditionID: fmt.Sprintf("0x%064x", i+1),
			Slug: fmt.Sprintf("m-%d", i), TokensRaw: fmt.Sprintf(`["yes-%d","no-%d"]`, i, i),
			Active: true, AcceptingOrders: true, BestBid: 0.4, BestAsk: 0.6}
	}
	type ev struct {
		Slug    string              `json:"slug"`
		Active  bool                `json:"active"`
		Closed  bool                `json:"closed"`
		Tags    []map[string]string `json:"tags"`
		Markets []Market            `json:"markets"`
	}
	firstMarkets := make([]Market, 35) // regression: nested outcomes must not be capped at 30
	for i := range firstMarkets {
		firstMarkets[i] = mkMarket(i)
	}
	p1, _ := json.Marshal(map[string]any{"events": []ev{{Slug: "event-one", Active: true,
		Tags: []map[string]string{{"label": "Sports", "slug": "sports"}}, Markets: firstMarkets}}, "next_cursor": "cursor-2"})
	p2, _ := json.Marshal(map[string]any{"events": []ev{{Slug: "event-two", Active: true,
		Markets: []Market{mkMarket(35), mkMarket(36)}}}, "next_cursor": ""})
	rt := &r132RoundTripper{pages: map[string][]byte{"": p1, "cursor-2": p2}}
	c := NewClient(time.Second)
	c.http.Transport = rt
	c.maybeRefreshEventCrawl()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c.evtMu.Lock()
		busy, rows := c.evtRefreshing, append([]Market(nil), c.evtCache...)
		c.evtMu.Unlock()
		if !busy {
			if len(rows) != 37 {
				t.Fatalf("complete keyset crawl retained %d markets, want 37", len(rows))
			}
			if len(rows[0].Events) == 0 || rows[0].Events[0].Slug != "event-one" || rows[0].Category() != "Sports" {
				t.Fatalf("parent event metadata was not preserved: %+v", rows[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("keyset crawl did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	rt.mu.Lock()
	seen := append([]string(nil), rt.seen...)
	rt.mu.Unlock()
	if len(seen) != 2 || seen[0] != "" || seen[1] != "cursor-2" {
		t.Fatalf("cursor walk = %v, want [\"\" cursor-2]", seen)
	}
	if _, ok := c.clobAssets[fmt.Sprintf("0x%064x", 37)]; !ok {
		t.Fatal("last-page market tokens were not registered for CLOB subscription")
	}
}

func TestR133EventKeysetCrawlResumesFailedPageCheckpoint(t *testing.T) {
	mk := func(i int) Market {
		return Market{ConditionID: fmt.Sprintf("cond-%d", i), TokensRaw: fmt.Sprintf(`["y-%d","n-%d"]`, i, i), Active: true}
	}
	p1, _ := json.Marshal(map[string]any{"events": []map[string]any{{"active": true, "markets": []Market{mk(1)}}}, "next_cursor": "next"})
	p2, _ := json.Marshal(map[string]any{"events": []map[string]any{{"active": true, "markets": []Market{mk(2)}}}, "next_cursor": ""})
	var mu sync.Mutex
	seen := []string{}
	failNext := true
	c := NewClient(time.Second)
	c.http.Transport = r132RoundTripFunc(func(req *http.Request) (*http.Response, error) {
		cursor := req.URL.Query().Get("after_cursor")
		mu.Lock()
		seen = append(seen, cursor)
		fail := cursor == "next" && failNext
		if fail {
			failNext = false
		}
		mu.Unlock()
		status, body := http.StatusOK, p1
		if cursor == "next" {
			body = p2
		}
		if fail {
			status, body = http.StatusBadRequest, []byte(`{"error":"transient edge page"}`)
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: req}, nil
	})
	wait := func() {
		deadline := time.Now().Add(3 * time.Second)
		for {
			c.evtMu.Lock()
			busy := c.evtRefreshing
			c.evtMu.Unlock()
			if !busy {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("keyset attempt did not finish")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	c.maybeRefreshEventCrawl()
	wait()
	partial := c.CompleteCatalogSnapshot()
	if partial.Complete || partial.CheckpointRows != 1 || partial.CheckpointPage != 1 || partial.CheckpointLimit != 10 {
		t.Fatalf("failed page lost checkpoint or claimed completeness: %+v", partial)
	}
	c.maybeRefreshEventCrawl()
	wait()
	got := c.CompleteCatalogSnapshot()
	if !got.Complete || len(got.Markets) != 2 || got.CheckpointRows != 0 {
		t.Fatalf("resumed crawl did not publish complete union: %+v", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 || seen[0] != "" || seen[1] != "next" || seen[2] != "next" {
		t.Fatalf("crawl restarted instead of resuming failed cursor: %v", seen)
	}
}

func TestR133CLOBSubscriptionChunksBoundInitialSnapshotFrame(t *testing.T) {
	assets := make([]string, 398)
	for i := range assets {
		assets[i] = fmt.Sprintf("token-%03d", i)
	}
	chunks := clobAssetChunks(assets)
	seen := 0
	for i, chunk := range chunks {
		if len(chunk) == 0 || len(chunk) > clobSubChunk {
			t.Fatalf("chunk %d size=%d cap=%d", i, len(chunk), clobSubChunk)
		}
		seen += len(chunk)
	}
	if seen != len(assets) || len(chunks) != 10 {
		t.Fatalf("chunking lost assets: chunks=%d seen=%d want=%d", len(chunks), seen, len(assets))
	}
}

func TestR159CLOBPriorityPlanRefreshDiffsInPlace(t *testing.T) {
	add, remove := clobAssetDiff(
		[]string{"a", "b", "b", "c"},
		[]string{"b", "c", "d", "e", "e"},
	)
	if got, want := strings.Join(add, ","), "d,e"; got != want {
		t.Fatalf("add=%q want=%q", got, want)
	}
	if got, want := strings.Join(remove, ","), "a"; got != want {
		t.Fatalf("remove=%q want=%q", got, want)
	}
}

func TestR132MarketBySlugRetriesClosedLifecycle(t *testing.T) {
	const condition = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	c := NewClient(time.Second)
	var calls []bool
	c.http.Transport = r132RoundTripFunc(func(req *http.Request) (*http.Response, error) {
		closed := req.URL.Query().Get("closed") == "true"
		calls = append(calls, closed)
		body := `[]`
		if closed {
			body = `[{"conditionId":"` + condition + `","slug":"settled-slug","closed":true,"active":false,"clobTokenIds":"[\"yes-token\",\"no-token\"]"}]`
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
	})
	m, ok := c.MarketBySlug(context.Background(), "settled-slug")
	if !ok || m.ConditionID != condition || !m.Closed {
		t.Fatalf("closed slug lookup failed: %+v ok=%v", m, ok)
	}
	if len(calls) != 2 || calls[0] || !calls[1] {
		t.Fatalf("slug lifecycle query order = %v, want [open, closed]", calls)
	}
	if assets := c.clobAssets[condition]; len(assets) != 2 || assets[0] != "yes-token" || assets[1] != "no-token" {
		t.Fatalf("closed market token schema not registered: %v", assets)
	}
}
