package polymarketus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func r132Bool(v bool) *bool { return &v }

func r132OpenMarket(i int) Market {
	return Market{
		ID:       fmt.Sprintf("id-%03d", i),
		Slug:     fmt.Sprintf("slug-%03d", i),
		Active:   r132Bool(true),
		Closed:   r132Bool(false),
		Archived: r132Bool(false),
		State:    "MARKET_STATE_OPEN",
	}
}

func TestOpenMarketsAllRetainsEveryPageAndFiltersLifecycle(t *testing.T) {
	all := make([]Market, 0, 46)
	for i := 0; i < 45; i++ {
		all = append(all, r132OpenMarket(i))
	}
	all[20].State = "MARKET_STATE_SUSPENDED"
	all[21].Archived = r132Bool(true)
	all[22].Closed = r132Bool(true)
	all[23].Active = r132Bool(false)

	var mu sync.Mutex
	var offsets []int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/markets" {
			http.NotFound(w, r)
			return
		}
		q := r.URL.Query()
		if q.Get("active") != "true" || q.Get("closed") != "false" || q.Get("category") != "sports" {
			t.Errorf("query filters not preserved/forced: %s", r.URL.RawQuery)
		}
		if q.Get("limit") != strconv.Itoa(marketsPageSize) {
			t.Errorf("limit=%q, want %d", q.Get("limit"), marketsPageSize)
		}
		off, _ := strconv.Atoi(q.Get("offset"))
		mu.Lock()
		offsets = append(offsets, off)
		mu.Unlock()
		// Simulate a gateway that silently caps pages at 20 even though 250 was requested.
		end := off + 20
		if end > len(all) {
			end = len(all)
		}
		page := []Market{}
		if off < len(all) {
			page = all[off:end]
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"markets": page})
	}))
	defer ts.Close()

	p := NewPublicClient(time.Second)
	p.baseURL = ts.URL
	got, stats, err := p.OpenMarketsAll(context.Background(), "category=sports&limit=1&offset=999&active=false")
	if err != nil {
		t.Fatalf("OpenMarketsAll: %v", err)
	}
	if len(got) != 41 {
		t.Fatalf("open markets=%d, want 41 (45 minus four terminal/conflicting rows)", len(got))
	}
	if stats.Pages != 4 || stats.Seen != 45 || stats.Unique != 45 || stats.Open != 41 || stats.UnknownLifecycle != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(offsets) != "[0 20 40 45]" {
		t.Fatalf("offsets=%v; crawler must advance by actual page length through empty EOF page", offsets)
	}
}

func TestOpenMarketsAllFailsWholeSnapshotOnUnknownLifecycle(t *testing.T) {
	unknown := Market{ID: "unknown", Slug: "unknown"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("offset") == "0" {
			_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{unknown}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{}})
	}))
	defer ts.Close()
	p := NewPublicClient(time.Second)
	p.baseURL = ts.URL
	got, stats, err := p.OpenMarketsAll(context.Background(), "")
	if !errors.Is(err, ErrUnknownMarketLifecycle) || got != nil || stats.UnknownLifecycle != 1 {
		t.Fatalf("unknown lifecycle must preserve caller's prior complete snapshot: got=%v stats=%+v err=%v", got, stats, err)
	}
}

func TestOpenMarketsAllRejectsRepeatedPageInsteadOfClaimingCompleteness(t *testing.T) {
	m := r132OpenMarket(1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{m}})
	}))
	defer ts.Close()
	p := NewPublicClient(time.Second)
	p.baseURL = ts.URL
	got, _, err := p.OpenMarketsAll(context.Background(), "")
	if err == nil || !strings.Contains(err.Error(), "made no progress") {
		t.Fatalf("got rows=%v err=%v; repeated page must fail closed", got, err)
	}
	if got != nil {
		t.Fatalf("incomplete crawl must not return a partial universe: %v", got)
	}
}

func TestEventOpenMarketsRetainsMoreThanThirtyChildren(t *testing.T) {
	e := Event{ID: "game", Active: r132Bool(true), State: "MARKET_STATE_OPEN"}
	for i := 0; i < 45; i++ {
		e.Markets = append(e.Markets, r132OpenMarket(i))
	}
	closed := r132OpenMarket(99)
	closed.State = "MARKET_STATE_HALTED"
	e.Markets = append(e.Markets, closed)

	got := e.OpenMarkets()
	if len(got) != 45 {
		t.Fatalf("open children=%d, want all 45 (no 30-child cap)", len(got))
	}
	filtered := OpenEvents([]Event{e, {ID: "done", Ended: true, Active: r132Bool(true), Markets: e.Markets}})
	if len(filtered) != 1 || len(filtered[0].Markets) != 45 {
		t.Fatalf("OpenEvents=%+v", filtered)
	}
	if len(e.Markets) != 46 {
		t.Fatal("OpenMarkets/OpenEvents must not mutate the source event")
	}
}

func TestLifecycleTerminalDistinguishesPartialSchemaFromOpenProof(t *testing.T) {
	closedFalse := r132Bool(false)
	e := Event{Closed: closedFalse}
	m := Market{Closed: closedFalse}
	if !e.LifecycleKnown() || !m.LifecycleKnown() {
		t.Fatal("closed:false is a supplied lifecycle field")
	}
	if e.Open() || m.Open() {
		t.Fatal("closed:false alone must not prove accepting/open")
	}
	if e.Terminal() || m.Terminal() {
		t.Fatal("closed:false alone must not erase a league live-feed row as terminal")
	}

	halted := r132OpenMarket(7)
	halted.State = "MARKET_STATE_HALTED"
	if !halted.Terminal() || halted.Open() {
		t.Fatal("explicit halted state must be terminal")
	}
	contradict := r132OpenMarket(8)
	contradict.Status = "MARKET_STATE_HALTED"
	if !contradict.Terminal() || contradict.Open() {
		t.Fatal("state=OPEN plus status=HALTED must fail closed")
	}
	e = Event{Active: r132Bool(true), State: "ACTIVE", Status: "SUSPENDED"}
	if !e.Terminal() || e.Open() {
		t.Fatal("event lifecycle sources must all agree")
	}
}

func TestR132MarketExecutionMetadataSchema(t *testing.T) {
	raw := []byte(`{"id":"m1","slug":"s1","category":"sports","active":true,"closed":false,"archived":false,"volume":12345.5,"volume24hr":678.25,"minimumTradeQty":2,"orderPriceMinTickSize":0.001,"feeCoefficient":0.04}`)
	var m Market
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if !m.Open() || m.Category != "sports" || m.Volume != 12345.5 || m.Volume24hr != 678.25 || m.MinimumQty != 2 || m.TickSize != 0.001 {
		t.Fatalf("execution metadata decoded incorrectly: %+v", m)
	}
	if m.FeeCoeff == nil || *m.FeeCoeff != 0.04 {
		t.Fatalf("feeCoefficient = %v, want supplied 0.04", m.FeeCoeff)
	}
	var feeFree Market
	if err := json.Unmarshal([]byte(`{"feeCoefficient":0}`), &feeFree); err != nil {
		t.Fatal(err)
	}
	if feeFree.FeeCoeff == nil || *feeFree.FeeCoeff != 0 {
		t.Fatalf("explicit fee-free coefficient lost: %v", feeFree.FeeCoeff)
	}
}

func TestR132ParseBookPreservesExecutableTouchSize(t *testing.T) {
	raw := []byte(`{"marketData":{"marketSlug":"m","state":"MARKET_STATE_OPEN","bids":[{"px":{"value":"0.47"},"qty":"7"},{"px":{"value":"0.46"},"qty":"100"}],"offers":[{"px":{"value":"0.49"},"qty":"3.5"},{"px":{"value":"0.50"},"qty":"200"}]}}`)
	b := ParseBook(raw, "m")
	if b.BestBid != 0.47 || b.BestAsk != 0.49 || b.BestBidQty != 7 || b.BestAskQty != 3.5 {
		t.Fatalf("touch quote/size lost: %+v", b)
	}
	if b.BidQty != 107 || b.AskQty != 203.5 {
		t.Fatalf("total depth wrong: %+v", b)
	}
}
