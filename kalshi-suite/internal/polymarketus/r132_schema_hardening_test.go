package polymarketus

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func schemaBool(v bool) *bool { return &v }

func TestMarketFlexibleNumbersAndCurrentWrappedBBO(t *testing.T) {
	shapes := []struct {
		name string
		v    string
	}{
		{"number", `0.125`},
		{"string", `"0.125"`},
		{"amount", `{"value":"0.125","currency":"USD"}`},
	}
	for _, tc := range shapes {
		t.Run(tc.name, func(t *testing.T) {
			raw := fmt.Sprintf(`{"id":"m","slug":"s","ep3Status":"OPEN","bestBid":%[1]s,"bestAsk":%[1]s,"line":%[1]s,"volume":%[1]s,"volume24hr":%[1]s,"minimumTradeQty":%[1]s,"orderPriceMinTickSize":%[1]s,"feeCoefficient":%[1]s}`, tc.v)
			var m Market
			if err := json.Unmarshal([]byte(raw), &m); err != nil {
				t.Fatal(err)
			}
			if !m.Open() || m.BestBid != 0.125 || m.BestAsk != 0.125 || m.Line != 0.125 ||
				m.Volume != 0.125 || m.Volume24hr != 0.125 || m.MinimumQty != 0.125 ||
				m.TickSize != 0.125 || m.FeeCoeff == nil || *m.FeeCoeff != 0.125 {
				t.Fatalf("flexible market fields not normalized: %+v fee=%v", m, m.FeeCoeff)
			}
		})
	}

	// This is the production list shape observed on the gateway: Amount wrappers, with no bare BBO.
	var current Market
	if err := json.Unmarshal([]byte(`{"slug":"live","active":true,"closed":false,"archived":false,"ep3Status":"OPEN","bestBidQuote":{"value":"0.0070","currency":"USD"},"bestAskQuote":{"value":"0.0080","currency":"USD"},"marketSides":[{"long":true,"description":"Yes","quote":{"value":"0.0080"}}]}`), &current); err != nil {
		t.Fatal(err)
	}
	if current.BestBid != 0.007 || current.BestAsk != 0.008 || current.YesPrice() != 0.008 {
		t.Fatalf("wrapped BBO/side quote not normalized: %+v yes=%v", current, current.YesPrice())
	}
	if err := json.Unmarshal([]byte(`{"volume":"not-a-number"}`), &current); err == nil {
		t.Fatal("malformed supplied numeric schema must fail closed")
	}
}

func TestEP3LifecycleIsExactAndAllSourcesMustAgree(t *testing.T) {
	tests := []struct {
		name string
		m    Market
		open bool
		term bool
	}{
		{"ep3 open alone", Market{EP3Status: "OPEN"}, true, false},
		{"preopen is not open", Market{Active: schemaBool(true), EP3Status: "PREOPEN"}, false, true},
		{"halted beats active", Market{Active: schemaBool(true), EP3Status: "MARKET_STATE_HALTED"}, false, true},
		{"contradictory sources", Market{State: "OPEN", EP3Status: "HALTED"}, false, true},
		{"snake current field", func() Market {
			var m Market
			if err := json.Unmarshal([]byte(`{"ep3_status":"OPEN"}`), &m); err != nil {
				t.Fatal(err)
			}
			return m
		}(), true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.m.Open(); got != tc.open {
				t.Fatalf("Open=%v, want %v: %+v", got, tc.open, tc.m)
			}
			if got := tc.m.Terminal(); got != tc.term {
				t.Fatalf("Terminal=%v, want %v: %+v", got, tc.term, tc.m)
			}
		})
	}

	meta := MarketMeta{Active: schemaBool(true), State: "OPEN", EP3Status: "PREOPEN"}
	if !meta.LifecycleKnown() || meta.Open() || !meta.Terminal() {
		t.Fatalf("MarketMeta contradiction must fail closed: %+v", meta)
	}
	if open := (MarketMeta{EP3Status: "OPEN"}); !open.Open() || open.Terminal() {
		t.Fatalf("MarketMeta EP3 OPEN not recognized: %+v", open)
	}
}

func TestOpenMarketsAllFailsOnOneUnknownAmongKnownRows(t *testing.T) {
	open := Market{ID: "open", Slug: "open", EP3Status: "OPEN"}
	unknown := Market{ID: "unknown", Slug: "unknown"}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("offset") == "0" {
			_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{open, unknown}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"markets": []Market{}})
	}))
	defer ts.Close()
	p := NewPublicClient(time.Second)
	p.baseURL = ts.URL
	got, stats, err := p.OpenMarketsAll(context.Background(), "")
	if got != nil || !errors.Is(err, ErrUnknownMarketLifecycle) || stats.UnknownLifecycle != 1 {
		t.Fatalf("one unknown row must reject the partial universe: got=%v stats=%+v err=%v", got, stats, err)
	}
}

func TestGetMarketMetaDecodesCurrentLifecycleAndFlexibleConstraints(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/market/slug/meta" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"market":{"orderPriceMinTickSize":{"value":"0.001"},"minimumTradeQty":"0.25","active":true,"closed":false,"archived":false,"ep3Status":"OPEN"}}`))
	}))
	defer ts.Close()
	c := testSignedClient(ts)
	meta, err := c.GetMarketMeta(context.Background(), "meta")
	if err != nil {
		t.Fatal(err)
	}
	if meta.TickUSD != 0.001 || meta.MinQty != 0.25 || meta.EP3Status != "OPEN" || !meta.Open() {
		t.Fatalf("market meta not decoded: %+v", meta)
	}
}

func TestMarketsWSSnakeSchemaAndExecutableCount(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}}
	ws.ingest([]byte(`{"trade":{"market_slug":"trade-only","price":"0.4","quantity":"2","taker":{"intent":"ORDER_INTENT_BUY_LONG"}}}`))
	if ws.Count() != 1 || ws.ExecutableCount() != 0 {
		t.Fatalf("trade-only traffic must not prove an executable book: count=%d exec=%d", ws.Count(), ws.ExecutableCount())
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	ws.ingest([]byte(fmt.Sprintf(`{"market_data":{"market_slug":"book","ep3_status":"OPEN","transact_time":%q,"bids":[{"px":{"value":"0.40"},"qty":"5"}],"offers":[{"px":"0.42","qty":{"value":"6"}}]}}`, now)))
	if bid, ask, ok := ws.LiveBidAsk("book"); !ok || bid != 0.40 || ask != 0.42 || ws.ExecutableCount() != 1 {
		t.Fatalf("snake full book not executable: bid=%v ask=%v ok=%v exec=%d", bid, ask, ok, ws.ExecutableCount())
	}
	ws.ingest([]byte(`{"market_data":{"market_slug":"book","state":"OPEN","ep3Status":"HALTED","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"6"}]}}`))
	if _, _, ok := ws.LiveBidAsk("book"); ok || ws.ExecutableCount() != 0 {
		t.Fatal("contradictory/halted WS lifecycle must invalidate executable readiness")
	}
}

func TestMarketDataLiteNeedsIndependentFreshRESTOpenProof(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}}
	lite := `{"marketDataLite":{"marketSlug":"lite","bestBid":{"value":"0.40"},"bestAsk":{"value":"0.42"},"currentPx":{"value":"0.41"}}}`
	ws.ingest([]byte(lite))
	if _, _, ok := ws.LiveBidAsk("lite"); ok {
		t.Fatal("lifecycle-free lite BBO must fail closed before independent REST proof")
	}

	ws.SetOpenSlugs([]string{"lite"})
	if bid, ask, ok := ws.LiveBidAsk("lite"); !ok || bid != 0.40 || ask != 0.42 {
		t.Fatalf("fresh REST-open + lite BBO = %v/%v ok=%v, want executable", bid, ask, ok)
	}
	if n, at := ws.OpenSlugStats(); n != 1 || at.IsZero() {
		t.Fatalf("REST proof receipt = n=%d at=%v", n, at)
	}
	ws.mu.Lock()
	ws.restOpenAt = time.Now().Add(-marketsRESTLifecycleMaxAge - time.Second)
	ws.mu.Unlock()
	if _, _, ok := ws.LiveBidAsk("lite"); ok {
		t.Fatal("stale REST lifecycle proof must not authorize a lite-only BBO")
	}

	ws.SetOpenSlugs([]string{"lite"}) // refresh proof for explicit WS-state precedence checks
	ws.ingest([]byte(`{"marketDataLite":{"marketSlug":"lite","state":"MARKET_STATE_HALTED","bestBid":"0.40","bestAsk":"0.42"}}`))
	if _, _, ok := ws.LiveBidAsk("lite"); ok {
		t.Fatal("explicit WS terminal state must beat an affirmative REST-open snapshot")
	}
	ws.ingest([]byte(lite)) // no state cannot erase the explicit terminal latch
	if _, _, ok := ws.LiveBidAsk("lite"); ok {
		t.Fatal("lifecycle-free lite update must not erase an explicit terminal WS state")
	}
	ws.ingest([]byte(`{"marketDataLite":{"marketSlug":"lite","state":"MARKET_STATE_OPEN","bestBid":"0.40","bestAsk":"0.42"}}`))
	if _, _, ok := ws.LiveBidAsk("lite"); !ok {
		t.Fatal("later explicit WS-open + REST-open should restore the fresh BBO")
	}

	ws.SetOpenSlugs(nil) // complete replacement: absence is authoritative terminal/removal truth
	if _, _, ok := ws.LiveBidAsk("lite"); ok {
		t.Fatal("removed REST-open slug must immediately invalidate its cached lite BBO")
	}

	// A complete REST replacement also invalidates an older explicit WS-open quote.
	ws.ingest([]byte(`{"marketData":{"marketSlug":"old-open","state":"MARKET_STATE_OPEN","bids":[{"px":"0.30","qty":"5"}],"offers":[{"px":"0.32","qty":"5"}]}}`))
	if _, _, ok := ws.LiveBidAsk("old-open"); ok {
		t.Fatal("slug absent from the latest complete REST universe must remain invalid")
	}
}

func TestPlaceLiveOrderPreservesSubCentPrice(t *testing.T) {
	type captured struct {
		Value       string
		OutcomeSide string
	}
	captures := make(chan captured, 8)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Price struct {
				Value string `json:"value"`
			} `json:"price"`
			OutcomeSide string `json:"outcomeSide"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
		}
		captures <- captured{req.Price.Value, req.OutcomeSide}
		_, _ = w.Write([]byte(`{"id":"order-id"}`))
	}))
	defer ts.Close()
	c := testSignedClient(ts)
	tests := []struct {
		outcome string
		price   float64
		want    captured
	}{
		{"YES", 0.055, captured{"0.055", "OUTCOME_SIDE_YES"}},
		{"YES", 0.001, captured{"0.001", "OUTCOME_SIDE_YES"}},
		{"YES", 0.0001, captured{"0.0001", "OUTCOME_SIDE_YES"}},
		{"NO", 0.055, captured{"0.945", "OUTCOME_SIDE_NO"}},
	}
	for _, tc := range tests {
		if _, err := c.PlaceLiveOrder(context.Background(), LiveOrder{
			MarketSlug: "m", Outcome: tc.outcome, Side: "BUY", Price: tc.price, Size: 1, PostOnly: true,
		}); err != nil {
			t.Fatalf("%s %.6f: %v", tc.outcome, tc.price, err)
		}
		if got := <-captures; got != tc.want {
			t.Fatalf("%s %.6f wire=%+v, want %+v", tc.outcome, tc.price, got, tc.want)
		}
	}
}

func testSignedClient(ts *httptest.Server) *Client {
	seed := make([]byte, ed25519.SeedSize)
	return &Client{
		baseURL: ts.URL,
		keyID:   "test",
		priv:    ed25519.NewKeyFromSeed(seed),
		http:    ts.Client(),
	}
}
