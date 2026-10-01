package polymarketus

// R70-B SCHEMA_AUDIT #1: lock the MARKET_DATA per-level size decode — the venue's confirmed
// {"px":{"value":"0.55"},"qty":"120"} level encoding (same as the REST book, ParseBook) must feed
// Book3 (top-3 shape + touch sizes) and keep LiveBidAsk intact. If the venue renames qty, this
// fails loudly instead of silently re-NULLing every polyus book feature.

import (
	"encoding/json"
	"testing"
	"time"
)

func TestIngestMarketDataLevels(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}}
	frame := `{"marketData":{"marketSlug":"mlb-chc-nyy","state":"MARKET_STATE_OPEN","bids":[` +
		`{"px":{"value":"0.55"},"qty":"120"},{"px":{"value":"0.54"},"qty":"80"},{"px":{"value":"0.53"},"qty":"50"},{"px":{"value":"0.52"},"qty":"999"}],` +
		`"offers":[{"px":{"value":"0.57"},"qty":"60"},{"px":{"value":"0.58"},"qty":"40"}]}}`
	ws.ingest([]byte(frame))

	bid, ask, ok := ws.LiveBidAsk("mlb-chc-nyy")
	if !ok || bid != 0.55 || ask != 0.57 {
		t.Fatalf("LiveBidAsk = (%v,%v,%v), want (0.55,0.57,true)", bid, ask, ok)
	}
	imb3, depth3, bidSz, askSz, ok := ws.Book3("mlb-chc-nyy")
	if !ok {
		t.Fatal("Book3 not ok after a full MARKET_DATA frame")
	}
	// top-3 bids 120+80+50=250 (the 4th level must NOT count), top-3 offers 60+40=100.
	if depth3 != 350 {
		t.Fatalf("depth3 = %v, want 350 (top-3 only, both sides)", depth3)
	}
	if want := 250.0 / 350.0; imb3 < want-1e-9 || imb3 > want+1e-9 {
		t.Fatalf("imb3 = %v, want %v", imb3, want)
	}
	if bidSz != 120 || askSz != 60 {
		t.Fatalf("touch sizes = (%v,%v), want (120,60)", bidSz, askSz)
	}

	// Runtime-quiet complete snapshots remain current while their connection generation is healthy.
	ws.mu.Lock()
	ws.live["mlb-chc-nyy"].bookAt = time.Now().Add(-2 * time.Minute)
	ws.mu.Unlock()
	if _, _, _, _, ok := ws.Book3("mlb-chc-nyy"); !ok {
		t.Fatal("unchanged current-generation Book3 expired by wall clock")
	}
	ws.primaryFrameAt.Store(time.Now().Add(-marketsWSPrimaryTransportMaxAge - time.Second).UnixNano())
	if _, _, _, _, ok := ws.Book3("mlb-chc-nyy"); ok {
		t.Fatal("Book3 remained valid after primary transport vitality expired")
	}

	// One-sided/empty top-3 books: imb3 stays out-of-band (-1) → Book3 not ok, never a fake 0.
	ws.ingest([]byte(`{"marketData":{"marketSlug":"empty-book","state":"MARKET_STATE_OPEN","bids":[],"offers":[]}}`))
	if _, _, _, _, ok := ws.Book3("empty-book"); ok {
		t.Fatal("empty book must not report a shape")
	}
}

func TestExecutableBookFreshnessAndLifecycleAreIndependent(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}}
	ws.ingest([]byte(`{"marketData":{"marketSlug":"m","state":"MARKET_STATE_OPEN","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"5"}]}}`))
	ws.mu.Lock()
	ws.live["m"].quoteAt = time.Now().Add(-time.Minute)
	ws.mu.Unlock()
	// A newer trade updates momentum/tape time, not BBO contents. The unchanged current-generation
	// BBO remains authoritative; elapsed wall time alone is not a disconnect.
	ws.ingest([]byte(`{"trade":{"marketSlug":"m","price":{"value":"0.41"},"quantity":{"value":"2"},"taker":{"intent":"ORDER_INTENT_SELL_LONG"}}}`))
	if bid, ask, ok := ws.LiveBidAsk("m"); !ok || bid != 0.40 || ask != 0.42 {
		t.Fatalf("quiet current-generation BBO expired or changed: %.2f/%.2f ok=%v", bid, ask, ok)
	}

	newGen := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(newGen)
	ws.notePrimaryTransport(newGen) // heartbeat alone cannot carry the old generation's BBO
	if _, _, ok := ws.LiveBidAsk("m"); ok {
		t.Fatal("prior-generation BBO survived reconnect before a replacement snapshot")
	}

	ws.ingest([]byte(`{"marketData":{"marketSlug":"m","state":"MARKET_STATE_HALTED","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"5"}]}}`))
	if _, _, ok := ws.LiveBidAsk("m"); ok {
		t.Fatal("halted market must invalidate its cached executable BBO")
	}
}

func TestPrimaryFrameAgeIsIndependentFromExecutableQuoteAge(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}}
	ws.primaryFrameAt.Store(time.Now().Add(-2 * time.Second).UnixNano())
	age, ok := ws.PrimaryFrameAge()
	if !ok || age < time.Second || age > 5*time.Second {
		t.Fatalf("primary frame age = %v, ok=%v", age, ok)
	}
	if ws.ExecutableCount() != 0 {
		t.Fatal("heartbeat/frame vitality must not invent an executable quote")
	}
}

func TestMarketDataFrameClassificationExcludesHeartbeats(t *testing.T) {
	if isMarketsDataFrame([]byte(`{"heartbeat":{}}`)) || isMarketsDataFrame([]byte(`{"requestId":"mdl-0"}`)) {
		t.Fatal("heartbeat/subscription receipts must not prove market-data delivery")
	}
	for _, frame := range []string{
		`{"marketData":{"marketSlug":"m"}}`,
		`{"market_data_lite":{"market_slug":"m"}}`,
		`{"trade":{"market_slug":"m"}}`,
	} {
		if !isMarketsDataFrame([]byte(frame)) {
			t.Fatalf("market payload not recognized: %s", frame)
		}
	}
}

func TestMarketsHeartbeatReplyMatchesDocumentedSchema(t *testing.T) {
	b, err := json.Marshal(marketsHeartbeatReply())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), `{"heartbeat":{}}`; got != want {
		t.Fatalf("heartbeat reply schema = %s, want %s", got, want)
	}
}

func TestTakerIntentAllFourDirections(t *testing.T) {
	tests := []struct {
		intent string
		bull   bool
	}{
		{"ORDER_INTENT_BUY_LONG", true},
		{"ORDER_INTENT_SELL_LONG", false},
		{"ORDER_INTENT_BUY_SHORT", false},
		{"ORDER_INTENT_SELL_SHORT", true},
	}
	for _, tc := range tests {
		got, ok := takerBullish(tc.intent, "", "")
		if !ok || got != tc.bull {
			t.Fatalf("%s = (%v,%v), want (%v,true)", tc.intent, got, ok, tc.bull)
		}
	}
}
