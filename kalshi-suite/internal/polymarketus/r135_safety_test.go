package polymarketus

import (
	"encoding/json"
	"testing"
	"time"
)

func TestR135PrivateBalanceFailsClosedWhenStaleOrDisconnected(t *testing.T) {
	w := &PrivateWS{
		bal: 41.25, buyPow: 19.75, balOK: true,
		connected: true, lastMsg: time.Now(),
	}
	if bal, pow, ok := w.Balance(); !ok || bal != 41.25 || pow != 19.75 {
		t.Fatalf("fresh connected balance = %.2f/%.2f ok=%v", bal, pow, ok)
	}

	w.mu.Lock()
	w.lastMsg = time.Now().Add(-privateWSFreshMaxAge - time.Second)
	w.mu.Unlock()
	if bal, pow, ok := w.Balance(); ok || bal != 0 || pow != 0 {
		t.Fatalf("stale cached balance escaped: %.2f/%.2f ok=%v", bal, pow, ok)
	}

	w.mu.Lock()
	w.lastMsg, w.connected = time.Now(), false
	w.mu.Unlock()
	if bal, pow, ok := w.Balance(); ok || bal != 0 || pow != 0 {
		t.Fatalf("disconnected cached balance escaped: %.2f/%.2f ok=%v", bal, pow, ok)
	}

	w.mu.Lock()
	w.bal, w.buyPow, w.balOK = 99, 88, true // prior session's last snapshot
	w.beginSessionLocked(time.Now())
	w.mu.Unlock()
	if bal, pow, ok := w.Balance(); ok || bal != 0 || pow != 0 {
		t.Fatalf("reconnect reused prior-session balance: %.2f/%.2f ok=%v", bal, pow, ok)
	}
}

func TestR135LiteCannotBorrowHistoricalFullLifecycle(t *testing.T) {
	ws := &MarketsWS{live: map[string]*pusLive{}}
	ws.ingest([]byte(`{"marketData":{"marketSlug":"lite-after-full","state":"MARKET_STATE_OPEN","bids":[{"px":"0.40","qty":"5"}],"offers":[{"px":"0.42","qty":"6"}]}}`))
	if _, _, ok := ws.LiveBidAsk("lite-after-full"); !ok {
		t.Fatal("same-fresh full OPEN frame should authorize its own BBO")
	}

	newGen := ws.sessionSeq.Add(1)
	ws.notePrimaryStart(newGen) // reconnect: old-generation OPEN authority is invalid immediately
	ws.ingest([]byte(`{"marketDataLite":{"marketSlug":"lite-after-full","bestBid":"0.41","bestAsk":"0.43"}}`))
	if _, _, ok := ws.LiveBidAsk("lite-after-full"); ok {
		t.Fatal("fresh LITE BBO borrowed prior-generation full-frame OPEN authority")
	}

	ws.SetOpenSlugs([]string{"lite-after-full"})
	if bid, ask, ok := ws.LiveBidAsk("lite-after-full"); !ok || bid != 0.41 || ask != 0.43 {
		t.Fatalf("fresh REST-open proof should authorize LITE BBO: %.2f/%.2f ok=%v", bid, ask, ok)
	}

	ws.mu.Lock()
	ws.restOpenAt = time.Now().Add(-marketsRESTLifecycleMaxAge - time.Second)
	ws.mu.Unlock()
	if _, _, ok := ws.LiveBidAsk("lite-after-full"); ok {
		t.Fatal("stale REST plus stale full lifecycle authorized LITE BBO")
	}
}

func TestR135MarketSideCurrencyTokenFallsThroughToQuote(t *testing.T) {
	var side MarketSide
	if err := json.Unmarshal([]byte(`{"identifier":"m","price":"USD","quote":{"value":"0.375","currency":"USD"}}`), &side); err != nil {
		t.Fatalf("currency metadata must not abort the complete market crawl: %v", err)
	}
	if side.Price != "0.375" {
		t.Fatalf("quote fallback price=%q, want 0.375", side.Price)
	}
	if err := json.Unmarshal([]byte(`{"price":"definitely-not-a-price"}`), &side); err == nil {
		t.Fatal("non-currency malformed side price must still fail closed")
	}
}
