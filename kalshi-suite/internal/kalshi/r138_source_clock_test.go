package kalshi

import (
	"fmt"
	"testing"
	"time"
)

func TestR138TickerSourceClockExcludesRESTSeedsAndUsesVenueTime(t *testing.T) {
	c := NewClient(BaseDemo, nil, 10, time.Second)
	want := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	c.ingestTicker([]byte(fmt.Sprintf(`{"type":"ticker","msg":{"market_ticker":"KXCLOCK",
		"yes_bid_dollars":"0.4000","yes_ask_dollars":"0.4200","ts_ms":%d}}`, want.UnixMilli())))
	c.SeedTickers([]Market{{Ticker: "REST-SEED", LastPrice: flexFloat(0.50)}})
	got, rows, frames, rejects, _ := c.TickerSourceClock()
	if rows != 1 || frames != 1 || rejects != 0 || !got.Equal(want) {
		t.Fatalf("ticker clock rows=%d watermark=%s, want 1/%s", rows, got, want)
	}
}

func TestR138TickerExecutionRejectsMissingStaleAndFutureSourceClock(t *testing.T) {
	c := NewClient(BaseDemo, nil, 10, time.Second)
	frame := func(ticker string, ts *time.Time) []byte {
		clock := ""
		if ts != nil {
			clock = fmt.Sprintf(`,"ts_ms":%d`, ts.UnixMilli())
		}
		return []byte(fmt.Sprintf(`{"type":"ticker","msg":{"market_ticker":%q,
			"yes_bid_dollars":"0.4000","yes_ask_dollars":"0.4200"%s}}`, ticker, clock))
	}
	stale := time.Now().Add(-time.Minute)
	future := time.Now().Add(10 * time.Second)
	c.ingestTicker(frame("MISSING", nil))
	c.ingestTicker(frame("STALE", &stale))
	c.ingestTicker(frame("FUTURE", &future))
	for _, ticker := range []string{"MISSING", "STALE", "FUTURE"} {
		if _, ok := c.LivePrice(ticker); ok {
			t.Fatalf("%s source-clock defect reached LivePrice", ticker)
		}
		if _, _, ok := c.LiveBidAsk(ticker); ok {
			t.Fatalf("%s source-clock defect reached LiveBidAsk", ticker)
		}
	}
	_, rows, frames, rejects, lastReject := c.TickerSourceClock()
	if rows != 0 || frames != 3 || rejects != 3 || lastReject.IsZero() {
		t.Fatalf("clock rejection receipt rows=%d frames=%d rejects=%d at=%s", rows, frames, rejects, lastReject)
	}
}

func TestR138RejectedTickerCannotOverwriteFreshExecutableBBOOrFireCancelHook(t *testing.T) {
	c := NewClient(BaseDemo, nil, 10, time.Second)
	called := 0
	c.SetTickHandler(func(string, float64) { called++ })
	fresh := time.Now().Add(-time.Second)
	c.ingestTicker([]byte(fmt.Sprintf(`{"type":"ticker","msg":{"market_ticker":"KXKEEP",
		"yes_bid_dollars":"0.40","yes_ask_dollars":"0.42","ts_ms":%d}}`, fresh.UnixMilli())))
	stale := time.Now().Add(-time.Minute)
	c.ingestTicker([]byte(fmt.Sprintf(`{"type":"ticker","msg":{"market_ticker":"KXKEEP",
		"yes_bid_dollars":"0.10","yes_ask_dollars":"0.12","ts_ms":%d}}`, stale.UnixMilli())))
	bid, ask, ok := c.LiveBidAsk("KXKEEP")
	if !ok || bid != .40 || ask != .42 || called != 1 {
		t.Fatalf("stale overwrite/cancel hook: bid=%v ask=%v ok=%v calls=%d", bid, ask, ok, called)
	}
}

func TestR148AcceptedTickerExpiresOnVenueClockNotLocalReceipt(t *testing.T) {
	c := NewClient(BaseDemo, nil, 10, time.Second)
	nearExpiry := time.Now().UTC().Add(-29 * time.Second)
	c.ingestTicker([]byte(fmt.Sprintf(`{"type":"ticker","msg":{"market_ticker":"KXAGE",
		"yes_bid_dollars":"0.40","yes_ask_dollars":"0.42","ts_ms":%d}}`, nearExpiry.UnixMilli())))
	if _, _, ok := c.LiveBidAsk("KXAGE"); !ok {
		t.Fatal("accepted near-expiry venue frame was not initially executable")
	}
	// Simulate the passage beyond the 30-second source-time lease while leaving the local map row
	// otherwise untouched. Read-time authority must expire; arrival cannot grant another 30s.
	c.txMu.Lock()
	sample := c.txPx["KXAGE"]
	sample.sourceAt = time.Now().UTC().Add(-31 * time.Second)
	sample.at = time.Now() // adversarial local-arrival refresh must not rescue a stale WS clock
	c.txPx["KXAGE"] = sample
	c.txMu.Unlock()
	if _, ok := c.LivePrice("KXAGE"); ok {
		t.Fatal("stale venue source clock retained executable LivePrice authority")
	}
	if _, _, ok := c.LiveBidAsk("KXAGE"); ok {
		t.Fatal("stale venue source clock retained executable LiveBidAsk authority")
	}
	if _, at, ok := c.LivePriceAt("KXAGE"); !ok || time.Since(at) < 30*time.Second {
		t.Fatalf("LivePriceAt leaked local arrival instead of the old venue clock: ok=%v at=%s", ok, at)
	}

	c.txMu.Lock()
	missingClock := c.txPx["KXAGE"]
	missingClock.sourceAt = time.Time{}
	missingClock.at = time.Now()
	c.txPx["KXAGE"] = missingClock
	c.txMu.Unlock()
	if _, _, ok := c.LivePriceAt("KXAGE"); ok {
		t.Fatal("timestamp-less WS row reached a caller-managed freshness path")
	}

	// REST seeds intentionally have no venue clock. Their local seed time remains a short bounded
	// fallback so boot does not invent WS liveness while the ticker stream warms.
	c.SeedTickers([]Market{{Ticker: "REST-R148", LastPrice: flexFloat(.50), YesBid: flexFloat(.49), YesAsk: flexFloat(.51)}})
	if _, _, ok := c.LiveBidAsk("REST-R148"); !ok {
		t.Fatal("fresh bounded REST seed was accidentally forced to have a WS source timestamp")
	}
}

func TestR138LifecycleSourceClockDoesNotInventMissingTimestamp(t *testing.T) {
	c := NewClient(BaseDemo, nil, 10, time.Second)
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","sid":7,"seq":10,"msg":{"event_type":"activated","market_ticker":"KXNOCLK"}}`))
	if got, frames := c.LifecycleSourceClock(); frames != 1 || !got.IsZero() {
		t.Fatalf("missing timestamp became source time: frames=%d at=%s", frames, got)
	}
	truth := c.LifecycleSequenceTruth()
	if truth.SID != 7 || truth.Sequence != 10 || truth.Frames != 1 || !truth.Watermark.IsZero() {
		t.Fatalf("missing timestamp lost sequence liveness: %+v", truth)
	}
	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","sid":7,"seq":12,"msg":{"event_type":"deactivated","market_ticker":"KXNOCLK"}}`))
	truth = c.LifecycleSequenceTruth()
	if truth.Prior != 10 || truth.Sequence != 12 || truth.LastGap != 1 || truth.GapTotal != 1 {
		t.Fatalf("lifecycle source gap truth lost: %+v", truth)
	}
	want := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	c.ingestLifecycle([]byte(fmt.Sprintf(`{"type":"market_lifecycle_v2","msg":{"event_type":"deactivated",
		"market_ticker":"KXCLOCK","ts_ms":%d}}`, want.UnixMilli())))
	if got, frames := c.LifecycleSourceClock(); frames != 3 || !got.Equal(want) {
		t.Fatalf("lifecycle clock frames=%d at=%s, want 3/%s", frames, got, want)
	}
}
