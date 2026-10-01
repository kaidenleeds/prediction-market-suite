package kalshi

import (
	"fmt"
	"testing"
	"time"
)

func TestR159LifecycleStreamPatchesCachedExecutionMetadata(t *testing.T) {
	oldSlice := []Market{{
		Ticker: "KXR159", Status: "active",
		CloseTime: "2026-07-18T20:00:00Z", ExpectedExpiration: "2026-07-18T20:00:00Z",
	}}
	c := &Client{mktCache: oldSlice, mktCacheAt: time.Now()}

	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"deactivated","market_ticker":"KXR159","ts_ms":1700000000000}}`))
	got, ok := c.MarketCached("kxr159")
	if !ok || got.Status != "inactive" {
		t.Fatalf("deactivation did not patch cached lifecycle: %+v ok=%v", got, ok)
	}
	if oldSlice[0].Status != "active" {
		t.Fatalf("published pre-update slice was mutated in place: %+v", oldSlice[0])
	}

	c.ingestLifecycle([]byte(`{"type":"market_lifecycle_v2","msg":{"event_type":"activated","market_ticker":"KXR159","ts_ms":1700000001000}}`))
	got, _ = c.MarketCached("KXR159")
	if got.Status != "active" {
		t.Fatalf("activation did not restore cached lifecycle: %+v", got)
	}

	closeAt := time.Now().Add(90 * time.Minute).UTC().Truncate(time.Second).Format(time.RFC3339)
	c.ingestLifecycle([]byte(fmt.Sprintf(
		`{"type":"market_lifecycle_v2","msg":{"event_type":"close_date_updated","market_ticker":"KXR159","close_time":%q,"ts_ms":1700000002000}}`,
		closeAt)))
	got, _ = c.MarketCached("KXR159")
	if got.CloseTime != closeAt || got.ExpectedExpiration != closeAt {
		t.Fatalf("close-date update did not patch both execution clocks: %+v", got)
	}
}

func TestR159TerminalLifecycleImmediatelyVetoesCachedExecutionMetadata(t *testing.T) {
	for _, eventType := range []string{"determined", "settled"} {
		t.Run(eventType, func(t *testing.T) {
			published := []Market{{
				Ticker: "KXTERMINAL", Status: "active",
				CloseTime: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
			}}
			c := &Client{mktCache: published, mktCacheAt: time.Now()}

			c.ingestLifecycle([]byte(fmt.Sprintf(
				`{"type":"market_lifecycle_v2","msg":{"event_type":%q,"market_ticker":"KXTERMINAL","result":"yes","settlement_value":"1.0000","ts_ms":1700000003000}}`,
				eventType)))
			got, ok := c.MarketCached("KXTERMINAL")
			if !ok || got.Status != eventType {
				t.Fatalf("%s did not make cached market non-tradable immediately: %+v ok=%v",
					eventType, got, ok)
			}
			if published[0].Status != "active" {
				t.Fatalf("%s mutated the previously published slice in place: %+v",
					eventType, published[0])
			}
		})
	}
}

func TestR159ConcurrentBoardPublishCannotResurrectTerminalLifecycle(t *testing.T) {
	started := time.Now().UTC()
	c := &Client{mktCache: []Market{{
		Ticker: "KXCONCURRENT", Status: "active",
		CloseTime: started.Add(time.Hour).Format(time.RFC3339),
	}}, mktCacheAt: started}

	// This represents a terminal frame arriving after a long multi-page crawl began.
	c.applyMarketLifecycleUpdate(
		"KXCONCURRENT", "settled", "", started.Add(time.Millisecond))

	// The page was fetched before that frame and still claims the market is active.
	out := []Market{{
		Ticker: "KXCONCURRENT", Status: "active",
		CloseTime: started.Add(time.Hour).Format(time.RFC3339),
	}}
	c.mktMu.Lock()
	c.applyRecentMarketUpdatesLocked(out, started)
	c.mktCache = out
	c.mktMu.Unlock()

	got, ok := c.MarketCached("KXCONCURRENT")
	if !ok || got.Status != "settled" {
		t.Fatalf("concurrent board publish resurrected terminal market: %+v ok=%v", got, ok)
	}
}
