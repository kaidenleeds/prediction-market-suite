package polymarketus

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type captureMarketWrites struct {
	mu        sync.Mutex
	frames    []map[string]any
	active    atomic.Int64
	maxActive atomic.Int64
}

func (c *captureMarketWrites) write(v any) error {
	n := c.active.Add(1)
	for old := c.maxActive.Load(); n > old && !c.maxActive.CompareAndSwap(old, n); old = c.maxActive.Load() {
	}
	time.Sleep(100 * time.Microsecond) // widen any missing-writer-lock race
	c.mu.Lock()
	c.frames = append(c.frames, v.(map[string]any))
	c.mu.Unlock()
	c.active.Add(-1)
	return nil
}

func (c *captureMarketWrites) reset() {
	c.mu.Lock()
	c.frames = nil
	c.mu.Unlock()
}

func TestIncrementalMarketsSubscriptionsAreSessionDedupedAndSerialized(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	writes := &captureMarketWrites{}
	s := &pusSession{writeJSON: writes.write}
	base := make([]string, 99)
	for i := range base {
		base[i] = fmt.Sprintf("slug-%03d", i)
	}
	if err := ws.subscribeMarkets(s, base); err != nil {
		t.Fatal(err)
	}
	writes.reset()

	// Concurrent duplicate discovery must produce one logical add, never concurrent socket writers.
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = ws.subscribeNewMarkets(s, []string{"slug-099"})
		}()
	}
	wg.Wait()
	if got := writes.maxActive.Load(); got != 1 {
		t.Fatalf("data writers overlapped: max=%d want 1", got)
	}
	if got := s.wideCount(); got != 100 {
		t.Fatalf("session coverage=%d want 100", got)
	}
	writes.mu.Lock()
	frames := append([]map[string]any(nil), writes.frames...)
	writes.mu.Unlock()
	if len(frames) != 4 {
		t.Fatalf("duplicate discovery wrote %d frames want two tail unsubs + exact LITE/TRADE replacements", len(frames))
	}
	wantUnsub := []string{"mdl-0", "trade-0"}
	for i, want := range wantUnsub {
		u, ok := frames[i]["unsubscribe"].(map[string]any)
		if !ok || u["request_id"] != want {
			t.Fatalf("tail unsubscribe %d=%#v want %s", i, frames[i], want)
		}
	}
	for _, f := range frames[len(wantUnsub):] {
		sub := f["subscribe"].(map[string]any)
		if sub["request_id"] != map[int]string{2: "mdl-0", 3: "trade-0"}[sub["subscription_type"].(int)] {
			t.Fatalf("increment did not replace the fixed tail subscription: %#v", sub)
		}
		if got := len(sub["market_slugs"].([]string)); got != 100 {
			t.Fatalf("replacement retained stale tail membership: got %d members want 100", got)
		}
	}

	// Removal from REST and re-addition before this session rotates remains a no-op because the
	// session set, not the previous crawl, is the dedupe authority.
	writes.reset()
	if added, n, err := ws.subscribeNewMarkets(s, []string{"slug-099"}); err != nil || added != 0 || n != 0 {
		t.Fatalf("remove/re-add duplicate: added=%d frames=%d err=%v", added, n, err)
	}
	if len(writes.frames) != 0 {
		t.Fatalf("remove/re-add emitted %d duplicate TRADE/LITE frames", len(writes.frames))
	}
}

func TestSubscribeNewOpenSlugsRejectsUnprovenMarkets(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.SetOpenSlugs([]string{"proven"})
	var got []string
	ws.addMu.Lock()
	ws.addFn = func(slugs []string) (int, bool) {
		got = append(got, slugs...)
		return len(slugs), true
	}
	ws.addMu.Unlock()
	added, ok := ws.SubscribeNewOpenSlugs([]string{"PROVEN", "unproven", "proven"})
	if !ok || added != 1 || len(got) != 1 || got[0] != "proven" {
		t.Fatalf("added=%d ok=%v slugs=%v; want only proven", added, ok, got)
	}
}
