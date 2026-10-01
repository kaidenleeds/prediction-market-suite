package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestR153LivePriorityOwnsMaximumSafeRESTReserve(t *testing.T) {
	c := NewClient("https://example.invalid", nil, 30, time.Second)
	if got := c.PriorityReserveTokens(); got != 9 {
		t.Fatalf("normal priority reserve=%v, want 9", got)
	}
	c.SetLivePriority(true)
	if got := c.PriorityReserveTokens(); got != 15 {
		t.Fatalf("LIVE priority reserve=%v, want 15", got)
	}
	c.SetRateLimit(20)
	if got := c.PriorityReserveTokens(); got != 10 {
		t.Fatalf("LIVE reserve did not follow hot rate change: %v", got)
	}
	c.SetLivePriority(false)
	if got := c.PriorityReserveTokens(); got != 6 {
		t.Fatalf("normal reserve was not restored: %v", got)
	}
}

func TestR153OfficialReadAndWriteBudgetsDoNotBlockEachOther(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()
	c := NewClient(ts.URL, nil, 1, 2*time.Second)

	// Exhaust the read bucket. The official write budget is separate, so a mutation must not wait
	// one second for read refill.
	if err := c.limiter.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	var out map[string]any
	if err := c.doWithBody(context.Background(), http.MethodPost, "/write", map[string]any{"x": 1}, &out, false); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("write waited behind exhausted read budget: %v", elapsed)
	}

	// Now exhaust the refilled/reset write bucket and prove a GET still owns independent capacity.
	c.writeLimiter.Reset()
	if err := c.writeLimiter.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.limiter.Reset()
	start = time.Now()
	if err := c.do(context.Background(), http.MethodGet, "/read", &out, false); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("read waited behind exhausted write budget: %v", elapsed)
	}
}
