package kalshi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// ── R85 wedge regression: the board singleflight must release `busy` after a WEDGED/FAILED build ─
// The 2026-07-05 suspicion was the R75 stale-while-revalidate busy flag latching forever after one
// wedged pull. refreshBoard clears mktRefreshing in a defer; this test proves it end to end against
// a venue that fails every request, and that a LATER call can kick a FRESH refresh (the flag is
// genuinely released, not just unobserved).
func TestBoardSingleflightReleasedAfterWedgedBuild(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, `{"error":"venue down"}`, http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, nil, 1000, 2*time.Second) // huge rate budget: the limiter never gates this test
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if mkts, err := c.GetTopLiquidMarkets(ctx); err == nil && len(mkts) > 0 {
		t.Fatalf("failing venue produced %d rows", len(mkts))
	}

	// The failed background build MUST release the singleflight busy flag.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mktMu.Lock()
		busy := c.mktRefreshing
		c.mktMu.Unlock()
		if !busy {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mktRefreshing latched busy after a failed build — singleflight leak")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Force staleness (the failed build may have stamped an empty cache) and prove a NEW
	// refresh actually launches: fresh venue traffic must appear.
	c.mktMu.Lock()
	c.mktCacheAt = time.Time{}
	c.mktMu.Unlock()
	h1 := hits.Load()
	if _, busy := c.TopLiquidCached(); !busy {
		t.Fatal("stale board did not kick a new background refresh — busy flag/latch broken")
	}
	waitFor := time.Now().Add(5 * time.Second)
	for hits.Load() == h1 && time.Now().Before(waitFor) {
		time.Sleep(10 * time.Millisecond)
	}
	if hits.Load() == h1 {
		t.Fatal("no new venue traffic after the re-kick — refresher latched despite busy=false")
	}
}

// ── R85 ROOT CAUSE regression: WARM-cache board reads must not self-deadlock ─────────────────────
// GetTopLiquidMarkets/TopLiquidCached held mktMu while evaluating c.refreshEvery() — which locks
// mktMu again (non-reentrant ⇒ parked forever HOLDING the lock). The `cached != nil` short-circuit
// hid it from every cold-board test and every cold-board production run: the deadlock armed on the
// FIRST WARM READ ~4s after the first successful full pull (the 2026-07-05 12h universal wedge —
// WS green, every REST/board path parked, ctx deadlines powerless against a mutex wait). This test
// seeds a WARM cache and requires both accessors + a config setter to return promptly.
func TestWarmBoardReadDoesNotSelfDeadlock(t *testing.T) {
	c := NewClient("http://127.0.0.1:0", nil, 100, time.Second)
	c.mktMu.Lock()
	c.mktCache = []Market{{Ticker: "KXTEST-1"}} // WARM cache — the arm condition
	c.mktCacheAt = time.Now()                   // fresh ⇒ no background pull is kicked
	c.mktMu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if m, _ := c.TopLiquidCached(); len(m) != 1 {
			t.Error("warm TopLiquidCached did not serve the cache")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if m, err := c.GetTopLiquidMarkets(ctx); err != nil || len(m) != 1 {
			t.Errorf("warm GetTopLiquidMarkets: %v (%d rows)", err, len(m))
		}
		c.SetMarketsRefreshS(5) // the config setters share mktMu — must stay reachable too
		if got := c.refreshEvery(); got != 5*time.Second {
			t.Errorf("refreshEvery after set: %v", got)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("warm board read DEADLOCKED (mktMu re-entry regression — the 2026-07-05 wedge)")
	}
}

// TopLiquidCached is the request-path accessor (/api/markets): it must NEVER block — an ice-cold
// board with an unreachable venue returns instantly with (nil, refreshing=true).
func TestTopLiquidCachedNeverBlocks(t *testing.T) {
	blocked := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blocked // wedge every request until the test ends
	}))
	defer srv.Close()
	defer close(blocked)

	c := NewClient(srv.URL, nil, 1000, 30*time.Second)
	start := time.Now()
	mkts, refreshing := c.TopLiquidCached()
	if el := time.Since(start); el > 250*time.Millisecond {
		t.Fatalf("TopLiquidCached blocked %v on a cold board (must be instant)", el)
	}
	if mkts != nil {
		t.Fatalf("cold board returned %d rows", len(mkts))
	}
	if !refreshing {
		t.Fatal("cold call must report the background refresh it kicked")
	}
}
