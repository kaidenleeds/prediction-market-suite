package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func waitSystemsRegimeBuild(t *testing.T, c *systemsRegimeCache) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		building := c.building
		c.mu.Unlock()
		if !building {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("systems regime cache build did not finish")
}

func TestSystemsRegimeColdBuildIsBoundedAndSingleflight(t *testing.T) {
	c := &systemsRegimeCache{}
	release := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	var calls atomic.Int32
	build := func(ctx context.Context) (map[string]any, any) {
		calls.Add(1)
		startOnce.Do(func() { close(started) })
		select {
		case <-release:
			return map[string]any{"generated_at": "fresh", "research_only": true, "rows": []any{}}, nil
		case <-ctx.Done():
			return nil, nil
		}
	}

	const clients = 6
	type result struct {
		code    int
		elapsed time.Duration
	}
	results := make(chan result, clients)
	for range clients {
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/api/systems-regimes", nil)
			rr := httptest.NewRecorder()
			began := time.Now()
			serveSystemsRegimeCache(rr, req, c, build)
			results <- result{code: rr.Code, elapsed: time.Since(began)}
		}()
	}
	<-started
	for range clients {
		r := <-results
		if r.code != http.StatusServiceUnavailable {
			t.Fatalf("cold status=%d, want explicit warming 503", r.code)
		}
		if r.elapsed > systemsRegimeColdWait+500*time.Millisecond {
			t.Fatalf("cold request inherited slow build: %s", r.elapsed)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("concurrent cold requests started %d builders, want 1", got)
	}

	close(release)
	waitSystemsRegimeBuild(t, c)
	req := httptest.NewRequest(http.MethodGet, "/api/systems-regimes", nil)
	rr := httptest.NewRecorder()
	serveSystemsRegimeCache(rr, req, c, build)
	if rr.Code != http.StatusOK || rr.Header().Get("X-Systems-Regime-Cache") != "fresh" {
		t.Fatalf("finished cache status=%d cache=%q body=%s", rr.Code, rr.Header().Get("X-Systems-Regime-Cache"), rr.Body.String())
	}
}

func TestSystemsRegimeExpiredCacheServesStaleDuringRefresh(t *testing.T) {
	old := map[string]any{"generated_at": "old", "research_only": true, "rows": []any{map[string]any{"system": "old"}}}
	c := &systemsRegimeCache{payload: old, at: time.Now().Add(-2 * systemsRegimeCacheTTL)}
	release := make(chan struct{})
	started := make(chan struct{})
	var startOnce sync.Once
	var calls atomic.Int32
	build := func(ctx context.Context) (map[string]any, any) {
		calls.Add(1)
		startOnce.Do(func() { close(started) })
		select {
		case <-release:
			return map[string]any{"generated_at": "new", "research_only": true, "rows": []any{map[string]any{"system": "new"}}}, nil
		case <-ctx.Done():
			return nil, nil
		}
	}

	request := func() (*httptest.ResponseRecorder, time.Duration) {
		req := httptest.NewRequest(http.MethodGet, "/api/systems-regimes", nil)
		rr := httptest.NewRecorder()
		began := time.Now()
		serveSystemsRegimeCache(rr, req, c, build)
		return rr, time.Since(began)
	}
	rr, elapsed := request()
	if rr.Code != http.StatusOK || rr.Header().Get("X-Systems-Regime-Cache") != "stale" {
		t.Fatalf("stale status=%d cache=%q body=%s", rr.Code, rr.Header().Get("X-Systems-Regime-Cache"), rr.Body.String())
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("stale response waited for refresh: %s", elapsed)
	}
	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil || body["generated_at"] != "old" {
		t.Fatalf("last-good payload changed during refresh: err=%v body=%s", err, rr.Body.String())
	}
	<-started
	rr, _ = request()
	if rr.Code != http.StatusOK || rr.Header().Get("X-Systems-Regime-Cache") != "stale" || calls.Load() != 1 {
		t.Fatalf("second stale request was not coalesced: status=%d cache=%q calls=%d", rr.Code, rr.Header().Get("X-Systems-Regime-Cache"), calls.Load())
	}

	close(release)
	waitSystemsRegimeBuild(t, c)
	rr, _ = request()
	if rr.Code != http.StatusOK || rr.Header().Get("X-Systems-Regime-Cache") != "fresh" {
		t.Fatalf("refreshed status=%d cache=%q body=%s", rr.Code, rr.Header().Get("X-Systems-Regime-Cache"), rr.Body.String())
	}
	body = nil
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil || body["generated_at"] != "new" {
		t.Fatalf("refresh did not publish atomically: err=%v body=%s", err, rr.Body.String())
	}
}

func TestNativeMakerObservationSettlementLookupStaysPlatformScoped(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	id, err := s.store.InsertMakerAttempt(ctx, "kalshi", "KXSETTLE", "YES", "route", .40, 1, 5, 0, nil, 1, "", "bookws")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SetMakerStrategy(ctx, id, "edge", false); err != nil {
		t.Fatal(err)
	}
	if err := s.store.MarkMakerFilledWithFee(ctx, id, .01, "kalshi:receipt"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=1 WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	for _, sig := range []storage.Signal{
		{Platform: "kalshi", Ticker: "KXSETTLE", Side: "YES", SignalType: "settle-a", EntryPrice: .4},
		{Platform: "kalshi", Ticker: "KXSETTLE", Side: "YES", SignalType: "settle-b", EntryPrice: .4},
		{Platform: "polyus", Ticker: "KXSETTLE", Side: "YES", SignalType: "settle-c", EntryPrice: .4},
	} {
		if err := s.store.InsertSignal(ctx, sig); err != nil {
			t.Fatal(err)
		}
	}
	for typ, resolvedAt := range map[string]string{
		"settle-a": "2026-07-10T01:00:00Z",
		"settle-b": "2026-07-10T02:00:00Z",
		"settle-c": "2026-07-10T03:00:00Z",
	} {
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE signal_log SET resolved=1,resolved_at=? WHERE signal_type=?`, resolvedAt, typ); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := s.store.NativeMakerObservations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Ticker == "KXSETTLE" {
			if row.ClosedTS != "2026-07-10T02:00:00Z" {
				t.Fatalf("settlement lookup crossed venue or missed latest matching receipt: %+v", row)
			}
			return
		}
	}
	t.Fatal("maker observation missing")
}
