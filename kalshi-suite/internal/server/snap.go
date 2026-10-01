// snap.go — serve-stale snapshot caching + gzip + ETag for the heavy GET endpoints (audit §4).
//
// Why tabs took ~40s: Backtest/Edge/Curves/labs each ran UNCACHED full signal_log / paper_fills
// scans per request (45s internal timeouts — that was the literal 40s), the browser polled ~20
// endpoints on a 700ms loop over HTTP/1.1's 6-socket cap, and nothing was gzipped or 304able.
//
// The pattern here generalizes what the Live tab already did well (serve cached bytes instantly,
// refresh in the background when stale): NO handler should touch network/DB on the request path.
//   - fresh   → serve cached bytes (with ETag/304 + gzip)
//   - stale   → serve cached bytes NOW, rebuild once in the background (singleflight)
//   - cold    → build synchronously once (first open after boot)
package server

import (
	"compress/gzip"
	"context"
	"fmt"
	"hash/fnv"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// snapCache is one endpoint's cached response body.
type snapCache struct {
	mu   sync.Mutex
	b    []byte
	at   time.Time
	busy bool
}

func (sn *snapCache) store(b []byte) {
	sn.mu.Lock()
	sn.b, sn.at, sn.busy = b, time.Now(), false
	sn.mu.Unlock()
}

// ageAt returns when the snapshot was last built (zero = never) — readiness dots + prewarm checks.
func (sn *snapCache) ageAt() time.Time {
	sn.mu.Lock()
	defer sn.mu.Unlock()
	return sn.at
}

// snapBust — R51 speed pass (operator: "buttons take time to process, feeds are slow"). The single
// biggest perceived-latency bug wasn't compute at all: a mutating POST changed state, the dashboard
// re-fetched, and serveSnap dutifully served the PRE-CLICK bytes until the 8–20s TTL lapsed. The
// click always worked; the UI just kept showing yesterday. This marks the hot panels stale and
// rebuilds them NOW — primeSnap's completion sseNotify then repaints every open dashboard in well
// under a second, no polling involved. Debounced 1s with a trailing rebuild so an auto-pilot burst
// (2 orders/pass) costs one rebuild, and the LAST mutation of a burst still lands (a plain throttle
// would re-create the original bug for exactly the click the user is watching).
func (s *Server) snapBust() {
	if !s.bustGate(s.snapBust) {
		return
	}
	s.liveInvalidate() // live tab has its own cache machinery (orders/positions/proposals)
	for _, j := range []struct {
		sn    *snapCache
		name  string
		build func(ctx context.Context) []byte
	}{
		{&s.snapPaper, "paper", s.buildPaper},
		{&s.snapML, "ml", s.buildML},
		{&s.snapNetworth, "networth", s.buildNetworth},
		{&s.snapStats, "stats", s.buildStats}, // R98: reset must not serve pre-reset stats bytes
	} {
		j.sn.mu.Lock()
		j.sn.at = time.Time{} // stale NOW — a poll that races the rebuild serves old bytes once, then gets pushed
		j.sn.mu.Unlock()
		go s.primeSnap(j.sn, 0, j.name, j.build) // ttl 0 = rebuild immediately; sseNotify on completion
	}
}

// bustGate is snapBust's debounce core, extracted for unit testing (R70, audit §d: snapBust was
// UNTESTED). Returns true when the caller may rebuild NOW. Within 1s of the last rebuild it
// returns false and — once per burst — schedules `trailing` for when the window lapses, so the
// LAST mutation of a burst still lands (a plain throttle would re-create the stale-click bug).
func (s *Server) bustGate(trailing func()) bool {
	s.bustMu.Lock()
	if since := time.Since(s.bustLast); since < time.Second {
		if !s.bustQueued {
			s.bustQueued = true
			time.AfterFunc(time.Second-since, func() {
				s.bustMu.Lock()
				s.bustQueued = false
				s.bustMu.Unlock()
				trailing()
			})
		}
		s.bustMu.Unlock()
		return false
	}
	s.bustLast = time.Now()
	s.bustMu.Unlock()
	return true
}

// bustOnMutate wraps the mux: any successful mutating /api request busts the hot snapshot caches.
// Central on purpose — 20+ POST handlers exist and every one of them changes something a cached
// panel shows; hand-wiring each would guarantee the next handler forgets.
func (s *Server) bustOnMutate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions || !strings.HasPrefix(r.URL.Path, "/api/") {
			next.ServeHTTP(w, r)
			return
		}
		bw := &bustWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(bw, r)
		if bw.code < 400 { // only real state changes; auth failures / validation refusals don't churn caches
			s.snapBust()
		}
	})
}

type bustWriter struct {
	http.ResponseWriter
	code int
}

func (b *bustWriter) WriteHeader(c int) { b.code = c; b.ResponseWriter.WriteHeader(c) }

// Flush keeps SSE/streaming working through the wrapper (http.Flusher would otherwise be hidden).
func (b *bustWriter) Flush() {
	if f, ok := b.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// prime builds a snapshot in the background if it's stale/empty (singleflight via busy) — the boot
// prewarmer and the keep-warm ticker use this so tabs are ALWAYS served from cache (R10).
func (s *Server) primeSnap(sn *snapCache, ttl time.Duration, name string, build func(ctx context.Context) []byte) {
	sn.mu.Lock()
	if (sn.b != nil && time.Since(sn.at) < ttl) || sn.busy {
		sn.mu.Unlock()
		return
	}
	sn.busy = true
	sn.mu.Unlock()
	defer func() { // R69 (audit P1): DEFERRED release — a panicking builder must never latch busy (tab would serve stale/building forever)
		sn.mu.Lock()
		sn.busy = false
		sn.mu.Unlock()
	}()
	defer s.recoverGuard("prime:" + name) // R89 (bug 72): the runGuarded defer form never recovered
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	nb := build(ctx)
	sn.mu.Lock()
	if nb != nil {
		sn.b, sn.at = nb, time.Now()
	}
	sn.mu.Unlock()
	if nb != nil {
		s.sseNotify(name)
	}
}

// PrewarmSnapshots (R10 — operator: "autoload all tabs in background until ready, in parallel")
// builds EVERY tab's snapshot concurrently at boot, then keeps the two hottest (Portfolio + ML)
// perpetually fresh so opening them is always instant. Heavy analytics rebuild on their own TTLs
// once seeded. Call once, in a goroutine, after the feeds start.
func (s *Server) PrewarmSnapshots(ctx context.Context) {
	type job struct {
		sn    *snapCache
		ttl   time.Duration
		name  string
		build func(ctx context.Context) []byte
	}
	// EDGE INSTANT LOAD (operator: "edge still no loading unless I have the tab open for 8 mins"):
	// buildEdge full-scans 500k+ signals — a cold NVMe/SQLite couldn't finish inside its old 45s
	// timeout, so every attempt died until the OS page cache warmed (~8 min of retries). Two-part
	// fix: (1) the LAST GOOD edge report persists to disk and is served instantly at boot (stale is
	// fine — it's history analytics), (2) the rebuild gets a realistic timeout (buildEdge) and a
	// 10-min TTL instead of 60s so it isn't perpetually rebuilding.
	if b, err := os.ReadFile(filepath.Join(s.cfg().DataDir, "edge_snapshot.json")); err == nil && len(b) > 2 {
		if fi, e := os.Stat(filepath.Join(s.cfg().DataDir, "edge_snapshot.json")); e == nil && time.Since(fi.ModTime()) < 48*time.Hour {
			s.snapEdge.mu.Lock()
			s.snapEdge.b, s.snapEdge.at = b, fi.ModTime()
			s.snapEdge.mu.Unlock()
		}
	}
	// COMBO/HORIZON COLD START (operator: "Kalshi still takes ~3 min to warm"): bulk-fetch the ML's
	// Kalshi tickers in ~3 batch calls (100/call) so close times + prices are cached within seconds
	// of boot — parlay legs and proposals no longer trickle in via budgeted singles.
	go s.runGuarded("prewarm:kalshi-bulk", func() { s.bulkWarmKalshiMeta(ctx) })
	// R18 probe: sample combo-collection quoter counts every 10 min (the "when do makers come
	// online" series; audit category "rfqpulse").
	go s.runGuarded("rfq-quoter-sampler", func() { s.quoterSampler(ctx) })
	jobs := []job{
		{&s.snapPaper, 8 * time.Second, "paper", s.buildPaper},
		{&s.snapML, 15 * time.Second, "ml", s.buildML},
		{&s.snapNetworth, 20 * time.Second, "networth", s.buildNetworth},
	}
	for _, j := range jobs {
		s.primeSnap(j.sn, j.ttl, j.name, j.build)
	}
	// Second wave: endpoints whose builders are inline closures (stats/backtest/curves/stoplab +
	// the default replay grid). One recorder hit each — the serve-stale cold path (R4) kicks a
	// DETACHED background build and returns immediately, so this just lights the fuses in parallel.
	if false { // R140: legacy full-history analytics are on-demand; never fan them out during cold start.
		for _, p := range []struct {
			path string
			h    func(http.ResponseWriter, *http.Request)
		}{
			{"/api/stats", s.handleStats},
			{"/api/backtest", s.handleBacktest},
			{"/api/curves", s.handleCurves},
			// R63 3d: prewarm EVERY curves venue variant too (serveSnapKeyed keys on ?platform=) so
			// clicking a venue filter never triggers a blocking cold build. Each recorder hit returns
			// 202 immediately and kicks ONE detached background build (singleflight via busy) — the
			// only shared resource is the SQLite scan, which these already contend on today.
			{"/api/curves?platform=kalshi", s.handleCurves},
			{"/api/curves?platform=polyus", s.handleCurves},
			{"/api/curves?platform=polymarket", s.handleCurves},
			{"/api/stoplab", s.handleStopLab},
			{"/api/replay", s.handleReplay},
			{"/api/parlay/backtest", s.handleParlayBacktest},
		} {
			go func(path string, h func(http.ResponseWriter, *http.Request)) {
				defer s.recoverGuard("prewarm:" + path) // R89 (bug 72)
				rec := httptest.NewRecorder()
				req, _ := http.NewRequest(http.MethodGet, path, nil)
				h(rec, req)
			}(p.path, p.h)
		}
	}
	// keep-warm: Portfolio + ML refresh continuously so their 8s/15s TTLs never lapse for a user;
	// the LIVE snapshot (real-money balance/orders/positions) stays warm on every other tick — a
	// few portfolio API reads per ~14s, trivial inside the Advanced-tier budget.
	//
	// INSTANT PICKS (operator): a 1s watcher stats ml_predictions.json — the MOMENT the sidecar
	// finishes a scoring cycle, the live snapshot is invalidated + rebuilt and the ML snapshot
	// re-primed, then SSE pushes "live"+"ml" so open dashboards repaint within ~1s of the model
	// finishing. No more waiting out a poll interval to see fresh picks.
	primeLive := func() {
		defer s.recoverGuard("prewarm:/api/live") // R89 (bug 72)
		rec := httptest.NewRecorder()
		req, _ := http.NewRequest(http.MethodGet, "/api/live", nil)
		s.handleLive(rec, req) // self-caching handler: this just keeps its snapshot fresh
	}
	go func() {
		var lastMod time.Time
		w := time.NewTicker(1 * time.Second)
		defer w.Stop()
		predPath := filepath.Join(s.cfg().DataDir, "ml_predictions.json")
		for {
			select {
			case <-ctx.Done():
				return
			case <-w.C:
				fi, err := os.Stat(predPath)
				if err != nil {
					continue
				}
				if !fi.ModTime().Equal(lastMod) {
					first := lastMod.IsZero()
					lastMod = fi.ModTime()
					if first {
						continue // don't storm at boot; the seed pass covers it
					}
					s.liveInvalidate()
					go primeLive()
					go s.primeSnap(&s.snapML, 0, "ml", s.buildML) // ttl 0 = rebuild NOW
					s.sseNotify("live")
				}
			}
		}
	}()
	t := time.NewTicker(7 * time.Second)
	defer t.Stop()
	tick := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick++
			go s.primeSnap(&s.snapPaper, 8*time.Second, "paper", s.buildPaper)
			go s.primeSnap(&s.snapML, 15*time.Second, "ml", s.buildML)
			if tick%2 == 0 {
				go primeLive()
			}
			if tick%3 == 0 {
				// R106 (auditor r34, bug 263 second half): networth was seeded once at boot and
				// ABSENT from this keep-warm ticker — the first hit after any idle gap served a
				// boot-era fossil (empty live_pnl_hist, pre-boot paper_net; five documented
				// repros). Kept warm every ~21s, the builder is cheap (one cached fills read).
				go s.primeSnap(&s.snapNetworth, 18*time.Second, "networth", s.buildNetworth)
			}
		}
	}
}

// writeCachedJSON writes JSON bytes with ETag/304 + gzip (audit §4: the cached byte payloads were
// re-sent whole, uncompressed, on every poll).
func writeCachedJSON(w http.ResponseWriter, r *http.Request, b []byte) {
	h := fnv.New64a()
	_, _ = h.Write(b)
	etag := fmt.Sprintf(`"%x"`, h.Sum64())
	if r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", etag)
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && len(b) > 1024 {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		_, _ = gz.Write(b)
		_ = gz.Close()
		return
	}
	_, _ = w.Write(b)
}

// serveSnapKeyed is serveSnap for endpoints whose response depends on query params (replay's
// parameter grid, curves' platform filter): one snapCache per (name, key), bounded.
func (s *Server) serveSnapKeyed(w http.ResponseWriter, r *http.Request, key, name string, ttl time.Duration, build func(ctx context.Context) []byte) {
	s.replayMu.Lock()
	if s.replayCache == nil {
		s.replayCache = map[string]*snapCache{}
	}
	full := name + "|" + key
	sn := s.replayCache[full]
	if sn == nil {
		if len(s.replayCache) > 24 { // bound the param-hash fan-out; params churn slowly
			s.replayCache = map[string]*snapCache{}
		}
		sn = &snapCache{}
		s.replayCache[full] = sn
	}
	s.replayMu.Unlock()
	s.serveSnap(w, r, sn, ttl, name, build)
}

// serveSnap implements the serve-stale + singleflight pattern for one endpoint. build must return
// the full JSON body (nil = failure, in which case any stale bytes keep serving).
func (s *Server) serveSnap(w http.ResponseWriter, r *http.Request, sn *snapCache, ttl time.Duration, name string, build func(ctx context.Context) []byte) {
	sn.mu.Lock()
	b, age, busy := sn.b, time.Since(sn.at), sn.busy
	if b != nil && age < ttl {
		sn.mu.Unlock()
		w.Header().Set("X-Snapshot-Age-Ms", fmt.Sprintf("%d", age.Milliseconds())) // R99 bug 204
		writeCachedJSON(w, r, b)
		return
	}
	// R106 (auditor r34, bug 263): serve-stale had NO age ceiling — a snapshot idle for an hour
	// (or since boot) kept serving as "instant" bytes (the networth fossil, five repros). A
	// FOSSIL (older than max(2min, 6×ttl) — proportional so heavy lazy tabs keep their R98
	// stale-instant UX) now takes the cold-start path (202 building + immediate rebuild)
	// instead of serving the relic.
	fossil := 6 * ttl
	if fossil < 2*time.Minute {
		fossil = 2 * time.Minute
	}
	if b != nil && age > fossil {
		b = nil
	}
	if b != nil {
		if !busy {
			sn.busy = true
			go func() {
				defer func() { // R69 (audit P1): deferred — a builder panic must not latch busy
					sn.mu.Lock()
					sn.busy = false
					sn.mu.Unlock()
				}()
				defer s.recoverGuard("snap:" + name) // R89 (bug 72)
				ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
				defer cancel()
				nb := build(ctx)
				sn.mu.Lock()
				if nb != nil {
					sn.b, sn.at = nb, time.Now()
				}
				sn.mu.Unlock()
				s.sseNotify(name) // push the refresh to connected dashboards (audit §4: SSE)
			}()
		}
		sn.mu.Unlock()
		// R99 bug 204 (auditor r25): stale serves used to be silent — the age header makes
		// "how old is this snapshot" readable on every response (dashboards, curl, the auditor)
		// without touching the cached body bytes.
		w.Header().Set("X-Snapshot-Age-Ms", fmt.Sprintf("%d", age.Milliseconds()))
		writeCachedJSON(w, r, b) // STALE-BUT-INSTANT: the background refresh lands for the next poll
		return
	}
	// COLD START (R4 fix): the old path built synchronously on r.Context() — when the build outlived
	// the browser's patience the fetch aborted, WHICH CANCELLED THE BUILD, nothing was cached, and
	// every retry repeated the doomed cycle (the "Edge tab never loads" livelock). Now: kick ONE
	// detached background build (singleflight via busy) and answer 202 {"building":true} immediately;
	// the dashboard shows "building…" and re-polls until bytes exist.
	if !busy {
		sn.busy = true
		go func() {
			defer func() { // R69 (audit P1): deferred — a builder panic must not latch busy
				sn.mu.Lock()
				sn.busy = false
				sn.mu.Unlock()
			}()
			defer s.runGuarded("snap:"+name, func() {})
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			nb := build(ctx)
			sn.mu.Lock()
			if nb != nil {
				sn.b, sn.at = nb, time.Now()
			}
			sn.mu.Unlock()
			if nb != nil {
				s.sseNotify(name)
			}
		}()
	}
	sn.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"building":true}`))
}
