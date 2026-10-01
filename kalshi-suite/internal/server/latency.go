package server

// latency.go — R107 Part 5 (operator: connection dropped to 2Mbps up / 100 down — "build a
// latency health system"). Continuous measurement of everything between us and the venues:
//
//   - venue REST RTT: active 30s probes against each venue's cheapest PUBLIC endpoint (no key
//     material touches this file) + passive observers on every real client request (the
//     kalshi/polymarketus/polymarket do() hooks), so the panel shows both the clean-room number
//     and what our actual traffic experiences.
//   - WS health: kalshi live-ticker count + PUS markets-WS fresh count + private-WS liveness —
//     the honest freshness surfaces the suite already maintains.
//   - DB: a canary query timed per probe tick (p50/p95 over the hour).
//   - briefing render + live order round-trip: timed at their call sites (latNote hooks).
//   - clock skew: the Date header of the PUS health probe (±1s resolution — good enough to
//     catch the multi-second drifts that break signed-timestamp auth).
//   - upload budget: request counts/min × ~1.2KB request overhead vs the operator's 2Mbps —
//     the "is the hotspot hurting us" answer, stated plainly in the payload.
//
// UI: a header chip (via /api/latency polled by the dashboard) + a panel with 1h sparklines.
// Alerts: one telegram WARN per class per 6h when a threshold is breached for 3 consecutive
// samples; recovery logs Info and re-arms. Thresholds config-tunable (latency_warn_*).

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

const latRingCap = 120 // 1h at 30s samples

type latPoint struct {
	T  int64   `json:"t"`
	MS float64 `json:"ms"`
}

type latRing struct {
	buf  [latRingCap]latPoint
	n, i int
}

func (r *latRing) add(ms float64) {
	r.buf[r.i] = latPoint{T: time.Now().Unix(), MS: ms}
	r.i = (r.i + 1) % latRingCap
	if r.n < latRingCap {
		r.n++
	}
}

// snap returns chronological points (oldest first).
func (r *latRing) snap() []latPoint {
	out := make([]latPoint, 0, r.n)
	start := (r.i - r.n + latRingCap) % latRingCap
	for k := 0; k < r.n; k++ {
		out = append(out, r.buf[(start+k)%latRingCap])
	}
	return out
}

func latPct(points []latPoint, p float64) float64 {
	if len(points) == 0 {
		return 0
	}
	v := make([]float64, len(points))
	for i, pt := range points {
		v[i] = pt.MS
	}
	sort.Float64s(v)
	idx := int(p * float64(len(v)-1))
	return v[idx]
}

type latMon struct {
	mu     sync.Mutex
	rings  map[string]*latRing
	counts map[string]*int64 // passive request counters (per venue, monotonic)
	breach map[string]int    // consecutive-sample breach counts per class
	warned map[string]time.Time
	// R108 WS staleness watchdog state: consecutive stale samples + last forced-reconnect time
	// per WS class (poly_tape / kalshi_ws / polyus_ws).
	staleN  map[string]int
	kickAt  map[string]time.Time
	kickTld map[string]time.Time // last kick telegram per class (warn-once/6h; audit rows every kick)
	// R112: recovery-note-sent flag per warn window. The pre-R112 bug: recovery DELETED the
	// warned latch, so an oscillating class (slow-slow-slow-fast-…) re-warned every few
	// minutes despite the "won't repeat for 6h" promise. The latch now holds for the full 6h
	// regardless of recovery, is persisted to kv (survives restarts), and each warn window
	// sends at most one recovery note.
	recovered map[string]bool
	chip      atomic.Value // string: green|yellow|red
	why       atomic.Value // string: reasons
	skewMS    atomic.Value // float64
	lastAt    atomic.Value // time.Time of last probe tick
	flight    atomic.Bool
	network   networkPathReceipt // guarded by mu; diagnostic only, never trade-authorizing
}

func newLatMon() *latMon {
	m := &latMon{rings: map[string]*latRing{}, counts: map[string]*int64{}, breach: map[string]int{}, warned: map[string]time.Time{},
		staleN: map[string]int{}, kickAt: map[string]time.Time{}, kickTld: map[string]time.Time{}, recovered: map[string]bool{}}
	m.chip.Store("green")
	m.why.Store("")
	m.skewMS.Store(float64(0))
	m.lastAt.Store(time.Time{})
	return m
}

func (m *latMon) note(key string, ms float64) {
	m.mu.Lock()
	r := m.rings[key]
	if r == nil {
		r = &latRing{}
		m.rings[key] = r
	}
	r.add(ms)
	m.mu.Unlock()
}

func (m *latMon) bump(venue string) {
	m.mu.Lock()
	c := m.counts[venue]
	if c == nil {
		c = new(int64)
		m.counts[venue] = c
	}
	m.mu.Unlock()
	atomic.AddInt64(c, 1)
}

// latMonHooks wires the passive per-request observers into the venue clients (package vars —
// set once at server construction; nil-safe in the clients).
func (s *Server) latMonHooks() {
	m := s.latmon
	kalshi.RESTObs = func(d time.Duration) { m.note("kalshi_rest_passive", float64(d.Milliseconds())); m.bump("kalshi") }
	polymarketus.RESTObs = func(d time.Duration) { m.note("polyus_rest_passive", float64(d.Milliseconds())); m.bump("polyus") }
	polymarket.RESTObs = func(d time.Duration) { m.note("polyint_rest_passive", float64(d.Milliseconds())); m.bump("polymarket") }
}

// latNote — call-site hook for one-off timings (briefing render, live order RTT).
func (s *Server) latNote(key string, d time.Duration) {
	if s.latmon != nil {
		s.latmon.note(key, float64(d.Milliseconds()))
	}
}

// latencyTick — called from the MonitorPaper loop every sweep; self-throttles to 30s and runs
// the probe batch on a guarded goroutine (never blocks the paper loop; single-flight).
func (s *Server) latencyTick() {
	m := s.latmon
	if m == nil {
		return
	}
	if la, _ := m.lastAt.Load().(time.Time); time.Since(la) < 30*time.Second {
		return
	}
	if !m.flight.CompareAndSwap(false, true) {
		return
	}
	m.lastAt.Store(time.Now())
	go s.runGuarded("latency-probe", func() {
		defer m.flight.Store(false)
		s.latencyProbe()
	})
}

// latencyProbe runs one 30s sample: three public venue GETs (timed), the DB canary, WS
// freshness gauges, clock skew, then threshold evaluation.
func (s *Server) latencyProbe() {
	m := s.latmon
	client := &http.Client{Timeout: 8 * time.Second}
	probe := func(key, url string) (hdrDate time.Time) {
		t0 := time.Now()
		resp, err := client.Get(url)
		d := time.Since(t0)
		if err != nil {
			m.note(key, 8000) // timeout/unreachable = ceiling sample (visible, counts toward breach)
			return time.Time{}
		}
		defer resp.Body.Close()
		m.note(key, float64(d.Milliseconds()))
		if ds := resp.Header.Get("Date"); ds != "" {
			if t, e := http.ParseTime(ds); e == nil {
				return t
			}
		}
		return time.Time{}
	}
	// Public, unauthenticated, cheapest-known endpoints (verified: no key material involved).
	probe("kalshi_rest", "https://api.elections.kalshi.com/trade-api/v2/exchange/status")
	if dt := probe("polyus_rest", "https://api.polymarket.us/v1/health"); !dt.IsZero() {
		// clock skew: venue Date vs local, ±1s header resolution — catches auth-breaking drift
		m.skewMS.Store(float64(time.Since(dt).Milliseconds()) - 500) // mid-bucket correction
	}
	probe("polyint_rest", "https://gamma-api.polymarket.com/markets?limit=1")
	// Reuse this existing monitor: one local link sample + one tiny independent request per minute.
	// Both are hard-bounded and diagnostic-only.
	if now := time.Now(); m.networkSampleDue(now) {
		link := queryWLANLink(context.Background(), wlanCommandTimeout, defaultWLANCommand)
		internet := probeIndependentInternet(context.Background())
		thr := s.cfg().Auto.LatWarnRESTMs
		if thr <= 0 {
			thr = 2000
		}
		m.recordNetworkPath(now, link, internet, thr)
	}
	// DB canary (indexed aggregate on an ~empty table — measures lock/IO latency, not scan cost).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t0 := time.Now()
	_, _ = s.store.LiveCombosRealized(ctx)
	cancel()
	m.note("db_ms", float64(time.Since(t0).Milliseconds()))
	// WS freshness gauges (counts, not latencies — appended to their own rings for sparklines).
	if s.polyUSWS != nil {
		m.note("polyus_ws_fresh", float64(s.polyUSWS.ExecutableCount()))
	}
	if s.kal != nil {
		m.note("kalshi_ws_tickers", float64(s.kal.TickerCount()))
	}
	if s.poly != nil {
		_, _, fresh, _ := s.poly.CLOBStats()
		m.note("poly_clob_ws_fresh", float64(fresh))
	}
	privOK := 0.0
	if s.pusPriv != nil && s.pusPriv.Healthy() {
		privOK = 1
	}
	m.note("pus_priv_ok", privOK)
	s.latencyEval()
	s.wsStaleSweep()
}

// ── R108 WS STALENESS AUTO-RECONNECT ─────────────────────────────────────────────────────────
// R107 found the poly-int trade tape wedged on a degraded link: the socket kept ponging (so the
// read-deadline keepalive never fired) while DATA age grew second-for-second and REST fallbacks
// stormed — only a full restart healed it. This watchdog measures DATA age per WS class every
// 30s probe; sustained staleness (3 consecutive samples past lat_warn_ws_age_s) force-closes the
// socket via the client's kick hook, and the existing redial/standby-promote+resub machinery
// heals it. One audit row per kick; one plain telegram per class per 6h; 10-min kick cooldown so
// a venue outage (redial can't help) doesn't turn into a socket-kill loop.

const wsKickCooldown = 10 * time.Minute

// wsStaleCheck runs ONE class's decision: stale sample counting, cooldown, kick. Returns whether
// a kick fired. Pure state machine over latMon (unit-tested with a fake kick in r108).
func (s *Server) wsStaleCheck(class string, stale bool, kick func() bool) bool {
	m := s.latmon
	m.mu.Lock()
	if !stale {
		m.staleN[class] = 0
		m.mu.Unlock()
		return false
	}
	m.staleN[class]++
	n := m.staleN[class]
	last := m.kickAt[class]
	fire := n >= 3 && time.Since(last) >= wsKickCooldown
	if fire {
		m.kickAt[class] = time.Now()
		m.staleN[class] = 0 // restart the count against the fresh socket
	}
	m.mu.Unlock()
	if !fire {
		return false
	}
	if !kick() {
		s.log.Warn("ws staleness: kick requested but no live socket (redial loop already working)", "class", class)
		return false
	}
	s.log.Warn("ws staleness sustained — forced reconnect", "class", class)
	_ = s.store.Audit(context.Background(), "warn", "ws-reconnect", "sustained stale "+class+" feed — socket force-closed for redial", "")
	m.mu.Lock()
	tgDue := time.Since(m.kickTld[class]) > 6*time.Hour
	if tgDue {
		m.kickTld[class] = time.Now()
	}
	m.mu.Unlock()
	if tgDue {
		s.TgSend("One of the live data feeds (" + class + ") went quiet while the connection looked healthy. I closed and reopened it automatically — no action needed. This message won't repeat for 6h for this feed.")
		s.latLatchSave() // R112: kick latch persists across restarts too
	}
	return true
}

// wsStaleSweep evaluates all three venues' WS data ages each probe tick.
func (s *Server) wsStaleSweep() {
	thr := s.cfg().Auto.LatWarnWSAgeS
	if thr <= 0 {
		thr = 120
	}
	// Poly-int trade tape: global activity stream — minutes of silence = wedge (the R107 case).
	if s.poly != nil {
		age := s.poly.LiveTradesAge()
		s.wsStaleCheck("poly_tape", age >= float64(thr), s.poly.ForceReconnect)
		connected, _, _, clobAge := s.poly.CLOBStats()
		s.wsStaleCheck("poly_clob_ws", connected && clobAge >= float64(thr), s.poly.ForceReconnectCLOB)
	}
	// Kalshi ticker/book WS: subscribed books with ZERO fresh feeds = the socket is dark.
	if s.kal != nil {
		subs, fresh, _, _ := s.kal.BookStats()
		s.wsStaleCheck("kalshi_ws", subs >= 10 && fresh == 0, s.kal.ForceReconnectWS)
	}
	// PUS markets WS: require one real MARKET payload from each promoted primary, then judge the
	// connection by transport continuity. Official docs define MARKET_DATA as a full order book and
	// runtime shows unchanged books may be quiet; time since the last price change is not a disconnect
	// signal. Heartbeats alone still cannot make a newly promoted, market-data-dark generation ready.
	if s.polyUSWS != nil {
		slugs, _, _ := s.polyUSWS.SubPlan()
		dataAge, primaryAge, haveData, havePrimary := s.polyUSWS.PrimaryDataAge()
		transportAge, haveTransport := s.polyUSWS.PrimaryFrameAge()
		s.wsStaleCheck("polyus_ws", polyUSWSConnectionStale(slugs, havePrimary, primaryAge, haveData, dataAge,
			haveTransport, transportAge, time.Duration(thr)*time.Second, polymarketus.MarketsWSPrimaryDataMaxAge,
			polymarketus.MarketsWSPrimaryTransportMaxAge), s.polyUSWS.ForceReconnect)
	}
}

func polyUSWSConnectionStale(slugs int, havePrimary bool, primaryAge time.Duration, haveData bool,
	dataAge time.Duration, haveTransport bool, transportAge, initialDataThreshold, dataThreshold,
	transportThreshold time.Duration) bool {
	if slugs < 10 {
		return false
	}
	if !havePrimary {
		return true
	}
	if !haveData {
		return primaryAge < 0 || primaryAge >= initialDataThreshold
	}
	if dataAge < 0 || dataAge >= dataThreshold {
		return true
	}
	if !haveTransport {
		return true
	}
	return transportAge < 0 || transportAge >= transportThreshold
}

// latencyEval — chip color + warn-once telegrams. A class must breach for 3 consecutive samples
// to go RED (and warn); one clean sample resets its streak; recovery after a warn logs Info.
func (s *Server) latencyEval() {
	m := s.latmon
	thrREST := s.cfg().Auto.LatWarnRESTMs
	if thrREST <= 0 {
		thrREST = 2000
	}
	thrDB := s.cfg().Auto.LatWarnDBP95Ms
	if thrDB <= 0 {
		thrDB = 500
	}
	m.mu.Lock()
	classes := map[string]bool{} // class -> breached this sample
	for _, k := range []string{"kalshi_rest", "polyus_rest", "polyint_rest"} {
		if r := m.rings[k]; r != nil && r.n > 0 {
			last := r.buf[(r.i-1+latRingCap)%latRingCap].MS
			classes[k] = last >= thrREST
		}
	}
	if r := m.rings["db_ms"]; r != nil && r.n > 0 {
		classes["db_ms"] = latPct(r.snap(), 0.95) >= thrDB
	}
	if r := m.rings["pus_priv_ok"]; r != nil && r.n > 0 {
		classes["pus_priv_ws"] = r.buf[(r.i-1+latRingCap)%latRingCap].MS < 1
	}
	red, yellow, toWarn, toRecover := latEvalDecide(m, classes, time.Now())
	dirty := len(toWarn) > 0 || len(toRecover) > 0
	m.mu.Unlock()
	sort.Strings(red)
	sort.Strings(yellow)
	switch {
	case len(red) > 0:
		m.chip.Store("red")
		m.why.Store("sustained: " + strings.Join(red, ", "))
	case len(yellow) > 0:
		m.chip.Store("yellow")
		m.why.Store("breach: " + strings.Join(yellow, ", "))
	default:
		m.chip.Store("green")
		m.why.Store("")
	}
	if len(toWarn) > 0 {
		msg := "⚠ Connection health: " + strings.Join(toWarn, ", ") + " has been slow for 90+ seconds. The bot still works but reacts slower than usual. This message won't repeat for 6h per issue."
		s.TgSend(msg)
		s.log.Warn("latency degradation sustained — telegram sent (warn-once/6h, persisted)", "classes", strings.Join(toWarn, ","))
		_ = s.store.Audit(context.Background(), "warn", "latency", "sustained latency degradation: "+strings.Join(toWarn, ", "), "")
	}
	if len(toRecover) > 0 {
		s.TgSend("✅ Connection health: " + strings.Join(toRecover, ", ") + " is back to normal speed.")
		s.log.Info("latency recovered — recovery note sent", "classes", strings.Join(toRecover, ","))
	}
	if dirty {
		s.latLatchSave()
	}
}

// latEvalDecide — the pure warn/recovery decision (unit-tested in r112): caller holds m.mu.
// One warn per class per 6h GLOBALLY (the latch survives recovery and, via kv, restarts);
// one recovery note per warn window, sent when a warned class posts a clean sample after
// having been sustained-red.
func latEvalDecide(m *latMon, classes map[string]bool, now time.Time) (red, yellow, toWarn, toRecover []string) {
	for cls, breached := range classes {
		if breached {
			m.breach[cls]++
		} else {
			if at, warnedBefore := m.warned[cls]; warnedBefore && !m.recovered[cls] && m.breach[cls] >= 3 && now.Sub(at) <= 6*time.Hour {
				m.recovered[cls] = true
				toRecover = append(toRecover, cls)
			}
			m.breach[cls] = 0
			continue
		}
		if m.breach[cls] >= 3 {
			red = append(red, cls)
		} else {
			yellow = append(yellow, cls)
		}
	}
	for _, cls := range red {
		if at, ok := m.warned[cls]; !ok || now.Sub(at) > 6*time.Hour {
			m.warned[cls] = now
			m.recovered[cls] = false
			toWarn = append(toWarn, cls)
		}
	}
	return red, yellow, toWarn, toRecover
}

// latLatchDump — the persisted telegram-latch state (kv "latency_tg_latch").
type latLatchDump struct {
	Warned    map[string]time.Time `json:"warned"`
	Recovered map[string]bool      `json:"recovered"`
	KickTld   map[string]time.Time `json:"kick_tld"`
}

// latLatchSave persists the telegram latches so "won't repeat for 6h" survives restarts
// (pre-R112 the latch was memory-only: 8 boots in a day = 8 fresh warns for one issue).
func (s *Server) latLatchSave() {
	m := s.latmon
	m.mu.Lock()
	d := latLatchDump{Warned: map[string]time.Time{}, Recovered: map[string]bool{}, KickTld: map[string]time.Time{}}
	for k, v := range m.warned {
		d.Warned[k] = v
	}
	for k, v := range m.recovered {
		d.Recovered[k] = v
	}
	for k, v := range m.kickTld {
		d.KickTld[k] = v
	}
	m.mu.Unlock()
	b, err := json.Marshal(d)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.store.KVSet(ctx, "latency_tg_latch", string(b))
}

// latLatchLoad restores the latches at boot.
func (s *Server) latLatchLoad(ctx context.Context) {
	v, ok := s.store.KVGet(ctx, "latency_tg_latch")
	if !ok {
		return
	}
	var d latLatchDump
	if json.Unmarshal([]byte(v), &d) != nil {
		return
	}
	m := s.latmon
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, t := range d.Warned {
		if time.Since(t) <= 6*time.Hour {
			m.warned[k] = t
			m.recovered[k] = d.Recovered[k]
		}
	}
	for k, t := range d.KickTld {
		if time.Since(t) <= 6*time.Hour {
			m.kickTld[k] = t
		}
	}
}

// handleLatency (GET /api/latency) — the panel + chip payload.
func (s *Server) handleLatency(w http.ResponseWriter, r *http.Request) {
	m := s.latmon
	if m == nil {
		writeJSON(w, http.StatusOK, map[string]any{"chip": "green", "note": "latency monitor not started"})
		return
	}
	m.mu.Lock()
	rings := map[string]any{}
	cur := map[string]any{}
	for k, ring := range m.rings {
		pts := ring.snap()
		rings[k] = pts
		if len(pts) > 0 {
			cur[k] = map[string]any{"last": pts[len(pts)-1].MS, "p50": latPct(pts, 0.50), "p95": latPct(pts, 0.95), "n": len(pts)}
		}
	}
	reqMin := map[string]float64{}
	total := 0.0
	for v, c := range m.counts {
		n := float64(atomic.LoadInt64(c))
		la, _ := m.lastAt.Load().(time.Time)
		up := time.Since(s.bootAt).Minutes()
		_ = la
		if up < 1 {
			up = 1
		}
		reqMin[v] = math.Round(n/up*10) / 10
		total += n / up
	}
	network := m.network
	m.mu.Unlock()
	// Protocol-load estimate: REST request ≈1.2KB up (headers+TLS records amortized) + WS
	// keepalive/subscriptions ≈2kbps steady. Do not compare this with a remembered hotspot speed;
	// the network_path receipt below measures the current local link and independent internet path.
	estKbps := math.Round((total*1.2*8/60+2)*10) / 10
	verdict := fmt.Sprintf("suite protocol upload estimate ≈%.0f kbps; use network_path for current hotspot/link attribution", estKbps)
	if estKbps > 400 {
		verdict = fmt.Sprintf("suite protocol upload estimate ≈%.0f kbps is unusually high; watch network_path and WebSocket drops", estKbps)
	}
	thrREST := s.cfg().Auto.LatWarnRESTMs
	if thrREST <= 0 {
		thrREST = 2000
	}
	thrWS := s.cfg().Auto.LatWarnWSAgeS
	if thrWS <= 0 {
		thrWS = 120
	}
	thrDB := s.cfg().Auto.LatWarnDBP95Ms
	if thrDB <= 0 {
		thrDB = 500
	}
	skew, _ := m.skewMS.Load().(float64)
	polyintSlow := false
	if s.poly != nil {
		polyintSlow = s.poly.SlowMode() // R112: adaptive slow mode (venue-side degradation → TTLs ×3)
	}
	networkPayload := map[string]any{"sampled": false, "classification": "unknown", "state": "warming",
		"reason": "first bounded network-path sample pending", "nonblocking": true, "trade_authorizing": false}
	if !network.At.IsZero() {
		networkPayload = map[string]any{
			"sampled": true, "sample_at": network.At.UTC().Format(time.RFC3339Nano),
			"sample_age_s":   math.Max(0, math.Round(time.Since(network.At).Seconds())),
			"classification": network.Classification, "state": network.State, "reason": network.Reason,
			"internet_known": network.InternetKnown, "internet_ok": network.InternetOK,
			"internet_rtt_ms": math.Round(network.InternetRTTMS),
			"wifi_available":  network.WiFiAvailable, "wifi_connected": network.WiFiConnected,
			"wifi_signal_known": network.WiFiSignalKnown, "wifi_signal_pct": network.WiFiSignalPct,
			"wifi_receive_mbps": network.WiFiReceiveMbps, "wifi_transmit_mbps": network.WiFiTransmitMbps,
			"slow_venues": network.VenueSlow, "nonblocking": true, "trade_authorizing": false,
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"chip": m.chip.Load(), "why": m.why.Load(),
		"current": cur, "rings": rings,
		"clock_skew_ms":     math.Round(skew),
		"polyint_slow_mode": polyintSlow,
		"network_path":      networkPayload,
		"upload":            map[string]any{"reqs_per_min": reqMin, "est_kbps_up": estKbps, "verdict": verdict},
		"thresholds":        map[string]any{"rest_ms": thrREST, "ws_age_s": thrWS, "db_p95_ms": thrDB, "sustained_samples": 3, "sample_every_s": 30},
		"note":              "active probes hit each venue's public cheapest endpoint every 30s; *_passive rings observe real client traffic; order_polyus_ms lands when live orders flow",
	})
}
