package server

// R70 tests (audit §d): money-critical LOGIC that had zero coverage — the boot-path Reset P&L
// baseline math, the snapBust debounce/trailing gate, the live placement validation layer +
// YES→bid / NO→ask@(1−p) order mapping, and the WS-tick hedged-cancel dedup. Everything mocks at
// the storage/paper layer (temp SQLite) or uses fast-failing clients — no live feeds, no venue.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"encoding/json"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/killswitch"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// testServer builds a hermetic Server: real temp-dir SQLite store, discard logger, fresh kill
// switch, and a 1ms-timeout poly client so any accidental network path fails instantly instead of
// reaching a venue. No monitors run; only the code under test executes.
func testServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	st, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s := &Server{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil)), ks: killswitch.New(nil),
		allowSyntheticKalshiEventIdentity: true}
	// Production keeps the modeled 100 ms final-wire delay. Most hermetic tests exercise
	// unrelated behavior, so disable that wait centrally; the R154 realism tests explicitly
	// restore a positive delay when they verify the two-touch execution model.
	s.livePolicyMirrorWireDelay = -1
	// The R153 candidate-audit writer starts lazily only in tests that emit an armed LIVE receipt.
	// Join it before the earlier-registered Store cleanup closes SQLite; this keeps hermetic tests
	// from leaking a retry worker or racing database close.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.stopLiveCandidateAudit(ctx)
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.stopGenfollowPaperWorkers(ctx)
	})
	tcfg := config.Default()
	tcfg.DataDir = dir
	s.cfgP.Store(&tcfg)                                 // R76 (bug 18): seed the copy-on-write snapshot like server.New does
	s.poly = polymarket.NewClient(1 * time.Millisecond) // fast-fail: marks fall back to entry price
	s.polyResolve = map[string]polyResolved{}
	s.gateExecDone = map[string]time.Time{}
	s.miniProbe = func() bool { return false } // R95: a stray dev-box mini-server must never flip a header assertion
	s.plabHorizonFn = func(_ context.Context, _ plabLeg, now time.Time) (time.Time, bool) {
		return now.Add(time.Hour), true
	} // hermetic Combo Lab tests opt into a known current horizon; horizon-contract tests clear/replace it
	s.paperHorizonFn = func(context.Context, string, string, string) float64 { return 1 }
	// Hermetic maker-fill tests use a known one-hour horizon. Production always uses live venue time.
	return s
}

// ── doPaperReset: the boot-path baseline math (reset_on_start runs this EVERY boot) ─────────────

func TestDoPaperResetBaselineMath(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	buy := func(ticker string, ct, px float64) {
		t.Helper()
		if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: "polymarket", Ticker: ticker, Title: "T " + ticker,
			Side: "Yes", Action: "BUY", Price: px, Contracts: ct, Fee: 0.05, Source: "manual"}); err != nil {
			t.Fatalf("insert fill: %v", err)
		}
	}
	// one OPEN position (must be flattened by the reset)…
	buy("0xopen", 10, 0.40)
	// …and one already-CLOSED round trip (history — must SURVIVE the reset untouched).
	buy("0xdone", 5, 0.50)
	if _, err := s.store.InsertPaperFill(ctx, paper.Fill{Platform: "polymarket", Ticker: "0xdone", Title: "T 0xdone",
		Side: "Yes", Action: "SELL", Price: 0.70, Contracts: 5, Fee: 0.05, Source: "manual"}); err != nil {
		t.Fatalf("insert sell: %v", err)
	}
	// a persisted intraday curve point — Reset must wipe the series.
	if err := s.store.InsertPnLPoint(ctx, "2026-07-01T00:00:00Z", 1.23); err != nil {
		t.Fatalf("pnl point: %v", err)
	}

	closedN, busy := s.doPaperReset(ctx)
	if busy {
		t.Fatal("no ML sidecar in a temp dir — lock must not read busy")
	}
	if closedN != 1 {
		t.Fatalf("closedN = %d, want 1 (the single open position)", closedN)
	}
	fills, err := s.store.ListPaperFills(ctx)
	if err != nil {
		t.Fatalf("list fills: %v", err)
	}
	positions, _ := paper.Aggregate(fills)
	for _, p := range positions {
		if p.Contracts > 0.0001 {
			t.Fatalf("position %s still open after reset (%v contracts)", p.Ticker, p.Contracts)
		}
	}
	// closed history KEPT: the 0xdone round trip still has both fills.
	nDone := 0
	for _, f := range fills {
		if f.Ticker == "0xdone" {
			nDone++
		}
	}
	if nDone != 2 {
		t.Fatalf("closed-history fills for 0xdone = %d, want 2 (reset must never touch history)", nDone)
	}
	// baselines captured AFTER the closes: header math starts from the post-close stats.
	st := paper.Stats(fills)
	s.pnlMu.Lock()
	rb, fb, epoch := s.pnlRealizedBase, s.pnlFeesBase, s.pnlEpoch
	s.pnlMu.Unlock()
	if math.Abs(rb-st.Realized) > 1e-9 || math.Abs(fb-st.Fees) > 1e-9 {
		t.Fatalf("bases (rb=%v fb=%v) != post-close stats (%v, %v)", rb, fb, st.Realized, st.Fees)
	}
	if epoch.IsZero() {
		t.Fatal("pnlEpoch must be stamped by the reset")
	}
	// the reset PERSISTS (KV pnl_reset) and the curve is wiped.
	if _, ok := s.store.KVGet(ctx, "pnl_reset"); !ok {
		t.Fatal("pnl_reset KV snapshot missing — reset would silently un-reset on restart")
	}
	if pts, _ := s.store.ListPnLPoints(ctx, 10); len(pts) != 0 {
		t.Fatalf("pnl_series not cleared: %d points remain", len(pts))
	}
}

// ── snapBust: debounce + trailing rebuild (the R51 stale-click fix) ──────────────────────────────

func TestBustGateDebounceAndTrailing(t *testing.T) {
	s := &Server{}
	var trailed atomic.Int32
	trailing := func() { trailed.Add(1) }
	if !s.bustGate(trailing) {
		t.Fatal("first mutation must rebuild immediately")
	}
	// burst: both debounced, but exactly ONE trailing rebuild gets queued for the window edge.
	if s.bustGate(trailing) {
		t.Fatal("second mutation within 1s must be debounced")
	}
	if s.bustGate(trailing) {
		t.Fatal("third mutation within 1s must be debounced")
	}
	deadline := time.Now().Add(3 * time.Second)
	for trailed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := trailed.Load(); got != 1 {
		t.Fatalf("trailing rebuilds = %d, want exactly 1 (the LAST mutation of a burst must land, once)", got)
	}
	// window lapsed (bustLast is still the FIRST call's stamp) — the next mutation is immediate again.
	if !s.bustGate(trailing) {
		t.Fatal("post-window mutation must rebuild immediately")
	}
}

// ── live placement: the selfPOST validation layer + the YES→bid / NO→ask@(1−p) mapping ─────────

func TestKalshiLiveOrderReqMapping(t *testing.T) {
	now := time.Date(2026, 7, 4, 10, 15, 30, 0, time.UTC)
	yes := kalshiLiveOrderReq("KXBTC-TEST", "YES", 35, 2, now)
	if yes.Side != "bid" || yes.Price != "0.35" {
		t.Fatalf("YES 35¢ must map to bid @0.35, got %s @%s", yes.Side, yes.Price)
	}
	no := kalshiLiveOrderReq("KXBTC-TEST", "NO", 35, 2, now)
	if no.Side != "ask" || no.Price != "0.65" {
		t.Fatalf("NO 35¢ must map to ask @0.65 (1−p on the YES leg), got %s @%s", no.Side, no.Price)
	}
	if yes.Count != "2" || !yes.PostOnly || yes.TimeInForce != "good_till_canceled" {
		t.Fatalf("maker base shape wrong: %+v", yes)
	}
	// idempotency: the SAME intent in the same minute must produce the SAME client_order_id…
	again := kalshiLiveOrderReq("KXBTC-TEST", "YES", 35, 2, now.Add(20*time.Second))
	if again.ClientOrderID != yes.ClientOrderID {
		t.Fatal("same intent within the minute must reuse the client_order_id (venue-side dedup)")
	}
	// …and a different price is a different intent.
	if diff := kalshiLiveOrderReq("KXBTC-TEST", "YES", 36, 2, now); diff.ClientOrderID == yes.ClientOrderID {
		t.Fatal("different price must produce a different client_order_id")
	}
}

func TestR132KalshiLiveOrderReqPreservesSubCentPrice(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	yes := kalshiLiveOrderReqDollars("KXSUBCENT", "YES", 0.055, 2, now)
	if yes.Side != "bid" || yes.Price != "0.055" {
		t.Fatalf("YES 5.5c wire request = %+v", yes)
	}
	no := kalshiLiveOrderReqDollars("KXSUBCENT", "NO", 0.055, 2, now)
	if no.Side != "ask" || no.Price != "0.945" {
		t.Fatalf("NO 5.5c wire request = %+v", no)
	}
	if yes.ClientOrderID == kalshiLiveOrderReqDollars("KXSUBCENT", "YES", 0.056, 2, now).ClientOrderID {
		t.Fatal("sub-cent-distinct intents must have distinct idempotency keys")
	}
	for _, raw := range []json.RawMessage{json.RawMessage(`0.055`), json.RawMessage(`"0.055"`)} {
		if got, ok := decodeOrderPrice(raw); !ok || got != 0.055 {
			t.Fatalf("decodeOrderPrice(%s)=%v/%v", raw, got, ok)
		}
	}
}

func TestR160KalshiLiveOrderReqQuantizesFloatDust(t *testing.T) {
	now := time.Date(2026, 7, 18, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, side, wantSide, wantPrice string
		outcomePrice                    float64
	}{
		{"yes-five-cents", "YES", "bid", "0.05", 0.050000000000000044},
		{"yes-seven-cents", "YES", "bid", "0.07", 0.06999999999999995},
		{"yes-eight-cents", "YES", "bid", "0.08", 0.07999999999999996},
		{"yes-eighteen-cents", "YES", "bid", "0.18", 0.18000000000000005},
		{"no-complement", "NO", "ask", "0.05", 0.95},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := kalshiLiveOrderReqDollars("KXFLOATDUST", tc.side, tc.outcomePrice, 1, now)
			if req.Side != tc.wantSide || req.Price != tc.wantPrice {
				t.Fatalf("%s %.18f wire request = %s @%s, want %s @%s",
					tc.side, tc.outcomePrice, req.Side, req.Price, tc.wantSide, tc.wantPrice)
			}
		})
	}
}

func TestHandleLivePlaceValidationGates(t *testing.T) {
	s := testServer(t)
	post := func(body string) int {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/api/live/place", strings.NewReader(body))
		s.handleLivePlace(rec, req)
		return rec.Code
	}
	// price band 1–99¢ + count ≥1 + non-empty ticker: every malformed shape must 400 before ANY
	// state is consulted (these are the request-shape gates the auto-live selfPOST relies on).
	for _, bad := range []string{
		`not json`,
		`{"ticker":"","side":"yes","price_cents":50,"count":1}`,
		`{"ticker":"T","side":"yes","price_cents":0,"count":1}`,
		`{"ticker":"T","side":"yes","price_cents":100,"count":1}`,
		`{"ticker":"T","side":"yes","price_cents":50,"count":0}`,
	} {
		if code := post(bad); code != 400 {
			t.Fatalf("bad order %q must 400, got %d", bad, code)
		}
	}
	// well-formed but NOT ARMED → 403 (real money never places without the session arm).
	if code := post(`{"ticker":"T","side":"yes","price_cents":50,"count":1}`); code != 403 {
		t.Fatalf("unarmed placement must 403, got %d", code)
	}
	// armed but kill switch TRIPPED → 403 (the belt-and-suspenders halt).
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveMu.Unlock()
	s.ks.Trip("test")
	if code := post(`{"ticker":"T","side":"yes","price_cents":50,"count":1}`); code != 403 {
		t.Fatalf("tripped kill switch must 403, got %d", code)
	}
}

func TestLiveGateTunablesDefaultsAndBand(t *testing.T) {
	s := &Server{}
	v, lo, hi := s.liveGateTunables()
	if v != 20000 || lo != 2 || hi != 98 {
		t.Fatalf("zero config must yield the historical $20k / 2–98¢, got %v/%v/%v", v, lo, hi)
	}
	s.cfg().Risk.LiveMinVol24hUSD, s.cfg().Risk.LivePxBandMinC, s.cfg().Risk.LivePxBandMaxC = 5000, 10, 90
	if v, lo, hi = s.liveGateTunables(); v != 5000 || lo != 10 || hi != 90 {
		t.Fatalf("explicit tunables must pass through, got %v/%v/%v", v, lo, hi)
	}
	s.cfg().Risk.LivePxBandMinC, s.cfg().Risk.LivePxBandMaxC = 60, 40 // inverted band
	if _, lo, hi = s.liveGateTunables(); lo != 2 || hi != 98 {
		t.Fatalf("an inverted band must fall back to 2–98¢, got %v–%v", lo, hi)
	}
}

// ── WS-tick cancels: the 1¢-rule selection + the cancelInFlight dedup latch ──────────────────────

func TestKalshiTickCancelHitsDedup(t *testing.T) {
	s := &Server{}
	s.liveOrders = map[string]liveOrderInfo{
		"o1": {Ticker: "T", Side: "bid", YesPrice: 0.50},
		"o2": {Ticker: "T", Side: "bid", YesPrice: 0.50},
		"o3": {Ticker: "U", Side: "bid", YesPrice: 0.50},
	}
	// not armed ⇒ never a hit (paper stays paper).
	if hits := s.kalshiTickCancelHits("T", 0.55); hits != nil {
		t.Fatalf("unarmed must select nothing, got %v", hits)
	}
	s.liveArmed = true
	// sub-1¢ wiggle ⇒ let it rest.
	if hits := s.kalshiTickCancelHits("T", 0.505); len(hits) != 0 {
		t.Fatalf("0.5¢ move must not cancel, got %v", hits)
	}
	// a ≥1¢ move selects BOTH resting T orders (and never touches U)…
	hits := s.kalshiTickCancelHits("T", 0.52)
	if len(hits) != 2 {
		t.Fatalf("1¢ breach must select both T orders, got %v", hits)
	}
	for _, h := range hits {
		if h.id == "o3" {
			t.Fatal("an order on a different ticker must never be selected")
		}
		if h.rested != 0.50 {
			t.Fatalf("hit must carry the rested price, got %v", h.rested)
		}
	}
	// …and a tick BURST cannot double-cancel: the latch holds until the in-flight cancel completes.
	if hits := s.kalshiTickCancelHits("T", 0.53); len(hits) != 0 {
		t.Fatalf("latched orders must not re-select on a burst, got %v", hits)
	}
	s.liveMu.Lock()
	delete(s.cancelInFlight, "o1") // cancel completed → eligible again
	s.liveMu.Unlock()
	if hits := s.kalshiTickCancelHits("T", 0.53); len(hits) != 1 || hits[0].id != "o1" {
		t.Fatalf("released order must re-select alone, got %v", hits)
	}
}

func TestCancelGoneClassifier(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, true},
		{errors.New("kalshi: 404 order not found"), true},
		{errors.New("status not_found"), true},
		{errors.New("order already canceled"), true},
		{errors.New("500 internal server error"), false},
		{errors.New("context deadline exceeded"), false},
	} {
		if got := cancelGone(tc.err); got != tc.want {
			t.Fatalf("cancelGone(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// ── sharpline: crossMatchKalshi's (ticker, title, FRACTION price) contract (the R70 latent bug) ──

func TestCrossMatchKalshiReturnsTickerAndFraction(t *testing.T) {
	resetMatchTelemetry()
	var m kalshi.Market
	if err := json.Unmarshal([]byte(fmt.Sprintf(`{
		"ticker":"KXMLBGAME-YANKS","event_ticker":"KXMLBGAME",
		"title":"Yankees vs Red Sox Winner","yes_sub_title":"New York Yankees",
		"expected_expiration_time":"2026-07-03T22:00:00Z",
		"yes_ask_dollars":%q}`, "0.42")), &m); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	evDate, _ := parseTime("2026-07-03T20:00:00Z")
	ticker, title, px, ok := crossMatchKalshi("New York Yankees vs Boston Red Sox New York Yankees wins",
		[]kalshi.Market{m}, evDate, true)
	if !ok {
		t.Fatal("fixture must match")
	}
	if ticker != "KXMLBGAME-YANKS" {
		t.Fatalf("first return must be the TICKER (signal_log resolution key), got %q", ticker)
	}
	if title != "Yankees vs Red Sox Winner" {
		t.Fatalf("second return must be the title, got %q", title)
	}
	// THE R70 bug: this used to come back as 42 (percent) — sweepSharpline's 0.02–0.98 band then
	// rejected every single row, so sharpline never logged. It must be a 0–1 fraction.
	if math.Abs(px-0.42) > 1e-9 {
		t.Fatalf("price must be the 0–1 fraction 0.42, got %v", px)
	}
}

// ── capSyncMap: the evTitle/mktTS bound (R70 memory cap) ─────────────────────────────────────────

func TestCapSyncMap(t *testing.T) {
	var m sync.Map
	for i := 0; i < 5; i++ {
		m.Store(fmt.Sprintf("k%d", i), "v")
	}
	capSyncMap(&m, 10) // under cap: untouched
	n := 0
	m.Range(func(_, _ any) bool { n++; return true })
	if n != 5 {
		t.Fatalf("under cap must keep entries, got %d", n)
	}
	capSyncMap(&m, 3) // over cap: cleared (re-fetchable display labels rebuild lazily)
	n = 0
	m.Range(func(_, _ any) bool { n++; return true })
	if n != 0 {
		t.Fatalf("over cap must clear, got %d", n)
	}
}
