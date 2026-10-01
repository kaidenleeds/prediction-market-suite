package server

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func insertR135cUnitProofRows(t *testing.T, s *Server, family, origin, prefix, side string, depth float64, feeKnown bool) {
	t.Helper()
	ctx := context.Background()
	day0 := utcDay(time.Now().UTC())
	for i := 0; i < 21; i++ {
		ticker := fmt.Sprintf("%s-%02d", prefix, i)
		quoteSource := "kalshi-ws"
		if origin == "strategy" {
			quoteSource = "strategy-decision/kalshi-ws"
		}
		feeSource := ""
		if feeKnown {
			feeSource = "kalshi:test-schedule"
		}
		opened := day0.AddDate(0, 0, i-21) // twenty active completed days: -20 through -1
		if i == 0 {
			opened = day0.AddDate(0, 0, -40) // proves the route was observed for a full 30-day window
		}
		inserted, err := s.store.InsertUnitTrial(ctx, storage.UnitTrial{
			OpenedTS: opened, Family: family, Platform: "kalshi", OriginLayer: origin,
			Ticker: ticker, Side: side, Ask: .10, FeePC: .01, FeeKnown: feeKnown,
			FeeSource: feeSource, Depth: depth, QuoteSource: quoteSource,
		})
		if err != nil || !inserted {
			t.Fatalf("insert %s/%s/%s=%v err=%v", family, origin, ticker, inserted, err)
		}
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,settle_val=1,pnl_pc=.89,return_per_dollar=8
WHERE family=? AND origin_layer=? AND ticker=? AND side=?`,
			opened.Add(time.Hour).Format(time.RFC3339Nano), family, origin, ticker, side); err != nil {
			t.Fatal(err)
		}
	}
}

func mirrorTestCandidate() liveMirrorCandidate {
	return liveMirrorCandidate{
		Platform: "kalshi", Ticker: "KXR132-MIRROR", Title: "test", Side: "YES",
		Source: "auto-cons-kflow", Price: 0.42, At: time.Now(),
	}
}

func TestR132LiveMirrorNeedsArmAndAutoToQueueOrConsume(t *testing.T) {
	s := &Server{}
	c := mirrorTestCandidate()
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("disarmed/off paper decision must not enter the live candidate bus")
	}

	// Even an already queued candidate is not consumed while either half of the handshake is off.
	s.liveMirrorQ = []liveMirrorCandidate{c}
	s.liveArmed, s.liveAuto = true, false
	if _, ok := s.popLiveMirrorCandidate(); ok {
		t.Fatal("AUTO off must not consume a candidate")
	}
	if len(s.liveMirrorQ) != 1 {
		t.Fatal("off-state read must leave the candidate untouched")
	}
	s.liveArmed, s.liveAuto = false, true
	if _, ok := s.popLiveMirrorCandidate(); ok {
		t.Fatal("unarmed AUTO must not consume a candidate")
	}

	// The write handler independently enforces AUTO at handler time, closing a toggle race after
	// the monitor's first check. It rejects before any venue client can be reached.
	s.liveArmed, s.liveAuto = true, false
	code, out := s.selfPOST(s.handleLivePlace, "/api/live/place", map[string]any{
		"ticker": c.Ticker, "side": c.Side, "price_dollars": "0.42", "count": 1,
		"auto": true,
	})
	if code != http.StatusForbidden || out["error"] != "AUTO LIVE is off" {
		t.Fatalf("handler must reject an AUTO write after toggle-off: code=%d out=%v", code, out)
	}
}

func TestR132LiveMirrorDropsStaleAndDeduplicates(t *testing.T) {
	s := &Server{liveArmed: true, liveAuto: true}
	c := mirrorTestCandidate()
	if !s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("armed+auto fresh candidate should queue")
	}
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("same venue/market/side decision inside the dedup window must not queue twice")
	}
	if len(s.liveMirrorQ) != 1 {
		t.Fatalf("dedup queue len=%d want 1", len(s.liveMirrorQ))
	}
	if len(s.liveMirrorComboQ) != 1 {
		t.Fatalf("fresh Kalshi strategy decision must also enter the short combo-pair window, got %d", len(s.liveMirrorComboQ))
	}

	s.liveMirrorQ = []liveMirrorCandidate{{
		Platform: "kalshi", Ticker: "STALE", Side: "NO", Source: "auto-ml", Price: 0.40,
		At: time.Now().Add(-liveMirrorTTL - time.Second),
	}}
	if _, ok := s.popLiveMirrorCandidate(); ok {
		t.Fatal("stale candidate must be dropped, never dispatched")
	}
	if len(s.liveMirrorQ) != 0 {
		t.Fatal("stale candidate should be removed from the bounded queue")
	}
	staleLogged := false
	for _, row := range s.liveLog {
		if row["event"] == "AUTO-LIVE-MIRROR-DROP" && row["reason"] == "expired-before-dispatch" &&
			row["ticker"] == "STALE" {
			staleLogged = true
		}
	}
	if !staleLogged {
		t.Fatalf("stale queue removal must leave an explicit drop receipt: %+v", s.liveLog)
	}
}

func TestR132LiveMirrorUnknownConfidenceFailsClosed(t *testing.T) {
	s := &Server{}
	c := liveMirrorCandidate{
		Platform: "polyus", Ticker: "unknown-slug", Side: "YES", Source: "unknown-source",
		Price: 0.35, At: time.Now(),
	}
	if ok, why, edge := s.liveMirrorConfidence(context.Background(), c, 0.35, false); ok || why != "sealed-accepted-paper-intent-required" || edge != 0 {
		t.Fatalf("unknown source/model must fail closed: ok=%v why=%q edge=%v", ok, why, edge)
	}
}

func TestR133FreshListLiveProofIsVenueAware(t *testing.T) {
	k := liveMirrorCandidate{Platform: "kalshi", Source: "freshinv", Inverted: true}
	p := liveMirrorCandidate{Platform: "polyus", Source: "freshinv", Inverted: false}
	legacyP := liveMirrorCandidate{Platform: "polyus", Source: "freshinv", Inverted: true}
	if got := liveMirrorFamily(k); got != "invert:freshlist" {
		t.Fatalf("Kalshi fresh fade family=%q", got)
	}
	if got := liveMirrorFamily(p); got != "freshlist" {
		t.Fatalf("PolyUS fresh follow family=%q", got)
	}
	if got := liveMirrorFamily(legacyP); got != "freshlist" {
		t.Fatalf("legacy PolyUS flag must not override venue truth, got %q", got)
	}
}

func TestR133LiveAuthorityUsesExecutableUnitProofAndCurrentCost(t *testing.T) {
	lo, hi, netLo, netHi := .04, .12, .01, .20
	u := storage.UnitTrialLeaderboardStat{Family: "edge", Platform: "kalshi", Side: "YES", N: 100, SettledMarkets: 100,
		SettledEventClusters: 100,
		MatureUTCBlocks:      30, ObservedUTCBlocks: 20, EventDayClusters: 20,
		DayMeanPC: .08, DayMeanLoPC: &lo, DayMeanHiPC: &hi,
		ClusterNetPerDayLo: &netLo, ClusterNetPerDayHi: &netHi,
		MeanPC: .08, SDPC: .01, MeanAsk: 0.40, MeanFeePC: 0.01, ProofMeanAsk: 0.40, ProofMeanFeePC: 0.01}
	state, mean, lo := liveMirrorUnitAdjusted(u, 0.40, 0.01)
	if state != "PROVEN+" || mean <= 0 || lo <= 0 {
		t.Fatalf("strong executable unit lane should prove: state=%s mean=%v lo=%v", state, mean, lo)
	}
	_, expensiveMean, expensiveLo := liveMirrorUnitAdjusted(u, 0.49, 0.01)
	if math.Abs((mean-expensiveMean)-0.09) > 1e-9 || math.Abs((lo-expensiveLo)-0.09) > 1e-9 {
		t.Fatalf("worse current all-in cost must be fully charged: base=%v/%v expensive=%v/%v", mean, lo, expensiveMean, expensiveLo)
	}
	_, cheaperMean, cheaperLo := liveMirrorUnitAdjusted(u, 0.30, 0.01)
	if cheaperMean != mean || cheaperLo != lo {
		t.Fatalf("cheaper quote must receive no optimistic credit: base=%v/%v cheap=%v/%v", mean, lo, cheaperMean, cheaperLo)
	}
}

func TestR135cLiveTakerProofRequiresStrategySideDepthFeeAndMatureClusters(t *testing.T) {
	s := testServer(t)
	s.kalFees = map[string]kalFeeInfo{
		"KXUNITPROOF": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXUNITPROOF-NOW", Side: "YES", Source: "auto-cons-kflow", Price: .10}

	// Economically unexecutable observations remain coverage only even when someone writes a
	// positive outcome into the legacy columns.
	insertR135cUnitProofRows(t, s, "kflow", "strategy", "BADDEPTH", "YES", 0, true)
	insertR135cUnitProofRows(t, s, "kflow", "strategy", "BADFEE", "YES", 1, false)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .10, false); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("depth0/unknown-fee history authorized LIVE: ok=%v why=%q", ok, why)
	}

	// Discovery-model success is not accepted-strategy execution evidence.
	insertR135cUnitProofRows(t, s, "kflow", "model", "KXUNITPROOF-MODEL", "YES", 1, true)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .10, false); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("model-origin history authorized a strategy: ok=%v why=%q", ok, why)
	}

	insertR135cUnitProofRows(t, s, "kflow", "strategy", "KXUNITPROOF-STRAT", "YES", 1, true)
	if ok, why, mean, lo, _ := s.liveMirrorProof(context.Background(), c, .10, false); ok || why != "sealed-accepted-paper-intent-required" || mean != 0 || lo != 0 {
		t.Fatalf("rolling mature strategy route bypassed sealed Paper proof: ok=%v why=%q mean=%v lo=%v", ok, why, mean, lo)
	}
	c.Side = "NO"
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .10, false); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("YES evidence crossed into NO route: ok=%v why=%q", ok, why)
	}
	c.Side = "YES"
	if inserted, err := s.store.InsertUnitTrial(context.Background(), storage.UnitTrial{
		OpenedTS: time.Now().UTC().Add(-time.Hour), Family: "kflow", Platform: "kalshi", OriginLayer: "strategy",
		Ticker: "KXUNITPROOF-OPEN", Side: "YES", Ask: .10, FeePC: .01, FeeKnown: true,
		FeeSource: "kalshi:test-schedule", Depth: 1, QuoteSource: "strategy-decision/kalshi-ws",
	}); err != nil || !inserted {
		t.Fatalf("insert open=%v err=%v", inserted, err)
	}
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .10, false); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("open strategy cohort failed to censor proof: ok=%v why=%q", ok, why)
	}
}

func TestR135cCanonicalLeaderboardSameEventBurstIsPreliminary(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if err := s.store.UpsertMarketCatalog(ctx, []storage.CatalogRow{{Venue: "kalshi",
		Ticker: "KXSAMEEVENT-ONE", EventKey: "KXSAMEEVENT", Kind: "winner",
		Title: "same event fixture"}}); err != nil {
		t.Fatal(err)
	}
	day0 := utcDay(time.Now().UTC())
	opened := day0.AddDate(0, 0, -20)
	for episode := 0; episode < 21; episode++ {
		rowOpened := opened
		if episode == 0 {
			rowOpened = day0.AddDate(0, 0, -40) // establishes a complete 30-day observation window
		}
		inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{
			OpenedTS: rowOpened, Family: "same-event", Platform: "kalshi", OriginLayer: "model",
			Ticker: "KXSAMEEVENT-ONE", Side: "YES", Episode: episode, Ask: .10, FeePC: .01,
			FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws",
		})
		if err != nil || !inserted {
			t.Fatalf("insert episode %d=%v err=%v", episode, inserted, err)
		}
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,settle_val=1,pnl_pc=.89 WHERE family='same-event' AND episode=?`,
			rowOpened.Add(time.Hour).Format(time.RFC3339Nano), episode); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := s.store.UnitTrialLeaderboard(ctx, time.Now().UTC())
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	rows := leaderboardBacktestRows(stats)
	if len(rows) != 1 || rows[0].Proof == "PROVEN+" || stats[0].SettledEventClusters != 1 ||
		stats[0].ObservedUTCBlocks > 1 || stats[0].EventDayClusters > 1 {
		t.Fatalf("correlated same-event burst self-proved: stats=%+v rows=%+v", stats, rows)
	}
}

func TestR132MLBookCannotEnterLiveAuto(t *testing.T) {
	s := &Server{liveArmed: true, liveAuto: true}
	c := mirrorTestCandidate()
	c.Source = "auto-ml"
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("ML-book decision must never enter the live-auto candidate bus")
	}
	if ok, why, _ := s.liveMirrorConfidence(context.Background(), c, c.Price, false); ok || why != "ml-live-disabled" {
		t.Fatalf("ML must fail closed as live authority: ok=%v why=%q", ok, why)
	}
}

func TestR132LiveMirrorIntentClaimDedup(t *testing.T) {
	s := &Server{}
	c := mirrorTestCandidate()
	if !s.claimLiveMirrorIntent(context.Background(), c) {
		t.Fatal("first in-memory claim should succeed")
	}
	if s.claimLiveMirrorIntent(context.Background(), c) {
		t.Fatal("second identical claim inside restart/dedup horizon must fail")
	}
}

func TestR132LiveMirrorProofDispatchCapPreservesFloor(t *testing.T) {
	q := liveMirrorQuote{Price: 0.40, Maker: false, Route: "taker"}
	fields, ok := liveMirrorProofDispatchFields(q, "proven:xvgap", 0.06, 0.03, 0.02, 0.005, time.Unix(100, 0))
	if !ok {
		t.Fatal("valid proof receipt must produce a dispatch cap")
	}
	got, typed := fields["max_all_in_unit"].(float64)
	want := 0.40 + 0.02 + 0.03 - 0.005
	if !typed || math.Abs(got-want) > 1e-12 {
		t.Fatalf("max_all_in_unit=%v want %.12f", fields["max_all_in_unit"], want)
	}
	if fields["mirror_proof_basis"] != "proven:xvgap" || fields["mirror_proof_lower"] != 0.03 || fields["mirror_proof_fee_pc"] != 0.02 {
		t.Fatalf("proof receipt lost authorization metadata: %#v", fields)
	}
	if _, ok := fields["mirror_proof_checked"].(string); !ok {
		t.Fatalf("proof receipt needs an RFC3339 timestamp: %#v", fields)
	}
	if _, ok := liveMirrorProofDispatchFields(q, "proven:xvgap", 0.06, 0.004, 0.02, 0.005, time.Now()); ok {
		t.Fatal("lower bound below the required edge floor must not create a dispatch cap")
	}
}

func TestR132LiveMirrorSessionPlacementGuardCoversBothVenues(t *testing.T) {
	now := time.Now()
	s := &Server{
		kalPlaced: map[string]time.Time{"KXR132-MIRROR": now.Add(-7 * time.Hour)},
		pusPlaced: map[string]pusPlaceRec{"order-1": {slug: "pus-r132", outcome: "YES", at: now}},
		pusAmbig:  map[string]time.Time{"pus-ambiguous": now.Add(-time.Hour)},
	}
	if got := s.liveMirrorSessionConflict(mirrorTestCandidate()); got != "existing-kalshi-session-placement" {
		t.Fatalf("Kalshi placement from this process session must block regardless of feed lag/age: %q", got)
	}
	for slug, want := range map[string]string{
		"pus-r132":      "existing-polyus-session-placement",
		"pus-ambiguous": "existing-polyus-ambiguous-session-placement",
	} {
		c := mirrorTestCandidate()
		c.Platform, c.Ticker = "polyus", slug
		if got := s.liveMirrorSessionConflict(c); got != want {
			t.Fatalf("session guard for %s=%q want %q", slug, got, want)
		}
	}
}

func TestR132LiveMirrorKalshiGuardUsesFreshPositionAndOrderReads(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	signer, err := kalshi.NewSigner("r132-test", keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	var hits atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/portfolio/positions":
			hits.Add(1)
			_, _ = w.Write([]byte(`{"market_positions":[{"ticker":"KXR132-MIRROR","position_fp":"1.00"}],"cursor":""}`))
		case "/portfolio/orders":
			hits.Add(1)
			_, _ = w.Write([]byte(`{"orders":[],"cursor":""}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()
	s := &Server{kal: kalshi.NewClient(ts.URL, signer, 10000, 2*time.Second)}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if got := s.liveMirrorAccountGuard(ctx, mirrorTestCandidate()); got != "existing-kalshi-position" {
		t.Fatalf("fresh account guard=%q want existing position", got)
	}
	if hits.Load() != 2 {
		t.Fatalf("guard must freshly read both positions and resting orders, hits=%d", hits.Load())
	}

	if got := liveMirrorKalshiStateConflict(mirrorTestCandidate(), nil, []kalshi.Order{{Ticker: "KXR132-MIRROR"}}); got != "existing-kalshi-resting-order" {
		t.Fatalf("resting-order guard=%q", got)
	}
}

func TestR132LiveMirrorPolyUSGuardRequiresFreshCacheAndBlocksExposure(t *testing.T) {
	c := mirrorTestCandidate()
	c.Platform, c.Ticker = "polyus", "pus-r132"
	s := &Server{
		polyUSAuth: new(polymarketus.Client),
		pusLiveAt:  time.Now(),
		pusPosC:    []polymarketus.PUSPosition{{Slug: c.Ticker, Net: 2}},
	}
	if got := s.liveMirrorAccountGuard(context.Background(), c); got != "existing-polyus-position" {
		t.Fatalf("position guard=%q", got)
	}
	s.pusPosC = nil
	s.pusOrdsC = []polymarketus.PUSOrder{{Slug: c.Ticker, Leaves: 1, State: "ORDER_STATE_NEW"}}
	if got := s.liveMirrorAccountGuard(context.Background(), c); got != "existing-polyus-resting-order" {
		t.Fatalf("resting-order guard=%q", got)
	}
	s.pusOrdsC = nil
	if got := s.liveMirrorAccountGuard(context.Background(), c); got != "" {
		t.Fatalf("fresh known-flat account should pass, got %q", got)
	}
	s.pusLiveAt = time.Now().Add(-liveMirrorPUSRESTCacheTTL - time.Second)
	if got := s.liveMirrorAccountGuard(context.Background(), c); got != "polyus-account-cache-stale" {
		t.Fatalf("stale account truth must fail closed, got %q", got)
	}
	s.pusLiveAt = time.Now()
	s.pusErrC = "position refresh failed"
	if got := s.liveMirrorAccountGuard(context.Background(), c); got != "polyus-account-cache-error" {
		t.Fatalf("errored keep-last-good account cache must fail closed, got %q", got)
	}
}

func TestR132LiveAutoProofCapAndOrderOwnership(t *testing.T) {
	no := false
	if liveOrderMLManaged(true, &no) {
		t.Fatal("explicit proven-strategy ownership must override legacy auto=>ML ownership")
	}
	if !liveOrderMLManaged(true, nil) || liveOrderMLManaged(false, nil) {
		t.Fatal("legacy nil ownership must equal the auto flag")
	}
	if why := liveAutoAllInReason(true, 0, .40, .01, 1); why == "" {
		t.Fatal("AUTO without a proof cap must fail closed")
	}
	if why := liveAutoAllInReason(true, .41, .40, .01, 1); why != "" {
		t.Fatalf("exact all-in boundary should pass: %s", why)
	}
	if why := liveAutoAllInReason(true, .41, .400001, .01, 1); why == "" {
		t.Fatal("final all-in above proof cap must fail")
	}
	if why := liveAutoAllInReason(false, 0, .90, .05, 1); why != "" {
		t.Fatalf("manual order must not require an AUTO proof cap: %s", why)
	}
}
