package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestProperScoreVectorsPreserveRawAndOneSidedScale(t *testing.T) {
	y, n, qty, ok := properScoreVector("brier", .70, .50)
	if !ok || math.Abs(y-.4) > 1e-12 || math.Abs(n+.4) > 1e-12 || math.Abs(qty-.8) > 1e-12 {
		t.Fatalf("brier vector=(%v,%v) qty=%v ok=%v", y, n, qty, ok)
	}
	y, n, qty, ok = properScoreVector("log", .70, .50)
	wantY, wantN := math.Log(.7/.5), math.Log(.3/.5)
	if !ok || math.Abs(y-wantY) > 1e-12 || math.Abs(n-wantN) > 1e-12 || math.Abs(qty-(wantY-wantN)) > 1e-12 {
		t.Fatalf("log vector=(%v,%v) qty=%v", y, n, qty)
	}
}

func TestProperScoreCandidateUsesSideSpecificBookAndDeadZone(t *testing.T) {
	book := properScoreBook{yesBid: .45, yesAsk: .50, yesBidDepth: 3, yesAskDepth: 4,
		noBid: .50, noAsk: .55, noBidDepth: 4, noAskDepth: 3, tick: .01, lot: 1,
		yesBids: []properbetting.Level{{Price: .45, Quantity: 3, Tick: .01}},
		yesAsks: []properbetting.Level{{Price: .50, Quantity: 4, Tick: .01}},
		noBids:  []properbetting.Level{{Price: .50, Quantity: 4, Tick: .01}},
		noAsks:  []properbetting.Level{{Price: .55, Quantity: 3, Tick: .01}}}
	c, reason := properScoreRawCandidate("brier", .70, book)
	if reason != "" || c.side != "YES" || math.Abs(c.canonical-.8) > 1e-12 || c.price != .50 || c.depth != 4 {
		t.Fatalf("YES candidate=%+v reason=%q", c, reason)
	}
	c, reason = properScoreRawCandidate("log", .30, book)
	if reason != "" || c.side != "NO" || c.price != .55 || math.Abs(c.qYes-.45) > 1e-12 || c.depth != 3 {
		t.Fatalf("NO candidate=%+v reason=%q", c, reason)
	}
	_, reason = properScoreRawCandidate("spherical", .47, book)
	if reason != "inside_bid_ask_dead_zone" {
		t.Fatalf("dead-zone reason=%q", reason)
	}
	incomplete := book
	incomplete.noBids = nil
	if _, reason := properScoreRawCandidate("brier", .70, incomplete); reason != "complete_outcome_set_book_unavailable" {
		t.Fatalf("canonical shift admitted without the complete outcome-set book: %q", reason)
	}
}

func TestProperScorePortfolioIsOneShareUnitAndCapacityIsSeparate(t *testing.T) {
	rows := []properScorePrepared{
		{candidates: map[string]properScoreCandidate{"brier": {side: "YES", canonical: 3}}, abstentions: map[string]string{"brier": ""}},
		{candidates: map[string]properScoreCandidate{"brier": {side: "NO", canonical: 1}}, abstentions: map[string]string{"brier": ""}},
		{candidates: map[string]properScoreCandidate{"brier": {}}, abstentions: map[string]string{"brier": "inside_bid_ask_dead_zone"}},
	}
	w, err := properScoreNormalizedWeights(rows, "brier")
	if err != nil || len(w) != 3 || math.Abs(math.Abs(w[0])+math.Abs(w[1])+math.Abs(w[2])-1) > 1e-12 ||
		w[0] <= 0 || w[1] >= 0 || w[2] != 0 {
		t.Fatalf("bankroll-free normalized weights=%v err=%v", w, err)
	}
	cohort := properScoreCohort("momentum", "brier", "ml-book-v2-calibrated", "v1", "kalshi", "YES", "ws", "fee")
	if !strings.Contains(cohort, "scale=l1-one-unit-plus-one-share-capacity-v1") ||
		strings.Contains(cohort, "1000") || strings.Contains(strings.ToLower(cohort), "bankroll") {
		t.Fatalf("cohort does not freeze bankroll-free scale: %s", cohort)
	}
}

func TestProperScoreBoundForecastsRotatesEveryUniqueForecastBeforeRepeating(t *testing.T) {
	rows := make([]properScoreForecast, 0, 497)
	for i := 0; i < 496; i++ {
		rows = append(rows, properScoreForecast{Platform: "kalshi",
			Ticker: fmt.Sprintf("KX-FAIR-%03d", i), Side: "YES", SignalType: "fixture"})
	}
	// A duplicate source row may not consume another coordinate or another page slot.
	rows = append(rows, rows[17])
	const limit = properScoreCycleCap
	base := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	seen := map[string]int{}
	pages := (496 + limit - 1) / limit
	for cycle := 0; cycle < pages; cycle++ {
		slot := base.Add(time.Duration(cycle) * properScoreSelectionCadence).Format(time.RFC3339)
		page := properScoreBoundForecasts(rows, slot, limit)
		if len(page) == 0 || len(page) > limit {
			t.Fatalf("cycle %d page size=%d, want 1..%d", cycle, len(page), limit)
		}
		again := properScoreBoundForecasts(rows, slot, limit)
		if len(again) != len(page) {
			t.Fatalf("same slot changed page size: %d vs %d", len(page), len(again))
		}
		for i := range page {
			key := properScoreForecastKey(page[i])
			if key != properScoreForecastKey(again[i]) {
				t.Fatalf("same slot selection is not deterministic at %d: %q vs %q", i, key,
					properScoreForecastKey(again[i]))
			}
			seen[key]++
		}
	}
	if len(seen) != 496 {
		t.Fatalf("one five-minute rotation covered %d/496 unique forecasts", len(seen))
	}
	for key, count := range seen {
		if count != 1 {
			t.Fatalf("forecast %q was selected %d times before full rotation", key, count)
		}
	}
}

func TestProperScoreSelectBookPageCapsOnlyAfterBookEligibility(t *testing.T) {
	rows := []properScoreForecast{
		{Platform: "kalshi", Ticker: "KX-MISSING-A", Side: "YES", SignalType: "fixture", PWin: 0.55},
		{Platform: "kalshi", Ticker: "KX-READY-A", Side: "YES", SignalType: "fixture", PWin: 0.55},
		{Platform: "polyus", Ticker: "PUS-READY-B", Side: "NO", SignalType: "fixture", PWin: 0.45},
		{Platform: "kalshi", Ticker: "KX-MISSING-B", Side: "YES", SignalType: "fixture", PWin: 0.55},
		{Platform: "kalshi", Ticker: "KX-READY-C", Side: "YES", SignalType: "fixture", PWin: 0.55},
		{Platform: "kalshi", Ticker: "KX-MISSING-C", Side: "YES", SignalType: "fixture", PWin: 0.55},
	}
	// A repeated source forecast must not consume another lookup, eligibility count, or page slot.
	rows = append(rows, rows[1])
	// Invalid coordinates are rejected before touching the book lookup.
	rows = append(rows, properScoreForecast{Platform: "polyint", Ticker: "PI-BLOCKED", Side: "YES", PWin: 0.5})

	ready := map[string]bool{"KX-READY-A": true, "PUS-READY-B": true, "KX-READY-C": true}
	lookups := map[string]int{}
	page := properScoreSelectBookPage(rows, "2026-07-15T12:00:00Z", 2,
		func(_ string, ticker string) (properScoreBook, bool) {
			lookups[ticker]++
			if !ready[ticker] {
				return properScoreBook{}, false
			}
			return properScoreBook{yesBid: 0.40, yesAsk: 0.60}, true
		})
	if page.available != 3 || page.noBook != 3 || page.invalid != 1 {
		t.Fatalf("book page counts available=%d no_book=%d invalid=%d, want 3/3/1",
			page.available, page.noBook, page.invalid)
	}
	if len(page.forecasts) < 1 || len(page.forecasts) > 2 || page.pages != 2 {
		t.Fatalf("selected=%d pages=%d, want one coherent page of 1..2 rows across 2 pages",
			len(page.forecasts), page.pages)
	}
	for _, forecast := range page.forecasts {
		key := properScoreForecastKey(forecast)
		if _, ok := page.books[key]; !ok {
			t.Fatalf("selected forecast %q lacks its exact checked book", key)
		}
		if _, ok := page.latencyMS[key]; !ok {
			t.Fatalf("selected forecast %q lacks its lookup latency", key)
		}
	}
	for ticker, count := range lookups {
		if count != 1 {
			t.Fatalf("ticker %q looked up %d times, want one deduplicated lookup", ticker, count)
		}
	}
	if _, ok := lookups["PI-BLOCKED"]; ok {
		t.Fatal("invalid platform reached the book lookup")
	}
}

func TestProperScoreCoherentPageDoesNotMixVenueOrSettlementHour(t *testing.T) {
	rows := []properScoreForecast{
		{Platform: "kalshi", Ticker: "K-EARLY-1", Side: "YES", SignalType: "fixture", ResolveHours: .20},
		{Platform: "kalshi", Ticker: "K-EARLY-2", Side: "NO", SignalType: "fixture", ResolveHours: .70},
		{Platform: "kalshi", Ticker: "K-LATE", Side: "YES", SignalType: "fixture", ResolveHours: 1.20},
		{Platform: "polyus", Ticker: "P-EARLY", Side: "YES", SignalType: "fixture", ResolveHours: .30},
	}
	selected, _, pages := properScoreCoherentPage(rows, "2026-07-15T12:00:00Z", 8)
	if pages != 3 || len(selected) == 0 {
		t.Fatalf("coherent pages=%d selected=%d, want 3 non-empty venue/hour pages", pages, len(selected))
	}
	platform := selected[0].Platform
	base := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	bucket := base.Add(time.Duration(selected[0].ResolveHours * float64(time.Hour))).Truncate(time.Hour)
	for _, row := range selected[1:] {
		gotBucket := base.Add(time.Duration(row.ResolveHours * float64(time.Hour))).Truncate(time.Hour)
		if row.Platform != platform || !gotBucket.Equal(bucket) {
			t.Fatalf("one batch mixed venue/hour: first=%s/%s row=%s/%s", platform, bucket,
				row.Platform, gotBucket)
		}
	}
}

func TestProperScoreCoherentPagesVisitStablePoolBeforeRepeating(t *testing.T) {
	rows := []properScoreForecast{
		{Platform: "kalshi", Ticker: "K-EARLY-1", Side: "YES", SignalType: "fixture", ResolveHours: .20},
		{Platform: "kalshi", Ticker: "K-EARLY-2", Side: "NO", SignalType: "fixture", ResolveHours: .30},
		{Platform: "kalshi", Ticker: "K-EARLY-3", Side: "YES", SignalType: "fixture", ResolveHours: .40},
		{Platform: "kalshi", Ticker: "K-LATE", Side: "YES", SignalType: "fixture", ResolveHours: 1.20},
		{Platform: "polyus", Ticker: "P-EARLY", Side: "YES", SignalType: "fixture", ResolveHours: .30},
	}
	base := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	_, _, pages := properScoreCoherentPage(rows, base.Format(time.RFC3339), 2)
	if pages != 4 {
		t.Fatalf("coherent page count=%d, want 4", pages)
	}
	seen := map[string]int{}
	for cycle := 0; cycle < pages; cycle++ {
		slot := base.Add(time.Duration(cycle) * properScoreSelectionCadence).Format(time.RFC3339)
		page, _, gotPages := properScoreCoherentPage(rows, slot, 2)
		if gotPages != pages || len(page) == 0 {
			t.Fatalf("cycle %d pages=%d rows=%d", cycle, gotPages, len(page))
		}
		for _, row := range page {
			seen[row.Ticker]++
		}
	}
	if len(seen) != len(rows) {
		t.Fatalf("one stable-pool cycle visited %d/%d forecasts: %v", len(seen), len(rows), seen)
	}
	for ticker, count := range seen {
		if count != 1 {
			t.Fatalf("forecast %s visited %d times before full cycle", ticker, count)
		}
	}
}

func TestProperScoreSelectBookPageReportsHonestNoBookStarvation(t *testing.T) {
	rows := []properScoreForecast{
		{Platform: "kalshi", Ticker: "KX-NO-BOOK", Side: "YES", SignalType: "fixture", PWin: 0.5},
		{Platform: "polyus", Ticker: "PUS-NO-BOOK", Side: "NO", SignalType: "fixture", PWin: 0.5},
	}
	page := properScoreSelectBookPage(rows, "2026-07-15T12:00:00Z", 8,
		func(_, _ string) (properScoreBook, bool) { return properScoreBook{}, false })
	if page.available != 0 || len(page.forecasts) != 0 || page.noBook != 2 || page.pages != 0 {
		t.Fatalf("all-missing page=%+v, want available=0 selected=0 no_book=2 pages=0", page)
	}
}

func TestProperScoreFastLaneUsesFundedHorizonWithoutDeletingLongResearchRows(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	rows := []properScoreForecast{
		{Ticker: "KXGAME-SOON", Title: "Game soon", ResolveHours: 4},
		{Ticker: "KXGAME-LATER", Title: "Game later", ResolveHours: 4.01},
		{Ticker: "KXBTC15M-SOON", Title: "Bitcoin soon", ResolveHours: 2},
		{Ticker: "KXBTC15M-LATER", Title: "Bitcoin later", ResolveHours: 2.01},
		// resolve_hours is immutable time-to-close at signal time. These long-at-signal rows have
		// since entered the funded window and must not remain permanently excluded.
		{Ticker: "KXGAME-AGED-IN", Title: "Game now soon", SignalTS: now.Add(-17 * time.Hour).Format(time.RFC3339), ResolveHours: 20},
		{Ticker: "KXBTC15M-AGED-IN", Title: "Bitcoin now soon", SignalTS: now.Add(-9 * time.Hour).Format(time.RFC3339), ResolveHours: 10},
		{Ticker: "KXGAME-EXPIRED", Title: "Game expired", SignalTS: now.Add(-5 * time.Hour).Format(time.RFC3339), ResolveHours: 4},
		{Ticker: "KXUNKNOWN", ResolveHours: 0},
	}
	got := properScoreCurrentHorizonForecasts(rows, 4, 2, now)
	if len(got) != 4 || got[0].Ticker != "KXGAME-SOON" || got[1].Ticker != "KXBTC15M-SOON" ||
		got[2].Ticker != "KXGAME-AGED-IN" || got[3].Ticker != "KXBTC15M-AGED-IN" {
		t.Fatalf("current-horizon proper-score rows=%+v, want static and newly-entered regular<=4h/crypto<=2h", got)
	}
	if math.Abs(got[2].ResolveHours-3) > 1e-9 || math.Abs(got[3].ResolveHours-1) > 1e-9 {
		t.Fatalf("decision rows retained stale signal-time capital clocks: regular=%v crypto=%v",
			got[2].ResolveHours, got[3].ResolveHours)
	}
	if len(rows) != 8 {
		t.Fatal("horizon selection mutated the source research universe")
	}
}

func TestProperScoreManifestSlotAllowsChangedSameHourSweepWithoutMutatingFirst(t *testing.T) {
	ctx := context.Background()
	s := testServer(t)
	hour := time.Date(2026, 7, 14, 1, 0, 0, 0, time.UTC).Format(time.RFC3339)
	mk := func(ticker string) properScorePrepared {
		return properScorePrepared{platform: "kalshi", originSide: "YES",
			forecast: properScoreForecast{Ticker: ticker, Side: "YES", SignalType: "book-v2"}}
	}
	first := []properScorePrepared{mk("KX-SLOT-A"), mk("KX-SLOT-B")}
	reordered := []properScorePrepared{first[1], first[0]}
	changedSet := []properScorePrepared{mk("KX-SLOT-A")}
	slot1 := properScoreManifestSlot(hour, "ml-book-v2-calibrated", "model-v1", first)
	if same := properScoreManifestSlot(hour, "ml-book-v2-calibrated", "model-v1", reordered); same != slot1 {
		t.Fatalf("coordinate reordering changed immutable slot: %q != %q", same, slot1)
	}
	slot2 := properScoreManifestSlot(hour, "ml-book-v2-calibrated", "model-v1", changedSet)
	slot3 := properScoreManifestSlot(hour, "ml-book-v2-calibrated", "model-v2", changedSet)
	if slot1 == slot2 || slot2 == slot3 || slot1 == slot3 {
		t.Fatalf("same-hour candidate/model changes collided: %q %q %q", slot1, slot2, slot3)
	}
	coords := func(rows []properScorePrepared, version string) []storage.ProperScoreCoordinate {
		out := make([]storage.ProperScoreCoordinate, 0, len(rows))
		for _, row := range rows {
			out = append(out, storage.ProperScoreCoordinate{Platform: row.platform,
				Ticker: row.forecast.Ticker, ForecastOriginSide: row.originSide,
				ForecastSignal: row.forecast.SignalType, ForecastSource: "ml-book-v2-calibrated",
				ForecastVersion: version, Route: "taker"})
		}
		return out
	}
	register := func(slot, version string, rows []properScorePrepared) bool {
		_, inserted, err := s.store.RegisterProperScoreManifest(ctx, storage.ProperScoreManifest{
			Created: time.Now(), Slot: slot, Transform: "log", StrategyMode: "fundamental",
			ForecastSource: "ml-book-v2-calibrated", ForecastVersion: version,
			Coordinates: coords(rows, version),
		})
		if err != nil {
			t.Fatalf("register %q: %v", slot, err)
		}
		return inserted
	}
	if !register(slot1, "model-v1", first) || !register(slot2, "model-v1", changedSet) ||
		!register(slot3, "model-v2", changedSet) {
		t.Fatal("changed same-hour sweeps did not append distinct immutable manifests")
	}
	if register(slot1, "model-v1", reordered) {
		t.Fatal("identical reordered retry inserted a duplicate manifest")
	}
	if _, _, err := s.store.RegisterProperScoreManifest(ctx, storage.ProperScoreManifest{
		Created: time.Now(), Slot: slot1, Transform: "log", StrategyMode: "fundamental",
		ForecastSource: "ml-book-v2-calibrated", ForecastVersion: "model-v1",
		Coordinates: coords(changedSet, "model-v1"),
	}); err == nil {
		t.Fatal("first immutable manifest accepted an in-place coordinate mutation")
	}
}

func TestProperScorePolyUSConstraintsUseFreshCompleteUniverseWithoutAuthREST(t *testing.T) {
	s := &Server{}
	s.pusSweep = []polymarketus.Market{{Slug: "fresh-market", EP3Status: "OPEN",
		TickSize: .001, MinimumQty: .25}}
	s.pusSweepAt = time.Now()
	// polyUSAuth is intentionally nil. A helper that falls through to the old per-ticker REST path
	// cannot pass this test; the fresh complete-universe receipt is sufficient and authoritative.
	tick, lot, ok := s.properScorePUSConstraints("FRESH-MARKET")
	if !ok || tick != .001 || lot != .25 {
		t.Fatalf("fresh cached constraints=(%v,%v,%v)", tick, lot, ok)
	}
	s.pusSweepAt = time.Now().Add(-polymarketus.MarketsRESTLifecycleMaxAge - time.Second)
	if _, _, ok := s.properScorePUSConstraints("fresh-market"); ok {
		t.Fatal("stale universe receipt authorized current constraints")
	}
}

func TestProperScoreBoundedWorkPolicyMatchesSharedLaneDeadline(t *testing.T) {
	if properScoreCycleCap <= 0 || properScoreCycleCap > 8 {
		t.Fatalf("proper-score bounded five-minute write page=%d; oversized page returned", properScoreCycleCap)
	}
	found := false
	for _, lane := range researchSystemHeavyLaneSpecs {
		if lane.name == "proper-score" {
			found = true
			if lane.timeout != properScoreLaneBudget {
				t.Fatalf("collector budget=%v but shared lane=%v", properScoreLaneBudget, lane.timeout)
			}
		}
	}
	if !found || !strings.Contains(properScoreBudgetReason, "35s") ||
		!strings.Contains(properScoreBudgetReason, "5-minute") {
		t.Fatalf("proper-score lane/budget reason missing: found=%v reason=%q", found, properScoreBudgetReason)
	}
}

func TestProperScoreBudgetReserveStopsBeforeManifestDeadlineStarvation(t *testing.T) {
	started := time.Now().UTC().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), started.Add(properScoreLaneBudget))
	defer cancel()
	lastSafe := started.Add(properScoreLaneBudget - properScoreReceiptReserve - properScorePhaseReserve)
	if !properScoreCanStartPhase(ctx, started, lastSafe) {
		t.Fatal("exact bounded phase reserve was rejected")
	}
	if properScoreCanStartPhase(ctx, started, lastSafe.Add(time.Nanosecond)) {
		t.Fatal("collector could freeze a manifest without phase + receipt reserve")
	}

	shortCtx, shortCancel := context.WithDeadline(context.Background(), started.Add(15*time.Second))
	defer shortCancel()
	shortLastSafe := started.Add(15*time.Second - properScoreReceiptReserve - properScorePhaseReserve)
	if !properScoreCanStartPhase(shortCtx, started, shortLastSafe) ||
		properScoreCanStartPhase(shortCtx, started, shortLastSafe.Add(time.Nanosecond)) {
		t.Fatal("parent deadline did not tighten the proper-score work cutoff")
	}
}

func TestProperScoreExactCatalogIdentityClosesRuntimeSixtyWriteGap(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	tickers := []string{"KXNPBGAME-26JUL140000FUKHOK-FUK", "KXNPBGAME-26JUL140000FUKHOK-HOK"}
	rows := make([]storage.CatalogRow, 0, len(tickers))
	for _, ticker := range tickers {
		rows = append(rows, storage.CatalogRow{Venue: "kalshi", Ticker: ticker,
			EventKey: "KXNPBGAME-26JUL140000FUKHOK", Kind: "winner", Title: ticker})
	}
	if err := s.store.UpsertMarketCatalog(ctx, rows); err != nil {
		t.Fatal(err)
	}
	for _, ticker := range tickers {
		identity, ok, err := s.ensureExactCatalogCanonicalInstrument(ctx, "kalshi", ticker)
		if err != nil || !ok || identity.IdentityStatus != "unverified" || identity.EventVersion <= 0 ||
			identity.PayoffVersion <= 0 {
			t.Fatalf("exact catalog ensure %s: identity=%+v ok=%v err=%v", ticker, identity, ok, err)
		}
	}
	// Runtime had two missing tickers x three transforms x two modes. Each action attempted four
	// matched comparators plus one common observation: exactly the 48+12=60 rejected writes.
	if affected := len(tickers) * 3 * 2 * (4 + 1); affected != 60 {
		t.Fatalf("runtime regression fixture covers %d writes, want 60", affected)
	}
}

func TestProperScoreExactCatalogIdentityPreservesVerifiedAndFailsClosedWithoutCatalog(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	eventID, payoffID, ticker := "verified:event", "verified:event|yes", "KX-VERIFIED"
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{EventID: eventID,
		EventType: "fixture", Domain: "test", SourceArtifact: "fixture", SourceClockID: "fixture"}},
		[]storage.CanonicalPayoffSpec{{PayoffID: payoffID, EventID: eventID, PredicateJSON: `{}`,
			PayoutFloor: 0, PayoutCeiling: 1, SourceArtifact: "fixture", IdentityStatus: "verified"}},
		[]storage.CanonicalInstrumentSpec{{Venue: "kalshi", Ticker: ticker, EventID: eventID,
			PayoffID: payoffID, NativeSide: "YES", Orientation: "same", IdentityStatus: "verified"}}); err != nil {
		t.Fatal(err)
	}
	before, ok, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", ticker)
	if err != nil || !ok {
		t.Fatalf("verified fixture missing: %+v %v %v", before, ok, err)
	}
	if err := s.store.UpsertMarketCatalog(ctx, []storage.CatalogRow{{Venue: "kalshi", Ticker: ticker,
		EventKey: "different-catalog-container", Kind: "unknown", Title: "unverified fallback"}}); err != nil {
		t.Fatal(err)
	}
	after, ok, err := s.ensureExactCatalogCanonicalInstrument(ctx, "kalshi", ticker)
	if err != nil || !ok || after.IdentityStatus != "verified" || after.InstrumentVersion != before.InstrumentVersion ||
		after.EventID != before.EventID || after.PayoffID != before.PayoffID {
		t.Fatalf("verified identity was replaced: before=%+v after=%+v ok=%v err=%v", before, after, ok, err)
	}
	if missing, ok, err := s.ensureExactCatalogCanonicalInstrument(ctx, "kalshi", "KX-NOT-IN-CATALOG"); err != nil || ok || missing != (storage.CurrentCanonicalInstrument{}) {
		t.Fatalf("missing catalog identity did not fail closed: %+v ok=%v err=%v", missing, ok, err)
	}
}

func TestProperScoreExecutableSurvivorsRenormalizeToOneL1(t *testing.T) {
	rows := []properScorePrepared{
		{candidates: map[string]properScoreCandidate{"log": {side: "YES", canonical: 3}}, abstentions: map[string]string{"log": ""}},
		{candidates: map[string]properScoreCandidate{"log": {side: "YES", canonical: 1}}, abstentions: map[string]string{"log": ""}},
		{candidates: map[string]properScoreCandidate{"log": {side: "NO", canonical: 2}}, abstentions: map[string]string{"log": ""}},
	}
	evaluated := []properScoreCandidate{
		{side: "YES", canonical: 3, qty: 1, normalized: .5},
		{side: "YES", canonical: 1, qty: 0, normalized: 1.0 / 6},
		{side: "NO", canonical: 2, qty: 1, normalized: 1.0 / 3},
	}
	reasons := []string{"", "exact_fee_unavailable", ""}
	weights, err := properScoreExecutableWeights(rows, "log", evaluated, reasons)
	if err != nil || len(weights) != 3 {
		t.Fatalf("executable weights=%v err=%v", weights, err)
	}
	l1 := math.Abs(weights[0]) + math.Abs(weights[1]) + math.Abs(weights[2])
	if math.Abs(l1-1) > 1e-12 || math.Abs(weights[0]-.6) > 1e-12 ||
		weights[1] != 0 || math.Abs(weights[2]+.4) > 1e-12 {
		t.Fatalf("survivor weights=%v L1=%v; rejected coordinate retained budget", weights, l1)
	}
	if _, err := properScoreExecutableWeights(rows, "log", evaluated[:2], reasons); err == nil {
		t.Fatal("dimension mismatch accepted")
	}
	if _, err := properScoreExecutableWeights(rows, "log", evaluated,
		[]string{"no edge", "no fee", "no depth"}); err == nil {
		t.Fatal("empty executable vector accepted")
	}
}

func TestProperScoreBriefWarmsUntilFullVectorSlotCompletes(t *testing.T) {
	report := map[string]any{
		"transforms": map[string]any{
			"brier": map[string]any{"normalized_executable_settled": 1,
				"normalized_executable_settled_l1":      .6,
				"normalized_executable_completed_slots": 0,
				"l1_weighted_realized_net":              .25},
		},
		"comparators": map[string]any{"metrics": map[string]any{
			"max-margin": map[string]any{"completed_slots": 0, "settled_l1": 1.0,
				"l1_weighted_realized_net": .25},
		}},
	}
	got := properScoreBriefLine(report)
	for _, want := range []string{"Brier · 0", "Log · 0", "Spherical · 0"} {
		if !strings.Contains(got, want) {
			t.Fatalf("partial vector was presented as effective; missing %q in %s", want, got)
		}
	}
	if strings.Contains(got, "+41.7") || strings.Contains(got, "+25.0") {
		t.Fatalf("partial vector leaked into effectiveness: %s", got)
	}
	if strings.Contains(got, "max-margin") {
		t.Fatalf("dashboard-only comparator leaked into compact briefing: %s", got)
	}
	report["transforms"].(map[string]any)["brier"] = map[string]any{
		"normalized_executable_settled":         1,
		"normalized_executable_settled_l1":      .6,
		"normalized_executable_completed_slots": 0,
		"l1_weighted_realized_net":              .25,
		"manifest_incomplete_slots":             1,
		"manifest_missing_coordinates":          2,
	}
	got = properScoreBriefLine(report)
	if !strings.Contains(got, "Brier · 0") || strings.Contains(got, "incomplete slots") || strings.Contains(got, "missing coords") {
		t.Fatalf("compact brief must keep incomplete-coordinate diagnostics on the dashboard: %s", got)
	}
}

func TestProperScoreKalshiSourceClockUsesOnlySequencedReceiptFallback(t *testing.T) {
	received := time.Date(2026, 7, 12, 18, 0, 0, 123, time.UTC)
	clock, clockAt, ok := properScoreKalshiSourceClock(4, 9, 17, time.Time{}, received)
	if !ok || !clockAt.Equal(received) || !strings.Contains(clock, "g4:s9:q17:received:") ||
		!strings.Contains(clock, "venue-time-omitted") {
		t.Fatalf("sequenced timestamp-omitted frame starved: clock=%q at=%v ok=%v", clock, clockAt, ok)
	}
	if clock, _, ok := properScoreKalshiSourceClock(4, 9, 0, time.Time{}, received); ok || clock != "" {
		t.Fatalf("unsequenced arrival refreshed authority: clock=%q ok=%v", clock, ok)
	}
	source := received.Add(-time.Second)
	clock, clockAt, ok = properScoreKalshiSourceClock(4, 9, 18, source, received)
	if !ok || !clockAt.Equal(source) || !strings.Contains(clock, ":source:") ||
		strings.Contains(clock, "venue-time-omitted") {
		t.Fatalf("venue source clock was not preferred: clock=%q at=%v ok=%v", clock, clockAt, ok)
	}
}

func TestProperScoreOneShareObservationIsPreregisteredNotAuthority(t *testing.T) {
	s := testServer(t)
	ctx, now := context.Background(), time.Now().UTC()
	eventID, payoffID, ticker := "venue:kalshi:KXPROP-T", "payoff:kalshi:KXPROP-T:YES", "KXPROP-T"
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{EventID: eventID,
		EventType: "fixture", Domain: "test", SettlementSource: "fixture", SourceArtifact: "fixture",
		SourceClockID: "fixture-clock", OutcomeSetStatus: "complete", EvidenceJSON: `{}`}},
		[]storage.CanonicalPayoffSpec{{PayoffID: payoffID, EventID: eventID, PredicateJSON: `{}`,
			SettlementSource: "fixture", SourceArtifact: "fixture", IdentityStatus: "verified",
			PayoutFloor: 0, PayoutCeiling: 1, EvidenceJSON: `{}`}},
		[]storage.CanonicalInstrumentSpec{{Venue: "kalshi", Ticker: ticker, EventID: eventID,
			PayoffID: payoffID, NativeSide: "YES", Orientation: "same", MarketKind: "binary",
			SettlementSource: "fixture", RulesArtifact: "fixture", RulesHash: "rules",
			FeeAuthority: "fee-v1", IdentityStatus: "verified", EvidenceJSON: `{}`}}); err != nil {
		t.Fatal(err)
	}
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXPROP": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = now
	s.kalFeeMu.Unlock()
	book := properScoreBook{yesBid: .39, yesAsk: .40, yesBidDepth: 6, yesAskDepth: 8,
		noBid: .60, noAsk: .61, noBidDepth: 8, noAskDepth: 6, tick: .01, lot: 1,
		source: "kalshi_book_ws_full", sourceClock: "kalshi-book:g1:s1:q1:" + now.Format(time.RFC3339Nano), age: .1,
		yesBids: []properbetting.Level{{Price: .39, Quantity: 6, Tick: .01}},
		yesAsks: []properbetting.Level{{Price: .40, Quantity: 8, Tick: .01}},
		noBids:  []properbetting.Level{{Price: .60, Quantity: 8, Tick: .01}},
		noAsks:  []properbetting.Level{{Price: .61, Quantity: 6, Tick: .01}}}
	raw, why := properScoreRawCandidate("spherical", .75, book)
	if why != "" {
		t.Fatal(why)
	}
	candidate, why := s.properScoreEvaluate("kalshi", ticker, "spherical", .75, book, raw, 1)
	if why != "" || candidate.qty != 1 || candidate.expected <= 0 {
		t.Fatalf("candidate=%+v why=%q", candidate, why)
	}
	row := properScorePrepared{forecast: properScoreForecast{Ticker: ticker, Title: "fixture",
		SignalType: "book-ml", ResolveHours: 4}, platform: "kalshi", originSide: "YES",
		pYes: .75, latencyMS: 2, book: book}
	cohort := properScoreCohort("fundamental", "spherical", "ml-book-v2-calibrated", "v-test",
		"kalshi", "YES", book.source, candidate.feeSource)
	id, observationErr := s.properScoreSystemObservation(ctx, now, now.Truncate(time.Hour).Format(time.RFC3339),
		"fundamental", "spherical", row, candidate, cohort)
	if observationErr != nil || id <= 0 {
		t.Fatalf("proper-score one-share observation was not inserted: id=%d err=%v", id, observationErr)
	}
	var kind, storedCohort, inputs string
	var candidateBit, funded, paperAuthority, liveAuthority int
	var size, payoutLow, payoutHigh, netLow, netHigh float64
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT observation_kind,cohort,candidate,
funded,paper_authority,live_authority,size_units,payout_lower,payout_upper,net_lower,net_upper,inputs_json
FROM research_system_observations WHERE id=?`, id).Scan(&kind, &storedCohort, &candidateBit,
		&funded, &paperAuthority, &liveAuthority, &size, &payoutLow, &payoutHigh, &netLow, &netHigh,
		&inputs); err != nil {
		t.Fatal(err)
	}
	if kind != "candidate" || candidateBit != 1 || funded+paperAuthority+liveAuthority != 0 || size != 1 ||
		payoutLow != 0 || payoutHigh != 1 || netLow >= 0 || netHigh <= 0 || storedCohort != cohort ||
		!strings.Contains(inputs, `"promotion_expected_net_lower"`) || !strings.Contains(inputs, `"normalized_weight":1`) {
		t.Fatalf("unsafe or incomplete observation kind=%s candidate=%d authority=%d/%d/%d size=%v payout=[%v,%v] net=[%v,%v] cohort=%s inputs=%s",
			kind, candidateBit, funded, paperAuthority, liveAuthority, size, payoutLow, payoutHigh, netLow, netHigh, storedCohort, inputs)
	}
	slot := now.Truncate(time.Hour).Format(time.RFC3339)
	if _, _, err := s.store.RegisterProperScoreManifest(ctx, storage.ProperScoreManifest{
		Created: now, Slot: slot, Transform: "spherical", StrategyMode: "fundamental",
		ForecastSource: "ml-book-v2-calibrated", ForecastVersion: "v-test",
		Coordinates: []storage.ProperScoreCoordinate{{Platform: row.platform, Ticker: row.forecast.Ticker,
			ForecastOriginSide: row.originSide, ForecastSignal: row.forecast.SignalType,
			ForecastSource: "ml-book-v2-calibrated", ForecastVersion: "v-test", Route: "taker"}},
	}); err != nil {
		t.Fatalf("freeze comparator coordinate manifest: %v", err)
	}
	for _, arm := range []struct {
		name     string
		target   float64
		selected bool
	}{{"equal-share", .25, true}, {"max-margin", 1, true}, {"mode4-normalized", .40, true},
		{"no-trade", 0, false}} {
		if _, err := s.properScoreComparatorObservation(ctx, now, slot, "fundamental", "spherical", arm.name,
			arm.target, arm.selected, row, candidate, "ml-book-v2-calibrated", "v-test"); err != nil {
			t.Fatalf("comparator %s: %v", arm.name, err)
		}
	}
	var controls, comparatorCandidates, comparatorAuthority, distinctTimes, distinctEvents, distinctClocks int
	query := `SELECT COUNT(*),COALESCE(SUM(candidate),0),COUNT(DISTINCT observed_ts),
COALESCE(SUM(funded+paper_authority+live_authority),0),
COUNT(DISTINCT canonical_event_id),COUNT(DISTINCT source_clock_id)
FROM research_system_observations WHERE system_id='proper-score-executor'
 AND observation_kind='control' AND cohort LIKE 'proper-score-comparator-v1|%'`
	if err := s.store.DBForTest().QueryRowContext(ctx, query).Scan(&controls, &comparatorCandidates,
		&distinctTimes, &comparatorAuthority, &distinctEvents, &distinctClocks); err != nil {
		t.Fatal(err)
	}
	if controls != 4 || comparatorCandidates != 0 || comparatorAuthority != 0 || distinctTimes != 1 || distinctEvents != 1 ||
		distinctClocks != 1 {
		t.Fatalf("comparators are not same-clock/identity zero-authority controls: rows=%d candidates=%d authority=%d times=%d events=%d clocks=%d",
			controls, comparatorCandidates, comparatorAuthority, distinctTimes, distinctEvents, distinctClocks)
	}
	var settledControls int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*) FROM research_system_payoff_updates u
JOIN research_system_observations o ON o.id=u.observation_id
WHERE o.cohort LIKE 'proper-score-comparator-v1|%'`).Scan(&settledControls); err != nil {
		t.Fatal(err)
	}
	if settledControls != 1 {
		t.Fatalf("deterministic no-trade comparator was not terminal immediately: %d", settledControls)
	}
	report, err := s.store.ProperScoreReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	comparators := report["comparators"].(map[string]any)["metrics"].(map[string]any)
	noTrade := comparators["no-trade"].(map[string]any)
	if noTrade["settled_rows"].(int) != 1 || noTrade["completed_slots"].(int) != 1 ||
		noTrade["l1_weighted_realized_net"].(float64) != 0 {
		t.Fatalf("no-trade comparator metric=%+v", noTrade)
	}
}

func TestProperScoreSimulatedFundamentalAccumulatesMomentumRebalances(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	start := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	ticker := "KXSIM-T"
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXSIM": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	book := properScoreBook{yesBid: .39, yesAsk: .40, yesBidDepth: 8, yesAskDepth: 8,
		noBid: .60, noAsk: .61, noBidDepth: 8, noAskDepth: 8, tick: .01, lot: 1,
		source: "kalshi_book_ws_full", sourceClock: "kalshi-book:g1:s1:q1:source:" + start.Format(time.RFC3339Nano), age: .1,
		yesBids: []properbetting.Level{{Price: .39, Quantity: 8, Tick: .01}},
		yesAsks: []properbetting.Level{{Price: .40, Quantity: 8, Tick: .01}},
		noBids:  []properbetting.Level{{Price: .60, Quantity: 8, Tick: .01}},
		noAsks:  []properbetting.Level{{Price: .61, Quantity: 8, Tick: .01}}}
	row := properScorePrepared{forecast: properScoreForecast{Ticker: ticker, SignalType: "book-ml"},
		platform: "kalshi", originSide: "YES", pYes: .75, latencyMS: 1, book: book}
	raw, why := properScoreRawCandidate("brier", row.pYes, book)
	if why != "" {
		t.Fatal(why)
	}
	c, why := s.properScoreEvaluate("kalshi", ticker, "brier", row.pYes, book, raw, 1)
	if why != "" {
		t.Fatal(why)
	}
	cohort := properScoreCohort("fundamental", "brier", "ml-book-v2-calibrated", "v-sim",
		"kalshi", "YES", book.source, c.feeSource)
	run := func(at time.Time, mode string, row properScorePrepared, c properScoreCandidate) properScoreSimResult {
		t.Helper()
		got, err := s.simulateProperScoreImmediateTaker(ctx, at, at.Truncate(time.Hour).Format(time.RFC3339),
			mode, "brier", row, c, properScoreCohort(mode, "brier", "ml-book-v2-calibrated",
				"v-sim", "kalshi", c.side, book.source, c.feeSource),
			"ml-book-v2-calibrated", "v-sim")
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	f1, m1 := run(start, "fundamental", row, c), run(start, "momentum", row, c)
	f2, m2 := run(start.Add(time.Hour), "fundamental", row, c), run(start.Add(time.Hour), "momentum", row, c)
	if f1.post != 1 || f2.prior != 1 || f2.post != 2 || f2.status != "filled" ||
		m1.post != 1 || m2.prior != 1 || m2.post != 1 || m2.status != "noop" ||
		len(m2.legs) != 0 || cohort == "" {
		t.Fatalf("same-side fundamental/momentum f1=%+v f2=%+v m1=%+v m2=%+v", f1, f2, m1, m2)
	}

	// A momentum side flip must first sell the actually simulated YES fill, then buy NO. It may
	// never teleport from +1 to -1 or use the canceled target as prior state.
	row.pYes = .20
	raw, why = properScoreRawCandidate("brier", row.pYes, book)
	if why != "" {
		t.Fatal(why)
	}
	noCandidate, why := s.properScoreEvaluate("kalshi", ticker, "brier", row.pYes, book, raw, 1)
	if why != "" || noCandidate.side != "NO" {
		t.Fatalf("NO candidate=%+v why=%q", noCandidate, why)
	}
	m3 := run(start.Add(2*time.Hour), "momentum", row, noCandidate)
	if m3.prior != 1 || m3.target != -1 || m3.post != -1 || m3.filled != -2 ||
		m3.status != "filled" || len(m3.legs) != 2 ||
		m3.legs[0]["action"] != "SELL" || m3.legs[1]["action"] != "BUY" {
		t.Fatalf("momentum crossing did not use two exact routes: %+v", m3)
	}
	if actual, err := s.store.ProperScoreActualPosition(ctx, "momentum", "brier", "kalshi", ticker,
		"ml-book-v2-calibrated", "v-sim"); err != nil || actual != 0 {
		t.Fatalf("zero-authority simulation moved actual position: actual=%v err=%v", actual, err)
	}
	var rowsN, funded, paperAuthority, liveAuthority int
	query := "SELECT COUNT(*),COALESCE(SUM(funded),0),COALESCE(SUM(paper_authority),0)," +
		"COALESCE(SUM(live_authority),0) FROM research_proper_score_simulated_positions"
	if err := s.store.DBForTest().QueryRowContext(ctx, query).Scan(&rowsN, &funded, &paperAuthority,
		&liveAuthority); err != nil {
		t.Fatal(err)
	}
	if rowsN != 5 || funded+paperAuthority+liveAuthority != 0 {
		t.Fatalf("simulation ledger rows/authority=%d %d/%d/%d", rowsN, funded, paperAuthority, liveAuthority)
	}
}

func TestProperScoreForecastBoundIsDeterministicAndRotates(t *testing.T) {
	rows := make([]properScoreForecast, 50)
	for i := range rows {
		rows[i] = properScoreForecast{Platform: "kalshi", Ticker: string(rune('A' + i)), Side: "YES", SignalType: "x", PWin: .6}
	}
	a := properScoreBoundForecasts(rows, "2026-07-11T20:00:00Z", 10)
	b := properScoreBoundForecasts(rows, "2026-07-11T20:00:00Z", 10)
	c := properScoreBoundForecasts(rows, "2026-07-11T20:05:00Z", 10)
	if len(a) != 10 || len(b) != 10 || len(c) != 10 {
		t.Fatalf("bounded lengths %d %d %d", len(a), len(b), len(c))
	}
	for i := range a {
		if properScoreForecastKey(a[i]) != properScoreForecastKey(b[i]) {
			t.Fatal("same five-minute slot produced a different deterministic sample")
		}
	}
	same := true
	for i := range a {
		same = same && properScoreForecastKey(a[i]) == properScoreForecastKey(c[i])
	}
	if same {
		t.Fatal("five-minute slot did not rotate bounded coverage")
	}
}

func TestProperScoreCoverageAndLiveGateAreResearchOnly(t *testing.T) {
	seen := map[string]leaderboardCoverageRow{}
	for _, r := range researchSystemCoverageRows() {
		seen[r.Family] = r
	}
	for _, family := range []string{"proper-score-brier", "proper-score-log", "proper-score-spherical"} {
		r, ok := seen[family]
		if !ok || !r.ResearchOnly || r.LiveAuthorizes || r.Timed || r.State != "RESEARCH_ONLY_COLLECTING" {
			t.Fatalf("unsafe coverage %s: %+v", family, r)
		}
	}
	s := &Server{}
	ok, basis, _, _, _ := s.liveMirrorProof(context.Background(), liveMirrorCandidate{
		Source: "auto-cons-proper-score-brier", Platform: "kalshi", Ticker: "KX", Side: "YES"}, .5, false)
	if ok || basis != "proper-score-research-only" {
		t.Fatalf("proper score reached live proof: ok=%v basis=%q", ok, basis)
	}
}

func TestProperScoreWarmingManifestIsHealthyEmptyNotSchemaFailure(t *testing.T) {
	s := testServer(t)
	manifest := properScoreForecastFile{GeneratedAt: time.Now().Unix(), ModelStatus: "WARMING",
		ForecastSource: "ml-book-v2-calibrated", ModelVersion: "warming", FeatureSchema: "book-native-v2",
		AllScored: []properScoreForecast{}}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	s.sweepProperScore(context.Background())
	report, err := s.store.CollectorLivenessReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range report["collectors"].([]storage.CollectorLivenessView) {
		if row.CollectorID == "proper-score" {
			if row.Status != "healthy_empty" || row.NeverRan || row.Alert || row.Attempted != 0 || row.Inserted != 0 ||
				row.Exclusions["model_warming"] != 1 || !strings.Contains(row.ZeroReason, "warming") {
				t.Fatalf("warming model was misclassified: %+v", row)
			}
			return
		}
	}
	t.Fatal("proper-score liveness receipt missing")
}

func TestProperScoreWarmingManifestWithForecastsFailsClosed(t *testing.T) {
	s := testServer(t)
	manifest := properScoreForecastFile{GeneratedAt: time.Now().Unix(), ModelStatus: "WARMING",
		ForecastSource: "ml-book-v2-calibrated", ModelVersion: "warming", FeatureSchema: "book-native-v2",
		AllScored: []properScoreForecast{{Ticker: "KX-FORBIDDEN", Platform: "kalshi", PWin: .6}}}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	s.sweepProperScore(context.Background())
	report, err := s.store.CollectorLivenessReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range report["collectors"].([]storage.CollectorLivenessView) {
		if row.CollectorID == "proper-score" {
			if row.Status != "blocked" || row.ErrorClass != "forecast_status" || row.Attempted != 0 || row.Inserted != 0 {
				t.Fatalf("WARMING forecasts did not fail closed: %+v", row)
			}
			return
		}
	}
	t.Fatal("proper-score liveness receipt missing")
}

func TestProperScorePaperProvisionalManifestStillCollectsResearch(t *testing.T) {
	s := testServer(t)
	manifest := properScoreForecastFile{GeneratedAt: time.Now().Unix(), ModelStatus: "PAPER_PROVISIONAL",
		ForecastSource: "ml-book-v2-calibrated", ModelVersion: "v2-provisional", FeatureSchema: "book-native-v2",
		AllScored: []properScoreForecast{{Ticker: "KX-PROVISIONAL", Platform: "kalshi", Side: "YES",
			SignalType: "fixture", PWin: .6, ResolveHours: 1}}}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	s.sweepProperScore(context.Background())
	report, err := s.store.CollectorLivenessReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range report["collectors"].([]storage.CollectorLivenessView) {
		if row.CollectorID != "proper-score" {
			continue
		}
		if row.Status == "blocked" && row.ErrorClass == "forecast_status" {
			t.Fatalf("Paper-provisional forecasts were incorrectly rejected as a status error: %+v", row)
		}
		// Status eligibility is independent from economic book eligibility. A provisional forecast
		// enters the normal collector, but a missing executable book must be counted once at the
		// book gate and must not fabricate six downstream transform/mode attempts.
		if row.Status != "healthy_empty" || !row.ExpectedZero || row.Attempted != 0 ||
			row.Exclusions["no_current_complete_book"] != 1 ||
			!strings.Contains(row.ZeroReason, "fresh complete executable book") {
			t.Fatalf("Paper-provisional manifest did not enter normal research collection: %+v", row)
		}
		return
	}
	t.Fatal("proper-score liveness receipt missing")
}
