package server

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR148ComboKalshiHorizonFallsBackToCompleteCatalog(t *testing.T) {
	s := testServer(t)
	s.plabHorizonFn = nil
	now := time.Now().UTC()
	if err := s.store.UpsertMarketCatalog(context.Background(), []storage.CatalogRow{{
		Venue: "kalshi", Ticker: "KX-R148-CATALOG", EventKey: "KX-R148",
		CloseTS: now.Add(90 * time.Minute).Format(time.RFC3339Nano),
	}}); err != nil {
		t.Fatal(err)
	}

	resolveAt, ok := s.plabLegResolveAt(context.Background(), plabLeg{
		Platform: "kalshi", Ticker: "KX-R148-CATALOG",
	}, now)
	if !ok || resolveAt.Sub(now) < 89*time.Minute || resolveAt.Sub(now) > 91*time.Minute {
		t.Fatalf("catalog fallback resolveAt=%v ok=%v", resolveAt, ok)
	}
	if _, reason, eligible := s.plabLegCurrentHorizon(context.Background(), plabLeg{
		Platform: "kalshi", Ticker: "KX-R148-CATALOG",
	}, now); !eligible || reason != "" {
		t.Fatalf("cataloged current Kalshi leg eligible=%v reason=%q", eligible, reason)
	}
}

func TestR148ComboPolyUSUsesGameStartBeforeEventBoundary(t *testing.T) {
	s := testServer(t)
	s.plabHorizonFn = nil
	now := time.Now().UTC()
	start := now.Add(-30 * time.Minute)
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{{
		Slug: "r148-live-sport", Start: start.Format(time.RFC3339Nano),
		Resolve: now.Add(24 * time.Hour).Format(time.RFC3339Nano), Live: true,
	}}
	s.polyUSMu.Unlock()

	resolveAt, ok := s.plabLegResolveAt(context.Background(), plabLeg{
		Platform: "polyus", Ticker: "r148-live-sport",
	}, now)
	want := start.Add(3*time.Hour + 30*time.Minute)
	if !ok || resolveAt.Sub(want) < -time.Second || resolveAt.Sub(want) > time.Second {
		t.Fatalf("PolyUS sports resolveAt=%v ok=%v want=%v", resolveAt, ok, want)
	}
	if _, reason, eligible := s.plabLegCurrentHorizon(context.Background(), plabLeg{
		Platform: "polyus", Ticker: "r148-live-sport",
	}, now); !eligible || reason != "" {
		t.Fatalf("in-progress PolyUS leg eligible=%v reason=%q", eligible, reason)
	}
}

func TestR148ComboPolyUSCrawlOnlyUsesVenueEndDate(t *testing.T) {
	s := testServer(t)
	s.plabHorizonFn = nil
	now := time.Now().UTC()
	want := now.Add(3 * time.Hour)
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{{
		Slug: "r148-crawl-only", Resolve: want.Format(time.RFC3339Nano),
	}}
	s.polyUSMu.Unlock()

	resolveAt, ok := s.plabLegResolveAt(context.Background(), plabLeg{
		Platform: "polyus", Ticker: "r148-crawl-only",
	}, now)
	if !ok || resolveAt.Sub(want) < -time.Second || resolveAt.Sub(want) > time.Second {
		t.Fatalf("crawl-only resolveAt=%v ok=%v want=%v", resolveAt, ok, want)
	}
}

func TestR148NewMLComboKeepsCurrentPolyUSSportsPredictions(t *testing.T) {
	s := testServer(t)
	s.plabHorizonFn = nil
	now := time.Now().UTC()
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{
		{Slug: "r148-ml-a", Start: now.Add(6 * time.Hour).Format(time.RFC3339Nano), Resolve: now.Add(48 * time.Hour).Format(time.RFC3339Nano)},
		{Slug: "r148-ml-b", Start: now.Add(7 * time.Hour).Format(time.RFC3339Nano), Resolve: now.Add(48 * time.Hour).Format(time.RFC3339Nano)},
	}
	s.polyUSMu.Unlock()
	s.plabQuoteFn = func(context.Context, plabLeg) (float64, float64, string, bool) {
		return .40, 25, "test-book", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return .01, true }
	body := `{"execution_enabled":true,"paper_authority":true,"feature_schema":"book-native-v2","model_version":"book-native-v2","predictions":[` +
		`{"ticker":"r148-ml-a","platform":"polyus","side":"YES","price":0.40,"p_win":0.70,"ev_net":0.29},` +
		`{"ticker":"r148-ml-b","platform":"polyus","side":"NO","price":0.40,"p_win":0.68,"ev_net":0.27}]}`
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_predictions.json"), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}

	st := newMLComboStatus()
	pool := s.mlComboPredictionPool(context.Background(), &st)
	if len(pool) != 2 || st.Predictions != 2 || st.ExactLegs != 2 {
		t.Fatalf("pool=%d predictions=%d exact=%d reasons=%v", len(pool), st.Predictions, st.ExactLegs, st.Reasons)
	}
	if st.Reasons["horizon_regular_over_horizon"] != 0 {
		t.Fatalf("24h Paper ML Combo predictions were incorrectly rejected: %v", st.Reasons)
	}
	if _, reason, eligible := s.plabLegCurrentHorizon(context.Background(), plabLeg{
		Platform: "polyus", Ticker: "r148-ml-a",
	}, now); eligible || reason != "regular_over_horizon" {
		t.Fatalf("generic 4h Combo Lab contract widened with funded Paper: eligible=%v reason=%q", eligible, reason)
	}
}

func TestR148PaperComboWidensCollectionButNotLiveHandoff(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	s.plabHorizonFn = func(_ context.Context, l plabLeg, _ time.Time) (time.Time, bool) {
		switch l.Ticker {
		case "REG-23H":
			return now.Add(23 * time.Hour), true
		case "REG-25H":
			return now.Add(25 * time.Hour), true
		case "KXBTC-5H":
			return now.Add(5 * time.Hour), true
		case "KXBTC-7H":
			return now.Add(7 * time.Hour), true
		default:
			return time.Time{}, false
		}
	}
	for _, tc := range []struct {
		ticker string
		want   bool
		reason string
	}{
		{"REG-23H", true, ""},
		{"REG-25H", false, "regular_over_horizon"},
		{"KXBTC-5H", true, ""},
		{"KXBTC-7H", false, "crypto_over_horizon"},
	} {
		_, reason, got := s.paperComboLegCurrentHorizon(context.Background(), plabLeg{Platform: "kalshi", Ticker: tc.ticker}, now)
		if got != tc.want || reason != tc.reason {
			t.Errorf("Paper Combo %s eligible=%v reason=%q, want %v/%q", tc.ticker, got, reason, tc.want, tc.reason)
		}
	}
	if paperEntryHorizonEligible(23, s.liveComboEntryHorizonLimit("REG-23H", "")) ||
		paperEntryHorizonEligible(5, s.liveComboEntryHorizonLimit("KXBTC-5H", "")) {
		t.Fatal("24h/6h Paper collection limits leaked into the final 4h/2h LIVE handoff")
	}
}

func TestR148PaperNewMLWidensCollectionButNotLiveHandoff(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = func(_ context.Context, _ string, ticker, _ string) float64 {
		switch ticker {
		case "REG-23H":
			return 23
		case "KXBTC-5H":
			return 5
		case "UNKNOWN":
			return 0
		default:
			return 25
		}
	}
	ctx := context.Background()
	if !s.paperMLEntryHorizonNow(ctx, "kalshi", "REG-23H", "") ||
		!s.paperRouteEntryHorizonNow(ctx, "kalshi", "REG-23H", "", "auto-ml") {
		t.Fatal("23h regular New ML Paper candidate was not admitted")
	}
	if !s.paperMLEntryHorizonNow(ctx, "kalshi", "KXBTC-5H", "") {
		t.Fatal("5h crypto New ML Paper candidate was not admitted")
	}
	if s.paperEntryHorizonNow(ctx, "kalshi", "REG-23H", "") ||
		s.paperEntryHorizonNow(ctx, "kalshi", "KXBTC-5H", "") {
		t.Fatal("wider New ML Paper clock leaked into the final LIVE/single 4h/2h contract")
	}
	if s.paperMLEntryHorizonNow(ctx, "kalshi", "UNKNOWN", "") ||
		s.paperRouteEntryHorizonNow(ctx, "kalshi", "REG-23H", "", "auto-system") {
		t.Fatal("unknown New ML clock or non-ML wider Paper route was admitted")
	}
}

func TestR148PaperNewMLUsesAuthoritativeKalshiCatalogFallback(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = nil
	now := time.Now().UTC()
	if err := s.store.UpsertMarketCatalog(context.Background(), []storage.CatalogRow{{
		Venue: "kalshi", Ticker: "KX-R148-ML-CATALOG", EventKey: "KX-R148-ML",
		CloseTS: now.Add(23 * time.Hour).Format(time.RFC3339Nano),
	}}); err != nil {
		t.Fatal(err)
	}
	if !s.paperMLEntryHorizonNow(context.Background(), "kalshi", "KX-R148-ML-CATALOG", "") {
		t.Fatal("Paper New ML did not use the complete Kalshi catalog clock")
	}
	if s.paperEntryHorizonNow(context.Background(), "kalshi", "KX-R148-ML-CATALOG", "") {
		t.Fatal("23h catalog clock widened the final LIVE/single contract")
	}
}

func TestR148ComboWarmTriggerRetriesAdmissionAndRunsOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	readyCalls, attempts := 0, 0
	runComboWarmTrigger(ctx, time.Millisecond, func() bool {
		readyCalls++
		return readyCalls >= 3
	}, func() bool {
		attempts++
		return attempts >= 3
	})
	if readyCalls < 5 || attempts != 3 {
		t.Fatalf("readyCalls=%d attempts=%d, want readiness wait and exactly three gate attempts", readyCalls, attempts)
	}
}
