package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR148LiveDestinationSwitchesAreIndependentAndDefaultOff(t *testing.T) {
	s := testServer(t)
	if s.liveSystemVenueEnabled("kalshi") || s.liveSystemVenueEnabled("polyus") ||
		s.liveNewMLVenueEnabled("kalshi") || s.liveNewMLVenueEnabled("polyus") {
		t.Fatal("a destination LIVE lane was enabled by default")
	}

	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemPolyUS = true
	cfg.Risk.LiveNewMLPolyUS = true
	s.cfgP.Store(&cfg)
	if s.liveSystemVenueEnabled("kalshi") || !s.liveSystemVenueEnabled("polyus") ||
		s.liveNewMLVenueEnabled("kalshi") || !s.liveNewMLVenueEnabled("polyus") {
		t.Fatalf("venue switches leaked across destinations: risk=%+v", s.cfg().Risk)
	}
}

func TestR148LiveSystemAllowlistIsExactBoundedAndFailClosed(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	s.cfgP.Store(&cfg)
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXBTC", Side: "YES",
		Source: "auto-cons-spotlag", Family: "spotlag"}
	if why := s.liveSystemPostProofReason(c, "taker"); why != "live-system-allowlist-empty" {
		t.Fatalf("empty allowlist reason=%q", why)
	}

	cfg = *s.cfg()
	cfg.Risk.LiveSystemAllowlist = " kalshi|spotlag|yes|TAKER, polyus|polyus-flow|NO|taker "
	s.cfgP.Store(&cfg)
	if why := s.liveSystemPostProofReason(c, "taker"); why != "" {
		t.Fatalf("exact allowlisted identity rejected: %q", why)
	}
	for name, changed := range map[string]liveMirrorCandidate{
		"side":   {Platform: "kalshi", Ticker: "KXBTC", Side: "NO", Family: "spotlag"},
		"family": {Platform: "kalshi", Ticker: "KXBTC", Side: "YES", Family: "kcrypto"},
		"venue":  {Platform: "polyus", Ticker: "KXBTC", Side: "YES", Family: "spotlag"},
	} {
		if why := s.liveSystemPostProofReason(changed, "taker"); !strings.HasPrefix(why, "live-system-") {
			t.Fatalf("%s variant escaped exact identity belt: %q", name, why)
		}
	}
	if why := s.liveSystemPostProofReason(c, "maker"); !strings.Contains(why, "not-allowlisted") {
		t.Fatalf("maker borrowed taker authority: %q", why)
	}

	seven := make([]string, 0, liveSystemAllowlistMax+1)
	for i := 0; i <= liveSystemAllowlistMax; i++ {
		seven = append(seven, fmt.Sprintf("kalshi|family-%d|YES|taker", i))
	}
	if _, _, why := parseLiveSystemAllowlist(strings.Join(seven, ",")); why != "live-system-allowlist-exceeds-six-identities" {
		t.Fatalf("seven-entry allowlist reason=%q", why)
	}
	if _, _, why := parseLiveSystemAllowlist("kalshi|spotlag|BOTH|taker"); why != "live-system-allowlist-entry-invalid" {
		t.Fatalf("aggregate side was accepted: %q", why)
	}
}

func TestR148LiveSystemAllowlistSettingsValidateAndCanonicalize(t *testing.T) {
	s := testServer(t)
	body := strings.NewReader(`{"live_system_allowlist":" Kalshi|spotlag|yes|TAKER ; kalshi|spotlag|YES|taker "}`)
	rec := httptest.NewRecorder()
	s.handleSettings(rec, httptest.NewRequest(http.MethodPost, "/api/settings", body))
	if rec.Code != http.StatusOK || s.cfg().Risk.LiveSystemAllowlist != "kalshi|spotlag|YES|taker" {
		t.Fatalf("allowlist POST code=%d stored=%q body=%s", rec.Code, s.cfg().Risk.LiveSystemAllowlist, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.handleSettings(rec, httptest.NewRequest(http.MethodGet, "/api/settings", nil))
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || got["live_system_allowlist"] != "kalshi|spotlag|YES|taker" {
		t.Fatalf("allowlist GET=%v err=%v", got, err)
	}

	seven := make([]string, 0, liveSystemAllowlistMax+1)
	for i := 0; i <= liveSystemAllowlistMax; i++ {
		seven = append(seven, fmt.Sprintf("kalshi|family-%d|YES|taker", i))
	}
	b, _ := json.Marshal(map[string]any{"live_system_allowlist": strings.Join(seven, ",")})
	rec = httptest.NewRecorder()
	s.handleSettings(rec, httptest.NewRequest(http.MethodPost, "/api/settings", strings.NewReader(string(b))))
	if rec.Code != http.StatusBadRequest || s.cfg().Risk.LiveSystemAllowlist != "kalshi|spotlag|YES|taker" {
		t.Fatalf("oversized allowlist code=%d stored=%q body=%s", rec.Code, s.cfg().Risk.LiveSystemAllowlist, rec.Body.String())
	}
}

func TestR148NewMLProofPoolsCompatibleCurrentEpochLineageNotExactRefitHash(t *testing.T) {
	s := testServer(t)
	closed := make([]map[string]any, 0, newMLV2MinProofMarkets+5)
	for i := 0; i < newMLV2MinProofMarkets; i++ {
		row := r144NewMLClosed("polyus", fmt.Sprintf("pus-lineage-%02d", i), "YES", "taker", 10, 2, true)
		row["model_version"] = map[bool]string{true: "book-v2-refit-a", false: "book-v2-refit-b"}[i%2 == 0]
		// The first half proves the explicit stable lineage. The second half proves the deliberately
		// narrow migration of already-written current-epoch book-v2 lots from before this field.
		if i >= newMLV2MinProofMarkets/2 {
			delete(row, "model_lineage")
		}
		closed = append(closed, row)
	}

	wrongLineage := r144NewMLClosed("polyus", "pus-wrong-lineage", "YES", "taker", 10, 1000, true)
	wrongLineage["model_lineage"] = "different-features-or-labels"
	wrongLineage["model_version"] = "book-v2-wrong-lineage"
	oldEpoch := r144NewMLClosed("polyus", "pus-old-epoch", "YES", "taker", 10, 1000, true)
	oldEpoch["epoch_id"] = "retired-epoch"
	preReset := r144NewMLClosed("polyus", "pus-pre-reset", "YES", "taker", 10, 1000, true)
	preReset["opened"] = time.Now().Add(-2 * time.Hour).Unix()
	preReset["decision_ts"] = time.Now().Add(-2 * time.Hour).Unix()
	missingGeneration := r144NewMLClosed("polyus", "pus-no-generation", "YES", "taker", 10, 1000, true)
	missingGeneration["model_version"] = ""
	outsideLiveHorizon := r144NewMLClosed("polyus", "pus-paper-24h-only", "YES", "taker", 10, 1000, true)
	outsideLiveHorizon["entry_resolve_hours"] = 12.0
	closed = append(closed, wrongLineage, oldEpoch, preReset, missingGeneration, outsideLiveHorizon)
	writeR144NewMLFiles(t, s, nil, closed)

	mean, lower, markets, why := s.newMLV2PositivePaperLineageRoute("polyus", "YES", "taker", newMLV2ModelLineage)
	if why != "" || markets != newMLV2MinProofMarkets || math.Abs(mean-.20) > 1e-9 || math.Abs(lower-.20) > 1e-9 {
		t.Fatalf("compatible current lineage proof mean=%v lower=%v markets=%d why=%q", mean, lower, markets, why)
	}
	if _, _, markets, why := s.newMLV2PositivePaperRoute("polyus", "YES", "taker", "book-v2-refit-a"); markets != newMLV2MinProofMarkets/2 || why != "insufficient-independent-markets-for-new-ml-live-proof" {
		t.Fatalf("exact online-refit hash unexpectedly owned pooled proof: markets=%d why=%q", markets, why)
	}
}

func r148InsertPolyUSAllocationHistory(t *testing.T, s *Server, markets, events int) liveMirrorCandidate {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	c := liveMirrorCandidate{Platform: "polyus", Ticker: "pus-canary-now", Title: "current PolyUS canary",
		Side: "YES", Source: "auto-cons-polyus-flow", Price: .40, At: time.Now()}
	bound, _, why := r147BindSignalContract(c, "taker")
	if why != "" {
		t.Fatalf("bind PolyUS allocation contract: %s", why)
	}
	c = bound
	economicFamily := "polyus-flow"
	catalog := make([]storage.CatalogRow, 0, markets+1)
	for i := 0; i < markets; i++ {
		event := i
		if events > 0 {
			event = i % events
		}
		catalog = append(catalog, storage.CatalogRow{Venue: "polyus", Ticker: fmt.Sprintf("pus-canary-hist-%03d", i),
			EventKey: fmt.Sprintf("pus-canary-event-%03d", event), Kind: "winner", Title: "PolyUS canary fixture"})
	}
	catalog = append(catalog, storage.CatalogRow{Venue: "polyus", Ticker: c.Ticker,
		EventKey: "pus-canary-now-event", Kind: "winner", Title: c.Title})
	if err := s.store.UpsertMarketCatalog(ctx, catalog); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < markets; i++ {
		ticker := fmt.Sprintf("pus-canary-hist-%03d", i)
		opened := now.Add(-time.Duration(2+i%48) * time.Hour)
		inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{OpenedTS: opened,
			Family: economicFamily, Platform: "polyus", OriginLayer: "strategy", Ticker: ticker,
			Side: "YES", Episode: livePriorityProofGenerationEpisode, Ask: .40, FeePC: .01,
			FeeKnown: true, FeeSource: polyUSFeeAuthoritySource, Depth: 10,
			QuoteSource: livePriorityProofQuoteSource("polyus_ws_full_market_data", "r148-test-contract")})
		if err != nil || !inserted {
			t.Fatalf("PolyUS history insert %d=%v err=%v", i, inserted, err)
		}
		pnl := .18
		if i%2 == 0 {
			pnl = .22
		}
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,settle_val=1,pnl_pc=?,return_per_dollar=?
WHERE family=? AND platform='polyus' AND ticker=? AND side='YES'`,
			opened.Add(time.Minute).Format(time.RFC3339Nano), pnl, pnl/.41, economicFamily, ticker); err != nil {
			t.Fatal(err)
		}
	}
	inserted, err := s.insertCanonicalUnitTrial(ctx, storage.UnitTrial{OpenedTS: now,
		Family: economicFamily, Platform: "polyus", OriginLayer: "strategy", Ticker: c.Ticker,
		Side: "YES", Episode: livePriorityProofGenerationEpisode, Ask: .40, FeePC: .01,
		FeeKnown: true, FeeSource: polyUSFeeAuthoritySource, Depth: 10,
		QuoteSource: livePriorityProofQuoteSource("polyus_ws_full_market_data", "r148-test-contract")})
	if err != nil || !inserted {
		t.Fatalf("fresh PolyUS strategy receipt=%v err=%v", inserted, err)
	}
	s.liveAllocationSignal = map[string]time.Time{liveAllocationSignalKey(c): time.Now()}
	return c
}

func TestR148PolyUSSystemsUseSameProspectiveProofAndIndependentDestinationSwitch(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = false
	cfg.Risk.LiveSystemPolyUS = true
	cfg.Risk.LiveAllocationMinMarkets = liveAllocationMinMarketsFloor
	cfg.Risk.LiveAllocationMinEdge = .005
	s.cfgP.Store(&cfg)
	c := r148InsertPolyUSAllocationHistory(t, s, liveAllocationMinMarketsFloor, 1)
	theta := .0625
	s.pusSweep = []polymarketus.Market{{Slug: c.Ticker, FeeCoeff: &theta, TickSize: .01}}
	s.pusSweepAt = time.Now()

	ok, basis, mean, lower, fee :=
		s.liveProspectiveDiagnosticProof(context.Background(), c, .40, false)
	if !ok || !strings.HasPrefix(basis, "prospective-allocation:polyus-flow@polyus[YES]/taker:input=PUS:contract=") ||
		mean <= 0 || lower < s.liveAllocationEdgeFloor() || fee < 0 {
		t.Fatalf("valid PolyUS diagnostic route rejected: ok=%v basis=%q mean=%v lower=%v fee=%v",
			ok, basis, mean, lower, fee)
	}
	if ok, why, _, _, _ := s.liveMirrorProof(
		context.Background(), c, .40, false); ok ||
		why != liveFillConditionedProofUnavailableReason {
		t.Fatalf("hypothetical PolyUS route authorized LIVE: ok=%v why=%q", ok, why)
	}
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, true); ok ||
		why != "prospective-allocation-kalshi-or-polyus-taker-singles-only" {
		t.Fatalf("unproved PolyUS maker route authorized: ok=%v why=%q", ok, why)
	}

	cfg = *s.cfg()
	cfg.Risk.LiveSystemPolyUS = false
	s.cfgP.Store(&cfg)
	if ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, false); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("disabled PolyUS destination switch leaked authority: ok=%v why=%q", ok, why)
	}
}

func TestR148PolyUSCrossVenueSystemRequiresFreshKalshiInputTransport(t *testing.T) {
	contract := r147ContractForTest(t, "xvlag", "polyus", "YES", "K-PUS")
	fresh := r147LiveInputClockSnapshot{KalshiClient: true, KalshiSubscribed: 1000, KalshiFresh: 1000,
		PUSClient: true, PUSFrameOK: true, PUSExecutable: 1000, PUSFrameAge: time.Second}
	if why := liveAllocationInputClockReason(contract, fresh); why != "" {
		t.Fatalf("fresh K-PUS input rejected: %q", why)
	}
	missing := fresh
	missing.KalshiClient = false
	if why := liveAllocationInputClockReason(contract, missing); why != "prospective-allocation-kalshi-input-client-unavailable" {
		t.Fatalf("missing Kalshi input receipt=%q", why)
	}
	gapped := fresh
	gapped.KalshiGaps = 1
	if why := liveAllocationInputClockReason(contract, gapped); why != "prospective-allocation-kalshi-input-books-not-fresh" {
		t.Fatalf("gapped Kalshi input receipt=%q", why)
	}
}

func TestR148LiveEconomicHistoryCannotBorrowAcrossInputTopologies(t *testing.T) {
	if why := liveAllocationEconomicHistoryTopologyReason("spotlag", "kalshi", "YES", "taker"); why != "" {
		t.Fatalf("single-path spotlag history rejected: %q", why)
	}
	if why := liveAllocationEconomicHistoryTopologyReason("kalshi-flow", "kalshi", "YES", "taker"); why != "" {
		t.Fatalf("single-path kalshi-flow history rejected: %q", why)
	}
	if why := liveAllocationEconomicHistoryTopologyReason("confluence", "kalshi", "YES", "taker"); why != "prospective-allocation-economic-history-mixes-multiple-input-topologies" {
		t.Fatalf("multi-path confluence borrowed pooled economics: %q", why)
	}
}
