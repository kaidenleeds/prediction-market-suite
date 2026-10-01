package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR144TrackedSystemCountIsCacheOnlyWithClosedDatabase(t *testing.T) {
	s := testServer(t)
	if err := s.store.Close(); err != nil {
		t.Fatal(err)
	}
	verdicts := []verdictEnt{{
		Family: "taker:alpha@kalshi", SourceFamily: "alpha", Platform: "kalshi",
		OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	variants, families, ok := s.trackedSystemCounts(ctx, verdicts)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("cache-only system count blocked for %s", elapsed)
	}
	if !ok || variants <= 0 || families <= 0 {
		t.Fatalf("cache-only system count = variants %d families %d ok %v", variants, families, ok)
	}
	if s.researchDigestBusy {
		t.Fatal("phone count started a database digest refresh")
	}
	if v, f, ready := s.briefTrackedSystemCounts(verdicts); !ready || v <= 0 || f <= 0 {
		t.Fatalf("briefing count remained refreshing: variants %d families %d ready %v", v, f, ready)
	}
}

func TestR144TrackedSystemCountSeparatesExecutionVariantsFromFamilies(t *testing.T) {
	rows := []leaderboardBacktestRow{
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "alpha", Platform: "kalshi", Side: "YES", OriginLayer: "model"}, SystemID: "taker:alpha@kalshi [YES]", Route: "taker"},
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "alpha", Platform: "kalshi", Side: "YES", OriginLayer: "strategy"}, SystemID: "taker:alpha@kalshi [YES]", Route: "taker"},
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "alpha", Platform: "kalshi", Side: "NO", OriginLayer: "model"}, SystemID: "taker:alpha@kalshi [NO]", Route: "taker"},
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "alpha", Platform: "polyus", Side: "YES", OriginLayer: "model"}, SystemID: "taker:alpha@polyus [YES]", Route: "taker"},
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "invert:alpha", Platform: "kalshi", Side: "NO", OriginLayer: "model"}, SystemID: "taker:invert:alpha@kalshi [NO]", Route: "taker"},
		{UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{Family: "side-control:alpha", Platform: "kalshi", Side: "NO", OriginLayer: "model"}, SystemID: "taker:side-control:alpha@kalshi [NO]", Route: "taker"},
	}
	variants, families := trackedSystemCountRows(rows)
	if variants != 4 || families != 2 {
		t.Fatalf("count=%d variants/%d families want 4/2", variants, families)
	}
}

func TestR144SystemsLeaderboardServesCachedDigestWithCanceledRequest(t *testing.T) {
	s := testServer(t)
	dayLo, dayHi := .02, .05
	rateLo, rateHi := .20, .60
	s.researchDigestMu.Lock()
	s.researchDigestTrials = []storage.UnitTrialLeaderboardStat{{
		Family: "alpha", Platform: "kalshi", OriginLayer: "model", Side: "YES",
		N: 25, Total: 25, UniqueMarkets: 25, SettledMarkets: 25, MeanPC: .30, SDPC: .01,
		TrackedSeconds:       30 * 86400,
		SettledEventClusters: 25, EventClusterMeanPC: .035, EventClusterSDPC: .01,
		EventClusterMeanAsk: .41, EventClusterMeanFeePC: .007, ProofMeanAsk: .42,
		ProofMeanFeePC: .008, DepthKnownShare: 1, MatureUTCBlocks: 30,
		ObservedUTCBlocks: 25, EventDayClusters: 25, DayMeanPC: .035,
		DayMeanLoPC: &dayLo, DayMeanHiPC: &dayHi, ClusterNetPerDayLo: &rateLo,
		ClusterNetPerDayHi: &rateHi, NetPerCalendarDay: .42,
	}}
	s.researchDigestJSON = []byte(`{"state":"READY","promotion":{"state":"READY","sealed_eligible":2}}`)
	s.researchDigestAt = time.Now()
	s.researchDigestNext = time.Now().Add(time.Hour)
	s.researchDigestMu.Unlock()
	if err := s.store.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	started := time.Now()
	s.handleLeaderboardBacktest(rec, httptest.NewRequest(http.MethodGet, "/api/systems-leaderboard", nil).WithContext(ctx))
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("cached Systems endpoint blocked for %s", elapsed)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("Systems endpoint = %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Rows     []leaderboardBacktestRow `json:"rows"`
		Coverage struct {
			DigestReady bool `json:"digest_ready"`
		} `json:"coverage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Coverage.DigestReady || len(body.Rows) == 0 {
		t.Fatalf("cached digest was not served: %+v", body)
	}
	found := false
	for _, row := range body.Rows {
		if row.Family == "alpha" && row.N == 25 {
			found = true
			if row.NetPerCalendarDay < .249999 || row.NetPerCalendarDay > .250001 {
				t.Fatalf("distinct-contract Net/d = %v want .25", row.NetPerCalendarDay)
			}
			if row.Proof != "VOID ASSUMED-FILL HISTORY" || row.SimulationState != "LOWER-BOUND+" || !row.ConfidenceReady ||
				row.EvidenceTier != "historical_assumed_fill_simulation_void" || row.FillConditioned || row.ProfitEvidence || row.LiveAuthorizes {
				t.Fatalf("cached proof state was lost: %+v", row)
			}
			if row.ProofMeanAsk != .42 || row.ProofMeanFeePC != .008 || row.DepthKnownShare != 1 ||
				row.MatureUTCBlocks != 30 || row.ObservedUTCBlocks != 25 || row.EventDayClusters != 25 ||
				row.DayMeanLoPC == nil || *row.DayMeanLoPC != dayLo || row.ClusterNetPerDayLo == nil ||
				*row.ClusterNetPerDayLo != rateLo {
				t.Fatalf("cached proof/maturity/cost/depth fields were lost: %+v", row)
			}
		}
	}
	if !found {
		t.Fatalf("cached economic row missing: %+v", body.Rows)
	}
}

func TestR144ComboPaperHeaderCacheReusesLastGoodMetrics(t *testing.T) {
	s := testServer(t)
	s.cacheBriefPortfolioMetrics(vbCombos, bookSessionMetrics{
		NetUSD: 4.50, Bets: 3, Units: 9, UnitsComplete: true, Open: 2, OpenComplete: true,
	}, true)
	// A later transient failure must retain the last-good snapshot rather than replacing it with n/a.
	s.cacheBriefPortfolioMetrics(vbCombos, bookSessionMetrics{}, false)
	got, ok, _ := s.cachedBriefPortfolioMetrics(vbCombos)
	if !ok || got.Open != 2 || got.Bets != 3 || got.NetUSD != 4.50 {
		t.Fatalf("Combo Paper did not reuse the header snapshot: ok=%v metrics=%+v", ok, got)
	}
}

func TestR144ComboPaperFailedAttemptExpiresInsteadOfSuppressingFutureReads(t *testing.T) {
	s := testServer(t)
	s.cacheBriefPortfolioMetrics(vbCombos, bookSessionMetrics{}, false)

	s.briefPortfolioMu.Lock()
	s.briefPortfolioTried[vbCombos] = time.Now().Add(-briefPortfolioAttemptTTL - time.Second)
	s.briefPortfolioMu.Unlock()

	_, ok, tried := s.cachedBriefPortfolioMetrics(vbCombos)
	if ok || tried {
		t.Fatalf("expired failed attempt = ok %v tried %v; want false/false", ok, tried)
	}
}

func TestR144PortfolioCacheCannotCrossPnLResetEpoch(t *testing.T) {
	s := testServer(t)
	s.pnlMu.Lock()
	s.pnlEpoch = time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	s.pnlMu.Unlock()
	s.cacheBriefPortfolioMetrics(vbCombos, bookSessionMetrics{
		NetUSD: 4.50, Bets: 3, Units: 9, UnitsComplete: true, Open: 2, OpenComplete: true,
	}, true)

	s.pnlMu.Lock()
	s.pnlEpoch = s.pnlEpoch.Add(time.Hour)
	s.pnlMu.Unlock()
	_, ok, tried := s.cachedBriefPortfolioMetrics(vbCombos)
	if ok || tried {
		t.Fatalf("pre-reset cache crossed P&L epoch = ok %v tried %v", ok, tried)
	}
}

func TestR144RelationCacheCannotCrossPnLResetEpoch(t *testing.T) {
	s := testServer(t)
	s.pnlMu.Lock()
	s.pnlEpoch = time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	epoch := s.pnlEpoch
	s.pnlMu.Unlock()
	s.briefRelationMu.Lock()
	s.briefRelationEpoch = epoch
	s.briefRelationAt = time.Now()
	s.briefRelationLine = "stale relation line\n"
	s.briefRelationMu.Unlock()

	s.pnlMu.Lock()
	s.pnlEpoch = epoch.Add(time.Hour)
	s.pnlMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := s.briefFundedRelations(ctx)
	if strings.Contains(got, "stale relation line") {
		t.Fatalf("pre-reset relation cache crossed P&L epoch: %q", got)
	}
}
