package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func r144StaleBriefVerdict() verdictEnt {
	return verdictEnt{
		Family: "taker:xvgap@kalshi", SourceFamily: "xvgap", Group: "taker",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES",
		State: "COLLECTING", Mean: 0.03, Lo: 0.01, Hi: 0.05, N: 20, Markets: 20,
		EventClusters: 20, ContractMarkets: 20, SettledRows: 40,
		ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill",
	}
}

func TestR144BriefScoreboardServesStaleVerdictsAndCountWithoutWaitingForRefresh(t *testing.T) {
	s := testServer(t)
	s.verdMu.Lock()
	s.verdCache = []verdictEnt{r144StaleBriefVerdict()}
	s.verdAt = time.Now().Add(-time.Hour) // old code synchronously recomputed instead of serving this
	s.verdMu.Unlock()

	// Seed a last-good exact row count, then make the due refresh block. The request must still use
	// both stale values immediately while one background refresh owns the expensive database work.
	s.briefCountMu.Lock()
	s.briefCountVariants, s.briefCountFamilies, s.briefCountOK = 17, 9, true
	s.briefCountAt = time.Now().Add(-briefSystemCountFreshTTL - time.Second)
	s.briefCountMu.Unlock()
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.briefCountFn = func(context.Context, []verdictEnt) (int, int, bool) {
		close(started)
		<-release
		close(finished)
		return 18, 10, true
	}

	t0 := time.Now()
	got := s.briefScoreboard(context.Background())
	if elapsed := time.Since(t0); elapsed > time.Second {
		close(release)
		t.Fatalf("cached briefing scoreboard blocked for %s", elapsed)
	}
	if !strings.Contains(got, "17 variants / 9 systems") || !strings.Contains(got, "cross-venue gap") ||
		!strings.Contains(got, "K/Y/T") {
		close(release)
		t.Fatalf("briefing did not serve the last-good count + stale verdict snapshot:\n%s", got)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("stale count did not start one background refresh")
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("background count refresh did not finish")
	}
}

func TestR144BriefingMarkSentPersistsRenderedSnapshotWithoutRecompute(t *testing.T) {
	s := testServer(t)
	want := r144StaleBriefVerdict()
	s.verdMu.Lock()
	s.verdCache = []verdictEnt{want}
	s.verdAt = time.Now().Add(-time.Hour) // deliberately stale: delivery must not refresh it inline
	s.verdMu.Unlock()

	s.BriefingMarkSent(context.Background())
	raw, ok := s.store.KVGet(context.Background(), briefVerdSnapKV)
	if !ok {
		t.Fatal("BriefingMarkSent did not persist its rendered verdict snapshot")
	}
	var got map[string]string
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("decode persisted briefing snapshot: %v", err)
	}
	if got[verdictStableKey(want)] != want.State {
		t.Fatalf("persisted snapshot = %v, want stale rendered key %q=%q", got, verdictStableKey(want), want.State)
	}
}

func TestR144BriefProperScoreServesLastGoodWithoutWaitingForHeavyReport(t *testing.T) {
	s := testServer(t)
	s.briefProperMu.Lock()
	s.briefProperLine = "🧮 Proper Betting\nBrier · +1.0¢ · 8 batches · 12 markets\nLog · 0\nSpherical · 0"
	s.briefProperAt = time.Now().Add(-briefProperScoreFreshTTL - time.Second)
	s.briefProperMu.Unlock()
	started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.briefProperFn = func(context.Context) (map[string]any, error) {
		close(started)
		<-release
		close(finished)
		return map[string]any{"transforms": map[string]any{}}, nil
	}

	t0 := time.Now()
	got := s.briefProperScoreLine()
	if elapsed := time.Since(t0); elapsed > 100*time.Millisecond {
		close(release)
		t.Fatalf("last-good Proper Betting line blocked for %s", elapsed)
	}
	if !strings.Contains(got, "Brier · +1.0¢ · 8 batches · 12 markets") {
		close(release)
		t.Fatalf("last-good Proper Betting line not served: %q", got)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("stale Proper Betting line did not start one background refresh")
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("background Proper Betting refresh did not finish")
	}
}

func TestR144BriefingUsesOperatorCompactLabels(t *testing.T) {
	s := testServer(t)
	s.briefCountMu.Lock()
	s.briefCountVariants, s.briefCountFamilies, s.briefCountOK = 17, 9, true
	s.briefCountAt = time.Now()
	s.briefCountNext = time.Now().Add(briefSystemCountFreshTTL)
	s.briefCountMu.Unlock()

	got := s.BriefingText(context.Background())
	for _, want := range []string{
		"🚀 Portfolio systems · 0 positive authenticated exchange-profit routes · avg n/a",
		"⚙ 17 variants / 9 systems · n = unique settled venue+ticker contracts",
		"\n🤖 New ML\nTest · unavailable\nPaper · unavailable",
		briefProperScoreCold,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("compact briefing missing %q:\n%s", want, got)
		}
	}
	for _, banned := range []string{
		"Paper allocations", "promoted = receives allocation only; not LIVE", "why:",
		"Y/N rank separately", "B=stored both-side/route aggregate",
		"settled rows", "candidates/+", "grades", "629→0",
		"New ML · book-native-v2", "🧪 Paper ·", "m0 🔴 / rows0",
		"Brier n0", "Log n0", "Spherical n0", "Proper Betting · Brier 0", "unique independent markets",
		"provisional holdout", "rolling holdout",
	} {
		if strings.Contains(got, banned) {
			t.Fatalf("compact briefing retained removed wording %q:\n%s", banned, got)
		}
	}
}

func TestR144ActivePortfolioSystemMetricsExcludeDiagnosticsAndLosers(t *testing.T) {
	verdicts := []verdictEnt{
		{Family: "taker:alpha@kalshi", SourceFamily: "alpha", Platform: "kalshi", Side: "YES",
			OriginLayer: "model", Route: "taker", Group: "taker", N: 2, Markets: 1, Mean: 0.02,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{Family: "strategy:beta@polyus", SourceFamily: "beta", Platform: "polyus", Side: "NO",
			OriginLayer: "strategy", Route: "taker", Group: "strategy", N: 30, Markets: 9, Mean: 0.04,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{Family: "taker:side-control:alpha@kalshi", SourceFamily: "side-control:alpha", Platform: "kalshi", Side: "NO",
			OriginLayer: "model", Route: "taker", Group: "taker", N: 4, Mean: 0.20,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{Family: "taker:loser@kalshi", SourceFamily: "loser", Platform: "kalshi", Side: "YES",
			OriginLayer: "model", Route: "taker", Group: "taker", N: 5, Mean: -0.01,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{Family: "taker:locked@polyus", SourceFamily: "locked", Platform: "polyus", Side: "YES",
			OriginLayer: "model", Route: "taker", Group: "taker", N: 6, Mean: 0.30, Locked: true,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
	}
	active, avg := activePortfolioSystemMetrics(verdicts)
	if active != 2 || avg != 3.5 {
		t.Fatalf("active portfolio metrics = %d, %.1f¢; want 2, sqrt-market-weighted +3.5¢", active, avg)
	}

	s := testServer(t)
	injectVerdicts(s, verdicts)
	if got := s.activePortfolioSystemsLine(); got != "🚀 Portfolio systems · 2 positive authenticated exchange-profit routes · contract-sample-weighted avg +3.5¢/share fee-net" {
		t.Fatalf("active portfolio line = %q", got)
	}
}
