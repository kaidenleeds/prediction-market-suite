package server

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR143VerdictRefreshPreservesCriticalLastGoodGroupsOnDBError(t *testing.T) {
	s := testServer(t)
	prior := []verdictEnt{
		{Family: "signal-edge", Group: "signal", N: 5, Mean: .02,
			Venues: map[string]venueVerdict{"kalshi": {N: 5, Mean: .02}}},
		{Family: "taker:signal-edge@kalshi", SourceFamily: "signal-edge", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 1, Mean: .03},
		{Family: "strategy:accepted-edge@polyus", SourceFamily: "accepted-edge", Platform: "polyus",
			OriginLayer: "strategy", Route: "taker", Side: "NO", Group: "strategy", N: 2, Mean: .04},
	}
	s.verdMu.Lock()
	s.verdCache, s.verdAt = prior, time.Now().Add(-2*time.Minute)
	s.verdMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	got := s.computeExperimentVerdicts(ctx)
	found := map[string]bool{}
	for _, row := range got {
		found[row.Group+"|"+row.Family+"|"+row.Side] = true
	}
	for _, key := range []string{
		"signal|signal-edge|",
		"taker|taker:signal-edge@kalshi|YES",
		"strategy|strategy:accepted-edge@polyus|NO",
	} {
		if !found[key] {
			t.Fatalf("failed aggregate erased last-good %s; got=%+v", key, got)
		}
	}
}

func TestR143FollowerRosterKeepsExactSidesAndOriginRoutesSeparate(t *testing.T) {
	verds := []verdictEnt{
		{Family: "taker:two-sided@kalshi", SourceFamily: "two-sided", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 1, Mean: .01},
		{Family: "taker:two-sided@kalshi", SourceFamily: "two-sided", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker", N: 2, Mean: .02},
		{Family: "strategy:already-runs@polyus", SourceFamily: "already-runs", Platform: "polyus",
			OriginLayer: "strategy", Route: "taker", Side: "YES", Group: "strategy", N: 1, Mean: .03},
		{Family: "taker:side-control:two-sided@kalshi", SourceFamily: "side-control:two-sided", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker", N: 10, Mean: .50},
	}
	subs := gfDynamicSubs(verds)
	seen := map[string]bool{}
	for _, sub := range subs {
		seen[sub.Family] = true
	}
	if len(subs) != 2 || !seen["gf:two-sided:yes:taker-k"] || !seen["gf:two-sided:no:taker-k"] {
		t.Fatalf("exact YES/NO cells were pooled or omitted: %+v", subs)
	}
	coverage := gfPaperExecutionCoverage(verds)
	foundOrigin, foundControl := false, false
	for _, row := range coverage {
		if row.Family == "already-runs" && row.Executor == "originating-strategy" {
			foundOrigin = true
		}
		if row.Family == "side-control:two-sided" && row.Executor == "" && row.Excluded != "" {
			foundControl = true
		}
	}
	if !foundOrigin || !foundControl {
		t.Fatalf("strategy/control coverage incorrect: %+v", coverage)
	}
}

func TestR165AssumedFillSystemPointIsNotPaperAuthority(t *testing.T) {
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{{
		Family: "taker:kflow@kalshi", SourceFamily: "kflow", Platform: "kalshi",
		OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 1, Mean: .004,
	}})
	if mean, ok := s.paperSystemRoutePoint("auto-cons-kflow", "kalshi", "YES"); ok || mean != 0 {
		t.Fatalf("assumed-fill route retained Paper authority: mean=%v ok=%v", mean, ok)
	}
	if _, ok := s.paperSystemRoutePoint("auto-cons-kflow", "kalshi", "NO"); ok {
		t.Fatal("YES evidence authorized NO without an independent route")
	}
	if _, ok := s.paperSystemRoutePoint("auto-ml", "kalshi", "YES"); ok {
		t.Fatal("ML gained originating-system Paper authority")
	}
}

func TestR145SystemPaperPointPaysCurrentAdversePriceAndFeeMovement(t *testing.T) {
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{{
		Family: "taker:kflow@kalshi", SourceFamily: "kflow", Platform: "kalshi",
		OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 1, Markets: 1, Mean: .05, MeanAsk: .40, FeePC: .01,
		ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill",
	}})
	point, ok := s.paperSystemRouteCurrentPoint("auto-cons-kflow", "kalshi", "YES", .42, .015, 1)
	if !ok || math.Abs(point-.025) > 1e-12 {
		t.Fatalf("current point=%v ok=%v want .025", point, ok)
	}
	if point, ok = s.paperSystemRouteCurrentPoint("auto-cons-kflow", "kalshi", "YES", .46, .015, 1); ok || point != 0 {
		t.Fatalf("erased current edge remained eligible: point=%v ok=%v", point, ok)
	}
}
