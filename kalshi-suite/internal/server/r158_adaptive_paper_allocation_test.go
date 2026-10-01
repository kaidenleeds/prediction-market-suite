package server

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r158AllocationTrial(family, platform, side string, n int, mean, sd, trackedDays float64) storage.UnitTrialLeaderboardStat {
	return storage.UnitTrialLeaderboardStat{
		Family: family, Platform: platform, OriginLayer: "model", Side: side, Route: "taker",
		N: n, SettledMarkets: n, Total: n, UniqueMarkets: n, MeanPC: mean, SDPC: sd,
		TrackedSeconds: trackedDays * 24 * 60 * 60, TrackedDays: trackedDays,
		FirstOpened:  time.Now().UTC().Add(-time.Duration(trackedDays * float64(24*time.Hour))).Format(time.RFC3339Nano),
		LastActivity: time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano),
	}
}

func r158AllocationTrialCluster(row storage.UnitTrialLeaderboardStat, lower float64) storage.UnitTrialLeaderboardStat {
	row.ClusterNetPerDayLo = &lower
	upper := lower + .5
	row.ClusterNetPerDayHi = &upper
	return row
}

func TestR165FollowerAllocationRejectsAssumedFillProfitRates(t *testing.T) {
	stats := []storage.UnitTrialLeaderboardStat{
		r158AllocationTrialCluster(r158AllocationTrial("strong", "kalshi", "YES", 200, .10, .05, 2), 1.5),
		r158AllocationTrialCluster(r158AllocationTrial("noisy", "kalshi", "NO", 20, .02, .30, 2), 1),
		r158AllocationTrialCluster(r158AllocationTrial("young", "polyus", "YES", 200, .10, .05, .5), 1),
		r158AllocationTrial("cluster-missing", "kalshi", "YES", 200, .10, .05, 2),
		r158AllocationTrialCluster(r158AllocationTrial("cluster-negative", "kalshi", "YES", 200, .10, .05, 2), -.1),
	}
	strategy := r158AllocationTrialCluster(
		r158AllocationTrial("owned-elsewhere", "kalshi", "YES", 200, .20, .05, 2), 2)
	strategy.OriginLayer = "strategy"
	stats = append(stats, strategy)

	got := gfConservativeAllocationScores(stats)
	if len(got) != 0 {
		t.Fatalf("assumed-fill UnitTrial rates retained Paper allocation authority: %+v", got)
	}
}

func TestR158FollowerAdaptiveSplitPreservesLegacyAndExploration(t *testing.T) {
	ents := []swEntry{
		{Family: "legacy", Cents: 2, N: 100, State: "PROVEN+"},
		{Family: "gf:strong:yes:taker-k", Cents: 5, N: 100, State: gfPaperPointState},
		{Family: "gf:steady:no:taker-k", Cents: 4, N: 100, State: gfPaperPointState},
		{Family: "gf:new:yes:taker-k", Cents: 30, N: 1, State: gfPaperPointState},
	}
	swNormalize(ents)
	legacyWeight := ents[0].Weight
	followerPool := ents[1].Weight + ents[2].Weight + ents[3].Weight
	swRedistributeGenericFollowers(ents, map[string]gfAllocationEvidence{
		"gf:strong:yes:taker-k": {Score: 3, N: 100, Ready: true},
		"gf:steady:no:taker-k":  {Score: 1, N: 100, Ready: true},
		"gf:new:yes:taker-k":    {N: 1},
	})

	if math.Abs(ents[0].Weight-legacyWeight) > 1e-12 {
		t.Fatalf("static legacy allocation changed: got %.12f want %.12f", ents[0].Weight, legacyWeight)
	}
	if want := followerPool * .9 * .75; math.Abs(ents[1].Weight-want) > 1e-12 {
		t.Fatalf("strong route weight=%.12f want %.12f", ents[1].Weight, want)
	}
	if want := followerPool * .9 * .25; math.Abs(ents[2].Weight-want) > 1e-12 {
		t.Fatalf("steady route weight=%.12f want %.12f", ents[2].Weight, want)
	}
	if want := followerPool * .1; math.Abs(ents[3].Weight-want) > 1e-12 || !ents[3].Explore {
		t.Fatalf("unproven route did not retain the 10%% exploration sleeve: %+v want %.12f", ents[3], want)
	}
	if ents[1].Explore || ents[2].Explore || !ents[1].AllocationReady || !ents[2].AllocationReady {
		t.Fatalf("confidence-positive routes were mislabeled: %+v %+v", ents[1], ents[2])
	}
}

func TestR158FollowerAllUnprovenKeepsCollectionAlive(t *testing.T) {
	ents := []swEntry{
		{Family: "legacy", Cents: 2, N: 100, State: "PROVEN+"},
		{Family: "gf:a:yes:taker-k", Cents: 5, N: 1, State: gfPaperPointState},
		{Family: "gf:b:no:taker-k", Cents: 1, N: 2, State: gfPaperPointState},
	}
	swNormalize(ents)
	legacyWeight := ents[0].Weight
	followerPool := ents[1].Weight + ents[2].Weight
	swRedistributeGenericFollowers(ents, nil)
	if math.Abs(ents[0].Weight-legacyWeight) > 1e-12 ||
		math.Abs(ents[1].Weight-followerPool/2) > 1e-12 ||
		math.Abs(ents[2].Weight-followerPool/2) > 1e-12 ||
		!ents[1].Explore || !ents[2].Explore {
		t.Fatalf("all-unproven fallback must split only the follower pool equally: %+v", ents)
	}
}

func TestR165ScoreWeightTableVoidsCachedAssumedFillRates(t *testing.T) {
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{
		{Family: "taker:alpha@kalshi", SourceFamily: "alpha", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 100, Mean: .08},
		{Family: "taker:beta@kalshi", SourceFamily: "beta", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 200, Mean: .05},
	})
	asOf := time.Date(2026, 7, 17, 0, 0, 0, 0, time.UTC)
	s.researchDigestMu.Lock()
	s.gfAllocationTrials = []storage.UnitTrialLeaderboardStat{
		r158AllocationTrialCluster(r158AllocationTrial("alpha", "kalshi", "YES", 100, .08, .05, 4), .5),
		r158AllocationTrialCluster(r158AllocationTrial("beta", "kalshi", "YES", 200, .05, .05, 2), 1.5),
	}
	s.gfAllocationAsOf, s.gfAllocationDay, s.gfAllocationBuiltAt = asOf, "2026-07-17", asOf.Add(time.Minute)
	s.researchDigestMu.Unlock()

	tab := s.scoreWeights(context.Background())
	weights := map[string]swEntry{}
	for _, row := range tab.Books[vbKalshi] {
		if row.Family == "gf:alpha:yes:taker-k" || row.Family == "gf:beta:yes:taker-k" {
			weights[row.Family] = row
		}
	}
	alpha, aok := weights["gf:alpha:yes:taker-k"]
	beta, bok := weights["gf:beta:yes:taker-k"]
	if !aok || !bok || alpha.AllocationReady || beta.AllocationReady ||
		alpha.ProfitEvidence || beta.ProfitEvidence {
		t.Fatalf("assumed-fill rates retained allocation authority: %+v", weights)
	}
	if math.Abs(beta.Weight-alpha.Weight) > 1e-12 || !alpha.Explore || !beta.Explore {
		t.Fatalf("hypothesis routes were not equally exploration-sized: alpha=%+v beta=%+v", alpha, beta)
	}
	if tab.AllocationFrozen || !tab.AllocationAsOf.IsZero() || tab.AllocationDay != "" ||
		!tab.AllocationBuiltAt.IsZero() {
		t.Fatalf("void assumed-fill snapshot remained funded metadata: %+v", tab)
	}
}

func TestR158CurrentDigestCannotAuthorizeFundedAllocation(t *testing.T) {
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{
		{Family: "taker:alpha@kalshi", SourceFamily: "alpha", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 100, Mean: .08},
		{Family: "taker:beta@kalshi", SourceFamily: "beta", Platform: "kalshi",
			OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 200, Mean: .05},
	})
	// Deliberately populate only the current display digest. Funded allocation must ignore it.
	s.researchDigestMu.Lock()
	s.researchDigestTrials = []storage.UnitTrialLeaderboardStat{
		r158AllocationTrialCluster(r158AllocationTrial("alpha", "kalshi", "YES", 100, .08, .05, 4), 5),
		r158AllocationTrialCluster(r158AllocationTrial("beta", "kalshi", "YES", 200, .05, .05, 2), 1),
	}
	s.researchDigestMu.Unlock()
	tab := s.scoreWeights(context.Background())
	var alpha, beta swEntry
	for _, row := range tab.Books[vbKalshi] {
		switch row.Family {
		case "gf:alpha:yes:taker-k":
			alpha = row
		case "gf:beta:yes:taker-k":
			beta = row
		}
	}
	if tab.AllocationFrozen || !alpha.Explore || !beta.Explore ||
		alpha.AllocationReady || beta.AllocationReady || math.Abs(alpha.Weight-beta.Weight) > 1e-12 {
		t.Fatalf("current-day digest leaked into funded allocation: alpha=%+v beta=%+v table=%+v", alpha, beta, tab)
	}
}

func TestR158FrozenAllocationLoaderUsesUTCStartCutoff(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	dayStart := time.Date(2026, 7, 18, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 21; i++ {
		ticker := fmt.Sprintf("PRIOR-%02d", i)
		inserted, err := s.store.InsertUnitTrial(ctx, storage.UnitTrial{
			OpenedTS: dayStart.Add(-36 * time.Hour), Family: "prior-only", Platform: "kalshi",
			OriginLayer: "model", Ticker: ticker, Side: "YES", Ask: .40, FeePC: .01,
			FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws",
		})
		if err != nil || !inserted {
			t.Fatalf("insert prior %s=%v err=%v", ticker, inserted, err)
		}
	}
	inserted, err := s.store.InsertUnitTrial(ctx, storage.UnitTrial{
		OpenedTS: dayStart.Add(time.Hour), Family: "prior-only", Platform: "kalshi",
		OriginLayer: "model", Ticker: "CURRENT-DAY", Side: "YES", Ask: .40, FeePC: .01,
		FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws",
	})
	if err != nil || !inserted {
		t.Fatalf("insert current-day=%v err=%v", inserted, err)
	}
	if _, err = s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,pnl_pc=.10,return_per_dollar=.25,capital_day=.25
WHERE ticker LIKE 'PRIOR-%'`, dayStart.Add(-12*time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.store.DBForTest().ExecContext(ctx, `UPDATE unit_trials
SET settled=1,closed_ts=?,pnl_pc=.90,return_per_dollar=2.25,capital_day=.25
WHERE ticker='CURRENT-DAY'`, dayStart.Add(2*time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	snap, err := s.loadGFFrozenAllocation(ctx, dayStart.Add(12*time.Hour))
	if err != nil || !snap.AsOf.Equal(dayStart) || snap.Day != "2026-07-18" || snap.BuiltAt.IsZero() ||
		len(snap.Trials) != 1 || snap.Trials[0].N != 21 || snap.Trials[0].Total != 21 {
		t.Fatalf("prior-only frozen snapshot=%+v err=%v", snap, err)
	}
}

func TestR158TinyFollowerShareIsNotInflatedToOneContract(t *testing.T) {
	s := testServer(t)
	const weight = .0001
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), Books: map[string][]swEntry{
		vbKalshi: {{Family: "gf:tiny:yes:taker-k", Cents: 1, Weight: weight}},
	}}
	s.swMu.Unlock()
	equity := s.bookSizingEquityUSD(context.Background(), vbKalshi)
	got, ok := s.subShareUSD("gf:tiny:yes:taker-k")
	want := weight * equity
	if !ok || math.Abs(got-want) > 1e-12 || got >= 1 {
		t.Fatalf("tiny adaptive weight was inflated into funded capacity: got=%v ok=%v want=%v", got, ok, want)
	}
}
