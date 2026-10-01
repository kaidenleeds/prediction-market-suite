package server

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR133DirectionalBookHorizonConfiguredAndFallback(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	reg, crypto := directionalBookHours(config.AutoConfig{
		ConsensusMaxHoursOut: 4, ConsensusCryptoMaxHoursOut: 2,
	})
	if reg != 4 || crypto != 2 {
		t.Fatalf("configured horizons = %.1f/%.1fh, want 4/2h", reg, crypto)
	}
	if !withinDirectionalBookHorizon(now, now.Add(4*time.Hour), false, reg, crypto) ||
		withinDirectionalBookHorizon(now, now.Add(4*time.Hour+time.Second), false, reg, crypto) {
		t.Fatal("configured regular horizon must include exactly <=4h and exclude later markets")
	}
	if !withinDirectionalBookHorizon(now, now.Add(2*time.Hour), true, reg, crypto) ||
		withinDirectionalBookHorizon(now, now.Add(2*time.Hour+time.Second), true, reg, crypto) {
		t.Fatal("crypto horizon must include exactly <=2h and exclude later markets")
	}

	reg, crypto = directionalBookHours(config.AutoConfig{})
	if reg != 6 || crypto != 2 {
		t.Fatalf("missing-config fallback = %.1f/%.1fh, want safe 6/2h", reg, crypto)
	}
	if !withinDirectionalBookHorizon(now, now.Add(5*time.Hour), false, reg, crypto) {
		t.Fatal("default/fallback regular horizon must retain a 5h market")
	}
	if withinDirectionalBookHorizon(now, now, false, reg, crypto) {
		t.Fatal("already-resolved markets must never enter the horizon band")
	}
}

func TestR143DepthPriorityPositiveSystemsBeforeNearHorizonAndVolume(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	candidates := []depthBookCandidate{
		{ID: "FAR-HUGE-VOLUME", ResolveAt: now.Add(24 * time.Hour), Value: 1_000_000},
		{ID: "NEAR-LATER", Near: true, ResolveAt: now.Add(3 * time.Hour), Value: 1},
		{ID: "NEAR-SOONER", Near: true, ResolveAt: now.Add(time.Hour), Value: 0},
		{ID: "LIVE", Live: true, ResolveAt: now.Add(5 * time.Hour), Value: 0},
	}
	plan := planDepthBooks(10, [][]string{{"HELD"}, {"RESTING"}}, candidates,
		[]depthBookStrategyGroup{{System: "z-proposal", Side: "YES", IDs: []string{"PROPOSAL-Z-BEST", "PROPOSAL-A-SECOND"}, Positive: true},
			{System: "a-combo", Side: "NO", IDs: []string{"COMBO-Z-BEST", "COMBO-A-SECOND"}, Positive: true}}, 4, 2, true)
	want := []string{"HELD", "RESTING", "COMBO-Z-BEST", "PROPOSAL-Z-BEST",
		"COMBO-A-SECOND", "PROPOSAL-A-SECOND", "LIVE", "NEAR-SOONER", "NEAR-LATER", "FAR-HUGE-VOLUME"}
	if !reflect.DeepEqual(plan.IDs, want) {
		t.Fatalf("priority plan=%v, want %v (system/side queues must round-robin before generic near horizon)", plan.IDs, want)
	}
	if plan.Stats.StrategyShortfall != 0 || len(plan.Stats.StrategySystems) != 2 {
		t.Fatalf("strategy coverage receipt missing: %+v", plan.Stats)
	}
}

func TestR133DepthPriorityCapIsLiveFirstSoonestAndReportsShortfall(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	candidates := []depthBookCandidate{
		{ID: "LIVE-LATE", Live: true, ResolveAt: now.Add(3 * time.Hour)},
		{ID: "NEAR-ONE", Near: true, ResolveAt: now.Add(time.Hour)},
		{ID: "LIVE-SOON", Live: true, ResolveAt: now.Add(2 * time.Hour)},
		{ID: "NEAR-TWO", Near: true, ResolveAt: now.Add(2 * time.Hour)},
	}
	plan := planDepthBooks(3, [][]string{{"HELD"}}, candidates, nil, 4, 2, true)
	want := []string{"HELD", "LIVE-SOON", "LIVE-LATE"}
	if !reflect.DeepEqual(plan.IDs, want) {
		t.Fatalf("capped plan=%v, want deterministic held then live/soonest %v", plan.IDs, want)
	}
	if plan.Stats.NearHorizon != 2 || plan.Stats.NearHorizonSelected != 0 || plan.Stats.RequiredShortfall != 2 {
		t.Fatalf("shortfall receipt=%+v, want both near markets explicitly reported", plan.Stats)
	}
}

func TestR143DepthPriorityRoundRobinsSystemsAndReportsPerSystemShortfall(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	candidates := []depthBookCandidate{
		{ID: "LIVE", Live: true, ResolveAt: now.Add(time.Hour)},
		{ID: "NEAR", Near: true, ResolveAt: now.Add(2 * time.Hour)},
	}
	plan := planDepthBooks(5, [][]string{{"HELD"}}, candidates, []depthBookStrategyGroup{
		{System: "alpha", Side: "YES", IDs: []string{"A1", "A2", "A3"}, Positive: true},
		{System: "beta", Side: "NO", IDs: []string{"B1", "B2"}, Positive: true},
	}, 4, 2, true)
	want := []string{"HELD", "A1", "B1", "A2", "B2"}
	if !reflect.DeepEqual(plan.IDs, want) {
		t.Fatalf("fair capped plan=%v, want %v", plan.IDs, want)
	}
	if plan.Stats.Strategy != 5 || plan.Stats.StrategySelected != 4 || plan.Stats.StrategyShortfall != 1 ||
		len(plan.Stats.StrategySystems) != 2 {
		t.Fatalf("aggregate/per-system strategy receipt=%+v", plan.Stats)
	}
	got := plan.Stats.StrategySystems
	if got[0].System != "alpha" || got[0].Side != "YES" || got[0].Selected != 2 || got[0].Shortfall != 1 ||
		got[1].System != "beta" || got[1].Side != "NO" || got[1].Selected != 2 || got[1].Shortfall != 0 {
		t.Fatalf("per-system coverage=%+v", got)
	}
	if plan.Stats.LiveSelected != 0 || plan.Stats.NearHorizonSelected != 0 || plan.Stats.RequiredShortfall != 3 {
		t.Fatalf("near/system union shortfall not truthful: %+v", plan.Stats)
	}
}

func TestR156PositiveSystemsCannotBeStarvedByOverCapLiveSlate(t *testing.T) {
	now := time.Date(2026, 7, 17, 20, 0, 0, 0, time.UTC)
	for _, platform := range []string{"kalshi", "polyus"} {
		t.Run(platform, func(t *testing.T) {
			candidates := make([]depthBookCandidate, 0, 20)
			for i := 0; i < 20; i++ {
				candidates = append(candidates, depthBookCandidate{
					ID: fmt.Sprintf("%s-LIVE-%02d", platform, i), Live: true,
					ResolveAt: now.Add(time.Duration(i+1) * time.Minute),
				})
			}
			plan := planDepthBooks(3, [][]string{{platform + "-HELD"}}, candidates,
				[]depthBookStrategyGroup{
					{Platform: platform, System: "positive-a", Side: "YES",
						IDs: []string{platform + "-POS-A"}, Positive: true},
					{Platform: platform, System: "positive-b", Side: "NO",
						IDs: []string{platform + "-POS-B"}, Positive: true},
				}, 4, 2, platform == "polyus")
			want := []string{platform + "-HELD", platform + "-POS-A", platform + "-POS-B"}
			if !reflect.DeepEqual(plan.IDs, want) {
				t.Fatalf("over-cap live slate starved positive routes: got=%v want=%v stats=%+v",
					plan.IDs, want, plan.Stats)
			}
			if plan.Stats.StrategySelected != 2 || plan.Stats.StrategyShortfall != 0 ||
				plan.Stats.LiveSelected != 0 || plan.Stats.Live != 20 {
				t.Fatalf("priority coverage receipt is wrong: %+v", plan.Stats)
			}
		})
	}
}

func TestR143GenericResearchQueuesRemainBehindNearHorizon(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	plan := planDepthBooks(4, nil, []depthBookCandidate{
		{ID: "NEAR", Near: true, ResolveAt: now.Add(time.Hour)},
		{ID: "TAIL", ResolveAt: now.Add(12 * time.Hour), Value: 100},
	}, []depthBookStrategyGroup{
		{System: "positive", Side: "YES", IDs: []string{"POS"}, Positive: true},
		{System: "generic-combo-leg", IDs: []string{"GENERIC"}},
	}, 4, 2, true)
	want := []string{"POS", "NEAR", "GENERIC", "TAIL"}
	if !reflect.DeepEqual(plan.IDs, want) {
		t.Fatalf("positive/near/generic/tail order=%v, want %v", plan.IDs, want)
	}
}

func TestR143PositiveBookCacheSplitsEverySystemAndSide(t *testing.T) {
	s := &Server{}
	s.cachePositiveDepthBookGroups([]plabLeg{
		{Ticker: "KX-A", Side: "YES", Platform: "kalshi", EVNet: .01, Depth: 2,
			RollingPositive: true, PositiveSystems: []string{"sys-a", "sys-b"}, SystemRoute: "taker"},
		{Ticker: "KX-B", Side: "NO", Platform: "kalshi", EVNet: .02, Depth: 3,
			RollingPositive: true, PositiveSystems: []string{"sys-a"}, SystemRoute: "taker"},
		// Depth=0 is deliberate: this pre-quote hint must request full Book3 before the exact
		// manifest gate can observe depth and decide whether the leg is economically usable.
		{Ticker: "PUS-A", Side: "YES", Platform: "polyus", EVNet: .03, Depth: 0,
			RollingPositive: true, PositiveSystems: []string{"sys-p"}, SystemRoute: "taker"},
		{Ticker: "IGNORED", Side: "YES", Platform: "kalshi", EVNet: -.01, Depth: 4,
			RollingPositive: true, PositiveSystems: []string{"loser"}, SystemRoute: "taker"},
	})
	kal := s.positiveDepthBookGroups("kalshi")
	pus := s.positiveDepthBookGroups("polyus")
	if len(kal) != 3 || len(pus) != 1 {
		t.Fatalf("cached groups kal=%+v pus=%+v", kal, pus)
	}
	if pus[0].System != "sys-p" || !reflect.DeepEqual(pus[0].IDs, []string{"PUS-A"}) {
		t.Fatalf("PolyUS positive-system group=%+v", pus)
	}
}

func TestR133PolyUSLightweightTailRetainedBeyondFullDepthCap(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	markets := make([]polyUSMarket, polymarketus.MarketsFullBookCap+37)
	for i := range markets {
		markets[i] = polyUSMarket{Slug: fmt.Sprintf("tail-%04d", i), Kind: "prop", Volume24h: float64(i)}
	}
	all, stats := polyUSSlugPlan(nil, markets, now, config.AutoConfig{
		ConsensusMaxHoursOut: 4, ConsensusCryptoMaxHoursOut: 2,
	})
	if len(all) != len(markets) {
		t.Fatalf("lightweight plan retained %d/%d slugs", len(all), len(markets))
	}
	if stats.Selected != polymarketus.MarketsFullBookCap || !stats.LightweightFullBoard {
		t.Fatalf("depth/lightweight receipt=%+v", stats)
	}
	seen := map[string]bool{}
	for _, slug := range all {
		if seen[slug] {
			t.Fatalf("duplicate slug %q", slug)
		}
		seen[slug] = true
	}
	for i := range markets {
		if !seen[markets[i].Slug] {
			t.Fatalf("tail slug %q was lost from LITE/TRADE coverage", markets[i].Slug)
		}
	}
}

func TestR133KalshiPlannerUsesCompleteBoardBeyondWarmSubset(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	far := kalshi.Market{Ticker: "KXFAR", Status: "active", ExpectedExpiration: now.Add(12 * time.Hour).Format(time.RFC3339)}
	near := kalshi.Market{Ticker: "KXNEAR", Status: "active", ExpectedExpiration: now.Add(3 * time.Hour).Format(time.RFC3339)}
	// The old planner saw only warm (KXFAR) and completely missed KXNEAR from the 20k board.
	complete := completeKalshiBookMarkets([]kalshi.Market{far, near}, map[string]kalshi.Market{"KXFAR": far})
	candidates := kalshiDepthCandidates(complete, now, 4, 2)
	plan := planDepthBooks(1, nil, candidates, nil, 4, 2, true)
	if len(complete) != 2 || plan.Stats.Universe != 2 {
		t.Fatalf("considered universe complete=%d stats=%+v, want both board rows", len(complete), plan.Stats)
	}
	if !reflect.DeepEqual(plan.IDs, []string{"KXNEAR"}) || plan.Stats.NearHorizon != 1 {
		t.Fatalf("outside-warm near market did not win depth priority: ids=%v stats=%+v", plan.IDs, plan.Stats)
	}
}

func TestR133PolyUSPlannerMergesStrictOpenFullCrawlBeyondLeagueSubset(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	active, closed := true, true
	snapshot := []polyUSMarket{{
		Slug: "league-far", Game: "Rich league row", Kind: "prop", Question: "Player strike",
		Resolve: now.Add(12 * time.Hour).Format(time.RFC3339), Volume24h: 1_000_000,
	}}
	crawl := []polymarketus.Market{
		// This open market exists only in the complete /v1 crawl and is inside the 4h lane.
		{Slug: "crawl-near", Title: "Crawl-only near", EndDate: now.Add(2 * time.Hour).Format(time.RFC3339), Active: &active},
		// Duplicate crawl metadata must not erase the richer nested sub-bet classification.
		{Slug: "league-far", Title: "Generic duplicate", EndDate: now.Add(12 * time.Hour).Format(time.RFC3339), Active: &active,
			SportsTypeV2: "SPORTS_MARKET_TYPE_MONEYLINE"},
		// A terminal row is never admitted even if it was accidentally supplied to the helper.
		{Slug: "crawl-closed", Title: "Closed", EndDate: now.Add(time.Hour).Format(time.RFC3339), Closed: &closed},
	}
	merged := mergePolyUSPlannerMarkets(snapshot, crawl, now)
	if len(merged) != 2 {
		t.Fatalf("merged planner universe=%d, want rich snapshot + strict-open crawl-only row", len(merged))
	}
	if merged[0].Kind != "prop" || merged[0].Question != "Player strike" {
		t.Fatalf("crawl overlay erased nested/sub-bet metadata: %+v", merged[0])
	}
	all, stats := polyUSSlugPlan(nil, merged, now, config.AutoConfig{
		ConsensusMaxHoursOut: 4, ConsensusCryptoMaxHoursOut: 2,
	})
	if stats.Universe != 2 || stats.NearHorizon != 1 || len(all) != 2 || all[0] != "crawl-near" {
		t.Fatalf("crawl-only near row did not enter first full-depth band: all=%v stats=%+v", all, stats)
	}
}

func TestR133PolyUSStaleStartedGameDoesNotCrowdLiveDepthBand(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	rows := []polyUSMarket{
		{Slug: "ended-but-open", Live: true, Start: now.Add(-4 * time.Hour).Format(time.RFC3339)},
		{Slug: "genuinely-live", Live: true, Start: now.Add(-2 * time.Hour).Format(time.RFC3339)},
	}
	cs := polyUSDepthCandidates(rows, now, 4, 2)
	if len(cs) != 2 || cs[0].Live || !cs[1].Live {
		t.Fatalf("stale/live classification = %+v, want expired fallback false and current true", cs)
	}
}
