package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR132PolyUSSlugPlanIsUncappedAndPrioritized(t *testing.T) {
	markets := make([]polyUSMarket, 2605)
	for i := range markets {
		markets[i] = polyUSMarket{Slug: fmt.Sprintf("slug-%04d", i), Kind: "prop"}
	}
	markets[2401].Live = true
	markets[2502].Kind = "winner"

	got, stats := polyUSSlugPlan([]string{"position-only", "slug-0003", "position-only", ""}, markets, time.Now(), config.AutoConfig{})
	if len(got) != len(markets)+1 {
		t.Fatalf("subscription slugs=%d, want all %d discovered markets plus one aged position", len(got), len(markets))
	}
	if stats.Cap != polymarketus.MarketsFullBookCap || !stats.LightweightFullBoard {
		t.Fatalf("tier receipt=%+v, want bounded full depth plus full-board lightweight coverage", stats)
	}
	wantPrefix := []string{"position-only", "slug-0003", "slug-2401", "slug-2502"}
	for i, want := range wantPrefix {
		if got[i] != want {
			t.Fatalf("priority[%d]=%q, want %q; decision/live/winner slugs must warm first", i, got[i], want)
		}
	}
	seen := make(map[string]bool, len(got))
	for _, slug := range got {
		if seen[slug] {
			t.Fatalf("duplicate subscription slug %q", slug)
		}
		seen[slug] = true
	}
	for i := range markets {
		if !seen[markets[i].Slug] {
			t.Fatalf("market %q was truncated from the subscription tail", markets[i].Slug)
		}
	}
}

func TestPolyUSSlugPlanReportsBoundedLiveCoverageForHugeCatalog(t *testing.T) {
	markets := make([]polyUSMarket, polymarketus.DefaultMarketsWideBookCap+1)
	for i := range markets {
		markets[i] = polyUSMarket{Slug: fmt.Sprintf("wide-%05d", i), Kind: "prop"}
	}
	got, stats := polyUSSlugPlan(nil, markets, time.Now(), config.AutoConfig{})
	if len(got) != len(markets) {
		t.Fatalf("planner retained %d/%d catalog rows", len(got), len(markets))
	}
	if stats.LightweightFullBoard {
		t.Fatalf("catalog above the %d-market socket bound reported full-board live coverage", polymarketus.DefaultMarketsWideBookCap)
	}
}

func TestR132PolyUSWSBootSyncRetriesUntilRealKickThenLatches(t *testing.T) {
	calls := 0
	force := func() bool {
		calls++
		return calls >= 2
	}
	if synced, kicked := pusWSBootSync(false, false, force); synced || kicked || calls != 0 {
		t.Fatalf("incomplete crawl must not kick: synced=%v kicked=%v calls=%d", synced, kicked, calls)
	}
	if synced, kicked := pusWSBootSync(true, false, force); synced || kicked || calls != 1 {
		t.Fatalf("no-socket/failed kick must remain retryable: synced=%v kicked=%v calls=%d", synced, kicked, calls)
	}
	synced, kicked := pusWSBootSync(true, false, force)
	if !synced || !kicked || calls != 2 {
		t.Fatalf("successful kick must latch: synced=%v kicked=%v calls=%d", synced, kicked, calls)
	}
	if synced2, kicked2 := pusWSBootSync(true, synced, force); !synced2 || kicked2 || calls != 2 {
		t.Fatalf("latched boot plan must never re-kick: synced=%v kicked=%v calls=%d", synced2, kicked2, calls)
	}
}

func TestR132PolyUSWatchdogUsesConnectionFramesNotQuietQuotes(t *testing.T) {
	threshold := 2 * time.Minute
	dataThreshold := polymarketus.MarketsWSPrimaryDataMaxAge
	transportThreshold := 45 * time.Second
	if polyUSWSConnectionStale(0, false, 0, false, 0, false, 0, threshold, dataThreshold, transportThreshold) {
		t.Fatal("an unsubscribed client is not a dark subscribed socket")
	}
	if polyUSWSConnectionStale(9000, true, 30*time.Second, true, 30*time.Second, true, 30*time.Second,
		threshold, dataThreshold, transportThreshold) {
		t.Fatal("a recent market-data payload must keep a quiet broad socket healthy")
	}
	if polyUSWSConnectionStale(9000, true, 30*time.Second, false, 0, true, 0,
		threshold, dataThreshold, transportThreshold) {
		t.Fatal("a new primary gets one threshold window for its initial snapshots")
	}
	if !polyUSWSConnectionStale(9000, true, 3*time.Minute, false, 0, true, 0,
		threshold, dataThreshold, transportThreshold) {
		t.Fatal("a heartbeat-alive primary with no market payload after warm-up must be stale")
	}
	if polyUSWSConnectionStale(9000, true, 10*time.Second, true, dataThreshold-time.Second, true, 2*time.Second,
		threshold, dataThreshold, transportThreshold) {
		t.Fatal("quiet current-generation snapshot with fresh transport must remain healthy")
	}
	if !polyUSWSConnectionStale(9000, true, 10*time.Second, true, dataThreshold, true, 2*time.Second,
		threshold, dataThreshold, transportThreshold) {
		t.Fatal("heartbeat-alive primary with globally dark market data must reconnect")
	}
	if !polyUSWSConnectionStale(9000, true, 10*time.Second, true, 2*time.Second, true, transportThreshold,
		threshold, dataThreshold, transportThreshold) {
		t.Fatal("stale primary transport must reconnect")
	}
	if !polyUSWSConnectionStale(9000, false, 0, false, 0, false, 0,
		threshold, dataThreshold, transportThreshold) {
		t.Fatal("a subscribed client with no primary receipt must be stale")
	}
}

func TestR132PolyUSLeagueLastGoodIsPerLeague(t *testing.T) {
	now := time.Now()
	a := []polymarketus.Event{{ID: "a"}}
	b := []polymarketus.Event{{ID: "b"}}
	gotA, cacheA := pusLeagueEffective(now, a, true, pusLeagueSnapshot{})
	gotB, cacheB := pusLeagueEffective(now, b, true, pusLeagueSnapshot{})
	if len(gotA) != 1 || len(gotB) != 1 {
		t.Fatal("initial successful league snapshots missing")
	}

	// A succeeds with a replacement while B alone fails: B retains only its own last good.
	gotA, cacheA = pusLeagueEffective(now.Add(time.Minute), []polymarketus.Event{{ID: "a2"}}, true, cacheA)
	gotB, cacheB = pusLeagueEffective(now.Add(time.Minute), nil, false, cacheB)
	if gotA[0].ID != "a2" || gotB[0].ID != "b" {
		t.Fatalf("partial failure mixed/deleted leagues: A=%v B=%v", gotA, gotB)
	}

	// HTTP 200 empty is authoritative; it clears immediately instead of reusing B.
	gotB, cacheB = pusLeagueEffective(now.Add(70*time.Second), nil, true, cacheB)
	if len(gotB) != 0 || cacheB.At.IsZero() || len(cacheB.Events) != 0 {
		t.Fatalf("authoritative empty did not clear league: got=%v cache=%+v", gotB, cacheB)
	}

	// A transport failure beyond TTL fails closed rather than serving stale games forever.
	gotA, _ = pusLeagueEffective(now.Add(3*time.Minute), nil, false, cacheA)
	if len(gotA) != 0 {
		t.Fatalf("expired failed league retained stale rows: %v", gotA)
	}
}

func TestR132XvGapBlocksOnlyProvenInPlayBaseball(t *testing.T) {
	cases := []struct {
		m    polyUSMarket
		want bool
	}{
		{polyUSMarket{League: "mlb", Live: true}, true},
		{polyUSMarket{League: "CWS", Live: true}, true},
		{polyUSMarket{League: "mlb", Live: false}, false},
		{polyUSMarket{League: "nba", Live: true}, false},
		{polyUSMarket{League: "", Live: true}, true}, // unknown live category fails closed
	}
	for _, tc := range cases {
		if got := xvgLiveBaseballBlocked(tc.m); got != tc.want {
			t.Fatalf("xvgLiveBaseballBlocked(%+v)=%v, want %v", tc.m, got, tc.want)
		}
	}
}

func TestR133XvGapPlacementRequiresVenueMetadata(t *testing.T) {
	s := &Server{}
	if s.xvgPlacementCategoryOK("missing") {
		t.Fatal("missing PolyUS metadata must fail closed")
	}
	s.polyUSMkts = []polyUSMarket{
		{Slug: "unknown-live", Live: true},
		{Slug: "known-live", League: "nba", Live: true},
		{Slug: "prematch", Live: false},
	}
	if s.xvgPlacementCategoryOK("unknown-live") {
		t.Fatal("live market with unknown league must fail closed")
	}
	if !s.xvgPlacementCategoryOK("known-live") || !s.xvgPlacementCategoryOK("prematch") {
		t.Fatal("known non-baseball live and prematch rows should remain eligible")
	}
}
