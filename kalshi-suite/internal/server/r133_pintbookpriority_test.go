package server

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

func TestR133PintMatchedLowVolumeGetsSeatBeforeUnmatchedVolume(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	plan := planPintCLOBBooks(1, 2, []pintBookCandidate{
		{ConditionID: "high-volume-filler", Tokens: []string{"fy", "fn"}, Volume: 1_000_000},
		{ConditionID: "low-volume-match", Tokens: []string{"my", "mn"}, Volume: 1, MatchK: true},
	}, 4, 2, now)
	if !reflect.DeepEqual(plan.Conditions, []string{"low-volume-match"}) {
		t.Fatalf("selected=%v, matched low-volume condition must beat unmatched volume filler", plan.Conditions)
	}
	if !reflect.DeepEqual(plan.Assets, []string{"my", "mn"}) {
		t.Fatalf("assets=%v, both authoritative outcome tokens must be subscribed", plan.Assets)
	}
	if plan.Stats.Matched != 1 || plan.Stats.MatchedSelected != 1 || plan.Stats.VolumeFillSelected != 0 {
		t.Fatalf("coverage=%+v", plan.Stats)
	}
}

func TestR133PintPlanKeepsEveryVenuePairCombinationObservable(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	plan := planPintCLOBBooks(3, 3, []pintBookCandidate{
		{ConditionID: "k-only", Tokens: []string{"ky", "kn"}, MatchK: true},
		{ConditionID: "pus-only", Tokens: []string{"py", "pn"}, MatchPUS: true},
		{ConditionID: "three-way", Tokens: []string{"ty", "tn"}, MatchK: true, MatchPUS: true},
	}, 4, 2, now)
	st := plan.Stats
	if st.KPair != 2 || st.KPairSelected != 2 || st.PUSPair != 2 || st.PUSPairSelected != 2 ||
		st.ThreeWay != 1 || st.ThreeWaySelected != 1 || st.RequiredShortfall != 0 {
		t.Fatalf("pair coverage=%+v", st)
	}
	if st.LiveAuthorizing || !st.ResearchOnly {
		t.Fatalf("Poly-int plan crossed live boundary: %+v", st)
	}
}

func TestR133PintDynamicIdentityIncludesPUSAndThreeWay(t *testing.T) {
	xvReg.reset()
	defer xvReg.reset()
	key := "crypto|BTC|2026-07-10T16:00:00Z|T56000|up"
	xvReg.add("polymarket", "0xdynamic", key, "crypto")
	xvReg.add("kalshi", "KXDYNAMIC", key, "crypto")
	xvReg.add("polyus", "pus-dynamic", key, "crypto")
	if k, pus := pintDynamicXVVenues("0xdynamic", "unused-slug"); !k || !pus {
		t.Fatalf("dynamic identity destinations k=%v pus=%v, want all pair inputs visible", k, pus)
	}
}

func TestR133PintPlanTelemetryIsServerOwned(t *testing.T) {
	a, b := &Server{}, &Server{}
	a.recordPintCLOBPlan(pintBookPlan{Stats: pintBookPlanStats{Cap: 600, Selected: 17}})
	if got := a.pintCLOBPlanView(); got.Selected != 17 {
		t.Fatalf("owner receipt=%+v", got)
	}
	if got := b.pintCLOBPlanView(); got.Selected != 0 || got.Cap != 0 {
		t.Fatalf("separate server leaked coverage telemetry: %+v", got)
	}
}

func TestR133PintPartial199CannotShrinkLastGood600(t *testing.T) {
	now := time.Date(2026, 7, 10, 20, 0, 0, 0, time.UTC)
	old := make([]pintBookCandidate, 600)
	for i := range old {
		old[i] = pintBookCandidate{ConditionID: fmt.Sprintf("old-%03d", i),
			Tokens: []string{fmt.Sprintf("oy-%03d", i), fmt.Sprintf("on-%03d", i)}, Volume: float64(600 - i)}
	}
	old[0].Held = true
	old[1].MatchK = true
	last := planPintCLOBBooks(600, 600, old, 4, 2, now.Add(-5*time.Minute))
	partial := make([]pintBookCandidate, 199)
	for i := range partial {
		partial[i] = pintBookCandidate{ConditionID: fmt.Sprintf("partial-%03d", i),
			Tokens: []string{fmt.Sprintf("py-%03d", i), fmt.Sprintf("pn-%03d", i)}, Volume: 1_000_000 - float64(i)}
	}
	partial[0].Held = true // a genuinely new held dependency must still displace an old filler
	kept := retainPintCandidates(partial, last.Selected)
	plan := planPintCLOBBooks(600, 199, kept, 4, 2, now)
	if len(plan.Conditions) != 600 || len(plan.Assets) != 1200 {
		t.Fatalf("partial refresh shrank last-good plan: conditions=%d assets=%d", len(plan.Conditions), len(plan.Assets))
	}
	seen := map[string]bool{}
	for _, id := range plan.Conditions {
		seen[id] = true
	}
	if !seen["old-000"] || !seen["old-001"] || !seen["partial-000"] {
		t.Fatalf("held/matched/new-held safety seats missing: old-held=%v old-match=%v new-held=%v", seen["old-000"], seen["old-001"], seen["partial-000"])
	}
	if plan.Stats.RetainedSelected < 598 {
		t.Fatalf("last-good retention not visible: %+v", plan.Stats)
	}
}

func TestR133PintCompleteSnapshotMayAuthoritativelyShrink(t *testing.T) {
	now := time.Date(2026, 7, 10, 20, 0, 0, 0, time.UTC)
	// Even a newer partial head cannot re-add rows absent from the completed breadth authority.
	head := polymarket.CatalogSnapshot{At: now}
	for i := 0; i < 600; i++ {
		head.Markets = append(head.Markets, polymarket.Market{ConditionID: fmt.Sprintf("old-%03d", i)})
	}
	complete := polymarket.CatalogSnapshot{At: now.Add(-time.Minute), Complete: true}
	for i := 0; i < 199; i++ {
		complete.Markets = append(complete.Markets, polymarket.Market{ConditionID: fmt.Sprintf("live-%03d", i)})
	}
	merged := mergePintCatalog(head, complete)
	if len(merged) != 199 {
		t.Fatalf("newer complete catalog did not authoritatively replace old head: %d", len(merged))
	}
	cs := make([]pintBookCandidate, len(merged))
	for i, m := range merged {
		cs[i] = pintBookCandidate{ConditionID: m.ConditionID, Tokens: []string{"y-" + m.ConditionID, "n-" + m.ConditionID}}
	}
	plan := planPintCLOBBooks(600, len(merged), cs, 4, 2, now)
	stampPintCatalogTelemetry(&plan, head, complete, now, "complete-keyset+head-overlay")
	if len(plan.Conditions) != 199 || !plan.Stats.FullCatalogDiscovery || plan.Stats.CompleteCatalogRows != 199 || plan.Stats.RetainedSelected != 0 {
		t.Fatalf("authoritative shrink/telemetry wrong: selected=%d stats=%+v", len(plan.Conditions), plan.Stats)
	}
}

func TestR133PintCompleteShrinkKeepsOnlyCurrentMoneyDependencies(t *testing.T) {
	old := []pintBookCandidate{
		{ConditionID: "held-still-open", Slug: "held-slug", Tokens: []string{"hy", "hn"}, Held: true},
		{ConditionID: "lock-still-open", Slug: "lock-slug", Tokens: []string{"ay", "an"}, Arb: true},
		{ConditionID: "old-filler", Tokens: []string{"fy", "fn"}, Retained: true},
		{ConditionID: "closed-position", Slug: "closed-slug", Tokens: []string{"cy", "cn"}, Held: true},
		{ConditionID: "closed-lock", Slug: "closed-lock-slug", Tokens: []string{"ly", "ln"}, Arb: true},
	}
	held := map[string]bool{"held-slug": true}
	arb := map[string]bool{"lock-still-open": true}
	got := filterRetainedPintCandidates(old, held, arb, true, true, false)
	if len(got) != 2 || got[0].ConditionID != "held-still-open" || got[1].ConditionID != "lock-still-open" || !got[0].Held || !got[1].Arb {
		t.Fatalf("complete shrink safety dependencies=%+v, want only current held and active lock", got)
	}

	// Failed local reads cannot prove that a prior money dependency is gone.
	failClosed := filterRetainedPintCandidates(old, nil, nil, false, false, false)
	if len(failClosed) != 4 {
		t.Fatalf("failed dependency reads dropped prior held/lock safety seats: %+v", failClosed)
	}
}

func TestR133PintPartialTelemetryNeverClaimsComplete(t *testing.T) {
	now := time.Date(2026, 7, 10, 20, 0, 0, 0, time.UTC)
	head := polymarket.CatalogSnapshot{Markets: make([]polymarket.Market, 199), At: now.Add(-2 * time.Second), Refreshing: true}
	complete := polymarket.CatalogSnapshot{Refreshing: true}
	plan := pintBookPlan{Stats: pintBookPlanStats{Selected: 199}}
	stampPintCatalogTelemetry(&plan, head, complete, now, "partial-head-cold-start")
	if plan.Stats.FullCatalogDiscovery || plan.Stats.CompleteCatalogRows != 0 || plan.Stats.CompleteCatalogAgeSec != -1 || !plan.Stats.CatalogRefreshing || plan.Stats.PlanSource != "partial-head-cold-start" {
		t.Fatalf("partial crawl advertised as complete: %+v", plan.Stats)
	}
}

func TestR133PintAssetsColdCacheReturnsLastGoodWithoutNetworkWait(t *testing.T) {
	s := &Server{poly: polymarket.NewClient(15 * time.Second)}
	last := planPintCLOBBooks(2, 2, []pintBookCandidate{
		{ConditionID: "a", Tokens: []string{"ay", "an"}},
		{ConditionID: "b", Tokens: []string{"by", "bn"}},
	}, 4, 2, time.Now())
	s.recordPintCLOBPlan(last)
	started := time.Now()
	got := s.pintCLOBAssets(context.Background())
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("cache-only assets path blocked %.0fms", elapsed.Seconds()*1000)
	}
	if !reflect.DeepEqual(got, last.Assets) {
		t.Fatalf("cold cache returned %v, want last-good %v", got, last.Assets)
	}
}

func TestR133PintExactNoTokenAskBeatsSyntheticComplement(t *testing.T) {
	s := &Server{}
	// 1-YES bid would be 0.58, but the separately subscribed NO token can actually be bought at
	// 0.61. The lock scanner must price/pay 0.61 and carry its own 17-contract touch depth.
	c := xvlCandidate{pair: "K-PINT", aVenue: "test", bVenue: "test", sameSide: true,
		aBid: .40, aAsk: .41, bBid: .42, bAsk: .45, aAskSz: 9, aBidSz: 8,
		bBidSz: 99, bNoAsk: .61, bNoAskSz: 17}
	orients := s.xvlOrientations(c)
	if len(orients) != 2 || orients[0].bAsk != .61 || orients[0].bDepth != 17 {
		t.Fatalf("orientations=%+v, exact NO token ask/depth was not used", orients)
	}
	c.bNoAskSz = 0
	orients = s.xvlOrientations(c)
	if len(orients) != 2 || orients[0].bDepth != 0 {
		t.Fatalf("orientations=%+v, known zero NO-token depth must not borrow YES-bid liquidity", orients)
	}
}

func TestR133PolyIntCannotEnterLiveAutoOrderPath(t *testing.T) {
	s := &Server{}
	c := liveMirrorCandidate{Platform: "polymarket", Ticker: "0xresearch", Side: "YES",
		Source: "pflow", Price: .40, At: time.Now()}
	if s.enqueueLiveMirrorCandidate(c) {
		t.Fatal("research-only Poly-int candidate entered the live-auto queue")
	}
	if _, why := s.liveMirrorExecutable(t.Context(), c); why != "unsupported-live-mirror-platform" {
		t.Fatalf("live executable boundary=%q, want explicit unsupported platform", why)
	}
}
