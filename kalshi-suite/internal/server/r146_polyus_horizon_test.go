package server

import (
	"context"
	"testing"
	"time"
)

func TestR146PolyUSFundedClockDoesNotTreatEventEndDateAsSettlement(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = nil
	now := time.Now().UTC()

	// This is the production failure shape: PolyUS supplies an event boundary in Resolve, the
	// scheduled start remains usable, and the venue live flag can lag. Collection correctly uses
	// start+3.5h; the final funded check must use the identical strict clock.
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{{
		Slug:    "in-progress",
		Start:   now.Add(-time.Hour).Format(time.RFC3339),
		Resolve: now.Add(-time.Hour).Format(time.RFC3339),
		Live:    false,
	}}
	s.polyUSMu.Unlock()

	hours := s.paperEntryResolveHours(context.Background(), "polyus", "in-progress", "")
	if hours < 2.45 || hours > 2.55 {
		t.Fatalf("PolyUS funded clock=%v, want about 2.5h from start+3.5h", hours)
	}
	if !s.paperEntryHorizonNow(context.Background(), "polyus", "in-progress", "") {
		t.Fatal("same PolyUS signal was eligible at collection but rejected at the final funded clock")
	}
}

func TestR146PolyUSFundedClockStillRejectsAfterEstimatedGameEnd(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = nil
	now := time.Now().UTC()
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{{
		Slug:    "ended",
		Start:   now.Add(-4 * time.Hour).Format(time.RFC3339),
		Resolve: now.Add(time.Hour).Format(time.RFC3339), // not settlement authority
		Live:    false,
	}}
	s.polyUSMu.Unlock()

	if got := s.paperEntryResolveHours(context.Background(), "polyus", "ended", ""); got >= 0 {
		t.Fatalf("ended PolyUS funded clock=%v, want negative", got)
	}
	if s.paperEntryHorizonNow(context.Background(), "polyus", "ended", "") {
		t.Fatal("ended PolyUS market entered the funded horizon")
	}
}

func TestR148PolyUSLiveFlagCannotMintPerpetualFundedHorizon(t *testing.T) {
	s := testServer(t)
	s.paperHorizonFn = nil
	now := time.Now().UTC()
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{{Slug: "stale-live", Start: now.Add(-5 * time.Hour).Format(time.RFC3339), Live: true},
		{Slug: "live-no-clock", Live: true}}
	s.polyUSMu.Unlock()

	if got := s.paperEntryResolveHours(context.Background(), "polyus", "stale-live", ""); got >= 0 {
		t.Fatalf("stale LIVE flag minted positive horizon %v", got)
	}
	if s.paperEntryHorizonNow(context.Background(), "polyus", "stale-live", "") {
		t.Fatal("stale LIVE flag admitted a funded entry after bounded game end")
	}
	if got := s.paperEntryResolveHours(context.Background(), "polyus", "live-no-clock", ""); got != 0 {
		t.Fatalf("clockless LIVE market horizon=%v, want unknown/zero", got)
	}
}

func TestR146PolyUSDepthPlannerUsesSameSportsClock(t *testing.T) {
	now := time.Date(2026, 7, 15, 14, 0, 0, 0, time.UTC)
	start := now.Add(-time.Hour)
	rows := []polyUSMarket{
		{
			Slug:    "in-progress-sport",
			Start:   start.Format(time.RFC3339),
			Resolve: start.Format(time.RFC3339), // event boundary, not settlement
			Live:    true,
		},
		{
			Slug:    "crawl-only-future",
			Resolve: now.Add(3 * time.Hour).Format(time.RFC3339),
		},
	}

	got := polyUSDepthCandidates(rows, now, 4, 2)
	if len(got) != 2 {
		t.Fatalf("planner candidates=%d, want 2", len(got))
	}
	if !got[0].Live || !got[0].Near || got[0].ResolveAt.Sub(start.Add(3*time.Hour+30*time.Minute)) != 0 {
		t.Fatalf("in-progress sport was not prioritized on start+3.5h: %+v", got[0])
	}
	if got[1].Live || !got[1].Near || got[1].ResolveAt.Sub(now.Add(3*time.Hour)) != 0 {
		t.Fatalf("crawl-only endDate fallback changed: %+v", got[1])
	}
}

func TestR146CryptoClassifierUsesTickerGrammarNotNameSubstrings(t *testing.T) {
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"KXBTC-26JUL15", true},
		{"btc-up-or-down-july-15", true},
		{"market-1 Bitcoin price", true},
		{"marina-bassols-win Marina Bassols", false},
		{"solary-eclipse-map-1 Solary Eclipse", false},
	} {
		if got := isCryptoTicker(tc.text); got != tc.want {
			t.Fatalf("isCryptoTicker(%q)=%v want %v", tc.text, got, tc.want)
		}
	}
}
