package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r148ServerPaperBundle(system, id, opportunity string, observed time.Time) storage.ResearchRouteBundle {
	level := storage.ResearchRouteBundleLevel{Price: .4, Quantity: 2,
		FeeQuotes: []storage.ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}
	unwind := storage.ResearchRouteBundleLevel{Price: .39, Quantity: 2,
		FeeQuotes: []storage.ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}
	leg := func(index int, venue, ticker, side string, payoff []float64) storage.ResearchRouteBundleLeg {
		return storage.ResearchRouteBundleLeg{Index: index, LegID: ticker + "|" + side,
			Venue: venue, Ticker: ticker, Side: side, PayoffID: ticker, Quantity: 1,
			IntegratedCost: .4, ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1,
			BookSource: venue + "-book", SourceClockID: venue + "-clock", FeeSource: venue + "-fee",
			Levels: []storage.ResearchRouteBundleLevel{level}, Payoff: payoff, UnwindKnown: true,
			UnwindBookSource: venue + "-book", UnwindFeeSource: venue + "-fee",
			UnwindLevels: []storage.ResearchRouteBundleLevel{unwind}}
	}
	return storage.ResearchRouteBundle{
		BundleID: id, SystemID: system, Cohort: "verified-test", OpportunityID: opportunity,
		CanonicalEventID: "event:" + opportunity, CertificateHash: "cert:" + id,
		CertificateStatus: "verified", StateVectorHash: "states:" + id,
		RouteKind: "all_leg_taker", ExperimentVersion: 1, EventVersion: 1,
		Observed: observed, Size: 1, Cost: .8, Fee: .02, PayoutFloor: 1, NetFloor: .18,
		PartialFillWorst: -.4, UnwindWorst: -.04, UnwindKnown: true,
		DecisionLatencyMS: 1, LatencyKnown: true, Blocker: "staged", Evidence: map[string]any{"test": true},
		Legs: []storage.ResearchRouteBundleLeg{
			leg(0, "kalshi", "K-A", "YES", []float64{1, 0}),
			leg(1, "kalshi", "K-B", "NO", []float64{0, 1}),
		},
		States: []storage.ResearchRouteBundleState{
			{Index: 0, StateID: "a", Payout: 1}, {Index: 1, StateID: "b", Payout: 1},
		},
	}
}

func TestR148StagedPaperCheckUsesCompleteFrozenPackage(t *testing.T) {
	now := time.Now().UTC()
	b := r148ServerPaperBundle("nested-ladder-lock", "check", "check", now.Add(-time.Second))
	got := r148StagedPaperCheck(b, now, 10)
	if !got.Accept || math.Abs(got.Cost-.8) > 1e-9 || math.Abs(got.Fee-.02) > 1e-9 ||
		math.Abs(got.NetFloor-.18) > 1e-9 || got.VenueShape != "kalshi" {
		t.Fatalf("complete package check=%+v", got)
	}
	if got = r148StagedPaperCheck(b, now, .5); got.Accept || !strings.Contains(got.Reason, "capacity") {
		t.Fatalf("underfunded package was not flat: %+v", got)
	}
	bad := b
	bad.BundleID = "bad-fee"
	bad.Legs = append([]storage.ResearchRouteBundleLeg(nil), b.Legs...)
	bad.Legs[0].ExactFee = .02
	if got = r148StagedPaperCheck(bad, now, 10); got.Accept || !strings.Contains(got.Reason, "immutable receipt") {
		t.Fatalf("fee mismatch was admitted: %+v", got)
	}
	stale := b
	stale.BundleID = "stale"
	stale.Observed = now.Add(-6 * time.Second)
	if got = r148StagedPaperCheck(stale, now, 10); got.Accept || !strings.Contains(got.Reason, "no longer current") {
		t.Fatalf("stale package was admitted: %+v", got)
	}
	structural := b
	structural.BundleID = "structural"
	structural.CertificateStatus = "structural"
	if got = r148StagedPaperCheck(structural, now, 10); got.Accept || !strings.Contains(got.Reason, "not verified") {
		t.Fatalf("structural certificate was admitted: %+v", got)
	}
}

func TestR148StagedPaperSweepCompoundsComboPortfolioAndSettles(t *testing.T) {
	s := testServer(t)
	r166AllowUnverifiedMultiLegPaperForTest(s)
	ctx := context.Background()
	s.autoMu.Lock()
	s.autoOn = true
	s.autoInvert = false
	s.autoMu.Unlock()
	s.sugMu.Lock()
	s.collecting = false
	s.sugMu.Unlock()
	s.portfolioEquityReadForTest = func(context.Context, string) (bookSessionMetrics, bool) {
		return bookSessionMetrics{NetUSD: 0, UnitsComplete: true, OpenComplete: true}, true
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	b := r148ServerPaperBundle("nested-ladder-lock", "sweep", "sweep-opportunity", now)
	if ok, err := s.store.InsertResearchRouteBundle(ctx, b); err != nil || !ok {
		t.Fatalf("insert bundle: ok=%v err=%v", ok, err)
	}
	s.sweepR148StagedPaperBundles(ctx)
	stats, err := s.store.StagedPaperBundleSystemStats(ctx)
	if err != nil || len(stats) != 1 || stats[0].Open != 1 || stats[0].N != 0 {
		t.Fatalf("post-admission stats=%+v err=%v", stats, err)
	}
	funds, err := s.store.StagedPaperBundleFundsSince(ctx, time.Time{})
	if err != nil || funds.OpenCount != 1 || math.Abs(funds.OpenCost-.82) > 1e-9 {
		t.Fatalf("post-admission funds=%+v err=%v", funds, err)
	}
	gradeAt := now.Add(time.Hour)
	if ok, err := s.store.AppendResearchRouteBundleGrade(ctx, storage.ResearchRouteBundleGrade{
		BundleID: b.BundleID, Observed: gradeAt, OutcomeStatus: "counterfactual_settled",
		Payout: 1, RealizedNet: .18, CapitalSeconds: time.Hour.Seconds(),
		Reason: "authoritative exact per-leg settlement", EvidenceJSON: `{"authority":"venue"}`,
	}); err != nil || !ok {
		t.Fatalf("grade bundle: ok=%v err=%v", ok, err)
	}
	s.sweepR148StagedPaperBundles(ctx)
	stats, err = s.store.StagedPaperBundleSystemStats(ctx)
	if err != nil || len(stats) != 1 || stats[0].Open != 0 || stats[0].N != 1 || math.Abs(stats[0].Realized-.18) > 1e-9 {
		t.Fatalf("post-settlement stats=%+v err=%v", stats, err)
	}
	if metrics, ok := s.bookSessionClosedMetrics(ctx, vbCombos); !ok || metrics.Bets != 1 ||
		metrics.Open != 0 || math.Abs(metrics.NetUSD-.18) > 1e-9 || math.Abs(metrics.Units-1) > 1e-9 {
		t.Fatalf("Combo portfolio did not receive staged package P&L: metrics=%+v ok=%v", metrics, ok)
	}
	// A larger current-epoch profit increases the dollar capacity without any contract hard cap.
	s.portfolioEquityReadForTest = func(context.Context, string) (bookSessionMetrics, bool) {
		return bookSessionMetrics{NetUSD: 600, UnitsComplete: true, OpenComplete: true}, true
	}
	s.invalidatePortfolioEquityCache()
	if got := s.r148StagedPaperBudget(ctx); got < 59.99 || got > 60.01 {
		t.Fatalf("compounded Combo package budget=%.4f want 60", got)
	}
}

func TestR148XVLockPaperIsExplicitlyNotLiveAuthority(t *testing.T) {
	if r148LegacyLockstackFundingAllowed() {
		t.Fatal("legacy lockstack and exact staged xvlock Paper must not fund the same opportunity")
	}
	b := r148ServerPaperBundle("xvlock", "xv", "xv-opportunity", time.Now().UTC())
	b.RouteKind = "cross_venue_non_atomic"
	b.Legs[1].Venue = "polyus"
	b.Legs[1].BookSource = "polyus-book"
	b.Legs[1].SourceClockID = "polyus-clock"
	b.Legs[1].FeeSource = "polyus-fee"
	got := r148StagedPaperCheck(b, time.Now().UTC(), 10)
	if !got.Accept || got.VenueShape != "crossvenue" || !strings.Contains(got.LiveState, "PAPER_ONLY_EXTERNAL_LIVE_BLOCK") {
		t.Fatalf("xvlock Paper/LIVE classification=%+v", got)
	}
}
