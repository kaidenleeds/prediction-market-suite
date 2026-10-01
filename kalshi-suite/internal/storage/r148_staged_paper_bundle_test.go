package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func r148PaperBundleFixture(id, opportunity string, observed time.Time) ResearchRouteBundle {
	level := ResearchRouteBundleLevel{Price: .40, Quantity: 2,
		FeeQuotes: []ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}
	unwind := ResearchRouteBundleLevel{Price: .39, Quantity: 2,
		FeeQuotes: []ResearchRouteBundleFeeQuote{{Quantity: 1, Total: .01}}}
	return ResearchRouteBundle{
		BundleID: id, SystemID: "xvlock", Cohort: "test", OpportunityID: opportunity,
		CanonicalEventID: "event:test", CertificateHash: "cert:" + id,
		CertificateStatus: "verified", StateVectorHash: "states:" + id,
		RouteKind: "cross_venue_non_atomic", ExperimentVersion: 1, EventVersion: 1,
		Observed: observed, Size: 1, Cost: .8, Fee: .02, PayoutFloor: 1, NetFloor: .18,
		PartialFillWorst: -.41, UnwindWorst: -.04, UnwindKnown: true,
		DecisionLatencyMS: 1, LatencyKnown: true, Blocker: "Paper only",
		Legs: []ResearchRouteBundleLeg{
			{Index: 0, LegID: "K|YES", Venue: "kalshi", Ticker: "K", Side: "YES", PayoffID: "K",
				Quantity: 1, IntegratedCost: .4, ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1,
				BookSource: "kalshi-book", SourceClockID: "clock-k", FeeSource: "kalshi-fee",
				Levels: []ResearchRouteBundleLevel{level}, Payoff: []float64{1, 0}, UnwindKnown: true,
				UnwindBookSource: "kalshi-book", UnwindFeeSource: "kalshi-fee", UnwindLevels: []ResearchRouteBundleLevel{unwind}},
			{Index: 1, LegID: "P|NO", Venue: "polyus", Ticker: "P", Side: "NO", PayoffID: "P",
				Quantity: 1, IntegratedCost: .4, ExactFee: .01, VisibleDepth: 2, Tick: .01, Age: .1,
				BookSource: "polyus-book", SourceClockID: "clock-p", FeeSource: "polyus-fee",
				Levels: []ResearchRouteBundleLevel{level}, Payoff: []float64{0, 1}, UnwindKnown: true,
				UnwindBookSource: "polyus-book", UnwindFeeSource: "polyus-fee", UnwindLevels: []ResearchRouteBundleLevel{unwind}},
		},
		States: []ResearchRouteBundleState{
			{Index: 0, StateID: "yes", Payout: 1}, {Index: 1, StateID: "no", Payout: 1},
		}, Evidence: map[string]any{"fixture": true},
	}
}

func r148PaperDecision(b ResearchRouteBundle, now time.Time, accept bool) StagedPaperBundleDecision {
	return StagedPaperBundleDecision{
		PackageID: "staged-paper:" + b.BundleID, BundleID: b.BundleID, SystemID: b.SystemID,
		OpportunityID: b.OpportunityID, CanonicalEventID: b.CanonicalEventID, Cohort: b.Cohort,
		VenueShape: "crossvenue", LiveState: "PAPER_ONLY_EXTERNAL_LIVE_BLOCK", Reason: "test decision",
		EvidenceJSON: `{"fixture":true}`, Candidate: b.Observed, Decided: now,
		Size: b.Size, Cost: b.Cost, Fee: b.Fee, PayoutFloor: b.PayoutFloor,
		NetFloor: b.NetFloor, LegCount: len(b.Legs), Accept: accept,
	}
}

func TestR148StagedPaperBundleIsAppendOnlyDistinctAndAuthoritativelySettled(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Truncate(time.Microsecond)
	first := r148PaperBundleFixture("bundle-1", "same-opportunity", now)
	second := r148PaperBundleFixture("bundle-2", "same-opportunity", now.Add(time.Millisecond))
	for _, b := range []ResearchRouteBundle{first, second} {
		if inserted, err := st.InsertResearchRouteBundle(ctx, b); err != nil || !inserted {
			t.Fatalf("insert bundle %s: inserted=%v err=%v", b.BundleID, inserted, err)
		}
	}
	tampered := r148PaperDecision(first, now.Add(time.Second), true)
	tampered.Cost = .79
	if inserted, accepted, err := st.AppendStagedPaperBundleDecision(ctx, tampered); err == nil || inserted || accepted {
		t.Fatalf("tampered source receipt entered Paper: inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	inserted, accepted, err := st.AppendStagedPaperBundleDecision(ctx, r148PaperDecision(first, now.Add(time.Second), true))
	if err != nil || !inserted || !accepted {
		t.Fatalf("first decision inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	if inserted, accepted, err = st.AppendStagedPaperBundleDecision(ctx, r148PaperDecision(first, now.Add(2*time.Second), true)); err != nil || inserted || accepted {
		t.Fatalf("restart/duplicate decision inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	inserted, accepted, err = st.AppendStagedPaperBundleDecision(ctx, r148PaperDecision(second, now.Add(2*time.Second), true))
	if err != nil || !inserted || accepted {
		t.Fatalf("second capacity row must be durable but flat: inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	funds, err := st.StagedPaperBundleFundsSince(ctx, time.Time{})
	if err != nil || funds.OpenCount != 1 || math.Abs(funds.OpenCost-.82) > 1e-9 {
		t.Fatalf("pre-settlement funds=%+v err=%v", funds, err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE staged_paper_bundle_attempts SET total_cost=.01 WHERE package_id=?`, "staged-paper:bundle-1"); err == nil {
		t.Fatal("immutable attempt accepted an UPDATE")
	}
	gradeAt := now.Add(time.Hour)
	if ok, err := st.AppendResearchRouteBundleGrade(ctx, ResearchRouteBundleGrade{
		BundleID: first.BundleID, Observed: gradeAt, OutcomeStatus: "counterfactual_settled",
		Payout: 1, RealizedNet: .18, CapitalSeconds: time.Hour.Seconds(),
		Reason: "authoritative exact venue settlements", EvidenceJSON: `{"authority":"venue_settlements"}`,
	}); err != nil || !ok {
		t.Fatalf("append authoritative grade: ok=%v err=%v", ok, err)
	}
	pending, err := st.PendingStagedPaperBundleSettlements(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	if ok, err := st.AppendStagedPaperBundleSettlement(ctx, pending[0]); err != nil || !ok {
		t.Fatalf("append settlement: ok=%v err=%v", ok, err)
	}
	if ok, err := st.AppendStagedPaperBundleSettlement(ctx, pending[0]); err != nil || ok {
		t.Fatalf("duplicate settlement: ok=%v err=%v", ok, err)
	}
	funds, err = st.StagedPaperBundleFundsSince(ctx, time.Time{})
	if err != nil || funds.OpenCount != 0 || funds.ClosedCount != 1 ||
		math.Abs(funds.Realized-.18) > 1e-9 || math.Abs(funds.ClosedUnits-1) > 1e-9 {
		t.Fatalf("settled funds=%+v err=%v", funds, err)
	}
	stats, err := st.StagedPaperBundleSystemStats(ctx)
	if err != nil || len(stats) != 1 || stats[0].N != 1 || stats[0].Attempts != 2 || stats[0].Rejected != 1 {
		t.Fatalf("system stats=%+v err=%v", stats, err)
	}
}

func TestR148StagedPaperBundleSurvivesRestartWithoutDuplicateExposure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	now := time.Now().UTC().Truncate(time.Microsecond)
	b := r148PaperBundleFixture("restart-bundle", "restart-opportunity", now)
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := st.InsertResearchRouteBundle(ctx, b); err != nil || !ok {
		t.Fatalf("insert bundle: ok=%v err=%v", ok, err)
	}
	if inserted, accepted, err := st.AppendStagedPaperBundleDecision(ctx, r148PaperDecision(b, now.Add(time.Second), true)); err != nil || !inserted || !accepted {
		t.Fatalf("accept before restart: inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if inserted, accepted, err := st.AppendStagedPaperBundleDecision(ctx, r148PaperDecision(b, now.Add(2*time.Second), true)); err != nil || inserted || accepted {
		t.Fatalf("restart duplicated package: inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	funds, err := st.StagedPaperBundleFundsSince(ctx, time.Time{})
	if err != nil || funds.OpenCount != 1 || math.Abs(funds.OpenCost-.82) > 1e-9 {
		t.Fatalf("restart funds=%+v err=%v", funds, err)
	}
}

func TestR166StagedPaperResetExcludesOldOpenAndLaterSettlement(t *testing.T) {
	ctx := context.Background()
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC().Add(time.Millisecond).Truncate(time.Microsecond)
	old := r148PaperBundleFixture("r166-old-bundle", "r166-old-opportunity", now)
	if ok, err := st.InsertResearchRouteBundle(ctx, old); err != nil || !ok {
		t.Fatalf("insert old bundle: ok=%v err=%v", ok, err)
	}
	if inserted, accepted, err := st.AppendStagedPaperBundleDecision(ctx,
		r148PaperDecision(old, now.Add(time.Second), true)); err != nil || !inserted || !accepted {
		t.Fatalf("old decision inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	epoch := now.Add(2 * time.Second)
	receipt, err := st.ResetStagedPaperBundlePortfolio(ctx, epoch)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ExcludedAttempts != 1 || receipt.ExcludedOpen != 1 {
		t.Fatalf("reset receipt=%+v", receipt)
	}
	if funds, err := st.StagedPaperBundleFundsSince(ctx, time.Time{}); err != nil || funds.OpenCount != 0 || funds.Realized != 0 {
		t.Fatalf("old open contaminated current funds=%+v err=%v", funds, err)
	}
	gradeAt := epoch.Add(time.Hour)
	if ok, err := st.AppendResearchRouteBundleGrade(ctx, ResearchRouteBundleGrade{
		BundleID: old.BundleID, Observed: gradeAt, OutcomeStatus: "counterfactual_settled",
		Payout: 1, RealizedNet: .18, CapitalSeconds: time.Hour.Seconds(),
		Reason: "authoritative old settlement", EvidenceJSON: `{"authority":"venue_settlements"}`,
	}); err != nil || !ok {
		t.Fatalf("grade old bundle: ok=%v err=%v", ok, err)
	}
	pending, err := st.PendingStagedPaperBundleSettlements(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("historical pending settlement=%+v err=%v", pending, err)
	}
	if ok, err := st.AppendStagedPaperBundleSettlement(ctx, pending[0]); err != nil || !ok {
		t.Fatalf("settle old package: ok=%v err=%v", ok, err)
	}
	if funds, err := st.StagedPaperBundleFundsSince(ctx, time.Time{}); err != nil || funds.OpenCount != 0 || funds.ClosedCount != 0 || funds.Realized != 0 {
		t.Fatalf("old settlement contaminated current funds=%+v err=%v", funds, err)
	}

	fresh := r148PaperBundleFixture("r166-current-bundle", "r166-current-opportunity", epoch.Add(time.Second))
	if ok, err := st.InsertResearchRouteBundle(ctx, fresh); err != nil || !ok {
		t.Fatalf("insert current bundle: ok=%v err=%v", ok, err)
	}
	if inserted, accepted, err := st.AppendStagedPaperBundleDecision(ctx,
		r148PaperDecision(fresh, epoch.Add(2*time.Second), true)); err != nil || !inserted || !accepted {
		t.Fatalf("current decision inserted=%v accepted=%v err=%v", inserted, accepted, err)
	}
	if funds, err := st.StagedPaperBundleFundsSince(ctx, time.Time{}); err != nil || funds.OpenCount != 1 || math.Abs(funds.OpenCost-.82) > 1e-9 {
		t.Fatalf("current funds=%+v err=%v", funds, err)
	}
	stats, err := st.StagedPaperBundleSystemStats(ctx)
	if err != nil || len(stats) != 1 || stats[0].Attempts != 1 || stats[0].Open != 1 {
		t.Fatalf("current stats=%+v err=%v", stats, err)
	}
	var attempts, events int
	if err = st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM staged_paper_bundle_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = st.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM staged_paper_bundle_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || events != 3 {
		t.Fatalf("append-only history attempts=%d events=%d", attempts, events)
	}
}
