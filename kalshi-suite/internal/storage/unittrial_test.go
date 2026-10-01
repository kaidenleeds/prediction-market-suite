package storage

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestR132UnitTrialsAreBankrollFreeDedupedAndGraded(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Now().UTC().Add(-24 * time.Hour)
	u := UnitTrial{OpenedTS: opened, Family: "edge-a", Platform: "kalshi", Ticker: "KXUNIT",
		Side: "YES", Ask: 0.40, FeePC: 0.01, FeeKnown: true, FeeSource: "kalshi:test", Depth: 12, QuoteSource: "kalshi-ws"}
	if inserted, err := st.InsertUnitTrial(ctx, u); err != nil || !inserted {
		t.Fatalf("first insert=%v err=%v", inserted, err)
	}
	if inserted, err := st.InsertUnitTrial(ctx, u); err != nil || inserted {
		t.Fatalf("duplicate insert=%v err=%v", inserted, err)
	}
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "KXUNIT", Side: "YES",
		SignalType: "edge-a", EntryPrice: 0.39}); err != nil {
		t.Fatal(err)
	}
	closed := opened.Add(24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1,settle_val=1,resolved_at=? WHERE ticker='KXUNIT'`, closed); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ResolveUnitTrialsFromSignals(ctx, 10); err != nil || n != 1 {
		t.Fatalf("resolved=%d err=%v", n, err)
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	g := stats[0]
	wantPC := 0.59
	wantROI := wantPC / 0.41
	if g.N != 1 || g.Open != 0 || math.Abs(g.MeanPC-wantPC) > 1e-9 || math.Abs(g.MeanFeePC-0.01) > 1e-9 || math.Abs(g.ReturnPerDollar-wantROI) > 1e-9 || math.Abs(g.CapitalDay-wantROI) > 1e-6 {
		t.Fatalf("bad unit stats: %+v", g)
	}
	digest, err := st.UnitTrialDigest(ctx, time.Now().UTC())
	if err != nil || len(digest) != 1 || digest[0].N != 1 || digest[0].Open != 0 ||
		math.Abs(digest[0].MeanPC-wantPC) > 1e-9 || digest[0].NetPerCalendarDay < .58 || digest[0].NetPerCalendarDay > .60 {
		t.Fatalf("compact unit digest=%+v err=%v", digest, err)
	}
	routeReport, err := st.ResearchRouteReport(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	routeCounts := routeReport["counts"].(map[string]int)
	if routeCounts["routes"] != 1 || routeCounts["grade_events"] != 1 {
		t.Fatalf("unit trial was not linked through route settlement: %+v", routeReport)
	}
}

func TestR143UnitTrialStatsKeepModelAndStrategyRoutesSeparate(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Add(-time.Hour)
	for _, u := range []UnitTrial{
		{OpenedTS: now, Family: "same-system", Platform: "kalshi", OriginLayer: "model",
			Ticker: "KX-MODEL", Side: "YES", Ask: .40, FeePC: .01, FeeKnown: true,
			FeeSource: "kalshi:test", Depth: 2, QuoteSource: "kalshi-ws"},
		{OpenedTS: now, Family: "same-system", Platform: "kalshi", OriginLayer: "strategy",
			Ticker: "KX-STRATEGY", Side: "YES", Ask: .50, FeePC: .01, FeeKnown: true,
			FeeSource: "kalshi:test", Depth: 2, QuoteSource: "strategy-decision/kalshi-ws"},
	} {
		if inserted, ierr := st.InsertUnitTrial(ctx, u); ierr != nil || !inserted {
			t.Fatalf("insert %+v = %v, %v", u, inserted, ierr)
		}
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=CASE origin_layer WHEN 'strategy' THEN .20 ELSE .10 END,return_per_dollar=.25,capital_day=.25`, now.Add(time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 2 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	got := map[string]float64{}
	for _, row := range stats {
		got[row.OriginLayer] = row.MeanPC
	}
	if math.Abs(got["model"]-.10) > 1e-9 || math.Abs(got["strategy"]-.20) > 1e-9 {
		t.Fatalf("origin routes pooled or mislabeled: %+v", got)
	}
}

func TestR144UnitTrialProofAveragesWithinMarketBeforeCountingEvidence(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	for _, trial := range []UnitTrial{
		{OpenedTS: now.Add(-3 * time.Hour), Family: "market-cluster", Platform: "kalshi", Ticker: "SAME", Side: "YES", Episode: 1, Ask: .10, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
		{OpenedTS: now.Add(-2 * time.Hour), Family: "market-cluster", Platform: "kalshi", Ticker: "SAME", Side: "YES", Episode: 2, Ask: .90, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
		{OpenedTS: now.Add(-time.Hour), Family: "market-cluster", Platform: "kalshi", Ticker: "OTHER", Side: "YES", Episode: 1, Ask: .40, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
	} {
		if inserted, insertErr := st.InsertUnitTrial(ctx, trial); insertErr != nil || !inserted {
			t.Fatalf("insert %+v = %v, %v", trial, inserted, insertErr)
		}
	}
	closed := now.Format(time.RFC3339Nano)
	for _, update := range []struct {
		ticker  string
		episode int
		pnl     float64
	}{{"SAME", 1, .90}, {"SAME", 2, -.90}, {"OTHER", 1, .60}} {
		if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=?,return_per_dollar=? WHERE ticker=? AND episode=?`,
			closed, update.pnl, update.pnl, update.ticker, update.episode); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	got := stats[0]
	wantSD := math.Sqrt(.18)
	if got.N != 3 || got.SettledMarkets != 2 || math.Abs(got.MeanPC-.30) > 1e-12 ||
		math.Abs(got.SDPC-wantSD) > 1e-12 {
		t.Fatalf("rows were pseudo-replicated instead of market-clustered: %+v", got)
	}
	leaders, err := st.UnitTrialLeaderboard(ctx, now.Add(time.Hour))
	if err != nil || len(leaders) != 1 {
		t.Fatalf("leaders=%+v err=%v", leaders, err)
	}
	lead := leaders[0]
	if lead.N != 3 || lead.SettledMarkets != 2 || math.Abs(lead.MeanPC-.30) > 1e-12 ||
		math.Abs(lead.SDPC-wantSD) > 1e-12 || math.Abs(lead.TotalPnL-.60) > 1e-12 {
		t.Fatalf("leaderboard market proof or raw economics wrong: %+v", lead)
	}
}

func TestAuditor67SettledUnitTrialCannotBeConflictUpgraded(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	originalOpened := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	original := UnitTrial{OpenedTS: originalOpened, Family: "immutable", Platform: "kalshi",
		Ticker: "KXIMMUTABLE", Side: "YES", Episode: 7, Category: "original", Ask: .40,
		FeePC: 0, FeeKnown: false, Depth: 0, QuoteSource: "original-unknown", ResolveHours: 24}
	if inserted, ierr := st.InsertUnitTrial(ctx, original); ierr != nil || !inserted {
		t.Fatalf("insert original=%v err=%v", inserted, ierr)
	}
	closed := originalOpened.Add(24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,settle_val=1,
pnl_pc=.60,return_per_dollar=1.5,capital_day=1.5 WHERE family='immutable'`, closed); err != nil {
		t.Fatal(err)
	}

	upgrade := original
	upgrade.OpenedTS = originalOpened.Add(2 * time.Hour)
	upgrade.Category = "rewritten"
	upgrade.Ask = .35
	upgrade.FeePC = .01
	upgrade.FeeKnown = true
	upgrade.FeeSource = "kalshi:exact"
	upgrade.Depth = 25
	upgrade.QuoteSource = "later-exact"
	upgrade.ResolveHours = 12
	if changed, ierr := st.InsertUnitTrial(ctx, upgrade); ierr != nil || changed {
		t.Fatalf("settled conflict upgrade changed=%v err=%v", changed, ierr)
	}

	var opened, gotClosed, category, feeSource, quoteSource string
	var ask, fee, depth, resolveHours, settle, pnl, roi, capitalDay float64
	var feeKnown, settled int
	if err := st.db.QueryRowContext(ctx, `SELECT opened_ts,closed_ts,category,ask,fee_pc,fee_known,
fee_source,depth,quote_source,resolve_hours,settled,settle_val,pnl_pc,return_per_dollar,capital_day
FROM unit_trials WHERE family='immutable'`).Scan(&opened, &gotClosed, &category, &ask, &fee,
		&feeKnown, &feeSource, &depth, &quoteSource, &resolveHours, &settled, &settle, &pnl, &roi, &capitalDay); err != nil {
		t.Fatal(err)
	}
	if opened != originalOpened.Format(time.RFC3339Nano) || gotClosed != closed || category != "original" ||
		ask != .40 || fee != 0 || feeKnown != 0 || feeSource != "" || depth != 0 ||
		quoteSource != "original-unknown" || resolveHours != 24 || settled != 1 || settle != 1 ||
		pnl != .60 || roi != 1.5 || capitalDay != 1.5 {
		t.Fatalf("settled unit trial was rewritten: opened=%q closed=%q category=%q ask=%v fee=%v fee_known=%d fee_source=%q depth=%v quote=%q resolve=%v settled=%d settle=%v pnl=%v roi=%v capital_day=%v",
			opened, gotClosed, category, ask, fee, feeKnown, feeSource, depth, quoteSource, resolveHours,
			settled, settle, pnl, roi, capitalDay)
	}
}

func TestR133UnitTrialCostBaselineExcludesOpenRows(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Now().UTC().Add(-24 * time.Hour)
	for _, u := range []UnitTrial{
		{OpenedTS: opened, Family: "edge-cost", Platform: "kalshi", Ticker: "SETTLED",
			Side: "YES", Ask: 0.20, FeePC: 0.01, FeeKnown: true, FeeSource: "kalshi:test", Depth: 10, QuoteSource: "kalshi-ws"},
		{OpenedTS: opened, Family: "edge-cost", Platform: "kalshi", Ticker: "OPEN-EXPENSIVE",
			Side: "YES", Ask: 0.90, FeePC: 0.04, FeeKnown: true, FeeSource: "kalshi:test", Depth: 10, QuoteSource: "kalshi-ws"},
	} {
		if inserted, ierr := st.InsertUnitTrial(ctx, u); ierr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", u.Ticker, inserted, ierr)
		}
	}
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "SETTLED", Side: "YES",
		SignalType: "edge-cost", EntryPrice: 0.20}); err != nil {
		t.Fatal(err)
	}
	closed := opened.Add(24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1,settle_val=1,resolved_at=? WHERE ticker='SETTLED'`, closed); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ResolveUnitTrialsFromSignals(ctx, 10); err != nil || n != 1 {
		t.Fatalf("resolved=%d err=%v", n, err)
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	g := stats[0]
	if g.N != 1 || g.Open != 1 || math.Abs(g.MeanAsk-0.20) > 1e-12 || math.Abs(g.MeanFeePC-0.01) > 1e-12 {
		t.Fatalf("open rows contaminated settled cost baseline: %+v", g)
	}
}

func TestR135cUnitTrialCapitalDayUsesExactSecondsRatio(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Date(2026, 7, 11, 10, 0, 0, 0, time.UTC)
	trial := UnitTrial{OpenedTS: opened, Family: "ten-minute", Platform: "kalshi", Ticker: "TEN",
		Side: "YES", Ask: .50, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws"}
	if inserted, ierr := st.InsertUnitTrial(ctx, trial); ierr != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, ierr)
	}
	if err := st.InsertSignal(ctx, Signal{Platform: "kalshi", Ticker: "TEN", Side: "YES", SignalType: "ten-minute", EntryPrice: .50}); err != nil {
		t.Fatal(err)
	}
	closed := opened.Add(10 * time.Minute).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `UPDATE signal_log SET resolved=1,settle_val=1,resolved_at=? WHERE ticker='TEN'`, closed); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ResolveUnitTrialsFromSignals(ctx, 10); err != nil || n != 1 {
		t.Fatalf("resolve n=%d err=%v", n, err)
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	want := .50 / (.50 * (10.0 / 1440.0))
	if math.Abs(stats[0].CapitalDay-want) > 1e-5 {
		t.Fatalf("ten-minute rate was bucketed: got=%v want=%v", stats[0].CapitalDay, want)
	}
}

func TestR133UnitTrialStatsOpenOnlyGroupIsZeroEvidence(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	inserted, err := st.InsertUnitTrial(ctx, UnitTrial{
		OpenedTS: time.Now().UTC(), Family: "open-only", Platform: "polyus", Ticker: "OPEN",
		Side: "YES", Ask: 0.88, FeePC: 0.03, FeeKnown: true, FeeSource: "polyus:test", Depth: 5, QuoteSource: "polyus-ws",
	})
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	g := stats[0]
	if g.N != 0 || g.Open != 1 || g.MeanPC != 0 || g.SDPC != 0 || g.MeanAsk != 0 || g.MeanFeePC != 0 {
		t.Fatalf("open-only group must publish zero realized evidence: %+v", g)
	}
}

func TestR135UnitTrialLeaderboardIsOneShareCalendarRateAndRatioOfSums(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	first := now.Add(-48 * time.Hour)
	second := now.Add(-24 * time.Hour)
	for _, u := range []UnitTrial{
		{OpenedTS: first, Family: "rate", Platform: "kalshi", Ticker: "A", Side: "YES", Ask: .10, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
		{OpenedTS: second, Family: "rate", Platform: "kalshi", Ticker: "B", Side: "YES", Ask: .90, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
		{OpenedTS: second, Family: "rate", Platform: "kalshi", Ticker: "OPEN", Side: "YES", Ask: .50, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
	} {
		if inserted, ierr := st.InsertUnitTrial(ctx, u); ierr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", u.Ticker, inserted, ierr)
		}
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=.10 WHERE ticker='A'`, first.Add(24*time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	// Ten minutes proves the leaderboard uses the exact clock rather than an hourly bucket.
	if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=-.05 WHERE ticker='B'`, second.Add(10*time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	stats, err := st.UnitTrialLeaderboard(ctx, now)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	g := stats[0]
	// The unresolved 50¢ share contributes one day of occupied capital through as-of, but no P&L.
	wantHoldDays := 1.0 + 10.0/1440.0 + 1.0
	wantDollarDays := .10*1.0 + .90*(10.0/1440.0) + .50*1.0
	if g.N != 2 || g.Open != 1 || g.Total != 3 || math.Abs(g.TrackedDays-2) > 1e-12 ||
		math.Abs(g.TrackedSeconds-2*24*60*60) > 1e-12 ||
		math.Abs(g.OpportunitiesPerDay-1.5) > 1e-12 || math.Abs(g.TotalPnL-.05) > 1e-12 ||
		math.Abs(g.NetPerCalendarDay-.025) > 1e-12 ||
		math.Abs(g.OccupiedShareSeconds-wantHoldDays*24*60*60) > 1e-9 ||
		math.Abs(g.OccupiedShareDays-wantHoldDays) > 1e-12 ||
		math.Abs(g.NetPerOccupiedShareDay-.05/wantHoldDays) > 1e-12 ||
		math.Abs(g.EntryDollarSeconds-wantDollarDays*24*60*60) > 1e-9 ||
		math.Abs(g.EntryDollarDays-wantDollarDays) > 1e-12 ||
		math.Abs(g.ReturnPerDollarDay-.05/wantDollarDays) > 1e-12 {
		t.Fatalf("bad one-share profit-rate row: %+v", g)
	}
}

func TestR135cUnitTrialLeaderboardYoungHistoryUsesExactSeconds(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	if inserted, ierr := st.InsertUnitTrial(ctx, UnitTrial{
		OpenedTS: now.Add(-30 * time.Second), Family: "young", Platform: "kalshi",
		Ticker: "Y", Side: "YES", Ask: .25, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1,
	}); ierr != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, ierr)
	}
	stats, err := st.UnitTrialLeaderboard(ctx, now)
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats=%+v err=%v", stats, err)
	}
	g := stats[0]
	if math.Abs(g.TrackedSeconds-30) > 1e-12 || math.Abs(g.TrackedDays-30.0/86400.0) > 1e-12 ||
		math.Abs(g.OccupiedShareSeconds-30) > 1e-12 || math.Abs(g.OpportunitiesPerDay-2880) > 1e-9 {
		t.Fatalf("young history was bucketed instead of timed exactly: %+v", g)
	}
}

func TestR135cUnitTrialLeaderboardSeparatesOriginAndRejectsFutureEvidence(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC)
	trials := []UnitTrial{
		{OpenedTS: now.Add(-time.Hour), Family: "origin", Platform: "kalshi", Ticker: "SAME", Side: "YES", Ask: .25, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "kalshi-ws"},
		{OpenedTS: now.Add(-time.Hour), Family: "origin", Platform: "kalshi", Ticker: "SAME", Side: "YES", Ask: .25, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1, QuoteSource: "strategy-decision/kalshi-ws"},
		{OpenedTS: now.Add(time.Hour), Family: "future-open", Platform: "kalshi", Ticker: "FUTURE", Side: "YES", Ask: .25, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
		{OpenedTS: now.Add(-time.Hour), Family: "future-close", Platform: "kalshi", Ticker: "BAD-CLOSE", Side: "YES", Ask: .25, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
	}
	for _, trial := range trials {
		if inserted, ierr := st.InsertUnitTrial(ctx, trial); ierr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", trial.Ticker, inserted, ierr)
		}
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=.75 WHERE ticker='BAD-CLOSE'`, now.Add(time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	stats, err := st.UnitTrialLeaderboard(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	byKey := map[string]UnitTrialLeaderboardStat{}
	for _, stat := range stats {
		byKey[stat.Family+"/"+stat.OriginLayer] = stat
	}
	if len(stats) != 3 || byKey["origin/model"].Total != 1 || byKey["origin/strategy"].Total != 1 {
		t.Fatalf("origin layers were pooled or future open was retained: %+v", stats)
	}
	bad := byKey["future-close/model"]
	if bad.N != 0 || bad.Open != 1 || bad.TotalPnL != 0 || math.Abs(bad.OccupiedShareSeconds-3600) > 1e-12 {
		t.Fatalf("future close became realized evidence instead of an as-of censor: %+v", bad)
	}
}

func TestR135cUnitTrialEconomicProofRequiresDepthAndFeeReceipt(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	trials := []UnitTrial{
		{OpenedTS: now.Add(-40 * 24 * time.Hour), Family: "valid", Platform: "kalshi", Ticker: "VALID", Side: "YES", Ask: .10, FeePC: .01, FeeKnown: true, FeeSource: "kalshi:test", Depth: 1},
		{OpenedTS: now.Add(-40 * 24 * time.Hour), Family: "zero-depth", Platform: "kalshi", Ticker: "D0", Side: "YES", Ask: .10, FeePC: .01, FeeKnown: true, FeeSource: "kalshi:test", Depth: 0},
		{OpenedTS: now.Add(-40 * 24 * time.Hour), Family: "unknown-fee", Platform: "polyus", Ticker: "NOFEE", Side: "YES", Ask: .10, FeePC: .01, FeeKnown: false, Depth: 1},
	}
	for _, trial := range trials {
		if inserted, ierr := st.InsertUnitTrial(ctx, trial); ierr != nil || !inserted {
			t.Fatalf("insert %s=%v err=%v", trial.Family, inserted, ierr)
		}
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE unit_trials SET settled=1,closed_ts=?,pnl_pc=.89,return_per_dollar=8 WHERE 1=1`, now.Add(-39*24*time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	stats, err := st.UnitTrialStats(ctx)
	if err != nil || len(stats) != 1 || stats[0].Family != "valid" {
		t.Fatalf("depth/fee coverage rows leaked into economic stats: %+v err=%v", stats, err)
	}
	leaders, err := st.UnitTrialLeaderboard(ctx, now)
	if err != nil || len(leaders) != 1 || leaders[0].Family != "valid" || leaders[0].DepthKnownShare != 1 {
		t.Fatalf("depth/fee coverage rows leaked into leaderboard: %+v err=%v", leaders, err)
	}
}
