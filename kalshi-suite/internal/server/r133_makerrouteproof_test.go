package server

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR133MakerRouteEconomicsIncludeCanceledAttemptsAndMakerFee(t *testing.T) {
	rows := []storage.MakerRouteAttempt{
		{Platform: "kalshi", Source: "auto-cons-edge", Family: "edge", Ticker: "KXEDGE-A", Side: "YES", PostPx: .40},
		{Platform: "kalshi", Source: "auto-cons-edge", Family: "edge", Ticker: "KXEDGE-B", Side: "YES", PostPx: .40, Filled: true, Settle: 1},
		{Platform: "kalshi", Source: "auto-cons-edge", Family: "edge", Ticker: "KXEDGE-C", Side: "YES", PostPx: .40, Filled: true, Settle: 0},
	}
	st := makerRouteStatFrom(rows, func(storage.MakerRouteAttempt) (float64, bool) { return .01, true })
	// observations: canceled=$0, YES win=+$0.59, YES loss=-$0.41 => +$0.06/attempt
	if st.N != 3 || st.Markets != 3 || st.SettledRows != 3 || st.FillN != 2 || st.Invalid != 0 || math.Abs(st.Mean-.06) > 1e-12 ||
		math.Abs(st.MeanPost-.40) > 1e-12 || math.Abs(st.MeanFeePC-.01) > 1e-12 {
		t.Fatalf("maker route stat=%+v", st)
	}
	bad := append([]storage.MakerRouteAttempt(nil), rows...)
	bad[1].Ticker = "UNKNOWN-FEE"
	badStat := makerRouteStatFrom(bad, func(a storage.MakerRouteAttempt) (float64, bool) {
		return .01, a.Ticker != "UNKNOWN-FEE"
	})
	if badStat.Invalid != 1 {
		t.Fatalf("unknown maker fee must make route proof invalid, got %+v", badStat)
	}
}

func TestR139MakerRouteStatsRejectMixedSides(t *testing.T) {
	now := time.Now().UTC().Add(-48 * time.Hour)
	rows := []storage.MakerRouteAttempt{
		{Platform: "kalshi", Source: "auto-cons-edge", Family: "edge", Ticker: "KX-YES", Side: "YES",
			PostPx: .4, OpenedTS: now, Terminal: true},
		{Platform: "kalshi", Source: "auto-cons-edge", Family: "edge", Ticker: "KX-NO", Side: "NO",
			PostPx: .4, OpenedTS: now, Terminal: true},
	}
	st := makerRouteStatFrom(rows, func(storage.MakerRouteAttempt) (float64, bool) { return 0, true })
	if st.Side != "YES" || st.N != 1 || st.Invalid != 1 {
		t.Fatalf("mixed-side maker cohort was pooled: %+v", st)
	}
}

func TestR133MakerRouteProofCannotBorrowTakerUnitLane(t *testing.T) {
	s := testServer(t)
	s.kalFees = map[string]kalFeeInfo{
		"KXROUTE": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXROUTE-1", Side: "YES", Source: "auto-cons-kflow", Price: .40}
	ok, why, _, _, _ := s.liveMirrorProof(context.Background(), c, .40, true)
	if ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("maker route must fail closed instead of reading unit asks: ok=%v why=%q", ok, why)
	}
}

func TestR133MakerRouteProofUsesPerAttemptEvidence(t *testing.T) {
	s := testServer(t)
	s.kalFees = map[string]kalFeeInfo{
		"KXROUTE": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1},
	}
	s.kalFeesAt = time.Now()
	ctx := context.Background()
	c := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXROUTE-NOW", Side: "YES", Source: "auto-cons-kflow", Price: .10}
	// 40 settled wins plus 20 cancellations. Per-fill-only grading would report ~89c; the route
	// proof correctly reports ~59c per attempted quote and includes the 20 zero-PnL outcomes.
	for i := 0; i < 60; i++ {
		id, err := s.store.InsertMakerAttempt(ctx, "kalshi", "KXROUTE-HIST", "YES", c.Source, .10, 2, 10, 0, nil, 1, "", "bookws")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.store.SetMakerStrategy(ctx, id, "kflow", false); err != nil {
			t.Fatal(err)
		}
		opened := time.Now().UTC().AddDate(0, 0, -(40 - i%30)).Format(time.RFC3339Nano)
		if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE maker_fill_stats SET ts=? WHERE id=?`, opened, id); err != nil {
			t.Fatal(err)
		}
		if i < 40 {
			if err := s.store.MarkMakerFilledWithFee(ctx, id, .01, "kalshi:test-schedule"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=1 WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
		} else if err := s.store.MarkMakerExpired(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	ok, why, mean, lo, fee := s.liveMirrorProof(ctx, c, .10, true)
	if ok || why != "sealed-accepted-paper-intent-required" || mean != 0 || lo != 0 || fee < 0 {
		t.Fatalf("rolling maker evidence bypassed sealed Paper proof: ok=%v why=%q mean=%v lo=%v fee=%v", ok, why, mean, lo, fee)
	}
	st, err := s.makerRouteStat(ctx, "kalshi", c.Source, "kflow", false)
	if err != nil || st.N != 1 || st.Markets != 1 || st.SettledRows != 60 || st.FillN != 40 || st.Mean >= .70 {
		t.Fatalf("maker market/receipt denominators were lost: stat=%+v err=%v", st, err)
	}
	rows := s.makerRouteVerdicts(ctx)
	if len(rows) != 1 || rows[0].Family != "maker:kflow@kalshi" || rows[0].Group != "maker" ||
		rows[0].SourceFamily != "kflow" || rows[0].Platform != "kalshi" ||
		rows[0].OriginLayer != "model" || rows[0].Route != "maker" || rows[0].Side != "YES" ||
		rows[0].N != 1 || rows[0].Markets != 1 || rows[0].SettledRows != 60 {
		t.Fatalf("explicit maker scoreboard row=%+v", rows)
	}
	if cell, exact := briefExactExecutionCell(rows[0]); !exact || cell != "K/Y/M" {
		t.Fatalf("maker route was excluded from compact execution cells: cell=%q exact=%v row=%+v", cell, exact, rows[0])
	}
	ranked := briefRankedSystems(rows, true, 5)
	if len(ranked) != 0 {
		t.Fatalf("one-contract maker route entered the sample-aware compact leaderboard: %+v", ranked)
	}
	// A slow filled position stays in the denominator as censored/open. Quick settled winners may
	// not authorize another live order while that possible loss is unresolved.
	openID, err := s.store.InsertMakerAttempt(ctx, "kalshi", "KXROUTE-OPEN", "YES", c.Source, .10, 2, 10, 0, nil, 1, "", "bookws")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SetMakerStrategy(ctx, openID, "kflow", false); err != nil {
		t.Fatal(err)
	}
	if err := s.store.MarkMakerFilledWithFee(ctx, openID, .01, "kalshi:test-schedule"); err != nil {
		t.Fatal(err)
	}
	if ok, why, _, _, _ := s.liveMirrorProof(ctx, c, .10, true); ok || why != "sealed-accepted-paper-intent-required" {
		t.Fatalf("unsettled maker fill must censor LIVE proof: ok=%v why=%q", ok, why)
	}
}

func TestR144MakerRouteVerdictsExposePolyUSNOAndActualStrategyOrigin(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	source := "r139p:" + strings.Repeat("a", 64)
	id, err := s.store.InsertMakerAttempt(ctx, "polyus", "pus-maker-no", "NO", source,
		.35, 1, 5, 0, nil, 1, "", "book3")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.store.SetMakerStrategy(ctx, id, "proper-score-executor", false); err != nil {
		t.Fatal(err)
	}
	if err := s.store.MarkMakerFilledWithFee(ctx, id, .005, "polyus:test-schedule"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.store.DBForTest().ExecContext(ctx,
		`UPDATE maker_fill_stats SET settle_val=0 WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}

	rows := s.makerRouteVerdicts(ctx)
	if len(rows) != 1 {
		t.Fatalf("maker verdict rows=%+v", rows)
	}
	row := rows[0]
	if row.Family != "maker:proper-score-executor@polyus" || row.SourceFamily != "proper-score-executor" ||
		row.Platform != "polyus" || row.OriginLayer != "strategy" || row.Route != "maker" || row.Side != "NO" {
		t.Fatalf("PolyUS strategy maker identity=%+v", row)
	}
	if cell, exact := briefExactExecutionCell(row); !exact || cell != "PUS/N/M" {
		t.Fatalf("PolyUS NO maker route was excluded: cell=%q exact=%v row=%+v", cell, exact, row)
	}
}

func TestR144MakerRouteOriginComesFromActualDispatchSource(t *testing.T) {
	if got := makerRouteOriginLayer("auto-cons-kflow"); got != "model" {
		t.Fatalf("legacy detector maker origin=%q", got)
	}
	if got := makerRouteOriginLayer("r139p:" + strings.Repeat("b", 64)); got != "strategy" {
		t.Fatalf("sealed system maker origin=%q", got)
	}
	if got := makerRouteOriginLayer("r142x:maker:42:time-nested-lock"); got != "strategy" {
		t.Fatalf("system exploration maker origin=%q", got)
	}
}

func TestR133MakerRouteCurrentPostPaysFullWorseCostPenalty(t *testing.T) {
	lo, hi, netLo, netHi := .05, .11, .01, .20
	st := makerRouteStat{Platform: "kalshi", Family: "edge", N: 100, FillN: 60,
		Mean: .08, SD: .08, MeanPost: .40, MeanFeePC: .01,
		MatureUTCBlocks: 30, ObservedUTCBlocks: 20, EventDayClusters: 20,
		DayMean: .08, DayLo: &lo, DayHi: &hi, NetDayLo: &netLo, NetDayHi: &netHi}
	state, baseMean, baseLo := makerRouteAdjusted(st, .40, .01)
	if state != "PROVEN+" || baseLo <= 0 {
		t.Fatalf("base route state=%s mean=%v lo=%v", state, baseMean, baseLo)
	}
	_, expensiveMean, expensiveLo := makerRouteAdjusted(st, .49, .01)
	if math.Abs((baseMean-expensiveMean)-.09) > 1e-12 || math.Abs((baseLo-expensiveLo)-.09) > 1e-12 {
		t.Fatalf("worse post not fully charged: base=%v/%v expensive=%v/%v", baseMean, baseLo, expensiveMean, expensiveLo)
	}
}
