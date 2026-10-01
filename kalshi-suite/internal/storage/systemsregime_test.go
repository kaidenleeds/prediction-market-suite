package storage

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func TestR135cNativeUnitReaderPreservesStrategySourceAndIgnoresLegacyRate(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	_, err = st.db.ExecContext(ctx, `INSERT INTO unit_trials(
opened_ts,closed_ts,family,platform,origin_layer,ticker,side,episode,ask,fee_pc,depth,
fee_known,fee_source,quote_source,settled,pnl_pc,capital_day)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, opened.Format(time.RFC3339), opened.Add(time.Hour).Format(time.RFC3339),
		"xvgap", "polyus", "strategy", "PUS-X", "YES", 1, .4, .01, 8, 1, "polyus:test",
		"strategy-decision/polyus-book3", 1, .09, 999999.0)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := st.NativeUnitSystemObservations(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if rows[0].Source != "strategy-decision/polyus-book3" || rows[0].OriginLayer != "strategy" || !rows[0].FeeKnown || !rows[0].PnLKnown || math.Abs(rows[0].PnLPC-.09) > 1e-12 {
		t.Fatalf("source double-prefixed or legacy capital_day leaked: %+v", rows[0])
	}
	if rows[0].Filled || !rows[0].DepthKnown {
		t.Fatalf("visible quote depth was mislabeled as an exchange fill: %+v", rows[0])
	}
}

func TestR135cSubcentSystemIdentityNeverUsesTerminalHindsight(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	for i, terminal := range []string{"active", "eliminated"} {
		if _, err := st.db.ExecContext(ctx, `INSERT INTO subcent_golf_trials(
observed_ts,slot,platform,ticker,side,phase,player_status,terminal_status,market_type,route,tick_size,
bid_px,ask_px,ask_depth,fill_state,cancel_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, time.Now().UTC().Add(-time.Minute).Format(time.RFC3339),
			string(rune('A'+i)), "kalshi", string(rune('X'+i)), "YES", "pre_event", "active", terminal,
			"winner", "maker", .001, .001, .005, 50, "canceled", time.Now().UTC().Format(time.RFC3339)); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := st.NativeSubcentGolfObservations(ctx)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if rows[0].System != rows[1].System || strings.Contains(rows[0].System, "eliminated") || strings.Contains(rows[1].System, "eliminated") {
		t.Fatalf("terminal hindsight split prospective system identity: %+v", rows)
	}
}

func TestR135cNativeMakerReaderJoinsAuthoritativeSettlementClock(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	closed := opened.Add(3 * time.Hour)
	if _, err := st.db.ExecContext(ctx, `INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,settle_val,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, opened.Format(time.RFC3339), "2026-07-10", "slot", "kalshi", "KX-M", "test", "YES", "bookskew", .4, 1, 1, closed.Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO maker_fill_stats(
ts,platform,ticker,side,source,strategy_family,post_px,book_depth,filled,fill_ts,settle_val)
VALUES(?,?,?,?,?,?,?,?,?,?,?)`, opened.Format(time.RFC3339), "kalshi", "KX-M", "YES",
		"auto-cons-kalshi", "bookskew", .39, 7, 1, opened.Add(time.Minute).Format(time.RFC3339), 1); err != nil {
		t.Fatal(err)
	}
	rows, err := st.NativeMakerObservations(ctx)
	if err != nil || len(rows) != 1 || rows[0].ClosedTS != closed.Format(time.RFC3339) {
		t.Fatalf("maker settlement clock unavailable/wrong: rows=%+v err=%v", rows, err)
	}
}

func TestR135cSubcentCanceledMakerUsesReservedBidNotAsk(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if _, err := st.db.ExecContext(ctx, `INSERT INTO subcent_golf_trials(
observed_ts,slot,platform,ticker,side,phase,player_status,market_type,route,tick_size,
bid_px,ask_px,ask_depth,fill_state,cancel_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, time.Now().UTC().Add(-time.Minute).Format(time.RFC3339),
		"2026-07-11T12", "kalshi", "KXGOLF-X", "YES", "pre_event", "active", "winner",
		"maker", .001, .001, .005, 50, "canceled", time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	rows, err := st.NativeSubcentGolfObservations(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if math.Abs(rows[0].CostPC-.001) > 1e-12 || !rows[0].PnLKnown || rows[0].PnLPC != 0 {
		t.Fatalf("canceled maker used taker ask or nonzero PnL: %+v", rows[0])
	}
}

func TestR135cWeatherRegimeAdmitsOnlyCompleteBookV1Rows(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	ts := time.Date(2026, 7, 11, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
	insert := func(ticker string, ver int, ask, fee, depth any) {
		t.Helper()
		_, err := st.db.ExecContext(ctx, `INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,book_feature_ver,
book_ask,book_ask_depth,book_taker_fee_pc,book_source,resolved,won,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, ts, "2026-07-11", ticker, "kalshi", ticker,
			"weather", "YES", "weather-curve-residual", .99, ver, ask, depth, fee, "kalshi_book_ws", 1, 1, ts)
		if err != nil {
			t.Fatal(err)
		}
	}
	insert("LEGACY", 0, .4, .01, 5)
	insert("BOOKV1", 1, .4, .01, 5)
	insert("NODEPTH", 1, .4, .01, nil)
	rows, err := st.NativeWeatherResearchObservations(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if rows[0].System != "weather-curve-residual" || math.Abs(rows[0].CostPC-.41) > 1e-12 || math.Abs(rows[0].PnLPC-.59) > 1e-12 {
		t.Fatalf("weather book-v1 economics wrong: %+v", rows[0])
	}
}

func TestR135cMakerFillPersistsSignedFeeReceipt(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	id, err := st.InsertMakerAttempt(ctx, "polyus", "PUS-FEE", "YES", "auto-cons-edge", .40, 2, 8, 0, nil, 1, "", "book3")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetMakerStrategy(ctx, id, "edge", false); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMakerFilledWithFee(ctx, id, -.003, "polyus:market.feeCoefficient"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=1 WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	rows, err := st.NativeMakerObservations(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if !rows[0].FeeKnown || math.Abs(rows[0].FeePC-(-.003)) > 1e-12 || rows[0].FeeSource != "polyus:market.feeCoefficient" {
		t.Fatalf("signed maker rebate receipt lost: %+v", rows[0])
	}
}
