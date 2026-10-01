package storage

import (
	"context"
	"testing"
	"time"
)

func TestR133MakerRouteAttemptsRequireExactTaggedTerminalRoute(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	insert := func(source, ticker string) int64 {
		t.Helper()
		id, err := st.InsertMakerAttempt(ctx, "kalshi", ticker, "YES", source, .40, 2, 10, 0, nil, 1, "", "bookws")
		if err != nil {
			t.Fatal(err)
		}
		return id
	}

	legacy := insert("auto-cons-edge", "LEGACY")
	if err := st.MarkMakerExpired(ctx, legacy); err != nil {
		t.Fatal(err)
	}
	canceled := insert("auto-cons-edge", "CANCEL")
	if err := st.SetMakerStrategy(ctx, canceled, "edge", false); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMakerExpired(ctx, canceled); err != nil {
		t.Fatal(err)
	}
	settledFill := insert("auto-cons-edge", "WIN")
	if err := st.SetMakerStrategy(ctx, settledFill, "edge", false); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMakerFilledWithFee(ctx, settledFill, .01, "kalshi:test-schedule"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE maker_fill_stats SET settle_val=1 WHERE id=?`, settledFill); err != nil {
		t.Fatal(err)
	}
	unsettledFill := insert("auto-cons-edge", "OPEN")
	if err := st.SetMakerStrategy(ctx, unsettledFill, "edge", false); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMakerFilledWithFee(ctx, unsettledFill, .01, "kalshi:test-schedule"); err != nil {
		t.Fatal(err)
	}
	inverted := insert("auto-cons-edge", "INVERT")
	if err := st.SetMakerStrategy(ctx, inverted, "invert:edge", true); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMakerExpired(ctx, inverted); err != nil {
		t.Fatal(err)
	}
	otherSource := insert("auto-cons-other", "OTHER")
	if err := st.SetMakerStrategy(ctx, otherSource, "edge", false); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkMakerExpired(ctx, otherSource); err != nil {
		t.Fatal(err)
	}

	rows, err := st.MakerRouteAttempts(ctx, "kalshi", "auto-cons-edge", "edge", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].ID != canceled || rows[0].Filled || rows[0].Open ||
		rows[1].ID != settledFill || !rows[1].Filled || rows[1].Open || rows[1].Settle != 1 || !rows[1].FeeKnown || rows[1].FeePC != .01 ||
		rows[2].ID != unsettledFill || !rows[2].Filled || !rows[2].Open || rows[2].Terminal {
		t.Fatalf("exact route rows=%+v", rows)
	}
	keys, err := st.ListMakerRouteKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 3 { // direct edge for two raw sources + the separately-tagged inverted edge
		t.Fatalf("route keys=%+v", keys)
	}
}

func TestR139MakerRouteProofNeverCrossesOutcomeSide(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, side := range []string{"YES", "NO"} {
		if _, err := st.db.ExecContext(ctx, `INSERT INTO maker_fill_stats(
ts,platform,ticker,side,source,post_px,filled,strategy_family,strategy_inverted,
settle_val,maker_fee_pc,maker_rebate_pc,maker_fee_source)
VALUES(?,?,?,?,?,?,0,?,0,0,0,0,'test:exact')`, now, "kalshi", "KXSIDE-"+side,
			side, "auto-cons-edge", .4, "edge"); err != nil {
			t.Fatal(err)
		}
	}
	yes, err := st.MakerRouteAttempts(ctx, "kalshi", "auto-cons-edge", "edge", false, "YES")
	if err != nil || len(yes) != 1 || yes[0].Side != "YES" {
		t.Fatalf("YES route leaked another side: rows=%+v err=%v", yes, err)
	}
	no, err := st.MakerRouteAttempts(ctx, "kalshi", "auto-cons-edge", "edge", false, "NO")
	if err != nil || len(no) != 1 || no[0].Side != "NO" {
		t.Fatalf("NO route leaked another side: rows=%+v err=%v", no, err)
	}
	keys, err := st.ListMakerRouteKeys(ctx)
	if err != nil || len(keys) != 2 || keys[0].Side == keys[1].Side {
		t.Fatalf("maker route registry did not split sides: keys=%+v err=%v", keys, err)
	}
}

func TestR133MakerStrategyStampCannotRewriteProvenance(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	id, err := st.InsertMakerAttempt(ctx, "polyus", "slug", "NO", "freshinv", .35, 1, 5, 0, nil, 1, "", "book3")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetMakerStrategy(ctx, id, "freshlist", false); err != nil {
		t.Fatal(err)
	}
	if err := st.SetMakerStrategy(ctx, id, "invert:freshlist", true); err != nil {
		t.Fatal(err)
	}
	var family string
	var inverted int
	if err := st.db.QueryRowContext(ctx, `SELECT strategy_family,strategy_inverted FROM maker_fill_stats WHERE id=?`, id).Scan(&family, &inverted); err != nil {
		t.Fatal(err)
	}
	if family != "freshlist" || inverted != 0 {
		t.Fatalf("provenance was rewritten: family=%q inverted=%d", family, inverted)
	}
}
