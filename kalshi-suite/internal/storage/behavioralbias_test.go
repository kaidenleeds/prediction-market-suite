package storage

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func behavioralExplainPlan(t *testing.T, st *Store, query string) string {
	t.Helper()
	rows, err := st.db.Query("EXPLAIN QUERY PLAN " + query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, " | ")
}

func TestBehavioralBiasReaderUsesCoveringProspectiveIndexes(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	countPlan := behavioralExplainPlan(t, st, `SELECT
  (SELECT COUNT(*) FROM signal_log),
	(SELECT COUNT(*) FROM signal_log INDEXED BY idx_signal_book_v1 WHERE book_feature_ver=1),
	(SELECT COUNT(*) FROM signal_log INDEXED BY idx_signal_book_native_v2
	 WHERE book_feature_ver=1 AND pricing_version='book-native-v2' AND label_version='kalshi_start_clock_v2')`)
	if strings.Count(countPlan, "USING COVERING INDEX") < 2 || !strings.Contains(countPlan, "USING INDEX idx_signal_book_native_v2") {
		t.Fatalf("coverage count regressed to table scan: %s", countPlan)
	}
	rowPlan := behavioralExplainPlan(t, st, `SELECT id FROM signal_log INDEXED BY idx_signal_book_native_v2
WHERE book_feature_ver=1 AND pricing_version='book-native-v2' AND label_version='kalshi_start_clock_v2'
ORDER BY resolved,id`)
	if !strings.Contains(rowPlan, "idx_signal_book_native_v2") || strings.Contains(strings.ToUpper(rowPlan), "TEMP B-TREE") {
		t.Fatalf("prospective row scan lost partial-index order: %s", rowPlan)
	}
}

func TestBehavioralBiasReaderAdmitsOnlySettledExecutableBookV1(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	closed := opened.Add(2 * time.Hour)

	insert := func(ticker string, ver int, ask, depth, fee any, resolved int, won any) {
		t.Helper()
		labelVersion, pricingVersion := "kalshi_start_clock_v2", "book-native-v2"
		if ticker == "LEGACY" || ticker == "LEGACY-BOOK" {
			labelVersion, pricingVersion = "", ""
		}
		_, err := st.db.ExecContext(ctx, `INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,book_feature_ver,
label_version,pricing_version,book_bid,book_ask,book_ask_depth,book_taker_fee_pc,book_source,resolved,won,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, opened.Format(time.RFC3339), "2026-07-08", ticker,
			"kalshi", ticker, "bias", "YES", "favlong", .99, ver, labelVersion, pricingVersion, .39, ask, depth, fee,
			"kalshi_book_ws", resolved, won, closed.Format(time.RFC3339))
		if err != nil {
			t.Fatal(err)
		}
	}
	insert("LEGACY", 0, .40, 5.0, .01, 1, 1)
	insert("LEGACY-BOOK", 1, .40, 5.0, .01, 1, 1)
	insert("GOOD", 1, .40, 5.0, .01, 1, 1)
	insert("FRACTIONAL", 1, .40, .999, .01, 1, 1)
	insert("NOFEE", 1, .40, 5.0, nil, 1, 1)
	insert("OPEN", 1, .40, 5.0, .01, 0, nil)

	rows, cov, err := st.BehavioralBiasProspectiveObservations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Ticker != "GOOD" {
		t.Fatalf("unsafe signal rows admitted: %+v", rows)
	}
	if cov.TotalRows != 6 || cov.LegacyOrUnversioned != 1 || cov.BookV1Rows != 5 ||
		cov.BookNativeV2Rows != 4 || cov.LegacyBookV1Rows != 1 || cov.Eligible != 1 ||
		cov.InsufficientDepth != 1 || cov.MissingExactFee != 1 || cov.UnsettledOrBadPayout != 1 {
		t.Fatalf("coverage buckets wrong: %+v", cov)
	}
	if math.Abs(rows[0].Ask-.40) > 1e-12 || math.Abs(rows[0].TakerFeePC-.01) > 1e-12 ||
		math.Abs(rows[0].Payout-1) > 1e-12 || !rows[0].BidKnown {
		t.Fatalf("book economics changed: %+v", rows[0])
	}
}

func TestBehavioralBiasReaderUsesBoughtSideSettlementAndRejectsBadClocks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	opened := time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)
	closed := opened.Add(time.Hour)
	insert := func(ticker, side, start, end string, settle float64) {
		t.Helper()
		_, err := st.db.ExecContext(ctx, `INSERT INTO signal_log(
ts,day,slot,platform,ticker,title,side,signal_type,entry_price,book_feature_ver,
label_version,pricing_version,book_bid,book_ask,book_ask_depth,book_taker_fee_pc,book_source,resolved,settle_val,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, start, "2026-07-08", ticker, "kalshi", ticker,
			"bias", side, "bookskew", .50, 1, "kalshi_start_clock_v2", "book-native-v2",
			.48, .50, 2.0, 0.0, "kalshi_book_ws", 1,
			settle, end)
		if err != nil {
			t.Fatal(err)
		}
	}
	insert("NO-WINS", "NO", opened.Format(time.RFC3339), closed.Format(time.RFC3339), 0)
	insert("BACKWARD", "YES", closed.Format(time.RFC3339), opened.Format(time.RFC3339), 1)
	rows, cov, err := st.BehavioralBiasProspectiveObservations(ctx)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v coverage=%+v err=%v", rows, cov, err)
	}
	if rows[0].Payout != 1 || cov.InvalidTimestamps != 1 {
		t.Fatalf("NO-side payout or timestamp validation wrong: rows=%+v coverage=%+v", rows, cov)
	}
}
