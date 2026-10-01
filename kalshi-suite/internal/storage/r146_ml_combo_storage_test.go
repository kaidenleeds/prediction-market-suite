package storage

import (
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

const (
	r146MLComboRoute  = "new-ml-combo-paper"
	r146MLComboCohort = "book-native-v2-system-combos"
)

func r146Parlay(route, cohort string, stake, fees float64) paper.Parlay {
	return paper.Parlay{
		Stake: stake, Price: .25, Contracts: stake / .25, Fees: fees,
		RouteSource: route, Cohort: cohort, SystemIDs: []string{"new-ml"},
		JointP: .35, ExpectedNetPerDollar: .1,
		Legs: []paper.Leg{{Platform: "kalshi", Ticker: "K-A", Side: "YES", Entry: .5},
			{Platform: "kalshi", Ticker: "K-B", Side: "NO", Entry: .5}},
	}
}

func TestR146ParlayRouteCohortIndexMigratesLegacyTableBeforeCreation(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(filepath.Join(dir, "kalshi.db")))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE paper_parlays(
id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, stake REAL NOT NULL, price REAL NOT NULL,
contracts REAL NOT NULL, tp REAL, sl REAL, status TEXT NOT NULL, legs TEXT NOT NULL)`)
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("legacy Open failed before route/cohort index creation: %v", err)
	}
	defer st.Close()
	rows, err := st.db.Query(`PRAGMA index_info(idx_paper_parlays_route_cohort_status_id)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var seq, cid int
		var name string
		if err := rows.Scan(&seq, &cid, &name); err != nil {
			t.Fatal(err)
		}
		columns = append(columns, name)
	}
	if got := strings.Join(columns, ","); got != "route_source,cohort,status,id" {
		t.Fatalf("route/cohort index columns=%q", got)
	}
	var plan string
	if err := st.db.QueryRow(`EXPLAIN QUERY PLAN SELECT id FROM paper_parlays
WHERE route_source=? AND cohort=? AND status=?`, "r", "c", "open").Scan(new(int), new(int), new(int), &plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan, "idx_paper_parlays_route_cohort_status_id") {
		t.Fatalf("indexed route/cohort/status query plan=%q", plan)
	}
}

func TestR146ListAndAggregateParlaysByExactRouteCohortEpoch(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	boundary, err := st.InsertParlay(ctx, r146Parlay("legacy", "legacy", 9, .9))
	if err != nil {
		t.Fatal(err)
	}
	settledID, err := st.InsertParlay(ctx, r146Parlay(r146MLComboRoute, r146MLComboCohort, 3, .15))
	if err != nil {
		t.Fatal(err)
	}
	openID, err := st.InsertParlay(ctx, r146Parlay(r146MLComboRoute, r146MLComboCohort, 4, .20))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.InsertParlay(ctx, r146Parlay(r146MLComboRoute, "other-cohort", 7, .7)); err != nil {
		t.Fatal(err)
	}
	changed, err := st.SettleParlayIfOpen(ctx, settledID, 1, 2.75)
	if err != nil || !changed {
		t.Fatalf("settle changed=%v err=%v", changed, err)
	}
	all, err := st.ListParlaysByRouteCohort(ctx, r146MLComboRoute, r146MLComboCohort, "")
	if err != nil || len(all) != 2 {
		t.Fatalf("exact rows=%d err=%v", len(all), err)
	}
	opens, err := st.ListParlaysByRouteCohort(ctx, r146MLComboRoute, r146MLComboCohort, "open")
	if err != nil || len(opens) != 1 || opens[0].ID != openID {
		t.Fatalf("open exact rows=%+v err=%v", opens, err)
	}
	if _, err := st.ListParlaysByRouteCohort(ctx, "", r146MLComboCohort, ""); err == nil {
		t.Fatal("blank route widened the funded portfolio query")
	}
	funds, err := st.RouteCohortParlayFunds(ctx, r146MLComboRoute, r146MLComboCohort, boundary, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(funds.Realized-2.75) > 1e-9 || math.Abs(funds.ClosedContracts-12) > 1e-9 ||
		math.Abs(funds.OpenStake-4) > 1e-9 ||
		math.Abs(funds.OpenFees-.2) > 1e-9 || math.Abs(funds.OpenCommitted-4.2) > 1e-9 ||
		funds.OpenCount != 1 || funds.ClosedCount != 1 {
		t.Fatalf("epoch funds=%+v", funds)
	}
	postSettlement, err := st.RouteCohortParlayFunds(ctx, r146MLComboRoute, r146MLComboCohort, settledID, time.Time{})
	if err != nil || postSettlement.Realized != 0 || postSettlement.ClosedContracts != 0 ||
		postSettlement.OpenCount != 1 || postSettlement.ClosedCount != 0 {
		t.Fatalf("after-id funds=%+v err=%v", postSettlement, err)
	}
}

func TestR146ParlayTerminalTransitionsAreOpenOnlyAndResetIsCancellation(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	receipt := fundedRelationTestReceipt("v", "ml-combos", "kalshi", "K-A", "YES", "event-v", 1, "independent")
	receiptID, _, err := st.InsertFundedRelationReceipt(ctx, receipt)
	if err != nil {
		t.Fatal(err)
	}
	voidID, err := st.InsertParlayWithRelation(ctx, r146Parlay(r146MLComboRoute, r146MLComboCohort, 5, .25), receiptID)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := st.VoidParlayForReset(ctx, voidID)
	if err != nil || !changed {
		t.Fatalf("void changed=%v err=%v", changed, err)
	}
	if changed, err = st.SettleParlayIfOpen(ctx, voidID, 1, 99); err != nil || changed {
		t.Fatalf("settlement overwrote reset void: changed=%v err=%v", changed, err)
	}
	var status, outcomeStatus, source string
	var payout, realized, relationPNL float64
	if err := st.db.QueryRowContext(ctx, `SELECT status,payout,realized FROM paper_parlays WHERE id=?`, voidID).
		Scan(&status, &payout, &realized); err != nil {
		t.Fatal(err)
	}
	if status != "reset_void" || payout != 0 || realized != 0 {
		t.Fatalf("reset row status=%q payout=%v realized=%v", status, payout, realized)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT outcome_status,pnl_dollars,result_source
FROM funded_relation_outcomes WHERE receipt_id=?`, receiptID).Scan(&outcomeStatus, &relationPNL, &source); err != nil {
		t.Fatal(err)
	}
	if outcomeStatus != "cancelled" || relationPNL != 0 || source != "combo-reset-void" {
		t.Fatalf("reset relation status=%q pnl=%v source=%q", outcomeStatus, relationPNL, source)
	}

	settleID, err := st.InsertParlay(ctx, r146Parlay(r146MLComboRoute, r146MLComboCohort, 2, .1))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err = st.SettleParlayIfOpen(ctx, settleID, 1, 3); err != nil || !changed {
		t.Fatalf("first settle changed=%v err=%v", changed, err)
	}
	if changed, err = st.CloseParlayIfOpen(ctx, settleID, .5, -1); err != nil || changed {
		t.Fatalf("close overwrote settlement: changed=%v err=%v", changed, err)
	}
	if err := st.db.QueryRowContext(ctx, `SELECT status,realized FROM paper_parlays WHERE id=?`, settleID).
		Scan(&status, &realized); err != nil || status != "settled" || realized != 3 {
		t.Fatalf("terminal row status=%q realized=%v err=%v", status, realized, err)
	}
	funds, err := st.RouteCohortParlayFunds(ctx, r146MLComboRoute, r146MLComboCohort, 0, time.Time{})
	if err != nil || funds.ClosedCount != 1 || funds.OpenCount != 0 || funds.Realized != 3 ||
		math.Abs(funds.ClosedContracts-8) > 1e-9 {
		t.Fatalf("reset void contaminated funds=%+v err=%v", funds, err)
	}
}
