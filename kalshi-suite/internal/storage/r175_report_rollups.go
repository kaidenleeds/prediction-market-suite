package storage

import (
	"context"
	"database/sql"
	"fmt"
)

const (
	r175SystemRawTotalsLatch = "r175_research_system_raw_totals_v1"
	r175RouteReportLatch     = "r175_research_route_report_totals_v1"
)

// preMigrateR175ResearchReportRollups widens only an existing R144 counter table before schema.sql
// installs the R175 trigger that names these columns. Fresh databases do not have the table yet
// and receive the complete definition directly from schema.sql.
func preMigrateR175ResearchReportRollups(db *sql.DB) error {
	var exists int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name='research_system_collection_totals'`).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return nil
	}
	rows, err := db.Query(`PRAGMA table_info(research_system_collection_totals)`)
	if err != nil {
		return err
	}
	columns := map[string]bool{}
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			_ = rows.Close()
			return err
		}
		columns[name] = true
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, column := range []struct {
		name string
		ddl  string
	}{
		{"raw_observations", "INTEGER NOT NULL DEFAULT 0 CHECK(raw_observations>=0)"},
		{"raw_candidates", "INTEGER NOT NULL DEFAULT 0 CHECK(raw_candidates>=0)"},
		{"raw_negative", "INTEGER NOT NULL DEFAULT 0 CHECK(raw_negative>=0)"},
		{"raw_open_envelopes", "INTEGER NOT NULL DEFAULT 0 CHECK(raw_open_envelopes>=0)"},
	} {
		if columns[column.name] {
			continue
		}
		if _, err := db.Exec(`ALTER TABLE research_system_collection_totals ADD COLUMN ` +
			column.name + ` ` + column.ddl); err != nil {
			return fmt.Errorf("add research system total %s: %w", column.name, err)
		}
	}
	return nil
}

// ensureR175ResearchReportRollups performs each historical recount exactly once. Both recounts
// run in their own immediate transaction: a writer commits either before the snapshot and is
// included by SELECT, or after commit and is included by the insert trigger. The latch commits in
// the same transaction, so cancellation cannot leave a partial or double-counted summary.
func ensureR175ResearchReportRollups(ctx context.Context, db *sql.DB) error {
	if err := ensureR175ResearchSystemRawTotals(ctx, db); err != nil {
		return err
	}
	return ensureR175ResearchRouteReportTotals(ctx, db)
}

func ensureR175ResearchSystemRawTotals(ctx context.Context, db *sql.DB) error {
	var done int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kv WHERE k=?`,
		r175SystemRawTotalsLatch).Scan(&done); err != nil {
		return fmt.Errorf("check R175 system raw totals latch: %w", err)
	}
	if done > 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM kv WHERE k=?`,
		r175SystemRawTotalsLatch).Scan(&done); err != nil {
		return fmt.Errorf("recheck R175 system raw totals latch: %w", err)
	}
	if done > 0 {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE research_system_collection_totals SET
controls=0,raw_observations=0,raw_candidates=0,raw_negative=0,raw_open_envelopes=0`); err != nil {
		return fmt.Errorf("reset R175 system raw totals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_system_collection_totals(
system_id,controls,raw_observations,raw_candidates,raw_negative,raw_open_envelopes)
SELECT system_id,
 COALESCE(SUM(observation_kind='control'),0),
 COUNT(*),
 COALESCE(SUM(candidate=1),0),
 COALESCE(SUM(observation_kind='negative'),0),
 COALESCE(SUM(outcome_status='open'),0)
FROM research_system_observations
GROUP BY system_id
ON CONFLICT(system_id) DO UPDATE SET
 controls=excluded.controls,
 raw_observations=excluded.raw_observations,
 raw_candidates=excluded.raw_candidates,
 raw_negative=excluded.raw_negative,
 raw_open_envelopes=excluded.raw_open_envelopes`); err != nil {
		return fmt.Errorf("bootstrap R175 system raw totals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)`,
		r175SystemRawTotalsLatch, "done", nowRFC()); err != nil {
		return fmt.Errorf("latch R175 system raw totals: %w", err)
	}
	return tx.Commit()
}

func ensureR175ResearchRouteReportTotals(ctx context.Context, db *sql.DB) error {
	var done int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kv WHERE k=?`,
		r175RouteReportLatch).Scan(&done); err != nil {
		return fmt.Errorf("check R175 route report latch: %w", err)
	}
	if done > 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM kv WHERE k=?`,
		r175RouteReportLatch).Scan(&done); err != nil {
		return fmt.Errorf("recheck R175 route report latch: %w", err)
	}
	if done > 0 {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM research_route_report_totals`); err != nil {
		return fmt.Errorf("reset R175 route report totals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM research_route_decision_totals`); err != nil {
		return fmt.Errorf("reset R175 route decision totals: %w", err)
	}
	var routes, candidates, blocked, abstains, controls int64
	var feeRows, nonpositive, missingIdentity, completeMoney int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(decision='candidate'),0),COALESCE(SUM(decision='blocked'),0),
COALESCE(SUM(decision='abstain'),0),COALESCE(SUM(decision='control'),0),
COALESCE(SUM(fee_authority!=''),0),COALESCE(SUM(expected_net_low<=0),0),
COALESCE(SUM(identity_status NOT IN ('verified','structural')),0),
COALESCE(SUM(quote_age_known=1 AND latency_known=1 AND tick_known=1
 AND depth_known=1 AND fee_known=1),0)
FROM research_route_opportunities`).Scan(&routes, &candidates, &blocked, &abstains,
		&controls, &feeRows, &nonpositive, &missingIdentity, &completeMoney); err != nil {
		return fmt.Errorf("aggregate R175 route opportunities: %w", err)
	}
	var events, fills, cancels, grades int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(event_type IN ('fill','partial_fill')),0),
COALESCE(SUM(event_type='cancel'),0),COALESCE(SUM(event_type='grade'),0)
FROM research_route_events`).Scan(&events, &fills, &cancels, &grades); err != nil {
		return fmt.Errorf("aggregate R175 route events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_route_report_totals(
singleton,routes,candidates,blocked,abstains,controls,fee_authority_rows,
nonpositive_lower_bound,identity_unverified_or_rejected,complete_money_truth,
lifecycle_events,fill_events,cancel_events,grade_events)
VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?)`, routes, candidates, blocked, abstains, controls,
		feeRows, nonpositive, missingIdentity, completeMoney, events, fills, cancels, grades); err != nil {
		return fmt.Errorf("bootstrap R175 route report totals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_route_decision_totals(route,decision,route_count)
SELECT route,decision,COUNT(*) FROM research_route_opportunities
GROUP BY route,decision ORDER BY route,decision`); err != nil {
		return fmt.Errorf("bootstrap R175 route decision totals: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)`,
		r175RouteReportLatch, "done", nowRFC()); err != nil {
		return fmt.Errorf("latch R175 route report totals: %w", err)
	}
	return tx.Commit()
}
