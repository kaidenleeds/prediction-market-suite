package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

func embeddedCreateTable(table string) (string, error) {
	needle := "CREATE TABLE IF NOT EXISTS " + table + " ("
	start := strings.Index(schemaSQL, needle)
	if start < 0 {
		return "", fmt.Errorf("embedded table %s missing", table)
	}
	rest := schemaSQL[start:]
	end := strings.Index(rest, ";")
	if end < 0 {
		return "", fmt.Errorf("embedded table %s statement unterminated", table)
	}
	return rest[:end+1], nil
}

// preMigrateR139InferenceTables runs before schemaSQL. The first monitoring-only R139 draft
// pinned status/gate/candidate with CHECK(...=0); merely adding columns cannot remove those
// constraints, and the new prereg index would reference a missing column before an ALTER could
// run. Rebuild both append-only tables transactionally while preserving every old monitoring row.
func preMigrateR139InferenceTables(db *sql.DB) error {
	var runSQL, resultSQL string
	err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='research_inference_runs'`).Scan(&runSQL)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='research_inference_system_results'`).Scan(&resultSQL); err != nil {
		return err
	}
	compatible := strings.Contains(runSQL, "sealed_preregistered_untouched") &&
		strings.Contains(runSQL, "preregistration_id") && strings.Contains(resultSQL, "untouched_events") &&
		!strings.Contains(resultSQL, "preregistered_untouched_gate_pass=0") &&
		!strings.Contains(resultSQL, "execution_candidate=0")
	if compatible {
		return nil
	}
	runDDL, err := embeddedCreateTable("research_inference_runs")
	if err != nil {
		return err
	}
	resultDDL, err := embeddedCreateTable("research_inference_system_results")
	if err != nil {
		return err
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys=OFF`); err != nil {
		return err
	}
	_, _ = conn.ExecContext(context.Background(), `PRAGMA legacy_alter_table=ON`)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `PRAGMA legacy_alter_table=OFF`)
		_, _ = conn.ExecContext(context.Background(), `PRAGMA foreign_keys=ON`)
	}()
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`ALTER TABLE research_inference_system_results RENAME TO research_inference_system_results_r139_legacy`,
		`ALTER TABLE research_inference_runs RENAME TO research_inference_runs_r139_legacy`,
		runDDL, resultDDL,
		`INSERT INTO research_inference_runs(id,created_ts,as_of_completed_ts,pipeline_version,input_manifest_hash,
input_manifest_json,result_hash,status,observed_rows,exact_terminal_rows,excluded_open,excluded_void,
excluded_censored,excluded_nonexact,excluded_identity,excluded_route_truth,systems_reported,funded,
paper_authority,live_authority)
SELECT id,created_ts,as_of_completed_ts,pipeline_version,input_manifest_hash,input_manifest_json,
result_hash,status,observed_rows,exact_terminal_rows,excluded_open,excluded_void,excluded_censored,
excluded_nonexact,excluded_identity,excluded_route_truth,systems_reported,funded,paper_authority,
live_authority FROM research_inference_runs_r139_legacy`,
		`INSERT INTO research_inference_system_results(run_id,system_id,experiment_version,state,reason,
terminal_rows,train_events,validation_events,monitoring_events,purged_events,validation_days,
validation_mean,validation_lower,validation_upper,validation_bounds_known,monitoring_days,
monitoring_mean,monitoring_lower,monitoring_upper,monitoring_bounds_known,raw_p,holm_p,by_q,
monitoring_lower_bound_positive,preregistered_untouched_gate_pass,cells_json,execution_candidate,
funded,paper_authority,live_authority)
SELECT run_id,system_id,experiment_version,state,reason,terminal_rows,train_events,validation_events,
monitoring_events,purged_events,validation_days,validation_mean,validation_lower,validation_upper,
validation_bounds_known,monitoring_days,monitoring_mean,monitoring_lower,monitoring_upper,
monitoring_bounds_known,raw_p,holm_p,by_q,monitoring_lower_bound_positive,
preregistered_untouched_gate_pass,cells_json,execution_candidate,funded,paper_authority,live_authority
FROM research_inference_system_results_r139_legacy`,
		`DROP TABLE research_inference_system_results_r139_legacy`,
		`DROP TABLE research_inference_runs_r139_legacy`,
	} {
		if _, err := tx.ExecContext(context.Background(), q); err != nil {
			return err
		}
	}
	return tx.Commit()
}
