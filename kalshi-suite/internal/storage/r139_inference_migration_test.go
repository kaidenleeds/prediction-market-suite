package storage

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestR139LegacyMonitoringInferenceTablesRebuildWithoutLosingRows(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+`\legacy-inference.db?_pragma=foreign_keys(ON)`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	oldRun := `CREATE TABLE research_inference_runs(
id INTEGER PRIMARY KEY AUTOINCREMENT,created_ts TEXT NOT NULL,as_of_completed_ts TEXT NOT NULL,
pipeline_version INTEGER NOT NULL,input_manifest_hash TEXT NOT NULL,input_manifest_json TEXT NOT NULL,
result_hash TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('complete','blocked_input_limit')),
observed_rows INTEGER NOT NULL,exact_terminal_rows INTEGER NOT NULL,excluded_open INTEGER NOT NULL,
excluded_void INTEGER NOT NULL,excluded_censored INTEGER NOT NULL,excluded_nonexact INTEGER NOT NULL,
excluded_identity INTEGER NOT NULL,excluded_route_truth INTEGER NOT NULL,systems_reported INTEGER NOT NULL,
funded INTEGER NOT NULL DEFAULT 0,paper_authority INTEGER NOT NULL DEFAULT 0,live_authority INTEGER NOT NULL DEFAULT 0,
UNIQUE(pipeline_version,input_manifest_hash))`
	oldResult := `CREATE TABLE research_inference_system_results(
run_id INTEGER NOT NULL,system_id TEXT NOT NULL,experiment_version INTEGER NOT NULL,state TEXT NOT NULL,
reason TEXT NOT NULL,terminal_rows INTEGER NOT NULL,train_events INTEGER NOT NULL,validation_events INTEGER NOT NULL,
monitoring_events INTEGER NOT NULL,purged_events INTEGER NOT NULL,validation_days INTEGER NOT NULL,
validation_mean REAL NOT NULL,validation_lower REAL NOT NULL,validation_upper REAL NOT NULL,
validation_bounds_known INTEGER NOT NULL,monitoring_days INTEGER NOT NULL,monitoring_mean REAL NOT NULL,
monitoring_lower REAL NOT NULL,monitoring_upper REAL NOT NULL,monitoring_bounds_known INTEGER NOT NULL,
raw_p REAL NOT NULL,holm_p REAL NOT NULL,by_q REAL NOT NULL,monitoring_lower_bound_positive INTEGER NOT NULL,
preregistered_untouched_gate_pass INTEGER NOT NULL DEFAULT 0 CHECK(preregistered_untouched_gate_pass=0),
cells_json TEXT NOT NULL,execution_candidate INTEGER NOT NULL DEFAULT 0 CHECK(execution_candidate=0),
funded INTEGER NOT NULL DEFAULT 0,paper_authority INTEGER NOT NULL DEFAULT 0,live_authority INTEGER NOT NULL DEFAULT 0,
PRIMARY KEY(run_id,system_id),FOREIGN KEY(run_id) REFERENCES research_inference_runs(id))`
	for _, q := range []string{oldRun, oldResult,
		`INSERT INTO research_inference_runs(created_ts,as_of_completed_ts,pipeline_version,input_manifest_hash,
input_manifest_json,result_hash,status,observed_rows,exact_terminal_rows,excluded_open,excluded_void,
excluded_censored,excluded_nonexact,excluded_identity,excluded_route_truth,systems_reported)
VALUES('2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',1,'old','{}','sha256:old','complete',0,0,0,0,0,0,0,0,19)`,
		`INSERT INTO research_inference_system_results(run_id,system_id,experiment_version,state,reason,
terminal_rows,train_events,validation_events,monitoring_events,purged_events,validation_days,
validation_mean,validation_lower,validation_upper,validation_bounds_known,monitoring_days,monitoring_mean,
monitoring_lower,monitoring_upper,monitoring_bounds_known,raw_p,holm_p,by_q,
monitoring_lower_bound_positive,cells_json) VALUES(1,'proper-score-executor',1,'COLLECTING','legacy',0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,0,'[]')`} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if err := preMigrateR139InferenceTables(db); err != nil {
		t.Fatal(err)
	}
	for table, column := range map[string]string{"research_inference_runs": "preregistration_id",
		"research_inference_system_results": "untouched_events"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name=?`, table, column).Scan(&n); err != nil || n != 1 {
			// SQLite's pragma table-valued function does not bind its table name on every build.
			if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('`+table+`') WHERE name=?`, column).Scan(&n); err != nil || n != 1 {
				t.Fatalf("%s.%s=%d err=%v", table, column, n, err)
			}
		}
	}
	var runs, results int
	if err := db.QueryRow(`SELECT COUNT(*) FROM research_inference_runs`).Scan(&runs); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM research_inference_system_results`).Scan(&results); err != nil {
		t.Fatal(err)
	}
	if runs != 1 || results != 1 {
		t.Fatalf("legacy monitoring history lost: runs=%d results=%d", runs, results)
	}
}
