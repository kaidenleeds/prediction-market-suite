package storage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

const properScoreV2DDL = `CREATE TABLE research_proper_score_trials (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 observed_ts TEXT NOT NULL,
 slot TEXT NOT NULL,
 system_name TEXT NOT NULL,
 transform TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
 strategy_mode TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
 cohort TEXT NOT NULL,
 platform TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 title TEXT NOT NULL DEFAULT '',
 category TEXT NOT NULL DEFAULT '',
 resolve_hours REAL NOT NULL DEFAULT 0,
 forecast_yes REAL NOT NULL,
 forecast_origin_side TEXT NOT NULL CHECK(forecast_origin_side IN ('YES','NO')),
 forecast_signal TEXT NOT NULL DEFAULT '',
 forecast_source TEXT NOT NULL,
 forecast_version TEXT NOT NULL,
 model_backend TEXT NOT NULL DEFAULT '',
 calibration TEXT NOT NULL DEFAULT '',
 generated_at INTEGER NOT NULL DEFAULT 0,
 route TEXT NOT NULL CHECK(route IN ('taker','maker')),
 yes_bid REAL NOT NULL,
 yes_ask REAL NOT NULL,
 yes_bid_depth REAL NOT NULL,
 yes_ask_depth REAL NOT NULL,
 no_bid REAL NOT NULL,
 no_ask REAL NOT NULL,
 no_bid_depth REAL NOT NULL,
 no_ask_depth REAL NOT NULL,
 book_source TEXT NOT NULL,
 source_clock_id TEXT NOT NULL DEFAULT '',
 quote_age_s REAL NOT NULL,
 decision_latency_ms REAL NOT NULL DEFAULT 0,
 q_yes REAL NOT NULL,
 raw_yes REAL NOT NULL DEFAULT 0,
 raw_no REAL NOT NULL DEFAULT 0,
 raw_vector_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(raw_vector_json)),
 vector_dim INTEGER NOT NULL DEFAULT 2 CHECK(vector_dim>=2),
 constant_shift REAL NOT NULL DEFAULT 0,
 rescale REAL NOT NULL DEFAULT 1 CHECK(rescale>0),
 normalized_weight REAL NOT NULL DEFAULT 0,
 canonical_qty REAL NOT NULL DEFAULT 0,
 requested_qty REAL NOT NULL DEFAULT 0,
 executable_qty REAL NOT NULL DEFAULT 0,
 selected_side TEXT NOT NULL DEFAULT '',
 tick_size REAL NOT NULL DEFAULT 0,
 lot_size REAL NOT NULL DEFAULT 0,
 depth_curve_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(depth_curve_json)),
 entry_price REAL NOT NULL DEFAULT 0,
 entry_depth REAL NOT NULL DEFAULT 0,
 integrated_cost REAL NOT NULL DEFAULT 0,
 spot_cost REAL NOT NULL DEFAULT 0,
 liquidity_loss REAL NOT NULL DEFAULT 0,
 exact_fee_total REAL NOT NULL DEFAULT 0,
 fee_source TEXT NOT NULL DEFAULT '',
 expected_net REAL NOT NULL DEFAULT 0,
 abstain_reason TEXT NOT NULL DEFAULT '',
 prior_actual_position REAL NOT NULL DEFAULT 0,
 target_position REAL NOT NULL DEFAULT 0,
 actual_filled_delta REAL NOT NULL DEFAULT 0,
 cancelled_delta REAL NOT NULL DEFAULT 0,
 post_actual_position REAL NOT NULL DEFAULT 0,
 fill_status TEXT NOT NULL DEFAULT 'counterfactual' CHECK(fill_status IN ('counterfactual','filled','partial','cancelled','legacy_unknown')),
 actual_fill INTEGER NOT NULL DEFAULT 0 CHECK(actual_fill IN (0,1)),
 research_observation_id INTEGER NOT NULL DEFAULT 0,
 settled INTEGER NOT NULL DEFAULT 0,
 grade_status TEXT NOT NULL DEFAULT 'open',
 grade_reason TEXT NOT NULL DEFAULT '',
 settle_yes REAL,
 forecast_score REAL,
 book_score REAL,
 score_delta REAL,
 realized_net REAL,
 closed_ts TEXT NOT NULL DEFAULT '',
 UNIQUE(slot,system_name,strategy_mode,platform,ticker,forecast_origin_side,forecast_signal,
        forecast_source,forecast_version,route)
)`

const properScoreSimulationDDL = `
CREATE TABLE IF NOT EXISTS research_proper_score_simulated_positions (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 observed_ts TEXT NOT NULL,
 slot TEXT NOT NULL,
 system_name TEXT NOT NULL,
 transform TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
 strategy_mode TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
 cohort TEXT NOT NULL,
 platform TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 forecast_source TEXT NOT NULL,
 forecast_version TEXT NOT NULL,
 prior_position REAL NOT NULL,
 target_position REAL NOT NULL,
 requested_delta REAL NOT NULL,
 filled_delta REAL NOT NULL,
 cancelled_delta REAL NOT NULL,
 post_position REAL NOT NULL,
 gross_cash_flow REAL NOT NULL,
 fee_total REAL NOT NULL,
 expected_value_delta REAL NOT NULL,
 fill_status TEXT NOT NULL CHECK(fill_status IN ('filled','partial','cancelled','noop','blocked')),
 book_source TEXT NOT NULL,
 source_clock_id TEXT NOT NULL,
 decision_latency_ms REAL NOT NULL,
 legs_json TEXT NOT NULL CHECK(json_valid(legs_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(slot,system_name,strategy_mode,platform,ticker,forecast_source,forecast_version)
);
CREATE INDEX IF NOT EXISTS idx_rpssim_position ON research_proper_score_simulated_positions(
 strategy_mode,transform,platform,ticker,forecast_source,forecast_version,id DESC);
CREATE TRIGGER IF NOT EXISTS research_proper_score_sim_no_update BEFORE UPDATE ON research_proper_score_simulated_positions
BEGIN SELECT RAISE(ABORT,'immutable proper-score simulation receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_sim_no_delete BEFORE DELETE ON research_proper_score_simulated_positions
BEGIN SELECT RAISE(ABORT,'immutable proper-score simulation receipt'); END;`

// A vector slot is not defined by whichever trial rows happened to survive insertion.  The
// manifest freezes the exact coordinate set before any trial is written; the child rows make the
// hash contract auditable in SQL while the parent count/hash provide a compact immutable receipt.
// Both tables are append-only so a later collector pass cannot silently redefine a partial slot.
const properScoreManifestDDL = `
CREATE TABLE IF NOT EXISTS research_proper_score_manifests (
 slot TEXT NOT NULL,
 transform TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
 strategy_mode TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
 created_ts TEXT NOT NULL,
 forecast_source TEXT NOT NULL,
 forecast_version TEXT NOT NULL,
 expected_coordinate_count INTEGER NOT NULL CHECK(expected_coordinate_count>0),
 expected_coordinate_hash TEXT NOT NULL CHECK(length(expected_coordinate_hash)=64),
 contract_version INTEGER NOT NULL DEFAULT 1 CHECK(contract_version=1),
 PRIMARY KEY(slot,transform,strategy_mode)
);
CREATE TABLE IF NOT EXISTS research_proper_score_manifest_coordinates (
 slot TEXT NOT NULL,
 transform TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
 strategy_mode TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
 coordinate_ordinal INTEGER NOT NULL CHECK(coordinate_ordinal>=0),
 coordinate_key TEXT NOT NULL CHECK(length(coordinate_key)=64),
 platform TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 forecast_origin_side TEXT NOT NULL CHECK(forecast_origin_side IN ('YES','NO')),
 forecast_signal TEXT NOT NULL,
 forecast_source TEXT NOT NULL,
 forecast_version TEXT NOT NULL,
 route TEXT NOT NULL CHECK(route IN ('taker','maker')),
 PRIMARY KEY(slot,transform,strategy_mode,coordinate_key),
 UNIQUE(slot,transform,strategy_mode,coordinate_ordinal),
 FOREIGN KEY(slot,transform,strategy_mode)
  REFERENCES research_proper_score_manifests(slot,transform,strategy_mode)
);
CREATE INDEX IF NOT EXISTS idx_rpsmc_identity ON research_proper_score_manifest_coordinates(
 slot,transform,strategy_mode,platform,ticker,forecast_origin_side,forecast_signal,
 forecast_source,forecast_version,route);
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_no_update
BEFORE UPDATE ON research_proper_score_manifests
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_no_delete
BEFORE DELETE ON research_proper_score_manifests
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_coordinate_no_update
BEFORE UPDATE ON research_proper_score_manifest_coordinates
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest coordinate'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_coordinate_no_delete
BEFORE DELETE ON research_proper_score_manifest_coordinates
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest coordinate'); END;`

func ensureProperScoreSimulationSchema(db *sql.DB) error {
	_, err := db.Exec(properScoreSimulationDDL)
	return err
}

func ensureProperScoreManifestSchema(db *sql.DB) error {
	_, err := db.Exec(properScoreManifestDDL)
	return err
}

func ensureProperScoreAuxiliarySchema(db *sql.DB) error {
	if err := ensureProperScoreSimulationSchema(db); err != nil {
		return err
	}
	if err := ensureProperScoreManifestSchema(db); err != nil {
		return err
	}
	return ensureProperScorePaperSchema(db)
}

// migrateProperScoreV2 replaces the R137 binary/touch-only ledger with the paper-faithful vector,
// depth, tick, fill, and sequential receipt. Legacy rows remain available but are explicitly
// actual_fill=0 with unknown tick/depth provenance and therefore cannot qualify for promotion.
func migrateProperScoreV2(db *sql.DB) error {
	var tableSQL string
	err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='research_proper_score_trials'`).Scan(&tableSQL)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = db.Exec(properScoreV2DDL + `;
CREATE INDEX IF NOT EXISTS idx_rpst_open ON research_proper_score_trials(settled,platform,ticker);
CREATE INDEX IF NOT EXISTS idx_rpst_system ON research_proper_score_trials(system_name,transform,strategy_mode,settled,observed_ts);`)
		if err != nil {
			return err
		}
		return ensureProperScoreAuxiliarySchema(db)
	}
	if err != nil {
		return err
	}
	normalized := strings.ToLower(tableSQL)
	if strings.Contains(normalized, "'spherical'") && strings.Contains(normalized, "depth_curve_json") &&
		strings.Contains(normalized, "research_observation_id") {
		return ensureProperScoreAuxiliarySchema(db)
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`DROP INDEX IF EXISTS idx_rpst_open`,
		`DROP INDEX IF EXISTS idx_rpst_system`,
		`ALTER TABLE research_proper_score_trials RENAME TO research_proper_score_trials_v1_legacy`,
		properScoreV2DDL,
		`INSERT INTO research_proper_score_trials(
id,observed_ts,slot,system_name,transform,strategy_mode,cohort,platform,ticker,title,category,
resolve_hours,forecast_yes,forecast_origin_side,forecast_signal,forecast_source,forecast_version,
model_backend,calibration,generated_at,route,yes_bid,yes_ask,yes_bid_depth,yes_ask_depth,no_bid,no_ask,
no_bid_depth,no_ask_depth,book_source,source_clock_id,quote_age_s,decision_latency_ms,q_yes,raw_yes,raw_no,
raw_vector_json,vector_dim,constant_shift,rescale,normalized_weight,canonical_qty,requested_qty,
executable_qty,selected_side,tick_size,lot_size,depth_curve_json,entry_price,entry_depth,integrated_cost,
spot_cost,liquidity_loss,exact_fee_total,fee_source,expected_net,abstain_reason,prior_actual_position,
target_position,actual_filled_delta,cancelled_delta,post_actual_position,fill_status,actual_fill,
research_observation_id,settled,grade_status,grade_reason,settle_yes,forecast_score,book_score,score_delta,
realized_net,closed_ts)
SELECT id,observed_ts,slot,system_name,transform,'fundamental','legacy-v1',platform,ticker,title,category,
resolve_hours,forecast_yes,forecast_origin_side,forecast_signal,forecast_source,forecast_version,
model_backend,calibration,generated_at,route,yes_bid,yes_ask,yes_bid_depth,yes_ask_depth,no_bid,no_ask,
no_bid_depth,no_ask_depth,book_source,'',quote_age_s,0,q_yes,raw_yes,raw_no,
json_array(raw_yes,raw_no),2,0,1,0,canonical_qty,executable_qty,executable_qty,selected_side,0,0,'[]',
entry_price,entry_depth,executable_qty*entry_price,executable_qty*entry_price,0,exact_fee_total,fee_source,
expected_net,abstain_reason,0,0,0,executable_qty,0,'legacy_unknown',0,0,settled,grade_status,grade_reason,
settle_yes,forecast_score,book_score,score_delta,realized_net,closed_ts
FROM research_proper_score_trials_v1_legacy`,
		`DROP TABLE research_proper_score_trials_v1_legacy`,
		`CREATE INDEX idx_rpst_open ON research_proper_score_trials(settled,platform,ticker)`,
		`CREATE INDEX idx_rpst_system ON research_proper_score_trials(system_name,transform,strategy_mode,settled,observed_ts)`,
	} {
		if _, err := tx.ExecContext(context.Background(), q); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return ensureProperScoreAuxiliarySchema(db)
}
