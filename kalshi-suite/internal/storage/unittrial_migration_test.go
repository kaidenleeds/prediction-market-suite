package storage

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestR135cUnitTrialOriginMigrationRebuildsLegacyUniqueKey(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE unit_trials (
 id INTEGER PRIMARY KEY AUTOINCREMENT, opened_ts TEXT NOT NULL, closed_ts TEXT NOT NULL DEFAULT '',
 family TEXT NOT NULL, platform TEXT NOT NULL, ticker TEXT NOT NULL, side TEXT NOT NULL,
 episode INTEGER NOT NULL DEFAULT 0, category TEXT NOT NULL DEFAULT '', ask REAL NOT NULL,
 fee_pc REAL NOT NULL DEFAULT 0, depth REAL NOT NULL DEFAULT 0, quote_source TEXT NOT NULL DEFAULT '',
 resolve_hours REAL NOT NULL DEFAULT 0, settled INTEGER NOT NULL DEFAULT 0, settle_val REAL,
 pnl_pc REAL, return_per_dollar REAL, capital_day REAL,
 UNIQUE(family,platform,ticker,side,episode));
 CREATE INDEX idx_unit_trials_open ON unit_trials(settled,platform,ticker);
 CREATE INDEX idx_unit_trials_family ON unit_trials(family,platform,settled);
 INSERT INTO unit_trials(opened_ts,family,platform,ticker,side,ask,quote_source)
 VALUES('2026-07-11T00:00:00Z','legacy-model','kalshi','A','YES',.2,'kalshi-ws'),
       ('2026-07-11T00:00:00Z','legacy-strategy','kalshi','B','YES',.2,'strategy-decision/kalshi-ws');`); err != nil {
		t.Fatal(err)
	}
	if err := migrateUnitTrialOrigin(db); err != nil {
		t.Fatal(err)
	}
	var modelN, strategyN int
	if err := db.QueryRow(`SELECT SUM(origin_layer='model'),SUM(origin_layer='strategy') FROM unit_trials`).Scan(&modelN, &strategyN); err != nil {
		t.Fatal(err)
	}
	if modelN != 1 || strategyN != 1 {
		t.Fatalf("legacy provenance lost: model=%d strategy=%d", modelN, strategyN)
	}
	var feeKnown int
	var feeSource string
	if err := db.QueryRow(`SELECT fee_known,fee_source FROM unit_trials WHERE family='legacy-model'`).Scan(&feeKnown, &feeSource); err != nil {
		t.Fatal(err)
	}
	if feeKnown != 0 || feeSource != "" {
		t.Fatalf("legacy fallback fee was promoted into an exact receipt: known=%d source=%q", feeKnown, feeSource)
	}
	// The same opportunity may now exist once per deliberate origin cohort.
	for _, origin := range []string{"model", "strategy"} {
		if _, err := db.Exec(`INSERT INTO unit_trials(opened_ts,family,platform,origin_layer,ticker,side,ask)
VALUES('2026-07-11T01:00:00Z','same','kalshi',?,'SAME','YES',.3)`, origin); err != nil {
			t.Fatalf("insert %s origin: %v", origin, err)
		}
	}
	var ddl string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='unit_trials'`).Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	compact := strings.NewReplacer(" ", "", "\n", "", "\r", "", "\t", "").Replace(strings.ToLower(ddl))
	if !strings.Contains(compact, "unique(family,platform,ticker,side,episode,origin_layer)") {
		t.Fatalf("new unique key missing: %s", ddl)
	}
	if !strings.Contains(compact, "fee_known") || !strings.Contains(compact, "fee_source") {
		t.Fatalf("fee provenance columns missing: %s", ddl)
	}
}

func TestR144OpenMigratesLegacyUnitTrialsBeforeEventIndex(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "kalshi.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE unit_trials (
 id INTEGER PRIMARY KEY AUTOINCREMENT, opened_ts TEXT NOT NULL, closed_ts TEXT NOT NULL DEFAULT '',
 family TEXT NOT NULL, platform TEXT NOT NULL, ticker TEXT NOT NULL, side TEXT NOT NULL,
 episode INTEGER NOT NULL DEFAULT 0, category TEXT NOT NULL DEFAULT '', ask REAL NOT NULL,
 fee_pc REAL NOT NULL DEFAULT 0, depth REAL NOT NULL DEFAULT 0, quote_source TEXT NOT NULL DEFAULT '',
 resolve_hours REAL NOT NULL DEFAULT 0, settled INTEGER NOT NULL DEFAULT 0, settle_val REAL,
 pnl_pc REAL, return_per_dollar REAL, capital_day REAL,
 UNIQUE(family,platform,ticker,side,episode));
 CREATE INDEX idx_unit_trials_open ON unit_trials(settled,platform,ticker);
 CREATE INDEX idx_unit_trials_family ON unit_trials(family,platform,settled);
 INSERT INTO unit_trials(opened_ts,family,platform,ticker,side,ask,quote_source)
 VALUES('2026-07-11T00:00:00Z','legacy','kalshi','LEGACY','YES',.2,'kalshi-ws');`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open must migrate the pre-R144 unit_trials table before creating its event index: %v", err)
	}
	defer st.Close()

	for _, column := range []string{"origin_layer", "canonical_event_id", "event_version"} {
		var count int
		if err := st.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('unit_trials') WHERE name=?`, column).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("migrated unit_trials column %s count=%d", column, count)
		}
	}
	var indexes int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_unit_trials_event'`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if indexes != 1 {
		t.Fatalf("idx_unit_trials_event count=%d", indexes)
	}
	var rows int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM unit_trials WHERE family='legacy'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("legacy unit trial rows=%d", rows)
	}
}
