package storage

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestR139FreshResearchObservationSchemaHasExactIdentityColumnsAndTrigger(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	want := map[string]bool{"ticker": false, "canonical_payoff_id": false, "payoff_version": false,
		"instrument_version": false, "decision_ts": false}
	rows, err := st.db.Query(`PRAGMA table_info(research_system_observations)`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &def, &pk); err != nil {
			t.Fatal(err)
		}
		if _, ok := want[name]; ok {
			want[name] = true
		}
	}
	rows.Close()
	for name, found := range want {
		if !found {
			t.Fatalf("fresh schema missing %s", name)
		}
	}
	var triggers int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'
AND name='research_system_observations_exact_identity_insert'`).Scan(&triggers); err != nil || triggers != 1 {
		t.Fatalf("exact identity trigger=%d err=%v", triggers, err)
	}
}

func TestR139LegacyResearchObservationSchemaMigratesAdditively(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+`\legacy.db?_pragma=foreign_keys(ON)`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, ddl := range []string{
		`CREATE TABLE research_system_observations(id INTEGER PRIMARY KEY, canonical_event_id TEXT NOT NULL DEFAULT '', event_version INTEGER NOT NULL DEFAULT 0, venue TEXT NOT NULL DEFAULT '', route TEXT NOT NULL)`,
		`CREATE TABLE research_event_specs(event_id TEXT NOT NULL,version INTEGER NOT NULL,PRIMARY KEY(event_id,version))`,
		`CREATE TABLE research_payoff_specs(event_id TEXT NOT NULL,event_version INTEGER NOT NULL,payoff_id TEXT NOT NULL,version INTEGER NOT NULL,PRIMARY KEY(event_id,event_version,payoff_id,version))`,
		`CREATE TABLE research_instrument_specs(venue TEXT NOT NULL,ticker TEXT NOT NULL,version INTEGER NOT NULL,event_id TEXT NOT NULL,event_version INTEGER NOT NULL,payoff_id TEXT NOT NULL,payoff_version INTEGER NOT NULL,PRIMARY KEY(venue,ticker,version))`,
	} {
		if _, err := db.Exec(ddl); err != nil {
			t.Fatal(err)
		}
	}
	if err := migrateResearchSystemObservationIdentity(db); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"ticker", "canonical_payoff_id", "payoff_version", "instrument_version", "decision_ts"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('research_system_observations') WHERE name=?`, column).Scan(&n); err != nil || n != 1 {
			t.Fatalf("legacy column %s=%d err=%v", column, n, err)
		}
	}
	if _, err := db.Exec(`INSERT INTO research_system_observations(canonical_event_id,event_version,
venue,route,ticker,canonical_payoff_id,payoff_version) VALUES('missing',1,'kalshi','taker','KX','P',1)`); err == nil {
		t.Fatal("migrated legacy schema accepted an economic row without exact instrument/payoff identity")
	}
}
