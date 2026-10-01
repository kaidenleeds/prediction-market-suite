package storage

// R90 bug 99 (auditor DO-THIS 2 — the boot-brick): Open used to DROP + CREATE the signal dedup
// index on every boot with errors discarded; one failed CREATE left the DB index-less (dedup
// silently dead), and schema.sql's legacy per-DAY index shape then made every later boot die
// fatally at the schema pass. These tests lock the guarded rebuild: healthy DBs skip the
// rebuild, index-less DBs with real duplicates are deduped (earliest row wins) and repaired,
// and legacy day-shape indexes are migrated to the slot shape.

import (
	"database/sql"
	"strings"
	"testing"
)

func dedupIndexSQL(t *testing.T, db *sql.DB) string {
	t.Helper()
	var s string
	_ = db.QueryRow(`SELECT COALESCE(sql,'') FROM sqlite_master WHERE type='index' AND name='idx_signal_dedup'`).Scan(&s)
	return s
}

func assertSlotShaped(t *testing.T, db *sql.DB) {
	t.Helper()
	norm := strings.ReplaceAll(strings.ToLower(dedupIndexSQL(t, db)), " ", "")
	if !strings.Contains(norm, "unique") ||
		!strings.Contains(norm, "(slot,platform,ticker,side,signal_type,input_topology)") {
		t.Fatalf("idx_signal_dedup not the unique slot shape: %q", norm)
	}
}

// The post-failure state a swallowed CREATE used to leave behind: no dedup index at all, plus
// real duplicates on the dedup key inserted while dedup was dead. Open must repair, not brick.
func TestBug99RecoversFromMissingIndexWithDuplicates(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := st.db.Exec("DROP INDEX idx_signal_dedup"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := st.db.Exec(`INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved)
			VALUES('2026-07-06T12:00:0'||?,'2026-07-06','2026-07-06T12:0','kalshi','R90-TICK-A','T','YES','kalshi-flow',0.5,0)`, i); err != nil {
			t.Fatalf("insert dup %d: %v", i, err)
		}
	}
	st.Close()

	st2, err := Open(dir) // must NOT brick: dedupe (keep earliest) + rebuild
	if err != nil {
		t.Fatalf("Open after simulated index loss must recover, got: %v", err)
	}
	defer st2.Close()
	var n int
	if err := st2.db.QueryRow(`SELECT COUNT(*) FROM signal_log WHERE ticker='R90-TICK-A'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("want 1 deduped row, got %d (err %v)", n, err)
	}
	var ts string
	if err := st2.db.QueryRow(`SELECT ts FROM signal_log WHERE ticker='R90-TICK-A'`).Scan(&ts); err != nil || !strings.HasSuffix(ts, "00") {
		t.Fatalf("earliest row must win the dedupe, got ts=%q (err %v)", ts, err)
	}
	assertSlotShaped(t, st2.db)
	// Dedup semantics must be live again: same key twice → one row.
	ins := `INSERT OR IGNORE INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved)
		VALUES(?,'2026-07-06','2026-07-06T13:0','kalshi','R90-TICK-B','T','NO','kalshi-flow',0.4,0)`
	if _, err := st2.db.Exec(ins, "2026-07-06T13:00:00Z"); err != nil {
		t.Fatalf("insert B1: %v", err)
	}
	if _, err := st2.db.Exec(ins, "2026-07-06T13:00:05Z"); err != nil {
		t.Fatalf("insert B2: %v", err)
	}
	if err := st2.db.QueryRow(`SELECT COUNT(*) FROM signal_log WHERE ticker='R90-TICK-B'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("post-repair dedup broken: %d rows for one key (err %v)", n, err)
	}
}

// A DB carrying the legacy per-DAY unique index (what schema.sql used to create when the index
// name was free) must migrate to the slot shape instead of dying at the schema pass.
func TestBug99MigratesLegacyDayShape(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := st.db.Exec("DROP INDEX idx_signal_dedup"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := st.db.Exec("CREATE UNIQUE INDEX idx_signal_dedup ON signal_log(day,ticker,side,signal_type)"); err != nil {
		t.Fatalf("legacy create: %v", err)
	}
	st.Close()
	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("Open with legacy day shape: %v", err)
	}
	defer st2.Close()
	assertSlotShaped(t, st2.db)
}

// Healthy DBs (correct index already present) must reopen cleanly — the guard skips the rebuild.
func TestBug99HealthyReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	assertSlotShaped(t, st.db)
	st.Close()
	st2, err := Open(dir)
	if err != nil {
		t.Fatalf("healthy re-Open: %v", err)
	}
	defer st2.Close()
	assertSlotShaped(t, st2.db)
}
