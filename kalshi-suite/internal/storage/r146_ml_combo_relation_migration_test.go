package storage

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestR146FundedRelationMigrationAddsMLComboPortfolioWithoutDataLoss(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "legacy.db")+"?_pragma=foreign_keys(ON)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	legacyDDL := strings.Replace(fundedRelationDDL, ",'ml-combos'", "", 1)
	if _, err := db.Exec(legacyDDL); err != nil {
		t.Fatal(err)
	}
	fingerprint := strings.Repeat("a", 64)
	position := strings.Repeat("b", 64)
	receipt := strings.Repeat("c", 64)
	if _, err := db.Exec(`INSERT INTO funded_relation_receipts(
receipt_id,decision_fingerprint,decision_ts,portfolio,position_fingerprint,candidate_venue,candidate_ticker,
candidate_side,system_id,route_kind,relation_state,allowed,reason,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, receipt, fingerprint, "2026-07-15T00:00:00Z", "combos", position,
		"kalshi", "LEGACY", "YES", "legacy-system", "combo-taker", "unknown", 1, "legacy", "2026-07-15T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := migrateFundedRelationSchema(db); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := db.QueryRow(`SELECT portfolio FROM funded_relation_receipts WHERE receipt_id=?`, receipt).Scan(&got); err != nil || got != "combos" {
		t.Fatalf("legacy receipt lost: portfolio=%q err=%v", got, err)
	}
	if _, err := db.Exec(`INSERT INTO funded_relation_receipts(
receipt_id,decision_fingerprint,decision_ts,portfolio,position_fingerprint,candidate_venue,candidate_ticker,
candidate_side,system_id,route_kind,relation_state,allowed,reason,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, strings.Repeat("d", 64), strings.Repeat("e", 64), "2026-07-15T00:01:00Z",
		"ml-combos", strings.Repeat("f", 64), "polyus", "NEW", "YES", "new-ml", "combo-taker",
		"unknown", 1, "new", "2026-07-15T00:01:00Z"); err != nil {
		t.Fatalf("migrated CHECK still rejects ml-combos: %v", err)
	}
}
