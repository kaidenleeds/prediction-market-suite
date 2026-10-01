package storage

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The fresh-grade reconciliation must consume only the same bounded candidate batch as the
// archive mover. A full-archive UPDATE held the attached connection and main WAL snapshot for
// minutes on the production tape even though ArchiveResolvedPTT advertised a bounded batch.
func TestArchiveResolvedPTTWonSyncIsBatchBounded(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	old := time.Now().UTC().Add(-40 * 24 * time.Hour).Unix()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
VALUES('0xa',?,'c1','a1','BUY',1,.4,1,'tx1',1,1),
      ('0xb',?,'c2','a2','BUY',2,.6,1,'tx2',1,1)`, old, old)
	if moved, dup, err := s.ArchiveResolvedPTT(ctx, 30, 10); err != nil || moved != 2 || dup != 0 {
		t.Fatalf("seed archive: moved=%d dup=%d err=%v", moved, dup, err)
	}
	// Both prints re-enter with a fresh, opposite venue grade. batch=1 may reconcile only tx1.
	exec(`INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved,won)
VALUES('0xa',?,'c1','a1','BUY',1,.4,1,'tx1',1,0),
      ('0xb',?,'c2','a2','BUY',2,.6,1,'tx2',1,0)`, old, old)
	if moved, dup, err := s.ArchiveResolvedPTT(ctx, 30, 1); err != nil || moved != 0 || dup != 1 {
		t.Fatalf("first bounded sync: moved=%d dup=%d err=%v", moved, dup, err)
	}
	var tx1Won, tx2Won, mainTx1, mainTx2 int
	if err := s.archConn.QueryRowContext(ctx, `SELECT won FROM archive.poly_trader_trades WHERE tx_hash='tx1'`).Scan(&tx1Won); err != nil {
		t.Fatal(err)
	}
	if err := s.archConn.QueryRowContext(ctx, `SELECT won FROM archive.poly_trader_trades WHERE tx_hash='tx2'`).Scan(&tx2Won); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM poly_trader_trades WHERE tx_hash='tx1'`).Scan(&mainTx1); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM poly_trader_trades WHERE tx_hash='tx2'`).Scan(&mainTx2); err != nil {
		t.Fatal(err)
	}
	if tx1Won != 0 || tx2Won != 1 || mainTx1 != 0 || mainTx2 != 1 {
		t.Fatalf("batch boundary leaked: archive won tx1=%d tx2=%d main tx1=%d tx2=%d", tx1Won, tx2Won, mainTx1, mainTx2)
	}
	if moved, dup, err := s.ArchiveResolvedPTT(ctx, 30, 1); err != nil || moved != 0 || dup != 1 {
		t.Fatalf("second bounded sync: moved=%d dup=%d err=%v", moved, dup, err)
	}
	if err := s.archConn.QueryRowContext(ctx, `SELECT won FROM archive.poly_trader_trades WHERE tx_hash='tx2'`).Scan(&tx2Won); err != nil || tx2Won != 0 {
		t.Fatalf("second batch did not converge: won=%d err=%v", tx2Won, err)
	}
}

func TestArchivePTTWonSyncPlanIsCandidateDriven(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	if err := s.EnsureArchive(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.archConn.QueryContext(ctx, "EXPLAIN QUERY PLAN "+archivePTTWonSyncSQL,
		time.Now().UTC().Add(-30*24*time.Hour).Unix(), 20)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "MATERIALIZE archive_batch") ||
		!strings.Contains(joined, "SEARCH hit USING INDEX arc_ptt_dedup2") {
		t.Fatalf("won-sync plan lost its bounded candidate/index contract:\n%s", joined)
	}
	if strings.Contains(joined, "SCAN hit") || strings.Contains(joined, "SCAN a") {
		t.Fatalf("won-sync plan scans the full archive instead of candidate rowids:\n%s", joined)
	}
}
