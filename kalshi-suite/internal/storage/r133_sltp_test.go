package storage

import (
	"context"
	"fmt"
	"testing"
)

// The pre-R133 tuner read the newest 8,000 signal rows and filtered in Go. A busy unresolved
// board could therefore hide every older resolved path and silently leave the Adaptive Allocation Model untuned.
func TestR133SLTPEligibleQueryCannotBeStarvedByNewerUnresolvedRows(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,
 resolved,won,fill_price,post_path)
VALUES('2026-07-01T00:00:00Z','2026-07-01','0','kalshi','ELIGIBLE','eligible','YES',
 'xvgap',0.40,1,1,0.41,'0.41,0.55')`); err != nil {
		t.Fatalf("insert eligible: %v", err)
	}
	if _, err := s.db.ExecContext(ctx, `
INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,
 resolved,won,fill_price,post_path)
VALUES('2026-07-01T00:01:00Z','2026-07-01','0','kalshi','ONE-TOKEN','bad path','YES',
 'xvgap',0.40,1,1,0.41,'0.41')`); err != nil {
		t.Fatalf("insert one-token path: %v", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,resolved,won)
VALUES('2026-07-10T00:00:00Z','2026-07-10','0','kalshi',?,'open','YES','freshlist',0.50,0,NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8001; i++ {
		if _, err := stmt.ExecContext(ctx, fmt.Sprintf("OPEN-%05d", i)); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			t.Fatalf("insert unresolved %d: %v", i, err)
		}
	}
	_ = stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	rows, err := s.ListSLTPEligibleSignals(ctx, 8000)
	if err != nil {
		t.Fatalf("ListSLTPEligibleSignals: %v", err)
	}
	if len(rows) != 1 || rows[0].Ticker != "ELIGIBLE" || rows[0].PostPath != "0.41,0.55" {
		t.Fatalf("eligible query was starved or admitted malformed path: %#v", rows)
	}
}
