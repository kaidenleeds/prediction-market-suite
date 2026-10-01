package storage

import (
	"context"
	"testing"
)

func TestNewestUnresolvedTradeTimesDoesNotUseOldestRepresentativeRow(t *testing.T) {
	s, _ := openTemp(t)
	ctx := context.Background()
	condRecent := r132Condition("12")
	condOld := r132Condition("34")
	condResolved := r132Condition("56")

	insert := func(cid, tx string, ts int64, resolved int) {
		t.Helper()
		if _, err := s.db.Exec(`INSERT INTO poly_trader_trades
(wallet,ts,condition_id,asset,side,size,price,usdc_size,tx_hash,resolved)
VALUES('wallet',?,?,?,'BUY',1,.5,.5,?,?)`, ts, cid, "asset-"+tx, tx, resolved); err != nil {
			t.Fatal(err)
		}
	}
	insert(condRecent, "recent-old", 100, 0)
	insert(condRecent, "recent-new", 9_500, 0)
	insert(condRecent, "recent-already-graded", 10_500, 1)
	insert(condOld, "old", 200, 0)
	insert(condResolved, "resolved", 300, 1)

	page, _, _, err := s.UnresolvedRowsPage(ctx, 0, 0, 10_000, 20)
	if err != nil {
		t.Fatal(err)
	}
	var representative int64
	for _, p := range page {
		if p.ConditionID == condRecent {
			representative = p.RepresentativeTS
		}
	}
	if representative != 100 {
		t.Fatalf("representative=%d, want oldest discovered row 100", representative)
	}

	got, err := s.NewestUnresolvedTradeTimes(ctx, []string{condRecent, condOld, condRecent, condResolved, ""})
	if err != nil {
		t.Fatal(err)
	}
	if got[condRecent] != 9_500 {
		t.Fatalf("recent newest=%d, want 9500 (resolved newer row excluded)", got[condRecent])
	}
	if got[condOld] != 200 {
		t.Fatalf("old newest=%d, want 200", got[condOld])
	}
	if _, ok := got[condResolved]; ok {
		t.Fatalf("resolved-only condition unexpectedly returned: %#v", got)
	}

	// The final mutation repeats the age check atomically: a condition discovered by its old
	// row cannot be retired when it also has an unresolved row at or beyond the cutoff.
	if n, err := s.MarkConditionUnresolvableVenueIfNewestBefore(ctx, condRecent, 1_000); err != nil || n != 0 {
		t.Fatalf("recent condition marked rows=%d err=%v", n, err)
	}
	if n, err := s.MarkConditionUnresolvableVenueIfNewestBefore(ctx, condOld, 1_000); err != nil || n != 1 {
		t.Fatalf("old condition marked rows=%d err=%v", n, err)
	}
	var recentResolved, oldResolved int
	if err := s.db.QueryRow(`SELECT resolved FROM poly_trader_trades WHERE tx_hash='recent-old'`).Scan(&recentResolved); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT resolved FROM poly_trader_trades WHERE tx_hash='old'`).Scan(&oldResolved); err != nil {
		t.Fatal(err)
	}
	if recentResolved != 0 || oldResolved != -2 {
		t.Fatalf("atomic age guard recent=%d old=%d", recentResolved, oldResolved)
	}
}
