package storage

import (
	"context"
	"testing"
	"time"
)

func TestR144InversePaperJournalClaimsSignalAndCommitsAtomically(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	fee := .01
	if inserted, err := st.InsertSignalResult(ctx, Signal{Platform: "kalshi", Ticker: "KXR144-J",
		Side: "NO", SignalType: "invert:edge", EntryPrice: .4, FeePC: &fee}); err != nil || !inserted {
		t.Fatalf("insert signal: %v %v", inserted, err)
	}
	p, err := st.PrepareInversePaperPlacement(ctx, "kalshi", "KXR144-J", "NO", "invert:edge",
		.4, .01, 1, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.PrepareInversePaperPlacement(ctx, "kalshi", "KXR144-J", "NO", "invert:edge",
		.4, .01, 1, time.Now().UTC().Add(time.Nanosecond)); err == nil {
		t.Fatal("two active journals claimed the same unfilled signal")
	}
	if ok, err := st.CommitInversePaperPlacement(ctx, p.PlacementID); err != nil || ok {
		t.Fatalf("prepared journal skipped ledger-persisted state: ok=%v err=%v", ok, err)
	}
	if ok, err := st.MarkInversePaperLedgerPersisted(ctx, p.PlacementID); err != nil || !ok {
		t.Fatalf("mark ledger: ok=%v err=%v", ok, err)
	}
	if ok, err := st.CommitInversePaperPlacement(ctx, p.PlacementID); err != nil || !ok {
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}
	var fill, storedFee float64
	if err := st.db.QueryRowContext(ctx, `SELECT fill_price,fee_pc FROM signal_log WHERE id=?`, p.SignalID).
		Scan(&fill, &storedFee); err != nil || fill != .4 || storedFee != .01 {
		t.Fatalf("atomic signal fill missing: fill=%v fee=%v err=%v", fill, storedFee, err)
	}
	journal, ok, err := st.InversePaperPlacement(ctx, p.PlacementID)
	if err != nil || !ok || journal.State != "committed" {
		t.Fatalf("journal not committed with fill: ok=%v state=%q err=%v", ok, journal.State, err)
	}
}
