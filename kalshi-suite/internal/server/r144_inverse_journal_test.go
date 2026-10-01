package server

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r144JournalFixture(t *testing.T, s *Server, ticker string) (storage.InversePaperPlacement, kfPos, string) {
	t.Helper()
	ctx := context.Background()
	fee := .01
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "journal fixture", Side: "NO",
		SignalType: "invert:kalshi-flow", EntryPrice: .40, FeePC: &fee,
		ResolveHours: 1, PricingVersion: "independent-inverse-executable-v1"}
	if inserted, err := s.store.InsertSignalResult(ctx, sig); err != nil || !inserted {
		t.Fatalf("insert inverse signal: inserted=%v err=%v", inserted, err)
	}
	decision := time.Now().UTC()
	p, err := s.store.PrepareInversePaperPlacement(ctx, "kalshi", ticker, "NO",
		"invert:kalshi-flow", .40, .01, 2, decision)
	if err != nil {
		t.Fatal(err)
	}
	key := gfRosterKey("invert:kalshi-flow", "kalshi", "NO", "taker")
	lot := kfPos{TS: p.DecisionTS, SignalTS: p.SignalTS, DecisionTS: p.DecisionTS,
		FillTS: p.DecisionTS, PlacementID: p.PlacementID, Platform: "kalshi", Ticker: ticker,
		Title: "journal fixture", Side: "NO", Price: .40, Contracts: 2, Fee: .02,
		FeeKnown: true, FeeSource: "test", FillKind: "taker",
		RouteReason: "fam:invert:kalshi-flow route=taker side=NO"}
	return p, lot, key
}

func TestR144PreparedInverseProjectionIsQuarantinedBeforeSettlement(t *testing.T) {
	s := testServer(t)
	p, lot, key := r144JournalFixture(t, s, "KXR144-JOURNAL-PREPARED")
	s.gfBookMu.Lock()
	s.gfLoadLocked().Subs[key] = &kfBook{Open: []kfPos{lot}}
	s.gfBookDirty = true
	s.gfBookMu.Unlock()
	if err := s.genfollowFlush(); err != nil {
		t.Fatal(err)
	}

	s.gfJournalMu.Lock()
	err := s.reconcileInverseGenfollowPlacements(context.Background())
	s.gfJournalMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	s.gfBookMu.Lock()
	open := len(s.gfLoadLocked().Subs[key].Open)
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("prepared crash-window lot survived reconciliation: open=%d", open)
	}
	journal, ok, err := s.store.InversePaperPlacement(context.Background(), p.PlacementID)
	if err != nil || !ok || journal.State != "quarantined" {
		t.Fatalf("prepared journal not quarantined: ok=%v state=%q err=%v", ok, journal.State, err)
	}
	var fill float64
	if err := s.store.DBForTest().QueryRow(`SELECT fill_price FROM signal_log WHERE id=?`, p.SignalID).Scan(&fill); err != nil || fill != 0 {
		t.Fatalf("quarantined projection stamped a fill: fill=%v err=%v", fill, err)
	}
}

func TestR144SettlementWaitsForInverseJournalCommit(t *testing.T) {
	s := testServer(t)
	p, lot, key := r144JournalFixture(t, s, "KXR144-JOURNAL-COMMIT")
	s.gfJournalMu.Lock() // model the placement critical section through JSON projection + commit
	s.gfBookMu.Lock()
	s.gfLoadLocked().Subs[key] = &kfBook{Open: []kfPos{lot}}
	s.gfBookDirty = true
	s.gfBookMu.Unlock()
	if err := s.genfollowFlush(); err != nil {
		s.gfJournalMu.Unlock()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		s.settleGenfollowBook(context.Background())
		close(done)
	}()
	select {
	case <-done:
		s.gfJournalMu.Unlock()
		t.Fatal("settlement crossed an in-flight inverse placement")
	case <-time.After(30 * time.Millisecond):
	}
	if ok, err := s.store.MarkInversePaperLedgerPersisted(context.Background(), p.PlacementID); err != nil || !ok {
		s.gfJournalMu.Unlock()
		t.Fatalf("mark projected: ok=%v err=%v", ok, err)
	}
	if ok, err := s.store.CommitInversePaperPlacement(context.Background(), p.PlacementID); err != nil || !ok {
		s.gfJournalMu.Unlock()
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}
	if err := s.store.ResolveSignals(context.Background(), lot.Ticker, 1); err != nil {
		s.gfJournalMu.Unlock()
		t.Fatal(err)
	}
	s.gfJournalMu.Unlock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("settlement did not resume after inverse commit")
	}
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	closed, net := len(book.Closed), book.Net
	s.gfBookMu.Unlock()
	if closed != 1 || net >= 0 || math.Abs(net-(-.82)) > .011 {
		t.Fatalf("committed NO lot did not settle once: closed=%d net=%v", closed, net)
	}
	journal, ok, err := s.store.InversePaperPlacement(context.Background(), p.PlacementID)
	if err != nil || !ok || journal.State != "settled" {
		t.Fatalf("durable close did not settle journal: ok=%v state=%q err=%v", ok, journal.State, err)
	}
}
