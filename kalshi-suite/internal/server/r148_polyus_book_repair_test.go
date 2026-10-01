package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR148StartupRepairErrorStopsServerBeforeListen(t *testing.T) {
	s := testServer(t)
	s.startupErr = errors.New("fixture funded-ledger repair failure")
	if err := s.Start(); err == nil || !strings.Contains(err.Error(), "fixture funded-ledger") {
		t.Fatalf("Start did not fail closed on repair error: %v", err)
	}
}

func r148InsertSettlementQuarantine(t *testing.T, s *Server, ticker string, value float64) {
	t.Helper()
	stamp := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	if _, err := s.store.DBForTest().Exec(`INSERT INTO polyus_settlement_quarantine(
platform,ticker,yes_value,resolved_at,source_artifact,reason,quarantined_at)
VALUES('polyus',?,?,?,'legacy BookFull settlementPx','fixture',?)`, ticker, value, stamp, stamp); err != nil {
		t.Fatal(err)
	}
}

func r148ClosedFixture(ticker string, payout float64, settled time.Time) kfClosed {
	p := kfPos{TS: settled.Add(-time.Hour).Format(time.RFC3339Nano), Platform: "polyus",
		Ticker: ticker, Title: "R148 repair fixture", Side: "YES", Price: .40, Contracts: 2,
		Fee: .02, Marks: []kfMark{{TS: settled.Add(-time.Minute).Unix(), Px: payout, Src: "settle"}}}
	pnl := p.Contracts*(payout-p.Price) - p.Fee
	return kfClosed{kfPos: p, Payout: payout, PnL: math.Round(pnl*100) / 100,
		Won: payout > p.Price, SettledTS: settled.UTC().Format(time.RFC3339Nano), Reason: "settled"}
}

func TestR148PolyUSFundedJSONBooksReopenAndQuarantineIdempotently(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	settled := time.Now().UTC().Add(-30 * time.Minute)
	tickers := []string{"R148-GEN", "R148-CHEAP", "R148-FRESH", "R148-XVG", "R148-XVL"}
	for i, ticker := range tickers {
		r148InsertSettlementQuarantine(t, s, ticker, float64(i%2))
	}

	gen := r148ClosedFixture(tickers[0], 1, settled)
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{"gf:r148-p": {
		Bank: 600, Closed: []kfClosed{gen}, Net: r148SettlementPnL(gen), Wins: 1,
	}}}
	cheap := r148ClosedFixture(tickers[1], 0, settled)
	s.cbBook = &cbBook{P: kfBook{Bank: 600, Closed: []kfClosed{cheap},
		Net: r148SettlementPnL(cheap), Losses: 1}}
	fresh := r148ClosedFixture(tickers[2], 1, settled)
	s.fiBook = &kfBook{Bank: 600, Closed: []kfClosed{fresh}, Net: r148SettlementPnL(fresh), Wins: 1}
	xvg := r148ClosedFixture(tickers[3], 0, settled)
	s.xvgBook = &kfBook{Bank: 600, Closed: []kfClosed{xvg}, Net: r148SettlementPnL(xvg), Losses: 1}
	xvl := xvLockOpp{Key: "r148-xvl", Pair: "K-PUS", AVenue: "kalshi", AID: "K-R148-XVL",
		ASide: "YES", BVenue: "polyus", BID: tickers[4], BSide: "NO", AAsk: .4, BAsk: .4,
		AWon: 1, BWon: 1, RealizedC: 120, Mismatch: true, PairQuarantined: true,
		SettledTS: settled.Format(time.RFC3339Nano), Staked: 2, StakedPnL: 2.4}
	pairKey := xvlOppPairKey(xvl)
	s.xvlBook = &xvLockBook{Closed: []xvLockOpp{xvl}, MismatchN: 1,
		QuarantinedPairs: map[string]xvLockQuarantine{pairKey: {Pair: "K-PUS", MismatchN: 1}}}

	if err := s.repairR148PolyUSFundedJSONBooks(ctx); err != nil {
		t.Fatal(err)
	}
	for name, book := range map[string]*kfBook{
		"genfollow": s.gfBook.Subs["gf:r148-p"], "cheapband": &s.cbBook.P,
		"freshinv": s.fiBook, "xvgap": s.xvgBook,
	} {
		if len(book.Open) != 1 || len(book.Closed) != 0 || math.Abs(book.Net) > 1e-9 ||
			book.Wins != 0 || book.Losses != 0 {
			t.Fatalf("%s was not exactly reopened: %+v", name, book)
		}
		if marks := book.Open[0].Marks; len(marks) != 0 {
			t.Fatalf("%s retained false terminal mark: %+v", name, marks)
		}
	}
	if len(s.xvlBook.Open) != 1 || len(s.xvlBook.Closed) != 0 || s.xvlBook.MismatchN != 0 ||
		s.xvlBook.Open[0].BWon != -1 || s.xvlBook.Open[0].SettledTS != "" ||
		len(s.xvlBook.QuarantinedPairs) != 0 {
		t.Fatalf("xvlock was not exactly reopened: %+v", s.xvlBook)
	}
	var quarantined int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM polyus_kf_settlement_quarantine`).Scan(&quarantined); err != nil {
		t.Fatal(err)
	}
	if quarantined != 5 {
		t.Fatalf("quarantined=%d want 5", quarantined)
	}
	for _, file := range []string{"genfollow_book.json", "cheapband_book.json", "freshinv_book.json", "xvgap_book.json", "xvlock_book.json"} {
		raw, err := os.ReadFile(filepath.Join(s.cfg().DataDir, file))
		if err != nil || !json.Valid(raw) {
			t.Fatalf("%s was not atomically persisted: err=%v raw=%s", file, err, raw)
		}
	}
	if err := s.repairR148PolyUSFundedJSONBooks(ctx); err != nil {
		t.Fatal(err)
	}
	var replayN int
	_ = s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM polyus_kf_settlement_quarantine`).Scan(&replayN)
	if replayN != quarantined {
		t.Fatalf("restart replay duplicated quarantine: %d -> %d", quarantined, replayN)
	}
}

func TestR148PolyUSInverseJournalReopensWithJSONLot(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	ticker := "R148-INVERSE-JOURNAL"
	r148InsertSettlementQuarantine(t, s, ticker, 1)
	fee := .01
	decision := time.Now().UTC()
	sig := storage.Signal{Platform: "polyus", Ticker: ticker, Title: "R148 inverse", Side: "NO",
		SignalType: "invert:fbridge", EntryPrice: .40, FeePC: &fee, ResolveHours: 1,
		PricingVersion: "independent-inverse-executable-v1"}
	if inserted, err := s.store.InsertSignalResult(ctx, sig); err != nil || !inserted {
		t.Fatalf("insert inverse signal: inserted=%v err=%v", inserted, err)
	}
	journal, err := s.store.PrepareInversePaperPlacement(ctx, "polyus", ticker, "NO",
		"invert:fbridge", .40, .01, 2, decision)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := s.store.MarkInversePaperLedgerPersisted(ctx, journal.PlacementID); err != nil || !ok {
		t.Fatalf("mark projected: ok=%v err=%v", ok, err)
	}
	if ok, err := s.store.CommitInversePaperPlacement(ctx, journal.PlacementID); err != nil || !ok {
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}
	if ok, err := s.store.SettleInversePaperPlacement(ctx, journal.PlacementID); err != nil || !ok {
		t.Fatalf("settle: ok=%v err=%v", ok, err)
	}
	closed := r148ClosedFixture(ticker, 1, time.Now().UTC().Add(-30*time.Minute))
	closed.Side, closed.PlacementID = "NO", journal.PlacementID
	closed.Payout = 0
	closed.PnL = math.Round(r148SettlementPnL(closed)*100) / 100
	closed.Won = false
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{"gf:invert-fbridge-p": {
		Closed: []kfClosed{closed}, Net: r148SettlementPnL(closed), Losses: 1,
	}}}
	if err := s.repairR148PolyUSFundedJSONBooks(ctx); err != nil {
		t.Fatal(err)
	}
	got, found, err := s.store.InversePaperPlacement(ctx, journal.PlacementID)
	if err != nil || !found || got.State != "committed" || got.Detail != r148PolyUSSettlementRegrade {
		t.Fatalf("journal not reopened: found=%v state=%q detail=%q err=%v", found, got.State, got.Detail, err)
	}
	if len(s.gfBook.Subs["gf:invert-fbridge-p"].Open) != 1 || len(s.gfBook.Subs["gf:invert-fbridge-p"].Closed) != 0 {
		t.Fatalf("inverse lot not reopened: %+v", s.gfBook.Subs["gf:invert-fbridge-p"])
	}
	// Replaying after the JSON write is a no-op and must leave the journal committed.
	if err := s.repairR148PolyUSFundedJSONBooks(ctx); err != nil {
		t.Fatal(err)
	}
	got, _, _ = s.store.InversePaperPlacement(ctx, journal.PlacementID)
	if got.State != "committed" || !strings.Contains(got.Detail, "settlement provenance") {
		t.Fatalf("journal replay changed repaired state: %+v", got)
	}
}

func TestR148TrimmedFundedBookStartsVersionedCleanEpochInsteadOfStayingPoisoned(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	ticker := "R148-TRIMMED-GEN"
	r148InsertSettlementQuarantine(t, s, ticker, 1)
	closed := r148ClosedFixture(ticker, 1, time.Now().UTC().Add(-time.Hour))
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{"gf:trimmed-p": {
		Bank: 600, Closed: []kfClosed{closed}, Net: 14.25, Wins: 3, Losses: 2,
		NetBase: 4.25, WinsBase: 1, LossesBase: 1,
	}}}
	if err := s.repairR148PolyUSFundedJSONBooks(ctx); err != nil {
		t.Fatal(err)
	}
	book := s.gfBook.Subs["gf:trimmed-p"]
	if s.gfBookPoisoned.Load() || len(book.Open) != 1 || len(book.Closed) != 0 ||
		book.Net != 0 || book.Wins != 0 || book.Losses != 0 || book.NetBase != 0 ||
		book.SettlementEpoch != r148SettlementCleanEpoch || !book.SettlementLegacyExcluded ||
		len(book.SettlementLegacyArchiveSHA) != 64 {
		t.Fatalf("trimmed ledger did not enter a clean collectible epoch: poisoned=%v book=%+v",
			s.gfBookPoisoned.Load(), book)
	}
	var archives int
	if err := s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM polyus_funded_json_epoch_quarantine
WHERE ledger_name='genfollow_book.json' AND source_sha256=?`,
		book.SettlementLegacyArchiveSHA).Scan(&archives); err != nil || archives != 1 {
		t.Fatalf("immutable clean-epoch archive count=%d err=%v", archives, err)
	}
	archivePath := filepath.Join(s.cfg().DataDir, "genfollow_book.json") +
		".r148-pre-clean-epoch-" + book.SettlementLegacyArchiveSHA[:12] + ".json"
	raw, err := os.ReadFile(archivePath)
	if err != nil || !strings.Contains(string(raw), `"wins":3`) || !strings.Contains(string(raw), `"losses":2`) {
		t.Fatalf("operator archive missing original aggregates err=%v raw=%s", err, raw)
	}
	if err := s.repairR148PolyUSFundedJSONBooks(ctx); err != nil {
		t.Fatal(err)
	}
	var archivesAfter int
	_ = s.store.DBForTest().QueryRow(`SELECT COUNT(*) FROM polyus_funded_json_epoch_quarantine
WHERE ledger_name='genfollow_book.json'`).Scan(&archivesAfter)
	if archivesAfter != archives || s.gfBookPoisoned.Load() {
		t.Fatalf("clean epoch replay was not idempotent: archives=%d->%d poisoned=%v",
			archives, archivesAfter, s.gfBookPoisoned.Load())
	}
	// A healthy post-R148 epoch will eventually rotate its own 300-row tail. That must not be
	// mistaken for legacy contamination and reset a second time.
	book.Closed = []kfClosed{r148ClosedFixture("R148-NEW-FINAL", 1, time.Now().UTC())}
	book.Net, book.Wins, book.Losses = 12.50, 301, 0
	if err := s.repairR148PolyUSFundedJSONBooks(ctx); err != nil {
		t.Fatal(err)
	}
	if book.Net != 12.50 || book.Wins != 301 || len(book.Closed) != 1 || s.gfBookPoisoned.Load() {
		t.Fatalf("post-clean-epoch tail rotation triggered a second reset: %+v", book)
	}
}
