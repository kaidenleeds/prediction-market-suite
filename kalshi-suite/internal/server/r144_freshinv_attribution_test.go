package server

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r144FreshBookSignal(platform, ticker, side, family string, ask, bid, depth float64) storage.Signal {
	makerDepth, quoteAge, tick, makerFee, takerFee := depth+2, .2, .01, 0.0, .01
	return storage.Signal{Platform: platform, Ticker: ticker, Title: "R144 fresh market", Side: side,
		SignalType: family, EntryPrice: ask, SpreadCents: (ask - bid) * 100, BookDepth: depth,
		ResolveHours: 1, BookFeatureVer: mlBookFeatureVersion, BookBid: &bid, BookAsk: &ask,
		BookBidDepth: &makerDepth, BookAskDepth: &depth, BookQuoteAgeS: &quoteAge,
		BookMakerTick: &tick, BookTakerTick: &tick, BookMakerFeePC: &makerFee,
		BookTakerFeePC: &takerFee, BookSource: platform + "-ws-depth", PricingVersion: mlBookFeatureSchema}
}

func r144OpenFreshLot(t *testing.T, s *Server, sig storage.Signal, lot kfPos) {
	t.Helper()
	ctx := context.Background()
	if err := s.prepareFreshInvSignalReceipt(ctx, sig, lot); err != nil {
		t.Fatalf("prepare %s: %v", fiLotFamily(lot), err)
	}
	s.fiBookMu.Lock()
	s.fiLoadLocked().Open = append(s.fiLoadLocked().Open, lot)
	s.fiBookDirty = true
	s.fiBookMu.Unlock()
	if err := s.freshInvFlushChecked(); err != nil {
		t.Fatalf("persist %s lot: %v", fiLotFamily(lot), err)
	}
	if err := s.stampFreshInvSignalFill(ctx, lot, false); err != nil {
		t.Fatalf("stamp %s fill: %v", fiLotFamily(lot), err)
	}
}

func TestR144FreshInvExactFamilyFillPathAndSettlement(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	kLot := kfPos{TS: now, SignalTS: now, DecisionTS: now, FillTS: now, Platform: "kalshi",
		Ticker: "KXR144-FRESH", Title: "Kalshi fresh", Side: "NO", Price: .42, Contracts: 3, Fee: .03,
		FeeKnown: true, FeeSource: "kalshi:test-exact", FillKind: "taker", FillRule: "complete-book-touch",
		RouteReason: "fam:invert:freshlist origin=strategy route=taker book=kalshi-ws-depth"}
	pLot := kfPos{TS: now, SignalTS: now, DecisionTS: now, FillTS: now, Platform: "polyus",
		Ticker: "r144-poly-fresh", Title: "PolyUS fresh", Side: "YES", Price: .38, Contracts: 2, Fee: .02,
		FeeKnown: true, FeeSource: "polyus:test-exact", FillKind: "taker", FillRule: "complete-book-touch",
		RouteReason: "fam:freshlist origin=strategy route=taker book=polyus-ws-full"}

	r144OpenFreshLot(t, s, r144FreshBookSignal("kalshi", kLot.Ticker, "NO", "invert:freshlist", .42, .39, 8), kLot)
	r144OpenFreshLot(t, s, r144FreshBookSignal("polyus", pLot.Ticker, "YES", "freshlist", .38, .35, 7), pLot)
	// Same-side decoys are the historical ambiguity this regression closes. Neither may inherit
	// the specialist's fill, path, CLV or settlement receipt.
	for _, decoy := range []storage.Signal{
		{Platform: "kalshi", Ticker: kLot.Ticker, Title: "decoy", Side: "NO", SignalType: "freshfade", EntryPrice: .42},
		{Platform: "polyus", Ticker: pLot.Ticker, Title: "decoy", Side: "YES", SignalType: "invert:freshfade", EntryPrice: .38},
	} {
		if _, err := s.store.InsertSignalResult(ctx, decoy); err != nil {
			t.Fatal(err)
		}
	}

	// One unresolved book fold records exact-family post-entry paths from each venue cache.
	s.topMarksMu.Lock()
	s.topMarks = map[string]float64{kLot.Ticker: .55} // Kalshi NO mark = .45
	s.topMarksAt = time.Now()
	s.topMarksMu.Unlock()
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{{Slug: pLot.Ticker, Yes: .44}}
	s.polyUSAt = time.Now()
	s.polyUSMu.Unlock()
	s.settleFreshInvBook(ctx)

	assertRow := func(platform, ticker, side, family string, wantFill float64, wantPath bool) {
		t.Helper()
		var fill, fee float64
		var path string
		if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT fill_price,COALESCE(fee_pc,0),post_path FROM signal_log
WHERE platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=? ORDER BY id DESC LIMIT 1`,
			platform, ticker, side, family).Scan(&fill, &fee, &path); err != nil {
			t.Fatalf("row %s: %v", family, err)
		}
		if math.Abs(fill-wantFill) > 1e-9 || (wantPath != (path != "")) {
			t.Fatalf("row %s fill=%v path=%q, want fill=%v path=%v", family, fill, path, wantFill, wantPath)
		}
		if wantFill > 0 && math.Abs(fee-.01) > 1e-9 {
			t.Fatalf("row %s sized fee/contract=%v, want .01", family, fee)
		}
	}
	assertRow("kalshi", kLot.Ticker, "NO", "invert:freshlist", .42, true)
	assertRow("polyus", pLot.Ticker, "YES", "freshlist", .38, true)
	assertRow("kalshi", kLot.Ticker, "NO", "freshfade", 0, false)
	assertRow("polyus", pLot.Ticker, "YES", "invert:freshfade", 0, false)

	if err := s.store.ResolveSignals(ctx, kLot.Ticker, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.store.ResolveSignals(ctx, pLot.Ticker, 1); err != nil {
		t.Fatal(err)
	}
	if inserted, err := s.store.RecordVenueSettlement(ctx, "polyus", pLot.Ticker, 1, time.Now(),
		storage.PolyUSFinalEndpointSettlementV2); err != nil || !inserted {
		t.Fatalf("trusted PolyUS terminal receipt inserted=%v err=%v", inserted, err)
	}
	s.settleFreshInvBook(ctx)
	s.fiBookMu.Lock()
	b := s.fiLoadLocked()
	if len(b.Open) != 0 || len(b.Closed) != 2 || fiLotFamily(b.Closed[0].kfPos) != "invert:freshlist" ||
		fiLotFamily(b.Closed[1].kfPos) != "freshlist" {
		s.fiBookMu.Unlock()
		t.Fatalf("closed specialist identities: open=%d closed=%+v", len(b.Open), b.Closed)
	}
	s.fiBookMu.Unlock()

	audits, err := s.store.ListAudit(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	seenK, seenP := false, false
	for _, row := range audits {
		if !strings.HasPrefix(row.Message, "CLOSED ") {
			continue
		}
		var detail map[string]any
		if json.Unmarshal([]byte(row.Detail), &detail) != nil {
			continue
		}
		switch detail["system"] {
		case "invert:freshlist":
			seenK = row.Category == "inverse-system" && detail["venue"] == "kalshi" && math.Abs(detail["clv"].(float64)-.03) < 1e-9
		case "freshlist":
			seenP = row.Category == "system-execution" && detail["venue"] == "polyus" && math.Abs(detail["clv"].(float64)-.06) < 1e-9
		}
	}
	if !seenK || !seenP {
		t.Fatalf("exact close receipts missing: kalshi=%v polyus=%v audits=%+v", seenK, seenP, audits)
	}
}

func TestR144FreshInvCrashRepairCannotBorrowLifecycleReopenRow(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR144-REOPEN", Title: "old lifecycle",
		Side: "NO", SignalType: "invert:freshlist", EntryPrice: .42}
	if _, err := s.store.InsertSignalResult(ctx, sig); err != nil {
		t.Fatal(err)
	}
	old := time.Now().UTC().Add(-24 * time.Hour)
	if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE signal_log
SET ts=?,day=?,slot=?,resolved=1,fill_price=.42 WHERE ticker=?`, old.Format(time.RFC3339),
		old.Format("2006-01-02"), old.Format(time.RFC3339)[:15], sig.Ticker); err != nil {
		t.Fatal(err)
	}
	// A newer unrelated observation is also ineligible: its timestamp is outside this lot's signal
	// episode, so the bounded updater may touch neither the resolved old lifecycle nor this row.
	if _, err := s.store.InsertSignalResult(ctx, sig); err != nil {
		t.Fatal(err)
	}
	newer := time.Now().UTC().Add(time.Hour)
	if _, err := s.store.DBForTest().ExecContext(ctx, `UPDATE signal_log SET ts=?
WHERE ticker=? AND resolved=0`, newer.Format(time.RFC3339), sig.Ticker); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	lot := kfPos{TS: now, SignalTS: now, FillTS: now, Platform: "kalshi", Ticker: sig.Ticker,
		Side: "NO", Price: .42, Contracts: 1, Fee: .01, FillKind: "taker",
		RouteReason: "fam:invert:freshlist origin=strategy route=taker"}
	if err := s.ensureFreshInvLotAttribution(ctx, lot); err == nil {
		t.Fatal("lifecycle-reopen lot borrowed an unrelated signal row")
	}
	rows, err := s.store.DBForTest().QueryContext(ctx, `SELECT resolved,fill_price FROM signal_log WHERE ticker=? ORDER BY id`, sig.Ticker)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got [][2]float64
	for rows.Next() {
		var resolved int
		var fill float64
		if err := rows.Scan(&resolved, &fill); err != nil {
			t.Fatal(err)
		}
		got = append(got, [2]float64{float64(resolved), fill})
	}
	if len(got) != 2 || got[0] != [2]float64{1, .42} || got[1] != [2]float64{0, 0} {
		t.Fatalf("unrelated lifecycle rows were mutated: %+v", got)
	}
}

func TestR144FreshInvSameSlotDuplicateRefreshesExactBookReceipt(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	lot := kfPos{TS: now, SignalTS: now, FillTS: now, Platform: "kalshi", Ticker: "KXR144-REFRESH",
		Side: "NO", Price: .42, Contracts: 2, Fee: .02, FillKind: "taker",
		RouteReason: "fam:invert:freshlist origin=strategy route=taker"}
	first := r144FreshBookSignal("kalshi", lot.Ticker, lot.Side, "invert:freshlist", .42, .39, 3)
	if err := s.prepareFreshInvSignalReceipt(ctx, first, lot); err != nil {
		t.Fatal(err)
	}
	// The same-slot collector row is not blindly reused. A new complete decision snapshot replaces
	// all executable price/depth fields and its timestamp is rebound to this lot's decision.
	lot.Price, lot.Contracts, lot.Fee = .44, 4, .08
	lot.SignalTS = time.Now().UTC().Format(time.RFC3339)
	second := r144FreshBookSignal("kalshi", lot.Ticker, lot.Side, "invert:freshlist", .44, .40, 9)
	if err := s.prepareFreshInvSignalReceipt(ctx, second, lot); err != nil {
		t.Fatal(err)
	}
	var entry, bid, ask, depth float64
	var ts, source string
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT ts,entry_price,book_bid,book_ask,
book_ask_depth,book_source FROM signal_log WHERE ticker=? AND signal_type='invert:freshlist'`, lot.Ticker).
		Scan(&ts, &entry, &bid, &ask, &depth, &source); err != nil {
		t.Fatal(err)
	}
	if entry != .44 || bid != .40 || ask != .44 || depth != 9 || source != "kalshi-ws-depth" {
		t.Fatalf("same-slot row retained stale book receipt: entry=%v bid=%v ask=%v depth=%v source=%q", entry, bid, ask, depth, source)
	}
	rowTS, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil || math.Abs(rowTS.Sub(time.Now()).Seconds()) > 5 {
		t.Fatalf("same-slot receipt timestamp was not rebound to decision: ts=%q err=%v", ts, err)
	}
	if err := s.stampFreshInvSignalFill(ctx, lot, false); err != nil {
		t.Fatalf("refreshed exact row did not accept its own fill: %v", err)
	}
}
