package server

// r129_test.go — pins for the R129 pass: the C4 EV-per-capital-day ranking multiplier (med-high),
// the Cheapband book (R126 band-study bettor: gates, settle, verdict fold, hedge guard), and the
// LockStack staked-lock expression (verdict fold + Combos-book exposure/net math).

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// TestR129EvDayRankMult — the documented C4 curve: (6h / max(lh, 0.5h))^exp, off at exp≤0,
// 1× at the 6h gate limit, monotone increasing as the horizon shrinks, floored at 0.5h.
func TestR129EvDayRankMult(t *testing.T) {
	if m := evDayRankMult(3, 0); m != 1 {
		t.Fatalf("exp=0 must be off (1×), got %v", m)
	}
	if m := evDayRankMult(0, 0.65); m != 1 {
		t.Fatalf("unknown horizon must be neutral (1×), got %v", m)
	}
	if m := evDayRankMult(6, 0.65); math.Abs(m-1) > 1e-9 {
		t.Fatalf("6h (the gate limit) must be exactly 1×, got %v", m)
	}
	want1h := math.Pow(6, 0.65) // ≈3.20 — the med-high headline number
	if m := evDayRankMult(1, 0.65); math.Abs(m-want1h) > 1e-9 {
		t.Fatalf("1h at exp .65 must be %v, got %v", want1h, m)
	}
	// 0.5h floor: anything below 30 min multiplies the same as 30 min.
	if a, b := evDayRankMult(0.5, 0.65), evDayRankMult(0.1, 0.65); math.Abs(a-b) > 1e-9 {
		t.Fatalf("sub-30min horizons must hit the floor: %v vs %v", a, b)
	}
	// Monotone: sooner always ranks ≥.
	last := 0.0
	for _, lh := range []float64{6, 4, 2, 1, 0.5} {
		m := evDayRankMult(lh, 0.65)
		if m < last {
			t.Fatalf("multiplier must grow as horizon shrinks: lh=%v gave %v < %v", lh, m, last)
		}
		last = m
	}
	// Clamp rail (only reachable if the 6h gate ever widens / exp is cranked).
	if m := evDayRankMult(0.5, 3); m != 8 {
		t.Fatalf("upper clamp must hold, got %v", m)
	}
}

// TestR129CheapbandFamilyPositive — the family gate: cache-only, signal families only, N≥100,
// mean>0, never venue-locked, cold/stale cache fails closed.
func TestR129CheapbandFamilyPositive(t *testing.T) {
	s := testServer(t)
	if s.cbFamilyPositive("xvgap", "kalshi") {
		t.Fatal("cold verdict cache must fail closed")
	}
	injectVerdicts(s, []verdictEnt{
		{Family: "goodfam", Group: "signal", Unit: "$/contract", N: 500, Mean: 0.03, Venues: map[string]venueVerdict{"kalshi": {N: 500, Mean: 0.03}, "polyus": {N: 500, Mean: -0.04}}},
		{Family: "thinfam", Group: "signal", Unit: "$/contract", N: 40, Mean: 0.09, Venues: map[string]venueVerdict{"kalshi": {N: 40, Mean: 0.09}}},
		{Family: "lockedfam", Group: "signal", Unit: "$/contract", N: 900, Mean: 0.05, Locked: true, Venues: map[string]venueVerdict{"kalshi": {N: 900, Mean: 0.05}}},
		{Family: "negfam", Group: "signal", Unit: "$/contract", N: 900, Mean: -0.02, Venues: map[string]venueVerdict{"kalshi": {N: 900, Mean: -0.02}}},
		{Family: "bookfam", Group: "book", Unit: "$/round", N: 900, Mean: 0.05, Venues: map[string]venueVerdict{"kalshi": {N: 900, Mean: 0.05}}},
	})
	for fam, want := range map[string]bool{
		"goodfam": true, "thinfam": false, "lockedfam": false,
		"negfam": false, "bookfam": false, "ghost": false,
	} {
		if got := s.cbFamilyPositive(fam, "kalshi"); got != want {
			t.Fatalf("cbFamilyPositive(%s) = %v, want %v", fam, got, want)
		}
	}
	if s.cbFamilyPositive("goodfam", "polyus") {
		t.Fatal("a positive pooled family must not qualify on its losing venue")
	}
	// stale cache fails closed
	s.verdMu.Lock()
	s.verdAt = time.Now().Add(-time.Hour)
	s.verdMu.Unlock()
	if s.cbFamilyPositive("goodfam", "kalshi") {
		t.Fatal("stale verdict cache must fail closed")
	}
}

func TestR132CheapbandCategoryGate(t *testing.T) {
	for _, tc := range []struct {
		venue, category string
		want            bool
	}{
		{"kalshi", "World Cup", false}, {"kalshi", "Baseball", true},
		{"polyus", "Soccer", false}, {"polyus", "Tennis", false},
		{"polyus", "", false}, {"polyus", "generic", false},
		{"polyus", "Esports", true}, {"polyus", "Basketball", true},
		{"polymarket", "Basketball", false},
	} {
		if got := cbCategoryOK(tc.venue, tc.category); got != tc.want {
			t.Fatalf("cbCategoryOK(%q,%q)=%v, want %v", tc.venue, tc.category, got, tc.want)
		}
	}
}

// TestR129CheapbandGates — band/venue/crypto/knob refusals happen before any book mutation.
func TestR129CheapbandGates(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	injectVerdicts(s, []verdictEnt{
		{Family: "goodfam", Group: "signal", Unit: "$/contract", N: 500, Mean: 0.03,
			Venues: map[string]venueVerdict{"kalshi": {N: 500, Mean: 0.03}, "polyus": {N: 500, Mean: 0.03}}}})
	lots := func() int {
		s.cbBookMu.Lock()
		defer s.cbBookMu.Unlock()
		b := s.cbLoadLocked()
		return len(b.K.Open) + len(b.P.Open)
	}
	try := func(plat, ticker string, px float64, fam string) {
		s.cheapbandConsider(ctx, storage.Signal{Platform: plat, Ticker: ticker, Title: "t",
			Side: "YES", SignalType: fam, EntryPrice: px})
	}
	try("kalshi", "CB-BAND", 0.12, "goodfam")       // above the Kalshi 10¢ band
	try("kalshi", "CB-BAND", 0.004, "goodfam")      // below the 1¢ floor
	try("polyus", "cb-band", 0.16, "goodfam")       // above the PolyUS 15¢ band
	try("polymarket", "cb-pint", 0.05, "goodfam")   // poly-int excluded by name
	try("kalshi", "KXBTC15M-TEST", 0.05, "goodfam") // crypto gated out (UNPROVEN cell)
	try("kalshi", "CB-FAM", 0.05, "negfam")         // family not measured-positive
	if n := lots(); n != 0 {
		t.Fatalf("every gate case must refuse, book has %d lots", n)
	}
	off := false
	s.mutateCfg(func(c *config.Config) { c.Auto.CheapbandEnabled = &off })
	try("kalshi", "CB-KNOB", 0.05, "goodfam")
	if n := lots(); n != 0 {
		t.Fatalf("knob-off must refuse, book has %d lots", n)
	}
}

// TestR165CheapbandLegacyPaperSettlementStaysOutOfProfitVerdicts keeps settlement and hedge
// behavior covered while proving hand-built legacy Paper lots cannot become money evidence.
func TestR165CheapbandLegacyPaperSettlementStaysOutOfProfitVerdicts(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)
	s.cbBookMu.Lock()
	b := s.cbLoadLocked()
	b.K.Open = append(b.K.Open, kfPos{TS: now, Ticker: "CBS-K", Title: "t", Side: "YES",
		Price: 0.06, Contracts: 20, Fee: 0.05, Platform: "kalshi"})
	b.P.Open = append(b.P.Open, kfPos{TS: now, Ticker: "cbs-p", Title: "t", Side: "NO",
		Price: 0.10, Contracts: 10, Fee: 0.02, Platform: "polyus"})
	s.cbBookDirty = true
	s.cbBookMu.Unlock()
	// hedge guard: platform-scoped
	if side := s.cbBookOpenSide("kalshi", "CBS-K"); side != "YES" {
		t.Fatalf("kalshi half must expose YES, got %q", side)
	}
	if side := s.cbBookOpenSide("polyus", "CBS-K"); side != "" {
		t.Fatalf("polyus half must not expose the kalshi lot, got %q", side)
	}
	mustInsertResolvedSignal(t, s, "kalshi", "CBS-K", "YES", 0.06, 1) // YES wins
	mustInsertResolvedSignal(t, s, "polyus", "cbs-p", "NO", 0.90, 0)  // NO wins
	if inserted, err := s.store.RecordVenueSettlement(ctx, "polyus", "cbs-p", 0, time.Now(),
		storage.PolyUSFinalEndpointSettlementV2); err != nil || !inserted {
		t.Fatalf("trusted PolyUS terminal receipt inserted=%v err=%v", inserted, err)
	}
	s.settleCheapbandBook(ctx)
	s.cbBookMu.Lock()
	kw, pw := b.K.Wins, b.P.Wins
	ko, po := len(b.K.Open), len(b.P.Open)
	s.cbBookMu.Unlock()
	if kw != 1 || pw != 1 || ko != 0 || po != 0 {
		t.Fatalf("settle failed: K wins=%d open=%d · P wins=%d open=%d", kw, ko, pw, po)
	}
	s.verdMu.Lock()
	s.verdCache, s.verdAt = nil, time.Time{}
	s.verdMu.Unlock()
	found := map[string]bool{}
	for _, v := range s.computeExperimentVerdicts(ctx) {
		if (v.Family == "book:cheapband-k" || v.Family == "book:cheapband-p") && v.N == 1 {
			found[v.Family] = true
		}
	}
	if found["book:cheapband-k"] || found["book:cheapband-p"] {
		t.Fatalf("legacy cheapband Paper settlements entered money-facing verdicts: %v", found)
	}
}

// TestR129LockStackFoldAndBooks — a staked lock row grades as family "lockstack", spends the
// Combos book while open, and lands in the Combos display net when closed.
func TestR129LockStackFoldAndBooks(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.xvlMu.Lock()
	b := s.xvlLoadLocked()
	open := xvLockOpp{Key: "K-PUS|A|B|YESa+NOb", Pair: "K-PUS", AVenue: "kalshi", AID: "A", ASide: "YES",
		BVenue: "polyus", BID: "B", BSide: "NO", AAsk: 0.45, BAsk: 0.50, FeeA: 0.01, FeeB: 0.01,
		MarginC: 3.0, Staked: 5, AWon: -1, BWon: -1}
	open.ResolutionBasis = r139VerifiedLockCertificate(open.Pair, open.AVenue, open.AID,
		open.BVenue, open.BID, true, open.ASide, open.BSide)
	closedStack := xvLockOpp{Key: "K-PUS|C|D|YESa+NOb", Pair: "K-PUS", AVenue: "kalshi", AID: "C", ASide: "YES",
		BVenue: "polyus", BID: "D", BSide: "NO", AAsk: 0.40, BAsk: 0.53,
		RealizedC: 4.0, Staked: 2, AWon: 1, BWon: 0}
	closedStack.ResolutionBasis = r139VerifiedLockCertificate(closedStack.Pair, closedStack.AVenue,
		closedStack.AID, closedStack.BVenue, closedStack.BID, true, closedStack.ASide, closedStack.BSide)
	closedLog := xvLockOpp{Key: "K-PINT|E|F|YESa+NOb", Pair: "K-PINT", AVenue: "kalshi", AID: "E", ASide: "YES",
		BVenue: "polymarket", BID: "F", BSide: "NO", AAsk: 0.40, BAsk: 0.55,
		RealizedC: 2.0, AWon: 0, BWon: 1}
	closedLog.ResolutionBasis = r139VerifiedLockCertificate(closedLog.Pair, closedLog.AVenue,
		closedLog.AID, closedLog.BVenue, closedLog.BID, true, closedLog.ASide, closedLog.BSide)
	b.Open = append(b.Open, open)
	b.Closed = append(b.Closed, closedStack, closedLog) // log-only row: must NOT grade as lockstack
	s.xvlDirty = true
	s.xvlMu.Unlock()
	// verdict fold: lockstack n=1 (the staked closed row only), xvlock n=2 (everything)
	s.verdMu.Lock()
	s.verdCache, s.verdAt = nil, time.Time{}
	s.verdMu.Unlock()
	gotLock, gotStack := 0, 0
	for _, v := range s.computeExperimentVerdicts(ctx) {
		switch v.Family {
		case "xvlock":
			gotLock = v.N
		case "lockstack":
			gotStack = v.N
		}
	}
	if gotLock != 2 || gotStack != 1 {
		t.Fatalf("verdict fold wrong: xvlock n=%d (want 2), lockstack n=%d (want 1)", gotLock, gotStack)
	}
	// Combos exposure: the open staked row spends (asks+fees)×staked = (0.45+0.50+0.02)×5 = 4.85
	usd, lots, ok := s.bookOpenExposure(ctx, vbCombos)
	if !ok || math.Abs(usd-4.85) > 1e-9 || lots != 1 {
		t.Fatalf("combos exposure wrong: usd=%v lots=%d ok=%v (want 4.85/1/true)", usd, lots, ok)
	}
	// Combos display net: closed staked realized = 4¢ × 2 = $0.08 (log-only rows contribute 0)
	net, ok := s.bookAllTimeNet(ctx, vbCombos)
	if !ok || math.Abs(net-0.08) > 1e-9 {
		t.Fatalf("combos net wrong: %v ok=%v (want 0.08)", net, ok)
	}
}
