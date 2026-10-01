package server

// r127_test.go — ship-gate pins for the R127 FOUR-BOOK RESTRUCTURE (operator's exact orders):
//   - venue-book mapping + available-bank math (bank − Σ open exposure of ALL subs; fail-closed)
//   - the one-time transfer journal (kv latch r127_books_v1) writes exactly once
//   - D4 demotion-to-watch placement gate (WATCH pauses freshinv/xvgap placement)
//   - briefing: four compact portfolio lines + "promoted:"; no "[VL]", no
//     "since last", no "gap-book:"; the 🎯 line is a net-EV line; the parlay label's new phrasing
//   - league table: 🔒 replaces [VL]; 🔄 marks inverts; PROVEN− folds to the inverted expression
//   - combo expression tags: product-sim/synthetic/rfq grade under their own verdict families,
//     and the synthetic leg-stack settles at the SUM of leg payouts

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func fEq(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

// TestR127VenueBookAvailableMath — the operator's formula: available = fixed bank − Σ open
// exposure of ALL subs in the venue book (fresh-listing lots follow their actual venue).
func TestR127VenueBookAvailableMath(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// Shared auto opens: kalshi 10 × 50¢ = $5 · polyus 20 × 30¢ = $6 (CostBasis excludes fees).
	for _, f := range []paper.Fill{
		{Platform: "kalshi", Ticker: "KXR127A", Title: "A", Side: "YES", Action: "BUY", Price: 0.50, Contracts: 10, Fee: 0.05, Source: "auto-ml"},
		{Platform: "polyus", Ticker: "r127-slug", Title: "B", Side: "YES", Action: "BUY", Price: 0.30, Contracts: 20, Fee: 0.02, Source: "auto-ml"},
	} {
		if _, err := s.store.InsertPaperFill(ctx, f); err != nil {
			t.Fatalf("insert fill: %v", err)
		}
	}
	// Sub-book lots (contracts×price + fee counts — the R122/455 rule).
	s.kfBookMu.Lock()
	s.kfLoadLocked().Pre.Open = append(s.kfLoadLocked().Pre.Open, kfPos{Ticker: "KF1", Price: 0.40, Contracts: 10, Fee: 0.10}) // 4.10
	s.kfBookMu.Unlock()
	s.wxBookMu.Lock()
	s.wxBookLoadLocked().Open = append(s.wxBookLoadLocked().Open, kfPos{Ticker: "WX1", Price: 0.25, Contracts: 8, Fee: 0.05}) // 2.05
	s.wxBookMu.Unlock()
	s.fiBookMu.Lock()
	// The shared fresh-listing ledger holds a POLYUS lot — R132 attributes it to PolyUS.
	s.fiLoadLocked().Open = append(s.fiLoadLocked().Open, kfPos{Ticker: "fi-pus", Platform: "polyus", Price: 0.50, Contracts: 4, Fee: 0.02}) // 2.02
	s.fiBookMu.Unlock()
	s.xvgBookMu.Lock()
	s.xvgLoadLocked().Open = append(s.xvgLoadLocked().Open, kfPos{Ticker: "xg-pus", Platform: "polyus", Price: 0.30, Contracts: 10}) // 3.00
	s.xvgBookMu.Unlock()
	s.xvcMu.Lock()
	s.xvcLoadLocked().Open = append(s.xvcLoadLocked().Open,
		xvcPos{LegA: "KXC1", LegB: "KXC2", Cost: 1.10, Contracts: 2, Fee: 0.05, Expr: xvcExprSynth},   // 2.25 — money lot
		xvcPos{LegA: "KXC3", LegB: "KXC4", Cost: 0.30, Contracts: 3, Fee: 0.02, Expr: xvcExprProduct}) // study lot — NOT counted
	s.xvcMu.Unlock()

	if bank := s.bookBankUSD(vbKalshi); !fEq(bank, 600) {
		t.Fatalf("kalshi portfolio bank = %v, want the $600 default", bank)
	}
	if bank := s.bookBankUSD(vbML); !fEq(bank, 600) {
		t.Fatalf("ml book bank = %v, want the $600 default", bank)
	}
	exp, lots, ok := s.bookOpenExposure(ctx, vbKalshi)
	if !ok || !fEq(exp, 5+4.10+2.05) || lots != 3 {
		t.Fatalf("kalshi exposure = %v (%d lots, ok=%v), want 11.15 across 3 lots", exp, lots, ok)
	}
	if avail := s.bookAvailableUSD(ctx, vbKalshi); !fEq(avail, 600-11.15) {
		t.Fatalf("kalshi available = %v, want 988.85", avail)
	}
	if avail := s.bookAvailableUSD(ctx, vbPolyus); !fEq(avail, 600-11.02) {
		t.Fatalf("polyus available = %v, want 988.98 (auto $6 + xvgap $3 + fresh $2.02)", avail)
	}
	if avail := s.bookAvailableUSD(ctx, vbCombos); !fEq(avail, 600-2.25) {
		t.Fatalf("combos available = %v, want 997.75 (synthetic lot only — product-sim is the log-only study)", avail)
	}
	// Fail-closed: a blind shared-fills read (no cache) must yield available 0, never a
	// fabricated full bank (paper over-placement is the failure mode).
	s2 := testServer(t)
	s2.fillsCacheTTL = -1
	s2.fillsReadFn = func(context.Context) ([]paper.Fill, error) { return nil, errors.New("db locked") }
	if avail := s2.bookAvailableUSD(ctx, vbKalshi); avail != 0 {
		t.Fatalf("blind exposure read must fail CLOSED (available 0), got %v", avail)
	}
}

// TestR127TransferJournalOnce — the kv-latched migration journals exactly once.
func TestR127TransferJournalOnce(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	path := filepath.Join(s.cfg().DataDir, "r127_book_transfer.json")
	s.migrateBooksR127(ctx)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("first run must write the transfer journal: %v", err)
	}
	if v, ok := s.store.KVGet(ctx, r127MigLatch); !ok || v == "" {
		t.Fatalf("first run must set the kv latch")
	}
	// Latched: a second run must not rewrite the journal (delete it and prove it stays gone).
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.migrateBooksR127(ctx)
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("latched migration must never write again")
	}
}

// R165 voids all legacy promotion states as execution authority. The records remain readable for
// history, but no saved WATCH/PROMOTED/GROWN label may start a new Paper lot.
func TestR165LegacyPromotionStatesCannotActivatePaperExecutors(t *testing.T) {
	s := testServer(t)
	setState := func(fam, state string) {
		s.promoMu.Lock()
		st := s.promoLoadLocked()
		st.Records[fam] = &promoRecord{Family: fam, State: state, AllocUSD: 100}
		s.promoMu.Unlock()
	}
	setState("invert:freshlist", promoStateWatchD4)
	setState("xvgap", promoStateWatchD4)
	if s.freshInvActive() || s.xvgActive() {
		t.Fatalf("WATCH revived legacy Paper authority (freshinv=%v xvgap=%v)", s.freshInvActive(), s.xvgActive())
	}
	setState("invert:freshlist", promoStatePromoted)
	setState("xvgap", promoStateGrown)
	if s.freshInvActive() || s.xvgActive() {
		t.Fatalf("PROMOTED/GROWN revived legacy Paper authority (freshinv=%v xvgap=%v)", s.freshInvActive(), s.xvgActive())
	}
	setState("invert:freshlist", promoStateRetired)
	setState("xvgap", promoStateRetired)
	if s.freshInvActive() || s.xvgActive() {
		t.Fatalf("RETIRED records keep the gate off (freshinv=%v xvgap=%v)", s.freshInvActive(), s.xvgActive())
	}
}

// injectVerdicts primes the 60s verdict cache so briefing surfaces render deterministic rows.
func injectVerdicts(s *Server, vs []verdictEnt) {
	s.verdMu.Lock()
	s.verdCache, s.verdAt = vs, time.Now()
	s.verdMu.Unlock()
}

// TestR127BriefingFourBooks — punch-list A/C/D/F on the full BriefingText: four book headers with
// all-time nets + rosters, the active-system line, the net-EV 🎯 line, the new parlay phrasing,
// and the banned strings gone.
func TestR127BriefingFourBooks(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	injectVerdicts(s, []verdictEnt{
		{Family: "kflow-pre", Group: "book", Unit: "$/round", N: 210, Mean: 0.123, State: "COLLECTING"},
		{Family: "book:xvgap", Group: "book", Unit: "$/round", N: 64, Mean: 0.342, State: "PROVEN+"},
		{Family: "combo-synth", Group: "book", Unit: "$/combo-ct", N: 4, Mean: -0.01, State: "COLLECTING"},
		{Family: "ml-book", Group: "book", Unit: "$/contract", N: 2100, Mean: 0.011, State: "PROVEN+"},
	})
	s.promoMu.Lock()
	st := s.promoLoadLocked()
	st.Records["invert:freshlist"] = &promoRecord{Family: "invert:freshlist", State: promoStateWatchD4, AllocUSD: 100}
	st.Records["xvgap"] = &promoRecord{Family: "xvgap", State: promoStatePromoted, AllocUSD: 100}
	s.promoMu.Unlock()
	writeBookJSON(t, s.cfg().DataDir, "ml_paper.json", map[string]any{
		"bank0": 1000.0, "epoch_id": "r127-v2", "open": []any{},
		"closed": []any{
			map[string]any{"platform": "kalshi", "model_cohort": currentMLCohort, "epoch_id": "r127-v2", "pnl": 5.0, "contracts": 10.0, "closed_ts": time.Now().Unix()},
			map[string]any{"platform": "polyus", "model_cohort": currentMLCohort, "epoch_id": "r127-v2", "pnl": -2.0, "contracts": 10.0, "closed_ts": time.Now().Unix()},
		},
		"stats":    map[string]any{"net": 3.0, "equity": 1003.0, "win_rate": 0.5, "closed": 2, "open_n": 7},
		"lifetime": map[string]any{"net": 3.0, "closed": 2.0, "wins": 1.0, "net_base": 0.0, "bank_ver": 3.0},
	})
	s.plabMu.Lock()
	s.plabCand = 3 // renders the parlay-lab line
	s.plabMu.Unlock()
	txt := s.BriefingText(ctx)
	for _, want := range []string{
		"⚪ Kalshi +$0.00 · n/a/bet · n/a¢/u 🅾️0©️0", "⚪ PolyUS +$0.00 · n/a/bet · n/a¢/u 🅾️0©️0",
		"⚪ Combos +$0.00 · n/a/bet · n/a¢/u 🅾️0©️0", "🟢 ML +$3.00 · +$1.50/bet · +15.0¢/u 🅾️0©️2",
		"🚀 Portfolio systems · 0 positive authenticated exchange-profit routes · avg n/a",
		// These historical fixtures have no fee-net ev_net field. The briefing must not silently
		// substitute gross/missing prediction truth with a fake +0.0¢ forecast.
		"Paper · 2 settled · +$3.00 · +15.0¢/share · predicted n/a · open 0", // $3 over 20 current-v2 contracts
	} {
		if !strings.Contains(txt, want) {
			t.Fatalf("briefing missing %q:\n%s", want, txt)
		}
	}
	for _, banned := range []string{"[VL]", "since last", "gap-book:", "$333", "· kalshi auto", "· ML book", "mini: off", "📚 portfolios", "n=🅾️", "¢/unit", "Paper allocations", "promoted to portfolios right now", "promoted = receives allocation only", "why:", "629→0", "🏁 systems ("} {
		if strings.Contains(txt, banned) {
			t.Fatalf("briefing must not contain %q anymore:\n%s", banned, txt)
		}
	}
}

// TestR127LeagueTableInvertFold — R144: real executable inverse systems are independent rows. Only
// diagnostic counterfactuals fold; the original negative system remains visible beside its inverse.
func TestR127LeagueTableInvertFold(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	injectVerdicts(s, []verdictEnt{
		{Family: "taker:freshlist@kalshi", SourceFamily: "freshlist", Platform: "kalshi", Side: "YES", Route: "taker", OriginLayer: "model", Group: "taker", Unit: "$/contract", N: 100, Markets: 100, ContractMarkets: 100, Mean: -0.12, Lo: -.15, Hi: -.09, State: "PROVEN-", Invert: 0.05, ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{Family: "taker:invert:freshlist@kalshi", SourceFamily: "invert:freshlist", Platform: "kalshi", Side: "NO", Route: "taker", OriginLayer: "model", Group: "taker", Unit: "$/contract", N: 100, Markets: 100, ContractMarkets: 100, Mean: 0.05, Lo: .02, Hi: .08, State: "COLLECTING", ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
		{Family: "taker:invert:meanrev@polyus", SourceFamily: "invert:meanrev", Platform: "polyus", Side: "YES", Route: "taker", OriginLayer: "model", Group: "taker", Unit: "$/contract", N: 30, Markets: 30, ContractMarkets: 30, Mean: 0.01, Lo: -.02, Hi: .04, State: "COLLECTING", ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
	})
	txt := s.briefScoreboard(ctx)
	if !strings.Contains(txt, "fade of fresh listings · $/d warming · +5.0¢/s · n100") || !strings.Contains(txt, "K/N/T") {
		t.Fatalf("real inverse must appear as its own economic row:\n%s", txt)
	}
	if !strings.Contains(txt, "fresh listings · $/d warming · -12.0¢/s · n100") || !strings.Contains(txt, "K/Y/T") {
		t.Fatalf("original negative system must remain independently visible:\n%s", txt)
	}
	if !strings.Contains(txt, "fade of mean reversion · $/d warming · +1.0¢/s · n30") || !strings.Contains(txt, "PUS/Y/T") {
		t.Fatalf("standalone inverse keeps its independent exact execution identity:\n%s", txt)
	}
	if strings.Contains(txt, "gap-book:") {
		t.Fatalf("the gap-book line must be gone:\n%s", txt)
	}
}

// TestR127ComboExprFamilies — expression tags grade under their own verdict families.
func TestR127ComboExprFamilies(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.xvcMu.Lock()
	b := s.xvcLoadLocked()
	b.Closed = []xvcClosed{
		{xvcPos: xvcPos{Contracts: 2, Cost: 1.10, Expr: xvcExprSynth}, PnL: 0.50},
		{xvcPos: xvcPos{Contracts: 1, Cost: 0.30}, PnL: -0.20}, // legacy row = product-sim
		{xvcPos: xvcPos{Contracts: 1, Cost: 0.25, Expr: xvcExprProduct}, PnL: 0.10},
		{xvcPos: xvcPos{Contracts: 1, Cost: 0.90, Expr: xvcExprRFQ}, PnL: 0.05},
	}
	s.xvcMu.Unlock()
	byFam := map[string]verdictEnt{}
	for _, v := range s.computeExperimentVerdicts(ctx) {
		byFam[v.Family] = v
	}
	if v := byFam["combo-overlay"]; v.N != 1 {
		t.Fatalf("combo-overlay must count the duplicate-leg product rows as one independent market: n=%d", v.N)
	}
	if v := byFam["combo-synth"]; v.N != 1 {
		t.Fatalf("combo-synth must grade the synthetic leg-stack rows: n=%d", v.N)
	}
	if v := byFam["combo-rfq"]; v.N != 1 {
		t.Fatalf("combo-rfq must grade the venue-quote rows: n=%d", v.N)
	}
}

// TestR127SyntheticSettleMath — the leg-stack settles at the SUM of leg payouts (two real singles
// graded as one unit); product-sim keeps the all-or-nothing product grading.
func TestR127SyntheticSettleMath(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	for _, sig := range []struct {
		tk string
		yv float64
	}{{"KXR127WIN", 1}, {"KXR127LOSE", 0}} {
		if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: sig.tk, Title: "T " + sig.tk,
			Side: "YES", SignalType: "xvgap", EntryPrice: 0.5}); err != nil {
			t.Fatalf("insert signal: %v", err)
		}
		if err := s.store.ResolveSignals(ctx, sig.tk, sig.yv); err != nil {
			t.Fatalf("resolve: %v", err)
		}
	}
	s.xvcMu.Lock()
	b := s.xvcLoadLocked()
	b.Open = []xvcPos{
		{TS: "2026-07-09T00:00:00Z", LegA: "KXR127WIN", SideA: "YES", PxA: 0.5, LegB: "KXR127LOSE", SideB: "YES", PxB: 0.6,
			Cost: 1.10, Contracts: 2, Fee: 0.10, Mode: "combo_overlay", Expr: xvcExprSynth},
		{TS: "2026-07-09T00:00:00Z", LegA: "KXR127WIN", SideA: "YES", PxA: 0.5, LegB: "KXR127LOSE", SideB: "YES", PxB: 0.6,
			Cost: 0.30, Contracts: 1, Fee: 0.02, Mode: "combo_overlay", Expr: xvcExprProduct},
	}
	s.xvcMu.Unlock()
	s.settleXvComboBook(ctx)
	s.xvcMu.Lock()
	defer s.xvcMu.Unlock()
	b = s.xvcLoadLocked()
	if len(b.Open) != 0 || len(b.Closed) != 2 {
		t.Fatalf("both lots must settle (open=%d closed=%d)", len(b.Open), len(b.Closed))
	}
	for _, c := range b.Closed {
		switch c.Expr {
		case xvcExprSynth:
			// payout = 1 + 0 = 1 → pnl = 2×(1 − 1.10) − 0.10 = −0.30
			if !fEq(c.Payout, 1.0) || !fEq(c.PnL, -0.30) || c.Won {
				t.Fatalf("synthetic settle wrong: payout=%v pnl=%v won=%v (want 1.0 / −0.30 / false)", c.Payout, c.PnL, c.Won)
			}
		case xvcExprProduct:
			// payout = 1 × 0 = 0 → pnl = 1×(0 − 0.30) − 0.02 = −0.32
			if !fEq(c.Payout, 0.0) || !fEq(c.PnL, -0.32) || c.Won {
				t.Fatalf("product-sim settle wrong: payout=%v pnl=%v won=%v (want 0 / −0.32 / false)", c.Payout, c.PnL, c.Won)
			}
		}
	}
	// The rfq quote hook stays a not-found stub until the venue ever shows resting combo quotes.
	if _, ok := s.xvcVenueQuote("KXR127WIN", "KXR127LOSE"); ok {
		t.Fatalf("xvcVenueQuote must report not-found (R126 measured zero resting quotes)")
	}
}
