package server

// R128 pins — unlimited combo ledger (SQLite, ticker-centric settle-sweep), the scoreboard-weight
// allocation engine (no-pause doctrine), variance-shrunk Kelly, favlong80, and the px-offline
// gate coverage added to the sub-book placement paths.

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// r128FreshFeeds stamps fresh venue-feed ages so hermetic placement tests pass the R128
// px-offline fail-closed gate (testServer has no live feeds — the provably-dead shape).
func r128FreshFeeds(s *Server, kalshiTickers ...string) {
	s.metaMu.Lock()
	if s.kmktsAt == nil {
		s.kmktsAt = map[string]time.Time{}
	}
	for _, tk := range kalshiTickers {
		s.kmktsAt[tk] = time.Now()
	}
	s.metaMu.Unlock()
	s.polyUSMu.Lock()
	s.polyUSAt = time.Now()
	s.polyUSMu.Unlock()
}

// TestR128PlabUnlimitedLedgerFlow — insert a batch beyond the OLD 8k-cap semantics is moot now;
// pin the new mechanics: batch insert is idempotent, the ticker-centric sweep fans a settlement
// out to every combo holding the leg, unresolved counts hit 0, gradables return, deletes clean up.
func TestR128PlabUnlimitedLedgerFlow(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	mk := func(id string, legs []plabLeg) storage.PlabCand {
		return plabToRow(plabOpenEnt{ID: id, At: time.Now().Add(-time.Hour), Legs: legs,
			Bucket: "indep", Class: "ind:2leg", Prod: 0.25, JointP: 0.3, FeeMVE: -1,
			LegFees: []float64{0.01, 0.01}})
	}
	legA := plabLeg{Ticker: "TA", Side: "YES", Platform: "kalshi", Price: 0.5, PWin: 0.6}
	legB := plabLeg{Ticker: "TB", Side: "NO", Platform: "kalshi", Price: 0.5, PWin: 0.6}
	legC := plabLeg{Ticker: "TC", Side: "YES", Platform: "kalshi", Price: 0.5, PWin: 0.6}
	batch := []storage.PlabCand{
		mk("c1", []plabLeg{legA, legB}),
		mk("c2", []plabLeg{legA, legC}),
	}
	n, err := s.store.PlabInsertBatch(ctx, batch)
	if err != nil || n != 2 {
		t.Fatalf("batch insert: n=%d err=%v", n, err)
	}
	// idempotent re-insert
	if n2, _ := s.store.PlabInsertBatch(ctx, batch); n2 != 0 {
		t.Fatalf("re-insert must be ignored, got %d new", n2)
	}
	tks, err := s.store.PlabUnresolvedTickers(ctx, 100)
	if err != nil || len(tks) != 3 {
		t.Fatalf("expected 3 distinct unresolved tickers, got %d err=%v", len(tks), err)
	}
	// resolve the shared leg: BOTH combos' unresolved drop to 1; none gradable yet.
	if _, err := s.store.PlabResolveTicker(ctx, "kalshi", "TA", 1); err != nil {
		t.Fatal(err)
	}
	if rows, _ := s.store.PlabGradable(ctx, time.Now().Unix(), 10); len(rows) != 0 {
		t.Fatalf("nothing gradable at 1 unresolved leg, got %d", len(rows))
	}
	// resolve the rest: both combos gradable, with correct leg values.
	_, _ = s.store.PlabResolveTicker(ctx, "kalshi", "TB", 0) // NO leg wins (yes=0)
	_, _ = s.store.PlabResolveTicker(ctx, "kalshi", "TC", 1)
	rows, err := s.store.PlabGradable(ctx, time.Now().Unix(), 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("expected 2 gradable, got %d err=%v", len(rows), err)
	}
	vals, err := s.store.PlabLegVals(ctx, []string{"c1", "c2"})
	if err != nil || vals["c1"]["kalshi|TA"] != 1 || vals["c1"]["kalshi|TB"] != 0 || vals["c2"]["kalshi|TC"] != 1 {
		t.Fatalf("leg values wrong: %+v err=%v", vals, err)
	}
	if err := s.store.PlabDeleteBatch(ctx, []string{"c1", "c2"}); err != nil {
		t.Fatal(err)
	}
	if c, _ := s.store.PlabOpenCount(ctx); c != 0 {
		t.Fatalf("delete must clean up, %d rows remain", c)
	}
}

// TestR128PlabGradePassEndToEnd — the full sweep: settle via signal_log, grade, aggregate.
func TestR128PlabGradePassEndToEnd(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.plabMu.Lock()
	s.plabState(ctx)
	s.plabMu.Unlock()
	legs := []plabLeg{
		{Ticker: "GA", Side: "YES", Platform: "kalshi", Price: 0.5, PWin: 0.6},
		{Ticker: "GB", Side: "YES", Platform: "kalshi", Price: 0.5, PWin: 0.6},
	}
	ent := plabOpenEnt{ID: "g1", At: time.Now().Add(-time.Hour), Legs: legs, Bucket: "indep",
		Class: "ind:2leg", Prod: 0.25, JointP: 0.3, FeeMVE: -1, LegFees: []float64{0.01, 0.01},
		Legality: "synthetic_legs_only"}
	if n, err := s.store.PlabInsertBatch(ctx, []storage.PlabCand{plabToRow(ent)}); n != 1 || err != nil {
		t.Fatalf("insert: n=%d err=%v", n, err)
	}
	// settle both legs in signal_log (ResolvedYes authority): both YES win.
	for _, tk := range []string{"GA", "GB"} {
		mustInsertResolvedSignal(t, s, "kalshi", tk, "YES", 0.5, 1)
	}
	before := s.plabGraded
	s.parlayLabGradePass(ctx)
	if s.plabGraded != before+1 {
		t.Fatalf("grade pass must grade the combo (graded %d → %d)", before, s.plabGraded)
	}
	if c, _ := s.store.PlabOpenCount(ctx); c != 0 {
		t.Fatalf("graded combo must leave the ledger, %d remain", c)
	}
	key := "indep|2leg|ind:2leg|synthetic_legs_only|all-eligible"
	s.plabMu.Lock()
	ag := s.plabStats[key]
	s.plabMu.Unlock()
	if ag == nil || ag.N != 1 || ag.Wins != 1 {
		t.Fatalf("aggregate cell wrong: %+v", ag)
	}
}

// mustInsertResolvedSignal seeds one resolved signal_log row (the leg-settlement authority).
// yesVal is the market's settled YES value.
func mustInsertResolvedSignal(t *testing.T, s *Server, platform, ticker, side string, px float64, yesVal float64) {
	t.Helper()
	ctx := context.Background()
	if err := s.store.InsertSignal(ctx, storage.Signal{Platform: platform, Ticker: ticker,
		Title: ticker, Side: side, SignalType: "kalshi-flow", EntryPrice: px}); err != nil {
		t.Fatal(err)
	}
	if err := s.store.ResolveSignals(ctx, ticker, yesVal); err != nil {
		t.Fatal(err)
	}
}

// TestR128SwNormalize — the pool rules: positives split 90% proportionally, collecting members
// split the 10% exploration pool, measured-negatives get 0; no positives ⇒ explorers take all.
func TestR128SwNormalize(t *testing.T) {
	ents := []swEntry{
		{Family: "a", Cents: 3, N: 100, State: "PROVEN+", ProfitEvidence: true},
		{Family: "b", Cents: 1, N: 50, State: "COLLECTING", ProfitEvidence: true},
		{Family: "c", Cents: -2, N: 80, State: "PROVEN-", ProfitEvidence: true},
		{Family: "d", Cents: 0, N: 0, State: "COLLECTING"},
	}
	swNormalize(ents)
	if math.Abs(ents[0].Weight-0.9*0.75) > 1e-9 || math.Abs(ents[1].Weight-0.9*0.25) > 1e-9 {
		t.Fatalf("positive split wrong: %+v", ents)
	}
	if ents[2].Weight != 0 {
		t.Fatalf("measured-negative must be 0: %+v", ents[2])
	}
	if math.Abs(ents[3].Weight-0.10) > 1e-9 || !ents[3].Explore {
		t.Fatalf("collecting must take the exploration pool: %+v", ents[3])
	}
	// no positives ⇒ the collecting set takes everything.
	ents2 := []swEntry{{Family: "x", Cents: -1, N: 10, State: "PROVEN-", ProfitEvidence: true}, {Family: "y", N: 0, State: "COLLECTING"}}
	swNormalize(ents2)
	if ents2[0].Weight != 0 || math.Abs(ents2[1].Weight-1.0) > 1e-9 {
		t.Fatalf("no-positives fallback wrong: %+v", ents2)
	}
	ents3 := []swEntry{{Family: "one-loss", Cents: -20, N: 1, State: "COLLECTING"},
		{Family: "winner", Cents: 5, N: 100, State: "PROVEN+", ProfitEvidence: true}}
	swNormalize(ents3)
	if !ents3[0].Explore || math.Abs(ents3[0].Weight-swExploreFrac) > 1e-9 {
		t.Fatalf("collecting strategy died after one loss: %+v", ents3)
	}
}

// TestR128VarianceShrinkKelly — famEdgeSE feeds m²/(m²+SE²); no verdict ⇒ ok=false (the 0.4
// exploration constant applies upstream, never an n-gate).
func TestR128VarianceShrinkKelly(t *testing.T) {
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{
		{Family: "taker:kthresh@kalshi", SourceFamily: "kthresh", Platform: "kalshi", Side: "YES",
			OriginLayer: "model", Route: "taker", Group: "taker", Unit: "$/contract",
			N: 200, Mean: 0.04, Lo: 0.02, Hi: 0.06, State: "PROVEN+",
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
	})
	m, se, ok := s.famEdgeSE("kalshi", "auto-cons-kthresh", "YES", "taker")
	if !ok || math.Abs(m-0.04) > 1e-9 || math.Abs(se-0.01) > 1e-9 {
		t.Fatalf("famEdgeSE: m=%v se=%v ok=%v", m, se, ok)
	}
	// shrink = 0.0016/(0.0016+0.0001) ≈ 0.941 — high-confidence family barely shrinks.
	sh := (m * m) / (m*m + se*se)
	if sh < 0.9 || sh > 1 {
		t.Fatalf("shrink out of expected range: %v", sh)
	}
	if _, _, ok := s.famEdgeSE("kalshi", "auto-cons-nonexistent", "YES", "taker"); ok {
		t.Fatalf("unknown family must return ok=false (exploration constant upstream)")
	}
}

// TestR128ScoreWeightMult — measured-positive families get mean-normalized multipliers;
// negatives ~0.05; unknown neutral 1; cold table neutral 1.
func TestR128ScoreWeightMult(t *testing.T) {
	s := testServer(t)
	if m := s.scoreWeightMult("auto-cons-kthresh", "kalshi"); m != 1 {
		t.Fatalf("cold table must be neutral, got %v", m)
	}
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), AutoMult: map[string]float64{"kthresh": 2.5, "favlong": 0.05}}
	s.swMu.Unlock()
	if m := s.scoreWeightMult("auto-cons-kthresh", "kalshi"); m != 2.5 {
		t.Fatalf("mapped family mult wrong: %v", m)
	}
	if m := s.scoreWeightMult("auto-cons-favlong", "kalshi"); m != 0.05 {
		t.Fatalf("negative family mult wrong: %v", m)
	}
	if m := s.scoreWeightMult("auto-cons-unknownfam", "kalshi"); m != 1 {
		t.Fatalf("unknown family must be neutral, got %v", m)
	}
	if m := s.scoreWeightMult("auto-cons-kthresh", "polymarket"); m != 1 {
		t.Fatalf("research venue must be neutral, got %v", m)
	}
}

// TestR128FavLong80Gates — band + knob + poisoned-book guards refuse before any book mutation.
func TestR128FavLong80Gates(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	place := func(px float64) int {
		s.favLong80Place(ctx, "FLT-TEST", "t", "YES", px, 2, 1)
		s.flBookMu.Lock()
		defer s.flBookMu.Unlock()
		return len(s.flLoadLocked().Open)
	}
	if n := place(0.79); n != 0 {
		t.Fatalf("below-band entry must refuse, book has %d lots", n)
	}
	if n := place(0.93); n != 0 {
		t.Fatalf("above-band entry must refuse, book has %d lots", n)
	}
	off := false
	s.mutateCfg(func(c *config.Config) { c.Auto.Favlong80Enabled = &off })
	if n := place(0.85); n != 0 {
		t.Fatalf("knob-off must refuse, book has %d lots", n)
	}
	on := true
	s.mutateCfg(func(c *config.Config) { c.Auto.Favlong80Enabled = &on })
	// spread screen
	s.favLong80Place(ctx, "FLT-TEST", "t", "YES", 0.85, 20, 1)
	s.flBookMu.Lock()
	n := len(s.flLoadLocked().Open)
	s.flBookMu.Unlock()
	if n != 0 {
		t.Fatalf("wide-spread entry must refuse, book has %d lots", n)
	}
}

// TestR165FavLong80LegacyPaperSettlementStaysOutOfProfitVerdicts proves a settled legacy Paper
// lot remains available in its own ledger without becoming exchange profit evidence.
func TestR165FavLong80LegacyPaperSettlementStaysOutOfProfitVerdicts(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	s.flBookMu.Lock()
	b := s.flLoadLocked()
	b.Open = append(b.Open, kfPos{TS: time.Now().UTC().Format(time.RFC3339), Ticker: "FLS-1",
		Title: "t", Side: "YES", Price: 0.85, Contracts: 10, Fee: 0.05, Platform: "kalshi"})
	s.flBookDirty = true
	s.flBookMu.Unlock()
	mustInsertResolvedSignal(t, s, "kalshi", "FLS-1", "YES", 0.85, 1)
	s.settleFavLong80Book(ctx)
	s.flBookMu.Lock()
	wins, open := s.flLoadLocked().Wins, len(s.flLoadLocked().Open)
	s.flBookMu.Unlock()
	if wins != 1 || open != 0 {
		t.Fatalf("settle failed: wins=%d open=%d", wins, open)
	}
	// This hand-built lot has no corrected two-touch execution or authenticated LIVE-fill linkage.
	// It must not fold into a money-facing verdict merely because it settled.
	s.verdMu.Lock()
	s.verdCache, s.verdAt = nil, time.Time{}
	s.verdMu.Unlock()
	found := false
	for _, v := range s.computeExperimentVerdicts(ctx) {
		if v.Family == "book:favlong80" && v.N > 0 {
			found = true
		}
	}
	if found {
		t.Fatal("legacy FavLong80 Paper settlement entered a money-facing verdict")
	}
}

// TestR128EdgeLegsInverted — plabEdgeLegs turns a PROVEN− family's fresh signals into flipped
// legs at the drag-adjusted invert edge, and PROVEN+ signals into direct legs.
func TestR128EdgeLegsInverted(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// fresh unresolved rows for both families
	if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: "EL-POS",
		Title: "t", Side: "YES", SignalType: "kthresh", EntryPrice: 0.40}); err != nil {
		t.Fatal(err)
	}
	if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: "EL-NEG",
		Title: "t", Side: "YES", SignalType: "favlong", EntryPrice: 0.70}); err != nil {
		t.Fatal(err)
	}
	injectVerdicts(s, []verdictEnt{
		{Family: "kthresh", Group: "signal", Unit: "$/contract", N: 100, Mean: 0.03, Lo: 0.01, Hi: 0.05, State: "PROVEN+"},
		{Family: "favlong", Group: "signal", Unit: "$/contract", N: 200, Mean: -0.06, Lo: -0.08, Hi: -0.04, State: "PROVEN-", Invert: 0.02},
	})
	legs := s.plabEdgeLegs(ctx, 0.005)
	var pos, inv *plabLeg
	for i := range legs {
		switch legs[i].Ticker {
		case "EL-POS":
			pos = &legs[i]
		case "EL-NEG":
			inv = &legs[i]
		}
	}
	if pos == nil || pos.Side != "YES" || pos.Src != "edge" || math.Abs(pos.EVNet-0.03) > 1e-9 {
		t.Fatalf("direct edge leg wrong: %+v", pos)
	}
	if inv != nil {
		t.Fatalf("mechanically inverted edge leg must stay diagnostic-only, got %+v", inv)
	}
}

// TestR128PxOfflineCoversNewPaths — the sub-book placement paths fail closed on dead feeds.
func TestR128PxOfflineCoversNewPaths(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// hermetic server: no WS, no meta stamps ⇒ pxOfflineReason must be non-"" for kalshi/polyus.
	if r := s.pxOfflineReason("kalshi", "PXO-1"); r == "" {
		t.Skip("test server has fresh kalshi meta — px-offline shape not reproducible here")
	}
	on := true
	s.mutateCfg(func(c *config.Config) { c.Auto.Favlong80Enabled = &on })
	s.favLong80Place(ctx, "PXO-1", "t", "YES", 0.85, 2, 1)
	s.flBookMu.Lock()
	n := len(s.flLoadLocked().Open)
	s.flBookMu.Unlock()
	if n != 0 {
		t.Fatalf("favlong80 must refuse on px-offline, book has %d lots", n)
	}
	// combos overlay path: xvComboTry must return before any partner scan on dead feeds.
	s.xvComboTry(ctx, "auto", "PXO-1", "YES", 0.5, 0.02, 0)
	s.xvcMu.Lock()
	cn := len(s.xvcLoadLocked().Open)
	s.xvcMu.Unlock()
	if cn != 0 {
		t.Fatalf("combo overlay must refuse on px-offline, %d open", cn)
	}
}

// TestR128PromoShareCapWeightBased — the share cap is weight × book bank when the table is warm,
// legacy AllocUSD fallback when cold.
func TestR128PromoShareCapWeightBased(t *testing.T) {
	s := testServer(t)
	// cold: legacy default ($100 promotion bankroll)
	if capUSD := s.promoShareCap("favlong80"); capUSD != 100 {
		t.Fatalf("cold-table fallback should be the $100 default, got %v", capUSD)
	}
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), SubShare: map[string]float64{
		"book:favlong80": 55.5, "book:freshinv-k": 12.25, "book:xvgap": 200}}
	s.swMu.Unlock()
	if capUSD := s.promoShareCap("favlong80"); math.Abs(capUSD-55.5) > 1e-9 {
		t.Fatalf("favlong80 share must be the weight share, got %v", capUSD)
	}
	if capUSD := s.promoShareCap("invert:freshlist"); math.Abs(capUSD-12.25) > 1e-9 {
		t.Fatalf("freshinv share must map through book:freshinv-k, got %v", capUSD)
	}
	if capUSD := s.promoShareCap("xvgap"); math.Abs(capUSD-200) > 1e-9 {
		t.Fatalf("xvgap share must map through book:xvgap, got %v", capUSD)
	}
}

// TestR128XvcSrcTag — combo-overlay rows carry the originating book's src tag (json round-trip).
func TestR128XvcSrcTag(t *testing.T) {
	p := xvcPos{TS: "t", LegA: "A", LegB: "B", Mode: "combo_overlay", Expr: xvcExprSynth, Src: "ml"}
	b, _ := json.Marshal(p)
	var back xvcPos
	if json.Unmarshal(b, &back) != nil || back.Src != "ml" {
		t.Fatalf("src tag must survive the book file round-trip: %s", b)
	}
}
