package server

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR142ComboLabCurrentHorizonHardCapsAndFailClosed(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.ParlayMaxHoursOut = 9
	cfg.Auto.ConsensusCryptoMaxHoursOut = 4
	s.cfgP.Store(&cfg)
	now := time.Now().UTC()
	horizons := map[string]time.Duration{
		"REG-OK": 3*time.Hour + 59*time.Minute, "REG-FAR": 4*time.Hour + time.Minute,
		"KXBTC-OK": 119 * time.Minute, "KXBTC-FAR": 121 * time.Minute,
		"PAST": -time.Minute,
	}
	s.plabHorizonFn = func(_ context.Context, l plabLeg, at time.Time) (time.Time, bool) {
		d, ok := horizons[l.Ticker]
		if !ok {
			return time.Time{}, false
		}
		return at.Add(d), true
	}
	cases := []struct {
		ticker, wantReason string
		wantOK             bool
	}{
		{"REG-OK", "", true}, {"REG-FAR", "regular_over_horizon", false},
		{"KXBTC-OK", "", true}, {"KXBTC-FAR", "crypto_over_horizon", false},
		{"PAST", "nonpositive_horizon", false}, {"UNKNOWN", "unknown_horizon", false},
	}
	for _, tc := range cases {
		_, reason, ok := s.plabLegCurrentHorizon(context.Background(), plabLeg{Platform: "kalshi", Ticker: tc.ticker}, now)
		if ok != tc.wantOK || reason != tc.wantReason {
			t.Errorf("%s ok=%v reason=%q, want ok=%v reason=%q", tc.ticker, ok, reason, tc.wantOK, tc.wantReason)
		}
	}
	cfg.Auto.ParlayMaxHoursOut = 3
	cfg.Auto.ConsensusCryptoMaxHoursOut = 1
	s.cfgP.Store(&cfg)
	if _, reason, ok := s.plabLegCurrentHorizon(context.Background(), plabLeg{Platform: "kalshi", Ticker: "REG-OK"}, now); ok || reason != "regular_over_horizon" {
		t.Fatalf("configured regular cap did not tighten hard cap: ok=%v reason=%q", ok, reason)
	}
	if _, reason, ok := s.plabLegCurrentHorizon(context.Background(), plabLeg{Platform: "kalshi", Ticker: "KXBTC-OK"}, now); ok || reason != "crypto_over_horizon" {
		t.Fatalf("configured crypto cap did not tighten hard cap: ok=%v reason=%q", ok, reason)
	}
}

func TestR142ComboLabGenerationPersistsHorizonRejections(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	s.plabHorizonFn = func(_ context.Context, l plabLeg, _ time.Time) (time.Time, bool) {
		if l.Ticker == "FAR" {
			return now.Add(12 * time.Hour), true
		}
		return now.Add(time.Hour), true
	}
	s.plabQuoteFn = func(_ context.Context, l plabLeg) (float64, float64, string, bool) {
		return l.Price, 10, "test-book", true
	}
	s.plabSampleFeeFn = func(plabLeg, float64) (float64, bool) { return 0, true }
	s.plabWriteManifest(ctx, []plabLeg{
		{Platform: "kalshi", Ticker: "A", Side: "YES", Price: .4, PWin: .7, EventKey: "a"},
		{Platform: "kalshi", Ticker: "B", Side: "YES", Price: .4, PWin: .7, EventKey: "b"},
		{Platform: "kalshi", Ticker: "FAR", Side: "YES", Price: .4, PWin: .7, EventKey: "far"},
	}, .005)
	raw, ok := s.store.KVGet(ctx, plabKVHorizon)
	if !ok {
		t.Fatal("durable Combo Lab horizon ledger missing")
	}
	var ledger plabHorizonLedger
	if err := json.Unmarshal([]byte(raw), &ledger); err != nil || ledger.GeneratedRejected["regular_over_horizon"] != 1 {
		t.Fatalf("horizon ledger=%+v err=%v", ledger, err)
	}
}

func TestR142ComboLabGradesSettlementAndPreservesPostponedLegacy(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	s.plabHorizonFn = func(_ context.Context, _ plabLeg, _ time.Time) (time.Time, bool) {
		return now.Add(12 * time.Hour), true
	}
	s.plabMu.Lock()
	s.plabState(ctx)
	s.plabMu.Unlock()
	entry := func(id string, legs []plabLeg) plabOpenEnt {
		return plabOpenEnt{ID: id, At: now.Add(-20 * time.Minute), Legs: legs, Bucket: "indep", Class: "ind:2leg",
			Prod: .25, JointP: .36, FeeMVE: -1, EVSyn: .44, LegFees: []float64{0, 0},
			Legality: "synthetic_legs_only", Cohort: "all-eligible", RouteState: "synthetic-settlement-only"}
	}
	settled := entry("settled-first", []plabLeg{{Platform: "kalshi", Ticker: "A", Side: "YES"}, {Platform: "kalshi", Ticker: "B", Side: "YES"}})
	pending := entry("pending-far", []plabLeg{{Platform: "kalshi", Ticker: "C", Side: "YES"}, {Platform: "kalshi", Ticker: "D", Side: "YES"}})
	if n, err := s.store.PlabInsertBatch(ctx, []storage.PlabCand{plabToRow(settled), plabToRow(pending)}); err != nil || n != 2 {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	_, _ = s.store.PlabResolveTicker(ctx, "kalshi", "A", 1)
	_, _ = s.store.PlabResolveTicker(ctx, "kalshi", "B", 1)
	s.parlayLabGradePass(ctx)
	graded := 0
	s.plabMu.Lock()
	for _, cell := range s.plabStats {
		graded += cell.N
	}
	s.plabMu.Unlock()
	if graded != 1 {
		t.Fatalf("settled row was not graded before cleanup: graded=%d", graded)
	}
	if open, err := s.store.PlabOpenCount(ctx); err != nil || open != 1 {
		t.Fatalf("settled row must grade while postponed legacy remains: open=%d err=%v", open, err)
	}
	raw, _ := s.store.KVGet(ctx, plabKVHorizon)
	var ledger plabHorizonLedger
	_ = json.Unmarshal([]byte(raw), &ledger)
	if ledger.OpenRowsPruned["regular_over_horizon"] != 0 {
		t.Fatalf("postponed legacy truth was erased: %+v", ledger)
	}
}

func TestR142ComboLabKeepsEndedOrUnknownLegsForVenueSettlement(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	s.plabHorizonFn = func(_ context.Context, leg plabLeg, _ time.Time) (time.Time, bool) {
		switch leg.Ticker {
		case "ENDED":
			return now.Add(-time.Hour), true
		case "UNKNOWN":
			return time.Time{}, false
		default:
			return now.Add(12 * time.Hour), true
		}
	}
	mustJSON := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal fixture: %v", err)
		}
		return string(b)
	}
	row := func(id, ticker string) storage.PlabCand {
		legs := []plabLeg{
			{Platform: "kalshi", Ticker: ticker, Side: "YES"},
			{Platform: "kalshi", Ticker: "ANCHOR", Side: "NO"},
		}
		return storage.PlabCand{ID: id, At: now.Unix(), Legs: mustJSON(legs), NLegs: 2,
			Bucket: "indep", Class: "ind:2leg", Prod: .25, JointP: .30, FeeMVE: -1,
			EVSyn: .20, LegFees: mustJSON([]float64{0, 0}), LegKeys: []storage.PlabLegKey{
				{Platform: "kalshi", Ticker: ticker}, {Platform: "kalshi", Ticker: "ANCHOR"},
			}}
	}
	bad := row("malformed", "BAD")
	bad.Legs = "{"
	if n, err := s.store.PlabInsertBatch(ctx, []storage.PlabCand{row("ended", "ENDED"), row("unknown", "UNKNOWN"), row("far", "FAR"), bad}); err != nil || n != 4 {
		t.Fatalf("insert n=%d err=%v", n, err)
	}
	pruned, reasons, err := s.plabPruneCurrentHorizon(ctx, now, 100)
	if err != nil || pruned != 1 || reasons["invalid_legacy_row"] != 1 {
		t.Fatalf("pruned=%d reasons=%v err=%v", pruned, reasons, err)
	}
	if open, err := s.store.PlabOpenCount(ctx); err != nil || open != 3 {
		t.Fatalf("ended/unknown/postponed rows must remain for settlement: open=%d err=%v", open, err)
	}
}
