package server

// R91 tests: (1) the reset_close number-vs-bool poison that silently killed the R90 realization
// haircut (flexBool tolerance + end-to-end revival on a file that carries numeric flags), and
// (2) the Poly US resolve-horizon fix (Task B: 100% of PUS live-proposal candidates died
// "resolve horizon unknown" because the snapshot-only lookup returned 0 for futures/props and
// aged-out slugs — market_catalog close_ts is the fallback), and (3) the empirically derived
// ML borders remain enforceable at fractional cent floors.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestFlexBoolTolerance(t *testing.T) {
	var v struct {
		A flexBool `json:"a"`
		B flexBool `json:"b"`
		C flexBool `json:"c"`
		D flexBool `json:"d"`
		E flexBool `json:"e"`
	}
	blob := `{"a": 1, "b": 1.0, "c": true, "d": false, "e": 0}`
	if err := json.Unmarshal([]byte(blob), &v); err != nil {
		t.Fatalf("flexBool must tolerate 1 / 1.0 / true / false / 0: %v", err)
	}
	if !bool(v.A) || !bool(v.B) || !bool(v.C) || bool(v.D) || bool(v.E) {
		t.Fatalf("flexBool truth table wrong: %+v", v)
	}
	var w struct {
		X flexBool `json:"x"`
	}
	if err := json.Unmarshal([]byte(`{"x": "banana"}`), &w); err == nil {
		t.Fatalf("garbage must still error, got %+v", w)
	}
}

// TestRealizationSurvivesNumericResetClose reproduces the live failure verbatim: the settle
// path wrote `"reset_close": 1` (number) and refreshRealizationRatios' struct said `bool`, so
// json.Unmarshal failed the WHOLE file and the haircut ran blind (ratio 1.0 for every family).
func TestRealizationSurvivesNumericResetClose(t *testing.T) {
	s := testServer(t)
	book := `{"closed":[
		{"signal_type":"kalshi-flow","ev_net":0.10,"contracts":10,"pnl":0.10,"reset_close":0},
		{"signal_type":"kalshi-flow","ev_net":0.10,"contracts":10,"pnl":0.10},
		{"signal_type":"kalshi-flow","ev_net":0.10,"contracts":10,"pnl":0.10,"reset_close":false},
		{"signal_type":"kalshi-flow","ev_net":0.10,"contracts":10,"pnl":0.10,"reset_close":0.0},
		{"signal_type":"kalshi-flow","ev_net":9.99,"contracts":99,"pnl":-9.99,"reset_close":1},
		{"signal_type":"kalshi-flow","ev_net":9.99,"contracts":99,"pnl":-9.99,"reset_close":true}
	]}`
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "ml_paper.json"), []byte(book), 0o644); err != nil {
		t.Fatalf("write book: %v", err)
	}
	s.refreshRealizationRatios(context.Background())
	ratio, n := s.realizationRatio("kalshi-flow")
	if n != 4 {
		t.Fatalf("want n=4 (reset_close rows excluded, numeric AND bool), got n=%d ratio=%v", n, ratio)
	}
	// pred = 4×(0.10×10) = 4.0, act = 0.4 → raw 0.1 → shrunk (4×0.1+25)/(4+25) ≈ 0.876
	if ratio < 0.85 || ratio > 0.90 {
		t.Fatalf("shrunk ratio ≈0.876 expected, got %v", ratio)
	}
}

func TestSigResolveHoursPUSCatalogFallback(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	rows := []storage.CatalogRow{
		{Venue: "polyus", Ticker: "aec-nba-fut-2026-07-06", Kind: "winner", Title: "pre-game 2h out",
			CloseTS: now.Add(2 * time.Hour).Format(time.RFC3339)},
		{Venue: "polyus", Ticker: "aec-nba-live-2026-07-06", Kind: "winner", Title: "started 1h ago",
			CloseTS: now.Add(-1 * time.Hour).Format(time.RFC3339)},
		{Venue: "polyus", Ticker: "aec-cs2-dead-2026-06-27", Kind: "winner", Title: "ended long ago",
			CloseTS: now.Add(-9 * time.Hour).Format(time.RFC3339)},
		{Venue: "polyus", Ticker: "aec-mlb-nocts-2026-07-06", Kind: "winner", Title: "no close_ts", CloseTS: ""},
	}
	if err := s.store.UpsertMarketCatalog(ctx, rows); err != nil {
		t.Fatalf("upsert catalog: %v", err)
	}
	// direct getter sanity
	if ts, kind, ok := s.store.CatalogCloseTS(ctx, "polyus", "aec-nba-fut-2026-07-06"); !ok || kind != "winner" || ts == "" {
		t.Fatalf("CatalogCloseTS miss: %q %q %v", ts, kind, ok)
	}
	if _, _, ok := s.store.CatalogCloseTS(ctx, "polyus", "aec-mlb-nocts-2026-07-06"); ok {
		t.Fatalf("empty close_ts must read ok=false")
	}
	cases := []struct {
		slug   string
		lo, hi float64 // expected hours window (gameLen=3.5 pad included)
	}{
		{"aec-nba-fut-2026-07-06", 5.3, 5.7},  // 2h to start + 3.5 game
		{"aec-nba-live-2026-07-06", 2.3, 2.7}, // −1h + 3.5 → in-play, resolving soon
		{"aec-cs2-dead-2026-06-27", 0, 0},     // −9h + 3.5 = −5.5 ≤ −2 → zombie stays unknown
		{"aec-mlb-nocts-2026-07-06", 0, 0},    // catalog row without close_ts → unknown
		{"never-seen-slug", 0, 0},             // not in snapshot, not in catalog → unknown
	}
	for _, tc := range cases {
		got := s.sigResolveHours(ctx, "polyus", tc.slug)
		if got < tc.lo || got > tc.hi {
			t.Fatalf("%s: sigResolveHours=%v, want [%v,%v]", tc.slug, got, tc.lo, tc.hi)
		}
	}
	// cached second read must agree (and not re-hit the DB — value equality is the observable)
	if a, b := s.sigResolveHours(ctx, "polyus", "aec-nba-fut-2026-07-06"), s.sigResolveHours(ctx, "polyus", "aec-nba-fut-2026-07-06"); a < 5.3 || b < 5.3 {
		t.Fatalf("cache path diverged: %v vs %v", a, b)
	}
}

func TestSigResolveHoursPUSSnapshotShapes(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	start := time.Now().UTC().Add(90 * time.Minute)
	s.polyUSMkts = []polyUSMarket{
		{Slug: "gs-shape", Start: start.Format("2006-01-02 15:04:05+00")}, // gameStartTime shape — pre-R91 read "unknown"
		{Slug: "rfc-shape", Start: start.Format(time.RFC3339)},
		{Slug: "blank-live", Start: "", Live: true},
	}
	for slug, want := range map[string][2]float64{
		"gs-shape":   {4.8, 5.2}, // 1.5h + 3.5
		"rfc-shape":  {4.8, 5.2},
		"blank-live": {2.0, 2.0}, // in-play fallback unchanged
	} {
		if got := s.sigResolveHours(ctx, "polyus", slug); got < want[0] || got > want[1] {
			t.Fatalf("%s: got %v want [%v,%v]", slug, got, want[0], want[1])
		}
	}
}

// TestMLBordersR91Derived pins the R91 empirical borders' mechanics: fractional EV floors work
// (config 0 still means "disabled", so the derived 0.1¢ floor must be expressible) and the 15¢
// ceiling quarantines. Derivation (2026-07-06, deduped fee-net signal rows, n=2,798): the ONLY
// CI-positive EV band was 0-2¢ (+11.8¢/ct); 15-30¢ bands realized −7.3..−7.6¢/ct.
func TestMLBordersR91Derived(t *testing.T) {
	s := borderServer(t, 0.35, 0.95, 0.1, 15)
	cases := []struct {
		pwin    float64
		evNet   float64
		verdict string
	}{
		{0.60, 0.0005, "reject"},     // 0.05¢ < 0.1¢ floor
		{0.60, 0.002, ""},            // 0.2¢ passes the fractional floor
		{0.60, 0.001, ""},            // AT the 0.1¢ floor passes (strict inequality)
		{0.60, 0.155, "quarantine"},  // above the 15¢ too-good ceiling
		{0.349, 0.05, "reject"},      // below the 0.35 plausibility floor
		{0.35, 0.05, ""},             // AT the floor passes
		{0.951, 0.05, "reject"},      // ceiling unchanged
	}
	for _, tc := range cases {
		if v, why := s.mlBorders(tc.pwin, tc.evNet); v != tc.verdict {
			t.Fatalf("pwin=%v ev=%v: got %q (%s), want %q", tc.pwin, tc.evNet, v, why, tc.verdict)
		}
	}
}
