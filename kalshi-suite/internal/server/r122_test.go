package server

// r122_test.go — R122 pins:
//   1. routeVerdict decision matrix (queue-aware maker/taker routing, Part 2)
//   2. per-family edge-decay horizons + ML calibration gate (407 + decision item 6)
//   3. LOGGING CONTINUITY regression (Part 3): a NOT-promoted/retired freshinv book must NOT
//      silence the PolyUS freshlist/freshfade detector (the audit's one hard book-state coupling)
//   4. kill switch gates PLACEMENT only: kflowBookPlace refuses while tripped (the one place
//      hook that had NO kill check), while the detector-side logging paths carry no ks gates
//   5. xvgap pocket screen (executor entry rules)
//   6. parlay lab: open-ledger persistence reload + graded-counter seeding (447/448/408),
//      4/5-leg enumeration gated on venue collections, seen-func budget streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestR122RouteVerdictMatrix(t *testing.T) {
	cases := []struct {
		name       string
		qa         float64
		qk         bool
		rate       float64
		rok        bool
		hz, margin float64
		wantMaker  bool
		wantReason string
	}{
		{"no-queue-data", 0, false, 0, false, 1800, 1, false, "taker:no-queue-data"},
		{"empty-queue-long-horizon", 0, true, 0, false, 7200, 1, true, "maker:queue-clear"},
		{"tiny-queue", 1, true, 0, false, 60, 1, true, "maker:queue-clear"},
		{"deep-queue-no-flow", 500, true, 0, true, 7200, 1, false, "taker:no-flow"},
		{"deep-queue-short-horizon", 250, true, 36, true, 300, 1, false, "taker:queue-deep"},  // eta 417s > 300s
		{"deep-queue-long-horizon", 250, true, 36, true, 3600, 1, true, "maker:queue-clear"},  // eta 417s ≤ 3600s
		{"boundary-eta-equals-horizon", 60, true, 60, true, 60, 1, true, "maker:queue-clear"}, // eta 60s ≤ 60s (≤ is maker)
		{"boundary-eta-just-over", 61, true, 60, true, 60, 1, false, "taker:queue-deep"},      // eta 61s > 60s
		{"margin-stretches-horizon", 250, true, 36, true, 300, 2, true, "maker:queue-clear"},  // eta 417 ≤ 600
	}
	for _, c := range cases {
		d := routeVerdict(c.qa, c.qk, c.rate, c.rok, c.hz, c.margin)
		if d.Maker != c.wantMaker || d.Reason != c.wantReason {
			t.Fatalf("%s: got maker=%v reason=%q, want maker=%v reason=%q (d=%+v)",
				c.name, d.Maker, d.Reason, c.wantMaker, c.wantReason, d)
		}
	}
	// the tag must carry the estimate inputs for the auditor
	d := routeVerdict(250, true, 36, true, 300, 1)
	if tag := d.Tag(); tag == "" || tag == d.Reason {
		t.Fatalf("Tag() must embed the estimate inputs, got %q", tag)
	}
}

func TestR122FamDecayHorizons(t *testing.T) {
	if famDecayHorizonSec("rawflow") <= 0 || famDecayHorizonSec("auto-cons-kflow") <= 0 ||
		famDecayHorizonSec("ml-book") <= 0 {
		t.Fatal("measured families must carry a positive decay horizon")
	}
	if famDecayHorizonSec("kflow-pre") <= famDecayHorizonSec("kflow-live") {
		t.Fatal("kflow-pre (1.40h median peak) must out-horizon kflow-live (0.43h)")
	}
	if famDecayHorizonSec("weather") != 0 || famDecayHorizonSec("xvgap") != 0 {
		t.Fatal("unmeasured families must return 0 (resolve-based fallback)")
	}
}

func TestR122MLCalibGate(t *testing.T) {
	s := testServer(t)
	// No predictions file at all → sensor absent → PAUSED (fail-safe per the auditor's proposal).
	if red, why := s.mlCalibGate(); !red || !strings.Contains(why, "sensor-thin") {
		t.Fatalf("absent sensor must pause (red), got red=%v why=%q", red, why)
	}
	// Publish a healthy sensor → gate green.
	path := filepath.Join(s.cfg().DataDir, "ml_predictions.json")
	if err := os.WriteFile(path, []byte(`{"ece_gated":0.01,"ece_gated_n":300,"predictions":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s.mlPredMu.Lock()
	s.mlPredAt = time.Time{} // force the cache refresh
	s.mlPredMu.Unlock()
	if red, why := s.mlCalibGate(); red {
		t.Fatalf("healthy sensor (1%%) must not pause, got why=%q", why)
	}
	// Degraded sensor → paused with the numbers in the reason.
	if err := os.WriteFile(path, []byte(`{"ece_gated":0.09,"ece_gated_n":300,"predictions":[]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s.mlPredMu.Lock()
	s.mlPredAt = time.Time{}
	s.mlPredMu.Unlock()
	if red, _ := s.mlCalibGate(); !red {
		t.Fatal("ece_gated 9% must pause ML maker posting")
	}
	// Operator off-switch (<0) → never pauses.
	cfg := *s.cfg()
	cfg.Auto.MLMakerCalibGateECE = -1
	s.cfgP.Store(&cfg)
	if red, _ := s.mlCalibGate(); red {
		t.Fatal("gate disabled (<0) must never pause")
	}
}

// TestR122LoggingContinuityFreshinvNotPromoted — Part 3's hard-coupling regression: the PolyUS
// freshlist/freshfade detector must log rows even when the freshinv book is NOT promoted
// (pre-R122 it returned before the inserts whenever the promotion record wasn't PROMOTED/GROWN).
func TestR122LoggingContinuityFreshinvNotPromoted(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// a fresh two-sided PUS listing, first seen 10 min ago, seed pass done
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{{Slug: "atc-test-aaa-bbb-2026-07-09-aaa", Game: "AAA @ BBB", Yes: 0.44, Bid: 0.43, Ask: 0.45}}
	s.polyUSMu.Unlock()
	s.pusFreshMu.Lock()
	s.pusFreshSeeded = true
	s.pusFreshSeen = map[string]time.Time{"atc-test-aaa-bbb-2026-07-09-aaa": time.Now().Add(-10 * time.Minute)}
	s.pusFreshMu.Unlock()
	// NO promotion record exists → freshInvActive() == false → pre-R122 this silenced logging.
	s.sweepFreshInvPolyUS(ctx)
	var n int
	if err := s.store.DBForTest().QueryRow(
		`SELECT COUNT(*) FROM signal_log WHERE signal_type IN ('freshlist','freshfade') AND platform='polyus'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("PolyUS freshlist+freshfade must log while the book is NOT promoted (Part 3 decoupling), got %d rows", n)
	}
}

// TestR122KillSwitchGatesPlacementOnly — kflowBookPlace was the ONE placement hook with no kill
// check (it hangs off MonitorSignalLog, which correctly ignores the switch for logging).
func TestR122KillSwitchGatesPlacementOnly(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Auto.KflowBooksEnabled = true
	s.cfgP.Store(&cfg)
	s.ks.Trip("r122-test")
	s.kflowBookPlace(1, "R122KS", "title", "YES", 0.40, 1)
	s.kfBookMu.Lock()
	open := len(s.kfLoadLocked().Live.Open)
	s.kfBookMu.Unlock()
	if open != 0 {
		t.Fatal("kflowBookPlace must refuse while the kill switch is tripped (R122 placement gate)")
	}
}

func TestR122XvgEntryScreen(t *testing.T) {
	cases := []struct {
		plat    string
		px, spC float64
		want    bool
	}{
		{"kalshi", 0.35, 2, false},  // the kalshi cells are CI-negative — never traded
		{"polyus", 0.19, 2, false},  // below the pocket
		{"polyus", 0.20, 2, true},   // pocket floor
		{"polyus", 0.50, 2, true},   // pocket ceiling
		{"polyus", 0.51, 2, false},  // above the pocket
		{"polyus", 0.35, 11, false}, // spread screen
		{"polyus", 0.35, 3, true},
	}
	for _, c := range cases {
		if got := xvgEntryOK(c.plat, c.px, c.spC); got != c.want {
			t.Fatalf("xvgEntryOK(%s, %.2f, %.0f) = %v, want %v", c.plat, c.px, c.spC, got, c.want)
		}
	}
}

func TestR122PlabOpenPersistenceAndCounterSeed(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// one persisted open entry + one stats cell in kv
	ent := plabOpenEnt{ID: "abc123", At: time.Now().Add(-time.Hour), Bucket: "indep", Class: "ind:2leg:other+other",
		Legs: []plabLeg{{Ticker: "T1", Side: "YES", Platform: "kalshi", Price: 0.4, PWin: 0.5, EVNet: 0.02},
			{Ticker: "T2", Side: "NO", Platform: "kalshi", Price: 0.4, PWin: 0.5, EVNet: 0.02}},
		Prod: .16, JointP: .25, LegFees: []float64{0, 0}}
	eb, _ := json.Marshal(ent)
	if err := s.store.KVSet(ctx, plabKVOpenPfx+"abc123", string(eb)); err != nil {
		t.Fatal(err)
	}
	stats := map[string]*plabAgg{"indep|2leg|ind:2leg|unprobed": {N: 49, Wins: 20}}
	sb, _ := json.Marshal(stats)
	if err := s.store.KVSet(ctx, plabKVStats, string(sb)); err != nil {
		t.Fatal(err)
	}
	s.plabMu.Lock()
	s.plabState(ctx)
	graded, cand := s.plabGraded, s.plabCand
	_, seenOK := s.plabSeen["abc123"]
	s.plabMu.Unlock()
	// R128: legacy kv rows MIGRATE into the unlimited SQLite ledger (plab_open) and the kv copies
	// are dropped; the dedup latch still survives the restart.
	openN, err := s.store.PlabOpenCount(ctx)
	if err != nil || openN != 1 || !seenOK {
		t.Fatalf("open ledger must migrate kv → plab_open (447/R128): open=%d err=%v seen=%v", openN, err, seenOK)
	}
	if m, _ := s.store.KVPrefix(ctx, plabKVOpenPfx); len(m) != 0 {
		t.Fatalf("legacy kv open rows must be deleted after migration, %d remain", len(m))
	}
	if graded != 0 {
		t.Fatalf("aggregate-only rows must not seed the immutable all-in-v2 grade count: got %d", graded)
	}
	if cand < 1 {
		t.Fatalf("candidates counter must still cover the migrated open row: got %d", cand)
	}
}

func TestR122PlabEnumerateWideLegs(t *testing.T) {
	// R123: the exhaustive enumerator SUPERSEDES the R122 collection-gated 4/5-leg special
	// cases — every subset up to maxLegs enumerates when it clears the floor; venue-legality
	// stays the probe cache's per-combo job. This pin: full completeness on a small pool.
	mk := func(i int, ev string) plabLeg {
		return plabLeg{Ticker: fmt.Sprintf("TK%d", i), EventKey: ev, Price: 0.5, PWin: 0.55, EVNet: 0.02}
	}
	pool := []plabLeg{mk(1, "e1"), mk(2, "e2"), mk(3, "e3"), mk(4, "e4"), mk(5, "e5")}
	tw := make([]string, len(pool))
	combos, st := plabEnumerateEx(pool, tw, 10, 400, -100, nil, timeNowPlus15s())
	// C(5,2)+C(5,3)+C(5,4)+C(5,5) = 10+10+5+1 = 26 — literally every combination
	if len(combos) != 26 || !st.Complete {
		t.Fatalf("exhaustive 5-pool must yield 26 combos complete=true, got %d complete=%v", len(combos), st.Complete)
	}
	var got4, got5 bool
	for _, legs := range combos {
		switch len(legs) {
		case 4:
			got4 = true
		case 5:
			got5 = true
		}
	}
	if !got4 || !got5 {
		t.Fatalf("4/5-leg combos must enumerate without any collection gate (got4=%v got5=%v)", got4, got5)
	}
	// seen-func streaming: a fully-seen space yields nothing (and no budget panic)
	seenAll := func(string) bool { return true }
	if out, _ := plabEnumerateEx(pool, tw, 10, 400, -100, seenAll, timeNowPlus15s()); len(out) != 0 {
		t.Fatalf("fully-deduped space must enumerate nothing, got %d", len(out))
	}
}
