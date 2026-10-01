package server

// r127b_test.go — R127-D ship-gate pins (agent D; r127_test.go untouched by agreement):
//   B3  notail: the pure candidate filter (90–99¢ NO / broad board / liquidity floor), the sweep
//       against a cached-board fixture, and the per-UTC-day dedup incl. the kv restart latch.
//   B4  CLV: computed from a synthetic post_path through the REAL pipeline (insert → fill →
//       breadcrumb marks → resolve), malformed tails NULL out instead of corrupting the mean,
//       and the ONE briefing line renders in its documented shape.
//   C4/C5  money-policy knobs: shipped defaults are OFF (+ cluster_cap_pct 22), the preview is a
//       total no-op at defaults, and ARMED it only audit-logs (throttled) — never touches stake.

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// ── B3: notail candidate filter (pure) ──────────────────────────────────────────────────────────

func TestR127BNotailCandidateFilter(t *testing.T) {
	cases := []struct {
		name                   string
		yes, no, bid, ask, vol float64
		siblings               int
		want                   bool
	}{
		{"tail on a broad liquid board", 0.05, 0.95, 0.03, 0.05, 100, 3, true},
		{"NO at exactly 90c passes", 0.10, 0.90, 0.08, 0.10, 10, 3, true},
		{"NO at exactly 99c passes", 0.01, 0.99, 0.01, 0.02, 10, 5, true},
		{"YES above 10c = not a tail", 0.12, 0.88, 0.10, 0.12, 100, 5, false},
		{"YES below 1c = untradeable dust", 0.005, 0.995, 0.0, 0.01, 100, 5, false},
		{"NO above 99c fails even with yes in band", 0.01, 0.995, 0.01, 0.02, 10, 5, false},
		{"narrow board (2 siblings) refused", 0.05, 0.95, 0.03, 0.05, 100, 2, false},
		{"no volume + one-sided book refused", 0.05, 0.95, 0.0, 0.05, 0, 3, false},
		{"no volume but two-sided book passes", 0.05, 0.95, 0.03, 0.05, 0, 3, true},
		{"volume alone passes the liquidity floor", 0.05, 0.95, 0.0, 0.0, 25, 3, true},
	}
	for _, c := range cases {
		if got := notailCandidate(c.yes, c.no, c.bid, c.ask, c.vol, c.siblings); got != c.want {
			t.Errorf("%s: notailCandidate = %v, want %v", c.name, got, c.want)
		}
	}
}

// ── B3: the sweep against a cached-board fixture + per-day dedup + kv restart latch ─────────────

func TestR127BNotailSweepAndDedup(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	fresh := time.Now()
	exp := time.Now().Add(5 * time.Hour).UTC().Format(time.RFC3339)
	s.kmkts = map[string]kalshi.Market{
		// broad event (3 siblings), one in the tail band → the ONE expected kalshi row
		"KXR127NT-T1": {Ticker: "KXR127NT-T1", EventTicker: "KXR127NT", Title: "tail runner", Status: "active",
			YesBid: 0.03, YesAsk: 0.05, NoAsk: 0.96, Volume24h: 100, ExpectedExpiration: exp},
		"KXR127NT-T2": {Ticker: "KXR127NT-T2", EventTicker: "KXR127NT", Title: "mid", Status: "active",
			YesBid: 0.48, YesAsk: 0.52, Volume24h: 200},
		"KXR127NT-T3": {Ticker: "KXR127NT-T3", EventTicker: "KXR127NT", Title: "mid2", Status: "active",
			YesBid: 0.40, YesAsk: 0.46, Volume24h: 150},
		// narrow event: tail shape but only 1 sibling → refused (favorite bet, not a tail harvest)
		"KXR127NARROW-T1": {Ticker: "KXR127NARROW-T1", EventTicker: "KXR127NARROW", Title: "narrow tail", Status: "active",
			YesBid: 0.02, YesAsk: 0.04, NoAsk: 0.97, Volume24h: 500},
	}
	s.kmktsAt = map[string]time.Time{
		"KXR127NT-T1": fresh, "KXR127NT-T2": fresh, "KXR127NT-T3": fresh, "KXR127NARROW-T1": fresh,
	}
	// PolyUS: a pregame 3-market event with one tail (expected) + a LIVE tail (skipped: in-play
	// tail prices are game state, not the structural bias).
	s.polyUSMu.Lock()
	s.polyUSMkts = []polyUSMarket{
		{Slug: "r127-tail", EventID: "ev-r127", Game: "AAA @ BBB", TeamName: "AAA", Yes: 0.05, Bid: 0.04, Ask: 0.06, Volume: 50},
		{Slug: "r127-mid", EventID: "ev-r127", Game: "AAA @ BBB", Yes: 0.55, Bid: 0.53, Ask: 0.57, Volume: 60},
		{Slug: "r127-mid2", EventID: "ev-r127", Game: "AAA @ BBB", Yes: 0.40, Bid: 0.38, Ask: 0.42, Volume: 70},
		{Slug: "r127-live-tail", EventID: "ev-r127", Game: "AAA @ BBB", Yes: 0.04, Bid: 0.03, Ask: 0.05, Volume: 90, Live: true},
	}
	s.polyUSMu.Unlock()

	countNotail := func() (n int, rows []storage.SignalRow) {
		t.Helper()
		all, err := s.store.ListSignalsLean(ctx, 200)
		if err != nil {
			t.Fatalf("ListSignalsLean: %v", err)
		}
		for _, r := range all {
			if r.SignalType == "notail" {
				n++
				rows = append(rows, r)
			}
		}
		return
	}

	s.sweepNoTail(ctx)
	n, rows := countNotail()
	if n != 2 {
		t.Fatalf("first sweep logged %d notail rows, want 2 (kalshi tail + polyus pregame tail): %+v", n, rows)
	}
	for _, r := range rows {
		if r.Side != "NO" {
			t.Fatalf("notail rows must be side NO, got %q on %s", r.Side, r.Ticker)
		}
		switch r.Platform {
		case "kalshi":
			if r.Ticker != "KXR127NT-T1" || math.Abs(r.EntryPrice-0.96) > 1e-9 {
				t.Fatalf("kalshi row wrong: %s @ %v (want KXR127NT-T1 @ 0.96 — the venue NO ask)", r.Ticker, r.EntryPrice)
			}
		case "polyus":
			if r.Ticker != "r127-tail" || math.Abs(r.EntryPrice-0.95) > 1e-9 {
				t.Fatalf("polyus row wrong: %s @ %v (want r127-tail @ 0.95 = 1−yes)", r.Ticker, r.EntryPrice)
			}
		default:
			t.Fatalf("unexpected venue %q (poly-int must be skipped)", r.Platform)
		}
	}
	// Second sweep in the same UTC day: the day-latch dedups everything.
	s.sweepNoTail(ctx)
	if n, _ := countNotail(); n != 2 {
		t.Fatalf("second sweep re-logged (n=%d, want 2) — per-day dedup broken", n)
	}
	// Restart simulation: wipe the in-memory set — the kv latch must reload TODAY's keys, so the
	// markers stay consumed (this pins the LATCH itself, independent of InsertSignal's slot dedup).
	s.ntMu.Lock()
	s.ntDay, s.ntSeen = "", nil
	s.ntMu.Unlock()
	if s.notailMarkOnce(ctx, "kalshi|KXR127NT-T1") {
		t.Fatal("restart lost the kv day-latch: an already-logged market marked as fresh")
	}
	if !s.notailMarkOnce(ctx, "kalshi|KXR127NT-NEW") {
		t.Fatal("a genuinely new market must still mark once after the reload")
	}
	if s.notailMarkOnce(ctx, "kalshi|KXR127NT-NEW") {
		t.Fatal("the second mark of the same key in one day must refuse")
	}
}

// ── B4: CLV from a synthetic post_path through the real pipeline ────────────────────────────────

func TestR127BCLVFromSyntheticPostPath(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	mk := func(ticker, side string, entry float64, marks ...float64) {
		t.Helper()
		if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: ticker,
			Title: "T " + ticker, Side: side, SignalType: "r127clv", EntryPrice: entry}); err != nil {
			t.Fatalf("insert %s: %v", ticker, err)
		}
		if err := s.store.UpdateSignalFill(ctx, "kalshi", ticker, side, entry); err != nil {
			t.Fatalf("fill %s: %v", ticker, err)
		}
		for _, m := range marks { // the live breadcrumb writer — marks are side-directional by contract
			if err := s.store.AppendSignalPostPath(ctx, "kalshi", ticker, side, m); err != nil {
				t.Fatalf("post-path %s: %v", ticker, err)
			}
		}
		if err := s.store.ResolveSignals(ctx, ticker, 1); err != nil {
			t.Fatalf("resolve %s: %v", ticker, err)
		}
	}
	mk("KXR127B1", "YES", 0.40, 0.42, 0.50) // clv +0.10 (tail 0.50 − entry 0.40)
	mk("KXR127B2", "YES", 0.40, 0.42, 0.50) // +0.10
	mk("KXR127B3", "YES", 0.40, 0.42, 0.50) // +0.10
	mk("KXR127B4", "YES", 0.40, 0.44)       // single-mark path (no comma) → +0.04
	mk("KXR127B5", "NO", 0.90, 0.95)        // NO marks are already side-directional → +0.05
	// A malformed tail (4-char token — not the writers' %.3f shape) must NULL out, not concatenate
	// digits into a fake price: same family, so a corrupt parse would move the mean.
	if err := s.store.InsertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: "KXR127B6",
		Title: "T bad", Side: "YES", SignalType: "r127clv", EntryPrice: 0.40}); err != nil {
		t.Fatal(err)
	}
	if err := s.store.ResolveSignals(ctx, "KXR127B6", 1); err != nil {
		t.Fatal(err)
	}
	needs, err := s.store.ResolvedNeedPath(ctx, "kalshi", 50)
	if err != nil || len(needs) != 1 || needs[0].Ticker != "KXR127B6" {
		t.Fatalf("backfill queue should hold exactly the pathless row: %v / %+v", err, needs)
	}
	if err := s.store.SetSignalPostPathByID(ctx, needs[0].ID, "0.5,0.62"); err != nil {
		t.Fatal(err)
	}

	fams, err := s.store.FamilyCLVStats(ctx, time.Now().AddDate(0, 0, -90))
	if err != nil {
		t.Fatalf("FamilyCLVStats: %v", err)
	}
	var got *storage.FamilyCLV
	for i := range fams {
		if fams[i].Family == "r127clv" {
			got = &fams[i]
		}
	}
	if got == nil {
		t.Fatalf("family r127clv missing from the CLV card: %+v", fams)
	}
	if got.N != 5 || got.Markets != 5 {
		t.Fatalf("CLV sample = %d rows / %d markets, want 5 / 5 (the malformed 6th tail must NULL out)", got.N, got.Markets)
	}
	wantMean := (0.10 + 0.10 + 0.10 + 0.04 + 0.05) / 5 // = 0.078 $/ct
	if math.Abs(got.Mean-wantMean) > 1e-6 {
		t.Fatalf("mean CLV = %v, want %v", got.Mean, wantMean)
	}
	if got.SD <= 0 {
		t.Fatalf("sd must be positive on a mixed sample, got %v", got.SD)
	}
	// The server card converts to cents and keeps the family visible.
	var ent *clvEnt
	for _, e := range s.computeCLV(ctx) {
		if e.Family == "r127clv" {
			ent = &e
			break
		}
	}
	if ent == nil || math.Abs(ent.MeanC-7.8) > 1e-3 || ent.N != 5 || ent.Markets != 5 || ent.LoC >= ent.HiC {
		t.Fatalf("computeCLV card wrong: %+v (want ≈+7.8¢, m=5, n=5, lo<hi)", ent)
	}
}

// ── C4/C5: knobs ship OFF and the preview is inert ──────────────────────────────────────────────

func TestR127BMoneyKnobsDefaultOffAndInert(t *testing.T) {
	def := config.Default()
	if def.Auto.EVCapitalDayGate || def.Auto.ClusterKellyArm {
		t.Fatalf("money-policy knobs MUST ship OFF (gate=%v arm=%v)", def.Auto.EVCapitalDayGate, def.Auto.ClusterKellyArm)
	}
	if def.Auto.EVCapitalDayMin != 0 || def.Auto.ClusterCapPct != 22 {
		t.Fatalf("defaults wrong: ev_capital_day_min=%v (want 0) cluster_cap_pct=%v (want 22)",
			def.Auto.EVCapitalDayMin, def.Auto.ClusterCapPct)
	}
	if mpClusterCapPct(0) != 22 || mpClusterCapPct(-3) != 22 || mpClusterCapPct(15) != 15 {
		t.Fatal("mpClusterCapPct: ≤0 must read as the documented 22 default")
	}

	s := testServer(t)
	ctx := context.Background()
	auditRows := func() int {
		t.Helper()
		rows, err := s.store.ListAudit(ctx, 100)
		if err != nil {
			t.Fatalf("ListAudit: %v", err)
		}
		n := 0
		for _, r := range rows {
			if r.Category == "moneypolicy" {
				n++
			}
		}
		return n
	}
	// Defaults OFF: any number of previews is a total no-op — zero audit rows, nothing stamped.
	for i := 0; i < 25; i++ {
		s.moneyPolicyPreview(ctx, "polyus", "r127-test-slug", "YES", "auto-ml", 0.50, 10, nil)
	}
	if n := auditRows(); n != 0 {
		t.Fatalf("knobs OFF must write NOTHING, got %d moneypolicy audit rows", n)
	}
	// ARMED (operator's future decision): log-only, one line per 10 min, still no side effects.
	nc := *s.cfg()
	nc.Auto.ClusterKellyArm = true
	nc.Auto.EVCapitalDayGate = true
	s.cfgP.Store(&nc)
	s.moneyPolicyPreview(ctx, "polyus", "r127-test-slug", "YES", "auto-ml", 0.50, 10, nil)
	s.moneyPolicyPreview(ctx, "polyus", "r127-test-slug", "YES", "auto-ml", 0.50, 10, nil) // throttled
	if n := auditRows(); n != 1 {
		t.Fatalf("armed preview must audit exactly once per 10 min, got %d rows", n)
	}
	rows, _ := s.store.ListAudit(ctx, 100)
	for _, r := range rows {
		if r.Category == "moneypolicy" && !strings.Contains(r.Message, "no stake was changed") {
			t.Fatalf("the preview line must declare itself log-only: %q", r.Message)
		}
	}
}

// ── B4: the briefing CLV line shape ─────────────────────────────────────────────────────────────

func TestR127BCLVBriefLineShape(t *testing.T) {
	// Pure formatting: best/worst, n≥20 floor, singleton and empty cases.
	ents := []clvEnt{
		{Family: "xvgap", Plain: "cross-venue gap", N: 64, Markets: 60, MeanC: 3.2},
		{Family: "favlong", Plain: "favorite-long", N: 40, Markets: 38, MeanC: 0.4},
		{Family: "notail", Plain: "notail", N: 7, Markets: 7, MeanC: 9.9}, // under the n≥20 floor — must not appear
		{Family: "freshlist", Plain: "fresh listings", N: 120, Markets: 99, MeanC: -1.7},
	}
	line := clvLine(ents)
	want := "📐 signal CLV (sampled): best cross-venue gap +3.2¢ · n60 🟡 | worst fresh listings -1.7¢ · n99 🟡 · 3 families"
	if line != want {
		t.Fatalf("clvLine =\n%q\nwant\n%q", line, want)
	}
	if got := clvLine(ents[:1]); !strings.Contains(got, "cross-venue gap +3.2¢ · n60 🟡 · 1 family") {
		t.Fatalf("singleton shape wrong: %q", got)
	}
	if clvLine(nil) != "" || clvLine(ents[2:3]) != "" {
		t.Fatal("no measurable family (m≥20) must render NO line at all")
	}
	// The compact phone briefing intentionally omits CLV detail; the dashboard retains it.
	s := testServer(t)
	injectVerdicts(s, []verdictEnt{{Family: "xvgap", Group: "signal", Unit: "$/contract", N: 30, Mean: 0.01, State: "COLLECTING"}})
	s.clvMu.Lock()
	s.clvCache, s.clvAt = ents, time.Now()
	s.clvMu.Unlock()
	txt := s.briefScoreboard(context.Background())
	if strings.Contains(txt, "📐 signal CLV") {
		t.Fatalf("compact briefing must leave CLV detail on the dashboard:\n%s", txt)
	}
}
