package server

// R117 promotion pipeline — unit tests over promoDecide (PURE function: no network, no DB, no
// server). Each case builds one snapshot and asserts the decided transitions.

import (
	"testing"
	"time"
)

func TestR139LegacyRollingPromotionStateCannotActivateExecutor(t *testing.T) {
	s := testServer(t)
	s.promoSt = &promoState{Records: map[string]*promoRecord{
		"legacy": {Family: "legacy", State: promoStatePromoted, AllocUSD: 100},
	}}
	if got := s.promoRecordState("legacy"); got != promoStateWatch {
		t.Fatalf("historical rolling PROVEN+ state remained executable: %q", got)
	}
}

func promoTestCfg() promoCfgT {
	return promoCfgT{Enabled: true, BankrollUSD: 100, // R119: MaxBooks cap removed (unlimited promotions)
		GrowCapUSD: 800, ReserveUSD: 500, DrawdownPausePct: 20}
}

func promoTestExecs() map[string]promoExecutor {
	return map[string]promoExecutor{
		"invert:freshlist": {Name: "freshinv", BookFile: "freshinv_book.json", BookFamily: "book:freshinv"},
	}
}

func provenPlus(fam string, n int) verdictEnt {
	return verdictEnt{Family: fam, Group: "invert", N: n, Mean: 0.09, SD: 0.4, Lo: 0.03, Hi: 0.15, State: "PROVEN+"}
}

// findTrans returns the first transition for a family with the given To state (nil = none).
func findTrans(ts []promoTransition, fam, to string) *promoTransition {
	for i := range ts {
		if ts[i].Family == fam && ts[i].To == to {
			return &ts[i]
		}
	}
	return nil
}

func TestPromoDecidePromoteHappyPath(t *testing.T) {
	// The invert-twin path IS the happy path: invert:freshlist PROVEN+ and bettable promotes on
	// the FIRST sweep (no records exist yet). R118: PROVEN+ alone is the significance gate.
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"invert:freshlist": provenPlus("invert:freshlist", 600)},
		Records:  map[string]promoRecord{}, Reserve: 500}
	ts := promoDecide(sn)
	if findTrans(ts, "invert:freshlist", promoStateWatch) == nil {
		t.Fatalf("expected a first-sighting WATCH transition, got %+v", ts)
	}
	p := findTrans(ts, "invert:freshlist", promoStatePromoted)
	if p == nil {
		t.Fatalf("expected PROMOTED on the first sweep (n>=500 OR-rail), got %+v", ts)
	}
	// R127: promotion moves NO reserve money — AllocDelta 0; NewAllocUSD is the sub's share cap.
	if p.AllocDelta != 0 || p.NewAllocUSD != 100 {
		t.Fatalf("R127: expected zero reserve movement + a $100 share cap, got delta=%.2f alloc=%.2f", p.AllocDelta, p.NewAllocUSD)
	}
}

func TestPromoDecideRails(t *testing.T) {
	base := func() promoSnap {
		return promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
			Verdicts: map[string]verdictEnt{"invert:freshlist": provenPlus("invert:freshlist", 600)},
			Records: map[string]promoRecord{"invert:freshlist": {Family: "invert:freshlist",
				State: promoStateWatch, FirstSeen: time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)}},
			Reserve: 500}
	}
	// R127: the "reserve empty" rail is GONE — promotion moves no reserve money (a sub-strategy
	// share of the venue book). Pin that an empty reserve does NOT block promotion anymore.
	{
		sn := base()
		sn.Reserve = 0
		if p := findTrans(promoDecide(sn), "invert:freshlist", promoStatePromoted); p == nil {
			t.Fatalf("R127: an empty reserve must NOT block promotion (no reserve machinery)")
		}
	}
	// R128: the drawdown-pause rail is GONE (operator: NO pausing — weights do the work). Pin it.
	{
		sn := base()
		sn.DrawdownPct = 25
		if p := findTrans(promoDecide(sn), "invert:freshlist", promoStatePromoted); p == nil {
			t.Fatalf("R128: drawdown must NOT block promotion (no-pause doctrine)")
		}
	}
	cases := []struct {
		name string
		mut  func(*promoSnap)
	}{
		{"venue locked", func(sn *promoSnap) {
			v := sn.Verdicts["invert:freshlist"]
			v.Locked = true
			sn.Verdicts["invert:freshlist"] = v
		}},
		{"not proven", func(sn *promoSnap) {
			v := sn.Verdicts["invert:freshlist"]
			v.State = "COLLECTING"
			sn.Verdicts["invert:freshlist"] = v
		}},
	}
	for _, c := range cases {
		sn := base()
		c.mut(&sn)
		if p := findTrans(promoDecide(sn), "invert:freshlist", promoStatePromoted); p != nil {
			t.Errorf("rail %q: expected NO promotion, got %+v", c.name, *p)
		}
	}
	// Control: the unmutated base DOES promote (so the rail cases prove causation).
	if p := findTrans(promoDecide(base()), "invert:freshlist", promoStatePromoted); p == nil {
		t.Fatalf("control: base snapshot should promote")
	}
}

// TestPromoDecideUnlimitedBooks — R119 pin (operator order): the max-books cap is GONE. A
// PROVEN+ bettable strategy promotes even with many books already promoted, reserve permitting.
func TestPromoDecideUnlimitedBooks(t *testing.T) {
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"invert:freshlist": provenPlus("invert:freshlist", 600)},
		Records: map[string]promoRecord{"invert:freshlist": {Family: "invert:freshlist",
			State: promoStateWatch}},
		Reserve: 500}
	for i := 0; i < 7; i++ {
		f := string(rune('a' + i))
		sn.Records[f] = promoRecord{Family: f, State: promoStatePromoted, AllocUSD: 100}
	}
	if p := findTrans(promoDecide(sn), "invert:freshlist", promoStatePromoted); p == nil {
		t.Fatalf("R119: 7 already-promoted books must NOT block a PROVEN+ promotion")
	}
}

// TestPromoDecideLegacyQueueRecords — R127: the R119 reserve queue is RETIRED. A legacy
// QUEUED-RESERVE record promotes directly while still PROVEN+ (no reserve check) and falls back
// to watch when the verdict lapsed.
func TestPromoDecideLegacyQueueRecords(t *testing.T) {
	mk := func(reserve float64, state, queuedAt, verdictState string) promoSnap {
		v := provenPlus("invert:freshlist", 600)
		v.State = verdictState
		return promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
			Verdicts: map[string]verdictEnt{"invert:freshlist": v},
			Records: map[string]promoRecord{"invert:freshlist": {Family: "invert:freshlist",
				State: state, QueuedAt: queuedAt}},
			Reserve: reserve}
	}
	// 1) legacy queued record, still PROVEN+ — promotes directly even with $0 reserve.
	ts := promoDecide(mk(0, promoStateQueued, "2026-07-08T00:00:00Z", "PROVEN+"))
	p := findTrans(ts, "invert:freshlist", promoStatePromoted)
	if p == nil || p.AllocDelta != 0 {
		t.Fatalf("legacy queued + PROVEN+: must promote with zero reserve movement, got %+v", ts)
	}
	// 2) verdict lapsed while queued: queued → watch (unchanged).
	ts = promoDecide(mk(500, promoStateQueued, "2026-07-08T00:00:00Z", "COLLECTING"))
	if findTrans(ts, "invert:freshlist", promoStateWatch) == nil {
		t.Fatalf("lapsed verdict: queued strategy must return to watch, got %+v", ts)
	}
	// 3) fresh watch record with an empty reserve promotes immediately — nothing ever queues now.
	ts = promoDecide(mk(0, promoStateWatch, "", "PROVEN+"))
	if findTrans(ts, "invert:freshlist", promoStateQueued) != nil {
		t.Fatalf("R127: nothing may enter QUEUED-RESERVE anymore, got %+v", ts)
	}
	if findTrans(ts, "invert:freshlist", promoStatePromoted) == nil {
		t.Fatalf("R127: empty reserve must not block promotion, got %+v", ts)
	}
}

// TestPromoDecideSignificanceOnly — R118 pin (operator order "7 days?!?! make it significance"):
// a PROVEN+ family with n=483 rows and only ~5.7 days of data PROMOTES. The old min-rows(500)
// and min-days(7d) rails are gone — the always-valid CS excluding zero IS the eligibility.
func TestPromoDecideSignificanceOnly(t *testing.T) {
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"invert:freshlist": provenPlus("invert:freshlist", 483)},
		Records: map[string]promoRecord{"invert:freshlist": {Family: "invert:freshlist",
			State: promoStateWatch, FirstSeen: time.Now().Add(-137 * time.Hour).UTC().Format(time.RFC3339)}}, // ~5.7d
		Reserve: 500}
	p := findTrans(promoDecide(sn), "invert:freshlist", promoStatePromoted)
	if p == nil {
		t.Fatalf("PROVEN+ with n=483 and 5.7d of data must promote under the R118 significance-only gate")
	}
	if p.AllocDelta != 0 || p.NewAllocUSD != 100 {
		t.Fatalf("R127: expected a $100 share cap with zero reserve movement, got %+v", *p)
	}
}

func TestPromoDecideNoExecutorWatchOnly(t *testing.T) {
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"xvgap": provenPlus("xvgap", 900)},
		Records:  map[string]promoRecord{}, Reserve: 500}
	ts := promoDecide(sn)
	if findTrans(ts, "xvgap", promoStateNoExec) == nil {
		t.Fatalf("expected ELIGIBLE-NO-EXECUTOR sighting, got %+v", ts)
	}
	if findTrans(ts, "xvgap", promoStatePromoted) != nil {
		t.Fatalf("a family without an executor must never promote")
	}
}

func TestPromoDecideBookFamiliesExcluded(t *testing.T) {
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"book:freshinv": provenPlus("book:freshinv", 900)},
		Records:  map[string]promoRecord{}, Reserve: 500}
	if ts := promoDecide(sn); len(ts) != 0 {
		t.Fatalf("book:* families are executor-internal — no transitions expected, got %+v", ts)
	}
}

func TestPromoDecideGrowAtCheckpoint(t *testing.T) {
	rec := promoRecord{Family: "invert:freshlist", State: promoStatePromoted, AllocUSD: 100,
		FirstSeen: time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)}
	bookV := provenPlus("book:freshinv", 120)
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"invert:freshlist": provenPlus("invert:freshlist", 600), "book:freshinv": bookV},
		Records:  map[string]promoRecord{"invert:freshlist": rec}, Reserve: 400,
		Books: map[string]promoBookStats{"invert:freshlist": {SettledN: 120, OpenLots: 3, BankNet: 105}}}
	g := findTrans(promoDecide(sn), "invert:freshlist", promoStateGrown)
	if g == nil {
		t.Fatalf("expected GROW at the first checkpoint (settled 120 >= initial %d)", promoGrowInitialN)
	}
	if g.AllocDelta != 0 || g.NewAllocUSD != 200 || g.CheckpointN != 120 {
		t.Fatalf("R127: expected share-cap doubling 100→200 at n=120 (no reserve movement), got %+v", *g)
	}
	// Below the NEXT checkpoint (last=120 → need 240): no grow.
	rec.State, rec.LastCheckpointN = promoStateGrown, 120
	sn.Records["invert:freshlist"] = rec
	sn.Books["invert:freshlist"] = promoBookStats{SettledN: 200, OpenLots: 3, BankNet: 210}
	if g := findTrans(promoDecide(sn), "invert:freshlist", promoStateGrown); g != nil {
		t.Fatalf("no grow before 2x the last checkpoint, got %+v", *g)
	}
	// Grow cap: alloc 500, cap 800 → the share cap clamps at 800 (delta limited to 300).
	rec.AllocUSD, rec.LastCheckpointN = 500, 120
	sn.Records["invert:freshlist"] = rec
	sn.Books["invert:freshlist"] = promoBookStats{SettledN: 260, OpenLots: 3, BankNet: 520}
	g = findTrans(promoDecide(sn), "invert:freshlist", promoStateGrown)
	if g == nil || g.NewAllocUSD != 800 {
		t.Fatalf("grow must clamp the share cap at $800, got %+v", g)
	}
	// R128: drawdown does NOT block grows anymore (no-pause doctrine — weights do the work).
	sn.DrawdownPct = 30
	if g := findTrans(promoDecide(sn), "invert:freshlist", promoStateGrown); g == nil {
		t.Fatalf("R128: drawdown must not block the journal grow")
	}
}

func TestPromoDecideRetireAndRefund(t *testing.T) {
	rec := promoRecord{Family: "invert:freshlist", State: promoStateGrown, AllocUSD: 200, LastCheckpointN: 120}
	bookV := verdictEnt{Family: "book:freshinv", Group: "book", N: 300, Mean: -0.08, SD: 0.4, Lo: -0.13, Hi: -0.03, State: "PROVEN-"}
	sn := promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
		Verdicts: map[string]verdictEnt{"invert:freshlist": provenPlus("invert:freshlist", 900), "book:freshinv": bookV},
		Records:  map[string]promoRecord{"invert:freshlist": rec}, Reserve: 300,
		Books: map[string]promoBookStats{"invert:freshlist": {SettledN: 300, OpenLots: 5, BankNet: 160}}}
	ts := promoDecide(sn)
	// R128 (no-pause doctrine): a PROVEN− book no longer auto-RETIRES — the scoreboard weight
	// drives its share to ~0 instead (placement never pauses, logging never stops). It must not
	// grow either (grow requires the book's own PROVEN+).
	if findTrans(ts, "invert:freshlist", promoStateRetired) != nil {
		t.Fatalf("R128: a PROVEN- book must NOT auto-retire (weight ~0 does the work), got %+v", ts)
	}
	if findTrans(ts, "invert:freshlist", promoStateGrown) != nil {
		t.Fatalf("a PROVEN- book must not grow")
	}
	// Wind-down not complete (open lots remain): no completion latch yet.
	rec.State, rec.RetiredAt = promoStateRetired, "2026-07-08T00:00:00Z"
	sn.Records["invert:freshlist"] = rec
	sn.Verdicts["book:freshinv"] = bookV
	ts = promoDecide(sn)
	for _, tr := range ts {
		if tr.Family == "invert:freshlist" && tr.From == promoStateRetired {
			t.Fatalf("no wind-down completion while lots are still open, got %+v", tr)
		}
	}
	// Full wind-down: the RETIRED→RETIRED completion latch fires (R127: share released, no
	// reserve money moves — AllocDelta stays 0).
	sn.Books["invert:freshlist"] = promoBookStats{SettledN: 305, OpenLots: 0, BankNet: 142.50}
	ts = promoDecide(sn)
	var done *promoTransition
	for i := range ts {
		if ts[i].Family == "invert:freshlist" && ts[i].From == promoStateRetired && ts[i].To == promoStateRetired {
			done = &ts[i]
		}
	}
	if done == nil || done.AllocDelta != 0 || done.NewAllocUSD != 0 {
		t.Fatalf("expected the wind-down completion latch (share released, zero money moved), got %+v", done)
	}
	// Latched records stay terminal — no second completion transition.
	rec.Refunded = true
	sn.Records["invert:freshlist"] = rec
	for _, tr := range promoDecide(sn) {
		if tr.Family == "invert:freshlist" {
			t.Fatalf("latched retired record must be terminal, got %+v", tr)
		}
	}
}

// TestPromoDecideDemotionToWatch — R128 REWRITE (operator no-pause doctrine): the R127 D4
// demote-to-WATCH is GONE. A verdict lapse causes NO state change (the scoreboard weight drives
// the share toward 0 instead); parked WATCH records migrate back to PROMOTED unconditionally.
func TestPromoDecideDemotionToWatch(t *testing.T) {
	rec := promoRecord{Family: "invert:freshlist", State: promoStatePromoted, AllocUSD: 100}
	bookOK := provenPlus("book:freshinv", 50)
	mk := func(verds map[string]verdictEnt) promoSnap {
		return promoSnap{Now: time.Now(), Cfg: promoTestCfg(), Executors: promoTestExecs(),
			Verdicts: verds, Records: map[string]promoRecord{"invert:freshlist": rec},
			Books: map[string]promoBookStats{"invert:freshlist": {SettledN: 50, OpenLots: 2, BankNet: 100}}}
	}
	// 1) family decayed to COLLECTING → NO WATCH transition (stays PROMOTED; weight governs).
	lapsed := provenPlus("invert:freshlist", 600)
	lapsed.State = "COLLECTING"
	if w := findTrans(promoDecide(mk(map[string]verdictEnt{"invert:freshlist": lapsed, "book:freshinv": bookOK})),
		"invert:freshlist", promoStateWatchD4); w != nil {
		t.Fatalf("R128: a family lapse must NOT demote to WATCH (no-pause doctrine), got %+v", w)
	}
	// 2) family MISSING from a non-empty verdict set → no WATCH either.
	if w := findTrans(promoDecide(mk(map[string]verdictEnt{"book:freshinv": bookOK})),
		"invert:freshlist", promoStateWatchD4); w != nil {
		t.Fatalf("R128: a vanished family must NOT demote to WATCH, got %+v", w)
	}
	// 3) parked WATCH record → migrates back to PROMOTED unconditionally (even while lapsed).
	rec.State = promoStateWatchD4
	back := findTrans(promoDecide(mk(map[string]verdictEnt{"invert:freshlist": lapsed, "book:freshinv": bookOK})),
		"invert:freshlist", promoStatePromoted)
	if back == nil || back.NewAllocUSD != 100 || back.AllocDelta != 0 {
		t.Fatalf("R128: WATCH must migrate back to PROMOTED unconditionally (share retained), got %+v", back)
	}
	// 4) WATCH + the book's OWN fills PROVEN− → STILL migrates to PROMOTED, never RETIRED
	// (weight ~0 replaces the wind-down; logging and settlement continue).
	bookBad := verdictEnt{Family: "book:freshinv", Group: "book", N: 300, Mean: -0.08, SD: 0.4, Lo: -0.13, Hi: -0.03, State: "PROVEN-"}
	ts := promoDecide(mk(map[string]verdictEnt{"invert:freshlist": lapsed, "book:freshinv": bookBad}))
	if findTrans(ts, "invert:freshlist", promoStateRetired) != nil {
		t.Fatalf("R128: no auto-retire from WATCH — weight does the work: %+v", ts)
	}
	if findTrans(ts, "invert:freshlist", promoStatePromoted) == nil {
		t.Fatalf("R128: WATCH must still migrate to PROMOTED: %+v", ts)
	}
}
