package server

// r107_xvident_test.go — R107: structural cross-venue identity (xvident.go) + markettree warm
// boot / full-universe fold. Synthetic-only: no network, no DB, no feeds — every ticker/slug
// below is a REAL shape sampled from the market_catalog DB copy or live gamma on 2026-07-07.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// xvTestReset gives each test a clean registry (identity maps only — serve counters are
// lifetime by design) and restores nothing: every test starts from reset, so ordering is moot.
func xvTestReset() { xvReg.reset() }

// ── TASK A: crypto ──────────────────────────────────────────────────────────────────────────────

func TestR107CryptoThresholdJoin(t *testing.T) {
	xvTestReset()
	// Kalshi daily threshold: Jul 10 12pm EDT observation = 16:00Z; T55999.99 ⇒ "$56,000 or above"
	kKey, kind, ok := xvKalshiAnchor("KXBTCD-26JUL1012-T55999.99")
	if !ok || kind != "crypto" {
		t.Fatalf("kalshi thresh anchor failed: %q %q %v", kKey, kind, ok)
	}
	want := "crypto|BTC|2026-07-10T16:00:00Z|T56000|up"
	if kKey != want {
		t.Fatalf("kalshi key = %q, want %q", kKey, want)
	}
	// Poly twin (real slug shape): noon-ET close rides the venue endDate
	pKey, kind2, ok2 := xvPolyAnchor("bitcoin-above-56k-on-july-10-2026", "2026-07-10T16:00:00Z")
	if !ok2 || kind2 != "crypto" || pKey != kKey {
		t.Fatalf("poly above anchor = %q (%v), want %q", pKey, ok2, kKey)
	}
	xvReg.add("kalshi", "KXBTCD-26JUL1012-T55999.99", kKey, "crypto")
	xvReg.add("polymarket", "bitcoin-above-56k-on-july-10-2026", pKey, "crypto")
	v, id, kd, okT := xvTwin("polymarket", "bitcoin-above-56k-on-july-10-2026")
	if !okT || v != "kalshi" || id != "KXBTCD-26JUL1012-T55999.99" || kd != "crypto" {
		t.Fatalf("poly→kalshi twin = %q %q %q %v", v, id, kd, okT)
	}
	if v, id, _, okT := xvTwin("kalshi", "KXBTCD-26JUL1012-T55999.99"); !okT || v != "polymarket" || id != "bitcoin-above-56k-on-july-10-2026" {
		t.Fatalf("kalshi→poly twin = %q %q %v", v, id, okT)
	}
	// sub-dollar strike normalization: XRP T0.5999 ⇒ 0.6 == poly "0pt6"
	kx, _, _ := xvKalshiAnchor("KXXRPD-26JUL0812-T0.5999")
	px, _, okX := xvPolyAnchor("xrp-above-0pt6-on-july-8-2026", "2026-07-08T16:00:00Z")
	if !okX || kx != px {
		t.Fatalf("xrp strike normalization: kalshi %q vs poly %q", kx, px)
	}
}

func TestR107CryptoMismatchNoJoin(t *testing.T) {
	xvTestReset()
	kKey, _, _ := xvKalshiAnchor("KXBTCD-26JUL1012-T55999.99") // Jul 10 window
	// window mismatch: same strike, next day
	pKey, _, ok := xvPolyAnchor("bitcoin-above-56k-on-july-11-2026", "2026-07-11T16:00:00Z")
	if !ok || pKey == kKey {
		t.Fatalf("window mismatch must anchor to a DIFFERENT key: %q vs %q", pKey, kKey)
	}
	// strike mismatch: same window, different rung
	k58, _, _ := xvKalshiAnchor("KXBTCD-26JUL1012-T57999.99")
	if k58 == kKey {
		t.Fatalf("strike mismatch collapsed onto one key: %q", k58)
	}
	xvReg.add("kalshi", "KXBTCD-26JUL1012-T55999.99", kKey, "crypto")
	xvReg.add("polymarket", "bitcoin-above-56k-on-july-11-2026", pKey, "crypto")
	if _, _, _, okT := xvTwin("polymarket", "bitcoin-above-56k-on-july-11-2026"); okT {
		t.Fatal("window-mismatched rows must not twin")
	}
	// a slug whose venue endDate contradicts its own date is refused outright
	if _, _, ok := xvPolyAnchor("bitcoin-above-56k-on-july-10-2026", "2026-07-11T16:00:00Z"); ok {
		t.Fatal("endDate/slug-date disagreement must refuse the anchor")
	}
}

func TestR107Updown15mJoin(t *testing.T) {
	xvTestReset()
	// Kalshi 15-min up/down: embedded 12:15 ET = window END 16:15Z (close probe: 16:20Z = +5min)
	kKey, kind, ok := xvKalshiAnchor("KXBTC15M-26JUL071215-15")
	if !ok || kind != "crypto" || kKey != "crypto|BTC|2026-07-07T16:15:00Z|updown15m|up" {
		t.Fatalf("kalshi 15m key = %q (%v)", kKey, ok)
	}
	// Poly 15m: slug epoch 1783440000 = 2026-07-07T16:00:00Z window START; end = +15m
	pKey, _, ok2 := xvPolyAnchor("btc-updown-15m-1783440000", "")
	if !ok2 || pKey != kKey {
		t.Fatalf("poly 15m key = %q, want %q", pKey, kKey)
	}
	xvReg.add("kalshi", "KXBTC15M-26JUL071215-15", kKey, "crypto")
	xvReg.add("polymarket", "btc-updown-15m-1783440000", pKey, "crypto")
	// R142: retained settlement evidence showed repeated same-side payouts across venues. Keep
	// both exact anchors, but the apparent 15-minute twin is no longer lock-safe.
	if _, id, _, okT := xvTwin("kalshi", "KXBTC15M-26JUL071215-15"); okT {
		t.Fatalf("empirically incompatible 15m pair escaped quarantine as %q", id)
	}
	// 5m family anchors (Kalshi lists no 5m series → no twin, but the anchor must parse)
	p5, _, ok5 := xvPolyAnchor("btc-updown-5m-1783442700", "")
	if !ok5 || p5 != "crypto|BTC|2026-07-07T16:50:00Z|updown5m|up" {
		t.Fatalf("poly 5m key = %q (%v)", p5, ok5)
	}
	// hourly window: slug hour = START; 12pm-et ends 17:00Z (live gamma probe)
	ph, _, okH := xvPolyAnchor("bitcoin-up-or-down-july-7-2026-12pm-et", "2026-07-07T17:00:00Z")
	if !okH || ph != "crypto|BTC|2026-07-07T17:00:00Z|updown1h|up" {
		t.Fatalf("poly hourly key = %q (%v)", ph, okH)
	}
}

// R118: xvAnchorPolyLive registers a deterministically-fetched current-window poly market
// (slug + conditionId alias) so xvTwin serves it even though the top-volume rebuild feed
// never holds the live window (the R117 joined_keys=0 root cause).
func TestR118AnchorPolyLive(t *testing.T) {
	xvTestReset()
	kKey, _, ok := xvKalshiAnchor("KXBTCD-26JUL1012-T55999.99")
	if !ok {
		t.Fatal("kalshi anchor")
	}
	xvReg.add("kalshi", "KXBTCD-26JUL1012-T55999.99", kKey, "crypto")
	// A currently lock-eligible threshold market registered through the live helper resolves by
	// condition ID. The helper itself does not override predicate-level safety exclusions.
	xvAnchorPolyLive("bitcoin-above-56k-on-july-10-2026", "2026-07-10T16:00:00Z", "0xCOND118")
	if v, id, kind, okT := xvTwin("polymarket", "0xCOND118"); !okT || v != "kalshi" || kind != "crypto" || id != "KXBTCD-26JUL1012-T55999.99" {
		t.Fatalf("live-anchored twin = %q %q %q %v", v, id, kind, okT)
	}
	// garbage slug refuses (same strict grammar — no fabricated join)
	xvAnchorPolyLive("btc-moon-lambo", "", "0xNOPE")
	if _, _, _, okT := xvTwin("polymarket", "0xNOPE"); okT {
		t.Fatal("non-anchorable slug must not register")
	}
}

// ── TASK A: weather ─────────────────────────────────────────────────────────────────────────────

func TestR107WeatherKeys(t *testing.T) {
	xvTestReset()
	// Kalshi 2°F bin B84.5 and Poly-int expose the same visible bucket shape, so both stay
	// structurally anchored for coverage. They are not member twins: Kalshi settles from the NWS
	// Climatological Report while Poly-int settles from Wunderground.
	kKey, kind, ok := xvKalshiAnchor("KXHIGHMIA-26JUL08-B84.5")
	if !ok || kind != "wx" || kKey != "wx|MIA|high|2026-07-08|84-85F" {
		t.Fatalf("kalshi wx key = %q (%v)", kKey, ok)
	}
	pKey, _, ok2 := xvPolyAnchor("highest-temperature-in-miami-on-july-8-2026-84-85f", "2026-07-08T12:00:00Z")
	if !ok2 || pKey != kKey {
		t.Fatalf("poly wx key = %q, want %q", pKey, kKey)
	}
	xvReg.add("kalshi", "KXHIGHMIA-26JUL08-B84.5", kKey, "wx")
	xvReg.add("polymarket", "highest-temperature-in-miami-on-july-8-2026-84-85f", pKey, "wx")
	if _, _, _, okT := xvTwin("kalshi", "KXHIGHMIA-26JUL08-B84.5"); okT {
		t.Fatal("same-shape weather buckets with different settlement authorities must not twin")
	}
	// KXLOWT* vintage + nyc city alias
	if k, _, ok := xvKalshiAnchor("KXLOWTSEA-26JUL08-B55.5"); !ok || k != "wx|SEA|low|2026-07-08|55-56F" {
		t.Fatalf("KXLOWTSEA key = %q (%v)", k, ok)
	}
	if p, _, ok := xvPolyAnchor("highest-temperature-in-nyc-on-july-8-2026-72-73f", "2026-07-08T12:00:00Z"); !ok || p != "wx|NYC|high|2026-07-08|72-73F" {
		t.Fatalf("nyc station map = %q (%v)", p, ok)
	}
	// international °C city: anchors verbatim (own node) — never a Kalshi twin
	lKey, _, okL := xvPolyAnchor("highest-temperature-in-london-on-july-9-2026-30corbelow", "2026-07-09T12:00:00Z")
	if !okL || lKey != "wx|LONDON|high|2026-07-09|T30C-below" {
		t.Fatalf("london key = %q (%v)", lKey, okL)
	}
	// tails NEVER join: Kalshi's tail direction lives in the subtitle, not the ticker
	kTail, _, okKT := xvKalshiAnchor("KXHIGHAUS-26JUL08-T95")
	pTail, _, okPT := xvPolyAnchor("highest-temperature-in-austin-on-july-8-2026-95forbelow", "2026-07-08T12:00:00Z")
	if !okKT || !okPT {
		t.Fatalf("tails must still anchor: %v %v", okKT, okPT)
	}
	if kTail == pTail {
		t.Fatalf("direction-blind Kalshi tail must NOT equal poly's directed tail: %q", kTail)
	}
}

// ── TASK A: econ + politics ─────────────────────────────────────────────────────────────────────

func TestR107EconKeys(t *testing.T) {
	xvTestReset()
	// CPI YoY: Kalshi period from the event seg; poly period month from the slug + year from endDate
	kKey, kind, ok := xvKalshiAnchor("KXCPIYOY-26JUN-T3.9")
	if !ok || kind != "econ" || kKey != "econ|us-cpi-yoy|2026-06" {
		t.Fatalf("KXCPIYOY key = %q (%v)", kKey, ok)
	}
	pKey, _, ok2 := xvPolyAnchor("will-annual-inflation-be-3pt9-in-june-20260610151731661", "2026-07-15T03:59:00Z")
	if !ok2 || pKey != kKey {
		t.Fatalf("poly inflation key = %q, want %q", pKey, kKey)
	}
	// fed decision family → cb-us at month grain (both bps and no-change shapes)
	f1, _, okF1 := xvPolyAnchor("will-the-fed-decrease-interest-rates-by-25-bps-after-the-july-2026-meeting", "2026-07-29T00:00:00Z")
	f2, _, okF2 := xvPolyAnchor("will-there-be-no-change-in-fed-interest-rates-after-the-july-2026-meeting", "2026-07-29T00:00:00Z")
	if !okF1 || !okF2 || f1 != "econ|cb-us|2026-07" || f2 != f1 {
		t.Fatalf("fed keys = %q / %q (%v %v)", f1, f2, okF1, okF2)
	}
	// other CB + country-prefixed unemployment event segs
	if k, _, ok := xvKalshiAnchor("KXCBDECISIONEU-26JUL23-H25"); !ok || k != "econ|cb-eu|2026-07" {
		t.Fatalf("KXCBDECISIONEU key = %q (%v)", k, ok)
	}
	if k, _, ok := xvKalshiAnchor("KXUE-AUS26JUN-4.0"); !ok || k != "econ|unemployment-aus|2026-06" {
		t.Fatalf("KXUE key = %q (%v)", k, ok)
	}
	// econ is LADDER-level identity: rungs join the key for coverage, but member twins are
	// REFUSED even 1:1 — "above 3.9%" and "3.9% exact" are different contracts.
	xvReg.add("kalshi", "KXCPIYOY-26JUN-T3.9", kKey, "econ")
	xvReg.add("polymarket", "will-annual-inflation-be-3pt9-in-june-20260610151731661", pKey, "econ")
	if _, _, _, okT := xvTwin("polymarket", "will-annual-inflation-be-3pt9-in-june-20260610151731661"); okT {
		t.Fatal("econ ladder keys must never claim member-level twins")
	}
	// December period reported in January backs the year up
	if p, _, ok := xvPolyAnchor("will-annual-inflation-be-2pt9-in-december", "2027-01-15T03:59:00Z"); !ok || p != "econ|us-cpi-yoy|2026-12" {
		t.Fatalf("december year-backup key = %q (%v)", p, ok)
	}
	// politics: conservative anchor-only grammars
	if p, kd, ok := xvPolyAnchor("next-prime-minister-of-italy", "2026-12-31T00:00:00Z"); !ok || kd != "pol" || p != "pol|pm-italy|2026" {
		t.Fatalf("pm-italy key = %q %q (%v)", p, kd, ok)
	}
	if p, _, ok := xvPolyAnchor("alaska-governor-election-winner", "2026-11-04T00:00:00Z"); !ok || p != "pol|gov-alaska|2026" {
		t.Fatalf("governor key = %q (%v)", p, ok)
	}
}

// ── TASK A: strict-grammar refusals + counters ──────────────────────────────────────────────────

func TestR107NonsenseRefused(t *testing.T) {
	for _, slug := range []string{
		"bitcoin-to-the-moon-lol",                      // no grammar
		"will-btc-pump-tomorrow",                       // no grammar
		"highest-temperature-in-london-on-july-9-2026", // EVENT slug (no bin) — not a market anchor
		"mlb-col-lad-2026-07-06",                       // sports — gameident's domain, not xvident's
		"fed-decision-in-july-181",                     // event slug, not a market slug
		"bitcoin-up-or-down",                           // dateless fragment
	} {
		if key, kind, ok := xvPolyAnchor(slug, "2026-07-10T16:00:00Z"); ok {
			t.Fatalf("nonsense slug %q anchored to %q (%s)", slug, key, kind)
		}
	}
	for _, tk := range []string{
		"KXMLBGAME-26JUL062210COLLAD-COL", // sports
		"KXETH-26JUL0713-T1030",           // range-series tail: direction not in the ticker — refuse
		"KXBTCD-26JUL07-T55999.99",        // day-only event seg on an hourly threshold series
		"KXBTCD-26JUL0713-B62550",         // B-suffix on a T-threshold series
		"KXWHATNOT-26JUL08-T5",            // unknown series
	} {
		if key, kind, ok := xvKalshiAnchor(tk); ok {
			t.Fatalf("ticker %q anchored to %q (%s)", tk, key, kind)
		}
	}
}

func TestR107Counters(t *testing.T) {
	xvTestReset()
	xvReg.mu.Lock()
	s0, f0 := xvReg.structHits["int-bridge"], xvReg.fuzzHits["int-bridge"]
	xvReg.mu.Unlock()
	xvNoteStruct("int-bridge")
	xvNoteStruct("int-bridge")
	xvNoteFuzz("int-bridge")
	xvReg.mu.Lock()
	s1, f1 := xvReg.structHits["int-bridge"], xvReg.fuzzHits["int-bridge"]
	xvReg.mu.Unlock()
	if s1-s0 != 2 || f1-f0 != 1 {
		t.Fatalf("counter deltas struct=%d fuzz=%d, want 2/1", s1-s0, f1-f0)
	}
}

// ── TASK B: markettree warm boot + catalog fold ─────────────────────────────────────────────────

func TestR107SeriesCatFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, mtSeriesCatFile)
	cats := map[string]string{"KXCPIYOY": "Economics", "KXBTCD": "Crypto", "KXHIGHMIA": "Climate and Weather"}
	at := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	if err := mtWriteSeriesFile(path, cats, at); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, gotAt, ok := mtReadSeriesFile(path)
	if !ok || !gotAt.Equal(at) || len(got) != len(cats) {
		t.Fatalf("read: ok=%v at=%v n=%d", ok, gotAt, len(got))
	}
	for k, v := range cats {
		if got[k] != v {
			t.Fatalf("round-trip lost %s=%s (got %q)", k, v, got[k])
		}
	}
	// an empty map must never clobber a good file
	if err := mtWriteSeriesFile(path, map[string]string{}, at); err == nil {
		t.Fatal("empty-map write must refuse")
	}
	if got2, _, ok2 := mtReadSeriesFile(path); !ok2 || len(got2) != len(cats) {
		t.Fatalf("good file damaged by refused write: ok=%v n=%d", ok2, len(got2))
	}
	// corrupt + missing files read as not-ok
	if err := os.WriteFile(filepath.Join(dir, "bad.json"), []byte("{oops"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := mtReadSeriesFile(filepath.Join(dir, "bad.json")); ok {
		t.Fatal("corrupt file must read as not-ok")
	}
	if _, _, ok := mtReadSeriesFile(filepath.Join(dir, "missing.json")); ok {
		t.Fatal("missing file must read as not-ok")
	}
}

func TestR107CatalogFoldDedupe(t *testing.T) {
	s := &Server{}
	cats := map[string]string{"KXBTCD": "Crypto"}
	b := newMtBuilder()
	// live row first (prices) — the /api/markets cache path
	live := mtMember{Venue: "kalshi", ID: "KXBTCD-26JUL0713-T64999.99", Title: "BTC — $65,000 or above", YesBid: 0.4, YesAsk: 0.42}
	if !b.add("Crypto", "KXBTCD-26JUL0713", "Bitcoin price at 1pm", "K", "KXBTCD", live, true) {
		t.Fatal("live add refused")
	}
	// catalog fold of the SAME market: live row wins, nothing double-counted
	eventKey := "KXBTCD-" + "26JUL0713"
	dup := storage.CatalogRow{Venue: "kalshi", Ticker: "KXBTCD-26JUL0713-T64999.99", EventKey: eventKey, Title: "dup"}
	if s.mtFoldCatalogRow(b, "kalshi", dup, cats) {
		t.Fatal("catalog fold of a live row must be a no-op (live wins)")
	}
	// catalog-only sibling: counted in the universe, not in live; title+url shell, no prices
	extra := storage.CatalogRow{Venue: "kalshi", Ticker: "KXBTCD-26JUL0713-T69999.99", EventKey: eventKey, Title: "BTC — $70,000 or above", Kind: "winner"}
	if !s.mtFoldCatalogRow(b, "kalshi", extra, cats) {
		t.Fatal("catalog fold of a new row refused")
	}
	n := b.genres["Crypto"]
	if n == nil || n.n != 1 || n.nu != 2 {
		t.Fatalf("crypto counts live=%v universe=%v, want 1/2", n.n, n.nu)
	}
	ev := n.events["K|KXBTCD-26JUL0713"]
	if ev == nil || ev.N != 1 || ev.NUniverse != 2 {
		t.Fatalf("event counts N=%v NU=%v, want 1/2", ev.N, ev.NUniverse)
	}
	mem := n.members["K|KXBTCD-26JUL0713"]
	if len(mem) != 2 {
		t.Fatalf("members = %d, want 2", len(mem))
	}
	for _, m := range mem {
		if m.ID == extra.Ticker && (m.YesBid != 0 || m.YesAsk != 0 || m.URL == "") {
			t.Fatalf("catalog-only member must be a priceless title+url shell: %+v", m)
		}
	}
	// unknowable genre is COUNTED under Other/Unknown (poly catalog rows carry no tag)…
	pRow := storage.CatalogRow{Venue: "polymarket", Ticker: "0xabc123", EventKey: "some-random-event", Title: "Mystery?"}
	if !s.mtFoldCatalogRow(b, "polymarket", pRow, cats) || b.genres["Other/Unknown"] == nil || b.genres["Other/Unknown"].nu != 1 {
		t.Fatal("unknowable-genre catalog row must land counted under Other/Unknown")
	}
	// …unless a LIVE sibling already resolved the event's genre — the fold adopts it
	b2 := newMtBuilder()
	b2.add("Crypto", "bitcoin-above-on-july-10-2026", "bitcoin-above-on-july-10-2026", "I", "",
		mtMember{Venue: "polymarket", ID: "0xlive", Title: "Will BTC be above 56k?"}, true)
	sib := storage.CatalogRow{Venue: "polymarket", Ticker: "0xfolded", EventKey: "bitcoin-above-on-july-10-2026", Title: "Will BTC be above 58k?"}
	if !s.mtFoldCatalogRow(b2, "polymarket", sib, cats) {
		t.Fatal("sibling fold refused")
	}
	if cn := b2.genres["Crypto"]; cn == nil || cn.nu != 2 || b2.genres["Other/Unknown"] != nil {
		t.Fatal("catalog fold must adopt the live sibling's event genre")
	}
}

func TestR107GenreFromSeriesPrefix(t *testing.T) {
	s := &Server{}
	cats := map[string]string{"KXCPIYOY": "Economics", "KXBTCD": "Crypto"}
	if g := s.mtGenreOfSeries("KXCPIYOY", cats); g != "Economics" {
		t.Fatalf("KXCPIYOY → %q", g)
	}
	if g := s.mtGenreOfSeries("kxbtcd", cats); g != "Crypto" { // case-normalized
		t.Fatalf("kxbtcd → %q", g)
	}
	// sports fallback works with an EMPTY category map (the pre-warm boot window)
	if g := s.mtGenreOfSeries("KXMLBSPREAD", map[string]string{}); g != "Sports" {
		t.Fatalf("KXMLBSPREAD cold → %q", g)
	}
	if g := s.mtGenreOfSeries("KXWHATNOT", map[string]string{}); g != "Other/Unknown" {
		t.Fatalf("KXWHATNOT → %q", g)
	}
}
