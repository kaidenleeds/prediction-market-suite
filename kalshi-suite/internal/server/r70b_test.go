package server

// R70-B tests (SCHEMA_AUDIT wave 2): the money-touching pieces of the new feature wiring —
// per-series venue fee truth (kalFee override vs legacy fallback, maker semantics, fee-free
// program series), the polyus stale-pregame guard's period clock, and the score parser. All
// hermetic: schedule maps are injected directly, no network.

import (
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/ev"
)

// ── R70-B #2: kalFee — venue schedule override with 0.07/0.0175 legacy fallback ──────────────────

func TestKalFeeFallbackMatchesLegacyModel(t *testing.T) {
	s := testServer(t)
	// No schedule loaded (s.kal == nil → refreshKalFees no-ops): every ticker must price EXACTLY
	// like the legacy model. R106 (auditor bug 253): the legacy model no longer halves index
	// series — the venue ended the S&P/Nasdaq halving 2026-07-03, so the fallback is General
	// everywhere (kalshiFeeClass) and this parity check now covers that too.
	for _, tk := range []string{"KXMLBGAME-26JUL04-X", "KXINX-26JUL04-B6300", "KXBTC15M-26JUL041200-T1"} {
		for _, maker := range []bool{false, true} {
			got := s.kalFee(tk, maker, 100, 0.50)
			want := ev.Fee(kalshiFeeClass(tk), maker, 100, 0.50)
			if got != want {
				t.Fatalf("kalFee(%s, maker=%v) fallback = %v, want legacy %v", tk, maker, got, want)
			}
		}
	}
}

func TestKalFeeScheduleOverride(t *testing.T) {
	s := testServer(t)
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{
		"KXINX":     {taker: 0.07, maker: 0, typ: "quadratic"},                      // probe 2026-07-04: index halving ENDED — mult back to 1, taker-only
		"KXINXY":    {taker: 0.07, maker: 0.0175, typ: "quadratic_with_maker_fees"}, // maker-fee series
		"KXZECPERP": {taker: 0, maker: 0, typ: "quadratic"},                         // multiplier-zero schedule
		"KXFLAT":    {typ: "flat"},                                                  // separate table/formula: unsupported here
	}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()

	// KXINX taker: schedule says multiplier 1 → 0.07 (the pre-R106 legacy IndexSP class WRONGLY
	// halved it whenever the schedule wasn't loaded).
	if got, want := s.kalFee("KXINX-26JUL04-B6300", false, 100, 0.50), ev.FeeCoeff(0.07, 100, 0.50); got != want {
		t.Fatalf("KXINX taker = %v, want venue-truth %v", got, want)
	}
	// R106 (auditor bug 253): the fallback now AGREES with venue truth — kalshiFeeClass routes
	// index tickers to General (the venue ended the halving 2026-07-03), so schedule-loaded and
	// schedule-missing paths price index takers identically. The old assertion ("override must
	// DIFFER from the halved fallback") is obsolete BY DESIGN.
	if legacy := ev.Fee(kalshiFeeClass("KXINX-26JUL04-B6300"), false, 100, 0.50); s.kalFee("KXINX-26JUL04-B6300", false, 100, 0.50) != legacy {
		t.Fatalf("KXINX fallback must now MATCH venue truth (bug 253 fix): kalFee=%v legacy=%v",
			s.kalFee("KXINX-26JUL04-B6300", false, 100, 0.50), legacy)
	}
	// Plain quadratic charges makers NOTHING (venue type vocabulary).
	if got := s.kalFee("KXINX-26JUL04-B6300", true, 100, 0.50); got != 0 {
		t.Fatalf("plain-quadratic maker fee = %v, want 0", got)
	}
	// quadratic_with_maker_fees keeps the 0.0175 maker charge.
	if got, want := s.kalFee("KXINXY-26DEC31-T6500", true, 100, 0.50), ev.FeeCoeff(0.0175, 100, 0.50); got != want {
		t.Fatalf("maker-fee series maker = %v, want %v", got, want)
	}
	// Multiplier-0 program series are fee-free both sides.
	if got := s.kalFee("KXZECPERP-X", false, 100, 0.50); got != 0 {
		t.Fatalf("fee-free program taker = %v, want 0", got)
	}
	if s.kalFeeSupported("KXFLAT-X") || s.kalFee("KXFLAT-X", false, 1, 0.50) != unsupportedKalFee {
		t.Fatal("flat fee type must fail execution/EV closed instead of using the quadratic formula")
	}
	// The endpoint is a change log. Once loaded, an unlisted series uses the venue defaults.
	if got := s.kalFee("KXNBAGAME-26JUL04-X", true, 100, 0.50); got != 0 {
		t.Fatalf("unlisted-series default maker fee = %v, want 0", got)
	}
	if got, want := s.kalFee("KXNBAGAME-26JUL04-X", false, 100, 0.50), ev.FeeCoeff(0.07, 100, 0.50); got != want {
		t.Fatalf("unlisted-series default taker fee = %v, want %v", got, want)
	}
}

func TestFeeCoeffRounding(t *testing.T) {
	// Same de-dust + ceil-to-cent contract as ev.Fee: 0.07·100·0.5·0.5 = $1.75 exactly (not $1.76).
	if got := ev.FeeCoeff(0.07, 100, 0.50); got != 1.75 {
		t.Fatalf("FeeCoeff dust rounding = %v, want 1.75", got)
	}
	if got := ev.FeeCoeff(0, 100, 0.50); got != 0 {
		t.Fatalf("zero coefficient must be fee-free, got %v", got)
	}
	// Venue receipt parity: trade fee is $0.0348, then the balance-alignment component makes
	// the all-in whole-order fee $0.04. Keeping both prevents optimistic P&L projections.
	p := ev.FeeBreakdownCoeff(0.07, 2, 0.46, true)
	if math.Abs(p.Trade-0.0348) > 1e-9 || math.Abs(p.Rounding-0.0052) > 1e-9 || math.Abs(p.Net-0.04) > 1e-9 {
		t.Fatalf("fee breakdown = %+v, want trade=.0348 rounding=.0052 net=.04", p)
	}
}

// ── R70-B #8: polyus game state — features + the stale-pregame guard clock ───────────────────────

func TestParseScoreMargin(t *testing.T) {
	cases := []struct {
		in   string
		want float64
		ok   bool
	}{
		{"101-98", 3, true}, {"3 - 1", 2, true}, {"2:0", 2, true}, {"0-0", 0, true},
		{"", 0, false}, {"NS", 0, false}, {"6-4, 3-2", 0, false}, // tennis set strings don't parse as one margin
	}
	for _, c := range cases {
		got, ok := parseScoreMargin(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Fatalf("parseScoreMargin(%q) = (%v,%v), want (%v,%v)", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestPusStalePregameGuard(t *testing.T) {
	s := testServer(t)
	slug := "mlb-chc-nyy-2026-07-04"
	preGamePred := time.Now().Add(-30 * time.Minute) // model priced this half an hour ago

	// Unknown slug → never stale (the guard only fires on KNOWN in-play games).
	if s.pusStalePregame(slug, preGamePred) {
		t.Fatal("unknown slug must not be stale")
	}
	// Known but PRE-GAME → not stale; in_play must read 0.
	s.pusNoteGameState(slug, false, "", "NS")
	if s.pusStalePregame(slug, preGamePred) {
		t.Fatal("pre-game slug must not be stale")
	}
	if ip, _ := s.pusGameFeats(slug); ip == nil || *ip != 0 {
		t.Fatalf("pre-game in_play = %v, want 0", ip)
	}
	// Game goes LIVE (period flip observed NOW) → the 30-min-old pred is a pre-game prior → STALE.
	s.pusNoteGameState(slug, true, "2-1", "3rd")
	if !s.pusStalePregame(slug, preGamePred) {
		t.Fatal("in-play game must reject a pred older than the observed period start")
	}
	// A pred priced AFTER the flip is fine.
	if s.pusStalePregame(slug, time.Now()) {
		t.Fatal("fresh pred must not be stale")
	}
	// Features: in_play=1, score_margin=|2−1|=1.
	ip, mg := s.pusGameFeats(slug)
	if ip == nil || *ip != 1 {
		t.Fatalf("live in_play = %v, want 1", ip)
	}
	if mg == nil || *mg != 1 {
		t.Fatalf("live score_margin = %v, want 1", mg)
	}
	// Same period re-observed (score ticks) must NOT move the period clock: a pred placed between
	// observations stays valid.
	between := time.Now()
	time.Sleep(2 * time.Millisecond)
	s.pusNoteGameState(slug, true, "3-1", "3rd")
	if s.pusStalePregame(slug, between) {
		t.Fatal("a score change within the same period must not re-arm the guard")
	}
}
