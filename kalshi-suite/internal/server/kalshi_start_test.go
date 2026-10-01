package server

// R66 regression locks for kalshiTickerStart — the ticker-embedded event datetime that fills
// Kalshi's secs_to_start gap (R65 logged NULL there: the API payload has no start field, but the
// sports/crypto TICKERS encode it). The table pins:
//
//	(a) the four known shapes as exact ET wall times: sports HHMM, esports HHMM, crypto DAILY
//	    hour-only (whose strike suffix must be ignored), crypto 15-min HHMM;
//	(b) the ANCHOR: a date-like chunk in a strike/market SUFFIX (not the event segment) and a
//	    date not at the START of the event segment must both fail to parse;
//	(c) no-time tickers (election style) are not-ok;
//	(d) the PLAUSIBILITY GATE: starts >14d ahead or >24h past "now" are rejected as mis-parses.
//
// kalshiTickerStartAt takes the clock, so the gate is deterministic under test. The wiring tests
// then cover sigSecsToStart's kalshi branch: ticker source first, PolyUS-borrowed start second
// (reusing the polyUSPriceForSide join with a seeded polyUSMkts cache — same convention as
// polyusmatch_test.go), NULL when neither fires.

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // the test binary doesn't get main.go's embed — keep America/New_York loadable everywhere
)

func TestKalshiTickerStart(t *testing.T) {
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load America/New_York: %v", err)
	}
	now := time.Date(2026, time.July, 3, 9, 0, 0, 0, et) // fixed clock for the plausibility gate
	cases := []struct {
		name, ticker string
		want         time.Time // zero ⇒ expect not-ok
	}{
		{"sports HHMM", "KXWT20MATCH-26JUL031000NORSUS-NOR",
			time.Date(2026, time.July, 3, 10, 0, 0, 0, et)},
		{"esports HHMM", "KXCS2GAME-26JUL030945ENTSAW-SAW",
			time.Date(2026, time.July, 3, 9, 45, 0, 0, et)},
		{"crypto daily hour-only, strike suffix ignored", "KXBTCD-26JUL0313-T61999.99",
			time.Date(2026, time.July, 3, 13, 0, 0, 0, et)},
		{"crypto 15m HHMM", "KXBTC15M-26JUL031245-45",
			time.Date(2026, time.July, 3, 12, 45, 0, 0, et)},
		{"date-like STRIKE suffix must not parse (anchor)", "KXBTCD-B61999-T26JUL0313", time.Time{}},
		{"date not at START of event segment", "KXFOO-X26JUL0310-BAR", time.Time{}},
		{"no embedded time (election)", "KXCAELECTION-2637-SKAM", time.Time{}},
		{"plausibility: >14d ahead rejected", "KXWT20MATCH-26AUG011000NORSUS-NOR", time.Time{}},
		{"plausibility: >24h past rejected", "KXCS2GAME-26JUL010945ENTSAW-SAW", time.Time{}},
		{"nonsense hour rejected, not normalized", "KXBTCD-26JUL0361-T61999.99", time.Time{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := kalshiTickerStartAt(c.ticker, now)
			if c.want.IsZero() {
				if ok {
					t.Fatalf("kalshiTickerStartAt(%q) = (%v, true), want not-ok", c.ticker, got)
				}
				return
			}
			if !ok || !got.Equal(c.want) {
				t.Fatalf("kalshiTickerStartAt(%q) = (%v, %v), want (%v, true)", c.ticker, got, ok, c.want)
			}
		})
	}
}

// TestSigSecsToStartKalshiTicker exercises the WIRED path (sigSecsToStart source 1): a ticker
// embedding now+2h (built with the live clock so the plausibility gate always passes) must come
// back as ≈+7200s. Format "06Jan021504" upper-cased is exactly the venue's YYMONDDHHMM shape.
func TestSigSecsToStartKalshiTicker(t *testing.T) {
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatalf("load America/New_York: %v", err)
	}
	tm := time.Now().In(et).Add(2 * time.Hour)
	ticker := "KXCS2GAME-" + strings.ToUpper(tm.Format("06Jan021504")) + "ENTSAW-SAW"
	s := &Server{}
	got := s.sigSecsToStart("kalshi", ticker, "", "")
	if got == nil {
		t.Fatalf("sigSecsToStart(kalshi, %q) = nil, want ticker-embedded start", ticker)
	}
	// the format truncates seconds, so expect 2h minus <60s of truncation
	if *got < 2*3600-90 || *got > 2*3600+5 {
		t.Fatalf("secs_to_start = %d, want ≈7200 (±format truncation)", *got)
	}
}

// TestSigSecsToStartKalshiPolyUSFallback exercises source 2: a no-time Kalshi ticker whose
// title/side match a seeded PolyUS game borrows THAT game's start (+90m here); and with no
// match at all the row stays nil → NULL (never a wrong proxy).
func TestSigSecsToStartKalshiPolyUSFallback(t *testing.T) {
	s := &Server{polyUSMkts: []polyUSMarket{{
		League: "mlb", Game: "New York Yankees vs Boston Red Sox",
		Team: "NYY", TeamName: "New York Yankees",
		Slug: "aec-mlb-nyy-bos-2026-07-03", Yes: 0.61, Bid: 0.60, Ask: 0.62,
		Start: time.Now().Add(90 * time.Minute).UTC().Format(time.RFC3339),
	}}}
	got := s.sigSecsToStart("kalshi", "KXMLBGAME-NOTIME-NYY",
		"Will the New York Yankees beat the Boston Red Sox?", "New York Yankees")
	if got == nil {
		t.Fatal("fallback returned nil, want the PolyUS twin's start borrowed")
	}
	if *got < 90*60-90 || *got > 90*60+5 {
		t.Fatalf("secs_to_start = %d, want ≈%d (+90m)", *got, 90*60)
	}
	if v := s.sigSecsToStart("kalshi", "KXCAELECTION-2637-SKAM", "Next California governor?", "YES"); v != nil {
		t.Fatalf("unmatched kalshi row = %d, want nil (NULL)", *v)
	}
}
