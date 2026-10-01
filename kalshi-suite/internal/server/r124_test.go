package server

// R124 pins:
//   1. C1b — the xvgap EXECUTOR path's settlement-rules equivalence gate (xvgRulesEquiv):
//      fuzz-only matches, twin drift, inverted twins, stale/settled/non-active Kalshi twins are
//      all REFUSED; only a fresh, active, struct-re-derived same-side twin passes. Refuse-only.
//   2. The briefing scoreboard's plain-words class names (plabClassPlain).
//   3. The last-hour lab volume ring (plabHourRing) — bump, window, expiry, wrap.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

func TestR124XvgTwinFromTitle(t *testing.T) {
	if got := xvgTwinFromTitle("Cubs vs Cardinals ⇄ KXMLBGAME-26JUL09CHCSTL-CHC"); got != "KXMLBGAME-26JUL09CHCSTL-CHC" {
		t.Fatalf("twin parse = %q", got)
	}
	if got := xvgTwinFromTitle("no arrow here"); got != "" {
		t.Fatalf("no-arrow title must yield empty twin, got %q", got)
	}
}

func TestR124XvgRulesEquivGate(t *testing.T) {
	s := &Server{}
	slug := "atc-mlb-chc-stl-2026-07-09-chc"
	twin := "KXMLBGAME-26JUL09CHCSTL-CHC"

	refuse := func(wantWhy, gotWhy string, ok bool) {
		t.Helper()
		if ok || gotWhy != wantWhy {
			t.Fatalf("want refusal %q, got ok=%v why=%q", wantWhy, ok, gotWhy)
		}
	}
	// no twin embedded in the signal
	ok, why := s.xvgRulesEquiv(slug, "")
	refuse("no-twin-in-signal", why, ok)
	// empty registry = the regex-fuzz-only match class → REFUSED at execution
	ok, why = s.xvgRulesEquiv(slug, twin)
	refuse("no-struct-twin", why, ok)

	// anchor the struct twin (deterministic venue-metadata join)
	st := s.gi()
	st.byMkt["polyus|"+slug] = &giMktRef{gameID: "g1", venue: "polyus", id: slug, mktType: "winner", yesTeam: "CHC"}
	st.games["g1"] = &giGame{}
	st.gameMkts["g1"] = map[string][]string{"kalshi": {twin}}
	st.byMkt["kalshi|"+twin] = &giMktRef{gameID: "g1", venue: "kalshi", id: twin, mktType: "winner", yesTeam: "CHC"}

	// signal's embedded twin no longer re-derives → drift
	ok, why = s.xvgRulesEquiv(slug, "KXSOMETHINGELSE")
	refuse("twin-drift", why, ok)
	// re-derives, but no fresh Kalshi meta for the twin
	ok, why = s.xvgRulesEquiv(slug, twin)
	refuse("twin-meta-stale", why, ok)
	// fresh meta but the twin already SETTLED (bug-462 closed-market class)
	s.kmkts = map[string]kalshi.Market{twin: {Result: "yes"}}
	s.kmktsAt = map[string]time.Time{twin: time.Now()}
	ok, why = s.xvgRulesEquiv(slug, twin)
	refuse("twin-settled", why, ok)
	// fresh meta but non-active status
	s.kmkts[twin] = kalshi.Market{Status: "finalized"}
	ok, why = s.xvgRulesEquiv(slug, twin)
	refuse("twin-not-active:finalized", why, ok)
	// fresh + active + same-side struct twin → the ONLY passing shape
	s.kmkts[twin] = kalshi.Market{Status: "active"}
	if ok, why = s.xvgRulesEquiv(slug, twin); !ok {
		t.Fatalf("healthy twin must pass, refused with %q", why)
	}
	// meta goes stale (>2min) → refused again
	s.kmktsAt[twin] = time.Now().Add(-3 * time.Minute)
	ok, why = s.xvgRulesEquiv(slug, twin)
	refuse("twin-meta-stale", why, ok)

	// INVERTED twin (R106 advance complement: kal YES == pus NO) — economics flip, never
	// gap-tradeable as same-side.
	slug2 := "aec-fwc-fra-mar-2026-07-14"
	kadv := "KXADVANCE-MAR"
	st.byMkt["polyus|"+slug2] = &giMktRef{gameID: "g2", venue: "polyus", id: slug2, mktType: "advance", yesTeam: "FRA"}
	st.games["g2"] = &giGame{away: "FRA", home: "MAR"}
	st.gameMkts["g2"] = map[string][]string{"kalshi": {kadv}}
	st.byMkt["kalshi|"+kadv] = &giMktRef{gameID: "g2", venue: "kalshi", id: kadv, mktType: "advance", yesTeam: "MAR"}
	ok, why = s.xvgRulesEquiv(slug2, kadv)
	refuse("inverted-twin", why, ok)
}

func TestR124PlabClassPlain(t *testing.T) {
	for in, want := range map[string]string{
		"ind:2leg:crypto+weather":   "2-leg, no known link (crypto + weather; independence not proven)",
		"ind:3leg":                  "3-leg, no known link (independence not proven)",
		"ov:kgame:KXMLB+KXMLBTOTAL": "same game (MLB+MLBTOTAL)",
		"ov:multi:4leg":             "4-leg linked mix",
		"ov:coin:BTC+ETH":           "same coin (BTC+ETH)",
		"ov:weird:X+Y":              "linked weird (X+Y)",
	} {
		if got := plabClassPlain(in); got != want {
			t.Fatalf("plabClassPlain(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestR140BriefingSeparatesLabAndShowsTwoThroughSixLegs(t *testing.T) {
	s := testServer(t)
	s.plabLoaded = true
	s.plabGraded = 100
	s.plabStats = map[string]*plabAgg{}
	for legs := 2; legs <= parlayLegLimit; legs++ {
		class := fmt.Sprintf("ind:%dleg", legs)
		if legs == 2 {
			class = "ind:2leg:crypto+weather"
		}
		s.plabStats[fmt.Sprintf("indep|%dleg|%s|unprobed", legs, class)] =
			&plabAgg{N: 10, SumReal: float64(legs), N2: 10, Sum2: float64(legs), Sq2: float64(legs * legs)}
	}
	got := s.briefScoreboard(context.Background())
	for _, want := range []string{"📊 Systems"} {
		if !strings.Contains(got, want) {
			t.Fatalf("briefing missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Combo Lab") {
		t.Fatalf("retired Combo Lab name leaked into compact briefing:\n%s", got)
	}
	if strings.Contains(got, "Combo Paper") {
		t.Fatalf("duplicate Combo Paper line leaked below the portfolio header:\n%s", got)
	}
	for _, labDetail := range []string{"settled rows50", "candidates", "grades", "629→0"} {
		if strings.Contains(got, labDetail) {
			t.Fatalf("research-lab detail %q leaked into funded Combo Paper line:\n%s", labDetail, got)
		}
	}
}

func TestR124PlabHourRing(t *testing.T) {
	var r plabHourRing
	t0 := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	r.add(t0, 5)
	r.add(t0, 2) // same minute accumulates
	r.add(t0.Add(30*time.Minute), 3)
	if got := r.lastHour(t0.Add(30 * time.Minute)); got != 10 {
		t.Fatalf("lastHour at +30m = %d, want 10", got)
	}
	if got := r.lastHour(t0.Add(70 * time.Minute)); got != 3 {
		t.Fatalf("lastHour at +70m = %d, want 3 (the +30m bump only)", got)
	}
	if got := r.lastHour(t0.Add(2 * time.Hour)); got != 0 {
		t.Fatalf("lastHour at +2h = %d, want 0 (all expired)", got)
	}
	// wrap: a bump 60 minutes later reuses the slot and must not double-count
	r.add(t0.Add(60*time.Minute), 7)
	if got := r.lastHour(t0.Add(61 * time.Minute)); got != 10 {
		t.Fatalf("lastHour after wrap = %d, want 10 (7 + the +30m 3)", got)
	}
}
