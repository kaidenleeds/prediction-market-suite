package server

// r108_test.go — R108 pins for the Jul-9 two-sided flip fuse (auditor 293/299).
//
// R106 (bug 255) taught the shared matcher polyUSMatchForSide to serve a two-sided single
// instrument's SHORT team as the SAME slug with flip=true at the side-adjusted (1−YES) price,
// and fixed rbridge/pmatch/smartmoney. Four consumers still discarded the flip flag and stamped
// Side:"YES" on the polyus leg: logArbSignal (via sweepArbForward's two call sites), xvlag,
// meanrev and confluence — from Jul-9 (first two-sided singles: World Cup QF to-advance,
// docs.polymarket.us/changelog "Soccer To Advance applies starting with the World Cup
// quarter-finals") those rows would grade inverted and xvlag/confluence autoPlace would buy the
// wrong side. R108 threads the flip through all four; these tests pin the convention BOTH ways:
// pre-Jul-9 shape (one-sided, flip=false ⇒ YES, unchanged) and post-Jul-9 shape (flipped short
// leg ⇒ NO at the already-adjusted price).

import (
	"context"
	"testing"
	"time"
)

// findSig returns the newest signal row for (ticker, sigType) or fails the test.
func r108FindSig(t *testing.T, s *Server, ticker, sigType string) (side string, px float64) {
	t.Helper()
	rows, err := s.store.ListSignals(context.Background(), 50)
	if err != nil {
		t.Fatalf("ListSignals: %v", err)
	}
	for _, r := range rows {
		if r.Ticker == ticker && r.SignalType == sigType {
			return r.Side, r.EntryPrice
		}
	}
	t.Fatalf("no %s signal for %s", sigType, ticker)
	return "", 0
}

// TestR108LogArbSignalFlipSideNO — the arb family's polyus buy leg must say NO on a flipped
// two-sided match (price already side-adjusted), YES on a plain match, and kalshi legs are
// untouched by the flag.
func TestR108LogArbSignalFlipSideNO(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	const slug = "aadc-fwc-fra-mar-2026-07-09-to-advance"

	// POST-JUL-9 shape: flipped short leg (Morocco on the France-denominated instrument).
	s.logArbSignal(ctx, "polyus", "KXWCADVANCE-26JUL09FRAMAR-MAR", slug,
		"France vs Morocco: to advance (short leg)", "Morocco", 0.225, 0.30, 0.012, true)
	if side, px := r108FindSig(t, s, slug, "arb"); side != "NO" || px != 0.225 {
		t.Fatalf("flipped polyus arb leg = side %q @ %v, want NO @ 0.225", side, px)
	}

	// PRE-JUL-9 / same-side shape: plain match keeps the canonical YES.
	const slug2 = "aec-mlb-nyy-bos-2026-07-03"
	s.logArbSignal(ctx, "polyus", "KXMLBGAME-26JUL03NYYBOS-NYY", slug2,
		"Yankees beat Red Sox?", "New York Yankees", 0.55, 0.60, 0.01, false)
	if side, _ := r108FindSig(t, s, slug2, "arb"); side != "YES" {
		t.Fatalf("plain polyus arb leg side = %q, want YES", side)
	}

	// (Kalshi buy leg untestable here — kalSigMeta needs a live client; the flip guard is
	// explicitly plat=="polyus"-scoped in logArbSignal, so kalshi rows cannot be affected.)
}

// TestR108MatcherToSignalFlipWalk — end-to-end against the POST-JUL-9 venue shape: the R106
// two-sided fixture (one slug, marketSides long/short) through the REAL shared matcher, its flip
// return threaded into logArbSignal exactly as sweepArbForward now does, landing a NO row at the
// side-adjusted price. This is the walk that goes live when the first QF to-advance trades Jul-9.
func TestR108MatcherToSignalFlipWalk(t *testing.T) {
	s := testServer(t)
	r106AnchorAdvance(t, s)
	s.polyUSMkts = []polyUSMarket{{
		League: "fwc", EventID: "ev-framar", Game: "France vs Morocco",
		Team: "FRA", TeamName: "France", ShortTeam: "mar", ShortTeamName: "Morocco", TwoSided: true,
		Slug: r106AdvSlug, Kind: "advance", Question: "To Advance",
		Yes: 0.775, Bid: 0.77, Ask: 0.78, Start: "2026-07-09T20:00:00Z",
	}}
	q := "Which team will advance from France vs Morocco on 2026-07-09 4:00PM ET?"
	slug, px, flip, ok := s.polyUSMatchForSide("arb", q, "Morocco", r106AdvSlug)
	if !ok || slug != r106AdvSlug || !flip {
		t.Fatalf("matcher = (%q,%v,flip=%v,%v), want %q flipped", slug, px, flip, ok, r106AdvSlug)
	}
	// Thread the matcher's flip into the signal exactly like the fixed sweepArbForward call site.
	s.logArbSignal(context.Background(), "polyus", "KXWCADVANCE-26JUL09FRAMAR-MAR", slug, q, "Morocco", px, 0.30, 0.01, flip)
	if side, gotPx := r108FindSig(t, s, slug, "arb"); side != "NO" || gotPx-0.225 > 1e-9 || gotPx-0.225 < -1e-9 {
		t.Fatalf("stored row = side %q @ %v, want NO @ 0.225 (1−0.775)", side, gotPx)
	}
}

// TestR108WSStaleWatchdog — forced-stale simulation for the WS auto-reconnect state machine:
// no kick below 3 sustained stale samples, exactly one kick at 3, cooldown suppresses repeats,
// a fresh sample resets the streak, and a kick with no live socket doesn't count as a fix.
func TestR108WSStaleWatchdog(t *testing.T) {
	s := testServer(t)
	s.latmon = newLatMon()
	kicks := 0
	kick := func() bool { kicks++; return true }

	// Two stale samples: below the sustain threshold — no kick.
	for i := 0; i < 2; i++ {
		if s.wsStaleCheck("poly_tape", true, kick) {
			t.Fatalf("kick fired at sample %d, want none before 3", i+1)
		}
	}
	// A fresh sample resets the streak.
	s.wsStaleCheck("poly_tape", false, kick)
	for i := 0; i < 2; i++ {
		if s.wsStaleCheck("poly_tape", true, kick) {
			t.Fatal("kick fired — the fresh sample should have reset the streak")
		}
	}
	// Third consecutive stale sample: exactly one kick.
	if !s.wsStaleCheck("poly_tape", true, kick) || kicks != 1 {
		t.Fatalf("want exactly one kick at 3 sustained samples, kicks=%d", kicks)
	}
	// Continued staleness inside the cooldown: no re-kick even at 3+ samples.
	for i := 0; i < 5; i++ {
		if s.wsStaleCheck("poly_tape", true, kick) {
			t.Fatal("re-kick inside the 10-min cooldown")
		}
	}
	if kicks != 1 {
		t.Fatalf("kicks = %d, want 1", kicks)
	}
	// Cooldown expiry: age the last kick, three more stale samples → second kick.
	s.latmon.mu.Lock()
	s.latmon.kickAt["poly_tape"] = time.Now().Add(-wsKickCooldown - time.Minute)
	s.latmon.staleN["poly_tape"] = 0
	s.latmon.mu.Unlock()
	fired := 0
	for i := 0; i < 3; i++ {
		if s.wsStaleCheck("poly_tape", true, kick) {
			fired++
		}
	}
	if fired != 1 || kicks != 2 {
		t.Fatalf("post-cooldown: fired=%d kicks=%d, want 1/2", fired, kicks)
	}
	// A kick that finds no live socket reports false (redial loop already owns recovery).
	s.latmon.mu.Lock()
	s.latmon.kickAt["kalshi_ws"] = time.Time{}
	s.latmon.mu.Unlock()
	for i := 0; i < 2; i++ {
		s.wsStaleCheck("kalshi_ws", true, func() bool { return false })
	}
	if s.wsStaleCheck("kalshi_ws", true, func() bool { return false }) {
		t.Fatal("no-socket kick must not report as a fix")
	}
	// Classes are independent: poly_tape state untouched by kalshi_ws activity.
	if s.latmon.staleN["poly_tape"] != 0 {
		t.Fatal("class state bled across classes")
	}
}
