package server

import (
	"testing"
	"time"
)

// ── R85 REST-health watchdog verdict (pure function) ─────────────────────────────────────────────
// wedged ⇔ WS alive ∧ attempts recent ∧ no HTTP response for >wedgeAfter (never-responded counts
// from boot). WS-dead or attempt-stale states are outage/idle classes — resetting the limiter for
// those would be noise, so they must read healthy here.
func TestRestWedgeEval(t *testing.T) {
	now := time.Now()
	boot := now.Add(-time.Hour)
	const after = 120 * time.Second
	try := now.Add(-2 * time.Second) // attempts ongoing

	if restWedgeEval(now, boot, try, now.Add(-10*time.Second), 5000, after) {
		t.Fatal("healthy REST (response 10s ago) read as wedged")
	}
	if !restWedgeEval(now, boot, try, now.Add(-3*time.Minute), 5000, after) {
		t.Fatal("wedge not detected: attempts recent, no response 3min, WS alive")
	}
	if restWedgeEval(now, boot, try, now.Add(-3*time.Minute), 0, after) {
		t.Fatal("WS-dead outage misread as a REST wedge")
	}
	if restWedgeEval(now, boot, now.Add(-10*time.Minute), now.Add(-3*time.Minute), 5000, after) {
		t.Fatal("idle loops (stale attempts) misread as a wedge")
	}
	if restWedgeEval(now, boot, time.Time{}, now.Add(-3*time.Minute), 5000, after) {
		t.Fatal("zero attempt clock misread as a wedge")
	}
	if !restWedgeEval(now, boot, try, time.Time{}, 5000, after) {
		t.Fatal("never-responded-since-boot wedge not detected (boot 1h ago)")
	}
	if restWedgeEval(now, now.Add(-30*time.Second), try, time.Time{}, 5000, after) {
		t.Fatal("fresh boot (30s) misread as a wedge")
	}
}
