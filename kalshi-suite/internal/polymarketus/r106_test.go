// r106_test.go — R106 pins (auditor bug 255): single-slug two-sided instruments (WC "to advance",
// live Jul-9). Their short-team prints (a) NEVER vote on the global denomination latch (a WC-heavy
// boot must not latch the venue-wide mode off the new class) and (b) are disambiguated PER PRINT
// against their own fresh long-denominated book mid — whichever space the venue reports, the
// YES-momentum window and the flow dollars stay honest.
package polymarketus

import (
	"testing"
	"time"
)

const advSlug = "aadc-fwc-fra-mar-2026-07-09-to-advance"

func TestR106TwoSidedPrintsExemptFromLatch(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.SetTwoSidedSlugs([]string{advSlug})
	feedBook(ws, advSlug, 0.77, 0.78) // France-denominated book, mid 0.775
	// 30 Morocco (short-side) buys reported in the SHORT side's own price — on a legacy market
	// this exact shape stacks denomOutcome votes; two-sided prints must be vote-exempt.
	for i := 0; i < 30; i++ {
		feedNoTrade(ws, advSlug, 0.23)
	}
	if mode, ov, yv := ws.DenomStats(); mode != "unknown" || ov != 0 || yv != 0 {
		t.Fatalf("two-sided prints must not vote: mode=%q ov=%d yv=%d", mode, ov, yv)
	}
	// Per-print disambiguation: the momentum window must hold ~0.77 (1−0.23), not 0.23.
	px, _, ok := ws.LiveYesAt(advSlug)
	if !ok || px < 0.7 {
		t.Fatalf("two-sided short print left YES at %.3f — momentum window poisoned", px)
	}
	// A LEGACY slug must still vote (the global latch machinery is untouched for the old shape).
	feedBook(ws, "legacy-s1", 0.96, 0.98)
	feedNoTrade(ws, "legacy-s1", 0.03)
	if _, ov, yv := ws.DenomStats(); ov+yv != 1 {
		t.Fatalf("legacy print must still vote: ov=%d yv=%d", ov, yv)
	}
}

func TestR106TwoSidedNotionalBothSpaces(t *testing.T) {
	ws := (&Client{}).NewMarketsWS()
	ws.SetTwoSidedSlugs([]string{advSlug})
	feedBook(ws, advSlug, 0.77, 0.78) // mid 0.775
	// Short-side buy reported in OUTCOME space (0.23): the buyer paid 0.23·100 = $23.
	_, no0, _ := ws.TakerStats(advSlug, time.Minute)
	feedNoTrade(ws, advSlug, 0.23)
	_, no1, _ := ws.TakerStats(advSlug, time.Minute)
	if d := no1 - no0; d < 22 || d > 24 {
		t.Fatalf("outcome-space short print notional delta $%.2f, want ~$23", d)
	}
	// Same trade reported in LONG/YES space (0.77): the short buyer still paid (1−0.77)·100 = $23.
	feedNoTrade(ws, advSlug, 0.77)
	_, no2, _ := ws.TakerStats(advSlug, time.Minute)
	if d := no2 - no1; d < 22 || d > 24 {
		t.Fatalf("long-space short print notional delta $%.2f, want ~$23", d)
	}
}
