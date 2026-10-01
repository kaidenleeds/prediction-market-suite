package server

// R80 tests — the equity-alloc COLD PATH (operator P0: paper auto placed nothing while NAV was
// boot-cold/erroring). A fresh Server with NO sweep run must size kalshi at its paper_total_start
// allocation (0.25 × 1000 = 250); a FAILING store must serve the same warm-up fallback WITHOUT
// stamping the NAV cache (so the next sweep retries instead of caching a fabricated fresh $0);
// and the R79 placement policy's boot posture must be PLACE (fail-open), never pending.

import (
	"math"
	"testing"
)

func TestR80PlatformBankrollFreshServerNoSweep(t *testing.T) {
	s := testServer(t)
	// Fresh Server, no sweepEquityAlloc ever run: R127 — the venue budgets are the FIXED book
	// portfolios ($1,000), served without any NAV dependency — never $0.
	if got := s.platformBankroll("kalshi"); math.Abs(got-600) > 0.01 {
		t.Fatalf("fresh-server (no sweep) kalshi bankroll = %v, want the fixed $600 portfolio bank (R143)", got)
	}
	if got := s.platformBankroll("polyus"); math.Abs(got-600) > 0.01 {
		t.Fatalf("fresh-server (no sweep) polyus bankroll = %v, want the fixed $600 portfolio bank (R143)", got)
	}
	// R79 boot posture: before the policy table's FIRST build every family must be PLACE
	// (policyStatus "" fail-open) — a pending/blocked default would silence the whole auto book.
	for _, src := range []string{"auto-cons-kcrypto", "auto-cons-pusflow", "auto-arb", "auto-ml"} {
		if s.policyRetired(src) || s.policyInverted(src) {
			t.Fatalf("policy for %s blocks placement before its first build — boot default must be PLACE (fail-open)", src)
		}
	}
}

func TestR80NAVWarmupFallbackOnStoreError(t *testing.T) {
	s := testServer(t)
	_ = s.store.Close() // boot-cold / unavailable store: every fills read now errors
	// The old path computed NAV from zeroed components, clamped to $0 and STAMPED it fresh —
	// $0 budgets for 90s per erroring sweep. R127: sizing budgets are the fixed book banks (no
	// NAV dependency at all), and the NAV warm-up fallback below still guards its own consumers.
	if got := s.platformBankroll("kalshi"); got != 0 {
		t.Fatalf("store-error kalshi sizing authority = %v, want fail-closed $0 until current fee-net truth is readable", got)
	}
	if nav := s.totalPaperEquityUSD(); math.Abs(nav-1000) > 0.01 {
		t.Fatalf("store-error NAV = %v, want the 1000 paper_total_start warm-up fallback", nav)
	}
	// A FAILED compute must leave the cache unstamped: navColdStart stays true (autoPlace tags
	// skips "alloc-warmup") and the next successful sweep heals immediately.
	if !s.navColdStart() {
		t.Fatal("failed NAV compute stamped the cache — it must stay cold so the next sweep retries")
	}
}
