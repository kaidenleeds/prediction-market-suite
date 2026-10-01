package server

// R79 tests — the AUTO PLACEMENT POLICY (operator: "all signals ON for auto, or retired only if
// BOTH direct AND inverted EV fail to be positive after maker fees"). Hermetic: temp SQLite,
// resolved signal rows inserted directly; the policy is rebuilt and its statuses + the
// autoPlace-facing accessors (policyRetired / policyInverted) are asserted per case.

import (
	"context"
	"fmt"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// sigOf builds a minimal Kalshi YES signal row for the policy tests.
func sigOf(fam, ticker string, price float64) storage.Signal {
	return storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "T " + ticker, Side: "YES",
		SignalType: fam, EntryPrice: price}
}

func TestPolicySourceFamily(t *testing.T) {
	cases := map[string]string{
		"auto-arb":             "arb",
		"auto-cons-x":          "cross",
		"auto-cons-kalshi":     "kalshi-flow",
		"auto-cons-poly":       "poly-consensus",
		"auto-cons-pusflow":    "polyus-flow",
		"auto-cons-kflow":      "kflow",
		"auto-cons-kcrypto":    "kcrypto",
		"auto-cons-pmatch":     "pmatch",
		"auto-cons-pbridge":    "pbridge",
		"auto-cons-confluence": "confluence",
		"auto-cons-xvlag":      "xvlag",
		"auto-ml":              "", // graded by the ML books, never policy-gated
		"gate":                 "",
		"manual":               "",
	}
	for src, want := range cases {
		if got := policySourceFamily(src); got != want {
			t.Errorf("policySourceFamily(%q) = %q, want %q", src, got, want)
		}
	}
}

// seedResolved inserts n resolved signal rows for one family at a fixed entry price. wonEvery
// controls the outcome pattern: 0 = all lose, 1 = all win, 2 = alternate win/lose.
func seedResolved(t *testing.T, s *Server, fam string, n int, price float64, wonEvery int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		tk := fmt.Sprintf("R79-%s-%03d", fam, i)
		if err := s.store.InsertSignal(ctx, sigOf(fam, tk, price)); err != nil {
			t.Fatalf("insert %s: %v", fam, err)
		}
		yes := 0.0
		switch wonEvery {
		case 1:
			yes = 1
		case 2:
			if i%2 == 0 {
				yes = 1
			}
		}
		if err := s.store.ResolveSignals(ctx, tk, yes); err != nil {
			t.Fatalf("resolve %s: %v", fam, err)
		}
	}
}

func TestPlacementPolicyStatuses(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	// kthresh: 40/40 winners at 50¢ → direct LB > 0 → ON.
	seedResolved(t, s, "kthresh", 40, 0.50, 1)
	// xmatch: 40/40 losers at 50¢ → direct LB ≤ 0, INVERTED LB > 0 → INVERTED (placement stays on, flipped).
	seedResolved(t, s, "xmatch", 40, 0.50, 0)
	// cross: 50/50 coin-flip at 50¢ → both sides lose the maker fee → BOTH LBs ≤ 0 → RETIRED.
	seedResolved(t, s, "cross", 40, 0.50, 2)
	// kflow: only 10 rows (< policyMinN), all losers → still ON (default-on below the evidence bar).
	seedResolved(t, s, "kflow", 10, 0.50, 0)
	// pcrypto: research family — even 40/40 winners stay RESEARCH (data-only, never places).
	seedResolved(t, s, "pcrypto", 40, 0.50, 1)

	s.rebuildPlacementPolicy(ctx)

	want := map[string]string{
		"kthresh": "on", "xmatch": "retired", "cross": "retired", "kflow": "on", "pcrypto": "research",
		"favlong": "on", // never logged → n=0 → default ON
	}
	rows, builtAt := s.policyView()
	if builtAt.IsZero() || len(rows) == 0 {
		t.Fatal("policy table must be built")
	}
	got := map[string]policyRow{}
	for _, r := range rows {
		got[r.Family] = r
	}
	for fam, st := range want {
		r, ok := got[fam]
		if !ok {
			t.Fatalf("family %s missing from the policy table", fam)
		}
		if r.Status != st {
			t.Errorf("%s status = %q (n=%d dirLB=%.4f invLB=%.4f), want %q", fam, r.Status, r.N, r.DirLB, r.InvLB, st)
		}
	}
	// The evidence numbers behind the verdicts must point the right way.
	if r := got["kthresh"]; !(r.N == 40 && r.DirLB > 0) {
		t.Errorf("kthresh should be n=40 with a positive direct LB, got n=%d dirLB=%.4f", r.N, r.DirLB)
	}
	if r := got["xmatch"]; !(r.DirLB <= 0 && r.InvLB > 0) {
		t.Errorf("xmatch should fail direct but clear inverted, got dirLB=%.4f invLB=%.4f", r.DirLB, r.InvLB)
	}
	if r := got["cross"]; !(r.DirLB <= 0 && r.InvLB <= 0) {
		t.Errorf("cross should fail BOTH bounds, got dirLB=%.4f invLB=%.4f", r.DirLB, r.InvLB)
	}

	// The autoPlace-facing accessors (source-string keyed).
	if s.policyRetired("auto-cons-kthresh") || s.policyInverted("auto-cons-kthresh") {
		t.Error("kthresh (direct winner) must be neither retired nor inverted")
	}
	if s.policyInverted("auto-cons-xmatch") || !s.policyRetired("auto-cons-xmatch") {
		t.Error("xmatch must be retired; diagnostic inverse evidence cannot flip placement")
	}
	if !s.policyRetired("auto-cons-x") {
		t.Error("cross (both-fail at n≥30) must be RETIRED")
	}
	if s.policyRetired("auto-cons-kflow") || s.policyInverted("auto-cons-kflow") {
		t.Error("kflow below the n≥30 bar must stay default-ON")
	}
	if s.policyRetired("auto-cons-pcrypto") || s.policyInverted("auto-cons-pcrypto") {
		t.Error("research families are never gated by the policy (data-only stays data-only)")
	}
	if s.policyRetired("auto-ml") || s.policyRetired("gate") {
		t.Error("non-family sources must never be policy-gated")
	}
}

// Before the first build the table is empty — everything FAIL-OPENS to the default-ON posture.
func TestPlacementPolicyFailOpenBeforeFirstBuild(t *testing.T) {
	s := testServer(t)
	if s.policyRetired("auto-cons-x") || s.policyInverted("auto-cons-xmatch") {
		t.Fatal("no table yet → nothing may be retired or inverted (default-ON)")
	}
}
