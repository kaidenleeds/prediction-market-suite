package server

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

func TestR135cDedicatedAndMLBooksRequirePersistedFeeAuthority(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339)
	exact := kfPos{TS: now, Ticker: "KX-FEE-EXACT", Side: "YES", Price: .4, Contracts: 2,
		Fee: .01, FillKind: "taker", FeeKnown: true, FeeSource: "kalshi:historical-receipt"}
	legacy := exact
	legacy.Ticker, legacy.FeeKnown, legacy.FeeSource = "KX-FEE-NUMERIC-ONLY", false, ""
	// Exercise the inner fee-receipt boundary directly. The outer corrected-generation and
	// authenticated-LIVE-link boundary has dedicated R165 tests.
	out, used, excluded := appendKFSystemBookWhere(nil, &kfBook{Open: []kfPos{legacy, exact}}, "fee.json",
		fixedKFIdentity("fee-system", "fee-signal", "kalshi"), false, nil)
	if used != 1 || excluded != 1 || len(out) != 1 || out[0].FeePC != .005 || !out[0].FeeKnown {
		t.Fatalf("dedicated fee provenance gate failed: used=%d excluded=%d rows=%+v", used, excluded, out)
	}

	dir := t.TempDir()
	ml := map[string]any{"open": []map[string]any{
		{"ticker": "legacy", "signal_type": "flow", "platform": "kalshi", "price": .4,
			"contracts": 2, "fee": .01, "opened": time.Now().Unix(), "fill_kind": "taker"},
		{"ticker": "exact", "signal_type": "flow", "platform": "kalshi", "price": .4,
			"contracts": 2, "fee": .01, "fee_known": true, "fee_source": "kalshi:book-v1",
			"opened": time.Now().Unix(), "fill_kind": "taker"},
	}}
	b, err := json.Marshal(ml)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ml_paper.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	rows, cov := collectMLSystemBook(path)
	if len(rows) != 1 || rows[0].System != "ml-book/flow" || !rows[0].FeeKnown ||
		!strings.Contains(cov.Note, "fee-receipt") {
		t.Fatalf("ML fee provenance gate failed: rows=%+v coverage=%+v", rows, cov)
	}
}

func TestR135cNewDedicatedLotStampsExactRouteFeeWhenAuthoritative(t *testing.T) {
	s := testServer(t)
	s.kalFeeMu.Lock()
	s.kalFees = map[string]kalFeeInfo{"KXFEE": {taker: .07, maker: .0175, typ: "quadratic_with_maker_fees", multiplier: 1}}
	s.kalFeesAt = time.Now()
	s.kalFeeMu.Unlock()
	lot := kfPos{Ticker: "KXFEE-LOT", Platform: "kalshi", Price: .5, Contracts: 100, Fee: 99,
		FillKind: "taker"}
	s.stampKFExactFee(&lot)
	if !lot.FeeKnown || strings.TrimSpace(lot.FeeSource) == "" || math.Abs(lot.Fee-1.75) > 1e-12 {
		t.Fatalf("new lot did not persist exact route receipt: %+v", lot)
	}

	// Unknown authority preserves conservative paper accounting but must not manufacture proof.
	unknown := kfPos{Ticker: "UNKNOWN-LOT", Platform: "kalshi", Price: .5, Contracts: 2, Fee: .10,
		FillKind: "taker"}
	s.stampKFExactFee(&unknown)
	if unknown.FeeKnown || unknown.FeeSource != "" || unknown.Fee != .10 {
		t.Fatalf("unknown fee authority rewrote/book-proved lot: %+v", unknown)
	}
}

func TestR135cPolyUSFeeAuthorityExpiresWithCompleteSweep(t *testing.T) {
	s := testServer(t)
	theta := .06
	s.polyUSMu.Lock()
	s.pusSweep = []polymarketus.Market{{Slug: "fee-market", FeeCoeff: &theta, TickSize: .01}}
	s.pusSweepAt = time.Now()
	s.polyUSMu.Unlock()

	fee, source, known := s.polyUSFeeExactAuthority("fee-market", false, 100, .5)
	if !known || source != polyUSFeeAuthoritySource || math.Abs(fee-1.50) > 1e-12 {
		t.Fatalf("fresh complete fee authority not accepted: fee=%v source=%q known=%v", fee, source, known)
	}
	if _, src, ok := s.makerFillFeeReceipt("polyus", "fee-market", 100, .5); !ok || src != polyUSFeeAuthoritySource {
		t.Fatalf("fresh PolyUS maker receipt missing: source=%q known=%v", src, ok)
	}
	if meta, ok := s.mlPUSBookMeta("fee-market"); !ok || meta.FeeCoeff == nil || meta.TickSize != .01 {
		t.Fatalf("fresh complete sweep did not authorize ML metadata: %+v/%v", meta, ok)
	}
	if got := s.liveMirrorFeePC(liveMirrorCandidate{Platform: "polyus", Ticker: "fee-market"}, true, .5); got < 0 {
		t.Fatalf("live proof granted signed maker rebate as buying power: %v", got)
	}

	s.polyUSMu.Lock()
	s.pusSweepAt = time.Now().Add(-6 * time.Minute)
	s.polyUSMu.Unlock()
	if fee, source, known := s.polyUSFeeExactAuthority("fee-market", false, 100, .5); known || fee != 0 || source != "" {
		t.Fatalf("stale complete sweep remained fee authority: fee=%v source=%q known=%v", fee, source, known)
	}
	if _, _, ok := s.makerFillFeeReceipt("polyus", "fee-market", 100, .5); ok {
		t.Fatal("stale PolyUS fee entered maker proof receipt")
	}
	if _, ok := s.mlPUSBookMeta("fee-market"); ok {
		t.Fatal("stale PolyUS fee entered ML book-v1 authority")
	}
}

func TestR135cLocksAndCombosExcludeNumericOnlyFees(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC().Format(time.RFC3339)
	exact := xvLockOpp{Key: "exact", Pair: "K-PUS", AVenue: "kalshi", AID: "KX-LOCK",
		ASide: "YES", BVenue: "polyus", BID: "pus-lock", BSide: "NO", AAsk: .45, BAsk: .50, FeeA: .01, FeeB: .01,
		FeeAKnown: true, FeeBKnown: true, FeeASource: "kalshi:receipt", FeeBSource: polyUSFeeAuthoritySource,
		ADepth: 2, BDepth: 2, FirstSeen: now, AWon: -1, BWon: -1}
	exact.ResolutionBasis = r139VerifiedLockCertificate(exact.Pair, exact.AVenue, exact.AID,
		exact.BVenue, exact.BID, true, exact.ASide, exact.BSide)
	if !xvlProofEligible(exact) {
		t.Fatalf("fixture resolution certificate is not proof eligible: %+v", exact.ResolutionBasis)
	}
	legacy := exact
	legacy.Key, legacy.FeeAKnown, legacy.FeeBKnown, legacy.FeeASource, legacy.FeeBSource = "legacy", false, false, "", ""
	b, err := json.Marshal(xvLockBook{Open: []xvLockOpp{legacy, exact}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, "xvlock_book.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	locks, cov := s.collectLockSystems()
	if len(locks) != 1 || !locks[0].FeeKnown || !strings.Contains(cov[0].Note, "fee-receipt-less") {
		t.Fatalf("lock fee evidence gate failed: rows=%+v coverage=%+v", locks, cov)
	}

	exactCombo := xvcPos{TS: now, LegA: "A", LegB: "B", Cost: .4, Contracts: 1,
		Fee: .01, FeeKnown: true, FeeSource: "kalshi:combo-receipt", Expr: xvcExprRFQ}
	legacyCombo := exactCombo
	legacyCombo.LegA, legacyCombo.FeeKnown, legacyCombo.FeeSource = "LEGACY", false, ""
	cb, err := json.Marshal(xvcBook{Open: []xvcPos{legacyCombo, exactCombo}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(s.cfg().DataDir, xvcFile), cb, 0o644); err != nil {
		t.Fatal(err)
	}
	combos, comboCov := s.collectComboSystems()
	if len(combos) != 1 || !combos[0].FeeKnown || !strings.Contains(comboCov[0].Note, "numeric-only") {
		t.Fatalf("combo fee evidence gate failed: rows=%+v coverage=%+v", combos, comboCov)
	}
}

func TestR135cParlayLabFeeNetAggregateIsNotFeeAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parlay_lab.jsonl")
	now, later := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)
	cand := map[string]any{"kind": "cand", "at": now, "id": "p1", "bucket": "linked", "class": "sports",
		"venue_prod": .4, "fee_synth": .01, "n_legs": 2, "legs": []map[string]any{{"ticker": "A", "platform": "kalshi", "depth": 2}}}
	grade := map[string]any{"kind": "grade", "at": later, "id": "p1", "real_synth_per_$1": .2}
	a, _ := json.Marshal(cand)
	b, _ := json.Marshal(grade)
	if err := os.WriteFile(path, append(append(a, '\n'), append(b, '\n')...), 0o644); err != nil {
		t.Fatal(err)
	}
	rows, cov := collectParlayLabSystems(path)
	if len(rows) != 0 || len(cov) == 0 || !strings.Contains(cov[0].Note, "lacks a persisted fee") {
		t.Fatalf("Parlay Lab numeric fee aggregate entered Systems economics: rows=%+v coverage=%+v", rows, cov)
	}
}
