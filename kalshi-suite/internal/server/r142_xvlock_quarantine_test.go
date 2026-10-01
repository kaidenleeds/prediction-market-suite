package server

import (
	"encoding/json"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarket"
)

func r142VerifiedThresholdLock(aID, bID, orient, aSide, bSide string) xvLockOpp {
	op := xvLockOpp{Pair: "K-PINT", Orient: orient, AVenue: "kalshi", AID: aID,
		ASide: aSide, BVenue: "polymarket", BID: bID, BSide: bSide,
		AWon: 1, BWon: 0, SettledTS: "2026-07-13T12:00:00Z"}
	op.ResolutionBasis = r139VerifiedLockCertificate(op.Pair, op.AVenue, op.AID,
		op.BVenue, op.BID, true, op.ASide, op.BSide)
	return op
}

func TestR142XVLockMismatchDurablyQuarantinesBothOrientations(t *testing.T) {
	aID := "KXBTCD-26JUL1012-T55999.99"
	bID := "bitcoin-above-56k-on-july-10-2026"
	good := r142VerifiedThresholdLock(aID, bID, "YESa+NOb", "YES", "NO")
	bad := r142VerifiedThresholdLock(aID, bID, "NOa+YESb", "NO", "YES")
	bad.AWon, bad.BWon, bad.Mismatch = 0, 0, true
	bad.SettledTS = "2026-07-13T12:01:00Z"
	b := &xvLockBook{Closed: []xvLockOpp{good, bad}, MismatchN: 1}

	if !xvlReconcileQuarantines(b) {
		t.Fatal("mismatch did not create durable quarantine feedback")
	}
	if len(b.QuarantinedPairs) != 1 || !b.Closed[0].PairQuarantined || !b.Closed[1].PairQuarantined {
		t.Fatalf("pair quarantine did not cover every orientation: %+v", b)
	}
	if xvlProofEligible(b.Closed[0]) || xvlProofEligible(b.Closed[1]) {
		t.Fatal("a poisoned semantic pair remained eligible for proof")
	}
	c := xvlCandidate{pair: "K-PINT", aVenue: "kalshi", aID: aID,
		bVenue: "polymarket", bID: bID, sameSide: true}
	if !xvlPairQuarantined(b, c) {
		t.Fatal("future scanner candidate was not refused")
	}
	reversed := xvlCandidate{pair: "K-PINT", aVenue: "polymarket", aID: bID,
		bVenue: "kalshi", bID: aID, sameSide: true}
	if !xvlPairQuarantined(b, reversed) {
		t.Fatal("pair key depended on venue ordering")
	}

	raw, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	var restored xvLockBook
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if xvlReconcileQuarantines(&restored) {
		t.Fatal("restored quarantine reconciliation was not idempotent")
	}
	if len(restored.QuarantinedPairs) != 1 || !xvlPairQuarantined(&restored, c) {
		t.Fatalf("quarantine did not survive restart serialization: %+v", restored.QuarantinedPairs)
	}
	secondMismatch := good
	secondMismatch.Mismatch, secondMismatch.SettledTS = true, "2026-07-13T12:02:00Z"
	xvlRecordMismatchQuarantine(&restored, &secondMismatch)
	q := restored.QuarantinedPairs[xvlOppPairKey(secondMismatch)]
	if q.MismatchN != 2 || q.LastMismatch != secondMismatch.SettledTS || !secondMismatch.PairQuarantined {
		t.Fatalf("new mismatch was not added independently of the bounded tail: %+v row=%+v", q, secondMismatch)
	}
}

func TestR142XVLockMismatchCannotProveItselfBeforeBookReconcile(t *testing.T) {
	op := r142VerifiedThresholdLock("KXBTCD-26JUL1012-T55999.99",
		"bitcoin-above-56k-on-july-10-2026", "YESa+NOb", "YES", "NO")
	op.Mismatch = true
	if xvlProofEligible(op) {
		t.Fatal("a mismatch row with an otherwise valid certificate entered proof")
	}
}

func TestR142Updown15mMismatchClassRemainsAnchoredButNeverTwins(t *testing.T) {
	xvTestReset()
	defer xvTestReset()
	kKey, kKind, kOK := xvKalshiAnchor("KXBTC15M-26JUL071215-15")
	pKey, pKind, pOK := xvPolyAnchor("btc-updown-15m-1783440000", "")
	if !kOK || !pOK || kKey != pKey || kKind != "crypto" || pKind != "crypto" {
		t.Fatalf("forensic anchors lost: K=(%q,%q,%v) P=(%q,%q,%v)",
			kKey, kKind, kOK, pKey, pKind, pOK)
	}
	if xvKeyRulesEquivalent(kKind, kKey) {
		t.Fatal("empirically mismatched 15-minute predicate remained lock-safe")
	}
	xvReg.add("kalshi", "KXBTC15M-26JUL071215-15", kKey, kKind)
	xvReg.add("polymarket", "btc-updown-15m-1783440000", pKey, pKind)
	if _, _, _, ok := xvTwin("kalshi", "KXBTC15M-26JUL071215-15"); ok {
		t.Fatal("15-minute class escaped the member-twin gate")
	}
	if k, pus := pintDynamicXVTwins("btc-updown-15m-1783440000"); k != "" || pus != "" {
		t.Fatalf("Poly-int priority/lock matcher bypassed quarantine: kalshi=%q polyus=%q", k, pus)
	}
}

func TestR142Updown15mCannotBypassQuarantineThroughTitleFuzz(t *testing.T) {
	xvTestReset()
	pm := polymarket.Market{Slug: "btc-updown-15m-1783440000",
		Question: "Will Bitcoin be up during the 15 minute window?"}
	km := kalshi.Market{Ticker: "KXBTC15M-26JUL071215-15",
		Title: "Will Bitcoin be up during the 15 minute window?"}
	if _, ok := bestKalshiMatch(pm, []kalshi.Market{km}); ok {
		t.Fatal("title fuzz bypassed the empirical 15-minute settlement quarantine")
	}
}
