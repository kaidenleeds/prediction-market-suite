package resolutionbasis

import (
	"encoding/json"
	"testing"
)

func verifiedLeg(venue, id string) LegTerms {
	return LegTerms{Venue: venue, InstrumentID: id, EventID: "event-1", PayoffID: "payoff-1",
		BasisID: "basis-1", NativeSide: "YES", InstrumentOrientation: "same",
		SettlementSource: "official-box-score", RulesHash: "abc123",
		VoidPolicy: "refund-0.5", ScalarPolicy: "binary-or-refund", UnknownPolicy: "refund-0.5",
		IdentityStatus: "verified", InstrumentVersion: 2, EventVersion: 3, PayoffVersion: 4}
}

func TestResolutionBasisRequiresEverySemanticAndHashesImmutably(t *testing.T) {
	c := Issue("K-PUS|a|b", "same", "YES", "NO", verifiedLeg("kalshi", "a"), verifiedLeg("polyus", "b"))
	if !c.Compatible || Verify(c) != nil || c.ContentHash == "" {
		t.Fatalf("verified certificate failed: %+v err=%v", c, Verify(c))
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip Certificate
	if err := json.Unmarshal(b, &roundTrip); err != nil || Verify(roundTrip) != nil {
		t.Fatalf("persisted certificate lost immutable fields: json=%s cert=%+v err=%v", b, roundTrip, err)
	}
	mutated := c
	mutated.Left.VoidPolicy = "cancel-as-loss"
	if Verify(mutated) == nil {
		t.Fatal("post-issue mutation passed immutable hash verification")
	}
}

func TestResolutionBasisFailsClosedOnMissingOrIncompatibleTerms(t *testing.T) {
	left, right := verifiedLeg("kalshi", "a"), verifiedLeg("polyus", "b")
	left.UnknownPolicy = ""
	if c := Issue("pair", "same", "YES", "NO", left, right); c.Compatible || c.Blocker == "" || Verify(c) == nil {
		t.Fatalf("missing unknown policy passed: %+v", c)
	}
	left = verifiedLeg("kalshi", "a")
	right.VoidPolicy = "cancel-as-loss"
	if c := Issue("pair", "same", "YES", "NO", left, right); c.Compatible || c.Blocker != "void/refund policy mismatch" {
		t.Fatalf("void mismatch passed: %+v", c)
	}
	right = verifiedLeg("polyus", "b")
	right.RulesHash = "different"
	if c := Issue("pair", "same", "YES", "NO", left, right); c.Compatible || c.Blocker != "normalized rules hash mismatch" {
		t.Fatalf("rules mismatch passed: %+v", c)
	}
}

func TestResolutionBasisUsesNativeSideAndInstrumentOrientation(t *testing.T) {
	left, right := verifiedLeg("kalshi", "a"), verifiedLeg("polyus", "b")
	right.NativeSide = "NO"
	// Venue YES now represents the opposite canonical state on the right, so the venue pair is
	// inverse and buying YES on both sides is complementary.
	good := Issue("pair", "inverse", "YES", "YES", left, right)
	if err := Verify(good); err != nil {
		t.Fatalf("native-side-aware inverse certificate rejected: %+v err=%v", good, err)
	}
	bad := Issue("pair", "same", "YES", "NO", left, right)
	if bad.Compatible || bad.Blocker != "pair orientation disagrees with instrument certificates" {
		t.Fatalf("native side was ignored: %+v", bad)
	}
}
