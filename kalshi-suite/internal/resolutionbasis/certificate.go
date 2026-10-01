// Package resolutionbasis issues immutable, fail-closed certificates for cross-venue payoff
// comparisons. A structural event match is intentionally insufficient: settlement source,
// normalized rules, void/refund behavior, scalar payouts, and unknown/indeterminate handling must
// all be explicit and equal before a two-leg position may be classified as a lock.
package resolutionbasis

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

const SchemaVersion = 1

type LegTerms struct {
	Venue                 string `json:"venue"`
	InstrumentID          string `json:"instrument_id"`
	EventID               string `json:"event_id"`
	PayoffID              string `json:"payoff_id"`
	BasisID               string `json:"basis_id"`
	NativeSide            string `json:"native_side"`
	InstrumentOrientation string `json:"instrument_orientation"`
	SettlementSource      string `json:"settlement_source"`
	RulesHash             string `json:"rules_hash"`
	VoidPolicy            string `json:"void_policy"`
	ScalarPolicy          string `json:"scalar_policy"`
	UnknownPolicy         string `json:"unknown_policy"`
	IdentityStatus        string `json:"identity_status"`
	InstrumentVersion     int    `json:"instrument_version"`
	EventVersion          int    `json:"event_version"`
	PayoffVersion         int    `json:"payoff_version"`
}

type Certificate struct {
	SchemaVersion int      `json:"schema_version"`
	PairID        string   `json:"pair_id"`
	Orientation   string   `json:"orientation"`
	LeftSide      string   `json:"left_side"`
	RightSide     string   `json:"right_side"`
	Left          LegTerms `json:"left"`
	Right         LegTerms `json:"right"`
	Compatible    bool     `json:"compatible"`
	Blocker       string   `json:"blocker,omitempty"`
	ContentHash   string   `json:"content_hash"`
}

func normalizedLeg(v LegTerms) LegTerms {
	v.Venue = strings.ToLower(strings.TrimSpace(v.Venue))
	v.InstrumentID = strings.TrimSpace(v.InstrumentID)
	v.EventID = strings.TrimSpace(v.EventID)
	v.PayoffID = strings.TrimSpace(v.PayoffID)
	v.BasisID = strings.TrimSpace(v.BasisID)
	v.NativeSide = normalizedSide(v.NativeSide)
	v.InstrumentOrientation = strings.ToLower(strings.TrimSpace(v.InstrumentOrientation))
	v.SettlementSource = strings.TrimSpace(v.SettlementSource)
	v.RulesHash = strings.ToLower(strings.TrimSpace(v.RulesHash))
	v.VoidPolicy = strings.ToLower(strings.TrimSpace(v.VoidPolicy))
	v.ScalarPolicy = strings.ToLower(strings.TrimSpace(v.ScalarPolicy))
	v.UnknownPolicy = strings.ToLower(strings.TrimSpace(v.UnknownPolicy))
	v.IdentityStatus = strings.ToLower(strings.TrimSpace(v.IdentityStatus))
	return v
}

func normalizedSide(v string) string { return strings.ToUpper(strings.TrimSpace(v)) }

func sideRepresentsBasis(leg LegTerms, side string) bool {
	affirmsNative := side == leg.NativeSide
	if leg.InstrumentOrientation == "inverse" {
		return !affirmsNative
	}
	return affirmsNative
}

func firstBlocker(c Certificate) string {
	if c.PairID == "" || c.Orientation == "" || (c.Orientation != "same" && c.Orientation != "inverse") {
		return "pair identity/orientation missing or unsupported"
	}
	if (c.LeftSide != "YES" && c.LeftSide != "NO") || (c.RightSide != "YES" && c.RightSide != "NO") {
		return "canonical YES/NO sides required"
	}
	for _, leg := range []LegTerms{c.Left, c.Right} {
		if leg.Venue == "" || leg.InstrumentID == "" || leg.EventID == "" || leg.PayoffID == "" || leg.BasisID == "" {
			return "canonical instrument/event/payoff/basis identity missing"
		}
		if leg.InstrumentVersion <= 0 || leg.EventVersion <= 0 || leg.PayoffVersion <= 0 {
			return "immutable rules/event/payoff version missing"
		}
		if leg.IdentityStatus != "verified" {
			return "both instrument identities must be verified"
		}
		if (leg.NativeSide != "YES" && leg.NativeSide != "NO") ||
			(leg.InstrumentOrientation != "same" && leg.InstrumentOrientation != "inverse" && leg.InstrumentOrientation != "basis") {
			return "native side/orientation missing or unsupported"
		}
		if leg.SettlementSource == "" || leg.RulesHash == "" || leg.VoidPolicy == "" ||
			leg.ScalarPolicy == "" || leg.UnknownPolicy == "" {
			return "settlement source/rules/void/scalar/unknown semantics incomplete"
		}
	}
	if c.Left.Venue == c.Right.Venue {
		return "cross-venue certificate requires different venues"
	}
	if c.Left.EventID != c.Right.EventID || c.Left.EventVersion != c.Right.EventVersion {
		return "event identity/version mismatch"
	}
	if c.Left.PayoffID != c.Right.PayoffID || c.Left.PayoffVersion != c.Right.PayoffVersion {
		return "payoff identity/version mismatch"
	}
	if c.Left.BasisID != c.Right.BasisID {
		return "cross-venue basis mismatch"
	}
	if (c.Orientation == "same") !=
		(sideRepresentsBasis(c.Left, "YES") == sideRepresentsBasis(c.Right, "YES")) {
		return "pair orientation disagrees with instrument certificates"
	}
	if sideRepresentsBasis(c.Left, c.LeftSide) == sideRepresentsBasis(c.Right, c.RightSide) {
		return "selected sides do not form a complementary payout"
	}
	if c.Left.SettlementSource != c.Right.SettlementSource {
		return "settlement source mismatch"
	}
	if c.Left.RulesHash != c.Right.RulesHash {
		return "normalized rules hash mismatch"
	}
	if c.Left.VoidPolicy != c.Right.VoidPolicy {
		return "void/refund policy mismatch"
	}
	if c.Left.ScalarPolicy != c.Right.ScalarPolicy {
		return "scalar payout policy mismatch"
	}
	if c.Left.UnknownPolicy != c.Right.UnknownPolicy {
		return "unknown/indeterminate policy mismatch"
	}
	return ""
}

func digest(c Certificate) (string, error) {
	c.ContentHash = ""
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

// Issue always returns a hashed immutable receipt. Compatible is true only when every semantic
// field is present and equal; blocked receipts remain useful provenance without granting proof.
func Issue(pairID, orientation, leftSide, rightSide string, left, right LegTerms) Certificate {
	c := Certificate{SchemaVersion: SchemaVersion, PairID: strings.TrimSpace(pairID),
		Orientation: strings.ToLower(strings.TrimSpace(orientation)), LeftSide: normalizedSide(leftSide),
		RightSide: normalizedSide(rightSide), Left: normalizedLeg(left), Right: normalizedLeg(right)}
	c.Blocker = firstBlocker(c)
	c.Compatible = c.Blocker == ""
	c.ContentHash, _ = digest(c)
	return c
}

// Verify rejects mutation, a forged compatibility flag, and every incomplete/incompatible basis.
func Verify(c Certificate) error {
	if c.SchemaVersion != SchemaVersion || c.ContentHash == "" {
		return errors.New("resolution certificate version/hash missing")
	}
	want, err := digest(c)
	if err != nil || want != c.ContentHash {
		return errors.New("resolution certificate hash mismatch")
	}
	blocker := firstBlocker(c)
	if blocker != c.Blocker || c.Compatible != (blocker == "") {
		return errors.New("resolution certificate verdict mismatch")
	}
	if !c.Compatible {
		return errors.New(c.Blocker)
	}
	return nil
}
