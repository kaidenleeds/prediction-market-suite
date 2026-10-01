// Package objectiveidentity classifies cross-venue contracts by their actual payoff predicate.
// It never uses a title or friendly name as identity evidence.  The package is deliberately pure:
// venue adapters must first translate authoritative structured fields into Contract values, then
// Evaluate either returns one stable decision code or a fully hashed same/inverse equivalence.
package objectiveidentity

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"
)

const TaxonomyVersion = "objective-payoff-v2"

const (
	AcceptObjectiveExact       = "ACCEPT_OBJECTIVE_EXACT"
	AcceptObjectiveDirectional = "ACCEPT_OBJECTIVE_DIRECTIONAL"

	RejectTitleOnly           = "REJECT_TITLE_ONLY"
	RejectUnstructured        = "REJECT_UNSTRUCTURED_SOURCE"
	RejectReviewGenre         = "REJECT_REVIEW_REQUIRED_GENRE"
	RejectDiscretionary       = "REJECT_DISCRETIONARY_SETTLEMENT"
	RejectUnsupportedGenre    = "REJECT_UNSUPPORTED_GENRE"
	RejectEvent               = "REJECT_EVENT_MISMATCH"
	RejectEventStart          = "REJECT_EVENT_START_MISMATCH"
	RejectParticipants        = "REJECT_PARTICIPANT_MISMATCH"
	RejectPeriod              = "REJECT_PERIOD_MISMATCH"
	RejectMarketType          = "REJECT_MARKET_TYPE_MISMATCH"
	RejectOutcomeCardinality  = "REJECT_OUTCOME_CARDINALITY_MISMATCH"
	RejectSelection           = "REJECT_SELECTION_ORIENTATION_MISMATCH"
	RejectPlayer              = "REJECT_PLAYER_MISMATCH"
	RejectMetric              = "REJECT_METRIC_MISMATCH"
	RejectComparator          = "REJECT_COMPARATOR_MISMATCH"
	RejectThreshold           = "REJECT_THRESHOLD_MISMATCH"
	RejectDeadline            = "REJECT_DEADLINE_MISMATCH"
	RejectTimezone            = "REJECT_TIMEZONE_MISMATCH"
	RejectVoidMissing         = "REJECT_VOID_POLICY_MISSING"
	RejectVoid                = "REJECT_VOID_POLICY_MISMATCH"
	RejectOvertimeMissing     = "REJECT_OVERTIME_POLICY_MISSING"
	RejectOvertime            = "REJECT_OVERTIME_POLICY_MISMATCH"
	RejectTieMissing          = "REJECT_TIE_POLICY_MISSING"
	RejectTie                 = "REJECT_TIE_POLICY_MISMATCH"
	RejectPolicySourceMissing = "REJECT_MATERIAL_POLICY_SOURCE_MISSING"
	RejectMissingPlayer       = "REJECT_PLAYER_ID_MISSING"
	RejectAmbiguous           = "REJECT_AMBIGUOUS_OBJECTIVE_PREDICATE"
	RejectMissingRawRules     = "REJECT_MISSING_RAW_RULE_ARTIFACT"
	RejectMissingCanonical    = "REJECT_MISSING_CANONICAL_IDENTITY"
	RejectCertificateStorage  = "REJECT_CERTIFICATE_REGISTRATION"
)

// Contract is one venue-native YES proposition translated exclusively from structured venue
// fields.  Participants and PlayerID are stable entity identifiers, not display strings.  The
// settlement-source URL is intentionally absent: an objective score/result may be equivalent even
// when two venues cite different publishers, but every material payoff rule below must agree.
type Contract struct {
	Venue, InstrumentID string
	Genre, EventID      string
	League              string
	Participants        []string
	// OutcomeCardinality is the number of mutually-exclusive outcomes in the proposition's
	// exhaustive outcome family. It is deliberately separate from Participants: a soccer match
	// has two team participants but three winner outcomes (home/draw/away). A binary venue
	// contract still has YES and NO sides; this field describes the underlying outcome family,
	// not the two sides of one contract.
	OutcomeCardinality  int
	Period, MarketType  string
	Selection, Opposite string
	PlayerID, Metric    string
	Comparator          string
	Threshold           string
	DeadlineUTC         string
	EventStartUTC       string
	VenueCloseUTC       string
	Timezone            string
	VoidPolicy          string
	OvertimePolicy      string
	TiePolicy           string
	PolicySource        string
	SettlementClass     string
	SourceKind          string
	Structured          bool
}

type Verdict struct {
	Compatible      bool     `json:"compatible"` // safe as a directional same-payoff comparison
	LockEligible    bool     `json:"lock_eligible"`
	Orientation     string   `json:"orientation"`
	ReasonCode      string   `json:"reason_code"`
	LockReasonCode  string   `json:"lock_reason_code,omitempty"`
	RiskTier        string   `json:"risk_tier"`
	TaxonomyVersion string   `json:"taxonomy_version"`
	PredicateHash   string   `json:"predicate_hash"`
	RulesHash       string   `json:"rules_hash"`
	EvidenceHash    string   `json:"evidence_hash"`
	NormalizedLeft  Contract `json:"normalized_left"`
	NormalizedRight Contract `json:"normalized_right"`
}

func normToken(v string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(v)), "_"))
}
func normEntity(v string) string { return strings.ToUpper(strings.TrimSpace(v)) }

func normThreshold(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return ""
	}
	if f == 0 {
		f = 0 // collapse negative zero
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func normDeadline(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func Normalize(in Contract) Contract {
	in.Venue, in.InstrumentID = normToken(in.Venue), strings.TrimSpace(in.InstrumentID)
	in.Genre, in.EventID, in.League = normToken(in.Genre), strings.TrimSpace(in.EventID), normToken(in.League)
	for i := range in.Participants {
		in.Participants[i] = normEntity(in.Participants[i])
	}
	sort.Strings(in.Participants)
	out := in.Participants[:0]
	for _, p := range in.Participants {
		if p != "" && (len(out) == 0 || out[len(out)-1] != p) {
			out = append(out, p)
		}
	}
	in.Participants = out
	in.Period, in.MarketType = normToken(in.Period), normToken(in.MarketType)
	in.Selection, in.Opposite, in.PlayerID = normEntity(in.Selection), normEntity(in.Opposite), normEntity(in.PlayerID)
	in.Metric, in.Comparator, in.Timezone = normToken(in.Metric), normToken(in.Comparator), normToken(in.Timezone)
	in.Threshold, in.DeadlineUTC = normThreshold(in.Threshold), normDeadline(in.DeadlineUTC)
	in.EventStartUTC, in.VenueCloseUTC = normDeadline(in.EventStartUTC), normDeadline(in.VenueCloseUTC)
	in.VoidPolicy, in.OvertimePolicy, in.TiePolicy = normToken(in.VoidPolicy), normToken(in.OvertimePolicy), normToken(in.TiePolicy)
	in.PolicySource = strings.TrimSpace(in.PolicySource)
	in.SettlementClass, in.SourceKind = normToken(in.SettlementClass), normToken(in.SourceKind)
	return in
}

func hash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func reject(code string, left, right Contract) Verdict {
	v := Verdict{ReasonCode: code, LockReasonCode: code, RiskTier: "rejected",
		TaxonomyVersion: TaxonomyVersion, NormalizedLeft: left, NormalizedRight: right}
	v.EvidenceHash = hash(v)
	return v
}

// Blocked preserves the normalized objective evidence while replacing an otherwise compatible
// verdict with a downstream prerequisite failure (raw artifact, canonical identity, or immutable
// certificate write). The replacement is re-hashed so the audit ledger cannot be mutated later.
func Blocked(v Verdict, code string) Verdict {
	return reject(code, v.NormalizedLeft, v.NormalizedRight)
}

// LockBlocked preserves an accepted directional identity while preventing risk-free lock/arb use.
// It is used for downstream certificate prerequisites such as a lagging canonical registry.
func LockBlocked(v Verdict, code string) Verdict {
	v.LockEligible = false
	v.ReasonCode = AcceptObjectiveDirectional
	v.LockReasonCode = code
	v.RiskTier = "directional_only_material_or_certificate_unverified"
	v.EvidenceHash = ""
	v.EvidenceHash = hash(v)
	return v
}

// Certified upgrades an exact directional predicate after a separate current immutable material-
// rules certificate has been verified by the caller. It does not manufacture any venue policy;
// the certificate hash is stored separately in the decision ledger.
func Certified(v Verdict) Verdict {
	v.Compatible, v.LockEligible = true, true
	v.ReasonCode, v.LockReasonCode = AcceptObjectiveExact, ""
	v.RiskTier = "lock_safe_current_reviewed_certificate"
	v.EvidenceHash = ""
	v.EvidenceHash = hash(v)
	return v
}

func materialPolicyReason(code string) bool {
	switch code {
	case RejectPolicySourceMissing, RejectVoidMissing, RejectVoid, RejectOvertimeMissing,
		RejectOvertime, RejectTieMissing, RejectTie:
		return true
	}
	return false
}

// EvaluateDirectional implements the operator-approved objective-result tier. Exact structured
// sports/crypto predicates may be compared directionally when settlement-source prose or material
// invalid/OT/tie policies are unresolved. It never grants a risk-free lock: LockEligible remains
// false and LockReasonCode preserves the exact missing/mismatched policy.
func EvaluateDirectional(a, b Contract) Verdict {
	strict := Evaluate(a, b)
	if strict.Compatible {
		return strict
	}
	if !materialPolicyReason(strict.ReasonCode) {
		return strict
	}
	left, right := Normalize(a), Normalize(b)
	// Re-run the core predicate with equal sentinel material policies. These values exist only in
	// the local copies used by Evaluate and are never returned as venue evidence.
	for _, c := range []*Contract{&left, &right} {
		c.PolicySource = "directional-sentinel-not-venue-evidence"
		c.VoidPolicy = "directional-unverified"
		if c.Genre == "sports" {
			c.OvertimePolicy, c.TiePolicy = "directional-unverified", "directional-unverified"
		}
	}
	core := Evaluate(left, right)
	if !core.Compatible {
		return strict
	}
	core.NormalizedLeft, core.NormalizedRight = Normalize(a), Normalize(b)
	core.Compatible, core.LockEligible = true, false
	core.ReasonCode, core.LockReasonCode = AcceptObjectiveDirectional, strict.ReasonCode
	core.RiskTier = "directional_only_material_policy_unverified"
	core.EvidenceHash = ""
	core.EvidenceHash = hash(core)
	return core
}

func comparatorComplement(a, b string) bool {
	switch a + "|" + b {
	case "gt|lte", "gte|lt", "lt|gte", "lte|gt", "over|under", "under|over":
		return true
	}
	return false
}

func participantSetContains(c Contract, id string) bool {
	for _, p := range c.Participants {
		if p == id {
			return true
		}
	}
	return false
}

func exactParticipants(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func orientation(left, right Contract) (string, bool) {
	switch left.MarketType {
	case "winner":
		if left.Selection != "" && left.Selection == right.Selection {
			return "same", true
		}
		// Different winner selections are complements only when authoritative structure proves an
		// exhaustive TWO-outcome event. Team count is not that proof: soccer has two teams and a
		// third draw outcome, so NO(Home) is Draw OR Away and can never be relabelled YES(Away).
		if left.Selection != "" && right.Selection != "" && left.Selection != right.Selection &&
			left.OutcomeCardinality == 2 && right.OutcomeCardinality == 2 &&
			participantSetContains(left, left.Selection) && participantSetContains(left, right.Selection) &&
			(left.Opposite == right.Selection || right.Opposite == left.Selection) {
			return "inverse", true
		}
	case "advance":
		if left.Selection != "" && left.Selection == right.Selection {
			return "same", true
		}
		if left.Selection != "" && right.Selection != "" && left.Selection != right.Selection &&
			left.OutcomeCardinality == 2 && right.OutcomeCardinality == 2 &&
			participantSetContains(left, left.Selection) && participantSetContains(left, right.Selection) &&
			(left.Opposite == right.Selection || right.Opposite == left.Selection) {
			return "inverse", true
		}
	case "spread":
		if left.Selection == right.Selection && left.Selection != "" && left.Threshold == right.Threshold {
			return "same", true
		}
		lf, le := strconv.ParseFloat(left.Threshold, 64)
		rf, re := strconv.ParseFloat(right.Threshold, 64)
		if le == nil && re == nil && left.Selection != right.Selection &&
			participantSetContains(left, left.Selection) && participantSetContains(left, right.Selection) &&
			lf+rf > -1e-9 && lf+rf < 1e-9 {
			return "inverse", true
		}
	case "total", "crypto_threshold", "player_prop":
		if left.Selection == right.Selection && left.PlayerID == right.PlayerID && left.Metric == right.Metric &&
			left.Comparator == right.Comparator && left.Threshold == right.Threshold {
			return "same", true
		}
		if left.Selection == right.Selection && left.PlayerID == right.PlayerID && left.Metric == right.Metric &&
			left.Threshold == right.Threshold && comparatorComplement(left.Comparator, right.Comparator) {
			return "inverse", true
		}
	case "player_scorer":
		if left.PlayerID != "" && left.PlayerID == right.PlayerID && left.Metric == right.Metric &&
			left.Comparator == right.Comparator && left.Threshold == right.Threshold {
			return "same", true
		}
	}
	return "", false
}

// Evaluate admits only objective, structured propositions. Weather, elections/politics, judged
// events, discretionary resolutions, and title-only matches are always routed to review.  Sports
// and crypto may ignore publisher wording only after every actual payoff field agrees.
func Evaluate(a, b Contract) Verdict {
	left, right := Normalize(a), Normalize(b)
	if !left.Structured || !right.Structured {
		return reject(RejectTitleOnly, left, right)
	}
	if left.SourceKind != "official_structured" || right.SourceKind != "official_structured" {
		return reject(RejectUnstructured, left, right)
	}
	if left.Genre != right.Genre {
		return reject(RejectUnsupportedGenre, left, right)
	}
	switch left.Genre {
	case "weather", "election", "politics", "judged", "discretionary":
		return reject(RejectReviewGenre, left, right)
	case "sports", "crypto":
	default:
		return reject(RejectUnsupportedGenre, left, right)
	}
	if left.SettlementClass != "objective_result" || right.SettlementClass != "objective_result" {
		return reject(RejectDiscretionary, left, right)
	}
	if left.EventID == "" || left.EventID != right.EventID {
		return reject(RejectEvent, left, right)
	}
	if (left.EventStartUTC == "") != (right.EventStartUTC == "") ||
		(left.EventStartUTC != "" && left.EventStartUTC != right.EventStartUTC) {
		return reject(RejectEventStart, left, right)
	}
	if !exactParticipants(left.Participants, right.Participants) {
		return reject(RejectParticipants, left, right)
	}
	if left.Period == "" || left.Period != right.Period {
		return reject(RejectPeriod, left, right)
	}
	if left.MarketType == "" || left.MarketType != right.MarketType {
		return reject(RejectMarketType, left, right)
	}
	if left.OutcomeCardinality != right.OutcomeCardinality {
		return reject(RejectOutcomeCardinality, left, right)
	}
	if (left.DeadlineUTC == "") != (right.DeadlineUTC == "") ||
		(left.DeadlineUTC != "" && left.DeadlineUTC != right.DeadlineUTC) ||
		(left.Genre == "crypto" && left.DeadlineUTC == "") {
		return reject(RejectDeadline, left, right)
	}
	if left.Timezone == "" || left.Timezone != right.Timezone {
		return reject(RejectTimezone, left, right)
	}
	if left.PolicySource == "" || right.PolicySource == "" {
		return reject(RejectPolicySourceMissing, left, right)
	}
	if left.VoidPolicy == "" || right.VoidPolicy == "" {
		return reject(RejectVoidMissing, left, right)
	}
	if left.VoidPolicy != right.VoidPolicy {
		return reject(RejectVoid, left, right)
	}
	if left.Genre == "sports" {
		if left.OvertimePolicy == "" || right.OvertimePolicy == "" {
			return reject(RejectOvertimeMissing, left, right)
		}
		if left.OvertimePolicy != right.OvertimePolicy {
			return reject(RejectOvertime, left, right)
		}
		if left.TiePolicy == "" || right.TiePolicy == "" {
			return reject(RejectTieMissing, left, right)
		}
		if left.TiePolicy != right.TiePolicy {
			return reject(RejectTie, left, right)
		}
	}
	if strings.HasPrefix(left.MarketType, "player_") && (left.PlayerID == "" || right.PlayerID == "") {
		return reject(RejectMissingPlayer, left, right)
	}
	if left.PlayerID != right.PlayerID {
		return reject(RejectPlayer, left, right)
	}
	if left.Metric != right.Metric {
		return reject(RejectMetric, left, right)
	}
	switch left.MarketType {
	case "winner", "advance", "spread", "player_scorer":
		if left.Comparator == "" || left.Comparator != right.Comparator {
			return reject(RejectComparator, left, right)
		}
	case "total", "crypto_threshold", "player_prop":
		if left.Comparator == "" || (left.Comparator != right.Comparator && !comparatorComplement(left.Comparator, right.Comparator)) {
			return reject(RejectComparator, left, right)
		}
	}
	if left.MarketType == "spread" || left.MarketType == "total" || left.MarketType == "crypto_threshold" || strings.HasPrefix(left.MarketType, "player_") {
		if left.Threshold == "" || right.Threshold == "" {
			return reject(RejectThreshold, left, right)
		}
	}
	if orient, ok := orientation(left, right); ok {
		predicate := struct {
			Genre, EventID, EventStartUTC, Period, MarketType, Selection, PlayerID, Metric, Comparator, Threshold string
			Participants                                                                                          []string
			OutcomeCardinality                                                                                    int
		}{left.Genre, left.EventID, left.EventStartUTC, left.Period, left.MarketType, left.Selection, left.PlayerID,
			left.Metric, left.Comparator, left.Threshold, left.Participants, left.OutcomeCardinality}
		rules := struct {
			Void, Overtime, Tie, Deadline, Timezone string
		}{left.VoidPolicy, left.OvertimePolicy, left.TiePolicy, left.DeadlineUTC, left.Timezone}
		v := Verdict{Compatible: true, LockEligible: true, Orientation: orient,
			ReasonCode: AcceptObjectiveExact, RiskTier: "lock_safe_exact_material_policy",
			TaxonomyVersion: TaxonomyVersion, PredicateHash: hash(predicate), RulesHash: hash(rules),
			NormalizedLeft: left, NormalizedRight: right}
		v.EvidenceHash = hash(v)
		return v
	}
	if left.Comparator != right.Comparator && !comparatorComplement(left.Comparator, right.Comparator) {
		return reject(RejectComparator, left, right)
	}
	if left.Threshold != right.Threshold {
		return reject(RejectThreshold, left, right)
	}
	return reject(RejectSelection, left, right)
}
