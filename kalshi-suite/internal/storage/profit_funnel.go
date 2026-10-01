package storage

// This file is the read-only, full-history view over execution_shadow.db. It intentionally does
// not add a producer, mutate an attempt, or infer a fill from a quote. Every count below comes
// from immutable attempt/event receipts already written by the execution-shadow pipeline.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const ProfitFunnelSchemaVersion = "execution-shadow-profit-funnel-v3"

var ErrInvalidProfitFunnelQuery = errors.New("invalid profit-funnel query")

var profitFunnelDefaultGroupBy = []string{
	"system", "venue", "side", "action", "route", "input_topology",
}

// ProfitFunnelQuery filters on the immutable detector attempt. From is inclusive and To is
// exclusive; both use the shared trigger_unix_ms rather than a later branch/event timestamp.
// A nil GroupBy uses every supported dimension. A non-nil empty GroupBy requests totals only.
type ProfitFunnelQuery struct {
	FromInclusive   *time.Time
	ToExclusive     *time.Time
	Systems         []string
	Venues          []string
	Sides           []string
	Actions         []string
	Routes          []string
	InputTopologies []string
	GroupBy         []string
}

type ProfitFunnelTimeRange struct {
	Field         string  `json:"field"`
	FromInclusive *string `json:"from_inclusive"`
	ToExclusive   *string `json:"to_exclusive"`
	FirstMatched  *string `json:"first_matched"`
	LastMatched   *string `json:"last_matched"`
}

type ProfitFunnelFilterEcho struct {
	Systems         []string `json:"systems"`
	Venues          []string `json:"venues"`
	Sides           []string `json:"sides"`
	Actions         []string `json:"actions"`
	Routes          []string `json:"routes"`
	InputTopologies []string `json:"input_topologies"`
}

type ProfitFunnelLiveTerminals struct {
	Filled   int64 `json:"filled"`
	ZeroFill int64 `json:"zero_fill"`
	NotSent  int64 `json:"not_sent"`
	Unknown  int64 `json:"unknown"`
}

type ProfitFunnelPaperTerminals struct {
	Filled      int64 `json:"filled"`
	ZeroFill    int64 `json:"zero_fill"`
	Rejected    int64 `json:"rejected"`
	NotObserved int64 `json:"not_observed"`
	Pending     int64 `json:"pending"`
}

type ProfitFunnelShadowTerminals struct {
	ModeledFill int64 `json:"modeled_fill"`
	ZeroFill    int64 `json:"zero_fill"`
	NotObserved int64 `json:"not_observed"`
	Pending     int64 `json:"pending"`
}

// USD remains null when no attempt has an observed settlement net in that lane. A real, known
// zero-dollar total is therefore distinguishable from missing economics.
type ProfitFunnelLaneNet struct {
	KnownSettled        int64    `json:"known_settled"`
	SettledFillsMissing int64    `json:"settled_fills_missing_net"`
	USD                 *float64 `json:"usd"`
}

type ProfitFunnelFeeNet struct {
	Live   ProfitFunnelLaneNet `json:"live"`
	Paper  ProfitFunnelLaneNet `json:"paper"`
	Shadow ProfitFunnelLaneNet `json:"shadow"`
}

// ProfitFunnelCohort separates what LIVE provably took, provably skipped, and did not conclusively
// classify. Outcome truth comes from the authoritative settlement receipt. Comparable economics
// use LIVE net for taken attempts and the delayed execution-shadow net for proven skipped attempts;
// they never substitute Paper, a later visible quote, or an unknown LIVE disposition.
type ProfitFunnelCohort struct {
	Total                      int64    `json:"total"`
	OutcomeKnown               int64    `json:"outcome_known"`
	OutcomeUnknown             int64    `json:"outcome_unknown"`
	SettlementYES              int64    `json:"settlement_yes"`
	SettlementNO               int64    `json:"settlement_no"`
	SettlementFractional       int64    `json:"settlement_fractional"`
	LiveEconomicsKnown         int64    `json:"live_economics_known"`
	PaperEconomicsKnown        int64    `json:"paper_economics_known"`
	ShadowEconomicsKnown       int64    `json:"shadow_economics_known"`
	ComparableEconomicsKnown   int64    `json:"comparable_economics_known"`
	ComparableEconomicsUnknown int64    `json:"comparable_economics_unknown"`
	ComparableFeeNetUSD        *float64 `json:"comparable_fee_net_usd"`
}

type ProfitFunnelTakenSkipped struct {
	Taken   ProfitFunnelCohort `json:"taken"`
	Skipped ProfitFunnelCohort `json:"skipped"`
	Unknown ProfitFunnelCohort `json:"unknown"`
}

type ProfitFunnelGaps struct {
	AttemptsWithoutEvents          int64 `json:"attempts_without_events"`
	DetectorReceiptMissing         int64 `json:"detector_receipt_missing"`
	BookEvidenceMissing            int64 `json:"book_evidence_missing"`
	FeeEvidenceMissing             int64 `json:"fee_evidence_missing"`
	VenueAttemptWithoutFeeEvidence int64 `json:"venue_attempt_without_fee_evidence"`
	LiveFillWithoutVenueAttempt    int64 `json:"live_fill_without_venue_attempt"`
	LiveTerminalMissing            int64 `json:"live_terminal_missing"`
	LiveTerminalIncomplete         int64 `json:"live_terminal_incomplete"`
	PaperBranchMissing             int64 `json:"paper_branch_missing"`
	PaperTerminalMissing           int64 `json:"paper_terminal_missing"`
	PaperTerminalIncomplete        int64 `json:"paper_terminal_incomplete"`
	ShadowBranchMissing            int64 `json:"shadow_branch_missing"`
	ShadowTerminalMissing          int64 `json:"shadow_terminal_missing"`
	ShadowTerminalIncomplete       int64 `json:"shadow_terminal_incomplete"`
	Unsettled                      int64 `json:"unsettled"`
	SettledLiveFillMissingNet      int64 `json:"settled_live_fill_missing_net"`
	SettledPaperFillMissingNet     int64 `json:"settled_paper_fill_missing_net"`
	SettledShadowFillMissingNet    int64 `json:"settled_shadow_fill_missing_net"`
	LiveTerminalConflicts          int64 `json:"live_terminal_conflicts"`
	PaperTerminalConflicts         int64 `json:"paper_terminal_conflicts"`
	ShadowTerminalConflicts        int64 `json:"shadow_terminal_conflicts"`
	SupersededLiveReceipts         int64 `json:"superseded_live_receipts"`
}

type ProfitFunnelExclusivity struct {
	// Classified counts only immutable terminal receipts. A missing branch or terminal is a Gap;
	// it is never manufactured into a pending/not-observed terminal category.
	ExpectedAttempts int64 `json:"expected_attempts"`
	LiveClassified   int64 `json:"live_classified"`
	PaperClassified  int64 `json:"paper_classified"`
	ShadowClassified int64 `json:"shadow_classified"`
	LiveGap          int64 `json:"live_gap"`
	PaperGap         int64 `json:"paper_gap"`
	ShadowGap        int64 `json:"shadow_gap"`
}

type ProfitFunnelCounts struct {
	Detected       int64                       `json:"detected"`
	BookValid      int64                       `json:"book_valid"`
	FeeValid       int64                       `json:"fee_valid"`
	VenueAttempted int64                       `json:"venue_attempted"`
	Live           ProfitFunnelLiveTerminals   `json:"live"`
	Paper          ProfitFunnelPaperTerminals  `json:"paper"`
	Shadow         ProfitFunnelShadowTerminals `json:"shadow"`
	Settled        int64                       `json:"settled"`
	FeeNet         ProfitFunnelFeeNet          `json:"fee_net"`
	TakenVsSkipped ProfitFunnelTakenSkipped    `json:"taken_vs_skipped"`
	Gaps           ProfitFunnelGaps            `json:"gaps"`
	Exclusivity    ProfitFunnelExclusivity     `json:"exclusive_branch_terminals"`
}

type ProfitFunnelGroup struct {
	Dimensions map[string]string  `json:"dimensions"`
	Counts     ProfitFunnelCounts `json:"counts"`
}

type ProfitFunnelReport struct {
	SchemaVersion string                 `json:"schema_version"`
	GeneratedAt   string                 `json:"generated_at"`
	TimeRange     ProfitFunnelTimeRange  `json:"time_range"`
	Filters       ProfitFunnelFilterEcho `json:"filters"`
	GroupBy       []string               `json:"group_by"`
	Totals        ProfitFunnelCounts     `json:"totals"`
	Groups        []ProfitFunnelGroup    `json:"groups"`
	FullHistory   bool                   `json:"full_history"`
	DetailLimited bool                   `json:"detail_limited"`
}

type profitFunnelAttemptState struct {
	attempt        ExecutionShadowAttempt
	eventCount     int64
	detectorSeen   bool
	bookValid      bool
	feeValid       bool
	venueAttempted bool

	liveTerminal      *ExecutionShadowEvent
	liveTerminalRank  int
	liveTerminalClass string
	liveTerminalRows  int64
	liveConflict      bool

	paperObserved      bool
	paperTerminal      *ExecutionShadowEvent
	paperTerminalClass string
	paperConflict      bool

	shadowObserved      bool
	shadowEligible      bool
	shadowTerminal      *ExecutionShadowEvent
	shadowTerminalRank  int
	shadowTerminalClass string
	shadowConflict      bool

	settled         bool
	settlementValue *float64
	liveNet         *float64
	paperNet        *float64
	shadowNet       *float64
}

func profitFunnelTimeText(v *time.Time) *string {
	if v == nil {
		return nil
	}
	text := v.UTC().Format(time.RFC3339Nano)
	return &text
}

func normalizeProfitFunnelValues(values []string, upper, allowEmpty bool) []string {
	if values == nil {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		value := strings.TrimSpace(raw)
		if allowEmpty && (strings.EqualFold(value, "<empty>") || strings.EqualFold(value, "(empty)")) {
			value = ""
		}
		if value == "" && !allowEmpty {
			continue
		}
		if upper {
			value = strings.ToUpper(value)
		} else {
			value = strings.ToLower(value)
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func normalizeProfitFunnelQuery(in ProfitFunnelQuery) (ProfitFunnelQuery, error) {
	out := in
	if out.FromInclusive != nil {
		v := out.FromInclusive.UTC()
		out.FromInclusive = &v
	}
	if out.ToExclusive != nil {
		v := out.ToExclusive.UTC()
		out.ToExclusive = &v
	}
	if out.FromInclusive != nil && out.ToExclusive != nil &&
		!out.FromInclusive.Before(*out.ToExclusive) {
		return out, fmt.Errorf("%w: from must be before to", ErrInvalidProfitFunnelQuery)
	}
	out.Systems = normalizeProfitFunnelValues(out.Systems, false, false)
	out.Venues = normalizeProfitFunnelValues(out.Venues, false, false)
	out.Sides = normalizeProfitFunnelValues(out.Sides, true, false)
	out.Actions = normalizeProfitFunnelValues(out.Actions, true, false)
	out.Routes = normalizeProfitFunnelValues(out.Routes, false, false)
	out.InputTopologies = normalizeProfitFunnelValues(out.InputTopologies, false, true)
	for _, venue := range out.Venues {
		if venue != "kalshi" && venue != "polyus" {
			return out, fmt.Errorf("%w: unsupported venue %q", ErrInvalidProfitFunnelQuery, venue)
		}
	}
	for _, side := range out.Sides {
		if side != "YES" && side != "NO" {
			return out, fmt.Errorf("%w: unsupported side %q", ErrInvalidProfitFunnelQuery, side)
		}
	}
	for _, action := range out.Actions {
		if action != "BUY" && action != "SELL" {
			return out, fmt.Errorf("%w: unsupported action %q", ErrInvalidProfitFunnelQuery, action)
		}
	}
	for _, route := range out.Routes {
		if route != "maker" && route != "taker" {
			return out, fmt.Errorf("%w: unsupported route %q", ErrInvalidProfitFunnelQuery, route)
		}
	}
	if out.GroupBy == nil {
		out.GroupBy = append([]string(nil), profitFunnelDefaultGroupBy...)
	} else {
		seen := make(map[string]struct{}, len(out.GroupBy))
		normalized := make([]string, 0, len(out.GroupBy))
		for _, raw := range out.GroupBy {
			field := strings.ToLower(strings.TrimSpace(raw))
			switch field {
			case "system_id":
				field = "system"
			case "topology":
				field = "input_topology"
			}
			if field == "" || field == "none" {
				continue
			}
			switch field {
			case "system", "venue", "side", "action", "route", "input_topology":
			default:
				return out, fmt.Errorf("%w: unsupported group_by %q", ErrInvalidProfitFunnelQuery, raw)
			}
			if _, ok := seen[field]; ok {
				continue
			}
			seen[field] = struct{}{}
			normalized = append(normalized, field)
		}
		out.GroupBy = normalized
	}
	return out, nil
}

func profitFunnelWhere(q ProfitFunnelQuery) (string, []any) {
	clauses := []string{"1=1"}
	args := make([]any, 0, 16)
	if q.FromInclusive != nil {
		clauses = append(clauses, "a.trigger_unix_ms>=?")
		args = append(args, q.FromInclusive.UnixMilli())
	}
	if q.ToExclusive != nil {
		clauses = append(clauses, "a.trigger_unix_ms<?")
		args = append(args, q.ToExclusive.UnixMilli())
	}
	addIn := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		clauses = append(clauses, column+" IN ("+strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")+")")
		for _, value := range values {
			args = append(args, value)
		}
	}
	addIn("LOWER(a.system_id)", q.Systems)
	addIn("LOWER(a.venue)", q.Venues)
	addIn("UPPER(a.side)", q.Sides)
	addIn("UPPER(a.action)", q.Actions)
	addIn("LOWER(a.route)", q.Routes)
	addIn("LOWER(a.input_topology)", q.InputTopologies)
	return strings.Join(clauses, " AND "), args
}

func profitFunnelQualifiedColumns(columns, alias string) string {
	parts := strings.Split(columns, ",")
	for i := range parts {
		parts[i] = alias + "." + strings.TrimSpace(parts[i])
	}
	return strings.Join(parts, ",")
}

func profitFunnelBookValid(attempt ExecutionShadowAttempt, event ExecutionShadowEvent) bool {
	if strings.TrimSpace(event.BookSource) == "" || event.OriginalLimit == nil ||
		event.VisibleDepth == nil || *event.VisibleDepth <= 0 {
		return false
	}
	if strings.EqualFold(attempt.Action, "SELL") {
		return event.SideBid != nil
	}
	return event.SideAsk != nil
}

func profitFunnelLiveRank(event ExecutionShadowEvent) int {
	if strings.TrimSpace(event.LiveState) == "" {
		return 0
	}
	switch event.Stage {
	case "live-exact-reconcile":
		return 3
	case "live-terminal":
		return 2
	}
	if !strings.EqualFold(event.LiveState, "not-sent") || event.VenueAttempted ||
		event.LiveAuthoritative || event.Stage == "combo-candidate-queue" {
		return 0
	}
	if event.Stage == "queue-clear" && event.Evidence != nil &&
		fmt.Sprint(event.Evidence["queue_branch"]) == "live-mirror-combo" {
		return 0
	}
	return 1
}

func profitFunnelLiveClass(event *ExecutionShadowEvent) string {
	if event == nil {
		return "unknown"
	}
	if event.LiveFilledQty != nil && *event.LiveFilledQty > 0 {
		return "filled"
	}
	if !event.VenueAttempted || strings.EqualFold(event.LiveState, "not-sent") {
		return "not_sent"
	}
	if event.LiveAuthoritative && event.LiveFilledQty != nil && *event.LiveFilledQty == 0 {
		return "zero_fill"
	}
	return "unknown"
}

func profitFunnelPaperClass(event *ExecutionShadowEvent) string {
	if event == nil {
		return ""
	}
	state := strings.ToUpper(strings.TrimSpace(event.PaperState))
	if event.PaperFilledQty != nil && *event.PaperFilledQty > 0 && state == "PAPER-FILLED" {
		return "filled"
	}
	switch state {
	case "PAPER-ZERO-FILL":
		return "zero_fill"
	case "PAPER-REJECTED", "REJECTED":
		return "rejected"
	case "PAPER-NOT-OBSERVED", "PAPER-DROPPED", "PAPER-DEFERRED", "PAPER-ERROR":
		return "not_observed"
	case "":
		return "pending"
	default:
		return "pending"
	}
}

func profitFunnelShadowClass(event *ExecutionShadowEvent) string {
	if event == nil {
		return ""
	}
	state := strings.ToLower(strings.TrimSpace(event.ShadowState))
	if state == "" {
		state = strings.ToLower(strings.TrimSpace(event.Outcome))
	}
	if event.ShadowFilledQty != nil && *event.ShadowFilledQty > 0 &&
		(state == "modeled_fill" || state == "filled") {
		return "modeled_fill"
	}
	switch state {
	case "modeled_zero_fill", "zero_fill", "zero-fill", "unfilled":
		return "zero_fill"
	case "not_observed", "not-observed":
		return "not_observed"
	default:
		return "pending"
	}
}

func (state *profitFunnelAttemptState) apply(event ExecutionShadowEvent) {
	state.eventCount++
	if event.Stage == "detector-branch" || strings.Contains(event.Stage, "detector") {
		state.detectorSeen = true
	}
	bookValid := profitFunnelBookValid(state.attempt, event)
	state.bookValid = state.bookValid || bookValid
	state.feeValid = state.feeValid || bookValid && event.FeeQuote != nil &&
		strings.TrimSpace(event.FeeQuoteSource) != ""
	state.venueAttempted = state.venueAttempted || event.VenueAttempted

	if rank := profitFunnelLiveRank(event); rank > 0 {
		class := profitFunnelLiveClass(&event)
		state.liveTerminalRows++
		if state.liveTerminal != nil && rank == state.liveTerminalRank &&
			class != state.liveTerminalClass {
			state.liveConflict = true
		}
		if state.liveTerminal == nil || rank >= state.liveTerminalRank {
			copyEvent := event
			state.liveTerminal, state.liveTerminalRank, state.liveTerminalClass =
				&copyEvent, rank, class
		}
	}

	if event.Stage == "funded-paper-branch" || event.Stage == "funded-paper-scheduler" ||
		event.Stage == "funded-paper-terminal" {
		state.paperObserved = true
	}
	if event.Stage == "funded-paper-terminal" {
		class := profitFunnelPaperClass(&event)
		if state.paperTerminal != nil && class != state.paperTerminalClass &&
			class != "pending" && state.paperTerminalClass != "pending" {
			state.paperConflict = true
		}
		copyEvent := event
		state.paperTerminal, state.paperTerminalClass = &copyEvent, class
	}

	if event.Stage == "live-first-preflight" && strings.EqualFold(event.Outcome, "passed") {
		state.shadowEligible = true
	}
	if strings.HasPrefix(event.Stage, "counterfactual-") {
		state.shadowObserved = true
	}
	shadowRank := 0
	if event.Stage == "counterfactual-terminal" {
		shadowRank = 1
	}
	if event.Stage == "counterfactual-execution-terminal" {
		shadowRank = 2
		// An execution-only terminal supersedes the old fake-portfolio lane. Any settlement net
		// written before it is no longer canonical; a later net-backfill may restore it.
		if state.shadowTerminalRank < 2 {
			state.shadowNet = nil
		}
	}
	if shadowRank > 0 {
		class := profitFunnelShadowClass(&event)
		if state.shadowTerminal != nil && shadowRank == state.shadowTerminalRank &&
			class != state.shadowTerminalClass {
			state.shadowConflict = true
		}
		if state.shadowTerminal == nil || shadowRank >= state.shadowTerminalRank {
			copyEvent := event
			state.shadowTerminal, state.shadowTerminalRank, state.shadowTerminalClass =
				&copyEvent, shadowRank, class
		}
	}

	if event.SettlementKnown {
		state.settled = true
		if event.SettlementValue != nil {
			value := *event.SettlementValue
			state.settlementValue = &value
		}
		if event.LiveNet != nil {
			value := *event.LiveNet
			state.liveNet = &value
		}
		if event.PaperNet != nil {
			value := *event.PaperNet
			state.paperNet = &value
		}
		if event.ShadowNet != nil {
			value := *event.ShadowNet
			state.shadowNet = &value
		}
	}
}

func addProfitFunnelNet(dst *ProfitFunnelLaneNet, src ProfitFunnelLaneNet) {
	dst.KnownSettled += src.KnownSettled
	dst.SettledFillsMissing += src.SettledFillsMissing
	if src.USD != nil {
		if dst.USD == nil {
			zero := 0.0
			dst.USD = &zero
		}
		*dst.USD += *src.USD
	}
}

func addProfitFunnelCohort(dst *ProfitFunnelCohort, src ProfitFunnelCohort) {
	dst.Total += src.Total
	dst.OutcomeKnown += src.OutcomeKnown
	dst.OutcomeUnknown += src.OutcomeUnknown
	dst.SettlementYES += src.SettlementYES
	dst.SettlementNO += src.SettlementNO
	dst.SettlementFractional += src.SettlementFractional
	dst.LiveEconomicsKnown += src.LiveEconomicsKnown
	dst.PaperEconomicsKnown += src.PaperEconomicsKnown
	dst.ShadowEconomicsKnown += src.ShadowEconomicsKnown
	dst.ComparableEconomicsKnown += src.ComparableEconomicsKnown
	dst.ComparableEconomicsUnknown += src.ComparableEconomicsUnknown
	if src.ComparableFeeNetUSD != nil {
		if dst.ComparableFeeNetUSD == nil {
			zero := 0.0
			dst.ComparableFeeNetUSD = &zero
		}
		*dst.ComparableFeeNetUSD += *src.ComparableFeeNetUSD
	}
}

func addProfitFunnelCounts(dst *ProfitFunnelCounts, src ProfitFunnelCounts) {
	dst.Detected += src.Detected
	dst.BookValid += src.BookValid
	dst.FeeValid += src.FeeValid
	dst.VenueAttempted += src.VenueAttempted
	dst.Live.Filled += src.Live.Filled
	dst.Live.ZeroFill += src.Live.ZeroFill
	dst.Live.NotSent += src.Live.NotSent
	dst.Live.Unknown += src.Live.Unknown
	dst.Paper.Filled += src.Paper.Filled
	dst.Paper.ZeroFill += src.Paper.ZeroFill
	dst.Paper.Rejected += src.Paper.Rejected
	dst.Paper.NotObserved += src.Paper.NotObserved
	dst.Paper.Pending += src.Paper.Pending
	dst.Shadow.ModeledFill += src.Shadow.ModeledFill
	dst.Shadow.ZeroFill += src.Shadow.ZeroFill
	dst.Shadow.NotObserved += src.Shadow.NotObserved
	dst.Shadow.Pending += src.Shadow.Pending
	dst.Settled += src.Settled
	addProfitFunnelNet(&dst.FeeNet.Live, src.FeeNet.Live)
	addProfitFunnelNet(&dst.FeeNet.Paper, src.FeeNet.Paper)
	addProfitFunnelNet(&dst.FeeNet.Shadow, src.FeeNet.Shadow)
	addProfitFunnelCohort(&dst.TakenVsSkipped.Taken, src.TakenVsSkipped.Taken)
	addProfitFunnelCohort(&dst.TakenVsSkipped.Skipped, src.TakenVsSkipped.Skipped)
	addProfitFunnelCohort(&dst.TakenVsSkipped.Unknown, src.TakenVsSkipped.Unknown)
	dst.Gaps.AttemptsWithoutEvents += src.Gaps.AttemptsWithoutEvents
	dst.Gaps.DetectorReceiptMissing += src.Gaps.DetectorReceiptMissing
	dst.Gaps.BookEvidenceMissing += src.Gaps.BookEvidenceMissing
	dst.Gaps.FeeEvidenceMissing += src.Gaps.FeeEvidenceMissing
	dst.Gaps.VenueAttemptWithoutFeeEvidence += src.Gaps.VenueAttemptWithoutFeeEvidence
	dst.Gaps.LiveFillWithoutVenueAttempt += src.Gaps.LiveFillWithoutVenueAttempt
	dst.Gaps.LiveTerminalMissing += src.Gaps.LiveTerminalMissing
	dst.Gaps.LiveTerminalIncomplete += src.Gaps.LiveTerminalIncomplete
	dst.Gaps.PaperBranchMissing += src.Gaps.PaperBranchMissing
	dst.Gaps.PaperTerminalMissing += src.Gaps.PaperTerminalMissing
	dst.Gaps.PaperTerminalIncomplete += src.Gaps.PaperTerminalIncomplete
	dst.Gaps.ShadowBranchMissing += src.Gaps.ShadowBranchMissing
	dst.Gaps.ShadowTerminalMissing += src.Gaps.ShadowTerminalMissing
	dst.Gaps.ShadowTerminalIncomplete += src.Gaps.ShadowTerminalIncomplete
	dst.Gaps.Unsettled += src.Gaps.Unsettled
	dst.Gaps.SettledLiveFillMissingNet += src.Gaps.SettledLiveFillMissingNet
	dst.Gaps.SettledPaperFillMissingNet += src.Gaps.SettledPaperFillMissingNet
	dst.Gaps.SettledShadowFillMissingNet += src.Gaps.SettledShadowFillMissingNet
	dst.Gaps.LiveTerminalConflicts += src.Gaps.LiveTerminalConflicts
	dst.Gaps.PaperTerminalConflicts += src.Gaps.PaperTerminalConflicts
	dst.Gaps.ShadowTerminalConflicts += src.Gaps.ShadowTerminalConflicts
	dst.Gaps.SupersededLiveReceipts += src.Gaps.SupersededLiveReceipts
	dst.Exclusivity.ExpectedAttempts += src.Exclusivity.ExpectedAttempts
	dst.Exclusivity.LiveClassified += src.Exclusivity.LiveClassified
	dst.Exclusivity.PaperClassified += src.Exclusivity.PaperClassified
	dst.Exclusivity.ShadowClassified += src.Exclusivity.ShadowClassified
	dst.Exclusivity.LiveGap += src.Exclusivity.LiveGap
	dst.Exclusivity.PaperGap += src.Exclusivity.PaperGap
	dst.Exclusivity.ShadowGap += src.Exclusivity.ShadowGap
}

func profitFunnelAttemptCounts(state *profitFunnelAttemptState) ProfitFunnelCounts {
	var out ProfitFunnelCounts
	out.Detected = 1
	if state.bookValid {
		out.BookValid = 1
	} else {
		out.Gaps.BookEvidenceMissing = 1
	}
	if state.feeValid {
		out.FeeValid = 1
	} else {
		out.Gaps.FeeEvidenceMissing = 1
	}
	if state.venueAttempted {
		out.VenueAttempted = 1
		if !state.feeValid {
			out.Gaps.VenueAttemptWithoutFeeEvidence = 1
		}
	}
	if state.eventCount == 0 {
		out.Gaps.AttemptsWithoutEvents = 1
	}
	if !state.detectorSeen {
		out.Gaps.DetectorReceiptMissing = 1
	}

	liveClass := profitFunnelLiveClass(state.liveTerminal)
	if state.liveTerminal != nil {
		switch liveClass {
		case "filled":
			out.Live.Filled = 1
			if !state.venueAttempted {
				out.Gaps.LiveFillWithoutVenueAttempt = 1
			}
		case "zero_fill":
			out.Live.ZeroFill = 1
		case "not_sent":
			out.Live.NotSent = 1
		default:
			out.Live.Unknown = 1
		}
	}
	if state.liveTerminal == nil {
		out.Gaps.LiveTerminalMissing = 1
	} else if liveClass == "unknown" {
		out.Gaps.LiveTerminalIncomplete = 1
	}
	if state.liveConflict {
		out.Gaps.LiveTerminalConflicts = 1
	}
	if state.liveTerminalRows > 1 {
		out.Gaps.SupersededLiveReceipts = state.liveTerminalRows - 1
	}

	paperClass := state.paperTerminalClass
	if state.paperTerminal != nil {
		switch paperClass {
		case "filled":
			out.Paper.Filled = 1
		case "zero_fill":
			out.Paper.ZeroFill = 1
		case "rejected":
			out.Paper.Rejected = 1
		case "not_observed":
			out.Paper.NotObserved = 1
		default:
			out.Paper.Pending = 1
			out.Gaps.PaperTerminalIncomplete = 1
		}
	} else if state.paperObserved {
		out.Gaps.PaperTerminalMissing = 1
	} else {
		out.Gaps.PaperBranchMissing = 1
	}
	if state.paperConflict {
		out.Gaps.PaperTerminalConflicts = 1
	}

	shadowClass := state.shadowTerminalClass
	if state.shadowTerminal != nil {
		switch shadowClass {
		case "modeled_fill":
			out.Shadow.ModeledFill = 1
		case "zero_fill":
			out.Shadow.ZeroFill = 1
		case "not_observed":
			out.Shadow.NotObserved = 1
		default:
			out.Shadow.Pending = 1
			out.Gaps.ShadowTerminalIncomplete = 1
		}
	} else if state.shadowEligible || state.shadowObserved {
		out.Gaps.ShadowTerminalMissing = 1
	} else {
		out.Gaps.ShadowBranchMissing = 1
	}
	if state.shadowConflict {
		out.Gaps.ShadowTerminalConflicts = 1
	}

	if state.settled {
		out.Settled = 1
	} else {
		out.Gaps.Unsettled = 1
	}
	applyLaneNet := func(dst *ProfitFunnelLaneNet, filled bool, net *float64) {
		if net != nil {
			dst.KnownSettled = 1
			value := *net
			dst.USD = &value
		} else if state.settled && filled {
			dst.SettledFillsMissing = 1
		}
	}
	applyLaneNet(&out.FeeNet.Live, liveClass == "filled", state.liveNet)
	applyLaneNet(&out.FeeNet.Paper, paperClass == "filled", state.paperNet)
	applyLaneNet(&out.FeeNet.Shadow, shadowClass == "modeled_fill", state.shadowNet)
	out.Gaps.SettledLiveFillMissingNet = out.FeeNet.Live.SettledFillsMissing
	out.Gaps.SettledPaperFillMissingNet = out.FeeNet.Paper.SettledFillsMissing
	out.Gaps.SettledShadowFillMissingNet = out.FeeNet.Shadow.SettledFillsMissing

	cohort := &out.TakenVsSkipped.Unknown
	var comparable *float64
	switch liveClass {
	case "filled":
		cohort = &out.TakenVsSkipped.Taken
		comparable = state.liveNet
	case "zero_fill", "not_sent":
		cohort = &out.TakenVsSkipped.Skipped
		comparable = state.shadowNet
	}
	cohort.Total = 1
	if state.settled && state.settlementValue != nil {
		cohort.OutcomeKnown = 1
		switch value := *state.settlementValue; {
		case value == 1:
			cohort.SettlementYES = 1
		case value == 0:
			cohort.SettlementNO = 1
		default:
			cohort.SettlementFractional = 1
		}
	} else {
		cohort.OutcomeUnknown = 1
	}
	if state.liveNet != nil {
		cohort.LiveEconomicsKnown = 1
	}
	if state.paperNet != nil {
		cohort.PaperEconomicsKnown = 1
	}
	if state.shadowNet != nil {
		cohort.ShadowEconomicsKnown = 1
	}
	if comparable != nil {
		cohort.ComparableEconomicsKnown = 1
		value := *comparable
		cohort.ComparableFeeNetUSD = &value
	} else {
		cohort.ComparableEconomicsUnknown = 1
	}

	out.Exclusivity.ExpectedAttempts = 1
	out.Exclusivity.LiveClassified = out.Live.Filled + out.Live.ZeroFill + out.Live.NotSent + out.Live.Unknown
	out.Exclusivity.PaperClassified = out.Paper.Filled + out.Paper.ZeroFill + out.Paper.Rejected +
		out.Paper.NotObserved + out.Paper.Pending
	out.Exclusivity.ShadowClassified = out.Shadow.ModeledFill + out.Shadow.ZeroFill +
		out.Shadow.NotObserved + out.Shadow.Pending
	out.Exclusivity.LiveGap = 1 - out.Exclusivity.LiveClassified
	out.Exclusivity.PaperGap = 1 - out.Exclusivity.PaperClassified
	out.Exclusivity.ShadowGap = 1 - out.Exclusivity.ShadowClassified
	return out
}

func profitFunnelDimensions(attempt ExecutionShadowAttempt, groupBy []string) map[string]string {
	if len(groupBy) == 0 {
		return nil
	}
	out := make(map[string]string, len(groupBy))
	for _, field := range groupBy {
		switch field {
		case "system":
			out[field] = attempt.SystemID
		case "venue":
			out[field] = attempt.Venue
		case "side":
			out[field] = attempt.Side
		case "action":
			out[field] = attempt.Action
		case "route":
			out[field] = attempt.Route
		case "input_topology":
			out[field] = attempt.InputTopology
		}
	}
	return out
}

func profitFunnelGroupKey(dimensions map[string]string, groupBy []string) string {
	var b strings.Builder
	for _, field := range groupBy {
		b.WriteString(field)
		b.WriteByte('=')
		b.WriteString(dimensions[field])
		b.WriteByte(0)
	}
	return b.String()
}

// ExecutionShadowProfitFunnel reads every matching attempt and every matching event. Unlike
// ListExecutionShadowAttempts, it has no detail cap; aggregate totals are therefore independent
// of the diagnostic endpoint's 200/2,000-row display limit.
func (s *Store) ExecutionShadowProfitFunnel(ctx context.Context,
	query ProfitFunnelQuery) (ProfitFunnelReport, error) {
	var report ProfitFunnelReport
	q, err := normalizeProfitFunnelQuery(query)
	if err != nil {
		return report, err
	}
	db, err := s.executionShadowHandle()
	if err != nil {
		return report, err
	}
	where, args := profitFunnelWhere(q)
	rows, err := db.QueryContext(ctx, `SELECT `+executionShadowAttemptColumnsAliasA+`
FROM execution_shadow_attempts AS a WHERE `+where+`
ORDER BY a.trigger_unix_ms,a.attempt_id`, args...)
	if err != nil {
		return report, err
	}
	states := make(map[string]*profitFunnelAttemptState)
	ordered := make([]*profitFunnelAttemptState, 0, 1024)
	var firstMatched, lastMatched *time.Time
	for rows.Next() {
		attempt, scanErr := scanExecutionShadowAttempt(rows)
		if scanErr != nil {
			rows.Close()
			return report, scanErr
		}
		state := &profitFunnelAttemptState{attempt: attempt}
		states[attempt.AttemptID] = state
		ordered = append(ordered, state)
		observed := attempt.ObservedAt.UTC()
		if firstMatched == nil {
			v := observed
			firstMatched = &v
		}
		v := observed
		lastMatched = &v
	}
	if err = rows.Close(); err != nil {
		return report, err
	}
	if err = rows.Err(); err != nil {
		return report, err
	}
	if len(states) > 0 {
		eventRows, queryErr := db.QueryContext(ctx, `SELECT `+
			profitFunnelQualifiedColumns(executionShadowEventColumns, "e")+`
FROM execution_shadow_events AS e
JOIN execution_shadow_attempts AS a ON a.attempt_id=e.attempt_id
WHERE `+where+` ORDER BY a.trigger_unix_ms,a.attempt_id,e.id`, args...)
		if queryErr != nil {
			return report, queryErr
		}
		for eventRows.Next() {
			event, scanErr := scanExecutionShadowEvent(eventRows)
			if scanErr != nil {
				eventRows.Close()
				return report, scanErr
			}
			if state := states[event.AttemptID]; state != nil {
				state.apply(event)
			}
		}
		if err = eventRows.Close(); err != nil {
			return report, err
		}
		if err = eventRows.Err(); err != nil {
			return report, err
		}
	}

	report = ProfitFunnelReport{
		SchemaVersion: ProfitFunnelSchemaVersion,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		TimeRange: ProfitFunnelTimeRange{Field: "trigger_unix_ms",
			FromInclusive: profitFunnelTimeText(q.FromInclusive),
			ToExclusive:   profitFunnelTimeText(q.ToExclusive),
			FirstMatched:  profitFunnelTimeText(firstMatched),
			LastMatched:   profitFunnelTimeText(lastMatched)},
		Filters: ProfitFunnelFilterEcho{Systems: q.Systems, Venues: q.Venues,
			Sides: q.Sides, Actions: q.Actions, Routes: q.Routes,
			InputTopologies: q.InputTopologies},
		GroupBy: append([]string(nil), q.GroupBy...), Groups: []ProfitFunnelGroup{},
		FullHistory: true, DetailLimited: false,
	}
	type groupedCounts struct {
		dimensions map[string]string
		counts     ProfitFunnelCounts
	}
	groups := make(map[string]*groupedCounts)
	for _, state := range ordered {
		counts := profitFunnelAttemptCounts(state)
		addProfitFunnelCounts(&report.Totals, counts)
		if len(q.GroupBy) == 0 {
			continue
		}
		dimensions := profitFunnelDimensions(state.attempt, q.GroupBy)
		key := profitFunnelGroupKey(dimensions, q.GroupBy)
		group := groups[key]
		if group == nil {
			group = &groupedCounts{dimensions: dimensions}
			groups[key] = group
		}
		addProfitFunnelCounts(&group.counts, counts)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		group := groups[key]
		report.Groups = append(report.Groups, ProfitFunnelGroup{
			Dimensions: group.dimensions, Counts: group.counts,
		})
	}
	return report, nil
}
