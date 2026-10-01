package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchstats"
)

const (
	// v4 selects the exact immutable instrument version frozen on each observation and refuses an
	// inference window while a raw Step-7 fact in that window still has a pending projection.
	// v3 receipts cannot be reused because both row eligibility and window completeness changed.
	R139InferencePipelineVersion = 4
	r139InferencePageRows        = 10000
	r139InferenceReplicates      = 10000
)

// ResearchInferenceSystemResult is one and only one result for an immutable R138 system spec.
// The rolling final slice is monitoring, not untouched replication. A true untouched gate remains
// hard-disabled until a freeze manifest exists before test rows and a one-time sealed evaluation runs.
type ResearchInferenceSystemResult struct {
	SystemID                       string                       `json:"system_id"`
	ExperimentVersion              int                          `json:"experiment_version"`
	State                          string                       `json:"state"`
	Reason                         string                       `json:"reason"`
	TerminalRows                   int                          `json:"terminal_rows"`
	TrainEvents                    int                          `json:"train_events"`
	ValidationEvents               int                          `json:"validation_events"`
	MonitoringEvents               int                          `json:"monitoring_events"`
	PurgedEvents                   int                          `json:"purged_events"`
	ValidationDays                 int                          `json:"validation_days"`
	ValidationMean                 float64                      `json:"validation_mean"`
	ValidationLower                float64                      `json:"validation_lower"`
	ValidationUpper                float64                      `json:"validation_upper"`
	ValidationBoundsKnown          bool                         `json:"validation_bounds_known"`
	MonitoringDays                 int                          `json:"monitoring_days"`
	MonitoringMean                 float64                      `json:"monitoring_mean"`
	MonitoringLower                float64                      `json:"monitoring_lower"`
	MonitoringUpper                float64                      `json:"monitoring_upper"`
	MonitoringBoundsKnown          bool                         `json:"monitoring_bounds_known"`
	RawP                           float64                      `json:"raw_p"`
	HolmP                          float64                      `json:"holm_p"`
	BYQ                            float64                      `json:"by_q"`
	MonitoringLowerBoundPositive   bool                         `json:"monitoring_lower_bound_positive"`
	UntouchedEvents                int                          `json:"untouched_events"`
	UntouchedDays                  int                          `json:"untouched_days"`
	UntouchedMean                  float64                      `json:"untouched_mean"`
	UntouchedLower                 float64                      `json:"untouched_lower"`
	UntouchedUpper                 float64                      `json:"untouched_upper"`
	UntouchedP                     float64                      `json:"untouched_p"`
	UntouchedBoundsKnown           bool                         `json:"untouched_bounds_known"`
	PreregisteredUntouchedGatePass bool                         `json:"preregistered_untouched_gate_pass"`
	Cells                          []researchstats.CellEstimate `json:"cells"`
	ExecutionCandidate             bool                         `json:"execution_candidate"`
	Funded                         bool                         `json:"funded"`
	PaperAuthority                 bool                         `json:"paper_authority"`
	LiveAuthority                  bool                         `json:"live_authority"`
}

type ResearchInferenceExclusions struct {
	Open       int `json:"open"`
	Void       int `json:"void"`
	Censored   int `json:"censored"`
	NonExact   int `json:"nonexact"`
	Identity   int `json:"identity"`
	RouteTruth int `json:"route_truth"`
}

type ResearchInferenceReport struct {
	RunID                      int64                           `json:"run_id"`
	CreatedAt                  string                          `json:"created_at"`
	AsOfCompleted              string                          `json:"as_of_completed_ts"`
	PipelineVersion            int                             `json:"pipeline_version"`
	InputManifestHash          string                          `json:"input_manifest_hash"`
	ResultHash                 string                          `json:"result_hash"`
	PreregistrationID          string                          `json:"preregistration_id,omitempty"`
	Status                     string                          `json:"status"`
	ObservedRows               int                             `json:"observed_rows"`
	ExactTerminalRows          int                             `json:"exact_terminal_rows"`
	Exclusions                 ResearchInferenceExclusions     `json:"exclusions"`
	SystemsReported            int                             `json:"systems_reported"`
	Results                    []ResearchInferenceSystemResult `json:"systems"`
	Funded                     bool                            `json:"funded"`
	PaperAuthority             bool                            `json:"paper_authority"`
	LiveAuthority              bool                            `json:"live_authority"`
	MoneyTruth                 string                          `json:"money_truth"`
	InferenceTruth             string                          `json:"inference_truth"`
	TerminalOnlyMonitoring     bool                            `json:"terminal_only_monitoring"`
	PreregisteredFreezePresent bool                            `json:"preregistered_freeze_present"`
}

type r139RawInferenceRow struct {
	ObservationID        int64    `json:"observation_id"`
	ObservedTS           string   `json:"observed_ts"`
	SystemID             string   `json:"system_id"`
	ExperimentVersion    int      `json:"experiment_version"`
	CanonicalEventID     string   `json:"canonical_event_id"`
	EventVersion         int      `json:"event_version"`
	CanonicalPayoffID    string   `json:"canonical_payoff_id"`
	PayoffVersion        int      `json:"payoff_version"`
	InstrumentVersion    int      `json:"instrument_version"`
	Ticker               string   `json:"ticker"`
	Venue                string   `json:"venue"`
	Route                string   `json:"route"`
	CertificateStatus    string   `json:"certificate_status"`
	SourceClockID        string   `json:"source_clock_id"`
	BookSource           string   `json:"book_source"`
	FeeSource            string   `json:"fee_source"`
	Size                 float64  `json:"size"`
	Tick                 float64  `json:"tick"`
	VisibleCapacity      float64  `json:"visible_capacity"`
	LatencyKnown         bool     `json:"latency_known"`
	QuoteAgeKnown        bool     `json:"quote_age_known"`
	TickKnown            bool     `json:"tick_known"`
	DepthKnown           bool     `json:"depth_known"`
	FeeKnown             bool     `json:"fee_known"`
	ObservationStatus    string   `json:"observation_status"`
	CanonicalEventKnown  bool     `json:"canonical_event_known"`
	CanonicalPayoffKnown bool     `json:"canonical_payoff_known"`
	PayoffUpdateID       int64    `json:"payoff_update_id"`
	PayoffUpdateTS       string   `json:"payoff_update_ts"`
	PayoffStatus         string   `json:"payoff_status"`
	PayoutLower          float64  `json:"payout_lower"`
	PayoutUpper          float64  `json:"payout_upper"`
	RealizedNet          *float64 `json:"realized_net"`
}

type r139TerminalObservation struct {
	SystemID, EventID, Venue, Route string
	ExperimentVersion               int
	Day                             time.Time
	Value                           float64
}

type r139InputManifest struct {
	PipelineVersion          int    `json:"pipeline_version"`
	AsOfCompletedTS          string `json:"as_of_completed_ts"`
	SelectionContract        string `json:"selection_contract"`
	ObservedRows             int    `json:"observed_rows"`
	MaxObservationID         int64  `json:"max_observation_id"`
	MaxPayoffUpdateID        int64  `json:"max_payoff_update_id"`
	ProjectionCompletedCount int    `json:"projection_completed_count"`
	ProjectionMaxCompletedTS string `json:"projection_max_completed_ts"`
	ProjectionManifestHash   string `json:"projection_manifest_hash"`
	RowsHash                 string `json:"rows_hash"`
	RowLimit                 int    `json:"row_limit"` // always zero in v2: keyset pages never truncate the snapshot
	PageRows                 int    `json:"page_rows"`
	SystemsHash              string `json:"systems_hash"`
}

type r139InferenceContract struct {
	SystemID, SpecHash    string
	Version, RequiredDays int
}

func (s *Store) r139InferenceContracts(ctx context.Context) ([]r139InferenceContract, error) {
	ids := ResearchExperimentIDs()
	if len(ids) != 19 {
		return nil, fmt.Errorf("inference registry count=%d, want 19", len(ids))
	}
	out := make([]r139InferenceContract, 0, len(ids))
	for _, id := range ids {
		var c r139InferenceContract
		c.SystemID = id
		if err := s.db.QueryRowContext(ctx, `SELECT version,spec_hash,required_days FROM research_experiment_specs
WHERE experiment_id=? ORDER BY version DESC LIMIT 1`, id).Scan(&c.Version, &c.SpecHash, &c.RequiredDays); err != nil {
			return nil, fmt.Errorf("inference contract %s: %w", id, err)
		}
		if c.Version <= 0 || c.RequiredDays < 2 || c.SpecHash == "" {
			return nil, fmt.Errorf("invalid inference contract for %s", id)
		}
		out = append(out, c)
	}
	return out, nil
}

func r139Hash(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:]), nil
}

func r139Day(raw string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, raw); err == nil {
			return t.UTC().Truncate(24 * time.Hour), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid observation timestamp %q", raw)
}

// r139LoadInferenceSnapshot keyset-pages one immutable observation/update-ID snapshot. It hashes
// every raw row in database order, but retains only exact terminal observations for inference, so
// a growing research ledger neither truncates at an arbitrary row count nor recreates the prior
// whole-ledger memory spike. pageRows is injectable only for deterministic pagination tests.
func (s *Store) r139LoadInferenceSnapshot(ctx context.Context, completedCutoff, asOf time.Time,
	pageRows int) ([]r139TerminalObservation, ResearchInferenceExclusions, string, int, int64, int64, error) {
	if pageRows <= 0 {
		return nil, ResearchInferenceExclusions{}, "", 0, 0, 0, errors.New("inference page size must be positive")
	}
	var observed int
	var maxObservationID, maxUpdateID int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(id),0)
FROM research_system_observations
WHERE observation_kind!='control' AND julianday(observed_ts)<julianday(?)`,
		completedCutoff.Format(time.RFC3339Nano)).
		Scan(&observed, &maxObservationID); err != nil {
		return nil, ResearchInferenceExclusions{}, "", 0, 0, 0, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0)
FROM research_system_payoff_updates WHERE julianday(observed_ts)<=julianday(?)`,
		asOf.Format(time.RFC3339Nano)).Scan(&maxUpdateID); err != nil {
		return nil, ResearchInferenceExclusions{}, "", 0, 0, 0, err
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("["))
	firstHashRow := true
	terminal := make([]r139TerminalObservation, 0)
	excluded := ResearchInferenceExclusions{}
	lastObservationID := int64(0)
	for lastObservationID < maxObservationID {
		rows, err := s.db.QueryContext(ctx, `SELECT o.id,o.observed_ts,o.system_id,o.experiment_version,o.canonical_event_id,o.event_version,
COALESCE(o.canonical_payoff_id,''),COALESCE(o.payoff_version,0),o.instrument_version,o.ticker,
o.venue,o.route,o.certificate_status,o.source_clock_id,o.book_source,o.fee_source,o.size_units,
o.tick_min,o.visible_capacity,o.latency_known,o.quote_age_known,o.tick_known,o.depth_known,o.fee_known,
o.outcome_status,EXISTS(SELECT 1 FROM research_event_specs e WHERE e.event_id=o.canonical_event_id AND e.version=o.event_version),
EXISTS(SELECT 1 FROM research_instrument_specs i
 JOIN research_payoff_specs p ON p.event_id=i.event_id AND p.event_version=i.event_version
  AND p.payoff_id=i.payoff_id AND p.version=i.payoff_version
 WHERE i.venue=o.venue AND i.ticker=o.ticker AND i.event_id=o.canonical_event_id
  AND i.event_version=o.event_version AND i.payoff_id=o.canonical_payoff_id
  AND i.payoff_version=o.payoff_version AND o.instrument_version>0
  AND i.version=o.instrument_version),
u.id,u.observed_ts,u.status,u.payout_lower,u.payout_upper,u.realized_net
FROM research_system_observations o
LEFT JOIN research_system_payoff_updates u ON u.id=(
 SELECT u2.id FROM research_system_payoff_updates u2
	 WHERE u2.observation_id=o.id AND u2.id<=?
	  AND julianday(u2.observed_ts)<=julianday(?) ORDER BY u2.id DESC LIMIT 1)
WHERE o.observation_kind!='control' AND julianday(o.observed_ts)<julianday(?)
 AND o.id>? AND o.id<=?
ORDER BY o.id LIMIT ?`, maxUpdateID, asOf.Format(time.RFC3339Nano), completedCutoff.Format(time.RFC3339Nano),
			lastObservationID, maxObservationID, pageRows)
		if err != nil {
			return nil, excluded, "", 0, 0, 0, err
		}
		page := make([]r139RawInferenceRow, 0, pageRows)
		for rows.Next() {
			var r r139RawInferenceRow
			var latency, quoteAge, tick, depth, fee, canonicalEvent, canonicalPayoff int
			var updateID sql.NullInt64
			var updateTS, updateStatus sql.NullString
			var payoutLower, payoutUpper, realized sql.NullFloat64
			if err := rows.Scan(&r.ObservationID, &r.ObservedTS, &r.SystemID, &r.ExperimentVersion, &r.CanonicalEventID,
				&r.EventVersion, &r.CanonicalPayoffID, &r.PayoffVersion, &r.InstrumentVersion, &r.Ticker,
				&r.Venue, &r.Route, &r.CertificateStatus, &r.SourceClockID,
				&r.BookSource, &r.FeeSource, &r.Size, &r.Tick, &r.VisibleCapacity, &latency,
				&quoteAge, &tick, &depth, &fee, &r.ObservationStatus, &canonicalEvent, &canonicalPayoff, &updateID,
				&updateTS, &updateStatus, &payoutLower, &payoutUpper, &realized); err != nil {
				rows.Close()
				return nil, excluded, "", 0, 0, 0, err
			}
			r.LatencyKnown, r.QuoteAgeKnown, r.TickKnown = latency == 1, quoteAge == 1, tick == 1
			r.DepthKnown, r.FeeKnown = depth == 1, fee == 1
			r.CanonicalEventKnown, r.CanonicalPayoffKnown = canonicalEvent == 1, canonicalPayoff == 1
			if updateID.Valid {
				r.PayoffUpdateID, r.PayoffUpdateTS, r.PayoffStatus = updateID.Int64, updateTS.String, updateStatus.String
				r.PayoutLower, r.PayoutUpper = payoutLower.Float64, payoutUpper.Float64
				if realized.Valid {
					v := realized.Float64
					r.RealizedNet = &v
				}
			}
			page = append(page, r)
			lastObservationID = r.ObservationID
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, excluded, "", 0, 0, 0, err
		}
		if err := rows.Close(); err != nil {
			return nil, excluded, "", 0, 0, 0, err
		}
		if len(page) == 0 {
			break
		}
		for _, r := range page {
			encoded, err := json.Marshal(r)
			if err != nil {
				return nil, excluded, "", 0, 0, 0, err
			}
			if !firstHashRow {
				_, _ = hasher.Write([]byte(","))
			}
			firstHashRow = false
			_, _ = hasher.Write(encoded)
		}
		pageTerminal, pageExcluded, err := r139ClassifyRows(page)
		if err != nil {
			return nil, excluded, "", 0, 0, 0, err
		}
		terminal = append(terminal, pageTerminal...)
		excluded.Open += pageExcluded.Open
		excluded.Void += pageExcluded.Void
		excluded.Censored += pageExcluded.Censored
		excluded.NonExact += pageExcluded.NonExact
		excluded.Identity += pageExcluded.Identity
		excluded.RouteTruth += pageExcluded.RouteTruth
		if len(page) < pageRows {
			break
		}
	}
	_, _ = hasher.Write([]byte("]"))
	rowsHash := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	return terminal, excluded, rowsHash, observed, maxObservationID, maxUpdateID, nil
}

func r139ClassifyRows(rows []r139RawInferenceRow) ([]r139TerminalObservation, ResearchInferenceExclusions, error) {
	terminal := make([]r139TerminalObservation, 0, len(rows))
	var excluded ResearchInferenceExclusions
	for _, r := range rows {
		status := strings.ToLower(r.ObservationStatus)
		if r.PayoffUpdateID > 0 {
			status = strings.ToLower(r.PayoffStatus)
		}
		isVoid := status == "voided"
		switch status {
		case "open", "":
			excluded.Open++
			continue
		case "voided": // an exact scalar void/refund with realized net is valid terminal money truth
		case "censored":
			excluded.Censored++
			continue
		case "settled":
		default:
			excluded.NonExact++
			continue
		}
		if r.PayoffUpdateID == 0 || r.RealizedNet == nil || math.Abs(r.PayoutUpper-r.PayoutLower) > 1e-12 ||
			math.IsNaN(*r.RealizedNet) || math.IsInf(*r.RealizedNet, 0) {
			if isVoid {
				excluded.Void++
			} else {
				excluded.NonExact++
			}
			continue
		}
		if r.CanonicalEventID == "" || r.EventVersion <= 0 || !r.CanonicalEventKnown ||
			r.CanonicalPayoffID == "" || r.PayoffVersion <= 0 || r.Ticker == "" || !r.CanonicalPayoffKnown ||
			(r.CertificateStatus != "verified" && r.CertificateStatus != "not_applicable") {
			excluded.Identity++
			continue
		}
		routeOK := r.Route == "taker" || r.Route == "maker" || r.Route == "maker-control" || r.Route == "rfq"
		if !routeOK || r.Size <= 0 || r.Tick <= 0 || r.VisibleCapacity+1e-12 < r.Size ||
			r.SourceClockID == "" || r.BookSource == "" || r.FeeSource == "" || !r.LatencyKnown ||
			!r.QuoteAgeKnown || !r.TickKnown || !r.DepthKnown || !r.FeeKnown {
			excluded.RouteTruth++
			continue
		}
		day, err := r139Day(r.ObservedTS)
		if err != nil {
			return nil, excluded, err
		}
		terminal = append(terminal, r139TerminalObservation{SystemID: r.SystemID,
			ExperimentVersion: r.ExperimentVersion,
			EventID:           r.CanonicalEventID, Venue: strings.ToLower(r.Venue),
			Route: r.Route, Day: day, Value: *r.RealizedNet / r.Size})
	}
	return terminal, excluded, nil
}

func r139DayTotals(rows []researchstats.Observation, end time.Time) ([]float64, error) {
	if len(rows) == 0 {
		return nil, errors.New("no completed days")
	}
	start := rows[0].Day.UTC().Truncate(24 * time.Hour)
	for _, r := range rows[1:] {
		d := r.Day.UTC().Truncate(24 * time.Hour)
		if d.Before(start) {
			start = d
		}
	}
	end = end.UTC().Truncate(24 * time.Hour)
	if end.Before(start) {
		return nil, errors.New("completed-day window predates partition")
	}
	days := int(end.Sub(start)/(24*time.Hour)) + 1
	if days > 3660 {
		return nil, errors.New("completed-day window exceeds ten-year safety bound")
	}
	totals := make([]float64, days)
	for _, r := range rows {
		i := int(r.Day.UTC().Truncate(24*time.Hour).Sub(start) / (24 * time.Hour))
		if i >= 0 && i < len(totals) {
			totals[i] += r.Value
		}
	}
	return totals, nil
}

func r139Variance(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))
	if len(values) == 1 {
		return mean, 0
	}
	v := 0.0
	for _, x := range values {
		v += (x - mean) * (x - mean)
	}
	return mean, v / float64(len(values)-1)
}

func r139Seed(system string, salt int64) int64 {
	h := sha256.Sum256([]byte(system))
	var v int64
	for i := 0; i < 8; i++ {
		v = (v << 8) | int64(h[i])
	}
	return v ^ salt
}

func r139BlockedResults(contracts []r139InferenceContract, state, reason string) []ResearchInferenceSystemResult {
	out := make([]ResearchInferenceSystemResult, len(contracts))
	for i, contract := range contracts {
		out[i] = ResearchInferenceSystemResult{SystemID: contract.SystemID, ExperimentVersion: contract.Version, State: state, Reason: reason,
			RawP: 1, HolmP: 1, BYQ: 1, Cells: []researchstats.CellEstimate{}}
	}
	return out
}

func r139InferSystems(terminal []r139TerminalObservation, completedCutoff time.Time, contracts []r139InferenceContract) ([]ResearchInferenceSystemResult, error) {
	if len(contracts) != 19 {
		return nil, fmt.Errorf("inference contract count=%d, want 19", len(contracts))
	}
	bySystem := make(map[string][]r139TerminalObservation, len(contracts))
	activeVersion := make(map[string]int, len(contracts))
	for _, contract := range contracts {
		activeVersion[contract.SystemID] = contract.Version
	}
	for _, o := range terminal {
		if o.ExperimentVersion != activeVersion[o.SystemID] {
			continue
		}
		bySystem[o.SystemID] = append(bySystem[o.SystemID], o)
	}
	results := make([]ResearchInferenceSystemResult, len(contracts))
	cellValues := map[string][]float64{}
	for i, contract := range contracts {
		id := contract.SystemID
		result := ResearchInferenceSystemResult{SystemID: id, ExperimentVersion: contract.Version, RawP: 1, HolmP: 1, BYQ: 1,
			Cells: []researchstats.CellEstimate{}}
		input := bySystem[id]
		result.TerminalRows = len(input)
		if len(input) == 0 {
			result.State = "INSUFFICIENT_NO_EXACT_TERMINAL_ROWS"
			result.Reason = "no settled, exact-money, canonical-event, complete-route observations"
			results[i] = result
			continue
		}
		rows := make([]researchstats.Observation, 0, len(input))
		for _, o := range input {
			cell := o.SystemID + "|" + o.Venue + "|" + o.Route
			rows = append(rows, researchstats.Observation{EventID: o.EventID, Day: o.Day, Cell: cell, Value: o.Value})
		}
		split, err := researchstats.ChronologicalEventSplit(rows, .50, .25, 1)
		if err != nil {
			result.State, result.Reason = "INSUFFICIENT_CHRONOLOGICAL_SPLIT", err.Error()
			results[i] = result
			continue
		}
		result.TrainEvents, result.ValidationEvents, result.MonitoringEvents = len(split.TrainEvents), len(split.ValidationEvents), len(split.TestEvents)
		result.PurgedEvents = len(split.PurgedEvents)
		valEnd := split.Validation[0].Day
		for _, o := range split.Validation {
			if o.Day.After(valEnd) {
				valEnd = o.Day
			}
		}
		valDays, err := r139DayTotals(split.Validation, valEnd)
		if err == nil && len(valDays) >= 2 {
			b, boundsErr := researchstats.DeterministicDayBlockBounds(valDays, r139InferenceReplicates, r139Seed(id, 1391))
			if boundsErr == nil {
				result.ValidationDays, result.ValidationMean = b.Days, b.Mean
				result.ValidationLower, result.ValidationUpper, result.ValidationBoundsKnown = b.Lower, b.Upper, true
			}
		}
		monitoringEnd := completedCutoff.Add(-24 * time.Hour)
		monitoringDays, err := r139DayTotals(split.Test, monitoringEnd)
		if err != nil || len(monitoringDays) < 2 {
			result.State, result.Reason = "INSUFFICIENT_MONITORING_DAYS", "rolling final-slice monitor has fewer than two completed UTC days"
			results[i] = result
			continue
		}
		b, err := researchstats.DeterministicDayBlockBounds(monitoringDays, r139InferenceReplicates, r139Seed(id, 1392))
		if err != nil {
			result.State, result.Reason = "INSUFFICIENT_MONITORING_DAYS", err.Error()
			results[i] = result
			continue
		}
		result.MonitoringDays, result.MonitoringMean, result.MonitoringLower, result.MonitoringUpper, result.MonitoringBoundsKnown = b.Days, b.Mean, b.Lower, b.Upper, true
		result.RawP, err = researchstats.DeterministicDaySignP(monitoringDays, r139InferenceReplicates, r139Seed(id, 1393))
		if err != nil {
			return nil, err
		}
		for _, o := range split.Test {
			cellValues[o.Cell] = append(cellValues[o.Cell], o.Value)
		}
		if result.ValidationDays < 2 {
			result.State, result.Reason = "INSUFFICIENT_VALIDATION_DAYS", "validation partition has fewer than two completed UTC days"
		} else if result.MonitoringDays < contract.RequiredDays {
			result.State = "INSUFFICIENT_MONITORING_DAYS"
			result.Reason = fmt.Sprintf("%d/%d completed rolling-monitor UTC days; quiet days are included", result.MonitoringDays, contract.RequiredDays)
		} else {
			result.State, result.Reason = "READY_FOR_DEPENDENCE_SAFE_CORRECTION", "bounds computed; waiting for all-19 Holm and BY correction"
		}
		results[i] = result
	}

	cellInputs := make([]researchstats.CellEstimate, 0, len(cellValues))
	for cell, values := range cellValues {
		mean, variance := r139Variance(values)
		cellInputs = append(cellInputs, researchstats.CellEstimate{Cell: cell, N: len(values), Mean: mean, Variance: variance})
	}
	cellBySystem := map[string][]researchstats.CellEstimate{}
	if len(cellInputs) > 0 {
		shrunk, err := researchstats.ShrinkNormalCells(cellInputs, 1e-6)
		if err != nil {
			return nil, err
		}
		for _, cell := range shrunk.Cells {
			system := strings.SplitN(cell.Cell, "|", 2)[0]
			cellBySystem[system] = append(cellBySystem[system], cell)
		}
	}
	tests := make([]researchstats.TestP, len(results))
	for i := range results {
		tests[i] = researchstats.TestP{ID: results[i].SystemID, P: results[i].RawP}
	}
	adjusted, err := researchstats.AdjustDependenceSafe(tests)
	if err != nil {
		return nil, err
	}
	byID := map[string]researchstats.TestP{}
	for _, v := range adjusted {
		byID[v.ID] = v
	}
	for i := range results {
		adj := byID[results[i].SystemID]
		results[i].HolmP, results[i].BYQ = adj.HolmP, adj.BYQ
		results[i].Cells = cellBySystem[results[i].SystemID]
		if results[i].State != "READY_FOR_DEPENDENCE_SAFE_CORRECTION" {
			continue
		}
		monitorPass := results[i].ValidationBoundsKnown && results[i].ValidationLower > 0 &&
			results[i].MonitoringBoundsKnown && results[i].MonitoringLower > 0 && adj.HolmP <= .05 && adj.BYQ <= .10
		results[i].MonitoringLowerBoundPositive = monitorPass
		results[i].PreregisteredUntouchedGatePass = false
		if monitorPass {
			results[i].State = "AWAITING_PREREGISTERED_UNTOUCHED_REPLICATION"
			results[i].Reason = "rolling final-slice monitoring bounds clear, but no freeze manifest predates the test rows and no one-time sealed evaluation exists"
		} else {
			results[i].State = "MONITORING_NULL_NOT_REJECTED"
			results[i].Reason = "rolling final-slice monitoring did not clear validation/monitoring lower bounds plus Holm and BY; it is not untouched proof in either case"
		}
	}
	return results, nil
}

func (s *Store) storeR139Inference(ctx context.Context, asOfCompleted time.Time, manifest r139InputManifest,
	status string, observed int, excluded ResearchInferenceExclusions, exact int,
	results []ResearchInferenceSystemResult) (ResearchInferenceReport, bool, error) {
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	manifestHash, err := r139Hash(manifest)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	resultHash, err := r139Hash(results)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	defer tx.Rollback()
	projectionManifest := Step7ProjectionCompletionManifest{
		Count:          manifest.ProjectionCompletedCount,
		MaxCompletedTS: manifest.ProjectionMaxCompletedTS,
		Hash:           manifest.ProjectionManifestHash,
	}
	if err := validateStep7ProjectionFenceTx(ctx, tx, time.Time{}, asOfCompleted,
		projectionManifest); err != nil {
		return ResearchInferenceReport{}, false, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_inference_runs(
created_ts,as_of_completed_ts,pipeline_version,input_manifest_hash,input_manifest_json,result_hash,status,
observed_rows,exact_terminal_rows,excluded_open,excluded_void,excluded_censored,excluded_nonexact,
excluded_identity,excluded_route_truth,systems_reported,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, now, asOfCompleted.Format(time.RFC3339Nano),
		R139InferencePipelineVersion, manifestHash, string(manifestJSON), resultHash, status, observed, exact,
		excluded.Open, excluded.Void, excluded.Censored, excluded.NonExact, excluded.Identity,
		excluded.RouteTruth, len(results))
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	insertedN, err := res.RowsAffected()
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	var runID int64
	if insertedN == 0 {
		if err := tx.QueryRowContext(ctx, `SELECT id FROM research_inference_runs WHERE pipeline_version=? AND input_manifest_hash=?`,
			R139InferencePipelineVersion, manifestHash).Scan(&runID); err != nil {
			return ResearchInferenceReport{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return ResearchInferenceReport{}, false, err
		}
		report, err := s.ResearchInferenceRun(ctx, runID)
		return report, false, err
	}
	runID, err = res.LastInsertId()
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	if len(results) != 19 {
		return ResearchInferenceReport{}, false, fmt.Errorf("inference result count=%d, want 19", len(results))
	}
	seen := map[string]bool{}
	for _, r := range results {
		if seen[r.SystemID] {
			return ResearchInferenceReport{}, false, fmt.Errorf("duplicate inference result %s", r.SystemID)
		}
		seen[r.SystemID] = true
		cellsJSON, err := json.Marshal(r.Cells)
		if err != nil {
			return ResearchInferenceReport{}, false, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO research_inference_system_results(
run_id,system_id,experiment_version,state,reason,terminal_rows,train_events,validation_events,monitoring_events,purged_events,
validation_days,validation_mean,validation_lower,validation_upper,validation_bounds_known,monitoring_days,
monitoring_mean,monitoring_lower,monitoring_upper,monitoring_bounds_known,raw_p,holm_p,by_q,
monitoring_lower_bound_positive,preregistered_untouched_gate_pass,cells_json,
execution_candidate,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,0,0,0,0)`, runID, r.SystemID, r.ExperimentVersion, r.State, r.Reason,
			r.TerminalRows, r.TrainEvents, r.ValidationEvents, r.MonitoringEvents, r.PurgedEvents,
			r.ValidationDays, r.ValidationMean, r.ValidationLower, r.ValidationUpper, boolInt(r.ValidationBoundsKnown),
			r.MonitoringDays, r.MonitoringMean, r.MonitoringLower, r.MonitoringUpper, boolInt(r.MonitoringBoundsKnown), r.RawP, r.HolmP,
			r.BYQ, boolInt(r.MonitoringLowerBoundPositive), string(cellsJSON))
		if err != nil {
			return ResearchInferenceReport{}, false, err
		}
	}
	if len(seen) != 19 {
		return ResearchInferenceReport{}, false, errors.New("inference result registry coverage mismatch")
	}
	if err := tx.Commit(); err != nil {
		return ResearchInferenceReport{}, false, err
	}
	report, err := s.ResearchInferenceRun(ctx, runID)
	return report, true, err
}

// RunR139ResearchInference executes one bounded deterministic pass. A repeated input manifest is
// deduplicated to its prior immutable receipt. It never writes an experiment result/promotion event.
func (s *Store) RunR139ResearchInference(ctx context.Context, asOf time.Time) (ResearchInferenceReport, bool, error) {
	if asOf.IsZero() {
		asOf = time.Now()
	}
	asOf = asOf.UTC()
	completedCutoff := asOf.Truncate(24 * time.Hour)
	if pending, err := s.Step7ProjectionPendingInWindow(ctx, time.Time{}, completedCutoff); err != nil {
		return ResearchInferenceReport{}, false, err
	} else if pending > 0 {
		return ResearchInferenceReport{}, false,
			fmt.Errorf("inference window incomplete: %d Step-7 projections pending", pending)
	}
	projectionManifest, err := s.Step7ProjectionCompletionManifest(ctx, completedCutoff)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	contracts, err := s.r139InferenceContracts(ctx)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	terminal, excluded, rowsHash, observed, maxObs, maxUpdate, err := s.r139LoadInferenceSnapshot(
		ctx, completedCutoff, asOf, r139InferencePageRows)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	currentTerminal := terminal[:0]
	activeVersion := make(map[string]int, len(contracts))
	for _, contract := range contracts {
		activeVersion[contract.SystemID] = contract.Version
	}
	for _, row := range terminal {
		if row.ExperimentVersion == activeVersion[row.SystemID] {
			currentTerminal = append(currentTerminal, row)
		}
	}
	terminal = currentTerminal
	systemsHash, err := r139Hash(contracts)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	manifest := r139InputManifest{PipelineVersion: R139InferencePipelineVersion,
		AsOfCompletedTS:   completedCutoff.Format(time.RFC3339Nano),
		SelectionContract: "rolling terminal-only monitor v4: immutable max observation/update IDs and completed Step-7 projection count/high-water/hash; no pending Step-7 projection before cutoff; complete keyset pages with no row truncation; only each system's latest immutable experiment version; latest payoff update as-of run within frozen update ID; exact realized net and collapsed settled-or-void payout; exact frozen instrument/event/payoff version; complete source-clock/book/tick/depth/fee/latency route truth; open/censored/inexact excluded; no preregistered untouched freeze",
		ObservedRows:      observed, MaxObservationID: maxObs, MaxPayoffUpdateID: maxUpdate,
		ProjectionCompletedCount: projectionManifest.Count,
		ProjectionMaxCompletedTS: projectionManifest.MaxCompletedTS,
		ProjectionManifestHash:   projectionManifest.Hash,
		RowsHash:                 rowsHash, RowLimit: 0, PageRows: r139InferencePageRows, SystemsHash: systemsHash}
	results, err := r139InferSystems(terminal, completedCutoff, contracts)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	return s.storeR139Inference(ctx, completedCutoff, manifest, "complete", observed, excluded,
		len(terminal), results)
}

func (s *Store) ResearchInferenceRun(ctx context.Context, runID int64) (ResearchInferenceReport, error) {
	var out ResearchInferenceReport
	var funded, paper, live int
	err := s.db.QueryRowContext(ctx, `SELECT id,created_ts,as_of_completed_ts,pipeline_version,input_manifest_hash,
result_hash,COALESCE(preregistration_id,''),status,observed_rows,exact_terminal_rows,excluded_open,excluded_void,excluded_censored,
excluded_nonexact,excluded_identity,excluded_route_truth,systems_reported,funded,paper_authority,live_authority
FROM research_inference_runs WHERE id=?`, runID).Scan(&out.RunID, &out.CreatedAt, &out.AsOfCompleted,
		&out.PipelineVersion, &out.InputManifestHash, &out.ResultHash, &out.PreregistrationID, &out.Status, &out.ObservedRows,
		&out.ExactTerminalRows, &out.Exclusions.Open, &out.Exclusions.Void, &out.Exclusions.Censored,
		&out.Exclusions.NonExact, &out.Exclusions.Identity, &out.Exclusions.RouteTruth,
		&out.SystemsReported, &funded, &paper, &live)
	if err != nil {
		return out, err
	}
	out.Funded, out.PaperAuthority, out.LiveAuthority = funded != 0, paper != 0, live != 0
	rows, err := s.db.QueryContext(ctx, `SELECT system_id,experiment_version,state,reason,terminal_rows,train_events,
validation_events,monitoring_events,purged_events,validation_days,validation_mean,validation_lower,
validation_upper,validation_bounds_known,monitoring_days,monitoring_mean,monitoring_lower,monitoring_upper,monitoring_bounds_known,
raw_p,holm_p,by_q,monitoring_lower_bound_positive,preregistered_untouched_gate_pass,cells_json,
untouched_events,untouched_days,untouched_mean,untouched_lower,untouched_upper,untouched_p,untouched_bounds_known,
execution_candidate,funded,paper_authority,live_authority
FROM research_inference_system_results WHERE run_id=? ORDER BY system_id`, runID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var r ResearchInferenceSystemResult
		var valKnown, monitoringKnown, monitoringPositive, preregisteredGate, untouchedKnown, candidate, rf, rp, rl int
		var cells string
		if err := rows.Scan(&r.SystemID, &r.ExperimentVersion, &r.State, &r.Reason, &r.TerminalRows, &r.TrainEvents,
			&r.ValidationEvents, &r.MonitoringEvents, &r.PurgedEvents, &r.ValidationDays,
			&r.ValidationMean, &r.ValidationLower, &r.ValidationUpper, &valKnown, &r.MonitoringDays,
			&r.MonitoringMean, &r.MonitoringLower, &r.MonitoringUpper, &monitoringKnown, &r.RawP, &r.HolmP, &r.BYQ,
			&monitoringPositive, &preregisteredGate, &cells, &r.UntouchedEvents, &r.UntouchedDays,
			&r.UntouchedMean, &r.UntouchedLower, &r.UntouchedUpper, &r.UntouchedP, &untouchedKnown,
			&candidate, &rf, &rp, &rl); err != nil {
			return out, err
		}
		r.ValidationBoundsKnown, r.MonitoringBoundsKnown = valKnown != 0, monitoringKnown != 0
		r.MonitoringLowerBoundPositive, r.PreregisteredUntouchedGatePass = monitoringPositive != 0, preregisteredGate != 0
		r.UntouchedBoundsKnown = untouchedKnown != 0
		r.ExecutionCandidate, r.Funded, r.PaperAuthority, r.LiveAuthority = candidate != 0, rf != 0, rp != 0, rl != 0
		if err := json.Unmarshal([]byte(cells), &r.Cells); err != nil {
			return out, err
		}
		out.Results = append(out.Results, r)
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if len(out.Results) != 19 || out.SystemsReported != 19 {
		return out, fmt.Errorf("inference receipt %d has %d/19 system rows", runID, len(out.Results))
	}
	out.MoneyTruth = "terminal-only inference admits exact realized-net settled rows and exact scalar void/refund rows with canonical event+payoff+instrument versions and complete route clocks/books/ticks/depth/fees/latency; open, censored, inexact settlement, identity, and incomplete-route rows are counted but excluded"
	if out.Status == "sealed_preregistered_untouched" && out.PreregistrationID != "" {
		out.InferenceTruth = "one-time sealed preregistered untouched UTC-day evaluation; frozen code/data/source/inference manifests, event disjointness, embargo, stopping time, and complete money truth are enforced"
		out.TerminalOnlyMonitoring, out.PreregisteredFreezePresent = false, true
	} else {
		out.InferenceTruth = "50/25/25 chronological canonical-event rolling monitor with one-day purge/embargo; quiet completed UTC days; deterministic day-block bounds and sign test; hierarchical system/venue/route shrinkage; all-19 Holm and BY. The newest slice is repeatedly monitored, not untouched. A true untouched pass requires a freeze manifest created before test rows and one one-time sealed evaluation; neither exists, so the gate and all execution authority are hard zero"
		out.TerminalOnlyMonitoring, out.PreregisteredFreezePresent = true, false
	}
	return out, nil
}

func (s *Store) LatestResearchInference(ctx context.Context) (ResearchInferenceReport, error) {
	var id int64
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM research_inference_runs ORDER BY id DESC LIMIT 1`).Scan(&id); err != nil {
		return ResearchInferenceReport{}, err
	}
	return s.ResearchInferenceRun(ctx, id)
}

// R139InferenceAuthorityCounts is a narrow invariant probe used by server readiness and tests.
func (s *Store) R139InferenceAuthorityCounts(ctx context.Context) (candidate, funded, paper, live int, err error) {
	err = s.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(execution_candidate),0),COALESCE(SUM(funded),0),
COALESCE(SUM(paper_authority),0),COALESCE(SUM(live_authority),0) FROM research_inference_system_results`).
		Scan(&candidate, &funded, &paper, &live)
	return
}
