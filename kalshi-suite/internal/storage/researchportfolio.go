package storage

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// FrozenEVIInput is an immutable, explicitly sourced research assumption.  It is not inferred
// from a collector count, a leaderboard mean, or a market price.  A semantic change must use a
// larger Version; a later retired version removes the task from the active manifest without
// rewriting history.
type FrozenEVIInput struct {
	TaskID, InputState, SystemID, RouteID    string
	CauseIDs                                 []string
	Provenance, EvidenceHash                 string
	SealedInferenceRunID                     int64
	EvidenceObserved, ValidUntil             time.Time
	ProbabilityChangesDecision               float64
	DecisionValueDollarsPerDay               float64
	CollectionCostDollars                    float64
	ComputeMinutes, APICalls                 float64
	ComputeBudget, APIBudget                 float64
	ComputeDollarPerMinute, APIDollarPerCall float64
	Version                                  int
	SpecHash                                 string
	Funded, PaperAuthority, LiveAuthority    bool
}

// FrozenCauseExposure is a versioned untouched-replication lower bound.  Point estimates and
// validation-only rows cannot be represented as active cause-graph inputs.
type FrozenCauseExposure struct {
	ExposureID, InputState, SystemID, RouteID, Venue string
	CauseIDs                                         []string
	Provenance, EvidenceHash                         string
	SealedInferenceRunID                             int64
	SealedRouteEconomicsReceiptID                    string
	EvidenceObserved, ValidUntil                     time.Time
	ReplicationID, LowerBoundMethod                  string
	ReplicatedUntouched, ExecutableFeeNetLowerBound  bool
	NetPerDayLower, Capacity                         float64
	CapitalDollarHoursPerDay                         float64
	Version                                          int
	SpecHash                                         string
	Funded, PaperAuthority, LiveAuthority            bool
}

type frozenEVIHash struct {
	TaskID, InputState, SystemID, RouteID    string
	CauseIDs                                 []string
	Provenance, EvidenceHash                 string
	SealedInferenceRunID                     int64
	EvidenceObserved, ValidUntil             string
	ProbabilityChangesDecision               float64
	DecisionValueDollarsPerDay               float64
	CollectionCostDollars                    float64
	ComputeMinutes, APICalls                 float64
	ComputeBudget, APIBudget                 float64
	ComputeDollarPerMinute, APIDollarPerCall float64
	Version                                  int
}

type frozenCauseHash struct {
	ExposureID, InputState, SystemID, RouteID, Venue string
	CauseIDs                                         []string
	Provenance, EvidenceHash                         string
	SealedInferenceRunID                             int64
	SealedRouteEconomicsReceiptID                    string
	EvidenceObserved, ValidUntil                     string
	ReplicationID, LowerBoundMethod                  string
	ReplicatedUntouched, ExecutableFeeNetLowerBound  bool
	NetPerDayLower, Capacity                         float64
	CapitalDollarHoursPerDay                         float64
	Version                                          int
}

func finiteResearchNumber(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validSHA256(v string) bool {
	v = strings.TrimSpace(v)
	b, err := hex.DecodeString(v)
	return err == nil && len(b) == 32
}

func normalizeFrozenState(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "active":
		return "active"
	case "retired":
		return "retired"
	default:
		return ""
	}
}

func normalizeFrozenEVI(v FrozenEVIInput) (FrozenEVIInput, frozenEVIHash, error) {
	v.TaskID, v.SystemID, v.RouteID = strings.TrimSpace(v.TaskID), strings.TrimSpace(v.SystemID), strings.TrimSpace(v.RouteID)
	v.Provenance, v.EvidenceHash = strings.TrimSpace(v.Provenance), strings.ToLower(strings.TrimSpace(v.EvidenceHash))
	v.InputState, v.CauseIDs = normalizeFrozenState(v.InputState), cleanStringSet(v.CauseIDs)
	if v.Version <= 0 || v.TaskID == "" || v.SystemID == "" || v.RouteID == "" || len(v.CauseIDs) == 0 ||
		v.Provenance == "" || !validSHA256(v.EvidenceHash) || v.SealedInferenceRunID <= 0 || v.InputState == "" || v.EvidenceObserved.IsZero() ||
		v.ValidUntil.IsZero() || !v.ValidUntil.After(v.EvidenceObserved) || v.Funded || v.PaperAuthority || v.LiveAuthority {
		return FrozenEVIInput{}, frozenEVIHash{}, errors.New("invalid frozen EVI identity, provenance, freshness, or authority")
	}
	nums := []float64{v.ProbabilityChangesDecision, v.DecisionValueDollarsPerDay, v.CollectionCostDollars,
		v.ComputeMinutes, v.APICalls, v.ComputeBudget, v.APIBudget, v.ComputeDollarPerMinute, v.APIDollarPerCall}
	for _, n := range nums {
		if !finiteResearchNumber(n) || n < 0 {
			return FrozenEVIInput{}, frozenEVIHash{}, errors.New("invalid frozen EVI numeric input")
		}
	}
	if v.ProbabilityChangesDecision > 1 {
		return FrozenEVIInput{}, frozenEVIHash{}, errors.New("decision-change probability must be in [0,1]")
	}
	v.EvidenceObserved, v.ValidUntil = v.EvidenceObserved.UTC(), v.ValidUntil.UTC()
	h := frozenEVIHash{TaskID: v.TaskID, InputState: v.InputState, SystemID: v.SystemID, RouteID: v.RouteID,
		CauseIDs: v.CauseIDs, Provenance: v.Provenance, EvidenceHash: v.EvidenceHash, SealedInferenceRunID: v.SealedInferenceRunID,
		EvidenceObserved: v.EvidenceObserved.Format(time.RFC3339Nano), ValidUntil: v.ValidUntil.Format(time.RFC3339Nano),
		ProbabilityChangesDecision: v.ProbabilityChangesDecision, DecisionValueDollarsPerDay: v.DecisionValueDollarsPerDay,
		CollectionCostDollars: v.CollectionCostDollars, ComputeMinutes: v.ComputeMinutes, APICalls: v.APICalls,
		ComputeBudget: v.ComputeBudget, APIBudget: v.APIBudget, ComputeDollarPerMinute: v.ComputeDollarPerMinute,
		APIDollarPerCall: v.APIDollarPerCall, Version: v.Version}
	return v, h, nil
}

func normalizeFrozenCause(v FrozenCauseExposure) (FrozenCauseExposure, frozenCauseHash, error) {
	v.ExposureID, v.SystemID, v.RouteID, v.Venue = strings.TrimSpace(v.ExposureID), strings.TrimSpace(v.SystemID), strings.TrimSpace(v.RouteID), strings.ToLower(strings.TrimSpace(v.Venue))
	v.Provenance, v.EvidenceHash = strings.TrimSpace(v.Provenance), strings.ToLower(strings.TrimSpace(v.EvidenceHash))
	v.ReplicationID, v.LowerBoundMethod = strings.TrimSpace(v.ReplicationID), strings.TrimSpace(v.LowerBoundMethod)
	v.SealedRouteEconomicsReceiptID = strings.TrimSpace(v.SealedRouteEconomicsReceiptID)
	v.InputState, v.CauseIDs = normalizeFrozenState(v.InputState), cleanStringSet(v.CauseIDs)
	if v.Version <= 0 || v.ExposureID == "" || v.SystemID == "" || v.RouteID == "" || v.Venue == "" ||
		len(v.CauseIDs) == 0 || v.Provenance == "" || !validSHA256(v.EvidenceHash) || v.SealedInferenceRunID <= 0 || v.SealedRouteEconomicsReceiptID == "" || v.InputState == "" ||
		v.EvidenceObserved.IsZero() || v.ValidUntil.IsZero() || !v.ValidUntil.After(v.EvidenceObserved) ||
		v.ReplicationID == "" || v.LowerBoundMethod == "" || !v.ReplicatedUntouched || !v.ExecutableFeeNetLowerBound ||
		v.Funded || v.PaperAuthority || v.LiveAuthority {
		return FrozenCauseExposure{}, frozenCauseHash{}, errors.New("invalid replicated cause exposure or authority")
	}
	for _, n := range []float64{v.NetPerDayLower, v.Capacity, v.CapitalDollarHoursPerDay} {
		if !finiteResearchNumber(n) {
			return FrozenCauseExposure{}, frozenCauseHash{}, errors.New("non-finite cause exposure")
		}
	}
	if v.Capacity < 0 || v.CapitalDollarHoursPerDay < 0 {
		return FrozenCauseExposure{}, frozenCauseHash{}, errors.New("negative capacity or capital time")
	}
	v.EvidenceObserved, v.ValidUntil = v.EvidenceObserved.UTC(), v.ValidUntil.UTC()
	h := frozenCauseHash{ExposureID: v.ExposureID, InputState: v.InputState, SystemID: v.SystemID, RouteID: v.RouteID,
		Venue: v.Venue, CauseIDs: v.CauseIDs, Provenance: v.Provenance, EvidenceHash: v.EvidenceHash,
		SealedInferenceRunID: v.SealedInferenceRunID, SealedRouteEconomicsReceiptID: v.SealedRouteEconomicsReceiptID,
		EvidenceObserved: v.EvidenceObserved.Format(time.RFC3339Nano), ValidUntil: v.ValidUntil.Format(time.RFC3339Nano),
		ReplicationID: v.ReplicationID, LowerBoundMethod: v.LowerBoundMethod,
		ReplicatedUntouched: v.ReplicatedUntouched, ExecutableFeeNetLowerBound: v.ExecutableFeeNetLowerBound,
		NetPerDayLower: v.NetPerDayLower, Capacity: v.Capacity, CapitalDollarHoursPerDay: v.CapitalDollarHoursPerDay,
		Version: v.Version}
	return v, h, nil
}

func (s *Store) validateSealedInferenceLink(ctx context.Context, runID int64, systemID, evidenceHash string) error {
	var resultHash, status string
	var gate, candidate, rf, rp, rl, sf, sp, sl int
	err := s.db.QueryRowContext(ctx, `SELECT r.result_hash,r.status,
s.preregistered_untouched_gate_pass,s.execution_candidate,r.funded,r.paper_authority,r.live_authority,
s.funded,s.paper_authority,s.live_authority
FROM research_inference_runs r JOIN research_inference_system_results s ON s.run_id=r.id
WHERE r.id=? AND s.system_id=?`, runID, strings.TrimSpace(systemID)).Scan(&resultHash, &status,
		&gate, &candidate, &rf, &rp, &rl, &sf, &sp, &sl)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("no immutable inference result exists for the referenced run and system")
	}
	if err != nil {
		return err
	}
	resultHash = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(resultHash)), "sha256:")
	if resultHash != strings.ToLower(strings.TrimSpace(evidenceHash)) {
		return errors.New("evidence hash does not match the referenced immutable inference result")
	}
	if status != "sealed_preregistered_untouched" || gate != 1 || candidate != 1 {
		return errors.New("referenced inference result is rolling monitoring, not a sealed preregistered untouched gate pass")
	}
	if rf != 0 || rp != 0 || rl != 0 || sf != 0 || sp != 0 || sl != 0 {
		return errors.New("referenced inference result violates zero-authority contract")
	}
	return nil
}

func (s *Store) validateSealedRouteEconomicsLink(ctx context.Context, in FrozenCauseExposure) error {
	var runID int64
	var resultHash, systemID, routeID, venue, conversionHash string
	var netLower, capacity, capitalHours float64
	var funded, paper, live int
	err := s.db.QueryRowContext(ctx, `SELECT sealed_inference_run_id,sealed_result_hash,system_id,
route_id,venue,net_per_day_lower,capacity,capital_dollar_hours_per_day,conversion_evidence_hash,
funded,paper_authority,live_authority FROM research_sealed_route_economics WHERE receipt_id=?`,
		in.SealedRouteEconomicsReceiptID).Scan(&runID, &resultHash, &systemID, &routeID, &venue,
		&netLower, &capacity, &capitalHours, &conversionHash, &funded, &paper, &live)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("no immutable sealed route economics receipt exists for Net/day, capacity, and capital time")
	}
	if err != nil {
		return err
	}
	if runID != in.SealedInferenceRunID || strings.TrimPrefix(strings.ToLower(resultHash), "sha256:") != in.EvidenceHash ||
		systemID != in.SystemID || routeID != in.RouteID || strings.ToLower(venue) != in.Venue ||
		math.Abs(netLower-in.NetPerDayLower) > 1e-12 || math.Abs(capacity-in.Capacity) > 1e-12 ||
		math.Abs(capitalHours-in.CapitalDollarHoursPerDay) > 1e-12 || !validSHA256(conversionHash) ||
		funded != 0 || paper != 0 || live != 0 {
		return errors.New("sealed route economics receipt does not exactly match the cause exposure")
	}
	return nil
}

func (s *Store) RegisterFrozenEVIInput(ctx context.Context, in FrozenEVIInput) (FrozenEVIInput, bool, error) {
	in, semantic, err := normalizeFrozenEVI(in)
	if err != nil {
		return FrozenEVIInput{}, false, err
	}
	if err := s.validateSealedInferenceLink(ctx, in.SealedInferenceRunID, in.SystemID, in.EvidenceHash); err != nil {
		return FrozenEVIInput{}, false, err
	}
	in.SpecHash, err = researchSpecHash(semantic)
	if err != nil {
		return FrozenEVIInput{}, false, err
	}
	var existing string
	err = s.db.QueryRowContext(ctx, `SELECT spec_hash FROM research_evi_input_specs WHERE task_id=? AND version=?`, in.TaskID, in.Version).Scan(&existing)
	if err == nil {
		if existing != in.SpecHash {
			return FrozenEVIInput{}, false, fmt.Errorf("immutable EVI input drift: %s v%d (bump version)", in.TaskID, in.Version)
		}
		return in, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return FrozenEVIInput{}, false, err
	}
	causes, _ := json.Marshal(in.CauseIDs)
	_, err = s.db.ExecContext(ctx, `INSERT INTO research_evi_input_specs(
task_id,version,spec_hash,input_state,system_id,route_id,cause_ids_json,provenance,evidence_hash,
sealed_inference_run_id,evidence_observed_ts,valid_until_ts,probability_changes_decision,decision_value_dollars_per_day,
collection_cost_dollars,compute_minutes,api_calls,compute_budget,api_budget,compute_dollar_per_minute,
api_dollar_per_call,created_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, in.TaskID, in.Version, in.SpecHash, in.InputState,
		in.SystemID, in.RouteID, string(causes), in.Provenance, in.EvidenceHash,
		in.SealedInferenceRunID, in.EvidenceObserved.Format(time.RFC3339Nano), in.ValidUntil.Format(time.RFC3339Nano),
		in.ProbabilityChangesDecision, in.DecisionValueDollarsPerDay, in.CollectionCostDollars,
		in.ComputeMinutes, in.APICalls, in.ComputeBudget, in.APIBudget, in.ComputeDollarPerMinute,
		in.APIDollarPerCall, nowRFC())
	return in, err == nil, err
}

func (s *Store) RegisterFrozenCauseExposure(ctx context.Context, in FrozenCauseExposure) (FrozenCauseExposure, bool, error) {
	in, semantic, err := normalizeFrozenCause(in)
	if err != nil {
		return FrozenCauseExposure{}, false, err
	}
	if err := s.validateSealedInferenceLink(ctx, in.SealedInferenceRunID, in.SystemID, in.EvidenceHash); err != nil {
		return FrozenCauseExposure{}, false, err
	}
	if err := s.validateSealedRouteEconomicsLink(ctx, in); err != nil {
		return FrozenCauseExposure{}, false, err
	}
	in.SpecHash, err = researchSpecHash(semantic)
	if err != nil {
		return FrozenCauseExposure{}, false, err
	}
	var existing string
	err = s.db.QueryRowContext(ctx, `SELECT spec_hash FROM research_cause_exposure_specs WHERE exposure_id=? AND version=?`, in.ExposureID, in.Version).Scan(&existing)
	if err == nil {
		if existing != in.SpecHash {
			return FrozenCauseExposure{}, false, fmt.Errorf("immutable cause exposure drift: %s v%d (bump version)", in.ExposureID, in.Version)
		}
		return in, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return FrozenCauseExposure{}, false, err
	}
	causes, _ := json.Marshal(in.CauseIDs)
	_, err = s.db.ExecContext(ctx, `INSERT INTO research_cause_exposure_specs(
exposure_id,version,spec_hash,input_state,system_id,route_id,venue,cause_ids_json,provenance,evidence_hash,
sealed_inference_run_id,sealed_route_economics_receipt_id,evidence_observed_ts,valid_until_ts,replication_id,lower_bound_method,replicated_untouched,
executable_fee_net_lower_bound,net_per_day_lower,capacity,capital_dollar_hours_per_day,created_ts,
funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		in.ExposureID, in.Version, in.SpecHash, in.InputState, in.SystemID, in.RouteID, in.Venue,
		string(causes), in.Provenance, in.EvidenceHash, in.SealedInferenceRunID, in.SealedRouteEconomicsReceiptID, in.EvidenceObserved.Format(time.RFC3339Nano),
		in.ValidUntil.Format(time.RFC3339Nano), in.ReplicationID, in.LowerBoundMethod,
		boolInt(in.ReplicatedUntouched), boolInt(in.ExecutableFeeNetLowerBound), in.NetPerDayLower,
		in.Capacity, in.CapitalDollarHoursPerDay, nowRFC())
	return in, err == nil, err
}

func parsePortfolioTime(v string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, v)
	return t.UTC()
}

func (s *Store) CurrentFrozenEVIInputs(ctx context.Context) ([]FrozenEVIInput, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT task_id,version,spec_hash,input_state,system_id,route_id,
cause_ids_json,provenance,evidence_hash,sealed_inference_run_id,evidence_observed_ts,valid_until_ts,probability_changes_decision,
decision_value_dollars_per_day,collection_cost_dollars,compute_minutes,api_calls,compute_budget,api_budget,
compute_dollar_per_minute,api_dollar_per_call,funded,paper_authority,live_authority
FROM research_evi_input_specs e WHERE version=(SELECT MAX(x.version) FROM research_evi_input_specs x WHERE x.task_id=e.task_id)
ORDER BY task_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FrozenEVIInput
	for rows.Next() {
		var v FrozenEVIInput
		var causes, observed, until string
		var funded, paper, live int
		if err := rows.Scan(&v.TaskID, &v.Version, &v.SpecHash, &v.InputState, &v.SystemID, &v.RouteID,
			&causes, &v.Provenance, &v.EvidenceHash, &v.SealedInferenceRunID, &observed, &until, &v.ProbabilityChangesDecision,
			&v.DecisionValueDollarsPerDay, &v.CollectionCostDollars, &v.ComputeMinutes, &v.APICalls,
			&v.ComputeBudget, &v.APIBudget, &v.ComputeDollarPerMinute, &v.APIDollarPerCall,
			&funded, &paper, &live); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(causes), &v.CauseIDs)
		v.EvidenceObserved, v.ValidUntil = parsePortfolioTime(observed), parsePortfolioTime(until)
		v.Funded, v.PaperAuthority, v.LiveAuthority = funded != 0, paper != 0, live != 0
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) CurrentFrozenCauseExposures(ctx context.Context) ([]FrozenCauseExposure, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT exposure_id,version,spec_hash,input_state,system_id,route_id,
venue,cause_ids_json,provenance,evidence_hash,sealed_inference_run_id,sealed_route_economics_receipt_id,evidence_observed_ts,valid_until_ts,replication_id,
lower_bound_method,replicated_untouched,executable_fee_net_lower_bound,net_per_day_lower,capacity,
capital_dollar_hours_per_day,funded,paper_authority,live_authority
FROM research_cause_exposure_specs e WHERE version=(SELECT MAX(x.version) FROM research_cause_exposure_specs x WHERE x.exposure_id=e.exposure_id)
ORDER BY exposure_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FrozenCauseExposure
	for rows.Next() {
		var v FrozenCauseExposure
		var causes, observed, until string
		var replicated, lowerBound, funded, paper, live int
		if err := rows.Scan(&v.ExposureID, &v.Version, &v.SpecHash, &v.InputState, &v.SystemID, &v.RouteID,
			&v.Venue, &causes, &v.Provenance, &v.EvidenceHash, &v.SealedInferenceRunID, &v.SealedRouteEconomicsReceiptID, &observed, &until, &v.ReplicationID,
			&v.LowerBoundMethod, &replicated, &lowerBound, &v.NetPerDayLower, &v.Capacity,
			&v.CapitalDollarHoursPerDay, &funded, &paper, &live); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(causes), &v.CauseIDs)
		v.EvidenceObserved, v.ValidUntil = parsePortfolioTime(observed), parsePortfolioTime(until)
		v.ReplicatedUntouched, v.ExecutableFeeNetLowerBound = replicated != 0, lowerBound != 0
		v.Funded, v.PaperAuthority, v.LiveAuthority = funded != 0, paper != 0, live != 0
		out = append(out, v)
	}
	return out, rows.Err()
}

type ResearchPortfolioRun struct {
	ManifestHash                          string
	Observed                              time.Time
	State                                 string
	InputHashes                           []string
	InputCount, ValidInputCount           int
	ResultJSON, Reason                    string
	Funded, PaperAuthority, LiveAuthority bool
}

// ResearchPortfolioManifestHash hashes sorted immutable spec hashes plus explicit eligibility
// markers.  Collector row counts and current prices are intentionally absent.
func ResearchPortfolioManifestHash(kind string, entries []string) (string, error) {
	entries = cleanStringSet(entries)
	return researchSpecHash(struct {
		Kind    string
		Entries []string
	}{strings.TrimSpace(kind), entries})
}

func validatePortfolioRun(r ResearchPortfolioRun, allowed map[string]bool) (ResearchPortfolioRun, error) {
	r.ManifestHash, r.State, r.Reason = strings.ToLower(strings.TrimSpace(r.ManifestHash)), strings.TrimSpace(r.State), strings.TrimSpace(r.Reason)
	r.InputHashes = cleanStringSet(r.InputHashes)
	if !validSHA256(r.ManifestHash) || !allowed[r.State] || r.InputCount < 0 || r.ValidInputCount < 0 ||
		r.ValidInputCount > r.InputCount || r.Reason == "" || !json.Valid([]byte(r.ResultJSON)) ||
		r.Funded || r.PaperAuthority || r.LiveAuthority {
		return ResearchPortfolioRun{}, errors.New("invalid immutable research portfolio run")
	}
	if r.Observed.IsZero() {
		r.Observed = time.Now().UTC()
	}
	return r, nil
}

func (s *Store) InsertEVIScheduleRun(ctx context.Context, r ResearchPortfolioRun) (ResearchPortfolioRun, bool, error) {
	var err error
	r, err = validatePortfolioRun(r, map[string]bool{"WAITING_FOR_FROZEN_INPUTS": true, "BLOCKED_STALE_OR_INCONSISTENT_INPUTS": true, "SCHEDULED": true})
	if err != nil {
		return ResearchPortfolioRun{}, false, err
	}
	hashes, _ := json.Marshal(r.InputHashes)
	var priorState, priorHashes, priorResult, priorReason string
	var priorInput, priorValid int
	err = s.db.QueryRowContext(ctx, `SELECT state,input_hashes_json,input_count,valid_input_count,result_json,reason
FROM research_evi_schedule_runs WHERE manifest_hash=?`, r.ManifestHash).Scan(&priorState, &priorHashes,
		&priorInput, &priorValid, &priorResult, &priorReason)
	if err == nil {
		if priorState != r.State || priorHashes != string(hashes) || priorInput != r.InputCount ||
			priorValid != r.ValidInputCount || priorResult != r.ResultJSON || priorReason != r.Reason {
			return ResearchPortfolioRun{}, false, errors.New("deterministic EVI receipt drift for existing input manifest")
		}
		return r, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ResearchPortfolioRun{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_evi_schedule_runs(
manifest_hash,observed_ts,state,input_hashes_json,input_count,valid_input_count,result_json,reason,
funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,0,0,0)`, r.ManifestHash,
		r.Observed.UTC().Format(time.RFC3339Nano), r.State, string(hashes), r.InputCount,
		r.ValidInputCount, r.ResultJSON, r.Reason)
	if err != nil {
		return ResearchPortfolioRun{}, false, err
	}
	n, err := res.RowsAffected()
	if err == nil && n == 0 {
		return s.InsertEVIScheduleRun(ctx, r)
	}
	return r, n > 0, err
}

func (s *Store) InsertCauseGraphRun(ctx context.Context, r ResearchPortfolioRun) (ResearchPortfolioRun, bool, error) {
	var err error
	r, err = validatePortfolioRun(r, map[string]bool{"BLOCKED_NO_REPLICATED_ROUTE_LOWER_BOUNDS": true, "BLOCKED_STALE_REPLICATED_ROUTE_LOWER_BOUNDS": true, "READY": true})
	if err != nil {
		return ResearchPortfolioRun{}, false, err
	}
	hashes, _ := json.Marshal(r.InputHashes)
	var priorState, priorHashes, priorResult, priorReason string
	var priorInput, priorValid int
	err = s.db.QueryRowContext(ctx, `SELECT state,input_hashes_json,input_count,valid_input_count,result_json,reason
FROM research_cause_graph_runs WHERE manifest_hash=?`, r.ManifestHash).Scan(&priorState, &priorHashes,
		&priorInput, &priorValid, &priorResult, &priorReason)
	if err == nil {
		if priorState != r.State || priorHashes != string(hashes) || priorInput != r.InputCount ||
			priorValid != r.ValidInputCount || priorResult != r.ResultJSON || priorReason != r.Reason {
			return ResearchPortfolioRun{}, false, errors.New("deterministic cause-graph receipt drift for existing input manifest")
		}
		return r, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ResearchPortfolioRun{}, false, err
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_cause_graph_runs(
manifest_hash,observed_ts,state,input_hashes_json,input_count,valid_input_count,result_json,reason,
funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,0,0,0)`, r.ManifestHash,
		r.Observed.UTC().Format(time.RFC3339Nano), r.State, string(hashes), r.InputCount,
		r.ValidInputCount, r.ResultJSON, r.Reason)
	if err != nil {
		return ResearchPortfolioRun{}, false, err
	}
	n, err := res.RowsAffected()
	if err == nil && n == 0 {
		return s.InsertCauseGraphRun(ctx, r)
	}
	return r, n > 0, err
}

func (s *Store) latestPortfolioRun(ctx context.Context, table string) (ResearchPortfolioRun, bool, error) {
	if table != "research_evi_schedule_runs" && table != "research_cause_graph_runs" {
		return ResearchPortfolioRun{}, false, errors.New("invalid portfolio receipt table")
	}
	var r ResearchPortfolioRun
	var observed, hashes string
	var funded, paper, live int
	err := s.db.QueryRowContext(ctx, `SELECT manifest_hash,observed_ts,state,input_hashes_json,input_count,
valid_input_count,result_json,reason,funded,paper_authority,live_authority FROM `+table+` ORDER BY observed_ts DESC,manifest_hash DESC LIMIT 1`).Scan(
		&r.ManifestHash, &observed, &r.State, &hashes, &r.InputCount, &r.ValidInputCount,
		&r.ResultJSON, &r.Reason, &funded, &paper, &live)
	if errors.Is(err, sql.ErrNoRows) {
		return ResearchPortfolioRun{}, false, nil
	}
	if err != nil {
		return ResearchPortfolioRun{}, false, err
	}
	_ = json.Unmarshal([]byte(hashes), &r.InputHashes)
	r.Observed = parsePortfolioTime(observed)
	r.Funded, r.PaperAuthority, r.LiveAuthority = funded != 0, paper != 0, live != 0
	return r, true, nil
}

func (s *Store) LatestEVIScheduleRun(ctx context.Context) (ResearchPortfolioRun, bool, error) {
	return s.latestPortfolioRun(ctx, "research_evi_schedule_runs")
}

func (s *Store) LatestCauseGraphRun(ctx context.Context) (ResearchPortfolioRun, bool, error) {
	return s.latestPortfolioRun(ctx, "research_cause_graph_runs")
}

// Active hash helpers are used by the deterministic loop.  A stale marker is part of the
// manifest so an expired input can never reuse a prior READY/SCHEDULED receipt.
func FrozenEVIManifestEntries(rows []FrozenEVIInput, now time.Time) (entries []string, active, valid int) {
	for _, row := range rows {
		if row.InputState != "active" {
			continue
		}
		active++
		marker := "active:"
		if row.ValidUntil.After(now) && !row.EvidenceObserved.After(now.Add(5*time.Minute)) {
			valid++
			marker = "valid:"
		} else {
			marker = "stale:"
		}
		entries = append(entries, marker+row.SpecHash)
	}
	sort.Strings(entries)
	return entries, active, valid
}

func FrozenCauseManifestEntries(rows []FrozenCauseExposure, now time.Time) (entries []string, active, valid int) {
	for _, row := range rows {
		if row.InputState != "active" {
			continue
		}
		active++
		marker := "stale:"
		if row.ValidUntil.After(now) && !row.EvidenceObserved.After(now.Add(5*time.Minute)) &&
			row.ReplicatedUntouched && row.ExecutableFeeNetLowerBound {
			valid++
			marker = "valid:"
		}
		entries = append(entries, marker+row.SpecHash)
	}
	sort.Strings(entries)
	return entries, active, valid
}
