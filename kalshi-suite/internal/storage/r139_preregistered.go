package storage

// This file implements the one path that may turn rolling research into a true untouched
// replication receipt. A preregistration freezes identity, code/data/source manifests, selection,
// embargo, future UTC-day window, stopping time, and inference contract before any test row exists.
// The later seal is one-shot and append-only; it still grants no Paper or LIVE authority.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchstats"
)

type ResearchPayoffUpdateManifest struct {
	ObservationCount int    `json:"observation_count"`
	UpdateCount      int    `json:"update_count"`
	MaxUpdateID      int64  `json:"max_update_id"`
	Hash             string `json:"hash"`
}

type researchPayoffManifestRow struct {
	ObservationID int64    `json:"observation_id"`
	UpdateID      int64    `json:"update_id"`
	ObservedTS    string   `json:"observed_ts"`
	Status        string   `json:"status"`
	PayoutLower   float64  `json:"payout_lower"`
	PayoutUpper   float64  `json:"payout_upper"`
	RealizedNet   *float64 `json:"realized_net"`
}

func researchPayoffUpdateManifestFromRows(rows []r139RawInferenceRow) (ResearchPayoffUpdateManifest, error) {
	exact := make([]researchPayoffManifestRow, 0, len(rows))
	for _, row := range rows {
		exact = append(exact, researchPayoffManifestRow{ObservationID: row.ObservationID,
			UpdateID: row.PayoffUpdateID, ObservedTS: row.PayoffUpdateTS, Status: row.PayoffStatus,
			PayoutLower: row.PayoutLower, PayoutUpper: row.PayoutUpper, RealizedNet: row.RealizedNet})
	}
	hash, err := r139Hash(exact)
	if err != nil {
		return ResearchPayoffUpdateManifest{}, err
	}
	out := ResearchPayoffUpdateManifest{ObservationCount: len(exact),
		Hash: strings.TrimPrefix(hash, "sha256:")}
	for _, row := range exact {
		if row.UpdateID > 0 {
			out.UpdateCount++
		}
		if row.UpdateID > out.MaxUpdateID {
			out.MaxUpdateID = row.UpdateID
		}
	}
	return out, nil
}

func researchPayoffUpdateManifest(ctx context.Context, q step7ProjectionQuerier,
	p ResearchInferencePreregistration, cutoff time.Time) (ResearchPayoffUpdateManifest, error) {
	latest := `u2.observation_id=o.id`
	args := make([]any, 0, 9)
	if !cutoff.IsZero() {
		latest += ` AND julianday(u2.observed_ts)<=julianday(?)`
		args = append(args, cutoff.UTC().Format(time.RFC3339Nano))
	}
	query := `SELECT o.id,COALESCE(u.id,0),COALESCE(u.observed_ts,''),
COALESCE(u.status,''),COALESCE(u.payout_lower,0),COALESCE(u.payout_upper,0),u.realized_net
FROM research_system_observations o
LEFT JOIN research_system_payoff_updates u ON u.id=(
 SELECT u2.id FROM research_system_payoff_updates u2 WHERE ` + latest + `
 ORDER BY u2.id DESC LIMIT 1)
WHERE o.system_id=? AND o.experiment_version=? AND o.cohort=? AND o.venue=? AND o.route=?
 AND o.observation_kind!='control'
 AND julianday(o.observed_ts)>=julianday(?) AND julianday(o.observed_ts)<julianday(?)
ORDER BY o.id`
	args = append(args, p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route,
		p.UntouchedStart.UTC().Format(time.RFC3339Nano),
		p.UntouchedEnd.UTC().Format(time.RFC3339Nano))
	dbRows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return ResearchPayoffUpdateManifest{}, err
	}
	defer dbRows.Close()
	exact := []researchPayoffManifestRow{}
	for dbRows.Next() {
		var row researchPayoffManifestRow
		var realized sql.NullFloat64
		if err := dbRows.Scan(&row.ObservationID, &row.UpdateID, &row.ObservedTS, &row.Status,
			&row.PayoutLower, &row.PayoutUpper, &realized); err != nil {
			return ResearchPayoffUpdateManifest{}, err
		}
		if realized.Valid {
			value := realized.Float64
			row.RealizedNet = &value
		}
		exact = append(exact, row)
	}
	if err := dbRows.Err(); err != nil {
		return ResearchPayoffUpdateManifest{}, err
	}
	raw := make([]r139RawInferenceRow, 0, len(exact))
	for _, row := range exact {
		raw = append(raw, r139RawInferenceRow{ObservationID: row.ObservationID,
			PayoffUpdateID: row.UpdateID, PayoffUpdateTS: row.ObservedTS, PayoffStatus: row.Status,
			PayoutLower: row.PayoutLower, PayoutUpper: row.PayoutUpper, RealizedNet: row.RealizedNet})
	}
	return researchPayoffUpdateManifestFromRows(raw)
}

func (s *Store) preregisteredEquivalentBundleEvents(ctx context.Context,
	p ResearchInferencePreregistration, start, end time.Time, limit int) (map[string]bool, error) {
	return preregisteredEquivalentBundleEvents(ctx, s.db, p, start, end, limit)
}

func preregisteredEquivalentBundleEvents(ctx context.Context, q step7ProjectionQuerier,
	p ResearchInferencePreregistration, start, end time.Time, limit int) (map[string]bool, error) {
	var maxRowID int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM research_route_bundles`).Scan(&maxRowID); err != nil {
		return nil, err
	}
	queryBase := `SELECT observed_ts,bundle_id FROM research_route_bundles WHERE system_id=? AND experiment_version=?
	 AND cohort=? AND certificate_status='verified' AND rowid<=?`
	baseArgs := []any{p.SystemID, p.ExperimentVersion, p.Cohort, maxRowID}
	if !start.IsZero() {
		queryBase += ` AND julianday(observed_ts)>=julianday(?)`
		baseArgs = append(baseArgs, start.UTC().Format(time.RFC3339Nano))
	}
	if !end.IsZero() {
		queryBase += ` AND julianday(observed_ts)<julianday(?)`
		baseArgs = append(baseArgs, end.UTC().Format(time.RFC3339Nano))
	}
	ids := []string{}
	const pageSize = 5000
	var afterTS, afterID string
	for limit == 0 || len(ids) < limit {
		fetch := pageSize
		if limit > 0 && limit-len(ids) < fetch {
			fetch = limit - len(ids)
		}
		query := queryBase
		args := append([]any{}, baseArgs...)
		if afterTS != "" {
			query += `
 AND (julianday(observed_ts)>julianday(?) OR
  (julianday(observed_ts)=julianday(?) AND bundle_id>?))`
			args = append(args, afterTS, afterTS, afterID)
		}
		query += ` ORDER BY julianday(observed_ts),bundle_id LIMIT ?`
		args = append(args, fetch)
		rows, err := q.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		pageN := 0
		for rows.Next() {
			var ts, id string
			if err := rows.Scan(&ts, &id); err != nil {
				rows.Close()
				return nil, err
			}
			ids, afterTS, afterID, pageN = append(ids, id), ts, id, pageN+1
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if pageN < fetch {
			break
		}
	}
	events := map[string]bool{}
	for _, id := range ids {
		b, found, err := researchRouteBundleByID(ctx, q, id)
		if err != nil {
			return nil, err
		}
		if found && researchBundleKalshiComboEquivalent(b.Size, b.Legs, b.States) {
			events[b.CanonicalEventID] = true
		}
	}
	return events, nil
}

type preregisteredPriorEventManifest struct {
	Count int
	Hash  string
}

type preregisteredRFQGradeManifest struct {
	ID                        int64
	ObservedTS, OutcomeStatus string
	Payout, RealizedNet       *float64
	CapitalSeconds            float64
	Reason, EvidenceJSON      string
}

type preregisteredRFQBundleManifest struct {
	Bundle ResearchRouteBundle
	Grade  *preregisteredRFQGradeManifest
}

func preregisteredRFQUntouchedManifest(ctx context.Context, q step7ProjectionQuerier,
	p ResearchInferencePreregistration) (string, error) {
	if p.Venue != "kalshi" || p.Route != "rfq" {
		return "", errors.New("RFQ untouched manifest requires kalshi/rfq")
	}
	rows, err := q.QueryContext(ctx, `SELECT bundle_id FROM research_route_bundles
WHERE system_id=? AND experiment_version=? AND cohort=? AND certificate_status='verified'
 AND julianday(observed_ts)>=julianday(?) AND julianday(observed_ts)<julianday(?)
ORDER BY rowid`, p.SystemID, p.ExperimentVersion, p.Cohort,
		p.UntouchedStart.Format(time.RFC3339Nano), p.UntouchedEnd.Format(time.RFC3339Nano))
	if err != nil {
		return "", err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return "", err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return "", err
	}
	if err := rows.Close(); err != nil {
		return "", err
	}
	manifest := make([]preregisteredRFQBundleManifest, 0, len(ids))
	for _, id := range ids {
		bundle, found, err := researchRouteBundleByID(ctx, q, id)
		if err != nil {
			return "", err
		}
		if !found || !researchBundleKalshiComboEquivalent(bundle.Size, bundle.Legs, bundle.States) {
			continue
		}
		entry := preregisteredRFQBundleManifest{Bundle: bundle}
		var grade preregisteredRFQGradeManifest
		var payout, realized sql.NullFloat64
		err = q.QueryRowContext(ctx, `SELECT id,observed_ts,outcome_status,payout,realized_net,
capital_seconds,reason,evidence_json FROM research_route_bundle_events
WHERE bundle_id=? AND event_type='grade' AND julianday(observed_ts)<=julianday(?)`,
			id, p.SealAt.Format(time.RFC3339Nano)).Scan(&grade.ID, &grade.ObservedTS,
			&grade.OutcomeStatus, &payout, &realized, &grade.CapitalSeconds, &grade.Reason,
			&grade.EvidenceJSON)
		if err == nil {
			if payout.Valid {
				value := payout.Float64
				grade.Payout = &value
			}
			if realized.Valid {
				value := realized.Float64
				grade.RealizedNet = &value
			}
			entry.Grade = &grade
		} else if !errors.Is(err, sql.ErrNoRows) {
			return "", err
		}
		manifest = append(manifest, entry)
	}
	hash, err := r139Hash(manifest)
	return strings.TrimPrefix(hash, "sha256:"), err
}

func preregisteredPriorEvents(ctx context.Context, q step7ProjectionQuerier,
	p ResearchInferencePreregistration) (map[string]bool, preregisteredPriorEventManifest, error) {
	events := map[string]bool{}
	if p.Route == "rfq" {
		var err error
		events, err = preregisteredEquivalentBundleEvents(ctx, q, p, time.Time{},
			p.UntouchedStart, 0)
		if err != nil {
			return nil, preregisteredPriorEventManifest{}, err
		}
	} else {
		rows, err := q.QueryContext(ctx, `SELECT DISTINCT canonical_event_id
FROM research_system_observations
WHERE system_id=? AND experiment_version=? AND cohort=? AND venue=? AND route=?
 AND observation_kind!='control' AND julianday(observed_ts)<julianday(?)
 AND canonical_event_id!='' ORDER BY canonical_event_id`, p.SystemID, p.ExperimentVersion,
			p.Cohort, p.Venue, p.Route, p.UntouchedStart.Format(time.RFC3339Nano))
		if err != nil {
			return nil, preregisteredPriorEventManifest{}, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, preregisteredPriorEventManifest{}, err
			}
			events[id] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, preregisteredPriorEventManifest{}, err
		}
		if err := rows.Close(); err != nil {
			return nil, preregisteredPriorEventManifest{}, err
		}
	}
	ids := make([]string, 0, len(events))
	for id := range events {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	hash, err := r139Hash(map[string]any{"system_id": p.SystemID,
		"experiment_version": p.ExperimentVersion, "cohort": p.Cohort, "venue": p.Venue,
		"route": p.Route, "untouched_start_ts": p.UntouchedStart.Format(time.RFC3339Nano),
		"canonical_event_ids": ids})
	if err != nil {
		return nil, preregisteredPriorEventManifest{}, err
	}
	return events, preregisteredPriorEventManifest{Count: len(ids),
		Hash: strings.TrimPrefix(hash, "sha256:")}, nil
}

type ResearchInferencePreregistration struct {
	PreregistrationID, SystemID, ExperimentSpecHash string
	Cohort, Venue, Route                            string
	Created, TrainValidationCutoff                  time.Time
	UntouchedStart, UntouchedEnd, SealAt            time.Time
	Embargo                                         time.Duration
	RequiredDays, RequiredEvents, ExperimentVersion int
	PipelineVersion                                 int
	CodeManifestHash, DataManifestHash              string
	SourceManifestHash, InferenceContractHash       string
	FrozenInputManifestHash, SpecHash               string
}

type PreregisteredUntouchedSealRequest struct {
	PreregistrationID                         string
	CodeManifestHash, DataManifestHash        string
	SourceManifestHash, InferenceContractHash string
}

func normalizeHash64(v string) (string, bool) {
	v = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(v)), "sha256:")
	return v, validSHA256(v)
}

func utcDayBoundary(t time.Time) bool {
	return !t.IsZero() && t.Equal(t.UTC().Truncate(24*time.Hour))
}

func (s *Store) currentInferenceContract(ctx context.Context, system string) (version, requiredDays int,
	specHash, contractHash string, err error) {
	var inferenceJSON, holdoutJSON, stoppingRule string
	err = s.db.QueryRowContext(ctx, `SELECT version,spec_hash,required_days,inference_json,holdout_json,stopping_rule
FROM research_experiment_specs WHERE experiment_id=? ORDER BY version DESC LIMIT 1`, system).
		Scan(&version, &specHash, &requiredDays, &inferenceJSON, &holdoutJSON, &stoppingRule)
	if err != nil {
		return
	}
	h, hashErr := r139Hash(map[string]any{"pipeline_version": R139InferencePipelineVersion,
		"system_id": system, "experiment_version": version, "experiment_spec_hash": specHash,
		"inference_json": inferenceJSON, "holdout_json": holdoutJSON, "stopping_rule": stoppingRule})
	if hashErr != nil {
		err = hashErr
		return
	}
	contractHash = strings.TrimPrefix(h, "sha256:")
	return
}

func (s *Store) frozenPreregInputManifest(ctx context.Context, p ResearchInferencePreregistration) (string, error) {
	return frozenPreregInputManifest(ctx, s.db, p)
}

func frozenPreregInputManifest(ctx context.Context, q step7ProjectionQuerier,
	p ResearchInferencePreregistration) (string, error) {
	if pending, err := step7ProjectionPendingInWindow(ctx, q, time.Time{},
		p.TrainValidationCutoff); err != nil {
		return "", err
	} else if pending > 0 {
		return "", fmt.Errorf("training/validation input window incomplete: %d Step-7 projections pending",
			pending)
	}
	projectionManifest, err := step7ProjectionCompletionManifest(ctx, q, p.TrainValidationCutoff)
	if err != nil {
		return "", err
	}
	if p.Route == "rfq" {
		rows, err := preregisteredBundleRawRowsPageSize(ctx, q, p, time.Time{},
			p.TrainValidationCutoff, p.TrainValidationCutoff, 0, 5000)
		if err != nil {
			return "", err
		}
		h, err := r139Hash(map[string]any{"system_id": p.SystemID, "cohort": p.Cohort,
			"venue": p.Venue, "route": p.Route, "cutoff": p.TrainValidationCutoff.Format(time.RFC3339Nano),
			"selection":                   "verified complete typed Kalshi bundle whose additive state vector exactly equals the MVE conjunction vector",
			"projection_completed_count":  projectionManifest.Count,
			"projection_max_completed_ts": projectionManifest.MaxCompletedTS,
			"projection_manifest_hash":    projectionManifest.Hash,
			"rows":                        rows})
		return strings.TrimPrefix(h, "sha256:"), err
	}
	var rows, maxObservation, maxUpdate int64
	err = q.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MAX(id),0) FROM research_system_observations
WHERE system_id=? AND cohort=? AND venue=? AND route=?
	 AND observation_kind!='control' AND julianday(observed_ts)<julianday(?)`, p.SystemID, p.Cohort,
		p.Venue, p.Route, p.TrainValidationCutoff.Format(time.RFC3339Nano)).Scan(&rows, &maxObservation)
	if err != nil {
		return "", err
	}
	err = q.QueryRowContext(ctx, `SELECT COALESCE(MAX(u.id),0) FROM research_system_payoff_updates u
JOIN research_system_observations o ON o.id=u.observation_id
WHERE o.system_id=? AND o.cohort=? AND o.venue=? AND o.route=?
	 AND o.observation_kind!='control' AND julianday(o.observed_ts)<julianday(?) AND julianday(u.observed_ts)<=julianday(?)`,
		p.SystemID, p.Cohort, p.Venue, p.Route, p.TrainValidationCutoff.Format(time.RFC3339Nano),
		p.TrainValidationCutoff.Format(time.RFC3339Nano)).Scan(&maxUpdate)
	if err != nil {
		return "", err
	}
	h, err := r139Hash(map[string]any{"system_id": p.SystemID, "cohort": p.Cohort, "venue": p.Venue,
		"route": p.Route, "cutoff": p.TrainValidationCutoff.Format(time.RFC3339Nano), "rows": rows,
		"max_observation_id": maxObservation, "max_payoff_update_id": maxUpdate,
		"projection_completed_count":  projectionManifest.Count,
		"projection_max_completed_ts": projectionManifest.MaxCompletedTS,
		"projection_manifest_hash":    projectionManifest.Hash})
	return strings.TrimPrefix(h, "sha256:"), err
}

func (s *Store) registerResearchInferencePreregistrationAt(ctx context.Context,
	p ResearchInferencePreregistration, now time.Time) (ResearchInferencePreregistration, bool, error) {
	now = now.UTC()
	p.PreregistrationID, p.SystemID = strings.TrimSpace(p.PreregistrationID), strings.TrimSpace(p.SystemID)
	p.Cohort, p.Venue, p.Route = strings.TrimSpace(p.Cohort), strings.ToLower(strings.TrimSpace(p.Venue)), strings.ToLower(strings.TrimSpace(p.Route))
	if p.PreregistrationID == "" || !researchSystemIDKnown(p.SystemID) || p.Cohort == "" ||
		(p.Venue != "kalshi" && p.Venue != "polyus" && p.Venue != "polymarket") ||
		(p.Route != "taker" && p.Route != "maker" && p.Route != "maker-control" && p.Route != "rfq") {
		return p, false, errors.New("invalid preregistered system/cohort/venue/route")
	}
	for dst, raw := range map[string]*string{"code": &p.CodeManifestHash, "data": &p.DataManifestHash,
		"source": &p.SourceManifestHash} {
		h, ok := normalizeHash64(*raw)
		if !ok {
			return p, false, fmt.Errorf("invalid %s manifest hash", dst)
		}
		*raw = h
	}
	p.TrainValidationCutoff, p.UntouchedStart = p.TrainValidationCutoff.UTC(), p.UntouchedStart.UTC()
	p.UntouchedEnd, p.SealAt = p.UntouchedEnd.UTC(), p.SealAt.UTC()
	if !utcDayBoundary(p.TrainValidationCutoff) || !utcDayBoundary(p.UntouchedStart) ||
		!utcDayBoundary(p.UntouchedEnd) || !utcDayBoundary(p.SealAt) || p.Embargo < 24*time.Hour ||
		p.UntouchedStart.Before(p.TrainValidationCutoff.Add(p.Embargo)) || !p.UntouchedEnd.After(p.UntouchedStart) ||
		p.SealAt.Before(p.UntouchedEnd) || !p.UntouchedStart.After(now) {
		return p, false, errors.New("preregistration requires future disjoint UTC-day window, at least one-day embargo, and frozen seal time")
	}
	version, contractDays, specHash, contractHash, err := s.currentInferenceContract(ctx, p.SystemID)
	if err != nil {
		return p, false, err
	}
	if p.ExperimentVersion != 0 && p.ExperimentVersion != version {
		return p, false, errors.New("preregistration experiment version is not current")
	}
	p.ExperimentVersion, p.ExperimentSpecHash = version, specHash
	p.PipelineVersion, p.InferenceContractHash = R139InferencePipelineVersion, contractHash
	if p.RequiredDays < contractDays {
		p.RequiredDays = contractDays
	}
	if p.RequiredEvents < p.RequiredDays {
		p.RequiredEvents = p.RequiredDays
	}
	if p.RequiredDays < 2 || p.UntouchedEnd.Sub(p.UntouchedStart) < time.Duration(p.RequiredDays)*24*time.Hour {
		return p, false, errors.New("frozen untouched window is shorter than the inference contract")
	}
	p.Created = now
	var preexistingUntouched int
	if p.Route == "rfq" {
		events, rowsErr := s.preregisteredEquivalentBundleEvents(ctx, p, p.UntouchedStart, time.Time{}, 1)
		if rowsErr != nil {
			return p, false, rowsErr
		}
		preexistingUntouched = len(events)
	} else if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_system_observations
WHERE system_id=? AND cohort=? AND venue=? AND route=?
	 AND observation_kind!='control' AND julianday(observed_ts)>=julianday(?)`, p.SystemID, p.Cohort,
		p.Venue, p.Route, p.UntouchedStart.Format(time.RFC3339Nano)).Scan(&preexistingUntouched); err != nil {
		return p, false, err
	}
	if preexistingUntouched != 0 {
		return p, false, errors.New("untouched rows already exist; preregistration must precede every test observation")
	}
	p.FrozenInputManifestHash, err = s.frozenPreregInputManifest(ctx, p)
	if err != nil {
		return p, false, err
	}
	// One immutable look per exact system/version/cohort/venue/route contract. A caller cannot
	// repeatedly preregister fresh IDs until one happens to pass. A materially new experiment must
	// bump its experiment version or freeze a genuinely different cohort/route contract.
	if _, err = s.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS idx_rinference_one_look_contract
ON research_inference_preregistrations(system_id,experiment_version,cohort,venue,route)`); err != nil {
		return p, false, fmt.Errorf("cannot enforce one-look preregistration contract: %w", err)
	}
	var priorID string
	err = s.db.QueryRowContext(ctx, `SELECT preregistration_id FROM research_inference_preregistrations
WHERE system_id=? AND experiment_version=? AND cohort=? AND venue=? AND route=? LIMIT 1`,
		p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route).Scan(&priorID)
	if err == nil && priorID != p.PreregistrationID {
		return p, false, errors.New("sequential re-preregistration is forbidden for an unchanged experiment contract")
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return p, false, err
	}
	semantic := map[string]any{"preregistration_id": p.PreregistrationID, "created_ts": p.Created.Format(time.RFC3339Nano),
		"system_id": p.SystemID, "experiment_version": p.ExperimentVersion, "experiment_spec_hash": p.ExperimentSpecHash,
		"cohort": p.Cohort, "venue": p.Venue, "route": p.Route,
		"train_validation_cutoff_ts": p.TrainValidationCutoff.Format(time.RFC3339Nano),
		"untouched_start_ts":         p.UntouchedStart.Format(time.RFC3339Nano), "untouched_end_ts": p.UntouchedEnd.Format(time.RFC3339Nano),
		"seal_at_ts": p.SealAt.Format(time.RFC3339Nano), "embargo_seconds": p.Embargo.Seconds(),
		"required_days": p.RequiredDays, "required_events": p.RequiredEvents, "pipeline_version": p.PipelineVersion,
		"code_manifest_hash": p.CodeManifestHash, "data_manifest_hash": p.DataManifestHash,
		"source_manifest_hash": p.SourceManifestHash, "inference_contract_hash": p.InferenceContractHash,
		"frozen_input_manifest_hash": p.FrozenInputManifestHash}
	h, err := r139Hash(semantic)
	if err != nil {
		return p, false, err
	}
	p.SpecHash = strings.TrimPrefix(h, "sha256:")
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_inference_preregistrations(
preregistration_id,created_ts,system_id,experiment_version,experiment_spec_hash,cohort,venue,route,
train_validation_cutoff_ts,untouched_start_ts,untouched_end_ts,seal_at_ts,embargo_seconds,required_days,
required_events,pipeline_version,code_manifest_hash,data_manifest_hash,source_manifest_hash,
inference_contract_hash,frozen_input_manifest_hash,spec_hash,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, p.PreregistrationID, p.Created.Format(time.RFC3339Nano),
		p.SystemID, p.ExperimentVersion, p.ExperimentSpecHash, p.Cohort, p.Venue, p.Route,
		p.TrainValidationCutoff.Format(time.RFC3339Nano), p.UntouchedStart.Format(time.RFC3339Nano),
		p.UntouchedEnd.Format(time.RFC3339Nano), p.SealAt.Format(time.RFC3339Nano), p.Embargo.Seconds(),
		p.RequiredDays, p.RequiredEvents, p.PipelineVersion, p.CodeManifestHash, p.DataManifestHash,
		p.SourceManifestHash, p.InferenceContractHash, p.FrozenInputManifestHash, p.SpecHash)
	if err != nil {
		return p, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return p, false, err
	}
	if n == 0 {
		var existing string
		err := s.db.QueryRowContext(ctx, `SELECT spec_hash FROM research_inference_preregistrations WHERE preregistration_id=?`,
			p.PreregistrationID).Scan(&existing)
		if errors.Is(err, sql.ErrNoRows) {
			return p, false, errors.New("sequential re-preregistration is forbidden for an unchanged experiment contract")
		}
		if err != nil {
			return p, false, err
		}
		if existing != p.SpecHash {
			return p, false, errors.New("immutable preregistration id already has different semantics")
		}
	}
	return p, n > 0, nil
}

func (s *Store) RegisterResearchInferencePreregistration(ctx context.Context,
	p ResearchInferencePreregistration) (ResearchInferencePreregistration, bool, error) {
	return s.registerResearchInferencePreregistrationAt(ctx, p, time.Now().UTC())
}

func (s *Store) researchInferencePreregistration(ctx context.Context, id string) (ResearchInferencePreregistration, error) {
	var p ResearchInferencePreregistration
	var created, cutoff, start, end, seal string
	var embargo float64
	err := s.db.QueryRowContext(ctx, `SELECT preregistration_id,created_ts,system_id,experiment_version,
experiment_spec_hash,cohort,venue,route,train_validation_cutoff_ts,untouched_start_ts,untouched_end_ts,
seal_at_ts,embargo_seconds,required_days,required_events,pipeline_version,code_manifest_hash,
data_manifest_hash,source_manifest_hash,inference_contract_hash,frozen_input_manifest_hash,spec_hash
FROM research_inference_preregistrations WHERE preregistration_id=?`, strings.TrimSpace(id)).Scan(
		&p.PreregistrationID, &created, &p.SystemID, &p.ExperimentVersion, &p.ExperimentSpecHash,
		&p.Cohort, &p.Venue, &p.Route, &cutoff, &start, &end, &seal, &embargo, &p.RequiredDays,
		&p.RequiredEvents, &p.PipelineVersion, &p.CodeManifestHash, &p.DataManifestHash,
		&p.SourceManifestHash, &p.InferenceContractHash, &p.FrozenInputManifestHash, &p.SpecHash)
	if err != nil {
		return p, err
	}
	for raw, dst := range map[string]*time.Time{created: &p.Created, cutoff: &p.TrainValidationCutoff,
		start: &p.UntouchedStart, end: &p.UntouchedEnd, seal: &p.SealAt} {
		t, parseErr := time.Parse(time.RFC3339Nano, raw)
		if parseErr != nil {
			return p, parseErr
		}
		*dst = t
	}
	p.Embargo = time.Duration(embargo * float64(time.Second))
	return p, nil
}

func (s *Store) loadPreregisteredRows(ctx context.Context, p ResearchInferencePreregistration) ([]r139RawInferenceRow, error) {
	if pending, err := s.Step7ProjectionPendingInWindow(ctx, time.Time{}, p.UntouchedEnd); err != nil {
		return nil, err
	} else if pending > 0 {
		return nil, fmt.Errorf("sealed inference window incomplete: %d Step-7 projections pending", pending)
	}
	if p.Route == "rfq" {
		return s.preregisteredBundleRawRows(ctx, p, p.UntouchedStart, p.UntouchedEnd,
			p.SealAt, 0)
	}
	const pageSize = 5000
	var out []r139RawInferenceRow
	var afterID int64
	var maxObservationID, maxPayoffUpdateID int64
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM research_system_observations`).Scan(&maxObservationID); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM research_system_payoff_updates`).Scan(&maxPayoffUpdateID); err != nil {
		return nil, err
	}
	for {
		rows, err := s.db.QueryContext(ctx, `SELECT o.id,o.observed_ts,o.system_id,o.experiment_version,o.canonical_event_id,o.event_version,
COALESCE(o.canonical_payoff_id,''),COALESCE(o.payoff_version,0),o.instrument_version,o.ticker,o.venue,o.route,o.certificate_status,
o.source_clock_id,o.book_source,o.fee_source,o.size_units,o.tick_min,o.visible_capacity,o.latency_known,
o.quote_age_known,o.tick_known,o.depth_known,o.fee_known,o.outcome_status,
EXISTS(SELECT 1 FROM research_event_specs e WHERE e.event_id=o.canonical_event_id AND e.version=o.event_version),
EXISTS(SELECT 1 FROM research_instrument_specs i JOIN research_payoff_specs x
 ON x.event_id=i.event_id AND x.event_version=i.event_version AND x.payoff_id=i.payoff_id AND x.version=i.payoff_version
 WHERE i.venue=o.venue AND i.ticker=o.ticker AND i.event_id=o.canonical_event_id AND i.event_version=o.event_version
 AND i.payoff_id=o.canonical_payoff_id AND i.payoff_version=o.payoff_version
 AND o.instrument_version>0 AND i.version=o.instrument_version),
u.id,u.observed_ts,u.status,u.payout_lower,u.payout_upper,u.realized_net
FROM research_system_observations o LEFT JOIN research_system_payoff_updates u ON u.id=(
	 SELECT u2.id FROM research_system_payoff_updates u2 WHERE u2.observation_id=o.id AND julianday(u2.observed_ts)<=julianday(?) AND u2.id<=?
 ORDER BY u2.id DESC LIMIT 1)
WHERE o.system_id=? AND o.experiment_version=? AND o.cohort=? AND o.venue=? AND o.route=?
	 AND o.observation_kind!='control' AND julianday(o.observed_ts)>=julianday(?) AND julianday(o.observed_ts)<julianday(?)
 AND o.id>? AND o.id<=?
ORDER BY o.id LIMIT ?`, p.SealAt.Format(time.RFC3339Nano), maxPayoffUpdateID,
			p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route,
			p.UntouchedStart.Format(time.RFC3339Nano), p.UntouchedEnd.Format(time.RFC3339Nano),
			afterID, maxObservationID, pageSize)
		if err != nil {
			return nil, err
		}
		pageN := 0
		for rows.Next() {
			var r r139RawInferenceRow
			var latency, quoteAge, tick, depth, fee, eventKnown, payoffKnown int
			var updateID sql.NullInt64
			var updateTS, updateStatus sql.NullString
			var low, high, realized sql.NullFloat64
			if err := rows.Scan(&r.ObservationID, &r.ObservedTS, &r.SystemID, &r.ExperimentVersion,
				&r.CanonicalEventID, &r.EventVersion,
				&r.CanonicalPayoffID, &r.PayoffVersion, &r.InstrumentVersion, &r.Ticker, &r.Venue, &r.Route, &r.CertificateStatus,
				&r.SourceClockID, &r.BookSource, &r.FeeSource, &r.Size, &r.Tick, &r.VisibleCapacity,
				&latency, &quoteAge, &tick, &depth, &fee, &r.ObservationStatus, &eventKnown, &payoffKnown,
				&updateID, &updateTS, &updateStatus, &low, &high, &realized); err != nil {
				rows.Close()
				return nil, err
			}
			r.LatencyKnown, r.QuoteAgeKnown, r.TickKnown = latency == 1, quoteAge == 1, tick == 1
			r.DepthKnown, r.FeeKnown = depth == 1, fee == 1
			r.CanonicalEventKnown, r.CanonicalPayoffKnown = eventKnown == 1, payoffKnown == 1
			if updateID.Valid {
				r.PayoffUpdateID, r.PayoffUpdateTS, r.PayoffStatus = updateID.Int64, updateTS.String, updateStatus.String
				r.PayoutLower, r.PayoutUpper = low.Float64, high.Float64
				if realized.Valid {
					v := realized.Float64
					r.RealizedNet = &v
				}
			}
			out, afterID, pageN = append(out, r), r.ObservationID, pageN+1
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if pageN < pageSize {
			break
		}
	}
	return out, nil
}

// preregisteredBundleRawRows is the only typed-bundle selector admitted to the sealed inference
// engine. It drops additive locks and every other portfolio whose certified payout vector is not
// exactly the Kalshi MVE all-legs-win vector. Thus route=rfq freezes a security identity, not merely
// a list of similarly named legs.
func (s *Store) preregisteredBundleRawRows(ctx context.Context, p ResearchInferencePreregistration,
	start, end, gradeCutoff time.Time, limit int) ([]r139RawInferenceRow, error) {
	return preregisteredBundleRawRowsPageSize(ctx, s.db, p, start, end, gradeCutoff, limit, 5000)
}

func (s *Store) preregisteredBundleRawRowsPageSize(ctx context.Context, p ResearchInferencePreregistration,
	start, end, gradeCutoff time.Time, limit, pageSize int) ([]r139RawInferenceRow, error) {
	return preregisteredBundleRawRowsPageSize(ctx, s.db, p, start, end, gradeCutoff, limit, pageSize)
}

func preregisteredBundleRawRowsPageSize(ctx context.Context, q step7ProjectionQuerier,
	p ResearchInferencePreregistration, start, end, gradeCutoff time.Time, limit,
	pageSize int) ([]r139RawInferenceRow, error) {
	if p.Venue != "kalshi" || p.Route != "rfq" || limit < 0 || pageSize <= 0 || pageSize > 5000 {
		return nil, errors.New("typed bundle preregistration requires kalshi/rfq")
	}
	var maxBundleRowID, maxGradeEventID int64
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid),0) FROM research_route_bundles`).Scan(&maxBundleRowID); err != nil {
		return nil, err
	}
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM research_route_bundle_events`).Scan(&maxGradeEventID); err != nil {
		return nil, err
	}
	queryBase := `SELECT b.rowid,b.bundle_id,b.observed_ts,b.system_id,b.canonical_event_id,b.event_version,
b.state_vector_hash,b.size_units,e.id,e.observed_ts,e.outcome_status,e.payout,e.realized_net
FROM research_route_bundles b JOIN research_route_bundle_events e ON e.bundle_id=b.bundle_id AND e.event_type='grade'
WHERE b.system_id=? AND b.experiment_version=? AND b.cohort=? AND b.certificate_status='verified'
	 AND julianday(e.observed_ts)<=julianday(?) AND b.rowid<=? AND e.id<=?`
	baseArgs := []any{p.SystemID, p.ExperimentVersion, p.Cohort,
		gradeCutoff.UTC().Format(time.RFC3339Nano), maxBundleRowID, maxGradeEventID}
	if !start.IsZero() {
		queryBase += ` AND julianday(b.observed_ts)>=julianday(?)`
		baseArgs = append(baseArgs, start.UTC().Format(time.RFC3339Nano))
	}
	if !end.IsZero() {
		queryBase += ` AND julianday(b.observed_ts)<julianday(?)`
		baseArgs = append(baseArgs, end.UTC().Format(time.RFC3339Nano))
	}
	type candidate struct {
		rowID, gradeID                        int64
		bundleID, observed, systemID, eventID string
		eventVersion                          int
		stateHash                             string
		size                                  float64
		gradeTS, status                       string
		payout, realized                      float64
	}
	candidates := []candidate{}
	var afterRowID int64
	for limit == 0 || len(candidates) < limit {
		fetch := pageSize
		if limit > 0 && limit-len(candidates) < fetch {
			fetch = limit - len(candidates)
		}
		args := append(append([]any{}, baseArgs...), afterRowID, fetch)
		rows, err := q.QueryContext(ctx, queryBase+` AND b.rowid>? ORDER BY b.rowid LIMIT ?`, args...)
		if err != nil {
			return nil, err
		}
		pageN := 0
		for rows.Next() {
			var c candidate
			if err := rows.Scan(&c.rowID, &c.bundleID, &c.observed, &c.systemID, &c.eventID,
				&c.eventVersion, &c.stateHash, &c.size, &c.gradeID, &c.gradeTS, &c.status,
				&c.payout, &c.realized); err != nil {
				rows.Close()
				return nil, err
			}
			candidates, afterRowID, pageN = append(candidates, c), c.rowID, pageN+1
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if pageN < fetch {
			break
		}
	}
	out := make([]r139RawInferenceRow, 0, len(candidates))
	for _, c := range candidates {
		b, found, readErr := researchRouteBundleByID(ctx, q, c.bundleID)
		if readErr != nil {
			return nil, readErr
		}
		if !found || b.CertificateStatus != "verified" ||
			!researchBundleKalshiComboEquivalent(b.Size, b.Legs, b.States) {
			continue
		}
		tick, capacity := math.Inf(1), math.Inf(1)
		clocks, books, fees := []string{}, []string{}, []string{}
		for _, leg := range b.Legs {
			tick, capacity = math.Min(tick, leg.Tick), math.Min(capacity, leg.VisibleDepth)
			clocks, books, fees = append(clocks, leg.SourceClockID), append(books, leg.BookSource), append(fees, leg.FeeSource)
		}
		realized := c.realized
		status := "settled"
		if strings.Contains(strings.ToLower(c.status), "void") {
			status = "voided"
		}
		out = append(out, r139RawInferenceRow{ObservationID: c.rowID, ObservedTS: c.observed,
			SystemID: c.systemID, CanonicalEventID: c.eventID, EventVersion: c.eventVersion,
			CanonicalPayoffID: "typed-state-vector:" + c.stateHash, PayoffVersion: 1,
			Ticker: c.bundleID, Venue: "kalshi", Route: "rfq", CertificateStatus: "verified",
			SourceClockID: strings.Join(clocks, "|"), BookSource: strings.Join(books, "|"),
			FeeSource: strings.Join(fees, "|"), Size: c.size, Tick: tick, VisibleCapacity: capacity,
			LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
			ObservationStatus: status, CanonicalEventKnown: true, CanonicalPayoffKnown: true,
			PayoffUpdateID: c.gradeID, PayoffUpdateTS: c.gradeTS, PayoffStatus: status,
			PayoutLower: c.payout, PayoutUpper: c.payout, RealizedNet: &realized})
	}
	return out, nil
}

func validatePreregisteredImmutableSealInputsTx(ctx context.Context, tx *sql.Tx,
	p ResearchInferencePreregistration, wantPrior preregisteredPriorEventManifest,
	wantRFQUntouched string) error {
	currentFrozenInputs, err := frozenPreregInputManifest(ctx, tx, p)
	if err != nil {
		return err
	}
	if currentFrozenInputs != p.FrozenInputManifestHash {
		return errors.New("training/validation input manifest changed during frozen seal")
	}
	_, currentPrior, err := preregisteredPriorEvents(ctx, tx, p)
	if err != nil {
		return err
	}
	if currentPrior != wantPrior {
		return fmt.Errorf("prior-event membership changed during frozen seal: got %+v want %+v",
			currentPrior, wantPrior)
	}
	if p.Route == "rfq" {
		currentRFQ, err := preregisteredRFQUntouchedManifest(ctx, tx, p)
		if err != nil {
			return err
		}
		if currentRFQ != wantRFQUntouched {
			return errors.New("untouched RFQ bundle/grade manifest changed during frozen seal")
		}
	}
	return nil
}

func (s *Store) sealPreregisteredUntouchedAt(ctx context.Context, req PreregisteredUntouchedSealRequest,
	now time.Time) (ResearchInferenceReport, bool, error) {
	p, err := s.researchInferencePreregistration(ctx, req.PreregistrationID)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	now = now.UTC()
	if now.Before(p.SealAt) {
		return ResearchInferenceReport{}, false, errors.New("frozen untouched seal time has not arrived")
	}
	for label, pair := range map[string][2]string{"code": {req.CodeManifestHash, p.CodeManifestHash},
		"data": {req.DataManifestHash, p.DataManifestHash}, "source": {req.SourceManifestHash, p.SourceManifestHash},
		"inference": {req.InferenceContractHash, p.InferenceContractHash}} {
		got, ok := normalizeHash64(pair[0])
		if !ok || got != pair[1] {
			return ResearchInferenceReport{}, false, fmt.Errorf("%s manifest/contract changed after preregistration", label)
		}
	}
	version, _, specHash, contractHash, err := s.currentInferenceContract(ctx, p.SystemID)
	if err != nil || version != p.ExperimentVersion || specHash != p.ExperimentSpecHash || contractHash != p.InferenceContractHash {
		return ResearchInferenceReport{}, false, errors.New("experiment or inference contract changed after preregistration")
	}
	contracts, err := s.r139InferenceContracts(ctx)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	// Strong family-wise error control across the complete registered system family. The seal is a
	// single frozen look, and registration above forbids a second look at the same contract.
	adjustedAlpha := .05 / float64(len(contracts))
	var priorRun int64
	if err := s.db.QueryRowContext(ctx, `SELECT id FROM research_inference_runs WHERE preregistration_id=?`, p.PreregistrationID).Scan(&priorRun); err == nil {
		report, readErr := s.ResearchInferenceRun(ctx, priorRun)
		return report, false, readErr
	} else if !errors.Is(err, sql.ErrNoRows) {
		return ResearchInferenceReport{}, false, err
	}
	currentFrozenInputs, err := s.frozenPreregInputManifest(ctx, p)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	if currentFrozenInputs != p.FrozenInputManifestHash {
		return ResearchInferenceReport{}, false, errors.New("training/validation input manifest changed after preregistration")
	}
	rfqUntouchedManifest := ""
	if p.Route == "rfq" {
		rfqUntouchedManifest, err = preregisteredRFQUntouchedManifest(ctx, s.db, p)
		if err != nil {
			return ResearchInferenceReport{}, false, err
		}
	}
	rows, err := s.loadPreregisteredRows(ctx, p)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	if p.Route == "rfq" {
		afterLoad, err := preregisteredRFQUntouchedManifest(ctx, s.db, p)
		if err != nil {
			return ResearchInferenceReport{}, false, err
		}
		if afterLoad != rfqUntouchedManifest {
			return ResearchInferenceReport{}, false,
				errors.New("untouched RFQ bundle/grade manifest changed while loading frozen rows")
		}
	}
	terminal, exclusions, err := r139ClassifyRows(rows)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	priorEvents, priorEventManifest, err := preregisteredPriorEvents(ctx, s.db, p)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	filtered := terminal[:0]
	purged := 0
	for _, row := range terminal {
		if priorEvents[row.EventID] {
			purged++
			continue
		}
		filtered = append(filtered, row)
	}
	terminal = filtered
	eventAgg := map[string]struct {
		sum float64
		n   int
		day time.Time
	}{}
	for _, row := range terminal {
		key := row.EventID
		a := eventAgg[key]
		a.sum, a.n = a.sum+row.Value, a.n+1
		if a.day.IsZero() || row.Day.Before(a.day) {
			a.day = row.Day
		}
		eventAgg[key] = a
	}
	clustered := make([]researchstats.Observation, 0, len(eventAgg))
	for key, a := range eventAgg {
		clustered = append(clustered, researchstats.Observation{EventID: key, Day: a.day, Cell: p.SystemID + "|" + p.Venue + "|" + p.Route, Value: a.sum / float64(a.n)})
	}
	result := ResearchInferenceSystemResult{SystemID: p.SystemID, ExperimentVersion: p.ExperimentVersion,
		State: "PREREGISTERED_UNTOUCHED_FAIL", Reason: "frozen untouched contract did not clear",
		TerminalRows: len(terminal), PurgedEvents: purged, UntouchedEvents: len(eventAgg), RawP: 1, HolmP: 1, BYQ: 1,
		UntouchedP: 1, Cells: []researchstats.CellEstimate{}}
	if len(clustered) > 0 {
		days := make([]float64, int(p.UntouchedEnd.Sub(p.UntouchedStart)/(24*time.Hour)))
		for _, row := range clustered {
			i := int(row.Day.UTC().Truncate(24*time.Hour).Sub(p.UntouchedStart) / (24 * time.Hour))
			if i >= 0 && i < len(days) {
				days[i] += row.Value
			}
		}
		result.UntouchedDays = len(days)
		if bounds, boundsErr := researchstats.DeterministicDayBlockBounds(days, r139InferenceReplicates,
			r139Seed(p.SystemID+"|"+p.PreregistrationID, 2391)); boundsErr == nil {
			result.UntouchedMean, result.UntouchedLower, result.UntouchedUpper = bounds.Mean, bounds.Lower, bounds.Upper
			result.UntouchedBoundsKnown = true
			result.UntouchedP, _ = researchstats.DeterministicDaySignP(days, r139InferenceReplicates,
				r139Seed(p.SystemID+"|"+p.PreregistrationID, 2392))
			result.RawP = result.UntouchedP
		}
	}
	exclusionN := exclusions.Open + exclusions.Void + exclusions.Censored + exclusions.NonExact + exclusions.Identity + exclusions.RouteTruth
	result.HolmP = math.Min(1, result.UntouchedP*float64(len(contracts)))
	result.BYQ = result.HolmP
	pass := result.UntouchedBoundsKnown && result.UntouchedEvents >= p.RequiredEvents &&
		result.UntouchedDays >= p.RequiredDays && result.UntouchedLower > 0 && result.UntouchedP <= adjustedAlpha &&
		exclusionN == 0 && purged == 0
	if pass {
		result.State, result.Reason = "PREREGISTERED_UNTOUCHED_PASS", fmt.Sprintf("one-time frozen disjoint untouched event/day lower bound cleared all-system Bonferroni alpha %.8f", adjustedAlpha)
		result.PreregisteredUntouchedGatePass, result.ExecutionCandidate = true, true
	} else {
		result.Reason = fmt.Sprintf("frozen one-time evaluation failed: days=%d/%d events=%d/%d lower=%.6f p=%.6f all-system-alpha=%.8f exclusions=%d purged=%d",
			result.UntouchedDays, p.RequiredDays, result.UntouchedEvents, p.RequiredEvents,
			result.UntouchedLower, result.UntouchedP, adjustedAlpha, exclusionN, purged)
	}
	results := r139BlockedResults(contracts, "NOT_IN_PREREGISTRATION", "this immutable preregistration names a different system")
	for i := range results {
		if results[i].SystemID == p.SystemID {
			results[i] = result
		}
	}
	rowsHash, _ := r139Hash(rows)
	payoffManifest, err := researchPayoffUpdateManifestFromRows(nil)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	if p.Route != "rfq" {
		payoffManifest, err = researchPayoffUpdateManifestFromRows(rows)
		if err != nil {
			return ResearchInferenceReport{}, false, err
		}
	}
	projectionManifest, err := s.Step7ProjectionCompletionManifest(ctx, p.UntouchedEnd)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	manifest := map[string]any{"preregistration_id": p.PreregistrationID, "preregistration_spec_hash": p.SpecHash,
		"system_id": p.SystemID, "cohort": p.Cohort, "venue": p.Venue, "route": p.Route,
		"untouched_start_ts": p.UntouchedStart.Format(time.RFC3339Nano), "untouched_end_ts": p.UntouchedEnd.Format(time.RFC3339Nano),
		"seal_at_ts": p.SealAt.Format(time.RFC3339Nano), "rows_hash": rowsHash, "observed_rows": len(rows),
		"code_manifest_hash": p.CodeManifestHash, "data_manifest_hash": p.DataManifestHash,
		"source_manifest_hash": p.SourceManifestHash, "inference_contract_hash": p.InferenceContractHash,
		"projection_completed_count":     projectionManifest.Count,
		"projection_max_completed_ts":    projectionManifest.MaxCompletedTS,
		"projection_manifest_hash":       projectionManifest.Hash,
		"payoff_observation_count":       payoffManifest.ObservationCount,
		"payoff_update_count":            payoffManifest.UpdateCount,
		"payoff_update_max_id":           payoffManifest.MaxUpdateID,
		"payoff_update_manifest_hash":    payoffManifest.Hash,
		"prior_event_count":              priorEventManifest.Count,
		"prior_event_manifest_hash":      priorEventManifest.Hash,
		"rfq_untouched_manifest_hash":    rfqUntouchedManifest,
		"all_system_multiplicity_method": "Bonferroni FWER", "family_size": len(contracts),
		"familywise_alpha": .05, "per_contract_alpha": adjustedAlpha,
		"sequential_contract": "one frozen seal and one preregistration per unchanged system/version/cohort/venue/route"}
	manifestJSON, _ := json.Marshal(manifest)
	manifestHash, _ := r139Hash(manifest)
	resultHash, _ := r139Hash(results)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	defer tx.Rollback()
	if err := validateStep7ProjectionFenceTx(ctx, tx, time.Time{}, p.UntouchedEnd,
		projectionManifest); err != nil {
		return ResearchInferenceReport{}, false, err
	}
	if err := validatePreregisteredImmutableSealInputsTx(ctx, tx, p,
		priorEventManifest, rfqUntouchedManifest); err != nil {
		return ResearchInferenceReport{}, false, err
	}
	if p.Route != "rfq" {
		currentPayoffs, err := researchPayoffUpdateManifest(ctx, tx, p, p.SealAt)
		if err != nil {
			return ResearchInferenceReport{}, false, err
		}
		if currentPayoffs != payoffManifest {
			return ResearchInferenceReport{}, false,
				fmt.Errorf("untouched payoff-update manifest changed before seal: got %+v want %+v",
					currentPayoffs, payoffManifest)
		}
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO research_inference_runs(created_ts,as_of_completed_ts,
pipeline_version,input_manifest_hash,input_manifest_json,result_hash,preregistration_id,status,observed_rows,
exact_terminal_rows,excluded_open,excluded_void,excluded_censored,excluded_nonexact,excluded_identity,
excluded_route_truth,systems_reported,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,'sealed_preregistered_untouched',?,?,?,?,?,?,?,?,19,0,0,0)`, now.Format(time.RFC3339Nano),
		p.SealAt.Format(time.RFC3339Nano), R139InferencePipelineVersion, manifestHash, string(manifestJSON),
		resultHash, p.PreregistrationID, len(rows), len(terminal), exclusions.Open, exclusions.Void,
		exclusions.Censored, exclusions.NonExact, exclusions.Identity, exclusions.RouteTruth)
	if err != nil {
		return ResearchInferenceReport{}, false, err
	}
	runID, _ := res.LastInsertId()
	for _, row := range results {
		cells, _ := json.Marshal(row.Cells)
		_, err = tx.ExecContext(ctx, `INSERT INTO research_inference_system_results(run_id,system_id,
experiment_version,state,reason,terminal_rows,train_events,validation_events,monitoring_events,purged_events,
validation_days,validation_mean,validation_lower,validation_upper,validation_bounds_known,monitoring_days,
monitoring_mean,monitoring_lower,monitoring_upper,monitoring_bounds_known,raw_p,holm_p,by_q,
monitoring_lower_bound_positive,preregistered_untouched_gate_pass,cells_json,untouched_events,
untouched_days,untouched_mean,untouched_lower,untouched_upper,untouched_p,untouched_bounds_known,
execution_candidate,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, runID, row.SystemID,
			row.ExperimentVersion, row.State, row.Reason, row.TerminalRows, row.TrainEvents, row.ValidationEvents,
			row.MonitoringEvents, row.PurgedEvents, row.ValidationDays, row.ValidationMean, row.ValidationLower,
			row.ValidationUpper, boolInt(row.ValidationBoundsKnown), row.MonitoringDays, row.MonitoringMean,
			row.MonitoringLower, row.MonitoringUpper, boolInt(row.MonitoringBoundsKnown), row.RawP, row.HolmP,
			row.BYQ, boolInt(row.MonitoringLowerBoundPositive), boolInt(row.PreregisteredUntouchedGatePass),
			string(cells), row.UntouchedEvents, row.UntouchedDays, row.UntouchedMean, row.UntouchedLower,
			row.UntouchedUpper, row.UntouchedP, boolInt(row.UntouchedBoundsKnown), boolInt(row.ExecutionCandidate))
		if err != nil {
			return ResearchInferenceReport{}, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ResearchInferenceReport{}, false, err
	}
	report, err := s.ResearchInferenceRun(ctx, runID)
	if err == nil {
		report.TerminalOnlyMonitoring, report.PreregisteredFreezePresent = false, true
		report.InferenceTruth = "one-time sealed preregistered untouched UTC-day evaluation; frozen code/data/source/inference manifests, event disjointness, embargo, stopping time, and complete money truth were enforced"
	}
	return report, true, err
}

func (s *Store) SealPreregisteredUntouched(ctx context.Context,
	req PreregisteredUntouchedSealRequest) (ResearchInferenceReport, bool, error) {
	return s.sealPreregisteredUntouchedAt(ctx, req, time.Now().UTC())
}
