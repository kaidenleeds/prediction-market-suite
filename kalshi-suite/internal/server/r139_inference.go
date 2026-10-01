package server

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// monitorR139Inference is deliberately independent of the collector loops: a slow inference read
// cannot stall feeds, collector receipts, paper simulation, or venue liveness. The first pass waits
// briefly for boot migrations/collectors; subsequent passes are bounded and deduplicated by the
// immutable input manifest.
func (s *Server) monitorR139Inference(ctx context.Context) {
	// Core venue feeds and execution safety become ready first. The inference pass is research-only
	// and may scan the full immutable ledger, so starting it at boot+15s multiplied the same cold
	// SQLite/GC pressure as snapshot prewarm and wallet scoring.
	// Do not phase-align this full-ledger pass with 5s collectors or the old 30s portfolio clock.
	// Its bounded base remains 3 * time.Minute; the small coprime offset prevents phase collision.
	// The 17-second offset is deterministic, visible in tests, and avoids recreating the boot+3m
	// research stampede even before the shared heavy-work gate is considered.
	timer := time.NewTimer(3*time.Minute + 17*time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	run := func() bool {
		return s.tryRunHeavyResearch(ctx, "inference", 10*time.Second, func(ctx context.Context) {
			runCtx, cancel := context.WithTimeout(ctx, 75*time.Second)
			defer cancel()
			audit := func(level, message, detail string) {
				auditCtx, auditCancel := context.WithTimeout(ctx, 5*time.Second)
				defer auditCancel()
				_ = s.store.Audit(auditCtx, level, "research", message, detail)
			}
			report, inserted, err := s.store.RunR139ResearchInference(runCtx, time.Now())
			if err != nil {
				if s.log != nil {
					s.log.Warn("research inference pass failed", "err", err)
				}
				audit("error", "R139 deterministic inference pass failed", err.Error())
				return
			}
			if inserted {
				audit("info", "R139 deterministic inference receipt appended",
					"run="+strconv.FormatInt(report.RunID, 10)+" systems=19 exact_terminal="+strconv.Itoa(report.ExactTerminalRows)+" authority=0")
			}
		})
	}
	// A heavy job already in flight is a deferral, not a lost 30-minute inference cycle.
	for !run() {
		retry := time.NewTimer(17 * time.Second)
		select {
		case <-ctx.Done():
			retry.Stop()
			return
		case <-retry.C:
		}
	}
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for !run() {
				retry := time.NewTimer(17 * time.Second)
				select {
				case <-ctx.Done():
					retry.Stop()
					return
				case <-retry.C:
				}
			}
		}
	}
}

func (s *Server) handleResearchInference(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.LatestResearchInference(r.Context())
	if errors.Is(err, sql.ErrNoRows) {
		systems := make([]map[string]any, 0, 19)
		for _, id := range storage.ResearchExperimentIDs() {
			systems = append(systems, map[string]any{
				"system_id": id, "state": "SCHEDULER_STARTING", "reason": "waiting for the first bounded deterministic inference receipt",
				"terminal_rows": 0, "execution_candidate": false, "funded": false,
				"paper_authority": false, "live_authority": false,
				"monitoring_lower_bound_positive": false, "preregistered_untouched_gate_pass": false,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": "scheduler_starting", "pipeline_version": storage.R139InferencePipelineVersion,
			"systems_reported": 19, "systems": systems, "funded": false,
			"paper_authority": false, "live_authority": false,
			"terminal_only_monitoring": true, "preregistered_freeze_present": false,
			"money_truth": "the first run will monitor only exact terminal, canonical-event, complete-route observations; this is not a sealed untouched replication",
		})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}

type researchInferencePreregisterRequest struct {
	PreregistrationID       string  `json:"preregistration_id"`
	SystemID                string  `json:"system_id"`
	Cohort                  string  `json:"cohort"`
	Venue                   string  `json:"venue"`
	Route                   string  `json:"route"`
	TrainValidationCutoffTS string  `json:"train_validation_cutoff_ts"`
	UntouchedStartTS        string  `json:"untouched_start_ts"`
	UntouchedEndTS          string  `json:"untouched_end_ts"`
	SealAtTS                string  `json:"seal_at_ts"`
	EmbargoSeconds          float64 `json:"embargo_seconds"`
	RequiredDays            int     `json:"required_days"`
	RequiredEvents          int     `json:"required_events"`
	CodeManifestHash        string  `json:"code_manifest_hash"`
	DataManifestHash        string  `json:"data_manifest_hash"`
	SourceManifestHash      string  `json:"source_manifest_hash"`
}

func (s *Server) handleResearchInferencePreregister(w http.ResponseWriter, r *http.Request) {
	var req researchInferencePreregisterRequest
	if err := decodeBoundedJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded preregistration"})
		return
	}
	parse := func(raw string) (time.Time, error) { return time.Parse(time.RFC3339Nano, raw) }
	cutoff, err := parse(req.TrainValidationCutoffTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "train_validation_cutoff_ts must be RFC3339 UTC midnight"})
		return
	}
	start, err := parse(req.UntouchedStartTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "untouched_start_ts must be RFC3339 UTC midnight"})
		return
	}
	end, err := parse(req.UntouchedEndTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "untouched_end_ts must be RFC3339 UTC midnight"})
		return
	}
	seal, err := parse(req.SealAtTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "seal_at_ts must be RFC3339 UTC midnight"})
		return
	}
	p, inserted, err := s.store.RegisterResearchInferencePreregistration(r.Context(), storage.ResearchInferencePreregistration{
		PreregistrationID: req.PreregistrationID, SystemID: req.SystemID, Cohort: req.Cohort,
		Venue: req.Venue, Route: req.Route, TrainValidationCutoff: cutoff, UntouchedStart: start,
		UntouchedEnd: end, SealAt: seal, Embargo: time.Duration(req.EmbargoSeconds * float64(time.Second)),
		RequiredDays: req.RequiredDays, RequiredEvents: req.RequiredEvents,
		CodeManifestHash: req.CodeManifestHash, DataManifestHash: req.DataManifestHash,
		SourceManifestHash: req.SourceManifestHash,
	})
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "inserted": false,
			"funded": false, "paper_authority": false, "live_authority": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"preregistration": p, "inserted": inserted,
		"funded": false, "paper_authority": false, "live_authority": false})
}

type researchInferenceSealRequest struct {
	PreregistrationID     string `json:"preregistration_id"`
	CodeManifestHash      string `json:"code_manifest_hash"`
	DataManifestHash      string `json:"data_manifest_hash"`
	SourceManifestHash    string `json:"source_manifest_hash"`
	InferenceContractHash string `json:"inference_contract_hash"`
}

func (s *Server) handleResearchInferenceSeal(w http.ResponseWriter, r *http.Request) {
	var req researchInferenceSealRequest
	if err := decodeBoundedJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded untouched seal request"})
		return
	}
	report, inserted, err := s.store.SealPreregisteredUntouched(r.Context(), storage.PreregisteredUntouchedSealRequest{
		PreregistrationID: req.PreregistrationID, CodeManifestHash: req.CodeManifestHash,
		DataManifestHash: req.DataManifestHash, SourceManifestHash: req.SourceManifestHash,
		InferenceContractHash: req.InferenceContractHash,
	})
	if err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "sealed": false,
			"funded": false, "paper_authority": false, "live_authority": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sealed": inserted, "report": report,
		"funded": false, "paper_authority": false, "live_authority": false})
}
