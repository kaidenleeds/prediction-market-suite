package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchdr"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func (s *Server) handleResearchRoutes(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.ResearchRouteReport(r.Context(), 100)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	bundles, legs, states, grades, bundleErr := s.store.ResearchRouteBundleCounts(r.Context())
	promotion, promotionErr := s.store.ResearchBundlePromotionStatus(r.Context())
	if bundleErr != nil || promotionErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "typed multi-leg route report unavailable"})
		return
	}
	report["typed_multi_leg"] = map[string]any{"bundles": bundles, "ordered_legs": legs,
		"payoff_states": states, "counterfactual_grades": grades, "hard_max_legs": 6,
		"promotion": promotion, "actual_fill_claimed": false,
		"immutable_research_rows_have_authority": false,
		"legacy_sealed_candidate_count":          promotion.SealedEligible,
		"legacy_combo_equivalent_count":          promotion.ComboEquivalentBundles,
		"legacy_paper_accepted_count":            promotion.PaperAccepted,
		"exact_conjunction_paper_bridge_ready":   false,
		"armed_live_bridge_ready":                false,
		"bridge_status":                          "R165 retired: historical research/Paper counters cannot authorize Paper or LIVE",
		"additive_bundle_execution":              "blocked: no venue-native atomic additive portfolio route; separately filled legs retain partial-fill/unwind risk and cannot claim Paper/LIVE parity"}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) handleResearchGovernance(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.ResearchGovernanceReport(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	s.liveMu.Lock()
	armed, auto := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	report["runtime"] = map[string]any{
		"live_armed": armed, "live_auto": auto, "kill_switch_blocked": s.ksBlocked(),
		"arm_auto_invariant_ok": !auto || armed,
		"note":                  "runtime state is descriptive only; research authority remains zero",
	}
	if auto && !armed {
		report["compliant_for_research"] = false
	}
	writeJSON(w, http.StatusOK, report)
}

func decodeBoundedJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 256<<10)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// handleResearchEVI deliberately retires the unversioned calculator endpoint. Accepting ad-hoc
// economic assumptions here would bypass immutable preregistration even though it had no order
// authority. The pure math remains unit-tested in researchportfolio; production scheduling uses
// only POST /api/research/evi/inputs plus the deterministic loop.
func (s *Server) handleResearchEVI(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":    "unversioned EVI calculations are retired; preregister an immutable input version at POST /api/research/evi/inputs and read GET /api/research/portfolio",
		"executed": false, "research_authority": false, "automatic_collection_changes": false,
		"automatic_budget_changes": false,
	})
}

// handleResearchCauseGraph retires the ad-hoc point-estimate path. The production graph can only
// consume immutable untouched-replication executable fee-net route lower bounds.
func (s *Server) handleResearchCauseGraph(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":              "ad-hoc cause graphs are retired; preregister an untouched-replication route lower bound at POST /api/research/cause-inputs and read GET /api/research/portfolio",
		"research_authority": false, "automatic_allocation_changes": false, "order_authority": false,
	})
}

type frozenEVIInputRequest struct {
	TaskID                     string   `json:"task_id"`
	InputState                 string   `json:"input_state"`
	SystemID                   string   `json:"system_id"`
	RouteID                    string   `json:"route_id"`
	CauseIDs                   []string `json:"cause_ids"`
	Provenance                 string   `json:"provenance"`
	EvidenceHash               string   `json:"evidence_hash"`
	SealedInferenceRunID       int64    `json:"sealed_inference_run_id"`
	EvidenceObservedTS         string   `json:"evidence_observed_ts"`
	ValidUntilTS               string   `json:"valid_until_ts"`
	ProbabilityChangesDecision float64  `json:"probability_changes_decision"`
	DecisionValueDollarsPerDay float64  `json:"decision_value_dollars_per_day"`
	CollectionCostDollars      float64  `json:"collection_cost_dollars"`
	ComputeMinutes             float64  `json:"compute_minutes"`
	APICalls                   float64  `json:"api_calls"`
	ComputeBudget              float64  `json:"compute_budget"`
	APIBudget                  float64  `json:"api_budget"`
	ComputeDollarPerMinute     float64  `json:"compute_dollar_per_minute"`
	APIDollarPerCall           float64  `json:"api_dollar_per_call"`
	Version                    int      `json:"version"`
	Funded                     bool     `json:"funded"`
	PaperAuthority             bool     `json:"paper_authority"`
	LiveAuthority              bool     `json:"live_authority"`
}

func parseFrozenTime(v string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, strings.TrimSpace(v))
}

// handleFrozenEVIInputs registers or lists append-only task assumptions. Registration is an
// evidence/governance action only; the subsequent tick appends a schedule receipt but cannot start
// a collector or change the frozen budgets.
func (s *Server) handleFrozenEVIInputs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		rows, err := s.store.CurrentFrozenEVIInputs(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"inputs": rows, "count": len(rows),
			"funded": false, "paper_authority": false, "live_authority": false,
			"contract": "latest immutable version per task; row counts never supply missing economic assumptions"})
		return
	}
	var req frozenEVIInputRequest
	if err := decodeBoundedJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded frozen EVI input"})
		return
	}
	observed, err := parseFrozenTime(req.EvidenceObservedTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "evidence_observed_ts must be RFC3339"})
		return
	}
	until, err := parseFrozenTime(req.ValidUntilTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid_until_ts must be RFC3339"})
		return
	}
	in, inserted, err := s.store.RegisterFrozenEVIInput(r.Context(), storage.FrozenEVIInput{
		TaskID: req.TaskID, InputState: req.InputState, SystemID: req.SystemID, RouteID: req.RouteID,
		CauseIDs: req.CauseIDs, Provenance: req.Provenance, EvidenceHash: req.EvidenceHash,
		SealedInferenceRunID: req.SealedInferenceRunID,
		EvidenceObserved:     observed, ValidUntil: until, ProbabilityChangesDecision: req.ProbabilityChangesDecision,
		DecisionValueDollarsPerDay: req.DecisionValueDollarsPerDay, CollectionCostDollars: req.CollectionCostDollars,
		ComputeMinutes: req.ComputeMinutes, APICalls: req.APICalls, ComputeBudget: req.ComputeBudget,
		APIBudget: req.APIBudget, ComputeDollarPerMinute: req.ComputeDollarPerMinute,
		APIDollarPerCall: req.APIDollarPerCall, Version: req.Version, Funded: req.Funded,
		PaperAuthority: req.PaperAuthority, LiveAuthority: req.LiveAuthority})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tickCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
	s.runResearchPortfolioTick(tickCtx, 10*time.Second)
	cancel()
	writeJSON(w, http.StatusOK, map[string]any{"input": in, "inserted": inserted,
		"funded": false, "paper_authority": false, "live_authority": false, "executed": false})
}

type frozenCauseInputRequest struct {
	ExposureID                    string   `json:"exposure_id"`
	InputState                    string   `json:"input_state"`
	SystemID                      string   `json:"system_id"`
	RouteID                       string   `json:"route_id"`
	Venue                         string   `json:"venue"`
	CauseIDs                      []string `json:"cause_ids"`
	Provenance                    string   `json:"provenance"`
	EvidenceHash                  string   `json:"evidence_hash"`
	SealedInferenceRunID          int64    `json:"sealed_inference_run_id"`
	SealedRouteEconomicsReceiptID string   `json:"sealed_route_economics_receipt_id"`
	EvidenceObservedTS            string   `json:"evidence_observed_ts"`
	ValidUntilTS                  string   `json:"valid_until_ts"`
	ReplicationID                 string   `json:"replication_id"`
	LowerBoundMethod              string   `json:"lower_bound_method"`
	ReplicatedUntouched           bool     `json:"replicated_untouched"`
	ExecutableFeeNetLowerBound    bool     `json:"executable_fee_net_lower_bound"`
	NetPerDayLower                float64  `json:"net_per_day_lower"`
	Capacity                      float64  `json:"capacity"`
	CapitalDollarHoursPerDay      float64  `json:"capital_dollar_hours_per_day"`
	Version                       int      `json:"version"`
	Funded                        bool     `json:"funded"`
	PaperAuthority                bool     `json:"paper_authority"`
	LiveAuthority                 bool     `json:"live_authority"`
}

func (s *Server) handleFrozenCauseInputs(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		rows, err := s.store.CurrentFrozenCauseExposures(r.Context())
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"inputs": rows, "count": len(rows),
			"funded": false, "paper_authority": false, "live_authority": false,
			"contract": "latest immutable version per exposure; only untouched-replication executable fee-net route lower bounds qualify"})
		return
	}
	var req frozenCauseInputRequest
	if err := decodeBoundedJSON(w, r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid bounded frozen cause input"})
		return
	}
	observed, err := parseFrozenTime(req.EvidenceObservedTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "evidence_observed_ts must be RFC3339"})
		return
	}
	until, err := parseFrozenTime(req.ValidUntilTS)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid_until_ts must be RFC3339"})
		return
	}
	in, inserted, err := s.store.RegisterFrozenCauseExposure(r.Context(), storage.FrozenCauseExposure{
		ExposureID: req.ExposureID, InputState: req.InputState, SystemID: req.SystemID, RouteID: req.RouteID,
		Venue: req.Venue, CauseIDs: req.CauseIDs, Provenance: req.Provenance, EvidenceHash: req.EvidenceHash,
		SealedInferenceRunID:          req.SealedInferenceRunID,
		SealedRouteEconomicsReceiptID: req.SealedRouteEconomicsReceiptID,
		EvidenceObserved:              observed, ValidUntil: until, ReplicationID: req.ReplicationID,
		LowerBoundMethod: req.LowerBoundMethod, ReplicatedUntouched: req.ReplicatedUntouched,
		ExecutableFeeNetLowerBound: req.ExecutableFeeNetLowerBound, NetPerDayLower: req.NetPerDayLower,
		Capacity: req.Capacity, CapitalDollarHoursPerDay: req.CapitalDollarHoursPerDay,
		Version: req.Version, Funded: req.Funded, PaperAuthority: req.PaperAuthority,
		LiveAuthority: req.LiveAuthority})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	tickCtx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 15*time.Second)
	s.runResearchPortfolioTick(tickCtx, 10*time.Second)
	cancel()
	writeJSON(w, http.StatusOK, map[string]any{"input": in, "inserted": inserted,
		"funded": false, "paper_authority": false, "live_authority": false, "executed": false})
}

// handleResearchPortfolio exposes integration readiness without fabricating EVI inputs or a cause
// graph from point estimates. The POST calculators above become meaningful only after frozen task
// values and replicated route lower bounds exist.
func (s *Server) handleResearchPortfolio(w http.ResponseWriter, r *http.Request) {
	routes, err := s.store.ResearchRouteReport(r.Context(), 20)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	eviInputs, err := s.store.CurrentFrozenEVIInputs(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	causeInputs, err := s.store.CurrentFrozenCauseExposures(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	eviRun, hasEVI, err := s.store.LatestEVIScheduleRun(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	causeRun, hasCause, err := s.store.LatestCauseGraphRun(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	now := time.Now().UTC()
	_, eviActive, eviValid := storage.FrozenEVIManifestEntries(eviInputs, now)
	_, causeActive, causeValid := storage.FrozenCauseManifestEntries(causeInputs, now)
	eviState, eviReason := "WAITING_FOR_FROZEN_INPUTS", "no sealed preregistered untouched inference result exists; rolling monitoring and row count cannot qualify an input"
	if hasEVI {
		eviState, eviReason = eviRun.State, eviRun.Reason
	} else if eviActive > 0 && eviValid != eviActive {
		eviState, eviReason = "BLOCKED_STALE_OR_INCONSISTENT_INPUTS", "one or more active immutable task inputs is stale or future-dated"
	} else if eviActive > 0 {
		eviState, eviReason = "FROZEN_INPUTS_PENDING_RECEIPT", "fresh immutable task inputs are waiting for the deterministic scheduler loop"
	}
	causeState, causeReason := "BLOCKED_NO_REPLICATED_ROUTE_LOWER_BOUNDS", "no sealed preregistered inference gate plus immutable route Net/day/capacity/capital-time conversion receipt exists"
	if hasCause {
		causeState, causeReason = causeRun.State, causeRun.Reason
	} else if causeActive > 0 && causeValid != causeActive {
		causeState, causeReason = "BLOCKED_STALE_REPLICATED_ROUTE_LOWER_BOUNDS", "one or more active replicated route lower bounds is stale or future-dated"
	} else if causeActive > 0 {
		causeState, causeReason = "FROZEN_INPUTS_PENDING_RECEIPT", "fresh replicated route lower bounds are waiting for the deterministic cause-graph loop"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"evi_scheduler": map[string]any{"state": eviState, "endpoint": "POST /api/research/evi/inputs",
			"automatic_collection_changes": false,
			"automatic_budget_changes":     false, "order_authority": false,
			"plain_language": "The queue runs only from signed-off, versioned assumptions. Empty means waiting for those inputs, not a broken collector.",
			"reason":         eviReason, "current_input_versions": eviActive, "fresh_input_versions": eviValid,
			"latest_receipt": portfolioRunView(eviRun, hasEVI),
			"required_inputs": []string{"task id", "probability the result changes a decision", "economic value if it changes the decision",
				"collection cost", "compute minutes", "API calls", "compute and API budgets", "sealed preregistered inference run + exact result hash", "provenance", "system, route, and cause IDs", "valid-until time"}},
		"cause_graph": map[string]any{"state": causeState, "endpoint": "POST /api/research/cause-inputs",
			"reason": causeReason, "current_input_versions": causeActive, "fresh_input_versions": causeValid,
			"latest_receipt":          portfolioRunView(causeRun, hasCause),
			"required_inputs":         []string{"sealed preregistered inference run + exact result hash", "immutable route economics conversion receipt", "executable fee-net Net/day lower bound", "capacity", "capital time", "system, route, and cause IDs", "provenance", "valid-until time"},
			"point_estimates_allowed": false, "automatic_allocation_changes": false, "order_authority": false},
		"route_ledger": routes, "allocation_authority": false,
		// Compatibility alias retained for stored/API consumers from R139.
		"mode4_authority": false,
		"funded":          false, "paper_authority": false, "live_authority": false,
		"rank_order": "positive executable edge lower bound -> conservative Net/day -> capacity -> capital time -> shared-cause diversification -> Adaptive Allocation Model sizing",
	})
}

func (s *Server) recoveryManager() (*researchdr.Manager, error) {
	return researchdr.New(s.cfg().DataDir, s.store, 3)
}

func (s *Server) handleResearchRecovery(w http.ResponseWriter, r *http.Request) {
	mgr, err := s.recoveryManager()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if name := strings.TrimSpace(r.URL.Query().Get("plan")); name != "" {
		writeJSON(w, http.StatusOK, mgr.PlanRestore(name))
		return
	}
	rows, err := mgr.List()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshots": rows, "automatic_restore": false,
		"contract": "verified artifacts only; restore is an offline operator drill with the suite stopped",
	})
}

func (s *Server) handleResearchRecoverySnapshot(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Confirm string `json:"confirm"`
	}
	if err := decodeBoundedJSON(w, r, &body); err != nil || body.Confirm != "create-verified-research-snapshot" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "explicit recovery snapshot confirmation required"})
		return
	}
	// The main database is multi-gigabyte. VACUUM/hash/quick_check from an HTTP handler competed
	// with trading and research writers and could pin a large WAL. Snapshot creation is therefore
	// an offline CLI operation only; this endpoint remains as a deliberate, testable refusal so an
	// old UI or automation cannot reintroduce online I/O starvation.
	_, _ = s.store.AppendResearchSecurityEvent(context.WithoutCancel(r.Context()), storage.ResearchSecurityEvent{
		Category: "recovery", Severity: "blocked", Message: "online recovery snapshot refused; suite must be stopped",
		EvidenceJSON: `{"live_database_replaced":false,"offline_only":true}`,
	})
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":        "online recovery snapshots are disabled; stop the suite and run kalshi-suite.exe research-snapshot",
		"offline_only": true, "live_database_replaced": false,
	})
}
