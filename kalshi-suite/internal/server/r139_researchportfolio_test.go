package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r139EVIInput(now time.Time) storage.FrozenEVIInput {
	return storage.FrozenEVIInput{TaskID: "repair-funnel", Version: 1, InputState: "active",
		SystemID: "flow-direction-integrity", RouteID: "research:collector", CauseIDs: []string{"feed", "flow"},
		Provenance: "R139 preregistered operator research decision", EvidenceHash: strings.Repeat("c", 64),
		EvidenceObserved: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour),
		ProbabilityChangesDecision: .8, DecisionValueDollarsPerDay: 20, CollectionCostDollars: 1,
		ComputeMinutes: 5, APICalls: 10, ComputeBudget: 10, APIBudget: 20,
		ComputeDollarPerMinute: .01, APIDollarPerCall: .001}
}

func TestR139PersistedEVISchedulerNeverInfersFromRowCountOrMonitoringClaims(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		_ = s.store.Audit(ctx, "info", "research", "unrelated rows cannot become EVI assumptions", "")
	}
	now := time.Now().UTC()
	s.runFrozenEVISchedule(ctx, now)
	run, exists, err := s.store.LatestEVIScheduleRun(ctx)
	if err != nil || !exists || run.State != "WAITING_FOR_FROZEN_INPUTS" || run.InputCount != 0 {
		t.Fatalf("row count became an EVI input: run=%+v exists=%v err=%v", run, exists, err)
	}
	claim := r139EVIInput(now)
	claim.SealedInferenceRunID = 999
	if _, _, err := s.store.RegisterFrozenEVIInput(ctx, claim); err == nil || !strings.Contains(err.Error(), "no immutable inference result") {
		t.Fatalf("unlinked self-asserted evidence entered EVI scheduler: %v", err)
	}
	if err := s.runFrozenEVISchedule(ctx, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	run, exists, err = s.store.LatestEVIScheduleRun(ctx)
	if err != nil || !exists || run.State != "WAITING_FOR_FROZEN_INPUTS" || run.InputCount != 0 {
		t.Fatalf("rejected self-claim changed scheduler=%+v exists=%v err=%v", run, exists, err)
	}
}

func TestR139PersistedCauseGraphRequiresReplicatedExecutableLowerBound(t *testing.T) {
	s := testServer(t)
	ctx, now := context.Background(), time.Now().UTC()
	if err := s.runFrozenCauseGraph(ctx, now); err != nil {
		t.Fatal(err)
	}
	run, exists, err := s.store.LatestCauseGraphRun(ctx)
	if err != nil || !exists || run.State != "BLOCKED_NO_REPLICATED_ROUTE_LOWER_BOUNDS" {
		t.Fatalf("empty cause graph=%+v exists=%v err=%v", run, exists, err)
	}
	bad := storage.FrozenCauseExposure{ExposureID: "point", Version: 1, InputState: "active",
		SystemID: "flow-direction-integrity", RouteID: "kalshi:maker", Venue: "kalshi",
		CauseIDs: []string{"flow"}, Provenance: "validation point estimate", EvidenceHash: strings.Repeat("d", 64),
		EvidenceObserved: now.Add(-time.Minute), ValidUntil: now.Add(time.Hour), ReplicationID: "validation",
		LowerBoundMethod: "mean", NetPerDayLower: 10, Capacity: 20}
	if _, _, err := s.store.RegisterFrozenCauseExposure(ctx, bad); err == nil {
		t.Fatal("unreplicated point estimate entered cause graph")
	}
	good := bad
	good.ExposureID, good.ReplicationID = "replicated", "untouched-holdout-1"
	good.LowerBoundMethod = "clustered one-sided 95% executable fee-net Net/day lower bound"
	good.ReplicatedUntouched, good.ExecutableFeeNetLowerBound = true, true
	good.SealedInferenceRunID, good.SealedRouteEconomicsReceiptID = 999, "self-asserted-route"
	if _, _, err := s.store.RegisterFrozenCauseExposure(ctx, good); err == nil || !strings.Contains(err.Error(), "no immutable inference result") {
		t.Fatalf("unsealed self-asserted cause entered graph: %v", err)
	}
	if err := s.runFrozenCauseGraph(ctx, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	run, _, _ = s.store.LatestCauseGraphRun(ctx)
	if run.State != "BLOCKED_NO_REPLICATED_ROUTE_LOWER_BOUNDS" || run.ValidInputCount != 0 {
		t.Fatalf("unqualified cause changed graph=%+v", run)
	}
}

func TestR139FrozenInputAPIRejectsAuthorityAndSurfacesZeroAuthority(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	body := map[string]any{
		"task_id": "authority-attempt", "version": 1, "input_state": "active",
		"system_id": "flow-direction-integrity", "route_id": "research:collector", "cause_ids": []string{"flow"},
		"provenance": "test", "evidence_hash": strings.Repeat("e", 64),
		"sealed_inference_run_id":      999,
		"evidence_observed_ts":         now.Add(-time.Minute).Format(time.RFC3339Nano),
		"valid_until_ts":               now.Add(time.Hour).Format(time.RFC3339Nano),
		"probability_changes_decision": .5, "decision_value_dollars_per_day": 1,
		"collection_cost_dollars": 0, "compute_minutes": 1, "api_calls": 1,
		"compute_budget": 1, "api_budget": 1, "compute_dollar_per_minute": 0,
		"api_dollar_per_call": 0, "live_authority": true,
	}
	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	s.handleFrozenEVIInputs(rec, httptest.NewRequest(http.MethodPost, "/api/research/evi/inputs", bytes.NewReader(b)))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "authority") {
		t.Fatalf("authority registration response=%d %s", rec.Code, rec.Body.String())
	}
	body["live_authority"] = false
	b, _ = json.Marshal(body)
	rec = httptest.NewRecorder()
	s.handleFrozenEVIInputs(rec, httptest.NewRequest(http.MethodPost, "/api/research/evi/inputs", bytes.NewReader(b)))
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "no immutable inference result") {
		t.Fatalf("unsealed registration response=%d %s", rec.Code, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	s.handleResearchPortfolio(rec, httptest.NewRequest(http.MethodGet, "/api/research/portfolio", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"automatic_budget_changes":false`) ||
		!strings.Contains(rec.Body.String(), `"order_authority":false`) ||
		!strings.Contains(rec.Body.String(), `"point_estimates_allowed":false`) {
		t.Fatalf("portfolio authority surface=%d %s", rec.Code, rec.Body.String())
	}
}
