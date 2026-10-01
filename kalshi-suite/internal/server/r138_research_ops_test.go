package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestR138ResearchFirstDashboardNineDestinations(t *testing.T) {
	for _, id := range []string{"overview", "systems", "experiments", "evidence", "data", "markets", "paper", "live", "operations"} {
		if !strings.Contains(dashboardHTML, `id="tab_`+id+`"`) {
			t.Fatalf("research-first tab %q missing", id)
		}
	}
	for _, truth := range []string{
		"30-day lower bound is the only promotion-review lane", "ZERO AUTHORITY", "untouched event/day", "Maker and taker are different experiments",
		"restore has no automatic or HTTP action", "No unified routes yet; missing rows are not proxied from signal price.",
		"/api/research/system-evidence", "registry alone never counts as implemented", "COLLECTING_PARTIAL",
		"Registry rows are not operational.", "No immutable system registry rows exist. Nothing is operational.",
		"No collector receipts exist. This is missing instrumentation, not a healthy zero.",
	} {
		if !strings.Contains(dashboardHTML, truth) {
			t.Fatalf("research dashboard money truth missing %q", truth)
		}
	}
	if strings.Contains(dashboardHTML, `id="buycard"`) || strings.Contains(dashboardHTML, `/api/paper/order`) {
		t.Fatal("manual paper placement resurfaced")
	}
	if strings.Contains(dashboardHTML, "createRecoverySnapshot(") {
		t.Fatal("dashboard can invoke the heavy recovery snapshot endpoint")
	}
}

func TestR138SystemEvidenceEndpointReportsAllRuntimeReasonsWithoutAuthority(t *testing.T) {
	s := testServer(t)
	s.sweepR138SystemCoverage(context.Background())
	rec := httptest.NewRecorder()
	s.handleR138SystemEvidence(rec, httptest.NewRequest(http.MethodGet, "/api/research/system-evidence", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("system evidence = %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Funded          bool           `json:"funded"`
		PaperAuthority  bool           `json:"paper_authority"`
		LiveAuthority   bool           `json:"live_authority"`
		RuntimeCoverage map[string]any `json:"runtime_coverage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Funded || body.PaperAuthority || body.LiveAuthority {
		t.Fatalf("system evidence granted authority: %+v", body)
	}
	if int(body.RuntimeCoverage["expected_systems"].(float64)) != 19 || int(body.RuntimeCoverage["reported_systems"].(float64)) != 19 ||
		body.RuntimeCoverage["registry_alone_counts_as_implementation"] != false {
		t.Fatalf("runtime coverage=%+v", body.RuntimeCoverage)
	}
	rows, ok := body.RuntimeCoverage["systems"].([]any)
	if !ok || len(rows) != 19 {
		t.Fatalf("runtime rows=%T %#v", body.RuntimeCoverage["systems"], body.RuntimeCoverage["systems"])
	}
	collecting, blocked, blockedWithPrereq := 0, 0, false
	for _, raw := range rows {
		row := raw.(map[string]any)
		state, reason := row["State"], row["Reason"]
		if reason == "" || reason == nil {
			t.Fatalf("runtime row lacks reason: %+v", row)
		}
		if state == "COLLECTING_PARTIAL" || state == "COLLECTING" {
			collecting++
		}
		if state == "BLOCKED" {
			blocked++
			if p, ok := row["Prerequisites"].([]any); ok && len(p) > 0 {
				blockedWithPrereq = true
			}
		}
	}
	if collecting != 0 || blocked != 19 || !blockedWithPrereq {
		t.Fatalf("empty runtime must fail closed: collecting=%d blocked=%d blocked_prereq=%v", collecting, blocked, blockedWithPrereq)
	}
}

func TestR139UnversionedEVISchedulerAndCauseGraphInputsAreRetired(t *testing.T) {
	s := testServer(t)
	body := `{"tasks":[{"ID":"repair","ProbabilityChangesDecision":0.8,"DecisionValueDollarsPerDay":10,"ComputeMinutes":2,"APICalls":3}],"compute_budget":5,"api_budget":5,"compute_dollar_per_minute":0.1,"api_dollar_per_call":0.01}`
	rec := httptest.NewRecorder()
	s.handleResearchEVI(rec, httptest.NewRequest(http.MethodPost, "/api/research/evi", strings.NewReader(body)))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "/api/research/evi/inputs") {
		t.Fatalf("EVI = %d %s", rec.Code, rec.Body.String())
	}
	var evi map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &evi); err != nil || evi["executed"] != false || evi["research_authority"] != false || evi["automatic_budget_changes"] != false {
		t.Fatalf("EVI response=%v err=%v", evi, err)
	}
	body = `{"rows":[{"SystemID":"a","RouteID":"maker","Venue":"kalshi","CauseIDs":["event:1"],"NetPerDayLower":2,"Capacity":3},{"SystemID":"b","RouteID":"taker","Venue":"polyus","CauseIDs":["event:1"],"NetPerDayLower":1,"Capacity":2}]}`
	rec = httptest.NewRecorder()
	s.handleResearchCauseGraph(rec, httptest.NewRequest(http.MethodPost, "/api/research/cause-graph", strings.NewReader(body)))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "/api/research/cause-inputs") {
		t.Fatalf("cause graph = %d %s", rec.Code, rec.Body.String())
	}
	var cause map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &cause); err != nil || cause["research_authority"] != false || cause["order_authority"] != false {
		t.Fatalf("cause response=%v err=%v", cause, err)
	}
}

func TestR138RecoverySnapshotEndpointIsOfflineOnly(t *testing.T) {
	s := testServer(t)
	body := `{"confirm":"create-verified-research-snapshot"}`
	rec := httptest.NewRecorder()
	s.handleResearchRecoverySnapshot(rec, httptest.NewRequest(http.MethodPost, "/api/research/recovery/snapshot", strings.NewReader(body)))
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "offline") ||
		!strings.Contains(rec.Body.String(), "research-snapshot") {
		t.Fatalf("online snapshot refusal = %d %s", rec.Code, rec.Body.String())
	}
}
