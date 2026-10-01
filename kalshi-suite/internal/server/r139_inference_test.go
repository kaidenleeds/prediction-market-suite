package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestR139InferenceEndpointNeverHidesUnimplementedSystems(t *testing.T) {
	s := testServer(t)
	rec := httptest.NewRecorder()
	s.handleResearchInference(rec, httptest.NewRequest(http.MethodGet, "/api/research/inference", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("endpoint=%d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Status          string `json:"status"`
		SystemsReported int    `json:"systems_reported"`
		Funded          bool   `json:"funded"`
		PaperAuthority  bool   `json:"paper_authority"`
		LiveAuthority   bool   `json:"live_authority"`
		Systems         []struct {
			SystemID           string `json:"system_id"`
			State              string `json:"state"`
			ExecutionCandidate bool   `json:"execution_candidate"`
			PreregisteredGate  bool   `json:"preregistered_untouched_gate_pass"`
			Funded             bool   `json:"funded"`
			PaperAuthority     bool   `json:"paper_authority"`
			LiveAuthority      bool   `json:"live_authority"`
		} `json:"systems"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.SystemsReported != 19 || len(body.Systems) != 19 || body.Funded || body.PaperAuthority || body.LiveAuthority {
		t.Fatalf("coverage/authority=%+v", body)
	}
	seen := map[string]bool{}
	for _, row := range body.Systems {
		if row.SystemID == "" || row.State == "" || seen[row.SystemID] || row.ExecutionCandidate || row.PreregisteredGate ||
			row.Funded || row.PaperAuthority || row.LiveAuthority {
			t.Fatalf("invalid system row: %+v", row)
		}
		seen[row.SystemID] = true
	}

	if _, _, err := s.store.RunR139ResearchInference(context.Background(),
		time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	rec = httptest.NewRecorder()
	s.handleResearchInference(rec, httptest.NewRequest(http.MethodGet, "/api/research/inference", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"pipeline_version":4`) ||
		!strings.Contains(rec.Body.String(), `"systems_reported":19`) ||
		!strings.Contains(rec.Body.String(), `"execution_candidate":false`) {
		t.Fatalf("stored endpoint=%d %s", rec.Code, rec.Body.String())
	}
}

func TestR139InferenceHasIndependentBoundedSchedulerAndGETRoute(t *testing.T) {
	source, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, want := range []string{
		`mux.HandleFunc("GET /api/research/inference", s.handleResearchInference)`,
		`go s.monitorR139Inference(ctx)`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing inference route/scheduler %q", want)
		}
	}
	worker, err := os.ReadFile("r139_inference.go")
	if err != nil {
		t.Fatal(err)
	}
	workerText := string(worker)
	for _, want := range []string{"context.WithTimeout(ctx, 75*time.Second)", "30 * time.Minute", "3 * time.Minute"} {
		if !strings.Contains(workerText, want) {
			t.Fatalf("inference scheduler is not bounded: missing %q", want)
		}
	}
	for _, want := range []string{"/api/research/inference", "Strict validation-subset monitoring",
		"not untouched proof", "one-day purge/embargo", "Holm, and BY",
		"0 preregistered untouched passes", "Exact scalar void/refund"} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("Systems/Experiments inference explanation missing %q", want)
		}
	}
}
