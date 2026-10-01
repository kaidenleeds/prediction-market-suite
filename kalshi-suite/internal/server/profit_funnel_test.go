package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestProfitFunnelEndpointFiltersGroupsAndIgnoresDetailLimit(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	base := time.Date(2026, 7, 22, 14, 0, 0, 0, time.UTC)
	for i, fixture := range []struct{ system, side string }{
		{"spotlag", "YES"}, {"spotlag", "NO"}, {"other", "YES"},
	} {
		at := base.Add(time.Duration(i) * time.Second)
		attempt := storage.ExecutionShadowAttempt{
			AttemptID: fmt.Sprintf("endpoint-%d", i), SignalDecisionID: fmt.Sprintf("decision-%d", i),
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: "kalshi",
			Ticker: fmt.Sprintf("KX-ENDPOINT-%d", i), Side: fixture.side, Action: "BUY",
			SystemID: fixture.system, Route: "taker", SignalSource: "endpoint-test",
			InputTopology: "coinbase->kalshi", SignalPrice: .40,
			QualificationBasis: "endpoint-test",
		}
		if _, err := st.InsertExecutionShadowAttempt(context.Background(), attempt); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			// A real but ambiguous terminal must be visible as an unknown LIVE terminal and in
			// the unknown disposition cohort. The second matching fixture has no terminal at all.
			event := storage.ExecutionShadowEvent{
				EventID: "endpoint-ambiguous", AttemptID: attempt.AttemptID,
				At: at.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
				Stage: "live-terminal", Outcome: "ambiguous", VenueAttempted: true,
				LiveState: "ambiguous",
			}
			if inserted, err := st.AppendExecutionShadowEvent(context.Background(), event); err != nil || !inserted {
				t.Fatalf("append ambiguous terminal inserted=%v err=%v", inserted, err)
			}
		}
	}

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet,
		"/api/profit-funnel?limit=1&system=SPOTLAG&group_by=side", nil)
	s.handleProfitFunnel(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		SchemaVersion string                      `json:"schema_version"`
		FullHistory   bool                        `json:"full_history"`
		DetailLimited bool                        `json:"detail_limited"`
		Totals        storage.ProfitFunnelCounts  `json:"totals"`
		Groups        []storage.ProfitFunnelGroup `json:"groups"`
		Telemetry     map[string]any              `json:"telemetry"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.SchemaVersion != storage.ProfitFunnelSchemaVersion || !body.FullHistory ||
		body.DetailLimited || body.Totals.Detected != 2 || body.Totals.Live.Unknown != 1 ||
		body.Totals.TakenVsSkipped.Unknown.Total != 2 ||
		body.Totals.Gaps.LiveTerminalMissing != 1 ||
		body.Totals.Gaps.PaperBranchMissing != 2 ||
		body.Totals.Gaps.ShadowBranchMissing != 2 ||
		body.Totals.Exclusivity.LiveClassified != 1 ||
		body.Totals.Exclusivity.LiveGap != 1 || len(body.Groups) != 2 ||
		body.Telemetry["storage"] == nil {
		t.Fatalf("endpoint did not return uncapped filtered aggregate: %+v", body)
	}

	bad := httptest.NewRecorder()
	s.handleProfitFunnel(bad, httptest.NewRequest(http.MethodGet,
		"/api/profit-funnel?from=not-a-time", nil))
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("invalid time status=%d body=%s", bad.Code, bad.Body.String())
	}
}
