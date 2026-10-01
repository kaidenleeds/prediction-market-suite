package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR141DefaultResearchTabsUseOneDigestAndDetailsStayLazy(t *testing.T) {
	for _, want := range []string{
		`jget('/api/research/digest',8000)`,
		`loadResearchSystemDetails()`, `loadResearchExperimentDetails()`,
		`loadResearchEvidenceDetails()`, `loadResearchDataDetails()`,
		`loadResearchOperationsDetails()`,
		`Show every system collector and the complete Systems Leaderboard`,
		`Report refresh timed out; retry shortly.`, `_report_last_good_at`,
		`0 settled execution-backed rows`, `Not measured`,
		`profit_evidence`, `fill_conditioned`, `live_authorizes`,
		`🟡 Warming`, `🔴 Summary unavailable`, `Details on demand`,
		`r141ComponentPresentation`, `p.detail`,
		`cache:'no-store'`, `Retrying automatically.`, `lastResearchView`,
		`Read samples simply:`, `r142ComboSnapshotHTML`, `n never includes unresolved candidates`,
		`Show class-level uncertainty, manifest coverage, and route proof`,
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("R141 compact research UI missing %q", want)
		}
	}
	if strings.Contains(dashboardHTML, "signal is aborted without reason") {
		t.Fatal("raw browser AbortError text can still reach the UI")
	}
}

func TestR142DigestFreshnessAndRetryAreBounded(t *testing.T) {
	if researchDigestFreshTTL > time.Minute {
		t.Fatalf("compact digest freshness %s can leave operator cards stale", researchDigestFreshTTL)
	}
	if researchDigestRetryDelay > 30*time.Second {
		t.Fatalf("failed digest retry %s is too slow", researchDigestRetryDelay)
	}
	if researchDigestRetryDelay <= 0 || researchDigestBuildLimit <= 0 {
		t.Fatal("digest retry/build bounds must be positive")
	}
}

func TestR142DigestDistinguishesWarmingFailingAndRetrying(t *testing.T) {
	decode := func(s *Server) map[string]any {
		t.Helper()
		rec := httptest.NewRecorder()
		s.handleResearchDigest(rec, httptest.NewRequest(http.MethodGet, "/api/research/digest", nil))
		var got map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode digest: %v (%s)", err, rec.Body.String())
		}
		return got
	}

	warming := &Server{researchDigestBusy: true}
	if got := decode(warming); got["state"] != "WARMING" || got["warming"] != true {
		t.Fatalf("warming receipt = %+v", got)
	}
	warmingRec := httptest.NewRecorder()
	warming.handleResearchDigest(warmingRec, httptest.NewRequest(http.MethodGet, "/api/research/digest", nil))
	if got := warmingRec.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("warming digest cache control=%q, want no-store", got)
	}

	failing := &Server{researchDigestErr: "database busy", researchDigestNext: time.Now().Add(time.Minute)}
	if got := decode(failing); got["state"] != "FAILING" || got["warming"] != false {
		t.Fatalf("failing receipt = %+v", got)
	}

	retrying := &Server{researchDigestJSON: []byte(`{"state":"READY","systems":{"top":[]}}`),
		researchDigestAt: time.Now(), researchDigestNext: time.Now().Add(time.Minute), researchDigestErr: "database busy"}
	if got := decode(retrying); got["state"] != "READY" || got["refresh_state"] != "RETRYING" || got["refresh_warning"] != "database busy" {
		t.Fatalf("last-good retry receipt = %+v", got)
	}
}

func TestR142CompactCardsDoNotInventExactCountsOrUnknownSourceStates(t *testing.T) {
	experiments := dashboardHTML[strings.Index(dashboardHTML, "function loadResearchExperimentsPage()"):strings.Index(dashboardHTML, "function loadResearchExperimentDetails()")]
	if !strings.Contains(experiments, "Details on demand") || !strings.Contains(experiments, "Open validation details for exact outcome") {
		t.Fatalf("compact Validation card still implies an unqueried zero: %s", experiments)
	}
	data := dashboardHTML[strings.Index(dashboardHTML, "function loadResearchDataPage()"):strings.Index(dashboardHTML, "function loadResearchDataDetails()")]
	if strings.Contains(data, "state||'unknown'") {
		t.Fatal("compact Data card still replaces actual state/detail with generic unknown")
	}
	for _, want := range []string{"p.detail", "p.label", "p.dot", "polyView.detail"} {
		if !strings.Contains(data, want) {
			t.Fatalf("compact Data card missing actual source presentation %q", want)
		}
	}
}

func TestR141DigestHandlerReturnsLastGoodImmediately(t *testing.T) {
	s := &Server{}
	s.researchDigestJSON = []byte(`{"state":"READY","systems":{"top":[]}}`)
	s.researchDigestAt = time.Now()
	s.researchDigestNext = time.Now().Add(time.Minute)
	rec := httptest.NewRecorder()
	start := time.Now()
	s.handleResearchDigest(rec, httptest.NewRequest(http.MethodGet, "/api/research/digest", nil))
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("cached digest blocked for %s", elapsed)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode cached digest: %v", err)
	}
	if rec.Code != http.StatusOK || got["state"] != "READY" || got["refresh_state"] != "FRESH" {
		t.Fatalf("cached digest = %d %+v", rec.Code, got)
	}
}

func TestR144CompactBriefLeavesResearchDigestDetailOnDashboard(t *testing.T) {
	s := testServer(t)
	got := s.briefScoreboard(context.Background())
	for _, dashboardOnly := range []string{"🧪 progress:", "⚠ research:", "collector alerts", "compact collection summary"} {
		if strings.Contains(got, dashboardOnly) {
			t.Fatalf("phone briefing leaked dashboard research detail %q:\n%s", dashboardOnly, got)
		}
	}
	for _, required := range []string{"📊 Systems", "📈 Top 7 positive", "📉 Bottom 2 negative"} {
		if !strings.Contains(got, required) {
			t.Fatalf("compact decision surface missing %q:\n%s", required, got)
		}
	}
}

func TestR141DefaultDigestNeverLaunchesNativeCollectorReport(t *testing.T) {
	s := testServer(t)
	if err := s.store.Close(); err != nil {
		t.Fatal(err)
	}
	payload, _, _, _ := buildResearchDigest(context.Background(), s)
	if payload.Research.Registered != len(storage.ResearchExperimentIDs()) {
		t.Fatalf("failed report hid registered systems: got %d", payload.Research.Registered)
	}
	nativeWarnings := 0
	for _, warning := range payload.Warnings {
		if strings.HasPrefix(warning, "native collector summary:") {
			nativeWarnings++
		}
	}
	if nativeWarnings != 0 {
		t.Fatalf("default digest launched a heavy native report: %+v", payload.Warnings)
	}
}
