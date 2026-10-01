package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func sevenParlayLegs() []paper.Leg {
	legs := make([]paper.Leg, parlayLegLimit+1)
	for i := range legs {
		legs[i] = paper.Leg{Platform: "kalshi", Ticker: "KX-SEVEN-" + string(rune('A'+i)), Side: "YES", Entry: .5}
	}
	return legs
}

func TestR139SystemsLeaderboardNeverDropsRegisteredOrNativeSystems(t *testing.T) {
	s := testServer(t)
	s.verdMu.Lock()
	s.verdCache = []verdictEnt{
		{Family: "whale-flow-test", Group: "signal", N: 12, State: "COLLECTING"},
		{Family: "whale-flow-test", Group: "maker", N: 4, State: "COLLECTING"},
	}
	s.verdAt = time.Now()
	s.verdMu.Unlock()
	rec := httptest.NewRecorder()
	s.handleLeaderboardBacktest(rec, httptest.NewRequest(http.MethodGet, "/api/systems-leaderboard", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("leaderboard=%d %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Rows []struct {
			Family string `json:"family"`
		} `json:"rows"`
		Coverage struct {
			Components []leaderboardCoverageRow `json:"components"`
		} `json:"coverage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	seenRows, seenCoverage := map[string]bool{}, map[string]bool{}
	for _, row := range out.Rows {
		seenRows[row.Family] = true
	}
	for _, row := range out.Coverage.Components {
		seenCoverage[row.Family] = true
	}
	want := append([]string(nil), storage.SystemIDs()...)
	for _, row := range researchSystemCoverageRows() {
		want = append(want, row.Family)
	}
	for _, id := range append(want, "whale-flow-test") {
		if !seenCoverage[id] || !seenRows[id] {
			t.Fatalf("system %q missing: coverage=%v row=%v", id, seenCoverage[id], seenRows[id])
		}
	}
	coverageBySystem := map[string][]leaderboardCoverageRow{}
	for _, row := range out.Coverage.Components {
		coverageBySystem[row.Family] = append(coverageBySystem[row.Family], row)
	}
	for _, spec := range storage.SystemExecutionSpecs() {
		cells := coverageBySystem[spec.SystemID]
		wantCells := len(spec.ExactVariants())
		if wantCells == 0 {
			wantCells = 1
		}
		if len(cells) != wantCells {
			t.Fatalf("System %s has %d coverage cells, want %d", spec.SystemID, len(cells), wantCells)
		}
		adapter := storage.SystemExecutionAdapterStatus(spec)
		for _, row := range cells {
			wantLiveHandoff := ""
			if adapter.Connected {
				wantLiveHandoff = spec.LiveHandoff
			}
			stateOK := row.State != ""
			if !adapter.Connected {
				stateOK = row.State == adapter.State
			}
			if row.ExecutionConnected != adapter.Connected || row.ResearchOnly || row.LiveAuthorizes ||
				row.ExecutionRole != string(spec.ActionClass)+"/"+string(spec.HandoffClass) ||
				row.LiveHandoff != wantLiveHandoff || !stateOK {
				t.Fatalf("System %s coverage is not its canonical execution contract: row=%+v spec=%+v", spec.SystemID, row, spec)
			}
		}
		class, handoff, _ := researchPromotionApplicability(spec.SystemID)
		if class == "unclassified" || handoff == "" {
			t.Fatalf("registered System %s has no explicit handoff applicability", spec.SystemID)
		}
	}
}

func TestR139HardSixLegCeilingCoversPaperAndLiveEntryPaths(t *testing.T) {
	if boundedParlayMaxLegs(0) != 6 || boundedParlayMaxLegs(1) != 2 || boundedParlayMaxLegs(4) != 4 || boundedParlayMaxLegs(10) != 6 {
		t.Fatal("configured parlay ceiling is not normalized to 2-6")
	}
	s := testServer(t)
	legs := sevenParlayLegs()
	if _, err := s.placeParlayLegs(context.Background(), legs, 10, "test"); err == nil || !strings.Contains(err.Error(), "2-6") {
		t.Fatalf("shared paper path accepted seven legs: %v", err)
	}

	post := func(handler http.HandlerFunc, body any) *httptest.ResponseRecorder {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b)))
		return rec
	}
	for name, handler := range map[string]http.HandlerFunc{
		"paper combo":        s.handleParlayPlace,
		"legacy paper combo": s.handlePlaceParlay,
	} {
		rec := post(handler, map[string]any{"stake": 10, "legs": legs})
		if rec.Code != http.StatusGone || !strings.Contains(rec.Body.String(), "manual Paper combo orders were removed") {
			t.Fatalf("%s manual response = %d %s", name, rec.Code, rec.Body.String())
		}
	}
	liveLegs := make([]map[string]string, len(legs))
	for i, leg := range legs {
		liveLegs[i] = map[string]string{"ticker": leg.Ticker, "event": "EV" + leg.Ticker, "side": "yes"}
	}
	rec := post(s.handleLiveComboPlace, map[string]any{"collection_ticker": "COL", "legs": liveLegs})
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "2-6") {
		t.Fatalf("live seven-leg response = %d %s", rec.Code, rec.Body.String())
	}

	quotes := make([]liveComboLegQuote, parlayLegLimit+1)
	for i := range quotes {
		quotes[i] = liveComboLegQuote{Ask: .5, Depth: 1, PWin: .6}
	}
	if _, _, ok := liveComboProducts(quotes); ok {
		t.Fatal("pure LIVE valuation accepted seven legs")
	}
}

func TestR139ResearchUIExposesSettingsCompleteSystemsAndPlainEVI(t *testing.T) {
	ops := strings.Index(dashboardHTML, `id="tab_operations"`)
	settings := strings.Index(dashboardHTML, `id="tab_settings"`)
	if ops < 0 || settings < ops || settings-ops > 500 {
		t.Fatalf("Settings is not beside Operations: operations=%d settings=%d", ops, settings)
	}
	for _, want := range []string{
		"Complete Systems Leaderboard", "Every registered system stays visible in this expanded diagnostic.",
		"🧪 VALIDATION", "Registered system tests",
		"Best measured systems", "Show every system collector and the complete Systems Leaderboard",
		"New book-native ML", "AUC n/a · Brier n/a",
		"New ML", "book-native-v2", "Legacy ML is retained in exports only.",
		"The six-leg ceiling applies", "assumed a fill from visible depth",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("R139 research UI missing %q", want)
		}
	}
	for _, retired := range []string{"Legacy analysis", "Legacy ML baseline — historical comparison only",
		"The old signal-price model did not out-predict market price", "'stats-backtest':", "'replay-backtest':"} {
		if strings.Contains(dashboardHTML, retired) {
			t.Fatalf("retired backtest UI resurfaced: %q", retired)
		}
	}

	s := testServer(t)
	rec := httptest.NewRecorder()
	s.handleResearchPortfolio(rec, httptest.NewRequest(http.MethodGet, "/api/research/portfolio", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "WAITING_FOR_FROZEN_INPUTS") ||
		!strings.Contains(rec.Body.String(), "required_inputs") || !strings.Contains(rec.Body.String(), "automatic_collection_changes\":false") {
		t.Fatalf("plain safe EVI contract = %d %s", rec.Code, rec.Body.String())
	}
}

func TestR145EconomicallyGradeableSystemReplacesOnlyItsExactRouteCell(t *testing.T) {
	all := r138SystemCoverageRows([]storage.ResearchSystemCollectionStat{{SystemID: "proper-score-executor",
		State: "COLLECTING", InputRows: 12, CollectorIDs: []string{"proper-score"}}})
	var exact []leaderboardCoverageRow
	for _, row := range all {
		if row.Venue == "kalshi" && row.Side == "YES" && row.Route == "taker" {
			exact = append(exact, row)
		}
	}
	rows := markTimedResearchCoverage(exact, map[string]bool{"proper-score-executor": true})
	if len(rows) != 1 || !rows[0].Timed || rows[0].ResearchOnly || rows[0].LiveAuthorizes ||
		!rows[0].ExecutionConnected || rows[0].ExecutionRole != "single/single" ||
		!strings.Contains(rows[0].LiveHandoff, "armed LIVE mirror") ||
		!strings.Contains(rows[0].Note, "not fill-conditioned profit evidence") {
		t.Fatalf("System economics were hidden, disconnected, or granted current LIVE authority: %+v", rows)
	}
	graded := appendLeaderboardCollectionPlaceholders(leaderboardBacktestRows([]storage.UnitTrialLeaderboardStat{{
		Family: "proper-score-executor", Platform: "kalshi", Side: "YES", N: 12, MeanPC: .02,
		OriginLayer: "system", NetPerCalendarDay: .15,
	}}), rows)
	if len(graded) != 1 || !graded[0].EconomicsReady || graded[0].Family != "proper-score-executor" {
		t.Fatalf("gradeable research system did not retain economic row: %+v", graded)
	}
}

func TestR139BookNativeMLMetricsAppearAndWarmingDoesNotBorrowOldScores(t *testing.T) {
	s := testServer(t)
	write := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(s.cfg().DataDir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("ml_paper.json", `{"stats":{"equity":1000,"net":0,"roi":0,"closed":0,"open_n":0},"lifetime":{"net":0},"closed":[]}`)
	write("ml_predictions.json", `{"model_status":"ACTIVE_RESEARCH","feature_schema":"book-native-v2","oos_auc":0.787,"oos_brier":0.1963,"oos_log_loss":0.5712,"predictions":[]}`)
	mlBook, main := s.MLBookBriefing(), s.BriefingText(context.Background())
	if !strings.Contains(mlBook, "book-native-v2") || !strings.Contains(mlBook, "AUC 0.787") ||
		!strings.Contains(mlBook, "Brier 0.1963") || !strings.Contains(mlBook, "log loss 0.5712") {
		t.Fatalf("ML book briefing missing new model metrics:\n%s", mlBook)
	}
	if !strings.Contains(main, "Test · AUC 0.787") || !strings.Contains(main, "Brier 0.1963") ||
		!strings.Contains(main, "Log 0.5712") || strings.Contains(main, "book-native-v2") ||
		strings.Contains(main, "provisional holdout") || strings.Contains(main, "rolling holdout") {
		t.Fatalf("main briefing model-test line is missing or cluttered:\n%s", main)
	}

	write("ml_predictions.json", `{"model_status":"WARMING_SPLIT","feature_schema":"book-native-v2","oos_auc":null,"oos_brier":null,"book_v2_resolved":321,"book_v2_open":44,"live_validation":{"days_observed":2,"days_required":5},"predictions":[]}`)
	mlBook, main = s.MLBookBriefing(), s.BriefingText(context.Background())
	if !strings.Contains(mlBook, "warming") || !strings.Contains(mlBook, "AUC/Brier/log loss pending") ||
		!strings.Contains(mlBook, "resolved n=321") || !strings.Contains(mlBook, "LIVE 2/5 days") ||
		strings.Contains(mlBook, "AUC 0.787") {
		t.Fatalf("ML book warming truth is stale or unclear:\n%s", mlBook)
	}
	if !strings.Contains(main, "Test · warming") || !strings.Contains(main, "AUC/Brier/Log pending") ||
		!strings.Contains(main, "samples 321") || !strings.Contains(main, "LIVE 2/5 days") ||
		strings.Contains(main, "AUC 0.787") {
		t.Fatalf("main briefing warming truth is stale or unclear:\n%s", main)
	}
}
