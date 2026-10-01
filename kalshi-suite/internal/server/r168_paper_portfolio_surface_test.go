package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestR168ProperPaperAPIKeepsAllThreeZeroLanes(t *testing.T) {
	s := testServer(t)
	report, err := s.store.ProperScorePaperPortfolioReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 3 {
		t.Fatalf("Proper Paper portfolios=%d, want exactly Brier/log/spherical: %#v", len(report), report)
	}
	for _, lane := range []string{"brier", "log", "spherical"} {
		row, ok := report[lane].(map[string]any)
		if !ok {
			t.Fatalf("%s portfolio missing from healthy empty report: %#v", lane, report[lane])
		}
		if row["seed_usd"] != float64(400) || row["entry_mark_equity_usd"] != float64(400) ||
			row["net_profit_usd"] != float64(0) || row["fills"] != 0 ||
			row["open_positions"] != 0 || row["settled_positions"] != 0 {
			t.Fatalf("%s healthy empty portfolio is not an explicit zero ledger: %#v", lane, row)
		}
	}
	rr := httptest.NewRecorder()
	s.handleProperScorePaper(rr, httptest.NewRequest(http.MethodGet, "/api/proper-score-paper", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("lightweight Proper Paper endpoint status=%d body=%s", rr.Code, rr.Body.String())
	}
	var payload struct {
		PaperPortfolios map[string]any `json:"paper_portfolios"`
	}
	if err = json.Unmarshal(rr.Body.Bytes(), &payload); err != nil || len(payload.PaperPortfolios) != 3 {
		t.Fatalf("lightweight Proper Paper endpoint lost a lane: err=%v payload=%#v", err, payload)
	}
}

func TestR168DashboardShowsEveryPaperPortfolioAndHonestProperErrors(t *testing.T) {
	for _, want := range []string{
		"🟩 Kalshi", "🇺🇸 PolyUS", "🧩 Combos", "🤖 ML", "🤖🎲 ML Combo",
		"properPaperW", "function loadProperPaper()", "jget('/api/proper-score-paper',20000)",
		"Proper Betting Paper · $400 each", "lane('brier','Brier')",
		"lane('log','Log')", "lane('spherical','Spherical')",
		"unavailable — not counted as $0", "typeof s.bankroll==='number'",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard missing persistent Paper portfolio surface %q", want)
		}
	}
	if strings.Contains(dashboardHTML, "money(s.bankroll||200)") {
		t.Fatal("Combo Paper still turns an honest zero bankroll into the retired $200 fallback")
	}
}
