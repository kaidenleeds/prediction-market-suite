package server

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestR165PortfolioSurfacesTellPaperSimulationTruth(t *testing.T) {
	for _, want := range []string{
		"Legacy Paper allocation model · research only",
		"Paper simulation portfolios (separate hypothetical $600 starting grants)",
		"book_kalshi_usd",
		"book_polyus_usd",
		"book_combos_usd",
		"book_ml_usd",
		"not exchange equity",
		"never sizes cash",
		"cannot promote, size, or authorize LIVE",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard missing R133 portfolio truth %q", want)
		}
	}
	for _, stale := range []string{
		"budgets = frac × live total equity",
		"equity split <span",
		"Equity-fraction mode OFF",
		"<b>Compounding Paper equity</b>",
		"what autoPlace applies",
		"FAIL-OPENS to the default-ON posture",
	} {
		if strings.Contains(dashboardHTML, stale) {
			t.Fatalf("dashboard still exposes retired money story %q", stale)
		}
	}
	if fixedPaperPortfolioUSD(0) != 600 || fixedPaperPortfolioUSD(-1) != 600 || fixedPaperPortfolioUSD(4321) != 4321 {
		t.Fatal("effective fixed-portfolio defaults/configured values changed")
	}
}

func TestR133PortfolioSettingsRoundTrip(t *testing.T) {
	s := testServer(t)
	body := strings.NewReader(`{"book_kalshi_usd":1101,"book_polyus_usd":1102,"book_combos_usd":1103,"book_ml_usd":1104}`)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/settings", body)
	req.Header.Set("Content-Type", "application/json")
	s.handleSettings(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("settings POST = %d: %s", rec.Code, rec.Body.String())
	}
	a := s.cfg().Auto
	if a.BookKalshiUSD != 1101 || a.BookPolyusUSD != 1102 || a.BookCombosUSD != 1103 || a.BookMLUSD != 1104 {
		t.Fatalf("portfolio settings did not apply: K/US/C/ML = %.0f/%.0f/%.0f/%.0f", a.BookKalshiUSD, a.BookPolyusUSD, a.BookCombosUSD, a.BookMLUSD)
	}

	rec = httptest.NewRecorder()
	s.handleSettings(rec, httptest.NewRequest(http.MethodGet, "/api/settings", nil))
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("settings GET JSON: %v (%s)", err, rec.Body.String())
	}
	for k, want := range map[string]float64{
		"book_kalshi_usd": 1101,
		"book_polyus_usd": 1102,
		"book_combos_usd": 1103,
		"book_ml_usd":     1104,
	} {
		v, ok := got[k].(float64)
		if !ok || math.Abs(v-want) > 1e-9 {
			t.Fatalf("settings GET %s = %#v, want %.0f", k, got[k], want)
		}
	}
}
