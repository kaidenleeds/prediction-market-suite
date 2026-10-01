package server

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

// Accepted cost remains durable UTC-day telemetry after the operator removed the turnover
// allowance. Releasing current exposure restores headroom; prior churn stays visible but does not
// refuse a new order. The 7.5% stop-loss remains separate.
func TestR163ReleasedCapitalTurnoverIsReportedButDoesNotBlock(t *testing.T) {
	s := testServer(t)
	s.liveBankArm = map[string]float64{"kalshi": 100, "polyus": 100}
	s.liveBankCashRead = func(_ context.Context, _ string) (float64, bool) { return 100, true }
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveMaxOrderUSD = -1
		c.Risk.LiveMaxOrderPct = 0.05
		c.Risk.LiveExposureCapUSD = -1
		c.Risk.LiveExposureCapPct = 0.50
		c.Risk.LiveMaxDailyLossUSD = -1
		c.Risk.LiveDailyLossPct = 0.10
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemPolyUS = true
	})

	// Two earlier full deployment cycles remain in the reporting counter. Hermetic venue state has
	// no current positions/resting orders, so the current exposure rail permits redeployment.
	s.liveRiskBook(200, "kalshi")
	if got := s.liveVenueDayTurnover("kalshi"); math.Abs(got-200) > 1e-9 {
		t.Fatalf("durable turnover observation=%v want 200", got)
	}
	if why := s.liveRiskCheck(5, "kalshi"); why != "" {
		t.Fatalf("reported turnover must not act as a daily allowance: %s", why)
	}
	if why := s.liveVenueBudgetCheck(context.Background(), "kalshi", 5); why != "" {
		t.Fatalf("current-at-risk rail should admit redeployment with zero outstanding exposure: %s", why)
	}
	view := s.liveRailsView()
	if got, _ := view["turnover_today"].(float64); math.Abs(got-200) > 1e-9 {
		t.Fatalf("turnover telemetry=%v want 200", got)
	}
	if got, _ := view["turnover_limit"].(string); got != "none; reporting only" {
		t.Fatalf("turnover limit label=%q want reporting-only", got)
	}
	if !strings.Contains(dashboardHTML,
		"Accepted turnover is reporting-only and has no daily allowance") ||
		strings.Contains(dashboardHTML, "Turnover has a separate automatic UTC-day limit") {
		t.Fatal("dashboard turnover tooltip does not match the reporting-only server rail")
	}
}
