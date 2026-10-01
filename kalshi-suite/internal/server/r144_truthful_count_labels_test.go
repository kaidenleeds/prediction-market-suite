package server

import (
	"strings"
	"testing"
)

func TestR144ResearchRegistryAndLatestCycleHaveTruthfulUILabels(t *testing.T) {
	for _, want := range []string{
		"Collector status (subset)",
		"validation collectors",
		"Collector rows are inputs/health checks, not extra executable systems",
		"immutable validation contracts; not the full tracked-system total",
		"latest cycle matched ",
		"cumulative exact route rows",
		"cumulative economics not loaded here",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard is missing truthful count label %q", want)
		}
	}
	for _, stale := range []string{
		"r141Card('Research progress'",
		"research hypotheses; not the full tracked-system total",
		"registered study systems",
		"This is a subset of the full Systems total",
	} {
		if strings.Contains(dashboardHTML, stale) {
			t.Fatalf("dashboard still implies the research-hypothesis count is the full Systems total: %q", stale)
		}
	}
}

func TestR144PortfolioBriefLabelsEligibilityInsteadOfRuntimeLiveness(t *testing.T) {
	line := (&Server{}).activePortfolioSystemsLine()
	if !strings.Contains(line, "positive authenticated exchange-profit routes") || strings.Contains(line, " active ") {
		t.Fatalf("portfolio summary overstates current liveness: %q", line)
	}
}
