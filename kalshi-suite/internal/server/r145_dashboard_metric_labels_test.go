package server

import (
	"strings"
	"testing"
)

func TestR145SystemsDashboardUsesCompactDollarDayWithoutSeparateROI(t *testing.T) {
	start := strings.Index(dashboardHTML, "function loadLeaderboardBacktest(")
	end := strings.Index(dashboardHTML[start:], "function setSystemsRegimeWindow(")
	if start < 0 || end < 0 {
		t.Fatal("systems leaderboard renderer boundary missing")
	}
	section := dashboardHTML[start : start+end]
	for _, forbidden := range []string{"Net/day", "%/$-day", "return_per_dollar_day"} {
		if strings.Contains(section, forbidden) {
			t.Fatalf("systems leaderboard still exposes %q", forbidden)
		}
	}
	if !strings.Contains(section, ">$/d</th>") || !strings.Contains(section, ">¢/share</th>") ||
		!strings.Contains(section, "point dollars/day ") {
		t.Fatal("systems leaderboard does not use the explicit $/d and ¢/share labels")
	}
	if strings.Contains(section, ">¢/s</th>") {
		t.Fatal("ambiguous cents-per-s label returned")
	}
	if !strings.Contains(dashboardHTML, "function r145NetD(v)") ||
		!strings.Contains(dashboardHTML, "r145NetD(r.net_per_day_lower)+'…'+r145NetD(r.net_per_day_upper)") {
		t.Fatal("dollar/day point or interval is not formatted as signed dollars/day")
	}
	if !strings.Contains(dashboardHTML, "Math.abs(v).toFixed(d)+'/d'") ||
		strings.Contains(section, "+' Net/d") {
		t.Fatal("Systems UI must render signed $.../d without duplicating the old Net/d suffix")
	}
	// ROI remains a useful combo-specific display and an internal eligibility input. This change is
	// deliberately limited to the duplicate Systems Leaderboard dollar-day column.
	if !strings.Contains(dashboardHTML, "Net ROI per $1") {
		t.Fatal("combo ROI UI was removed while changing the Systems Leaderboard")
	}
}

func TestR165SystemsDashboardNamesSimulationAndShowsPaperLosers(t *testing.T) {
	start := strings.Index(dashboardHTML, "function loadFundedSystemPerformance(")
	if start < 0 {
		t.Fatal("funded Paper renderer start missing")
	}
	end := strings.Index(dashboardHTML[start:], "// R142:")
	if end < 0 {
		t.Fatal("funded Paper renderer boundary missing")
	}
	funded := dashboardHTML[start : start+end]
	if strings.Contains(funded, "total_realized_profit_dollars||0)>0") {
		t.Fatal("funded Paper losers are still hidden")
	}
	for _, want := range []string{"FUNDED PAPER SIMULATION", "Winners and losers are both shown", "do not prove an exchange order or fill"} {
		if !strings.Contains(funded, want) {
			t.Fatalf("funded Paper simulation truth label missing %q", want)
		}
	}
	leaderboardStart := strings.Index(dashboardHTML, "function loadLeaderboardBacktest(")
	if leaderboardStart < 0 {
		t.Fatal("leaderboard renderer start missing")
	}
	leaderboardEnd := strings.Index(dashboardHTML[leaderboardStart:], "function setSystemsRegimeWindow(")
	if leaderboardEnd < 0 {
		t.Fatal("leaderboard renderer end missing")
	}
	leaderboard := dashboardHTML[leaderboardStart : leaderboardStart+leaderboardEnd]
	for _, want := range []string{"evidence_tier", "fill-conditioned", "profit_evidence", "live_authorizes", "assumed-fill history"} {
		if !strings.Contains(leaderboard, want) {
			t.Fatalf("leaderboard evidence truth missing %q", want)
		}
	}
}

func r165DashboardSection(t *testing.T, start, end string) string {
	t.Helper()
	from := strings.Index(dashboardHTML, start)
	if from < 0 {
		t.Fatalf("dashboard section start missing %q", start)
	}
	to := strings.Index(dashboardHTML[from:], end)
	if to < 0 {
		t.Fatalf("dashboard section end missing %q after %q", end, start)
	}
	return dashboardHTML[from : from+to]
}

func TestR165DashboardSeparatesReplayPaperAndExchangeProfitTruth(t *testing.T) {
	for _, want := range []string{
		"Paper placement and visible-book assumed fills are not exchange profit evidence",
		"SIGNAL-LOG REPLAY · RESEARCH ONLY.",
		"MODELED +", "MODELED −",
		"Legacy Paper allocation model · research only",
		"Paper-simulation signal toggles",
		"cannot promote, size, or authorize LIVE",
	} {
		if !strings.Contains(dashboardHTML, want) {
			t.Fatalf("dashboard money-truth label missing %q", want)
		}
	}

	edge := r165DashboardSection(t, "function loadEdge()", "function closeCurves()")
	for _, stale := range []string{"clear a real edge", "&gt;0 = real edge", "filled bets"} {
		if strings.Contains(edge, stale) {
			t.Fatalf("signal-log replay still claims exchange truth %q", stale)
		}
	}

	curves := r165DashboardSection(t, "function vpill(v)", "function loadAllSigEV()")
	for _, stale := range []string{"['PROMOTE'", "money that was actually winnable", "The real edge", "Net by signal + verdict (DECIDES)"} {
		if strings.Contains(curves, stale) {
			t.Fatalf("Paper curves still expose money-authority wording %q", stale)
		}
	}

	allocation := r165DashboardSection(t, "function loadAlloc()", "function loadCoverage()")
	for _, want := range []string{"MODELED WEIGHT", "legacy Paper weights only", "does not place or authorize an exchange order"} {
		if !strings.Contains(allocation, want) {
			t.Fatalf("legacy allocation truth missing %q", want)
		}
	}
	for _, stale := range []string{"what autoPlace applies", "proven: n≥30", "FAIL-OPENS to the default-ON posture", "placement ON (direct emitted side)"} {
		if strings.Contains(allocation, stale) {
			t.Fatalf("legacy allocation still claims cash authority %q", stale)
		}
	}

	verdicts := r165DashboardSection(t, "function loadVerdicts()", "function renderWeatherBook()")
	for _, want := range []string{"EXCHANGE PROFIT", "RESEARCH ONLY", "v.profit_evidence===true", "v.fill_conditioned===true", "v.live_authorizes===true"} {
		if !strings.Contains(verdicts, want) {
			t.Fatalf("verdict evidence-class label missing %q", want)
		}
	}
	for _, stale := range []string{"verdicts are peek-proof", "realized fee-net edge per unit"} {
		if strings.Contains(verdicts, stale) {
			t.Fatalf("verdict panel still pools research with exchange evidence %q", stale)
		}
	}

	signals := r165DashboardSection(t, "function renderSignals()", "function toggleSig(")
	for _, stale := range []string{"inverting profits after fees", "Verdict (realized)", "Toggle trading, flip direction"} {
		if strings.Contains(signals, stale) {
			t.Fatalf("Paper signal controls still claim cash truth %q", stale)
		}
	}
}
