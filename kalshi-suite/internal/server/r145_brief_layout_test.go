package server

import (
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR145SystemBriefUsesDistinctContractsNotRepeatedRows(t *testing.T) {
	canonical := verdictEnt{Family: "taker:spotlag@kalshi", SourceFamily: "spotlag",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 51, Markets: 51, EventClusters: 51, ContractMarkets: 84, SettledRows: 900,
		Mean: .237, Lo: .053, Hi: .421, State: "PROVEN+",
		ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"}
	rows := briefRankedSystems([]verdictEnt{canonical}, true, 5)
	if len(rows) != 1 {
		t.Fatalf("canonical route missing from compact rank: %+v", rows)
	}
	line := briefRankedSystemLine(rows[0], map[string]int{})
	for _, want := range []string{"spot lag", "$/d warming", "+23.7¢/s", "n84", "CI +5.3..+42.1¢", "K/Y/T"} {
		if !strings.Contains(line, want) {
			t.Fatalf("canonical row missing %q: %s", want, line)
		}
	}
	if strings.Contains(line, "n900") || strings.Contains(line, "m51") {
		t.Fatalf("repeated rows or removed m leaked into the system line: %s", line)
	}
}

func TestR145CompactRateUnitsAreSignedDollarsAndCents(t *testing.T) {
	if got := briefSignedDollarsPerDay(12.4); got != "+$12.40/d" {
		t.Fatalf("positive dollar/day display = %q, want +$12.40/d", got)
	}
	if got := briefSignedDollarsPerDay(-12.4); got != "-$12.40/d" {
		t.Fatalf("negative dollar/day display = %q, want -$12.40/d", got)
	}
	row := briefRankedSystem{v: verdictEnt{Family: "taker:unit-format@kalshi", SourceFamily: "unit-format",
		Platform: "kalshi", Side: "YES", Route: "taker", ContractMarkets: 100,
		Mean: .045, Lo: .02, Hi: .07}, cell: "K/Y/T", mean: .045, ratePoint: 12.4, ratePointOK: true}
	line := briefRankedSystemLine(row, map[string]int{})
	if !strings.Contains(line, "+$12.40/d · +4.5¢/s") || strings.Contains(line, "Net/d") {
		t.Fatalf("compact system units are not exact or duplicate the old label: %s", line)
	}
}

func TestR165PhoneRankUsesOnlyAuthenticatedConservativeNetD(t *testing.T) {
	s := &Server{}
	s.researchDigestTrials = []storage.UnitTrialLeaderboardStat{
		{Family: "large-edge-slow", Platform: "kalshi", OriginLayer: "model", Side: "YES",
			SettledMarkets: 100, TrackedSeconds: 100 * 86400, MeanPC: .20, SDPC: .01},
		{Family: "smaller-edge-fast", Platform: "polyus", OriginLayer: "model", Side: "YES",
			SettledMarkets: 100, TrackedSeconds: 10 * 86400, MeanPC: .10, SDPC: .01},
		{Family: "negative-noisy", Platform: "kalshi", OriginLayer: "model", Side: "NO",
			SettledMarkets: 20, TrackedSeconds: 86400, MeanPC: -.20, SDPC: .40},
		{Family: "negative-sure", Platform: "kalshi", OriginLayer: "model", Side: "NO",
			SettledMarkets: 100, TrackedSeconds: 10 * 86400, MeanPC: -.10, SDPC: .01},
		{Family: "zero-variance", Platform: "kalshi", OriginLayer: "model", Side: "YES",
			SettledMarkets: 100, TrackedSeconds: 10 * 86400, MeanPC: .99, SDPC: 0},
	}
	if got := s.briefCachedRouteRates(); len(got) != 0 {
		t.Fatalf("assumed-fill UnitTrial rates entered the operator phone cache: %+v", got)
	}
	// Ranking mechanics remain covered with an explicitly authenticated synthetic rate map. The
	// production cache is empty until a real exchange-filled rate reader exists.
	rates := map[string]briefRouteRate{
		briefRouteRateKey("large-edge-slow", "kalshi", "model", "YES", "taker"): {
			point: .20, lo: .12, hi: .28, mean: .20, n: 100, pointOK: true, ready: true},
		briefRouteRateKey("smaller-edge-fast", "polyus", "model", "YES", "taker"): {
			point: 1.0, lo: .60, hi: 1.40, mean: .10, n: 100, pointOK: true, ready: true},
		briefRouteRateKey("negative-noisy", "kalshi", "model", "NO", "taker"): {
			point: -.20, lo: -.90, hi: .50, mean: -.20, n: 20, pointOK: true, ready: true},
		briefRouteRateKey("negative-sure", "kalshi", "model", "NO", "taker"): {
			point: -1.0, lo: -1.30, hi: -.70, mean: -.10, n: 100, pointOK: true, ready: true},
	}
	verdict := func(family, venue, side string, n int, mean, lo, hi float64) verdictEnt {
		return verdictEnt{Family: "taker:" + family + "@" + venue, SourceFamily: family,
			Platform: venue, OriginLayer: "model", Route: "taker", Side: side, Group: "taker",
			N: n, Markets: n, ContractMarkets: n, Mean: mean, Lo: lo, Hi: hi,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"}
	}
	rows := []verdictEnt{
		verdict("large-edge-slow", "kalshi", "YES", 100, .20, .15, .25),
		verdict("smaller-edge-fast", "polyus", "YES", 100, .10, .07, .13),
		verdict("negative-noisy", "kalshi", "NO", 20, -.20, -.90, .50),
		verdict("negative-sure", "kalshi", "NO", 100, -.10, -.13, -.07),
		verdict("zero-variance", "kalshi", "YES", 100, .99, .98, 1.0),
		// Exact maker evidence remains on the dashboard, but has no cached maker Net/d bound.
		{Family: "maker:no-rate@kalshi", SourceFamily: "no-rate", Platform: "kalshi",
			OriginLayer: "model", Route: "maker", Side: "YES", N: 100, ContractMarkets: 100,
			Mean: .90, Lo: .80, Hi: .95, ProfitEvidence: true,
			FillConditioned: true, EvidenceTier: "authenticated_live_fill"},
	}
	positive := briefRankedSystemsWithRates(rows, true, 5, rates)
	if len(positive) != 2 || positive[0].v.SourceFamily != "smaller-edge-fast" {
		t.Fatalf("phone Top did not rank conservative Net/d first or admitted no-rate maker: %+v", positive)
	}
	line := briefRankedSystemLine(positive[0], map[string]int{})
	if !strings.Contains(line, "/d [") || !strings.Contains(line, "..") ||
		!strings.Contains(line, "] · +10.0¢/s") || strings.Index(line, "/d") > strings.Index(line, "¢/s") {
		t.Fatalf("phone line must show signed dollars/day before cents/share: %s", line)
	}
	negative := briefRankedSystemsWithRates(rows, false, 2, rates)
	if len(negative) != 2 || negative[0].v.SourceFamily != "negative-sure" {
		t.Fatalf("Bottom must use the conservative upper Net/d bound: %+v", negative)
	}
	stale := verdict("large-edge-slow", "kalshi", "YES", 99, .20, .15, .25)
	if got := briefRankedSystemsWithRates([]verdictEnt{stale}, true, 5, rates); len(got) != 0 {
		t.Fatalf("mixed-cohort verdict/rate cache entered phone rank: %+v", got)
	}
}

func TestR145ColdPhoneRateCacheSchedulesNonblockingDigestRefresh(t *testing.T) {
	s := &Server{}
	kicks := 0
	s.researchDigestKickFn = func() { kicks++ }
	if got := s.briefCachedRouteRates(); len(got) != 0 || kicks != 1 {
		t.Fatalf("cold phone cache = %d rates, %d kicks; want warming + one scheduled refresh", len(got), kicks)
	}
}

func TestR145LegacyUnknownEventIdentityCannotEnterCompactRank(t *testing.T) {
	legacy := verdictEnt{Family: "taker:legacy@kalshi", SourceFamily: "legacy",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		ContractMarkets: 5, SettledRows: 500, PaperPointMean: .80, Mean: 0,
		Lo: -9999, Hi: 9999, State: "COLLECTING"}
	current := verdictEnt{Family: "taker:current@kalshi", SourceFamily: "current",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 50, Markets: 50, EventClusters: 50, ContractMarkets: 60, SettledRows: 600,
		Mean: .03, Lo: .005, Hi: .055, State: "PROVEN+",
		ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"}
	rows := briefRankedSystems([]verdictEnt{legacy, current}, true, 5)
	if len(rows) != 1 || rows[0].v.SourceFamily != "current" {
		t.Fatalf("unknown-identity legacy row competed with canonical evidence: %+v", rows)
	}
	line := briefRankedSystemLine(briefRankedSystem{v: legacy, cell: "K/Y/T", mean: .80,
		pointOnly: true, pointN: 5}, map[string]int{})
	if !strings.Contains(line, "n5 · CI unavailable") || strings.Contains(line, "m") {
		t.Fatalf("legacy sample label is misleading: %s", line)
	}
}

func TestR145RelatedCellsOmitEmptyCategories(t *testing.T) {
	if got, ok := briefRelationCell("independent", storage.RelationPerformanceCell{}); ok || got != "" {
		t.Fatalf("empty relation cell rendered: %q %v", got, ok)
	}
	got, ok := briefRelationCell("unknown", storage.RelationPerformanceCell{UniqueBets: 8, DollarsPerBet: -1.97})
	if !ok || got != "unknown -$1.97/bet · 8 bets" {
		t.Fatalf("relation cell = %q,%v", got, ok)
	}
}

func TestR145MLBriefIsVerticalAndOmitsProfitPerDay(t *testing.T) {
	auc, brier, logLoss := .681, .2222, .6342
	pr := mlPredictionBrief{OOSAUC: &auc, OOSBrier: &brier, OOSLogLoss: &logLoss}
	pr.SplitReceipt.TestRows = 1408
	pf := mlPaperBrief{VenueSleeves: map[string]mlVenueSleeveBrief{
		vbKalshi: {Net: 4.74, Closed: 5}, vbPolyus: {Net: 91.87, Closed: 6, Open: 1},
	}}
	pf.Stats.OpenN = 1
	e := mlBriefEconomics{Net: 96.61, Contracts: 478.27, PredictionContracts: 478.27,
		SettledN: 11, RealizedCents: 20.2, PredictedCents: 9.1, RateReady: true, NetPerDay: 121.75}
	got := mlCompactMainBrief(pr, true, pf, true, e)
	for _, want := range []string{
		"🤖 New ML\nTest · AUC 0.681 · Brier 0.2222 · Log 0.6342 · samples 1,408",
		"Paper · 11 settled · +$96.61 · +20.2¢/share · predicted +9.1¢ · open 1",
		"Kalshi · P&L +$4.74 · closed 5",
		"PolyUS · P&L +$91.87 · closed 6 · open 1",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("vertical ML brief missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "/day") || strings.Contains(got, "equity") || strings.Contains(got, "available") {
		t.Fatalf("ML brief retained removed clutter:\n%s", got)
	}
}

func TestR145ProperBriefIsOneResultPerLine(t *testing.T) {
	report := map[string]any{"paper_portfolios": map[string]any{
		"brier": map[string]any{"entry_mark_equity_usd": 401.25, "net_profit_usd": 1.25,
			"fills": 3, "open_positions": 1, "settled_positions": 2},
		"log": map[string]any{"entry_mark_equity_usd": 399.50, "net_profit_usd": -.50,
			"fills": 2, "open_positions": 0, "settled_positions": 2},
		"spherical": map[string]any{"entry_mark_equity_usd": 400.0, "net_profit_usd": 0.0,
			"fills": 0, "open_positions": 0, "settled_positions": 0},
	}, "transforms": map[string]any{
		"brier": map[string]any{"normalized_executable_settled_l1": 4.0,
			"normalized_executable_completed_slots": 7, "normalized_executable_unique_settled_markets": 6,
			"l1_weighted_realized_net": -1.0},
		"log": map[string]any{"normalized_executable_settled_l1": 4.0,
			"normalized_executable_completed_slots": 7, "normalized_executable_unique_settled_markets": 6,
			"l1_weighted_realized_net": -1.012},
		"spherical": map[string]any{"normalized_executable_settled_l1": 4.0,
			"normalized_executable_completed_slots": 7, "normalized_executable_unique_settled_markets": 6,
			"l1_weighted_realized_net": -.988},
	}}
	got := properScoreBriefLine(report)
	want := "🧮 Proper Betting Paper · $400 each\n" +
		"Brier Paper · net +$1.25 · NAV $401.25 · fills 3 (open 1 / settled 2)\n" +
		"Log Paper · net -$0.50 · NAV $399.50 · fills 2 (open 0 / settled 2)\n" +
		"Spherical Paper · net +$0.00 · NAV $400.00 · fills 0 (open 0 / settled 0)\n" +
		"Research/group result (not cash P&L) · Brier · -25.0¢ · 7 batches · 6 markets | " +
		"Log · -25.3¢ · 7 batches · 6 markets | Spherical · -24.7¢ · 7 batches · 6 markets"
	if got != want {
		t.Fatalf("Proper Betting layout:\n got %q\nwant %q", got, want)
	}
}

func TestR145SystemsUIUsesCompactDollarAndShareLabels(t *testing.T) {
	if !strings.Contains(dashboardHTML, ">$/d</th>") || !strings.Contains(dashboardHTML, ">¢/share</th>") ||
		strings.Contains(dashboardHTML, ">Net/day</th>") {
		t.Fatalf("Systems UI must use the explicit $/d and ¢/share labels")
	}
	if strings.Contains(dashboardHTML, ">%/$-day</th>") || strings.Contains(dashboardHTML, "return_per_dollar_day") {
		t.Fatalf("Systems UI must not display a fee-net ROI proxy")
	}
}
