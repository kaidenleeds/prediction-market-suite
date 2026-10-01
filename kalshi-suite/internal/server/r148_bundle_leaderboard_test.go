package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR148BundleEconomicRowsKeepStagedIdentity(t *testing.T) {
	stat := storage.UnitTrialLeaderboardStat{Family: "event-basket-lock", Platform: "kalshi",
		OriginLayer: "strategy", Side: "BUNDLE", N: 24, Total: 25, SettledMarkets: 24,
		UniqueMarkets: 25, Open: 1, OpenMarkets: 1, MeanPC: .04, SDPC: .10,
		TrackedSeconds: 48 * 60 * 60, TrackedDays: 2, NetPerCalendarDay: .48}
	rows := leaderboardBacktestRows([]storage.UnitTrialLeaderboardStat{stat})
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if rows[0].Route != "staged" || rows[0].SystemID != "staged:event-basket-lock@kalshi [BUNDLE]" ||
		rows[0].Side != "BUNDLE" || rows[0].SettledMarkets != 24 || rows[0].EconomicUnit != "package_unit" ||
		!rows[0].Counterfactual || !rows[0].ResearchOnly || rows[0].CollectionSource != "research_route_bundles" {
		t.Fatalf("bundle leaderboard identity=%+v", rows[0])
	}
	compact := compactTrial(stat)
	if compact.Route != "staged" || compact.Side != "BUNDLE" || compact.N != 24 || compact.EconomicUnit != "package_unit" {
		t.Fatalf("compact bundle identity=%+v", compact)
	}
}
