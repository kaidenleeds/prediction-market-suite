package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func countTruthRow(family, platform, side, route, origin string) leaderboardBacktestRow {
	return leaderboardBacktestRow{
		UnitTrialLeaderboardStat: storage.UnitTrialLeaderboardStat{
			Family: family, Platform: platform, Side: side, OriginLayer: origin,
		},
		SystemID: family,
		Route:    route,
	}
}

func TestR145SystemCountDeduplicatesEvidenceAndFundedAliases(t *testing.T) {
	rows := []leaderboardBacktestRow{
		countTruthRow("kalshi-flow", "kalshi", "YES", "taker", "model"),
		countTruthRow("kalshi-flow", "kalshi", "YES", "taker", "strategy"),
		// Strategy aliases carry the venue in the name while their placeholder still says multi.
		countTruthRow("strategy:kalshi-flow@kalshi", "multi", "YES", "taker", "strategy"),
		// Funded mirrors carry the complete identity in the roster key while warming row fields are blank.
		countTruthRow("gf:kalshi-flow:yes:taker-k", "multi", "", "", "strategy"),
		countTruthRow("maker:kalshi-flow@kalshi", "kalshi", "YES", "maker", "strategy"),
		countTruthRow("kalshi-flow", "kalshi", "NO", "taker", "model"),
		countTruthRow("invert:kalshi-flow", "kalshi", "NO", "taker", "model"),
	}
	variants, systems := trackedSystemCountRows(rows)
	if variants != 4 || systems != 2 {
		t.Fatalf("count=%d exact variants/%d base systems, want 4/2", variants, systems)
	}
}

func TestR145SystemCountUsesEmbeddedVariantIdentity(t *testing.T) {
	rows := []leaderboardBacktestRow{
		countTruthRow("strategy:alpha@polyus", "multi", "YES", "taker", "strategy"),
		countTruthRow("taker:alpha@polyus [YES]", "multi", "", "", "model"),
	}
	variants, systems := trackedSystemCountRows(rows)
	if variants != 1 || systems != 1 {
		t.Fatalf("embedded identity count=%d/%d, want 1/1", variants, systems)
	}
}

func TestR145SystemCountExcludesControlsObserversAndSummaryRows(t *testing.T) {
	rows := []leaderboardBacktestRow{
		countTruthRow("side-control:alpha", "kalshi", "NO", "taker", "model"),
		countTruthRow("counterfactual:invert:alpha", "kalshi", "NO", "taker", "model"),
		countTruthRow("book:alpha", "multi", "", "", "strategy"),
		countTruthRow("raw-flow-observer@kalshi", "kalshi", "", "", "model"),
		countTruthRow("adaptive-whale-scorer@polyus", "polyus", "", "", "system"),
		countTruthRow("systems-regimes", "multi", "", "", "system"),
		countTruthRow("behavioral-bias-regime", "multi", "", "", "system"),
		countTruthRow("weather-curve-residual", "polymarket", "", "", "model"),
		countTruthRow("proper-score-brier", "multi", "", "", "strategy"),
		countTruthRow("proper-score-log", "multi", "", "", "strategy"),
		countTruthRow("proper-score-spherical", "multi", "", "", "strategy"),
		countTruthRow("auto-ml", "multi", "", "", "strategy"),
		countTruthRow("manual", "multi", "", "", "strategy"),
		countTruthRow("maker-fills", "multi", "", "", "execution_route"),
		countTruthRow("rfq-sim", "multi", "", "", "strategy"),
		countTruthRow("parlay-2leg", "multi", "", "", "strategy"),
		// Unsupported and not-yet-routed registered systems still count as system definitions.
		countTruthRow("identity-challenged-cross-venue-lock", "unsupported", "no executable side", "needs execution adapter", "strategy"),
		countTruthRow("queue-priority", "kalshi", "", "", "execution_route"),
	}
	variants, systems := trackedSystemCountRows(rows)
	if variants != 0 || systems != 2 {
		t.Fatalf("diagnostic filter count=%d/%d, want 0 exact variants/2 base systems", variants, systems)
	}
}
