package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR147CompactRankExcludesTinyExtremeResults(t *testing.T) {
	verdict := func(family string, n int, mean, lo, hi float64) verdictEnt {
		return verdictEnt{Family: "taker:" + family + "@kalshi", SourceFamily: family,
			Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "NO", Group: "taker",
			N: n, Markets: n, ContractMarkets: n, Mean: mean, Lo: lo, Hi: hi,
			ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill"}
	}
	tiny := verdict("fbridge", 2, -.90, -.99, -.80)
	credible := verdict("spotlag", 32, -.10, -.20, -.05)
	rates := map[string]briefRouteRate{
		briefRouteRateKey("fbridge", "kalshi", "model", "NO", "taker"): {
			point: -50, lo: -55, hi: -45, mean: -.90, n: 2, pointOK: true, ready: true,
		},
		briefRouteRateKey("spotlag", "kalshi", "model", "NO", "taker"): {
			point: -2, lo: -4, hi: -1, mean: -.10, n: 32, pointOK: true, ready: true,
		},
	}

	rows := briefRankedSystemsWithRates([]verdictEnt{tiny, credible}, false, 2, rates)
	if len(rows) != 1 || rows[0].v.SourceFamily != "spotlag" {
		t.Fatalf("tiny extreme displaced credible compact result: %+v", rows)
	}
}

func TestR165CachedRateRejectsAssumedFillHistoryAtEverySampleSize(t *testing.T) {
	s := &Server{}
	s.researchDigestTrials = []storage.UnitTrialLeaderboardStat{
		{Family: "tiny", Platform: "kalshi", OriginLayer: "model", Side: "YES",
			SettledMarkets: 2, TrackedSeconds: 2 * 86400, MeanPC: .20, SDPC: .01},
		{Family: "credible", Platform: "kalshi", OriginLayer: "model", Side: "YES",
			SettledMarkets: briefRankMinSettledContracts, TrackedSeconds: 2 * 86400, MeanPC: .20, SDPC: .01},
	}
	rates := s.briefCachedRouteRates()
	if len(rates) != 0 {
		t.Fatalf("sample size converted assumed-fill history into an operator profit rate: %+v", rates)
	}
}
