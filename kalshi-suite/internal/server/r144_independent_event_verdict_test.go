package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR145UnitTrialVerdictUsesDistinctContractsAndRetainsClustersAsDiagnostics(t *testing.T) {
	v := unitTrialVerdict(storage.UnitTrialStat{
		Family: "same-game", Platform: "kalshi", OriginLayer: "strategy", Side: "YES",
		N: 9, SettledMarkets: 5, SettledEventClusters: 2, ClusteredSettledMarkets: 4,
		UnclusteredSettledMarkets: 1, EventClusterMeanPC: .12, EventClusterSDPC: .03,
		EventClusterMeanAsk: .40, EventClusterMeanFeePC: .002,
		MeanPC: -.40, SDPC: .90, MeanAsk: .44, MeanFeePC: .01,
	})
	if v.N != 5 || v.Markets != 5 || v.EventClusters != 2 {
		t.Fatalf("contract display n or diagnostic clusters are wrong: %+v", v)
	}
	if v.ContractMarkets != 5 || v.UnclusteredMarkets != 1 || v.SettledRows != 9 {
		t.Fatalf("diagnostic contract/row counts were lost: %+v", v)
	}
	if v.Mean != -.40 || v.SD != .90 {
		t.Fatalf("verdict did not use one observation per settled contract: %+v", v)
	}
	if v.MeanAsk != .44 || v.FeePC != .01 || v.PaperPointMeanAsk != .44 || v.PaperPointMeanFeePC != .01 {
		t.Fatalf("contract point and cost cohort were mixed: %+v", v)
	}
	if v.Family != "strategy:same-game@kalshi" || v.Side != "YES" || v.Route != "taker" {
		t.Fatalf("exact execution identity changed: %+v", v)
	}
	if v.EvidenceTier != "historical_assumed_fill_simulation_void" || v.FillConditioned || v.ProfitEvidence || v.LiveAuthorizes ||
		v.State != "VOID ASSUMED-FILL HISTORY" || v.Invert != 0 {
		t.Fatalf("observed-book quote snapshot masqueraded as filled or LIVE-authorizing proof: %+v", v)
	}
}

func TestR145UnitTrialVerdictCountsUnclusteredDistinctContractsWithoutCallingThemIndependent(t *testing.T) {
	v := unitTrialVerdict(storage.UnitTrialStat{
		Family: "legacy", Platform: "polyus", OriginLayer: "model", Side: "NO",
		N: 200, SettledMarkets: 100, UnclusteredSettledMarkets: 100,
		MeanPC: .50, SDPC: .01,
	})
	if v.N != 100 || v.Markets != 100 || v.EventClusters != 0 || v.Mean != .50 {
		t.Fatalf("distinct contract coverage was hidden or mislabeled: %+v", v)
	}
	if v.ContractMarkets != 100 || v.UnclusteredMarkets != 100 || v.SettledRows != 200 {
		t.Fatalf("unclustered history was hidden instead of surfaced: %+v", v)
	}
	if v.State != "VOID ASSUMED-FILL HISTORY" || v.EvidenceTier != "historical_assumed_fill_simulation_void" ||
		v.FillConditioned || v.ProfitEvidence || v.LiveAuthorizes {
		t.Fatalf("confidence-qualified simulation was mislabeled as exchange-fill proof: %+v", v)
	}
}
