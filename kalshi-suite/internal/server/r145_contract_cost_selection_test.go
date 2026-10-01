package server

import (
	"math"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR145PaperCurrentCostUsesContractCohortNotCanonicalSubset(t *testing.T) {
	u := storage.UnitTrialLeaderboardStat{Family: "contract-cost", Platform: "kalshi",
		N: 200, SettledMarkets: 30, MeanPC: .10, SDPC: .01,
		MeanAsk: .70, MeanFeePC: .03,
		// A sparse canonical subset happened to be much cheaper. It is research diagnostics,
		// not the cost baseline for the 30-contract PAPER cohort.
		ProofMeanAsk: .20, ProofMeanFeePC: 0}
	state, mean, lo := liveMirrorUnitAdjusted(u, .70, .03)
	if state != "PROVEN+" || math.Abs(mean-.10) > 1e-12 || lo <= 0 {
		t.Fatalf("canonical subset repriced PAPER cohort: state=%s mean=%v lo=%v", state, mean, lo)
	}
	_, worseMean, _ := liveMirrorUnitAdjusted(u, .75, .04)
	if math.Abs(worseMean-.04) > 1e-12 {
		t.Fatalf("current all-in deterioration not charged from contract baseline: got=%v want=.04", worseMean)
	}
}

func TestR145ContractCostsFlowIntoVerdictAndPaperRepricing(t *testing.T) {
	wantAsk := (0.90 + 0.90 + 0.10) / 3
	wantFee := (0.09 + 0.09 + 0.01) / 3
	verdictFee := math.Round(wantFee*1e4) / 1e4
	v := unitTrialVerdict(storage.UnitTrialStat{Family: "contract-cost", Platform: "kalshi",
		Side: "YES", N: 7, SettledMarkets: 3, MeanPC: .10, SDPC: .01,
		MeanAsk: wantAsk, MeanFeePC: wantFee})
	if v.N != 3 || v.SettledRows != 7 {
		t.Fatalf("verdict sample was not distinct contracts: %+v", v)
	}
	ask, fee := gfPaperPointCosts(v)
	if math.Abs(ask-wantAsk) > 1e-12 || math.Abs(fee-verdictFee) > 1e-12 {
		t.Fatalf("PAPER lost contract-equal cost cohort: ask=%v fee=%v", ask, fee)
	}
	got, ok := gfCurrentRouteEdge(v.Mean, ask, .70, fee, verdictFee, 1)
	want := .10 - (.70 - wantAsk)
	if !ok || math.Abs(got-want) > 1e-12 {
		t.Fatalf("PAPER current-cost edge=%v ok=%v want=%v", got, ok, want)
	}
}
