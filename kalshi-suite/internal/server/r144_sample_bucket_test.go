package server

import (
	"math"
	"strings"
	"testing"
)

func TestR144BriefSampleBucketBoundaries(t *testing.T) {
	for _, tc := range []struct {
		markets int
		want    string
	}{{0, "🔴"}, {10, "🔴"}, {11, "🟠"}, {40, "🟠"}, {41, "🟡"},
		{120, "🟡"}, {121, "🟢"}, {500, "🟢"}, {501, "🔵"}, {1000, "🔵"}, {1001, "🟣"}} {
		if got := briefSampleBucket(tc.markets); got != tc.want {
			t.Fatalf("n=%d bucket=%q want %q", tc.markets, got, tc.want)
		}
	}
}

func TestR144ConnectedComboRowsBecomeIndependentBlockMeans(t *testing.T) {
	got := connectedOutcomeMeans([]float64{1, -1, .5}, [][]string{{"A"}, {"A", "B"}, {"C"}})
	if len(got) != 2 {
		t.Fatalf("connected blocks=%v want two", got)
	}
	_, mean, sd := meanSD(got)
	if math.Abs(mean-.25) > 1e-12 || math.Abs(sd-math.Sqrt(.125)) > 1e-12 {
		t.Fatalf("block means=%v mean=%v sd=%v", got, mean, sd)
	}
}

func TestR144VerdictEvidenceAveragesRepeatedRowsWithinMarket(t *testing.T) {
	var evidence verdictMarketEvidence
	evidence.Add("kalshi|A", .9)
	evidence.Add("kalshi|A", -.9)
	evidence.Add("polyus|B", .2)
	v := verdictFromMarketEvidence("test", "book", "$/round", evidence, 0)
	if v.N != 2 || v.Markets != 2 || v.SettledRows != 3 {
		t.Fatalf("verdict denominators=%+v want n2 rows3", v)
	}
	if math.Abs(v.Mean-.1) > 1e-9 {
		t.Fatalf("market-averaged mean=%v want .1", v.Mean)
	}
}

func TestR144BriefVerdictReceiptNamesCompactRouteAndDistinctContractCount(t *testing.T) {
	got := briefVerdictReceipt(verdictEnt{Platform: "kalshi", Side: "YES", Route: "taker",
		State: "PROVEN+", N: 32, Markets: 32, ContractMarkets: 32, SettledRows: 35})
	want := " · K/Y/T · n32 🟠 · PROVEN+"
	if got != want {
		t.Fatalf("receipt=%q want %q", got, want)
	}
	if strings.Contains(got, "m32") || strings.Contains(got, "rows35") {
		t.Fatalf("compact receipt regressed to duplicate verbose denominator: %q", got)
	}
}

func TestR144VerdictTelegramBatchIsOnePlainFifteenMinuteSummary(t *testing.T) {
	got := verdictTelegramBatchMessage(map[string]verdictTelegramChange{
		"edge|side=YES": {Key: "edge|side=YES", Kind: "verdict", Line: "Experiment verdict: taker:edge@kalshi [YES] is now PROVEN+"},
		"other|side=NO": {Key: "other|side=NO", Kind: "verdict", Line: "Experiment verdict: taker:other@polyus [NO] is now PROVEN-"},
	})
	for _, want := range []string{"⚙️ System results · 2 changes in the last 15 minutes",
		"• taker:edge@kalshi [YES] is now PROVEN+", "• taker:other@polyus [NO] is now PROVEN-"} {
		if !strings.Contains(got, want) {
			t.Fatalf("batched message missing %q: %s", want, got)
		}
	}
}
