package server

import (
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR139RegisteredSideClassificationIsExact(t *testing.T) {
	rows, counts := registeredSideSelectionRows()
	if len(rows) != 111 || counts[sideClassTwo] != 24 || counts[sideClassOne] != 53 ||
		counts[sideClassMulti] != 19 || counts[sideClassRouter] != 15 {
		t.Fatalf("side registry rows=%d counts=%+v", len(rows), counts)
	}
	if counts["single_trigger"] != 23 || counts["multi_indicator"] != 78 || counts["not_discovery"] != 10 {
		t.Fatalf("discovery counts=%+v", counts)
	}
	seen := map[string]bool{}
	var singles []string
	for _, row := range rows {
		if seen[row.Family] || row.SideClass == "" || row.DiscoveryClass == "" {
			t.Fatalf("duplicate/unclassified row=%+v", row)
		}
		seen[row.Family] = true
		if row.DiscoveryClass == "single_trigger" {
			singles = append(singles, row.Family)
		}
	}
	if len(singles) != 23 {
		t.Fatalf("single-trigger rows=%v", singles)
	}
}

func TestR139SemanticSideClassesPinHighRiskFamilies(t *testing.T) {
	wants := map[string]string{
		"xvgap": sideClassOne, "xvgap2": sideClassOne, "xvgapk": sideClassOne,
		"freshlist": sideClassOne, "freshfade": sideClassOne, "wxedge": sideClassOne,
		"weather": sideClassOne, "weather-curve-revision": sideClassOne,
		"independent-probabilistic-weather": sideClassTwo, "weather-curve-residual": sideClassTwo,
		"arb": sideClassOne, "rfq-sim": sideClassMulti,
		"attention-spillover-graph": sideClassTwo, "forecast-persona-router": sideClassTwo,
		"deadline-hazard-surface": sideClassTwo, "flow-direction-integrity": sideClassTwo,
		"semantic-complexity-premium": sideClassTwo, "maker-salvage-matched-cohort": sideClassRouter,
		"payoff-constraint-solver": sideClassMulti,
	}
	for family, want := range wants {
		if got, _, _ := systemSideClassification(family); got != want {
			t.Fatalf("%s class=%s want %s", family, got, want)
		}
	}
}

func TestR144OppositeObservationHasIndependentSystemIdentity(t *testing.T) {
	in := storage.Signal{SignalType: "kalshi-flow", Platform: "kalshi", Ticker: "KXSIDE", Side: "YES"}
	out, ok := independentInverseSignalIdentity(in)
	if !ok || out.SignalType != "invert:kalshi-flow" || out.Side != "NO" || out.Ticker != in.Ticker {
		t.Fatalf("independent inverse identity=%+v ok=%v", out, ok)
	}
	if got, _, _ := systemSideClassification(out.SignalType); got != sideClassOne {
		t.Fatalf("independent inverse has wrong side class: %s", got)
	}
	if out.EntryPrice != 0 || out.FeePC != nil || out.ModelProb != nil || out.BookFeatureVer != 0 {
		t.Fatalf("inverse copied emitted-side money/model truth: %+v", out)
	}
}

func TestR139GenericFollowerCannotMechanicallyFlipOneSideFamily(t *testing.T) {
	v := verdictEnt{Family: "kalshi-flow", Group: "signal", Venues: map[string]venueVerdict{
		"kalshi": {N: 100, Mean: -.10, FeePC: .01, InvHC: .02},
	}}
	if inv, _, ok := gfVenueMode(v, v.Family, "kalshi"); inv || ok {
		t.Fatalf("negative emitted route became executable inverse: inv=%v ok=%v", inv, ok)
	}
	exact := verdictEnt{Family: "taker:kalshi-flow@kalshi", SourceFamily: "kalshi-flow", Platform: "kalshi",
		OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker", N: 100, Mean: -.10}
	rows := gfPaperExecutionCoverage([]verdictEnt{exact})
	if len(rows) != 1 || rows[0].Executor != "generic-follower" || rows[0].Direction != "direct" {
		t.Fatalf("coverage changed the emitted side instead of neutrally collecting it: %+v", rows)
	}
}

func TestR139SyntheticInverseHasNoPaperOrLiveAuthority(t *testing.T) {
	s := testServer(t)
	s.polMu.Lock()
	s.polMap = map[string]string{"kalshi-flow": "inverted"}
	s.polMu.Unlock()
	if s.policyInverted("auto-cons-kalshi-flow") {
		t.Fatal("legacy inverted policy state authorized an order")
	}
	yes := verdictStableKey(verdictEnt{Family: "taker:edge@kalshi", Side: "YES"})
	no := verdictStableKey(verdictEnt{Family: "taker:edge@kalshi", Side: "NO"})
	if yes == no {
		t.Fatal("YES and NO verdict states share a persistence key")
	}
}
