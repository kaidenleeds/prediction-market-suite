package server

import (
	"strings"
	"testing"
)

func TestR145TakerSystemIsNotDeclaredStreamless(t *testing.T) {
	s := testServer(t)
	v := verdictEnt{
		Family: "taker:favlong@polyus", SourceFamily: "favlong",
		Platform: "polyus", OriginLayer: "model", Route: "taker", Side: "YES",
		Group: "taker", Unit: "$/contract", N: 794, Mean: -.1468,
		FeePC: .005, InvHC: .02, State: "PROVEN-",
	}
	got := s.invertVerdictStatus(v)
	for _, want := range []string{"invert:favlong", "COLLECTING", "actual opposite asks/depth/fees/settlements"} {
		if !strings.Contains(got, want) {
			t.Fatalf("real taker system status missing %q: %s", want, got)
		}
	}
	for _, forbidden := range []string{"this experiment", "no executable signal stream", "BLOCKED because this experiment"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("real taker system was falsely described as streamless: %s", got)
		}
	}
}

func TestR145InverseStatusCannotBorrowAnotherVenueSideOrRoute(t *testing.T) {
	s := testServer(t)
	s.verdMu.Lock()
	s.verdCache = []verdictEnt{
		{Family: "taker:invert:edge@polyus", SourceFamily: "invert:edge", Platform: "polyus",
			Route: "taker", Side: "NO", Group: "taker", N: 900, Mean: .90},
		{Family: "taker:invert:edge@kalshi", SourceFamily: "invert:edge", Platform: "kalshi",
			Route: "taker", Side: "YES", Group: "taker", N: 800, Mean: .80},
		{Family: "maker:invert:edge@kalshi", SourceFamily: "invert:edge", Platform: "kalshi",
			Route: "maker", Side: "NO", Group: "maker", N: 700, Mean: .70},
		{Family: "taker:invert:edge@kalshi", SourceFamily: "invert:edge", Platform: "kalshi",
			Route: "taker", Side: "NO", Group: "taker", N: 12, Mean: .04},
	}
	s.verdMu.Unlock()
	got := s.invertVerdictStatus(verdictEnt{
		Family: "taker:edge@kalshi", SourceFamily: "edge", Platform: "kalshi",
		Route: "taker", Side: "YES", Group: "taker", Unit: "$/contract",
		N: 50, Mean: -.10, FeePC: .01, InvHC: .01, State: "PROVEN-",
	})
	if !strings.Contains(got, "+4.0 cents/ct") || !strings.Contains(got, "n=12") {
		t.Fatalf("exact inverse cell was not selected: %s", got)
	}
	for _, leaked := range []string{"+90.0", "+80.0", "+70.0", "n=900", "n=800", "n=700"} {
		if strings.Contains(got, leaked) {
			t.Fatalf("inverse status borrowed wrong exact cell %q: %s", leaked, got)
		}
	}
}

func TestR145SideControlIsNotCalledAnIndependentSystem(t *testing.T) {
	s := testServer(t)
	v := verdictEnt{
		Family: "taker:side-control:kalshi-flow@kalshi", SourceFamily: "side-control:kalshi-flow",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "NO",
		Group: "taker", Unit: "$/contract", N: 734, Mean: -.1331,
		FeePC: .01, InvHC: .02, State: "PROVEN-",
	}
	got := s.invertVerdictStatus(v)
	for _, want := range []string{"opposite-side control", "not an order-producing system", "no Paper/LIVE order authority"} {
		if !strings.Contains(got, want) {
			t.Fatalf("control status missing %q: %s", want, got)
		}
	}
	for _, forbidden := range []string{"independent invert:side-control", "no executable signal stream"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("control was promoted to an inverse system: %s", got)
		}
	}
}

func TestR145CounterfactualAndNamedInverseNeverRecursivelyInvert(t *testing.T) {
	s := testServer(t)
	counterfactual := verdictEnt{
		Family: "counterfactual:invert:favlong", Group: "invert",
		Unit: "$/contract", N: 100, Mean: -.05, State: "PROVEN-",
	}
	got := s.invertVerdictStatus(counterfactual)
	if !strings.Contains(got, "diagnostic counterfactual only") ||
		!strings.Contains(got, "cannot be recursively inverted") {
		t.Fatalf("counterfactual recursion status=%s", got)
	}

	named := verdictEnt{
		Family: "taker:invert:favlong@polyus", SourceFamily: "invert:favlong",
		Platform: "polyus", Route: "taker", Side: "NO", Group: "taker",
		Unit: "$/contract", N: 30, Mean: -.04, State: "PROVEN-",
	}
	got = s.invertVerdictStatus(named)
	if !strings.Contains(got, "named inverse invert:favlong") ||
		!strings.Contains(got, "recursive inversion is forbidden") ||
		strings.Contains(got, "invert:invert:") {
		t.Fatalf("named inverse recursion status=%s", got)
	}
}

func TestR145CompositeSystemsDescribeTheirRealExecutionRole(t *testing.T) {
	s := testServer(t)
	multi := verdictEnt{
		Family: "strategy:payoff-constraint-solver@kalshi", SourceFamily: "payoff-constraint-solver",
		Group: "strategy", Unit: "$/contract", Mean: -.08, FeePC: .01, InvHC: .02,
	}
	got := s.invertVerdictStatus(multi)
	if !strings.Contains(got, "multi-leg system") || !strings.Contains(got, "combo/RFQ route") {
		t.Fatalf("multi-leg execution role=%s", got)
	}

	registeredSingle := verdictEnt{
		Family: "strategy:forecast-persona-router@kalshi", SourceFamily: "forecast-persona-router",
		Platform: "kalshi", Side: "YES", Route: "taker",
		Group: "strategy", Unit: "$/contract", Mean: -.08, FeePC: .01, InvHC: .02,
	}
	got = s.invertVerdictStatus(registeredSingle)
	if !strings.Contains(got, "independent invert:forecast-persona-router") ||
		!strings.Contains(got, "actual opposite asks/depth/fees/settlements") {
		t.Fatalf("registered single execution role=%s", got)
	}
	if strings.Contains(got, "no executable signal stream") {
		t.Fatalf("composite system was falsely described as disconnected: %s", got)
	}
}

func TestR145OperatorVerdictTextUsesSystemTerminology(t *testing.T) {
	line := verdictTransitionLine(verdictEnt{
		Family: "taker:favlong@polyus", Side: "YES", State: "PROVEN-",
		N: 25, Mean: -.03, Lo: -.05, Hi: -.01, Unit: "$/contract",
		ProfitEvidence: true, FillConditioned: true, EvidenceTier: "authenticated_live_fill",
	})
	if !strings.HasPrefix(line, "System result:") || strings.Contains(line, "Experiment") {
		t.Fatalf("transition line retained old terminology: %s", line)
	}
	message := verdictTelegramBatchMessage(map[string]verdictTelegramChange{
		"favlong": {Key: "favlong", Kind: "verdict", Line: line},
	})
	if !strings.Contains(message, "System results") || strings.Contains(message, "Experiment") {
		t.Fatalf("batch retained old terminology: %s", message)
	}
	if key := legacyVerdictLineKey(
		"Experiment verdict: legacy is now PROVEN+ (n=20, edge +0.01 $/contract)", 0,
	); key != "legacy:legacy" {
		t.Fatalf("legacy queued line compatibility key=%q", key)
	}
}
