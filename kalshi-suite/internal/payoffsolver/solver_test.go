package payoffsolver

import (
	"math"
	"testing"
)

func fixtureCertificate(states []string) Certificate {
	return Certificate{CanonicalEventID: "event:1", EventVersion: 1, States: states,
		RulesHash: "sha256:rules", RelationsHash: "sha256:relations", Verified: true, Complete: true}
}

func TestExclusiveSolverFindsFeeNetFloorAndCapacity(t *testing.T) {
	books := map[string]Leg{}
	for _, id := range []string{"A", "B", "C"} {
		books[id] = Leg{ID: id, Venue: "kalshi", Ticker: "KX-" + id, Side: "YES", PayoffID: id,
			Levels: []Level{{Price: .20, Quantity: 2, FeePerShare: .01}}, QuoteAgeSeconds: .1, TickSize: .01}
	}
	legs, err := ExclusiveOutcomeLegs([]string{"A", "B", "C"}, books)
	if err != nil {
		t.Fatal(err)
	}
	s, ok, err := Solve(Problem{Certificate: fixtureCertificate([]string{"A", "B", "C"}),
		Legs: legs, Sizes: []float64{1, 2, 3}, MaxLegs: 3, MaxQuoteAgeSeconds: 1})
	if err != nil || !ok {
		t.Fatalf("solution=%+v ok=%v err=%v", s, ok, err)
	}
	if s.Size != 2 || math.Abs(s.NetFloor-.74) > 1e-12 || s.BottleneckDepth != 2 || s.AtomicRoute {
		t.Fatalf("money truth=%+v", s)
	}
	if s.PartialFillWorstLoss >= 0 || !s.ResearchOnly {
		t.Fatalf("partial/authority truth=%+v", s)
	}
}

func TestNestedThresholdStateVector(t *testing.T) {
	// States: <=low, between, >high. YES(low) + NO(high) pays 1,2,1.
	legs := []Leg{
		{ID: "yes-low", Ticker: "LOW", Side: "YES", PayoffID: "p-low", Payoff: []float64{0, 1, 1},
			Levels: []Level{{Price: .45, Quantity: 1, FeePerShare: .01}}, QuoteAgeSeconds: .2, TickSize: .01},
		{ID: "no-high", Ticker: "HIGH", Side: "NO", PayoffID: "not-p-high", Payoff: []float64{1, 1, 0},
			Levels: []Level{{Price: .45, Quantity: 1, FeePerShare: .01}}, QuoteAgeSeconds: .2, TickSize: .01},
	}
	s, ok, err := Solve(Problem{Certificate: fixtureCertificate([]string{"below", "between", "above"}),
		Legs: legs, Sizes: []float64{1}, MaxLegs: 2, MaxQuoteAgeSeconds: 1})
	if err != nil || !ok || math.Abs(s.NetFloor-.08) > 1e-12 {
		t.Fatalf("nested solution=%+v ok=%v err=%v", s, ok, err)
	}
}

func TestSolverFailsClosedWithoutCertificateOrAfterFees(t *testing.T) {
	legs := []Leg{
		{ID: "a", Ticker: "A", Side: "YES", PayoffID: "a", Payoff: []float64{1, 0}, Levels: []Level{{Price: .49, Quantity: 1, FeePerShare: .02}}, TickSize: .01},
		{ID: "b", Ticker: "B", Side: "YES", PayoffID: "b", Payoff: []float64{0, 1}, Levels: []Level{{Price: .49, Quantity: 1, FeePerShare: .02}}, TickSize: .01},
	}
	bad := fixtureCertificate([]string{"a", "b"})
	bad.Verified = false
	if _, _, err := Solve(Problem{Certificate: bad, Legs: legs, Sizes: []float64{1}, MaxLegs: 2, MaxQuoteAgeSeconds: 1}); err == nil {
		t.Fatal("title/unverified identity reached solver")
	}
	good := fixtureCertificate([]string{"a", "b"})
	if s, ok, err := Solve(Problem{Certificate: good, Legs: legs, Sizes: []float64{1}, MaxLegs: 2, MaxQuoteAgeSeconds: 1}); err != nil || ok {
		t.Fatalf("fee-erased lock survived: %+v ok=%v err=%v", s, ok, err)
	}
}

func TestSolverUsesExactNonlinearFeeQuotesAndRetainsNegativeControls(t *testing.T) {
	legs := []Leg{
		{ID: "a", Ticker: "A", Side: "YES", PayoffID: "a", Payoff: []float64{1, 0},
			Levels:          []Level{{Price: .40, Quantity: 2, FeeQuotes: []FeeQuote{{Quantity: 1, Total: .01}, {Quantity: 2, Total: .07}}}},
			QuoteAgeSeconds: .1, TickSize: .01},
		{ID: "b", Ticker: "B", Side: "YES", PayoffID: "b", Payoff: []float64{0, 1},
			Levels:          []Level{{Price: .40, Quantity: 2, FeeQuotes: []FeeQuote{{Quantity: 1, Total: .01}, {Quantity: 2, Total: .07}}}},
			QuoteAgeSeconds: .1, TickSize: .01},
	}
	rows, err := Evaluate(Problem{Certificate: fixtureCertificate([]string{"a", "b"}), Legs: legs,
		Sizes: []float64{1, 2}, MaxLegs: 2, MaxQuoteAgeSeconds: 1})
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if math.Abs(rows[0].Fees-.02) > 1e-12 || math.Abs(rows[1].Fees-.14) > 1e-12 {
		t.Fatalf("exact nonlinear fees not used: %+v", rows)
	}
	// The missing q=1 quote must fail closed for a q=1 evaluation; it cannot multiply q=2's fee.
	legs[0].Levels[0].FeeQuotes = []FeeQuote{{Quantity: 2, Total: .07}}
	if rows, err := Evaluate(Problem{Certificate: fixtureCertificate([]string{"a", "b"}), Legs: legs,
		Sizes: []float64{1}, MaxLegs: 2, MaxQuoteAgeSeconds: 1}); err != nil || len(rows) != 0 {
		t.Fatalf("unmatched exact fee quote should produce no executable row, rows=%+v err=%v", rows, err)
	}
}

func TestSolverCannotBypassSixLegCeiling(t *testing.T) {
	legs := make([]Leg, 7)
	states := make([]string, 7)
	for i := range legs {
		states[i] = string(rune('A' + i))
		payoff := make([]float64, 7)
		payoff[i] = 1
		legs[i] = Leg{ID: states[i], Ticker: "KX-" + states[i], Side: "YES", PayoffID: states[i],
			Payoff: payoff, Levels: []Level{{Price: .1, Quantity: 1, FeePerShare: .001}},
			QuoteAgeSeconds: .1, TickSize: .01}
	}
	if _, err := Evaluate(Problem{Certificate: fixtureCertificate(states), Legs: legs,
		Sizes: []float64{1}, MaxLegs: 7, MaxQuoteAgeSeconds: 1}); err == nil {
		t.Fatal("payoff solver accepted a seven-leg research/execution route")
	}
}

func TestSolverKeepsTypedUnwindEnvelopeSeparateFromHoldToZero(t *testing.T) {
	legs := []Leg{
		{ID: "a", Ticker: "A", Side: "YES", PayoffID: "a", Payoff: []float64{1, 0},
			Levels:          []Level{{Price: .40, Quantity: 1, FeeQuotes: []FeeQuote{{Quantity: 1, Total: .01}}}},
			UnwindLevels:    []Level{{Price: .38, Quantity: 1, FeeQuotes: []FeeQuote{{Quantity: 1, Total: .01}}}},
			QuoteAgeSeconds: .1, TickSize: .01},
		{ID: "b", Ticker: "B", Side: "YES", PayoffID: "b", Payoff: []float64{0, 1},
			Levels:          []Level{{Price: .40, Quantity: 1, FeeQuotes: []FeeQuote{{Quantity: 1, Total: .01}}}},
			UnwindLevels:    []Level{{Price: .35, Quantity: 1, FeeQuotes: []FeeQuote{{Quantity: 1, Total: .01}}}},
			QuoteAgeSeconds: .1, TickSize: .01},
	}
	rows, err := Evaluate(Problem{Certificate: fixtureCertificate([]string{"a", "b"}), Legs: legs,
		Sizes: []float64{1}, MaxLegs: 2, MaxQuoteAgeSeconds: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if !rows[0].UnwindKnown || math.Abs(rows[0].UnwindWorstLoss-(-.07)) > 1e-12 ||
		math.Abs(rows[0].PartialFillWorstLoss-(-.82)) > 1e-12 {
		t.Fatalf("unwind/partial truth=%+v", rows[0])
	}
	legs[1].UnwindLevels = nil
	rows, err = Evaluate(Problem{Certificate: fixtureCertificate([]string{"a", "b"}), Legs: legs,
		Sizes: []float64{1}, MaxLegs: 2, MaxQuoteAgeSeconds: 1})
	if err != nil || len(rows) != 1 || rows[0].UnwindKnown || rows[0].UnwindWorstLoss != rows[0].PartialFillWorstLoss {
		t.Fatalf("unknown unwind did not fail closed: rows=%+v err=%v", rows, err)
	}
}
