package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/payoffsolver"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR148EveryStagedProductOwnsAnExactNamedTypedBundleStream(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := &Server{store: st}
	level := payoffsolver.Level{Price: .40, Quantity: 2,
		FeeQuotes: []payoffsolver.FeeQuote{{Quantity: 1, Total: .01}}}
	unwind := payoffsolver.Level{Price: .39, Quantity: 2,
		FeeQuotes: []payoffsolver.FeeQuote{{Quantity: 1, Total: .01}}}
	problem := payoffsolver.Problem{Certificate: payoffsolver.Certificate{
		CanonicalEventID: "event:r148-named", EventVersion: 1, States: []string{"a", "b"},
		RulesHash: "rules", RelationsHash: "relations", Verified: true, Complete: true,
	}, Legs: []payoffsolver.Leg{
		{ID: "A|YES", Venue: "kalshi", Ticker: "A", Side: "YES", PayoffID: "A", Payoff: []float64{1, 0},
			Levels: []payoffsolver.Level{level}, FullBookLevels: []payoffsolver.Level{level},
			UnwindLevels: []payoffsolver.Level{unwind}, FullUnwindLevels: []payoffsolver.Level{unwind},
			QuoteAgeSeconds: .1, TickSize: .01, BookSource: "book", SourceClockID: "clock-a", FeeSource: "fee",
			UnwindBookSource: "book", UnwindFeeSource: "fee"},
		{ID: "B|NO", Venue: "polyus", Ticker: "B", Side: "NO", PayoffID: "B", Payoff: []float64{0, 1},
			Levels: []payoffsolver.Level{level}, FullBookLevels: []payoffsolver.Level{level},
			UnwindLevels: []payoffsolver.Level{unwind}, FullUnwindLevels: []payoffsolver.Level{unwind},
			QuoteAgeSeconds: .1, TickSize: .01, BookSource: "book", SourceClockID: "clock-b", FeeSource: "fee",
			UnwindBookSource: "book", UnwindFeeSource: "fee"},
	}}
	row := payoffsolver.Solution{LegIDs: []string{"A|YES", "B|NO"}, Tickers: []string{"A", "B"},
		Size: 1, Cost: .80, Fees: .02, PayoutFloor: 1, NetFloor: .18, BottleneckDepth: 2,
		PartialFillWorstLoss: -.41, UnwindWorstLoss: -.04, UnwindKnown: true,
		ResearchOnly: true, StatePayouts: []float64{1, 1}}
	now := time.Now().UTC()
	want := map[string]bool{"event-basket-lock": false, "nested-ladder-lock": false, "time-nested-lock": false, "xvlock": false}
	for system := range want {
		inserted, _, recordErr := s.recordR148NamedRouteBundles(context.Background(), now, system,
			"cohort", "opportunity-"+system, "certificate-"+system, "verified", "staged route",
			problem, []payoffsolver.Solution{row}, map[string]any{"fixture": true})
		if recordErr != nil || inserted != 1 {
			t.Fatalf("%s inserted=%d err=%v", system, inserted, recordErr)
		}
	}
	bundles, err := st.RecentPositiveResearchRouteBundles(context.Background(), now.Add(-time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, bundle := range bundles {
		if _, ok := want[bundle.SystemID]; ok && len(bundle.Legs) == 2 && bundle.UnwindKnown {
			want[bundle.SystemID] = true
		}
	}
	for system, seen := range want {
		if !seen {
			t.Errorf("%s has no exact named typed bundle", system)
		}
	}
}
