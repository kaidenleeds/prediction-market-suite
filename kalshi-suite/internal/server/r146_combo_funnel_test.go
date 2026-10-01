package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestR146ComboSpacesDoNotCountMutuallyExclusiveRowsAsCombos(t *testing.T) {
	oneInstrument := make([]plabLeg, 0, 10)
	for i := 0; i < 10; i++ {
		side := "YES"
		if i%2 == 1 {
			side = "NO"
		}
		oneInstrument = append(oneInstrument, plabLeg{Platform: "kalshi", Ticker: "ONLY-ONE", Side: side})
	}
	if raw := plabRawSpace(len(oneInstrument), 6).String(); raw != "837" {
		t.Fatalf("raw route-row upper bound = %s, want 837", raw)
	}
	unique, structural, sameVenue := plabStructuralSpaces(oneInstrument, 6)
	if unique != 1 || structural.Sign() != 0 || sameVenue.Sign() != 0 {
		t.Fatalf("one instrument must yield no legal multi-instrument combo: unique=%d structural=%s same-venue=%s",
			unique, structural, sameVenue)
	}
}

func TestR146ComboSpacesCountRouteChoicesAndVenueConstraint(t *testing.T) {
	legs := []plabLeg{
		{Platform: "kalshi", Ticker: "A", Side: "YES"},
		{Platform: "kalshi", Ticker: "A", Side: "NO"},
		{Platform: "kalshi", Ticker: "B", Side: "YES"},
		{Platform: "kalshi", Ticker: "B", Side: "NO"},
	}
	unique, structural, sameVenue := plabStructuralSpaces(legs, 6)
	if unique != 2 || structural.String() != "4" || sameVenue.String() != "4" {
		t.Fatalf("two 2-choice instruments = four valid two-leg route shapes: unique=%d structural=%s same-venue=%s",
			unique, structural, sameVenue)
	}

	crossVenue := []plabLeg{
		{Platform: "kalshi", Ticker: "A", Side: "YES"},
		{Platform: "polyus", Ticker: "B", Side: "YES"},
	}
	unique, structural, sameVenue = plabStructuralSpaces(crossVenue, 6)
	if unique != 2 || structural.String() != "1" || sameVenue.Sign() != 0 {
		t.Fatalf("mixed-venue pair is research-structural but cannot be one atomic venue combo: unique=%d structural=%s same-venue=%s",
			unique, structural, sameVenue)
	}
}

func TestR146ComboSamplerSelectsDistinctInstrumentsByConstruction(t *testing.T) {
	pool := make([]plabLeg, 0, 101)
	for i := 0; i < 100; i++ {
		side := "YES"
		if i%2 == 1 {
			side = "NO"
		}
		pool = append(pool, plabLeg{Platform: "kalshi", Ticker: "CROWDED", Side: side, Price: .4, PWin: .6})
	}
	pool = append(pool, plabLeg{Platform: "kalshi", Ticker: "SPARSE", Side: "YES", Price: .4, PWin: .6})

	for attempt := 0; attempt < 20; attempt++ {
		legs := plabSamplePick(pool, 2, "r146-distinct-"+string(rune('a'+attempt)), nil)
		if len(legs) != 2 || plabValidateLegSet(legs) != nil {
			t.Fatalf("attempt %d did not produce two distinct instruments: %+v", attempt, legs)
		}
	}
	proposals, _ := plabStableProspectiveSample(pool, 2, 32)
	if len(proposals) == 0 {
		t.Fatal("a heavily duplicated route-row pool with two instruments produced no prospective combo")
	}
	for _, proposal := range proposals {
		if err := plabValidateLegSet(proposal.Legs); err != nil {
			t.Fatalf("sampler emitted an invalid repeated-instrument combo: %v", err)
		}
	}
}

func TestR146ComboFunnelReportsExplicitZeroAndReason(t *testing.T) {
	s := testServer(t)
	pool := []plabLeg{
		{Platform: "kalshi", Ticker: "ONLY-ONE", Side: "YES", Price: .4, PWin: .6},
		{Platform: "kalshi", Ticker: "ONLY-ONE", Side: "NO", Price: .6, PWin: .4},
	}
	st := s.plabWriteProspectiveSample(context.Background(), pool, 6, 0, time.Now().UTC())
	if st.Proposed != 0 || st.Eligible != 0 || st.Inserted != 0 || st.Rejected["no_structurally_valid_proposal"] != 1 {
		t.Fatalf("zero funnel must be explicit and diagnosed, got %+v", st)
	}

	body, err := json.Marshal(plabEnumStats{SampleRejected: st.Rejected})
	if err != nil {
		t.Fatal(err)
	}
	got := string(body)
	for _, key := range []string{`"sample_proposed":0`, `"sample_eligible":0`, `"sample_inserted":0`,
		`"sample_rejected":{"no_structurally_valid_proposal":1}`} {
		if !strings.Contains(got, key) {
			t.Fatalf("explicit zero-stage API field %s missing from %s", key, got)
		}
	}
}

func TestR146ComboProbabilityEdgeFailsClosedInsteadOfClampingBadUnits(t *testing.T) {
	if p, ok := plabProbabilityFromEdge(.41, .04); !ok || p < .449999 || p > .450001 {
		t.Fatalf("ordinary probability-space edge rejected: p=%v ok=%v", p, ok)
	}
	for _, tc := range []struct{ allIn, edge float64 }{
		{.41, 45}, // historical cents-vs-dollars corruption which previously clamped to .9999
		{.99, .02},
		{.41, 0},
	} {
		if p, ok := plabProbabilityFromEdge(tc.allIn, tc.edge); ok {
			t.Fatalf("impossible edge admitted: all-in=%v edge=%v p=%v ok=%v", tc.allIn, tc.edge, p, ok)
		}
	}
}
