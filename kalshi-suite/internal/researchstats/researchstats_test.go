package researchstats

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestChronologicalEventSplitIsEventDisjointAndEmbargoed(t *testing.T) {
	base := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	var rows []Observation
	for i := 0; i < 10; i++ {
		id := string(rune('A' + i))
		rows = append(rows,
			Observation{EventID: id, Day: base.AddDate(0, 0, i), Value: float64(i)},
			Observation{EventID: id, Day: base.AddDate(0, 0, i), Value: float64(i) + .1})
	}
	s, err := ChronologicalEventSplit(rows, .4, .2, 1)
	if err != nil {
		t.Fatal(err)
	}
	owner := map[string]string{}
	for label, ids := range map[string][]string{"train": s.TrainEvents, "validation": s.ValidationEvents, "test": s.TestEvents, "purged": s.PurgedEvents} {
		for _, id := range ids {
			if prior := owner[id]; prior != "" {
				t.Fatalf("event %s crossed %s/%s", id, prior, label)
			}
			owner[id] = label
		}
	}
	if len(owner) != 10 || len(s.PurgedEvents) != 2 || len(s.TestEvents) == 0 {
		t.Fatalf("unexpected split: %+v", s)
	}
}

func TestChronologicalEventSplitPurgesLateSortedSpanningEvent(t *testing.T) {
	base := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	rows := []Observation{}
	for i := 0; i < 10; i++ {
		rows = append(rows, Observation{EventID: string(rune('A' + i)), Day: base.AddDate(0, 0, i), Value: 1})
	}
	// Z sorts late by its last day but began in the train window. It must never enter validation
	// or untouched test merely because a safe, shorter event sorted before it.
	rows = append(rows,
		Observation{EventID: "Z-spanning", Day: base.AddDate(0, 0, 1), Value: 1},
		Observation{EventID: "Z-spanning", Day: base.AddDate(0, 0, 7), Value: 1})
	s, err := ChronologicalEventSplit(rows, .35, .2, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range append(append([]string{}, s.ValidationEvents...), s.TestEvents...) {
		if id == "Z-spanning" {
			t.Fatalf("spanning event leaked across chronological partition: %+v", s)
		}
	}
}

func TestDeterministicDayBlockBoundsKeepsZeroDays(t *testing.T) {
	days := []float64{1, 0, -1, 0, 2, 0, -0.5}
	a, err := DeterministicDayBlockBounds(days, 2000, 138)
	if err != nil {
		t.Fatal(err)
	}
	b, err := DeterministicDayBlockBounds(days, 2000, 138)
	if err != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("bootstrap not deterministic: %+v %+v err=%v", a, b, err)
	}
	if a.Days != len(days) || a.Lower > a.Mean || a.Upper < a.Mean {
		t.Fatalf("bad bounds: %+v", a)
	}
}

func TestDeterministicDaySignPIsDeterministicAndOneSided(t *testing.T) {
	days := []float64{1, 0, 1, 0, 1, 0, 1, 0}
	a, err := DeterministicDaySignP(days, 4000, 138)
	if err != nil {
		t.Fatal(err)
	}
	b, err := DeterministicDaySignP(days, 4000, 138)
	if err != nil || a != b || a <= 0 || a > 1 {
		t.Fatalf("invalid deterministic p-value a=%v b=%v err=%v", a, b, err)
	}
	if p, err := DeterministicDaySignP([]float64{-1, 0, -2}, 1000, 1); err != nil || p != 1 {
		t.Fatalf("non-positive mean must not produce positive-edge evidence: p=%v err=%v", p, err)
	}
}

func TestHierarchicalShrinkagePenalizesSparseCells(t *testing.T) {
	r, err := ShrinkNormalCells([]CellEstimate{
		{Cell: "large", N: 100, Mean: .10, Variance: .04},
		{Cell: "singleton", N: 1, Mean: .90, Variance: .25},
		{Cell: "middle", N: 20, Mean: .05, Variance: .05},
	}, 1e-6)
	if err != nil {
		t.Fatal(err)
	}
	var large, singleton CellEstimate
	for _, c := range r.Cells {
		if c.Cell == "large" {
			large = c
		}
		if c.Cell == "singleton" {
			singleton = c
		}
	}
	if singleton.ShrinkageWeight >= large.ShrinkageWeight || math.Abs(singleton.PosteriorMean-r.GlobalMean) >= math.Abs(singleton.Mean-r.GlobalMean) {
		t.Fatalf("sparse cell not shrunk: global=%v large=%+v singleton=%+v", r.GlobalMean, large, singleton)
	}
}

func TestDependenceSafeAdjustments(t *testing.T) {
	out, err := AdjustDependenceSafe([]TestP{{ID: "a", P: .01}, {ID: "b", P: .03}, {ID: "c", P: .20}})
	if err != nil {
		t.Fatal(err)
	}
	for i := range out {
		if out[i].HolmP < out[i].P || out[i].BYQ < out[i].P || out[i].HolmP > 1 || out[i].BYQ > 1 {
			t.Fatalf("invalid adjusted row: %+v", out[i])
		}
		if i > 0 && (out[i].HolmP < out[i-1].HolmP || out[i].BYQ < out[i-1].BYQ) {
			t.Fatalf("adjustment not monotone: %+v", out)
		}
	}
}
