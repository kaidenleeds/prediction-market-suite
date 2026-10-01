package scorestate

import (
	"math"
	"reflect"
	"testing"
)

func TestFitLeaveOneOutIgnoresTargetQuoteAndIsDeterministic(t *testing.T) {
	rows := []Market{
		{ID: "home-win", Kind: "winner", Mid: .62, Predicate: Predicate{Axis: "margin", Threshold: 0, Greater: true}},
		{ID: "home-minus-3", Kind: "spread", Mid: .51, Predicate: Predicate{Axis: "margin", Threshold: 3, Greater: true}},
		{ID: "home-minus-7", Kind: "spread", Mid: .36, Predicate: Predicate{Axis: "margin", Threshold: 7, Greater: true}},
	}
	a, err := FitLeaveOneOut(rows, "home-win")
	if err != nil {
		t.Fatal(err)
	}
	rows[0].Mid = .03
	b, err := FitLeaveOneOut(rows, "home-win")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("target quote leaked into its fair fit:\n%+v\n%+v", a, b)
	}
	if a.JointClaim || a.TrainingN != 2 || a.Lower > a.Fair || a.Fair > a.Upper {
		t.Fatalf("bad conservative estimate: %+v", a)
	}
}

func TestCoherentMarginalOrdering(t *testing.T) {
	rows := []Market{
		{ID: "over-210", Kind: "total", Mid: .72, Predicate: Predicate{Axis: "total", Threshold: 210, Greater: true}},
		{ID: "over-220", Kind: "total", Mid: .50, Predicate: Predicate{Axis: "total", Threshold: 220, Greater: true}},
		{ID: "over-230", Kind: "total", Mid: .28, Predicate: Predicate{Axis: "total", Threshold: 230, Greater: true}},
		{ID: "target-low", Kind: "total", Mid: .01, Predicate: Predicate{Axis: "total", Threshold: 214, Greater: true}},
		{ID: "target-high", Kind: "total", Mid: .99, Predicate: Predicate{Axis: "total", Threshold: 226, Greater: true}},
	}
	low, err := FitLeaveOneOut(rows[:4], "target-low")
	if err != nil {
		t.Fatal(err)
	}
	highRows := append([]Market{}, rows[:3]...)
	highRows = append(highRows, rows[4])
	high, err := FitLeaveOneOut(highRows, "target-high")
	if err != nil {
		t.Fatal(err)
	}
	if !(low.Fair > high.Fair) {
		t.Fatalf("over probability must fall with threshold: low=%v high=%v", low.Fair, high.Fair)
	}
	if math.IsNaN(low.Fair) || math.IsNaN(high.Fair) {
		t.Fatal("non-finite fair")
	}
}

func TestUnsupportedPropsAndInsufficientAxisAreBlocked(t *testing.T) {
	if _, err := FitLeaveOneOut([]Market{{ID: "p", Kind: "prop", Mid: .5,
		Predicate: Predicate{Axis: "prop", Greater: true}}}, "p"); err == nil {
		t.Fatal("prop must not become an unsupported joint claim")
	}
	rows := []Market{
		{ID: "w", Kind: "winner", Mid: .6, Predicate: Predicate{Axis: "margin", Greater: true}},
		{ID: "s", Kind: "spread", Mid: .5, Predicate: Predicate{Axis: "margin", Threshold: 3, Greater: true}},
	}
	if _, err := FitLeaveOneOut(rows, "w"); err == nil {
		t.Fatal("one training constraint must be blocked")
	}
}

func TestEquivalentCrossVenueTargetIsAlsoLeftOut(t *testing.T) {
	rows := []Market{
		{ID: "k-home", Kind: "winner", Mid: .65, Predicate: Predicate{Axis: "margin", Threshold: 0, Greater: true}},
		{ID: "us-home", Kind: "winner", Mid: .02, Predicate: Predicate{Axis: "margin", Threshold: 0, Greater: true}},
		{ID: "minus-3", Kind: "spread", Mid: .55, Predicate: Predicate{Axis: "margin", Threshold: 3, Greater: true}},
		{ID: "minus-7", Kind: "spread", Mid: .35, Predicate: Predicate{Axis: "margin", Threshold: 7, Greater: true}},
	}
	a, err := FitLeaveOneOut(rows, "k-home")
	if err != nil {
		t.Fatal(err)
	}
	rows[1].Mid = .98
	b, err := FitLeaveOneOut(rows, "k-home")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) || a.TrainingN != 2 {
		t.Fatalf("equivalent target leaked into fit: a=%+v b=%+v", a, b)
	}
}
