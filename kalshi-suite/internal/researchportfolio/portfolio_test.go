package researchportfolio

import (
	"fmt"
	"math"
	"reflect"
	"testing"
)

func TestEVISchedulerRespectsBudgetsAndNegativeWork(t *testing.T) {
	s, err := ScheduleEVI([]ResearchTask{
		{ID: "repair-high-value", ProbabilityChangesDecision: .8, DecisionValueDollarsPerDay: 10, ComputeMinutes: 4, APICalls: 5},
		{ID: "millionth-tick", ProbabilityChangesDecision: .01, DecisionValueDollarsPerDay: 1, ComputeMinutes: 1, APICalls: 1},
		{ID: "blocked", ProbabilityChangesDecision: 1, DecisionValueDollarsPerDay: 100, Blocked: true, BlockReason: "missing official source"},
		{ID: "second", ProbabilityChangesDecision: .5, DecisionValueDollarsPerDay: 8, ComputeMinutes: 4, APICalls: 6},
	}, 5, 6, .1, .01)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Selected) != 1 || s.Selected[0].ID != "repair-high-value" || s.ComputeMinutes > 5 || s.APICalls > 6 {
		t.Fatalf("schedule=%+v", s)
	}
	if s.Rejected["millionth-tick"] == "" || s.Rejected["blocked"] == "" || s.Rejected["second"] == "" {
		t.Fatalf("missing rejection truth: %+v", s.Rejected)
	}
}

func TestCauseGraphDoesNotDoubleCountSharedAlpha(t *testing.T) {
	clusters, err := CauseGraph([]SystemExposure{
		{SystemID: "gap", Venue: "kalshi", CauseIDs: []string{"game:1", "cross-venue"}, NetPerDayLower: 5, Capacity: 10},
		{SystemID: "bridge", Venue: "polyus", CauseIDs: []string{"game:1", "bridge"}, NetPerDayLower: 4, Capacity: 8},
		{SystemID: "weather", Venue: "kalshi", CauseIDs: []string{"station:KORD"}, NetPerDayLower: 2, Capacity: 3},
	})
	if err != nil || len(clusters) != 2 {
		t.Fatalf("clusters=%+v err=%v", clusters, err)
	}
	for _, c := range clusters {
		if len(c.Systems) == 2 && (c.StandaloneNetPerDayLowerSum != 9 || c.ConservativeSharedCauseNetPerDay != 5) {
			t.Fatalf("shared cause double-counted: %+v", c)
		}
	}
}

func TestEVISchedulerHasBoundedWorkAtTwentyFourTasks(t *testing.T) {
	tasks := make([]ResearchTask, 24)
	for i := range tasks {
		tasks[i] = ResearchTask{ID: string(rune('a' + i)), ProbabilityChangesDecision: .5,
			DecisionValueDollarsPerDay: float64(i + 1), ComputeMinutes: 1, APICalls: 1}
	}
	s, err := ScheduleEVI(tasks, 12, 12, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if s.Exact || s.Method != "bounded-multi-order-heuristic" || s.StatesEvaluated > 4*len(tasks) {
		t.Fatalf("unbounded or mislabeled schedule: %+v", s)
	}
	if len(s.Selected) != 12 {
		t.Fatalf("selected %d tasks, want 12", len(s.Selected))
	}
}

func TestR139EVISchedulerIsDeterministicAboveSixtyFourTasks(t *testing.T) {
	tasks := make([]ResearchTask, 100)
	for i := range tasks {
		tasks[i] = ResearchTask{ID: fmt.Sprintf("task-%03d", i), ProbabilityChangesDecision: .5,
			DecisionValueDollarsPerDay: float64(i + 1), ComputeMinutes: 1, APICalls: 1}
	}
	a, err := ScheduleEVI(tasks, 10, 10, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ScheduleEVI(tasks, 10, 10, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) || len(a.Selected) != 10 || a.StatesEvaluated > 4*len(tasks) {
		t.Fatalf("large deterministic bounded schedule mismatch: a=%+v b=%+v", a, b)
	}
}

func TestEVISchedulerRejectsNonFiniteFrozenInputs(t *testing.T) {
	for _, task := range []ResearchTask{
		{ID: "nan-prob", ProbabilityChangesDecision: math.NaN(), DecisionValueDollarsPerDay: 1},
		{ID: "inf-value", ProbabilityChangesDecision: .5, DecisionValueDollarsPerDay: math.Inf(1)},
	} {
		if _, err := ScheduleEVI([]ResearchTask{task}, 1, 1, 0, 0); err == nil {
			t.Fatalf("non-finite frozen input accepted: %+v", task)
		}
	}
	if _, err := ScheduleEVI(nil, math.NaN(), 1, 0, 0); err == nil {
		t.Fatal("non-finite budget accepted")
	}
}
