// Package researchportfolio schedules research and groups systems by economic cause. It is
// research governance only and has no order, bankroll, or authority dependency.
package researchportfolio

import (
	"errors"
	"math"
	"sort"
)

type ResearchTask struct {
	ID                         string
	ProbabilityChangesDecision float64
	DecisionValueDollarsPerDay float64
	CollectionCostDollars      float64
	ComputeMinutes, APICalls   float64
	Blocked                    bool
	BlockReason                string
}

type ScheduledTask struct {
	ResearchTask
	EVI, Score float64
}

type Schedule struct {
	Selected                 []ScheduledTask
	TotalEVI                 float64
	ComputeMinutes, APICalls float64
	Rejected                 map[string]string
	Method                   string
	Exact                    bool
	StatesEvaluated          int
}

func taskEVI(t ResearchTask, computeDollarPerMinute, apiDollarPerCall float64) float64 {
	return t.ProbabilityChangesDecision*t.DecisionValueDollarsPerDay - t.CollectionCostDollars -
		t.ComputeMinutes*computeDollarPerMinute - t.APICalls*apiDollarPerCall
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// ScheduleEVI solves small 0/1 research-budget requests exactly and uses a disclosed, deterministic
// multi-order heuristic for larger requests. The HTTP path must have a hard work bound: enumerating
// large powersets synchronously could starve the suite. The heuristic uses a boolean selection
// vector, so it remains correct above 64 tasks without bit-mask overflow. Negative-EVI or blocked work is retained as a
// rejection, not silently scheduled because it sounds novel.
func ScheduleEVI(tasks []ResearchTask, computeBudget, apiBudget, computeDollarPerMinute, apiDollarPerCall float64) (Schedule, error) {
	if len(tasks) > 500 || !finite(computeBudget) || !finite(apiBudget) ||
		!finite(computeDollarPerMinute) || !finite(apiDollarPerCall) ||
		computeBudget < 0 || apiBudget < 0 || computeDollarPerMinute < 0 || apiDollarPerCall < 0 {
		return Schedule{}, errors.New("invalid EVI schedule contract")
	}
	out := Schedule{Rejected: map[string]string{}}
	var eligible []ScheduledTask
	seen := map[string]bool{}
	for _, t := range tasks {
		if t.ID == "" || seen[t.ID] || !finite(t.ProbabilityChangesDecision) ||
			!finite(t.DecisionValueDollarsPerDay) || !finite(t.CollectionCostDollars) ||
			!finite(t.ComputeMinutes) || !finite(t.APICalls) ||
			t.ProbabilityChangesDecision < 0 || t.ProbabilityChangesDecision > 1 ||
			t.DecisionValueDollarsPerDay < 0 || t.CollectionCostDollars < 0 || t.ComputeMinutes < 0 || t.APICalls < 0 {
			return Schedule{}, errors.New("invalid EVI task")
		}
		seen[t.ID] = true
		if t.Blocked {
			out.Rejected[t.ID] = "blocked: " + t.BlockReason
			continue
		}
		evi := taskEVI(t, computeDollarPerMinute, apiDollarPerCall)
		if evi <= 0 {
			out.Rejected[t.ID] = "non-positive expected value of information"
			continue
		}
		denom := 1 + t.ComputeMinutes/math.Max(1, computeBudget) + t.APICalls/math.Max(1, apiBudget)
		eligible = append(eligible, ScheduledTask{ResearchTask: t, EVI: evi, Score: evi / denom})
	}
	bestSelected, bestEVI := make([]bool, len(eligible)), 0.0
	if len(eligible) <= 16 {
		out.Method, out.Exact = "exact-enumeration", true
		bestMask := uint64(0)
		for mask := uint64(1); mask < uint64(1)<<len(eligible); mask++ {
			out.StatesEvaluated++
			compute, api, evi := 0.0, 0.0, 0.0
			for i, task := range eligible {
				if mask&(uint64(1)<<i) != 0 {
					compute, api, evi = compute+task.ComputeMinutes, api+task.APICalls, evi+task.EVI
				}
			}
			if compute <= computeBudget && api <= apiBudget && evi > bestEVI+1e-12 {
				bestMask, bestEVI = mask, evi
			}
		}
		for i := range eligible {
			bestSelected[i] = bestMask&(uint64(1)<<i) != 0
		}
	} else {
		out.Method, out.Exact = "bounded-multi-order-heuristic", false
		// Try several transparent priority orders. This is not mislabeled as an optimum; it is a
		// bounded research queue recommendation whose frozen inputs remain visible to the caller.
		orders := make([][]int, 4)
		for k := range orders {
			orders[k] = make([]int, len(eligible))
			for i := range eligible {
				orders[k][i] = i
			}
		}
		less := []func(a, b ScheduledTask) bool{
			func(a, b ScheduledTask) bool { return a.Score > b.Score },
			func(a, b ScheduledTask) bool { return a.EVI > b.EVI },
			func(a, b ScheduledTask) bool { return a.EVI/(1+a.ComputeMinutes) > b.EVI/(1+b.ComputeMinutes) },
			func(a, b ScheduledTask) bool { return a.EVI/(1+a.APICalls) > b.EVI/(1+b.APICalls) },
		}
		for k := range orders {
			sort.SliceStable(orders[k], func(i, j int) bool {
				a, b := eligible[orders[k][i]], eligible[orders[k][j]]
				if less[k](a, b) == less[k](b, a) {
					return a.ID < b.ID
				}
				return less[k](a, b)
			})
			candidate := make([]bool, len(eligible))
			compute, api, evi := 0.0, 0.0, 0.0
			for _, i := range orders[k] {
				out.StatesEvaluated++
				task := eligible[i]
				if compute+task.ComputeMinutes <= computeBudget && api+task.APICalls <= apiBudget {
					candidate[i] = true
					compute, api, evi = compute+task.ComputeMinutes, api+task.APICalls, evi+task.EVI
				}
			}
			if evi > bestEVI+1e-12 {
				bestEVI = evi
				copy(bestSelected, candidate)
			}
		}
	}
	selected := map[string]bool{}
	for i, task := range eligible {
		if bestSelected[i] {
			out.Selected = append(out.Selected, task)
			out.TotalEVI += task.EVI
			out.ComputeMinutes += task.ComputeMinutes
			out.APICalls += task.APICalls
			selected[task.ID] = true
		}
	}
	for _, task := range eligible {
		if !selected[task.ID] {
			out.Rejected[task.ID] = "positive EVI but displaced by higher-value work within compute/API budget"
		}
	}
	sort.Slice(out.Selected, func(i, j int) bool {
		if out.Selected[i].Score == out.Selected[j].Score {
			return out.Selected[i].ID < out.Selected[j].ID
		}
		return out.Selected[i].Score > out.Selected[j].Score
	})
	return out, nil
}

type SystemExposure struct {
	SystemID, RouteID, Venue string
	CauseIDs                 []string
	NetPerDayLower, Capacity float64
	CapitalDollarHoursPerDay float64
}

type CauseCluster struct {
	ClusterID                        string
	Systems, Causes, Venues          []string
	StandaloneNetPerDayLowerSum      float64
	ConservativeSharedCauseNetPerDay float64
	CapacitySum, CapitalDollarHours  float64
}

// CauseGraph groups any systems connected by a shared underlying economic cause. To avoid
// double-counting the same alpha, the conservative cluster value is the largest positive member
// lower bound (plus every negative member), not the sum of correlated positives.
func CauseGraph(rows []SystemExposure) ([]CauseCluster, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	parent := make([]int, len(rows))
	for i := range parent {
		parent[i] = i
	}
	var find func(int) int
	find = func(x int) int {
		if parent[x] != x {
			parent[x] = find(parent[x])
		}
		return parent[x]
	}
	union := func(a, b int) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[rb] = ra
		}
	}
	causeOwner := map[string]int{}
	seenRoute := map[string]bool{}
	for i, row := range rows {
		key := row.SystemID + "\x00" + row.RouteID
		if row.SystemID == "" || seenRoute[key] || len(row.CauseIDs) == 0 ||
			!finite(row.NetPerDayLower) || !finite(row.Capacity) || !finite(row.CapitalDollarHoursPerDay) ||
			row.Capacity < 0 || row.CapitalDollarHoursPerDay < 0 {
			return nil, errors.New("invalid cause-graph row")
		}
		seenRoute[key] = true
		for _, cause := range row.CauseIDs {
			if cause == "" {
				return nil, errors.New("blank economic cause")
			}
			if j, ok := causeOwner[cause]; ok {
				union(i, j)
			} else {
				causeOwner[cause] = i
			}
		}
	}
	byRoot := map[int][]int{}
	for i := range rows {
		byRoot[find(i)] = append(byRoot[find(i)], i)
	}
	var out []CauseCluster
	for _, idxs := range byRoot {
		systems, causes, venues := map[string]bool{}, map[string]bool{}, map[string]bool{}
		cluster := CauseCluster{}
		bestPositive := 0.0
		for _, i := range idxs {
			row := rows[i]
			systems[row.SystemID], venues[row.Venue] = true, true
			for _, cause := range row.CauseIDs {
				causes[cause] = true
			}
			cluster.StandaloneNetPerDayLowerSum += row.NetPerDayLower
			cluster.CapacitySum += row.Capacity
			cluster.CapitalDollarHours += row.CapitalDollarHoursPerDay
			if row.NetPerDayLower > bestPositive {
				bestPositive = row.NetPerDayLower
			}
			if row.NetPerDayLower < 0 {
				cluster.ConservativeSharedCauseNetPerDay += row.NetPerDayLower
			}
		}
		cluster.ConservativeSharedCauseNetPerDay += bestPositive
		for s := range systems {
			cluster.Systems = append(cluster.Systems, s)
		}
		for c := range causes {
			cluster.Causes = append(cluster.Causes, c)
		}
		for v := range venues {
			cluster.Venues = append(cluster.Venues, v)
		}
		sort.Strings(cluster.Systems)
		sort.Strings(cluster.Causes)
		sort.Strings(cluster.Venues)
		cluster.ClusterID = cluster.Causes[0]
		out = append(out, cluster)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClusterID < out[j].ClusterID })
	return out, nil
}
