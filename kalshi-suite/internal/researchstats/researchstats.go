// Package researchstats contains deterministic, research-only statistical contracts.  It has no
// dependency on order placement, bankrolls, or venue clients.
package researchstats

import (
	"errors"
	"math"
	"math/rand"
	"sort"
	"time"
)

type Observation struct {
	EventID string
	Day     time.Time
	Cell    string
	Value   float64
}

type Split struct {
	Train, Validation, Test, Purged                         []Observation
	TrainEvents, ValidationEvents, TestEvents, PurgedEvents []string
}

type eventGroup struct {
	id          string
	first, last time.Time
	rows        []Observation
}

// ChronologicalEventSplit assigns a canonical event to exactly one partition, then purges whole
// events inside the requested calendar embargo.  It never row-splits one ticker/game across train,
// validation, and untouched test.
func ChronologicalEventSplit(rows []Observation, trainFraction, validationFraction float64, embargoDays int) (Split, error) {
	if trainFraction <= 0 || validationFraction <= 0 || trainFraction+validationFraction >= 1 || embargoDays < 0 {
		return Split{}, errors.New("invalid chronological split contract")
	}
	byID := make(map[string]*eventGroup)
	for _, r := range rows {
		if r.EventID == "" || r.Day.IsZero() || math.IsNaN(r.Value) || math.IsInf(r.Value, 0) {
			return Split{}, errors.New("invalid clustered observation")
		}
		d := r.Day.UTC().Truncate(24 * time.Hour)
		r.Day = d
		g := byID[r.EventID]
		if g == nil {
			g = &eventGroup{id: r.EventID, first: d, last: d}
			byID[r.EventID] = g
		}
		if d.Before(g.first) {
			g.first = d
		}
		if d.After(g.last) {
			g.last = d
		}
		g.rows = append(g.rows, r)
	}
	groups := make([]*eventGroup, 0, len(byID))
	for _, g := range byID {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].last.Equal(groups[j].last) {
			return groups[i].id < groups[j].id
		}
		return groups[i].last.Before(groups[j].last)
	})
	if len(groups) < 3 {
		return Split{}, errors.New("at least three canonical events are required")
	}
	nTrain := int(math.Floor(float64(len(groups)) * trainFraction))
	nVal := int(math.Floor(float64(len(groups)) * validationFraction))
	if nTrain < 1 {
		nTrain = 1
	}
	if nVal < 1 {
		nVal = 1
	}
	if nTrain+nVal >= len(groups) {
		return Split{}, errors.New("split leaves no untouched events")
	}
	embargo := time.Duration(embargoDays) * 24 * time.Hour
	var out Split
	appendGroup := func(dstRows *[]Observation, dstIDs *[]string, g *eventGroup) {
		*dstRows = append(*dstRows, g.rows...)
		*dstIDs = append(*dstIDs, g.id)
	}
	for _, g := range groups[:nTrain] {
		appendGroup(&out.Train, &out.TrainEvents, g)
	}
	// A long-running event can start before a boundary even when its final observation sorts after
	// an otherwise safe event.  Therefore each remaining event is checked independently; stopping
	// the purge at the first safe event would allow a later long-running event to leak across the
	// train/validation boundary.
	trainBoundary := groups[nTrain-1].last.Add(embargo)
	idx := nTrain
	for idx < len(groups) && len(out.ValidationEvents) < nVal {
		g := groups[idx]
		idx++
		if !g.first.After(trainBoundary) {
			appendGroup(&out.Purged, &out.PurgedEvents, g)
			continue
		}
		appendGroup(&out.Validation, &out.ValidationEvents, g)
	}
	if len(out.ValidationEvents) == 0 || idx >= len(groups) {
		return Split{}, errors.New("embargo leaves no validation or untouched test")
	}
	valLast := time.Time{}
	for _, r := range out.Validation {
		if r.Day.After(valLast) {
			valLast = r.Day
		}
	}
	valBoundary := valLast.Add(embargo)
	for ; idx < len(groups); idx++ {
		g := groups[idx]
		if !g.first.After(valBoundary) {
			appendGroup(&out.Purged, &out.PurgedEvents, g)
			continue
		}
		appendGroup(&out.Test, &out.TestEvents, g)
	}
	if len(out.TestEvents) == 0 {
		return Split{}, errors.New("embargo leaves no untouched test")
	}
	return out, nil
}

// DeterministicDaySignP returns a one-sided, day-block randomization p-value for a positive mean.
// Complete UTC days are the exchangeable blocks, so same-day event/route dependence is never
// split.  It is deliberately deterministic for an immutable research receipt and is not an
// execution or promotion authority.
func DeterministicDaySignP(dayTotals []float64, replicates int, seed int64) (float64, error) {
	if len(dayTotals) < 2 || replicates < 100 {
		return 1, errors.New("insufficient deterministic sign-test contract")
	}
	observed := 0.0
	for _, v := range dayTotals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 1, errors.New("non-finite day total")
		}
		observed += v
	}
	observed /= float64(len(dayTotals))
	if observed <= 0 {
		return 1, nil
	}
	rng := rand.New(rand.NewSource(seed))
	extreme := 0
	for b := 0; b < replicates; b++ {
		v := 0.0
		for _, day := range dayTotals {
			if rng.Intn(2) == 0 {
				v -= day
			} else {
				v += day
			}
		}
		if v/float64(len(dayTotals)) >= observed-1e-15 {
			extreme++
		}
	}
	return float64(extreme+1) / float64(replicates+1), nil
}

type Bounds struct {
	Mean, Lower, Upper float64
	Days, Replicates   int
}

// DeterministicDayBlockBounds resamples complete UTC-day totals. Callers must include quiet days as
// explicit zeroes. Same-day/event dependence is therefore kept inside one block.
func DeterministicDayBlockBounds(dayTotals []float64, replicates int, seed int64) (Bounds, error) {
	if len(dayTotals) < 2 || replicates < 100 {
		return Bounds{}, errors.New("insufficient deterministic bootstrap contract")
	}
	mean := 0.0
	for _, v := range dayTotals {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return Bounds{}, errors.New("non-finite day total")
		}
		mean += v
	}
	mean /= float64(len(dayTotals))
	rng := rand.New(rand.NewSource(seed))
	boot := make([]float64, replicates)
	for b := range boot {
		total := 0.0
		for range dayTotals {
			total += dayTotals[rng.Intn(len(dayTotals))]
		}
		boot[b] = total / float64(len(dayTotals))
	}
	sort.Float64s(boot)
	quantile := func(p float64) float64 {
		idx := int(math.Floor(p * float64(len(boot)-1)))
		return boot[idx]
	}
	return Bounds{Mean: mean, Lower: quantile(.05), Upper: quantile(.95), Days: len(dayTotals), Replicates: replicates}, nil
}

type CellEstimate struct {
	Cell                 string
	N                    int
	Mean, Variance       float64
	PosteriorMean        float64
	PosteriorSE, Lower95 float64
	ShrinkageWeight      float64
}

type HierarchicalResult struct {
	GlobalMean, BetweenVariance float64
	Cells                       []CellEstimate
}

// ShrinkNormalCells performs a conservative empirical-Bayes normal/normal partial pooling pass.
// It is a descriptive stabilizer, not a promotion test; untouched replication remains mandatory.
func ShrinkNormalCells(cells []CellEstimate, varianceFloor float64) (HierarchicalResult, error) {
	if len(cells) == 0 || varianceFloor <= 0 {
		return HierarchicalResult{}, errors.New("invalid shrinkage contract")
	}
	totalN, global := 0, 0.0
	for _, c := range cells {
		if c.Cell == "" || c.N <= 0 || c.Variance < 0 || math.IsNaN(c.Mean) || math.IsInf(c.Mean, 0) {
			return HierarchicalResult{}, errors.New("invalid cell estimate")
		}
		totalN += c.N
		global += float64(c.N) * c.Mean
	}
	global /= float64(totalN)
	between, noiseWeight := 0.0, 0.0
	for _, c := range cells {
		w := float64(c.N)
		between += w * (c.Mean - global) * (c.Mean - global)
		noiseWeight += w * math.Max(c.Variance, varianceFloor) / float64(c.N)
	}
	between = math.Max(0, (between-noiseWeight)/float64(totalN))
	// A zero estimate means the cells have not demonstrated heterogeneity. Keep a small proper
	// prior variance so the result remains finite and sparse cells still shrink heavily.
	tau2 := math.Max(between, varianceFloor/float64(totalN))
	out := HierarchicalResult{GlobalMean: global, BetweenVariance: between, Cells: make([]CellEstimate, len(cells))}
	for i, c := range cells {
		sampleVar := math.Max(c.Variance, varianceFloor) / float64(c.N)
		weight := tau2 / (tau2 + sampleVar)
		postVar := 1 / (1/tau2 + 1/sampleVar)
		c.PosteriorMean = weight*c.Mean + (1-weight)*global
		c.PosteriorSE = math.Sqrt(postVar)
		c.Lower95 = c.PosteriorMean - 1.6448536269514722*c.PosteriorSE
		c.ShrinkageWeight = weight
		out.Cells[i] = c
	}
	sort.Slice(out.Cells, func(i, j int) bool { return out.Cells[i].Cell < out.Cells[j].Cell })
	return out, nil
}

type TestP struct {
	ID    string
	P     float64
	HolmP float64
	BYQ   float64
}

// AdjustDependenceSafe returns Holm family-wise adjusted p-values and Benjamini-Yekutieli q-values,
// both valid without assuming independent prediction-market contracts.
func AdjustDependenceSafe(tests []TestP) ([]TestP, error) {
	if len(tests) == 0 {
		return nil, errors.New("empty multiplicity family")
	}
	out := append([]TestP(nil), tests...)
	seen := map[string]bool{}
	for _, t := range out {
		if t.ID == "" || seen[t.ID] || t.P < 0 || t.P > 1 || math.IsNaN(t.P) {
			return nil, errors.New("invalid multiplicity row")
		}
		seen[t.ID] = true
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].P == out[j].P {
			return out[i].ID < out[j].ID
		}
		return out[i].P < out[j].P
	})
	m := len(out)
	prev := 0.0
	harmonic := 0.0
	for i := 1; i <= m; i++ {
		harmonic += 1 / float64(i)
	}
	for i := range out {
		adj := math.Min(1, float64(m-i)*out[i].P)
		if adj < prev {
			adj = prev
		}
		out[i].HolmP, prev = adj, adj
	}
	next := 1.0
	for i := m - 1; i >= 0; i-- {
		q := math.Min(1, out[i].P*float64(m)*harmonic/float64(i+1))
		if q > next {
			q = next
		}
		out[i].BYQ, next = q, q
	}
	return out, nil
}
