package server

import (
	"math"
	"sort"
)

type exitClusterObservation struct {
	Cluster string
	Day     string
	Net     float64
}

type exitClusterBounds struct {
	N, Clusters, Days, Replicates int
	Mean, Lower95, Upper95        float64
	ClusterRule                   string
}

// clusteredExitBounds resamples whole ticker/event proxies, never individual linked rows. Legacy
// path rows do not carry the R138 canonical event ID, so the response labels ticker grouping as a
// conservative proxy rather than claiming exact event isolation.
func clusteredExitBounds(rows []exitClusterObservation, replicates int) exitClusterBounds {
	out := exitClusterBounds{N: len(rows), Replicates: replicates,
		ClusterRule: "venue+ticker proxy; legacy exit rows lack canonical event IDs"}
	if len(rows) == 0 || replicates < 100 {
		return out
	}
	type aggregate struct {
		sum float64
		n   int
	}
	byCluster := map[string]aggregate{}
	days := map[string]bool{}
	validN := 0
	for _, row := range rows {
		if row.Cluster == "" || math.IsNaN(row.Net) || math.IsInf(row.Net, 0) {
			continue
		}
		a := byCluster[row.Cluster]
		a.sum, a.n = a.sum+row.Net, a.n+1
		byCluster[row.Cluster] = a
		validN++
		if row.Day != "" {
			days[row.Day] = true
		}
		out.Mean += row.Net
	}
	out.N, out.Clusters, out.Days = validN, len(byCluster), len(days)
	if validN > 0 {
		out.Mean /= float64(validN)
	}
	if len(byCluster) < 2 {
		return out
	}
	keys := make([]string, 0, len(byCluster))
	for key := range byCluster {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	boot := make([]float64, replicates)
	state := uint64(0x1385eed)
	next := func() uint64 {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		return state
	}
	for b := range boot {
		total, n := 0.0, 0
		for range keys {
			a := byCluster[keys[int(next()%uint64(len(keys)))]]
			total, n = total+a.sum, n+a.n
		}
		if n > 0 {
			boot[b] = total / float64(n)
		}
	}
	sort.Float64s(boot)
	out.Lower95 = boot[int(.025*float64(len(boot)-1))]
	out.Upper95 = boot[int(.975*float64(len(boot)-1))]
	return out
}
