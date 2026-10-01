package server

import "testing"

func TestClusteredExitBoundsResamplesClustersNotRows(t *testing.T) {
	rows := []exitClusterObservation{
		{Cluster: "event-a", Day: "2026-07-01", Net: 1},
		{Cluster: "event-a", Day: "2026-07-01", Net: 1},
		{Cluster: "event-b", Day: "2026-07-02", Net: -1},
	}
	a := clusteredExitBounds(rows, 1000)
	b := clusteredExitBounds(rows, 1000)
	if a.N != 3 || a.Clusters != 2 || a.Days != 2 || a.Mean != 1.0/3 || a != b {
		t.Fatalf("bounds not deterministic/clustered: a=%+v b=%+v", a, b)
	}
	if a.Lower95 >= 0 || a.Upper95 <= 0 {
		t.Fatalf("linked-row uncertainty was understated: %+v", a)
	}
}
