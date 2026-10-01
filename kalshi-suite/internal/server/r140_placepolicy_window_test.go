package server

import (
	"context"
	"testing"
)

func TestPlacementPolicyEmptyWindowBuildsDefaultTable(t *testing.T) {
	s := testServer(t)
	s.rebuildPlacementPolicy(context.Background())
	rows, builtAt := s.policyView()
	if builtAt.IsZero() || len(rows) != len(policyFamilies) {
		t.Fatalf("healthy empty window must build the default table: built=%v rows=%d want=%d", !builtAt.IsZero(), len(rows), len(policyFamilies))
	}
	for _, row := range rows {
		if row.N != 0 {
			t.Fatalf("empty policy window produced evidence for %s: n=%d", row.Family, row.N)
		}
		want := "on"
		for _, pf := range policyFamilies {
			if pf.fam == row.Family && pf.research {
				want = "research"
				break
			}
		}
		if row.Status != want {
			t.Fatalf("%s status=%s, want %s", row.Family, row.Status, want)
		}
	}
}
