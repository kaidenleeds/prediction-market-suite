package storage

import (
	"context"
	"testing"
	"time"
)

func TestOutcomeSetFrameFairCursorAndImmutablePair(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	t0 := time.Date(2026, 7, 12, 10, 0, 0, 0, time.UTC)
	base := OutcomeSetFrame{Observed: t0, SourceUpdated: t0, Venue: "kalshi", EventID: "EVT",
		SourceArtifact: "fixture", MutuallyExclusive: true, Exhaustive: true, VoidVerified: true,
		Members: []OutcomeSetMember{{Ticker: "A", Status: "active", Bid: .2, Ask: .25, Depth: 2},
			{Ticker: "B", Status: "active", Bid: .3, Ask: .35, Depth: 2},
			{Ticker: "C", Status: "active", Bid: .4, Ask: .45, Depth: 2}}}
	if got, err := st.InsertOutcomeSetFrame(ctx, base); err != nil || !got.Inserted || got.ChangeClass != "baseline" {
		t.Fatalf("baseline=%+v err=%v", got, err)
	}
	changed := base
	changed.Observed, changed.SourceUpdated = t0.Add(6*time.Minute), t0.Add(6*time.Minute)
	changed.Members = append([]OutcomeSetMember(nil), base.Members...)
	changed.Members[2].Status = "withdrawn"
	got, err := st.InsertOutcomeSetFrame(ctx, changed)
	if err != nil || !got.Inserted || got.ChangeClass != "status_change" {
		t.Fatalf("change=%+v err=%v", got, err)
	}
	pairs, err := st.OutcomeSetFrameChangesAfter(ctx, 0, 10)
	if err != nil || len(pairs) != 1 {
		t.Fatalf("pairs=%+v err=%v", pairs, err)
	}
	if pairs[0].Prior.ID >= pairs[0].Current.ID || pairs[0].Current.ChangeClass != "status_change" ||
		!pairs[0].Prior.Exhaustive || !pairs[0].Current.VoidVerified {
		t.Fatalf("bad immutable pair: %+v", pairs[0])
	}
	if err := st.AdvanceR139LatentCursor(ctx, "outcome", pairs[0].Current.ID); err != nil {
		t.Fatal(err)
	}
	// A stale overlapping worker cannot regress the bookmark.
	if err := st.AdvanceR139LatentCursor(ctx, "outcome", pairs[0].Prior.ID); err != nil {
		t.Fatal(err)
	}
	if cursor, err := st.R139LatentCursor(ctx, "outcome"); err != nil || cursor != pairs[0].Current.ID {
		t.Fatalf("cursor=%d err=%v", cursor, err)
	}
	if rows, err := st.OutcomeSetFrameChangesAfter(ctx, pairs[0].Current.ID, 10); err != nil || len(rows) != 0 {
		t.Fatalf("already processed change replayed: rows=%d err=%v", len(rows), err)
	}
}

func TestR139LatentCollectorSpecsAreImmutableAndDistinct(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR139LatentCollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	for _, spec := range R139LatentCollectorSpecs() {
		got, err := st.currentCollectorSpec(ctx, spec.CollectorID)
		if err != nil || got.Source != spec.Source || got.SchemaVersion != spec.SchemaVersion ||
			got.ExperimentID != spec.ExperimentID {
			t.Fatalf("collector %s got=%+v err=%v", spec.CollectorID, got, err)
		}
	}
}
