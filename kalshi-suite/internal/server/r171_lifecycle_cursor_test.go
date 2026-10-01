package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR171LifecycleCursorNeverAdvancesPastInterruptedRow(t *testing.T) {
	s := &Server{researchLifecycleCursor: 10}
	ctx := context.Background()
	if !s.completeResearchLifecycleRow(ctx, 11) {
		t.Fatal("completed lifecycle row did not advance the cursor")
	}
	if s.researchLifecycleCursor != 11 {
		t.Fatalf("cursor=%d want 11", s.researchLifecycleCursor)
	}

	interrupted, cancel := context.WithCancel(context.Background())
	cancel()
	if s.completeResearchLifecycleRow(interrupted, 12) {
		t.Fatal("interrupted lifecycle row advanced the cursor")
	}
	if s.researchLifecycleCursor != 11 {
		t.Fatalf("interrupted row skipped: cursor=%d want 11", s.researchLifecycleCursor)
	}
}

func TestR171LifecycleInterruptionRecognizesDeadlineAndCancellation(t *testing.T) {
	if !lifecycleSweepInterrupted(nil, nil) {
		t.Fatal("nil sweep context was treated as live")
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded} {
		if !lifecycleSweepInterrupted(context.Background(), err) {
			t.Fatalf("interruption %v was not recognized", err)
		}
	}
	if lifecycleSweepInterrupted(context.Background(), nil) {
		t.Fatal("healthy lifecycle sweep was treated as interrupted")
	}
}

func TestR171LifecycleUnavailableBookRetriesBeforeFixedHorizonExpires(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, _, err := s.store.InsertLifecycleResearchEvent(ctx, storage.LifecycleResearchEvent{
		Observed: now.Add(-7 * time.Second), Ticker: "KXR171-RETRY",
		EventType: "deactivated", RawHash: "r171-deactivated",
	}); err != nil {
		t.Fatal(err)
	}
	id, cohort, err := s.store.InsertLifecycleResearchEvent(ctx, storage.LifecycleResearchEvent{
		Observed: now.Add(-6 * time.Second), Ticker: "KXR171-RETRY",
		EventType: "activated", RawHash: "r171-activated",
	})
	if err != nil || !cohort || id <= 0 {
		t.Fatalf("cohort id=%d cohort=%t err=%v", id, cohort, err)
	}
	// testServer intentionally has no Kalshi client, so both WS and REST book sources are absent.
	// At age six seconds the 5-second horizon is still inside its capture tolerance and must stay
	// at the keyset head for the next 2-second retry instead of disappearing until catalog wrap.
	s.sweepLifecycleReopen(ctx)
	if s.researchLifecycleCursor != 0 {
		t.Fatalf("temporarily unavailable book advanced cursor=%d past row=%d",
			s.researchLifecycleCursor, id)
	}
	if have, err := s.store.HasLifecycleHorizon(ctx, id, 5); err != nil || have {
		t.Fatalf("temporary unavailability became terminal have=%t err=%v", have, err)
	}
}

func TestR171LifecycleFirstHorizonWaitDoesNotDependOnCatalogWrap(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if _, _, err := s.store.InsertLifecycleResearchEvent(ctx, storage.LifecycleResearchEvent{
		Observed: now.Add(-2 * time.Second), Ticker: "KXR171-WAIT",
		EventType: "deactivated", RawHash: "r171-wait-deactivated",
	}); err != nil {
		t.Fatal(err)
	}
	id, cohort, err := s.store.InsertLifecycleResearchEvent(ctx, storage.LifecycleResearchEvent{
		Observed: now.Add(-time.Second), Ticker: "KXR171-WAIT",
		EventType: "activated", RawHash: "r171-wait-activated",
	})
	if err != nil || !cohort || id <= 0 {
		t.Fatalf("cohort id=%d cohort=%t err=%v", id, cohort, err)
	}
	s.sweepLifecycleReopen(ctx)
	if s.researchLifecycleCursor != 0 {
		t.Fatalf("not-yet-due first horizon advanced cursor=%d past row=%d",
			s.researchLifecycleCursor, id)
	}
}
