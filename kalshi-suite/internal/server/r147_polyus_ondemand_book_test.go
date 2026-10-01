package server

import (
	"fmt"
	"testing"
	"time"
)

func TestR147PolyUSOnDemandCacheIsBoundedToFreshFullBookReceipt(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	state := &polyUSOnDemandBookState{rows: map[string]polyUSOnDemandBookReceipt{
		"fresh": {at: now.Add(-polyUSOnDemandBookTTL + time.Millisecond)},
		"stale": {at: now.Add(-polyUSOnDemandBookTTL - time.Millisecond)},
	}}
	if _, ok := state.cached("fresh", now); !ok {
		t.Fatal("fresh authoritative overflow receipt was not reusable")
	}
	if _, ok := state.cached("stale", now); ok {
		t.Fatal("stale overflow receipt bypassed a current full-book fetch")
	}
}

func TestR147PolyUSOnDemandPrunesFailedSlugCooldowns(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	state := &polyUSOnDemandBookState{rows: map[string]polyUSOnDemandBookReceipt{},
		lastAttempt: map[string]time.Time{}}
	for i := 0; i < 300; i++ {
		key := fmt.Sprintf("failed-%03d", i)
		state.lastAttempt[key] = now.Add(-2 * time.Minute)
	}
	state.pruneLocked(now)
	if len(state.lastAttempt) != 0 {
		t.Fatalf("failed-slug cooldown map leaked %d stale keys", len(state.lastAttempt))
	}
}

func TestR147PolyUSOnDemandStateIsServerOwnedAndBounded(t *testing.T) {
	s := &Server{}
	state := s.polyUSOnDemandState()
	if state == nil || cap(state.slots) != polyUSOnDemandConcurrency ||
		state.rows == nil || state.inflight == nil || state.lastAttempt == nil {
		t.Fatalf("invalid bounded overflow state: %+v", state)
	}
	if again := s.polyUSOnDemandState(); again != state {
		t.Fatal("same server did not reuse its bounded overflow limiter/cache")
	}
}
