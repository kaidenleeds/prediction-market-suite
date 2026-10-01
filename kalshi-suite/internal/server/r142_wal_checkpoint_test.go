package server

import (
	"testing"
	"time"
)

func TestR142OversizedWALRetriesBeforeTenMinutes(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	last := now.Add(-2 * time.Minute)
	if !walTruncateDue(last, now, 256.01) {
		t.Fatal("oversized WAL must retry on the next two-minute sweep")
	}
	if walTruncateDue(last, now, 256) {
		t.Fatal("bounded WAL must retain the ordinary ten-minute truncate cadence")
	}
	if !walTruncateDue(now.Add(-10*time.Minute), now, 1) {
		t.Fatal("ordinary ten-minute truncate cadence was lost")
	}
	if !walTruncateDue(time.Time{}, now, 0) {
		t.Fatal("cold boot must attempt an initial truncate")
	}
	if got := walTruncateBudget(256.01); got != 10*time.Second {
		t.Fatalf("oversized retry budget=%s, want 10s", got)
	}
	if got := walTruncateBudget(256); got != 2*time.Second {
		t.Fatalf("ordinary retry budget=%s, want 2s", got)
	}
}
