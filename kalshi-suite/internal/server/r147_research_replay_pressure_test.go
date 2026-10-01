package server

import (
	"context"
	"errors"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/researchreplay"
)

func TestR147ReplayFlushGetsBoundedDurabilityAfterLaneCancellation(t *testing.T) {
	w, err := researchreplay.Open(researchreplay.Config{
		Dir: t.TempDir(), MaxSegments: 4, MaxBatchFrames: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	frame, err := researchreplay.NewFrame("test", "r147", "governance", "cancelled-lane",
		"prove a cancelled analytics lane cannot strand the replay writer", map[string]any{"ok": true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Queue(frame); err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	meta, wrote, err := flushResearchReplayDurably(parent, w)
	if err != nil || !wrote || meta.Frames != 1 {
		t.Fatalf("bounded durability flush = wrote %v frames %d err %v", wrote, meta.Frames, err)
	}
	if frames, bytes := w.Pending(); frames != 0 || bytes != 0 {
		t.Fatalf("cancelled lane stranded pending replay batch: frames=%d bytes=%d", frames, bytes)
	}
}

func TestR147LifecyclePersistenceRetriesOnlyTransientWriterContention(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, errors.New("database is locked (5)"),
		errors.New("SQLITE_BUSY: database is busy")} {
		if !lifecyclePersistenceRetryable(err) {
			t.Fatalf("transient writer failure was not retryable: %v", err)
		}
	}
	for _, err := range []error{errors.New("database is closed"), errors.New("disk is full"),
		errors.New("constraint failed"), errors.New("invalid lifecycle row")} {
		if lifecyclePersistenceRetryable(err) {
			t.Fatalf("permanent persistence failure would retry forever: %v", err)
		}
	}
	busy := errors.New("SQLITE_BUSY: database is busy")
	if !lifecyclePersistenceShouldRetry(lifecyclePersistenceMaxAttempts-1, busy) {
		t.Fatal("transient lifecycle writer contention stopped before the bounded retry budget")
	}
	if lifecyclePersistenceShouldRetry(lifecyclePersistenceMaxAttempts, busy) {
		t.Fatal("persistent lifecycle writer contention could hold the worker forever")
	}
}
