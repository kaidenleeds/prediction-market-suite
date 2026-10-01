package researchreplay

import (
	"errors"
	"os"
	"testing"
	"time"
)

func sequencedFrame(t *testing.T, entity string, prior, current int64) Frame {
	t.Helper()
	f, err := NewFrame("kalshi-orderbook-ws", "book-normalized-v1", "book", entity,
		"top-of-book changed on the existing sequenced subscription",
		map[string]any{"yes_bid": .41, "yes_ask": .43})
	if err != nil {
		t.Fatal(err)
	}
	f.ObservedAt = time.Date(2026, 7, 12, 5, 0, int(current), 0, time.UTC)
	f.SourceAt = f.ObservedAt.Add(-time.Millisecond)
	f.SourceClock = "venue"
	f.SourceSequence = &current
	f.PriorSourceSequence = &prior
	f.SequenceState, f.SequenceGap = replaySequenceTruth(f.SourceSequence, f.PriorSourceSequence)
	return f
}

func TestRecentVerificationIsBoundedAndReportsSelectedKinds(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, MaxSegments: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	kinds := []string{"book", "trade", "lifecycle", "source-clock", "foundation"}
	for i, kind := range kinds {
		frame := sequencedFrame(t, string(rune('A'+i)), int64(i+1), int64(i+2))
		frame.Kind = kind
		if _, err := w.AppendBatch(t.Context(), []Frame{frame}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := VerifyDirectoryRecent(dir, 2)
	if err != nil || !report.Healthy || report.VerifiedSegments != 2 || report.VerificationScope != "manifest+recent" {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	for _, kind := range []string{"book", "trade", "lifecycle", "source", "governance"} {
		if report.FrameKindCounts[kind] != 1 {
			t.Fatalf("kind %s counts=%v", kind, report.FrameKindCounts)
		}
	}
	if len(report.FrameKinds) != 5 {
		t.Fatalf("kind list=%v", report.FrameKinds)
	}
	files := segmentFiles(t, dir)
	last := files[len(files)-1]
	b, err := os.ReadFile(last)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(last, b, 0o640); err != nil {
		t.Fatal(err)
	}
	if report, err = VerifyDirectoryRecent(dir, 2); err == nil || report.Healthy {
		t.Fatalf("tampered recent tail verified: %+v err=%v", report, err)
	}
}

func TestRuntimeOpenUsesBoundedTailAndFullAuditRemainsExplicit(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, MaxSegments: 20})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		frame := sequencedFrame(t, string(rune('A'+i)), int64(i+1), int64(i+2))
		if _, err := w.AppendBatch(t.Context(), []Frame{frame}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	files := segmentFiles(t, dir)
	b, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	if err := os.WriteFile(files[0], b, 0o640); err != nil {
		t.Fatal(err)
	}
	// Runtime startup verifies only the manifest plus newest eight files and therefore remains
	// bounded. The explicit offline full-chain drill still catches old-file tampering.
	reopened, err := Open(Config{Dir: dir, MaxSegments: 20})
	if err != nil {
		t.Fatalf("bounded runtime open scanned old segment: %v", err)
	}
	reopened.Close()
	if report, err := VerifyDirectory(dir); err == nil || report.Healthy {
		t.Fatalf("explicit full audit missed old tamper: report=%+v err=%v", report, err)
	}
}

func TestSelectedReplayPreservesExactSequenceGapAndClockTruth(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	contiguous := sequencedFrame(t, "KX-A", 10, 11)
	if _, err := w.Queue(contiguous); err != nil {
		t.Fatal(err)
	}
	gapped := sequencedFrame(t, "KX-B", 11, 15)
	if gapped.SequenceState != "gap" || gapped.SequenceGap != 3 {
		t.Fatalf("gap truth=%s/%d", gapped.SequenceState, gapped.SequenceGap)
	}
	if _, err := w.Queue(gapped); err != nil {
		t.Fatal(err)
	}
	bad := gapped
	bad.SequenceGap = 2
	if _, err := w.Queue(bad); err == nil {
		t.Fatal("inconsistent sequence gap accepted")
	}
	bad = gapped
	bad.SourceClock = "arrival_only"
	if _, err := w.Queue(bad); err == nil {
		t.Fatal("arrival-only frame claimed a venue source timestamp")
	}
}

func TestSelectedReplayQueueIsCoalescedAndHardBounded(t *testing.T) {
	q := NewSelectionQueue(2, 16<<10)
	if kept, err := q.Offer("book:A", sequencedFrame(t, "A", 1, 2), 50, 2); err != nil || !kept {
		t.Fatalf("kept=%v err=%v", kept, err)
	}
	if kept, err := q.Offer("book:A", sequencedFrame(t, "A", 2, 3), 50, 3); err != nil || !kept {
		t.Fatalf("replacement kept=%v err=%v", kept, err)
	}
	if kept, err := q.Offer("trade:B", sequencedFrame(t, "B", 3, 4), 20, 4); err != nil || !kept {
		t.Fatalf("second kept=%v err=%v", kept, err)
	}
	if kept, err := q.Offer("trade:C", sequencedFrame(t, "C", 4, 5), 10, 5); err != nil || kept {
		t.Fatalf("weaker overflow kept=%v err=%v", kept, err)
	}
	if kept, err := q.Offer("gap:C", sequencedFrame(t, "C", 5, 9), 100, 9); err != nil || !kept {
		t.Fatalf("critical gap kept=%v err=%v", kept, err)
	}
	stats := q.Stats()
	if stats.Pending != 2 || stats.Bytes <= 0 || stats.Dropped == 0 || stats.Evicted == 0 {
		t.Fatalf("stats=%+v", stats)
	}
	frames := q.Drain()
	if len(frames) != 2 || q.Stats().Pending != 0 {
		t.Fatalf("drained=%d after=%+v", len(frames), q.Stats())
	}
}

func TestSelectedReplayRejectsSecretsAndEveryTradingAuthority(t *testing.T) {
	q := NewSelectionQueue(4, 16<<10)
	secret := sequencedFrame(t, "secret", 1, 2)
	secret.Payload = []byte(`{"headers":{"Authorization":"Bearer bad"}}`)
	if _, err := q.Offer("secret", secret, 100, 1); !errors.Is(err, ErrSecretMaterial) {
		t.Fatalf("secret accepted: %v", err)
	}
	for _, mutate := range []func(*Frame){
		func(f *Frame) { f.ResearchOnly = false },
		func(f *Frame) { f.Funded = true },
		func(f *Frame) { f.PaperAuthority = true },
		func(f *Frame) { f.LiveAuthority = true },
	} {
		f := sequencedFrame(t, "authority", 1, 2)
		mutate(&f)
		if _, err := q.Offer("authority", f, 100, 1); err == nil {
			t.Fatalf("authority frame accepted: %+v", f)
		}
	}
}
