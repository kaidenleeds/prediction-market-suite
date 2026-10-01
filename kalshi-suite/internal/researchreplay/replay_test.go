package researchreplay

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testFrame(t *testing.T, id string) Frame {
	t.Helper()
	f, err := NewFrame("kalshi-book-ws", "book-v2", "selected-book", id,
		"proper-score candidate entered the executable-book cohort",
		map[string]any{"yes_bid": .41, "yes_ask": .43, "depth": 12, "token_id": "public-market-token"})
	if err != nil {
		t.Fatal(err)
	}
	f.SourceAt = time.Date(2026, 7, 12, 3, 0, 0, 0, time.UTC)
	f.ObservedAt = f.SourceAt.Add(time.Second)
	f.SourceClock = "venue"
	f.OpportunityID = "opp-" + id
	return f
}

func segmentFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(filepath.Join(dir, "segments"), func(path string, entry os.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(entry.Name(), ".jsonl.gz") {
			out = append(out, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestReplayBatchesAreCompressedHashChainedAndReopenCleanly(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, MaxSegments: 10, MaxBatchFrames: 10})
	if err != nil {
		t.Fatal(err)
	}
	if should, err := w.Queue(testFrame(t, "A")); err != nil || should {
		t.Fatalf("queue should=%v err=%v", should, err)
	}
	if got := len(segmentFiles(t, dir)); got != 0 {
		t.Fatalf("Queue performed per-frame disk writes: %d files", got)
	}
	if _, wrote, err := w.Flush(context.Background()); err != nil || !wrote {
		t.Fatalf("flush wrote=%v err=%v", wrote, err)
	}
	if _, err := w.AppendBatch(context.Background(), []Frame{testFrame(t, "B"), testFrame(t, "C")}); err != nil {
		t.Fatal(err)
	}
	report, err := w.Verify()
	if err != nil || !report.Healthy || report.Segments != 2 || report.Frames != 3 || report.LiveAuthority {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	idx := w.SnapshotIndex()
	if idx.Segments[1].PreviousSegmentHash != idx.Segments[0].SegmentHash ||
		idx.Segments[1].FirstSequence != idx.Segments[0].LastSequence+1 || idx.NextSequence != 4 {
		t.Fatalf("index chain=%+v", idx)
	}
	w.Close()
	reopened, err := Open(Config{Dir: dir, MaxSegments: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if report, err = reopened.Verify(); err != nil || !report.Healthy || report.Frames != 3 {
		t.Fatalf("reopened report=%+v err=%v", report, err)
	}
}

func TestReplayRejectsAuthSecretsButAllowsPublicMarketTokenID(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	allowed := testFrame(t, "allowed")
	if _, err := w.Queue(allowed); err != nil {
		t.Fatalf("public token_id rejected: %v", err)
	}
	for _, payload := range []json.RawMessage{
		json.RawMessage(`{"Authorization":"Bearer abc"}`),
		json.RawMessage(`{"kalshi_access_signature":"abc"}`),
		json.RawMessage(`{"nested":{"private_key":"-----BEGIN PRIVATE KEY-----"}}`),
		json.RawMessage(`{"telegram_bot_token":"nope"}`),
	} {
		bad := testFrame(t, "bad")
		bad.Payload = payload
		if _, err := w.Queue(bad); !errors.Is(err, ErrSecretMaterial) {
			t.Fatalf("secret payload accepted: %s err=%v", payload, err)
		}
	}
}

func TestReplayRotationPreservesVerifiableAnchorAndBoundsFiles(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(Config{Dir: dir, MaxSegments: 2, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for i := 0; i < 5; i++ {
		if _, err := w.AppendBatch(context.Background(), []Frame{testFrame(t, string(rune('A'+i)))}); err != nil {
			t.Fatal(err)
		}
	}
	idx := w.SnapshotIndex()
	if len(idx.Segments) != 2 || idx.AnchorSegmentHash == "" || idx.AnchorFrameHash == "" ||
		idx.Segments[0].PreviousSegmentHash != idx.AnchorSegmentHash || idx.NextSequence != 6 {
		t.Fatalf("rotated index=%+v", idx)
	}
	if got := len(segmentFiles(t, dir)); got != 2 {
		t.Fatalf("segment files=%d want 2", got)
	}
	report, err := w.Verify()
	if err != nil || !report.Healthy || report.Segments != 2 || report.Frames != 2 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestReplayTamperAndIndexDriftFailVerification(t *testing.T) {
	t.Run("segment", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.AppendBatch(context.Background(), []Frame{testFrame(t, "A")}); err != nil {
			t.Fatal(err)
		}
		path := segmentFiles(t, dir)[0]
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		b[len(b)/2] ^= 0xff
		if err := os.WriteFile(path, b, 0o640); err != nil {
			t.Fatal(err)
		}
		if report, err := w.Verify(); err == nil || report.Healthy {
			t.Fatalf("tampered segment verified: %+v err=%v", report, err)
		}
	})
	t.Run("index", func(t *testing.T) {
		dir := t.TempDir()
		w, err := Open(Config{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := w.AppendBatch(context.Background(), []Frame{testFrame(t, "A")}); err != nil {
			t.Fatal(err)
		}
		w.Close()
		path := filepath.Join(dir, IndexFileName)
		b, _ := os.ReadFile(path)
		b = []byte(strings.Replace(string(b), `"next_sequence": 2`, `"next_sequence": 999`, 1))
		if err := os.WriteFile(path, b, 0o640); err != nil {
			t.Fatal(err)
		}
		if report, err := VerifyDirectory(dir); err == nil || report.Healthy {
			t.Fatalf("tampered index verified: %+v err=%v", report, err)
		}
	})
}

func TestReplayBatchBoundIsExplicitAndDoesNotDropPendingFrames(t *testing.T) {
	w, err := Open(Config{Dir: t.TempDir(), MaxBatchFrames: 2, MaxBatchBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	should, err := w.QueueBatch([]Frame{testFrame(t, "A"), testFrame(t, "B")})
	if err != nil || !should {
		t.Fatalf("should=%v err=%v", should, err)
	}
	if should, err = w.Queue(testFrame(t, "C")); !should || !errors.Is(err, ErrBatchFull) {
		t.Fatalf("overflow should=%v err=%v", should, err)
	}
	if frames, _ := w.Pending(); frames != 2 {
		t.Fatalf("pending=%d", frames)
	}
	if _, wrote, err := w.Flush(context.Background()); err != nil || !wrote {
		t.Fatalf("flush wrote=%v err=%v", wrote, err)
	}
	bad := testFrame(t, "D")
	bad.Selection = ""
	if _, err := w.Queue(bad); err == nil {
		t.Fatal("unselected raw frame accepted")
	}
}

func TestReplayReportsOrphansAndCleansOnlyAtomicTempDebris(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "segments"), 0o750); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, "segments", ".replay-crash.tmp")
	if err := os.WriteFile(tmp, []byte("partial"), 0o640); err != nil {
		t.Fatal(err)
	}
	w, err := Open(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("atomic temp debris was not cleaned: %v", err)
	}
	if _, err := w.AppendBatch(context.Background(), []Frame{testFrame(t, "A")}); err != nil {
		t.Fatal(err)
	}
	original := segmentFiles(t, dir)[0]
	b, err := os.ReadFile(original)
	if err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(filepath.Dir(original), "orphan-copy.jsonl.gz")
	if err := os.WriteFile(orphan, b, 0o640); err != nil {
		t.Fatal(err)
	}
	report, err := w.Verify()
	if err == nil || report.Healthy || report.OrphanSegments != 1 {
		t.Fatalf("orphan not surfaced: report=%+v err=%v", report, err)
	}
}
