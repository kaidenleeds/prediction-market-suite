package researchreplay

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Writer struct {
	mu           sync.Mutex
	cfg          Config
	index        Index
	pending      []Frame
	pendingBytes int
	closed       bool
}

func emptyIndex() Index {
	return Index{Format: FormatVersion, NextSequence: 1, Segments: []SegmentMeta{}}
}

func Open(cfg Config) (*Writer, error) {
	cfg = cfg.normalized()
	if cfg.Dir == "" {
		return nil, fmt.Errorf("research replay directory is required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, "segments"), 0o750); err != nil {
		return nil, err
	}
	cleanupReplayTemps(cfg.Dir)
	idx, err := loadIndex(cfg.Dir)
	if err != nil {
		return nil, err
	}
	// Runtime Open verifies the integrity-protected manifest plus only the newest eight compressed
	// segments. A full 4,032-segment/2GB decompression on the suite thread after every restart would
	// violate research-load isolation. VerifyDirectory remains the explicit offline full-chain drill.
	report, err := VerifyDirectoryRecent(cfg.Dir, 8)
	if err != nil {
		return nil, fmt.Errorf("verify recent research replay tail before open: %w", err)
	}
	if report.HeadSegmentHash != idx.HeadSegmentHash || report.HeadFrameHash != idx.HeadFrameHash ||
		report.NextSequence != idx.NextSequence {
		return nil, fmt.Errorf("research replay index changed during bounded open verification")
	}
	w := &Writer{cfg: cfg, index: idx}
	if removed := rotateIndex(&w.index, cfg); len(removed) > 0 {
		w.index.Generation++
		if err := writeIndex(cfg.Dir, &w.index); err != nil {
			return nil, err
		}
		removeSegments(cfg.Dir, removed)
	}
	return w, nil
}

func (w *Writer) Queue(frame Frame) (bool, error) {
	return w.QueueBatch([]Frame{frame})
}

// QueueBatch validates and copies a selected batch atomically in memory. It never writes a file;
// callers flush on a bounded cadence or when shouldFlush is true. This prevents a book-tick loop
// from turning the replay archive into another synchronous write path.
func (w *Writer) QueueBatch(frames []Frame) (shouldFlush bool, err error) {
	if len(frames) == 0 {
		return false, nil
	}
	prepared := make([]Frame, 0, len(frames))
	bytesN := 0
	for _, frame := range frames {
		copyFrame, n, err := validateAndCopyFrame(frame, w.cfg.MaxFrameBytes)
		if err != nil {
			return false, err
		}
		prepared, bytesN = append(prepared, copyFrame), bytesN+n
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return false, ErrClosed
	}
	if len(prepared) > w.cfg.MaxBatchFrames || bytesN > w.cfg.MaxBatchBytes {
		return false, fmt.Errorf("%w: incoming frames=%d bytes=%d", ErrBatchFull, len(prepared), bytesN)
	}
	if len(w.pending)+len(prepared) > w.cfg.MaxBatchFrames || w.pendingBytes+bytesN > w.cfg.MaxBatchBytes {
		return true, ErrBatchFull
	}
	w.pending = append(w.pending, prepared...)
	w.pendingBytes += bytesN
	return len(w.pending) >= w.cfg.MaxBatchFrames || w.pendingBytes >= w.cfg.MaxBatchBytes, nil
}

func (w *Writer) Pending() (frames, bytes int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending), w.pendingBytes
}

func shaHex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func appendJSONLine(buf *bytes.Buffer, value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	buf.Write(b)
	buf.WriteByte('\n')
	return b, nil
}

func sourceTimeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func writeGzipAtomic(path string, plain []byte) (int64, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return 0, err
	}
	f, err := os.CreateTemp(dir, ".replay-*.tmp")
	if err != nil {
		return 0, err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	zw, err := gzip.NewWriterLevel(f, gzip.BestSpeed)
	if err != nil {
		return 0, err
	}
	zw.Header.ModTime = time.Unix(0, 0).UTC()
	zw.Header.OS = 255
	if _, err := zw.Write(plain); err != nil {
		_ = zw.Close()
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	ok = true
	return info.Size(), nil
}

func (w *Writer) Flush(ctx context.Context) (SegmentMeta, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return SegmentMeta{}, false, ErrClosed
	}
	if len(w.pending) == 0 {
		return SegmentMeta{}, false, nil
	}
	if err := ctx.Err(); err != nil {
		return SegmentMeta{}, false, err
	}
	now := time.Now().UTC()
	hour := now.Truncate(time.Hour)
	firstSequence := w.index.NextSequence
	segmentID := fmt.Sprintf("%s-%020d-%d", hour.Format("20060102T15"), firstSequence, now.UnixNano())
	header := segmentHeader{RecordType: "header", Format: FormatVersion, SegmentID: segmentID,
		HourUTC: hour.Format(time.RFC3339), CreatedAt: now.Format(time.RFC3339Nano),
		PreviousSegmentHash: w.index.HeadSegmentHash, PreviousFrameHash: w.index.HeadFrameHash,
		FirstSequence: firstSequence}
	var content bytes.Buffer
	if _, err := appendJSONLine(&content, header); err != nil {
		return SegmentMeta{}, false, err
	}
	previousFrameHash := w.index.HeadFrameHash
	firstFrameHash := ""
	frameKinds := map[string]int{}
	for i, frame := range w.pending {
		if err := ctx.Err(); err != nil {
			return SegmentMeta{}, false, err
		}
		recorded := now.Add(time.Duration(i) * time.Nanosecond).Format(time.RFC3339Nano)
		rec := frameRecord{RecordType: "frame", Sequence: firstSequence + uint64(i), RecordedAt: recorded,
			Source: frame.Source, SchemaVersion: frame.SchemaVersion, Kind: frame.Kind, EntityID: frame.EntityID,
			CanonicalEventID: frame.CanonicalEventID, OpportunityID: frame.OpportunityID, Selection: frame.Selection,
			SourceAt: sourceTimeString(frame.SourceAt), ObservedAt: sourceTimeString(frame.ObservedAt),
			SourceSequence: frame.SourceSequence, PriorSourceSequence: frame.PriorSourceSequence,
			SequenceGap: frame.SequenceGap, SequenceState: frame.SequenceState, SourceClock: frame.SourceClock,
			Payload: frame.Payload, ResearchOnly: frame.ResearchOnly, Funded: frame.Funded,
			PaperAuthority: frame.PaperAuthority, LiveAuthority: frame.LiveAuthority,
			PreviousHash: previousFrameHash}
		material, err := json.Marshal(rec.hashMaterial())
		if err != nil {
			return SegmentMeta{}, false, err
		}
		rec.Hash = shaHex(material)
		if firstFrameHash == "" {
			firstFrameHash = rec.Hash
		}
		previousFrameHash = rec.Hash
		frameKinds[replayKindBucket(frame.Kind)]++
		if _, err := appendJSONLine(&content, rec); err != nil {
			return SegmentMeta{}, false, err
		}
	}
	segmentHash := shaHex(content.Bytes())
	lastSequence := firstSequence + uint64(len(w.pending)) - 1
	trailer := segmentTrailer{RecordType: "trailer", SegmentHash: segmentHash, FrameCount: len(w.pending),
		LastSequence: lastSequence, LastFrameHash: previousFrameHash, UncompressedSize: content.Len(),
		FrameKinds: frameKinds}
	plain := append([]byte(nil), content.Bytes()...)
	trailerLine, err := json.Marshal(trailer)
	if err != nil {
		return SegmentMeta{}, false, err
	}
	plain = append(plain, trailerLine...)
	plain = append(plain, '\n')
	rel := filepath.Join("segments", hour.Format("2006"), hour.Format("01"), hour.Format("02"),
		hour.Format("15"), fmt.Sprintf("%020d_%s.jsonl.gz", firstSequence, segmentHash[:16]))
	finalPath := filepath.Join(w.cfg.Dir, rel)
	compressedBytes, err := writeGzipAtomic(finalPath, plain)
	if err != nil {
		return SegmentMeta{}, false, err
	}
	meta := SegmentMeta{SegmentID: segmentID, RelativePath: filepath.ToSlash(rel), HourUTC: header.HourUTC,
		CreatedAt: header.CreatedAt, FirstSequence: firstSequence, LastSequence: lastSequence,
		Frames: len(w.pending), CompressedBytes: compressedBytes, UncompressedBytes: len(plain),
		PreviousSegmentHash: header.PreviousSegmentHash, SegmentHash: segmentHash,
		FirstFrameHash: firstFrameHash, LastFrameHash: previousFrameHash, FrameKinds: frameKinds}
	next := w.index
	next.Segments = append(append([]SegmentMeta(nil), w.index.Segments...), meta)
	next.Generation++
	next.NextSequence = lastSequence + 1
	next.HeadSegmentHash, next.HeadFrameHash = segmentHash, previousFrameHash
	next.TotalCompressedBytes += compressedBytes
	removed := rotateIndex(&next, w.cfg)
	if err := writeIndex(w.cfg.Dir, &next); err != nil {
		_ = os.Remove(finalPath)
		return SegmentMeta{}, false, err
	}
	w.index = next
	w.pending, w.pendingBytes = nil, 0
	removeSegments(w.cfg.Dir, removed)
	return meta, true, nil
}

func (w *Writer) AppendBatch(ctx context.Context, frames []Frame) (SegmentMeta, error) {
	if _, err := w.QueueBatch(frames); err != nil {
		return SegmentMeta{}, err
	}
	meta, wrote, err := w.Flush(ctx)
	if err != nil {
		return SegmentMeta{}, err
	}
	if !wrote {
		return SegmentMeta{}, fmt.Errorf("research replay batch was empty")
	}
	return meta, nil
}

func (w *Writer) SnapshotIndex() Index {
	w.mu.Lock()
	defer w.mu.Unlock()
	idx := w.index
	idx.Segments = append([]SegmentMeta(nil), w.index.Segments...)
	return idx
}

func (w *Writer) Verify() (VerificationReport, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return verifyIndex(w.cfg.Dir, w.index)
}

// Close deliberately does not flush. The owner chooses an explicit bounded flush point; shutdown
// must never unexpectedly turn an unreviewed in-memory payload into durable research evidence.
func (w *Writer) Close() {
	w.mu.Lock()
	w.closed = true
	w.pending, w.pendingBytes = nil, 0
	w.mu.Unlock()
}
