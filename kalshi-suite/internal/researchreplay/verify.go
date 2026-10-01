package researchreplay

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func indexIntegrityHash(idx Index) (string, error) {
	idx.IntegrityHash = ""
	b, err := json.Marshal(idx)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func loadIndex(dir string) (Index, error) {
	path := filepath.Join(dir, IndexFileName)
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return emptyIndex(), nil
	}
	if err != nil {
		return Index{}, err
	}
	var idx Index
	if err := json.Unmarshal(b, &idx); err != nil {
		return Index{}, fmt.Errorf("research replay index JSON: %w", err)
	}
	if idx.Format != FormatVersion || idx.NextSequence == 0 {
		return Index{}, fmt.Errorf("research replay index format=%q next_sequence=%d", idx.Format, idx.NextSequence)
	}
	want, err := indexIntegrityHash(idx)
	if err != nil {
		return Index{}, err
	}
	if idx.IntegrityHash == "" || idx.IntegrityHash != want {
		return Index{}, fmt.Errorf("research replay index integrity hash mismatch")
	}
	if idx.Segments == nil {
		idx.Segments = []SegmentMeta{}
	}
	return idx, nil
}

func writeIndex(dir string, idx *Index) error {
	idx.Format = FormatVersion
	if idx.NextSequence == 0 {
		idx.NextSequence = 1
	}
	idx.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	h, err := indexIntegrityHash(*idx)
	if err != nil {
		return err
	}
	idx.IntegrityHash = h
	b, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, IndexFileName)
	f, err := os.CreateTemp(dir, ".replay-index-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		_ = f.Close()
		if !ok {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	return nil
}

func rotateIndex(idx *Index, cfg Config) []SegmentMeta {
	var removed []SegmentMeta
	for len(idx.Segments) > 1 &&
		(len(idx.Segments) > cfg.MaxSegments || idx.TotalCompressedBytes > cfg.MaxBytes) {
		old := idx.Segments[0]
		removed = append(removed, old)
		idx.Segments = append([]SegmentMeta(nil), idx.Segments[1:]...)
		idx.TotalCompressedBytes -= old.CompressedBytes
		idx.AnchorSegmentHash, idx.AnchorFrameHash = old.SegmentHash, old.LastFrameHash
	}
	if idx.TotalCompressedBytes < 0 {
		idx.TotalCompressedBytes = 0
	}
	return removed
}

func removeSegments(dir string, segments []SegmentMeta) {
	for _, segment := range segments {
		_ = os.Remove(filepath.Join(dir, filepath.FromSlash(segment.RelativePath)))
	}
}

func safeSegmentPath(dir, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return "", fmt.Errorf("invalid replay segment path %q", relative)
	}
	base, err := filepath.Abs(filepath.Join(dir, "segments"))
	if err != nil {
		return "", err
	}
	full, err := filepath.Abs(filepath.Join(dir, filepath.FromSlash(relative)))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(base, full)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("replay segment escapes archive root: %q", relative)
	}
	return full, nil
}

func verifySegment(dir string, meta SegmentMeta, expectedSegment, expectedFrame string, expectedSequence uint64) error {
	if meta.PreviousSegmentHash != expectedSegment || meta.FirstSequence != expectedSequence {
		return fmt.Errorf("segment %s manifest chain/sequence mismatch", meta.SegmentID)
	}
	path, err := safeSegmentPath(dir, meta.RelativePath)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if info, err := f.Stat(); err != nil || info.Size() != meta.CompressedBytes {
		if err != nil {
			return err
		}
		return fmt.Errorf("segment %s compressed size mismatch", meta.SegmentID)
	}
	zr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("segment %s gzip: %w", meta.SegmentID, err)
	}
	defer zr.Close()
	plain, err := io.ReadAll(io.LimitReader(zr, 128<<20))
	if err != nil {
		return fmt.Errorf("segment %s decompress: %w", meta.SegmentID, err)
	}
	if len(plain) >= 128<<20 {
		return fmt.Errorf("segment %s exceeds decompression bound", meta.SegmentID)
	}
	if len(plain) != meta.UncompressedBytes {
		return fmt.Errorf("segment %s uncompressed size mismatch", meta.SegmentID)
	}
	scanner := bufio.NewScanner(bytes.NewReader(plain))
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	if !scanner.Scan() {
		return fmt.Errorf("segment %s missing header", meta.SegmentID)
	}
	headerLine := append([]byte(nil), scanner.Bytes()...)
	var header segmentHeader
	if err := json.Unmarshal(headerLine, &header); err != nil {
		return fmt.Errorf("segment %s header: %w", meta.SegmentID, err)
	}
	if header.RecordType != "header" || header.Format != FormatVersion || header.SegmentID != meta.SegmentID ||
		header.PreviousSegmentHash != expectedSegment || header.PreviousFrameHash != expectedFrame ||
		header.FirstSequence != expectedSequence || header.HourUTC != meta.HourUTC {
		return fmt.Errorf("segment %s header truth mismatch", meta.SegmentID)
	}
	var hashed bytes.Buffer
	hashed.Write(headerLine)
	hashed.WriteByte('\n')
	previousFrameHash := expectedFrame
	frameCount := 0
	frameKinds := map[string]int{}
	firstFrameHash := ""
	var trailer *segmentTrailer
	for scanner.Scan() {
		line := append([]byte(nil), scanner.Bytes()...)
		var kind struct {
			RecordType string `json:"record_type"`
		}
		if err := json.Unmarshal(line, &kind); err != nil {
			return fmt.Errorf("segment %s record JSON: %w", meta.SegmentID, err)
		}
		if kind.RecordType == "trailer" {
			var t segmentTrailer
			if err := json.Unmarshal(line, &t); err != nil {
				return err
			}
			trailer = &t
			if scanner.Scan() {
				return fmt.Errorf("segment %s has records after trailer", meta.SegmentID)
			}
			break
		}
		if kind.RecordType != "frame" || trailer != nil {
			return fmt.Errorf("segment %s unexpected record type %q", meta.SegmentID, kind.RecordType)
		}
		var rec frameRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return err
		}
		if !rec.ResearchOnly || rec.Funded || rec.PaperAuthority || rec.LiveAuthority {
			return fmt.Errorf("segment %s frame %d carries trading authority", meta.SegmentID, frameCount)
		}
		sequenceState, sequenceGap := replaySequenceTruth(rec.SourceSequence, rec.PriorSourceSequence)
		if rec.SourceSequence == nil && rec.PriorSourceSequence == nil && rec.SequenceState == "not_applicable" {
			sequenceState = "not_applicable"
		}
		if rec.SequenceState != sequenceState || rec.SequenceGap != sequenceGap {
			return fmt.Errorf("segment %s frame %d sequence truth mismatch", meta.SegmentID, frameCount)
		}
		switch rec.SourceClock {
		case "venue":
			if rec.SourceAt == "" {
				return fmt.Errorf("segment %s frame %d missing venue source clock", meta.SegmentID, frameCount)
			}
		case "arrival_only", "missing":
			if rec.SourceAt != "" {
				return fmt.Errorf("segment %s frame %d invents source time", meta.SegmentID, frameCount)
			}
		case "not_applicable":
			// Governance frames have neither a venue nor an arrival-only source clock.
			if rec.SourceAt != "" {
				return fmt.Errorf("segment %s frame %d has source time without a source clock", meta.SegmentID, frameCount)
			}
		default:
			return fmt.Errorf("segment %s frame %d has invalid source clock", meta.SegmentID, frameCount)
		}
		wantSequence := expectedSequence + uint64(frameCount)
		if rec.Sequence != wantSequence || rec.PreviousHash != previousFrameHash {
			return fmt.Errorf("segment %s frame %d chain/sequence mismatch", meta.SegmentID, frameCount)
		}
		payload, err := canonicalPayload(rec.Payload)
		if err != nil || !bytes.Equal(payload, rec.Payload) {
			if err != nil {
				return fmt.Errorf("segment %s frame %d payload: %w", meta.SegmentID, frameCount, err)
			}
			return fmt.Errorf("segment %s frame %d payload is not canonical", meta.SegmentID, frameCount)
		}
		material, _ := json.Marshal(rec.hashMaterial())
		if shaHex(material) != rec.Hash {
			return fmt.Errorf("segment %s frame %d hash mismatch", meta.SegmentID, frameCount)
		}
		if firstFrameHash == "" {
			firstFrameHash = rec.Hash
		}
		previousFrameHash = rec.Hash
		frameKinds[replayKindBucket(rec.Kind)]++
		frameCount++
		hashed.Write(line)
		hashed.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if trailer == nil {
		return fmt.Errorf("segment %s missing trailer", meta.SegmentID)
	}
	segmentHash := shaHex(hashed.Bytes())
	lastSequence := expectedSequence + uint64(frameCount) - 1
	if frameCount <= 0 || trailer.SegmentHash != segmentHash || meta.SegmentHash != segmentHash ||
		trailer.FrameCount != frameCount || meta.Frames != frameCount || trailer.LastSequence != lastSequence ||
		meta.LastSequence != lastSequence || trailer.LastFrameHash != previousFrameHash ||
		meta.LastFrameHash != previousFrameHash || meta.FirstFrameHash != firstFrameHash ||
		trailer.UncompressedSize != hashed.Len() || !sameKindCounts(frameKinds, meta.FrameKinds) ||
		!sameKindCounts(frameKinds, trailer.FrameKinds) {
		return fmt.Errorf("segment %s trailer/manifest integrity mismatch", meta.SegmentID)
	}
	return nil
}

func sameKindCounts(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func addKindCounts(report *VerificationReport, counts map[string]int) {
	if report.FrameKindCounts == nil {
		report.FrameKindCounts = map[string]int{}
	}
	for key, value := range counts {
		report.FrameKindCounts[key] += value
	}
}

func finishKindCounts(report *VerificationReport) {
	report.FrameKinds = report.FrameKinds[:0]
	for _, kind := range []string{"book", "trade", "lifecycle", "source", "governance"} {
		if report.FrameKindCounts[kind] > 0 {
			report.FrameKinds = append(report.FrameKinds, kind)
		}
	}
	if len(report.FrameKinds) == 0 && len(report.FrameKindCounts) > 0 {
		for kind := range report.FrameKindCounts {
			report.FrameKinds = append(report.FrameKinds, kind)
		}
		sort.Strings(report.FrameKinds)
	}
	report.SelectedKinds = append(report.SelectedKinds[:0], report.FrameKinds...)
}

func countReplayFiles(dir string, idx Index) (orphans, temps int) {
	known := make(map[string]bool, len(idx.Segments))
	for _, segment := range idx.Segments {
		known[filepath.Clean(filepath.Join(dir, filepath.FromSlash(segment.RelativePath)))] = true
	}
	_ = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		name := strings.ToLower(entry.Name())
		if strings.HasSuffix(name, ".tmp") || strings.Contains(name, ".tmp-") || strings.HasPrefix(name, ".replay-") {
			temps++
		}
		if strings.HasSuffix(name, ".jsonl.gz") && !known[filepath.Clean(path)] {
			orphans++
		}
		return nil
	})
	return orphans, temps
}

func cleanupReplayTemps(dir string) {
	_ = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		name := strings.ToLower(entry.Name())
		if (strings.HasPrefix(name, ".replay-") || strings.HasPrefix(name, ".replay-index-")) && strings.HasSuffix(name, ".tmp") {
			_ = os.Remove(path)
		}
		return nil
	})
}

func verifyIndex(dir string, idx Index) (VerificationReport, error) {
	report := VerificationReport{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Format: idx.Format,
		Segments: len(idx.Segments), CompressedBytes: idx.TotalCompressedBytes,
		AnchorSegmentHash: idx.AnchorSegmentHash, HeadSegmentHash: idx.HeadSegmentHash,
		HeadFrameHash: idx.HeadFrameHash, NextSequence: idx.NextSequence,
		ResearchOnly: true, Funded: false, PaperAuthority: false, LiveAuthority: false,
		VerificationScope: "full"}
	fail := func(err error) (VerificationReport, error) {
		report.Healthy, report.Error = false, err.Error()
		return report, err
	}
	if idx.Format != FormatVersion || idx.NextSequence == 0 {
		return fail(fmt.Errorf("invalid replay index format/sequence"))
	}
	if idx.IntegrityHash != "" {
		want, err := indexIntegrityHash(idx)
		if err != nil || want != idx.IntegrityHash {
			if err == nil {
				err = fmt.Errorf("replay index integrity mismatch")
			}
			return fail(err)
		}
	}
	expectedSegment, expectedFrame := idx.AnchorSegmentHash, idx.AnchorFrameHash
	expectedSequence := uint64(1)
	if len(idx.Segments) > 0 {
		expectedSequence = idx.Segments[0].FirstSequence
		report.OldestHour = idx.Segments[0].HourUTC
		report.NewestHour = idx.Segments[len(idx.Segments)-1].HourUTC
	}
	var bytesN int64
	for _, segment := range idx.Segments {
		if err := verifySegment(dir, segment, expectedSegment, expectedFrame, expectedSequence); err != nil {
			return fail(err)
		}
		report.Frames += segment.Frames
		addKindCounts(&report, segment.FrameKinds)
		bytesN += segment.CompressedBytes
		expectedSegment, expectedFrame = segment.SegmentHash, segment.LastFrameHash
		expectedSequence = segment.LastSequence + 1
	}
	report.VerifiedSegments = len(idx.Segments)
	finishKindCounts(&report)
	if bytesN != idx.TotalCompressedBytes || expectedSegment != idx.HeadSegmentHash ||
		expectedFrame != idx.HeadFrameHash || expectedSequence != idx.NextSequence {
		return fail(fmt.Errorf("replay index aggregate/head mismatch"))
	}
	report.OrphanSegments, report.TemporaryFiles = countReplayFiles(dir, idx)
	report.Healthy = report.OrphanSegments == 0 && report.TemporaryFiles == 0
	if !report.Healthy {
		report.Error = fmt.Sprintf("orphan_segments=%d temporary_files=%d", report.OrphanSegments, report.TemporaryFiles)
		return report, fmt.Errorf("research replay has %s", report.Error)
	}
	return report, nil
}

// VerifyDirectoryRecent verifies the integrity-protected manifest and only the newest bounded
// number of compressed segments. Writer Open and explicit offline checks still run the full chain;
// a dashboard refresh must not decompress thousands of immutable files on every GET.
func VerifyDirectoryRecent(dir string, maxSegments int) (VerificationReport, error) {
	if maxSegments <= 0 {
		maxSegments = 128
	}
	idx, err := loadIndex(dir)
	if err != nil {
		report := VerificationReport{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Format: FormatVersion, ResearchOnly: true, Error: err.Error(), VerificationScope: "recent"}
		return report, err
	}
	report := VerificationReport{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Format: idx.Format,
		Segments: len(idx.Segments), CompressedBytes: idx.TotalCompressedBytes,
		AnchorSegmentHash: idx.AnchorSegmentHash, HeadSegmentHash: idx.HeadSegmentHash,
		HeadFrameHash: idx.HeadFrameHash, NextSequence: idx.NextSequence,
		ResearchOnly: true, VerificationScope: "manifest+recent"}
	fail := func(cause error) (VerificationReport, error) {
		report.Healthy, report.Error = false, cause.Error()
		return report, cause
	}
	if idx.Format != FormatVersion || idx.NextSequence == 0 {
		return fail(fmt.Errorf("invalid replay index format/sequence"))
	}
	expectedSegment, expectedFrame := idx.AnchorSegmentHash, idx.AnchorFrameHash
	expectedSequence := uint64(1)
	if len(idx.Segments) > 0 {
		expectedSequence = idx.Segments[0].FirstSequence
		report.OldestHour, report.NewestHour = idx.Segments[0].HourUTC, idx.Segments[len(idx.Segments)-1].HourUTC
	}
	var compressed int64
	for _, segment := range idx.Segments {
		if segment.PreviousSegmentHash != expectedSegment || segment.FirstSequence != expectedSequence ||
			segment.LastSequence < segment.FirstSequence || segment.Frames != int(segment.LastSequence-segment.FirstSequence+1) {
			return fail(fmt.Errorf("segment %s manifest chain/sequence mismatch", segment.SegmentID))
		}
		report.Frames += segment.Frames
		addKindCounts(&report, segment.FrameKinds)
		compressed += segment.CompressedBytes
		expectedSegment, expectedFrame = segment.SegmentHash, segment.LastFrameHash
		expectedSequence = segment.LastSequence + 1
	}
	finishKindCounts(&report)
	if compressed != idx.TotalCompressedBytes || expectedSegment != idx.HeadSegmentHash ||
		expectedFrame != idx.HeadFrameHash || expectedSequence != idx.NextSequence {
		return fail(fmt.Errorf("replay index aggregate/head mismatch"))
	}
	start := len(idx.Segments) - maxSegments
	if start < 0 {
		start = 0
	}
	expectedSegment, expectedFrame = idx.AnchorSegmentHash, idx.AnchorFrameHash
	if start > 0 {
		previous := idx.Segments[start-1]
		expectedSegment, expectedFrame = previous.SegmentHash, previous.LastFrameHash
	}
	for _, segment := range idx.Segments[start:] {
		if err := verifySegment(dir, segment, expectedSegment, expectedFrame, segment.FirstSequence); err != nil {
			return fail(err)
		}
		expectedSegment, expectedFrame = segment.SegmentHash, segment.LastFrameHash
		report.VerifiedSegments++
	}
	report.OrphanSegments, report.TemporaryFiles = countReplayFiles(dir, idx)
	report.Healthy = report.OrphanSegments == 0 && report.TemporaryFiles == 0
	if !report.Healthy {
		report.Error = fmt.Sprintf("orphan_segments=%d temporary_files=%d", report.OrphanSegments, report.TemporaryFiles)
		return report, fmt.Errorf("research replay has %s", report.Error)
	}
	return report, nil
}

func VerifyDirectory(dir string) (VerificationReport, error) {
	idx, err := loadIndex(dir)
	if err != nil {
		report := VerificationReport{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
			Format: FormatVersion, ResearchOnly: true, Error: err.Error()}
		return report, err
	}
	return verifyIndex(dir, idx)
}
