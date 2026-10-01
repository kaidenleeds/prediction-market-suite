// Package researchreplay stores selected, sanitized research state outside the trading database.
// Files are immutable gzip-compressed JSONL chunks grouped by UTC hour and chained by SHA-256.
// The package has no venue client, order, paper, LIVE, ARM, or SQLite dependency.
package researchreplay

import (
	"encoding/json"
	"errors"
	"time"
)

const (
	FormatVersion = "research-replay-v2"
	IndexFileName = "index.json"
)

var (
	ErrBatchFull      = errors.New("research replay batch full; flush before adding more selected frames")
	ErrClosed         = errors.New("research replay writer is closed")
	ErrSecretMaterial = errors.New("research replay frame contains forbidden credential/auth material")
)

type Config struct {
	Dir            string
	MaxSegments    int
	MaxBytes       int64
	MaxBatchFrames int
	MaxBatchBytes  int
	MaxFrameBytes  int
}

func (c Config) normalized() Config {
	if c.MaxSegments <= 0 {
		c.MaxSegments = 720
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = 2 << 30
	}
	if c.MaxBatchFrames <= 0 {
		c.MaxBatchFrames = 500
	}
	if c.MaxBatchBytes <= 0 {
		c.MaxBatchBytes = 8 << 20
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = 1 << 20
	}
	return c
}

// Frame is intentionally a selected research envelope, not a raw WebSocket/HTTP dump. Selection
// is required so callers must state why this frame deserves replay capacity. Payload must already
// omit headers, credentials and account secrets; Queue revalidates it before retaining any bytes.
type Frame struct {
	Source              string          `json:"source"`
	SchemaVersion       string          `json:"schema_version"`
	Kind                string          `json:"kind"`
	EntityID            string          `json:"entity_id"`
	CanonicalEventID    string          `json:"canonical_event_id,omitempty"`
	OpportunityID       string          `json:"opportunity_id,omitempty"`
	Selection           string          `json:"selection"`
	SourceAt            time.Time       `json:"source_at,omitempty"`
	ObservedAt          time.Time       `json:"observed_at"`
	SourceSequence      *int64          `json:"source_sequence,omitempty"`
	PriorSourceSequence *int64          `json:"prior_source_sequence,omitempty"`
	SequenceGap         int64           `json:"sequence_gap,omitempty"`
	SequenceState       string          `json:"sequence_state"`
	SourceClock         string          `json:"source_clock"`
	Payload             json.RawMessage `json:"payload"`
	ResearchOnly        bool            `json:"research_only"`
	Funded              bool            `json:"funded"`
	PaperAuthority      bool            `json:"paper_authority"`
	LiveAuthority       bool            `json:"live_authority"`
}

// NewFrame canonicalizes payload now, so mutable maps cannot change after they are queued.
func NewFrame(source, schemaVersion, kind, entityID, selection string, payload any) (Frame, error) {
	b, err := canonicalPayload(payload)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Source: source, SchemaVersion: schemaVersion, Kind: kind, EntityID: entityID,
		Selection: selection, ObservedAt: time.Now().UTC(), SourceClock: "not_applicable",
		SequenceState: "not_applicable", Payload: b, ResearchOnly: true}, nil
}

func replayKindBucket(kind string) string {
	switch kind {
	case "book", "trade", "lifecycle":
		return kind
	case "source-clock", "source":
		return "source"
	default:
		return "governance"
	}
}

type segmentHeader struct {
	RecordType          string `json:"record_type"`
	Format              string `json:"format"`
	SegmentID           string `json:"segment_id"`
	HourUTC             string `json:"hour_utc"`
	CreatedAt           string `json:"created_at"`
	PreviousSegmentHash string `json:"previous_segment_hash"`
	PreviousFrameHash   string `json:"previous_frame_hash"`
	FirstSequence       uint64 `json:"first_sequence"`
}

type frameRecord struct {
	RecordType          string          `json:"record_type"`
	Sequence            uint64          `json:"sequence"`
	RecordedAt          string          `json:"recorded_at"`
	Source              string          `json:"source"`
	SchemaVersion       string          `json:"schema_version"`
	Kind                string          `json:"kind"`
	EntityID            string          `json:"entity_id"`
	CanonicalEventID    string          `json:"canonical_event_id,omitempty"`
	OpportunityID       string          `json:"opportunity_id,omitempty"`
	Selection           string          `json:"selection"`
	SourceAt            string          `json:"source_at,omitempty"`
	ObservedAt          string          `json:"observed_at"`
	SourceSequence      *int64          `json:"source_sequence,omitempty"`
	PriorSourceSequence *int64          `json:"prior_source_sequence,omitempty"`
	SequenceGap         int64           `json:"sequence_gap,omitempty"`
	SequenceState       string          `json:"sequence_state"`
	SourceClock         string          `json:"source_clock"`
	Payload             json.RawMessage `json:"payload"`
	ResearchOnly        bool            `json:"research_only"`
	Funded              bool            `json:"funded"`
	PaperAuthority      bool            `json:"paper_authority"`
	LiveAuthority       bool            `json:"live_authority"`
	PreviousHash        string          `json:"previous_hash"`
	Hash                string          `json:"hash"`
}

type frameHashMaterial struct {
	Sequence            uint64          `json:"sequence"`
	RecordedAt          string          `json:"recorded_at"`
	Source              string          `json:"source"`
	SchemaVersion       string          `json:"schema_version"`
	Kind                string          `json:"kind"`
	EntityID            string          `json:"entity_id"`
	CanonicalEventID    string          `json:"canonical_event_id,omitempty"`
	OpportunityID       string          `json:"opportunity_id,omitempty"`
	Selection           string          `json:"selection"`
	SourceAt            string          `json:"source_at,omitempty"`
	ObservedAt          string          `json:"observed_at"`
	SourceSequence      *int64          `json:"source_sequence,omitempty"`
	PriorSourceSequence *int64          `json:"prior_source_sequence,omitempty"`
	SequenceGap         int64           `json:"sequence_gap,omitempty"`
	SequenceState       string          `json:"sequence_state"`
	SourceClock         string          `json:"source_clock"`
	Payload             json.RawMessage `json:"payload"`
	ResearchOnly        bool            `json:"research_only"`
	Funded              bool            `json:"funded"`
	PaperAuthority      bool            `json:"paper_authority"`
	LiveAuthority       bool            `json:"live_authority"`
	PreviousHash        string          `json:"previous_hash"`
}

func (f frameRecord) hashMaterial() frameHashMaterial {
	return frameHashMaterial{Sequence: f.Sequence, RecordedAt: f.RecordedAt, Source: f.Source,
		SchemaVersion: f.SchemaVersion, Kind: f.Kind, EntityID: f.EntityID,
		CanonicalEventID: f.CanonicalEventID, OpportunityID: f.OpportunityID, Selection: f.Selection,
		SourceAt: f.SourceAt, ObservedAt: f.ObservedAt, SourceSequence: f.SourceSequence,
		PriorSourceSequence: f.PriorSourceSequence, SequenceGap: f.SequenceGap,
		SequenceState: f.SequenceState, SourceClock: f.SourceClock, Payload: f.Payload,
		ResearchOnly: f.ResearchOnly, Funded: f.Funded, PaperAuthority: f.PaperAuthority,
		LiveAuthority: f.LiveAuthority, PreviousHash: f.PreviousHash}
}

type segmentTrailer struct {
	RecordType       string         `json:"record_type"`
	SegmentHash      string         `json:"segment_hash"`
	FrameCount       int            `json:"frame_count"`
	LastSequence     uint64         `json:"last_sequence"`
	LastFrameHash    string         `json:"last_frame_hash"`
	UncompressedSize int            `json:"uncompressed_size"`
	FrameKinds       map[string]int `json:"frame_kinds"`
}

type SegmentMeta struct {
	SegmentID           string         `json:"segment_id"`
	RelativePath        string         `json:"relative_path"`
	HourUTC             string         `json:"hour_utc"`
	CreatedAt           string         `json:"created_at"`
	FirstSequence       uint64         `json:"first_sequence"`
	LastSequence        uint64         `json:"last_sequence"`
	Frames              int            `json:"frames"`
	CompressedBytes     int64          `json:"compressed_bytes"`
	UncompressedBytes   int            `json:"uncompressed_bytes"`
	PreviousSegmentHash string         `json:"previous_segment_hash"`
	SegmentHash         string         `json:"segment_hash"`
	FirstFrameHash      string         `json:"first_frame_hash"`
	LastFrameHash       string         `json:"last_frame_hash"`
	FrameKinds          map[string]int `json:"frame_kinds"`
}

type Index struct {
	Format               string        `json:"format"`
	Generation           uint64        `json:"generation"`
	UpdatedAt            string        `json:"updated_at"`
	AnchorSegmentHash    string        `json:"anchor_segment_hash,omitempty"`
	AnchorFrameHash      string        `json:"anchor_frame_hash,omitempty"`
	HeadSegmentHash      string        `json:"head_segment_hash,omitempty"`
	HeadFrameHash        string        `json:"head_frame_hash,omitempty"`
	NextSequence         uint64        `json:"next_sequence"`
	TotalCompressedBytes int64         `json:"total_compressed_bytes"`
	Segments             []SegmentMeta `json:"segments"`
	IntegrityHash        string        `json:"integrity_hash"`
}

type VerificationReport struct {
	GeneratedAt           string         `json:"generated_at"`
	Healthy               bool           `json:"healthy"`
	Format                string         `json:"format"`
	Segments              int            `json:"segments"`
	Frames                int            `json:"frames"`
	CompressedBytes       int64          `json:"compressed_bytes"`
	OldestHour            string         `json:"oldest_hour,omitempty"`
	NewestHour            string         `json:"newest_hour,omitempty"`
	AnchorSegmentHash     string         `json:"anchor_segment_hash,omitempty"`
	HeadSegmentHash       string         `json:"head_segment_hash,omitempty"`
	HeadFrameHash         string         `json:"head_frame_hash,omitempty"`
	NextSequence          uint64         `json:"next_sequence"`
	OrphanSegments        int            `json:"orphan_segments"`
	TemporaryFiles        int            `json:"temporary_files"`
	VerifiedSegments      int            `json:"verified_segments"`
	VerificationScope     string         `json:"verification_scope"`
	FrameKinds            []string       `json:"frame_kinds"`
	FrameKindCounts       map[string]int `json:"frame_kind_counts"`
	SelectedKinds         []string       `json:"selected_kinds"`
	PendingSelectedFrames int            `json:"pending_selected_frames"`
	PendingSelectedBytes  int            `json:"pending_selected_bytes"`
	SelectionDropped      uint64         `json:"selection_dropped"`
	SelectionEvicted      uint64         `json:"selection_evicted"`
	Cached                bool           `json:"cached"`
	Error                 string         `json:"error,omitempty"`
	ResearchOnly          bool           `json:"research_only"`
	Funded                bool           `json:"funded"`
	PaperAuthority        bool           `json:"paper_authority"`
	LiveAuthority         bool           `json:"live_authority"`
}
