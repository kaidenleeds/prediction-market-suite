package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

type SourceClockSpec struct {
	SourceID, DisplayName, AuthorityURL, SchemaVersion, SchemaHash string
	ClockKind, TimestampField, SequenceField, Timezone             string
	RevisionPolicy, SettlementCompatibility, CachePolicy           string
	Version                                                        int
	ExpectedCadence, GapTolerance, MaxLag, PublicationLag          time.Duration
	Active                                                         bool
}

type sourceClockHash struct {
	SourceID, DisplayName, AuthorityURL, SchemaVersion, SchemaHash string
	ClockKind, TimestampField, SequenceField, Timezone             string
	RevisionPolicy, SettlementCompatibility, CachePolicy           string
	ExpectedCadenceS, GapToleranceS, MaxLagS, PublicationLagS      float64
	Active                                                         bool
}

func normalizeClockKind(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "source_timestamp", "monotonic_sequence", "source_timestamp_sequence", "arrival_only":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

func validSHA256Hex(v string) bool {
	if len(v) != 64 {
		return false
	}
	b, err := hex.DecodeString(v)
	return err == nil && len(b) == sha256.Size
}

func sourceClockSpecHash(s SourceClockSpec) (string, error) {
	v := sourceClockHash{SourceID: s.SourceID, DisplayName: s.DisplayName, AuthorityURL: s.AuthorityURL,
		SchemaVersion: s.SchemaVersion, SchemaHash: strings.ToLower(s.SchemaHash), ClockKind: s.ClockKind,
		TimestampField: s.TimestampField, SequenceField: s.SequenceField, Timezone: s.Timezone,
		RevisionPolicy: s.RevisionPolicy, SettlementCompatibility: s.SettlementCompatibility,
		CachePolicy: s.CachePolicy, ExpectedCadenceS: s.ExpectedCadence.Seconds(),
		GapToleranceS: s.GapTolerance.Seconds(), MaxLagS: s.MaxLag.Seconds(),
		PublicationLagS: s.PublicationLag.Seconds(), Active: s.Active}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func normalizeSourceClockSpec(s SourceClockSpec) (SourceClockSpec, error) {
	s.SourceID, s.DisplayName = strings.TrimSpace(s.SourceID), strings.TrimSpace(s.DisplayName)
	s.AuthorityURL, s.SchemaVersion = strings.TrimSpace(s.AuthorityURL), strings.TrimSpace(s.SchemaVersion)
	s.SchemaHash = strings.ToLower(strings.TrimSpace(s.SchemaHash))
	s.ClockKind = normalizeClockKind(s.ClockKind)
	s.TimestampField, s.SequenceField = strings.TrimSpace(s.TimestampField), strings.TrimSpace(s.SequenceField)
	s.Timezone = strings.TrimSpace(s.Timezone)
	if s.Timezone == "" {
		s.Timezone = "UTC"
	}
	s.RevisionPolicy, s.SettlementCompatibility = strings.TrimSpace(s.RevisionPolicy), strings.TrimSpace(s.SettlementCompatibility)
	s.CachePolicy = strings.TrimSpace(s.CachePolicy)
	// Registry inputs are active by default. Retirement is represented by a later immutable spec
	// and is intentionally not exposed until a concrete consumer needs it.
	s.Active = true
	if s.SourceID == "" || s.DisplayName == "" || s.SchemaVersion == "" || s.SchemaHash == "" ||
		s.ClockKind == "" || s.ExpectedCadence <= 0 || s.RevisionPolicy == "" || s.SettlementCompatibility == "" {
		return s, fmt.Errorf("incomplete source-clock specification")
	}
	if !validSHA256Hex(s.SchemaHash) {
		return s, fmt.Errorf("source-clock schema_hash must be SHA-256 hex")
	}
	if s.GapTolerance < s.ExpectedCadence {
		return s, fmt.Errorf("source-clock gap tolerance must be at least expected cadence")
	}
	if s.MaxLag < 0 || s.PublicationLag < 0 {
		return s, fmt.Errorf("source-clock lag bounds cannot be negative")
	}
	if strings.Contains(s.ClockKind, "timestamp") && s.TimestampField == "" {
		return s, fmt.Errorf("timestamp clock requires timestamp_field")
	}
	if strings.Contains(s.ClockKind, "sequence") && s.SequenceField == "" {
		return s, fmt.Errorf("sequence clock requires sequence_field")
	}
	return s, nil
}

// RegisterSourceClockSpec appends semantic changes as a new immutable version. Supplying Version
// pins the payload; drift under that version fails rather than silently rewriting prior evidence.
func (s *Store) RegisterSourceClockSpec(ctx context.Context, spec SourceClockSpec) (version int, inserted bool, err error) {
	spec, err = normalizeSourceClockSpec(spec)
	if err != nil {
		return 0, false, err
	}
	h, err := sourceClockSpecHash(spec)
	if err != nil {
		return 0, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT version FROM research_source_clock_specs
WHERE source_id=? AND spec_hash=?`, spec.SourceID, h).Scan(&version); err == nil {
		return version, false, tx.Commit()
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, err
	}
	if spec.Version > 0 {
		var existing string
		err := tx.QueryRowContext(ctx, `SELECT spec_hash FROM research_source_clock_specs
WHERE source_id=? AND version=?`, spec.SourceID, spec.Version).Scan(&existing)
		if err == nil && existing != h {
			return 0, false, fmt.Errorf("immutable source-clock spec drift: %s v%d", spec.SourceID, spec.Version)
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, false, err
		}
		version = spec.Version
	} else if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0)+1
FROM research_source_clock_specs WHERE source_id=?`, spec.SourceID).Scan(&version); err != nil {
		return 0, false, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO research_source_clock_specs(
source_id,version,spec_hash,display_name,authority_url,schema_version,schema_hash,clock_kind,
timestamp_field,sequence_field,timezone,expected_cadence_s,gap_tolerance_s,max_lag_s,
publication_lag_s,revision_policy,settlement_compatibility,cache_policy,active,
funded,paper_authority,live_authority,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0,?)`, spec.SourceID, version, h, spec.DisplayName,
		spec.AuthorityURL, spec.SchemaVersion, spec.SchemaHash, spec.ClockKind, spec.TimestampField,
		spec.SequenceField, spec.Timezone, spec.ExpectedCadence.Seconds(), spec.GapTolerance.Seconds(),
		spec.MaxLag.Seconds(), spec.PublicationLag.Seconds(), spec.RevisionPolicy,
		spec.SettlementCompatibility, spec.CachePolicy, boolInt(spec.Active), nowRFC())
	if err != nil {
		return 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return version, true, nil
}

type SourceClockReceipt struct {
	SourceID, SchemaVersion, SchemaHash, ErrorClass, ErrorText string
	EvidenceJSON, ReplaySegmentHash                            string
	SpecVersion                                                int
	Received, Watermark                                        time.Time
	SequenceStart, SequenceEnd                                 *int64
	// SequencePrior/SequenceGap carry actual adjacent source-frame truth when an adapter has it.
	// A nil SequenceGap preserves the legacy receipt-to-receipt comparison; a non-nil value forbids
	// inventing a contiguous range between periodic samples.
	SequencePrior, SequenceGap *int64
	RowsSeen                   int
	// WatermarkGapObservable is true only when this adapter observes every scheduled publication,
	// so a jump between consecutive source watermarks proves a missing source interval. Periodic
	// samples of a busy event stream must leave this false: two latest timestamps one minute apart do
	// not prove the WebSocket was silent between them.
	WatermarkGapObservable bool
}

type SourceClockReceiptResult struct {
	ID                int64   `json:"id"`
	SourceID          string  `json:"source_id"`
	Status            string  `json:"status"`
	ReceivedTS        string  `json:"received_ts"`
	WatermarkTS       string  `json:"watermark_ts,omitempty"`
	SpecVersion       int     `json:"spec_version"`
	GapSeconds        float64 `json:"gap_seconds"`
	RegressionSeconds float64 `json:"regression_seconds"`
	LagSeconds        float64 `json:"lag_seconds"`
	SequenceGap       int64   `json:"sequence_gap"`
	SchemaDrift       bool    `json:"schema_drift"`
	Alert             bool    `json:"alert"`
}

type sourceClockCurrent struct {
	Version                                           int
	SchemaVersion, SchemaHash, ClockKind              string
	ExpectedCadenceS, GapToleranceS, MaxLagS, PubLagS float64
}

func (s *Store) currentSourceClock(ctx context.Context, sourceID string, version int) (sourceClockCurrent, error) {
	q := `SELECT version,schema_version,schema_hash,clock_kind,expected_cadence_s,gap_tolerance_s,
max_lag_s,publication_lag_s FROM research_source_clock_specs WHERE source_id=?`
	args := []any{sourceID}
	if version > 0 {
		q += ` AND version=?`
		args = append(args, version)
	} else {
		q += ` ORDER BY version DESC LIMIT 1`
	}
	var out sourceClockCurrent
	err := s.db.QueryRowContext(ctx, q, args...).Scan(&out.Version, &out.SchemaVersion, &out.SchemaHash,
		&out.ClockKind, &out.ExpectedCadenceS, &out.GapToleranceS, &out.MaxLagS, &out.PubLagS)
	return out, err
}

func canonicalEvidence(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "{}", nil
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return "", err
	}
	b, err := json.Marshal(v)
	return string(b), err
}

// RecordSourceClock appends even blocked, drifted, regressed and gapped observations. Bad receipts
// never replace the last healthy watermark and cannot authorize any execution path.
func (s *Store) RecordSourceClock(ctx context.Context, in SourceClockReceipt) (SourceClockReceiptResult, error) {
	in.SourceID = strings.TrimSpace(in.SourceID)
	if in.SourceID == "" || in.RowsSeen < 0 {
		return SourceClockReceiptResult{}, fmt.Errorf("invalid source-clock receipt")
	}
	spec, err := s.currentSourceClock(ctx, in.SourceID, in.SpecVersion)
	if err != nil {
		return SourceClockReceiptResult{}, err
	}
	in.SpecVersion = spec.Version
	if in.Received.IsZero() {
		in.Received = time.Now().UTC()
	} else {
		in.Received = in.Received.UTC()
	}
	if !in.Watermark.IsZero() {
		in.Watermark = in.Watermark.UTC()
	}
	in.SchemaVersion, in.SchemaHash = strings.TrimSpace(in.SchemaVersion), strings.ToLower(strings.TrimSpace(in.SchemaHash))
	if in.ReplaySegmentHash != "" && !validSHA256Hex(strings.ToLower(strings.TrimSpace(in.ReplaySegmentHash))) {
		return SourceClockReceiptResult{}, fmt.Errorf("source-clock replay_segment_hash must be SHA-256 hex")
	}
	evidence, err := canonicalEvidence(in.EvidenceJSON)
	if err != nil {
		return SourceClockReceiptResult{}, fmt.Errorf("source-clock evidence JSON: %w", err)
	}
	if len(evidence) > 64<<10 || len(in.ErrorText) > 2048 {
		return SourceClockReceiptResult{}, fmt.Errorf("source-clock evidence/error exceeds bounded receipt size")
	}
	result := SourceClockReceiptResult{SourceID: in.SourceID, SpecVersion: spec.Version,
		Status: "healthy", ReceivedTS: in.Received.Format(time.RFC3339Nano),
		WatermarkTS: sourceTimeStringStorage(in.Watermark)}
	sequenceAvailable := in.SequenceStart != nil && in.SequenceEnd != nil
	missingTimestamp := strings.Contains(spec.ClockKind, "timestamp") && in.Watermark.IsZero()
	missingSequence := strings.Contains(spec.ClockKind, "sequence") && !sequenceAvailable
	if sequenceAvailable && (*in.SequenceStart < 0 || *in.SequenceEnd < *in.SequenceStart) {
		return SourceClockReceiptResult{}, fmt.Errorf("invalid source-clock sequence range")
	}
	if in.SequenceGap != nil {
		if !sequenceAvailable || *in.SequenceGap < 0 || in.SequencePrior != nil && *in.SequencePrior < 0 {
			return SourceClockReceiptResult{}, fmt.Errorf("invalid explicit source-clock sequence truth")
		}
		if in.SequencePrior == nil {
			if *in.SequenceGap != 0 {
				return SourceClockReceiptResult{}, fmt.Errorf("baseline source-clock sequence cannot have a gap")
			}
		} else {
			current := *in.SequenceEnd
			wantGap := int64(0)
			if current > *in.SequencePrior+1 {
				wantGap = current - *in.SequencePrior - 1
			}
			if current > *in.SequencePrior && *in.SequenceGap != wantGap {
				return SourceClockReceiptResult{}, fmt.Errorf("explicit source-clock sequence gap disagrees with adjacent frames")
			}
		}
	}
	if !in.Watermark.IsZero() {
		result.LagSeconds = math.Max(0, in.Received.Sub(in.Watermark).Seconds()-spec.PubLagS)
	}
	var priorWatermark string
	var priorSequence sql.NullInt64
	_ = s.db.QueryRowContext(ctx, `SELECT source_watermark_ts,sequence_end
FROM research_source_clock_receipts WHERE source_id=? AND spec_version=?
  AND status IN ('healthy','gap') AND schema_version=? AND schema_hash=?
ORDER BY id DESC LIMIT 1`, in.SourceID, spec.Version, spec.SchemaVersion, spec.SchemaHash).Scan(&priorWatermark, &priorSequence)
	if priorWatermark != "" && !in.Watermark.IsZero() {
		if previous, err := time.Parse(time.RFC3339Nano, priorWatermark); err == nil {
			delta := in.Watermark.Sub(previous).Seconds()
			if delta < 0 {
				result.RegressionSeconds = -delta
			} else if in.WatermarkGapObservable && delta > spec.GapToleranceS {
				result.GapSeconds = delta
			}
		}
	}
	if sequenceAvailable && in.SequenceGap != nil {
		result.SequenceGap = *in.SequenceGap
		if in.SequencePrior != nil && *in.SequenceEnd <= *in.SequencePrior {
			result.RegressionSeconds = math.Max(result.RegressionSeconds, 1)
		}
	} else if sequenceAvailable && priorSequence.Valid {
		switch {
		case *in.SequenceEnd < priorSequence.Int64 || *in.SequenceStart <= priorSequence.Int64:
			result.RegressionSeconds = math.Max(result.RegressionSeconds, 1)
		case *in.SequenceStart > priorSequence.Int64+1:
			result.SequenceGap = *in.SequenceStart - priorSequence.Int64 - 1
		}
	}
	schemaDrift := in.SchemaVersion != spec.SchemaVersion || in.SchemaHash != spec.SchemaHash
	result.SchemaDrift = schemaDrift
	explicitBlocked := in.ErrorClass == "adapter_blocked" || in.ErrorClass == "source_data_absent"
	switch {
	case explicitBlocked:
		// Expected unavailable inputs are blockers, not operational errors. Preserve the exact
		// reason while distinguishing a deliberately unimplemented/absent source from a failed one.
		result.Status = "blocked"
	case strings.TrimSpace(in.ErrorText) != "":
		result.Status = "error"
	case schemaDrift:
		result.Status = "schema_drift"
		if in.ErrorClass == "" {
			in.ErrorClass = "schema"
		}
	case missingTimestamp || missingSequence:
		result.Status = "blocked"
		if in.ErrorClass == "" {
			in.ErrorClass = "clock_field_missing"
		}
	case result.RegressionSeconds > 0:
		result.Status = "regression"
	case result.GapSeconds > 0 || result.SequenceGap > 0 || (spec.MaxLagS > 0 && result.LagSeconds > spec.MaxLagS):
		result.Status = "gap"
	}
	result.Alert = result.Status != "healthy"
	res, err := s.db.ExecContext(ctx, `INSERT INTO research_source_clock_receipts(
source_id,spec_version,received_ts,source_watermark_ts,schema_version,schema_hash,
sequence_available,sequence_start,sequence_end,rows_seen,status,gap_seconds,regression_seconds,
sequence_gap,lag_seconds,error_class,error_text,evidence_json,replay_segment_hash,
funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, in.SourceID, spec.Version,
		result.ReceivedTS, result.WatermarkTS, in.SchemaVersion, in.SchemaHash, boolInt(sequenceAvailable),
		nullInt64(in.SequenceStart), nullInt64(in.SequenceEnd), in.RowsSeen, result.Status,
		result.GapSeconds, result.RegressionSeconds, result.SequenceGap, result.LagSeconds,
		strings.TrimSpace(in.ErrorClass), strings.TrimSpace(in.ErrorText), evidence,
		strings.TrimSpace(in.ReplaySegmentHash))
	if err != nil {
		return SourceClockReceiptResult{}, err
	}
	result.ID, _ = res.LastInsertId()
	return result, nil
}

func sourceTimeStringStorage(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func nullInt64(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}

type SourceClockLiveness struct {
	SourceID, DisplayName, AuthorityURL, SchemaVersion, SchemaHash, ClockKind          string
	Status, ReceivedTS, WatermarkTS, LastHealthyTS, ErrorClass, ErrorText              string
	Version, RowsSeen, LifetimeReceipts, Gaps, Regressions, SchemaDrifts               int
	ExpectedCadenceS, GapToleranceS, MaxLagS, PublicationLagS, ReceiptLagS, SourceLagS float64
	SequenceGap                                                                        int64
	NeverRan, Alert, Funded, PaperAuthority, LiveAuthority                             bool
}

const sourceClockReportSQL = `WITH latest_versions AS (
 SELECT source_id,MAX(version) AS version FROM research_source_clock_specs GROUP BY source_id
), latest_specs AS (
 SELECT c.* FROM research_source_clock_specs c JOIN latest_versions v
  ON v.source_id=c.source_id AND v.version=c.version WHERE c.active=1
), receipt_rollup AS (
 SELECT r.source_id,r.spec_version,MAX(r.id) AS latest_id,COUNT(*) AS lifetime_receipts,
  COALESCE(SUM(r.status='gap'),0) AS gaps,
  COALESCE(SUM(r.status='regression'),0) AS regressions,
  COALESCE(SUM(r.status='schema_drift'),0) AS schema_drifts,
  COALESCE(MAX(CASE WHEN r.status='healthy' THEN r.received_ts ELSE '' END),'') AS last_healthy_ts
 FROM research_source_clock_receipts r JOIN latest_specs c
  ON c.source_id=r.source_id AND c.version=r.spec_version
 GROUP BY r.source_id,r.spec_version
)
SELECT c.source_id,c.display_name,c.authority_url,c.schema_version,c.schema_hash,c.clock_kind,
c.version,c.expected_cadence_s,c.gap_tolerance_s,c.max_lag_s,c.publication_lag_s,
COALESCE(r.status,''),COALESCE(r.received_ts,''),COALESCE(r.source_watermark_ts,''),
COALESCE(r.rows_seen,0),COALESCE(r.sequence_gap,0),COALESCE(r.error_class,''),COALESCE(r.error_text,''),
COALESCE(r.funded,0),COALESCE(r.paper_authority,0),COALESCE(r.live_authority,0),
COALESCE(a.lifetime_receipts,0),COALESCE(a.gaps,0),COALESCE(a.regressions,0),
COALESCE(a.schema_drifts,0),COALESCE(a.last_healthy_ts,'')
FROM latest_specs c
LEFT JOIN receipt_rollup a ON a.source_id=c.source_id AND a.spec_version=c.version
LEFT JOIN research_source_clock_receipts r ON r.id=a.latest_id
ORDER BY c.source_id`

func (s *Store) SourceClockReport(ctx context.Context) (map[string]any, error) {
	// One statement returns latest specs, latest receipts, and lifetime counters. The previous
	// implementation held these rows open and issued one aggregate QueryRow per source (N+1),
	// multiplying pool pressure exactly when a degraded dashboard was polling most often.
	rows, err := s.db.QueryContext(ctx, sourceClockReportSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	now := time.Now().UTC()
	var out []SourceClockLiveness
	alerts := 0
	for rows.Next() {
		var v SourceClockLiveness
		var funded, paper, live int
		if err := rows.Scan(&v.SourceID, &v.DisplayName, &v.AuthorityURL, &v.SchemaVersion,
			&v.SchemaHash, &v.ClockKind, &v.Version, &v.ExpectedCadenceS, &v.GapToleranceS, &v.MaxLagS, &v.PublicationLagS,
			&v.Status, &v.ReceivedTS, &v.WatermarkTS, &v.RowsSeen, &v.SequenceGap,
			&v.ErrorClass, &v.ErrorText, &funded, &paper, &live, &v.LifetimeReceipts,
			&v.Gaps, &v.Regressions, &v.SchemaDrifts, &v.LastHealthyTS); err != nil {
			return nil, err
		}
		v.Funded, v.PaperAuthority, v.LiveAuthority = funded != 0, paper != 0, live != 0
		v.NeverRan = v.ReceivedTS == ""
		if t, err := time.Parse(time.RFC3339Nano, v.ReceivedTS); err == nil {
			v.ReceiptLagS = math.Max(0, now.Sub(t).Seconds())
		}
		if t, err := time.Parse(time.RFC3339Nano, v.WatermarkTS); err == nil {
			v.SourceLagS = math.Max(0, now.Sub(t).Seconds()-v.PublicationLagS)
		}
		// The runtime observer intentionally emits at most once per minute. ExpectedCadenceS is the
		// source's own clock, not the observer's write frequency; using a 1s feed cadence here made a
		// healthy WebSocket look stale three seconds after every minute-level receipt.
		receiptStaleAfter := math.Max(3*v.ExpectedCadenceS, (3 * time.Minute).Seconds())
		v.Alert = v.NeverRan || v.Status != "healthy" ||
			(v.ExpectedCadenceS > 0 && v.ReceiptLagS > receiptStaleAfter) ||
			(v.MaxLagS > 0 && v.SourceLagS > v.MaxLagS)
		if v.Alert {
			alerts++
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"generated_at": now.Format(time.RFC3339Nano), "research_only": true,
		"funded": false, "paper_authority": false, "live_authority": false,
		"sources": out, "active": len(out), "alerts": alerts}, nil
}
