package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"
)

// ExecutionShadowWatermark is an immutable prefix of the isolated comparison ledger.
// AttemptSequence comes from execution_shadow_attempt_order rather than SQLite's implicit rowid,
// because VACUUM is allowed to renumber implicit rowids. EventRowID is the table's explicit
// INTEGER PRIMARY KEY and is therefore stable across a snapshot.
type ExecutionShadowWatermark struct {
	AttemptSequence int64     `json:"attempt_sequence"`
	EventRowID      int64     `json:"event_row_id"`
	CapturedAt      time.Time `json:"captured_at"`
}

func validateExecutionShadowWatermark(watermark ExecutionShadowWatermark) error {
	if watermark.AttemptSequence < 0 || watermark.EventRowID < 0 {
		return errors.New("execution-shadow watermark cannot be negative")
	}
	if watermark.CapturedAt.IsZero() {
		return errors.New("execution-shadow watermark has no capture time")
	}
	return nil
}

// CaptureExecutionShadowWatermark takes both high-water marks from one read transaction. WAL mode
// lets this run beside the isolated telemetry writer without touching the main cash database or
// adding any work to order submission.
func (s *Store) CaptureExecutionShadowWatermark(
	ctx context.Context,
) (ExecutionShadowWatermark, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return ExecutionShadowWatermark{}, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ExecutionShadowWatermark{}, err
	}
	defer tx.Rollback()
	var watermark ExecutionShadowWatermark
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(sequence),0) FROM execution_shadow_attempt_order`,
	).Scan(&watermark.AttemptSequence); err != nil {
		return ExecutionShadowWatermark{}, fmt.Errorf(
			"capture execution-shadow attempt watermark: %w", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(id),0) FROM execution_shadow_events`,
	).Scan(&watermark.EventRowID); err != nil {
		return ExecutionShadowWatermark{}, fmt.Errorf(
			"capture execution-shadow event watermark: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return ExecutionShadowWatermark{}, fmt.Errorf(
			"commit execution-shadow watermark read: %w", err)
	}
	watermark.CapturedAt = time.Now().UTC()
	return watermark, nil
}

const executionShadowDropAppendOnlyDeleteTriggers = `
DROP TRIGGER IF EXISTS execution_shadow_attempts_no_delete;
DROP TRIGGER IF EXISTS execution_shadow_events_no_delete;
`

const executionShadowRestoreAppendOnlyDeleteTriggers = `
CREATE TRIGGER IF NOT EXISTS execution_shadow_attempts_no_delete
 BEFORE DELETE ON execution_shadow_attempts
 BEGIN SELECT RAISE(ABORT,'execution-shadow attempts are append-preserved'); END;
CREATE TRIGGER IF NOT EXISTS execution_shadow_events_no_delete
 BEFORE DELETE ON execution_shadow_events
 BEGIN SELECT RAISE(ABORT,'execution-shadow events are append-preserved'); END;
`

// TrimExecutionShadowSnapshotToWatermark converts a later shadow VACUUM into the exact immutable
// prefix captured before the main snapshot began. It operates only on the unpublished destination
// file; live append-only history is never changed. A later comparison attempt/event may therefore
// be present in the raw VACUUM result, but can never enter the paired, hashed artifact.
func TrimExecutionShadowSnapshotToWatermark(
	ctx context.Context,
	path string,
	watermark ExecutionShadowWatermark,
) error {
	if err := validateExecutionShadowWatermark(watermark); err != nil {
		return err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve execution-shadow snapshot: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(absPath)+
		"?_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("open execution-shadow snapshot: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin execution-shadow cutoff: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, executionShadowDropAppendOnlyDeleteTriggers); err != nil {
		return fmt.Errorf("open execution-shadow snapshot cutoff: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM execution_shadow_events WHERE id>?`, watermark.EventRowID); err != nil {
		return fmt.Errorf("trim post-watermark execution-shadow events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM execution_shadow_attempts
WHERE attempt_id IN (
 SELECT attempt_id FROM execution_shadow_attempt_order WHERE sequence>?
)`, watermark.AttemptSequence); err != nil {
		return fmt.Errorf("trim post-watermark execution-shadow attempts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, executionShadowRestoreAppendOnlyDeleteTriggers); err != nil {
		return fmt.Errorf("restore execution-shadow append-only triggers: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO execution_shadow_meta(key,value,updated_ts)
VALUES('snapshot_watermark',?,?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`,
		fmt.Sprintf("attempt_sequence=%d;event_row_id=%d",
			watermark.AttemptSequence, watermark.EventRowID),
		watermark.CapturedAt.UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("record execution-shadow snapshot watermark: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit execution-shadow snapshot cutoff: %w", err)
	}
	return ValidateExecutionShadowSnapshotWatermark(ctx, path, watermark)
}

// ValidateExecutionShadowSnapshotWatermark is the restore/analysis enforcement gate. A v2
// manifest is not selectable unless its exact shadow artifact contains no attempt or event beyond
// the declared prefix and every retained attempt has a stable sequence row.
func ValidateExecutionShadowSnapshotWatermark(
	ctx context.Context,
	path string,
	watermark ExecutionShadowWatermark,
) error {
	if err := validateExecutionShadowWatermark(watermark); err != nil {
		return err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve execution-shadow snapshot: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(absPath)+
		"?mode=ro&_pragma=query_only(ON)&_pragma=busy_timeout(5000)")
	if err != nil {
		return fmt.Errorf("open execution-shadow snapshot read-only: %w", err)
	}
	defer db.Close()
	var maxAttemptSequence, maxEventRowID, postAttempts, postEvents, unmappedAttempts int64
	if err := db.QueryRowContext(ctx, `
SELECT
 (SELECT COALESCE(MAX(sequence),0) FROM execution_shadow_attempt_order),
 (SELECT COALESCE(MAX(id),0) FROM execution_shadow_events),
 (SELECT COUNT(*) FROM execution_shadow_attempt_order WHERE sequence>?),
 (SELECT COUNT(*) FROM execution_shadow_events WHERE id>?),
 (SELECT COUNT(*) FROM execution_shadow_attempts a
  WHERE NOT EXISTS (
   SELECT 1 FROM execution_shadow_attempt_order o WHERE o.attempt_id=a.attempt_id
  ))`,
		watermark.AttemptSequence, watermark.EventRowID,
	).Scan(&maxAttemptSequence, &maxEventRowID,
		&postAttempts, &postEvents, &unmappedAttempts); err != nil {
		return fmt.Errorf("read execution-shadow snapshot watermark: %w", err)
	}
	if maxAttemptSequence != watermark.AttemptSequence ||
		maxEventRowID != watermark.EventRowID ||
		postAttempts != 0 || postEvents != 0 || unmappedAttempts != 0 {
		return fmt.Errorf(
			"execution-shadow snapshot does not equal watermark prefix: max_attempt_sequence=%d want=%d max_event_row_id=%d want=%d post_attempts=%d post_events=%d unmapped_attempts=%d",
			maxAttemptSequence, watermark.AttemptSequence,
			maxEventRowID, watermark.EventRowID,
			postAttempts, postEvents, unmappedAttempts)
	}
	return nil
}
