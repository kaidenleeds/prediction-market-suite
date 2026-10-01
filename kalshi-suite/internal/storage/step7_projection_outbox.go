package storage

// The Step-7 projection outbox binds each compact raw microstructure fact to one immutable,
// deterministic projection payload. Raw insertion and outbox enqueue share a transaction. A
// worker later replays only the stored bytes and applies every output plus completion in one
// transaction, so restart and crash boundaries cannot create a half-projected source fact.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const step7ProjectionOutboxSchema = `
CREATE TABLE IF NOT EXISTS research_step7_projection_jobs (
 job_id TEXT PRIMARY KEY,
 raw_kind TEXT NOT NULL CHECK(raw_kind IN ('episode','mark','flow')),
 raw_id TEXT NOT NULL,
 raw_observed_ts TEXT NOT NULL,
 source_key TEXT NOT NULL,
 source_fingerprint TEXT NOT NULL CHECK(length(source_fingerprint)=64),
 replay_fingerprint TEXT NOT NULL CHECK(length(replay_fingerprint)=64),
 payload_version INTEGER NOT NULL CHECK(payload_version>0),
 payload_hash TEXT NOT NULL CHECK(length(payload_hash)=64),
 terminal_reason TEXT NOT NULL DEFAULT '',
 created_ts TEXT NOT NULL,
 completed_ts TEXT NOT NULL DEFAULT '',
 output_count INTEGER NOT NULL DEFAULT 0 CHECK(output_count>=0),
 UNIQUE(raw_kind,raw_id),
 UNIQUE(source_key),
 UNIQUE(payload_hash),
 CHECK(source_key=raw_kind||':'||raw_id),
 CHECK(output_count>0 OR terminal_reason!='')
);
CREATE INDEX IF NOT EXISTS idx_step7_projection_pending
ON research_step7_projection_jobs(completed_ts,created_ts,job_id);
CREATE TRIGGER IF NOT EXISTS research_step7_projection_jobs_replay_fingerprint_insert
BEFORE INSERT ON research_step7_projection_jobs
WHEN length(NEW.replay_fingerprint)!=64
BEGIN SELECT RAISE(ABORT,'invalid Step-7 replay fingerprint'); END;
CREATE TRIGGER IF NOT EXISTS research_step7_projection_jobs_payload_immutable
BEFORE UPDATE OF job_id,raw_kind,raw_id,raw_observed_ts,source_key,source_fingerprint,payload_version,
 replay_fingerprint,payload_hash,terminal_reason,created_ts ON research_step7_projection_jobs
BEGIN SELECT RAISE(ABORT,'immutable Step-7 projection payload'); END;
CREATE TRIGGER IF NOT EXISTS research_step7_projection_jobs_no_delete
BEFORE DELETE ON research_step7_projection_jobs
BEGIN SELECT RAISE(ABORT,'durable Step-7 projection replay tombstone'); END;

CREATE TABLE IF NOT EXISTS research_step7_projection_pending (
 job_id TEXT PRIMARY KEY,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
 last_attempt_ts TEXT NOT NULL DEFAULT '',
 last_error TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(job_id) REFERENCES research_step7_projection_jobs(job_id)
);
CREATE INDEX IF NOT EXISTS idx_step7_projection_pending_fair
ON research_step7_projection_pending(
 CASE WHEN last_attempt_ts='' THEN '' ELSE last_attempt_ts END,job_id);
CREATE TRIGGER IF NOT EXISTS research_step7_projection_pending_payload_immutable
BEFORE UPDATE OF job_id,payload_json ON research_step7_projection_pending
BEGIN SELECT RAISE(ABORT,'immutable pending Step-7 projection payload'); END;

CREATE TABLE IF NOT EXISTS research_step7_projection_attempts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 job_id TEXT NOT NULL,
 attempted_ts TEXT NOT NULL,
 payload_hash TEXT NOT NULL CHECK(length(payload_hash)=64),
 result TEXT NOT NULL CHECK(result='failed'),
 output_count INTEGER NOT NULL DEFAULT 0 CHECK(output_count>=0),
 error_text TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(job_id) REFERENCES research_step7_projection_jobs(job_id)
);
CREATE INDEX IF NOT EXISTS idx_step7_projection_attempt_job
ON research_step7_projection_attempts(job_id,id);
CREATE TRIGGER IF NOT EXISTS research_step7_projection_attempts_no_update
BEFORE UPDATE ON research_step7_projection_attempts
BEGIN SELECT RAISE(ABORT,'append-only Step-7 projection attempt'); END;

CREATE TABLE IF NOT EXISTS research_step7_projection_completions (
 job_id TEXT PRIMARY KEY,
 completed_ts TEXT NOT NULL,
 payload_hash TEXT NOT NULL CHECK(length(payload_hash)=64),
 output_count INTEGER NOT NULL CHECK(output_count>=0),
 terminal_reason TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(job_id) REFERENCES research_step7_projection_jobs(job_id)
);
CREATE TRIGGER IF NOT EXISTS research_step7_projection_completions_no_update
BEFORE UPDATE ON research_step7_projection_completions
BEGIN SELECT RAISE(ABORT,'immutable Step-7 projection completion'); END;
CREATE TRIGGER IF NOT EXISTS research_step7_projection_completions_no_delete
BEFORE DELETE ON research_step7_projection_completions
BEGIN SELECT RAISE(ABORT,'durable Step-7 projection completion'); END;

-- Legacy raw rows predate the outbox contract. A later callback collision is retained as an
-- explicit no-backfill tombstone, never projected from a later book/fee/identity state.
CREATE TABLE IF NOT EXISTS research_step7_projection_legacy_collisions (
 source_key TEXT PRIMARY KEY,
 raw_kind TEXT NOT NULL CHECK(raw_kind IN ('episode','mark','flow')),
 raw_id TEXT NOT NULL,
 source_fingerprint TEXT NOT NULL CHECK(length(source_fingerprint)=64),
 rejected_payload_hash TEXT NOT NULL CHECK(length(rejected_payload_hash)=64),
 detected_ts TEXT NOT NULL,
 reason TEXT NOT NULL CHECK(reason='legacy_raw_without_frozen_projection_no_backfill'),
 UNIQUE(raw_kind,raw_id),
 CHECK(source_key=raw_kind||':'||raw_id)
);
CREATE TRIGGER IF NOT EXISTS research_step7_projection_legacy_collision_no_update
BEFORE UPDATE ON research_step7_projection_legacy_collisions
BEGIN SELECT RAISE(ABORT,'immutable Step-7 legacy collision'); END;
CREATE TRIGGER IF NOT EXISTS research_step7_projection_legacy_collision_no_delete
BEFORE DELETE ON research_step7_projection_legacy_collisions
BEGIN SELECT RAISE(ABORT,'immutable Step-7 legacy collision'); END;
`

func preserveIntermediateStep7PendingPayloads(db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS research_step7_projection_pending (
 job_id TEXT PRIMARY KEY,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
 attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0),
 last_attempt_ts TEXT NOT NULL DEFAULT '',
 last_error TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(job_id) REFERENCES research_step7_projection_jobs(job_id)
)`); err != nil {
		return err
	}
	columns := map[string]bool{}
	rows, err := db.Query(`PRAGMA table_info(research_step7_projection_pending)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !columns["job_id"] || !columns["payload_json"] {
		return errors.New("intermediate Step-7 pending table lacks job_id or payload_json")
	}
	for column, ddl := range map[string]string{
		"attempt_count":   "attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count>=0)",
		"last_attempt_ts": "last_attempt_ts TEXT NOT NULL DEFAULT ''",
		"last_error":      "last_error TEXT NOT NULL DEFAULT ''",
	} {
		if !columns[column] {
			if _, err := db.Exec(`ALTER TABLE research_step7_projection_pending ADD COLUMN ` + ddl); err != nil {
				return fmt.Errorf("currentize intermediate Step-7 pending %s: %w", column, err)
			}
		}
	}
	type pendingPayload struct{ jobID, payload, hash string }
	pending := []pendingPayload{}
	rows, err = db.Query(`SELECT job_id,payload_json,payload_hash
FROM research_step7_projection_jobs WHERE completed_ts='' ORDER BY job_id`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var row pendingPayload
		if err := rows.Scan(&row.jobID, &row.payload, &row.hash); err != nil {
			rows.Close()
			return err
		}
		if !json.Valid([]byte(row.payload)) ||
			step7SHA256("step7-projection-payload-v1", row.payload) != row.hash {
			rows.Close()
			return fmt.Errorf("incomplete intermediate Step-7 job %s has invalid payload/hash", row.jobID)
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, row := range pending {
		if _, err := db.Exec(`INSERT OR IGNORE INTO research_step7_projection_pending(
job_id,payload_json) VALUES(?,?)`, row.jobID, row.payload); err != nil {
			return fmt.Errorf("preserve incomplete Step-7 payload %s: %w", row.jobID, err)
		}
		var preserved string
		if err := db.QueryRow(`SELECT payload_json FROM research_step7_projection_pending
WHERE job_id=?`, row.jobID).Scan(&preserved); err != nil {
			return fmt.Errorf("verify incomplete Step-7 payload %s: %w", row.jobID, err)
		}
		if preserved != row.payload ||
			step7SHA256("step7-projection-payload-v1", preserved) != row.hash {
			return fmt.Errorf("incomplete Step-7 pending payload conflict for %s", row.jobID)
		}
	}
	return nil
}

func migrateStep7ProjectionOutbox(db *sql.DB) error {
	var jobsTable int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name='research_step7_projection_jobs'`).Scan(&jobsTable); err != nil {
		return err
	}
	if jobsTable != 0 {
		// R174 briefly existed without the raw clock on the compact replay tombstone. Add it
		// before recreating the immutable trigger so a rolling upgrade can still open that DB.
		if _, err := db.Exec(`DROP TRIGGER IF EXISTS research_step7_projection_jobs_payload_immutable`); err != nil {
			return err
		}
		// An intermediate, never-released R174 layout kept the bulky payload on the permanent job
		// row. CREATE TABLE IF NOT EXISTS cannot remove that required column, so inserts using the
		// compact jobs + pending-payload layout would fail after a restart unless we drop it first.
		var legacyPayloadColumn int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('research_step7_projection_jobs')
WHERE name='payload_json'`).Scan(&legacyPayloadColumn); err != nil {
			return err
		}
		if legacyPayloadColumn != 0 {
			if err := preserveIntermediateStep7PendingPayloads(db); err != nil {
				return err
			}
			if _, err := db.Exec(`ALTER TABLE research_step7_projection_jobs DROP COLUMN payload_json`); err != nil {
				return fmt.Errorf("migrate intermediate Step-7 job payload column: %w", err)
			}
		}
		var rawClockColumn int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('research_step7_projection_jobs')
WHERE name='raw_observed_ts'`).Scan(&rawClockColumn); err != nil {
			return err
		}
		if rawClockColumn == 0 {
			if _, err := db.Exec(`ALTER TABLE research_step7_projection_jobs
ADD COLUMN raw_observed_ts TEXT NOT NULL DEFAULT ''`); err != nil {
				return err
			}
		}
		var replayFingerprintColumn int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('research_step7_projection_jobs')
WHERE name='replay_fingerprint'`).Scan(&replayFingerprintColumn); err != nil {
			return err
		}
		if replayFingerprintColumn == 0 {
			if _, err := db.Exec(`ALTER TABLE research_step7_projection_jobs
ADD COLUMN replay_fingerprint TEXT NOT NULL DEFAULT ''`); err != nil {
				return err
			}
		}
		// This is deliberately outside the ADD-COLUMN branch. A process can die after ALTER but
		// before the backfill; every later startup must finish any empty clocks idempotently.
		var emptyRawClocks int
		if err := db.QueryRow(`SELECT EXISTS(
SELECT 1 FROM research_step7_projection_jobs WHERE raw_observed_ts='' LIMIT 1)`).
			Scan(&emptyRawClocks); err != nil {
			return err
		}
		if emptyRawClocks != 0 {
			if _, err := db.Exec(`UPDATE research_step7_projection_jobs
SET raw_observed_ts=COALESCE(
 CASE raw_kind
  WHEN 'episode' THEN (SELECT observed_ts FROM research_replenishment_episodes
   WHERE episode_id=raw_id)
  WHEN 'mark' THEN (SELECT observed_ts FROM research_replenishment_marks
   WHERE episode_id||'|'||event_type||'|'||horizon_ms=raw_id)
  WHEN 'flow' THEN (SELECT observed_ts FROM research_flow_direction_pairs
   WHERE pair_id=raw_id)
 END,created_ts)
WHERE raw_observed_ts=''`); err != nil {
				return err
			}
		}
		// The compact job is the permanent replay tombstone after 90-day raw pruning. Recover the
		// immutable venue-trade identity while the raw flow row still exists; interrupted upgrades
		// retry this on every startup. Jobs whose old raw row was already pruned retain the stricter
		// full-source fingerprint and therefore fail closed on a changed replay.
		var emptyReplayFingerprints int
		if err := db.QueryRow(`SELECT EXISTS(
SELECT 1 FROM research_step7_projection_jobs WHERE replay_fingerprint='' LIMIT 1)`).
			Scan(&emptyReplayFingerprints); err != nil {
			return err
		}
		if emptyReplayFingerprints != 0 {
			var flowTable int
			if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name='research_flow_direction_pairs'`).Scan(&flowTable); err != nil {
				return err
			}
			if flowTable != 0 {
				rows, err := db.Query(`SELECT j.job_id,f.pair_id,f.source_ts,f.ticker,f.trade_id,
f.count_units,f.yes_price,f.authoritative_side
FROM research_step7_projection_jobs j JOIN research_flow_direction_pairs f ON f.pair_id=j.raw_id
WHERE j.raw_kind='flow' AND j.replay_fingerprint=''`)
				if err != nil {
					return err
				}
				type replayBackfill struct{ jobID, fingerprint string }
				backfills := []replayBackfill{}
				for rows.Next() {
					var jobID, pairID, sourceTS, ticker, tradeID, authoritativeSide string
					var count, yesPrice float64
					if err := rows.Scan(&jobID, &pairID, &sourceTS, &ticker, &tradeID, &count,
						&yesPrice, &authoritativeSide); err != nil {
						rows.Close()
						return err
					}
					sourceAt, err := step7ProjectionTime(sourceTS)
					if err != nil {
						rows.Close()
						return err
					}
					backfills = append(backfills, replayBackfill{jobID, step7FlowTradeReplayFingerprint(
						Step7FlowDirectionPair{PairID: pairID, SourceAt: sourceAt, Ticker: ticker,
							TradeID: tradeID, Count: count, YesPrice: yesPrice,
							AuthoritativeSide: authoritativeSide})})
				}
				if err := rows.Err(); err != nil {
					rows.Close()
					return err
				}
				if err := rows.Close(); err != nil {
					return err
				}
				for _, backfill := range backfills {
					if _, err := db.Exec(`UPDATE research_step7_projection_jobs
SET replay_fingerprint=? WHERE job_id=? AND replay_fingerprint=''`,
						backfill.fingerprint, backfill.jobID); err != nil {
						return err
					}
				}
			}
			if _, err := db.Exec(`UPDATE research_step7_projection_jobs
SET replay_fingerprint=source_fingerprint WHERE replay_fingerprint=''`); err != nil {
				return err
			}
		}
	}
	if _, err := db.Exec(step7ProjectionOutboxSchema); err != nil {
		return err
	}
	for _, trigger := range []string{
		"research_step7_projection_attempts_no_delete",
	} {
		if _, err := db.Exec("DROP TRIGGER IF EXISTS " + trigger); err != nil {
			return err
		}
	}
	return nil
}

const (
	Step7ProjectionEpisode = "episode"
	Step7ProjectionMark    = "mark"
	Step7ProjectionFlow    = "flow"
)

type Step7FrozenObservation struct {
	Observation       ResearchSystemObservation `json:"observation"`
	InstrumentVersion int                       `json:"instrument_version"`
}

type Step7ProjectionJob struct {
	JobID, RawKind, RawID, SourceKey, SourceFingerprint string
	ReplayFingerprint                                   string
	RawObservedTS                                       string
	PayloadJSON, PayloadHash, TerminalReason            string
	PayloadVersion                                      int
}

type step7ProjectionPayload struct {
	Version           int                      `json:"version"`
	RawKind           string                   `json:"raw_kind"`
	RawID             string                   `json:"raw_id"`
	SourceKey         string                   `json:"source_key"`
	SourceFingerprint string                   `json:"source_fingerprint"`
	TerminalReason    string                   `json:"terminal_reason"`
	Outputs           []Step7FrozenObservation `json:"outputs"`
}

type Step7ProjectionApplyResult struct {
	Selected, Completed, OutputsInserted, OutputsDuplicate int
	Failed, Pending                                        int
	OldestPendingAge                                       time.Duration
	LastError                                              string
}

type Step7ProjectionCompletionManifest struct {
	Count          int    `json:"count"`
	MaxCompletedTS string `json:"max_completed_ts"`
	Hash           string `json:"hash"`
}

type step7ProjectionQuerier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Step7ProjectionCompletionManifest binds inference to every completed projection whose raw
// decision clock precedes end. It includes terminal zero-output projections, which intentionally
// create no observation row and therefore cannot be detected through observation IDs alone.
func (s *Store) Step7ProjectionCompletionManifest(ctx context.Context,
	end time.Time) (Step7ProjectionCompletionManifest, error) {
	return step7ProjectionCompletionManifest(ctx, s.db, end)
}

func step7ProjectionCompletionManifest(ctx context.Context, q step7ProjectionQuerier,
	end time.Time) (Step7ProjectionCompletionManifest, error) {
	if end.IsZero() {
		end = time.Now().UTC()
	}
	rows, err := q.QueryContext(ctx, `SELECT j.job_id,j.payload_hash,j.source_fingerprint,
j.raw_observed_ts,c.completed_ts,c.output_count,c.terminal_reason
FROM research_step7_projection_completions c
JOIN research_step7_projection_jobs j ON j.job_id=c.job_id
WHERE julianday(j.raw_observed_ts)<julianday(?) ORDER BY j.job_id`, step7Time(end))
	if err != nil {
		return Step7ProjectionCompletionManifest{}, err
	}
	defer rows.Close()
	h := sha256.New()
	out := Step7ProjectionCompletionManifest{}
	var maxCompleted time.Time
	for rows.Next() {
		var jobID, payloadHash, sourceFingerprint, rawObserved, completed, terminalReason string
		var outputCount int
		if err := rows.Scan(&jobID, &payloadHash, &sourceFingerprint, &rawObserved, &completed,
			&outputCount, &terminalReason); err != nil {
			return Step7ProjectionCompletionManifest{}, err
		}
		for _, part := range []string{jobID, payloadHash, sourceFingerprint, rawObserved, completed,
			strconv.Itoa(outputCount), terminalReason} {
			_, _ = h.Write([]byte(strconv.Itoa(len(part))))
			_, _ = h.Write([]byte{':'})
			_, _ = h.Write([]byte(part))
		}
		out.Count++
		completedAt, err := time.Parse(time.RFC3339Nano, completed)
		if err != nil {
			return Step7ProjectionCompletionManifest{}, fmt.Errorf(
				"invalid Step-7 completion timestamp %q: %w", completed, err)
		}
		if maxCompleted.IsZero() || completedAt.After(maxCompleted) {
			maxCompleted = completedAt
			out.MaxCompletedTS = step7Time(completedAt)
		}
	}
	if err := rows.Err(); err != nil {
		return Step7ProjectionCompletionManifest{}, err
	}
	out.Hash = hex.EncodeToString(h.Sum(nil))
	return out, nil
}

// Step7ProjectionPendingInWindow is the completeness fence used by inference and promotion.
// Empty start means all history before end. It follows each job to its raw source timestamp; job
// creation/attempt clocks can never substitute for the decision-time raw clock.
func (s *Store) Step7ProjectionPendingInWindow(ctx context.Context, start, end time.Time) (int, error) {
	return step7ProjectionPendingInWindow(ctx, s.db, start, end)
}

func step7ProjectionPendingInWindow(ctx context.Context, q step7ProjectionQuerier,
	start, end time.Time) (int, error) {
	if end.IsZero() {
		end = time.Now().UTC()
	}
	endText, startText := step7Time(end), step7Time(start)
	query := `SELECT COUNT(*) FROM research_step7_projection_jobs
WHERE completed_ts='' AND julianday(raw_observed_ts)<julianday(?)`
	args := []any{endText}
	if !start.IsZero() {
		query += ` AND julianday(raw_observed_ts)>=julianday(?)`
		args = append(args, startText)
	}
	var count int
	err := q.QueryRowContext(ctx, query, args...).Scan(&count)
	return count, err
}

func validateStep7ProjectionFenceTx(ctx context.Context, tx *sql.Tx, start, end time.Time,
	want Step7ProjectionCompletionManifest) error {
	pending, err := step7ProjectionPendingInWindow(ctx, tx, start, end)
	if err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("Step-7 projection fence changed: %d projections pending", pending)
	}
	got, err := step7ProjectionCompletionManifest(ctx, tx, end)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("Step-7 projection completion manifest changed: got %+v want %+v",
			got, want)
	}
	return nil
}

func step7SHA256(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(strconv.Itoa(len(part))))
		_, _ = h.Write([]byte{':'})
		_, _ = h.Write([]byte(part))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func Step7ProjectionJobKey(rawKind, rawID string) string {
	return step7SHA256("step7-projection-job-v1", strings.TrimSpace(rawKind), strings.TrimSpace(rawID))
}

func step7ProjectionSourceKey(rawKind, rawID string) string {
	return strings.TrimSpace(rawKind) + ":" + strings.TrimSpace(rawID)
}

func Step7MarkProjectionRawID(episodeID, eventType string, horizonMS int64) string {
	return strings.TrimSpace(episodeID) + "|" + strings.TrimSpace(eventType) + "|" + strconv.FormatInt(horizonMS, 10)
}

func step7ProjectionFingerprint(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return step7SHA256("step7-raw-source-v1", string(b)), nil
}

func step7EpisodeProjectionIdentity(row Step7ReplenishmentEpisode) (string, string, error) {
	evidence, err := step7JSON(row.Evidence)
	if err != nil {
		return "", "", err
	}
	rawID := strings.TrimSpace(row.EpisodeID)
	fp, err := step7ProjectionFingerprint(struct {
		EpisodeID, Observed, Ticker, Side, SourceClock, Evidence string
		Generation                                               uint64
		SubscriptionID                                           int64
		Sequence, PriorSequence, SequenceGap                     int64
		StartPrice, StartDepth, DepletedPrice, DepletedDepth     float64
		DepletionUnits, TradeUnits, Unexplained, QueueChurn      float64
	}{rawID, step7Time(row.Observed), row.Ticker, row.Side, row.SourceClock, evidence,
		row.Generation, row.SubscriptionID, row.Sequence, row.PriorSequence, row.SequenceGap,
		row.StartPrice, row.StartDepth, row.DepletedPrice, row.DepletedDepth, row.DepletionUnits,
		row.TradeUnits, row.UnexplainedUnits, row.QueueChurnUnits})
	return rawID, fp, err
}

func step7MarkProjectionIdentity(row Step7ReplenishmentMark) (string, string, error) {
	evidence, err := step7JSON(row.Evidence)
	if err != nil {
		return "", "", err
	}
	rawID := Step7MarkProjectionRawID(row.EpisodeID, row.EventType, row.HorizonMS)
	fp, err := step7ProjectionFingerprint(struct {
		EpisodeID, Observed, EventType, Evidence              string
		HorizonMS, SequenceGap                                int64
		BookPrice, BookDepth, RefillLatencyMS, RefillFraction float64
		TradeUnits, Unexplained, QueueChurn                   float64
	}{row.EpisodeID, step7Time(row.Observed), row.EventType, evidence, row.HorizonMS,
		row.SequenceGap, row.BookPrice, row.BookDepth, row.RefillLatencyMS, row.RefillFraction,
		row.TradeUnits, row.UnexplainedUnits, row.QueueChurnUnits})
	return rawID, fp, err
}

func step7FlowProjectionIdentity(row Step7FlowDirectionPair) (string, string, error) {
	evidence, err := step7JSON(row.Evidence)
	if err != nil {
		return "", "", err
	}
	rawID := strings.TrimSpace(row.PairID)
	fp, err := step7ProjectionFingerprint(struct {
		PairID, Observed, SourceAt, Ticker, TradeID                 string
		TakerOutcomeSide, TakerBookSide, LegacyTakerSide            string
		LegacyOutcomeSide, AuthoritativeSide, InferredSide          string
		ComparisonStatus, BookObserved, SourceClockStatus, Evidence string
		Count, YesPrice, BookAgeMS, YesBid, YesAsk                  float64
		BookGeneration                                              uint64
		BookSequence, TradeSequence                                 int64
	}{rawID, step7Time(row.Observed), step7Time(row.SourceAt), row.Ticker, row.TradeID,
		row.TakerOutcomeSide, row.TakerBookSide, row.LegacyTakerSide, row.LegacyOutcomeSide,
		row.AuthoritativeSide, row.InferredSide, row.ComparisonStatus, step7Time(row.BookObserved),
		row.SourceClockStatus, evidence, row.Count, row.YesPrice, row.BookAgeMS, row.YesBid,
		row.YesAsk, row.BookGeneration, row.BookSequence, row.TradeSequence})
	return rawID, fp, err
}

func step7FlowTradeReplayFingerprint(row Step7FlowDirectionPair) string {
	return step7SHA256("step7-flow-trade-replay-v1", strings.TrimSpace(row.PairID),
		row.Ticker, row.TradeID, step7Time(row.SourceAt), row.AuthoritativeSide,
		strconv.FormatFloat(row.Count, 'g', -1, 64),
		strconv.FormatFloat(row.YesPrice, 'g', -1, 64))
}

func NewStep7ProjectionJob(rawKind, rawID, sourceFingerprint string, payloadVersion int,
	terminalReason string, outputs []Step7FrozenObservation) (Step7ProjectionJob, error) {
	rawKind, rawID = strings.TrimSpace(rawKind), strings.TrimSpace(rawID)
	sourceFingerprint = strings.ToLower(strings.TrimSpace(sourceFingerprint))
	terminalReason = strings.TrimSpace(terminalReason)
	if (rawKind != Step7ProjectionEpisode && rawKind != Step7ProjectionMark &&
		rawKind != Step7ProjectionFlow) || rawID == "" || len(sourceFingerprint) != 64 ||
		payloadVersion <= 0 {
		return Step7ProjectionJob{}, errors.New("invalid Step-7 projection job identity")
	}
	if len(outputs) == 0 && terminalReason == "" {
		return Step7ProjectionJob{}, errors.New("zero-output Step-7 projection job lacks terminal reason")
	}
	frozen := append([]Step7FrozenObservation(nil), outputs...)
	for i := range frozen {
		route := strings.ToLower(strings.TrimSpace(frozen[i].Observation.Route))
		if route == "observer" {
			if frozen[i].InstrumentVersion != 0 {
				return Step7ProjectionJob{}, errors.New("observer projection claims instrument version")
			}
		} else if frozen[i].InstrumentVersion <= 0 {
			return Step7ProjectionJob{}, errors.New("economic projection lacks exact instrument version")
		}
		frozen[i].Observation.InstrumentVersion = frozen[i].InstrumentVersion
	}
	sourceKey := step7ProjectionSourceKey(rawKind, rawID)
	payload := step7ProjectionPayload{Version: payloadVersion, RawKind: rawKind, RawID: rawID,
		SourceKey: sourceKey, SourceFingerprint: sourceFingerprint, TerminalReason: terminalReason,
		Outputs: frozen}
	body, err := json.Marshal(payload)
	if err != nil {
		return Step7ProjectionJob{}, err
	}
	hash := step7SHA256("step7-projection-payload-v1", string(body))
	return Step7ProjectionJob{JobID: Step7ProjectionJobKey(rawKind, rawID), RawKind: rawKind,
		RawID: rawID, SourceKey: sourceKey, SourceFingerprint: sourceFingerprint,
		ReplayFingerprint: sourceFingerprint,
		PayloadVersion:    payloadVersion, PayloadJSON: string(body), PayloadHash: hash,
		TerminalReason: terminalReason}, nil
}

func NewStep7EpisodeProjectionJob(row Step7ReplenishmentEpisode, payloadVersion int,
	terminalReason string, outputs []Step7FrozenObservation) (Step7ProjectionJob, error) {
	rawID, fingerprint, err := step7EpisodeProjectionIdentity(row)
	if err != nil {
		return Step7ProjectionJob{}, err
	}
	job, err := NewStep7ProjectionJob(Step7ProjectionEpisode, rawID, fingerprint, payloadVersion,
		terminalReason, outputs)
	job.RawObservedTS = step7Time(row.Observed)
	return job, err
}

func NewStep7MarkProjectionJob(row Step7ReplenishmentMark, payloadVersion int,
	terminalReason string, outputs []Step7FrozenObservation) (Step7ProjectionJob, error) {
	rawID, fingerprint, err := step7MarkProjectionIdentity(row)
	if err != nil {
		return Step7ProjectionJob{}, err
	}
	job, err := NewStep7ProjectionJob(Step7ProjectionMark, rawID, fingerprint, payloadVersion,
		terminalReason, outputs)
	job.RawObservedTS = step7Time(row.Observed)
	return job, err
}

func NewStep7FlowProjectionJob(row Step7FlowDirectionPair, payloadVersion int,
	terminalReason string, outputs []Step7FrozenObservation) (Step7ProjectionJob, error) {
	rawID, fingerprint, err := step7FlowProjectionIdentity(row)
	if err != nil {
		return Step7ProjectionJob{}, err
	}
	job, err := NewStep7ProjectionJob(Step7ProjectionFlow, rawID, fingerprint, payloadVersion,
		terminalReason, outputs)
	job.RawObservedTS = step7Time(row.Observed)
	job.ReplayFingerprint = step7FlowTradeReplayFingerprint(row)
	return job, err
}

type step7ExpectedProjection struct {
	rawKind, rawID, fingerprint, replayFingerprint, observedTS string
}

var errStep7ProjectionConflict = errors.New("conflicting immutable Step-7 projection retry")

func step7ExpectedProjectionRows(episodes []Step7ReplenishmentEpisode, marks []Step7ReplenishmentMark,
	flows []Step7FlowDirectionPair) (map[string]step7ExpectedProjection, error) {
	out := make(map[string]step7ExpectedProjection, len(episodes)+len(marks)+len(flows))
	add := func(kind, rawID, fingerprint, replayFingerprint, observedTS string, err error) error {
		if err != nil {
			return err
		}
		key := step7ProjectionSourceKey(kind, rawID)
		if rawID == "" || fingerprint == "" {
			return errors.New("empty Step-7 projection source identity")
		}
		if prior, exists := out[key]; exists {
			if prior.fingerprint != fingerprint {
				return fmt.Errorf("conflicting duplicate Step-7 raw source %s", key)
			}
			return nil // an identical callback replay is one raw source, not a second job
		}
		out[key] = step7ExpectedProjection{kind, rawID, fingerprint, replayFingerprint, observedTS}
		return nil
	}
	for _, row := range episodes {
		id, fp, err := step7EpisodeProjectionIdentity(row)
		if err := add(Step7ProjectionEpisode, id, fp, fp, step7Time(row.Observed), err); err != nil {
			return nil, err
		}
	}
	for _, row := range marks {
		id, fp, err := step7MarkProjectionIdentity(row)
		if err := add(Step7ProjectionMark, id, fp, fp, step7Time(row.Observed), err); err != nil {
			return nil, err
		}
	}
	for _, row := range flows {
		id, fp, err := step7FlowProjectionIdentity(row)
		if err := add(Step7ProjectionFlow, id, fp, step7FlowTradeReplayFingerprint(row),
			step7Time(row.Observed), err); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func validateStep7ProjectionJobs(expected map[string]step7ExpectedProjection,
	jobs []Step7ProjectionJob) error {
	if len(jobs) != len(expected) {
		return fmt.Errorf("Step-7 projection cardinality mismatch: raw=%d jobs=%d", len(expected), len(jobs))
	}
	seen := make(map[string]struct{}, len(jobs))
	for _, job := range jobs {
		want, ok := expected[job.SourceKey]
		if !ok || job.RawKind != want.rawKind || job.RawID != want.rawID ||
			job.SourceFingerprint != want.fingerprint || job.RawObservedTS != want.observedTS ||
			job.ReplayFingerprint != want.replayFingerprint ||
			job.JobID != Step7ProjectionJobKey(job.RawKind, job.RawID) ||
			job.SourceKey != step7ProjectionSourceKey(job.RawKind, job.RawID) {
			return fmt.Errorf("Step-7 projection source key/fingerprint mismatch for %q", job.SourceKey)
		}
		if _, duplicate := seen[job.SourceKey]; duplicate {
			return fmt.Errorf("duplicate Step-7 projection job %q", job.SourceKey)
		}
		seen[job.SourceKey] = struct{}{}
		var payload step7ProjectionPayload
		if job.PayloadVersion <= 0 || !json.Valid([]byte(job.PayloadJSON)) ||
			step7SHA256("step7-projection-payload-v1", job.PayloadJSON) != job.PayloadHash ||
			json.Unmarshal([]byte(job.PayloadJSON), &payload) != nil ||
			payload.Version != job.PayloadVersion || payload.RawKind != job.RawKind ||
			payload.RawID != job.RawID || payload.SourceKey != job.SourceKey ||
			payload.SourceFingerprint != job.SourceFingerprint ||
			payload.TerminalReason != job.TerminalReason {
			return fmt.Errorf("invalid frozen Step-7 projection payload for %q", job.SourceKey)
		}
		if len(payload.Outputs) == 0 && payload.TerminalReason == "" {
			return fmt.Errorf("zero-output Step-7 projection payload lacks terminal reason for %q", job.SourceKey)
		}
	}
	return nil
}

func validateExistingStep7ProjectionJobTx(ctx context.Context, tx *sql.Tx,
	job Step7ProjectionJob) error {
	var jobID, rawKind, rawID, rawObserved, sourceKey, fingerprint, replayFingerprint, payloadHash, reason string
	var version int
	err := tx.QueryRowContext(ctx, `SELECT job_id,raw_kind,raw_id,raw_observed_ts,source_key,source_fingerprint,
replay_fingerprint,payload_version,payload_hash,terminal_reason FROM research_step7_projection_jobs
WHERE source_key=?`, job.SourceKey).Scan(&jobID, &rawKind, &rawID, &rawObserved, &sourceKey,
		&fingerprint, &replayFingerprint, &version, &payloadHash, &reason)
	if err != nil {
		return err
	}
	if jobID != job.JobID || rawKind != job.RawKind || rawID != job.RawID ||
		rawObserved != job.RawObservedTS || sourceKey != job.SourceKey ||
		fingerprint != job.SourceFingerprint ||
		replayFingerprint != job.ReplayFingerprint ||
		version != job.PayloadVersion || payloadHash != job.PayloadHash ||
		reason != job.TerminalReason {
		return fmt.Errorf("%w for %s", errStep7ProjectionConflict, job.SourceKey)
	}
	return nil
}

// classifyStep7ProjectionBatchTx distinguishes a genuinely new source from an exact replay and
// from a legacy raw row that predates the outbox. Exact replays skip both raw and job writes.
// Legacy collisions get a durable no-backfill tombstone; they never receive later economics.
func classifyStep7ProjectionBatchTx(ctx context.Context, tx *sql.Tx, jobs []Step7ProjectionJob,
	flowRows map[string]Step7FlowDirectionPair,
) (map[string]bool, []Step7ProjectionJob, error) {
	newSources := make(map[string]bool, len(jobs))
	newJobs := make([]Step7ProjectionJob, 0, len(jobs))
	now := step7Time(time.Now().UTC())
	for _, job := range jobs {
		err := validateExistingStep7ProjectionJobTx(ctx, tx, job)
		if err == nil {
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			if errors.Is(err, errStep7ProjectionConflict) && job.RawKind == Step7ProjectionFlow {
				_, ok := flowRows[job.RawID]
				if !ok {
					return nil, nil, fmt.Errorf("flow replay %s lacks incoming immutable trade", job.RawID)
				}
				var storedFingerprint, storedReplayFingerprint string
				rawErr := tx.QueryRowContext(ctx, `SELECT source_fingerprint,replay_fingerprint
FROM research_step7_projection_jobs WHERE raw_kind='flow' AND raw_id=?`,
					job.RawID).Scan(&storedFingerprint, &storedReplayFingerprint)
				if rawErr != nil {
					return nil, nil, fmt.Errorf("durable flow replay source %s: %w",
						job.RawID, rawErr)
				}
				if storedFingerprint == job.SourceFingerprint {
					return nil, nil, err // same raw snapshot tried to change its frozen projection
				}
				if storedReplayFingerprint == job.ReplayFingerprint {
					continue // venue trade is identical; preserve the first frozen raw/job snapshot
				}
				return nil, nil, fmt.Errorf(
					"conflicting immutable Step-7 flow trade replay %s", job.RawID)
			}
			return nil, nil, err
		}
		var legacyKind, legacyRawID, legacyFingerprint, legacyHash string
		legacyErr := tx.QueryRowContext(ctx, `SELECT raw_kind,raw_id,source_fingerprint,
rejected_payload_hash FROM research_step7_projection_legacy_collisions WHERE source_key=?`,
			job.SourceKey).Scan(&legacyKind, &legacyRawID, &legacyFingerprint, &legacyHash)
		if legacyErr == nil {
			if legacyKind != job.RawKind || legacyRawID != job.RawID ||
				legacyFingerprint != job.SourceFingerprint || legacyHash != job.PayloadHash {
				return nil, nil, fmt.Errorf("conflicting Step-7 legacy collision %s", job.SourceKey)
			}
			continue
		}
		if !errors.Is(legacyErr, sql.ErrNoRows) {
			return nil, nil, legacyErr
		}
		var payload step7ProjectionPayload
		if err := json.Unmarshal([]byte(job.PayloadJSON), &payload); err != nil {
			return nil, nil, err
		}
		sourceErr := validateStep7ProjectionSourceTx(ctx, tx, payload)
		switch {
		case sourceErr == nil:
			res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_step7_projection_legacy_collisions(
source_key,raw_kind,raw_id,source_fingerprint,rejected_payload_hash,detected_ts,reason)
VALUES(?,?,?,?,?,?,'legacy_raw_without_frozen_projection_no_backfill')`, job.SourceKey, job.RawKind,
				job.RawID, job.SourceFingerprint, job.PayloadHash, now)
			if err != nil {
				return nil, nil, err
			}
			if n, _ := res.RowsAffected(); n == 0 {
				var kind, rawID, fingerprint, hash string
				if err := tx.QueryRowContext(ctx, `SELECT raw_kind,raw_id,source_fingerprint,
rejected_payload_hash FROM research_step7_projection_legacy_collisions WHERE source_key=?`,
					job.SourceKey).Scan(&kind, &rawID, &fingerprint, &hash); err != nil {
					return nil, nil, err
				}
				if kind != job.RawKind || rawID != job.RawID ||
					fingerprint != job.SourceFingerprint || hash != job.PayloadHash {
					return nil, nil, fmt.Errorf("conflicting Step-7 legacy collision %s", job.SourceKey)
				}
			}
			continue
		case errors.Is(sourceErr, sql.ErrNoRows):
			newSources[job.SourceKey] = true
			newJobs = append(newJobs, job)
		default:
			return nil, nil, sourceErr
		}
	}
	return newSources, newJobs, nil
}

func insertStep7ProjectionJobsTx(ctx context.Context, tx *sql.Tx, jobs []Step7ProjectionJob,
	created time.Time) (int, error) {
	n := 0
	now := step7Time(created)
	if now == "" {
		now = step7Time(time.Now().UTC())
	}
	for _, job := range jobs {
		outputCount := step7ProjectionOutputCount(job.PayloadJSON)
		completed := ""
		if outputCount == 0 {
			// A terminal no-output projection has no worker work to perform. Commit its explicit
			// completion beside the raw source rather than creating a pointless pending window.
			completed = now
		}
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_step7_projection_jobs(
job_id,raw_kind,raw_id,raw_observed_ts,source_key,source_fingerprint,replay_fingerprint,payload_version,payload_hash,
terminal_reason,created_ts,completed_ts,output_count)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			job.JobID, job.RawKind, job.RawID, job.RawObservedTS, job.SourceKey, job.SourceFingerprint,
			job.ReplayFingerprint, job.PayloadVersion,
			job.PayloadHash, job.TerminalReason, now, completed, outputCount)
		if err != nil {
			return 0, err
		}
		affected, _ := res.RowsAffected()
		if affected > 0 {
			n++
			if outputCount == 0 {
				if _, err := tx.ExecContext(ctx, `INSERT INTO research_step7_projection_completions(
job_id,completed_ts,payload_hash,output_count,terminal_reason) VALUES(?,?,?,?,?)`,
					job.JobID, now, job.PayloadHash, 0, job.TerminalReason); err != nil {
					return 0, err
				}
			} else if _, err := tx.ExecContext(ctx, `INSERT INTO research_step7_projection_pending(
job_id,payload_json) VALUES(?,?)`, job.JobID, job.PayloadJSON); err != nil {
				return 0, err
			}
			continue
		}
		if err := validateExistingStep7ProjectionJobTx(ctx, tx, job); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func step7ProjectionOutputCount(payloadJSON string) int {
	var payload step7ProjectionPayload
	if json.Unmarshal([]byte(payloadJSON), &payload) != nil {
		return 0
	}
	return len(payload.Outputs)
}

func step7ProjectionTime(value string) (time.Time, error) {
	if strings.TrimSpace(value) == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, value)
}

// validateStep7ProjectionSourceTx recomputes the source fingerprint from the committed raw row.
// Enqueue already checked caller bytes; replay checks durable bytes again so a changed/deleted
// source can never project under a still-valid payload hash.
func validateStep7ProjectionSourceTx(ctx context.Context, tx *sql.Tx,
	payload step7ProjectionPayload) error {
	var got string
	switch payload.RawKind {
	case Step7ProjectionEpisode:
		var row Step7ReplenishmentEpisode
		var observed, evidence string
		var generation int64
		err := tx.QueryRowContext(ctx, `SELECT episode_id,observed_ts,ticker,side,
source_generation,source_subscription_id,source_sequence,prior_source_sequence,sequence_gap,
start_price,start_depth,depleted_price,depleted_depth,depletion_units,authoritative_trade_units,
unexplained_removal_units,queue_churn_proxy_units,source_clock,evidence_json
FROM research_replenishment_episodes WHERE episode_id=?`, payload.RawID).Scan(
			&row.EpisodeID, &observed, &row.Ticker, &row.Side, &generation, &row.SubscriptionID,
			&row.Sequence, &row.PriorSequence, &row.SequenceGap, &row.StartPrice, &row.StartDepth,
			&row.DepletedPrice, &row.DepletedDepth, &row.DepletionUnits, &row.TradeUnits,
			&row.UnexplainedUnits, &row.QueueChurnUnits, &row.SourceClock, &evidence)
		if err != nil {
			return fmt.Errorf("projection episode source: %w", err)
		}
		row.Generation = uint64(generation)
		row.Observed, err = step7ProjectionTime(observed)
		if err != nil {
			return err
		}
		row.Evidence = json.RawMessage(evidence)
		_, got, err = step7EpisodeProjectionIdentity(row)
		if err != nil {
			return err
		}
	case Step7ProjectionMark:
		parts := strings.Split(payload.RawID, "|")
		if len(parts) != 3 {
			return errors.New("projection mark source key is malformed")
		}
		horizon, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return err
		}
		var row Step7ReplenishmentMark
		var observed, evidence string
		var latency sql.NullFloat64
		err = tx.QueryRowContext(ctx, `SELECT episode_id,observed_ts,event_type,horizon_ms,
book_price,book_depth,refill_latency_ms,refill_fraction,authoritative_trade_units,
unexplained_removal_units,queue_churn_proxy_units,sequence_gap,evidence_json
FROM research_replenishment_marks WHERE episode_id=? AND event_type=? AND horizon_ms=?`,
			parts[0], parts[1], horizon).Scan(&row.EpisodeID, &observed, &row.EventType,
			&row.HorizonMS, &row.BookPrice, &row.BookDepth, &latency, &row.RefillFraction,
			&row.TradeUnits, &row.UnexplainedUnits, &row.QueueChurnUnits, &row.SequenceGap,
			&evidence)
		if err != nil {
			return fmt.Errorf("projection mark source: %w", err)
		}
		if latency.Valid {
			row.RefillLatencyMS = latency.Float64
		}
		row.Observed, err = step7ProjectionTime(observed)
		if err != nil {
			return err
		}
		row.Evidence = json.RawMessage(evidence)
		_, got, err = step7MarkProjectionIdentity(row)
		if err != nil {
			return err
		}
	case Step7ProjectionFlow:
		var row Step7FlowDirectionPair
		var observed, sourceAt, bookObserved, evidence string
		var generation int64
		err := tx.QueryRowContext(ctx, `SELECT pair_id,observed_ts,source_ts,ticker,trade_id,
count_units,yes_price,taker_outcome_side,taker_book_side,legacy_taker_side,legacy_outcome_side,
authoritative_side,inferred_side,comparison_status,book_observed_ts,book_age_ms,yes_bid,yes_ask,
book_source_generation,book_source_sequence,trade_source_sequence,source_clock_status,evidence_json
FROM research_flow_direction_pairs WHERE pair_id=?`, payload.RawID).Scan(
			&row.PairID, &observed, &sourceAt, &row.Ticker, &row.TradeID, &row.Count, &row.YesPrice,
			&row.TakerOutcomeSide, &row.TakerBookSide, &row.LegacyTakerSide, &row.LegacyOutcomeSide,
			&row.AuthoritativeSide, &row.InferredSide, &row.ComparisonStatus, &bookObserved,
			&row.BookAgeMS, &row.YesBid, &row.YesAsk, &generation, &row.BookSequence,
			&row.TradeSequence, &row.SourceClockStatus, &evidence)
		if err != nil {
			return fmt.Errorf("projection flow source: %w", err)
		}
		row.BookGeneration = uint64(generation)
		if row.Observed, err = step7ProjectionTime(observed); err != nil {
			return err
		}
		if row.SourceAt, err = step7ProjectionTime(sourceAt); err != nil {
			return err
		}
		if row.BookObserved, err = step7ProjectionTime(bookObserved); err != nil {
			return err
		}
		row.Evidence = json.RawMessage(evidence)
		_, got, err = step7FlowProjectionIdentity(row)
		if err != nil {
			return err
		}
	default:
		return errors.New("projection source kind is unknown")
	}
	if payload.SourceKey != step7ProjectionSourceKey(payload.RawKind, payload.RawID) ||
		got != payload.SourceFingerprint {
		return errors.New("committed Step-7 source key/fingerprint mismatch")
	}
	return nil
}

type step7PendingProjection struct {
	jobID, payloadJSON, payloadHash, terminalReason string
	payloadVersion, outputCount                     int
}

// ApplyStep7ProjectionOutbox takes a bounded, retry-fair page. Each job's frozen observations,
// unified route mirrors, completion receipt and pending-state transition commit together.
func (s *Store) ApplyStep7ProjectionOutbox(ctx context.Context, limit int) (Step7ProjectionApplyResult, error) {
	if limit <= 0 || limit > 256 {
		limit = 64
	}
	rows, err := s.db.QueryContext(ctx, `SELECT j.job_id,j.payload_version,p.payload_json,j.payload_hash,
j.terminal_reason,j.output_count FROM research_step7_projection_pending p
JOIN research_step7_projection_jobs j ON j.job_id=p.job_id
ORDER BY CASE WHEN p.last_attempt_ts='' THEN j.created_ts ELSE p.last_attempt_ts END,
j.job_id LIMIT ?`, limit)
	if err != nil {
		return Step7ProjectionApplyResult{}, err
	}
	var pending []step7PendingProjection
	for rows.Next() {
		var row step7PendingProjection
		if err := rows.Scan(&row.jobID, &row.payloadVersion, &row.payloadJSON, &row.payloadHash,
			&row.terminalReason, &row.outputCount); err != nil {
			rows.Close()
			return Step7ProjectionApplyResult{}, err
		}
		pending = append(pending, row)
	}
	if err := rows.Close(); err != nil {
		return Step7ProjectionApplyResult{}, err
	}
	result := Step7ProjectionApplyResult{Selected: len(pending)}
	for _, job := range pending {
		inserted, duplicate, applyErr := s.applyStep7ProjectionJob(ctx, job)
		if applyErr != nil {
			result.Failed++
			result.LastError = applyErr.Error()
			if err := s.recordStep7ProjectionFailure(ctx, job, applyErr); err != nil {
				return result, err
			}
			continue
		}
		result.Completed++
		result.OutputsInserted += inserted
		result.OutputsDuplicate += duplicate
	}
	var oldest string
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MIN(j.created_ts),'')
FROM research_step7_projection_pending p JOIN research_step7_projection_jobs j
 ON j.job_id=p.job_id`).Scan(&result.Pending, &oldest); err != nil {
		return result, err
	}
	if parsed, err := time.Parse(time.RFC3339Nano, oldest); err == nil {
		result.OldestPendingAge = time.Since(parsed)
		if result.OldestPendingAge < 0 {
			result.OldestPendingAge = 0
		}
	}
	return result, nil
}

func (s *Store) applyStep7ProjectionJob(ctx context.Context, job step7PendingProjection) (int, int, error) {
	if step7SHA256("step7-projection-payload-v1", job.payloadJSON) != job.payloadHash {
		return 0, 0, errors.New("stored Step-7 projection payload hash mismatch")
	}
	var payload step7ProjectionPayload
	if err := json.Unmarshal([]byte(job.payloadJSON), &payload); err != nil {
		return 0, 0, err
	}
	if payload.Version != job.payloadVersion || payload.TerminalReason != job.terminalReason ||
		len(payload.Outputs) != job.outputCount || (len(payload.Outputs) == 0 && payload.TerminalReason == "") {
		return 0, 0, errors.New("stored Step-7 projection payload envelope mismatch")
	}
	if job.jobID != Step7ProjectionJobKey(payload.RawKind, payload.RawID) {
		return 0, 0, errors.New("stored Step-7 projection source key mismatch")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer tx.Rollback()
	var completed string
	if err := tx.QueryRowContext(ctx, `SELECT completed_ts FROM research_step7_projection_jobs
WHERE job_id=?`, job.jobID).Scan(&completed); err != nil {
		return 0, 0, err
	}
	if completed != "" {
		return 0, 0, nil
	}
	if err := validateStep7ProjectionSourceTx(ctx, tx, payload); err != nil {
		return 0, 0, err
	}
	outputN, insertedN := 0, 0
	for _, frozen := range payload.Outputs {
		frozen.Observation.InstrumentVersion = frozen.InstrumentVersion
		_, inserted, err := s.insertResearchSystemObservationTx(ctx, tx, frozen.Observation,
			frozen.InstrumentVersion)
		if err != nil {
			return 0, 0, err
		}
		if inserted {
			insertedN++
		}
		outputN++
	}
	now := step7Time(time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_step7_projection_completions(
job_id,completed_ts,payload_hash,output_count,terminal_reason) VALUES(?,?,?,?,?)`,
		job.jobID, now, job.payloadHash, outputN, payload.TerminalReason); err != nil {
		return 0, 0, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE research_step7_projection_jobs SET
completed_ts=?,output_count=? WHERE job_id=? AND completed_ts=''`, now, outputN, job.jobID)
	if err != nil {
		return 0, 0, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return 0, 0, errors.New("Step-7 projection completion lost pending ownership")
	}
	if res, err := tx.ExecContext(ctx, `DELETE FROM research_step7_projection_pending
WHERE job_id=?`, job.jobID); err != nil {
		return 0, 0, err
	} else if n, _ := res.RowsAffected(); n != 1 {
		return 0, 0, errors.New("Step-7 projection completion did not remove pending payload")
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}
	return insertedN, outputN - insertedN, nil
}

func (s *Store) recordStep7ProjectionFailure(ctx context.Context, job step7PendingProjection,
	cause error) error {
	now := step7Time(time.Now().UTC())
	message := cause.Error()
	if len(message) > 1000 {
		message = message[:1000]
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_step7_projection_attempts(
job_id,attempted_ts,payload_hash,result,output_count,error_text)
VALUES(?,?,?,'failed',0,?)`, job.jobID, now, job.payloadHash, message); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE research_step7_projection_pending SET
attempt_count=attempt_count+1,last_attempt_ts=?,last_error=? WHERE job_id=?`,
		now, message, job.jobID); err != nil {
		return err
	}
	return tx.Commit()
}
