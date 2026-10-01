package storage

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func step7ProjectionTestFlow(id string, observed time.Time) Step7FlowDirectionPair {
	return Step7FlowDirectionPair{PairID: id, Ticker: "KX-STEP7-" + id, TradeID: "trade-" + id,
		Observed: observed, Count: 1, YesPrice: .42, AuthoritativeSide: "yes",
		InferredSide: "yes", ComparisonStatus: "agreement", SourceClockStatus: "sequenced",
		BookAgeMS: 1, YesBid: .41, YesAsk: .42, BookGeneration: 1, BookSequence: 2,
		TradeSequence: 3, Evidence: map[string]any{"source": "fixture"}}
}

func step7ProjectionObserver(opportunity string, observed time.Time) ResearchSystemObservation {
	return ResearchSystemObservation{Observed: observed, SystemID: "replenishment-fingerprint",
		OpportunityID: opportunity, Kind: "control", Cohort: "step7-projection-test",
		Venue: "kalshi", Ticker: "KX-STEP7", Route: "observer", Side: "YES",
		CertificateStatus: "not_applicable", OutcomeStatus: "open",
		Inputs: map[string]any{"frozen": true}}
}

func TestStep7ProjectionMigrationAtomicCardinalityAndConflict(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	var instrumentColumn, outboxTable int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('research_system_observations')
WHERE name='instrument_version'`).Scan(&instrumentColumn); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table'
AND name='research_step7_projection_jobs'`).Scan(&outboxTable); err != nil {
		t.Fatal(err)
	}
	if instrumentColumn != 1 || outboxTable != 1 {
		t.Fatalf("migration instrument_column=%d outbox=%d", instrumentColumn, outboxTable)
	}

	flow := step7ProjectionTestFlow("atomic", time.Now().UTC())
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, nil); err == nil ||
		!strings.Contains(err.Error(), "cardinality mismatch") {
		t.Fatalf("raw committed without its exact job: %v", err)
	}
	var raws int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_flow_direction_pairs
WHERE pair_id=?`, flow.PairID).Scan(&raws); err != nil || raws != 0 {
		t.Fatalf("non-atomic raw count=%d err=%v", raws, err)
	}
	job, err := NewStep7FlowProjectionJob(flow, 1, "no eligible projection", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, flowN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil ||
		flowN != 1 || jobN != 1 {
		t.Fatalf("atomic raw/job insert=%d/%d err=%v", flowN, jobN, err)
	}
	var completions int
	var completedTS string
	if err := st.db.QueryRow(`SELECT completed_ts FROM research_step7_projection_jobs
WHERE job_id=?`, job.JobID).Scan(&completedTS); err != nil || completedTS == "" {
		t.Fatalf("zero-output job was not atomically completed: %q err=%v", completedTS, err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_step7_projection_completions
WHERE job_id=?`, job.JobID).Scan(&completions); err != nil || completions != 1 {
		t.Fatalf("completion receipt=%d err=%v", completions, err)
	}
	conflict, err := NewStep7FlowProjectionJob(flow, 1, "changed terminal meaning", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{conflict}); err == nil ||
		!strings.Contains(err.Error(), "conflicting immutable") {
		t.Fatalf("changed payload did not hard-fail: %v", err)
	}
	mismatch := job
	mismatch.SourceFingerprint = strings.Repeat("f", 64)
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{mismatch}); err == nil ||
		!strings.Contains(err.Error(), "fingerprint mismatch") {
		t.Fatalf("source fingerprint mismatch did not hard-fail: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE research_step7_projection_jobs SET payload_hash=?
WHERE job_id=?`, strings.Repeat("e", 64), job.JobID); err == nil {
		t.Fatal("database allowed projection payload mutation")
	}

	duplicate := step7ProjectionTestFlow("same-batch-replay", time.Now().UTC())
	duplicateJob, err := NewStep7FlowProjectionJob(duplicate, 1, "duplicate fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, rawN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{duplicate, duplicate}, []Step7ProjectionJob{duplicateJob}); err != nil || rawN != 1 || jobN != 1 {
		t.Fatalf("identical in-batch replay raw/job=%d/%d err=%v", rawN, jobN, err)
	}
	if _, _, rawN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{duplicate}, []Step7ProjectionJob{duplicateJob}); err != nil || rawN != 0 || jobN != 0 {
		t.Fatalf("durable exact replay raw/job=%d/%d err=%v", rawN, jobN, err)
	}

	legacy := step7ProjectionTestFlow("legacy-no-job", time.Now().UTC().AddDate(0, 0, -100))
	evidence, _ := step7JSON(legacy.Evidence)
	if _, err := st.db.Exec(`INSERT INTO research_flow_direction_pairs(
pair_id,observed_ts,source_ts,ticker,trade_id,count_units,yes_price,taker_outcome_side,taker_book_side,
legacy_taker_side,legacy_outcome_side,authoritative_side,inferred_side,comparison_status,book_observed_ts,
book_age_ms,yes_bid,yes_ask,book_source_generation,book_source_sequence,trade_source_sequence,
source_clock_status,evidence_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, legacy.PairID, step7Time(legacy.Observed),
		"", legacy.Ticker, legacy.TradeID, legacy.Count, legacy.YesPrice, "", "", "", "",
		legacy.AuthoritativeSide, legacy.InferredSide, legacy.ComparisonStatus, "", legacy.BookAgeMS,
		legacy.YesBid, legacy.YesAsk, legacy.BookGeneration, legacy.BookSequence, legacy.TradeSequence,
		legacy.SourceClockStatus, evidence); err != nil {
		t.Fatal(err)
	}
	legacyObservation := step7ProjectionObserver("legacy-must-not-backfill", legacy.Observed)
	legacyJob, err := NewStep7FlowProjectionJob(legacy, 1, "",
		[]Step7FrozenObservation{{Observation: legacyObservation}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, rawN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{legacy}, []Step7ProjectionJob{legacyJob}); err != nil || rawN != 0 || jobN != 0 {
		t.Fatalf("legacy collision raw/job=%d/%d err=%v", rawN, jobN, err)
	}
	var legacyJobs, legacyCollisions int
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_step7_projection_jobs
WHERE job_id=?`, legacyJob.JobID).Scan(&legacyJobs)
	_ = st.db.QueryRow(`SELECT COUNT(*) FROM research_step7_projection_legacy_collisions
WHERE source_key=?`, legacyJob.SourceKey).Scan(&legacyCollisions)
	if legacyJobs != 0 || legacyCollisions != 1 {
		t.Fatalf("legacy row was backfilled jobs/collisions=%d/%d", legacyJobs, legacyCollisions)
	}
	if _, err := st.PruneStep7Evidence(ctx, time.Now().UTC().AddDate(0, 0, -90)); err != nil {
		t.Fatal(err)
	}
	if _, _, rawN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{legacy}, []Step7ProjectionJob{legacyJob}); err != nil || rawN != 0 || jobN != 0 {
		t.Fatalf("legacy tombstone allowed raw resurrection=%d/%d err=%v", rawN, jobN, err)
	}
	changedLegacy := legacy
	changedLegacy.YesPrice = .43
	changedLegacyJob, err := NewStep7FlowProjectionJob(changedLegacy, 1, "",
		[]Step7FrozenObservation{{Observation: legacyObservation}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{changedLegacy}, []Step7ProjectionJob{changedLegacyJob}); err == nil || !strings.Contains(err.Error(), "conflicting Step-7 legacy collision") {
		t.Fatalf("changed legacy replay did not hard-fail: %v", err)
	}
}

func TestStep7ProjectionMigrationRepairsInterruptedRawClockBackfill(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	observed := time.Date(2026, 7, 25, 12, 0, 0, 100_000_000, time.UTC)
	flow := step7ProjectionTestFlow("restart-clock-backfill", observed)
	job, err := NewStep7FlowProjectionJob(flow, 1, "terminal fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`DROP TRIGGER research_step7_projection_jobs_payload_immutable;
UPDATE research_step7_projection_jobs SET raw_observed_ts='' WHERE job_id=?`,
		job.JobID); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(dir)
	if err != nil {
		t.Fatalf("restart after interrupted backfill: %v", err)
	}
	defer st.Close()
	var got, replayFingerprint string
	if err := st.db.QueryRow(`SELECT raw_observed_ts,replay_fingerprint
FROM research_step7_projection_jobs WHERE job_id=?`, job.JobID).
		Scan(&got, &replayFingerprint); err != nil || got != step7Time(observed) ||
		replayFingerprint != step7FlowTradeReplayFingerprint(flow) {
		t.Fatalf("restart backfill raw clock=%q replay=%q want=%q/%q err=%v", got,
			replayFingerprint, step7Time(observed), step7FlowTradeReplayFingerprint(flow), err)
	}
	if _, err := st.db.Exec(`UPDATE research_step7_projection_jobs SET raw_observed_ts=''
WHERE job_id=?`, job.JobID); err == nil ||
		!strings.Contains(err.Error(), "immutable Step-7 projection payload") {
		t.Fatalf("restart did not restore raw-clock immutability: %v", err)
	}
}

func TestStep7ProjectionMigrationRemovesIntermediateRequiredJobPayload(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+`\intermediate.db?_pragma=foreign_keys(ON)`)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fingerprint, payloadHash := strings.Repeat("a", 64), strings.Repeat("b", 64)
	if _, err := db.Exec(`CREATE TABLE research_step7_projection_jobs(
 job_id TEXT PRIMARY KEY,raw_kind TEXT NOT NULL,raw_id TEXT NOT NULL,raw_observed_ts TEXT NOT NULL,
 source_key TEXT NOT NULL,source_fingerprint TEXT NOT NULL,payload_version INTEGER NOT NULL,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),payload_hash TEXT NOT NULL,
 terminal_reason TEXT NOT NULL,created_ts TEXT NOT NULL,completed_ts TEXT NOT NULL DEFAULT '',
 output_count INTEGER NOT NULL DEFAULT 0,UNIQUE(raw_kind,raw_id),UNIQUE(source_key),UNIQUE(payload_hash));
CREATE TRIGGER research_step7_projection_jobs_payload_immutable
BEFORE UPDATE OF payload_json,payload_hash ON research_step7_projection_jobs
BEGIN SELECT RAISE(ABORT,'immutable Step-7 projection payload'); END;
INSERT INTO research_step7_projection_jobs(job_id,raw_kind,raw_id,raw_observed_ts,source_key,
 source_fingerprint,payload_version,payload_json,payload_hash,terminal_reason,created_ts,completed_ts,
 output_count) VALUES('legacy-job','episode','legacy-raw','2026-07-25T12:00:00Z',
 'episode:legacy-raw',?,1,'{"version":1}',?,'legacy terminal','2026-07-25T12:00:01Z',
 '2026-07-25T12:00:01Z',0)`, fingerprint, payloadHash); err != nil {
		t.Fatal(err)
	}
	if err := migrateStep7ProjectionOutbox(db); err != nil {
		t.Fatal(err)
	}
	var payloadColumn int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('research_step7_projection_jobs')
WHERE name='payload_json'`).Scan(&payloadColumn); err != nil || payloadColumn != 0 {
		t.Fatalf("intermediate payload column=%d err=%v", payloadColumn, err)
	}
	var replayFingerprint string
	if err := db.QueryRow(`SELECT replay_fingerprint FROM research_step7_projection_jobs
WHERE job_id='legacy-job'`).Scan(&replayFingerprint); err != nil || replayFingerprint != fingerprint {
		t.Fatalf("preserved job replay fingerprint=%q err=%v", replayFingerprint, err)
	}
	if _, err := db.Exec(`INSERT INTO research_step7_projection_jobs(job_id,raw_kind,raw_id,
raw_observed_ts,source_key,source_fingerprint,replay_fingerprint,payload_version,payload_hash,
terminal_reason,created_ts,completed_ts,output_count)
VALUES('current-job','episode','current-raw','2026-07-25T12:00:02Z','episode:current-raw',
?,?,1,?,'current terminal','2026-07-25T12:00:03Z','2026-07-25T12:00:03Z',0)`,
		strings.Repeat("c", 64), strings.Repeat("c", 64), strings.Repeat("d", 64)); err != nil {
		t.Fatalf("compact post-migration job insert failed: %v", err)
	}
}

func TestStep7ProjectionMigrationPreservesAndAppliesIncompleteIntermediatePayload(t *testing.T) {
	dir := t.TempDir()
	observed := time.Date(2026, 7, 25, 12, 0, 0, 123000000, time.UTC)
	flow := step7ProjectionTestFlow("intermediate-incomplete", observed)
	observation := step7ProjectionObserver("intermediate-incomplete", observed)
	job, err := NewStep7FlowProjectionJob(flow, 1, "",
		[]Step7FrozenObservation{{Observation: observation}})
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", "file:"+dir+`\kalshi.db?_pragma=foreign_keys(ON)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE research_step7_projection_jobs(
 job_id TEXT PRIMARY KEY,raw_kind TEXT NOT NULL,raw_id TEXT NOT NULL,raw_observed_ts TEXT NOT NULL,
 source_key TEXT NOT NULL,source_fingerprint TEXT NOT NULL,payload_version INTEGER NOT NULL,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),payload_hash TEXT NOT NULL,
 terminal_reason TEXT NOT NULL,created_ts TEXT NOT NULL,completed_ts TEXT NOT NULL DEFAULT '',
 output_count INTEGER NOT NULL DEFAULT 0,UNIQUE(raw_kind,raw_id),UNIQUE(source_key),UNIQUE(payload_hash));
CREATE TRIGGER research_step7_projection_jobs_payload_immutable
BEFORE UPDATE OF payload_json,payload_hash ON research_step7_projection_jobs
BEGIN SELECT RAISE(ABORT,'immutable Step-7 projection payload'); END;
INSERT INTO research_step7_projection_jobs(job_id,raw_kind,raw_id,raw_observed_ts,source_key,
 source_fingerprint,payload_version,payload_json,payload_hash,terminal_reason,created_ts,completed_ts,
 output_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,'',?)`, job.JobID, job.RawKind, job.RawID,
		job.RawObservedTS, job.SourceKey, job.SourceFingerprint, job.PayloadVersion, job.PayloadJSON,
		job.PayloadHash, job.TerminalReason, step7Time(observed.Add(time.Second)), 1); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var pendingPayload string
	if err := st.db.QueryRow(`SELECT payload_json FROM research_step7_projection_pending
WHERE job_id=?`, job.JobID).Scan(&pendingPayload); err != nil || pendingPayload != job.PayloadJSON {
		t.Fatalf("migrated pending payload=%q err=%v", pendingPayload, err)
	}
	evidence, err := step7JSON(flow.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO research_flow_direction_pairs(
pair_id,observed_ts,source_ts,ticker,trade_id,count_units,yes_price,taker_outcome_side,taker_book_side,
legacy_taker_side,legacy_outcome_side,authoritative_side,inferred_side,comparison_status,book_observed_ts,
book_age_ms,yes_bid,yes_ask,book_source_generation,book_source_sequence,trade_source_sequence,
source_clock_status,evidence_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, flow.PairID, step7Time(flow.Observed),
		step7Time(flow.SourceAt), flow.Ticker, flow.TradeID, flow.Count, flow.YesPrice,
		flow.TakerOutcomeSide, flow.TakerBookSide, flow.LegacyTakerSide, flow.LegacyOutcomeSide,
		flow.AuthoritativeSide, flow.InferredSide, flow.ComparisonStatus, step7Time(flow.BookObserved),
		flow.BookAgeMS, flow.YesBid, flow.YesAsk, flow.BookGeneration, flow.BookSequence,
		flow.TradeSequence, flow.SourceClockStatus, evidence); err != nil {
		t.Fatal(err)
	}
	result, err := st.ApplyStep7ProjectionOutbox(context.Background(), 10)
	if err != nil || result.Completed != 1 || result.OutputsInserted != 1 || result.Pending != 0 {
		t.Fatalf("preserved intermediate payload apply=%+v err=%v", result, err)
	}
	var outputs int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_system_observations
WHERE opportunity_id='intermediate-incomplete'`).Scan(&outputs); err != nil || outputs != 1 {
		t.Fatalf("applied preserved output count=%d err=%v", outputs, err)
	}
}

func TestStep7FlowRestartReplayPreservesFirstFrozenJobAndRejectsEconomicConflict(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	sourceAt := time.Date(2026, 7, 25, 12, 0, 0, 100_000_000, time.UTC)
	first := step7ProjectionTestFlow("durable-trade-replay", sourceAt.Add(time.Second))
	first.SourceAt = sourceAt
	firstJob, err := NewStep7FlowProjectionJob(first, 1, "first frozen terminal", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, flowN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{first}, []Step7ProjectionJob{firstJob}); err != nil ||
		flowN != 1 || jobN != 1 {
		t.Fatalf("first flow/job=%d/%d err=%v", flowN, jobN, err)
	}
	replay := first
	replay.Observed = replay.Observed.Add(15 * time.Minute)
	replay.BookObserved = replay.Observed.Add(-time.Millisecond)
	replay.BookGeneration, replay.BookSequence = 99, 999
	replay.TradeSequence = 1001
	replay.BookAgeMS, replay.YesBid, replay.YesAsk = 2, .40, .43
	replay.InferredSide, replay.ComparisonStatus = "no", "mismatch"
	replay.Evidence = map[string]any{"source": "restart replay with a later book"}
	replayJob, err := NewStep7FlowProjectionJob(replay, 2, "changed replay terminal", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, flowN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{replay}, []Step7ProjectionJob{replayJob}); err != nil ||
		flowN != 0 || jobN != 0 {
		t.Fatalf("same venue trade replay flow/job=%d/%d err=%v", flowN, jobN, err)
	}
	var observed, payloadHash string
	if err := st.db.QueryRow(`SELECT f.observed_ts,j.payload_hash
FROM research_flow_direction_pairs f JOIN research_step7_projection_jobs j
 ON j.raw_kind='flow' AND j.raw_id=f.pair_id WHERE f.pair_id=?`, first.PairID).
		Scan(&observed, &payloadHash); err != nil {
		t.Fatal(err)
	}
	if observed != step7Time(first.Observed) || payloadHash != firstJob.PayloadHash {
		t.Fatalf("restart replay replaced first frozen snapshot observed=%q hash=%q", observed, payloadHash)
	}
	conflict := replay
	conflict.Count = first.Count + 1
	conflictJob, err := NewStep7FlowProjectionJob(conflict, 2, "economic conflict", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{conflict}, []Step7ProjectionJob{conflictJob}); err == nil ||
		!strings.Contains(err.Error(), "conflicting immutable Step-7 flow trade replay") {
		t.Fatalf("changed venue-trade economics did not hard-fail: %v", err)
	}
}

func TestStep7ProjectionWindowsUseChronologicalRFC3339Boundaries(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	base := time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC)
	for _, fixture := range []struct {
		id       string
		observed time.Time
	}{
		{"pending-whole-second", base},
		{"pending-one-hundredth", base.Add(10 * time.Millisecond)},
	} {
		flow := step7ProjectionTestFlow(fixture.id, fixture.observed)
		job, err := NewStep7FlowProjectionJob(flow, 1, "",
			[]Step7FrozenObservation{{Observation: step7ProjectionObserver(fixture.id, fixture.observed)}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
			[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
			t.Fatal(err)
		}
	}
	var completedJobs []Step7ProjectionJob
	for _, fixture := range []struct {
		id       string
		observed time.Time
	}{
		{"complete-nine-hundredths", base.Add(90 * time.Millisecond)},
		{"complete-one-tenth", base.Add(100 * time.Millisecond)},
	} {
		flow := step7ProjectionTestFlow(fixture.id, fixture.observed)
		job, err := NewStep7FlowProjectionJob(flow, 1, "terminal boundary fixture", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
			[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
			t.Fatal(err)
		}
		completedJobs = append(completedJobs, job)
	}
	if pending, err := st.Step7ProjectionPendingInWindow(ctx, base, base.Add(11*time.Millisecond)); err != nil ||
		pending != 2 {
		t.Fatalf("same-second [00Z,.011Z) pending=%d want=2 err=%v", pending, err)
	}
	if pending, err := st.Step7ProjectionPendingInWindow(ctx, base.Add(10*time.Millisecond),
		base.Add(100*time.Millisecond)); err != nil || pending != 1 {
		t.Fatalf("same-second [.01Z,.1Z) pending=%d want=1 err=%v", pending, err)
	}
	// Force opposite lexical/chronological completion order: ".1Z" is later than ".09Z" even
	// though an ordinary TEXT MAX/compare gives the wrong answer.
	if _, err := st.db.Exec(`DROP TRIGGER research_step7_projection_completions_no_update`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE research_step7_projection_completions SET completed_ts=?
WHERE job_id=?`, base.Add(90*time.Millisecond).Format(time.RFC3339Nano),
		completedJobs[0].JobID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE research_step7_projection_completions SET completed_ts=?
WHERE job_id=?`, base.Add(100*time.Millisecond).Format(time.RFC3339Nano),
		completedJobs[1].JobID); err != nil {
		t.Fatal(err)
	}
	manifest, err := st.Step7ProjectionCompletionManifest(ctx, base.Add(110*time.Millisecond))
	if err != nil || manifest.Count != 2 ||
		manifest.MaxCompletedTS != base.Add(100*time.Millisecond).Format(time.RFC3339Nano) {
		t.Fatalf("same-second completion manifest=%+v err=%v", manifest, err)
	}
	if exactEnd, err := st.Step7ProjectionCompletionManifest(ctx, base.Add(100*time.Millisecond)); err != nil ||
		exactEnd.Count != 1 {
		t.Fatalf("exclusive .1Z completion cutoff=%+v want count=1 err=%v", exactEnd, err)
	}
}

func TestStep7ProjectionFenceRejectsChangedCompletionOrPendingState(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	cutoff := time.Now().UTC().Add(time.Hour)
	before, err := st.Step7ProjectionCompletionManifest(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	completed := step7ProjectionTestFlow("fence-completed", cutoff.Add(-2*time.Hour))
	completedJob, err := NewStep7FlowProjectionJob(completed, 1, "terminal fence fixture", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{completed}, []Step7ProjectionJob{completedJob}); err != nil {
		t.Fatal(err)
	}
	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateStep7ProjectionFenceTx(ctx, tx, time.Time{}, cutoff, before); err == nil ||
		!strings.Contains(err.Error(), "completion manifest changed") {
		tx.Rollback()
		t.Fatalf("changed completion manifest passed write fence: %v", err)
	}
	_ = tx.Rollback()
	current, err := st.Step7ProjectionCompletionManifest(ctx, cutoff)
	if err != nil {
		t.Fatal(err)
	}
	pending := step7ProjectionTestFlow("fence-pending", cutoff.Add(-time.Hour))
	pendingJob, err := NewStep7FlowProjectionJob(pending, 1, "",
		[]Step7FrozenObservation{{Observation: step7ProjectionObserver("fence-pending", pending.Observed)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{pending}, []Step7ProjectionJob{pendingJob}); err != nil {
		t.Fatal(err)
	}
	tx, err = st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateStep7ProjectionFenceTx(ctx, tx, time.Time{}, cutoff, current); err == nil ||
		!strings.Contains(err.Error(), "projections pending") {
		tx.Rollback()
		t.Fatalf("new pending projection passed write fence: %v", err)
	}
	_ = tx.Rollback()
}

func TestStep7ProjectionLegacyMigrationAddsExactVersionTrigger(t *testing.T) {
	db, err := sql.Open("sqlite", "file:"+t.TempDir()+"/legacy.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE research_system_observations(
route TEXT,venue TEXT,ticker TEXT,canonical_event_id TEXT,event_version INTEGER,
canonical_payoff_id TEXT,payoff_version INTEGER);
CREATE TABLE research_instrument_specs(
venue TEXT,ticker TEXT,version INTEGER,event_id TEXT,event_version INTEGER,
payoff_id TEXT,payoff_version INTEGER);
CREATE TABLE research_payoff_specs(
event_id TEXT,event_version INTEGER,payoff_id TEXT,version INTEGER);
INSERT INTO research_payoff_specs VALUES('event',1,'payoff',1);
INSERT INTO research_instrument_specs VALUES('kalshi','ticker',1,'event',1,'payoff',1);`); err != nil {
		t.Fatal(err)
	}
	if err := migrateResearchSystemObservationIdentity(db); err != nil {
		t.Fatal(err)
	}
	if err := migrateStep7ProjectionOutbox(db); err != nil {
		t.Fatal(err)
	}
	if err := migrateStep7ProjectionOutbox(db); err != nil {
		t.Fatalf("outbox migration was not idempotent: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO research_system_observations(
route,venue,ticker,canonical_event_id,event_version,canonical_payoff_id,payoff_version,instrument_version,decision_ts)
VALUES('taker','kalshi','ticker','event',1,'payoff',1,1,'2026-07-25T12:00:00Z')`); err != nil {
		t.Fatalf("exact legacy identity rejected: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO research_system_observations(
route,venue,ticker,canonical_event_id,event_version,canonical_payoff_id,payoff_version,instrument_version,decision_ts)
VALUES('taker','kalshi','ticker','event',1,'payoff',1,2,'2026-07-25T12:00:00Z')`); err == nil ||
		!strings.Contains(err.Error(), "exact immutable") {
		t.Fatalf("wrong frozen instrument version passed trigger: %v", err)
	}
}

func TestStep7ProjectionRetryDedupeAndPerJobAtomicity(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()

	flow := step7ProjectionTestFlow("dedupe", now)
	observation := step7ProjectionObserver("step7-dedupe", now)
	job, err := NewStep7FlowProjectionJob(flow, 1, "", []Step7FrozenObservation{{
		Observation: observation,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
		t.Fatal(err)
	}
	// This simulates an output that already committed before an old worker died without recording
	// its completion. The retry must dedupe it and still atomically complete the source job.
	if _, inserted, err := st.InsertResearchSystemObservation(ctx, observation); err != nil || !inserted {
		t.Fatalf("preexisting output inserted=%v err=%v", inserted, err)
	}
	result, err := st.ApplyStep7ProjectionOutbox(ctx, 1)
	if err != nil || result.Completed != 1 || result.OutputsInserted != 0 ||
		result.OutputsDuplicate != 1 || result.Pending != 0 {
		t.Fatalf("dedupe apply=%+v err=%v", result, err)
	}
	var pendingPayloads int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_step7_projection_pending
WHERE job_id=?`, job.JobID).Scan(&pendingPayloads); err != nil || pendingPayloads != 0 {
		t.Fatalf("completed full payload retained=%d err=%v", pendingPayloads, err)
	}
	if again, err := st.ApplyStep7ProjectionOutbox(ctx, 1); err != nil ||
		again.Selected != 0 || again.Completed != 0 {
		t.Fatalf("completed job replayed=%+v err=%v", again, err)
	}

	badFlow := step7ProjectionTestFlow("poison-first", now.Add(time.Millisecond))
	badOutputs := []Step7FrozenObservation{
		{Observation: step7ProjectionObserver("pair-first", now.Add(time.Millisecond))},
		{Observation: ResearchSystemObservation{Observed: now, SystemID: "not-a-system",
			OpportunityID: "pair-second", Kind: "control", Route: "observer",
			CertificateStatus: "not_applicable"}},
	}
	badJob, err := NewStep7FlowProjectionJob(badFlow, 1, "", badOutputs)
	if err != nil {
		t.Fatal(err)
	}
	goodFlow := step7ProjectionTestFlow("fair-second", now.Add(2*time.Millisecond))
	goodObservation := step7ProjectionObserver("fair-second", now.Add(2*time.Millisecond))
	goodJob, err := NewStep7FlowProjectionJob(goodFlow, 1, "",
		[]Step7FrozenObservation{{Observation: goodObservation}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{badFlow, goodFlow}, []Step7ProjectionJob{badJob, goodJob}); err != nil {
		t.Fatal(err)
	}
	result, err = st.ApplyStep7ProjectionOutbox(ctx, 2)
	if err != nil || result.Failed != 1 || result.Completed != 1 || result.Pending != 1 {
		t.Fatalf("bounded fair apply=%+v err=%v", result, err)
	}
	var firstHalf int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_system_observations
WHERE opportunity_id='pair-first'`).Scan(&firstHalf); err != nil || firstHalf != 0 {
		t.Fatalf("half of failed pair committed=%d err=%v", firstHalf, err)
	}
}

func TestStep7ProjectionConflictingObservationDuplicateCannotComplete(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC()
	legacy := step7ProjectionObserver("versioned-duplicate", now)
	legacy.ExperimentVersion = 1
	if _, inserted, err := st.InsertResearchSystemObservation(ctx, legacy); err != nil || !inserted {
		t.Fatalf("legacy fixture inserted=%v err=%v", inserted, err)
	}
	frozen := legacy
	frozen.ExperimentVersion = 2
	frozen.Cohort = "new-frozen-cohort"
	flow := step7ProjectionTestFlow("versioned-duplicate", now)
	job, err := NewStep7FlowProjectionJob(flow, 1, "",
		[]Step7FrozenObservation{{Observation: frozen}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
		t.Fatal(err)
	}
	result, err := st.ApplyStep7ProjectionOutbox(ctx, 10)
	if err != nil || result.Failed != 1 || result.Completed != 0 || result.Pending != 1 ||
		!strings.Contains(result.LastError, "conflicting research system observation duplicate") {
		t.Fatalf("conflicting duplicate completed or vanished: %+v err=%v", result, err)
	}
	var rows int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_system_observations
WHERE opportunity_id='versioned-duplicate'`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("conflicting output count=%d err=%v", rows, err)
	}
}

func TestStep7ProjectionUsesFrozenInstrumentAfterCurrentChanges(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Add(-24 * time.Hour)
	row := r138EvidenceFixture("negative", false)
	row.Observed = now
	row.OpportunityID = "frozen-v1"
	row.EventVersion, row.CanonicalPayoffID, row.PayoffVersion = registerR138EvidenceInstrument(t,
		st, row.CanonicalEventID, row.Venue, row.Ticker)
	var instrumentV1 int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM research_instrument_specs
WHERE venue=? AND ticker=?`, row.Venue, row.Ticker).Scan(&instrumentV1); err != nil || instrumentV1 != 1 {
		t.Fatalf("instrument v1=%d err=%v", instrumentV1, err)
	}
	flow := step7ProjectionTestFlow("frozen-identity", now)
	job, err := NewStep7FlowProjectionJob(flow, 1, "",
		[]Step7FrozenObservation{{Observation: row, InstrumentVersion: instrumentV1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RegisterCanonicalBatch(ctx, nil, nil, []CanonicalInstrumentSpec{{
		Venue: row.Venue, Ticker: row.Ticker, EventID: row.CanonicalEventID,
		PayoffID: row.CanonicalPayoffID, NativeSide: "YES", Orientation: "same",
		Scope: "changed-after-projection-enqueue", RulesArtifact: "test fixture",
		IdentityStatus: "verified",
	}}); err != nil {
		t.Fatal(err)
	}
	var current int
	if err := st.db.QueryRow(`SELECT MAX(version) FROM research_instrument_specs
WHERE venue=? AND ticker=?`, row.Venue, row.Ticker).Scan(&current); err != nil || current != 2 {
		t.Fatalf("current instrument version=%d err=%v", current, err)
	}
	result, err := st.ApplyStep7ProjectionOutbox(ctx, 10)
	if err != nil || result.Completed != 1 || result.OutputsInserted != 1 {
		t.Fatalf("frozen apply=%+v err=%v", result, err)
	}
	var frozenVersion int
	if err := st.db.QueryRow(`SELECT instrument_version FROM research_system_observations
WHERE opportunity_id='frozen-v1'`).Scan(&frozenVersion); err != nil || frozenVersion != 1 {
		t.Fatalf("frozen output silently switched to current v%d err=%v", frozenVersion, err)
	}
	currentRow := row
	currentRow.OpportunityID = "ordinary-current-v2"
	currentRow.InstrumentVersion = 0
	if _, inserted, err := st.InsertResearchSystemObservation(ctx, currentRow); err != nil || !inserted {
		t.Fatalf("ordinary current insert=%v err=%v", inserted, err)
	}
	if err := st.db.QueryRow(`SELECT instrument_version FROM research_system_observations
WHERE opportunity_id='ordinary-current-v2'`).Scan(&current); err != nil || current != 2 {
		t.Fatalf("ordinary insert did not retain current-mode behavior: v%d err=%v", current, err)
	}
	var frozenID int64
	if err := st.db.QueryRow(`SELECT id FROM research_system_observations
WHERE opportunity_id='frozen-v1'`).Scan(&frozenID); err != nil {
		t.Fatal(err)
	}
	gradeAt := time.Now().UTC()
	if _, err := st.db.Exec(`INSERT INTO research_system_payoff_updates(
observation_id,observed_ts,status,payout_lower,payout_upper,realized_net,
source_artifact,source_hash,reason) VALUES(?,?,'settled',1,1,.10,
'frozen identity fixture','frozen-v1-grade','')`, frozenID, step7Time(gradeAt)); err != nil {
		t.Fatal(err)
	}
	terminal, excluded, _, _, _, _, err := st.r139LoadInferenceSnapshot(ctx,
		gradeAt.Add(time.Hour), gradeAt.Add(2*time.Hour), 100)
	if err != nil || len(terminal) != 1 || excluded.Identity != 0 {
		t.Fatalf("frozen historical identity did not reach inference: terminal=%d exclusions=%+v err=%v",
			len(terminal), excluded, err)
	}
}

func TestStep7PendingProjectionProtectsRawUntilCompletionThenPrunesTogether(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	old := time.Now().UTC().AddDate(0, 0, -100)
	flow := step7ProjectionTestFlow("prune-protected", old)
	observation := step7ProjectionObserver("prune-protected", old)
	job, err := NewStep7FlowProjectionJob(flow, 1, "",
		[]Step7FrozenObservation{{Observation: observation}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
		t.Fatal(err)
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -90)
	if n, err := st.PruneStep7Evidence(ctx, cutoff); err != nil || n != 0 {
		t.Fatalf("pending raw was pruned n=%d err=%v", n, err)
	}
	var raw int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_flow_direction_pairs
WHERE pair_id=?`, flow.PairID).Scan(&raw); err != nil || raw != 1 {
		t.Fatalf("protected raw=%d err=%v", raw, err)
	}
	if result, err := st.ApplyStep7ProjectionOutbox(ctx, 10); err != nil ||
		result.Completed != 1 {
		t.Fatalf("apply before prune=%+v err=%v", result, err)
	}
	if n, err := st.PruneStep7Evidence(ctx, cutoff); err != nil || n != 1 {
		t.Fatalf("completed raw prune n=%d err=%v", n, err)
	}
	var jobs, attempts, completions int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_step7_projection_jobs
WHERE job_id=?`, job.JobID).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_step7_projection_attempts
WHERE job_id=?`, job.JobID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_step7_projection_completions
WHERE job_id=?`, job.JobID).Scan(&completions); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 || attempts != 0 || completions != 1 {
		t.Fatalf("durable replay tombstone missing after raw prune: %d/%d/%d", jobs, attempts, completions)
	}
	if _, _, rawN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil ||
		rawN != 0 || jobN != 0 {
		t.Fatalf("replayed source raw/job=%d/%d err=%v", rawN, jobN, err)
	}
	reconnect := flow
	reconnect.Observed = reconnect.Observed.Add(100 * 24 * time.Hour)
	reconnect.BookObserved = reconnect.Observed.Add(-time.Millisecond)
	reconnect.BookGeneration, reconnect.BookSequence, reconnect.TradeSequence = 99, 999, 1001
	reconnect.BookAgeMS, reconnect.YesBid, reconnect.YesAsk = 2, .40, .43
	reconnect.InferredSide, reconnect.ComparisonStatus = "no", "mismatch"
	reconnect.Evidence = map[string]any{"source": "reconnect after raw prune"}
	reconnectJob, err := NewStep7FlowProjectionJob(reconnect, 2, "changed replay terminal", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, rawN, jobN, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{reconnect}, []Step7ProjectionJob{reconnectJob}); err != nil ||
		rawN != 0 || jobN != 0 {
		t.Fatalf("same trade reconnect after prune raw/job=%d/%d err=%v", rawN, jobN, err)
	}
	economicConflict := reconnect
	economicConflict.Count++
	conflictJob, err := NewStep7FlowProjectionJob(economicConflict, 2, "economic conflict", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{economicConflict}, []Step7ProjectionJob{conflictJob}); err == nil ||
		!strings.Contains(err.Error(), "conflicting immutable Step-7 flow trade replay") {
		t.Fatalf("economic conflict after raw prune did not fail closed: %v", err)
	}
	if result, err := st.ApplyStep7ProjectionOutbox(ctx, 10); err != nil ||
		result.Selected != 0 || result.Completed != 0 {
		t.Fatalf("completed source replayed its projection: %+v err=%v", result, err)
	}
	var outputs int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM research_system_observations
WHERE opportunity_id='prune-protected'`).Scan(&outputs); err != nil || outputs != 1 {
		t.Fatalf("replay barrier output count=%d err=%v", outputs, err)
	}
}

func TestStep7PendingProjectionFailsClosedInferenceAndPromotionWindows(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	start := time.Now().UTC().Add(-48 * time.Hour)
	end := start.Add(24 * time.Hour)
	flow := step7ProjectionTestFlow("pending-window", start.Add(time.Hour))
	job, err := NewStep7FlowProjectionJob(flow, 1, "",
		[]Step7FrozenObservation{{Observation: step7ProjectionObserver("pending-window", flow.Observed)}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := st.InsertStep7MicrostructureBatch(ctx, nil, nil,
		[]Step7FlowDirectionPair{flow}, []Step7ProjectionJob{job}); err != nil {
		t.Fatal(err)
	}
	if pending, err := st.Step7ProjectionPendingInWindow(ctx, start, end); err != nil || pending != 1 {
		t.Fatalf("pending window=%d err=%v", pending, err)
	}
	if _, _, err := st.RunR139ResearchInference(ctx, time.Now().UTC()); err == nil ||
		!strings.Contains(err.Error(), "inference window incomplete") {
		t.Fatalf("rolling inference admitted incomplete projection window: %v", err)
	}
	prereg := ResearchInferencePreregistration{SystemID: "replenishment-fingerprint",
		ExperimentVersion: 1, Cohort: "pending", Venue: "kalshi", Route: "taker",
		TrainValidationCutoff: end, UntouchedStart: start, UntouchedEnd: end,
		SealAt: time.Now().UTC()}
	if _, err := st.frozenPreregInputManifest(ctx, prereg); err == nil ||
		!strings.Contains(err.Error(), "training/validation input window incomplete") {
		t.Fatalf("frozen training manifest admitted incomplete projection history: %v", err)
	}
	if _, err := st.loadPreregisteredRows(ctx, prereg); err == nil ||
		!strings.Contains(err.Error(), "sealed inference window incomplete") {
		t.Fatalf("sealed inference admitted incomplete projection window: %v", err)
	}
	if _, _, err := st.InsertResearchPromotionIntent(ctx, ResearchPromotionProof{
		UntouchedStart: start, UntouchedEnd: end,
	}, ResearchPromotionCandidate{}, .4, .01, .01); err == nil ||
		!strings.Contains(err.Error(), "promotion proof window incomplete") {
		t.Fatalf("promotion intent admitted incomplete projection window: %v", err)
	}
}
