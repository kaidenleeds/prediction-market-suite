package storage

// ExecutionShadow is the append-only reconciliation ledger that joins one qualified strategy
// signal to its LIVE, funded-Paper, and execution-counterfactual outcomes. It is deliberately
// separate from every money ledger: rows can explain or measure a decision, but can never grant
// authority, reserve capital, or create an order.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ExecutionShadowFileName is the isolated append-only comparison database. It intentionally
// shares neither a SQLite file nor a connection pool with kalshi.db.
const ExecutionShadowFileName = "execution_shadow.db"

// ExecutionShadowBatchMax bounds one FULL-synchronous transaction and its retry surface.
const ExecutionShadowBatchMax = 128

// ExecutionShadowExactIDLookupMax bounds one reader request assembled from file-backed Paper
// ledgers. Queries are still split into small SQLite-variable batches below; the outer bound keeps
// corrupt or unexpectedly huge JSON from turning a diagnostic read into unbounded database work.
const ExecutionShadowExactIDLookupMax = 100000

const executionShadowExactIDLookupChunk = 400

const executionShadowSchema = `
CREATE TABLE IF NOT EXISTS execution_shadow_attempts (
 attempt_id TEXT PRIMARY KEY,
 signal_decision_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 trigger_unix_ms INTEGER NOT NULL CHECK(trigger_unix_ms>0),
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 title TEXT NOT NULL DEFAULT '',
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 action TEXT NOT NULL CHECK(action IN ('BUY','SELL')),
 system_id TEXT NOT NULL,
 route TEXT NOT NULL CHECK(route IN ('maker','taker')),
 signal_source TEXT NOT NULL,
 input_topology TEXT NOT NULL DEFAULT '',
 signal_contract TEXT NOT NULL DEFAULT '',
 input_observed_ts TEXT NOT NULL DEFAULT '',
 signal_price REAL NOT NULL CHECK(signal_price>=0 AND signal_price<1),
 qualification_basis TEXT NOT NULL,
 created_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_signal
 ON execution_shadow_attempts(signal_decision_id,observed_ts,attempt_id);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_market
 ON execution_shadow_attempts(venue,ticker,side,observed_ts,attempt_id);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_trigger
 ON execution_shadow_attempts(trigger_unix_ms,attempt_id);

CREATE TABLE IF NOT EXISTS execution_shadow_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 event_id TEXT NOT NULL UNIQUE,
 attempt_id TEXT NOT NULL,
 event_ts TEXT NOT NULL,
 elapsed_from_trigger_ms INTEGER NOT NULL CHECK(elapsed_from_trigger_ms>=0),
 stage TEXT NOT NULL,
 outcome TEXT NOT NULL,
 reason TEXT NOT NULL DEFAULT '',

 book_source TEXT NOT NULL DEFAULT '',
 book_generation INTEGER,
 book_subscription_id INTEGER,
 book_sequence INTEGER,
 book_source_ts TEXT NOT NULL DEFAULT '',
 book_received_ts TEXT NOT NULL DEFAULT '',
 book_age_ms REAL,
 side_bid REAL,
 side_ask REAL,
 spread_cents REAL,
 visible_depth REAL,
 tick_size REAL,
 original_limit REAL,
 requested_qty REAL,
 fee_quote REAL,
 fee_quote_source TEXT NOT NULL DEFAULT '',

 live_reservation_id TEXT NOT NULL DEFAULT '',
 venue_attempted INTEGER NOT NULL DEFAULT 0 CHECK(venue_attempted IN (0,1)),
 venue_ack INTEGER NOT NULL DEFAULT 0 CHECK(venue_ack IN (0,1)),
 venue_order_id TEXT NOT NULL DEFAULT '',
 live_state TEXT NOT NULL DEFAULT '',
 live_authoritative INTEGER NOT NULL DEFAULT 0 CHECK(live_authoritative IN (0,1)),
 live_filled_qty REAL,
 live_fill_price REAL,
 live_fee REAL,
 live_fee_source TEXT NOT NULL DEFAULT '',
 live_receipt_source TEXT NOT NULL DEFAULT '',

 paper_attempt_id TEXT NOT NULL DEFAULT '',
 paper_state TEXT NOT NULL DEFAULT '',
 paper_filled_qty REAL,
 paper_fill_price REAL,
 paper_fee REAL,
 paper_fee_source TEXT NOT NULL DEFAULT '',
 paper_book_source TEXT NOT NULL DEFAULT '',

 shadow_state TEXT NOT NULL DEFAULT '',
 shadow_filled_qty REAL,
 shadow_fill_price REAL,
 shadow_fee REAL,
 shadow_fee_source TEXT NOT NULL DEFAULT '',
 shadow_reason TEXT NOT NULL DEFAULT '',

 settlement_known INTEGER NOT NULL DEFAULT 0 CHECK(settlement_known IN (0,1)),
 settlement_value REAL,
 settlement_ts TEXT NOT NULL DEFAULT '',
 settlement_source TEXT NOT NULL DEFAULT '',
 settlement_hash TEXT NOT NULL DEFAULT '',
 live_net REAL,
 paper_net REAL,
 shadow_net REAL,
 evidence_json TEXT NOT NULL DEFAULT '{}',

 FOREIGN KEY(attempt_id) REFERENCES execution_shadow_attempts(attempt_id),
 CHECK(book_age_ms IS NULL OR book_age_ms>=0),
 CHECK(side_bid IS NULL OR (side_bid>0 AND side_bid<1)),
 CHECK(side_ask IS NULL OR (side_ask>0 AND side_ask<1)),
 CHECK(spread_cents IS NULL OR spread_cents>=0),
 CHECK(visible_depth IS NULL OR visible_depth>=0),
 CHECK(tick_size IS NULL OR (tick_size>0 AND tick_size<=1)),
 CHECK(original_limit IS NULL OR (original_limit>0 AND original_limit<1)),
 CHECK(requested_qty IS NULL OR requested_qty>0),
 CHECK(fee_quote IS NULL OR fee_quote>=0),
 CHECK(live_filled_qty IS NULL OR live_filled_qty>=0),
 CHECK(live_fill_price IS NULL OR (live_fill_price>0 AND live_fill_price<1)),
 CHECK(live_fee IS NULL OR live_fee>=0),
 CHECK(paper_filled_qty IS NULL OR paper_filled_qty>=0),
 CHECK(paper_fill_price IS NULL OR (paper_fill_price>0 AND paper_fill_price<1)),
 CHECK(paper_fee IS NULL OR paper_fee>=0),
 CHECK(shadow_filled_qty IS NULL OR shadow_filled_qty>=0),
 CHECK(shadow_fill_price IS NULL OR (shadow_fill_price>0 AND shadow_fill_price<1)),
 CHECK(shadow_fee IS NULL OR shadow_fee>=0),
 CHECK(settlement_value IS NULL OR (settlement_value>=0 AND settlement_value<=1)),
 CHECK((fee_quote IS NULL AND fee_quote_source='') OR
       (fee_quote IS NOT NULL AND fee_quote_source<>'')),
 CHECK((live_fee IS NULL AND live_fee_source='') OR
       (live_fee IS NOT NULL AND live_fee_source<>'')),
 CHECK((paper_fee IS NULL AND paper_fee_source='') OR
       (paper_fee IS NOT NULL AND paper_fee_source<>'')),
 CHECK((shadow_fee IS NULL AND shadow_fee_source='') OR
       (shadow_fee IS NOT NULL AND shadow_fee_source<>'')),
 CHECK((settlement_known=0 AND settlement_value IS NULL AND settlement_ts='' AND
        settlement_source='' AND settlement_hash='' AND live_net IS NULL AND
        paper_net IS NULL AND shadow_net IS NULL) OR
       (settlement_known=1 AND settlement_value IS NOT NULL AND settlement_ts<>'' AND
        settlement_source<>'' AND settlement_hash<>''))
);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_events_attempt
 ON execution_shadow_events(attempt_id,id);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_events_stage
 ON execution_shadow_events(stage,event_ts,id);
-- Recovery begins from two rare stage/outcome pairs. Keeping attempt_id next makes each seed a
-- small covering scan instead of repeatedly walking every attempt or the broad stage/time index.
CREATE INDEX IF NOT EXISTS idx_execution_shadow_events_recovery_seed
 ON execution_shadow_events(stage,outcome,attempt_id,id);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_events_attempt_stage_outcome
 ON execution_shadow_events(attempt_id,stage,outcome,id);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_events_live_terminal
 ON execution_shadow_events(attempt_id,id) WHERE live_state<>'';
CREATE INDEX IF NOT EXISTS idx_execution_shadow_events_live_fill
 ON execution_shadow_events(attempt_id,id) WHERE live_filled_qty>0;
CREATE INDEX IF NOT EXISTS idx_execution_shadow_events_settlement
 ON execution_shadow_events(attempt_id,id) WHERE settlement_known=1;

CREATE TRIGGER IF NOT EXISTS execution_shadow_attempts_no_update
 BEFORE UPDATE ON execution_shadow_attempts
 BEGIN SELECT RAISE(ABORT,'immutable execution-shadow attempt'); END;
CREATE TRIGGER IF NOT EXISTS execution_shadow_attempts_no_delete
 BEFORE DELETE ON execution_shadow_attempts
 BEGIN SELECT RAISE(ABORT,'execution-shadow attempts are append-preserved'); END;
CREATE TRIGGER IF NOT EXISTS execution_shadow_events_no_update
 BEFORE UPDATE ON execution_shadow_events
 BEGIN SELECT RAISE(ABORT,'immutable execution-shadow event'); END;
CREATE TRIGGER IF NOT EXISTS execution_shadow_events_no_delete
 BEFORE DELETE ON execution_shadow_events
 BEGIN SELECT RAISE(ABORT,'execution-shadow events are append-preserved'); END;

CREATE TABLE IF NOT EXISTS execution_shadow_meta (
 key TEXT PRIMARY KEY,
 value TEXT NOT NULL,
 updated_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_shadow_writer_sessions (
 session_id TEXT PRIMARY KEY,
 started_ts TEXT NOT NULL,
 ended_ts TEXT NOT NULL DEFAULT '',
 clean_shutdown INTEGER NOT NULL DEFAULT 0 CHECK(clean_shutdown IN (0,1)),
 committed_records INTEGER NOT NULL DEFAULT 0 CHECK(committed_records>=0),
 dropped_records INTEGER NOT NULL DEFAULT 0 CHECK(dropped_records>=0)
);
CREATE INDEX IF NOT EXISTS idx_execution_shadow_writer_sessions_started
 ON execution_shadow_writer_sessions(started_ts DESC,session_id);

-- attempt_id is intentionally semantic text, so it cannot serve as a stable prefix watermark.
-- This companion gives every committed attempt an immutable INTEGER PRIMARY KEY that survives
-- VACUUM. Existing attempts are backfilled once at migration; the trigger orders every later
-- insert inside the same transaction as its parent attempt.
CREATE TABLE IF NOT EXISTS execution_shadow_attempt_order (
 sequence INTEGER PRIMARY KEY AUTOINCREMENT,
 attempt_id TEXT NOT NULL UNIQUE,
 FOREIGN KEY(attempt_id) REFERENCES execution_shadow_attempts(attempt_id) ON DELETE CASCADE
);
INSERT OR IGNORE INTO execution_shadow_attempt_order(attempt_id)
 SELECT attempt_id FROM execution_shadow_attempts ORDER BY created_ts,attempt_id;
CREATE TRIGGER IF NOT EXISTS execution_shadow_attempt_order_after_insert
 AFTER INSERT ON execution_shadow_attempts
 BEGIN
  INSERT OR IGNORE INTO execution_shadow_attempt_order(attempt_id) VALUES(NEW.attempt_id);
 END;
`

func migrateExecutionShadowSchema(db *sql.DB) error {
	if db == nil {
		return errors.New("nil database")
	}
	_, err := db.Exec(executionShadowSchema)
	return err
}

func openExecutionShadowDB(dataDir string) (*sql.DB, string, error) {
	path, err := filepath.Abs(filepath.Join(dataDir, ExecutionShadowFileName))
	if err != nil {
		return nil, "", fmt.Errorf("resolve execution-shadow path: %w", err)
	}
	// FULL is affordable because the isolated writer batches records. It makes every committed
	// comparison batch durable across a process or power failure without taking the cash DB lock.
	dsn := "file:" + filepath.ToSlash(path) +
		"?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)" +
		"&_pragma=busy_timeout(5000)&_pragma=foreign_keys(ON)" +
		"&_pragma=mmap_size(67108864)&_pragma=cache_size(-8192)" +
		"&_pragma=temp_store(FILE)&_pragma=journal_size_limit(67108864)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, "", err
	}
	db.SetMaxOpenConns(4) // one writer plus bounded endpoint/settlement readers
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(time.Hour)
	if err = db.Ping(); err == nil {
		err = migrateExecutionShadowSchema(db)
	}
	if err == nil {
		err = migrateKalshiTradeReconciliationSchema(db)
	}
	if err != nil {
		_ = db.Close()
		return nil, "", err
	}
	return db, path, nil
}

func (s *Store) executionShadowHandle() (*sql.DB, error) {
	if s == nil || s.executionShadowDB == nil {
		return nil, errors.New("isolated execution-shadow database unavailable")
	}
	return s.executionShadowDB, nil
}

// HasRecentExecutionShadowAttempt is the restart-safe episode guard for preregistered shadow
// experiments. It reads only the isolated comparison database and therefore cannot contend with
// the cash ledger. Callers still own an in-memory fast-path claim to avoid one read per feed tick.
func (s *Store) HasRecentExecutionShadowAttempt(ctx context.Context, venue, ticker, side,
	systemID, signalSource, qualification string, since time.Time) (bool, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return false, err
	}
	var found int
	err = db.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM execution_shadow_attempts INDEXED BY idx_execution_shadow_market
 WHERE venue=? AND ticker=? AND side=? AND observed_ts>=? AND system_id=?
   AND signal_source=? AND qualification_basis=? LIMIT 1
)`, strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(ticker),
		strings.ToUpper(strings.TrimSpace(side)), since.UTC().Format(time.RFC3339Nano),
		strings.ToLower(strings.TrimSpace(systemID)), strings.TrimSpace(signalSource),
		strings.TrimSpace(qualification)).Scan(&found)
	return found != 0, err
}

const executionShadowLegacyImportKey = "legacy-kalshi-db-import-counts-v1"

// migrateLegacyExecutionShadow copies the old immutable comparison tables out of kalshi.db before
// any runtime producer starts. The source is never deleted or rewritten. Repeated opens compare a
// count marker and replay idempotently when needed; same-id/different-evidence fails boot loudly.
func (s *Store) migrateLegacyExecutionShadow(ctx context.Context) error {
	if s == nil || s.db == nil || s.executionShadowDB == nil {
		return errors.New("execution-shadow migration missing database handle")
	}
	var sourceTables int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master
WHERE type='table' AND name IN ('execution_shadow_attempts','execution_shadow_events')`).
		Scan(&sourceTables); err != nil {
		return err
	}
	if sourceTables != 2 {
		return errors.New("legacy execution-shadow source schema incomplete")
	}
	var attemptCount, eventCount int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_shadow_attempts`).
		Scan(&attemptCount); err != nil {
		return err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_shadow_events`).
		Scan(&eventCount); err != nil {
		return err
	}
	wantMarker := fmt.Sprintf("%d:%d", attemptCount, eventCount)
	var priorMarker string
	err := s.executionShadowDB.QueryRowContext(ctx,
		`SELECT value FROM execution_shadow_meta WHERE key=?`,
		executionShadowLegacyImportKey).Scan(&priorMarker)
	if err == nil && priorMarker == wantMarker {
		return nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	tx, err := s.executionShadowDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	attemptRows, err := s.db.QueryContext(ctx, `SELECT `+executionShadowAttemptColumns+
		` FROM execution_shadow_attempts ORDER BY observed_ts,attempt_id`)
	if err != nil {
		return err
	}
	for attemptRows.Next() {
		attempt, scanErr := scanExecutionShadowAttempt(attemptRows)
		if scanErr != nil {
			_ = attemptRows.Close()
			return scanErr
		}
		attempt, scanErr = normalizeExecutionShadowAttempt(attempt)
		if scanErr != nil {
			_ = attemptRows.Close()
			return scanErr
		}
		if _, scanErr = insertExecutionShadowAttemptTx(ctx, tx, attempt); scanErr != nil {
			_ = attemptRows.Close()
			return fmt.Errorf("copy legacy execution-shadow attempt: %w", scanErr)
		}
	}
	if err = attemptRows.Close(); err != nil {
		return err
	}
	if err = attemptRows.Err(); err != nil {
		return err
	}
	eventRows, err := s.db.QueryContext(ctx, `SELECT `+executionShadowEventColumns+
		` FROM execution_shadow_events ORDER BY id`)
	if err != nil {
		return err
	}
	for eventRows.Next() {
		event, scanErr := scanExecutionShadowEvent(eventRows)
		if scanErr != nil {
			_ = eventRows.Close()
			return scanErr
		}
		event, scanErr = normalizeExecutionShadowEvent(event)
		if scanErr != nil {
			_ = eventRows.Close()
			return scanErr
		}
		evidence, marshalErr := marshalExecutionShadowEvidence(event.Evidence)
		if marshalErr != nil {
			_ = eventRows.Close()
			return marshalErr
		}
		if _, scanErr = appendExecutionShadowEventTx(ctx, tx, event, evidence); scanErr != nil {
			_ = eventRows.Close()
			return fmt.Errorf("copy legacy execution-shadow event: %w", scanErr)
		}
	}
	if err = eventRows.Close(); err != nil {
		return err
	}
	if err = eventRows.Err(); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO execution_shadow_meta(key,value,updated_ts)
VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`,
		executionShadowLegacyImportKey, wantMarker, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit()
}

// ExecutionShadowAttempt is the immutable signal/decision identity shared by LIVE, funded Paper,
// and the counterfactual. SignalDecisionID remains stable across those consumers; AttemptID is
// unique to this emitted decision and never falls back to ticker-only matching.
type ExecutionShadowAttempt struct {
	AttemptID, SignalDecisionID   string
	ObservedAt                    time.Time
	TriggerUnixMS                 int64 `json:"trigger_unix_ms"`
	Venue, Ticker, Title          string
	Side, Action, SystemID, Route string
	SignalSource, InputTopology   string
	SignalContract                string
	InputObservedAt               time.Time
	SignalPrice                   float64
	QualificationBasis            string
	CreatedAt                     time.Time
}

// ExecutionShadowEvent is one append-only stage receipt. Optional numeric facts use pointers so
// unavailable evidence cannot silently become a zero price, fee, size, sequence, or P&L.
type ExecutionShadowEvent struct {
	ID                                    int64
	EventID, AttemptID                    string
	At                                    time.Time
	ElapsedFromTriggerMS                  int64 `json:"elapsed_from_trigger_ms"`
	Stage, Outcome, Reason                string
	BookSource                            string
	BookGeneration                        *int64
	BookSubscriptionID                    *int64
	BookSequence                          *int64
	BookSourceAt, BookReceivedAt          time.Time
	BookAgeMS                             *float64
	SideBid, SideAsk, SpreadCents         *float64
	VisibleDepth, TickSize                *float64
	OriginalLimit, RequestedQty           *float64
	FeeQuote                              *float64
	FeeQuoteSource                        string
	LiveReservationID                     string
	VenueAttempted, VenueAck              bool
	VenueOrderID, LiveState               string
	LiveAuthoritative                     bool
	LiveFilledQty, LiveFillPrice, LiveFee *float64
	LiveFeeSource, LiveReceiptSource      string
	PaperAttemptID, PaperState            string
	PaperFilledQty, PaperFillPrice        *float64
	PaperFee                              *float64
	PaperFeeSource, PaperBookSource       string
	ShadowState                           string
	ShadowFilledQty, ShadowFillPrice      *float64
	ShadowFee                             *float64
	ShadowFeeSource, ShadowReason         string
	SettlementKnown                       bool
	SettlementValue                       *float64
	SettledAt                             time.Time
	SettlementSource, SettlementHash      string
	LiveNet, PaperNet, ShadowNet          *float64
	Evidence                              map[string]any
}

type ExecutionShadowAttemptView struct {
	Sequence int64                  `json:"sequence,omitempty"`
	Attempt  ExecutionShadowAttempt `json:"attempt"`
	Events   []ExecutionShadowEvent `json:"events"`
}

// ExecutionShadowWrite is one immutable record offered to the isolated writer. A batch may carry
// attempts and events in any producer order; persistence inserts all parents first, then events in
// their original order. OrphanIndexes identifies only events whose parent has not arrived yet.
type ExecutionShadowWrite struct {
	Attempt     *ExecutionShadowAttempt
	Event       *ExecutionShadowEvent
	KalshiTrade *KalshiTradeReconciliationEvent
}

// ExecutionShadowStoreStatus is filesystem/session truth for the diagnostic API.
type ExecutionShadowStoreStatus struct {
	DatabaseFile        string `json:"database_file"`
	DatabaseBytes       int64  `json:"database_bytes"`
	WALBytes            int64  `json:"wal_bytes"`
	CurrentSessionID    string `json:"current_session_id,omitempty"`
	LatestSessionID     string `json:"latest_session_id,omitempty"`
	LatestStartedAt     string `json:"latest_started_at,omitempty"`
	LatestEndedAt       string `json:"latest_ended_at,omitempty"`
	LatestClean         bool   `json:"latest_clean_shutdown"`
	LatestCommitted     int64  `json:"latest_committed_records"`
	LatestDropped       int64  `json:"latest_dropped_records"`
	PriorUncleanRuns    int64  `json:"prior_unclean_sessions"`
	CommittedDurability string `json:"committed_durability"`
}

type preparedExecutionShadowWrite struct {
	attempt     *ExecutionShadowAttempt
	event       *ExecutionShadowEvent
	kalshiTrade *KalshiTradeReconciliationEvent
	evidence    string
	index       int
}

func (s *Store) executionShadowSessionID() string {
	s.executionShadowMu.Lock()
	defer s.executionShadowMu.Unlock()
	if s.executionShadowRun == "" {
		s.executionShadowRun = fmt.Sprintf("shadow-%d-%d", time.Now().UTC().UnixNano(), os.Getpid())
	}
	return s.executionShadowRun
}

func ensureExecutionShadowSessionTx(ctx context.Context, tx *sql.Tx, sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return errors.New("empty execution-shadow writer session")
	}
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO execution_shadow_writer_sessions(
session_id,started_ts,clean_shutdown,committed_records,dropped_records) VALUES(?,?,0,0,0)`,
		sessionID, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// PersistExecutionShadowBatch writes one bounded worker batch in a single FULL-synchronous
// transaction on execution_shadow.db. It never reads from or writes to kalshi.db.
func (s *Store) PersistExecutionShadowBatch(ctx context.Context,
	writes []ExecutionShadowWrite) (orphanIndexes []int, err error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if len(writes) == 0 {
		return nil, nil
	}
	if len(writes) > ExecutionShadowBatchMax {
		return nil, fmt.Errorf("execution-shadow batch has %d records; max is %d",
			len(writes), ExecutionShadowBatchMax)
	}
	prepared := make([]preparedExecutionShadowWrite, 0, len(writes))
	batchTriggers := make(map[string]int64)
	// Normalize every immutable parent first. An event can reach the nonblocking writer before its
	// producer has published the trigger in process memory, in which case -1 is an intentional
	// "derive from the exact parent" sentinel. Looking across the entire batch preserves
	// parent-first crash atomicity even when the event happened to enter the batch first.
	for i, write := range writes {
		payloads := 0
		if write.Attempt != nil {
			payloads++
		}
		if write.Event != nil {
			payloads++
		}
		if write.KalshiTrade != nil {
			payloads++
		}
		if payloads != 1 {
			return nil, errors.New("execution-shadow batch record must contain exactly one payload")
		}
		if write.Attempt == nil {
			continue
		}
		normalized, normalizeErr := normalizeExecutionShadowAttempt(*write.Attempt)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		batchTriggers[normalized.AttemptID] = normalized.TriggerUnixMS
		prepared = append(prepared, preparedExecutionShadowWrite{attempt: &normalized, index: i})
	}
	for i, write := range writes {
		if write.Event == nil {
			continue
		}
		event := *write.Event
		if event.ElapsedFromTriggerMS == -1 {
			attemptID := strings.TrimSpace(event.AttemptID)
			triggerUnixMS, found := batchTriggers[attemptID]
			if !found && attemptID != "" {
				queryErr := db.QueryRowContext(ctx,
					`SELECT trigger_unix_ms FROM execution_shadow_attempts WHERE attempt_id=?`,
					attemptID).Scan(&triggerUnixMS)
				switch {
				case queryErr == nil:
					found = true
				case errors.Is(queryErr, sql.ErrNoRows):
					// Preserve the ordinary orphan contract: commit every ready row and return only
					// this exact index for bounded requeue after its parent becomes durable.
					orphanIndexes = append(orphanIndexes, i)
					continue
				default:
					return nil, queryErr
				}
			}
			if found {
				if event.At.IsZero() {
					event.At = time.Now().UTC()
				}
				event.ElapsedFromTriggerMS = event.At.UTC().UnixMilli() - triggerUnixMS
			}
		}
		normalized, normalizeErr := normalizeExecutionShadowEvent(event)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		evidence, marshalErr := marshalExecutionShadowEvidence(normalized.Evidence)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal execution-shadow evidence: %w", marshalErr)
		}
		prepared = append(prepared, preparedExecutionShadowWrite{
			event: &normalized, evidence: evidence, index: i})
	}
	for i, write := range writes {
		if write.KalshiTrade == nil {
			continue
		}
		normalized, evidence, normalizeErr :=
			normalizeKalshiTradeReconciliationEvent(*write.KalshiTrade)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
		prepared = append(prepared, preparedExecutionShadowWrite{
			kalshiTrade: &normalized, evidence: evidence, index: i})
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	sessionID := s.executionShadowSessionID()
	if err = ensureExecutionShadowSessionTx(ctx, tx, sessionID); err != nil {
		return nil, err
	}
	inserted := int64(0)
	// Parent-first across the entire transaction makes the detector attempt + its initial event
	// crash-atomic even when a racing LIVE receipt entered the queue first.
	for _, write := range prepared {
		if write.attempt == nil {
			continue
		}
		didInsert, insertErr := insertExecutionShadowAttemptTx(ctx, tx, *write.attempt)
		if insertErr != nil {
			return nil, insertErr
		}
		if didInsert {
			inserted++
		}
	}
	for _, write := range prepared {
		if write.event == nil {
			continue
		}
		didInsert, insertErr := appendExecutionShadowEventTx(ctx, tx, *write.event, write.evidence)
		if errors.Is(insertErr, sql.ErrNoRows) {
			orphanIndexes = append(orphanIndexes, write.index)
			continue
		}
		if insertErr != nil {
			return nil, insertErr
		}
		if didInsert {
			inserted++
		}
	}
	for _, write := range prepared {
		if write.kalshiTrade == nil {
			continue
		}
		didInsert, insertErr := appendKalshiTradeReconciliationEventTx(
			ctx, tx, *write.kalshiTrade, write.evidence)
		if insertErr != nil {
			return nil, insertErr
		}
		if didInsert {
			inserted++
		}
	}
	if inserted > 0 {
		if _, err = tx.ExecContext(ctx, `UPDATE execution_shadow_writer_sessions
SET committed_records=committed_records+? WHERE session_id=?`, inserted, sessionID); err != nil {
			return nil, err
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return orphanIndexes, nil
}

// FinishExecutionShadowSession marks a writer run clean only after its accepted queue drained.
// A hard crash or a shutdown deadline leaves the row at clean_shutdown=0, making a possible RAM
// tail explicit rather than silently presenting the comparison as complete.
func (s *Store) FinishExecutionShadowSession(ctx context.Context, dropped uint64) error {
	db, err := s.executionShadowHandle()
	if err != nil {
		return err
	}
	s.executionShadowMu.Lock()
	sessionID := s.executionShadowRun
	s.executionShadowMu.Unlock()
	if sessionID == "" {
		return nil
	}
	_, err = db.ExecContext(ctx, `UPDATE execution_shadow_writer_sessions
SET ended_ts=?,clean_shutdown=1,dropped_records=?
WHERE session_id=? AND clean_shutdown=0`,
		time.Now().UTC().Format(time.RFC3339Nano), int64(dropped), sessionID)
	return err
}

func executionShadowFileSize(path string) int64 {
	if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
		return info.Size()
	}
	return 0
}

func (s *Store) ExecutionShadowStatus(ctx context.Context) ExecutionShadowStoreStatus {
	status := ExecutionShadowStoreStatus{
		DatabaseFile:        filepath.Base(s.executionShadowPath),
		DatabaseBytes:       executionShadowFileSize(s.executionShadowPath),
		WALBytes:            executionShadowFileSize(s.executionShadowPath + "-wal"),
		CommittedDurability: "isolated SQLite WAL; synchronous=FULL; accepted RAM tail is clean only after shutdown drain",
	}
	s.executionShadowMu.Lock()
	status.CurrentSessionID = s.executionShadowRun
	s.executionShadowMu.Unlock()
	db, err := s.executionShadowHandle()
	if err != nil {
		return status
	}
	var clean int
	_ = db.QueryRowContext(ctx, `SELECT session_id,started_ts,ended_ts,clean_shutdown,
committed_records,dropped_records FROM execution_shadow_writer_sessions
ORDER BY started_ts DESC,session_id DESC LIMIT 1`).Scan(
		&status.LatestSessionID, &status.LatestStartedAt, &status.LatestEndedAt, &clean,
		&status.LatestCommitted, &status.LatestDropped)
	status.LatestClean = clean != 0
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM execution_shadow_writer_sessions
WHERE clean_shutdown=0 AND session_id<>?`, status.CurrentSessionID).Scan(&status.PriorUncleanRuns)
	return status
}

// BackupExecutionShadow writes one WAL-aware, transactionally consistent shadow snapshot.
func (s *Store) BackupExecutionShadow(ctx context.Context, destPath string) error {
	db, err := s.executionShadowHandle()
	if err != nil {
		return err
	}
	_ = os.Remove(destPath)
	stmt := "VACUUM INTO '" + strings.ReplaceAll(destPath, "'", "''") + "'"
	_, err = db.ExecContext(ctx, stmt)
	return err
}

// ExecutionShadowDBForTest exposes only the isolated handle to package-external regressions.
func (s *Store) ExecutionShadowDBForTest() *sql.DB { return s.executionShadowDB }

func executionShadowFinitePtr(v *float64) bool {
	return v == nil || (!math.IsNaN(*v) && !math.IsInf(*v, 0))
}

func normalizeExecutionShadowAttempt(in ExecutionShadowAttempt) (ExecutionShadowAttempt, error) {
	in.AttemptID = strings.TrimSpace(in.AttemptID)
	in.SignalDecisionID = strings.TrimSpace(in.SignalDecisionID)
	in.Venue = strings.ToLower(strings.TrimSpace(in.Venue))
	in.Ticker = strings.TrimSpace(in.Ticker)
	in.Title = strings.TrimSpace(in.Title)
	in.Side = strings.ToUpper(strings.TrimSpace(in.Side))
	in.Action = strings.ToUpper(strings.TrimSpace(in.Action))
	in.SystemID = strings.ToLower(strings.TrimSpace(in.SystemID))
	in.Route = strings.ToLower(strings.TrimSpace(in.Route))
	in.SignalSource = strings.TrimSpace(in.SignalSource)
	in.InputTopology = strings.TrimSpace(in.InputTopology)
	in.SignalContract = strings.TrimSpace(in.SignalContract)
	in.QualificationBasis = strings.TrimSpace(in.QualificationBasis)
	in.ObservedAt = in.ObservedAt.UTC()
	in.InputObservedAt = in.InputObservedAt.UTC()
	if in.CreatedAt.IsZero() {
		in.CreatedAt = time.Now().UTC()
	} else {
		in.CreatedAt = in.CreatedAt.UTC()
	}
	if in.AttemptID == "" || in.SignalDecisionID == "" || in.ObservedAt.IsZero() ||
		in.TriggerUnixMS <= 0 || in.TriggerUnixMS != in.ObservedAt.UnixMilli() ||
		(in.Venue != "kalshi" && in.Venue != "polyus") || in.Ticker == "" ||
		(in.Side != "YES" && in.Side != "NO") || (in.Action != "BUY" && in.Action != "SELL") ||
		in.SystemID == "" || (in.Route != "maker" && in.Route != "taker") ||
		in.SignalSource == "" || in.QualificationBasis == "" ||
		math.IsNaN(in.SignalPrice) || math.IsInf(in.SignalPrice, 0) ||
		in.SignalPrice < 0 || in.SignalPrice >= 1 {
		return in, errors.New("invalid execution-shadow attempt")
	}
	return in, nil
}

func sameExecutionShadowAttempt(a, b ExecutionShadowAttempt) bool {
	return a.AttemptID == b.AttemptID && a.SignalDecisionID == b.SignalDecisionID &&
		a.ObservedAt.Equal(b.ObservedAt) && a.TriggerUnixMS == b.TriggerUnixMS &&
		a.Venue == b.Venue && a.Ticker == b.Ticker &&
		a.Title == b.Title && a.Side == b.Side && a.Action == b.Action &&
		a.SystemID == b.SystemID && a.Route == b.Route && a.SignalSource == b.SignalSource &&
		a.InputTopology == b.InputTopology && a.SignalContract == b.SignalContract &&
		a.InputObservedAt.Equal(b.InputObservedAt) &&
		math.Abs(a.SignalPrice-b.SignalPrice) <= 1e-12 &&
		a.QualificationBasis == b.QualificationBasis
}

func parseExecutionShadowTime(raw, field string, required bool) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if required {
			return time.Time{}, fmt.Errorf("execution-shadow %s is empty", field)
		}
		return time.Time{}, nil
	}
	out, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse execution-shadow %s: %w", field, err)
	}
	return out, nil
}

func scanExecutionShadowAttempt(scanner interface{ Scan(...any) error }) (ExecutionShadowAttempt, error) {
	var out ExecutionShadowAttempt
	var observed, inputObserved, created string
	err := scanner.Scan(&out.AttemptID, &out.SignalDecisionID, &observed, &out.TriggerUnixMS, &out.Venue,
		&out.Ticker, &out.Title, &out.Side, &out.Action, &out.SystemID, &out.Route,
		&out.SignalSource, &out.InputTopology, &out.SignalContract, &inputObserved,
		&out.SignalPrice, &out.QualificationBasis, &created)
	if err != nil {
		return out, err
	}
	if out.ObservedAt, err = parseExecutionShadowTime(observed, "observed_ts", true); err != nil {
		return out, err
	}
	if out.InputObservedAt, err = parseExecutionShadowTime(inputObserved, "input_observed_ts", false); err != nil {
		return out, err
	}
	out.CreatedAt, err = parseExecutionShadowTime(created, "created_ts", true)
	return out, err
}

const executionShadowAttemptColumns = `attempt_id,signal_decision_id,observed_ts,trigger_unix_ms,venue,ticker,
title,side,action,system_id,route,signal_source,input_topology,signal_contract,input_observed_ts,
signal_price,qualification_basis,created_ts`

const executionShadowAttemptColumnsAliasA = `a.attempt_id,a.signal_decision_id,a.observed_ts,
a.trigger_unix_ms,a.venue,a.ticker,a.title,a.side,a.action,a.system_id,a.route,a.signal_source,
a.input_topology,a.signal_contract,a.input_observed_ts,a.signal_price,a.qualification_basis,
a.created_ts`

func insertExecutionShadowAttemptTx(ctx context.Context, tx *sql.Tx,
	in ExecutionShadowAttempt) (bool, error) {
	prior, scanErr := scanExecutionShadowAttempt(tx.QueryRowContext(ctx,
		`SELECT `+executionShadowAttemptColumns+` FROM execution_shadow_attempts WHERE attempt_id=?`,
		in.AttemptID))
	if scanErr == nil {
		if !sameExecutionShadowAttempt(prior, in) {
			return false, errors.New("execution-shadow attempt id reused with different identity")
		}
		return false, nil
	}
	if !errors.Is(scanErr, sql.ErrNoRows) {
		return false, scanErr
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO execution_shadow_attempts(
attempt_id,signal_decision_id,observed_ts,venue,ticker,title,side,action,system_id,route,
signal_source,input_topology,signal_contract,input_observed_ts,signal_price,qualification_basis,
created_ts,trigger_unix_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		in.AttemptID, in.SignalDecisionID, in.ObservedAt.Format(time.RFC3339Nano), in.Venue,
		in.Ticker, in.Title, in.Side, in.Action, in.SystemID, in.Route, in.SignalSource,
		in.InputTopology, in.SignalContract, optionalExecutionShadowTime(in.InputObservedAt),
		in.SignalPrice, in.QualificationBasis, in.CreatedAt.Format(time.RFC3339Nano),
		in.TriggerUnixMS)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) InsertExecutionShadowAttempt(ctx context.Context,
	in ExecutionShadowAttempt) (bool, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return false, err
	}
	in, err = normalizeExecutionShadowAttempt(in)
	if err != nil {
		return false, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	inserted, err := insertExecutionShadowAttemptTx(ctx, tx, in)
	if err != nil {
		return false, err
	}
	return inserted, tx.Commit()
}

// ExecutionShadowAttemptByID resolves restart lineage without ticker/time matching. The immutable
// parent supplies the original trigger clock used to normalize recovered reservation events.
func (s *Store) ExecutionShadowAttemptByID(ctx context.Context,
	attemptID string) (ExecutionShadowAttempt, bool, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return ExecutionShadowAttempt{}, false, err
	}
	attempt, err := scanExecutionShadowAttempt(db.QueryRowContext(ctx,
		`SELECT `+executionShadowAttemptColumns+` FROM execution_shadow_attempts WHERE attempt_id=?`,
		strings.TrimSpace(attemptID)))
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionShadowAttempt{}, false, nil
	}
	if err != nil {
		return ExecutionShadowAttempt{}, false, err
	}
	return attempt, true, nil
}

func optionalExecutionShadowTime(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.UTC().Format(time.RFC3339Nano)
}

func normalizeExecutionShadowEvent(in ExecutionShadowEvent) (ExecutionShadowEvent, error) {
	in.EventID = strings.TrimSpace(in.EventID)
	in.AttemptID = strings.TrimSpace(in.AttemptID)
	in.Stage = strings.TrimSpace(in.Stage)
	in.Outcome = strings.TrimSpace(in.Outcome)
	in.Reason = strings.TrimSpace(in.Reason)
	in.BookSource = strings.TrimSpace(in.BookSource)
	in.FeeQuoteSource = strings.TrimSpace(in.FeeQuoteSource)
	in.LiveReservationID = strings.TrimSpace(in.LiveReservationID)
	in.VenueOrderID = strings.TrimSpace(in.VenueOrderID)
	in.LiveState = strings.TrimSpace(in.LiveState)
	in.LiveFeeSource = strings.TrimSpace(in.LiveFeeSource)
	in.LiveReceiptSource = strings.TrimSpace(in.LiveReceiptSource)
	in.PaperAttemptID = strings.TrimSpace(in.PaperAttemptID)
	in.PaperState = strings.TrimSpace(in.PaperState)
	in.PaperFeeSource = strings.TrimSpace(in.PaperFeeSource)
	in.PaperBookSource = strings.TrimSpace(in.PaperBookSource)
	in.ShadowState = strings.TrimSpace(in.ShadowState)
	in.ShadowFeeSource = strings.TrimSpace(in.ShadowFeeSource)
	in.ShadowReason = strings.TrimSpace(in.ShadowReason)
	in.SettlementSource = strings.TrimSpace(in.SettlementSource)
	in.SettlementHash = strings.TrimSpace(in.SettlementHash)
	if in.At.IsZero() {
		in.At = time.Now().UTC()
	} else {
		in.At = in.At.UTC()
	}
	in.BookSourceAt = in.BookSourceAt.UTC()
	in.BookReceivedAt = in.BookReceivedAt.UTC()
	in.SettledAt = in.SettledAt.UTC()
	if in.Evidence == nil {
		in.Evidence = map[string]any{}
	}
	if in.EventID == "" || in.AttemptID == "" || in.Stage == "" || in.Outcome == "" {
		return in, errors.New("invalid execution-shadow event identity")
	}
	if in.ElapsedFromTriggerMS < 0 {
		return in, errors.New("negative execution-shadow trigger elapsed time")
	}
	for _, value := range []*float64{in.BookAgeMS, in.SideBid, in.SideAsk, in.SpreadCents,
		in.VisibleDepth, in.TickSize, in.OriginalLimit, in.RequestedQty, in.FeeQuote,
		in.LiveFilledQty, in.LiveFillPrice, in.LiveFee, in.PaperFilledQty, in.PaperFillPrice,
		in.PaperFee, in.ShadowFilledQty, in.ShadowFillPrice, in.ShadowFee,
		in.SettlementValue, in.LiveNet, in.PaperNet, in.ShadowNet} {
		if !executionShadowFinitePtr(value) {
			return in, errors.New("non-finite execution-shadow event value")
		}
	}
	if in.BookAgeMS != nil && *in.BookAgeMS < 0 ||
		in.SpreadCents != nil && *in.SpreadCents < 0 ||
		in.VisibleDepth != nil && *in.VisibleDepth < 0 ||
		in.RequestedQty != nil && *in.RequestedQty <= 0 ||
		in.FeeQuote != nil && *in.FeeQuote < 0 ||
		in.LiveFilledQty != nil && *in.LiveFilledQty < 0 ||
		in.PaperFilledQty != nil && *in.PaperFilledQty < 0 ||
		in.ShadowFilledQty != nil && *in.ShadowFilledQty < 0 {
		return in, errors.New("out-of-range execution-shadow event value")
	}
	if (in.FeeQuote == nil) != (in.FeeQuoteSource == "") ||
		(in.LiveFee == nil) != (in.LiveFeeSource == "") ||
		(in.PaperFee == nil) != (in.PaperFeeSource == "") ||
		(in.ShadowFee == nil) != (in.ShadowFeeSource == "") {
		return in, errors.New("execution-shadow fee value/source mismatch")
	}
	if in.SettlementKnown {
		if in.SettlementValue == nil || *in.SettlementValue < 0 || *in.SettlementValue > 1 ||
			in.SettledAt.IsZero() || in.SettlementSource == "" || in.SettlementHash == "" {
			return in, errors.New("incomplete execution-shadow settlement")
		}
	} else if in.SettlementValue != nil || !in.SettledAt.IsZero() ||
		in.SettlementSource != "" || in.SettlementHash != "" ||
		in.LiveNet != nil || in.PaperNet != nil || in.ShadowNet != nil {
		return in, errors.New("unsettled execution-shadow event carries settlement economics")
	}
	return in, nil
}

func boolIntShadow(v bool) int {
	if v {
		return 1
	}
	return 0
}

func executionShadowEventRetryEquivalent(prior, next ExecutionShadowEvent) bool {
	prior.ID, next.ID = 0, 0
	// A settlement EventID names its immutable outcome/hash/lane economics, not the wall-clock
	// instant when a particular retry joined that already-known result. Keep the first durable
	// observation clock while allowing a later deterministic retry with identical facts to become
	// a no-op. Any changed hash, value, source, outcome, fee-net value, or evidence still differs
	// below and fails loudly.
	if prior.SettlementKnown && next.SettlementKnown &&
		prior.Stage == "settlement" && next.Stage == "settlement" {
		prior.At, next.At = time.Time{}, time.Time{}
		prior.ElapsedFromTriggerMS, next.ElapsedFromTriggerMS = 0, 0
	}
	priorJSON, priorErr := json.Marshal(prior)
	nextJSON, nextErr := json.Marshal(next)
	return priorErr == nil && nextErr == nil && string(priorJSON) == string(nextJSON)
}

func marshalExecutionShadowEvidence(v map[string]any) (string, error) {
	if v == nil {
		return "{}", nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func appendExecutionShadowEventTx(ctx context.Context, tx *sql.Tx,
	in ExecutionShadowEvent, evidence string) (bool, error) {
	var triggerUnixMS int64
	if err := tx.QueryRowContext(ctx,
		`SELECT trigger_unix_ms FROM execution_shadow_attempts WHERE attempt_id=?`,
		in.AttemptID).Scan(&triggerUnixMS); err != nil {
		return false, fmt.Errorf("load execution-shadow trigger: %w", err)
	}
	expectedElapsed := in.At.UnixMilli() - triggerUnixMS
	if expectedElapsed < 0 || in.ElapsedFromTriggerMS != expectedElapsed {
		return false, fmt.Errorf("execution-shadow elapsed mismatch: got %d want %d",
			in.ElapsedFromTriggerMS, expectedElapsed)
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO execution_shadow_events(
event_id,attempt_id,event_ts,elapsed_from_trigger_ms,stage,outcome,reason,book_source,book_generation,
book_subscription_id,book_sequence,book_source_ts,book_received_ts,book_age_ms,side_bid,side_ask,
spread_cents,visible_depth,tick_size,original_limit,requested_qty,fee_quote,fee_quote_source,
live_reservation_id,venue_attempted,venue_ack,venue_order_id,live_state,live_authoritative,
live_filled_qty,live_fill_price,live_fee,live_fee_source,live_receipt_source,paper_attempt_id,
paper_state,paper_filled_qty,paper_fill_price,paper_fee,paper_fee_source,paper_book_source,
shadow_state,shadow_filled_qty,shadow_fill_price,shadow_fee,shadow_fee_source,shadow_reason,
settlement_known,settlement_value,settlement_ts,settlement_source,settlement_hash,live_net,
paper_net,shadow_net,evidence_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		in.EventID, in.AttemptID, in.At.Format(time.RFC3339Nano), in.ElapsedFromTriggerMS,
		in.Stage, in.Outcome,
		in.Reason, in.BookSource, in.BookGeneration, in.BookSubscriptionID, in.BookSequence,
		optionalExecutionShadowTime(in.BookSourceAt), optionalExecutionShadowTime(in.BookReceivedAt),
		in.BookAgeMS, in.SideBid, in.SideAsk, in.SpreadCents, in.VisibleDepth, in.TickSize,
		in.OriginalLimit, in.RequestedQty, in.FeeQuote, in.FeeQuoteSource,
		in.LiveReservationID, boolIntShadow(in.VenueAttempted), boolIntShadow(in.VenueAck),
		in.VenueOrderID, in.LiveState, boolIntShadow(in.LiveAuthoritative),
		in.LiveFilledQty, in.LiveFillPrice, in.LiveFee, in.LiveFeeSource, in.LiveReceiptSource,
		in.PaperAttemptID, in.PaperState, in.PaperFilledQty, in.PaperFillPrice, in.PaperFee,
		in.PaperFeeSource, in.PaperBookSource, in.ShadowState, in.ShadowFilledQty,
		in.ShadowFillPrice, in.ShadowFee, in.ShadowFeeSource, in.ShadowReason,
		boolIntShadow(in.SettlementKnown), in.SettlementValue,
		optionalExecutionShadowTime(in.SettledAt), in.SettlementSource, in.SettlementHash,
		in.LiveNet, in.PaperNet, in.ShadowNet, evidence)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil || affected == 1 {
		return affected == 1, err
	}
	prior, scanErr := scanExecutionShadowEvent(tx.QueryRowContext(ctx,
		`SELECT `+executionShadowEventColumns+` FROM execution_shadow_events WHERE event_id=?`,
		in.EventID))
	if scanErr != nil {
		return false, scanErr
	}
	if !executionShadowEventRetryEquivalent(prior, in) {
		return false, errors.New("execution-shadow event id reused with different evidence")
	}
	return false, nil
}

// AppendExecutionShadowEvent stores one exact stage receipt. EventID is an idempotency key for
// worker retry only; distinct attempts and distinct gate transitions are never deduplicated.
func (s *Store) AppendExecutionShadowEvent(ctx context.Context,
	in ExecutionShadowEvent) (bool, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return false, err
	}
	in, err = normalizeExecutionShadowEvent(in)
	if err != nil {
		return false, err
	}
	evidence, err := marshalExecutionShadowEvidence(in.Evidence)
	if err != nil {
		return false, fmt.Errorf("marshal execution-shadow evidence: %w", err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	inserted, err := appendExecutionShadowEventTx(ctx, tx, in, evidence)
	if err != nil {
		return false, err
	}
	return inserted, tx.Commit()
}

func scanNullableFloat(v sql.NullFloat64) *float64 {
	if !v.Valid {
		return nil
	}
	out := v.Float64
	return &out
}

func scanNullableInt(v sql.NullInt64) *int64 {
	if !v.Valid {
		return nil
	}
	out := v.Int64
	return &out
}

const executionShadowEventColumns = `id,event_id,attempt_id,event_ts,elapsed_from_trigger_ms,stage,outcome,reason,
book_source,book_generation,book_subscription_id,book_sequence,book_source_ts,book_received_ts,
book_age_ms,side_bid,side_ask,spread_cents,visible_depth,tick_size,original_limit,requested_qty,
fee_quote,fee_quote_source,live_reservation_id,venue_attempted,venue_ack,venue_order_id,live_state,
live_authoritative,live_filled_qty,live_fill_price,live_fee,live_fee_source,live_receipt_source,
paper_attempt_id,paper_state,paper_filled_qty,paper_fill_price,paper_fee,paper_fee_source,
paper_book_source,shadow_state,shadow_filled_qty,shadow_fill_price,shadow_fee,shadow_fee_source,
shadow_reason,settlement_known,settlement_value,settlement_ts,settlement_source,settlement_hash,
live_net,paper_net,shadow_net,evidence_json`

func scanExecutionShadowEvent(scanner interface{ Scan(...any) error }) (ExecutionShadowEvent, error) {
	var out ExecutionShadowEvent
	var eventAt, bookSourceAt, bookReceivedAt, settledAt, evidence string
	var generation, subscription, sequence sql.NullInt64
	var bookAge, sideBid, sideAsk, spread, depth, tick, limit, requested, feeQuote sql.NullFloat64
	var liveFilled, livePrice, liveFee, paperFilled, paperPrice, paperFee sql.NullFloat64
	var shadowFilled, shadowPrice, shadowFee, settlement, liveNet, paperNet, shadowNet sql.NullFloat64
	var venueAttempted, venueAck, liveAuthoritative, settlementKnown int
	err := scanner.Scan(&out.ID, &out.EventID, &out.AttemptID, &eventAt,
		&out.ElapsedFromTriggerMS, &out.Stage,
		&out.Outcome, &out.Reason, &out.BookSource, &generation, &subscription, &sequence,
		&bookSourceAt, &bookReceivedAt, &bookAge, &sideBid, &sideAsk, &spread, &depth, &tick,
		&limit, &requested, &feeQuote, &out.FeeQuoteSource, &out.LiveReservationID,
		&venueAttempted, &venueAck, &out.VenueOrderID, &out.LiveState, &liveAuthoritative,
		&liveFilled, &livePrice, &liveFee, &out.LiveFeeSource, &out.LiveReceiptSource,
		&out.PaperAttemptID, &out.PaperState, &paperFilled, &paperPrice, &paperFee,
		&out.PaperFeeSource, &out.PaperBookSource, &out.ShadowState, &shadowFilled,
		&shadowPrice, &shadowFee, &out.ShadowFeeSource, &out.ShadowReason, &settlementKnown,
		&settlement, &settledAt, &out.SettlementSource, &out.SettlementHash, &liveNet,
		&paperNet, &shadowNet, &evidence)
	if err != nil {
		return out, err
	}
	if out.At, err = parseExecutionShadowTime(eventAt, "event_ts", true); err != nil {
		return out, err
	}
	if out.BookSourceAt, err = parseExecutionShadowTime(bookSourceAt, "book_source_ts", false); err != nil {
		return out, err
	}
	if out.BookReceivedAt, err = parseExecutionShadowTime(bookReceivedAt, "book_received_ts", false); err != nil {
		return out, err
	}
	if out.SettledAt, err = parseExecutionShadowTime(settledAt, "settlement_ts", false); err != nil {
		return out, err
	}
	out.BookGeneration, out.BookSubscriptionID, out.BookSequence =
		scanNullableInt(generation), scanNullableInt(subscription), scanNullableInt(sequence)
	out.BookAgeMS, out.SideBid, out.SideAsk = scanNullableFloat(bookAge), scanNullableFloat(sideBid), scanNullableFloat(sideAsk)
	out.SpreadCents, out.VisibleDepth, out.TickSize = scanNullableFloat(spread), scanNullableFloat(depth), scanNullableFloat(tick)
	out.OriginalLimit, out.RequestedQty, out.FeeQuote = scanNullableFloat(limit), scanNullableFloat(requested), scanNullableFloat(feeQuote)
	out.VenueAttempted, out.VenueAck, out.LiveAuthoritative = venueAttempted != 0, venueAck != 0, liveAuthoritative != 0
	out.LiveFilledQty, out.LiveFillPrice, out.LiveFee = scanNullableFloat(liveFilled), scanNullableFloat(livePrice), scanNullableFloat(liveFee)
	out.PaperFilledQty, out.PaperFillPrice, out.PaperFee = scanNullableFloat(paperFilled), scanNullableFloat(paperPrice), scanNullableFloat(paperFee)
	out.ShadowFilledQty, out.ShadowFillPrice, out.ShadowFee = scanNullableFloat(shadowFilled), scanNullableFloat(shadowPrice), scanNullableFloat(shadowFee)
	out.SettlementKnown, out.SettlementValue = settlementKnown != 0, scanNullableFloat(settlement)
	out.LiveNet, out.PaperNet, out.ShadowNet = scanNullableFloat(liveNet), scanNullableFloat(paperNet), scanNullableFloat(shadowNet)
	if strings.TrimSpace(evidence) != "" {
		if err = json.Unmarshal([]byte(evidence), &out.Evidence); err != nil {
			return out, fmt.Errorf("parse execution-shadow evidence: %w", err)
		}
	}
	return out, nil
}

// ListExecutionShadowAttempts returns complete joined histories. The attempt limit is applied
// before events, so no selected signal's gate/receipt sequence is truncated.
func (s *Store) ListExecutionShadowAttempts(ctx context.Context,
	limit int) ([]ExecutionShadowAttemptView, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 200
	}
	if limit > 2000 {
		limit = 2000
	}
	rows, err := db.QueryContext(ctx, `SELECT `+executionShadowAttemptColumns+
		` FROM execution_shadow_attempts ORDER BY observed_ts DESC,attempt_id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	views := make([]ExecutionShadowAttemptView, 0, limit)
	ids := make([]any, 0, limit)
	for rows.Next() {
		attempt, scanErr := scanExecutionShadowAttempt(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		views = append(views, ExecutionShadowAttemptView{Attempt: attempt})
		ids = append(ids, attempt.AttemptID)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil || len(ids) == 0 {
		return views, err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	eventRows, err := db.QueryContext(ctx, `SELECT `+executionShadowEventColumns+
		` FROM execution_shadow_events WHERE attempt_id IN (`+placeholders+`) ORDER BY id`, ids...)
	if err != nil {
		return nil, err
	}
	defer eventRows.Close()
	index := make(map[string]int, len(views))
	for i := range views {
		index[views[i].Attempt.AttemptID] = i
	}
	for eventRows.Next() {
		event, scanErr := scanExecutionShadowEvent(eventRows)
		if scanErr != nil {
			return nil, scanErr
		}
		if i, ok := index[event.AttemptID]; ok {
			views[i].Events = append(views[i].Events, event)
		}
	}
	return views, eventRows.Err()
}

// ExecutionShadowAttemptsByIDs returns complete immutable histories for exactly the requested
// attempt IDs. Unlike ListExecutionShadowAttempts it has no newest-row window, so an older Paper
// row cannot silently bind to an incomplete recent slice. One read transaction gives every chunk
// the same database snapshot; a concurrently appended terminal is therefore either wholly visible
// on the next review or safely absent from this one.
func (s *Store) ExecutionShadowAttemptsByIDs(ctx context.Context,
	attemptIDs []string) (map[string]ExecutionShadowAttemptView, error) {
	ids := make([]string, 0, len(attemptIDs))
	seen := make(map[string]struct{}, len(attemptIDs))
	for _, attemptID := range attemptIDs {
		attemptID = strings.TrimSpace(attemptID)
		if attemptID == "" {
			continue
		}
		if _, ok := seen[attemptID]; ok {
			continue
		}
		seen[attemptID] = struct{}{}
		ids = append(ids, attemptID)
		if len(ids) > ExecutionShadowExactIDLookupMax {
			return nil, fmt.Errorf("execution-shadow exact-id lookup exceeds %d attempts",
				ExecutionShadowExactIDLookupMax)
		}
	}
	out := make(map[string]ExecutionShadowAttemptView, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for start := 0; start < len(ids); start += executionShadowExactIDLookupChunk {
		end := min(start+executionShadowExactIDLookupChunk, len(ids))
		chunk := ids[start:end]
		args := make([]any, len(chunk))
		for i := range chunk {
			args[i] = chunk[i]
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		rows, queryErr := tx.QueryContext(ctx, `SELECT `+executionShadowAttemptColumns+
			` FROM execution_shadow_attempts WHERE attempt_id IN (`+placeholders+`)`, args...)
		if queryErr != nil {
			return nil, queryErr
		}
		for rows.Next() {
			attempt, scanErr := scanExecutionShadowAttempt(rows)
			if scanErr != nil {
				rows.Close()
				return nil, scanErr
			}
			out[attempt.AttemptID] = ExecutionShadowAttemptView{Attempt: attempt}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return nil, rowsErr
		}
		if closeErr := rows.Close(); closeErr != nil {
			return nil, closeErr
		}

		eventRows, queryErr := tx.QueryContext(ctx, `SELECT `+executionShadowEventColumns+
			` FROM execution_shadow_events WHERE attempt_id IN (`+placeholders+`) ORDER BY id`, args...)
		if queryErr != nil {
			return nil, queryErr
		}
		for eventRows.Next() {
			event, scanErr := scanExecutionShadowEvent(eventRows)
			if scanErr != nil {
				eventRows.Close()
				return nil, scanErr
			}
			view, ok := out[event.AttemptID]
			if !ok {
				continue
			}
			view.Events = append(view.Events, event)
			out[event.AttemptID] = view
		}
		if rowsErr := eventRows.Err(); rowsErr != nil {
			eventRows.Close()
			return nil, rowsErr
		}
		if closeErr := eventRows.Close(); closeErr != nil {
			return nil, closeErr
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// ExecutionShadowFundedSettlementPendingIDs returns only exact shared attempts whose funded-Paper
// lane has terminal economics still waiting for a Paper settlement net. It deliberately performs
// this narrow indexed test before complete histories and venue receipts are loaded, so repeatedly
// retained portfolio closes cost one small exact-id query instead of a permanent settlement scan.
// Returned IDs preserve the caller's normalized, de-duplicated order.
func (s *Store) ExecutionShadowFundedSettlementPendingIDs(ctx context.Context,
	attemptIDs []string) ([]string, error) {
	ids := make([]string, 0, len(attemptIDs))
	seen := make(map[string]struct{}, len(attemptIDs))
	for _, attemptID := range attemptIDs {
		attemptID = strings.TrimSpace(attemptID)
		if attemptID == "" {
			continue
		}
		if _, ok := seen[attemptID]; ok {
			continue
		}
		seen[attemptID] = struct{}{}
		ids = append(ids, attemptID)
		if len(ids) > ExecutionShadowExactIDLookupMax {
			return nil, fmt.Errorf("execution-shadow funded-settlement lookup exceeds %d attempts",
				ExecutionShadowExactIDLookupMax)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	pending := make(map[string]struct{}, len(ids))
	for start := 0; start < len(ids); start += executionShadowExactIDLookupChunk {
		end := min(start+executionShadowExactIDLookupChunk, len(ids))
		chunk := ids[start:end]
		args := make([]any, len(chunk))
		for i := range chunk {
			args[i] = chunk[i]
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		rows, queryErr := tx.QueryContext(ctx, `SELECT a.attempt_id
FROM execution_shadow_attempts a
WHERE a.attempt_id IN (`+placeholders+`)
AND EXISTS (
 SELECT 1 FROM execution_shadow_events e
 WHERE e.attempt_id=a.attempt_id
 AND (
  (COALESCE(e.paper_filled_qty,0)>0 AND e.paper_fill_price IS NOT NULL
   AND e.paper_fee IS NOT NULL)
  OR UPPER(TRIM(COALESCE(e.paper_state,'')))='PAPER-ZERO-FILL'
 )
)
AND NOT EXISTS (
 SELECT 1 FROM execution_shadow_events e
 WHERE e.attempt_id=a.attempt_id AND e.settlement_known=1 AND e.paper_net IS NOT NULL
)`, args...)
		if queryErr != nil {
			return nil, queryErr
		}
		for rows.Next() {
			var attemptID string
			if scanErr := rows.Scan(&attemptID); scanErr != nil {
				rows.Close()
				return nil, scanErr
			}
			pending[attemptID] = struct{}{}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			rows.Close()
			return nil, rowsErr
		}
		if closeErr := rows.Close(); closeErr != nil {
			return nil, closeErr
		}
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(pending))
	for _, attemptID := range ids {
		if _, ok := pending[attemptID]; ok {
			out = append(out, attemptID)
		}
	}
	return out, nil
}

// ExecutionShadowMeta returns one isolated-ledger maintenance value. These values are operational
// cursors only: they cannot create attempts, events, profit, or cash authority.
func (s *Store) ExecutionShadowMeta(ctx context.Context, key string) (string, bool, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return "", false, err
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", false, errors.New("empty execution-shadow metadata key")
	}
	var value string
	err = db.QueryRowContext(ctx,
		`SELECT value FROM execution_shadow_meta WHERE key=?`, key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return value, err == nil, err
}

// SetExecutionShadowMeta durably advances one isolated-ledger maintenance cursor. The cursor is
// deliberately stored beside the attempt sequence it names, so a paired restore cannot leave the
// main cash database pointing past an older comparison-ledger snapshot.
func (s *Store) SetExecutionShadowMeta(ctx context.Context, key, value string) error {
	db, err := s.executionShadowHandle()
	if err != nil {
		return err
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return errors.New("empty execution-shadow metadata key")
	}
	_, err = db.ExecContext(ctx, `INSERT INTO execution_shadow_meta(key,value,updated_ts)
VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_ts=excluded.updated_ts`,
		key, value, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// ListOpenExecutionShadowAttemptsBySequence returns one fair immutable-sequence window from a
// frozen prefix of the isolated ledger. Unlike ListOpenExecutionShadowAttempts, a caller can
// durably advance past unresolved or comparison-pending rows instead of rereading the same head
// forever. New attempts above throughSequence wait for the caller's next frozen cycle.
func (s *Store) ListOpenExecutionShadowAttemptsBySequence(ctx context.Context,
	afterSequence, throughSequence int64, limit int) ([]ExecutionShadowAttemptView, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if afterSequence < 0 || throughSequence < 0 || afterSequence > throughSequence {
		return nil, errors.New("invalid execution-shadow settlement sequence window")
	}
	if throughSequence == 0 {
		return nil, nil
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT o.sequence,a.attempt_id
FROM execution_shadow_attempt_order o
JOIN execution_shadow_attempts a ON a.attempt_id=o.attempt_id
WHERE o.sequence>? AND o.sequence<=? AND (
 (
  NOT EXISTS (
   SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
   AND e.settlement_known=1
  )
  AND EXISTS (
   SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
   AND (
    e.stage='counterfactual-execution-terminal'
    OR (e.live_authoritative=1 AND e.live_filled_qty IS NOT NULL
        AND TRIM(COALESCE(e.live_state,''))<>'')
    OR
    (COALESCE(e.live_filled_qty,0)>0 AND e.live_fill_price IS NOT NULL AND e.live_fee IS NOT NULL)
    OR (COALESCE(e.paper_filled_qty,0)>0 AND e.paper_fill_price IS NOT NULL AND e.paper_fee IS NOT NULL)
    OR (
     COALESCE(e.shadow_filled_qty,0)>0 AND e.shadow_fill_price IS NOT NULL AND e.shadow_fee IS NOT NULL
     AND (
      e.stage='counterfactual-execution-terminal'
      OR NOT EXISTS (
       SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
       AND x.stage='counterfactual-execution-terminal'
      )
     )
    )
   )
  )
 )
 OR (
  EXISTS (
   SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
   AND e.settlement_known=1
  )
  AND (
   (
    EXISTS (
     SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
     AND (
      (COALESCE(e.live_filled_qty,0)>0 AND e.live_fill_price IS NOT NULL AND e.live_fee IS NOT NULL)
      OR (e.live_authoritative=1 AND e.live_filled_qty=0
          AND TRIM(COALESCE(e.live_state,''))<>'')
     )
    )
    AND NOT EXISTS (
     SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
     AND e.settlement_known=1 AND e.live_net IS NOT NULL
    )
   )
   OR (
    EXISTS (
     SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
     AND (
      (COALESCE(e.paper_filled_qty,0)>0 AND e.paper_fill_price IS NOT NULL AND e.paper_fee IS NOT NULL)
      OR UPPER(TRIM(COALESCE(e.paper_state,'')))='PAPER-ZERO-FILL'
     )
    )
    AND NOT EXISTS (
     SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
     AND e.settlement_known=1 AND e.paper_net IS NOT NULL
    )
   )
   OR (
    EXISTS (
     SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
     AND (
      (
       COALESCE(e.shadow_filled_qty,0)>0 AND e.shadow_fill_price IS NOT NULL AND e.shadow_fee IS NOT NULL
       AND (
        e.stage='counterfactual-execution-terminal'
        OR NOT EXISTS (
         SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
         AND x.stage='counterfactual-execution-terminal'
        )
       )
      )
      OR (
       e.stage='counterfactual-execution-terminal'
       AND LOWER(TRIM(CASE WHEN TRIM(COALESCE(e.shadow_state,''))<>''
                           THEN e.shadow_state ELSE e.outcome END))
           IN ('modeled_zero_fill','zero_fill','zero-fill','unfilled')
      )
     )
    )
    AND (
     (
      NOT EXISTS (
       SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
       AND x.stage='counterfactual-execution-terminal'
      )
      AND NOT EXISTS (
       SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
       AND e.settlement_known=1 AND e.shadow_net IS NOT NULL
      )
     )
     OR (
      EXISTS (
       SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
       AND x.stage='counterfactual-execution-terminal'
      )
      AND NOT EXISTS (
       SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
       AND e.settlement_known=1 AND e.shadow_net IS NOT NULL
       AND e.id > (
        SELECT MAX(x.id) FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
        AND x.stage='counterfactual-execution-terminal'
       )
      )
     )
    )
   )
  )
 )
)
ORDER BY o.sequence LIMIT ?`, afterSequence, throughSequence, limit)
	if err != nil {
		return nil, err
	}
	sequences := make([]int64, 0, limit)
	ids := make([]string, 0, limit)
	for rows.Next() {
		var sequence int64
		var attemptID string
		if err = rows.Scan(&sequence, &attemptID); err != nil {
			rows.Close()
			return nil, err
		}
		sequences = append(sequences, sequence)
		ids = append(ids, attemptID)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil || len(ids) == 0 {
		return nil, err
	}
	histories, err := s.ExecutionShadowAttemptsByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	out := make([]ExecutionShadowAttemptView, 0, len(ids))
	for i, attemptID := range ids {
		view, ok := histories[attemptID]
		if !ok {
			return nil, fmt.Errorf("execution-shadow sequence %d lost attempt %q",
				sequences[i], attemptID)
		}
		view.Sequence = sequences[i]
		out = append(out, view)
	}
	return out, nil
}

// ExecutionShadowLiveTerminal returns the canonical LIVE terminal for one immutable detector
// attempt. Exact venue reconciliation outranks the handler receipt, which outranks a local
// economic no-send. Legacy combo-queue cleanup rows are explicitly excluded: those secondary
// copies may be removed after the handler completed and can never supersede cash truth.
func (s *Store) ExecutionShadowLiveTerminal(ctx context.Context,
	attemptID string) (ExecutionShadowEvent, bool, error) {
	var out ExecutionShadowEvent
	attemptID = strings.TrimSpace(attemptID)
	if attemptID == "" {
		return out, false, nil
	}
	db, err := s.executionShadowHandle()
	if err != nil {
		return out, false, err
	}
	out, err = scanExecutionShadowEvent(db.QueryRowContext(ctx, `SELECT `+
		executionShadowEventColumns+` FROM execution_shadow_events
WHERE attempt_id=? AND TRIM(COALESCE(live_state,''))<>''
AND (
 stage IN ('live-terminal','live-exact-reconcile')
 OR (
  live_state='not-sent' AND venue_attempted=0 AND live_authoritative=0
  AND stage NOT IN ('combo-candidate-queue','combo-candidate-read')
  AND NOT (
   stage='queue-clear'
   AND COALESCE(json_extract(evidence_json,'$.queue_branch'),'')='live-mirror-combo'
  )
 )
)
ORDER BY CASE
 WHEN stage='live-exact-reconcile' THEN 0
 WHEN stage='live-terminal' THEN 1
 ELSE 2
END, id DESC LIMIT 1`, attemptID))
	if errors.Is(err, sql.ErrNoRows) {
		return ExecutionShadowEvent{}, false, nil
	}
	return out, err == nil, err
}

func executionShadowCounterfactualExecutionPendingQuery() string {
	return `WITH seed(attempt_id) AS MATERIALIZED (
 SELECT DISTINCT seed.attempt_id
 FROM execution_shadow_events AS seed
 INDEXED BY idx_execution_shadow_events_recovery_seed
 WHERE seed.stage='live-first-preflight' AND seed.outcome='passed'
) SELECT ` + executionShadowAttemptColumnsAliasA + `
FROM seed
CROSS JOIN execution_shadow_attempts AS a ON a.attempt_id=seed.attempt_id
WHERE a.venue='kalshi'
AND a.action='BUY'
AND a.route='taker'
AND EXISTS (
 SELECT 1 FROM execution_shadow_events AS e
 INDEXED BY idx_execution_shadow_events_live_terminal
 WHERE e.attempt_id=a.attempt_id AND e.live_state<>''
)
AND NOT EXISTS (
 SELECT 1 FROM execution_shadow_events AS e
 INDEXED BY idx_execution_shadow_events_attempt_stage_outcome
 WHERE e.attempt_id=a.attempt_id AND e.stage='counterfactual-execution-terminal'
)
AND NOT EXISTS (
 SELECT 1 FROM execution_shadow_events AS e
 INDEXED BY idx_execution_shadow_events_settlement
 WHERE e.attempt_id=a.attempt_id AND e.settlement_known=1
)
ORDER BY CASE WHEN EXISTS (
 SELECT 1 FROM execution_shadow_events AS e
 INDEXED BY idx_execution_shadow_events_live_fill
 WHERE e.attempt_id=a.attempt_id AND e.live_filled_qty>0 AND e.live_state<>''
) THEN 0 ELSE 1 END,
a.observed_ts DESC,a.attempt_id DESC LIMIT ?`
}

// ListExecutionShadowCounterfactualExecutionPending returns recent attempts whose LIVE lane has
// reached a terminal state but whose delayed execution-only comparison has not. Complete event
// histories are joined so a recovery worker can reconstruct the immutable candidate and terminal
// receipt without reading the cash database. Actual LIVE fills are returned before non-fills,
// then newest attempts first. The bounded limit prevents recovery from monopolizing the isolated
// comparison database after a long outage.
func (s *Store) ListExecutionShadowCounterfactualExecutionPending(ctx context.Context,
	limit int) ([]ExecutionShadowAttemptView, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx,
		executionShadowCounterfactualExecutionPendingQuery(), limit)
	if err != nil {
		return nil, err
	}
	views := make([]ExecutionShadowAttemptView, 0, limit)
	ids := make([]any, 0, limit)
	for rows.Next() {
		attempt, scanErr := scanExecutionShadowAttempt(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		views = append(views, ExecutionShadowAttemptView{Attempt: attempt})
		ids = append(ids, attempt.AttemptID)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil || len(ids) == 0 {
		return views, err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	eventRows, err := db.QueryContext(ctx, `SELECT `+executionShadowEventColumns+
		` FROM execution_shadow_events WHERE attempt_id IN (`+placeholders+`) ORDER BY id`, ids...)
	if err != nil {
		return nil, err
	}
	defer eventRows.Close()
	index := make(map[string]int, len(views))
	for i := range views {
		index[views[i].Attempt.AttemptID] = i
	}
	for eventRows.Next() {
		event, scanErr := scanExecutionShadowEvent(eventRows)
		if scanErr != nil {
			return nil, scanErr
		}
		if i, ok := index[event.AttemptID]; ok {
			views[i].Events = append(views[i].Events, event)
		}
	}
	return views, eventRows.Err()
}

func executionShadowFundedPaperGapsQuery() string {
	return `WITH seed(attempt_id) AS MATERIALIZED (
 SELECT DISTINCT seed.attempt_id
 FROM execution_shadow_events AS seed
 INDEXED BY idx_execution_shadow_events_recovery_seed
 WHERE seed.stage='funded-paper-branch' AND seed.outcome='offered'
) SELECT ` + executionShadowAttemptColumnsAliasA + `
FROM seed
CROSS JOIN execution_shadow_attempts AS a ON a.attempt_id=seed.attempt_id
WHERE EXISTS (
 SELECT 1 FROM execution_shadow_events AS e
 INDEXED BY idx_execution_shadow_events_live_terminal
 WHERE e.attempt_id=a.attempt_id AND e.live_state<>''
)
AND NOT EXISTS (
 SELECT 1 FROM execution_shadow_events AS e
 INDEXED BY idx_execution_shadow_events_attempt_stage_outcome
 WHERE e.attempt_id=a.attempt_id AND e.stage='funded-paper-terminal'
 AND (
  UPPER(TRIM(COALESCE(e.paper_state,''))) IN
   ('PAPER-FILLED','PAPER-ZERO-FILL','PAPER-REJECTED')
  OR UPPER(TRIM(e.outcome))<>'PAPER-DROPPED'
 )
)
ORDER BY CASE WHEN EXISTS (
 SELECT 1 FROM execution_shadow_events AS e
 INDEXED BY idx_execution_shadow_events_live_fill
 WHERE e.attempt_id=a.attempt_id AND e.live_filled_qty>0 AND e.live_state<>''
) THEN 0 ELSE 1 END,
a.observed_ts DESC,a.attempt_id DESC LIMIT ?`
}

// ListExecutionShadowFundedPaperGaps returns offered funded-Paper branches that have enough durable
// LIVE evidence to continue but no genuine Paper completion. Historical PAPER-DROPPED rows describe
// scheduler loss rather than an execution result, so they deliberately do not close the gap. Every
// selected attempt includes its complete event history for deterministic recovery.
func (s *Store) ListExecutionShadowFundedPaperGaps(ctx context.Context,
	limit int) ([]ExecutionShadowAttemptView, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, executionShadowFundedPaperGapsQuery(), limit)
	if err != nil {
		return nil, err
	}
	views := make([]ExecutionShadowAttemptView, 0, limit)
	ids := make([]any, 0, limit)
	for rows.Next() {
		attempt, scanErr := scanExecutionShadowAttempt(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		views = append(views, ExecutionShadowAttemptView{Attempt: attempt})
		ids = append(ids, attempt.AttemptID)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil || len(ids) == 0 {
		return views, err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	eventRows, err := db.QueryContext(ctx, `SELECT `+executionShadowEventColumns+
		` FROM execution_shadow_events WHERE attempt_id IN (`+placeholders+`) ORDER BY id`, ids...)
	if err != nil {
		return nil, err
	}
	defer eventRows.Close()
	index := make(map[string]int, len(views))
	for i := range views {
		index[views[i].Attempt.AttemptID] = i
	}
	for eventRows.Next() {
		event, scanErr := scanExecutionShadowEvent(eventRows)
		if scanErr != nil {
			return nil, scanErr
		}
		if i, ok := index[event.AttemptID]; ok {
			views[i].Events = append(views[i].Events, event)
		}
	}
	return views, eventRows.Err()
}

// ListOpenExecutionShadowAttempts returns canonical attempts that still need either their
// authoritative market outcome or exact lane economics. A terminal execution observation remains
// eligible even when nothing filled: modeled/venue zero-fills have known zero economics, while a
// not-observed terminal still needs the venue outcome with its economics left unknown. A later
// exact fill fee can make an already-settled attempt eligible again, allowing a net-only backfill
// without rewriting history.
func (s *Store) ListOpenExecutionShadowAttempts(ctx context.Context,
	limit int) ([]ExecutionShadowAttemptView, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT `+executionShadowAttemptColumns+`
FROM execution_shadow_attempts a
WHERE (
 NOT EXISTS (
  SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
  AND e.settlement_known=1
 )
 AND EXISTS (
  SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
  AND (
   e.stage='counterfactual-execution-terminal'
   OR (e.live_authoritative=1 AND e.live_filled_qty IS NOT NULL
       AND TRIM(COALESCE(e.live_state,''))<>'')
   OR
   (COALESCE(e.live_filled_qty,0)>0 AND e.live_fill_price IS NOT NULL AND e.live_fee IS NOT NULL)
   OR (COALESCE(e.paper_filled_qty,0)>0 AND e.paper_fill_price IS NOT NULL AND e.paper_fee IS NOT NULL)
   OR (
    COALESCE(e.shadow_filled_qty,0)>0 AND e.shadow_fill_price IS NOT NULL AND e.shadow_fee IS NOT NULL
    AND (
     e.stage='counterfactual-execution-terminal'
     OR NOT EXISTS (
      SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
      AND x.stage='counterfactual-execution-terminal'
     )
    )
   )
  )
 )
)
OR (
 EXISTS (
  SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
  AND e.settlement_known=1
 )
 AND (
  (
   EXISTS (
    SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
    AND (
     (COALESCE(e.live_filled_qty,0)>0 AND e.live_fill_price IS NOT NULL AND e.live_fee IS NOT NULL)
     OR (e.live_authoritative=1 AND e.live_filled_qty=0
         AND TRIM(COALESCE(e.live_state,''))<>'')
    )
   )
   AND NOT EXISTS (
    SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
    AND e.settlement_known=1 AND e.live_net IS NOT NULL
   )
  )
  OR (
   EXISTS (
    SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
    AND (
     (COALESCE(e.paper_filled_qty,0)>0 AND e.paper_fill_price IS NOT NULL AND e.paper_fee IS NOT NULL)
     OR UPPER(TRIM(COALESCE(e.paper_state,'')))='PAPER-ZERO-FILL'
    )
   )
   AND NOT EXISTS (
    SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
    AND e.settlement_known=1 AND e.paper_net IS NOT NULL
   )
  )
  OR (
   EXISTS (
    SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
    AND (
     (
      COALESCE(e.shadow_filled_qty,0)>0 AND e.shadow_fill_price IS NOT NULL AND e.shadow_fee IS NOT NULL
      AND (
       e.stage='counterfactual-execution-terminal'
       OR NOT EXISTS (
        SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
        AND x.stage='counterfactual-execution-terminal'
       )
      )
     )
     OR (
      e.stage='counterfactual-execution-terminal'
      AND LOWER(TRIM(CASE WHEN TRIM(COALESCE(e.shadow_state,''))<>''
                          THEN e.shadow_state ELSE e.outcome END))
          IN ('modeled_zero_fill','zero_fill','zero-fill','unfilled')
     )
    )
   )
   AND (
    (
     NOT EXISTS (
      SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
      AND x.stage='counterfactual-execution-terminal'
     )
     AND NOT EXISTS (
      SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
      AND e.settlement_known=1 AND e.shadow_net IS NOT NULL
     )
    )
    OR (
     EXISTS (
      SELECT 1 FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
      AND x.stage='counterfactual-execution-terminal'
     )
     AND NOT EXISTS (
      SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
      AND e.settlement_known=1 AND e.shadow_net IS NOT NULL
      AND e.id > (
       SELECT MAX(x.id) FROM execution_shadow_events x WHERE x.attempt_id=a.attempt_id
       AND x.stage='counterfactual-execution-terminal'
      )
     )
    )
   )
  )
 )
)
ORDER BY a.observed_ts,a.attempt_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	var attempts []ExecutionShadowAttemptView
	var ids []any
	for rows.Next() {
		attempt, scanErr := scanExecutionShadowAttempt(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		attempts = append(attempts, ExecutionShadowAttemptView{Attempt: attempt})
		ids = append(ids, attempt.AttemptID)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil || len(ids) == 0 {
		return attempts, err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	eventRows, err := db.QueryContext(ctx, `SELECT `+executionShadowEventColumns+
		` FROM execution_shadow_events WHERE attempt_id IN (`+placeholders+`) ORDER BY id`, ids...)
	if err != nil {
		return nil, err
	}
	defer eventRows.Close()
	index := make(map[string]int, len(attempts))
	for i := range attempts {
		index[attempts[i].Attempt.AttemptID] = i
	}
	for eventRows.Next() {
		event, scanErr := scanExecutionShadowEvent(eventRows)
		if scanErr != nil {
			return nil, scanErr
		}
		if i, ok := index[event.AttemptID]; ok {
			attempts[i].Events = append(attempts[i].Events, event)
		}
	}
	return attempts, eventRows.Err()
}

// ListExecutionShadowLiveReceiptPending finds every venue attempt with a durable reservation but
// no exact terminal fee receipt yet. This deliberately includes zero-at-ACK pending/maker orders:
// a later order-scoped fill must not disappear merely because the first handler response showed 0.
func (s *Store) ListExecutionShadowLiveReceiptPending(ctx context.Context,
	limit int) ([]ExecutionShadowAttemptView, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT `+executionShadowAttemptColumns+`
FROM execution_shadow_attempts a
WHERE EXISTS (
 SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
 AND e.live_reservation_id<>'' AND e.venue_attempted=1
)
AND NOT EXISTS (
 SELECT 1 FROM execution_shadow_events e WHERE e.attempt_id=a.attempt_id
 AND e.live_fee IS NOT NULL
)
ORDER BY a.observed_ts,a.attempt_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	var attempts []ExecutionShadowAttemptView
	var ids []any
	for rows.Next() {
		attempt, scanErr := scanExecutionShadowAttempt(rows)
		if scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		attempts = append(attempts, ExecutionShadowAttemptView{Attempt: attempt})
		ids = append(ids, attempt.AttemptID)
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil || len(ids) == 0 {
		return attempts, err
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	eventRows, err := db.QueryContext(ctx, `SELECT `+executionShadowEventColumns+
		` FROM execution_shadow_events WHERE attempt_id IN (`+placeholders+`) ORDER BY id`, ids...)
	if err != nil {
		return nil, err
	}
	defer eventRows.Close()
	index := make(map[string]int, len(attempts))
	for i := range attempts {
		index[attempts[i].Attempt.AttemptID] = i
	}
	for eventRows.Next() {
		event, scanErr := scanExecutionShadowEvent(eventRows)
		if scanErr != nil {
			return nil, scanErr
		}
		if i, ok := index[event.AttemptID]; ok {
			attempts[i].Events = append(attempts[i].Events, event)
		}
	}
	return attempts, eventRows.Err()
}
