package storage

// The research microstructure sampler is intentionally bounded, but a lifetime cap must not turn
// into a permanent collector outage.  Completed raw frames therefore use the same two-durable-
// phase rule as the PTT archive: commit a content-preserving copy to kalshi_archive.db first, then
// delete only byte-equivalent main-side rows.  A crash between phases leaves duplicates, never a
// missing frame.  Derived route observations remain in the main research ledger.

import (
	"context"
	"fmt"
	"time"
)

const (
	// Seven days keeps at most about 35k frames in the hot DB under the 5k/day sampler budget,
	// comfortably below the 100k lifetime guard while retaining recent horizon/debug reads.
	MicrostructureArchiveAfterDays = 7
	microstructureArchiveBatch     = 20000
)

const microstructureFrameCols = `id,observed_ts,received_ts,source_channel,source_generation,
source_subscription_id,source_sequence,ticker,canonical_event_id,market_status,close_ts,
book_source,quote_age_s,tick_size,yes_bid,yes_ask,yes_bid_depth,yes_ask_depth,bid_levels_json,
ask_levels_json,book_hash,prior_book_hash,transition_class,signed_flow_10s,flow_units_10s,
signed_flow_60s,flow_units_60s,yes_taker_fee_1,no_taker_fee_1,yes_maker_fee_1,no_maker_fee_1,
fee_source,funded,paper_authority,live_authority`

const microstructureHorizonCols = `id,frame_id,horizon_s,captured_ts,status,yes_bid,yes_ask,no_bid,
no_ask,yes_exit_fee_1,no_exit_fee_1,quote_age_s,reason,funded,paper_authority,live_authority`

const microstructureArchiveSchemaSQL = `
CREATE TABLE IF NOT EXISTS archive.research_microstructure_frames (
 id INTEGER PRIMARY KEY, observed_ts TEXT NOT NULL, received_ts TEXT NOT NULL,
 source_channel TEXT NOT NULL, source_generation INTEGER NOT NULL,
 source_subscription_id INTEGER NOT NULL, source_sequence INTEGER NOT NULL,
 ticker TEXT NOT NULL, canonical_event_id TEXT NOT NULL DEFAULT '', market_status TEXT NOT NULL DEFAULT '',
 close_ts TEXT NOT NULL DEFAULT '', book_source TEXT NOT NULL, quote_age_s REAL NOT NULL,
 tick_size REAL NOT NULL, yes_bid REAL NOT NULL, yes_ask REAL NOT NULL,
 yes_bid_depth REAL NOT NULL, yes_ask_depth REAL NOT NULL,
 bid_levels_json TEXT NOT NULL, ask_levels_json TEXT NOT NULL, book_hash TEXT NOT NULL,
 prior_book_hash TEXT NOT NULL DEFAULT '', transition_class TEXT NOT NULL,
 signed_flow_10s REAL NOT NULL, flow_units_10s REAL NOT NULL,
 signed_flow_60s REAL NOT NULL, flow_units_60s REAL NOT NULL,
 yes_taker_fee_1 REAL NOT NULL, no_taker_fee_1 REAL NOT NULL,
 yes_maker_fee_1 REAL NOT NULL, no_maker_fee_1 REAL NOT NULL, fee_source TEXT NOT NULL,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(ticker,observed_ts,book_hash)
);
CREATE INDEX IF NOT EXISTS archive.idx_arc_rmicro_ticker
 ON research_microstructure_frames(ticker,id DESC);
CREATE INDEX IF NOT EXISTS archive.idx_arc_rmicro_observed
 ON research_microstructure_frames(observed_ts,id);
CREATE TABLE IF NOT EXISTS archive.research_microstructure_horizons (
 id INTEGER PRIMARY KEY, frame_id INTEGER NOT NULL, horizon_s INTEGER NOT NULL,
 captured_ts TEXT NOT NULL, status TEXT NOT NULL, yes_bid REAL, yes_ask REAL,
 no_bid REAL, no_ask REAL, yes_exit_fee_1 REAL, no_exit_fee_1 REAL, quote_age_s REAL,
 reason TEXT NOT NULL DEFAULT '', funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(frame_id,horizon_s)
);
CREATE INDEX IF NOT EXISTS archive.idx_arc_rmicro_horizon_frame
 ON research_microstructure_horizons(frame_id,horizon_s);
`

// The guard is transaction-local in effect: it is set and cleared in the same main transaction.
// A crash rolls the transaction back, so ordinary DELETEs remain rejected by the append-only
// triggers.  This replaces the unconditional R138 triggers without weakening their default.
const microstructureMainMoveSchemaSQL = `
CREATE TABLE IF NOT EXISTS research_archive_move_guard (
 id INTEGER PRIMARY KEY CHECK(id=1), microstructure_move INTEGER NOT NULL DEFAULT 0
 CHECK(microstructure_move IN (0,1))
);
INSERT OR IGNORE INTO research_archive_move_guard(id,microstructure_move) VALUES(1,0);
DROP TRIGGER IF EXISTS research_microstructure_horizons_no_delete;
DROP TRIGGER IF EXISTS research_microstructure_frames_no_delete;
CREATE TRIGGER research_microstructure_horizons_no_delete BEFORE DELETE ON research_microstructure_horizons
WHEN COALESCE((SELECT microstructure_move FROM research_archive_move_guard WHERE id=1),0)<>1
BEGIN SELECT RAISE(ABORT,'append-only microstructure horizon'); END;
CREATE TRIGGER research_microstructure_frames_no_delete BEFORE DELETE ON research_microstructure_frames
WHEN COALESCE((SELECT microstructure_move FROM research_archive_move_guard WHERE id=1),0)<>1
BEGIN SELECT RAISE(ABORT,'append-only microstructure frame'); END;
`

const exactMicrostructureHorizonMatch = `a.id=h.id AND a.frame_id=h.frame_id AND
a.horizon_s=h.horizon_s AND a.captured_ts=h.captured_ts AND a.status=h.status AND
a.yes_bid IS h.yes_bid AND a.yes_ask IS h.yes_ask AND a.no_bid IS h.no_bid AND a.no_ask IS h.no_ask AND
a.yes_exit_fee_1 IS h.yes_exit_fee_1 AND a.no_exit_fee_1 IS h.no_exit_fee_1 AND
a.quote_age_s IS h.quote_age_s AND a.reason=h.reason AND a.funded=h.funded AND
a.paper_authority=h.paper_authority AND a.live_authority=h.live_authority`

const exactMicrostructureFrameMatch = `a.id=f.id AND a.observed_ts=f.observed_ts AND
a.received_ts=f.received_ts AND a.source_channel=f.source_channel AND
a.source_generation=f.source_generation AND a.source_subscription_id=f.source_subscription_id AND
a.source_sequence=f.source_sequence AND a.ticker=f.ticker AND
a.canonical_event_id=f.canonical_event_id AND a.market_status=f.market_status AND
a.close_ts=f.close_ts AND a.book_source=f.book_source AND a.quote_age_s=f.quote_age_s AND
a.tick_size=f.tick_size AND a.yes_bid=f.yes_bid AND a.yes_ask=f.yes_ask AND
a.yes_bid_depth=f.yes_bid_depth AND a.yes_ask_depth=f.yes_ask_depth AND
a.bid_levels_json=f.bid_levels_json AND a.ask_levels_json=f.ask_levels_json AND
a.book_hash=f.book_hash AND a.prior_book_hash=f.prior_book_hash AND
a.transition_class=f.transition_class AND a.signed_flow_10s=f.signed_flow_10s AND
a.flow_units_10s=f.flow_units_10s AND a.signed_flow_60s=f.signed_flow_60s AND
a.flow_units_60s=f.flow_units_60s AND a.yes_taker_fee_1=f.yes_taker_fee_1 AND
a.no_taker_fee_1=f.no_taker_fee_1 AND a.yes_maker_fee_1=f.yes_maker_fee_1 AND
a.no_maker_fee_1=f.no_maker_fee_1 AND a.fee_source=f.fee_source AND a.funded=f.funded AND
a.paper_authority=f.paper_authority AND a.live_authority=f.live_authority`

// ArchiveResearchMicrostructure moves one bounded batch of completed raw frames.  It preserves
// every attached captured/missed horizon and deletes only rows whose complete content is already
// durable in the archive.  The operation is idempotent after interruption.
func (s *Store) ArchiveResearchMicrostructure(ctx context.Context, olderThanDays, batch int) (frames, horizons int64, err error) {
	if olderThanDays <= 0 {
		olderThanDays = MicrostructureArchiveAfterDays
	}
	if batch <= 0 || batch > microstructureArchiveBatch {
		batch = microstructureArchiveBatch
	}
	cut := time.Now().UTC().Add(-time.Duration(olderThanDays) * 24 * time.Hour).Format(time.RFC3339Nano)
	s.archMu.Lock()
	defer s.archMu.Unlock()
	if err = s.ensureArchiveLocked(ctx); err != nil {
		return 0, 0, err
	}
	if _, err = s.archConn.ExecContext(ctx, microstructureArchiveSchemaSQL+microstructureMainMoveSchemaSQL); err != nil {
		return 0, 0, fmt.Errorf("microstructure archive migrate: %w", err)
	}

	archiveTx, err := s.archConn.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = archiveTx.Rollback() }()
	if _, err = archiveTx.ExecContext(ctx, `INSERT OR IGNORE INTO archive.research_microstructure_frames (`+microstructureFrameCols+`)
SELECT `+microstructureFrameCols+` FROM main.research_microstructure_frames
WHERE id IN (SELECT id FROM main.research_microstructure_frames WHERE observed_ts<? ORDER BY id LIMIT ?)`, cut, batch); err != nil {
		return 0, 0, fmt.Errorf("microstructure archive frame copy: %w", err)
	}
	if _, err = archiveTx.ExecContext(ctx, `INSERT OR IGNORE INTO archive.research_microstructure_horizons (`+microstructureHorizonCols+`)
SELECT `+microstructureHorizonCols+` FROM main.research_microstructure_horizons
WHERE frame_id IN (SELECT id FROM main.research_microstructure_frames WHERE observed_ts<? ORDER BY id LIMIT ?)`, cut, batch); err != nil {
		return 0, 0, fmt.Errorf("microstructure archive horizon copy: %w", err)
	}
	if err = archiveTx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("microstructure archive durable phase: %w", err)
	}

	pruneTx, err := s.archConn.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = pruneTx.Rollback() }()
	if _, err = pruneTx.ExecContext(ctx, `UPDATE main.research_archive_move_guard SET microstructure_move=1 WHERE id=1`); err != nil {
		return 0, 0, err
	}
	hRes, err := pruneTx.ExecContext(ctx, `DELETE FROM main.research_microstructure_horizons AS h
WHERE h.frame_id IN (SELECT id FROM main.research_microstructure_frames WHERE observed_ts<? ORDER BY id LIMIT ?)
AND EXISTS (SELECT 1 FROM archive.research_microstructure_horizons a WHERE `+exactMicrostructureHorizonMatch+`)`, cut, batch)
	if err != nil {
		return 0, 0, fmt.Errorf("microstructure archive horizon prune: %w", err)
	}
	fRes, err := pruneTx.ExecContext(ctx, `DELETE FROM main.research_microstructure_frames AS f
WHERE f.id IN (SELECT id FROM main.research_microstructure_frames WHERE observed_ts<? ORDER BY id LIMIT ?)
AND NOT EXISTS (SELECT 1 FROM main.research_microstructure_horizons h WHERE h.frame_id=f.id)
AND EXISTS (SELECT 1 FROM archive.research_microstructure_frames a WHERE `+exactMicrostructureFrameMatch+`)`, cut, batch)
	if err != nil {
		return 0, 0, fmt.Errorf("microstructure archive frame prune: %w", err)
	}
	if _, err = pruneTx.ExecContext(ctx, `UPDATE main.research_archive_move_guard SET microstructure_move=0 WHERE id=1`); err != nil {
		return 0, 0, err
	}
	if err = pruneTx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("microstructure archive prune phase: %w", err)
	}
	frames, _ = fRes.RowsAffected()
	horizons, _ = hRes.RowsAffected()
	return frames, horizons, nil
}

func (s *Store) ResearchMicrostructureArchiveCounts(ctx context.Context) (frames, horizons int64, err error) {
	s.archMu.Lock()
	defer s.archMu.Unlock()
	if err = s.ensureArchiveLocked(ctx); err != nil {
		return 0, 0, err
	}
	if _, err = s.archConn.ExecContext(ctx, microstructureArchiveSchemaSQL); err != nil {
		return 0, 0, err
	}
	if err = s.archConn.QueryRowContext(ctx, `SELECT COUNT(*) FROM archive.research_microstructure_frames`).Scan(&frames); err != nil {
		return 0, 0, err
	}
	err = s.archConn.QueryRowContext(ctx, `SELECT COUNT(*) FROM archive.research_microstructure_horizons`).Scan(&horizons)
	return
}
