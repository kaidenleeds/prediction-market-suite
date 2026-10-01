package storage

// R124 PTT ARCHIVE-THEN-PRUNE (operator approved: "archive it and allow backtests to keep using
// it when suite is running"). poly_trader_trades + its 6 indexes measured 3.8GiB = 71% of the
// 5.37GB main DB (auditor r56 dbstat) — almost all of it RESOLVED research tape the live loops
// never read. Policy amendment (retention_test.go pins it): resolved ptt rows may only be MOVED
// to <dataDir>/kalshi_archive.db — NEVER deleted. Nothing is destroyed; the archive is a plain
// SQLite file any analysis copy can ATTACH next to a kalshi.db copy:
//
//     sqlite3 kalshi_copy.db "ATTACH 'kalshi_archive_copy.db' AS archive;
//                             SELECT ... FROM poly_trader_trades
//                             UNION ALL SELECT ... FROM archive.poly_trader_trades;"
//
// DESIGN
//   - The archive file carries the SAME ptt shape (schema.sql + the close_px migration) with
//     MINIMAL indexes: the dedup unique (idempotency), wallet (skill scans), condition_id
//     (backtest joins). The other main-side indexes exist for hot-path queries the archive
//     never serves.
//   - ATTACH is PER-CONNECTION in SQLite, and the pool holds 16 conns — so the archive lives on
//     ONE dedicated *sql.Conn held for the process lifetime, and every archive-touching call
//     (mover + full-history readers) serializes under archMu, consuming its rows fully before
//     unlocking (a *sql.Conn allows one active statement at a time).
//   - IDEMPOTENT, NOT ATOMIC: a transaction spanning two ATTACHed WAL files is atomic per file,
//     not across them. The mover therefore preserves main-side ids, INSERT OR IGNOREs into the
//     archive (its own (tx_hash,asset,side) unique dedup), and deletes from main ONLY rows whose
//     id verifiably landed in the archive. A crash between the two commits leaves duplicates
//     that the next batch re-converges (ignore-then-delete); rows can never be lost.
//   - Full-history READERS (BasketWallets / WalletSkillScores / ListPolyTraders — the wallet
//     skill training set spans all history) run UNION ALL across main+archive via
//     pttUnionRows. If the archive cannot be ensured they DEGRADE to main-only (hot window)
//     rather than fail — degradation is recorded once in ArchiveWarn for /api surfaces + logs.
//   - Hot-window readers stay on the pool untouched: NetBuyFlow (48h-bounded), the resolution
//     sweeps (resolved=0 only), MarkStaleConditionsUnresolvable (resolved=0), and the export
//     tab ListPolyTraderTrades (cap-8000 newest-first ≈ minutes of tape; full history is the
//     analysis-copy ATTACH pattern above).

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ArchiveFileName is the archive DB file, living next to kalshi.db in the data dir.
const ArchiveFileName = "kalshi_archive.db"

// ArchiveAfterDays: resolved (resolved=1) ptt rows whose trade ts is older than this move to
// the archive. 30d per the operator decision — comfortably wider than every hot-window reader
// (NetBuyFlow reads 48h; the sweeps read resolved=0 only).
const ArchiveAfterDays = 30

// pttCols — the full poly_trader_trades column list (schema.sql shape + the close_px migration
// storage.go adds at Open). Shared by the mover's INSERT..SELECT and the union readers so the
// two files can never drift silently: a missing column errors loudly at query time.
const pttCols = "id,wallet,ts,condition_id,asset,title,outcome,outcome_index,side,size,price,usdc_size,tx_hash,resolved,won,resolved_at,close_px"

// archivePTTWonSyncSQL reconciles a fresh venue grade only for the SAME bounded main-side
// candidate batch selected by ArchiveResolvedPTT. The former UPDATE was logically correct but
// scanned the entire archive and correlated every archive row back to main. On the production
// tape that held an attached-database transaction for minutes, pinning the main WAL snapshot and
// starving ordinary feed/collector writes. Driving the UPDATE FROM a materialized <=batch row CTE
// preserves the exact content checks while making the work proportional to one retention batch.
const archivePTTWonSyncSQL = `WITH archive_batch AS MATERIALIZED (
  SELECT id,wallet,ts,condition_id,asset,side,size,price,tx_hash,won
  FROM main.poly_trader_trades
  WHERE resolved=1 AND ts < ?1
  ORDER BY id LIMIT ?2
)
UPDATE archive.poly_trader_trades AS a SET won = (
  SELECT m.won FROM archive_batch AS m
  WHERE m.tx_hash = a.tx_hash AND m.asset = a.asset
    AND COALESCE(m.side,'') = COALESCE(a.side,'')
    AND m.wallet = a.wallet AND m.ts = a.ts
    AND m.condition_id = a.condition_id
    AND ABS(m.price - a.price) < 1e-6
    AND ABS(m.size  - a.size)  < 1e-6
  ORDER BY m.id DESC LIMIT 1
)
WHERE a.id IN (
  SELECT (
    SELECT hit.id FROM archive.poly_trader_trades AS hit INDEXED BY arc_ptt_dedup2
    WHERE hit.tx_hash = m.tx_hash AND hit.asset = m.asset
      AND COALESCE(hit.side,'') = COALESCE(m.side,'')
      AND hit.wallet = m.wallet AND hit.ts = m.ts
      AND hit.condition_id = m.condition_id
      AND ABS(hit.price - m.price) < 1e-6
      AND ABS(hit.size  - m.size)  < 1e-6
      AND COALESCE(hit.won,-9) <> COALESCE(m.won,-9)
    LIMIT 1
  )
  FROM archive_batch AS m
)`

// archiveSchemaSQL — same ptt shape, minimal indexes (see DESIGN). id is a plain PRIMARY KEY
// (no AUTOINCREMENT): values are always preserved from main, never generated here.
const archiveSchemaSQL = `
CREATE TABLE IF NOT EXISTS poly_trader_trades (
    id            INTEGER PRIMARY KEY,
    wallet        TEXT NOT NULL,
    ts            INTEGER NOT NULL,
    condition_id  TEXT NOT NULL,
    asset         TEXT NOT NULL,
    title         TEXT,
    outcome       TEXT,
    outcome_index INTEGER NOT NULL DEFAULT 0,
    side          TEXT,
    size          REAL NOT NULL DEFAULT 0,
    price         REAL NOT NULL DEFAULT 0,
    usdc_size     REAL NOT NULL DEFAULT 0,
    tx_hash       TEXT,
    resolved      INTEGER NOT NULL DEFAULT 0,
    won           INTEGER,
    resolved_at   TEXT,
    close_px      REAL NOT NULL DEFAULT -1
);
DROP INDEX IF EXISTS arc_ptt_dedup;
CREATE UNIQUE INDEX IF NOT EXISTS arc_ptt_dedup2 ON poly_trader_trades(tx_hash,asset,side,wallet,ts);
CREATE INDEX IF NOT EXISTS arc_ptt_wallet ON poly_trader_trades(wallet);
CREATE INDEX IF NOT EXISTS arc_ptt_cond   ON poly_trader_trades(condition_id);
`

// The duplicate-row inspection found four rows in two classes:
//   - arc_ptt_dedup was (tx_hash,asset,side) — UNDER-SPECIFIED: one blockchain transaction can
//     carry fills from DIFFERENT wallets (live case: two BUY fills of one asset in one tx,
//     wallets 0x7e97…/0x178d…, sizes 488k/18.6k). Both prints are REAL; the coarse unique key
//     made the second one unarchivable forever. arc_ptt_dedup2 adds wallet+ts — strictly finer,
//     so existing rows can't violate it; the DROP+CREATE pair is an idempotent migration.
//   - Float-precision drift: re-ingested prints carry the venue's full-precision price
//     (0.618931177942261) where the archived copy was rounded (0.6189311779) — same print, same
//     grade. The reconciliation below compares price/size with a 1e-6 tolerance instead of
//     bit-equality.
//   - won-DISAGREEMENT (the class R125 anticipated; none live today): the main-side row was
//     re-graded by the R125 two-pass venue client — the FRESH venue grade wins. The sync step
//     below copies main's won onto the archived print, then the row reconciles as a duplicate.
// A same-wallet same-second collision with a MATERIALLY different price/size remains a true
// anomaly: it stays in main and keeps wedging loudly (never auto-deleted).

// EnsureArchive creates/migrates the archive file and attaches it on the held connection.
// Idempotent and cheap once attached; safe to call from the retention monitor every pass.
func (s *Store) EnsureArchive(ctx context.Context) error {
	s.archMu.Lock()
	defer s.archMu.Unlock()
	return s.ensureArchiveLocked(ctx)
}

// ArchiveWarn returns the first archive-degradation error observed by a union reader ("" when
// healthy) — surfaced so a broken archive can never silently shrink the wallet-skill inputs.
func (s *Store) ArchiveWarn() string {
	s.archMu.Lock()
	defer s.archMu.Unlock()
	return s.archWarn
}

func (s *Store) ensureArchiveLocked(ctx context.Context) error {
	if s.archConn != nil {
		return nil
	}
	if s.archPath == "" {
		return fmt.Errorf("archive: store opened without a data dir")
	}
	// 1) Create/migrate the archive with a short-lived handle of its own. journal_mode=WAL is
	//    persistent in the file header, so later ATTACHed access inherits it.
	adb, err := sql.Open("sqlite", "file:"+s.archPath+
		"?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(35000)")
	if err != nil {
		return fmt.Errorf("archive open: %w", err)
	}
	_, mErr := adb.ExecContext(ctx, archiveSchemaSQL)
	if cErr := adb.Close(); mErr == nil {
		mErr = cErr
	}
	if mErr != nil {
		return fmt.Errorf("archive migrate: %w", mErr)
	}
	// 2) Hold ONE pool connection with the archive attached (ATTACH is per-connection; doing it
	//    pool-wide would need driver hooks). busy_timeout etc. ride the pool DSN.
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("archive conn: %w", err)
	}
	esc := strings.ReplaceAll(s.archPath, "'", "''")
	if _, err := conn.ExecContext(ctx, "ATTACH DATABASE '"+esc+"' AS archive"); err != nil {
		_ = conn.Close()
		return fmt.Errorf("archive attach: %w", err)
	}
	s.archConn = conn
	s.archWarn = ""
	return nil
}

// ArchiveResolvedPTT moves ONE bounded batch of resolved-and-aged ptt rows into the archive.
// Returns (moved, deduped): moved = rows deleted from main after verified archive presence;
// deduped = main-side rows removed because they were CONTENT-VERIFIED duplicates of prints
// already preserved in the archive (see the reconciliation note below). Callers loop until both
// are 0 (MonitorRetention pass / the archive-ptt subcommand). Never touches resolved=0
// (resolution sweep) or resolved=-1 (quarantine — PruneRetention's, deletable by the R70 policy).
func (s *Store) ArchiveResolvedPTT(ctx context.Context, olderThanDays, batch int) (int64, int64, error) {
	if olderThanDays <= 0 {
		olderThanDays = ArchiveAfterDays
	}
	if batch <= 0 {
		batch = retentionBatch
	}
	cut := time.Now().UTC().Add(-time.Duration(olderThanDays) * 24 * time.Hour).Unix()
	s.archMu.Lock()
	defer s.archMu.Unlock()
	if err := s.ensureArchiveLocked(ctx); err != nil {
		return 0, 0, err
	}
	// Two durable phases are deliberate. SQLite cannot atomically commit an ATTACHed pair when
	// either database uses WAL. Phase 1 writes/syncs the archive and commits it. Only then may
	// phase 2 delete verified copies from main. A crash between phases leaves a duplicate, which
	// the next pass safely reconciles; it can never leave a deletion without an archived copy.
	archiveTx, err := s.archConn.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = archiveTx.Rollback() }()
	// Candidate set: idx_ptt_res_ts-driven, ORDER BY id so both statements pick the same rows.
	insRes, err := archiveTx.ExecContext(ctx, `INSERT OR IGNORE INTO archive.poly_trader_trades (`+pttCols+`)
SELECT `+pttCols+` FROM main.poly_trader_trades
WHERE id IN (SELECT id FROM main.poly_trader_trades WHERE resolved=1 AND ts < ? ORDER BY id LIMIT ?)`, cut, batch)
	if err != nil {
		return 0, 0, fmt.Errorf("archive insert: %w", err)
	}
	// R124 DEDUP RECONCILIATION (measured live minutes after the bulk move): the whale puller
	// re-pulls wallet histories, so a print whose dedup key was freed by an earlier move can
	// RE-ENTER main under a NEW id, re-resolve, and collide with its archived copy — the plain
	// insert skips it (OR IGNORE) and the id-join delete can't touch it (ins=0, del=0 wedge;
	// live receipts: main ids 5,082,7xx vs archive ids 5001/6401, content-identical). Such a row
	// is a VERBATIM duplicate of a print already preserved in the archive, so removing the
	// main-side copy keeps the never-destroy policy at the PRINT level (the tape's unit).
	// Content-verified on every research-relevant column — a key collision with DIFFERENT
	// content stays put and keeps warning (true anomaly, surfaced every pass).
	// R128 won-SYNC (the R125-anticipated grade-disagreement class): when a re-entered print's
	// FRESH main-side grade disagrees with its archived copy (all identity fields equal, price/
	// size within tolerance), the venue re-query wins — copy main's won onto the archived print;
	// the row then reconciles as a duplicate below. See the class notes above archiveSchemaSQL.
	syncRes, err := archiveTx.ExecContext(ctx, archivePTTWonSyncSQL, cut, batch)
	if err != nil {
		return 0, 0, fmt.Errorf("archive won-sync: %w", err)
	}
	if err := archiveTx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("archive durable phase: %w", err)
	}

	pruneTx, err := s.archConn.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = pruneTx.Rollback() }()
	// Delete only rows whose byte-semantic archive copy was committed in phase 1. Presence of the
	// same integer id is not enough: INSERT OR IGNORE can also skip on a pre-existing archive PK.
	// A corrupt/manually-restored archive row with that id must wedge visibly, never authorize the
	// main-side delete. INSERT..SELECT preserves the REAL values exactly; small tolerances only
	// protect SQLite numeric representation at this verification boundary.
	delRes, err := pruneTx.ExecContext(ctx, `DELETE FROM main.poly_trader_trades AS m
WHERE m.id IN (SELECT id FROM main.poly_trader_trades WHERE resolved=1 AND ts < ? ORDER BY id LIMIT ?)
  AND EXISTS (SELECT 1 FROM archive.poly_trader_trades a
              WHERE a.id = m.id
                AND a.wallet = m.wallet
                AND a.ts = m.ts
                AND a.condition_id = m.condition_id
                AND a.asset = m.asset
                AND COALESCE(a.title,'') = COALESCE(m.title,'')
                AND COALESCE(a.outcome,'') = COALESCE(m.outcome,'')
                AND a.outcome_index = m.outcome_index
                AND COALESCE(a.side,'') = COALESCE(m.side,'')
                AND ABS(a.size - m.size) < 1e-9
                AND ABS(a.price - m.price) < 1e-9
                AND ABS(a.usdc_size - m.usdc_size) < 1e-9
                AND COALESCE(a.tx_hash,'') = COALESCE(m.tx_hash,'')
                AND a.resolved = m.resolved
                AND COALESCE(a.won,-9) = COALESCE(m.won,-9)
                AND COALESCE(a.resolved_at,'') = COALESCE(m.resolved_at,'')
                AND ABS(a.close_px - m.close_px) < 1e-9)`, cut, batch)
	if err != nil {
		return 0, 0, fmt.Errorf("archive delete: %w", err)
	}
	// R128: price/size compare with a 1e-6 tolerance (float-precision drift class — same print,
	// different decimal representation after a re-pull); won must MATCH (post-sync).
	dupRes, err := pruneTx.ExecContext(ctx, `DELETE FROM main.poly_trader_trades AS m
WHERE m.id IN (SELECT id FROM main.poly_trader_trades WHERE resolved=1 AND ts < ? ORDER BY id LIMIT ?)
  AND m.id NOT IN (SELECT id FROM archive.poly_trader_trades)
  AND EXISTS (SELECT 1 FROM archive.poly_trader_trades a
              WHERE a.tx_hash = m.tx_hash AND a.asset = m.asset
                AND COALESCE(a.side,'') = COALESCE(m.side,'')
                AND a.wallet = m.wallet AND a.ts = m.ts
                AND a.condition_id = m.condition_id
                AND ABS(a.price - m.price) < 1e-6 AND ABS(a.size - m.size) < 1e-6
                AND COALESCE(a.won,-9) = COALESCE(m.won,-9))`, cut, batch)
	if err != nil {
		return 0, 0, fmt.Errorf("archive dedup: %w", err)
	}
	ins, _ := insRes.RowsAffected()
	del, _ := delRes.RowsAffected()
	dup, _ := dupRes.RowsAffected()
	sync, _ := syncRes.RowsAffected()
	if ins == 0 && del == 0 && dup == 0 && sync == 0 {
		// Either drained, or a genuinely wedged row (same dedup key as an archived print but
		// DIFFERENT content). Distinguish so a wedge can never spin silently forever.
		var left int
		if err := pruneTx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM (SELECT id FROM main.poly_trader_trades WHERE resolved=1 AND ts < ? LIMIT 1)`,
			cut).Scan(&left); err != nil {
			return 0, 0, err
		}
		if left > 0 {
			return 0, 0, fmt.Errorf("archive move wedged: a resolved aged row collides with an archived print but its CONTENT differs — inspect it (never auto-deleted)")
		}
	}
	if err := pruneTx.Commit(); err != nil {
		return 0, 0, err
	}
	return del, dup, nil
}

// ArchivePTTCount returns the archive's row count (0 with no error when the archive is healthy
// and empty). Monitor line + the retention test's moved-count == archive-count invariant.
func (s *Store) ArchivePTTCount(ctx context.Context) (int64, error) {
	s.archMu.Lock()
	defer s.archMu.Unlock()
	if err := s.ensureArchiveLocked(ctx); err != nil {
		return 0, err
	}
	var n int64
	err := s.archConn.QueryRowContext(ctx, `SELECT COUNT(*) FROM archive.poly_trader_trades`).Scan(&n)
	return n, err
}

// pttUnionRows runs a full-history ptt query. unionSQL sees main.poly_trader_trades AND
// archive.poly_trader_trades on the held connection; when the archive cannot be ensured the
// reader degrades to mainSQL on the pool (hot window only) and the degradation is recorded.
// The returned cleanup MUST run after rows are fully consumed (it releases archMu when the
// union path was taken — the held conn allows one active statement).
func (s *Store) pttUnionRows(ctx context.Context, unionSQL, mainSQL string, args ...any) (*sql.Rows, func(), error) {
	s.archMu.Lock()
	if err := s.ensureArchiveLocked(ctx); err != nil {
		if s.archWarn == "" {
			s.archWarn = err.Error()
		}
		s.archMu.Unlock()
		rows, qerr := s.db.QueryContext(ctx, mainSQL, args...)
		return rows, func() {}, qerr
	}
	rows, err := s.archConn.QueryContext(ctx, unionSQL, args...)
	if err != nil {
		s.archMu.Unlock()
		return nil, func() {}, err
	}
	return rows, func() { s.archMu.Unlock() }, nil
}
