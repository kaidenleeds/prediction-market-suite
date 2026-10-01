// Package storage is the SQLite persistence layer (pure-Go driver, WAL mode).
package storage

import (
	"context"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/polyid"

	_ "modernc.org/sqlite"
)

//go:embed schema.sql
var schemaSQL string

// Credential is the encrypted-at-rest Kalshi API credential for one environment.
type Credential struct {
	Environment string
	KeyID       string
	Salt        []byte
	Nonce       []byte
	Ciphertext  []byte
}

type Store struct {
	db *sql.DB
	// executionShadowDB is deliberately a different SQLite file and connection pool from db.
	// Unified comparison telemetry is observational: it must never take, wait on, or lengthen
	// the cash ledger's single-writer lock.
	executionShadowDB   *sql.DB
	executionShadowPath string
	executionShadowMu   sync.Mutex
	executionShadowRun  string
	// R144's one-time research collection counter migration may retry outside the 15-second
	// collector-registration context after transient DB contention. Close cancels and joins this
	// worker before closing SQLite so a shutdown can never race a background transaction.
	r144CollectionBootstrapMu      sync.Mutex
	r144CollectionBootstrapRunning bool
	r144CollectionBootstrapCancel  context.CancelFunc
	r144CollectionBootstrapWG      sync.WaitGroup
	// collectorRuntimeStart is process-local liveness truth. Collector receipts intentionally
	// survive restarts, but a pre-boot success must not masquerade as proof that the current
	// process completed its collector. It is set by server.New and never persisted.
	collectorRuntimeMu    sync.RWMutex
	collectorRuntimeStart time.Time
	// IndexWarnings — R101 (auditor r28 bug 236): failures from best-effort perf-index/DDL
	// statements in Open, recorded instead of discarded (`_, _ = db.Exec`). A failed
	// CREATE INDEX used to boot the suite into silent full-scan mode (the pre-R76 wedge
	// class). main WARN-logs these right after Open; expected ALTER "duplicate column"
	// noise is not collected.
	IndexWarnings []string

	// R124 ptt archive (archive.go): the aged-resolved research tape lives in
	// <dataDir>/kalshi_archive.db, ATTACHed on ONE held connection — every archive-touching
	// call serializes under archMu and fully consumes its rows before unlocking.
	archPath string
	archMu   sync.Mutex
	archConn *sql.Conn
	archWarn string // first union-reader degradation ("" = healthy) — see ArchiveWarn
}

// Open opens (creating if needed) the SQLite database at <dataDir>/kalshi.db and
// runs migrations. WAL mode is enabled for concurrent reads while writing.
func Open(dataDir string) (*Store, error) {
	path := filepath.Join(dataDir, "kalshi.db")
	// PERF: pragmas live in the DSN so they apply to EVERY pooled connection (a PRAGMA run via Exec only
	// affects the one connection that ran it). mmap_size maps the whole DB into memory → reads are
	// memory-speed with no read() syscalls. Keep each connection cache bounded at 64MB and spill
	// large analytic sorts to the temp file: 16 private 256MB caches plus MEMORY temp tables caused
	// a measured 4.4–5.3GB cold-start working set before Go allocations were counted.
	// R75 SQLITE_BUSY ROOT CAUSE (live log: every insertSignal + market_catalog upsert failing
	// "database is locked (5)" — the catalog table was 100% EMPTY on a 25-min-old session):
	// BeginTx with the default DEFERRED txlock starts as a reader and UPGRADES to writer at the
	// first INSERT — and SQLite returns SQLITE_BUSY IMMEDIATELY on a contended upgrade (deadlock
	// avoidance): busy_timeout NEVER APPLIES to that path. _txlock=immediate makes every
	// transaction take the write lock at BEGIN, where busy_timeout DOES apply — writers now WAIT
	// behind the ML sidecar's Python writes instead of erroring. Plain autocommit
	// Exec/Query paths are unaffected.
	// R84 (auditor bug 26): 10s → 35s. The sidecar's heavy training transactions were measured
	// holding the write lock >10s; at 10s the suite's writers (insertSignal, catalog upserts,
	// latches) erred "database is locked (5)" right through the sidecar's longest bursts. 35s
	// outwaits the observed worst case; per-call context deadlines still bound total latency.
	// R98 (auditor r23 #1 / bug 157): journal_size_limit makes SQLite itself truncate the WAL back
	// to ≤256MB whenever a checkpoint empties it — the belt under the R98 TRUNCATE sweep, so a
	// missed sweep window can never re-grow a permanent multi-GB file (R97 found 1,297MB).
	// R140 final measured bound: 256MB mmap + eight 32MB page caches. This supersedes the
	// historical cache figures above and keeps complete venue books responsive without the prior
	// multi-gigabyte resident-set surge.
	dsn := "file:" + path + "?_txlock=immediate&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(35000)" +
		"&_pragma=foreign_keys(ON)&_pragma=mmap_size(268435456)&_pragma=cache_size(-32768)&_pragma=temp_store(FILE)" +
		"&_pragma=journal_size_limit(268435456)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// CONCURRENCY FIX (the UI freeze): WAL lets many readers run CONCURRENTLY with a single writer, so a
	// slow Stats/Edge scan no longer blocks the portfolio, the refresh poll, and the auto-loop behind one
	// connection. Writes still serialize at the SQLite level — busy_timeout(35s, R84) makes a competing
	// write WAIT rather than error. (Was SetMaxOpenConns(1), which serialized everything → the freeze.)
	db.SetMaxOpenConns(8) // bounded WAL concurrency; heavy analytics are staged instead of multiplying private page caches
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(time.Hour)
	// R76 WEDGE FIX (auditor bug 1) — probe BEFORE the schema runs: was idx_ptt_cond already there?
	// schema.sql now creates it (see the note there); if THIS boot is the one that builds it on a
	// populated DB (~2.5M rows, a few seconds), we must also ANALYZE the table below or the planner
	// keeps ignoring the new index and the retention orphan probe stays quadratic under the write lock.
	var hadPttCond int
	_ = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_ptt_cond'`).Scan(&hadPttCond)
	if err := preMigrateR139InferenceTables(db); err != nil {
		return nil, fmt.Errorf("pre-migrate R139 inference: %w", err)
	}
	if err := preMigrateR175ResearchReportRollups(db); err != nil {
		return nil, fmt.Errorf("pre-migrate R175 research report rollups: %w", err)
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	if err := ensureR175ResearchReportRollups(context.Background(), db); err != nil {
		return nil, fmt.Errorf("backfill R175 research report rollups: %w", err)
	}
	if err := migrateResearchGovernanceAuthority(db); err != nil {
		return nil, fmt.Errorf("migrate research governance authority: %w", err)
	}
	if err := migrateProperScoreV2(db); err != nil {
		return nil, fmt.Errorf("migrate proper-score v2: %w", err)
	}
	if err := migrateResearchSystemObservationIdentity(db); err != nil {
		return nil, fmt.Errorf("migrate research observation identity: %w", err)
	}
	if err := migrateGeneralRulePairSchema(db); err != nil {
		return nil, fmt.Errorf("migrate generalized rule-pair identity: %w", err)
	}
	if err := migrateConcreteResearchSchema(db); err != nil {
		return nil, fmt.Errorf("migrate concrete research inputs: %w", err)
	}
	if err := migrateConcreteCompletionSchema(db); err != nil {
		return nil, fmt.Errorf("migrate concrete timed/prior evidence: %w", err)
	}
	if err := migrateResearchPromotionSchema(db); err != nil {
		return nil, fmt.Errorf("migrate research promotion bridge: %w", err)
	}
	if err := migrateInversePaperPlacementSchema(db); err != nil {
		return nil, fmt.Errorf("migrate inverse Paper placement journal: %w", err)
	}
	if err := migrateProperMomentumPromotionSchema(db); err != nil {
		return nil, fmt.Errorf("migrate proper-score momentum SELL bridge: %w", err)
	}
	if err := migrateFrozenSelectorSchema(db); err != nil {
		return nil, fmt.Errorf("migrate frozen research selectors: %w", err)
	}
	if err := migrateResearchBundleSchema(db); err != nil {
		return nil, fmt.Errorf("migrate typed research route bundles: %w", err)
	}
	if err := migrateLivePendingRiskSchema(db); err != nil {
		return nil, fmt.Errorf("migrate live pending-risk reservations: %w", err)
	}
	if err := migrateLiveDispatchShadowSchema(db); err != nil {
		return nil, fmt.Errorf("migrate live-dispatch shadow ledger: %w", err)
	}
	if err := migrateLivePolicyMirrorSchema(db); err != nil {
		return nil, fmt.Errorf("migrate independent LIVE-policy mirror ledger: %w", err)
	}
	if err := migrateExecutionShadowSchema(db); err != nil {
		return nil, fmt.Errorf("migrate unified execution-shadow ledger: %w", err)
	}
	if err := migrateStep7EvidenceSchema(db); err != nil {
		return nil, fmt.Errorf("migrate Step-7 event evidence: %w", err)
	}
	if err := migrateUnitTrialOrigin(db); err != nil {
		return nil, fmt.Errorf("migrate unit-trial origin: %w", err)
	}
	if err := migrateUnitTrialEventIdentity(db); err != nil {
		return nil, fmt.Errorf("migrate unit-trial event identity: %w", err)
	}
	if err := migrateFundedRelationSchema(db); err != nil {
		return nil, fmt.Errorf("migrate funded relation receipts: %w", err)
	}
	var idxWarns []string // R101 (auditor r28 bug 236): best-effort DDL failures surface via Store.IndexWarnings
	// Best-effort columns for upgrading a pre-TP/SL paper_fills table (errors—e.g.
	// "duplicate column"—are expected and ignored). sig_px_at_fill: R90 DO-THIS 5ii (edge 29) —
	// the signal-side live price at fill time, so slip-vs-curse decomposes per lot.
	for _, col := range []string{"tp_price REAL", "sl_price REAL", "sig_px_at_fill REAL NOT NULL DEFAULT 0"} {
		_, _ = db.Exec("ALTER TABLE paper_fills ADD COLUMN " + col)
	}
	// R90 DO-THIS 5iii/14 (edge 30): would_gate on maker posts — 1 = the adverse-posting guard
	// would have skipped this post (depth-0 book or ~5m momentum against). Stamped on EVERY post
	// so the guard's counterfactual is measurable whether or not the guard is armed.
	_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN would_gate INTEGER NOT NULL DEFAULT 0")
	// R101 (auditor r27/r28 DO-THIS 1 — the gate now controls ~90% of post-attempts with its
	// inputs invisible): stamp the adverse-guard's DECISION INPUTS on every mfs row, pass AND
	// block, so the next audit can measure whether it over-blocks (and which trigger dominates).
	//   mom5m       — the ~5m YES-price momentum (cents) the guard read; NULL = ring unobservable
	//   mom_gate_c  — the momentum threshold in force (¢ against the posted side; 1.0 today)
	//   gate_reason — ''|'depth0'|'momentum'|'depth0+momentum' (why would_gate=1; '' on pass)
	//   depth_src   — where book_depth came from: kalshi bookws|kob|none · polyus book3|none
	//                 ('none' = we could not OBSERVE a book — the dominant depth-0 subcase)
	for _, col := range []string{"mom5m REAL", "mom_gate_c REAL NOT NULL DEFAULT 0",
		"gate_reason TEXT NOT NULL DEFAULT ''", "depth_src TEXT NOT NULL DEFAULT ''"} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	// R106 (paper-maker live parity): which cancel rule ended a simulated resting post —
	// 'moved-1c' (the live 1¢ layers' analog) | 'close-3min' (R24b pull) | 'expire-10m'.
	// '' = filled or pre-R106 row. Lets the auditor grade sim cancels against live behavior.
	_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN cancel_rule TEXT NOT NULL DEFAULT ''")
	// R106 (auditor r33 DO-THIS 6, edges 30/35): settlement mirror on maker rows — grades the
	// would_gate counterfactual (the gated -2 pile was 1,080 rows and ungradable). NULL = ungraded;
	// outcome_won uses the posted side vs the settled outcome (ties = loss). ResolveSignals writes
	// both in the same settle pass.
	for _, col := range []string{"settle_val REAL", "outcome_won INTEGER", "settle_mirrored_at TEXT"} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	// R107 (operator: maker sim on every paper book — "cost of patience" telemetry): after a
	// simulated resting post CANCELS, a 30-min watch keeps checking whether the market would
	// have filled it anyway. late_fill: NULL = never watched (filled rows / pre-R107), 1 = the
	// price DID trade through the canceled level within the watch (late_fill_s = seconds from
	// cancel to that cross), 0 = watch expired without a cross. Measures what the R106 cancel
	// parity rules cost in foregone fills.
	for _, col := range []string{"late_fill INTEGER", "late_fill_s INTEGER"} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	// R115: (a) queue-position model tags — queue_ahead = visible size at our level at POST (we
	// join the back), queue_left = queue still ahead at fill/cancel, fill_ct = contracts the tape
	// filled (partials allowed); NULL = queue unobservable (legacy touch-through rule applied).
	// (b) fill-time re-check — fill_px = market price at the fill moment, dec_pwin/fill_pwin = the
	// sidecar's p_win at decision vs fill time (drift = fill_pwin − dec_pwin), fill_rule = how the
	// fill happened (touch-through | queue-through | queue-partial:<cancel rule>).
	for _, col := range []string{"queue_ahead REAL", "queue_left REAL", "fill_ct REAL",
		"fill_px REAL", "dec_pwin REAL", "fill_pwin REAL", "fill_rule TEXT NOT NULL DEFAULT ''"} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	// R122 queue-aware router (router.go): the routing decision + its estimate inputs ride every
	// maker attempt (route_reason '' = pre-R122 row or router off), and every paper fill carries
	// fill_kind (maker|taker) + route_reason so maker-vs-taker grading no longer parses note text.
	for _, col := range []string{"route_reason TEXT NOT NULL DEFAULT ''", "route_queue REAL",
		"route_rate_pm REAL", "route_eta_s REAL", "route_horizon_s REAL"} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	// R133 route-specific LIVE proof: source alone cannot distinguish direct from inverted
	// execution (kthresh is currently inverted, and automatic policy inversion can change over
	// time). New attempts therefore carry their canonical executable family plus the actual
	// inversion bit. Legacy blank rows remain research evidence only and can never authorize LIVE.
	for _, col := range []string{"strategy_family TEXT NOT NULL DEFAULT ''", "strategy_inverted INTEGER NOT NULL DEFAULT 0"} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	// R135c Systems regimes: historical maker P&L must use the fee/rebate authority captured at
	// fill time, never today's mutable schedule. NULL fee/rebate + empty source means legacy/no
	// receipt and is excluded from economic proof (canceled attempts remain exact $0 outcomes).
	for _, col := range []string{"maker_fee_pc REAL", "maker_rebate_pc REAL", "maker_fee_source TEXT NOT NULL DEFAULT ''"} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	for _, col := range []string{"fill_kind TEXT NOT NULL DEFAULT ''", "route_reason TEXT NOT NULL DEFAULT ''"} {
		_, _ = db.Exec("ALTER TABLE paper_fills ADD COLUMN " + col)
	}
	// PARLAY: settlement columns on paper_parlays (payout 0..1 = product of leg outcomes; realized $ +
	// settle timestamp). fees = rolled-stake entry fees booked at placement (audit Q4: P&L was gross).
	// artifact = pre-correlation-guard churn rows (audit F4). Best-effort (duplicate-column ignored).
	for _, col := range []string{"payout REAL NOT NULL DEFAULT 0", "realized REAL NOT NULL DEFAULT 0", "settled_ts TEXT NOT NULL DEFAULT ''",
		"fees REAL NOT NULL DEFAULT 0", "artifact INTEGER NOT NULL DEFAULT 0",
		"route_source TEXT NOT NULL DEFAULT ''", "cohort TEXT NOT NULL DEFAULT ''", "system_ids TEXT NOT NULL DEFAULT '[]'",
		"joint_p REAL NOT NULL DEFAULT 0", "expected_net_per_dollar REAL NOT NULL DEFAULT 0",
		"canonical_system_id TEXT NOT NULL DEFAULT ''", "combo_venue TEXT NOT NULL DEFAULT ''",
		"leg_count INTEGER NOT NULL DEFAULT 0", "relation_class TEXT NOT NULL DEFAULT ''",
		"producer_family TEXT NOT NULL DEFAULT ''", "combo_route TEXT NOT NULL DEFAULT ''",
		"experiment_epoch TEXT NOT NULL DEFAULT ''", "combo_key TEXT NOT NULL DEFAULT ''"} {
		_, _ = db.Exec("ALTER TABLE paper_parlays ADD COLUMN " + col)
	}
	if err := backfillPaperComboAttribution(db); err != nil {
		return nil, fmt.Errorf("backfill exact combo attribution: %w", err)
	}
	// R146 New-ML Combo Paper shares paper_parlays with the System Combo book but owns a
	// separate route, cohort and reset epoch. Build this index only AFTER the additive ALTERs:
	// creating it in schema.sql would brick a legacy database whose table predates those columns.
	const parlayRouteCohortIndex = "CREATE INDEX IF NOT EXISTS idx_paper_parlays_route_cohort_status_id ON paper_parlays(route_source, cohort, status, id)"
	if _, err := db.Exec(parlayRouteCohortIndex); err != nil {
		idxWarns = append(idxWarns, fmt.Sprintf("index DDL failed (ML Combo full-scan risk): %s: %v", parlayRouteCohortIndex, err))
	}
	// Best-effort feature columns for upgrading a pre-feature signal_log (Phase 1a): widen the
	// log so every observation carries the features (strength/notional/rank/concentration/…) we
	// need to FIND the edge, not just guess thresholds. Duplicate-column errors are ignored.
	for _, col := range []string{
		"slot TEXT NOT NULL DEFAULT ''",
		"strength REAL NOT NULL DEFAULT 0",
		"notional REAL NOT NULL DEFAULT 0",
		"best_rank INTEGER NOT NULL DEFAULT 0",
		"trader_count INTEGER NOT NULL DEFAULT 0",
		"trader_pnl REAL NOT NULL DEFAULT 0",
		"concentration REAL NOT NULL DEFAULT 0",
		"momentum REAL NOT NULL DEFAULT 0",
		"spread_cents REAL NOT NULL DEFAULT 0",
		"resolve_hours REAL NOT NULL DEFAULT 0",
		"imbalance REAL NOT NULL DEFAULT 0",
		"underlying REAL NOT NULL DEFAULT 0",
		"price_path TEXT NOT NULL DEFAULT ''",
		"category TEXT NOT NULL DEFAULT ''",
		"confidence REAL NOT NULL DEFAULT 0",
		// Phase 5b honest-labels: book depth at signal time (maker-fill odds), the ACTUAL fill price once
		// the signal becomes a bet (so ML trains on real fills, not signal price), and the post-entry price
		// path (for SL/TP calibration). fill_price/post_path are written by updates, not at insert.
		"book_depth REAL NOT NULL DEFAULT 0",
		"fill_price REAL NOT NULL DEFAULT 0",
		"post_path TEXT NOT NULL DEFAULT ''",
		// SCALARPAY: the authoritative settled YES value (0..1) recorded at resolution. -1 = unresolved/
		// unknown (binary fallback). Lets the ML book pay the EXACT scalar payout, not a binary win/loss.
		"settle_val REAL NOT NULL DEFAULT -1",
		// WHALE V2 (audit Q2): the acting wallet's MEASURED market-level shrunk skill at signal time —
		// the ML feature that replaces raw leaderboard profit (capital-confounded, plausibly sign-wrong).
		"trader_skill REAL NOT NULL DEFAULT 0",
		// TYPE LOGGING (audit Q3 — "the 15-line unlock"): the granular market TYPE (Moneyline/Spread/
		// Total 2.5/Crypto Threshold/…) that was previously display-only. The prerequisite for every
		// per-type decision on the venue that loses money.
		"market_type TEXT NOT NULL DEFAULT ''",
		// R38 LIVE-VS-PRE (operator): is the market IN-PLAY at signal time? 1 = live (game running /
		// price actively moving), 0 = pre/stable, -1 = unknown (legacy rows). PolyUS has a venue
		// flag; Kalshi is inferred from WS tick recency. ML feature + Edge bin dimension.
		"is_live INTEGER NOT NULL DEFAULT -1",
		// R65 FEATURE FAMILIES (operator): book shape near the touch, multi-horizon momentum +
		// ring-scope extreme distances, and time-to-event-start. NULLABLE by design — pre-R65 rows
		// stay NULL and readers COALESCE to documented sentinels (live_ml.py `_LEAN_COLS`); the
		// trees learn these features as labeled rows accrue.
		"book_imb3 REAL",        // top-3-level bid share of (bid+ask) top-3 depth, 0..1; NULL = book not cached at signal time
		"book_depth3 REAL",      // summed top-3 resting depth, both sides (contracts); NULL = book not cached
		"mom_1h REAL",           // YES-price change over ~1h, in cents (price ring); NULL = ring too young/absent
		"mom_4h REAL",           // YES-price change over ~4h, in cents; NULL = ring too young/absent
		"dist_hi REAL",          // cents BELOW the ring-scope session high (0 = at the high); NULL = no ring
		"dist_lo REAL",          // cents ABOVE the ring-scope session low (0 = at the low); NULL = no ring
		"secs_to_start INTEGER", // SIGNED seconds until event start (NEGATIVE once started); NULL = start unknown
		// R67l TAKER-FLOW RATIO family — per-ticker aggressive (taker) money over the last 15 min,
		// from the trade tapes the suite already ingests (Kalshi trades WS · PolyUS WS tape ·
		// poly-int on-chain trades). NULLABLE like every R65 family (sentinels in _LEAN_COLS).
		"flow_ratio_15m REAL", // taker BUY-YES share of taker notional, 0..1 (0.5 = balanced); NULL = no tape/no trades
		"flow_n_15m INTEGER",  // taker trade count behind the ratio; NULL = no tape
		// R70-B SCHEMA_AUDIT features — same NULLABLE convention (sentinels in _LEAN_COLS):
		"holders_hhi REAL",   // #4 poly-int: Herfindahl index over the top-20 holders' token amounts, 0..1 (1 = one whale holds it all); NULL = holders cache cold
		"holders_skill REAL", // #4 poly-int: Σ share×shrunk-skill×side over top holders, YES-space (>0 = skilled money leans YES); NULL = cache cold
		"in_play INTEGER",    // #8 polyus: 1 = venue reports the game LIVE with state, 0 = known pre-game; NULL = game state unknown
		"score_margin REAL",  // #8 polyus: |scoreA−scoreB| lead size when in-play with a parseable score; NULL = unknown
		"oi_delta_1h REAL",   // #7 kalshi: open-interest change over ~1h (contracts, from the ticker-WS OI ring); NULL = ring cold
		// R72-B SUB-MARKETS: coarse market KIND (winner/total/spread/prop/unknown), stamped at
		// insert from the R68 classifier family (comboType/mktKind/matchTypeOf). market_type above
		// stays the GRANULAR label ("Total 2.5"); kind is the stable 5-value vocab the sidecar
		// one-hots (added to live_ml.py CATS) and per-kind analysis groups by.
		"kind TEXT NOT NULL DEFAULT ''",
		// R84 PER-ROW MODEL-PROB (auditor DO-THIS 8, nagged 13 runs): the sidecar's CURRENT p_win
		// for (ticker, side) at signal-insert time, from the ml_predictions.json cache — NULL when
		// the model hadn't scored the market. AUDIT/CALIBRATION column ONLY: deliberately NOT in
		// live_ml.py's feature SELECT (_LEAN_COLS) — feeding the model its own output back as a
		// training feature would be circular.
		"model_prob REAL",
		// R132 exact per-contract fee at observation time (active venue/series schedule).
		"fee_pc REAL",
		// R114 DECISION-PRICE AGE (ML review add — log now, train as it accrues): seconds since the
		// suite last refreshed the market snapshot the signal priced off (kalshi kmkts cache age).
		// Stale decision prices are a plausible loss driver the model can't currently see. NULL =
		// unmeasured (non-kalshi or cache miss); sidecar sentinel -1.
		"px_age_s REAL",
		// R133 BOOK-NATIVE ML COHORT: additive + versioned. Never backfill these from
		// entry_price/spread; version 0 is the durable legacy marker.
		"book_feature_ver INTEGER NOT NULL DEFAULT 0",
		"book_bid REAL",
		"book_ask REAL",
		"book_bid_depth REAL",
		"book_ask_depth REAL",
		"book_quote_age_s REAL",
		"book_maker_tick REAL",
		"book_taker_tick REAL",
		"book_maker_fee_pc REAL",
		"book_taker_fee_pc REAL",
		"book_latency_ms REAL",
		"book_source TEXT NOT NULL DEFAULT ''",
		// R138 ML integrity: row-level cohort provenance. Empty is intentionally legacy/untrusted;
		// only the central signal path stamps the current label and executable-pricing contracts.
		"label_version TEXT NOT NULL DEFAULT ''",
		"pricing_version TEXT NOT NULL DEFAULT ''",
		// R126 (both-side gap scanning + episode re-entry): gap-family tags. episode = the
		// per-(pair,direction) gap-episode ordinal at signal time (0 = non-gap family / untracked);
		// exec_expr = the execution-expression tag ("pus_yes:best", "k_no:alt", … — Part 1's
		// dedup-honest cohort split rides here so per-expression grading needs no Title parsing).
		"episode INTEGER NOT NULL DEFAULT 0",
		"exec_expr TEXT NOT NULL DEFAULT ''",
		// R166: exact detector topology is durable, not only embedded in an audit string. Distinct
		// K-PUS/K-PINT/etc. observations may share ticker/side/family within one slot.
		"input_topology TEXT NOT NULL DEFAULT ''",
	} {
		_, _ = db.Exec("ALTER TABLE signal_log ADD COLUMN " + col)
	}
	// PERF indexes for the hot aggregation/recent queries (Stats / Edge / DECIDES verdicts / export caps).
	// IF NOT EXISTS → built once (a few seconds on the existing 428k rows), instant on every later start.
	// Without these those reads full-scanned the whole signal_log, which is a big part of the UI freeze.
	for _, ix := range []string{
		"CREATE INDEX IF NOT EXISTS idx_signal_resolved_type ON signal_log(resolved, signal_type)",
		"CREATE INDEX IF NOT EXISTS idx_signal_ts ON signal_log(ts)",
		// P3 (audit §3 "DB indexing"): the remaining hot scan shapes. resolved+platform drives the
		// settlement sweeps + venue Stats; the partial index is EXACTLY the post_path backfill
		// queue (ResolvedNeedPath / AppendSignalPostPath candidates) so those sweeps stop walking
		// megabytes of already-pathed rows. Partial indexes keep them tiny (~the open/needy sets).
		"CREATE INDEX IF NOT EXISTS idx_signal_resolved_platform ON signal_log(resolved, platform)",
		"CREATE INDEX IF NOT EXISTS idx_signal_book_v1 ON signal_log(resolved,id) WHERE book_feature_ver = 1",
		"CREATE INDEX IF NOT EXISTS idx_signal_book_native_v2 ON signal_log(resolved,id) WHERE book_feature_ver = 1 AND pricing_version = 'book-native-v2' AND label_version = 'kalshi_start_clock_v2'",
		"CREATE INDEX IF NOT EXISTS idx_signal_postpath_need ON signal_log(platform, resolved) WHERE fill_price > 0 AND post_path = ''",
		// R133 Adaptive Allocation Model: the stop lab / self-tuner must select eligible path rows directly.  The old
		// ListSignals(8000)-then-filter shape could return 8,000 newer unresolved rows and zero
		// usable paths even when older evidence existed.  This partial index keeps the bounded
		// newest-eligible query off the million-row signal-log scan.
		"CREATE INDEX IF NOT EXISTS idx_signal_sltp_eligible ON signal_log(id DESC) WHERE resolved = 1 AND fill_price > 0 AND post_path != '' AND instr(post_path, ',') > 0",
		// R98 (operator: "signal famine query failed: context deadline exceeded" — fix the disease):
		// the three request/sweep-path shapes that still walked the whole 922k-row table.
		// (signal_type, ts): SignalFamine per-family MAX(ts) point seeks (was a 24h range walk +
		// temp b-tree, 709ms cold on a quiet host copy → >5s live under boot-storm + WAL lookup
		// load) + narrows ResolvedSignalEcon's family windows. Build cost ≈1.1s once.
		"CREATE INDEX IF NOT EXISTS idx_signal_type_ts ON signal_log(signal_type, ts)",
		// Partial open-rows index: the settlement sweep's 3× per-pass ListOpenSignalTickersByPlatform
		// (was 176ms warm/2.4s cold per call over 327k open rows; now covering, 4ms) + ListOpenSignalTickers.
		"CREATE INDEX IF NOT EXISTS idx_signal_open_plat_ticker ON signal_log(platform, ticker, ts) WHERE resolved=0",
		// Covering (platform, signal_type, resolved): SignalLogCountsByVenue streams the GROUP BY
		// index-only (was a 2.1-3.5s full-table scan on EVERY /api/stats poll — the R73 comment
		// claimed "ms on the aggregation indexes" but no such index ever existed).
		"CREATE INDEX IF NOT EXISTS idx_signal_venue_type_res ON signal_log(platform, signal_type, resolved)",
		// R98 (auditor r23 #1): the sidecar's EVERY-10s incremental pass filters
		// `resolved != 0 AND resolved_at >= <2min ago>` — resolved_at makes that a tiny range
		// read (rows lacking resolved_at simply aren't in the index range) instead of a scan.
		"CREATE INDEX IF NOT EXISTS idx_signal_resolved_at ON signal_log(resolved_at)",
		// R143 terminal-reconciliation lookups are venue+ticker exact.  The former planner chose
		// (resolved,platform), then walked hundreds of thousands of settled rows once per research
		// candidate.  This partial covering shape reduces each lookup to one market's receipts.
		"CREATE INDEX IF NOT EXISTS idx_signal_exact_settlement ON signal_log(platform,ticker,resolved,resolved_at DESC,id DESC) WHERE resolved=1 AND settle_val>=0 AND settle_val<=1",
		"CREATE INDEX IF NOT EXISTS idx_signal_series_roll ON signal_log(platform,ticker,id DESC) WHERE resolved=1 AND settle_val>=0 AND book_feature_ver=1 AND pricing_version='book-native-v2' AND book_ask>0 AND book_taker_fee_pc IS NOT NULL",
		"CREATE INDEX IF NOT EXISTS idx_rseries_series_event ON research_series_event_map(series_ticker,event_ticker)",
		// R101 (auditor r28 bug 233): maker_fill_stats retention prunes by ts (PruneRetention's
		// 90d rule) and the R101 gate-input audits slice by ts — both full-scanned without this.
		"CREATE INDEX IF NOT EXISTS idx_mfs_ts ON maker_fill_stats(ts)",
		// kv is written per-mutation (latch persistence) — index-free by design (PK lookup only).
	} {
		if _, err := db.Exec(ix); err != nil { // R101 (bug 236): a failed index build must be VISIBLE
			idxWarns = append(idxWarns, fmt.Sprintf("index DDL failed (silent full-scan risk): %s: %v", ix, err))
		}
	}
	// R143: these new targeted indexes replace two measured whole-ledger scans. Analyze them now so
	// SQLite does not keep choosing the older broad resolved-platform index from stale statistics.
	for _, name := range []string{"idx_signal_exact_settlement", "idx_signal_series_roll",
		"idx_rseries_series_event"} {
		if _, err := db.Exec("ANALYZE " + name); err != nil {
			idxWarns = append(idxWarns, fmt.Sprintf("index ANALYZE failed (stale planner risk): %s: %v", name, err))
		}
	}
	// One-time data fix (PRAGMA user_version 1): the kalshi-flow consensus row logged NO-side signals at
	// the YES price (it used Price=implied for both sides), so every NO looked hugely +EV and the ML book
	// loaded up on them (387% ROI / +70¢ EV artifact). Correct historical NO rows to the true NO cost
	// (1−yes) and delete ml_paper.json so the ML book re-picks/settles on the corrected prices. The source
	// is already fixed; this only repairs rows logged before the fix. Guarded → runs exactly once.
	var uv int
	_ = db.QueryRow("PRAGMA user_version").Scan(&uv)
	if err := migratePolyUSFinalSettlementAuthority(db); err != nil {
		return nil, fmt.Errorf("migrate PolyUS final-settlement authority: %w", err)
	}
	if uv < 1 {
		_, _ = db.Exec("UPDATE signal_log SET entry_price = 1 - entry_price WHERE signal_type = 'kalshi-flow' AND UPPER(side) = 'NO' AND entry_price > 0 AND entry_price < 1")
		_, _ = db.Exec("UPDATE signal_log SET fill_price = 1 - fill_price WHERE signal_type = 'kalshi-flow' AND UPPER(side) = 'NO' AND fill_price > 0 AND fill_price < 1") // ML prefers fill_price → fix it too
		_ = os.Remove(filepath.Join(dataDir, "ml_paper.json"))                                                                                                             // reset the corrupted ML paper book
		_, _ = db.Exec("PRAGMA user_version = 1")
	}
	if uv < 2 {
		// v1's in-place flip can't be re-applied safely once the (then still-buggy) source kept logging NEW
		// NO-at-YES-price rows after it ran — re-flipping would double-flip the already-fixed ones. So clear
		// kalshi-flow history outright; the now-source-fixed logger repopulates it clean within minutes. This
		// is what finally kills the +77¢ artifact in the ML's "Top scored". Other signals are untouched.
		_, _ = db.Exec("DELETE FROM signal_log WHERE signal_type = 'kalshi-flow'")
		_ = os.Remove(filepath.Join(dataDir, "ml_paper.json"))
		_, _ = db.Exec("PRAGMA user_version = 2")
	}
	if uv < 3 {
		// MLBUGFIX (v3): poly-pred-kalshi logged NO-side rows at Poly's up-prob (cheap, e.g. 0.13)
		// instead of the Kalshi NO cost (1−yes ≈ 0.87) — the SAME sign-flip bug as kalshi-flow. It
		// minted a fake +45¢ EV that took over the ML "Top scored" right after kalshi-flow was
		// cleaned (the artifact "relocated"). The source is now fixed (logs the real Kalshi side
		// cost: YES=kup, NO=1−kup); clear the poisoned history so the model retrains clean. The
		// fixed logger repopulates it within minutes. Other signal types are untouched.
		_, _ = db.Exec("DELETE FROM signal_log WHERE signal_type = 'poly-pred-kalshi'")
		_ = os.Remove(filepath.Join(dataDir, "ml_paper.json"))
		_, _ = db.Exec("PRAGMA user_version = 3")
	}
	if uv < 4 {
		// LABEL QUARANTINE (v4 — audit #7 + follow-up addendum): Poly-US markets that read "CLOSED" but
		// were never graded returned NO settlement price; the JSON zero-value 0 then mass-settled every
		// signal on the slug at settle_val=0 (YES rows → fake losses, NO rows → fake wins). The DB shows
		// the exact signature: 27,750 polyus rows at settle_val≤0.01 vs 5,536 at ≥0.99 with ZERO
		// mid-values. True zero-settles are indistinguishable from the corruption, so every such row is
		// QUARANTINED: resolved=-1 removes them from all resolved=1 consumers (ML training, Edge finder,
		// stop-lab, parlay backtests, DECIDES) without re-entering the resolved=0 sweep queues. The sweep
		// now requires a venue-SENT settlement price (HasSettlement), so rows resolved after this are clean.
		_, _ = db.Exec("UPDATE signal_log SET resolved=-1 WHERE platform='polyus' AND resolved=1 AND settle_val >= 0 AND settle_val <= 0.01")
		_ = os.Remove(filepath.Join(dataDir, "ml_paper.json")) // book settled/trained on the corrupted labels — reset
		_, _ = db.Exec("PRAGMA user_version = 4")
	}
	if uv < 5 {
		// PARLAY ARTIFACTS (v5 — audit F4): the pre-guard parlay book's +$1,809 "realized" was fabricated
		// by a 10-second place→settle→replace loop betting stale legs on already-finished games (one
		// same-game combo placed 25×, +$1,576 = 87% of all profit; 327 parlays = 77 distinct leg-sets).
		// Those rows stay for forensics but are FLAGGED so no headline P&L or backtest cites them as
		// evidence combos work. Everything placed after the correlation guard + live re-pricing landed
		// (2026-07-01) is clean.
		_, _ = db.Exec("UPDATE paper_parlays SET artifact=1 WHERE ts < '2026-07-01T00:00:00Z'")
		_, _ = db.Exec("PRAGMA user_version = 5")
	}
	if uv < 6 {
		// MAINTENANCE BLOWUP QUARANTINE (v6 — the −$1.7B header): during the 2026-07-02 09:15–10:15Z
		// Kalshi/PolyUS maintenance window the frozen books "quoted" crypto-15M legs at 0.1¢; 40
		// parlays booked stake/price ≈ 58 BILLION contracts, rolled nine-figure "fees", and settled
		// at 0 within the hour — −$1,701,558,470 of pure artifact. Placement now refuses collapsed/
		// out-of-band leg prices (placeParlayLegs) and the settle fuse flags any |realized|>$10k,
		// but the rows themselves are quarantined here so restored backups heal too.
		// Two cuts: the global extreme signature, plus EVERYTHING sub-1¢ placed inside the incident
		// window (a tail of 54 medium-blown rows — prices 0.0003–0.0008, 39k–72k contracts, −$9.5k —
		// sat under the extreme thresholds).
		_, _ = db.Exec("UPDATE paper_parlays SET artifact=1 WHERE price < 0.0001 OR contracts > 1000000 OR ABS(realized) > 100000")
		_, _ = db.Exec("UPDATE paper_parlays SET artifact=1 WHERE ts >= '2026-07-02T09:00' AND ts < '2026-07-02T11:00' AND price < 0.01")
		_, _ = db.Exec("PRAGMA user_version = 6")
	}
	if uv < 7 {
		// R90 SIDE-LABEL POISON QUARANTINE (v7 — auditor DO-THIS 1, bugs 73/74/75, watch 36):
		// bridging/arb producers logged exec-venue rows whose side is a Poly OUTCOME LABEL
		// ('Marta Kostyuk', 'NO LPH GAMING') instead of the venue's YES/NO vocabulary. The
		// resolver's CASE graded every one ELSE-0 → 58 fabricated losses + 48 open rows at
		// auditor r22 — fake losses that depressed direct-side LBs and inflated the inverted-side
		// stats fbridge/pfbridge placement rests on (fbridge:inverted +11.6¢ was an artifact of
		// exactly these). Per-row truth is unknowable (the side is garbage), so QUARANTINE
		// (resolved=-1, the v4 pattern): out of every resolved=1 consumer (ML training, Edge,
		// placement policy, DECIDES) without re-entering the resolved=0 sweeps. Producers, the
		// resolver skip-guard (bug 126) and the insertSignal chokepoint are all fixed this same
		// pass; 'fade' is polymarket-only by canon and excluded by both filters. The before/after
		// counts are journaled to audit_log so the one-shot is reviewable from /api/auditlog.
		var nGr, nOp int
		_ = db.QueryRow(`SELECT COUNT(*) FROM signal_log WHERE platform IN ('kalshi','polyus') AND UPPER(TRIM(side)) NOT IN ('YES','NO') AND signal_type != 'fade' AND resolved = 1`).Scan(&nGr)
		_ = db.QueryRow(`SELECT COUNT(*) FROM signal_log WHERE platform IN ('kalshi','polyus') AND UPPER(TRIM(side)) NOT IN ('YES','NO') AND signal_type != 'fade' AND resolved = 0`).Scan(&nOp)
		_, _ = db.Exec(`UPDATE signal_log SET resolved = -1 WHERE platform IN ('kalshi','polyus') AND UPPER(TRIM(side)) NOT IN ('YES','NO') AND signal_type != 'fade' AND resolved IN (0,1)`)
		if nGr+nOp > 0 { // a no-op migration (fresh DB) leaves no audit noise
			_, _ = db.Exec(`INSERT INTO audit_log(ts,level,category,message,detail) VALUES(?,?,?,?,?)`,
				nowRFC(), "info", "storage", "R90 side-label poison quarantine (auditor DO-THIS 1, bugs 73/74/75)",
				fmt.Sprintf("quarantined resolved=-1: %d graded + %d open exec-venue non-canonical-side rows (canon: platform IN kalshi/polyus, UPPER(side) NOT IN YES/NO, type != fade)", nGr, nOp))
		}
		_, _ = db.Exec("PRAGMA user_version = 7")
	}
	if err := repairPolyUSMLFractionalSettlements(db, dataDir); err != nil {
		return nil, fmt.Errorf("repair PolyUS ML false settlements: %w", err)
	}
	// Switch dedup from per-DAY to per-~10-min SLOT so we capture how a signal EVOLVES through a
	// market's life (whales adding, flow strengthening) instead of one row per market/day.
	// R90 bug 99 (auditor DO-THIS 2 — the boot-brick): this used to be an unconditional
	// DROP+CREATE on EVERY boot with both errors discarded. Failure chain: a crash between the
	// two statements, or a swallowed CREATE failure (SQLITE_BUSY outlasting the 35s timeout
	// during a sidecar training burst, or real duplicates), left the DB with NO unique index —
	// InsertSignal's INSERT OR IGNORE then silently stopped deduping — and on the NEXT boot
	// schema.sql re-created idx_signal_dedup under its LEGACY per-DAY shape, a UNIQUE constraint
	// slot-era data genuinely violates; schema failure is fatal in Open, so every boot after that
	// died at "migrate:" (the brick). Now: schema.sql no longer owns this index; we (re)build it
	// here ONLY when it is absent or wrong-shaped, dedupe-and-retry once if the build hits real
	// duplicates (keep the earliest row per key — the semantics INSERT OR IGNORE would have
	// enforced), and abort Open LOUDLY on any other failure instead of booting into silent
	// corruption. Healthy boots skip the whole block: no drop window, no ~1M-row index rebuild.
	var dedupSQL string
	_ = db.QueryRow(`SELECT COALESCE(sql,'') FROM sqlite_master WHERE type='index' AND name='idx_signal_dedup'`).Scan(&dedupSQL)
	dedupNorm := strings.ReplaceAll(strings.ToLower(dedupSQL), " ", "")
	const signalDedupShape = "(slot,platform,ticker,side,signal_type,input_topology)"
	if !strings.Contains(dedupNorm, "unique") || !strings.Contains(dedupNorm, signalDedupShape) {
		// Backfill slot from ts first so legacy rows don't collide on the new unique index.
		_, _ = db.Exec("UPDATE signal_log SET slot = substr(ts,1,15) WHERE slot = '' OR slot IS NULL")
		_, _ = db.Exec("DROP INDEX IF EXISTS idx_signal_dedup")
		if _, err := db.Exec("CREATE UNIQUE INDEX idx_signal_dedup ON signal_log(slot,platform,ticker,side,signal_type,input_topology)"); err != nil {
			_, _ = db.Exec(`DELETE FROM signal_log WHERE id NOT IN (
				SELECT MIN(id) FROM signal_log GROUP BY slot, platform, ticker, side, signal_type, input_topology)`)
			if _, err2 := db.Exec("CREATE UNIQUE INDEX idx_signal_dedup ON signal_log(slot,platform,ticker,side,signal_type,input_topology)"); err2 != nil {
				return nil, fmt.Errorf("signal dedup index rebuild (bug 99 guard): %w", err2)
			}
		}
	}
	// Covering index for the per-tick fill/post-path UPDATEs (UpdateSignalFill / AppendSignalPostPath):
	// they seek by (platform,ticker,side,resolved) → make it an index seek, not a growing-table scan.
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_signal_live ON signal_log(platform,ticker,side,resolved)"); err != nil {
		idxWarns = append(idxWarns, fmt.Sprintf("idx_signal_live build failed (bug 236): %v", err)) // R101
	}
	// live_combos forward migrations. Duplicate-column errors are expected after the first
	// successful boot; defaults deliberately leave every pre-migration row fee-unknown and
	// acceptance-unknown so gross legacy P&L can never masquerade as fee-net evidence.
	for _, col := range []string{
		"contracts REAL NOT NULL DEFAULT 1",
		"accepted_fee REAL NOT NULL DEFAULT 0",
		"fee_source TEXT NOT NULL DEFAULT ''",
		"fee_known INTEGER NOT NULL DEFAULT 0",
		"accept_state TEXT NOT NULL DEFAULT 'legacy_unknown'",
		"filled_contracts REAL NOT NULL DEFAULT 0",
		"fill_receipt TEXT NOT NULL DEFAULT ''",
		"rfq_id TEXT NOT NULL DEFAULT ''",
		"quote_id TEXT NOT NULL DEFAULT ''",
		"promotion_intent_id TEXT NOT NULL DEFAULT ''",
		"rfq_creator_order_id TEXT NOT NULL DEFAULT ''",
		"model_cohort TEXT NOT NULL DEFAULT ''",
	} {
		_, _ = db.Exec("ALTER TABLE live_combos ADD COLUMN " + col)
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS research_route_bundle_live_fills (
		intent_id TEXT PRIMARY KEY,
		observed_ts TEXT NOT NULL,
		market_ticker TEXT NOT NULL,
		rfq_id TEXT NOT NULL,
		quote_id TEXT NOT NULL,
		quote_status TEXT NOT NULL,
		filled_quantity REAL NOT NULL CHECK(filled_quantity>0),
		average_price REAL NOT NULL CHECK(average_price>0 AND average_price<1),
		actual_fee REAL NOT NULL CHECK(actual_fee>=0),
		fee_source TEXT NOT NULL,
		fill_ids_json TEXT NOT NULL CHECK(json_valid(fill_ids_json)),
		position_quantity REAL NOT NULL,
		FOREIGN KEY(intent_id) REFERENCES research_route_bundle_promotion_intents(intent_id)
	);
	CREATE TRIGGER IF NOT EXISTS rr_bundle_live_fill_requires_dispatch BEFORE INSERT ON research_route_bundle_live_fills
	WHEN NOT EXISTS(SELECT 1 FROM research_route_bundle_promotion_intents p
	 JOIN research_route_bundle_promotion_events e ON e.intent_id=p.intent_id AND e.event_type IN ('live_dispatched','live_ambiguous')
	 WHERE p.intent_id=NEW.intent_id AND e.rfq_id=NEW.rfq_id AND e.quote_id=NEW.quote_id
	  AND json_extract(e.evidence_json,'$.market_ticker')=NEW.market_ticker
	  AND NEW.filled_quantity+0.000000001>=p.quantity)
	BEGIN SELECT RAISE(ABORT,'typed bundle live_filled lacks identical dispatched RFQ or full quantity'); END;
	CREATE TRIGGER IF NOT EXISTS rr_bundle_live_fills_no_update BEFORE UPDATE ON research_route_bundle_live_fills BEGIN SELECT RAISE(ABORT,'immutable typed bundle live_filled receipt'); END;
	CREATE TRIGGER IF NOT EXISTS rr_bundle_live_fills_no_delete BEFORE DELETE ON research_route_bundle_live_fills BEGIN SELECT RAISE(ABORT,'immutable typed bundle live_filled receipt'); END;`); err != nil {
		return nil, fmt.Errorf("migrate typed bundle live fill receipts: %w", err)
	}
	// WHALE V2 (audit F7/Q2): closing-price capture on trader trades (CLV) + a per-condition
	// resolution-status table for ROTATION + RETRY/BACKOFF (the old sweep re-fetched the same
	// unordered head-40 conditions every 7s forever — 0.37% of 1.4M rows labeled, zero new
	// resolutions while ~5.7 req/s burned on 2024-era markets).
	_, _ = db.Exec("ALTER TABLE poly_trader_trades ADD COLUMN close_px REAL NOT NULL DEFAULT -1")
	_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS poly_condition_status (
		condition_id TEXT PRIMARY KEY,
		attempts     INTEGER NOT NULL DEFAULT 0,
		next_retry   INTEGER NOT NULL DEFAULT 0
	)`)
	// R132 condition-id quarantine. The public activity feed has emitted a 62-hex prefix
	// derived from asset/token id in conditionId. It is not a condition id and can never resolve,
	// so one poisoned value otherwise occupies the retry worker forever. Scan once, atomically
	// quarantine every malformed row as resolved=-3, and remove its retry state.
	const invalidPTTConditionSQL = `(length(condition_id) <> 66 OR substr(condition_id,1,2) <> '0x' OR substr(condition_id,3) GLOB '*[^0-9A-Fa-f]*')`
	var conditionQuarantineDone int
	_ = db.QueryRow(`SELECT COUNT(*) FROM kv WHERE k='r132_ptt_condition_quarantine'`).Scan(&conditionQuarantineDone)
	if conditionQuarantineDone == 0 {
		if err := func() error {
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			var n int64
			if err := tx.QueryRow(`SELECT COUNT(*) FROM poly_trader_trades WHERE resolved <> -3 AND ` + invalidPTTConditionSQL).Scan(&n); err != nil {
				return err
			}
			if _, err := tx.Exec(`UPDATE poly_trader_trades SET resolved=-3, resolved_at=? WHERE resolved <> -3 AND `+invalidPTTConditionSQL, nowRFC()); err != nil {
				return err
			}
			if _, err := tx.Exec(`DELETE FROM poly_condition_status WHERE ` + invalidPTTConditionSQL); err != nil {
				return err
			}
			if n > 0 {
				if _, err := tx.Exec(`INSERT INTO audit_log(ts,level,category,message,detail) VALUES(?,?,?,?,?)`,
					nowRFC(), "warn", "storage", "R132 malformed Polymarket condition ids quarantined",
					fmt.Sprintf("resolved=-3 rows=%d; strict condition format is 0x plus 64 hex", n)); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(`INSERT INTO kv(k,v,ts) VALUES('r132_ptt_condition_quarantine',?,?)`, strconv.FormatInt(n, 10), nowRFC()); err != nil {
				return err
			}
			return tx.Commit()
		}(); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("R132 condition-id quarantine migration: %w", err)
		}
	}
	// R76 WEDGE FIX (auditor bug 1): these two composites were previously created under the names
	// idx_ptt_open / idx_ptt_wallet — names ALREADY TAKEN by schema.sql's single-column indexes, so
	// IF NOT EXISTS silently no-opped and the composites never existed on any real DB. Renamed to
	// fresh names (schema.sql creates them too, error-checked, before we get here — these are
	// belt-and-suspenders) and joined by idx_ptt_cond, the bare condition_id index the retention
	// orphan probe needs. ANALYZE only on the boot that actually built the index (hadPttCond probe
	// above) so every later start stays instant while the planner learns the new index once.
	for _, ix := range []string{ // R101 (bug 236): error-checked — a silent miss here re-creates the R76 wedge
		"CREATE INDEX IF NOT EXISTS idx_ptt_cond ON poly_trader_trades(condition_id)",
		"CREATE INDEX IF NOT EXISTS idx_ptt_resolved_cond ON poly_trader_trades(resolved, condition_id)",
		"CREATE INDEX IF NOT EXISTS idx_ptt_wallet_res ON poly_trader_trades(wallet, resolved)",
	} {
		if _, err := db.Exec(ix); err != nil {
			idxWarns = append(idxWarns, fmt.Sprintf("index DDL failed (silent full-scan risk): %s: %v", ix, err))
		}
	}
	if hadPttCond == 0 {
		_, _ = db.Exec("ANALYZE poly_trader_trades")
	}
	// R69 item 7: archived-tape backfill (cmd/kalshi-backfill — a separate, operator-run binary).
	// Kalshi's /historical/trades tier serves pre-cutoff public trades; this table stores them per
	// series for research/backtests. trade_id is the venue's own unique id → INSERT OR IGNORE makes
	// re-runs/resumes idempotent. Prices in DOLLARS (fixed-point era only — no legacy cents here).
	_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS backfill_trades (
		trade_id     TEXT PRIMARY KEY,
		series       TEXT NOT NULL,
		ticker       TEXT NOT NULL,
		created_time TEXT NOT NULL,
		count        REAL NOT NULL,
		yes_price    REAL NOT NULL,
		no_price     REAL NOT NULL,
		taker_side   TEXT NOT NULL
	)`)
	_, _ = db.Exec("CREATE INDEX IF NOT EXISTS idx_bkfl_ticker_ts ON backfill_trades(ticker, created_time)")
	_, _ = db.Exec("CREATE INDEX IF NOT EXISTS idx_bkfl_series ON backfill_trades(series, created_time)")
	// R72-B MARKET CATALOG (operator: "all subbets … in markets AND logged as data"): a persistent
	// per-venue catalog of every market the ingestion has ever seen — the durable record of WHAT was
	// listed (winner/total/spread/prop), independent of whether a signal ever fired on it. Upserted
	// in bounded batches on each catalog sweep; last_seen rows older than 90d are pruned (retention).
	_, _ = db.Exec(`CREATE TABLE IF NOT EXISTS market_catalog (
		venue      TEXT NOT NULL,
		ticker     TEXT NOT NULL,
		event_key  TEXT NOT NULL DEFAULT '',
		kind       TEXT NOT NULL DEFAULT '',
		title      TEXT NOT NULL DEFAULT '',
		first_seen TEXT NOT NULL,
		last_seen  TEXT NOT NULL,
		close_ts   TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (venue, ticker)
	)`)
	_, _ = db.Exec("CREATE INDEX IF NOT EXISTS idx_mcat_seen ON market_catalog(last_seen)")
	_, _ = db.Exec("CREATE INDEX IF NOT EXISTS idx_mcat_event ON market_catalog(venue, event_key)")
	if _, err := db.Exec("CREATE INDEX IF NOT EXISTS idx_mcat_event_kind_close ON market_catalog(venue,event_key,kind,close_ts DESC,ticker)"); err != nil {
		idxWarns = append(idxWarns, fmt.Sprintf("market-catalog index DDL failed: %v", err))
	} else if _, err := db.Exec("ANALYZE idx_mcat_event_kind_close"); err != nil {
		idxWarns = append(idxWarns, fmt.Sprintf("market-catalog index ANALYZE failed: %v", err))
	}
	// R102 STRUCTURAL GAME MATCHING: the canonical game-identity table + the per-market hard join,
	// both populated from VENUE METADATA at ingest (Kalshi event tickers/strike fields, PolyUS game
	// objects) — the durable record the cross-venue matchers consume FIRST (fuzzy title matching is
	// fallback-only for markets with no structural anchor). game_id is deterministic:
	// "<league>:<ET-date>:<AWAY>@<HOME>[#2]" (#2 = doubleheader second start).
	for _, ddl := range []string{
		`CREATE TABLE IF NOT EXISTS game_identity (
			game_id    TEXT PRIMARY KEY,
			league     TEXT NOT NULL,
			away       TEXT NOT NULL DEFAULT '',
			home       TEXT NOT NULL DEFAULT '',
			away_name  TEXT NOT NULL DEFAULT '',
			home_name  TEXT NOT NULL DEFAULT '',
			start_utc  TEXT NOT NULL DEFAULT '',
			kalshi_event TEXT NOT NULL DEFAULT '',
			pus_event_id TEXT NOT NULL DEFAULT '',
			pus_game_id  INTEGER NOT NULL DEFAULT 0,
			first_seen TEXT NOT NULL,
			last_seen  TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS market_game (
			venue      TEXT NOT NULL,
			ticker     TEXT NOT NULL,
			game_id    TEXT NOT NULL,
			mkt_type   TEXT NOT NULL DEFAULT '',
			yes_team   TEXT NOT NULL DEFAULT '',
			no_team    TEXT NOT NULL DEFAULT '',
			line       REAL NOT NULL DEFAULT 0,
			src        TEXT NOT NULL DEFAULT 'struct',
			first_seen TEXT NOT NULL,
			last_seen  TEXT NOT NULL,
			PRIMARY KEY (venue, ticker)
		)`,
		"CREATE INDEX IF NOT EXISTS idx_mgame_game ON market_game(game_id)",
		"CREATE INDEX IF NOT EXISTS idx_mgame_struct_recent ON market_game(venue,src,last_seen DESC,game_id)",
		"CREATE INDEX IF NOT EXISTS idx_mgame_struct_join ON market_game(game_id,venue,src,mkt_type,yes_team)",
		"CREATE INDEX IF NOT EXISTS idx_gident_start ON game_identity(start_utc)",
	} {
		if _, err := db.Exec(ddl); err != nil { // R101 bug-236 doctrine: DDL failures surface, never silent
			idxWarns = append(idxWarns, fmt.Sprintf("game-identity DDL failed: %v", err))
		}
	}
	// R106 (bug 255): two-sided single instruments — the short/NO side's team on the same join row.
	// Additive migration (pre-existing DBs); fresh DDL above stays canonical.
	_, _ = db.Exec("ALTER TABLE market_game ADD COLUMN no_team TEXT NOT NULL DEFAULT ''")
	if err := migratePolymarketGameAlias(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate Polymarket structural venue alias: %w", err)
	}
	// R125 (matcher audit: 13/50 sampled joins wrong): pre-R125 polyus join rows were classified
	// SCOPE-BLIND (first-five/first-half/second-half markets stored as full-game totals/spreads)
	// and neg-slug spreads carried a double-flipped sign. One-time latched purge of the polyus
	// rows — the in-memory registry rebuilds them within one refreshPolyUS cycle under the fixed
	// classifier, and giPersist re-fills the table. Kalshi rows are untouched.
	var giRescoped int
	_ = db.QueryRow(`SELECT COUNT(*) FROM kv WHERE k='r125_gi_rescope'`).Scan(&giRescoped)
	if giRescoped == 0 {
		if _, err := db.Exec(`DELETE FROM market_game WHERE venue='polyus'`); err == nil {
			_, _ = db.Exec(`INSERT INTO kv(k,v,ts) VALUES('r125_gi_rescope','done',?)
ON CONFLICT(k) DO UPDATE SET v='done', ts=excluded.ts`, nowRFC())
		}
	}
	// R72-B post_path widening (QUANT_STUDY rec #4): the candle backfill now also serves rows that
	// never got a real fill — this index makes the widened queue query an index walk, not a scan.
	_, _ = db.Exec("CREATE INDEX IF NOT EXISTS idx_signal_needpath_any ON signal_log(platform, resolved, id) WHERE post_path = ''")
	// R72-B: ResolvedYes (WHERE ticker=? AND resolved=1 ORDER BY ts DESC) had NO ticker-leading
	// index — every ML-book settle probe walked the table; the kflow twin books add more callers
	// on the same 7s tick. Point query → point seek.
	_, _ = db.Exec("CREATE INDEX IF NOT EXISTS idx_signal_ticker_resolved ON signal_log(ticker, resolved, ts)")
	// Unified execution comparison is intentionally isolated from the cash/research database.
	// The legacy tables above remain an immutable boot-migration source, but no runtime shadow
	// read or write uses the main handle after this point.
	executionShadowDB, executionShadowPath, err := openExecutionShadowDB(dataDir)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("open isolated execution-shadow storage: %w", err)
	}
	// R124: archive lives next to kalshi.db; attached lazily on first use (archive.go).
	store := &Store{db: db, executionShadowDB: executionShadowDB,
		executionShadowPath: executionShadowPath, IndexWarnings: idxWarns,
		archPath: filepath.Join(dataDir, ArchiveFileName)}
	shadowBootCtx, cancelShadowBoot := context.WithTimeout(context.Background(), 30*time.Second)
	err = store.migrateLegacyExecutionShadow(shadowBootCtx)
	cancelShadowBoot()
	if err != nil {
		_ = executionShadowDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("migrate isolated execution-shadow storage: %w", err)
	}
	// R138 Step 1: the immutable 19-system research blueprint is part of storage truth. A changed
	// payload under the same version fails boot loudly; the author must append a new version rather
	// than silently rewriting the hypothesis behind already-collected evidence.
	researchBootCtx, cancelResearchBoot := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancelResearchBoot()
	if err := store.EnsureR138ResearchBlueprints(researchBootCtx); err != nil {
		_ = executionShadowDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("register R138 research blueprints: %w", err)
	}
	if err := store.EnsureR138CollectorBlueprints(researchBootCtx); err != nil {
		_ = executionShadowDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("register R138 collector blueprints: %w", err)
	}
	if err := store.EnsureR138SourceClockBlueprints(researchBootCtx); err != nil {
		_ = executionShadowDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("register R138 source-clock blueprints: %w", err)
	}
	// A process can stop after reserving a delayed Proper Betting Paper IOC but before recording
	// its second-book result. Release every old reservation during storage startup, independently
	// of whether this boot later finds any eligible forecasts or starts the research scheduler.
	properPaperBootCtx, cancelProperPaperBoot := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = store.RecoverProperScorePaperAttempts(properPaperBootCtx, time.Now().UTC())
	cancelProperPaperBoot()
	if err != nil {
		_ = executionShadowDB.Close()
		_ = db.Close()
		return nil, fmt.Errorf("recover Proper Betting Paper reservations: %w", err)
	}
	return store, nil
}

// migrateResearchSystemObservationIdentity upgrades the pre-R139 observation ledger without
// rewriting historical rows. New economic routes are database-guarded against an exact immutable
// venue instrument/event/payoff version; legacy/observer rows remain nullable and inference
// continues to exclude them.
func migrateResearchSystemObservationIdentity(db *sql.DB) error {
	for _, col := range []string{
		"ticker TEXT NOT NULL DEFAULT ''",
		"canonical_payoff_id TEXT",
		"payoff_version INTEGER",
		"instrument_version INTEGER NOT NULL DEFAULT 0",
		"decision_ts TEXT NOT NULL DEFAULT ''",
	} {
		if _, err := db.Exec("ALTER TABLE research_system_observations ADD COLUMN " + col); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	if _, err := db.Exec(`DROP TRIGGER IF EXISTS research_system_observations_exact_identity_insert`); err != nil {
		return err
	}
	if _, err := db.Exec(`DROP TRIGGER IF EXISTS research_system_observations_decision_clock_insert`); err != nil {
		return err
	}
	if _, err := db.Exec(`CREATE TRIGGER research_system_observations_decision_clock_insert
BEFORE INSERT ON research_system_observations
WHEN TRIM(COALESCE(NEW.decision_ts,''))='' OR julianday(NEW.decision_ts) IS NULL
BEGIN SELECT RAISE(ABORT,'new research observation lacks exact decision clock'); END`); err != nil {
		return err
	}
	_, err := db.Exec(`CREATE TRIGGER research_system_observations_exact_identity_insert
BEFORE INSERT ON research_system_observations WHEN NEW.route!='observer'
BEGIN
  SELECT CASE WHEN TRIM(NEW.ticker)='' OR TRIM(COALESCE(NEW.canonical_payoff_id,''))='' OR
    COALESCE(NEW.payoff_version,0)<=0 OR NEW.event_version<=0 OR
    COALESCE(NEW.instrument_version,0)<=0 OR NOT EXISTS(
      SELECT 1 FROM research_instrument_specs i
      JOIN research_payoff_specs p ON p.event_id=i.event_id AND p.event_version=i.event_version
       AND p.payoff_id=i.payoff_id AND p.version=i.payoff_version
      WHERE i.venue=NEW.venue AND i.ticker=NEW.ticker
       AND i.version=NEW.instrument_version
       AND i.event_id=NEW.canonical_event_id AND i.event_version=NEW.event_version
       AND i.payoff_id=NEW.canonical_payoff_id AND i.payoff_version=NEW.payoff_version
    ) THEN RAISE(ABORT,'economic research observation lacks exact immutable instrument/event/payoff identity') END;
END`)
	return err
}

// CatalogRow is one market_catalog upsert (R72-B): a market the ingestion currently sees.
type CatalogRow struct {
	Venue    string // kalshi | polyus | polymarket
	Ticker   string // venue-native id (ticker / slug / conditionId)
	EventKey string // grouping key (Kalshi event ticker, PUS game, poly event slug); "" if unknown
	Kind     string // winner | total | spread | advance (R106) | prop | unknown
	Title    string
	CloseTS  string // venue close/resolve time (RFC3339 where known; "" if unknown)
}

// UpsertMarketCatalog folds a snapshot of currently-visible markets into market_catalog in
// CHUNKED transactions (R85: 2,000 rows each — the R73 full kalshi board is ~14k rows and a single
// all-or-nothing tx never survived sidecar-retrain contention). first_seen is written once;
// last_seen/close_ts/title/kind refresh on every conflict, so the catalog tracks both the
// market's lifetime window and its latest metadata.
func (s *Store) UpsertMarketCatalog(ctx context.Context, rows []CatalogRow) error {
	if len(rows) == 0 {
		return nil
	}
	// R75: retry once after a beat — the catalog sweep is a 5-min background job, and a >10s
	// writer stall (the ML sidecar's Python process holds long write txns during retrains)
	// should cost one delayed sweep, not an EMPTY catalog (which is what the live DB showed).
	//
	// R85 (operator: "mkts need to be 12k/12k"): CHUNK the batch — 2,000 rows per transaction.
	// The R73 kalshi board is ~14k rows in ONE all-or-nothing tx; against the sidecar's retrain
	// write-txns (every 4 min) + busy_timeout waits it lost the race on EVERY pass since R73
	// (live DB probe 2026-07-05: market_catalog had ZERO venue='kalshi' rows — poly 10,447 /
	// polyus 1,189 only, total 11,636 = exactly the auditor's count). A failed/deadline-cut
	// chunk now costs only ITS 2,000 rows; committed chunks stick, so the catalog converges
	// across passes instead of rolling back wholesale forever.
	const chunk = 2000
	for start := 0; start < len(rows); start += chunk {
		end := start + chunk
		if end > len(rows) {
			end = len(rows)
		}
		part := rows[start:end]
		err := s.upsertMarketCatalogOnce(ctx, part)
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return err
			case <-time.After(3 * time.Second):
			}
			if err = s.upsertMarketCatalogOnce(ctx, part); err != nil {
				return err // committed chunks stay committed — the next 5-min pass resumes the rest
			}
		}
	}
	return nil
}

// WALCheckpointPassive — R97: attempt a PASSIVE WAL checkpoint. PASSIVE never blocks readers or
// writers (it simply makes no progress while a read txn pins the reader mark), so it is safe on
// any cadence; the three PRAGMA outputs say what happened (busy=1 → another checkpointer/writer
// was active; logFrames/checkpointed → WAL frames total vs moved into the main db).
func (s *Store) WALCheckpointPassive(ctx context.Context) (busy, logFrames, checkpointed int, err error) {
	row := s.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	err = row.Scan(&busy, &logFrames, &checkpointed)
	return busy, logFrames, checkpointed, err
}

// WALCheckpointTruncate — R98 (the R97 queue item: PASSIVE folds frames but can never shrink the
// 1.3GB file). TRUNCATE waits for every reader to clear the WAL, checkpoints ALL frames, then
// truncates the file to zero — so it must NOT inherit the pool's 35s busy_timeout (a pinned WAL
// would stall the sweep, and the wait holds the checkpoint lock against writers). It runs on a
// dedicated pool conn with busy_timeout dropped to budget ms (restored before the conn returns to
// the pool; the pragma is per-connection). busy=1 → a reader pinned it inside the budget; the
// caller falls back to PASSIVE and tries again next pass. With the sidecar's reads now short
// (R98 fetchall fix) an idle window shows up within a pass or two.
func (s *Store) WALCheckpointTruncate(ctx context.Context, budget time.Duration) (busy, logFrames, checkpointed int, err error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, 0, 0, err
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", budget.Milliseconds())); err != nil {
		return 0, 0, 0, err
	}
	row := conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	err = row.Scan(&busy, &logFrames, &checkpointed)
	// R99 bug 174 (auditor r24): the restore used to ride the CALLER ctx — which is dead exactly
	// when the budget was exceeded, so the pooled conn returned with the ~2s busy_timeout and
	// poisoned later queries with spurious BUSY. Restore on a background ctx; if even that fails,
	// mark the conn bad (driver.ErrBadConn from Raw) so the pool discards it instead of recycling
	// a broken one.
	restCtx, rcancel := context.WithTimeout(context.Background(), 3*time.Second)
	if _, e2 := conn.ExecContext(restCtx, "PRAGMA busy_timeout=35000"); e2 != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		if err == nil {
			err = e2
		}
	}
	rcancel()
	return busy, logFrames, checkpointed, err
}

func (s *Store) upsertMarketCatalogOnce(ctx context.Context, rows []CatalogRow) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	st, err := tx.PrepareContext(ctx, `
INSERT INTO market_catalog(venue,ticker,event_key,kind,title,first_seen,last_seen,close_ts)
VALUES(?,?,?,?,?,?,?,?)
ON CONFLICT(venue,ticker) DO UPDATE SET
  last_seen=excluded.last_seen, close_ts=excluded.close_ts, title=excluded.title,
  kind=excluded.kind, event_key=excluded.event_key`)
	if err != nil {
		return err
	}
	defer st.Close()
	now := nowRFC()
	for _, r := range rows {
		if r.Venue == "" || r.Ticker == "" {
			continue
		}
		if _, err := st.ExecContext(ctx, r.Venue, r.Ticker, r.EventKey, r.Kind, r.Title, now, now, r.CloseTS); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GameIdentityRow is one canonical game (R102 structural matching) — venue-metadata-derived.
type GameIdentityRow struct {
	GameID, League, Away, Home, AwayName, HomeName, StartUTC string
	KalshiEvent, PUSEventID                                  string
	PUSGameID                                                int
}

// MarketGameRow is one market → canonical game hard join (R102): mkt_type winner|spread|total|
// advance|prop, yes_team = the canonical team key the YES side backs ("over"/"under" for totals;
// "" for props), line = total line or SIGNED spread line (YES team covers `line`).
// R106 (bug 255): no_team = the short/NO side's team on single-slug two-sided instruments
// (PUS "to advance"), "" everywhere else.
type MarketGameRow struct {
	Venue, Ticker, GameID, MktType, YesTeam, NoTeam, Src string
	Line                                                 float64
}

// UpsertGameIdentity folds canonical games into game_identity (one tx; callers batch + dedupe and
// skip-if-unchanged, so this stays a small write). Venue anchors only ever fill IN (COALESCE-style:
// an empty excluded value never blanks a known anchor).
func (s *Store) UpsertGameIdentity(ctx context.Context, rows []GameIdentityRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	st, err := tx.PrepareContext(ctx, `
INSERT INTO game_identity(game_id,league,away,home,away_name,home_name,start_utc,kalshi_event,pus_event_id,pus_game_id,first_seen,last_seen)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(game_id) DO UPDATE SET
  last_seen=excluded.last_seen,
  away_name=CASE WHEN excluded.away_name<>'' THEN excluded.away_name ELSE game_identity.away_name END,
  home_name=CASE WHEN excluded.home_name<>'' THEN excluded.home_name ELSE game_identity.home_name END,
  start_utc=CASE WHEN excluded.start_utc<>'' THEN excluded.start_utc ELSE game_identity.start_utc END,
  kalshi_event=CASE WHEN excluded.kalshi_event<>'' THEN excluded.kalshi_event ELSE game_identity.kalshi_event END,
  pus_event_id=CASE WHEN excluded.pus_event_id<>'' THEN excluded.pus_event_id ELSE game_identity.pus_event_id END,
  pus_game_id=CASE WHEN excluded.pus_game_id<>0 THEN excluded.pus_game_id ELSE game_identity.pus_game_id END`)
	if err != nil {
		return err
	}
	defer st.Close()
	now := nowRFC()
	for _, r := range rows {
		if r.GameID == "" {
			continue
		}
		if _, err := st.ExecContext(ctx, r.GameID, r.League, r.Away, r.Home, r.AwayName, r.HomeName, r.StartUTC,
			r.KalshiEvent, r.PUSEventID, r.PUSGameID, now, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// UpsertMarketGame folds market→game joins into market_game (one tx; callers batch + skip-if-fresh).
func (s *Store) UpsertMarketGame(ctx context.Context, rows []MarketGameRow) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	st, err := tx.PrepareContext(ctx, `
INSERT INTO market_game(venue,ticker,game_id,mkt_type,yes_team,no_team,line,src,first_seen,last_seen)
VALUES(?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(venue,ticker) DO UPDATE SET
  game_id=excluded.game_id, mkt_type=excluded.mkt_type, yes_team=excluded.yes_team,
  no_team=excluded.no_team, line=excluded.line, src=excluded.src, last_seen=excluded.last_seen`)
	if err != nil {
		return err
	}
	defer st.Close()
	now := nowRFC()
	for _, r := range rows {
		if r.Venue == "" || r.Ticker == "" || r.GameID == "" {
			continue
		}
		if _, err := st.ExecContext(ctx, r.Venue, r.Ticker, r.GameID, r.MktType, r.YesTeam, r.NoTeam, r.Line, r.Src, now, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GameJoinCounts reports the durable structural-join footprint (games + per-venue joined markets) —
// the R102 auditor coverage surface.
func (s *Store) GameJoinCounts(ctx context.Context) (games, kal, pus int, err error) {
	if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM game_identity").Scan(&games); err != nil {
		return
	}
	if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM market_game WHERE venue='kalshi'").Scan(&kal); err != nil {
		return
	}
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM market_game WHERE venue='polyus'").Scan(&pus)
	return
}

// PolyIntEventGroup is one distinct poly-int catalog event key + its open-row count (R127).
type PolyIntEventGroup struct {
	EventKey string
	Rows     int
}

// PolyIntEventPage — R127 pint-sports cursor walk: pages DISTINCT OPEN poly-int (venue
// 'polymarket') catalog event keys lexicographically after afterKey. "Open" = close_ts strictly
// greater than nowTS (both RFC3339 — string order is time order for Z-suffixed stamps). The
// caller applies the game-slug grammar; this just serves ordered breadth cheaply.
func (s *Store) PolyIntEventPage(ctx context.Context, afterKey, nowTS string, limit int) ([]PolyIntEventGroup, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT event_key, COUNT(*) FROM market_catalog
WHERE venue='polymarket' AND event_key > ? AND close_ts > ?
GROUP BY event_key ORDER BY event_key LIMIT ?`, afterKey, nowTS, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PolyIntEventGroup
	for rows.Next() {
		var g PolyIntEventGroup
		if rows.Scan(&g.EventKey, &g.Rows) == nil && g.EventKey != "" {
			out = append(out, g)
		}
	}
	return out, rows.Err()
}

// MarketGameRows returns every market_game join for one venue (R127: the polyint registry's
// boot reload — anchors survive restarts without refetching gamma).
func (s *Store) MarketGameRows(ctx context.Context, venue string) ([]MarketGameRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT venue, ticker, game_id, mkt_type, yes_team, no_team, line, src
FROM market_game WHERE venue=?`, venue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MarketGameRow
	for rows.Next() {
		var r MarketGameRow
		if rows.Scan(&r.Venue, &r.Ticker, &r.GameID, &r.MktType, &r.YesTeam, &r.NoTeam, &r.Line, &r.Src) == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

// CatalogTitle returns one cataloged market's title (R127: the pint_sports sample surface reads
// both sides' human titles for hand-verification). ok=false when never cataloged.
func (s *Store) CatalogTitle(ctx context.Context, venue, ticker string) (string, bool) {
	var title string
	err := s.db.QueryRowContext(ctx,
		"SELECT title FROM market_catalog WHERE venue=? AND ticker=?", venue, ticker).Scan(&title)
	if err != nil || strings.TrimSpace(title) == "" {
		return "", false
	}
	return title, true
}

// GameIdentityRows returns every canonical game row (R127: offline dry-run hydration + any
// future boot-time registry warm).
func (s *Store) GameIdentityRows(ctx context.Context) ([]GameIdentityRow, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT game_id, league, away, home, away_name, home_name, start_utc, kalshi_event, pus_event_id, pus_game_id
FROM game_identity`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GameIdentityRow
	for rows.Next() {
		var r GameIdentityRow
		if rows.Scan(&r.GameID, &r.League, &r.Away, &r.Home, &r.AwayName, &r.HomeName,
			&r.StartUTC, &r.KalshiEvent, &r.PUSEventID, &r.PUSGameID) == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

// CatalogCloseTS returns one market's catalog close/resolve time + kind (R91 Task B: the Poly US
// horizon fallback — the rotating gateway snapshot misses futures/props and aged-out slugs, but
// the catalog holds close_ts for every ingested PUS row). ok=false when the market was never
// cataloged or carries no close_ts.
func (s *Store) CatalogCloseTS(ctx context.Context, venue, ticker string) (closeTS, kind string, ok bool) {
	row := s.db.QueryRowContext(ctx,
		"SELECT close_ts, kind FROM market_catalog WHERE venue=? AND ticker=?", venue, ticker)
	if err := row.Scan(&closeTS, &kind); err != nil || strings.TrimSpace(closeTS) == "" {
		return "", "", false
	}
	return closeTS, kind, true
}

// PruneMarketCatalog deletes catalog rows not last seen since cutoffTS (90d retention; batched via
// rowid-IN so one sweep can't hold the write lock long). Returns rows removed.
func (s *Store) PruneMarketCatalog(ctx context.Context, cutoffTS string, batch int) (int64, error) {
	if batch <= 0 {
		batch = 20000
	}
	res, err := s.db.ExecContext(ctx, `
DELETE FROM market_catalog WHERE rowid IN (
  SELECT rowid FROM market_catalog WHERE last_seen < ? LIMIT ?)`, cutoffTS, batch)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// MarketCatalogStats returns (total rows, rows seen within ~the last day) — the catalog health
// numbers surfaced on the Stats payload.
func (s *Store) MarketCatalogStats(ctx context.Context) (total, fresh int64) {
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM market_catalog`).Scan(&total)
	cut := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339Nano)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM market_catalog WHERE last_seen >= ?`, cut).Scan(&fresh)
	return
}

// MarketCatalogVenueCounts returns market_catalog row counts per venue (R77 item 7 — the bar1
// "mkts" coverage chip tooltip). One indexed GROUP BY; called only from the 5-min catalog sweep,
// never on a request hot path.
func (s *Store) MarketCatalogVenueCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT venue, COUNT(*) FROM market_catalog GROUP BY venue`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var v string
		var n int64
		if rows.Scan(&v, &n) == nil {
			out[v] = n
		}
	}
	return out, rows.Err()
}

// SigEconRow is one resolved signal observation for a signal-level economics readout (R72-B
// kcrypto unretire review — QUANT_STUDY rec #5): entry/fill price, side and the settled value.
type SigEconRow struct {
	Ticker    string
	Side      string
	Entry     float64 // fill price when captured, else the signal price
	SettleVal float64 // authoritative settled YES value 0..1; -1 = binary-only row
	Won       int     // side-relative binary outcome (fallback when SettleVal < 0)
}

// ResolvedSignalEcon returns the newest resolved rows for one signal type since sinceTS (dedup:
// one row per (ticker, side) — fill-priced preferred, like the study/sidecar convention), capped.
func (s *Store) ResolvedSignalEcon(ctx context.Context, sigType, sinceTS string, limit int) ([]SigEconRow, error) {
	if limit <= 0 {
		limit = 2000
	}
	// SQLite bare-column-with-MAX idiom (documented): with MAX(id) aggregated, the other selected
	// columns are taken from that max-id row — i.e. the NEWEST observation per (ticker, side).
	rows, err := s.db.QueryContext(ctx, `
SELECT ticker, side, CASE WHEN fill_price > 0 THEN fill_price ELSE entry_price END,
       COALESCE(settle_val,-1), COALESCE(won,0), MAX(id)
FROM signal_log
WHERE signal_type=? AND resolved=1 AND ts>=? AND won IN (0,1)
GROUP BY ticker, side
ORDER BY MAX(id) DESC LIMIT ?`, sigType, sinceTS, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SigEconRow
	for rows.Next() {
		var r SigEconRow
		var maxID int64
		if err := rows.Scan(&r.Ticker, &r.Side, &r.Entry, &r.SettleVal, &r.Won, &maxID); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Close() error {
	s.r144CollectionBootstrapMu.Lock()
	if s.r144CollectionBootstrapCancel != nil {
		s.r144CollectionBootstrapCancel()
	}
	s.r144CollectionBootstrapMu.Unlock()
	s.r144CollectionBootstrapWG.Wait()
	s.archMu.Lock()
	if s.archConn != nil { // R124: release the held ATTACH connection back before the pool closes
		_ = s.archConn.Close()
		s.archConn = nil
	}
	s.archMu.Unlock()
	var shadowErr error
	if s.executionShadowDB != nil {
		shadowErr = s.executionShadowDB.Close()
	}
	mainErr := s.db.Close()
	if mainErr != nil {
		return mainErr
	}
	return shadowErr
}

// Backup writes a CONSISTENT snapshot of the whole database to destPath using SQLite's
// `VACUUM INTO`. It runs on the live connection (a read transaction), so it is SAFE while the suite
// is actively using the DB — readers/writers aren't blocked and the snapshot is a single,
// defragmented, transactionally-consistent file (often a bit smaller than the live DB). destPath must
// not already exist (VACUUM INTO refuses to overwrite), so we remove any stale file first. The path is
// embedded as an escaped SQL string literal because VACUUM INTO does not reliably accept a bound
// parameter across drivers.
func (s *Store) Backup(ctx context.Context, destPath string) error {
	_ = os.Remove(destPath)
	stmt := "VACUUM INTO '" + strings.ReplaceAll(destPath, "'", "''") + "'"
	_, err := s.db.ExecContext(ctx, stmt)
	return err
}

// BackupSQLiteFile creates a transactionally consistent, WAL-aware snapshot of an arbitrary
// SQLite database. It is used by the offline research recovery command for kalshi_archive.db,
// which deliberately lives on a different SQLite connection from the main Store. Opening the
// source through SQLite (rather than copying db/wal/shm bytes independently) makes every committed
// WAL frame visible in one coherent destination file. The source is opened read-only and is never
// checkpointed, truncated, renamed, or otherwise mutated.
func BackupSQLiteFile(ctx context.Context, sourcePath, destPath string) error {
	sourceAbs, err := filepath.Abs(sourcePath)
	if err != nil {
		return fmt.Errorf("backup sqlite source path: %w", err)
	}
	if fi, err := os.Stat(sourceAbs); err != nil {
		return fmt.Errorf("backup sqlite source: %w", err)
	} else if !fi.Mode().IsRegular() || fi.Size() == 0 {
		return fmt.Errorf("backup sqlite source is not a non-empty regular file")
	}
	_ = os.Remove(destPath)
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(sourceAbs)+"?mode=ro&_pragma=busy_timeout(35000)")
	if err != nil {
		return fmt.Errorf("open sqlite source read-only: %w", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite source read-only: %w", err)
	}
	stmt := "VACUUM INTO '" + strings.ReplaceAll(destPath, "'", "''") + "'"
	if _, err := db.ExecContext(ctx, stmt); err != nil {
		_ = os.Remove(destPath)
		return fmt.Errorf("vacuum sqlite source into recovery copy: %w", err)
	}
	return nil
}

// QuickCheck opens the SQLite file at path READ-ONLY and runs PRAGMA quick_check, returning an
// error unless it reports "ok". Gate for the backup/restore path (audit #5): a malformed snapshot
// must never become the backup of record, and auto-restore must never copy a malformed snapshot
// over the live path. Cheap relative to a full integrity_check; catches truncation/page damage.
func QuickCheck(path string) error {
	if fi, err := os.Stat(path); err != nil {
		return err
	} else if fi.Size() == 0 {
		return fmt.Errorf("empty file")
	}
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query("PRAGMA quick_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	var msgs []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return err
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(msgs) == 1 && strings.EqualFold(msgs[0], "ok") {
		return nil
	}
	if len(msgs) == 0 {
		return fmt.Errorf("quick_check returned no rows")
	}
	return fmt.Errorf("quick_check failed: %s", strings.Join(msgs, "; "))
}

// tsLayout is the FIXED-WIDTH UTC stamp layout (RFC3339, 9 fractional digits, zero-padded). R89
// (auditor bug 56): RFC3339Nano DROPS trailing zeros, so lexicographic `ORDER BY ts` could sort
// "…05.5Z" AFTER "…05Z" within one second — a settlement SELL could replay before its BUY in
// every chronological walk (Stats/ClosedTrades/Aggregate). Fixed width keeps new rows
// lexicographically = chronologically ordered; the ts,id ORDER BY tiebreak covers legacy rows.
const tsLayout = "2006-01-02T15:04:05.000000000Z07:00"

func nowRFC() string { return time.Now().UTC().Format(tsLayout) }

// SaveCredential upserts the encrypted credential for an environment.
func (s *Store) SaveCredential(ctx context.Context, c Credential) error {
	now := nowRFC()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO credentials(environment,key_id,salt,nonce,ciphertext,created_at,updated_at)
VALUES(?,?,?,?,?,?,?)
ON CONFLICT(environment) DO UPDATE SET
  key_id=excluded.key_id, salt=excluded.salt, nonce=excluded.nonce,
  ciphertext=excluded.ciphertext, updated_at=excluded.updated_at;`,
		c.Environment, c.KeyID, c.Salt, c.Nonce, c.Ciphertext, now, now)
	return err
}

// LoadCredential returns the stored credential for env, or (nil, nil) if absent.
func (s *Store) LoadCredential(ctx context.Context, env string) (*Credential, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT environment,key_id,salt,nonce,ciphertext FROM credentials WHERE environment=?`, env)
	var c Credential
	if err := row.Scan(&c.Environment, &c.KeyID, &c.Salt, &c.Nonce, &c.Ciphertext); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &c, nil
}

// Audit appends a row to the audit log.
func (s *Store) Audit(ctx context.Context, level, category, message, detail string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO audit_log(ts,level,category,message,detail) VALUES(?,?,?,?,?)`,
		nowRFC(), level, category, message, detail)
	return err
}

// AuditEntry is one immutable audit row accepted by AuditBatch. TS may be empty; in that case the
// batch writer stamps the row at commit time. Keeping this small generic shape lets latency-
// sensitive producers serialize their payloads off their hot path and commit many receipts under
// one SQLite writer lock.
type AuditEntry struct {
	TS       string
	Level    string
	Category string
	Message  string
	Detail   string
}

// AuditBatch appends all rows in one transaction. It is all-or-nothing: a transient lock or a
// canceled context leaves the caller's batch intact so its queue worker can retry without either
// losing or duplicating an individual receipt.
func (s *Store) AuditBatch(ctx context.Context, entries []AuditEntry) (err error) {
	if len(entries) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO audit_log(ts,level,category,message,detail) VALUES(?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, entry := range entries {
		ts := strings.TrimSpace(entry.TS)
		if ts == "" {
			ts = nowRFC()
		}
		if _, err = stmt.ExecContext(ctx, ts, entry.Level, entry.Category, entry.Message, entry.Detail); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// AuditRow is one audit_log entry (read side, for the export).
type AuditRow struct {
	TS       string `json:"ts"`
	Level    string `json:"level"`
	Category string `json:"category"`
	Message  string `json:"message"`
	Detail   string `json:"detail"`
}

// ListAudit returns the most-recent audit_log entries (newest first), capped to limit (0 → 2000).
// AUDITLOG: feeds the export so the full action/tune/settle trail ships with the data dump.
func (s *Store) ListAudit(ctx context.Context, limit int) ([]AuditRow, error) {
	if limit <= 0 {
		limit = 2000
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT ts,level,category,message,COALESCE(detail,'') FROM audit_log ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditRow
	for rows.Next() {
		var a AuditRow
		if err := rows.Scan(&a.TS, &a.Level, &a.Category, &a.Message, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// InsertPaperFill records one simulated fill and returns its new row id. R122: fill_kind +
// route_reason ride every row (structured maker/taker tag — was note-text only).
func (s *Store) InsertPaperFill(ctx context.Context, f paper.Fill) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
INSERT INTO paper_fills(ts,platform,ticker,title,side,action,price,contracts,fee,tp_price,sl_price,source,note,sig_px_at_fill,fill_kind,route_reason)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		nowRFC(), f.Platform, f.Ticker, f.Title, f.Side, f.Action, f.Price, f.Contracts, f.Fee, f.TP, f.SL, f.Source, f.Note, f.SigPxAtFill, f.FillKind, f.RouteReason)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// ListPaperFills returns all paper fills, oldest first.
func (s *Store) ListPaperFills(ctx context.Context) ([]paper.Fill, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,ts,platform,ticker,title,side,action,price,contracts,fee,
       COALESCE(tp_price,0),COALESCE(sl_price,0),COALESCE(source,''),COALESCE(note,''),COALESCE(sig_px_at_fill,0),
       COALESCE(fill_kind,''),COALESCE(route_reason,'')
FROM paper_fills ORDER BY ts ASC, id ASC`) // R89 (bug 56): id tiebreak for same-stamp + legacy variable-width rows
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []paper.Fill
	for rows.Next() {
		var f paper.Fill
		if err := rows.Scan(&f.ID, &f.TS, &f.Platform, &f.Ticker, &f.Title, &f.Side,
			&f.Action, &f.Price, &f.Contracts, &f.Fee, &f.TP, &f.SL, &f.Source, &f.Note, &f.SigPxAtFill,
			&f.FillKind, &f.RouteReason); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// LatestPaperFillID is an O(1) rowid-tail probe used by the sub-second stop loop. It lets the
// server reuse its aggregated open-position generation while still noticing a new BUY/SELL within
// one tick, without rescanning or copying the historical fill ledger.
func (s *Store) LatestPaperFillID(ctx context.Context) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(id),0) FROM paper_fills`).Scan(&id)
	return id, err
}

// ListPaperFillsSince — R98 (the last storm gap): the briefing/stats fills cache refreshes with
// an O(delta) id-range read instead of re-scanning the whole table every 15s. Under a saturated
// disk (the R96/R97 3.5GB host-copy killer) the full scan blew its 8s attempts; a `id > ?` point
// range hits hot WAL/tail pages and answers in ms. Same column list + (ts,id) order as
// ListPaperFills; callers merge + re-sort. In-place stop edits (UpdatePositionStops) don't mint
// ids — the server side BUSTS its cache on that path.
func (s *Store) ListPaperFillsSince(ctx context.Context, sinceID int64) ([]paper.Fill, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id,ts,platform,ticker,title,side,action,price,contracts,fee,
       COALESCE(tp_price,0),COALESCE(sl_price,0),COALESCE(source,''),COALESCE(note,''),COALESCE(sig_px_at_fill,0),
       COALESCE(fill_kind,''),COALESCE(route_reason,'')
FROM paper_fills WHERE id > ? ORDER BY ts ASC, id ASC`, sinceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []paper.Fill
	for rows.Next() {
		var f paper.Fill
		if err := rows.Scan(&f.ID, &f.TS, &f.Platform, &f.Ticker, &f.Title, &f.Side,
			&f.Action, &f.Price, &f.Contracts, &f.Fee, &f.TP, &f.SL, &f.Source, &f.Note, &f.SigPxAtFill,
			&f.FillKind, &f.RouteReason); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// UpdatePositionStops sets take-profit / stop-loss (prices 0..1) on every open BUY
// fill of a position (platform+ticker+side). Aggregate takes the last BUY's levels,
// and updating all fills uniformly makes the edit authoritative; passing 0 clears.
func (s *Store) UpdatePositionStops(ctx context.Context, platform, ticker, side string, tp, sl float64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE paper_fills SET tp_price=?, sl_price=? WHERE platform=? AND ticker=? AND side=? AND action IN ('BUY','buy')`,
		tp, sl, platform, ticker, side)
	return err
}

// InsertParlay stores a multi-leg paper combo, including its route-specific entry-fee receipt.
func (s *Store) InsertParlay(ctx context.Context, p paper.Parlay) (int64, error) {
	legs, _ := json.Marshal(p.Legs)
	systemIDs, _ := json.Marshal(p.SystemIDs)
	res, err := s.db.ExecContext(ctx, `
INSERT INTO paper_parlays(ts,stake,price,contracts,tp,sl,status,legs,fees,route_source,cohort,system_ids,joint_p,expected_net_per_dollar,
canonical_system_id,combo_venue,leg_count,relation_class,producer_family,combo_route,experiment_epoch,combo_key)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		nowRFC(), p.Stake, p.Price, p.Contracts, p.TP, p.SL, "open", string(legs), p.Fees,
		p.RouteSource, p.Cohort, string(systemIDs), p.JointP, p.ExpectedNetPerDollar,
		p.CanonicalSystemID, p.ComboVenue, p.LegCount, p.RelationClass, p.ProducerFamily,
		p.ComboRoute, p.ExperimentEpoch, p.ComboKey)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

const paperParlaySelectColumns = `id,ts,stake,price,contracts,COALESCE(tp,0),COALESCE(sl,0),status,legs,
COALESCE(payout,0),COALESCE(realized,0),COALESCE(fees,0),COALESCE(settled_ts,''),COALESCE(artifact,0),
COALESCE(route_source,''),COALESCE(cohort,''),COALESCE(system_ids,'[]'),COALESCE(joint_p,0),
COALESCE(expected_net_per_dollar,0),COALESCE(canonical_system_id,''),COALESCE(combo_venue,''),
COALESCE(leg_count,0),COALESCE(relation_class,''),COALESCE(producer_family,''),
COALESCE(combo_route,''),COALESCE(experiment_epoch,''),COALESCE(combo_key,'')`

func scanPaperParlays(rows *sql.Rows) ([]paper.Parlay, error) {
	defer rows.Close()
	var out []paper.Parlay
	for rows.Next() {
		var p paper.Parlay
		var legs, systemIDs string
		var artifact int
		if err := rows.Scan(&p.ID, &p.TS, &p.Stake, &p.Price, &p.Contracts, &p.TP, &p.SL, &p.Status, &legs, &p.Payout, &p.Realized, &p.Fees, &p.SettledTS, &artifact,
			&p.RouteSource, &p.Cohort, &systemIDs, &p.JointP, &p.ExpectedNetPerDollar,
			&p.CanonicalSystemID, &p.ComboVenue, &p.LegCount, &p.RelationClass,
			&p.ProducerFamily, &p.ComboRoute, &p.ExperimentEpoch, &p.ComboKey); err != nil {
			return nil, err
		}
		p.Artifact = artifact != 0
		_ = json.Unmarshal([]byte(legs), &p.Legs)
		_ = json.Unmarshal([]byte(systemIDs), &p.SystemIDs)
		out = append(out, p)
	}
	return out, rows.Err()
}

// ListParlays returns parlays, newest first; pass status="open" to filter.
func (s *Store) ListParlays(ctx context.Context, status string) ([]paper.Parlay, error) {
	q := `SELECT ` + paperParlaySelectColumns + ` FROM paper_parlays`
	var args []any
	if status = strings.TrimSpace(status); status != "" {
		q += ` WHERE status=?`
		args = append(args, status)
	}
	q += ` ORDER BY ts DESC, id DESC` // R89 (bug 56): id tiebreak
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanPaperParlays(rows)
}

// ListParlaysByRouteCohort is the indexed reader for independently funded combo portfolios.
// Exact route+cohort ownership is mandatory; blank values are rejected rather than accidentally
// widening the query to legacy/generic rows. status="" includes every lifecycle state.
func (s *Store) ListParlaysByRouteCohort(ctx context.Context, route, cohort, status string) ([]paper.Parlay, error) {
	route, cohort, status = strings.TrimSpace(route), strings.TrimSpace(cohort), strings.TrimSpace(status)
	if route == "" || cohort == "" {
		return nil, fmt.Errorf("parlay route and cohort are required")
	}
	q := `SELECT ` + paperParlaySelectColumns + ` FROM paper_parlays WHERE route_source=? AND cohort=?`
	args := []any{route, cohort}
	if status != "" {
		q += ` AND status=?`
		args = append(args, status)
	}
	q += ` ORDER BY ts DESC, id DESC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return scanPaperParlays(rows)
}

// ParlayRouteCohortFunds is a one-query, JSON-free view of one funded combo portfolio's current
// epoch. OpenCommitted is principal plus entry fees currently tied up. Realized and ClosedCount
// include only economically terminal, non-artifact rows; reset_void rows never become P&L.
type ParlayRouteCohortFunds struct {
	Realized float64
	// ClosedContracts is the sum of economically settled package contracts. It lets compact
	// reporting preserve the same cents/unit denominator as the full JSON row reader without
	// decoding every leg payload.
	ClosedContracts float64
	OpenStake       float64
	OpenFees        float64
	OpenCommitted   float64
	OpenCount       int
	ClosedCount     int
}

// RouteCohortParlayFunds aggregates one exact route/cohort after its durable epoch boundary.
// A positive afterID is authoritative; otherwise a non-zero epoch timestamp is used. Passing
// neither returns lifetime totals for that exact portfolio.
func (s *Store) RouteCohortParlayFunds(ctx context.Context, route, cohort string, afterID int64, epoch time.Time) (ParlayRouteCohortFunds, error) {
	var out ParlayRouteCohortFunds
	route, cohort = strings.TrimSpace(route), strings.TrimSpace(cohort)
	if route == "" || cohort == "" || afterID < 0 {
		return out, fmt.Errorf("invalid parlay route/cohort epoch")
	}
	q := `SELECT
COALESCE(SUM(CASE WHEN status IN ('settled','closed') AND COALESCE(artifact,0)=0 THEN realized ELSE 0 END),0),
COALESCE(SUM(CASE WHEN status IN ('settled','closed') AND COALESCE(artifact,0)=0 THEN contracts ELSE 0 END),0),
COALESCE(SUM(CASE WHEN status='open' AND COALESCE(artifact,0)=0 THEN stake ELSE 0 END),0),
COALESCE(SUM(CASE WHEN status='open' AND COALESCE(artifact,0)=0 THEN fees ELSE 0 END),0),
COALESCE(SUM(CASE WHEN status='open' AND COALESCE(artifact,0)=0 THEN stake+fees ELSE 0 END),0),
COALESCE(SUM(CASE WHEN status='open' AND COALESCE(artifact,0)=0 THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN status IN ('settled','closed') AND COALESCE(artifact,0)=0 THEN 1 ELSE 0 END),0)
FROM paper_parlays WHERE route_source=? AND cohort=?`
	args := []any{route, cohort}
	if afterID > 0 {
		q += ` AND id>?`
		args = append(args, afterID)
	} else if !epoch.IsZero() {
		q += ` AND ts>=?`
		args = append(args, epoch.UTC().Format(tsLayout))
	}
	if err := s.db.QueryRowContext(ctx, q, args...).Scan(&out.Realized, &out.ClosedContracts, &out.OpenStake,
		&out.OpenFees, &out.OpenCommitted, &out.OpenCount, &out.ClosedCount); err != nil {
		return ParlayRouteCohortFunds{}, err
	}
	return out, nil
}

// ResetParlayPnL clears the parlay book's realized P&L history by deleting all SETTLED parlays (open
// parlays are kept). Paper-only bookkeeping reset — returns how many were removed.
func (s *Store) ResetParlayPnL(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM paper_parlays WHERE status='settled'`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ResetParlayPnLExceptRouteCohort resets the legacy/System Combo book while preserving a separately
// funded combo portfolio that shares paper_parlays for settlement durability.
func (s *Store) ResetParlayPnLExceptRouteCohort(ctx context.Context, route, cohort string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM paper_parlays
WHERE status IN ('settled','closed') AND NOT (route_source=? AND cohort=?)`,
		strings.TrimSpace(route), strings.TrimSpace(cohort))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// finishOpenParlay is the single atomic lifecycle boundary for a funded Paper combo. The
// status='open' predicate makes settlement, early exit and reset mutually exclusive even when
// their callers race. A linked relation receipt is finalized in the SAME transaction, so a
// database error cannot leave the combo terminal while its relation remains falsely open.
func (s *Store) finishOpenParlay(ctx context.Context, id int64, parlayStatus, relationStatus string,
	payout, realized float64, relationSource string) (bool, error) {
	if id <= 0 || (parlayStatus != "settled" && parlayStatus != "closed" && parlayStatus != "reset_void") ||
		(relationStatus != "settled" && relationStatus != "cancelled") || strings.TrimSpace(relationSource) == "" ||
		math.IsNaN(payout) || math.IsInf(payout, 0) || math.IsNaN(realized) || math.IsInf(realized, 0) {
		return false, fmt.Errorf("invalid parlay terminal transition")
	}
	stamp := nowRFC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`UPDATE paper_parlays SET status=?, payout=?, realized=?, settled_ts=? WHERE id=? AND status='open'`,
		parlayStatus, payout, realized, stamp, id)
	if err != nil {
		return false, err
	}
	changed, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if changed == 0 {
		return false, tx.Commit()
	}
	// A reset is a cancellation with zero P&L, not a fabricated settlement or early-exit return.
	// INSERT OR IGNORE preserves the append-only, one-terminal-outcome receipt contract.
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO funded_relation_outcomes(
receipt_id,outcome_status,settled_ts,pnl_dollars,result_source,created_ts)
SELECT l.receipt_id,?,?,?,?,? FROM funded_relation_position_links l
JOIN funded_relation_receipts r ON r.receipt_id=l.receipt_id AND r.allowed=1
WHERE l.position_kind='parlay' AND l.position_id=?`, relationStatus, stamp, realized,
		strings.TrimSpace(relationSource), stamp, id); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// SettleParlayIfOpen marks a still-open parlay settled and reports whether this call won the
// terminal transition. payout is the exact 0..1 product outcome; realized is fee-net dollars.
func (s *Store) SettleParlayIfOpen(ctx context.Context, id int64, payout, realized float64) (bool, error) {
	return s.finishOpenParlay(ctx, id, "settled", "settled", payout, realized, "combo-settlement")
}

// SettleParlay retains the established API while inheriting open-only, race-safe semantics.
// New callers that must distinguish a raced/no-op settlement use SettleParlayIfOpen.
func (s *Store) SettleParlay(ctx context.Context, id int64, payout, realized float64) error {
	_, err := s.SettleParlayIfOpen(ctx, id, payout, realized)
	return err
}

// VoidParlayForReset removes an open combo from exposure without manufacturing an economic
// close. It records a zero-dollar relation cancellation (not a settlement result), preserving
// dependency provenance without allowing the reset to enter P&L or system evidence.
func (s *Store) VoidParlayForReset(ctx context.Context, id int64) (bool, error) {
	return s.finishOpenParlay(ctx, id, "reset_void", "cancelled", 0, 0, "combo-reset-void")
}

// FlagParlayArtifact marks one parlay row as an artifact (excluded from headline P&L, curves and
// backtests — same flag the v5 churn quarantine uses). Settle fuse for blown-up maintenance rows.
func (s *Store) FlagParlayArtifact(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE paper_parlays SET artifact=1 WHERE id=?`, id)
	return err
}

// ResolvedYes returns a ticker's settled YES value (0..1) if it has resolved in signal_log, else ok=false.
// Powers parlay settlement (a leg's value = yes if side YES, else 1−yes). Prefers the exact scalar
// settle_val; falls back to the binary won flag — which is SIDE-RELATIVE (audit F13): a NO-side row
// with won=1 means the YES value was 0, and an outcome-label side (poly teams, "Up"/"Down") carries
// no YES semantics at all, so those return ok=false rather than guessing (the caller then resolves
// against the live venue instead of settling the leg inverted).
func (s *Store) ResolvedYes(ctx context.Context, ticker string) (float64, bool) {
	var sv sql.NullFloat64
	var won sql.NullInt64
	var side sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT settle_val, won, side FROM signal_log WHERE ticker=? AND resolved=1 ORDER BY ts DESC, id DESC LIMIT 1`, ticker).Scan(&sv, &won, &side)
	if err != nil {
		return 0, false
	}
	if sv.Valid && sv.Float64 >= 0 {
		return sv.Float64, true // exact scalar/binary settled YES value
	}
	if won.Valid && side.Valid {
		switch strings.ToUpper(strings.TrimSpace(side.String)) {
		case "YES", "UP": // side aligned with YES
			if won.Int64 >= 1 {
				return 1, true
			}
			return 0, true
		case "NO", "DOWN": // side is the inverse of YES — a NO win means YES settled 0
			if won.Int64 >= 1 {
				return 0, true
			}
			return 1, true
		}
	}
	return 0, false
}

// ResolvedLeg is one historical, settled market used to backtest parlays: its venue, entry price,
// whether the logged side won, plus the signal timestamp and ticker (audit Q4: the backtest must
// draw TIME-COHERENT leg sets — legs actually open simultaneously — and must never pair two legs
// from the same market; the old shape could express neither).
type ResolvedLeg struct {
	Platform string
	Price    float64
	Won      bool
	TS       string
	Ticker   string
}

// ListResolvedLegs returns recent RESOLVED signals (entry price in [minP,maxP], known win/loss) for the
// parlay backtest — the raw legs combos are drawn from. Newest first, capped at `limit`.
func (s *Store) ListResolvedLegs(ctx context.Context, minP, maxP float64, limit int) ([]ResolvedLeg, error) {
	if limit <= 0 {
		limit = 4000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT platform, entry_price, won, ts, ticker FROM signal_log
WHERE resolved=1 AND won IN (0,1) AND entry_price>=? AND entry_price<=?
ORDER BY ts DESC LIMIT ?`, minP, maxP, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResolvedLeg
	for rows.Next() {
		var plat, ts, tick sql.NullString
		var price sql.NullFloat64
		var won sql.NullInt64
		if err := rows.Scan(&plat, &price, &won, &ts, &tick); err != nil {
			return nil, err
		}
		out = append(out, ResolvedLeg{Platform: plat.String, Price: price.Float64, Won: won.Int64 >= 1, TS: ts.String, Ticker: tick.String})
	}
	return out, rows.Err()
}

// Signal is one logged signal observation for the backtest. The feature fields are
// optional — 0 means "not applicable to this signal type" (e.g. Kalshi is anonymous,
// so it has no Rank/Concentration/TraderPnL). They exist so we can later discover which
// conditions actually predict winners and fit thresholds from data instead of guessing.
type Signal struct {
	Platform   string
	Ticker     string
	Title      string
	Side       string // YES | NO | outcome label
	SignalType string // kalshi-flow | kalshi-whale | poly-whale | poly-consensus | polyus-flow | polyus-whale | poly-pred-kalshi
	EntryPrice float64
	// feature vector (0 = not applicable)
	Strength      float64 // flow one-sidedness 0..1
	Notional      float64 // $ size of the flow / position
	Rank          int     // best leaderboard rank in the flow (poly; 0 = none/anon)
	TraderCount   int     // distinct whales on that side (poly-consensus)
	TraderPnL     float64 // combined all-time PnL of those whales (poly)
	Concentration float64 // group $ ÷ portfolio = conviction (poly-consensus)
	Momentum      float64 // recent price move over the window (kalshi)
	SpreadCents   float64 // bid/ask spread in cents at signal time (book quality)
	ResolveHours  float64 // hours until the market resolves at signal time
	Imbalance     float64 // resting order-book imbalance (-1..1)
	Underlying    float64 // underlying spot price at signal time (crypto signals)
	PricePath     string  // JSON array of the market's recent price path (prices-history), "" if none
	Category      string  // market category from its first tag (Sports/Crypto/Politics/…); "" if unknown
	Confidence    float64 // betConfidence at signal time: session win-rate(signal_type) × mid-price quality (the gate's score)
	BookDepth     float64 // resting size at the touch (top-of-book) at signal time; 0 = not captured — feeds maker-fill-odds modeling
	TraderSkill   float64 // WHALE V2 (audit Q2): acting wallet's shrunk market-level skill (replaces raw profit as the ML feature)
	MarketType    string  // TYPE LOGGING (audit Q3): granular market type (Moneyline/Spread/Total 2.5/Crypto Threshold/…)
	Kind          string  // R72-B: coarse market kind — winner | total | spread | prop | unknown (stable sidecar vocab)
	IsLive        int     // R38: 1 = market in-play at signal time, 0 = pre/stable, -1 = unknown
	// R65 feature families — POINTERS so "not measurable" stays NULL in the DB (readers COALESCE),
	// because 0 is a MEANINGFUL value for every one of these (flat momentum, at-the-extreme, …):
	BookImb3    *float64 // top-3-level bid share of top-3 (bid+ask) depth, 0..1 (kalshi book cache; polyus/poly-int nil)
	BookDepth3  *float64 // summed top-3 resting depth, both sides, contracts
	Mom1h       *float64 // YES-price move over ~1h in cents (momentum ring; nil = ring too young/absent)
	Mom4h       *float64 // YES-price move over ~4h in cents
	DistHi      *float64 // cents below the ring-scope session high (ring-scope, NOT venue 24h)
	DistLo      *float64 // cents above the ring-scope session low
	SecsToStart *int64   // seconds until event start; NEGATIVE once started; nil = no start time known
	// R67l taker-flow family (tapes already ingested — no new subscriptions):
	FlowRatio15m *float64 // taker BUY-YES share of taker notional on this ticker, last 15m, 0..1; nil = no tape
	FlowN15m     *int64   // taker trade count behind the ratio; nil = no tape
	// R70-B SCHEMA_AUDIT families (stamped centrally in insertSignal, all cache-only — nil = NULL):
	HoldersHHI   *float64 // #4 poly-int: HHI over top-20 holder amounts, 0..1 (single-whale vs crowd)
	HoldersSkill *float64 // #4 poly-int: skill-weighted holder lean in YES-space (Σ share×shrunk×dir)
	InPlay       *int64   // #8 polyus: 1 = venue-reported LIVE game state, 0 = known pre-game
	ScoreMargin  *float64 // #8 polyus: |scoreA−scoreB| lead size while in-play
	OIDelta1h    *float64 // #7 kalshi: open-interest build/unwind over ~1h (contracts, ticker-WS OI ring)
	// R84 (auditor DO-THIS 8): the ML sidecar's p_win for (ticker, side) at insert time — stamped
	// centrally in server.insertSignal from the ml_predictions cache; nil = model hadn't scored it.
	// Audit/calibration only; NEVER selected as a training feature (circularity).
	ModelProb *float64
	// R132 exact one-contract entry fee captured from the venue's active schedule at decision
	// time. NULL on legacy rows; verdict queries use a venue-specific conservative fallback.
	FeePC *float64
	// R126 both-side gap scanning: gap-episode ordinal (Part 2 re-entry grading) and the
	// execution-expression tag (Part 1: pus_yes | pus_no | k_yes | k_no with ":best"/":alt"
	// and ":flip" for R106 flip-twin pairs). Zero-values for every non-gap family.
	Episode  int
	ExecExpr string
	// R114 (ML review): decision-price age in seconds — age of the market snapshot the signal
	// priced off at insert time (kalshi kmkts cache). nil = unmeasured. Stamped centrally.
	PxAgeS *float64
	// R133 book-native ML cohort. Pointer fields preserve missingness. BookFeatureVer stays zero
	// unless the server captured a complete executable snapshot; entry_price remains legacy input.
	BookFeatureVer int
	BookBid        *float64
	BookAsk        *float64
	BookBidDepth   *float64
	BookAskDepth   *float64
	BookQuoteAgeS  *float64
	BookMakerTick  *float64
	BookTakerTick  *float64
	BookMakerFeePC *float64
	BookTakerFeePC *float64
	BookLatencyMS  *float64
	BookSource     string
	LabelVersion   string // prospective liveness/outcome labeling contract; blank = legacy/untrusted
	PricingVersion string // executable pricing contract; blank = legacy/untrusted
}

// signalInputTopology extracts the exact topology receipt without teaching storage about any
// particular venue vocabulary. The server validates supported values before insertion.
func signalInputTopology(execExpr string) string {
	for _, token := range strings.FieldsFunc(execExpr, func(r rune) bool {
		return r == '/' || r == ';' || r == ' ' || r == '\t' || r == '\r' || r == '\n'
	}) {
		if value, ok := strings.CutPrefix(strings.TrimSpace(token), "input="); ok {
			return strings.ToUpper(strings.TrimSpace(value))
		}
	}
	return ""
}

// InsertSignal logs a signal once per (~10-min slot, platform, ticker, side, type, input
// topology) — the unique index
// makes repeat observations within a slot no-ops, but a NEW slot logs a fresh snapshot, so we
// capture how the signal's features evolve through the market's life. Safe to call freely.
func (s *Store) InsertSignal(ctx context.Context, sig Signal) error {
	_, err := s.InsertSignalResult(ctx, sig)
	return err
}

// InsertSignalResult is InsertSignal plus the INSERT OR IGNORE outcome. Server-side strategy
// hooks use inserted=false to avoid treating a duplicate observation as fresh evidence.
func (s *Store) InsertSignalResult(ctx context.Context, sig Signal) (inserted bool, err error) {
	now := nowRFC()
	day := now
	if len(now) >= 10 {
		day = now[:10]
	}
	slot := now
	if len(now) >= 15 {
		slot = now[:15] // "2026-06-26T07:3" → ~10-minute bucket
	}
	ins := func() (bool, error) {
		res, e := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO signal_log(ts,day,slot,platform,ticker,title,side,signal_type,entry_price,
  strength,notional,best_rank,trader_count,trader_pnl,concentration,momentum,spread_cents,resolve_hours,imbalance,underlying,price_path,category,confidence,book_depth,trader_skill,market_type,kind,is_live,
  book_imb3,book_depth3,mom_1h,mom_4h,dist_hi,dist_lo,secs_to_start,flow_ratio_15m,flow_n_15m,
  holders_hhi,holders_skill,in_play,score_margin,oi_delta_1h,model_prob,fee_pc,px_age_s,
  book_feature_ver,book_bid,book_ask,book_bid_depth,book_ask_depth,book_quote_age_s,
  book_maker_tick,book_taker_tick,book_maker_fee_pc,book_taker_fee_pc,book_latency_ms,book_source,
  label_version,pricing_version,episode,exec_expr,input_topology,resolved)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)`,
			now, day, slot, sig.Platform, sig.Ticker, sig.Title, sig.Side, sig.SignalType, sig.EntryPrice,
			sig.Strength, sig.Notional, sig.Rank, sig.TraderCount, sig.TraderPnL, sig.Concentration, sig.Momentum,
			sig.SpreadCents, sig.ResolveHours, sig.Imbalance, sig.Underlying, sig.PricePath, sig.Category, sig.Confidence, sig.BookDepth, sig.TraderSkill, sig.MarketType, sig.Kind, sig.IsLive,
			sig.BookImb3, sig.BookDepth3, sig.Mom1h, sig.Mom4h, sig.DistHi, sig.DistLo, sig.SecsToStart, sig.FlowRatio15m, sig.FlowN15m,
			sig.HoldersHHI, sig.HoldersSkill, sig.InPlay, sig.ScoreMargin, sig.OIDelta1h, sig.ModelProb, sig.FeePC, sig.PxAgeS,
			sig.BookFeatureVer, sig.BookBid, sig.BookAsk, sig.BookBidDepth, sig.BookAskDepth, sig.BookQuoteAgeS,
			sig.BookMakerTick, sig.BookTakerTick, sig.BookMakerFeePC, sig.BookTakerFeePC, sig.BookLatencyMS, sig.BookSource,
			sig.LabelVersion, sig.PricingVersion, sig.Episode, sig.ExecExpr,
			signalInputTopology(sig.ExecExpr)) // nullable features land as NULL
		if e != nil {
			return false, e
		}
		n, e := res.RowsAffected()
		return n > 0, e
	}
	inserted, err = ins()
	// R99 bug 196 (auditor r25): every producer discards this error, so ONE transient BUSY used to
	// be a silent observation hole (and latch-before-insert families turned it into a whole-window
	// gap). One bounded retry on the busy/locked class only; a dead ctx or a second failure still
	// surfaces to the caller.
	if err != nil && ctx.Err() == nil {
		if low := strings.ToLower(err.Error()); strings.Contains(low, "busy") || strings.Contains(low, "locked") {
			select {
			case <-time.After(150 * time.Millisecond):
				inserted, err = ins()
			case <-ctx.Done():
			}
		}
	}
	return inserted, err
}

// NullModelProbRow is one recent signal row still lacking a model_prob stamp (R90 DO-THIS 14).
type NullModelProbRow struct {
	ID     int64
	Ticker string
	Side   string
}

// ListNullModelProb — R90 DO-THIS 14 (model_prob backfill <15min): rows inserted before the
// sidecar scored their market (insertSignal cache-miss → NULL) stay NULL forever without this.
// Bounded, newest-first; the caller matches against the live predictions cache.
func (s *Store) ListNullModelProb(ctx context.Context, sinceTS string, limit int) ([]NullModelProbRow, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, ticker, side FROM signal_log
WHERE model_prob IS NULL AND ts >= ? ORDER BY id DESC LIMIT ?`, sinceTS, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NullModelProbRow
	for rows.Next() {
		var r NullModelProbRow
		if err := rows.Scan(&r.ID, &r.Ticker, &r.Side); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetModelProb stamps a late-arriving sidecar p_win onto one row — backfill only, never
// overwrites an existing stamp (model_prob is an insert-time calibration column by contract).
func (s *Store) SetModelProb(ctx context.Context, id int64, p float64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE signal_log SET model_prob=? WHERE id=? AND model_prob IS NULL`, p, id)
	return err
}

// UpdateSignalFill stamps the ACTUAL fill price onto the most recent still-open, not-yet-filled signal
// row matching this market+side — called when a signal becomes a paper bet. This is what lets the ML
// train on real fills instead of the (rosier) signal-time price. Best-effort: no match = no-op.
func (s *Store) UpdateSignalFill(ctx context.Context, platform, ticker, side string, fillPrice float64) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET fill_price=?
WHERE id = (SELECT id FROM signal_log
            WHERE platform=? AND ticker=? AND side=? AND resolved=0 AND fill_price=0
            ORDER BY id DESC LIMIT 1)`,
		fillPrice, platform, ticker, side)
	return err
}

// UpdateSignalFamilyFill is the exact-family form used when multiple systems observe the same
// market and side. It prevents an independently named inverse fill from being stamped onto a
// newer unrelated signal row.
func (s *Store) UpdateSignalFamilyFill(ctx context.Context, platform, ticker, side, family string, fillPrice float64) error {
	_, err := s.UpdateSignalFamilyFillResult(ctx, platform, ticker, side, family, fillPrice)
	return err
}

// UpdateSignalFamilyFillResult is the checked form used by dedicated executors.  A nil error is
// not enough for those routes: zero affected rows would leave a real Paper lot attributed to no
// independently named system.  Returning the affected-row bit lets the executor fail closed.
func (s *Store) UpdateSignalFamilyFillResult(ctx context.Context, platform, ticker, side, family string, fillPrice float64) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET fill_price=?
WHERE id = (SELECT id FROM signal_log
            WHERE platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=?
              AND resolved=0 AND fill_price=0
            ORDER BY id DESC LIMIT 1)`,
		fillPrice, platform, ticker, side, family)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// UpdateSignalFamilyExecutionFill is the dedicated-executor form: besides the actual fill it
// replaces the one-share observation fee with the realized sized fee per contract. This keeps the
// named system's settled economics tied to the order it actually simulated.
func (s *Store) UpdateSignalFamilyExecutionFill(ctx context.Context, platform, ticker, side, family string, fillPrice, feePC float64, signalTS string, includeResolved bool) (bool, error) {
	if strings.TrimSpace(signalTS) == "" {
		return false, nil
	}
	resolvedClause := "AND resolved=0"
	if includeResolved {
		resolvedClause = "AND resolved IN (0,1)"
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET fill_price=?, fee_pc=?
WHERE id = (SELECT id FROM signal_log
            WHERE platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=?
			  AND fill_price=0 `+resolvedClause+`
			  AND ABS((julianday(ts)-julianday(?))*86400.0)<=5.0
			ORDER BY ABS((julianday(ts)-julianday(?))*86400.0) ASC, id DESC LIMIT 1)`,
		fillPrice, feePC, platform, ticker, side, family, signalTS, signalTS)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// SignalFamilyFillMatches verifies the durable join between a Paper lot and its exact named
// system. It deliberately includes venue and side so same-market freshlist, freshfade and their
// independently quoted inverses can never satisfy one another's receipt.
func (s *Store) SignalFamilyFillMatches(ctx context.Context, platform, ticker, side, family string, fillPrice float64) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM signal_log
WHERE platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=?
	AND resolved=0 AND fill_price>0 AND ABS(fill_price-?)<0.0000001`,
		platform, ticker, side, family, fillPrice).Scan(&n)
	return n > 0, err
}

// SignalFamilyFillMatchesAt is the crash-recovery form. Settlement can race ahead of a file-book
// fold, so it may include a resolved row, but only from the same five-second decision receipt as
// the lot. A historical lifecycle/reopen row with the same ticker, family, side and price cannot
// satisfy a new lot.
func (s *Store) SignalFamilyFillMatchesAt(ctx context.Context, platform, ticker, side, family string, fillPrice float64, signalTS string, includeResolved bool) (bool, error) {
	if strings.TrimSpace(signalTS) == "" {
		return false, nil
	}
	resolvedClause := "AND resolved=0"
	if includeResolved {
		resolvedClause = "AND resolved IN (0,1)"
	}
	var n int
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM signal_log
WHERE platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=?
  AND fill_price>0 AND ABS(fill_price-?)<0.0000001 `+resolvedClause+`
  AND ABS((julianday(ts)-julianday(?))*86400.0)<=5.0`,
		platform, ticker, side, family, fillPrice, signalTS).Scan(&n)
	return n > 0, err
}

// RefreshSignalFamilyExecutionReceipt replaces a same-slot, still-unfilled collector observation
// with the complete book snapshot that actually authorized a dedicated Paper decision. This is the
// only safe duplicate path: a stale same-slot row may not silently inherit a newer fill.
func (s *Store) RefreshSignalFamilyExecutionReceipt(ctx context.Context, sig Signal) (bool, error) {
	now := nowRFC()
	slot := now
	if len(slot) >= 15 {
		slot = slot[:15]
	}
	day := now
	if len(day) >= 10 {
		day = day[:10]
	}
	res, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET ts=?,day=?,entry_price=?, fee_pc=?, spread_cents=?, book_depth=?,
  resolve_hours=?,px_age_s=?,secs_to_start=?,is_live=?,market_type=?,kind=?,category=?,
  book_feature_ver=?,book_bid=?,book_ask=?,book_bid_depth=?,book_ask_depth=?,book_quote_age_s=?,
  book_maker_tick=?,book_taker_tick=?,book_maker_fee_pc=?,book_taker_fee_pc=?,book_latency_ms=?,
  book_source=?,label_version=?,pricing_version=?,exec_expr=?
WHERE id=(SELECT id FROM signal_log
  WHERE slot=? AND platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=?
    AND resolved=0 AND fill_price=0 ORDER BY id DESC LIMIT 1)`,
		now, day, sig.EntryPrice, sig.FeePC, sig.SpreadCents, sig.BookDepth,
		sig.ResolveHours, sig.PxAgeS, sig.SecsToStart, sig.IsLive, sig.MarketType, sig.Kind, sig.Category,
		sig.BookFeatureVer, sig.BookBid, sig.BookAsk, sig.BookBidDepth, sig.BookAskDepth, sig.BookQuoteAgeS,
		sig.BookMakerTick, sig.BookTakerTick, sig.BookMakerFeePC, sig.BookTakerFeePC, sig.BookLatencyMS,
		sig.BookSource, sig.LabelVersion, sig.PricingVersion, sig.ExecExpr,
		slot, sig.Platform, sig.Ticker, sig.Side, sig.SignalType)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// SignalFillCoverage reports fill-price label coverage on signal_log — rows carrying a REAL fill
// price vs total (R65 4d: the before/after metric for the retro sweep). Read-only, one aggregate scan.
func (s *Store) SignalFillCoverage(ctx context.Context) (labeled, total int64, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(CASE WHEN fill_price > 0 THEN 1 ELSE 0 END),0), COUNT(*) FROM signal_log`).
		Scan(&labeled, &total)
	return
}

// BackfillSignalFills (R65 4c): retro sweep that stamps REAL fill prices from the newest maxFills
// stored BUY fills onto historical signal rows that never got a label. The live path
// (UpdateSignalFill) marks only the single newest OPEN row at fill time — rows that had already
// resolved, the other ~10-min slots of the same traded market, and anything missed across restarts
// stayed at the (rosier) signal-time price forever. Match: same platform+ticker+side with
// |fill.ts − signal.ts| ≤ 60 min; the CLOSEST-in-time fill wins. Idempotent (only fill_price=0 rows
// are touched — a matching fill always exists for selected rows, so the correlated subquery can't
// write NULL) and bounded; the temp table is pinned to ONE pooled connection so pool rotation can't
// strand it. Returns how many rows gained a real-fill label.
func (s *Store) BackfillSignalFills(ctx context.Context, maxFills int) (int64, error) {
	if maxFills <= 0 {
		maxFills = 5000
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `DROP TABLE IF EXISTS temp._fill_bf`); err != nil {
		return 0, err
	}
	if _, err := conn.ExecContext(ctx, `
CREATE TEMP TABLE _fill_bf AS
SELECT platform, ticker, UPPER(side) AS side, price, ts
FROM paper_fills WHERE action='BUY' AND price > 0 AND price < 1
ORDER BY id DESC LIMIT `+strconv.Itoa(maxFills)); err != nil {
		return 0, err
	}
	// Index the fill set so both the row-selection JOIN and the per-row closest-fill probe are seeks.
	if _, err := conn.ExecContext(ctx, `CREATE INDEX _fill_bf_key ON _fill_bf(platform, ticker, side)`); err != nil {
		return 0, err
	}
	// "Closest fill wins" via a correlated MIN — NOT via ORDER BY in the scalar subquery: SQLite
	// does not resolve references to the UPDATE target inside a subquery's ORDER BY ("no such
	// column: signal_log.ts"), while WHERE/SELECT-list correlation is fine (verified on a replica).
	res, err := conn.ExecContext(ctx, `
UPDATE signal_log SET fill_price = (
   SELECT b.price FROM _fill_bf b
   WHERE b.platform = signal_log.platform AND b.ticker = signal_log.ticker
     AND b.side = UPPER(signal_log.side)
     AND ABS(julianday(b.ts) - julianday(signal_log.ts)) = (
        SELECT MIN(ABS(julianday(b2.ts) - julianday(signal_log.ts)))
        FROM _fill_bf b2
        WHERE b2.platform = signal_log.platform AND b2.ticker = signal_log.ticker
          AND b2.side = UPPER(signal_log.side)
          AND ABS(julianday(b2.ts) - julianday(signal_log.ts)) <= 60.0/1440.0)
   LIMIT 1)
WHERE fill_price = 0 AND id IN (
   SELECT s.id FROM _fill_bf b
   JOIN signal_log s ON s.platform = b.platform AND s.ticker = b.ticker
    AND UPPER(s.side) = b.side AND s.fill_price = 0
    AND ABS(julianday(b.ts) - julianday(s.ts)) <= 60.0/1440.0)`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	_, _ = conn.ExecContext(ctx, `DROP TABLE IF EXISTS temp._fill_bf`)
	return n, nil
}

// AppendSignalPostPath tacks the current mark price onto the post-ENTRY price path of the open, filled
// signal row for this market+side — called each marking tick while a position is held, so we can later
// backtest exits (SL/TP) on the path AFTER entry. Comma-separated, appended in SQL (no read-modify-write),
// and capped (length guard) so a long hold can't bloat the row.
func (s *Store) AppendSignalPostPath(ctx context.Context, platform, ticker, side string, price float64) error {
	v := strconv.FormatFloat(price, 'f', 3, 64)
	_, err := s.db.ExecContext(ctx, `
UPDATE signal_log
SET post_path = CASE WHEN post_path='' THEN ? ELSE post_path || ',' || ? END
WHERE id = (SELECT id FROM signal_log
            WHERE platform=? AND ticker=? AND side=? AND resolved=0 AND fill_price>0 AND length(post_path) < 1200
            ORDER BY id DESC LIMIT 1)`,
		v, v, platform, ticker, side)
	return err
}

// AppendSignalFamilyPostPath keeps CLV/mark history attached to the exact named system whose Paper
// position is being marked, rather than whichever same-side detector happened to write last.
func (s *Store) AppendSignalFamilyPostPath(ctx context.Context, platform, ticker, side, family string, price float64) error {
	_, err := s.AppendSignalFamilyPostPathResult(ctx, platform, ticker, side, family, price)
	return err
}

// AppendSignalFamilyPostPathResult lets a dedicated executor distinguish a durable exact-family
// breadcrumb from a no-op caused by the wrong family/side identity.
func (s *Store) AppendSignalFamilyPostPathResult(ctx context.Context, platform, ticker, side, family string, price float64) (bool, error) {
	v := strconv.FormatFloat(price, 'f', 3, 64)
	res, err := s.db.ExecContext(ctx, `
UPDATE signal_log
SET post_path = CASE WHEN post_path='' THEN ? ELSE post_path || ',' || ? END
WHERE id = (SELECT id FROM signal_log
            WHERE platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=?
              AND resolved=0 AND fill_price>0 AND length(post_path) < 1200
            ORDER BY id DESC LIMIT 1)`,
		v, v, platform, ticker, side, family)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// AppendSignalFamilyPostPathAtResult is the dedicated-executor form. The decision timestamp is
// part of the join so a lifecycle-reopened ticker cannot lend its path to an older position.
func (s *Store) AppendSignalFamilyPostPathAtResult(ctx context.Context, platform, ticker, side, family string, price float64, signalTS string) (bool, error) {
	if strings.TrimSpace(signalTS) == "" {
		return false, nil
	}
	v := strconv.FormatFloat(price, 'f', 3, 64)
	res, err := s.db.ExecContext(ctx, `
UPDATE signal_log
SET post_path = CASE WHEN post_path='' THEN ? ELSE post_path || ',' || ? END
WHERE id = (SELECT id FROM signal_log
            WHERE platform=? AND ticker=? AND UPPER(side)=UPPER(?) AND signal_type=?
              AND resolved=0 AND fill_price>0 AND length(post_path) < 1200
              AND ABS((julianday(ts)-julianday(?))*86400.0)<=5.0
            ORDER BY ABS((julianday(ts)-julianday(?))*86400.0) ASC, id DESC LIMIT 1)`,
		v, v, platform, ticker, side, family, signalTS, signalTS)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// PathNeed is a resolved signal row missing its post-entry price path (a backfill candidate).
type PathNeed struct {
	ID     int64
	Ticker string
	Side   string
	TS     string
}

// ResolvedNeedPath returns resolved signal rows on a platform that have NO post_path yet — the
// candidates for candle-based backfill (so the SL/TP backtest can use history, not just live
// breadcrumbs). R72-B (QUANT_STUDY rec #4): WIDENED from filled-only to ALL resolved tradeable
// rows — the exit-ladder / SL-10¢ questions were starving on ~1.6k filled paths while ~138k
// resolved signal rows had reconstructable candle paths. Fill-priced rows still come FIRST
// (adversely-selected but highest-value labels), then newest-first; the caller's per-run cap
// keeps the API cost identical.
func (s *Store) ResolvedNeedPath(ctx context.Context, platform string, limit int) ([]PathNeed, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, ticker, side, ts FROM signal_log
WHERE platform=? AND resolved=1 AND post_path=''
ORDER BY (fill_price>0) DESC, id DESC LIMIT ?`, platform, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PathNeed
	for rows.Next() {
		var p PathNeed
		if err := rows.Scan(&p.ID, &p.Ticker, &p.Side, &p.TS); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// SetSignalPostPathByID writes a reconstructed post_path onto a specific row (backfill), only if it's
// still empty (never clobbers a real logged breadcrumb path).
func (s *Store) SetSignalPostPathByID(ctx context.Context, id int64, path string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE signal_log SET post_path=? WHERE id=? AND post_path=''`, path, id)
	return err
}

// BasketWallets returns the QUALITY BASKET: wallets with >= minN resolved BUY trades whose realized edge
// (win-rate − avg price paid) is >= minEdge — i.e., they consistently BEAT the price (skill), not just big
// or lucky. The research-backed copy set (consistent, below-consensus), computed from the logged trades.
// R124: resolved history spans main + the ptt archive (UNION ALL, per-branch WHERE so each side
// stays index/scan-friendly) — archiving aged rows must never shrink the skill training set.
func (s *Store) BasketWallets(ctx context.Context, minN int, minEdge float64) (map[string]bool, error) {
	const where = `WHERE resolved=1 AND side='BUY' AND price>0 AND price<1 AND won IN (0,1)`
	rows, done, err := s.pttUnionRows(ctx, `
SELECT wallet, COUNT(*) AS n, AVG(won) AS wr, AVG(price) AS ap
FROM (SELECT wallet, won, price FROM main.poly_trader_trades `+where+`
      UNION ALL
      SELECT wallet, won, price FROM archive.poly_trader_trades `+where+`)
GROUP BY wallet HAVING n >= ?`, `
SELECT wallet, COUNT(*) AS n, AVG(won) AS wr, AVG(price) AS ap
FROM poly_trader_trades
`+where+`
GROUP BY wallet HAVING n >= ?`, minN)
	if err != nil {
		return nil, err
	}
	defer done()
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var w string
		var n int
		var wr, ap float64
		if err := rows.Scan(&w, &n, &wr, &ap); err != nil {
			return nil, err
		}
		if wr-ap >= minEdge { // beat the market price they paid → genuine edge
			out[w] = true
		}
	}
	return out, rows.Err()
}

// WalletSkill is a wallet's MARKET-LEVEL performance profile (audit F9/Q2). One market = one
// observation, however many fills built the position — the old row-level counting scored a wallet
// with 1,130 fills in ONE Morocco market as 1130/1130 wins (binomially impossible).
type WalletSkill struct {
	Wallet         string  `json:"wallet"`
	Markets        int     `json:"markets"` // distinct resolved markets with a real net-long stance
	Edge           float64 `json:"edge"`    // recency-decayed, notional-weighted mean of (won − vwap) per market
	Shrunk         float64 `json:"shrunk"`  // Edge · n/(n+8) — the score consumers use (shrinks small samples to 0)
	CLV            float64 `json:"clv"`     // mean (close_px − vwap) where the closing price was captured
	CLVN           int     `json:"clv_n"`
	MMShare        float64 `json:"mm_share"`         // fraction of its markets that were net-flat churn (market-maker signature)
	Martingale     bool    `json:"martingale"`       // sizes UP after losses (behavioral FADE flag)
	NegCLVLongshot bool    `json:"neg_clv_longshot"` // chronic negative-edge/CLV longshot buying (behavioral FADE flag)
}

// WalletSkillScores computes every tracked wallet's market-level skill (audit F9 + Q2 upgrade #1):
//   - MM FILTER: a (wallet, market) pair whose sells ≥ 80% of buys is net-flat churn (the
//     market-maker signature — 81.6% of buy+sell pairs in this DB) and is EXCLUDED from skill.
//   - Per market: the wallet's dominant net-long outcome, its buy VWAP, and whether that outcome
//     won → market edge = won − vwap, weighted by net notional and decayed with a 75-day half-life.
//   - Wallet: Edge = weighted mean, Shrunk = Edge·n/(n+8) (k=8 — the honest-qualifier bar),
//     plus the behavioral FADE flags (martingale-after-loss, chronic negative-CLV longshots).
//
// R124: resolved history spans main + the ptt archive. The GROUP BY runs OVER the union, so a
// (wallet, condition, outcome, side) group whose fills straddle the 30d archive boundary still
// aggregates as ONE group — identical math to the pre-archive single-table scan.
func (s *Store) WalletSkillScores(ctx context.Context) (map[string]*WalletSkill, error) {
	const cols = `wallet, condition_id, outcome_index, side, usdc_size, size, price, won, ts, close_px`
	const where = `WHERE resolved=1 AND won IN (0,1) AND price>0 AND price<1`
	const agg = `
SELECT wallet, condition_id, outcome_index, side,
       SUM(usdc_size) AS usdc, SUM(size) AS sz,
       CASE WHEN SUM(size) > 0 THEN SUM(size*price)/SUM(size) ELSE 0 END AS vwap,
       MAX(won) AS won, MAX(ts) AS last_ts, MAX(close_px) AS close_px`
	rows, done, err := s.pttUnionRows(ctx, agg+`
FROM (SELECT `+cols+` FROM main.poly_trader_trades `+where+`
      UNION ALL
      SELECT `+cols+` FROM archive.poly_trader_trades `+where+`)
GROUP BY wallet, condition_id, outcome_index, side`, agg+`
FROM poly_trader_trades
`+where+`
GROUP BY wallet, condition_id, outcome_index, side`)
	if err != nil {
		return nil, err
	}
	defer done()
	defer rows.Close()
	type sideAgg struct {
		buyUsd, sellUsd, buySz, sellSz, buyVwap, closePx float64
		won                                              int
	}
	type mktAgg struct {
		outs   map[int]*sideAgg
		lastTS int64
	}
	byWM := map[string]map[string]*mktAgg{} // wallet -> condition -> agg
	for rows.Next() {
		var w, cid, side string
		var oi, won int
		var usdc, sz, vwap, closePx float64
		var lastTS int64
		if err := rows.Scan(&w, &cid, &oi, &side, &usdc, &sz, &vwap, &won, &lastTS, &closePx); err != nil {
			return nil, err
		}
		w = strings.ToLower(w)
		if byWM[w] == nil {
			byWM[w] = map[string]*mktAgg{}
		}
		m := byWM[w][cid]
		if m == nil {
			m = &mktAgg{outs: map[int]*sideAgg{}}
			byWM[w][cid] = m
		}
		if lastTS > m.lastTS {
			m.lastTS = lastTS
		}
		o := m.outs[oi]
		if o == nil {
			o = &sideAgg{closePx: -1}
			m.outs[oi] = o
		}
		if strings.EqualFold(side, "BUY") {
			o.buyUsd += usdc
			o.buySz += sz
			o.buyVwap = vwap
			o.won = won
		} else {
			o.sellUsd += usdc
			o.sellSz += sz
		}
		if closePx >= 0 {
			o.closePx = closePx
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	now := float64(time.Now().Unix())
	out := map[string]*WalletSkill{}
	type mktObs struct {
		ts             int64
		edge, w, stake float64
		won            bool
		longshot       bool
		clv            float64
		hasCLV         bool
	}
	for w, mkts := range byWM {
		var obs []mktObs
		flat := 0
		for _, m := range mkts {
			grossBuy, grossSell := 0.0, 0.0
			bestOI, bestNet := -1, 0.0
			for oi, o := range m.outs {
				grossBuy += o.buyUsd
				grossSell += o.sellUsd
				if net := o.buyUsd - o.sellUsd; net > bestNet {
					bestNet, bestOI = net, oi
				}
			}
			if grossBuy <= 0 {
				continue
			}
			if grossSell >= 0.8*grossBuy { // MM FILTER: net-flat churn ≠ a directional opinion
				flat++
				continue
			}
			o := m.outs[bestOI]
			if o == nil || o.buyVwap <= 0 || o.buyVwap >= 1 {
				continue
			}
			ob := mktObs{ts: m.lastTS, w: bestNet, stake: grossBuy,
				won: o.won == 1, longshot: o.buyVwap < 0.35}
			ob.edge = -o.buyVwap
			if o.won == 1 {
				ob.edge = 1 - o.buyVwap
			}
			if o.closePx >= 0 && o.closePx <= 1 {
				ob.clv, ob.hasCLV = o.closePx-o.buyVwap, true
			}
			obs = append(obs, ob)
		}
		if len(obs) == 0 {
			continue
		}
		sort.Slice(obs, func(i, j int) bool { return obs[i].ts < obs[j].ts })
		sk := &WalletSkill{Wallet: w, Markets: len(obs)}
		sumW, sumWE := 0.0, 0.0
		clvSum, clvN := 0.0, 0
		longshots, negLong := 0, 0
		var afterLossStake, afterLossN, afterWinStake, afterWinN float64
		for i, ob := range obs {
			decay := math.Pow(0.5, (now-float64(ob.ts))/(75*86400)) // 75-day half-life
			wt := ob.w * decay
			if wt <= 0 {
				wt = 1e-9
			}
			sumW += wt
			sumWE += wt * ob.edge
			if ob.hasCLV {
				clvSum += ob.clv
				clvN++
			}
			if ob.longshot {
				longshots++
				if (ob.hasCLV && ob.clv < 0) || (!ob.hasCLV && ob.edge < 0) {
					negLong++
				}
			}
			if i > 0 { // martingale detection: stake response to the PREVIOUS market's result
				if obs[i-1].won {
					afterWinStake += ob.stake
					afterWinN++
				} else {
					afterLossStake += ob.stake
					afterLossN++
				}
			}
		}
		sk.Edge = sumWE / sumW
		sk.Shrunk = sk.Edge * float64(len(obs)) / float64(len(obs)+8)
		if clvN > 0 {
			sk.CLV, sk.CLVN = clvSum/float64(clvN), clvN
		}
		if tot := len(obs) + flat; tot > 0 {
			sk.MMShare = float64(flat) / float64(tot)
		}
		if afterLossN >= 5 && afterWinN >= 5 &&
			(afterLossStake/afterLossN) >= 1.6*(afterWinStake/afterWinN) {
			sk.Martingale = true // sizes up ≥1.6× after losses — the doubling-down signature
		}
		if len(obs) >= 8 && longshots >= (len(obs)*6)/10 && negLong*2 >= longshots {
			sk.NegCLVLongshot = true // ≥60% longshot entries and at least half of them negative
		}
		out[w] = sk
	}
	return out, nil
}

// BasketWalletScores is BASKETQ (A6): each qualifying wallet's MEASURED market-level edge.
// REWRITTEN (audit F9): the old version counted trade ROWS — 1,130 fills in one market scored as
// 1,130 wins. minN is now DISTINCT RESOLVED MARKETS; the edge is the shrunk market-level score
// from WalletSkillScores (MM churn excluded, recency-decayed).
func (s *Store) BasketWalletScores(ctx context.Context, minN int, minEdge float64) (map[string]float64, error) {
	skills, err := s.WalletSkillScores(ctx)
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for w, sk := range skills {
		if sk.Markets >= minN && sk.Shrunk >= minEdge {
			out[w] = sk.Shrunk
		}
	}
	return out, nil
}

// ListOpenSignalTickers returns distinct tickers with at least one unresolved signal.
func (s *Store) ListOpenSignalTickers(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT ticker FROM signal_log WHERE resolved=0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err == nil {
			out = append(out, t)
		}
	}
	return out, rows.Err()
}

// SignalLogCount — R67k: raw signal_log volume per (platform, signal_type), the honest zero-state
// context behind grey stats rows ("logged N · resolved 0 — awaiting settlement" vs "never logged").
type SignalLogCount struct {
	Platform   string `json:"platform"`
	SignalType string `json:"signal_type"`
	Logged     int    `json:"logged"`
	Resolved   int    `json:"resolved"`
}

// SignalLogCountsByVenue returns logged/resolved row counts per (platform, signal_type). Uses the
// signal_log aggregation indexes; runs in milliseconds even on 600k+ rows.
func (s *Store) SignalLogCountsByVenue(ctx context.Context) ([]SignalLogCount, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT platform, signal_type, COUNT(*), COALESCE(SUM(resolved),0) FROM signal_log GROUP BY platform, signal_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SignalLogCount
	for rows.Next() {
		var c SignalLogCount
		if err := rows.Scan(&c.Platform, &c.SignalType, &c.Logged, &c.Resolved); err == nil {
			out = append(out, c)
		}
	}
	return out, rows.Err()
}

// ListOpenSignalTickersByPlatform returns distinct unresolved-signal tickers for one
// platform (kalshi tickers, or polymarket conditionIds).
func (s *Store) ListOpenSignalTickersByPlatform(ctx context.Context, platform string) ([]string, error) {
	// Order by each ticker's MOST RECENT log time, OLDEST FIRST (ASC — the comment used to claim
	// "newest first", R67j). Ordering is stable so the sweep's round-robin window walks the whole
	// set every cycle (~9 sweeps × 200 at 7s = full coverage ≈ 1 min); the R67j wxedge audit
	// verified no starvation — 444/444 wxedge rows resolved once their markets actually settled
	// (weather markets settle at the next-morning CLI report, ~35h after the first log).
	rows, err := s.db.QueryContext(ctx, `SELECT ticker FROM signal_log WHERE resolved=0 AND platform=? GROUP BY ticker ORDER BY MAX(ts) ASC`, platform)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err == nil {
			out = append(out, t)
		}
	}
	return out, rows.Err()
}

// RetireStaleOpenSignals (R114, auditor 348/349 — deterministic grading): the settlement sweep's
// per-pass budget walks an UNBOUNDED resolved=0 pool; markets that expire but never report a
// settlement stay resolved=0 forever, so cycle time to revisit any live ticker grows without
// bound (the grading-starvation the auditor watched for 7 runs). Quarantine (resolved=-1, the v4
// pattern) rows whose market's cataloged close_ts is > graceDays old (long closed, still no venue
// settlement = ungradable) plus uncataloged rows whose own log time is > 4×graceDays old. Rows
// exit the sweep queue VISIBLY (resolved=-1 is excluded from training/Edge, counted on /api/stats)
// instead of silently starving everything behind them. Returns rows retired.
func (s *Store) RetireStaleOpenSignals(ctx context.Context, graceDays int) (int64, error) {
	if graceDays <= 0 {
		graceDays = 7
	}
	closeCut := time.Now().UTC().AddDate(0, 0, -graceDays).Format(time.RFC3339)
	tsCut := time.Now().UTC().AddDate(0, 0, -4*graceDays).Format(time.RFC3339)
	res, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET resolved=-1, resolved_at=?
WHERE resolved=0 AND (
  ticker IN (SELECT ticker FROM market_catalog WHERE close_ts != '' AND close_ts < ?)
  OR (ts < ? AND ticker NOT IN (SELECT ticker FROM market_catalog))
)`, nowRFC(), closeCut, tsCut)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ResolveSignalsByOutcome marks open signals on a settled market won/lost by matching the
// signal's side to the winning outcome label (case-insensitive). Used for Polymarket,
// whose sides are outcome labels rather than YES/NO.
func (s *Store) ResolveSignalsByOutcome(ctx context.Context, ticker, winningOutcome string) error {
	// audit F13: when the winning outcome IS a yes/no-style label, the market's YES value is derivable —
	// persist settle_val so ResolvedYes never has to guess from the side-relative won flag.
	sv := -1.0
	switch strings.ToUpper(strings.TrimSpace(winningOutcome)) {
	case "YES", "UP":
		sv = 1
	case "NO", "DOWN":
		sv = 0
	}
	if sv >= 0 {
		_, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET resolved=1, resolved_at=?, settle_val=?,
  won = CASE WHEN LOWER(TRIM(side)) = LOWER(TRIM(?)) THEN 1 ELSE 0 END
WHERE ticker=? AND resolved=0`, nowRFC(), sv, winningOutcome, ticker)
		return err
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET resolved=1, resolved_at=?,
  won = CASE WHEN LOWER(TRIM(side)) = LOWER(TRIM(?)) THEN 1 ELSE 0 END
WHERE ticker=? AND resolved=0`, nowRFC(), winningOutcome, ticker)
	return err
}

// ResolveSignals marks every open signal on a settled ticker won/lost AND records the exact settled
// YES value (SCALARPAY). yesVal is the authoritative settled YES price in [0,1]: 1 (yes), 0 (no), or a
// scalar like 0.10 (goalscorer). The binary `won` flag (for win-rate stats) uses yesVal>=0.5; `settle_val`
// stores the exact value so downstream (ML book) can pay the true per-side payout, not a binary 1/0.
func (s *Store) ResolveSignals(ctx context.Context, ticker string, yesVal float64) error {
	yw := 0
	if yesVal >= 0.5 {
		yw = 1
	}
	// R90 bug 126 skip guard (auditor DO-THIS 1, bugs 73/74/75): the CASE below can only grade
	// canonical YES/NO sides — every other label used to fall through to ELSE 0, silently minting
	// a fabricated LOSS for exec-venue rows whose side was a Poly outcome label (team/player
	// names). Those were auditor r22's 58 graded-all-losses poison rows. Now: canonical sides
	// grade (case-robustly — 'Yes' no longer ELSE-0s either); non-canonical exec-venue rows are
	// QUARANTINED (resolved=-1, the v4 pattern) so they exit the sweep queue visibly instead of
	// re-scanning forever. insertSignal refuses new non-canonical exec rows at the chokepoint, so
	// anything landing in the quarantine leg is a regressed producer. Rows from OTHER platforms
	// sharing this ticker string are left for ResolveSignalsByOutcome (outcome labels are the
	// legitimate vocabulary on poly-int).
	_, err := s.db.ExecContext(ctx, `
UPDATE signal_log SET resolved=1, resolved_at=?, settle_val=?,
  won = CASE WHEN (UPPER(TRIM(side))='YES' AND ?=1) OR (UPPER(TRIM(side))='NO' AND ?=0) THEN 1 ELSE 0 END
WHERE ticker=? AND resolved=0 AND (platform NOT IN ('kalshi','polyus') OR UPPER(TRIM(side)) IN ('YES','NO'))`,
		nowRFC(), yesVal, yw, yw, ticker)
	if err == nil {
		// R106 (auditor bug 283): the quarantine UPDATE's error is no longer swallowed — a
		// persistently-failing quarantine leg meant non-canonical rows re-scanned every drain
		// invisibly (the bug-126 churn class). It now joins the return.
		_, qerr := s.db.ExecContext(ctx, `
UPDATE signal_log SET resolved=-1, resolved_at=?
WHERE ticker=? AND resolved=0 AND platform IN ('kalshi','polyus') AND UPPER(TRIM(side)) NOT IN ('YES','NO')`,
			nowRFC(), ticker)
		if qerr != nil {
			return fmt.Errorf("quarantine leg: %w", qerr)
		}
		// R106 (auditor r33 DO-THIS 6, edges 30/35): grade the maker would-gate counterfactual —
		// mirror the settlement onto maker_fill_stats rows for this ticker that never got one
		// (idempotent: WHERE settle_val IS NULL; `filled` untouched — -2 gated / 0 expired / 1
		// filled semantics stay exactly as written; ties (settle 0.5) grade as loss per spec).
		_, gerr := s.db.ExecContext(ctx, `
UPDATE maker_fill_stats SET settle_val=?,settle_mirrored_at=?,
  outcome_won = CASE WHEN (UPPER(TRIM(side))='YES' AND ?=1) OR (UPPER(TRIM(side))='NO' AND ?=0) THEN 1 ELSE 0 END
WHERE ticker=? AND settle_val IS NULL`,
			yesVal, nowRFC(), yw, yw, ticker)
		if gerr != nil {
			return fmt.Errorf("would-gate grading leg: %w", gerr)
		}
		// R135c: the sub-cent golf lane is a research-only counterfactual ledger, not a
		// signal/paper position.  Mirror this same authoritative settlement directly so
		// maker cancels, maker fills and ask-taking observations share one outcome truth.
		if gerr := s.ResolveSubcentGolfTicker(ctx, ticker, yesVal); gerr != nil {
			return fmt.Errorf("subcent-golf grading leg: %w", gerr)
		}
	}
	return err
}

// ArbRow is one logged arbitrage opportunity (ARBXV) — cross-venue (Kalshi↔Poly US) or within-venue.
type ArbRow struct {
	TS        string  `json:"ts"`
	Kind      string  `json:"kind"`
	Market    string  `json:"market"`
	Category  string  `json:"category"`
	BuyVenue  string  `json:"buy_venue"`
	SellVenue string  `json:"sell_venue"`
	BuyPrice  float64 `json:"buy_price"`
	OppPrice  float64 `json:"opp_price"`
	GrossEdge float64 `json:"gross_edge"`
	FeeEst    float64 `json:"fee_est"`
	NetEdge   float64 `json:"net_edge"`
}

// InsertArb records an arb opportunity (deduped to one row per market per ~10-min slot).
func (s *Store) InsertArb(ctx context.Context, a ArbRow) error {
	now := nowRFC()
	slot := now
	if len(now) >= 15 {
		slot = now[:15]
	}
	_, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO arb_log(ts,slot,kind,market,category,buy_venue,sell_venue,buy_price,opp_price,gross_edge,fee_est,net_edge)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		now, slot, a.Kind, a.Market, a.Category, a.BuyVenue, a.SellVenue, a.BuyPrice, a.OppPrice, a.GrossEdge, a.FeeEst, a.NetEdge)
	return err
}

// ListArb returns logged arb opportunities, newest first (capped). R132 permanently quarantines
// the legacy dutch-sell/dutch-buy cohort: those rows were produced before exact outcome-set
// validation and include spreads/totals/props grouped as if they were mutually exclusive. The raw
// rows stay in SQLite for forensic work; no normal export/verdict consumer can ingest them.
func (s *Store) ListArb(ctx context.Context, limit int) ([]ArbRow, error) {
	if limit <= 0 {
		limit = 2000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT ts,kind,market,category,buy_venue,sell_venue,buy_price,opp_price,gross_edge,fee_est,net_edge
FROM arb_log
WHERE kind NOT IN ('dutch-sell','dutch-buy')
ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ArbRow
	for rows.Next() {
		var a ArbRow
		if err := rows.Scan(&a.TS, &a.Kind, &a.Market, &a.Category, &a.BuyVenue, &a.SellVenue, &a.BuyPrice, &a.OppPrice, &a.GrossEdge, &a.FeeEst, &a.NetEdge); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, nil
}

// SignalStat is hit-rate for one signal type.
type SignalStat struct {
	SignalType string  `json:"signal_type"`
	Total      int     `json:"total"`
	Resolved   int     `json:"resolved"`
	Wins       int     `json:"wins"`
	HitRate    float64 `json:"hit_rate"`
}

// SignalStats returns hit-rate per signal type (resolved signals only count toward
// HitRate; Total includes still-open ones).
func (s *Store) SignalStats(ctx context.Context) ([]SignalStat, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT signal_type, COUNT(*), COALESCE(SUM(resolved),0),
       COALESCE(SUM(CASE WHEN won=1 THEN 1 ELSE 0 END),0)
FROM signal_log GROUP BY signal_type ORDER BY signal_type`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SignalStat
	for rows.Next() {
		var st SignalStat
		if err := rows.Scan(&st.SignalType, &st.Total, &st.Resolved, &st.Wins); err != nil {
			return nil, err
		}
		if st.Resolved > 0 {
			st.HitRate = float64(st.Wins) / float64(st.Resolved)
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// SignalRow is one logged observation with its full feature vector + outcome, for export
// and offline analysis. Won is -1 while unresolved (kept as int so JSON stays simple).
type SignalRow struct {
	TS            string  `json:"ts"`
	ResolvedAt    string  `json:"resolved_at"`
	Platform      string  `json:"platform"`
	Ticker        string  `json:"ticker"`
	Title         string  `json:"title"`
	Side          string  `json:"side"`
	SignalType    string  `json:"signal_type"`
	EntryPrice    float64 `json:"entry_price"`
	Strength      float64 `json:"strength"`
	Notional      float64 `json:"notional"`
	BestRank      int     `json:"best_rank"`
	TraderCount   int     `json:"trader_count"`
	TraderPnL     float64 `json:"trader_pnl"`
	Concentration float64 `json:"concentration"`
	Momentum      float64 `json:"momentum"`
	SpreadCents   float64 `json:"spread_cents"`
	ResolveHours  float64 `json:"resolve_hours"`
	Imbalance     float64 `json:"imbalance"`
	Underlying    float64 `json:"underlying"`
	PricePath     string  `json:"price_path"`
	Category      string  `json:"category"`
	Confidence    float64 `json:"confidence"`   // gate score at signal time (win-rate × price quality)
	BookDepth     float64 `json:"book_depth"`   // resting size at the touch at signal time (maker-fill odds)
	FillPrice     float64 `json:"fill_price"`   // ACTUAL fill price once this signal became a bet (0 = never filled) — real-fill ML label
	PostPath      string  `json:"post_path"`    // JSON price path AFTER entry (for SL/TP calibration), "" if none
	TraderSkill   float64 `json:"trader_skill"` // acting wallet's shrunk market-level skill (whale v2)
	MarketType    string  `json:"market_type"`  // granular market type (audit Q3)
	IsLive        int     `json:"is_live"`      // R38: 1 = in-play at signal time, 0 = pre/stable, -1 = unknown (legacy)
	// R65 feature families, COALESCE'd to the SAME sentinels the ML sidecar uses (NULL on pre-R65 rows):
	BookImb3    float64 `json:"book_imb3"`     // top-3 bid share of top-3 depth 0..1; -1 = not captured
	BookDepth3  float64 `json:"book_depth3"`   // summed top-3 depth both sides; 0 = not captured
	Mom1h       float64 `json:"mom_1h"`        // ~1h YES move, cents (ring-scope); 0 = flat OR not captured
	Mom4h       float64 `json:"mom_4h"`        // ~4h YES move, cents
	DistHi      float64 `json:"dist_hi"`       // cents below ring-scope session high; 0 = at high OR not captured
	DistLo      float64 `json:"dist_lo"`       // cents above ring-scope session low
	SecsToStart int64   `json:"secs_to_start"` // signed secs to event start (neg = started); 1e7 = unknown
	Resolved    int     `json:"resolved"`
	Won         int     `json:"won"` // 1/0 once resolved, -1 while open
}

// ListSignals returns logged signal observations (newest first), capped at limit, with
// features + outcome — the dataset for edge discovery. limit<=0 returns all.
func (s *Store) ListSignals(ctx context.Context, limit int) ([]SignalRow, error) {
	return s.listSignals(ctx, limit, true)
}

// ListSignalsLean is ListSignals WITHOUT the price_path/post_path text blobs (audit §3): the Edge
// finder aggregates features + outcomes and never reads a path, but was dragging ~100s of MB of
// path text through every 500k-row scan — the reason the Edge tab's cold build outlived the
// browser. Same row shape; the two path fields come back "".
func (s *Store) ListSignalsLean(ctx context.Context, limit int) ([]SignalRow, error) {
	return s.listSignals(ctx, limit, false)
}

// ListSLTPEligibleSignals returns the newest rows that can actually feed the stop/target path
// replay: resolved binary outcome, a real fill, and a post-entry path containing at least two
// tokens.  The Go replay still parses and validates the floats; the SQL predicate is the bounded,
// indexed admission filter that prevents a flood of newer unresolved observations from starving
// older usable evidence (the pre-R133 ListSignals(8000)-then-filter bug).
func (s *Store) ListSLTPEligibleSignals(ctx context.Context, limit int) ([]SignalRow, error) {
	if limit <= 0 {
		limit = 8000
	}
	q := `
SELECT ts, COALESCE(resolved_at,''), platform, ticker, title, side, signal_type, entry_price,
       strength, notional, best_rank, trader_count, trader_pnl, concentration, momentum,
       spread_cents, resolve_hours, imbalance, underlying, price_path, category, confidence,
       book_depth, fill_price, COALESCE(post_path,''), COALESCE(trader_skill,0), COALESCE(market_type,''), COALESCE(is_live,-1),
       COALESCE(book_imb3,-1), COALESCE(book_depth3,0), COALESCE(mom_1h,0), COALESCE(mom_4h,0),
       COALESCE(dist_hi,0), COALESCE(dist_lo,0), COALESCE(secs_to_start,10000000), resolved, won
FROM signal_log
WHERE resolved = 1 AND won IN (0,1) AND fill_price > 0
  AND post_path != '' AND instr(post_path, ',') > 0
ORDER BY id DESC LIMIT ` + strconv.Itoa(limit)
	return s.querySignalRows(ctx, q)
}

func (s *Store) listSignals(ctx context.Context, limit int, withPaths bool) ([]SignalRow, error) {
	pathCols := "price_path, "
	postCol := "COALESCE(post_path,'')"
	if !withPaths {
		pathCols = "'', "
		postCol = "''"
	}
	q := `
SELECT ts, COALESCE(resolved_at,''), platform, ticker, title, side, signal_type, entry_price,
       strength, notional, best_rank, trader_count, trader_pnl, concentration, momentum,
       spread_cents, resolve_hours, imbalance, underlying, ` + pathCols + `category, confidence,
       book_depth, fill_price, ` + postCol + `, COALESCE(trader_skill,0), COALESCE(market_type,''), COALESCE(is_live,-1),
       COALESCE(book_imb3,-1), COALESCE(book_depth3,0), COALESCE(mom_1h,0), COALESCE(mom_4h,0),
       COALESCE(dist_hi,0), COALESCE(dist_lo,0), COALESCE(secs_to_start,10000000), resolved, won
FROM signal_log ORDER BY id DESC`
	if limit > 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	return s.querySignalRows(ctx, q)
}

func (s *Store) querySignalRows(ctx context.Context, q string) ([]SignalRow, error) {
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SignalRow
	for rows.Next() {
		var r SignalRow
		var won sql.NullInt64
		if err := rows.Scan(&r.TS, &r.ResolvedAt, &r.Platform, &r.Ticker, &r.Title, &r.Side,
			&r.SignalType, &r.EntryPrice, &r.Strength, &r.Notional, &r.BestRank, &r.TraderCount,
			&r.TraderPnL, &r.Concentration, &r.Momentum, &r.SpreadCents, &r.ResolveHours, &r.Imbalance,
			&r.Underlying, &r.PricePath, &r.Category, &r.Confidence,
			&r.BookDepth, &r.FillPrice, &r.PostPath, &r.TraderSkill, &r.MarketType, &r.IsLive,
			&r.BookImb3, &r.BookDepth3, &r.Mom1h, &r.Mom4h, &r.DistHi, &r.DistLo, &r.SecsToStart, &r.Resolved, &won); err != nil {
			return nil, err
		}
		if won.Valid {
			r.Won = int(won.Int64)
		} else {
			r.Won = -1
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListResolvedSignalsLeanSince returns RESOLVED signal rows with id > sinceID (ascending) plus the
// max id seen — the delta feed for the Edge report's resident cache (R19). Resolved rows are
// immutable, so an in-memory cache + this delta replaces the full 500k-row rescan that starved the
// process (WS drops, minutes-long builds) every time the Edge snapshot rebuilt.
func (s *Store) ListResolvedSignalsLeanSince(ctx context.Context, sinceID int64) ([]SignalRow, int64, error) {
	q := `
SELECT id, ts, COALESCE(resolved_at,''), platform, ticker, title, side, signal_type, entry_price,
       strength, notional, best_rank, trader_count, trader_pnl, concentration, momentum,
       spread_cents, resolve_hours, imbalance, underlying, '', category, confidence,
       book_depth, fill_price, '', COALESCE(trader_skill,0), COALESCE(market_type,''), COALESCE(is_live,-1),
       COALESCE(book_imb3,-1), COALESCE(book_depth3,0), COALESCE(mom_1h,0), COALESCE(mom_4h,0),
       COALESCE(dist_hi,0), COALESCE(dist_lo,0), COALESCE(secs_to_start,10000000), resolved, won
FROM signal_log WHERE resolved = 1 AND id > ? ORDER BY id ASC`
	rows, err := s.db.QueryContext(ctx, q, sinceID)
	if err != nil {
		return nil, sinceID, err
	}
	defer rows.Close()
	maxID := sinceID
	var out []SignalRow
	for rows.Next() {
		var r SignalRow
		var id int64
		var won sql.NullInt64
		if err := rows.Scan(&id, &r.TS, &r.ResolvedAt, &r.Platform, &r.Ticker, &r.Title, &r.Side,
			&r.SignalType, &r.EntryPrice, &r.Strength, &r.Notional, &r.BestRank, &r.TraderCount,
			&r.TraderPnL, &r.Concentration, &r.Momentum, &r.SpreadCents, &r.ResolveHours, &r.Imbalance,
			&r.Underlying, &r.PricePath, &r.Category, &r.Confidence,
			&r.BookDepth, &r.FillPrice, &r.PostPath, &r.TraderSkill, &r.MarketType, &r.IsLive,
			&r.BookImb3, &r.BookDepth3, &r.Mom1h, &r.Mom4h, &r.DistHi, &r.DistLo, &r.SecsToStart, &r.Resolved, &won); err != nil {
			return nil, sinceID, err
		}
		if won.Valid {
			r.Won = int(won.Int64)
		} else {
			r.Won = -1
		}
		if id > maxID {
			maxID = id
		}
		out = append(out, r)
	}
	return out, maxID, rows.Err()
}

// PolicySignalRow is the compact, executable-economics subset needed by the direct-side
// placement policy. Keeping this separate from SignalRow prevents a 30-day policy refresh from
// materializing the full ML/research feature vector (and, before R140, the entire resolved
// history) during suite startup.
type PolicySignalRow struct {
	TS         string
	Platform   string
	Ticker     string
	Side       string
	SignalType string
	EntryPrice float64
	FillPrice  float64
	Category   string
	Won        int
}

// ListResolvedPolicySignalsSince returns one honest entry observation per
// ticker+side+signal-family inside the requested window. The earliest qualifying row is the
// entry, matching dedupSignals, but the cutoff is applied BEFORE deduplication so an old episode
// cannot suppress a current-window observation. Non-tradeable venues and malformed side labels
// never enter the policy dataset.
func (s *Store) ListResolvedPolicySignalsSince(ctx context.Context, since time.Time) ([]PolicySignalRow, error) {
	cutoff := since.UTC().Format(tsLayout)
	rows, err := s.db.QueryContext(ctx, `
WITH first_in_window AS (
  SELECT MIN(id) AS id
  FROM signal_log INDEXED BY idx_signal_ts
  WHERE resolved = 1
    AND won IN (0,1)
    AND ts >= ?
    AND platform IN ('kalshi','polyus')
    AND UPPER(TRIM(side)) IN ('YES','NO')
  GROUP BY ticker, UPPER(TRIM(side)), signal_type
)
SELECT s.ts, s.platform, s.ticker, s.side, s.signal_type, s.entry_price,
       COALESCE(s.fill_price,0), COALESCE(s.category,''), s.won
FROM signal_log AS s
JOIN first_in_window AS f ON f.id = s.id
ORDER BY s.id ASC`, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]PolicySignalRow, 0, 4096)
	for rows.Next() {
		var r PolicySignalRow
		if err := rows.Scan(&r.TS, &r.Platform, &r.Ticker, &r.Side, &r.SignalType,
			&r.EntryPrice, &r.FillPrice, &r.Category, &r.Won); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- intraday P&L curve persistence ----

// PnLRow is one persisted intraday net-P&L sample.
type PnLRow struct {
	TS  string
	Net float64
}

// InsertPnLPoint appends one net-P&L sample, then prunes the table to the most recent ~20000 rows so
// it can't grow without bound.
func (s *Store) InsertPnLPoint(ctx context.Context, ts string, net float64) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO pnl_series(ts,net) VALUES(?,?)`, ts, net); err != nil {
		return err
	}
	_, _ = s.db.ExecContext(ctx, `DELETE FROM pnl_series WHERE id <= (SELECT MAX(id)-20000 FROM pnl_series)`)
	return nil
}

// ListPnLPoints returns the most recent `limit` samples in chronological (oldest→newest) order.
func (s *Store) ListPnLPoints(ctx context.Context, limit int) ([]PnLRow, error) {
	if limit <= 0 {
		limit = 5000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ts,net FROM (SELECT id,ts,net FROM pnl_series ORDER BY id DESC LIMIT ?) ORDER BY id ASC`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PnLRow
	for rows.Next() {
		var r PnLRow
		if err := rows.Scan(&r.TS, &r.Net); err == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

// ClearPnLSeries wipes the persisted intraday curve (called by Reset P&L).
func (s *Store) ClearPnLSeries(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pnl_series`)
	return err
}

// ---- Polymarket smart-money database (Phase 1b) ----

// UpsertPolyTrader records/refreshes a ranked wallet's identity + leaderboard rank/profit.
// CLOBBER GUARD (audit F8): the flow path calls this with rank=0/profit=0/name=addr[:10] AFTER the
// roster loop — the old unconditional overwrite stubbed mega-whales (swisstony/RN1) down to 0/0.
// Real values never regress to zeros/placeholders: name only improves, best_rank keeps the BEST
// (lowest positive) rank ever seen, profit only updates from a non-zero reading.
func (s *Store) UpsertPolyTrader(ctx context.Context, wallet, name string, rank int, profit float64) error {
	now := nowRFC()
	_, err := s.db.ExecContext(ctx, `
INSERT INTO poly_traders(wallet,name,best_rank,profit,first_seen,last_seen,cursor_ts)
VALUES(?,?,?,?,?,?,0)
ON CONFLICT(wallet) DO UPDATE SET
  name = CASE
           WHEN excluded.name != '' AND excluded.name != substr(poly_traders.wallet,1,10)
             THEN excluded.name
           WHEN poly_traders.name = '' THEN excluded.name
           ELSE poly_traders.name
         END,
  best_rank = CASE
                WHEN excluded.best_rank > 0 AND (poly_traders.best_rank <= 0 OR excluded.best_rank < poly_traders.best_rank)
                  THEN excluded.best_rank
                ELSE poly_traders.best_rank
              END,
  profit = CASE WHEN excluded.profit != 0 THEN excluded.profit ELSE poly_traders.profit END,
  last_seen = excluded.last_seen`,
		strings.ToLower(wallet), name, rank, profit, now, now)
	return err
}

// GetPolyTraderCursor returns the newest activity timestamp pulled so far for a wallet
// (0 if unknown) — used to pull only NEW activity each cycle.
func (s *Store) GetPolyTraderCursor(ctx context.Context, wallet string) (int64, error) {
	var ts int64
	err := s.db.QueryRowContext(ctx, `SELECT cursor_ts FROM poly_traders WHERE wallet=?`, strings.ToLower(wallet)).Scan(&ts)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return ts, err
}

// SetPolyTraderCursor advances a wallet's incremental-pull cursor.
func (s *Store) SetPolyTraderCursor(ctx context.Context, wallet string, ts int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE poly_traders SET cursor_ts=? WHERE wallet=?`, ts, strings.ToLower(wallet))
	return err
}

// TraderTrade is one logged smart-money TRADE for insertion.
type TraderTrade struct {
	Wallet       string
	TS           int64
	ConditionID  string
	Asset        string
	Title        string
	Outcome      string
	OutcomeIndex int
	Side         string
	Size         float64
	Price        float64
	UsdcSize     float64
	TxHash       string
}

// InsertPolyTraderTrade appends a smart-money trade (deduped on tx_hash+asset+side, so it's
// safe to re-pull overlapping windows).
func (s *Store) InsertPolyTraderTrade(ctx context.Context, t TraderTrade) error {
	conditionID, valid := polyid.NormalizeConditionID(t.ConditionID)
	resolved := 0
	var resolvedAt any
	if !valid {
		// Preserve the raw activity for forensic/flow analysis, but never admit a truncated
		// token-derived value to the condition-resolution backlog.
		conditionID = strings.TrimSpace(t.ConditionID)
		resolved = -3
		resolvedAt = nowRFC()
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO poly_trader_trades(wallet,ts,condition_id,asset,title,outcome,outcome_index,side,size,price,usdc_size,tx_hash,resolved,resolved_at)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(tx_hash,asset,side) DO UPDATE SET
  condition_id=excluded.condition_id, resolved=0, resolved_at=NULL
WHERE poly_trader_trades.resolved=-3 AND excluded.resolved=0`,
		strings.ToLower(t.Wallet), t.TS, conditionID, t.Asset, t.Title, t.Outcome, t.OutcomeIndex,
		t.Side, t.Size, t.Price, t.UsdcSize, t.TxHash, resolved, resolvedAt)
	return err
}

// OpenPolyTraderConditions returns distinct condition ids with unresolved trades (for the
// resolution sweep). limit<=0 returns all.
func (s *Store) OpenPolyTraderConditions(ctx context.Context, limit int) ([]string, error) {
	q := `SELECT DISTINCT condition_id FROM poly_trader_trades WHERE resolved=0`
	if limit > 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err == nil {
			out = append(out, c)
		}
	}
	return out, rows.Err()
}

// OpenPolyTraderConditionsDue is the ROTATING, BACKOFF-AWARE replacement for the resolution sweep
// (audit F7): open conditions whose retry window has passed, RETRY-SOONEST + NEWEST-TRADES first —
// so fresh markets (which actually resolve) drain ahead of the 2024-era head that wedged the old
// unordered head-40 forever.
func (s *Store) OpenPolyTraderConditionsDue(ctx context.Context, limit int, nowUnix int64) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT tt.condition_id
FROM poly_trader_trades AS tt INDEXED BY idx_ptt_resolved_cond
LEFT JOIN poly_condition_status cs ON cs.condition_id = tt.condition_id
WHERE tt.resolved=0 AND COALESCE(cs.next_retry,0) <= ?
GROUP BY tt.condition_id
ORDER BY COALESCE(MAX(cs.attempts),0) ASC, MAX(tt.ts) DESC
LIMIT ?`, nowUnix, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err == nil {
			out = append(out, c)
		}
	}
	return out, rows.Err()
}

// BumpConditionRetry records a failed resolution attempt: exponential backoff from 10 minutes up
// to 6 hours, so dead conditions stop burning the request budget every sweep (audit F7).
func (s *Store) BumpConditionRetry(ctx context.Context, conditionID string, nowUnix int64) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO poly_condition_status(condition_id, attempts, next_retry) VALUES(?, 1, ?)
ON CONFLICT(condition_id) DO UPDATE SET
  attempts = poly_condition_status.attempts + 1,
  next_retry = ? + MIN(21600, 600 * (1 << MIN(poly_condition_status.attempts, 5)))`,
		conditionID, nowUnix+600, nowUnix)
	return err
}

// MarkStaleConditionsUnresolvable flags conditions whose NEWEST trade is older than `days` days as
// resolved=-1 (unresolvable — venue purged/never graded), removing them from the sweep queue
// forever (audit F7: the head-of-queue was wedged on Oct-2024 election markets). Returns rows marked.
func (s *Store) MarkStaleConditionsUnresolvable(ctx context.Context, days int, nowUnix int64) (int64, error) {
	cutoff := nowUnix - int64(days)*86400
	res, err := s.db.ExecContext(ctx, `
UPDATE poly_trader_trades SET resolved=-1
WHERE resolved=0 AND condition_id IN (
  SELECT condition_id FROM poly_trader_trades WHERE resolved=0 GROUP BY condition_id HAVING MAX(ts) < ?
)`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ResolvePolyTraderTrades marks every open trade on a settled market won/lost by matching the
// trade's outcome label to the winning outcome (case-insensitive) — same convention as signals.
// Kept for callers without index/price data; prefer ResolvePolyTraderTradesIdx.
func (s *Store) ResolvePolyTraderTrades(ctx context.Context, conditionID, winningOutcome string) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE poly_trader_trades SET resolved=1, resolved_at=?,
  won = CASE WHEN LOWER(TRIM(outcome)) = LOWER(TRIM(?)) THEN 1 ELSE 0 END
WHERE condition_id=? AND resolved=0`, nowRFC(), winningOutcome, conditionID)
	return err
}

// ResolvePolyTraderTradesIdx resolves a settled market's trades by OUTCOME INDEX (audit Q2 #6:
// label matching breaks on renamed/localized outcomes; the index is stable), and captures each
// row's closing price for CLV (close_px = its outcome's final price from the same gamma response —
// zero extra requests). winningIdx<0 falls back to label matching; prices may be nil.
func (s *Store) ResolvePolyTraderTradesIdx(ctx context.Context, conditionID, winningOutcome string, winningIdx int, prices []float64) error {
	if winningIdx < 0 {
		return s.ResolvePolyTraderTrades(ctx, conditionID, winningOutcome)
	}
	now := nowRFC()
	if _, err := s.db.ExecContext(ctx, `
UPDATE poly_trader_trades SET resolved=1, resolved_at=?,
  won = CASE WHEN outcome_index = ? THEN 1 ELSE 0 END
WHERE condition_id=? AND resolved=0`, now, winningIdx, conditionID); err != nil {
		return err
	}
	for idx, px := range prices {
		if px < 0 || px > 1 {
			continue
		}
		_, _ = s.db.ExecContext(ctx,
			`UPDATE poly_trader_trades SET close_px=? WHERE condition_id=? AND outcome_index=? AND close_px < 0`,
			px, conditionID, idx)
	}
	// success clears the retry/backoff record
	_, _ = s.db.ExecContext(ctx, `DELETE FROM poly_condition_status WHERE condition_id=?`, conditionID)
	return nil
}

// ── R125: ptt backlog backfill support ──────────────────────────────────────────────────────────
// resolved semantics after R125: 0 open · 1 graded (won set) · -1 stale-age quarantine
// (MarkStaleConditionsUnresolvable, >18mo) · -2 UNRESOLVABLE AT VENUE (market deleted — CLOB 404
// + absent from gamma incl. closed=true — or VOIDED: venue-resolved with no decisive outcome).
// -3 is R132's malformed-condition-id quarantine. Negative rows are never treated as graded.

// MarkConditionUnresolvableVenue flags every open row of one condition resolved=-2 and clears its
// retry record. Returns rows marked. reason is for the caller's journal only.
func (s *Store) MarkConditionUnresolvableVenue(ctx context.Context, conditionID string) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE poly_trader_trades SET resolved=-2, resolved_at=? WHERE condition_id=? AND resolved=0`,
		nowRFC(), conditionID)
	if err != nil {
		return 0, err
	}
	_, _ = s.db.ExecContext(ctx, `DELETE FROM poly_condition_status WHERE condition_id=?`, conditionID)
	return res.RowsAffected()
}

// MarkConditionUnresolvableVenueIfNewestBefore is the deletion-only form with the age guard in
// the same SQLite statement as the mutation. The backfill first batches MAX(ts) for efficiency,
// but a new activity row can arrive while venue lookups are in flight; this atomic recheck keeps
// that row (and the condition's older rows) open instead of retiring a newly active condition.
func (s *Store) MarkConditionUnresolvableVenueIfNewestBefore(ctx context.Context, conditionID string, cutoffUnix int64) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
UPDATE poly_trader_trades SET resolved=-2, resolved_at=?
WHERE condition_id=? AND resolved=0
  AND NOT EXISTS (
    SELECT 1 FROM poly_trader_trades recent
    WHERE recent.condition_id=? AND recent.resolved=0 AND recent.ts >= ?
  )`, nowRFC(), conditionID, conditionID, cutoffUnix)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if n > 0 {
		_, _ = s.db.ExecContext(ctx, `DELETE FROM poly_condition_status WHERE condition_id=?`, conditionID)
	}
	return n, nil
}

// UnresolvedConditionPage is one distinct unresolved condition with a representative trade time.
type UnresolvedConditionPage struct {
	ConditionID string
	// RepresentativeTS is the first (oldest) row encountered by the keyset walk. It is a
	// cursor-discovery value only and must never be interpreted as the condition's age.
	RepresentativeTS int64
}

// UnresolvedRowsPage walks the OPEN tape oldest-row-first as a pure keyset index-range scan on
// idx_ptt_res_ts (resolved, ts) — O(page) per call, never a table-wide GROUP BY (the first R125
// cut grouped 4.2M rows every pass and starved concurrent readers — root-caused same day).
// Returns the page's DISTINCT condition ids in first-seen order (deduped Go-side), plus the
// cursor (ts, id) of the last row consumed. olderThanUnix bounds the walk away from the fresh
// tape the per-minute sweep already owns.
func (s *Store) UnresolvedRowsPage(ctx context.Context, afterTS, afterID, olderThanUnix int64, rowLimit int) ([]UnresolvedConditionPage, int64, int64, error) {
	if rowLimit <= 0 {
		rowLimit = 8000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT condition_id, ts, id FROM poly_trader_trades
WHERE resolved=0 AND ts < ? AND (ts > ? OR (ts = ? AND id > ?))
ORDER BY ts ASC, id ASC
LIMIT ?`, olderThanUnix, afterTS, afterTS, afterID, rowLimit)
	if err != nil {
		return nil, afterTS, afterID, err
	}
	defer rows.Close()
	var out []UnresolvedConditionPage
	seen := map[string]bool{}
	lastTS, lastID := afterTS, afterID
	for rows.Next() {
		var cid string
		var ts, id int64
		if err := rows.Scan(&cid, &ts, &id); err != nil {
			continue
		}
		lastTS, lastID = ts, id
		if !seen[cid] {
			seen[cid] = true
			out = append(out, UnresolvedConditionPage{ConditionID: cid, RepresentativeTS: ts})
		}
	}
	return out, lastTS, lastID, rows.Err()
}

// NewestUnresolvedTradeTimes returns the actual newest unresolved trade timestamp for each
// requested condition. The backfill keyset walk discovers a condition through its oldest row;
// using that representative row as the condition age can incorrectly retire a condition that
// also has a recent trade. Bounded IN batches keep this on idx_ptt_resolved_cond and below every
// supported SQLite variable limit without a full-table GROUP BY.
func (s *Store) NewestUnresolvedTradeTimes(ctx context.Context, conditionIDs []string) (map[string]int64, error) {
	const chunkSize = 400
	out := make(map[string]int64, len(conditionIDs))
	seen := make(map[string]struct{}, len(conditionIDs))
	uniq := make([]string, 0, len(conditionIDs))
	for _, cid := range conditionIDs {
		cid = strings.TrimSpace(cid)
		if cid == "" {
			continue
		}
		if _, ok := seen[cid]; ok {
			continue
		}
		seen[cid] = struct{}{}
		uniq = append(uniq, cid)
	}
	for start := 0; start < len(uniq); start += chunkSize {
		end := start + chunkSize
		if end > len(uniq) {
			end = len(uniq)
		}
		args := make([]any, 0, end-start)
		marks := make([]string, 0, end-start)
		for _, cid := range uniq[start:end] {
			args = append(args, cid)
			marks = append(marks, "?")
		}
		q := `SELECT condition_id, MAX(ts)
FROM poly_trader_trades INDEXED BY idx_ptt_resolved_cond
WHERE resolved=0 AND condition_id IN (` + strings.Join(marks, ",") + `)
GROUP BY condition_id`
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var cid string
			var newest int64
			if err := rows.Scan(&cid, &newest); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[cid] = newest
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// CountActiveSignalFamilies — distinct signal_type with an OPEN row stamped in the trailing
// window: the briefing table's "currently logging" proxy (R125). resolved=0 keeps the walk on
// the open subset rather than the whole log.
func (s *Store) CountActiveSignalFamilies(ctx context.Context, since time.Time) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT signal_type) FROM signal_log WHERE resolved=0 AND ts >= ?`,
		since.UTC().Format(time.RFC3339)).Scan(&n)
	return n, err
}

// PTTBacklogStats — cheap backlog snapshot for the backfill worker's journal: open rows, distinct
// open conditions, and rows by terminal class (both indexed on resolved).
func (s *Store) PTTBacklogStats(ctx context.Context) (openRows, openConds, graded, unresolvable int64, err error) {
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(DISTINCT condition_id) FROM poly_trader_trades WHERE resolved=0`).Scan(&openRows, &openConds); err != nil {
		return
	}
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM poly_trader_trades WHERE resolved=1`).Scan(&graded); err != nil {
		return
	}
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM poly_trader_trades WHERE resolved < 0`).Scan(&unresolvable)
	return
}

// LastEpochTS returns the timestamp of the most recent CONFIG/TUNE change from the audit log —
// the regime boundary for epoch-aware verdicts (audit §2/Q7: DECIDES pooled trades across invert
// flips, EV-gate changes, and SLTP re-tunes, so each policy change poisoned the next month of
// verdicts). "" when no such event exists.
func (s *Store) LastEpochTS(ctx context.Context) (string, error) {
	var ts sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT MAX(ts) FROM audit_log WHERE category IN ('config','tune')`).Scan(&ts)
	return ts.String, err
}

// ---- kv latch persistence (P3, audit §6: "persist arm/dedup state") ----
// One row per latch key, upserted at mutation time (mutations are rare — a handful per hour), so a
// restart can't re-fire a gate order, re-buy a consensus bridge, or re-free-roll a position.

// KVSet upserts a latch key. ts is stamped server-side for age-based pruning.
func (s *Store) KVSet(ctx context.Context, k, v string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)
ON CONFLICT(k) DO UPDATE SET v=excluded.v, ts=excluded.ts`, k, v, nowRFC())
	return err
}

// KVDel removes a single latch key (used when a position fully closes).
func (s *Store) KVDel(ctx context.Context, k string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM kv WHERE k=?`, k)
	return err
}

// KVPrefix returns every k(without prefix)→v under a prefix — the boot-time latch reload.
func (s *Store) KVPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT k, v FROM kv WHERE k LIKE ? ESCAPE '\'`, likeEscape(prefix)+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if rows.Scan(&k, &v) == nil {
			out[strings.TrimPrefix(k, prefix)] = v
		}
	}
	return out, rows.Err()
}

// KVPruneOlder deletes latch rows under a prefix written before cutoff — bounds the table forever.
func (s *Store) KVPruneOlder(ctx context.Context, prefix string, cutoff time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM kv WHERE k LIKE ? ESCAPE '\' AND ts < ?`,
		likeEscape(prefix)+"%", cutoff.UTC().Format(time.RFC3339))
	return err
}

// likeEscape escapes LIKE metacharacters in a literal prefix (latch prefixes contain '_').
func likeEscape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `%`, `\%`)
	return strings.ReplaceAll(s, `_`, `\_`)
}

// ---- honest-fill harness (audit Q5/Q6/Q7) ----

// InsertMakerAttempt records a posted (resting) paper maker order; returns its row id.
// R90: wouldGate stamps the edge-30 adverse-guard counterfactual (1 = the guard would have
// skipped this post — depth-0 book or ~5m momentum against) on EVERY post row.
// R101 (auditor r27/r28 DO-THIS 1): the guard's decision INPUTS ride every row too —
// mom5m (nil = momentum ring unobservable), the momentum threshold in force, the reason the
// gate would fire (” on a clean pass) and the depth source ('none' = book unobservable).
func (s *Store) InsertMakerAttempt(ctx context.Context, platform, ticker, side, source string, postPx, spreadC, depth float64, wouldGate int, mom5m *float64, momGateC float64, gateReason, depthSrc string) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
INSERT INTO maker_fill_stats(ts, platform, ticker, side, source, post_px, spread_cents, book_depth, filled, would_gate, mom5m, mom_gate_c, gate_reason, depth_src)
VALUES(?,?,?,?,?,?,?,?,-1,?,?,?,?,?)`, nowRFC(), platform, ticker, side, source, postPx, spreadC, depth, wouldGate, mom5m, momGateC, gateReason, depthSrc)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// InsertMakerGated — R99 bug 194 (edge 30): record a maker post the adverse guard REFUSED.
// filled=-2 keeps the row out of MakerFillSummary's fill-rate (it counts filled IN (0,1) only)
// and out of the pending sweep (which touches filled=-1 only) — the row exists purely so
// would_gate=1 lands and the guard's counterfactual is measurable. Before this, the armed guard
// returned BEFORE the insert, so the would_gate column was all-0 by construction (n=1,129) and
// edge 30 was unmeasurable. R101: gate inputs stamped (see InsertMakerAttempt).
func (s *Store) InsertMakerGated(ctx context.Context, platform, ticker, side, source string, postPx, spreadC, depth float64, mom5m *float64, momGateC float64, gateReason, depthSrc string) error {
	_, err := s.db.ExecContext(ctx, `
INSERT INTO maker_fill_stats(ts, platform, ticker, side, source, post_px, spread_cents, book_depth, filled, would_gate, mom5m, mom_gate_c, gate_reason, depth_src)
VALUES(?,?,?,?,?,?,?,?,-2,1,?,?,?,?)`, nowRFC(), platform, ticker, side, source, postPx, spreadC, depth, mom5m, momGateC, gateReason, depthSrc)
	return err
}

// MarkMakerFilled books a touch-through fill on a pending attempt.
func (s *Store) MarkMakerFilled(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET filled=1, fill_ts=? WHERE id=? AND filled=-1`, nowRFC(), id)
	return err
}

// MarkMakerFilledWithFee atomically closes a maker attempt with the exact aggregate-order fee or
// rebate normalized per filled contract and the authority used at fill time. A signed netFeePC is
// split into non-negative fee/rebate columns so a rebate can never be mistaken for a free market.
func (s *Store) MarkMakerFilledWithFee(ctx context.Context, id int64, netFeePC float64, source string) error {
	source = strings.TrimSpace(source)
	if id <= 0 || source == "" || math.IsNaN(netFeePC) || math.IsInf(netFeePC, 0) {
		return fmt.Errorf("invalid maker fee receipt")
	}
	fee, rebate := netFeePC, 0.0
	if netFeePC < 0 {
		fee, rebate = 0, -netFeePC
	}
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats
SET filled=1,fill_ts=?,maker_fee_pc=?,maker_rebate_pc=?,maker_fee_source=?
WHERE id=? AND filled=-1`, nowRFC(), fee, rebate, source, id)
	return err
}

// MarkMakerExpired closes a pending attempt that never filled (or was chase-canceled).
func (s *Store) MarkMakerExpired(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET filled=0, expire_ts=? WHERE id=? AND filled=-1`, nowRFC(), id)
	return err
}

// MarkMakerExpiredRule — R106 (paper-maker live parity): close a pending attempt AND tag which
// cancel rule fired (moved-1c | close-3min | expire-10m), so the auditor can grade the simulated
// cancel layers against the live ones (R103 smoke-proven REST sweep + WS tick).
func (s *Store) MarkMakerExpiredRule(ctx context.Context, id int64, rule string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET filled=0, expire_ts=?, cancel_rule=? WHERE id=? AND filled=-1`, nowRFC(), rule, id)
	return err
}

// ExpireOrphanMakerAttempts — R106 (auditor bug 282): close every pending (filled=-1) attempt
// older than the given age. pendingMakers is memory-only, so rows opened before a restart could
// never be filled/expired by the running process — they sat pending forever and biased fill-rate
// stats. Boot one-shot; tagged 'orphan-boot'.
func (s *Store) ExpireOrphanMakerAttempts(ctx context.Context, olderThan time.Duration) (int64, error) {
	cut := time.Now().UTC().Add(-olderThan).Format(time.RFC3339)
	res, err := s.db.ExecContext(ctx,
		`UPDATE maker_fill_stats SET filled=0, expire_ts=?, cancel_rule='orphan-boot' WHERE filled=-1 AND ts < ?`,
		nowRFC(), cut)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// DBForTest exposes the raw handle to package-external tests (R107 pins query columns the
// reader API doesn't surface). Never for app paths.
func (s *Store) DBForTest() *sql.DB { return s.db }

// SetMakerLateFill — R107: the canceled post's level WAS traded through inside the 30-min watch
// window; secs = cancel→cross delay. Only ever stamps canceled rows (filled=0) once (late_fill
// stays NULL until the watch concludes).
func (s *Store) SetMakerLateFill(ctx context.Context, id int64, secs int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET late_fill=1, late_fill_s=? WHERE id=? AND filled=0 AND late_fill IS NULL`, secs, id)
	return err
}

// SetMakerLateNo — R107: the watch expired with no cross — patience would NOT have been paid.
func (s *Store) SetMakerLateNo(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET late_fill=0 WHERE id=? AND filled=0 AND late_fill IS NULL`, id)
	return err
}

// SetMakerAdverse stamps the post-fill 5-minute drift (adverse-selection measurement).
func (s *Store) SetMakerAdverse(ctx context.Context, id int64, drift float64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET adverse_5m=?,adverse_5m_at=? WHERE id=?`, drift, nowRFC(), id)
	return err
}

// SetMakerRoute (R122, router.go): stamps the queue-aware router's decision + estimate inputs on
// a maker attempt row, so the auditor/verdict engine can grade the router against realized fills.
func (s *Store) SetMakerRoute(ctx context.Context, id int64, reason string, queue, ratePM, etaS, horizonS float64) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE maker_fill_stats SET route_reason=?, route_queue=?, route_rate_pm=?, route_eta_s=?, route_horizon_s=? WHERE id=?`,
		reason, queue, ratePM, etaS, horizonS, id)
	return err
}

// FamilyEdge is one signal family's realized-edge aggregate for the R115 verdict engine:
// fee-net $/contract observations (side-adjusted settle − entry − modeled taker fee 0.07·p·(1−p)).
type FamilyEdge struct {
	Family    string
	N         int // distinct market tickers; the confidence-sequence sample bucket
	Rows      int // settled signal rows folded into those market means
	Mean      float64
	SD        float64
	FeePC     float64 // mean modeled fee per contract (the INVERT drag unit)
	PolyShare float64 // R116: share of rows priced on poly-int (platform='polymarket') — an UNBETTABLE venue; >0.5 ⇒ VENUE-LOCKED verdict
	KalShare  float64 // R130: share of graded rows priced on kalshi — venue membership for the automatic follower roster
	PusShare  float64 // R130: share of graded rows priced on polyus
	// InvHaircutPC — R118 honest invert cost: mean per-contract SPREAD HAIRCUT the inverted twin
	// pays to actually enter the other side. Per row: half the recorded bid/ask spread
	// (entry_price is a mark ≈ mid, so other-side ask ≈ mid + spread/2); rows with no recorded
	// spread (spread_cents<=0, most families) charge a conservative 2¢ default instead of the old
	// silent zero ("spread-free upper bound", R116). Taker fee needs NO inverted-side recompute:
	// 0.07·p·(1−p) is symmetric in p ↔ 1−p, so the inverted side's fee equals the original's.
	InvHaircutPC float64
}

// FamilyVenueEdge is the same unit economics split by executable venue. It prevents an edge on
// one book from funding the opposite venue when their realized directions differ.
type FamilyVenueEdge struct {
	Family, Platform              string
	N, Rows                       int
	Mean, SD, FeePC, InvHaircutPC float64
}

func (s *Store) FamilyVenueEdgeStats(ctx context.Context, since time.Time) ([]FamilyVenueEdge, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT signal_type, platform, COUNT(*), SUM(rows_n), AVG(pc), AVG(pc*pc), AVG(fee), AVG(invhc) FROM (
  SELECT signal_type, platform, ticker,
	COUNT(*) AS rows_n,
    AVG((CASE WHEN LOWER(TRIM(side)) IN ('no','down') THEN (1.0-settle_val) ELSE settle_val END) - entry_price - fee) AS pc,
    AVG(fee) AS fee, AVG(CASE WHEN spread_cents > 0 THEN spread_cents/200.0 ELSE 0.02 END) AS invhc
  FROM (
    SELECT *, COALESCE(fee_pc,
      CASE platform
        WHEN 'kalshi' THEN CAST(0.07*entry_price*(1.0-entry_price)*100.0 + 0.999999999 AS INTEGER)/100.0
        WHEN 'polyus' THEN ROUND(0.06*entry_price*(1.0-entry_price)*100.0)/100.0
        ELSE ROUND(0.05*entry_price*(1.0-entry_price),5)
      END) AS fee
    FROM signal_log
    WHERE resolved=1 AND settle_val IS NOT NULL AND settle_val >= 0 AND settle_val <= 1
      AND entry_price > 0 AND entry_price < 1 AND ts >= ?
      -- Fail-closed fee sentinels (for example 1e9 for an unsupported venue schedule) are
      -- operational rejection values, not economic observations.  Letting one enter AVG(pc)
      -- can turn an otherwise bounded binary-contract family into a five-digit verdict.
      -- NULL remains valid legacy evidence because the CASE below supplies the documented
      -- venue fallback; a real per-contract fee/rebate must itself fit the binary payoff scale.
      AND (fee_pc IS NULL OR (fee_pc >= -1 AND fee_pc <= 1))
      AND platform IN ('kalshi','polyus')
  ) GROUP BY signal_type,platform,ticker
) GROUP BY signal_type,platform HAVING COUNT(*) >= 1 ORDER BY signal_type,platform`, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FamilyVenueEdge
	for rows.Next() {
		var f FamilyVenueEdge
		var ex2 float64
		if err := rows.Scan(&f.Family, &f.Platform, &f.N, &f.Rows, &f.Mean, &ex2, &f.FeePC, &f.InvHaircutPC); err != nil {
			return nil, err
		}
		if v := ex2 - f.Mean*f.Mean; v > 0 && f.N > 1 {
			f.SD = math.Sqrt(v * float64(f.N) / float64(f.N-1))
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// FamilyEdgeStats aggregates realized fee-net edge per signal family since a cutoff (R115 verdict
// engine input). Mean/SD computed in SQL so a 750k-row grade table never crosses the wire.
func (s *Store) FamilyEdgeStats(ctx context.Context, since time.Time) ([]FamilyEdge, error) {
	// R122 (auditor 369): side flip must catch poly Down rows too — LOWER(TRIM(side)) IN
	// ('no','down'). Plain ='no' silently graded Down rows as YES-side, manufacturing the fake
	// invert:pcrypto +18.7¢ PROVEN+ (n=7k). Same fix at the MLUniverse + MakerSettledEdge CASEs.
	rows, err := s.db.QueryContext(ctx, `
SELECT signal_type, COUNT(*), SUM(rows_n), AVG(pc), AVG(pc*pc), AVG(fee), AVG(polyint), AVG(invhc), AVG(kal), AVG(pus) FROM (
  SELECT signal_type, platform, ticker,
	COUNT(*) AS rows_n,
    AVG((CASE WHEN LOWER(TRIM(side)) IN ('no','down') THEN (1.0-settle_val) ELSE settle_val END) - entry_price - fee) AS pc,
    AVG(fee) AS fee,
    (CASE WHEN platform='polymarket' THEN 1.0 ELSE 0.0 END) AS polyint,
    AVG(CASE WHEN spread_cents > 0 THEN spread_cents/200.0 ELSE 0.02 END) AS invhc,
    (CASE WHEN platform='kalshi' THEN 1.0 ELSE 0.0 END) AS kal,
    (CASE WHEN platform='polyus' THEN 1.0 ELSE 0.0 END) AS pus
  FROM (
    SELECT *, COALESCE(fee_pc,
      CASE platform
        WHEN 'kalshi' THEN CAST(0.07*entry_price*(1.0-entry_price)*100.0 + 0.999999999 AS INTEGER)/100.0
        WHEN 'polyus' THEN ROUND(0.06*entry_price*(1.0-entry_price)*100.0)/100.0
        ELSE ROUND(0.05*entry_price*(1.0-entry_price),5)
      END) AS fee
    FROM signal_log
    WHERE resolved=1 AND settle_val IS NOT NULL AND settle_val >= 0 AND settle_val <= 1
      AND entry_price > 0 AND entry_price < 1 AND ts >= ?
      -- See FamilyVenueEdgeStats: fail-closed fee sentinels block execution but are not a
      -- measured loss.  Exclude them instead of allowing operational state to poison alpha.
      AND (fee_pc IS NULL OR (fee_pc >= -1 AND fee_pc <= 1))
  ) GROUP BY signal_type,platform,ticker
) GROUP BY signal_type HAVING COUNT(*) >= 20 ORDER BY COUNT(*) DESC`, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FamilyEdge
	for rows.Next() {
		var f FamilyEdge
		var ex2 float64
		if err := rows.Scan(&f.Family, &f.N, &f.Rows, &f.Mean, &ex2, &f.FeePC, &f.PolyShare, &f.InvHaircutPC, &f.KalShare, &f.PusShare); err != nil {
			return nil, err
		}
		if v := ex2 - f.Mean*f.Mean; v > 0 && f.N > 1 {
			f.SD = math.Sqrt(v * float64(f.N) / float64(f.N-1))
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// FamilyCLV is one strategy family's closing-line-value aggregate (R127-D B4, server/clv.go).
type FamilyCLV struct {
	Family  string
	N       int     // raw settled CLV rows retained for operational transparency
	Markets int     // coverage n: distinct venue+ticker contracts after within-contract averaging
	Mean    float64 // mean of market-level CLV means, $/contract and side-directional
	SD      float64 // sample SD across market-level means
}

// FamilyCLVStats — R127-D (B4): per-family CLV over resolved rows with a post-entry price path.
// The last post_path mark approximates the pre-settlement close (sampled/backfilled marks — see
// the clv.go header for the honest caveats). Both post_path writers (AppendSignalPostPath and the
// candle backfill) store marks ALREADY side-adjusted and '%.3f'-formatted (exactly 5 chars in
// [0,1]), so: tail = the whole path when it has no comma, else the last 5 chars — accepted ONLY
// when the 6th-from-last char is the separating comma. Anything else (legacy odd-width tokens)
// yields NULL and drops out of COUNT/AVG instead of corrupting the mean with digit
// concatenation. entry_price is the side's own cost, so clv = tail − entry needs no side CASE.
// One grouped scan, bounded by the caller's ctx; families under 5 venue+ticker contracts are
// noise and elided. A shared ticker label on Kalshi and PolyUS is two different contracts.
func (s *Store) FamilyCLVStats(ctx context.Context, since time.Time) ([]FamilyCLV, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT signal_type,COUNT(*) markets,SUM(rows_n) settled_rows,
       AVG(market_clv),AVG(market_clv*market_clv)
FROM (
  SELECT signal_type,platform,ticker,COUNT(*) rows_n,AVG(clv) market_clv
  FROM (
    SELECT signal_type,LOWER(TRIM(platform)) platform,TRIM(ticker) ticker,
      (CASE WHEN instr(post_path, ',') = 0 THEN CAST(post_path AS REAL)
            WHEN substr(post_path, -6, 1) = ',' THEN CAST(substr(post_path, -5) AS REAL)
            ELSE NULL END) - entry_price AS clv
    FROM signal_log
    WHERE resolved = 1 AND post_path != '' AND entry_price > 0 AND entry_price < 1
      AND TRIM(platform) != '' AND TRIM(ticker) != '' AND ts >= ?
  ) WHERE clv IS NOT NULL
  GROUP BY signal_type,platform,ticker
)
GROUP BY signal_type HAVING COUNT(*) >= 5 ORDER BY COUNT(*) DESC`, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FamilyCLV
	for rows.Next() {
		var f FamilyCLV
		var ex2 float64
		if err := rows.Scan(&f.Family, &f.Markets, &f.N, &f.Mean, &ex2); err != nil {
			return nil, err
		}
		if v := ex2 - f.Mean*f.Mean; v > 0 && f.Markets > 1 {
			f.SD = math.Sqrt(v * float64(f.Markets) / float64(f.Markets-1))
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// MLUniversePoint is one (decile, price-grid) aggregate from the resolved signal_log universe —
// the R118 market-implied calibration contrast for the ML accuracy card. PK is the entry price
// rounded to 3 decimals × 1000 (cent-grid in practice), Dec the calibration decile 0..9, Wins the
// rows whose side-adjusted settle ≥ 0.5. SumP/SumY rebuild exact bucket means in the server.
type MLUniversePoint struct {
	Dec  int
	PK   int
	N    int
	Wins int
	SumP float64
	SumY float64
}

// MLUniverseCell is one (series, venue) aggregate from the same universe pass: the server folds
// series → genre (series_categories map) so this layer stays genre-agnostic. Hi*/Lo* are the
// discrimination counters (rows priced >0.60 / <0.40 and how many of each settled as wins).
type MLUniverseCell struct {
	Series string
	Venue  string
	N      int
	SumP   float64
	SumY   float64
	HiN    int
	HiW    int
	LoN    int
	LoW    int
}

// MLUniverseCalibration aggregates the ENTIRE resolved signal_log (platform != 'polymarket' — an
// unbettable venue) into (a) price-grid calibration points and (b) per-series×venue cells, all in
// SQL (R118: ~312k rows never cross the wire row-by-row). probability = entry_price (the price of
// the side taken IS its market-implied probability — no model p_win exists on signal_log, none is
// fabricated); outcome = side-adjusted settle_val, the same CASE FamilyEdgeStats uses, falling
// back to the SIDE-RELATIVE won flag (already side-adjusted by definition) on the ~83k resolved
// rows that carry won but no scalar settle_val. Runs on the weekly ML-accuracy refresh only, so
// two grouped scans are acceptable.
func (s *Store) MLUniverseCalibration(ctx context.Context) ([]MLUniversePoint, []MLUniverseCell, error) {
	const base = `
  SELECT UPPER(CASE WHEN INSTR(ticker,'-')>1 THEN SUBSTR(ticker,1,INSTR(ticker,'-')-1) ELSE ticker END) AS series,
    LOWER(COALESCE(platform,'kalshi')) AS venue,
    entry_price AS p,
    (CASE WHEN settle_val IS NOT NULL AND settle_val >= 0
       THEN (CASE WHEN LOWER(TRIM(side)) IN ('no','down') THEN (1.0-settle_val) ELSE settle_val END)
       ELSE CAST(won AS REAL) END) AS y
  FROM signal_log
  WHERE resolved=1 AND entry_price > 0 AND entry_price < 1 AND platform != 'polymarket'
    AND ((settle_val IS NOT NULL AND settle_val >= 0) OR won IN (0,1))`
	rows, err := s.db.QueryContext(ctx, `
SELECT CASE WHEN CAST(p*10 AS INTEGER) > 9 THEN 9 ELSE CAST(p*10 AS INTEGER) END AS dec,
  CAST(ROUND(p*1000) AS INTEGER) AS pk,
  COUNT(*), SUM(CASE WHEN y >= 0.5 THEN 1 ELSE 0 END), SUM(p), SUM(y)
FROM (`+base+`) GROUP BY dec, pk`)
	if err != nil {
		return nil, nil, err
	}
	var pts []MLUniversePoint
	for rows.Next() {
		var pt MLUniversePoint
		if err := rows.Scan(&pt.Dec, &pt.PK, &pt.N, &pt.Wins, &pt.SumP, &pt.SumY); err != nil {
			rows.Close()
			return nil, nil, err
		}
		pts = append(pts, pt)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	rows, err = s.db.QueryContext(ctx, `
SELECT series, venue, COUNT(*), SUM(p), SUM(y),
  SUM(CASE WHEN p > 0.60 THEN 1 ELSE 0 END),
  SUM(CASE WHEN p > 0.60 AND y >= 0.5 THEN 1 ELSE 0 END),
  SUM(CASE WHEN p < 0.40 THEN 1 ELSE 0 END),
  SUM(CASE WHEN p < 0.40 AND y >= 0.5 THEN 1 ELSE 0 END)
FROM (`+base+`) GROUP BY series, venue`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var cells []MLUniverseCell
	for rows.Next() {
		var c MLUniverseCell
		if err := rows.Scan(&c.Series, &c.Venue, &c.N, &c.SumP, &c.SumY, &c.HiN, &c.HiW, &c.LoN, &c.LoW); err != nil {
			return nil, nil, err
		}
		cells = append(cells, c)
	}
	return pts, cells, rows.Err()
}

// FamilyFirstRows returns min(ts) per signal family since a cutoff (R117 promotion min-days rail:
// "7d of rows" means the FAMILY'S DATA AGE, not when the pipeline first sighted it). One grouped
// query; the promotion sweep caches the result per pass.
func (s *Store) FamilyFirstRows(ctx context.Context, since time.Time) (map[string]time.Time, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT signal_type, MIN(ts) FROM signal_log
WHERE ts >= ? GROUP BY signal_type`, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var fam, tsStr string
		if err := rows.Scan(&fam, &tsStr); err != nil {
			return nil, err
		}
		if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
			out[fam] = t
		}
	}
	return out, rows.Err()
}

// MakerSettledEdge aggregates the settled maker-fill family (R115 verdict engine): per-contract
// net = side-adjusted settle − post price (pure-maker fee ≈ 0 on most Kalshi series).
func (s *Store) MakerSettledEdge(ctx context.Context) (n int, mean, sd float64, err error) {
	var ex, ex2 sql.NullFloat64
	err = s.db.QueryRowContext(ctx, `
SELECT COUNT(*), AVG(pc), AVG(pc*pc) FROM (
  SELECT (CASE WHEN LOWER(TRIM(side)) IN ('no','down') THEN (1.0-settle_val) ELSE settle_val END) - post_px AS pc
  FROM maker_fill_stats WHERE filled=1 AND settle_val IS NOT NULL AND post_px > 0 AND post_px < 1
)`).Scan(&n, &ex, &ex2)
	if err != nil || n == 0 {
		return n, 0, 0, err
	}
	mean = ex.Float64
	if v := ex2.Float64 - mean*mean; v > 0 && n > 1 {
		sd = math.Sqrt(v * float64(n) / float64(n-1))
	}
	return n, mean, sd, nil
}

// MakerSettledMarketEdge is the confidence-safe version of MakerSettledEdge. Repeated fills on
// one venue+ticker are averaged before the outer mean/SD; raw settled fill rows remain available
// separately so operational volume is never confused with independent statistical evidence.
func (s *Store) MakerSettledMarketEdge(ctx context.Context) (markets, settledRows int, mean, sd float64, err error) {
	var ex, ex2 sql.NullFloat64
	err = s.db.QueryRowContext(ctx, `
SELECT COUNT(*), COALESCE(SUM(raw_n),0), AVG(pc), AVG(pc*pc) FROM (
  SELECT LOWER(TRIM(platform)) AS platform, ticker, COUNT(*) AS raw_n,
         AVG((CASE WHEN LOWER(TRIM(side)) IN ('no','down') THEN (1.0-settle_val) ELSE settle_val END) - post_px) AS pc
  FROM maker_fill_stats
  WHERE filled=1 AND settle_val IS NOT NULL AND post_px > 0 AND post_px < 1 AND TRIM(ticker) <> ''
  GROUP BY LOWER(TRIM(platform)), ticker
)`).Scan(&markets, &settledRows, &ex, &ex2)
	if err != nil || markets == 0 {
		return markets, settledRows, 0, 0, err
	}
	mean = ex.Float64
	if variance := ex2.Float64 - mean*mean; variance > 0 && markets > 1 {
		sd = math.Sqrt(variance * float64(markets) / float64(markets-1))
	}
	return markets, settledRows, mean, sd, nil
}

// MakerDepthSrcCounts returns depth_src → row count on maker posts since a cutoff (R115 Part 3:
// the depth_src=none rate is the before→after measure for the invisible-books fix).
func (s *Store) MakerDepthSrcCounts(ctx context.Context, since time.Time) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT COALESCE(NULLIF(depth_src,''),'none'), COUNT(*)
FROM maker_fill_stats WHERE ts >= ? GROUP BY 1`, since.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}

// SetMakerQueueState stamps the R115 queue-model tags on a maker attempt: visible queue ahead at
// post, queue still ahead at the terminal event, and contracts the tape filled (partials allowed).
func (s *Store) SetMakerQueueState(ctx context.Context, id int64, ahead, left, fillCt float64) error {
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET queue_ahead=?, queue_left=?, fill_ct=? WHERE id=?`, ahead, left, fillCt, id)
	return err
}

// SetMakerFillCheck stamps the R115 fill-moment re-check: market price at fill, decision-time vs
// fill-time sidecar p_win (0/NULL when unknown), and the rule that produced the fill.
func (s *Store) SetMakerFillCheck(ctx context.Context, id int64, fillPx, decPWin, fillPWin float64, fillPWinOK bool, rule string) error {
	var fp any
	if fillPWinOK {
		fp = fillPWin
	}
	var dp any
	if decPWin > 0 {
		dp = decPWin
	}
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET fill_px=?, dec_pwin=?, fill_pwin=?, fill_rule=? WHERE id=?`, fillPx, dp, fp, rule, id)
	return err
}

// MakerFillSummary returns the MEASURED maker fill-rate and mean post-fill drift for a venue
// (audit §7: replaces Replay's invented 0.85/0.15 constants once enough attempts accrue).
func (s *Store) MakerFillSummary(ctx context.Context, platform string) (fillRate, adverseMean float64, n int, err error) {
	var filled, total int
	var adv sql.NullFloat64
	err = s.db.QueryRowContext(ctx, `
SELECT COALESCE(SUM(CASE WHEN filled=1 THEN 1 ELSE 0 END),0), COUNT(*),
       AVG(CASE WHEN filled=1 THEN adverse_5m END)
FROM maker_fill_stats WHERE platform=? AND filled IN (0,1)`, platform).Scan(&filled, &total, &adv)
	if err != nil || total == 0 {
		return 0, 0, total, err
	}
	return float64(filled) / float64(total), adv.Float64, total, nil
}

// NetBuyFlow returns Σ(BUY usdc) − Σ(SELL usdc) on a market from the tracked-wallet tape since
// `sinceUnix` (audit Q2 #3: the staleness/decay gate — a consensus whose members are net selling
// is dissolving regardless of their held positions). Zero-request: reads only stored trades.
func (s *Store) NetBuyFlow(ctx context.Context, conditionID string, sinceUnix int64) (float64, error) {
	var net sql.NullFloat64
	err := s.db.QueryRowContext(ctx, `
SELECT SUM(CASE WHEN side='BUY' THEN usdc_size ELSE -usdc_size END)
FROM poly_trader_trades WHERE condition_id=? AND ts >= ?`, conditionID, sinceUnix).Scan(&net)
	return net.Float64, err
}

// PolyTraderRow is a per-wallet summary with the wallet's REAL resolved hit rate.
type PolyTraderRow struct {
	Wallet   string  `json:"wallet"`
	Name     string  `json:"name"`
	BestRank int     `json:"best_rank"`
	Profit   float64 `json:"profit"`
	Trades   int     `json:"trades"`
	Resolved int     `json:"resolved"`
	Wins     int     `json:"wins"`
	HitRate  float64 `json:"hit_rate"`
	LastSeen string  `json:"last_seen"`
}

// ListPolyTraders returns the per-wallet smart-money summary (rank, profit, trade count, and
// resolved hit rate), profit-sorted — the track record we mine.
// R124: per-wallet counts span main + the ptt archive via wallet-indexed correlated subqueries
// (both files carry a wallet index — cheaper than materializing a 4.5M-row union for a join).
// LEFT-JOIN zero semantics preserved: no trades → 0/0/0.
func (s *Store) ListPolyTraders(ctx context.Context) ([]PolyTraderRow, error) {
	rows, done, err := s.pttUnionRows(ctx, `
SELECT t.wallet, COALESCE(t.name,''), t.best_rank, t.profit, COALESCE(t.last_seen,''),
       (SELECT COUNT(id) FROM main.poly_trader_trades tt WHERE tt.wallet=t.wallet)
     + (SELECT COUNT(id) FROM archive.poly_trader_trades tt WHERE tt.wallet=t.wallet),
       (SELECT COALESCE(SUM(CASE WHEN tt.resolved=1 THEN 1 ELSE 0 END),0) FROM main.poly_trader_trades tt WHERE tt.wallet=t.wallet)
     + (SELECT COALESCE(SUM(CASE WHEN tt.resolved=1 THEN 1 ELSE 0 END),0) FROM archive.poly_trader_trades tt WHERE tt.wallet=t.wallet),
       (SELECT COALESCE(SUM(CASE WHEN tt.won=1 THEN 1 ELSE 0 END),0) FROM main.poly_trader_trades tt WHERE tt.wallet=t.wallet)
     + (SELECT COALESCE(SUM(CASE WHEN tt.won=1 THEN 1 ELSE 0 END),0) FROM archive.poly_trader_trades tt WHERE tt.wallet=t.wallet)
FROM poly_traders t ORDER BY t.profit DESC`, `
SELECT t.wallet, COALESCE(t.name,''), t.best_rank, t.profit, COALESCE(t.last_seen,''),
       COUNT(tt.id), COALESCE(SUM(CASE WHEN tt.resolved=1 THEN 1 ELSE 0 END),0),
       COALESCE(SUM(CASE WHEN tt.won=1 THEN 1 ELSE 0 END),0)
FROM poly_traders t LEFT JOIN poly_trader_trades tt ON tt.wallet=t.wallet
GROUP BY t.wallet ORDER BY t.profit DESC`)
	if err != nil {
		return nil, err
	}
	defer done()
	defer rows.Close()
	var out []PolyTraderRow
	for rows.Next() {
		var r PolyTraderRow
		if err := rows.Scan(&r.Wallet, &r.Name, &r.BestRank, &r.Profit, &r.LastSeen,
			&r.Trades, &r.Resolved, &r.Wins); err != nil {
			return nil, err
		}
		if r.Resolved > 0 {
			r.HitRate = float64(r.Wins) / float64(r.Resolved)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TraderTradeRow is one logged smart-money trade with its outcome, for export/mining.
type TraderTradeRow struct {
	Wallet      string  `json:"wallet"`
	TS          int64   `json:"ts"`
	ConditionID string  `json:"condition_id"`
	Title       string  `json:"title"`
	Outcome     string  `json:"outcome"`
	Side        string  `json:"side"`
	Size        float64 `json:"size"`
	Price       float64 `json:"price"`
	UsdcSize    float64 `json:"usdc_size"`
	Resolved    int     `json:"resolved"`
	Won         int     `json:"won"` // 1/0 once resolved, -1 while open
}

// ListPolyTraderTrades returns recent smart-money trades (newest first) with outcomes, capped
// at limit — the raw dataset for edge discovery. limit<=0 returns all.
// R124 disposition: HOT-ONLY by design. The export caller caps at 8000 newest-first (≈ minutes
// of tape at daytime ingest), far inside the 30d hot window; full history is the analysis-copy
// ATTACH pattern (archive.go header). An unindexed ts-sort across the 4.5M-row archive per
// export click would be pure cost for rows the cap can never reach.
func (s *Store) ListPolyTraderTrades(ctx context.Context, limit int) ([]TraderTradeRow, error) {
	q := `
SELECT wallet, ts, condition_id, COALESCE(title,''), COALESCE(outcome,''), COALESCE(side,''),
       size, price, usdc_size, resolved, won
FROM poly_trader_trades ORDER BY resolved DESC, ts DESC`
	if limit > 0 {
		q += " LIMIT " + strconv.Itoa(limit)
	}
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TraderTradeRow
	for rows.Next() {
		var r TraderTradeRow
		var won sql.NullInt64
		if err := rows.Scan(&r.Wallet, &r.TS, &r.ConditionID, &r.Title, &r.Outcome, &r.Side,
			&r.Size, &r.Price, &r.UsdcSize, &r.Resolved, &won); err != nil {
			return nil, err
		}
		if won.Valid {
			r.Won = int(won.Int64)
		} else {
			r.Won = -1
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// CloseParlay closes a parlay at an early-exit mark, BOOKING its P&L (audit #9: the old close only
// set status='closed' — no payout, no realized, no timestamp, so closed-at-TP parlays vanished from
// both the open book and settled history and TP performance was untrackable). payout = the exit
// mark (0..1); realized = contracts×mark − stake − fees (entry + exit).
func (s *Store) CloseParlayIfOpen(ctx context.Context, id int64, payout, realized float64) (bool, error) {
	return s.finishOpenParlay(ctx, id, "closed", "settled", payout, realized, "combo-early-close")
}

// CloseParlay retains the established API while inheriting open-only, race-safe semantics.
// Call CloseParlayIfOpen when the caller must distinguish a raced/no-op close.
func (s *Store) CloseParlay(ctx context.Context, id int64, payout, realized float64) error {
	_, err := s.CloseParlayIfOpen(ctx, id, payout, realized)
	return err
}

// Live-combo execution states and receipt schema.
const (
	LiveComboDispatched      = "dispatched"
	LiveComboPartial         = "partial"
	LiveComboFilled          = "filled"
	LiveComboUnfilled        = "unfilled"
	LiveComboAccepted        = "accepted" // legacy pre-reconciliation state; never fee-net reportable
	LiveComboAcceptAmbiguous = "accept_ambiguous"
	LiveComboLegacyUnknown   = "legacy_unknown"
)

// LiveComboInsert is the durable dispatch receipt for one AcceptQuote call (or transport-ambiguous
// call). It is not a fill. ConfirmLiveComboExecution later replaces the estimated fee with the
// authoritative cumulative fill fee and advances dispatched -> partial/full.
type LiveComboInsert struct {
	Market                                               string
	Collection                                           string
	LegsJSON                                             string
	ModelCohort                                          string
	Fair                                                 float64
	ProdPrice                                            float64
	Quote                                                float64
	Contracts                                            float64
	AcceptedFee                                          float64
	FeeSource                                            string
	FeeKnown                                             bool
	AcceptState                                          string
	QuotesSeen                                           string
	RFQID, QuoteID, PromotionIntentID, RFQCreatorOrderID string
}

func (s *Store) MarkLiveComboUnfilled(ctx context.Context, comboID int64, market, source, detail string) error {
	if comboID <= 0 || strings.TrimSpace(market) == "" || strings.TrimSpace(source) == "" {
		return fmt.Errorf("invalid live combo unfilled receipt")
	}
	receipt, _ := json.Marshal(map[string]string{"source": source, "detail": detail})
	res, err := s.db.ExecContext(ctx, `UPDATE live_combos SET accept_state=?,filled_contracts=0,
	 fill_receipt=?,settled=1,payout=0,realized=0,settled_ts=? WHERE id=? AND market=? AND settled=0
	 AND accept_state IN (?,?,?)`, LiveComboUnfilled, string(receipt), nowRFC(), comboID, market,
		LiveComboDispatched, LiveComboPartial, LiveComboAcceptAmbiguous)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("live combo terminal-unfilled receipt matched %d rows", n)
	}
	return nil
}

// InsertLiveCombo records an RFQ dispatch with the current exact fee schedule. That estimate is
// sufficient for pre-submit rails, but never for realized P&L until authoritative fills reconcile.
func (s *Store) InsertLiveCombo(ctx context.Context, in LiveComboInsert) error {
	in.Market = strings.TrimSpace(in.Market)
	in.FeeSource = strings.TrimSpace(in.FeeSource)
	in.AcceptState = strings.TrimSpace(in.AcceptState)
	if in.Market == "" || in.LegsJSON == "" || in.Contracts <= 0 || in.Quote <= 0 || in.Quote >= 1 ||
		strings.TrimSpace(in.RFQID) == "" || strings.TrimSpace(in.QuoteID) == "" ||
		math.IsNaN(in.Contracts) || math.IsInf(in.Contracts, 0) || math.IsNaN(in.Quote) || math.IsInf(in.Quote, 0) {
		return fmt.Errorf("invalid live combo execution receipt")
	}
	if !in.FeeKnown || in.FeeSource == "" || in.AcceptedFee < 0 || math.IsNaN(in.AcceptedFee) || math.IsInf(in.AcceptedFee, 0) {
		return fmt.Errorf("live combo exact accepted fee is required")
	}
	if in.AcceptState != LiveComboDispatched && in.AcceptState != LiveComboAcceptAmbiguous {
		return fmt.Errorf("invalid live combo accept state %q", in.AcceptState)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO live_combos
		 (ts, market, collection, legs, model_cohort, fair, prod_price, quote, contracts, accepted_fee, fee_source, fee_known, accept_state, quotes_seen,
		  rfq_id,quote_id,promotion_intent_id,rfq_creator_order_id)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		time.Now().UTC().Format(time.RFC3339), in.Market, in.Collection, in.LegsJSON, strings.TrimSpace(in.ModelCohort), in.Fair, in.ProdPrice,
		in.Quote, in.Contracts, in.AcceptedFee, in.FeeSource, 1, in.AcceptState, in.QuotesSeen,
		in.RFQID, in.QuoteID, in.PromotionIntentID, in.RFQCreatorOrderID)
	return err
}

// ConfirmLiveComboExecution moves an accepted RFQ dispatch to partial/full only from the
// authoritative fills reader. The actual cumulative fill fee replaces the quote estimate.
func (s *Store) ConfirmLiveComboExecution(ctx context.Context, comboID int64, market, state string, filledQty,
	actualFee float64, feeSource, receipt string) error {
	if comboID <= 0 || strings.TrimSpace(market) == "" || (state != LiveComboPartial && state != LiveComboFilled) ||
		filledQty <= 0 || actualFee < 0 || strings.TrimSpace(feeSource) == "" || strings.TrimSpace(receipt) == "" {
		return fmt.Errorf("invalid live combo fill receipt")
	}
	var required float64
	if err := s.db.QueryRowContext(ctx, `SELECT contracts FROM live_combos WHERE id=? AND market=? AND settled=0
	 AND accept_state IN (?,?,?)`, comboID, market, LiveComboDispatched, LiveComboPartial, LiveComboAcceptAmbiguous).Scan(&required); err != nil {
		return err
	}
	if state == LiveComboFilled && filledQty+1e-9 < required {
		return fmt.Errorf("live combo full receipt %.6f below required %.6f", filledQty, required)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE live_combos SET accept_state=?,filled_contracts=?,accepted_fee=?,
	 fee_source=?,fee_known=1,fill_receipt=? WHERE id=? AND market=? AND settled=0 AND accept_state IN (?,?,?)`,
		state, filledQty, actualFee, feeSource, receipt, comboID, market, LiveComboDispatched, LiveComboPartial, LiveComboAcceptAmbiguous)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("live combo execution receipt update matched %d rows", n)
	}
	return nil
}

func (s *Store) SetLiveComboRFQCreatorOrderID(ctx context.Context, comboID int64, orderID string) error {
	orderID = strings.TrimSpace(orderID)
	if comboID <= 0 || orderID == "" {
		return fmt.Errorf("invalid RFQ creator order identity")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE live_combos SET rfq_creator_order_id=?
	 WHERE id=? AND settled=0 AND (rfq_creator_order_id='' OR rfq_creator_order_id=?)`, orderID, comboID, orderID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("RFQ creator order identity changed or combo missing")
	}
	return nil
}

type ResearchRouteBundleLiveFill struct {
	IntentID, MarketTicker, RFQID, QuoteID, QuoteStatus string
	Observed                                            time.Time
	FilledQuantity, AveragePrice, ActualFee             float64
	FeeSource, FillIDsJSON                              string
	PositionQuantity                                    float64
}

func (s *Store) InsertResearchRouteBundleLiveFill(ctx context.Context, r ResearchRouteBundleLiveFill) error {
	if r.Observed.IsZero() {
		r.Observed = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO research_route_bundle_live_fills
(intent_id,observed_ts,market_ticker,rfq_id,quote_id,quote_status,filled_quantity,average_price,
 actual_fee,fee_source,fill_ids_json,position_quantity) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.IntentID, r.Observed.UTC().Format(time.RFC3339Nano), r.MarketTicker, r.RFQID, r.QuoteID,
		r.QuoteStatus, r.FilledQuantity, r.AveragePrice, r.ActualFee, r.FeeSource, r.FillIDsJSON,
		r.PositionQuantity)
	return err
}

// BackfillTrade is one archived public trade destined for backfill_trades (cmd/kalshi-backfill).
// Prices are dollars (fixed-point era); TradeID is Kalshi's own unique id (the dedup key).
type BackfillTrade struct {
	TradeID     string
	Series      string
	Ticker      string
	CreatedTime string
	Count       float64
	YesPrice    float64
	NoPrice     float64
	TakerSide   string
}

// InsertBackfillTrades bulk-inserts one page of archived trades in a single transaction.
// INSERT OR IGNORE on the trade_id PK makes resumed/re-run backfills idempotent; returns how
// many rows were actually NEW.
func (s *Store) InsertBackfillTrades(ctx context.Context, trades []BackfillTrade) (int64, error) {
	if len(trades) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO backfill_trades
		(trade_id, series, ticker, created_time, count, yes_price, no_price, taker_side)
		VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	var n int64
	for _, t := range trades {
		res, err := stmt.ExecContext(ctx, t.TradeID, t.Series, t.Ticker, t.CreatedTime, t.Count, t.YesPrice, t.NoPrice, t.TakerSide)
		if err != nil {
			return n, err
		}
		if a, _ := res.RowsAffected(); a > 0 {
			n += a
		}
	}
	if err := tx.Commit(); err != nil {
		return n, err
	}
	return n, nil
}

// BackfillTradeCount reports stored rows for one series (progress display in cmd/kalshi-backfill).
func (s *Store) BackfillTradeCount(ctx context.Context, series string) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM backfill_trades WHERE series=?`, series).Scan(&n)
	return n, err
}

// KVGet returns one kv value ("" + false when absent) — the single-key read the backfill
// bookmarks use (KVPrefix walks a LIKE scan; this is a PK seek).
func (s *Store) KVGet(ctx context.Context, k string) (string, bool) {
	var v string
	if err := s.db.QueryRowContext(ctx, `SELECT v FROM kv WHERE k=?`, k).Scan(&v); err != nil {
		return "", false
	}
	return v, true
}

// LiveCombo is one real-money RFQ dispatch row (settlement begins only after accept_state=filled).
type LiveCombo struct {
	ID                                                   int64
	TS                                                   string
	Market                                               string
	Legs                                                 string
	ModelCohort                                          string
	Quote                                                float64
	Contracts                                            float64
	AcceptedFee                                          float64
	FeeSource                                            string
	FeeKnown                                             bool
	AcceptState                                          string
	RFQID, QuoteID, PromotionIntentID, RFQCreatorOrderID string
	FilledContracts                                      float64
}

// LiveComboSettlement is the persisted outcome. Reportable is true only after an authoritative
// full-fill receipt with exact cumulative fee; legacy, dispatched, partial and ambiguous rows stay
// visible without entering fee-net portfolio/proof totals.
type LiveComboSettlement struct {
	Realized    float64
	AcceptedFee float64
	FeeSource   string
	FeeKnown    bool
	AcceptState string
	Reportable  bool
}

// ListOpenLiveCombos returns accepted combos not yet settled, oldest first.
// ListLiveCombosAll returns EVERY live RFQ combo row (open + settled) as generic maps — the export's
// full real-money combo record (R9: the export previously carried none of the live-combo book).
func (s *Store) ListLiveCombosAll(ctx context.Context, limit int) ([]map[string]any, error) {
	if limit == 0 {
		limit = 2000
	} else if limit < 0 {
		limit = -1 // SQLite LIMIT -1 = complete retained history (Systems proof lane).
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT id, ts, market, collection, legs, model_cohort, fair, prod_price, quote, contracts,
       accepted_fee, fee_source, fee_known, accept_state, quotes_seen,
       rfq_id,quote_id,promotion_intent_id,rfq_creator_order_id,filled_contracts,fill_receipt,
       settled, payout, realized, settled_ts
FROM live_combos ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id int64
		var settled, feeKnown int
		var ts, market, collection, legs, modelCohort, feeSource, acceptState, quotesSeen, settledTS string
		var rfqID, quoteID, promotionIntentID, creatorOrderID, fillReceipt string
		var fair, prodPrice, quote, contracts, acceptedFee, filledContracts, payout, realized float64
		if err := rows.Scan(&id, &ts, &market, &collection, &legs, &modelCohort, &fair, &prodPrice, &quote,
			&contracts, &acceptedFee, &feeSource, &feeKnown, &acceptState, &quotesSeen,
			&rfqID, &quoteID, &promotionIntentID, &creatorOrderID, &filledContracts, &fillReceipt,
			&settled, &payout, &realized, &settledTS); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id": id, "ts": ts, "market": market, "collection": collection, "legs": json.RawMessage(legs),
			"model_cohort": modelCohort,
			"fair":         fair, "prod_price": prodPrice, "quote": quote, "contracts": contracts,
			"accepted_fee": acceptedFee, "fee_source": feeSource, "fee_known": feeKnown == 1, "accept_state": acceptState,
			"rfq_id": rfqID, "quote_id": quoteID, "promotion_intent_id": promotionIntentID,
			"rfq_creator_order_id": creatorOrderID, "filled_contracts": filledContracts, "fill_receipt": fillReceipt,
			// Eligibility is independent of settlement: confirmed exact-fee OPEN rows must remain
			// visible to Systems as open exposure. Consumers still require settled=1 for realized P&L.
			"fee_net_reportable": feeKnown == 1 && acceptState == LiveComboFilled,
			"quotes_seen":        quotesSeen, "settled": settled, "payout": payout, "realized": realized, "settled_ts": settledTS,
		})
	}
	return out, rows.Err()
}

func (s *Store) ListOpenLiveCombos(ctx context.Context, limit int) ([]LiveCombo, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, ts, market, legs, model_cohort, quote, contracts, accepted_fee, fee_source, fee_known, accept_state,
		 rfq_id,quote_id,promotion_intent_id,rfq_creator_order_id,filled_contracts
		 FROM live_combos WHERE settled=0
		 ORDER BY CASE WHEN accept_state IN ('dispatched','partial','accept_ambiguous') THEN 0 ELSE 1 END,id ASC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LiveCombo
	for rows.Next() {
		var c LiveCombo
		var feeKnown int
		if err := rows.Scan(&c.ID, &c.TS, &c.Market, &c.Legs, &c.ModelCohort, &c.Quote, &c.Contracts,
			&c.AcceptedFee, &c.FeeSource, &feeKnown, &c.AcceptState, &c.RFQID, &c.QuoteID,
			&c.PromotionIntentID, &c.RFQCreatorOrderID, &c.FilledContracts); err != nil {
			return nil, err
		}
		c.FeeKnown = feeKnown == 1
		out = append(out, c)
	}
	return out, rows.Err()
}

// SettleLiveCombo books payout and exact fee-net P&L as contracts*(payout-quote)-accepted_fee.
// Fee-unknown legacy rows retain their payout but get no reportable realized value.
func (s *Store) SettleLiveCombo(ctx context.Context, id int64, payout float64) (LiveComboSettlement, error) {
	var out LiveComboSettlement
	if payout < 0 || payout > 1 || math.IsNaN(payout) || math.IsInf(payout, 0) {
		return out, fmt.Errorf("invalid live combo payout %.6f", payout)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer func() { _ = tx.Rollback() }()
	var quote, contracts float64
	var feeKnown, settled int
	if err := tx.QueryRowContext(ctx,
		`SELECT quote, contracts, accepted_fee, fee_source, fee_known, accept_state, settled
		 FROM live_combos WHERE id=?`, id).Scan(&quote, &contracts, &out.AcceptedFee, &out.FeeSource,
		&feeKnown, &out.AcceptState, &settled); err != nil {
		return out, err
	}
	if settled != 0 {
		return out, fmt.Errorf("live combo %d already settled", id)
	}
	if contracts <= 0 {
		contracts = 1
	}
	out.FeeKnown = feeKnown == 1
	if out.FeeKnown {
		out.Realized = contracts*(payout-quote) - out.AcceptedFee
	}
	out.Reportable = out.FeeKnown && out.AcceptState == LiveComboFilled
	res, err := tx.ExecContext(ctx,
		`UPDATE live_combos SET settled=1, payout=?, realized=?, settled_ts=? WHERE id=? AND settled=0`,
		payout, out.Realized, nowRFC(), id)
	if err != nil {
		return LiveComboSettlement{}, err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return LiveComboSettlement{}, fmt.Errorf("live combo %d settlement raced", id)
	}
	if err := tx.Commit(); err != nil {
		return LiveComboSettlement{}, err
	}
	return out, nil
}

// LiveCombosRealized sums only confirmed, exact-fee settled RFQ combos. Migrated legacy and
// transport-ambiguous rows remain visible for reconciliation but cannot enter this fee-net total.
func (s *Store) LiveCombosRealized(ctx context.Context) (float64, error) {
	var v sql.NullFloat64
	err := s.db.QueryRowContext(ctx, `SELECT SUM(realized) FROM live_combos
		WHERE settled=1 AND fee_known=1 AND accept_state=?`, LiveComboFilled).Scan(&v)
	return v.Float64, err
}

// ParlaysRealized sums realized $ P&L across settled+closed parlays, excluding pre-guard artifacts.
func (s *Store) ParlaysRealized(ctx context.Context) (float64, error) {
	var v sql.NullFloat64
	err := s.db.QueryRowContext(ctx,
		`SELECT SUM(realized) FROM paper_parlays WHERE status IN ('settled','closed') AND COALESCE(artifact,0)=0
         AND route_source=? AND cohort=?`, FundedPositiveSystemComboRoute, ComboLabCohortRollingPositive).Scan(&v)
	return v.Float64, err
}

// HasOpenLiveCombo reports whether an UNSETTLED accepted combo already exists with this exact
// (canonicalized) legs JSON — the persisted dedup that stops the same combo being re-bought on a
// double-click, a retry, or a restart (audit F-R5).
func (s *Store) HasOpenLiveCombo(ctx context.Context, legsJSON string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(1) FROM live_combos WHERE settled=0 AND legs=?`, legsJSON).Scan(&n)
	return n > 0, err
}

// SignalFamine (R76, auditor nag — famine sentinel, 8th run): the wedge detector /api/ready never
// had. Returns the newest signal_log ts overall (index-backed MAX on idx_signal_ts — O(log n))
// plus each family's newest ts within the trailing 24h (one ranged GROUP BY over the same index).
// A healthy suite writes signals continuously; families going silent past their expected cadence
// is exactly what 7h of wedge looked like from outside — with zero red anywhere.
// SignalFamine — R98 REWRITE (the "signal famine query failed: context deadline exceeded" fix,
// operator-rejected-bandaid class). The old shape (`SELECT signal_type, MAX(ts) ... WHERE ts >= cut
// GROUP BY signal_type`) walked every row in the 24h window through idx_signal_ts + a temp b-tree
// (~150k row lookups; 709ms cold on a QUIET host copy, >5s in production under boot-storm load) —
// and the sentinel only ever consulted the handful of cadence-table families. Now: one MAX(ts)
// index-tail seek + one covering (signal_type, ts) point seek PER REQUESTED FAMILY — O(fams·logN),
// sub-ms cold or warm, immune to window size and to absent families (the old shape's worst case).
// Semantics preserved: a family with no rows inside the 24h window is simply absent from byFam.
func (s *Store) SignalFamine(ctx context.Context, fams []string) (newest string, byFam map[string]string, err error) {
	if err = s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(ts),'') FROM signal_log`).Scan(&newest); err != nil {
		return "", nil, err
	}
	cut := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	byFam = map[string]string{}
	for _, f := range fams {
		var ts string
		if qerr := s.db.QueryRowContext(ctx,
			`SELECT COALESCE(MAX(ts),'') FROM signal_log WHERE signal_type=? AND ts>=?`, f, cut).Scan(&ts); qerr != nil {
			return newest, byFam, qerr
		}
		if ts != "" {
			byFam[f] = ts
		}
	}
	return newest, byFam, nil
}

// ---- retention pruning (R70, audit §e: kalshi.db at 2.12 GB with only 3/13 tables pruned) ----

// retentionBatch bounds every retention DELETE/UPDATE so one sweep can never hold the SQLite write
// lock for minutes on the first (backlogged) run — the monitor loops batches instead.
const retentionBatch = 20000

// EnsureRetentionIndexes makes CERTAIN the indexes the retention sweep depends on exist before any
// prune runs (R76 wedge fix, bug 1). Open() already builds them (schema.sql + migrations), so this
// is normally an instant no-op — but MonitorRetention calls it (and refuses to prune until it
// succeeds) as the last line of defense: the orphan probe without idx_ptt_cond is a quadratic scan
// of poly_trader_trades under the write lock, which is exactly the 7h wedge. Idempotent; returns
// the first error so the caller can retry.
func (s *Store) EnsureRetentionIndexes(ctx context.Context) error {
	for _, ix := range []string{
		"CREATE INDEX IF NOT EXISTS idx_ptt_cond ON poly_trader_trades(condition_id)",
		"CREATE INDEX IF NOT EXISTS idx_ptt_resolved_cond ON poly_trader_trades(resolved, condition_id)",
		"CREATE INDEX IF NOT EXISTS idx_ptt_wallet_res ON poly_trader_trades(wallet, resolved)",
		// R124: the archive mover's candidate probe (resolved=1 AND ts<cutoff) — without it every
		// batch re-scans the resolved set; with it each batch is O(batch). Also in schema.sql.
		"CREATE INDEX IF NOT EXISTS idx_ptt_res_ts ON poly_trader_trades(resolved, ts)",
		`CREATE INDEX IF NOT EXISTS idx_rsystem_obs_flow_label_cleanup
ON research_system_observations(id)
WHERE system_id='flow-direction-integrity' AND route='observer'
 AND observation_kind='control' AND blocker='label_audit_only_no_trade_authority'
 AND cohort IN ('kalshi-authoritative-aggressor-v1','kalshi-inferred-aggressor-v1')`,
	} {
		if _, err := s.db.ExecContext(ctx, ix); err != nil {
			return err
		}
	}
	return nil
}

// PruneRetention applies the conservative retention policy in BOUNDED batches and reports rows
// touched per rule. Policy (R70):
//   - signal_log        KEEP EVERY ROW (training data) — only the price_path/post_path TEXT blobs
//     on RESOLVED rows older than 180d are blanked (the audit's "trim the paths":
//     features + labels stay; the tuners only ever scan the newest ~8k rows).
//   - audit_log         90 days.
//   - arb_log           90 days (forward-collected opportunity log; analytics use recent windows).
//   - maker_fill_stats  90 days (rolling fill-rate calibration; MakerFillSummary then reflects the
//     recent execution regime, which is what Replay should price anyway).
//   - poly_trader_trades: ONLY resolved=-1 rows (venue-purged/unresolvable quarantine — explicitly
//     NOT training data) older than 30d. resolved=1 rows are the wallet-skill
//     training set and are NEVER deleted — R124 policy amendment: aged resolved
//     rows may only be MOVED to the archive (archive.go, ArchiveResolvedPTT;
//     moved-count == archive-count pinned in retention_test.go); resolved=0
//     stay for the sweep.
//   - poly_condition_status: rows whose condition no longer has any unresolved trade (orphans).
//   - redundant Step-7 flow-label route mirrors: bounded migration only. The immutable historical
//     observation plus compact raw-pair/daily-aggregate evidence remains; only its event-free
//     mechanical route copy is removed. Every other observer, executable, candidate, negative,
//     cross-venue identity/rule, and lifecycle-bearing route row is never touched.
//
// Deleted pages go to SQLite's freelist (the file stops growing and space is reused); the periodic
// VACUUM INTO backup produces the compacted copy. Callers loop while total>0 to drain a backlog.
func (s *Store) PruneRetention(ctx context.Context) (map[string]int64, error) {
	now := time.Now().UTC()
	cut90 := now.Add(-90 * 24 * time.Hour).Format(time.RFC3339)
	cut180 := now.Add(-180 * 24 * time.Hour).Format(time.RFC3339)
	cut30unix := now.Add(-30 * 24 * time.Hour).Unix()
	out := map[string]int64{}
	var firstErr error
	run := func(name, q string, args ...any) {
		if ctx.Err() != nil {
			return
		}
		// R76 (DO-THIS 5): per-STATEMENT timeout — one degenerate rule (e.g. an un-indexed probe on a
		// backlogged table) errors out after 30s instead of holding the write lock for hours. The
		// monitor logs the error and the next pass retries; every other rule in the batch still runs.
		sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		res, err := s.db.ExecContext(sctx, q, args...)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%s: %w", name, err)
			}
			return
		}
		n, _ := res.RowsAffected()
		out[name] += n
	}
	run("audit_log_90d",
		`DELETE FROM audit_log WHERE id IN (SELECT id FROM audit_log WHERE ts < ? LIMIT ?)`, cut90, retentionBatch)
	run("arb_log_90d",
		`DELETE FROM arb_log WHERE id IN (SELECT id FROM arb_log WHERE ts < ? LIMIT ?)`, cut90, retentionBatch)
	run("maker_fill_stats_90d",
		`DELETE FROM maker_fill_stats WHERE id IN (SELECT id FROM maker_fill_stats WHERE ts < ? LIMIT ?)`, cut90, retentionBatch)
	run("trader_trades_unresolvable_30d",
		`DELETE FROM poly_trader_trades WHERE id IN (
		   SELECT id FROM poly_trader_trades WHERE resolved = -1 AND ts < ? LIMIT ?)`, cut30unix, retentionBatch)
	run("condition_status_orphans",
		`DELETE FROM poly_condition_status WHERE condition_id IN (
		   SELECT cs.condition_id FROM poly_condition_status cs
		   WHERE NOT EXISTS (SELECT 1 FROM poly_trader_trades tt WHERE tt.condition_id = cs.condition_id AND tt.resolved = 0)
		   LIMIT ?)`, retentionBatch)
	// The big one: path TEXT blobs are the bulk of signal_log's bytes. Rows/labels/features KEPT.
	run("signal_path_blobs_180d",
		`UPDATE signal_log SET price_path = '', post_path = ''
		 WHERE id IN (SELECT id FROM signal_log
		              WHERE resolved != 0 AND ts < ? AND (price_path != '' OR COALESCE(post_path,'') != '')
		              LIMIT ?)`, cut180, retentionBatch/4)
	if ctx.Err() == nil {
		// Keep this migration much smaller than the ordinary 20k retention batch. The live monitor
		// breathes between calls, so even a multi-million-row historical backlog cannot recreate the
		// large WAL/write-lock burst that this compaction is intended to prevent.
		_, removed, _, err := s.CompactRedundantObserverRouteMirrors(ctx, 500)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("redundant_observer_route_mirrors: %w", err)
			}
		} else if removed > 0 {
			out["redundant_observer_route_mirrors"] += removed
		}
	}
	return out, firstErr
}

// ResetPaper wipes all paper fills and parlays for a fresh P&L start (portfolio empties,
// realized/stats zero out). The signal_log (signal backtest history) is intentionally KEPT.
func (s *Store) ResetPaper(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM paper_fills`); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM paper_parlays`)
	return err
}

// ClearOpenPaperFills removes only the fills belonging to currently-OPEN positions (net contracts
// still > 0), flattening the live portfolio WHILE KEEPING every already-closed round-trip — so the
// closed-bet history (and its realized P&L) survives a portfolio reset. The closed-bet log is wiped
// ONLY by DeleteClosedPaperFills (the History window's Clear button). PAPER ONLY.
func (s *Store) ClearOpenPaperFills(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
DELETE FROM paper_fills WHERE (platform||'|'||ticker||'|'||side) IN (
  SELECT platform||'|'||ticker||'|'||side FROM paper_fills
  GROUP BY platform, ticker, side
  HAVING SUM(CASE WHEN action='BUY' THEN contracts ELSE -contracts END) > 0.0001
)`)
	return err
}

// DeleteClosedPaperFills removes fills for markets that are currently flat (fully closed
// rounds) while KEEPING fills for the still-open positions whose "platform|ticker|side" key
// is in keepKeys. Empty keepKeys clears everything. Powers the History "Clear" button so the
// closed-bet log can be wiped without touching live positions. PAPER ONLY.
func (s *Store) DeleteClosedPaperFills(ctx context.Context, keepKeys []string) error {
	if len(keepKeys) == 0 {
		_, err := s.db.ExecContext(ctx, `DELETE FROM paper_fills`)
		return err
	}
	q := `DELETE FROM paper_fills WHERE (platform||'|'||ticker||'|'||side) NOT IN (`
	args := make([]any, len(keepKeys))
	for i, k := range keepKeys {
		if i > 0 {
			q += ","
		}
		q += "?"
		args[i] = k
	}
	q += ")"
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}
