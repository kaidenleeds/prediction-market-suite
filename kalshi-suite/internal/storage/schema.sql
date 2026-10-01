-- Phase 0 schema. Trading, positions, fills, signals and stats tables arrive in
-- later phases; Phase 0 establishes credentials, a key/value config store, and a
-- tamper-evident audit log.

CREATE TABLE IF NOT EXISTS credentials (
    environment TEXT PRIMARY KEY,      -- 'demo' | 'prod'
    key_id      TEXT NOT NULL,
    salt        BLOB NOT NULL,         -- Argon2id salt
    nonce       BLOB NOT NULL,         -- AES-GCM nonce
    ciphertext  BLOB NOT NULL,         -- AES-256-GCM ciphertext of the RSA private key (PEM)
    created_at  TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kv_config (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_log (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       TEXT NOT NULL,
    level    TEXT NOT NULL,            -- info | warn | error | critical
    category TEXT NOT NULL,            -- system | auth | killswitch | order | risk | data
    message  TEXT NOT NULL,
    detail   TEXT                      -- optional free-form / JSON
);

CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);

-- Phase 4: paper (simulated) fills. No real orders are ever placed in paper mode.
CREATE TABLE IF NOT EXISTS paper_fills (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT NOT NULL,
    platform  TEXT NOT NULL,        -- kalshi | polymarket
    ticker    TEXT NOT NULL,        -- market ticker / condition id
    title     TEXT NOT NULL,        -- human label
    side      TEXT NOT NULL,        -- YES | NO | outcome label
    action    TEXT NOT NULL,        -- BUY | SELL
    price     REAL NOT NULL,        -- 0..1
    contracts REAL NOT NULL,
    fee       REAL NOT NULL,        -- dollars
    tp_price  REAL,                 -- optional take-profit price (0..1, the side's price)
    sl_price  REAL,                 -- optional stop-loss price (0..1)
    source    TEXT,                 -- manual | gate | arb | whale | auto-tp | auto-sl
    note      TEXT
);

CREATE INDEX IF NOT EXISTS idx_paper_fills_ts ON paper_fills(ts);

-- Phase 4: paper parlays (multi-leg, all-or-nothing). legs is a JSON array.
CREATE TABLE IF NOT EXISTS paper_parlays (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    ts        TEXT NOT NULL,
    stake     REAL NOT NULL,
    price     REAL NOT NULL,   -- combined entry (product of leg prices), 0..1
    contracts REAL NOT NULL,   -- stake/price
    tp        REAL,            -- optional take-profit on the combined mark (0..1)
    sl        REAL,            -- optional stop-loss
    status    TEXT NOT NULL,   -- open | closed
    legs      TEXT NOT NULL,   -- JSON array of legs
    route_source TEXT NOT NULL DEFAULT '', -- durable placement route; blank = legacy/generic
    cohort      TEXT NOT NULL DEFAULT '',  -- research cohort at placement
    system_ids  TEXT NOT NULL DEFAULT '[]', -- frozen JSON system-id list
    joint_p     REAL NOT NULL DEFAULT 0,   -- frozen prospective joint probability
    expected_net_per_dollar REAL NOT NULL DEFAULT 0, -- fee-net EV at final repriced entry
    canonical_system_id TEXT NOT NULL DEFAULT '',
    combo_venue TEXT NOT NULL DEFAULT '',
    leg_count INTEGER NOT NULL DEFAULT 0,
    relation_class TEXT NOT NULL DEFAULT '',
    producer_family TEXT NOT NULL DEFAULT '',
    combo_route TEXT NOT NULL DEFAULT '',
    experiment_epoch TEXT NOT NULL DEFAULT '',
    combo_key TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_paper_parlays_ts ON paper_parlays(ts);

-- Signal backtest: each distinct signal (per market+side+type+slot) is logged when it
-- appears, then marked won/lost once the market settles — so we can measure whether the
-- whale/flow signals actually predict winners (independent of whether a trade was taken).
-- The feature columns (strength..momentum) let us DISCOVER which conditions predict winners,
-- so thresholds can be fit from data instead of guessed. 0 = feature not applicable to that
-- signal type (e.g. Kalshi is anonymous → no rank/concentration/trader_pnl).
-- NOTE: the (slot,...) dedup index is created in code (storage.Open) so existing DBs migrate
-- cleanly; the index line below stays on (day,...) for first-run safety and is then replaced.
CREATE TABLE IF NOT EXISTS signal_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    ts            TEXT NOT NULL,
    day           TEXT NOT NULL,        -- YYYY-MM-DD, for reporting
    slot          TEXT NOT NULL DEFAULT '', -- ~10-min time bucket: dedups within a slot but captures evolution across slots
    platform      TEXT NOT NULL,
    ticker        TEXT NOT NULL,
    title         TEXT NOT NULL,
    side          TEXT NOT NULL,        -- YES | NO | outcome label
    signal_type   TEXT NOT NULL,        -- kalshi-flow | kalshi-whale | poly-whale | poly-consensus | polyus-flow | polyus-whale | poly-pred-kalshi
    input_topology TEXT NOT NULL DEFAULT '', -- exact source-input contract (K, PUS, K-PUS, K-SPOT, ...)
    entry_price   REAL NOT NULL,        -- legacy visible/signal price at signal time (0..1); book-v1 keeps it only as an input
    -- R133 BOOK-NATIVE ML COHORT. These stay NULL/version 0 on every historical row: no
    -- midpoint/spread reconstruction is allowed to masquerade as an executable book snapshot.
    book_feature_ver INTEGER NOT NULL DEFAULT 0,
    book_bid       REAL,                 -- side-specific executable maker touch (0..1)
    book_ask       REAL,                 -- side-specific executable taker touch (0..1)
    book_bid_depth REAL,                 -- contracts resting at book_bid
    book_ask_depth REAL,                 -- contracts resting at book_ask
    book_quote_age_s REAL,               -- source quote/book age when captured
    book_maker_tick REAL,                -- actual directional tick at the maker touch
    book_taker_tick REAL,                -- actual directional tick at the taker touch
    book_maker_fee_pc REAL,              -- exact fee/rebate for one maker contract at book_bid
    book_taker_fee_pc REAL,               -- exact fee for one taker contract at book_ask
    book_latency_ms REAL,                 -- measured decision/transport latency; NULL when unavailable
    book_source    TEXT NOT NULL DEFAULT '',
    label_version  TEXT NOT NULL DEFAULT '', -- prospective outcome/liveness-label contract; blank = legacy/untrusted
    pricing_version TEXT NOT NULL DEFAULT '', -- executable-book pricing contract; blank = legacy/untrusted
    fee_pc        REAL,                 -- exact 1-contract entry fee under the venue schedule at signal time; NULL legacy
    strength      REAL NOT NULL DEFAULT 0, -- flow one-sidedness 0..1
    notional      REAL NOT NULL DEFAULT 0, -- $ size of the flow / position
    best_rank     INTEGER NOT NULL DEFAULT 0, -- best leaderboard rank in the flow (poly; 0 = none/anon)
    trader_count  INTEGER NOT NULL DEFAULT 0, -- distinct whales on that side (poly-consensus)
    trader_pnl    REAL NOT NULL DEFAULT 0, -- combined all-time PnL of those whales (poly)
    concentration REAL NOT NULL DEFAULT 0, -- group $ ÷ portfolio = conviction (poly-consensus)
    momentum      REAL NOT NULL DEFAULT 0, -- recent price move over the window (kalshi)
    spread_cents  REAL NOT NULL DEFAULT 0, -- bid/ask spread in cents at signal time (book quality)
    resolve_hours REAL NOT NULL DEFAULT 0, -- hours until the market resolves at signal time
    imbalance     REAL NOT NULL DEFAULT 0, -- resting order-book imbalance (-1..1; which side the book leans)
    underlying    REAL NOT NULL DEFAULT 0, -- underlying spot price at signal time (crypto signals: BTC/ETH/… USD)
    price_path    TEXT NOT NULL DEFAULT '', -- JSON array of the market's recent price path (prices-history) at signal time
    category      TEXT NOT NULL DEFAULT '', -- market category from its first tag (Sports/Crypto/Politics/…), Poly signals
    resolved      INTEGER NOT NULL DEFAULT 0,
    won           INTEGER,              -- 1/0 once resolved
    resolved_at   TEXT
);
-- idx_signal_dedup (UNIQUE, per-~10-min slot) is owned by storage.Open's guarded migration — R90
-- bug 99 (auditor DO-THIS 2): the legacy per-DAY shape that used to live here could BRICK boot.
-- If the boot-time DROP+CREATE in Open ever failed (both errors were discarded), the next boot's
-- schema pass re-created the index under the per-DAY unique constraint, which slot-era data
-- genuinely violates → schema failure is fatal in Open → the suite could never boot again.
-- Do not re-add the index here; Open builds/repairs it with the exact
-- (slot,platform,ticker,side,signal_type,input_topology) shape.

-- Authoritative terminal truth is durable independently of signal collection. A funded Paper/ML
-- lot can legitimately exist for a destination ticker that never produced a signal_log row (for
-- example a cross-venue system whose source signal used the other venue's identity). Keeping the
-- venue+ticker result here prevents that lot from becoming permanently un-settleable after a
-- restart, while the primary key makes repeated push/poll reconciliation idempotent.
CREATE TABLE IF NOT EXISTS venue_settlements (
    platform        TEXT NOT NULL CHECK(platform IN ('kalshi','polyus','polymarket')),
    ticker          TEXT NOT NULL,
    yes_value       REAL NOT NULL CHECK(yes_value>=0 AND yes_value<=1),
    resolved_at     TEXT NOT NULL,
    source_artifact TEXT NOT NULL,
    PRIMARY KEY(platform,ticker)
);
CREATE INDEX IF NOT EXISTS idx_venue_settlements_resolved
    ON venue_settlements(resolved_at,platform,ticker);

-- Intraday NET P&L samples (the smooth live curve). Persisted so the curve survives a restart
-- instead of resetting to the per-fill steps. Pruned to the most recent rows on insert.
CREATE TABLE IF NOT EXISTS pnl_series (
    id  INTEGER PRIMARY KEY AUTOINCREMENT,
    ts  TEXT NOT NULL,
    net REAL NOT NULL
);

-- Polymarket smart-money database (Phase 1b): for each ranked leaderboard wallet we persist
-- their on-chain TRADE history + leaderboard rank/profit, then resolve each trade's outcome —
-- so we learn each whale's REAL hit rate over time (not just their leaderboard rank), the
-- per-wallet track record we'll mine for edge. Polymarket is the only venue with per-wallet
-- data (Kalshi + Poly US are anonymous), so this is unique to it.
CREATE TABLE IF NOT EXISTS poly_traders (
    wallet     TEXT PRIMARY KEY,        -- proxy wallet, lowercased
    name       TEXT,
    best_rank  INTEGER NOT NULL DEFAULT 0,
    profit     REAL NOT NULL DEFAULT 0, -- all-time leaderboard profit (USD)
    first_seen TEXT,
    last_seen  TEXT,
    cursor_ts  INTEGER NOT NULL DEFAULT 0 -- newest activity ts pulled so far (incremental cursor)
);
CREATE TABLE IF NOT EXISTS poly_trader_trades (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet        TEXT NOT NULL,
    ts            INTEGER NOT NULL,        -- trade time, unix seconds
    condition_id  TEXT NOT NULL,
    asset         TEXT NOT NULL,           -- outcome token id
    title         TEXT,
    outcome       TEXT,                    -- outcome label
    outcome_index INTEGER NOT NULL DEFAULT 0,
    side          TEXT,                    -- BUY | SELL
    size          REAL NOT NULL DEFAULT 0, -- shares
    price         REAL NOT NULL DEFAULT 0,
    usdc_size     REAL NOT NULL DEFAULT 0, -- $ notional
    tx_hash       TEXT,
    resolved      INTEGER NOT NULL DEFAULT 0, -- 0 open, 1 graded; negative = quarantine (-3 malformed condition id)
    won           INTEGER,                 -- 1/0 once the market resolves (did this outcome win)
    resolved_at   TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_ptt_dedup ON poly_trader_trades(tx_hash,asset,side);
CREATE INDEX IF NOT EXISTS idx_ptt_wallet ON poly_trader_trades(wallet);
CREATE INDEX IF NOT EXISTS idx_ptt_open ON poly_trader_trades(resolved);
-- R76 WEDGE FIX (auditor bug 1): the composite indexes storage.go used to create were named
-- idx_ptt_open/idx_ptt_wallet — the SAME names as the two single-column indexes above, so on any
-- DB that ran this schema first, CREATE INDEX IF NOT EXISTS was a silent no-op and the composites
-- NEVER existed. The retention orphan probe then correlated poly_condition_status against
-- poly_trader_trades.condition_id with no usable index — a full scan of ~2.5M rows PER status row,
-- quadratic, held the write lock for hours (wedge #2). NEW names below (never reused), so the next
-- boot creates them for real. idx_ptt_cond is THE index the orphan probe needs (correlates on bare
-- condition_id); the two composites are what storage.go always intended.
CREATE INDEX IF NOT EXISTS idx_ptt_cond ON poly_trader_trades(condition_id);
CREATE INDEX IF NOT EXISTS idx_ptt_resolved_cond ON poly_trader_trades(resolved, condition_id);
CREATE INDEX IF NOT EXISTS idx_ptt_wallet_res ON poly_trader_trades(wallet, resolved);
-- R124: the archive mover's candidate probe (resolved=1 AND ts < cutoff) — see archive.go.
CREATE INDEX IF NOT EXISTS idx_ptt_res_ts ON poly_trader_trades(resolved, ts);

-- ARBXV: cross-venue arbitrage opportunities found by the live scanner. Arb needs real SIMULTANEOUS
-- quotes, so we FORWARD-COLLECT (the only honest "backtest"): each row is a moment a matched market was
-- mispriced across venues by more than fees. Not a bet — a logged opportunity for frequency/edge analysis.
CREATE TABLE IF NOT EXISTS arb_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         TEXT NOT NULL,
    slot       TEXT NOT NULL DEFAULT '',  -- ~10-min bucket; one row per opportunity per slot (dedup)
    kind       TEXT NOT NULL,             -- 'xv' cross-venue Kalshi<->PolyUS | 'yn' single-venue YES+NO<1
    market     TEXT NOT NULL,             -- human label
    category   TEXT NOT NULL DEFAULT '',
    buy_venue  TEXT NOT NULL,             -- where you buy the cheap side
    sell_venue TEXT NOT NULL,             -- where you buy the opposite side (same venue for 'yn')
    buy_price  REAL NOT NULL DEFAULT 0,
    opp_price  REAL NOT NULL DEFAULT 0,
    gross_edge REAL NOT NULL DEFAULT 0,   -- 1 - total cost to lock $1 (pre-fee)
    fee_est    REAL NOT NULL DEFAULT 0,   -- estimated round-trip fee per contract
    net_edge   REAL NOT NULL DEFAULT 0    -- gross_edge - fee_est (the locked profit after fees)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_arb_dedup ON arb_log(slot,kind,market);
CREATE INDEX IF NOT EXISTS idx_arb_ts ON arb_log(ts);

-- HONEST-FILL HARNESS (audit Q5/Q6/Q7): every maker fill ATTEMPT — posted price, book context, whether
-- the touch ever traded through (filled) or it expired, and the post-fill drift (adverse selection).
-- Replaces the invented 0.85 fill-rate / 0.15 adverse constants with measured numbers, and is the
-- pre-registered validation surface for pcrypto-on-Kalshi, favlong, xmatch and every future signal.
CREATE TABLE IF NOT EXISTS maker_fill_stats (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    platform     TEXT NOT NULL,
    ticker       TEXT NOT NULL,
    side         TEXT NOT NULL,
    source       TEXT NOT NULL DEFAULT '',
    strategy_family   TEXT NOT NULL DEFAULT '', -- canonical executable family; blank = legacy/unknown, never LIVE proof
    strategy_inverted INTEGER NOT NULL DEFAULT 0, -- actual routed side was the inverse of the source signal
    post_px      REAL NOT NULL,             -- our resting side-price
    spread_cents REAL NOT NULL DEFAULT 0,
    book_depth   REAL NOT NULL DEFAULT 0,
    filled       INTEGER NOT NULL DEFAULT -1, -- -1 pending, 1 filled (touch traded through), 0 expired
    fill_ts      TEXT NOT NULL DEFAULT '',
    expire_ts    TEXT NOT NULL DEFAULT '',
    adverse_5m   REAL NOT NULL DEFAULT 0,   -- side-price drift 5 min after the fill (negative = adverse for a buy)
    maker_fee_pc REAL,                      -- exact fee paid per filled contract; NULL = no fill-time receipt
    maker_rebate_pc REAL,                   -- exact rebate earned per filled contract; NULL = no fill-time receipt
    maker_fee_source TEXT NOT NULL DEFAULT '' -- fee registry/schema authority captured at fill time
);
CREATE INDEX IF NOT EXISTS idx_mfs_plat ON maker_fill_stats(platform, filled);

-- R135c: prospective, research-only sub-cent golf execution cohort.  This is intentionally
-- separate from paper_fills/positions: a row is a one-contract counterfactual observation, never
-- an order authorization.  Hourly quote slots prevent correlated 10-minute snapshots from
-- flooding the database while preserving route, queue, fee and settlement evidence.
CREATE TABLE IF NOT EXISTS subcent_golf_trials (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts      TEXT NOT NULL,
    slot             TEXT NOT NULL,
    platform         TEXT NOT NULL DEFAULT 'kalshi',
    ticker           TEXT NOT NULL,
    event_ticker     TEXT NOT NULL DEFAULT '',
    title            TEXT NOT NULL DEFAULT '',
    player           TEXT NOT NULL DEFAULT '',
    side             TEXT NOT NULL CHECK(side IN ('YES','NO')),
    phase            TEXT NOT NULL DEFAULT 'unknown'
                         CHECK(phase IN ('pre_event','live','unknown')),
    phase_evidence   TEXT NOT NULL DEFAULT '',
    player_status    TEXT NOT NULL DEFAULT 'unknown'
                         CHECK(player_status IN ('active','cut','withdrawn','eliminated','unknown')),
    status_evidence  TEXT NOT NULL DEFAULT '',
    terminal_status  TEXT NOT NULL DEFAULT 'unknown'
                         CHECK(terminal_status IN ('active','cut','withdrawn','eliminated','unknown')),
    terminal_status_evidence TEXT NOT NULL DEFAULT '',
    market_type      TEXT NOT NULL DEFAULT 'prop'
                         CHECK(market_type IN ('winner','round_leader','matchup','prop')),
    route            TEXT NOT NULL CHECK(route IN ('maker','taker')),
    book_source      TEXT NOT NULL DEFAULT '',
    quote_age_s      REAL NOT NULL DEFAULT 0,
    tick_size        REAL NOT NULL,
    bid_px           REAL NOT NULL DEFAULT 0,
    ask_px           REAL NOT NULL,
    bid_depth        REAL NOT NULL DEFAULT 0,
    ask_depth        REAL NOT NULL DEFAULT 0,
    queue_ahead      REAL,
    maker_attempt_id INTEGER,
    fill_state       TEXT NOT NULL DEFAULT 'resting'
                         CHECK(fill_state IN ('resting','filled','canceled','not_quoteable','blocked')),
    fill_rule        TEXT NOT NULL DEFAULT '',
    cancel_reason    TEXT NOT NULL DEFAULT '',
    fill_ts          TEXT NOT NULL DEFAULT '',
    cancel_ts        TEXT NOT NULL DEFAULT '',
    fill_price       REAL NOT NULL DEFAULT 0,
    fee_pc           REAL NOT NULL DEFAULT 0,
    rebate_pc        REAL NOT NULL DEFAULT 0,
    fee_source       TEXT NOT NULL DEFAULT '',
    settled          INTEGER NOT NULL DEFAULT 0,
    settle_val       REAL,
    payout_pc        REAL,
    net_pc           REAL,
    settled_ts       TEXT NOT NULL DEFAULT '',
    UNIQUE(slot,ticker,side,route)
);
CREATE INDEX IF NOT EXISTS idx_sgolf_open ON subcent_golf_trials(settled,ticker);
CREATE INDEX IF NOT EXISTS idx_sgolf_cohort ON subcent_golf_trials(phase,player_status,terminal_status,market_type,route);
CREATE INDEX IF NOT EXISTS idx_sgolf_maker ON subcent_golf_trials(maker_attempt_id);

-- R135c: strictly research-only prospective systems. These tables are deliberately disjoint
-- from paper_fills, unit_trials and live order state: observations can never create a position.

-- `queue-priority`: official zero-indexed queue positions for naturally resting REAL Kalshi
-- orders, compared with the visible book-level upper-bound estimate. No order is ever placed to
-- manufacture a sample; disappearance is classified later through the read-only order endpoint.
CREATE TABLE IF NOT EXISTS research_queue_orders (
    order_id        TEXT PRIMARY KEY,
    system_name     TEXT NOT NULL DEFAULT 'queue-priority',
    ticker          TEXT NOT NULL,
    outcome_side    TEXT NOT NULL DEFAULT '',
    book_side       TEXT NOT NULL DEFAULT '',
    action          TEXT NOT NULL DEFAULT '',
    price           REAL NOT NULL DEFAULT 0,
    initial_count   REAL NOT NULL DEFAULT 0,
    remaining_count REAL NOT NULL DEFAULT 0,
    filled_count    REAL NOT NULL DEFAULT 0,
    venue_created_ts TEXT NOT NULL DEFAULT '',
    first_seen_ts   TEXT NOT NULL,
    last_seen_ts    TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'resting',
    outcome         TEXT NOT NULL DEFAULT '', -- filled | canceled | partial_cancel; blank while resting
    outcome_ts      TEXT NOT NULL DEFAULT '',
    samples         INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_rqo_open ON research_queue_orders(outcome,last_seen_ts);
CREATE TABLE IF NOT EXISTS research_queue_samples (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id          TEXT NOT NULL,
    observed_ts       TEXT NOT NULL,
    slot              TEXT NOT NULL,
    true_queue_fp     REAL NOT NULL,
    visible_level_fp  REAL,
    visible_ahead_fp  REAL,
    estimate_error_fp REAL,
    estimate_known    INTEGER NOT NULL DEFAULT 0,
    book_age_s        REAL,
    remaining_count   REAL NOT NULL DEFAULT 0,
    UNIQUE(order_id,slot),
    FOREIGN KEY(order_id) REFERENCES research_queue_orders(order_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_rqs_order ON research_queue_samples(order_id,observed_ts);

-- `incentive-maker`: active official program terms joined to an executable side-specific book
-- and exact one-share maker fee. period_reward is retained in venue units and explicitly never
-- added to EV: participant competition and allocation are unknown.
CREATE TABLE IF NOT EXISTS research_incentive_maker (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name           TEXT NOT NULL DEFAULT 'incentive-maker',
    observed_ts           TEXT NOT NULL,
    slot                  TEXT NOT NULL,
    program_id            TEXT NOT NULL,
    market_ticker         TEXT NOT NULL,
    series_ticker         TEXT NOT NULL DEFAULT '',
    side                  TEXT NOT NULL CHECK(side IN ('YES','NO')),
    incentive_type        TEXT NOT NULL DEFAULT '',
    description           TEXT NOT NULL DEFAULT '',
    start_date            TEXT NOT NULL DEFAULT '',
    end_date              TEXT NOT NULL DEFAULT '',
    period_reward_raw     REAL NOT NULL DEFAULT 0,
    discount_factor_bps   REAL NOT NULL DEFAULT 0,
    target_size_fp        REAL NOT NULL DEFAULT 0,
    bid_px                REAL NOT NULL,
    ask_px                REAL NOT NULL,
    bid_depth             REAL NOT NULL,
    ask_depth             REAL NOT NULL,
    maker_fee_pc          REAL NOT NULL,
    capacity_contracts    REAL NOT NULL DEFAULT 0,
    competition_known     INTEGER NOT NULL DEFAULT 0,
    reward_included_in_ev INTEGER NOT NULL DEFAULT 0,
    book_source           TEXT NOT NULL,
    quote_age_s           REAL NOT NULL DEFAULT 0,
    UNIQUE(slot,program_id,market_ticker,side)
);
CREATE INDEX IF NOT EXISTS idx_rim_program ON research_incentive_maker(program_id,observed_ts);

-- `lifecycle-reopen`: activated-after-pause/reopen episodes and fixed-horizon executable-book
-- samples. prior_event_type proves why an activation belongs to the cohort; queue_reset_context
-- records the venue rule that reactivation cancels all previously resting orders.
CREATE TABLE IF NOT EXISTS research_lifecycle_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name         TEXT NOT NULL DEFAULT 'lifecycle-reopen',
    observed_ts         TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    prior_event_type    TEXT NOT NULL DEFAULT '',
    close_time          TEXT NOT NULL DEFAULT '',
    raw_hash            TEXT NOT NULL DEFAULT '',
    cohort              INTEGER NOT NULL DEFAULT 0,
    pre_bid             REAL,
    pre_ask             REAL,
    pre_bid_depth       REAL,
    pre_ask_depth       REAL,
    pre_book_source     TEXT NOT NULL DEFAULT '',
    post0_bid           REAL,
    post0_ask           REAL,
    post0_bid_depth     REAL,
    post0_ask_depth     REAL,
    post0_book_source   TEXT NOT NULL DEFAULT '',
    queue_reset_context TEXT NOT NULL DEFAULT '',
    UNIQUE(ticker,event_type,observed_ts)
);
CREATE INDEX IF NOT EXISTS idx_rle_cohort ON research_lifecycle_events(cohort,observed_ts);
CREATE INDEX IF NOT EXISTS idx_rle_ticker_id ON research_lifecycle_events(ticker,id DESC);
CREATE TABLE IF NOT EXISTS research_lifecycle_horizons (
    event_id        INTEGER NOT NULL,
    horizon_s       INTEGER NOT NULL,
    captured_ts     TEXT NOT NULL,
    bid_px          REAL NOT NULL,
    ask_px          REAL NOT NULL,
    bid_depth       REAL NOT NULL,
    ask_depth       REAL NOT NULL,
    book_source     TEXT NOT NULL,
    quote_age_s     REAL NOT NULL DEFAULT 0,
    taker_fee_pc    REAL NOT NULL DEFAULT 0,
    PRIMARY KEY(event_id,horizon_s),
    FOREIGN KEY(event_id) REFERENCES research_lifecycle_events(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS research_lifecycle_horizon_misses (
    event_id        INTEGER NOT NULL,
    horizon_s       INTEGER NOT NULL,
    missed_ts       TEXT NOT NULL,
    reason          TEXT NOT NULL,
    PRIMARY KEY(event_id,horizon_s),
    FOREIGN KEY(event_id) REFERENCES research_lifecycle_events(id) ON DELETE CASCADE
);

-- `event-basket-lock`: one-share-per-leg, all-leg executable baskets. Complete identity is from
-- official Kalshi nested event children or PolyUS' explicit league/outcome cardinality. The row
-- preserves capacity and worst-case partial-fill exposure; it is never sent to an order path.
CREATE TABLE IF NOT EXISTS research_event_baskets (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name                TEXT NOT NULL DEFAULT 'event-basket-lock',
    observed_ts                TEXT NOT NULL,
    slot                       TEXT NOT NULL,
    venue                      TEXT NOT NULL,
    event_id                   TEXT NOT NULL,
    title                      TEXT NOT NULL DEFAULT '',
    route                      TEXT NOT NULL, -- buy-yes-all | buy-no-all
    identity_source            TEXT NOT NULL,
    identity_complete          INTEGER NOT NULL DEFAULT 0,
    mutually_exclusive         INTEGER NOT NULL DEFAULT 0,
    exhaustive                 INTEGER NOT NULL DEFAULT 0,
    leg_count                  INTEGER NOT NULL,
    legs_json                  TEXT NOT NULL,
    all_leg_cost               REAL NOT NULL,
    exact_fee                  REAL NOT NULL,
    payout_lower_bound         REAL NOT NULL,
    net_lock                   REAL NOT NULL,
    capacity_contracts         REAL NOT NULL,
    partial_fill_worst_loss    REAL NOT NULL,
    candidate                  INTEGER NOT NULL DEFAULT 0,
    UNIQUE(slot,venue,event_id,route)
);
CREATE INDEX IF NOT EXISTS idx_reb_candidate ON research_event_baskets(candidate,observed_ts);

-- `nested-ladder-lock`: same-event monotone `greater` thresholds. Conditional on BOTH fills,
-- YES(low)+NO(high), low<high, pays at least $1 and pays $2 inside the interval. Venue orders are
-- not atomic, so candidate is research notation only; partial/unwind risk is explicit.
CREATE TABLE IF NOT EXISTS research_nested_ladders (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name             TEXT NOT NULL DEFAULT 'nested-ladder-lock',
    observed_ts             TEXT NOT NULL,
    slot                    TEXT NOT NULL,
    venue                   TEXT NOT NULL DEFAULT 'kalshi',
    event_id                TEXT NOT NULL,
    title                   TEXT NOT NULL DEFAULT '',
    low_ticker              TEXT NOT NULL,
    high_ticker             TEXT NOT NULL,
    low_strike              REAL NOT NULL,
    high_strike             REAL NOT NULL,
    low_yes_bid             REAL NOT NULL,
    low_yes_ask             REAL NOT NULL,
    high_yes_bid            REAL NOT NULL,
    high_yes_ask            REAL NOT NULL,
    low_ask_depth           REAL NOT NULL,
    high_no_ask_depth       REAL NOT NULL,
    all_leg_cost            REAL NOT NULL,
    exact_entry_fee         REAL NOT NULL,
    payout_lower_bound      REAL NOT NULL DEFAULT 1,
    net_lock_if_both_fill   REAL NOT NULL,
    capacity_contracts      REAL NOT NULL,
    partial_fill_worst_loss REAL NOT NULL,
    unwind_buffer           REAL NOT NULL,
    quote_violation         INTEGER NOT NULL DEFAULT 0,
    violation_amount        REAL NOT NULL DEFAULT 0,
    candidate               INTEGER NOT NULL DEFAULT 0,
    atomic_fill             INTEGER NOT NULL DEFAULT 0,
    funded                  INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority         INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority          INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    identity_source         TEXT NOT NULL,
    UNIQUE(slot,event_id,low_ticker,high_ticker)
);
CREATE INDEX IF NOT EXISTS idx_rnl_candidate ON research_nested_ladders(candidate,observed_ts);
CREATE INDEX IF NOT EXISTS idx_rnl_violation ON research_nested_ladders(quote_violation,observed_ts);
CREATE TRIGGER IF NOT EXISTS research_nested_ladders_no_update BEFORE UPDATE ON research_nested_ladders BEGIN SELECT RAISE(ABORT,'immutable research nested ladder'); END;
CREATE TRIGGER IF NOT EXISTS research_nested_ladders_no_delete BEFORE DELETE ON research_nested_ladders BEGIN SELECT RAISE(ABORT,'immutable research nested ladder'); END;
CREATE TRIGGER IF NOT EXISTS research_nested_ladders_zero_authority BEFORE INSERT ON research_nested_ladders
WHEN NEW.funded!=0 OR NEW.paper_authority!=0 OR NEW.live_authority!=0
BEGIN SELECT RAISE(ABORT,'research nested ladder has zero authority'); END;

-- R137 `proper-score-*`: a calibrated probability forecast is converted into the position
-- vector implied by a strictly proper scoring rule, then tested against a CURRENT executable
-- two-sided book. This is deliberately disjoint from paper_fills/unit_trials/live state: it is a
-- research counterfactual, never proof or order authority. Both raw vector components and the
-- normalized one-sided quantity are retained so no hidden scale/normalization can rewrite history.
CREATE TABLE IF NOT EXISTS research_proper_score_trials (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts           TEXT NOT NULL,
    slot                  TEXT NOT NULL,
    system_name           TEXT NOT NULL,
    transform             TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
    strategy_mode         TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
    cohort                TEXT NOT NULL,
    platform              TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
    ticker                TEXT NOT NULL,
    title                 TEXT NOT NULL DEFAULT '',
    category              TEXT NOT NULL DEFAULT '',
    resolve_hours         REAL NOT NULL DEFAULT 0,
    forecast_yes          REAL NOT NULL,
    forecast_origin_side  TEXT NOT NULL CHECK(forecast_origin_side IN ('YES','NO')),
    forecast_signal       TEXT NOT NULL DEFAULT '',
    forecast_source       TEXT NOT NULL,
    forecast_version      TEXT NOT NULL,
    model_backend         TEXT NOT NULL DEFAULT '',
    calibration           TEXT NOT NULL DEFAULT '',
    generated_at          INTEGER NOT NULL DEFAULT 0,
    route                 TEXT NOT NULL CHECK(route IN ('taker','maker')),
    yes_bid               REAL NOT NULL,
    yes_ask               REAL NOT NULL,
    yes_bid_depth         REAL NOT NULL,
    yes_ask_depth         REAL NOT NULL,
    no_bid                REAL NOT NULL,
    no_ask                REAL NOT NULL,
    no_bid_depth          REAL NOT NULL,
    no_ask_depth          REAL NOT NULL,
    book_source           TEXT NOT NULL,
    source_clock_id       TEXT NOT NULL DEFAULT '',
    quote_age_s           REAL NOT NULL,
    decision_latency_ms   REAL NOT NULL DEFAULT 0,
    q_yes                 REAL NOT NULL,
    raw_yes               REAL NOT NULL DEFAULT 0,
    raw_no                REAL NOT NULL DEFAULT 0,
    raw_vector_json       TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(raw_vector_json)),
    vector_dim            INTEGER NOT NULL DEFAULT 2 CHECK(vector_dim>=2),
    constant_shift        REAL NOT NULL DEFAULT 0,
    rescale               REAL NOT NULL DEFAULT 1 CHECK(rescale>0),
    normalized_weight     REAL NOT NULL DEFAULT 0,
    canonical_qty         REAL NOT NULL DEFAULT 0,
    requested_qty         REAL NOT NULL DEFAULT 0,
    executable_qty        REAL NOT NULL DEFAULT 0,
    selected_side         TEXT NOT NULL DEFAULT '',
    tick_size             REAL NOT NULL DEFAULT 0,
    lot_size              REAL NOT NULL DEFAULT 0,
    depth_curve_json      TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(depth_curve_json)),
    entry_price           REAL NOT NULL DEFAULT 0,
    entry_depth           REAL NOT NULL DEFAULT 0,
    integrated_cost       REAL NOT NULL DEFAULT 0,
    spot_cost             REAL NOT NULL DEFAULT 0,
    liquidity_loss        REAL NOT NULL DEFAULT 0,
    exact_fee_total       REAL NOT NULL DEFAULT 0,
    fee_source            TEXT NOT NULL DEFAULT '',
    expected_net          REAL NOT NULL DEFAULT 0,
    abstain_reason        TEXT NOT NULL DEFAULT '',
    prior_actual_position REAL NOT NULL DEFAULT 0,
    target_position       REAL NOT NULL DEFAULT 0,
    actual_filled_delta   REAL NOT NULL DEFAULT 0,
    cancelled_delta       REAL NOT NULL DEFAULT 0,
    post_actual_position  REAL NOT NULL DEFAULT 0,
    fill_status           TEXT NOT NULL DEFAULT 'counterfactual'
                          CHECK(fill_status IN ('counterfactual','filled','partial','cancelled','legacy_unknown')),
    actual_fill           INTEGER NOT NULL DEFAULT 0 CHECK(actual_fill IN (0,1)),
    research_observation_id INTEGER NOT NULL DEFAULT 0,
    settled               INTEGER NOT NULL DEFAULT 0,
    grade_status          TEXT NOT NULL DEFAULT 'open', -- open | graded | unsupported
    grade_reason          TEXT NOT NULL DEFAULT '',
    settle_yes            REAL,
    forecast_score        REAL,
    book_score            REAL,
    score_delta           REAL,
    realized_net          REAL,
    closed_ts             TEXT NOT NULL DEFAULT '',
    UNIQUE(slot,system_name,strategy_mode,platform,ticker,forecast_origin_side,forecast_signal,
           forecast_source,forecast_version,route)
);
CREATE INDEX IF NOT EXISTS idx_rpst_open ON research_proper_score_trials(settled,platform,ticker);
CREATE INDEX IF NOT EXISTS idx_rpst_system ON research_proper_score_trials(system_name,transform,strategy_mode,settled,observed_ts);
-- R144 compact-briefing path: exact manifest membership is a ten-field identity. The existing
-- UNIQUE key has system_name between slot and strategy_mode, so it cannot serve this join. Without
-- this index the phone briefing repeatedly timed out while scanning the full research ledger.
CREATE INDEX IF NOT EXISTS idx_rpst_manifest_identity ON research_proper_score_trials(
    slot,transform,strategy_mode,platform,ticker,forecast_origin_side,forecast_signal,
    forecast_source,forecast_version,route,observed_ts);

-- R143 Proper Betting vector contract: freeze the complete coordinate universe before any trial
-- row is attempted.  A slot is economically complete only when its stored rows exactly match this
-- immutable count/hash-backed manifest and every coordinate has terminal supported truth.
CREATE TABLE IF NOT EXISTS research_proper_score_manifests (
    slot TEXT NOT NULL,
    transform TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
    strategy_mode TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
    created_ts TEXT NOT NULL,
    forecast_source TEXT NOT NULL,
    forecast_version TEXT NOT NULL,
    expected_coordinate_count INTEGER NOT NULL CHECK(expected_coordinate_count>0),
    expected_coordinate_hash TEXT NOT NULL CHECK(length(expected_coordinate_hash)=64),
    contract_version INTEGER NOT NULL DEFAULT 1 CHECK(contract_version=1),
    PRIMARY KEY(slot,transform,strategy_mode)
);
CREATE TABLE IF NOT EXISTS research_proper_score_manifest_coordinates (
    slot TEXT NOT NULL,
    transform TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
    strategy_mode TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
    coordinate_ordinal INTEGER NOT NULL CHECK(coordinate_ordinal>=0),
    coordinate_key TEXT NOT NULL CHECK(length(coordinate_key)=64),
    platform TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
    ticker TEXT NOT NULL,
    forecast_origin_side TEXT NOT NULL CHECK(forecast_origin_side IN ('YES','NO')),
    forecast_signal TEXT NOT NULL,
    forecast_source TEXT NOT NULL,
    forecast_version TEXT NOT NULL,
    route TEXT NOT NULL CHECK(route IN ('taker','maker')),
    PRIMARY KEY(slot,transform,strategy_mode,coordinate_key),
    UNIQUE(slot,transform,strategy_mode,coordinate_ordinal),
    FOREIGN KEY(slot,transform,strategy_mode)
      REFERENCES research_proper_score_manifests(slot,transform,strategy_mode)
);
CREATE INDEX IF NOT EXISTS idx_rpsmc_identity ON research_proper_score_manifest_coordinates(
    slot,transform,strategy_mode,platform,ticker,forecast_origin_side,forecast_signal,
    forecast_source,forecast_version,route);
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_no_update
BEFORE UPDATE ON research_proper_score_manifests
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_no_delete
BEFORE DELETE ON research_proper_score_manifests
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_coordinate_no_update
BEFORE UPDATE ON research_proper_score_manifest_coordinates
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest coordinate'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_manifest_coordinate_no_delete
BEFORE DELETE ON research_proper_score_manifest_coordinates
BEGIN SELECT RAISE(ABORT,'immutable proper-score manifest coordinate'); END;

CREATE TABLE IF NOT EXISTS research_proper_score_simulated_positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts TEXT NOT NULL, slot TEXT NOT NULL, system_name TEXT NOT NULL,
    transform TEXT NOT NULL CHECK(transform IN ('brier','log','spherical')),
    strategy_mode TEXT NOT NULL CHECK(strategy_mode IN ('fundamental','momentum')),
    cohort TEXT NOT NULL, platform TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
    ticker TEXT NOT NULL, forecast_source TEXT NOT NULL, forecast_version TEXT NOT NULL,
    prior_position REAL NOT NULL, target_position REAL NOT NULL, requested_delta REAL NOT NULL,
    filled_delta REAL NOT NULL, cancelled_delta REAL NOT NULL, post_position REAL NOT NULL,
    gross_cash_flow REAL NOT NULL, fee_total REAL NOT NULL, expected_value_delta REAL NOT NULL,
    fill_status TEXT NOT NULL CHECK(fill_status IN ('filled','partial','cancelled','noop','blocked')),
    book_source TEXT NOT NULL, source_clock_id TEXT NOT NULL, decision_latency_ms REAL NOT NULL,
    legs_json TEXT NOT NULL CHECK(json_valid(legs_json)),
    funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(slot,system_name,strategy_mode,platform,ticker,forecast_source,forecast_version)
);
CREATE INDEX IF NOT EXISTS idx_rpssim_position ON research_proper_score_simulated_positions(
    strategy_mode,transform,platform,ticker,forecast_source,forecast_version,id DESC);
CREATE TRIGGER IF NOT EXISTS research_proper_score_sim_no_update
BEFORE UPDATE ON research_proper_score_simulated_positions
BEGIN SELECT RAISE(ABORT,'immutable proper-score simulation receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_proper_score_sim_no_delete
BEFORE DELETE ON research_proper_score_simulated_positions
BEGIN SELECT RAISE(ABORT,'immutable proper-score simulation receipt'); END;

-- LIVE RFQ combos (audit F-R4): every ACCEPTED real-money combo, with our valuation at accept time and
-- every maker quote seen — the dataset that calibrates the correlation haircut against real maker pricing
-- and lets settlement/P&L be tracked for live combos (they never touch paper_parlays).
CREATE TABLE IF NOT EXISTS live_combos (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           TEXT NOT NULL,
    market       TEXT NOT NULL,             -- the instantiated MVE market ticker
    collection   TEXT NOT NULL DEFAULT '',
    legs         TEXT NOT NULL,             -- JSON [{ticker,event,side}]
    model_cohort TEXT NOT NULL DEFAULT '',  -- probability model that produced fair; blank for non-ML/legacy
    fair         REAL NOT NULL DEFAULT 0,   -- our model fair p_win (∏p_win × haircut) at accept
    prod_price   REAL NOT NULL DEFAULT 0,   -- ∏ live leg prices at accept
    quote        REAL NOT NULL DEFAULT 0,   -- accepted maker quote (per-contract $)
    contracts    REAL NOT NULL DEFAULT 1,   -- quoted size accepted (from the quote's yes_contracts_fp)
    accepted_fee REAL NOT NULL DEFAULT 0,   -- exact total taker fee attached to the accepted quote
    fee_source   TEXT NOT NULL DEFAULT '',  -- authoritative registry/waiver source captured at accept
    fee_known    INTEGER NOT NULL DEFAULT 0, -- 1 only when accepted_fee is execution-time venue truth
    accept_state TEXT NOT NULL DEFAULT 'legacy_unknown', -- dispatched | partial | filled | unfilled | accept_ambiguous | legacy_unknown
    rfq_id       TEXT NOT NULL DEFAULT '',
    quote_id     TEXT NOT NULL DEFAULT '',
    promotion_intent_id TEXT NOT NULL DEFAULT '',
    rfq_creator_order_id TEXT NOT NULL DEFAULT '',
    filled_contracts REAL NOT NULL DEFAULT 0,
    fill_receipt TEXT NOT NULL DEFAULT '',
    quotes_seen  TEXT NOT NULL DEFAULT '',  -- all open quote prices observed during the poll window
    settled      INTEGER NOT NULL DEFAULT 0,
    payout       REAL NOT NULL DEFAULT 0,   -- per-contract settlement value once known
    realized     REAL NOT NULL DEFAULT 0,   -- $ P&L once settled
    settled_ts   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_live_combos_open ON live_combos(settled);

-- P3 durability (audit §6): tiny key/value store for the cross-restart latches that used to be
-- memory-only (gate-exec dedup signatures, consensus-bridge seen-keys, free-roll / scale-out
-- progress). One row per latch key, written at mutation time, pruned by prefix+age. liveArmed is
-- DELIBERATELY not here: real-money arming must never survive a restart (fail-safe direction).
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL DEFAULT '',
    ts TEXT NOT NULL DEFAULT ''               -- RFC3339 write time (drives age-based pruning)
);

-- R128: UNLIMITED parlay-lab open ledger (operator: "every possible acceptable combo" — the
-- fee-net +EV acceptability floor is the ONLY admission screen; no cap, no eviction; combos WAIT
-- here until every leg settles). Compact rows: legs ride as one JSON blob; per-leg settlement
-- state lives in plab_leg_open so the settle-sweep is TICKER-centric and O(new settlements).
-- See internal/storage/plab.go.
CREATE TABLE IF NOT EXISTS plab_open (
    id           TEXT PRIMARY KEY,          -- 16-hex combo id (sha1 of sorted platform|ticker|side)
    at           INTEGER NOT NULL,          -- unix seconds logged
    bucket       TEXT NOT NULL DEFAULT '',  -- overlap | indep
    class        TEXT NOT NULL DEFAULT '',  -- linkage class label
    legality     TEXT NOT NULL DEFAULT '',  -- probe-cache verdict at log time
    corr_cluster INTEGER NOT NULL DEFAULT 0,
    prod         REAL NOT NULL DEFAULT 0,   -- ∏ leg prices
    joint_p      REAL NOT NULL DEFAULT 0,   -- correlation-adjusted model joint p
    rho          REAL NOT NULL DEFAULT 0,
    rho_n        INTEGER NOT NULL DEFAULT 0,
    fee_syn      REAL NOT NULL DEFAULT 0,
    fee_mve      REAL NOT NULL DEFAULT -1,
    ev_syn       REAL NOT NULL DEFAULT 0,
    ev_mve       REAL NOT NULL DEFAULT -1000,
    leg_fees     TEXT NOT NULL DEFAULT '[]',
    legs         TEXT NOT NULL,             -- JSON []plabLeg
    nlegs        INTEGER NOT NULL,
    cohort       TEXT NOT NULL DEFAULT 'all-eligible',
    route_state  TEXT NOT NULL DEFAULT 'synthetic-settlement-only',
    canonical_system_id TEXT NOT NULL DEFAULT '',
    combo_venue TEXT NOT NULL DEFAULT '',
    relation_class TEXT NOT NULL DEFAULT '',
    producer_family TEXT NOT NULL DEFAULT '',
    combo_route TEXT NOT NULL DEFAULT '',
    experiment_epoch TEXT NOT NULL DEFAULT '',
    combo_key TEXT NOT NULL DEFAULT '',
    unresolved   INTEGER NOT NULL           -- legs still awaiting settlement (0 ⇒ gradable)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_plab_open_gradable ON plab_open(unresolved, at);
CREATE INDEX IF NOT EXISTS idx_plab_open_at ON plab_open(at);
CREATE TABLE IF NOT EXISTS plab_leg_open (
    combo_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    ticker   TEXT NOT NULL,
    yes      REAL,                          -- settled YES value once known (NULL = unresolved)
    PRIMARY KEY (platform, ticker, combo_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_plab_leg_combo ON plab_leg_open(combo_id);

-- R144: one immutable receipt per economically graded Combo Lab sample. The former
-- aggregate-only KV path could count the same row again if deleting plab_open failed, and it
-- could not reconstruct a clean denominator after the pre-R144 fee/$1 bug. These receipts make
-- the all-in-capital return and its exact resolution-dependence blocks the rebuildable truth.
CREATE TABLE IF NOT EXISTS plab_grade_receipts (
    combo_id               TEXT PRIMARY KEY,
    candidate_at           INTEGER NOT NULL,
    graded_ts              TEXT NOT NULL,
    cell                   TEXT NOT NULL,
    bucket                 TEXT NOT NULL,
    class                  TEXT NOT NULL,
    legality               TEXT NOT NULL,
    cohort                 TEXT NOT NULL,
    route_state            TEXT NOT NULL,
    canonical_system_id    TEXT NOT NULL DEFAULT '',
    combo_venue            TEXT NOT NULL DEFAULT '',
    relation_class         TEXT NOT NULL DEFAULT '',
    producer_family        TEXT NOT NULL DEFAULT '',
    combo_route            TEXT NOT NULL DEFAULT '',
    experiment_epoch       TEXT NOT NULL DEFAULT '',
    combo_key              TEXT NOT NULL DEFAULT '',
    nlegs                  INTEGER NOT NULL CHECK(nlegs >= 2),
    prod                   REAL NOT NULL CHECK(prod > 0 AND prod < 1),
    joint_p                REAL NOT NULL CHECK(joint_p > 0 AND joint_p < 1),
    fees                   REAL NOT NULL CHECK(fees >= 0),
    entry_capital          REAL NOT NULL CHECK(entry_capital >= 1),
    joint_payout           REAL NOT NULL CHECK(joint_payout >= 0 AND joint_payout <= 1),
    realized_return        REAL NOT NULL CHECK(realized_return >= -1),
    predicted_return       REAL NOT NULL CHECK(predicted_return > -1),
    mve_valid              INTEGER NOT NULL DEFAULT 0,
    realized_mve_return    REAL NOT NULL DEFAULT 0 CHECK(realized_mve_return >= -1),
    won                    INTEGER NOT NULL DEFAULT 0,
    independence_keys_json TEXT NOT NULL,
    market_keys_json       TEXT NOT NULL DEFAULT '[]', -- distinct venue+ticker samples; not event dependence
    legs_json              TEXT NOT NULL,
    payouts_json           TEXT NOT NULL,
    economics_version      TEXT NOT NULL CHECK(economics_version='all-in-v2')
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_plab_grade_cell ON plab_grade_receipts(cell, graded_ts);

-- R132 BANKROLL-NEUTRAL UNIT LANE. One executable one-share observation per
-- family×venue×market×side×gap-episode, regardless of whether any paper book had allocation.
-- This is research truth only; real/paper execution retains collateral and exposure walls.
CREATE TABLE IF NOT EXISTS unit_trials (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    opened_ts      TEXT NOT NULL,
    closed_ts      TEXT NOT NULL DEFAULT '',
      family         TEXT NOT NULL,
      platform       TEXT NOT NULL,
      origin_layer   TEXT NOT NULL DEFAULT 'model'
                         CHECK(origin_layer IN ('model','strategy')),
      ticker         TEXT NOT NULL,
    side           TEXT NOT NULL,
    episode        INTEGER NOT NULL DEFAULT 0,
    -- R144: the immutable canonical event version that was current when this opportunity
    -- first entered the one-share ledger. Blank/zero means identity was not yet available;
    -- it is deliberately never guessed or backfilled from a later mutable catalog join.
    canonical_event_id TEXT NOT NULL DEFAULT '',
    event_version  INTEGER NOT NULL DEFAULT 0 CHECK(event_version >= 0),
    category       TEXT NOT NULL DEFAULT '',
    ask            REAL NOT NULL,
    fee_pc         REAL NOT NULL DEFAULT 0,
    fee_known      INTEGER NOT NULL DEFAULT 0,
    fee_source     TEXT NOT NULL DEFAULT '',
    depth          REAL NOT NULL DEFAULT 0,
    quote_source   TEXT NOT NULL DEFAULT '',
    resolve_hours  REAL NOT NULL DEFAULT 0,
    settled        INTEGER NOT NULL DEFAULT 0,
    settle_val     REAL,
    pnl_pc         REAL,
    return_per_dollar REAL,
    capital_day    REAL,
      UNIQUE(family, platform, ticker, side, episode, origin_layer)
);
CREATE INDEX IF NOT EXISTS idx_unit_trials_open ON unit_trials(settled, platform, ticker);
CREATE INDEX IF NOT EXISTS idx_unit_trials_family ON unit_trials(family, platform, settled);
-- Do not create idx_unit_trials_event here. On an existing pre-R144 database, CREATE TABLE IF
-- NOT EXISTS leaves the legacy unit_trials shape in place, so indexing canonical_event_id here
-- bricks Open before its additive/rebuild migrations can add that column. migrateUnitTrialOrigin
-- and migrateUnitTrialEventIdentity own this index after the legacy shape is upgraded.

-- R139: bounded per-family signal -> executable-book funnel receipts. Historical verdict rows
-- are not evidence that a producer is still alive; these minute-batched receipts preserve current
-- candidate, quote, fee/depth, and one-share-ledger outcomes without adding one SQLite write per
-- signal. Producer state is advisory/operational only and never grants Paper or LIVE authority.
CREATE TABLE IF NOT EXISTS native_system_funnel_receipts (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts          TEXT NOT NULL,
    cycle_id             TEXT NOT NULL,
    family               TEXT NOT NULL,
    platform             TEXT NOT NULL,
    origin_layer         TEXT NOT NULL DEFAULT 'model',
    producer_state       TEXT NOT NULL,
    producer_reason      TEXT NOT NULL,
    eligible_count       INTEGER NOT NULL DEFAULT 0 CHECK(eligible_count >= 0),
    input_rows           INTEGER NOT NULL DEFAULT 0 CHECK(input_rows >= 0),
    quote_attempts       INTEGER NOT NULL DEFAULT 0 CHECK(quote_attempts >= 0),
    unit_rows            INTEGER NOT NULL DEFAULT 0 CHECK(unit_rows >= 0),
    money_truth_attempts INTEGER NOT NULL DEFAULT 0 CHECK(money_truth_attempts >= 0),
    exclusions_json      TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(exclusions_json)),
    first_input_ts       TEXT NOT NULL DEFAULT '',
    last_input_ts        TEXT NOT NULL DEFAULT '',
    last_economic_ts     TEXT NOT NULL DEFAULT '',
    funded               INTEGER NOT NULL DEFAULT 0 CHECK(funded = 0),
    paper_authority      INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority = 0),
    live_authority       INTEGER NOT NULL DEFAULT 0 CHECK(live_authority = 0),
    UNIQUE(cycle_id,family,platform,origin_layer)
);
CREATE INDEX IF NOT EXISTS idx_native_system_funnel_latest
    ON native_system_funnel_receipts(family,platform,origin_layer,id DESC);

-- R138 STEP 1: immutable research identity + governance foundation.  These are specifications,
-- not trading state.  Every semantic change creates a new version; UPDATE/DELETE is prohibited by
-- triggers so a result can never be retrofitted to a changed hypothesis or payoff definition.
CREATE TABLE IF NOT EXISTS research_event_specs (
    event_id            TEXT NOT NULL,
    version             INTEGER NOT NULL,
    spec_hash           TEXT NOT NULL,
    event_type          TEXT NOT NULL DEFAULT 'unknown',
    domain              TEXT NOT NULL DEFAULT '',
    title               TEXT NOT NULL DEFAULT '',
    start_ts            TEXT NOT NULL DEFAULT '',
    deadline_ts         TEXT NOT NULL DEFAULT '',
    timezone            TEXT NOT NULL DEFAULT '',
    settlement_source   TEXT NOT NULL DEFAULT '',
    source_artifact     TEXT NOT NULL DEFAULT '',
    source_clock_id     TEXT NOT NULL DEFAULT '',
    boundary_rule       TEXT NOT NULL DEFAULT '',
    revision_policy     TEXT NOT NULL DEFAULT '',
    void_policy         TEXT NOT NULL DEFAULT '',
    outcome_set_status  TEXT NOT NULL DEFAULT 'unknown'
                            CHECK(outcome_set_status IN ('complete','incomplete','unknown')),
    evidence_json       TEXT NOT NULL DEFAULT '{}',
    created_ts          TEXT NOT NULL,
    PRIMARY KEY(event_id,version),
    UNIQUE(event_id,spec_hash)
);
CREATE INDEX IF NOT EXISTS idx_revent_domain ON research_event_specs(domain,event_type,event_id);

CREATE TABLE IF NOT EXISTS research_payoff_specs (
    payoff_id           TEXT NOT NULL,
    version             INTEGER NOT NULL,
    spec_hash           TEXT NOT NULL,
    event_id            TEXT NOT NULL,
    event_version       INTEGER NOT NULL,
    label               TEXT NOT NULL DEFAULT '',
    predicate_json      TEXT NOT NULL DEFAULT '{}',
    boundary_rule       TEXT NOT NULL DEFAULT '',
    payout_floor        REAL NOT NULL DEFAULT 0,
    payout_ceiling      REAL NOT NULL DEFAULT 1,
    settlement_source   TEXT NOT NULL DEFAULT '',
    source_artifact     TEXT NOT NULL DEFAULT '',
    identity_status     TEXT NOT NULL DEFAULT 'unverified'
                            CHECK(identity_status IN ('verified','structural','unverified','rejected')),
    evidence_json       TEXT NOT NULL DEFAULT '{}',
    created_ts          TEXT NOT NULL,
    PRIMARY KEY(payoff_id,version),
    UNIQUE(payoff_id,spec_hash),
    UNIQUE(event_id,event_version,payoff_id,version),
    FOREIGN KEY(event_id,event_version) REFERENCES research_event_specs(event_id,version)
);
CREATE INDEX IF NOT EXISTS idx_rpayoff_event ON research_payoff_specs(event_id,event_version,payoff_id);

CREATE TABLE IF NOT EXISTS research_instrument_specs (
    venue               TEXT NOT NULL,
    ticker              TEXT NOT NULL,
    version             INTEGER NOT NULL,
    spec_hash           TEXT NOT NULL,
    event_id            TEXT NOT NULL,
    event_version       INTEGER NOT NULL,
    payoff_id           TEXT NOT NULL,
    payoff_version      INTEGER NOT NULL,
    native_side         TEXT NOT NULL DEFAULT 'YES',
    orientation         TEXT NOT NULL DEFAULT 'same'
                            CHECK(orientation IN ('same','inverse','basis','unknown')),
    market_kind         TEXT NOT NULL DEFAULT '',
    scope               TEXT NOT NULL DEFAULT '',
    line                REAL NOT NULL DEFAULT 0,
    close_ts            TEXT NOT NULL DEFAULT '',
    settlement_source   TEXT NOT NULL DEFAULT '',
    rules_artifact      TEXT NOT NULL DEFAULT '',
    rules_hash          TEXT NOT NULL DEFAULT '',
    fee_authority       TEXT NOT NULL DEFAULT '',
    cross_venue_basis   TEXT NOT NULL DEFAULT '',
    identity_status     TEXT NOT NULL DEFAULT 'unverified'
                            CHECK(identity_status IN ('verified','structural','unverified','rejected')),
    evidence_json       TEXT NOT NULL DEFAULT '{}',
    created_ts          TEXT NOT NULL,
    PRIMARY KEY(venue,ticker,version),
    UNIQUE(venue,ticker,spec_hash),
    FOREIGN KEY(event_id,event_version,payoff_id,payoff_version)
        REFERENCES research_payoff_specs(event_id,event_version,payoff_id,version)
);
CREATE INDEX IF NOT EXISTS idx_rinstrument_payoff ON research_instrument_specs(payoff_id,payoff_version,venue);
CREATE INDEX IF NOT EXISTS idx_rinstrument_event ON research_instrument_specs(event_id,event_version,venue);
-- R139: the certified-deadline collector starts from a tiny verified subset. Without this
-- partial-order index SQLite materialized every latest instrument version before discovering that
-- the current registry contained zero verified implication rows, consuming the full four-minute
-- research deadline and starving unrelated collectors/writers.
CREATE INDEX IF NOT EXISTS idx_rinstrument_verified_payoff_latest
    ON research_instrument_specs(venue,identity_status,event_id,event_version,payoff_id,payoff_version,ticker,version DESC);

CREATE TABLE IF NOT EXISTS research_payoff_relations (
    relation_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    spec_hash           TEXT NOT NULL,
    event_id            TEXT NOT NULL,
    event_version       INTEGER NOT NULL,
    left_payoff_id      TEXT NOT NULL,
    left_payoff_version INTEGER NOT NULL,
    right_payoff_id     TEXT NOT NULL,
    right_payoff_version INTEGER NOT NULL,
    relation_type       TEXT NOT NULL
                            CHECK(relation_type IN ('implies','excludes','exhaustive_with','basis','correlated')),
    payout_floor        REAL,
    identity_status     TEXT NOT NULL DEFAULT 'unverified'
                            CHECK(identity_status IN ('verified','structural','unverified','rejected')),
    evidence_json       TEXT NOT NULL DEFAULT '{}',
    created_ts          TEXT NOT NULL,
    PRIMARY KEY(relation_id,version),
    UNIQUE(relation_id,spec_hash),
    FOREIGN KEY(event_id,event_version,left_payoff_id,left_payoff_version)
        REFERENCES research_payoff_specs(event_id,event_version,payoff_id,version),
    FOREIGN KEY(event_id,event_version,right_payoff_id,right_payoff_version)
        REFERENCES research_payoff_specs(event_id,event_version,payoff_id,version)
);
CREATE INDEX IF NOT EXISTS idx_rrelation_event ON research_payoff_relations(event_id,event_version,relation_type);
CREATE INDEX IF NOT EXISTS idx_rrelation_verified_latest
    ON research_payoff_relations(relation_type,identity_status,relation_id,version DESC);

-- One source-time sighting records each newly inserted immutable version. Collector receipts,
-- not rescans of old rows, carry ongoing liveness so this table stays bounded by semantic change.
CREATE TABLE IF NOT EXISTS research_identity_sightings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts     TEXT NOT NULL,
    slot            TEXT NOT NULL,
    object_type     TEXT NOT NULL CHECK(object_type IN ('event','payoff','instrument','relation')),
    object_key      TEXT NOT NULL,
    object_version  INTEGER NOT NULL,
    source          TEXT NOT NULL DEFAULT '',
    UNIQUE(slot,object_type,object_key,object_version,source)
);
CREATE INDEX IF NOT EXISTS idx_ridentity_seen ON research_identity_sightings(object_type,object_key,observed_ts);

CREATE TABLE IF NOT EXISTS research_experiment_specs (
    experiment_id       TEXT NOT NULL,
    version             INTEGER NOT NULL,
    spec_hash           TEXT NOT NULL,
    system_name         TEXT NOT NULL,
    mechanism           TEXT NOT NULL,
    hypothesis          TEXT NOT NULL,
    null_hypothesis     TEXT NOT NULL,
    direction           TEXT NOT NULL DEFAULT 'two-sided',
    cohort_json         TEXT NOT NULL DEFAULT '{}',
    route               TEXT NOT NULL DEFAULT 'research-only',
    holdout_json        TEXT NOT NULL DEFAULT '{}',
    stopping_rule       TEXT NOT NULL,
    multiplicity_family TEXT NOT NULL,
    cells_json          TEXT NOT NULL DEFAULT '{}',
    primary_estimand    TEXT NOT NULL,
    horizon_rule        TEXT NOT NULL,
    fee_route_rule      TEXT NOT NULL,
    source_schema_json  TEXT NOT NULL DEFAULT '{}',
    inference_json      TEXT NOT NULL DEFAULT '{}',
    shrinkage_json      TEXT NOT NULL DEFAULT '{}',
    multiplicity_method TEXT NOT NULL,
    capacity_metric     TEXT NOT NULL,
    capital_time_metric TEXT NOT NULL,
    system_kill_rule    TEXT NOT NULL,
    code_manifest_hash  TEXT NOT NULL,
    data_manifest_hash  TEXT NOT NULL,
    parent_experiment   TEXT,
    parent_version      INTEGER,
    required_days       INTEGER NOT NULL DEFAULT 30,
    initial_state       TEXT NOT NULL DEFAULT 'SPECIFIED_NOT_IMPLEMENTED',
    funded              INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority     INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority      INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    created_ts          TEXT NOT NULL,
    PRIMARY KEY(experiment_id,version),
    UNIQUE(experiment_id,spec_hash),
    CHECK((parent_experiment IS NULL AND parent_version IS NULL) OR
          (parent_experiment IS NOT NULL AND parent_version IS NOT NULL)),
    FOREIGN KEY(parent_experiment,parent_version)
        REFERENCES research_experiment_specs(experiment_id,version)
);
CREATE INDEX IF NOT EXISTS idx_rexperiment_system ON research_experiment_specs(system_name,version);

CREATE TABLE IF NOT EXISTS research_experiment_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    experiment_id       TEXT NOT NULL,
    experiment_version  INTEGER NOT NULL,
    observed_ts         TEXT NOT NULL,
    event_type          TEXT NOT NULL
                            CHECK(event_type IN ('registered','collecting','result','holdout_pass','holdout_fail','killed','blocked','note')),
    state               TEXT NOT NULL DEFAULT '',
    message             TEXT NOT NULL DEFAULT '',
    evidence_json       TEXT NOT NULL DEFAULT '{}',
    result_class        TEXT NOT NULL DEFAULT '',
    code_manifest_hash  TEXT NOT NULL DEFAULT '',
    data_manifest_hash  TEXT NOT NULL DEFAULT '',
    source_manifest_hash TEXT NOT NULL DEFAULT '',
    prior_event_id      INTEGER,
    FOREIGN KEY(experiment_id,experiment_version) REFERENCES research_experiment_specs(experiment_id,version)
);
CREATE INDEX IF NOT EXISTS idx_rexperiment_events ON research_experiment_events(experiment_id,experiment_version,id);

-- Immutable means immutable: later truth is appended as a new version/event, never rewritten.
CREATE TRIGGER IF NOT EXISTS research_event_specs_no_update BEFORE UPDATE ON research_event_specs BEGIN SELECT RAISE(ABORT,'immutable research event spec'); END;
CREATE TRIGGER IF NOT EXISTS research_event_specs_no_delete BEFORE DELETE ON research_event_specs BEGIN SELECT RAISE(ABORT,'immutable research event spec'); END;
CREATE TRIGGER IF NOT EXISTS research_payoff_specs_no_update BEFORE UPDATE ON research_payoff_specs BEGIN SELECT RAISE(ABORT,'immutable research payoff spec'); END;
CREATE TRIGGER IF NOT EXISTS research_payoff_specs_no_delete BEFORE DELETE ON research_payoff_specs BEGIN SELECT RAISE(ABORT,'immutable research payoff spec'); END;
CREATE TRIGGER IF NOT EXISTS research_instrument_specs_no_update BEFORE UPDATE ON research_instrument_specs BEGIN SELECT RAISE(ABORT,'immutable research instrument spec'); END;
CREATE TRIGGER IF NOT EXISTS research_instrument_specs_no_delete BEFORE DELETE ON research_instrument_specs BEGIN SELECT RAISE(ABORT,'immutable research instrument spec'); END;
CREATE TRIGGER IF NOT EXISTS research_payoff_relations_no_update BEFORE UPDATE ON research_payoff_relations BEGIN SELECT RAISE(ABORT,'immutable research payoff relation'); END;
CREATE TRIGGER IF NOT EXISTS research_payoff_relations_no_delete BEFORE DELETE ON research_payoff_relations BEGIN SELECT RAISE(ABORT,'immutable research payoff relation'); END;
CREATE TRIGGER IF NOT EXISTS research_experiment_specs_no_update BEFORE UPDATE ON research_experiment_specs BEGIN SELECT RAISE(ABORT,'immutable research experiment spec'); END;
CREATE TRIGGER IF NOT EXISTS research_experiment_specs_no_delete BEFORE DELETE ON research_experiment_specs BEGIN SELECT RAISE(ABORT,'immutable research experiment spec'); END;
CREATE TRIGGER IF NOT EXISTS research_experiment_events_no_update BEFORE UPDATE ON research_experiment_events BEGIN SELECT RAISE(ABORT,'append-only research experiment event'); END;
CREATE TRIGGER IF NOT EXISTS research_experiment_events_no_delete BEFORE DELETE ON research_experiment_events BEGIN SELECT RAISE(ABORT,'append-only research experiment event'); END;

-- R138 STEP 2: one durable liveness contract for every research collector. A zero is useful only
-- when the operator can distinguish no eligible universe from an API error, schema rejection,
-- exclusion funnel, duplicate-only cycle, or a worker that never ran.
CREATE TABLE IF NOT EXISTS research_collector_specs (
    collector_id        TEXT NOT NULL,
    version             INTEGER NOT NULL,
    spec_hash           TEXT NOT NULL,
    experiment_id       TEXT NOT NULL DEFAULT '',
    experiment_version  INTEGER NOT NULL DEFAULT 0,
    expected_cadence_s  REAL NOT NULL,
    source              TEXT NOT NULL,
    schema_version      TEXT NOT NULL,
    systems_json        TEXT NOT NULL DEFAULT '[]',
    zero_policy         TEXT NOT NULL,
    active              INTEGER NOT NULL DEFAULT 1,
    created_ts          TEXT NOT NULL,
    PRIMARY KEY(collector_id,version),
    UNIQUE(collector_id,spec_hash)
);
CREATE TRIGGER IF NOT EXISTS research_collector_specs_no_update BEFORE UPDATE ON research_collector_specs BEGIN SELECT RAISE(ABORT,'immutable collector spec'); END;
CREATE TRIGGER IF NOT EXISTS research_collector_specs_no_delete BEFORE DELETE ON research_collector_specs BEGIN SELECT RAISE(ABORT,'immutable collector spec'); END;

CREATE TABLE IF NOT EXISTS research_collector_receipts (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    collector_id        TEXT NOT NULL,
    cycle_id            TEXT NOT NULL,
    experiment_id       TEXT NOT NULL DEFAULT '',
    experiment_version  INTEGER NOT NULL DEFAULT 0,
    started_ts          TEXT NOT NULL,
    completed_ts        TEXT NOT NULL,
    status              TEXT NOT NULL
                            CHECK(status IN ('healthy','healthy_empty','starved','blocked','error')),
    eligible            INTEGER NOT NULL DEFAULT 0 CHECK(eligible>=0),
    attempted           INTEGER NOT NULL DEFAULT 0 CHECK(attempted>=0),
    inserted            INTEGER NOT NULL DEFAULT 0 CHECK(inserted>=0),
    duplicates          INTEGER NOT NULL DEFAULT 0 CHECK(duplicates>=0),
    excluded_total      INTEGER NOT NULL DEFAULT 0 CHECK(excluded_total>=0),
    exclusions_json     TEXT NOT NULL DEFAULT '{}',
    metrics_json        TEXT NOT NULL DEFAULT '{}',
    expected_zero       INTEGER NOT NULL DEFAULT 0,
    zero_reason         TEXT NOT NULL DEFAULT '',
    error_class         TEXT NOT NULL DEFAULT '',
    error_text          TEXT NOT NULL DEFAULT '',
    source              TEXT NOT NULL DEFAULT '',
    schema_version      TEXT NOT NULL DEFAULT '',
    expected_cadence_s  REAL NOT NULL DEFAULT 0,
    duration_ms         REAL NOT NULL DEFAULT 0,
    replay_sequence_gaps INTEGER NOT NULL DEFAULT 0,
    systems_json        TEXT NOT NULL DEFAULT '[]',
    funded              INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority     INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority      INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(collector_id,cycle_id)
);
CREATE INDEX IF NOT EXISTS idx_rcollector_latest ON research_collector_receipts(collector_id,id DESC);
CREATE INDEX IF NOT EXISTS idx_rcollector_status ON research_collector_receipts(status,completed_ts);
CREATE TRIGGER IF NOT EXISTS research_collector_receipts_no_update BEFORE UPDATE ON research_collector_receipts BEGIN SELECT RAISE(ABORT,'append-only collector receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_collector_receipts_no_delete BEFORE DELETE ON research_collector_receipts BEGIN SELECT RAISE(ABORT,'append-only collector receipt'); END;

-- R138 SOURCE CLOCK: immutable definitions of authoritative publication/venue clocks and an
-- append-only watermark trail. These tables are research provenance only; all authority flags are
-- structurally pinned to zero and no execution component reads them.
CREATE TABLE IF NOT EXISTS research_source_clock_specs (
    source_id                 TEXT NOT NULL,
    version                   INTEGER NOT NULL,
    spec_hash                 TEXT NOT NULL,
    display_name              TEXT NOT NULL,
    authority_url             TEXT NOT NULL DEFAULT '',
    schema_version            TEXT NOT NULL,
    schema_hash               TEXT NOT NULL,
    clock_kind                TEXT NOT NULL
                                  CHECK(clock_kind IN ('source_timestamp','monotonic_sequence','source_timestamp_sequence','arrival_only')),
    timestamp_field           TEXT NOT NULL DEFAULT '',
    sequence_field            TEXT NOT NULL DEFAULT '',
    timezone                  TEXT NOT NULL DEFAULT 'UTC',
    expected_cadence_s        REAL NOT NULL CHECK(expected_cadence_s>0),
    gap_tolerance_s           REAL NOT NULL CHECK(gap_tolerance_s>=expected_cadence_s),
    max_lag_s                 REAL NOT NULL CHECK(max_lag_s>=0),
    publication_lag_s         REAL NOT NULL DEFAULT 0 CHECK(publication_lag_s>=0),
    revision_policy           TEXT NOT NULL,
    settlement_compatibility  TEXT NOT NULL,
    cache_policy              TEXT NOT NULL DEFAULT '',
    active                    INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
    funded                    INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority           INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority            INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    created_ts                TEXT NOT NULL,
    PRIMARY KEY(source_id,version),
    UNIQUE(source_id,spec_hash)
);
CREATE INDEX IF NOT EXISTS idx_rsource_clock_active ON research_source_clock_specs(active,source_id,version DESC);
CREATE TRIGGER IF NOT EXISTS research_source_clock_specs_no_update BEFORE UPDATE ON research_source_clock_specs BEGIN SELECT RAISE(ABORT,'immutable source clock spec'); END;
CREATE TRIGGER IF NOT EXISTS research_source_clock_specs_no_delete BEFORE DELETE ON research_source_clock_specs BEGIN SELECT RAISE(ABORT,'immutable source clock spec'); END;

CREATE TABLE IF NOT EXISTS research_source_clock_receipts (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id                TEXT NOT NULL,
    spec_version             INTEGER NOT NULL,
    received_ts              TEXT NOT NULL,
    source_watermark_ts      TEXT NOT NULL DEFAULT '',
    schema_version           TEXT NOT NULL,
    schema_hash              TEXT NOT NULL,
    sequence_available       INTEGER NOT NULL DEFAULT 0 CHECK(sequence_available IN (0,1)),
    sequence_start           INTEGER,
    sequence_end             INTEGER,
    rows_seen                INTEGER NOT NULL DEFAULT 0 CHECK(rows_seen>=0),
    status                   TEXT NOT NULL
                                 CHECK(status IN ('healthy','gap','regression','schema_drift','blocked','error')),
    gap_seconds              REAL NOT NULL DEFAULT 0 CHECK(gap_seconds>=0),
    regression_seconds       REAL NOT NULL DEFAULT 0 CHECK(regression_seconds>=0),
    sequence_gap             INTEGER NOT NULL DEFAULT 0 CHECK(sequence_gap>=0),
    lag_seconds              REAL NOT NULL DEFAULT 0 CHECK(lag_seconds>=0),
    error_class              TEXT NOT NULL DEFAULT '',
    error_text               TEXT NOT NULL DEFAULT '',
    evidence_json            TEXT NOT NULL DEFAULT '{}',
    replay_segment_hash      TEXT NOT NULL DEFAULT '',
    funded                   INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority          INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority           INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    FOREIGN KEY(source_id,spec_version) REFERENCES research_source_clock_specs(source_id,version)
);
CREATE INDEX IF NOT EXISTS idx_rsource_clock_latest ON research_source_clock_receipts(source_id,id DESC);
CREATE INDEX IF NOT EXISTS idx_rsource_clock_spec_latest ON research_source_clock_receipts(source_id,spec_version,id DESC);
CREATE INDEX IF NOT EXISTS idx_rsource_clock_status ON research_source_clock_receipts(status,received_ts);
CREATE TRIGGER IF NOT EXISTS research_source_clock_receipts_no_update BEFORE UPDATE ON research_source_clock_receipts BEGIN SELECT RAISE(ABORT,'append-only source clock receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_source_clock_receipts_no_delete BEFORE DELETE ON research_source_clock_receipts BEGIN SELECT RAISE(ABORT,'append-only source clock receipt'); END;

-- R138 VENUE NOTICE: immutable, versioned copies of authoritative public venue notices and
-- bounded liveness sightings. Advisory research only: the database structurally denies paper or
-- LIVE authority, and no fee/schema/config/order path reads these tables.
CREATE TABLE IF NOT EXISTS research_venue_notices (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    venue               TEXT NOT NULL CHECK(venue IN ('kalshi','polyus','polymarket')),
    source_name         TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    version             INTEGER NOT NULL CHECK(version>0),
    content_hash        TEXT NOT NULL,
    title               TEXT NOT NULL,
    summary             TEXT NOT NULL DEFAULT '',
    published_ts        TEXT NOT NULL DEFAULT '',
    effective_ts        TEXT NOT NULL DEFAULT '',
    source_url          TEXT NOT NULL DEFAULT '',
    source_feed         TEXT NOT NULL,
    categories_json     TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(categories_json)),
    change_classes_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(change_classes_json)),
    severity            TEXT NOT NULL CHECK(severity IN ('info','attention','breaking')),
    artifact_hash       TEXT NOT NULL,
    first_seen_ts       TEXT NOT NULL,
    funded              INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority     INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority      INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(venue,source_name,source_id,version),
    UNIQUE(venue,source_name,source_id,content_hash)
);
CREATE INDEX IF NOT EXISTS idx_rvenue_notice_current ON research_venue_notices(venue,source_name,source_id,version DESC);
CREATE INDEX IF NOT EXISTS idx_rvenue_notice_published ON research_venue_notices(published_ts DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_rvenue_notice_severity ON research_venue_notices(severity,first_seen_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_venue_notices_no_update BEFORE UPDATE ON research_venue_notices BEGIN SELECT RAISE(ABORT,'immutable venue notice version'); END;
CREATE TRIGGER IF NOT EXISTS research_venue_notices_no_delete BEFORE DELETE ON research_venue_notices BEGIN SELECT RAISE(ABORT,'immutable venue notice version'); END;

CREATE TABLE IF NOT EXISTS research_venue_notice_sightings (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    notice_id          INTEGER NOT NULL,
    observed_ts        TEXT NOT NULL,
    slot               TEXT NOT NULL,
    feed_etag          TEXT NOT NULL DEFAULT '',
    feed_last_modified TEXT NOT NULL DEFAULT '',
    funded             INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority    INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority     INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(notice_id,slot),
    FOREIGN KEY(notice_id) REFERENCES research_venue_notices(id)
);
CREATE INDEX IF NOT EXISTS idx_rvenue_notice_sighting_latest ON research_venue_notice_sightings(notice_id,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_venue_notice_sightings_no_update BEFORE UPDATE ON research_venue_notice_sightings BEGIN SELECT RAISE(ABORT,'append-only venue notice sighting'); END;
CREATE TRIGGER IF NOT EXISTS research_venue_notice_sightings_no_delete BEFORE DELETE ON research_venue_notice_sightings BEGIN SELECT RAISE(ABORT,'append-only venue notice sighting'); END;

-- R138 STEPS 6-9: prospective, route-specific system evidence.  These rows are observations,
-- never orders or promotion state.  An open payoff is an interval rather than a scratch; exact
-- settlement/void evidence appends to research_system_payoff_updates and never rewrites history.
CREATE TABLE IF NOT EXISTS research_system_observations (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts           TEXT NOT NULL,
    decision_ts           TEXT NOT NULL DEFAULT '',
    observed_slot         TEXT NOT NULL,
    system_id             TEXT NOT NULL,
    experiment_version    INTEGER NOT NULL DEFAULT 1 CHECK(experiment_version>0),
    opportunity_id        TEXT NOT NULL,
    observation_kind      TEXT NOT NULL CHECK(observation_kind IN ('candidate','negative','control')),
    cohort                TEXT NOT NULL DEFAULT '',
    canonical_event_id    TEXT NOT NULL DEFAULT '',
    event_version         INTEGER NOT NULL DEFAULT 0 CHECK(event_version>=0),
    canonical_payoff_id   TEXT,
    payoff_version        INTEGER CHECK(payoff_version IS NULL OR payoff_version>0),
    instrument_version    INTEGER NOT NULL DEFAULT 0 CHECK(instrument_version>=0),
    venue                 TEXT NOT NULL DEFAULT '',
    ticker                TEXT NOT NULL DEFAULT '',
    route                 TEXT NOT NULL CHECK(route IN ('observer','taker','maker','maker-control','rfq')),
    side                  TEXT NOT NULL DEFAULT '',
    certificate_status    TEXT NOT NULL CHECK(certificate_status IN ('verified','structural','unverified','rejected','not_applicable')),
    certificate_hash      TEXT NOT NULL DEFAULT '',
    source_clock_id       TEXT NOT NULL DEFAULT '',
    source_artifact       TEXT NOT NULL DEFAULT '',
    book_source           TEXT NOT NULL DEFAULT '',
    fee_source            TEXT NOT NULL DEFAULT '',
    quote_age_max_s       REAL NOT NULL DEFAULT 0 CHECK(quote_age_max_s>=0),
    tick_min              REAL NOT NULL DEFAULT 0 CHECK(tick_min>=0),
    size_units            REAL NOT NULL DEFAULT 0 CHECK(size_units>=0),
    executable_cost       REAL NOT NULL DEFAULT 0 CHECK(executable_cost>=0),
    exact_fee             REAL NOT NULL DEFAULT 0,
    payout_lower          REAL NOT NULL DEFAULT 0,
    payout_upper          REAL NOT NULL DEFAULT 0,
    net_lower             REAL NOT NULL DEFAULT 0,
    net_upper             REAL NOT NULL DEFAULT 0,
    visible_capacity      REAL NOT NULL DEFAULT 0 CHECK(visible_capacity>=0),
    capital_seconds       REAL NOT NULL DEFAULT 0 CHECK(capital_seconds>=0),
    decision_latency_ms   REAL NOT NULL DEFAULT 0 CHECK(decision_latency_ms>=0),
    latency_known         INTEGER NOT NULL DEFAULT 0 CHECK(latency_known IN (0,1)),
    quote_age_known       INTEGER NOT NULL DEFAULT 0 CHECK(quote_age_known IN (0,1)),
    tick_known            INTEGER NOT NULL DEFAULT 0 CHECK(tick_known IN (0,1)),
    depth_known           INTEGER NOT NULL DEFAULT 0 CHECK(depth_known IN (0,1)),
    fee_known             INTEGER NOT NULL DEFAULT 0 CHECK(fee_known IN (0,1)),
    capacity_curve_json   TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(capacity_curve_json)),
    inputs_json           TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(inputs_json)),
    outcome_status        TEXT NOT NULL DEFAULT 'open'
                              CHECK(outcome_status IN ('open','settled','voided','censored')),
    blocker               TEXT NOT NULL DEFAULT '',
    candidate             INTEGER NOT NULL DEFAULT 0 CHECK(candidate IN (0,1)),
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(system_id,opportunity_id,observed_slot,route,size_units,observation_kind),
    CHECK(candidate=0 OR (latency_known=1 AND quote_age_known=1 AND tick_known=1 AND depth_known=1 AND fee_known=1)),
    FOREIGN KEY(canonical_event_id,event_version,canonical_payoff_id,payoff_version)
        REFERENCES research_payoff_specs(event_id,event_version,payoff_id,version)
);
CREATE INDEX IF NOT EXISTS idx_rsystem_obs_system ON research_system_observations(system_id,observed_ts DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_rsystem_obs_open ON research_system_observations(outcome_status,system_id,id);
CREATE INDEX IF NOT EXISTS idx_rsystem_obs_event ON research_system_observations(canonical_event_id,event_version,observed_ts DESC);
CREATE INDEX IF NOT EXISTS idx_rsystem_obs_observed ON research_system_observations(observed_ts,id);
-- R143 terminal grading starts from only open, immediately-executable taker observations.  The
-- old reader joined 583k observations to 598k JSON route rows and then sorted the result, holding
-- a live WAL reader for up to 90 seconds.  This partial index keeps that maintenance lane bounded.
CREATE INDEX IF NOT EXISTS idx_rsystem_obs_pending_taker
    ON research_system_observations(observed_ts,id)
    WHERE outcome_status='open' AND route='taker' AND observation_kind IN ('candidate','control');
-- R175: exact venue-local Step-7 structural actions are blocked negatives, not executable
-- candidates. Keep their settlement lookup bounded so an authoritative venue outcome can attach
-- fee-net research truth without widening the general candidate or authority contract.
CREATE INDEX IF NOT EXISTS idx_rsystem_obs_pending_step7_structural_v3
    ON research_system_observations(observed_ts,id)
    WHERE outcome_status='open' AND route='taker'
      AND observation_kind='negative' AND candidate=0
      AND system_id='flow-direction-integrity' AND experiment_version=3
      AND cohort='authoritative-flow-follow-atomic-v3|structural-action|policy=authoritative-flow-follow-atomic-v3'
      AND certificate_status='structural'
      AND blocker='venue_local_structural_identity_only_no_profit_or_cash_authority';

-- R144: dashboard/liveness summaries must never full-scan this append-only ledger while the
-- collector is writing.  The tiny materialized counter table is maintained in the same INSERT
-- transaction, so UI/report readers cannot pin the WAL behind a GROUP BY over the full history.
CREATE TABLE IF NOT EXISTS research_system_collection_totals (
    system_id              TEXT PRIMARY KEY,
    economic_observations  INTEGER NOT NULL DEFAULT 0 CHECK(economic_observations>=0),
    candidates             INTEGER NOT NULL DEFAULT 0 CHECK(candidates>=0),
    controls               INTEGER NOT NULL DEFAULT 0 CHECK(controls>=0),
    open_economic          INTEGER NOT NULL DEFAULT 0 CHECK(open_economic>=0),
    raw_observations       INTEGER NOT NULL DEFAULT 0 CHECK(raw_observations>=0),
    raw_candidates         INTEGER NOT NULL DEFAULT 0 CHECK(raw_candidates>=0),
    raw_negative           INTEGER NOT NULL DEFAULT 0 CHECK(raw_negative>=0),
    raw_open_envelopes     INTEGER NOT NULL DEFAULT 0 CHECK(raw_open_envelopes>=0)
);
CREATE TRIGGER IF NOT EXISTS research_system_collection_totals_insert
AFTER INSERT ON research_system_observations
BEGIN
  INSERT INTO research_system_collection_totals(
    system_id,economic_observations,candidates,controls,open_economic
  ) VALUES(
    NEW.system_id,
    CASE WHEN NEW.route!='observer' AND TRIM(NEW.source_clock_id)!=''
      AND TRIM(NEW.book_source)!='' AND TRIM(NEW.fee_source)!=''
      AND NEW.size_units>0 AND NEW.executable_cost>0
      AND NEW.latency_known=1 AND NEW.quote_age_known=1 AND NEW.tick_known=1
      AND NEW.depth_known=1 AND NEW.fee_known=1 THEN 1 ELSE 0 END,
    CASE WHEN NEW.candidate=1 AND NEW.route!='observer' AND TRIM(NEW.source_clock_id)!=''
      AND TRIM(NEW.book_source)!='' AND TRIM(NEW.fee_source)!=''
      AND NEW.size_units>0 AND NEW.executable_cost>0
      AND NEW.latency_known=1 AND NEW.quote_age_known=1 AND NEW.tick_known=1
      AND NEW.depth_known=1 AND NEW.fee_known=1 THEN 1 ELSE 0 END,
    CASE WHEN NEW.observation_kind='control' THEN 1 ELSE 0 END,
    CASE WHEN NEW.outcome_status='open' AND NEW.route!='observer'
      AND TRIM(NEW.source_clock_id)!='' AND TRIM(NEW.book_source)!=''
      AND TRIM(NEW.fee_source)!='' AND NEW.size_units>0 AND NEW.executable_cost>0
      AND NEW.latency_known=1 AND NEW.quote_age_known=1 AND NEW.tick_known=1
      AND NEW.depth_known=1 AND NEW.fee_known=1 THEN 1 ELSE 0 END
  ) ON CONFLICT(system_id) DO UPDATE SET
    economic_observations=economic_observations+excluded.economic_observations,
    candidates=candidates+excluded.candidates,
    controls=controls+excluded.controls,
    open_economic=open_economic+excluded.open_economic;
END;
-- R175: /api/research/system-evidence reports raw ledger coverage, which is deliberately broader
-- than the economically-complete subset above. Keep those four missing raw counters alongside the
-- existing exact control count; terminal payoff updates do not change raw outcome_status truth.
CREATE TRIGGER IF NOT EXISTS research_system_collection_raw_totals_insert
AFTER INSERT ON research_system_observations
BEGIN
  INSERT INTO research_system_collection_totals(
    system_id,raw_observations,raw_candidates,raw_negative,raw_open_envelopes
  ) VALUES(
    NEW.system_id,1,NEW.candidate,
    CASE WHEN NEW.observation_kind='negative' THEN 1 ELSE 0 END,
    CASE WHEN NEW.outcome_status='open' THEN 1 ELSE 0 END
  ) ON CONFLICT(system_id) DO UPDATE SET
    raw_observations=raw_observations+1,
    raw_candidates=raw_candidates+excluded.raw_candidates,
    raw_negative=raw_negative+excluded.raw_negative,
    raw_open_envelopes=raw_open_envelopes+excluded.raw_open_envelopes;
END;
CREATE TRIGGER IF NOT EXISTS research_system_observations_no_update BEFORE UPDATE ON research_system_observations BEGIN SELECT RAISE(ABORT,'append-only research system observation'); END;
CREATE TRIGGER IF NOT EXISTS research_system_observations_no_delete BEFORE DELETE ON research_system_observations BEGIN SELECT RAISE(ABORT,'append-only research system observation'); END;

CREATE TABLE IF NOT EXISTS research_system_payoff_updates (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observation_id        INTEGER NOT NULL,
    observed_ts           TEXT NOT NULL,
    status                TEXT NOT NULL CHECK(status IN ('settled','voided','censored')),
    payout_lower          REAL NOT NULL,
    payout_upper          REAL NOT NULL,
    realized_net          REAL,
    source_artifact       TEXT NOT NULL,
    source_hash           TEXT NOT NULL,
    reason                TEXT NOT NULL DEFAULT '',
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(observation_id,status,source_hash),
    FOREIGN KEY(observation_id) REFERENCES research_system_observations(id)
);
CREATE INDEX IF NOT EXISTS idx_rsystem_payoff_observation ON research_system_payoff_updates(observation_id,id);
CREATE INDEX IF NOT EXISTS idx_rsystem_payoff_observed ON research_system_payoff_updates(observed_ts,id);
-- The observation ledger is append-only, so terminal state arrives here rather than by updating
-- observation.outcome_status. Decrement the materialized open counter only for the first terminal
-- receipt for an economically complete observation.
CREATE TRIGGER IF NOT EXISTS research_system_collection_totals_terminal
AFTER INSERT ON research_system_payoff_updates
WHEN NOT EXISTS(
  SELECT 1 FROM research_system_payoff_updates prior
  WHERE prior.observation_id=NEW.observation_id AND prior.id<NEW.id
)
BEGIN
  UPDATE research_system_collection_totals
  SET open_economic=CASE WHEN open_economic>0 THEN open_economic-1 ELSE 0 END
  WHERE system_id=(SELECT system_id FROM research_system_observations WHERE id=NEW.observation_id)
    AND EXISTS(
      SELECT 1 FROM research_system_observations o WHERE o.id=NEW.observation_id
        AND o.outcome_status='open' AND o.route!='observer' AND TRIM(o.source_clock_id)!=''
        AND TRIM(o.book_source)!='' AND TRIM(o.fee_source)!='' AND o.size_units>0
        AND o.executable_cost>0 AND o.latency_known=1 AND o.quote_age_known=1
        AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1
    );
END;
CREATE TRIGGER IF NOT EXISTS research_system_payoff_updates_no_update BEFORE UPDATE ON research_system_payoff_updates BEGIN SELECT RAISE(ABORT,'append-only payoff update'); END;
CREATE TRIGGER IF NOT EXISTS research_system_payoff_updates_no_delete BEFORE DELETE ON research_system_payoff_updates BEGIN SELECT RAISE(ABORT,'append-only payoff update'); END;

-- R139 STEP 3: one immutable, deterministic inference receipt per exact input manifest.  The
-- pipeline is research-only: even a lower-bound/multiplicity pass has no candidate, funding,
-- Paper, or LIVE authority.  Every receipt must have exactly one child result for each of the 19
-- immutable R138 experiments; application tests enforce that coverage before commit.
CREATE TABLE IF NOT EXISTS research_inference_preregistrations (
    preregistration_id        TEXT PRIMARY KEY,
    created_ts                TEXT NOT NULL,
    system_id                 TEXT NOT NULL,
    experiment_version        INTEGER NOT NULL CHECK(experiment_version>0),
    experiment_spec_hash      TEXT NOT NULL,
    cohort                    TEXT NOT NULL DEFAULT '',
    venue                     TEXT NOT NULL,
    route                     TEXT NOT NULL CHECK(route IN ('taker','maker','maker-control','rfq')),
    train_validation_cutoff_ts TEXT NOT NULL,
    untouched_start_ts        TEXT NOT NULL,
    untouched_end_ts          TEXT NOT NULL,
    seal_at_ts                TEXT NOT NULL,
    embargo_seconds           REAL NOT NULL CHECK(embargo_seconds>=0),
    required_days             INTEGER NOT NULL CHECK(required_days>=2),
    required_events           INTEGER NOT NULL CHECK(required_events>=2),
    pipeline_version          INTEGER NOT NULL CHECK(pipeline_version>0),
    code_manifest_hash        TEXT NOT NULL,
    data_manifest_hash        TEXT NOT NULL,
    source_manifest_hash      TEXT NOT NULL,
    inference_contract_hash   TEXT NOT NULL,
    frozen_input_manifest_hash TEXT NOT NULL,
    spec_hash                 TEXT NOT NULL UNIQUE,
    funded                    INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority           INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority            INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    FOREIGN KEY(system_id,experiment_version)
        REFERENCES research_experiment_specs(experiment_id,version)
);
CREATE TRIGGER IF NOT EXISTS research_inference_preregistrations_no_update BEFORE UPDATE ON research_inference_preregistrations BEGIN SELECT RAISE(ABORT,'immutable inference preregistration'); END;
CREATE TRIGGER IF NOT EXISTS research_inference_preregistrations_no_delete BEFORE DELETE ON research_inference_preregistrations BEGIN SELECT RAISE(ABORT,'immutable inference preregistration'); END;

CREATE TABLE IF NOT EXISTS research_inference_runs (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    created_ts               TEXT NOT NULL,
    as_of_completed_ts       TEXT NOT NULL,
    pipeline_version         INTEGER NOT NULL CHECK(pipeline_version>0),
    input_manifest_hash      TEXT NOT NULL,
    input_manifest_json      TEXT NOT NULL CHECK(json_valid(input_manifest_json)),
    result_hash              TEXT NOT NULL,
    preregistration_id       TEXT,
    status                   TEXT NOT NULL CHECK(status IN ('complete','blocked_input_limit','sealed_preregistered_untouched')),
    observed_rows            INTEGER NOT NULL CHECK(observed_rows>=0),
    exact_terminal_rows      INTEGER NOT NULL CHECK(exact_terminal_rows>=0),
    excluded_open            INTEGER NOT NULL CHECK(excluded_open>=0),
    excluded_void            INTEGER NOT NULL CHECK(excluded_void>=0),
    excluded_censored        INTEGER NOT NULL CHECK(excluded_censored>=0),
    excluded_nonexact        INTEGER NOT NULL CHECK(excluded_nonexact>=0),
    excluded_identity        INTEGER NOT NULL CHECK(excluded_identity>=0),
    excluded_route_truth     INTEGER NOT NULL CHECK(excluded_route_truth>=0),
    systems_reported         INTEGER NOT NULL CHECK(systems_reported=19),
    funded                   INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority          INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority           INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(pipeline_version,input_manifest_hash),
    FOREIGN KEY(preregistration_id) REFERENCES research_inference_preregistrations(preregistration_id)
);
CREATE INDEX IF NOT EXISTS idx_rinference_latest ON research_inference_runs(id DESC);
CREATE UNIQUE INDEX IF NOT EXISTS idx_rinference_prereg_once ON research_inference_runs(preregistration_id) WHERE preregistration_id IS NOT NULL;
CREATE TRIGGER IF NOT EXISTS research_inference_runs_no_update BEFORE UPDATE ON research_inference_runs BEGIN SELECT RAISE(ABORT,'append-only inference run'); END;
CREATE TRIGGER IF NOT EXISTS research_inference_runs_no_delete BEFORE DELETE ON research_inference_runs BEGIN SELECT RAISE(ABORT,'append-only inference run'); END;

CREATE TABLE IF NOT EXISTS research_inference_system_results (
    run_id                   INTEGER NOT NULL,
    system_id                TEXT NOT NULL,
    experiment_version       INTEGER NOT NULL CHECK(experiment_version>0),
    state                    TEXT NOT NULL,
    reason                   TEXT NOT NULL,
    terminal_rows            INTEGER NOT NULL CHECK(terminal_rows>=0),
    train_events             INTEGER NOT NULL CHECK(train_events>=0),
    validation_events        INTEGER NOT NULL CHECK(validation_events>=0),
    monitoring_events        INTEGER NOT NULL CHECK(monitoring_events>=0),
    purged_events            INTEGER NOT NULL CHECK(purged_events>=0),
    validation_days          INTEGER NOT NULL CHECK(validation_days>=0),
    validation_mean          REAL NOT NULL,
    validation_lower         REAL NOT NULL,
    validation_upper         REAL NOT NULL,
    validation_bounds_known  INTEGER NOT NULL CHECK(validation_bounds_known IN (0,1)),
    monitoring_days          INTEGER NOT NULL CHECK(monitoring_days>=0),
    monitoring_mean          REAL NOT NULL,
    monitoring_lower         REAL NOT NULL,
    monitoring_upper         REAL NOT NULL,
    monitoring_bounds_known  INTEGER NOT NULL CHECK(monitoring_bounds_known IN (0,1)),
    raw_p                    REAL NOT NULL CHECK(raw_p>=0 AND raw_p<=1),
    holm_p                   REAL NOT NULL CHECK(holm_p>=0 AND holm_p<=1),
    by_q                     REAL NOT NULL CHECK(by_q>=0 AND by_q<=1),
    monitoring_lower_bound_positive INTEGER NOT NULL CHECK(monitoring_lower_bound_positive IN (0,1)),
    untouched_events          INTEGER NOT NULL DEFAULT 0 CHECK(untouched_events>=0),
    untouched_days            INTEGER NOT NULL DEFAULT 0 CHECK(untouched_days>=0),
    untouched_mean            REAL NOT NULL DEFAULT 0,
    untouched_lower           REAL NOT NULL DEFAULT 0,
    untouched_upper           REAL NOT NULL DEFAULT 0,
    untouched_p               REAL NOT NULL DEFAULT 1 CHECK(untouched_p>=0 AND untouched_p<=1),
    untouched_bounds_known    INTEGER NOT NULL DEFAULT 0 CHECK(untouched_bounds_known IN (0,1)),
    preregistered_untouched_gate_pass INTEGER NOT NULL DEFAULT 0 CHECK(preregistered_untouched_gate_pass IN (0,1)),
    cells_json               TEXT NOT NULL CHECK(json_valid(cells_json)),
    execution_candidate      INTEGER NOT NULL DEFAULT 0 CHECK(execution_candidate IN (0,1)),
    funded                   INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority          INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority           INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    PRIMARY KEY(run_id,system_id),
    FOREIGN KEY(run_id) REFERENCES research_inference_runs(id)
);
CREATE INDEX IF NOT EXISTS idx_rinference_system ON research_inference_system_results(system_id,run_id DESC);
CREATE TRIGGER IF NOT EXISTS research_inference_system_results_require_prereg_gate BEFORE INSERT ON research_inference_system_results
WHEN NEW.preregistered_untouched_gate_pass=1 OR NEW.execution_candidate=1
BEGIN
  SELECT CASE WHEN NEW.preregistered_untouched_gate_pass!=1 OR NEW.execution_candidate!=1 OR
    NEW.state!='PREREGISTERED_UNTOUCHED_PASS' OR NEW.untouched_bounds_known!=1 OR
    NEW.untouched_lower<=0 OR NEW.untouched_p>0.05 OR NOT EXISTS(
      SELECT 1 FROM research_inference_runs r JOIN research_inference_preregistrations p
        ON p.preregistration_id=r.preregistration_id
      WHERE r.id=NEW.run_id AND r.status='sealed_preregistered_untouched'
       AND p.system_id=NEW.system_id AND p.experiment_version=NEW.experiment_version
       AND NEW.untouched_days>=p.required_days AND NEW.untouched_events>=p.required_events
       AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
    ) THEN RAISE(ABORT,'inference candidate lacks immutable preregistered untouched contract') END;
END;
CREATE TRIGGER IF NOT EXISTS research_inference_system_results_no_update BEFORE UPDATE ON research_inference_system_results BEGIN SELECT RAISE(ABORT,'append-only inference system result'); END;
CREATE TRIGGER IF NOT EXISTS research_inference_system_results_no_delete BEFORE DELETE ON research_inference_system_results BEGIN SELECT RAISE(ABORT,'append-only inference system result'); END;

-- Selected Kalshi WS books only.  The in-memory book remains complete; this bounded research lane
-- stores the exported ten levels, the actual tick, exact route fees, and authoritative aggressor
-- flow.  It is deliberately not a second subscription or REST poller.
CREATE TABLE IF NOT EXISTS research_microstructure_frames (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts           TEXT NOT NULL,
    received_ts           TEXT NOT NULL,
    source_channel        TEXT NOT NULL,
    source_generation     INTEGER NOT NULL CHECK(source_generation>0),
    source_subscription_id INTEGER NOT NULL CHECK(source_subscription_id>0),
    source_sequence       INTEGER NOT NULL CHECK(source_sequence>0),
    ticker                TEXT NOT NULL,
    canonical_event_id    TEXT NOT NULL DEFAULT '',
    market_status         TEXT NOT NULL DEFAULT '',
    close_ts              TEXT NOT NULL DEFAULT '',
    book_source           TEXT NOT NULL,
    quote_age_s           REAL NOT NULL CHECK(quote_age_s>=0),
    tick_size             REAL NOT NULL CHECK(tick_size>0),
    yes_bid               REAL NOT NULL CHECK(yes_bid>0 AND yes_bid<1),
    yes_ask               REAL NOT NULL CHECK(yes_ask>yes_bid AND yes_ask<1),
    yes_bid_depth         REAL NOT NULL CHECK(yes_bid_depth>=0),
    yes_ask_depth         REAL NOT NULL CHECK(yes_ask_depth>=0),
    bid_levels_json       TEXT NOT NULL CHECK(json_valid(bid_levels_json)),
    ask_levels_json       TEXT NOT NULL CHECK(json_valid(ask_levels_json)),
    book_hash             TEXT NOT NULL,
    prior_book_hash       TEXT NOT NULL DEFAULT '',
    transition_class      TEXT NOT NULL DEFAULT 'baseline'
                              CHECK(transition_class IN ('baseline','unchanged','bid_depletion','ask_depletion','bid_refill','ask_refill','mixed')),
    signed_flow_10s       REAL NOT NULL DEFAULT 0,
    flow_units_10s        REAL NOT NULL DEFAULT 0 CHECK(flow_units_10s>=0),
    signed_flow_60s       REAL NOT NULL DEFAULT 0,
    flow_units_60s        REAL NOT NULL DEFAULT 0 CHECK(flow_units_60s>=0),
    yes_taker_fee_1       REAL NOT NULL,
    no_taker_fee_1        REAL NOT NULL,
    yes_maker_fee_1       REAL NOT NULL,
    no_maker_fee_1        REAL NOT NULL,
    fee_source            TEXT NOT NULL,
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(ticker,observed_ts,book_hash)
);
CREATE INDEX IF NOT EXISTS idx_rmicro_ticker ON research_microstructure_frames(ticker,id DESC);
CREATE INDEX IF NOT EXISTS idx_rmicro_transition ON research_microstructure_frames(transition_class,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_microstructure_frames_no_update BEFORE UPDATE ON research_microstructure_frames BEGIN SELECT RAISE(ABORT,'append-only microstructure frame'); END;
CREATE TRIGGER IF NOT EXISTS research_microstructure_frames_no_delete BEFORE DELETE ON research_microstructure_frames BEGIN SELECT RAISE(ABORT,'append-only microstructure frame'); END;

CREATE TABLE IF NOT EXISTS research_microstructure_horizons (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    frame_id              INTEGER NOT NULL,
    horizon_s             INTEGER NOT NULL CHECK(horizon_s>0),
    captured_ts           TEXT NOT NULL,
    status                TEXT NOT NULL CHECK(status IN ('captured','missed')),
    yes_bid               REAL,
    yes_ask               REAL,
    no_bid                REAL,
    no_ask                REAL,
    yes_exit_fee_1        REAL,
    no_exit_fee_1         REAL,
    quote_age_s           REAL,
    reason                TEXT NOT NULL DEFAULT '',
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(frame_id,horizon_s),
    FOREIGN KEY(frame_id) REFERENCES research_microstructure_frames(id)
);
CREATE INDEX IF NOT EXISTS idx_rmicro_horizon_due ON research_microstructure_horizons(frame_id,horizon_s);
CREATE TRIGGER IF NOT EXISTS research_microstructure_horizons_no_update BEFORE UPDATE ON research_microstructure_horizons BEGIN SELECT RAISE(ABORT,'append-only microstructure horizon'); END;
CREATE TRIGGER IF NOT EXISTS research_microstructure_horizons_no_delete BEFORE DELETE ON research_microstructure_horizons BEGIN SELECT RAISE(ABORT,'append-only microstructure horizon'); END;

-- Every authoritative outcome-set fetch produces a frame, including unchanged controls.  A
-- membership change is never itself a trade: the row keeps completeness, void, book and source
-- blockers explicit so redistribution can be studied without title inference.
CREATE TABLE IF NOT EXISTS research_outcome_set_frames (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts           TEXT NOT NULL,
    observed_slot         TEXT NOT NULL,
    venue                 TEXT NOT NULL,
    event_id              TEXT NOT NULL,
    title                 TEXT NOT NULL DEFAULT '',
    source_updated_ts     TEXT NOT NULL DEFAULT '',
    source_artifact       TEXT NOT NULL,
    membership_hash       TEXT NOT NULL,
    prior_membership_hash TEXT NOT NULL DEFAULT '',
    members_json          TEXT NOT NULL CHECK(json_valid(members_json)),
    added_json            TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(added_json)),
    removed_json          TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(removed_json)),
    changed_json          TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(changed_json)),
    change_class          TEXT NOT NULL CHECK(change_class IN ('baseline','unchanged','expanded','contracted','status_change','mixed')),
    mutually_exclusive    INTEGER NOT NULL DEFAULT 0 CHECK(mutually_exclusive IN (0,1)),
    exhaustive            INTEGER NOT NULL DEFAULT 0 CHECK(exhaustive IN (0,1)),
    void_policy_verified  INTEGER NOT NULL DEFAULT 0 CHECK(void_policy_verified IN (0,1)),
    executable_members    INTEGER NOT NULL DEFAULT 0 CHECK(executable_members>=0),
    total_members         INTEGER NOT NULL DEFAULT 0 CHECK(total_members>=0),
    blocker               TEXT NOT NULL DEFAULT '',
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(venue,event_id,observed_slot)
);
CREATE INDEX IF NOT EXISTS idx_routcome_event ON research_outcome_set_frames(venue,event_id,id DESC);
CREATE INDEX IF NOT EXISTS idx_routcome_change ON research_outcome_set_frames(change_class,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_outcome_set_frames_no_update BEFORE UPDATE ON research_outcome_set_frames BEGIN SELECT RAISE(ABORT,'append-only outcome-set frame'); END;
CREATE TRIGGER IF NOT EXISTS research_outcome_set_frames_no_delete BEFORE DELETE ON research_outcome_set_frames BEGIN SELECT RAISE(ABORT,'append-only outcome-set frame'); END;

-- Source adapter receipts are immutable even when an adapter is intentionally BLOCKED.  This is
-- what prevents a market-derived forecast, arrival-only option summary, or point forecast from
-- being silently promoted to an independent exact-clock release.
CREATE TABLE IF NOT EXISTS research_official_release_frames (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts           TEXT NOT NULL,
    source_id             TEXT NOT NULL,
    artifact_id           TEXT NOT NULL,
    source_ts             TEXT NOT NULL DEFAULT '',
    valid_ts              TEXT NOT NULL DEFAULT '',
    schema_version        TEXT NOT NULL,
    clock_status          TEXT NOT NULL CHECK(clock_status IN ('exact','arrival_only','blocked')),
    artifact_hash         TEXT NOT NULL,
    values_json           TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(values_json)),
    settlement_compatible INTEGER NOT NULL DEFAULT 0 CHECK(settlement_compatible IN (0,1)),
    blocker               TEXT NOT NULL DEFAULT '',
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(source_id,artifact_id,artifact_hash)
);
CREATE INDEX IF NOT EXISTS idx_rofficial_source ON research_official_release_frames(source_id,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_official_release_frames_no_update BEFORE UPDATE ON research_official_release_frames BEGIN SELECT RAISE(ABORT,'append-only official release frame'); END;
CREATE TRIGGER IF NOT EXISTS research_official_release_frames_no_delete BEFORE DELETE ON research_official_release_frames BEGIN SELECT RAISE(ABORT,'append-only official release frame'); END;

-- R138 system-contract coverage: a registry entry is not implementation.  Every one of the 19
-- systems receives a periodic runtime state naming its concrete collectors/evidence, or a durable
-- BLOCKED reason and exact prerequisites.  Authority remains structurally impossible.
CREATE TABLE IF NOT EXISTS research_system_runtime_receipts (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts           TEXT NOT NULL,
    observed_slot         TEXT NOT NULL,
    system_id             TEXT NOT NULL,
    state                 TEXT NOT NULL CHECK(state IN ('COLLECTING','COLLECTING_PARTIAL','BLOCKED')),
    collector_ids_json    TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(collector_ids_json)),
    evidence_tables_json  TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(evidence_tables_json)),
    reason                TEXT NOT NULL,
    prerequisites_json    TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(prerequisites_json)),
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(system_id,observed_slot)
);
CREATE INDEX IF NOT EXISTS idx_rsystem_runtime_latest ON research_system_runtime_receipts(system_id,id DESC);
CREATE INDEX IF NOT EXISTS idx_rsystem_runtime_state ON research_system_runtime_receipts(state,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_system_runtime_receipts_no_update BEFORE UPDATE ON research_system_runtime_receipts BEGIN SELECT RAISE(ABORT,'append-only system runtime receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_system_runtime_receipts_no_delete BEFORE DELETE ON research_system_runtime_receipts BEGIN SELECT RAISE(ABORT,'append-only system runtime receipt'); END;

-- R138 UNIFIED ROUTE LEDGER.  One immutable row is one detector/route alternative at one
-- point-in-time executable book.  Rejected, abstained, and control alternatives are retained;
-- lifecycle changes append to research_route_events instead of rewriting the quote decision.
-- Research authority is denied by schema so this ledger can never become an order permission.
CREATE TABLE IF NOT EXISTS research_route_opportunities (
    opportunity_id       TEXT NOT NULL,
    route_id             TEXT NOT NULL,
    observed_ts          TEXT NOT NULL,
    decision_ts          TEXT NOT NULL,
    experiment_id        TEXT NOT NULL DEFAULT '',
    experiment_version   INTEGER NOT NULL DEFAULT 0,
    system_name          TEXT NOT NULL,
    canonical_event_id   TEXT NOT NULL DEFAULT '',
    canonical_payoff_id  TEXT NOT NULL DEFAULT '',
    identity_status      TEXT NOT NULL DEFAULT 'unverified'
                             CHECK(identity_status IN ('verified','structural','unverified','rejected')),
    venue                TEXT NOT NULL CHECK(venue IN ('kalshi','polyus','polymarket','multi','none')),
    ticker               TEXT NOT NULL DEFAULT '',
    side                 TEXT NOT NULL CHECK(side IN ('YES','NO','MULTI','NONE')),
    route                TEXT NOT NULL CHECK(route IN ('maker','taker','rfq','combo','lock','abstain','control')),
    action               TEXT NOT NULL CHECK(action IN ('buy','sell','post','quote','abstain','reject','control')),
    quote_source         TEXT NOT NULL,
    quote_sequence       TEXT NOT NULL DEFAULT '',
    quote_age_s          REAL NOT NULL CHECK(quote_age_s>=0),
    quote_age_known      INTEGER NOT NULL DEFAULT 0 CHECK(quote_age_known IN (0,1)),
    decision_latency_ms  REAL NOT NULL CHECK(decision_latency_ms>=0),
    latency_known        INTEGER NOT NULL DEFAULT 0 CHECK(latency_known IN (0,1)),
    tick_size            REAL NOT NULL CHECK(tick_size>=0),
    tick_known           INTEGER NOT NULL DEFAULT 0 CHECK(tick_known IN (0,1)),
    executable_price     REAL NOT NULL CHECK(executable_price>=0 AND executable_price<=1),
    executable_depth     REAL NOT NULL CHECK(executable_depth>=0),
    depth_known          INTEGER NOT NULL DEFAULT 0 CHECK(depth_known IN (0,1)),
    requested_qty        REAL NOT NULL CHECK(requested_qty>=0),
    fee_amount           REAL NOT NULL CHECK(fee_amount>=0),
    rebate_amount        REAL NOT NULL CHECK(rebate_amount>=0),
    fee_authority        TEXT NOT NULL,
    fee_known            INTEGER NOT NULL DEFAULT 0 CHECK(fee_known IN (0,1)),
    expected_payout_low  REAL NOT NULL,
    expected_payout_high REAL NOT NULL,
    expected_net_low     REAL NOT NULL,
    expected_net_high    REAL NOT NULL,
    partial_fill_worst   REAL NOT NULL DEFAULT 0,
    capital_seconds      REAL NOT NULL DEFAULT 0 CHECK(capital_seconds>=0),
    decision             TEXT NOT NULL CHECK(decision IN ('candidate','blocked','abstain','control')),
    decision_reason      TEXT NOT NULL,
    alternative_group    TEXT NOT NULL DEFAULT '',
    evidence_json        TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(evidence_json)),
    funded               INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority      INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority       INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    PRIMARY KEY(opportunity_id,route_id),
    CHECK(expected_payout_low<=expected_payout_high),
    CHECK(expected_net_low<=expected_net_high),
    CHECK((action IN ('abstain','reject','control')) OR
          (ticker!='' AND side!='NONE' AND executable_price>0 AND executable_depth>0 AND requested_qty>0)),
    CHECK((decision='candidate' AND decision_reason!='') OR decision!='candidate')
);
CREATE INDEX IF NOT EXISTS idx_rroute_system ON research_route_opportunities(system_name,observed_ts DESC);
CREATE INDEX IF NOT EXISTS idx_rroute_decision ON research_route_opportunities(decision,observed_ts DESC);
CREATE INDEX IF NOT EXISTS idx_rroute_instrument ON research_route_opportunities(venue,ticker,side,observed_ts DESC);
-- The identity-lock funnel asks for the newest cross-venue route regardless of family spelling.
-- Its contains predicate cannot use idx_rroute_system, so walk newest-first and stop at the bound
-- instead of sorting/scanning the entire append-only route ledger every five minutes.
CREATE INDEX IF NOT EXISTS idx_rroute_observed ON research_route_opportunities(observed_ts DESC,system_name);
-- R175: the portfolio report orders by this exact tuple. The prior observed/system index still
-- needed a full temp sort of the append-only ledger before applying LIMIT.
CREATE INDEX IF NOT EXISTS idx_rroute_report_recent
    ON research_route_opportunities(observed_ts DESC,opportunity_id,route_id);
-- R143: every common-system observation stamps its immutable observation id in route evidence.
-- Indexing that expression turns the terminal-grade join into point lookups instead of a full
-- scan of the unified route ledger.  JSON remains immutable and auditable; this is only access.
CREATE INDEX IF NOT EXISTS idx_rroute_system_observation
    ON research_route_opportunities(CAST(json_extract(evidence_json,'$.system_observation_id') AS INTEGER),route,decision);
CREATE TRIGGER IF NOT EXISTS research_route_opportunities_no_update BEFORE UPDATE ON research_route_opportunities BEGIN SELECT RAISE(ABORT,'immutable research route opportunity'); END;
CREATE TRIGGER IF NOT EXISTS research_route_opportunities_no_delete BEFORE DELETE ON research_route_opportunities BEGIN SELECT RAISE(ABORT,'immutable research route opportunity'); END;

CREATE TABLE IF NOT EXISTS research_route_events (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    opportunity_id        TEXT NOT NULL,
    route_id              TEXT NOT NULL,
    observed_ts           TEXT NOT NULL,
    event_type            TEXT NOT NULL
                              CHECK(event_type IN ('intent','refused','posted','queue','partial_fill','fill','cancel','expire','unwind','settlement','grade','note')),
    quantity              REAL NOT NULL DEFAULT 0 CHECK(quantity>=0),
    price                 REAL NOT NULL DEFAULT 0 CHECK(price>=0 AND price<=1),
    fee_amount            REAL NOT NULL DEFAULT 0 CHECK(fee_amount>=0),
    rebate_amount         REAL NOT NULL DEFAULT 0 CHECK(rebate_amount>=0),
    queue_ahead           REAL CHECK(queue_ahead IS NULL OR queue_ahead>=0),
    order_ref_hash        TEXT NOT NULL DEFAULT '',
    payout                REAL,
    realized_net          REAL,
    capital_seconds       REAL NOT NULL DEFAULT 0 CHECK(capital_seconds>=0),
    markout               REAL,
    outcome_status        TEXT NOT NULL DEFAULT '',
    reason                TEXT NOT NULL DEFAULT '',
    evidence_json         TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(evidence_json)),
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    FOREIGN KEY(opportunity_id,route_id) REFERENCES research_route_opportunities(opportunity_id,route_id)
);
CREATE INDEX IF NOT EXISTS idx_rroute_event_route ON research_route_events(opportunity_id,route_id,id);
CREATE INDEX IF NOT EXISTS idx_rroute_event_type ON research_route_events(event_type,observed_ts DESC);
CREATE INDEX IF NOT EXISTS idx_rroute_event_route_type ON research_route_events(opportunity_id,route_id,event_type);
CREATE TRIGGER IF NOT EXISTS research_route_events_no_update BEFORE UPDATE ON research_route_events BEGIN SELECT RAISE(ABORT,'append-only research route event'); END;
CREATE TRIGGER IF NOT EXISTS research_route_events_no_delete BEFORE DELETE ON research_route_events BEGIN SELECT RAISE(ABORT,'append-only research route event'); END;

-- R175: /api/research/portfolio is a live status read, never a reason to GROUP BY or aggregate the
-- complete route/event histories. These tiny tables are exact insert-maintained mirrors. Their
-- one-time deterministic backfill is latched in r175_report_rollups.go.
CREATE TABLE IF NOT EXISTS research_route_report_totals (
    singleton                       INTEGER PRIMARY KEY CHECK(singleton=1),
    routes                          INTEGER NOT NULL DEFAULT 0 CHECK(routes>=0),
    candidates                      INTEGER NOT NULL DEFAULT 0 CHECK(candidates>=0),
    blocked                         INTEGER NOT NULL DEFAULT 0 CHECK(blocked>=0),
    abstains                        INTEGER NOT NULL DEFAULT 0 CHECK(abstains>=0),
    controls                        INTEGER NOT NULL DEFAULT 0 CHECK(controls>=0),
    fee_authority_rows              INTEGER NOT NULL DEFAULT 0 CHECK(fee_authority_rows>=0),
    nonpositive_lower_bound         INTEGER NOT NULL DEFAULT 0 CHECK(nonpositive_lower_bound>=0),
    identity_unverified_or_rejected INTEGER NOT NULL DEFAULT 0 CHECK(identity_unverified_or_rejected>=0),
    complete_money_truth            INTEGER NOT NULL DEFAULT 0 CHECK(complete_money_truth>=0),
    lifecycle_events                INTEGER NOT NULL DEFAULT 0 CHECK(lifecycle_events>=0),
    fill_events                     INTEGER NOT NULL DEFAULT 0 CHECK(fill_events>=0),
    cancel_events                   INTEGER NOT NULL DEFAULT 0 CHECK(cancel_events>=0),
    grade_events                    INTEGER NOT NULL DEFAULT 0 CHECK(grade_events>=0)
);
CREATE TABLE IF NOT EXISTS research_route_decision_totals (
    route       TEXT NOT NULL,
    decision    TEXT NOT NULL,
    route_count INTEGER NOT NULL DEFAULT 0 CHECK(route_count>=0),
    PRIMARY KEY(route,decision)
);
CREATE TRIGGER IF NOT EXISTS research_route_report_opportunity_insert
AFTER INSERT ON research_route_opportunities
BEGIN
  INSERT INTO research_route_report_totals(
    singleton,routes,candidates,blocked,abstains,controls,fee_authority_rows,
    nonpositive_lower_bound,identity_unverified_or_rejected,complete_money_truth
  ) VALUES(
    1,1,NEW.decision='candidate',NEW.decision='blocked',NEW.decision='abstain',
    NEW.decision='control',NEW.fee_authority!='',NEW.expected_net_low<=0,
    NEW.identity_status NOT IN ('verified','structural'),
    NEW.quote_age_known=1 AND NEW.latency_known=1 AND NEW.tick_known=1
      AND NEW.depth_known=1 AND NEW.fee_known=1
  ) ON CONFLICT(singleton) DO UPDATE SET
    routes=routes+1,
    candidates=candidates+excluded.candidates,
    blocked=blocked+excluded.blocked,
    abstains=abstains+excluded.abstains,
    controls=controls+excluded.controls,
    fee_authority_rows=fee_authority_rows+excluded.fee_authority_rows,
    nonpositive_lower_bound=nonpositive_lower_bound+excluded.nonpositive_lower_bound,
    identity_unverified_or_rejected=identity_unverified_or_rejected+excluded.identity_unverified_or_rejected,
    complete_money_truth=complete_money_truth+excluded.complete_money_truth;
  INSERT INTO research_route_decision_totals(route,decision,route_count)
  VALUES(NEW.route,NEW.decision,1)
  ON CONFLICT(route,decision) DO UPDATE SET route_count=route_count+1;
END;
-- The public ledger is append-only. The only delete path is the bounded internal removal of
-- redundant Step-7 observer mirrors; this trigger keeps the compact report exact during that path.
CREATE TRIGGER IF NOT EXISTS research_route_report_opportunity_delete
AFTER DELETE ON research_route_opportunities
BEGIN
  UPDATE research_route_report_totals SET
    routes=routes-1,
    candidates=candidates-(OLD.decision='candidate'),
    blocked=blocked-(OLD.decision='blocked'),
    abstains=abstains-(OLD.decision='abstain'),
    controls=controls-(OLD.decision='control'),
    fee_authority_rows=fee_authority_rows-(OLD.fee_authority!=''),
    nonpositive_lower_bound=nonpositive_lower_bound-(OLD.expected_net_low<=0),
    identity_unverified_or_rejected=identity_unverified_or_rejected-
      (OLD.identity_status NOT IN ('verified','structural')),
    complete_money_truth=complete_money_truth-
      (OLD.quote_age_known=1 AND OLD.latency_known=1 AND OLD.tick_known=1
       AND OLD.depth_known=1 AND OLD.fee_known=1)
  WHERE singleton=1;
  UPDATE research_route_decision_totals
  SET route_count=route_count-1 WHERE route=OLD.route AND decision=OLD.decision;
  DELETE FROM research_route_decision_totals
  WHERE route=OLD.route AND decision=OLD.decision AND route_count=0;
END;
CREATE TRIGGER IF NOT EXISTS research_route_report_event_insert
AFTER INSERT ON research_route_events
BEGIN
  INSERT INTO research_route_report_totals(
    singleton,lifecycle_events,fill_events,cancel_events,grade_events
  ) VALUES(
    1,1,NEW.event_type IN ('fill','partial_fill'),NEW.event_type='cancel',NEW.event_type='grade'
  ) ON CONFLICT(singleton) DO UPDATE SET
    lifecycle_events=lifecycle_events+1,
    fill_events=fill_events+excluded.fill_events,
    cancel_events=cancel_events+excluded.cancel_events,
    grade_events=grade_events+excluded.grade_events;
END;
-- Quarantine cleanup can temporarily drop the public no-delete guard. Its exact historical
-- removal must also remove the corresponding lifecycle count in the same transaction.
CREATE TRIGGER IF NOT EXISTS research_route_report_event_delete
AFTER DELETE ON research_route_events
BEGIN
  UPDATE research_route_report_totals SET
    lifecycle_events=lifecycle_events-1,
    fill_events=fill_events-(OLD.event_type IN ('fill','partial_fill')),
    cancel_events=cancel_events-(OLD.event_type='cancel'),
    grade_events=grade_events-(OLD.event_type='grade')
  WHERE singleton=1;
END;

-- R138 COMPLIANCE/SECURITY RECEIPTS. These are append-only policy observations. They contain no
-- secrets or raw credentials and cannot grant trading authority.
CREATE TABLE IF NOT EXISTS research_security_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts      TEXT NOT NULL,
    category         TEXT NOT NULL CHECK(category IN ('policy_check','forbidden_attempt','credential_scope','rate_limit','recovery','authority_check')),
    severity         TEXT NOT NULL CHECK(severity IN ('info','attention','blocked','critical')),
    message          TEXT NOT NULL,
    evidence_json    TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(evidence_json)),
    funded           INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority  INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority   INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)
);
CREATE INDEX IF NOT EXISTS idx_rsecurity_latest ON research_security_events(category,id DESC);
CREATE TRIGGER IF NOT EXISTS research_security_events_no_update BEFORE UPDATE ON research_security_events BEGIN SELECT RAISE(ABORT,'append-only research security event'); END;
CREATE TRIGGER IF NOT EXISTS research_security_events_no_delete BEFORE DELETE ON research_security_events BEGIN SELECT RAISE(ABORT,'append-only research security event'); END;

-- R139 STEP 10: immutable expected-value-of-information and portfolio-cause inputs.
-- These tables contain operator/researcher-supplied assumptions and replicated route lower
-- bounds.  They are deliberately separate from collector row counts and trading configuration:
-- no receipt here can start a collector, change a budget, size a position, or authorize an order.
-- A semantic change is a new version; UPDATE/DELETE are forbidden.
CREATE TABLE IF NOT EXISTS research_sealed_route_economics (
    receipt_id                       TEXT PRIMARY KEY,
    sealed_inference_run_id          INTEGER NOT NULL,
    sealed_result_hash               TEXT NOT NULL CHECK(length(sealed_result_hash)=64),
    system_id                        TEXT NOT NULL,
    route_id                         TEXT NOT NULL,
    venue                            TEXT NOT NULL,
    net_per_day_lower                REAL NOT NULL,
    capacity                         REAL NOT NULL CHECK(capacity>=0),
    capital_dollar_hours_per_day     REAL NOT NULL CHECK(capital_dollar_hours_per_day>=0),
    conversion_evidence_hash         TEXT NOT NULL CHECK(length(conversion_evidence_hash)=64),
    created_ts                       TEXT NOT NULL,
    funded                           INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority                  INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority                   INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    FOREIGN KEY(sealed_inference_run_id) REFERENCES research_inference_runs(id)
);
-- Current rolling inference can never satisfy this trigger: its run status is monitoring-only and
-- its preregistered gate/candidate columns are schema-pinned to zero. A future sealed pipeline must
-- introduce an explicit sealed run contract before it can write a route conversion receipt.
CREATE TRIGGER IF NOT EXISTS research_sealed_route_economics_require_gate BEFORE INSERT ON research_sealed_route_economics
WHEN NOT EXISTS (
    SELECT 1 FROM research_inference_runs r JOIN research_inference_system_results s
      ON s.run_id=r.id AND s.system_id=NEW.system_id
    WHERE r.id=NEW.sealed_inference_run_id
      AND (r.result_hash=NEW.sealed_result_hash OR r.result_hash='sha256:'||NEW.sealed_result_hash)
      AND r.status='sealed_preregistered_untouched'
      AND s.preregistered_untouched_gate_pass=1 AND s.execution_candidate=1
      AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
      AND s.funded=0 AND s.paper_authority=0 AND s.live_authority=0
) BEGIN SELECT RAISE(ABORT,'route economics lacks sealed preregistered inference gate'); END;
CREATE TRIGGER IF NOT EXISTS research_sealed_route_economics_no_update BEFORE UPDATE ON research_sealed_route_economics BEGIN SELECT RAISE(ABORT,'immutable sealed route economics'); END;
CREATE TRIGGER IF NOT EXISTS research_sealed_route_economics_no_delete BEFORE DELETE ON research_sealed_route_economics BEGIN SELECT RAISE(ABORT,'immutable sealed route economics'); END;

CREATE TABLE IF NOT EXISTS research_evi_input_specs (
    task_id                         TEXT NOT NULL,
    version                         INTEGER NOT NULL CHECK(version>0),
    spec_hash                       TEXT NOT NULL CHECK(length(spec_hash)=64),
    input_state                     TEXT NOT NULL CHECK(input_state IN ('active','retired')),
    system_id                       TEXT NOT NULL,
    route_id                        TEXT NOT NULL,
    cause_ids_json                  TEXT NOT NULL CHECK(json_valid(cause_ids_json)),
    provenance                      TEXT NOT NULL,
    evidence_hash                   TEXT NOT NULL CHECK(length(evidence_hash)=64),
    sealed_inference_run_id         INTEGER NOT NULL,
    evidence_observed_ts            TEXT NOT NULL,
    valid_until_ts                  TEXT NOT NULL,
    probability_changes_decision    REAL NOT NULL CHECK(probability_changes_decision>=0 AND probability_changes_decision<=1),
    decision_value_dollars_per_day  REAL NOT NULL CHECK(decision_value_dollars_per_day>=0),
    collection_cost_dollars         REAL NOT NULL CHECK(collection_cost_dollars>=0),
    compute_minutes                 REAL NOT NULL CHECK(compute_minutes>=0),
    api_calls                       REAL NOT NULL CHECK(api_calls>=0),
    compute_budget                  REAL NOT NULL CHECK(compute_budget>=0),
    api_budget                      REAL NOT NULL CHECK(api_budget>=0),
    compute_dollar_per_minute       REAL NOT NULL CHECK(compute_dollar_per_minute>=0),
    api_dollar_per_call             REAL NOT NULL CHECK(api_dollar_per_call>=0),
    created_ts                      TEXT NOT NULL,
    funded                          INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority                 INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority                  INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    PRIMARY KEY(task_id,version),
    FOREIGN KEY(sealed_inference_run_id) REFERENCES research_inference_runs(id)
);
CREATE INDEX IF NOT EXISTS idx_revi_input_latest ON research_evi_input_specs(task_id,version DESC);
CREATE TRIGGER IF NOT EXISTS research_evi_input_specs_no_update BEFORE UPDATE ON research_evi_input_specs BEGIN SELECT RAISE(ABORT,'immutable EVI input spec'); END;
CREATE TRIGGER IF NOT EXISTS research_evi_input_specs_no_delete BEFORE DELETE ON research_evi_input_specs BEGIN SELECT RAISE(ABORT,'immutable EVI input spec'); END;

CREATE TABLE IF NOT EXISTS research_cause_exposure_specs (
    exposure_id                     TEXT NOT NULL,
    version                         INTEGER NOT NULL CHECK(version>0),
    spec_hash                       TEXT NOT NULL CHECK(length(spec_hash)=64),
    input_state                     TEXT NOT NULL CHECK(input_state IN ('active','retired')),
    system_id                       TEXT NOT NULL,
    route_id                        TEXT NOT NULL,
    venue                           TEXT NOT NULL,
    cause_ids_json                  TEXT NOT NULL CHECK(json_valid(cause_ids_json)),
    provenance                      TEXT NOT NULL,
    evidence_hash                   TEXT NOT NULL CHECK(length(evidence_hash)=64),
    sealed_inference_run_id         INTEGER NOT NULL,
    sealed_route_economics_receipt_id TEXT NOT NULL,
    evidence_observed_ts            TEXT NOT NULL,
    valid_until_ts                  TEXT NOT NULL,
    replication_id                  TEXT NOT NULL,
    lower_bound_method              TEXT NOT NULL,
    replicated_untouched            INTEGER NOT NULL CHECK(replicated_untouched=1),
    executable_fee_net_lower_bound  INTEGER NOT NULL CHECK(executable_fee_net_lower_bound=1),
    net_per_day_lower               REAL NOT NULL,
    capacity                        REAL NOT NULL CHECK(capacity>=0),
    capital_dollar_hours_per_day    REAL NOT NULL CHECK(capital_dollar_hours_per_day>=0),
    created_ts                      TEXT NOT NULL,
    funded                          INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority                 INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority                  INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    PRIMARY KEY(exposure_id,version),
    FOREIGN KEY(sealed_inference_run_id) REFERENCES research_inference_runs(id),
    FOREIGN KEY(sealed_route_economics_receipt_id) REFERENCES research_sealed_route_economics(receipt_id)
);
CREATE INDEX IF NOT EXISTS idx_rcause_input_latest ON research_cause_exposure_specs(exposure_id,version DESC);
CREATE TRIGGER IF NOT EXISTS research_cause_exposure_specs_no_update BEFORE UPDATE ON research_cause_exposure_specs BEGIN SELECT RAISE(ABORT,'immutable cause exposure spec'); END;
CREATE TRIGGER IF NOT EXISTS research_cause_exposure_specs_no_delete BEFORE DELETE ON research_cause_exposure_specs BEGIN SELECT RAISE(ABORT,'immutable cause exposure spec'); END;

-- One deterministic receipt per exact active-input manifest.  Re-running the scheduler with the
-- same frozen inputs returns the same receipt; wall-clock timestamps are metadata, not inputs.
CREATE TABLE IF NOT EXISTS research_evi_schedule_runs (
    manifest_hash        TEXT PRIMARY KEY CHECK(length(manifest_hash)=64),
    observed_ts          TEXT NOT NULL,
    state                TEXT NOT NULL CHECK(state IN ('WAITING_FOR_FROZEN_INPUTS','BLOCKED_STALE_OR_INCONSISTENT_INPUTS','SCHEDULED')),
    input_hashes_json    TEXT NOT NULL CHECK(json_valid(input_hashes_json)),
    input_count          INTEGER NOT NULL CHECK(input_count>=0),
    valid_input_count    INTEGER NOT NULL CHECK(valid_input_count>=0),
    result_json          TEXT NOT NULL CHECK(json_valid(result_json)),
    reason               TEXT NOT NULL,
    funded               INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority      INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority       INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)
);
CREATE TRIGGER IF NOT EXISTS research_evi_schedule_runs_no_update BEFORE UPDATE ON research_evi_schedule_runs BEGIN SELECT RAISE(ABORT,'immutable EVI schedule receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_evi_schedule_runs_no_delete BEFORE DELETE ON research_evi_schedule_runs BEGIN SELECT RAISE(ABORT,'immutable EVI schedule receipt'); END;

CREATE TABLE IF NOT EXISTS research_cause_graph_runs (
    manifest_hash        TEXT PRIMARY KEY CHECK(length(manifest_hash)=64),
    observed_ts          TEXT NOT NULL,
    state                TEXT NOT NULL CHECK(state IN ('BLOCKED_NO_REPLICATED_ROUTE_LOWER_BOUNDS','BLOCKED_STALE_REPLICATED_ROUTE_LOWER_BOUNDS','READY')),
    input_hashes_json    TEXT NOT NULL CHECK(json_valid(input_hashes_json)),
    input_count          INTEGER NOT NULL CHECK(input_count>=0),
    valid_input_count    INTEGER NOT NULL CHECK(valid_input_count>=0),
    result_json          TEXT NOT NULL CHECK(json_valid(result_json)),
    reason               TEXT NOT NULL,
    funded               INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority      INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority       INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)
);
CREATE TRIGGER IF NOT EXISTS research_cause_graph_runs_no_update BEFORE UPDATE ON research_cause_graph_runs BEGIN SELECT RAISE(ABORT,'immutable cause graph receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_cause_graph_runs_no_delete BEFORE DELETE ON research_cause_graph_runs BEGIN SELECT RAISE(ABORT,'immutable cause graph receipt'); END;

-- R139 STEP 1 RULE ARTIFACTS. Structural sports identity discovers which venue instruments may
-- describe the same payoff; it never proves settlement equivalence. These append-only rows retain
-- exact market rules and official settlement-source identifiers/URLs without interpreting prose.
CREATE TABLE IF NOT EXISTS research_rule_artifacts (
    venue                    TEXT NOT NULL CHECK(venue IN ('kalshi','polyus','polymarket')),
    instrument_id            TEXT NOT NULL,
    version                  INTEGER NOT NULL CHECK(version>0),
    artifact_hash            TEXT NOT NULL CHECK(length(artifact_hash)=64),
    observed_ts              TEXT NOT NULL,
    source                   TEXT NOT NULL,
    raw_rules_json           TEXT NOT NULL CHECK(json_valid(raw_rules_json)),
    settlement_sources_json  TEXT NOT NULL CHECK(json_valid(settlement_sources_json)),
    capture_state             TEXT NOT NULL CHECK(capture_state IN ('RAW_COMPLETE_REVIEW_REQUIRED','RAW_RULES_ONLY','SETTLEMENT_SOURCE_ONLY')),
    funded                    INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority           INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority            INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    PRIMARY KEY(venue,instrument_id,version),
    UNIQUE(venue,instrument_id,artifact_hash)
);
CREATE INDEX IF NOT EXISTS idx_rrule_artifact_latest ON research_rule_artifacts(venue,instrument_id,version DESC);
CREATE TRIGGER IF NOT EXISTS research_rule_artifacts_no_update BEFORE UPDATE ON research_rule_artifacts BEGIN SELECT RAISE(ABORT,'immutable venue rule artifact'); END;
CREATE TRIGGER IF NOT EXISTS research_rule_artifacts_no_delete BEFORE DELETE ON research_rule_artifacts BEGIN SELECT RAISE(ABORT,'immutable venue rule artifact'); END;

CREATE TABLE IF NOT EXISTS research_rule_pair_certificates (
    pair_id                    TEXT NOT NULL,
    version                    INTEGER NOT NULL CHECK(version>0),
    spec_hash                  TEXT NOT NULL CHECK(length(spec_hash)=64),
    left_venue                 TEXT NOT NULL CHECK(left_venue IN ('kalshi','polyus','polymarket')),
    left_instrument_id         TEXT NOT NULL,
    right_venue                TEXT NOT NULL CHECK(right_venue IN ('kalshi','polyus','polymarket')),
    right_instrument_id        TEXT NOT NULL,
    orientation                TEXT NOT NULL CHECK(orientation IN ('same','inverse')),
    left_artifact_hash         TEXT NOT NULL CHECK(length(left_artifact_hash)=64),
    right_artifact_hash        TEXT NOT NULL CHECK(length(right_artifact_hash)=64),
    left_payoff_id             TEXT NOT NULL,
    right_payoff_id            TEXT NOT NULL,
    -- Legacy aliases remain populated for old readers. Generic identity above is authoritative.
    kalshi_ticker              TEXT NOT NULL,
    polyus_slug                TEXT NOT NULL,
    kalshi_artifact_hash       TEXT NOT NULL CHECK(length(kalshi_artifact_hash)=64),
    polyus_artifact_hash       TEXT NOT NULL CHECK(length(polyus_artifact_hash)=64),
    canonical_event_id         TEXT NOT NULL,
    canonical_payoff_id        TEXT NOT NULL,
    normalized_predicate_hash  TEXT NOT NULL CHECK(length(normalized_predicate_hash)=64),
    normalized_rules_hash      TEXT NOT NULL CHECK(length(normalized_rules_hash)=64),
    settlement_source_id       TEXT NOT NULL,
    settlement_source_url      TEXT NOT NULL,
    void_policy                TEXT NOT NULL,
    scalar_policy              TEXT NOT NULL,
    unknown_policy             TEXT NOT NULL,
    timing_policy              TEXT NOT NULL,
    review_method              TEXT NOT NULL CHECK(review_method IN ('human_review','structured_official_schema')),
    review_provenance          TEXT NOT NULL,
    review_evidence_hash       TEXT NOT NULL CHECK(length(review_evidence_hash)=64),
    created_ts                 TEXT NOT NULL,
    funded                     INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority            INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority             INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    PRIMARY KEY(pair_id,version),
    UNIQUE(pair_id,spec_hash)
);
CREATE INDEX IF NOT EXISTS idx_rrule_certificate_pair ON research_rule_pair_certificates(kalshi_ticker,polyus_slug,version DESC);
CREATE TRIGGER IF NOT EXISTS research_rule_pair_certificates_no_update BEFORE UPDATE ON research_rule_pair_certificates BEGIN SELECT RAISE(ABORT,'immutable normalized rule certificate'); END;
CREATE TRIGGER IF NOT EXISTS research_rule_pair_certificates_no_delete BEFORE DELETE ON research_rule_pair_certificates BEGIN SELECT RAISE(ABORT,'immutable normalized rule certificate'); END;

CREATE TABLE IF NOT EXISTS research_rule_pair_reviews (
    id                         INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts                TEXT NOT NULL,
    pair_id                    TEXT NOT NULL,
    left_venue                 TEXT NOT NULL CHECK(left_venue IN ('kalshi','polyus','polymarket')),
    left_instrument_id         TEXT NOT NULL,
    right_venue                TEXT NOT NULL CHECK(right_venue IN ('kalshi','polyus','polymarket')),
    right_instrument_id        TEXT NOT NULL,
    orientation                TEXT NOT NULL CHECK(orientation IN ('same','inverse')),
    left_payoff_id             TEXT NOT NULL DEFAULT '',
    right_payoff_id            TEXT NOT NULL DEFAULT '',
    left_artifact_hash         TEXT NOT NULL DEFAULT '',
    right_artifact_hash        TEXT NOT NULL DEFAULT '',
    -- Legacy aliases remain populated for old readers. Generic identity above is authoritative.
    kalshi_ticker              TEXT NOT NULL,
    polyus_slug                TEXT NOT NULL,
    canonical_event_id         TEXT NOT NULL DEFAULT '',
    canonical_payoff_id        TEXT NOT NULL DEFAULT '',
    kalshi_artifact_hash       TEXT NOT NULL DEFAULT '',
    polyus_artifact_hash       TEXT NOT NULL DEFAULT '',
    normalization_state        TEXT NOT NULL CHECK(normalization_state IN ('MISSING_KALSHI_RULES','MISSING_POLYUS_RULES','MISSING_KALSHI_SETTLEMENT_SOURCE','MISSING_POLYUS_SETTLEMENT_SOURCE','RAW_CAPTURED_REVIEW_REQUIRED','CERTIFICATE_STALE','COMPATIBLE_CERTIFIED')),
    exact_blocker              TEXT NOT NULL,
    certificate_hash           TEXT NOT NULL DEFAULT '',
    funded                     INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority            INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority             INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(pair_id,kalshi_artifact_hash,polyus_artifact_hash,normalization_state,certificate_hash)
);
CREATE INDEX IF NOT EXISTS idx_rrule_review_latest ON research_rule_pair_reviews(pair_id,id DESC);
CREATE INDEX IF NOT EXISTS idx_rrule_review_state ON research_rule_pair_reviews(normalization_state,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_rule_pair_reviews_no_update BEFORE UPDATE ON research_rule_pair_reviews BEGIN SELECT RAISE(ABORT,'append-only rule pair review'); END;
CREATE TRIGGER IF NOT EXISTS research_rule_pair_reviews_no_delete BEFORE DELETE ON research_rule_pair_reviews BEGIN SELECT RAISE(ABORT,'append-only rule pair review'); END;

-- Every bounded cross-venue objective candidate receives one immutable decision per collector
-- cycle. Raw venue IDs are the identity; friendly labels are display-only context. Directional
-- acceptance and risk-free lock eligibility are separate fields: exact objective predicates may
-- be directionally comparable while unresolved material void/OT/tie rules keep lock_eligible=0.
-- Accepted and rejected comparisons share this ledger so recall loss, side flips, rule drift, and
-- missing prerequisites cannot disappear as an unmeasured pre-filter.
CREATE TABLE IF NOT EXISTS research_crossvenue_match_decisions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_ts           TEXT NOT NULL,
    cycle_id              TEXT NOT NULL,
    pair_id               TEXT NOT NULL,
    pair_type             TEXT NOT NULL CHECK(pair_type IN ('K-PUS','K-PINT','PUS-PINT')),
    left_venue            TEXT NOT NULL CHECK(left_venue IN ('kalshi','polyus','polymarket')),
    left_instrument_id    TEXT NOT NULL,
    right_venue           TEXT NOT NULL CHECK(right_venue IN ('kalshi','polyus','polymarket')),
    right_instrument_id   TEXT NOT NULL,
    friendly_left         TEXT NOT NULL DEFAULT '',
    friendly_right        TEXT NOT NULL DEFAULT '',
    genre                 TEXT NOT NULL,
    market_type           TEXT NOT NULL,
    orientation           TEXT NOT NULL CHECK(orientation IN ('','same','inverse')),
    accepted              INTEGER NOT NULL CHECK(accepted IN (0,1)),
    lock_eligible         INTEGER NOT NULL CHECK(lock_eligible IN (0,1)),
    reason_code           TEXT NOT NULL CHECK(reason_code GLOB 'ACCEPT_*' OR reason_code GLOB 'REJECT_*'),
    lock_reason_code      TEXT NOT NULL DEFAULT '',
    risk_tier             TEXT NOT NULL,
    taxonomy_version      TEXT NOT NULL,
    predicate_hash        TEXT NOT NULL DEFAULT '',
    rules_hash            TEXT NOT NULL DEFAULT '',
    evidence_hash         TEXT NOT NULL CHECK(length(evidence_hash)=64),
    left_contract_json    TEXT NOT NULL CHECK(json_valid(left_contract_json)),
    right_contract_json   TEXT NOT NULL CHECK(json_valid(right_contract_json)),
    certificate_hash      TEXT NOT NULL DEFAULT '',
    funded                INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
    paper_authority       INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
    live_authority        INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
    UNIQUE(cycle_id,pair_id)
);
CREATE INDEX IF NOT EXISTS idx_rxvdecision_recent ON research_crossvenue_match_decisions(observed_ts DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_rxvdecision_reason ON research_crossvenue_match_decisions(accepted,reason_code,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_crossvenue_match_decisions_no_update BEFORE UPDATE ON research_crossvenue_match_decisions BEGIN SELECT RAISE(ABORT,'immutable cross-venue match decision'); END;
CREATE TRIGGER IF NOT EXISTS research_crossvenue_match_decisions_no_delete BEFORE DELETE ON research_crossvenue_match_decisions BEGIN SELECT RAISE(ABORT,'immutable cross-venue match decision'); END;
