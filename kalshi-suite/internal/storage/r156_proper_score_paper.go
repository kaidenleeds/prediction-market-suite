package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// properScorePaperDDL owns three real Paper ledgers, one per scoring transform.  They are
// deliberately disjoint from paper_fills and from the older zero-authority research simulation:
// each lane has its own $400 cash account, reservations, open positions, fees and settlements.
// Database constraints make an accidental LIVE or real-order authority bit impossible.
const properScorePaperDDL = `
CREATE TABLE IF NOT EXISTS proper_score_paper_meta (
 lane TEXT PRIMARY KEY CHECK(lane IN ('brier','log','spherical')),
 seed_usd REAL NOT NULL CHECK(seed_usd=400),
 created_ts TEXT NOT NULL
);
INSERT OR IGNORE INTO proper_score_paper_meta(lane,seed_usd,created_ts)
VALUES('brier',400,strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      ('log',400,strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      ('spherical',400,strftime('%Y-%m-%dT%H:%M:%fZ','now'));

CREATE TABLE IF NOT EXISTS proper_score_paper_attempts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 attempt_key TEXT NOT NULL UNIQUE,
 observed_ts TEXT NOT NULL,
 execute_after_ts TEXT NOT NULL,
 lane TEXT NOT NULL CHECK(lane IN ('brier','log','spherical')),
 transform TEXT NOT NULL CHECK(transform=lane),
 slot TEXT NOT NULL,
 platform TEXT NOT NULL CHECK(platform IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 event_key TEXT NOT NULL,
 title TEXT NOT NULL DEFAULT '',
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 forecast_yes REAL NOT NULL CHECK(forecast_yes>=0 AND forecast_yes<=1),
 normalized_weight REAL NOT NULL CHECK(normalized_weight>0),
 vector_budget_usd REAL NOT NULL CHECK(vector_budget_usd>0),
 coordinate_budget_usd REAL NOT NULL CHECK(coordinate_budget_usd>0),
 limit_price REAL NOT NULL CHECK(limit_price>0 AND limit_price<1),
 requested_qty REAL NOT NULL CHECK(requested_qty>0),
 reservation_usd REAL NOT NULL CHECK(reservation_usd>=0),
 fee_source TEXT NOT NULL,
 book_source TEXT NOT NULL,
 source_clock_id TEXT NOT NULL,
 execution_shadow_attempt_id TEXT NOT NULL DEFAULT '',
 delay_ms INTEGER NOT NULL CHECK(delay_ms>=25),
 paper_authority INTEGER NOT NULL DEFAULT 1 CHECK(paper_authority=1),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 real_order_capability INTEGER NOT NULL DEFAULT 0 CHECK(real_order_capability=0)
);
CREATE INDEX IF NOT EXISTS idx_pspp_attempt_lane_event
 ON proper_score_paper_attempts(lane,event_key,id DESC);
CREATE INDEX IF NOT EXISTS idx_pspp_attempt_lane_ticker
 ON proper_score_paper_attempts(lane,platform,ticker,id DESC);

CREATE TABLE IF NOT EXISTS proper_score_paper_outcomes (
 attempt_id INTEGER PRIMARY KEY,
 processed_ts TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('rejected','zero_fill','filled','settled')),
 reason TEXT NOT NULL,
 filled_qty REAL NOT NULL DEFAULT 0 CHECK(filled_qty>=0),
 fill_price REAL NOT NULL DEFAULT 0 CHECK(fill_price>=0 AND fill_price<1),
 cost_usd REAL NOT NULL DEFAULT 0 CHECK(cost_usd>=0),
 fee_usd REAL NOT NULL DEFAULT 0 CHECK(fee_usd>=0),
 fee_source TEXT NOT NULL DEFAULT '',
 book_source TEXT NOT NULL DEFAULT '',
 source_clock_id TEXT NOT NULL DEFAULT '',
 settlement_yes REAL,
 payout_usd REAL NOT NULL DEFAULT 0 CHECK(payout_usd>=0),
 realized_net REAL NOT NULL DEFAULT 0,
 settled_ts TEXT NOT NULL DEFAULT '',
 settlement_source TEXT NOT NULL DEFAULT '',
 settlement_hash TEXT NOT NULL DEFAULT '',
 paper_authority INTEGER NOT NULL DEFAULT 1 CHECK(paper_authority=1),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 real_order_capability INTEGER NOT NULL DEFAULT 0 CHECK(real_order_capability=0),
 FOREIGN KEY(attempt_id) REFERENCES proper_score_paper_attempts(id),
 CHECK(
   (state IN ('rejected','zero_fill') AND reason!='' AND filled_qty=0 AND fill_price=0
      AND cost_usd=0 AND fee_usd=0 AND settlement_yes IS NULL AND payout_usd=0)
   OR
   (state='filled' AND filled_qty>0 AND fill_price>0 AND cost_usd>0 AND fee_source!=''
      AND settlement_yes IS NULL AND payout_usd=0 AND settled_ts='')
   OR
   (state='settled' AND filled_qty>0 AND fill_price>0 AND cost_usd>0 AND fee_source!=''
      AND settlement_yes>=0 AND settlement_yes<=1 AND settled_ts!=''
      AND settlement_source!='' AND settlement_hash!='')
 )
);
CREATE INDEX IF NOT EXISTS idx_pspp_outcome_state
 ON proper_score_paper_outcomes(state,attempt_id);

-- Epoch state is separate from the immutable economic rows. Reset advances only the three
-- current pointers; attempts/outcomes and every prior epoch remain append-preserved.
CREATE TABLE IF NOT EXISTS proper_score_paper_epochs (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 lane TEXT NOT NULL CHECK(lane IN ('brier','log','spherical')),
 seed_usd REAL NOT NULL CHECK(seed_usd=400),
 reset_ts TEXT NOT NULL,
 reset_reason TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pspp_epoch_lane
 ON proper_score_paper_epochs(lane,id DESC);
CREATE TABLE IF NOT EXISTS proper_score_paper_current_epoch (
 lane TEXT PRIMARY KEY CHECK(lane IN ('brier','log','spherical')),
 epoch_id INTEGER NOT NULL,
 FOREIGN KEY(epoch_id) REFERENCES proper_score_paper_epochs(id)
);
CREATE TABLE IF NOT EXISTS proper_score_paper_attempt_epoch (
 attempt_id INTEGER PRIMARY KEY,
 epoch_id INTEGER NOT NULL,
 source_attempt_key TEXT NOT NULL,
 FOREIGN KEY(attempt_id) REFERENCES proper_score_paper_attempts(id),
 FOREIGN KEY(epoch_id) REFERENCES proper_score_paper_epochs(id),
 UNIQUE(epoch_id,source_attempt_key)
);
CREATE INDEX IF NOT EXISTS idx_pspp_attempt_epoch
 ON proper_score_paper_attempt_epoch(epoch_id,attempt_id);

-- One-time compatibility boundary: all rows that predate explicit epochs belong to the first
-- preserved epoch for their lane. Re-running schema setup never advances the current pointer.
INSERT INTO proper_score_paper_epochs(lane,seed_usd,reset_ts,reset_reason)
SELECT m.lane,m.seed_usd,m.created_ts,'schema-bootstrap-preserve-history'
FROM proper_score_paper_meta m
WHERE NOT EXISTS(SELECT 1 FROM proper_score_paper_epochs e WHERE e.lane=m.lane);
INSERT OR IGNORE INTO proper_score_paper_current_epoch(lane,epoch_id)
SELECT m.lane,(SELECT MIN(e.id) FROM proper_score_paper_epochs e WHERE e.lane=m.lane)
FROM proper_score_paper_meta m;
INSERT OR IGNORE INTO proper_score_paper_attempt_epoch(attempt_id,epoch_id,source_attempt_key)
SELECT a.id,(SELECT MIN(e.id) FROM proper_score_paper_epochs e WHERE e.lane=a.lane),a.attempt_key
FROM proper_score_paper_attempts a
WHERE NOT EXISTS(SELECT 1 FROM proper_score_paper_attempt_epoch x WHERE x.attempt_id=a.id);

CREATE TRIGGER IF NOT EXISTS proper_score_paper_attempt_no_update
BEFORE UPDATE ON proper_score_paper_attempts
BEGIN SELECT RAISE(ABORT,'immutable Proper Betting Paper attempt'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_paper_attempt_no_delete
BEFORE DELETE ON proper_score_paper_attempts
BEGIN SELECT RAISE(ABORT,'append-preserved Proper Betting Paper attempt'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_paper_outcome_no_delete
BEFORE DELETE ON proper_score_paper_outcomes
BEGIN SELECT RAISE(ABORT,'append-preserved Proper Betting Paper outcome'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_paper_epoch_no_update
BEFORE UPDATE ON proper_score_paper_epochs
BEGIN SELECT RAISE(ABORT,'immutable Proper Betting Paper epoch'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_paper_epoch_no_delete
BEFORE DELETE ON proper_score_paper_epochs
BEGIN SELECT RAISE(ABORT,'append-preserved Proper Betting Paper epoch'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_paper_attempt_epoch_no_update
BEFORE UPDATE ON proper_score_paper_attempt_epoch
BEGIN SELECT RAISE(ABORT,'immutable Proper Betting Paper attempt epoch'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_paper_attempt_epoch_no_delete
BEFORE DELETE ON proper_score_paper_attempt_epoch
BEGIN SELECT RAISE(ABORT,'append-preserved Proper Betting Paper attempt epoch'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_paper_outcome_terminal_update
BEFORE UPDATE ON proper_score_paper_outcomes
WHEN NOT (
 OLD.state='filled' AND NEW.state='settled'
 AND OLD.attempt_id=NEW.attempt_id
 AND OLD.processed_ts=NEW.processed_ts
 AND OLD.reason=NEW.reason
 AND OLD.filled_qty=NEW.filled_qty
 AND OLD.fill_price=NEW.fill_price
 AND OLD.cost_usd=NEW.cost_usd
 AND OLD.fee_usd=NEW.fee_usd
 AND OLD.fee_source=NEW.fee_source
 AND OLD.book_source=NEW.book_source
 AND OLD.source_clock_id=NEW.source_clock_id
 AND OLD.paper_authority=NEW.paper_authority
 AND OLD.live_authority=NEW.live_authority
 AND OLD.real_order_capability=NEW.real_order_capability
 AND OLD.settlement_yes IS NULL
 AND OLD.payout_usd=0
 AND OLD.realized_net=0
 AND OLD.settled_ts=''
 AND OLD.settlement_source=''
 AND OLD.settlement_hash=''
)
BEGIN SELECT RAISE(ABORT,'Proper Betting Paper outcome is terminal except exact settlement'); END;`

func ensureProperScorePaperSchema(db *sql.DB) error {
	if _, err := db.Exec(properScorePaperDDL); err != nil {
		return err
	}
	// Existing append-preserved ledgers predate canonical cross-branch lineage. Legacy rows stay
	// explicitly blank; every new claimed attempt is required to name one immutable shadow ID.
	_, _ = db.Exec(`ALTER TABLE proper_score_paper_attempts
ADD COLUMN execution_shadow_attempt_id TEXT NOT NULL DEFAULT ''`)
	_, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_pspp_execution_shadow_attempt
ON proper_score_paper_attempts(execution_shadow_attempt_id)
WHERE execution_shadow_attempt_id<>''`)
	return err
}

var properScorePaperLanes = [...]string{"brier", "log", "spherical"}

func validProperScorePaperLane(lane string) bool {
	return lane == "brier" || lane == "log" || lane == "spherical"
}

// ResetProperScorePaperPortfolios atomically advances all three $400 Paper lanes to new epochs.
// Attempts, outcomes, and older epoch receipts are never changed or deleted; all money-facing
// reads join through the current pointer, so the new lanes begin cleanly without losing history.
func (s *Store) ResetProperScorePaperPortfolios(ctx context.Context, resetAt time.Time) (map[string]int64, error) {
	if resetAt.IsZero() {
		resetAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	epochs := make(map[string]int64, len(properScorePaperLanes))
	for _, lane := range properScorePaperLanes {
		res, insertErr := tx.ExecContext(ctx, `INSERT INTO proper_score_paper_epochs(
lane,seed_usd,reset_ts,reset_reason) VALUES(?,400,?,'paper-pnl-reset')`,
			lane, resetAt.UTC().Format(time.RFC3339Nano))
		if insertErr != nil {
			return nil, insertErr
		}
		id, idErr := res.LastInsertId()
		if idErr != nil || id <= 0 {
			return nil, firstNonNil(idErr, errors.New("Proper Betting Paper reset omitted epoch id"))
		}
		if _, updateErr := tx.ExecContext(ctx, `INSERT INTO proper_score_paper_current_epoch(lane,epoch_id)
VALUES(?,?) ON CONFLICT(lane) DO UPDATE SET epoch_id=excluded.epoch_id`, lane, id); updateErr != nil {
			return nil, updateErr
		}
		epochs[lane] = id
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return epochs, nil
}

type ProperScorePaperAttempt struct {
	AttemptKey, Lane, Transform, Slot              string
	Platform, Ticker, EventKey, Title, Side        string
	FeeSource, BookSource, SourceClockID           string
	ExecutionShadowAttemptID                       string
	ObservedAt, ExecuteAfter                       time.Time
	ForecastYes, NormalizedWeight, VectorBudgetUSD float64
	CoordinateBudgetUSD, LimitPrice, RequestedQty  float64
	ReservationUSD                                 float64
	DelayMS                                        int
}

type ProperScorePaperOutcome struct {
	AttemptID                             int64
	ProcessedAt                           time.Time
	State, Reason                         string
	FilledQty, FillPrice, CostUSD, FeeUSD float64
	FeeSource, BookSource, SourceClockID  string
}

// ProperScorePaperOutcomeForAttempt returns the one immutable terminal used to fan the dedicated
// $400 lane back into the canonical comparison ledger. Missing means the reservation is still
// genuinely pending; no zero-fill or rejection is inferred.
func (s *Store) ProperScorePaperOutcomeForAttempt(ctx context.Context,
	attemptID int64) (ProperScorePaperOutcome, bool, error) {
	var out ProperScorePaperOutcome
	var processed string
	err := s.db.QueryRowContext(ctx, `SELECT attempt_id,processed_ts,state,reason,filled_qty,
fill_price,cost_usd,fee_usd,fee_source,book_source,source_clock_id
FROM proper_score_paper_outcomes WHERE attempt_id=?`, attemptID).Scan(&out.AttemptID,
		&processed, &out.State, &out.Reason, &out.FilledQty, &out.FillPrice, &out.CostUSD,
		&out.FeeUSD, &out.FeeSource, &out.BookSource, &out.SourceClockID)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	out.ProcessedAt, _ = time.Parse(time.RFC3339Nano, processed)
	return out, true, nil
}

func validProperScorePaperFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func normalizeProperScorePaperAttempt(v ProperScorePaperAttempt) (ProperScorePaperAttempt, error) {
	v.AttemptKey = strings.TrimSpace(v.AttemptKey)
	v.Lane = strings.ToLower(strings.TrimSpace(v.Lane))
	v.Transform = strings.ToLower(strings.TrimSpace(v.Transform))
	v.Slot = strings.TrimSpace(v.Slot)
	v.Platform = strings.ToLower(strings.TrimSpace(v.Platform))
	v.Ticker = strings.TrimSpace(v.Ticker)
	v.EventKey = strings.TrimSpace(v.EventKey)
	v.Title = strings.TrimSpace(v.Title)
	v.Side = strings.ToUpper(strings.TrimSpace(v.Side))
	v.FeeSource = strings.TrimSpace(v.FeeSource)
	v.BookSource = strings.TrimSpace(v.BookSource)
	v.SourceClockID = strings.TrimSpace(v.SourceClockID)
	v.ExecutionShadowAttemptID = strings.TrimSpace(v.ExecutionShadowAttemptID)
	if v.ObservedAt.IsZero() {
		v.ObservedAt = time.Now().UTC()
	}
	if v.ExecuteAfter.IsZero() {
		v.ExecuteAfter = v.ObservedAt.Add(time.Duration(v.DelayMS) * time.Millisecond)
	}
	for _, n := range []float64{v.ForecastYes, v.NormalizedWeight, v.VectorBudgetUSD,
		v.CoordinateBudgetUSD, v.LimitPrice, v.RequestedQty, v.ReservationUSD} {
		if !validProperScorePaperFloat(n) {
			return v, errors.New("non-finite Proper Betting Paper attempt")
		}
	}
	if v.AttemptKey == "" || v.Lane != v.Transform ||
		(v.Lane != "brier" && v.Lane != "log" && v.Lane != "spherical") ||
		v.Slot == "" || (v.Platform != "kalshi" && v.Platform != "polyus") ||
		v.Ticker == "" || v.EventKey == "" || (v.Side != "YES" && v.Side != "NO") ||
		v.ForecastYes < 0 || v.ForecastYes > 1 || v.NormalizedWeight <= 0 ||
		v.VectorBudgetUSD <= 0 || v.CoordinateBudgetUSD <= 0 ||
		v.LimitPrice <= 0 || v.LimitPrice >= 1 || v.RequestedQty <= 0 ||
		v.ReservationUSD <= 0 || v.FeeSource == "" || v.BookSource == "" ||
		v.SourceClockID == "" || v.ExecutionShadowAttemptID == "" || v.DelayMS < 25 {
		return v, errors.New("invalid Proper Betting Paper attempt")
	}
	return v, nil
}

func insertProperScorePaperTerminalTx(ctx context.Context, tx *sql.Tx, attemptID int64,
	state, reason string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO proper_score_paper_outcomes(
attempt_id,processed_ts,state,reason,paper_authority,live_authority,real_order_capability)
VALUES(?,?,?,?,1,0,0)`, attemptID, time.Now().UTC().Format(time.RFC3339Nano), state, reason)
	return err
}

// ClaimProperScorePaperAttempt atomically reserves only this lane's cash and refuses ticker/event
// conflicts only inside that same lane. A refusal is still a durable terminal Paper receipt.
func (s *Store) ClaimProperScorePaperAttempt(ctx context.Context,
	in ProperScorePaperAttempt) (int64, bool, string, error) {
	in, err := normalizeProperScorePaperAttempt(in)
	if err != nil {
		return 0, false, "", err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, "", err
	}
	defer tx.Rollback()
	var epochID int64
	var seed float64
	if err = tx.QueryRowContext(ctx, `SELECT c.epoch_id,e.seed_usd
FROM proper_score_paper_current_epoch c
JOIN proper_score_paper_epochs e ON e.id=c.epoch_id AND e.lane=c.lane
WHERE c.lane=?`, in.Lane).Scan(&epochID, &seed); err != nil {
		return 0, false, "", err
	}
	var priorID int64
	if err = tx.QueryRowContext(ctx, `SELECT a.id FROM proper_score_paper_attempt_epoch x
JOIN proper_score_paper_attempts a ON a.id=x.attempt_id
WHERE x.epoch_id=? AND x.source_attempt_key=?`, epochID, in.AttemptKey).Scan(&priorID); err == nil {
		return priorID, false, "duplicate-attempt", nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return 0, false, "", err
	}
	var cash, pending, openCost, openExposure float64
	if err = tx.QueryRowContext(ctx, `SELECT
 ?-COALESCE(SUM(CASE WHEN o.state IN ('filled','settled') THEN o.cost_usd+o.fee_usd ELSE 0 END),0)
   +COALESCE(SUM(CASE WHEN o.state='settled' THEN o.payout_usd ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN o.attempt_id IS NULL THEN a.reservation_usd ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN o.state='filled' THEN o.cost_usd ELSE 0 END),0),
 COALESCE(SUM(CASE WHEN o.state='filled' THEN o.cost_usd+o.fee_usd ELSE 0 END),0)
FROM proper_score_paper_attempt_epoch x
JOIN proper_score_paper_attempts a ON a.id=x.attempt_id
LEFT JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id WHERE x.epoch_id=?`,
		seed, epochID).Scan(&cash, &pending, &openCost, &openExposure); err != nil {
		return 0, false, "", err
	}
	reason := ""
	var n int
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM proper_score_paper_attempt_epoch x
JOIN proper_score_paper_attempts a ON a.id=x.attempt_id
JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id
WHERE x.epoch_id=? AND a.platform=? AND a.ticker=? AND o.state IN ('filled','settled')`,
		epochID, in.Platform, in.Ticker).Scan(&n); err != nil {
		return 0, false, "", err
	}
	if n > 0 {
		reason = "ticker-already-held-or-settled"
	}
	if reason == "" {
		if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM proper_score_paper_attempt_epoch x
JOIN proper_score_paper_attempts a ON a.id=x.attempt_id
LEFT JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id
WHERE x.epoch_id=? AND a.event_key=? AND (o.attempt_id IS NULL OR o.state='filled')`,
			epochID, in.EventKey).Scan(&n); err != nil {
			return 0, false, "", err
		}
		if n > 0 {
			reason = "same-event-position-or-reservation"
		}
	}
	if reason == "" && cash-pending+1e-9 < in.ReservationUSD {
		reason = "isolated-lane-cash-unavailable"
	}
	equity := cash + openCost
	if reason == "" && openExposure+pending+in.ReservationUSD > 0.20*equity+1e-9 {
		reason = "isolated-lane-20pct-open-exposure-cap"
	}
	reservation := in.ReservationUSD
	if reason != "" {
		reservation = 0
	}
	physicalAttemptKey := in.AttemptKey
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM proper_score_paper_attempts
WHERE attempt_key=?`, physicalAttemptKey).Scan(&n); err != nil {
		return 0, false, "", err
	}
	if n > 0 {
		sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s", epochID, in.AttemptKey)))
		physicalAttemptKey = fmt.Sprintf("pspp:%d:%x", epochID, sum)
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO proper_score_paper_attempts(
attempt_key,observed_ts,execute_after_ts,lane,transform,slot,platform,ticker,event_key,title,side,
forecast_yes,normalized_weight,vector_budget_usd,coordinate_budget_usd,limit_price,requested_qty,
reservation_usd,fee_source,book_source,source_clock_id,delay_ms,paper_authority,live_authority,
real_order_capability,execution_shadow_attempt_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,0,0,?)`,
		physicalAttemptKey, in.ObservedAt.UTC().Format(time.RFC3339Nano),
		in.ExecuteAfter.UTC().Format(time.RFC3339Nano), in.Lane, in.Transform, in.Slot,
		in.Platform, in.Ticker, in.EventKey, in.Title, in.Side, in.ForecastYes,
		in.NormalizedWeight, in.VectorBudgetUSD, in.CoordinateBudgetUSD, in.LimitPrice,
		in.RequestedQty, reservation, in.FeeSource, in.BookSource, in.SourceClockID, in.DelayMS,
		in.ExecutionShadowAttemptID)
	if err != nil {
		return 0, false, "", err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, false, "", err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO proper_score_paper_attempt_epoch(
attempt_id,epoch_id,source_attempt_key) VALUES(?,?,?)`, id, epochID, in.AttemptKey); err != nil {
		return 0, false, "", err
	}
	if reason != "" {
		if err = insertProperScorePaperTerminalTx(ctx, tx, id, "rejected", reason); err != nil {
			return 0, false, "", err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, false, "", err
	}
	return id, reason == "", reason, nil
}

// ProperScorePaperLaneSizing is the lane-local bankroll receipt used before a scoring vector is
// converted into dollars. Equity marks open contracts at entry cost, so neither stale quotes nor
// optimistic midpoints can enlarge the next 5% vector.
func (s *Store) ProperScorePaperLaneSizing(ctx context.Context, lane string) (cash, equity float64, err error) {
	lane = strings.ToLower(strings.TrimSpace(lane))
	if !validProperScorePaperLane(lane) {
		return 0, 0, errors.New("invalid Proper Betting Paper lane")
	}
	var seed, cost, fees, payout, openCost sql.NullFloat64
	err = s.db.QueryRowContext(ctx, `SELECT MAX(e.seed_usd),
 SUM(CASE WHEN o.state IN ('filled','settled') THEN o.cost_usd ELSE 0 END),
 SUM(CASE WHEN o.state IN ('filled','settled') THEN o.fee_usd ELSE 0 END),
 SUM(CASE WHEN o.state='settled' THEN o.payout_usd ELSE 0 END),
 SUM(CASE WHEN o.state='filled' THEN o.cost_usd ELSE 0 END)
FROM proper_score_paper_current_epoch c
JOIN proper_score_paper_epochs e ON e.id=c.epoch_id AND e.lane=c.lane
LEFT JOIN proper_score_paper_attempt_epoch x ON x.epoch_id=e.id
LEFT JOIN proper_score_paper_attempts a ON a.id=x.attempt_id
LEFT JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id WHERE c.lane=?`, lane).Scan(
		&seed, &cost, &fees, &payout, &openCost)
	if err != nil {
		return 0, 0, err
	}
	cash = seed.Float64 - cost.Float64 - fees.Float64 + payout.Float64
	equity = cash + openCost.Float64
	if !validProperScorePaperFloat(cash) || !validProperScorePaperFloat(equity) {
		return 0, 0, errors.New("non-finite Proper Betting Paper bankroll")
	}
	return cash, equity, nil
}

func (s *Store) CompleteProperScorePaperAttempt(ctx context.Context,
	in ProperScorePaperOutcome) error {
	in.State = strings.ToLower(strings.TrimSpace(in.State))
	in.Reason = strings.TrimSpace(in.Reason)
	in.FeeSource = strings.TrimSpace(in.FeeSource)
	in.BookSource = strings.TrimSpace(in.BookSource)
	in.SourceClockID = strings.TrimSpace(in.SourceClockID)
	if in.ProcessedAt.IsZero() {
		in.ProcessedAt = time.Now().UTC()
	}
	for _, n := range []float64{in.FilledQty, in.FillPrice, in.CostUSD, in.FeeUSD} {
		if !validProperScorePaperFloat(n) {
			return errors.New("non-finite Proper Betting Paper outcome")
		}
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var requested, reserved float64
	if err = tx.QueryRowContext(ctx, `SELECT requested_qty,reservation_usd
FROM proper_score_paper_attempts WHERE id=?`, in.AttemptID).Scan(&requested, &reserved); err != nil {
		return err
	}
	switch in.State {
	case "zero_fill":
		if in.Reason == "" || in.FilledQty != 0 || in.FillPrice != 0 ||
			in.CostUSD != 0 || in.FeeUSD != 0 {
			return errors.New("invalid Proper Betting Paper zero fill")
		}
	case "filled":
		if in.Reason != "" || in.FilledQty <= 0 || in.FilledQty > requested+1e-9 ||
			in.FillPrice <= 0 || in.FillPrice >= 1 || in.CostUSD <= 0 ||
			in.FeeUSD < 0 || in.FeeSource == "" || in.BookSource == "" ||
			in.SourceClockID == "" || in.CostUSD+in.FeeUSD > reserved+1e-8 {
			return errors.New("invalid Proper Betting Paper fill")
		}
	default:
		return errors.New("invalid Proper Betting Paper outcome state")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO proper_score_paper_outcomes(
attempt_id,processed_ts,state,reason,filled_qty,fill_price,cost_usd,fee_usd,fee_source,
book_source,source_clock_id,paper_authority,live_authority,real_order_capability)
VALUES(?,?,?,?,?,?,?,?,?,?,?,1,0,0)`, in.AttemptID,
		in.ProcessedAt.UTC().Format(time.RFC3339Nano), in.State, in.Reason,
		in.FilledQty, in.FillPrice, in.CostUSD, in.FeeUSD, in.FeeSource,
		in.BookSource, in.SourceClockID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// TerminalizeProperScorePaperAttemptFailure releases a claimed reservation after an execution
// check/persistence error. It is intentionally idempotent: an ambiguous commit may already have
// created a filled/zero-fill outcome, in which case that immutable terminal receipt wins.
func (s *Store) TerminalizeProperScorePaperAttemptFailure(ctx context.Context,
	attemptID int64, reason string) error {
	reason = strings.TrimSpace(reason)
	if attemptID <= 0 || reason == "" {
		return errors.New("invalid Proper Betting Paper failure terminal")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `INSERT INTO proper_score_paper_outcomes(
attempt_id,processed_ts,state,reason,paper_authority,live_authority,real_order_capability)
SELECT a.id,?,'zero_fill',?,1,0,0 FROM proper_score_paper_attempts a
WHERE a.id=? AND NOT EXISTS(
 SELECT 1 FROM proper_score_paper_outcomes o WHERE o.attempt_id=a.id
)`, now, reason, attemptID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	var terminal int
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM proper_score_paper_outcomes
WHERE attempt_id=?`, attemptID).Scan(&terminal); err != nil {
		return err
	}
	if terminal == 1 {
		return nil
	}
	return sql.ErrNoRows
}

// RecoverProperScorePaperAttempts converts pre-crash reservations into explicit zero fills. It
// never retries an old decision against a later book.
func (s *Store) RecoverProperScorePaperAttempts(ctx context.Context, now time.Time) (int64, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO proper_score_paper_outcomes(
attempt_id,processed_ts,state,reason,paper_authority,live_authority,real_order_capability)
SELECT a.id,?,'zero_fill','process-ended-before-delayed-IOC-check',1,0,0
FROM proper_score_paper_attempts a LEFT JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id
WHERE o.attempt_id IS NULL AND a.execute_after_ts<?`,
		now.UTC().Format(time.RFC3339Nano), now.Add(-30*time.Second).UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) settleProperScorePaperAttempt(ctx context.Context, attemptID int64, yesValue float64,
	closed, source, hash string) (bool, error) {
	if attemptID <= 0 || !validProperScorePaperFloat(yesValue) || yesValue < 0 || yesValue > 1 ||
		strings.TrimSpace(closed) == "" || strings.TrimSpace(source) == "" ||
		strings.TrimSpace(hash) == "" {
		return false, errors.New("invalid Proper Betting Paper settlement")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var side, state, settledTS, settlementSource, settlementHash string
	var qty, price, fee float64
	var priorYes sql.NullFloat64
	if err = tx.QueryRowContext(ctx, `SELECT a.side,o.state,o.filled_qty,o.fill_price,o.fee_usd,
o.settlement_yes,o.settled_ts,o.settlement_source,o.settlement_hash
FROM proper_score_paper_attempts a JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id
WHERE a.id=?`, attemptID).Scan(&side, &state, &qty, &price, &fee, &priorYes,
		&settledTS, &settlementSource, &settlementHash); err != nil {
		return false, err
	}
	if state == "settled" {
		if !priorYes.Valid || math.Abs(priorYes.Float64-yesValue) > 1e-12 ||
			settledTS != closed || settlementSource != strings.TrimSpace(source) ||
			settlementHash != strings.TrimSpace(hash) {
			return false, errors.New("Proper Betting Paper settlement cannot be rewritten")
		}
		return false, nil
	}
	if state != "filled" {
		return false, errors.New("Proper Betting Paper settlement requires an open fill")
	}
	payoutValue := yesValue
	if side == "NO" {
		payoutValue = 1 - yesValue
	}
	payout := qty * payoutValue
	realized := payout - qty*price - fee
	res, err := tx.ExecContext(ctx, `UPDATE proper_score_paper_outcomes SET
state='settled',settlement_yes=?,payout_usd=?,realized_net=?,settled_ts=?,
settlement_source=?,settlement_hash=? WHERE attempt_id=? AND state='filled'`,
		yesValue, payout, realized, closed, source, hash, attemptID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		return false, firstNonNil(err, errors.New("Proper Betting Paper settlement lost its open row"))
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func firstNonNil(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// ResolveProperScorePaperPortfolios uses the same authoritative settlement sources as the proper
// scoring experiment, but updates only the three isolated Paper cash ledgers.
func (s *Store) ResolveProperScorePaperPortfolios(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 4000 {
		limit = 4000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.id,a.platform,a.ticker
FROM proper_score_paper_attempts a JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id
WHERE o.state='filled' AND (
 EXISTS(SELECT 1 FROM venue_settlements v WHERE v.platform=a.platform AND v.ticker=a.ticker)
 OR EXISTS(SELECT 1 FROM signal_log s INDEXED BY idx_signal_exact_settlement
  WHERE s.platform=a.platform AND s.ticker=a.ticker AND s.resolved=1
   AND s.settle_val>=0 AND s.settle_val<=1))
ORDER BY a.id LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	type key struct {
		id               int64
		platform, ticker string
	}
	var keys []key
	for rows.Next() {
		var k key
		if err = rows.Scan(&k.id, &k.platform, &k.ticker); err != nil {
			_ = rows.Close()
			return 0, err
		}
		keys = append(keys, k)
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	settled := 0
	for _, k := range keys {
		var yes float64
		var closed string
		source := "venue_settlements"
		err = s.db.QueryRowContext(ctx, `SELECT yes_value,resolved_at FROM venue_settlements
WHERE platform=? AND ticker=?`, k.platform, k.ticker).Scan(&yes, &closed)
		if errors.Is(err, sql.ErrNoRows) {
			source = "signal_log"
			err = s.db.QueryRowContext(ctx, `SELECT settle_val,resolved_at FROM signal_log
INDEXED BY idx_signal_ticker_resolved
WHERE platform=? AND ticker=? AND resolved=1 AND settle_val IS NOT NULL
 AND resolved_at IS NOT NULL AND resolved_at!='' ORDER BY ts DESC LIMIT 1`,
				k.platform, k.ticker).Scan(&yes, &closed)
		}
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return settled, err
		}
		rawHash := fmt.Sprintf("%s|%s|%s|%.12f|%s", source, k.platform, k.ticker, yes, closed)
		hash := fmt.Sprintf("%x", sha256.Sum256([]byte(rawHash)))
		ok, settleErr := s.settleProperScorePaperAttempt(ctx, k.id, yes, closed, source, hash)
		if settleErr != nil {
			return settled, settleErr
		}
		if ok {
			settled++
		}
	}
	return settled, nil
}

// ProperScorePaperPortfolioReport returns simple money truth by isolated lane. Open positions are
// marked at their entry cost (not a stale/current midpoint), so the reported net includes paid
// fees but never invents an unrealized gain.
func (s *Store) ProperScorePaperPortfolioReport(ctx context.Context) (map[string]any, error) {
	out := map[string]any{}
	for _, lane := range properScorePaperLanes {
		var epochID int64
		var resetAt string
		var seed float64
		var attempts, rejected, zeroFills, fills, openN, settledN int
		var cost, openCost, fees, payout, realized, qty, reserved sql.NullFloat64
		err := s.db.QueryRowContext(ctx, `SELECT e.id,e.reset_ts,e.seed_usd,COUNT(a.id),
 COALESCE(SUM(o.state='rejected'),0),COALESCE(SUM(o.state='zero_fill'),0),
 COALESCE(SUM(o.state IN ('filled','settled')),0),COALESCE(SUM(o.state='filled'),0),
 COALESCE(SUM(o.state='settled'),0),
 SUM(CASE WHEN o.state IN ('filled','settled') THEN o.cost_usd ELSE 0 END),
 SUM(CASE WHEN o.state='filled' THEN o.cost_usd ELSE 0 END),
 SUM(CASE WHEN o.state IN ('filled','settled') THEN o.fee_usd ELSE 0 END),
 SUM(CASE WHEN o.state='settled' THEN o.payout_usd ELSE 0 END),
 SUM(CASE WHEN o.state='settled' THEN o.realized_net ELSE 0 END),
 SUM(CASE WHEN o.state IN ('filled','settled') THEN o.filled_qty ELSE 0 END),
 SUM(CASE WHEN o.attempt_id IS NULL THEN a.reservation_usd ELSE 0 END)
 FROM proper_score_paper_current_epoch c
 JOIN proper_score_paper_epochs e ON e.id=c.epoch_id AND e.lane=c.lane
 LEFT JOIN proper_score_paper_attempt_epoch x ON x.epoch_id=e.id
 LEFT JOIN proper_score_paper_attempts a ON a.id=x.attempt_id
 LEFT JOIN proper_score_paper_outcomes o ON o.attempt_id=a.id WHERE c.lane=?`, lane).Scan(
			&epochID, &resetAt, &seed, &attempts, &rejected, &zeroFills, &fills, &openN, &settledN,
			&cost, &openCost, &fees, &payout, &realized, &qty, &reserved)
		if err != nil {
			return nil, err
		}
		cash := seed - cost.Float64 - fees.Float64 + payout.Float64
		equity := cash + openCost.Float64
		out[lane] = map[string]any{
			"epoch_id": epochID, "epoch_reset_at": resetAt,
			"seed_usd": seed, "cash_usd": cash, "entry_mark_equity_usd": equity,
			"net_profit_usd": equity - seed, "realized_net_usd": realized.Float64,
			"open_positions": openN, "settled_positions": settledN,
			"attempts": attempts, "fills": fills, "zero_fills": zeroFills,
			"rejections": rejected, "contracts_filled": qty.Float64,
			"turnover_usd": cost.Float64, "fees_usd": fees.Float64,
			"pending_reservations_usd": reserved.Float64,
			"history_policy":           "current epoch only; prior epochs remain immutable audit history",
			"open_value_policy":        "entry cost only; no invented midpoint gain",
			"execution_model":          "configured-delay second fresh-book IOC, original limit, visible depth, exact aggregate fee",
			"paper_authority":          true, "live_authority": false, "real_order_capability": false,
		}
	}
	return out, nil
}
