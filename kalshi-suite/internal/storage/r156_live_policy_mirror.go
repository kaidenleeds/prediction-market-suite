package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const LivePolicyMirrorSeedUSD = 400.0

const (
	LivePolicyMirrorRejected = "rejected"
	LivePolicyMirrorZeroFill = "zero_fill"
	LivePolicyMirrorFilled   = "filled"
	LivePolicyMirrorSettled  = "settled"
)

// LivePolicyMirrorRow is one isolated fake-money decision made by the exact LIVE System policy.
// It never joins paper_fills, live pending risk, or any venue-order table.
type LivePolicyMirrorRow struct {
	ID              int64
	CandidateID     string
	IntentKey       string
	ObservedAt      time.Time
	ProcessedAt     time.Time
	Venue           string
	Ticker          string
	Title           string
	Side            string
	SystemID        string
	Route           string
	ModelVersion    string
	State           string
	Reason          string
	SignalPrice     float64
	LimitPrice      float64
	RequestedQty    float64
	FilledQty       float64
	FillPrice       float64
	TouchDepth      float64
	FeeUSD          float64
	FeeSource       string
	CostUSD         float64
	ProofMean       float64
	ProofLower      float64
	ProofFeePC      float64
	SizingBankroll  float64
	SizingTargetUSD float64
	EventKey        string
	ClusterKey      string
	Crypto          bool
	DelayMS         int64
	WireDelayMS     int64
	BookSource      string
	SettlementKnown bool
	SettlementValue float64
	SettledAt       time.Time
	SettlementSrc   string
	SettlementHash  string
	RealizedNet     float64
}

type LivePolicyMirrorMeta struct {
	EpochAt     time.Time
	SeedUSD     float64
	PeakNAVUSD  float64
	DayKey      string
	DayTurnover float64
	UpdatedAt   time.Time
}

type LivePolicyMirrorRollup struct {
	StateCounts  map[string]int
	ReasonCounts map[string]int
	SettledNet   float64
	FeesUSD      float64
}

type LivePolicyMirrorResetReceipt struct {
	EpochAt         time.Time
	SeedUSD         float64
	ExcludedOpen    int
	ExcludedSettled int
	ExcludedRows    int
}

func migrateLivePolicyMirrorSchema(db *sql.DB) error {
	if db == nil {
		return errors.New("nil database")
	}
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS live_policy_mirror_meta (
 id INTEGER PRIMARY KEY CHECK(id=1),
 epoch_ts TEXT NOT NULL,
 seed_usd REAL NOT NULL CHECK(seed_usd>0),
 peak_nav_usd REAL NOT NULL CHECK(peak_nav_usd>0),
 day_key TEXT NOT NULL,
 day_turnover_usd REAL NOT NULL CHECK(day_turnover_usd>=0),
 updated_ts TEXT NOT NULL
);
INSERT OR IGNORE INTO live_policy_mirror_meta(
 id,epoch_ts,seed_usd,peak_nav_usd,day_key,day_turnover_usd,updated_ts
) VALUES(
 1,strftime('%Y-%m-%dT%H:%M:%fZ','now'),400.0,400.0,strftime('%Y-%m-%d','now'),0,
 strftime('%Y-%m-%dT%H:%M:%fZ','now')
);
CREATE TABLE IF NOT EXISTS live_policy_mirror (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 candidate_id TEXT NOT NULL UNIQUE,
 intent_key TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 processed_ts TEXT NOT NULL,
 venue TEXT NOT NULL CHECK(venue='kalshi'),
 ticker TEXT NOT NULL,
 title TEXT NOT NULL DEFAULT '',
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 system_id TEXT NOT NULL,
 route TEXT NOT NULL CHECK(route='taker'),
 model_version TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('rejected','zero_fill','filled','settled')),
 reason TEXT NOT NULL DEFAULT '',
 signal_price REAL NOT NULL DEFAULT 0,
 limit_price REAL NOT NULL DEFAULT 0,
 requested_qty REAL NOT NULL DEFAULT 0 CHECK(requested_qty>=0),
 filled_qty REAL NOT NULL DEFAULT 0 CHECK(filled_qty>=0),
 fill_price REAL NOT NULL DEFAULT 0,
 touch_depth REAL NOT NULL DEFAULT 0 CHECK(touch_depth>=0),
 fee_usd REAL NOT NULL DEFAULT 0 CHECK(fee_usd>=0),
 fee_source TEXT NOT NULL DEFAULT '',
 cost_usd REAL NOT NULL DEFAULT 0 CHECK(cost_usd>=0),
 proof_mean REAL NOT NULL DEFAULT 0,
 proof_lower REAL NOT NULL DEFAULT 0,
 proof_fee_pc REAL NOT NULL DEFAULT 0,
 sizing_bankroll REAL NOT NULL DEFAULT 0,
 sizing_target_usd REAL NOT NULL DEFAULT 0,
 event_key TEXT NOT NULL DEFAULT '',
 cluster_key TEXT NOT NULL DEFAULT '',
 is_crypto INTEGER NOT NULL DEFAULT 0 CHECK(is_crypto IN (0,1)),
 delay_ms INTEGER NOT NULL DEFAULT 0 CHECK(delay_ms>=0),
 wire_delay_ms INTEGER NOT NULL DEFAULT 0 CHECK(wire_delay_ms>=0),
 book_source TEXT NOT NULL DEFAULT '',
 settlement_known INTEGER NOT NULL DEFAULT 0 CHECK(settlement_known IN (0,1)),
 settlement_value REAL NOT NULL DEFAULT 0,
 settled_ts TEXT NOT NULL DEFAULT '',
 settlement_source TEXT NOT NULL DEFAULT '',
 settlement_hash TEXT NOT NULL DEFAULT '',
 realized_net REAL NOT NULL DEFAULT 0,
 updated_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_live_policy_mirror_intent
 ON live_policy_mirror(intent_key,observed_ts DESC,id DESC);
CREATE INDEX IF NOT EXISTS idx_live_policy_mirror_state
 ON live_policy_mirror(state,id);
CREATE TRIGGER IF NOT EXISTS live_policy_mirror_no_delete
BEFORE DELETE ON live_policy_mirror
BEGIN SELECT RAISE(ABORT,'live-policy mirror evidence is append-preserved'); END;
CREATE TRIGGER IF NOT EXISTS live_policy_mirror_fill_economics_insert
BEFORE INSERT ON live_policy_mirror
WHEN (
 (NEW.state='filled' AND (
   NEW.filled_qty<=0 OR NEW.fill_price<=0 OR NEW.fill_price>=1 OR NEW.fee_usd<0
   OR ABS(NEW.cost_usd-(NEW.filled_qty*NEW.fill_price+NEW.fee_usd))>0.000000001
 ))
 OR
 (NEW.state IN ('rejected','zero_fill') AND (
   NEW.filled_qty<>0 OR NEW.fee_usd<>0 OR NEW.cost_usd<>0
 ))
)
BEGIN SELECT RAISE(ABORT,'live-policy mirror row has inconsistent fill economics'); END;
DROP TRIGGER IF EXISTS live_policy_mirror_settlement_only;
CREATE TRIGGER live_policy_mirror_settlement_only
BEFORE UPDATE ON live_policy_mirror
WHEN NOT (
 OLD.state='filled' AND NEW.state='settled'
 AND NEW.id IS OLD.id
 AND NEW.candidate_id IS OLD.candidate_id
 AND NEW.intent_key IS OLD.intent_key
 AND NEW.observed_ts IS OLD.observed_ts
 AND NEW.processed_ts IS OLD.processed_ts
 AND NEW.venue IS OLD.venue
 AND NEW.ticker IS OLD.ticker
 AND NEW.title IS OLD.title
 AND NEW.side IS OLD.side
 AND NEW.system_id IS OLD.system_id
 AND NEW.route IS OLD.route
 AND NEW.model_version IS OLD.model_version
 AND NEW.reason IS OLD.reason
 AND NEW.signal_price IS OLD.signal_price
 AND NEW.limit_price IS OLD.limit_price
 AND NEW.requested_qty IS OLD.requested_qty
 AND NEW.filled_qty IS OLD.filled_qty
 AND NEW.fill_price IS OLD.fill_price
 AND NEW.touch_depth IS OLD.touch_depth
 AND NEW.fee_usd IS OLD.fee_usd
 AND NEW.fee_source IS OLD.fee_source
 AND NEW.cost_usd IS OLD.cost_usd
 AND NEW.proof_mean IS OLD.proof_mean
 AND NEW.proof_lower IS OLD.proof_lower
 AND NEW.proof_fee_pc IS OLD.proof_fee_pc
 AND NEW.sizing_bankroll IS OLD.sizing_bankroll
 AND NEW.sizing_target_usd IS OLD.sizing_target_usd
 AND NEW.event_key IS OLD.event_key
 AND NEW.cluster_key IS OLD.cluster_key
 AND NEW.is_crypto IS OLD.is_crypto
 AND NEW.delay_ms IS OLD.delay_ms
 AND NEW.wire_delay_ms IS OLD.wire_delay_ms
 AND NEW.book_source IS OLD.book_source
 AND OLD.settlement_known=0
 AND NEW.settlement_known=1
 AND NEW.settlement_value>=0 AND NEW.settlement_value<=1
 AND NEW.settled_ts<>''
 AND NEW.settlement_source<>''
 AND NEW.settlement_hash<>''
 AND ABS(NEW.realized_net - (
   OLD.filled_qty *
     (CASE WHEN OLD.side='YES' THEN NEW.settlement_value ELSE 1-NEW.settlement_value END)
   - OLD.cost_usd
 ))<=0.000000001
)
BEGIN SELECT RAISE(ABORT,'live-policy mirror rows only permit one authoritative settlement'); END;
`)
	return err
}

func validLivePolicyMirrorFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func normalizeLivePolicyMirrorRow(in LivePolicyMirrorRow) (LivePolicyMirrorRow, error) {
	in.CandidateID = strings.TrimSpace(in.CandidateID)
	in.IntentKey = strings.TrimSpace(in.IntentKey)
	in.Venue = strings.ToLower(strings.TrimSpace(in.Venue))
	in.Ticker = strings.TrimSpace(in.Ticker)
	in.Title = strings.TrimSpace(in.Title)
	in.Side = strings.ToUpper(strings.TrimSpace(in.Side))
	in.SystemID = strings.ToLower(strings.TrimSpace(in.SystemID))
	in.Route = strings.ToLower(strings.TrimSpace(in.Route))
	in.ModelVersion = strings.TrimSpace(in.ModelVersion)
	in.State = strings.ToLower(strings.TrimSpace(in.State))
	in.Reason = strings.TrimSpace(in.Reason)
	in.FeeSource = strings.TrimSpace(in.FeeSource)
	in.EventKey = strings.TrimSpace(in.EventKey)
	in.ClusterKey = strings.TrimSpace(in.ClusterKey)
	in.BookSource = strings.TrimSpace(in.BookSource)
	if in.ObservedAt.IsZero() {
		in.ObservedAt = time.Now().UTC()
	}
	if in.ProcessedAt.IsZero() {
		in.ProcessedAt = time.Now().UTC()
	}
	if in.CandidateID == "" || in.IntentKey == "" || in.Venue != "kalshi" ||
		in.Ticker == "" || (in.Side != "YES" && in.Side != "NO") ||
		in.SystemID == "" || in.Route != "taker" || in.ModelVersion == "" {
		return in, errors.New("invalid live-policy mirror identity")
	}
	switch in.State {
	case LivePolicyMirrorRejected, LivePolicyMirrorZeroFill:
		if in.Reason == "" || in.FilledQty != 0 || in.CostUSD != 0 || in.FeeUSD != 0 {
			return in, errors.New("invalid terminal no-fill live-policy mirror row")
		}
	case LivePolicyMirrorFilled:
		if in.RequestedQty <= 0 || in.FilledQty <= 0 || in.FilledQty > in.RequestedQty+1e-9 ||
			in.FillPrice <= 0 || in.FillPrice >= 1 || in.CostUSD <= 0 ||
			in.FeeSource == "" || in.SettlementKnown ||
			math.Abs(in.CostUSD-(in.FilledQty*in.FillPrice+in.FeeUSD)) > 1e-9 {
			return in, errors.New("invalid filled live-policy mirror row")
		}
	default:
		return in, errors.New("invalid new live-policy mirror state")
	}
	for _, v := range []float64{in.SignalPrice, in.LimitPrice, in.RequestedQty, in.FilledQty,
		in.FillPrice, in.TouchDepth, in.FeeUSD, in.CostUSD, in.ProofMean, in.ProofLower,
		in.ProofFeePC, in.SizingBankroll, in.SizingTargetUSD} {
		if !validLivePolicyMirrorFloat(v) {
			return in, errors.New("non-finite live-policy mirror value")
		}
	}
	if in.SignalPrice < 0 || in.SignalPrice >= 1 || in.LimitPrice < 0 || in.LimitPrice >= 1 ||
		in.TouchDepth < 0 || in.FeeUSD < 0 || in.CostUSD < 0 {
		return in, errors.New("out-of-range live-policy mirror value")
	}
	return in, nil
}

func sameLivePolicyMirrorTerminal(existing, incoming LivePolicyMirrorRow) bool {
	// A retry of the original fill may arrive after the one-way settlement worker has already
	// settled that row. Settlement does not change any captured execution economics, so the
	// original terminal fill is still the same idempotent write.
	existingState := existing.State
	if existingState == LivePolicyMirrorSettled && incoming.State == LivePolicyMirrorFilled {
		existingState = LivePolicyMirrorFilled
	}
	return existing.CandidateID == incoming.CandidateID &&
		existing.IntentKey == incoming.IntentKey &&
		existing.ObservedAt.Equal(incoming.ObservedAt) &&
		existing.ProcessedAt.Equal(incoming.ProcessedAt) &&
		existing.Venue == incoming.Venue &&
		existing.Ticker == incoming.Ticker &&
		existing.Title == incoming.Title &&
		existing.Side == incoming.Side &&
		existing.SystemID == incoming.SystemID &&
		existing.Route == incoming.Route &&
		existing.ModelVersion == incoming.ModelVersion &&
		existingState == incoming.State &&
		existing.Reason == incoming.Reason &&
		existing.SignalPrice == incoming.SignalPrice &&
		existing.LimitPrice == incoming.LimitPrice &&
		existing.RequestedQty == incoming.RequestedQty &&
		existing.FilledQty == incoming.FilledQty &&
		existing.FillPrice == incoming.FillPrice &&
		existing.TouchDepth == incoming.TouchDepth &&
		existing.FeeUSD == incoming.FeeUSD &&
		existing.FeeSource == incoming.FeeSource &&
		existing.CostUSD == incoming.CostUSD &&
		existing.ProofMean == incoming.ProofMean &&
		existing.ProofLower == incoming.ProofLower &&
		existing.ProofFeePC == incoming.ProofFeePC &&
		existing.SizingBankroll == incoming.SizingBankroll &&
		existing.SizingTargetUSD == incoming.SizingTargetUSD &&
		existing.EventKey == incoming.EventKey &&
		existing.ClusterKey == incoming.ClusterKey &&
		existing.Crypto == incoming.Crypto &&
		existing.DelayMS == incoming.DelayMS &&
		existing.WireDelayMS == incoming.WireDelayMS &&
		existing.BookSource == incoming.BookSource
}

// InsertLivePolicyMirror persists one terminal simulation decision. A filled row and the accepted
// UTC-day turnover increment are committed together. CandidateID is also the idempotency key:
// an exact retry returns the original row ID without changing turnover, while a conflicting reuse
// remains an error.
func (s *Store) InsertLivePolicyMirror(ctx context.Context, in LivePolicyMirrorRow) (int64, error) {
	in, err := normalizeLivePolicyMirrorRow(in)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `INSERT INTO live_policy_mirror(
candidate_id,intent_key,observed_ts,processed_ts,venue,ticker,title,side,system_id,route,model_version,state,
reason,signal_price,limit_price,requested_qty,filled_qty,fill_price,touch_depth,fee_usd,fee_source,
cost_usd,proof_mean,proof_lower,proof_fee_pc,sizing_bankroll,sizing_target_usd,event_key,cluster_key,
is_crypto,delay_ms,wire_delay_ms,book_source,updated_ts
) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(candidate_id) DO NOTHING`,
		in.CandidateID, in.IntentKey, in.ObservedAt.UTC().Format(time.RFC3339Nano),
		in.ProcessedAt.UTC().Format(time.RFC3339Nano), in.Venue, in.Ticker, in.Title, in.Side,
		in.SystemID, in.Route, in.ModelVersion, in.State, in.Reason, in.SignalPrice, in.LimitPrice,
		in.RequestedQty, in.FilledQty, in.FillPrice, in.TouchDepth, in.FeeUSD, in.FeeSource,
		in.CostUSD, in.ProofMean, in.ProofLower, in.ProofFeePC, in.SizingBankroll,
		in.SizingTargetUSD, in.EventKey, in.ClusterKey, boolInt(in.Crypto), in.DelayMS,
		in.WireDelayMS, in.BookSource, now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return 0, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if affected == 0 {
		existing, queryErr := scanLivePolicyMirrorRow(tx.QueryRowContext(ctx,
			`SELECT `+livePolicyMirrorColumns+` FROM live_policy_mirror WHERE candidate_id=?`,
			in.CandidateID))
		if queryErr != nil {
			return 0, queryErr
		}
		if !sameLivePolicyMirrorTerminal(existing, in) {
			return 0, errors.New("live-policy mirror candidate identity collision")
		}
		return existing.ID, nil
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if in.State == LivePolicyMirrorFilled {
		day := in.ProcessedAt.UTC().Format("2006-01-02")
		if _, err = tx.ExecContext(ctx, `UPDATE live_policy_mirror_meta SET
day_key=?,day_turnover_usd=CASE WHEN day_key=? THEN day_turnover_usd+? ELSE ? END,updated_ts=?
WHERE id=1 AND epoch_ts<=?`, day, day, in.CostUSD, in.CostUSD, now.Format(time.RFC3339Nano),
			in.ObservedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

func scanLivePolicyMirrorRow(scanner interface{ Scan(...any) error }) (LivePolicyMirrorRow, error) {
	var out LivePolicyMirrorRow
	var observed, processed, settled string
	var crypto, settlementKnown int
	err := scanner.Scan(&out.ID, &out.CandidateID, &out.IntentKey, &observed, &processed,
		&out.Venue, &out.Ticker, &out.Title, &out.Side, &out.SystemID, &out.Route,
		&out.ModelVersion, &out.State,
		&out.Reason, &out.SignalPrice, &out.LimitPrice, &out.RequestedQty, &out.FilledQty,
		&out.FillPrice, &out.TouchDepth, &out.FeeUSD, &out.FeeSource, &out.CostUSD,
		&out.ProofMean, &out.ProofLower, &out.ProofFeePC, &out.SizingBankroll,
		&out.SizingTargetUSD, &out.EventKey, &out.ClusterKey, &crypto, &out.DelayMS,
		&out.WireDelayMS, &out.BookSource, &settlementKnown, &out.SettlementValue, &settled,
		&out.SettlementSrc, &out.SettlementHash, &out.RealizedNet)
	if err != nil {
		return out, err
	}
	out.ObservedAt, err = time.Parse(time.RFC3339Nano, observed)
	if err != nil {
		return out, fmt.Errorf("parse mirror observed time: %w", err)
	}
	out.ProcessedAt, err = time.Parse(time.RFC3339Nano, processed)
	if err != nil {
		return out, fmt.Errorf("parse mirror processed time: %w", err)
	}
	out.Crypto = crypto != 0
	out.SettlementKnown = settlementKnown != 0
	if settled != "" {
		out.SettledAt, err = time.Parse(time.RFC3339Nano, settled)
		if err != nil {
			return out, fmt.Errorf("parse mirror settled time: %w", err)
		}
	}
	return out, nil
}

const livePolicyMirrorColumns = `id,candidate_id,intent_key,observed_ts,processed_ts,venue,ticker,
title,side,system_id,route,model_version,state,reason,signal_price,limit_price,requested_qty,filled_qty,fill_price,
touch_depth,fee_usd,fee_source,cost_usd,proof_mean,proof_lower,proof_fee_pc,sizing_bankroll,
sizing_target_usd,event_key,cluster_key,is_crypto,delay_ms,wire_delay_ms,book_source,
settlement_known,settlement_value,settled_ts,settlement_source,settlement_hash,realized_net`

func (s *Store) ListLivePolicyMirror(ctx context.Context, limit int) ([]LivePolicyMirrorRow, error) {
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+livePolicyMirrorColumns+
		` FROM live_policy_mirror ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]LivePolicyMirrorRow, 0)
	for rows.Next() {
		row, scanErr := scanLivePolicyMirrorRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// ListLivePolicyMirrorCurrent is the current $400 experiment display. The append-only all-history
// reader above remains available for audits and settlement reconciliation.
func (s *Store) ListLivePolicyMirrorCurrent(ctx context.Context, limit int) ([]LivePolicyMirrorRow, error) {
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+livePolicyMirrorColumns+`
FROM live_policy_mirror
WHERE observed_ts >= (SELECT epoch_ts FROM live_policy_mirror_meta WHERE id=1)
ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]LivePolicyMirrorRow, 0)
	for rows.Next() {
		row, scanErr := scanLivePolicyMirrorRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) OpenLivePolicyMirror(ctx context.Context) ([]LivePolicyMirrorRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+livePolicyMirrorColumns+
		` FROM live_policy_mirror WHERE state='filled' ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]LivePolicyMirrorRow, 0)
	for rows.Next() {
		row, scanErr := scanLivePolicyMirrorRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// OpenLivePolicyMirrorCurrent excludes pre-reset fills from current cash/exposure while the
// all-history reader keeps them settleable in the background.
func (s *Store) OpenLivePolicyMirrorCurrent(ctx context.Context) ([]LivePolicyMirrorRow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+livePolicyMirrorColumns+`
FROM live_policy_mirror
WHERE state='filled' AND observed_ts >= (SELECT epoch_ts FROM live_policy_mirror_meta WHERE id=1)
ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]LivePolicyMirrorRow, 0)
	for rows.Next() {
		row, scanErr := scanLivePolicyMirrorRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) LivePolicyMirrorIntentExistsSince(ctx context.Context, key string, since time.Time) (bool, error) {
	meta, err := s.LivePolicyMirrorMeta(ctx)
	if err != nil {
		return false, err
	}
	if meta.EpochAt.After(since) {
		since = meta.EpochAt
	}
	var n int
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM live_policy_mirror
WHERE intent_key=? AND observed_ts>=? AND state IN ('filled','settled')`, strings.TrimSpace(key),
		since.UTC().Format(time.RFC3339Nano)).Scan(&n)
	return n > 0, err
}

func (s *Store) LivePolicyMirrorMeta(ctx context.Context) (LivePolicyMirrorMeta, error) {
	var out LivePolicyMirrorMeta
	var epoch, updated string
	err := s.db.QueryRowContext(ctx, `SELECT epoch_ts,seed_usd,peak_nav_usd,day_key,
day_turnover_usd,updated_ts FROM live_policy_mirror_meta WHERE id=1`).Scan(
		&epoch, &out.SeedUSD, &out.PeakNAVUSD, &out.DayKey, &out.DayTurnover, &updated)
	if err != nil {
		return out, err
	}
	out.EpochAt, err = time.Parse(time.RFC3339Nano, epoch)
	if err != nil {
		return out, err
	}
	out.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
	return out, err
}

// ResetLivePolicyMirrorPortfolio advances the fake-money accounting epoch and resets its $400
// cash/peak/turnover state. Evidence rows remain append-only. Old open fills may still settle via
// OpenLivePolicyMirror, but current readers exclude them by observed_ts.
func (s *Store) ResetLivePolicyMirrorPortfolio(ctx context.Context, epoch time.Time) (LivePolicyMirrorResetReceipt, error) {
	var out LivePolicyMirrorResetReceipt
	if epoch.IsZero() {
		epoch = time.Now().UTC()
	}
	epoch = epoch.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var current string
	if err = tx.QueryRowContext(ctx, `SELECT epoch_ts FROM live_policy_mirror_meta WHERE id=1`).Scan(&current); err != nil {
		return out, err
	}
	currentAt, err := time.Parse(time.RFC3339Nano, current)
	if err != nil {
		return out, fmt.Errorf("parse live-policy mirror epoch: %w", err)
	}
	if epoch.Before(currentAt) {
		return out, errors.New("live-policy mirror epoch cannot move backwards")
	}
	cut := epoch.Format(time.RFC3339Nano)
	if err = tx.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(CASE WHEN state='filled' THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN state='settled' THEN 1 ELSE 0 END),0)
FROM live_policy_mirror WHERE observed_ts<?`, cut).Scan(
		&out.ExcludedRows, &out.ExcludedOpen, &out.ExcludedSettled); err != nil {
		return out, err
	}
	now := time.Now().UTC()
	if _, err = tx.ExecContext(ctx, `UPDATE live_policy_mirror_meta SET
epoch_ts=?,seed_usd=?,peak_nav_usd=?,day_key=?,day_turnover_usd=0,updated_ts=? WHERE id=1`,
		cut, LivePolicyMirrorSeedUSD, LivePolicyMirrorSeedUSD, now.Format("2006-01-02"),
		now.Format(time.RFC3339Nano)); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return out, err
	}
	out.EpochAt, out.SeedUSD = epoch, LivePolicyMirrorSeedUSD
	return out, nil
}

// LivePolicyMirrorRollup returns current-epoch accounting. Recent-row display limits must never
// truncate fake cash, risk, P&L, or briefing counts once the experiment has many rejections.
func (s *Store) LivePolicyMirrorRollup(ctx context.Context) (LivePolicyMirrorRollup, error) {
	out := LivePolicyMirrorRollup{
		StateCounts:  make(map[string]int),
		ReasonCounts: make(map[string]int),
	}
	rows, err := s.db.QueryContext(ctx, `SELECT state,reason,COUNT(*),
COALESCE(SUM(realized_net),0),COALESCE(SUM(fee_usd),0)
FROM live_policy_mirror
WHERE observed_ts >= (SELECT epoch_ts FROM live_policy_mirror_meta WHERE id=1)
GROUP BY state,reason`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var state, reason string
		var count int
		var net, fees float64
		if err = rows.Scan(&state, &reason, &count, &net, &fees); err != nil {
			return out, err
		}
		out.StateCounts[state] += count
		if reason != "" {
			out.ReasonCounts[reason] += count
		}
		out.SettledNet += net
		out.FeesUSD += fees
	}
	return out, rows.Err()
}

func (s *Store) UpdateLivePolicyMirrorPeak(ctx context.Context, nav float64, at, epoch time.Time) error {
	if nav <= 0 || !validLivePolicyMirrorFloat(nav) || epoch.IsZero() {
		return errors.New("invalid live-policy mirror NAV")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	// The bootstrap row is written by SQLite strftime with fixed millisecond
	// precision (for example ".120Z"), while Go's RFC3339Nano formatter trims
	// trailing zeroes (".12Z"). Comparing those strings directly made the
	// epoch guard randomly reject about one update in ten. Read and compare the
	// instant semantically, then retain the exact stored text for the atomic
	// compare-and-set so a concurrent reset still cannot inherit an old peak.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var storedEpoch string
	if err = tx.QueryRowContext(ctx,
		`SELECT epoch_ts FROM live_policy_mirror_meta WHERE id=1`).Scan(&storedEpoch); err != nil {
		return err
	}
	storedAt, err := time.Parse(time.RFC3339Nano, storedEpoch)
	if err != nil {
		return fmt.Errorf("parse live-policy mirror epoch: %w", err)
	}
	if !storedAt.Equal(epoch.UTC()) {
		return nil
	}
	if _, err = tx.ExecContext(ctx, `UPDATE live_policy_mirror_meta SET
peak_nav_usd=MAX(peak_nav_usd,?),updated_ts=? WHERE id=1 AND epoch_ts=?`,
		nav, at.UTC().Format(time.RFC3339Nano), storedEpoch); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SettleLivePolicyMirror(ctx context.Context, id int64, yesValue float64,
	settledAt time.Time, source, hash string) (bool, error) {
	if id <= 0 || yesValue < 0 || yesValue > 1 || !validLivePolicyMirrorFloat(yesValue) ||
		settledAt.IsZero() || strings.TrimSpace(source) == "" || strings.TrimSpace(hash) == "" {
		return false, errors.New("invalid live-policy mirror settlement")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	row, err := scanLivePolicyMirrorRow(tx.QueryRowContext(ctx,
		`SELECT `+livePolicyMirrorColumns+` FROM live_policy_mirror WHERE id=?`, id))
	if err != nil {
		return false, err
	}
	if row.State == LivePolicyMirrorSettled {
		same := row.SettlementKnown && math.Abs(row.SettlementValue-yesValue) <= 1e-12 &&
			row.SettledAt.Equal(settledAt) && row.SettlementSrc == strings.TrimSpace(source) &&
			row.SettlementHash == strings.TrimSpace(hash)
		if !same {
			return false, errors.New("live-policy mirror settlement cannot be rewritten")
		}
		return false, nil
	}
	if row.State != LivePolicyMirrorFilled {
		return false, errors.New("live-policy mirror settlement requires an open fill")
	}
	sideValue := yesValue
	if row.Side == "NO" {
		sideValue = 1 - yesValue
	}
	net := row.FilledQty*sideValue - row.CostUSD
	res, err := tx.ExecContext(ctx, `UPDATE live_policy_mirror SET state='settled',
settlement_known=1,settlement_value=?,settled_ts=?,settlement_source=?,settlement_hash=?,
realized_net=?,updated_ts=? WHERE id=? AND state='filled'`,
		yesValue, settledAt.UTC().Format(time.RFC3339Nano), strings.TrimSpace(source),
		strings.TrimSpace(hash), net, time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil || affected != 1 {
		if err == nil {
			err = errors.New("live-policy mirror settlement lost its open row")
		}
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
