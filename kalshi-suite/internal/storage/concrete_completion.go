package storage

// Final prospective contracts for the concrete research systems. Timed attention exits and
// misses are append-only. Carry assumptions are immutable prior-only receipts: a later outcome
// can never repair or replace a missing input for an earlier observation.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"time"
)

const concreteCompletionSchema = `
CREATE TABLE IF NOT EXISTS research_attention_markouts (
 observation_id INTEGER NOT NULL,
 horizon_s INTEGER NOT NULL CHECK(horizon_s IN (60,300)),
 target_ts TEXT NOT NULL,
 captured_ts TEXT NOT NULL,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 exit_bid REAL NOT NULL CHECK(exit_bid>0 AND exit_bid<1),
 exit_depth REAL NOT NULL CHECK(exit_depth>=1),
 tick_size REAL NOT NULL CHECK(tick_size>0),
 quote_age_s REAL NOT NULL CHECK(quote_age_s>=0),
 exit_fee_pc REAL NOT NULL,
 entry_all_in REAL NOT NULL CHECK(entry_all_in>0),
 executable_markout REAL NOT NULL,
 book_source TEXT NOT NULL,
 fee_source TEXT NOT NULL,
 source_clock_id TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 PRIMARY KEY(observation_id,horizon_s),
 FOREIGN KEY(observation_id) REFERENCES research_system_observations(id)
);
CREATE INDEX IF NOT EXISTS idx_rattention_mark_ts ON research_attention_markouts(target_ts,observation_id);
CREATE TRIGGER IF NOT EXISTS research_attention_markouts_no_update BEFORE UPDATE ON research_attention_markouts BEGIN SELECT RAISE(ABORT,'immutable attention markout'); END;
CREATE TRIGGER IF NOT EXISTS research_attention_markouts_no_delete BEFORE DELETE ON research_attention_markouts BEGIN SELECT RAISE(ABORT,'immutable attention markout'); END;

CREATE TABLE IF NOT EXISTS research_attention_markout_misses (
 observation_id INTEGER NOT NULL,
 horizon_s INTEGER NOT NULL CHECK(horizon_s IN (60,300)),
 target_ts TEXT NOT NULL,
 missed_ts TEXT NOT NULL,
 reason TEXT NOT NULL,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 PRIMARY KEY(observation_id,horizon_s),
 FOREIGN KEY(observation_id) REFERENCES research_system_observations(id)
);
CREATE INDEX IF NOT EXISTS idx_rattention_miss_ts ON research_attention_markout_misses(target_ts,observation_id);
CREATE TRIGGER IF NOT EXISTS research_attention_markout_misses_no_update BEFORE UPDATE ON research_attention_markout_misses BEGIN SELECT RAISE(ABORT,'immutable attention markout miss'); END;
CREATE TRIGGER IF NOT EXISTS research_attention_markout_misses_no_delete BEFORE DELETE ON research_attention_markout_misses BEGIN SELECT RAISE(ABORT,'immutable attention markout miss'); END;

CREATE TABLE IF NOT EXISTS research_carry_frozen_inputs (
 freeze_id TEXT PRIMARY KEY,
 frozen_ts TEXT NOT NULL,
 source_cutoff_ts TEXT NOT NULL,
 source_signal_id INTEGER NOT NULL,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 entry_all_in REAL NOT NULL CHECK(entry_all_in>0),
 expected_hold_days REAL NOT NULL CHECK(expected_hold_days>=0),
 void_prior_n INTEGER NOT NULL CHECK(void_prior_n>=0),
 void_prior_events INTEGER NOT NULL CHECK(void_prior_events>=0 AND void_prior_events<=void_prior_n),
 void_probability_upper REAL NOT NULL CHECK(void_probability_upper>=0 AND void_probability_upper<=1),
 void_reserve REAL NOT NULL CHECK(void_reserve>=0),
 void_policy TEXT NOT NULL,
 benchmark_kind TEXT NOT NULL CHECK(benchmark_kind IN ('sealed_route','nominal_cash_floor','blocked')),
 benchmark_receipt_id TEXT NOT NULL DEFAULT '',
 benchmark_rate_per_dollar_day REAL NOT NULL CHECK(benchmark_rate_per_dollar_day>=0),
 benchmark_reserve REAL NOT NULL CHECK(benchmark_reserve>=0),
 benchmark_justification TEXT NOT NULL,
 input_state TEXT NOT NULL CHECK(input_state IN ('ready','blocked')),
 blocker TEXT NOT NULL,
 spec_hash TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(source_signal_id,venue,ticker,side)
);
CREATE INDEX IF NOT EXISTS idx_rcarry_freeze ON research_carry_frozen_inputs(frozen_ts,freeze_id);
CREATE TRIGGER IF NOT EXISTS research_carry_frozen_inputs_no_update BEFORE UPDATE ON research_carry_frozen_inputs BEGIN SELECT RAISE(ABORT,'immutable carry input freeze'); END;
CREATE TRIGGER IF NOT EXISTS research_carry_frozen_inputs_no_delete BEFORE DELETE ON research_carry_frozen_inputs BEGIN SELECT RAISE(ABORT,'immutable carry input freeze'); END;
`

func migrateConcreteCompletionSchema(db *sql.DB) error {
	_, err := db.Exec(concreteCompletionSchema)
	return err
}

type AttentionMarkoutDue struct {
	ObservationID       int64
	Observed, Target    time.Time
	Horizon             int
	Venue, Ticker, Side string
	EntryCost, EntryFee float64
}

func (s *Store) PendingAttentionMarkouts(ctx context.Context, now time.Time, limit int) ([]AttentionMarkoutDue, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `WITH horizons(h) AS (VALUES(60),(300))
SELECT o.id,o.observed_ts,h.h,o.venue,o.ticker,o.side,o.executable_cost,o.exact_fee
FROM research_system_observations o CROSS JOIN horizons h
WHERE o.system_id='attention-spillover-graph' AND o.ticker!='' AND o.side IN ('YES','NO')
 AND datetime(o.observed_ts,'+'||h.h||' seconds')<=datetime(?)
 AND NOT EXISTS(SELECT 1 FROM research_attention_markouts m WHERE m.observation_id=o.id AND m.horizon_s=h.h)
 AND NOT EXISTS(SELECT 1 FROM research_attention_markout_misses x WHERE x.observation_id=o.id AND x.horizon_s=h.h)
ORDER BY o.observed_ts,o.id,h.h LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AttentionMarkoutDue
	for rows.Next() {
		var v AttentionMarkoutDue
		var observed string
		if err = rows.Scan(&v.ObservationID, &observed, &v.Horizon, &v.Venue, &v.Ticker, &v.Side, &v.EntryCost, &v.EntryFee); err != nil {
			return nil, err
		}
		v.Observed, err = time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			return nil, err
		}
		v.Target = v.Observed.Add(time.Duration(v.Horizon) * time.Second)
		out = append(out, v)
	}
	return out, rows.Err()
}

type AttentionMarkout struct {
	ObservationID                                                    int64
	Horizon                                                          int
	Target, Captured                                                 time.Time
	Venue, Ticker, Side                                              string
	ExitBid, ExitDepth, Tick, QuoteAge, ExitFee, EntryAllIn, Markout float64
	BookSource, FeeSource, SourceClockID                             string
	Evidence                                                         any
}

func (s *Store) InsertAttentionMarkout(ctx context.Context, v AttentionMarkout) (bool, error) {
	if v.ObservationID <= 0 || (v.Horizon != 60 && v.Horizon != 300) || v.Target.IsZero() || v.Captured.IsZero() ||
		(v.Venue != "kalshi" && v.Venue != "polyus") || v.Ticker == "" || (v.Side != "YES" && v.Side != "NO") ||
		v.ExitBid <= 0 || v.ExitBid >= 1 || v.ExitDepth < 1 || v.Tick <= 0 || v.QuoteAge < 0 || v.EntryAllIn <= 0 ||
		v.BookSource == "" || v.FeeSource == "" || v.SourceClockID == "" || !step7Finite(v.ExitBid, v.ExitDepth, v.Tick,
		v.QuoteAge, v.ExitFee, v.EntryAllIn, v.Markout) {
		return false, errors.New("invalid attention markout")
	}
	evidence, err := json.Marshal(v.Evidence)
	if err != nil {
		return false, err
	}
	if len(evidence) == 0 || string(evidence) == "null" {
		evidence = []byte("{}")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_attention_markouts(
observation_id,horizon_s,target_ts,captured_ts,venue,ticker,side,exit_bid,exit_depth,tick_size,
quote_age_s,exit_fee_pc,entry_all_in,executable_markout,book_source,fee_source,source_clock_id,
evidence_json,funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		v.ObservationID, v.Horizon, step7Time(v.Target), step7Time(v.Captured), v.Venue, v.Ticker, v.Side,
		v.ExitBid, v.ExitDepth, v.Tick, v.QuoteAge, v.ExitFee, v.EntryAllIn, v.Markout, v.BookSource, v.FeeSource,
		v.SourceClockID, string(evidence))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) InsertAttentionMarkoutMiss(ctx context.Context, v AttentionMarkoutDue, missed time.Time, reason string) (bool, error) {
	reason = strings.TrimSpace(reason)
	if v.ObservationID <= 0 || (v.Horizon != 60 && v.Horizon != 300) || v.Target.IsZero() || missed.IsZero() || reason == "" {
		return false, errors.New("invalid attention markout miss")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_attention_markout_misses(
observation_id,horizon_s,target_ts,missed_ts,reason,funded,paper_authority,live_authority)
SELECT ?,?,?,?,?,0,0,0 WHERE NOT EXISTS(SELECT 1 FROM research_attention_markouts WHERE observation_id=? AND horizon_s=?)`,
		v.ObservationID, v.Horizon, step7Time(v.Target), step7Time(missed), reason, v.ObservationID, v.Horizon)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type CarryFreezeRequest struct {
	FreezeID                     string
	Frozen                       time.Time
	SignalID                     int64
	Venue, Ticker, Side          string
	EntryAllIn, ExpectedHoldDays float64
	Evidence                     any
}

type CarryFrozenInput struct {
	FreezeID, Venue, Ticker, Side, VoidPolicy, BenchmarkKind, BenchmarkReceiptID string
	BenchmarkJustification, InputState, Blocker, SpecHash                        string
	Frozen, SourceCutoff                                                         time.Time
	SignalID, VoidPriorN, VoidPriorEvents                                        int64
	EntryAllIn, ExpectedHoldDays, VoidProbabilityUpper, VoidReserve              float64
	BenchmarkRate, BenchmarkReserve                                              float64
}

func wilsonUpper(events, n int64) float64 {
	if n <= 0 {
		return 1
	}
	z := 1.959963984540054
	p := float64(events) / float64(n)
	den := 1 + z*z/float64(n)
	center := p + z*z/(2*float64(n))
	spread := z * math.Sqrt((p*(1-p)+z*z/(4*float64(n)))/float64(n))
	return math.Min(1, (center+spread)/den)
}

func (s *Store) FreezeCarryInput(ctx context.Context, v CarryFreezeRequest) (CarryFrozenInput, bool, error) {
	var out CarryFrozenInput
	v.Venue, v.Ticker, v.Side = strings.ToLower(strings.TrimSpace(v.Venue)), strings.TrimSpace(v.Ticker), strings.ToUpper(strings.TrimSpace(v.Side))
	if v.FreezeID == "" || v.Frozen.IsZero() || v.SignalID <= 0 || (v.Venue != "kalshi" && v.Venue != "polyus") || v.Ticker == "" ||
		(v.Side != "YES" && v.Side != "NO") || v.EntryAllIn <= 0 || v.ExpectedHoldDays < 0 || !step7Finite(v.EntryAllIn, v.ExpectedHoldDays) {
		return out, false, errors.New("invalid carry freeze request")
	}
	var n, voids int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(u.status IN ('voided','censored')),0)
FROM research_system_payoff_updates u JOIN research_system_observations o ON o.id=u.observation_id
WHERE o.venue=? AND o.route='taker' AND u.observed_ts<? AND u.status IN ('settled','voided','censored')
 AND u.source_artifact!='' AND u.source_hash!=''`, v.Venue, v.Frozen.UTC().Format(time.RFC3339Nano)).Scan(&n, &voids)
	if err != nil {
		return out, false, err
	}
	voidUpper := wilsonUpper(voids, n)
	voidReserve := voidUpper * v.EntryAllIn
	benchmarkKind, benchmarkID, benchmarkWhy := "nominal_cash_floor", "", "no positive sealed route economics receipt existed before freeze; nominal uninvested cash has a conservative zero-dollar return floor and no external yield is assumed"
	benchmarkRate := 0.0
	var receiptID string
	var netPerDay, capitalHours float64
	err = s.db.QueryRowContext(ctx, `SELECT receipt_id,net_per_day_lower,capital_dollar_hours_per_day
FROM research_sealed_route_economics WHERE venue=? AND created_ts<? AND net_per_day_lower>0
 AND capital_dollar_hours_per_day>0 ORDER BY (net_per_day_lower/(capital_dollar_hours_per_day/24.0)) DESC,receipt_id LIMIT 1`,
		v.Venue, v.Frozen.UTC().Format(time.RFC3339Nano)).Scan(&receiptID, &netPerDay, &capitalHours)
	if err == nil {
		benchmarkKind, benchmarkID = "sealed_route", receiptID
		benchmarkRate = netPerDay / (capitalHours / 24)
		benchmarkWhy = "best positive sealed preregistered route lower bound available strictly before freeze, normalized per capital-dollar-day"
	} else if !errors.Is(err, sql.ErrNoRows) {
		return out, false, err
	}
	state, blocker := "ready", ""
	if v.ExpectedHoldDays <= 0 {
		state, blocker, benchmarkKind = "blocked", "resolve_horizon_unknown_at_freeze", "blocked"
		benchmarkWhy = "expected capital time was not known at freeze; later values may not backfill this receipt"
	}
	benchmarkReserve := benchmarkRate * v.EntryAllIn * v.ExpectedHoldDays
	manifest := map[string]any{"freeze_id": v.FreezeID, "frozen_ts": v.Frozen.UTC(), "source_cutoff": v.Frozen.UTC(),
		"signal_id": v.SignalID, "venue": v.Venue, "ticker": v.Ticker, "side": v.Side, "entry_all_in": v.EntryAllIn,
		"expected_hold_days": v.ExpectedHoldDays, "void_prior_n": n, "void_events": voids, "void_upper": voidUpper,
		"void_policy": "wilson95_prior_only_void_dispute_censor_full_all_in_loss_v1", "benchmark_kind": benchmarkKind,
		"benchmark_receipt": benchmarkID, "benchmark_rate": benchmarkRate, "benchmark_justification": benchmarkWhy,
		"input_state": state, "blocker": blocker, "evidence": v.Evidence}
	specHash := R138HashJSON(manifest)
	evidence, _ := json.Marshal(manifest)
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_carry_frozen_inputs(
freeze_id,frozen_ts,source_cutoff_ts,source_signal_id,venue,ticker,side,entry_all_in,expected_hold_days,
void_prior_n,void_prior_events,void_probability_upper,void_reserve,void_policy,benchmark_kind,
benchmark_receipt_id,benchmark_rate_per_dollar_day,benchmark_reserve,benchmark_justification,input_state,
blocker,spec_hash,evidence_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, v.FreezeID, step7Time(v.Frozen), step7Time(v.Frozen),
		v.SignalID, v.Venue, v.Ticker, v.Side, v.EntryAllIn, v.ExpectedHoldDays, n, voids, voidUpper, voidReserve,
		"wilson95_prior_only_void_dispute_censor_full_all_in_loss_v1", benchmarkKind, benchmarkID, benchmarkRate, benchmarkReserve,
		benchmarkWhy, state, blocker, specHash, string(evidence))
	if err != nil {
		return out, false, err
	}
	insertedN, _ := res.RowsAffected()
	var frozenRaw, cutoffRaw string
	err = s.db.QueryRowContext(ctx, `SELECT freeze_id,frozen_ts,source_cutoff_ts,source_signal_id,venue,ticker,side,
entry_all_in,expected_hold_days,void_prior_n,void_prior_events,void_probability_upper,void_reserve,void_policy,
benchmark_kind,benchmark_receipt_id,benchmark_rate_per_dollar_day,benchmark_reserve,benchmark_justification,
input_state,blocker,spec_hash FROM research_carry_frozen_inputs WHERE freeze_id=?`, v.FreezeID).Scan(&out.FreezeID,
		&frozenRaw, &cutoffRaw, &out.SignalID, &out.Venue, &out.Ticker, &out.Side, &out.EntryAllIn,
		&out.ExpectedHoldDays, &out.VoidPriorN, &out.VoidPriorEvents, &out.VoidProbabilityUpper, &out.VoidReserve,
		&out.VoidPolicy, &out.BenchmarkKind, &out.BenchmarkReceiptID, &out.BenchmarkRate, &out.BenchmarkReserve,
		&out.BenchmarkJustification, &out.InputState, &out.Blocker, &out.SpecHash)
	if err != nil {
		return out, false, err
	}
	if out.SignalID != v.SignalID || out.Venue != v.Venue || out.Ticker != v.Ticker || out.Side != v.Side {
		return CarryFrozenInput{}, false, errors.New("carry freeze identity collision")
	}
	out.Frozen, _ = time.Parse(time.RFC3339Nano, frozenRaw)
	out.SourceCutoff, _ = time.Parse(time.RFC3339Nano, cutoffRaw)
	return out, insertedN > 0, nil
}
