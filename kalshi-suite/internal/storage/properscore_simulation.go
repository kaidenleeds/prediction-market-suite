package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// ProperScoreSimulation is a zero-authority immediate-taker replay receipt. It is deliberately
// separate from actual Paper/LIVE fills: the row may inform research, but DB constraints pin every
// authority flag to zero and only its filled delta may advance the next simulated position.
type ProperScoreSimulation struct {
	Observed                                                   time.Time
	Slot, System, Transform, StrategyMode, Cohort              string
	Platform, Ticker, ForecastSource, ForecastVersion          string
	BookSource, SourceClockID, FillStatus                      string
	PriorPosition, TargetPosition, RequestedDelta, FilledDelta float64
	CancelledDelta, PostPosition, GrossCashFlow, FeeTotal      float64
	ExpectedValueDelta, DecisionLatencyMS                      float64
	Legs                                                       any
}

func (s *Store) InsertProperScoreSimulation(ctx context.Context, v ProperScoreSimulation) (int64, bool, error) {
	if v.Observed.IsZero() {
		v.Observed = time.Now().UTC()
	}
	if v.Slot == "" {
		v.Slot = researchSlot(v.Observed, time.Hour)
	}
	v.Transform = strings.ToLower(strings.TrimSpace(v.Transform))
	v.StrategyMode = strings.ToLower(strings.TrimSpace(v.StrategyMode))
	v.Platform = strings.ToLower(strings.TrimSpace(v.Platform))
	v.FillStatus = strings.ToLower(strings.TrimSpace(v.FillStatus))
	for _, value := range []float64{v.PriorPosition, v.TargetPosition, v.RequestedDelta, v.FilledDelta,
		v.CancelledDelta, v.PostPosition, v.GrossCashFlow, v.FeeTotal, v.ExpectedValueDelta,
		v.DecisionLatencyMS} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, false, errors.New("non-finite proper-score simulation")
		}
	}
	if v.System != "proper-score-"+v.Transform ||
		(v.Transform != "brier" && v.Transform != "log" && v.Transform != "spherical") ||
		(v.StrategyMode != "fundamental" && v.StrategyMode != "momentum") ||
		(v.Platform != "kalshi" && v.Platform != "polyus") || strings.TrimSpace(v.Ticker) == "" ||
		strings.TrimSpace(v.Cohort) == "" || strings.TrimSpace(v.ForecastSource) == "" ||
		strings.TrimSpace(v.ForecastVersion) == "" || strings.TrimSpace(v.BookSource) == "" ||
		strings.TrimSpace(v.SourceClockID) == "" || v.DecisionLatencyMS < 0 ||
		(v.FillStatus != "filled" && v.FillStatus != "partial" && v.FillStatus != "cancelled" &&
			v.FillStatus != "noop" && v.FillStatus != "blocked") ||
		math.Abs((v.PriorPosition+v.FilledDelta)-v.PostPosition) > 1e-8 ||
		math.Abs((v.RequestedDelta-v.FilledDelta)-v.CancelledDelta) > 1e-8 {
		return 0, false, errors.New("invalid proper-score simulated position transition")
	}
	if math.Abs(v.FilledDelta) > math.Abs(v.RequestedDelta)+1e-8 ||
		v.RequestedDelta*v.FilledDelta < -1e-10 {
		return 0, false, errors.New("proper-score simulated fill exceeds or opposes requested delta")
	}
	legs, err := json.Marshal(v.Legs)
	if err != nil || len(legs) == 0 || string(legs) == "null" {
		legs = []byte("[]")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_proper_score_simulated_positions(
observed_ts,slot,system_name,transform,strategy_mode,cohort,platform,ticker,forecast_source,
forecast_version,prior_position,target_position,requested_delta,filled_delta,cancelled_delta,
post_position,gross_cash_flow,fee_total,expected_value_delta,fill_status,book_source,source_clock_id,
decision_latency_ms,legs_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		v.Observed.UTC().Format(time.RFC3339Nano), v.Slot, v.System, v.Transform, v.StrategyMode,
		v.Cohort, v.Platform, v.Ticker, v.ForecastSource, v.ForecastVersion, v.PriorPosition,
		v.TargetPosition, v.RequestedDelta, v.FilledDelta, v.CancelledDelta, v.PostPosition,
		v.GrossCashFlow, v.FeeTotal, v.ExpectedValueDelta, v.FillStatus, v.BookSource,
		v.SourceClockID, v.DecisionLatencyMS, string(legs))
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, false, err
	}
	id := int64(0)
	if n > 0 {
		id, err = res.LastInsertId()
	}
	return id, n > 0, err
}

func (s *Store) ProperScoreSimulatedPosition(ctx context.Context, mode, transform, platform, ticker,
	forecastSource, forecastVersion string) (float64, error) {
	return s.ProperScoreSimulatedPositionBefore(ctx, mode, transform, platform, ticker,
		forecastSource, forecastVersion, "")
}

func (s *Store) ProperScoreSimulatedPositionBefore(ctx context.Context, mode, transform, platform, ticker,
	forecastSource, forecastVersion, beforeSlot string) (float64, error) {
	var position float64
	query := `SELECT post_position
FROM research_proper_score_simulated_positions WHERE strategy_mode=? AND transform=?
 AND platform=? AND ticker=? AND forecast_source=? AND forecast_version=?`
	args := []any{strings.ToLower(strings.TrimSpace(mode)),
		strings.ToLower(strings.TrimSpace(transform)), strings.ToLower(strings.TrimSpace(platform)),
		strings.TrimSpace(ticker), strings.TrimSpace(forecastSource), strings.TrimSpace(forecastVersion)}
	if strings.TrimSpace(beforeSlot) != "" {
		query += " AND slot<?"
		args = append(args, strings.TrimSpace(beforeSlot))
	}
	query += " ORDER BY id DESC LIMIT 1"
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&position)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("proper-score simulated position: %w", err)
	}
	return position, nil
}
