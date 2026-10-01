package storage

// Behavioral-bias research reads only the prospective, side-specific book-v1 lane in signal_log.
// It deliberately does not create a new ledger: signal_log is the immutable observation tape and
// the server-side report is a read-only analysis over rows whose executable one-share economics
// were captured at observation time.

import (
	"context"
	"database/sql"
	"math"
	"strings"
	"time"
)

// BehavioralBiasObservation is one settled, one-share-taker observation with complete prospective
// book provenance. Optional feature pointers preserve the difference between a measured zero and
// an unavailable feature; callers must not turn unavailable values into cohort membership.
type BehavioralBiasObservation struct {
	ID                                              int64
	ObservedAt, ResolvedAt                          time.Time
	Venue, Ticker, CanonicalEvent, Side, SignalType string
	Category, MarketType, Kind, BookSource          string
	PricePath, ExecExpr                             string
	Bid, Ask, AskDepth, TakerFeePC                  float64
	BidKnown                                        bool
	Payout                                          float64
	ResolveHours                                    float64
	Momentum, Strength, Notional                    float64
	TraderCount                                     int
	Concentration, Imbalance                        float64
	IsLive                                          int
	InPlay                                          *int64
	SecsToStart                                     *int64
	Mom1h, Mom4h, FlowRatio15m                      *float64
	HoldersHHI, BookImb3                            *float64
}

// BehavioralBiasCoverage makes every rejected provenance class visible. Counts overlap only where
// explicitly named: TotalRows and BookV1Rows are universe counts; every BookV1 row then lands in
// exactly one of Eligible or one first-failure exclusion bucket below.
type BehavioralBiasCoverage struct {
	TotalRows            int64 `json:"signal_log_rows"`
	LegacyOrUnversioned  int64 `json:"legacy_or_unversioned"`
	BookV1Rows           int64 `json:"book_v1_rows"`
	BookNativeV2Rows     int64 `json:"book_native_v2_rows"`
	LegacyBookV1Rows     int64 `json:"excluded_legacy_book_v1_contract"`
	Eligible             int64 `json:"eligible_settled_book_v1"`
	MissingAsk           int64 `json:"excluded_missing_or_invalid_ask"`
	InsufficientDepth    int64 `json:"excluded_ask_depth_below_one"`
	MissingExactFee      int64 `json:"excluded_missing_or_invalid_exact_taker_fee"`
	MissingBookSource    int64 `json:"excluded_missing_book_source"`
	InvalidSide          int64 `json:"excluded_invalid_bought_side"`
	UnsettledOrBadPayout int64 `json:"excluded_unsettled_or_invalid_payout"`
	InvalidTimestamps    int64 `json:"excluded_invalid_timestamps"`
}

func parseBehavioralBiasTime(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func behavioralSidePayout(side string, settle sql.NullFloat64, won sql.NullInt64) (float64, bool) {
	side = strings.ToUpper(strings.TrimSpace(side))
	if side != "YES" && side != "NO" {
		return 0, false
	}
	// settle_val is authoritative for scalar and binary contracts. Legacy binary settlement uses
	// -1 and a side-relative won bit; accepting the bit here does not reconstruct a price or fee.
	if settle.Valid && !math.IsNaN(settle.Float64) && !math.IsInf(settle.Float64, 0) &&
		settle.Float64 >= 0 && settle.Float64 <= 1 {
		if side == "NO" {
			return 1 - settle.Float64, true
		}
		return settle.Float64, true
	}
	if won.Valid && (won.Int64 == 0 || won.Int64 == 1) {
		return float64(won.Int64), true
	}
	return 0, false
}

// BehavioralBiasProspectiveObservations returns only economically gradeable book-v1 rows. The
// exact stored book_taker_fee_pc is used even when it is zero; fee_pc, entry_price, fill_price,
// midpoint reconstruction and current fee schedules are intentionally not read.
func (s *Store) BehavioralBiasProspectiveObservations(ctx context.Context) ([]BehavioralBiasObservation, BehavioralBiasCoverage, error) {
	var cov BehavioralBiasCoverage
	// The former SUM(CASE book_feature_ver...) forced a 6GB table scan even though both needed
	// populations have covering indexes. Keep the same counts from one SQLite statement/snapshot;
	// SQLite can choose its narrowest all-row covering index, while the prospective count is pinned
	// to the tiny partial index.
	if err := s.db.QueryRowContext(ctx, `
SELECT
  (SELECT COUNT(*) FROM signal_log),
	(SELECT COUNT(*) FROM signal_log INDEXED BY idx_signal_book_v1 WHERE book_feature_ver=1),
	(SELECT COUNT(*) FROM signal_log INDEXED BY idx_signal_book_native_v2
	 WHERE book_feature_ver=1 AND pricing_version='book-native-v2' AND label_version='kalshi_start_clock_v2')`).
		Scan(&cov.TotalRows, &cov.BookV1Rows, &cov.BookNativeV2Rows); err != nil {
		return nil, cov, err
	}
	cov.LegacyOrUnversioned = cov.TotalRows - cov.BookV1Rows
	cov.LegacyBookV1Rows = cov.BookV1Rows - cov.BookNativeV2Rows

	// resolved,id is the physical order of idx_signal_book_v1. Report construction independently
	// selects the earliest ticker/cohort/day row and sorts cohorts, so this scan order is semantic-
	// neutral while preventing SQLite from choosing a rowid walk over the full legacy table.
	rows, err := s.db.QueryContext(ctx, `
SELECT s.id,s.ts,COALESCE(s.resolved_at,''),s.platform,s.ticker,
       COALESCE(
         NULLIF((SELECT 'sports:'||mg.game_id FROM market_game mg
                 WHERE mg.venue=s.platform AND mg.ticker=s.ticker AND mg.src='struct'),''),
         NULLIF((SELECT 'venue:'||mc.venue||':'||COALESCE(NULLIF(mc.event_key,''),mc.ticker)
                 FROM market_catalog mc WHERE mc.venue=s.platform AND mc.ticker=s.ticker),''),
         s.platform||':'||s.ticker),
       s.side,s.signal_type,
       book_bid,book_ask,book_ask_depth,book_taker_fee_pc,book_source,
       resolved,settle_val,won,category,market_type,kind,is_live,in_play,secs_to_start,
       resolve_hours,momentum,mom_1h,mom_4h,strength,notional,trader_count,
       concentration,flow_ratio_15m,holders_hhi,imbalance,book_imb3,
       price_path,exec_expr
FROM signal_log AS s INDEXED BY idx_signal_book_native_v2
WHERE s.book_feature_ver=1
  AND s.pricing_version='book-native-v2'
  AND s.label_version='kalshi_start_clock_v2'
ORDER BY s.resolved,s.id`)
	if err != nil {
		return nil, cov, err
	}
	defer rows.Close()

	out := make([]BehavioralBiasObservation, 0)
	for rows.Next() {
		var o BehavioralBiasObservation
		var observed, resolvedAt string
		var bid, ask, depth, fee sql.NullFloat64
		var settled int
		var settle sql.NullFloat64
		var won sql.NullInt64
		var inPlay, secsToStart sql.NullInt64
		var mom1h, mom4h, flow, hhi, imb3 sql.NullFloat64
		if err := rows.Scan(&o.ID, &observed, &resolvedAt, &o.Venue, &o.Ticker, &o.CanonicalEvent, &o.Side,
			&o.SignalType, &bid, &ask, &depth, &fee, &o.BookSource, &settled, &settle,
			&won, &o.Category, &o.MarketType, &o.Kind, &o.IsLive, &inPlay, &secsToStart,
			&o.ResolveHours, &o.Momentum, &mom1h, &mom4h, &o.Strength, &o.Notional,
			&o.TraderCount, &o.Concentration, &flow, &hhi, &o.Imbalance, &imb3,
			&o.PricePath, &o.ExecExpr); err != nil {
			return nil, cov, err
		}
		if !ask.Valid || math.IsNaN(ask.Float64) || math.IsInf(ask.Float64, 0) || ask.Float64 <= 0 || ask.Float64 >= 1 {
			cov.MissingAsk++
			continue
		}
		if !depth.Valid || math.IsNaN(depth.Float64) || math.IsInf(depth.Float64, 0) || depth.Float64 < 1 {
			cov.InsufficientDepth++
			continue
		}
		if !fee.Valid || math.IsNaN(fee.Float64) || math.IsInf(fee.Float64, 0) || fee.Float64 < -1 || fee.Float64 > 1 {
			cov.MissingExactFee++
			continue
		}
		if strings.TrimSpace(o.BookSource) == "" {
			cov.MissingBookSource++
			continue
		}
		o.Side = strings.ToUpper(strings.TrimSpace(o.Side))
		if o.Side != "YES" && o.Side != "NO" {
			cov.InvalidSide++
			continue
		}
		payout, payoutOK := behavioralSidePayout(o.Side, settle, won)
		if settled != 1 || !payoutOK {
			cov.UnsettledOrBadPayout++
			continue
		}
		openedTS, openedOK := parseBehavioralBiasTime(observed)
		closedTS, closedOK := parseBehavioralBiasTime(resolvedAt)
		if !openedOK || !closedOK || closedTS.Before(openedTS) {
			cov.InvalidTimestamps++
			continue
		}

		o.ObservedAt, o.ResolvedAt = openedTS, closedTS
		o.Ask, o.AskDepth, o.TakerFeePC, o.Payout = ask.Float64, depth.Float64, fee.Float64, payout
		if bid.Valid && !math.IsNaN(bid.Float64) && !math.IsInf(bid.Float64, 0) &&
			bid.Float64 > 0 && bid.Float64 < 1 && bid.Float64 <= ask.Float64 {
			o.Bid, o.BidKnown = bid.Float64, true
		}
		if inPlay.Valid {
			v := inPlay.Int64
			o.InPlay = &v
		}
		if secsToStart.Valid {
			v := secsToStart.Int64
			o.SecsToStart = &v
		}
		if mom1h.Valid && !math.IsNaN(mom1h.Float64) && !math.IsInf(mom1h.Float64, 0) {
			v := mom1h.Float64
			o.Mom1h = &v
		}
		if mom4h.Valid && !math.IsNaN(mom4h.Float64) && !math.IsInf(mom4h.Float64, 0) {
			v := mom4h.Float64
			o.Mom4h = &v
		}
		if flow.Valid && flow.Float64 >= 0 && flow.Float64 <= 1 {
			v := flow.Float64
			o.FlowRatio15m = &v
		}
		if hhi.Valid && hhi.Float64 >= 0 && hhi.Float64 <= 1 {
			v := hhi.Float64
			o.HoldersHHI = &v
		}
		if imb3.Valid && imb3.Float64 >= 0 && imb3.Float64 <= 1 {
			v := imb3.Float64
			o.BookImb3 = &v
		}
		cov.Eligible++
		out = append(out, o)
	}
	return out, cov, rows.Err()
}
