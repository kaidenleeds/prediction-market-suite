package storage

// Terminal reconciliation for the three R139 native route-control systems. These readers only
// expose routes whose own venue-scoped canonical settlement rows are already present. The server
// still recomputes every leg payout and labels the result counterfactual: no fill, atomicity, Paper,
// or LIVE authority is inferred from a quote-time control.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

type NativeRoutePendingGrade struct {
	OpportunityID, RouteID, SystemID, CanonicalEventID string
	Venue, Ticker, Side, EvidenceJSON                  string
	Observed                                           time.Time
	Quantity, ExecutablePrice, FeeAmount, RebateAmount float64
}

// PendingNativeRouteGrades is resolution-driven rather than oldest-row-driven: unresolved
// long-dated controls cannot starve a settled route behind them. JSON predicates are limited to
// the three versioned native evidence shapes and the result set is bounded.
func (s *Store) PendingNativeRouteGrades(ctx context.Context, limit int) ([]NativeRoutePendingGrade, error) {
	if limit <= 0 || limit > 1000 {
		limit = 250
	}
	rows, err := s.db.QueryContext(ctx, `SELECT o.opportunity_id,o.route_id,o.observed_ts,
o.system_name,o.canonical_event_id,o.venue,o.ticker,o.side,o.requested_qty,o.executable_price,
o.fee_amount,o.rebate_amount,o.evidence_json
FROM research_route_opportunities o
WHERE o.system_name IN ('time-nested-lock','joint-marginal-lock','fee-rounding-batch')
AND NOT EXISTS (
 SELECT 1 FROM research_route_events e
 WHERE e.opportunity_id=o.opportunity_id AND e.route_id=o.route_id AND e.event_type='grade'
)
AND CASE o.system_name
 WHEN 'fee-rounding-batch' THEN EXISTS (
  SELECT 1 FROM venue_settlements v WHERE v.platform='kalshi' AND v.ticker=o.ticker
 ) OR EXISTS (SELECT 1 FROM signal_log s WHERE s.platform='kalshi' AND s.ticker=o.ticker
   AND s.resolved=1 AND s.settle_val IS NOT NULL AND s.settle_val>=0 AND s.settle_val<=1)
 WHEN 'time-nested-lock' THEN
  (EXISTS (SELECT 1 FROM venue_settlements v WHERE v.platform='kalshi'
    AND v.ticker=json_extract(o.evidence_json,'$.aggregate_route.left_ticker'))
   OR EXISTS (SELECT 1 FROM signal_log s WHERE s.platform='kalshi'
    AND s.ticker=json_extract(o.evidence_json,'$.aggregate_route.left_ticker')
    AND s.resolved=1 AND s.settle_val IS NOT NULL AND s.settle_val>=0 AND s.settle_val<=1))
  AND (EXISTS (SELECT 1 FROM venue_settlements v WHERE v.platform='kalshi'
    AND v.ticker=json_extract(o.evidence_json,'$.aggregate_route.right_ticker'))
   OR EXISTS (SELECT 1 FROM signal_log s WHERE s.platform='kalshi'
    AND s.ticker=json_extract(o.evidence_json,'$.aggregate_route.right_ticker')
    AND s.resolved=1 AND s.settle_val IS NOT NULL AND s.settle_val>=0 AND s.settle_val<=1))
 WHEN 'joint-marginal-lock' THEN
  json_array_length(o.evidence_json,'$.aggregate_route.legs') BETWEEN 2 AND 6
  AND NOT EXISTS (
   SELECT 1 FROM json_each(o.evidence_json,'$.aggregate_route.legs') leg
   WHERE NOT EXISTS (SELECT 1 FROM venue_settlements v
    WHERE v.platform=o.venue AND v.ticker=json_extract(leg.value,'$.ticker'))
   AND NOT EXISTS (
    SELECT 1 FROM signal_log s
    WHERE s.platform=o.venue AND s.ticker=json_extract(leg.value,'$.ticker')
      AND s.resolved=1 AND s.settle_val IS NOT NULL AND s.settle_val>=0 AND s.settle_val<=1
   )
  )
 ELSE 0 END
ORDER BY o.observed_ts,o.opportunity_id,o.route_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]NativeRoutePendingGrade, 0, limit)
	for rows.Next() {
		var v NativeRoutePendingGrade
		var observed string
		if err := rows.Scan(&v.OpportunityID, &v.RouteID, &observed, &v.SystemID,
			&v.CanonicalEventID, &v.Venue, &v.Ticker, &v.Side, &v.Quantity,
			&v.ExecutablePrice, &v.FeeAmount, &v.RebateAmount, &v.EvidenceJSON); err != nil {
			return nil, err
		}
		v.Observed, err = time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			return nil, fmt.Errorf("parse native route observation: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type VenueSettlementReceipt struct {
	RowID                int64
	Platform, Ticker     string
	ResolvedAt           time.Time
	YesValue             float64
	SourceArtifact, Hash string
}

// VenueSettlementForTicker deliberately requires the exact venue-sent settle_val and a matching
// platform. It never falls back to a side-relative won flag and cannot cross-contaminate identical
// ticker strings listed on two venues.
func (s *Store) VenueSettlementForTicker(ctx context.Context, platform, ticker string) (VenueSettlementReceipt, bool, error) {
	platform = strings.ToLower(strings.TrimSpace(platform))
	ticker = strings.TrimSpace(ticker)
	if platform == "" || ticker == "" {
		return VenueSettlementReceipt{}, false, errors.New("empty venue settlement identity")
	}
	var out VenueSettlementReceipt
	var resolved string
	var source string
	err := s.db.QueryRowContext(ctx, `SELECT yes_value,resolved_at,source_artifact
FROM venue_settlements WHERE platform=? AND ticker=?`, platform, ticker).Scan(&out.YesValue, &resolved, &source)
	if err == nil {
		out.ResolvedAt, err = time.Parse(time.RFC3339Nano, resolved)
		if err != nil {
			out.ResolvedAt, err = time.Parse(time.RFC3339, resolved)
		}
		if err != nil || !authoritativeVenueSettlementReceipt(platform, out.YesValue, source) {
			return VenueSettlementReceipt{}, false, nil
		}
		out.Platform, out.Ticker = platform, ticker
		out.SourceArtifact = "venue settlement receipt: " + source
		payload, _ := json.Marshal(map[string]any{"platform": platform, "ticker": ticker,
			"settle_val": out.YesValue, "resolved_at": out.ResolvedAt.UTC().Format(time.RFC3339Nano),
			"source_artifact": source})
		h := sha256.Sum256(payload)
		out.Hash = "sha256:" + hex.EncodeToString(h[:])
		return out, true, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return VenueSettlementReceipt{}, false, err
	}
	// PolyUS signal rows do not encode whether their 0/1 came from a preliminary
	// BookFull mark or an authoritative final source. Current settlement writers
	// always create a v2 receipt, so there is no safe legacy fallback here.
	if platform == "polyus" {
		return VenueSettlementReceipt{}, false, nil
	}
	err = s.db.QueryRowContext(ctx, `SELECT id,settle_val,resolved_at
FROM signal_log WHERE platform=? AND ticker=? AND resolved=1
 AND settle_val IS NOT NULL AND settle_val>=0 AND settle_val<=1
ORDER BY resolved_at DESC,id DESC LIMIT 1`, platform, ticker).Scan(&out.RowID, &out.YesValue, &resolved)
	if errors.Is(err, sql.ErrNoRows) {
		return VenueSettlementReceipt{}, false, nil
	}
	if err != nil {
		return VenueSettlementReceipt{}, false, err
	}
	out.ResolvedAt, err = time.Parse(time.RFC3339Nano, resolved)
	if err != nil {
		out.ResolvedAt, err = time.Parse(time.RFC3339, resolved)
	}
	if err != nil || !authoritativeVenueSettlementValue(platform, out.YesValue) {
		return VenueSettlementReceipt{}, false, nil
	}
	out.Platform, out.Ticker = platform, ticker
	out.SourceArtifact = "canonical signal settlement: venue-scoped exact settle_val"
	payload, _ := json.Marshal(map[string]any{"platform": platform, "ticker": ticker,
		"settle_val": out.YesValue, "resolved_at": out.ResolvedAt.UTC().Format(time.RFC3339Nano),
		"signal_row_id": out.RowID})
	h := sha256.Sum256(payload)
	out.Hash = "sha256:" + hex.EncodeToString(h[:])
	return out, true, nil
}

type NativeRouteTerminalGrade struct {
	OpportunityID, RouteID, OutcomeStatus, Reason, EvidenceJSON string
	Observed                                                    time.Time
	Quantity, Payout, RealizedNet, CapitalSeconds               float64
}

// AppendNativeRouteTerminalGrade is idempotent under retries and concurrent maintenance ticks.
// The schema itself keeps all authority at zero; this INSERT additionally requires the immutable
// parent route to exist and refuses a second grade.
func (s *Store) AppendNativeRouteTerminalGrade(ctx context.Context, v NativeRouteTerminalGrade) (bool, error) {
	if v.Observed.IsZero() {
		v.Observed = time.Now().UTC()
	}
	if strings.TrimSpace(v.OpportunityID) == "" || strings.TrimSpace(v.RouteID) == "" ||
		strings.TrimSpace(v.OutcomeStatus) == "" || strings.TrimSpace(v.Reason) == "" ||
		!json.Valid([]byte(v.EvidenceJSON)) || v.Quantity < 0 || v.CapitalSeconds < 0 ||
		math.IsNaN(v.Payout) || math.IsInf(v.Payout, 0) || math.IsNaN(v.RealizedNet) ||
		math.IsInf(v.RealizedNet, 0) {
		return false, errors.New("invalid native terminal grade")
	}
	res, err := s.db.ExecContext(ctx, `INSERT INTO research_route_events(
opportunity_id,route_id,observed_ts,event_type,quantity,payout,realized_net,capital_seconds,
outcome_status,reason,evidence_json,funded,paper_authority,live_authority)
SELECT ?,?,?,'grade',?,?,?,?,?,?,?,0,0,0
WHERE EXISTS (SELECT 1 FROM research_route_opportunities o
 WHERE o.opportunity_id=? AND o.route_id=?
 AND o.system_name IN ('time-nested-lock','joint-marginal-lock','fee-rounding-batch'))
AND NOT EXISTS (SELECT 1 FROM research_route_events e
 WHERE e.opportunity_id=? AND e.route_id=? AND e.event_type='grade')`,
		v.OpportunityID, v.RouteID, v.Observed.UTC().Format(time.RFC3339Nano), v.Quantity,
		v.Payout, v.RealizedNet, v.CapitalSeconds, v.OutcomeStatus, v.Reason, v.EvidenceJSON,
		v.OpportunityID, v.RouteID, v.OpportunityID, v.RouteID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}
