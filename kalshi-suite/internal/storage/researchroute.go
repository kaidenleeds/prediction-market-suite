package storage

// The unified route ledger is research provenance, not an execution queue.  It records every
// point-in-time route alternative (including rejects and no-trade controls) and then appends its
// lifecycle.  No consumer of this file can grant paper or LIVE authority.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const redundantObserverRouteCleanupCursor = "r146_redundant_observer_route_mirror_cursor_v1"

type ResearchRouteOpportunity struct {
	OpportunityID, RouteID                          string
	Observed, DecisionAt                            time.Time
	ExperimentID                                    string
	ExperimentVersion                               int
	SystemName, CanonicalEventID, CanonicalPayoffID string
	IdentityStatus, Venue, Ticker, Side, Route      string
	Action, QuoteSource, QuoteSequence              string
	QuoteAgeSeconds, DecisionLatencyMS, TickSize    float64
	QuoteAgeKnown, LatencyKnown, TickKnown          bool
	ExecutablePrice, ExecutableDepth, RequestedQty  float64
	DepthKnown                                      bool
	FeeAmount, RebateAmount                         float64
	FeeAuthority                                    string
	FeeKnown                                        bool
	ExpectedPayoutLow, ExpectedPayoutHigh           float64
	ExpectedNetLow, ExpectedNetHigh                 float64
	PartialFillWorst, CapitalSeconds                float64
	Decision, DecisionReason, AlternativeGroup      string
	EvidenceJSON                                    string
}

type ResearchRouteEvent struct {
	OpportunityID, RouteID, EventType, OrderRefHash string
	Observed                                        time.Time
	Quantity, Price, FeeAmount, RebateAmount        float64
	QueueAhead                                      *float64
	Payout, RealizedNet, Markout                    *float64
	CapitalSeconds                                  float64
	OutcomeStatus, Reason, EvidenceJSON             string
}

type researchRouteExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func researchRouteFinite(values ...float64) bool {
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}
	return true
}

// ResearchRouteStableID creates an opaque deterministic ID without putting a title, order ID, or
// other potentially sensitive raw identifier in logs/UI. Callers must include their frozen source
// timestamp or slot so separate opportunities do not collapse together.
func ResearchRouteStableID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(h[:16])
}

func normalizeRouteOpportunity(v *ResearchRouteOpportunity) error {
	if v.Observed.IsZero() {
		v.Observed = time.Now().UTC()
	}
	if v.DecisionAt.IsZero() {
		v.DecisionAt = v.Observed
	}
	v.OpportunityID = strings.TrimSpace(v.OpportunityID)
	v.RouteID = strings.TrimSpace(v.RouteID)
	v.SystemName = strings.TrimSpace(v.SystemName)
	v.IdentityStatus = strings.ToLower(strings.TrimSpace(v.IdentityStatus))
	v.Venue = strings.ToLower(strings.TrimSpace(v.Venue))
	v.Side = strings.ToUpper(strings.TrimSpace(v.Side))
	v.Route = strings.ToLower(strings.TrimSpace(v.Route))
	v.Action = strings.ToLower(strings.TrimSpace(v.Action))
	v.Decision = strings.ToLower(strings.TrimSpace(v.Decision))
	if v.IdentityStatus == "" {
		v.IdentityStatus = "unverified"
	}
	if v.EvidenceJSON == "" {
		v.EvidenceJSON = "{}"
	}
	if v.OpportunityID == "" || v.RouteID == "" || v.SystemName == "" ||
		v.QuoteSource == "" || v.FeeAuthority == "" || !json.Valid([]byte(v.EvidenceJSON)) ||
		!researchRouteFinite(v.QuoteAgeSeconds, v.DecisionLatencyMS, v.TickSize,
			v.ExecutablePrice, v.ExecutableDepth, v.RequestedQty, v.FeeAmount, v.RebateAmount,
			v.ExpectedPayoutLow, v.ExpectedPayoutHigh, v.ExpectedNetLow, v.ExpectedNetHigh,
			v.PartialFillWorst, v.CapitalSeconds) || v.QuoteAgeSeconds < 0 ||
		v.DecisionLatencyMS < 0 || v.TickSize < 0 || v.ExecutablePrice < 0 ||
		v.ExecutablePrice > 1 || v.ExecutableDepth < 0 || v.RequestedQty < 0 ||
		v.FeeAmount < 0 || v.RebateAmount < 0 || v.CapitalSeconds < 0 ||
		v.ExpectedPayoutLow > v.ExpectedPayoutHigh || v.ExpectedNetLow > v.ExpectedNetHigh {
		return fmt.Errorf("invalid research route opportunity")
	}
	validIdentity := map[string]bool{"verified": true, "structural": true, "unverified": true, "rejected": true}
	validVenue := map[string]bool{"kalshi": true, "polyus": true, "polymarket": true, "multi": true, "none": true}
	validSide := map[string]bool{"YES": true, "NO": true, "MULTI": true, "NONE": true}
	validRoute := map[string]bool{"maker": true, "taker": true, "rfq": true, "combo": true, "lock": true, "abstain": true, "control": true}
	validAction := map[string]bool{"buy": true, "sell": true, "post": true, "quote": true, "abstain": true, "reject": true, "control": true}
	validDecision := map[string]bool{"candidate": true, "blocked": true, "abstain": true, "control": true}
	if !validIdentity[v.IdentityStatus] || !validVenue[v.Venue] || !validSide[v.Side] ||
		!validRoute[v.Route] || !validAction[v.Action] || !validDecision[v.Decision] {
		return fmt.Errorf("invalid research route enum")
	}
	if v.Action != "abstain" && v.Action != "reject" && v.Action != "control" &&
		(strings.TrimSpace(v.Ticker) == "" || v.Side == "NONE" || v.ExecutablePrice <= 0 ||
			v.ExecutableDepth <= 0 || v.RequestedQty <= 0) {
		return fmt.Errorf("executable route lacks side-specific price, depth, or quantity")
	}
	if v.Decision == "candidate" {
		// A candidate is a preregistered executable action under test, not a claim of profit and not
		// order authority. Directional rows therefore retain the literal 0..1 payoff censor envelope
		// and may correctly have a negative ExpectedNetLow until terminal evidence is sealed.
		if strings.TrimSpace(v.DecisionReason) == "" ||
			v.IdentityStatus != "verified" || !v.QuoteAgeKnown || !v.LatencyKnown || !v.TickKnown ||
			!v.DepthKnown || !v.FeeKnown {
			return fmt.Errorf("candidate route lacks verified identity or complete quote/latency/tick/depth/fee truth")
		}
	}
	return nil
}

// InsertResearchRouteOpportunity retains the first immutable observation for a route ID. A second
// insert is a duplicate, never an update that can improve the historical quote after the fact.
func insertResearchRouteOpportunityWith(ctx context.Context, execer researchRouteExecer, v ResearchRouteOpportunity) (bool, error) {
	if err := normalizeRouteOpportunity(&v); err != nil {
		return false, err
	}
	r, err := execer.ExecContext(ctx, `INSERT OR IGNORE INTO research_route_opportunities(
opportunity_id,route_id,observed_ts,decision_ts,experiment_id,experiment_version,system_name,
canonical_event_id,canonical_payoff_id,identity_status,venue,ticker,side,route,action,quote_source,
quote_sequence,quote_age_s,quote_age_known,decision_latency_ms,latency_known,tick_size,tick_known,
executable_price,executable_depth,depth_known,requested_qty,fee_amount,rebate_amount,fee_authority,
fee_known,expected_payout_low,expected_payout_high,expected_net_low,
expected_net_high,partial_fill_worst,capital_seconds,decision,decision_reason,alternative_group,evidence_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		v.OpportunityID, v.RouteID, v.Observed.UTC().Format(time.RFC3339Nano),
		v.DecisionAt.UTC().Format(time.RFC3339Nano), v.ExperimentID, v.ExperimentVersion, v.SystemName,
		v.CanonicalEventID, v.CanonicalPayoffID, v.IdentityStatus, v.Venue, v.Ticker, v.Side, v.Route,
		v.Action, v.QuoteSource, v.QuoteSequence, v.QuoteAgeSeconds, boolInt(v.QuoteAgeKnown),
		v.DecisionLatencyMS, boolInt(v.LatencyKnown), v.TickSize, boolInt(v.TickKnown),
		v.ExecutablePrice, v.ExecutableDepth, boolInt(v.DepthKnown), v.RequestedQty,
		v.FeeAmount, v.RebateAmount, v.FeeAuthority, boolInt(v.FeeKnown),
		v.ExpectedPayoutLow, v.ExpectedPayoutHigh, v.ExpectedNetLow, v.ExpectedNetHigh,
		v.PartialFillWorst, v.CapitalSeconds, v.Decision, v.DecisionReason, v.AlternativeGroup, v.EvidenceJSON)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	return n > 0, err
}

func (s *Store) InsertResearchRouteOpportunity(ctx context.Context, v ResearchRouteOpportunity) (bool, error) {
	return insertResearchRouteOpportunityWith(ctx, s.db, v)
}

// CompactRedundantObserverRouteMirrors advances through the historical system-observation ledger
// in a bounded primary-key page and removes only the old Step-7 flow-label route copies whose full
// paired source is retained in research_flow_direction_pairs and its compact daily archive. The
// original research_system_observations row is never changed or deleted.
// A mirror carrying any route lifecycle event is also retained, even though grade-only events may
// duplicate payoff updates; this intentionally favors provenance over maximum space recovery.
//
// New observer observations are no longer mirrored, so the durable cursor is a one-way migration.
// Deleted pages enter SQLite's freelist and are reused without a risky live VACUUM or file rewrite.
func (s *Store) CompactRedundantObserverRouteMirrors(ctx context.Context, limit int) (
	scanned, removed int64, done bool, err error) {
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	// This is disk reclamation, never a trading prerequisite. A missing/regressed index must fail
	// fast instead of holding SQLite's single writer behind a historical scan. The candidate page
	// is selected before BeginTx, and the entire operation has a hard two-second ceiling.
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var cursorText string
	err = s.db.QueryRowContext(opCtx, `SELECT v FROM kv WHERE k=?`, redundantObserverRouteCleanupCursor).Scan(&cursorText)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false, err
	}
	if cursorText == "done" {
		return 0, 0, true, nil
	}
	cursor, parseErr := strconv.ParseInt(cursorText, 10, 64)
	if cursorText != "" && parseErr != nil {
		return 0, 0, false, fmt.Errorf("invalid observer-route cleanup cursor %q", cursorText)
	}
	rows, err := s.db.QueryContext(opCtx, `SELECT id FROM research_system_observations INDEXED BY idx_rsystem_obs_flow_label_cleanup
WHERE id>? AND system_id='flow-direction-integrity' AND route='observer'
 AND observation_kind='control' AND blocker='label_audit_only_no_trade_authority'
 AND cohort IN ('kalshi-authoritative-aggressor-v1','kalshi-inferred-aggressor-v1')
ORDER BY id LIMIT ?`, cursor, limit)
	if err != nil {
		return 0, 0, false, err
	}
	last := cursor
	for rows.Next() {
		if err := rows.Scan(&last); err != nil {
			_ = rows.Close()
			return 0, 0, false, err
		}
		scanned++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, false, err
	}
	if err := rows.Close(); err != nil {
		return 0, 0, false, err
	}
	// Only now take the writer. Recheck the durable cursor after BEGIN so two maintenance callers
	// that selected the same page cannot delete/count it twice. SQLite's pool-wide 35s busy timeout
	// outlives a Go context while BEGIN IMMEDIATE waits, so this maintenance connection temporarily
	// uses 250ms and restores the ordinary timeout before returning it to the pool.
	conn, err := s.db.Conn(opCtx)
	if err != nil {
		return 0, 0, false, err
	}
	defer func() {
		restoreCtx, restoreCancel := context.WithTimeout(context.Background(), time.Second)
		defer restoreCancel()
		_, _ = conn.ExecContext(restoreCtx, `PRAGMA busy_timeout=35000`)
		_ = conn.Close()
	}()
	if _, err := conn.ExecContext(opCtx, `PRAGMA busy_timeout=250`); err != nil {
		return 0, 0, false, err
	}
	tx, err := conn.BeginTx(opCtx, nil)
	if err != nil {
		return 0, 0, false, err
	}
	defer tx.Rollback()
	var currentCursor string
	err = tx.QueryRowContext(opCtx, `SELECT v FROM kv WHERE k=?`, redundantObserverRouteCleanupCursor).Scan(&currentCursor)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, false, err
	}
	if currentCursor != cursorText {
		if err := tx.Commit(); err != nil {
			return 0, 0, false, err
		}
		return 0, 0, currentCursor == "done", nil
	}
	if scanned == 0 {
		_, err = tx.ExecContext(opCtx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)
ON CONFLICT(k) DO UPDATE SET v=excluded.v,ts=excluded.ts`, redundantObserverRouteCleanupCursor, "done", nowRFC())
		if err != nil {
			return 0, 0, false, err
		}
		return 0, 0, true, tx.Commit()
	}
	// The public append-only contract stays in force before and after this internal migration.
	// DDL is transactional in SQLite: any failure rolls back both the delete and the trigger drop.
	if _, err := tx.ExecContext(opCtx, `DROP TRIGGER IF EXISTS research_route_opportunities_no_delete`); err != nil {
		return 0, 0, false, err
	}
	res, err := tx.ExecContext(opCtx, `DELETE FROM research_route_opportunities WHERE rowid IN (
 SELECT r.rowid
 FROM research_system_observations AS o INDEXED BY idx_rsystem_obs_flow_label_cleanup
 JOIN research_route_opportunities AS r INDEXED BY idx_rroute_system_observation
   ON CAST(json_extract(r.evidence_json,'$.system_observation_id') AS INTEGER)=o.id
 WHERE o.id>? AND o.id<=? AND o.system_id='flow-direction-integrity' AND o.route='observer'
   AND o.observation_kind='control' AND o.blocker='label_audit_only_no_trade_authority'
   AND o.cohort IN ('kalshi-authoritative-aggressor-v1','kalshi-inferred-aggressor-v1')
   AND CAST(json_extract(r.evidence_json,'$.system_observation_id') AS INTEGER)>?
   AND CAST(json_extract(r.evidence_json,'$.system_observation_id') AS INTEGER)<=?
   AND r.system_name=o.system_id AND r.experiment_id=o.system_id
   AND r.route='abstain' AND r.action='control' AND r.decision='control'
   AND NOT EXISTS (
     SELECT 1 FROM research_route_events AS e
     WHERE e.opportunity_id=r.opportunity_id AND e.route_id=r.route_id
   )
)`, cursor, last, cursor, last)
	if err != nil {
		return 0, 0, false, err
	}
	removed, err = res.RowsAffected()
	if err != nil {
		return 0, 0, false, err
	}
	if _, err := tx.ExecContext(opCtx, `CREATE TRIGGER research_route_opportunities_no_delete
BEFORE DELETE ON research_route_opportunities BEGIN
 SELECT RAISE(ABORT,'immutable research route opportunity');
END`); err != nil {
		return 0, 0, false, err
	}
	value := strconv.FormatInt(last, 10)
	if scanned < int64(limit) {
		value, done = "done", true
	}
	if _, err := tx.ExecContext(opCtx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)
ON CONFLICT(k) DO UPDATE SET v=excluded.v,ts=excluded.ts`, redundantObserverRouteCleanupCursor, value, nowRFC()); err != nil {
		return 0, 0, false, err
	}
	if err := tx.Commit(); err != nil {
		return 0, 0, false, err
	}
	return scanned, removed, done, nil
}

func (s *Store) AppendResearchRouteEvent(ctx context.Context, v ResearchRouteEvent) (int64, error) {
	if v.Observed.IsZero() {
		v.Observed = time.Now().UTC()
	}
	v.EventType = strings.ToLower(strings.TrimSpace(v.EventType))
	if v.EvidenceJSON == "" {
		v.EvidenceJSON = "{}"
	}
	validType := map[string]bool{"intent": true, "refused": true, "posted": true, "queue": true,
		"partial_fill": true, "fill": true, "cancel": true, "expire": true, "unwind": true,
		"settlement": true, "grade": true, "note": true}
	if strings.TrimSpace(v.OpportunityID) == "" || strings.TrimSpace(v.RouteID) == "" ||
		!validType[v.EventType] || !json.Valid([]byte(v.EvidenceJSON)) ||
		!researchRouteFinite(v.Quantity, v.Price, v.FeeAmount, v.RebateAmount, v.CapitalSeconds) ||
		v.Quantity < 0 || v.Price < 0 || v.Price > 1 || v.FeeAmount < 0 ||
		v.RebateAmount < 0 || v.CapitalSeconds < 0 {
		return 0, fmt.Errorf("invalid research route event")
	}
	for _, p := range []*float64{v.QueueAhead, v.Payout, v.RealizedNet, v.Markout} {
		if p != nil && (!researchRouteFinite(*p) || (p == v.QueueAhead && *p < 0)) {
			return 0, fmt.Errorf("invalid research route event value")
		}
	}
	r, err := s.db.ExecContext(ctx, `INSERT INTO research_route_events(
opportunity_id,route_id,observed_ts,event_type,quantity,price,fee_amount,rebate_amount,queue_ahead,
order_ref_hash,payout,realized_net,capital_seconds,markout,outcome_status,reason,evidence_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, v.OpportunityID, v.RouteID,
		v.Observed.UTC().Format(time.RFC3339Nano), v.EventType, v.Quantity, v.Price, v.FeeAmount,
		v.RebateAmount, v.QueueAhead, v.OrderRefHash, v.Payout, v.RealizedNet, v.CapitalSeconds,
		v.Markout, v.OutcomeStatus, v.Reason, v.EvidenceJSON)
	if err != nil {
		return 0, err
	}
	return r.LastInsertId()
}

type ResearchRouteRecent struct {
	OpportunityID, RouteID, ObservedTS, SystemName, Venue, Ticker string
	Side, Route, Decision, DecisionReason, IdentityStatus         string
	ExecutablePrice, ExecutableDepth, FeeAmount, RebateAmount     float64
	ExpectedNetLow, ExpectedNetHigh                               float64
	QuoteAgeKnown, LatencyKnown, TickKnown, DepthKnown, FeeKnown  bool
}

const researchRouteRecentSQL = `SELECT opportunity_id,route_id,observed_ts,system_name,venue,
ticker,side,route,decision,decision_reason,identity_status,executable_price,executable_depth,
fee_amount,rebate_amount,expected_net_low,expected_net_high,
quote_age_known,latency_known,tick_known,depth_known,fee_known
FROM research_route_opportunities INDEXED BY idx_rroute_report_recent
ORDER BY observed_ts DESC,opportunity_id,route_id LIMIT ?`

const researchRouteReportTotalsSQL = `SELECT routes,candidates,blocked,abstains,controls,
fee_authority_rows,nonpositive_lower_bound,identity_unverified_or_rejected,complete_money_truth,
lifecycle_events,fill_events,cancel_events,grade_events
FROM research_route_report_totals WHERE singleton=1`

const researchRouteDecisionTotalsSQL = `SELECT route,decision,route_count
FROM research_route_decision_totals ORDER BY route,decision`

// ResearchRouteReport is deliberately descriptive. It reports exact route completeness and
// lifecycle coverage but never computes authority from row count or a point estimate.
func (s *Store) ResearchRouteReport(ctx context.Context, limit int) (map[string]any, error) {
	if limit <= 0 || limit > 250 {
		limit = 100
	}
	type counts struct {
		Rows, Candidates, Blocked, Abstains, Controls, ExactFees, Events, Fills, Cancels, Grades int
		NegativeOrZeroLower, MissingIdentity, CompleteMoneyTruth                                 int
	}
	var c counts
	err := s.db.QueryRowContext(ctx, researchRouteReportTotalsSQL).Scan(
		&c.Rows, &c.Candidates, &c.Blocked, &c.Abstains, &c.Controls, &c.ExactFees,
		&c.NegativeOrZeroLower, &c.MissingIdentity, &c.CompleteMoneyTruth,
		&c.Events, &c.Fills, &c.Cancels, &c.Grades)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, researchRouteRecentSQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	recent := make([]ResearchRouteRecent, 0, limit)
	for rows.Next() {
		var v ResearchRouteRecent
		var quoteAgeKnown, latencyKnown, tickKnown, depthKnown, feeKnown int
		if err := rows.Scan(&v.OpportunityID, &v.RouteID, &v.ObservedTS, &v.SystemName, &v.Venue,
			&v.Ticker, &v.Side, &v.Route, &v.Decision, &v.DecisionReason, &v.IdentityStatus,
			&v.ExecutablePrice, &v.ExecutableDepth, &v.FeeAmount, &v.RebateAmount,
			&v.ExpectedNetLow, &v.ExpectedNetHigh, &quoteAgeKnown, &latencyKnown, &tickKnown,
			&depthKnown, &feeKnown); err != nil {
			return nil, err
		}
		v.QuoteAgeKnown, v.LatencyKnown, v.TickKnown = quoteAgeKnown != 0, latencyKnown != 0, tickKnown != 0
		v.DepthKnown, v.FeeKnown = depthKnown != 0, feeKnown != 0
		recent = append(recent, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	routeRows, err := s.db.QueryContext(ctx, researchRouteDecisionTotalsSQL)
	if err != nil {
		return nil, err
	}
	routes := map[string]map[string]int{}
	for routeRows.Next() {
		var route, decision string
		var n int
		if err := routeRows.Scan(&route, &decision, &n); err != nil {
			_ = routeRows.Close()
			return nil, err
		}
		if routes[route] == nil {
			routes[route] = map[string]int{}
		}
		routes[route][decision] = n
	}
	if err := routeRows.Close(); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(routes))
	for key := range routes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return map[string]any{
		"generated_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"state":           "RESEARCH_ONLY",
		"funded":          false,
		"paper_authority": false,
		"live_authority":  false,
		"contract":        "signal/model/control -> decision -> side-specific executable quote or explicit rejection -> queue/order/fill/cancel -> exact fee/rebate -> settlement/grade; negative, no-order, and cross-venue identity/rule alternatives are retained; only Step-7 flow-label copies already owned by the raw-pair and daily aggregate ledgers are not duplicated here",
		"counts": map[string]int{"routes": c.Rows, "candidates": c.Candidates, "blocked": c.Blocked,
			"abstains": c.Abstains, "controls": c.Controls, "fee_authority_rows": c.ExactFees,
			"nonpositive_lower_bound": c.NegativeOrZeroLower, "identity_unverified_or_rejected": c.MissingIdentity,
			"complete_money_truth": c.CompleteMoneyTruth,
			"lifecycle_events":     c.Events, "fill_events": c.Fills, "cancel_events": c.Cancels, "grade_events": c.Grades},
		"route_decisions": routes, "route_order": keys, "recent": recent,
		"promotion_rule": "row count and point estimates never authorize; untouched event/day holdout plus positive fee-net lower bound is required",
	}, nil
}

// Compile-time check that sql.Null* remains available when this file grows settlement views; it
// also documents that NULL is intentional for queue/outcome fields rather than encoded as zero.
var _ = sql.NullFloat64{}
