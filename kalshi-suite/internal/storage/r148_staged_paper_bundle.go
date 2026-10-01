package storage

// The staged Paper package ledger is deliberately separate from paper_parlays.  These systems
// own additive payoff vectors (often a one-dollar floor), not an all-legs-win parlay product.
// Every candidate decision and terminal result is append-only, while an immutable claim table
// guarantees that one system opportunity can fund at most one simulated package across restarts.

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

const r148StagedPaperBundleSchema = `
CREATE TABLE IF NOT EXISTS staged_paper_bundle_meta (
 id INTEGER PRIMARY KEY CHECK(id=1),
 epoch_ts TEXT NOT NULL,
 updated_ts TEXT NOT NULL
);
INSERT OR IGNORE INTO staged_paper_bundle_meta(id,epoch_ts,updated_ts)
VALUES(1,'1970-01-01T00:00:00Z',strftime('%Y-%m-%dT%H:%M:%fZ','now'));

CREATE TABLE IF NOT EXISTS staged_paper_bundle_attempts (
 package_id TEXT PRIMARY KEY,
 bundle_id TEXT NOT NULL UNIQUE,
 system_id TEXT NOT NULL,
 opportunity_id TEXT NOT NULL,
 canonical_event_id TEXT NOT NULL,
 cohort TEXT NOT NULL,
 venue_shape TEXT NOT NULL CHECK(venue_shape IN ('kalshi','polyus','crossvenue','unsupported')),
 candidate_ts TEXT NOT NULL,
 decision_ts TEXT NOT NULL,
 size_units REAL NOT NULL CHECK(size_units>0),
 total_cost REAL NOT NULL CHECK(total_cost>0),
 total_fee REAL NOT NULL CHECK(total_fee>=0),
 payout_floor REAL NOT NULL CHECK(payout_floor>=0),
 net_floor REAL NOT NULL,
 leg_count INTEGER NOT NULL CHECK(leg_count BETWEEN 2 AND 6),
 live_state TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 FOREIGN KEY(bundle_id) REFERENCES research_route_bundles(bundle_id)
);
CREATE INDEX IF NOT EXISTS idx_staged_paper_bundle_system
 ON staged_paper_bundle_attempts(system_id,decision_ts,package_id);
CREATE TRIGGER IF NOT EXISTS staged_paper_bundle_attempts_no_update BEFORE UPDATE ON staged_paper_bundle_attempts
 BEGIN SELECT RAISE(ABORT,'immutable staged Paper bundle attempt'); END;
CREATE TRIGGER IF NOT EXISTS staged_paper_bundle_attempts_no_delete BEFORE DELETE ON staged_paper_bundle_attempts
 BEGIN SELECT RAISE(ABORT,'immutable staged Paper bundle attempt'); END;

CREATE TABLE IF NOT EXISTS staged_paper_bundle_claims (
 system_id TEXT NOT NULL,
 opportunity_id TEXT NOT NULL,
 package_id TEXT NOT NULL UNIQUE,
 claimed_ts TEXT NOT NULL,
 PRIMARY KEY(system_id,opportunity_id),
 FOREIGN KEY(package_id) REFERENCES staged_paper_bundle_attempts(package_id)
);
CREATE TRIGGER IF NOT EXISTS staged_paper_bundle_claims_no_update BEFORE UPDATE ON staged_paper_bundle_claims
 BEGIN SELECT RAISE(ABORT,'immutable staged Paper bundle claim'); END;
CREATE TRIGGER IF NOT EXISTS staged_paper_bundle_claims_no_delete BEFORE DELETE ON staged_paper_bundle_claims
 BEGIN SELECT RAISE(ABORT,'immutable staged Paper bundle claim'); END;

CREATE TABLE IF NOT EXISTS staged_paper_bundle_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 package_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('accepted','rejected','settled')),
 outcome_status TEXT NOT NULL,
 payout REAL,
 realized_net REAL,
 reason TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 FOREIGN KEY(package_id) REFERENCES staged_paper_bundle_attempts(package_id),
 UNIQUE(package_id,event_type)
);
CREATE INDEX IF NOT EXISTS idx_staged_paper_bundle_event
 ON staged_paper_bundle_events(package_id,event_type,id);
CREATE TRIGGER IF NOT EXISTS staged_paper_bundle_events_no_update BEFORE UPDATE ON staged_paper_bundle_events
 BEGIN SELECT RAISE(ABORT,'append-only staged Paper bundle event'); END;
CREATE TRIGGER IF NOT EXISTS staged_paper_bundle_events_no_delete BEFORE DELETE ON staged_paper_bundle_events
 BEGIN SELECT RAISE(ABORT,'append-only staged Paper bundle event'); END;
`

func migrateR148StagedPaperBundleSchema(db *sql.DB) error {
	_, err := db.Exec(r148StagedPaperBundleSchema)
	return err
}

type StagedPaperBundleDecision struct {
	PackageID, BundleID, SystemID, OpportunityID, CanonicalEventID string
	Cohort, VenueShape, LiveState, Reason, EvidenceJSON            string
	Candidate, Decided                                             time.Time
	Size, Cost, Fee, PayoutFloor, NetFloor                         float64
	LegCount                                                       int
	Accept                                                         bool
}

func validStagedPaperFinite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validateStagedPaperBundleDecision(d StagedPaperBundleDecision) error {
	if strings.TrimSpace(d.PackageID) == "" || strings.TrimSpace(d.BundleID) == "" ||
		strings.TrimSpace(d.SystemID) == "" || strings.TrimSpace(d.OpportunityID) == "" ||
		strings.TrimSpace(d.CanonicalEventID) == "" || strings.TrimSpace(d.Cohort) == "" ||
		d.Candidate.IsZero() || d.Decided.IsZero() || d.Decided.Before(d.Candidate) ||
		d.LegCount < 2 || d.LegCount > 6 || d.Size <= 0 || d.Cost <= 0 || d.Fee < 0 ||
		d.PayoutFloor < 0 || strings.TrimSpace(d.LiveState) == "" || strings.TrimSpace(d.Reason) == "" ||
		!json.Valid([]byte(d.EvidenceJSON)) {
		return errors.New("invalid staged Paper bundle decision")
	}
	for _, v := range []float64{d.Size, d.Cost, d.Fee, d.PayoutFloor, d.NetFloor} {
		if !validStagedPaperFinite(v) {
			return errors.New("non-finite staged Paper bundle decision")
		}
	}
	switch d.VenueShape {
	case "kalshi", "polyus", "crossvenue", "unsupported":
	default:
		return errors.New("invalid staged Paper venue shape")
	}
	if d.Accept && d.NetFloor <= 0 {
		return errors.New("accepted staged Paper bundle lacks a positive fee-net floor")
	}
	return nil
}

// AppendStagedPaperBundleDecision atomically writes the immutable candidate receipt and either a
// simulated full-package FOK fill or a flat rejection.  A previously accepted system+opportunity
// owns the unique claim; later capacity rows remain visible as rejected attempts but cannot create
// a duplicate Paper position.
func (s *Store) AppendStagedPaperBundleDecision(ctx context.Context, d StagedPaperBundleDecision) (bool, bool, error) {
	if err := validateStagedPaperBundleDecision(d); err != nil {
		return false, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback()
	var system, opportunity, eventID, cohort, certificateStatus, observed string
	var size, cost, fee, payoutFloor, netFloor float64
	var legCount int
	if err := tx.QueryRowContext(ctx, `SELECT system_id,opportunity_id,canonical_event_id,cohort,
certificate_status,observed_ts,size_units,total_cost,total_fee,payout_floor,net_floor,leg_count
FROM research_route_bundles WHERE bundle_id=?`, d.BundleID).Scan(&system, &opportunity, &eventID,
		&cohort, &certificateStatus, &observed, &size, &cost, &fee, &payoutFloor, &netFloor, &legCount); err != nil {
		return false, false, err
	}
	sourceAt, err := time.Parse(time.RFC3339Nano, observed)
	if err != nil || system != d.SystemID || opportunity != d.OpportunityID || eventID != d.CanonicalEventID ||
		cohort != d.Cohort || !sourceAt.Equal(d.Candidate) || legCount != d.LegCount ||
		math.Abs(size-d.Size) > 1e-9 || math.Abs(cost-d.Cost) > 1e-7 || math.Abs(fee-d.Fee) > 1e-7 ||
		math.Abs(payoutFloor-d.PayoutFloor) > 1e-7 || math.Abs(netFloor-d.NetFloor) > 1e-7 ||
		(d.Accept && certificateStatus != "verified") {
		return false, false, errors.New("staged Paper decision does not match its immutable source bundle")
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO staged_paper_bundle_attempts(
package_id,bundle_id,system_id,opportunity_id,canonical_event_id,cohort,venue_shape,candidate_ts,
decision_ts,size_units,total_cost,total_fee,payout_floor,net_floor,leg_count,live_state,evidence_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, d.PackageID, d.BundleID, d.SystemID, d.OpportunityID,
		d.CanonicalEventID, d.Cohort, d.VenueShape, d.Candidate.UTC().Format(time.RFC3339Nano),
		d.Decided.UTC().Format(time.RFC3339Nano), d.Size, d.Cost, d.Fee, d.PayoutFloor, d.NetFloor,
		d.LegCount, d.LiveState, d.EvidenceJSON)
	if err != nil {
		return false, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, false, err
	}
	accepted := d.Accept
	reason := d.Reason
	if accepted {
		claim, claimErr := tx.ExecContext(ctx, `INSERT OR IGNORE INTO staged_paper_bundle_claims(
system_id,opportunity_id,package_id,claimed_ts) VALUES(?,?,?,?)`, d.SystemID, d.OpportunityID,
			d.PackageID, d.Decided.UTC().Format(time.RFC3339Nano))
		if claimErr != nil {
			return false, false, claimErr
		}
		claimed, claimErr := claim.RowsAffected()
		if claimErr != nil {
			return false, false, claimErr
		}
		if claimed == 0 {
			accepted = false
			reason = "flat duplicate: this exact system opportunity already owns a Paper package"
		}
	}
	eventType, outcome := "rejected", "flat"
	var payout, realized any
	if accepted {
		eventType, outcome = "accepted", "open"
		payout, realized = nil, nil
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO staged_paper_bundle_events(
package_id,observed_ts,event_type,outcome_status,payout,realized_net,reason,evidence_json)
VALUES(?,?,?,?,?,?,?,?)`, d.PackageID, d.Decided.UTC().Format(time.RFC3339Nano), eventType,
		outcome, payout, realized, reason, d.EvidenceJSON); err != nil {
		return false, false, err
	}
	if err = tx.Commit(); err != nil {
		return false, false, err
	}
	return true, accepted, nil
}

// RecentNamedResearchRouteBundles returns every recent capacity row for the four concrete staged
// products, including rows that Paper must reject.  This makes the rejection funnel observable;
// it never changes the immutable research ledger or grants LIVE authority.
func (s *Store) RecentNamedResearchRouteBundles(ctx context.Context, since time.Time, limit int) ([]ResearchRouteBundle, error) {
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `SELECT bundle_id FROM research_route_bundles
WHERE observed_ts>=? AND system_id IN ('event-basket-lock','nested-ladder-lock','time-nested-lock','xvlock')
 AND NOT EXISTS(SELECT 1 FROM staged_paper_bundle_attempts p WHERE p.bundle_id=research_route_bundles.bundle_id)
ORDER BY observed_ts DESC,total_cost DESC,bundle_id DESC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, limit)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]ResearchRouteBundle, 0, len(ids))
	for _, id := range ids {
		b, found, err := s.ResearchRouteBundleByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, b)
		}
	}
	return out, nil
}

type StagedPaperBundlePendingSettlement struct {
	PackageID, BundleID, SystemID, OutcomeStatus, GradeReason, GradeEvidenceJSON string
	Accepted, Graded                                                             time.Time
	Size, Cost, Fee, Payout, GradeRealizedNet                                    float64
}

func stagedPaperTerminalOutcomeStatus(raw string) bool {
	status := strings.ToLower(strings.TrimSpace(raw))
	return strings.Contains(status, "settled") || strings.Contains(status, "resolved")
}

// PendingStagedPaperBundleSettlements exposes only packages with an append-only accepted event
// and an authoritative per-leg research grade.  Rejected attempts never appear and remain flat.
func (s *Store) PendingStagedPaperBundleSettlements(ctx context.Context, limit int) ([]StagedPaperBundlePendingSettlement, error) {
	if limit <= 0 || limit > 250 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.package_id,a.bundle_id,a.system_id,accept.observed_ts,
grade.observed_ts,grade.outcome_status,a.size_units,a.total_cost,a.total_fee,grade.payout,
grade.realized_net,grade.reason,grade.evidence_json
FROM staged_paper_bundle_attempts a
JOIN staged_paper_bundle_events accept ON accept.package_id=a.package_id AND accept.event_type='accepted'
JOIN research_route_bundle_events grade ON grade.bundle_id=a.bundle_id AND grade.event_type='grade'
WHERE NOT EXISTS(SELECT 1 FROM staged_paper_bundle_events done
 WHERE done.package_id=a.package_id AND done.event_type='settled')
ORDER BY grade.observed_ts,a.package_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StagedPaperBundlePendingSettlement{}
	for rows.Next() {
		var v StagedPaperBundlePendingSettlement
		var accepted, graded string
		if err := rows.Scan(&v.PackageID, &v.BundleID, &v.SystemID, &accepted, &graded,
			&v.OutcomeStatus, &v.Size, &v.Cost, &v.Fee, &v.Payout, &v.GradeRealizedNet, &v.GradeReason,
			&v.GradeEvidenceJSON); err != nil {
			return nil, err
		}
		v.Accepted, err = time.Parse(time.RFC3339Nano, accepted)
		if err != nil {
			return nil, err
		}
		v.Graded, err = time.Parse(time.RFC3339Nano, graded)
		if err != nil {
			return nil, err
		}
		if !stagedPaperTerminalOutcomeStatus(v.OutcomeStatus) || !validStagedPaperFinite(v.Payout) ||
			!validStagedPaperFinite(v.GradeRealizedNet) || v.Payout < 0 ||
			math.Abs(v.GradeRealizedNet-(v.Payout-v.Cost-v.Fee)) > 1e-7 || !json.Valid([]byte(v.GradeEvidenceJSON)) {
			return nil, errors.New("invalid authoritative grade joined to staged Paper package")
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) AppendStagedPaperBundleSettlement(ctx context.Context, v StagedPaperBundlePendingSettlement) (bool, error) {
	if strings.TrimSpace(v.PackageID) == "" || v.Graded.IsZero() || v.Graded.Before(v.Accepted) ||
		v.Size <= 0 || v.Cost <= 0 || v.Fee < 0 || v.Payout < 0 ||
		!validStagedPaperFinite(v.Size) || !validStagedPaperFinite(v.Cost) ||
		!validStagedPaperFinite(v.Fee) || !validStagedPaperFinite(v.Payout) ||
		!validStagedPaperFinite(v.GradeRealizedNet) || math.Abs(v.GradeRealizedNet-(v.Payout-v.Cost-v.Fee)) > 1e-7 ||
		!stagedPaperTerminalOutcomeStatus(v.OutcomeStatus) || strings.TrimSpace(v.GradeReason) == "" ||
		!json.Valid([]byte(v.GradeEvidenceJSON)) {
		return false, errors.New("invalid staged Paper bundle settlement")
	}
	realized := v.Payout - v.Cost - v.Fee
	if !validStagedPaperFinite(realized) {
		return false, errors.New("non-finite staged Paper bundle P&L")
	}
	evidence, _ := json.Marshal(map[string]any{
		"bundle_id": v.BundleID, "grade_reason": v.GradeReason,
		"grade_evidence":      json.RawMessage(v.GradeEvidenceJSON),
		"settlement_contract": "authoritative per-leg research grade joined to the exact accepted bundle",
	})
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO staged_paper_bundle_events(
package_id,observed_ts,event_type,outcome_status,payout,realized_net,reason,evidence_json)
VALUES(?,?,'settled',?,?,?,?,?)`, v.PackageID, v.Graded.UTC().Format(time.RFC3339Nano),
		v.OutcomeStatus, v.Payout, realized, "authoritative per-leg settlement: "+v.GradeReason, string(evidence))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type StagedPaperBundleFunds struct {
	OpenCount, ClosedCount          int
	OpenCost, Realized, ClosedUnits float64
}

type StagedPaperBundleResetReceipt struct {
	EpochAt          time.Time
	ExcludedOpen     int
	ExcludedSettled  int
	ExcludedAttempts int
}

// ResetStagedPaperBundlePortfolio advances the accounting boundary without rewriting any attempt,
// claim, or event. Pre-reset packages continue to receive their authoritative settlement event,
// but they can no longer consume current Combo exposure or change the new epoch's P&L.
func (s *Store) ResetStagedPaperBundlePortfolio(ctx context.Context, epoch time.Time) (StagedPaperBundleResetReceipt, error) {
	var out StagedPaperBundleResetReceipt
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
	if err = tx.QueryRowContext(ctx, `SELECT epoch_ts FROM staged_paper_bundle_meta WHERE id=1`).Scan(&current); err != nil {
		return out, err
	}
	currentAt, err := time.Parse(time.RFC3339Nano, current)
	if err != nil {
		return out, fmt.Errorf("parse staged Paper epoch: %w", err)
	}
	if epoch.Before(currentAt) {
		return out, errors.New("staged Paper epoch cannot move backwards")
	}
	cut := epoch.Format(time.RFC3339Nano)
	if err = tx.QueryRowContext(ctx, `SELECT
COUNT(*),
COALESCE(SUM(CASE WHEN accept.id IS NOT NULL AND settled.id IS NULL THEN 1 ELSE 0 END),0),
COALESCE(SUM(CASE WHEN settled.id IS NOT NULL THEN 1 ELSE 0 END),0)
FROM staged_paper_bundle_attempts a
LEFT JOIN staged_paper_bundle_events accept ON accept.package_id=a.package_id AND accept.event_type='accepted'
LEFT JOIN staged_paper_bundle_events settled ON settled.package_id=a.package_id AND settled.event_type='settled'
WHERE a.decision_ts<?`, cut).Scan(&out.ExcludedAttempts, &out.ExcludedOpen, &out.ExcludedSettled); err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE staged_paper_bundle_meta SET epoch_ts=?,updated_ts=? WHERE id=1`,
		cut, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return out, err
	}
	out.EpochAt = epoch
	return out, nil
}

func (s *Store) StagedPaperBundleEpoch(ctx context.Context) (time.Time, error) {
	var raw string
	if err := s.db.QueryRowContext(ctx, `SELECT epoch_ts FROM staged_paper_bundle_meta WHERE id=1`).Scan(&raw); err != nil {
		return time.Time{}, err
	}
	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse staged Paper epoch: %w", err)
	}
	return at, nil
}

// StagedPaperBundleFundsSince is the Combo portfolio accounting adapter. Open cost is all-in
// (legs plus exact fees); realized is fee-net. The effective boundary is the later of the durable
// reset epoch and since, so neither an old open nor its later settlement can leak into current
// exposure/P&L.
func (s *Store) StagedPaperBundleFundsSince(ctx context.Context, since time.Time) (StagedPaperBundleFunds, error) {
	var out StagedPaperBundleFunds
	epoch, err := s.StagedPaperBundleEpoch(ctx)
	if err != nil {
		return out, err
	}
	if since.After(epoch) {
		epoch = since.UTC()
	}
	// Start from the tiny accepted-claim set. Rejected capacity rows can be numerous and belong on
	// the explicit system-funnel query, never on the hot portfolio-equity path.
	rows, err := s.db.QueryContext(ctx, `SELECT a.size_units,a.total_cost,a.total_fee,
accept.observed_ts,settled.observed_ts,settled.realized_net
FROM staged_paper_bundle_claims claim
JOIN staged_paper_bundle_attempts a ON a.package_id=claim.package_id
JOIN staged_paper_bundle_events accept ON accept.package_id=a.package_id AND accept.event_type='accepted'
LEFT JOIN staged_paper_bundle_events settled ON settled.package_id=a.package_id AND settled.event_type='settled'
WHERE a.decision_ts>=?
ORDER BY a.package_id`, epoch.Format(time.RFC3339Nano))
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var size, cost, fee float64
		var accepted, settled sql.NullString
		var realized sql.NullFloat64
		if err := rows.Scan(&size, &cost, &fee, &accepted, &settled, &realized); err != nil {
			return out, err
		}
		if !accepted.Valid {
			return out, fmt.Errorf("staged Paper attempt has neither accepted nor rejected terminal decision")
		}
		if !settled.Valid {
			out.OpenCount++
			out.OpenCost += cost + fee
			continue
		}
		closedAt, err := time.Parse(time.RFC3339Nano, settled.String)
		if err != nil || !realized.Valid || !validStagedPaperFinite(realized.Float64) {
			return out, errors.New("invalid staged Paper terminal accounting row")
		}
		if !closedAt.Before(epoch) {
			out.ClosedCount++
			out.ClosedUnits += size
			out.Realized += realized.Float64
		}
	}
	return out, rows.Err()
}

type StagedPaperBundleSystemStat struct {
	SystemID                    string
	Attempts, Rejected, Open, N int
	Units, Realized             float64
}

func (s *Store) StagedPaperBundleSystemStats(ctx context.Context) ([]StagedPaperBundleSystemStat, error) {
	epoch, err := s.StagedPaperBundleEpoch(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.system_id,COUNT(*),
SUM(CASE WHEN reject.id IS NULL THEN 0 ELSE 1 END),
SUM(CASE WHEN accept.id IS NOT NULL AND settled.id IS NULL THEN 1 ELSE 0 END),
SUM(CASE WHEN settled.id IS NULL THEN 0 ELSE 1 END),
COALESCE(SUM(CASE WHEN settled.id IS NULL THEN 0 ELSE a.size_units END),0),
COALESCE(SUM(settled.realized_net),0)
FROM staged_paper_bundle_attempts a
LEFT JOIN staged_paper_bundle_events accept ON accept.package_id=a.package_id AND accept.event_type='accepted'
LEFT JOIN staged_paper_bundle_events reject ON reject.package_id=a.package_id AND reject.event_type='rejected'
LEFT JOIN staged_paper_bundle_events settled ON settled.package_id=a.package_id AND settled.event_type='settled'
WHERE a.decision_ts>=?
GROUP BY a.system_id ORDER BY a.system_id`, epoch.Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []StagedPaperBundleSystemStat{}
	for rows.Next() {
		var v StagedPaperBundleSystemStat
		if err := rows.Scan(&v.SystemID, &v.Attempts, &v.Rejected, &v.Open, &v.N, &v.Units, &v.Realized); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
