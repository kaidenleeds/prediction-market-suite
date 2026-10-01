package storage

// Durable staged execution journal for additive multi-leg systems.  The immutable research
// bundle remains the quote-time certificate; this ledger records the actual ordered route.  It is
// append-only so a restart can distinguish a clean refusal, a known fill, and an ambiguous write.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const stagedBundleExecutionSchema = `
CREATE TABLE IF NOT EXISTS staged_bundle_execution_intents (
 execution_id TEXT PRIMARY KEY,
 created_ts TEXT NOT NULL,
 bundle_id TEXT NOT NULL UNIQUE,
 system_id TEXT NOT NULL CHECK(system_id IN ('event-basket-lock','nested-ladder-lock','time-nested-lock','xvlock')),
 bundle_hash TEXT NOT NULL CHECK(length(bundle_hash)=64),
 proof_json TEXT NOT NULL CHECK(json_valid(proof_json)),
 ordered_legs_json TEXT NOT NULL CHECK(json_valid(ordered_legs_json)),
 ordered_legs_hash TEXT NOT NULL CHECK(length(ordered_legs_hash)=64),
 requested_size REAL NOT NULL CHECK(requested_size>0),
 max_all_in_unit REAL NOT NULL CHECK(max_all_in_unit>0),
 first_leg_index INTEGER NOT NULL CHECK(first_leg_index>=0 AND first_leg_index<6),
 route_policy TEXT NOT NULL CHECK(route_policy='staged_fok_v1'),
 FOREIGN KEY(bundle_id) REFERENCES research_route_bundles(bundle_id)
);
CREATE INDEX IF NOT EXISTS idx_staged_bundle_intent_created ON staged_bundle_execution_intents(created_ts,execution_id);
CREATE TRIGGER IF NOT EXISTS staged_bundle_intents_exact_bundle BEFORE INSERT ON staged_bundle_execution_intents
WHEN NOT EXISTS (
 SELECT 1 FROM research_route_bundles b WHERE b.bundle_id=NEW.bundle_id
  AND b.system_id=NEW.system_id AND b.certificate_status='verified' AND b.net_floor>0
  AND b.unwind_known=1 AND b.atomic_route=0 AND b.leg_count BETWEEN 2 AND 6
) BEGIN SELECT RAISE(ABORT,'staged execution requires a verified positive non-atomic bundle with known unwind'); END;
CREATE TRIGGER IF NOT EXISTS staged_bundle_intents_no_update BEFORE UPDATE ON staged_bundle_execution_intents BEGIN SELECT RAISE(ABORT,'immutable staged execution intent'); END;
CREATE TRIGGER IF NOT EXISTS staged_bundle_intents_no_delete BEFORE DELETE ON staged_bundle_execution_intents BEGIN SELECT RAISE(ABORT,'immutable staged execution intent'); END;

CREATE TABLE IF NOT EXISTS staged_bundle_execution_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 execution_id TEXT NOT NULL,
 event_seq INTEGER NOT NULL CHECK(event_seq>=1),
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN (
  'admitted','leg_intent','leg_submitted','leg_filled','leg_unfilled',
  'unwind_intent','unwind_submitted','unwind_filled','unwind_unfilled','reconciled','completed','rejected','frozen')),
 leg_index INTEGER CHECK(leg_index IS NULL OR (leg_index>=0 AND leg_index<6)),
 venue TEXT NOT NULL CHECK(venue IN ('','kalshi','polyus')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('','YES','NO')),
 action TEXT NOT NULL CHECK(action IN ('','BUY','SELL')),
 order_id TEXT NOT NULL,
 requested_qty REAL NOT NULL CHECK(requested_qty>=0),
 filled_qty REAL NOT NULL CHECK(filled_qty>=0 AND filled_qty<=requested_qty+0.000000001),
 average_price REAL NOT NULL CHECK(average_price>=0 AND average_price<=1),
 fee_total REAL NOT NULL CHECK(fee_total>=0),
 receipt_source TEXT NOT NULL,
 reason TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 FOREIGN KEY(execution_id) REFERENCES staged_bundle_execution_intents(execution_id),
 UNIQUE(execution_id,event_seq)
);
CREATE INDEX IF NOT EXISTS idx_staged_bundle_events_state ON staged_bundle_execution_events(execution_id,event_seq);
CREATE UNIQUE INDEX IF NOT EXISTS idx_staged_bundle_order_event ON staged_bundle_execution_events(execution_id,event_type,order_id)
 WHERE order_id<>'' AND event_type IN ('leg_submitted','unwind_submitted');
CREATE TRIGGER IF NOT EXISTS staged_bundle_events_no_update BEFORE UPDATE ON staged_bundle_execution_events BEGIN SELECT RAISE(ABORT,'append-only staged execution event'); END;
CREATE TRIGGER IF NOT EXISTS staged_bundle_events_no_delete BEFORE DELETE ON staged_bundle_execution_events BEGIN SELECT RAISE(ABORT,'append-only staged execution event'); END;
`

func migrateStagedBundleExecutionSchema(db *sql.DB) error {
	if _, err := db.Exec(stagedBundleExecutionSchema); err != nil {
		return err
	}
	// R148 was developed while the suite could already have created the first draft of this
	// table. Preserve those immutable rows and add the persisted full order without rebuilding the
	// journal. No production adapter existed for the draft, so a legacy [] row is intentionally
	// unreadable/fail-closed rather than guessing an execution order after restart.
	rows, err := db.Query(`PRAGMA table_info(staged_bundle_execution_intents)`)
	if err != nil {
		return err
	}
	found := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		found = found || name == "ordered_legs_json"
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !found {
		_, err = db.Exec(`ALTER TABLE staged_bundle_execution_intents ADD COLUMN ordered_legs_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(ordered_legs_json))`)
	}
	return err
}

type StagedBundleExecutionIntent struct {
	ExecutionID, BundleID, SystemID, BundleHash, OrderedLegsHash, RoutePolicy string
	Created                                                                   time.Time
	Proof                                                                     any
	OrderedLegs                                                               []int
	RequestedSize, MaxAllInUnit                                               float64
	FirstLegIndex                                                             int
}

type StagedBundleExecutionEvent struct {
	ID                                                           int64
	ExecutionID, EventType, Venue, Ticker, Side, Action, OrderID string
	ReceiptSource, Reason, EvidenceJSON                          string
	Sequence                                                     int
	Observed                                                     time.Time
	LegIndex                                                     *int
	RequestedQty, FilledQty, AveragePrice, FeeTotal              float64
}

type StagedBundleExecutionProof struct {
	SystemID, ProofSystem, Cohort string
	RunIDs                        []int64
	Venues                        []string
	// Mean and LowerBound are the sealed untouched fee-net dollars per route unit.  The
	// production staged allocator needs both: LowerBound is the Kelly edge, while Mean keeps the
	// same uncertainty shrink used by single-order Adaptive Allocation sizing.
	Mean, LowerBound float64
}

func stagedBundleProofSystem(system string) string {
	switch strings.ToLower(strings.TrimSpace(system)) {
	case "event-basket-lock", "nested-ladder-lock":
		return "payoff-constraint-solver"
	case "time-nested-lock":
		return "deadline-hazard-surface"
	case "xvlock":
		return "identity-challenged-cross-venue-lock"
	}
	return ""
}

// CurrentStagedBundleExecutionProof requires an independently sealed untouched taker result for
// every destination venue in the bundle.  A cross-venue bundle therefore cannot borrow a Kalshi
// result for its PolyUS leg (or vice versa), and a rolling leaderboard point is never sufficient.
func (s *Store) CurrentStagedBundleExecutionProof(ctx context.Context, b ResearchRouteBundle) (StagedBundleExecutionProof, bool, error) {
	proofSystem := stagedBundleProofSystem(b.SystemID)
	if proofSystem == "" || b.CertificateStatus != "verified" || b.NetFloor <= 0 || !b.UnwindKnown || b.AtomicRoute {
		return StagedBundleExecutionProof{}, false, nil
	}
	venueSet := map[string]bool{}
	for _, leg := range b.Legs {
		if leg.Venue != "kalshi" && leg.Venue != "polyus" {
			return StagedBundleExecutionProof{}, false, nil
		}
		venueSet[leg.Venue] = true
	}
	venues := make([]string, 0, len(venueSet))
	for venue := range venueSet {
		venues = append(venues, venue)
	}
	sort.Strings(venues)
	out := StagedBundleExecutionProof{SystemID: b.SystemID, ProofSystem: proofSystem, Cohort: b.Cohort,
		Venues: venues, Mean: math.Inf(1), LowerBound: math.Inf(1)}
	for _, venue := range venues {
		var runID int64
		var mean, lower float64
		err := s.db.QueryRowContext(ctx, `SELECT r.id,s.untouched_mean,s.untouched_lower
FROM research_inference_preregistrations p
JOIN research_inference_runs r ON r.preregistration_id=p.preregistration_id
JOIN research_inference_system_results s ON s.run_id=r.id AND s.system_id=p.system_id
WHERE p.system_id=? AND p.experiment_version=? AND p.cohort=? AND p.venue=? AND p.route='taker'
 AND r.status='sealed_preregistered_untouched' AND s.state='PREREGISTERED_UNTOUCHED_PASS'
 AND s.preregistered_untouched_gate_pass=1 AND s.execution_candidate=1
 AND s.untouched_bounds_known=1 AND s.untouched_lower>0
 AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
 AND s.funded=0 AND s.paper_authority=0 AND s.live_authority=0
ORDER BY r.id DESC LIMIT 1`, proofSystem, b.ExperimentVersion, b.Cohort, venue).Scan(&runID, &mean, &lower)
		if errors.Is(err, sql.ErrNoRows) {
			return StagedBundleExecutionProof{}, false, nil
		}
		if err != nil {
			return StagedBundleExecutionProof{}, false, err
		}
		out.RunIDs = append(out.RunIDs, runID)
		out.Mean = math.Min(out.Mean, mean)
		out.LowerBound = math.Min(out.LowerBound, lower)
	}
	if len(out.RunIDs) != len(venues) || len(out.RunIDs) == 0 || !finiteStaged(out.Mean) ||
		!finiteStaged(out.LowerBound) || out.Mean <= 0 || out.LowerBound <= 0 || out.LowerBound > out.Mean+1e-12 {
		return StagedBundleExecutionProof{}, false, nil
	}
	return out, true, nil
}

func finiteStaged(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validateStagedIntent(in StagedBundleExecutionIntent) error {
	if strings.TrimSpace(in.ExecutionID) == "" || strings.TrimSpace(in.BundleID) == "" ||
		strings.TrimSpace(in.SystemID) == "" || len(in.BundleHash) != 64 || len(in.OrderedLegsHash) != 64 ||
		in.Created.IsZero() || !finiteStaged(in.RequestedSize) || in.RequestedSize <= 0 ||
		!finiteStaged(in.MaxAllInUnit) || in.MaxAllInUnit <= 0 || in.FirstLegIndex < 0 ||
		in.FirstLegIndex >= 6 || in.RoutePolicy != "staged_fok_v1" {
		return errors.New("invalid staged bundle execution intent")
	}
	if len(in.OrderedLegs) < 2 || len(in.OrderedLegs) > 6 || in.FirstLegIndex != in.OrderedLegs[0] {
		return errors.New("invalid staged bundle execution leg order")
	}
	seen := make(map[int]bool, len(in.OrderedLegs))
	for _, idx := range in.OrderedLegs {
		if idx < 0 || idx >= len(in.OrderedLegs) || seen[idx] {
			return errors.New("staged bundle execution leg order is not a permutation")
		}
		seen[idx] = true
	}
	return nil
}

func (s *Store) InsertStagedBundleExecutionIntent(ctx context.Context, in StagedBundleExecutionIntent) (bool, error) {
	if err := validateStagedIntent(in); err != nil {
		return false, err
	}
	proof, err := json.Marshal(in.Proof)
	if err != nil || len(proof) == 0 || string(proof) == "null" {
		return false, errors.New("staged execution proof is missing")
	}
	ordered, err := json.Marshal(in.OrderedLegs)
	if err != nil || !json.Valid(ordered) {
		return false, errors.New("staged execution leg order is missing")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO staged_bundle_execution_intents(
execution_id,created_ts,bundle_id,system_id,bundle_hash,proof_json,ordered_legs_json,ordered_legs_hash,requested_size,
max_all_in_unit,first_leg_index,route_policy) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, in.ExecutionID,
		in.Created.UTC().Format(time.RFC3339Nano), in.BundleID, in.SystemID, in.BundleHash, string(proof),
		string(ordered), in.OrderedLegsHash, in.RequestedSize, in.MaxAllInUnit, in.FirstLegIndex, in.RoutePolicy)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func validStagedEvent(e StagedBundleExecutionEvent) error {
	if strings.TrimSpace(e.ExecutionID) == "" || e.Observed.IsZero() || e.EventType == "" ||
		!finiteStaged(e.RequestedQty) || !finiteStaged(e.FilledQty) || !finiteStaged(e.AveragePrice) ||
		!finiteStaged(e.FeeTotal) || e.RequestedQty < 0 || e.FilledQty < 0 ||
		e.FilledQty > e.RequestedQty+1e-9 || e.AveragePrice < 0 || e.AveragePrice > 1 ||
		e.FeeTotal < 0 || !json.Valid([]byte(e.EvidenceJSON)) {
		return errors.New("invalid staged bundle execution event")
	}
	if e.LegIndex != nil && (*e.LegIndex < 0 || *e.LegIndex >= 6) {
		return errors.New("invalid staged bundle leg index")
	}
	return nil
}

// AppendStagedBundleExecutionEvent allocates sequence numbers inside the same transaction as the
// insert. Two recovery workers can race safely: only one can own the next sequence.
func (s *Store) AppendStagedBundleExecutionEvent(ctx context.Context, e StagedBundleExecutionEvent) (StagedBundleExecutionEvent, error) {
	if err := validStagedEvent(e); err != nil {
		return e, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return e, err
	}
	defer tx.Rollback()
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(event_seq),0)+1 FROM staged_bundle_execution_events WHERE execution_id=?`, e.ExecutionID).Scan(&e.Sequence); err != nil {
		return e, err
	}
	var leg any
	if e.LegIndex != nil {
		leg = *e.LegIndex
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO staged_bundle_execution_events(execution_id,event_seq,
observed_ts,event_type,leg_index,venue,ticker,side,action,order_id,requested_qty,filled_qty,average_price,
fee_total,receipt_source,reason,evidence_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, e.ExecutionID,
		e.Sequence, e.Observed.UTC().Format(time.RFC3339Nano), e.EventType, leg, e.Venue, e.Ticker, e.Side,
		e.Action, e.OrderID, e.RequestedQty, e.FilledQty, e.AveragePrice, e.FeeTotal,
		e.ReceiptSource, e.Reason, e.EvidenceJSON)
	if err != nil {
		return e, err
	}
	e.ID, err = res.LastInsertId()
	if err != nil {
		return e, err
	}
	if err = tx.Commit(); err != nil {
		return e, err
	}
	return e, nil
}

func (s *Store) StagedBundleExecutionEvents(ctx context.Context, executionID string) ([]StagedBundleExecutionEvent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,event_seq,observed_ts,event_type,leg_index,venue,ticker,
side,action,order_id,requested_qty,filled_qty,average_price,fee_total,receipt_source,reason,evidence_json
FROM staged_bundle_execution_events WHERE execution_id=? ORDER BY event_seq`, executionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StagedBundleExecutionEvent
	for rows.Next() {
		var e StagedBundleExecutionEvent
		var observed string
		var leg sql.NullInt64
		if err := rows.Scan(&e.ID, &e.Sequence, &observed, &e.EventType, &leg, &e.Venue, &e.Ticker,
			&e.Side, &e.Action, &e.OrderID, &e.RequestedQty, &e.FilledQty, &e.AveragePrice,
			&e.FeeTotal, &e.ReceiptSource, &e.Reason, &e.EvidenceJSON); err != nil {
			return nil, err
		}
		e.ExecutionID = executionID
		e.Observed, err = time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			return nil, err
		}
		if leg.Valid {
			v := int(leg.Int64)
			e.LegIndex = &v
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) PendingStagedBundleExecutionIDs(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	return s.pendingStagedBundleExecutionIDs(ctx, limit)
}

// AllPendingStagedBundleExecutionIDs is the startup money-safety scan. It is deliberately not
// capped: a backlog of harmless admitted-only research intents must never hide a newer submitted,
// filled, unwind, or intent-without-receipt state beyond an arbitrary first page.
func (s *Store) AllPendingStagedBundleExecutionIDs(ctx context.Context) ([]string, error) {
	return s.pendingStagedBundleExecutionIDs(ctx, 0)
}

// HasPendingStagedBundleExecutions is the no-lock presence half of startup recovery.  A timeout
// here means state is unknown and callers must pause/retry; true means the full immutable journal
// must be loaded and any later read failure must fail closed.
func (s *Store) HasPendingStagedBundleExecutions(ctx context.Context) (bool, error) {
	var present int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
SELECT 1 FROM staged_bundle_execution_intents i
WHERE NOT EXISTS (SELECT 1 FROM staged_bundle_execution_events e
 WHERE e.execution_id=i.execution_id AND e.event_type IN ('completed','rejected','frozen'))
LIMIT 1)`).Scan(&present)
	return present != 0, err
}

func (s *Store) pendingStagedBundleExecutionIDs(ctx context.Context, limit int) ([]string, error) {
	query := `SELECT i.execution_id FROM staged_bundle_execution_intents i
WHERE NOT EXISTS (SELECT 1 FROM staged_bundle_execution_events e WHERE e.execution_id=i.execution_id
	 AND e.event_type IN ('completed','rejected','frozen'))
ORDER BY CASE WHEN EXISTS (SELECT 1 FROM staged_bundle_execution_events risk
 WHERE risk.execution_id=i.execution_id AND risk.event_type IN
 ('leg_intent','unwind_intent','leg_submitted','unwind_submitted','leg_filled','unwind_filled'))
 THEN 0 ELSE 1 END, i.created_ts,i.execution_id`
	args := []any{}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) StagedBundleExecutionIntentByID(ctx context.Context, id string) (StagedBundleExecutionIntent, bool, error) {
	return s.stagedBundleExecutionIntent(ctx, "execution_id", strings.TrimSpace(id))
}

// StagedBundleExecutionIntentByBundleID returns the one immutable execution identity owned by a
// bundle. It is used when a renewed proof hashes to a different proposed execution id: bundle_id
// uniqueness means recovery must resume the already-durable journal, never return an invented id.
func (s *Store) StagedBundleExecutionIntentByBundleID(ctx context.Context, bundleID string) (StagedBundleExecutionIntent, bool, error) {
	return s.stagedBundleExecutionIntent(ctx, "bundle_id", strings.TrimSpace(bundleID))
}

func (s *Store) stagedBundleExecutionIntent(ctx context.Context, column, value string) (StagedBundleExecutionIntent, bool, error) {
	var in StagedBundleExecutionIntent
	var created, proof, ordered string
	if value == "" || (column != "execution_id" && column != "bundle_id") {
		return in, false, nil
	}
	query := `SELECT execution_id,created_ts,bundle_id,system_id,bundle_hash,
proof_json,ordered_legs_json,ordered_legs_hash,requested_size,max_all_in_unit,first_leg_index,route_policy
FROM staged_bundle_execution_intents WHERE ` + column + `=?`
	err := s.db.QueryRowContext(ctx, query, value).Scan(&in.ExecutionID,
		&created, &in.BundleID, &in.SystemID, &in.BundleHash, &proof, &ordered, &in.OrderedLegsHash,
		&in.RequestedSize, &in.MaxAllInUnit, &in.FirstLegIndex, &in.RoutePolicy)
	if errors.Is(err, sql.ErrNoRows) {
		return in, false, nil
	}
	if err != nil {
		return in, false, err
	}
	in.Created, err = time.Parse(time.RFC3339Nano, created)
	if err == nil {
		err = json.Unmarshal([]byte(proof), &in.Proof)
	}
	if err == nil {
		err = json.Unmarshal([]byte(ordered), &in.OrderedLegs)
	}
	if err == nil {
		err = validateStagedIntent(in)
	}
	if err != nil {
		return in, false, fmt.Errorf("read staged execution intent: %w", err)
	}
	return in, true, nil
}
