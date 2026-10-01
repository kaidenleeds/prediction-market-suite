package storage

// Immutable, typed multi-leg research routes. A bundle is a counterfactual executable quote, not
// an order or authority grant. Ordered legs preserve the exact book/fee/payoff inputs used by the
// solver; terminal events later grade every leg from its own venue settlement without claiming a
// fill. No table in this schema is allowed to fund, paper-trade, or LIVE-trade a route.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"
)

const researchBundleSchema = `
CREATE TABLE IF NOT EXISTS research_route_bundles (
 bundle_id TEXT PRIMARY KEY,
 observed_ts TEXT NOT NULL,
 system_id TEXT NOT NULL,
 cohort TEXT NOT NULL,
 experiment_version INTEGER NOT NULL CHECK(experiment_version>0),
 opportunity_id TEXT NOT NULL,
 canonical_event_id TEXT NOT NULL,
 event_version INTEGER NOT NULL CHECK(event_version>0),
 certificate_hash TEXT NOT NULL,
 certificate_status TEXT NOT NULL CHECK(certificate_status IN ('verified','structural','unknown')),
 route_kind TEXT NOT NULL CHECK(route_kind IN ('all_leg_taker','kalshi_rfq','cross_venue_non_atomic')),
 atomic_route INTEGER NOT NULL CHECK(atomic_route IN (0,1)),
 leg_count INTEGER NOT NULL CHECK(leg_count>=2 AND leg_count<=6),
 state_count INTEGER NOT NULL CHECK(state_count>=1),
 size_units REAL NOT NULL CHECK(size_units>0),
 total_cost REAL NOT NULL CHECK(total_cost>0),
 total_fee REAL NOT NULL CHECK(total_fee>=0),
 payout_floor REAL NOT NULL CHECK(payout_floor>=0),
 net_floor REAL NOT NULL,
 partial_fill_worst REAL NOT NULL CHECK(partial_fill_worst<=0),
 unwind_worst REAL NOT NULL CHECK(unwind_worst<=0),
 unwind_known INTEGER NOT NULL CHECK(unwind_known IN (0,1)),
 decision_latency_ms REAL NOT NULL CHECK(decision_latency_ms>=0),
 latency_known INTEGER NOT NULL CHECK(latency_known IN (0,1)),
 state_vector_hash TEXT NOT NULL,
 blocker TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(system_id,opportunity_id,observed_ts,size_units,state_vector_hash)
);
CREATE INDEX IF NOT EXISTS idx_rrbundle_pending ON research_route_bundles(observed_ts,bundle_id);
CREATE INDEX IF NOT EXISTS idx_rrbundle_system ON research_route_bundles(system_id,observed_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_route_bundles_no_update BEFORE UPDATE ON research_route_bundles BEGIN SELECT RAISE(ABORT,'immutable research route bundle'); END;
CREATE TRIGGER IF NOT EXISTS research_route_bundles_no_delete BEFORE DELETE ON research_route_bundles BEGIN SELECT RAISE(ABORT,'immutable research route bundle'); END;

CREATE TABLE IF NOT EXISTS research_route_bundle_legs (
 bundle_id TEXT NOT NULL,
 leg_index INTEGER NOT NULL CHECK(leg_index>=0 AND leg_index<6),
 leg_id TEXT NOT NULL,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus','polymarket')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 payoff_id TEXT NOT NULL,
 quantity REAL NOT NULL CHECK(quantity>0),
 integrated_cost REAL NOT NULL CHECK(integrated_cost>0),
 exact_fee REAL NOT NULL CHECK(exact_fee>=0),
 visible_depth REAL NOT NULL CHECK(visible_depth>=quantity),
 tick_size REAL NOT NULL CHECK(tick_size>0),
 quote_age_s REAL NOT NULL CHECK(quote_age_s>=0),
 book_source TEXT NOT NULL,
 source_clock_id TEXT NOT NULL,
 fee_source TEXT NOT NULL,
 levels_json TEXT NOT NULL CHECK(json_valid(levels_json)),
 payoff_json TEXT NOT NULL CHECK(json_valid(payoff_json)),
 unwind_known INTEGER NOT NULL CHECK(unwind_known IN (0,1)),
 unwind_book_source TEXT NOT NULL,
 unwind_fee_source TEXT NOT NULL,
 unwind_levels_json TEXT NOT NULL CHECK(json_valid(unwind_levels_json)),
 PRIMARY KEY(bundle_id,leg_index),
 UNIQUE(bundle_id,leg_id),
 FOREIGN KEY(bundle_id) REFERENCES research_route_bundles(bundle_id)
);
CREATE INDEX IF NOT EXISTS idx_rrbundle_leg_ticker ON research_route_bundle_legs(venue,ticker,bundle_id);
CREATE TRIGGER IF NOT EXISTS research_route_bundle_legs_no_update BEFORE UPDATE ON research_route_bundle_legs BEGIN SELECT RAISE(ABORT,'immutable research route bundle leg'); END;
CREATE TRIGGER IF NOT EXISTS research_route_bundle_legs_no_delete BEFORE DELETE ON research_route_bundle_legs BEGIN SELECT RAISE(ABORT,'immutable research route bundle leg'); END;

CREATE TABLE IF NOT EXISTS research_route_bundle_states (
 bundle_id TEXT NOT NULL,
 state_index INTEGER NOT NULL CHECK(state_index>=0),
 state_id TEXT NOT NULL,
 payout REAL NOT NULL CHECK(payout>=0),
 net REAL NOT NULL,
 PRIMARY KEY(bundle_id,state_index),
 UNIQUE(bundle_id,state_id),
 FOREIGN KEY(bundle_id) REFERENCES research_route_bundles(bundle_id)
);
CREATE TRIGGER IF NOT EXISTS research_route_bundle_states_no_update BEFORE UPDATE ON research_route_bundle_states BEGIN SELECT RAISE(ABORT,'immutable research route bundle state'); END;
CREATE TRIGGER IF NOT EXISTS research_route_bundle_states_no_delete BEFORE DELETE ON research_route_bundle_states BEGIN SELECT RAISE(ABORT,'immutable research route bundle state'); END;

CREATE TABLE IF NOT EXISTS research_route_bundle_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 bundle_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('grade','rfq_quote','promotion_check')),
 outcome_status TEXT NOT NULL,
 payout REAL,
 realized_net REAL,
 capital_seconds REAL NOT NULL DEFAULT 0 CHECK(capital_seconds>=0),
 actual_fill INTEGER NOT NULL DEFAULT 0 CHECK(actual_fill=0),
 atomic_fill INTEGER NOT NULL DEFAULT 0 CHECK(atomic_fill=0),
 reason TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 FOREIGN KEY(bundle_id) REFERENCES research_route_bundles(bundle_id),
 UNIQUE(bundle_id,event_type)
);
CREATE INDEX IF NOT EXISTS idx_rrbundle_event ON research_route_bundle_events(bundle_id,id);
CREATE TRIGGER IF NOT EXISTS research_route_bundle_events_no_update BEFORE UPDATE ON research_route_bundle_events BEGIN SELECT RAISE(ABORT,'append-only research route bundle event'); END;
CREATE TRIGGER IF NOT EXISTS research_route_bundle_events_no_delete BEFORE DELETE ON research_route_bundle_events BEGIN SELECT RAISE(ABORT,'append-only research route bundle event'); END;
`

func migrateResearchBundleSchema(db *sql.DB) error {
	// Early R139 development builds may already have created the table. The additive identity field
	// is deliberately defaulted to unknown: old rows may never be upgraded into executable proof by
	// inference. A fresh row must carry the certificate status recorded by its collector.
	_, _ = db.Exec(`ALTER TABLE research_route_bundles ADD COLUMN certificate_status TEXT NOT NULL DEFAULT 'unknown' CHECK(certificate_status IN ('verified','structural','unknown'))`)
	_, _ = db.Exec(`ALTER TABLE research_route_bundles ADD COLUMN cohort TEXT NOT NULL DEFAULT 'legacy-unknown'`)
	if _, err := db.Exec(researchBundleSchema); err != nil {
		return err
	}
	if err := migrateResearchBundlePromotionSchema(db); err != nil {
		return err
	}
	if err := migrateStagedBundleExecutionSchema(db); err != nil {
		return err
	}
	return migrateR148StagedPaperBundleSchema(db)
}

type ResearchRouteBundleLevel struct {
	Price, Quantity float64
	FeeQuotes       []ResearchRouteBundleFeeQuote
}

type ResearchRouteBundleFeeQuote struct{ Quantity, Total float64 }

type ResearchRouteBundleLeg struct {
	Index                                                       int
	LegID, Venue, Ticker, Side, PayoffID                        string
	BookSource, SourceClockID, FeeSource                        string
	UnwindBookSource, UnwindFeeSource                           string
	Quantity, IntegratedCost, ExactFee, VisibleDepth, Tick, Age float64
	Levels, UnwindLevels                                        []ResearchRouteBundleLevel
	Payoff                                                      []float64
	UnwindKnown                                                 bool
}

type ResearchRouteBundleState struct {
	Index   int
	StateID string
	Payout  float64
}

type ResearchRouteBundle struct {
	BundleID, SystemID, Cohort, OpportunityID, CanonicalEventID, CertificateHash string
	CertificateStatus                                                            string
	RouteKind, StateVectorHash, Blocker                                          string
	Observed                                                                     time.Time
	ExperimentVersion, EventVersion                                              int
	AtomicRoute, UnwindKnown, LatencyKnown                                       bool
	Size, Cost, Fee, PayoutFloor, NetFloor                                       float64
	PartialFillWorst, UnwindWorst, DecisionLatencyMS                             float64
	Legs                                                                         []ResearchRouteBundleLeg
	States                                                                       []ResearchRouteBundleState
	Evidence                                                                     any
}

func finiteBundle(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validateBundleLevels(levels []ResearchRouteBundleLevel, required float64) error {
	if len(levels) == 0 {
		return errors.New("empty bundle book")
	}
	depth := 0.0
	for _, level := range levels {
		if !finiteBundle(level.Price) || !finiteBundle(level.Quantity) || level.Price <= 0 || level.Price >= 1 || level.Quantity <= 0 {
			return errors.New("invalid bundle book level")
		}
		depth += level.Quantity
		last := 0.0
		for _, quote := range level.FeeQuotes {
			if !finiteBundle(quote.Quantity) || !finiteBundle(quote.Total) || quote.Quantity <= last || quote.Total < 0 || quote.Quantity > level.Quantity+1e-9 {
				return errors.New("invalid bundle exact fee quote")
			}
			last = quote.Quantity
		}
	}
	if depth+1e-9 < required {
		return errors.New("bundle book lacks requested depth")
	}
	return nil
}

func bundleLevelFeeAt(level ResearchRouteBundleLevel, quantity float64) (float64, bool) {
	for _, quote := range level.FeeQuotes {
		if math.Abs(quote.Quantity-quantity) <= 1e-9 {
			return quote.Total, true
		}
	}
	return 0, false
}

func bundleCostAt(levels []ResearchRouteBundleLevel, quantity float64) (cost, fee float64, ok bool) {
	remaining := quantity
	for _, level := range levels {
		take := math.Min(remaining, level.Quantity)
		if take <= 1e-12 {
			continue
		}
		qfee, known := bundleLevelFeeAt(level, take)
		if !known {
			return 0, 0, false
		}
		cost, fee, remaining = cost+take*level.Price, fee+qfee, remaining-take
		if remaining <= 1e-12 {
			return cost, fee, true
		}
	}
	return 0, 0, false
}

func validateResearchRouteBundle(b ResearchRouteBundle) error {
	if b.BundleID == "" || b.SystemID == "" || b.Cohort == "" || b.OpportunityID == "" || b.CanonicalEventID == "" ||
		b.CertificateHash == "" || b.StateVectorHash == "" || b.ExperimentVersion <= 0 || b.EventVersion <= 0 ||
		len(b.Legs) < 2 || len(b.Legs) > 6 || len(b.States) == 0 || b.Size <= 0 || b.Cost <= 0 || b.Fee < 0 ||
		b.PayoutFloor < 0 || b.PartialFillWorst > 0 || b.UnwindWorst > 0 || b.DecisionLatencyMS < 0 || !b.LatencyKnown {
		return errors.New("invalid research route bundle")
	}
	if b.CertificateStatus != "verified" && b.CertificateStatus != "structural" && b.CertificateStatus != "unknown" {
		return errors.New("invalid research bundle certificate status")
	}
	if b.RouteKind != "all_leg_taker" && b.RouteKind != "kalshi_rfq" && b.RouteKind != "cross_venue_non_atomic" {
		return errors.New("invalid research bundle route kind")
	}
	if b.AtomicRoute && b.RouteKind != "kalshi_rfq" {
		return errors.New("only a venue RFQ may claim atomic route")
	}
	for _, v := range []float64{b.Size, b.Cost, b.Fee, b.PayoutFloor, b.NetFloor, b.PartialFillWorst, b.UnwindWorst, b.DecisionLatencyMS} {
		if !finiteBundle(v) {
			return errors.New("non-finite research route bundle")
		}
	}
	seenLegs := map[string]bool{}
	for i, leg := range b.Legs {
		if leg.Index != i || leg.Index < 0 || leg.Index >= 6 || leg.LegID == "" || seenLegs[leg.LegID] ||
			(leg.Venue != "kalshi" && leg.Venue != "polyus" && leg.Venue != "polymarket") || leg.Ticker == "" ||
			(leg.Side != "YES" && leg.Side != "NO") || leg.PayoffID == "" || leg.Quantity != b.Size ||
			leg.IntegratedCost <= 0 || leg.ExactFee < 0 || leg.VisibleDepth+1e-9 < leg.Quantity || leg.Tick <= 0 || leg.Age < 0 ||
			leg.BookSource == "" || leg.SourceClockID == "" || leg.FeeSource == "" || len(leg.Payoff) != len(b.States) {
			return errors.New("invalid typed research bundle leg")
		}
		if err := validateBundleLevels(leg.Levels, leg.Quantity); err != nil {
			return err
		}
		cost, fee, exact := bundleCostAt(leg.Levels, leg.Quantity)
		if !exact || math.Abs(cost-leg.IntegratedCost) > 1e-7 || math.Abs(fee-leg.ExactFee) > 1e-7 {
			return errors.New("bundle leg cost/fee does not match its frozen full book")
		}
		if leg.UnwindKnown {
			if leg.UnwindBookSource == "" || leg.UnwindFeeSource == "" {
				return errors.New("known unwind lacks authority")
			}
			if err := validateBundleLevels(leg.UnwindLevels, leg.Quantity); err != nil {
				return err
			}
		}
		seenLegs[leg.LegID] = true
	}
	for i, state := range b.States {
		if state.Index != i || state.StateID == "" || !finiteBundle(state.Payout) || state.Payout < 0 {
			return errors.New("invalid research bundle state")
		}
	}
	return nil
}

// InsertResearchRouteBundle writes header, ordered legs, and state vector atomically. Any missing
// money-truth field rejects the whole bundle; a partial header can never look healthy.
func (s *Store) InsertResearchRouteBundle(ctx context.Context, b ResearchRouteBundle) (bool, error) {
	if b.Observed.IsZero() {
		b.Observed = time.Now().UTC()
	}
	if err := validateResearchRouteBundle(b); err != nil {
		return false, err
	}
	evidence, err := json.Marshal(b.Evidence)
	if err != nil {
		return false, err
	}
	if len(evidence) == 0 || string(evidence) == "null" {
		evidence = []byte("{}")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_route_bundles(
bundle_id,observed_ts,system_id,cohort,experiment_version,opportunity_id,canonical_event_id,event_version,
certificate_hash,certificate_status,route_kind,atomic_route,leg_count,state_count,size_units,total_cost,total_fee,payout_floor,
net_floor,partial_fill_worst,unwind_worst,unwind_known,decision_latency_ms,latency_known,state_vector_hash,
blocker,evidence_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, b.BundleID, b.Observed.UTC().Format(time.RFC3339Nano),
		b.SystemID, b.Cohort, b.ExperimentVersion, b.OpportunityID, b.CanonicalEventID, b.EventVersion, b.CertificateHash,
		b.CertificateStatus, b.RouteKind, boolInt(b.AtomicRoute), len(b.Legs), len(b.States), b.Size, b.Cost, b.Fee, b.PayoutFloor,
		b.NetFloor, b.PartialFillWorst, b.UnwindWorst, boolInt(b.UnwindKnown), b.DecisionLatencyMS,
		boolInt(b.LatencyKnown), b.StateVectorHash, b.Blocker, string(evidence))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return false, err
	}
	for _, leg := range b.Legs {
		levels, _ := json.Marshal(leg.Levels)
		payoff, _ := json.Marshal(leg.Payoff)
		unwind, _ := json.Marshal(leg.UnwindLevels)
		if _, err = tx.ExecContext(ctx, `INSERT INTO research_route_bundle_legs(
bundle_id,leg_index,leg_id,venue,ticker,side,payoff_id,quantity,integrated_cost,exact_fee,visible_depth,
tick_size,quote_age_s,book_source,source_clock_id,fee_source,levels_json,payoff_json,unwind_known,
unwind_book_source,unwind_fee_source,unwind_levels_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			b.BundleID, leg.Index, leg.LegID, leg.Venue, leg.Ticker, leg.Side, leg.PayoffID, leg.Quantity,
			leg.IntegratedCost, leg.ExactFee, leg.VisibleDepth, leg.Tick, leg.Age, leg.BookSource,
			leg.SourceClockID, leg.FeeSource, string(levels), string(payoff), boolInt(leg.UnwindKnown),
			leg.UnwindBookSource, leg.UnwindFeeSource, string(unwind)); err != nil {
			return false, err
		}
	}
	for _, state := range b.States {
		if _, err = tx.ExecContext(ctx, `INSERT INTO research_route_bundle_states(bundle_id,state_index,state_id,payout,net)
VALUES(?,?,?,?,?)`, b.BundleID, state.Index, state.StateID, state.Payout, state.Payout-b.Cost-b.Fee); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

type ResearchRouteBundlePendingGrade struct {
	BundleID, SystemID, CanonicalEventID string
	Observed                             time.Time
	Cost, Fee, Size                      float64
	Legs                                 []ResearchRouteBundleLeg
}

func (s *Store) PendingResearchRouteBundleGrades(ctx context.Context, limit int) ([]ResearchRouteBundlePendingGrade, error) {
	if limit <= 0 || limit > 250 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT bundle_id,system_id,canonical_event_id,observed_ts,total_cost,total_fee,size_units
FROM research_route_bundles b WHERE NOT EXISTS(SELECT 1 FROM research_route_bundle_events e
 WHERE e.bundle_id=b.bundle_id AND e.event_type='grade')
 AND NOT EXISTS (
  SELECT 1 FROM research_route_bundle_legs l WHERE l.bundle_id=b.bundle_id
   AND NOT EXISTS (SELECT 1 FROM venue_settlements v
    WHERE v.platform=l.venue AND v.ticker=l.ticker AND v.resolved_at>=b.observed_ts)
   AND NOT EXISTS (SELECT 1 FROM signal_log s
    WHERE s.platform=l.venue AND s.ticker=l.ticker AND s.resolved=1
      AND s.settle_val>=0 AND s.settle_val<=1 AND s.resolved_at>=b.observed_ts)
 ) ORDER BY observed_ts,bundle_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ResearchRouteBundlePendingGrade{}
	for rows.Next() {
		var v ResearchRouteBundlePendingGrade
		var observed string
		if err := rows.Scan(&v.BundleID, &v.SystemID, &v.CanonicalEventID, &observed, &v.Cost, &v.Fee, &v.Size); err != nil {
			return nil, err
		}
		v.Observed, err = time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			return nil, err
		}
		legRows, qerr := s.db.QueryContext(ctx, `SELECT leg_index,leg_id,venue,ticker,side,payoff_id,quantity
FROM research_route_bundle_legs WHERE bundle_id=? ORDER BY leg_index`, v.BundleID)
		if qerr != nil {
			return nil, qerr
		}
		for legRows.Next() {
			var leg ResearchRouteBundleLeg
			if qerr = legRows.Scan(&leg.Index, &leg.LegID, &leg.Venue, &leg.Ticker, &leg.Side, &leg.PayoffID, &leg.Quantity); qerr != nil {
				legRows.Close()
				return nil, qerr
			}
			v.Legs = append(v.Legs, leg)
		}
		qerr = legRows.Close()
		if qerr != nil {
			return nil, qerr
		}
		if len(v.Legs) < 2 || len(v.Legs) > 6 {
			return nil, errors.New("stored research bundle has invalid leg cardinality")
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type ResearchRouteBundleGrade struct {
	BundleID, OutcomeStatus, Reason, EvidenceJSON string
	Observed                                      time.Time
	Payout, RealizedNet, CapitalSeconds           float64
}

func (s *Store) AppendResearchRouteBundleGrade(ctx context.Context, g ResearchRouteBundleGrade) (bool, error) {
	if g.BundleID == "" || g.Observed.IsZero() || !finiteBundle(g.Payout) || !finiteBundle(g.RealizedNet) ||
		!finiteBundle(g.CapitalSeconds) || g.Payout < 0 || g.CapitalSeconds < 0 || g.OutcomeStatus == "" ||
		g.Reason == "" || !json.Valid([]byte(g.EvidenceJSON)) {
		return false, errors.New("invalid research bundle terminal grade")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_route_bundle_events(
bundle_id,observed_ts,event_type,outcome_status,payout,realized_net,capital_seconds,actual_fill,atomic_fill,
reason,evidence_json,funded,paper_authority,live_authority) VALUES(?,?,'grade',?,?,?,?,0,0,?,?,0,0,0)`,
		g.BundleID, g.Observed.UTC().Format(time.RFC3339Nano), g.OutcomeStatus, g.Payout, g.RealizedNet,
		g.CapitalSeconds, g.Reason, g.EvidenceJSON)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type ResearchRouteBundlePromotionStatus struct {
	State                   string `json:"state"`
	Truth                   string `json:"truth"`
	Bundles                 int    `json:"bundles"`
	Graded                  int    `json:"counterfactual_graded"`
	KalshiVerifiedBundles   int    `json:"kalshi_verified_bundles"`
	ComboEquivalentBundles  int    `json:"kalshi_combo_equivalent_bundles"`
	ComboPayoffMismatches   int    `json:"kalshi_combo_payoff_mismatches"`
	SealedEligible          int    `json:"sealed_eligible"`
	CurrentAtomicRFQ        int    `json:"current_atomic_rfq_quotes"`
	PaperAccepted           int    `json:"identical_paper_accepted"`
	LiveDispatched          int    `json:"live_accept_dispatched_not_filled"`
	LiveAmbiguous           int    `json:"live_accept_ambiguous"`
	RFQRequiresCreatorWrite bool   `json:"rfq_requires_creator_write"`
	PassiveQuoteDiscovery   bool   `json:"passive_quote_discovery_supported"`
	ResearchRFQWriteGate    string `json:"research_rfq_write_gate"`
}

// researchBundleKalshiComboEquivalent proves whether one typed solver portfolio can be represented
// by Kalshi's multivariate combined-market primitive. The solver owns q units of EACH ordered leg,
// so its certificate payout is additive. A Kalshi combo YES is a different security: q dollars only
// when EVERY selected side wins, otherwise zero. We compare those complete state vectors exactly;
// matching names/tickers is never enough to authorize an RFQ handoff.
func researchBundleKalshiComboEquivalent(size float64, legs []ResearchRouteBundleLeg,
	states []ResearchRouteBundleState) bool {
	if size <= 0 || len(legs) < 2 || len(legs) > 6 || len(states) == 0 {
		return false
	}
	for _, leg := range legs {
		if leg.Venue != "kalshi" || len(leg.Payoff) != len(states) {
			return false
		}
	}
	for stateIndex, state := range states {
		comboPayout := size
		for _, leg := range legs {
			p := leg.Payoff[stateIndex]
			if math.Abs(p-1) <= 1e-9 {
				continue
			}
			if math.Abs(p) > 1e-9 { // a fractional/unknown predicate cannot map to binary MVE semantics
				return false
			}
			comboPayout = 0
			break
		}
		if math.Abs(state.Payout-comboPayout) > 1e-9 {
			return false
		}
	}
	return true
}

type ResearchRouteBundleHandoff struct {
	Bundle             ResearchRouteBundle         `json:"bundle"`
	ComboEquivalent    bool                        `json:"combo_equivalent"`
	SealedUntouched    bool                        `json:"sealed_untouched_pass"`
	ProofRunID         int64                       `json:"proof_run_id,omitempty"`
	ProofResultHash    string                      `json:"proof_result_hash,omitempty"`
	PreregistrationID  string                      `json:"preregistration_id,omitempty"`
	ProofMeanAllInUnit float64                     `json:"proof_mean_all_in_unit,omitempty"`
	ProofLowerPerUnit  float64                     `json:"proof_lower_per_unit,omitempty"`
	MaxAllInUnit       float64                     `json:"max_all_in_unit,omitempty"`
	Governance         ResearchPromotionGovernance `json:"promotion_governance"`
}

func (s *Store) ResearchRouteBundleByID(ctx context.Context, id string) (ResearchRouteBundle, bool, error) {
	return researchRouteBundleByID(ctx, s.db, id)
}

func researchRouteBundleByID(ctx context.Context, q step7ProjectionQuerier,
	id string) (ResearchRouteBundle, bool, error) {
	var b ResearchRouteBundle
	var observed, evidence string
	var atomic, unwindKnown, latencyKnown int
	err := q.QueryRowContext(ctx, `SELECT bundle_id,observed_ts,system_id,cohort,experiment_version,
opportunity_id,canonical_event_id,event_version,certificate_hash,certificate_status,route_kind,
atomic_route,size_units,total_cost,total_fee,payout_floor,net_floor,partial_fill_worst,unwind_worst,
unwind_known,decision_latency_ms,latency_known,state_vector_hash,blocker,evidence_json
FROM research_route_bundles WHERE bundle_id=?`, id).Scan(&b.BundleID, &observed, &b.SystemID, &b.Cohort,
		&b.ExperimentVersion, &b.OpportunityID, &b.CanonicalEventID, &b.EventVersion, &b.CertificateHash,
		&b.CertificateStatus, &b.RouteKind, &atomic, &b.Size, &b.Cost, &b.Fee, &b.PayoutFloor,
		&b.NetFloor, &b.PartialFillWorst, &b.UnwindWorst, &unwindKnown, &b.DecisionLatencyMS,
		&latencyKnown, &b.StateVectorHash, &b.Blocker, &evidence)
	if errors.Is(err, sql.ErrNoRows) {
		return b, false, nil
	}
	if err != nil {
		return b, false, err
	}
	b.Observed, err = time.Parse(time.RFC3339Nano, observed)
	if err != nil {
		return b, false, err
	}
	b.AtomicRoute, b.UnwindKnown, b.LatencyKnown = atomic == 1, unwindKnown == 1, latencyKnown == 1
	if err := json.Unmarshal([]byte(evidence), &b.Evidence); err != nil {
		return b, false, err
	}
	legRows, err := q.QueryContext(ctx, `SELECT leg_index,leg_id,venue,ticker,side,payoff_id,quantity,
integrated_cost,exact_fee,visible_depth,tick_size,quote_age_s,book_source,source_clock_id,fee_source,
levels_json,payoff_json,unwind_known,unwind_book_source,unwind_fee_source,unwind_levels_json
FROM research_route_bundle_legs WHERE bundle_id=? ORDER BY leg_index`, id)
	if err != nil {
		return b, false, err
	}
	for legRows.Next() {
		var leg ResearchRouteBundleLeg
		var levels, payoff, unwind string
		var uk int
		if err = legRows.Scan(&leg.Index, &leg.LegID, &leg.Venue, &leg.Ticker, &leg.Side, &leg.PayoffID,
			&leg.Quantity, &leg.IntegratedCost, &leg.ExactFee, &leg.VisibleDepth, &leg.Tick, &leg.Age,
			&leg.BookSource, &leg.SourceClockID, &leg.FeeSource, &levels, &payoff, &uk,
			&leg.UnwindBookSource, &leg.UnwindFeeSource, &unwind); err != nil {
			legRows.Close()
			return b, false, err
		}
		leg.UnwindKnown = uk == 1
		if err = json.Unmarshal([]byte(levels), &leg.Levels); err == nil {
			err = json.Unmarshal([]byte(payoff), &leg.Payoff)
		}
		if err == nil {
			err = json.Unmarshal([]byte(unwind), &leg.UnwindLevels)
		}
		if err != nil {
			legRows.Close()
			return b, false, err
		}
		b.Legs = append(b.Legs, leg)
	}
	if err = legRows.Close(); err != nil {
		return b, false, err
	}
	stateRows, err := q.QueryContext(ctx, `SELECT state_index,state_id,payout FROM research_route_bundle_states
WHERE bundle_id=? ORDER BY state_index`, id)
	if err != nil {
		return b, false, err
	}
	defer stateRows.Close()
	for stateRows.Next() {
		var state ResearchRouteBundleState
		if err := stateRows.Scan(&state.Index, &state.StateID, &state.Payout); err != nil {
			return b, false, err
		}
		b.States = append(b.States, state)
	}
	if err := stateRows.Err(); err != nil {
		return b, false, err
	}
	return b, true, nil
}

// RecentPositiveResearchRouteBundles is the PAPER-research, unsealed view used by Combo Lab. A
// structural certificate and route/void blocker are allowed here because native payoff-state
// settlement is precisely how that uncertainty gathers evidence. Both remain attached to the
// row and still bar promotion/LIVE. Unknown identity never enters this view.
//
// The returned bundles keep their immutable frozen books, fees, ordered legs and full state vector;
// they remain native route-bundle evidence and must never be reinterpreted as all-win parlay rows.
func (s *Store) RecentPositiveResearchRouteBundles(ctx context.Context, since time.Time,
	limit int) ([]ResearchRouteBundle, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT bundle_id FROM research_route_bundles
WHERE observed_ts>=? AND certificate_status IN ('verified','structural') AND net_floor>0
 AND leg_count BETWEEN 2 AND 6
ORDER BY observed_ts DESC,bundle_id DESC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), limit)
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
		bundle, found, readErr := s.ResearchRouteBundleByID(ctx, id)
		if readErr != nil {
			return nil, readErr
		}
		if found {
			out = append(out, bundle)
		}
	}
	return out, nil
}

// ResearchRouteBundleHandoffStatus binds a fresh immutable candidate to the latest sealed
// preregistered untouched PASS for the exact system/version/cohort and the dedicated rfq handoff
// route. It never treats a rolling leaderboard result as proof.
func (s *Store) ResearchRouteBundleHandoffStatus(ctx context.Context, id string,
	edgeFloor float64) (ResearchRouteBundleHandoff, bool, error) {
	b, ok, err := s.ResearchRouteBundleByID(ctx, id)
	if err != nil || !ok {
		return ResearchRouteBundleHandoff{}, ok, err
	}
	out := ResearchRouteBundleHandoff{Bundle: b,
		ComboEquivalent: b.CertificateStatus == "verified" && researchBundleKalshiComboEquivalent(b.Size, b.Legs, b.States)}
	if !out.ComboEquivalent || b.Size <= 0 {
		return out, true, nil
	}
	var end, inputManifest string
	err = s.db.QueryRowContext(ctx, `SELECT r.id,r.result_hash,p.preregistration_id,
p.untouched_end_ts,r.input_manifest_json,s.untouched_lower
FROM research_inference_runs r JOIN research_inference_preregistrations p ON p.preregistration_id=r.preregistration_id
JOIN research_inference_system_results s ON s.run_id=r.id AND s.system_id=p.system_id
WHERE p.system_id=? AND p.experiment_version=? AND p.cohort=? AND p.venue='kalshi' AND p.route='rfq'
 AND r.status='sealed_preregistered_untouched' AND s.state='PREREGISTERED_UNTOUCHED_PASS'
 AND s.preregistered_untouched_gate_pass=1 AND s.execution_candidate=1
 AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
 AND s.funded=0 AND s.paper_authority=0 AND s.live_authority=0
	ORDER BY r.id DESC LIMIT 1`, b.SystemID, b.ExperimentVersion, b.Cohort).Scan(&out.ProofRunID,
		&out.ProofResultHash, &out.PreregistrationID, &end, &inputManifest, &out.ProofLowerPerUnit)
	if errors.Is(err, sql.ErrNoRows) {
		return out, true, nil
	}
	if err != nil {
		return out, false, err
	}
	untouchedEnd, parseErr := time.Parse(time.RFC3339Nano, end)
	if parseErr != nil || b.Observed.Before(untouchedEnd) {
		return out, true, nil
	}
	// Re-load the exact preregistered bundle rows and require a byte-semantic match to the sealed
	// row hash. A later grade/backfill can never change the historical mean all-in used for pricing.
	frozen, frozenErr := s.researchInferencePreregistration(ctx, out.PreregistrationID)
	if frozenErr != nil {
		return out, false, frozenErr
	}
	sealedRows, frozenErr := s.loadPreregisteredRows(ctx, frozen)
	if frozenErr != nil {
		return out, false, frozenErr
	}
	var manifest struct {
		RowsHash     string `json:"rows_hash"`
		ObservedRows int    `json:"observed_rows"`
	}
	sealedRowsHash, _ := r139Hash(sealedRows)
	if json.Unmarshal([]byte(inputManifest), &manifest) != nil || manifest.RowsHash != sealedRowsHash ||
		manifest.ObservedRows != len(sealedRows) {
		return out, true, nil
	}
	meanSum, meanN := 0.0, 0
	for _, sealedRow := range sealedRows {
		row, found, readErr := s.ResearchRouteBundleByID(ctx, sealedRow.Ticker)
		if readErr != nil {
			return out, false, readErr
		}
		if found && row.CertificateStatus == "verified" &&
			researchBundleKalshiComboEquivalent(row.Size, row.Legs, row.States) && row.Size > 0 {
			meanSum, meanN = meanSum+(row.Cost+row.Fee)/row.Size, meanN+1
		}
	}
	if meanN == len(sealedRows) && meanN > 0 {
		out.ProofMeanAllInUnit = meanSum / float64(meanN)
	}
	if !finiteBundle(out.ProofMeanAllInUnit) || out.ProofMeanAllInUnit <= 0 ||
		!finiteBundle(out.ProofLowerPerUnit) || out.ProofLowerPerUnit <= edgeFloor {
		return out, true, nil
	}
	out.MaxAllInUnit = out.ProofMeanAllInUnit + out.ProofLowerPerUnit - edgeFloor
	proof := ResearchPromotionProof{RunID: out.ProofRunID, ResultHash: out.ProofResultHash,
		PreregistrationID: out.PreregistrationID, SystemID: b.SystemID,
		ExperimentVersion: int64(b.ExperimentVersion), Cohort: b.Cohort, Venue: "kalshi", Route: "rfq"}
	governance, eligible, governanceErr := s.researchPromotionGovernance(ctx, proof, time.Now().UTC())
	if governanceErr != nil {
		return out, false, governanceErr
	}
	out.Governance = governance
	out.SealedUntouched = out.MaxAllInUnit > 0 && out.MaxAllInUnit < 1 && eligible
	return out, true, nil
}

func (s *Store) CurrentResearchRouteBundleHandoffs(ctx context.Context, since time.Time,
	edgeFloor float64, limit int) ([]ResearchRouteBundleHandoff, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := s.db.QueryContext(ctx, `SELECT b.bundle_id FROM research_route_bundles b
WHERE b.observed_ts>=? AND b.certificate_status='verified'
 AND NOT EXISTS(SELECT 1 FROM research_route_bundle_promotion_intents p WHERE p.bundle_id=b.bundle_id)
 AND NOT EXISTS(SELECT 1 FROM research_route_bundle_legs l WHERE l.bundle_id=b.bundle_id AND l.venue!='kalshi')
ORDER BY b.observed_ts DESC,b.bundle_id DESC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := []ResearchRouteBundleHandoff{}
	for _, id := range ids {
		h, found, err := s.ResearchRouteBundleHandoffStatus(ctx, id, edgeFloor)
		if err != nil {
			return nil, err
		}
		if found && h.ComboEquivalent && h.SealedUntouched {
			out = append(out, h)
		}
	}
	return out, nil
}

func (s *Store) researchBundleComboCompatibility(ctx context.Context) (verified, equivalent int, err error) {
	rows, err := s.db.QueryContext(ctx, `SELECT bundle_id,size_units,state_count FROM research_route_bundles b
WHERE certificate_status='verified' AND leg_count BETWEEN 2 AND 6
 AND NOT EXISTS(SELECT 1 FROM research_route_bundle_legs l WHERE l.bundle_id=b.bundle_id AND l.venue!='kalshi')`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var size float64
		var stateCount int
		if err := rows.Scan(&id, &size, &stateCount); err != nil {
			return 0, 0, err
		}
		verified++
		legRows, qerr := s.db.QueryContext(ctx, `SELECT leg_index,venue,payoff_json
FROM research_route_bundle_legs WHERE bundle_id=? ORDER BY leg_index`, id)
		if qerr != nil {
			return 0, 0, qerr
		}
		legs := []ResearchRouteBundleLeg{}
		for legRows.Next() {
			var leg ResearchRouteBundleLeg
			var payoff string
			if qerr = legRows.Scan(&leg.Index, &leg.Venue, &payoff); qerr == nil {
				qerr = json.Unmarshal([]byte(payoff), &leg.Payoff)
			}
			if qerr != nil {
				legRows.Close()
				return 0, 0, qerr
			}
			legs = append(legs, leg)
		}
		if qerr = legRows.Close(); qerr != nil {
			return 0, 0, qerr
		}
		stateRows, qerr := s.db.QueryContext(ctx, `SELECT state_index,state_id,payout
FROM research_route_bundle_states WHERE bundle_id=? ORDER BY state_index`, id)
		if qerr != nil {
			return 0, 0, qerr
		}
		states := []ResearchRouteBundleState{}
		for stateRows.Next() {
			var state ResearchRouteBundleState
			if qerr = stateRows.Scan(&state.Index, &state.StateID, &state.Payout); qerr != nil {
				stateRows.Close()
				return 0, 0, qerr
			}
			states = append(states, state)
		}
		if qerr = stateRows.Close(); qerr != nil {
			return 0, 0, qerr
		}
		if len(states) == stateCount && researchBundleKalshiComboEquivalent(size, legs, states) {
			equivalent++
		}
	}
	return verified, equivalent, rows.Err()
}

// ResearchBundlePromotionStatus reports the sealed conjunction-only bridge. Additive bundle grades
// remain research; a current atomic quote means the exact one-contract Paper receipt exists and the
// same quote was re-read open within the narrow handoff window.
func (s *Store) ResearchBundlePromotionStatus(ctx context.Context) (ResearchRouteBundlePromotionStatus, error) {
	var out ResearchRouteBundlePromotionStatus
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(EXISTS(SELECT 1 FROM research_route_bundle_events e WHERE e.bundle_id=b.bundle_id AND e.event_type='grade')),0),
COALESCE(SUM(atomic_route=1 AND route_kind='kalshi_rfq'),0) FROM research_route_bundles b`).Scan(
		&out.Bundles, &out.Graded, &out.CurrentAtomicRFQ)
	if err != nil {
		return out, err
	}
	out.KalshiVerifiedBundles, out.ComboEquivalentBundles, err = s.researchBundleComboCompatibility(ctx)
	if err != nil {
		return out, err
	}
	out.ComboPayoffMismatches = out.KalshiVerifiedBundles - out.ComboEquivalentBundles
	out.RFQRequiresCreatorWrite = true
	out.PassiveQuoteDiscovery = false
	out.ResearchRFQWriteGate = "exact conjunction certificate + all-system multiplicity-controlled sealed untouched PASS + positive executable and Net/day lower bounds + capacity + capital-time + positive cause-graph cluster + frozen quarter-Kelly Adaptive Allocation Model ceiling + Paper AUTO + ARM + LIVE AUTO"
	proofRows, err := s.db.QueryContext(ctx, `SELECT DISTINCT r.id,r.result_hash,p.preregistration_id,
p.system_id,p.experiment_version,p.cohort FROM research_inference_runs r
JOIN research_inference_preregistrations p ON p.preregistration_id=r.preregistration_id
JOIN research_inference_system_results x ON x.run_id=r.id AND x.system_id=p.system_id
WHERE p.venue='kalshi' AND p.route='rfq' AND r.status='sealed_preregistered_untouched'
 AND x.state='PREREGISTERED_UNTOUCHED_PASS' AND x.preregistered_untouched_gate_pass=1
 AND x.execution_candidate=1 AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
	 AND x.funded=0 AND x.paper_authority=0 AND x.live_authority=0`)
	if err != nil {
		return out, err
	}
	var proofs []ResearchPromotionProof
	for proofRows.Next() {
		var p ResearchPromotionProof
		if err = proofRows.Scan(&p.RunID, &p.ResultHash, &p.PreregistrationID, &p.SystemID,
			&p.ExperimentVersion, &p.Cohort); err != nil {
			proofRows.Close()
			return out, err
		}
		p.Venue, p.Route = "kalshi", "rfq"
		proofs = append(proofs, p)
	}
	if err = proofRows.Close(); err != nil {
		return out, err
	}
	for _, p := range proofs {
		if _, eligible, governanceErr := s.researchPromotionGovernance(ctx, p, time.Now().UTC()); governanceErr != nil {
			return out, governanceErr
		} else if eligible {
			out.SealedEligible++
		}
	}
	if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_route_bundle_promotion_intents p
WHERE EXISTS(SELECT 1 FROM research_route_bundle_promotion_events e WHERE e.intent_id=p.intent_id
 AND e.event_type='paper_accepted')
 AND EXISTS(SELECT 1 FROM research_route_bundle_promotion_events e WHERE e.intent_id=p.intent_id
 AND e.event_type='quote_revalidated' AND e.observed_ts>=?)
 AND NOT EXISTS(SELECT 1 FROM research_route_bundle_promotion_events e WHERE e.intent_id=p.intent_id
 AND e.event_type IN ('live_dispatched','live_rejected','live_ambiguous'))`,
		time.Now().UTC().Add(-3*time.Second).Format(time.RFC3339Nano)).Scan(&out.CurrentAtomicRFQ); err != nil {
		return out, err
	}
	if err = s.db.QueryRowContext(ctx, `SELECT
COALESCE(SUM(event_type='paper_accepted'),0),COALESCE(SUM(event_type='live_dispatched'),0),
COALESCE(SUM(event_type='live_ambiguous'),0) FROM research_route_bundle_promotion_events`).Scan(
		&out.PaperAccepted, &out.LiveDispatched, &out.LiveAmbiguous); err != nil {
		return out, err
	}
	if out.KalshiVerifiedBundles > 0 && out.ComboEquivalentBundles == 0 {
		out.State = "BLOCKED_KALSHI_MVE_CONJUNCTION_DIFFERS_FROM_SOLVER_PORTFOLIO"
	} else if out.SealedEligible == 0 {
		out.State = "WAITING_FOR_SEALED_CONJUNCTION_BUNDLE_PASS"
	} else if out.CurrentAtomicRFQ > 0 {
		out.State = "IDENTICAL_PAPER_ACCEPTED_SAME_RFQ_REVALIDATED"
	} else {
		out.State = "READY_FOR_PAPER_AUTO_AUTHENTICATED_RFQ"
	}
	out.Truth = "additive typed bundles remain non-atomic research. An exact certified conjunction must clear the same frozen route-economics, cause/correlation, capital-time, capacity, and deterministic Adaptive Allocation Model hierarchy as a single-instrument promotion. The returned quote is booked as the identical one-contract Paper combo, then RFQ id, quote id, market, open status, YES price, contracts and fee must all revalidate before ARM + LIVE AUTO may dispatch AcceptQuote. HTTP acceptance is dispatch/ambiguity evidence, never a fill claim; venue reconciliation remains authoritative."
	return out, nil
}

func (s *Store) ResearchRouteBundleCounts(ctx context.Context) (bundles, legs, states, grades int, err error) {
	for query, dst := range map[string]*int{
		`SELECT COUNT(*) FROM research_route_bundles`:                                &bundles,
		`SELECT COUNT(*) FROM research_route_bundle_legs`:                            &legs,
		`SELECT COUNT(*) FROM research_route_bundle_states`:                          &states,
		`SELECT COUNT(*) FROM research_route_bundle_events WHERE event_type='grade'`: &grades,
	} {
		if err = s.db.QueryRowContext(ctx, query).Scan(dst); err != nil {
			return
		}
	}
	return
}
