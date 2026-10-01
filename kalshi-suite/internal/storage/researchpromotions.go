package storage

// Fail-closed bridge from a sealed, preregistered untouched result to an actual Paper decision.
// Research evidence remains immutable and zero-authority. These separate intent/event tables only
// admit an exact matching system/version/cohort/venue/route candidate; LIVE must additionally see
// the accepted Paper event and recheck the same all-in ceiling under ARM + LIVE AUTO.

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

const researchPromotionSchema = `
CREATE TABLE IF NOT EXISTS research_promotion_intents (
 intent_id TEXT PRIMARY KEY,
 created_ts TEXT NOT NULL,
 sealed_inference_run_id INTEGER NOT NULL,
 sealed_result_hash TEXT NOT NULL,
 preregistration_id TEXT NOT NULL,
 system_id TEXT NOT NULL,
 experiment_version INTEGER NOT NULL CHECK(experiment_version>0),
 cohort TEXT NOT NULL,
 strategy_family TEXT NOT NULL DEFAULT '',
 selector_id TEXT NOT NULL DEFAULT '',
 fired_side TEXT NOT NULL DEFAULT '' CHECK(fired_side IN ('','YES','NO')),
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 route TEXT NOT NULL CHECK(route IN ('maker','taker')),
 observation_id INTEGER NOT NULL,
 canonical_event_id TEXT NOT NULL,
 event_version INTEGER NOT NULL CHECK(event_version>0),
 canonical_payoff_id TEXT NOT NULL,
 payoff_version INTEGER NOT NULL CHECK(payoff_version>0),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 paper_price REAL NOT NULL CHECK(paper_price>0 AND paper_price<1),
 paper_fee_pc REAL NOT NULL,
 current_expected_net_lower REAL NOT NULL CHECK(current_expected_net_lower>0),
 proof_mean_all_in REAL NOT NULL CHECK(proof_mean_all_in>0),
 proof_lower_pc REAL NOT NULL CHECK(proof_lower_pc>0),
 required_edge_floor REAL NOT NULL CHECK(required_edge_floor>0),
 max_all_in_unit REAL NOT NULL CHECK(max_all_in_unit>0),
 route_economics_receipt_id TEXT NOT NULL,
 cause_exposure_id TEXT NOT NULL,
 cause_exposure_version INTEGER NOT NULL CHECK(cause_exposure_version>0),
 cause_spec_hash TEXT NOT NULL CHECK(length(cause_spec_hash)=64),
 cause_graph_manifest_hash TEXT NOT NULL CHECK(length(cause_graph_manifest_hash)=64),
 net_per_day_lower REAL NOT NULL CHECK(net_per_day_lower>0),
 capacity REAL NOT NULL CHECK(capacity>=1),
 capital_dollar_hours_per_day REAL NOT NULL CHECK(capital_dollar_hours_per_day>=0),
 mode4_fraction_ceiling REAL NOT NULL CHECK(mode4_fraction_ceiling>0 AND mode4_fraction_ceiling<=0.25),
 mode4_input_hash TEXT NOT NULL CHECK(length(mode4_input_hash)=64),
 source_id TEXT NOT NULL UNIQUE,
 FOREIGN KEY(sealed_inference_run_id) REFERENCES research_inference_runs(id),
 FOREIGN KEY(preregistration_id) REFERENCES research_inference_preregistrations(preregistration_id),
 FOREIGN KEY(observation_id) REFERENCES research_system_observations(id),
 UNIQUE(sealed_inference_run_id,observation_id),
 CHECK(paper_price+paper_fee_pc<=max_all_in_unit+0.000000001)
);
CREATE INDEX IF NOT EXISTS idx_rpromotion_contract ON research_promotion_intents(system_id,experiment_version,cohort,venue,route,created_ts DESC);
CREATE TRIGGER IF NOT EXISTS research_promotion_intents_require_sealed_contract BEFORE INSERT ON research_promotion_intents
WHEN NOT EXISTS (
 SELECT 1 FROM research_inference_runs r
 JOIN research_inference_system_results s ON s.run_id=r.id AND s.system_id=NEW.system_id
 JOIN research_inference_preregistrations p ON p.preregistration_id=r.preregistration_id
 JOIN research_system_observations o ON o.id=NEW.observation_id
 WHERE r.id=NEW.sealed_inference_run_id AND r.status='sealed_preregistered_untouched'
  AND (r.result_hash=NEW.sealed_result_hash OR r.result_hash='sha256:'||NEW.sealed_result_hash)
  AND r.pipeline_version=4 AND p.pipeline_version=4
  AND s.state='PREREGISTERED_UNTOUCHED_PASS'
  AND s.preregistered_untouched_gate_pass=1 AND s.execution_candidate=1
  AND s.experiment_version=p.experiment_version
  AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
  AND s.funded=0 AND s.paper_authority=0 AND s.live_authority=0
  AND p.preregistration_id=NEW.preregistration_id AND p.system_id=NEW.system_id
  AND p.experiment_version=NEW.experiment_version AND p.cohort=NEW.cohort
  AND p.experiment_version=(SELECT MAX(x.version) FROM research_experiment_specs x
   WHERE x.experiment_id=p.system_id)
  AND p.venue=NEW.venue AND p.route=NEW.route
  AND o.system_id=NEW.system_id AND o.experiment_version=NEW.experiment_version
  AND o.cohort=NEW.cohort AND o.venue=NEW.venue AND o.route=NEW.route
  AND o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
  AND NEW.current_expected_net_lower>0
  AND o.canonical_event_id=NEW.canonical_event_id AND o.event_version=NEW.event_version
  AND o.canonical_payoff_id=NEW.canonical_payoff_id AND o.payoff_version=NEW.payoff_version
  AND o.ticker=NEW.ticker AND o.side=NEW.side AND o.certificate_status='verified'
  AND (NEW.system_id!='paired-bridge-inversion' OR NEW.cohort NOT LIKE 'native-inverse-v1|%' OR (
   NEW.strategy_family LIKE 'invert:%' AND NEW.strategy_family NOT LIKE 'invert:invert:%'
   AND NEW.selector_id<>'' AND NEW.fired_side IN ('YES','NO') AND NEW.fired_side<>NEW.side
   AND NEW.cohort='native-inverse-v1|selector='||NEW.selector_id||'|candidate|policy='||NEW.selector_id
   AND CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT)=NEW.strategy_family
   AND CAST(json_extract(o.inputs_json,'$.selector_id') AS TEXT)=NEW.selector_id
   AND UPPER(CAST(json_extract(o.inputs_json,'$.fired_side') AS TEXT))=NEW.fired_side
   AND UPPER(CAST(json_extract(o.inputs_json,'$.inverted_side') AS TEXT))=NEW.side
   AND LOWER(CAST(json_extract(o.inputs_json,'$.execution_route') AS TEXT))=NEW.route
  ))
  AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1
  AND o.depth_known=1 AND o.fee_known=1 AND o.visible_capacity>=1
  AND NEW.net_per_day_lower>0 AND NEW.capacity>=1 AND NEW.capital_dollar_hours_per_day>=0
  AND NEW.mode4_fraction_ceiling>0 AND NEW.mode4_fraction_ceiling<=0.25
  AND ABS(NEW.mode4_fraction_ceiling-
   MIN(0.25,0.25*NEW.current_expected_net_lower/(NEW.paper_price+NEW.paper_fee_pc)))<0.000000000001
  AND EXISTS (SELECT 1 FROM research_sealed_route_economics x
   WHERE x.receipt_id=NEW.route_economics_receipt_id AND x.sealed_inference_run_id=NEW.sealed_inference_run_id
    AND (x.sealed_result_hash=NEW.sealed_result_hash OR 'sha256:'||x.sealed_result_hash=NEW.sealed_result_hash)
    AND x.system_id=NEW.system_id AND x.route_id=NEW.route AND x.venue=NEW.venue
    AND x.net_per_day_lower=NEW.net_per_day_lower AND x.capacity=NEW.capacity
    AND x.capital_dollar_hours_per_day=NEW.capital_dollar_hours_per_day
    AND x.net_per_day_lower>0 AND x.capacity>=1)
  AND EXISTS (SELECT 1 FROM research_cause_exposure_specs c
   WHERE c.exposure_id=NEW.cause_exposure_id AND c.version=NEW.cause_exposure_version
    AND c.spec_hash=NEW.cause_spec_hash AND c.input_state='active'
    AND c.version=(SELECT MAX(v.version) FROM research_cause_exposure_specs v
     WHERE v.exposure_id=c.exposure_id)
    AND julianday(c.valid_until_ts)>julianday('now')
    AND julianday(c.evidence_observed_ts)<=julianday('now','+5 minutes')
    AND c.system_id=NEW.system_id AND c.route_id=NEW.route AND c.venue=NEW.venue
    AND c.evidence_hash=REPLACE(NEW.sealed_result_hash,'sha256:','')
    AND c.sealed_inference_run_id=NEW.sealed_inference_run_id
    AND c.sealed_route_economics_receipt_id=NEW.route_economics_receipt_id
    AND c.replicated_untouched=1 AND c.executable_fee_net_lower_bound=1
    AND c.net_per_day_lower=NEW.net_per_day_lower AND c.capacity=NEW.capacity
    AND c.capital_dollar_hours_per_day=NEW.capital_dollar_hours_per_day)
  AND EXISTS (SELECT 1 FROM research_cause_graph_runs g
   WHERE g.manifest_hash=NEW.cause_graph_manifest_hash AND g.state='READY'
    AND g.input_count=g.valid_input_count AND g.valid_input_count>0
    AND EXISTS(SELECT 1 FROM json_each(g.input_hashes_json) j WHERE j.value='valid:'||NEW.cause_spec_hash))
 ) BEGIN SELECT RAISE(ABORT,'promotion intent lacks sealed untouched pass or exact matching candidate'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_intents_no_update BEFORE UPDATE ON research_promotion_intents BEGIN SELECT RAISE(ABORT,'immutable research promotion intent'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_intents_no_delete BEFORE DELETE ON research_promotion_intents BEGIN SELECT RAISE(ABORT,'immutable research promotion intent'); END;

CREATE TABLE IF NOT EXISTS research_promotion_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 intent_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('paper_accepted','paper_rejected','live_dispatched','live_rejected')),
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 route TEXT NOT NULL CHECK(route IN ('maker','taker')),
 price REAL NOT NULL CHECK(price>=0 AND price<1),
 fee_pc REAL NOT NULL,
 reason TEXT NOT NULL,
 FOREIGN KEY(intent_id) REFERENCES research_promotion_intents(intent_id),
 UNIQUE(intent_id,event_type)
);
CREATE INDEX IF NOT EXISTS idx_rpromotion_events_intent ON research_promotion_events(intent_id,id);
CREATE TRIGGER IF NOT EXISTS research_promotion_events_require_identity BEFORE INSERT ON research_promotion_events
WHEN NOT EXISTS (SELECT 1 FROM research_promotion_intents p WHERE p.intent_id=NEW.intent_id
 AND p.venue=NEW.venue AND p.ticker=NEW.ticker AND p.side=NEW.side AND p.route=NEW.route)
BEGIN SELECT RAISE(ABORT,'promotion event contract differs from immutable intent'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_events_require_paper_ceiling BEFORE INSERT ON research_promotion_events
WHEN NEW.event_type='paper_accepted' AND NOT EXISTS (
 SELECT 1 FROM research_promotion_intents p WHERE p.intent_id=NEW.intent_id
  AND NEW.price>0 AND NEW.price+NEW.fee_pc<=p.max_all_in_unit+0.000000001)
BEGIN SELECT RAISE(ABORT,'accepted Paper decision exceeds sealed all-in ceiling'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_events_one_paper_outcome BEFORE INSERT ON research_promotion_events
WHEN NEW.event_type IN ('paper_accepted','paper_rejected') AND EXISTS (
 SELECT 1 FROM research_promotion_events e WHERE e.intent_id=NEW.intent_id
  AND e.event_type IN ('paper_accepted','paper_rejected'))
BEGIN SELECT RAISE(ABORT,'promotion intent already has a Paper outcome'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_events_live_requires_paper BEFORE INSERT ON research_promotion_events
WHEN NEW.event_type='live_dispatched' AND NOT EXISTS (
 SELECT 1 FROM research_promotion_events e WHERE e.intent_id=NEW.intent_id AND e.event_type='paper_accepted')
BEGIN SELECT RAISE(ABORT,'LIVE promotion dispatch lacks accepted Paper decision'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_events_live_requires_current_governance BEFORE INSERT ON research_promotion_events
WHEN NEW.event_type='live_dispatched' AND NOT EXISTS (
 SELECT 1 FROM research_promotion_intents p JOIN research_cause_exposure_specs c
  ON c.exposure_id=p.cause_exposure_id AND c.version=p.cause_exposure_version AND c.spec_hash=p.cause_spec_hash
 JOIN research_cause_graph_runs g ON g.manifest_hash=p.cause_graph_manifest_hash
 WHERE p.intent_id=NEW.intent_id AND c.input_state='active'
  AND c.version=(SELECT MAX(v.version) FROM research_cause_exposure_specs v WHERE v.exposure_id=c.exposure_id)
  AND julianday(c.valid_until_ts)>julianday('now') AND c.replicated_untouched=1
  AND c.executable_fee_net_lower_bound=1 AND g.state='READY'
  AND g.input_count=g.valid_input_count AND g.valid_input_count>0
  AND EXISTS(SELECT 1 FROM json_each(g.input_hashes_json) j WHERE j.value='valid:'||p.cause_spec_hash))
BEGIN SELECT RAISE(ABORT,'LIVE promotion dispatch lacks current frozen cause and portfolio governance'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_events_no_update BEFORE UPDATE ON research_promotion_events BEGIN SELECT RAISE(ABORT,'immutable research promotion event'); END;
CREATE TRIGGER IF NOT EXISTS research_promotion_events_no_delete BEFORE DELETE ON research_promotion_events BEGIN SELECT RAISE(ABORT,'immutable research promotion event'); END;

-- Restart-safe, fair Paper exploration claims.  An observation may be handed to the shared
-- executor once; rejection is still a completed attempt and must not be replayed after restart.
-- This table grants no LIVE authority and is deliberately separate from sealed promotion intents.
CREATE TABLE IF NOT EXISTS research_paper_exploration_claims (
 observation_id INTEGER PRIMARY KEY,
 claimed_ts TEXT NOT NULL,
 completed_ts TEXT NOT NULL DEFAULT '',
 source_id TEXT NOT NULL UNIQUE,
 system_id TEXT NOT NULL,
 strategy_family TEXT NOT NULL DEFAULT '',
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 route TEXT NOT NULL CHECK(route IN ('maker','taker')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 state TEXT NOT NULL CHECK(state IN ('claimed','accepted','rejected')),
 reason TEXT NOT NULL DEFAULT '',
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 FOREIGN KEY(observation_id) REFERENCES research_system_observations(id)
);
CREATE INDEX IF NOT EXISTS idx_rpaper_exploration_claimed ON research_paper_exploration_claims(claimed_ts,system_id,venue,route);

-- Venue submit acknowledgement is not a fill.  These immutable transition receipts keep the
-- exact sealed Paper intent attached to every later pending/partial/full/ambiguous observation.
-- A full state is admissible only from an authoritative venue order/fill/position receipt.
CREATE TABLE IF NOT EXISTS research_live_execution_receipts (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 intent_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 route TEXT NOT NULL CHECK(route IN ('maker','taker')),
 order_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','partial','full','unfilled','ambiguous','rejected')),
 requested_qty REAL NOT NULL CHECK(requested_qty>0),
 filled_qty REAL NOT NULL CHECK(filled_qty>=0),
 remaining_qty REAL NOT NULL CHECK(remaining_qty>=0),
 average_price REAL NOT NULL CHECK(average_price>=0 AND average_price<1),
 fee_total REAL NOT NULL CHECK(fee_total>=0),
 rebate_total REAL NOT NULL DEFAULT 0 CHECK(rebate_total>=0),
 fee_known INTEGER NOT NULL CHECK(fee_known IN (0,1)),
 authoritative INTEGER NOT NULL CHECK(authoritative IN (0,1)),
 receipt_source TEXT NOT NULL,
 detail TEXT NOT NULL,
 FOREIGN KEY(intent_id) REFERENCES research_promotion_intents(intent_id),
 UNIQUE(intent_id,state,order_id,filled_qty,remaining_qty,receipt_source)
);
CREATE INDEX IF NOT EXISTS idx_rpromotion_live_exec ON research_live_execution_receipts(intent_id,id);
CREATE TRIGGER IF NOT EXISTS research_live_execution_receipts_require_identity BEFORE INSERT ON research_live_execution_receipts
WHEN NOT EXISTS (SELECT 1 FROM research_promotion_intents p WHERE p.intent_id=NEW.intent_id
 AND p.venue=NEW.venue AND p.ticker=NEW.ticker AND p.side=NEW.side AND p.route=NEW.route
 AND EXISTS (SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='paper_accepted'))
BEGIN SELECT RAISE(ABORT,'LIVE execution receipt lacks identical accepted Paper intent'); END;
CREATE TRIGGER IF NOT EXISTS research_live_execution_receipts_full_authoritative BEFORE INSERT ON research_live_execution_receipts
WHEN NEW.state='full' AND (NEW.authoritative<>1 OR NEW.fee_known<>1
 OR NEW.receipt_source NOT IN ('kalshi-create-order-receipt','kalshi-portfolio-fills+order','polyus-get-order')
 OR NEW.filled_qty+0.000000001<NEW.requested_qty OR NEW.remaining_qty>0.000000001)
BEGIN SELECT RAISE(ABORT,'full LIVE receipt must be authoritative and completely filled'); END;
CREATE TRIGGER IF NOT EXISTS research_live_execution_receipts_no_update BEFORE UPDATE ON research_live_execution_receipts BEGIN SELECT RAISE(ABORT,'immutable LIVE execution receipt'); END;
CREATE TRIGGER IF NOT EXISTS research_live_execution_receipts_no_delete BEFORE DELETE ON research_live_execution_receipts BEGIN SELECT RAISE(ABORT,'immutable LIVE execution receipt'); END;
`

func migrateResearchPromotionSchema(db *sql.DB) error {
	// This bridge was introduced during R139 while local runtime testing was in progress. Make the
	// additive column and trigger replacement idempotent so an early local DB can reopen safely.
	_, _ = db.Exec(`ALTER TABLE research_promotion_intents ADD COLUMN current_expected_net_lower REAL NOT NULL DEFAULT 0 CHECK(current_expected_net_lower>=0)`)
	for _, col := range []string{
		"strategy_family TEXT NOT NULL DEFAULT ''",
		"selector_id TEXT NOT NULL DEFAULT ''",
		"fired_side TEXT NOT NULL DEFAULT ''",
		"route_economics_receipt_id TEXT NOT NULL DEFAULT ''",
		"cause_exposure_id TEXT NOT NULL DEFAULT ''",
		"cause_exposure_version INTEGER NOT NULL DEFAULT 0",
		"cause_spec_hash TEXT NOT NULL DEFAULT ''",
		"cause_graph_manifest_hash TEXT NOT NULL DEFAULT ''",
		"net_per_day_lower REAL NOT NULL DEFAULT 0",
		"capacity REAL NOT NULL DEFAULT 0",
		"capital_dollar_hours_per_day REAL NOT NULL DEFAULT 0",
		"mode4_fraction_ceiling REAL NOT NULL DEFAULT 0",
		"mode4_input_hash TEXT NOT NULL DEFAULT ''",
	} {
		_, _ = db.Exec(`ALTER TABLE research_promotion_intents ADD COLUMN ` + col)
	}
	_, _ = db.Exec(`ALTER TABLE research_live_execution_receipts ADD COLUMN fee_known INTEGER NOT NULL DEFAULT 0 CHECK(fee_known IN (0,1))`)
	_, _ = db.Exec(`ALTER TABLE research_live_execution_receipts ADD COLUMN rebate_total REAL NOT NULL DEFAULT 0 CHECK(rebate_total>=0)`)
	_, _ = db.Exec(`DROP TRIGGER IF EXISTS research_promotion_intents_require_sealed_contract`)
	_, _ = db.Exec(`DROP TRIGGER IF EXISTS research_promotion_events_live_requires_current_governance`)
	_, _ = db.Exec(`DROP TRIGGER IF EXISTS research_live_execution_receipts_full_authoritative`)
	_, err := db.Exec(researchPromotionSchema)
	return err
}

type ResearchPromotionGovernance struct {
	RouteEconomicsReceiptID, CauseExposureID, CauseSpecHash string
	CauseGraphManifestHash, Mode4InputHash                  string
	CauseExposureVersion                                    int
	NetPerDayLower, Capacity, CapitalDollarHoursPerDay      float64
	Mode4FractionCeiling                                    float64
}

type ResearchPromotionProof struct {
	RunID, ExperimentVersion                                      int64
	ObservedRows                                                  int
	ResultHash, PreregistrationID, SystemID, Cohort, Venue, Route string
	StrategyFamily, SelectorID, FiredSide                         string
	Created, UntouchedStart, UntouchedEnd                         time.Time
	MeanAllIn, MeanPC, LowerPC                                    float64
	Governance                                                    ResearchPromotionGovernance
}

type ResearchPromotionCandidate struct {
	ObservationID, ExperimentVersion                                int64
	Observed                                                        time.Time
	SystemID, Cohort, Venue, Route, Ticker, Title, Side, SelectorID string
	StrategyFamily, FiredSide                                       string
	CertificateHash                                                 string
	CanonicalEventID, CanonicalPayoffID                             string
	EventVersion, PayoffVersion                                     int
	ObservedPrice, ObservedFeePC, VisibleCapacity, ExpectedNetLower float64
}

// ResearchPaperExplorationClaim is a durable once-only handoff receipt. For current single-order
// research, Accepted means the delayed two-complete-book Paper IOC produced a realistic one-share
// fill with exact fee authority. It remains simulated evidence and never authorizes LIVE.
type ResearchPaperExplorationClaim struct {
	ObservationID              int64
	SourceID, SystemID, Family string
	Venue, Route, Ticker, Side string
	State, Reason              string
	Claimed, Completed         time.Time
}

type ResearchPromotionIntent struct {
	IntentID, SourceID                                      string
	Created                                                 time.Time
	Proof                                                   ResearchPromotionProof
	Candidate                                               ResearchPromotionCandidate
	PaperPrice, PaperFeePC, RequiredEdgeFloor, MaxAllInUnit float64
	Governance                                              ResearchPromotionGovernance
}

type ResearchPromotionBridgeStatus struct {
	State          string `json:"state"`
	StateLabel     string `json:"state_label"`
	SealedPasses   int    `json:"sealed_passes"`
	SealedEligible int    `json:"sealed_eligible"`
	Intents        int    `json:"intents"`
	PaperAccepted  int    `json:"paper_accepted"`
	PaperRejected  int    `json:"paper_rejected"`
	PaperPending   int    `json:"paper_pending"`
	LiveDispatched int    `json:"live_dispatched"`
	LiveRejected   int    `json:"live_rejected"`
	Truth          string `json:"truth"`
}

type ResearchPromotionPaperState struct {
	State, Reason, FeeSource string
	Price, FeePC             float64
}

func researchPromotionStateLabel(state string) string {
	return strings.ReplaceAll(state, "MODE4", "ADAPTIVE_ALLOCATION_MODEL")
}

type ResearchLiveExecutionReceipt struct {
	IntentID, Venue, Ticker, Side, Route, OrderID string
	State, ReceiptSource, Detail                  string
	Observed                                      time.Time
	RequestedQty, FilledQty, RemainingQty         float64
	AveragePrice, FeeTotal, RebateTotal           float64
	FeeKnown, Authoritative                       bool
}

type PendingResearchLiveExecution struct {
	Intent  ResearchPromotionIntent
	Receipt ResearchLiveExecutionReceipt
}

// ResearchLiveExecutionCohort summarizes prior exchange truth for one exact sealed promotion
// contract. Paper acceptance alone is intentionally absent: only authenticated venue receipts
// count as attempts, fills, or terminal zero-fills.
type ResearchLiveExecutionCohort struct {
	Attempts, Terminal, Filled, ZeroFilled, Incomplete int
}

func (c ResearchLiveExecutionCohort) Ready() bool {
	return c.Attempts > 0 && c.Terminal == c.Attempts && c.Incomplete == 0 &&
		c.Filled+c.ZeroFilled == c.Terminal
}

func researchLiveExecutionReceiptClass(r ResearchLiveExecutionReceipt) string {
	state := strings.ToLower(strings.TrimSpace(r.State))
	source := strings.ToLower(strings.TrimSpace(r.ReceiptSource))
	fillSource := source == "kalshi-create-order-receipt" ||
		source == "kalshi-portfolio-fills+order" || source == "polyus-get-order"
	zeroSource := fillSource || source == "kalshi-order-terminal"
	if !fillSource && !zeroSource {
		return ""
	}
	if r.Authoritative && r.FeeKnown && r.FilledQty > 0 && r.RemainingQty <= 1e-9 &&
		(state == "full" || state == "partial") && fillSource {
		return "fill"
	}
	if r.Authoritative && r.FilledQty <= 1e-9 && r.RemainingQty <= 1e-9 &&
		(state == "unfilled" || state == "rejected") && zeroSource {
		return "zero-fill"
	}
	return "incomplete"
}

// ResearchPromotionAuthoritativeLiveCohort reads the latest immutable receipt for every prior
// intent with the same system/version/cohort/selector/side/venue/route contract. The current
// intent is excluded so an order cannot bootstrap its own authority after the fact.
func (s *Store) ResearchPromotionAuthoritativeLiveCohort(ctx context.Context,
	in ResearchPromotionIntent) (ResearchLiveExecutionCohort, error) {
	var out ResearchLiveExecutionCohort
	if s == nil || s.db == nil {
		return out, errors.New("research LIVE execution store unavailable")
	}
	args := []any{
		in.IntentID, in.Proof.SystemID, in.Proof.ExperimentVersion, in.Proof.Cohort,
		in.Candidate.StrategyFamily, in.Candidate.SelectorID,
		strings.ToUpper(strings.TrimSpace(in.Candidate.FiredSide)),
		in.Proof.Venue, in.Proof.Route, strings.ToUpper(strings.TrimSpace(in.Candidate.Side)),
	}
	// Count intents, not receipts. A prior cash intent whose authenticated terminal receipt is
	// missing is an incomplete exchange attempt and must fail the cohort closed rather than vanish
	// from the denominator through the inner join below.
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_promotion_intents p
WHERE p.intent_id<>? AND p.system_id=? AND p.experiment_version=? AND p.cohort=?
 AND p.strategy_family=? AND p.selector_id=? AND UPPER(p.fired_side)=?
 AND p.venue=? AND p.route=? AND UPPER(p.side)=?`, args...).Scan(&out.Attempts); err != nil {
		return out, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.state,r.order_id,r.requested_qty,r.filled_qty,
r.remaining_qty,r.average_price,r.fee_total,r.rebate_total,r.fee_known,r.authoritative,
r.receipt_source,r.detail
FROM research_live_execution_receipts r
JOIN research_promotion_intents p ON p.intent_id=r.intent_id
WHERE r.id=(SELECT MAX(x.id) FROM research_live_execution_receipts x WHERE x.intent_id=r.intent_id)
 AND p.intent_id<>? AND p.system_id=? AND p.experiment_version=? AND p.cohort=?
 AND p.strategy_family=? AND p.selector_id=? AND UPPER(p.fired_side)=?
 AND p.venue=? AND p.route=? AND UPPER(p.side)=?`,
		args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		seen++
		var r ResearchLiveExecutionReceipt
		var feeKnown, authoritative int
		if err := rows.Scan(&r.State, &r.OrderID, &r.RequestedQty, &r.FilledQty,
			&r.RemainingQty, &r.AveragePrice, &r.FeeTotal, &r.RebateTotal,
			&feeKnown, &authoritative, &r.ReceiptSource, &r.Detail); err != nil {
			return out, err
		}
		r.FeeKnown, r.Authoritative = feeKnown == 1, authoritative == 1
		class := researchLiveExecutionReceiptClass(r)
		switch class {
		case "fill":
			out.Filled++
			out.Terminal++
		case "zero-fill":
			out.ZeroFilled++
			out.Terminal++
		default:
			out.Incomplete++
		}
	}
	if err := rows.Err(); err != nil {
		return out, err
	}
	if missing := out.Attempts - seen; missing > 0 {
		out.Incomplete += missing
	}
	return out, nil
}

// PendingResearchLiveExecutions preserves the original Kalshi-only caller contract. New venue
// reconcilers must use PendingResearchLiveExecutionsForVenue so a PolyUS order id is never sent to
// a Kalshi endpoint (or vice versa).
func (s *Store) PendingResearchLiveExecutions(ctx context.Context, limit int) ([]PendingResearchLiveExecution, error) {
	return s.PendingResearchLiveExecutionsForVenue(ctx, "kalshi", limit)
}

// PendingResearchLiveExecutionsForVenue is the restart-safe queue for accepted single-market
// submits that have not reached terminal venue truth. A partial row with zero remainder is a
// terminal partial fill and must not be polled forever.
func (s *Store) PendingResearchLiveExecutionsForVenue(ctx context.Context, venue string, limit int) ([]PendingResearchLiveExecution, error) {
	venue = strings.ToLower(strings.TrimSpace(venue))
	if venue != "kalshi" && venue != "polyus" {
		return nil, fmt.Errorf("unsupported LIVE execution venue %q", venue)
	}
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.source_id,r.intent_id,r.observed_ts,r.venue,r.ticker,
r.side,r.route,r.order_id,r.state,r.requested_qty,r.filled_qty,r.remaining_qty,r.average_price,
r.fee_total,r.rebate_total,r.fee_known,r.authoritative,r.receipt_source,r.detail
FROM research_live_execution_receipts r JOIN research_promotion_intents p ON p.intent_id=r.intent_id
WHERE r.id=(SELECT MAX(x.id) FROM research_live_execution_receipts x WHERE x.intent_id=r.intent_id)
 AND r.venue=? AND (r.state='pending' OR (r.state='partial' AND r.remaining_qty>0.000000001))
ORDER BY r.id LIMIT ?`, venue, limit)
	if err != nil {
		return nil, err
	}
	type raw struct {
		source, observed string
		r                ResearchLiveExecutionReceipt
		feeKnown, auth   int
	}
	rawRows := []raw{}
	for rows.Next() {
		var x raw
		if err := rows.Scan(&x.source, &x.r.IntentID, &x.observed, &x.r.Venue, &x.r.Ticker,
			&x.r.Side, &x.r.Route, &x.r.OrderID, &x.r.State, &x.r.RequestedQty, &x.r.FilledQty,
			&x.r.RemainingQty, &x.r.AveragePrice, &x.r.FeeTotal, &x.r.RebateTotal, &x.feeKnown, &x.auth,
			&x.r.ReceiptSource, &x.r.Detail); err != nil {
			rows.Close()
			return nil, err
		}
		x.r.Observed = parsePromotionTime(x.observed)
		x.r.FeeKnown, x.r.Authoritative = x.feeKnown == 1, x.auth == 1
		rawRows = append(rawRows, x)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]PendingResearchLiveExecution, 0, len(rawRows))
	for _, x := range rawRows {
		in, accepted, err := s.AcceptedResearchPromotionIntent(ctx, x.source)
		if err != nil {
			return nil, err
		}
		if accepted {
			out = append(out, PendingResearchLiveExecution{Intent: in, Receipt: x.r})
		}
	}
	return out, nil
}

// AppendResearchLiveExecutionReceipt records one venue-truth transition. Dispatch/pending may be
// non-authoritative, but no caller can manufacture a full fill: the DB trigger requires both an
// authoritative source and filled>=requested with zero remainder.
func (s *Store) AppendResearchLiveExecutionReceipt(ctx context.Context, in ResearchPromotionIntent,
	r ResearchLiveExecutionReceipt) error {
	if strings.TrimSpace(in.IntentID) == "" || strings.TrimSpace(r.State) == "" || r.RequestedQty <= 0 ||
		r.FilledQty < 0 || r.RemainingQty < 0 || r.AveragePrice < 0 || r.AveragePrice >= 1 || r.FeeTotal < 0 || r.RebateTotal < 0 ||
		math.IsNaN(r.RequestedQty) || math.IsNaN(r.FilledQty) || math.IsNaN(r.RemainingQty) ||
		math.IsNaN(r.AveragePrice) || math.IsNaN(r.FeeTotal) || math.IsNaN(r.RebateTotal) {
		return errors.New("invalid LIVE execution receipt")
	}
	if r.Observed.IsZero() {
		r.Observed = time.Now().UTC()
	}
	if r.IntentID == "" {
		r.IntentID = in.IntentID
	}
	if r.Venue == "" {
		r.Venue = in.Candidate.Venue
	}
	if r.Ticker == "" {
		r.Ticker = in.Candidate.Ticker
	}
	if r.Side == "" {
		r.Side = strings.ToUpper(in.Candidate.Side)
	}
	if r.Route == "" {
		r.Route = in.Proof.Route
	}
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_live_execution_receipts
(intent_id,observed_ts,venue,ticker,side,route,order_id,state,requested_qty,filled_qty,remaining_qty,
 average_price,fee_total,rebate_total,fee_known,authoritative,receipt_source,detail)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, r.IntentID, r.Observed.UTC().Format(time.RFC3339Nano),
		r.Venue, r.Ticker, strings.ToUpper(r.Side), r.Route, strings.TrimSpace(r.OrderID), r.State,
		r.RequestedQty, r.FilledQty, r.RemainingQty, r.AveragePrice, r.FeeTotal, r.RebateTotal, boolInt(r.FeeKnown), boolInt(r.Authoritative),
		strings.TrimSpace(r.ReceiptSource), strings.TrimSpace(r.Detail))
	if err != nil {
		return fmt.Errorf("append LIVE execution receipt state=%s qty=%.8f/%.8f remaining=%.8f fee=%.8f rebate=%.8f fee_known=%t authoritative=%t source=%q: %w",
			r.State, r.FilledQty, r.RequestedQty, r.RemainingQty, r.FeeTotal, r.RebateTotal, r.FeeKnown, r.Authoritative,
			r.ReceiptSource, err)
	}
	return err
}

func (s *Store) ResearchPromotionStatus(ctx context.Context) (ResearchPromotionBridgeStatus, error) {
	var out ResearchPromotionBridgeStatus
	proofs, err := s.CurrentResearchPromotionProofs(ctx)
	if err != nil {
		return out, err
	}
	out.SealedEligible = len(proofs)
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_inference_runs r
JOIN research_inference_system_results x ON x.run_id=r.id
WHERE r.status='sealed_preregistered_untouched' AND x.state='PREREGISTERED_UNTOUCHED_PASS'
 AND x.preregistered_untouched_gate_pass=1 AND x.execution_candidate=1`).Scan(&out.SealedPasses); err != nil {
		return out, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(EXISTS(SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='paper_accepted')),0),
COALESCE(SUM(EXISTS(SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='paper_rejected')),0),
COALESCE(SUM(EXISTS(SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='live_dispatched')),0),
COALESCE(SUM(EXISTS(SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='live_rejected')),0)
FROM research_promotion_intents p`).Scan(&out.Intents, &out.PaperAccepted, &out.PaperRejected,
		&out.LiveDispatched, &out.LiveRejected); err != nil {
		return out, err
	}
	out.PaperPending = out.Intents - out.PaperAccepted - out.PaperRejected
	if out.PaperPending < 0 {
		out.PaperPending = 0
	}
	switch {
	case out.SealedPasses == 0:
		out.State = "WAITING_FOR_SEALED_UNTOUCHED_PASS"
	case out.SealedEligible == 0:
		out.State = "WAITING_FOR_FROZEN_ROUTE_CAUSE_AND_MODE4_GOVERNANCE"
	case out.PaperPending > 0:
		out.State = "PAPER_ROUTE_PENDING"
	case out.PaperAccepted == 0:
		out.State = "READY_FOR_FRESH_IDENTICAL_PAPER_CANDIDATE"
	default:
		out.State = "PAPER_PROMOTIONS_ACTIVE"
	}
	// State remains a stable machine enum for API compatibility. StateLabel is the operator term.
	out.StateLabel = researchPromotionStateLabel(out.State)
	out.Truth = "row counts and rolling verdicts cannot promote; eligibility requires all-system multiplicity-controlled untouched proof, positive executable per-share and Net/day lower bounds, capacity, capital-time, a positive conservative cause cluster, and frozen Adaptive Allocation Model inputs; LIVE additionally requires the same accepted Paper contract, ARM, and LIVE AUTO"
	return out, nil
}

func parsePromotionTime(raw string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, raw)
	return t.UTC()
}

type researchPromotionProjectionManifest struct {
	CompletedCount         *int    `json:"projection_completed_count"`
	MaxCompletedTS         *string `json:"projection_max_completed_ts"`
	ManifestHash           *string `json:"projection_manifest_hash"`
	PayoffObservationCount *int    `json:"payoff_observation_count"`
	PayoffUpdateCount      *int    `json:"payoff_update_count"`
	PayoffUpdateMaxID      *int64  `json:"payoff_update_max_id"`
	PayoffManifestHash     *string `json:"payoff_update_manifest_hash"`
}

// researchPromotionProofFresh closes the gap between an immutable historical pass and permission
// to create a new intent now. A pass is current only while its pipeline, experiment contract, exact
// frozen window, and Step-7 completion manifest still match current durable truth. Existing intents
// deliberately do not call this helper: an order already submitted to a venue must remain
// recoverable and reconcilable even after its proof becomes stale.
func researchPromotionCurrentInferenceContract(ctx context.Context, q step7ProjectionQuerier,
	system string) (version int, specHash, contractHash string, err error) {
	var requiredDays int
	var inferenceJSON, holdoutJSON, stoppingRule string
	err = q.QueryRowContext(ctx, `SELECT version,spec_hash,required_days,inference_json,holdout_json,stopping_rule
FROM research_experiment_specs WHERE experiment_id=? ORDER BY version DESC LIMIT 1`, system).
		Scan(&version, &specHash, &requiredDays, &inferenceJSON, &holdoutJSON, &stoppingRule)
	if err != nil {
		return
	}
	h, hashErr := r139Hash(map[string]any{"pipeline_version": R139InferencePipelineVersion,
		"system_id": system, "experiment_version": version, "experiment_spec_hash": specHash,
		"inference_json": inferenceJSON, "holdout_json": holdoutJSON, "stopping_rule": stoppingRule})
	if hashErr != nil {
		err = hashErr
		return
	}
	contractHash = strings.TrimPrefix(h, "sha256:")
	return
}

func (s *Store) researchPromotionProofFresh(ctx context.Context,
	p ResearchPromotionProof) (bool, string, error) {
	return researchPromotionProofFreshWith(ctx, s.db, p)
}

func researchPromotionProofFreshWith(ctx context.Context, q step7ProjectionQuerier,
	p ResearchPromotionProof) (bool, string, error) {
	var runPipeline, preregPipeline int
	var experimentVersion int64
	var inputManifestJSON, experimentSpecHash, inferenceContractHash, startRaw, endRaw string
	resultHash := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(p.ResultHash)), "sha256:")
	err := q.QueryRowContext(ctx, `SELECT r.pipeline_version,r.input_manifest_json,
pr.pipeline_version,pr.experiment_version,pr.experiment_spec_hash,pr.inference_contract_hash,
pr.untouched_start_ts,pr.untouched_end_ts
FROM research_inference_runs r
JOIN research_inference_preregistrations pr ON pr.preregistration_id=r.preregistration_id
WHERE r.id=? AND r.status='sealed_preregistered_untouched'
 AND REPLACE(LOWER(r.result_hash),'sha256:','')=?
 AND pr.preregistration_id=? AND pr.system_id=? AND pr.experiment_version=?
 AND pr.cohort=? AND pr.venue=? AND pr.route=?`, p.RunID, resultHash,
		p.PreregistrationID, p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route).
		Scan(&runPipeline, &inputManifestJSON, &preregPipeline, &experimentVersion,
			&experimentSpecHash, &inferenceContractHash, &startRaw, &endRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "sealed proof identity no longer matches its immutable run", nil
	}
	if err != nil {
		return false, "", err
	}
	if runPipeline != R139InferencePipelineVersion || preregPipeline != R139InferencePipelineVersion {
		return false, fmt.Sprintf("pipeline is v%d/v%d; current is v%d",
			runPipeline, preregPipeline, R139InferencePipelineVersion), nil
	}
	currentVersion, currentSpecHash, currentContractHash, err :=
		researchPromotionCurrentInferenceContract(ctx, q, p.SystemID)
	if err != nil {
		return false, "", err
	}
	if experimentVersion != int64(currentVersion) || p.ExperimentVersion != int64(currentVersion) ||
		strings.TrimPrefix(strings.ToLower(experimentSpecHash), "sha256:") !=
			strings.TrimPrefix(strings.ToLower(currentSpecHash), "sha256:") ||
		strings.TrimPrefix(strings.ToLower(inferenceContractHash), "sha256:") !=
			strings.TrimPrefix(strings.ToLower(currentContractHash), "sha256:") {
		return false, fmt.Sprintf("experiment v%d is no longer the current v%d contract",
			experimentVersion, currentVersion), nil
	}
	start, end := parsePromotionTime(startRaw), parsePromotionTime(endRaw)
	if start.IsZero() || end.IsZero() || !p.UntouchedStart.Equal(start) || !p.UntouchedEnd.Equal(end) {
		return false, "proof window differs from its immutable preregistration", nil
	}
	var sealed researchPromotionProjectionManifest
	if err := json.Unmarshal([]byte(inputManifestJSON), &sealed); err != nil ||
		sealed.CompletedCount == nil || sealed.MaxCompletedTS == nil || sealed.ManifestHash == nil ||
		sealed.PayoffObservationCount == nil || sealed.PayoffUpdateCount == nil ||
		sealed.PayoffUpdateMaxID == nil || sealed.PayoffManifestHash == nil {
		return false, "sealed run lacks exact Step-7 and payoff-update manifests", nil
	}
	current, err := step7ProjectionCompletionManifest(ctx, q, end)
	if err != nil {
		return false, "", err
	}
	if *sealed.CompletedCount != current.Count || *sealed.MaxCompletedTS != current.MaxCompletedTS ||
		*sealed.ManifestHash != current.Hash {
		return false, "Step-7 completion manifest changed after the sealed run", nil
	}
	currentPayoffs, err := researchPayoffUpdateManifest(ctx, q, ResearchInferencePreregistration{
		PreregistrationID: p.PreregistrationID, SystemID: p.SystemID,
		ExperimentVersion: int(p.ExperimentVersion), Cohort: p.Cohort, Venue: p.Venue, Route: p.Route,
		UntouchedStart: start, UntouchedEnd: end,
	}, time.Time{})
	if err != nil {
		return false, "", err
	}
	if *sealed.PayoffObservationCount != currentPayoffs.ObservationCount ||
		*sealed.PayoffUpdateCount != currentPayoffs.UpdateCount ||
		*sealed.PayoffUpdateMaxID != currentPayoffs.MaxUpdateID ||
		*sealed.PayoffManifestHash != currentPayoffs.Hash {
		return false, "payoff-update manifest changed after the sealed run", nil
	}
	return true, "", nil
}

func validateResearchPromotionEconomics(ctx context.Context, q step7ProjectionQuerier,
	p ResearchPromotionProof, c ResearchPromotionCandidate) error {
	var sealedRows, currentRows, invalidRows int
	var sealedMean, sealedLower, meanAllIn float64
	err := q.QueryRowContext(ctx, `SELECT r.observed_rows,s.untouched_mean,s.untouched_lower,
(SELECT COUNT(*) FROM research_system_observations o
 WHERE o.system_id=p.system_id AND o.experiment_version=p.experiment_version
  AND o.cohort=p.cohort AND o.venue=p.venue AND o.route=p.route
  AND julianday(o.observed_ts)>=julianday(p.untouched_start_ts)
  AND julianday(o.observed_ts)<julianday(p.untouched_end_ts)),
COALESCE((SELECT SUM(CASE WHEN o.observation_kind!='candidate' OR o.candidate!=1 OR o.size_units!=1
 THEN 1 ELSE 0 END) FROM research_system_observations o
 WHERE o.system_id=p.system_id AND o.experiment_version=p.experiment_version
  AND o.cohort=p.cohort AND o.venue=p.venue AND o.route=p.route
  AND julianday(o.observed_ts)>=julianday(p.untouched_start_ts)
  AND julianday(o.observed_ts)<julianday(p.untouched_end_ts)),0),
COALESCE((SELECT AVG(o.executable_cost+o.exact_fee) FROM research_system_observations o
 WHERE o.system_id=p.system_id AND o.experiment_version=p.experiment_version
  AND o.cohort=p.cohort AND o.venue=p.venue AND o.route=p.route
  AND o.observation_kind='candidate' AND o.candidate=1 AND o.size_units=1
  AND julianday(o.observed_ts)>=julianday(p.untouched_start_ts)
  AND julianday(o.observed_ts)<julianday(p.untouched_end_ts)),-1)
FROM research_inference_runs r
JOIN research_inference_preregistrations p ON p.preregistration_id=r.preregistration_id
JOIN research_inference_system_results s ON s.run_id=r.id AND s.system_id=p.system_id
WHERE r.id=? AND REPLACE(LOWER(r.result_hash),'sha256:','')=?
 AND p.preregistration_id=? AND p.system_id=? AND p.experiment_version=?
 AND p.cohort=? AND p.venue=? AND p.route=?
 AND r.status='sealed_preregistered_untouched'
 AND s.state='PREREGISTERED_UNTOUCHED_PASS'
 AND s.preregistered_untouched_gate_pass=1 AND s.execution_candidate=1`,
		p.RunID, strings.TrimPrefix(strings.ToLower(strings.TrimSpace(p.ResultHash)), "sha256:"),
		p.PreregistrationID, p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route).
		Scan(&sealedRows, &sealedMean, &sealedLower, &currentRows, &invalidRows, &meanAllIn)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("sealed promotion proof economics are unavailable")
	}
	if err != nil {
		return err
	}
	if sealedRows <= 0 || sealedRows != currentRows || invalidRows != 0 ||
		p.ObservedRows != sealedRows || p.MeanPC != sealedMean ||
		p.LowerPC != sealedLower || p.MeanAllIn != meanAllIn {
		return errors.New("caller promotion proof economics differ from authoritative sealed evidence")
	}

	var certificateHash string
	var observedPrice, observedFee, visibleCapacity float64
	err = q.QueryRowContext(ctx, `SELECT o.certificate_hash,o.executable_cost,o.exact_fee,o.visible_capacity
FROM research_system_observations o WHERE o.id=?
 AND o.system_id=? AND o.experiment_version=? AND o.cohort=? AND o.venue=? AND o.route=?
 AND o.ticker=? AND UPPER(o.side)=? AND o.canonical_event_id=? AND o.event_version=?
 AND o.canonical_payoff_id=? AND o.payoff_version=?
 AND o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
 AND o.certificate_status='verified' AND o.size_units=1
 AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1
 AND o.depth_known=1 AND o.fee_known=1 AND o.visible_capacity>=1`,
		c.ObservationID, p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route,
		c.Ticker, strings.ToUpper(c.Side), c.CanonicalEventID, c.EventVersion,
		c.CanonicalPayoffID, c.PayoffVersion).
		Scan(&certificateHash, &observedPrice, &observedFee, &visibleCapacity)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("authoritative promotion candidate is unavailable")
	}
	if err != nil {
		return err
	}
	if c.SystemID != p.SystemID || c.ExperimentVersion != p.ExperimentVersion ||
		c.Cohort != p.Cohort || c.Venue != p.Venue || c.Route != p.Route ||
		c.CertificateHash != certificateHash || c.ObservedPrice != observedPrice ||
		c.ObservedFeePC != observedFee || c.VisibleCapacity != visibleCapacity {
		return errors.New("caller promotion candidate economics differ from authoritative observation")
	}
	return nil
}

func validateResearchPromotionGovernanceCurrent(ctx context.Context, q step7ProjectionQuerier,
	g ResearchPromotionGovernance, now time.Time) error {
	var observedRaw, validUntilRaw string
	err := q.QueryRowContext(ctx, `SELECT c.evidence_observed_ts,c.valid_until_ts
FROM research_cause_exposure_specs c
WHERE c.exposure_id=? AND c.version=? AND c.spec_hash=? AND c.input_state='active'
 AND c.version=(SELECT MAX(v.version) FROM research_cause_exposure_specs v
  WHERE v.exposure_id=c.exposure_id)`,
		g.CauseExposureID, g.CauseExposureVersion, g.CauseSpecHash).
		Scan(&observedRaw, &validUntilRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("promotion intent lacks current frozen cause governance")
	}
	if err != nil {
		return err
	}
	observed, validUntil := parsePromotionTime(observedRaw), parsePromotionTime(validUntilRaw)
	if observed.IsZero() || observed.After(now.Add(5*time.Minute)) || !validUntil.After(now) {
		return errors.New("promotion intent frozen cause governance is expired or future-dated")
	}
	return nil
}

// researchPromotionGovernance resolves the complete frozen compounding hierarchy. No value is
// inferred from collector row counts or a rolling leaderboard: route economics, cause membership,
// portfolio eligibility, and freshness must already exist as immutable receipts.
func (s *Store) researchPromotionGovernance(ctx context.Context, p ResearchPromotionProof,
	now time.Time) (ResearchPromotionGovernance, bool, error) {
	var g ResearchPromotionGovernance
	var observed, validUntil, inputHashes, graphResult string
	err := s.db.QueryRowContext(ctx, `SELECT x.receipt_id,c.exposure_id,c.version,c.spec_hash,
cg.manifest_hash,x.net_per_day_lower,x.capacity,x.capital_dollar_hours_per_day,
c.evidence_observed_ts,c.valid_until_ts,cg.input_hashes_json,cg.result_json
FROM research_sealed_route_economics x JOIN research_cause_exposure_specs c
 ON c.sealed_route_economics_receipt_id=x.receipt_id
JOIN research_cause_graph_runs cg ON cg.state='READY' AND cg.input_count=cg.valid_input_count
 AND cg.valid_input_count>0
WHERE x.sealed_inference_run_id=? AND REPLACE(x.sealed_result_hash,'sha256:','')=?
 AND x.system_id=? AND x.route_id=? AND x.venue=? AND x.net_per_day_lower>0 AND x.capacity>=1
 AND c.version=(SELECT MAX(v.version) FROM research_cause_exposure_specs v WHERE v.exposure_id=c.exposure_id)
 AND c.input_state='active' AND c.system_id=x.system_id AND c.route_id=x.route_id AND c.venue=x.venue
 AND c.evidence_hash=? AND c.sealed_inference_run_id=x.sealed_inference_run_id
 AND c.replicated_untouched=1 AND c.executable_fee_net_lower_bound=1
 AND c.net_per_day_lower=x.net_per_day_lower AND c.capacity=x.capacity
 AND c.capital_dollar_hours_per_day=x.capital_dollar_hours_per_day
 AND EXISTS(SELECT 1 FROM json_each(cg.input_hashes_json) j WHERE j.value='valid:'||c.spec_hash)
ORDER BY c.version DESC,cg.observed_ts DESC LIMIT 1`, p.RunID,
		strings.TrimPrefix(p.ResultHash, "sha256:"), p.SystemID, p.Route, p.Venue,
		strings.TrimPrefix(p.ResultHash, "sha256:")).Scan(&g.RouteEconomicsReceiptID,
		&g.CauseExposureID, &g.CauseExposureVersion, &g.CauseSpecHash, &g.CauseGraphManifestHash,
		&g.NetPerDayLower, &g.Capacity, &g.CapitalDollarHoursPerDay, &observed, &validUntil,
		&inputHashes, &graphResult)
	if errors.Is(err, sql.ErrNoRows) {
		return g, false, nil
	}
	if err != nil {
		return g, false, err
	}
	evidenceAt, expires := parsePromotionTime(observed), parsePromotionTime(validUntil)
	if evidenceAt.IsZero() || evidenceAt.After(now.Add(5*time.Minute)) || !expires.After(now) ||
		!finiteResearchNumber(g.NetPerDayLower) || g.NetPerDayLower <= 0 ||
		!finiteResearchNumber(g.Capacity) || g.Capacity < 1 ||
		!finiteResearchNumber(g.CapitalDollarHoursPerDay) || g.CapitalDollarHoursPerDay < 0 ||
		!strings.Contains(inputHashes, `"valid:`+g.CauseSpecHash+`"`) {
		return ResearchPromotionGovernance{}, false, nil
	}
	var clusters []struct {
		Systems                          []string `json:"Systems"`
		ConservativeSharedCauseNetPerDay float64  `json:"ConservativeSharedCauseNetPerDay"`
	}
	if json.Unmarshal([]byte(graphResult), &clusters) != nil {
		return ResearchPromotionGovernance{}, false, nil
	}
	diversificationEligible := false
	clusterSystems := map[string]bool{}
	for _, cluster := range clusters {
		containsCandidate := false
		for _, system := range cluster.Systems {
			if system == p.SystemID {
				containsCandidate = true
			}
		}
		if containsCandidate && cluster.ConservativeSharedCauseNetPerDay > 0 {
			diversificationEligible = true
			for _, system := range cluster.Systems {
				clusterSystems[system] = true
			}
		}
	}
	if !diversificationEligible {
		return ResearchPromotionGovernance{}, false, nil
	}
	// A positive shared-cause cluster can fund only its best frozen route. This prevents multiple
	// correlated positives from each claiming the same alpha. The deterministic hierarchy is
	// conservative Net/day, capacity, lower capital time, then stable identity; the Adaptive Allocation Model follows.
	exposures, exposureErr := s.CurrentFrozenCauseExposures(ctx)
	if exposureErr != nil {
		return ResearchPromotionGovernance{}, false, exposureErr
	}
	var best *FrozenCauseExposure
	for i := range exposures {
		x := &exposures[i]
		if !clusterSystems[x.SystemID] || x.InputState != "active" || !x.ValidUntil.After(now) ||
			x.NetPerDayLower <= 0 || x.Capacity < 1 || !strings.Contains(inputHashes, `"valid:`+x.SpecHash+`"`) {
			continue
		}
		if best == nil || x.NetPerDayLower > best.NetPerDayLower ||
			(x.NetPerDayLower == best.NetPerDayLower && x.Capacity > best.Capacity) ||
			(x.NetPerDayLower == best.NetPerDayLower && x.Capacity == best.Capacity &&
				x.CapitalDollarHoursPerDay < best.CapitalDollarHoursPerDay) ||
			(x.NetPerDayLower == best.NetPerDayLower && x.Capacity == best.Capacity &&
				x.CapitalDollarHoursPerDay == best.CapitalDollarHoursPerDay &&
				x.SystemID+"|"+x.RouteID < best.SystemID+"|"+best.RouteID) {
			best = x
		}
	}
	if best == nil || best.SystemID != p.SystemID || best.RouteID != p.Route {
		return ResearchPromotionGovernance{}, false, nil
	}
	return g, true, nil
}

func (s *Store) CurrentResearchPromotionProofs(ctx context.Context) ([]ResearchPromotionProof, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.id,r.result_hash,r.created_ts,r.observed_rows,p.preregistration_id,
p.system_id,p.experiment_version,p.cohort,p.venue,p.route,p.untouched_start_ts,p.untouched_end_ts,
s.untouched_mean,s.untouched_lower
FROM research_inference_runs r JOIN research_inference_preregistrations p ON p.preregistration_id=r.preregistration_id
JOIN research_inference_system_results s ON s.run_id=r.id AND s.system_id=p.system_id
WHERE r.status='sealed_preregistered_untouched' AND s.state='PREREGISTERED_UNTOUCHED_PASS'
 AND s.preregistered_untouched_gate_pass=1 AND s.execution_candidate=1
 AND r.pipeline_version=? AND p.pipeline_version=?
 AND s.experiment_version=p.experiment_version
 AND p.experiment_version=(SELECT MAX(x.version) FROM research_experiment_specs x
  WHERE x.experiment_id=p.system_id)
 AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
 AND s.funded=0 AND s.paper_authority=0 AND s.live_authority=0
ORDER BY r.id DESC`, R139InferencePipelineVersion, R139InferencePipelineVersion)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen, out := map[string]bool{}, []ResearchPromotionProof{}
	for rows.Next() {
		var p ResearchPromotionProof
		var created, start, end string
		if err := rows.Scan(&p.RunID, &p.ResultHash, &created, &p.ObservedRows, &p.PreregistrationID, &p.SystemID,
			&p.ExperimentVersion, &p.Cohort, &p.Venue, &p.Route, &start, &end, &p.MeanPC, &p.LowerPC); err != nil {
			return nil, err
		}
		key := p.SystemID + "|" + fmt.Sprint(p.ExperimentVersion) + "|" + p.Cohort + "|" + p.Venue + "|" + p.Route
		if seen[key] ||
			(p.Venue != "kalshi" && p.Venue != "polyus") || (p.Route != "maker" && p.Route != "taker") || p.LowerPC <= 0 {
			continue
		}
		p.Created, p.UntouchedStart, p.UntouchedEnd = parsePromotionTime(created), parsePromotionTime(start), parsePromotionTime(end)
		if fresh, _, err := s.researchPromotionProofFresh(ctx, p); err != nil {
			return nil, err
		} else if !fresh {
			continue
		}
		// The sealed run's completion manifest covers all raw decisions before UntouchedEnd, not
		// only the untouched slice. Any older pending projection can still change that manifest.
		if pending, err := s.Step7ProjectionPendingInWindow(ctx, time.Time{}, p.UntouchedEnd); err != nil {
			return nil, err
		} else if pending > 0 {
			continue // the sealed all-history manifest is incomplete until every raw projection commits
		}
		var rowsN, invalidN int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN observation_kind!='candidate' OR candidate!=1 OR size_units!=1 THEN 1 ELSE 0 END),0)
FROM research_system_observations WHERE system_id=? AND experiment_version=? AND cohort=? AND venue=? AND route=?
	 AND julianday(observed_ts)>=julianday(?) AND julianday(observed_ts)<julianday(?)`, p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route,
			p.UntouchedStart.Format(time.RFC3339Nano), p.UntouchedEnd.Format(time.RFC3339Nano)).Scan(&rowsN, &invalidN); err != nil {
			return nil, err
		}
		if rowsN == 0 || rowsN != p.ObservedRows || invalidN != 0 {
			continue // a mixed abstention/control cohort can inform research but cannot authorize a Paper action
		}
		if err := s.db.QueryRowContext(ctx, `SELECT AVG(executable_cost+exact_fee)
FROM research_system_observations WHERE system_id=? AND experiment_version=? AND cohort=? AND venue=? AND route=?
	 AND observation_kind='candidate' AND candidate=1 AND size_units=1
	 AND julianday(observed_ts)>=julianday(?) AND julianday(observed_ts)<julianday(?)`,
			p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route,
			p.UntouchedStart.Format(time.RFC3339Nano), p.UntouchedEnd.Format(time.RFC3339Nano)).Scan(&p.MeanAllIn); err != nil {
			return nil, err
		}
		if !finiteResearchNumber(p.MeanAllIn) || p.MeanAllIn <= 0 {
			continue
		}
		if p.SystemID == "paired-bridge-inversion" && strings.HasPrefix(p.Cohort, "native-inverse-v1|") {
			var variants int
			err := s.db.QueryRowContext(ctx, `SELECT
COALESCE(MIN(CAST(json_extract(inputs_json,'$.strategy_family') AS TEXT)),''),
COALESCE(MIN(CAST(json_extract(inputs_json,'$.selector_id') AS TEXT)),''),
COALESCE(MIN(UPPER(CAST(json_extract(inputs_json,'$.fired_side') AS TEXT))),''),
COUNT(DISTINCT COALESCE(CAST(json_extract(inputs_json,'$.strategy_family') AS TEXT),'')||'|'||
 COALESCE(CAST(json_extract(inputs_json,'$.selector_id') AS TEXT),'')||'|'||
 COALESCE(UPPER(CAST(json_extract(inputs_json,'$.fired_side') AS TEXT)),'')||'|'||UPPER(side))
FROM research_system_observations WHERE system_id=? AND experiment_version=? AND cohort=?
 AND venue=? AND route=? AND observation_kind='candidate' AND candidate=1
 AND julianday(observed_ts)>=julianday(?) AND julianday(observed_ts)<julianday(?)`,
				p.SystemID, p.ExperimentVersion, p.Cohort, p.Venue, p.Route,
				p.UntouchedStart.Format(time.RFC3339Nano), p.UntouchedEnd.Format(time.RFC3339Nano)).
				Scan(&p.StrategyFamily, &p.SelectorID, &p.FiredSide, &variants)
			if err != nil || variants != 1 || !nativeInversePromotionCandidateValid(p,
				ResearchPromotionCandidate{SystemID: p.SystemID, Cohort: p.Cohort, Venue: p.Venue,
					Route: p.Route, Side: map[string]string{"YES": "NO", "NO": "YES"}[p.FiredSide],
					StrategyFamily: p.StrategyFamily, SelectorID: p.SelectorID, FiredSide: p.FiredSide}) {
				continue
			}
		}
		p.ResultHash = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(p.ResultHash)), "sha256:")
		governance, eligible, governanceErr := s.researchPromotionGovernance(ctx, p, time.Now().UTC())
		if governanceErr != nil {
			return nil, governanceErr
		}
		if !eligible {
			continue
		}
		p.Governance = governance
		seen[key] = true
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) LatestResearchPromotionCandidate(ctx context.Context, p ResearchPromotionProof,
	since time.Time) (ResearchPromotionCandidate, bool, error) {
	var c ResearchPromotionCandidate
	var observed string
	err := s.db.QueryRowContext(ctx, `SELECT o.id,o.observed_ts,o.system_id,o.experiment_version,o.cohort,
o.venue,o.route,o.ticker,COALESCE(e.title,''),o.side,o.canonical_event_id,o.event_version,
o.canonical_payoff_id,o.payoff_version,o.certificate_hash,o.executable_cost,o.exact_fee,o.visible_capacity,o.net_lower,
COALESCE(CAST(json_extract(o.inputs_json,'$.selector_id') AS TEXT),''),
COALESCE(CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT),''),
UPPER(COALESCE(CAST(json_extract(o.inputs_json,'$.fired_side') AS TEXT),''))
FROM research_system_observations o INDEXED BY idx_rsystem_obs_system JOIN research_event_specs e
 ON e.event_id=o.canonical_event_id AND e.version=o.event_version
WHERE o.system_id=? AND o.experiment_version=? AND o.cohort=? AND o.venue=? AND o.route=?
 AND o.observation_kind='candidate' AND o.candidate=1 AND o.blocker='' AND o.certificate_status='verified'
	AND UPPER(COALESCE(json_extract(o.inputs_json,'$.promotion_action'),'BUY'))='BUY'
 AND o.size_units=1 AND o.visible_capacity>=1
 AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1
	 AND o.observed_ts>=? ORDER BY o.observed_ts DESC,o.id DESC LIMIT 1`, p.SystemID, p.ExperimentVersion,
		p.Cohort, p.Venue, p.Route, since.UTC().Format(time.RFC3339Nano)).Scan(&c.ObservationID, &observed,
		&c.SystemID, &c.ExperimentVersion, &c.Cohort, &c.Venue, &c.Route, &c.Ticker, &c.Title, &c.Side,
		&c.CanonicalEventID, &c.EventVersion, &c.CanonicalPayoffID, &c.PayoffVersion,
		&c.CertificateHash, &c.ObservedPrice, &c.ObservedFeePC, &c.VisibleCapacity, &c.ExpectedNetLower, &c.SelectorID,
		&c.StrategyFamily, &c.FiredSide)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	c.Observed = parsePromotionTime(observed)
	if c.Title == "" {
		c.Title = c.Ticker
	}
	return c, true, nil
}

// RecentResearchPaperCandidates returns fresh, exact-route research candidates without treating
// them as proof. This is the Paper exploration lane: the delayed two-complete-book executor still
// refreshes book, depth, fee, timing and route, while LIVE continues to require the separate sealed
// untouched ResearchPromotionProof contract. Observer/control rows are deliberately excluded.
// observed_ts is written as UTC RFC3339Nano by InsertResearchSystemObservation, so byte ordering
// is chronological. A plain range keeps this 45-second lookup on idx_rsystem_obs_observed;
// julianday(observed_ts) previously forced a full immutable-observation scan every five seconds.
func (s *Store) RecentResearchPaperCandidates(ctx context.Context, since time.Time,
	limit int) ([]ResearchPromotionCandidate, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	// Take at most two rows from each exact system/venue/side/route lane before the global limit.
	// Without this partition, one high-rate collector can occupy the newest page forever and starve
	// every quieter system. Durable claims are excluded in SQL, so a restart cannot replay them.
	rows, err := s.db.QueryContext(ctx, `WITH eligible AS (
 SELECT o.id,o.observed_ts,o.system_id,o.experiment_version,o.cohort,
  o.venue,o.route,o.ticker,COALESCE(e.title,'') AS title,o.side,o.canonical_event_id,o.event_version,
  o.canonical_payoff_id,o.payoff_version,o.certificate_hash,o.executable_cost,o.exact_fee,o.visible_capacity,o.net_lower,
  COALESCE(CAST(json_extract(o.inputs_json,'$.selector_id') AS TEXT),'') AS selector_id,
  COALESCE(CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT),'') AS strategy_family,
  UPPER(COALESCE(CAST(json_extract(o.inputs_json,'$.fired_side') AS TEXT),'')) AS fired_side,
  ROW_NUMBER() OVER (PARTITION BY
   COALESCE(NULLIF(LOWER(CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT)),''),o.system_id),
   o.venue,o.side,o.route ORDER BY o.observed_ts DESC,o.id DESC) AS lane_rank
 FROM research_system_observations o INDEXED BY idx_rsystem_obs_observed JOIN research_event_specs e
  ON e.event_id=o.canonical_event_id AND e.version=o.event_version
 WHERE o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
  AND o.certificate_status='verified' AND o.route IN ('maker','taker')
  AND o.venue IN ('kalshi','polyus')
  AND UPPER(COALESCE(json_extract(o.inputs_json,'$.promotion_action'),'BUY'))='BUY'
  AND o.size_units=1 AND o.visible_capacity>=1
  AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1
  AND o.depth_known=1 AND o.fee_known=1 AND o.observed_ts>=?
  AND NOT EXISTS(SELECT 1 FROM research_paper_exploration_claims c WHERE c.observation_id=o.id)
)
SELECT id,observed_ts,system_id,experiment_version,cohort,venue,route,ticker,title,side,
 canonical_event_id,event_version,canonical_payoff_id,payoff_version,certificate_hash,
 executable_cost,exact_fee,visible_capacity,net_lower,selector_id,strategy_family,fired_side
FROM eligible WHERE lane_rank<=2
ORDER BY observed_ts DESC,id DESC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ResearchPromotionCandidate, 0, limit)
	for rows.Next() {
		var c ResearchPromotionCandidate
		var observed string
		if err := rows.Scan(&c.ObservationID, &observed, &c.SystemID, &c.ExperimentVersion,
			&c.Cohort, &c.Venue, &c.Route, &c.Ticker, &c.Title, &c.Side,
			&c.CanonicalEventID, &c.EventVersion, &c.CanonicalPayoffID, &c.PayoffVersion,
			&c.CertificateHash, &c.ObservedPrice, &c.ObservedFeePC, &c.VisibleCapacity, &c.ExpectedNetLower, &c.SelectorID,
			&c.StrategyFamily, &c.FiredSide); err != nil {
			return nil, err
		}
		c.Observed = parsePromotionTime(observed)
		if c.Title == "" {
			c.Title = c.Ticker
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ClaimResearchPaperCandidate atomically reserves one immutable observation before any order
// attempt. The claim is durable across restarts and does not expire: a new attempt requires a new
// observation, preventing repeated Paper orders from the same evidence row.
func (s *Store) ClaimResearchPaperCandidate(ctx context.Context, c ResearchPromotionCandidate,
	sourceID string) (bool, error) {
	sourceID = strings.TrimSpace(sourceID)
	c.SystemID = strings.TrimSpace(c.SystemID)
	c.StrategyFamily = strings.ToLower(strings.TrimSpace(c.StrategyFamily))
	c.Venue = strings.ToLower(strings.TrimSpace(c.Venue))
	c.Route = strings.ToLower(strings.TrimSpace(c.Route))
	c.Ticker = strings.TrimSpace(c.Ticker)
	c.Side = strings.ToUpper(strings.TrimSpace(c.Side))
	if c.ObservationID <= 0 || sourceID == "" || c.SystemID == "" || c.Ticker == "" ||
		(c.Venue != "kalshi" && c.Venue != "polyus") ||
		(c.Route != "maker" && c.Route != "taker") || (c.Side != "YES" && c.Side != "NO") {
		return false, errors.New("invalid Paper exploration claim identity")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_paper_exploration_claims(
observation_id,claimed_ts,source_id,system_id,strategy_family,venue,route,ticker,side,state)
SELECT ?,?,?,?,?,?,?,?,?, 'claimed' WHERE EXISTS(
 SELECT 1 FROM research_system_observations o WHERE o.id=? AND o.system_id=? AND o.venue=?
  AND o.route=? AND o.ticker=? AND UPPER(o.side)=? AND o.observation_kind='candidate'
  AND o.candidate=1 AND o.blocker='' AND o.certificate_status='verified'
)`, c.ObservationID, time.Now().UTC().Format(time.RFC3339Nano), sourceID, c.SystemID,
		c.StrategyFamily, c.Venue, c.Route, c.Ticker, c.Side,
		c.ObservationID, c.SystemID, c.Venue, c.Route, c.Ticker, c.Side)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// CompleteResearchPaperCandidate records the shared executor's decision. It cannot change an
// accepted/rejected claim later and therefore cannot be used to replay an observation.
func (s *Store) CompleteResearchPaperCandidate(ctx context.Context, observationID int64,
	accepted bool, reason string) error {
	state := "rejected"
	if accepted {
		state = "accepted"
	}
	reason = strings.TrimSpace(reason)
	if observationID <= 0 || reason == "" {
		return errors.New("invalid Paper exploration completion")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE research_paper_exploration_claims
SET completed_ts=?,state=?,reason=? WHERE observation_id=? AND state='claimed'`,
		time.Now().UTC().Format(time.RFC3339Nano), state, reason, observationID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("Paper exploration completion changed %d claims for observation %d", n, observationID)
	}
	return nil
}

// CompleteStaleResearchPaperCandidates closes the only crash gap in the asynchronous delayed
// Paper handoff. A claim older than the executor's bounded signal lifetime cannot still produce a
// valid fill after restart, so it becomes an explicit not-observed rejection instead of remaining
// a silent permanent claim. Accepted/rejected history is immutable and never touched.
func (s *Store) CompleteStaleResearchPaperCandidates(ctx context.Context,
	before time.Time) (int64, error) {
	if before.IsZero() {
		return 0, errors.New("stale Paper claim cutoff is missing")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `UPDATE research_paper_exploration_claims
SET completed_ts=?,state='rejected',reason='delayed-paper PAPER-NOT-OBSERVED: process-ended-before-terminal'
WHERE state='claimed' AND claimed_ts<?`, now, before.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// ResearchPaperCandidateByID re-reads the exact immutable action carried by an r142x source.
// It deliberately applies the same executable-candidate contract as the rolling Paper scan; an
// observation id alone can never turn a control, stale schema row, or incomplete route into an
// order input. Callers still refresh the venue book, fee, depth, horizon, sizing and risk before
// placing anything.
func (s *Store) ResearchPaperCandidateByID(ctx context.Context, observationID int64) (ResearchPromotionCandidate, bool, error) {
	var c ResearchPromotionCandidate
	if observationID <= 0 {
		return c, false, nil
	}
	var observed string
	err := s.db.QueryRowContext(ctx, `SELECT o.id,o.observed_ts,o.system_id,o.experiment_version,o.cohort,
o.venue,o.route,o.ticker,COALESCE(e.title,''),o.side,o.canonical_event_id,o.event_version,
o.canonical_payoff_id,o.payoff_version,o.certificate_hash,o.executable_cost,o.exact_fee,o.visible_capacity,o.net_lower,
COALESCE(CAST(json_extract(o.inputs_json,'$.selector_id') AS TEXT),''),
COALESCE(CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT),''),
UPPER(COALESCE(CAST(json_extract(o.inputs_json,'$.fired_side') AS TEXT),''))
FROM research_system_observations o JOIN research_event_specs e
 ON e.event_id=o.canonical_event_id AND e.version=o.event_version
WHERE o.id=? AND o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
 AND o.certificate_status='verified' AND o.route IN ('maker','taker')
 AND o.venue IN ('kalshi','polyus')
 AND UPPER(COALESCE(json_extract(o.inputs_json,'$.promotion_action'),'BUY'))='BUY'
 AND o.size_units=1 AND o.visible_capacity>=1
 AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1
 AND o.depth_known=1 AND o.fee_known=1`, observationID).Scan(&c.ObservationID, &observed,
		&c.SystemID, &c.ExperimentVersion, &c.Cohort, &c.Venue, &c.Route, &c.Ticker, &c.Title, &c.Side,
		&c.CanonicalEventID, &c.EventVersion, &c.CanonicalPayoffID, &c.PayoffVersion,
		&c.CertificateHash, &c.ObservedPrice, &c.ObservedFeePC, &c.VisibleCapacity, &c.ExpectedNetLower,
		&c.SelectorID, &c.StrategyFamily, &c.FiredSide)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	c.Observed = parsePromotionTime(observed)
	if c.Title == "" {
		c.Title = c.Ticker
	}
	return c, true, nil
}

// RecentPositiveResearchComboCandidates is the unsealed PAPER-only Combo Lab source. It returns
// current one-share BUY/taker routes whose own prospective fee-net lower bound is positive. These
// rows are exact-route inputs, not promotion proof; callers must refresh the current book, fee,
// depth, lifecycle and horizon again before admitting a combo leg.
func (s *Store) RecentPositiveResearchComboCandidates(ctx context.Context, since time.Time,
	limit int) ([]ResearchPromotionCandidate, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT o.id,o.observed_ts,o.system_id,o.experiment_version,o.cohort,
o.venue,o.route,o.ticker,COALESCE(e.title,''),o.side,o.canonical_event_id,o.event_version,
o.canonical_payoff_id,o.payoff_version,o.executable_cost,o.exact_fee,o.visible_capacity,
COALESCE(CAST(json_extract(o.inputs_json,'$.selector_id') AS TEXT),''),
COALESCE(CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT),''),
UPPER(COALESCE(CAST(json_extract(o.inputs_json,'$.fired_side') AS TEXT),'')),o.net_lower
FROM research_system_observations o INDEXED BY idx_rsystem_obs_observed JOIN research_event_specs e
 ON e.event_id=o.canonical_event_id AND e.version=o.event_version
WHERE o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
 AND o.certificate_status='verified' AND o.route='taker'
 AND o.venue IN ('kalshi','polyus') AND o.net_lower>0
 AND UPPER(COALESCE(json_extract(o.inputs_json,'$.promotion_action'),'BUY'))='BUY'
 AND o.size_units=1 AND o.visible_capacity>=1
 AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1
 AND o.depth_known=1 AND o.fee_known=1 AND o.observed_ts>=?
ORDER BY o.observed_ts DESC,o.id DESC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ResearchPromotionCandidate, 0, limit)
	for rows.Next() {
		var c ResearchPromotionCandidate
		var observed string
		if err := rows.Scan(&c.ObservationID, &observed, &c.SystemID, &c.ExperimentVersion,
			&c.Cohort, &c.Venue, &c.Route, &c.Ticker, &c.Title, &c.Side,
			&c.CanonicalEventID, &c.EventVersion, &c.CanonicalPayoffID, &c.PayoffVersion,
			&c.ObservedPrice, &c.ObservedFeePC, &c.VisibleCapacity, &c.SelectorID,
			&c.StrategyFamily, &c.FiredSide,
			&c.ExpectedNetLower); err != nil {
			return nil, err
		}
		c.Observed = parsePromotionTime(observed)
		if c.Title == "" {
			c.Title = c.Ticker
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func researchPromotionIntentID(p ResearchPromotionProof, c ResearchPromotionCandidate) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%d|%s|%s|%s|%d|%s|%s|%s|%s|%s|%s", p.RunID, p.ResultHash,
		c.ObservationID, c.SystemID, c.Venue, c.Route, c.ExperimentVersion,
		p.Governance.RouteEconomicsReceiptID, p.Governance.CauseSpecHash,
		p.Governance.CauseGraphManifestHash, c.StrategyFamily, c.SelectorID, c.FiredSide)))
	return hex.EncodeToString(h[:])
}

func nativeInversePromotionCandidateValid(p ResearchPromotionProof, c ResearchPromotionCandidate) bool {
	if p.SystemID != "paired-bridge-inversion" || !strings.HasPrefix(p.Cohort, "native-inverse-v1|") {
		return true
	}
	family := strings.ToLower(strings.TrimSpace(c.StrategyFamily))
	selector := strings.TrimSpace(c.SelectorID)
	fired, bought := strings.ToUpper(strings.TrimSpace(c.FiredSide)), strings.ToUpper(strings.TrimSpace(c.Side))
	return strings.HasPrefix(family, "invert:") && !strings.HasPrefix(family, "invert:invert:") &&
		strings.EqualFold(strings.TrimSpace(p.StrategyFamily), family) &&
		strings.TrimSpace(p.SelectorID) == selector && strings.EqualFold(strings.TrimSpace(p.FiredSide), fired) &&
		selector != "" && p.Cohort == "native-inverse-v1|selector="+selector+"|candidate|policy="+selector &&
		c.Cohort == p.Cohort &&
		p.Route == "taker" && c.Route == "taker" && (fired == "YES" || fired == "NO") &&
		(bought == "YES" || bought == "NO") && fired != bought
}

func (s *Store) InsertResearchPromotionIntent(ctx context.Context, p ResearchPromotionProof,
	c ResearchPromotionCandidate, paperPrice, paperFeePC, edgeFloor float64) (ResearchPromotionIntent, bool, error) {
	in := ResearchPromotionIntent{Proof: p, Candidate: c, Created: time.Now().UTC(),
		PaperPrice: paperPrice, PaperFeePC: paperFeePC, RequiredEdgeFloor: edgeFloor,
		Governance: p.Governance}
	in.IntentID = researchPromotionIntentID(p, c)
	in.SourceID = "r139p:" + in.IntentID
	// The primary Store DSN uses _txlock=immediate. Holding this write transaction across the
	// pending/freshness reads and INSERT prevents a Step-7 completion or experiment-version append
	// from racing between validation and intent creation.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return in, false, err
	}
	defer tx.Rollback()
	if pending, err := step7ProjectionPendingInWindow(ctx, tx, time.Time{}, p.UntouchedEnd); err != nil {
		return in, false, err
	} else if pending > 0 {
		return in, false, fmt.Errorf("promotion proof window incomplete: all-history manifest has %d Step-7 projections pending", pending)
	}
	if fresh, reason, err := researchPromotionProofFreshWith(ctx, tx, p); err != nil {
		return in, false, err
	} else if !fresh {
		return in, false, fmt.Errorf("stale sealed promotion proof: %s", reason)
	}
	if err := validateResearchPromotionEconomics(ctx, tx, p, c); err != nil {
		return in, false, err
	}
	if err := validateResearchPromotionGovernanceCurrent(ctx, tx, in.Governance, in.Created); err != nil {
		return in, false, err
	}
	in.MaxAllInUnit = p.MeanAllIn + p.LowerPC - edgeFloor
	currentAllIn := c.ObservedPrice + c.ObservedFeePC
	c.ExpectedNetLower = math.Min(p.LowerPC-math.Max(0, currentAllIn-p.MeanAllIn), 1-currentAllIn)
	in.Candidate.ExpectedNetLower = c.ExpectedNetLower
	if currentAllIn > 0 {
		in.Governance.Mode4FractionCeiling = math.Min(.25, .25*c.ExpectedNetLower/currentAllIn)
	}
	mode4Raw := fmt.Sprintf("%s|%s|%s|%d|%s|%.12f|%.12f|%.12f|%.12f|%.12f",
		in.Governance.RouteEconomicsReceiptID, in.Governance.CauseExposureID,
		in.Governance.CauseSpecHash, in.Governance.CauseExposureVersion,
		in.Governance.CauseGraphManifestHash, in.Governance.NetPerDayLower,
		in.Governance.Capacity, in.Governance.CapitalDollarHoursPerDay,
		c.ExpectedNetLower, in.Governance.Mode4FractionCeiling)
	mode4Hash := sha256.Sum256([]byte(mode4Raw))
	in.Governance.Mode4InputHash = hex.EncodeToString(mode4Hash[:])
	if p.RunID <= 0 || !validSHA256(p.ResultHash) || c.ObservationID <= 0 || edgeFloor <= 0 ||
		paperPrice <= 0 || paperPrice >= 1 || !finiteResearchNumber(paperFeePC) || currentAllIn <= 0 ||
		!finiteResearchNumber(in.MaxAllInUnit) || !finiteResearchNumber(c.ExpectedNetLower) ||
		c.ExpectedNetLower < edgeFloor || paperPrice+paperFeePC > in.MaxAllInUnit+1e-9 ||
		in.Governance.RouteEconomicsReceiptID == "" || in.Governance.CauseExposureID == "" ||
		!validSHA256(in.Governance.CauseSpecHash) || !validSHA256(in.Governance.CauseGraphManifestHash) ||
		in.Governance.NetPerDayLower <= 0 || in.Governance.Capacity < 1 ||
		!finiteResearchNumber(in.Governance.CapitalDollarHoursPerDay) || in.Governance.CapitalDollarHoursPerDay < 0 ||
		in.Governance.Mode4FractionCeiling <= 0 || in.Governance.Mode4FractionCeiling > .25 ||
		!validSHA256(in.Governance.Mode4InputHash) || !nativeInversePromotionCandidateValid(p, c) {
		return in, false, errors.New("current Paper all-in cost does not fit sealed promotion proof")
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_promotion_intents(
intent_id,created_ts,sealed_inference_run_id,sealed_result_hash,preregistration_id,system_id,
experiment_version,cohort,strategy_family,selector_id,fired_side,venue,route,observation_id,canonical_event_id,event_version,
canonical_payoff_id,payoff_version,ticker,side,paper_price,paper_fee_pc,proof_mean_all_in,
current_expected_net_lower,proof_lower_pc,required_edge_floor,max_all_in_unit,
route_economics_receipt_id,cause_exposure_id,cause_exposure_version,cause_spec_hash,
cause_graph_manifest_hash,net_per_day_lower,capacity,capital_dollar_hours_per_day,
mode4_fraction_ceiling,mode4_input_hash,source_id)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, in.IntentID, in.Created.Format(time.RFC3339Nano),
		p.RunID, p.ResultHash, p.PreregistrationID, p.SystemID, p.ExperimentVersion, p.Cohort,
		c.StrategyFamily, c.SelectorID, strings.ToUpper(c.FiredSide), p.Venue, p.Route,
		c.ObservationID, c.CanonicalEventID, c.EventVersion, c.CanonicalPayoffID,
		c.PayoffVersion, c.Ticker, strings.ToUpper(c.Side), paperPrice, paperFeePC, p.MeanAllIn, c.ExpectedNetLower,
		p.LowerPC, edgeFloor, in.MaxAllInUnit, in.Governance.RouteEconomicsReceiptID,
		in.Governance.CauseExposureID, in.Governance.CauseExposureVersion, in.Governance.CauseSpecHash,
		in.Governance.CauseGraphManifestHash, in.Governance.NetPerDayLower, in.Governance.Capacity,
		in.Governance.CapitalDollarHoursPerDay, in.Governance.Mode4FractionCeiling,
		in.Governance.Mode4InputHash, in.SourceID)
	if err != nil {
		return in, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return in, false, err
	}
	if err := tx.Commit(); err != nil {
		return in, false, err
	}
	return in, n > 0, nil
}

func (s *Store) AppendResearchPromotionEvent(ctx context.Context, in ResearchPromotionIntent,
	eventType, route string, price, feePC float64, reason string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO research_promotion_events(intent_id,observed_ts,event_type,
venue,ticker,side,route,price,fee_pc,reason) VALUES(?,?,?,?,?,?,?,?,?,?)`, in.IntentID,
		time.Now().UTC().Format(time.RFC3339Nano), eventType, in.Candidate.Venue, in.Candidate.Ticker,
		strings.ToUpper(in.Candidate.Side), route, math.Max(0, price), feePC, firstNonEmptyStorage(reason, eventType))
	return err
}

func ResearchPromotionIntentFromSource(source string) string {
	id, ok := strings.CutPrefix(strings.TrimSpace(source), "r139p:")
	if !ok || len(id) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(id); err != nil {
		return ""
	}
	return id
}

func (s *Store) ResearchPromotionIntentRoute(ctx context.Context, source string) (route string, ok bool, err error) {
	id := ResearchPromotionIntentFromSource(source)
	if id == "" {
		return "", false, nil
	}
	err = s.db.QueryRowContext(ctx, `SELECT route FROM research_promotion_intents WHERE intent_id=?`, id).Scan(&route)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return route, err == nil, err
}

func (s *Store) ResearchPromotionIntentForSource(ctx context.Context, source string) (ResearchPromotionIntent, bool, error) {
	var in ResearchPromotionIntent
	id := ResearchPromotionIntentFromSource(source)
	if id == "" {
		return in, false, nil
	}
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT p.intent_id,p.source_id,p.created_ts,p.sealed_inference_run_id,
p.sealed_result_hash,p.preregistration_id,p.system_id,p.experiment_version,p.cohort,
p.strategy_family,p.selector_id,p.fired_side,p.venue,p.route,
p.observation_id,p.canonical_event_id,p.event_version,p.canonical_payoff_id,p.payoff_version,
p.ticker,p.side,p.paper_price,p.paper_fee_pc,p.current_expected_net_lower,p.proof_mean_all_in,
p.proof_lower_pc,p.required_edge_floor,p.max_all_in_unit,p.route_economics_receipt_id,
p.cause_exposure_id,p.cause_exposure_version,p.cause_spec_hash,p.cause_graph_manifest_hash,
p.net_per_day_lower,p.capacity,p.capital_dollar_hours_per_day,p.mode4_fraction_ceiling,p.mode4_input_hash
FROM research_promotion_intents p WHERE p.intent_id=? AND p.current_expected_net_lower>0
 AND (p.system_id!='paired-bridge-inversion' OR p.cohort NOT LIKE 'native-inverse-v1|%' OR (
  p.strategy_family LIKE 'invert:%' AND p.strategy_family NOT LIKE 'invert:invert:%'
  AND p.selector_id<>'' AND p.fired_side IN ('YES','NO') AND p.fired_side<>p.side
  AND p.cohort='native-inverse-v1|selector='||p.selector_id||'|candidate|policy='||p.selector_id
  AND EXISTS(SELECT 1 FROM research_system_observations o WHERE o.id=p.observation_id
   AND o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
   AND CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT)=p.strategy_family
   AND CAST(json_extract(o.inputs_json,'$.selector_id') AS TEXT)=p.selector_id
   AND UPPER(CAST(json_extract(o.inputs_json,'$.fired_side') AS TEXT))=p.fired_side
   AND UPPER(CAST(json_extract(o.inputs_json,'$.inverted_side') AS TEXT))=p.side
   AND LOWER(CAST(json_extract(o.inputs_json,'$.execution_route') AS TEXT))=p.route)))`, id).Scan(
		&in.IntentID, &in.SourceID, &created, &in.Proof.RunID, &in.Proof.ResultHash,
		&in.Proof.PreregistrationID, &in.Proof.SystemID, &in.Proof.ExperimentVersion, &in.Proof.Cohort,
		&in.Candidate.StrategyFamily, &in.Candidate.SelectorID, &in.Candidate.FiredSide,
		&in.Proof.Venue, &in.Proof.Route, &in.Candidate.ObservationID, &in.Candidate.CanonicalEventID,
		&in.Candidate.EventVersion, &in.Candidate.CanonicalPayoffID, &in.Candidate.PayoffVersion,
		&in.Candidate.Ticker, &in.Candidate.Side, &in.PaperPrice, &in.PaperFeePC,
		&in.Candidate.ExpectedNetLower, &in.Proof.MeanAllIn, &in.Proof.LowerPC,
		&in.RequiredEdgeFloor, &in.MaxAllInUnit, &in.Governance.RouteEconomicsReceiptID,
		&in.Governance.CauseExposureID, &in.Governance.CauseExposureVersion, &in.Governance.CauseSpecHash,
		&in.Governance.CauseGraphManifestHash, &in.Governance.NetPerDayLower, &in.Governance.Capacity,
		&in.Governance.CapitalDollarHoursPerDay, &in.Governance.Mode4FractionCeiling,
		&in.Governance.Mode4InputHash)
	if errors.Is(err, sql.ErrNoRows) {
		return in, false, nil
	}
	if err != nil {
		return in, false, err
	}
	in.Created = parsePromotionTime(created)
	in.Candidate.SystemID, in.Candidate.ExperimentVersion = in.Proof.SystemID, in.Proof.ExperimentVersion
	in.Candidate.Cohort, in.Candidate.Venue, in.Candidate.Route = in.Proof.Cohort, in.Proof.Venue, in.Proof.Route
	in.Proof.StrategyFamily, in.Proof.SelectorID, in.Proof.FiredSide = in.Candidate.StrategyFamily, in.Candidate.SelectorID, in.Candidate.FiredSide
	in.Proof.Governance = in.Governance
	return in, true, nil
}

func (s *Store) UnresolvedResearchPromotionIntents(ctx context.Context, limit int) ([]ResearchPromotionIntent, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.source_id FROM research_promotion_intents p
WHERE NOT EXISTS(SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id
 AND e.event_type IN ('paper_accepted','paper_rejected')) ORDER BY p.created_ts,p.intent_id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	sources := []string{}
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]ResearchPromotionIntent, 0, len(sources))
	for _, source := range sources {
		in, ok, readErr := s.ResearchPromotionIntentForSource(ctx, source)
		if readErr != nil {
			return nil, readErr
		}
		if ok {
			out = append(out, in)
		}
	}
	return out, nil
}

func (s *Store) AcceptedResearchPromotionIntent(ctx context.Context, source string) (ResearchPromotionIntent, bool, error) {
	var in ResearchPromotionIntent
	id := ResearchPromotionIntentFromSource(source)
	if id == "" {
		return in, false, nil
	}
	var created string
	err := s.db.QueryRowContext(ctx, `SELECT p.intent_id,p.source_id,p.created_ts,p.sealed_inference_run_id,
p.sealed_result_hash,p.preregistration_id,p.system_id,p.experiment_version,p.cohort,
p.strategy_family,p.selector_id,p.fired_side,p.venue,p.route,
p.observation_id,p.canonical_event_id,p.event_version,p.canonical_payoff_id,p.payoff_version,
p.ticker,p.side,p.paper_price,p.paper_fee_pc,p.current_expected_net_lower,p.proof_mean_all_in,p.proof_lower_pc,
p.required_edge_floor,p.max_all_in_unit,p.route_economics_receipt_id,p.cause_exposure_id,
p.cause_exposure_version,p.cause_spec_hash,p.cause_graph_manifest_hash,p.net_per_day_lower,p.capacity,
p.capital_dollar_hours_per_day,p.mode4_fraction_ceiling,p.mode4_input_hash
FROM research_promotion_intents p WHERE p.intent_id=?
 AND p.current_expected_net_lower>0
 AND (p.system_id!='paired-bridge-inversion' OR p.cohort NOT LIKE 'native-inverse-v1|%' OR (
  p.strategy_family LIKE 'invert:%' AND p.strategy_family NOT LIKE 'invert:invert:%'
  AND p.selector_id<>'' AND p.fired_side IN ('YES','NO') AND p.fired_side<>p.side
  AND p.cohort='native-inverse-v1|selector='||p.selector_id||'|candidate|policy='||p.selector_id
  AND EXISTS(SELECT 1 FROM research_system_observations o WHERE o.id=p.observation_id
   AND o.observation_kind='candidate' AND o.candidate=1 AND o.blocker=''
   AND CAST(json_extract(o.inputs_json,'$.strategy_family') AS TEXT)=p.strategy_family
   AND CAST(json_extract(o.inputs_json,'$.selector_id') AS TEXT)=p.selector_id
   AND UPPER(CAST(json_extract(o.inputs_json,'$.fired_side') AS TEXT))=p.fired_side
   AND UPPER(CAST(json_extract(o.inputs_json,'$.inverted_side') AS TEXT))=p.side
   AND LOWER(CAST(json_extract(o.inputs_json,'$.execution_route') AS TEXT))=p.route)))
 AND EXISTS(SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='paper_accepted')
	 AND NOT EXISTS(SELECT 1 FROM research_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='paper_rejected')
	 AND EXISTS(SELECT 1 FROM research_cause_exposure_specs c WHERE c.exposure_id=p.cause_exposure_id
	  AND c.version=p.cause_exposure_version AND c.spec_hash=p.cause_spec_hash AND c.input_state='active'
	  AND c.version=(SELECT MAX(v.version) FROM research_cause_exposure_specs v WHERE v.exposure_id=c.exposure_id)
	  AND julianday(c.valid_until_ts)>julianday(?) AND c.replicated_untouched=1
	  AND c.executable_fee_net_lower_bound=1)
	 AND EXISTS(SELECT 1 FROM research_cause_graph_runs g WHERE g.manifest_hash=p.cause_graph_manifest_hash
	  AND g.state='READY' AND g.input_count=g.valid_input_count AND g.valid_input_count>0
	  AND EXISTS(SELECT 1 FROM json_each(g.input_hashes_json) j WHERE j.value='valid:'||p.cause_spec_hash))`,
		id, time.Now().UTC().Format(time.RFC3339Nano)).Scan(
		&in.IntentID, &in.SourceID, &created, &in.Proof.RunID, &in.Proof.ResultHash,
		&in.Proof.PreregistrationID, &in.Proof.SystemID, &in.Proof.ExperimentVersion, &in.Proof.Cohort,
		&in.Candidate.StrategyFamily, &in.Candidate.SelectorID, &in.Candidate.FiredSide,
		&in.Proof.Venue, &in.Proof.Route, &in.Candidate.ObservationID, &in.Candidate.CanonicalEventID,
		&in.Candidate.EventVersion, &in.Candidate.CanonicalPayoffID, &in.Candidate.PayoffVersion,
		&in.Candidate.Ticker, &in.Candidate.Side, &in.PaperPrice, &in.PaperFeePC,
		&in.Candidate.ExpectedNetLower, &in.Proof.MeanAllIn,
		&in.Proof.LowerPC, &in.RequiredEdgeFloor, &in.MaxAllInUnit,
		&in.Governance.RouteEconomicsReceiptID, &in.Governance.CauseExposureID,
		&in.Governance.CauseExposureVersion, &in.Governance.CauseSpecHash,
		&in.Governance.CauseGraphManifestHash, &in.Governance.NetPerDayLower,
		&in.Governance.Capacity, &in.Governance.CapitalDollarHoursPerDay,
		&in.Governance.Mode4FractionCeiling, &in.Governance.Mode4InputHash)
	if errors.Is(err, sql.ErrNoRows) {
		return in, false, nil
	}
	if err != nil {
		return in, false, err
	}
	in.Created = parsePromotionTime(created)
	in.Candidate.SystemID, in.Candidate.ExperimentVersion = in.Proof.SystemID, in.Proof.ExperimentVersion
	in.Candidate.Cohort, in.Candidate.Venue, in.Candidate.Route = in.Proof.Cohort, in.Proof.Venue, in.Proof.Route
	in.Proof.StrategyFamily, in.Proof.SelectorID, in.Proof.FiredSide = in.Candidate.StrategyFamily, in.Candidate.SelectorID, in.Candidate.FiredSide
	in.Proof.Governance = in.Governance
	return in, true, nil
}

// ResearchPromotionIntentFreshForNewDispatch is the last proof-only gate before a new venue POST.
// It is intentionally separate from AcceptedResearchPromotionIntent and the pending-reconciliation
// readers: once a POST has happened, stale research proof must never hide the durable order from
// reconciliation. This method admits only an accepted intent with no prior dispatch/venue receipt,
// then reloads its authoritative proof window and rechecks all-history Step-7 completeness plus the
// current pipeline, experiment contract, and exact completion manifest in one immediate snapshot.
func (s *Store) ResearchPromotionIntentFreshForNewDispatch(ctx context.Context,
	in ResearchPromotionIntent) (bool, string, error) {
	if strings.TrimSpace(in.IntentID) == "" || strings.TrimSpace(in.SourceID) == "" {
		return false, "promotion intent identity is missing", nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback()
	var startRaw, endRaw string
	var observedRows int
	var meanPC, lowerPC float64
	proof := in.Proof
	governance := in.Governance
	err = tx.QueryRowContext(ctx, `SELECT p.untouched_start_ts,p.untouched_end_ts,
r.observed_rows,s.untouched_mean,s.untouched_lower,
i.cause_exposure_id,i.cause_exposure_version,i.cause_spec_hash
FROM research_promotion_intents i
JOIN research_inference_runs r ON r.id=i.sealed_inference_run_id
JOIN research_inference_preregistrations p ON p.preregistration_id=i.preregistration_id
JOIN research_inference_system_results s ON s.run_id=r.id AND s.system_id=i.system_id
WHERE i.intent_id=? AND i.source_id=? AND i.sealed_inference_run_id=?
 AND REPLACE(LOWER(i.sealed_result_hash),'sha256:','')=?
 AND i.preregistration_id=? AND i.system_id=? AND i.experiment_version=?
 AND i.cohort=? AND i.venue=? AND i.route=?
 AND EXISTS(SELECT 1 FROM research_promotion_events e
  WHERE e.intent_id=i.intent_id AND e.event_type='paper_accepted')
 AND NOT EXISTS(SELECT 1 FROM research_promotion_events e
  WHERE e.intent_id=i.intent_id AND e.event_type IN ('paper_rejected','live_dispatched'))
 AND NOT EXISTS(SELECT 1 FROM research_live_execution_receipts x WHERE x.intent_id=i.intent_id)`,
		in.IntentID, in.SourceID, proof.RunID,
		strings.TrimPrefix(strings.ToLower(strings.TrimSpace(proof.ResultHash)), "sha256:"),
		proof.PreregistrationID, proof.SystemID, proof.ExperimentVersion,
		proof.Cohort, proof.Venue, proof.Route).
		Scan(&startRaw, &endRaw, &observedRows, &meanPC, &lowerPC,
			&governance.CauseExposureID, &governance.CauseExposureVersion, &governance.CauseSpecHash)
	if errors.Is(err, sql.ErrNoRows) {
		return false, "intent is not an accepted, undispatched durable promotion", nil
	}
	if err != nil {
		return false, "", err
	}
	proof.UntouchedStart, proof.UntouchedEnd = parsePromotionTime(startRaw), parsePromotionTime(endRaw)
	proof.ObservedRows, proof.MeanPC, proof.LowerPC = observedRows, meanPC, lowerPC
	if pending, err := step7ProjectionPendingInWindow(ctx, tx, time.Time{}, proof.UntouchedEnd); err != nil {
		return false, "", err
	} else if pending > 0 {
		return false, fmt.Sprintf("all-history manifest has %d Step-7 projections pending", pending), nil
	}
	if fresh, reason, err := researchPromotionProofFreshWith(ctx, tx, proof); err != nil {
		return false, "", err
	} else if !fresh {
		return false, reason, nil
	}
	if err := validateResearchPromotionGovernanceCurrent(ctx, tx, governance, time.Now().UTC()); err != nil {
		return false, err.Error(), nil
	}
	if err := tx.Commit(); err != nil {
		return false, "", err
	}
	return true, "", nil
}

func (s *Store) ResearchPromotionPaperRouteState(ctx context.Context, source, route string,
	since time.Time) (ResearchPromotionPaperState, error) {
	var out ResearchPromotionPaperState
	switch route {
	case "taker":
		var fee, contracts float64
		err := s.db.QueryRowContext(ctx, `SELECT price,fee,contracts FROM paper_fills
WHERE source=? AND action='BUY' AND ts>=? ORDER BY id DESC LIMIT 1`, source,
			since.UTC().Format(time.RFC3339)).Scan(&out.Price, &fee, &contracts)
		if err == nil && contracts > 0 {
			out.State, out.FeePC, out.FeeSource = "filled", fee/contracts, "paper_fills.exact_fee"
			return out, nil
		}
		if errors.Is(err, sql.ErrNoRows) {
			out.State = "missing"
			return out, nil
		}
		return out, err
	case "maker":
		var filled int
		var makerFee, makerRebate sql.NullFloat64
		var feeSource, cancelRule string
		err := s.db.QueryRowContext(ctx, `SELECT post_px,filled,maker_fee_pc,maker_rebate_pc,
COALESCE(maker_fee_source,''),COALESCE(cancel_rule,'') FROM maker_fill_stats
WHERE source=? AND ts>=? ORDER BY id DESC LIMIT 1`, source,
			since.UTC().Format(time.RFC3339)).Scan(&out.Price, &filled, &makerFee, &makerRebate,
			&feeSource, &cancelRule)
		if errors.Is(err, sql.ErrNoRows) {
			out.State = "missing"
			return out, nil
		}
		if err != nil {
			return out, err
		}
		switch filled {
		case -1:
			out.State, out.Reason = "pending", "maker order is still resting"
		case 1:
			if !makerFee.Valid || !makerRebate.Valid || strings.TrimSpace(feeSource) == "" {
				out.State, out.Reason = "terminal_rejected", "maker fill lacks exact fee/rebate authority"
				break
			}
			out.State, out.FeePC, out.FeeSource = "filled", makerFee.Float64-makerRebate.Float64, feeSource
		case 0, -2:
			out.State, out.Reason = "terminal_rejected", firstNonEmptyStorage(cancelRule, "maker order canceled, expired, or gated")
		default:
			out.State, out.Reason = "terminal_rejected", "unknown maker terminal state"
		}
		return out, nil
	default:
		return out, errors.New("unsupported promotion route")
	}
}
