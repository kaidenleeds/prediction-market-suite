package storage

// Resolution-driven terminal inputs for the 19 immutable R138 experiments. Prospective taker
// candidates and their fully priced matched taker controls both grade for training when route and
// settlement truth are exact. R175 also retains the one exact Step-7 structural action lane long
// enough to attach its venue outcome; that lane remains blocked, non-candidate, and excluded from
// inference/Paper/LIVE. Observer and maker/no-fill controls remain absent here.

import (
	"context"
	"fmt"
	"time"
)

const (
	Step7StructuralFlowExperimentVersion = 3
	Step7StructuralFlowCohort            = "authoritative-flow-follow-atomic-v3"
	Step7StructuralFlowSelector          = "authoritative-flow-follow-atomic-v3"
	Step7StructuralFlowSemantics         = "kalshi-venue-local-structural-identity-v3"
	Step7StructuralFlowBlocker           = "venue_local_structural_identity_only_no_profit_or_cash_authority"
)

type ResearchSystemTakerSettlement struct {
	ObservationID                                                    int64
	Observed                                                         time.Time
	SystemID, ObservationKind, CanonicalEventID, Venue, Ticker, Side string
	Size, Cost, Fee, CapitalSeconds                                  float64
	EventVersion                                                     int
}

// Keep the ordinary candidate/control lane and the R175 structural-negative lane as separate
// UNION ALL branches. Putting both behind one OR makes SQLite drive this query from a full scan of
// research_route_opportunities, even though each observation branch has a purpose-built partial
// index. The explicit INDEXED BY clauses and outer merge preserve one coherent oldest-first limit.
const researchSystemTakerSettlementSQL = `SELECT * FROM (
SELECT o.id AS observation_id,o.observed_ts,o.system_id,o.observation_kind,o.canonical_event_id,
o.event_version,r.venue,r.ticker,r.side,o.size_units,o.executable_cost,o.exact_fee,o.capital_seconds
FROM research_system_observations o INDEXED BY idx_rsystem_obs_pending_taker
JOIN research_route_opportunities r INDEXED BY idx_rroute_system_observation
 ON CAST(json_extract(r.evidence_json,'$.system_observation_id') AS INTEGER)=o.id
WHERE o.outcome_status='open' AND o.route='taker'
 AND o.observation_kind IN ('candidate','control')
 AND ((o.observation_kind='candidate' AND o.candidate=1 AND r.decision='candidate')
   OR (o.observation_kind='control' AND o.candidate=0 AND r.decision='control'))
 AND r.route='taker' AND r.action='buy'
 AND r.venue=o.venue AND r.ticker=o.ticker AND r.side=o.side
 AND o.ticker!='' AND o.side IN ('YES','NO')
 AND o.size_units>0 AND o.event_version>0 AND o.canonical_event_id!=''
 AND o.canonical_payoff_id IS NOT NULL AND o.canonical_payoff_id!='' AND o.payoff_version>0
 AND o.certificate_status IN ('verified','structural')
 AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1
 AND o.source_clock_id!='' AND o.book_source!='' AND o.fee_source!=''
 AND EXISTS (SELECT 1 FROM research_event_specs e
  WHERE e.event_id=o.canonical_event_id AND e.version=o.event_version)
 AND EXISTS (SELECT 1 FROM research_payoff_specs p
  WHERE p.payoff_id=o.canonical_payoff_id AND p.version=o.payoff_version
   AND p.event_id=o.canonical_event_id AND p.event_version=o.event_version)
 AND NOT EXISTS (SELECT 1 FROM research_system_payoff_updates u WHERE u.observation_id=o.id)
 AND (EXISTS (SELECT 1 FROM venue_settlements v
   WHERE v.platform=r.venue AND v.ticker=r.ticker AND v.resolved_at>=o.observed_ts)
  OR EXISTS (SELECT 1 FROM signal_log s
   WHERE s.platform=r.venue AND s.ticker=r.ticker AND s.resolved=1
    AND s.settle_val IS NOT NULL AND s.settle_val>=0 AND s.settle_val<=1
    AND s.resolved_at!='' AND s.resolved_at>=o.observed_ts))
UNION ALL
SELECT o.id AS observation_id,o.observed_ts,o.system_id,o.observation_kind,o.canonical_event_id,
o.event_version,r.venue,r.ticker,r.side,o.size_units,o.executable_cost,o.exact_fee,o.capital_seconds
FROM research_system_observations o INDEXED BY idx_rsystem_obs_pending_step7_structural_v3
JOIN research_route_opportunities r INDEXED BY idx_rroute_system_observation
 ON CAST(json_extract(r.evidence_json,'$.system_observation_id') AS INTEGER)=o.id
WHERE o.outcome_status='open' AND o.route='taker'
 AND o.observation_kind='negative' AND o.candidate=0 AND r.decision='blocked'
 AND o.system_id='flow-direction-integrity' AND o.experiment_version=3
 AND o.cohort='` + Step7StructuralFlowCohort + `|structural-action|policy=` + Step7StructuralFlowSelector + `'
 AND o.certificate_status='structural' AND o.blocker='` + Step7StructuralFlowBlocker + `'
 AND r.identity_status='structural' AND r.decision_reason=o.blocker
 AND CAST(json_extract(o.inputs_json,'$.direction_selected') AS INTEGER)=1
 AND CAST(json_extract(o.inputs_json,'$.frozen_identity_status') AS TEXT)='structural'
 AND CAST(json_extract(o.inputs_json,'$.identity_scope') AS TEXT)='kalshi_exact_ticker_payoff_route'
 AND CAST(json_extract(o.inputs_json,'$.flow_projection_semantics') AS TEXT)='` + Step7StructuralFlowSemantics + `'
 AND CAST(json_extract(o.inputs_json,'$.cross_venue_equivalence_verified') AS INTEGER)=0
 AND CAST(json_extract(o.inputs_json,'$.profit_authority') AS INTEGER)=0
 AND CAST(json_extract(o.inputs_json,'$.paper_authority') AS INTEGER)=0
 AND CAST(json_extract(o.inputs_json,'$.live_authority') AS INTEGER)=0
 AND r.route='taker' AND r.action='buy'
 AND r.venue=o.venue AND r.ticker=o.ticker AND r.side=o.side
 AND o.ticker!='' AND o.side IN ('YES','NO')
 AND o.size_units>0 AND o.event_version>0 AND o.canonical_event_id!=''
 AND o.canonical_payoff_id IS NOT NULL AND o.canonical_payoff_id!='' AND o.payoff_version>0
 AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1
 AND o.source_clock_id!='' AND o.book_source!='' AND o.fee_source!=''
 AND EXISTS (SELECT 1 FROM research_event_specs e
  WHERE e.event_id=o.canonical_event_id AND e.version=o.event_version)
 AND EXISTS (SELECT 1 FROM research_payoff_specs p
  WHERE p.payoff_id=o.canonical_payoff_id AND p.version=o.payoff_version
   AND p.event_id=o.canonical_event_id AND p.event_version=o.event_version)
 AND NOT EXISTS (SELECT 1 FROM research_system_payoff_updates u WHERE u.observation_id=o.id)
 AND (EXISTS (SELECT 1 FROM venue_settlements v
   WHERE v.platform=r.venue AND v.ticker=r.ticker AND v.resolved_at>=o.observed_ts)
  OR EXISTS (SELECT 1 FROM signal_log s
   WHERE s.platform=r.venue AND s.ticker=r.ticker AND s.resolved=1
    AND s.settle_val IS NOT NULL AND s.settle_val>=0 AND s.settle_val<=1
    AND s.resolved_at!='' AND s.resolved_at>=o.observed_ts))
) ORDER BY observed_ts,observation_id LIMIT ?`

func (s *Store) PendingResearchSystemTakerSettlements(ctx context.Context, limit int) ([]ResearchSystemTakerSettlement, error) {
	if limit <= 0 || limit > 1000 {
		limit = 250
	}
	rows, err := s.db.QueryContext(ctx, researchSystemTakerSettlementSQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ResearchSystemTakerSettlement, 0, limit)
	for rows.Next() {
		var v ResearchSystemTakerSettlement
		var observed string
		if err := rows.Scan(&v.ObservationID, &observed, &v.SystemID, &v.ObservationKind, &v.CanonicalEventID,
			&v.EventVersion, &v.Venue, &v.Ticker, &v.Side, &v.Size, &v.Cost, &v.Fee,
			&v.CapitalSeconds); err != nil {
			return nil, err
		}
		var parseErr error
		v.Observed, parseErr = time.Parse(time.RFC3339Nano, observed)
		if parseErr != nil {
			return nil, fmt.Errorf("parse research system observation time: %w", parseErr)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
