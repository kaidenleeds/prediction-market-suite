package storage

// Frozen prior-window selectors turn completed matched controls into a deterministic policy for
// later, untouched opportunities. They never re-label training rows and carry no Paper/LIVE
// authority. A selected post-freeze row is still only a prospective research action; promotion
// requires a separate sealed untouched result and the Paper-first bridge.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const frozenSelectorSchema = `
CREATE TABLE IF NOT EXISTS research_frozen_selectors (
 selector_id TEXT PRIMARY KEY,
 created_ts TEXT NOT NULL,
 effective_start_ts TEXT NOT NULL,
 training_cutoff_ts TEXT NOT NULL,
 system_id TEXT NOT NULL,
 experiment_version INTEGER NOT NULL CHECK(experiment_version>0),
 source_cohort TEXT NOT NULL,
 selector_scope TEXT NOT NULL,
 selector_cell TEXT NOT NULL,
 selected_arm TEXT NOT NULL,
 training_days INTEGER NOT NULL CHECK(training_days>=1),
 training_events INTEGER NOT NULL CHECK(training_events>=1),
 selected_mean_net REAL NOT NULL,
 runner_up_mean_net REAL NOT NULL,
 rule_json TEXT NOT NULL CHECK(json_valid(rule_json)),
 spec_hash TEXT NOT NULL UNIQUE,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(system_id,experiment_version,source_cohort,selector_scope,selector_cell)
);
CREATE INDEX IF NOT EXISTS idx_frozen_selector_lookup ON research_frozen_selectors(system_id,experiment_version,source_cohort,selector_scope,selector_cell,effective_start_ts);
CREATE UNIQUE INDEX IF NOT EXISTS idx_frozen_selector_contract_v2 ON research_frozen_selectors(system_id,experiment_version,source_cohort,selector_scope,selector_cell);
CREATE TRIGGER IF NOT EXISTS research_frozen_selectors_no_update BEFORE UPDATE ON research_frozen_selectors BEGIN SELECT RAISE(ABORT,'immutable frozen selector'); END;
CREATE TRIGGER IF NOT EXISTS research_frozen_selectors_no_delete BEFORE DELETE ON research_frozen_selectors BEGIN SELECT RAISE(ABORT,'immutable frozen selector'); END;

CREATE TABLE IF NOT EXISTS research_frozen_selector_emissions (
 selector_id TEXT NOT NULL,
 source_observation_id INTEGER NOT NULL,
 candidate_observation_id INTEGER NOT NULL,
 emitted_ts TEXT NOT NULL,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 PRIMARY KEY(selector_id,source_observation_id),
 UNIQUE(candidate_observation_id),
 FOREIGN KEY(selector_id) REFERENCES research_frozen_selectors(selector_id),
 FOREIGN KEY(source_observation_id) REFERENCES research_system_observations(id),
 FOREIGN KEY(candidate_observation_id) REFERENCES research_system_observations(id)
);
CREATE TRIGGER IF NOT EXISTS research_frozen_selector_emissions_no_update BEFORE UPDATE ON research_frozen_selector_emissions BEGIN SELECT RAISE(ABORT,'immutable selector emission'); END;
CREATE TRIGGER IF NOT EXISTS research_frozen_selector_emissions_no_delete BEFORE DELETE ON research_frozen_selector_emissions BEGIN SELECT RAISE(ABORT,'immutable selector emission'); END;
`

func migrateFrozenSelectorSchema(db *sql.DB) error {
	_, _ = db.Exec(`ALTER TABLE research_frozen_selectors ADD COLUMN selector_scope TEXT NOT NULL DEFAULT ''`)
	_, err := db.Exec(frozenSelectorSchema)
	return err
}

type selectorTrainingRow struct {
	SystemID, Cohort, Scope, Cell, Arm, EventID string
	Version                                     int
	Observed                                    time.Time
	Realized                                    float64
}

type selectorArmStats struct {
	N      int
	Sum    float64
	Days   map[string]bool
	Events map[string]bool
}

func selectorString(inputs map[string]any, key string) string {
	v, _ := inputs[key].(string)
	return strings.TrimSpace(v)
}

// FreezeEligibleResearchSelectors consumes only terminal controls strictly before the current UTC
// day. Each canonical event contributes once per arm, preventing repeated five-minute snapshots
// from masquerading as independent evidence. The first sufficiently mature selector is immutable;
// a new policy requires a new experiment version.
func (s *Store) FreezeEligibleResearchSelectors(ctx context.Context, now time.Time,
	minDays, minEvents int) (int, error) {
	if minDays < 2 {
		minDays = 7
	}
	if minEvents < 2 {
		minEvents = 20
	}
	dayStart := now.UTC().Truncate(24 * time.Hour)
	cutoff := dayStart.Add(-24 * time.Hour) // one complete UTC-day purge/embargo before evaluation
	// Start from the terminal payoff ledger, not the multi-million-row observation ledger. The
	// ORDER BY otherwise made SQLite prefer idx_rsystem_obs_observed and probe payoff once for
	// every historical observation even when very few observations have any terminal payoff.
	// CROSS JOIN fixes only the loop order; the explicit ORDER BY preserves the prior deterministic
	// floating-point accumulation order.
	rows, err := s.db.QueryContext(ctx, `SELECT o.observed_ts,o.system_id,o.experiment_version,o.cohort,
o.canonical_event_id,o.inputs_json,u.realized_net
FROM research_system_payoff_updates u
CROSS JOIN research_system_observations o ON o.id=u.observation_id
WHERE u.status='settled' AND u.realized_net IS NOT NULL
 AND o.observation_kind='control' AND o.candidate=0 AND o.route='taker' AND o.observed_ts<?
 AND json_extract(o.inputs_json,'$.selector_training')=1
	AND COALESCE(json_extract(o.inputs_json,'$.no_release_in_recent_30m_cell'),0)!=1
ORDER BY o.observed_ts,o.id`, cutoff.Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	training := []selectorTrainingRow{}
	for rows.Next() {
		var rawTime, rawInputs string
		var row selectorTrainingRow
		if err := rows.Scan(&rawTime, &row.SystemID, &row.Version, &row.Cohort, &row.EventID,
			&rawInputs, &row.Realized); err != nil {
			return 0, err
		}
		row.Observed = parsePromotionTime(rawTime)
		inputs := map[string]any{}
		if json.Unmarshal([]byte(rawInputs), &inputs) != nil {
			continue
		}
		row.Scope, row.Cell = selectorString(inputs, "selector_scope"), selectorString(inputs, "selector_cell")
		row.Arm = selectorString(inputs, "selector_arm")
		if row.Scope == "" || row.Cell == "" || row.Arm == "" || row.EventID == "" || !finiteResearchNumber(row.Realized) {
			continue
		}
		training = append(training, row)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	type groupKey struct {
		system, cohort, scope, cell string
		version                     int
	}
	groups := map[groupKey]map[string]*selectorArmStats{}
	seenEventArm := map[string]bool{}
	for _, row := range training {
		key := groupKey{row.SystemID, row.Cohort, row.Scope, row.Cell, row.Version}
		dedupe := fmt.Sprintf("%s|%d|%s|%s|%s|%s|%s", key.system, key.version, key.cohort, key.scope, key.cell, row.Arm, row.EventID)
		if seenEventArm[dedupe] {
			continue
		}
		seenEventArm[dedupe] = true
		if groups[key] == nil {
			groups[key] = map[string]*selectorArmStats{}
		}
		if groups[key][row.Arm] == nil {
			groups[key][row.Arm] = &selectorArmStats{Days: map[string]bool{}, Events: map[string]bool{}}
		}
		arm := groups[key][row.Arm]
		arm.N++
		arm.Sum += row.Realized
		arm.Days[row.Observed.UTC().Format("2006-01-02")] = true
		arm.Events[row.EventID] = true
	}
	keys := make([]groupKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return fmt.Sprint(keys[i]) < fmt.Sprint(keys[j])
	})
	inserted := 0
	for _, key := range keys {
		arms := groups[key]
		eligible := make([]string, 0, len(arms))
		for arm, stats := range arms {
			if len(stats.Days) >= minDays && len(stats.Events) >= minEvents {
				eligible = append(eligible, arm)
			}
		}
		if len(eligible) < 2 {
			continue
		}
		sort.Slice(eligible, func(i, j int) bool {
			li, lj := arms[eligible[i]].Sum/float64(arms[eligible[i]].N), arms[eligible[j]].Sum/float64(arms[eligible[j]].N)
			if li == lj {
				return eligible[i] < eligible[j]
			}
			return li > lj
		})
		best, runner := arms[eligible[0]], arms[eligible[1]]
		bestMean, runnerMean := best.Sum/float64(best.N), runner.Sum/float64(runner.N)
		rule := map[string]any{"policy": "highest_event-disjoint_prior-utc-day_mean_net",
			"selected_arm": eligible[0], "tie_break": "lexicographic", "min_days_per_arm": minDays,
			"min_events_per_arm": minEvents, "training_cutoff_exclusive": cutoff.Format(time.RFC3339Nano),
			"embargo_seconds": 86400, "evaluation_day_start": dayStart.Format(time.RFC3339Nano),
			"same_clock_controls_retained": true, "reselection_without_version_bump": false}
		specHash := R138HashJSON(map[string]any{"system": key.system, "version": key.version,
			"cohort": key.cohort, "scope": key.scope, "cell": key.cell, "rule": rule})
		selectorID := "selector:" + strings.TrimPrefix(specHash, "sha256:")
		ruleJSON, _ := json.Marshal(rule)
		res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_frozen_selectors(
selector_id,created_ts,effective_start_ts,training_cutoff_ts,system_id,experiment_version,
source_cohort,selector_scope,selector_cell,selected_arm,training_days,training_events,selected_mean_net,
runner_up_mean_net,rule_json,spec_hash,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, selectorID, now.UTC().Format(time.RFC3339Nano),
			dayStart.Format(time.RFC3339Nano), cutoff.Format(time.RFC3339Nano), key.system, key.version,
			key.cohort, key.scope, key.cell, eligible[0], len(best.Days), len(best.Events), bestMean, runnerMean,
			string(ruleJSON), specHash)
		if err != nil {
			return inserted, err
		}
		n, _ := res.RowsAffected()
		inserted += int(n)
	}
	return inserted, nil
}

type frozenSelectorSource struct {
	SelectorID, SelectedArm, SourceCohort, SelectorScope string
	SelectedMean, RunnerMean                             float64
	ObservationID                                        int64
	Observation                                          ResearchSystemObservation
}

const frozenSelectorEmissionSelect = `SELECT f.selector_id,f.selected_arm,f.source_cohort,f.selector_scope,
f.selected_mean_net,f.runner_up_mean_net,o.id,o.observed_ts,o.system_id,o.experiment_version,
o.opportunity_id,o.canonical_event_id,o.event_version,o.canonical_payoff_id,o.payoff_version,
o.venue,o.ticker,o.route,o.side,o.certificate_status,o.certificate_hash,o.source_clock_id,
o.source_artifact,o.book_source,o.fee_source,o.quote_age_max_s,o.tick_min,o.size_units,
o.executable_cost,o.exact_fee,o.payout_lower,o.payout_upper,o.net_lower,o.net_upper,
o.visible_capacity,o.capital_seconds,o.decision_latency_ms,o.latency_known,o.quote_age_known,
o.tick_known,o.depth_known,o.fee_known,o.capacity_curve_json,o.inputs_json,o.outcome_status
FROM research_frozen_selectors f
CROSS JOIN research_system_observations o INDEXED BY idx_rsystem_obs_system
 ON o.system_id=f.system_id AND o.experiment_version=f.experiment_version AND o.cohort=f.source_cohort
WHERE o.observation_kind='control' AND o.candidate=0 AND o.observed_ts>=f.effective_start_ts
 AND o.observed_ts>=f.created_ts
 AND json_extract(o.inputs_json,'$.selector_training')=1
 AND CAST(json_extract(o.inputs_json,'$.selector_scope') AS TEXT)=f.selector_scope
 AND CAST(json_extract(o.inputs_json,'$.selector_cell') AS TEXT)=f.selector_cell
 AND CAST(json_extract(o.inputs_json,'$.selector_arm') AS TEXT)=f.selected_arm
 AND NOT EXISTS(SELECT 1 FROM research_frozen_selector_emissions e
  WHERE e.selector_id=f.selector_id AND e.source_observation_id=o.id)
ORDER BY o.observed_ts,o.id LIMIT ?`

// EmitFrozenSelectorCandidates copies only a post-freeze control whose immutable selector arm
// matches the frozen choice. The source control remains untouched and terminal-gradable.
func (s *Store) EmitFrozenSelectorCandidates(ctx context.Context, now time.Time, limit int) (int, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, frozenSelectorEmissionSelect, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	sources := []frozenSelectorSource{}
	for rows.Next() {
		var src frozenSelectorSource
		var rawObserved, curveJSON, inputsJSON string
		var latency, quoteAge, tick, depth, fee int
		o := &src.Observation
		if err := rows.Scan(&src.SelectorID, &src.SelectedArm, &src.SourceCohort, &src.SelectorScope,
			&src.SelectedMean, &src.RunnerMean, &src.ObservationID, &rawObserved, &o.SystemID,
			&o.ExperimentVersion, &o.OpportunityID, &o.CanonicalEventID, &o.EventVersion,
			&o.CanonicalPayoffID, &o.PayoffVersion, &o.Venue, &o.Ticker, &o.Route, &o.Side,
			&o.CertificateStatus, &o.CertificateHash, &o.SourceClockID, &o.SourceArtifact,
			&o.BookSource, &o.FeeSource, &o.QuoteAgeMax, &o.TickMin, &o.Size, &o.Cost, &o.Fee,
			&o.PayoutLower, &o.PayoutUpper, &o.NetLower, &o.NetUpper, &o.VisibleCapacity,
			&o.CapitalSeconds, &o.DecisionLatencyMS, &latency, &quoteAge, &tick, &depth, &fee,
			&curveJSON, &inputsJSON, &o.OutcomeStatus); err != nil {
			return 0, err
		}
		o.Observed = parsePromotionTime(rawObserved)
		o.LatencyKnown, o.QuoteAgeKnown, o.TickKnown = latency == 1, quoteAge == 1, tick == 1
		o.DepthKnown, o.FeeKnown = depth == 1, fee == 1
		_ = json.Unmarshal([]byte(curveJSON), &o.CapacityCurve)
		inputs := map[string]any{}
		_ = json.Unmarshal([]byte(inputsJSON), &inputs)
		o.Inputs = inputs
		sources = append(sources, src)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	emitted := 0
	for _, src := range sources {
		o := src.Observation
		o.Kind, o.Candidate, o.Blocker = "candidate", true, ""
		o.Cohort = src.SourceCohort + "|candidate|selector=" + src.SelectorID
		o.OpportunityID += "|selector=" + src.SelectorID
		inputs, _ := o.Inputs.(map[string]any)
		if inputs == nil {
			inputs = map[string]any{}
		}
		inputs["selector_id"] = src.SelectorID
		inputs["selector_selected_arm"] = src.SelectedArm
		inputs["selector_frozen_before_observation"] = true
		inputs["selector_training_mean_net"] = src.SelectedMean
		inputs["selector_runner_up_mean_net"] = src.RunnerMean
		inputs["economic_candidate"] = true
		o.Inputs = inputs
		candidateID, inserted, insertErr := s.InsertResearchSystemObservation(ctx, o)
		if insertErr != nil {
			return emitted, insertErr
		}
		if !inserted {
			insertErr = s.db.QueryRowContext(ctx, `SELECT id FROM research_system_observations
WHERE system_id=? AND opportunity_id=? AND observed_slot=? AND route=? AND size_units=?
 AND observation_kind='candidate'`, o.SystemID, o.OpportunityID, researchSlot(o.Observed, 5*time.Minute),
				o.Route, o.Size).Scan(&candidateID)
			if insertErr != nil && !errors.Is(insertErr, sql.ErrNoRows) {
				return emitted, insertErr
			}
		}
		if candidateID <= 0 {
			continue
		}
		res, insertErr := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_frozen_selector_emissions(
selector_id,source_observation_id,candidate_observation_id,emitted_ts,funded,paper_authority,live_authority)
VALUES(?,?,?,?,0,0,0)`, src.SelectorID, src.ObservationID, candidateID, now.UTC().Format(time.RFC3339Nano))
		if insertErr != nil {
			return emitted, insertErr
		}
		n, _ := res.RowsAffected()
		emitted += int(n)
	}
	return emitted, nil
}
