package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PolyUS exposes preliminary/intraday settlementPx marks as well as the final event result.
// A legacy mark can be fractional OR happen to equal 0/1, so payout alone cannot establish final
// authority. Preserve every receipt not written by one of the versioned final parsers and every
// dependent grade in immutable quarantine tables, then return those rows to the unresolved lane
// for re-grading from a future trusted receipt.
const polyUSSettlementQuarantineDDL = `
CREATE TABLE IF NOT EXISTS polyus_settlement_quarantine (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 platform TEXT NOT NULL,
 ticker TEXT NOT NULL,
 yes_value REAL NOT NULL,
 resolved_at TEXT NOT NULL,
 source_artifact TEXT NOT NULL,
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL,
 UNIQUE(platform,ticker,resolved_at,source_artifact,reason)
);
CREATE TABLE IF NOT EXISTS polyus_settlement_grade_quarantine (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 ledger TEXT NOT NULL CHECK(ledger IN ('signal_log','maker_fill_stats','unit_trials')),
 ledger_row_id INTEGER NOT NULL,
 platform TEXT NOT NULL,
 ticker TEXT NOT NULL,
 side TEXT NOT NULL DEFAULT '',
 source TEXT NOT NULL DEFAULT '',
 settle_value REAL NOT NULL,
 outcome INTEGER,
 resolved_at TEXT,
 pnl_pc REAL,
 return_per_dollar REAL,
 capital_day REAL,
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL,
 UNIQUE(ledger,ledger_row_id,reason)
);
CREATE TABLE IF NOT EXISTS polyus_paper_fill_settlement_quarantine (
 fill_id INTEGER PRIMARY KEY,
 platform TEXT NOT NULL,
 ticker TEXT NOT NULL,
 row_json TEXT NOT NULL CHECK(json_valid(row_json)),
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS polyus_parlay_settlement_quarantine (
 parlay_id INTEGER PRIMARY KEY,
 row_json TEXT NOT NULL CHECK(json_valid(row_json)),
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS polyus_kf_settlement_quarantine (
 quarantine_id TEXT PRIMARY KEY CHECK(length(quarantine_id)=64),
 book_name TEXT NOT NULL,
 sub_name TEXT NOT NULL DEFAULT '',
 ticker TEXT NOT NULL,
 side TEXT NOT NULL,
 closed_ts TEXT NOT NULL,
 row_json TEXT NOT NULL CHECK(json_valid(row_json)),
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS polyus_research_grade_quarantine (
 ledger TEXT NOT NULL CHECK(ledger IN ('proper_score_trials','system_payoff_updates','route_events','bundle_events')),
 ledger_row_id INTEGER NOT NULL,
 row_json TEXT NOT NULL CHECK(json_valid(row_json)),
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL,
 PRIMARY KEY(ledger,ledger_row_id,reason)
);
CREATE TABLE IF NOT EXISTS polyus_funded_json_epoch_quarantine (
 ledger_name TEXT NOT NULL,
 source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64),
 source_json TEXT NOT NULL CHECK(json_valid(source_json)),
 reason TEXT NOT NULL,
 quarantined_at TEXT NOT NULL,
 PRIMARY KEY(ledger_name,source_sha256)
);
CREATE TRIGGER IF NOT EXISTS polyus_settlement_quarantine_no_update
BEFORE UPDATE ON polyus_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_settlement_quarantine_no_delete
BEFORE DELETE ON polyus_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_settlement_grade_quarantine_no_update
BEFORE UPDATE ON polyus_settlement_grade_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS settlement-grade quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_settlement_grade_quarantine_no_delete
BEFORE DELETE ON polyus_settlement_grade_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS settlement-grade quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_paper_fill_settlement_quarantine_no_update
BEFORE UPDATE ON polyus_paper_fill_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS Paper-fill settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_paper_fill_settlement_quarantine_no_delete
BEFORE DELETE ON polyus_paper_fill_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS Paper-fill settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_parlay_settlement_quarantine_no_update
BEFORE UPDATE ON polyus_parlay_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS parlay settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_parlay_settlement_quarantine_no_delete
BEFORE DELETE ON polyus_parlay_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS parlay settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_kf_settlement_quarantine_no_update
BEFORE UPDATE ON polyus_kf_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS JSON-book settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_kf_settlement_quarantine_no_delete
BEFORE DELETE ON polyus_kf_settlement_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS JSON-book settlement quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_research_grade_quarantine_no_update
BEFORE UPDATE ON polyus_research_grade_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS research-grade quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_research_grade_quarantine_no_delete
BEFORE DELETE ON polyus_research_grade_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS research-grade quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_funded_json_epoch_quarantine_no_update
BEFORE UPDATE ON polyus_funded_json_epoch_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS funded JSON epoch quarantine'); END;
CREATE TRIGGER IF NOT EXISTS polyus_funded_json_epoch_quarantine_no_delete
BEFORE DELETE ON polyus_funded_json_epoch_quarantine
BEGIN SELECT RAISE(ABORT,'immutable PolyUS funded JSON epoch quarantine'); END;`

const (
	polyUSUntrustedSettlementReason = "R148 legacy/fractional PolyUS settlement lacks versioned final-source authority"
	polyUSJSONCleanEpochReason      = "R148 bounded JSON history cannot identify every legacy PolyUS settlement; original archived and unverifiable aggregate authority excluded"
	// The ML paper-file repair is intentionally narrower: unlike the SQL ledgers it can identify
	// only lots whose stored payout itself is fractional.
	polyUSFractionalSettlementReason = "R148 preliminary/fractional settlementPx is not final binary EVENT_TIER_1 truth"
)

func quarantinePolyUSFundedJSONEpoch(ctx context.Context, db *sql.DB, ledger string, raw []byte) (string, bool, error) {
	ledger = strings.TrimSpace(ledger)
	if ledger == "" || !json.Valid(raw) {
		return "", false, errors.New("invalid PolyUS funded JSON clean-epoch archive")
	}
	h := sha256.Sum256(raw)
	hash := hex.EncodeToString(h[:])
	res, err := db.ExecContext(ctx, `INSERT OR IGNORE INTO polyus_funded_json_epoch_quarantine(
ledger_name,source_sha256,source_json,reason,quarantined_at) VALUES(?,?,?,?,?)`, ledger, hash,
		string(raw), polyUSJSONCleanEpochReason, nowRFC())
	if err != nil {
		return "", false, err
	}
	n, err := res.RowsAffected()
	return hash, n == 1, err
}

func (s *Store) QuarantinePolyUSFundedJSONEpoch(ctx context.Context, ledger string, raw []byte) (string, bool, error) {
	return quarantinePolyUSFundedJSONEpoch(ctx, s.db, ledger, raw)
}

// PolyUSSettlementRepairState marks a ticker whose older terminal projection was derived from an
// untrusted receipt. A later trusted v2 receipt forms a cutoff: closures at/after it are genuine
// and must not be reopened by future restart replays.
type PolyUSSettlementRepairState struct {
	TrustedResolvedAt time.Time
	HasTrustedFinal   bool
}

func (s PolyUSSettlementRepairState) ClosureNeedsRepair(closedAt time.Time) bool {
	return !s.HasTrustedFinal || closedAt.IsZero() || closedAt.Before(s.TrustedResolvedAt)
}

func polyUSSettlementRepairStates(ctx context.Context, db *sql.DB) (map[string]PolyUSSettlementRepairState, error) {
	rows, err := db.QueryContext(ctx, `SELECT q.ticker,COALESCE(v.resolved_at,''),COALESCE(v.source_artifact,'')
FROM (SELECT DISTINCT ticker FROM polyus_settlement_quarantine) q
LEFT JOIN venue_settlements v ON v.platform='polyus' AND v.ticker=q.ticker
ORDER BY q.ticker`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]PolyUSSettlementRepairState{}
	for rows.Next() {
		var ticker, resolved, source string
		if err := rows.Scan(&ticker, &resolved, &source); err != nil {
			return nil, err
		}
		state := PolyUSSettlementRepairState{}
		if TrustedPolyUSFinalSettlementSource(source) {
			when, parseErr := time.Parse(time.RFC3339Nano, resolved)
			if parseErr != nil {
				when, parseErr = time.Parse(time.RFC3339, resolved)
			}
			if parseErr == nil {
				state.HasTrustedFinal, state.TrustedResolvedAt = true, when.UTC()
			}
		}
		out[ticker] = state
	}
	return out, rows.Err()
}

func (s *Store) PolyUSSettlementRepairStates(ctx context.Context) (map[string]PolyUSSettlementRepairState, error) {
	return polyUSSettlementRepairStates(ctx, s.db)
}

func (s *Store) QuarantinePolyUSKFSettlement(ctx context.Context, book, sub, ticker, side,
	closedTS, rowJSON string) (bool, error) {
	book, sub, ticker = strings.TrimSpace(book), strings.TrimSpace(sub), strings.TrimSpace(ticker)
	side, closedTS, rowJSON = strings.ToUpper(strings.TrimSpace(side)), strings.TrimSpace(closedTS), strings.TrimSpace(rowJSON)
	if book == "" || ticker == "" || (side != "YES" && side != "NO") || closedTS == "" ||
		!json.Valid([]byte(rowJSON)) {
		return false, errors.New("invalid PolyUS JSON-book settlement quarantine row")
	}
	h := sha256.Sum256([]byte(strings.Join([]string{book, sub, ticker, side, closedTS, rowJSON}, "\x00")))
	id := hex.EncodeToString(h[:])
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO polyus_kf_settlement_quarantine(
quarantine_id,book_name,sub_name,ticker,side,closed_ts,row_json,reason,quarantined_at)
VALUES(?,?,?,?,?,?,?,?,?)`, id, book, sub, ticker, side, closedTS, rowJSON,
		polyUSUntrustedSettlementReason, nowRFC())
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

type r148ResearchGradeRepairCounts struct {
	ProperScore, PayoffUpdates, RouteEvents, BundleEvents int64
}

// quarantineR148PolyUSResearchGrades removes every terminal research projection that inherited
// an untrusted PolyUS result. The immutable observation/opportunity rows remain untouched; only
// the derived grade is reopened so a later versioned final receipt can append the honest result.
// Cross-venue rows are included when their typed leg or frozen evidence names an affected ticker.
func quarantineR148PolyUSResearchGrades(tx *sql.Tx, stamp string) (r148ResearchGradeRepairCounts, error) {
	var counts r148ResearchGradeRepairCounts
	proper, err := tx.Exec(`INSERT OR IGNORE INTO polyus_research_grade_quarantine(
ledger,ledger_row_id,row_json,reason,quarantined_at)
SELECT 'proper_score_trials',p.id,json_object(
 'id',p.id,'platform',p.platform,'ticker',p.ticker,'transform',p.transform,
 'strategy_mode',p.strategy_mode,'selected_side',p.selected_side,'settled',p.settled,
 'grade_status',p.grade_status,'grade_reason',p.grade_reason,'settle_yes',p.settle_yes,
 'forecast_score',p.forecast_score,'book_score',p.book_score,'score_delta',p.score_delta,
 'realized_net',p.realized_net,'closed_ts',p.closed_ts,
 'research_observation_id',p.research_observation_id
),?,?
FROM research_proper_score_trials p
JOIN r148_polyus_untrusted_settlements bad ON p.ticker=bad.ticker
WHERE p.platform='polyus' AND p.settled!=0`, polyUSUntrustedSettlementReason, stamp)
	if err != nil {
		return counts, err
	}
	counts.ProperScore, err = proper.RowsAffected()
	if err != nil {
		return counts, err
	}
	if counts.ProperScore > 0 {
		if _, err := tx.Exec(`UPDATE research_proper_score_trials
SET settled=0,grade_status='open',grade_reason='',settle_yes=NULL,forecast_score=NULL,
book_score=NULL,score_delta=NULL,realized_net=NULL,closed_ts=''
WHERE platform='polyus' AND settled!=0
 AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`); err != nil {
			return counts, err
		}
	}

	payoffs, err := tx.Exec(`INSERT OR IGNORE INTO polyus_research_grade_quarantine(
ledger,ledger_row_id,row_json,reason,quarantined_at)
SELECT 'system_payoff_updates',u.id,json_object(
 'id',u.id,'observation_id',u.observation_id,'observed_ts',u.observed_ts,'status',u.status,
 'payout_lower',u.payout_lower,'payout_upper',u.payout_upper,'realized_net',u.realized_net,
 'source_artifact',u.source_artifact,'source_hash',u.source_hash,'reason',u.reason,
 'funded',u.funded,'paper_authority',u.paper_authority,'live_authority',u.live_authority
),?,?
FROM research_system_payoff_updates u
JOIN research_system_observations o ON o.id=u.observation_id
WHERE (o.venue='polyus' AND o.ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements))
 OR EXISTS (
   SELECT 1 FROM json_tree(o.inputs_json) j
   JOIN r148_polyus_untrusted_settlements bad ON CAST(j.value AS TEXT)=bad.ticker
 )`, polyUSUntrustedSettlementReason, stamp)
	if err != nil {
		return counts, err
	}
	counts.PayoffUpdates, err = payoffs.RowsAffected()
	if err != nil {
		return counts, err
	}
	if counts.PayoffUpdates > 0 {
		if _, err := tx.Exec(`DROP TRIGGER IF EXISTS research_system_payoff_updates_no_delete`); err != nil {
			return counts, err
		}
		if _, err := tx.Exec(`DELETE FROM research_system_payoff_updates
WHERE id IN (SELECT ledger_row_id FROM polyus_research_grade_quarantine
 WHERE ledger='system_payoff_updates' AND reason=? AND quarantined_at=?)`,
			polyUSUntrustedSettlementReason, stamp); err != nil {
			return counts, err
		}
		if _, err := tx.Exec(`CREATE TRIGGER research_system_payoff_updates_no_delete
BEFORE DELETE ON research_system_payoff_updates
BEGIN SELECT RAISE(ABORT,'append-only payoff update'); END`); err != nil {
			return counts, err
		}
		// Deleting a false first terminal receipt reopens the economically complete observation.
		// Recompute this tiny materialized counter from source truth; no observation is mutated.
		if _, err := tx.Exec(`UPDATE research_system_collection_totals SET open_economic=(
 SELECT COUNT(*) FROM research_system_observations o
 WHERE o.system_id=research_system_collection_totals.system_id AND o.outcome_status='open'
  AND o.route!='observer' AND TRIM(o.source_clock_id)!='' AND TRIM(o.book_source)!=''
  AND TRIM(o.fee_source)!='' AND o.size_units>0 AND o.executable_cost>0
  AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1
  AND o.depth_known=1 AND o.fee_known=1
  AND NOT EXISTS(SELECT 1 FROM research_system_payoff_updates u WHERE u.observation_id=o.id)
)`); err != nil {
			return counts, err
		}
	}

	routes, err := tx.Exec(`INSERT OR IGNORE INTO polyus_research_grade_quarantine(
ledger,ledger_row_id,row_json,reason,quarantined_at)
SELECT 'route_events',e.id,json_object(
 'id',e.id,'opportunity_id',e.opportunity_id,'route_id',e.route_id,'observed_ts',e.observed_ts,
 'event_type',e.event_type,'quantity',e.quantity,'price',e.price,'fee_amount',e.fee_amount,
 'rebate_amount',e.rebate_amount,'queue_ahead',e.queue_ahead,'order_ref_hash',e.order_ref_hash,
 'payout',e.payout,'realized_net',e.realized_net,'capital_seconds',e.capital_seconds,
 'markout',e.markout,'outcome_status',e.outcome_status,'reason',e.reason,
 'evidence_json',json(e.evidence_json),'funded',e.funded,
 'paper_authority',e.paper_authority,'live_authority',e.live_authority
),?,?
FROM research_route_events e
JOIN research_route_opportunities o
 ON o.opportunity_id=e.opportunity_id AND o.route_id=e.route_id
WHERE e.event_type='grade' AND (
 (o.venue='polyus' AND o.ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements))
 OR EXISTS (
   SELECT 1 FROM json_tree(o.evidence_json) j
   JOIN r148_polyus_untrusted_settlements bad ON CAST(j.value AS TEXT)=bad.ticker
 )
)`, polyUSUntrustedSettlementReason, stamp)
	if err != nil {
		return counts, err
	}
	counts.RouteEvents, err = routes.RowsAffected()
	if err != nil {
		return counts, err
	}
	if counts.RouteEvents > 0 {
		if _, err := tx.Exec(`DROP TRIGGER IF EXISTS research_route_events_no_delete`); err != nil {
			return counts, err
		}
		if _, err := tx.Exec(`DELETE FROM research_route_events
WHERE id IN (SELECT ledger_row_id FROM polyus_research_grade_quarantine
 WHERE ledger='route_events' AND reason=? AND quarantined_at=?)`,
			polyUSUntrustedSettlementReason, stamp); err != nil {
			return counts, err
		}
		if _, err := tx.Exec(`CREATE TRIGGER research_route_events_no_delete
BEFORE DELETE ON research_route_events
BEGIN SELECT RAISE(ABORT,'append-only research route event'); END`); err != nil {
			return counts, err
		}
	}

	bundles, err := tx.Exec(`INSERT OR IGNORE INTO polyus_research_grade_quarantine(
ledger,ledger_row_id,row_json,reason,quarantined_at)
SELECT 'bundle_events',e.id,json_object(
 'id',e.id,'bundle_id',e.bundle_id,'observed_ts',e.observed_ts,'event_type',e.event_type,
 'outcome_status',e.outcome_status,'payout',e.payout,'realized_net',e.realized_net,
 'capital_seconds',e.capital_seconds,'actual_fill',e.actual_fill,'atomic_fill',e.atomic_fill,
 'reason',e.reason,'evidence_json',json(e.evidence_json),'funded',e.funded,
 'paper_authority',e.paper_authority,'live_authority',e.live_authority
),?,?
FROM research_route_bundle_events e
WHERE e.event_type='grade' AND EXISTS (
 SELECT 1 FROM research_route_bundle_legs l
 JOIN r148_polyus_untrusted_settlements bad ON l.venue='polyus' AND l.ticker=bad.ticker
 WHERE l.bundle_id=e.bundle_id
)`, polyUSUntrustedSettlementReason, stamp)
	if err != nil {
		return counts, err
	}
	counts.BundleEvents, err = bundles.RowsAffected()
	if err != nil {
		return counts, err
	}
	if counts.BundleEvents > 0 {
		if _, err := tx.Exec(`DROP TRIGGER IF EXISTS research_route_bundle_events_no_delete`); err != nil {
			return counts, err
		}
		if _, err := tx.Exec(`DELETE FROM research_route_bundle_events
WHERE id IN (SELECT ledger_row_id FROM polyus_research_grade_quarantine
 WHERE ledger='bundle_events' AND reason=? AND quarantined_at=?)`,
			polyUSUntrustedSettlementReason, stamp); err != nil {
			return counts, err
		}
		if _, err := tx.Exec(`CREATE TRIGGER research_route_bundle_events_no_delete
BEFORE DELETE ON research_route_bundle_events
BEGIN SELECT RAISE(ABORT,'append-only research route bundle event'); END`); err != nil {
			return counts, err
		}
	}
	return counts, nil
}

func migratePolyUSFinalSettlementAuthority(db *sql.DB) error {
	if _, err := db.Exec(polyUSSettlementQuarantineDDL); err != nil {
		return err
	}
	// Relation outcomes power the related/independent P&L report. They must be removed alongside
	// their false grades or a corrected settlement can neither replace them nor repair the report.
	if _, err := db.Exec(polyUSMLSettlementQuarantineDDL); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stamp := nowRFC()
	// Snapshot the affected tickers before deleting their receipts. This makes every following
	// quarantine/reset statement use the exact same set and lets us reset all derived grades even
	// when an old preliminary mark happened to be exactly zero or one.
	if _, err := tx.Exec(`DROP TABLE IF EXISTS temp.r148_polyus_untrusted_settlements`); err != nil {
		return err
	}
	if _, err := tx.Exec(`CREATE TEMP TABLE r148_polyus_untrusted_settlements(
ticker TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO r148_polyus_untrusted_settlements(ticker)
SELECT ticker FROM venue_settlements
WHERE platform='polyus' AND (
 yes_value NOT IN (0.0,1.0) OR source_artifact NOT IN (?,?)
)`, PolyUSFinalBookSettlementV2, PolyUSFinalEndpointSettlementV2); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO polyus_settlement_quarantine(
platform,ticker,yes_value,resolved_at,source_artifact,reason,quarantined_at)
SELECT platform,ticker,yes_value,resolved_at,source_artifact,?,?
FROM venue_settlements WHERE platform='polyus' AND ticker IN (
 SELECT ticker FROM r148_polyus_untrusted_settlements
)`, polyUSUntrustedSettlementReason, stamp); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO polyus_settlement_grade_quarantine(
ledger,ledger_row_id,platform,ticker,side,source,settle_value,outcome,resolved_at,reason,quarantined_at)
SELECT 'signal_log',id,platform,ticker,side,signal_type,settle_val,won,resolved_at,?,?
FROM signal_log WHERE platform='polyus' AND resolved=1 AND settle_val IS NOT NULL
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`,
		polyUSUntrustedSettlementReason, stamp); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO polyus_settlement_grade_quarantine(
ledger,ledger_row_id,platform,ticker,side,source,settle_value,outcome,resolved_at,reason,quarantined_at)
SELECT 'maker_fill_stats',id,platform,ticker,side,source,settle_val,outcome_won,settle_mirrored_at,?,?
FROM maker_fill_stats WHERE platform='polyus' AND settle_val IS NOT NULL
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`,
		polyUSUntrustedSettlementReason, stamp); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO polyus_settlement_grade_quarantine(
ledger,ledger_row_id,platform,ticker,side,source,settle_value,resolved_at,
pnl_pc,return_per_dollar,capital_day,reason,quarantined_at)
SELECT 'unit_trials',id,platform,ticker,side,family,settle_val,closed_ts,
pnl_pc,return_per_dollar,capital_day,?,?
FROM unit_trials WHERE platform='polyus' AND settled=1 AND settle_val IS NOT NULL
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`,
		polyUSUntrustedSettlementReason, stamp); err != nil {
		return err
	}
	paperResult, err := tx.Exec(`INSERT OR IGNORE INTO polyus_paper_fill_settlement_quarantine(
fill_id,platform,ticker,row_json,reason,quarantined_at)
SELECT id,platform,ticker,json_object(
 'id',id,'ts',ts,'platform',platform,'ticker',ticker,'title',title,'side',side,
 'action',action,'price',price,'contracts',contracts,'fee',fee,'source',COALESCE(source,''),
 'note',COALESCE(note,''),'fill_kind',COALESCE(fill_kind,''),'route_reason',COALESCE(route_reason,'')
),?,?
FROM paper_fills
WHERE platform='polyus' AND UPPER(action)='SELL' AND source='settled-polyus'
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`,
		polyUSUntrustedSettlementReason, stamp)
	if err != nil {
		return err
	}
	paperN, err := paperResult.RowsAffected()
	if err != nil {
		return err
	}
	parlayResult, err := tx.Exec(`INSERT OR IGNORE INTO polyus_parlay_settlement_quarantine(
parlay_id,row_json,reason,quarantined_at)
SELECT p.id,json_object(
 'id',p.id,'ts',p.ts,'stake',p.stake,'price',p.price,'contracts',p.contracts,
 'tp',p.tp,'sl',p.sl,'status',p.status,'legs',json(CASE WHEN json_valid(p.legs) THEN p.legs ELSE '[]' END),'payout',p.payout,
 'realized',p.realized,'fees',p.fees,'settled_ts',p.settled_ts,'artifact',p.artifact,
 'route_source',p.route_source,'cohort',p.cohort,'system_ids',json(CASE WHEN json_valid(p.system_ids) THEN p.system_ids ELSE '[]' END),
 'joint_p',p.joint_p,'expected_net_per_dollar',p.expected_net_per_dollar
),?,?
FROM paper_parlays p
WHERE p.status='settled' AND EXISTS (
 SELECT 1 FROM json_each(CASE WHEN json_valid(p.legs) THEN p.legs ELSE '[]' END) leg
 JOIN r148_polyus_untrusted_settlements bad
   ON LOWER(json_extract(leg.value,'$.platform'))='polyus'
  AND json_extract(leg.value,'$.ticker')=bad.ticker
)`, polyUSUntrustedSettlementReason, stamp)
	if err != nil {
		return err
	}
	parlayN, err := parlayResult.RowsAffected()
	if err != nil {
		return err
	}
	relationResult, err := tx.Exec(`INSERT OR IGNORE INTO polyus_ml_relation_outcome_quarantine(
receipt_id,outcome_id,outcome_status,settled_ts,pnl_dollars,result_source,created_ts,reason,quarantined_at)
SELECT o.receipt_id,o.outcome_id,o.outcome_status,o.settled_ts,o.pnl_dollars,o.result_source,
o.created_ts,?,?
FROM funded_relation_outcomes o
WHERE o.outcome_status='settled' AND o.receipt_id IN (
 SELECT r.receipt_id FROM funded_relation_receipts r
 JOIN r148_polyus_untrusted_settlements bad
   ON r.candidate_venue='polyus' AND r.candidate_ticker=bad.ticker
 UNION
 SELECT l.receipt_id FROM funded_relation_receipt_legs l
 JOIN r148_polyus_untrusted_settlements bad
   ON l.venue='polyus' AND l.ticker=bad.ticker
)`, polyUSUntrustedSettlementReason, stamp)
	if err != nil {
		return err
	}
	relationN, err := relationResult.RowsAffected()
	if err != nil {
		return err
	}
	researchN, err := quarantineR148PolyUSResearchGrades(tx, stamp)
	if err != nil {
		return fmt.Errorf("quarantine derived PolyUS research grades: %w", err)
	}
	if relationN > 0 {
		if _, err := tx.Exec(`DROP TRIGGER IF EXISTS funded_relation_outcomes_no_delete`); err != nil {
			return err
		}
		// Delete only outcomes copied by this exact migration turn. If a corrected outcome is
		// inserted later, replay sees the existing immutable quarantine row and cannot delete it.
		if _, err := tx.Exec(`DELETE FROM funded_relation_outcomes
WHERE EXISTS (
 SELECT 1 FROM polyus_ml_relation_outcome_quarantine q
 WHERE q.receipt_id=funded_relation_outcomes.receipt_id
   AND q.outcome_id=funded_relation_outcomes.outcome_id
   AND q.reason=? AND q.quarantined_at=?
)`, polyUSUntrustedSettlementReason, stamp); err != nil {
			return err
		}
		if _, err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS funded_relation_outcomes_no_delete
BEFORE DELETE ON funded_relation_outcomes
BEGIN SELECT RAISE(ABORT,'immutable funded relation outcome'); END`); err != nil {
			return err
		}
	}
	if paperN > 0 {
		if _, err := tx.Exec(`DELETE FROM paper_fills
WHERE id IN (
 SELECT fill_id FROM polyus_paper_fill_settlement_quarantine
 WHERE reason=? AND quarantined_at=?
)`, polyUSUntrustedSettlementReason, stamp); err != nil {
			return err
		}
	}
	if parlayN > 0 {
		if _, err := tx.Exec(`UPDATE paper_parlays SET status='open',payout=0,realized=0,settled_ts=''
WHERE id IN (
 SELECT parlay_id FROM polyus_parlay_settlement_quarantine
 WHERE reason=? AND quarantined_at=?
)`, polyUSUntrustedSettlementReason, stamp); err != nil {
			return err
		}
	}
	var receiptN, signalN, makerN, unitN int
	_ = tx.QueryRow(`SELECT COUNT(*) FROM venue_settlements
WHERE platform='polyus' AND ticker IN (
 SELECT ticker FROM r148_polyus_untrusted_settlements
)`).Scan(&receiptN)
	_ = tx.QueryRow(`SELECT COUNT(*) FROM signal_log
WHERE platform='polyus' AND resolved=1
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`).Scan(&signalN)
	_ = tx.QueryRow(`SELECT COUNT(*) FROM maker_fill_stats
WHERE platform='polyus' AND settle_val IS NOT NULL
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`).Scan(&makerN)
	_ = tx.QueryRow(`SELECT COUNT(*) FROM unit_trials
WHERE platform='polyus' AND settled=1
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`).Scan(&unitN)
	if _, err := tx.Exec(`UPDATE signal_log SET resolved=0,won=NULL,settle_val=-1,resolved_at=NULL
WHERE platform='polyus' AND resolved=1
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE maker_fill_stats
SET settle_val=NULL,outcome_won=NULL,settle_mirrored_at=''
WHERE platform='polyus' AND settle_val IS NOT NULL
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE unit_trials SET settled=0,closed_ts='',settle_val=NULL,
pnl_pc=NULL,return_per_dollar=NULL,capital_day=NULL
WHERE platform='polyus' AND settled=1
AND ticker IN (SELECT ticker FROM r148_polyus_untrusted_settlements)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM venue_settlements
WHERE platform='polyus' AND ticker IN (
 SELECT ticker FROM r148_polyus_untrusted_settlements
)`); err != nil {
		return err
	}
	if receiptN+signalN+makerN+unitN+int(relationN)+int(paperN)+int(parlayN)+
		int(researchN.ProperScore+researchN.PayoffUpdates+researchN.RouteEvents+researchN.BundleEvents) > 0 {
		if _, err := tx.Exec(`INSERT INTO audit_log(ts,level,category,message,detail)
VALUES(?,?,?,?,?)`, stamp, "warn", "settlement",
			"R148 quarantined legacy/non-final PolyUS settlements",
			fmt.Sprintf("receipts=%d signal_grades=%d maker_grades=%d unit_grades=%d paper_settlement_fills=%d parlays=%d relation_outcomes=%d proper_score=%d research_payoffs=%d route_grades=%d bundle_grades=%d; rows reset for authoritative re-grade",
				receiptN, signalN, makerN, unitN, paperN, parlayN, relationN,
				researchN.ProperScore, researchN.PayoffUpdates, researchN.RouteEvents, researchN.BundleEvents)); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DROP TABLE temp.r148_polyus_untrusted_settlements`); err != nil {
		return err
	}
	return tx.Commit()
}
