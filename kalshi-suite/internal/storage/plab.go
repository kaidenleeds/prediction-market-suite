// plab.go — R128: the UNLIMITED parlay-lab open ledger (operator order: "unlimited cap... we
// need all the billions... every possible acceptable combo").
//
// Pre-R128 the open ledger was an in-memory map + one kv row per combo (plabopen: prefix) with
// an 8,000-entry turnover cap and evict-oldest; grading scanned EVERY open combo each pass and
// resolved legs per-COMBO under a 15-fetch budget — the "graded 50 of 12k/hr" death.
//
// R128 shape:
//   - plab_open: one compact row per logged combo (WITHOUT ROWID, TEXT PK = the 16-hex combo id).
//     `unresolved` counts legs still awaiting settlement — combos WAIT here until every leg
//     settles; the only admission screen is the fee-net +EV acceptability floor upstream.
//     NO cap, NO eviction. Valid unresolved rows are never removed merely for age, a changed
//     horizon, postponement or a later leg-limit policy; authoritative late outcomes still grade.
//   - plab_leg_open: (platform, ticker) → combo_id index rows. The settle-sweep is TICKER-centric:
//     resolve each distinct unresolved ticker ONCE, fan the value out to every combo holding it,
//     and grade only combos whose unresolved count hits 0 — O(new settlements), not O(open).
//   - Batch inserts: one transaction per enumeration cycle (InsertBackfillTrades pattern).
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	ComboLabCohortAllEligible      = "all-eligible"
	ComboLabCohortRollingPositive  = "rolling-positive-system"
	ComboLabCohortPromotedSystem   = "promoted-system-combo"
	FundedPositiveSystemComboRoute = "positive-system-paper-v1"
)

var comboLabSchemaReady sync.Map

// ensureComboLabCohortSchema upgrades the pre-R140 open ledger in place. The active suite may
// already own a large immutable history, so the migration adds honest defaults instead of
// rebuilding the table.
func (s *Store) ensureComboLabCohortSchema(ctx context.Context) error {
	if _, ok := comboLabSchemaReady.Load(s); ok {
		return nil
	}
	for _, ddl := range []string{
		`ALTER TABLE plab_open ADD COLUMN cohort TEXT NOT NULL DEFAULT 'all-eligible'`,
		`ALTER TABLE plab_open ADD COLUMN route_state TEXT NOT NULL DEFAULT 'synthetic-settlement-only'`,
		`ALTER TABLE plab_open ADD COLUMN canonical_system_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_open ADD COLUMN combo_venue TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_open ADD COLUMN relation_class TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_open ADD COLUMN producer_family TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_open ADD COLUMN combo_route TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_open ADD COLUMN experiment_epoch TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_open ADD COLUMN combo_key TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE IF NOT EXISTS plab_grade_receipts (
combo_id TEXT PRIMARY KEY,candidate_at INTEGER NOT NULL,graded_ts TEXT NOT NULL,cell TEXT NOT NULL,
bucket TEXT NOT NULL,class TEXT NOT NULL,legality TEXT NOT NULL,cohort TEXT NOT NULL,route_state TEXT NOT NULL,
canonical_system_id TEXT NOT NULL DEFAULT '',combo_venue TEXT NOT NULL DEFAULT '',
relation_class TEXT NOT NULL DEFAULT '',producer_family TEXT NOT NULL DEFAULT '',combo_route TEXT NOT NULL DEFAULT '',
experiment_epoch TEXT NOT NULL DEFAULT '',combo_key TEXT NOT NULL DEFAULT '',
nlegs INTEGER NOT NULL CHECK(nlegs>=2),prod REAL NOT NULL CHECK(prod>0 AND prod<1),
joint_p REAL NOT NULL CHECK(joint_p>0 AND joint_p<1),fees REAL NOT NULL CHECK(fees>=0),
entry_capital REAL NOT NULL CHECK(entry_capital>=1),joint_payout REAL NOT NULL CHECK(joint_payout>=0 AND joint_payout<=1),
realized_return REAL NOT NULL CHECK(realized_return>=-1),predicted_return REAL NOT NULL CHECK(predicted_return>-1),
mve_valid INTEGER NOT NULL DEFAULT 0,realized_mve_return REAL NOT NULL DEFAULT 0 CHECK(realized_mve_return>=-1),won INTEGER NOT NULL DEFAULT 0,
independence_keys_json TEXT NOT NULL,market_keys_json TEXT NOT NULL DEFAULT '[]',legs_json TEXT NOT NULL,payouts_json TEXT NOT NULL,
economics_version TEXT NOT NULL CHECK(economics_version='all-in-v2')) WITHOUT ROWID`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN market_keys_json TEXT NOT NULL DEFAULT '[]'`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN canonical_system_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN combo_venue TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN relation_class TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN producer_family TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN combo_route TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN experiment_epoch TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE plab_grade_receipts ADD COLUMN combo_key TEXT NOT NULL DEFAULT ''`,
		`CREATE INDEX IF NOT EXISTS idx_plab_grade_cell ON plab_grade_receipts(cell,graded_ts)`,
	} {
		if _, err := s.db.ExecContext(ctx, ddl); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	// Backfill only identities inherent in the immutable row. Contributor names are intentionally
	// not promoted to ownership: one parlay-Nleg product owns each economic sample.
	for _, q := range []string{
		`UPDATE plab_open SET
canonical_system_id='parlay-'||nlegs||'leg',
combo_venue=CASE WHEN json_valid(legs) AND json_array_length(legs)=nlegs
 AND NOT EXISTS(SELECT 1 FROM json_each(legs) j
   WHERE LOWER(TRIM(json_extract(j.value,'$.platform')))<>LOWER(TRIM(json_extract(legs,'$[0].platform'))))
 THEN LOWER(TRIM(json_extract(legs,'$[0].platform'))) ELSE '' END,
relation_class=CASE bucket WHEN 'indep' THEN 'independent' WHEN 'overlap' THEN 'related' ELSE 'unknown' END,
producer_family=CASE cohort WHEN 'rolling-positive-system' THEN 'positive-system'
 WHEN 'promoted-system-combo' THEN 'rfq' ELSE 'generic' END,
combo_route=CASE WHEN cohort='promoted-system-combo' OR route_state='awaiting-rfq-identical-proof'
 THEN 'rfq-research' ELSE 'synthetic-taker-chain' END,
experiment_epoch='legacy-all-in-v2',combo_key=id
WHERE nlegs BETWEEN 2 AND 6 AND canonical_system_id='' AND json_valid(legs)`,
		`UPDATE plab_grade_receipts SET
canonical_system_id='parlay-'||nlegs||'leg',
combo_venue=CASE WHEN json_valid(legs_json) AND json_array_length(legs_json)=nlegs
 AND NOT EXISTS(SELECT 1 FROM json_each(legs_json) j
   WHERE LOWER(TRIM(json_extract(j.value,'$.platform')))<>LOWER(TRIM(json_extract(legs_json,'$[0].platform'))))
 THEN LOWER(TRIM(json_extract(legs_json,'$[0].platform'))) ELSE '' END,
relation_class=CASE bucket WHEN 'indep' THEN 'independent' WHEN 'overlap' THEN 'related' ELSE 'unknown' END,
producer_family=CASE cohort WHEN 'rolling-positive-system' THEN 'positive-system'
 WHEN 'promoted-system-combo' THEN 'rfq' ELSE 'generic' END,
combo_route=CASE WHEN cohort='promoted-system-combo' OR route_state='awaiting-rfq-identical-proof'
 THEN 'rfq-research' ELSE 'synthetic-taker-chain' END,
experiment_epoch='legacy-all-in-v2',combo_key=combo_id
WHERE nlegs BETWEEN 2 AND 6 AND canonical_system_id='' AND json_valid(legs_json)`,
	} {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	comboLabSchemaReady.Store(s, struct{}{})
	return nil
}

// PlabLegKey identifies one leg's market.
type PlabLegKey struct {
	Platform string
	Ticker   string
	// FirstAt is the oldest prospective candidate still waiting on this leg. It lets the
	// settlement worker prioritize legs whose maximum admitted horizon has elapsed without
	// making extra catalog/API calls merely to rank the queue.
	FirstAt int64
}

// PlabCand is one open-ledger row (compact: legs ride as one JSON blob, settlement state lives
// in plab_leg_open).
type PlabCand struct {
	ID                                                           string
	At                                                           int64 // unix seconds (UTC)
	Bucket                                                       string
	Class                                                        string
	Legality                                                     string
	CorrCluster                                                  bool
	Prod                                                         float64
	JointP                                                       float64
	Rho                                                          float64
	RhoN                                                         int
	FeeSyn                                                       float64
	FeeMVE                                                       float64
	EVSyn                                                        float64
	EVMVE                                                        float64
	LegFees                                                      string // JSON []float64 — per-leg fee chain for realized grading
	Legs                                                         string // JSON []plabLeg (server-side type)
	NLegs                                                        int
	Cohort                                                       string // all-eligible | rolling-positive-system | promoted-system-combo
	RouteState                                                   string // synthetic-settlement-only | paper-only-rolling-positive | awaiting-rfq-identical-proof
	CanonicalSystemID, ComboVenue, RelationClass, ProducerFamily string
	ComboRoute, ExperimentEpoch, ComboKey                        string
	LegKeys                                                      []PlabLegKey
}

// PlabGradeReceipt is the immutable, idempotent current-economics truth for one terminal Combo
// Lab sample. Aggregate KV blobs are caches only; this row survives a crash between grading and
// deleting the open candidate and therefore prevents a second economic count on retry.
type PlabGradeReceipt struct {
	ComboID, GradedTS, Cell, Bucket, Class, Legality, Cohort, RouteState string
	CanonicalSystemID, ComboVenue, RelationClass, ProducerFamily         string
	ComboRoute, ExperimentEpoch, ComboKey                                string
	CandidateAt, NLegs                                                   int64
	Prod, JointP, Fees, EntryCapital, JointPayout                        float64
	RealizedReturn, PredictedReturn, RealizedMVEReturn                   float64
	MVEValid, Won                                                        bool
	IndependenceKeys                                                     []string
	// MarketKeys are venue+ticker identities. They deliberately remain separate from
	// IndependenceKeys, which may collapse several instruments onto one event/resolution block.
	MarketKeys            []string
	LegsJSON, PayoutsJSON string
}

func finitePlab(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// InsertPlabGradeReceipt inserts exactly once per combo id. false,nil means this exact candidate
// was already economically graded and the caller may safely remove the leftover open row without
// adding it to n again.
func (s *Store) InsertPlabGradeReceipt(ctx context.Context, r PlabGradeReceipt) (bool, error) {
	if err := s.ensureComboLabCohortSchema(ctx); err != nil {
		return false, err
	}
	r.ComboID, r.Cell = strings.TrimSpace(r.ComboID), strings.TrimSpace(r.Cell)
	r.Bucket, r.Class, r.Legality = strings.TrimSpace(r.Bucket), strings.TrimSpace(r.Class), strings.TrimSpace(r.Legality)
	r.Cohort, r.RouteState = strings.TrimSpace(r.Cohort), strings.TrimSpace(r.RouteState)
	if r.GradedTS == "" {
		r.GradedTS = time.Now().UTC().Format(time.RFC3339Nano)
	}
	if r.ComboID == "" || r.Cell == "" || r.Bucket == "" || r.Class == "" || r.Legality == "" ||
		r.RouteState == "" || (r.Cohort != ComboLabCohortAllEligible && r.Cohort != ComboLabCohortRollingPositive &&
		r.Cohort != ComboLabCohortPromotedSystem) || r.NLegs < 2 || r.NLegs > 64 ||
		r.Prod <= 0 || r.Prod >= 1 || r.JointP <= 0 || r.JointP >= 1 || r.Fees < 0 ||
		r.EntryCapital < 1 || math.Abs(r.EntryCapital-(1+r.Fees)) > 1e-8*math.Max(1, r.EntryCapital) ||
		r.JointPayout < 0 || r.JointPayout > 1 || r.RealizedReturn < -1-1e-9 ||
		r.PredictedReturn <= -1 || (r.MVEValid && r.RealizedMVEReturn < -1-1e-9) ||
		!finitePlab(r.Prod) || !finitePlab(r.JointP) || !finitePlab(r.Fees) ||
		!finitePlab(r.EntryCapital) || !finitePlab(r.JointPayout) || !finitePlab(r.RealizedReturn) ||
		!finitePlab(r.PredictedReturn) || !finitePlab(r.RealizedMVEReturn) ||
		len(r.IndependenceKeys) == 0 || len(r.MarketKeys) == 0 ||
		!json.Valid([]byte(r.LegsJSON)) || !json.Valid([]byte(r.PayoutsJSON)) {
		return false, fmt.Errorf("invalid Combo Lab grade receipt")
	}
	var legs []json.RawMessage
	var payouts []float64
	if json.Unmarshal([]byte(r.LegsJSON), &legs) != nil || json.Unmarshal([]byte(r.PayoutsJSON), &payouts) != nil ||
		len(legs) != int(r.NLegs) || len(payouts) != int(r.NLegs) {
		return false, fmt.Errorf("invalid Combo Lab grade receipt leg/payout arrays")
	}
	for _, payout := range payouts {
		if !finitePlab(payout) || payout < 0 || payout > 1 {
			return false, fmt.Errorf("invalid Combo Lab grade receipt payout")
		}
	}
	keys := make([]string, 0, len(r.IndependenceKeys))
	for _, raw := range r.IndependenceKeys {
		key := strings.TrimSpace(raw)
		if key == "" {
			return false, fmt.Errorf("empty Combo Lab independence key")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	unique := keys[:0]
	for _, key := range keys {
		if len(unique) == 0 || unique[len(unique)-1] != key {
			unique = append(unique, key)
		}
	}
	keysJSON, _ := json.Marshal(unique)
	marketKeys := make([]string, 0, len(r.MarketKeys))
	for _, raw := range r.MarketKeys {
		key := strings.TrimSpace(raw)
		if key == "" {
			return false, fmt.Errorf("empty Combo Lab market key")
		}
		marketKeys = append(marketKeys, key)
	}
	sort.Strings(marketKeys)
	uniqueMarkets := marketKeys[:0]
	for _, key := range marketKeys {
		if len(uniqueMarkets) == 0 || uniqueMarkets[len(uniqueMarkets)-1] != key {
			uniqueMarkets = append(uniqueMarkets, key)
		}
	}
	marketKeysJSON, _ := json.Marshal(uniqueMarkets)
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO plab_grade_receipts(
combo_id,candidate_at,graded_ts,cell,bucket,class,legality,cohort,route_state,nlegs,prod,joint_p,
fees,entry_capital,joint_payout,realized_return,predicted_return,mve_valid,realized_mve_return,won,
independence_keys_json,market_keys_json,legs_json,payouts_json,economics_version,
canonical_system_id,combo_venue,relation_class,producer_family,combo_route,experiment_epoch,combo_key)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'all-in-v2',?,?,?,?,?,?,?)`, r.ComboID, r.CandidateAt,
		r.GradedTS, r.Cell, r.Bucket, r.Class, r.Legality, r.Cohort, r.RouteState, r.NLegs,
		r.Prod, r.JointP, r.Fees, r.EntryCapital, r.JointPayout, math.Max(-1, r.RealizedReturn),
		r.PredictedReturn, boolInt(r.MVEValid), math.Max(-1, r.RealizedMVEReturn), boolInt(r.Won),
		string(keysJSON), string(marketKeysJSON), r.LegsJSON, r.PayoutsJSON,
		r.CanonicalSystemID, r.ComboVenue, r.RelationClass, r.ProducerFamily,
		r.ComboRoute, r.ExperimentEpoch, r.ComboKey)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) PlabGradeReceipts(ctx context.Context) ([]PlabGradeReceipt, error) {
	if err := s.ensureComboLabCohortSchema(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT combo_id,candidate_at,graded_ts,cell,bucket,class,
legality,cohort,route_state,nlegs,prod,joint_p,fees,entry_capital,joint_payout,realized_return,
predicted_return,mve_valid,realized_mve_return,won,independence_keys_json,market_keys_json,legs_json,payouts_json
	,canonical_system_id,combo_venue,relation_class,producer_family,combo_route,experiment_epoch,combo_key
FROM plab_grade_receipts WHERE economics_version='all-in-v2' ORDER BY cell,combo_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PlabGradeReceipt
	for rows.Next() {
		var r PlabGradeReceipt
		var mve, won int
		var keysJSON, marketKeysJSON string
		if err := rows.Scan(&r.ComboID, &r.CandidateAt, &r.GradedTS, &r.Cell, &r.Bucket,
			&r.Class, &r.Legality, &r.Cohort, &r.RouteState, &r.NLegs, &r.Prod, &r.JointP,
			&r.Fees, &r.EntryCapital, &r.JointPayout, &r.RealizedReturn, &r.PredictedReturn,
			&mve, &r.RealizedMVEReturn, &won, &keysJSON, &marketKeysJSON, &r.LegsJSON, &r.PayoutsJSON,
			&r.CanonicalSystemID, &r.ComboVenue, &r.RelationClass, &r.ProducerFamily,
			&r.ComboRoute, &r.ExperimentEpoch, &r.ComboKey); err != nil {
			return nil, err
		}
		r.MVEValid, r.Won = mve != 0, won != 0
		if json.Unmarshal([]byte(keysJSON), &r.IndependenceKeys) != nil || len(r.IndependenceKeys) == 0 {
			return nil, fmt.Errorf("invalid Combo Lab grade receipt keys for %s", r.ComboID)
		}
		if json.Unmarshal([]byte(marketKeysJSON), &r.MarketKeys) != nil {
			return nil, fmt.Errorf("invalid Combo Lab market keys for %s", r.ComboID)
		}
		// Compatibility for receipts written between the all-in-v2 migration and the split-key
		// correction: derive exact venue+ticker identities from their immutable legs JSON.
		if len(r.MarketKeys) == 0 {
			var legs []struct {
				Platform string `json:"platform"`
				Ticker   string `json:"ticker"`
			}
			if json.Unmarshal([]byte(r.LegsJSON), &legs) != nil {
				return nil, fmt.Errorf("invalid Combo Lab legacy legs for %s", r.ComboID)
			}
			seen := map[string]struct{}{}
			for _, leg := range legs {
				key := strings.ToLower(strings.TrimSpace(leg.Platform)) + "|" + strings.ToUpper(strings.TrimSpace(leg.Ticker))
				if key == "|" {
					continue
				}
				seen[key] = struct{}{}
			}
			for key := range seen {
				r.MarketKeys = append(r.MarketKeys, key)
			}
			sort.Strings(r.MarketKeys)
		}
		if len(r.MarketKeys) == 0 {
			return nil, fmt.Errorf("invalid Combo Lab market keys for %s", r.ComboID)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PlabPendingHorizonRow is an unresolved candidate eligible for current-horizon cleanup. Fully
// settled rows are deliberately absent: they must be economically graded before any cleanup.
type PlabPendingHorizonRow struct {
	ID   string
	Legs string
}

// PlabInsertBatch inserts one enumeration cycle's candidates in a single transaction.
// INSERT OR IGNORE on the combo-id PK makes re-enumeration idempotent (the 12h in-memory dedup
// snapshot is an optimization, not the guarantee). Returns rows actually NEW.
func (s *Store) PlabInsertBatch(ctx context.Context, cands []PlabCand) (int64, error) {
	if len(cands) == 0 {
		return 0, nil
	}
	if err := s.ensureComboLabCohortSchema(ctx); err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	ins, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO plab_open
		(id, at, bucket, class, legality, corr_cluster, prod, joint_p, rho, rho_n,
		 fee_syn, fee_mve, ev_syn, ev_mve, leg_fees, legs, nlegs, unresolved,cohort,route_state,
		 canonical_system_id,combo_venue,relation_class,producer_family,combo_route,experiment_epoch,combo_key)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return 0, err
	}
	defer ins.Close()
	leg, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO plab_leg_open
		(combo_id, platform, ticker, yes) VALUES (?,?,?,NULL)`)
	if err != nil {
		return 0, err
	}
	defer leg.Close()
	var n int64
	for _, c := range cands {
		// Every current collector row carries one canonical key per leg. Reject an incomplete or
		// repeated instrument set before writing the parent row: otherwise unresolved can never
		// reach zero reliably and a YES+NO copy of the same market can masquerade as two legs.
		// Empty LegKeys remains read-compatible for pre-R128 migration fixtures only; plabToRow
		// always supplies keys for every newly sampled row.
		if len(c.LegKeys) > 0 {
			if c.NLegs < 2 || (c.NLegs <= 6 && len(c.LegKeys) != c.NLegs) {
				return 0, fmt.Errorf("invalid Combo Lab leg-key count for %q: got %d, want %d", c.ID, len(c.LegKeys), c.NLegs)
			}
			seen := make(map[string]struct{}, len(c.LegKeys))
			for _, lk := range c.LegKeys {
				platform := strings.ToLower(strings.TrimSpace(lk.Platform))
				ticker := strings.ToUpper(strings.TrimSpace(lk.Ticker))
				if platform == "" || ticker == "" {
					return 0, fmt.Errorf("empty Combo Lab leg key for %q", c.ID)
				}
				key := platform + "|" + ticker
				if _, duplicate := seen[key]; duplicate {
					return 0, fmt.Errorf("repeated Combo Lab instrument for %q: %s", c.ID, key)
				}
				seen[key] = struct{}{}
			}
		}
		if c.Cohort == "" {
			c.Cohort = ComboLabCohortAllEligible
		}
		if c.Cohort != ComboLabCohortAllEligible && c.Cohort != ComboLabCohortRollingPositive &&
			c.Cohort != ComboLabCohortPromotedSystem {
			continue
		}
		if c.RouteState == "" {
			c.RouteState = "synthetic-settlement-only"
		}
		cc := 0
		if c.CorrCluster {
			cc = 1
		}
		res, err := ins.ExecContext(ctx, c.ID, c.At, c.Bucket, c.Class, c.Legality, cc,
			c.Prod, c.JointP, c.Rho, c.RhoN, c.FeeSyn, c.FeeMVE, c.EVSyn, c.EVMVE,
			c.LegFees, c.Legs, c.NLegs, c.NLegs, c.Cohort, c.RouteState,
			c.CanonicalSystemID, c.ComboVenue, c.RelationClass, c.ProducerFamily,
			c.ComboRoute, c.ExperimentEpoch, c.ComboKey)
		if err != nil {
			return 0, err
		}
		if a, _ := res.RowsAffected(); a > 0 {
			n += a
			for _, lk := range c.LegKeys {
				if _, err := leg.ExecContext(ctx, c.ID, lk.Platform, lk.Ticker); err != nil {
					return 0, err
				}
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// PlabOpenCount — open-ledger size (the API/briefing display number).
func (s *Store) PlabOpenCount(ctx context.Context) (int64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plab_open`).Scan(&n)
	return n, err
}

// PlabOpenCurrentCount is the bounded prospective sampler's admission denominator. Historical
// rows above today's leg ceiling remain immutable and continue settling, but cannot consume every
// seat intended for current 2..maxLegs research.
func (s *Store) PlabOpenCurrentCount(ctx context.Context, maxLegs int) (int64, error) {
	if maxLegs < 2 {
		return 0, nil
	}
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plab_open WHERE nlegs BETWEEN 2 AND ?`, maxLegs).Scan(&n)
	return n, err
}

// PlabOpenCurrentCountSince is the current-policy admission denominator. Rows created before the
// supplied durable policy epoch remain in the immutable settlement backlog, but cannot consume
// seats intended for 2..maxLegs candidates admitted under the current horizon contract.
func (s *Store) PlabOpenCurrentCountSince(ctx context.Context, maxLegs int, admittedAtOrAfter int64) (int64, error) {
	if maxLegs < 2 || admittedAtOrAfter <= 0 {
		return 0, nil
	}
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM plab_open
		WHERE nlegs BETWEEN 2 AND ? AND at>=?`, maxLegs, admittedAtOrAfter).Scan(&n)
	return n, err
}

// PlabOpenCohortCounts keeps the two research populations visible. These are prospective
// settlement rows, not placed bets or LIVE-transfer proof.
func (s *Store) PlabOpenCohortCounts(ctx context.Context) (map[string]int64, error) {
	if err := s.ensureComboLabCohortSchema(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT cohort,COUNT(*) FROM plab_open GROUP BY cohort`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{ComboLabCohortAllEligible: 0, ComboLabCohortRollingPositive: 0,
		ComboLabCohortPromotedSystem: 0}
	for rows.Next() {
		var cohort string
		var n int64
		if err := rows.Scan(&cohort, &n); err != nil {
			return nil, err
		}
		out[cohort] = n
	}
	return out, rows.Err()
}

// PlabOpenCurrentCohortLegCounts is the human-facing denominator for the current 2..maxLegs
// experiment.  Keeping cohort and ticket length together prevents a large generic sample from
// making the positive-system lane look empty even while its rows are waiting to settle.
func (s *Store) PlabOpenCurrentCohortLegCounts(ctx context.Context, maxLegs int) (map[string]map[int]int64, error) {
	if err := s.ensureComboLabCohortSchema(ctx); err != nil {
		return nil, err
	}
	out := map[string]map[int]int64{
		ComboLabCohortAllEligible:     {},
		ComboLabCohortRollingPositive: {},
		ComboLabCohortPromotedSystem:  {},
	}
	if maxLegs < 2 {
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT cohort,nlegs,COUNT(*) FROM plab_open
		WHERE nlegs BETWEEN 2 AND ? GROUP BY cohort,nlegs ORDER BY cohort,nlegs`, maxLegs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var cohort string
		var legs int
		var n int64
		if err := rows.Scan(&cohort, &legs, &n); err != nil {
			return nil, err
		}
		if out[cohort] == nil {
			out[cohort] = map[int]int64{}
		}
		out[cohort][legs] = n
	}
	return out, rows.Err()
}

// PlabOpenCurrentCohortLegCountsSince is the human-facing twin of
// PlabOpenCurrentCountSince. Historical prospective rows stay settlement-visible through the
// broad open counters, while this view reports only rows admitted under the current policy epoch.
func (s *Store) PlabOpenCurrentCohortLegCountsSince(ctx context.Context, maxLegs int, admittedAtOrAfter int64) (map[string]map[int]int64, error) {
	if err := s.ensureComboLabCohortSchema(ctx); err != nil {
		return nil, err
	}
	out := map[string]map[int]int64{
		ComboLabCohortAllEligible:     {},
		ComboLabCohortRollingPositive: {},
		ComboLabCohortPromotedSystem:  {},
	}
	if maxLegs < 2 || admittedAtOrAfter <= 0 {
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT cohort,nlegs,COUNT(*) FROM plab_open
		WHERE nlegs BETWEEN 2 AND ? AND at>=? GROUP BY cohort,nlegs ORDER BY cohort,nlegs`,
		maxLegs, admittedAtOrAfter)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var cohort string
		var legs int
		var n int64
		if err := rows.Scan(&cohort, &legs, &n); err != nil {
			return nil, err
		}
		if out[cohort] == nil {
			out[cohort] = map[int]int64{}
		}
		out[cohort][legs] = n
	}
	return out, rows.Err()
}

// PlabOpenLegCounts exposes the open settlement sample by ticket length. It deliberately counts
// candidate rows, not the implicit logical manifest and not economically graded outcomes.
func (s *Store) PlabOpenLegCounts(ctx context.Context) (map[int]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT nlegs,COUNT(*) FROM plab_open GROUP BY nlegs ORDER BY nlegs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int]int64{}
	for rows.Next() {
		var legs int
		var n int64
		if err := rows.Scan(&legs, &n); err != nil {
			return nil, err
		}
		out[legs] = n
	}
	return out, rows.Err()
}

func (s *Store) PlabPendingHorizonRows(ctx context.Context, limit int) ([]PlabPendingHorizonRow, error) {
	if limit <= 0 {
		limit = 5000
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT id,legs FROM plab_open WHERE unresolved > 0 ORDER BY at LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]PlabPendingHorizonRow, 0, limit)
	for rows.Next() {
		var row PlabPendingHorizonRow
		if err := rows.Scan(&row.ID, &row.Legs); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// PlabPruneAboveLegLimit is a compatibility seam that preserves valid legacy experiments. Current
// leg limits apply at admission; a later policy change cannot erase an outcome already in flight.
func (s *Store) PlabPruneAboveLegLimit(ctx context.Context, maxLegs, limit int) (int64, error) {
	// A policy change cannot erase a prospective experiment after its outcome is in flight.
	// Current leg limits apply at admission only; valid legacy rows keep settling and grading.
	_ = ctx
	_ = maxLegs
	_ = limit
	return 0, nil
}

// PlabUnresolvedTickers returns distinct legs still awaiting settlement — the settle-sweep's
// work list (each ticker is resolved ONCE per sweep regardless of how many combos hold it).
func (s *Store) PlabUnresolvedTickers(ctx context.Context, limit int) ([]PlabLegKey, error) {
	return s.PlabUnresolvedTickersAfter(ctx, PlabLegKey{}, limit)
}

// PlabUnresolvedTickersAfter returns one stable, wrapping keyset page. The old unordered DISTINCT
// LIMIT query could return the same front page forever, so a settlement-fetch budget smaller than
// the unresolved universe permanently starved later tickers. The caller persists the last key it
// receives and advances through the entire universe across passes. Wrapping keeps small ledgers
// useful without requiring a special reset transaction.
func (s *Store) PlabUnresolvedTickersAfter(ctx context.Context, after PlabLegKey, limit int) ([]PlabLegKey, error) {
	if limit <= 0 {
		limit = 1000
	}
	read := func(where string, args ...any) ([]PlabLegKey, error) {
		q := `SELECT l.platform,l.ticker,MIN(o.at)
			FROM plab_leg_open l JOIN plab_open o ON o.id=l.combo_id
			WHERE l.yes IS NULL ` + where +
			` GROUP BY l.platform,l.ticker ORDER BY l.platform,l.ticker LIMIT ?`
		args = append(args, limit)
		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		out := make([]PlabLegKey, 0, limit)
		for rows.Next() {
			var k PlabLegKey
			if err := rows.Scan(&k.Platform, &k.Ticker, &k.FirstAt); err != nil {
				return nil, err
			}
			out = append(out, k)
		}
		return out, rows.Err()
	}

	// An empty cursor is the initial page. Real admitted legs always own both fields; malformed
	// rows are handled by the separate malformed-row cleanup and must not distort keyset order.
	if after.Platform == "" && after.Ticker == "" {
		return read("")
	}
	first, err := read(`AND (l.platform > ? OR (l.platform = ? AND l.ticker > ?))`,
		after.Platform, after.Platform, after.Ticker)
	if err != nil {
		return nil, err
	}
	if len(first) >= limit {
		return first, nil
	}
	remaining := limit - len(first)
	// Keep the helper's LIMIT local for the wrapped page.
	oldLimit := limit
	limit = remaining
	wrapped, err := read(`AND (l.platform < ? OR (l.platform = ? AND l.ticker <= ?))`,
		after.Platform, after.Platform, after.Ticker)
	limit = oldLimit
	if err != nil {
		return nil, err
	}
	return append(first, wrapped...), nil
}

// PlabResolveTicker fans a settled YES value out to every open combo holding the leg and
// recomputes those combos' unresolved counts. Returns how many leg rows were stamped.
func (s *Store) PlabResolveTicker(ctx context.Context, platform, ticker string, yes float64) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx,
		`UPDATE plab_leg_open SET yes=? WHERE platform=? AND ticker=? AND yes IS NULL`,
		yes, platform, ticker)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		if _, err := tx.ExecContext(ctx, `UPDATE plab_open SET unresolved =
			(SELECT COUNT(*) FROM plab_leg_open l WHERE l.combo_id = plab_open.id AND l.yes IS NULL)
			WHERE id IN (SELECT combo_id FROM plab_leg_open WHERE platform=? AND ticker=?)`,
			platform, ticker); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return n, nil
}

// PlabGradableRow is one fully-settled combo ready to grade.
type PlabGradableRow struct {
	ID                                                           string
	At                                                           int64
	Bucket                                                       string
	Class                                                        string
	Legality                                                     string
	CorrCluster                                                  bool
	Prod                                                         float64
	JointP                                                       float64
	FeeMVE                                                       float64
	EVSyn                                                        float64
	LegFees                                                      string
	Legs                                                         string
	Cohort                                                       string
	RouteState                                                   string
	CanonicalSystemID, ComboVenue, RelationClass, ProducerFamily string
	ComboRoute, ExperimentEpoch, ComboKey                        string
}

// PlabGradable returns combos with every leg settled, logged before olderThan (unix seconds),
// oldest first.
func (s *Store) PlabGradable(ctx context.Context, olderThan int64, limit int) ([]PlabGradableRow, error) {
	if err := s.ensureComboLabCohortSchema(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, at, bucket, class, legality, corr_cluster,
		prod, joint_p, fee_mve, ev_syn, leg_fees, legs,cohort,route_state,
		canonical_system_id,combo_venue,relation_class,producer_family,combo_route,experiment_epoch,combo_key
		FROM plab_open WHERE unresolved = 0 AND at <= ? ORDER BY at LIMIT ?`, olderThan, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PlabGradableRow
	for rows.Next() {
		var r PlabGradableRow
		var cc int
		if rows.Scan(&r.ID, &r.At, &r.Bucket, &r.Class, &r.Legality, &cc,
			&r.Prod, &r.JointP, &r.FeeMVE, &r.EVSyn, &r.LegFees, &r.Legs,
			&r.Cohort, &r.RouteState, &r.CanonicalSystemID, &r.ComboVenue,
			&r.RelationClass, &r.ProducerFamily, &r.ComboRoute,
			&r.ExperimentEpoch, &r.ComboKey) == nil {
			r.CorrCluster = cc != 0
			out = append(out, r)
		}
	}
	return out, rows.Err()
}

// PlabLegVals returns each combo's settled leg YES values keyed "platform|ticker".
func (s *Store) PlabLegVals(ctx context.Context, ids []string) (map[string]map[string]float64, error) {
	out := map[string]map[string]float64{}
	for len(ids) > 0 {
		chunk := ids
		if len(chunk) > 400 {
			chunk = chunk[:400]
		}
		ids = ids[len(chunk):]
		ph := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := s.db.QueryContext(ctx,
			`SELECT combo_id, platform, ticker, yes FROM plab_leg_open WHERE combo_id IN (`+ph+`)`, args...)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var cid, plat, tick string
			var yes sql.NullFloat64
			if rows.Scan(&cid, &plat, &tick, &yes) == nil && yes.Valid {
				m := out[cid]
				if m == nil {
					m = map[string]float64{}
					out[cid] = m
				}
				m[plat+"|"+tick] = yes.Float64
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// PlabDeleteBatch removes graded combos (and their leg rows) in one transaction.
func (s *Store) PlabDeleteBatch(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	rest := ids
	for len(rest) > 0 {
		chunk := rest
		if len(chunk) > 400 {
			chunk = chunk[:400]
		}
		rest = rest[len(chunk):]
		ph := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM plab_leg_open WHERE combo_id IN (`+ph+`)`, args...); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM plab_open WHERE id IN (`+ph+`)`, args...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// PlabDeletePendingBatch rechecks unresolved inside the deletion transaction. A settlement that
// lands between cleanup selection and deletion therefore wins: the row remains for grading.
func (s *Store) PlabDeletePendingBatch(ctx context.Context, ids []string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var removed int64
	rest := ids
	for len(rest) > 0 {
		chunk := rest
		if len(chunk) > 400 {
			chunk = chunk[:400]
		}
		rest = rest[len(chunk):]
		ph := strings.TrimRight(strings.Repeat("?,", len(chunk)), ",")
		args := make([]any, len(chunk))
		for i, id := range chunk {
			args[i] = id
		}
		rows, err := tx.QueryContext(ctx, `SELECT id FROM plab_open WHERE unresolved > 0 AND id IN (`+ph+`)`, args...)
		if err != nil {
			return removed, err
		}
		pending := make([]string, 0, len(chunk))
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return removed, err
			}
			pending = append(pending, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil || len(pending) == 0 {
			if err != nil {
				return removed, err
			}
			continue
		}
		pendingPH := strings.TrimRight(strings.Repeat("?,", len(pending)), ",")
		pendingArgs := make([]any, len(pending))
		for i, id := range pending {
			pendingArgs[i] = id
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM plab_leg_open WHERE combo_id IN (`+pendingPH+`)`, pendingArgs...); err != nil {
			return removed, err
		}
		res, err := tx.ExecContext(ctx, `DELETE FROM plab_open WHERE unresolved > 0 AND id IN (`+pendingPH+`)`, pendingArgs...)
		if err != nil {
			return removed, err
		}
		n, _ := res.RowsAffected()
		removed += n
	}
	if err := tx.Commit(); err != nil {
		return removed, err
	}
	return removed, nil
}

// PlabExpire is a compatibility seam that preserves unresolved truth. Age alone cannot distinguish
// a true void from delayed publication, a postponement or an outage, so it never deletes rows.
func (s *Store) PlabExpire(ctx context.Context, olderThan int64, limit int) (int64, error) {
	// Age alone cannot distinguish a void from delayed publication, a postponement or an outage.
	// Preserve the row for authoritative late settlement; only structurally malformed rows may be
	// removed by the server's explicit malformed-row cleanup.
	_ = ctx
	_ = olderThan
	_ = limit
	return 0, nil
}

// RecentSignal is one fresh, unresolved signal row — an EDGE-BASED combo leg source (R128:
// PROVEN+ families directly; PROVEN− families through their R118 drag-adjusted inverse).
type RecentSignal struct {
	Platform   string
	Ticker     string
	Side       string
	EntryPrice float64
}

// RecentOpenSignals returns the newest unresolved rows of one signal family since sinceTS
// (RFC3339 UTC — signal_log.ts sorts lexically).
func (s *Store) RecentOpenSignals(ctx context.Context, family, sinceTS string, limit int) ([]RecentSignal, error) {
	q := `SELECT platform, ticker, side, entry_price
		FROM signal_log WHERE signal_type=? AND resolved=0 AND ts>=? ORDER BY id DESC`
	args := []any{family, sinceTS}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RecentSignal
	for rows.Next() {
		var r RecentSignal
		if rows.Scan(&r.Platform, &r.Ticker, &r.Side, &r.EntryPrice) == nil {
			out = append(out, r)
		}
	}
	return out, rows.Err()
}
