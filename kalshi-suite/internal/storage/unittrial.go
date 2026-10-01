package storage

import (
	"context"
	"database/sql"
	"hash/fnv"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

func unitTrialRouteKeys(u UnitTrial) (string, string) {
	opp := ResearchRouteStableID("unit", u.Family, u.Platform, u.Ticker, u.Side,
		strconv.Itoa(u.Episode), u.OriginLayer)
	route := "taker-" + ResearchRouteStableID(u.OpenedTS.UTC().Format(time.RFC3339Nano),
		strconv.FormatFloat(u.Ask, 'g', -1, 64), strconv.FormatFloat(u.FeePC, 'g', -1, 64),
		strconv.FormatFloat(u.Depth, 'g', -1, 64), u.QuoteSource, u.FeeSource)
	return opp, route
}

// UnitTrial is a one-share, executable-ask research observation. It deliberately has no stake,
// bankroll, allocation, or placement-status field: opportunity quality and portfolio capacity
// are separate questions.
type UnitTrial struct {
	OpenedTS    time.Time
	Family      string
	Platform    string
	OriginLayer string
	Ticker      string
	Side        string
	Episode     int
	// CanonicalEventID/EventVersion are storage-owned frozen dependence identity. InsertUnitTrial
	// reads them from the current immutable instrument registry; callers cannot manufacture them.
	CanonicalEventID string
	EventVersion     int
	Category         string
	Ask              float64
	FeePC            float64
	FeeKnown         bool
	FeeSource        string
	Depth            float64
	QuoteSource      string
	ResolveHours     float64
}

// PositiveUnitTrialLeg is one still-open, exact-money observation whose exact
// family+venue+origin+side taker route has positive settled one-share economics. It is a Combo
// Lab discovery input only; current book/fee/horizon gates still run again before admission.
type PositiveUnitTrialLeg struct {
	OpenedTS     time.Time
	Family       string
	Platform     string
	OriginLayer  string
	Ticker       string
	Side         string
	Category     string
	Ask          float64
	FeePC        float64
	Depth        float64
	QuoteSource  string
	ResolveHours float64
	MeanPC       float64
}

// RecentPositiveUnitTrialLegs returns current opportunities for every exact positive UnitTrial
// route. Route cells never pool venue, side, or model-vs-strategy origin, and rows without an
// authoritative fee/depth receipt are excluded rather than upgraded by inference.
func (s *Store) RecentPositiveUnitTrialLegs(ctx context.Context, since time.Time, limit int) ([]PositiveUnitTrialLeg, error) {
	if limit <= 0 {
		limit = 64
	}
	if limit > 64 {
		limit = 64 // per exact route, not a global LIMIT a prolific route can monopolize
	}
	rows, err := s.db.QueryContext(ctx, `
WITH market_edge AS (
 SELECT family,LOWER(TRIM(platform)) platform,
        CASE WHEN origin_layer='strategy' THEN 'strategy' ELSE 'model' END origin_layer,
        UPPER(TRIM(side)) side,TRIM(ticker) ticker,AVG(pnl_pc) market_mean_pc
 FROM unit_trials
 WHERE settled=1 AND pnl_pc IS NOT NULL AND depth>=1 AND fee_known=1
   AND TRIM(fee_source)!='' AND TRIM(ticker)!=''
 GROUP BY family,platform,origin_layer,side,TRIM(ticker)
), positive_route AS (
 SELECT family,platform,origin_layer,side,AVG(market_mean_pc) mean_pc
 FROM market_edge
 GROUP BY family,platform,origin_layer,side
 HAVING AVG(market_mean_pc)>0
), current_ticker AS (
 SELECT u.id,u.opened_ts,u.family,LOWER(TRIM(u.platform)) platform,
        CASE WHEN u.origin_layer='strategy' THEN 'strategy' ELSE 'model' END origin_layer,
        u.ticker,UPPER(TRIM(u.side)) side,u.category,u.ask,u.fee_pc,u.depth,u.quote_source,u.resolve_hours,p.mean_pc,
        ROW_NUMBER() OVER (PARTITION BY u.family,LOWER(TRIM(u.platform)),
          CASE WHEN u.origin_layer='strategy' THEN 'strategy' ELSE 'model' END,
          UPPER(TRIM(u.side)),u.ticker ORDER BY u.opened_ts DESC,u.id DESC) ticker_rank
 FROM unit_trials u JOIN positive_route p ON p.family=u.family AND p.platform=LOWER(TRIM(u.platform))
  AND p.origin_layer=CASE WHEN u.origin_layer='strategy' THEN 'strategy' ELSE 'model' END
  AND p.side=UPPER(TRIM(u.side))
 WHERE u.settled=0 AND u.opened_ts>=? AND u.depth>=1 AND u.fee_known=1 AND TRIM(u.fee_source)!=''
), route_ranked AS (
 SELECT *,ROW_NUMBER() OVER (PARTITION BY family,platform,origin_layer,side
                             ORDER BY opened_ts DESC,id DESC) route_rank
 FROM current_ticker WHERE ticker_rank=1
)
SELECT opened_ts,family,platform,origin_layer,ticker,side,category,ask,fee_pc,depth,quote_source,resolve_hours,mean_pc
FROM route_ranked WHERE route_rank<=?
ORDER BY family,platform,origin_layer,side,route_rank`, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]PositiveUnitTrialLeg, 0)
	seen := map[string]bool{}
	for rows.Next() {
		var opened string
		var leg PositiveUnitTrialLeg
		if err := rows.Scan(&opened, &leg.Family, &leg.Platform, &leg.OriginLayer, &leg.Ticker,
			&leg.Side, &leg.Category, &leg.Ask, &leg.FeePC, &leg.Depth, &leg.QuoteSource,
			&leg.ResolveHours, &leg.MeanPC); err != nil {
			return nil, err
		}
		leg.OpenedTS, _ = time.Parse(time.RFC3339Nano, opened)
		key := leg.Family + "\x00" + leg.Platform + "\x00" + leg.OriginLayer + "\x00" + leg.Side + "\x00" + leg.Ticker
		if !seen[key] {
			seen[key] = true
			out = append(out, leg)
		}
	}
	return out, rows.Err()
}

// migrateUnitTrialOrigin makes model-origin and accepted-strategy-origin observations distinct
// prospective cohorts. SQLite cannot drop the legacy five-column UNIQUE constraint in place, so
// the small research table is rebuilt transactionally once. Existing provenance is recovered from
// the durable quote_source prefix; no outcome or price is rewritten.
func migrateUnitTrialOrigin(db *sql.DB) error {
	var ddl string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='unit_trials'`).Scan(&ddl); err != nil {
		return err
	}
	compact := strings.NewReplacer(" ", "", "\n", "", "\r", "", "\t", "").Replace(strings.ToLower(ddl))
	if strings.Contains(compact, "origin_layer") && strings.Contains(compact, "fee_known") &&
		strings.Contains(compact, "fee_source") &&
		strings.Contains(compact, "unique(family,platform,ticker,side,episode,origin_layer)") {
		return nil
	}
	hasOrigin := strings.Contains(compact, "origin_layer")
	hasFeeKnown := strings.Contains(compact, "fee_known")
	hasFeeSource := strings.Contains(compact, "fee_source")
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, stmt := range []string{
		`DROP TABLE IF EXISTS unit_trials_r135c`,
		`CREATE TABLE unit_trials_r135c (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 opened_ts TEXT NOT NULL, closed_ts TEXT NOT NULL DEFAULT '',
 family TEXT NOT NULL, platform TEXT NOT NULL,
 origin_layer TEXT NOT NULL DEFAULT 'model' CHECK(origin_layer IN ('model','strategy')),
 ticker TEXT NOT NULL, side TEXT NOT NULL, episode INTEGER NOT NULL DEFAULT 0,
 canonical_event_id TEXT NOT NULL DEFAULT '', event_version INTEGER NOT NULL DEFAULT 0 CHECK(event_version >= 0),
 category TEXT NOT NULL DEFAULT '', ask REAL NOT NULL, fee_pc REAL NOT NULL DEFAULT 0,
 fee_known INTEGER NOT NULL DEFAULT 0, fee_source TEXT NOT NULL DEFAULT '',
 depth REAL NOT NULL DEFAULT 0, quote_source TEXT NOT NULL DEFAULT '',
 resolve_hours REAL NOT NULL DEFAULT 0, settled INTEGER NOT NULL DEFAULT 0,
 settle_val REAL, pnl_pc REAL, return_per_dollar REAL, capital_day REAL,
 UNIQUE(family,platform,ticker,side,episode,origin_layer)
)`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	originExpr := `CASE WHEN lower(trim(quote_source)) LIKE 'strategy-decision/%' THEN 'strategy' ELSE 'model' END`
	if hasOrigin {
		originExpr = `CASE WHEN origin_layer='strategy' THEN 'strategy' ELSE 'model' END`
	}
	feeKnownExpr, feeSourceExpr := `0`, `''`
	if hasFeeKnown {
		feeKnownExpr = `CASE WHEN fee_known=1 THEN 1 ELSE 0 END`
	}
	if hasFeeSource {
		feeSourceExpr = `COALESCE(fee_source,'')`
	}
	copySQL := `INSERT INTO unit_trials_r135c(
 id,opened_ts,closed_ts,family,platform,origin_layer,ticker,side,episode,canonical_event_id,event_version,category,
 ask,fee_pc,fee_known,fee_source,depth,quote_source,resolve_hours,settled,settle_val,pnl_pc,return_per_dollar,capital_day)
SELECT id,opened_ts,closed_ts,family,platform,` + originExpr + `,ticker,side,episode,'',0,category,
 ask,fee_pc,` + feeKnownExpr + `,` + feeSourceExpr + `,depth,quote_source,resolve_hours,settled,settle_val,pnl_pc,return_per_dollar,capital_day
FROM unit_trials`
	if _, err := tx.Exec(copySQL); err != nil {
		return err
	}
	for _, stmt := range []string{
		`DROP INDEX IF EXISTS idx_unit_trials_open`,
		`DROP INDEX IF EXISTS idx_unit_trials_family`,
		`DROP TABLE unit_trials`,
		`ALTER TABLE unit_trials_r135c RENAME TO unit_trials`,
		`CREATE INDEX idx_unit_trials_open ON unit_trials(settled,platform,ticker)`,
		`CREATE INDEX idx_unit_trials_family ON unit_trials(family,platform,settled)`,
		`CREATE INDEX idx_unit_trials_event ON unit_trials(family,platform,origin_layer,side,canonical_event_id,settled)`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// migrateUnitTrialEventIdentity adds a prospective, immutable dependence key without rewriting
// history. Existing rows remain blank: joining an old outcome to today's mutable catalog would be
// outcome-aware backfill and could falsely turn related contracts into independent evidence.
func migrateUnitTrialEventIdentity(db *sql.DB) error {
	for _, col := range []string{
		"canonical_event_id TEXT NOT NULL DEFAULT ''",
		"event_version INTEGER NOT NULL DEFAULT 0 CHECK(event_version >= 0)",
	} {
		if _, err := db.Exec("ALTER TABLE unit_trials ADD COLUMN " + col); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	_, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_unit_trials_event
ON unit_trials(family,platform,origin_layer,side,canonical_event_id,settled)`)
	return err
}

// InsertUnitTrial records the first executable observation of an opportunity episode.
func (s *Store) InsertUnitTrial(ctx context.Context, u UnitTrial) (bool, error) {
	if u.OpenedTS.IsZero() {
		u.OpenedTS = time.Now().UTC()
	}
	if u.OriginLayer != "model" && u.OriginLayer != "strategy" {
		u.OriginLayer = unitTrialOriginLayer(u.QuoteSource)
	}
	// Freeze only the immutable identity registry's current version at observation time. A missing
	// registry row remains explicitly unclustered; ticker/title/event-key inference is forbidden.
	u.CanonicalEventID, u.EventVersion = "", 0
	if identity, ok, identityErr := s.CurrentCanonicalInstrument(ctx, u.Platform, u.Ticker); identityErr != nil {
		return false, identityErr
	} else if ok && strings.TrimSpace(identity.EventID) != "" && identity.EventVersion > 0 {
		u.CanonicalEventID, u.EventVersion = strings.TrimSpace(identity.EventID), identity.EventVersion
	}
	feeKnown := 0
	if u.FeeKnown && strings.TrimSpace(u.FeeSource) != "" {
		feeKnown = 1
	}
	r, err := s.db.ExecContext(ctx, `INSERT INTO unit_trials
(opened_ts,family,platform,origin_layer,ticker,side,episode,canonical_event_id,event_version,category,ask,fee_pc,fee_known,fee_source,depth,quote_source,resolve_hours)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(family,platform,ticker,side,episode,origin_layer) DO UPDATE SET
 opened_ts=excluded.opened_ts,
 canonical_event_id=CASE WHEN excluded.event_version>0 AND TRIM(excluded.canonical_event_id)!=''
   THEN excluded.canonical_event_id ELSE unit_trials.canonical_event_id END,
 event_version=CASE WHEN excluded.event_version>0 AND TRIM(excluded.canonical_event_id)!=''
   THEN excluded.event_version ELSE unit_trials.event_version END,
 category=excluded.category,ask=excluded.ask,fee_pc=excluded.fee_pc,
 fee_known=excluded.fee_known,fee_source=excluded.fee_source,depth=excluded.depth,
 quote_source=excluded.quote_source,resolve_hours=excluded.resolve_hours,closed_ts='',settled=0,
 settle_val=NULL,pnl_pc=NULL,return_per_dollar=NULL,capital_day=NULL
WHERE unit_trials.settled=0
  AND (unit_trials.depth<1 OR unit_trials.fee_known!=1 OR TRIM(unit_trials.fee_source)='')
  AND excluded.depth>=1 AND excluded.fee_known=1 AND TRIM(excluded.fee_source)!=''`,
		u.OpenedTS.UTC().Format(time.RFC3339Nano), u.Family, u.Platform, u.OriginLayer, u.Ticker, u.Side,
		u.Episode, u.CanonicalEventID, u.EventVersion, u.Category, u.Ask, u.FeePC, feeKnown,
		strings.TrimSpace(u.FeeSource), u.Depth, u.QuoteSource, u.ResolveHours)
	if err != nil {
		return false, err
	}
	n, err := r.RowsAffected()
	if err != nil {
		return false, err
	}
	// Mirror every economically valid one-share observation into the unified ledger, including
	// incomplete fee/depth rows as explicit blocked alternatives. No point estimate is promoted to
	// a lower bound; settlement later appends the grade event.
	if n > 0 && researchRouteFinite(u.Ask, u.FeePC, u.Depth, u.ResolveHours) && u.Ask >= 0 && u.Ask <= 1 &&
		u.FeePC >= 0 && u.Depth >= 0 && u.ResolveHours >= 0 {
		oppID, routeID := unitTrialRouteKeys(u)
		quoteSource, feeAuthority := strings.TrimSpace(u.QuoteSource), strings.TrimSpace(u.FeeSource)
		if quoteSource == "" {
			quoteSource = "missing_quote_source"
		}
		if !u.FeeKnown || feeAuthority == "" {
			feeAuthority = "missing_exact_fee_receipt"
		}
		action, reason := "reject", "blocked: incomplete side-specific depth or exact fee receipt"
		if u.Ask > 0 && u.Ask < 1 && u.Depth >= 1 && u.FeeKnown && strings.TrimSpace(u.FeeSource) != "" {
			action, reason = "buy", "blocked: untouched executable-edge lower bound and lifecycle outcome pending"
		}
		_, routeErr := s.InsertResearchRouteOpportunity(ctx, ResearchRouteOpportunity{
			OpportunityID: oppID, RouteID: routeID, Observed: u.OpenedTS, DecisionAt: u.OpenedTS,
			SystemName: u.Family, IdentityStatus: "unverified", Venue: strings.ToLower(u.Platform),
			Ticker: u.Ticker, Side: strings.ToUpper(u.Side), Route: "taker", Action: action,
			QuoteSource: quoteSource, ExecutablePrice: u.Ask, ExecutableDepth: u.Depth,
			DepthKnown: u.Depth >= 1, RequestedQty: 1, FeeAmount: u.FeePC,
			FeeAuthority: feeAuthority, FeeKnown: u.FeeKnown && strings.TrimSpace(u.FeeSource) != "",
			ExpectedPayoutLow: 0, ExpectedPayoutHigh: 1,
			ExpectedNetLow: -(u.Ask + u.FeePC), ExpectedNetHigh: 1 - u.Ask - u.FeePC,
			PartialFillWorst: -(u.Ask + u.FeePC), CapitalSeconds: u.ResolveHours * 3600,
			Decision: "blocked", DecisionReason: reason, AlternativeGroup: oppID,
			EvidenceJSON: `{"origin":"unit_trials","quote_age_tick_latency":"not_present_in_unit-v1; blocked"}`,
		})
		if routeErr != nil {
			return false, routeErr
		}
	}
	return n > 0, nil
}

// ResolveUnitTrialsFromSignals grades open unit trials from the canonical signal settlement
// ledger. A bounded batch keeps this maintenance write short even after a long outage.
func (s *Store) ResolveUnitTrialsFromSignals(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		limit = 2000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT u.id,u.family,u.platform,u.origin_layer,u.ticker,u.side,u.episode,u.ask,u.fee_pc,
u.fee_known,u.fee_source,u.depth,u.quote_source,u.resolve_hours,u.opened_ts,
COALESCE(v.yes_value,r.settle_val),COALESCE(v.resolved_at,r.resolved_at)
FROM unit_trials u
LEFT JOIN venue_settlements v ON v.platform=u.platform AND v.ticker=u.ticker
LEFT JOIN signal_log r ON r.id=(
  SELECT r2.id FROM signal_log r2
  WHERE r2.platform=u.platform AND r2.ticker=u.ticker AND r2.resolved=1
    AND r2.settle_val>=0 AND r2.settle_val<=1 AND r2.resolved_at IS NOT NULL AND r2.resolved_at!=''
  ORDER BY r2.resolved_at DESC,r2.id DESC LIMIT 1
)
WHERE u.settled=0 AND (v.ticker IS NOT NULL OR r.id IS NOT NULL) ORDER BY u.id LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	type grade struct {
		id                              int64
		settle, pnl, roi                float64
		pay, capitalDay, capitalSeconds float64
		closed                          string
		trial                           UnitTrial
	}
	var grades []grade
	for rows.Next() {
		var id int64
		var family, platform, origin, ticker, side, feeSource, depthSource, opened, closed string
		var episode, feeKnown int
		var ask, fee, depth, resolveHours, settle float64
		if err := rows.Scan(&id, &family, &platform, &origin, &ticker, &side, &episode, &ask, &fee,
			&feeKnown, &feeSource, &depth, &depthSource, &resolveHours, &opened, &settle, &closed); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ot, e1 := time.Parse(time.RFC3339Nano, opened)
		if e1 != nil {
			ot, e1 = time.Parse(time.RFC3339, opened)
		}
		ct, e2 := time.Parse(time.RFC3339Nano, closed)
		if e2 != nil {
			ct, e2 = time.Parse(time.RFC3339, closed)
		}
		if e1 != nil || e2 != nil || ct.Before(ot) || ask <= 0 || ask >= 1 || settle < 0 || settle > 1 {
			continue
		}
		pay := settle
		if side == "NO" {
			pay = 1 - settle
		}
		pnl := pay - ask - fee
		cost := ask + fee
		if cost <= 0 {
			continue
		}
		roi := pnl / cost
		days := unitTrialElapsedSeconds(ct, ot) / (24 * 60 * 60)
		grades = append(grades, grade{id: id, settle: settle, pay: pay, pnl: pnl, roi: roi,
			capitalDay: roi / days, capitalSeconds: unitTrialElapsedSeconds(ct, ot),
			closed: ct.UTC().Format(time.RFC3339Nano), trial: UnitTrial{OpenedTS: ot, Family: family,
				Platform: platform, OriginLayer: origin, Ticker: ticker, Side: side, Episode: episode,
				Ask: ask, FeePC: fee, FeeKnown: feeKnown == 1, FeeSource: feeSource,
				Depth: depth, QuoteSource: depthSource, ResolveHours: resolveHours}})
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(grades) == 0 {
		return 0, rows.Err()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	n := 0
	for _, g := range grades {
		r, err := tx.ExecContext(ctx, `UPDATE unit_trials SET settled=1,settle_val=?,closed_ts=?,pnl_pc=?,return_per_dollar=?,capital_day=? WHERE id=? AND settled=0`,
			g.settle, g.closed, g.pnl, g.roi, g.capitalDay, g.id)
		if err != nil {
			return 0, err
		}
		if k, _ := r.RowsAffected(); k > 0 {
			n++
			oppID, routeID := unitTrialRouteKeys(g.trial)
			_, err = tx.ExecContext(ctx, `INSERT INTO research_route_events(
opportunity_id,route_id,observed_ts,event_type,quantity,payout,realized_net,capital_seconds,
outcome_status,reason,evidence_json)
SELECT ?,?,?, 'grade',1,?,?,?,'graded','canonical signal settlement','{"source":"unit_trials"}'
WHERE EXISTS(SELECT 1 FROM research_route_opportunities WHERE opportunity_id=? AND route_id=?)`,
				oppID, routeID, g.closed, g.pay, g.pnl, g.capitalSeconds, oppID, routeID)
			if err != nil {
				return 0, err
			}
		}
	}
	return n, tx.Commit()
}

type UnitTrialStat struct {
	Family         string `json:"family"`
	Platform       string `json:"platform"`
	OriginLayer    string `json:"origin_layer"`
	Side           string `json:"side"`
	N              int    `json:"n"` // raw settled route rows; SettledMarkets is the distinct-contract display sample
	Open           int    `json:"open"`
	UniqueMarkets  int    `json:"unique_markets"`
	SettledMarkets int    `json:"settled_unique_markets"`
	OpenMarkets    int    `json:"open_unique_markets"`
	// SettledEventClusters is retained relationship research only. It is incomplete for legacy
	// rows and is not displayed or used for ranking, PAPER selection, or LIVE authorization.
	SettledEventClusters       int     `json:"settled_canonical_event_clusters"`
	OpenCanonicalEventClusters int     `json:"open_canonical_event_clusters"`
	ClusteredSettledMarkets    int     `json:"clustered_settled_markets"`
	UnclusteredSettledMarkets  int     `json:"unclustered_settled_markets"`
	EventClusterMeanPC         float64 `json:"event_cluster_mean_pc"`
	EventClusterSDPC           float64 `json:"event_cluster_sd_pc"`
	EventClusterMeanAsk        float64 `json:"event_cluster_mean_ask"`
	EventClusterMeanFeePC      float64 `json:"event_cluster_mean_fee_pc"`
	MeanPC                     float64 `json:"mean_pc"`
	SDPC                       float64 `json:"sd_pc"`
	ReturnPerDollar            float64 `json:"return_per_dollar"`
	CapitalDay                 float64 `json:"capital_day"`
	MeanAsk                    float64 `json:"mean_ask"`
	MeanFeePC                  float64 `json:"mean_fee_pc"`
	DepthKnownShare            float64 `json:"depth_known_share"`
}

// UnitTrialLeaderboardStat is the bankroll-free profit-rate view of the prospective executable
// lane. One row means "buy one share of every observed episode"; no portfolio balance, sizing,
// allocation, or hypothetical fill rate is introduced here.
type UnitTrialLeaderboardStat struct {
	Family                     string   `json:"family"`
	Platform                   string   `json:"platform"`
	OriginLayer                string   `json:"origin_layer"`
	Side                       string   `json:"side"`
	Route                      string   `json:"route,omitempty"`
	ProducerFamily             string   `json:"producer_family,omitempty"`
	RelationClass              string   `json:"relation_class,omitempty"`
	ExperimentEpoch            string   `json:"experiment_epoch,omitempty"`
	EconomicSource             string   `json:"economic_source,omitempty"`
	EconomicUnit               string   `json:"economic_unit,omitempty"`
	EconomicsScope             string   `json:"economics_scope,omitempty"`
	N                          int      `json:"n"` // raw settled route rows; SettledMarkets is the distinct-contract display sample
	Open                       int      `json:"open"`
	Total                      int      `json:"total"`
	UniqueMarkets              int      `json:"unique_markets"`
	SettledMarkets             int      `json:"settled_unique_markets"`
	OpenMarkets                int      `json:"open_unique_markets"`
	SettledEventClusters       int      `json:"settled_canonical_event_clusters"`
	OpenCanonicalEventClusters int      `json:"open_canonical_event_clusters"`
	ClusteredSettledMarkets    int      `json:"clustered_settled_markets"`
	UnclusteredSettledMarkets  int      `json:"unclustered_settled_markets"`
	EventClusterMeanPC         float64  `json:"event_cluster_mean_pc"`
	EventClusterSDPC           float64  `json:"event_cluster_sd_pc"`
	EventClusterMeanAsk        float64  `json:"event_cluster_mean_ask"`
	EventClusterMeanFeePC      float64  `json:"event_cluster_mean_fee_pc"`
	FirstOpened                string   `json:"first_opened"`
	LastActivity               string   `json:"last_activity"`
	TrackedSeconds             float64  `json:"tracked_seconds"`
	TrackedDays                float64  `json:"tracked_days"`
	OpportunitiesPerDay        float64  `json:"opportunities_per_day"`
	SettledPerDay              float64  `json:"settled_per_day"`
	MeanPC                     float64  `json:"mean_pc"`
	SDPC                       float64  `json:"sd_pc"`
	TotalPnL                   float64  `json:"total_pnl"`
	NetPerCalendarDay          float64  `json:"net_per_calendar_day"`
	NetPerOccupiedShareDay     float64  `json:"net_per_occupied_share_day"`
	ReturnPerDollarDay         float64  `json:"return_per_dollar_day"`
	OccupiedShareSeconds       float64  `json:"occupied_share_seconds"`
	OccupiedShareDays          float64  `json:"occupied_share_days"`
	EntryDollarSeconds         float64  `json:"entry_dollar_seconds"`
	EntryDollarDays            float64  `json:"entry_dollar_days"`
	MeanAsk                    float64  `json:"mean_ask"`
	MeanFeePC                  float64  `json:"mean_fee_pc"`
	ProofMeanAsk               float64  `json:"proof_30d_mean_ask"`
	ProofMeanFeePC             float64  `json:"proof_30d_mean_fee_pc"`
	DepthKnownShare            float64  `json:"depth_known_share"`
	MatureUTCBlocks            int      `json:"mature_utc_blocks"`
	ObservedUTCBlocks          int      `json:"observed_utc_blocks"`
	EventDayClusters           int      `json:"event_day_clusters"`
	DayMeanPC                  float64  `json:"day_cluster_mean_pc"`
	DayMeanLoPC                *float64 `json:"day_cluster_mean_lo_pc,omitempty"`
	DayMeanHiPC                *float64 `json:"day_cluster_mean_hi_pc,omitempty"`
	ClusterNetPerDayLo         *float64 `json:"cluster_net_per_calendar_day_lo,omitempty"`
	ClusterNetPerDayHi         *float64 `json:"cluster_net_per_calendar_day_hi,omitempty"`
}

func unitTrialOriginLayer(quoteSource string) string {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(quoteSource)), "strategy-decision/") {
		return "strategy"
	}
	return "model"
}

// unitTrialElapsedSeconds keeps the clock exact while preventing divide-by-zero for two events
// observed in the same timestamp quantum. It deliberately does not round short holds up to an
// hour or young histories up to a day.
func unitTrialElapsedSeconds(end, start time.Time) float64 {
	seconds := end.Sub(start).Seconds()
	if seconds < 1 {
		return 1
	}
	return seconds
}

func unitTrialMeanSD(values []float64) (n int, mean, sd float64) {
	n = len(values)
	if n == 0 {
		return
	}
	for _, value := range values {
		mean += value
	}
	mean /= float64(n)
	if n > 1 {
		for _, value := range values {
			sd += (value - mean) * (value - mean)
		}
		sd = math.Sqrt(sd / float64(n-1))
	}
	return
}

type unitTrialLeaderboardAgg struct {
	stat            UnitTrialLeaderboardStat
	depthKnown      int
	first, last     time.Time
	dayEvents       map[string]map[string][]float64
	dayAsk, dayFee  map[string]float64
	dayN            map[string]int
	markets         map[string]struct{}
	settledMarkets  map[string]struct{}
	openMarkets     map[string]struct{}
	marketPnL       map[string]*unitTrialMarketOutcome
	proofEvents     map[string]map[string]*unitTrialProofMarketOutcome
	openProofEvents map[string]struct{}
	proofFirst      time.Time
}

// unitTrialMarketOutcome is the contract-level cell for one route and ticker. A route may observe
// or execute the same contract more than once, so retries are averaged here. Canonical event
// clustering happens separately because multiple distinct contracts can still share one outcome.
type unitTrialMarketOutcome struct {
	pnlSum, askSum, feeSum float64
	n                      int
}

// unitTrialProofMarketOutcome is one frozen contract cell inside a canonical event. Repeated
// episodes of the same contract are averaged first; contracts are then averaged inside the event.
// This prevents a game with many spreads/props (or many retries) from manufacturing sample size.
type unitTrialProofMarketOutcome struct {
	pnlSum, askSum, feeSum float64
	n                      int
	first                  time.Time
}

type unitTrialEventClusterCount struct {
	clusters, openClusters, clusteredMarkets, unclustered int
	mean, sd, meanAsk, meanFee                            float64
}

func unitTrialStatKey(family, platform, origin, side string) string {
	if origin != "strategy" {
		origin = "model"
	}
	return strings.TrimSpace(family) + "\x00" + strings.ToLower(strings.TrimSpace(platform)) + "\x00" +
		origin + "\x00" + strings.ToUpper(strings.TrimSpace(side))
}

// unitTrialEventClusterCounts reports only the identity frozen on the economic observation. It
// intentionally does not join market_game, market_catalog, or the current canonical registry:
// doing so would silently backfill old outcomes with information learned after settlement.
func (s *Store) unitTrialEventClusterCounts(ctx context.Context, now time.Time) (map[string]unitTrialEventClusterCount, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	asOf := now.UTC().Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
WITH eligible_trials AS (
 SELECT *,
        CASE WHEN settled=1 AND pnl_pc IS NOT NULL AND TRIM(closed_ts)!=''
                   AND julianday(closed_ts) IS NOT NULL
                   AND julianday(closed_ts)>=julianday(opened_ts)
                   AND julianday(closed_ts)<=julianday(?)
             THEN 1 ELSE 0 END terminal
 FROM unit_trials
 WHERE depth>=1 AND fee_known=1 AND TRIM(fee_source)!='' AND TRIM(ticker)!=''
   AND julianday(opened_ts) IS NOT NULL AND julianday(opened_ts)<=julianday(?)
), market_cells AS (
 SELECT family,LOWER(TRIM(platform)) platform,
        CASE WHEN origin_layer='strategy' THEN 'strategy' ELSE 'model' END origin,
        UPPER(TRIM(side)) side,TRIM(ticker) ticker,
        CASE WHEN event_version>0 AND TRIM(canonical_event_id)!=''
             THEN TRIM(canonical_event_id) ELSE '' END event_id,
        AVG(pnl_pc) market_mean_pc,AVG(ask) market_mean_ask,AVG(fee_pc) market_mean_fee
 FROM eligible_trials
 WHERE terminal=1
 GROUP BY family,platform,origin,side,ticker,event_id
), event_cells AS (
 SELECT family,platform,origin,side,event_id,
        COUNT(*) clustered_markets,AVG(market_mean_pc) event_mean_pc,
        AVG(market_mean_ask) event_mean_ask,AVG(market_mean_fee) event_mean_fee
 FROM market_cells WHERE event_id!=''
 GROUP BY family,platform,origin,side,event_id
), known_groups AS (
 SELECT family,platform,origin,side,COUNT(*) clusters,
        SUM(clustered_markets) clustered_markets,
        AVG(event_mean_pc) event_mean_pc,
        AVG(event_mean_pc*event_mean_pc) event_ex2,
        AVG(event_mean_ask) event_mean_ask,AVG(event_mean_fee) event_mean_fee
 FROM event_cells GROUP BY family,platform,origin,side
), unknown_groups AS (
 SELECT family,platform,origin,side,COUNT(*) unclustered_markets
 FROM market_cells WHERE event_id=''
 GROUP BY family,platform,origin,side
), open_groups AS (
 SELECT family,LOWER(TRIM(platform)) platform,
        CASE WHEN origin_layer='strategy' THEN 'strategy' ELSE 'model' END origin,
        UPPER(TRIM(side)) side,COUNT(DISTINCT TRIM(canonical_event_id)) open_clusters
 FROM eligible_trials
 WHERE event_version>0 AND TRIM(canonical_event_id)!='' AND terminal=0
 GROUP BY family,platform,origin,side
), all_groups AS (
 SELECT family,platform,origin,side FROM known_groups
 UNION
 SELECT family,platform,origin,side FROM unknown_groups
 UNION
 SELECT family,platform,origin,side FROM open_groups
)
SELECT g.family,g.platform,g.origin,g.side,
       COALESCE(k.clusters,0),COALESCE(o.open_clusters,0),COALESCE(k.clustered_markets,0),
       COALESCE(u.unclustered_markets,0),k.event_mean_pc,k.event_ex2,
       k.event_mean_ask,k.event_mean_fee
FROM all_groups g
LEFT JOIN known_groups k USING(family,platform,origin,side)
LEFT JOIN unknown_groups u USING(family,platform,origin,side)
LEFT JOIN open_groups o USING(family,platform,origin,side)`, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]unitTrialEventClusterCount)
	for rows.Next() {
		var family, platform, origin, side string
		var count unitTrialEventClusterCount
		var mean, ex2, meanAsk, meanFee sql.NullFloat64
		if err := rows.Scan(&family, &platform, &origin, &side, &count.clusters,
			&count.openClusters, &count.clusteredMarkets, &count.unclustered, &mean, &ex2,
			&meanAsk, &meanFee); err != nil {
			return nil, err
		}
		count.mean = mean.Float64
		count.meanAsk, count.meanFee = meanAsk.Float64, meanFee.Float64
		if variance := ex2.Float64 - count.mean*count.mean; variance > 0 && count.clusters > 1 {
			count.sd = math.Sqrt(variance * float64(count.clusters) / float64(count.clusters-1))
		}
		out[unitTrialStatKey(family, platform, origin, side)] = count
	}
	return out, rows.Err()
}

// unitTrialDayBounds is deliberately deterministic. Resampling whole UTC-day values keeps every
// same-day/event burst together instead of pretending correlated sub-bets are independent rows.
func unitTrialDayBounds(values []float64, key string) (lo, hi float64, ok bool) {
	if len(values) < 2 {
		return 0, 0, false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	x := h.Sum64()
	if x == 0 {
		x = 1
	}
	const reps = 2048
	means := make([]float64, reps)
	for b := range means {
		sum := 0.0
		for range values {
			x = x*6364136223846793005 + 1442695040888963407
			sum += values[int(x%uint64(len(values)))]
		}
		means[b] = sum / float64(len(values))
	}
	sort.Float64s(means)
	return means[reps/20], means[reps*19/20], true
}

func unitTrialUTCDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func parseUnitTrialTime(v string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

// UnitTrialLeaderboard returns the exact one-share profit-rate history from unit_trials. The
// calendar denominator includes zero-opportunity time from the family's first observation through
// now. Both the calendar and occupied-capital clocks are measured to the second, with only a
// one-second divide-by-zero guard. Ratio-of-sums is deliberate: averaging per-row ROI/day would let
// tiny-cost, very-short rows dominate the answer.
func (s *Store) UnitTrialLeaderboard(ctx context.Context, now time.Time) ([]UnitTrialLeaderboardStat, error) {
	return s.unitTrialLeaderboard(ctx, now, "")
}

// UnitTrialDigest returns the exact point-rate fields needed by the compact operator UI without
// transferring every underlying trial into Go or computing bootstrap proof. Full leaderboard and
// promotion readers still use UnitTrialLeaderboard; this aggregate can never authorize money.
func (s *Store) UnitTrialDigest(ctx context.Context, now time.Time) ([]UnitTrialLeaderboardStat, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	clusterCounts, err := s.unitTrialEventClusterCounts(ctx, now)
	if err != nil {
		return nil, err
	}
	asOf := now.Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
WITH eligible_trials AS (
  SELECT *,
         CASE WHEN settled=1 AND pnl_pc IS NOT NULL AND TRIM(closed_ts)!=''
                    AND julianday(closed_ts) IS NOT NULL
                    AND julianday(closed_ts)>=julianday(opened_ts)
                    AND julianday(closed_ts)<=julianday(?)
              THEN 1 ELSE 0 END terminal
  FROM unit_trials
  WHERE depth>=1 AND fee_known=1 AND TRIM(fee_source)!=''
    AND julianday(opened_ts) IS NOT NULL AND julianday(opened_ts)<=julianday(?)
), market_cells AS (
  SELECT family,platform,
         CASE WHEN origin_layer='strategy' THEN 'strategy' ELSE 'model' END origin,
         UPPER(TRIM(side)) side,TRIM(ticker) ticker,
         COUNT(*) total_rows,
         SUM(terminal) settled_rows,
         SUM(CASE WHEN terminal=0 THEN 1 ELSE 0 END) open_rows,
         SUM(CASE WHEN terminal=1 THEN pnl_pc ELSE 0 END) pnl_sum,
         AVG(CASE WHEN terminal=1 THEN pnl_pc END) market_mean_pnl,
	     AVG(CASE WHEN terminal=1 THEN ask END) market_mean_ask,
	     AVG(CASE WHEN terminal=1 THEN fee_pc END) market_mean_fee,
         MIN(opened_ts) first_opened,
         MAX(CASE WHEN terminal=1 THEN closed_ts ELSE opened_ts END) last_activity
  FROM eligible_trials
  GROUP BY family,platform,origin,side,TRIM(ticker)
)
SELECT family,platform,origin,side,
       SUM(total_rows) total,
       SUM(settled_rows) n,
       SUM(open_rows) open_n,
       SUM(CASE WHEN ticker!='' THEN 1 ELSE 0 END) unique_markets,
       SUM(CASE WHEN ticker!='' AND settled_rows>0 THEN 1 ELSE 0 END) settled_markets,
       SUM(CASE WHEN ticker!='' AND open_rows>0 THEN 1 ELSE 0 END) open_markets,
       COALESCE(SUM(pnl_sum),0) total_pnl,
       COALESCE(AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_mean_pnl END),0) mean_pnl,
	   COALESCE(AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_mean_ask END),0) mean_ask,
	   COALESCE(AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_mean_fee END),0) mean_fee,
       MIN(first_opened) first_opened,
       MAX(last_activity) last_activity
FROM market_cells
GROUP BY family,platform,origin,side`, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UnitTrialLeaderboardStat
	for rows.Next() {
		var v UnitTrialLeaderboardStat
		var firstS, lastS string
		if err := rows.Scan(&v.Family, &v.Platform, &v.OriginLayer, &v.Side, &v.Total, &v.N,
			&v.Open, &v.UniqueMarkets, &v.SettledMarkets, &v.OpenMarkets,
			&v.TotalPnL, &v.MeanPC, &v.MeanAsk, &v.MeanFeePC, &firstS, &lastS); err != nil {
			return nil, err
		}
		first, ok := parseUnitTrialTime(firstS)
		if !ok || first.After(now) {
			continue
		}
		last, ok := parseUnitTrialTime(lastS)
		if !ok {
			last = first
		}
		v.FirstOpened, v.LastActivity = first.Format(time.RFC3339Nano), last.Format(time.RFC3339Nano)
		v.TrackedSeconds = unitTrialElapsedSeconds(now, first)
		v.TrackedDays = v.TrackedSeconds / (24 * 60 * 60)
		v.NetPerCalendarDay = v.TotalPnL / v.TrackedDays
		v.OpportunitiesPerDay = float64(v.Total) / v.TrackedDays
		v.SettledPerDay = float64(v.N) / v.TrackedDays
		if count, ok := clusterCounts[unitTrialStatKey(v.Family, v.Platform, v.OriginLayer, v.Side)]; ok {
			v.SettledEventClusters = count.clusters
			v.OpenCanonicalEventClusters = count.openClusters
			v.ClusteredSettledMarkets = count.clusteredMarkets
			v.UnclusteredSettledMarkets = count.unclustered
			v.EventClusterMeanPC = count.mean
			v.EventClusterSDPC = count.sd
			v.EventClusterMeanAsk = count.meanAsk
			v.EventClusterMeanFeePC = count.meanFee
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NetPerCalendarDay == out[j].NetPerCalendarDay {
			return out[i].Family+out[i].Platform+out[i].OriginLayer < out[j].Family+out[j].Platform+out[j].OriginLayer
		}
		return out[i].NetPerCalendarDay > out[j].NetPerCalendarDay
	})
	return out, nil
}

// UnitTrialRouteLeaderboard computes one exact proof lane without rebuilding every family. LIVE
// uses this targeted reader so frequent candidate checks cannot create a CPU/SQLite scan flood.
func (s *Store) UnitTrialRouteLeaderboard(ctx context.Context, now time.Time, family, platform, originLayer, side string) (UnitTrialLeaderboardStat, bool, error) {
	rows, err := s.unitTrialLeaderboard(ctx, now,
		` AND family=? AND LOWER(TRIM(platform))=LOWER(TRIM(?)) AND origin_layer=? AND UPPER(TRIM(side))=UPPER(TRIM(?))`,
		family, platform, originLayer, side)
	if err != nil || len(rows) == 0 {
		return UnitTrialLeaderboardStat{}, false, err
	}
	return rows[0], true, nil
}

func (s *Store) unitTrialLeaderboard(ctx context.Context, now time.Time, filter string, args ...any) ([]UnitTrialLeaderboardStat, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	query := `
SELECT opened_ts,closed_ts,family,platform,origin_layer,ticker,side,ask,fee_pc,depth,settled,pnl_pc,
       canonical_event_id,event_version
FROM unit_trials
WHERE depth>=1 AND fee_known=1 AND TRIM(fee_source)!=''` + filter + `
ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	aggs := map[string]*unitTrialLeaderboardAgg{}
	for rows.Next() {
		var openedS, closedS, family, platform, originLayer, ticker, side, canonicalEventID string
		var ask, fee, depth float64
		var settled, eventVersion int
		var pnl sql.NullFloat64
		if err := rows.Scan(&openedS, &closedS, &family, &platform, &originLayer, &ticker, &side,
			&ask, &fee, &depth, &settled, &pnl, &canonicalEventID, &eventVersion); err != nil {
			return nil, err
		}
		opened, ok := parseUnitTrialTime(openedS)
		if !ok || opened.After(now) {
			// A future opening cannot contribute a fabricated one-second history or realized edge.
			continue
		}
		if originLayer != "strategy" {
			originLayer = "model"
		}
		side = strings.ToUpper(strings.TrimSpace(side))
		key := family + "\x00" + platform + "\x00" + originLayer + "\x00" + side
		a := aggs[key]
		if a == nil {
			a = &unitTrialLeaderboardAgg{dayEvents: make(map[string]map[string][]float64),
				dayAsk: make(map[string]float64), dayFee: make(map[string]float64), dayN: make(map[string]int),
				markets: make(map[string]struct{}), settledMarkets: make(map[string]struct{}),
				openMarkets: make(map[string]struct{}), marketPnL: make(map[string]*unitTrialMarketOutcome),
				proofEvents:     make(map[string]map[string]*unitTrialProofMarketOutcome),
				openProofEvents: make(map[string]struct{})}
			a.stat.Family, a.stat.Platform, a.stat.OriginLayer, a.stat.Side = family, platform, originLayer, side
			a.first, a.last = opened, opened
			aggs[key] = a
		}
		a.stat.Total++
		marketKey := strings.TrimSpace(ticker)
		eventKey := strings.TrimSpace(canonicalEventID)
		validEvent := eventVersion > 0 && eventKey != "" && marketKey != ""
		if marketKey != "" {
			a.markets[marketKey] = struct{}{}
		}
		if depth > 0 {
			a.depthKnown++
		}
		if opened.Before(a.first) {
			a.first = opened
		}
		if opened.After(a.last) {
			a.last = opened
		}
		if settled == 0 || !pnl.Valid {
			a.stat.Open++
			if marketKey != "" {
				a.openMarkets[marketKey] = struct{}{}
			}
			if validEvent {
				a.openProofEvents[eventKey] = struct{}{}
			}
			holdSeconds := unitTrialElapsedSeconds(now, opened)
			a.stat.OccupiedShareSeconds += holdSeconds
			a.stat.EntryDollarSeconds += (ask + fee) * holdSeconds
			continue
		}
		closed, ok := parseUnitTrialTime(closedS)
		if !ok || closed.Before(opened) || closed.After(now) {
			// A settlement without a valid close clock is censored, not a zero-duration win. Keep
			// its capital occupied through as-of and exclude its outcome from realized evidence.
			// The same rule covers a close timestamp that is in the future relative to as-of.
			a.stat.Open++
			if marketKey != "" {
				a.openMarkets[marketKey] = struct{}{}
			}
			if validEvent {
				a.openProofEvents[eventKey] = struct{}{}
			}
			holdSeconds := unitTrialElapsedSeconds(now, opened)
			a.stat.OccupiedShareSeconds += holdSeconds
			a.stat.EntryDollarSeconds += (ask + fee) * holdSeconds
			continue
		}
		if closed.After(a.last) {
			a.last = closed
		}
		a.stat.N++
		if marketKey != "" {
			a.settledMarkets[marketKey] = struct{}{}
			cell := a.marketPnL[marketKey]
			if cell == nil {
				cell = &unitTrialMarketOutcome{}
				a.marketPnL[marketKey] = cell
			}
			cell.pnlSum += pnl.Float64
			cell.askSum += ask
			cell.feeSum += fee
			cell.n++
		}
		x := pnl.Float64
		a.stat.TotalPnL += x
		if validEvent {
			markets := a.proofEvents[eventKey]
			if markets == nil {
				markets = make(map[string]*unitTrialProofMarketOutcome)
				a.proofEvents[eventKey] = markets
			}
			cell := markets[marketKey]
			if cell == nil {
				cell = &unitTrialProofMarketOutcome{first: opened}
				markets[marketKey] = cell
			}
			cell.pnlSum += x
			cell.askSum += ask
			cell.feeSum += fee
			cell.n++
			if cell.first.IsZero() || opened.Before(cell.first) {
				cell.first = opened
			}
			if a.proofFirst.IsZero() || opened.Before(a.proofFirst) {
				a.proofFirst = opened
			}
		}
		holdSeconds := unitTrialElapsedSeconds(closed, opened)
		a.stat.OccupiedShareSeconds += holdSeconds
		a.stat.EntryDollarSeconds += (ask + fee) * holdSeconds
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]UnitTrialLeaderboardStat, 0, len(aggs))
	for _, a := range aggs {
		a.stat.FirstOpened = a.first.Format(time.RFC3339Nano)
		a.stat.LastActivity = a.last.Format(time.RFC3339Nano)
		a.stat.TrackedSeconds = unitTrialElapsedSeconds(now, a.first)
		a.stat.TrackedDays = a.stat.TrackedSeconds / (24 * 60 * 60)
		a.stat.OccupiedShareDays = a.stat.OccupiedShareSeconds / (24 * 60 * 60)
		a.stat.EntryDollarDays = a.stat.EntryDollarSeconds / (24 * 60 * 60)
		a.stat.UniqueMarkets = len(a.markets)
		a.stat.SettledMarkets = len(a.settledMarkets)
		a.stat.OpenMarkets = len(a.openMarkets)
		a.stat.OpportunitiesPerDay = float64(a.stat.Total) / a.stat.TrackedDays
		if a.stat.Total > 0 {
			a.stat.DepthKnownShare = float64(a.depthKnown) / float64(a.stat.Total)
		}
		a.stat.SettledPerDay = float64(a.stat.N) / a.stat.TrackedDays
		marketMeans := make([]float64, 0, len(a.marketPnL))
		marketAsks := make([]float64, 0, len(a.marketPnL))
		marketFees := make([]float64, 0, len(a.marketPnL))
		for _, cell := range a.marketPnL {
			if cell != nil && cell.n > 0 {
				marketMeans = append(marketMeans, cell.pnlSum/float64(cell.n))
				marketAsk := cell.askSum / float64(cell.n)
				marketFee := cell.feeSum / float64(cell.n)
				marketAsks = append(marketAsks, marketAsk)
				marketFees = append(marketFees, marketFee)
			}
		}
		_, a.stat.MeanPC, a.stat.SDPC = unitTrialMeanSD(marketMeans)
		_, a.stat.MeanAsk, _ = unitTrialMeanSD(marketAsks)
		_, a.stat.MeanFeePC, _ = unitTrialMeanSD(marketFees)
		if len(marketMeans) != a.stat.SettledMarkets {
			// Defensive only: blank tickers are excluded from both maps and proof, while every
			// named market must contribute exactly one market-level observation.
			a.stat.SettledMarkets = len(marketMeans)
		}
		a.stat.NetPerCalendarDay = a.stat.TotalPnL / a.stat.TrackedDays
		if a.stat.OccupiedShareDays > 0 {
			a.stat.NetPerOccupiedShareDay = a.stat.TotalPnL / a.stat.OccupiedShareDays
		}
		if a.stat.EntryDollarDays > 0 {
			a.stat.ReturnPerDollarDay = a.stat.TotalPnL / a.stat.EntryDollarDays
		}
		// Build proof from the immutable event identity stored at entry. Retries are averaged inside
		// a contract, contracts inside an event, and each event enters exactly one UTC day. Raw legacy
		// contracts remain in the PAPER point fields above but can never enter these proof fields.
		clusteredMarkets := make(map[string]struct{})
		eventMeans := make([]float64, 0, len(a.proofEvents))
		eventAsks := make([]float64, 0, len(a.proofEvents))
		eventFees := make([]float64, 0, len(a.proofEvents))
		for eventID, markets := range a.proofEvents {
			eventPnL, eventAsk, eventFee, marketN := 0.0, 0.0, 0.0, 0
			var eventFirst time.Time
			for marketID, cell := range markets {
				if cell == nil || cell.n <= 0 {
					continue
				}
				clusteredMarkets[marketID] = struct{}{}
				eventPnL += cell.pnlSum / float64(cell.n)
				eventAsk += cell.askSum / float64(cell.n)
				eventFee += cell.feeSum / float64(cell.n)
				marketN++
				if eventFirst.IsZero() || cell.first.Before(eventFirst) {
					eventFirst = cell.first
				}
			}
			if marketN == 0 || eventFirst.IsZero() {
				continue
			}
			eventPnL /= float64(marketN)
			eventAsk /= float64(marketN)
			eventFee /= float64(marketN)
			eventMeans = append(eventMeans, eventPnL)
			eventAsks = append(eventAsks, eventAsk)
			eventFees = append(eventFees, eventFee)
			day := unitTrialUTCDay(eventFirst).Format("2006-01-02")
			if a.dayEvents[day] == nil {
				a.dayEvents[day] = make(map[string][]float64)
			}
			a.dayEvents[day][eventID] = []float64{eventPnL}
			a.dayAsk[day] += eventAsk
			a.dayFee[day] += eventFee
			a.dayN[day]++
		}
		a.stat.SettledEventClusters, a.stat.EventClusterMeanPC, a.stat.EventClusterSDPC = unitTrialMeanSD(eventMeans)
		a.stat.OpenCanonicalEventClusters = len(a.openProofEvents)
		a.stat.ClusteredSettledMarkets = len(clusteredMarkets)
		for marketID := range a.settledMarkets {
			if _, ok := clusteredMarkets[marketID]; !ok {
				a.stat.UnclusteredSettledMarkets++
			}
		}
		_, a.stat.EventClusterMeanAsk, _ = unitTrialMeanSD(eventAsks)
		_, a.stat.EventClusterMeanFeePC, _ = unitTrialMeanSD(eventFees)
		// Only completed UTC days enter proof. The first observation day is included only when
		// collection began exactly at midnight; the current partial UTC day is never mature.
		if !a.proofFirst.IsZero() {
			coverageStart := unitTrialUTCDay(a.proofFirst)
			if !a.proofFirst.Equal(coverageStart) {
				coverageStart = coverageStart.AddDate(0, 0, 1)
			}
			currentDay := unitTrialUTCDay(now)
			proofStart := currentDay.AddDate(0, 0, -30)
			if coverageStart.After(proofStart) {
				proofStart = coverageStart
			}
			if currentDay.After(proofStart) {
				a.stat.MatureUTCBlocks = int(currentDay.Sub(proofStart).Hours() / 24)
			}
			activeDayMeans := make([]float64, 0, a.stat.MatureUTCBlocks)
			calendarDayNet := make([]float64, 0, a.stat.MatureUTCBlocks)
			proofAsk, proofFee, proofN := 0.0, 0.0, 0
			for d := proofStart; d.Before(currentDay); d = d.AddDate(0, 0, 1) {
				dayKey := d.Format("2006-01-02")
				events := a.dayEvents[dayKey]
				dayNet, dayEventMean := 0.0, 0.0
				for _, xs := range events {
					es := 0.0
					for _, x := range xs {
						es += x
						dayNet += x
					}
					dayEventMean += es / float64(len(xs))
					a.stat.EventDayClusters++
				}
				calendarDayNet = append(calendarDayNet, dayNet)
				if len(events) > 0 {
					activeDayMeans = append(activeDayMeans, dayEventMean/float64(len(events)))
					a.stat.ObservedUTCBlocks++
				}
				proofAsk += a.dayAsk[dayKey]
				proofFee += a.dayFee[dayKey]
				proofN += a.dayN[dayKey]
			}
			if proofN > 0 {
				a.stat.ProofMeanAsk = proofAsk / float64(proofN)
				a.stat.ProofMeanFeePC = proofFee / float64(proofN)
			}
			if len(activeDayMeans) > 0 {
				for _, x := range activeDayMeans {
					a.stat.DayMeanPC += x
				}
				a.stat.DayMeanPC /= float64(len(activeDayMeans))
			}
			clusterKey := a.stat.Family + "|" + a.stat.Platform + "|" + a.stat.OriginLayer + "|" + a.stat.Side
			if lo, hi, ok := unitTrialDayBounds(activeDayMeans, clusterKey+"|edge"); ok {
				a.stat.DayMeanLoPC, a.stat.DayMeanHiPC = &lo, &hi
			}
			if lo, hi, ok := unitTrialDayBounds(calendarDayNet, clusterKey+"|rate"); ok {
				a.stat.ClusterNetPerDayLo, a.stat.ClusterNetPerDayHi = &lo, &hi
			}
		}
		out = append(out, a.stat)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NetPerCalendarDay == out[j].NetPerCalendarDay {
			return out[i].Family+out[i].Platform+out[i].OriginLayer < out[j].Family+out[j].Platform+out[j].OriginLayer
		}
		return out[i].NetPerCalendarDay > out[j].NetPerCalendarDay
	})
	return out, nil
}

// UnitTrialStats returns fee-inclusive outcomes in all three requested comparison units as of the
// current clock. Call UnitTrialStatsAt when several reports must share one frozen as-of boundary.
func (s *Store) UnitTrialStats(ctx context.Context) ([]UnitTrialStat, error) {
	return s.UnitTrialStatsAt(ctx, time.Now().UTC())
}

// UnitTrialExecutionStatsAt is the decision-path form of UnitTrialStatsAt. It computes the exact
// venue+ticker contract cells used by Paper routing and the compact system snapshot, but deliberately
// skips the separate canonical-event diagnostic query. Relationship clusters are still frozen on
// every row and remain available through UnitTrialStatsAt/UnitTrialLeaderboard; they are not part of
// the operator-defined n and must not be allowed to starve the executable-route roster on a busy DB.
func (s *Store) UnitTrialExecutionStatsAt(ctx context.Context, now time.Time) ([]UnitTrialStat, error) {
	return s.unitTrialStatsAt(ctx, now, false)
}

// UnitTrialStatsAt applies the same settlement-clock censoring as UnitTrialLeaderboard: future
// openings are absent, while missing, pre-open, invalid, or future close clocks remain open and
// cannot contribute realized evidence.
func (s *Store) UnitTrialStatsAt(ctx context.Context, now time.Time) ([]UnitTrialStat, error) {
	return s.unitTrialStatsAt(ctx, now, true)
}

func (s *Store) unitTrialStatsAt(ctx context.Context, now time.Time, includeEventDiagnostics bool) ([]UnitTrialStat, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	var clusterCounts map[string]unitTrialEventClusterCount
	if includeEventDiagnostics {
		var err error
		clusterCounts, err = s.unitTrialEventClusterCounts(ctx, now)
		if err != nil {
			return nil, err
		}
	}
	asOf := now.Format(time.RFC3339Nano)
	rows, err := s.db.QueryContext(ctx, `
WITH eligible_trials AS (
  SELECT *,
         CASE WHEN settled=1 AND pnl_pc IS NOT NULL AND TRIM(closed_ts)!=''
                    AND julianday(closed_ts) IS NOT NULL
                    AND julianday(closed_ts)>=julianday(opened_ts)
                    AND julianday(closed_ts)<=julianday(?)
              THEN 1 ELSE 0 END terminal
  FROM unit_trials
  WHERE depth>=1 AND fee_known=1 AND TRIM(fee_source)!=''
    AND julianday(opened_ts) IS NOT NULL AND julianday(opened_ts)<=julianday(?)
), market_cells AS (
  SELECT family,platform,
         CASE WHEN origin_layer='strategy' THEN 'strategy' ELSE 'model' END origin,
         UPPER(TRIM(side)) side, TRIM(ticker) ticker,
         COUNT(*) total_rows,
         SUM(terminal) settled_rows,
         SUM(CASE WHEN terminal=0 THEN 1 ELSE 0 END) open_rows,
         AVG(CASE WHEN terminal=1 THEN pnl_pc END) market_mean_pc,
         AVG(CASE WHEN terminal=1 THEN return_per_dollar END) market_roi,
	     AVG(CASE WHEN terminal=1 THEN ask END) market_mean_ask,
	     AVG(CASE WHEN terminal=1 THEN fee_pc END) market_mean_fee,
         SUM(CASE WHEN terminal=1 THEN pnl_pc ELSE 0 END) pnl_sum,
         SUM(CASE WHEN terminal=1
                  THEN (ask+fee_pc) *
                       (CASE WHEN (julianday(closed_ts)-julianday(opened_ts))*86400.0 > 1
                             THEN (julianday(closed_ts)-julianday(opened_ts))*86400.0 ELSE 1 END) / 86400.0
                  ELSE 0 END) capital_dollar_days,
         SUM(CASE WHEN depth>0 THEN 1 ELSE 0 END) depth_rows
  FROM eligible_trials
  GROUP BY family,platform,origin,side,TRIM(ticker)
)
SELECT family,platform,origin,side,
       SUM(settled_rows) n,
       SUM(open_rows) open,
       SUM(CASE WHEN ticker!='' THEN 1 ELSE 0 END) unique_markets,
       SUM(CASE WHEN ticker!='' AND settled_rows>0 THEN 1 ELSE 0 END) settled_markets,
       SUM(CASE WHEN ticker!='' AND open_rows>0 THEN 1 ELSE 0 END) open_markets,
       AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_mean_pc END) mean_pc,
       AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_mean_pc*market_mean_pc END) ex2,
       AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_roi END) roi,
       SUM(pnl_sum) / NULLIF(SUM(capital_dollar_days),0) capday,
	   AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_mean_ask END) mean_ask,
	   AVG(CASE WHEN ticker!='' AND settled_rows>0 THEN market_mean_fee END) mean_fee,
       CAST(SUM(depth_rows) AS REAL) / NULLIF(SUM(total_rows),0) depth_share
FROM market_cells
GROUP BY family,platform,origin,side ORDER BY mean_pc DESC`, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UnitTrialStat
	for rows.Next() {
		var v UnitTrialStat
		var mean, ex2, roi, capday, meanAsk, meanFee sql.NullFloat64
		if err := rows.Scan(&v.Family, &v.Platform, &v.OriginLayer, &v.Side, &v.N, &v.Open,
			&v.UniqueMarkets, &v.SettledMarkets, &v.OpenMarkets,
			&mean, &ex2, &roi, &capday, &meanAsk, &meanFee, &v.DepthKnownShare); err != nil {
			return nil, err
		}
		v.MeanPC, v.ReturnPerDollar, v.CapitalDay = mean.Float64, roi.Float64, capday.Float64
		if count, ok := clusterCounts[unitTrialStatKey(v.Family, v.Platform, v.OriginLayer, v.Side)]; ok {
			v.SettledEventClusters = count.clusters
			v.OpenCanonicalEventClusters = count.openClusters
			v.ClusteredSettledMarkets = count.clusteredMarkets
			v.UnclusteredSettledMarkets = count.unclustered
			v.EventClusterMeanPC = count.mean
			v.EventClusterSDPC = count.sd
			v.EventClusterMeanAsk = count.meanAsk
			v.EventClusterMeanFeePC = count.meanFee
		}
		// Open-only groups have no realized cost sample. Publish explicit zeros; N=0 keeps
		// verdict/proof code in COLLECTING and prevents unresolved prices from authorizing LIVE.
		v.MeanAsk, v.MeanFeePC = meanAsk.Float64, meanFee.Float64
		if x := ex2.Float64 - v.MeanPC*v.MeanPC; x > 0 && v.SettledMarkets > 1 {
			v.SDPC = math.Sqrt(x * float64(v.SettledMarkets) / float64(v.SettledMarkets-1))
		}
		out = append(out, v)
	}
	return out, rows.Err()
}
