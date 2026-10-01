package storage

// Source-native inputs for R139's concrete paired research systems. These tables and readers are
// append-only research provenance. They do not grant Paper/LIVE authority and never synthesize a
// canonical event/payoff identity from a title or ticker.

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

func migrateConcreteResearchSchema(db *sql.DB) error {
	_, err := db.Exec(`
CREATE TABLE IF NOT EXISTS research_series_event_map (
 series_ticker TEXT NOT NULL,
 event_ticker TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 source_artifact TEXT NOT NULL,
 source_hash TEXT NOT NULL,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 PRIMARY KEY(series_ticker,event_ticker)
);
CREATE INDEX IF NOT EXISTS idx_rseries_event ON research_series_event_map(series_ticker,observed_ts,event_ticker);
CREATE TRIGGER IF NOT EXISTS research_series_event_map_no_update BEFORE UPDATE ON research_series_event_map BEGIN SELECT RAISE(ABORT,'immutable series event map'); END;
CREATE TRIGGER IF NOT EXISTS research_series_event_map_no_delete BEFORE DELETE ON research_series_event_map BEGIN SELECT RAISE(ABORT,'immutable series event map'); END;

CREATE TABLE IF NOT EXISTS research_credit_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 instrument_id TEXT NOT NULL,
 credit_ts TEXT NOT NULL,
 quantity REAL NOT NULL CHECK(quantity>=0),
 credited_amount REAL,
 realized_pnl REAL,
 settle_px REAL,
 amount_known INTEGER NOT NULL CHECK(amount_known IN (0,1)),
 source_artifact TEXT NOT NULL,
 source_hash TEXT NOT NULL,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(venue,instrument_id,credit_ts,source_hash)
);
CREATE INDEX IF NOT EXISTS idx_rcredit_recent ON research_credit_events(credit_ts DESC,venue,instrument_id);
CREATE TRIGGER IF NOT EXISTS research_credit_events_no_update BEFORE UPDATE ON research_credit_events BEGIN SELECT RAISE(ABORT,'immutable credit event'); END;
CREATE TRIGGER IF NOT EXISTS research_credit_events_no_delete BEFORE DELETE ON research_credit_events BEGIN SELECT RAISE(ABORT,'immutable credit event'); END;`)
	return err
}

func (s *Store) InsertSeriesEventMap(ctx context.Context, series, event string, observed time.Time, source string) (bool, error) {
	series, event, source = strings.TrimSpace(series), strings.TrimSpace(event), strings.TrimSpace(source)
	if series == "" || event == "" || source == "" {
		return false, errors.New("invalid official series event map")
	}
	if observed.IsZero() {
		observed = time.Now().UTC()
	}
	h := sha256.Sum256([]byte(series + "\x00" + event + "\x00" + source))
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_series_event_map(
series_ticker,event_ticker,observed_ts,source_artifact,source_hash,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,0,0,0)`, series, event, observed.UTC().Format(time.RFC3339Nano), source,
		"sha256:"+hex.EncodeToString(h[:]))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type ResearchCreditEvent struct {
	Venue, InstrumentID, SourceArtifact, SourceHash string
	CreditAt                                        time.Time
	Quantity, CreditedAmount, RealizedPnL, SettlePx float64
	AmountKnown                                     bool
}

func (s *Store) InsertResearchCreditEvent(ctx context.Context, v ResearchCreditEvent) (bool, error) {
	v.Venue, v.InstrumentID = strings.ToLower(strings.TrimSpace(v.Venue)), strings.TrimSpace(v.InstrumentID)
	if (v.Venue != "kalshi" && v.Venue != "polyus") || v.InstrumentID == "" || v.CreditAt.IsZero() ||
		v.Quantity < 0 || strings.TrimSpace(v.SourceArtifact) == "" || strings.TrimSpace(v.SourceHash) == "" ||
		math.IsNaN(v.CreditedAmount) || math.IsInf(v.CreditedAmount, 0) || math.IsNaN(v.RealizedPnL) ||
		math.IsInf(v.RealizedPnL, 0) || math.IsNaN(v.SettlePx) || math.IsInf(v.SettlePx, 0) {
		return false, errors.New("invalid authoritative credit event")
	}
	var amount any
	if v.AmountKnown {
		if v.CreditedAmount < 0 {
			return false, errors.New("negative credited amount")
		}
		amount = v.CreditedAmount
	}
	var pnl, settle any
	if v.RealizedPnL != 0 {
		pnl = v.RealizedPnL
	}
	if v.SettlePx >= 0 && v.SettlePx <= 1 {
		settle = v.SettlePx
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_credit_events(
venue,instrument_id,credit_ts,quantity,credited_amount,realized_pnl,settle_px,amount_known,
source_artifact,source_hash,funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,0,0,0)`,
		v.Venue, v.InstrumentID, v.CreditAt.UTC().Format(time.RFC3339Nano), v.Quantity, amount,
		pnl, settle, boolInt(v.AmountKnown), v.SourceArtifact, v.SourceHash)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) RecentResearchCreditEvents(ctx context.Context, since time.Time, limit int) ([]ResearchCreditEvent, error) {
	if limit <= 0 || limit > 250 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `SELECT venue,instrument_id,credit_ts,quantity,
credited_amount,realized_pnl,settle_px,amount_known,source_artifact,source_hash
FROM research_credit_events WHERE credit_ts>=? ORDER BY credit_ts DESC,id DESC LIMIT ?`,
		since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ResearchCreditEvent
	for rows.Next() {
		var v ResearchCreditEvent
		var ts string
		var amount, pnl, settle sql.NullFloat64
		var known int
		if err := rows.Scan(&v.Venue, &v.InstrumentID, &ts, &v.Quantity, &amount, &pnl, &settle,
			&known, &v.SourceArtifact, &v.SourceHash); err != nil {
			return nil, err
		}
		v.CreditAt, _ = time.Parse(time.RFC3339Nano, ts)
		v.AmountKnown, v.CreditedAmount = known != 0 && amount.Valid, amount.Float64
		v.RealizedPnL, v.SettlePx = pnl.Float64, -1
		if settle.Valid {
			v.SettlePx = settle.Float64
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

type ConcreteSignalTrigger struct {
	ID                                                       int64
	Observed                                                 time.Time
	Platform, Ticker, Side, Family, Slot, Category           string
	BookSource, FeeSource, PricingVersion, LabelVersion      string
	BookBid, BookAsk, BidDepth, AskDepth, QuoteAge           float64
	MakerTick, TakerTick, MakerFee, TakerFee, LatencyMS      float64
	Strength, Notional, Concentration, Imbalance, Momentum   float64
	FlowRatio, FlowN, HoldersHHI, HoldersSkill, ResolveHours float64
}

type CurrentCanonicalInstrument struct {
	Venue, Ticker, EventID, PayoffID, IdentityStatus string
	EventVersion, PayoffVersion, InstrumentVersion   int
}

func (s *Store) CurrentCanonicalInstrument(ctx context.Context, venue, ticker string) (CurrentCanonicalInstrument, bool, error) {
	var v CurrentCanonicalInstrument
	venue, ticker = strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(ticker)
	err := s.db.QueryRowContext(ctx, `SELECT venue,ticker,event_id,event_version,payoff_id,payoff_version,
version,identity_status FROM research_instrument_specs i WHERE venue=? AND ticker=?
 AND version=(SELECT MAX(v.version) FROM research_instrument_specs v WHERE v.venue=i.venue AND v.ticker=i.ticker)
LIMIT 1`, venue, ticker).Scan(&v.Venue, &v.Ticker, &v.EventID, &v.EventVersion,
		&v.PayoffID, &v.PayoffVersion, &v.InstrumentVersion, &v.IdentityStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return CurrentCanonicalInstrument{}, false, nil
	}
	return v, err == nil, err
}

const concreteSignalCursorPrefix = "r144:concrete-signal-cursor:"

func concreteSignalCursorKey(consumer string) (string, error) {
	consumer = strings.ToLower(strings.TrimSpace(consumer))
	if consumer == "" || strings.ContainsAny(consumer, "\r\n\x00") {
		return "", errors.New("invalid concrete signal consumer")
	}
	return concreteSignalCursorPrefix + consumer, nil
}

// ConcreteSignalCursor returns one consumer's durable processing bookmark. Consumers deliberately
// own separate cursors: paired/inverse experiments and carry experiments must not advance each
// other past a signal they have not evaluated.
func (s *Store) ConcreteSignalCursor(ctx context.Context, consumer string) (int64, error) {
	key, err := concreteSignalCursorKey(consumer)
	if err != nil {
		return 0, err
	}
	var raw string
	err = s.db.QueryRowContext(ctx, `SELECT v FROM kv WHERE k=?`, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	id, parseErr := strconv.ParseInt(raw, 10, 64)
	if parseErr != nil || id < 0 {
		return 0, errors.New("malformed concrete signal cursor")
	}
	return id, nil
}

// AdvanceConcreteSignalCursor is monotone so an overlapping or replayed sweep cannot move a
// consumer backwards. Callers advance only after every durable write for the page has succeeded;
// a crash before this write replays the same immutable IDs into idempotent experiment keys.
func (s *Store) AdvanceConcreteSignalCursor(ctx context.Context, consumer string, id int64) error {
	key, err := concreteSignalCursorKey(consumer)
	if err != nil {
		return err
	}
	if id <= 0 {
		return errors.New("invalid concrete signal cursor advance")
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO kv(k,v,ts) VALUES(?,?,?)
ON CONFLICT(k) DO UPDATE SET v=excluded.v,ts=excluded.ts
WHERE CAST(kv.v AS INTEGER)<CAST(excluded.v AS INTEGER)`, key, strconv.FormatInt(id, 10), nowRFC())
	return err
}

// ConcreteSignalTriggersAfter reads an ID-ordered page of prospective executable-book rows after
// one consumer's cursor. Historical visible prices and rows missing tick/fee/depth/latency are
// excluded rather than repaired. Oldest-first keyset traversal prevents a continuously busy family
// from hiding older signals belonging to quieter families.
func (s *Store) ConcreteSignalTriggersAfter(ctx context.Context, since time.Time, afterID int64, limit int) ([]ConcreteSignalTrigger, error) {
	if afterID < 0 {
		afterID = 0
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,ts,platform,ticker,UPPER(TRIM(side)),signal_type,slot,category,
book_source,pricing_version,label_version,book_bid,book_ask,book_bid_depth,book_ask_depth,
book_quote_age_s,book_maker_tick,book_taker_tick,book_maker_fee_pc,book_taker_fee_pc,book_latency_ms,
COALESCE((SELECT u.fee_source FROM unit_trials u WHERE u.platform=signal_log.platform
 AND u.ticker=signal_log.ticker AND u.side=UPPER(TRIM(signal_log.side))
 AND ABS(julianday(u.opened_ts)-julianday(signal_log.ts))*86400<=10
 ORDER BY u.id DESC LIMIT 1),''),
strength,notional,concentration,imbalance,momentum,COALESCE(flow_ratio_15m,0),COALESCE(flow_n_15m,0),
COALESCE(holders_hhi,0),COALESCE(holders_skill,0),resolve_hours
FROM signal_log WHERE ts>=? AND id>? AND platform IN ('kalshi','polyus') AND resolved=0
 AND book_feature_ver=1 AND pricing_version='book-native-v2' AND book_source!=''
 AND side IN ('YES','NO','Yes','No') AND book_bid>0 AND book_ask>book_bid AND book_ask<1
 AND book_bid_depth>0 AND book_ask_depth>0 AND book_quote_age_s>=0
 AND book_maker_tick>0 AND book_taker_tick>0 AND book_maker_fee_pc IS NOT NULL
 AND book_taker_fee_pc IS NOT NULL AND book_latency_ms IS NOT NULL AND book_latency_ms>=0
ORDER BY id ASC LIMIT ?`, since.UTC().Format(time.RFC3339Nano), afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConcreteSignalTrigger
	for rows.Next() {
		var v ConcreteSignalTrigger
		var ts string
		if err := rows.Scan(&v.ID, &ts, &v.Platform, &v.Ticker, &v.Side, &v.Family, &v.Slot,
			&v.Category, &v.BookSource, &v.PricingVersion, &v.LabelVersion, &v.BookBid, &v.BookAsk,
			&v.BidDepth, &v.AskDepth, &v.QuoteAge, &v.MakerTick, &v.TakerTick, &v.MakerFee,
			&v.TakerFee, &v.LatencyMS, &v.FeeSource, &v.Strength, &v.Notional, &v.Concentration, &v.Imbalance,
			&v.Momentum, &v.FlowRatio, &v.FlowN, &v.HoldersHHI, &v.HoldersSkill,
			&v.ResolveHours); err != nil {
			return nil, err
		}
		v.Observed, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, v)
	}
	return out, rows.Err()
}

// RecentConcreteSignalTriggers is retained for callers that need a bounded initial page. New
// collectors must use ConcreteSignalTriggersAfter with a distinct durable consumer cursor.
func (s *Store) RecentConcreteSignalTriggers(ctx context.Context, since time.Time, limit int) ([]ConcreteSignalTrigger, error) {
	return s.ConcreteSignalTriggersAfter(ctx, since, 0, limit)
}

// ConcreteResearchObservationExists provides the stable replay identity used by the concrete
// collectors. The base evidence table also includes a five-minute observed slot in its uniqueness
// key so general collectors may study repeated clocks. Concrete signal opportunities instead own
// an immutable source ID; replaying one after a crash or slow restart must not manufacture a new n.
func (s *Store) ConcreteResearchObservationExists(ctx context.Context, system, opportunity, route, kind string,
	size float64) (bool, error) {
	var exists int
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM research_system_observations
WHERE system_id=? AND opportunity_id=? AND route=? AND observation_kind=? AND size_units=? LIMIT 1)`,
		strings.TrimSpace(system), strings.TrimSpace(opportunity), strings.ToLower(strings.TrimSpace(route)),
		strings.ToLower(strings.TrimSpace(kind)), size).Scan(&exists)
	return exists != 0, err
}

type ConcreteRuleInput struct {
	Venue, InstrumentID, ArtifactHash, Source, RawRules, SettlementSources string
	Observed                                                               time.Time
}

func (s *Store) RecentConcreteRuleInputs(ctx context.Context, limit int) ([]ConcreteRuleInput, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `SELECT venue,instrument_id,artifact_hash,observed_ts,
source,raw_rules_json,settlement_sources_json FROM research_rule_artifacts a
WHERE venue IN ('kalshi','polyus') AND version=(SELECT MAX(v.version) FROM research_rule_artifacts v
 WHERE v.venue=a.venue AND v.instrument_id=a.instrument_id)
ORDER BY observed_ts DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConcreteRuleInput
	for rows.Next() {
		var v ConcreteRuleInput
		var observed string
		if err := rows.Scan(&v.Venue, &v.InstrumentID, &v.ArtifactHash, &observed, &v.Source,
			&v.RawRules, &v.SettlementSources); err != nil {
			return nil, err
		}
		v.Observed, _ = time.Parse(time.RFC3339Nano, observed)
		out = append(out, v)
	}
	return out, rows.Err()
}

type FrozenPersonaCandidate struct {
	Platform, Ticker, SourceSystem, ForecastSource, ForecastVersion string
	ModelBackend, Calibration, StrategyMode, SelectedTransform      string
	SelectedSide                                                    string
	FreezeStart, FreezeEnd, TrialObserved                           time.Time
	PriorN, BrierN, LogN, SphericalN                                int
	BrierMean, LogMean, SphericalMean                               float64
}

// FrozenPersonaCandidates compares all paper transforms inside each distinct strategy mode using
// only a 30-day window ending 24 hours before evaluation. This prior-window selector remains
// descriptive; only a later frozen untouched replication can promote it.
func (s *Store) FrozenPersonaCandidates(ctx context.Context, now time.Time, limit int) ([]FrozenPersonaCandidate, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	end, start := now.UTC().Add(-24*time.Hour), now.UTC().Add(-31*24*time.Hour)
	rows, err := s.db.QueryContext(ctx, `WITH prior AS (
 SELECT forecast_source,forecast_version,model_backend,calibration,strategy_mode,transform,
        COUNT(*) n,AVG(realized_net) mean_net
 FROM research_proper_score_trials
 WHERE settled=1 AND selected_side!='' AND realized_net IS NOT NULL
   AND observed_ts>=? AND observed_ts<?
 GROUP BY forecast_source,forecast_version,model_backend,calibration,strategy_mode,transform
), paired AS (
 SELECT b.forecast_source,b.forecast_version,b.model_backend,b.calibration,b.strategy_mode,
        b.n bn,b.mean_net bm,l.n ln,l.mean_net lm,s.n sn,s.mean_net sm
 FROM prior b JOIN prior l ON l.forecast_source=b.forecast_source
  AND l.forecast_version=b.forecast_version AND l.model_backend=b.model_backend
  AND l.calibration=b.calibration AND l.strategy_mode=b.strategy_mode
 JOIN prior s ON s.forecast_source=b.forecast_source AND s.forecast_version=b.forecast_version
  AND s.model_backend=b.model_backend AND s.calibration=b.calibration AND s.strategy_mode=b.strategy_mode
 WHERE b.transform='brier' AND l.transform='log' AND s.transform='spherical'
  AND b.n>=20 AND l.n>=20 AND s.n>=20
), current_rows AS (
 SELECT p.*,ROW_NUMBER() OVER (PARTITION BY p.platform,p.ticker,p.forecast_source,
  p.forecast_version,p.model_backend,p.calibration,p.strategy_mode,p.transform ORDER BY p.id DESC) rn
 FROM research_proper_score_trials p WHERE p.observed_ts>=? AND p.settled=0 AND p.selected_side!=''
)
SELECT c.platform,c.ticker,c.system_name,c.forecast_source,c.forecast_version,c.model_backend,
c.calibration,c.strategy_mode,
CASE WHEN x.sm>x.bm AND x.sm>x.lm THEN 'spherical' WHEN x.lm>x.bm THEN 'log' ELSE 'brier' END,
c.selected_side,c.observed_ts,x.bn,x.ln,x.sn,x.bm,x.lm,x.sm
FROM current_rows c JOIN paired x ON x.forecast_source=c.forecast_source
 AND x.forecast_version=c.forecast_version AND x.model_backend=c.model_backend
 AND x.calibration=c.calibration AND x.strategy_mode=c.strategy_mode
WHERE c.rn=1 AND c.transform=(CASE WHEN x.sm>x.bm AND x.sm>x.lm THEN 'spherical'
 WHEN x.lm>x.bm THEN 'log' ELSE 'brier' END)
ORDER BY c.observed_ts DESC LIMIT ?`, start.Format(time.RFC3339Nano), end.Format(time.RFC3339Nano),
		end.Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FrozenPersonaCandidate
	for rows.Next() {
		var v FrozenPersonaCandidate
		var observed string
		if err := rows.Scan(&v.Platform, &v.Ticker, &v.SourceSystem, &v.ForecastSource,
			&v.ForecastVersion, &v.ModelBackend, &v.Calibration, &v.StrategyMode,
			&v.SelectedTransform, &v.SelectedSide, &observed, &v.BrierN, &v.LogN,
			&v.SphericalN, &v.BrierMean, &v.LogMean, &v.SphericalMean); err != nil {
			return nil, err
		}
		v.TrialObserved, _ = time.Parse(time.RFC3339Nano, observed)
		v.FreezeStart, v.FreezeEnd, v.PriorN = start, end, v.BrierN+v.LogN+v.SphericalN
		out = append(out, v)
	}
	return out, rows.Err()
}

type SeriesRollInput struct {
	Series, CurrentEvent, CurrentTicker, PriorEvent, PriorTicker, Kind string
	PriorOpened, PriorResolved                                         time.Time
	PriorAsk, PriorFee, PriorSettle                                    float64
}

// SeriesRollInputs uses only official series->event mappings and catalog event keys. The prior
// issue must have a prospective exact-book row and exact venue settlement; ticker-prefix guesses
// are forbidden.
func (s *Store) SeriesRollInputs(ctx context.Context, now time.Time, limit int) ([]SeriesRollInput, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `WITH current AS MATERIALIZED (
 SELECT sm.series_ticker,sm.event_ticker current_event,c.ticker current_ticker,c.kind,c.close_ts
 FROM research_series_event_map sm INDEXED BY idx_rseries_series_event
 CROSS JOIN market_catalog c INDEXED BY idx_mcat_event_kind_close
 WHERE c.venue='kalshi' AND c.event_key=sm.event_ticker
  AND c.close_ts>? AND c.ticker!=''
 ORDER BY c.close_ts LIMIT 5000
), candidates AS MATERIALIZED (
 SELECT cur.*,
  (SELECT pm.event_ticker FROM research_series_event_map pm
   INDEXED BY idx_rseries_series_event
   CROSS JOIN market_catalog pc INDEXED BY idx_mcat_event_kind_close
   WHERE pm.series_ticker=cur.series_ticker AND pc.venue='kalshi'
    AND pc.event_key=pm.event_ticker AND pc.kind=cur.kind AND pc.close_ts<cur.close_ts
   ORDER BY pc.close_ts DESC LIMIT 1) prior_event
 FROM current cur
)
SELECT x.series_ticker,x.current_event,x.current_ticker,x.prior_event,p.ticker,x.kind,
s.ts,s.resolved_at,s.book_ask,s.book_taker_fee_pc,s.settle_val
FROM candidates x CROSS JOIN market_catalog p INDEXED BY idx_mcat_event_kind_close
JOIN signal_log s ON s.id=(SELECT z.id FROM signal_log z INDEXED BY idx_signal_series_roll
 WHERE z.platform='kalshi' AND z.ticker=p.ticker AND z.resolved=1 AND z.settle_val>=0
  AND z.book_feature_ver=1 AND z.pricing_version='book-native-v2' AND z.book_ask>0
  AND z.book_taker_fee_pc IS NOT NULL ORDER BY z.id DESC LIMIT 1)
WHERE x.prior_event IS NOT NULL AND p.venue='kalshi' AND p.event_key=x.prior_event AND p.kind=x.kind
ORDER BY s.resolved_at DESC LIMIT ?`,
		now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SeriesRollInput
	for rows.Next() {
		var v SeriesRollInput
		var opened, resolved string
		if err := rows.Scan(&v.Series, &v.CurrentEvent, &v.CurrentTicker, &v.PriorEvent,
			&v.PriorTicker, &v.Kind, &opened, &resolved, &v.PriorAsk, &v.PriorFee,
			&v.PriorSettle); err != nil {
			return nil, err
		}
		v.PriorOpened, _ = time.Parse(time.RFC3339Nano, opened)
		v.PriorResolved, _ = time.Parse(time.RFC3339Nano, resolved)
		out = append(out, v)
	}
	return out, rows.Err()
}

func hashConcreteInput(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(h[:])
}

func sortedJSONKeys(raw string) int {
	var v map[string]any
	if json.Unmarshal([]byte(raw), &v) != nil {
		return 0
	}
	keys := make([]string, 0, len(v))
	for key := range v {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return len(keys)
}

var _ = fmt.Sprint
