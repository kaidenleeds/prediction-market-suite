package storage

// Event-driven Step-7 evidence. These ledgers are research-only and deliberately separate from
// order/fill accounting: book callbacks enqueue bounded summaries in memory, then a scheduled
// writer persists only depletion episodes, fixed marks, paired direction labels, natural maker
// lifecycle events, and conservative incentive evaluations. Nothing here creates an order or
// grants Paper/LIVE authority.

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

const step7EvidenceSchema = `
-- R146: the one-time historical route compactor must never scan the multi-million-row observation
-- ledger while holding SQLite's writer lock. This exact partial index is intentionally narrower
-- than the general system index and matches the two superseded Step-7 label-audit cohorts only.
CREATE INDEX IF NOT EXISTS idx_rsystem_obs_flow_label_cleanup
ON research_system_observations(id)
WHERE system_id='flow-direction-integrity' AND route='observer'
 AND observation_kind='control' AND blocker='label_audit_only_no_trade_authority'
 AND cohort IN ('kalshi-authoritative-aggressor-v1','kalshi-inferred-aggressor-v1');

CREATE TABLE IF NOT EXISTS research_replenishment_episodes (
 episode_id TEXT PRIMARY KEY,
 observed_ts TEXT NOT NULL,
 ticker TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES_ASK','YES_BID')),
 source_generation INTEGER NOT NULL CHECK(source_generation>0),
 source_subscription_id INTEGER NOT NULL CHECK(source_subscription_id>0),
 source_sequence INTEGER NOT NULL CHECK(source_sequence>0),
 prior_source_sequence INTEGER NOT NULL DEFAULT 0 CHECK(prior_source_sequence>=0),
 sequence_gap INTEGER NOT NULL DEFAULT 0 CHECK(sequence_gap>=0),
 start_price REAL NOT NULL CHECK(start_price>0 AND start_price<1),
 start_depth REAL NOT NULL CHECK(start_depth>0),
 depleted_price REAL NOT NULL CHECK(depleted_price>=0 AND depleted_price<=1),
 depleted_depth REAL NOT NULL CHECK(depleted_depth>=0),
 depletion_units REAL NOT NULL CHECK(depletion_units>=0),
 authoritative_trade_units REAL NOT NULL CHECK(authoritative_trade_units>=0),
 unexplained_removal_units REAL NOT NULL CHECK(unexplained_removal_units>=0),
 queue_churn_proxy_units REAL NOT NULL CHECK(queue_churn_proxy_units>=0),
 cancel_authority TEXT NOT NULL CHECK(cancel_authority='book_minus_authoritative_trade_proxy'),
 source_clock TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)
);
CREATE INDEX IF NOT EXISTS idx_rreplenish_ts ON research_replenishment_episodes(observed_ts,episode_id);
CREATE INDEX IF NOT EXISTS idx_rreplenish_ticker ON research_replenishment_episodes(ticker,observed_ts DESC);

CREATE TABLE IF NOT EXISTS research_replenishment_marks (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 episode_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('refill','horizon','sequence_reset','evicted')),
 horizon_ms INTEGER NOT NULL CHECK(horizon_ms>=0),
 book_price REAL NOT NULL CHECK(book_price>=0 AND book_price<=1),
 book_depth REAL NOT NULL CHECK(book_depth>=0),
 refill_latency_ms REAL,
 refill_fraction REAL NOT NULL DEFAULT 0 CHECK(refill_fraction>=0),
 authoritative_trade_units REAL NOT NULL DEFAULT 0 CHECK(authoritative_trade_units>=0),
 unexplained_removal_units REAL NOT NULL DEFAULT 0 CHECK(unexplained_removal_units>=0),
 queue_churn_proxy_units REAL NOT NULL DEFAULT 0 CHECK(queue_churn_proxy_units>=0),
 sequence_gap INTEGER NOT NULL DEFAULT 0 CHECK(sequence_gap>=0),
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(episode_id,event_type,horizon_ms),
 FOREIGN KEY(episode_id) REFERENCES research_replenishment_episodes(episode_id)
);
CREATE INDEX IF NOT EXISTS idx_rreplenish_mark_ts ON research_replenishment_marks(observed_ts,id);

CREATE TABLE IF NOT EXISTS research_flow_direction_pairs (
 pair_id TEXT PRIMARY KEY,
 observed_ts TEXT NOT NULL,
 source_ts TEXT NOT NULL DEFAULT '',
 ticker TEXT NOT NULL,
 trade_id TEXT NOT NULL,
 count_units REAL NOT NULL CHECK(count_units>0),
 yes_price REAL NOT NULL CHECK(yes_price>0 AND yes_price<1),
 taker_outcome_side TEXT NOT NULL DEFAULT '',
 taker_book_side TEXT NOT NULL DEFAULT '',
 legacy_taker_side TEXT NOT NULL DEFAULT '',
 legacy_outcome_side TEXT NOT NULL DEFAULT '',
 authoritative_side TEXT NOT NULL CHECK(authoritative_side IN ('yes','no','unknown','conflict')),
 inferred_side TEXT NOT NULL CHECK(inferred_side IN ('yes','no','unknown')),
 comparison_status TEXT NOT NULL CHECK(comparison_status IN ('agreement','mismatch','inferred_unknown','authoritative_unknown','authoritative_conflict')),
 book_observed_ts TEXT NOT NULL DEFAULT '',
 book_age_ms REAL NOT NULL CHECK(book_age_ms>=0),
 yes_bid REAL NOT NULL CHECK(yes_bid>=0 AND yes_bid<1),
 yes_ask REAL NOT NULL CHECK(yes_ask>=0 AND yes_ask<=1),
 book_source_generation INTEGER NOT NULL DEFAULT 0 CHECK(book_source_generation>=0),
 book_source_sequence INTEGER NOT NULL DEFAULT 0 CHECK(book_source_sequence>=0),
 trade_source_sequence INTEGER NOT NULL DEFAULT 0 CHECK(trade_source_sequence>=0),
 source_clock_status TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)
);
CREATE INDEX IF NOT EXISTS idx_rflowpair_ts ON research_flow_direction_pairs(observed_ts,pair_id);
CREATE INDEX IF NOT EXISTS idx_rflowpair_status ON research_flow_direction_pairs(comparison_status,observed_ts DESC);

-- Compact durable truth retained after the raw 90-day flow-pair audit window. One row per UTC
-- day/ticker/direction cell preserves per-market heterogeneity plus agreement/mismatch/unknown
-- counts, size, price, clock quality, and boundary provenance without keeping an unbounded
-- duplicate JSON observation per public trade.
CREATE TABLE IF NOT EXISTS research_flow_direction_daily (
 utc_day TEXT NOT NULL,
 ticker TEXT NOT NULL,
 comparison_status TEXT NOT NULL CHECK(comparison_status IN ('agreement','mismatch','inferred_unknown','authoritative_unknown','authoritative_conflict')),
 authoritative_side TEXT NOT NULL CHECK(authoritative_side IN ('yes','no','unknown','conflict')),
 inferred_side TEXT NOT NULL CHECK(inferred_side IN ('yes','no','unknown')),
 pair_count INTEGER NOT NULL CHECK(pair_count>0),
 contract_units REAL NOT NULL CHECK(contract_units>0),
 yes_price_units REAL NOT NULL CHECK(yes_price_units>0),
 complete_clock_count INTEGER NOT NULL CHECK(complete_clock_count>=0 AND complete_clock_count<=pair_count),
 first_observed_ts TEXT NOT NULL,
 last_observed_ts TEXT NOT NULL,
 max_pair_id TEXT NOT NULL,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 PRIMARY KEY(utc_day,ticker,comparison_status,authoritative_side,inferred_side)
);
CREATE INDEX IF NOT EXISTS idx_rflowdaily_day ON research_flow_direction_daily(utc_day,comparison_status,ticker);

CREATE TABLE IF NOT EXISTS research_maker_salvage_trials (
 attempt_id INTEGER PRIMARY KEY,
 observed_ts TEXT NOT NULL,
 platform TEXT NOT NULL,
 ticker TEXT NOT NULL,
 side TEXT NOT NULL,
 source TEXT NOT NULL,
 strategy_family TEXT NOT NULL DEFAULT '',
 strategy_inverted INTEGER NOT NULL CHECK(strategy_inverted IN (0,1)),
 post_price REAL NOT NULL CHECK(post_price>0 AND post_price<1),
 spread_cents REAL NOT NULL DEFAULT 0,
 book_depth REAL NOT NULL DEFAULT 0,
 route_reason TEXT NOT NULL DEFAULT '',
 queue_ahead REAL,
 decision_id TEXT NOT NULL DEFAULT '',
 maker_net_lower REAL,
 taker_cost REAL,
 taker_fee REAL,
 taker_net_lower REAL,
 taker_rejection_reason TEXT NOT NULL DEFAULT '',
 tick_size REAL,
 quote_age_s REAL,
 decision_latency_ms REAL,
 visible_capacity REAL,
 book_source TEXT NOT NULL DEFAULT '',
 source_clock_id TEXT NOT NULL DEFAULT '',
 certificate_hash TEXT NOT NULL DEFAULT '',
 matched_candidate INTEGER NOT NULL CHECK(matched_candidate IN (0,1)),
 blocker TEXT NOT NULL,
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)
);
CREATE INDEX IF NOT EXISTS idx_rmakersalvage_ts ON research_maker_salvage_trials(observed_ts,attempt_id);

CREATE TABLE IF NOT EXISTS research_maker_salvage_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 attempt_id INTEGER NOT NULL,
 event_key TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('matched_no_order_control','queue','fill','cancel','markout','settlement')),
 queue_ahead REAL,
 queue_left REAL,
 fill_units REAL,
 maker_fee_pc REAL,
 maker_rebate_pc REAL,
 fee_source TEXT NOT NULL DEFAULT '',
 markout_5m REAL,
 settlement_value REAL,
 realized_net REAL NOT NULL DEFAULT 0,
 realized_known INTEGER NOT NULL CHECK(realized_known IN (0,1)),
 blocker TEXT NOT NULL DEFAULT '',
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0),
 UNIQUE(attempt_id,event_key),
 FOREIGN KEY(attempt_id) REFERENCES research_maker_salvage_trials(attempt_id)
);
CREATE INDEX IF NOT EXISTS idx_rmakersalvage_event ON research_maker_salvage_events(observed_ts,id);

CREATE TABLE IF NOT EXISTS research_incentive_lock_evaluations (
 evaluation_id TEXT PRIMARY KEY,
 observed_ts TEXT NOT NULL,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 program_id TEXT NOT NULL,
 ticker TEXT NOT NULL,
 bundle_id TEXT NOT NULL DEFAULT '',
 certificate_hash TEXT NOT NULL DEFAULT '',
 bundle_current INTEGER NOT NULL CHECK(bundle_current IN (0,1)),
 eligibility_known INTEGER NOT NULL CHECK(eligibility_known IN (0,1)),
 competition_known INTEGER NOT NULL CHECK(competition_known IN (0,1)),
 queue_known INTEGER NOT NULL CHECK(queue_known IN (0,1)),
 fill_known INTEGER NOT NULL CHECK(fill_known IN (0,1)),
 reward_credit_known INTEGER NOT NULL CHECK(reward_credit_known IN (0,1)),
 competition_units REAL NOT NULL DEFAULT 0 CHECK(competition_units>=0),
 queue_units REAL NOT NULL DEFAULT 0 CHECK(queue_units>=0),
 actual_credited_reward REAL NOT NULL DEFAULT 0 CHECK(actual_credited_reward>=0),
 reward_lower REAL NOT NULL DEFAULT 0 CHECK(reward_lower>=0),
 base_bundle_net_floor REAL NOT NULL DEFAULT 0,
 partial_fill_worst REAL NOT NULL DEFAULT 0 CHECK(partial_fill_worst<=0),
 unwind_worst REAL NOT NULL DEFAULT 0 CHECK(unwind_worst<=0),
 capital_time_reserve REAL NOT NULL DEFAULT 0 CHECK(capital_time_reserve>=0),
 conservative_total_lower REAL NOT NULL,
 candidate INTEGER NOT NULL CHECK(candidate IN (0,1)),
 blocker TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 funded INTEGER NOT NULL DEFAULT 0 CHECK(funded=0),
 paper_authority INTEGER NOT NULL DEFAULT 0 CHECK(paper_authority=0),
 live_authority INTEGER NOT NULL DEFAULT 0 CHECK(live_authority=0)
);
CREATE INDEX IF NOT EXISTS idx_rincentivelock_ts ON research_incentive_lock_evaluations(observed_ts,evaluation_id);
CREATE INDEX IF NOT EXISTS idx_rincentivelock_program ON research_incentive_lock_evaluations(venue,program_id,observed_ts DESC);
`

func migrateStep7EvidenceSchema(db *sql.DB) error {
	if _, err := db.Exec(step7EvidenceSchema); err != nil {
		return err
	}
	if err := migrateStep7ProjectionOutbox(db); err != nil {
		return err
	}
	// A zero 5m mark is meaningful, so old default-zero rows remain unknown while every new
	// SetMakerAdverse call stamps the observation clock. Frozen taker-decision fields are optional;
	// absence makes the matched cohort a control rather than being guessed from source text.
	for _, col := range []string{
		"adverse_5m_at TEXT NOT NULL DEFAULT ''",
		"salvage_decision_id TEXT NOT NULL DEFAULT ''",
		"salvage_maker_net_lower REAL",
		"salvage_taker_cost REAL",
		"salvage_taker_fee REAL",
		"salvage_taker_net_lower REAL",
		"salvage_taker_rejection TEXT NOT NULL DEFAULT ''",
		"salvage_tick_size REAL",
		"salvage_quote_age_s REAL",
		"salvage_decision_latency_ms REAL",
		"salvage_visible_capacity REAL",
		"salvage_book_source TEXT NOT NULL DEFAULT ''",
		"salvage_source_clock_id TEXT NOT NULL DEFAULT ''",
		"salvage_certificate_hash TEXT NOT NULL DEFAULT ''",
		"settle_mirrored_at TEXT NOT NULL DEFAULT ''",
	} {
		_, _ = db.Exec("ALTER TABLE maker_fill_stats ADD COLUMN " + col)
	}
	for _, col := range []string{"maker_net_lower REAL", "tick_size REAL", "quote_age_s REAL", "decision_latency_ms REAL",
		"visible_capacity REAL", "book_source TEXT NOT NULL DEFAULT ''", "source_clock_id TEXT NOT NULL DEFAULT ''",
		"certificate_hash TEXT NOT NULL DEFAULT ''"} {
		_, _ = db.Exec("ALTER TABLE research_maker_salvage_trials ADD COLUMN " + col)
	}
	return nil
}

type Step7ReplenishmentEpisode struct {
	EpisodeID, Ticker, Side, SourceClock string
	Observed                             time.Time
	Generation                           uint64
	SubscriptionID                       int64
	Sequence, PriorSequence, SequenceGap int64
	StartPrice, StartDepth               float64
	DepletedPrice, DepletedDepth         float64
	DepletionUnits, TradeUnits           float64
	UnexplainedUnits, QueueChurnUnits    float64
	Evidence                             any
}

type Step7ReplenishmentMark struct {
	EpisodeID, EventType, Ticker, Side                    string
	Observed                                              time.Time
	HorizonMS, SequenceGap                                int64
	BookPrice, BookDepth, RefillLatencyMS, RefillFraction float64
	TradeUnits, UnexplainedUnits, QueueChurnUnits         float64
	Evidence                                              any
	Refilled                                              bool
	// Timing/provenance fields are frozen in memory when a horizon actually becomes due. They
	// are serialized inside Evidence by the server rather than becoming mutable columns in the
	// compact raw-mark table. Delayed SQLite persistence must never replace this sampled clock
	// or reprice the action from a later book.
	Target, BookObserved, BookSourceAt     time.Time
	BookGeneration                         uint64
	BookSubscriptionID, BookSourceSequence int64
	TimingVersion                          int
}

type Step7FlowDirectionPair struct {
	PairID, Ticker, TradeID                           string
	TakerOutcomeSide, TakerBookSide                   string
	LegacyTakerSide, LegacyOutcomeSide                string
	AuthoritativeSide, InferredSide, ComparisonStatus string
	SourceClockStatus                                 string
	Observed, SourceAt, BookObserved                  time.Time
	Count, YesPrice, BookAgeMS, YesBid, YesAsk        float64
	BookGeneration                                    uint64
	BookSequence, TradeSequence                       int64
	Evidence                                          any
}

func step7JSON(v any) (string, error) {
	if v == nil {
		return "{}", nil
	}
	b, err := json.Marshal(v)
	if err != nil || !json.Valid(b) {
		return "", fmt.Errorf("invalid Step-7 evidence JSON: %w", err)
	}
	return string(b), nil
}

func step7Finite(values ...float64) bool {
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}
	return true
}

// InsertStep7MicrostructureBatch commits every raw summary with exactly one immutable projection
// job. A missing, extra, conflicting, or differently fingerprinted job rolls back the whole batch.
// Duplicate source identities across retries remain harmless and never inflate n.
func (s *Store) InsertStep7MicrostructureBatch(ctx context.Context, episodes []Step7ReplenishmentEpisode,
	marks []Step7ReplenishmentMark, flows []Step7FlowDirectionPair, jobs []Step7ProjectionJob,
) (episodeN, markN, flowN, jobN int, err error) {
	expected, err := step7ExpectedProjectionRows(episodes, marks, flows)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if err := validateStep7ProjectionJobs(expected, jobs); err != nil {
		return 0, 0, 0, 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer tx.Rollback()
	flowRows := make(map[string]Step7FlowDirectionPair, len(flows))
	for _, flow := range flows {
		flowRows[flow.PairID] = flow
	}
	newSources, newJobs, err := classifyStep7ProjectionBatchTx(ctx, tx, jobs, flowRows)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	consumedSources := make(map[string]bool, len(newSources))
	takeNewSource := func(kind, rawID string) bool {
		key := step7ProjectionSourceKey(kind, rawID)
		if !newSources[key] || consumedSources[key] {
			return false
		}
		consumedSources[key] = true
		return true
	}
	for _, v := range episodes {
		if v.EpisodeID == "" || v.Ticker == "" || (v.Side != "YES_ASK" && v.Side != "YES_BID") ||
			v.Observed.IsZero() || v.Generation == 0 || v.SubscriptionID <= 0 || v.Sequence <= 0 ||
			v.StartPrice <= 0 || v.StartPrice >= 1 || v.StartDepth <= 0 || v.DepletedPrice < 0 ||
			v.DepletedPrice > 1 || v.DepletedDepth < 0 || v.DepletionUnits < 0 || v.TradeUnits < 0 ||
			v.UnexplainedUnits < 0 || v.QueueChurnUnits < 0 || !step7Finite(v.StartPrice, v.StartDepth,
			v.DepletedPrice, v.DepletedDepth, v.DepletionUnits, v.TradeUnits, v.UnexplainedUnits, v.QueueChurnUnits) {
			return 0, 0, 0, 0, errors.New("invalid replenishment episode")
		}
		if !takeNewSource(Step7ProjectionEpisode, v.EpisodeID) {
			continue
		}
		evidence, jerr := step7JSON(v.Evidence)
		if jerr != nil {
			return 0, 0, 0, 0, jerr
		}
		res, qerr := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_replenishment_episodes(
episode_id,observed_ts,ticker,side,source_generation,source_subscription_id,source_sequence,
prior_source_sequence,sequence_gap,start_price,start_depth,depleted_price,depleted_depth,
depletion_units,authoritative_trade_units,unexplained_removal_units,queue_churn_proxy_units,
cancel_authority,source_clock,evidence_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'book_minus_authoritative_trade_proxy',?,?,0,0,0)`,
			v.EpisodeID, v.Observed.UTC().Format(time.RFC3339Nano), v.Ticker, v.Side, v.Generation,
			v.SubscriptionID, v.Sequence, v.PriorSequence, v.SequenceGap, v.StartPrice, v.StartDepth,
			v.DepletedPrice, v.DepletedDepth, v.DepletionUnits, v.TradeUnits, v.UnexplainedUnits,
			v.QueueChurnUnits, v.SourceClock, evidence)
		if qerr != nil {
			return 0, 0, 0, 0, qerr
		}
		if n, _ := res.RowsAffected(); n > 0 {
			episodeN++
		}
	}
	for _, v := range marks {
		if v.EpisodeID == "" || (v.EventType != "refill" && v.EventType != "horizon" &&
			v.EventType != "sequence_reset" && v.EventType != "evicted") || v.Observed.IsZero() ||
			v.HorizonMS < 0 || v.BookPrice < 0 || v.BookPrice > 1 || v.BookDepth < 0 ||
			v.RefillLatencyMS < 0 || v.RefillFraction < 0 || v.TradeUnits < 0 ||
			v.UnexplainedUnits < 0 || v.QueueChurnUnits < 0 || !step7Finite(v.BookPrice, v.BookDepth,
			v.RefillLatencyMS, v.RefillFraction, v.TradeUnits, v.UnexplainedUnits, v.QueueChurnUnits) {
			return 0, 0, 0, 0, errors.New("invalid replenishment mark")
		}
		if !takeNewSource(Step7ProjectionMark,
			Step7MarkProjectionRawID(v.EpisodeID, v.EventType, v.HorizonMS)) {
			continue
		}
		evidence, jerr := step7JSON(v.Evidence)
		if jerr != nil {
			return 0, 0, 0, 0, jerr
		}
		var latency any
		if v.EventType == "refill" {
			latency = v.RefillLatencyMS
		}
		res, qerr := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_replenishment_marks(
episode_id,observed_ts,event_type,horizon_ms,book_price,book_depth,refill_latency_ms,refill_fraction,
authoritative_trade_units,unexplained_removal_units,queue_churn_proxy_units,sequence_gap,evidence_json,
funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, v.EpisodeID,
			v.Observed.UTC().Format(time.RFC3339Nano), v.EventType, v.HorizonMS, v.BookPrice, v.BookDepth,
			latency, v.RefillFraction, v.TradeUnits, v.UnexplainedUnits, v.QueueChurnUnits, v.SequenceGap, evidence)
		if qerr != nil {
			return 0, 0, 0, 0, qerr
		}
		if n, _ := res.RowsAffected(); n > 0 {
			markN++
		}
	}
	for _, v := range flows {
		validStatus := v.ComparisonStatus == "agreement" || v.ComparisonStatus == "mismatch" ||
			v.ComparisonStatus == "inferred_unknown" || v.ComparisonStatus == "authoritative_unknown" ||
			v.ComparisonStatus == "authoritative_conflict"
		if v.PairID == "" || v.Ticker == "" || v.TradeID == "" || v.Observed.IsZero() || v.Count <= 0 ||
			v.YesPrice <= 0 || v.YesPrice >= 1 || !validStatus || v.BookAgeMS < 0 ||
			!step7Finite(v.Count, v.YesPrice, v.BookAgeMS, v.YesBid, v.YesAsk) {
			return 0, 0, 0, 0, errors.New("invalid flow direction pair")
		}
		if !takeNewSource(Step7ProjectionFlow, v.PairID) {
			continue
		}
		evidence, jerr := step7JSON(v.Evidence)
		if jerr != nil {
			return 0, 0, 0, 0, jerr
		}
		res, qerr := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_flow_direction_pairs(
pair_id,observed_ts,source_ts,ticker,trade_id,count_units,yes_price,taker_outcome_side,taker_book_side,
legacy_taker_side,legacy_outcome_side,authoritative_side,inferred_side,comparison_status,book_observed_ts,
book_age_ms,yes_bid,yes_ask,book_source_generation,book_source_sequence,trade_source_sequence,
source_clock_status,evidence_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, v.PairID,
			v.Observed.UTC().Format(time.RFC3339Nano), step7Time(v.SourceAt), v.Ticker, v.TradeID, v.Count,
			v.YesPrice, v.TakerOutcomeSide, v.TakerBookSide, v.LegacyTakerSide, v.LegacyOutcomeSide,
			v.AuthoritativeSide, v.InferredSide, v.ComparisonStatus, step7Time(v.BookObserved), v.BookAgeMS,
			v.YesBid, v.YesAsk, v.BookGeneration, v.BookSequence, v.TradeSequence, v.SourceClockStatus, evidence)
		if qerr != nil {
			return 0, 0, 0, 0, qerr
		}
		if n, _ := res.RowsAffected(); n > 0 {
			flowN++
		}
	}
	jobN, err = insertStep7ProjectionJobsTx(ctx, tx, newJobs, time.Now().UTC())
	if err != nil {
		return 0, 0, 0, 0, err
	}
	if err = tx.Commit(); err != nil {
		return 0, 0, 0, 0, err
	}
	return episodeN, markN, flowN, jobN, nil
}

func step7Time(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.UTC().Format(time.RFC3339Nano)
}

type Step7MakerSnapshot struct {
	AttemptID                                                       int64
	Opened, FillAt, ExpireAt, MarkoutAt, SettlementAt               time.Time
	Platform, Ticker, Side, Source, Family, RouteReason, CancelRule string
	Inverted                                                        bool
	PostPrice, SpreadCents, BookDepth                               float64
	QueueAhead, QueueLeft, FillUnits                                *float64
	Filled                                                          int
	Fee, Rebate                                                     *float64
	FeeSource                                                       string
	Markout, Settlement                                             *float64
	DecisionID, TakerRejection                                      string
	MakerNetLower, TakerCost, TakerFee, TakerNetLower               *float64
	Tick, QuoteAge, DecisionLatency, VisibleCapacity                *float64
	BookSource, SourceClockID, CertificateHash                      string
}

func (s *Store) NaturalMakerSalvageSnapshots(ctx context.Context, limit int) ([]Step7MakerSnapshot, error) {
	if limit <= 0 || limit > 500 {
		limit = 250
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,ts,platform,ticker,side,source,strategy_family,
strategy_inverted,post_px,spread_cents,book_depth,filled,fill_ts,expire_ts,route_reason,cancel_rule,
queue_ahead,queue_left,fill_ct,maker_fee_pc,maker_rebate_pc,maker_fee_source,adverse_5m,
adverse_5m_at,settle_val,salvage_decision_id,salvage_maker_net_lower,salvage_taker_cost,salvage_taker_fee,
salvage_taker_net_lower,salvage_taker_rejection,salvage_tick_size,salvage_quote_age_s,
salvage_decision_latency_ms,salvage_visible_capacity,salvage_book_source,salvage_source_clock_id,
salvage_certificate_hash,settle_mirrored_at FROM maker_fill_stats
WHERE filled IN (-1,0,1) ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Step7MakerSnapshot
	for rows.Next() {
		var v Step7MakerSnapshot
		var opened, fillAt, expireAt, markAt, settleAt string
		var inverted int
		var queueAhead, queueLeft, fillUnits, fee, rebate, settle sql.NullFloat64
		var makerLow, takerCost, takerFee, takerLow sql.NullFloat64
		var tick, quoteAge, latency, capacity sql.NullFloat64
		var mark float64
		if err := rows.Scan(&v.AttemptID, &opened, &v.Platform, &v.Ticker, &v.Side, &v.Source, &v.Family,
			&inverted, &v.PostPrice, &v.SpreadCents, &v.BookDepth, &v.Filled, &fillAt, &expireAt,
			&v.RouteReason, &v.CancelRule, &queueAhead, &queueLeft, &fillUnits, &fee, &rebate,
			&v.FeeSource, &mark, &markAt, &settle, &v.DecisionID, &makerLow, &takerCost, &takerFee,
			&takerLow, &v.TakerRejection, &tick, &quoteAge, &latency, &capacity, &v.BookSource,
			&v.SourceClockID, &v.CertificateHash, &settleAt); err != nil {
			return nil, err
		}
		v.Inverted = inverted != 0
		v.Opened, _ = time.Parse(time.RFC3339Nano, opened)
		v.FillAt, _ = time.Parse(time.RFC3339Nano, fillAt)
		v.ExpireAt, _ = time.Parse(time.RFC3339Nano, expireAt)
		v.MarkoutAt, _ = time.Parse(time.RFC3339Nano, markAt)
		v.SettlementAt, _ = time.Parse(time.RFC3339Nano, settleAt)
		if queueAhead.Valid {
			x := queueAhead.Float64
			v.QueueAhead = &x
		}
		if queueLeft.Valid {
			x := queueLeft.Float64
			v.QueueLeft = &x
		}
		if fillUnits.Valid {
			x := fillUnits.Float64
			v.FillUnits = &x
		}
		if fee.Valid {
			x := fee.Float64
			v.Fee = &x
		}
		if rebate.Valid {
			x := rebate.Float64
			v.Rebate = &x
		}
		if !v.MarkoutAt.IsZero() {
			x := mark
			v.Markout = &x
		}
		if settle.Valid {
			x := settle.Float64
			v.Settlement = &x
		}
		if makerLow.Valid {
			x := makerLow.Float64
			v.MakerNetLower = &x
		}
		if takerCost.Valid {
			x := takerCost.Float64
			v.TakerCost = &x
		}
		if takerFee.Valid {
			x := takerFee.Float64
			v.TakerFee = &x
		}
		if takerLow.Valid {
			x := takerLow.Float64
			v.TakerNetLower = &x
		}
		if tick.Valid {
			x := tick.Float64
			v.Tick = &x
		}
		if quoteAge.Valid {
			x := quoteAge.Float64
			v.QuoteAge = &x
		}
		if latency.Valid {
			x := latency.Float64
			v.DecisionLatency = &x
		}
		if capacity.Valid {
			x := capacity.Float64
			v.VisibleCapacity = &x
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) SetMakerSalvageDecision(ctx context.Context, attemptID int64, decisionID string,
	takerCost, takerFee, takerNetLower float64, rejection string) error {
	decisionID, rejection = strings.TrimSpace(decisionID), strings.TrimSpace(rejection)
	if attemptID <= 0 || decisionID == "" || rejection == "" || takerCost <= 0 || takerCost >= 1 ||
		takerFee < 0 || !step7Finite(takerCost, takerFee, takerNetLower) {
		return errors.New("invalid frozen maker-salvage decision")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET salvage_decision_id=?,salvage_taker_cost=?,
salvage_taker_fee=?,salvage_taker_net_lower=?,salvage_taker_rejection=? WHERE id=? AND salvage_decision_id=''`,
		decisionID, takerCost, takerFee, takerNetLower, rejection, attemptID)
	return err
}

type MakerSalvageDecisionTruth struct {
	DecisionID, Rejection, BookSource, SourceClockID, CertificateHash string
	MakerNetLower, TakerCost, TakerFee, TakerNetLower                 float64
	Tick, QuoteAge, DecisionLatencyMS, VisibleCapacity                float64
}

// SetMakerSalvageDecisionTruth freezes the complete alternative-route decision before a natural
// post can fill. Partial setters remain controls; only this complete receipt can become a matched
// economic candidate after terminal money truth arrives.
func (s *Store) SetMakerSalvageDecisionTruth(ctx context.Context, attemptID int64, v MakerSalvageDecisionTruth) error {
	v.DecisionID, v.Rejection = strings.TrimSpace(v.DecisionID), strings.TrimSpace(v.Rejection)
	if attemptID <= 0 || v.DecisionID == "" || v.Rejection == "" || v.BookSource == "" ||
		v.SourceClockID == "" || v.CertificateHash == "" || v.TakerCost <= 0 || v.TakerCost >= 1 ||
		v.MakerNetLower <= 0 || v.TakerFee < 0 || v.Tick <= 0 || v.QuoteAge < 0 || v.DecisionLatencyMS < 0 ||
		v.VisibleCapacity < 1 || !step7Finite(v.TakerCost, v.TakerFee, v.TakerNetLower, v.Tick,
		v.MakerNetLower, v.QuoteAge, v.DecisionLatencyMS, v.VisibleCapacity) {
		return errors.New("invalid complete maker-salvage decision truth")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE maker_fill_stats SET salvage_decision_id=?,salvage_maker_net_lower=?,salvage_taker_cost=?,
salvage_taker_fee=?,salvage_taker_net_lower=?,salvage_taker_rejection=?,salvage_tick_size=?,
salvage_quote_age_s=?,salvage_decision_latency_ms=?,salvage_visible_capacity=?,salvage_book_source=?,
salvage_source_clock_id=?,salvage_certificate_hash=? WHERE id=? AND salvage_decision_id=''`, v.DecisionID,
		v.MakerNetLower, v.TakerCost, v.TakerFee, v.TakerNetLower, v.Rejection, v.Tick, v.QuoteAge, v.DecisionLatencyMS,
		v.VisibleCapacity, v.BookSource, v.SourceClockID, v.CertificateHash, attemptID)
	return err
}

func makerSalvageMatched(v Step7MakerSnapshot) bool {
	return v.DecisionID != "" && v.TakerCost != nil && v.TakerFee != nil && v.TakerNetLower != nil &&
		v.MakerNetLower != nil && *v.MakerNetLower > 0 && *v.TakerNetLower <= 0 &&
		strings.TrimSpace(v.TakerRejection) != "" && strings.TrimSpace(v.Family) != "" &&
		v.Tick != nil && *v.Tick > 0 && v.QuoteAge != nil && *v.QuoteAge >= 0 &&
		v.DecisionLatency != nil && *v.DecisionLatency >= 0 && v.VisibleCapacity != nil &&
		*v.VisibleCapacity >= 1 && v.BookSource != "" && v.SourceClockID != "" && v.CertificateHash != "" &&
		v.QueueAhead != nil
}

// MirrorNaturalMakerSalvage appends every lifecycle stage that is actually known. A cancel and
// its no-order control are exact zero; an unreceipted fill/settlement remains censored.
func (s *Store) MirrorNaturalMakerSalvage(ctx context.Context, v Step7MakerSnapshot) (int, error) {
	if v.AttemptID <= 0 || v.Opened.IsZero() || v.Platform == "" || v.Ticker == "" ||
		v.PostPrice <= 0 || v.PostPrice >= 1 || !step7Finite(v.PostPrice, v.SpreadCents, v.BookDepth) {
		return 0, errors.New("invalid natural maker snapshot")
	}
	matched := makerSalvageMatched(v)
	blocker := ""
	if !matched {
		blocker = "frozen_same_model_taker_rejection_missing"
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var queueAhead any
	if v.QueueAhead != nil {
		queueAhead = *v.QueueAhead
	}
	var makerLow, takerCost, takerFee, takerLow any
	if v.MakerNetLower != nil {
		makerLow = *v.MakerNetLower
	}
	if v.TakerCost != nil {
		takerCost = *v.TakerCost
	}
	if v.TakerFee != nil {
		takerFee = *v.TakerFee
	}
	if v.TakerNetLower != nil {
		takerLow = *v.TakerNetLower
	}
	var tick, quoteAge, decisionLatency, visibleCapacity any
	if v.Tick != nil {
		tick = *v.Tick
	}
	if v.QuoteAge != nil {
		quoteAge = *v.QuoteAge
	}
	if v.DecisionLatency != nil {
		decisionLatency = *v.DecisionLatency
	}
	if v.VisibleCapacity != nil {
		visibleCapacity = *v.VisibleCapacity
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_maker_salvage_trials(
attempt_id,observed_ts,platform,ticker,side,source,strategy_family,strategy_inverted,post_price,
spread_cents,book_depth,route_reason,queue_ahead,decision_id,maker_net_lower,taker_cost,taker_fee,taker_net_lower,
taker_rejection_reason,tick_size,quote_age_s,decision_latency_ms,visible_capacity,book_source,
source_clock_id,certificate_hash,matched_candidate,blocker,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, v.AttemptID, step7Time(v.Opened), v.Platform,
		v.Ticker, strings.ToUpper(v.Side), v.Source, v.Family, boolInt(v.Inverted), v.PostPrice,
		v.SpreadCents, v.BookDepth, v.RouteReason, queueAhead, v.DecisionID, makerLow, takerCost, takerFee,
		takerLow, v.TakerRejection, tick, quoteAge, decisionLatency, visibleCapacity, v.BookSource,
		v.SourceClockID, v.CertificateHash, boolInt(matched), blocker)
	if err != nil {
		return 0, err
	}
	inserted := 0
	if n, _ := res.RowsAffected(); n > 0 {
		inserted++
	}
	appendEvent := func(key, kind string, observed time.Time, realized float64, known bool, eventBlocker string,
		queueAhead, queueLeft, fillUnits, fee, rebate, markout, settlement any, evidence map[string]any) error {
		j, jerr := step7JSON(evidence)
		if jerr != nil {
			return jerr
		}
		if observed.IsZero() {
			observed = v.Opened
		}
		r, qerr := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_maker_salvage_events(
attempt_id,event_key,observed_ts,event_type,queue_ahead,queue_left,fill_units,maker_fee_pc,
maker_rebate_pc,fee_source,markout_5m,settlement_value,realized_net,realized_known,blocker,
evidence_json,funded,paper_authority,live_authority) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`,
			v.AttemptID, key, step7Time(observed), kind, queueAhead, queueLeft, fillUnits, fee, rebate, v.FeeSource,
			markout, settlement, realized, boolInt(known), eventBlocker, j)
		if qerr == nil {
			if n, _ := r.RowsAffected(); n > 0 {
				inserted++
			}
		}
		return qerr
	}
	if err = appendEvent("control", "matched_no_order_control", v.Opened, 0, true, "",
		nil, nil, nil, nil, nil, nil, nil, map[string]any{"realized_net": 0, "hypothetical_fill": false}); err != nil {
		return 0, err
	}
	if v.QueueAhead != nil {
		if err = appendEvent("queue", "queue", v.Opened, 0, false, "terminal_outcome_pending",
			*v.QueueAhead, valueOrNil(v.QueueLeft), valueOrNil(v.FillUnits), nil, nil, nil, nil,
			map[string]any{"queue_authority": "natural maker_fill_stats receipt"}); err != nil {
			return 0, err
		}
	}
	if v.Filled == 1 {
		feeKnown := v.Fee != nil && v.Rebate != nil && strings.TrimSpace(v.FeeSource) != ""
		fillBlocker := "settlement_pending"
		if !feeKnown {
			fillBlocker = "exact_fill_fee_rebate_receipt_missing"
		}
		if err = appendEvent("fill", "fill", v.FillAt, 0, false, fillBlocker, valueOrNil(v.QueueAhead),
			valueOrNil(v.QueueLeft), valueOrNil(v.FillUnits), valueOrNil(v.Fee), valueOrNil(v.Rebate), nil, nil,
			map[string]any{"actual_natural_fill": true}); err != nil {
			return 0, err
		}
	}
	if v.Filled == 0 {
		if err = appendEvent("cancel", "cancel", v.ExpireAt, 0, true, "", valueOrNil(v.QueueAhead),
			valueOrNil(v.QueueLeft), valueOrNil(v.FillUnits), nil, nil, nil, nil,
			map[string]any{"cancel_rule": v.CancelRule, "realized_net": 0}); err != nil {
			return 0, err
		}
	}
	if v.Markout != nil {
		if err = appendEvent("markout-5m", "markout", v.MarkoutAt, 0, false, "settlement_pending", nil, nil, nil,
			nil, nil, *v.Markout, nil, map[string]any{"horizon_seconds": 300}); err != nil {
			return 0, err
		}
	}
	if v.Filled == 1 && v.Settlement != nil {
		known := v.Fee != nil && v.Rebate != nil && strings.TrimSpace(v.FeeSource) != ""
		realized, settleSide := 0.0, *v.Settlement
		if strings.EqualFold(v.Side, "NO") {
			settleSide = 1 - settleSide
		}
		settleBlocker := "exact_fill_fee_rebate_receipt_missing"
		if known {
			realized = settleSide - v.PostPrice - *v.Fee + *v.Rebate
			settleBlocker = ""
		}
		settleAt := v.SettlementAt
		if settleAt.IsZero() {
			settleAt = time.Now().UTC()
			if settleBlocker == "" {
				settleBlocker = "authoritative_settlement_timestamp_missing"
			} else {
				settleBlocker += "; authoritative_settlement_timestamp_missing"
			}
		}
		if err = appendEvent("settlement", "settlement", settleAt, realized, known && !v.SettlementAt.IsZero(), settleBlocker,
			nil, nil, valueOrNil(v.FillUnits), valueOrNil(v.Fee), valueOrNil(v.Rebate), nil, *v.Settlement,
			map[string]any{"side_adjusted_settlement": settleSide, "one_unit_economics": true}); err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return inserted, nil
}

func valueOrNil(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

type CurrentIncentiveBundle struct {
	BundleID, CertificateHash, RouteKind, CanonicalEventID, PayoffID string
	Observed                                                         time.Time
	EventVersion                                                     int
	Atomic                                                           bool
	NetFloor, PartialFillWorst, UnwindWorst, Size                    float64
}

func (s *Store) CurrentCertifiedIncentiveBundles(ctx context.Context, venue, ticker string,
	since time.Time, limit int) ([]CurrentIncentiveBundle, error) {
	if limit <= 0 || limit > 20 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT b.bundle_id,b.certificate_hash,b.route_kind,b.canonical_event_id,
b.observed_ts,b.event_version,b.atomic_route,b.net_floor,b.partial_fill_worst,b.unwind_worst,b.size_units,l.payoff_id
FROM research_route_bundles b JOIN research_route_bundle_legs l ON l.bundle_id=b.bundle_id
JOIN research_instrument_specs i ON i.venue=l.venue AND i.ticker=l.ticker
 AND i.version=(SELECT MAX(v.version) FROM research_instrument_specs v WHERE v.venue=i.venue AND v.ticker=i.ticker)
WHERE l.venue=? AND l.ticker=? AND b.observed_ts>=? AND b.certificate_hash!=''
 AND i.event_id=b.canonical_event_id AND i.event_version=b.event_version AND i.payoff_id=l.payoff_id
 AND i.identity_status IN ('verified','structural') ORDER BY b.observed_ts DESC,b.bundle_id LIMIT ?`,
		strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(ticker), since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CurrentIncentiveBundle
	for rows.Next() {
		var v CurrentIncentiveBundle
		var observed string
		var atomic int
		if err = rows.Scan(&v.BundleID, &v.CertificateHash, &v.RouteKind, &v.CanonicalEventID, &observed,
			&v.EventVersion, &atomic, &v.NetFloor, &v.PartialFillWorst, &v.UnwindWorst, &v.Size, &v.PayoffID); err != nil {
			return nil, err
		}
		v.Observed, _ = time.Parse(time.RFC3339Nano, observed)
		v.Atomic = atomic != 0
		out = append(out, v)
	}
	return out, rows.Err()
}

type Step7IncentiveEvaluation struct {
	EvaluationID, Venue, ProgramID, Ticker, BundleID, CertificateHash, Blocker     string
	Observed                                                                       time.Time
	BundleCurrent, EligibilityKnown, CompetitionKnown, QueueKnown, FillKnown       bool
	RewardCreditKnown, Candidate                                                   bool
	CompetitionUnits, QueueUnits, ActualCredit, RewardLower                        float64
	BaseNetFloor, PartialFillWorst, UnwindWorst, CapitalReserve, ConservativeLower float64
	Evidence                                                                       any
}

func (s *Store) InsertStep7IncentiveEvaluation(ctx context.Context, v Step7IncentiveEvaluation) (bool, error) {
	if v.EvaluationID == "" || (v.Venue != "kalshi" && v.Venue != "polyus") || v.ProgramID == "" || v.Ticker == "" ||
		v.Observed.IsZero() || v.CompetitionUnits < 0 || v.QueueUnits < 0 || v.ActualCredit < 0 ||
		v.RewardLower < 0 || v.PartialFillWorst > 0 || v.UnwindWorst > 0 || v.CapitalReserve < 0 ||
		!step7Finite(v.CompetitionUnits, v.QueueUnits, v.ActualCredit, v.RewardLower, v.BaseNetFloor,
			v.PartialFillWorst, v.UnwindWorst, v.CapitalReserve, v.ConservativeLower) {
		return false, errors.New("invalid incentive lock evaluation")
	}
	allKnown := v.BundleCurrent && v.EligibilityKnown && v.CompetitionKnown && v.QueueKnown && v.FillKnown && v.RewardCreditKnown
	if v.Candidate && (!allKnown || v.BundleID == "" || v.CertificateHash == "" || v.RewardLower <= 0 ||
		v.ConservativeLower <= 0 || v.Blocker != "") {
		return false, errors.New("incentive candidate bypasses conservative contract")
	}
	if !v.RewardCreditKnown && v.RewardLower != 0 {
		return false, errors.New("uncredited reward lower bound must be zero")
	}
	evidence, err := step7JSON(v.Evidence)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_incentive_lock_evaluations(
evaluation_id,observed_ts,venue,program_id,ticker,bundle_id,certificate_hash,bundle_current,
eligibility_known,competition_known,queue_known,fill_known,reward_credit_known,competition_units,
queue_units,actual_credited_reward,reward_lower,base_bundle_net_floor,partial_fill_worst,unwind_worst,
capital_time_reserve,conservative_total_lower,candidate,blocker,evidence_json,funded,paper_authority,live_authority)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,0)`, v.EvaluationID, step7Time(v.Observed), v.Venue,
		v.ProgramID, v.Ticker, v.BundleID, v.CertificateHash, boolInt(v.BundleCurrent), boolInt(v.EligibilityKnown),
		boolInt(v.CompetitionKnown), boolInt(v.QueueKnown), boolInt(v.FillKnown), boolInt(v.RewardCreditKnown),
		v.CompetitionUnits, v.QueueUnits, v.ActualCredit, v.RewardLower, v.BaseNetFloor, v.PartialFillWorst,
		v.UnwindWorst, v.CapitalReserve, v.ConservativeLower, boolInt(v.Candidate), v.Blocker, evidence)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) PruneStep7Evidence(ctx context.Context, cutoff time.Time) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	cut := cutoff.UTC().Format(time.RFC3339Nano)
	var total int64
	// Preserve long-run label calibration before removing the 90-day raw pair. The bounded source
	// page and this UPSERT live in the same transaction, so a crash can neither double
	// count nor delete a pair before its daily/status/side aggregate commits.
	// The callback's hard ceiling is 600 flow pairs/minute (=36k/hour); 50k per hourly prune is
	// therefore both bounded and faster than the maximum admitted source rate, so the 90-day edge
	// cannot accumulate an ever-growing backlog.
	const flowArchiveBatch = 50000
	if _, err := tx.ExecContext(ctx, `INSERT INTO research_flow_direction_daily(
utc_day,ticker,comparison_status,authoritative_side,inferred_side,pair_count,contract_units,
yes_price_units,complete_clock_count,first_observed_ts,last_observed_ts,max_pair_id,
funded,paper_authority,live_authority)
SELECT substr(observed_ts,1,10),ticker,comparison_status,authoritative_side,inferred_side,COUNT(*),
SUM(count_units),SUM(count_units*yes_price),
SUM(source_clock_status NOT IN ('','unknown','arrival_only')),
MIN(observed_ts),MAX(observed_ts),MAX(pair_id),0,0,0
FROM research_flow_direction_pairs
WHERE pair_id IN (SELECT pair_id FROM research_flow_direction_pairs
 WHERE observed_ts<? AND NOT EXISTS(
  SELECT 1 FROM research_step7_projection_jobs j
  WHERE j.raw_kind='flow' AND j.raw_id=research_flow_direction_pairs.pair_id
   AND j.completed_ts='')
 ORDER BY observed_ts,pair_id LIMIT ?)
GROUP BY substr(observed_ts,1,10),ticker,comparison_status,authoritative_side,inferred_side
ON CONFLICT(utc_day,ticker,comparison_status,authoritative_side,inferred_side) DO UPDATE SET
 pair_count=pair_count+excluded.pair_count,
 contract_units=contract_units+excluded.contract_units,
 yes_price_units=yes_price_units+excluded.yes_price_units,
 complete_clock_count=complete_clock_count+excluded.complete_clock_count,
 first_observed_ts=MIN(first_observed_ts,excluded.first_observed_ts),
 last_observed_ts=MAX(last_observed_ts,excluded.last_observed_ts),
 max_pair_id=MAX(max_pair_id,excluded.max_pair_id)`,
		cut, flowArchiveBatch); err != nil {
		return 0, err
	}
	if res, err := tx.ExecContext(ctx, `DELETE FROM research_flow_direction_pairs WHERE pair_id IN (
 SELECT pair_id FROM research_flow_direction_pairs WHERE observed_ts<? AND NOT EXISTS(
  SELECT 1 FROM research_step7_projection_jobs j
  WHERE j.raw_kind='flow' AND j.raw_id=research_flow_direction_pairs.pair_id
   AND j.completed_ts='')
 ORDER BY observed_ts,pair_id LIMIT ?)`, cut, flowArchiveBatch); err != nil {
		return 0, err
	} else {
		n, _ := res.RowsAffected()
		total += n
	}
	for _, q := range []string{
		`DELETE FROM research_replenishment_marks WHERE observed_ts<? AND NOT EXISTS(
		 SELECT 1 FROM research_step7_projection_jobs j WHERE j.raw_kind='mark'
		  AND j.raw_id=research_replenishment_marks.episode_id||'|'||
		   research_replenishment_marks.event_type||'|'||research_replenishment_marks.horizon_ms
		  AND j.completed_ts='')`,
		`DELETE FROM research_replenishment_episodes WHERE observed_ts<? AND NOT EXISTS(
		 SELECT 1 FROM research_step7_projection_jobs j WHERE j.raw_kind='episode'
		  AND j.raw_id=research_replenishment_episodes.episode_id AND j.completed_ts='')
		 AND NOT EXISTS(SELECT 1 FROM research_replenishment_marks m
		  WHERE m.episode_id=research_replenishment_episodes.episode_id)`,
		`DELETE FROM research_maker_salvage_events WHERE observed_ts<?`,
		`DELETE FROM research_maker_salvage_trials WHERE observed_ts<?`,
		`DELETE FROM research_incentive_lock_evaluations WHERE observed_ts<?`,
	} {
		res, qerr := tx.ExecContext(ctx, q, cut)
		if qerr != nil {
			return 0, qerr
		}
		n, _ := res.RowsAffected()
		total += n
	}
	// Completed job, attempt, and completion rows are durable replay tombstones. The bulky raw
	// source may age out, but keeping its tiny immutable identity prevents a later duplicate source
	// replay from being projected a second time.
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return total, nil
}
