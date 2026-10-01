package storage

// A separate immutable promotion contract for proper-score momentum reductions. The generic bridge
// is intentionally BUY-only. This table preserves SELL action, owned side, signed prior position,
// actual proceeds, and the synthetic opposite-side cost used by the statistical grade.

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

const properMomentumPromotionSchema = `
CREATE TABLE IF NOT EXISTS proper_score_momentum_sell_intents (
 intent_id TEXT PRIMARY KEY,
 source_id TEXT NOT NULL UNIQUE,
 created_ts TEXT NOT NULL,
 sealed_inference_run_id INTEGER NOT NULL,
 sealed_result_hash TEXT NOT NULL,
 preregistration_id TEXT NOT NULL,
 observation_id INTEGER NOT NULL UNIQUE,
 cohort TEXT NOT NULL,
 ticker TEXT NOT NULL,
 synthetic_side TEXT NOT NULL CHECK(synthetic_side IN ('YES','NO')),
 owned_side TEXT NOT NULL CHECK(owned_side IN ('YES','NO')),
 required_prior_position REAL NOT NULL,
 paper_sell_price REAL NOT NULL CHECK(paper_sell_price>0 AND paper_sell_price<1),
 paper_sell_fee REAL NOT NULL CHECK(paper_sell_fee>=0),
 current_expected_net_lower REAL NOT NULL CHECK(current_expected_net_lower>0),
 proof_mean_synthetic_all_in REAL NOT NULL CHECK(proof_mean_synthetic_all_in>0),
 proof_lower_pc REAL NOT NULL CHECK(proof_lower_pc>0),
 required_edge_floor REAL NOT NULL CHECK(required_edge_floor>0),
 max_synthetic_all_in REAL NOT NULL CHECK(max_synthetic_all_in>0),
 preprice_book_source TEXT NOT NULL DEFAULT '',
 preprice_source_clock_id TEXT NOT NULL DEFAULT '',
 preprice_source_clock_ts TEXT NOT NULL DEFAULT '',
 preprice_visible_depth REAL NOT NULL DEFAULT 0 CHECK(preprice_visible_depth>=0),
 preprice_tick_size REAL NOT NULL DEFAULT 0 CHECK(preprice_tick_size>=0),
 preprice_fee_source TEXT NOT NULL DEFAULT '',
 execution_delay_ms INTEGER NOT NULL DEFAULT 0 CHECK(execution_delay_ms>=0),
 execute_after_ts TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(sealed_inference_run_id) REFERENCES research_inference_runs(id),
 FOREIGN KEY(preregistration_id) REFERENCES research_inference_preregistrations(preregistration_id),
 FOREIGN KEY(observation_id) REFERENCES research_system_observations(id),
 CHECK(ABS(ABS(required_prior_position)-1)<=0.000000001),
 CHECK((owned_side='YES' AND required_prior_position>0) OR (owned_side='NO' AND required_prior_position<0)),
 CHECK((1-paper_sell_price)+paper_sell_fee<=max_synthetic_all_in+0.000000001)
);
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_intents_require_sealed BEFORE INSERT ON proper_score_momentum_sell_intents
WHEN NOT EXISTS (
 SELECT 1 FROM research_inference_runs r
 JOIN research_inference_system_results sr ON sr.run_id=r.id AND sr.system_id='proper-score-executor'
 JOIN research_inference_preregistrations p ON p.preregistration_id=r.preregistration_id
 JOIN research_system_observations o ON o.id=NEW.observation_id
 WHERE r.id=NEW.sealed_inference_run_id AND r.status='sealed_preregistered_untouched'
  AND (r.result_hash=NEW.sealed_result_hash OR r.result_hash='sha256:'||NEW.sealed_result_hash)
  AND sr.state='PREREGISTERED_UNTOUCHED_PASS' AND sr.preregistered_untouched_gate_pass=1
  AND sr.execution_candidate=1 AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
  AND sr.funded=0 AND sr.paper_authority=0 AND sr.live_authority=0
  AND p.preregistration_id=NEW.preregistration_id AND p.system_id='proper-score-executor'
  AND p.experiment_version=1 AND p.cohort=NEW.cohort AND p.venue='kalshi' AND p.route='taker'
  AND o.system_id='proper-score-executor' AND o.experiment_version=1 AND o.cohort=NEW.cohort
  AND o.venue='kalshi' AND o.route='taker' AND o.observation_kind='candidate' AND o.candidate=1
  AND o.blocker='' AND o.certificate_status='verified' AND o.size_units=1 AND o.visible_capacity>=1
  AND o.latency_known=1 AND o.quote_age_known=1 AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1
  AND UPPER(COALESCE(json_extract(o.inputs_json,'$.promotion_action'),''))='SELL'
  AND UPPER(COALESCE(json_extract(o.inputs_json,'$.owned_side'),''))=NEW.owned_side
  AND ABS(CAST(json_extract(o.inputs_json,'$.required_prior_position') AS REAL)-NEW.required_prior_position)<=0.000000001
  AND o.ticker=NEW.ticker AND o.side=NEW.synthetic_side
) BEGIN SELECT RAISE(ABORT,'momentum SELL intent lacks sealed exact typed candidate'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_intents_no_update BEFORE UPDATE ON proper_score_momentum_sell_intents BEGIN SELECT RAISE(ABORT,'immutable momentum SELL intent'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_intents_no_delete BEFORE DELETE ON proper_score_momentum_sell_intents BEGIN SELECT RAISE(ABORT,'immutable momentum SELL intent'); END;

CREATE TABLE IF NOT EXISTS proper_score_momentum_sell_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 intent_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('paper_accepted','paper_rejected','live_dispatched','live_rejected','live_ambiguous')),
 execution_price REAL NOT NULL CHECK(execution_price>=0 AND execution_price<1),
 fee REAL NOT NULL CHECK(fee>=0),
 reason TEXT NOT NULL,
 FOREIGN KEY(intent_id) REFERENCES proper_score_momentum_sell_intents(intent_id),
 UNIQUE(intent_id,event_type)
);
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_events_require_paper BEFORE INSERT ON proper_score_momentum_sell_events
WHEN NEW.event_type='live_dispatched' AND NOT EXISTS (
 SELECT 1 FROM proper_score_momentum_sell_events e WHERE e.intent_id=NEW.intent_id AND e.event_type='paper_accepted'
) BEGIN SELECT RAISE(ABORT,'momentum LIVE SELL lacks accepted Paper SELL'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_events_require_ceiling BEFORE INSERT ON proper_score_momentum_sell_events
WHEN NEW.event_type IN ('paper_accepted','live_dispatched') AND NOT EXISTS (
 SELECT 1 FROM proper_score_momentum_sell_intents i WHERE i.intent_id=NEW.intent_id
  AND NEW.execution_price>0 AND (1-NEW.execution_price)+NEW.fee<=i.max_synthetic_all_in+0.000000001
) BEGIN SELECT RAISE(ABORT,'momentum SELL receipt exceeds sealed synthetic all-in ceiling'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_events_one_paper BEFORE INSERT ON proper_score_momentum_sell_events
WHEN NEW.event_type IN ('paper_accepted','paper_rejected') AND EXISTS (
 SELECT 1 FROM proper_score_momentum_sell_events e WHERE e.intent_id=NEW.intent_id AND e.event_type IN ('paper_accepted','paper_rejected')
) BEGIN SELECT RAISE(ABORT,'momentum SELL intent already has Paper outcome'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_events_one_live BEFORE INSERT ON proper_score_momentum_sell_events
WHEN NEW.event_type IN ('live_dispatched','live_rejected','live_ambiguous') AND EXISTS (
 SELECT 1 FROM proper_score_momentum_sell_events e WHERE e.intent_id=NEW.intent_id
  AND e.event_type IN ('live_dispatched','live_rejected','live_ambiguous')
) BEGIN SELECT RAISE(ABORT,'momentum SELL intent already has LIVE outcome'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_events_no_update BEFORE UPDATE ON proper_score_momentum_sell_events BEGIN SELECT RAISE(ABORT,'immutable momentum SELL event'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_sell_events_no_delete BEFORE DELETE ON proper_score_momentum_sell_events BEGIN SELECT RAISE(ABORT,'immutable momentum SELL event'); END;

CREATE TABLE IF NOT EXISTS proper_score_momentum_paper_execution_receipts (
 intent_id TEXT PRIMARY KEY,
 observed_ts TEXT NOT NULL,
 book_source TEXT NOT NULL,
 source_clock_id TEXT NOT NULL,
 source_clock_ts TEXT NOT NULL,
 visible_depth REAL NOT NULL CHECK(visible_depth>0),
 tick_size REAL NOT NULL CHECK(tick_size>0),
 execution_price REAL NOT NULL CHECK(execution_price>0 AND execution_price<1),
 fee REAL NOT NULL CHECK(fee>=0),
 fee_source TEXT NOT NULL,
 requested_qty REAL NOT NULL CHECK(requested_qty>0),
 state TEXT NOT NULL CHECK(state IN ('filled','rejected')),
 reason TEXT NOT NULL,
 FOREIGN KEY(intent_id) REFERENCES proper_score_momentum_sell_intents(intent_id)
);
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_paper_execution_receipts_no_update BEFORE UPDATE ON proper_score_momentum_paper_execution_receipts BEGIN SELECT RAISE(ABORT,'immutable momentum Paper execution receipt'); END;
CREATE TRIGGER IF NOT EXISTS proper_score_momentum_paper_execution_receipts_no_delete BEFORE DELETE ON proper_score_momentum_paper_execution_receipts BEGIN SELECT RAISE(ABORT,'immutable momentum Paper execution receipt'); END;
`

func migrateProperMomentumPromotionSchema(db *sql.DB) error {
	if _, err := db.Exec(properMomentumPromotionSchema); err != nil {
		return err
	}
	// Existing databases predate the immutable preprice receipt. Defaults are deliberately empty:
	// legacy intents then fail closed instead of pretending their first book clock was persisted.
	for _, column := range []string{
		"preprice_book_source TEXT NOT NULL DEFAULT ''",
		"preprice_source_clock_id TEXT NOT NULL DEFAULT ''",
		"preprice_source_clock_ts TEXT NOT NULL DEFAULT ''",
		"preprice_visible_depth REAL NOT NULL DEFAULT 0 CHECK(preprice_visible_depth>=0)",
		"preprice_tick_size REAL NOT NULL DEFAULT 0 CHECK(preprice_tick_size>=0)",
		"preprice_fee_source TEXT NOT NULL DEFAULT ''",
		"execution_delay_ms INTEGER NOT NULL DEFAULT 0 CHECK(execution_delay_ms>=0)",
		"execute_after_ts TEXT NOT NULL DEFAULT ''",
	} {
		if _, err := db.Exec(`ALTER TABLE proper_score_momentum_sell_intents ADD COLUMN ` + column); err != nil &&
			!strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return err
		}
	}
	return nil
}

type ProperMomentumSellCandidate struct {
	ObservationID                                                  int64
	Observed                                                       time.Time
	Cohort, Ticker, Title, SyntheticSide, OwnedSide, SourceClockID string
	RequiredPrior, ObservedSellPrice, ObservedSellFee              float64
	ExpectedNetLower                                               float64
}

type ProperMomentumSellBookReceipt struct {
	ObservedAt, SourceClockAt                           time.Time
	BookSource, SourceClockID, FeeSource, State, Reason string
	VisibleDepth, TickSize, Price, Fee, RequestedQty    float64
}

type ProperMomentumSellIntent struct {
	IntentID, SourceID                                                 string
	Created, ExecuteAfter                                              time.Time
	Proof                                                              ResearchPromotionProof
	Candidate                                                          ProperMomentumSellCandidate
	PaperSellPrice, PaperSellFee, RequiredEdgeFloor, MaxSyntheticAllIn float64
	Preprice                                                           ProperMomentumSellBookReceipt
	ExecutionDelayMS                                                   int64
}

type ProperMomentumSellPaperState struct {
	State, Reason  string
	SellPrice, Fee float64
}

// ProperMomentumPaperOpen is the only Paper lot shape eligible for the typed momentum SELL:
// exactly one untouched one-contract BUY in the current open epoch. Keeping the source here (and
// rechecking it in the write transaction) prevents paper.Aggregate's first-source label from
// laundering a mixed-source position into a promoted reduction.
type ProperMomentumPaperOpen struct {
	LastFillID int64
	Title      string
	Source     string
}

func ExactProperMomentumPaperOpen(fills []paper.Fill, ticker, ownedSide string) (ProperMomentumPaperOpen, bool) {
	ownedSide = strings.ToUpper(strings.TrimSpace(ownedSide))
	if ticker == "" || (ownedSide != "YES" && ownedSide != "NO") {
		return ProperMomentumPaperOpen{}, false
	}
	opposite := "NO"
	if ownedSide == "NO" {
		opposite = "YES"
	}
	var out ProperMomentumPaperOpen
	ownedQty, oppositeQty := 0.0, 0.0
	currentBuyCount := 0
	currentBuyQty := 0.0
	currentTouchedBySell := false
	valid := true
	for _, fill := range fills {
		if !strings.EqualFold(fill.Platform, "kalshi") || fill.Ticker != ticker {
			continue
		}
		if fill.ID > out.LastFillID {
			out.LastFillID = fill.ID
		}
		side := strings.ToUpper(strings.TrimSpace(fill.Side))
		action := strings.ToUpper(strings.TrimSpace(fill.Action))
		if (side != ownedSide && side != opposite) || (action != "BUY" && action != "SELL") ||
			fill.Contracts <= 0 || math.IsNaN(fill.Contracts) || math.IsInf(fill.Contracts, 0) {
			valid = false
			continue
		}
		qty := &oppositeQty
		if side == ownedSide {
			qty = &ownedQty
		}
		if action == "SELL" {
			if fill.Contracts > *qty+1e-8 {
				valid = false
			}
			applied := math.Min(fill.Contracts, *qty)
			*qty -= applied
			if math.Abs(*qty) <= 1e-8 {
				*qty = 0
			}
			if side == ownedSide {
				currentTouchedBySell = true
				if ownedQty == 0 {
					currentBuyCount, currentBuyQty, currentTouchedBySell = 0, 0, false
					out.Title, out.Source = "", ""
				}
			}
			continue
		}
		if side == ownedSide {
			if ownedQty == 0 {
				currentBuyCount, currentBuyQty, currentTouchedBySell = 0, 0, false
				out.Title, out.Source = "", ""
			}
			currentBuyCount++
			currentBuyQty += fill.Contracts
			if currentBuyCount == 1 {
				out.Title, out.Source = fill.Title, fill.Source
			}
		}
		*qty += fill.Contracts
	}
	if !valid || math.Abs(ownedQty-1) > 1e-8 || math.Abs(oppositeQty) > 1e-8 ||
		currentBuyCount != 1 || math.Abs(currentBuyQty-1) > 1e-8 || currentTouchedBySell ||
		strings.TrimSpace(out.Source) == "" {
		return out, false
	}
	return out, true
}

func ProperMomentumSellIntentFromSource(source string) string {
	id, ok := strings.CutPrefix(strings.TrimSpace(source), "r139ms:")
	if !ok || len(id) != 64 {
		return ""
	}
	if _, err := hex.DecodeString(id); err != nil {
		return ""
	}
	return id
}

func (s *Store) LatestProperMomentumSellCandidate(ctx context.Context, p ResearchPromotionProof,
	since time.Time) (ProperMomentumSellCandidate, bool, error) {
	var out ProperMomentumSellCandidate
	var observed string
	err := s.db.QueryRowContext(ctx, `SELECT o.id,o.observed_ts,o.cohort,o.ticker,COALESCE(e.title,''),
o.side,UPPER(CAST(json_extract(o.inputs_json,'$.owned_side') AS TEXT)),
CAST(json_extract(o.inputs_json,'$.required_prior_position') AS REAL),
CAST(json_extract(o.inputs_json,'$.actual_sale_proceeds') AS REAL),o.exact_fee,o.source_clock_id,
CAST(json_extract(o.inputs_json,'$.promotion_expected_net_lower') AS REAL)
FROM research_system_observations o JOIN research_event_specs e
 ON e.event_id=o.canonical_event_id AND e.version=o.event_version
WHERE o.system_id=? AND o.experiment_version=? AND o.cohort=? AND o.venue='kalshi' AND o.route='taker'
 AND o.observation_kind='candidate' AND o.candidate=1 AND o.blocker='' AND o.certificate_status='verified'
 AND o.size_units=1 AND o.visible_capacity>=1 AND o.latency_known=1 AND o.quote_age_known=1
 AND o.tick_known=1 AND o.depth_known=1 AND o.fee_known=1
 AND UPPER(COALESCE(json_extract(o.inputs_json,'$.promotion_action'),''))='SELL'
 AND o.observed_ts>=? ORDER BY o.observed_ts DESC,o.id DESC LIMIT 1`, p.SystemID,
		p.ExperimentVersion, p.Cohort, since.UTC().Format(time.RFC3339Nano)).Scan(&out.ObservationID,
		&observed, &out.Cohort, &out.Ticker, &out.Title, &out.SyntheticSide, &out.OwnedSide,
		&out.RequiredPrior, &out.ObservedSellPrice, &out.ObservedSellFee, &out.SourceClockID,
		&out.ExpectedNetLower)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	out.Observed = parsePromotionTime(observed)
	if out.Title == "" {
		out.Title = out.Ticker
	}
	if (out.OwnedSide != "YES" && out.OwnedSide != "NO") ||
		(out.SyntheticSide != "YES" && out.SyntheticSide != "NO") || out.OwnedSide == out.SyntheticSide ||
		math.Abs(math.Abs(out.RequiredPrior)-1) > 1e-8 || out.ObservedSellPrice <= 0 || out.ObservedSellPrice >= 1 ||
		out.ObservedSellFee < 0 || out.ExpectedNetLower <= 0 || out.SourceClockID == "" {
		return ProperMomentumSellCandidate{}, false, errors.New("malformed typed momentum SELL candidate")
	}
	return out, true, nil
}

func properMomentumIntentID(p ResearchPromotionProof, c ProperMomentumSellCandidate) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d|%s|%d|SELL|%s|%.9f", p.RunID, p.ResultHash,
		c.ObservationID, c.OwnedSide, c.RequiredPrior)))
	return hex.EncodeToString(h[:])
}

func (s *Store) InsertProperMomentumSellIntent(ctx context.Context, p ResearchPromotionProof,
	c ProperMomentumSellCandidate, preprice ProperMomentumSellBookReceipt, edgeFloor float64,
	delay time.Duration) (ProperMomentumSellIntent, bool, error) {
	sellPrice, sellFee := preprice.Price, preprice.Fee
	out := ProperMomentumSellIntent{Proof: p, Candidate: c, Created: time.Now().UTC(),
		PaperSellPrice: sellPrice, PaperSellFee: sellFee, RequiredEdgeFloor: edgeFloor,
		Preprice: preprice, ExecutionDelayMS: delay.Milliseconds()}
	out.ExecuteAfter = out.Created.Add(delay)
	out.IntentID = properMomentumIntentID(p, c)
	out.SourceID = "r139ms:" + out.IntentID
	out.MaxSyntheticAllIn = p.MeanAllIn + p.LowerPC - edgeFloor
	currentAllIn := 1 - sellPrice + sellFee
	currentExpected := math.Min(p.LowerPC-math.Max(0, currentAllIn-p.MeanAllIn), 1-currentAllIn)
	out.Candidate.ExpectedNetLower = currentExpected
	if p.SystemID != "proper-score-executor" || p.Venue != "kalshi" || p.Route != "taker" ||
		p.RunID <= 0 || !validSHA256(p.ResultHash) || c.ObservationID <= 0 || edgeFloor <= 0 ||
		c.OwnedSide == c.SyntheticSide || math.Abs(math.Abs(c.RequiredPrior)-1) > 1e-8 || sellPrice <= 0 || sellPrice >= 1 ||
		sellFee < 0 || !finiteResearchNumber(out.MaxSyntheticAllIn) || currentExpected < edgeFloor ||
		currentAllIn > out.MaxSyntheticAllIn+1e-9 || delay <= 0 ||
		strings.TrimSpace(preprice.BookSource) == "" || strings.TrimSpace(preprice.SourceClockID) == "" ||
		preprice.SourceClockAt.IsZero() || preprice.VisibleDepth < 1 || preprice.TickSize <= 0 ||
		strings.TrimSpace(preprice.FeeSource) == "" {
		return out, false, errors.New("current Paper SELL does not fit sealed momentum proof")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO proper_score_momentum_sell_intents(
intent_id,source_id,created_ts,sealed_inference_run_id,sealed_result_hash,preregistration_id,
observation_id,cohort,ticker,synthetic_side,owned_side,required_prior_position,paper_sell_price,
paper_sell_fee,current_expected_net_lower,proof_mean_synthetic_all_in,proof_lower_pc,
required_edge_floor,max_synthetic_all_in,preprice_book_source,preprice_source_clock_id,
preprice_source_clock_ts,preprice_visible_depth,preprice_tick_size,preprice_fee_source,
execution_delay_ms,execute_after_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		out.IntentID, out.SourceID, out.Created.Format(time.RFC3339Nano), p.RunID, p.ResultHash,
		p.PreregistrationID, c.ObservationID, c.Cohort, c.Ticker, c.SyntheticSide, c.OwnedSide,
		c.RequiredPrior, sellPrice, sellFee, currentExpected, p.MeanAllIn, p.LowerPC, edgeFloor,
		out.MaxSyntheticAllIn, preprice.BookSource, preprice.SourceClockID,
		preprice.SourceClockAt.UTC().Format(time.RFC3339Nano), preprice.VisibleDepth,
		preprice.TickSize, preprice.FeeSource, out.ExecutionDelayMS,
		out.ExecuteAfter.Format(time.RFC3339Nano))
	if err != nil {
		return out, false, err
	}
	n, err := res.RowsAffected()
	return out, n > 0, err
}

func scanProperMomentumIntent(scanner interface{ Scan(...any) error }) (ProperMomentumSellIntent, error) {
	var out ProperMomentumSellIntent
	var created, observed, prepriceClockAt, executeAfter string
	err := scanner.Scan(&out.IntentID, &out.SourceID, &created, &out.Proof.RunID, &out.Proof.ResultHash,
		&out.Proof.PreregistrationID, &out.Candidate.ObservationID, &out.Candidate.Cohort,
		&out.Candidate.Ticker, &out.Candidate.SyntheticSide, &out.Candidate.OwnedSide,
		&out.Candidate.RequiredPrior, &out.PaperSellPrice, &out.PaperSellFee,
		&out.Candidate.ExpectedNetLower, &out.Proof.MeanAllIn, &out.Proof.LowerPC,
		&out.RequiredEdgeFloor, &out.MaxSyntheticAllIn, &observed, &out.Candidate.SourceClockID,
		&out.Candidate.Title, &out.Candidate.ObservedSellPrice, &out.Candidate.ObservedSellFee,
		&out.Preprice.BookSource, &out.Preprice.SourceClockID, &prepriceClockAt,
		&out.Preprice.VisibleDepth, &out.Preprice.TickSize, &out.Preprice.FeeSource,
		&out.ExecutionDelayMS, &executeAfter)
	out.Created = parsePromotionTime(created)
	out.Candidate.Observed = parsePromotionTime(observed)
	out.Preprice.ObservedAt = out.Created
	out.Preprice.SourceClockAt = parsePromotionTime(prepriceClockAt)
	out.Preprice.Price, out.Preprice.Fee, out.Preprice.RequestedQty =
		out.PaperSellPrice, out.PaperSellFee, 1
	out.ExecuteAfter = parsePromotionTime(executeAfter)
	out.Proof.SystemID, out.Proof.ExperimentVersion, out.Proof.Cohort = "proper-score-executor", 1, out.Candidate.Cohort
	out.Proof.Venue, out.Proof.Route = "kalshi", "taker"
	return out, err
}

const properMomentumIntentSelect = `SELECT i.intent_id,i.source_id,i.created_ts,i.sealed_inference_run_id,
i.sealed_result_hash,i.preregistration_id,i.observation_id,i.cohort,i.ticker,i.synthetic_side,i.owned_side,
i.required_prior_position,i.paper_sell_price,i.paper_sell_fee,i.current_expected_net_lower,
i.proof_mean_synthetic_all_in,i.proof_lower_pc,i.required_edge_floor,i.max_synthetic_all_in,
COALESCE(o.observed_ts,''),COALESCE(o.source_clock_id,''),COALESCE(e.title,i.ticker),
COALESCE(CAST(json_extract(o.inputs_json,'$.actual_sale_proceeds') AS REAL),0),
COALESCE(o.exact_fee,0),i.preprice_book_source,i.preprice_source_clock_id,
i.preprice_source_clock_ts,i.preprice_visible_depth,i.preprice_tick_size,i.preprice_fee_source,
i.execution_delay_ms,i.execute_after_ts
FROM proper_score_momentum_sell_intents i
LEFT JOIN research_system_observations o ON o.id=i.observation_id
LEFT JOIN research_event_specs e ON e.event_id=o.canonical_event_id AND e.version=o.event_version`

func (s *Store) ProperMomentumSellIntentForSource(ctx context.Context, source string) (ProperMomentumSellIntent, bool, error) {
	id := ProperMomentumSellIntentFromSource(source)
	if id == "" {
		return ProperMomentumSellIntent{}, false, nil
	}
	out, err := scanProperMomentumIntent(s.db.QueryRowContext(ctx, properMomentumIntentSelect+` WHERE i.intent_id=?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return ProperMomentumSellIntent{}, false, nil
	}
	return out, err == nil, err
}

func (s *Store) AcceptedProperMomentumSellIntent(ctx context.Context, source string) (ProperMomentumSellIntent, bool, error) {
	id := ProperMomentumSellIntentFromSource(source)
	if id == "" {
		return ProperMomentumSellIntent{}, false, nil
	}
	out, err := scanProperMomentumIntent(s.db.QueryRowContext(ctx, properMomentumIntentSelect+`
 WHERE i.intent_id=? AND EXISTS(SELECT 1 FROM proper_score_momentum_sell_events pe
  WHERE pe.intent_id=i.intent_id AND pe.event_type='paper_accepted')
 AND NOT EXISTS(SELECT 1 FROM proper_score_momentum_sell_events pe
  WHERE pe.intent_id=i.intent_id AND pe.event_type='paper_rejected')`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return ProperMomentumSellIntent{}, false, nil
	}
	return out, err == nil, err
}

func (s *Store) UnresolvedProperMomentumSellIntents(ctx context.Context, limit int) ([]ProperMomentumSellIntent, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT source_id FROM proper_score_momentum_sell_intents i
WHERE NOT EXISTS(SELECT 1 FROM proper_score_momentum_sell_events e WHERE e.intent_id=i.intent_id
 AND e.event_type IN ('paper_accepted','paper_rejected')) ORDER BY created_ts LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []string
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	var out []ProperMomentumSellIntent
	for _, source := range sources {
		row, ok, err := s.ProperMomentumSellIntentForSource(ctx, source)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, row)
		}
	}
	return out, rows.Err()
}

// PendingProperMomentumLiveSellIntents makes the Paper->LIVE handoff crash recoverable. An accepted
// Paper action remains eligible for re-enqueue until a durable LIVE outcome (filled, rejected, or
// ambiguous) exists; the in-memory live queue is deliberately not treated as authority.
func (s *Store) PendingProperMomentumLiveSellIntents(ctx context.Context, limit int) ([]ProperMomentumSellIntent, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT source_id FROM proper_score_momentum_sell_intents i
WHERE EXISTS(SELECT 1 FROM proper_score_momentum_sell_events e WHERE e.intent_id=i.intent_id
 AND e.event_type='paper_accepted')
 AND NOT EXISTS(SELECT 1 FROM proper_score_momentum_sell_events e WHERE e.intent_id=i.intent_id
 AND e.event_type IN ('live_dispatched','live_rejected','live_ambiguous'))
ORDER BY created_ts LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []string
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
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var out []ProperMomentumSellIntent
	for _, source := range sources {
		row, ok, err := s.AcceptedProperMomentumSellIntent(ctx, source)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, row)
		}
	}
	return out, nil
}

func (s *Store) AppendProperMomentumSellEvent(ctx context.Context, in ProperMomentumSellIntent,
	eventType string, executionPrice, fee float64, reason string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO proper_score_momentum_sell_events(
intent_id,observed_ts,event_type,execution_price,fee,reason) VALUES(?,?,?,?,?,?)`, in.IntentID,
		nowRFC(), eventType, math.Max(0, executionPrice), fee, firstNonEmptyStorage(reason, eventType))
	return err
}

func validProperMomentumSellBookReceipt(in ProperMomentumSellBookReceipt) bool {
	return !in.ObservedAt.IsZero() && !in.SourceClockAt.IsZero() &&
		strings.TrimSpace(in.BookSource) != "" && strings.TrimSpace(in.SourceClockID) != "" &&
		strings.TrimSpace(in.FeeSource) != "" && in.VisibleDepth > 0 && in.TickSize > 0 &&
		in.Price > 0 && in.Price < 1 && in.Fee >= 0 && in.RequestedQty > 0 &&
		(in.State == "filled" || in.State == "rejected") && strings.TrimSpace(in.Reason) != ""
}

func insertProperMomentumExecutionReceiptTx(ctx context.Context, tx *sql.Tx, intentID string,
	receipt ProperMomentumSellBookReceipt) error {
	if strings.TrimSpace(intentID) == "" || !validProperMomentumSellBookReceipt(receipt) {
		return errors.New("invalid proper momentum Paper execution receipt")
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO proper_score_momentum_paper_execution_receipts(
intent_id,observed_ts,book_source,source_clock_id,source_clock_ts,visible_depth,tick_size,
execution_price,fee,fee_source,requested_qty,state,reason) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		intentID, receipt.ObservedAt.UTC().Format(time.RFC3339Nano), receipt.BookSource,
		receipt.SourceClockID, receipt.SourceClockAt.UTC().Format(time.RFC3339Nano),
		receipt.VisibleDepth, receipt.TickSize, receipt.Price, receipt.Fee, receipt.FeeSource,
		receipt.RequestedQty, receipt.State, receipt.Reason)
	return err
}

func (s *Store) ProperMomentumSellExecutionReceipt(ctx context.Context,
	intentID string) (ProperMomentumSellBookReceipt, bool, error) {
	var out ProperMomentumSellBookReceipt
	var observed, clockAt string
	err := s.db.QueryRowContext(ctx, `SELECT observed_ts,book_source,source_clock_id,source_clock_ts,
visible_depth,tick_size,execution_price,fee,fee_source,requested_qty,state,reason
FROM proper_score_momentum_paper_execution_receipts WHERE intent_id=?`, intentID).Scan(
		&observed, &out.BookSource, &out.SourceClockID, &clockAt, &out.VisibleDepth,
		&out.TickSize, &out.Price, &out.Fee, &out.FeeSource, &out.RequestedQty,
		&out.State, &out.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	out.ObservedAt, out.SourceClockAt = parsePromotionTime(observed), parsePromotionTime(clockAt)
	return out, true, nil
}

// AppendProperMomentumPaperRejected atomically retains the exact delayed execution book (when one
// existed) beside the immutable Paper rejection. Recovery can therefore rebuild the comparison
// ledger without relying on an in-memory event that may have died with the process.
func (s *Store) AppendProperMomentumPaperRejected(ctx context.Context,
	in ProperMomentumSellIntent, executionPrice, fee float64, reason string,
	receipt *ProperMomentumSellBookReceipt) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if receipt != nil {
		if err := insertProperMomentumExecutionReceiptTx(ctx, tx, in.IntentID, *receipt); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO proper_score_momentum_sell_events(
intent_id,observed_ts,event_type,execution_price,fee,reason) VALUES(?,?,?,?,?,?)`, in.IntentID,
		nowRFC(), "paper_rejected", math.Max(0, executionPrice), fee,
		firstNonEmptyStorage(reason, "paper_rejected")); err != nil {
		return err
	}
	return tx.Commit()
}

type ProperMomentumSellEventState struct {
	PaperEvent, PaperReason, LiveEvent, LiveReason string
	PaperPrice, PaperFee                           float64
	PaperAt, LiveAt                                time.Time
}

func (s *Store) ProperMomentumSellEventState(ctx context.Context,
	intentID string) (ProperMomentumSellEventState, error) {
	var out ProperMomentumSellEventState
	rows, err := s.db.QueryContext(ctx, `SELECT event_type,execution_price,fee,reason,observed_ts
FROM proper_score_momentum_sell_events WHERE intent_id=? ORDER BY id`, intentID)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var event string
		var price, fee float64
		var reason, observed string
		if err := rows.Scan(&event, &price, &fee, &reason, &observed); err != nil {
			return out, err
		}
		if event == "paper_accepted" || event == "paper_rejected" {
			out.PaperEvent, out.PaperPrice, out.PaperFee, out.PaperReason = event, price, fee, reason
			out.PaperAt = parsePromotionTime(observed)
		} else if strings.HasPrefix(event, "live_") {
			out.LiveEvent, out.LiveReason = event, reason
			out.LiveAt = parsePromotionTime(observed)
		}
	}
	return out, rows.Err()
}

func (s *Store) CompletedProperMomentumSellIntents(ctx context.Context) ([]ProperMomentumSellIntent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT source_id FROM proper_score_momentum_sell_intents i
WHERE EXISTS(SELECT 1 FROM proper_score_momentum_sell_events e WHERE e.intent_id=i.intent_id
 AND e.event_type IN ('paper_accepted','paper_rejected')) ORDER BY created_ts,intent_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sources []string
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
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var out []ProperMomentumSellIntent
	for _, source := range sources {
		row, ok, err := s.ProperMomentumSellIntentForSource(ctx, source)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, row)
		}
	}
	return out, nil
}

func (s *Store) ProperMomentumSellPaperState(ctx context.Context, in ProperMomentumSellIntent,
	since time.Time) (ProperMomentumSellPaperState, error) {
	var out ProperMomentumSellPaperState
	err := s.db.QueryRowContext(ctx, `SELECT price,fee FROM paper_fills
WHERE source=? AND platform='kalshi' AND ticker=? AND UPPER(side)=? AND UPPER(action)='SELL'
 AND contracts=1 AND ts>=? ORDER BY id DESC LIMIT 1`, in.SourceID, in.Candidate.Ticker,
		in.Candidate.OwnedSide, since.UTC().Format(time.RFC3339)).Scan(&out.SellPrice, &out.Fee)
	if errors.Is(err, sql.ErrNoRows) {
		out.State = "missing"
		return out, nil
	}
	if err != nil {
		return out, err
	}
	if out.SellPrice <= 0 || out.SellPrice >= 1 || out.Fee < 0 ||
		(1-out.SellPrice)+out.Fee > in.MaxSyntheticAllIn+1e-9 {
		out.State, out.Reason = "terminal_rejected", "Paper SELL receipt violates typed price/fee ceiling"
		return out, nil
	}
	out.State = "filled"
	return out, nil
}

// InsertProperMomentumPaperSell atomically proves the Paper book still has the exact signed prior
// position seen by the typed intent and that no fill landed after the caller's snapshot.
func (s *Store) InsertProperMomentumPaperSell(ctx context.Context, fill paper.Fill,
	requiredPrior float64, expectedLastFillID int64, expectedOpenSource string) (int64, bool, error) {
	return s.insertProperMomentumPaperSell(ctx, fill, requiredPrior, expectedLastFillID,
		expectedOpenSource, nil)
}

func (s *Store) InsertProperMomentumPaperSellWithReceipt(ctx context.Context, fill paper.Fill,
	requiredPrior float64, expectedLastFillID int64, expectedOpenSource string,
	receipt ProperMomentumSellBookReceipt) (int64, bool, error) {
	return s.insertProperMomentumPaperSell(ctx, fill, requiredPrior, expectedLastFillID,
		expectedOpenSource, &receipt)
}

func (s *Store) insertProperMomentumPaperSell(ctx context.Context, fill paper.Fill,
	requiredPrior float64, expectedLastFillID int64, expectedOpenSource string,
	receipt *ProperMomentumSellBookReceipt) (int64, bool, error) {
	owned := strings.ToUpper(strings.TrimSpace(fill.Side))
	if fill.Platform != "kalshi" || (owned != "YES" && owned != "NO") || fill.Action != "SELL" ||
		fill.Contracts != 1 || fill.Price <= 0 || fill.Price >= 1 || fill.Fee < 0 ||
		((owned == "YES") != (requiredPrior > 0)) || math.Abs(math.Abs(requiredPrior)-1) > 1e-8 ||
		expectedLastFillID < 0 || strings.TrimSpace(expectedOpenSource) == "" {
		return 0, false, errors.New("invalid typed Paper momentum SELL")
	}
	if receipt != nil && (!validProperMomentumSellBookReceipt(*receipt) ||
		receipt.State != "filled" || math.Abs(receipt.Price-fill.Price) > 1e-9 ||
		math.Abs(receipt.Fee-fill.Fee) > 1e-9 || receipt.RequestedQty != 1) {
		return 0, false, errors.New("typed Paper momentum SELL receipt mismatch")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id,ts,platform,ticker,title,side,action,price,contracts,fee,
COALESCE(tp_price,0),COALESCE(sl_price,0),COALESCE(source,''),COALESCE(note,''),COALESCE(sig_px_at_fill,0),
COALESCE(fill_kind,''),COALESCE(route_reason,'') FROM paper_fills
WHERE platform='kalshi' AND ticker=? ORDER BY ts ASC,id ASC`, fill.Ticker)
	if err != nil {
		return 0, false, err
	}
	var tickerFills []paper.Fill
	for rows.Next() {
		var row paper.Fill
		if err := rows.Scan(&row.ID, &row.TS, &row.Platform, &row.Ticker, &row.Title, &row.Side,
			&row.Action, &row.Price, &row.Contracts, &row.Fee, &row.TP, &row.SL, &row.Source,
			&row.Note, &row.SigPxAtFill, &row.FillKind, &row.RouteReason); err != nil {
			rows.Close()
			return 0, false, err
		}
		tickerFills = append(tickerFills, row)
	}
	if err := rows.Close(); err != nil {
		return 0, false, err
	}
	open, exact := ExactProperMomentumPaperOpen(tickerFills, fill.Ticker, owned)
	if !exact || open.LastFillID != expectedLastFillID || open.Source != expectedOpenSource {
		return 0, false, nil
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO paper_fills(ts,platform,ticker,title,side,action,
price,contracts,fee,tp_price,sl_price,source,note,sig_px_at_fill,fill_kind,route_reason)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, nowRFC(), fill.Platform, fill.Ticker, fill.Title, owned,
		"SELL", fill.Price, 1, fill.Fee, 0, 0, fill.Source, fill.Note, fill.Price,
		"taker", "sealed-proper-score-momentum-reduce-only")
	if err != nil {
		return 0, false, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, false, err
	}
	if receipt != nil {
		intentID := ProperMomentumSellIntentFromSource(fill.Source)
		if intentID == "" {
			return 0, false, errors.New("typed Paper momentum SELL source lacks intent id")
		}
		if err := insertProperMomentumExecutionReceiptTx(ctx, tx, intentID, *receipt); err != nil {
			return 0, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, false, err
	}
	return id, true, nil
}
