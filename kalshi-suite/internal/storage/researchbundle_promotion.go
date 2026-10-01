package storage

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

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

const researchBundlePromotionSchema = `
CREATE TABLE IF NOT EXISTS research_route_bundle_promotion_intents (
 intent_id TEXT PRIMARY KEY,
 created_ts TEXT NOT NULL,
 bundle_id TEXT NOT NULL UNIQUE,
 sealed_inference_run_id INTEGER NOT NULL,
 sealed_result_hash TEXT NOT NULL,
 preregistration_id TEXT NOT NULL,
 system_id TEXT NOT NULL,
 experiment_version INTEGER NOT NULL CHECK(experiment_version>0),
 cohort TEXT NOT NULL,
 certificate_hash TEXT NOT NULL,
 state_vector_hash TEXT NOT NULL,
 collection_ticker TEXT NOT NULL,
 market_ticker TEXT NOT NULL,
 rfq_id TEXT NOT NULL,
 quote_id TEXT NOT NULL,
 quote_observed_ts TEXT NOT NULL,
 quote_status TEXT NOT NULL CHECK(quote_status='open'),
 quote_price REAL NOT NULL CHECK(quote_price>0 AND quote_price<1),
 quantity REAL NOT NULL CHECK(quantity=1),
 exact_fee REAL NOT NULL CHECK(exact_fee>=0),
 fee_source TEXT NOT NULL,
 all_in_unit REAL NOT NULL CHECK(all_in_unit>0),
 max_all_in_unit REAL NOT NULL CHECK(max_all_in_unit>0),
 current_expected_net_lower REAL NOT NULL CHECK(current_expected_net_lower>0),
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
 ordered_legs_hash TEXT NOT NULL,
 quote_hash TEXT NOT NULL,
 paper_parlay_id INTEGER NOT NULL UNIQUE,
 FOREIGN KEY(bundle_id) REFERENCES research_route_bundles(bundle_id),
 FOREIGN KEY(sealed_inference_run_id) REFERENCES research_inference_runs(id),
 FOREIGN KEY(preregistration_id) REFERENCES research_inference_preregistrations(preregistration_id),
 FOREIGN KEY(paper_parlay_id) REFERENCES paper_parlays(id),
 CHECK(all_in_unit<=max_all_in_unit+0.000000001),
 UNIQUE(rfq_id,quote_id)
);
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_requires_sealed_exact_contract BEFORE INSERT ON research_route_bundle_promotion_intents
WHEN NOT EXISTS (
 SELECT 1 FROM research_route_bundles b
 JOIN research_inference_runs r ON r.id=NEW.sealed_inference_run_id
 JOIN research_inference_preregistrations p ON p.preregistration_id=r.preregistration_id
 JOIN research_inference_system_results s ON s.run_id=r.id AND s.system_id=p.system_id
 WHERE b.bundle_id=NEW.bundle_id AND b.system_id=NEW.system_id AND b.experiment_version=NEW.experiment_version
  AND b.cohort=NEW.cohort AND b.certificate_hash=NEW.certificate_hash
  AND b.state_vector_hash=NEW.state_vector_hash AND b.certificate_status='verified'
  AND p.preregistration_id=NEW.preregistration_id AND p.system_id=NEW.system_id
  AND p.experiment_version=NEW.experiment_version AND p.cohort=NEW.cohort
  AND p.venue='kalshi' AND p.route='rfq'
  AND r.status='sealed_preregistered_untouched'
  AND (r.result_hash=NEW.sealed_result_hash OR r.result_hash='sha256:'||NEW.sealed_result_hash)
  AND s.state='PREREGISTERED_UNTOUCHED_PASS' AND s.preregistered_untouched_gate_pass=1
  AND s.execution_candidate=1 AND r.funded=0 AND r.paper_authority=0 AND r.live_authority=0
  AND s.funded=0 AND s.paper_authority=0 AND s.live_authority=0
  AND NEW.current_expected_net_lower>0 AND NEW.net_per_day_lower>0 AND NEW.capacity>=1
  AND NEW.capital_dollar_hours_per_day>=0
  AND ABS(NEW.mode4_fraction_ceiling-
   MIN(0.25,0.25*NEW.current_expected_net_lower/NEW.all_in_unit))<0.000000000001
  AND EXISTS (SELECT 1 FROM research_sealed_route_economics x
   WHERE x.receipt_id=NEW.route_economics_receipt_id AND x.sealed_inference_run_id=NEW.sealed_inference_run_id
    AND (x.sealed_result_hash=NEW.sealed_result_hash OR 'sha256:'||x.sealed_result_hash=NEW.sealed_result_hash)
    AND x.system_id=NEW.system_id AND x.route_id='rfq' AND x.venue='kalshi'
    AND x.net_per_day_lower=NEW.net_per_day_lower AND x.capacity=NEW.capacity
    AND x.capital_dollar_hours_per_day=NEW.capital_dollar_hours_per_day
    AND x.net_per_day_lower>0 AND x.capacity>=1)
  AND EXISTS (SELECT 1 FROM research_cause_exposure_specs c
   WHERE c.exposure_id=NEW.cause_exposure_id AND c.version=NEW.cause_exposure_version
    AND c.spec_hash=NEW.cause_spec_hash AND c.input_state='active'
    AND c.system_id=NEW.system_id AND c.route_id='rfq' AND c.venue='kalshi'
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
) BEGIN SELECT RAISE(ABORT,'typed bundle promotion lacks exact sealed untouched contract'); END;
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_intents_no_update BEFORE UPDATE ON research_route_bundle_promotion_intents BEGIN SELECT RAISE(ABORT,'immutable typed bundle promotion intent'); END;
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_intents_no_delete BEFORE DELETE ON research_route_bundle_promotion_intents BEGIN SELECT RAISE(ABORT,'immutable typed bundle promotion intent'); END;

CREATE TABLE IF NOT EXISTS research_route_bundle_promotion_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 intent_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('paper_accepted','quote_revalidated','live_dispatched','live_rejected','live_ambiguous')),
 rfq_id TEXT NOT NULL,
 quote_id TEXT NOT NULL,
 quote_status TEXT NOT NULL,
 price REAL NOT NULL CHECK(price>=0 AND price<1),
 quantity REAL NOT NULL CHECK(quantity>=0),
 exact_fee REAL NOT NULL CHECK(exact_fee>=0),
 reason TEXT NOT NULL,
 evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
 FOREIGN KEY(intent_id) REFERENCES research_route_bundle_promotion_intents(intent_id),
 UNIQUE(intent_id,event_type)
);
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_event_identity BEFORE INSERT ON research_route_bundle_promotion_events
WHEN NEW.event_type='paper_accepted' AND NOT EXISTS(SELECT 1 FROM research_route_bundle_promotion_intents p WHERE p.intent_id=NEW.intent_id
 AND p.rfq_id=NEW.rfq_id AND p.quote_id=NEW.quote_id)
BEGIN SELECT RAISE(ABORT,'typed bundle promotion event changed RFQ identity'); END;
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_quote_recheck_exact BEFORE INSERT ON research_route_bundle_promotion_events
WHEN NEW.event_type='quote_revalidated' AND NOT EXISTS(SELECT 1 FROM research_route_bundle_promotion_intents p
 WHERE p.intent_id=NEW.intent_id AND NEW.quote_status='open' AND NEW.quantity=1 AND NEW.price>0
  AND NEW.price+NEW.exact_fee<=p.max_all_in_unit+0.000000001)
BEGIN SELECT RAISE(ABORT,'fresh LIVE RFQ quote is not open, one-contract, or inside the sealed ceiling'); END;
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_live_requires_paper_and_recheck BEFORE INSERT ON research_route_bundle_promotion_events
WHEN NEW.event_type IN ('live_dispatched','live_ambiguous') AND NOT EXISTS(
 SELECT 1 FROM research_route_bundle_promotion_events p WHERE p.intent_id=NEW.intent_id AND p.event_type='paper_accepted')
 OR NEW.event_type IN ('live_dispatched','live_ambiguous') AND NOT EXISTS(
 SELECT 1 FROM research_route_bundle_promotion_events q WHERE q.intent_id=NEW.intent_id
  AND q.event_type='quote_revalidated' AND q.rfq_id=NEW.rfq_id AND q.quote_id=NEW.quote_id)
BEGIN SELECT RAISE(ABORT,'LIVE typed bundle lacks identical Paper acceptance or fresh same-quote recheck'); END;
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_live_requires_current_governance BEFORE INSERT ON research_route_bundle_promotion_events
WHEN NEW.event_type IN ('live_dispatched','live_ambiguous') AND NOT EXISTS (
 SELECT 1 FROM research_route_bundle_promotion_intents p JOIN research_cause_exposure_specs c
  ON c.exposure_id=p.cause_exposure_id AND c.version=p.cause_exposure_version AND c.spec_hash=p.cause_spec_hash
 JOIN research_cause_graph_runs g ON g.manifest_hash=p.cause_graph_manifest_hash
 WHERE p.intent_id=NEW.intent_id AND c.input_state='active'
  AND c.version=(SELECT MAX(v.version) FROM research_cause_exposure_specs v WHERE v.exposure_id=c.exposure_id)
  AND julianday(c.valid_until_ts)>julianday('now') AND c.replicated_untouched=1
  AND c.executable_fee_net_lower_bound=1 AND g.state='READY'
  AND g.input_count=g.valid_input_count AND g.valid_input_count>0
  AND EXISTS(SELECT 1 FROM json_each(g.input_hashes_json) j WHERE j.value='valid:'||p.cause_spec_hash))
BEGIN SELECT RAISE(ABORT,'LIVE typed bundle lacks current frozen cause and portfolio governance'); END;
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_events_no_update BEFORE UPDATE ON research_route_bundle_promotion_events BEGIN SELECT RAISE(ABORT,'append-only typed bundle promotion event'); END;
CREATE TRIGGER IF NOT EXISTS rr_bundle_promotion_events_no_delete BEFORE DELETE ON research_route_bundle_promotion_events BEGIN SELECT RAISE(ABORT,'append-only typed bundle promotion event'); END;
`

func migrateResearchBundlePromotionSchema(db *sql.DB) error {
	for _, col := range []string{
		"current_expected_net_lower REAL NOT NULL DEFAULT 0",
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
		_, _ = db.Exec(`ALTER TABLE research_route_bundle_promotion_intents ADD COLUMN ` + col)
	}
	_, _ = db.Exec(`DROP TRIGGER IF EXISTS rr_bundle_promotion_requires_sealed_exact_contract`)
	for _, trigger := range []string{"rr_bundle_promotion_event_identity", "rr_bundle_promotion_quote_recheck_exact",
		"rr_bundle_promotion_live_requires_paper_and_recheck", "rr_bundle_promotion_live_requires_current_governance"} {
		_, _ = db.Exec(`DROP TRIGGER IF EXISTS ` + trigger)
	}
	_, err := db.Exec(researchBundlePromotionSchema)
	return err
}

type ResearchRouteBundleQuote struct {
	CollectionTicker, MarketTicker, RFQID, QuoteID, Status, FeeSource string
	Observed                                                          time.Time
	Price, Quantity, ExactFee                                         float64
}

type ResearchRouteBundlePromotionIntent struct {
	IntentID, BundleID, CollectionTicker, MarketTicker, RFQID, QuoteID string
	Created, QuoteObserved                                             time.Time
	QuotePrice, Quantity, ExactFee, MaxAllInUnit                       float64
	FeeSource, OrderedLegsHash, QuoteHash                              string
	PaperParlayID, ProofRunID                                          int64
	CurrentExpectedNetLower                                            float64
	Governance                                                         ResearchPromotionGovernance
}

func (s *Store) ResearchRouteBundlePromotionIntent(ctx context.Context,
	id string) (ResearchRouteBundlePromotionIntent, bool, error) {
	var out ResearchRouteBundlePromotionIntent
	var created, quoteObserved string
	err := s.db.QueryRowContext(ctx, `SELECT intent_id,bundle_id,created_ts,collection_ticker,market_ticker,
rfq_id,quote_id,quote_observed_ts,quote_price,quantity,exact_fee,max_all_in_unit,fee_source,
ordered_legs_hash,quote_hash,paper_parlay_id,sealed_inference_run_id,current_expected_net_lower,
route_economics_receipt_id,cause_exposure_id,cause_exposure_version,cause_spec_hash,
cause_graph_manifest_hash,net_per_day_lower,capacity,capital_dollar_hours_per_day,
mode4_fraction_ceiling,mode4_input_hash
FROM research_route_bundle_promotion_intents WHERE intent_id=?`, id).Scan(&out.IntentID, &out.BundleID,
		&created, &out.CollectionTicker, &out.MarketTicker, &out.RFQID, &out.QuoteID, &quoteObserved,
		&out.QuotePrice, &out.Quantity, &out.ExactFee, &out.MaxAllInUnit, &out.FeeSource,
		&out.OrderedLegsHash, &out.QuoteHash, &out.PaperParlayID, &out.ProofRunID,
		&out.CurrentExpectedNetLower, &out.Governance.RouteEconomicsReceiptID,
		&out.Governance.CauseExposureID, &out.Governance.CauseExposureVersion,
		&out.Governance.CauseSpecHash, &out.Governance.CauseGraphManifestHash,
		&out.Governance.NetPerDayLower, &out.Governance.Capacity,
		&out.Governance.CapitalDollarHoursPerDay, &out.Governance.Mode4FractionCeiling,
		&out.Governance.Mode4InputHash)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	out.Created, err = time.Parse(time.RFC3339Nano, created)
	if err == nil {
		out.QuoteObserved, err = time.Parse(time.RFC3339Nano, quoteObserved)
	}
	return out, err == nil, err
}

func (s *Store) PendingResearchRouteBundleLiveIntents(ctx context.Context,
	limit int) ([]ResearchRouteBundlePromotionIntent, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.intent_id FROM research_route_bundle_promotion_intents p
WHERE EXISTS(SELECT 1 FROM research_route_bundle_promotion_events e WHERE e.intent_id=p.intent_id AND e.event_type='paper_accepted')
 AND EXISTS(SELECT 1 FROM paper_parlays x WHERE x.id=p.paper_parlay_id AND x.status='open')
 AND NOT EXISTS(SELECT 1 FROM research_route_bundle_promotion_events e WHERE e.intent_id=p.intent_id
  AND e.event_type IN ('live_dispatched','live_rejected','live_ambiguous'))
	 AND EXISTS(SELECT 1 FROM research_cause_exposure_specs c WHERE c.exposure_id=p.cause_exposure_id
	  AND c.version=p.cause_exposure_version AND c.spec_hash=p.cause_spec_hash AND c.input_state='active'
	  AND c.version=(SELECT MAX(v.version) FROM research_cause_exposure_specs v WHERE v.exposure_id=c.exposure_id)
	  AND julianday(c.valid_until_ts)>julianday(?) AND c.replicated_untouched=1
	  AND c.executable_fee_net_lower_bound=1)
	 AND EXISTS(SELECT 1 FROM research_cause_graph_runs g WHERE g.manifest_hash=p.cause_graph_manifest_hash
	  AND g.state='READY' AND g.input_count=g.valid_input_count AND g.valid_input_count>0
	  AND EXISTS(SELECT 1 FROM json_each(g.input_hashes_json) j WHERE j.value='valid:'||p.cause_spec_hash))
ORDER BY p.created_ts,p.intent_id LIMIT ?`, time.Now().UTC().Format(time.RFC3339Nano), limit)
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
	out := make([]ResearchRouteBundlePromotionIntent, 0, len(ids))
	for _, id := range ids {
		in, found, err := s.ResearchRouteBundlePromotionIntent(ctx, id)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, in)
		}
	}
	return out, nil
}

func researchBundleOrderedLegsHash(b ResearchRouteBundle) string {
	legs := make([]map[string]any, 0, len(b.Legs))
	for _, leg := range b.Legs {
		legs = append(legs, map[string]any{"index": leg.Index, "ticker": leg.Ticker,
			"side": leg.Side, "payoff_id": leg.PayoffID})
	}
	return strings.TrimPrefix(R138HashJSON(legs), "sha256:")
}

func researchBundlePromotionIntentID(h ResearchRouteBundleHandoff, q ResearchRouteBundleQuote) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%s|%s|%s|%s|%s|%s", h.Bundle.BundleID,
		h.ProofRunID, h.ProofResultHash, q.RFQID, q.QuoteID,
		h.Governance.RouteEconomicsReceiptID, h.Governance.CauseSpecHash,
		h.Governance.CauseGraphManifestHash)))
	return hex.EncodeToString(sum[:])
}

// InsertResearchRouteBundlePaperIntent atomically records the venue quote, books one identical
// one-contract Paper conjunction, and appends the accepted Paper receipt. No LIVE event can be
// inserted until a second read proves this same quote is still open and byte-economically equal.
func (s *Store) InsertResearchRouteBundlePaperIntent(ctx context.Context, h ResearchRouteBundleHandoff,
	q ResearchRouteBundleQuote, legs []paper.Leg) (ResearchRouteBundlePromotionIntent, bool, error) {
	var out ResearchRouteBundlePromotionIntent
	now := time.Now().UTC()
	b := h.Bundle
	allIn := q.Price + q.ExactFee
	currentExpectedLower := math.Min(h.ProofLowerPerUnit-math.Max(0, allIn-h.ProofMeanAllInUnit), 1-allIn)
	mode4Fraction := 0.0
	if allIn > 0 {
		mode4Fraction = math.Min(.25, .25*currentExpectedLower/allIn)
	}
	if !h.ComboEquivalent || !h.SealedUntouched || h.ProofRunID <= 0 || h.PreregistrationID == "" ||
		len(b.Legs) < 2 || len(b.Legs) > 6 || len(legs) != len(b.Legs) || q.CollectionTicker == "" ||
		q.MarketTicker == "" || q.RFQID == "" || q.QuoteID == "" || q.Status != "open" ||
		q.Observed.IsZero() || now.Sub(q.Observed) < 0 || now.Sub(q.Observed) > 3*time.Second ||
		math.Abs(q.Quantity-1) > 1e-9 || q.Price <= 0 || q.Price >= 1 || q.ExactFee < 0 ||
		q.FeeSource == "" || q.Price+q.ExactFee > h.MaxAllInUnit+1e-9 ||
		currentExpectedLower <= 0 || h.Governance.RouteEconomicsReceiptID == "" ||
		h.Governance.CauseExposureID == "" || !validSHA256(h.Governance.CauseSpecHash) ||
		!validSHA256(h.Governance.CauseGraphManifestHash) || h.Governance.NetPerDayLower <= 0 ||
		h.Governance.Capacity < 1 || !finiteResearchNumber(h.Governance.CapitalDollarHoursPerDay) ||
		h.Governance.CapitalDollarHoursPerDay < 0 || mode4Fraction <= 0 || mode4Fraction > .25 ||
		!researchBundleKalshiComboEquivalent(b.Size, b.Legs, b.States) {
		return out, false, errors.New("typed bundle quote does not satisfy sealed one-contract Paper handoff")
	}
	for i := range b.Legs {
		if b.Legs[i].Venue != "kalshi" || legs[i].Platform != "kalshi" ||
			legs[i].Ticker != b.Legs[i].Ticker || !strings.EqualFold(legs[i].Side, b.Legs[i].Side) {
			return out, false, errors.New("Paper combo ordered legs differ from typed certificate")
		}
	}
	legsJSON, err := json.Marshal(legs)
	if err != nil {
		return out, false, err
	}
	comboVenue, comboKey, legCount, comboIdentityOK := comboIdentityFromLegJSON(string(legsJSON))
	if !comboIdentityOK || comboVenue != "kalshi" || legCount != len(b.Legs) {
		return out, false, errors.New("typed bundle Paper intent lacks canonical combo attribution")
	}
	orderedHash := researchBundleOrderedLegsHash(b)
	quoteHash := strings.TrimPrefix(R138HashJSON(map[string]any{"collection": q.CollectionTicker,
		"market": q.MarketTicker, "rfq": q.RFQID, "quote": q.QuoteID, "status": q.Status,
		"price": q.Price, "quantity": q.Quantity, "fee": q.ExactFee, "fee_source": q.FeeSource,
		"ordered_legs_hash": orderedHash}), "sha256:")
	mode4Raw := fmt.Sprintf("%s|%s|%s|%d|%s|%.12f|%.12f|%.12f|%.12f|%.12f|%s",
		h.Governance.RouteEconomicsReceiptID, h.Governance.CauseExposureID,
		h.Governance.CauseSpecHash, h.Governance.CauseExposureVersion,
		h.Governance.CauseGraphManifestHash, h.Governance.NetPerDayLower,
		h.Governance.Capacity, h.Governance.CapitalDollarHoursPerDay,
		currentExpectedLower, mode4Fraction, b.StateVectorHash)
	mode4Sum := sha256.Sum256([]byte(mode4Raw))
	h.Governance.Mode4FractionCeiling = mode4Fraction
	h.Governance.Mode4InputHash = hex.EncodeToString(mode4Sum[:])
	out = ResearchRouteBundlePromotionIntent{IntentID: researchBundlePromotionIntentID(h, q),
		BundleID: b.BundleID, CollectionTicker: q.CollectionTicker, MarketTicker: q.MarketTicker,
		RFQID: q.RFQID, QuoteID: q.QuoteID, Created: now, QuoteObserved: q.Observed,
		QuotePrice: q.Price, Quantity: 1, ExactFee: q.ExactFee, MaxAllInUnit: h.MaxAllInUnit,
		FeeSource: q.FeeSource, OrderedLegsHash: orderedHash, QuoteHash: quoteHash, ProofRunID: h.ProofRunID,
		CurrentExpectedNetLower: currentExpectedLower, Governance: h.Governance}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return out, false, err
	}
	defer tx.Rollback()
	parlay, err := tx.ExecContext(ctx, `INSERT INTO paper_parlays(ts,stake,price,contracts,tp,sl,status,legs,fees,
canonical_system_id,combo_venue,leg_count,relation_class,producer_family,combo_route,experiment_epoch,combo_key)
VALUES(?,?,?,?,0,0,'open',?,?,?,?,?,?,?,?,?,?)`, now.Format(time.RFC3339Nano), q.Price, q.Price, 1,
		string(legsJSON), q.ExactFee, b.SystemID, comboVenue, legCount, "related", "rfq", "kalshi-rfq",
		fmt.Sprintf("%s:v%d", b.Cohort, b.ExperimentVersion), comboKey)
	if err != nil {
		return out, false, err
	}
	out.PaperParlayID, err = parlay.LastInsertId()
	if err != nil {
		return out, false, err
	}
	resultHash := strings.TrimPrefix(h.ProofResultHash, "sha256:")
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_route_bundle_promotion_intents(
intent_id,created_ts,bundle_id,sealed_inference_run_id,sealed_result_hash,preregistration_id,system_id,
experiment_version,cohort,certificate_hash,state_vector_hash,collection_ticker,market_ticker,rfq_id,
quote_id,quote_observed_ts,quote_status,quote_price,quantity,exact_fee,fee_source,all_in_unit,
max_all_in_unit,current_expected_net_lower,route_economics_receipt_id,cause_exposure_id,cause_exposure_version,
cause_spec_hash,cause_graph_manifest_hash,net_per_day_lower,capacity,capital_dollar_hours_per_day,
mode4_fraction_ceiling,mode4_input_hash,ordered_legs_hash,quote_hash,paper_parlay_id)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, out.IntentID, now.Format(time.RFC3339Nano),
		b.BundleID, h.ProofRunID, resultHash, h.PreregistrationID, b.SystemID, b.ExperimentVersion,
		b.Cohort, b.CertificateHash, b.StateVectorHash, q.CollectionTicker, q.MarketTicker, q.RFQID,
		q.QuoteID, q.Observed.Format(time.RFC3339Nano), q.Status, q.Price, 1, q.ExactFee, q.FeeSource,
		allIn, h.MaxAllInUnit, currentExpectedLower, h.Governance.RouteEconomicsReceiptID,
		h.Governance.CauseExposureID, h.Governance.CauseExposureVersion, h.Governance.CauseSpecHash,
		h.Governance.CauseGraphManifestHash, h.Governance.NetPerDayLower, h.Governance.Capacity,
		h.Governance.CapitalDollarHoursPerDay, mode4Fraction, h.Governance.Mode4InputHash,
		orderedHash, quoteHash, out.PaperParlayID)
	if err != nil {
		return out, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return out, false, err
	}
	evidence, _ := json.Marshal(map[string]any{"paper_parlay_id": out.PaperParlayID,
		"same_security": "exact certified Kalshi MVE conjunction", "actual_live_fill": false})
	if _, err = tx.ExecContext(ctx, `INSERT INTO research_route_bundle_promotion_events(intent_id,
observed_ts,event_type,rfq_id,quote_id,quote_status,price,quantity,exact_fee,reason,evidence_json)
VALUES(?,?,'paper_accepted',?,?,?,?,?,?,?,?)`, out.IntentID, now.Format(time.RFC3339Nano), q.RFQID,
		q.QuoteID, q.Status, q.Price, 1, q.ExactFee,
		"identical one-contract Paper conjunction booked from authenticated maker quote", string(evidence)); err != nil {
		return out, false, err
	}
	if err = tx.Commit(); err != nil {
		return out, false, err
	}
	return out, true, nil
}

func (s *Store) AppendResearchRouteBundlePromotionEvent(ctx context.Context,
	in ResearchRouteBundlePromotionIntent, eventType, status string, price, quantity, fee float64,
	reason string, evidence any) error {
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	if len(encoded) == 0 || string(encoded) == "null" {
		encoded = []byte("{}")
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO research_route_bundle_promotion_events(intent_id,
observed_ts,event_type,rfq_id,quote_id,quote_status,price,quantity,exact_fee,reason,evidence_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?)`, in.IntentID, time.Now().UTC().Format(time.RFC3339Nano), eventType,
		in.RFQID, in.QuoteID, status, math.Max(0, price), math.Max(0, quantity), math.Max(0, fee),
		firstNonEmptyStorage(reason, eventType), string(encoded))
	return err
}
