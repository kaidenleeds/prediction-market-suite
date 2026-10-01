package storage

// Funded relation receipts are the immutable decision-time truth for Paper-capital candidates.
// They deliberately live outside unit_trials: a repeated signal observation is evidence, while a
// funded decision is the one moment where portfolio correlation and execution identity matter.

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
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

const fundedRelationDDL = `
CREATE TABLE IF NOT EXISTS funded_relation_receipts (
 receipt_id TEXT PRIMARY KEY CHECK(length(receipt_id)=64),
 decision_fingerprint TEXT NOT NULL UNIQUE CHECK(length(decision_fingerprint)=64),
 decision_ts TEXT NOT NULL,
 portfolio TEXT NOT NULL CHECK(portfolio IN ('kalshi','polyus','combos','ml-kalshi','ml-polyus','ml-combos')),
 position_fingerprint TEXT NOT NULL CHECK(length(position_fingerprint)=64),
 candidate_venue TEXT NOT NULL DEFAULT '',
 candidate_ticker TEXT NOT NULL DEFAULT '',
 candidate_side TEXT NOT NULL DEFAULT '',
 system_id TEXT NOT NULL,
 route_kind TEXT NOT NULL,
 canonical_event_id TEXT NOT NULL DEFAULT '',
 event_version INTEGER NOT NULL DEFAULT 0 CHECK(event_version>=0),
 relation_state TEXT NOT NULL CHECK(relation_state IN ('independent','dependent','unknown')),
 matched_position_ids_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(matched_position_ids_json)),
 matched_event_ids_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(matched_event_ids_json)),
 allowed INTEGER NOT NULL CHECK(allowed IN (0,1)),
 reason TEXT NOT NULL,
 quantity REAL NOT NULL DEFAULT 0 CHECK(quantity>=0),
 entry_price REAL NOT NULL DEFAULT 0 CHECK(entry_price>=0 AND entry_price<=1),
 entry_fee REAL NOT NULL DEFAULT 0,
 created_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS funded_relation_receipt_legs (
 receipt_id TEXT NOT NULL,
 leg_no INTEGER NOT NULL CHECK(leg_no>=0),
 venue TEXT NOT NULL,
 ticker TEXT NOT NULL,
 side TEXT NOT NULL,
 canonical_event_id TEXT NOT NULL DEFAULT '',
 event_version INTEGER NOT NULL DEFAULT 0 CHECK(event_version>=0),
 relation_state TEXT NOT NULL CHECK(relation_state IN ('independent','dependent','unknown')),
 matched_position_ids_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(matched_position_ids_json)),
 PRIMARY KEY(receipt_id,leg_no),
 FOREIGN KEY(receipt_id) REFERENCES funded_relation_receipts(receipt_id)
);
CREATE TABLE IF NOT EXISTS funded_relation_outcomes (
 outcome_id INTEGER PRIMARY KEY AUTOINCREMENT,
 receipt_id TEXT NOT NULL UNIQUE,
 outcome_status TEXT NOT NULL CHECK(outcome_status IN ('settled','cancelled','placement_failed')),
 settled_ts TEXT NOT NULL,
 pnl_dollars REAL NOT NULL,
 result_source TEXT NOT NULL,
 created_ts TEXT NOT NULL,
 FOREIGN KEY(receipt_id) REFERENCES funded_relation_receipts(receipt_id)
);
-- A specialist JSON lot can settle after the shared contract-level Paper book has already
-- published an outcome for the same economic contract. The original outcome remains immutable;
-- this append-only row preserves both the first writer and the exact specialist-position repair.
CREATE TABLE IF NOT EXISTS funded_relation_outcome_corrections (
 correction_id INTEGER PRIMARY KEY AUTOINCREMENT,
 receipt_id TEXT NOT NULL UNIQUE,
 original_outcome_id INTEGER NOT NULL,
 original_pnl_dollars REAL NOT NULL,
 original_result_source TEXT NOT NULL,
 corrected_pnl_dollars REAL NOT NULL,
 corrected_result_source TEXT NOT NULL,
 reason TEXT NOT NULL,
 corrected_ts TEXT NOT NULL,
 created_ts TEXT NOT NULL,
 FOREIGN KEY(receipt_id) REFERENCES funded_relation_receipts(receipt_id)
);
CREATE TABLE IF NOT EXISTS funded_relation_position_links (
 receipt_id TEXT NOT NULL,
 position_kind TEXT NOT NULL,
 position_id INTEGER NOT NULL CHECK(position_id>0),
 created_ts TEXT NOT NULL,
 PRIMARY KEY(position_kind,position_id),
 UNIQUE(receipt_id),
 FOREIGN KEY(receipt_id) REFERENCES funded_relation_receipts(receipt_id)
);
-- A candidate can reach the shared Paper gate without a funded binary execution identity (for
-- example a Poly-int research outcome named "Down" or a team name).  Such a rejection must remain
-- durable, but it must not be forced into a Kalshi/PolyUS portfolio or an independent/dependent
-- classification.  Keep the raw identity in a separate immutable lane that never joins funded P&L.
CREATE TABLE IF NOT EXISTS funded_relation_identity_rejections (
 rejection_id TEXT PRIMARY KEY CHECK(length(rejection_id)=64),
 decision_fingerprint TEXT NOT NULL UNIQUE CHECK(length(decision_fingerprint)=64),
 decision_ts TEXT NOT NULL,
 candidate_venue TEXT NOT NULL DEFAULT '',
 candidate_ticker TEXT NOT NULL DEFAULT '',
 candidate_side TEXT NOT NULL DEFAULT '',
 system_id TEXT NOT NULL,
 route_kind TEXT NOT NULL,
 reason TEXT NOT NULL,
 identity_issue TEXT NOT NULL,
 raw_legs_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(raw_legs_json)),
 created_ts TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_frel_open ON funded_relation_receipts(allowed,portfolio,canonical_event_id,event_version,decision_ts);
CREATE INDEX IF NOT EXISTS idx_frel_contract ON funded_relation_receipts(portfolio,candidate_venue,candidate_ticker,candidate_side,decision_ts);
CREATE INDEX IF NOT EXISTS idx_frel_leg_event ON funded_relation_receipt_legs(canonical_event_id,event_version,venue,ticker);
CREATE INDEX IF NOT EXISTS idx_frel_outcome_time ON funded_relation_outcomes(settled_ts,receipt_id);
CREATE INDEX IF NOT EXISTS idx_frel_outcome_correction_time ON funded_relation_outcome_corrections(corrected_ts,receipt_id);
CREATE INDEX IF NOT EXISTS idx_frel_identity_rejection_time ON funded_relation_identity_rejections(decision_ts,system_id);
CREATE TRIGGER IF NOT EXISTS funded_relation_receipts_no_update BEFORE UPDATE ON funded_relation_receipts
BEGIN SELECT RAISE(ABORT,'immutable funded relation receipt'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_receipts_no_delete BEFORE DELETE ON funded_relation_receipts
BEGIN SELECT RAISE(ABORT,'immutable funded relation receipt'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_legs_no_update BEFORE UPDATE ON funded_relation_receipt_legs
BEGIN SELECT RAISE(ABORT,'immutable funded relation leg'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_legs_no_delete BEFORE DELETE ON funded_relation_receipt_legs
BEGIN SELECT RAISE(ABORT,'immutable funded relation leg'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_outcomes_no_update BEFORE UPDATE ON funded_relation_outcomes
BEGIN SELECT RAISE(ABORT,'immutable funded relation outcome'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_outcomes_no_delete BEFORE DELETE ON funded_relation_outcomes
BEGIN SELECT RAISE(ABORT,'immutable funded relation outcome'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_outcome_corrections_no_update BEFORE UPDATE ON funded_relation_outcome_corrections
BEGIN SELECT RAISE(ABORT,'immutable funded relation outcome correction'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_outcome_corrections_no_delete BEFORE DELETE ON funded_relation_outcome_corrections
BEGIN SELECT RAISE(ABORT,'immutable funded relation outcome correction'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_links_no_update BEFORE UPDATE ON funded_relation_position_links
BEGIN SELECT RAISE(ABORT,'immutable funded relation position link'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_links_no_delete BEFORE DELETE ON funded_relation_position_links
BEGIN SELECT RAISE(ABORT,'immutable funded relation position link'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_identity_rejections_no_update BEFORE UPDATE ON funded_relation_identity_rejections
BEGIN SELECT RAISE(ABORT,'immutable funded relation identity rejection'); END;
CREATE TRIGGER IF NOT EXISTS funded_relation_identity_rejections_no_delete BEFORE DELETE ON funded_relation_identity_rejections
BEGIN SELECT RAISE(ABORT,'immutable funded relation identity rejection'); END;`

func migrateFundedRelationSchema(db *sql.DB) error {
	if _, err := db.Exec(fundedRelationDDL); err != nil {
		return err
	}
	// R146 adds an independently compounded New-ML Combo portfolio. SQLite cannot ALTER a CHECK
	// constraint, so upgrade existing ledgers in place before the first new receipt is written.
	var ddl string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='funded_relation_receipts'`).Scan(&ddl); err != nil {
		return err
	}
	if strings.Contains(ddl, "'ml-combos'") {
		return nil
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), `PRAGMA foreign_keys=OFF`); err != nil {
		return err
	}
	defer conn.ExecContext(context.Background(), `PRAGMA foreign_keys=ON`)
	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE funded_relation_receipts_r146 (
 receipt_id TEXT PRIMARY KEY CHECK(length(receipt_id)=64),
 decision_fingerprint TEXT NOT NULL UNIQUE CHECK(length(decision_fingerprint)=64),
 decision_ts TEXT NOT NULL,
 portfolio TEXT NOT NULL CHECK(portfolio IN ('kalshi','polyus','combos','ml-kalshi','ml-polyus','ml-combos')),
 position_fingerprint TEXT NOT NULL CHECK(length(position_fingerprint)=64),
 candidate_venue TEXT NOT NULL DEFAULT '', candidate_ticker TEXT NOT NULL DEFAULT '', candidate_side TEXT NOT NULL DEFAULT '',
 system_id TEXT NOT NULL, route_kind TEXT NOT NULL, canonical_event_id TEXT NOT NULL DEFAULT '',
 event_version INTEGER NOT NULL DEFAULT 0 CHECK(event_version>=0),
 relation_state TEXT NOT NULL CHECK(relation_state IN ('independent','dependent','unknown')),
 matched_position_ids_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(matched_position_ids_json)),
 matched_event_ids_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(matched_event_ids_json)),
 allowed INTEGER NOT NULL CHECK(allowed IN (0,1)), reason TEXT NOT NULL,
 quantity REAL NOT NULL DEFAULT 0 CHECK(quantity>=0),
 entry_price REAL NOT NULL DEFAULT 0 CHECK(entry_price>=0 AND entry_price<=1),
 entry_fee REAL NOT NULL DEFAULT 0, created_ts TEXT NOT NULL)`); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO funded_relation_receipts_r146 SELECT * FROM funded_relation_receipts`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DROP TABLE funded_relation_receipts`); err != nil {
		return err
	}
	if _, err := tx.Exec(`ALTER TABLE funded_relation_receipts_r146 RENAME TO funded_relation_receipts`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	_, err = conn.ExecContext(context.Background(), fundedRelationDDL)
	return err
}

type FundedRelationLeg struct {
	Venue, Ticker, Side, CanonicalEventID, RelationState string
	EventVersion                                         int
	MatchedPositionIDs                                   []string
}

type FundedRelationReceipt struct {
	ReceiptID, DecisionFingerprint, PositionFingerprint string
	Decision                                            time.Time
	Portfolio, CandidateVenue, CandidateTicker          string
	CandidateSide, SystemID, RouteKind                  string
	CanonicalEventID, RelationState, Reason             string
	EventVersion                                        int
	MatchedPositionIDs, MatchedEventIDs                 []string
	Allowed                                             bool
	Quantity, EntryPrice, EntryFee                      float64
	Legs                                                []FundedRelationLeg
}

// FundedRelationIdentityRejection preserves a rejected candidate whose raw execution identity
// cannot be represented as a funded Kalshi/PolyUS YES/NO leg.  It intentionally has no portfolio
// or relation-state field: storing the rejection must never fabricate capital ownership or
// statistical independence.
type FundedRelationIdentityRejection struct {
	Decision                                 time.Time
	Venue, Ticker, Side, SystemID, RouteKind string
	Reason, IdentityIssue                    string
	RawLegs                                  []FundedRelationLeg
}

func (s *Store) InsertFundedRelationIdentityRejection(ctx context.Context,
	in FundedRelationIdentityRejection) (string, bool, error) {
	in.Venue, in.Ticker, in.Side = strings.ToLower(strings.TrimSpace(in.Venue)), strings.TrimSpace(in.Ticker), strings.TrimSpace(in.Side)
	in.SystemID, in.RouteKind = strings.TrimSpace(in.SystemID), strings.TrimSpace(in.RouteKind)
	in.Reason, in.IdentityIssue = strings.TrimSpace(in.Reason), strings.TrimSpace(in.IdentityIssue)
	if in.Decision.IsZero() {
		in.Decision = time.Now()
	}
	if in.SystemID == "" || in.RouteKind == "" || in.Reason == "" || in.IdentityIssue == "" {
		return "", false, errors.New("invalid funded relation identity rejection")
	}
	raw, err := json.Marshal(in.RawLegs)
	if err != nil {
		return "", false, fmt.Errorf("marshal funded relation identity rejection: %w", err)
	}
	// Rejected hot-loop candidates dedupe per minute while retaining every distinct raw contract,
	// side label, system, gate reason, and identity failure.
	decisionMinute := in.Decision.UTC().Truncate(time.Minute).Format(time.RFC3339)
	fingerprintHash := sha256.Sum256([]byte(strings.Join([]string{"funded-relation-identity-rejection-v1",
		decisionMinute, in.Venue, in.Ticker, in.Side, in.SystemID, in.RouteKind, in.Reason,
		in.IdentityIssue, string(raw)}, "\x00")))
	fingerprint := hex.EncodeToString(fingerprintHash[:])
	idHash := sha256.Sum256([]byte("funded-relation-identity-rejection-id-v1|" + fingerprint))
	id := hex.EncodeToString(idHash[:])
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO funded_relation_identity_rejections(
rejection_id,decision_fingerprint,decision_ts,candidate_venue,candidate_ticker,candidate_side,
system_id,route_kind,reason,identity_issue,raw_legs_json,created_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, fingerprint, in.Decision.UTC().Format(time.RFC3339Nano), in.Venue, in.Ticker, in.Side,
		in.SystemID, in.RouteKind, in.Reason, in.IdentityIssue, string(raw),
		time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return "", false, err
	}
	n, err := res.RowsAffected()
	return id, n > 0, err
}

func validFundedRelationState(v string) bool {
	return v == "independent" || v == "dependent" || v == "unknown"
}

func validFundedPortfolio(v string) bool {
	switch v {
	case "kalshi", "polyus", "combos", "ml-kalshi", "ml-polyus", "ml-combos":
		return true
	}
	return false
}

func validFundedVenue(v string) bool {
	return v == "kalshi" || v == "polyus"
}

func fundedPortfolioAcceptsVenue(portfolio, venue string) bool {
	switch portfolio {
	case "kalshi", "ml-kalshi":
		return venue == "kalshi"
	case "polyus", "ml-polyus":
		return venue == "polyus"
	case "combos", "ml-combos":
		return validFundedVenue(venue)
	default:
		return false
	}
}

func fundedRelationJSON(v []string) string {
	if v == nil {
		v = []string{}
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func normalizeRelationReceipt(r *FundedRelationReceipt) error {
	r.Portfolio = strings.ToLower(strings.TrimSpace(r.Portfolio))
	r.CandidateVenue = strings.ToLower(strings.TrimSpace(r.CandidateVenue))
	r.CandidateTicker = strings.TrimSpace(r.CandidateTicker)
	r.CandidateSide = strings.ToUpper(strings.TrimSpace(r.CandidateSide))
	r.SystemID, r.RouteKind = strings.TrimSpace(r.SystemID), strings.TrimSpace(r.RouteKind)
	r.CanonicalEventID = strings.TrimSpace(r.CanonicalEventID)
	r.RelationState, r.Reason = strings.ToLower(strings.TrimSpace(r.RelationState)), strings.TrimSpace(r.Reason)
	if r.Decision.IsZero() {
		r.Decision = time.Now()
	}
	if !validFundedPortfolio(r.Portfolio) || !validFundedVenue(r.CandidateVenue) ||
		!fundedPortfolioAcceptsVenue(r.Portfolio, r.CandidateVenue) || r.CandidateTicker == "" ||
		(r.CandidateSide != "YES" && r.CandidateSide != "NO") ||
		r.SystemID == "" || r.RouteKind == "" || r.Reason == "" ||
		!validFundedRelationState(r.RelationState) || r.EventVersion < 0 || r.Quantity < 0 ||
		r.EntryPrice < 0 || r.EntryPrice > 1 {
		return errors.New("invalid funded relation receipt")
	}
	for _, v := range []float64{r.Quantity, r.EntryPrice, r.EntryFee} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return errors.New("non-finite funded relation receipt")
		}
	}
	if (r.CanonicalEventID == "") != (r.EventVersion == 0) {
		return errors.New("partial canonical funded relation identity")
	}
	if r.RelationState != "unknown" && r.CanonicalEventID == "" && len(r.Legs) <= 1 {
		return errors.New("known relation state lacks canonical event identity")
	}
	if len(r.Legs) == 0 {
		r.Legs = []FundedRelationLeg{{Venue: r.CandidateVenue, Ticker: r.CandidateTicker, Side: r.CandidateSide,
			CanonicalEventID: r.CanonicalEventID, EventVersion: r.EventVersion, RelationState: r.RelationState,
			MatchedPositionIDs: append([]string(nil), r.MatchedPositionIDs...)}}
	}
	for i := range r.Legs {
		l := &r.Legs[i]
		l.Venue, l.Ticker, l.Side = strings.ToLower(strings.TrimSpace(l.Venue)), strings.TrimSpace(l.Ticker), strings.ToUpper(strings.TrimSpace(l.Side))
		l.CanonicalEventID, l.RelationState = strings.TrimSpace(l.CanonicalEventID), strings.ToLower(strings.TrimSpace(l.RelationState))
		if !validFundedVenue(l.Venue) || !fundedPortfolioAcceptsVenue(r.Portfolio, l.Venue) ||
			l.Ticker == "" || (l.Side != "YES" && l.Side != "NO") ||
			!validFundedRelationState(l.RelationState) || l.EventVersion < 0 ||
			((l.CanonicalEventID == "") != (l.EventVersion == 0)) {
			return fmt.Errorf("invalid funded relation leg %d", i)
		}
		if l.RelationState != "unknown" && l.CanonicalEventID == "" {
			return fmt.Errorf("known funded relation leg %d lacks event identity", i)
		}
	}
	if len(r.DecisionFingerprint) != 64 || len(r.PositionFingerprint) != 64 {
		return errors.New("invalid funded relation fingerprint")
	}
	if r.ReceiptID == "" {
		h := sha256.Sum256([]byte("funded-relation-v1|" + r.DecisionFingerprint))
		r.ReceiptID = hex.EncodeToString(h[:])
	}
	if len(r.ReceiptID) != 64 {
		return errors.New("invalid funded relation receipt id")
	}
	return nil
}

// InsertFundedRelationReceipt commits classification before an accepted order can be persisted.
// A repeated identical decision is idempotent; a fingerprint collision with different immutable
// content is an error rather than an overwrite.
func (s *Store) InsertFundedRelationReceipt(ctx context.Context, in FundedRelationReceipt) (string, bool, error) {
	if err := normalizeRelationReceipt(&in); err != nil {
		return "", false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	allowed := 0
	if in.Allowed {
		allowed = 1
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO funded_relation_receipts(
receipt_id,decision_fingerprint,decision_ts,portfolio,position_fingerprint,candidate_venue,candidate_ticker,
candidate_side,system_id,route_kind,canonical_event_id,event_version,relation_state,matched_position_ids_json,
matched_event_ids_json,allowed,reason,quantity,entry_price,entry_fee,created_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, in.ReceiptID, in.DecisionFingerprint,
		in.Decision.UTC().Format(time.RFC3339Nano), in.Portfolio, in.PositionFingerprint, in.CandidateVenue,
		in.CandidateTicker, in.CandidateSide, in.SystemID, in.RouteKind, in.CanonicalEventID, in.EventVersion,
		in.RelationState, fundedRelationJSON(in.MatchedPositionIDs), fundedRelationJSON(in.MatchedEventIDs),
		allowed, in.Reason, in.Quantity, in.EntryPrice, in.EntryFee, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return "", false, err
	}
	inserted, _ := res.RowsAffected()
	if inserted == 0 {
		var id, portfolio, system, state string
		var wasAllowed int
		if err := tx.QueryRowContext(ctx, `SELECT receipt_id,portfolio,system_id,relation_state,allowed
FROM funded_relation_receipts WHERE decision_fingerprint=?`, in.DecisionFingerprint).
			Scan(&id, &portfolio, &system, &state, &wasAllowed); err != nil {
			return "", false, err
		}
		if id != in.ReceiptID || portfolio != in.Portfolio || system != in.SystemID || state != in.RelationState || wasAllowed != allowed {
			return "", false, errors.New("funded relation decision fingerprint collision")
		}
		return id, false, tx.Commit()
	}
	for i, leg := range in.Legs {
		if _, err := tx.ExecContext(ctx, `INSERT INTO funded_relation_receipt_legs(
receipt_id,leg_no,venue,ticker,side,canonical_event_id,event_version,relation_state,matched_position_ids_json)
VALUES(?,?,?,?,?,?,?,?,?)`, in.ReceiptID, i, leg.Venue, leg.Ticker, leg.Side, leg.CanonicalEventID,
			leg.EventVersion, leg.RelationState, fundedRelationJSON(leg.MatchedPositionIDs)); err != nil {
			return "", false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, err
	}
	return in.ReceiptID, true, nil
}

type OpenFundedRelationLeg struct {
	ReceiptID, PositionFingerprint, Portfolio, Venue, Ticker, Side, CanonicalEventID string
	EventVersion                                                                     int
}

func (s *Store) OpenFundedRelationLegs(ctx context.Context) ([]OpenFundedRelationLeg, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.receipt_id,r.position_fingerprint,r.portfolio,
l.venue,l.ticker,l.side,l.canonical_event_id,l.event_version
FROM funded_relation_receipts r JOIN funded_relation_receipt_legs l ON l.receipt_id=r.receipt_id
LEFT JOIN funded_relation_outcomes o ON o.receipt_id=r.receipt_id
WHERE r.allowed=1 AND o.receipt_id IS NULL ORDER BY r.decision_ts,r.receipt_id,l.leg_no`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OpenFundedRelationLeg
	for rows.Next() {
		var v OpenFundedRelationLeg
		if err := rows.Scan(&v.ReceiptID, &v.PositionFingerprint, &v.Portfolio, &v.Venue, &v.Ticker,
			&v.Side, &v.CanonicalEventID, &v.EventVersion); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) recordFundedRelationTerminal(ctx context.Context, receiptID, status string, settled time.Time, pnl float64, source string) (bool, error) {
	receiptID, source = strings.TrimSpace(receiptID), strings.TrimSpace(source)
	status = strings.ToLower(strings.TrimSpace(status))
	if len(receiptID) != 64 || (status != "settled" && status != "cancelled" && status != "placement_failed") ||
		source == "" || math.IsNaN(pnl) || math.IsInf(pnl, 0) {
		return false, errors.New("invalid funded relation outcome")
	}
	if settled.IsZero() {
		settled = time.Now()
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO funded_relation_outcomes(
receipt_id,outcome_status,settled_ts,pnl_dollars,result_source,created_ts) SELECT ?,?,?,?,?,?
WHERE EXISTS(SELECT 1 FROM funded_relation_receipts WHERE receipt_id=? AND allowed=1)`,
		receiptID, status, settled.UTC().Format(time.RFC3339Nano), pnl, source,
		time.Now().UTC().Format(time.RFC3339Nano), receiptID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) RecordFundedRelationOutcome(ctx context.Context, receiptID string, settled time.Time, pnl float64, source string) (bool, error) {
	return s.recordFundedRelationTerminal(ctx, receiptID, "settled", settled, pnl, source)
}

const fundedSpecialistOutcomeCorrectionReason = "specialist-position-outcome-supersedes-contract-allocation-v1"

// ReconcileFundedSpecialistRelationOutcome records the exact terminal P&L owned by one persisted
// specialist lot. The immutable receipt and the lot's frozen execution terms must agree. If the
// generic shared-Paper contract sweep won the race, its original outcome stays untouched and an
// append-only correction supersedes it in economic reports. A conflicting specialist writer is an
// error, never a silent rewrite.
func (s *Store) ReconcileFundedSpecialistRelationOutcome(ctx context.Context, receiptID, venue, ticker, side string,
	quantity, entryPrice, entryFee float64, settled time.Time, pnl float64, source string) (inserted, corrected bool, err error) {
	receiptID, venue, ticker = strings.TrimSpace(receiptID), strings.ToLower(strings.TrimSpace(venue)), strings.TrimSpace(ticker)
	side, source = strings.ToUpper(strings.TrimSpace(side)), strings.TrimSpace(source)
	if len(receiptID) != 64 || !validFundedVenue(venue) || ticker == "" ||
		(side != "YES" && side != "NO") || source == "" || strings.HasPrefix(strings.ToLower(source), "settled-") {
		return false, false, errors.New("invalid funded specialist outcome")
	}
	for _, v := range []float64{quantity, entryPrice, entryFee, pnl} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false, false, errors.New("non-finite funded specialist outcome")
		}
	}
	if quantity < 0 || entryPrice < 0 || entryPrice > 1 {
		return false, false, errors.New("invalid funded specialist execution terms")
	}
	if settled.IsZero() {
		settled = time.Now()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback()
	var gotVenue, gotTicker, gotSide string
	var gotQuantity, gotPrice, gotFee float64
	var allowed int
	if err := tx.QueryRowContext(ctx, `SELECT candidate_venue,candidate_ticker,candidate_side,
quantity,entry_price,entry_fee,allowed FROM funded_relation_receipts WHERE receipt_id=?`, receiptID).
		Scan(&gotVenue, &gotTicker, &gotSide, &gotQuantity, &gotPrice, &gotFee, &allowed); err != nil {
		return false, false, err
	}
	const executionTolerance = 1e-9
	if allowed != 1 || gotVenue != venue || gotTicker != ticker || strings.ToUpper(gotSide) != side ||
		math.Abs(gotQuantity-quantity) > executionTolerance ||
		math.Abs(gotPrice-entryPrice) > executionTolerance ||
		math.Abs(gotFee-entryFee) > executionTolerance {
		return false, false, errors.New("funded specialist outcome does not match immutable receipt")
	}
	var outcomeID int64
	var status, priorSource string
	var priorPnL float64
	readErr := tx.QueryRowContext(ctx, `SELECT outcome_id,outcome_status,pnl_dollars,result_source
FROM funded_relation_outcomes WHERE receipt_id=?`, receiptID).Scan(&outcomeID, &status, &priorPnL, &priorSource)
	if errors.Is(readErr, sql.ErrNoRows) {
		res, insertErr := tx.ExecContext(ctx, `INSERT INTO funded_relation_outcomes(
receipt_id,outcome_status,settled_ts,pnl_dollars,result_source,created_ts) VALUES(?,?,?,?,?,?)`,
			receiptID, "settled", settled.UTC().Format(time.RFC3339Nano), pnl, source,
			time.Now().UTC().Format(time.RFC3339Nano))
		if insertErr != nil {
			return false, false, insertErr
		}
		n, insertErr := res.RowsAffected()
		if insertErr != nil {
			return false, false, insertErr
		}
		return n > 0, false, tx.Commit()
	}
	if readErr != nil {
		return false, false, readErr
	}
	if status != "settled" {
		return false, false, fmt.Errorf("funded specialist receipt already terminal as %s", status)
	}
	var correctedPnL float64
	var correctedSource string
	correctionErr := tx.QueryRowContext(ctx, `SELECT corrected_pnl_dollars,corrected_result_source
FROM funded_relation_outcome_corrections WHERE receipt_id=?`, receiptID).Scan(&correctedPnL, &correctedSource)
	if correctionErr == nil {
		if math.Abs(correctedPnL-pnl) > executionTolerance || correctedSource != source {
			return false, false, errors.New("conflicting funded specialist outcome correction")
		}
		return false, false, tx.Commit()
	}
	if !errors.Is(correctionErr, sql.ErrNoRows) {
		return false, false, correctionErr
	}
	// Specialist ledgers persist closed P&L to cents. Ordinary rounding can differ from the exact
	// first-write result by exactly half a cent (for example -0.195 stored exactly and -0.20 in the
	// retained JSON ledger). Treat that boundary as the same economic result, but never let a
	// materially different specialist writer replace another one.
	priorWasGenericSweep := strings.HasPrefix(strings.ToLower(priorSource), "settled-")
	if math.Abs(priorPnL-pnl) <= 0.005+executionTolerance &&
		(priorSource == source || priorWasGenericSweep) {
		return false, false, tx.Commit()
	}
	if priorSource == source || !priorWasGenericSweep {
		return false, false, fmt.Errorf("conflicting funded specialist outcome: prior source %q", priorSource)
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO funded_relation_outcome_corrections(
receipt_id,original_outcome_id,original_pnl_dollars,original_result_source,
corrected_pnl_dollars,corrected_result_source,reason,corrected_ts,created_ts)
VALUES(?,?,?,?,?,?,?,?,?)`, receiptID, outcomeID, priorPnL, priorSource, pnl, source,
		fundedSpecialistOutcomeCorrectionReason, settled.UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, false, err
	}
	if err := tx.Commit(); err != nil {
		return false, false, err
	}
	return false, n > 0, nil
}

func (s *Store) RecordFundedRelationCancellation(ctx context.Context, receiptID, reason string) (bool, error) {
	return s.recordFundedRelationTerminal(ctx, receiptID, "cancelled", time.Now(), 0, reason)
}

func (s *Store) RecordFundedRelationPlacementFailure(ctx context.Context, receiptID, reason string) (bool, error) {
	return s.recordFundedRelationTerminal(ctx, receiptID, "placement_failed", time.Now(), 0, reason)
}

func (s *Store) LinkFundedRelationPosition(ctx context.Context, receiptID, kind string, id int64) error {
	receiptID, kind = strings.TrimSpace(receiptID), strings.ToLower(strings.TrimSpace(kind))
	if len(receiptID) != 64 || kind == "" || id <= 0 {
		return errors.New("invalid funded relation position link")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO funded_relation_position_links(
receipt_id,position_kind,position_id,created_ts) SELECT ?,?,?,? WHERE EXISTS(
 SELECT 1 FROM funded_relation_receipts WHERE receipt_id=? AND allowed=1)`, receiptID, kind, id,
		time.Now().UTC().Format(time.RFC3339Nano), receiptID)
	return err
}

// InsertParlayWithRelation makes the accepted decision receipt and funded combo inseparable at the
// durable placement boundary. The receipt already exists; this transaction inserts the position
// and its append-only link together or neither.
func (s *Store) InsertParlayWithRelation(ctx context.Context, p paper.Parlay, receiptID string) (int64, error) {
	if len(strings.TrimSpace(receiptID)) != 64 {
		return 0, errors.New("funded combo lacks relation receipt")
	}
	legs, _ := json.Marshal(p.Legs)
	systemIDs, _ := json.Marshal(p.SystemIDs)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var allowed int
	if err := tx.QueryRowContext(ctx, `SELECT allowed FROM funded_relation_receipts WHERE receipt_id=?`, receiptID).Scan(&allowed); err != nil || allowed != 1 {
		if err == nil {
			err = errors.New("funded combo relation receipt is not allowed")
		}
		return 0, err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO paper_parlays(
ts,stake,price,contracts,tp,sl,status,legs,fees,route_source,cohort,system_ids,joint_p,expected_net_per_dollar,
canonical_system_id,combo_venue,leg_count,relation_class,producer_family,combo_route,experiment_epoch,combo_key)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, nowRFC(), p.Stake, p.Price, p.Contracts, p.TP, p.SL, "open",
		string(legs), p.Fees, p.RouteSource, p.Cohort, string(systemIDs), p.JointP, p.ExpectedNetPerDollar,
		p.CanonicalSystemID, p.ComboVenue, p.LegCount, p.RelationClass, p.ProducerFamily,
		p.ComboRoute, p.ExperimentEpoch, p.ComboKey)
	if err != nil {
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO funded_relation_position_links(
receipt_id,position_kind,position_id,created_ts) VALUES(?,'parlay',?,?)`, receiptID, id,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Store) RecordFundedRelationOutcomeForLinkedPosition(ctx context.Context, kind string, id int64,
	settled time.Time, pnl float64, source string) (bool, error) {
	var receiptID string
	err := s.db.QueryRowContext(ctx, `SELECT receipt_id FROM funded_relation_position_links
WHERE position_kind=? AND position_id=?`, strings.ToLower(strings.TrimSpace(kind)), id).Scan(&receiptID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return s.RecordFundedRelationOutcome(ctx, receiptID, settled, pnl, source)
}

// RecordFundedRelationOutcomesForSystemContract closes only the shared-Paper receipts owned by one
// exact opening system and contract. Specialist JSON books settle by their receipt IDs; merely
// sharing portfolio+ticker+side never grants this generic sweep ownership of those positions.
func (s *Store) RecordFundedRelationOutcomesForSystemContract(ctx context.Context,
	portfolio, venue, ticker, side, systemID string, settled time.Time, pnl float64, source string) (int, error) {
	portfolio, venue = strings.ToLower(strings.TrimSpace(portfolio)), strings.ToLower(strings.TrimSpace(venue))
	ticker, side = strings.TrimSpace(ticker), strings.ToUpper(strings.TrimSpace(side))
	systemID = strings.TrimSpace(systemID)
	if systemID == "" {
		return 0, errors.New("funded contract settlement lacks exact system identity")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.receipt_id,r.quantity,r.entry_price,r.entry_fee
FROM funded_relation_receipts r LEFT JOIN funded_relation_outcomes o ON o.receipt_id=r.receipt_id
WHERE r.allowed=1 AND o.receipt_id IS NULL AND r.portfolio=? AND r.candidate_venue=?
 AND r.candidate_ticker=? AND r.candidate_side=? AND r.system_id=?
 ORDER BY r.decision_ts,r.receipt_id`, portfolio, venue, ticker, side, systemID)
	if err != nil {
		return 0, err
	}
	type pending struct {
		id     string
		weight float64
	}
	var all []pending
	total := 0.0
	for rows.Next() {
		var p pending
		var qty, px, fee float64
		if err := rows.Scan(&p.id, &qty, &px, &fee); err != nil {
			_ = rows.Close()
			return 0, err
		}
		p.weight = qty*px + fee
		if p.weight <= 0 {
			p.weight = 1
		}
		total += p.weight
		all = append(all, p)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	count := 0
	for _, p := range all {
		share := pnl / float64(len(all))
		if total > 0 {
			share = pnl * p.weight / total
		}
		inserted, err := s.RecordFundedRelationOutcome(ctx, p.id, settled, share, source)
		if err != nil {
			return count, err
		}
		if inserted {
			count++
		}
	}
	return count, nil
}

// RecordFundedRelationOutcomesForContract retains the older API only for unambiguous historical
// callers. If more than one system owns an open receipt on the contract it fails closed instead of
// recreating the cross-ledger P&L split that this compatibility path predates.
func (s *Store) RecordFundedRelationOutcomesForContract(ctx context.Context, portfolio, venue, ticker, side string,
	settled time.Time, pnl float64, source string) (int, error) {
	portfolio, venue = strings.ToLower(strings.TrimSpace(portfolio)), strings.ToLower(strings.TrimSpace(venue))
	ticker, side = strings.TrimSpace(ticker), strings.ToUpper(strings.TrimSpace(side))
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT r.system_id
FROM funded_relation_receipts r LEFT JOIN funded_relation_outcomes o ON o.receipt_id=r.receipt_id
WHERE r.allowed=1 AND o.receipt_id IS NULL AND r.portfolio=? AND r.candidate_venue=?
 AND r.candidate_ticker=? AND r.candidate_side=? ORDER BY r.system_id`,
		portfolio, venue, ticker, side)
	if err != nil {
		return 0, err
	}
	var systems []string
	for rows.Next() {
		var system string
		if err := rows.Scan(&system); err != nil {
			_ = rows.Close()
			return 0, err
		}
		systems = append(systems, system)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if len(systems) == 0 {
		return 0, nil
	}
	if len(systems) != 1 {
		return 0, errors.New("ambiguous funded contract settlement requires exact system identity")
	}
	return s.RecordFundedRelationOutcomesForSystemContract(ctx, portfolio, venue, ticker, side,
		systems[0], settled, pnl, source)
}

type RelationPerformanceCell struct {
	Relation      string  `json:"relation"`
	UniqueBets    int     `json:"unique_bets"`
	Receipts      int     `json:"receipts"`
	NetDollars    float64 `json:"net_dollars"`
	DollarsPerBet float64 `json:"dollars_per_bet"`
}

type PortfolioRelationPerformance struct {
	Portfolio string                    `json:"portfolio"`
	Epoch     string                    `json:"epoch"`
	Cells     []RelationPerformanceCell `json:"cells"`
}

// FundedSystemPerformance is sized simulated Paper money grouped by the immutable execution
// identity recorded before placement. It is intentionally separate from UnitTrialLeaderboardStat:
// unit trials answer "one executable share each", while this row answers "what the funded Paper
// portfolio actually made". N counts settled accepted decisions; UniquePositions and
// UniqueContracts expose the de-duplicated position/contract coverage alongside it.
type FundedSystemPerformance struct {
	SystemID                   string  `json:"system_id"`
	Portfolio                  string  `json:"portfolio"`
	Venue                      string  `json:"venue"`
	Side                       string  `json:"side"`
	RouteKind                  string  `json:"route_kind"`
	EconomicsScope             string  `json:"economics_scope"`
	EvidenceTier               string  `json:"evidence_tier"`
	FillConditioned            bool    `json:"fill_conditioned"`
	ProfitEvidence             bool    `json:"profit_evidence"`
	VoidReason                 string  `json:"void_reason,omitempty"`
	LiveAuthorizes             bool    `json:"live_authorizes"`
	AcceptedReceipts           int     `json:"accepted_receipts"`
	N                          int     `json:"n"`
	UniquePositions            int     `json:"unique_positions"`
	UniqueContracts            int     `json:"unique_contracts"`
	OpenReceipts               int     `json:"open_receipts"`
	CancelledReceipts          int     `json:"cancelled_receipts"`
	PlacementFailedReceipts    int     `json:"placement_failed_receipts"`
	ReconciledReceipts         int     `json:"reconciled_receipts"`
	SettledContracts           float64 `json:"settled_contracts"`
	TotalRealizedProfitDollars float64 `json:"total_realized_profit_dollars"`
	FirstDecision              string  `json:"first_decision"`
	LastActivity               string  `json:"last_activity"`
	ElapsedSeconds             float64 `json:"elapsed_seconds"`
	ElapsedDays                float64 `json:"elapsed_days"`
	NetPerCalendarDay          float64 `json:"net_per_calendar_day"`
}

// FundedSystemPerformanceRows reads only immutable accepted receipts and their terminal outcomes.
// The calendar clock runs from the first accepted decision through as-of, including quiet time.
// Future-dated outcomes are censored as still open at this as-of rather than leaking later truth.
func (s *Store) FundedSystemPerformanceRows(ctx context.Context, now time.Time) ([]FundedSystemPerformance, error) {
	return s.FundedSystemPerformanceRowsSince(ctx, time.Time{}, now)
}

// FundedSystemPerformanceRowsSince applies the same durable epoch boundary as the portfolio
// briefing. A zero boundary retains the lifetime-compatible behavior used by exports/tests.
func (s *Store) FundedSystemPerformanceRowsSince(ctx context.Context, since, now time.Time) ([]FundedSystemPerformance, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	asOf := now.Format(time.RFC3339Nano)
	sinceS := "0001-01-01T00:00:00Z"
	if !since.IsZero() {
		sinceS = since.UTC().Format(time.RFC3339Nano)
	}
	rows, err := s.db.QueryContext(ctx, `
WITH accepted AS (
 SELECT * FROM funded_relation_receipts
 WHERE allowed=1 AND julianday(decision_ts) IS NOT NULL
   AND julianday(decision_ts)>julianday(?) AND julianday(decision_ts)<=julianday(?)
), outcomes AS (
 SELECT o.*,COALESCE(c.corrected_pnl_dollars,o.pnl_dollars) effective_pnl,
        CASE WHEN c.receipt_id IS NOT NULL THEN 1 ELSE 0 END reconciled
 FROM funded_relation_outcomes o
 LEFT JOIN funded_relation_outcome_corrections c ON c.receipt_id=o.receipt_id
 WHERE julianday(settled_ts) IS NOT NULL AND julianday(settled_ts)<=julianday(?)
)
SELECT r.system_id,r.portfolio,r.candidate_venue,UPPER(r.candidate_side),r.route_kind,
       COUNT(*) accepted_receipts,
       SUM(CASE WHEN o.outcome_status='settled' THEN 1 ELSE 0 END) settled_receipts,
       COUNT(DISTINCT CASE WHEN o.outcome_status='settled' THEN r.position_fingerprint END) unique_positions,
       COUNT(DISTINCT CASE WHEN o.outcome_status='settled'
             THEN LOWER(r.candidate_venue)||'|'||r.candidate_ticker||'|'||UPPER(r.candidate_side) END) unique_contracts,
       SUM(CASE WHEN o.receipt_id IS NULL THEN 1 ELSE 0 END) open_receipts,
       SUM(CASE WHEN o.outcome_status='cancelled' THEN 1 ELSE 0 END) cancelled_receipts,
       SUM(CASE WHEN o.outcome_status='placement_failed' THEN 1 ELSE 0 END) placement_failed_receipts,
       SUM(CASE WHEN o.outcome_status='settled' THEN o.reconciled ELSE 0 END) reconciled_receipts,
       COALESCE(SUM(CASE WHEN o.outcome_status='settled' THEN r.quantity ELSE 0 END),0) settled_contracts,
       COALESCE(SUM(CASE WHEN o.outcome_status='settled' THEN o.effective_pnl ELSE 0 END),0) total_pnl,
       MIN(r.decision_ts) first_decision,
       MAX(CASE WHEN o.receipt_id IS NOT NULL THEN o.settled_ts ELSE r.decision_ts END) last_activity
FROM accepted r LEFT JOIN outcomes o ON o.receipt_id=r.receipt_id
GROUP BY r.system_id,r.portfolio,r.candidate_venue,UPPER(r.candidate_side),r.route_kind
ORDER BY total_pnl DESC,r.system_id,r.portfolio,r.candidate_venue,r.candidate_side,r.route_kind`, sinceS, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]FundedSystemPerformance, 0)
	for rows.Next() {
		var v FundedSystemPerformance
		var firstS, lastS string
		if err := rows.Scan(&v.SystemID, &v.Portfolio, &v.Venue, &v.Side, &v.RouteKind,
			&v.AcceptedReceipts, &v.N, &v.UniquePositions, &v.UniqueContracts, &v.OpenReceipts,
			&v.CancelledReceipts, &v.PlacementFailedReceipts, &v.ReconciledReceipts, &v.SettledContracts,
			&v.TotalRealizedProfitDollars, &firstS, &lastS); err != nil {
			return nil, err
		}
		first, ok := parseUnitTrialTime(firstS)
		if !ok || first.After(now) {
			continue
		}
		last, ok := parseUnitTrialTime(lastS)
		if !ok || last.Before(first) {
			last = first
		}
		v.EconomicsScope = "funded-paper-simulation"
		v.EvidenceTier = "funded_paper_simulation"
		v.FillConditioned = false
		v.ProfitEvidence = false
		v.VoidReason = "Paper placement and fill are simulated"
		v.LiveAuthorizes = false
		v.FirstDecision = first.Format(time.RFC3339Nano)
		v.LastActivity = last.Format(time.RFC3339Nano)
		v.ElapsedSeconds = unitTrialElapsedSeconds(now, first)
		v.ElapsedDays = v.ElapsedSeconds / (24 * 60 * 60)
		v.NetPerCalendarDay = v.TotalRealizedProfitDollars / v.ElapsedDays
		out = append(out, v)
	}
	return out, rows.Err()
}

type relationSettledRow struct {
	receiptID, portfolio, relation, betKey string
	pnl                                    float64
	legs                                   []FundedRelationLeg
}

// FundedRelationPerformance recomputes dependence over the requested epoch: an event represented
// by one distinct contract is independent; two or more distinct contracts make every linked bet
// dependent. Missing frozen event identity remains unknown and is never promoted to independent.
func (s *Store) FundedRelationPerformance(ctx context.Context, epoch time.Time) ([]PortfolioRelationPerformance, error) {
	// One joined read replaces the former receipt query plus one leg query per settled bet. The old
	// N+1 path routinely exhausted a 1.5-second briefing budget and occupied every SQLite connection
	// when several dashboards refreshed together.
	rows, err := s.db.QueryContext(ctx, `SELECT r.receipt_id,r.portfolio,r.relation_state,
r.position_fingerprint,COALESCE(c.corrected_pnl_dollars,o.pnl_dollars),l.venue,l.ticker,l.side,l.canonical_event_id,
l.event_version,l.relation_state,l.matched_position_ids_json
FROM funded_relation_receipts r
JOIN funded_relation_outcomes o ON o.receipt_id=r.receipt_id
LEFT JOIN funded_relation_outcome_corrections c ON c.receipt_id=o.receipt_id
LEFT JOIN funded_relation_receipt_legs l ON l.receipt_id=r.receipt_id
WHERE r.allowed=1 AND o.outcome_status='settled' AND o.settled_ts>=?
ORDER BY r.portfolio,r.receipt_id,l.leg_no`, epoch.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	var settled []relationSettledRow
	byReceipt := map[string]int{}
	eventContracts := map[string]map[string]struct{}{}
	for rows.Next() {
		var r relationSettledRow
		var venue, ticker, side, eventID, relationState, matched sql.NullString
		var eventVersion sql.NullInt64
		if err := rows.Scan(&r.receiptID, &r.portfolio, &r.relation, &r.betKey, &r.pnl,
			&venue, &ticker, &side, &eventID, &eventVersion, &relationState, &matched); err != nil {
			_ = rows.Close()
			return nil, err
		}
		idx, exists := byReceipt[r.receiptID]
		if !exists {
			idx = len(settled)
			byReceipt[r.receiptID] = idx
			settled = append(settled, r)
		}
		if venue.Valid {
			leg := FundedRelationLeg{Venue: venue.String, Ticker: ticker.String, Side: side.String,
				CanonicalEventID: eventID.String, EventVersion: int(eventVersion.Int64),
				RelationState: relationState.String}
			_ = json.Unmarshal([]byte(matched.String), &leg.MatchedPositionIDs)
			settled[idx].legs = append(settled[idx].legs, leg)
			if leg.CanonicalEventID != "" && leg.EventVersion > 0 {
				event := fmt.Sprintf("%s@%d", leg.CanonicalEventID, leg.EventVersion)
				if eventContracts[event] == nil {
					eventContracts[event] = map[string]struct{}{}
				}
				eventContracts[event][strings.ToLower(leg.Venue)+"|"+leg.Ticker+"|"+strings.ToUpper(leg.Side)] = struct{}{}
			}
		}
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	type agg struct {
		receipts int
		pnl      float64
	}
	byPortfolio := map[string]map[string]map[string]*agg{}
	for _, r := range settled {
		state := "independent"
		if len(r.legs) == 0 {
			state = "unknown"
		}
		for _, l := range r.legs {
			if l.CanonicalEventID == "" || l.EventVersion <= 0 || l.RelationState == "unknown" {
				state = "unknown"
				break
			}
			if len(eventContracts[fmt.Sprintf("%s@%d", l.CanonicalEventID, l.EventVersion)]) >= 2 {
				state = "dependent"
			}
		}
		if byPortfolio[r.portfolio] == nil {
			byPortfolio[r.portfolio] = map[string]map[string]*agg{}
		}
		if byPortfolio[r.portfolio][state] == nil {
			byPortfolio[r.portfolio][state] = map[string]*agg{}
		}
		a := byPortfolio[r.portfolio][state][r.betKey]
		if a == nil {
			a = &agg{}
			byPortfolio[r.portfolio][state][r.betKey] = a
		}
		a.receipts++
		a.pnl += r.pnl
	}
	portfolios := make([]string, 0, len(byPortfolio))
	for p := range byPortfolio {
		portfolios = append(portfolios, p)
	}
	sort.Strings(portfolios)
	out := make([]PortfolioRelationPerformance, 0, len(portfolios))
	for _, p := range portfolios {
		view := PortfolioRelationPerformance{Portfolio: p, Epoch: epoch.UTC().Format(time.RFC3339Nano)}
		for _, state := range []string{"independent", "dependent", "unknown"} {
			cell := RelationPerformanceCell{Relation: state}
			for _, a := range byPortfolio[p][state] {
				cell.UniqueBets++
				cell.Receipts += a.receipts
				cell.NetDollars += a.pnl
			}
			if cell.UniqueBets > 0 {
				cell.DollarsPerBet = cell.NetDollars / float64(cell.UniqueBets)
			}
			view.Cells = append(view.Cells, cell)
		}
		out = append(out, view)
	}
	return out, nil
}

type ProperRelationPerformance struct {
	Transform     string  `json:"transform"`
	Relation      string  `json:"relation"`
	UniqueBets    int     `json:"unique_bets"`
	ResearchNet   float64 `json:"research_net_dollars"`
	DollarsPerBet float64 `json:"research_dollars_per_bet"`
}

// ProperScoreRelationPerformance is prospective research economics, never portfolio P&L. Frozen
// research_observation_id supplies the event/version; the current catalog is never used to backfill.
func (s *Store) ProperScoreRelationPerformance(ctx context.Context, epoch time.Time) ([]ProperRelationPerformance, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT p.transform,p.platform,p.ticker,p.forecast_origin_side,
p.realized_net,o.canonical_event_id,o.event_version
FROM research_proper_score_trials p
JOIN research_system_observations o ON o.id=p.research_observation_id
WHERE p.settled=1 AND p.realized_net IS NOT NULL AND p.research_observation_id>0 AND p.closed_ts>=?
ORDER BY p.transform,p.id`, epoch.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	type row struct {
		transform, contract, event string
		net                        float64
	}
	var all []row
	events := map[string]map[string]struct{}{}
	for rows.Next() {
		var transform, venue, ticker, side, event string
		var net float64
		var version int
		if err := rows.Scan(&transform, &venue, &ticker, &side, &net, &event, &version); err != nil {
			_ = rows.Close()
			return nil, err
		}
		contract := strings.ToLower(venue) + "|" + ticker + "|" + strings.ToUpper(side)
		eventKey := ""
		if event != "" && version > 0 {
			eventKey = fmt.Sprintf("%s@%d", event, version)
			transformEventKey := strings.ToLower(strings.TrimSpace(transform)) + "\x00" + eventKey
			if events[transformEventKey] == nil {
				events[transformEventKey] = map[string]struct{}{}
			}
			events[transformEventKey][contract] = struct{}{}
		}
		all = append(all, row{transform: transform, contract: contract, event: eventKey, net: net})
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	type agg struct{ net float64 }
	groups := map[string]map[string]map[string]*agg{}
	for _, r := range all {
		state := "independent"
		if r.event == "" {
			state = "unknown"
		} else if len(events[strings.ToLower(strings.TrimSpace(r.transform))+"\x00"+r.event]) >= 2 {
			state = "dependent"
		}
		if groups[r.transform] == nil {
			groups[r.transform] = map[string]map[string]*agg{}
		}
		if groups[r.transform][state] == nil {
			groups[r.transform][state] = map[string]*agg{}
		}
		a := groups[r.transform][state][r.contract]
		if a == nil {
			a = &agg{}
			groups[r.transform][state][r.contract] = a
		}
		a.net += r.net
	}
	var out []ProperRelationPerformance
	for _, transform := range []string{"brier", "log", "spherical"} {
		for _, state := range []string{"independent", "dependent", "unknown"} {
			v := ProperRelationPerformance{Transform: transform, Relation: state}
			for _, a := range groups[transform][state] {
				v.UniqueBets++
				v.ResearchNet += a.net
			}
			if v.UniqueBets > 0 {
				v.DollarsPerBet = v.ResearchNet / float64(v.UniqueBets)
			}
			out = append(out, v)
		}
	}
	return out, nil
}
