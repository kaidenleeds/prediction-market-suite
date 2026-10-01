package storage

// Immutable reconciliation between one authenticated Kalshi account fill and the matching
// public trade print. Both venue messages use the same trade_id, but they arrive on independent
// WebSocket channels and may arrive in either order. This ledger lives only in
// execution_shadow.db; it is evidence, never trading authority.

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

const kalshiTradeReconciliationSchema = `
CREATE TABLE IF NOT EXISTS kalshi_trade_reconciliation_events (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 event_id TEXT NOT NULL UNIQUE,
 trade_id TEXT NOT NULL,
 observed_ts TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('PENDING','MATCHED','MISMATCH')),
 reason TEXT NOT NULL,
 reservation_id TEXT NOT NULL DEFAULT '',
 attempt_id TEXT NOT NULL DEFAULT '',

 private_present INTEGER NOT NULL CHECK(private_present IN (0,1)),
 private_order_id TEXT NOT NULL DEFAULT '',
 private_ticker TEXT NOT NULL DEFAULT '',
 private_side TEXT NOT NULL DEFAULT '' CHECK(private_side IN ('','YES','NO')),
 private_action TEXT NOT NULL DEFAULT '' CHECK(private_action IN ('','BUY','SELL')),
 private_qty REAL,
 private_price REAL,
 private_fee REAL,
 private_fee_known INTEGER NOT NULL DEFAULT 0 CHECK(private_fee_known IN (0,1)),
 private_is_taker INTEGER NOT NULL DEFAULT 0 CHECK(private_is_taker IN (0,1)),
 private_source_ts TEXT NOT NULL DEFAULT '',
 private_received_ts TEXT NOT NULL DEFAULT '',

 public_present INTEGER NOT NULL CHECK(public_present IN (0,1)),
 public_ticker TEXT NOT NULL DEFAULT '',
 public_qty REAL,
 public_yes_price REAL,
 public_no_price REAL,
 public_taker_outcome_side TEXT NOT NULL DEFAULT '',
 public_taker_book_side TEXT NOT NULL DEFAULT '',
 public_aggressor TEXT NOT NULL DEFAULT '',
 public_is_block INTEGER NOT NULL DEFAULT 0 CHECK(public_is_block IN (0,1)),
 public_source_ts TEXT NOT NULL DEFAULT '',
 public_received_ts TEXT NOT NULL DEFAULT '',

 evidence_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(evidence_json)),

 CHECK((private_present=0 AND private_order_id='' AND private_ticker='' AND private_side=''
        AND private_action='' AND private_qty IS NULL AND private_price IS NULL
        AND private_fee IS NULL AND private_fee_known=0 AND private_is_taker=0
        AND private_source_ts='' AND private_received_ts='')
    OR (private_present=1 AND private_order_id<>'' AND private_ticker<>''
        AND private_side<>'' AND private_qty>0 AND private_price>0 AND private_price<1
        AND private_received_ts<>''
        AND ((private_fee_known=0 AND private_fee IS NULL)
          OR (private_fee_known=1 AND private_fee IS NOT NULL AND private_fee>=0)))),
 CHECK((public_present=0 AND public_ticker='' AND public_qty IS NULL
        AND public_yes_price IS NULL AND public_no_price IS NULL
        AND public_taker_outcome_side='' AND public_taker_book_side=''
        AND public_aggressor='' AND public_is_block=0
        AND public_source_ts='' AND public_received_ts='')
    OR (public_present=1 AND public_ticker<>'' AND public_qty>0
        AND public_yes_price>0 AND public_yes_price<1
        AND public_no_price>0 AND public_no_price<1
        AND public_received_ts<>'')),
 CHECK(status='MISMATCH' OR private_present=1),
 CHECK(status='PENDING' OR status='MISMATCH' OR public_present=1)
);
CREATE INDEX IF NOT EXISTS idx_kalshi_trade_reconcile_trade
 ON kalshi_trade_reconciliation_events(trade_id,id);
CREATE INDEX IF NOT EXISTS idx_kalshi_trade_reconcile_status
 ON kalshi_trade_reconciliation_events(status,id);
CREATE INDEX IF NOT EXISTS idx_kalshi_trade_reconcile_order
 ON kalshi_trade_reconciliation_events(private_order_id,id)
 WHERE private_order_id<>'';
CREATE INDEX IF NOT EXISTS idx_kalshi_trade_reconcile_attempt
 ON kalshi_trade_reconciliation_events(attempt_id,id)
 WHERE attempt_id<>'';
CREATE TRIGGER IF NOT EXISTS kalshi_trade_reconciliation_events_no_update
 BEFORE UPDATE ON kalshi_trade_reconciliation_events
 BEGIN SELECT RAISE(ABORT,'immutable Kalshi trade reconciliation event'); END;
CREATE TRIGGER IF NOT EXISTS kalshi_trade_reconciliation_events_no_delete
 BEFORE DELETE ON kalshi_trade_reconciliation_events
 BEGIN SELECT RAISE(ABORT,'Kalshi trade reconciliation events are append-preserved'); END;
`

func migrateKalshiTradeReconciliationSchema(db *sql.DB) error {
	if db == nil {
		return errors.New("nil execution-shadow database")
	}
	_, err := db.Exec(kalshiTradeReconciliationSchema)
	return err
}

// KalshiTradeReconciliationEvent is one append-only state observation. PENDING means an
// authenticated private fill has no matching public print yet. MATCHED means all comparable venue
// facts agree. MISMATCH always carries an explicit reason; absence is never treated as a match.
type KalshiTradeReconciliationEvent struct {
	ID                                       int64
	EventID, TradeID, Status, Reason         string
	ObservedAt                               time.Time
	ReservationID, AttemptID                 string
	PrivatePresent                           bool
	PrivateOrderID, PrivateTicker            string
	PrivateSide, PrivateAction               string
	PrivateQty, PrivatePrice, PrivateFee     float64
	PrivateFeeKnown, PrivateIsTaker          bool
	PrivateSourceAt, PrivateReceivedAt       time.Time
	PublicPresent                            bool
	PublicTicker                             string
	PublicQty, PublicYesPrice, PublicNoPrice float64
	PublicTakerOutcomeSide                   string
	PublicTakerBookSide                      string
	PublicAggressor                          string
	PublicIsBlock                            bool
	PublicSourceAt, PublicReceivedAt         time.Time
	Evidence                                 map[string]any
}

func normalizeKalshiTradeReconciliationEvent(
	in KalshiTradeReconciliationEvent,
) (KalshiTradeReconciliationEvent, string, error) {
	in.EventID = strings.TrimSpace(in.EventID)
	in.TradeID = strings.TrimSpace(in.TradeID)
	in.Status = strings.ToUpper(strings.TrimSpace(in.Status))
	in.Reason = strings.TrimSpace(in.Reason)
	in.ReservationID = strings.TrimSpace(in.ReservationID)
	in.AttemptID = strings.TrimSpace(in.AttemptID)
	in.PrivateOrderID = strings.TrimSpace(in.PrivateOrderID)
	in.PrivateTicker = strings.TrimSpace(in.PrivateTicker)
	in.PrivateSide = strings.ToUpper(strings.TrimSpace(in.PrivateSide))
	in.PrivateAction = strings.ToUpper(strings.TrimSpace(in.PrivateAction))
	in.PublicTicker = strings.TrimSpace(in.PublicTicker)
	in.PublicTakerOutcomeSide = strings.ToLower(strings.TrimSpace(in.PublicTakerOutcomeSide))
	in.PublicTakerBookSide = strings.ToLower(strings.TrimSpace(in.PublicTakerBookSide))
	in.PublicAggressor = strings.ToLower(strings.TrimSpace(in.PublicAggressor))
	if in.EventID == "" || in.TradeID == "" || in.ObservedAt.IsZero() || in.Reason == "" {
		return in, "", errors.New("Kalshi trade reconciliation event lacks identity/time/reason")
	}
	switch in.Status {
	case "PENDING", "MATCHED", "MISMATCH":
	default:
		return in, "", fmt.Errorf("invalid Kalshi trade reconciliation status %q", in.Status)
	}
	finite := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }
	if in.PrivatePresent {
		if in.PrivateOrderID == "" || in.PrivateTicker == "" ||
			(in.PrivateSide != "YES" && in.PrivateSide != "NO") ||
			(in.PrivateAction != "" && in.PrivateAction != "BUY" && in.PrivateAction != "SELL") ||
			!finite(in.PrivateQty) || in.PrivateQty <= 0 ||
			!finite(in.PrivatePrice) || in.PrivatePrice <= 0 || in.PrivatePrice >= 1 ||
			in.PrivateReceivedAt.IsZero() || !finite(in.PrivateFee) || in.PrivateFee < 0 {
			return in, "", errors.New("invalid private Kalshi fill evidence")
		}
	} else {
		in.PrivateOrderID, in.PrivateTicker, in.PrivateSide, in.PrivateAction = "", "", "", ""
		in.PrivateQty, in.PrivatePrice, in.PrivateFee = 0, 0, 0
		in.PrivateFeeKnown, in.PrivateIsTaker = false, false
		in.PrivateSourceAt, in.PrivateReceivedAt = time.Time{}, time.Time{}
	}
	if in.PublicPresent {
		if in.PublicTicker == "" || !finite(in.PublicQty) || in.PublicQty <= 0 ||
			!finite(in.PublicYesPrice) || in.PublicYesPrice <= 0 || in.PublicYesPrice >= 1 ||
			!finite(in.PublicNoPrice) || in.PublicNoPrice <= 0 || in.PublicNoPrice >= 1 ||
			in.PublicReceivedAt.IsZero() {
			return in, "", errors.New("invalid public Kalshi trade evidence")
		}
	} else {
		in.PublicTicker, in.PublicTakerOutcomeSide = "", ""
		in.PublicTakerBookSide, in.PublicAggressor = "", ""
		in.PublicQty, in.PublicYesPrice, in.PublicNoPrice = 0, 0, 0
		in.PublicIsBlock = false
		in.PublicSourceAt, in.PublicReceivedAt = time.Time{}, time.Time{}
	}
	if in.Status != "MISMATCH" && !in.PrivatePresent {
		return in, "", errors.New("pending/matched reconciliation lacks authenticated private fill")
	}
	if in.Status == "MATCHED" && !in.PublicPresent {
		return in, "", errors.New("matched reconciliation lacks public trade")
	}
	if in.Evidence == nil {
		in.Evidence = map[string]any{}
	}
	raw, err := json.Marshal(in.Evidence)
	if err != nil {
		return in, "", fmt.Errorf("marshal Kalshi trade reconciliation evidence: %w", err)
	}
	var canonical any
	if err = json.Unmarshal(raw, &canonical); err != nil {
		return in, "", fmt.Errorf("normalize Kalshi trade reconciliation evidence: %w", err)
	}
	raw, err = json.Marshal(canonical)
	if err != nil {
		return in, "", err
	}
	return in, string(raw), nil
}

const kalshiTradeReconciliationColumns = `id,event_id,trade_id,observed_ts,status,reason,
reservation_id,attempt_id,private_present,private_order_id,private_ticker,private_side,
private_action,private_qty,private_price,private_fee,private_fee_known,private_is_taker,
private_source_ts,private_received_ts,public_present,public_ticker,public_qty,public_yes_price,
public_no_price,public_taker_outcome_side,public_taker_book_side,public_aggressor,public_is_block,
public_source_ts,public_received_ts,evidence_json`

func optionalKalshiTradeTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func optionalKalshiTradeFloat(present bool, value float64) any {
	if !present {
		return nil
	}
	return value
}

func scanKalshiTradeReconciliationEvent(scanner interface {
	Scan(...any) error
}) (KalshiTradeReconciliationEvent, error) {
	var out KalshiTradeReconciliationEvent
	var observed, privateSource, privateReceived, publicSource, publicReceived, evidence string
	var privatePresent, privateFeeKnown, privateIsTaker, publicPresent, publicIsBlock int
	var privateQty, privatePrice, privateFee sql.NullFloat64
	var publicQty, publicYes, publicNo sql.NullFloat64
	err := scanner.Scan(&out.ID, &out.EventID, &out.TradeID, &observed, &out.Status, &out.Reason,
		&out.ReservationID, &out.AttemptID, &privatePresent, &out.PrivateOrderID,
		&out.PrivateTicker, &out.PrivateSide, &out.PrivateAction, &privateQty, &privatePrice,
		&privateFee, &privateFeeKnown, &privateIsTaker, &privateSource, &privateReceived,
		&publicPresent, &out.PublicTicker, &publicQty, &publicYes, &publicNo,
		&out.PublicTakerOutcomeSide, &out.PublicTakerBookSide, &out.PublicAggressor,
		&publicIsBlock, &publicSource, &publicReceived, &evidence)
	if err != nil {
		return out, err
	}
	out.ObservedAt, err = time.Parse(time.RFC3339Nano, observed)
	if err != nil {
		return out, err
	}
	parseOptional := func(raw string, dst *time.Time) error {
		if raw == "" {
			return nil
		}
		parsed, parseErr := time.Parse(time.RFC3339Nano, raw)
		if parseErr == nil {
			*dst = parsed
		}
		return parseErr
	}
	if err = parseOptional(privateSource, &out.PrivateSourceAt); err != nil {
		return out, err
	}
	if err = parseOptional(privateReceived, &out.PrivateReceivedAt); err != nil {
		return out, err
	}
	if err = parseOptional(publicSource, &out.PublicSourceAt); err != nil {
		return out, err
	}
	if err = parseOptional(publicReceived, &out.PublicReceivedAt); err != nil {
		return out, err
	}
	out.PrivatePresent, out.PrivateFeeKnown = privatePresent == 1, privateFeeKnown == 1
	out.PrivateIsTaker, out.PublicPresent = privateIsTaker == 1, publicPresent == 1
	out.PublicIsBlock = publicIsBlock == 1
	out.PrivateQty, out.PrivatePrice, out.PrivateFee =
		privateQty.Float64, privatePrice.Float64, privateFee.Float64
	out.PublicQty, out.PublicYesPrice, out.PublicNoPrice =
		publicQty.Float64, publicYes.Float64, publicNo.Float64
	if err = json.Unmarshal([]byte(evidence), &out.Evidence); err != nil {
		return out, err
	}
	return out, nil
}

func sameKalshiTradeReconciliationEvent(a, b KalshiTradeReconciliationEvent) bool {
	af, _ := json.Marshal(a.Evidence)
	bf, _ := json.Marshal(b.Evidence)
	return a.EventID == b.EventID && a.TradeID == b.TradeID && a.Status == b.Status &&
		a.Reason == b.Reason && a.ObservedAt.Equal(b.ObservedAt) &&
		a.ReservationID == b.ReservationID && a.AttemptID == b.AttemptID &&
		a.PrivatePresent == b.PrivatePresent && a.PrivateOrderID == b.PrivateOrderID &&
		a.PrivateTicker == b.PrivateTicker && a.PrivateSide == b.PrivateSide &&
		a.PrivateAction == b.PrivateAction && a.PrivateQty == b.PrivateQty &&
		a.PrivatePrice == b.PrivatePrice && a.PrivateFee == b.PrivateFee &&
		a.PrivateFeeKnown == b.PrivateFeeKnown && a.PrivateIsTaker == b.PrivateIsTaker &&
		a.PrivateSourceAt.Equal(b.PrivateSourceAt) &&
		a.PrivateReceivedAt.Equal(b.PrivateReceivedAt) &&
		a.PublicPresent == b.PublicPresent && a.PublicTicker == b.PublicTicker &&
		a.PublicQty == b.PublicQty && a.PublicYesPrice == b.PublicYesPrice &&
		a.PublicNoPrice == b.PublicNoPrice &&
		a.PublicTakerOutcomeSide == b.PublicTakerOutcomeSide &&
		a.PublicTakerBookSide == b.PublicTakerBookSide &&
		a.PublicAggressor == b.PublicAggressor && a.PublicIsBlock == b.PublicIsBlock &&
		a.PublicSourceAt.Equal(b.PublicSourceAt) &&
		a.PublicReceivedAt.Equal(b.PublicReceivedAt) && string(af) == string(bf)
}

func appendKalshiTradeReconciliationEventTx(ctx context.Context, tx *sql.Tx,
	in KalshiTradeReconciliationEvent, evidence string) (bool, error) {
	var existingID int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM kalshi_trade_reconciliation_events
WHERE event_id=?`, in.EventID).Scan(&existingID)
	if err == nil {
		existing, scanErr := scanKalshiTradeReconciliationEvent(tx.QueryRowContext(ctx,
			`SELECT `+kalshiTradeReconciliationColumns+
				` FROM kalshi_trade_reconciliation_events WHERE id=?`, existingID))
		if scanErr != nil {
			return false, scanErr
		}
		if !sameKalshiTradeReconciliationEvent(existing, in) {
			return false, errors.New("Kalshi trade reconciliation event id reused with different evidence")
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	var latestStatus string
	latestErr := tx.QueryRowContext(ctx, `SELECT status
FROM kalshi_trade_reconciliation_events WHERE trade_id=? ORDER BY id DESC LIMIT 1`,
		in.TradeID).Scan(&latestStatus)
	if latestErr == nil {
		if in.Status == "PENDING" &&
			(latestStatus == "MATCHED" || latestStatus == "MISMATCH") {
			// An old private fill may replay after restart. Preserve append-only terminal truth
			// rather than letting a later PENDING row regress the latest state.
			return false, nil
		}
		if latestStatus == "MISMATCH" && in.Status == "MATCHED" {
			// Once two venue receipts contradict, later enrichment can add evidence but cannot
			// downgrade the latest state to MATCHED. Keep the conservative terminal sticky across
			// restart as well as in the in-memory join.
			return false, nil
		}
	}
	if latestErr != nil && !errors.Is(latestErr, sql.ErrNoRows) {
		return false, latestErr
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO kalshi_trade_reconciliation_events(
event_id,trade_id,observed_ts,status,reason,reservation_id,attempt_id,
private_present,private_order_id,private_ticker,private_side,private_action,private_qty,
private_price,private_fee,private_fee_known,private_is_taker,private_source_ts,private_received_ts,
public_present,public_ticker,public_qty,public_yes_price,public_no_price,
public_taker_outcome_side,public_taker_book_side,public_aggressor,public_is_block,
public_source_ts,public_received_ts,evidence_json)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		in.EventID, in.TradeID, in.ObservedAt.UTC().Format(time.RFC3339Nano), in.Status, in.Reason,
		in.ReservationID, in.AttemptID, in.PrivatePresent, in.PrivateOrderID, in.PrivateTicker,
		in.PrivateSide, in.PrivateAction, optionalKalshiTradeFloat(in.PrivatePresent, in.PrivateQty),
		optionalKalshiTradeFloat(in.PrivatePresent, in.PrivatePrice),
		optionalKalshiTradeFloat(in.PrivatePresent && in.PrivateFeeKnown, in.PrivateFee),
		in.PrivateFeeKnown, in.PrivateIsTaker, optionalKalshiTradeTime(in.PrivateSourceAt),
		optionalKalshiTradeTime(in.PrivateReceivedAt), in.PublicPresent, in.PublicTicker,
		optionalKalshiTradeFloat(in.PublicPresent, in.PublicQty),
		optionalKalshiTradeFloat(in.PublicPresent, in.PublicYesPrice),
		optionalKalshiTradeFloat(in.PublicPresent, in.PublicNoPrice),
		in.PublicTakerOutcomeSide, in.PublicTakerBookSide, in.PublicAggressor, in.PublicIsBlock,
		optionalKalshiTradeTime(in.PublicSourceAt), optionalKalshiTradeTime(in.PublicReceivedAt),
		evidence)
	return err == nil, err
}

// AppendKalshiTradeReconciliationEvent is the direct append API used by recovery and tests.
// Runtime hot paths enqueue through the execution-shadow writer instead.
func (s *Store) AppendKalshiTradeReconciliationEvent(ctx context.Context,
	in KalshiTradeReconciliationEvent) (bool, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return false, err
	}
	in, evidence, err := normalizeKalshiTradeReconciliationEvent(in)
	if err != nil {
		return false, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	sessionID := s.executionShadowSessionID()
	if err = ensureExecutionShadowSessionTx(ctx, tx, sessionID); err != nil {
		return false, err
	}
	inserted, err := appendKalshiTradeReconciliationEventTx(ctx, tx, in, evidence)
	if err != nil {
		return false, err
	}
	if inserted {
		if _, err = tx.ExecContext(ctx, `UPDATE execution_shadow_writer_sessions
SET committed_records=committed_records+1 WHERE session_id=?`, sessionID); err != nil {
			return false, err
		}
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return inserted, nil
}

// ListPendingKalshiTradeReconciliations returns only latest-state private fills which still lack a
// terminal public-print comparison. It is the bounded restart/recovery seed.
func (s *Store) ListPendingKalshiTradeReconciliations(ctx context.Context,
	limit int) ([]KalshiTradeReconciliationEvent, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 4096 {
		limit = 4096
	}
	rows, err := db.QueryContext(ctx, `SELECT `+kalshiTradeReconciliationColumns+`
FROM kalshi_trade_reconciliation_events e
WHERE e.id IN (
 SELECT MAX(x.id) FROM kalshi_trade_reconciliation_events x GROUP BY x.trade_id
) AND e.status='PENDING' AND e.private_present=1
ORDER BY e.id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]KalshiTradeReconciliationEvent, 0)
	for rows.Next() {
		event, scanErr := scanKalshiTradeReconciliationEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

// ListLatestKalshiTradeReconciliations returns one latest immutable state per trade id. Runtime
// recovery seeds both pending and terminal identities from this bounded view so a replayed private
// fill cannot make a previously matched trade appear pending again.
func (s *Store) ListLatestKalshiTradeReconciliations(ctx context.Context,
	limit int) ([]KalshiTradeReconciliationEvent, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 4096 {
		limit = 4096
	}
	rows, err := db.QueryContext(ctx, `SELECT `+kalshiTradeReconciliationColumns+`
FROM kalshi_trade_reconciliation_events e
WHERE e.id IN (
 SELECT MAX(x.id) FROM kalshi_trade_reconciliation_events x GROUP BY x.trade_id
)
ORDER BY e.id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]KalshiTradeReconciliationEvent, 0)
	for rows.Next() {
		event, scanErr := scanKalshiTradeReconciliationEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

// ListKalshiTradeReconciliations is a bounded newest-first diagnostic/test reader.
func (s *Store) ListKalshiTradeReconciliations(ctx context.Context,
	limit int) ([]KalshiTradeReconciliationEvent, error) {
	db, err := s.executionShadowHandle()
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT `+kalshiTradeReconciliationColumns+
		` FROM kalshi_trade_reconciliation_events ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]KalshiTradeReconciliationEvent, 0)
	for rows.Next() {
		event, scanErr := scanKalshiTradeReconciliationEvent(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

// KalshiTradeLineage is the optional immutable money-path identity for an account fill. Older or
// manual orders legitimately have no reservation/attempt; callers must retain the order id and
// report lineage as unavailable rather than guessing by ticker or time.
type KalshiTradeLineage struct {
	ReservationID string
	AttemptID     string
}

// KalshiTradeLineageForOrder resolves an exact venue order id against the append-only pre-send
// pending-risk journal. It reads the main ledger only from the background reconciliation worker;
// the order and WebSocket paths never call it.
func (s *Store) KalshiTradeLineageForOrder(ctx context.Context,
	orderID string) (KalshiTradeLineage, bool, error) {
	var out KalshiTradeLineage
	orderID = strings.TrimSpace(orderID)
	if s == nil || s.db == nil || orderID == "" {
		return out, false, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT i.reservation_id,
i.execution_shadow_attempt_id
FROM live_pending_risk_events e
JOIN live_pending_risk_intents i ON i.reservation_id=e.reservation_id
WHERE e.order_id=?
ORDER BY i.created_ts DESC,i.reservation_id
LIMIT 2`, orderID)
	if err != nil {
		return out, false, err
	}
	defer rows.Close()
	var found []KalshiTradeLineage
	for rows.Next() {
		var row KalshiTradeLineage
		if err = rows.Scan(&row.ReservationID, &row.AttemptID); err != nil {
			return out, false, err
		}
		found = append(found, row)
	}
	if err = rows.Err(); err != nil {
		return out, false, err
	}
	if len(found) == 0 {
		return out, false, nil
	}
	if len(found) > 1 {
		return out, false, errors.New("Kalshi order id maps to multiple pending-risk reservations")
	}
	return found[0], true, nil
}
