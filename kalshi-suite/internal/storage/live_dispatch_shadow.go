package storage

// The live-dispatch shadow is a deliberately isolated execution ledger. It records what the
// Paper execution model would have done at the exact moment a real-money order was dispatched,
// then follows that simulated order to cancellation/fill/settlement without touching paper_fills
// or any normal Paper portfolio statistic.

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
)

const liveDispatchShadowSchema = `
CREATE TABLE IF NOT EXISTS live_dispatch_shadow (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 reservation_id TEXT NOT NULL UNIQUE,
 client_order_id TEXT NOT NULL UNIQUE,
 captured_ts TEXT NOT NULL,
 venue TEXT NOT NULL CHECK(venue IN ('kalshi','polyus')),
 ticker TEXT NOT NULL,
 title TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('YES','NO')),
 action TEXT NOT NULL CHECK(action IN ('BUY','SELL')),
 system_id TEXT NOT NULL,
 route TEXT NOT NULL CHECK(route IN ('maker','taker')),
 requested_qty REAL NOT NULL CHECK(requested_qty>0),
 limit_price REAL NOT NULL CHECK(limit_price>0 AND limit_price<1),
 touch_depth REAL CHECK(touch_depth IS NULL OR touch_depth>=0),
 tick_size REAL NOT NULL CHECK(tick_size>0 AND tick_size<=1),
 book_source TEXT NOT NULL,
 quoted_fee REAL,
 quote_fee_source TEXT NOT NULL DEFAULT '',
 model_version TEXT NOT NULL,

 state TEXT NOT NULL CHECK(state IN ('resting','filled','cancelled','settled')),
 filled_qty REAL NOT NULL DEFAULT 0 CHECK(filled_qty>=0 AND filled_qty<=requested_qty),
 fill_price REAL CHECK(fill_price IS NULL OR (fill_price>0 AND fill_price<1)),
 fill_fee REAL,
 fill_fee_source TEXT NOT NULL DEFAULT '',
 fill_ts TEXT NOT NULL DEFAULT '',
 fill_rule TEXT NOT NULL DEFAULT '',

 queue_ahead REAL CHECK(queue_ahead IS NULL OR queue_ahead>=0),
 queue_left REAL CHECK(queue_left IS NULL OR queue_left>=0),
 queue_filled REAL CHECK(queue_filled IS NULL OR queue_filled>=0),
 tape_cutoff_ts TEXT NOT NULL DEFAULT '',

 cancel_ts TEXT NOT NULL DEFAULT '',
 cancel_rule TEXT NOT NULL DEFAULT '',

 settle_yes_value REAL CHECK(settle_yes_value IS NULL OR
                              (settle_yes_value>=0 AND settle_yes_value<=1)),
 settled_ts TEXT NOT NULL DEFAULT '',
 settlement_source TEXT NOT NULL DEFAULT '',
 settlement_hash TEXT NOT NULL DEFAULT '',
 realized_net REAL,
 updated_ts TEXT NOT NULL,

 FOREIGN KEY(reservation_id) REFERENCES live_pending_risk_intents(reservation_id),
 CHECK((quoted_fee IS NULL AND quote_fee_source='') OR
       (quoted_fee IS NOT NULL AND quote_fee_source<>'')),
 CHECK((fill_fee IS NULL AND fill_fee_source='') OR
       (fill_fee IS NOT NULL AND fill_fee_source<>'')),
 CHECK((route='maker' AND state IN ('resting','filled','cancelled','settled')) OR
       (route='taker' AND state IN ('filled','settled'))),
 CHECK((state='resting' AND filled_qty=0 AND fill_price IS NULL AND fill_ts='' AND
        cancel_ts='' AND settled_ts='' AND settle_yes_value IS NULL AND realized_net IS NULL) OR
       (state='filled' AND filled_qty>0 AND fill_price IS NOT NULL AND fill_ts<>'' AND
        cancel_ts='' AND settled_ts='' AND settle_yes_value IS NULL AND realized_net IS NULL) OR
       (state='cancelled' AND filled_qty=0 AND fill_price IS NULL AND fill_ts='' AND
        cancel_ts<>'' AND cancel_rule<>'' AND settled_ts='' AND settle_yes_value IS NULL AND
        realized_net IS NULL) OR
       (state='settled' AND filled_qty>0 AND fill_price IS NOT NULL AND fill_ts<>'' AND
        cancel_ts='' AND settled_ts<>'' AND settlement_source<>'' AND settlement_hash<>'' AND
        settle_yes_value IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_live_dispatch_shadow_state
 ON live_dispatch_shadow(state,captured_ts,id);
CREATE INDEX IF NOT EXISTS idx_live_dispatch_shadow_ticker
 ON live_dispatch_shadow(venue,ticker,captured_ts,id);

CREATE TRIGGER IF NOT EXISTS live_dispatch_shadow_immutable_dispatch
 BEFORE UPDATE ON live_dispatch_shadow
 WHEN NEW.reservation_id IS NOT OLD.reservation_id OR
      NEW.client_order_id IS NOT OLD.client_order_id OR
      NEW.captured_ts IS NOT OLD.captured_ts OR NEW.venue IS NOT OLD.venue OR
      NEW.ticker IS NOT OLD.ticker OR NEW.title IS NOT OLD.title OR
      NEW.side IS NOT OLD.side OR NEW.action IS NOT OLD.action OR
      NEW.system_id IS NOT OLD.system_id OR NEW.route IS NOT OLD.route OR
      NEW.requested_qty IS NOT OLD.requested_qty OR NEW.limit_price IS NOT OLD.limit_price OR
      NEW.touch_depth IS NOT OLD.touch_depth OR NEW.tick_size IS NOT OLD.tick_size OR
      NEW.book_source IS NOT OLD.book_source OR NEW.quoted_fee IS NOT OLD.quoted_fee OR
      NEW.quote_fee_source IS NOT OLD.quote_fee_source OR
      NEW.model_version IS NOT OLD.model_version
 BEGIN SELECT RAISE(ABORT,'immutable live-dispatch shadow capture'); END;

CREATE TRIGGER IF NOT EXISTS live_dispatch_shadow_state_machine
 BEFORE UPDATE ON live_dispatch_shadow
 WHEN NOT (
   (OLD.state='resting' AND NEW.state IN ('resting','filled','cancelled')) OR
   (OLD.state='filled' AND NEW.state IN ('filled','settled')) OR
   (OLD.state='cancelled' AND NEW.state='cancelled') OR
   (OLD.state='settled' AND NEW.state='settled')
 )
 BEGIN SELECT RAISE(ABORT,'invalid live-dispatch shadow transition'); END;

CREATE TRIGGER IF NOT EXISTS live_dispatch_shadow_no_delete
 BEFORE DELETE ON live_dispatch_shadow
 BEGIN SELECT RAISE(ABORT,'durable live-dispatch shadow cannot be deleted'); END;
`

func migrateLiveDispatchShadowSchema(db *sql.DB) error {
	_, err := db.Exec(liveDispatchShadowSchema)
	return err
}

const (
	LiveDispatchShadowResting   = "resting"
	LiveDispatchShadowFilled    = "filled"
	LiveDispatchShadowCancelled = "cancelled"
	LiveDispatchShadowSettled   = "settled"
)

// LiveDispatchShadow is one Paper-method counterfactual captured from a real-money dispatch.
// Optional numeric facts have an explicit Known flag so zero is never silently substituted for
// missing depth, fee, queue, or realized-profit evidence.
type LiveDispatchShadow struct {
	ID int64

	ReservationID, ClientOrderID string
	Captured                     time.Time
	Venue, Ticker, Title         string
	Side, Action                 string
	SystemID, Route              string
	RequestedQty, LimitPrice     float64
	TouchDepth                   float64
	DepthKnown                   bool
	TickSize                     float64
	BookSource                   string
	QuotedFee                    float64
	QuoteFeeKnown                bool
	QuoteFeeSource, ModelVersion string

	State                         string
	FilledQty, FillPrice, FillFee float64
	FillFeeKnown                  bool
	FillFeeSource                 string
	FillAt                        time.Time
	FillRule                      string
	QueueAhead, QueueLeft         float64
	QueueFilled                   float64
	QueueKnown                    bool
	TapeCutoff                    time.Time
	CancelAt                      time.Time
	CancelRule                    string
	SettlementValue               float64
	SettledAt                     time.Time
	SettlementSource              string
	SettlementHash                string
	RealizedNet                   float64
	RealizedNetKnown              bool
	UpdatedAt                     time.Time
}

const liveDispatchShadowColumns = `id,reservation_id,client_order_id,captured_ts,venue,ticker,title,
side,action,system_id,route,requested_qty,limit_price,touch_depth,tick_size,book_source,quoted_fee,
quote_fee_source,model_version,state,filled_qty,fill_price,fill_fee,fill_fee_source,fill_ts,fill_rule,
queue_ahead,queue_left,queue_filled,tape_cutoff_ts,cancel_ts,cancel_rule,settle_yes_value,settled_ts,
settlement_source,settlement_hash,realized_net,updated_ts`

type liveDispatchShadowScanner interface {
	Scan(...any) error
}

func parseLiveDispatchShadowTime(raw, field string, required bool) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		if required {
			return time.Time{}, fmt.Errorf("stored live-dispatch shadow %s is empty", field)
		}
		return time.Time{}, nil
	}
	v, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("stored live-dispatch shadow %s is invalid: %w", field, err)
	}
	return v, nil
}

func scanLiveDispatchShadow(row liveDispatchShadowScanner) (LiveDispatchShadow, error) {
	var out LiveDispatchShadow
	var captured, fillAt, tapeCutoff, cancelAt, settledAt, updated string
	var depth, quotedFee, fillPrice, fillFee, queueAhead, queueLeft, queueFilled sql.NullFloat64
	var settlementValue, realizedNet sql.NullFloat64
	err := row.Scan(&out.ID, &out.ReservationID, &out.ClientOrderID, &captured, &out.Venue,
		&out.Ticker, &out.Title, &out.Side, &out.Action, &out.SystemID, &out.Route,
		&out.RequestedQty, &out.LimitPrice, &depth, &out.TickSize, &out.BookSource, &quotedFee,
		&out.QuoteFeeSource, &out.ModelVersion, &out.State, &out.FilledQty, &fillPrice, &fillFee,
		&out.FillFeeSource, &fillAt, &out.FillRule, &queueAhead, &queueLeft, &queueFilled,
		&tapeCutoff, &cancelAt, &out.CancelRule, &settlementValue, &settledAt,
		&out.SettlementSource, &out.SettlementHash, &realizedNet, &updated)
	if err != nil {
		return out, err
	}
	if out.Captured, err = parseLiveDispatchShadowTime(captured, "captured_ts", true); err != nil {
		return out, err
	}
	if out.FillAt, err = parseLiveDispatchShadowTime(fillAt, "fill_ts", false); err != nil {
		return out, err
	}
	if out.TapeCutoff, err = parseLiveDispatchShadowTime(tapeCutoff, "tape_cutoff_ts", false); err != nil {
		return out, err
	}
	if out.CancelAt, err = parseLiveDispatchShadowTime(cancelAt, "cancel_ts", false); err != nil {
		return out, err
	}
	if out.SettledAt, err = parseLiveDispatchShadowTime(settledAt, "settled_ts", false); err != nil {
		return out, err
	}
	if out.UpdatedAt, err = parseLiveDispatchShadowTime(updated, "updated_ts", true); err != nil {
		return out, err
	}
	out.DepthKnown, out.TouchDepth = depth.Valid, depth.Float64
	out.QuoteFeeKnown, out.QuotedFee = quotedFee.Valid, quotedFee.Float64
	out.FillPrice = fillPrice.Float64
	out.FillFeeKnown, out.FillFee = fillFee.Valid, fillFee.Float64
	out.QueueKnown = queueAhead.Valid && queueLeft.Valid && queueFilled.Valid
	out.QueueAhead, out.QueueLeft, out.QueueFilled = queueAhead.Float64, queueLeft.Float64, queueFilled.Float64
	out.SettlementValue = settlementValue.Float64
	out.RealizedNetKnown, out.RealizedNet = realizedNet.Valid, realizedNet.Float64
	return out, nil
}

func normalizeLiveDispatchShadow(in LiveDispatchShadow) (LiveDispatchShadow, error) {
	in.ReservationID = strings.TrimSpace(in.ReservationID)
	in.ClientOrderID = strings.TrimSpace(in.ClientOrderID)
	in.Venue = strings.ToLower(strings.TrimSpace(in.Venue))
	in.Ticker = strings.TrimSpace(in.Ticker)
	in.Title = strings.TrimSpace(in.Title)
	in.Side = strings.ToUpper(strings.TrimSpace(in.Side))
	in.Action = strings.ToUpper(strings.TrimSpace(in.Action))
	in.SystemID = strings.TrimSpace(in.SystemID)
	in.Route = strings.ToLower(strings.TrimSpace(in.Route))
	in.BookSource = strings.TrimSpace(in.BookSource)
	in.QuoteFeeSource = strings.TrimSpace(in.QuoteFeeSource)
	in.ModelVersion = strings.TrimSpace(in.ModelVersion)
	in.State = strings.ToLower(strings.TrimSpace(in.State))
	in.FillFeeSource = strings.TrimSpace(in.FillFeeSource)
	in.FillRule = strings.TrimSpace(in.FillRule)
	in.CancelRule = strings.TrimSpace(in.CancelRule)
	in.SettlementSource = strings.TrimSpace(in.SettlementSource)
	in.SettlementHash = strings.TrimSpace(in.SettlementHash)
	in.Captured = in.Captured.UTC()
	in.FillAt = in.FillAt.UTC()
	in.TapeCutoff = in.TapeCutoff.UTC()
	in.CancelAt = in.CancelAt.UTC()
	in.SettledAt = in.SettledAt.UTC()

	if in.ReservationID == "" || in.ClientOrderID == "" || in.Captured.IsZero() ||
		(in.Venue != "kalshi" && in.Venue != "polyus") || in.Ticker == "" ||
		(in.Side != "YES" && in.Side != "NO") || (in.Action != "BUY" && in.Action != "SELL") ||
		in.SystemID == "" || (in.Route != "maker" && in.Route != "taker") ||
		!finite(in.RequestedQty) || in.RequestedQty <= 0 || !finite(in.LimitPrice) ||
		in.LimitPrice <= 0 || in.LimitPrice >= 1 || !finite(in.TickSize) || in.TickSize <= 0 ||
		in.TickSize > 1 || in.BookSource == "" || in.ModelVersion == "" {
		return in, errors.New("invalid live-dispatch shadow identity or execution capture")
	}
	if in.DepthKnown {
		if !finite(in.TouchDepth) || in.TouchDepth < 0 {
			return in, errors.New("invalid live-dispatch shadow touch depth")
		}
	} else {
		in.TouchDepth = 0
	}
	if in.QuoteFeeKnown {
		if !finite(in.QuotedFee) || in.QuoteFeeSource == "" {
			return in, errors.New("invalid live-dispatch shadow quoted fee")
		}
	} else if in.QuotedFee != 0 || in.QuoteFeeSource != "" {
		return in, errors.New("unknown live-dispatch shadow quoted fee carries a value or source")
	}
	if in.FillFeeKnown {
		if !finite(in.FillFee) || in.FillFeeSource == "" {
			return in, errors.New("invalid live-dispatch shadow fill fee")
		}
	} else if in.FillFee != 0 || in.FillFeeSource != "" {
		return in, errors.New("unknown live-dispatch shadow fill fee carries a value or source")
	}
	if in.QueueKnown {
		if !finite(in.QueueAhead) || !finite(in.QueueLeft) || !finite(in.QueueFilled) ||
			in.QueueAhead < 0 || in.QueueLeft < 0 || in.QueueFilled < 0 ||
			in.QueueLeft > in.QueueAhead+1e-9 || in.QueueFilled > in.RequestedQty+1e-9 ||
			in.TapeCutoff.IsZero() {
			return in, errors.New("invalid live-dispatch shadow queue capture")
		}
	} else {
		in.QueueAhead, in.QueueLeft, in.QueueFilled = 0, 0, 0
		in.TapeCutoff = time.Time{}
	}

	switch in.Route {
	case "maker":
		if in.State != LiveDispatchShadowResting || in.FilledQty != 0 || in.FillPrice != 0 ||
			in.FillFeeKnown ||
			!in.FillAt.IsZero() || in.FillRule != "" || !in.CancelAt.IsZero() ||
			in.CancelRule != "" || !in.SettledAt.IsZero() || in.SettlementSource != "" ||
			in.SettlementHash != "" || in.SettlementValue != 0 || in.RealizedNetKnown ||
			in.RealizedNet != 0 {
			return in, errors.New("new maker live-dispatch shadow must be resting and unfilled")
		}
	case "taker":
		if in.State != LiveDispatchShadowFilled || !finite(in.FilledQty) || in.FilledQty <= 0 ||
			in.FilledQty > in.RequestedQty+1e-9 || !finite(in.FillPrice) || in.FillPrice <= 0 ||
			in.FillPrice >= 1 || !in.FillFeeKnown || in.FillAt.IsZero() || in.FillRule == "" ||
			in.QueueKnown || !in.CancelAt.IsZero() || in.CancelRule != "" ||
			!in.SettledAt.IsZero() || in.SettlementSource != "" || in.SettlementHash != "" ||
			in.SettlementValue != 0 || in.RealizedNetKnown || in.RealizedNet != 0 {
			return in, errors.New("new taker live-dispatch shadow lacks a complete simulated fill")
		}
	}
	in.ID = 0
	in.UpdatedAt = time.Time{}
	return in, nil
}

func optionalLiveDispatchFloat(known bool, value float64) any {
	if !known {
		return nil
	}
	return value
}

func optionalLiveDispatchTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func sameLiveDispatchFloat(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9
}

func sameLiveDispatchShadow(a, b LiveDispatchShadow) bool {
	return a.ReservationID == b.ReservationID && a.ClientOrderID == b.ClientOrderID &&
		a.Captured.Equal(b.Captured) && a.Venue == b.Venue && a.Ticker == b.Ticker &&
		a.Title == b.Title && a.Side == b.Side && a.Action == b.Action &&
		a.SystemID == b.SystemID && a.Route == b.Route &&
		sameLiveDispatchFloat(a.RequestedQty, b.RequestedQty) &&
		sameLiveDispatchFloat(a.LimitPrice, b.LimitPrice) &&
		a.DepthKnown == b.DepthKnown && (!a.DepthKnown || sameLiveDispatchFloat(a.TouchDepth, b.TouchDepth)) &&
		sameLiveDispatchFloat(a.TickSize, b.TickSize) && a.BookSource == b.BookSource &&
		a.QuoteFeeKnown == b.QuoteFeeKnown &&
		(!a.QuoteFeeKnown || sameLiveDispatchFloat(a.QuotedFee, b.QuotedFee)) &&
		a.QuoteFeeSource == b.QuoteFeeSource && a.ModelVersion == b.ModelVersion &&
		a.State == b.State && sameLiveDispatchFloat(a.FilledQty, b.FilledQty) &&
		sameLiveDispatchFloat(a.FillPrice, b.FillPrice) && a.FillFeeKnown == b.FillFeeKnown &&
		(!a.FillFeeKnown || sameLiveDispatchFloat(a.FillFee, b.FillFee)) &&
		a.FillFeeSource == b.FillFeeSource && a.FillAt.Equal(b.FillAt) &&
		a.FillRule == b.FillRule && a.QueueKnown == b.QueueKnown &&
		(!a.QueueKnown || (sameLiveDispatchFloat(a.QueueAhead, b.QueueAhead) &&
			sameLiveDispatchFloat(a.QueueLeft, b.QueueLeft) &&
			sameLiveDispatchFloat(a.QueueFilled, b.QueueFilled) &&
			a.TapeCutoff.Equal(b.TapeCutoff))) &&
		a.CancelAt.Equal(b.CancelAt) && a.CancelRule == b.CancelRule &&
		a.SettledAt.Equal(b.SettledAt) && a.SettlementSource == b.SettlementSource &&
		a.SettlementHash == b.SettlementHash && a.RealizedNetKnown == b.RealizedNetKnown &&
		(!a.RealizedNetKnown || sameLiveDispatchFloat(a.RealizedNet, b.RealizedNet))
}

func loadLiveDispatchShadowWhere(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, where string, args ...any) (LiveDispatchShadow, bool, error) {
	row, err := scanLiveDispatchShadow(q.QueryRowContext(ctx,
		`SELECT `+liveDispatchShadowColumns+` FROM live_dispatch_shadow WHERE `+where, args...))
	if errors.Is(err, sql.ErrNoRows) {
		return LiveDispatchShadow{}, false, nil
	}
	return row, err == nil, err
}

// InsertLiveDispatchShadow atomically captures the counterfactual before the real venue mutation.
// An exact retry returns the original id with inserted=false; reusing either durable key for
// different economics is rejected.
func (s *Store) InsertLiveDispatchShadow(ctx context.Context, in LiveDispatchShadow) (int64, bool, error) {
	in, err := normalizeLiveDispatchShadow(in)
	if err != nil {
		return 0, false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, err
	}
	defer tx.Rollback()

	// The shadow may only describe the exact execution leg whose durable reservation precedes it.
	// This binds both idempotency keys to the same immutable ticker, side, action, quantity, and
	// limit instead of treating the reservation foreign key as a loose label.
	legRows, err := tx.QueryContext(ctx, `SELECT venue,ticker,side,action,quantity,limit_price
FROM live_pending_risk_legs WHERE reservation_id=? AND client_order_id=? ORDER BY leg_index`,
		in.ReservationID, in.ClientOrderID)
	if err != nil {
		return 0, false, err
	}
	var legCount int
	var legVenue, legTicker, legSide, legAction string
	var legQty, legLimit float64
	for legRows.Next() {
		legCount++
		if err = legRows.Scan(&legVenue, &legTicker, &legSide, &legAction, &legQty, &legLimit); err != nil {
			legRows.Close()
			return 0, false, err
		}
	}
	if err = legRows.Close(); err != nil {
		return 0, false, err
	}
	if err = legRows.Err(); err != nil {
		return 0, false, err
	}
	if legCount != 1 || legVenue != in.Venue || legTicker != in.Ticker ||
		legSide != in.Side || legAction != in.Action ||
		!sameLiveDispatchFloat(legQty, in.RequestedQty) ||
		!sameLiveDispatchFloat(legLimit, in.LimitPrice) {
		return 0, false, errors.New("live-dispatch shadow does not match its immutable pending-risk leg")
	}

	rows, err := tx.QueryContext(ctx, `SELECT `+liveDispatchShadowColumns+`
FROM live_dispatch_shadow WHERE reservation_id=? OR client_order_id=? ORDER BY id`,
		in.ReservationID, in.ClientOrderID)
	if err != nil {
		return 0, false, err
	}
	var existing []LiveDispatchShadow
	for rows.Next() {
		v, scanErr := scanLiveDispatchShadow(rows)
		if scanErr != nil {
			rows.Close()
			return 0, false, scanErr
		}
		existing = append(existing, v)
	}
	if err = rows.Close(); err != nil {
		return 0, false, err
	}
	if err = rows.Err(); err != nil {
		return 0, false, err
	}
	if len(existing) > 0 {
		if len(existing) != 1 || !sameLiveDispatchShadow(existing[0], in) {
			return 0, false, errors.New("live-dispatch shadow key reused with different capture")
		}
		if err = tx.Commit(); err != nil {
			return 0, false, err
		}
		return existing[0].ID, false, nil
	}

	now := time.Now().UTC()
	res, err := tx.ExecContext(ctx, `INSERT INTO live_dispatch_shadow(
reservation_id,client_order_id,captured_ts,venue,ticker,title,side,action,system_id,route,
requested_qty,limit_price,touch_depth,tick_size,book_source,quoted_fee,quote_fee_source,model_version,
state,filled_qty,fill_price,fill_fee,fill_fee_source,fill_ts,fill_rule,queue_ahead,queue_left,
queue_filled,tape_cutoff_ts,cancel_ts,cancel_rule,settle_yes_value,settled_ts,settlement_source,
settlement_hash,realized_net,updated_ts) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
?,?,?,?,?,?,?,?,?,?,?,?)`,
		in.ReservationID, in.ClientOrderID, in.Captured.Format(time.RFC3339Nano), in.Venue,
		in.Ticker, in.Title, in.Side, in.Action, in.SystemID, in.Route, in.RequestedQty,
		in.LimitPrice, optionalLiveDispatchFloat(in.DepthKnown, in.TouchDepth), in.TickSize,
		in.BookSource, optionalLiveDispatchFloat(in.QuoteFeeKnown, in.QuotedFee),
		in.QuoteFeeSource, in.ModelVersion, in.State, in.FilledQty,
		optionalLiveDispatchFloat(in.State == LiveDispatchShadowFilled, in.FillPrice),
		optionalLiveDispatchFloat(in.FillFeeKnown, in.FillFee), in.FillFeeSource,
		optionalLiveDispatchTime(in.FillAt), in.FillRule,
		optionalLiveDispatchFloat(in.QueueKnown, in.QueueAhead),
		optionalLiveDispatchFloat(in.QueueKnown, in.QueueLeft),
		optionalLiveDispatchFloat(in.QueueKnown, in.QueueFilled),
		optionalLiveDispatchTime(in.TapeCutoff), "", "", nil, "", "", "", nil,
		now.Format(time.RFC3339Nano))
	if err != nil {
		return 0, false, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, false, err
	}
	if err = tx.Commit(); err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// UpdateLiveDispatchShadowMakerProgress persists queue/tape progress monotonically. It never marks
// a fill; callers first persist the evidence here, then call FillLiveDispatchShadow.
func (s *Store) UpdateLiveDispatchShadowMakerProgress(ctx context.Context, id int64,
	queueLeft, queueFilled float64, tapeCutoff time.Time) (bool, error) {
	if id <= 0 || !finite(queueLeft) || !finite(queueFilled) || queueLeft < 0 ||
		queueFilled < 0 || tapeCutoff.IsZero() {
		return false, errors.New("invalid live-dispatch shadow maker progress")
	}
	tapeCutoff = tapeCutoff.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	prior, found, err := loadLiveDispatchShadowWhere(ctx, tx, "id=?", id)
	if err != nil {
		return false, err
	}
	if !found {
		return false, sql.ErrNoRows
	}
	if prior.Route != "maker" || prior.State != LiveDispatchShadowResting || !prior.QueueKnown {
		return false, errors.New("live-dispatch shadow maker progress requires a queue-known resting maker")
	}
	if queueLeft > prior.QueueLeft+1e-9 || queueLeft > prior.QueueAhead+1e-9 ||
		queueFilled+1e-9 < prior.QueueFilled || queueFilled > prior.RequestedQty+1e-9 ||
		tapeCutoff.Before(prior.TapeCutoff) {
		return false, errors.New("live-dispatch shadow maker progress moved backwards")
	}
	if sameLiveDispatchFloat(queueLeft, prior.QueueLeft) &&
		sameLiveDispatchFloat(queueFilled, prior.QueueFilled) &&
		tapeCutoff.Equal(prior.TapeCutoff) {
		return false, tx.Commit()
	}
	res, err := tx.ExecContext(ctx, `UPDATE live_dispatch_shadow
SET queue_left=?,queue_filled=?,tape_cutoff_ts=?,updated_ts=? WHERE id=? AND state='resting'`,
		queueLeft, queueFilled, tapeCutoff.Format(time.RFC3339Nano),
		time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		if err == nil {
			err = errors.New("live-dispatch shadow maker progress lost its resting row")
		}
		return false, err
	}
	return true, tx.Commit()
}

func equalLiveDispatchFill(prior LiveDispatchShadow, qty, price, fee float64, feeKnown bool,
	feeSource string, fillAt time.Time, fillRule string) bool {
	return sameLiveDispatchFloat(prior.FilledQty, qty) &&
		sameLiveDispatchFloat(prior.FillPrice, price) && prior.FillFeeKnown == feeKnown &&
		(!feeKnown || sameLiveDispatchFloat(prior.FillFee, fee)) &&
		prior.FillFeeSource == feeSource && prior.FillAt.Equal(fillAt) &&
		prior.FillRule == fillRule
}

// FillLiveDispatchShadow converts a resting maker shadow into its final Paper-method fill. Partial
// queue fills are valid final positions when the remainder would have been cancelled.
func (s *Store) FillLiveDispatchShadow(ctx context.Context, id int64, filledQty, fillPrice,
	fillFee float64, feeKnown bool, feeSource string, fillAt time.Time, fillRule string) (bool, error) {
	feeSource, fillRule = strings.TrimSpace(feeSource), strings.TrimSpace(fillRule)
	if id <= 0 || !finite(filledQty) || filledQty <= 0 || !finite(fillPrice) ||
		fillPrice <= 0 || fillPrice >= 1 || fillAt.IsZero() || fillRule == "" ||
		(feeKnown && (!finite(fillFee) || feeSource == "")) ||
		(!feeKnown && (fillFee != 0 || feeSource != "")) {
		return false, errors.New("invalid live-dispatch shadow fill")
	}
	fillAt = fillAt.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	prior, found, err := loadLiveDispatchShadowWhere(ctx, tx, "id=?", id)
	if err != nil {
		return false, err
	}
	if !found {
		return false, sql.ErrNoRows
	}
	if (prior.State == LiveDispatchShadowFilled || prior.State == LiveDispatchShadowSettled) &&
		equalLiveDispatchFill(prior, filledQty, fillPrice, fillFee, feeKnown, feeSource, fillAt, fillRule) {
		return false, tx.Commit()
	}
	if prior.Route != "maker" || prior.State != LiveDispatchShadowResting {
		return false, errors.New("live-dispatch shadow fill requires a resting maker")
	}
	if filledQty > prior.RequestedQty+1e-9 ||
		(prior.QueueKnown && filledQty > prior.QueueFilled+1e-9) {
		return false, errors.New("live-dispatch shadow fill exceeds captured size or queue evidence")
	}
	res, err := tx.ExecContext(ctx, `UPDATE live_dispatch_shadow SET state='filled',filled_qty=?,
fill_price=?,fill_fee=?,fill_fee_source=?,fill_ts=?,fill_rule=?,updated_ts=?
WHERE id=? AND state='resting'`, filledQty, fillPrice,
		optionalLiveDispatchFloat(feeKnown, fillFee), feeSource, fillAt.Format(time.RFC3339Nano),
		fillRule, time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		if err == nil {
			err = errors.New("live-dispatch shadow fill lost its resting row")
		}
		return false, err
	}
	return true, tx.Commit()
}

// CancelLiveDispatchShadow terminates an unfilled maker shadow. A filled or settled shadow can
// never be rewritten as a non-fill.
func (s *Store) CancelLiveDispatchShadow(ctx context.Context, id int64, cancelAt time.Time,
	cancelRule string) (bool, error) {
	cancelRule = strings.TrimSpace(cancelRule)
	if id <= 0 || cancelAt.IsZero() || cancelRule == "" {
		return false, errors.New("invalid live-dispatch shadow cancellation")
	}
	cancelAt = cancelAt.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	prior, found, err := loadLiveDispatchShadowWhere(ctx, tx, "id=?", id)
	if err != nil {
		return false, err
	}
	if !found {
		return false, sql.ErrNoRows
	}
	if prior.State == LiveDispatchShadowCancelled && prior.CancelAt.Equal(cancelAt) &&
		prior.CancelRule == cancelRule {
		return false, tx.Commit()
	}
	if prior.Route != "maker" || prior.State != LiveDispatchShadowResting ||
		(prior.QueueKnown && prior.QueueFilled >= 1) {
		return false, errors.New("live-dispatch shadow cancellation requires an unfilled resting maker")
	}
	res, err := tx.ExecContext(ctx, `UPDATE live_dispatch_shadow SET state='cancelled',
cancel_ts=?,cancel_rule=?,updated_ts=? WHERE id=? AND state='resting'`,
		cancelAt.Format(time.RFC3339Nano), cancelRule,
		time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		if err == nil {
			err = errors.New("live-dispatch shadow cancellation lost its resting row")
		}
		return false, err
	}
	return true, tx.Commit()
}

func liveDispatchShadowNet(row LiveDispatchShadow, fillFee float64) float64 {
	payout := row.SettlementValue
	if row.Side == "NO" {
		payout = 1 - payout
	}
	gross := row.FilledQty * (payout - row.FillPrice)
	if row.Action == "SELL" {
		gross = -gross
	}
	return gross - fillFee
}

// SetLiveDispatchShadowFillFee upgrades an unknown maker fee once exact route authority arrives.
// Existing exact fee truth is immutable. If settlement already happened, realized net is filled
// in atomically from the newly completed evidence.
func (s *Store) SetLiveDispatchShadowFillFee(ctx context.Context, id int64, fillFee float64,
	feeSource string) (bool, error) {
	feeSource = strings.TrimSpace(feeSource)
	if id <= 0 || !finite(fillFee) || feeSource == "" {
		return false, errors.New("invalid live-dispatch shadow exact fill fee")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	prior, found, err := loadLiveDispatchShadowWhere(ctx, tx, "id=?", id)
	if err != nil {
		return false, err
	}
	if !found {
		return false, sql.ErrNoRows
	}
	if prior.State != LiveDispatchShadowFilled && prior.State != LiveDispatchShadowSettled {
		return false, errors.New("live-dispatch shadow fee requires a filled position")
	}
	if prior.FillFeeKnown {
		if sameLiveDispatchFloat(prior.FillFee, fillFee) && prior.FillFeeSource == feeSource {
			return false, tx.Commit()
		}
		return false, errors.New("live-dispatch shadow exact fill fee cannot be rewritten")
	}
	var realized any
	if prior.State == LiveDispatchShadowSettled {
		realized = liveDispatchShadowNet(prior, fillFee)
	}
	res, err := tx.ExecContext(ctx, `UPDATE live_dispatch_shadow SET fill_fee=?,fill_fee_source=?,
realized_net=?,updated_ts=? WHERE id=? AND fill_fee IS NULL AND state IN ('filled','settled')`,
		fillFee, feeSource, realized, time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		if err == nil {
			err = errors.New("live-dispatch shadow exact fee lost its fill row")
		}
		return false, err
	}
	return true, tx.Commit()
}

// SettleLiveDispatchShadow applies the authoritative venue YES value. Missing fee authority does
// not fabricate profit: the row settles, but realized net remains explicitly unknown until the
// exact fee is supplied.
func (s *Store) SettleLiveDispatchShadow(ctx context.Context, id int64, yesValue float64,
	settledAt time.Time, source, sourceHash string) (bool, error) {
	source, sourceHash = strings.TrimSpace(source), strings.TrimSpace(sourceHash)
	if id <= 0 || !finite(yesValue) || yesValue < 0 || yesValue > 1 ||
		settledAt.IsZero() || source == "" || sourceHash == "" {
		return false, errors.New("invalid live-dispatch shadow settlement")
	}
	settledAt = settledAt.UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	prior, found, err := loadLiveDispatchShadowWhere(ctx, tx, "id=?", id)
	if err != nil {
		return false, err
	}
	if !found {
		return false, sql.ErrNoRows
	}
	if prior.State == LiveDispatchShadowSettled {
		if sameLiveDispatchFloat(prior.SettlementValue, yesValue) &&
			prior.SettledAt.Equal(settledAt) && prior.SettlementSource == source &&
			prior.SettlementHash == sourceHash {
			return false, tx.Commit()
		}
		return false, errors.New("live-dispatch shadow settlement cannot be rewritten")
	}
	if prior.State != LiveDispatchShadowFilled {
		return false, errors.New("live-dispatch shadow settlement requires a filled position")
	}
	prior.SettlementValue = yesValue
	var realized any
	if prior.FillFeeKnown {
		realized = liveDispatchShadowNet(prior, prior.FillFee)
	}
	res, err := tx.ExecContext(ctx, `UPDATE live_dispatch_shadow SET state='settled',
settle_yes_value=?,settled_ts=?,settlement_source=?,settlement_hash=?,realized_net=?,updated_ts=?
WHERE id=? AND state='filled'`, yesValue, settledAt.Format(time.RFC3339Nano), source, sourceHash,
		realized, time.Now().UTC().Format(time.RFC3339Nano), id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n != 1 {
		if err == nil {
			err = errors.New("live-dispatch shadow settlement lost its fill row")
		}
		return false, err
	}
	return true, tx.Commit()
}

func (s *Store) LiveDispatchShadowByID(ctx context.Context, id int64) (LiveDispatchShadow, bool, error) {
	if id <= 0 {
		return LiveDispatchShadow{}, false, nil
	}
	return loadLiveDispatchShadowWhere(ctx, s.db, "id=?", id)
}

func (s *Store) LiveDispatchShadowByReservation(ctx context.Context,
	reservationID string) (LiveDispatchShadow, bool, error) {
	reservationID = strings.TrimSpace(reservationID)
	if reservationID == "" {
		return LiveDispatchShadow{}, false, nil
	}
	return loadLiveDispatchShadowWhere(ctx, s.db, "reservation_id=?", reservationID)
}

func normalizeLiveDispatchListLimit(limit int) int {
	if limit <= 0 {
		return 200
	}
	if limit > 2000 {
		return 2000
	}
	return limit
}

func scanLiveDispatchShadowRows(rows *sql.Rows) ([]LiveDispatchShadow, error) {
	defer rows.Close()
	var out []LiveDispatchShadow
	for rows.Next() {
		v, err := scanLiveDispatchShadow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) ListLiveDispatchShadows(ctx context.Context, limit int) ([]LiveDispatchShadow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+liveDispatchShadowColumns+`
FROM live_dispatch_shadow ORDER BY captured_ts DESC,id DESC LIMIT ?`,
		normalizeLiveDispatchListLimit(limit))
	if err != nil {
		return nil, err
	}
	return scanLiveDispatchShadowRows(rows)
}

func (s *Store) ListOpenLiveDispatchShadows(ctx context.Context, limit int) ([]LiveDispatchShadow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+liveDispatchShadowColumns+`
FROM live_dispatch_shadow WHERE state IN ('resting','filled')
ORDER BY captured_ts,id LIMIT ?`, normalizeLiveDispatchListLimit(limit))
	if err != nil {
		return nil, err
	}
	return scanLiveDispatchShadowRows(rows)
}

func (s *Store) ListOpenLiveDispatchShadowsAfter(ctx context.Context, afterID int64,
	limit int) ([]LiveDispatchShadow, error) {
	if afterID < 0 {
		afterID = 0
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+liveDispatchShadowColumns+`
FROM live_dispatch_shadow WHERE state IN ('resting','filled') AND id>?
ORDER BY id LIMIT ?`, afterID, normalizeLiveDispatchListLimit(limit))
	if err != nil {
		return nil, err
	}
	return scanLiveDispatchShadowRows(rows)
}

func liveDispatchBatchValues(values []string, limit int) ([]string, string, []any) {
	if limit <= 0 {
		limit = 500
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
		if len(out) == limit {
			break
		}
	}
	args := make([]any, len(out))
	for i := range out {
		args[i] = out[i]
	}
	return out, strings.TrimSuffix(strings.Repeat("?,", len(out)), ","), args
}

// LivePendingRiskExecutionsByID is the shadow GET's read-only projection. It uses two autocommit
// batch queries rather than one _txlock=immediate transaction per row, and deliberately loads only
// immutable legs plus append-only execution events.
func (s *Store) LivePendingRiskExecutionsByID(ctx context.Context,
	ids []string) (map[string]LivePendingRiskReservation, error) {
	_, placeholders, args := liveDispatchBatchValues(ids, 500)
	out := make(map[string]LivePendingRiskReservation)
	if placeholders == "" {
		return out, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT reservation_id,leg_index,venue,ticker,side,action,
client_order_id,quantity,limit_price,baseline_position_qty,expected_position_qty,baseline_resting_risk_usd
FROM live_pending_risk_legs WHERE reservation_id IN (`+placeholders+`)
ORDER BY reservation_id,leg_index`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var leg LivePendingRiskLeg
		if err := rows.Scan(&leg.ReservationID, &leg.Index, &leg.Venue, &leg.Ticker, &leg.Side,
			&leg.Action, &leg.ClientOrderID, &leg.Quantity, &leg.LimitPrice,
			&leg.BaselinePositionQty, &leg.ExpectedPositionQty,
			&leg.BaselineRestingRiskUSD); err != nil {
			rows.Close()
			return nil, err
		}
		risk := out[leg.ReservationID]
		risk.Legs = append(risk.Legs, leg)
		out[leg.ReservationID] = risk
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}

	rows, err = s.db.QueryContext(ctx, `SELECT id,reservation_id,event_seq,observed_ts,event_type,
leg_index,attempt_key,action,client_order_id,order_id,filled_qty,average_price,fee_total,
account_observed_ts,receipt_source,reason,evidence_json
FROM live_pending_risk_events WHERE reservation_id IN (`+placeholders+`)
ORDER BY reservation_id,event_seq`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var event LivePendingRiskEvent
		var observed, account string
		var leg sql.NullInt64
		if err := rows.Scan(&event.ID, &event.ReservationID, &event.Sequence, &observed,
			&event.EventType, &leg, &event.AttemptKey, &event.Action, &event.ClientOrderID,
			&event.OrderID, &event.FilledQty, &event.AveragePrice, &event.FeeTotal, &account,
			&event.ReceiptSource, &event.Reason, &event.EvidenceJSON); err != nil {
			rows.Close()
			return nil, err
		}
		event.Observed, err = time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			rows.Close()
			return nil, errors.New("stored live pending-risk event timestamp is invalid")
		}
		if account != "" {
			event.AccountObserved, err = time.Parse(time.RFC3339Nano, account)
			if err != nil {
				rows.Close()
				return nil, errors.New("stored live pending-risk account timestamp is invalid")
			}
		}
		if leg.Valid {
			index := int(leg.Int64)
			event.LegIndex = &index
		}
		risk := out[event.ReservationID]
		risk.Events = append(risk.Events, event)
		out[event.ReservationID] = risk
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	return out, rows.Close()
}

// VenueSettlementsForTickers is a bounded venue-scoped batch lookup used by the shadow settlement
// lane. It preserves VenueSettlementForTicker's authority rules and hash shape without N reads.
func (s *Store) VenueSettlementsForTickers(ctx context.Context, platform string,
	tickers []string) (map[string]VenueSettlementReceipt, error) {
	platform = strings.ToLower(strings.TrimSpace(platform))
	_, placeholders, tickerArgs := liveDispatchBatchValues(tickers, 500)
	out := make(map[string]VenueSettlementReceipt)
	if platform == "" || placeholders == "" {
		if platform == "" {
			return nil, errors.New("empty venue settlement identity")
		}
		return out, nil
	}
	args := make([]any, 0, len(tickerArgs)+1)
	args = append(args, platform)
	args = append(args, tickerArgs...)
	rows, err := s.db.QueryContext(ctx, `SELECT ticker,yes_value,resolved_at,source_artifact
FROM venue_settlements WHERE platform=? AND ticker IN (`+placeholders+`)`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ticker, resolved, source string
		var value float64
		if err := rows.Scan(&ticker, &value, &resolved, &source); err != nil {
			rows.Close()
			return nil, err
		}
		resolvedAt, parseErr := time.Parse(time.RFC3339Nano, resolved)
		if parseErr != nil {
			resolvedAt, parseErr = time.Parse(time.RFC3339, resolved)
		}
		if parseErr != nil || !authoritativeVenueSettlementReceipt(platform, value, source) {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"platform": platform, "ticker": ticker,
			"settle_val": value, "resolved_at": resolvedAt.UTC().Format(time.RFC3339Nano),
			"source_artifact": source})
		hash := sha256.Sum256(payload)
		out[ticker] = VenueSettlementReceipt{Platform: platform, Ticker: ticker,
			ResolvedAt: resolvedAt, YesValue: value,
			SourceArtifact: "venue settlement receipt: " + source,
			Hash:           "sha256:" + hex.EncodeToString(hash[:])}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if platform == "polyus" {
		return out, nil
	}
	rows, err = s.db.QueryContext(ctx, `SELECT id,ticker,settle_val,resolved_at
FROM signal_log WHERE platform=? AND ticker IN (`+placeholders+`) AND resolved=1
 AND settle_val IS NOT NULL AND settle_val>=0 AND settle_val<=1
ORDER BY ticker,resolved_at DESC,id DESC`, args...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		var ticker, resolved string
		var value float64
		if err := rows.Scan(&id, &ticker, &value, &resolved); err != nil {
			rows.Close()
			return nil, err
		}
		if _, exists := out[ticker]; exists {
			continue
		}
		resolvedAt, parseErr := time.Parse(time.RFC3339Nano, resolved)
		if parseErr != nil {
			resolvedAt, parseErr = time.Parse(time.RFC3339, resolved)
		}
		if parseErr != nil || !authoritativeVenueSettlementValue(platform, value) {
			continue
		}
		payload, _ := json.Marshal(map[string]any{"platform": platform, "ticker": ticker,
			"settle_val": value, "resolved_at": resolvedAt.UTC().Format(time.RFC3339Nano),
			"signal_row_id": id})
		hash := sha256.Sum256(payload)
		out[ticker] = VenueSettlementReceipt{RowID: id, Platform: platform, Ticker: ticker,
			ResolvedAt: resolvedAt, YesValue: value,
			SourceArtifact: "canonical signal settlement: venue-scoped exact settle_val",
			Hash:           "sha256:" + hex.EncodeToString(hash[:])}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	return out, rows.Close()
}
