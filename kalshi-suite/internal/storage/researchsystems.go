package storage

// Durable, strictly research-only observations for queue-priority, incentive-maker,
// lifecycle-reopen, event-basket-lock, and nested-ladder-lock. These methods never touch paper fills, unit trials,
// allocation, arming, or any venue write path.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
)

type QueueResearchOrder struct {
	OrderID, Ticker, OutcomeSide, BookSide, Action string
	Price, Initial, Remaining, Filled              float64
	VenueCreated                                   string
}

type QueueResearchSample struct {
	Observed                          time.Time
	TrueQueue, Remaining              float64
	VisibleLevel, VisibleAhead, Error *float64
	BookAge                           *float64
}

func researchSlot(t time.Time, d time.Duration) string {
	if t.IsZero() {
		t = time.Now()
	}
	return t.UTC().Truncate(d).Format(time.RFC3339)
}

func (s *Store) InsertQueueResearchSample(ctx context.Context, o QueueResearchOrder, q QueueResearchSample) error {
	_, err := s.InsertQueueResearchSampleResult(ctx, o, q)
	return err
}

// InsertQueueResearchSampleResult reports whether this minute produced a new economic sample.
// The order row is still refreshed when the sample is a duplicate, so disappearance/outcome
// reconciliation remains correct while collector liveness can distinguish insert from dedupe.
func (s *Store) InsertQueueResearchSampleResult(ctx context.Context, o QueueResearchOrder, q QueueResearchSample) (bool, error) {
	if strings.TrimSpace(o.OrderID) == "" || strings.TrimSpace(o.Ticker) == "" || q.TrueQueue < 0 {
		return false, fmt.Errorf("invalid queue-priority observation")
	}
	if q.Observed.IsZero() {
		q.Observed = time.Now()
	}
	ts := q.Observed.UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO research_queue_orders(
order_id,ticker,outcome_side,book_side,action,price,initial_count,remaining_count,filled_count,
venue_created_ts,first_seen_ts,last_seen_ts,status)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'resting')
ON CONFLICT(order_id) DO UPDATE SET ticker=excluded.ticker,outcome_side=excluded.outcome_side,
book_side=excluded.book_side,action=excluded.action,price=excluded.price,
initial_count=excluded.initial_count,remaining_count=excluded.remaining_count,
filled_count=excluded.filled_count,last_seen_ts=excluded.last_seen_ts,status='resting'`,
		o.OrderID, o.Ticker, o.OutcomeSide, o.BookSide, o.Action, o.Price, o.Initial, o.Remaining,
		o.Filled, o.VenueCreated, ts, ts)
	if err != nil {
		return false, err
	}
	known := 0
	if q.VisibleAhead != nil {
		known = 1
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO research_queue_samples(
order_id,observed_ts,slot,true_queue_fp,visible_level_fp,visible_ahead_fp,estimate_error_fp,
estimate_known,book_age_s,remaining_count) VALUES(?,?,?,?,?,?,?,?,?,?)`,
		o.OrderID, ts, researchSlot(q.Observed, time.Minute), q.TrueQueue, q.VisibleLevel,
		q.VisibleAhead, q.Error, known, q.BookAge, q.Remaining)
	if err != nil {
		return false, err
	}
	inserted := false
	if n, _ := res.RowsAffected(); n > 0 {
		inserted = true
		if _, err = tx.ExecContext(ctx, `UPDATE research_queue_orders SET samples=samples+1 WHERE order_id=?`, o.OrderID); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return inserted, nil
}

type QueueResearchOpen struct {
	OrderID, Ticker string
}

func (s *Store) OpenQueueResearchOrders(ctx context.Context, limit int) ([]QueueResearchOpen, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT order_id,ticker FROM research_queue_orders
WHERE outcome='' ORDER BY last_seen_ts LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueResearchOpen
	for rows.Next() {
		var r QueueResearchOpen
		if err := rows.Scan(&r.OrderID, &r.Ticker); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) MarkQueueResearchOutcome(ctx context.Context, orderID, status string, filled float64) error {
	_, err := s.MarkQueueResearchOutcomeResult(ctx, orderID, status, filled)
	return err
}

func (s *Store) MarkQueueResearchOutcomeResult(ctx context.Context, orderID, status string, filled float64) (bool, error) {
	status = strings.ToLower(strings.TrimSpace(status))
	outcome := ""
	switch status {
	case "executed", "filled":
		outcome = "filled"
	case "canceled", "cancelled":
		outcome = "canceled"
		if filled > 0 {
			outcome = "partial_cancel"
		}
	default:
		return false, nil // unknown/intermediate schema: fail closed and retry later
	}
	res, err := s.db.ExecContext(ctx, `UPDATE research_queue_orders SET status=?,outcome=?,outcome_ts=?,
filled_count=? WHERE order_id=? AND outcome=''`, status, outcome, time.Now().UTC().Format(time.RFC3339Nano), filled, orderID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type IncentiveMakerObservation struct {
	Observed                                       time.Time
	ProgramID, MarketTicker, SeriesTicker, Side    string
	IncentiveType, Description, StartDate, EndDate string
	PeriodReward, DiscountBPS, TargetSize          float64
	Bid, Ask, BidDepth, AskDepth, MakerFee         float64
	Capacity, QuoteAge                             float64
	BookSource                                     string
}

func (s *Store) InsertIncentiveMaker(ctx context.Context, o IncentiveMakerObservation) error {
	if o.ProgramID == "" || o.MarketTicker == "" || (o.Side != "YES" && o.Side != "NO") ||
		o.Bid <= 0 || o.Ask <= o.Bid || o.Ask >= 1 || o.BidDepth < 1 || o.AskDepth < 1 || o.MakerFee < 0 {
		return fmt.Errorf("invalid incentive-maker observation")
	}
	if o.Observed.IsZero() {
		o.Observed = time.Now()
	}
	_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_incentive_maker(
observed_ts,slot,program_id,market_ticker,series_ticker,side,incentive_type,description,start_date,
end_date,period_reward_raw,discount_factor_bps,target_size_fp,bid_px,ask_px,bid_depth,ask_depth,
maker_fee_pc,capacity_contracts,competition_known,reward_included_in_ev,book_source,quote_age_s)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,0,?,?)`,
		o.Observed.UTC().Format(time.RFC3339Nano), researchSlot(o.Observed, time.Hour), o.ProgramID,
		o.MarketTicker, o.SeriesTicker, o.Side, o.IncentiveType, o.Description, o.StartDate, o.EndDate,
		o.PeriodReward, o.DiscountBPS, o.TargetSize, o.Bid, o.Ask, o.BidDepth, o.AskDepth,
		o.MakerFee, o.Capacity, o.BookSource, o.QuoteAge)
	return err
}

type LifecycleBook struct {
	Bid, Ask, BidDepth, AskDepth float64
	Source                       string
	Age                          float64
}

type LifecycleResearchEvent struct {
	Observed           time.Time
	Ticker, EventType  string
	CloseTime, RawHash string
	Book               LifecycleBook
}

// InsertLifecycleResearchEvent derives cohort membership from the prior authoritative lifecycle
// event for this ticker. An `activated` frame is a reopen sample only after `deactivated` or
// `close_date_updated`; ordinary initial activation never enters the cohort.
func (s *Store) InsertLifecycleResearchEvent(ctx context.Context, e LifecycleResearchEvent) (int64, bool, error) {
	e.EventType = strings.ToLower(strings.TrimSpace(e.EventType))
	if e.Ticker == "" || (e.EventType != "activated" && e.EventType != "deactivated" && e.EventType != "close_date_updated") {
		return 0, false, fmt.Errorf("invalid lifecycle-reopen event")
	}
	if e.Observed.IsZero() {
		e.Observed = time.Now()
	}
	var prior string
	var pbid, pask, pbidsz, pasksz sql.NullFloat64
	var psrc string
	err := s.db.QueryRowContext(ctx, `SELECT event_type,post0_bid,post0_ask,post0_bid_depth,
post0_ask_depth,post0_book_source FROM research_lifecycle_events WHERE ticker=?
ORDER BY id DESC LIMIT 1`, e.Ticker).Scan(&prior, &pbid, &pask, &pbidsz, &pasksz, &psrc)
	if err != nil && err != sql.ErrNoRows {
		return 0, false, err
	}
	cohort := e.EventType == "activated" && (prior == "deactivated" || prior == "close_date_updated")
	queueCtx := ""
	if cohort {
		var naturallyResting int
		_ = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_queue_orders WHERE ticker=? AND outcome=''`, e.Ticker).Scan(&naturallyResting)
		queueCtx = fmt.Sprintf("kalshi reactivation cancels all previously resting orders; queue restarts; naturally observed account orders before reconciliation=%d", naturallyResting)
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_lifecycle_events(
observed_ts,ticker,event_type,prior_event_type,close_time,raw_hash,cohort,
pre_bid,pre_ask,pre_bid_depth,pre_ask_depth,pre_book_source,
post0_bid,post0_ask,post0_bid_depth,post0_ask_depth,post0_book_source,queue_reset_context)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		e.Observed.UTC().Format(time.RFC3339Nano), e.Ticker, e.EventType, prior, e.CloseTime,
		e.RawHash, boolInt(cohort), nullFloat(pbid), nullFloat(pask), nullFloat(pbidsz), nullFloat(pasksz), psrc,
		nullResearchPrice(e.Book.Bid), nullResearchPrice(e.Book.Ask), nullResearchDepth(e.Book.BidDepth),
		nullResearchDepth(e.Book.AskDepth), e.Book.Source, queueCtx)
	if err != nil {
		return 0, false, err
	}
	if n, err := res.RowsAffected(); err != nil {
		return 0, false, err
	} else if n == 0 {
		return 0, cohort, nil
	}
	id, _ := res.LastInsertId()
	return id, cohort, nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func nullFloat(v sql.NullFloat64) any {
	if v.Valid {
		return v.Float64
	}
	return nil
}
func nullResearchPrice(v float64) any {
	if v > 0 && v < 1 {
		return v
	}
	return nil
}
func nullResearchDepth(v float64) any {
	if v >= 0 {
		return v
	}
	return nil
}

type LifecycleDue struct {
	ID       int64
	Ticker   string
	Observed time.Time
}

type LifecycleResearchMarker struct {
	Ticker, LastEvent, LastClose string
}

// LifecycleResearchMarkers reconstructs the last accepted transition and most recent known close
// time after a process restart. This avoids losing an activated-after-deactivated reopen merely
// because the in-memory WS filter restarted. The bounded reverse scan fails conservative if the
// historical table ever exceeds the filter's explicit memory ceiling.
func (s *Store) LifecycleResearchMarkers(ctx context.Context, limit int) ([]LifecycleResearchMarker, error) {
	if limit <= 0 || limit > 100_000 {
		limit = 100_000
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ticker,event_type,close_time FROM research_lifecycle_events
ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	byTicker := make(map[string]LifecycleResearchMarker)
	order := make([]string, 0)
	for rows.Next() {
		var ticker, eventType, closeTime string
		if err := rows.Scan(&ticker, &eventType, &closeTime); err != nil {
			return nil, err
		}
		m, seen := byTicker[ticker]
		if !seen {
			m.Ticker, m.LastEvent = ticker, eventType
			order = append(order, ticker)
		}
		if m.LastClose == "" && strings.TrimSpace(closeTime) != "" {
			m.LastClose = closeTime
		}
		byTicker[ticker] = m
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out := make([]LifecycleResearchMarker, 0, len(order))
	for _, ticker := range order {
		out = append(out, byTicker[ticker])
	}
	return out, nil
}

// LifecycleResearchDue returns one fair keyset page of incomplete cohorts. Captured and durably
// missed horizons both count as terminal, so an old unfillable row cannot sit at the head forever.
func (s *Store) LifecycleResearchDue(ctx context.Context, afterID int64, limit int) ([]LifecycleDue, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT e.id,e.ticker,e.observed_ts FROM research_lifecycle_events e
WHERE e.cohort=1 AND e.id>?
  AND ((SELECT COUNT(*) FROM research_lifecycle_horizons h WHERE h.event_id=e.id) +
       (SELECT COUNT(*) FROM research_lifecycle_horizon_misses m WHERE m.event_id=e.id)) < 4
ORDER BY e.id LIMIT ?`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []LifecycleDue
	for rows.Next() {
		var r LifecycleDue
		var ts string
		if err := rows.Scan(&r.ID, &r.Ticker, &ts); err != nil {
			return nil, err
		}
		r.Observed, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) HasLifecycleHorizon(ctx context.Context, eventID int64, horizon int) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT
 (SELECT COUNT(*) FROM research_lifecycle_horizons WHERE event_id=? AND horizon_s=?) +
 (SELECT COUNT(*) FROM research_lifecycle_horizon_misses WHERE event_id=? AND horizon_s=?)`,
		eventID, horizon, eventID, horizon).Scan(&n)
	return n > 0, err
}

func (s *Store) InsertLifecycleHorizonMiss(ctx context.Context, eventID int64, horizon int, reason string) error {
	_, err := s.InsertLifecycleHorizonMissResult(ctx, eventID, horizon, reason)
	return err
}

func (s *Store) InsertLifecycleHorizonMissResult(ctx context.Context, eventID int64, horizon int, reason string) (bool, error) {
	if eventID <= 0 || horizon <= 0 || strings.TrimSpace(reason) == "" {
		return false, fmt.Errorf("invalid lifecycle horizon miss")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_lifecycle_horizon_misses(
event_id,horizon_s,missed_ts,reason)
SELECT ?,?,?,? WHERE NOT EXISTS (
 SELECT 1 FROM research_lifecycle_horizons WHERE event_id=? AND horizon_s=?)`,
		eventID, horizon, nowRFC(), strings.TrimSpace(reason), eventID, horizon)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *Store) InsertLifecycleHorizon(ctx context.Context, eventID int64, horizon int, b LifecycleBook, fee float64) error {
	_, err := s.InsertLifecycleHorizonResult(ctx, eventID, horizon, b, fee)
	return err
}

func (s *Store) InsertLifecycleHorizonResult(ctx context.Context, eventID int64, horizon int, b LifecycleBook, fee float64) (bool, error) {
	if eventID <= 0 || horizon <= 0 || b.Bid <= 0 || b.Ask <= b.Bid || b.Ask >= 1 || b.BidDepth < 0 || b.AskDepth < 0 || fee < 0 {
		return false, fmt.Errorf("invalid lifecycle horizon")
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_lifecycle_horizons(
event_id,horizon_s,captured_ts,bid_px,ask_px,bid_depth,ask_depth,book_source,quote_age_s,taker_fee_pc)
SELECT ?,?,?,?,?,?,?,?,?,? WHERE NOT EXISTS (
 SELECT 1 FROM research_lifecycle_horizon_misses WHERE event_id=? AND horizon_s=?)`,
		eventID, horizon, time.Now().UTC().Format(time.RFC3339Nano), b.Bid, b.Ask,
		b.BidDepth, b.AskDepth, b.Source, b.Age, fee, eventID, horizon)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

type BasketLeg struct {
	Ticker string  `json:"ticker"`
	Side   string  `json:"side"`
	Ask    float64 `json:"ask"`
	Depth  float64 `json:"depth"`
	Fee    float64 `json:"fee"`
}

type EventBasketObservation struct {
	Observed                                        time.Time
	Venue, EventID, Title, Route, IdentitySource    string
	IdentityComplete, MutuallyExclusive, Exhaustive bool
	Legs                                            []BasketLeg
	PayoutLowerBound                                float64
}

type NestedLadderObservation struct {
	Observed                                     time.Time
	Venue, EventID, Title, LowTicker, HighTicker string
	IdentitySource                               string
	LowStrike, HighStrike                        float64
	LowBid, LowAsk, HighBid, HighAsk             float64
	LowAskDepth, HighNoAskDepth                  float64
	LowEntryFee, HighEntryFee                    float64
	LowExitFee, HighExitFee                      float64
}

// InsertNestedLadder persists the complete-fill payoff and the non-atomic execution risk side by
// side. A candidate means the two executable asks are fee-net below their $1 payout floor; it does
// not mean the venue can fill both atomically and never authorizes an order.
func (s *Store) InsertNestedLadder(ctx context.Context, o NestedLadderObservation) error {
	_, err := s.InsertNestedLadderResult(ctx, o)
	return err
}

// InsertNestedLadderResult preserves the legacy error-only API while exposing whether this exact
// cycle inserted a new row. Collector liveness must not label an INSERT OR IGNORE duplicate as new.
func (s *Store) InsertNestedLadderResult(ctx context.Context, o NestedLadderObservation) (bool, error) {
	if o.EventID == "" || o.LowTicker == "" || o.HighTicker == "" || o.IdentitySource == "" ||
		o.LowStrike >= o.HighStrike || o.LowBid <= 0 || o.LowAsk <= o.LowBid || o.LowAsk >= 1 ||
		o.HighBid <= 0 || o.HighAsk <= o.HighBid || o.HighAsk >= 1 || o.LowAskDepth < 1 ||
		o.HighNoAskDepth < 1 || o.LowEntryFee < 0 || o.HighEntryFee < 0 || o.LowExitFee < 0 || o.HighExitFee < 0 {
		return false, fmt.Errorf("invalid nested-ladder-lock observation")
	}
	if o.Observed.IsZero() {
		o.Observed = time.Now()
	}
	highNoAsk := 1 - o.HighBid
	cost := o.LowAsk + highNoAsk
	fees := o.LowEntryFee + o.HighEntryFee
	net := 1 - cost - fees
	capacity := math.Min(o.LowAskDepth, o.HighNoAskDepth)
	partialWorst := -math.Max(o.LowAsk+o.LowEntryFee, highNoAsk+o.HighEntryFee)
	// Cash reserve needed to cross out whichever single leg filled: the paid entry fee, touch
	// spread, and exact taker exit fee. It is telemetry, never a predicted execution cost.
	unwind := math.Max(o.LowEntryFee+(o.LowAsk-o.LowBid)+o.LowExitFee,
		o.HighEntryFee+(o.HighAsk-o.HighBid)+o.HighExitFee)
	violation := o.HighBid > o.LowAsk
	violationAmount := math.Max(0, o.HighBid-o.LowAsk)
	candidate := net > 0 && capacity >= 1
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_nested_ladders(
observed_ts,slot,venue,event_id,title,low_ticker,high_ticker,low_strike,high_strike,
low_yes_bid,low_yes_ask,high_yes_bid,high_yes_ask,low_ask_depth,high_no_ask_depth,
all_leg_cost,exact_entry_fee,payout_lower_bound,net_lock_if_both_fill,capacity_contracts,
partial_fill_worst_loss,unwind_buffer,quote_violation,violation_amount,candidate,atomic_fill,
funded,identity_source) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,0,0,?)`,
		o.Observed.UTC().Format(time.RFC3339Nano), researchSlot(o.Observed, 5*time.Minute), o.Venue,
		o.EventID, o.Title, o.LowTicker, o.HighTicker, o.LowStrike, o.HighStrike, o.LowBid, o.LowAsk,
		o.HighBid, o.HighAsk, o.LowAskDepth, o.HighNoAskDepth, cost, fees, net, capacity, partialWorst,
		unwind, boolInt(violation), violationAmount, boolInt(candidate), o.IdentitySource)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (s *Store) InsertEventBasket(ctx context.Context, o EventBasketObservation) error {
	_, err := s.InsertEventBasketResult(ctx, o)
	return err
}

// InsertEventBasketResult reports INSERT OR IGNORE truth for strict collector receipts.
func (s *Store) InsertEventBasketResult(ctx context.Context, o EventBasketObservation) (bool, error) {
	if o.Venue == "" || o.EventID == "" || len(o.Legs) < 2 || !o.IdentityComplete {
		return false, fmt.Errorf("invalid event-basket-lock identity")
	}
	if o.Observed.IsZero() {
		o.Observed = time.Now()
	}
	cost, fees, capacity := 0.0, 0.0, 0.0
	for i, l := range o.Legs {
		if l.Ticker == "" || l.Ask <= 0 || l.Ask >= 1 || l.Depth < 1 || l.Fee < 0 {
			return false, fmt.Errorf("invalid event-basket-lock leg")
		}
		cost += l.Ask
		fees += l.Fee
		if i == 0 || l.Depth < capacity {
			capacity = l.Depth
		}
	}
	net := o.PayoutLowerBound - cost - fees
	// Worst case after a partial all-or-none failure is that every acquired leg loses: capital
	// already paid plus its exact fees. This is exposure telemetry, never a synthetic fill model.
	partialWorst := -(cost + fees)
	// Route-specific proof: buy-YES-all needs exactly-one (exhaustive + mutually exclusive).
	// Buy-NO-all needs only at-most-one YES: with N legs its guaranteed payout is >=N-1 even
	// when zero outcomes settle YES. Complete official product identity is required for both.
	routeSafe := (o.Route == "buy-yes-all" && o.MutuallyExclusive && o.Exhaustive) ||
		(o.Route == "buy-no-all" && o.MutuallyExclusive)
	candidate := o.IdentityComplete && routeSafe && net > 0 && capacity >= 1
	b, err := json.Marshal(o.Legs)
	if err != nil {
		return false, err
	}
	res, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO research_event_baskets(
observed_ts,slot,venue,event_id,title,route,identity_source,identity_complete,mutually_exclusive,
exhaustive,leg_count,legs_json,all_leg_cost,exact_fee,payout_lower_bound,net_lock,
capacity_contracts,partial_fill_worst_loss,candidate) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		o.Observed.UTC().Format(time.RFC3339Nano), researchSlot(o.Observed, 5*time.Minute), o.Venue,
		o.EventID, o.Title, o.Route, o.IdentitySource, boolInt(o.IdentityComplete), boolInt(o.MutuallyExclusive),
		boolInt(o.Exhaustive), len(o.Legs), string(b), cost, fees, o.PayoutLowerBound, net, capacity,
		partialWorst, boolInt(candidate))
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

// ResearchSystemsReport is a compact read-only API payload. It reports prospective evidence and
// the intentional blind spots; it does not calculate a trade recommendation.
func (s *Store) ResearchSystemsReport(ctx context.Context) (map[string]any, error) {
	type queueSummary struct {
		Orders, Samples, Filled, Canceled, PartialCanceled, EstimateN int
		MAE                                                           float64
	}
	var q queueSummary
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(samples),0),
COALESCE(SUM(outcome='filled'),0),COALESCE(SUM(outcome='canceled'),0),
COALESCE(SUM(outcome='partial_cancel'),0) FROM research_queue_orders`).Scan(&q.Orders, &q.Samples, &q.Filled, &q.Canceled, &q.PartialCanceled); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(AVG(ABS(estimate_error_fp)),0) FROM research_queue_samples WHERE estimate_known=1`).Scan(&q.EstimateN, &q.MAE); err != nil {
		return nil, err
	}
	type simple struct {
		Observations, Candidates int
		BestNet, Capacity        float64
	}
	var incN, incPrograms int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COUNT(DISTINCT program_id) FROM research_incentive_maker`).Scan(&incN, &incPrograms); err != nil {
		return nil, err
	}
	var lifeRaw, lifeActivated, lifeDeactivated, lifeCloseChanges, lifeEpisodes, lifeHorizons, lifeMissed int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),
COALESCE(SUM(event_type='activated'),0),COALESCE(SUM(event_type='deactivated'),0),
COALESCE(SUM(event_type='close_date_updated'),0),COALESCE(SUM(cohort=1),0)
FROM research_lifecycle_events`).Scan(&lifeRaw, &lifeActivated, &lifeDeactivated, &lifeCloseChanges, &lifeEpisodes); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_lifecycle_horizons`).Scan(&lifeHorizons); err != nil {
		return nil, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM research_lifecycle_horizon_misses`).Scan(&lifeMissed); err != nil {
		return nil, err
	}
	var basket simple
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(candidate),0),COALESCE(MAX(net_lock),0),
COALESCE(MAX(CASE WHEN candidate=1 THEN capacity_contracts ELSE 0 END),0) FROM research_event_baskets`).Scan(&basket.Observations, &basket.Candidates, &basket.BestNet, &basket.Capacity); err != nil {
		return nil, err
	}
	var nestedObs, nestedCandidates, nestedViolations int
	var nestedBest, nestedCapacity float64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(candidate),0),COALESCE(SUM(quote_violation),0),
COALESCE(MAX(net_lock_if_both_fill),0),COALESCE(MAX(CASE WHEN candidate=1 THEN capacity_contracts ELSE 0 END),0)
FROM research_nested_ladders`).Scan(&nestedObs, &nestedCandidates, &nestedViolations, &nestedBest, &nestedCapacity); err != nil {
		return nil, err
	}
	recentQueue, err := s.recentQueueResearch(ctx, 20)
	if err != nil {
		return nil, err
	}
	recentIncentive, err := s.recentIncentiveResearch(ctx, 20)
	if err != nil {
		return nil, err
	}
	recentLifecycle, err := s.recentLifecycleResearch(ctx, 20)
	if err != nil {
		return nil, err
	}
	recentBaskets, err := s.recentBasketResearch(ctx, 20)
	if err != nil {
		return nil, err
	}
	recentNested, err := s.recentNestedResearch(ctx, 20)
	if err != nil {
		return nil, err
	}
	proper, err := s.ProperScoreReport(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"generated_at":    time.Now().UTC().Format(time.RFC3339Nano),
		"funded":          false,
		"queue-priority":  map[string]any{"orders": q.Orders, "samples": q.Samples, "filled": q.Filled, "canceled": q.Canceled, "partial_canceled": q.PartialCanceled, "estimate_n": q.EstimateN, "mean_abs_estimate_error": q.MAE, "limitation": "true queue exists only for naturally resting real account orders; the system never creates one", "recent": recentQueue},
		"incentive-maker": map[string]any{"observations": incN, "programs": incPrograms, "reward_in_ev": false, "competition": "unknown", "recent": recentIncentive},
		"lifecycle-reopen": map[string]any{"persisted_transition_frames": lifeRaw,
			"activated_frames": lifeActivated, "deactivated_frames": lifeDeactivated,
			"close_date_change_frames": lifeCloseChanges, "genuine_reopen_episodes": lifeEpisodes,
			"episodes": lifeEpisodes, "horizon_samples": lifeHorizons,
			"horizon_missed": lifeMissed, "horizons_seconds": []int{5, 30, 300, 1800},
			"raw_baseline_and_exclusion_counts": "see durable /api/research/collectors lifecycle-reopen receipt metrics",
			"rest_calls_per_sweep_max":          4, "recent": recentLifecycle},
		"event-basket-lock": map[string]any{"observations": basket.Observations, "candidates": basket.Candidates,
			"best_net_lock": basket.BestNet, "max_candidate_capacity": basket.Capacity, "atomic": false,
			"funded": false, "partial_fill_exposure_is_telemetry": true, "recent": recentBaskets},
		"nested-ladder-lock": map[string]any{"observations": nestedObs, "candidates": nestedCandidates,
			"monotonic_quote_violations": nestedViolations, "best_net_if_both_fill": nestedBest,
			"max_candidate_capacity": nestedCapacity, "atomic": false, "funded": false, "recent": recentNested},
		"proper-score": proper,
	}, nil
}

type queueResearchRecent struct {
	OrderID      string   `json:"order_id"`
	Ticker       string   `json:"ticker"`
	Outcome      string   `json:"outcome"`
	Observed     string   `json:"observed_ts"`
	TrueQueue    float64  `json:"true_queue_fp"`
	VisibleAhead *float64 `json:"visible_ahead_fp,omitempty"`
	Error        *float64 `json:"estimate_error_fp,omitempty"`
}

func (s *Store) recentQueueResearch(ctx context.Context, limit int) ([]queueResearchRecent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT q.order_id,o.ticker,o.outcome,q.observed_ts,
q.true_queue_fp,q.visible_ahead_fp,q.estimate_error_fp FROM research_queue_samples q
JOIN research_queue_orders o ON o.order_id=q.order_id ORDER BY q.id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []queueResearchRecent
	for rows.Next() {
		var r queueResearchRecent
		if err := rows.Scan(&r.OrderID, &r.Ticker, &r.Outcome, &r.Observed, &r.TrueQueue, &r.VisibleAhead, &r.Error); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type incentiveResearchRecent struct {
	ProgramID   string  `json:"program_id"`
	Ticker      string  `json:"ticker"`
	Side        string  `json:"side"`
	Description string  `json:"description"`
	Observed    string  `json:"observed_ts"`
	RewardRaw   float64 `json:"period_reward_raw"`
	Target      float64 `json:"target_size_fp"`
	Bid         float64 `json:"bid"`
	Ask         float64 `json:"ask"`
	MakerFee    float64 `json:"maker_fee_pc"`
	Capacity    float64 `json:"capacity_contracts"`
}

func (s *Store) recentIncentiveResearch(ctx context.Context, limit int) ([]incentiveResearchRecent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT program_id,market_ticker,side,description,observed_ts,
period_reward_raw,target_size_fp,bid_px,ask_px,maker_fee_pc,capacity_contracts
FROM research_incentive_maker ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []incentiveResearchRecent
	for rows.Next() {
		var r incentiveResearchRecent
		if err := rows.Scan(&r.ProgramID, &r.Ticker, &r.Side, &r.Description, &r.Observed, &r.RewardRaw, &r.Target, &r.Bid, &r.Ask, &r.MakerFee, &r.Capacity); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type lifecycleResearchRecent struct {
	ID         int64    `json:"id"`
	Ticker     string   `json:"ticker"`
	EventType  string   `json:"event_type"`
	PriorEvent string   `json:"prior_event_type"`
	Observed   string   `json:"observed_ts"`
	QueueReset string   `json:"queue_reset_context"`
	Cohort     bool     `json:"cohort"`
	PreBid     *float64 `json:"pre_bid,omitempty"`
	PreAsk     *float64 `json:"pre_ask,omitempty"`
	PostBid    *float64 `json:"post0_bid,omitempty"`
	PostAsk    *float64 `json:"post0_ask,omitempty"`
}

func (s *Store) recentLifecycleResearch(ctx context.Context, limit int) ([]lifecycleResearchRecent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,ticker,event_type,prior_event_type,observed_ts,
queue_reset_context,cohort,pre_bid,pre_ask,post0_bid,post0_ask FROM research_lifecycle_events
ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []lifecycleResearchRecent
	for rows.Next() {
		var r lifecycleResearchRecent
		var cohort int
		if err := rows.Scan(&r.ID, &r.Ticker, &r.EventType, &r.PriorEvent, &r.Observed, &r.QueueReset, &cohort, &r.PreBid, &r.PreAsk, &r.PostBid, &r.PostAsk); err != nil {
			return nil, err
		}
		r.Cohort = cohort != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

type basketResearchRecent struct {
	Venue        string  `json:"venue"`
	EventID      string  `json:"event_id"`
	Route        string  `json:"route"`
	Observed     string  `json:"observed_ts"`
	Legs         int     `json:"legs"`
	Cost         float64 `json:"all_leg_cost"`
	Fee          float64 `json:"exact_fee"`
	PayoutFloor  float64 `json:"payout_lower_bound"`
	Net          float64 `json:"net_lock"`
	Capacity     float64 `json:"capacity_contracts"`
	PartialWorst float64 `json:"partial_fill_worst_loss"`
	Candidate    bool    `json:"candidate"`
}

func (s *Store) recentBasketResearch(ctx context.Context, limit int) ([]basketResearchRecent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT venue,event_id,route,observed_ts,leg_count,all_leg_cost,
exact_fee,payout_lower_bound,net_lock,capacity_contracts,partial_fill_worst_loss,candidate
FROM research_event_baskets ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []basketResearchRecent
	for rows.Next() {
		var r basketResearchRecent
		var candidate int
		if err := rows.Scan(&r.Venue, &r.EventID, &r.Route, &r.Observed, &r.Legs, &r.Cost, &r.Fee, &r.PayoutFloor, &r.Net, &r.Capacity, &r.PartialWorst, &candidate); err != nil {
			return nil, err
		}
		r.Candidate = candidate != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

type nestedResearchRecent struct {
	EventID         string  `json:"event_id"`
	LowTicker       string  `json:"low_ticker"`
	HighTicker      string  `json:"high_ticker"`
	Observed        string  `json:"observed_ts"`
	LowStrike       float64 `json:"low_strike"`
	HighStrike      float64 `json:"high_strike"`
	Cost            float64 `json:"all_leg_cost"`
	Fee             float64 `json:"exact_entry_fee"`
	Net             float64 `json:"net_if_both_fill"`
	Capacity        float64 `json:"capacity_contracts"`
	PartialWorst    float64 `json:"partial_fill_worst_loss"`
	UnwindBuffer    float64 `json:"unwind_buffer"`
	ViolationAmount float64 `json:"violation_amount"`
	Violation       bool    `json:"quote_violation"`
	Candidate       bool    `json:"candidate"`
}

func (s *Store) recentNestedResearch(ctx context.Context, limit int) ([]nestedResearchRecent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT event_id,low_ticker,high_ticker,observed_ts,low_strike,
high_strike,all_leg_cost,exact_entry_fee,net_lock_if_both_fill,capacity_contracts,
partial_fill_worst_loss,unwind_buffer,violation_amount,quote_violation,candidate
FROM research_nested_ladders ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []nestedResearchRecent
	for rows.Next() {
		var r nestedResearchRecent
		var violation, candidate int
		if err := rows.Scan(&r.EventID, &r.LowTicker, &r.HighTicker, &r.Observed, &r.LowStrike, &r.HighStrike,
			&r.Cost, &r.Fee, &r.Net, &r.Capacity, &r.PartialWorst, &r.UnwindBuffer, &r.ViolationAmount,
			&violation, &candidate); err != nil {
			return nil, err
		}
		r.Violation = violation != 0
		r.Candidate = candidate != 0
		out = append(out, r)
	}
	return out, rows.Err()
}
