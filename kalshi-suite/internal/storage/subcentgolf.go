package storage

// Prospective sub-cent golf cohort persistence.  These rows are execution observations only:
// they never enter paper_fills, a portfolio ledger, or a live-order queue.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type SubcentGolfTrial struct {
	Slot           string
	Platform       string
	Ticker         string
	EventTicker    string
	Title          string
	Player         string
	Side           string
	Phase          string
	PhaseEvidence  string
	PlayerStatus   string
	StatusEvidence string
	MarketType     string
	Route          string
	BookSource     string
	QuoteAgeS      float64
	TickSize       float64
	BidPx          float64
	AskPx          float64
	BidDepth       float64
	AskDepth       float64
	QueueAhead     *float64
	FillState      string
	FillPrice      float64
	FeePC          float64
	RebatePC       float64
	FeeSource      string
	CancelReason   string
}

func validSubcentGolfTrial(t SubcentGolfTrial) error {
	if t.Ticker == "" || (t.Side != "YES" && t.Side != "NO") {
		return fmt.Errorf("invalid ticker/side")
	}
	if t.Route != "maker" && t.Route != "taker" {
		return fmt.Errorf("invalid route %q", t.Route)
	}
	if t.AskPx <= 0 || t.AskPx >= 1 || t.TickSize <= 0 {
		return fmt.Errorf("invalid ask/tick")
	}
	phaseOK := t.Phase == "pre_event" || t.Phase == "live" || t.Phase == "unknown"
	statusOK := t.PlayerStatus == "active" || t.PlayerStatus == "cut" || t.PlayerStatus == "withdrawn" || t.PlayerStatus == "eliminated" || t.PlayerStatus == "unknown"
	typeOK := t.MarketType == "winner" || t.MarketType == "round_leader" || t.MarketType == "matchup" || t.MarketType == "prop"
	stateOK := t.FillState == "resting" || t.FillState == "filled" || t.FillState == "canceled" || t.FillState == "not_quoteable" || t.FillState == "blocked"
	if !phaseOK || !statusOK || !typeOK || !stateOK {
		return fmt.Errorf("invalid cohort enum")
	}
	return nil
}

// InsertSubcentGolfTrial inserts one route observation per hourly slot.  inserted=false is the
// expected result when the ten-minute scanner revisits the same quote cohort within that hour.
func (s *Store) InsertSubcentGolfTrial(ctx context.Context, t SubcentGolfTrial) (id int64, inserted bool, err error) {
	if t.Platform == "" {
		t.Platform = "kalshi"
	}
	if t.Slot == "" {
		t.Slot = time.Now().UTC().Format("2006-01-02T15")
	}
	if err = validSubcentGolfTrial(t); err != nil {
		return 0, false, err
	}
	res, err := s.db.ExecContext(ctx, `
INSERT OR IGNORE INTO subcent_golf_trials(
 observed_ts,slot,platform,ticker,event_ticker,title,player,side,
 phase,phase_evidence,player_status,status_evidence,market_type,route,
 book_source,quote_age_s,tick_size,bid_px,ask_px,bid_depth,ask_depth,queue_ahead,
 fill_state,fill_price,fee_pc,rebate_pc,fee_source,cancel_reason,fill_ts,cancel_ts)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		nowRFC(), t.Slot, t.Platform, t.Ticker, t.EventTicker, t.Title, t.Player, t.Side,
		t.Phase, t.PhaseEvidence, t.PlayerStatus, t.StatusEvidence, t.MarketType, t.Route,
		t.BookSource, t.QuoteAgeS, t.TickSize, t.BidPx, t.AskPx, t.BidDepth, t.AskDepth, t.QueueAhead,
		t.FillState, t.FillPrice, t.FeePC, t.RebatePC, t.FeeSource, t.CancelReason,
		func() string {
			if t.FillState == "filled" {
				return nowRFC()
			}
			return ""
		}(),
		func() string {
			if t.FillState == "canceled" || t.FillState == "not_quoteable" || t.FillState == "blocked" {
				return nowRFC()
			}
			return ""
		}())
	if err != nil {
		return 0, false, err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return 0, false, err
	}
	id, err = res.LastInsertId()
	return id, true, err
}

func splitFeeRebate(net float64) (fee, rebate float64) {
	if net < 0 {
		return 0, -net
	}
	return net, 0
}

func (s *Store) AttachSubcentGolfMaker(ctx context.Context, trialID, attemptID int64, queueAhead *float64) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE subcent_golf_trials SET maker_attempt_id=?, queue_ahead=?
WHERE id=? AND route='maker' AND fill_state='resting'`, attemptID, queueAhead, trialID)
	return err
}

func (s *Store) MarkSubcentGolfTrialBlocked(ctx context.Context, trialID int64, reason string) error {
	if strings.TrimSpace(reason) == "" {
		reason = "blocked"
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE subcent_golf_trials SET fill_state='blocked',cancel_ts=?,cancel_reason=?
WHERE id=? AND route='maker' AND fill_state='resting'`, nowRFC(), reason, trialID)
	return err
}

func (s *Store) MarkSubcentGolfMakerFilled(ctx context.Context, attemptID int64, fillPx, netFee float64, feeSource, rule string) error {
	fee, rebate := splitFeeRebate(netFee)
	_, err := s.db.ExecContext(ctx, `
UPDATE subcent_golf_trials
SET fill_state='filled',fill_ts=?,fill_price=?,fee_pc=?,rebate_pc=?,fee_source=?,fill_rule=?
WHERE maker_attempt_id=? AND route='maker' AND fill_state='resting'`,
		nowRFC(), fillPx, fee, rebate, feeSource, rule, attemptID)
	return err
}

func (s *Store) MarkSubcentGolfMakerCanceled(ctx context.Context, attemptID int64, reason string) error {
	if strings.TrimSpace(reason) == "" {
		reason = "canceled"
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE subcent_golf_trials
SET fill_state='canceled',cancel_ts=?,cancel_reason=?
WHERE maker_attempt_id=? AND route='maker' AND fill_state='resting'`, nowRFC(), reason, attemptID)
	return err
}

func (s *Store) ExpireSubcentGolfOrphans(ctx context.Context, age time.Duration) (int64, error) {
	cut := time.Now().UTC().Add(-age).Format(time.RFC3339)
	res, err := s.db.ExecContext(ctx, `
UPDATE subcent_golf_trials SET fill_state='canceled',cancel_ts=?,cancel_reason='orphan-boot'
WHERE route='maker' AND fill_state='resting' AND observed_ts < ?`, nowRFC(), cut)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// ResolveSubcentGolfTicker mirrors authoritative Kalshi settlement onto every prospective route.
// Unfilled routes retain zero realized P/L; filled routes pay the exact side payout less the
// captured one-contract fee plus any rebate. Observation-time player_status is immutable (using
// settlement to rewrite it would leak the future into a prospective cohort). Winner settlement
// instead populates terminal_status: YES=1 means survived/won (the requested "active" bucket),
// YES=0 means eliminated. Props/matchups never pretend a lost bet means a player was cut.
func (s *Store) ResolveSubcentGolfTicker(ctx context.Context, ticker string, yesVal float64) error {
	if ticker == "" || yesVal < 0 || yesVal > 1 {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE subcent_golf_trials
SET settled=1, settle_val=?,
    payout_pc=CASE WHEN side='NO' THEN 1.0-? ELSE ? END,
    net_pc=CASE WHEN fill_state='filled'
                THEN (CASE WHEN side='NO' THEN 1.0-? ELSE ? END)-fill_price-fee_pc+rebate_pc
                ELSE 0 END,
    settled_ts=?,
    terminal_status=CASE
      WHEN market_type='winner' AND ? >= 0.999999 THEN 'active'
      WHEN market_type='winner' AND ? <= 0.000001 THEN 'eliminated'
      ELSE terminal_status END,
    terminal_status_evidence=CASE
      WHEN market_type='winner' THEN 'authoritative winner-market settlement (active means survived/not eliminated)'
      ELSE terminal_status_evidence END
WHERE ticker=? AND settled=0`, yesVal, yesVal, yesVal, yesVal, yesVal, nowRFC(), yesVal, yesVal, ticker)
	return err
}

// OpenSubcentGolfTickers is the bounded settlement work list. Reading it is cheap and local; the
// server decides from its market cache whether a ticker is still active before spending any REST.
func (s *Store) OpenSubcentGolfTickers(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 || limit > 2000 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT ticker FROM subcent_golf_trials WHERE settled=0
GROUP BY ticker ORDER BY MIN(observed_ts),ticker LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var ticker string
		if err := rows.Scan(&ticker); err != nil {
			return nil, err
		}
		out = append(out, ticker)
	}
	return out, rows.Err()
}

type SubcentGolfGroup struct {
	Phase          string   `json:"phase"`
	PlayerStatus   string   `json:"player_status"`
	TerminalStatus string   `json:"terminal_status"`
	MarketType     string   `json:"market_type"`
	Route          string   `json:"route"`
	N              int      `json:"n"`
	Filled         int      `json:"filled"`
	Canceled       int      `json:"canceled"`
	NotQuoteable   int      `json:"not_quoteable"`
	Blocked        int      `json:"blocked"`
	Settled        int      `json:"settled"`
	FillRate       float64  `json:"fill_rate"`
	MeanQueueAhead *float64 `json:"mean_queue_ahead,omitempty"`
	MeanFeePC      float64  `json:"mean_fee_pc"`
	MeanRebatePC   float64  `json:"mean_rebate_pc"`
	MeanNetPC      *float64 `json:"mean_net_pc,omitempty"`
}

type SubcentGolfRecent struct {
	ObservedTS             string   `json:"observed_ts"`
	Ticker                 string   `json:"ticker"`
	Player                 string   `json:"player"`
	Side                   string   `json:"side"`
	Phase                  string   `json:"phase"`
	PhaseEvidence          string   `json:"phase_evidence"`
	PlayerStatus           string   `json:"player_status"`
	StatusEvidence         string   `json:"status_evidence"`
	TerminalStatus         string   `json:"terminal_status"`
	TerminalStatusEvidence string   `json:"terminal_status_evidence"`
	MarketType             string   `json:"market_type"`
	Route                  string   `json:"route"`
	BookSource             string   `json:"book_source"`
	QuoteAgeS              float64  `json:"quote_age_s"`
	TickSize               float64  `json:"tick_size"`
	BidPx                  float64  `json:"bid_px"`
	AskPx                  float64  `json:"ask_px"`
	BidDepth               float64  `json:"bid_depth"`
	AskDepth               float64  `json:"ask_depth"`
	QueueAhead             *float64 `json:"queue_ahead,omitempty"`
	FillState              string   `json:"fill_state"`
	FillRule               string   `json:"fill_rule,omitempty"`
	CancelReason           string   `json:"cancel_reason,omitempty"`
	FillPrice              float64  `json:"fill_price"`
	FeePC                  float64  `json:"fee_pc"`
	RebatePC               float64  `json:"rebate_pc"`
	FeeSource              string   `json:"fee_source"`
	Settled                bool     `json:"settled"`
	SettleVal              *float64 `json:"settle_val,omitempty"`
	NetPC                  *float64 `json:"net_pc,omitempty"`
}

type SubcentGolfReport struct {
	GeneratedAt  string              `json:"generated_at"`
	System       string              `json:"system"`
	Signal       string              `json:"signal"`
	Promotion    string              `json:"promotion"`
	ResearchOnly bool                `json:"research_only"`
	Definition   string              `json:"definition"`
	Total        int                 `json:"total"`
	OpenMaker    int                 `json:"open_maker"`
	Settled      int                 `json:"settled"`
	Groups       []SubcentGolfGroup  `json:"groups"`
	Recent       []SubcentGolfRecent `json:"recent"`
}

func nullableFloat(n sql.NullFloat64) *float64 {
	if !n.Valid {
		return nil
	}
	v := n.Float64
	return &v
}

func (s *Store) SubcentGolfReport(ctx context.Context, recentLimit int) (SubcentGolfReport, error) {
	out := SubcentGolfReport{GeneratedAt: nowRFC(), System: "subcent-golf",
		Signal:       "a fresh side-specific Kalshi depth book offers one or more contracts below 1 cent on a fine-tick golf market; maker and taker routes are observed separately",
		Promotion:    "remain unfunded until a predeclared status/phase/type/route cell has positive fee-net lower-bound Net/day with honest capacity",
		ResearchOnly: true,
		Definition:   "one-contract, hourly, cached Kalshi depth-book observations; observation status never uses future settlement; winner terminal active means survived/not eliminated; no paper or live orders"}
	if recentLimit <= 0 || recentLimit > 500 {
		recentLimit = 100
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(fill_state='resting'),0),COALESCE(SUM(settled),0) FROM subcent_golf_trials`).Scan(&out.Total, &out.OpenMaker, &out.Settled); err != nil {
		return out, err
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT phase,player_status,terminal_status,market_type,route,COUNT(*),
       SUM(fill_state='filled'),SUM(fill_state='canceled'),SUM(fill_state='not_quoteable'),SUM(fill_state='blocked'),SUM(settled),
       CASE WHEN SUM(fill_state IN ('filled','canceled')) > 0
            THEN 1.0*SUM(fill_state='filled')/SUM(fill_state IN ('filled','canceled')) ELSE 0 END,
       AVG(queue_ahead),AVG(fee_pc),AVG(rebate_pc),
       AVG(CASE WHEN settled=1 AND fill_state='filled' THEN net_pc END)
FROM subcent_golf_trials GROUP BY phase,player_status,terminal_status,market_type,route
ORDER BY phase,player_status,terminal_status,market_type,route`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var g SubcentGolfGroup
		var q, net sql.NullFloat64
		if err := rows.Scan(&g.Phase, &g.PlayerStatus, &g.TerminalStatus, &g.MarketType, &g.Route, &g.N,
			&g.Filled, &g.Canceled, &g.NotQuoteable, &g.Blocked, &g.Settled, &g.FillRate,
			&q, &g.MeanFeePC, &g.MeanRebatePC, &net); err != nil {
			rows.Close()
			return out, err
		}
		g.MeanQueueAhead, g.MeanNetPC = nullableFloat(q), nullableFloat(net)
		out.Groups = append(out.Groups, g)
	}
	if err := rows.Close(); err != nil {
		return out, err
	}
	recent, err := s.db.QueryContext(ctx, `
SELECT observed_ts,ticker,player,side,phase,phase_evidence,player_status,status_evidence,
       terminal_status,terminal_status_evidence,market_type,route,book_source,quote_age_s,tick_size,
       bid_px,ask_px,bid_depth,ask_depth,queue_ahead,fill_state,fill_rule,cancel_reason,
       fill_price,fee_pc,rebate_pc,fee_source,
       settled,settle_val,net_pc
FROM subcent_golf_trials ORDER BY id DESC LIMIT ?`, recentLimit)
	if err != nil {
		return out, err
	}
	defer recent.Close()
	for recent.Next() {
		var r SubcentGolfRecent
		var q, sv, net sql.NullFloat64
		var settled int
		if err := recent.Scan(&r.ObservedTS, &r.Ticker, &r.Player, &r.Side, &r.Phase, &r.PhaseEvidence,
			&r.PlayerStatus, &r.StatusEvidence, &r.TerminalStatus, &r.TerminalStatusEvidence,
			&r.MarketType, &r.Route, &r.BookSource, &r.QuoteAgeS, &r.TickSize, &r.BidPx, &r.AskPx, &r.BidDepth, &r.AskDepth,
			&q, &r.FillState, &r.FillRule, &r.CancelReason, &r.FillPrice, &r.FeePC, &r.RebatePC,
			&r.FeeSource, &settled, &sv, &net); err != nil {
			return out, err
		}
		r.QueueAhead, r.SettleVal, r.NetPC, r.Settled = nullableFloat(q), nullableFloat(sv), nullableFloat(net), settled != 0
		out.Recent = append(out.Recent, r)
	}
	return out, recent.Err()
}
