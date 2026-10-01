package storage

// Native rows for the research-only Systems regime view.  These readers intentionally expose
// execution ledgers only.  signal_log is not consulted: a discovery-time percentage is never a
// substitute for an executable quote, fill, fee receipt, or settlement.

import (
	"context"
	"database/sql"
	"strings"
)

// NativeSystemObservation is one one-unit observation whose P&L is already fee-net when
// PnLKnown is true.  Open rows are retained so elapsed/occupied clocks and open counts do not
// become conditional on fast settlement.
type NativeSystemObservation struct {
	OpenedTS, ClosedTS             string
	System, Signal, Venue, Route   string
	Source, OriginLayer            string
	CostPC, FeePC, PnLPC, Depth    float64
	QueueAhead                     float64
	PnLKnown, FeeKnown, DepthKnown bool
	QueueKnown, Filled, Terminal   bool
}

// NativeUnitSystemObservations reads the prospective executable-ask lane.  capital_day is
// deliberately absent: that legacy column was written with an old one-hour floor.  Consumers
// must derive every rate from opened_ts/closed_ts themselves.
func (s *Store) NativeUnitSystemObservations(ctx context.Context) ([]NativeSystemObservation, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT opened_ts,closed_ts,family,platform,origin_layer,ask,fee_pc,fee_known,fee_source,depth,quote_source,
       settled,pnl_pc
FROM unit_trials ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeSystemObservation
	for rows.Next() {
		var o NativeSystemObservation
		var origin string
		var settled, feeKnown int
		var feeSource string
		var pnl sql.NullFloat64
		if err := rows.Scan(&o.OpenedTS, &o.ClosedTS, &o.System, &o.Venue, &origin,
			&o.CostPC, &o.FeePC, &feeKnown, &feeSource, &o.Depth, &o.Source, &settled, &pnl); err != nil {
			return nil, err
		}
		o.Signal = o.System
		o.OriginLayer = origin
		o.Route = "taker"
		o.CostPC += o.FeePC
		o.FeeKnown = feeKnown == 1 && strings.TrimSpace(feeSource) != ""
		o.DepthKnown = o.Depth >= 1
		// Visible depth proves that a quote was present, not that an exchange accepted or filled an
		// order. Unit trials never submit one, so Filled must remain false.
		o.Filled = false
		o.PnLKnown = o.DepthKnown && o.FeeKnown && settled != 0 && pnl.Valid
		o.Terminal = o.PnLKnown
		if pnl.Valid {
			o.PnLPC = pnl.Float64
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// NativeMakerObservation keeps the maker-specific raw fields needed to apply the venue's current
// exact/conservative fee function in the server package.  filled=0 is a terminal $0 attempt;
// filled=-1 is still resting; guard-refused counterfactual rows (filled=-2) are excluded.
type NativeMakerObservation struct {
	OpenedTS, ClosedTS                  string
	Venue, Ticker, Side, Source, Family string
	PostPx, Depth, QueueAhead, FeePC    float64
	QueueKnown                          bool
	FeeKnown                            bool
	FeeSource                           string
	Inverted                            bool
	State                               int
	Settle                              float64
	SettleKnown                         bool
}

func (s *Store) NativeMakerObservations(ctx context.Context) ([]NativeMakerObservation, error) {
	const indexedQuery = `
SELECT m.ts,
       CASE WHEN m.filled=1 AND m.settle_val IS NOT NULL THEN COALESCE((
         SELECT MAX(r.resolved_at)
         FROM signal_log r INDEXED BY idx_signal_ticker_resolved
         WHERE r.ticker=m.ticker AND r.resolved=1 AND r.platform=m.platform
           AND r.resolved_at IS NOT NULL AND r.resolved_at!=''
       ),'') ELSE '' END,
       m.fill_ts,m.expire_ts,m.platform,m.ticker,m.side,
       m.source,m.strategy_family,m.strategy_inverted,m.post_px,m.book_depth,m.queue_ahead,
       m.filled,m.settle_val,m.maker_fee_pc,m.maker_rebate_pc,m.maker_fee_source
FROM maker_fill_stats m
WHERE m.filled IN (-1,0,1) ORDER BY m.id`
	rows, err := s.db.QueryContext(ctx, indexedQuery)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "no such index") {
		// The index is installed by storage.Open. If an old/read-only store could not complete that
		// best-effort migration, retain correctness with the slower aggregate instead of making the
		// research surface unavailable.
		rows, err = s.db.QueryContext(ctx, `
SELECT m.ts,COALESCE(r.resolved_at,''),m.fill_ts,m.expire_ts,m.platform,m.ticker,m.side,
       m.source,m.strategy_family,m.strategy_inverted,m.post_px,m.book_depth,m.queue_ahead,
       m.filled,m.settle_val,m.maker_fee_pc,m.maker_rebate_pc,m.maker_fee_source
FROM maker_fill_stats m
LEFT JOIN (
  SELECT platform,ticker,MAX(resolved_at) AS resolved_at
  FROM signal_log
  WHERE resolved=1 AND resolved_at IS NOT NULL AND resolved_at!=''
  GROUP BY platform,ticker
) r ON r.platform=m.platform AND r.ticker=m.ticker
WHERE m.filled IN (-1,0,1) ORDER BY m.id`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeMakerObservation
	for rows.Next() {
		var o NativeMakerObservation
		var resolvedTS, fillTS, expireTS string
		var inv int
		var queue, settle, fee, rebate sql.NullFloat64
		if err := rows.Scan(&o.OpenedTS, &resolvedTS, &fillTS, &expireTS, &o.Venue, &o.Ticker,
			&o.Side, &o.Source, &o.Family, &inv, &o.PostPx, &o.Depth, &queue,
			&o.State, &settle, &fee, &rebate, &o.FeeSource); err != nil {
			return nil, err
		}
		o.Inverted = inv != 0
		o.QueueKnown = queue.Valid
		o.QueueAhead = queue.Float64
		o.SettleKnown = settle.Valid
		o.Settle = settle.Float64
		o.FeeKnown = fee.Valid && rebate.Valid && strings.TrimSpace(o.FeeSource) != ""
		if o.FeeKnown {
			o.FeePC = fee.Float64 - rebate.Float64
		}
		if o.State == 0 {
			o.ClosedTS = expireTS
		} else if o.State == 1 && settle.Valid {
			// The canonical settlement ledger supplies the close clock. fill_ts is deliberately not
			// used as a substitute: it is when capital became a position, not when P&L realized.
			o.ClosedTS = resolvedTS
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// NativePaperParlayObservations returns only non-artifact paper combos.  realized is already
// fee-net (settlement writes contracts*payout - stake - fees); division by contracts produces an
// exact one-combo-contract result.
func (s *Store) NativePaperParlayObservations(ctx context.Context) ([]NativeSystemObservation, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT ts,COALESCE(settled_ts,''),price,contracts,stake,COALESCE(fees,0),status,
       COALESCE(realized,0)
FROM paper_parlays WHERE COALESCE(artifact,0)=0 ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeSystemObservation
	for rows.Next() {
		var o NativeSystemObservation
		var price, contracts, stake, fees, realized float64
		var status string
		if err := rows.Scan(&o.OpenedTS, &o.ClosedTS, &price, &contracts, &stake, &fees,
			&status, &realized); err != nil {
			return nil, err
		}
		if contracts <= 0 || price <= 0 || price >= 1 {
			continue
		}
		o.System, o.Signal, o.Venue, o.Route = "paper-parlay", "combined-leg edge", "mixed", "paper-combo"
		o.OriginLayer = "strategy"
		o.Source = "paper_parlays"
		o.CostPC, o.FeePC = (stake+fees)/contracts, fees/contracts
		o.FeeKnown, o.Filled = true, true
		o.PnLKnown = status == "settled" || status == "closed"
		o.Terminal = o.PnLKnown
		if o.PnLKnown {
			o.PnLPC = realized / contracts
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// NativeSubcentGolfObservations exposes the full pre-registered split as concrete named systems.
// blocked/not-quoteable rows are coverage receipts, not quote attempts, and are therefore omitted
// from economic rows (the dedicated /api/subcent-golf report still counts them explicitly).
func (s *Store) NativeSubcentGolfObservations(ctx context.Context) ([]NativeSystemObservation, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT observed_ts,CASE WHEN settled_ts!='' THEN settled_ts WHEN cancel_ts!='' THEN cancel_ts ELSE '' END,
       platform,phase,player_status,terminal_status,market_type,route,book_source,bid_px,ask_px,
       ask_depth,queue_ahead,fill_state,fill_price,fee_pc,rebate_pc,settled,net_pc
FROM subcent_golf_trials
WHERE fill_state IN ('resting','filled','canceled') ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeSystemObservation
	for rows.Next() {
		var o NativeSystemObservation
		var phase, status, terminalStatus, marketType, fillState string
		var bid, ask, fillPrice, rebate float64
		var settled int
		var queue, net sql.NullFloat64
		if err := rows.Scan(&o.OpenedTS, &o.ClosedTS, &o.Venue, &phase, &status,
			&terminalStatus, &marketType, &o.Route, &o.Source, &bid, &ask, &o.Depth, &queue,
			&fillState, &fillPrice, &o.FeePC, &rebate, &settled, &net); err != nil {
			return nil, err
		}
		o.System = "subcent-golf/" + phase + "/" + status + "/" + marketType
		_ = terminalStatus // descriptive report field only; never part of prospective identity
		o.Signal = "fresh sub-cent golf depth-book quote"
		o.OriginLayer = "strategy"
		o.FeePC -= rebate // negative fee is an earned rebate
		o.FeeKnown = true
		o.DepthKnown = o.Depth > 0
		o.QueueKnown, o.QueueAhead = queue.Valid, queue.Float64
		o.Filled = fillState == "filled"
		if fillState == "canceled" {
			reserved := ask
			if o.Route == "maker" && bid > 0 {
				reserved = bid
			}
			o.CostPC, o.PnLPC, o.PnLKnown, o.Terminal = reserved, 0, true, true
		} else {
			if fillPrice <= 0 {
				fillPrice = ask
				if o.Route == "maker" && bid > 0 {
					fillPrice = bid // resting maker capital is reserved at the posted bid
				}
			}
			o.CostPC = fillPrice + o.FeePC
			o.PnLKnown = settled != 0 && net.Valid
			o.Terminal = o.PnLKnown
			if net.Valid {
				o.PnLPC = net.Float64
			}
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// NativeWeatherResearchObservations is the sole narrow exception to the no-signal_log rule.  The
// weather research producers persist their actual bought-side ask in the versioned book-v1 fields.
// Rows are admitted only when the executable ask/depth/source and exact taker fee are all present;
// entry_price and every legacy/non-versioned row remain ignored.
func (s *Store) NativeWeatherResearchObservations(ctx context.Context) ([]NativeSystemObservation, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT ts,COALESCE(resolved_at,''),signal_type,platform,book_ask,book_taker_fee_pc,
       book_ask_depth,book_source,resolved,won
FROM signal_log
WHERE signal_type IN ('weather-curve-residual','weather-curve-revision','independent-probabilistic-weather')
  AND book_feature_ver=1
  AND book_ask IS NOT NULL AND book_ask>0 AND book_ask<1
  AND book_ask_depth IS NOT NULL AND book_ask_depth>0
  AND book_taker_fee_pc IS NOT NULL
  AND TRIM(book_source)!=''
ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NativeSystemObservation
	for rows.Next() {
		var o NativeSystemObservation
		var settled int
		var won sql.NullInt64
		if err := rows.Scan(&o.OpenedTS, &o.ClosedTS, &o.System, &o.Venue, &o.CostPC,
			&o.FeePC, &o.Depth, &o.Source, &settled, &won); err != nil {
			return nil, err
		}
		o.Signal, o.Route = o.System, "taker"
		o.OriginLayer = "model"
		o.CostPC += o.FeePC
		o.FeeKnown, o.DepthKnown, o.Filled = true, true, true
		o.PnLKnown = settled != 0 && won.Valid && (won.Int64 == 0 || won.Int64 == 1)
		o.Terminal = o.PnLKnown
		if o.PnLKnown {
			o.PnLPC = float64(won.Int64) - (o.CostPC - o.FeePC) - o.FeePC
		}
		out = append(out, o)
	}
	return out, rows.Err()
}
