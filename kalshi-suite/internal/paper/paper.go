// Package paper is the paper-trading (simulated) book: it records fills and folds
// them into net positions with average-cost P&L. No real orders are ever placed —
// this is the foundation the external review gate and the Stats panel build on.
package paper

import (
	"sort"
	"strings"
)

// Fill is one simulated execution.
type Fill struct {
	ID        int64   `json:"id"`
	TS        string  `json:"ts"`
	Platform  string  `json:"platform"` // kalshi | polymarket
	Ticker    string  `json:"ticker"`
	Title     string  `json:"title"`
	Side      string  `json:"side"`   // YES | NO | outcome label
	Action    string  `json:"action"` // BUY | SELL
	Price     float64 `json:"price"`  // 0..1
	Contracts float64 `json:"contracts"`
	Fee       float64 `json:"fee"`
	TP        float64 `json:"tp"`     // optional take-profit price (0..1, the side's price)
	SL        float64 `json:"sl"`     // optional stop-loss price (0..1)
	Source    string  `json:"source"` // manual | gate | arb | whale | auto-tp | auto-sl
	Note      string  `json:"note"`
	// R90 (auditor DO-THIS 5ii / edge 29): the signal side's LIVE market price at the moment of
	// fill — separates slip (fill vs signal price) from winner's curse (signal price moved before
	// the fill). Instant/taker fills stamp the fill price itself; honest maker fills stamp the
	// touch price that triggered the fill. 0 = pre-R90 row (unknown).
	SigPxAtFill float64 `json:"sig_px_at_fill,omitempty"`
	// R122 queue-aware router: structured maker/taker tag ("" = pre-R122 row) + the routing
	// reason with estimate inputs, e.g. "taker:queue-deep(q=250,r=36.0/m,eta=417s,hz=300s)".
	FillKind    string `json:"fill_kind,omitempty"`
	RouteReason string `json:"route_reason,omitempty"`
}

// Position is the net OPEN paper position for one market+side (average-cost basis).
// CurPrice/Unrealized are left zero here and filled by the caller that has live marks.
type Position struct {
	Platform   string  `json:"platform"`
	Ticker     string  `json:"ticker"`
	Title      string  `json:"title"`
	Side       string  `json:"side"`
	Contracts  float64 `json:"contracts"`
	AvgPrice   float64 `json:"avg_price"`
	CostBasis  float64 `json:"cost_basis"` // cash tied up in the open contracts
	Fees       float64 `json:"fees"`
	Realized   float64 `json:"realized"`
	CurPrice   float64 `json:"cur_price"`
	Unrealized float64 `json:"unrealized"`
	TP         float64 `json:"tp"`
	SL         float64 `json:"sl"`
	TPHit      bool    `json:"tp_hit"`
	SLHit      bool    `json:"sl_hit"`
	Source     string  `json:"source"`         // signal that opened the position (manual/auto-arb/auto-signal/gate)
	Opened     string  `json:"opened"`         // R31: BOUGHT time — TS of the first BUY of the current lot (reset when the lot fully closes)
	URL        string  `json:"url"`            // public market link, filled live by the marking caller
	Confidence float64 `json:"confidence"`     // 0–1 live confidence of the opening signal (session win-rate × entry mid-price quality), filled by the marking caller
	Disp       string  `json:"disp,omitempty"` // ntfy-leg-style display name ("⚾ Baseball · Yankees @ Red Sox — YES"), filled by the marking caller; UI falls back to Title
}

// Leg is one selection inside a parlay.
type Leg struct {
	Platform string  `json:"platform"`
	Ticker   string  `json:"ticker"`
	Side     string  `json:"side"`
	Title    string  `json:"title"`
	Outcome  string  `json:"outcome"` // the actual outcome a YES means (e.g. "Daniel Rincon") — clarifies which side to pick
	Entry    float64 `json:"entry"`   // entry price 0..1
	// Funded Combo Paper receipts preserve the exact final executable touch. Legacy/generic rows
	// omit these fields and remain readable; they never inherit execution proof from defaults.
	BookBid        float64 `json:"book_bid,omitempty"`
	TouchDepth     float64 `json:"touch_depth,omitempty"`
	TickSize       float64 `json:"tick_size,omitempty"`
	QuantityStep   float64 `json:"quantity_step,omitempty"`
	MinimumQty     float64 `json:"minimum_qty,omitempty"`
	QuoteAgeS      float64 `json:"quote_age_s,omitempty"`
	BookSource     string  `json:"book_source,omitempty"`
	QuantitySource string  `json:"quantity_source,omitempty"`
	EntryFee       float64 `json:"entry_fee,omitempty"`
	FeeSource      string  `json:"fee_source,omitempty"`
}

// Parlay is a multi-leg, all-or-nothing paper bet. Price is the combined entry
// (product of leg prices); Contracts = Stake/Price; the payout if every leg hits
// is Contracts dollars. Mark/Value/Unrealized are computed live by the server.
type Parlay struct {
	ID         int64   `json:"id"`
	TS         string  `json:"ts"`
	Stake      float64 `json:"stake"`
	Price      float64 `json:"price"`
	Contracts  float64 `json:"contracts"`
	TP         float64 `json:"tp"`
	SL         float64 `json:"sl"`
	Status     string  `json:"status"`
	Legs       []Leg   `json:"legs"`
	Mark       float64 `json:"mark"`
	Value      float64 `json:"value"`
	Unrealized float64 `json:"unrealized"`
	Payout     float64 `json:"payout"`     // settled product-of-legs value 0..1 (0 until settled)
	Realized   float64 `json:"realized"`   // settled $ P&L = contracts×payout − stake − fees (0 until settled)
	Fees       float64 `json:"fees"`       // entry fees: generic rows use rolled simulation; funded rows store exact final package-quantity fees
	SettledTS  string  `json:"settled_ts"` // when it settled/closed ("" while open)
	Artifact   bool    `json:"artifact"`   // pre-correlation-guard churn rows (audit F4) — excluded from headline P&L
	// Funded combo provenance is durable. Blank RouteSource is intentionally legacy/generic:
	// callers must never infer a funded positive-system row by parsing an audit message.
	RouteSource          string   `json:"route_source,omitempty"`
	Cohort               string   `json:"cohort,omitempty"`
	SystemIDs            []string `json:"system_ids,omitempty"`
	JointP               float64  `json:"joint_p,omitempty"`
	ExpectedNetPerDollar float64  `json:"expected_net_per_dollar,omitempty"`
	// Combo attribution is frozen when the candidate is admitted. These fields identify one
	// exact registered 2-6 leg system variant; they are never reconstructed from a later catalog.
	CanonicalSystemID string `json:"canonical_system_id,omitempty"`
	ComboVenue        string `json:"combo_venue,omitempty"`
	LegCount          int    `json:"leg_count,omitempty"`
	RelationClass     string `json:"relation_class,omitempty"`
	ProducerFamily    string `json:"producer_family,omitempty"`
	ComboRoute        string `json:"combo_route,omitempty"`
	ExperimentEpoch   string `json:"experiment_epoch,omitempty"`
	ComboKey          string `json:"combo_key,omitempty"`
}

// Summary rolls up the whole paper book.
type Summary struct {
	OpenPositions int     `json:"open_positions"`
	CostBasis     float64 `json:"cost_basis"`
	Realized      float64 `json:"realized"`
	Unrealized    float64 `json:"unrealized"`
	Fees          float64 `json:"fees"`
	Fills         int     `json:"fills"`
}

// Aggregate folds fills into net positions using average-cost accounting. A SELL
// realizes P&L against the running average cost; you can't sell more than held.
func Aggregate(fills []Fill) ([]Position, Summary) {
	sort.SliceStable(fills, func(i, j int) bool { return fills[i].TS < fills[j].TS })

	type acc struct {
		platform, ticker, title, side, source string
		contracts, cost, fees, realized       float64
		tp, sl                                float64
		opened                                string // R31: BOUGHT time of the current lot
	}
	m := map[string]*acc{}
	var order []string
	sum := Summary{Fills: len(fills)}

	for _, f := range fills {
		key := f.Platform + "|" + f.Ticker + "|" + f.Side
		a := m[key]
		if a == nil {
			a = &acc{platform: f.Platform, ticker: f.Ticker, title: f.Title, side: f.Side}
			m[key] = a
			order = append(order, key)
		}
		if a.title == "" {
			a.title = f.Title
		}
		a.fees += f.Fee
		sum.Fees += f.Fee
		switch f.Action {
		case "SELL", "sell":
			avg := 0.0
			if a.contracts > 0 {
				avg = a.cost / a.contracts
			}
			qty := f.Contracts
			if qty > a.contracts {
				qty = a.contracts
			}
			a.realized += qty * (f.Price - avg)
			a.cost -= qty * avg
			a.contracts -= qty
			if a.contracts <= 1e-6 {
				// R89 (auditor bug 53): fold the finished round into the summary and RESET the lot.
				// The acc used to carry tp/sl/source/fees/realized across a flat→reopen, so a fresh
				// entry inherited the PREVIOUS lot's stops (the TP/SL sweep could auto-exit it
				// instantly) plus stale fee/realized attribution. sum.Fees already counts every
				// fill's fee globally; realized moves to the summary here (the end loop adds the
				// still-open lots' partials).
				sum.Realized += a.realized
				a.contracts, a.cost, a.fees, a.realized = 0, 0, 0, 0
				a.tp, a.sl = 0, 0
				a.source = ""
				a.opened = "" // lot fully closed — the next BUY stamps a fresh bought-time
			}
		default: // BUY
			if a.source == "" {
				a.source = f.Source
			}
			if a.opened == "" {
				a.opened = f.TS // R31: BOUGHT time = first BUY of this lot
			}
			a.contracts += f.Contracts
			a.cost += f.Contracts * f.Price
			if f.TP > 0 {
				a.tp = f.TP
			}
			if f.SL > 0 {
				a.sl = f.SL
			}
		}
	}

	var out []Position
	for _, key := range order {
		a := m[key]
		sum.Realized += a.realized // R89 (bug 53): closed rounds were folded in at flat time; this now adds only still-open lots' partial realized
		if a.contracts <= 1e-6 {
			continue // fully closed
		}
		out = append(out, Position{
			Platform: a.platform, Ticker: a.ticker, Title: a.title, Side: a.side, Source: a.source, Opened: a.opened,
			Contracts: a.contracts, AvgPrice: a.cost / a.contracts, CostBasis: a.cost,
			Fees: a.fees, Realized: a.realized, TP: a.tp, SL: a.sl, // carry the stops through so the TP/SL sweep can actually fire
		})
		sum.CostBasis += a.cost
	}
	sum.OpenPositions = len(out)
	sort.Slice(out, func(i, j int) bool { return out[i].CostBasis > out[j].CostBasis })
	return out, sum
}

// SourceStat is realized performance grouped by the signal that OPENED the trade
// (manual / whale / arb / consensus / gate), so you can see which signals make money.
type SourceStat struct {
	Source   string  `json:"source"`
	Platform string  `json:"platform"` // venue (kalshi | polymarket | polyus) so P&L can be grouped per platform
	Closed   int     `json:"closed"`
	Wins     int     `json:"wins"`
	WinRate  float64 `json:"win_rate"`
	Realized float64 `json:"realized"`
	Fees     float64 `json:"fees"` // entry+exit fees attributed to this signal (so net = realized − fees is visible per signal)
}

// StatsResult summarizes closed-trade performance. Unrealized is left zero here and
// filled by the caller that has live marks.
type StatsResult struct {
	ClosedTrades  int          `json:"closed_trades"`
	Wins          int          `json:"wins"`
	WinRate       float64      `json:"win_rate"`
	Realized      float64      `json:"realized"`
	Fees          float64      `json:"fees"`
	OpenPositions int          `json:"open_positions"`
	Unrealized    float64      `json:"unrealized"`
	BySource      []SourceStat `json:"by_source"`
}

// Stats walks fills chronologically and records a closed "round" each time a
// position returns to flat, attributing its realized P&L (and win/loss) to the
// source of the BUY that opened the round. A round wins if its realized P&L > 0.
func Stats(fills []Fill) StatsResult {
	sort.SliceStable(fills, func(i, j int) bool { return fills[i].TS < fills[j].TS })

	type lot struct {
		contracts, cost, realized, fees float64
		openSource                      string
		platform                        string
	}
	lots := map[string]*lot{}
	bysrc := map[string]*SourceStat{}
	var res StatsResult

	record := func(platform, src string, realized, fees float64) {
		if src == "" {
			src = "manual"
		}
		res.ClosedTrades++
		// FEE-NET WIN (audit Q1 #4): a round "wins" only if it made money AFTER fees. The old
		// gross test counted a +$0.05-gross/$0.40-fee round as a win, inflating wrCache → the
		// very win-rates the gates and probation ramps consume.
		if realized-fees > 0 {
			res.Wins++
		}
		res.Realized += realized
		key := platform + "|" + src // group P&L by venue + signal
		ss := bysrc[key]
		if ss == nil {
			ss = &SourceStat{Source: src, Platform: platform}
			bysrc[key] = ss
		}
		ss.Closed++
		if realized-fees > 0 {
			ss.Wins++
		}
		ss.Realized += realized
		ss.Fees += fees // entry+exit fees for this round, attributed to the signal that opened it
	}

	for _, f := range fills {
		key := f.Platform + "|" + f.Ticker + "|" + f.Side
		l := lots[key]
		if l == nil {
			l = &lot{}
			lots[key] = l
		}
		res.Fees += f.Fee
		l.fees += f.Fee // accumulate this round's fees, attributed to the opening signal on close
		switch strings.ToUpper(f.Action) {
		case "SELL":
			avg := 0.0
			if l.contracts > 0 {
				avg = l.cost / l.contracts
			}
			qty := f.Contracts
			if qty > l.contracts {
				qty = l.contracts
			}
			if qty <= 1e-9 && l.contracts <= 1e-6 {
				// R89 (auditor bug 42): a duplicate settlement SELL against an already-flat lot
				// re-created the emptied lot and recorded a ZERO-QUANTITY round — realized 0 →
				// counted as a LOSS — deflating the very win-rates the placement policy and
				// probation gates consume. Nothing traded ⇒ no round.
				delete(lots, key)
				continue
			}
			l.realized += qty * (f.Price - avg)
			l.cost -= qty * avg
			l.contracts -= qty
			if l.contracts <= 1e-6 { // round closed
				record(l.platform, l.openSource, l.realized, l.fees)
				delete(lots, key)
			}
		default: // BUY
			if l.contracts <= 1e-6 && l.openSource == "" {
				l.openSource = f.Source
			}
			l.platform = f.Platform
			l.contracts += f.Contracts
			l.cost += f.Contracts * f.Price
		}
	}

	if res.ClosedTrades > 0 {
		res.WinRate = float64(res.Wins) / float64(res.ClosedTrades)
	}
	for _, l := range lots {
		if l.contracts > 1e-6 {
			res.OpenPositions++
		}
	}
	for _, ss := range bysrc {
		if ss.Closed > 0 {
			ss.WinRate = float64(ss.Wins) / float64(ss.Closed)
		}
		res.BySource = append(res.BySource, *ss)
	}
	sort.Slice(res.BySource, func(i, j int) bool { return res.BySource[i].Realized > res.BySource[j].Realized })
	return res
}

// ClosedTrade is one fully-closed round (a position opened and returned to flat),
// with entry/exit prices, the signal that OPENED it, the reason it was CLOSED, and
// the realized P&L. This is the per-bet history the gate learns from and the History
// tab shows ("what we've done, won/lost, why we got in, why we got out").
type ClosedTrade struct {
	Platform   string  `json:"platform"`
	Ticker     string  `json:"ticker"`
	Title      string  `json:"title"`
	Side       string  `json:"side"`
	Source     string  `json:"source"`      // signal/reason that OPENED the round
	ExitSource string  `json:"exit_source"` // why it CLOSED (auto-sl / auto-tp / gate / settled / manual)
	Contracts  float64 `json:"contracts"`   // total contracts traded in the round
	EntryPrice float64 `json:"entry_price"` // size-weighted avg entry (0..1)
	ExitPrice  float64 `json:"exit_price"`  // size-weighted avg exit (0..1)
	Realized   float64 `json:"realized"`
	Fees       float64 `json:"fees"`
	Win        bool    `json:"win"`
	OpenedAt   string  `json:"opened_at"`
	ClosedAt   string  `json:"closed_at"`
	TP         float64 `json:"tp"`
	SL         float64 `json:"sl"`
	Note       string  `json:"note"`      // entry note (context at open)
	ExitNote   string  `json:"exit_note"` // exit note (reason detail at close)
}

// ClosedTrades walks fills chronologically and emits one ClosedTrade each time a
// position returns to flat — the same round logic as Stats, but it keeps the full
// per-trade detail (entry/exit price, open + close reason, timestamps) instead of
// only aggregating. Newest first.
func ClosedTrades(fills []Fill) []ClosedTrade {
	sort.SliceStable(fills, func(i, j int) bool { return fills[i].TS < fills[j].TS })

	type round struct {
		contracts, cost                     float64 // open lot (avg-cost basis)
		boughtQty, boughtCost               float64 // cumulative buys → avg entry
		soldQty, soldProceeds               float64 // cumulative sells → avg exit
		realized, fees                      float64
		title, openSource, openTS, openNote string
		tp, sl                              float64
		exitSource, exitNote, exitTS        string
	}
	rounds := map[string]*round{}
	var out []ClosedTrade

	for _, f := range fills {
		key := f.Platform + "|" + f.Ticker + "|" + f.Side
		r := rounds[key]
		if r == nil {
			r = &round{}
			rounds[key] = r
		}
		if r.title == "" {
			r.title = f.Title
		}
		r.fees += f.Fee
		switch strings.ToUpper(f.Action) {
		case "SELL":
			avg := 0.0
			if r.contracts > 0 {
				avg = r.cost / r.contracts
			}
			qty := f.Contracts
			if qty > r.contracts {
				qty = r.contracts
			}
			if qty <= 1e-9 && r.contracts <= 1e-6 {
				// R89 (auditor bug 42): same phantom-round guard as Stats — a dup settle SELL on a
				// flat round must not emit a zero-contract ClosedTrade loss row into History.
				delete(rounds, key)
				continue
			}
			r.realized += qty * (f.Price - avg)
			r.cost -= qty * avg
			r.contracts -= qty
			r.soldQty += qty
			r.soldProceeds += qty * f.Price
			r.exitSource, r.exitNote, r.exitTS = f.Source, f.Note, f.TS
			if r.contracts <= 1e-6 { // round closed
				ct := ClosedTrade{
					Platform: f.Platform, Ticker: f.Ticker, Title: r.title, Side: f.Side,
					Source: r.openSource, ExitSource: r.exitSource,
					Contracts: r.soldQty, Realized: r.realized, Fees: r.fees,
					Win:      r.realized-r.fees > 0, // FEE-NET WIN (audit Q1 #4): winning means clearing fees too
					OpenedAt: r.openTS, ClosedAt: r.exitTS,
					TP: r.tp, SL: r.sl, Note: r.openNote, ExitNote: r.exitNote,
				}
				if r.boughtQty > 0 {
					ct.EntryPrice = r.boughtCost / r.boughtQty
				}
				if r.soldQty > 0 {
					ct.ExitPrice = r.soldProceeds / r.soldQty
				}
				if ct.Source == "" {
					ct.Source = "manual"
				}
				out = append(out, ct)
				delete(rounds, key)
			}
		default: // BUY
			if r.contracts <= 1e-6 && r.openSource == "" {
				r.openSource, r.openTS, r.openNote = f.Source, f.TS, f.Note
				r.tp, r.sl = f.TP, f.SL
			}
			r.contracts += f.Contracts
			r.cost += f.Contracts * f.Price
			r.boughtQty += f.Contracts
			r.boughtCost += f.Contracts * f.Price
		}
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].ClosedAt > out[j].ClosedAt }) // newest first
	return out
}

// RealizedSince returns FEE-NET realized P&L from fills whose timestamp is >= sinceTS
// (RFC3339), using the same average-cost basis as Aggregate. Used for the daily-loss
// readout and breaker. R89 (auditor bug 54): the old value was GROSS of fees while every
// other consumer (RealizedSeries, fee-net wins) nets them — the breaker undercounted losses.
func RealizedSince(fills []Fill, sinceTS string) float64 {
	sort.SliceStable(fills, func(i, j int) bool { return fills[i].TS < fills[j].TS })
	type acc struct{ contracts, cost float64 }
	m := map[string]*acc{}
	var realized float64
	for _, f := range fills {
		key := f.Platform + "|" + f.Ticker + "|" + f.Side
		a := m[key]
		if a == nil {
			a = &acc{}
			m[key] = a
		}
		if f.TS >= sinceTS {
			realized -= f.Fee // R89 (bug 54): fees inside the window are real losses
		}
		switch strings.ToUpper(f.Action) {
		case "SELL":
			avg := 0.0
			if a.contracts > 0 {
				avg = a.cost / a.contracts
			}
			qty := f.Contracts
			if qty > a.contracts {
				qty = a.contracts
			}
			r := qty * (f.Price - avg)
			a.cost -= qty * avg
			a.contracts -= qty
			if f.TS >= sinceTS {
				realized += r
			}
		default:
			a.contracts += f.Contracts
			a.cost += f.Contracts * f.Price
		}
	}
	return realized
}

// PnLPoint is one sample of cumulative NET P&L (realized minus fees) at a fill's timestamp.
type PnLPoint struct {
	TS  string  `json:"ts"`  // RFC3339 fill time
	PnL float64 `json:"pnl"` // cumulative realized − cumulative fees up to and including this fill
}

// RealizedSeries replays every fill in time order and emits the running NET P&L
// (cumulative realized minus cumulative fees) after each one — the data behind the P&L
// chart. Buys dip the curve by their fee; profitable sells step it up.
func RealizedSeries(fills []Fill) []PnLPoint {
	sort.SliceStable(fills, func(i, j int) bool { return fills[i].TS < fills[j].TS })
	type acc struct{ contracts, cost float64 }
	m := map[string]*acc{}
	var realized, fees float64
	out := make([]PnLPoint, 0, len(fills))
	for _, f := range fills {
		key := f.Platform + "|" + f.Ticker + "|" + f.Side
		a := m[key]
		if a == nil {
			a = &acc{}
			m[key] = a
		}
		fees += f.Fee
		if strings.EqualFold(f.Action, "SELL") {
			avg := 0.0
			if a.contracts > 0 {
				avg = a.cost / a.contracts
			}
			qty := f.Contracts
			if qty > a.contracts {
				qty = a.contracts
			}
			realized += qty * (f.Price - avg)
			a.cost -= qty * avg
			a.contracts -= qty
		} else {
			a.contracts += f.Contracts
			a.cost += f.Contracts * f.Price
		}
		out = append(out, PnLPoint{TS: f.TS, PnL: realized - fees})
	}
	return out
}
