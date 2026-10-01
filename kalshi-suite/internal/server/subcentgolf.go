package server

// Prospective sub-cent golf execution cohort. The scanner prefers already-subscribed Kalshi depth
// books and fills only its bounded missing set with one authenticated batch-depth GET. Taker rows
// are one-contract ask counterfactuals; maker rows share the tape/queue simulator but route only
// back to this research ledger. Nothing here calls a portfolio placement hook, a venue order
// endpoint, or the live-mirror queue.

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	subcentGolfMaxMarketsPerSweep = 100
	subcentGolfDepthPriorityCap   = 64
	subcentGolfSettleRESTCap      = 8
)

type subcentGolfFunnel struct {
	CycleID, StartedAt, CompletedAt                        string
	Universe, Golf, Open, FineGrid, CatalogSubcentTouches  int
	BooksRequested, WSBooks, RESTBooks, FreshCompleteBooks int
	VerifiedSubcentRoutes, ExactFeeRoutes                  int
	InsertedRows, DuplicateRows                            int
	Exclusions                                             map[string]int
}

type subcentGolfRuntime struct {
	mu       sync.Mutex
	settleMu sync.Mutex // R142: collection sweep and settlement-only lane never duplicate REST checks
	latest   subcentGolfFunnel
}

var subcentGolfRuntimes sync.Map // *Server -> *subcentGolfRuntime

func (s *Server) subcentGolfRuntime() *subcentGolfRuntime {
	v, _ := subcentGolfRuntimes.LoadOrStore(s, &subcentGolfRuntime{})
	return v.(*subcentGolfRuntime)
}

func cloneCountMap(src map[string]int) map[string]int {
	out := make(map[string]int, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

func (s *Server) setSubcentGolfFunnel(f subcentGolfFunnel) {
	rt := s.subcentGolfRuntime()
	rt.mu.Lock()
	f.Exclusions = cloneCountMap(f.Exclusions)
	rt.latest = f
	rt.mu.Unlock()
}

func (s *Server) latestSubcentGolfFunnel() subcentGolfFunnel {
	rt := s.subcentGolfRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	f := rt.latest
	f.Exclusions = cloneCountMap(f.Exclusions)
	return f
}

var (
	sgWithdrawnRE = regexp.MustCompile(`(?i)(^|[^a-z])(withdrawn|withdrew|withdrawal|wd)([^a-z]|$)`)
	sgCutRE       = regexp.MustCompile(`(?i)(miss(ed)? (the )?cut|did not make (the )?cut|cut from)`)
	sgBareCutRE   = regexp.MustCompile(`(?i)(^|[^a-z])cut([^a-z]|$)`)
	sgElimRE      = regexp.MustCompile(`(?i)(^|[^a-z])(eliminated|disqualified|dq)([^a-z]|$)`)
	sgActiveRE    = regexp.MustCompile(`(?i)(^|[^a-z])active([^a-z]|$)`)
	sgLiveRE      = regexp.MustCompile(`(?i)(^|[^a-z])(live|in[- ]play)([^a-z]|$)`)
	sgPreRE       = regexp.MustCompile(`(?i)(pre[- ]event|before (the )?(event|tournament)|not yet started)`)
	sgWinRE       = regexp.MustCompile(`(?i)(^|[^a-z])(win|winner)([^a-z]|$)`)
)

func subcentGolfMarket(m kalshi.Market) bool {
	u := strings.ToUpper(m.Ticker + " " + m.EventTicker)
	return strings.Contains(u, "GOLF") || strings.Contains(u, "PGA") || strings.Contains(u, "LPGA")
}

func subcentGolfActive(m kalshi.Market, now time.Time) bool {
	if m.Ticker == "" || m.Result != "" {
		return false
	}
	if st := strings.TrimSpace(m.Status); st != "" && !strings.EqualFold(st, "active") && !strings.EqualFold(st, "open") {
		return false
	}
	// expected_expiration_time is an estimate of when the underlying outcome should be known, not
	// the trading close. Golf markets routinely remain status=active with valid books after it.
	// Treating the estimate as a hard close silently removed exactly the cut/eliminated tail this
	// prospective cohort is meant to measure. Venue status plus close_time is the tradability gate.
	if closeAt := strings.TrimSpace(m.CloseTime); closeAt != "" {
		at, err := time.Parse(time.RFC3339, closeAt)
		if err != nil || !at.After(now) {
			return false
		}
	}
	return true
}

func subcentGolfHasFineGrid(m kalshi.Market) bool {
	t, ok := m.TickForKnown(0.005)
	return ok && t > 0 && t < 0.01
}

func subcentGolfMarketType(m kalshi.Market) string {
	s := strings.ToLower(m.Ticker + " " + m.EventTicker + " " + m.Title + " " + m.YesSubTitle + " " + m.NoSubTitle)
	switch {
	case strings.Contains(s, "round leader") || strings.Contains(s, "leader after") || strings.Contains(s, "r1leader") || strings.Contains(s, "r2leader") || strings.Contains(s, "r3leader") || strings.Contains(s, "r4leader"):
		return "round_leader"
	case strings.Contains(s, "head-to-head") || strings.Contains(s, "head to head") || strings.Contains(s, "matchup") || strings.Contains(s, "h2h") || strings.Contains(s, " vs ") || strings.Contains(s, " versus "):
		return "matchup"
	case sgWinRE.MatchString(s) || strings.HasPrefix(strings.ToUpper(m.Ticker), "KXPGATOUR-") || strings.HasPrefix(strings.ToUpper(m.Ticker), "KXLPGATOUR-"):
		return "winner"
	default:
		return "prop"
	}
}

func subcentGolfPlayer(m kalshi.Market) string {
	if p := strings.TrimSpace(m.YesSubTitle); p != "" && !strings.EqualFold(p, "yes") {
		return p
	}
	t := strings.TrimSpace(m.Title)
	l := strings.ToLower(t)
	if i := strings.Index(l, "will "); i >= 0 {
		start := i + len("will ")
		for _, endToken := range []string{" win", " make", " finish", " be ", " score"} {
			if j := strings.Index(l[start:], endToken); j > 0 {
				return strings.TrimSpace(t[start : start+j])
			}
		}
	}
	return ""
}

func subcentGolfPlayerStatus(m kalshi.Market) (status, evidence string) {
	// Player status belongs to the YES player.  Never let an opponent annotation in no_sub_title
	// label the wrong person.  A one-player "Will X ..." title is safe fallback evidence; a generic
	// matchup title without a YES subtitle stays unknown.
	s := strings.TrimSpace(m.YesSubTitle)
	if s == "" && strings.Contains(strings.ToLower(m.Title), "will ") {
		s = m.Title
	}
	switch {
	case sgWithdrawnRE.MatchString(s):
		return "withdrawn", "explicit market text"
	case sgCutRE.MatchString(s) || (m.YesSubTitle != "" && sgBareCutRE.MatchString(s)):
		return "cut", "explicit market text"
	case sgElimRE.MatchString(s):
		return "eliminated", "explicit market text"
	case sgActiveRE.MatchString(s):
		return "active", "explicit market text"
	default:
		return "unknown", "no authoritative player-status field"
	}
}

func subcentGolfPhase(m kalshi.Market, now time.Time) (phase, evidence string) {
	s := strings.Join([]string{m.Title, m.YesSubTitle, m.NoSubTitle}, " ")
	if sgLiveRE.MatchString(s) {
		return "live", "explicit market text"
	}
	if exp := strings.TrimSpace(m.ExpectedExpiration); exp != "" {
		if at, err := time.Parse(time.RFC3339, exp); err == nil && !now.Before(at) {
			return "live", "expected event expiration reached while venue market remains active"
		}
	}
	if sgPreRE.MatchString(s) {
		return "pre_event", "explicit market text"
	}
	if start, ok := kalshiTickerStartAt(m.Ticker, now); ok {
		if now.Before(start) {
			return "pre_event", "ticker-embedded event start"
		}
		return "live", "ticker-embedded event start"
	}
	return "unknown", "Kalshi market schema has no event-start field"
}

func subcentGolfCatalogTouch(m kalshi.Market) bool {
	yesAsk, yesDepth := m.YesAsk.Float(), m.YesAskSize.Float()
	// Kalshi's market schema publishes yes_bid_size_fp and yes_ask_size_fp. A NO ask is the
	// complement of a YES bid and therefore has the same size as yes_bid_size_fp.
	noAsk, noDepth := m.NoAsk.Float(), m.YesBidSize.Float()
	return (yesAsk > 0 && yesAsk < .01 && yesDepth >= 1) ||
		(noAsk > 0 && noAsk < .01 && noDepth >= 1)
}

type subcentGolfTouch struct {
	Side               string
	Bid, Ask           float64
	BidDepth, AskDepth float64
}

func subcentGolfTouches(ob *kalshi.Orderbook) []subcentGolfTouch {
	if ob == nil {
		return nil
	}
	yes := subcentGolfTouch{Side: "YES"}
	no := subcentGolfTouch{Side: "NO"}
	if len(ob.YesBids) > 0 {
		yes.Bid, yes.BidDepth = ob.YesBids[0].Price, ob.YesBids[0].Size
		no.Ask, no.AskDepth = 1-ob.YesBids[0].Price, ob.YesBids[0].Size
	}
	if len(ob.YesAsks) > 0 {
		yes.Ask, yes.AskDepth = ob.YesAsks[0].Price, ob.YesAsks[0].Size
		no.Bid, no.BidDepth = 1-ob.YesAsks[0].Price, ob.YesAsks[0].Size
	}
	return []subcentGolfTouch{yes, no}
}

// subcentGolfDepthPriority picks a bounded set for the full-depth planner.  It is downstream of
// held/live/near-horizon money and existing strategy dependencies, so research can only use spare
// capacity.  The scanner still observes any other golf book already selected for another reason.
func subcentGolfDepthPriority(markets map[string]kalshi.Market, now time.Time, max int) []string {
	type row struct {
		ticker string
		close  time.Time
		volume float64
	}
	var rows []row
	for ticker, m := range markets {
		if !subcentGolfMarket(m) || !subcentGolfActive(m, now) || !subcentGolfHasFineGrid(m) {
			continue
		}
		closeAt, _ := parseTime(firstNonEmpty(m.ExpectedExpiration, m.CloseTime))
		rows = append(rows, row{ticker: ticker, close: closeAt, volume: m.Volume24h.Float()})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].close.IsZero() != rows[j].close.IsZero() {
			return !rows[i].close.IsZero()
		}
		if !rows[i].close.Equal(rows[j].close) {
			return rows[i].close.Before(rows[j].close)
		}
		if rows[i].volume != rows[j].volume {
			return rows[i].volume > rows[j].volume
		}
		return rows[i].ticker < rows[j].ticker
	})
	if max > 0 && len(rows) > max {
		rows = rows[:max]
	}
	out := make([]string, len(rows))
	for i := range rows {
		out[i] = rows[i].ticker
	}
	return out
}

func sgFeeParts(net float64) (fee, rebate float64) {
	if net < 0 {
		return 0, -net
	}
	return net, 0
}

func (s *Server) subcentGolfMakerPost(ctx context.Context, trialID int64, m kalshi.Market, touch subcentGolfTouch, feeSource string) bool {
	// Research posts have their own dedup namespace and never block a portfolio maker on the same
	// market.  Conversely, a portfolio post does not prevent this independent observation.
	if s.pendingBookHasTicker("subcent-golf", m.Ticker) {
		_ = s.store.MarkSubcentGolfTrialBlocked(ctx, trialID, "research-post-already-resting")
		return false
	}
	// Keep this cohort out of maker_fill_stats. That table calibrates funded paper/LIVE maker
	// routing, fee blends and adverse-selection guards; a high-volume research sampler would
	// otherwise change money policy simply by observing Golf. A negative, cohort-local simulation id
	// links the pending engine back to subcent_golf_trials and cannot collide with a real attempt id.
	simID := -trialID
	qa, qk := s.queueAheadAt("kalshi", m.Ticker, touch.Side, touch.Bid)
	if !qk && touch.Bid > 0 && touch.BidDepth >= 0 {
		// The batch endpoint is a complete depth snapshot. A newly posted order joins behind every
		// visible contract at its price, so the best-bid level size is the exact observed queue-ahead
		// bound even when this ticker has no full-depth WS seat.
		qa, qk = touch.BidDepth, true
	}
	var qap *float64
	if qk {
		q := qa
		qap = &q
	}
	if err := s.store.AttachSubcentGolfMaker(ctx, trialID, simID, qap); err != nil {
		_ = s.store.MarkSubcentGolfTrialBlocked(ctx, trialID, "cohort-link-failed")
		return false
	}
	s.pendMu.Lock()
	s.pendingMakers = append(s.pendingMakers, &pendingMaker{
		id: simID, platform: "kalshi", ticker: m.Ticker, title: m.Title, side: touch.Side,
		source: "subcent-golf", note: "research-only prospective cohort", px: touch.Bid,
		contracts: 1, postedAt: time.Now(), book: "subcent-golf",
		queueAhead: qa, queueLeft: qa, queueKnown: qk, tapeCutoff: time.Now(),
		routeTag: "research-only;" + feeSource,
	})
	s.pendMu.Unlock()
	return true
}

func (s *Server) subcentGolfMakerFill(ctx context.Context, p *pendingMaker) string {
	fee, known, source := s.kalFeeExact(p.ticker, true, p.contracts, p.px)
	if !known {
		return "fee-unknown"
	}
	if err := s.store.MarkSubcentGolfMakerFilled(ctx, p.id, p.px, fee, source, pendingFillRule(p)); err != nil {
		return "io"
	}
	return ""
}

// settleSubcentGolf guarantees a cohort ticker can grade even when no ordinary signal or position
// ever referenced it. Cached final markets cost no request. A REST check is reserved for a ticker
// that disappeared from the complete current board or is cached as ended/non-active, capped at
// eight per pass and rotated on the settlement lane's 90-second cadence so a provider-lagged row
// cannot starve the rest. The ten-minute collector may call this too; settleMu prevents overlap.
func (s *Server) settleSubcentGolf(ctx context.Context, markets map[string]kalshi.Market, now time.Time) {
	rt := s.subcentGolfRuntime()
	rt.settleMu.Lock()
	defer rt.settleMu.Unlock()
	if len(markets) == 0 { // cold board: do not turn startup into a settlement REST burst
		return
	}
	open, err := s.store.OpenSubcentGolfTickers(ctx, 1000)
	if err != nil || len(open) == 0 {
		return
	}
	off := int(now.Unix()/90) % len(open)
	restUsed := 0
	for i := 0; i < len(open) && ctx.Err() == nil; i++ {
		ticker := open[(off+i)%len(open)]
		m, cached := markets[ticker]
		if cached {
			if sv := m.SettledYes(); sv >= 0 {
				_ = s.store.ResolveSubcentGolfTicker(ctx, ticker, sv)
				continue
			}
			if subcentGolfActive(m, now) {
				continue // still open according to the cached authoritative lifecycle
			}
		}
		if restUsed >= subcentGolfSettleRESTCap {
			continue
		}
		restUsed++
		m, err := s.kal.GetMarket(ctx, ticker)
		if err != nil {
			continue
		}
		if sv := m.SettledYes(); sv >= 0 {
			_ = s.store.ResolveSubcentGolfTicker(ctx, ticker, sv)
		}
	}
}

func (s *Server) subcentGolfSweep(ctx context.Context) {
	if s.kal == nil || s.store == nil {
		return
	}
	started := time.Now().UTC()
	funnel := subcentGolfFunnel{
		CycleID:   started.Truncate(10 * time.Minute).Format(time.RFC3339),
		StartedAt: started.Format(time.RFC3339Nano), Exclusions: map[string]int{},
	}
	_, _ = s.store.ExpireSubcentGolfOrphans(ctx, 15*time.Minute)
	now := started
	board, _ := s.kal.TopLiquidCached()
	s.metaMu.Lock()
	warm := make(map[string]kalshi.Market, len(s.kmkts))
	for ticker, m := range s.kmkts {
		warm[ticker] = m
	}
	s.metaMu.Unlock()
	markets := completeKalshiBookMarkets(board, warm)
	funnel.Universe = len(markets)
	s.settleSubcentGolf(ctx, markets, now)
	ids := make([]string, 0, len(markets))
	for ticker, m := range markets {
		if !subcentGolfMarket(m) {
			continue
		}
		funnel.Golf++
		if !subcentGolfActive(m, now) {
			funnel.Exclusions["market_not_open"]++
			continue
		}
		funnel.Open++
		if !subcentGolfHasFineGrid(m) {
			funnel.Exclusions["not_fine_grid"]++
			continue
		}
		funnel.FineGrid++
		if !subcentGolfCatalogTouch(m) {
			funnel.Exclusions["no_subcent_catalog_bbo"]++
			continue
		}
		funnel.CatalogSubcentTouches++
		ids = append(ids, ticker)
	}
	sort.Strings(ids)
	// Rotate the deterministic catalog every cycle so a >100-book cohort does not permanently
	// select the same lexical player prefix. The venue's authenticated batch-depth endpoint accepts
	// at most 100 tickers, so this is one bounded GET rather than a per-market request flood.
	if len(ids) > 0 {
		off := (now.YearDay()*24*6 + now.Hour()*6 + now.Minute()/10) % len(ids)
		ids = append(append([]string(nil), ids[off:]...), ids[:off]...)
	}
	if len(ids) > subcentGolfMaxMarketsPerSweep {
		funnel.Exclusions["bounded_batch_rotation"] += len(ids) - subcentGolfMaxMarketsPerSweep
		ids = ids[:subcentGolfMaxMarketsPerSweep]
	}
	funnel.BooksRequested = len(ids)
	type bookProof struct {
		book   *kalshi.Orderbook
		age    time.Duration
		source string
	}
	proofs := make(map[string]bookProof, len(ids))
	missing := make([]string, 0, len(ids))
	for _, ticker := range ids {
		ob, age, ok := s.kal.LiveBook(ticker, 5*time.Second)
		if ok && ob != nil {
			proofs[ticker] = bookProof{book: ob, age: age, source: "kalshi-ws-depth"}
			funnel.WSBooks++
			continue
		}
		missing = append(missing, ticker)
	}
	batchErrorClass, batchErrorText := "", ""
	if len(missing) > 0 && ctx.Err() == nil {
		requestedAt := time.Now()
		books, err := s.kal.GetOrderbooks(ctx, missing)
		age := time.Since(requestedAt)
		if err != nil {
			batchErrorClass, batchErrorText = subcentGolfSafeError(err)
			funnel.Exclusions[batchErrorClass] += len(missing)
		} else if age > 5*time.Second {
			batchErrorClass, batchErrorText = "batch_depth_stale", "batch depth response exceeded the five-second freshness envelope"
			funnel.Exclusions[batchErrorClass] += len(missing)
		} else {
			for _, ticker := range missing {
				ob := books[ticker]
				if ob == nil {
					funnel.Exclusions["batch_depth_missing_ticker"]++
					continue
				}
				proofs[ticker] = bookProof{book: ob, age: age, source: "kalshi-rest-batch-depth"}
				funnel.RESTBooks++
			}
		}
	}
	funnel.FreshCompleteBooks = len(proofs)
	routesInserted, insertErrors := 0, 0
	for _, ticker := range ids {
		if ctx.Err() != nil {
			funnel.Exclusions["cycle_context_done"]++
			break
		}
		proof, ok := proofs[ticker]
		if !ok || proof.book == nil {
			continue
		}
		m := markets[ticker]
		phase, phaseEvidence := subcentGolfPhase(m, now)
		playerStatus, statusEvidence := subcentGolfPlayerStatus(m)
		marketType, player := subcentGolfMarketType(m), subcentGolfPlayer(m)
		marketHadTouch := false
		for _, touch := range subcentGolfTouches(proof.book) {
			if touch.Ask <= 0 || touch.Ask >= 0.01 || touch.AskDepth < 1 || touch.Bid < 0 || touch.Bid >= touch.Ask {
				continue
			}
			funnel.VerifiedSubcentRoutes++
			marketHadTouch = true
			takerTick, tickKnown := m.TickForKnown(touch.Ask)
			if !tickKnown || takerTick <= 0 || takerTick >= 0.01 {
				funnel.Exclusions["taker_tick_unknown_or_not_fine"]++
				continue
			}
			takerFee, feeKnown, takerFeeSource := s.kalFeeExact(ticker, false, 1, touch.Ask)
			if !feeKnown {
				funnel.Exclusions["taker_fee_unknown"]++
				continue
			}
			funnel.ExactFeeRoutes++
			base := storage.SubcentGolfTrial{Slot: now.Format("2006-01-02T15"), Platform: "kalshi",
				Ticker: ticker, EventTicker: m.EventTicker, Title: m.Title, Player: player, Side: touch.Side,
				Phase: phase, PhaseEvidence: phaseEvidence, PlayerStatus: playerStatus, StatusEvidence: statusEvidence,
				MarketType: marketType, BookSource: proof.source, QuoteAgeS: proof.age.Seconds(),
				TickSize: takerTick, BidPx: touch.Bid, AskPx: touch.Ask,
				BidDepth: touch.BidDepth, AskDepth: touch.AskDepth}

			fee, rebate := sgFeeParts(takerFee)
			taker := base
			taker.Route, taker.FillState, taker.FillPrice = "taker", "filled", touch.Ask
			taker.FeePC, taker.RebatePC, taker.FeeSource = fee, rebate, takerFeeSource
			if _, inserted, err := s.store.InsertSubcentGolfTrial(ctx, taker); err != nil {
				insertErrors++
				funnel.Exclusions["taker_insert_error"]++
			} else if inserted {
				routesInserted++
			} else {
				funnel.DuplicateRows++
			}

			maker := base
			maker.Route, maker.FillState = "maker", "resting"
			makerTick, makerTickKnown := m.TickForKnown(touch.Bid)
			switch {
			case touch.Bid <= 0 && touch.Ask <= takerTick+1e-9:
				maker.FillState, maker.CancelReason = "not_quoteable", "venue-minimum-ask-has-no-lower-maker-tick"
				funnel.Exclusions["maker_not_quoteable_at_venue_minimum"]++
			case touch.Bid <= 0:
				maker.FillState, maker.CancelReason = "blocked", "no-external-bid-reference-for-honest-maker-sim"
				funnel.Exclusions["maker_no_external_bid"]++
			case !makerTickKnown || makerTick <= 0:
				maker.FillState, maker.CancelReason = "blocked", "maker-tick-unknown"
				funnel.Exclusions["maker_tick_unknown"]++
			default:
				maker.TickSize = makerTick
				_, known, source := s.kalFeeExact(ticker, true, 1, touch.Bid)
				maker.FeeSource = source
				if !known {
					maker.FillState, maker.CancelReason = "blocked", "maker-fee-unknown"
					funnel.Exclusions["maker_fee_unknown"]++
				} else {
					funnel.ExactFeeRoutes++
				}
			}
			trialID, inserted, err := s.store.InsertSubcentGolfTrial(ctx, maker)
			if err != nil {
				insertErrors++
				funnel.Exclusions["maker_insert_error"]++
			} else if inserted {
				routesInserted++
				if maker.FillState == "resting" {
					s.subcentGolfMakerPost(ctx, trialID, m, touch, maker.FeeSource)
				}
			} else {
				funnel.DuplicateRows++
			}
		}
		if !marketHadTouch {
			funnel.Exclusions["catalog_touch_not_verified_in_fresh_depth"]++
		}
	}
	funnel.InsertedRows = routesInserted
	funnel.CompletedAt = time.Now().UTC().Format(time.RFC3339Nano)
	s.setSubcentGolfFunnel(funnel)
	status, expectedZero, zeroReason := "healthy", false, ""
	errorClass, errorText := "", ""
	switch {
	case insertErrors > 0:
		status, errorClass, errorText = "error", "storage_insert", "one or more subcent-golf research rows failed to persist"
	case batchErrorClass != "":
		status, errorClass, errorText = "blocked", batchErrorClass, batchErrorText
	case funnel.CatalogSubcentTouches == 0:
		status, expectedZero, zeroReason = "healthy_empty", true, "no open fine-grid golf market had a sub-cent catalog BBO touch"
	case funnel.FreshCompleteBooks == 0:
		status, errorClass, errorText = "starved", "no_fresh_complete_book", "sub-cent candidates had no current complete depth proof"
	case funnel.ExactFeeRoutes == 0:
		status, errorClass, errorText = "blocked", "no_exact_fee_route", "fresh sub-cent books existed but no route had exact fee authority"
	case routesInserted == 0:
		status, expectedZero = "healthy_empty", true
		if funnel.DuplicateRows > 0 {
			zeroReason = "all current route observations already exist in the immutable hourly slot"
		} else {
			zeroReason = "catalog sub-cent touches disappeared or became non-executable in fresh depth"
		}
	}
	metrics := map[string]any{
		"universe": funnel.Universe, "golf": funnel.Golf, "open": funnel.Open,
		"fine_grid": funnel.FineGrid, "subcent_touch": funnel.CatalogSubcentTouches,
		"books_requested": funnel.BooksRequested, "ws_books": funnel.WSBooks,
		"rest_batch_books": funnel.RESTBooks, "fresh_complete_book": funnel.FreshCompleteBooks,
		"verified_subcent_routes": funnel.VerifiedSubcentRoutes,
		"exact_fee_routes":        funnel.ExactFeeRoutes, "inserted_rows": funnel.InsertedRows,
		"duplicate_rows": funnel.DuplicateRows, "research_only": true,
	}
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	_, _ = s.store.InsertCollectorReceipt(wctx, storage.CollectorReceipt{
		CollectorID: "subcent-golf", CycleID: funnel.CycleID,
		ExperimentID: "outcome-set-expansion-shock", ExperimentVersion: 1,
		Status: status, Started: started, Completed: time.Now(), Eligible: funnel.CatalogSubcentTouches,
		Attempted: funnel.BooksRequested, Inserted: routesInserted, Duplicates: funnel.DuplicateRows,
		ExpectedZero: expectedZero, ZeroReason: zeroReason, ErrorClass: errorClass, ErrorText: errorText,
		Source:        "Kalshi market metadata + dynamic price ranges + current depth",
		SchemaVersion: "subcent-golf-v1", ExpectedCadence: 10 * time.Minute,
		Exclusions: funnel.Exclusions, Metrics: metrics,
		Systems: []string{"outcome-set-expansion-shock", "replenishment-fingerprint"},
	})
	cancel()
	if routesInserted > 0 && s.log != nil {
		s.log.Info("subcent-golf prospective cohort sampled", "golf", funnel.Golf,
			"fine_grid", funnel.FineGrid, "subcent_touch", funnel.CatalogSubcentTouches,
			"fresh_books", funnel.FreshCompleteBooks, "routes", routesInserted, "research_only", true)
	}
}

func subcentGolfSafeError(err error) (class, safe string) {
	if err == nil {
		return "", ""
	}
	msg := strings.ToLower(err.Error())
	if i := strings.Index(msg, "status "); i >= 0 && len(msg) >= i+10 {
		code := msg[i+7 : i+10]
		return "batch_depth_http_" + code, "batch depth venue response HTTP " + code
	}
	if strings.Contains(msg, "deadline") || strings.Contains(msg, "context canceled") {
		return "batch_depth_timeout", "batch depth request timed out or was canceled"
	}
	return "batch_depth_transport_or_decode", fmt.Sprintf("batch depth transport or response decode failed (%T)", err)
}

func (s *Server) handleSubcentGolf(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.SubcentGolfReport(r.Context(), 100)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, struct {
		storage.SubcentGolfReport
		CollectorLiveness  subcentGolfFunnel `json:"collector_liveness"`
		Funded             bool              `json:"funded"`
		ExecutionAuthority bool              `json:"execution_authority"`
		PromotionBlocked   bool              `json:"promotion_blocked"`
	}{SubcentGolfReport: report, CollectorLiveness: s.latestSubcentGolfFunnel(),
		Funded: false, ExecutionAuthority: false, PromotionBlocked: true})
}
