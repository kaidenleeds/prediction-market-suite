package server

// Concrete prospective microstructure systems. Every path in this file is read-only at the venues and
// writes only research_* SQLite tables. There are deliberately no calls to autoPlace,
// placePaperOrder, CreateOrder, arm, live mirrors, or unit-trial promotion.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const lifecyclePersistenceMaxAttempts = 8

func (s *Server) researchKalshiBBO(ctx context.Context, ticker string, restFallback bool) (storage.LifecycleBook, bool) {
	if s.kal == nil || ticker == "" {
		return storage.LifecycleBook{}, false
	}
	if ob, age, ok := s.kal.LiveBook(ticker, 5*time.Second); ok && ob != nil &&
		len(ob.YesBids) > 0 && len(ob.YesAsks) > 0 && ob.YesBids[0].Size >= 0 && ob.YesAsks[0].Size >= 0 {
		return storage.LifecycleBook{Bid: ob.YesBids[0].Price, Ask: ob.YesAsks[0].Price,
			BidDepth: ob.YesBids[0].Size, AskDepth: ob.YesAsks[0].Size, Source: "kalshi_book_ws", Age: age.Seconds()}, true
	}
	if !restFallback {
		return storage.LifecycleBook{}, false
	}
	ob, err := s.kal.GetOrderbook(ctx, ticker)
	if err != nil || ob == nil || len(ob.YesBids) == 0 || len(ob.YesAsks) == 0 {
		return storage.LifecycleBook{}, false
	}
	return storage.LifecycleBook{Bid: ob.YesBids[0].Price, Ask: ob.YesAsks[0].Price,
		BidDepth: ob.YesBids[0].Size, AskDepth: ob.YesAsks[0].Size, Source: "kalshi_orderbook_rest"}, true
}

// queueVisibleEstimate maps every order shape onto the underlying YES/NO bid ladder. The visible
// level includes our own remainder, so subtracting it yields an upper bound on contracts ahead;
// same-price orders posted later are indistinguishable and make this telemetry, not a fill model.
func (s *Server) queueVisibleEstimate(o kalshi.Order) (level, ahead, age float64, ok bool) {
	noLadder, price, ok := queueLadderRef(o)
	if !ok {
		return 0, 0, 0, false
	}
	level, ok = s.kal.BookLevelSize(o.Ticker, noLadder, price, 5*time.Second)
	if !ok {
		return 0, 0, 0, false
	}
	if _, a, live := s.kal.LiveBook(o.Ticker, 5*time.Second); live {
		age = a.Seconds()
	}
	ahead = math.Max(0, level-o.Remaining())
	return level, ahead, age, true
}

func queueLadderRef(o kalshi.Order) (noLadder bool, price float64, ok bool) {
	side := strings.ToLower(strings.TrimSpace(o.OutcomeSide))
	bookSide := strings.ToLower(strings.TrimSpace(o.BookSide))
	if side != "" && bookSide != "" {
		if (side == "yes") != (bookSide == "bid") {
			return false, 0, false // canonical fields encode the same direction bit
		}
	} else if side == "" && bookSide != "" {
		if bookSide == "bid" {
			side = "yes"
		} else if bookSide == "ask" {
			side = "no"
		}
	} else if side == "" { // legacy action+side: sell flips the owned outcome
		side = strings.ToLower(strings.TrimSpace(o.Side))
		if strings.EqualFold(o.Action, "sell") {
			if side == "yes" {
				side = "no"
			} else if side == "no" {
				side = "yes"
			}
		}
	}
	switch side {
	case "yes":
		price = o.YesPrice()
		if price <= 0 || price >= 1 {
			price = 1 - o.NoPrice()
		}
	case "no":
		noLadder, price = true, o.NoPrice()
		if price <= 0 || price >= 1 {
			price = 1 - o.YesPrice()
		}
	default:
		return false, 0, false
	}
	if price <= 0 || price >= 1 {
		return false, 0, false
	}
	return noLadder, price, true
}

// kalshiNoAskTouch uses the official current Market schema: NO ask is the complement of the YES
// bid and has the same quantity as yes_bid_size_fp. There is no no_ask_size_fp field. If the
// redundant no_ask_dollars field is supplied, it must agree rather than silently choosing one.
func kalshiNoAskTouch(m kalshi.Market) (ask, depth float64, ok bool) {
	yesBid := m.YesBid.Float()
	depth = m.YesBidSize.Float()
	if yesBid <= 0 || yesBid >= 1 || depth < 1 {
		return 0, 0, false
	}
	ask = 1 - yesBid
	if explicit := m.NoAsk.Float(); explicit > 0 && math.Abs(explicit-ask) > 1e-9 {
		return 0, 0, false
	}
	return ask, depth, true
}

func (s *Server) sweepQueuePriority(ctx context.Context) {
	started := time.Now()
	receipt := storage.CollectorReceipt{
		CollectorID: "queue-priority", CycleID: collectorCycleID(started),
		ExperimentID: "replenishment-fingerprint", ExperimentVersion: 1,
		Status: "healthy", Source: queueCollectorSource, SchemaVersion: "kalshi-queue-v1",
		Started: started, ExpectedCadence: time.Minute,
		Exclusions: make(map[string]int), Metrics: make(map[string]any),
		Systems: []string{"replenishment-fingerprint", "maker-salvage-matched-cohort"},
	}
	defer func() {
		receipt.Completed = time.Now()
		if _, err := s.store.InsertCollectorReceipt(ctx, receipt); err != nil {
			s.log.Warn("queue-priority collector receipt failed", "err", err)
		}
	}()
	exclude := func(reason string, n int) {
		if n > 0 {
			receipt.Exclusions[reason] += n
		}
	}
	setError := func(class, text string) {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", class, text
	}
	if s.kal == nil || !s.kal.HasCredentials() {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "blocked", "authentication", "Kalshi credentials unavailable"
		receipt.ZeroReason = "authenticated resting-order read could not run"
		return
	}
	orders, err := s.kal.GetOrders(ctx)
	if err != nil {
		setError(collectorErrorClass(err), err.Error())
		receipt.ZeroReason = "resting-order source failed"
		s.log.Warn("queue-priority resting-order read failed", "err", err)
		return
	}
	receipt.Metrics["resting_order_rows"] = len(orders)
	tickers := make([]string, 0, len(orders))
	active := make(map[string]bool, len(orders))
	byID := make(map[string]kalshi.Order, len(orders))
	for _, o := range orders {
		if o.OrderID == "" || o.Ticker == "" {
			exclude("malformed_resting_order", 1)
			setError("schema", "one or more resting orders lacked order_id or ticker")
			continue
		}
		active[o.OrderID], byID[o.OrderID] = true, o
		tickers = append(tickers, o.Ticker)
	}
	receipt.Eligible = len(byID)
	receipt.Metrics["natural_resting_orders"] = len(byID)
	var positions []kalshi.QueuePosition
	if len(byID) == 0 && receipt.Status != "error" {
		receipt.Status, receipt.ExpectedZero = "healthy_empty", true
		receipt.ZeroReason = "no naturally resting real Kalshi orders"
	} else if len(byID) > 0 {
		receipt.Attempted = len(byID)
		positions, err = s.kal.GetQueuePositions(ctx, tickers)
		if err != nil {
			setError(collectorErrorClass(err), err.Error())
			receipt.ZeroReason = "official queue-position source failed"
			s.log.Warn("queue-priority official queue read failed", "err", err)
			return // never replace true queue with the visible approximation
		}
	}
	receipt.Metrics["official_queue_positions"] = len(positions)
	now := time.Now()
	sampled := make(map[string]bool, len(positions))
	visibleKnown := 0
	for _, q := range positions {
		o, ok := byID[q.OrderID]
		if !ok {
			exclude("queue_position_unknown_order", 1)
			continue
		}
		if q.MarketTicker != o.Ticker {
			exclude("queue_position_ticker_mismatch", 1)
			setError("schema", "official queue position ticker did not match its resting order")
			continue
		}
		if q.Position() < 0 {
			exclude("negative_queue_position", 1)
			continue
		}
		sampled[q.OrderID] = true
		sample := storage.QueueResearchSample{Observed: now, TrueQueue: q.Position(), Remaining: o.Remaining()}
		if level, ahead, age, known := s.queueVisibleEstimate(o); known {
			visibleKnown++
			errV := ahead - q.Position()
			sample.VisibleLevel, sample.VisibleAhead, sample.Error, sample.BookAge = &level, &ahead, &errV, &age
		} else {
			exclude("fresh_visible_book_unavailable", 1)
		}
		side := o.OutcomeSide
		if side == "" {
			side = o.Side
		}
		price := o.YesPrice()
		if strings.EqualFold(side, "no") {
			price = o.NoPrice()
		}
		inserted, insertErr := s.store.InsertQueueResearchSampleResult(ctx, storage.QueueResearchOrder{
			OrderID: o.OrderID, Ticker: o.Ticker, OutcomeSide: strings.ToUpper(side), BookSide: o.BookSide,
			Action: o.Action, Price: price, Initial: o.Initial(), Remaining: o.Remaining(), Filled: o.Filled(),
			VenueCreated: o.CreatedTime,
		}, sample)
		if insertErr != nil {
			exclude("queue_sample_storage_error", 1)
			setError(collectorErrorClass(insertErr), insertErr.Error())
			continue
		}
		if inserted {
			receipt.Inserted++
		} else {
			receipt.Duplicates++
		}
	}
	for orderID := range byID {
		if !sampled[orderID] {
			exclude("resting_order_missing_official_queue_position", 1)
		}
	}
	receipt.Metrics["visible_estimates_known"] = visibleKnown
	if len(byID) > 0 && len(sampled) == 0 && receipt.Status != "error" {
		receipt.Status = "starved"
		receipt.ZeroReason = "official queue endpoint returned no matching positions for natural resting orders"
	}
	// Terminal outcome is read only after a previously sampled order naturally disappears. This
	// never cancels or creates an order, and the small bound prevents account-history reads from
	// competing with risk controls after a burst.
	open, err := s.store.OpenQueueResearchOrders(ctx, 100)
	if err != nil {
		setError(collectorErrorClass(err), err.Error())
		return
	}
	checked := 0
	terminalCandidates, terminalClassified := 0, 0
	for _, prior := range open {
		if active[prior.OrderID] || checked >= 20 {
			continue
		}
		terminalCandidates++
		checked++
		o, err := s.kal.GetOrder(ctx, prior.OrderID)
		if err == nil {
			classified, outcomeErr := s.store.MarkQueueResearchOutcomeResult(ctx, prior.OrderID, o.Status, o.Filled())
			if outcomeErr != nil {
				exclude("terminal_outcome_storage_error", 1)
				setError(collectorErrorClass(outcomeErr), outcomeErr.Error())
			} else if classified {
				terminalClassified++
			} else {
				exclude("terminal_status_not_final", 1)
			}
		} else {
			exclude("terminal_order_read_error", 1)
			setError(collectorErrorClass(err), err.Error())
		}
	}
	receipt.Metrics["terminal_candidates"] = terminalCandidates
	receipt.Metrics["terminal_checked"] = checked
	receipt.Metrics["terminal_classified"] = terminalClassified
}

func noSideBook(b storage.LifecycleBook) storage.LifecycleBook {
	return storage.LifecycleBook{Bid: 1 - b.Ask, Ask: 1 - b.Bid, BidDepth: b.AskDepth,
		AskDepth: b.BidDepth, Source: b.Source, Age: b.Age}
}

type lifecycleWSMarker struct {
	lastEvent string
	lastClose string
}

// lifecycleWSFilter turns the venue's lifecycle broadcast into genuine prospective transitions.
// The socket can replay close_date_updated for thousands of markets when a full-board subscription
// warms. A first close date is therefore only a baseline; it is not evidence that the date changed.
// Deactivate -> activate and a later, actually different close date -> activate remain observable.
// This filter is deliberately memory-only: after a restart it may conservatively miss one close-
// date change, but it can never manufacture a reopen from a replayed board snapshot.
type lifecycleWSFilter struct {
	mu       sync.Mutex
	byTicker map[string]lifecycleWSMarker
}

func (f *lifecycleWSFilter) accept(ev kalshi.MarketLifecycleEvent) bool {
	return f.classify(ev).Accepted
}

func (s *Server) initResearchLifecycle() {
	if s.kal == nil {
		return
	}
	// One worker preserves the venue's transition order (close-date/deactivated before activated)
	// while the bounded channel keeps all DB/book work off the WS reader.
	// Accepted transitions are rare after the baseline filter. Keep enough bounded slack for a
	// temporary SQLite checkpoint/contention window without ever doing database work on the WS
	// reader. The worker below retries only BUSY/deadline failures and never reclassifies a frame.
	ch := make(chan kalshi.MarketLifecycleEvent, 1024)
	acc := s.lifecycleAccumulator()
	filter := &lifecycleWSFilter{byTicker: make(map[string]lifecycleWSMarker)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	markers, err := s.store.LifecycleResearchMarkers(ctx, 100_000)
	cancel()
	if err == nil {
		for _, marker := range markers {
			filter.byTicker[marker.Ticker] = lifecycleWSMarker{lastEvent: marker.LastEvent, lastClose: marker.LastClose}
		}
	}
	acc.noteHydrated(len(markers), err)
	var warnMu sync.Mutex
	var dropped int64
	var warnAt time.Time
	go func() {
		for ev := range ch {
			s.runGuarded("lifecycle-reopen-event", func() {
				bookCtx, cancelBook := context.WithTimeout(context.Background(), 2*time.Second)
				book, _ := s.researchKalshiBBO(bookCtx, ev.Ticker, false)
				cancelBook()
				h := sha256.Sum256(ev.Raw)
				row := storage.LifecycleResearchEvent{Observed: ev.Observed, Ticker: ev.Ticker,
					EventType: ev.EventType, CloseTime: ev.CloseTime,
					RawHash: hex.EncodeToString(h[:8]), Book: book}
				var id int64
				var cohort bool
				var err error
				for attempt := 1; attempt <= lifecyclePersistenceMaxAttempts; attempt++ {
					writeCtx, cancelWrite := context.WithTimeout(context.Background(), 2*time.Second)
					id, cohort, err = s.store.InsertLifecycleResearchEvent(writeCtx, row)
					cancelWrite()
					if !lifecyclePersistenceShouldRetry(attempt, err) {
						break
					}
					// Do not lose the only prospective transition merely because a wide research
					// reader briefly held the WAL checkpoint. Backoff is bounded; the channel above
					// retains later transitions in venue order. Powers-of-two logging avoids spam.
					if attempt == 1 || attempt&(attempt-1) == 0 {
						s.log.Warn("lifecycle-reopen persistence waiting for database writer",
							"ticker", ev.Ticker, "attempt", attempt)
					}
					delay := time.Duration(attempt) * 100 * time.Millisecond
					if delay > 2*time.Second {
						delay = 2 * time.Second
					}
					time.Sleep(delay)
				}
				acc.noteTransition(id > 0, cohort, err)
				if err != nil && !strings.Contains(err.Error(), "database is closed") {
					s.log.Warn("lifecycle-reopen event persistence failed", "ticker", ev.Ticker, "err", err)
				}
			})
		}
	}()
	s.kal.SetMarketLifecycleHandler(func(ev kalshi.MarketLifecycleEvent) {
		decision := filter.classify(ev)
		acc.noteFrame(decision)
		if !decision.Accepted {
			return
		}
		select {
		case ch <- ev:
		default:
			acc.noteQueueDrop()
			warnMu.Lock()
			dropped++
			if time.Since(warnAt) >= time.Minute {
				warnAt = time.Now()
				s.log.Warn("lifecycle-reopen transition queue full; event omitted",
					"ticker", ev.Ticker, "event", ev.EventType, "dropped_total", dropped)
			}
			warnMu.Unlock()
		}
	})
}

func lifecyclePersistenceRetryable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "database is closed") || strings.Contains(msg, "disk is full") ||
		strings.Contains(msg, "malformed") || strings.Contains(msg, "constraint") {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "database is locked") || strings.Contains(msg, "database is busy") ||
		strings.Contains(msg, "sqlite_busy")
}

func lifecyclePersistenceShouldRetry(attempt int, err error) bool {
	return attempt > 0 && attempt < lifecyclePersistenceMaxAttempts && lifecyclePersistenceRetryable(err)
}

// completeResearchLifecycleRow advances the fair keyset only after every due horizon on this row
// has been visited without the sweep context expiring. A deadline in the middle of a page must
// leave the interrupted row at the head of the next page; jumping to due[len-1] would silently
// postpone it until a full catalog wrap and turn a recoverable delay into a missed fixed horizon.
func (s *Server) completeResearchLifecycleRow(ctx context.Context, rowID int64) bool {
	if s == nil || rowID <= 0 || ctx == nil || ctx.Err() != nil {
		return false
	}
	s.researchMu.Lock()
	defer s.researchMu.Unlock()
	// Recheck under the cursor lock so cancellation racing the row-complete boundary remains
	// conservative. Reprocessing an idempotent terminal is safe; skipping an unprocessed row is not.
	if ctx.Err() != nil {
		return false
	}
	s.researchLifecycleCursor = rowID
	return true
}

func lifecycleSweepInterrupted(ctx context.Context, err error) bool {
	return ctx == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (s *Server) sweepLifecycleReopen(ctx context.Context) {
	const (
		lifecycleAttemptCap = 100
		lifecycleRESTCap    = 4
	)
	acc := s.lifecycleAccumulator()
	defer s.flushLifecycleCollector(ctx)
	s.researchMu.Lock()
	cursor := s.researchLifecycleCursor
	s.researchMu.Unlock()
	due, err := s.store.LifecycleResearchDue(ctx, cursor, lifecycleAttemptCap)
	if err != nil {
		acc.noteError(err)
		return
	}
	if len(due) == 0 && cursor > 0 {
		due, err = s.store.LifecycleResearchDue(ctx, 0, lifecycleAttemptCap)
		if err != nil {
			acc.noteError(err)
			return
		}
	}
	now := time.Now()
	restUsed := 0
	for _, row := range due {
		if lifecycleSweepInterrupted(ctx, nil) {
			break
		}
		rowComplete := true
		age := now.Sub(row.Observed)
		if age < 5*time.Second {
			// LifecycleResearchDue returns incomplete cohorts, including a just-recorded reopen
			// whose first fixed horizon has not arrived. Holding this newest keyset row for at most
			// five seconds is intentional: advancing it would make the strict 5s sample depend on
			// a later full-catalog wrap.
			break
		}
		for _, horizon := range []int{5, 30, 300, 1800} {
			if lifecycleSweepInterrupted(ctx, nil) {
				rowComplete = false
				break
			}
			h := time.Duration(horizon) * time.Second
			if age < h {
				continue
			}
			have, err := s.store.HasLifecycleHorizon(ctx, row.ID, horizon)
			if err != nil {
				acc.noteError(err)
				rowComplete = false
				break
			}
			if have {
				continue
			}
			// Enter the attempted stage before either terminal-missing persistence or a live-book
			// capture. Both are deliberate outcomes of evaluating this due horizon; otherwise an
			// expired horizon can store a miss row while the liveness receipt falsely says tried=0.
			acc.noteHorizonDueAttempt()
			// Fixed-horizon integrity: never label a restart-delayed observation as the requested
			// horizon. Missing rows remain honest missingness.
			tolerance := 10 * time.Second
			if horizon >= 300 {
				tolerance = time.Minute
			}
			if horizon >= 1800 {
				tolerance = 2 * time.Minute
			}
			if age > h+tolerance {
				inserted, missErr := s.store.InsertLifecycleHorizonMissResult(ctx, row.ID, horizon,
					"fixed horizon passed before a fresh fee-complete executable book was captured")
				acc.noteHorizonMiss(inserted, "fixed_horizon_expired", missErr)
				if missErr != nil {
					rowComplete = false
					break
				}
				continue
			}
			book, ok := s.researchKalshiBBO(ctx, row.Ticker, false)
			if !ok && restUsed < lifecycleRESTCap {
				restUsed++ // failures count; a broken ticker cannot create an authenticated REST flood
				book, ok = s.researchKalshiBBO(ctx, row.Ticker, true)
			}
			if lifecycleSweepInterrupted(ctx, nil) {
				rowComplete = false
				break
			}
			if !ok || book.BidDepth < 1 || book.AskDepth < 1 {
				if restUsed >= lifecycleRESTCap {
					acc.noteHorizonExclusion("fresh_book_unavailable_rest_budget_exhausted")
				} else {
					acc.noteHorizonExclusion("fresh_executable_book_unavailable")
				}
				// This is temporary missing transport evidence, not a completed horizon. Keep this
				// row at the keyset head and retry on the next 2-second tick. The fixed-horizon
				// tolerance above supplies the bound: if availability does not recover, a durable
				// honest miss replaces it instead of silently postponing the row until catalog wrap.
				rowComplete = false
				break
			}
			fee, known, _ := s.kalFeeExact(row.Ticker, false, 1, book.Ask)
			if !known || fee < 0 {
				acc.noteHorizonExclusion("authoritative_taker_fee_unavailable")
				// Fee authority can refresh independently of the book. It gets the same bounded
				// near-horizon retry treatment; advancing here would permanently lose a sample
				// which became complete one tick later.
				rowComplete = false
				break
			}
			inserted, insertErr := s.store.InsertLifecycleHorizonResult(ctx, row.ID, horizon, book, fee)
			acc.noteHorizonSample(inserted, insertErr)
			if insertErr != nil {
				rowComplete = false
				break
			}
		}
		if !rowComplete || !s.completeResearchLifecycleRow(ctx, row.ID) {
			break
		}
	}
}

const polyUSFeeAuthoritySource = "polyus:complete-open-universe.feeCoefficient"

// polyUSFreshFeeAuthority returns a market-specific fee coefficient only from the last complete,
// successful open-universe crawl. The smaller league cache is useful discovery data, but it is not
// fee authority: it can survive a partial refresh and used to let stale feeCoefficient values prove
// research/ML/maker economics indefinitely. A zero coefficient remains a valid fee-free receipt.
func (s *Server) polyUSFreshFeeAuthority(ticker string) (theta, tick float64, ok bool) {
	m, _, known := s.polyUSFreshSweepMarket(ticker)
	if !known || m.FeeCoeff == nil || !validPolyUSFeeTheta(*m.FeeCoeff) {
		return 0, 0, false
	}
	return *m.FeeCoeff, m.TickSize, true
}

// polyUSFeeExactAuthority preserves signed maker rebates for research accounting. Money-risk and
// proof-dispatch callers must separately clamp negative fees to zero because rebates are not buying
// power before settlement.
func (s *Server) polyUSFeeExactAuthority(ticker string, maker bool, contracts, price float64) (float64, string, bool) {
	if contracts <= 0 || price <= 0 || price >= 1 || math.IsNaN(contracts) || math.IsInf(contracts, 0) ||
		math.IsNaN(price) || math.IsInf(price, 0) {
		return 0, "", false
	}
	theta, _, known := s.polyUSFreshFeeAuthority(ticker)
	if !known {
		return 0, "", false
	}
	fee := polyUSFeeWithTheta(maker, contracts, price, theta)
	if math.IsNaN(fee) || math.IsInf(fee, 0) || fee <= -contracts || fee >= contracts {
		return 0, "", false
	}
	return fee, polyUSFeeAuthoritySource, true
}

func (s *Server) polyUSResearchFeeExact(ticker string, price float64) (float64, bool) {
	return s.polyUSResearchFeeExactRoute(ticker, false, price)
}

func (s *Server) polyUSResearchFeeExactRoute(ticker string, maker bool, price float64) (float64, bool) {
	fee, _, known := s.polyUSFeeExactAuthority(ticker, maker, 1, price)
	return fee, known
}

// nestedGreaterPayout is the payoff identity behind nested-ladder-lock. A value above the low
// threshold pays YES(low); a value not above the high threshold pays NO(high). For low<high the
// sum is always >=1 and is 2 strictly inside the interval. This says nothing about getting both
// non-atomic legs filled.
func nestedGreaterPayout(value, low, high float64) float64 {
	payout := 0.0
	if value > low {
		payout++
	}
	if !(value > high) {
		payout++
	}
	return payout
}

type nestedLadderRung struct {
	ticker                               string
	strike, bid, ask, bidDepth, askDepth float64
}

type researchCycleCounts struct {
	Eligible, Attempted, Inserted, Duplicates, Excluded, Errors int
}

// nestedLadderRungs fails closed when floor_strike is zero. The current flexible decoder cannot
// distinguish JSON absence from an explicit numeric zero; sacrificing a legitimate zero-strike
// market is safer than inventing a logical threshold identity from a missing field.
func nestedLadderRungs(snap kalshi.EventSnapshot) []nestedLadderRung {
	var rows []nestedLadderRung
	for _, m := range snap.Markets {
		strike, bid, ask := m.FloorStrike.Float(), m.YesBid.Float(), m.YesAsk.Float()
		if strike == 0 || m.EventTicker != snap.EventTicker || !strings.EqualFold(m.Status, "active") || m.Result != "" ||
			!strings.EqualFold(m.StrikeType, "greater") || bid <= 0 || ask <= bid || ask >= 1 ||
			m.YesBidSize.Float() < 1 || m.YesAskSize.Float() < 1 {
			continue
		}
		rows = append(rows, nestedLadderRung{ticker: m.Ticker, strike: strike, bid: bid, ask: ask,
			bidDepth: m.YesBidSize.Float(), askDepth: m.YesAskSize.Float()})
	}
	return rows
}

func (s *Server) recordNestedLadders(ctx context.Context, snap kalshi.EventSnapshot, now time.Time) researchCycleCounts {
	var counts researchCycleCounts
	rows := nestedLadderRungs(snap)
	if len(rows) < 2 {
		return counts
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].strike < rows[j].strike })
	pairs := map[[2]int]bool{}
	for i := 0; i < len(rows)-1; i++ {
		if rows[i].strike >= rows[i+1].strike {
			continue
		}
		pairs[[2]int{i, i + 1}] = true // adjacent curve shape
		best := i + 1
		for j := i + 2; j < len(rows); j++ {
			if rows[j].bid > rows[best].bid {
				best = j
			}
		}
		pairs[[2]int{i, best}] = true // strongest higher-strike bid catches non-local violations
	}
	counts.Eligible = len(pairs)
	for pair := range pairs {
		lo, hi := rows[pair[0]], rows[pair[1]]
		highNoAsk, highNoBid := 1-hi.bid, 1-hi.ask
		lowEntry, lok, _ := s.kalFeeExact(lo.ticker, false, 1, lo.ask)
		highEntry, hok, _ := s.kalFeeExact(hi.ticker, false, 1, highNoAsk)
		lowExit, lxok, _ := s.kalFeeExact(lo.ticker, false, 1, lo.bid)
		highExit, hxok, _ := s.kalFeeExact(hi.ticker, false, 1, highNoBid)
		if !lok || !hok || !lxok || !hxok || lowEntry < 0 || highEntry < 0 || lowExit < 0 || highExit < 0 {
			counts.Excluded++
			continue
		}
		counts.Attempted++
		inserted, err := s.store.InsertNestedLadderResult(ctx, storage.NestedLadderObservation{Observed: now, Venue: "kalshi",
			EventID: snap.EventTicker, Title: snap.Title, LowTicker: lo.ticker, HighTicker: hi.ticker,
			IdentitySource: "same official nested event + strike_type=greater", LowStrike: lo.strike,
			HighStrike: hi.strike, LowBid: lo.bid, LowAsk: lo.ask, HighBid: hi.bid, HighAsk: hi.ask,
			LowAskDepth: lo.askDepth, HighNoAskDepth: hi.bidDepth, LowEntryFee: lowEntry,
			HighEntryFee: highEntry, LowExitFee: lowExit, HighExitFee: highExit})
		if err != nil {
			counts.Errors++
		} else if inserted {
			counts.Inserted++
		} else {
			counts.Duplicates++
		}
	}
	return counts
}

// rotateEligibleResearchKeys returns at most limit eligible keys and the next cursor after every
// key examined. This makes the fixed REST budget fair even when the first keys repeatedly become
// eligible as their 5-minute cooldown expires.
func rotateEligibleResearchKeys(keys []string, start, limit int, eligible func(string) bool) ([]string, int) {
	if len(keys) == 0 || limit <= 0 {
		return nil, 0
	}
	start %= len(keys)
	if start < 0 {
		start += len(keys)
	}
	out := make([]string, 0, limit)
	examined := 0
	for examined < len(keys) && len(out) < limit {
		idx := (start + examined) % len(keys)
		examined++
		if eligible == nil || eligible(keys[idx]) {
			out = append(out, keys[idx])
		}
	}
	return out, (start + examined) % len(keys)
}

func (s *Server) sweepEventBasketLock(ctx context.Context) {
	started := time.Now()
	// nested-ladder-lock is a Kalshi-native collector. During cold boot the authenticated client
	// and catalog arrive after the research scheduler starts; that expected warm-up is not a
	// completed collector failure and must not replace the last truthful receipt.
	persistNestedReceipt := false
	// outcome-set-surface and payoff-envelope-capacity normally receipt once per successful event
	// snapshot. Keep a cycle-level terminal ready for the truthful zero/error case so an empty fair
	// slice cannot leave either collector looking dead.
	derivedCycleStatus, derivedCycleErrorClass, derivedCycleErrorText := "", "", ""
	derivedCycleZeroReason := ""
	derivedCycleCatalogCandidates, derivedCycleEligible := 0, 0
	derivedCycleAttempted, derivedCycleFetched, derivedCycleSnapshotErrors := 0, 0, 0
	preservePriorCycleReceipts, restoreSelectedCooldowns := false, false
	type basketSeenPrior struct {
		at      time.Time
		existed bool
	}
	basketSeenBefore := map[string]basketSeenPrior{}
	basketCursorBefore := 0
	basketReceipt := storage.CollectorReceipt{
		CollectorID: "event-basket-lock", CycleID: collectorCycleID(started),
		ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1, Status: "healthy",
		Started: started, Source: "complete structural event sets + exact books/fees",
		SchemaVersion: "basket-v1", ExpectedCadence: 2 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"payoff-constraint-solver", "identity-challenged-cross-venue-lock"},
	}
	nestedReceipt := storage.CollectorReceipt{
		CollectorID: "nested-ladder-lock", CycleID: collectorCycleID(started),
		ExperimentID: "payoff-constraint-solver", ExperimentVersion: 1, Status: "healthy",
		Started: started, Source: "structural threshold ladders + exact books/fees",
		SchemaVersion: "nested-v1", ExpectedCadence: 2 * time.Minute,
		Exclusions: map[string]int{}, Metrics: map[string]any{},
		Systems: []string{"payoff-constraint-solver", "deadline-hazard-surface"},
	}
	defer func() {
		if restoreSelectedCooldowns {
			s.researchMu.Lock()
			s.researchBasketCursor = basketCursorBefore
			for event, prior := range basketSeenBefore {
				if prior.existed {
					s.researchBasketSeen[event] = prior.at
				} else {
					delete(s.researchBasketSeen, event)
				}
			}
			s.researchMu.Unlock()
		}
		if preservePriorCycleReceipts {
			return
		}
		durableCtx, cancelDurability := researchDurabilityContext(ctx)
		defer cancelDurability()
		complete := time.Now()
		finalize := func(receipt *storage.CollectorReceipt, zeroReason string) {
			receipt.Completed = complete
			if receipt.ErrorClass != "" && receipt.Status != "blocked" {
				receipt.Status = "error"
			} else if receipt.Inserted == 0 {
				if receipt.Status == "healthy" {
					receipt.Status, receipt.ExpectedZero, receipt.ZeroReason = "healthy_empty", true, zeroReason
				}
			}
			if _, err := s.store.InsertCollectorReceipt(durableCtx, *receipt); err != nil && s.log != nil {
				s.log.Warn("research basket collector receipt failed", "collector", receipt.CollectorID, "err", err)
			}
		}
		finalize(&basketReceipt, "no new complete exact-fee event basket survived this cycle; source attempts, duplicates and exclusions are explicit")
		if persistNestedReceipt {
			finalize(&nestedReceipt, "no new verified exact-fee nested ladder survived this cycle; source attempts, duplicates and exclusions are explicit")
		}
		if derivedCycleStatus != "" {
			for _, spec := range []struct {
				collector, experiment, source string
				systems                       []string
			}{
				{"outcome-set-surface", "outcome-set-expansion-shock", "official venue event membership + current side-specific books", []string{"outcome-set-expansion-shock", "payoff-constraint-solver"}},
				{"payoff-envelope-capacity", "payoff-constraint-solver", "verified payoff certificate + actual depth levels + exact per-level route fees", []string{"payoff-constraint-solver", "deadline-hazard-surface", "incentive-subsidized-structural-lock"}},
			} {
				receipt := storage.CollectorReceipt{CollectorID: spec.collector,
					CycleID: collectorCycleID(started) + "|event-basket-cycle", ExperimentID: spec.experiment,
					ExperimentVersion: 1, Status: derivedCycleStatus, Started: started, Completed: complete,
					Eligible: derivedCycleEligible, Attempted: derivedCycleAttempted,
					ErrorClass: derivedCycleErrorClass, ErrorText: derivedCycleErrorText,
					Source: spec.source, SchemaVersion: storage.R138EvidenceSchemaVersion(),
					ExpectedCadence: 2 * time.Minute, Systems: spec.systems,
					Exclusions: map[string]int{}, Metrics: map[string]any{
						"catalog_event_candidates":  derivedCycleCatalogCandidates,
						"event_snapshots_selected":  derivedCycleEligible,
						"event_snapshots_attempted": derivedCycleAttempted,
						"event_snapshots_fetched":   derivedCycleFetched,
						"event_snapshot_errors":     derivedCycleSnapshotErrors,
						"research_only":             true,
						"profit_evidence":           false,
						"profit_authority":          false,
						"funded":                    false,
						"paper_authority":           false,
						"live_authority":            false,
					},
				}
				if derivedCycleStatus == "healthy_empty" {
					receipt.ExpectedZero = true
					receipt.ZeroReason = derivedCycleZeroReason
					receipt.Exclusions["no_due_event_snapshot"] = 1
				} else if derivedCycleSnapshotErrors > 0 {
					receipt.Exclusions["event_snapshot_error"] = derivedCycleSnapshotErrors
				}
				if _, err := s.store.InsertCollectorReceipt(durableCtx, receipt); err != nil && s.log != nil {
					s.log.Warn("research derived event collector receipt failed", "collector", receipt.CollectorID, "err", err)
				}
			}
		}
	}()
	// Kalshi: the official nested event response proves complete current product identity and the
	// mutually-exclusive flag, but not exhaustiveness. The complete all-NO route needs only
	// at-most-one YES: its payout floor is N-1 even when zero outcomes settle YES.
	events := map[string]int{}
	kalshiBoard, kalshiBoardAt, kalshiBoardReady := s.researchEventBasketCompleteBoard(started)
	kalshiCatalogRows := len(kalshiBoard)
	for _, m := range kalshiBoard {
		if m.EventTicker != "" && m.Result == "" &&
			(m.Status == "" || strings.EqualFold(m.Status, "active")) {
			events[m.EventTicker]++
		}
	}
	keys := make([]string, 0, len(events))
	for event, n := range events {
		if n >= 2 {
			keys = append(keys, event)
		}
	}
	sort.Strings(keys)
	now := started
	basketReceipt.Eligible = len(keys)
	// Select exactly one fair slice before making REST calls. Attempts (including failures) are
	// capped at ten, and attempted events enter the cooldown so a broken event cannot monopolize
	// the next pass.
	s.researchMu.Lock()
	if s.researchBasketSeen == nil {
		s.researchBasketSeen = map[string]time.Time{}
	}
	basketCursorBefore = s.researchBasketCursor
	chosen, next := rotateEligibleResearchKeys(keys, s.researchBasketCursor, 10, func(event string) bool {
		seen := s.researchBasketSeen[event]
		return seen.IsZero() || now.Sub(seen) >= 5*time.Minute
	})
	s.researchBasketCursor = next
	for _, event := range chosen {
		prior, existed := s.researchBasketSeen[event]
		basketSeenBefore[event] = basketSeenPrior{at: prior, existed: existed}
		s.researchBasketSeen[event] = now
	}
	for k, at := range s.researchBasketSeen {
		if now.Sub(at) > time.Hour {
			delete(s.researchBasketSeen, k)
		}
	}
	s.researchMu.Unlock()
	basketReceipt.Attempted = len(chosen)
	kalshiSourceReady := s.kal != nil && kalshiBoardReady
	// A missing configured client is a real unavailable-source condition and remains a durable
	// block. An installed client with a catalog that has not arrived yet is ordinary boot warm-up;
	// retain the prior completed receipt until that client has something truthful to evaluate.
	persistNestedReceipt = s.kal == nil || kalshiSourceReady
	kalshiSnapshotsOK, kalshiSnapshotErrors, kalshiDerivedEligibleSnapshots := 0, 0, 0
	if s.kal == nil {
		basketReceipt.Exclusions["kalshi_client_unavailable"]++
		nestedReceipt.Exclusions["kalshi_client_unavailable"]++
	} else if kalshiCatalogRows == 0 {
		basketReceipt.Exclusions["kalshi_catalog_not_ready"]++
		nestedReceipt.Exclusions["kalshi_catalog_not_ready"]++
	} else {
		for _, event := range chosen {
			snap, err := s.kal.GetEventSnapshot(ctx, event)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					preservePriorCycleReceipts, restoreSelectedCooldowns = true, true
					return
				}
				kalshiSnapshotErrors++
				basketReceipt.Exclusions["event_snapshot_error"]++
				continue
			}
			kalshiSnapshotsOK++
			if snap.EventTicker != "" && len(snap.Markets) >= 2 {
				kalshiDerivedEligibleSnapshots++
			}
			s.captureR138EventSnapshot(ctx, snap, now)
			nested := s.recordNestedLadders(ctx, snap, now)
			nestedReceipt.Eligible += nested.Eligible
			nestedReceipt.Attempted += nested.Attempted
			nestedReceipt.Inserted += nested.Inserted
			nestedReceipt.Duplicates += nested.Duplicates
			nestedReceipt.Exclusions["incomplete_fee_or_book"] += nested.Excluded
			if nested.Errors > 0 {
				nestedReceipt.Exclusions["storage_error"] += nested.Errors
				nestedReceipt.ErrorClass, nestedReceipt.ErrorText = "storage", "one or more nested-ladder observations failed durable insertion"
			}
			if ctx.Err() != nil {
				preservePriorCycleReceipts, restoreSelectedCooldowns = true, true
				return
			}
			if !snap.MutuallyExclusive || len(snap.Markets) < 2 {
				basketReceipt.Exclusions["not_mutually_exclusive_or_too_small"]++
				continue
			}
			legs, complete := make([]storage.BasketLeg, 0, len(snap.Markets)), true
			for _, m := range snap.Markets {
				ask, depth, touchOK := kalshiNoAskTouch(m)
				fee, known, _ := s.kalFeeExact(m.Ticker, false, 1, ask)
				if m.Ticker == "" || !strings.EqualFold(m.Status, "active") || m.Result != "" ||
					!touchOK || !known || fee < 0 {
					complete = false
					break
				}
				legs = append(legs, storage.BasketLeg{Ticker: m.Ticker, Side: "NO", Ask: ask, Depth: depth, Fee: fee})
			}
			if complete {
				inserted, err := s.store.InsertEventBasketResult(ctx, storage.EventBasketObservation{Observed: now, Venue: "kalshi",
					EventID: event, Title: snap.Title, Route: "buy-no-all", IdentitySource: "official nested event markets",
					IdentityComplete: true, MutuallyExclusive: true, Exhaustive: false, Legs: legs,
					PayoutLowerBound: float64(len(legs) - 1)})
				if err != nil {
					basketReceipt.Exclusions["storage_error"]++
					basketReceipt.ErrorClass, basketReceipt.ErrorText = "storage", "one or more event-basket observations failed durable insertion"
				} else if inserted {
					basketReceipt.Inserted++
				} else {
					basketReceipt.Duplicates++
				}
			} else {
				basketReceipt.Exclusions["incomplete_book_or_fee"]++
			}
		}
	}
	if ctx.Err() != nil {
		preservePriorCycleReceipts, restoreSelectedCooldowns = true, true
		return
	}

	// PolyUS: only the explicit winner cardinalities accepted by completePolyUSDutchSets are both
	// mutually exclusive and exhaustive. Every leg must have current two-sided touch depth and a
	// per-market feeCoefficient; fallback fees are forbidden here.
	var markets []polyUSMarket
	polyUSReady := false
	if s.polyUSAuth == nil {
		basketReceipt.Exclusions["polyus_auth_unavailable"]++
	} else {
		s.polyUSMu.Lock()
		if s.polyUSAt.IsZero() || time.Since(s.polyUSAt) > 45*time.Second {
			basketReceipt.Exclusions["polyus_snapshot_stale"]++
		} else {
			markets = append([]polyUSMarket(nil), s.polyUSMkts...)
			polyUSReady = true
		}
		s.polyUSMu.Unlock()
	}
	sets := completePolyUSDutchSets(markets)
	basketReceipt.Eligible += len(sets)
	basketReceipt.Attempted += len(sets)
	for _, set := range sets {
		// R138: reuse the already certified/fresh winner set for immutable outcome-membership,
		// censor-safe payoff-solver and capacity evidence. This issues no additional venue call.
		solverInserted, solverDuplicates, solverErrors := s.captureR138PolyUSOutcomeSet(ctx, set, now)
		basketReceipt.Inserted += solverInserted
		basketReceipt.Duplicates += solverDuplicates
		if solverErrors > 0 {
			basketReceipt.Exclusions["polyus_typed_bundle_storage_error"] += solverErrors
			basketReceipt.ErrorClass, basketReceipt.ErrorText = "typed_bundle", "one or more PolyUS solver bundles failed atomic persistence"
		}
		yesLegs, noLegs, valid := make([]storage.BasketLeg, 0, len(set.legs)), make([]storage.BasketLeg, 0, len(set.legs)), true
		for _, l := range set.legs {
			yesFee, yok := s.polyUSResearchFeeExact(l.ticker, l.ask)
			noAsk := 1 - l.bid
			noFee, nok := s.polyUSResearchFeeExact(l.ticker, noAsk)
			if !yok || !nok {
				valid = false
				break
			}
			yesLegs = append(yesLegs, storage.BasketLeg{Ticker: l.ticker, Side: "YES", Ask: l.ask, Depth: l.askSz, Fee: yesFee})
			noLegs = append(noLegs, storage.BasketLeg{Ticker: l.ticker, Side: "NO", Ask: noAsk, Depth: l.bidSz, Fee: noFee})
		}
		if !valid {
			basketReceipt.Exclusions["polyus_fee_unknown"]++
			continue
		}
		title := set.name
		if title == "" {
			title = set.eventID
		}
		base := storage.EventBasketObservation{Observed: now, Venue: "polyus", EventID: set.eventID + "|scope=" + set.scope, Title: title,
			IdentitySource: "authenticated product event + exact sportsMarketType=" + set.scope + " + explicit league winner cardinality", IdentityComplete: true,
			MutuallyExclusive: true, Exhaustive: true, PayoutLowerBound: 1}
		base.Route, base.Legs = "buy-yes-all", yesLegs
		inserted, err := s.store.InsertEventBasketResult(ctx, base)
		if err != nil {
			basketReceipt.Exclusions["storage_error"]++
			basketReceipt.ErrorClass, basketReceipt.ErrorText = "storage", "one or more event-basket observations failed durable insertion"
		} else if inserted {
			basketReceipt.Inserted++
		} else {
			basketReceipt.Duplicates++
		}
		base.Route, base.Legs, base.PayoutLowerBound = "buy-no-all", noLegs, float64(len(noLegs)-1)
		inserted, err = s.store.InsertEventBasketResult(ctx, base)
		if err != nil {
			basketReceipt.Exclusions["storage_error"]++
			basketReceipt.ErrorClass, basketReceipt.ErrorText = "storage", "one or more event-basket observations failed durable insertion"
		} else if inserted {
			basketReceipt.Inserted++
		} else {
			basketReceipt.Duplicates++
		}
	}
	kalshiOperational := kalshiSourceReady && (len(chosen) == 0 || kalshiSnapshotsOK > 0)
	basketReceipt.Metrics["source_truth"] = map[string]any{"kalshi_catalog_rows": kalshiCatalogRows,
		"kalshi_complete_board_at": kalshiBoardAt,
		"kalshi_events_selected":   len(chosen), "kalshi_snapshots_ok": kalshiSnapshotsOK,
		"kalshi_snapshot_errors": kalshiSnapshotErrors, "kalshi_derived_eligible_snapshots": kalshiDerivedEligibleSnapshots,
		"polyus_snapshot_ready": polyUSReady,
		"polyus_markets":        len(markets)}
	nestedReceipt.Metrics["source_truth"] = map[string]any{"kalshi_catalog_rows": kalshiCatalogRows,
		"kalshi_events_selected": len(chosen), "kalshi_snapshots_ok": kalshiSnapshotsOK,
		"kalshi_snapshot_errors": kalshiSnapshotErrors}
	if nestedReceipt.ErrorClass == "" {
		switch {
		case !kalshiSourceReady:
			nestedReceipt.Status, nestedReceipt.ErrorClass, nestedReceipt.ErrorText = "blocked", "source_not_ready", "Kalshi client and current catalog are required before nested ladders can be evaluated"
		case len(chosen) > 0 && kalshiSnapshotsOK == 0 && kalshiSnapshotErrors > 0:
			nestedReceipt.Status, nestedReceipt.ErrorClass, nestedReceipt.ErrorText = "error", "source_api", "all selected Kalshi event snapshots failed"
		}
	}
	if basketReceipt.ErrorClass == "" && !kalshiOperational && !polyUSReady {
		if kalshiSnapshotErrors > 0 {
			basketReceipt.Status, basketReceipt.ErrorClass, basketReceipt.ErrorText = "error", "source_api", "all selected Kalshi event snapshots failed and no fresh PolyUS snapshot was available"
		} else {
			basketReceipt.Status, basketReceipt.ErrorClass, basketReceipt.ErrorText = "blocked", "source_not_ready", "no current Kalshi catalog/client or fresh authenticated PolyUS snapshot was available"
		}
	}
	// Per-event capture writes detailed receipts. This later aggregate is the truthful terminal for
	// the whole bounded source attempt: a partial venue failure must remain non-healthy even when an
	// earlier snapshot succeeded, while a completed zero/ineligible scan is explicit healthy-empty.
	if kalshiSourceReady {
		derivedCycleCatalogCandidates = len(keys)
		derivedCycleEligible, derivedCycleAttempted = len(chosen), len(chosen)
		derivedCycleFetched = kalshiSnapshotsOK
		derivedCycleSnapshotErrors = kalshiSnapshotErrors
		switch {
		case kalshiSnapshotErrors > 0:
			derivedCycleStatus, derivedCycleErrorClass = "error", "source_api"
			derivedCycleErrorText = "one or more selected Kalshi event snapshots failed; the attempted cycle is incomplete"
			if basketReceipt.ErrorClass == "" {
				basketReceipt.Status, basketReceipt.ErrorClass, basketReceipt.ErrorText = "error", "source_api", derivedCycleErrorText
			}
			if nestedReceipt.ErrorClass == "" {
				nestedReceipt.Status, nestedReceipt.ErrorClass, nestedReceipt.ErrorText = "error", "source_api", derivedCycleErrorText
			}
		case len(chosen) == 0:
			derivedCycleStatus = "healthy_empty"
			if len(keys) == 0 {
				derivedCycleZeroReason = "current Kalshi metadata catalog was ready, but no active multi-market event was eligible; zero official event snapshots were fetched"
			} else {
				derivedCycleZeroReason = "current Kalshi metadata catalog was ready, but every active multi-market event was inside the fair-scan cooldown; zero official event snapshots were eligible or fetched"
			}
		case kalshiDerivedEligibleSnapshots == 0:
			derivedCycleStatus = "healthy_empty"
			derivedCycleZeroReason = "selected Kalshi event snapshots completed, but none returned at least two current members for an outcome-set or payoff-envelope evaluation"
		default:
			derivedCycleStatus = "healthy"
		}
	}
}

func venueNoticeRunContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), time.Minute)
}

// researchFoundationTick isolates the production-size identity/catalog page scans from the
// minute-level source-clock/replay liveness loop. Its scheduler is single-goroutine and bounded, so
// this cannot overlap with itself; a slow foundation page can no longer make fresh WebSockets look
// stale or postpone every other research receipt until the four-minute outer deadline.
func (s *Server) researchFoundationTick(ctx context.Context) {
	s.tryRunHeavyResearch(ctx, "foundation", 0, func(ctx context.Context) {
		now := time.Now()
		if s.kal == nil || s.kal.TickerCount() == 0 {
			return
		}
		s.researchMu.Lock()
		due := now.Sub(s.lastResearchFoundationAt) >= 5*time.Minute
		if due {
			s.lastResearchFoundationAt = now
		}
		s.researchMu.Unlock()
		if !due {
			return
		}
		s.sweepResearchFoundation(ctx)
		s.sweepCatalogFoundation(ctx)
	})
}

// researchLivenessTick is deliberately independent from every authenticated/catalog/semantic
// research scan. Those scans may legitimately consume most of a four-minute deadline; official
// watcher, source-clock and selected-replay truth must still advance on their own cadence.
func (s *Server) researchLivenessTick(ctx context.Context) {
	now := time.Now()
	// Cache-only and independent from the heavy research rotation: every exact system contract is
	// refreshed even when wide catalog/evidence queries are delayed. Absence remains NOT_APPLICABLE.
	s.refreshR147SystemContractCoverage(now)
	s.flushNativeSystemFunnels(context.WithoutCancel(ctx), now, false)
	doVenueNotices := venueNoticeSweepDue(s, now)
	doSourceClocks := researchSourceClockSweepDue(s, now)
	if doVenueNotices {
		go func() {
			noticeCtx, cancel := venueNoticeRunContext(ctx)
			defer cancel()
			s.sweepVenueNotices(noticeCtx)
		}()
	}
	if doSourceClocks {
		s.sweepResearchSourceClocks(ctx)
	}
	// Source clocks and sparse funnel receipts stay independent and current. Replay is a wide
	// research-only checkpoint, so it joins the heavy lane; its due timestamp is consumed only
	// after admission, making a collision a retry rather than a dropped capture.
	s.tryRunHeavyResearch(ctx, "replay", 0, func(ctx context.Context) {
		if researchReplayCaptureDue(s, time.Now()) {
			s.captureResearchReplay(ctx)
		}
	})
}

func (s *Server) researchSystemsTick(ctx context.Context) {
	now := time.Now()

	// Promotion and queue truth precede the heavy rotation. Lifecycle fixed horizons have their
	// own serial bounded scheduler below: a production pass took 63 seconds under SQLite pressure
	// and used to reduce this five-second rotation to roughly one collector per minute.
	s.researchMu.Lock()
	doQueue := now.Sub(s.lastQueueResearchAt) >= time.Minute
	if doQueue {
		s.lastQueueResearchAt = now
	}
	s.researchMu.Unlock()
	// Promotion owns internal 5s/7s throttles and must see candidates before their 20s/45s
	// freshness windows close. It can hand work to Paper, so it never belongs to an analytics gate.
	s.sweepResearchPromotionDispatch(ctx, now)
	if doQueue {
		s.sweepQueuePriority(ctx)
	}

	// Heavy collectors rotate one bounded lane per admission. This releases the shared gate between
	// collectors, gives WAL checkpoints and other research jobs a turn, and prevents any early scan
	// from handing an expired four-minute context to latent registration or coverage.
	s.runNextResearchSystemHeavy(ctx, now)
}

// researchLifecycleTick is independently serial and receives a short scheduler deadline. It
// cannot overlap itself or delay the heavy collector rotation; if SQLite or one of the four
// bounded REST fallbacks uses the whole deadline, the next ordinary tick retries from the fair
// keyset cursor and the accumulator preserves any receipt counts whose write did not finish.
func (s *Server) researchLifecycleTick(ctx context.Context) {
	if s.kal == nil || s.kal.TickerCount() == 0 {
		return
	}
	now := time.Now()
	s.researchMu.Lock()
	due := now.Sub(s.lastLifecycleResearchAt) >= 2*time.Second
	if due {
		s.lastLifecycleResearchAt = now
	}
	s.researchMu.Unlock()
	if due {
		s.sweepLifecycleReopen(ctx)
	}
}

// researchEvidenceTimingTick has its own short scheduler lane. It is memory-only, bounded by the
// Step-7 hard queue/state caps, and mutex-serialized. A heavy SQLite call that ignores cancellation
// can therefore delay persistence without blocking the 5s/30s/300s sampling clock.
func (s *Server) researchEvidenceTimingTick(_ context.Context) {
	s.advanceStep7TimedEvidence(time.Now())
}

// researchEvidenceTick owns only WAL-protected persistence and the slower semantic/deadline scans.
func (s *Server) researchEvidenceTick(ctx context.Context) {
	s.tryRunHeavyResearch(ctx, "evidence", 0, func(ctx context.Context) {
		s.sweepR138Step6To9(ctx)
	})
}

func (s *Server) handleResearchSystems(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.ResearchSystemsReport(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, report)
}
