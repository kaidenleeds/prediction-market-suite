package server

// The legacy LIVE-dispatch shadow recorded a pre-wire visible-liquidity forecast for orders LIVE
// selected. It is intentionally isolated from normal Paper capital and LIVE authority. R163
// retires new rows because visible depth is not an authoritative IOC fill; the historical API
// labels those immutable rows as forecasts and excludes them from Paper/system P&L.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	liveDispatchShadowQueueCapacity   = 256
	liveDispatchShadowWriteTimeout    = 100 * time.Millisecond
	liveDispatchShadowQuietTime       = 1500 * time.Millisecond
	liveDispatchShadowRetryMin        = 100 * time.Millisecond
	liveDispatchShadowRetryMax        = 2 * time.Second
	liveDispatchShadowSettlementBatch = 32
)

func (s *Server) liveDispatchShadowCapture(bodyAuto bool, riskID string, reqPrice string,
	clientOrderID, ticker, title, side, system, route string, quantity, fee float64,
	quote liveMirrorQuote) *storage.LiveDispatchShadow {
	// Retired: this lane converted visible pre-wire depth into a simulated fill before CreateOrder
	// ran, then retained that "fill" even when Kalshi's authoritative IOC receipt was unfilled.
	// The unified execution-shadow already joins this pre-wire liquidity forecast to the exact LIVE
	// receipt and the delayed two-touch LIVE-policy mirror. Returning nil keeps every existing cash
	// call site nonblocking while preventing new fantasy-fill rows.
	return nil
}

func (s *Server) liveDispatchShadowQuietWindow() time.Duration {
	if s.liveDispatchShadowQuietDelay < 0 {
		return 0
	}
	if s.liveDispatchShadowQuietDelay > 0 {
		return s.liveDispatchShadowQuietDelay
	}
	return liveDispatchShadowQuietTime
}

// beginLiveCashPriority marks the entire signal/preflight/dispatch section, not only the final
// HTTP handler. Nested sections are safe; diagnostics resume only after the last owner exits and
// the quiet window has elapsed.
func (s *Server) beginLiveCashPriority() func() {
	if s == nil {
		return func() {}
	}
	s.liveDispatchShadowLivePriorityAt.Store(time.Now().UnixNano())
	s.liveDispatchShadowLiveActive.Add(1)
	return func() {
		s.liveDispatchShadowLivePriorityAt.Store(time.Now().UnixNano())
		s.liveDispatchShadowLiveActive.Add(-1)
	}
}

func (s *Server) liveCashWorkPending() bool {
	if s == nil {
		return false
	}
	if len(s.liveSignalIntentChannel()) > 0 {
		return true
	}
	s.liveMirrorMu.Lock()
	pending := len(s.liveMirrorQ) > 0
	s.liveMirrorMu.Unlock()
	return pending || liveSignalPreflightWorkPending(s)
}

// waitForLiveDispatchShadowQuiet keeps every diagnostic write behind the real-money handler.
// The shared database still has one SQLite writer, so this is a strict scheduling yield rather
// than a claim that the two ledgers are physically isolated.
func (s *Server) waitForLiveDispatchShadowQuiet(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		if s.liveDispatchShadowLiveActive.Load() > 0 || s.liveCashWorkPending() {
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return false
			}
			continue
		}
		quiet := s.liveDispatchShadowQuietWindow()
		last := s.liveDispatchShadowLivePriorityAt.Load()
		if quiet <= 0 || last <= 0 {
			return true
		}
		wait := quiet - time.Since(time.Unix(0, last))
		if wait <= 0 {
			return true
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		}
	}
}

func (s *Server) writeLiveDispatchShadow(ctx context.Context, row storage.LiveDispatchShadow) error {
	if s.liveDispatchShadowWrite != nil {
		return s.liveDispatchShadowWrite(ctx, row)
	}
	if s.store == nil {
		return fmt.Errorf("LIVE-dispatch shadow store unavailable")
	}
	_, _, err := s.store.InsertLiveDispatchShadow(ctx, row)
	return err
}

func (s *Server) persistLiveDispatchShadow(ctx context.Context, row storage.LiveDispatchShadow) error {
	backoff := liveDispatchShadowRetryMin
	logged := false
	for {
		if !s.waitForLiveDispatchShadowQuiet(ctx) {
			return ctx.Err()
		}
		writeCtx, cancel := context.WithTimeout(ctx, liveDispatchShadowWriteTimeout)
		err := s.writeLiveDispatchShadow(writeCtx, row)
		cancel()
		if err == nil {
			if logged && s.log != nil {
				s.log.Info("LIVE-dispatch shadow writer recovered",
					"reservation_id", row.ReservationID)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !logged && s.log != nil {
			logged = true
			s.log.Warn("LIVE-dispatch shadow writer delayed; queued row retained for retry",
				"reservation_id", row.ReservationID, "err", err)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
		if backoff < liveDispatchShadowRetryMax {
			backoff *= 2
			if backoff > liveDispatchShadowRetryMax {
				backoff = liveDispatchShadowRetryMax
			}
		}
	}
}

func (s *Server) auditDroppedLiveDispatchShadows(parent context.Context) {
	dropped := s.liveDispatchShadowWorkerDropped.Swap(0)
	if dropped == 0 || s.store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer cancel()
	detail, _ := json.Marshal(map[string]any{
		"dropped_rows": dropped, "queue_capacity": cap(s.liveDispatchShadowCh),
		"live_impact": "none; diagnostic shadow rows only",
	})
	if err := s.store.Audit(ctx, "error", "live-dispatch-shadow",
		fmt.Sprintf("LIVE-dispatch shadow queue dropped %d diagnostic row(s)", dropped),
		string(detail)); err != nil && s.log != nil {
		s.log.Error("LIVE-dispatch shadow overflow audit failed", "dropped", dropped, "err", err)
	}
}

func (s *Server) runLiveDispatchShadowWorker(ctx context.Context,
	in <-chan storage.LiveDispatchShadow, done chan<- struct{}) {
	defer close(done)
	for row := range in {
		if err := s.persistLiveDispatchShadow(ctx, row); err != nil {
			if s.log != nil {
				s.log.Error("LIVE-dispatch shadow writer stopped with an unpersisted row",
					"reservation_id", row.ReservationID, "err", err)
			}
			return
		}
		s.auditDroppedLiveDispatchShadows(ctx)
	}
	s.auditDroppedLiveDispatchShadows(context.Background())
}

// enqueueLiveDispatchShadow is nonblocking and must be called only after the corresponding real
// order has its durable ACK/rejection/ambiguity/terminal receipt. One worker owns every database
// attempt; a burst can never create one goroutine or one SQLite writer contender per order.
func (s *Server) enqueueLiveDispatchShadow(row *storage.LiveDispatchShadow) bool {
	if row == nil || s == nil || (s.store == nil && s.liveDispatchShadowWrite == nil) {
		return false
	}
	captured := *row
	s.liveDispatchShadowLivePriorityAt.Store(time.Now().UnixNano())
	s.liveDispatchShadowWorkerMu.Lock()
	defer s.liveDispatchShadowWorkerMu.Unlock()
	if s.liveDispatchShadowWorkerStopping {
		return false
	}
	if s.liveDispatchShadowCh == nil {
		capacity := s.liveDispatchShadowWorkerCapacity
		if capacity <= 0 {
			capacity = liveDispatchShadowQueueCapacity
		}
		s.liveDispatchShadowCh = make(chan storage.LiveDispatchShadow, capacity)
		s.liveDispatchShadowWorkerDone = make(chan struct{})
		workerCtx, cancel := context.WithCancel(context.Background())
		s.liveDispatchShadowWorkerCancel = cancel
		ch, done := s.liveDispatchShadowCh, s.liveDispatchShadowWorkerDone
		go s.runGuarded("live-dispatch-shadow-writer", func() {
			s.runLiveDispatchShadowWorker(workerCtx, ch, done)
		})
	}
	select {
	case s.liveDispatchShadowCh <- captured:
		return true
	default:
		dropped := s.liveDispatchShadowWorkerDropped.Add(1)
		if dropped == 1 && s.log != nil {
			s.log.Error("LIVE-dispatch shadow queue full; LIVE remained unblocked",
				"reservation_id", captured.ReservationID, "capacity", cap(s.liveDispatchShadowCh))
		}
		return false
	}
}

func (s *Server) stopLiveDispatchShadowWorker(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.liveDispatchShadowWorkerMu.Lock()
	if s.liveDispatchShadowCh == nil {
		s.liveDispatchShadowWorkerMu.Unlock()
		return nil
	}
	if !s.liveDispatchShadowWorkerStopping {
		s.liveDispatchShadowWorkerStopping = true
		close(s.liveDispatchShadowCh)
	}
	done, cancel := s.liveDispatchShadowWorkerDone, s.liveDispatchShadowWorkerCancel
	s.liveDispatchShadowWorkerMu.Unlock()

	select {
	case <-done:
		if cancel != nil {
			cancel()
		}
		return nil
	case <-ctx.Done():
		if cancel != nil {
			cancel()
		}
		return ctx.Err()
	}
}

func (s *Server) auditLiveDispatchShadowSettlementFailures(parent context.Context, failures []string) {
	if len(failures) == 0 || s.store == nil {
		return
	}
	total := len(failures)
	if len(failures) > 12 {
		failures = failures[:12]
	}
	detail, _ := json.Marshal(map[string]any{"failure_count": total, "samples": failures})
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 2*time.Second)
	defer cancel()
	if err := s.store.Audit(ctx, "error", "live-dispatch-shadow",
		fmt.Sprintf("shadow settlement pass had %d failure(s)", total), string(detail)); err != nil &&
		s.log != nil {
		s.log.Error("LIVE-dispatch shadow settlement failure audit failed",
			"failure_count", total, "err", err)
	}
}

func (s *Server) settleLiveDispatchShadows(ctx context.Context) {
	if s == nil || s.store == nil {
		return
	}
	s.liveDispatchShadowSettleMu.Lock()
	defer s.liveDispatchShadowSettleMu.Unlock()
	if !s.waitForLiveDispatchShadowQuiet(ctx) {
		return
	}
	after := s.liveDispatchShadowSettleAfterID
	rows, err := s.store.ListOpenLiveDispatchShadowsAfter(ctx, after,
		liveDispatchShadowSettlementBatch)
	if err == nil && len(rows) == 0 && after > 0 {
		s.liveDispatchShadowSettleAfterID = 0
		rows, err = s.store.ListOpenLiveDispatchShadowsAfter(ctx, 0,
			liveDispatchShadowSettlementBatch)
	}
	if err != nil {
		s.auditLiveDispatchShadowSettlementFailures(ctx,
			[]string{"settlement scan: " + err.Error()})
		return
	}
	if len(rows) == 0 {
		return
	}
	s.liveDispatchShadowSettleAfterID = rows[len(rows)-1].ID
	if len(rows) < liveDispatchShadowSettlementBatch {
		s.liveDispatchShadowSettleAfterID = 0
	}
	byVenue := make(map[string][]string)
	for _, row := range rows {
		byVenue[row.Venue] = append(byVenue[row.Venue], row.Ticker)
	}
	receipts := make(map[string]storage.VenueSettlementReceipt)
	failures := make([]string, 0)
	for venue, tickers := range byVenue {
		batch, batchErr := s.store.VenueSettlementsForTickers(ctx, venue, tickers)
		if batchErr != nil {
			failures = append(failures, venue+" settlement batch: "+batchErr.Error())
			continue
		}
		for ticker, receipt := range batch {
			receipts[venue+"\x00"+ticker] = receipt
		}
	}
	for _, row := range rows {
		settlement, ok := receipts[row.Venue+"\x00"+row.Ticker]
		if !ok {
			continue
		}
		if row.State == storage.LiveDispatchShadowResting {
			if _, err := s.store.CancelLiveDispatchShadow(ctx, row.ID, settlement.ResolvedAt,
				"market-resolved-before-shadow-maker-fill"); err != nil {
				failures = append(failures,
					fmt.Sprintf("%s cancel shadow %d: %v", row.Ticker, row.ID, err))
			}
			continue
		}
		if _, err := s.store.SettleLiveDispatchShadow(ctx, row.ID, settlement.YesValue,
			settlement.ResolvedAt, settlement.SourceArtifact, settlement.Hash); err != nil {
			failures = append(failures,
				fmt.Sprintf("%s settle shadow %d: %v", row.Ticker, row.ID, err))
		}
	}
	s.auditLiveDispatchShadowSettlementFailures(ctx, failures)
}

type liveDispatchShadowView struct {
	ID                     int64   `json:"id"`
	ReservationID          string  `json:"reservation_id"`
	Ticker                 string  `json:"ticker"`
	Side                   string  `json:"side"`
	System                 string  `json:"system"`
	Route                  string  `json:"route"`
	State                  string  `json:"state"`
	RequestedQty           float64 `json:"requested_qty"`
	ShadowFilledQty        float64 `json:"shadow_filled_qty"`
	ShadowPrice            float64 `json:"shadow_price"`
	ShadowFee              float64 `json:"shadow_fee"`
	ShadowNet              float64 `json:"shadow_net"`
	ShadowNetKnown         bool    `json:"shadow_net_known"`
	LiveNet                float64 `json:"live_net"`
	LiveNetKnown           bool    `json:"live_net_known"`
	LiveFilledQty          float64 `json:"live_filled_qty"`
	LivePrice              float64 `json:"live_price"`
	LiveFee                float64 `json:"live_fee"`
	LiveExecutionState     string  `json:"live_execution_state"`
	ComparisonCompleteness string  `json:"comparison_completeness"`
	CapturedAt             string  `json:"captured_at"`
	EvidenceClass          string  `json:"evidence_class"`
	PaperPnLEligible       bool    `json:"paper_pnl_eligible"`
	VerdictEligible        bool    `json:"verdict_eligible"`
	LegacyForecastQty      float64 `json:"legacy_pre_wire_visible_qty_forecast"`
}

func shadowSideValue(side string, yesValue float64) float64 {
	if strings.EqualFold(side, "NO") {
		return 1 - yesValue
	}
	return yesValue
}

func liveDispatchShadowNet(quantity, price, fee, sideValue float64) float64 {
	return quantity*(sideValue-price) - fee
}

// liveDispatchShadowCachedExitMark is diagnostic and cached-only. It may read the in-memory WS
// book or the portfolio mark cache, but it never borrows the priority REST lane.
func (s *Server) liveDispatchShadowCachedExitMark(ticker, side string) (float64, bool) {
	mark := func(book *kalshi.Orderbook) float64 {
		if strings.EqualFold(strings.TrimSpace(side), "NO") {
			if len(book.YesAsks) > 0 {
				return math.Max(0, math.Min(1, 1-book.YesAsks[0].Price))
			}
			return 0
		}
		if len(book.YesBids) > 0 {
			return math.Max(0, math.Min(1, book.YesBids[0].Price))
		}
		return 0
	}
	if s != nil && s.kal != nil {
		if book, _, ok := s.kal.LiveBook(ticker, 5*time.Second); ok && book != nil {
			return mark(book), true
		}
	}
	if s == nil {
		return 0, false
	}
	s.kobMu.Lock()
	defer s.kobMu.Unlock()
	cached, ok := s.kobCache[ticker]
	if !ok || !cached.bookKnown || time.Since(cached.at) >= 4*time.Second {
		return 0, false
	}
	if strings.EqualFold(strings.TrimSpace(side), "NO") {
		return cached.noBid, true
	}
	return cached.yesBid, true
}

func livePendingExecution(row storage.LivePendingRiskReservation) (state string,
	qty, price, fee float64, known bool) {
	for _, event := range row.Events {
		switch event.EventType {
		case storage.LivePendingRiskTerminalUnfilled:
			return "terminal_unfilled", 0, 0, 0, true
		case storage.LivePendingRiskCleanRejected:
			return "clean_rejected", 0, 0, 0, true
		}
	}
	if len(row.Legs) != 1 {
		return "incomplete", 0, 0, 0, false
	}
	attemptKey := ""
	for _, event := range row.Events {
		if event.EventType != storage.LivePendingRiskSubmitStarted ||
			event.ClientOrderID != row.Legs[0].ClientOrderID {
			continue
		}
		if attemptKey != "" && attemptKey != event.AttemptKey {
			return "incomplete", 0, 0, 0, false
		}
		attemptKey = event.AttemptKey
	}
	identity, err := r148RiskAttemptIdentityFor(row, attemptKey, "")
	if err != nil || !identity.FillKnown ||
		!r151ExactScopedFillSource(row.Legs[0].Venue, identity.Fill.ReceiptSource) {
		return "incomplete", 0, 0, 0, false
	}
	return "filled", identity.Fill.FilledQty, identity.Fill.AveragePrice, identity.Fill.FeeTotal, true
}

func (s *Server) liveDispatchShadowRows(ctx context.Context, limit int) ([]liveDispatchShadowView, map[string]any, error) {
	rows, err := s.store.ListLiveDispatchShadows(ctx, limit)
	if err != nil {
		return nil, nil, err
	}
	riskIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		riskIDs = append(riskIDs, row.ReservationID)
	}
	risks, err := s.store.LivePendingRiskExecutionsByID(ctx, riskIDs)
	if err != nil {
		return nil, nil, err
	}
	out := make([]liveDispatchShadowView, 0, len(rows))
	liveNet := 0.0
	liveKnownN, liveZeroN := 0, 0
	type markKey struct {
		ticker string
		side   string
	}
	type cachedMark struct {
		value float64
		known bool
	}
	marks := make(map[markKey]cachedMark)
	for _, row := range rows {
		view := liveDispatchShadowView{
			ID: row.ID, ReservationID: row.ReservationID, Ticker: row.Ticker, Side: row.Side,
			System: row.SystemID, Route: row.Route,
			State:           "legacy_pre_wire_liquidity_forecast",
			RequestedQty:    row.RequestedQty,
			ShadowFilledQty: row.FilledQty, ShadowPrice: row.FillPrice, ShadowFee: row.FillFee,
			CapturedAt:             row.Captured.Format(time.RFC3339Nano),
			ComparisonCompleteness: "legacy forecast; excluded from Paper and system verdict P&L",
			EvidenceClass:          "legacy_pre_wire_visible_liquidity_forecast",
			PaperPnLEligible:       false,
			VerdictEligible:        false,
			LegacyForecastQty:      row.FilledQty,
		}
		sideValue, valueKnown := 0.0, false
		if row.State == storage.LiveDispatchShadowSettled {
			sideValue, valueKnown = shadowSideValue(row.Side, row.SettlementValue), true
		} else if row.State == storage.LiveDispatchShadowFilled {
			key := markKey{ticker: row.Ticker, side: row.Side}
			cached, exists := marks[key]
			if !exists {
				cached.value, cached.known = s.liveDispatchShadowCachedExitMark(row.Ticker, row.Side)
				marks[key] = cached
			}
			sideValue, valueKnown = cached.value, cached.known
		}
		risk, found := risks[row.ReservationID]
		if found {
			state, qty, price, fee, executionKnown := livePendingExecution(risk)
			view.LiveExecutionState = state
			view.LiveFilledQty, view.LivePrice, view.LiveFee = qty, price, fee
			if executionKnown && qty == 0 {
				view.LiveNetKnown = true
				view.LiveNet = 0
				liveKnownN++
				liveZeroN++
			} else if executionKnown && valueKnown {
				view.LiveNet = liveDispatchShadowNet(qty, price, fee, sideValue)
				view.LiveNetKnown = true
				liveNet += view.LiveNet
				liveKnownN++
			}
		}
		out = append(out, view)
	}
	summary := map[string]any{
		"scope":    "historical legacy pre-wire visible-liquidity forecasts; not Paper fills",
		"coverage": "historical_auto_kalshi_taker_only", "maker_shadow_enabled": false,
		"legacy_historical_only": true, "new_rows_enabled": false,
		"paper_pnl_eligible": false, "system_verdict_eligible": false,
		"replacement": "/api/execution-shadow joined to the delayed LIVE-policy mirror",
		"orders":      len(out), "shadow_net_known_orders": 0, "live_net_known_orders": liveKnownN,
		"complete_pairs": 0, "live_zero_fill_orders": liveZeroN,
		"shadow_net_usd": nil, "paired_shadow_net_usd": nil,
		"paired_live_net_usd": nil, "paired_live_minus_shadow_usd": nil,
		"live_net_usd": math.Round(liveNet*10000) / 10000,
	}
	return out, summary, nil
}

func (s *Server) handleLiveDispatchShadow(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	rows, summary, err := s.liveDispatchShadowRows(ctx, 500)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"summary": summary, "rows": rows, "generated_at": time.Now().UTC().Format(time.RFC3339Nano),
		"note": fmt.Sprintf("%d immutable historical pre-wire liquidity forecasts shown. They are not Paper fills and are excluded from profit and system verdicts; use /api/execution-shadow for the delayed mirror versus authoritative LIVE.", len(rows)),
	})
}
