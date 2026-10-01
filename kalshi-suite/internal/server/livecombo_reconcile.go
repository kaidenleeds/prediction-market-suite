package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// liveComboFillAggregate admits only fills carrying the RFQ creator order id from the exact scoped
// quote. The MVE ticker is not unique over its lifetime and can contain older/manual fills.
func liveComboFillAggregate(rows []kalshi.Fill, market, orderID string) (qty, avg, fee float64, feeKnown bool, ids []string) {
	weighted := 0.0
	seen := map[string]bool{}
	feeKnown = true
	for _, f := range rows {
		if !strings.EqualFold(strings.TrimSpace(f.Ticker), strings.TrimSpace(market)) ||
			!strings.EqualFold(f.SideYesNo(), "yes") || strings.TrimSpace(f.FillID) == "" ||
			strings.TrimSpace(orderID) == "" || f.OrderID != orderID || f.Qty() <= 0 || seen[f.FillID] {
			continue
		}
		seen[f.FillID] = true
		feeKnown = feeKnown && f.FeeKnown()
		px := f.YesPriceUSD()
		if px <= 0 || px >= 1 {
			continue
		}
		q := f.Qty()
		qty += q
		weighted += q * px
		fee += math.Max(0, f.FeeUSD())
		ids = append(ids, f.FillID)
	}
	if qty > 0 {
		avg = weighted / qty
	} else {
		feeKnown = false
	}
	return
}

var liveComboReconcileInFlight sync.Map

func (s *Server) liveComboTerminalUnfilled(ctx context.Context, c storage.LiveCombo) bool {
	quote, err := s.kal.GetRFQQuote(ctx, c.RFQID, c.QuoteID)
	if err != nil || quote.ID != c.QuoteID || quote.RFQID != c.RFQID {
		return false
	}
	status := strings.ToLower(strings.TrimSpace(quote.Status))
	if status != "cancelled" && status != "canceled" && status != "expired" && status != "rejected" {
		return false
	}
	orderID := strings.TrimSpace(quote.RFQCreatorOrderID)
	if orderID == "" {
		if err := s.store.MarkLiveComboUnfilled(context.WithoutCancel(ctx), c.ID, c.Market,
			"kalshi-rfq-scoped-quote", "terminal quote with no creator order: "+status); err != nil {
			return false
		}
		if risk, found, _ := s.r148ActiveComboRisk(ctx, c.RFQID, c.QuoteID); found {
			if err := s.r148ComboTerminalUnfilledRisk(ctx, risk, "", "kalshi-rfq-scoped-quote",
				"terminal quote with no creator order: "+status,
				map[string]any{"rfq_id": c.RFQID, "quote_id": c.QuoteID, "quote_status": status}); err != nil {
				return false
			}
		}
		return true
	}
	fills, err := s.kal.GetFillsForOrder(ctx, orderID)
	if err != nil {
		return false
	}
	if qty, _, _, _, _ := liveComboFillAggregate(fills, c.Market, orderID); qty > 0 {
		return false
	}
	order, err := s.kal.GetOrder(ctx, orderID)
	if err != nil || order.Filled() > 0 || order.Remaining() > 0 {
		return false
	}
	orderStatus := strings.ToLower(strings.TrimSpace(order.Status))
	if orderStatus != "cancelled" && orderStatus != "canceled" && orderStatus != "expired" && orderStatus != "rejected" {
		return false
	}
	if err := s.store.MarkLiveComboUnfilled(context.WithoutCancel(ctx), c.ID, c.Market,
		"kalshi-rfq-scoped-quote+order+fills", "terminal exact order and zero order-id fills"); err != nil {
		return false
	}
	if risk, found, _ := s.r148ActiveComboRisk(ctx, c.RFQID, c.QuoteID); found {
		if err := s.r148ComboTerminalUnfilledRisk(ctx, risk, orderID,
			"kalshi-rfq-scoped-quote+order+fills", "terminal exact order and zero order-id fills",
			map[string]any{"rfq_id": c.RFQID, "quote_id": c.QuoteID, "order_id": orderID,
				"quote_status": status, "order_status": orderStatus}); err != nil {
			return false
		}
	}
	return true
}

// reconcileLiveComboOnce uses the exact RFQ-scoped quote resource to obtain the immutable creator
// order id, then joins only that order to /portfolio/fills. It is restart-safe because every join
// key is stored on live_combos before this method runs.
func (s *Server) reconcileLiveComboOnce(ctx context.Context, c storage.LiveCombo) bool {
	if c.ID <= 0 || c.RFQID == "" || c.QuoteID == "" || s.kal == nil {
		return false
	}
	quote, qerr := s.kal.GetRFQQuote(ctx, c.RFQID, c.QuoteID)
	creatorOrderID := strings.TrimSpace(c.RFQCreatorOrderID)
	if qerr == nil {
		if quote.ID != c.QuoteID || quote.RFQID != c.RFQID ||
			(quote.MarketTicker != "" && quote.MarketTicker != c.Market) {
			return false
		}
		wireOrderID := strings.TrimSpace(quote.RFQCreatorOrderID)
		if creatorOrderID != "" && wireOrderID != "" && creatorOrderID != wireOrderID {
			_ = s.store.Audit(context.WithoutCancel(ctx), "critical", "live",
				"RFQ creator order identity changed during combo reconciliation", c.Market)
			return false
		}
		if creatorOrderID == "" && wireOrderID != "" {
			creatorOrderID = wireOrderID
			if err := s.store.SetLiveComboRFQCreatorOrderID(context.WithoutCancel(ctx), c.ID, creatorOrderID); err != nil {
				return false
			}
		}
	}
	if creatorOrderID == "" {
		return false // accepted/confirmed communication has not exposed its order identity yet
	}
	fills, err := s.kal.GetFillsForOrder(ctx, creatorOrderID)
	if err != nil {
		return false
	}
	qty, avg, fee, fillFeesKnown, ids := liveComboFillAggregate(fills, c.Market, creatorOrderID)
	if qty <= 0 {
		return false
	}
	positionQty := 0.0
	if positions, err := s.kal.GetPositions(ctx); err == nil {
		for _, p := range positions {
			if strings.EqualFold(p.Ticker, c.Market) {
				positionQty = p.PositionQty()
				break
			}
		}
	}
	scheduledFee, feeKnown, feeAuthority := s.kalFeeExact(c.Market, false, qty, avg)
	if !fillFeesKnown || !feeKnown || math.Abs(scheduledFee-fee) > .010000001 {
		return false // missing fee_cost zero cannot masquerade as a genuine charged zero
	}
	state := storage.LiveComboPartial
	if qty+1e-9 >= c.Contracts {
		state = storage.LiveComboFilled
	}
	quoteStatus := "unavailable-after-accept"
	if qerr == nil && strings.TrimSpace(quote.Status) != "" {
		quoteStatus = quote.Status
	}
	receiptJSON, _ := json.Marshal(map[string]any{
		"rfq_id": c.RFQID, "quote_id": c.QuoteID, "quote_status": quoteStatus,
		"rfq_creator_order_id": creatorOrderID, "fill_ids": ids, "filled_quantity": qty,
		"average_price": avg, "actual_fee": fee, "position_quantity": positionQty,
		"fill_source": "kalshi-portfolio-fills", "position_source": "kalshi-portfolio-positions",
	})
	feeSource := "kalshi-portfolio-fills:fee_cost+" + feeAuthority
	if err := s.store.ConfirmLiveComboExecution(context.WithoutCancel(ctx), c.ID, c.Market, state,
		qty, fee, feeSource, string(receiptJSON)); err != nil {
		return false
	}
	if risk, found, riskErr := s.r148ActiveComboRisk(ctx, c.RFQID, c.QuoteID); riskErr != nil {
		return false
	} else if found {
		if riskErr = s.r148ComboRecordFillAndVisibility(ctx, risk, creatorOrderID, qty, avg, fee,
			positionQty, "kalshi-portfolio-fills+positions", json.RawMessage(receiptJSON)); riskErr != nil {
			return false
		}
	}
	if state != storage.LiveComboFilled {
		return false
	}
	if c.PromotionIntentID != "" {
		intent, found, err := s.store.ResearchRouteBundlePromotionIntent(ctx, c.PromotionIntentID)
		if err != nil || !found {
			_ = s.store.Audit(context.WithoutCancel(ctx), "critical", "live",
				"typed combo filled but immutable promotion intent could not be reloaded", c.Market)
			return true
		}
		fillIDs, _ := json.Marshal(ids)
		if err := s.store.InsertResearchRouteBundleLiveFill(context.WithoutCancel(ctx), storage.ResearchRouteBundleLiveFill{
			IntentID: intent.IntentID, MarketTicker: c.Market, RFQID: c.RFQID, QuoteID: c.QuoteID,
			QuoteStatus: quoteStatus, FilledQuantity: qty, AveragePrice: avg, ActualFee: fee,
			FeeSource: feeSource, FillIDsJSON: string(fillIDs), PositionQuantity: positionQty,
		}); err != nil {
			_ = s.store.Audit(context.WithoutCancel(ctx), "critical", "live",
				"typed combo filled but immutable live_filled receipt failed", c.Market+": "+err.Error())
		}
	}
	s.liveLogAdd(map[string]any{"event": "COMBO-LIVE-FILLED", "market": c.Market,
		"filled_quantity": qty, "average_price": avg, "actual_fee": fee,
		"quote_status": quoteStatus, "rfq_creator_order_id": creatorOrderID,
		"position_quantity": positionQty})
	_ = s.store.Audit(context.WithoutCancel(ctx), "warn", "live",
		fmt.Sprintf("LIVE combo fill reconciled %s %.4f @ %.4f fee $%.4f", c.Market, qty, avg, fee), "")
	return true
}

// reconcileLiveComboDispatch gives a just-accepted quote the documented confirmation window.
// The durable recurring sweep calls the same exact method after restart or a late fill. sync.Map
// prevents the fast path and sweep from reconciling one row concurrently.
func (s *Server) reconcileLiveComboDispatch(c storage.LiveCombo) {
	if _, loaded := liveComboReconcileInFlight.LoadOrStore(c.ID, struct{}{}); loaded {
		return
	}
	defer liveComboReconcileInFlight.Delete(c.ID)
	for _, wait := range []time.Duration{800 * time.Millisecond, 1200 * time.Millisecond, 1500 * time.Millisecond, 2 * time.Second} {
		time.Sleep(wait)
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		done := s.reconcileLiveComboOnce(ctx, c)
		cancel()
		if done {
			return
		}
	}
	s.liveLogAdd(map[string]any{"event": "COMBO-FILL-PENDING", "market": c.Market,
		"rfq_id": c.RFQID, "quote_id": c.QuoteID,
		"note": "accept dispatched; no authoritative full fill receipt inside bounded confirmation window; durable sweep continues"})
}
