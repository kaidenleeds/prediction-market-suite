package server

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func kalshiOrderFillAggregate(rows []kalshi.Fill, orderID, side string) (qty, avg, fee float64, feeKnown bool, ids []string) {
	weighted := 0.0
	seen := map[string]bool{}
	feeKnown = true
	for _, f := range rows {
		if strings.TrimSpace(orderID) == "" || f.OrderID != orderID || strings.TrimSpace(f.FillID) == "" ||
			seen[f.FillID] || !strings.EqualFold(f.SideYesNo(), side) || f.Qty() <= 0 {
			continue
		}
		px := f.YesPriceUSD()
		if strings.EqualFold(side, "NO") {
			px = f.NoPriceUSD()
		}
		if px <= 0 || px >= 1 {
			continue
		}
		seen[f.FillID] = true
		feeKnown = feeKnown && f.FeeKnown()
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

// reconcilePendingKalshiPromotionExecutions advances durable pending/partial sealed single orders
// from exact order-id fills. It runs independent of ARM/AUTO so a restart cannot strand money truth.
func (s *Server) reconcilePendingKalshiPromotionExecutions(ctx context.Context) {
	if s.store == nil || s.kal == nil || !s.kal.HasCredentials() {
		return
	}
	pending, err := s.store.PendingResearchLiveExecutions(ctx, 25)
	if err != nil || len(pending) == 0 {
		return
	}
	for _, p := range pending {
		r := p.Receipt
		if !strings.EqualFold(r.Venue, "kalshi") || strings.TrimSpace(r.OrderID) == "" {
			continue
		}
		order, orderErr := s.kal.GetOrder(ctx, r.OrderID)
		fills, fillErr := s.kal.GetFillsForOrder(ctx, r.OrderID)
		if fillErr != nil {
			continue
		}
		qty, avg, fee, fillFeesKnown, ids := kalshiOrderFillAggregate(fills, r.OrderID, r.Side)
		if qty > 0 {
			maker := strings.EqualFold(r.Route, "maker")
			scheduled, known, authority := s.kalFeeExact(r.Ticker, maker, qty, avg)
			if !fillFeesKnown || !known || math.Abs(scheduled-fee) > .010000001 {
				continue
			}
			state := "partial"
			remaining := math.Max(0, r.RequestedQty-qty)
			status := strings.ToLower(strings.TrimSpace(order.Status))
			terminal := orderErr == nil && (status == "canceled" || status == "cancelled" || status == "expired" || status == "rejected")
			if terminal {
				// The requested-but-canceled remainder is no longer working. Zero remainder marks this
				// authoritative partial terminal so the durable queue does not poll it forever.
				remaining = 0
			}
			if qty+1e-9 >= r.RequestedQty {
				state, remaining = "full", 0
			}
			detail := fmt.Sprintf("order_id=%s fill_ids=%s order_status=%s fee_authority=%s",
				r.OrderID, strings.Join(ids, ","), order.Status, authority)
			_ = s.store.AppendResearchLiveExecutionReceipt(context.WithoutCancel(ctx), p.Intent,
				storage.ResearchLiveExecutionReceipt{State: state, OrderID: r.OrderID,
					RequestedQty: r.RequestedQty, FilledQty: qty, RemainingQty: remaining,
					AveragePrice: avg, FeeTotal: fee, FeeKnown: true, Authoritative: true,
					ReceiptSource: "kalshi-portfolio-fills+order", Detail: detail})
			continue
		}
		if orderErr != nil {
			continue
		}
		status := strings.ToLower(strings.TrimSpace(order.Status))
		switch status {
		case "canceled", "cancelled", "expired", "rejected":
			if order.Filled() <= 0 && order.Remaining() <= 0 {
				_ = s.store.AppendResearchLiveExecutionReceipt(context.WithoutCancel(ctx), p.Intent,
					storage.ResearchLiveExecutionReceipt{State: "unfilled", OrderID: r.OrderID,
						RequestedQty: r.RequestedQty, AveragePrice: r.AveragePrice,
						Authoritative: true, ReceiptSource: "kalshi-order-terminal",
						Detail: "authoritative terminal order state with zero fill: " + status})
			}
		case "executed", "filled":
			// Terminal execution with no matching fill inside the authoritative 24h feed cannot be
			// priced or fee-reconciled. Freeze ambiguity rather than fabricate success/unfilled.
			_ = s.store.AppendResearchLiveExecutionReceipt(context.WithoutCancel(ctx), p.Intent,
				storage.ResearchLiveExecutionReceipt{State: "ambiguous", OrderID: r.OrderID,
					RequestedQty: r.RequestedQty, FilledQty: math.Max(0, order.Filled()),
					RemainingQty: math.Max(0, order.Remaining()), AveragePrice: r.AveragePrice,
					Authoritative: false, ReceiptSource: "kalshi-order-without-fill-receipt",
					Detail: "terminal execution exists but exact fill price/fee is unavailable"})
		}
	}
}

func splitPolyUSCommission(v float64) (fee, rebate float64) {
	if v < 0 {
		return 0, -v
	}
	return v, 0
}

func polyUSPromotionOrderReceipt(p storage.PendingResearchLiveExecution, st polymarketus.OrderState) storage.ResearchLiveExecutionReceipt {
	r := p.Receipt
	out := storage.ResearchLiveExecutionReceipt{State: "pending", OrderID: r.OrderID,
		RequestedQty: r.RequestedQty, RemainingQty: r.RequestedQty, AveragePrice: r.AveragePrice,
		ReceiptSource: "polyus-get-order", Detail: "authoritative order state remains open"}
	ambiguous := func(reason string) storage.ResearchLiveExecutionReceipt {
		out.State, out.Authoritative, out.FeeKnown, out.Detail = "ambiguous", false, false, reason
		out.FilledQty = math.Max(0, st.Cum)
		out.RemainingQty = math.Max(0, st.Leaves)
		if st.AvgPx > 0 && st.AvgPx < 1 {
			out.AveragePrice = st.AvgPx
		}
		return out
	}
	if strings.TrimSpace(r.OrderID) == "" || st.ID != r.OrderID {
		return ambiguous("exact venue order id does not match the durable Paper promotion receipt")
	}
	expectedIntent := "ORDER_INTENT_BUY_LONG"
	if strings.EqualFold(r.Side, "NO") {
		expectedIntent = "ORDER_INTENT_BUY_SHORT"
	}
	if st.Intent != expectedIntent {
		return ambiguous("venue order intent does not match the sealed YES/NO side")
	}
	expectedTIF := "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL"
	if strings.EqualFold(r.Route, "maker") {
		expectedTIF = "TIME_IN_FORCE_GOOD_TILL_CANCEL"
	}
	if st.TIF != expectedTIF {
		return ambiguous("venue time-in-force does not match the sealed maker/taker route")
	}
	if math.IsNaN(st.Cum) || math.IsInf(st.Cum, 0) || math.IsNaN(st.Leaves) || math.IsInf(st.Leaves, 0) ||
		st.Cum < 0 || st.Leaves < 0 || st.Cum > r.RequestedQty+1e-8 || st.Cum+st.Leaves > r.RequestedQty+1e-8 {
		return ambiguous("venue quantity receipt is inconsistent with the sealed requested quantity")
	}
	if st.CommissionTotalKnown && (math.IsNaN(st.CommissionTotalUSD) || math.IsInf(st.CommissionTotalUSD, 0)) {
		return ambiguous("venue cumulative commission is non-finite")
	}
	if st.Cum > 0 && (st.AvgPx <= 0 || st.AvgPx >= 1 || math.IsNaN(st.AvgPx) || math.IsInf(st.AvgPx, 0)) {
		return ambiguous("venue reports a fill without a valid side-specific average price")
	}
	if st.CommissionTotalKnown {
		out.FeeTotal, out.RebateTotal = splitPolyUSCommission(st.CommissionTotalUSD)
		out.FeeKnown = true
	}
	out.FilledQty = st.Cum
	if st.AvgPx > 0 && st.AvgPx < 1 {
		out.AveragePrice = st.AvgPx
	}

	switch {
	case st.DeadUnfilled():
		out.State, out.RemainingQty, out.Authoritative = "unfilled", 0, true
		out.FeeKnown = true // terminal zero fill cannot incur a fill commission or rebate
		out.FeeTotal, out.RebateTotal = 0, 0
		out.Detail = "authoritative terminal PolyUS order state with zero fill: " + st.State
	case st.State == "ORDER_STATE_FILLED":
		if st.Cum+1e-8 < r.RequestedQty || st.Leaves > 1e-8 || !st.CommissionTotalKnown {
			return ambiguous("venue says filled but quantity, remainder, or signed commission receipt is incomplete")
		}
		out.State, out.RemainingQty, out.Authoritative = "full", 0, true
		out.Detail = "authoritative PolyUS order state reconciled exact side, route, fill, fee, and rebate"
	case st.Terminal() && st.Cum > 0:
		if st.Leaves > 1e-8 || !st.CommissionTotalKnown {
			return ambiguous("terminal partial fill lacks an exact zero remainder or signed commission receipt")
		}
		out.State, out.RemainingQty, out.Authoritative = "partial", 0, true
		out.Detail = "authoritative terminal PolyUS partial fill; unfilled remainder is no longer working"
	case st.Cum > 0:
		if st.Leaves <= 0 {
			return ambiguous("nonterminal partial fill has no authoritative working remainder")
		}
		out.State, out.RemainingQty, out.Authoritative = "partial", st.Leaves, true
		out.Detail = "authoritative open PolyUS partial fill; later terminal truth remains queued"
	default:
		if st.Leaves > 0 {
			out.RemainingQty = st.Leaves
		}
		out.Authoritative = true
	}
	return out
}

// reconcilePendingPolyUSPromotionExecutions is the restart-safe counterpart to the fast post-place
// verifier. It is independent of ARM/AUTO and keeps polling a maker beyond the original eight-second
// observation window until exact terminal order truth is attached to the sealed Paper intent.
func (s *Server) reconcilePendingPolyUSPromotionExecutions(ctx context.Context) {
	if s.store == nil || s.polyUSAuth == nil {
		return
	}
	pending, err := s.store.PendingResearchLiveExecutionsForVenue(ctx, "polyus", 25)
	if err != nil || len(pending) == 0 {
		return
	}
	for _, p := range pending {
		if strings.TrimSpace(p.Receipt.OrderID) == "" {
			continue
		}
		st, getErr := s.polyUSAuth.GetOrderState(ctx, p.Receipt.OrderID)
		if getErr != nil {
			continue
		}
		next := polyUSPromotionOrderReceipt(p, st)
		if err := s.store.AppendResearchLiveExecutionReceipt(context.WithoutCancel(ctx), p.Intent, next); err != nil {
			_ = s.store.Audit(context.WithoutCancel(ctx), "critical", "live",
				"PolyUS promotion order truth could not be persisted", p.Receipt.OrderID+": "+err.Error())
		}
	}
}
