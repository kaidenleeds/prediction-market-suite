package server

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
)

// cancelAllPolyUSOrdersForArm is the pre-session orphan-order sweep. The function seam exists only
// so the handler's fail/retry state machine can be tested without touching a venue; production
// always falls through to the authenticated retail client.
func (s *Server) cancelAllPolyUSOrdersForArm(ctx context.Context) ([]string, error) {
	if s.pusArmSweep != nil {
		return s.pusArmSweep(ctx, nil)
	}
	return s.polyUSAuth.CancelAllLiveOrders(ctx, nil)
}

func (s *Server) openPolyUSOrdersForArm(ctx context.Context) ([]polymarketus.PUSOrder, error) {
	if s.pusArmOpenOrders != nil {
		return s.pusArmOpenOrders(ctx)
	}
	return s.polyUSAuth.OpenOrders(ctx)
}

func (s *Server) polyUSPositionsForArm(ctx context.Context) ([]polymarketus.PUSPosition, error) {
	if s.pusArmPositions != nil {
		return s.pusArmPositions(ctx)
	}
	return s.polyUSAuth.LivePositions(ctx)
}

func kalshiArmCleanSlateReason(orders []kalshi.Order, readErr error) string {
	if readErr != nil {
		return "Kalshi post-cancel order verification unreadable: " + readErr.Error()
	}
	// GetOrders itself requests status=resting. Status is a redundant response field that has
	// disappeared in other Kalshi schema cutovers; every returned row is therefore resting truth.
	if len(orders) > 0 {
		return fmt.Sprintf("Kalshi post-cancel verification still reports resting order %s", orders[0].OrderID)
	}
	return ""
}

// kalshiRestingOrderIDs consumes the response contract of GetOrders, whose request already carries
// status=resting. Do not re-filter on the optional response Status field: an omitted field must not
// strand a real resting order. Rows without an id are counted as failures so ARM/kill fail closed.
func kalshiRestingOrderIDs(orders []kalshi.Order) (ids []string, missingID int) {
	ids = make([]string, 0, len(orders))
	for _, order := range orders {
		id := strings.TrimSpace(order.OrderID)
		if id == "" {
			missingID++
			continue
		}
		ids = append(ids, id)
	}
	return ids, missingID
}

func polyUSArmCleanSlateReason(orders []polymarketus.PUSOrder, readErr error) string {
	if readErr != nil {
		return "PolyUS post-cancel order verification unreadable: " + readErr.Error()
	}
	if len(orders) > 0 {
		return fmt.Sprintf("PolyUS post-cancel verification still reports %d open order(s)", len(orders))
	}
	return ""
}

func liveArmBankrollReason(venue string, required bool, bank float64, source string) string {
	if !required {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(source), "unknown") || strings.TrimSpace(source) == "" {
		return fmt.Sprintf("%s authenticated bankroll/NAV is unreadable", venue)
	}
	if bank <= 0 || math.IsNaN(bank) || math.IsInf(bank, 0) {
		return fmt.Sprintf("%s authenticated bankroll/NAV is not positive", venue)
	}
	return ""
}
