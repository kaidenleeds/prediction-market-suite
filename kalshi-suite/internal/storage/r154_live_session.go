package storage

import (
	"context"
	"time"
)

// LiveAcceptedTotals is exact accepted real-order activity. It intentionally reads the immutable
// account_visible receipts rather than current positions, the UTC-day telemetry counter, or Paper.
type LiveAcceptedTotals struct {
	Orders, Contracts    float64
	PrincipalUSD, FeeUSD float64
	TotalCostUSD         float64
}

// LiveAcceptedTotalsSince returns actual filled quantity, average fill price and account-reported
// fee for every accepted order since the activation epoch. A closed/settled order remains present,
// so cumulative turnover and fee monitoring cannot be erased by a restart or UTC rollover.
func (s *Store) LiveAcceptedTotalsSince(ctx context.Context, since time.Time) (LiveAcceptedTotals, error) {
	var out LiveAcceptedTotals
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(*),
       COALESCE(SUM(e.filled_qty),0),
       COALESCE(SUM(e.filled_qty*e.average_price),0),
       COALESCE(SUM(e.fee_total),0)
FROM live_pending_risk_events e
JOIN live_pending_risk_intents i ON i.reservation_id=e.reservation_id
WHERE e.event_type='account_visible' AND i.created_ts>=?`,
		since.UTC().Format(time.RFC3339Nano)).
		Scan(&out.Orders, &out.Contracts, &out.PrincipalUSD, &out.FeeUSD)
	out.TotalCostUSD = out.PrincipalUSD + out.FeeUSD
	return out, err
}
