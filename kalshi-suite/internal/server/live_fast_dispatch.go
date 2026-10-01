package server

import (
	"context"
	"fmt"
)

// liveFastIntentDispatchSafe proves the narrow condition under which a freshly preflighted single
// may reach its final handler before the loop's background recovery pass. It never grants money
// authority: the handler still repeats account, book, fee, depth, cluster, sizing and durable-risk
// checks. Any unreadable or unresolved money journal keeps the candidate queued for recovery-first
// handling instead.
func (s *Server) liveFastIntentDispatchSafe(ctx context.Context) (bool, string) {
	if s == nil || s.store == nil {
		return false, "durable-live-store-unavailable"
	}
	risks, err := s.store.ActiveLivePendingRisk(ctx)
	if err != nil {
		return false, "pending-risk-read-failed:" + err.Error()
	}
	if len(risks) != 0 {
		return false, fmt.Sprintf("pending-risk-active:%d", len(risks))
	}
	staged, err := s.store.HasPendingStagedBundleExecutions(ctx)
	if err != nil {
		return false, "staged-journal-read-failed:" + err.Error()
	}
	if staged {
		return false, "staged-journal-recovery-required"
	}
	return true, "no-unresolved-durable-money-state"
}
