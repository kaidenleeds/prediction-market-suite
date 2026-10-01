package server

import (
	"context"
	"fmt"
	"time"
)

const r159LiveSignalFutureSkew = 2 * time.Second

// r159LiveAutoSignalAt restores the detector's numeric clock at every internal HTTP boundary.
// Manual orders have no detector lease and therefore return a zero timestamp without an error.
func r159LiveAutoSignalAt(auto bool, triggerUnixMS int64, now time.Time) (time.Time, string) {
	if !auto {
		return time.Time{}, ""
	}
	if triggerUnixMS <= 0 {
		return time.Time{}, "AUTO live numeric signal timestamp is missing or invalid"
	}
	signalAt := time.UnixMilli(triggerUnixMS)
	age := now.Sub(signalAt)
	if age < -r159LiveSignalFutureSkew {
		return time.Time{}, "AUTO live numeric signal timestamp is from the future"
	}
	if age > liveMirrorTTL {
		return time.Time{}, fmt.Sprintf(
			"AUTO live signal aged past the %s execution window before venue submission",
			liveMirrorTTL,
		)
	}
	return signalAt, ""
}

// r159LiveSubmitBoundaryReason is the last no-I/O fence before either venue mutation. It keeps
// caller cancellation and the original detector lease independent from all handler work.
func r159LiveSubmitBoundaryReason(ctx context.Context, auto bool, triggerUnixMS int64,
	now time.Time) string {
	if ctx == nil {
		return "LIVE request context is unavailable before venue submission"
	}
	if err := ctx.Err(); err != nil {
		return "LIVE request was canceled or timed out before venue submission"
	}
	_, why := r159LiveAutoSignalAt(auto, triggerUnixMS, now)
	return why
}
