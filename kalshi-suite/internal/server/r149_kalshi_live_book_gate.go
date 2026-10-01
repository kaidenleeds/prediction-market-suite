package server

import (
	"fmt"
	"strings"
)

// kalshiLiveBooksClassify separates board-wide depth coverage from transport health.
//
// The priority orderbook socket is intentionally bounded: it cannot hold every near-horizon and
// research market when their combined universe exceeds the configured cap. That capacity shortfall
// must remain visible, but it is not proof that an allowed LIVE route is unsafe. The actual money
// path obtains the candidate's exact ACTIVE market and full REST orderbook immediately before
// submission (liveMirrorExecutable), then refuses missing/stale depth and sizes to visible depth.
// Readiness therefore requires a live, provable orderbook transport, while capacity is a warning.
func kalshiLiveBooksClassify(subscribed, fresh int, gaps int64, requiredShortfall int, lastErr string) (ok bool, detail, warning string) {
	lastErr = strings.TrimSpace(lastErr)
	detail = fmt.Sprintf("subscribed=%d; fresh=%d; gaps=%d; board-wide priority shortfall=%d",
		subscribed, fresh, gaps, requiredShortfall)
	if lastErr != "" {
		detail += "; " + lastErr
	}
	if subscribed <= 0 || fresh <= 0 || lastErr != "" {
		return false, detail, ""
	}
	if requiredShortfall > 0 {
		warning = fmt.Sprintf("Kalshi priority depth capacity is short by %d board-wide market(s); each LIVE order still fails closed unless its exact current full book and depth are available.", requiredShortfall)
	}
	return true, detail, warning
}
