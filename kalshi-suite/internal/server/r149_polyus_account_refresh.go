package server

import (
	"strings"
)

// polyUSAccountRefreshClass keeps safety-critical account exposure truth separate from optional
// activity history. Open orders + positions can authorize a fresh zero/exposure receipt; activity
// history is used for display and realized-P&L evidence, but its independent 429 cannot erase that
// account receipt.
type polyUSAccountRefreshClass struct {
	ExposureErr         string
	HistoryErr          string
	ExposureRateLimited bool
	HistoryRateLimited  bool
}

func classifyPolyUSAccountRefresh(orderErr, positionErr, historyErr error) polyUSAccountRefreshClass {
	var out polyUSAccountRefreshClass
	exposureErr := orderErr
	if exposureErr == nil {
		exposureErr = positionErr
	}
	if exposureErr != nil {
		out.ExposureErr = exposureErr.Error()
		out.ExposureRateLimited = strings.Contains(out.ExposureErr, "429")
	}
	if historyErr != nil {
		out.HistoryErr = historyErr.Error()
		out.HistoryRateLimited = strings.Contains(out.HistoryErr, "429")
	}
	return out
}
