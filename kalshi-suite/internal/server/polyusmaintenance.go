package server

import "time"

// polyUSMaintenanceWindow reflects Polymarket US changelog v0.0.68 (effective 2026-07-09):
// Thursday 02:00-06:00 America/New_York. Five minutes of pre-window safety avoids submitting a
// resting order into the venue's cancel-all transition.
func polyUSMaintenanceWindow(now time.Time) (bool, time.Time) {
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		return false, time.Time{}
	}
	local := now.In(et)
	if local.Weekday() != time.Thursday {
		return false, time.Time{}
	}
	start := time.Date(local.Year(), local.Month(), local.Day(), 1, 55, 0, 0, et)
	end := time.Date(local.Year(), local.Month(), local.Day(), 6, 0, 0, 0, et)
	return !local.Before(start) && local.Before(end), end
}
