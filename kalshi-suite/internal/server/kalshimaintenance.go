package server

import "time"

// kalshiMaintenanceWindow follows the current official maintenance page: Thursday 03:00-05:00
// America/New_York. Place/amend is unavailable during the trading pause. Five minutes of lead
// time keeps a newly submitted resting order out of the transition; cancel remains available.
func kalshiMaintenanceWindow(now time.Time) (bool, time.Time) {
	et, err := time.LoadLocation("America/New_York")
	if err != nil {
		return false, time.Time{}
	}
	local := now.In(et)
	if local.Weekday() != time.Thursday {
		return false, time.Time{}
	}
	start := time.Date(local.Year(), local.Month(), local.Day(), 2, 55, 0, 0, et)
	end := time.Date(local.Year(), local.Month(), local.Day(), 5, 0, 0, 0, et)
	return !local.Before(start) && local.Before(end), end
}
