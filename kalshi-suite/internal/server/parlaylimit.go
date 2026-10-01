package server

// parlayLegLimit is the one hard ceiling shared by paper suggestions, automatic placement,
// Parlay Lab, and LIVE Kalshi RFQs. A lower configured ceiling remains valid; zero means use this
// safe project-wide maximum. No request or stale config value may raise it.
const parlayLegLimit = 6

func boundedParlayMaxLegs(configured int) int {
	if configured <= 0 || configured > parlayLegLimit {
		return parlayLegLimit
	}
	if configured < 2 {
		return 2
	}
	return configured
}

func validParlayLegCount(n int) bool {
	return n >= 2 && n <= parlayLegLimit
}
