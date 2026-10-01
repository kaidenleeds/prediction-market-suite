package server

import (
	"math"
	"strings"
)

// R110 shipped a single "restatement" identity that collapsed EVERY same-game same-team variant
// (full-game/F3/F5/F7 winners, spread lines, advance markets) into one blocked bet. R111 CORRECTS
// that (operator refutation, 2026-07-07 screenshot + review): a team can lead after 5 innings and
// lose the game; −1.5 is not the moneyline. Different payoff functions are genuinely different
// markets. The corrected taxonomy:
//
//   TRUE RESTATEMENT  — identical payoff function (same underlying, same threshold, same window,
//     same settlement rules). Live-verified sample 2026-07-07: KXETH-26JUL0817-T2509.99 and
//     KXETHD-26JUL0817-T2509.99 carry BYTE-IDENTICAL rules_primary (60s CF Benchmarks ERTI average
//     vs 2509.99 at 5pm EDT Jul 8) and the same close/expiry — one bet listed twice (the ~205
//     crypto dual-series twins from the R110 scan). Holding both = doubled identical exposure —
//     guards BLOCK these (dup-underlying).
//
//   CORRELATED-DISTINCT — same underlying, different payoff (innings splits, spread ladders,
//     strike ladders, corners bands, advance-vs-win). Guards ALLOW holding several, but they are
//     tracked as one correlated-exposure CLUSTER: per-cluster stake is summed across a book and
//     capped by config cluster_exposure_cap (generous default — sizing sees cluster exposure,
//     not a ban).

// trueRestatementKey returns the identical-payoff identity, or "" when none. Kalshi's known true
// twins are dual-series listings whose series differ only by a trailing 'D' (KXETH/KXETHD,
// KXSOL/KXSOLD, …) with the SAME mid (date/time window) and SAME outcome (strike). Canonicalizing
// the series by stripping one trailing 'D' makes both twins hash to one key; anything without a
// twin only ever matches its own ticker (which dup-ticker already catches upstream).
func trueRestatementKey(platform, ticker string) string {
	if platform != "kalshi" {
		return ""
	}
	parts := strings.Split(ticker, "-")
	if len(parts) < 3 {
		return ""
	}
	series := strings.ToUpper(parts[0])
	if len(series) > 4 && strings.HasSuffix(series, "D") {
		series = series[:len(series)-1]
	}
	return "ktr:" + series + ":" + strings.ToUpper(parts[1]) + ":" + strings.ToUpper(strings.Join(parts[2:], "-"))
}

// corrClusterKey returns the correlated-exposure CLUSTER identity for a market — "which real-world
// underlying is this bet about". Same cluster = correlated, NOT duplicate. R110's restatementKey
// logic survives here as the team-matchup cluster (matchup-mid + team letters: game/F5/spread/
// corners of one team cluster together); numeric-outcome ladders (totals, crypto/index strikes)
// cluster by EVENT (everything before the last '-': one ladder = one cluster, different windows =
// different clusters — the R107 xvident property). Non-kalshi: "" (inert).
func corrClusterKey(platform, ticker string) string {
	if platform != "kalshi" {
		return ""
	}
	parts := strings.Split(ticker, "-")
	if len(parts) < 3 {
		return ""
	}
	mid := parts[1]
	letters, digitRun := 0, 0
	for i := len(mid) - 1; i >= 0; i-- {
		c := mid[i]
		switch {
		case (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'):
			letters++
			digitRun = 0
		case c >= '0' && c <= '9':
			digitRun++
		default:
			digitRun = 3 // separator — stop
		}
		if digitRun >= 3 {
			break
		}
	}
	last := parts[len(parts)-1]
	team := ""
	for i := 0; i < len(last); i++ {
		c := last[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
			team += string(last[i])
			continue
		}
		break
	}
	if letters >= 4 && len(team) >= 2 {
		// Team-outcome market on a matchup: cluster = matchup + team (KXWCGAME/KXWCSPREAD/
		// KXWCTCORNERS-…-FRA6 all cluster as the France side of this game).
		return "kcl:" + strings.ToUpper(mid) + ":" + strings.ToUpper(team)
	}
	// Numeric-outcome ladder (totals, strikes, corners counts): cluster = the event ticker, so
	// each window/ladder is its own cluster and strikes within it share one.
	return "kev:" + strings.ToUpper(strings.Join(parts[:len(parts)-1], "-"))
}

// rfClusterStake sums the OPEN RawFlow stake (entry price × contracts) in a cluster and reports
// whether any open lot is a TRUE restatement (identical payoff) of tkey.
func (s *Server) rfClusterStake(ckey, tkey string) (stake float64, trueDup string) {
	if ckey == "" || (!s.rawFlowEnabled() && !s.rawFlowWindingDown()) {
		return 0, ""
	}
	s.rfBookMu.Lock()
	defer s.rfBookMu.Unlock()
	b := s.rfLoadLocked()
	for _, p := range b.Open {
		if corrClusterKey("kalshi", p.Ticker) == ckey {
			stake += p.Price * p.Contracts
		}
		if tkey != "" && trueRestatementKey("kalshi", p.Ticker) == tkey {
			trueDup = p.Side
		}
	}
	return stake, trueDup
}

// wxClusterStake — Weather book twin of rfClusterStake.
func (s *Server) wxClusterStake(ckey, tkey string) (stake float64, trueDup string) {
	if ckey == "" || (!s.weatherBookEnabled() && !s.weatherWindingDown()) {
		return 0, ""
	}
	s.wxBookMu.Lock()
	defer s.wxBookMu.Unlock()
	b := s.wxBookLoadLocked()
	for _, p := range b.Open {
		if corrClusterKey("kalshi", p.Ticker) == ckey {
			stake += p.Price * p.Contracts
		}
		if tkey != "" && trueRestatementKey("kalshi", p.Ticker) == tkey {
			trueDup = p.Side
		}
	}
	return stake, trueDup
}

// clusterExposureCap returns the per-cluster stake cap in dollars (config cluster_exposure_cap).
// R112 (operator order): cluster exposure is INFORMATION, not a brake — the metric stays visible
// everywhere (books, /api/rawflow, UI), but the cap DEFAULTS OFF (<=0 = unlimited). Set the
// config knob to a positive dollar value to re-arm the brake.
func (s *Server) clusterExposureCap() float64 {
	cap := s.cfg().Auto.ClusterExposureCap
	if cap <= 0 {
		return math.Inf(1)
	}
	return cap
}
