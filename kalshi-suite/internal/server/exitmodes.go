package server

// R133 exit-mode research.
//
// The configured SL/TP selector deliberately stays limited to the already-shipped modes.  The
// policies below are research replays only: they must beat ride-to-settlement on an older selection
// half AND a newer holdout before they are candidates for a later paper/live promotion.  Keeping the
// grid out of the execution switch prevents an in-sample winner from silently becoming a real exit.

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	exitModeMinHoldHours  = 1.0 // same anti-infinity floor used by the unit-trial capital-day metric
	exitModeMaxTimedHours = 6.0 // candle backfill is capped at 6h; longer paths lack honest timestamps
)

type exitReplayPath struct {
	platform     string
	ticker       string
	category     string
	observed     time.Time
	entry        float64
	won          bool
	marks        []float64
	slip         float64
	resolveHours float64
}

type exitResearchPolicy struct {
	mode    string
	label   string
	r       float64
	arm     float64
	retrace float64
}

// exitResearchCell is one post-fill policy replay. Net/EV include every policy-triggered taker exit
// fee and a crossed half-spread. Entry fees are intentionally absent: the comparison begins after an
// identical filled entry, so that sunk amount is common to every policy (including ride).
type exitResearchCell struct {
	Mode                string  `json:"mode"`
	Label               string  `json:"label"`
	N                   int     `json:"n"`
	TimedN              int     `json:"timed_n"`
	Exits               int     `json:"exits"`
	Net                 float64 `json:"net"`
	EV                  float64 `json:"ev"`
	DeltaRideEV         float64 `json:"delta_ride_ev"`
	AvgHoldHours        float64 `json:"avg_hold_hours"`
	HoursFreed          float64 `json:"hours_freed"`
	ReturnPerCapitalDay float64 `json:"return_per_capital_day"`
	OlderEV             float64 `json:"older_ev"`
	NewerEV             float64 `json:"newer_ev"`
	NewerDeltaRideEV    float64 `json:"newer_delta_ride_ev"`
	HoldoutPass         bool    `json:"holdout_pass"`
	LiveEligible        bool    `json:"live_eligible"`
}

type exitResearchOut struct {
	N                   int                `json:"n"`
	TimedN              int                `json:"timed_n"`
	Grid                []exitResearchCell `json:"grid"`
	EVSelection         exitResearchCell   `json:"ev_selection"`
	RateSelection       exitResearchCell   `json:"rate_selection"`
	Verdict             string             `json:"verdict"`
	CapitalDayNote      string             `json:"capital_day_note"`
	ResearchOnly        bool               `json:"research_only"`
	HoldoutClusterRule  string             `json:"holdout_cluster_rule"`
	OlderClusters       int                `json:"older_clusters"`
	NewerClusters       int                `json:"newer_clusters"`
	HoldoutExcludedRows int                `json:"holdout_excluded_rows"`
}

type exitReplayResult struct {
	n           int
	timedN      int
	exits       int
	net         float64
	timedNet    float64
	capitalDays float64
	holdHours   float64
}

type exitFeeFunc func(exitReplayPath, float64) float64

func parseExitReplayPaths(rows []storage.SignalRow) []exitReplayPath {
	out := make([]exitReplayPath, 0, len(rows))
	for _, row := range rows {
		if row.Resolved != 1 || row.Won < 0 || row.FillPrice <= 0 || row.FillPrice >= 1 || row.PostPath == "" {
			continue
		}
		marks := make([]float64, 0, strings.Count(row.PostPath, ",")+1)
		for _, tok := range strings.Split(row.PostPath, ",") {
			v, err := strconv.ParseFloat(strings.TrimSpace(tok), 64)
			if err == nil && v >= 0 && v <= 1 && !math.IsNaN(v) && !math.IsInf(v, 0) {
				marks = append(marks, v)
			}
		}
		if len(marks) < 2 {
			continue
		}
		observed, observedErr := time.Parse(time.RFC3339Nano, strings.TrimSpace(row.TS))
		if observedErr != nil {
			// Keep the path in the descriptive all-sample replay, but leave the clock unknown so
			// the chronological holdout partition excludes it rather than inventing ordering.
			observed = time.Time{}
		}
		slip := 0.01
		if row.SpreadCents > 0 {
			slip = row.SpreadCents / 200
			if slip < 0.003 {
				slip = 0.003
			} else if slip > 0.03 {
				slip = 0.03
			}
		}
		out = append(out, exitReplayPath{
			platform: strings.ToLower(row.Platform), ticker: row.Ticker, category: row.Category,
			observed: observed, entry: row.FillPrice, won: row.Won == 1, marks: marks, slip: slip,
			resolveHours: row.ResolveHours,
		})
	}
	return out
}

func exitResearchPolicies() []exitResearchPolicy {
	policies := []exitResearchPolicy{{mode: "ride", label: "ride to settlement"}}
	// Separate the two arms of the old symmetric ratio. This is important because historical work
	// found global profit-taking costly while stop/salvage sometimes helped; one r should not force
	// both decisions to fire together.
	for ri := 5; ri <= 95; ri += 5 {
		r := float64(ri) / 100
		policies = append(policies,
			exitResearchPolicy{mode: "sl-ratio", label: fmt.Sprintf("stop only r=%.2f", r), r: r},
			exitResearchPolicy{mode: "tp-ratio", label: fmt.Sprintf("profit only r=%.2f", r), r: r},
		)
	}
	// Free-roll returns the original post-fill principal when the price reaches a scaled target and
	// lets the unsold shares settle. It can improve turnover without pretending the remainder sold.
	for ri := 10; ri <= 90; ri += 10 {
		r := float64(ri) / 100
		policies = append(policies, exitResearchPolicy{mode: "free-roll", label: fmt.Sprintf("recover principal r=%.2f", r), r: r})
	}
	// A break-even stop arms only after a real favorable move, then exits on a return through entry.
	for _, armC := range []int{3, 5, 8, 10, 15, 20} {
		policies = append(policies, exitResearchPolicy{mode: "break-even", label: fmt.Sprintf("arm +%dc; stop near entry", armC), arm: float64(armC) / 100})
	}
	// Trailing exits are already available as an off-by-default operational primitive. This grid
	// measures whether any arm/retrace pair deserves promotion instead of assuming that it does.
	for _, armC := range []int{5, 10, 15, 20} {
		for _, retraceC := range []int{2, 3, 5, 8, 10} {
			policies = append(policies, exitResearchPolicy{mode: "trail",
				label: fmt.Sprintf("arm +%dc; trail %dc", armC, retraceC),
				arm:   float64(armC) / 100, retrace: float64(retraceC) / 100})
		}
	}
	return policies
}

func exitObservedFill(path exitReplayPath, observed float64) float64 {
	fill := observed - path.slip // a monitored exit crosses to the bid; gaps fill at the observed mark, not the trigger
	if fill < 0 {
		return 0
	}
	if fill > 1 {
		return 1
	}
	return fill
}

// replayExitPolicy returns post-fill P/L and the share-weighted number of hours the original entry
// capital stayed locked. For a free-roll, the original principal is fully returned at the exit and
// the zero-basis remainder does not keep that same cash unavailable.
func replayExitPolicy(path exitReplayPath, policy exitResearchPolicy, fee exitFeeFunc) (net, lockedHours float64, exited bool) {
	settle := 0.0
	if path.won {
		settle = 1
	}
	finish := func(i int, fill float64) (float64, float64, bool) {
		exitFee := 0.0
		if fill > 0 && fill < 1 { // boundary-price exits have zero quadratic fee; never feed 0/1 to a live-fee fail-closed sentinel
			exitFee = fee(path, fill)
			if exitFee < 0 || exitFee > 1 || math.IsNaN(exitFee) || math.IsInf(exitFee, 0) {
				// Unsupported/stale route truth must not manufacture a giant negative or a fake
				// winner. Treat this row as ride for the candidate until its fee is knowable.
				return settle - path.entry, path.resolveHours, false
			}
		}
		h := path.resolveHours
		if h > 0 && len(path.marks) > 1 {
			h *= float64(i) / float64(len(path.marks)-1)
		}
		if h > 0 && h < exitModeMinHoldHours {
			h = exitModeMinHoldHours
		}
		return fill - path.entry - exitFee, h, true
	}

	peak, armed := path.entry, false
	for i, mark := range path.marks {
		if i == 0 { // one observation of reaction latency; never fill on the entry sample
			continue
		}
		if mark > peak {
			peak = mark
		}
		switch policy.mode {
		case "sl-ratio":
			if mark <= path.entry*(1-policy.r) {
				return finish(i, exitObservedFill(path, mark))
			}
		case "tp-ratio":
			if mark >= path.entry+policy.r*(1-path.entry) {
				return finish(i, exitObservedFill(path, mark))
			}
		case "free-roll":
			if mark >= path.entry+policy.r*(1-path.entry) {
				fill := exitObservedFill(path, mark)
				exitFee := 0.0
				if fill > 0 && fill < 1 {
					exitFee = fee(path, fill)
					if exitFee < 0 || exitFee > 1 || math.IsNaN(exitFee) || math.IsInf(exitFee, 0) {
						continue
					}
				}
				proceeds := fill - exitFee
				if proceeds > path.entry {
					fraction := path.entry / proceeds
					remaining := 1 - fraction
					h := path.resolveHours
					if h > 0 && len(path.marks) > 1 {
						h *= float64(i) / float64(len(path.marks)-1)
					}
					if h > 0 && h < exitModeMinHoldHours {
						h = exitModeMinHoldHours
					}
					return fraction*proceeds + remaining*settle - path.entry, h, true
				}
			}
		case "break-even":
			if !armed && mark >= path.entry+policy.arm {
				armed = true
			}
			if armed && mark <= path.entry {
				return finish(i, exitObservedFill(path, mark))
			}
		case "trail":
			if !armed && mark >= path.entry+policy.arm {
				armed = true
			}
			if armed && mark <= peak-policy.retrace {
				return finish(i, exitObservedFill(path, mark))
			}
		}
	}
	return settle - path.entry, path.resolveHours, false
}

func summarizeExitPolicy(paths []exitReplayPath, policy exitResearchPolicy, fee exitFeeFunc) exitReplayResult {
	var out exitReplayResult
	for _, path := range paths {
		net, held, exited := replayExitPolicy(path, policy, fee)
		out.n++
		out.net += net
		if exited {
			out.exits++
		}
		// Timestamp-free breadcrumbs cannot support an honest capital clock beyond the 6h candle
		// reconstruction horizon. Keep those trades in EV, but exclude them from the speed metric.
		if path.resolveHours > 0 && path.resolveHours <= exitModeMaxTimedHours {
			if held < exitModeMinHoldHours {
				held = exitModeMinHoldHours
			}
			out.timedN++
			out.timedNet += net
			out.holdHours += held
			out.capitalDays += path.entry * held / 24
		}
	}
	return out
}

func researchCell(policy exitResearchPolicy, all, older, newer, rideAll, rideOld, rideNew exitReplayResult,
	olderClusters, newerClusters int) exitResearchCell {
	cell := exitResearchCell{Mode: policy.mode, Label: policy.label, N: all.n, TimedN: all.timedN,
		Exits: all.exits, Net: all.net, LiveEligible: false}
	if all.n > 0 {
		cell.EV = all.net / float64(all.n)
		cell.DeltaRideEV = cell.EV - rideAll.net/float64(rideAll.n)
	}
	if all.timedN > 0 {
		cell.AvgHoldHours = all.holdHours / float64(all.timedN)
	}
	if all.capitalDays > 0 {
		cell.ReturnPerCapitalDay = all.timedNet / all.capitalDays
	}
	if older.n > 0 {
		cell.OlderEV = older.net / float64(older.n)
	}
	if newer.n > 0 {
		cell.NewerEV = newer.net / float64(newer.n)
		cell.NewerDeltaRideEV = cell.NewerEV - rideNew.net/float64(rideNew.n)
	}
	if rideAll.timedN > 0 && all.timedN > 0 {
		cell.HoursFreed = rideAll.holdHours/float64(rideAll.timedN) - cell.AvgHoldHours
	}
	// This is a holdout check, not a PROVEN/live verdict: its legacy venue+ticker groups are only
	// event proxies and the grid tries many policies. Promotion still requires the normal
	// multiple-test and prospective gates.
	olderDeltaRideEV := 0.0
	if older.n > 0 && rideOld.n > 0 {
		olderDeltaRideEV = cell.OlderEV - rideOld.net/float64(rideOld.n)
	}
	cell.HoldoutPass = policy.mode != "ride" &&
		older.n >= 50 && newer.n >= 50 && olderClusters >= 2 && newerClusters >= 2 &&
		olderDeltaRideEV >= 0.005 && cell.NewerDeltaRideEV >= 0.005
	return cell
}

type exitHoldoutPartition struct {
	older, newer                 []exitReplayPath
	olderClusters, newerClusters int
	excluded                     int
}

// partitionExitResearchHoldout keeps every venue+ticker proxy wholly on one side of the
// authoritative observation-time boundary, so repeated entries in one contract cannot teach the
// older selector and then reappear in its supposedly untouched newer test. Legacy path rows do not
// retain canonical event identity; unknown proxies or clocks are excluded from the holdout rather
// than treated as independent.
func partitionExitResearchHoldout(paths []exitReplayPath) exitHoldoutPartition {
	type group struct {
		key    string
		newest time.Time
		rows   []exitReplayPath
	}
	out := exitHoldoutPartition{}
	groups := make([]group, 0)
	index := make(map[string]int)
	for _, path := range paths {
		platform := strings.ToLower(strings.TrimSpace(path.platform))
		ticker := strings.ToUpper(strings.TrimSpace(path.ticker))
		if platform == "" || ticker == "" || path.observed.IsZero() {
			out.excluded++
			continue
		}
		key := platform + "\x00" + ticker
		i, ok := index[key]
		if !ok {
			i = len(groups)
			index[key] = i
			groups = append(groups, group{key: key})
		}
		groups[i].rows = append(groups[i].rows, path)
		if path.observed.After(groups[i].newest) {
			groups[i].newest = path.observed
		}
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].newest.Equal(groups[j].newest) {
			return groups[i].key < groups[j].key
		}
		return groups[i].newest.After(groups[j].newest)
	})
	if len(groups) < 2 {
		return out
	}
	// Split chronologically ordered group count, not row count, to preserve independence.
	newerN := len(groups) / 2
	for i, group := range groups {
		if i < newerN {
			out.newer = append(out.newer, group.rows...)
			out.newerClusters++
		} else {
			out.older = append(out.older, group.rows...)
			out.olderClusters++
		}
	}
	return out
}

func buildExitResearch(paths []exitReplayPath, fee exitFeeFunc) exitResearchOut {
	out := exitResearchOut{N: len(paths), ResearchOnly: true,
		CapitalDayNote:     "return_per_capital_day = timed net / (entry dollars x held days), with a 1h floor; only 0<h<=6h paths are timed because longer timestamp-free breadcrumbs exceed the candle backfill horizon",
		HoldoutClusterRule: "newest 50% of venue+ticker proxy groups by authoritative observation time; no proxy may appear in the older selection set; unknown clocks/identities are excluded; legacy rows lack canonical event IDs"}
	if len(paths) == 0 {
		out.Verdict = "no eligible post-fill paths"
		return out
	}
	partition := partitionExitResearchHoldout(paths)
	newer, older := partition.newer, partition.older
	out.OlderClusters, out.NewerClusters = partition.olderClusters, partition.newerClusters
	out.HoldoutExcludedRows = partition.excluded
	policies := exitResearchPolicies()
	ridePolicy := policies[0]
	rideAll := summarizeExitPolicy(paths, ridePolicy, fee)
	rideOld := summarizeExitPolicy(older, ridePolicy, fee)
	rideNew := summarizeExitPolicy(newer, ridePolicy, fee)
	out.TimedN = rideAll.timedN

	olderResults := make(map[string]exitReplayResult, len(policies))
	newerResults := make(map[string]exitReplayResult, len(policies))
	key := func(p exitResearchPolicy) string { return p.mode + "|" + p.label }
	for _, policy := range policies {
		all := summarizeExitPolicy(paths, policy, fee)
		old := summarizeExitPolicy(older, policy, fee)
		newResult := summarizeExitPolicy(newer, policy, fee)
		olderResults[key(policy)] = old
		newerResults[key(policy)] = newResult
		out.Grid = append(out.Grid, researchCell(policy, all, old, newResult, rideAll, rideOld, rideNew,
			partition.olderClusters, partition.newerClusters))
	}

	// Choose on OLDER data; report the untouched NEWER result. This keeps the displayed winner from
	// being the same in-sample maximization error that the existing auto tuner guards against.
	evIdx := 0
	for i := 1; i < len(out.Grid); i++ {
		if out.Grid[i].OlderEV > out.Grid[evIdx].OlderEV {
			evIdx = i
		}
	}
	out.EVSelection = out.Grid[evIdx]

	// Profit-rate selection is constrained to policies that first improve older-settlement EV by at
	// least 0.5c/contract. Faster turnover may break an EV tie; it may not launder a worse exit into
	// a winner merely by annualizing it.
	rateIdx := 0
	rideOldEV := 0.0
	if rideOld.n > 0 {
		rideOldEV = rideOld.net / float64(rideOld.n)
	}
	for i := 1; i < len(out.Grid); i++ {
		old := olderResults[out.Grid[i].Mode+"|"+out.Grid[i].Label]
		oldRate := 0.0
		if old.capitalDays > 0 {
			oldRate = old.timedNet / old.capitalDays
		}
		bestOld := olderResults[out.Grid[rateIdx].Mode+"|"+out.Grid[rateIdx].Label]
		bestRate := 0.0
		if bestOld.capitalDays > 0 {
			bestRate = bestOld.timedNet / bestOld.capitalDays
		}
		if out.Grid[i].OlderEV >= rideOldEV+0.005 && oldRate > bestRate {
			rateIdx = i
		}
	}
	out.RateSelection = out.Grid[rateIdx]
	if out.RateSelection.Mode != "ride" {
		chosenNew := newerResults[out.RateSelection.Mode+"|"+out.RateSelection.Label]
		newerRate := 0.0
		if chosenNew.capitalDays > 0 {
			newerRate = chosenNew.timedNet / chosenNew.capitalDays
		}
		rideRate := 0.0
		if rideNew.capitalDays > 0 {
			rideRate = rideNew.timedNet / rideNew.capitalDays
		}
		out.RateSelection.HoldoutPass = out.RateSelection.HoldoutPass && newerRate > rideRate
	}

	if out.EVSelection.Mode == "ride" {
		out.Verdict = "ride remains the older-half EV winner; no added mode is enabled"
	} else if out.EVSelection.HoldoutPass {
		out.Verdict = out.EVSelection.Label + " also beat ride on the newer holdout; keep research-only until event-clustered prospective proof"
	} else {
		out.Verdict = out.EVSelection.Label + " won only the older selection half; it failed the newer holdout and stays research-only"
	}

	// Stable, useful ordering for API/UI inspection: ride first, then highest all-sample EV. The two
	// selected cells above are copied before sorting and retain their full diagnostics.
	rest := out.Grid[1:]
	sort.SliceStable(rest, func(i, j int) bool { return rest[i].EV > rest[j].EV })
	return out
}

func (s *Server) exitResearchResult(rows []storage.SignalRow) exitResearchOut {
	paths := parseExitReplayPaths(rows)
	return buildExitResearch(paths, func(path exitReplayPath, price float64) float64 {
		return s.blendedFeeAction(path.platform, path.ticker, path.category, false, 1, price, false)
	})
}
