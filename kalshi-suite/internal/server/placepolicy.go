// placepolicy.go — direct emitted-side Paper placement policy.
//
// Every tradeable signal family starts ON; poly-int stays research-only. At n>=30, a direct
// maker-net lower bound at/below zero retires placement while logging continues. The historical
// synthetic inverse calculation remains visible only as a discovery hypothesis. It cannot flip,
// fund, promote, or authorize Paper/LIVE: an opposite system needs its own name, current trigger,
// actual opposite ask/depth/fee, settlement history and sealed route proof.
//
// CADENCE: rebuilt every 30 minutes by sweepEquityAlloc, right beside the R78 EV-share allocator.
// Status flips are audited (kind "policy"); the live table renders in Research → PERFORMANCE next
// to the Capital-allocation table (/api/alloc "policy" block) and rides the export with it.
package server

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// policyMinN is the evidence bar: below it a family is ON by default (exploration — retiring on
// a small sample is exactly the noise-flip failure the CI machinery exists to prevent).
const policyMinN = 30

// policyDeadbandPC — R90 bug 133 (auditor DO-THIS 6): hysteresis on the LB thresholds. kflow
// retired at invLB −0.01¢ — a knife-edge where per-build sampling noise could re-flip status
// every 30 minutes (policy flap; the retire itself held ≥4 rebuilds, but only by luck of the
// draw). A status may only CHANGE when the lower bounds that DEFINE the new status clear zero
// by at least this margin (±0.05¢/ct); inside the band the previous status holds (dwell).
const policyDeadbandPC = 0.0005

// policyWindowDays — R84 POLICY v1.1 (auditor bug 31): grade only signal rows from the last N days.
// The v1 build graded a family's ENTIRE resolved history, so month-old rows from a since-fixed
// producer (or a since-shifted market regime) could hold a family retired/inverted forever — the
// un-retire promise ("earns its way back the moment the evidence turns") was practically dead
// weight under an ever-growing denominator. 30 days ≈ the same recency the R82 leaderboard window
// settled on; families whose WINDOWED n falls below policyMinN revert to default-ON (exploration).
const policyWindowDays = 30

// policyRow is one family's line in the placement-policy table (Research → PERFORMANCE).
type policyRow struct {
	Family string  `json:"family"`
	N      int     `json:"n"`         // deduped resolved signal rows behind the bounds
	DirEV  float64 `json:"dir_ev_pc"` // mean maker-net EV/ct, direct side (display)
	DirLB  float64 `json:"dir_lb_pc"` // one-sided 95% LOWER bound, maker-net EV/ct, direct side
	InvLB  float64 `json:"inv_lb_pc"` // one-sided 95% LOWER bound, maker-net EV/ct, INVERTED side
	Status string  `json:"status"`    // on | retired | research; inverse LB is diagnostic only
}

// policyFamilies — every SIGNAMES family the policy grades, in registry order. KEEP IN SYNC with
// dashboard.go's SIGNAMES and SIGNALMAP.md (same keys). research=true marks the poly-int-only
// families (the lab): data-only, never placed, exactly as before — listed for completeness, never
// gated or armed. SIGNAMES' "auto-ml" (graded by the ML books, not signal_log) and "manual"
// (a human) are intentionally absent: the policy governs signal-family placement only.
var policyFamilies = []struct {
	fam      string
	research bool
}{
	{"kalshi-whale", false}, {"kalshi-flow", false}, {"kflow", false},
	{"polyus-whale", false}, {"polyus-flow", false}, {"polyus-consensus", false},
	{"poly-whale", true}, {"poly-consensus", true}, {"pflow", true}, {"pcrypto", true},
	{"kcrypto", false}, {"kthresh", false}, {"xmatch", false}, {"pmatch", false},
	{"pbridge", false}, {"confluence", false}, {"cross", false}, {"favlong", false},
	{"basket", true}, {"fade", true}, {"divergence", true}, {"insider", true},
	{"skillbuy", true}, {"whale-exit", true}, {"whale-exit-hold-bridge", false},
	{"meanrev", false}, {"xvlag", false}, {"xvgap", false}, {"spotlag", false}, {"sharpline", false},
	{"arb", false}, {"pfbridge", false}, {"fbridge", false}, {"freshlist", false},
	{"freshfade", true},    // R106 (edge-38): fade-the-fresh-listing twin — log-only by design (research flag)
	{"xinv-pcrypto", true}, // R117: inverted crypto bridge twin (pcrypto's wrongness on the Kalshi twin) — rows-only like freshfade, no executor source maps to it
	{"bookskew", false}, {"fundtilt", false}, {"wxedge", false}, {"poly-pred-kalshi", false},
	// Current R126/R127 producers. These were omitted from both registries even though their
	// prospective rows were actively collecting, which made liveness call them historical.
	{"xvgap2", false}, {"xvgapk", false}, {"notail", false},
	// R135c: durable signal_log rows exist only so the ordinary settlement worker can grade these
	// two prospective cohorts. They bypass Server.insertSignal and are permanently research-only.
	{"weather-curve-residual", true}, {"weather-curve-revision", true},
	{"independent-probabilistic-weather", true},
}

// policySourceFamily maps an autoPlace source string onto the signal family whose evidence
// governs it. "" = not a signal family (auto-ml, gate, manual, live paths) — never policy-gated.
func policySourceFamily(source string) string {
	switch source {
	case "auto-arb":
		return "arb"
	case "auto-cons-x":
		return "cross"
	case "auto-cons-kalshi":
		return "kalshi-flow" // the blended source's live feed IS the flow rows (whale prints never place)
	case "auto-cons-poly":
		return "poly-consensus"
	case "auto-cons-pusflow":
		return "polyus-flow"
	}
	if f, ok := strings.CutPrefix(source, "auto-cons-"); ok {
		return f // kcrypto, pcrypto, xmatch, kthresh, pmatch, pbridge, favlong, confluence, xvlag, kflow, pflow, …
	}
	return ""
}

// policyStatus returns the family's current placement-policy status. "" when the source maps to
// no family or the table hasn't built yet — both FAIL-OPEN to the default-ON posture (the
// operator's rule: everything places until the evidence says otherwise).
func (s *Server) policyStatus(source string) string {
	fam := policySourceFamily(source)
	if fam == "" {
		return ""
	}
	s.polMu.Lock()
	defer s.polMu.Unlock()
	return s.polMap[fam]
}

// policyRetired is direct emitted-side only. Logging and opposite-control research continue.
func (s *Server) policyRetired(source string) bool { return s.policyStatus(source) == "retired" }

// paperPolicyRetired is the money-facing Phase-0 boundary. policyStatus is computed from
// signal_log replay prices, not authenticated exchange fills, so it remains research telemetry and
// cannot suppress corrected Paper. A future implementation must carry route-exact profit evidence
// rather than pairing this pooled status with an unrelated proof row.
func (s *Server) paperPolicyRetired(source string) bool {
	_ = source
	return false
}

// policyInverted is retained as an API-compatible hard false. Synthetic inverse evidence is
// diagnostic only and never mutates an order.
func (s *Server) policyInverted(source string) bool {
	_ = source
	return false // synthetic inverse evidence can never mutate a Paper/LIVE order
}

// rebuildPlacementPolicy recomputes every family's status from deduped resolved signal rows.
// Called by sweepEquityAlloc on the allocator's 30-minute cadence (and retried each 30s sweep
// until the first successful build — polBuiltAt is only stamped on success).
func (s *Server) rebuildPlacementPolicy(ctx context.Context) {
	cutoff := time.Now().AddDate(0, 0, -policyWindowDays)
	rows, err := s.store.ListResolvedPolicySignalsSince(ctx, cutoff)
	if err != nil {
		return // store hiccup / cold first pull timed out — keep the previous table (fail-open)
	}
	type acc struct {
		n           int
		dSum, dSum2 float64
		iSum, iSum2 float64
	}
	byFam := map[string]*acc{}
	// R84 POLICY v1.1 (auditor bug 31): (a) SINCE window — only rows newer than policyWindowDays
	// grade (ts is RFC3339, so the string compare is chronological); (b) TRADEABLE-VENUE filter —
	// placement decisions are graded on the venues placement actually happens on (kalshi/polyus);
	// poly-int rows are research telemetry and were skewing families that log on both.
	for _, sr := range rows {
		if up := strings.ToUpper(strings.TrimSpace(sr.Side)); up != "YES" && up != "NO" {
			// R90 DO-THIS 1 interim guard (the auditor's policy WHERE, expressed on this Go-side
			// row source): non-canonical side labels (bugs 73/74/75 poison — fabricated ELSE-0
			// losses) never grade placement. The v7 quarantine removed the historical rows and
			// the insert chokepoint blocks new ones; this is the belt-and-suspenders layer so a
			// regression can never flip a family's placement verdict again.
			continue
		}
		price := sr.EntryPrice
		if sr.FillPrice > 0 && sr.FillPrice < 1 {
			price = sr.FillPrice
		}
		if price <= 0 || price >= 1 {
			continue
		}
		// DIRECT: the same maker-net EV/ct per row the Backtest ev_net column grades.
		ev := -price
		if sr.Won == 1 {
			ev = 1 - price
		}
		ev -= s.blendedFee(sr.Platform, sr.Ticker, sr.Category, true, 1, price)
		// INVERTED: buy the complementary side at 1−price; wins exactly when the original lost;
		// maker fee at the inverted entry (the R73 INV convention — the honest mirror).
		pInv := 1 - price
		evInv := -pInv
		if sr.Won == 0 {
			evInv = 1 - pInv
		}
		evInv -= s.blendedFee(sr.Platform, sr.Ticker, sr.Category, true, 1, pInv)
		a := byFam[sr.SignalType]
		if a == nil {
			a = &acc{}
			byFam[sr.SignalType] = a
		}
		a.n++
		a.dSum += ev
		a.dSum2 += ev * ev
		a.iSum += evInv
		a.iSum2 += evInv * evInv
	}
	// R90 bug 133: the previous statuses are the hysteresis anchor — copy them out before the
	// rebuild so the deadband below can compare candidate vs prior state per family.
	s.polMu.Lock()
	prevStatus := make(map[string]string, len(s.polMap))
	for k, v := range s.polMap {
		if v == "inverted" { // normalize any in-memory pre-R139 state before hysteresis can retain it
			v = "retired"
		}
		prevStatus[k] = v
	}
	s.polMu.Unlock()
	newRows := make([]policyRow, 0, len(policyFamilies))
	newMap := make(map[string]string, len(policyFamilies))
	for _, pf := range policyFamilies {
		row := policyRow{Family: pf.fam, Status: "on"}
		if a := byFam[pf.fam]; a != nil && a.n > 0 {
			row.N = a.n
			row.DirEV = round4(a.dSum / float64(a.n))
			row.DirLB = round4(evLowerBound(a.dSum, a.dSum2, a.n))
			row.InvLB = round4(evLowerBound(a.iSum, a.iSum2, a.n))
		}
		switch {
		case pf.research:
			row.Status = "research"
		case row.N >= policyMinN && row.DirLB <= 0:
			row.Status = "retired"
		}
		// R90 bug 133 DEADBAND (auditor DO-THIS 6): a flip away from the previous status only
		// lands when the bounds defining the NEW status clear zero by ≥ policyDeadbandPC
		// (0.05¢/ct); sub-threshold evidence keeps the previous status. The n<policyMinN →
		// default-ON revert stays undamped BY DESIGN (the documented exploration posture — see
		// policyWindowDays); first build (no previous map) takes the naive status unchanged.
		if prev, ok := prevStatus[pf.fam]; ok && prev != "" && !pf.research && row.Status != prev && row.N >= policyMinN {
			switch row.Status {
			case "retired":
				if !(row.DirLB <= -policyDeadbandPC) {
					row.Status = prev
				}
			case "on":
				if !(row.DirLB >= policyDeadbandPC) {
					row.Status = prev
				}
			}
		}
		newRows = append(newRows, row)
		newMap[pf.fam] = row.Status
	}
	sig := policySignature(newRows)
	s.polMu.Lock()
	oldMap, oldSig := s.polMap, s.polSig
	first := oldMap == nil
	s.polRows, s.polMap, s.polSig, s.polBuiltAt = newRows, newMap, sig, time.Now()
	s.polMu.Unlock()
	// R83: the FIRST build always audits. The old `sig == oldSig` early-return swallowed the first
	// build whenever no family was retired/inverted (sig "" == oldSig "") — so a healthy sweep
	// left ZERO "policy" rows in the trail and looked dead during the 2026-07-05 incident triage.
	// One stamp per boot is cheap; sweep liveness must be provable from the audit trail alone.
	if !first && sig == oldSig {
		return
	}
	// Flips are AUDITED (operator spec): name each family that changed state, with the evidence.
	changes := []string{}
	for _, r := range newRows {
		if old, ok := oldMap[r.Family]; ok && old != r.Status {
			changes = append(changes, fmt.Sprintf("%s %s→%s (n=%d dirLB %+.2f¢ invLB %+.2f¢)",
				r.Family, old, r.Status, r.N, r.DirLB*100, r.InvLB*100))
		}
	}
	msg := "R79 placement policy updated: " + strings.Join(changes, " · ")
	if first {
		msg = "R79 placement policy built: " + sig
		if sig == "" {
			msg = "R79 placement policy built: every family ON (no family meets the n≥30 both-fail bar)"
		}
	}
	if len(msg) > 900 {
		msg = msg[:900] + "…"
	}
	// R84 v1.1(c) (auditor bug 31): log the DEDUPED windowed n per family — the evidence base behind
	// every verdict, so "retired at n=31" vs "retired at n=4,000" is visible without a DB query.
	nParts := []string{}
	for _, r := range newRows {
		if r.N > 0 {
			// R90 DO-THIS 14: per-family LBs ride the detail rows — "retired at dirLB −10.7¢"
			// vs "retired at −0.01¢ (knife-edge)" is now visible without a DB query.
			nParts = append(nParts, fmt.Sprintf("%s=%d(d%+.2f/i%+.2f¢)", r.Family, r.N, r.DirLB*100, r.InvLB*100))
		}
	}
	nStr := strings.Join(nParts, " ")
	if len(nStr) > 1200 { // widened cap (R90): the LB suffixes roughly double the per-family width
		nStr = nStr[:1200] + "…"
	}
	s.log.Info("R79 placement policy", "first_build", first, "non_default", sig, "window_days", policyWindowDays, "deduped_n", nStr)
	detail := sig
	if nStr != "" {
		detail = strings.TrimSpace(sig + " | window " + fmt.Sprintf("%dd", policyWindowDays) + " deduped-n: " + nStr)
	}
	_ = s.store.Audit(ctx, "info", "policy", msg, detail)
}

// policySignature is the stable non-default-state fingerprint ("fam:status;…") the flip audit
// keys on — "on"/"research" rows are the resting defaults and stay out of it.
func policySignature(rows []policyRow) string {
	parts := []string{}
	for _, r := range rows {
		if r.Status == "retired" || r.Status == "inverted" {
			parts = append(parts, r.Family+":"+r.Status)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

// policyView returns a copy of the current table + build stamp for /api/alloc (PERFORMANCE tab).
func (s *Server) policyView() ([]policyRow, time.Time) {
	s.polMu.Lock()
	defer s.polMu.Unlock()
	rows := make([]policyRow, len(s.polRows))
	copy(rows, s.polRows)
	return rows, s.polBuiltAt
}

func round4(v float64) float64 { return math.Round(v*10000) / 10000 }
