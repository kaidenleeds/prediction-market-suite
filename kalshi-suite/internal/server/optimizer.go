// optimizer.go — R7: auto-optimizes the Backtest (replay) tunables.
//
// The replay tab exposes the knobs the live bot actually has (per-bet cap, max exposure, max hold,
// execution style, Kelly fraction). Hand-tuning them one text box at a time is guesswork; this
// searches against the REAL replay engine — same walk-forward sim, same fees, same measured fill
// model — and scores each candidate by a drawdown-penalized log-growth:
// score = ln(growth) − 2.5·maxDD (R34: penalty raised 1.5→2.5, operator "minimize drawdown more").
//
// R35 (operator: "find the absolute best point for continuous grids, all dimensions"): the search
// is now three stages —
//  1. COARSE GRID on the TRAIN window (chronological oldest 70%) — locates the right region.
//  2. COORDINATE-DESCENT REFINEMENT — walks every continuous dimension (per-bet cap, max
//     exposure, max hold, confidence floor, KELLY FRACTION) in shrinking steps from the coarse
//     winner (plus a maker/taker flip test) until the local plateau center is found. There is no
//     closed-form optimum for this objective: it's a NOISY STEP FUNCTION (bets enter/leave
//     discretely as thresholds move), so derivative-free descent is the honest tool.
//  3. HOLDOUT VALIDATION on the TEST window (newest 30%) — the coarse top-3 and the refined point
//     compete on data none of them was fit to; the TEST winner is what gets APPLIED. This is the
//     guard against the classic failure of "absolute best point" searches: the exact in-sample
//     peak is usually the most overfit point. Plateaus beat peaks.
//
// Honesty notes: even with a holdout, one history is one regime — treat the winner as a sane
// default, watch its live divergence, re-run after regimes change. The response carries the top-5
// (flat = robust, spiky = fragile) and the holdout table so you can see generalization directly.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// legacyReplayMayApply is intentionally a compile-time safety rail, not a config knob. The old
// signal-price replay remains visible for research, but cannot tune execution settings until its
// chronology and prospective route-pricing invariants are proven by tests.
const legacyReplayMayApply = false

// handleReplayOptimize (GET /api/replay/optimize) — cached 10 min; a full search runs ~40–150s in
// the background via the serve-stale/202 pattern, so the tab shows "building…" then results.
func (s *Server) handleReplayOptimize(w http.ResponseWriter, r *http.Request) {
	s.serveSnap(w, r, &s.snapReplayOpt, 10*time.Minute, "replayopt", s.buildReplayOptimize)
}

// optScore is the shared objective: drawdown-penalized log-growth.
func optScore(g, dd float64) float64 {
	if g < 0.01 {
		g = 0.01 // ruin floors the log, doesn't -Inf it
	}
	return math.Log(g) - 2.5*dd
}

// optPoint is one fully-specified tunables configuration (all dimensions, continuous).
type optPoint struct {
	pb, me, mh, mc, kf float64
	fokm               bool
}

func (p optPoint) key() string {
	return fmt.Sprintf("%.4f|%.3f|%.3f|%.3f|%.3f|%v", p.pb, p.me, p.mh, p.mc, p.kf, p.fokm)
}

type optResult struct {
	Pbcap     float64 `json:"pbcap"`
	Maxexp    float64 `json:"maxexp"`
	MaxholdH  float64 `json:"maxhold_h"`
	Fokm      bool    `json:"fokm"`
	MinConf   float64 `json:"min_conf"` // R24: winning confidence floor (0 = no floor)
	KellyFrac float64 `json:"kelly_frac"`
	KellyLbl  string  `json:"kelly_label"`
	Score     float64 `json:"score"`
	Growth    float64 `json:"growth"`
	MaxDD     float64 `json:"max_dd"`
	Bets      int     `json:"bets"`
}

func (s *Server) buildReplayOptimize(pctx context.Context) []byte {
	ctx, cancel := context.WithTimeout(pctx, 170*time.Second)
	defer cancel()
	rows, err := s.store.ListSignalsLean(ctx, 0)
	if err != nil {
		return nil
	}
	rows = dedupSignals(rows)
	fillPriced := 0 // R34: rows whose entry is the REAL fill price (buildReplayOnRows swaps them in)
	for i := range rows {
		if fp := rows[i].FillPrice; fp > 0 && fp < 1 {
			fillPriced++
		}
	}
	// R35: chronological 70/30 split — fit on the past, validate on the (relative) future.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].TS < rows[j].TS })
	trainRows, testRows := rows, []storage.SignalRow(nil)
	holdoutOn := len(rows) >= 2000 // tiny histories: refuse to shave 30% off the fit set
	if holdoutOn {
		cut := int(0.7 * float64(len(rows)))
		trainRows, testRows = rows[:cut], rows[cut:]
	}

	kellyLadder := []float64{0, 0.25, 0.5, 1.0} // buildReplayOnRows sweep order: flat, ¼, ½, full

	// evalPoint runs ONE replay sim for a point on the given rows and extracts the row matching the
	// point's Kelly fraction (a ladder rung, or the custom ?kelly= row which lands LAST post-R34-dedupe).
	evalPoint := func(p optPoint, rws []storage.SignalRow) (optResult, bool) {
		q := url.Values{}
		q.Set("pbcap", fmt.Sprintf("%g", p.pb))
		q.Set("maxexp", fmt.Sprintf("%g", p.me))
		q.Set("maxhold", fmt.Sprintf("%g", p.mh))
		if p.fokm {
			q.Set("exec", "fokm")
		}
		if p.mc > 0 {
			q.Set("minconf", fmt.Sprintf("%g", p.mc))
		}
		ladderIdx := -1
		for i, f := range kellyLadder {
			if math.Abs(p.kf-f) < 1e-9 {
				ladderIdx = i
				break
			}
		}
		if ladderIdx < 0 {
			q.Set("kelly", fmt.Sprintf("%g", p.kf))
		}
		b := s.buildReplayOnRows(ctx, q, rws)
		if b == nil {
			return optResult{}, false
		}
		var rep struct {
			Sweep []struct {
				Label  string  `json:"label"`
				Growth float64 `json:"growth"`
				MaxDD  float64 `json:"max_dd"`
				Bets   int     `json:"bets"`
			} `json:"kelly_sweep"`
		}
		if json.Unmarshal(b, &rep) != nil || len(rep.Sweep) == 0 {
			return optResult{}, false
		}
		idx := ladderIdx
		if idx < 0 {
			idx = len(rep.Sweep) - 1 // custom fraction row (appended after the ladder)
		}
		if idx >= len(rep.Sweep) {
			return optResult{}, false
		}
		sw := rep.Sweep[idx]
		return optResult{
			Pbcap: p.pb, Maxexp: p.me, MaxholdH: p.mh, Fokm: p.fokm, MinConf: p.mc,
			KellyFrac: p.kf, KellyLbl: sw.Label,
			Score: optScore(sw.Growth, sw.MaxDD), Growth: sw.Growth, MaxDD: sw.MaxDD, Bets: sw.Bets,
		}, true
	}

	// ── Stage 1: COARSE GRID on TRAIN ──────────────────────────────────────────────────────────
	type cand struct {
		pbcap, maxexp, maxhold float64
		fokm                   bool
		minconf                float64
	}
	var cands []cand
	for _, pb := range []float64{0.01, 0.025, 0.05, 0.1} {
		for _, me := range []float64{0.3, 0.6, 0.94} {
			for _, mh := range []float64{0.12, 0.5, 2, 6, 12} { // R34: finer hold ladder — learn the hours, don't guess
				for _, fk := range []bool{false, true} {
					for _, mc := range []float64{0, 0.3, 0.5} {
						cands = append(cands, cand{pb, me, mh, fk, mc})
					}
				}
			}
		}
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	rnd.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })

	var results, seqResults []optResult
	tested := 0
	started := time.Now()
	for _, c := range cands {
		if ctx.Err() != nil || time.Since(started) > 90*time.Second {
			break // time budget — the grid is shuffled, so partial coverage is unbiased
		}
		q := url.Values{}
		q.Set("pbcap", fmt.Sprintf("%g", c.pbcap))
		q.Set("maxexp", fmt.Sprintf("%g", c.maxexp))
		q.Set("maxhold", fmt.Sprintf("%g", c.maxhold))
		if c.fokm {
			q.Set("exec", "fokm")
		}
		if c.minconf > 0 {
			q.Set("minconf", fmt.Sprintf("%g", c.minconf))
		}
		b := s.buildReplayOnRows(ctx, q, trainRows)
		if b == nil {
			continue
		}
		var rep struct {
			Sweep []struct {
				Label  string  `json:"label"`
				Growth float64 `json:"growth"`
				MaxDD  float64 `json:"max_dd"`
				Bets   int     `json:"bets"`
			} `json:"kelly_sweep"`
			Seq []struct {
				Label  string  `json:"label"`
				Growth float64 `json:"growth"`
				MaxDD  float64 `json:"max_dd"`
				Bets   int     `json:"bets"`
			} `json:"seq_compound"`
		}
		if json.Unmarshal(b, &rep) != nil {
			continue
		}
		tested++
		for i, sw := range rep.Sweep {
			if i >= len(kellyLadder) {
				break
			}
			results = append(results, optResult{
				Pbcap: c.pbcap, Maxexp: c.maxexp, MaxholdH: c.maxhold, Fokm: c.fokm, MinConf: c.minconf,
				KellyFrac: kellyLadder[i], KellyLbl: sw.Label,
				Score: optScore(sw.Growth, sw.MaxDD), Growth: sw.Growth, MaxDD: sw.MaxDD, Bets: sw.Bets,
			})
		}
		for i, sq := range rep.Seq { // R12: sequential scored with the same objective (report-only)
			if i >= len(kellyLadder) {
				break
			}
			seqResults = append(seqResults, optResult{
				Pbcap: c.pbcap, Maxexp: c.maxexp, MaxholdH: c.maxhold, Fokm: c.fokm, MinConf: c.minconf,
				KellyFrac: kellyLadder[i], KellyLbl: sq.Label,
				Score: optScore(sq.Growth, sq.MaxDD), Growth: sq.Growth, MaxDD: sq.MaxDD, Bets: sq.Bets,
			})
		}
	}
	if len(results) == 0 {
		b, _ := json.Marshal(map[string]any{"tested": 0, "error": "no candidate produced a result (not enough resolved signals?)"})
		return b
	}
	// R36 ACTIVITY FLOOR (operator's "interesting" screenshot): on honest fill-priced data every
	// real config scored NEGATIVE, so the descent found the degenerate optimum — a cap so small it
	// places ZERO bets (growth 1.00×, DD 0%, score 0.00 beats everything). "Don't trade" is
	// information, not a strategy: a config must place a minimum number of bets to be eligible to
	// win, refine, or be applied.
	const minTrainBets, minTestBets = 30, 8
	valid := func(r optResult) bool { return r.Bets >= minTrainBets }
	betterRes := func(a, b optResult) bool { // validity first; then R34: near-ties prefer the LONGER max-hold
		if valid(a) != valid(b) {
			return valid(a)
		}
		if math.Abs(a.Score-b.Score) >= 0.02 {
			return a.Score > b.Score
		}
		return a.MaxholdH > b.MaxholdH
	}
	sortRes := func(rs []optResult) []optResult {
		for i := 1; i < len(rs); i++ {
			for j := i; j > 0 && betterRes(rs[j], rs[j-1]); j-- {
				rs[j], rs[j-1] = rs[j-1], rs[j]
			}
		}
		if len(rs) > 5 {
			return rs[:5]
		}
		return rs
	}
	top := sortRes(results)
	seqTop := sortRes(seqResults)

	// ── Stage 2: COORDINATE-DESCENT REFINEMENT (continuous, all dimensions) ───────────────────
	toPoint := func(r optResult) optPoint {
		return optPoint{pb: r.Pbcap, me: r.Maxexp, mh: r.MaxholdH, mc: r.MinConf, kf: r.KellyFrac, fokm: r.Fokm}
	}
	cur := toPoint(top[0])
	curRes := top[0]
	refineSteps := 0
	clamp := func(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }
	tryPoint := func(p optPoint) bool { // evaluate on TRAIN; accept only ACTIVE configs that strictly improve
		if ctx.Err() != nil || time.Since(started) > 150*time.Second {
			return false
		}
		r, ok := evalPoint(p, trainRows)
		refineSteps++
		if ok && valid(r) && r.Score > curRes.Score+1e-9 { // R36: a no-trade config can't win refinement
			cur, curRes = p, r
			return true
		}
		return false
	}
	for round := 0; round < 2; round++ {
		shrink := 1.0 / float64(round+1)
		// additive dims: per-bet cap, max exposure, confidence floor, kelly fraction
		for _, d := range []struct {
			get  func(optPoint) float64
			set  func(*optPoint, float64)
			step float64
			lo   float64
			hi   float64
		}{
			{func(p optPoint) float64 { return p.pb }, func(p *optPoint, v float64) { p.pb = v }, 0.02 * shrink, 0.005, 0.15},
			{func(p optPoint) float64 { return p.me }, func(p *optPoint, v float64) { p.me = v }, 0.15 * shrink, 0.1, 1.0},
			{func(p optPoint) float64 { return p.mc }, func(p *optPoint, v float64) { p.mc = v }, 0.12 * shrink, 0, 0.7},
			{func(p optPoint) float64 { return p.kf }, func(p *optPoint, v float64) { p.kf = v }, 0.15 * shrink, 0, 1.0},
		} {
			for _, dir := range []float64{+1, -1} {
				p := cur
				d.set(&p, clamp(d.get(cur)+dir*d.step, d.lo, d.hi))
				if p != cur {
					tryPoint(p)
				}
			}
		}
		// max-hold is a LOG dimension (0.05h..24h): step multiplicatively
		mul := 1.8
		if round > 0 {
			mul = math.Sqrt(mul)
		}
		for _, f := range []float64{mul, 1 / mul} {
			p := cur
			p.mh = clamp(cur.mh*f, 0.05, 24)
			if p != cur {
				tryPoint(p)
			}
		}
		// execution flip: maker(FOKM) vs taker
		pf := cur
		pf.fokm = !cur.fokm
		tryPoint(pf)
	}

	// ── Stage 3: HOLDOUT — coarse top-3 + refined compete on the newest 30%; TEST winner applies ─
	type holdRow struct {
		Label     string    `json:"label"`
		Point     optResult `json:"point"`
		TestScore float64   `json:"test_score"`
		TestGrow  float64   `json:"test_growth"`
		TestDD    float64   `json:"test_dd"`
		TestBets  int       `json:"test_bets"`
		TestOK    bool      `json:"test_ok"`
	}
	var holdout []holdRow
	appliedRes := top[0]
	appliedBasis := "train score (holdout off: <2000 rows)"
	if holdoutOn && len(testRows) > 0 {
		finalists := []struct {
			label string
			res   optResult
		}{}
		seen := map[string]bool{}
		add := func(label string, r optResult) {
			p := toPoint(r)
			if !seen[p.key()] {
				seen[p.key()] = true
				finalists = append(finalists, struct {
					label string
					res   optResult
				}{label, r})
			}
		}
		add("refined", curRes)
		for i, t := range top {
			if i >= 3 {
				break
			}
			add(fmt.Sprintf("coarse#%d", i+1), t)
		}
		bestTest, haveBest := holdRow{}, false
		for _, f := range finalists {
			if ctx.Err() != nil {
				break
			}
			hr := holdRow{Label: f.label, Point: f.res}
			if tr, ok := evalPoint(toPoint(f.res), testRows); ok && tr.Bets > 0 {
				hr.TestScore, hr.TestGrow, hr.TestDD, hr.TestBets, hr.TestOK = tr.Score, tr.Growth, tr.MaxDD, tr.Bets, true
			}
			holdout = append(holdout, hr)
			// R36: the holdout winner must be ACTIVE on test too (≥ minTestBets) — 4 idle bets
			// squeaking past real configs is the same degenerate optimum in disguise.
			if hr.TestOK && hr.TestBets >= minTestBets && (!haveBest || hr.TestScore > bestTest.TestScore+1e-9 ||
				(math.Abs(hr.TestScore-bestTest.TestScore) <= 1e-9 && hr.Point.Score > bestTest.Point.Score)) {
				bestTest, haveBest = hr, true
			}
		}
		if haveBest {
			appliedRes = bestTest.Point
			appliedBasis = fmt.Sprintf("holdout TEST score %.3f (%s; 70/30 chronological split, %d test rows)", bestTest.TestScore, bestTest.Label, len(testRows))
		}
	}

	// R12 (operator directive): APPLY the winner to the LIVE tunables — the same keys the Settings
	// modal writes — persisted atomically + audit-logged. Concurrent drives live; seq is read-only.
	// R36: ONLY an ACTIVE config may be applied — if nothing cleared the activity floor, the live
	// tunables stay untouched (writing a no-trade config into live settings is how stake_pct=0.005
	// snuck into config once).
	applied := map[string]any{}
	bst := appliedRes
	// R135: the legacy replay is discovery-only until its open-time/result-time chronology and
	// prospective route pricing are repaired. A chronological 70/30 split does not cure label
	// leakage inside either split, so this endpoint may report candidates but must not mutate the
	// running config. The one-share Leaderboard Backtest is the book-native profit-rate lane.
	if legacyReplayMayApply && valid(bst) {
		// R83: compute the venue-budget anchor BEFORE taking cfgMu/autoMu — venueBudgetUSD can
		// trigger an inline NAV refresh (store read + REST/venue marks). Holding the config+auto
		// locks across that work wedged every settings/auto POST behind it during the 2026-07-05
		// REST logjam (locks must never be held across starvable I/O).
		bank := s.venueBudgetUSD("kalshi") + s.venueBudgetUSD("polyus")
		// R76 (bug 18): clone+swap under cfgMu (OUTER) with the autoMu mirror syncs nested inside —
		// the same lock order as handleSettings.
		s.cfgMu.Lock()
		clone := *s.cfgP.Load()
		cfg := &clone
		s.autoMu.Lock()
		cfg.Auto.KellyFrac = bst.KellyFrac // 0 = flat sizing
		cfg.Auto.StakePct = bst.Pbcap      // flat %-of-bankroll per bet = the per-bet cap
		// R77 item 8: the execution-style winner drives THE fee-mode key (fee_maker_share);
		// maker_first is only a derived mirror. This apply-site writing the legacy flag alone is
		// exactly how config.json ended up maker_first=false + fee_maker_share=1 (Settings showed
		// TAKER, fee paths blended MAKER). fokm=false → taker (share 0); fokm=true → maker intent
		// (share restored to 1.0 only when it was 0, so a tuned hybrid share survives).
		if bst.Fokm {
			if cfg.Auto.FeeMakerShare <= 0 {
				cfg.Auto.FeeMakerShare = 1.0
			}
		} else {
			cfg.Auto.FeeMakerShare = 0
		}
		cfg.Auto.MakerFirst = cfg.Auto.FeeMakerShare > 0.001
		cfg.Auto.ConsensusMaxHoursOut = bst.MaxholdH
		// R78: the exposure cap anchors to the venues' LIVE equity-fraction budgets (frac × NAV),
		// falling back to the legacy flat bankrolls in flat mode (venueBudgetUSD handles both).
		// (bank computed above, OUTSIDE the locks — R83.)
		if bank > 0 {
			cfg.Auto.MaxExposureUSD = math.Round(bst.Maxexp * bank)
		}
		s.autoMaxExp = cfg.Auto.MaxExposureUSD
		// R76 (auditor bug 21): sync the lock-guarded Kelly mirror the sizing path ACTUALLY reads
		// (autoPlace reads s.autoKellyFrac under autoMu — handleSettings syncs it; this path never
		// did), so an "applied" Kelly tune is no longer inert until the next restart.
		s.autoKellyFrac = bst.KellyFrac
		s.autoMu.Unlock()
		s.cfgP.Store(cfg)
		s.cfgMu.Unlock()
		if s.cfgPath != "" {
			// R69 (audit P1): error no longer swallowed — an unpersisted AUTOTUNE apply silently
			// reverts sizing at the next restart.
			if err := s.writeConfigChecked(); err != nil { // R106 (bug 271): mtime clobber guard
				s.log.Error("persist AUTOTUNE config failed — applied tunables revert on restart", "err", err)
			}
		}
		applied = map[string]any{
			"kelly_frac": bst.KellyFrac, "stake_pct": bst.Pbcap, "maker_first": bst.Fokm,
			"consensus_max_hours_out": bst.MaxholdH, "max_exposure_usd": cfg.Auto.MaxExposureUSD,
		}
		_ = s.store.Audit(context.Background(), "warn", "tune",
			fmt.Sprintf("AUTOTUNE applied %s: kelly=%.2f stake_pct=%.3f maker_first=%v maxhold=%.2fh maxexp=$%.0f (train score %.3f, growth %.2fx, dd %.0f%%)",
				appliedBasis, bst.KellyFrac, bst.Pbcap, bst.Fokm, bst.MaxholdH, cfg.Auto.MaxExposureUSD, bst.Score, bst.Growth, bst.MaxDD*100), "")
	} else {
		if valid(bst) {
			appliedBasis = "NOT APPLIED — R135 quarantined the legacy signal-price tuner: overlapping rows can reveal outcomes before resolution and the replay is not prospectively book-priced; live tunables unchanged"
		} else {
			appliedBasis = fmt.Sprintf("NOT APPLIED — no config cleared the activity floor (≥%d train bets / ≥%d test bets); live tunables unchanged", minTrainBets, minTestBets)
		}
		_ = s.store.Audit(context.Background(), "warn", "tune", "AUTOTUNE: "+appliedBasis, "")
	}

	out := map[string]any{
		"tested": tested, "grid": len(cands), "elapsed_s": math.Round(time.Since(started).Seconds()),
		"best": top[0], "top": top, "applied": applied, "applied_basis": appliedBasis,
		"fill_priced_rows": fillPriced, "refine_steps": refineSteps, "refined": curRes,
		"holdout_on": holdoutOn, "train_rows": len(trainRows), "test_rows": len(testRows),
		"note": "3-stage: coarse grid (train 70%) → coordinate-descent refinement over ALL continuous dims incl. Kelly (the objective is a noisy step function — no closed-form optimum exists; descent finds the plateau center) → 70/30 chronological HOLDOUT picks what gets applied (the exact in-sample peak is usually the most overfit point; plateaus beat peaks). ACTIVITY FLOOR: a config needs ≥30 train / ≥8 test bets to win or be applied — 'never trade' scores 0.00 and is information, not a strategy. score = ln(growth) − 2.5·max_dd; near-ties prefer longer hold; fill-priced rows use REAL entry prices.",
	}
	if len(holdout) > 0 {
		out["holdout"] = holdout
	}
	if len(seqTop) > 0 {
		out["best_sequential"] = seqTop[0]
		out["top_sequential"] = seqTop
	}
	b, _ := json.Marshal(out)
	return b
}
