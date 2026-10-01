// equityalloc.go — R78: EQUITY-FRACTION BANKROLLS + EV-SHARE AUTO-ALLOCATION. PAPER ONLY —
// nothing here touches the live-money caps or order paths.
//
//  1. LEGACY EQUITY-FRACTION BANKROLLS (superseded by the four fixed R143 $600 Paper portfolios;
//     TIMES equity-wise, compounding — don't flat-limit ML to $500 when total equity is $4000"):
//     the flat per-venue paper bankrolls become allocation FRACTIONS of the live TOTAL PAPER EQUITY
//     (NAV). NAV = paper_total_start + kalshi paper net + polyus paper net (both incl. open marks)
//     + ML book net. The shadow + kflow books are EXCLUDED (experiment controls, never capital) and
//     poly-int keeps its legacy research notional (excluded from tradeable math, standing rule).
//     Venue budget = frac × NAV, recomputed on read from a ~30s-cached NAV. The ML sidecar receives
//     its alloc_ml × NAV bank via the data/ml_alloc.json handshake (written by sweepEquityAlloc,
//     read each cycle by live_ml.py next to the load_maker_share/load_max_hours config reads), so
//     its Kelly sizing compounds with the whole portfolio. paper_total_start seeds a fresh epoch:
//     Reset P&L re-baselines every net component, returning NAV to the start value.
//
//  2. EV-SHARE AUTO-ALLOCATION (paper auto only): every 30 minutes the allocator recomputes, from
//     RESOLVED closed paper trades (the same maker-net per-contract economics the by_source_venue
//     verdicts use), a per-(family, venue) stake multiplier: weight_raw = max(0, one-sided 95%
//     LOWER bound of net $/contract) at n≥30 — the LOWER bound, not the point estimate, so an n=5
//     +25¢ fluke weighs ~0 until it proves out. Closed trades already embody the inversion flags in
//     effect at placement (autoPlace flips the side BEFORE booking), so realized nets are the
//     honest after-inversion measure. Weights normalize to venue-budget shares (hard bound: no
//     family above 50% of a venue budget on venues with ≥2 enabled families); families below the
//     floor get a small exploration stake (explore_stake_frac × equal-split, default 0.1) so evidence keeps
//     accruing. When NO family on a venue clears the significance floor the allocator goes NEUTRAL
//     (×1.0 for everyone) instead of starving the venue — it differentiates only with evidence.
//     Every existing per-bet gate (EV floor, min-stake, dup/cluster caps, live-only, retire) still
//     applies — this ONLY scales stake. Changes are audited; the current table renders in the
//     Research → PERFORMANCE section (/api/alloc) and rides the export.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/paper"
)

// allocFracs returns the NORMALIZED allocation fractions (kalshi, polyus, ml, rawflow, weather)
// and whether equity-fraction mode is active (paper_total_start > 0). Negative fractions clamp to
// 0; the set is normalized by its sum so "must sum ≈ 1" is enforced on read (0.3/0.3/0.3 →
// thirds). A config whose fractions sum to 0 falls back to the operator defaults (¼/¼/½, no
// RawFlow/Weather) rather than zeroing every budget. R103: alloc_rawflow joins the normalization —
// 0/unset keeps the exact pre-R103 three-way math. R106: alloc_weather joins identically (0/unset
// = exact pre-R106 math — adding the Weather book NEVER disturbs existing epochs by construction).
// Explicitly setting paper_total_start to 0 restores legacy flat mode.
func (s *Server) allocFracs() (k, pu, ml, rf, wx float64, active bool) {
	a := s.cfg().Auto
	if a.PaperTotalStart <= 0 {
		return 0, 0, 0, 0, 0, false
	}
	k, pu, ml, rf, wx = math.Max(0, a.AllocKalshi), math.Max(0, a.AllocPolyus), math.Max(0, a.AllocML), math.Max(0, a.AllocRawFlow), math.Max(0, a.AllocWeather)
	sum := k + pu + ml + rf + wx
	if sum <= 0.001 {
		k, pu, ml, rf, wx, sum = 0.25, 0.25, 0.5, 0, 0, 1 // all-zero fractions: fall back to the operator split
	}
	return k / sum, pu / sum, ml / sum, rf / sum, wx / sum, true
}

// refreshTotalEquity recomputes NAV and caches it. Components (R78 spec): paper book equity for
// BOTH tradeable venues (realized net since the reset epoch + open positions at live marks) + the
// ML book's net since ITS epoch (lifetime.net − net_base from ml_paper.json — the same rebase the
// Go reset stamps). Shadow/kflow books excluded; poly-int excluded (research venue). NAV floors at
// 0 so budgets can't go negative.
//
// R80 COLD PATH (operator P0: paper auto placed nothing, silently): the old version had NO failure
// signal — when the store read errored (boot-cold DB / WAL recovery / transient lock) it computed
// NAV from zeroed components MINUS the persisted Reset-P&L baselines (venueRealizedNet returns
// 0−eqBaseline on its error path and caches the empty map for 20s), clamped the negative to $0 and
// stamped it FRESH — so venueBudgetUSD → platformBankroll → autoPlace, the AUTOTUNE exposure
// anchor and the ml_alloc.json handshake all consumed a fabricated $0 NAV for the next 90s (and
// each erroring 30s sweep re-stamped it). Now: the fills read is the health probe — on error the
// cache is left UNSTAMPED (the next read/sweep retries immediately) and the caller gets the
// warm-up fallback (last-known NAV, else the paper_total_start seed — never a fake $0) with
// ok=false, audited via navWarmupFallback.
func (s *Server) refreshTotalEquity(ctx context.Context) (float64, bool) {
	start := s.cfg().Auto.PaperTotalStart
	fills, ferr := s.store.ListPaperFills(ctx) // the ONE health probe every NAV component depends on
	if ferr != nil {
		return s.navWarmupFallback(ctx, ferr), false
	}
	net := s.venueRealizedNet("kalshi") + s.venueRealizedNet("polyus") // realized − fees, since last Reset P&L (20s cache inside)
	unreal := 0.0
	// hermetic tests build a Server without a Kalshi client — open marks contribute 0 there
	// (markPositions dereferences the venue client); production always has one.
	if s.kal != nil {
		positions, _ := paper.Aggregate(fills)
		_ = s.markPositions(ctx, positions)
		openN, markedN := 0, 0
		for _, p := range positions {
			if p.Contracts > 0 && (p.Platform == "kalshi" || p.Platform == "polyus") {
				unreal += p.Unrealized
				openN++
				if p.CurPrice > 0 {
					markedN++
				}
			}
		}
		// R84 (auditor DO-THIS 3, "probe marks fails"): the fills read was the ONLY health probe —
		// a pass whose venue marks ALL failed (REST logjam / feeds down) silently stamped a NAV with
		// unrealized=0 as FRESH, the same fabricated-component class R80 fixed for the store path.
		// Open tradeable positions with ZERO marked = the mark pass didn't run; serve the warm-up
		// fallback UNSTAMPED and let the next 30s sweep retry.
		if openN > 0 && markedN == 0 {
			return s.navWarmupFallback(ctx, fmt.Errorf("marks cold: 0/%d open tradeable positions marked", openN)), false
		}
	}
	mlNet := 0.0
	var pf struct {
		Lifetime struct {
			Net     float64 `json:"net"`
			NetBase float64 `json:"net_base"`
		} `json:"lifetime"`
	}
	mlPath := filepath.Join(s.cfg().DataDir, "ml_paper.json")
	if s.readJSONLoose(mlPath, &pf) {
		mlNet = pf.Lifetime.Net - pf.Lifetime.NetBase
	} else if fi, err := os.Stat(mlPath); err == nil && fi.Size() > 0 {
		// R84 (auditor DO-THIS 3, "probe mlNet fails"): the book EXISTS but didn't parse (torn write /
		// sync mirror) — dropping its net silently deflates NAV by the whole ML component and stamps
		// that as fresh. Absent file = legitimately 0 (fresh install); unreadable file = failed probe.
		return s.navWarmupFallback(ctx, fmt.Errorf("ml_paper.json present (%dB) but unreadable", fi.Size())), false
	}
	// R103: the RawFlow book is ALLOCATED portfolio money (alloc_rawflow of the $ start) — its net
	// since epoch joins NAV exactly like the ML book's (kflow/shadow research books stay excluded).
	rfNet := s.rawFlowNetSinceEpoch()
	wxNet := s.weatherNetSinceEpoch() // R106: the Weather book is allocated money too
	nav := start + net + unreal + mlNet + rfNet + wxNet
	if math.IsNaN(nav) || math.IsInf(nav, 0) {
		// R84 (auditor DO-THIS 3, "NaN→last-known"): a NaN/Inf component used to become a FRESH $0
		// NAV — the exact fabricated-zero R80 eliminated on the store path. Fallback, unstamped.
		return s.navWarmupFallback(ctx, fmt.Errorf("NAV computed non-finite (start %.2f net %.2f unreal %.2f ml %.2f rf %.2f wx %.2f)", start, net, unreal, mlNet, rfNet, wxNet)), false
	}
	if nav < 0 {
		nav = 0 // real arithmetic below zero = the portfolio is genuinely wiped; $0 budgets are then the truth
	}
	s.navMu.Lock()
	s.navUSD, s.navAt = nav, time.Now()
	s.navMu.Unlock()
	return nav, true
}

// navWarmupFallback is what NAV consumers receive while no compute has ever succeeded (or the
// current one just failed): the LAST-KNOWN cached NAV when one exists (stale-but-known beats
// snapping to the seed mid-run — the same rule the sidecar's load_ml_bank applies), else the
// paper_total_start seed, so venue budgets during warm-up are the epoch-start allocations
// (kalshi ¼×start, polyus ¼×start, ml ½×start) and NEVER $0. Audited (category "alloc",
// throttled ~10 min — best-effort: the store may be the thing that's down) so warm-up sizing
// shows in the trail instead of failing silently.
func (s *Server) navWarmupFallback(ctx context.Context, cause error) float64 {
	s.navMu.Lock()
	nav, at := s.navUSD, s.navAt
	warned := time.Since(s.navFbWarnAt) < 10*time.Minute
	if at.IsZero() && !warned {
		s.navFbWarnAt = time.Now()
	}
	s.navMu.Unlock()
	if !at.IsZero() {
		return nav // stale-but-present: the last real NAV wins over the seed
	}
	start := s.cfg().Auto.PaperTotalStart
	if !warned {
		s.log.Warn("R80 alloc-warmup: NAV cold/absent — serving paper_total_start allocations, never $0",
			"paper_total_start", start, "cause", cause)
		_ = s.store.Audit(ctx, "warn", "alloc", fmt.Sprintf(
			"R80 alloc-warmup: NAV cold/absent (%v) — venue budgets fall back to the paper_total_start=$%.0f allocations until the equity sweep succeeds", cause, start), "")
	}
	return start
}

// navColdStart reports whether NO NAV compute has ever succeeded (boot warm-up / erroring store) —
// the window where budgets ride navWarmupFallback and autoPlace tags skips "alloc-warmup".
func (s *Server) navColdStart() bool {
	s.navMu.Lock()
	defer s.navMu.Unlock()
	return s.navAt.IsZero()
}

// totalPaperEquityUSD returns the cached NAV (the 30s equity sweep keeps it fresh; a >90s-stale
// read recomputes inline with a bounded context). R80: a FAILED recompute serves the warm-up
// fallback (last-known NAV, else paper_total_start) — sizing can never consume a boot-cold zero.
func (s *Server) totalPaperEquityUSD() float64 {
	s.navMu.Lock()
	nav, at := s.navUSD, s.navAt
	s.navMu.Unlock()
	if !at.IsZero() && time.Since(at) < 90*time.Second {
		return nav
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	v, _ := s.refreshTotalEquity(ctx) // ok=false ⇒ v is already the warm-up fallback
	return v
}

// venueStartUSD is the drawdown-floor anchor for platformBankroll. R127: in equity-fraction mode
// the anchor IS the fixed venue-book bank (the four-book restructure superseded frac ×
// paper_total_start for money; the alloc_* keys stay parseable for history). 0 in legacy mode.
func (s *Server) venueStartUSD(platform string) float64 {
	_, _, _, _, _, active := s.allocFracs()
	if !active {
		return 0
	}
	switch platform {
	case "kalshi", "polyus":
		return s.bookBankUSD(platform)
	}
	return 0
}

// venueBudgetUSD is the current independently compounded sizing equity for a funded venue.
// book_*_usd is its starting/reset grant; only that venue's current-epoch fee-net P&L moves it.
// The old shared frac × NAV path remains informational. Poly-int stays research-only notional.
func (s *Server) venueBudgetUSD(platform string) float64 {
	a := s.cfg().Auto
	switch platform {
	case "kalshi":
		return s.bookSizingEquityUSD(context.Background(), vbKalshi)
	case "polyus":
		return s.bookSizingEquityUSD(context.Background(), vbPolyus)
	default: // polymarket (Poly-int) — research notional in BOTH modes
		return a.BankrollPolyIntUSD
	}
}

// venueTradeable is the track-only gate input: a venue whose ALLOCATION (or legacy bankroll) is 0
// still logs/scores/backtests everything but places no auto/AI paper bets there.
func (s *Server) venueTradeable(platform string) bool {
	k, pu, _, _, _, active := s.allocFracs()
	a := s.cfg().Auto
	switch platform {
	case "kalshi":
		if active {
			return k > 0
		}
		return a.BankrollKalshiUSD > 0
	case "polyus":
		if active {
			return pu > 0
		}
		return a.BankrollPolyUSUSD > 0
	default: // polymarket (Poly-int)
		return a.BankrollPolyIntUSD > 0
	}
}

// writeMLAllocFile writes the sidecar handshake (data/ml_alloc.json): the ML book's dynamic bank =
// alloc_ml × NAV. live_ml.py reads it each cycle (load_ml_bank, next to its load_maker_share
// config read) and anchors its book to it — Kelly sizing then compounds with the whole portfolio.
// Legacy mode removes the file so the sidecar falls back to its $250 anchor. Sub-$0.50 moves skip
// the rewrite (churn guard); tmp+rename keeps the read side torn-proof.
// R106 (auditor r34, bug 287): takes the caller's ctx (was context.Background() on the 30s sweep —
// a DB stall wedged the goroutine unbounded) and reads through the R98 fills cache (was a
// full-table ListPaperFills, contradicting its own comment).
func (s *Server) writeMLAllocFile(ctx context.Context) {
	_, _, ml, _, _, active := s.allocFracs()
	path := filepath.Join(s.cfg().DataDir, "ml_alloc.json")
	shadowEnabled := !s.cfg().Auto.ShadowBookRetired // R98: retirement rides the same handshake the sidecar already hot-reads
	if !active {
		// R99 bug 184 (auditor r24/r25, frozen-record protection): legacy mode used to DELETE the
		// handshake — and an absent file/key reads as shadow_enabled=TRUE on the sidecar
		// (load_shadow_enabled's compat default), silently UN-RETIRING the frozen shadow control
		// the moment anyone flips to legacy bankrolls. Keep a minimal file carrying the retirement
		// while it matters; no bank keys = the sidecar still falls back to its legacy $250 anchor.
		if !shadowEnabled {
			if b, err := json.Marshal(map[string]any{"shadow_enabled": false, "updated": time.Now().Unix()}); err == nil {
				tmp := path + ".tmp"
				if os.WriteFile(tmp, b, 0o644) == nil {
					_ = os.Rename(tmp, path)
				}
			}
			return
		}
		_ = os.Remove(path)
		return
	}
	nav := s.totalPaperEquityUSD()
	// R143 (four-book restructure): the ML book is the FIXED $600 book — the handshake hands
	// the sidecar book_ml_usd instead of alloc_ml × NAV. The sidecar's load_ml_bank picks up any
	// positive ml_bank_usd unchanged; its R115 Kelly anchor still sizes on min(this, own equity),
	// and resetMLBook re-anchors bank0 to the same $600 at the next reset. alloc_ml/NAV keep
	// riding along as informational fields.
	bank := s.bookBankUSD(vbML)
	// R99 CROSS-BOOK HEDGE BLOCK: hand the sidecar the auto books' OPEN (platform,ticker,side)
	// lots so the ML book can refuse the opposite side of a market its sibling books hold (they
	// share the one bankroll — a cross-book YES+NO locks in fees+spread portfolio-wide). The R98
	// O(delta) fills cache makes this read ~free at the 30s sweep cadence.
	autoOpen := []map[string]string{}
	openFP := ""
	wctx, wcancel := context.WithTimeout(ctx, 15*time.Second) // R106 (287): bounded by the caller's sweep budget
	defer wcancel()
	if fills, _, _, err := s.paperFillsCached(wctx); err == nil { // R106 (287): the R98 cache, not a full-table read
		positions, _ := paper.Aggregate(fills)
		keys := make([]string, 0, 16)
		for _, p := range positions {
			if p.Contracts > 0 && p.Ticker != "" {
				keys = append(keys, p.Platform+"|"+p.Ticker+"|"+strings.ToUpper(strings.TrimSpace(p.Side)))
			}
		}
		sort.Strings(keys)
		if len(keys) > 800 {
			keys = keys[:800] // sanity bound — the handshake must stay a small file
			s.log.Warn("ml_alloc auto_open truncated at 800 keys — late-sorting lots lose cross-book hedge coverage (bug 291)", "open_positions", len(positions))
		}
		for _, k := range keys {
			parts := strings.SplitN(k, "|", 3)
			if len(parts) == 3 {
				autoOpen = append(autoOpen, map[string]string{"platform": parts[0], "ticker": parts[1], "side": parts[2]})
			}
		}
		openFP = strings.Join(keys, ";")
	}
	s.navMu.Lock()
	last := s.mlAllocLast
	lastShadow := s.mlAllocShadowLast
	lastFP := s.mlAllocOpenFP
	s.navMu.Unlock()
	if last > 0 && math.Abs(bank-last) < 0.5 && lastShadow != nil && *lastShadow == shadowEnabled && lastFP == openFP {
		return // churn guard: bank, shadow flag AND the open-lot set are all unchanged
	}
	b, err := json.Marshal(map[string]any{
		"ml_bank_usd": math.Round(bank*100) / 100,
		// R144: two explicit destination sleeves inside the one ML grant. Their sum is exactly
		// ml_bank_usd; the Python book sizes/caps on the destination sleeve and cannot spend $600
		// once on Kalshi and again on PolyUS.
		"ml_venue_banks":    mlPaperVenueBanks(bank),
		"alloc_ml":          ml,
		"total_equity_usd":  math.Round(nav*100) / 100,
		"paper_total_start": s.cfg().Auto.PaperTotalStart,
		"shadow_enabled":    shadowEnabled, // R98: sidecar stops OPENING shadow lots when false (settle path unaffected)
		"auto_open":         autoOpen,      // R99: open auto-book lots — sidecar's cross-book hedge block reads these
		"updated":           time.Now().Unix(),
	})
	if err != nil {
		return
	}
	tmp := path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) != nil {
		return
	}
	if os.Rename(tmp, path) != nil {
		_ = os.Remove(tmp)
		return
	}
	s.navMu.Lock()
	s.mlAllocLast = bank
	s.mlAllocShadowLast = &shadowEnabled
	s.mlAllocOpenFP = openFP
	s.navMu.Unlock()
}

// invalidateEquityCache busts the NAV cache (Reset P&L path — the fresh epoch must apply on the
// very next read, not up to 90s later) and pushes a fresh handshake to the sidecar.
func (s *Server) invalidateEquityCache(ctx context.Context) {
	s.navMu.Lock()
	s.navAt, s.mlAllocLast = time.Time{}, 0
	s.navMu.Unlock()
	s.refreshTotalEquity(ctx)
	s.writeMLAllocFile(ctx)
}

// sweepEquityAlloc is the 30s equity sweep (MonitorPaper): refresh NAV, keep the sidecar handshake
// current, and rerun the EV-share allocator on its 30-minute cadence (auto-regulating). R79: the
// PLACEMENT POLICY (placepolicy.go) rebuilds on the same 30-minute cadence right here — retire /
// invert / default-ON per family, flips audited — so allocation and placement re-decide together.
func (s *Server) sweepEquityAlloc(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	s.refreshTotalEquity(cctx)
	s.writeMLAllocFile(cctx)
	s.allocMu.Lock()
	stale := s.allocBuiltAt.IsZero() || time.Since(s.allocBuiltAt) >= 30*time.Minute
	s.allocMu.Unlock()
	if stale {
		s.rebuildAllocations(cctx)
	}
	s.polMu.Lock()
	pstale := s.polBuiltAt.IsZero() || time.Since(s.polBuiltAt) >= 30*time.Minute
	s.polMu.Unlock()
	if pstale { // polBuiltAt stamps only on SUCCESS → a cold-DB first pull retries every 30s sweep
		s.rebuildPlacementPolicy(cctx)
	}
	// R90 (auditor DO-THIS 5i / edge 29): per-family realization ratios ride the same sweep
	// (self-throttled ≥25min inside) — allocation, policy and the EV haircut re-decide together.
	s.refreshRealizationRatios(cctx)
	// R90 DO-THIS 14: model_prob backfill (<15min recency, ~5min cadence, bounded) — coverage
	// 88.4% → ~99% once rows logged before the sidecar scored get their late stamp.
	s.backfillModelProb(cctx)
}

// backfillModelProb — R90 DO-THIS 14: stamp late-arriving sidecar p_wins onto rows inserted
// before the model scored their market. Single-goroutine caller (MonitorPaper's sweep).
func (s *Server) backfillModelProb(ctx context.Context) {
	if time.Since(s.mprobBfAt) < 5*time.Minute {
		return
	}
	s.mprobBfAt = time.Now()
	rows, err := s.store.ListNullModelProb(ctx, time.Now().Add(-15*time.Minute).UTC().Format(time.RFC3339), 500)
	if err != nil || len(rows) == 0 {
		return
	}
	n := 0
	for _, r := range rows {
		if pw, ok := s.mlPWin(r.Ticker, r.Side); ok && pw > 0 {
			if s.store.SetModelProb(ctx, r.ID, pw) == nil {
				n++
			}
		}
	}
	if n > 0 {
		s.log.Info("R90 model_prob backfill", "stamped", n, "candidates", len(rows))
	}
}

// allocRow is one (family, venue) line of the EV-share allocation table (Research → PERFORMANCE).
type allocRow struct {
	Source  string  `json:"source"`
	Venue   string  `json:"venue"`
	N       int     `json:"n"`        // resolved closed paper trades behind the bound
	EVLBPC  float64 `json:"ev_lb_pc"` // one-sided 95% LOWER bound of realized net $/contract (fee-inclusive, after inversion flags)
	Share   float64 `json:"share"`    // fraction of the venue budget (post 50% hard bound)
	Mult    float64 `json:"mult"`     // stake multiplier vs equal-split (what autoPlace applies)
	Explore bool    `json:"explore"`  // below the significance floor → exploration stake only
	Neutral bool    `json:"neutral"`  // venue had NO proven family → allocator neutral (×1.0)
}

// allocCandidates lists the auto families that CAN place on a venue, with their live placement
// gates. Directional auto-cons-* families are ml-routed (not placement-enabled) while
// MLDrivenDirectional is on — exactly mirroring autoPlace's routing. Poly-int is excluded from
// the allocator entirely (research venue).
func allocCandidates(a config.AutoConfig, venue string) []string {
	out := []string{}
	add := func(src string, on bool) {
		if on {
			out = append(out, src)
		}
	}
	add("auto-ml", a.MLDrivenDirectional) // the ONE-PORTFOLIO directional executor
	cons := a.ConsensusEnabled
	dir := cons && !a.MLDrivenDirectional // directional families place only when NOT ml-routed
	switch venue {
	case "kalshi":
		add("auto-arb", cons && a.ConsensusArb)
		add("auto-cons-kflow", dir && a.ConsensusKalshiFlow) // R79: kflow places now (autoTick 3a-KFLOW) — allocator sizes it like every family
		add("auto-cons-kalshi", dir && a.ConsensusKalshi)
		add("auto-cons-x", dir && a.ConsensusCross)
		add("auto-cons-kcrypto", dir && a.ConsensusCrypto)
		add("auto-cons-xmatch", dir && a.ConsensusXMatch)
		add("auto-cons-kthresh", dir && a.ConsensusKThresh)
		add("auto-cons-pmatch", dir && a.ConsensusPMatch)
		add("auto-cons-pbridge", dir && a.ConsensusPCrypto) // pbridge places under the pcrypto gate
		add("auto-cons-favlong", dir && a.ConsensusFavLong)
		add("auto-cons-confluence", dir && a.ConsensusConfluence)
		add("auto-cons-xvlag", dir && a.ConsensusConfluence && a.ConsensusXVLag) // xvlag runs inside the confluence matched-set
	case "polyus":
		add("auto-cons-pusflow", dir && a.ConsensusPolyUSFlow)
		add("auto-cons-pmatch", dir && a.ConsensusPMatch)
		add("auto-cons-confluence", dir && a.ConsensusConfluence)
		add("auto-cons-xvlag", dir && a.ConsensusConfluence && a.ConsensusXVLag)
	}
	return out
}

// rebuildAllocations recomputes the EV-share table from resolved closed paper trades (the same
// realized maker-net per-contract economics computeVerdicts/by_source_venue grade — fees included,
// inversion flags already embodied in what was placed). Runs on the 30-minute sweep cadence; logs
// material changes to the audit trail.
func (s *Server) rebuildAllocations(ctx context.Context) {
	a := s.cfg().Auto
	explore := a.ExploreStakeFrac
	if explore <= 0 {
		explore = 0.10 // codebase convention: 0/unset = the documented default
	}
	if explore > 1 {
		explore = 1
	}
	fills, err := s.store.ListPaperFills(ctx)
	if err != nil {
		return // keep the previous table on a DB hiccup
	}
	type acc struct {
		n         int
		sum, sum2 float64
	}
	stats := map[string]*acc{} // "source|venue" over per-contract nets
	for _, t := range paper.ClosedTrades(fills) {
		if !strings.HasPrefix(t.Source, "auto-") || (t.Platform != "kalshi" && t.Platform != "polyus") {
			continue
		}
		ctr := t.Contracts
		if ctr < 1 {
			ctr = 1
		}
		pc := (t.Realized - t.Fees) / ctr
		k := t.Source + "|" + t.Platform
		g := stats[k]
		if g == nil {
			g = &acc{}
			stats[k] = g
		}
		g.n++
		g.sum += pc
		g.sum2 += pc * pc
	}
	const minN = 30 // significance floor: LB demands n≥30 resolved (an n=5 +25¢ fluke weighs ~0)
	newTable := map[string]float64{}
	newRows := []allocRow{}
	for _, venue := range []string{"kalshi", "polyus"} {
		fams := allocCandidates(a, venue)
		if len(fams) == 0 {
			continue
		}
		type fw struct {
			src   string
			n     int
			lb, w float64
		}
		fws := make([]fw, 0, len(fams))
		sumW := 0.0
		for _, src := range fams {
			g := stats[src+"|"+venue]
			row := fw{src: src}
			if g != nil {
				row.n = g.n
				row.lb = evLowerBound(g.sum, g.sum2, g.n)
			}
			if row.n >= minN && row.lb > 0 {
				row.w = row.lb
				sumW += row.w
			}
			fws = append(fws, row)
		}
		famN := float64(len(fams))
		if sumW <= 0 {
			// No family on this venue clears the significance floor → NEUTRAL (×1.0 for all).
			// The allocator only differentiates WITH evidence; going all-explorers here would
			// silently 10× down every stake on the venue — a regression, not an allocation.
			for _, f := range fws {
				newTable[f.src+"|"+venue] = 1
				newRows = append(newRows, allocRow{Source: f.src, Venue: venue, N: f.n,
					EVLBPC: math.Round(f.lb*10000) / 10000, Share: math.Round(1/famN*1000) / 1000, Mult: 1, Neutral: true})
			}
			continue
		}
		// Shares = weight/Σweights, then the HARD BOUND: no family above 50% of a venue budget.
		// Applied whenever the venue has ≥2 enabled families (the bound is a concentration control
		// BETWEEN families); a venue with a single enabled family has no allocation decision to
		// make — capping it would just idle half the budget, so it keeps its full share (×1.0).
		// Excess redistributes to the uncapped positive families proportionally.
		share := make([]float64, len(fws))
		for i, f := range fws {
			share[i] = f.w / sumW
		}
		if len(fws) >= 2 {
			for iter := 0; iter < 4; iter++ {
				excess, unc := 0.0, 0.0
				for i := range share {
					if share[i] > 0.5 {
						excess += share[i] - 0.5
						share[i] = 0.5
					}
				}
				for i := range share {
					if share[i] > 0 && share[i] < 0.5 {
						unc += share[i]
					}
				}
				if excess <= 1e-9 || unc <= 1e-9 {
					break
				}
				for i := range share {
					if share[i] > 0 && share[i] < 0.5 {
						share[i] += excess * (share[i] / unc)
					}
				}
			}
		}
		for i, f := range fws {
			mult, expl := share[i]*famN, false
			if f.w <= 0 { // below the floor → exploration stake so data keeps accruing
				mult, expl = explore, true
				share[i] = explore / famN
			}
			newTable[f.src+"|"+venue] = mult
			newRows = append(newRows, allocRow{Source: f.src, Venue: venue, N: f.n,
				EVLBPC: math.Round(f.lb*10000) / 10000, Share: math.Round(share[i]*1000) / 1000,
				Mult: math.Round(mult*100) / 100, Explore: expl})
		}
	}
	sort.Slice(newRows, func(i, j int) bool {
		if newRows[i].Venue != newRows[j].Venue {
			return newRows[i].Venue < newRows[j].Venue
		}
		return newRows[i].Share > newRows[j].Share
	})
	// Diff vs the previous table → audit material changes (new/gone families, mult moves ≥0.05).
	s.allocMu.Lock()
	old := s.allocTable
	first := s.allocBuiltAt.IsZero()
	s.allocTable, s.allocRows, s.allocBuiltAt = newTable, newRows, time.Now()
	s.allocMu.Unlock()
	changes := []string{}
	for k, v := range newTable {
		if ov, ok := old[k]; !ok || math.Abs(ov-v) >= 0.05 {
			from := "new"
			if ok {
				from = fmt.Sprintf("×%.2f", ov)
			}
			changes = append(changes, fmt.Sprintf("%s %s→×%.2f", k, from, v))
		}
	}
	for k, ov := range old {
		if _, ok := newTable[k]; !ok {
			changes = append(changes, fmt.Sprintf("%s ×%.2f→gone", k, ov))
		}
	}
	if len(changes) > 0 && !(first && len(old) == 0 && allNeutral(newRows)) {
		sort.Strings(changes)
		msg := "EV-share allocation updated: " + strings.Join(changes, " · ")
		if len(msg) > 900 {
			msg = msg[:900] + "…"
		}
		detail, _ := json.Marshal(newRows)
		s.log.Info("R78 EV-share allocation changed", "changes", len(changes))
		_ = s.store.Audit(ctx, "info", "alloc", msg, string(detail))
	}
}

// allNeutral reports whether every allocation row is the no-evidence neutral ×1.0 — a first build
// landing all-neutral is the steady state, not a change worth an audit row every boot.
func allNeutral(rows []allocRow) bool {
	for _, r := range rows {
		if !r.Neutral {
			return false
		}
	}
	return true
}

// allocStakeMult returns the EV-share stake multiplier for an auto bet (1.0 for gate/manual/AI,
// unknown families, poly-int, or before the first table build). autoPlace applies it AFTER the
// base/Kelly sizing and re-clamps to the 25%-of-budget cap — it ONLY scales, never gates.
func (s *Server) allocStakeMult(source, platform string) float64 {
	if !strings.HasPrefix(source, "auto-") || (platform != "kalshi" && platform != "polyus") {
		return 1 // paper AUTO only (spec) — non-AUTO legacy/research rows stay unscaled
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()
	if s.allocTable == nil {
		return 1
	}
	if m, ok := s.allocTable[source+"|"+platform]; ok && m > 0 {
		return m
	}
	return 1
}

// handleAlloc (GET /api/alloc) serves the live equity-fraction + EV-share state: NAV, per-venue
// budgets, and the allocation table — plus, R79, the PLACEMENT POLICY table (family →
// on/inverted/retired/research with both LBs) rendered right next to it in Research →
// PERFORMANCE; captured by the export.
func (s *Server) handleAlloc(w http.ResponseWriter, r *http.Request) {
	k, pu, ml, rf, wx, active := s.allocFracs()
	nav := s.totalPaperEquityUSD()
	s.allocMu.Lock()
	rows := make([]allocRow, len(s.allocRows))
	copy(rows, s.allocRows)
	builtAt := s.allocBuiltAt
	s.allocMu.Unlock()
	a := s.cfg().Auto
	explore := a.ExploreStakeFrac
	if explore <= 0 {
		explore = 0.10
	}
	built := ""
	if !builtAt.IsZero() {
		built = builtAt.UTC().Format(time.RFC3339)
	}
	prows, pbuiltAt := s.policyView()
	pbuilt := ""
	if !pbuiltAt.IsZero() {
		pbuilt = pbuiltAt.UTC().Format(time.RFC3339)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active":            active,
		"total_equity_usd":  math.Round(nav*100) / 100,
		"paper_total_start": a.PaperTotalStart,
		"alloc":             map[string]float64{"kalshi": k, "polyus": pu, "ml": ml, "rawflow": rf, "weather": wx},
		// R84 (auditor DO-THIS 3, "frac×nav" — the /api/alloc 8s timeout): the kalshi/polyus budgets
		// used to call venueBudgetUSD, EACH of which re-derives NAV (and, >90s stale, re-COMPUTES it
		// inline: store read + venue marks) — three potential NAV computes per request during a
		// stall. ONE compute above; every budget is frac × that same snapshot. In legacy (flat) mode
		// the fracs are 0 → serve the flat bankrolls (what venueBudgetUSD returned there).
		"budgets": func() map[string]float64 {
			// Current budgets are independently compounded Paper equities (shared NAV superseded).
			return map[string]float64{
				"kalshi":    s.bookSizingEquityUSD(r.Context(), vbKalshi),
				"polyus":    s.bookSizingEquityUSD(r.Context(), vbPolyus),
				"ml":        s.bookSizingEquityUSD(r.Context(), vbML),
				"combos":    s.bookSizingEquityUSD(r.Context(), vbCombos),
				"ml_combos": s.mlComboPaperStatus(r.Context()).Equity,
				"rawflow":   math.Round(rf*nav*100) / 100, // R103 (display; folded — alloc 0)
				"weather":   math.Round(wx*nav*100) / 100, // R106 (display; money now rides the Kalshi book)
			}
		}(),
		"r127_books_note":      "Compatibility key: auto.book_*_usd is each portfolio's reset grant; current sizing equity compounds only that portfolio's current-epoch fee-net P&L",
		"r133_portfolios_note": "Five independently compounding Paper portfolios; 'portfolio' is distinct from an exchange order book",
		// R128: the scoreboard-driven allocation engine — per-book roster weights
		// (weight_i = max(0,¢_i)/Σ max(0,¢_j), lifetime realized) + the live per-family stake
		// multipliers the shared auto executor applies (mean-normalized, replaces the R78 table).
		"book_weights": func() any {
			s.swMu.Lock()
			t := s.swTable
			s.swMu.Unlock()
			if t == nil {
				return nil
			}
			return map[string]any{"books": t.Books, "auto_mult": t.AutoMult,
				"sub_share_usd": t.SubShare, "built_at": t.At.UTC().Format(time.RFC3339),
				"formula": "weight = max(0, lifetime ¢/unit) / Σ positives · collecting strategies split a 10% exploration pool · negatives get 0 (logging never stops; inverted expression enters via placement policy)"}
		}(),
		"explore_stake_frac": explore,
		"min_n":              30,
		"rows":               rows,
		"built_at":           built,
		"policy":             prows,
		"policy_built_at":    pbuilt,
		"policy_min_n":       policyMinN,
	})
}
