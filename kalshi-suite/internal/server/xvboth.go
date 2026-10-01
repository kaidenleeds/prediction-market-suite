package server

// xvboth.go — R126: the both-side gap-scanning support module.
//
//   Part 1 (both-side scanning): pure side-derivation helpers + the per-day opportunity counters
//     that answer the operator's "is it really ~2×?" question with dedup-honest numbers (one
//     opportunity per (pair, direction, episode) — a YES-side gap and its mirrored NO-side reading
//     are ONE signal; the counters split legacy-executable vs newly-executable vs flip-new).
//   Part 2 (episode re-entry): the gap-episode tracker. A gap opens → episode N; closes (≤1¢);
//     reopens ≥ threshold → episode N+1 → the executor may re-enter (same direction only,
//     self-hedge guarded, capped per market). State is kv-persisted across restarts.
//   Part 4.2 (wait-vs-now, LOG-ONLY): for Kalshi-side gap candidates, log the decision the
//     E[wait] formula WOULD make and then measure what actually happened (partner arrival,
//     gap survival) so the formula's parameters (partner arrival rate, gap half-life) are
//     learned from reality before anything goes live.
//   Part 4.3 (combo overlay, PAPER): when a Kalshi gap candidate has an ALREADY-LIVE comboable
//     same-game partner, place a correlated 2-leg combo in a small paper ledger (tag
//     combo_overlay) priced at the NAIVE LEG-PRODUCT (the Part 3 probe measures whether the
//     venue really prices there). Kelly-aware half stakes. No venue orders, ever.
//   Part 5 (ROI-band weights): loader for data\roi_bands.json (written by tools\r126_bands.py
//     from resolved rows) + the ranking-weight lookup. LOG-ONLY unless roi_band_rank_arm.
//   Part 6.2 (offline gate): pxOfflineReason — fail-closed refusal of new auto placements when
//     the venue data pipeline for the market is provably dead (stale-snapshot pricing fictions).

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
)

// ---------------------------------------------------------------------------------------------
// Part 1 — pure side derivation (pinned in r126_test.go)
// ---------------------------------------------------------------------------------------------

// xvBothSides maps a signed unified gap (gap = kYes − pusK, where pusK is the PUS price expressed
// in Kalshi-YES space: pusK = pusYes on same-side twins, 1−pusYes on R106 flip twins) to the
// PHYSICAL sides each venue's expression buys:
//   - PUS expression: buy the PUS instrument priced BELOW its Kalshi-referenced fair (the proven
//     "PUS lags" mechanism — the only expression the executor ever trades);
//   - Kalshi expression: buy the cheap Kalshi side (log-only; the kalshi cells are CI-negative).
//
// gap>0 ⇒ PUS K-YES-equivalent cheap ⇒ PUS buys YES (same-side) / NO (flip); Kalshi NO is cheap.
// gap<0 ⇒ PUS K-NO-equivalent cheap ⇒ PUS buys NO (same-side) / YES (flip); Kalshi YES is cheap.
func xvBothSides(gap float64, flip bool) (pusSide, kSide string) {
	buyYes := (gap > 0) != flip
	pusSide = "NO"
	if buyYes {
		pusSide = "YES"
	}
	kSide = "NO"
	if gap < 0 {
		kSide = "YES"
	}
	return pusSide, kSide
}

// xvExprEdge is one execution expression's executable economics (ask + fee + depth), used to
// pick the BEST expression per deduped signal. Depth −1 = unobservable (logged honestly).
type xvExprEdge struct {
	Expr   string  // pus_yes | pus_no | k_yes | k_no
	AskPx  float64 // price actually paid crossing the book
	EdgeC  float64 // (fair − ask − fee) in cents — the executable edge
	DepthC float64 // contracts at the touch (−1 unknown)
}

// xvBestExpr compares the PUS vs Kalshi expressions of ONE deduped gap signal by executable
// edge at the ask net of fees (the operator's ask+fee+depth rule; zero/unknown depth loses ties).
func xvBestExpr(pus, kal xvExprEdge) (best, alt xvExprEdge) {
	pb, kb := pus.EdgeC, kal.EdgeC
	if pus.DepthC == 0 { // a visible EMPTY touch can't be executed; unknown (−1) stays comparable
		pb = math.Inf(-1)
	}
	if kal.DepthC == 0 {
		kb = math.Inf(-1)
	}
	if kb > pb {
		return kal, pus
	}
	return pus, kal
}

// ---------------------------------------------------------------------------------------------
// Part 2 — episode tracker
// ---------------------------------------------------------------------------------------------

const (
	xvEpCloseC  = 1.0 // a gap ≤1¢ ends the episode (operator spec)
	xvEpKVKey   = "xvgap_episodes_v1"
	xvEpMaxKeys = 4000 // map bound: oldest inactive pairs pruned at flush
)

type xvEpState struct {
	N      int     `json:"n"`       // lifetime episode ordinal for this pair
	Active bool    `json:"active"`  // a gap ≥ threshold is currently open
	Sign   int     `json:"sign"`    // +1: PUS K-YES-equiv cheap · −1: Kalshi side cheap
	OpenTS int64   `json:"open_ts"` // unix seconds the current/last episode opened
	LastC  float64 `json:"last_c"`  // last |gap| seen (cents)
	SeenTS int64   `json:"seen_ts"` // unix seconds last observed (prune key)
}

type xvEpStore struct {
	Eps map[string]*xvEpState       `json:"eps"`
	Dur map[string][]float64        `json:"dur"` // class → recent episode durations (minutes, ring ≤64)
	Opp map[string]map[string]int64 `json:"opp"` // day → {sameside_yes, sameside_no, flip} new-episode counts
}

func (s *Server) xvEpLoadLocked(ctx context.Context) *xvEpStore {
	if s.xvEpSt != nil {
		return s.xvEpSt
	}
	st := &xvEpStore{Eps: map[string]*xvEpState{}, Dur: map[string][]float64{}, Opp: map[string]map[string]int64{}}
	if raw, ok := s.store.KVGet(ctx, xvEpKVKey); ok && raw != "" {
		_ = json.Unmarshal([]byte(raw), st) // loose: a poisoned blob restarts tracking, never crashes
		if st.Eps == nil {
			st.Eps = map[string]*xvEpState{}
		}
		if st.Dur == nil {
			st.Dur = map[string][]float64{}
		}
		if st.Opp == nil {
			st.Opp = map[string]map[string]int64{}
		}
	}
	s.xvEpSt = st
	return st
}

// xvEpObserve advances one pair's episode state from the current signed gap (cents) and returns
// (episode ordinal, newEpisode). Called for EVERY matched pair each scan — including sub-threshold
// gaps, which is what closes episodes. class buckets the duration ring (ml/spread/total/other).
func (s *Server) xvEpObserve(ctx context.Context, pairKey, class string, gapC, minGapC, reopenC float64) (int, bool) {
	now := time.Now()
	sgn := 1
	if gapC < 0 {
		sgn = -1
	}
	mag := math.Abs(gapC)
	s.xvEpMu.Lock()
	defer s.xvEpMu.Unlock()
	st := s.xvEpLoadLocked(ctx)
	e := st.Eps[pairKey]
	if e == nil {
		e = &xvEpState{}
		st.Eps[pairKey] = e
	}
	e.SeenTS = now.Unix()
	isNew := false
	closeEp := func() {
		e.Active = false
		if e.OpenTS > 0 {
			durMin := now.Sub(time.Unix(e.OpenTS, 0)).Minutes()
			if durMin >= 0 && durMin < 24*60 {
				ring := append(st.Dur[class], math.Round(durMin*10)/10)
				if len(ring) > 64 {
					ring = ring[len(ring)-64:]
				}
				st.Dur[class] = ring
			}
		}
		s.xvEpDirty = true
	}
	openEp := func() {
		e.Active, e.Sign, e.OpenTS = true, sgn, now.Unix()
		e.N++
		isNew = true
		s.xvEpDirty = true
	}
	switch {
	case e.Active && mag <= xvEpCloseC:
		closeEp()
	case e.Active && sgn != e.Sign && mag >= minGapC:
		closeEp() // direction flipped through zero between scans — old episode over,
		openEp()  // the opposite-direction gap is a NEW episode
	case !e.Active:
		thr := minGapC // first-ever episode opens at the family's own threshold
		if e.N > 0 && reopenC > 0 {
			thr = reopenC // re-open threshold (config; 0 ⇒ same as minGap)
		}
		if mag >= thr {
			openEp()
		}
	}
	e.LastC = mag
	return e.N, isNew
}

// xvOppNote counts one NEW-episode opportunity into today's dedup-honest bucket.
func (s *Server) xvOppNote(ctx context.Context, bucket string) {
	day := time.Now().UTC().Format("2006-01-02")
	s.xvEpMu.Lock()
	st := s.xvEpLoadLocked(ctx)
	m := st.Opp[day]
	if m == nil {
		m = map[string]int64{}
		st.Opp[day] = m
		for d := range st.Opp { // keep ≤7 days
			if d < day && len(st.Opp) > 7 {
				delete(st.Opp, d)
			}
		}
	}
	m[bucket]++
	s.xvEpDirty = true
	s.xvEpMu.Unlock()
}

// xvEpHalfLifeMin — measured median episode duration for a class (the wait-formula's gap
// half-life input). Falls back to the all-class ring, then 10 minutes (documented prior).
func (s *Server) xvEpHalfLifeMin(ctx context.Context, class string) float64 {
	s.xvEpMu.Lock()
	defer s.xvEpMu.Unlock()
	st := s.xvEpLoadLocked(ctx)
	med := func(xs []float64) float64 {
		if len(xs) == 0 {
			return -1
		}
		cp := append([]float64(nil), xs...)
		sort.Float64s(cp)
		return cp[len(cp)/2]
	}
	if v := med(st.Dur[class]); v > 0 {
		return v
	}
	var all []float64
	for _, xs := range st.Dur {
		all = append(all, xs...)
	}
	if v := med(all); v > 0 {
		return v
	}
	return 10
}

// xvEpFlush persists the tracker when dirty (bounded map). Called at sweep end.
func (s *Server) xvEpFlush(ctx context.Context) {
	s.xvEpMu.Lock()
	if !s.xvEpDirty || s.xvEpSt == nil {
		s.xvEpMu.Unlock()
		return
	}
	st := s.xvEpSt
	if len(st.Eps) > xvEpMaxKeys { // prune oldest INACTIVE pairs
		type kv struct {
			k  string
			ts int64
		}
		var idle []kv
		for k, e := range st.Eps {
			if !e.Active {
				idle = append(idle, kv{k, e.SeenTS})
			}
		}
		sort.Slice(idle, func(i, j int) bool { return idle[i].ts < idle[j].ts })
		for i := 0; i < len(idle) && len(st.Eps) > xvEpMaxKeys; i++ {
			delete(st.Eps, idle[i].k)
		}
	}
	b, err := json.Marshal(st)
	s.xvEpDirty = false
	s.xvEpMu.Unlock()
	if err == nil {
		_ = s.store.KVSet(ctx, xvEpKVKey, string(b))
	}
}

// xvgEpisodeGate — the Part 2 entry rule that replaces one-lot-per-market (pure; pinned in
// r126_test.go). "" = enter; otherwise the refusal reason.
//   - opposite-side lot open on the market → refuse (self-hedge guard)
//   - a lot from the SAME episode (or an untracked-episode lot) → refuse (no stacking inside one episode)
//   - ≥ maxEp lots on the market → refuse (per-market exposure cap; default 3)
func xvgEpisodeGate(open []kfPos, ticker, side string, epN, maxEp int) string {
	if maxEp <= 0 {
		maxEp = 3
	}
	lots := 0
	for _, p := range open {
		if p.Ticker != ticker {
			continue
		}
		if !strings.EqualFold(p.Side, side) {
			return "self-hedge" // never bet against our own open lot
		}
		if epN <= 0 || p.EpisodeN == epN || p.EpisodeN == 0 {
			return "same-episode" // one entry per episode; pre-R126 lots (0) block conservatively
		}
		lots++
	}
	if lots >= maxEp {
		return "episode-cap"
	}
	return ""
}

// ---------------------------------------------------------------------------------------------
// Part 6.2 — offline placement gate
// ---------------------------------------------------------------------------------------------

const pxOfflineMaxAge = 3 * time.Minute

// pxOfflineReason returns non-"" when the venue data pipeline for this market is provably DEAD:
// no fresh WS price AND the REST-refreshed snapshot is stale beyond pxOfflineMaxAge. That is the
// PC-offline / feed-outage shape — entryAsk/entryBid would otherwise price off a frozen book or
// the signal±2¢ fiction. Fail-closed on staleness EVIDENCE only; when the R108 watchdogs heal the
// feeds, placements resume by themselves. poly-int has no cheap age signal here → never gated.
func (s *Server) pxOfflineReason(platform, ticker string) string {
	switch strings.ToLower(platform) {
	case "kalshi", "":
		if s.kal != nil {
			if _, at, ok := s.kal.LivePriceAt(ticker); ok && time.Since(at) <= 30*time.Second {
				return ""
			}
		}
		s.metaMu.Lock()
		at, ok := s.kmktsAt[ticker]
		s.metaMu.Unlock()
		if ok && time.Since(at) <= pxOfflineMaxAge {
			return ""
		}
		s.kobMu.Lock()
		if e, cok := s.kobCache[ticker]; cok && time.Since(e.at) <= pxOfflineMaxAge {
			s.kobMu.Unlock()
			return ""
		}
		s.kobMu.Unlock()
		return "px-offline:kalshi"
	case "polyus":
		if s.polyUSWS != nil {
			// LiveYesAt owns generation, primary-transport, and lifecycle validity.
			// PolyUS publishes complete snapshots on change, so receipt age alone is not offline.
			if _, _, ok := s.polyUSWS.LiveYesAt(ticker); ok {
				return ""
			}
		}
		s.polyUSMu.Lock()
		age := time.Since(s.polyUSAt)
		s.polyUSMu.Unlock()
		if age <= pxOfflineMaxAge {
			return ""
		}
		return "px-offline:polyus"
	}
	return ""
}

// ---------------------------------------------------------------------------------------------
// Part 4.2 — wait-vs-now (LOG-ONLY decision study)
// ---------------------------------------------------------------------------------------------

const (
	xvWaitKVKey   = "xvwait_stats_v1"
	xvWaitFile    = "xvwait.jsonl"
	xvWaitMaxPend = 200
	xvWaitRingCap = 20
)

type xvWaitPending struct {
	TS         time.Time `json:"ts"`
	Ticker     string    `json:"ticker"` // the Kalshi gap leg
	PusSlug    string    `json:"pus_slug"`
	Flip       bool      `json:"flip"`
	Class      string    `json:"class"`
	Sport      string    `json:"sport"`
	GapC       float64   `json:"gap_c"` // |gap| at decision time
	Deadline   time.Time `json:"deadline"`
	PartnerAt  int64     `json:"partner_at"` // unix; 0 = not yet
	PartnerTkr string    `json:"partner_tkr"`
}

// xvWaitCalc — the operator's formula, pure (pinned in r126_test.go):
//
//	E[wait]¢ = P(partner arrives within the gap half-life) × comboBonus¢ − expectedDecay¢
//
// pArrive = 1 − e^(−λ·HL)  (λ /hour, HL minutes); bonus = max(0,ρ)·√(p1q1·p2q2)·100 (the
// correlation premium IF the venue prices combos at the naive product — Part 3 measures that);
// decay = gap/2 (by the definition of half-life, the expected gap left after one HL is half).
func xvWaitCalc(lambdaHr, hlMin, rho, p1, p2, gapC float64) (pArr, bonusC, decayC, eWaitC float64) {
	if lambdaHr < 0 {
		lambdaHr = 0
	}
	pArr = 1 - math.Exp(-lambdaHr*hlMin/60)
	if rho > 0 && p1 > 0 && p1 < 1 && p2 > 0 && p2 < 1 {
		bonusC = rho * math.Sqrt(p1*(1-p1)*p2*(1-p2)) * 100
	}
	decayC = gapC / 2
	eWaitC = pArr*bonusC - decayC
	return
}

// xvKalMidFresh — fresh Kalshi YES mid: WS live price first, then meta ≤2min (the detector's rule).
func (s *Server) xvKalMidFresh(ticker string) (float64, bool) {
	if s.kal != nil {
		if lp, ok := s.kal.LivePrice(ticker); ok && lp > 0 && lp < 1 {
			return lp, true
		}
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	if km, ok := s.kmkts[ticker]; ok {
		if at, okA := s.kmktsAt[ticker]; okA && time.Since(at) <= 2*time.Minute {
			if p := km.ImpliedProbability(); p > 0 && p < 1 {
				return p, true
			}
		}
	}
	return 0, false
}

// xvPartnerScan finds live same-game different-class Kalshi partners for a gap leg (the
// "comboable partner" of the operator's design). Same middle ticker segment, fresh meta,
// priced 3–97¢. Bounded by the kmkts map size; callers are sweep-throttled.
func (s *Server) xvPartnerScan(kTicker string) []struct {
	Ticker string
	Class  string
	Mid    float64
} {
	cls, _, ok := r126Classify(kTicker)
	if !ok {
		return nil
	}
	i, j := strings.Index(kTicker, "-"), strings.LastIndex(kTicker, "-")
	if i < 0 || j <= i {
		return nil
	}
	mid := kTicker[i+1 : j]
	var out []struct {
		Ticker string
		Class  string
		Mid    float64
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	for tk, km := range s.kmkts {
		if tk == kTicker {
			continue
		}
		i2, j2 := strings.Index(tk, "-"), strings.LastIndex(tk, "-")
		if i2 < 0 || j2 <= i2 || tk[i2+1:j2] != mid {
			continue
		}
		c2, _, ok2 := r126Classify(tk)
		if !ok2 || c2 == cls {
			continue
		}
		if at, okA := s.kmktsAt[tk]; !okA || time.Since(at) > 2*time.Minute {
			continue
		}
		p := km.ImpliedProbability()
		if p <= 0.03 || p >= 0.97 {
			continue
		}
		out = append(out, struct {
			Ticker string
			Class  string
			Mid    float64
		}{tk, c2, p})
		if len(out) >= 8 {
			break
		}
	}
	return out
}

// xvPairRho — the measured class-pair correlation for two Kalshi legs of one game (parlaylab's
// own rho cells, shrunk; 0 when unmeasured). rhoN reports the evidence behind it.
func (s *Server) xvPairRho(legA, legB string) (rho float64, rhoN int, class string) {
	ek := s.legEventKey("kalshi", legA, "")
	legs := []plabLeg{{Ticker: legA, Platform: "kalshi", EventKey: ek}, {Ticker: legB, Platform: "kalshi", EventKey: ek}}
	class = plabClass(legs, ek)
	s.plabMu.Lock()
	if c, ok := s.plabCorr[class]; ok && c != nil {
		rho, rhoN = plabRho(*c)
	}
	s.plabMu.Unlock()
	return
}

func (s *Server) xvWaitStatsLocked(ctx context.Context) map[string]float64 {
	if s.xvWaitStats == nil {
		s.xvWaitStats = map[string]float64{}
		if raw, ok := s.store.KVGet(ctx, xvWaitKVKey); ok && raw != "" {
			_ = json.Unmarshal([]byte(raw), &s.xvWaitStats)
		}
	}
	return s.xvWaitStats
}

// xvWaitAppend writes one jsonl record (decision or outcome) to data\xvwait.jsonl (8MB rotate-to-.1).
func (s *Server) xvWaitAppend(rec map[string]any) {
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	s.xvWaitFileMu.Lock()
	defer s.xvWaitFileMu.Unlock()
	path := filepath.Join(s.cfg().DataDir, xvWaitFile)
	if st, err := os.Stat(path); err == nil && st.Size() > 8<<20 {
		_ = os.Rename(path, path+".1") // single archive generation is enough for a study log
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
	s.xvWaitMu.Lock()
	s.xvWaitRing = append(s.xvWaitRing, rec)
	if len(s.xvWaitRing) > xvWaitRingCap {
		s.xvWaitRing = s.xvWaitRing[len(s.xvWaitRing)-xvWaitRingCap:]
	}
	s.xvWaitMu.Unlock()
}

// xvWaitNote logs the wait-vs-now decision for one Kalshi-side gap candidate and starts tracking
// what actually happens. LOG-ONLY: nothing here places or delays any trade. Singles behavior is
// unchanged (Kalshi gap rows never executed; PUS rows execute immediately as always).
func (s *Server) xvWaitNote(ctx context.Context, kTicker, pusSlug string, flip bool, kPx, gapC float64, epN int) {
	cls, sport, ok := r126Classify(kTicker)
	if !ok {
		cls, sport = "other", "other"
	}
	partners := s.xvPartnerScan(kTicker)
	partnerNow := len(partners) > 0
	// λ per class from measured arrivals/exposure (starts 0 → pArrive 0 → decision "now": the
	// honest prior; reality fills these counters via the outcome sweep below).
	s.xvWaitMu.Lock()
	st := s.xvWaitStatsLocked(ctx)
	arr, expMin := st[cls+"|arrivals"], st[cls+"|exposure_min"]
	s.xvWaitMu.Unlock()
	lambdaHr := 0.0
	if expMin > 0 {
		lambdaHr = arr / (expMin / 60)
	}
	hl := s.xvEpHalfLifeMin(ctx, cls)
	rho, rhoN, rhoClass := 0.0, 0, ""
	p2 := 0.0
	if partnerNow {
		rho, rhoN, rhoClass = s.xvPairRho(kTicker, partners[0].Ticker)
		p2 = partners[0].Mid
	} else if len(partners) == 0 {
		// no live partner: bonus prices the MOST LIKELY future partner class at p2=0.5 (max
		// variance — an upper bound on the bonus, documented; still gated by measured ρ)
		p2 = 0.5
	}
	pArr, bonusC, decayC, eWaitC := xvWaitCalc(lambdaHr, hl, rho, kPx, p2, gapC)
	decision := "now"
	if partnerNow {
		decision = "combo-now" // Part 4.3 territory — no waiting needed
	} else if eWaitC > 0 {
		decision = "wait"
	}
	rec := map[string]any{
		"type": "decision", "ts": time.Now().UTC().Format(time.RFC3339), "ticker": kTicker,
		"pus_slug": pusSlug, "class": cls, "sport": sport, "episode": epN, "gap_c": math.Round(gapC*100) / 100,
		"lambda_hr": math.Round(lambdaHr*1000) / 1000, "hl_min": math.Round(hl*10) / 10,
		"p_arrive": math.Round(pArr*1000) / 1000, "rho": math.Round(rho*1000) / 1000, "rho_n": rhoN,
		"rho_class": rhoClass, "bonus_c": math.Round(bonusC*100) / 100, "decay_c": math.Round(decayC*100) / 100,
		"e_wait_c": math.Round(eWaitC*100) / 100, "decision": decision, "partner_now": partnerNow,
	}
	if partnerNow {
		rec["partner"] = partners[0].Ticker
	}
	s.xvWaitAppend(rec)
	s.xvWaitMu.Lock()
	st = s.xvWaitStatsLocked(ctx)
	st["decisions"]++
	st[decision+"_n"]++
	s.xvWaitDirty = true
	if s.xvWaitPend == nil {
		s.xvWaitPend = map[string]*xvWaitPending{}
	}
	if _, dup := s.xvWaitPend[kTicker]; !dup && !partnerNow && len(s.xvWaitPend) < xvWaitMaxPend {
		dl := time.Now().Add(time.Duration(math.Min(60, 2*hl)) * time.Minute)
		s.xvWaitPend[kTicker] = &xvWaitPending{TS: time.Now(), Ticker: kTicker, PusSlug: pusSlug,
			Flip: flip, Class: cls, Sport: sport, GapC: gapC, Deadline: dl}
	}
	s.xvWaitMu.Unlock()
}

// xvWaitSweep advances the pending decisions each gap-scan pass: notes partner arrivals (feeding
// the λ counters) and finalizes outcomes at deadline (did the gap survive?).
func (s *Server) xvWaitSweep(ctx context.Context, mkts []polyUSMarket) {
	s.xvWaitMu.Lock()
	if len(s.xvWaitPend) == 0 {
		s.xvWaitMu.Unlock()
		return
	}
	pend := make([]*xvWaitPending, 0, len(s.xvWaitPend))
	for _, p := range s.xvWaitPend {
		pend = append(pend, p)
	}
	s.xvWaitMu.Unlock()
	pusYes := map[string]float64{}
	for _, m := range mkts {
		pusYes[m.Slug] = m.Yes
	}
	now := time.Now()
	for _, p := range pend {
		if p.PartnerAt == 0 {
			if partners := s.xvPartnerScan(p.Ticker); len(partners) > 0 {
				p.PartnerAt, p.PartnerTkr = now.Unix(), partners[0].Ticker
				s.xvWaitMu.Lock()
				st := s.xvWaitStatsLocked(ctx)
				st[p.Class+"|arrivals"]++
				s.xvWaitDirty = true
				s.xvWaitMu.Unlock()
			}
		}
		if now.Before(p.Deadline) {
			continue
		}
		// finalize: current gap (same unified space as the detector)
		gapNow, gapKnown := 0.0, false
		if kYes, ok := s.xvKalMidFresh(p.Ticker); ok {
			if py, ok2 := pusYes[p.PusSlug]; ok2 && py > 0 && py < 1 {
				pusK := py
				if p.Flip {
					pusK = 1 - py
				}
				gapNow, gapKnown = kYes-pusK, true
			}
		}
		minGap := s.cfg().Auto.XVLagMinGapCents
		if minGap <= 0 {
			minGap = 3
		}
		out := map[string]any{
			"type": "outcome", "ts": now.UTC().Format(time.RFC3339), "ticker": p.Ticker,
			"class": p.Class, "gap_entry_c": math.Round(p.GapC*100) / 100,
			"partner_arrived": p.PartnerAt > 0, "waited_min": math.Round(now.Sub(p.TS).Minutes()*10) / 10,
		}
		if p.PartnerAt > 0 {
			out["partner"] = p.PartnerTkr
			out["minutes_to_partner"] = math.Round(time.Unix(p.PartnerAt, 0).Sub(p.TS).Minutes()*10) / 10
		}
		if gapKnown {
			out["gap_end_c"] = math.Round(math.Abs(gapNow)*10000) / 100
			out["gap_survived"] = math.Abs(gapNow)*100 >= minGap
		} else {
			out["gap_end_c"] = nil
		}
		s.xvWaitAppend(out)
		s.xvWaitMu.Lock()
		st := s.xvWaitStatsLocked(ctx)
		st[p.Class+"|exposure_min"] += now.Sub(p.TS).Minutes()
		st["outcomes"]++
		if p.PartnerAt > 0 {
			st["outcomes_partner"]++
		}
		if gapKnown && math.Abs(gapNow)*100 >= minGap {
			st["outcomes_gap_survived"]++
		}
		s.xvWaitDirty = true
		delete(s.xvWaitPend, p.Ticker)
		s.xvWaitMu.Unlock()
	}
}

func (s *Server) xvWaitFlush(ctx context.Context) {
	s.xvWaitMu.Lock()
	if !s.xvWaitDirty || s.xvWaitStats == nil {
		s.xvWaitMu.Unlock()
		return
	}
	b, err := json.Marshal(s.xvWaitStats)
	s.xvWaitDirty = false
	s.xvWaitMu.Unlock()
	if err == nil {
		_ = s.store.KVSet(ctx, xvWaitKVKey, string(b))
	}
}

// ---------------------------------------------------------------------------------------------
// Part 4.3 — combo overlay (paper ledger)
// ---------------------------------------------------------------------------------------------

const (
	xvcFile    = "xvcombo_book.json"
	xvcBank    = 50.0 // the product-sim STUDY ledger's fixed notional (log-only research — R127: the money lots ride the Combos book instead)
	xvcOpenCap = 40
)

// R127 COMBOS BOOK expression tags (operator item 5): the combo overlay becomes the Combos book
// ($600 since R143). Every lot carries an HONEST expression tag:
//   - "synthetic"  — leg-stack: two real single-market paper legs at their REAL asks, graded
//     together as one combo unit (cost = SUM of leg costs; payout = SUM of leg payouts; settles
//     when both resolve). Venue-legal per the R126 probe findings — THE money expression.
//   - "rfq"        — an actual combined market with a live venue quote (R126 found ZERO resting
//     quotes; the code path + tag stay, expect n≈0).
//   - "product-sim" (and legacy "" rows) — the R126 naive leg-product cost variant, continuing as
//     the log-only study family it already is (never spends the Combos bank).
//
// Verdict families: product-sim → combo-overlay (unchanged) · synthetic → combo-synth · rfq →
// combo-rfq. Entry logic is R126's (half-Kelly stakes, measured ρ≥0.05 gate, hedge guards) —
// only the accounting expression + tags changed.
const (
	xvcExprProduct = "product-sim"
	xvcExprSynth   = "synthetic"
	xvcExprRFQ     = "rfq"
)

type xvcPos struct {
	TS        string  `json:"ts"`
	LegA      string  `json:"leg_a"` // the Kalshi gap leg
	SideA     string  `json:"side_a"`
	PxA       float64 `json:"px_a"`
	LegB      string  `json:"leg_b"` // the correlated same-game partner
	SideB     string  `json:"side_b"`
	PxB       float64 `json:"px_b"`
	Cost      float64 `json:"cost"` // product-sim: naive leg-product · synthetic: askA+askB · rfq: the live venue quote
	Contracts float64 `json:"contracts"`
	Fee       float64 `json:"fee"`
	// FeeKnown requires a persisted venue schedule/receipt, not merely deterministic arithmetic.
	// Existing product/synthetic/RFQ rows used a modeled leg-fee number and therefore remain false.
	FeeKnown  bool    `json:"fee_known,omitempty"`
	FeeSource string  `json:"fee_source,omitempty"`
	Rho       float64 `json:"rho"`
	RhoN      int     `json:"rho_n"`
	JointP    float64 `json:"joint_p"`
	EVc       float64 `json:"ev_c"`
	Episode   int     `json:"episode_n,omitempty"`
	Mode      string  `json:"mode"`           // "combo_overlay" tag for the verdict engine / graders
	Expr      string  `json:"expr,omitempty"` // R127: "synthetic" | "rfq" | "product-sim" ("" = legacy product-sim row)
	Src       string  `json:"src,omitempty"`  // R128: originating book — "xvgap" | "auto" (Kalshi book) | "ml" ("" = legacy xvgap)
}

type xvcClosed struct {
	xvcPos
	Payout    float64 `json:"payout"`
	PnL       float64 `json:"pnl"`
	Won       bool    `json:"won"`
	SettledTS string  `json:"settled_ts"`
}

type xvcBook struct {
	Open         []xvcPos    `json:"open"`
	ArchivedOpen []xvcPos    `json:"archived_open,omitempty"`
	Closed       []xvcClosed `json:"closed"`
	Net          float64     `json:"net"`
	Wins         int         `json:"wins"`
	Losses       int         `json:"losses"`
	ResetEpoch   string      `json:"reset_epoch,omitempty"`
	NetBase      float64     `json:"net_base,omitempty"`
	WinsBase     int         `json:"wins_base,omitempty"`
	LossesBase   int         `json:"losses_base,omitempty"`
}

func xvcCurrentEpochStats(b *xvcBook) (net float64, wins, losses int) {
	if b == nil {
		return 0, 0, 0
	}
	net, wins, losses = b.Net-b.NetBase, b.Wins-b.WinsBase, b.Losses-b.LossesBase
	if wins < 0 {
		wins = 0
	}
	if losses < 0 {
		losses = 0
	}
	return net, wins, losses
}

func (s *Server) xvcLoadLocked() *xvcBook {
	if s.xvcBk != nil {
		return s.xvcBk
	}
	b := &xvcBook{}
	_ = s.readJSONLoose(filepath.Join(s.cfg().DataDir, xvcFile), b)
	s.xvcBk = b
	return b
}

func (s *Server) xvcFlush() {
	defer s.invalidatePortfolioEquityCache()
	s.xvcMu.Lock()
	defer s.xvcMu.Unlock()
	if !s.xvcDirty || s.xvcBk == nil {
		return
	}
	out, err := json.Marshal(s.xvcBk)
	if err != nil {
		return
	}
	path := filepath.Join(s.cfg().DataDir, xvcFile)
	tmp := path + ".tmp"
	if os.WriteFile(tmp, out, 0o644) == nil {
		if os.Rename(tmp, path) == nil {
			s.xvcDirty = false
		}
	}
}

// xvComboTry — Part 4.3 SPLIT variant: a Kalshi gap candidate with an ALREADY-LIVE comboable
// partner records a correlated 2-leg opportunity (config xvgap_combo_overlay; tag combo_overlay).
// Pricing assumption (under test by Part 3): the venue would charge the NAIVE LEG-PRODUCT of the
// two asks; our edge = correlation-adjusted joint − product − fees. Kelly-aware HALF stakes
// (higher variance than singles). Partner choice prefers high measured ρ and (Part 5) low-price
// bands: score = ρ · (1 + roiBandWeight). Requires measured ρ ≥ 0.05 — no invented correlations.
//
// R128 (operator Part 2: "COMBOS as an expression for ML + PolyUS + Kalshi books"): src tags the
// ORIGINATING book ("xvgap" — the R126 caller; "auto" — a Kalshi-book shared-auto candidate;
// "ml" — an ML-book post). Any book's Kalshi candidate with a live comboable partner that raises
// modeled EV reaches this observer. R166 stops every new product-sim/synthetic/RFQ P&L row until
// delayed newer executable verification exists. PolyUS-leg partner scan does not exist yet.
func (s *Server) xvComboTry(ctx context.Context, src, kTicker, kSide string, kPx, gapC float64, epN int) {
	if !mlFundedRouteAllowed(src, s.mlExecutionAuthorized) {
		return // an ML-origin combo is funded ML authority; strategy-origin combos stay independent
	}
	if !s.cfg().Auto.XvgapComboOverlay || s.ksBlocked() {
		return
	}
	// R128 timeout-inventory fix: the Combos book had NO stale-feed placement gate — fail closed
	// on provably-dead Kalshi feeds exactly like the venue books (self-heals with the watchdogs).
	if s.pxOfflineReason("kalshi", kTicker) != "" {
		return
	}
	partners := s.xvPartnerScan(kTicker)
	if len(partners) == 0 {
		return
	}
	bestI, bestScore, bestRho, bestN := -1, 0.0, 0.0, 0
	for i, p := range partners {
		rho, rhoN, _ := s.xvPairRho(kTicker, p.Ticker)
		if rho < 0.05 {
			continue
		}
		score := rho * (1 + s.roiBandWeight("kalshi", p.Mid))
		if score > bestScore {
			bestI, bestScore, bestRho, bestN = i, score, rho, rhoN
		}
	}
	if bestI < 0 {
		return
	}
	pb := partners[bestI]
	// R127: the Combos VENUE BOOK funds the money expression (synthetic/rfq lots) — available
	// computed OUTSIDE xvcMu (bookOpenExposure takes it). The product-sim study keeps its own
	// small fixed notional (xvcBank) and never spends the Combos bank.
	venueAvail := s.bookAvailableUSD(ctx, vbCombos)
	s.xvcMu.Lock()
	b := s.xvcLoadLocked()
	if len(b.Open) >= xvcOpenCap {
		s.xvcMu.Unlock()
		return
	}
	hasProduct, hasMoney := false, false
	heldStudy := 0.0
	for _, op := range b.Open {
		money := op.Expr == xvcExprSynth || op.Expr == xvcExprRFQ
		if op.LegA == kTicker { // one open overlay per gap leg, PER EXPRESSION CLASS
			if money {
				hasMoney = true
			} else {
				hasProduct = true
			}
		}
		if !money {
			heldStudy += op.Cost*op.Contracts + op.Fee
		}
	}
	studyEquity := xvcBank + b.Net // study cash bound (documented: Net includes all expressions)
	s.xvcMu.Unlock()
	if hasProduct && hasMoney {
		return
	}
	askA, depthA, _, okA := s.executableAsk(ctx, "kalshi", kTicker, kSide, true)
	if !okA || depthA < 1 || askA > kPx+0.03 {
		return // moved off the signal — never chase
	}
	askB, depthB, _, okB := s.executableAsk(ctx, "kalshi", pb.Ticker, "YES", true)
	if !okB || depthB < 1 {
		return
	}
	cost := askA * askB
	if cost <= 0.01 || cost >= 0.97 {
		return
	}
	// correlation-adjusted joint: leg A priced at OUR fair (ask + the measured gap edge —
	// the PUS-referenced view that makes this leg a candidate at all), leg B at the venue mid.
	ek := s.legEventKey("kalshi", kTicker, "")
	pwinA, pwinB := math.Min(0.97, askA+gapC), pb.Mid
	legs := []plabLeg{
		{Ticker: kTicker, Platform: "kalshi", Side: kSide, Price: askA, PWin: pwinA, EventKey: ek},
		{Ticker: pb.Ticker, Platform: "kalshi", Side: "YES", Price: askB, PWin: pwinB, EventKey: ek},
	}
	s.plabMu.Lock()
	jp, _, _ := s.plabJointPEx(legs, [][]int{{0, 1}})
	s.plabMu.Unlock()
	fee := s.blendedFee("kalshi", kTicker, "", false, 1, askA) + s.blendedFee("kalshi", pb.Ticker, "", false, 1, askB)
	evC := (jp - cost - fee) * 100
	if evC <= 0 {
		return // no modeled edge after fees — log nothing, place nothing (R126 entry logic, unchanged)
	}
	if !s.r166CanBookVerifiedMultiLegPaper() {
		_ = s.store.Audit(ctx, "info", "xvcombo", fmt.Sprintf(
			"combo_overlay OBSERVED [no Paper P&L] %s %s @%.0f¢ + %s YES @%.0f¢ (product %.1f¢, joint %.1f¢, EV %+.1f¢) ep%d",
			kTicker, kSide, askA*100, pb.Ticker, askB*100, cost*100, jp*100, evC, epN),
			r166UnverifiedMultiLegPaperReason)
		return
	}
	stake := 0.5 * s.engineStake(ctx, "kalshi", kTicker, kSide, "combo-overlay", cost, 0, math.Max(studyEquity, 1), false, "realized-only")
	now := time.Now().UTC().Format(time.RFC3339)
	// 1) PRODUCT-SIM study row (the R126 naive leg-product variant) — log-only family, its own
	//    small notional, graded under combo-overlay exactly as before.
	contracts := math.Floor(stake / cost)
	if contracts < 1 {
		contracts = 1
	}
	if !hasProduct && studyEquity > 0 && heldStudy+cost*contracts+fee*contracts <= studyEquity {
		pos := xvcPos{TS: now, LegA: kTicker, SideA: kSide, PxA: askA, LegB: pb.Ticker, SideB: "YES",
			PxB: askB, Cost: math.Round(cost*10000) / 10000, Contracts: contracts,
			Fee: math.Round(fee*contracts*10000) / 10000, Rho: math.Round(bestRho*1000) / 1000, RhoN: bestN,
			JointP: math.Round(jp*10000) / 10000, EVc: math.Round(evC*100) / 100, Episode: epN,
			Mode: "combo_overlay", Expr: xvcExprProduct, Src: src}
		s.xvcMu.Lock()
		b = s.xvcLoadLocked()
		b.Open = append(b.Open, pos)
		s.xvcDirty = true
		s.xvcMu.Unlock()
		_ = s.store.Audit(ctx, "info", "xvcombo",
			fmt.Sprintf("combo_overlay OPEN [product-sim study] %s %s @%.0f¢ + %s YES @%.0f¢ ×%.0f (cost %.1f¢/ct, ρ=%.2f n=%d, joint %.1f¢, EV %+.1f¢) ep%d — PAPER",
				kTicker, kSide, askA*100, pb.Ticker, askB*100, contracts, cost*100, bestRho, bestN, jp*100, evC, epN), "")
	}
	// 2) MONEY expression, funded by the COMBOS PORTFOLIO ($600): an actual combined market with a
	//    live quote when one exists. R132 removes the synthetic fallback: simultaneous singles
	//    are not a parlay, and a same-time event cannot be sequentially rolled.
	if !hasMoney && venueAvail > 0 {
		exprTag, costM, expPay := xvcExprRFQ, 0.0, jp
		if q, okQ := s.xvcVenueQuote(kTicker, pb.Ticker); okQ && q > 0 && q < 1 {
			costM = q
		}
		if costM > 0.02 && costM < 1.98 {
			// Half-Kelly on the Combos book's bank (R126's stake rule, R127's money).
			stakeM := 0.5 * s.engineStake(ctx, "kalshi", kTicker, kSide, "combo-overlay", cost, 0, s.bookSizingEquityUSD(ctx, vbCombos), false, "realized-only")
			contractsM := math.Floor(stakeM / costM)
			if contractsM < 1 {
				contractsM = 1
			}
			evMC := (expPay - costM - fee) * 100
			if evMC > 0 && costM*contractsM+fee*contractsM <= venueAvail {
				posM := xvcPos{TS: now, LegA: kTicker, SideA: kSide, PxA: askA, LegB: pb.Ticker, SideB: "YES",
					PxB: askB, Cost: math.Round(costM*10000) / 10000, Contracts: contractsM,
					Fee: math.Round(fee*contractsM*10000) / 10000, Rho: math.Round(bestRho*1000) / 1000, RhoN: bestN,
					JointP: math.Round(jp*10000) / 10000, EVc: math.Round(evMC*100) / 100, Episode: epN,
					Mode: "combo_overlay", Expr: exprTag, Src: src}
				s.xvcMu.Lock()
				b = s.xvcLoadLocked()
				if len(b.Open) < xvcOpenCap {
					b.Open = append(b.Open, posM)
					s.xvcDirty = true
				}
				s.xvcMu.Unlock()
				_ = s.store.Audit(ctx, "info", "xvcombo",
					fmt.Sprintf("Combos book OPEN [%s] %s %s @%.0f¢ + %s YES @%.0f¢ ×%.0f (unit cost %.1f¢, ρ=%.2f n=%d, exp payout %.1f¢, EV %+.1f¢) ep%d — PAPER",
						exprTag, kTicker, kSide, askA*100, pb.Ticker, askB*100, contractsM, costM*100, bestRho, bestN, expPay*100, evMC, epN), "")
			}
		}
	}
	s.xvcFlush()
}

// xvcVenueQuote — the "rfq" expression's quote source: a live two-sided quote on an ACTUAL
// combined (MVE) market covering both legs. The R126 Part-3 probe measured ZERO resting quotes
// across its sampled universe, so this returns not-found until a persistent quote source exists
// (the budgeted sampler in r126combo.go is the discovery instrument — wiring its finds through a
// cache here is the follow-up once it ever reports live quotes). Keeping the path + tag is the
// operator's order; n≈0 expected.
func (s *Server) xvcVenueQuote(legA, legB string) (float64, bool) {
	return 0, false
}

// settleXvComboBook grades overlays when BOTH legs have settled (each leg from the venue's own
// resolution). Scalar-safe: payout = payoutA × payoutB (binary sports ⇒ the AND of the legs).
func (s *Server) settleXvComboBook(ctx context.Context) {
	s.xvcMu.Lock()
	b := s.xvcLoadLocked()
	if len(b.Open) == 0 && len(b.ArchivedOpen) == 0 {
		s.xvcMu.Unlock()
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	changed := false
	var notes []string
	settle := func(rows []xvcPos, archived bool) []xvcPos {
		still := rows[:0:0]
		for _, p := range rows {
			yvA, resA := s.store.ResolvedYesForVenue(ctx, "kalshi", p.LegA)
			yvB, resB := s.store.ResolvedYesForVenue(ctx, "kalshi", p.LegB)
			if !resA || !resB {
				still = append(still, p)
				continue
			}
			payA, payB := yvA, yvB
			if strings.EqualFold(p.SideA, "NO") {
				payA = 1 - yvA
			}
			if strings.EqualFold(p.SideB, "NO") {
				payB = 1 - yvB
			}
			// R127 expression-aware grading: product-sim/rfq are all-or-nothing (payout = AND of
			// the legs); a SYNTHETIC leg-stack is two singles graded as one unit (payout = SUM).
			payout := payA * payB
			pnl := 0.0
			won := false
			if p.Expr == xvcExprSynth {
				payout = payA + payB
				pnl = p.Contracts*(payout-p.Cost) - p.Fee
				won = pnl > 0
			} else {
				pnl = p.Contracts*(payout-p.Cost) - p.Fee
				won = payout >= 0.5
				if p.Expr == xvcExprRFQ {
					won = pnl > 0
				}
			}
			b.Net += pnl
			if won {
				b.Wins++
			} else {
				b.Losses++
			}
			// A reset-archived lot still settles into lifetime evidence, while its result advances
			// the baseline equally so it cannot poison the new current portfolio.
			if archived {
				b.NetBase += pnl
				if won {
					b.WinsBase++
				} else {
					b.LossesBase++
				}
			}
			b.Closed = append(b.Closed, xvcClosed{xvcPos: p, Payout: payout,
				PnL: math.Round(pnl*100) / 100, Won: won, SettledTS: now})
			note := fmt.Sprintf("%s+%s settled %+0.2f", p.LegA, p.LegB, pnl)
			if archived {
				note += " (pre-reset history)"
			}
			notes = append(notes, note)
			changed = true
		}
		return still
	}
	b.Open = settle(b.Open, false)
	b.ArchivedOpen = settle(b.ArchivedOpen, true)
	if changed {
		if len(b.Closed) > 300 {
			b.Closed = b.Closed[len(b.Closed)-300:]
		}
		s.xvcDirty = true
	}
	s.xvcMu.Unlock()
	for _, n := range notes {
		_ = s.store.Audit(ctx, "info", "xvcombo", "combo_overlay CLOSE "+n, "")
	}
	s.xvcFlush()
}

// ---------------------------------------------------------------------------------------------
// Part 5 — ROI-band ranking weights (from tools\r126_bands.py → data\roi_bands.json)
// ---------------------------------------------------------------------------------------------

type roiBandEnt struct{ lo, hi, w float64 }

// roiBandsLocked lazily loads/refreshes (6h) the preferred-band table. The polymarket "10-15"
// band is EXCLUDED here by name: the R126 band study measured it as a 3-ticker concentration
// artifact (top-3 tickers = 114% of band profit) that met the mechanical lo>0 rule — honesty
// beats the rule. Everything else loads as measured.
func (s *Server) roiBandsLocked() map[string][]roiBandEnt {
	if s.roiBands != nil && time.Since(s.roiLoadAt) < 6*time.Hour {
		return s.roiBands
	}
	s.roiLoadAt = time.Now()
	if s.roiBands == nil {
		s.roiBands = map[string][]roiBandEnt{}
	}
	var blob struct {
		Preferred map[string][][2]any `json:"preferred_bands"`
	}
	if !s.readJSONLoose(filepath.Join(s.cfg().DataDir, "roi_bands.json"), &blob) || blob.Preferred == nil {
		return s.roiBands // keep last-good (or empty) on missing/poisoned file
	}
	next := map[string][]roiBandEnt{}
	for venue, rows := range blob.Preferred {
		for _, r := range rows {
			name, _ := r[0].(string)
			w, _ := r[1].(float64)
			if venue == "polymarket" && name == "10-15" {
				continue // measured artifact (see header) — never a ranking input
			}
			var lo, hi float64
			if _, err := fmt.Sscanf(name, "%f-%f", &lo, &hi); err != nil || w <= 0 {
				continue
			}
			next[venue] = append(next[venue], roiBandEnt{lo: lo, hi: hi, w: w})
		}
	}
	s.roiBands = next
	return s.roiBands
}

// roiBandWeight — the per-venue ROI-per-dollar preference weight for an entry price (0..1 fraction).
// 0 = no measured preference. LOG-ONLY influence unless roi_band_rank_arm (Part 5).
func (s *Server) roiBandWeight(venue string, px float64) float64 {
	c := px * 100
	s.roiMu.Lock()
	defer s.roiMu.Unlock()
	for _, e := range s.roiBandsLocked()[strings.ToLower(venue)] {
		if c >= e.lo && c < e.hi {
			return e.w
		}
	}
	return 0
}

// evDayRankMult — R129 C4 (operator: MED-HIGH): the EV-per-capital-day ranking multiplier.
// mult = (6h / max(lh, 0.5h))^exp — 6h is the proposal pipeline's own horizon ceiling, so an
// at-the-limit market multiplies by exactly 1 and everything sooner ranks up; the 0.5h floor
// stops near-expiry candidates from swamping the board (the <3-min physics gate still owns the
// true near-expiry edge). exp: 0 = off · 0.65 = med-high (1h ≈ 3.2×, 2h ≈ 2.0×, 3h ≈ 1.6×,
// 6h = 1×) · 1.0 would be raw EV/day. Clamped [0.25, 8] as a documented sanity rail (in-gate
// horizons keep it ≥1 in practice — the clamp only matters if the 6h gate ever widens).
func evDayRankMult(lh, exp float64) float64 {
	if exp <= 0 || lh <= 0 {
		return 1
	}
	return clampF(math.Pow(6.0/math.Max(lh, 0.5), exp), 0.25, 8)
}

// ---------------------------------------------------------------------------------------------
// /api/xvboth — the R126 verification surface
// ---------------------------------------------------------------------------------------------

func (s *Server) handleXvBoth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	day := time.Now().UTC().Format("2006-01-02")
	out := map[string]any{}
	s.xvEpMu.Lock()
	st := s.xvEpLoadLocked(ctx)
	active, reent := 0, 0
	for _, e := range st.Eps {
		if e.Active {
			active++
		}
		if e.N >= 2 {
			reent++
		}
	}
	durs := map[string]any{}
	for cls := range st.Dur {
		durs[cls] = nil
	}
	out["episodes"] = map[string]any{
		"pairs_tracked": len(st.Eps), "active_now": active, "pairs_with_reentry": reent,
		"opps_by_day": st.Opp, "duration_classes": len(st.Dur),
	}
	s.xvEpMu.Unlock()
	for cls := range durs {
		durs[cls] = s.xvEpHalfLifeMin(ctx, cls)
	}
	out["gap_halflife_min"] = durs
	s.xvWaitMu.Lock()
	stats := map[string]float64{}
	for k, v := range s.xvWaitStatsLocked(ctx) {
		stats[k] = math.Round(v*100) / 100
	}
	out["wait"] = map[string]any{
		"pending": len(s.xvWaitPend), "stats": stats,
		"recent": append([]map[string]any(nil), s.xvWaitRing...),
	}
	s.xvWaitMu.Unlock()
	s.xvcMu.Lock()
	cb := s.xvcLoadLocked()
	exprN := map[string]int{}
	for _, p := range cb.Open {
		e := p.Expr
		if e == "" {
			e = xvcExprProduct
		}
		exprN[e]++
	}
	out["combo_overlay"] = map[string]any{
		"enabled": s.cfg().Auto.XvgapComboOverlay, "open": len(cb.Open), "closed": len(cb.Closed),
		"net": math.Round(cb.Net*100) / 100, "wins": cb.Wins, "losses": cb.Losses, "open_lots": cb.Open,
		"open_by_expr": exprN,
		"r133":         "R143: Combos portfolio ($600) funds the synthetic leg-stack (+ rare rfq) lots; product-sim rows continue as the log-only study family",
	}
	s.xvcMu.Unlock()
	s.roiMu.Lock()
	bands := map[string]int{}
	for v, es := range s.roiBandsLocked() {
		bands[v] = len(es)
	}
	s.roiMu.Unlock()
	out["roi_bands"] = map[string]any{"venues": bands, "armed": s.cfg().Auto.ROIBandRankArm}
	out["day"] = day
	writeJSON(w, http.StatusOK, out)
}
