// scoreweight.go — scoreboard-driven allocation engine.
//
//	NO pausing · NO limiting · NO balance limiting · ALWAYS INVERT trash.
//
// Each venue book allocates across its roster strategies PROPORTIONAL to positive lifetime
// realized ¢/unit share — the SAME numbers the scoreboard shows (verdict engine + book ledgers):
//
//	weight_i = max(0, cents_i) / Σ_j max(0, cents_j)
//
// R158 keeps that rule for static legacy books and for the generic follower's total venue slice,
// then splits only that follower slice by the smaller of the Systems leaderboard's prior-UTC-day
// contract and dependence-aware event/day fee-net profit-rate lower bounds. Unproven routes retain
// a 10% exploration sleeve.
//
// Rules of the pool (documented plainly because the operator reads this):
//   - MEASURED-POSITIVE strategies split 90% of the book proportional to their ¢/unit.
//   - COLLECTING strategies (no realized data yet) split a 10% EXPLORATION pool equally — new
//     strategies start small immediately; uncertainty shrinks stakes, it never gates entry.
//     (No positives at all ⇒ the collecting set takes the whole pool.)
//   - MEASURED-NEGATIVE strategies get weight 0 — they never stop LOGGING, and where an inverted
//     expression exists it enters the roster instead (placement policy flips signal families at
//     bet time — placepolicy.go; the scoreboard shows every 🔄 twin with its drag-adjusted edge).
//
// Stake chain per bet (engineStake → signalKellyFrac), in plain words:
//
//		stake = current portfolio equity × Kelly(edge/odds) × variance-shrink × scoreboard-weight multiplier
//
//	  - Kelly: current equity × edge/(1−price) — the R103 engine, unchanged;
//	  - variance-shrink = m²/(m²+SE²), m = the family's lifetime mean edge, SE from its
//	    always-valid CS radius (≈ radius/2). Shrinks toward 0 ONLY for uncertainty in the MEAN —
//	    this REPLACES the old 0.4→1.0 n-ramp (operator: "never a raw n gate");
//	  - scoreboard-weight multiplier: mean-normalized within the measured-positive roster
//	    (an average positive member is ×1 — the engine REDISTRIBUTES flow, it doesn't shrink it);
//	  - sub-strategy books (freshinv/xvgap/favlong80): open-exposure share = weight × book bank,
//	    replacing the fixed $100 promotion share cap.
//
// The table refreshes with the verdict cache (~60s, rebuilt by the sweeps); placement-path reads
// are CACHE-ONLY (never a cold compute inside a bet).
package server

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/paper"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// swPointDefeated only lets authenticated profit evidence demote a Paper exploration route. Old
// Paper/model/assumed-fill numbers are hypotheses: neither their sign nor their statistical state
// may concentrate or starve the corrected one-share exploration portfolio.
func swPointDefeated(e swEntry) bool {
	return e.ProfitEvidence && (e.Cents <= 0 || e.State == "PROVEN-" || e.State == "FUTILE")
}

const (
	swExploreFrac = 0.10             // exploration pool for collecting roster members
	swTTL         = time.Minute      // table cadence (rides the verdict cache)
	swStale       = 10 * time.Minute // beyond this the cache is too old to size with — neutral
)

// swEntry is one roster line of a venue book's allocation table.
type swEntry struct {
	Family          string  `json:"family"` // roster key ("" = the shared auto executor's venue line)
	Plain           string  `json:"plain"`
	Cents           float64 `json:"cents_per_unit"` // lifetime realized (the scoreboard number)
	N               int     `json:"n"`
	State           string  `json:"state,omitempty"` // verdict state; COLLECTING keeps exploration alive
	Weight          float64 `json:"weight"`
	Explore         bool    `json:"explore,omitempty"` // collecting — funded from the exploration pool
	AllocationScore float64 `json:"allocation_score,omitempty"`
	AllocationN     int     `json:"allocation_n,omitempty"`
	AllocationReady bool    `json:"allocation_ready,omitempty"`
	ProfitEvidence  bool    `json:"profit_evidence"`
}

// swState is the cached engine output.
type swState struct {
	Books             map[string][]swEntry // book → roster with weights (display + sub shares)
	AutoMult          map[string]float64   // signal family → mean-normalized stake multiplier
	SubShare          map[string]float64   // promo/roster family → open-exposure share USD
	AllocationAsOf    time.Time
	AllocationDay     string
	AllocationBuiltAt time.Time
	AllocationFrozen  bool
	At                time.Time
}

// swNormalize applies the pool rules to a roster in place.
func swNormalize(ents []swEntry) {
	posSum := 0.0
	nExplore := 0
	for i := range ents {
		ents[i].Weight, ents[i].Explore = 0, false
		e := ents[i]
		defeated := swPointDefeated(e)
		if e.ProfitEvidence && e.Cents > 0 && !defeated {
			posSum += e.Cents
		} else if !defeated {
			nExplore++ // no authenticated result: equal one-share exploration, whatever simulation says
		}
	}
	explorePool := 0.0
	if nExplore > 0 {
		explorePool = swExploreFrac
		if posSum <= 0 {
			explorePool = 1.0
		}
	}
	posPool := 1.0 - explorePool
	for i := range ents {
		defeated := swPointDefeated(ents[i])
		switch {
		case !defeated && (!ents[i].ProfitEvidence || ents[i].Cents <= 0):
			ents[i].Weight = explorePool / float64(nExplore)
			ents[i].Explore = true
		case !defeated && ents[i].Cents > 0 && posSum > 0:
			ents[i].Weight = posPool * ents[i].Cents / posSum
		default:
			ents[i].Weight = 0 // only decision-grade losers reach zero; logging/invert twins continue
		}
	}
}

// gfAllocationEvidence is the conservative, pre-decision score for one exact generic-follower
// route. Score is the smaller of the existing Systems leaderboard's contract confidence-lower-bound
// fee-net dollars per elapsed calendar day and its dependence-aware complete-UTC-day/event-cluster
// lower bound. Sample strength enters exactly once through the contract confidence sequence; the
// allocator deliberately does not multiply it by a second raw-n or cents/second term.
type gfAllocationEvidence struct {
	Score float64
	N     int
	Ready bool
}

// gfConservativeAllocationScores converts the already-built in-memory Systems snapshot into exact
// follower roster keys. It never reads SQLite and never runs on the placement path. A route must
// have the leaderboard's complete confidence-ready history and a positive conservative Net/day
// lower bound, at least twenty settled contracts, and at least one elapsed day before it may leave
// the exploration sleeve. Open current opportunities do not erase settled history.
func gfConservativeAllocationScores(stats []storage.UnitTrialLeaderboardStat) map[string]gfAllocationEvidence {
	out := make(map[string]gfAllocationEvidence)
	for _, row := range leaderboardBacktestRows(stats) {
		if !row.ProfitEvidence {
			continue
		}
		family := strings.ToLower(strings.TrimSpace(row.Family))
		platform := strings.ToLower(strings.TrimSpace(row.Platform))
		side := strings.ToUpper(strings.TrimSpace(row.Side))
		origin := strings.ToLower(strings.TrimSpace(row.OriginLayer))
		route := strings.ToLower(strings.TrimSpace(row.Route))
		if family == "" || (platform != vbKalshi && platform != vbPolyus) ||
			(side != "YES" && side != "NO") || origin != "model" || route != "taker" {
			continue
		}
		key := gfRosterKey(family, platform, side, route)
		contractScore := row.NetPerCalendarDayLo
		clusterScore := math.Inf(-1)
		if row.ClusterNetPerDayLo != nil {
			clusterScore = *row.ClusterNetPerDayLo
		}
		score := math.Min(contractScore, clusterScore)
		ready := row.SettledMarkets >= briefRankMinSettledContracts &&
			row.TrackedSeconds >= 24*60*60 && contractScore > 0 && clusterScore > 0 && score > 0 &&
			!math.IsNaN(contractScore) && !math.IsInf(contractScore, 0) &&
			!math.IsNaN(clusterScore) && !math.IsInf(clusterScore, 0) &&
			!math.IsNaN(score) && !math.IsInf(score, 0)
		ev := gfAllocationEvidence{N: row.SettledMarkets, Ready: ready}
		if ready {
			ev.Score = score
		}
		// Exact identities should be unique. If an old snapshot contains duplicates, keep the
		// stronger conservative row deterministically rather than summing duplicated evidence.
		if prior, ok := out[key]; !ok || ev.Score > prior.Score ||
			(ev.Score == prior.Score && ev.N > prior.N) {
			out[key] = ev
		}
	}
	return out
}

// swRedistributeGenericFollowers leaves every static legacy book row and the generic follower's
// total venue-book slice exactly where swNormalize put it, then adapts only the split inside that
// follower slice. Confidence-positive routes share 90% proportional to conservative Net/day;
// unproven/missing/zero-score routes split 10% equally. If none is confidence-positive, all
// follower capacity remains exploration so collection never stops.
func swRedistributeGenericFollowers(ents []swEntry, evidence map[string]gfAllocationEvidence) {
	followerPool := 0.0
	var followerIdx []int
	for i := range ents {
		if strings.HasPrefix(ents[i].Family, "gf:") {
			followerPool += ents[i].Weight
			followerIdx = append(followerIdx, i)
		}
	}
	if len(followerIdx) == 0 || followerPool <= 0 {
		return
	}
	scoreSum := 0.0
	explorers := 0
	for _, i := range followerIdx {
		ents[i].AllocationScore = 0
		ents[i].AllocationN = 0
		ents[i].AllocationReady = false
		ents[i].Explore = true
		if ev, ok := evidence[ents[i].Family]; ok {
			ents[i].AllocationScore = ev.Score
			ents[i].AllocationN = ev.N
			ents[i].AllocationReady = ev.Ready
		}
		if ents[i].AllocationReady && ents[i].AllocationScore > 0 {
			scoreSum += ents[i].AllocationScore
			ents[i].Explore = false
		} else {
			explorers++
		}
	}
	if scoreSum <= 0 {
		equal := followerPool / float64(len(followerIdx))
		for _, i := range followerIdx {
			ents[i].Weight = equal
			ents[i].Explore = true
		}
		return
	}
	explorePool := 0.0
	if explorers > 0 {
		explorePool = followerPool * swExploreFrac
	}
	provenPool := followerPool - explorePool
	for _, i := range followerIdx {
		if ents[i].AllocationReady && ents[i].AllocationScore > 0 {
			ents[i].Weight = provenPool * ents[i].AllocationScore / scoreSum
			continue
		}
		ents[i].Weight = explorePool / float64(explorers)
	}
}

// scoreWeights rebuilds (or serves) the allocation table. Safe to call from sweeps and API
// handlers; placement paths use the cache-only readers below.
func (s *Server) scoreWeights(ctx context.Context) *swState {
	s.swMu.Lock()
	if s.swTable != nil && time.Since(s.swTable.At) < swTTL {
		t := s.swTable
		s.swMu.Unlock()
		return t
	}
	s.swMu.Unlock()

	verds := s.computeExperimentVerdicts(ctx)
	vmap := map[string]verdictEnt{}
	for _, v := range verds {
		vmap[v.Family] = v
	}
	bcpc := s.briefBookCentsPerContract(ctx)
	// shared-auto venue lines: lifetime realized ¢/ct from closed auto rounds (bookRosterLines math)
	type autoAgg struct {
		pnl, cts float64
		n        int
	}
	auto := map[string]*autoAgg{"kalshi": {}, "polyus": {}}
	if fills, _, _, err := s.paperFillsCached(ctx); err == nil {
		for _, t := range paper.ClosedTrades(fills) {
			a := auto[t.Platform]
			if a == nil {
				continue
			}
			ctr := t.Contracts
			if ctr < 1 {
				ctr = 1
			}
			a.pnl += t.Realized - t.Fees
			a.cts += ctr
			a.n++
		}
	}
	books := map[string][]swEntry{}
	for _, sub := range s.venueBookSubsAll(verds) { // R130: static mapping + automatic follower lines
		e := swEntry{Family: sub.Family, Plain: sub.Plain}
		if sub.Family == "" {
			key := sub.Book // vbKalshi/vbPolyus match the paper platform strings
			if a := auto[key]; a != nil && a.n > 0 && a.cts > 0 {
				e.Cents, e.N = a.pnl/a.cts*100, a.n
			}
		} else if c, ok := bcpc[sub.Family]; ok {
			e.Cents = c
			// This diagnostic currently validates copied LIVE linkage fields inside the Paper lot.
			// Until Phase 1 re-joins the immutable exchange ledger at read time, it is not allowed
			// to become profit evidence or change allocation away from equal exploration.
			e.ProfitEvidence = false
			if v, okV := vmap[sub.Family]; okV {
				e.N, e.State = v.N, v.State
			} else {
				e.N, e.State = 1, "COLLECTING" // ledger has closes but not decision-grade evidence
			}
		} else if strings.HasPrefix(sub.Family, "gf:") {
			e.Cents, e.N, e.State = 0, 0, gfPaperPointState
		} else if v, ok := vmap[sub.Family]; ok && v.N > 0 && verdictHasAuthenticatedProfitEvidence(v) {
			e.Cents, e.N, e.State = v.Mean*100, v.N, v.State
		} else if sub.SeedN > 0 {
			// R132 follower seed is familyÃ—venue truth, carried by the dynamic roster row.
			e.Cents, e.N, e.State = sub.SeedMean*100, sub.SeedN, sub.SeedState
		} else if v, ok := vmap[sub.Seed]; sub.Seed != "" && ok && v.N > 0 {
			// R130 follower seed: until the sub's own lots settle, its weight rides the underlying
			// family's scoreboard ¢ (the operator's ask: leaders join AT their measured share);
			// the moment its own ledger grades, the branch above takes over — self-correcting.
			e.Cents, e.N, e.State = v.Mean*100, v.N, v.State
		}
		books[sub.Book] = append(books[sub.Book], e)
	}
	// Only the prior-day frozen allocation snapshot may influence funded Paper. The current
	// research digest is intentionally ignored: if the heavy worker is paused or has not yet built
	// a snapshot, every generic follower route stays equal exploration.
	allocation := gfFrozenAllocationSnapshot{}
	allocationFrozen := false
	var gfEvidence map[string]gfAllocationEvidence
	for b := range books {
		swNormalize(books[b])
		swRedistributeGenericFollowers(books[b], gfEvidence)
	}
	// shared-auto family multipliers: the placeable signal-family roster, cents from the verdict
	// engine — policy-INVERTED families ride their drag-adjusted invert edge (the side actually
	// placed). Mean-normalized within the measured-positive set: average positive member ×1.
	type famCell struct {
		cents float64
		n     int
	}
	fams := map[string]famCell{}
	for _, pf := range policyFamilies {
		if pf.research {
			continue
		}
		fams[pf.fam] = famCell{}
	}
	autoMult := map[string]float64{}
	for f := range fams {
		autoMult[f] = 1
	}
	// sub-strategy open-exposure shares: weight × the venue book's bank.
	subShare := map[string]float64{}
	for b, ents := range books {
		// Dollar shares compound with this book only. Unreadable current truth yields zero and
		// therefore fails closed instead of silently restoring the configured starting grant.
		bank := s.bookSizingEquityUSD(ctx, b)
		for _, e := range ents {
			if e.Family != "" {
				subShare[e.Family] = e.Weight * bank
			}
		}
	}
	t := &swState{Books: books, AutoMult: autoMult, SubShare: subShare,
		AllocationAsOf: allocation.AsOf, AllocationDay: allocation.Day,
		AllocationBuiltAt: allocation.BuiltAt, AllocationFrozen: allocationFrozen, At: time.Now()}
	s.swMu.Lock()
	s.swTable = t
	s.swMu.Unlock()
	return t
}

// scoreWeightMult — CACHE-ONLY stake multiplier for a shared-auto source (engineStake's R128
// slot; replaces the R78 LB-EV table as the live multiplier — that table still builds for
// display). Neutral 1 when cold/stale/unmapped.
func (s *Server) scoreWeightMult(source, platform string) float64 {
	if platform != "kalshi" && platform != "polyus" {
		return 1
	}
	fam := policySourceFamily(source)
	if fam == "" {
		return 1
	}
	s.swMu.Lock()
	t := s.swTable
	s.swMu.Unlock()
	if t == nil || time.Since(t.At) > swStale {
		return 1
	}
	if m, ok := t.AutoMult[fam]; ok && m > 0 {
		return m
	}
	return 1
}

// subShareUSD combines the cached strategy weight with short-TTL current portfolio equity, so
// dollar shares compound between one-minute score-table rebuilds.
func (s *Server) subShareUSD(rosterFamily string) (float64, bool) {
	s.swMu.Lock()
	t := s.swTable
	s.swMu.Unlock()
	if t == nil || time.Since(t.At) > swStale {
		return 0, false
	}
	for book, rows := range t.Books {
		for _, row := range rows {
			if row.Family != rosterFamily {
				continue
			}
			return row.Weight * s.bookSizingEquityUSD(context.Background(), book), true
		}
	}
	// Compatibility for a restored/pre-R144 table (and focused fixtures) that persisted only the
	// already-dollarized share. Fresh production tables always carry Books and take the compounded
	// branch above.
	share, ok := t.SubShare[rosterFamily]
	return share, ok
}

// paperSystemRoutePoint returns the best positive prospective one-share result for the exact
// originating family + venue + side + taker route. It is a PAPER exploration authority only:
// callers must still re-read current book/depth/fee/horizon, and LIVE continues through the
// independently sealed lower-bound gate in liveautomirror.go.
func (s *Server) paperSystemRoutePoint(source, platform, side string) (float64, bool) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(source)), "auto-cons-") {
		return 0, false
	}
	fam := strings.ToLower(strings.TrimSpace(policySourceFamily(source)))
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	if fam == "" || (platform != "kalshi" && platform != "polyus") || (side != "YES" && side != "NO") {
		return 0, false
	}
	s.verdMu.Lock()
	cache, at := append([]verdictEnt(nil), s.verdCache...), s.verdAt
	s.verdMu.Unlock()
	if cache == nil || time.Since(at) > swStale {
		return 0, false
	}
	best := 0.0
	for _, v := range cache {
		cellFam, cellPlatform, cellSide, _, route, ok := gfExactCell(v)
		point, _, pointOK := gfPaperPoint(v)
		if !ok || !pointOK || cellFam != fam || cellPlatform != platform ||
			cellSide != side || route != "taker" || point <= 0 {
			continue
		}
		if point > best {
			best = point
		}
	}
	return best, best > 0
}

// paperSystemRouteCurrentPoint re-prices that exact historical Paper cell at the book and fee the
// shared executor is about to pay. Favorable movement is never counted as extra proof; adverse
// price/fee movement is charged in full. This closes the gap where a positive historical point
// could bypass the EV floor even after today's ask erased it.
func (s *Server) paperSystemRouteCurrentPoint(source, platform, side string,
	currentPrice, currentFeeTotal, contracts float64) (float64, bool) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(source)), "auto-cons-") {
		return 0, false
	}
	fam := strings.ToLower(strings.TrimSpace(policySourceFamily(source)))
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	if fam == "" || (platform != "kalshi" && platform != "polyus") ||
		(side != "YES" && side != "NO") || contracts <= 0 {
		return 0, false
	}
	s.verdMu.Lock()
	cache, at := append([]verdictEnt(nil), s.verdCache...), s.verdAt
	s.verdMu.Unlock()
	if cache == nil || at.IsZero() || time.Since(at) > swStale {
		return 0, false
	}
	best, found := 0.0, false
	for _, v := range cache {
		cellFam, cellPlatform, cellSide, _, route, ok := gfExactCell(v)
		if !ok || cellFam != fam || cellPlatform != platform || cellSide != side || route != "taker" {
			continue
		}
		mean, _, pointOK := gfPaperPoint(v)
		historicalAsk, historicalFee := gfPaperPointCosts(v)
		adjusted, adjustedOK := gfCurrentRouteEdge(mean, historicalAsk, currentPrice,
			historicalFee, currentFeeTotal, contracts)
		if !pointOK || !adjustedOK || adjusted <= 0 {
			continue
		}
		if !found || adjusted > best {
			best, found = adjusted, true
		}
	}
	return best, found
}

// famEdgeSE — CACHE-ONLY, route-exact authenticated-profit lookup for variance shrink. Legacy
// family aggregates and assumed-fill verdicts intentionally return unavailable: neither can resize
// Paper or cash. A future exchange cohort must match family + venue + side + route exactly.
func (s *Server) famEdgeSE(platform, source, side, route string) (m, se float64, ok bool) {
	fam := policySourceFamily(source)
	if fam == "" {
		return 0, 0, false
	}
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	route = strings.ToLower(strings.TrimSpace(route))
	s.verdMu.Lock()
	cache, at := s.verdCache, s.verdAt
	s.verdMu.Unlock()
	if cache == nil || time.Since(at) > swStale {
		return 0, 0, false
	}
	for _, v := range cache {
		cellFam, cellPlatform, cellSide, _, cellRoute, exact := gfExactCell(v)
		if !exact || !verdictHasAuthenticatedProfitEvidence(v) || v.N == 0 ||
			cellFam != fam || cellPlatform != platform || cellSide != side || cellRoute != route {
			continue
		}
		m = math.Abs(v.Mean)
		se = (v.Hi - v.Lo) / 4
		if se <= 0 {
			se = 0.005
		}
		return m, se, true
	}
	return 0, 0, false
}

// policyStatus2 — policyStatus by FAMILY (policyStatus takes a source string).
func (s *Server) policyStatus2(fam string) string {
	s.polMu.Lock()
	defer s.polMu.Unlock()
	return s.polMap[fam]
}
