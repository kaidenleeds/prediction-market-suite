package server

// R124 BRIEFING SYSTEMS LEADERBOARD (operator ask: "so I can see more statistical data" on the phone).
// A compact stats block rendered directly below the 5-min briefing's equity header:
//
//   📊 Systems Leaderboard
//   clearly separated Combo/Parlay Lab classes by realized net/$1 staked
//   (plain name, n, CI; "no known link" never claims independence) ┐ once ≥100 combos graded;
//   worst) the bottom class                                          ┘ until then a one-line count
//   verdict changes: system verdict transitions since the last SENT briefing
//   gap-book: xvgap executor fills + realized ¢/contract so far
//   parlay lab rows last hour: candidates · graded (manifest generation labelled separately)
//
// ≤8 lines total, plain words (plabClassPlain), zero new network calls: everything reads
// in-process state (plabStats/rings, the 60s-cached verdict engine, the xvgap book file cache).
// /api/briefing/preview matches EXACTLY: the "since last" verdict snapshot advances only in
// BriefingMarkSent, which the telegram loop calls after a successful send — previewing never
// consumes a transition.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	briefSystemCountFreshTTL  = 5 * time.Minute
	briefSystemCountRetry     = 30 * time.Second
	briefSystemCountBuildTime = 20 * time.Second
	// Keep the phone decision view aligned with the economic digest and proof engine. Rows below
	// this distinct-contract floor remain visible on the full dashboard, but a tiny extreme must
	// not displace a materially sampled system in the compact Top/Bottom list.
	briefRankMinSettledContracts = 20
)

// briefVerdSnapKV — the last-SENT briefing's family→state map (kv-persisted so restarts don't
// fabricate a wall of "transitions" from an empty baseline).
const briefVerdSnapKV = "briefing_verdict_snap"

type briefRankedSystem struct {
	v           verdictEnt
	score       float64
	confirmed   bool
	cell        string
	mean        float64
	pointOnly   bool
	pointN      int
	ratePoint   float64
	rateLo      float64
	rateHi      float64
	ratePointOK bool
	rateReady   bool
}

// briefRouteRate is derived only from the last complete in-memory research digest. The phone
// briefing must never start another SQLite report merely to sort its seven displayed systems.
type briefRouteRate struct {
	point, lo, hi float64
	mean          float64
	n             int
	pointOK       bool
	ready         bool
}

func briefExactExecutionCell(v verdictEnt) (cell string, ok bool) {
	venue := briefVenueCode(v.Platform)
	if venue != "K" && venue != "PUS" {
		return "", false
	}
	side := strings.ToUpper(strings.TrimSpace(v.Side))
	switch side {
	case "YES":
		side = "Y"
	case "NO":
		side = "N"
	default:
		return "", false
	}
	order := briefRouteCode(v.Route)
	if order != "M" && order != "T" {
		return "", false
	}
	return venue + "/" + side + "/" + order, true
}

// briefDistinctSettledContracts is the phone-facing n: one venue+ticker contract at most once.
// Current unit-trial routes carry ContractMarkets explicitly. Older ticker-keyed verdict sources
// used Markets for that same distinct-contract count; EventClusters are intentionally never used.
func briefDistinctSettledContracts(v verdictEnt) int {
	if v.ContractMarkets > 0 {
		return v.ContractMarkets
	}
	if v.EventClusters <= 0 && v.Markets > 0 {
		return v.Markets
	}
	return 0
}

func briefRouteRateKey(family, platform, origin, side, route string) string {
	return strings.ToLower(strings.TrimSpace(family)) + "\x00" +
		strings.ToLower(strings.TrimSpace(platform)) + "\x00" +
		strings.ToLower(strings.TrimSpace(origin)) + "\x00" +
		strings.ToUpper(strings.TrimSpace(side)) + "\x00" +
		strings.ToLower(strings.TrimSpace(route))
}

// briefCachedRouteRates enriches the phone briefing from the already-computed digest. It never
// reads the database on the request path; a cold cache only schedules the bounded background job.
func (s *Server) briefCachedRouteRates() map[string]briefRouteRate {
	trials, _, _, _, _ := s.researchDigestRowsCached()
	if len(trials) == 0 {
		// Headless boots may never open the research dashboard. Schedule the existing bounded
		// singleflight on a cold phone cache, but return immediately; this request does no DB read.
		if s.researchDigestKickFn != nil {
			s.researchDigestKickFn()
		} else {
			s.kickResearchDigestRefresh()
		}
		// Preserve last-good exact route rows on the first headless briefing while Net/d warms.
		// A nil map enables the clearly labelled temporary edge/evidence fallback; once this cache
		// is populated, production ranking requires and prioritizes its conservative Net/d bounds.
		return nil
	}
	rates := make(map[string]briefRouteRate, len(trials))
	for _, row := range leaderboardBacktestRows(trials) {
		if !row.ProfitEvidence {
			continue
		}
		if (row.Platform != "kalshi" && row.Platform != "polyus") ||
			(row.Side != "YES" && row.Side != "NO") || row.Route != "taker" {
			continue
		}
		trackedDays := row.TrackedDays
		if trackedDays <= 0 && row.TrackedSeconds > 0 {
			trackedDays = row.TrackedSeconds / (24 * 60 * 60)
		}
		key := briefRouteRateKey(row.Family, row.Platform, row.OriginLayer, row.Side, row.Route)
		finiteCS := !math.IsInf(csRadius(row.SettledMarkets, row.SDPC), 1) &&
			!math.IsNaN(row.MeanPC) && !math.IsInf(row.MeanPC, 0) &&
			!math.IsNaN(row.SDPC) && !math.IsInf(row.SDPC, 0)
		rates[key] = briefRouteRate{point: row.NetPerCalendarDay,
			lo: row.NetPerCalendarDayLo, hi: row.NetPerCalendarDayHi,
			mean: row.MeanPC, n: row.SettledMarkets,
			pointOK: row.SettledMarkets > 0 && trackedDays > 0,
			// A compact ranking rate needs elapsed time, a finite contract-sample interval, and
			// the same n>=20 evidence floor as the economic digest. It is still not proof-ready,
			// which additionally waits for zero open rows.
			ready: row.SettledMarkets >= briefRankMinSettledContracts && trackedDays >= 1 && finiteCS &&
				!math.IsNaN(row.NetPerCalendarDayLo) && !math.IsNaN(row.NetPerCalendarDayHi) &&
				!math.IsInf(row.NetPerCalendarDayLo, 0) && !math.IsInf(row.NetPerCalendarDayHi, 0)}
	}
	return rates
}

func briefRankedSystems(verdicts []verdictEnt, positive bool, limit int) []briefRankedSystem {
	return briefRankedSystemsWithRates(verdicts, positive, limit, nil)
}

func briefRankedSystemsWithRates(verdicts []verdictEnt, positive bool, limit int,
	rates map[string]briefRouteRate) []briefRankedSystem {
	byIdentity := map[string]briefRankedSystem{}
	for _, v := range verdicts {
		// Historical assumed-fill and Paper-simulation rows remain queryable for audit, but they
		// are not profit evidence and must never reappear in operator Top/Bottom rankings.
		if !verdictHasAuthenticatedProfitEvidence(v) {
			continue
		}
		family := strings.ToLower(strings.TrimSpace(firstNonEmpty(v.SourceFamily, v.Family)))
		if strings.HasPrefix(family, "side-control:") ||
			strings.HasPrefix(strings.ToLower(strings.TrimSpace(v.Family)), "counterfactual:invert:") {
			continue
		}
		settledN := briefDistinctSettledContracts(v)
		if settledN < briefRankMinSettledContracts {
			continue
		}
		mean, pointOnly, pointN := v.Mean, false, 0
		if v.N <= 0 {
			var pointOK bool
			mean, pointN, pointOK = gfPaperPoint(v)
			if !pointOK || pointN <= 0 {
				continue
			}
			pointOnly = true
		}
		// Paper points have no valid CI. They stay visible on the dashboard but cannot enter a
		// Top/Bottom list explicitly ranked on n + edge + confidence interval.
		if pointOnly {
			continue
		}
		if positive != (mean > 0) || mean == 0 {
			continue
		}
		cell, ok := briefExactExecutionCell(v)
		if !ok {
			continue // POOL/BOOK/signal aggregates are not exact maker/taker execution identities
		}
		origin := strings.ToLower(strings.TrimSpace(firstNonEmpty(v.OriginLayer, v.Group)))
		key := family + "\x00" + cell + "\x00" + origin
		saneCI := math.Abs(v.Lo) < 100 && math.Abs(v.Hi) < 100 && v.Lo <= v.Hi &&
			!(v.Lo == 0 && v.Hi == 0 && v.Mean != 0)
		if !saneCI {
			continue
		}
		confirmed := saneCI && ((positive && v.Lo > 0) || (!positive && v.Hi < 0))
		// Rank on edge, distinct settled contracts, and CI precision. A CI wholly beyond
		// zero remains the primary tier via confirmed above.
		settledWeight := float64(settledN) / float64(settledN+40)
		ciWeight := 0.5
		if saneCI {
			width := math.Max(0, v.Hi-v.Lo)
			precision := math.Abs(mean) / (math.Abs(mean) + width/2 + 1e-12)
			ciWeight = 0.5 + 0.5*precision
		}
		entry := briefRankedSystem{v: v, cell: cell, confirmed: confirmed, mean: mean,
			pointOnly: pointOnly, pointN: pointN,
			score: math.Abs(mean) * math.Sqrt(settledWeight) * ciWeight}
		rateKey := briefRouteRateKey(family, v.Platform, origin, v.Side, v.Route)
		if rate, exists := rates[rateKey]; exists {
			// Verdict and rate caches refresh independently. Never mix a new edge/n with an old
			// Net/d cohort merely because their text identity happens to match.
			if rate.n == settledN && math.Abs(rate.mean-mean) <= .00011 {
				entry.ratePoint, entry.rateLo, entry.rateHi = rate.point, rate.lo, rate.hi
				entry.ratePointOK, entry.rateReady = rate.pointOK, rate.ready
			}
		}
		// The production phone list supplies a non-nil digest-rate map. An exact maker row is
		// still inspectable on the full dashboard, but cannot enter a Net/d-ranked phone list
		// until its own elapsed-time rate and conservative bound are available.
		if rates != nil && !entry.rateReady {
			continue
		}
		if prior, exists := byIdentity[key]; !exists || settledN > briefDistinctSettledContracts(prior.v) ||
			(settledN == briefDistinctSettledContracts(prior.v) && entry.score > prior.score) {
			byIdentity[key] = entry
		}
	}
	rows := make([]briefRankedSystem, 0, len(byIdentity))
	for _, row := range byIdentity {
		rows = append(rows, row)
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].rateReady != rows[j].rateReady {
			return rows[i].rateReady
		}
		if rows[i].rateReady {
			// Top: highest conservative lower bound. Bottom: lowest conservative upper bound,
			// so one noisy downside tail cannot make an otherwise uncertain row look worst.
			left, right := rows[i].rateLo, rows[j].rateLo
			if !positive {
				left, right = rows[i].rateHi, rows[j].rateHi
			}
			if left != right {
				if positive {
					return left > right
				}
				return left < right
			}
		}
		if rows[i].confirmed != rows[j].confirmed {
			return rows[i].confirmed
		}
		if rows[i].pointOnly != rows[j].pointOnly {
			return !rows[i].pointOnly // independent event evidence always outranks legacy Paper points
		}
		if rows[i].score != rows[j].score {
			return rows[i].score > rows[j].score
		}
		if briefDistinctSettledContracts(rows[i].v) != briefDistinctSettledContracts(rows[j].v) {
			return briefDistinctSettledContracts(rows[i].v) > briefDistinctSettledContracts(rows[j].v)
		}
		return rows[i].v.Family < rows[j].v.Family
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows
}

func briefSignedDollars(v float64) string {
	if math.Abs(v) < .00005 {
		v = 0
	}
	digits := 2
	if v != 0 && math.Abs(v) < .01 {
		digits = 4
	}
	sign := "+"
	if v < 0 {
		sign = "-"
	}
	return fmt.Sprintf("%s$%.*f", sign, digits, math.Abs(v))
}

func briefSignedDollarsPerDay(v float64) string {
	return briefSignedDollars(v) + "/d"
}

func briefRankedSystemName(row briefRankedSystem, originCells map[string]int) string {
	v := row.v
	base := strings.TrimSpace(firstNonEmpty(v.SourceFamily, v.Family))
	name := famPlainName(base)
	key := strings.ToLower(base) + "\x00" + row.cell
	if originCells[key] > 1 {
		name += " (" + strings.ToLower(firstNonEmpty(v.OriginLayer, v.Group)) + ")"
	}
	return name
}

func briefRankedSystemLine(row briefRankedSystem, originCells map[string]int) string {
	v := row.v
	if row.pointOnly {
		return fmt.Sprintf("%s · $/d unavailable · %+.1f¢/s · n%d · CI unavailable · %s · PAPER point",
			briefRankedSystemName(row, originCells), row.mean*100, row.pointN, row.cell)
	}
	ci := "CI pending"
	if math.Abs(v.Lo) < 100 && math.Abs(v.Hi) < 100 && v.Lo <= v.Hi &&
		!(v.Lo == 0 && v.Hi == 0 && v.Mean != 0) {
		ci = fmt.Sprintf("CI %+.1f..%+.1f¢", v.Lo*100, v.Hi*100)
	}
	settledN := briefDistinctSettledContracts(v)
	rate := "$/d warming"
	if row.ratePointOK {
		rate = briefSignedDollarsPerDay(row.ratePoint)
		if row.rateReady {
			rate += " [" + briefSignedDollars(row.rateLo) + ".." + briefSignedDollars(row.rateHi) + "]"
		}
	}
	return fmt.Sprintf("%s %s · %s · %+.1f¢/s · n%d · %s · %s",
		briefSampleBucket(settledN), briefRankedSystemName(row, originCells), rate, row.mean*100,
		settledN, ci, row.cell)
}

// briefTrackedSystemCounts serves the most recent complete Systems-Leaderboard count and starts
// at most one bounded refresh in the background when it is absent or stale. buildSystemLeaderboardRows
// performs multiple large SQLite reports; putting it on the briefing request path caused the R144
// preview to exceed its 30-second client deadline while the rest of the suite remained healthy.
// A failed/timed-out refresh never erases a prior count, and a cold cache renders "refreshing".
func (s *Server) briefTrackedSystemCounts(verdicts []verdictEnt) (variants, families int, ok bool) {
	// The production count is assembled entirely from the immutable verdict snapshot, the cached
	// compact unit-trial digest, and static registries. It does not touch SQLite, so a cold briefing
	// can publish the real roster immediately instead of showing "refreshing" while launching the
	// full historical leaderboard. Keep the asynchronous seam below only for latency regression
	// tests and any future explicitly expensive override.
	if s.briefCountFn == nil {
		now := time.Now()
		s.briefCountMu.Lock()
		if s.briefCountOK && !s.briefCountAt.IsZero() &&
			now.Sub(s.briefCountAt) < briefSystemCountFreshTTL && now.Before(s.briefCountNext) {
			variants, families = s.briefCountVariants, s.briefCountFamilies
			s.briefCountMu.Unlock()
			return variants, families, true
		}
		s.briefCountMu.Unlock()
		variants, families, ok = s.trackedSystemCounts(context.Background(), verdicts)
		if ok {
			s.briefCountMu.Lock()
			s.briefCountVariants, s.briefCountFamilies = variants, families
			s.briefCountAt, s.briefCountOK = time.Now(), true
			s.briefCountNext = s.briefCountAt.Add(briefSystemCountFreshTTL)
			s.briefCountMu.Unlock()
		}
		return variants, families, ok
	}
	now := time.Now()
	s.briefCountMu.Lock()
	variants, families, ok = s.briefCountVariants, s.briefCountFamilies, s.briefCountOK
	due := !s.briefCountBusy && !now.Before(s.briefCountNext) &&
		(!s.briefCountOK || s.briefCountAt.IsZero() || now.Sub(s.briefCountAt) >= briefSystemCountFreshTTL)
	if due {
		s.briefCountBusy = true
	}
	build := s.briefCountFn
	s.briefCountMu.Unlock()
	if !due {
		return variants, families, ok
	}
	if build == nil {
		build = s.trackedSystemCounts
	}
	snapshot := append([]verdictEnt(nil), verdicts...)
	go func() {
		var nextVariants, nextFamilies int
		built := false
		defer func() {
			s.briefCountMu.Lock()
			s.briefCountBusy = false
			if built {
				s.briefCountVariants, s.briefCountFamilies = nextVariants, nextFamilies
				s.briefCountAt, s.briefCountOK = time.Now(), true
				s.briefCountNext = s.briefCountAt.Add(briefSystemCountFreshTTL)
			} else {
				// Preserve any last-good value and back off. A busy database must not be hammered by
				// every preview/poll while the next bounded attempt is pending.
				s.briefCountNext = time.Now().Add(briefSystemCountRetry)
			}
			s.briefCountMu.Unlock()
		}()
		defer s.recoverGuard("brief-system-count")
		ctx, cancel := context.WithTimeout(context.Background(), briefSystemCountBuildTime)
		defer cancel()
		nextVariants, nextFamilies, built = build(ctx, snapshot)
		if ctx.Err() != nil {
			built = false
		}
	}()
	return variants, families, ok
}

// cacheBriefPortfolioMetrics records the successful portfolio snapshot that equityHeaderBlock
// already paid to compute. A failed attempt is remembered so a later block in the same briefing
// does not immediately repeat the same history query with a smaller deadline; last-good truth is
// retained across transient database contention.
const briefPortfolioAttemptTTL = 10 * time.Second

func (s *Server) currentBriefPnLEpoch() time.Time {
	s.pnlMu.Lock()
	epoch := s.pnlEpoch
	s.pnlMu.Unlock()
	if epoch.IsZero() {
		return time.Unix(0, 0).UTC()
	}
	return epoch.UTC()
}

func (s *Server) cacheBriefPortfolioMetrics(book string, metrics bookSessionMetrics, ok bool) {
	epoch := s.currentBriefPnLEpoch()
	s.briefPortfolioMu.Lock()
	defer s.briefPortfolioMu.Unlock()
	if !s.briefPortfolioEpoch.Equal(epoch) {
		s.briefPortfolioEpoch = epoch
		s.briefPortfolioMetrics = nil
		s.briefPortfolioTried = nil
	}
	if s.briefPortfolioTried == nil {
		s.briefPortfolioTried = make(map[string]time.Time)
	}
	s.briefPortfolioTried[book] = time.Now()
	if !ok {
		return
	}
	if s.briefPortfolioMetrics == nil {
		s.briefPortfolioMetrics = make(map[string]bookSessionMetrics)
	}
	s.briefPortfolioMetrics[book] = metrics
}

func (s *Server) cachedBriefPortfolioMetrics(book string) (metrics bookSessionMetrics, ok, tried bool) {
	epoch := s.currentBriefPnLEpoch()
	s.briefPortfolioMu.Lock()
	defer s.briefPortfolioMu.Unlock()
	if !s.briefPortfolioEpoch.Equal(epoch) {
		s.briefPortfolioEpoch = epoch
		s.briefPortfolioMetrics = nil
		s.briefPortfolioTried = nil
		return bookSessionMetrics{}, false, false
	}
	if attemptedAt, exists := s.briefPortfolioTried[book]; exists {
		tried = time.Since(attemptedAt) <= briefPortfolioAttemptTTL
		if !tried {
			delete(s.briefPortfolioTried, book)
		}
	}
	if s.briefPortfolioMetrics != nil {
		metrics, ok = s.briefPortfolioMetrics[book]
	}
	return metrics, ok, tried
}

// briefScoreboard is the phone decision view. The dashboard retains every model, collector,
// control, route, raw row count and diagnostic; this surface deliberately shows only the actual
// tracked-system total, seven credible positive execution cells, and two negative cells. Exact Y/N
// and maker/taker cells stay separate—no pooled signal or book result can borrow an execution
// identity. Combo Paper already appears once in the portfolio header.
func (s *Server) briefScoreboard(ctx context.Context) string {
	_ = ctx
	// The two-minute verdict sweep owns the million-row refresh. A phone request reads its immutable
	// memory/disk snapshot exactly like /api/systems-leaderboard; stale evidence is both faster and
	// more honest than recomputing a partial roster against an expiring request context.
	verdicts, _, _ := s.leaderboardVerdictSnapshot()
	rates := s.briefCachedRouteRates()
	positive := briefRankedSystemsWithRates(verdicts, true, 7, rates)
	negative := briefRankedSystemsWithRates(verdicts, false, 2, rates)
	originCells := map[string]int{}
	seenOrigin := map[string]bool{}
	for _, row := range append(append([]briefRankedSystem(nil), positive...), negative...) {
		base := strings.ToLower(strings.TrimSpace(firstNonEmpty(row.v.SourceFamily, row.v.Family)))
		origin := strings.ToLower(strings.TrimSpace(firstNonEmpty(row.v.OriginLayer, row.v.Group)))
		key := base + "\x00" + row.cell
		originKey := key + "\x00" + origin
		if !seenOrigin[originKey] {
			seenOrigin[originKey] = true
			originCells[key]++
		}
	}

	var b strings.Builder
	b.WriteString("\n📊 Systems\n")
	if variants, families, ok := s.briefTrackedSystemCounts(verdicts); ok {
		counts := r145CanonicalSystemCatalogCounts()
		if variants == counts.DeclaredTypedVariants && families == counts.BaseSystems {
			b.WriteString(fmt.Sprintf("⚙ %d bases · %d order systems · %d typed variants · %d producer-capable / %d blocked · fresh-fed not inferred\n",
				families, counts.OrderOriginatingBaseSystems, variants,
				counts.ProducerCapableTypedVariants, counts.ProducerBlockedTypedVariants))
			if counts.CrossVenueInputVariants > 0 {
				b.WriteString(fmt.Sprintf("↔ %d cross-venue-input variants · input topology ≠ order venue · PINT input-only; orders K/PUS\n",
					counts.CrossVenueInputVariants))
			}
			if counts.ExternallyBlockedDecisionSystems > 0 {
				b.WriteString(fmt.Sprintf("⛔ %d multi-leg decisions externally blocked · %s\n",
					counts.ExternallyBlockedDecisionSystems,
					strings.Join(counts.ExternallyBlockedDecisionIDs, ", ")))
			}
			b.WriteString("n = unique settled venue+ticker contracts\n")
		} else {
			// Test/diagnostic builders can inject a deliberately smaller cached roster. Do not
			// attach the canonical missing-declaration count to that synthetic fixture.
			b.WriteString(fmt.Sprintf("⚙ %d variants / %d systems · n = unique settled venue+ticker contracts\n", variants, families))
		}
	} else {
		b.WriteString("⚙ system count refreshing · n = unique settled venue+ticker contracts\n")
	}
	b.WriteString("📈 Top 7 positive\n")
	if len(positive) == 0 {
		b.WriteString("— no settled exact K/PUS maker/taker route yet\n")
	}
	for _, row := range positive {
		b.WriteString(briefRankedSystemLine(row, originCells) + "\n")
	}
	b.WriteString("📉 Bottom 2 negative\n")
	if len(negative) == 0 {
		b.WriteString("— no settled exact K/PUS maker/taker route yet\n")
	}
	for _, row := range negative {
		b.WriteString(briefRankedSystemLine(row, originCells) + "\n")
	}

	return b.String()
}

func briefRelationCell(label string, c storage.RelationPerformanceCell) (string, bool) {
	if c.UniqueBets <= 0 {
		return "", false
	}
	sign := "+"
	if c.DollarsPerBet < 0 {
		sign = "-"
	}
	betWord := "bets"
	if c.UniqueBets == 1 {
		betWord = "bet"
	}
	return fmt.Sprintf("%s %s$%.2f/bet · %d %s", label, sign, math.Abs(c.DollarsPerBet), c.UniqueBets, betWord), true
}

// briefFundedRelations is the compact money-correlation view. It always renders all five funded
// sleeves; missing evidence is an em dash, never a fabricated zero. Unknown identity is its own
// cell and cannot leak into independent results.
func (s *Server) briefFundedRelations(ctx context.Context) string {
	epoch := s.currentBriefPnLEpoch()
	s.briefRelationMu.Lock()
	defer s.briefRelationMu.Unlock()
	if !s.briefRelationEpoch.Equal(epoch) {
		s.briefRelationEpoch = epoch
		s.briefRelationAt = time.Time{}
		s.briefRelationLine = ""
	}
	if s.briefRelationLine != "" && time.Since(s.briefRelationAt) < time.Minute {
		return s.briefRelationLine
	}
	line, ok := s.buildBriefFundedRelations(ctx, epoch)
	if ok {
		s.briefRelationLine, s.briefRelationAt = line, time.Now()
		return line
	}
	if s.briefRelationLine != "" {
		return s.briefRelationLine
	}
	return line
}

func (s *Server) buildBriefFundedRelations(ctx context.Context, epoch time.Time) (string, bool) {
	bctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	rows, err := s.store.FundedRelationPerformance(bctx, epoch)
	cancel()
	if err != nil {
		return "\n🔗 Related-bet P&L\nrefreshing\n", false
	}
	byPortfolio := map[string]map[string]storage.RelationPerformanceCell{}
	for _, row := range rows {
		byPortfolio[row.Portfolio] = map[string]storage.RelationPerformanceCell{}
		for _, cell := range row.Cells {
			byPortfolio[row.Portfolio][cell.Relation] = cell
		}
	}
	var b strings.Builder
	b.WriteString("\n🔗 Related-bet P&L\n")
	for _, item := range []struct{ key, label string }{
		{"kalshi", "Kalshi"}, {"polyus", "PolyUS"}, {"combos", "Combo"},
		{"ml-kalshi", "ML Kalshi"}, {"ml-polyus", "ML PolyUS"},
	} {
		cells := byPortfolio[item.key]
		parts := make([]string, 0, 3)
		for _, rel := range []struct{ key, label string }{
			{"independent", "independent"}, {"dependent", "related"}, {"unknown", "unknown"},
		} {
			if cell, ok := briefRelationCell(rel.label, cells[rel.key]); ok {
				parts = append(parts, cell)
			}
		}
		if len(parts) > 0 {
			b.WriteString(item.label + " · " + strings.Join(parts, " · ") + "\n")
		}
	}
	pctx, pcancel := context.WithTimeout(ctx, 3*time.Second)
	proper, err := s.store.ProperScoreRelationPerformance(pctx, epoch)
	pcancel()
	if err != nil {
		b.WriteString("Proper · refreshing\n")
		return b.String(), false
	}
	byTransform := map[string]map[string]storage.ProperRelationPerformance{}
	for _, row := range proper {
		if byTransform[row.Transform] == nil {
			byTransform[row.Transform] = map[string]storage.ProperRelationPerformance{}
		}
		byTransform[row.Transform][row.Relation] = row
	}
	properCell := func(label string, c storage.ProperRelationPerformance) (string, bool) {
		if c.UniqueBets <= 0 {
			return "", false
		}
		sign := "+"
		if c.DollarsPerBet < 0 {
			sign = "-"
		}
		betWord := "bets"
		if c.UniqueBets == 1 {
			betWord = "bet"
		}
		return fmt.Sprintf("%s %s$%.2f/bet · %d %s", label, sign,
			math.Abs(c.DollarsPerBet), c.UniqueBets, betWord), true
	}
	for _, item := range []struct{ key, label string }{{"brier", "Brier"}, {"log", "Log"}, {"spherical", "Spherical"}} {
		cells := byTransform[item.key]
		parts := make([]string, 0, 3)
		for _, rel := range []struct{ key, label string }{
			{"independent", "independent"}, {"dependent", "related"}, {"unknown", "unknown"},
		} {
			if cell, ok := properCell(rel.label, cells[rel.key]); ok {
				parts = append(parts, cell)
			}
		}
		if len(parts) > 0 {
			b.WriteString(item.label + " · " + strings.Join(parts, " · ") + "\n")
		}
	}
	return b.String(), true
}

func (s *Server) briefBookCentsPerContract(ctx context.Context) map[string]float64 {
	out := map[string]float64{}
	accWhere := func(closed []kfClosed, accept func(kfPos) bool) (float64, bool) {
		pnl, cts := 0.0, 0.0
		for _, c := range closed {
			if accept != nil && !accept(c.kfPos) {
				continue
			}
			pnl += c.PnL
			cts += c.Contracts
		}
		if cts <= 0 {
			return 0, false
		}
		return pnl / cts * 100, true
	}
	s.rfBookMu.Lock()
	rfClosed := fundedPaperClosedSnapshot(s.rfLoadLocked())
	s.rfBookMu.Unlock()
	s.wxBookMu.Lock()
	wxClosed := fundedPaperClosedSnapshot(s.wxBookLoadLocked())
	s.wxBookMu.Unlock()
	s.kfBookMu.Lock()
	kb := s.kfLoadLocked()
	kfPreClosed, kfLiveClosed := fundedPaperClosedSnapshot(&kb.Pre), fundedPaperClosedSnapshot(&kb.Live)
	s.kfBookMu.Unlock()
	s.fiBookMu.Lock()
	fiClosed := fundedPaperClosedSnapshot(s.fiLoadLocked())
	s.fiBookMu.Unlock()
	s.xvgBookMu.Lock()
	xvgClosed := fundedPaperClosedSnapshot(s.xvgLoadLocked())
	s.xvgBookMu.Unlock()
	s.flBookMu.Lock()
	flClosed := fundedPaperClosedSnapshot(s.flLoadLocked())
	s.flBookMu.Unlock()
	s.cbBookMu.Lock()
	cbB := s.cbLoadLocked()
	cbKClosed, cbPClosed := fundedPaperClosedSnapshot(&cbB.K), fundedPaperClosedSnapshot(&cbB.P)
	s.cbBookMu.Unlock()
	s.gfBookMu.Lock()
	gfClosed := make(map[string][]kfClosed, len(s.gfLoadLocked().Subs))
	for k, b := range s.gfLoadLocked().Subs {
		gfClosed[k] = fundedPaperClosedSnapshot(b)
	}
	s.gfBookMu.Unlock()

	allClosed := [][]kfClosed{rfClosed, wxClosed, kfPreClosed, kfLiveClosed, fiClosed,
		xvgClosed, flClosed, cbKClosed, cbPClosed}
	for _, closed := range gfClosed {
		allClosed = append(allClosed, closed)
	}
	proofs, _ := s.fundedPaperImmutableProofs(ctx, fundedPaperLots(allClosed...))
	acc := func(closed []kfClosed) (float64, bool) { return accWhere(closed, proofs.accepts) }
	for key, closed := range map[string][]kfClosed{
		"rawflow": rfClosed, "weather": wxClosed, "kflow-pre": kfPreClosed,
		"kflow-live": kfLiveClosed, "book:freshinv": fiClosed, "book:xvgap": xvgClosed,
		"book:favlong80": flClosed, "book:cheapband-k": cbKClosed, "book:cheapband-p": cbPClosed,
	} {
		if value, ok := acc(closed); ok {
			out[key] = value
		}
	}
	var fiK, fiP []kfClosed
	for _, row := range fiClosed {
		if fiLotPlatform(row.kfPos) == vbPolyus {
			fiP = append(fiP, row)
		} else {
			fiK = append(fiK, row)
		}
	}
	if value, ok := acc(fiK); ok {
		out["book:freshinv-k"] = value
	}
	if value, ok := acc(fiP); ok {
		out["book:freshlist-p"] = value
	}
	for key, closed := range gfClosed {
		if value, ok := acc(closed); ok {
			out[key] = value
		}
	}
	return out
}

// famPlain — plain-language strategy names for the phone briefing (R91 plain-words law). Keys
// stay canonical everywhere else (JSON, DB, dashboard); fallbacks below cover prefixes.
var famPlain = map[string]string{
	"xvgap":                  "cross-venue gap",
	"xvgap2":                 "cross-venue gap (mirrored/NO side)",  // R126: new coverage cohort, log-only
	"xvgapk":                 "cross-venue gap (Kalshi side)",       // R126: log-only mirror expression
	"combo-overlay":          "gap + partner combo (product study)", // R126 Part 4.3 overlay ledger — R127: the log-only product-cost study
	"combo-synth":            "gap combo (leg-stack)",               // R127: the Combos book's honest money expression
	"combo-rfq":              "gap combo (venue quote)",             // R127: actual combined-market quote (n≈0 expected)
	"xvlock":                 "cross-venue lock",
	"lockstack":              "cross-venue lock (staked)", // R129: the depth-verified staked slice
	"book:cheapband-k":       "cheap side <10¢ (kalshi)",  // R129: band-study bettor halves
	"book:cheapband-p":       "cheap side <15¢ (polyus)",
	"xvlag":                  "cross-venue lag",
	"xmatch":                 "crypto twin match",
	"poly-consensus":         "poly crowd-follow",
	"poly-whale":             "poly whale-follow",
	"kalshi-whale":           "kalshi whale-follow",
	"polyus-whale":           "polyus whale-follow",
	"kalshi-flow":            "kalshi order-flow",
	"polyus-flow":            "polyus order-flow",
	"kflow":                  "kalshi flow-follow",
	"kflow-pre":              "kflow book (pre)",
	"kflow-live":             "kflow book (live)",
	"rawflow":                "raw-flow book",
	"weather":                "weather book",
	"wxedge":                 "weather edge",
	"pcrypto":                "poly crypto momentum",
	"ml-book":                "ML book",
	"maker-fills":            "maker fills",
	"rfq-sim":                "RFQ quote sim",
	"favlong":                "favorite-long",
	"freshlist":              "fresh listings",
	"freshinv":               "fresh-listing fade",
	"fade":                   "consensus fade",
	"cross":                  "signal confluence",
	"sharpline":              "sharp line (pinnacle)",
	"meanrev":                "mean reversion",
	"insider":                "insider watch",
	"whale-exit":             "whale exit",
	"whale-exit-hold-bridge": "whale-exit hold bridge",
	"smartmoney":             "smart money",
	"spotlag":                "spot lag",
	"arb":                    "three-venue arb",
	"pmatch":                 "poly match",
	"pflow":                  "poly flow",
	"time-nested-lock":       "time-nested lock",
	"joint-marginal-lock":    "joint-vs-marginal lock",
	"fee-rounding-batch":     "fee-rounding batch",
}

// famPlainName resolves a strategy key to its plain name (prefix-aware; unknown keys pass through
// so nothing ever renders blank).
func famPlainName(f string) string {
	if p, ok := famPlain[f]; ok {
		return p
	}
	if base, ok := strings.CutPrefix(f, "counterfactual:invert:"); ok {
		return "diagnostic inverse of " + famPlainName(base)
	}
	for _, route := range []string{"taker:", "maker:"} {
		if base, ok := strings.CutPrefix(f, route); ok {
			venue := ""
			if b, v, found := strings.Cut(base, "@"); found {
				base, venue = b, " ("+v+")"
			}
			return strings.TrimSuffix(route, ":") + ": " + famPlainName(base) + venue
		}
	}
	if strings.HasPrefix(f, "invert:") {
		return "fade of " + famPlainName(strings.TrimPrefix(f, "invert:"))
	}
	if strings.HasPrefix(f, "book:") {
		return famPlainName(strings.TrimPrefix(f, "book:")) + " book"
	}
	if base, ok := strings.CutPrefix(f, "gf:"); ok { // R130 follower subs: gf:<family>-k / -p
		venue := ""
		if b2, ok2 := strings.CutSuffix(base, "-k"); ok2 {
			base, venue = b2, " (kalshi)"
		} else if b2, ok2 := strings.CutSuffix(base, "-p"); ok2 {
			base, venue = b2, " (polyus)"
		}
		return "follow: " + famPlainName(base) + venue
	}
	if strings.HasPrefix(f, "parlay-") && strings.HasSuffix(f, "leg") {
		return strings.TrimSuffix(strings.TrimPrefix(f, "parlay-"), "leg") + "-leg combos"
	}
	return f
}

func briefVerdictSide(v verdictEnt) string {
	side := strings.ToUpper(strings.TrimSpace(v.Side))
	if side == "YES" || side == "NO" {
		return " [" + side + "]"
	}
	return ""
}

// briefVerdictReceipt appends the exact execution cell and distinct-contract sample without
// disturbing the familiar score-first row. Repeated receipts never inflate n.
func briefVerdictReceipt(v verdictEnt) string {
	venue := briefVenueCode(strings.ToLower(strings.TrimSpace(v.Platform)))
	if venue == "" {
		var venues []string
		if v.KSh > 0 {
			venues = append(venues, "K")
		}
		if v.PSh > 0 {
			venues = append(venues, "PUS")
		}
		if v.Locked {
			venues = append(venues, "PINT")
		}
		if len(venues) > 0 {
			venue = strings.Join(venues, "+")
		} else {
			venue = "POOL"
		}
	}
	route := briefRouteCode(firstNonEmpty(v.Route, v.Group))
	side := strings.ToUpper(strings.TrimSpace(v.Side))
	switch side {
	case "YES":
		side = "Y"
	case "NO":
		side = "N"
	default:
		side = "*"
	}
	n := briefDistinctSettledContracts(v)
	return fmt.Sprintf(" · %s/%s/%s · n%d %s · %s", venue, side, route,
		n, briefSampleBucket(n), firstNonEmpty(v.State, "COLLECTING"))
}

func briefSampleBucket(markets int) string {
	return evidenceSampleBucket(markets).Emoji
}

func briefVenueCode(venue string) string {
	switch strings.ToLower(strings.TrimSpace(venue)) {
	case "kalshi":
		return "K"
	case "polyus", "poly-us":
		return "PUS"
	case "polymarket", "poly-int", "polyint":
		return "PINT"
	default:
		return strings.ToUpper(strings.TrimSpace(venue))
	}
}

func briefRouteCode(route string) string {
	switch strings.ToLower(strings.TrimSpace(route)) {
	case "taker":
		return "T"
	case "maker":
		return "M"
	case "both", "maker+taker", "taker+maker", "maker/taker", "taker/maker":
		return "B"
	case "signal", "invert":
		return "SIG"
	case "strategy":
		return "SYS"
	case "book":
		return "BOOK"
	case "rfq":
		return "RFQ"
	case "parlay", "combo":
		return "COMBO"
	default:
		return strings.ToUpper(firstNonEmpty(strings.TrimSpace(route), "?"))
	}
}

func firstPositiveInt(values ...int) int {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}

// briefCollectingCount — how many signal families logged OPEN rows in the last 24h but are not in
// the verdict slice (under FamilyEdgeStats' ≥20-graded-in-90d floor). 10-min cached AND bounded to
// a 2s query: the briefing (and its preview) must render fast even while the ptt backfill worker
// is hammering the DB — on timeout/error we keep the last value and back off, never block the
// render (the R125 preview-timeout catch: this query stalled the briefing under worker load).
func (s *Server) briefCollectingCount(parent context.Context, verds []verdictEnt) int {
	s.famLogMu.Lock()
	if time.Since(s.famLogAt) > 10*time.Minute {
		cctx, cancel := context.WithTimeout(parent, 2*time.Second)
		n, err := s.store.CountActiveSignalFamilies(cctx, time.Now().Add(-24*time.Hour))
		cancel()
		if err == nil {
			s.famLogN = n
		}
		s.famLogAt = time.Now() // back off regardless — a slow query must not retry every render
	}
	n := s.famLogN
	s.famLogMu.Unlock()
	sig := 0
	for _, v := range verds {
		if v.Group == "signal" {
			sig++
		}
	}
	if n > sig {
		return n - sig
	}
	return 0
}

// BriefingMarkSent advances the last-SENT verdict snapshot. Called by the telegram briefing loop
// ONLY after a successful send — /api/briefing/preview therefore always shows exactly what the
// next send will say, and previews never consume a transition.
func (s *Server) BriefingMarkSent(ctx context.Context) {
	cur := map[string]string{}
	// Persist the same immutable snapshot the briefing rendered. A successful Telegram delivery
	// must not synchronously launch the million-row verdict refresh on the queue/transport path.
	verdicts, _, _ := s.leaderboardVerdictSnapshot()
	for _, v := range verdicts {
		cur[verdictStableKey(v)] = v.State
	}
	if bts, err := json.Marshal(cur); err == nil {
		_ = s.store.KVSet(ctx, briefVerdSnapKV, string(bts))
	}
}
