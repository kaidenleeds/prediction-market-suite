package server

import (
	"context"
	"math"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// makerRouteStat grades the economic result per attempted one-share maker quote. Canceled quotes
// contribute exactly $0; only settled fills contribute payout - post - maker fee. This prevents
// the conditional-on-fill winner bias that made the old maker-fills row too optimistic.
type makerRouteStat struct {
	Platform          string
	Source            string
	Family            string
	OriginLayer       string
	Inverted          bool
	Side              string
	N                 int // independent venue+ticker markets used by confidence
	Markets           int
	SettledRows       int
	FillN             int
	Open              int
	Mean              float64
	SD                float64
	MeanPost          float64
	MeanFeePC         float64
	Invalid           int
	MatureUTCBlocks   int
	ObservedUTCBlocks int
	EventDayClusters  int
	DayMean           float64
	DayLo             *float64
	DayHi             *float64
	NetDayLo          *float64
	NetDayHi          *float64
	marketRows        map[string][]float64
}

type makerRouteFeeFn func(storage.MakerRouteAttempt) (float64, bool)

func makerRouteStatFrom(rows []storage.MakerRouteAttempt, feeFn makerRouteFeeFn) makerRouteStat {
	var st makerRouteStat
	if len(rows) > 0 {
		st.Platform, st.Source, st.Family, st.Inverted = rows[0].Platform, rows[0].Source, rows[0].Family, rows[0].Inverted
		st.OriginLayer = makerRouteOriginLayer(rows[0].Source)
		st.Side = strings.ToUpper(strings.TrimSpace(rows[0].Side))
	}
	obs := make([]float64, 0, len(rows))
	st.marketRows = map[string][]float64{}
	dayEvents := make(map[string]map[string][]float64)
	var first time.Time
	for _, r := range rows {
		if r.Platform != st.Platform || r.Source != st.Source || r.Family != st.Family || r.Inverted != st.Inverted ||
			strings.ToUpper(strings.TrimSpace(r.Side)) != st.Side ||
			r.PostPx <= 0 || r.PostPx >= 1 || math.IsNaN(r.PostPx) || math.IsInf(r.PostPx, 0) {
			st.Invalid++
			continue
		}
		if !r.OpenedTS.IsZero() && (first.IsZero() || r.OpenedTS.Before(first)) {
			first = r.OpenedTS.UTC()
		}
		if r.Open {
			st.Open++
			continue
		}
		pnl := 0.0
		if !r.Filled {
			pnl = 0 // no fill means no principal, fee, payout, or PnL
		} else {
			if r.Settle < 0 || r.Settle > 1 || math.IsNaN(r.Settle) || math.IsInf(r.Settle, 0) {
				st.Invalid++
				continue
			}
			fee, known := feeFn(r)
			if !known || math.IsNaN(fee) || math.IsInf(fee, 0) || fee <= -1 || fee >= 1 {
				st.Invalid++
				continue
			}
			payout := r.Settle
			if strings.EqualFold(strings.TrimSpace(r.Side), "NO") || strings.EqualFold(strings.TrimSpace(r.Side), "DOWN") {
				payout = 1 - r.Settle
			}
			pnl = payout - r.PostPx - fee
			st.FillN++
			st.MeanPost += r.PostPx
			st.MeanFeePC += fee
		}
		obs = append(obs, pnl)
		marketKey := strings.ToLower(strings.TrimSpace(r.Platform)) + "|" + strings.TrimSpace(r.Ticker)
		if strings.TrimSpace(r.Ticker) != "" {
			st.marketRows[marketKey] = append(st.marketRows[marketKey], pnl)
		}
		if !r.OpenedTS.IsZero() {
			day := utcDay(r.OpenedTS).Format("2006-01-02")
			if dayEvents[day] == nil {
				dayEvents[day] = make(map[string][]float64)
			}
			dayEvents[day][strings.TrimSpace(r.Ticker)] = append(dayEvents[day][strings.TrimSpace(r.Ticker)], pnl)
		}
	}
	st.SettledRows = len(obs)
	marketMeans := make([]float64, 0, len(st.marketRows))
	for _, values := range st.marketRows {
		if n, mean, _ := meanSD(values); n > 0 {
			marketMeans = append(marketMeans, mean)
		}
	}
	st.N, st.Mean, st.SD = meanSD(marketMeans)
	st.Markets = st.N
	if st.FillN > 0 {
		st.MeanPost /= float64(st.FillN)
		st.MeanFeePC /= float64(st.FillN)
	}
	if !first.IsZero() {
		coverageStart := utcDay(first)
		if !first.Equal(coverageStart) {
			coverageStart = coverageStart.AddDate(0, 0, 1)
		}
		currentDay := utcDay(time.Now().UTC())
		proofStart := currentDay.AddDate(0, 0, -30)
		if coverageStart.After(proofStart) {
			proofStart = coverageStart
		}
		if currentDay.After(proofStart) {
			st.MatureUTCBlocks = int(currentDay.Sub(proofStart).Hours() / 24)
		}
		activeMeans := make([]float64, 0, st.MatureUTCBlocks)
		calendarNet := make([]float64, 0, st.MatureUTCBlocks)
		proofPost, proofFee, proofFills := 0.0, 0.0, 0
		for _, r := range rows {
			if r.Open || !r.Filled || r.OpenedTS.Before(proofStart) || !r.OpenedTS.Before(currentDay) {
				continue
			}
			if fee, known := feeFn(r); known && !math.IsNaN(fee) && !math.IsInf(fee, 0) && fee > -1 && fee < 1 {
				proofPost += r.PostPx
				proofFee += fee
				proofFills++
			}
		}
		for d := proofStart; d.Before(currentDay); d = d.AddDate(0, 0, 1) {
			events := dayEvents[d.Format("2006-01-02")]
			dayNet, dayMean := 0.0, 0.0
			for _, xs := range events {
				es := 0.0
				for _, x := range xs {
					es += x
					dayNet += x
				}
				dayMean += es / float64(len(xs))
				st.EventDayClusters++
			}
			calendarNet = append(calendarNet, dayNet)
			if len(events) > 0 {
				activeMeans = append(activeMeans, dayMean/float64(len(events)))
				st.ObservedUTCBlocks++
			}
		}
		if len(activeMeans) > 0 {
			for _, x := range activeMeans {
				st.DayMean += x
			}
			st.DayMean /= float64(len(activeMeans))
		}
		if proofFills > 0 {
			st.MeanPost = proofPost / float64(proofFills)
			st.MeanFeePC = proofFee / float64(proofFills)
		}
		key := st.Platform + "|" + st.Source + "|" + st.Family
		if lo, hi, ok := deterministicDayBounds(activeMeans, key+"|maker-edge"); ok {
			st.DayLo, st.DayHi = &lo, &hi
		}
		if lo, hi, ok := deterministicDayBounds(calendarNet, key+"|maker-rate"); ok {
			st.NetDayLo, st.NetDayHi = &lo, &hi
		}
	}
	return st
}

// makerRouteOriginLayer is derived from the route that actually created the attempt, not from a
// family-name guess. r139p is an accepted immutable research-system intent and r142x is that same
// system registry's Paper exploration route; every other maker source currently comes from the
// legacy model/detector placement path. Keep these cohorts separate so neither can borrow the
// other's evidence in the leaderboard.
func makerRouteOriginLayer(source string) string {
	if storage.ResearchPromotionIntentFromSource(source) != "" {
		return "strategy"
	}
	if _, ok := researchPaperExplorationRoute(source); ok {
		return "strategy"
	}
	return "model"
}

func (s *Server) makerRouteStat(ctx context.Context, platform, source, family string, inverted bool, side ...string) (makerRouteStat, error) {
	rows, err := s.store.MakerRouteAttempts(ctx, platform, source, family, inverted, side...)
	if err != nil {
		return makerRouteStat{}, err
	}
	return makerRouteStatFrom(rows, func(a storage.MakerRouteAttempt) (float64, bool) {
		return a.FeePC, a.FeeKnown && !math.IsNaN(a.FeePC) && !math.IsInf(a.FeePC, 0)
	}), nil
}

func makerRouteAdjusted(st makerRouteStat, currentPrice, currentFee float64) (state string, meanNow, loNow float64) {
	state, meanNow = "COLLECTING", st.DayMean
	if st.DayLo != nil {
		loNow = *st.DayLo
	}
	// A maker price comparison is charged in full, not multiplied by the historical fill rate.
	// That is deliberately conservative if the quote cancels; a worse current post can never gain
	// authorization from an optimistic assumption that it will be one of the non-fills.
	penalty := math.Max(0, currentPrice+currentFee-(st.MeanPost+st.MeanFeePC))
	meanNow -= penalty
	loNow -= penalty
	mature := st.N >= 20 && st.Open == 0 && st.Invalid == 0 && st.MatureUTCBlocks >= 30 &&
		st.ObservedUTCBlocks >= 20 && st.EventDayClusters >= 20 && st.DayLo != nil &&
		st.DayHi != nil && st.NetDayLo != nil && st.NetDayHi != nil
	if mature && *st.NetDayLo > 0 && loNow > 0 {
		state = "PROVEN+"
	} else if mature && *st.NetDayHi < 0 && *st.DayHi-penalty < 0 {
		state = "PROVEN-"
	}
	return state, meanNow, loNow
}

func mergeMakerRouteStat(a, b makerRouteStat) makerRouteStat {
	if a.N == 0 {
		return b
	}
	if b.N == 0 {
		return a
	}
	fillN := a.FillN + b.FillN
	out := makerRouteStat{
		Platform: a.Platform, Family: a.Family, OriginLayer: a.OriginLayer,
		Inverted: a.Inverted, Side: a.Side,
		FillN: fillN, Open: a.Open + b.Open, Invalid: a.Invalid + b.Invalid,
		SettledRows: a.SettledRows + b.SettledRows, marketRows: map[string][]float64{},
	}
	for key, values := range a.marketRows {
		out.marketRows[key] = append(out.marketRows[key], values...)
	}
	for key, values := range b.marketRows {
		out.marketRows[key] = append(out.marketRows[key], values...)
	}
	marketMeans := make([]float64, 0, len(out.marketRows))
	for _, values := range out.marketRows {
		if n, mean, _ := meanSD(values); n > 0 {
			marketMeans = append(marketMeans, mean)
		}
	}
	out.N, out.Mean, out.SD = meanSD(marketMeans)
	out.Markets = out.N
	if fillN > 0 {
		out.MeanPost = (a.MeanPost*float64(a.FillN) + b.MeanPost*float64(b.FillN)) / float64(fillN)
		out.MeanFeePC = (a.MeanFeePC*float64(a.FillN) + b.MeanFeePC*float64(b.FillN)) / float64(fillN)
	}
	// Distinct raw sources can have different opportunity clocks. Do not fabricate a clustered
	// bound by algebraically merging their row-level moments; the aggregate remains descriptive.
	return out
}

// makerRouteVerdicts exposes one scoreboard/API row per canonical family+venue+origin maker route. Exact
// source separation is retained for LIVE authorization above; the display aggregates aliases of
// the same canonical strategy so it stays parallel with signal:<family> and taker:<family>@venue.
// No automatic invert twin is spawned because changing side can also change maker fill probability.
func (s *Server) makerRouteVerdicts(ctx context.Context) []verdictEnt {
	if s.store == nil {
		return nil
	}
	keys, err := s.store.ListMakerRouteKeys(ctx)
	if err != nil {
		return nil
	}
	byFamilyVenue := make(map[string]makerRouteStat, len(keys))
	for _, k := range keys {
		st, err := s.makerRouteStat(ctx, k.Platform, k.Source, k.Family, k.Inverted, k.Side)
		if err != nil || st.N == 0 {
			continue
		}
		key := strings.ToLower(k.Platform) + "|" + k.Family + "|" + strings.ToUpper(k.Side) + "|" + st.OriginLayer
		byFamilyVenue[key] = mergeMakerRouteStat(byFamilyVenue[key], st)
	}
	out := make([]verdictEnt, 0, len(byFamilyVenue))
	for _, st := range byFamilyVenue {
		if st.Invalid != 0 {
			continue
		}
		v := verdictFrom("maker:"+st.Family+"@"+strings.ToLower(st.Platform), "maker", "$/attempt",
			st.N, st.Mean, st.SD, st.MeanFeePC, 0)
		v.Markets, v.SettledRows = st.Markets, st.SettledRows
		v.SourceFamily = strings.TrimSpace(st.Family)
		v.Platform = strings.ToLower(strings.TrimSpace(st.Platform))
		v.OriginLayer = st.OriginLayer
		v.Route = "maker"
		v.Side = st.Side
		v.MeanAsk, v.PaperPointMeanAsk = st.MeanPost, st.MeanPost
		v.PaperPointMean, v.PaperPointMeanFeePC = st.Mean, st.MeanFeePC
		state, mean, lo := makerRouteAdjusted(st, st.MeanPost, st.MeanFeePC)
		v.State, v.Mean, v.Lo = state, mean, lo
		if st.DayHi != nil {
			v.Hi = *st.DayHi
		}
		v.Invert = 0 // maker-side inversion needs its own observed route, never algebraic reuse
		out = append(out, v)
	}
	return out
}

// nativeMakerSystemRouteCurrentPoint requires a real settled maker-attempt cell for this exact
// family, venue and side, then charges any adverse current post/fee movement in full. It is used
// only after the source System's normal maker order has passed every placement gate; it cannot
// originate an order or borrow a taker/signal result as maker proof.
func (s *Server) nativeMakerSystemRouteCurrentPoint(source, platform, side string,
	currentPost, currentFeeTotal, contracts float64) (float64, bool) {
	family := strings.ToLower(strings.TrimSpace(policySourceFamily(source)))
	platform = strings.ToLower(strings.TrimSpace(platform))
	side = strings.ToUpper(strings.TrimSpace(side))
	if family == "" || (platform != "kalshi" && platform != "polyus") ||
		(side != "YES" && side != "NO") || currentPost <= 0 || currentPost >= 1 || contracts <= 0 {
		return 0, false
	}
	s.verdMu.Lock()
	cache, at := append([]verdictEnt(nil), s.verdCache...), s.verdAt
	s.verdMu.Unlock()
	if len(cache) == 0 || at.IsZero() || time.Since(at) > swStale {
		return 0, false
	}
	best, found := 0.0, false
	for _, v := range cache {
		if v.Group != "maker" || strings.ToLower(strings.TrimSpace(v.SourceFamily)) != family ||
			strings.ToLower(strings.TrimSpace(v.Platform)) != platform ||
			strings.ToUpper(strings.TrimSpace(v.Side)) != side ||
			strings.ToLower(strings.TrimSpace(v.Route)) != "maker" || v.N <= 0 ||
			v.Mean <= 0 || v.MeanAsk <= 0 || v.MeanAsk >= 1 {
			continue
		}
		adjusted, ok := gfCurrentRouteEdge(v.Mean, v.MeanAsk, currentPost,
			v.FeePC, currentFeeTotal, contracts)
		if !ok || adjusted <= 0 {
			continue
		}
		if !found || adjusted > best {
			best, found = adjusted, true
		}
	}
	return best, found
}
