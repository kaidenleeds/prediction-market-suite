package server

// behavioral-bias-regime is a read-only meta-system over the prospective book-v1 signal tape.
// It can discover calibration and behavioral regularities, but it has no funding, sizing, paper,
// placement, policy, unit-trial, AUTO, ARM, or live path.

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchstats"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const behavioralBiasState = "RESEARCH_ONLY_NONPROMOTABLE"

const behavioralBiasCacheTTL = 5 * time.Minute

type behavioralBiasCache struct {
	mu      sync.Mutex
	builtAt time.Time
	body    []byte
}

var behavioralBiasCacheByServer sync.Map // *Server -> *behavioralBiasCache

func behavioralBiasServerCache(s *Server) *behavioralBiasCache {
	v, _ := behavioralBiasCacheByServer.LoadOrStore(s, &behavioralBiasCache{})
	return v.(*behavioralBiasCache)
}

type behavioralBiasMaturityCoverage struct {
	storage.BehavioralBiasCoverage
	CompleteDayEligible int `json:"complete_day_eligible"`
	CurrentDayExcluded  int `json:"excluded_current_utc_day"`
	FutureExcluded      int `json:"excluded_future_timestamp"`
}

type behavioralBiasMetrics struct {
	Observations                   int      `json:"observations"`
	UniqueTickers                  int      `json:"unique_tickers"`
	UniqueCompletedUTCDays         int      `json:"unique_completed_utc_days"`
	ElapsedCompleteUTCDays         int      `json:"elapsed_complete_utc_days_including_zero_days"`
	ExactElapsedSeconds            float64  `json:"exact_elapsed_seconds"`
	MidpointCoverage               float64  `json:"bid_ask_midpoint_coverage"`
	MeanCalibrationImplied         float64  `json:"mean_calibration_implied"`
	MeanExecutableAsk              float64  `json:"mean_executable_ask"`
	MeanRealized                   float64  `json:"mean_realized"`
	CalibrationResidual            float64  `json:"calibration_residual_realized_minus_implied"`
	Brier                          float64  `json:"brier"`
	MeanExactTakerFeeCents         float64  `json:"mean_exact_taker_fee_cents_per_share"`
	MeanExecutableNetCentsPerShare float64  `json:"mean_executable_fee_net_cents_per_share"`
	TotalOneShareNetDollars        float64  `json:"total_one_share_net_dollars"`
	ExactOneShareNetPerDay         float64  `json:"exact_one_share_net_per_day"`
	OOSNetPerDayLower              *float64 `json:"oos_one_share_net_per_day_lower,omitempty"`
	OpportunitiesPerDay            float64  `json:"opportunities_per_day"`
	MeanAskDepth                   float64  `json:"mean_ask_depth_shares"`
	MedianAskDepth                 float64  `json:"median_ask_depth_shares"`
	MinimumAskDepth                float64  `json:"minimum_ask_depth_shares"`
	VisibleDepthSharesPerDay       float64  `json:"mechanical_visible_depth_shares_per_day"`
}

type behavioralBiasCohort struct {
	System                string                `json:"system"`
	State                 string                `json:"state"`
	ResearchOnly          bool                  `json:"research_only"`
	Promotable            bool                  `json:"promotable"`
	Bias                  string                `json:"bias"`
	Bin                   string                `json:"predeclared_bin"`
	SourceSignal          string                `json:"source_signal"`
	ObservedLane          string                `json:"observed_lane"`
	Venue                 string                `json:"venue"`
	Category              string                `json:"category"`
	Phase                 string                `json:"phase"`
	TimeToResolution      string                `json:"time_to_resolution"`
	PriceBand             string                `json:"executable_ask_price_band"`
	MarketKind            string                `json:"market_kind"`
	Route                 string                `json:"route"`
	SplitRule             string                `json:"walk_forward_split"`
	Train                 behavioralBiasMetrics `json:"train"`
	Validation            behavioralBiasMetrics `json:"validation"`
	Test                  behavioralBiasMetrics `json:"test"`
	ShrunkTestNetPerDay   *float64              `json:"hierarchical_test_net_per_day,omitempty"`
	ShrunkTestLowerPerDay *float64              `json:"hierarchical_test_lower_per_day,omitempty"`
	ShrinkageWeight       *float64              `json:"hierarchical_weight,omitempty"`
	HolmAdjustedP         *float64              `json:"holm_adjusted_p,omitempty"`
	BYAdjustedQ           *float64              `json:"benjamini_yekutieli_q,omitempty"`
}

type behavioralBiasResponse struct {
	System                string                         `json:"system"`
	State                 string                         `json:"state"`
	ResearchOnly          bool                           `json:"research_only"`
	Promotable            bool                           `json:"promotable"`
	LiveAuthority         bool                           `json:"live_authority"`
	PaperAuthority        bool                           `json:"paper_authority"`
	AsOf                  string                         `json:"as_of"`
	MaturityCutoff        string                         `json:"maturity_cutoff"`
	Pricing               string                         `json:"pricing"`
	Economics             string                         `json:"economics"`
	Deduplication         string                         `json:"deduplication"`
	ProofRule             string                         `json:"proof_rule"`
	MultipleTesting       string                         `json:"multiple_testing_limitation"`
	HierarchicalShrinkage string                         `json:"hierarchical_shrinkage_limitation"`
	CapacityCaveat        string                         `json:"capacity_caveat"`
	Coverage              behavioralBiasMaturityCoverage `json:"coverage"`
	Cohorts               []behavioralBiasCohort         `json:"cohorts"`
}

type behavioralBiasMembership struct {
	Bias, Bin string
}

type behavioralBiasSample struct {
	Obs     storage.BehavioralBiasObservation
	Implied float64
}

type behavioralBiasKey struct {
	Bias, Bin, Signal, Lane, Venue, Category, Phase, Horizon, PriceBand, Kind string
}

func behavioralUTCDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func behavioralCleanDimension(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" {
		return "unknown"
	}
	if len(v) > 80 {
		v = v[:80]
	}
	return v
}

func behavioralPriceBand(p float64) string {
	switch {
	case p < .05:
		return "00-05c"
	case p < .15:
		return "05-15c"
	case p < .35:
		return "15-35c"
	case p < .65:
		return "35-65c"
	case p < .85:
		return "65-85c"
	case p < .95:
		return "85-95c"
	default:
		return "95-100c"
	}
}

func behavioralHorizon(hours float64) string {
	switch {
	case hours <= 0:
		return "unknown"
	case hours <= 1:
		return "0-1h"
	case hours <= 6:
		return "1-6h"
	case hours <= 24:
		return "6-24h"
	case hours <= 168:
		return "1-7d"
	default:
		return "7d+"
	}
}

func behavioralPhase(o storage.BehavioralBiasObservation) string {
	// Auditor run 66 proved the historical Kalshi is_live flag called tens of thousands of known
	// future events live. A known positive start clock therefore wins unconditionally. A known
	// non-positive clock still needs contemporaneous live/activity evidence; otherwise it remains
	// unknown. This lets the research endpoint use old rows without silently trusting the bad flag.
	if o.SecsToStart != nil {
		if *o.SecsToStart > 0 {
			return "pre_event"
		}
		if o.IsLive == 1 || (o.InPlay != nil && *o.InPlay == 1) {
			return "live"
		}
		return "unknown"
	}
	if o.IsLive == 1 || (o.InPlay != nil && *o.InPlay == 1) {
		return "live"
	}
	if o.IsLive == 0 || (o.InPlay != nil && *o.InPlay == 0) {
		return "pre_event"
	}
	return "unknown"
}

func behavioralObservedLane(signal string) string {
	f := strings.ToLower(strings.TrimSpace(signal))
	if strings.HasPrefix(f, "invert:") || strings.HasPrefix(f, "xinv-") ||
		f == "freshfade" || f == "freshinv" || f == "xvgap2" {
		return "inverse_observed"
	}
	return "direct_observed"
}

func behavioralMomentumBin(cents float64) string {
	switch {
	case cents <= -5:
		return "against-5c+"
	case cents < -1:
		return "against-1-5c"
	case cents <= 1:
		return "flat-within-1c"
	case cents < 5:
		return "with-1-5c"
	default:
		return "with-5c+"
	}
}

func behavioralDivergenceFamily(f string) bool {
	switch f {
	case "xvgap", "xvgap2", "xvgapk", "xmatch", "pmatch", "pbridge", "fbridge",
		"pfbridge", "cross", "arb", "confluence", "sharpline", "xinv-pcrypto":
		return true
	default:
		return false
	}
}

func behavioralDivergenceBin(signal string, magnitude float64) string {
	band := "gap-unmeasured"
	if magnitude > 0 && magnitude <= 1 {
		switch {
		case magnitude < .03:
			band = "gap-under-3c"
		case magnitude < .05:
			band = "gap-3-5c"
		case magnitude < .10:
			band = "gap-5-10c"
		default:
			band = "gap-10c+"
		}
	}
	return signal + "/" + band
}

func behavioralWhaleBin(o storage.BehavioralBiasObservation) (string, bool) {
	f := strings.ToLower(o.SignalType)
	if o.TraderCount >= 5 {
		return "multi-whale-5+", true
	}
	if o.TraderCount >= 2 {
		return "multi-whale-2-4", true
	}
	if o.HoldersHHI != nil && *o.HoldersHHI >= .25 {
		return "concentrated-holders-hhi25+", true
	}
	if o.FlowRatio15m != nil {
		aligned := *o.FlowRatio15m
		if o.Side == "NO" {
			aligned = 1 - aligned
		}
		if aligned >= .70 {
			return "aligned-taker-herd-70+", true
		}
		if aligned <= .30 {
			return "opposing-taker-herd-70+", true
		}
	}
	if strings.Contains(f, "whale") {
		return "observed-whale-print", true
	}
	if strings.Contains(f, "flow") && o.Strength >= .70 && o.Strength <= 1 {
		return "one-sided-flow-70+", true
	}
	return "", false
}

// behavioralPathVolatility uses only values already present before the signal timestamp. It
// reports root-mean-square consecutive moves in cents; malformed, out-of-range, or short paths do
// not become a fake quiet observation.
func behavioralPathVolatility(path string) (float64, bool) {
	var px []float64
	if strings.TrimSpace(path) == "" || json.Unmarshal([]byte(path), &px) != nil || len(px) < 3 {
		return 0, false
	}
	scale := 1.0
	maxAbs := 0.0
	for _, p := range px {
		if math.IsNaN(p) || math.IsInf(p, 0) || p < 0 {
			return 0, false
		}
		maxAbs = math.Max(maxAbs, math.Abs(p))
	}
	if maxAbs <= 1.0000001 {
		scale = 100
	} else if maxAbs > 100.0000001 {
		return 0, false
	}
	sum := 0.0
	for i := 1; i < len(px); i++ {
		d := (px[i] - px[i-1]) * scale
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(px)-1)), true
}

func behavioralVolatilityBin(cents float64) string {
	switch {
	case cents < 1:
		return "quiet-rms-under-1c"
	case cents < 3:
		return "normal-rms-1-3c"
	default:
		return "shock-rms-3c+"
	}
}

func behavioralMemberships(o storage.BehavioralBiasObservation) []behavioralBiasMembership {
	out := []behavioralBiasMembership{{Bias: "favorite-longshot", Bin: behavioralPriceBand(o.Ask)}}
	if o.Mom1h != nil {
		aligned := *o.Mom1h // mom_1h is YES-space cents; orient it to the side actually bought.
		if o.Side == "NO" {
			aligned = -aligned
		}
		out = append(out, behavioralBiasMembership{Bias: "hot-streak-momentum", Bin: behavioralMomentumBin(aligned)})
	}
	if bin, ok := behavioralWhaleBin(o); ok {
		out = append(out, behavioralBiasMembership{Bias: "whale-herding", Bin: bin})
	}
	f := strings.ToLower(strings.TrimSpace(o.SignalType))
	if strings.Contains(f, "fresh") {
		out = append(out, behavioralBiasMembership{Bias: "fresh-list-anchoring", Bin: f})
	}
	if behavioralDivergenceFamily(f) {
		out = append(out, behavioralBiasMembership{Bias: "cross-venue-divergence", Bin: behavioralDivergenceBin(f, math.Abs(o.Strength))})
	}
	if vol, ok := behavioralPathVolatility(o.PricePath); ok {
		out = append(out, behavioralBiasMembership{Bias: "volatility-shock", Bin: behavioralVolatilityBin(vol)})
	}
	if o.BookImb3 != nil {
		aligned := *o.BookImb3 // top-three YES bid share, oriented to the side bought.
		if o.Side == "NO" {
			aligned = 1 - aligned
		}
		bin := "balanced-35-65"
		if aligned < .35 {
			bin = "liquidity-against-under-35"
		} else if aligned > .65 {
			bin = "liquidity-with-over-65"
		}
		out = append(out, behavioralBiasMembership{Bias: "liquidity-imbalance", Bin: bin})
	}
	return out
}

func behavioralBiasLower(dayNet []float64, key string) *float64 {
	if len(dayNet) < 2 {
		return nil
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	x := h.Sum64()
	if x == 0 {
		x = 1
	}
	const reps = 4096
	means := make([]float64, reps)
	for b := range means {
		sum := 0.0
		for range dayNet {
			x = x*6364136223846793005 + 1442695040888963407
			sum += dayNet[int(x%uint64(len(dayNet)))]
		}
		means[b] = sum / float64(len(dayNet))
	}
	sort.Float64s(means)
	v := means[reps/20]
	return &v
}

func summarizeBehavioralBias(samples []behavioralBiasSample, start, end time.Time, lowerKey string) behavioralBiasMetrics {
	var m behavioralBiasMetrics
	if len(samples) == 0 || !end.After(start) {
		return m
	}
	m.Observations = len(samples)
	m.ExactElapsedSeconds = end.Sub(start).Seconds()
	m.ElapsedCompleteUTCDays = int(m.ExactElapsedSeconds / (24 * 60 * 60))
	if m.ElapsedCompleteUTCDays < 1 {
		return behavioralBiasMetrics{}
	}
	tickers, observedDays := map[string]bool{}, map[string]bool{}
	depths := make([]float64, 0, len(samples))
	dayNet := make([]float64, m.ElapsedCompleteUTCDays)
	calibrationSum, askSum, realizedSum, brierSum := 0.0, 0.0, 0.0, 0.0
	feeSum, netSum, depthSum, visibleDepthSum := 0.0, 0.0, 0.0, 0.0
	midN := 0
	for _, s := range samples {
		o := s.Obs
		tickers[o.Venue+"|"+o.Ticker] = true
		observedDays[behavioralUTCDay(o.ObservedAt).Format("2006-01-02")] = true
		implied := s.Implied
		if o.BidKnown {
			midN++
		}
		calibrationSum += implied
		askSum += o.Ask
		realizedSum += o.Payout
		d := implied - o.Payout
		brierSum += d * d
		feeSum += o.TakerFeePC
		net := o.Payout - o.Ask - o.TakerFeePC
		netSum += net
		dayIdx := int(behavioralUTCDay(o.ObservedAt).Sub(start).Hours() / 24)
		if dayIdx >= 0 && dayIdx < len(dayNet) {
			dayNet[dayIdx] += net
		}
		depths = append(depths, o.AskDepth)
		depthSum += o.AskDepth
		visibleDepthSum += math.Floor(o.AskDepth)
	}
	n := float64(len(samples))
	m.UniqueTickers, m.UniqueCompletedUTCDays = len(tickers), len(observedDays)
	m.MidpointCoverage = float64(midN) / n
	m.MeanCalibrationImplied = calibrationSum / n
	m.MeanExecutableAsk = askSum / n
	m.MeanRealized = realizedSum / n
	m.CalibrationResidual = m.MeanRealized - m.MeanCalibrationImplied
	m.Brier = brierSum / n
	m.MeanExactTakerFeeCents = 100 * feeSum / n
	m.MeanExecutableNetCentsPerShare = 100 * netSum / n
	m.TotalOneShareNetDollars = netSum
	days := m.ExactElapsedSeconds / 86400
	m.ExactOneShareNetPerDay = netSum / days
	m.OpportunitiesPerDay = n / days
	m.MeanAskDepth = depthSum / n
	sort.Float64s(depths)
	if len(depths)%2 == 1 {
		m.MedianAskDepth = depths[len(depths)/2]
	} else {
		m.MedianAskDepth = (depths[len(depths)/2-1] + depths[len(depths)/2]) / 2
	}
	m.MinimumAskDepth = depths[0]
	m.VisibleDepthSharesPerDay = visibleDepthSum / days
	m.OOSNetPerDayLower = behavioralBiasLower(dayNet, lowerKey)
	return m
}

func behavioralDayMoments(samples []behavioralBiasSample, start, end time.Time) (mean, variance, oneSidedP float64, n int, ok bool) {
	n = int(end.Sub(start).Seconds() / 86400)
	if n < 2 {
		return 0, 0, 1, n, false
	}
	days := make([]float64, n) // quiet completed days remain explicit zeroes
	for _, sample := range samples {
		idx := int(behavioralUTCDay(sample.Obs.ObservedAt).Sub(start).Hours() / 24)
		if idx >= 0 && idx < len(days) {
			days[idx] += sample.Obs.Payout - sample.Obs.Ask - sample.Obs.TakerFeePC
		}
	}
	for _, v := range days {
		mean += v
	}
	mean /= float64(n)
	for _, v := range days {
		variance += (v - mean) * (v - mean)
	}
	variance /= float64(n - 1)
	if variance <= 0 {
		if mean > 0 {
			return mean, 0, 0, n, true
		}
		return mean, 0, 1, n, true
	}
	z := mean / math.Sqrt(variance/float64(n))
	oneSidedP = .5 * math.Erfc(z/math.Sqrt2)
	return mean, variance, oneSidedP, n, true
}

func behavioralBiasReport(rows []storage.BehavioralBiasObservation, base storage.BehavioralBiasCoverage, asOf time.Time) behavioralBiasResponse {
	asOf = asOf.UTC()
	cutoff := behavioralUTCDay(asOf)
	resp := behavioralBiasResponse{
		System: "behavioral-bias-regime", State: behavioralBiasState, ResearchOnly: true,
		Promotable: false, LiveAuthority: false, PaperAuthority: false,
		AsOf: asOf.Format(time.RFC3339Nano), MaturityCutoff: cutoff.Format(time.RFC3339),
		Pricing:               "calibration uses the side-specific bid/ask midpoint when both were captured (ask otherwise); economics always pays the captured bought-side executable ask",
		Economics:             "one share per earliest ticker/cohort/UTC-day observation; exact stored taker fee and settlement; no balance, sizing, funding, or price proxy",
		Deduplication:         "earliest qualifying observation per venue+ticker+cohort+completed UTC day; a whole UTC day stays on one side of the chronological split",
		ProofRule:             "calibration can discover a bias, but only a future untouched OOS executable lower-bound Net/day could support profit review; this endpoint can never promote or authorize orders",
		MultipleTesting:       "within this frozen report, one-sided day-block tests receive Holm family-wise and Benjamini-Yekutieli arbitrary-dependence corrections; untouched replication is still mandatory",
		HierarchicalShrinkage: "test Net/day cells receive empirical-Bayes partial pooling toward the global mean; sparse cells shrink most and the output remains descriptive/nonpromotable",
		CapacityCaveat:        "visible depth is a contemporaneous mechanical ceiling, not fillable recurring capacity; consuming depth, impact, overlap, queueing, and signal correlation are not modeled",
		Coverage:              behavioralBiasMaturityCoverage{BehavioralBiasCoverage: base},
		Cohorts:               []behavioralBiasCohort{},
	}

	groups := map[behavioralBiasKey]map[string]behavioralBiasSample{}
	for _, o := range rows {
		if o.ObservedAt.After(asOf) || o.ResolvedAt.After(asOf) {
			resp.Coverage.FutureExcluded++
			continue
		}
		if !o.ObservedAt.Before(cutoff) || !o.ResolvedAt.Before(cutoff) {
			resp.Coverage.CurrentDayExcluded++
			continue
		}
		resp.Coverage.CompleteDayEligible++
		implied := o.Ask
		if o.BidKnown {
			implied = (o.Bid + o.Ask) / 2
		}
		category := behavioralCleanDimension(o.Category)
		kind := behavioralCleanDimension(o.Kind)
		if kind == "unknown" {
			kind = behavioralCleanDimension(o.MarketType)
		}
		for _, membership := range behavioralMemberships(o) {
			key := behavioralBiasKey{Bias: membership.Bias, Bin: membership.Bin,
				Signal: behavioralCleanDimension(o.SignalType), Lane: behavioralObservedLane(o.SignalType),
				Venue: behavioralCleanDimension(o.Venue), Category: category, Phase: behavioralPhase(o),
				Horizon: behavioralHorizon(o.ResolveHours), PriceBand: behavioralPriceBand(o.Ask), Kind: kind}
			if groups[key] == nil {
				groups[key] = map[string]behavioralBiasSample{}
			}
			dayTicker := o.Venue + "|" + o.Ticker + "|" + behavioralUTCDay(o.ObservedAt).Format("2006-01-02")
			prior, exists := groups[key][dayTicker]
			if !exists || o.ObservedAt.Before(prior.Obs.ObservedAt) ||
				(o.ObservedAt.Equal(prior.Obs.ObservedAt) && o.ID < prior.Obs.ID) {
				groups[key][dayTicker] = behavioralBiasSample{Obs: o, Implied: implied}
			}
		}
	}

	keys := make([]behavioralBiasKey, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		return a.Bias+"|"+a.Bin+"|"+a.Signal+"|"+a.Lane+"|"+a.Venue+"|"+a.Category+"|"+a.Phase+"|"+a.Horizon+"|"+a.PriceBand+"|"+a.Kind <
			b.Bias+"|"+b.Bin+"|"+b.Signal+"|"+b.Lane+"|"+b.Venue+"|"+b.Category+"|"+b.Phase+"|"+b.Horizon+"|"+b.PriceBand+"|"+b.Kind
	})
	cellEstimates := make([]researchstats.CellEstimate, 0, len(keys))
	pTests := make([]researchstats.TestP, 0, len(keys))
	for _, key := range keys {
		samples := make([]behavioralBiasSample, 0, len(groups[key]))
		for _, sample := range groups[key] {
			samples = append(samples, sample)
		}
		sort.Slice(samples, func(i, j int) bool {
			if samples[i].Obs.ObservedAt.Equal(samples[j].Obs.ObservedAt) {
				return samples[i].Obs.ID < samples[j].Obs.ID
			}
			return samples[i].Obs.ObservedAt.Before(samples[j].Obs.ObservedAt)
		})
		var train, validation, test []behavioralBiasSample
		clusterRows := make([]researchstats.Observation, 0, len(samples))
		for _, sample := range samples {
			eventID := strings.TrimSpace(sample.Obs.CanonicalEvent)
			if eventID == "" {
				eventID = sample.Obs.Venue + ":" + sample.Obs.Ticker
			}
			clusterRows = append(clusterRows, researchstats.Observation{EventID: eventID,
				Day: sample.Obs.ObservedAt, Value: sample.Obs.Payout - sample.Obs.Ask - sample.Obs.TakerFeePC})
		}
		split, splitErr := researchstats.ChronologicalEventSplit(clusterRows, .60, .20, 1)
		if splitErr != nil {
			train = samples // insufficient event history remains training-only, never pseudo-OOS
		} else {
			trainIDs, validationIDs, testIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}
			for _, id := range split.TrainEvents {
				trainIDs[id] = true
			}
			for _, id := range split.ValidationEvents {
				validationIDs[id] = true
			}
			for _, id := range split.TestEvents {
				testIDs[id] = true
			}
			for _, sample := range samples {
				eventID := strings.TrimSpace(sample.Obs.CanonicalEvent)
				if eventID == "" {
					eventID = sample.Obs.Venue + ":" + sample.Obs.Ticker
				}
				switch {
				case trainIDs[eventID]:
					train = append(train, sample)
				case validationIDs[eventID]:
					validation = append(validation, sample)
				case testIDs[eventID]:
					test = append(test, sample)
				}
			}
		}
		window := func(part []behavioralBiasSample) (time.Time, time.Time) {
			if len(part) == 0 {
				return cutoff, cutoff
			}
			lo, hi := behavioralUTCDay(part[0].Obs.ObservedAt), behavioralUTCDay(part[0].Obs.ObservedAt).Add(24*time.Hour)
			for _, sample := range part[1:] {
				d := behavioralUTCDay(sample.Obs.ObservedAt)
				if d.Before(lo) {
					lo = d
				}
				if d.Add(24 * time.Hour).After(hi) {
					hi = d.Add(24 * time.Hour)
				}
			}
			return lo, hi
		}
		trainStart, trainEnd := window(train)
		validationStart, validationEnd := window(validation)
		testStart, testEnd := window(test)
		id := strings.Join([]string{"behavioral-bias-regime", key.Bias, key.Bin, key.Signal,
			key.Lane, key.Venue, key.Category, key.Phase, key.Horizon, key.PriceBand, key.Kind}, "/")
		trainMetrics := summarizeBehavioralBias(train, trainStart, trainEnd, id+"|train")
		trainMetrics.OOSNetPerDayLower = nil // the training lane is not out-of-sample
		validationMetrics := summarizeBehavioralBias(validation, validationStart, validationEnd, id+"|validation")
		validationMetrics.OOSNetPerDayLower = nil // model/cell selection happens here, never proof
		testMetrics := summarizeBehavioralBias(test, testStart, testEnd, id+"|rolling-final-slice")
		resp.Cohorts = append(resp.Cohorts, behavioralBiasCohort{
			System: id, State: behavioralBiasState, ResearchOnly: true, Promotable: false,
			Bias: key.Bias, Bin: key.Bin, SourceSignal: key.Signal, ObservedLane: key.Lane,
			Venue: key.Venue, Category: key.Category, Phase: key.Phase,
			TimeToResolution: key.Horizon, PriceBand: key.PriceBand, MarketKind: key.Kind,
			Route: "taker", SplitRule: "canonical-event chronological 60/20/20 train/validation/rolling-final slice with one-day purge/embargo; recomputed walk-forward and never immutable promotion proof",
			Train: trainMetrics, Validation: validationMetrics, Test: testMetrics,
		})
		if mean, variance, p, n, ok := behavioralDayMoments(test, testStart, testEnd); ok {
			cellEstimates = append(cellEstimates, researchstats.CellEstimate{Cell: id, N: n, Mean: mean, Variance: variance})
			pTests = append(pTests, researchstats.TestP{ID: id, P: p})
		}
	}
	cohortIndex := make(map[string]int, len(resp.Cohorts))
	for i := range resp.Cohorts {
		cohortIndex[resp.Cohorts[i].System] = i
	}
	if shrunk, err := researchstats.ShrinkNormalCells(cellEstimates, 1e-8); err == nil {
		for _, cell := range shrunk.Cells {
			if i, ok := cohortIndex[cell.Cell]; ok {
				mean, lower, weight := cell.PosteriorMean, cell.Lower95, cell.ShrinkageWeight
				resp.Cohorts[i].ShrunkTestNetPerDay = &mean
				resp.Cohorts[i].ShrunkTestLowerPerDay = &lower
				resp.Cohorts[i].ShrinkageWeight = &weight
			}
		}
	}
	if adjusted, err := researchstats.AdjustDependenceSafe(pTests); err == nil {
		for _, test := range adjusted {
			if i, ok := cohortIndex[test.ID]; ok {
				holm, by := test.HolmP, test.BYQ
				resp.Cohorts[i].HolmAdjustedP, resp.Cohorts[i].BYAdjustedQ = &holm, &by
			}
		}
	}
	return resp
}

func (s *Server) behavioralBiasPayload(ctx context.Context) ([]byte, bool, error) {
	cache := behavioralBiasServerCache(s)
	cache.mu.Lock()
	defer cache.mu.Unlock()
	now := time.Now()
	sameMaturityDay := behavioralUTCDay(now.UTC()).Equal(behavioralUTCDay(cache.builtAt.UTC()))
	if len(cache.body) > 0 && sameMaturityDay && !now.Before(cache.builtAt) && now.Sub(cache.builtAt) < behavioralBiasCacheTTL {
		return cache.body, true, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	rows, coverage, err := s.store.BehavioralBiasProspectiveObservations(ctx)
	if err != nil {
		return nil, false, err
	}
	body, err := json.Marshal(behavioralBiasReport(rows, coverage, time.Now().UTC()))
	if err != nil {
		return nil, false, err
	}
	body = append(body, '\n') // preserve the prior json.Encoder wire shape
	cache.body, cache.builtAt = body, time.Now()
	return cache.body, false, nil
}

func (s *Server) handleBehavioralBiasRegime(w http.ResponseWriter, r *http.Request) {
	body, hit, err := s.behavioralBiasPayload(r.Context())
	if err != nil {
		http.Error(w, "behavioral-bias research unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if hit {
		w.Header().Set("X-Behavioral-Bias-Cache", "hit")
	} else {
		w.Header().Set("X-Behavioral-Bias-Cache", "miss")
	}
	_, _ = w.Write(body)
}
