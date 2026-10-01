package server

// Adaptive whale/flow research. Raw venue tapes retain every valid positive print; this file
// decides only whether a print is unusual relative to its venue's current flow distribution. It
// has no signal insertion, paper, LIVE, AUTO, ARM, sizing, or order path.

import (
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

const (
	adaptiveWhaleSystem        = "adaptive-whale-scorer"
	adaptiveWhaleRawSystem     = "raw-flow-observer"
	adaptiveWhaleWindow        = 30 * time.Minute
	adaptiveWhaleMinBaselineN  = 8
	adaptiveWhaleReceiptCap    = 200
	adaptiveWhaleVenueScoreCap = 600
	adaptiveWhaleBookMaxAge    = 5 * time.Second
)

type adaptiveWhaleCalibration struct {
	Mature          bool
	Executable      bool
	Outcomes        int
	UTCBlocks       int
	LowerNetPerUnit float64
}

type adaptiveWhaleBaseline struct {
	N                 int     `json:"n"`
	Median            float64 `json:"median_notional"`
	MAD               float64 `json:"mad_notional"`
	RelativeThreshold float64 `json:"relative_threshold"`
	Mature            bool    `json:"mature"`
}

type adaptiveWhaleInput struct {
	Venue, Market, Side, Action, Wallet string
	Notional, Price                     float64
	ObservedAt, Now                     time.Time
	WindowNotionals                     []float64
	Baseline                            *adaptiveWhaleBaseline
	ResolveHours                        *float64
	Frequency                           *int
	WalletRank                          *int
	WalletPnL                           *float64
	WalletSkill                         *float64
	WalletAllocation                    *float64
	AgreementCount                      *int
	SLTPDiscipline                      *float64
	HotColdStreak                       *float64
	InsiderNovelty                      *float64
	SpreadCents                         *float64
	DepthUSD                            *float64
	BookVolatility                      *float64
	Calibration                         *adaptiveWhaleCalibration
}

type adaptiveWhaleFactor struct {
	Name         string  `json:"name"`
	Value        float64 `json:"value_0_100"`
	Weight       float64 `json:"weight_pct"`
	Contribution float64 `json:"score_contribution"`
	Available    bool    `json:"available"`
	Raw          float64 `json:"raw,omitempty"`
	Note         string  `json:"note"`
}

type adaptiveWhaleReceipt struct {
	System                   string                `json:"system"`
	State                    string                `json:"state"`
	Venue                    string                `json:"venue"`
	Market                   string                `json:"market"`
	Side                     string                `json:"side"`
	Action                   string                `json:"action"`
	Wallet                   string                `json:"wallet,omitempty"`
	Evidence                 string                `json:"direction_evidence"`
	Notional                 float64               `json:"notional"`
	Price                    float64               `json:"price"`
	ObservedAt               string                `json:"observed_at"`
	AgeSeconds               float64               `json:"age_seconds"`
	ResolveHours             *float64              `json:"resolve_hours,omitempty"`
	RecencyHalfLifeSeconds   float64               `json:"recency_half_life_seconds"`
	Baseline                 adaptiveWhaleBaseline `json:"relative_size_baseline"`
	VenueBaseFloor           float64               `json:"venue_base_floor"`
	PassBaseFloor            bool                  `json:"pass_base_floor"`
	PassRelativeSignificance bool                  `json:"pass_relative_significance"`
	AdaptiveMinScore         int                   `json:"adaptive_min_score"`
	PassScore                bool                  `json:"pass_score"`
	ScoreContinuous          float64               `json:"score_continuous"`
	ScoreRounded5            int                   `json:"score_rounded_nearest_5"`
	Qualifies                bool                  `json:"qualifies_for_research_cohort"`
	ResearchOnly             bool                  `json:"research_only"`
	PaperAuthority           bool                  `json:"paper_authority"`
	LiveAuthority            bool                  `json:"live_authority"`
	CalibrationApplied       bool                  `json:"mature_oos_calibration_applied"`
	Factors                  []adaptiveWhaleFactor `json:"factors"`
	MissingNeutralFactors    []string              `json:"missing_neutral_factors,omitempty"`
	QualificationLimitations []string              `json:"qualification_limitations,omitempty"`
}

type adaptiveWhaleRaw struct {
	Venue, Market, Side, Action, Wallet string
	Notional, Price                     float64
	At                                  time.Time
}

type adaptiveWhaleVenueStats struct {
	Venue           string                `json:"venue"`
	RawPositiveN    int                   `json:"raw_valid_positive_n"`
	Smallest        float64               `json:"smallest_notional,omitempty"`
	Largest         float64               `json:"largest_notional,omitempty"`
	Baseline        adaptiveWhaleBaseline `json:"relative_size_baseline"`
	ResearchPassN   int                   `json:"adaptive_research_pass_n"`
	RawDollarFilter float64               `json:"raw_observer_min_notional"`
}

func adaptiveWhaleVenuePolicy(venue string) (baseFloor float64, minScore int) {
	switch strings.ToLower(strings.TrimSpace(venue)) {
	case "polyus":
		return 10, 70
	case "polymarket", "polyint":
		return 25, 70
	case "kalshi":
		return 25, 70
	default:
		return 25, 75
	}
}

func adaptiveWhaleClamp(v, lo, hi float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return lo
	}
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func adaptiveWhaleMedian(sorted []float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func robustAdaptiveWhaleBaseline(notionals []float64, venue string) adaptiveWhaleBaseline {
	vals := make([]float64, 0, len(notionals))
	for _, v := range notionals {
		if v > 0 && !math.IsNaN(v) && !math.IsInf(v, 0) {
			vals = append(vals, v)
		}
	}
	sort.Float64s(vals)
	b := adaptiveWhaleBaseline{N: len(vals), Mature: len(vals) >= adaptiveWhaleMinBaselineN}
	b.Median = adaptiveWhaleMedian(vals)
	dev := make([]float64, len(vals))
	for i, v := range vals {
		dev[i] = math.Abs(v - b.Median)
	}
	sort.Float64s(dev)
	b.MAD = adaptiveWhaleMedian(dev)
	floor, _ := adaptiveWhaleVenuePolicy(venue)
	// The adaptive bar must beat both a multiple of ordinary flow and a robust tail estimate. A
	// nonzero venue floor remains even when the window is quiet.
	b.RelativeThreshold = math.Max(floor, math.Max(2*b.Median, b.Median+2.5*1.4826*b.MAD))
	return b
}

func adaptiveWhaleRecencyHalfLife(resolveHours *float64) time.Duration {
	if resolveHours == nil || *resolveHours <= 0 || math.IsNaN(*resolveHours) || math.IsInf(*resolveHours, 0) {
		return 15 * time.Minute
	}
	minutes := adaptiveWhaleClamp(*resolveHours*5, 2, 30)
	return time.Duration(minutes * float64(time.Minute))
}

func adaptiveWhaleRound5(v float64) int {
	return int(adaptiveWhaleClamp(math.Round(v/5)*5, 0, 100))
}

func adaptiveWhaleSigned(v float64) float64 {
	return 50 + 50*adaptiveWhaleClamp(v, -1, 1)
}

func adaptiveWhaleAddFactor(dst *[]adaptiveWhaleFactor, missing *[]string, name string, raw, value, weight float64, available bool, note string) {
	if !available {
		value = 50 // unavailable evidence is explicit and neutral, never silently bearish/bullish
		*missing = append(*missing, name)
	}
	value = adaptiveWhaleClamp(value, 0, 100)
	*dst = append(*dst, adaptiveWhaleFactor{Name: name, Raw: raw, Value: value, Weight: weight,
		Contribution: weight * value / 100, Available: available, Note: note})
}

// scoreAdaptiveWhale is pure and deterministic. Qualifies means only "retain in the research
// significance cohort"; the receipt permanently carries zero paper/live authority.
func scoreAdaptiveWhale(in adaptiveWhaleInput) adaptiveWhaleReceipt {
	now := in.Now
	if now.IsZero() {
		now = time.Unix(0, 0).UTC()
	}
	observed := in.ObservedAt
	if observed.IsZero() {
		observed = now
	}
	age := now.Sub(observed).Seconds()
	if age < 0 {
		age = 0
	}
	baseFloor, baseMinScore := adaptiveWhaleVenuePolicy(in.Venue)
	baseline := adaptiveWhaleBaseline{}
	if in.Baseline != nil {
		baseline = *in.Baseline
	} else {
		baseline = robustAdaptiveWhaleBaseline(in.WindowNotionals, in.Venue)
	}
	halfLife := adaptiveWhaleRecencyHalfLife(in.ResolveHours)
	recency := math.Pow(0.5, age/halfLife.Seconds())
	action := strings.ToUpper(strings.TrimSpace(in.Action))
	directionKnown := action == "BUY" || action == "SELL"
	evidence := "unknown"
	if action == "BUY" {
		evidence = "for " + strings.TrimSpace(in.Side)
	} else if action == "SELL" {
		evidence = "against " + strings.TrimSpace(in.Side)
	}

	factors := make([]adaptiveWhaleFactor, 0, 14)
	missing := make([]string, 0, 10)
	relAvailable := baseline.Mature && baseline.RelativeThreshold > 0
	relRatio := 0.0
	if relAvailable {
		relRatio = in.Notional / baseline.RelativeThreshold
	}
	relValue := 50.0
	if relAvailable && relRatio > 0 {
		relValue = 50 + 20*math.Log2(relRatio)
	}
	adaptiveWhaleAddFactor(&factors, &missing, "relative_size", relRatio, relValue, 24, relAvailable,
		"robust venue-window median/MAD; raw observation itself has no dollar filter")
	adaptiveWhaleAddFactor(&factors, &missing, "recency", recency, 100*recency, 12, true,
		"exponential decay with time-to-resolution-aware half-life")
	freqOK := in.Frequency != nil && *in.Frequency >= 0
	freqRaw, freqValue := 0.0, 50.0
	if freqOK {
		freqRaw = float64(*in.Frequency)
		freqValue = math.Min(100, 35*math.Sqrt(freqRaw))
	}
	adaptiveWhaleAddFactor(&factors, &missing, "frequency", freqRaw, freqValue, 8, freqOK,
		"same venue/market/side/action prints in the retained window")
	adaptiveWhaleAddFactor(&factors, &missing, "buy_sell_direction", 0, 100, 2, directionKnown,
		"BUY supports the named side; SELL is evidence against it and is never laundered into a buy")
	rankOK := in.WalletRank != nil && *in.WalletRank > 0
	rankRaw, rankValue := 0.0, 50.0
	if rankOK {
		rankRaw = float64(*in.WalletRank)
		rankValue = 100 / (1 + math.Log10(rankRaw))
	}
	adaptiveWhaleAddFactor(&factors, &missing, "wallet_rank", rankRaw, rankValue, 5, rankOK,
		"wallet venues only; anonymous venues remain neutral")
	pnlOK := in.WalletPnL != nil && !math.IsNaN(*in.WalletPnL) && !math.IsInf(*in.WalletPnL, 0)
	pnlRaw, pnlValue := 0.0, 50.0
	if pnlOK {
		pnlRaw = *in.WalletPnL
		pnlValue = 50 + 50*math.Tanh(pnlRaw/100000)
	}
	adaptiveWhaleAddFactor(&factors, &missing, "wallet_pnl", pnlRaw, pnlValue, 5, pnlOK,
		"bounded transform; raw lifetime dollars never dominate the score")
	skillOK := in.WalletSkill != nil && !math.IsNaN(*in.WalletSkill) && !math.IsInf(*in.WalletSkill, 0)
	skillRaw, skillValue := 0.0, 50.0
	if skillOK {
		skillRaw = *in.WalletSkill
		skillValue = adaptiveWhaleSigned(skillRaw)
	}
	adaptiveWhaleAddFactor(&factors, &missing, "wallet_skill", skillRaw, skillValue, 8, skillOK,
		"prospective shrunk skill in [-1,1], when available")
	allocOK := in.WalletAllocation != nil && *in.WalletAllocation >= 0 && !math.IsNaN(*in.WalletAllocation)
	allocRaw, allocValue := 0.0, 50.0
	if allocOK {
		allocRaw = adaptiveWhaleClamp(*in.WalletAllocation, 0, 1)
		allocValue = 100 * allocRaw
	}
	adaptiveWhaleAddFactor(&factors, &missing, "wallet_allocation", allocRaw, allocValue, 4, allocOK,
		"print share of observed wallet flow; not assumed account NAV")
	agreeOK := in.AgreementCount != nil && *in.AgreementCount >= 0
	agreeRaw, agreeValue := 0.0, 50.0
	if agreeOK {
		agreeRaw = float64(*in.AgreementCount)
		agreeValue = math.Min(100, 25*agreeRaw)
	}
	adaptiveWhaleAddFactor(&factors, &missing, "agreement_count", agreeRaw, agreeValue, 6, agreeOK,
		"independent matched sources agreeing in direction")
	sltpOK := in.SLTPDiscipline != nil && !math.IsNaN(*in.SLTPDiscipline)
	sltpRaw, sltpValue := 0.0, 50.0
	if sltpOK {
		sltpRaw, sltpValue = *in.SLTPDiscipline, adaptiveWhaleSigned(*in.SLTPDiscipline)
	}
	adaptiveWhaleAddFactor(&factors, &missing, "sl_tp_discipline", sltpRaw, sltpValue, 4, sltpOK,
		"historical stop/take-profit discipline, neutral when unobserved")
	streakOK := in.HotColdStreak != nil && !math.IsNaN(*in.HotColdStreak)
	streakRaw, streakValue := 0.0, 50.0
	if streakOK {
		streakRaw, streakValue = *in.HotColdStreak, adaptiveWhaleSigned(*in.HotColdStreak)
	}
	adaptiveWhaleAddFactor(&factors, &missing, "hot_cold_streak", streakRaw, streakValue, 4, streakOK,
		"bounded current wallet/system streak; never a standalone trade trigger")
	novelOK := in.InsiderNovelty != nil && *in.InsiderNovelty >= 0 && !math.IsNaN(*in.InsiderNovelty)
	novelRaw, novelValue := 0.0, 50.0
	if novelOK {
		novelRaw = adaptiveWhaleClamp(*in.InsiderNovelty, 0, 1)
		novelValue = 100 * novelRaw
	}
	adaptiveWhaleAddFactor(&factors, &missing, "insider_novelty", novelRaw, novelValue, 5, novelOK,
		"new information/position novelty when independently measurable")

	bookParts := make([]float64, 0, 3)
	bookRaw := 0.0
	if in.SpreadCents != nil && *in.SpreadCents >= 0 && !math.IsNaN(*in.SpreadCents) {
		bookParts = append(bookParts, adaptiveWhaleClamp(100-10**in.SpreadCents, 0, 100))
		bookRaw = *in.SpreadCents
	}
	if in.DepthUSD != nil && *in.DepthUSD >= 0 && !math.IsNaN(*in.DepthUSD) {
		den := math.Max(in.Notional, 0.01)
		bookParts = append(bookParts, adaptiveWhaleClamp(100**in.DepthUSD/den, 0, 100))
	}
	if in.BookVolatility != nil && *in.BookVolatility >= 0 && !math.IsNaN(*in.BookVolatility) {
		bookParts = append(bookParts, adaptiveWhaleClamp(100*(1-*in.BookVolatility/0.25), 0, 100))
	}
	bookValue := 50.0
	if len(bookParts) > 0 {
		bookValue = 0
		for _, v := range bookParts {
			bookValue += v
		}
		bookValue /= float64(len(bookParts))
	}
	adaptiveWhaleAddFactor(&factors, &missing, "book_liquidity_context", bookRaw, bookValue, 8, len(bookParts) > 0,
		"fresh spread, executable depth/notional, and book volatility when available")

	calApplied := in.Calibration != nil && in.Calibration.Mature && in.Calibration.Executable &&
		in.Calibration.Outcomes >= 20 && in.Calibration.UTCBlocks >= 30
	calRaw, calValue := 0.0, 50.0
	adaptiveMin := baseMinScore
	if calApplied {
		calRaw = in.Calibration.LowerNetPerUnit
		calValue = 50 + 500*calRaw
		adaptiveMin = adaptiveWhaleRound5(float64(baseMinScore) - 100*calRaw)
		if adaptiveMin < 60 {
			adaptiveMin = 60
		} else if adaptiveMin > 85 {
			adaptiveMin = 85
		}
	}
	adaptiveWhaleAddFactor(&factors, &missing, "mature_oos_calibration", calRaw, calValue, 5, calApplied,
		"applies only to prospective executable outcomes with >=20 outcomes and >=30 UTC blocks")

	score := 0.0
	for _, f := range factors {
		score += f.Contribution
	}
	rounded := adaptiveWhaleRound5(score)
	passBase := in.Notional >= baseFloor && in.Notional > 0 && !math.IsNaN(in.Notional) && !math.IsInf(in.Notional, 0)
	passRelative := baseline.Mature && baseline.RelativeThreshold > 0 && in.Notional >= baseline.RelativeThreshold
	passScore := rounded >= adaptiveMin
	limitations := make([]string, 0, 4)
	if !baseline.Mature {
		limitations = append(limitations, "relative baseline is not mature")
	}
	if !directionKnown {
		limitations = append(limitations, "BUY/SELL direction unavailable")
	}
	if !calApplied {
		limitations = append(limitations, "no mature prospective executable OOS calibration; adaptive correction is neutral")
	}
	return adaptiveWhaleReceipt{
		System: adaptiveWhaleSystem, State: "RESEARCH_ONLY_COLLECTING", Venue: strings.ToLower(in.Venue), Market: in.Market,
		Side: in.Side, Action: action, Wallet: in.Wallet, Evidence: evidence, Notional: in.Notional, Price: in.Price,
		ObservedAt: observed.UTC().Format(time.RFC3339Nano), AgeSeconds: age, ResolveHours: in.ResolveHours,
		RecencyHalfLifeSeconds: halfLife.Seconds(), Baseline: baseline, VenueBaseFloor: baseFloor,
		PassBaseFloor: passBase, PassRelativeSignificance: passRelative, AdaptiveMinScore: adaptiveMin,
		PassScore: passScore, ScoreContinuous: score, ScoreRounded5: rounded,
		Qualifies:    passBase && passRelative && passScore && directionKnown,
		ResearchOnly: true, PaperAuthority: false, LiveAuthority: false, CalibrationApplied: calApplied,
		Factors: factors, MissingNeutralFactors: missing, QualificationLimitations: limitations,
	}
}

func adaptiveWhaleParseTime(raw string, fallback time.Time) time.Time {
	if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw)); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(raw)); err == nil {
		return t
	}
	return fallback
}

func adaptiveWhaleHours(ts string, now time.Time) *float64 {
	if t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(ts)); err == nil {
		h := t.Sub(now).Hours()
		if h >= 0 {
			return &h
		}
	}
	return nil
}

func adaptiveWhaleBookFresh(at, now time.Time) bool {
	if at.IsZero() || now.IsZero() || at.After(now.Add(time.Second)) {
		return false
	}
	age := now.Sub(at)
	return age >= 0 && age <= adaptiveWhaleBookMaxAge
}

func adaptiveWhaleCoverageRows() []leaderboardCoverageRow {
	venues := []string{"kalshi", "polyus", "polymarket"}
	out := make([]leaderboardCoverageRow, 0, len(venues)*2)
	for _, venue := range venues {
		locked := venue == "polymarket"
		out = append(out,
			leaderboardCoverageRow{Family: adaptiveWhaleRawSystem + "@" + venue, Group: "research", Layer: "model", State: "COLLECTING",
				Venue: venue, VenueLocked: locked, ResearchOnly: true, LiveAuthorizes: false, Timed: false,
				Note: "all valid positive WS prints retained in a bounded raw-flow window with no dollar filter; no signal-price or one-share proof"},
			leaderboardCoverageRow{Family: adaptiveWhaleSystem + "@" + venue, Group: "research", Layer: "system", State: "RESEARCH_ONLY_COLLECTING",
				Venue: venue, VenueLocked: locked, ResearchOnly: true, LiveAuthorizes: false, Timed: false,
				Note: "adaptive relative-significance scorer; research-only, no paper/live authority, and no legacy-price proof"},
		)
	}
	return out
}

func (s *Server) adaptiveWhaleRaw(now time.Time) []adaptiveWhaleRaw {
	cut := now.Add(-adaptiveWhaleWindow)
	out := make([]adaptiveWhaleRaw, 0, 1024)
	if s.kal != nil {
		if tape, ok := s.kal.LiveTape(); ok {
			for _, t := range tape {
				at := adaptiveWhaleParseTime(t.CreatedTime, now)
				if at.Before(cut) {
					continue
				}
				side := strings.ToUpper(t.Aggressor())
				px := t.YesPrice.Float()
				if side == "NO" {
					px = t.NoPrice.Float()
				}
				n := t.Count.Float() * px
				if (side == "YES" || side == "NO") && n > 0 && px > 0 && px <= 1 {
					out = append(out, adaptiveWhaleRaw{Venue: "kalshi", Market: t.Ticker, Side: side, Action: "BUY", Notional: n, Price: px, At: at})
				}
			}
		}
	}
	if s.polyUSWS != nil {
		for _, t := range s.polyUSWS.RawFlow(adaptiveWhaleWindow) {
			if t.Notional > 0 {
				out = append(out, adaptiveWhaleRaw{Venue: "polyus", Market: t.Slug, Side: t.Side, Action: t.Action,
					Notional: t.Notional, Price: t.Price, At: t.At})
			}
		}
	}
	if s.poly != nil {
		for _, t := range s.poly.LiveTrades() {
			at := time.Unix(t.Timestamp, 0)
			if at.Before(cut) {
				continue
			}
			n := t.Size * t.Price
			if n > 0 && t.Price > 0 && t.Price <= 1 {
				out = append(out, adaptiveWhaleRaw{Venue: "polymarket", Market: t.ConditionID, Side: t.Outcome,
					Action: t.Side, Wallet: strings.ToLower(t.ProxyWallet), Notional: n, Price: t.Price, At: at})
			}
		}
	}
	return out
}

func (s *Server) adaptiveWhaleBookContext(raw adaptiveWhaleRaw, in *adaptiveWhaleInput, now time.Time) {
	switch raw.Venue {
	case "kalshi":
		s.metaMu.Lock()
		m, ok := s.kmkts[raw.Market]
		bookAt := s.kmktsAt[raw.Market]
		s.metaMu.Unlock()
		if ok {
			exp := m.ExpectedExpiration
			if exp == "" {
				exp = m.CloseTime
			}
			in.ResolveHours = adaptiveWhaleHours(exp, now)
			if adaptiveWhaleBookFresh(bookAt, now) {
				bid, ask := m.YesBid.Float(), m.YesAsk.Float()
				if bid > 0 && ask >= bid {
					v := (ask - bid) * 100
					in.SpreadCents = &v
				}
				depth, sidePx := m.YesAskSize.Float(), ask
				if strings.EqualFold(raw.Side, "NO") {
					depth, sidePx = m.YesBidSize.Float(), 1-bid
				}
				if depth > 0 && sidePx > 0 {
					v := depth * sidePx
					in.DepthUSD = &v
				}
				if s.kal != nil {
					if vol, ok := s.kal.Volatility(raw.Market); ok && vol >= 0 {
						in.BookVolatility = &vol
					}
				}
			}
		}
	case "polyus":
		if s.polyUSWS == nil {
			return
		}
		// FullBookTouchAt already requires the active generation, live primary transport,
		// and current lifecycle proof. Runtime-quiet complete books do not expire by receipt age.
		if bid, ask, bidSz, askSz, _, ok := s.polyUSWS.FullBookTouchAt(raw.Market); ok &&
			bid > 0 && ask >= bid {
			spread := (ask - bid) * 100
			in.SpreadCents = &spread
			depth, sidePx := askSz, ask
			if strings.EqualFold(raw.Side, "NO") {
				depth, sidePx = bidSz, 1-bid
			}
			if depth > 0 && sidePx > 0 {
				v := depth * sidePx
				in.DepthUSD = &v
			}
			if _, vol, ok := s.polyUSWS.MomVol(raw.Market); ok && vol >= 0 {
				in.BookVolatility = &vol
			}
		}
	}
}

// adaptiveWhaleChronological scores each receipt only with information available strictly before
// that print. Same-timestamp peers are scored as one batch and cannot see one another. The current
// book is attached only to near-now prints; using today's book for a 20-minute-old trade would be
// another form of hindsight leakage.
func (s *Server) adaptiveWhaleChronological(raw []adaptiveWhaleRaw, now time.Time) ([]adaptiveWhaleReceipt, map[string]int) {
	ordered := append([]adaptiveWhaleRaw(nil), raw...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].At.Equal(ordered[j].At) {
			return ordered[i].Venue+ordered[i].Market+ordered[i].Side < ordered[j].Venue+ordered[j].Market+ordered[j].Side
		}
		return ordered[i].At.Before(ordered[j].At)
	})
	totals := map[string]int{}
	for _, x := range ordered {
		totals[x.Venue]++
	}
	seen := map[string]int{}
	history := map[string][]adaptiveWhaleRaw{}
	passes := map[string]int{}
	receipts := make([]adaptiveWhaleReceipt, 0, len(ordered))
	for start := 0; start < len(ordered); {
		end := start + 1
		for end < len(ordered) && ordered[end].At.Equal(ordered[start].At) {
			end++
		}
		// Prune every venue represented in this timestamp batch before any receipt is scored.
		for i := start; i < end; i++ {
			venue := ordered[i].Venue
			h := history[venue]
			cut := ordered[i].At.Add(-adaptiveWhaleWindow)
			first := 0
			for first < len(h) && h[first].At.Before(cut) {
				first++
			}
			if first > 0 {
				h = append([]adaptiveWhaleRaw(nil), h[first:]...)
			}
			if len(h) > adaptiveWhaleVenueScoreCap {
				h = append([]adaptiveWhaleRaw(nil), h[len(h)-adaptiveWhaleVenueScoreCap:]...)
			}
			history[venue] = h
		}
		for i := start; i < end; i++ {
			x := ordered[i]
			h := history[x.Venue]
			notionals := make([]float64, 0, len(h))
			frequency, walletPrior := 1, 0.0 // the current observation is known; peers at the same ts are not
			freqKey := strings.Join([]string{x.Market, x.Side, strings.ToUpper(x.Action)}, "\x00")
			for _, prior := range h {
				notionals = append(notionals, prior.Notional)
				if strings.Join([]string{prior.Market, prior.Side, strings.ToUpper(prior.Action)}, "\x00") == freqKey {
					frequency++
				}
				if x.Wallet != "" && prior.Wallet == x.Wallet {
					walletPrior += prior.Notional
				}
			}
			baseline := robustAdaptiveWhaleBaseline(notionals, x.Venue)
			in := adaptiveWhaleInput{Venue: x.Venue, Market: x.Market, Side: x.Side, Action: x.Action,
				Wallet: x.Wallet, Notional: x.Notional, Price: x.Price, ObservedAt: x.At, Now: now,
				Baseline: &baseline, Frequency: &frequency}
			if x.Wallet != "" && walletPrior+x.Notional > 0 {
				alloc := x.Notional / (walletPrior + x.Notional)
				in.WalletAllocation = &alloc
			}
			age := now.Sub(x.At)
			if s != nil && age >= 0 && age <= adaptiveWhaleBookMaxAge {
				s.adaptiveWhaleBookContext(x, &in, now)
			}
			rec := scoreAdaptiveWhale(in)
			if rec.Qualifies {
				passes[x.Venue]++
			}
			seen[x.Venue]++
			if seen[x.Venue] > totals[x.Venue]-adaptiveWhaleVenueScoreCap {
				receipts = append(receipts, rec)
			}
		}
		// Only after the entire timestamp batch is scored do its prints become prior history.
		for i := start; i < end; i++ {
			x := ordered[i]
			history[x.Venue] = append(history[x.Venue], x)
		}
		start = end
	}
	return receipts, passes
}

func (s *Server) handleAdaptiveWhaleResearch(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	raw := s.adaptiveWhaleRaw(now)
	window := map[string][]float64{}
	for _, x := range raw {
		window[x.Venue] = append(window[x.Venue], x.Notional)
	}
	baselines := map[string]adaptiveWhaleBaseline{}
	for _, venue := range []string{"kalshi", "polyus", "polymarket"} {
		baselines[venue] = robustAdaptiveWhaleBaseline(window[venue], venue)
	}

	receipts, passes := s.adaptiveWhaleChronological(raw, now)
	stats := map[string]*adaptiveWhaleVenueStats{}
	for _, venue := range []string{"kalshi", "polyus", "polymarket"} {
		b := baselines[venue]
		stats[venue] = &adaptiveWhaleVenueStats{Venue: venue, Baseline: b, RawDollarFilter: 0}
	}
	for _, x := range raw {
		st := stats[x.Venue]
		st.RawPositiveN++
		if st.Smallest == 0 || x.Notional < st.Smallest {
			st.Smallest = x.Notional
		}
		if x.Notional > st.Largest {
			st.Largest = x.Notional
		}
	}
	for venue, n := range passes {
		stats[venue].ResearchPassN = n
	}
	sort.SliceStable(receipts, func(i, j int) bool {
		if receipts[i].Qualifies != receipts[j].Qualifies {
			return receipts[i].Qualifies
		}
		if receipts[i].ScoreContinuous != receipts[j].ScoreContinuous {
			return receipts[i].ScoreContinuous > receipts[j].ScoreContinuous
		}
		return receipts[i].ObservedAt > receipts[j].ObservedAt
	})
	if len(receipts) > adaptiveWhaleReceiptCap {
		receipts = receipts[:adaptiveWhaleReceiptCap]
	}
	venueStats := make([]adaptiveWhaleVenueStats, 0, 3)
	for _, venue := range []string{"kalshi", "polyus", "polymarket"} {
		venueStats = append(venueStats, *stats[venue])
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"raw_flow_observer": map[string]any{
			"system": adaptiveWhaleRawSystem, "state": "COLLECTING", "research_only": true,
			"min_notional": 0, "venues": venueStats,
			"contract": "every valid positive WS print is retained in the venue's bounded tape; no dollar filter and no automatic trading signal",
		},
		"adaptive_whale_scorer": map[string]any{
			"system": adaptiveWhaleSystem, "state": "RESEARCH_ONLY_COLLECTING", "research_only": true,
			"paper_authority": false, "live_authority": false, "score_range": "0-100; rounded to nearest 5 for display",
			"qualification":       "nonzero venue floor + mature robust relative-size threshold + adaptive score minimum; qualification retains research only",
			"self_correction":     "only mature prospective executable OOS outcomes (>=20 outcomes, >=30 UTC blocks) may alter calibration score/minimum",
			"current_integration": "existing fixed whale/flow signal filters are unchanged until this scorer has its own mature executable outcome proof",
			"chronology":          "each receipt uses a strictly-prior rolling 30-minute venue baseline; same-timestamp peers cannot see one another; current book context is attached only within 5 seconds",
			"score_cap_per_venue": adaptiveWhaleVenueScoreCap,
			"receipts":            receipts,
		},
	})
}
