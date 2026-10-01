package server

// Proper-score research converts every unbiased book-v2 forecast persona into the position vector
// implied by Brier, log and spherical scoring rules. The venue side is chosen only after re-stamping a current
// complete book, exact fee and depth. This file has no paper/live/order call and writes only the
// dedicated research_proper_score_trials ledger.

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/properbetting"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	properScoreForecastMaxAge = 3 * time.Minute
	// Every persona emits three transforms, two strategy modes, four matched controls and its
	// simulation/trial rows. A 64-persona page therefore fans out into thousands of database
	// operations and repeatedly exhausted the shared lane before a durable vector completed.
	// Eight personas every five minutes is deliberately conservative: it attempts 96 fresh
	// personas/hour (versus the old intended 64/hour), while keeping each immutable vector small
	// enough to finish under ordinary writer contention.
	properScoreSelectionCadence = 5 * time.Minute
	properScoreCycleCap         = 8
	properScoreLaneBudget       = 35 * time.Second
	properScoreReceiptReserve   = 3 * time.Second
	properScorePhaseReserve     = 5 * time.Second
)

const properScoreBudgetReason = "deterministic 5-minute rotation selects at most 8 current funded-horizon personas and reserves the shared 35s lane for complete immutable vectors plus a durable receipt"

type properScoreForecast struct {
	Ticker       string  `json:"ticker"`
	SignalTS     string  `json:"ts"`
	Side         string  `json:"side"`
	Platform     string  `json:"platform"`
	Title        string  `json:"title"`
	SignalType   string  `json:"signal_type"`
	Category     string  `json:"category"`
	PWin         float64 `json:"p_win"`
	ResolveHours float64 `json:"resolve_hours"`
}

type properScoreForecastFile struct {
	GeneratedAt    int64                 `json:"generated_at"`
	ModelStatus    string                `json:"model_status"`
	ForecastSource string                `json:"forecast_source"`
	ModelVersion   string                `json:"model_version"`
	ModelBackend   string                `json:"model_backend"`
	Calibration    string                `json:"calib_method"`
	FeatureSchema  string                `json:"feature_schema"`
	AllScored      []properScoreForecast `json:"all_scored"`
}

type properScoreBook struct {
	yesBid, yesAsk, yesBidDepth, yesAskDepth float64
	noBid, noAsk, noBidDepth, noAskDepth     float64
	yesBids, yesAsks, noBids, noAsks         []properbetting.Level
	tick, lot                                float64
	source, sourceClock                      string
	sourceClockAt                            time.Time
	age                                      float64
}

func (b properScoreBook) completeBinarySet() bool {
	if b.tick <= 0 || b.lot <= 0 || len(b.yesBids) == 0 || len(b.yesAsks) == 0 ||
		len(b.noBids) == 0 || len(b.noAsks) == 0 || len(b.yesAsks) != len(b.noBids) ||
		len(b.yesBids) != len(b.noAsks) ||
		math.Abs(b.yesAsk+b.noBid-1) > 1e-9 || math.Abs(b.yesBid+b.noAsk-1) > 1e-9 {
		return false
	}
	check := func(left, right []properbetting.Level) bool {
		for i := range left {
			if math.Abs(left[i].Price+right[i].Price-1) > 1e-9 ||
				math.Abs(left[i].Quantity-right[i].Quantity) > 1e-9 ||
				left[i].Quantity <= 0 || left[i].Tick <= 0 || right[i].Tick <= 0 {
				return false
			}
		}
		return true
	}
	return check(b.yesAsks, b.noBids) && check(b.yesBids, b.noAsks)
}

type properScoreLiveness struct {
	AsOf, Slot                                  string
	GeneratedAt                                 int64
	SourceRows, UniqueRows, HorizonEligibleRows int
	BoundedRows, InvalidRows, NoBook            int
	BookEligibleRows, CompleteBooks             int
	Actions, Abstains, InsertedRows             int
	InsertErrors                                int
	DeferredRows, HorizonDeferredRows           int
	SelectionPage, SelectionPages               int
	BudgetDeferredRows, CompletedTransforms     int
	CycleDurationMS, WriteBudgetRemainingMS     float64
	BudgetStage                                 string
	ErrorSamples                                []string
}

type properScoreBookPage struct {
	forecasts                  []properScoreForecast
	books                      map[string]properScoreBook
	latencyMS                  map[string]float64
	invalid, noBook, available int
	page, pages                int
}

// properScoreSelectBookPage checks every unique current-horizon coordinate against the current
// cache/WS book before applying the eight-persona write cap. The old order capped first, so a page
// containing eight unsubscribed/stale books could store zero even while another page contained
// executable books. This helper preserves the bounded writer fan-out while ensuring the bounded
// vector is selected from coordinates that can actually be evaluated. The exact snapshot returned
// by lookup is carried into the vector; it is never reassembled from a later BBO.
func properScoreSelectBookPage(rows []properScoreForecast, slot string, limit int,
	lookup func(platform, ticker string) (properScoreBook, bool)) properScoreBookPage {
	out := properScoreBookPage{
		books:     make(map[string]properScoreBook),
		latencyMS: make(map[string]float64),
	}
	available := make([]properScoreForecast, 0, len(rows))
	for _, f := range properScoreBoundForecasts(rows, slot, 0) {
		platform := strings.ToLower(strings.TrimSpace(f.Platform))
		side := strings.ToUpper(strings.TrimSpace(f.Side))
		if (platform != "kalshi" && platform != "polyus") ||
			(side != "YES" && side != "NO") || f.PWin < 0 || f.PWin > 1 ||
			strings.TrimSpace(f.Ticker) == "" {
			out.invalid++
			continue
		}
		started := time.Now()
		book, ok := lookup(platform, f.Ticker)
		latencyMS := float64(time.Since(started).Microseconds()) / 1000
		if !ok || book.yesBid <= 0 || book.yesAsk <= book.yesBid || book.yesAsk >= 1 {
			out.noBook++
			continue
		}
		available = append(available, f)
		key := properScoreForecastKey(f)
		out.books[key] = book
		out.latencyMS[key] = latencyMS
	}
	out.available = len(available)
	// Keep each immutable comparison batch on one venue and inside one projected settlement-hour.
	// A mixed Kalshi/PolyUS or early/late page can remain incomplete long after most of its legs
	// settle, needlessly hiding otherwise usable matched comparisons. This changes only future
	// manifest composition; it never drops a frozen coordinate from an existing batch.
	out.forecasts, out.page, out.pages = properScoreCoherentPage(available, slot, limit)
	return out
}

func properScoreCoherentPage(rows []properScoreForecast, slot string, limit int) (
	[]properScoreForecast, int, int) {
	if len(rows) == 0 {
		return nil, 0, 0
	}
	if limit <= 0 {
		return properScoreBoundForecasts(rows, slot, 0), 0, 1
	}
	base, err := time.Parse(time.RFC3339, strings.TrimSpace(slot))
	if err != nil {
		base = time.Unix(0, 0).UTC()
	}
	groups := map[string][]properScoreForecast{}
	for _, row := range properScoreBoundForecasts(rows, slot, 0) {
		closeAt := base.Add(time.Duration(math.Max(0, row.ResolveHours) * float64(time.Hour)))
		bucket := closeAt.UTC().Truncate(time.Hour).Format(time.RFC3339)
		key := strings.ToLower(strings.TrimSpace(row.Platform)) + "|" + bucket
		groups[key] = append(groups[key], row)
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pages := make([][]properScoreForecast, 0, len(keys))
	for _, key := range keys {
		group := groups[key]
		for start := 0; start < len(group); start += limit {
			end := min(start+limit, len(group))
			pages = append(pages, append([]properScoreForecast(nil), group[start:end]...))
		}
	}
	if len(pages) == 0 {
		return nil, 0, 0
	}
	page := int(properScoreSelectionOrdinal(slot) % int64(len(pages)))
	if page < 0 {
		page += len(pages)
	}
	return pages[page], page, len(pages)
}

func properScoreErrorSample(stage, platform, ticker string, err error) string {
	if err == nil {
		return ""
	}
	message := strings.Join([]string{strings.TrimSpace(stage), strings.TrimSpace(platform),
		strings.TrimSpace(ticker), strings.TrimSpace(err.Error())}, " | ")
	message = strings.ReplaceAll(strings.ReplaceAll(message, "\r", " "), "\n", " ")
	if len(message) > 320 {
		message = message[:320]
	}
	return message
}

type properScoreCandidate struct {
	side, feeSource, abstain, cohort                string
	qYes, rawYes, rawNo, canonical, normalized      float64
	requested, qty, price, depth, fee, expected     float64
	shift, rescale, integrated, spot, liquidityLoss float64
	tick, lot, cancelled                            float64
	rawVector                                       []float64
	depthCurve                                      []properbetting.Fill
}

type properScorePrepared struct {
	forecast             properScoreForecast
	platform, originSide string
	pYes, latencyMS      float64
	canonicalIdentity    bool
	book                 properScoreBook
	candidates           map[string]properScoreCandidate
	abstentions          map[string]string
}

func properScoreKalshiSourceClock(generation uint64, subscriptionID, sequence int64,
	sourceAt, receivedAt time.Time) (string, time.Time, bool) {
	if generation == 0 || subscriptionID <= 0 || sequence <= 0 {
		return "", time.Time{}, false
	}
	prefix := fmt.Sprintf("kalshi-book:g%d:s%d:q%d", generation, subscriptionID, sequence)
	if !sourceAt.IsZero() {
		return prefix + ":source:" + sourceAt.UTC().Format(time.RFC3339Nano), sourceAt.UTC(), true
	}
	// The venue sometimes omits its source timestamp. A sequenced generation/SID/sequence still
	// identifies an immutable frame; ReceivedAt is retained only as the local receipt clock and is
	// explicitly labelled as such. An unsequenced arrival can never use this fallback.
	if receivedAt.IsZero() {
		return "", time.Time{}, false
	}
	return prefix + ":received:" + receivedAt.UTC().Format(time.RFC3339Nano) + ":venue-time-omitted",
		receivedAt.UTC(), true
}

func properScorePolyUSSourceClock(sourceAt time.Time) (string, time.Time, bool) {
	if sourceAt.IsZero() {
		return "", time.Time{}, false
	}
	sourceAt = sourceAt.UTC()
	return "polyus-market-data:" + sourceAt.Format(time.RFC3339Nano), sourceAt, true
}

func properScoreVector(transform string, p, q float64) (yes, no, qty float64, ok bool) {
	t, err := properbetting.ParseTransform(transform)
	if err != nil {
		return 0, 0, 0, false
	}
	pos, err := properbetting.NewPosition(t, []float64{p, 1 - p}, []float64{q, 1 - q})
	if err != nil || len(pos.Raw) != 2 || len(pos.Canonical) != 2 {
		return 0, 0, 0, false
	}
	yes, no = pos.Raw[0], pos.Raw[1]
	qty = math.Max(pos.Canonical[0], pos.Canonical[1])
	return yes, no, qty, qty > 0 && !math.IsNaN(qty) && !math.IsInf(qty, 0)
}

func (s *Server) properScoreCurrentBook(ctx context.Context, platform, ticker string) (properScoreBook, bool) {
	platform = strings.ToLower(strings.TrimSpace(platform))
	if platform == "kalshi" {
		if s.kal == nil {
			return properScoreBook{}, false
		}
		// Freeze the ladder and its sequence/source receipt under one book lock. Calling LiveBook
		// and LiveBookProvenance separately can pair a pre-delta ladder with a post-delta receipt,
		// which is not an immutable executable observation.
		ob, age, provenance, ok := s.kal.LiveBookWithProvenance(ticker, 5*time.Second)
		market, marketOK := s.kmkt(ticker)
		if !ok || ob == nil || len(ob.YesBids) == 0 || len(ob.YesAsks) == 0 || !marketOK {
			return properScoreBook{}, false
		}
		book := properScoreBook{yesBid: ob.YesBids[0].Price, yesAsk: ob.YesAsks[0].Price,
			yesBidDepth: ob.YesBids[0].Size, yesAskDepth: ob.YesAsks[0].Size,
			noBid: 1 - ob.YesAsks[0].Price, noAsk: 1 - ob.YesBids[0].Price,
			noBidDepth: ob.YesAsks[0].Size, noAskDepth: ob.YesBids[0].Size,
			lot: 1, source: "kalshi_book_ws_full", age: age.Seconds()}
		for _, level := range ob.YesAsks {
			yesTick, yesKnown := market.TickForKnown(level.Price)
			noTick, noKnown := market.TickForKnown(1 - level.Price)
			if !yesKnown || !noKnown {
				return properScoreBook{}, false
			}
			book.yesAsks = append(book.yesAsks, properbetting.Level{Price: level.Price, Quantity: level.Size, Tick: yesTick})
			book.noBids = append(book.noBids, properbetting.Level{Price: 1 - level.Price, Quantity: level.Size, Tick: noTick})
			for _, tick := range []float64{yesTick, noTick} {
				if book.tick == 0 || tick < book.tick {
					book.tick = tick
				}
			}
		}
		for _, level := range ob.YesBids {
			price := 1 - level.Price
			yesTick, yesKnown := market.TickForKnown(level.Price)
			noTick, noKnown := market.TickForKnown(price)
			if !yesKnown || !noKnown {
				return properScoreBook{}, false
			}
			book.noAsks = append(book.noAsks, properbetting.Level{Price: price, Quantity: level.Size, Tick: noTick})
			book.yesBids = append(book.yesBids, properbetting.Level{Price: level.Price, Quantity: level.Size, Tick: yesTick})
			for _, tick := range []float64{yesTick, noTick} {
				if book.tick == 0 || tick < book.tick {
					book.tick = tick
				}
			}
		}
		clock, clockAt, clockOK := properScoreKalshiSourceClock(provenance.Generation,
			provenance.SubscriptionID, provenance.Sequence, provenance.SourceAt, provenance.ReceivedAt)
		if clockOK {
			book.sourceClock = clock
			book.sourceClockAt = clockAt
		}
		if sourceAge := time.Since(clockAt).Seconds(); clockOK && sourceAge >= 0 && sourceAge > book.age {
			book.age = sourceAge
		}
		return book, book.completeBinarySet()
	}
	if platform == "polyus" && s.polyUSWS != nil {
		bids, asks, sourceAt, receivedAt, ok := s.polyUSWS.FullBookLevelsAt(ticker)
		// This research sweep is deliberately cache/WS-only. pusMeta performs an authenticated REST
		// request on a cache miss; calling it once per forecast consumed the whole heavy-lane budget
		// and also multiplied venue traffic. The complete open-universe crawl already owns the exact
		// tick/minimum/lifecycle receipt, so use that single fresh authority instead.
		tick, lot, metaOK := s.properScorePUSConstraints(ticker)
		age := time.Since(receivedAt).Seconds()
		if !ok || len(bids) == 0 || len(asks) == 0 || !metaOK || age < 0 {
			return properScoreBook{}, false
		}
		book := properScoreBook{yesBid: bids[0].Price, yesAsk: asks[0].Price,
			yesBidDepth: bids[0].Quantity, yesAskDepth: asks[0].Quantity,
			noBid: 1 - asks[0].Price, noAsk: 1 - bids[0].Price,
			noBidDepth: asks[0].Quantity, noAskDepth: bids[0].Quantity,
			tick: tick, lot: lot, source: "polyus_ws_full_depth", age: age}
		for _, level := range asks {
			book.yesAsks = append(book.yesAsks, properbetting.Level{Price: level.Price, Quantity: level.Quantity, Tick: tick})
			book.noBids = append(book.noBids, properbetting.Level{Price: 1 - level.Price, Quantity: level.Quantity, Tick: tick})
		}
		for _, level := range bids {
			book.noAsks = append(book.noAsks, properbetting.Level{Price: 1 - level.Price, Quantity: level.Quantity, Tick: tick})
			book.yesBids = append(book.yesBids, properbetting.Level{Price: level.Price, Quantity: level.Quantity, Tick: tick})
		}
		if clock, clockAt, clockOK := properScorePolyUSSourceClock(sourceAt); clockOK {
			book.sourceClock = clock
			book.sourceClockAt = clockAt
			if sourceAge := time.Since(clockAt).Seconds(); sourceAge >= 0 && sourceAge > book.age {
				book.age = sourceAge
			}
		}
		return book, book.completeBinarySet()
	}
	return properScoreBook{}, false
}

// properScorePUSConstraints reads one immutable market receipt from the latest complete REST-open
// universe. It performs no network I/O and fails closed when that crawl is stale, the market is no
// longer exactly open, or fractional quantity/tick rules are absent. Fee authority is rechecked
// independently by properScoreCurveFee, so neither fact can substitute for the other.
func (s *Server) properScorePUSConstraints(ticker string) (tick, lot float64, ok bool) {
	if s == nil || strings.TrimSpace(ticker) == "" {
		return 0, 0, false
	}
	s.polyUSMu.Lock()
	defer s.polyUSMu.Unlock()
	age := time.Since(s.pusSweepAt)
	if s.pusSweepAt.IsZero() || age < 0 || age > polymarketus.MarketsRESTLifecycleMaxAge {
		return 0, 0, false
	}
	for i := range s.pusSweep {
		m := &s.pusSweep[i]
		if !strings.EqualFold(strings.TrimSpace(m.Slug), strings.TrimSpace(ticker)) {
			continue
		}
		if !m.Open() || m.TickSize <= 0 || m.TickSize >= 1 || m.MinimumQty <= 0 ||
			math.IsNaN(m.TickSize) || math.IsInf(m.TickSize, 0) ||
			math.IsNaN(m.MinimumQty) || math.IsInf(m.MinimumQty, 0) {
			return 0, 0, false
		}
		return m.TickSize, m.MinimumQty, true
	}
	return 0, 0, false
}

// properScoreCurveFee freezes one route-level fee calculator. The full fill set is priced as one
// order accumulator, matching the venue's aggregate rounding semantics instead of summing a
// rounded fee independently at every depth level.
func (s *Server) properScoreCurveFee(platform, ticker, route string, probePrice float64) (properbetting.FeeFunc, string, bool) {
	return s.properScoreCurveFeeAction(platform, ticker, route, probePrice, true)
}

func (s *Server) properScoreCurveFeeAction(platform, ticker, route string, probePrice float64,
	buy bool) (properbetting.FeeFunc, string, bool) {
	maker := strings.EqualFold(route, "maker")
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "kalshi":
		_, known, source := s.kalFeeExactAction(ticker, maker, 1, probePrice, buy)
		if !known {
			return nil, source, false
		}
		if source == "active market fee waiver" {
			return func([]properbetting.Fill) (float64, bool) { return 0, true }, "kalshi:" + source, true
		}
		info, ok, loaded := s.kalFeeLookup(ticker)
		if !loaded || !ok || !kalFeeInfoSupported(info) {
			return nil, "kalshi:" + source, false
		}
		coeff := info.taker
		if maker {
			coeff = info.maker
		}
		fee := func(fills []properbetting.Fill) (float64, bool) {
			if len(fills) == 0 || coeff < 0 || math.IsNaN(coeff) || math.IsInf(coeff, 0) {
				return 0, false
			}
			tradeRaw, cost := 0.0, 0.0
			for _, fill := range fills {
				if fill.Quantity <= 0 || fill.Price <= 0 || fill.Price >= 1 {
					return 0, false
				}
				tradeRaw += coeff * fill.Quantity * fill.Price * (1 - fill.Price)
				cost += fill.Quantity * fill.Price
			}
			if coeff == 0 {
				return 0, true
			}
			trade := math.Ceil(math.Round(tradeRaw*10000*1e6)/1e6) / 10000
			change := cost - trade
			if buy {
				change = -cost - trade
			}
			floored := math.Floor((change+1e-12)*100) / 100
			rounding := change - floored
			if rounding < 1e-12 {
				rounding = 0
			}
			return trade + rounding, true
		}
		action := "sell"
		if buy {
			action = "buy"
		}
		return fee, "kalshi:" + source + ":aggregate-order:" + action, true

	case "polyus":
		theta, _, known := s.polyUSFreshFeeAuthority(ticker)
		if !known {
			return nil, "polyus:feeCoefficient-unavailable", false
		}
		fee := func(fills []properbetting.Fill) (float64, bool) {
			raw := 0.0
			for _, fill := range fills {
				if fill.Quantity <= 0 || fill.Price <= 0 || fill.Price >= 1 {
					return 0, false
				}
				coeff := theta
				if maker {
					if math.Abs(theta-polyUSTakerTheta) > 1e-12 {
						coeff = 0
					} else {
						coeff = -polyUSMakerTheta
					}
				}
				raw += coeff * fill.Quantity * fill.Price * (1 - fill.Price)
			}
			return math.RoundToEven(raw*100) / 100, true
		}
		action := "sell"
		if buy {
			action = "buy"
		}
		return fee, polyUSFeeAuthoritySource + ":aggregate-order:" + action, true
	}
	return nil, "", false
}

func properScoreRawCandidate(transform string, p float64, book properScoreBook) (properScoreCandidate, string) {
	if !book.completeBinarySet() {
		return properScoreCandidate{}, "complete_outcome_set_book_unavailable"
	}
	t, err := properbetting.ParseTransform(transform)
	if err != nil {
		return properScoreCandidate{}, "unknown_transform"
	}
	actions, err := properbetting.BidAskActions(t, []float64{p}, []float64{book.yesAsk}, []float64{book.noAsk})
	if err != nil || len(actions) != 1 {
		return properScoreCandidate{}, "invalid_probability_or_spread"
	}
	a := actions[0]
	c := properScoreCandidate{side: a.Side, qYes: a.Qtilde, canonical: a.Target,
		rawVector: append([]float64(nil), a.RawBinary...), lot: book.lot, tick: book.tick}
	if len(a.RawBinary) == 2 {
		c.rawYes, c.rawNo = a.RawBinary[0], a.RawBinary[1]
		c.shift = -math.Min(c.rawYes, c.rawNo)
	}
	if a.NoTrade {
		return c, "inside_bid_ask_dead_zone"
	}
	if c.side == "YES" {
		c.price, c.depth = book.yesAsk, 0
		for _, level := range book.yesAsks {
			c.depth += level.Quantity
		}
	} else {
		c.price, c.depth = book.noAsk, 0
		for _, level := range book.noAsks {
			c.depth += level.Quantity
		}
	}
	if c.canonical <= 0 || c.depth <= 0 {
		return c, "no_executable_depth"
	}
	return c, ""
}

func (s *Server) properScoreEvaluate(platform, ticker, transform string, p float64, book properScoreBook,
	c properScoreCandidate, normalizedWeight float64) (properScoreCandidate, string) {
	if c.side != "YES" && c.side != "NO" {
		return c, "inside_bid_ask_dead_zone"
	}
	c.normalized, c.rescale = normalizedWeight, 1
	if normalizedWeight <= 0 || c.price <= 0 {
		return c, "portfolio_normalization_zero"
	}
	// The research estimand is bankroll-free: all actionable coordinates are L1-normalized to one
	// total position unit. Execution/capacity is measured separately with a one-share route probe;
	// it never changes the normalized portfolio weight or introduces a hidden dollar bankroll.
	c.requested = 1
	levels, forecastSide := book.yesAsks, p
	if c.side == "NO" {
		levels, forecastSide = book.noAsks, 1-p
	}
	fee, feeSource, known := s.properScoreCurveFee(platform, ticker, "taker", c.price)
	if !known {
		return c, "exact_fee_unavailable"
	}
	result, err := properbetting.WalkDepth(levels, c.requested, forecastSide, book.tick, book.lot, fee)
	if err != nil {
		if strings.Contains(err.Error(), "below the venue lot") {
			return c, "below_venue_lot"
		}
		return c, "depth_fee_or_tick_invalid"
	}
	c.qty, c.cancelled = result.Filled, result.Cancelled
	c.price, c.tick = result.AveragePrice, result.MinimumTick
	c.integrated, c.spot, c.liquidityLoss = result.IntegratedCost, result.SpotCost, result.LiquidityLoss
	c.fee, c.expected, c.feeSource = result.Fee, result.ExpectedNet, feeSource
	c.depthCurve = append([]properbetting.Fill(nil), result.Fills...)
	if result.ExpectedNet <= 0 {
		return c, "fees_spread_and_slippage_erase_edge"
	}
	return c, ""
}

func properScoreNormalizedWeights(rows []properScorePrepared, transform string) ([]float64, error) {
	raw := make([]float64, len(rows))
	for i := range rows {
		c := rows[i].candidates[transform]
		if rows[i].abstentions[transform] == "" {
			raw[i] = properScoreSigned(c.side, c.canonical)
		}
	}
	return properbetting.NormalizeWeights(raw, 1)
}

// properScoreExecutableWeights performs the second normalization pass, after the current-book
// depth and exact-fee gates have run.  The proper-score position vector is the economic unit, so
// coordinates which could not execute must not consume part of its frozen L1 budget.  Preserve the
// scoring transform's relative canonical weights among the surviving coordinates and renormalize
// those survivors to exactly one total share unit.
func properScoreExecutableWeights(rows []properScorePrepared, transform string,
	evaluated []properScoreCandidate, reasons []string) ([]float64, error) {
	if len(rows) != len(evaluated) || len(rows) != len(reasons) {
		return nil, errors.New("proper-score executable weight dimension mismatch")
	}
	raw := make([]float64, len(rows))
	for i := range rows {
		source, ok := rows[i].candidates[transform]
		if !ok {
			return nil, fmt.Errorf("proper-score transform %q missing at coordinate %d", transform, i)
		}
		if reasons[i] == "" && evaluated[i].qty > 0 && source.canonical > 0 &&
			(source.side == "YES" || source.side == "NO") {
			raw[i] = properScoreSigned(source.side, source.canonical)
		}
	}
	return properbetting.NormalizeWeights(raw, 1)
}

func properScoreSigned(side string, quantity float64) float64 {
	if strings.EqualFold(side, "NO") {
		return -quantity
	}
	return quantity
}

type properScoreSimResult struct {
	prior, target, requested, filled, cancelled, post float64
	grossCash, fee, expected                          float64
	status                                            string
	legs                                              []map[string]any
	inserted                                          bool
}

func (s *Server) simulateProperScoreImmediateTaker(ctx context.Context, now time.Time, slot, mode,
	transform string, row properScorePrepared, c properScoreCandidate, cohort, forecastSource,
	forecastVersion string) (properScoreSimResult, error) {
	var out properScoreSimResult
	prior, err := s.store.ProperScoreSimulatedPositionBefore(ctx, mode, transform, row.platform,
		row.forecast.Ticker, forecastSource, forecastVersion, slot)
	if err != nil {
		return out, err
	}
	out.prior = prior
	unitTarget := properScoreSigned(c.side, 1)
	if mode == "fundamental" {
		out.target = prior + unitTarget
	} else {
		out.target = unitTarget
	}
	out.requested = out.target - prior
	plan, err := properbetting.PlanRebalance(prior, out.target)
	if err != nil {
		return out, err
	}
	out.status = "noop"
	for _, leg := range plan {
		if leg.Quantity <= 1e-12 {
			continue
		}
		side := strings.ToUpper(leg.Side)
		forecastSide := row.pYes
		asks, bids := row.book.yesAsks, row.book.yesBids
		if side == "NO" {
			forecastSide, asks, bids = 1-row.pYes, row.book.noAsks, row.book.noBids
		}
		if strings.EqualFold(leg.Action, "BUY") {
			if len(asks) == 0 {
				out.status = "blocked"
				break
			}
			feeFn, feeSource, known := s.properScoreCurveFeeAction(row.platform,
				row.forecast.Ticker, "taker", asks[0].Price, true)
			if !known {
				out.status = "blocked"
				out.legs = append(out.legs, map[string]any{"action": "BUY", "side": side,
					"requested": leg.Quantity, "status": "fee_unknown", "fee_source": feeSource})
				break
			}
			fill, fillErr := properbetting.WalkDepth(asks, leg.Quantity, forecastSide,
				row.book.tick, row.book.lot, feeFn)
			if fillErr != nil {
				out.status = "blocked"
				out.legs = append(out.legs, map[string]any{"action": "BUY", "side": side,
					"requested": leg.Quantity, "status": "unfilled", "error": fillErr.Error()})
				break
			}
			delta := fill.Filled
			if side == "NO" {
				delta = -delta
			}
			out.filled += delta
			out.grossCash -= fill.IntegratedCost
			out.fee += fill.Fee
			out.expected += fill.ExpectedNet
			out.legs = append(out.legs, map[string]any{"action": "BUY", "side": side,
				"requested": leg.Quantity, "filled": fill.Filled, "cancelled": fill.Cancelled,
				"average_price": fill.AveragePrice, "integrated_cost": fill.IntegratedCost,
				"liquidity_loss": fill.LiquidityLoss, "fee": fill.Fee, "fee_source": feeSource,
				"fills": fill.Fills, "status": map[bool]string{true: "filled", false: "partial"}[fill.FullFill]})
			if !fill.FullFill {
				out.status = "partial"
				break // never open the opposite side until the close leg filled completely
			}
		} else {
			if len(bids) == 0 {
				out.status = "blocked"
				break
			}
			feeFn, feeSource, known := s.properScoreCurveFeeAction(row.platform,
				row.forecast.Ticker, "taker", bids[0].Price, false)
			if !known {
				out.status = "blocked"
				out.legs = append(out.legs, map[string]any{"action": "SELL", "side": side,
					"requested": leg.Quantity, "status": "fee_unknown", "fee_source": feeSource})
				break
			}
			fill, fillErr := properbetting.WalkSaleDepth(bids, leg.Quantity, forecastSide,
				row.book.tick, row.book.lot, feeFn)
			if fillErr != nil {
				out.status = "blocked"
				out.legs = append(out.legs, map[string]any{"action": "SELL", "side": side,
					"requested": leg.Quantity, "status": "unfilled", "error": fillErr.Error()})
				break
			}
			delta := -fill.Filled
			if side == "NO" {
				delta = fill.Filled
			}
			out.filled += delta
			out.grossCash += fill.IntegratedValue
			out.fee += fill.Fee
			out.expected += fill.ExpectedNet
			out.legs = append(out.legs, map[string]any{"action": "SELL", "side": side,
				"requested": leg.Quantity, "filled": fill.Filled, "cancelled": fill.Cancelled,
				"average_price": fill.AveragePrice, "integrated_proceeds": fill.IntegratedValue,
				"liquidity_loss": fill.LiquidityLoss, "fee": fill.Fee, "fee_source": feeSource,
				"fills": fill.Fills, "status": map[bool]string{true: "filled", false: "partial"}[fill.FullFill]})
			if !fill.FullFill {
				out.status = "partial"
				break
			}
		}
	}
	out.post = out.prior + out.filled
	out.cancelled = out.requested - out.filled
	switch {
	case len(plan) == 0:
		out.status = "noop"
	case out.status == "blocked":
	case math.Abs(out.post-out.target) <= 1e-8:
		out.status = "filled"
	case math.Abs(out.filled) > 1e-12:
		out.status = "partial"
	default:
		out.status = "cancelled"
	}
	_, out.inserted, err = s.store.InsertProperScoreSimulation(ctx, storage.ProperScoreSimulation{
		Observed: now, Slot: slot, System: "proper-score-" + transform, Transform: transform,
		StrategyMode: mode, Cohort: cohort, Platform: row.platform, Ticker: row.forecast.Ticker,
		ForecastSource: forecastSource, ForecastVersion: forecastVersion,
		PriorPosition: out.prior, TargetPosition: out.target, RequestedDelta: out.requested,
		FilledDelta: out.filled, CancelledDelta: out.cancelled, PostPosition: out.post,
		GrossCashFlow: out.grossCash, FeeTotal: out.fee, ExpectedValueDelta: out.expected,
		FillStatus: out.status, BookSource: row.book.source, SourceClockID: row.book.sourceClock,
		DecisionLatencyMS: row.latencyMS, Legs: out.legs,
	})
	return out, err
}

// properScoreSystemObservation writes the exact current one-share action used by the common
// sealed-inference and Paper-promotion pipeline. The portfolio trial may request more shares, but
// promotion evidence stays bankroll-neutral and one-share. Settlement remains the honest 0..1
// censor envelope; the positive lower field is the current forecast-implied expected net, never a
// fabricated guaranteed payoff.
func (s *Server) properScoreSystemObservation(ctx context.Context, now time.Time, slot, mode, transform string,
	row properScorePrepared, c properScoreCandidate, cohort string) (int64, error) {
	if s.store == nil || c.side == "" || c.expected <= 0 || c.qty+1e-12 < 1 || c.depth < 1 {
		return 0, errors.New("proper-score one-share route is incomplete")
	}
	identity, identityOK, err := s.store.CurrentCanonicalInstrument(ctx, row.platform, row.forecast.Ticker)
	if err != nil || !identityOK {
		return 0, fmt.Errorf("proper-score canonical identity unavailable: %w", err)
	}
	levels, forecastSide := row.book.yesAsks, row.pYes
	if c.side == "NO" {
		levels, forecastSide = row.book.noAsks, 1-row.pYes
	}
	feeFn, feeSource, feeKnown := s.properScoreCurveFee(row.platform, row.forecast.Ticker, "taker", c.price)
	if !feeKnown {
		return 0, errors.New("proper-score one-share exact fee unavailable")
	}
	one, err := properbetting.WalkDepth(levels, 1, forecastSide, row.book.tick, row.book.lot, feeFn)
	if err != nil || one.Filled < 1-1e-9 || one.Cancelled > 1e-9 || one.ExpectedNet <= 0 {
		return 0, fmt.Errorf("proper-score one-share depth evaluation: %w", err)
	}
	verified := strings.EqualFold(identity.IdentityStatus, "verified")
	// Candidate marks the preregistered executable action under test, not trading authority. The
	// immutable schema keeps funded/Paper/LIVE authority at zero; only a later sealed untouched PASS
	// plus the separate Paper bridge can authorize an execution.
	kind, certificate := "candidate", "verified"
	blocker, candidate := "", true
	// The route ledger keeps the literal outcome-censor envelope. Expected value is a separate
	// statistical claim in Inputs and is the only field the sealed promotion gate may evaluate.
	netLower := -one.IntegratedCost - one.Fee
	if !verified {
		kind, certificate, blocker, candidate = "control", firstNonEmpty(identity.IdentityStatus, "unverified"),
			"canonical instrument identity is not verified", false
	}
	capacity := 0.0
	for _, level := range levels {
		capacity += level.Quantity
	}
	opportunityID := storage.ResearchRouteStableID("proper-score-executor", slot, mode, transform,
		row.platform, row.forecast.Ticker, c.side, row.forecast.SignalType,
		row.forecast.Title, cohort, row.book.sourceClock)
	id, _, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: now, SystemID: "proper-score-executor", ExperimentVersion: 1,
		OpportunityID: opportunityID, Kind: kind, Cohort: cohort,
		CanonicalEventID: identity.EventID, EventVersion: identity.EventVersion,
		CanonicalPayoffID: identity.PayoffID, PayoffVersion: identity.PayoffVersion,
		Venue: row.platform, Ticker: row.forecast.Ticker, Route: "taker", Side: c.side,
		CertificateStatus: certificate, SourceClockID: row.book.sourceClock,
		SourceArtifact: "ml_predictions.json:" + row.forecast.SignalType,
		BookSource:     row.book.source, FeeSource: feeSource, Blocker: blocker,
		QuoteAgeMax: row.book.age, TickMin: one.MinimumTick, Size: 1,
		Cost: one.IntegratedCost, Fee: one.Fee, PayoutLower: 0, PayoutUpper: 1,
		NetLower: netLower, NetUpper: 1 - one.IntegratedCost - one.Fee,
		VisibleCapacity: capacity, CapitalSeconds: math.Max(0, row.forecast.ResolveHours*3600),
		DecisionLatencyMS: row.latencyMS, Candidate: candidate,
		LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true, FeeKnown: true,
		CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: one.IntegratedCost, Fee: one.Fee,
			PayoutFloor: 0, NetFloor: -one.IntegratedCost - one.Fee}},
		Inputs: map[string]any{"paper": "arXiv:2607.06166v1", "strategy_mode": mode,
			"transform": transform, "forecast_yes": row.pYes, "forecast_side": forecastSide,
			"q_tilde": c.qYes, "raw_vector": c.rawVector, "constant_shift": c.shift,
			"raw_vector_is_paper_truth": true, "complete_outcome_set_book": row.book.completeBinarySet(),
			"canonical_shift_executed_as_free_trade": false,
			"normalized_weight":                      c.normalized, "portfolio_share_scale": c.rescale,
			"portfolio_requested_qty": c.requested, "portfolio_executable_qty": c.qty,
			"one_share_expected_net": one.ExpectedNet, "promotion_expected_net_lower": one.ExpectedNet,
			"open_payoff_envelope": []float64{0, 1},
			"fee_rounding":         "whole route fill set", "book_depth_levels": one.Fills},
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (s *Server) properScoreComparatorObservation(ctx context.Context, now time.Time, slot, mode,
	transform, arm string, normalizedTarget float64, selected bool, row properScorePrepared,
	c properScoreCandidate, forecastSource, forecastVersion string) (int64, error) {
	if s.store == nil {
		return 0, errors.New("proper-score comparator store unavailable")
	}
	identity, ok, err := s.store.CurrentCanonicalInstrument(ctx, row.platform, row.forecast.Ticker)
	if err != nil || !ok {
		return 0, fmt.Errorf("proper-score comparator canonical identity unavailable: %w", err)
	}
	route, size, cost, fee := "observer", 0.0, 0.0, 0.0
	payoutLow, payoutHigh, netLow, netHigh := 0.0, 0.0, 0.0, 0.0
	side := c.side
	if side == "" {
		side = "NONE"
	}
	feeSource := firstNonEmpty(c.feeSource, "not_applicable_no_trade")
	if selected && c.side != "" && c.qty >= 1-1e-9 && c.requested >= 1-1e-9 {
		route, size, cost, fee = "taker", 1, c.integrated, c.fee
		payoutHigh, netLow, netHigh = 1, -cost-fee, 1-cost-fee
	}
	cohort := strings.Join([]string{"proper-score-comparator-v1", "arm=" + arm, "mode=" + mode,
		"transform=" + transform, "forecast=" + forecastSource, "version=" + forecastVersion,
		"venue=" + row.platform, "route=" + route, "scale=l1-one-unit-v1"}, "|")
	blocker := "comparator control; zero authority by construction"
	certificate := firstNonEmpty(identity.IdentityStatus, "unverified")
	opportunityID := storage.ResearchRouteStableID("proper-score-comparator", slot, arm, mode,
		transform, row.platform, row.forecast.Ticker, row.book.sourceClock)
	curve := []storage.CapacityPoint(nil)
	outcomeStatus := "open"
	comparatorExpected := c.expected
	if route == "taker" {
		curve = []storage.CapacityPoint{{Size: 1, Cost: cost, Fee: fee,
			PayoutFloor: 0, NetFloor: -cost - fee}}
	} else {
		outcomeStatus, comparatorExpected = "settled", 0
	}
	id, inserted, err := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
		Observed: now, SystemID: "proper-score-executor", ExperimentVersion: 1,
		OpportunityID: opportunityID, Kind: "control", Cohort: cohort,
		CanonicalEventID: identity.EventID, EventVersion: identity.EventVersion,
		CanonicalPayoffID: identity.PayoffID, PayoffVersion: identity.PayoffVersion,
		Venue: row.platform, Ticker: row.forecast.Ticker, Route: route, Side: side,
		CertificateStatus: certificate, SourceClockID: row.book.sourceClock,
		SourceArtifact: "ml_predictions.json:" + row.forecast.SignalType,
		BookSource:     row.book.source, FeeSource: feeSource, Blocker: blocker,
		OutcomeStatus: outcomeStatus,
		QuoteAgeMax:   row.book.age, TickMin: row.book.tick, Size: size, Cost: cost, Fee: fee,
		PayoutLower: payoutLow, PayoutUpper: payoutHigh, NetLower: netLow, NetUpper: netHigh,
		VisibleCapacity: c.depth, CapitalSeconds: math.Max(0, row.forecast.ResolveHours*3600),
		DecisionLatencyMS: row.latencyMS, CapacityCurve: curve,
		LatencyKnown: true, QuoteAgeKnown: true, TickKnown: row.book.tick > 0,
		DepthKnown: true, FeeKnown: true, Candidate: false,
		Inputs: map[string]any{"comparator_arm": arm, "matched_transform": transform,
			"collection_slot":       slot,
			"matched_strategy_mode": mode, "matched_forecast_yes": row.pYes,
			"proper_coordinate_key": storage.ProperScoreCoordinateKey(storage.ProperScoreCoordinate{
				Platform: row.platform, Ticker: row.forecast.Ticker,
				ForecastOriginSide: row.originSide, ForecastSignal: row.forecast.SignalType,
				ForecastSource: forecastSource, ForecastVersion: forecastVersion, Route: "taker"}),
			"matched_book_source": row.book.source, "matched_source_clock_id": row.book.sourceClock,
			"normalized_one_unit_target": normalizedTarget, "selected_route": selected,
			"raw_vector_is_paper_truth": true, "complete_outcome_set_book": row.book.completeBinarySet(),
			"canonical_shift_executed_as_free_trade": false,
			"one_share_expected_net":                 comparatorExpected, "authority": "zero",
			"mode4_semantics": map[string]any{"bankroll": "excluded",
				"display_label": "Adaptive Allocation Model",
				"target":        "adaptive proof-Kelly shape normalized across the same candidate set"}},
	})
	if err != nil || !inserted || selected {
		return id, err
	}
	zero := 0.0
	_, err = s.store.AppendResearchPayoffUpdate(ctx, storage.ResearchPayoffUpdate{
		ObservationID: id, Observed: now, Status: "settled", PayoutLower: 0, PayoutUpper: 0,
		RealizedNet: &zero, SourceArtifact: "proper-score:no-trade-comparator",
		SourceHash: storage.R138HashJSON(map[string]any{"arm": arm, "slot": slot,
			"mode": mode, "transform": transform, "venue": row.platform,
			"ticker": row.forecast.Ticker, "source_clock_id": row.book.sourceClock}),
		Reason: "deterministic zero-position comparator payoff",
	})
	return id, err
}

func properScoreCohort(mode, transform, forecastSource, forecastVersion, platform, side, book, fee string) string {
	return strings.Join([]string{"proper-score-v2", "mode=" + mode, "transform=" + transform,
		"forecast=" + strings.TrimSpace(forecastSource), "version=" + strings.TrimSpace(forecastVersion),
		"venue=" + platform, "side=" + side, "route=taker", "book=" + book, "fee=" + fee,
		"scale=l1-one-unit-plus-one-share-capacity-v1"}, "|")
}

func properScoreForecastKey(f properScoreForecast) string {
	return strings.ToLower(strings.TrimSpace(f.Platform)) + "|" + strings.TrimSpace(f.Ticker) + "|" +
		strings.ToUpper(strings.TrimSpace(f.Side)) + "|" + strings.TrimSpace(f.SignalType)
}

func properScoreSelectionOrdinal(slot string) int64 {
	if parsed, err := time.Parse(time.RFC3339, slot); err == nil {
		return parsed.UTC().Unix() / int64(properScoreSelectionCadence/time.Second)
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(slot))
	return int64(h.Sum64() & math.MaxInt64)
}

// properScoreCanStartPhase leaves time for one bounded transform and the collector receipt. The
// collector never intentionally freezes a manifest after this cutoff: if a page cannot start or a
// later transform must wait, the durable receipt names that deferral and the next five-minute page
// proceeds normally. This is a safety backstop; the eight-persona cap is the primary bound.
func properScoreCanStartPhase(ctx context.Context, started, now time.Time) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	cutoff := started.Add(properScoreLaneBudget - properScoreReceiptReserve)
	if deadline, ok := ctx.Deadline(); ok {
		parentCutoff := deadline.Add(-properScoreReceiptReserve)
		if parentCutoff.Before(cutoff) {
			cutoff = parentCutoff
		}
	}
	return cutoff.Sub(now) >= properScorePhaseReserve
}

func properScoreBoundForecasts(rows []properScoreForecast, slot string, limit int) []properScoreForecast {
	uniq := make(map[string]properScoreForecast, len(rows))
	for _, r := range rows {
		k := properScoreForecastKey(r)
		if k != "|||" {
			uniq[k] = r
		}
	}
	type ranked struct {
		h uint64
		f properScoreForecast
	}
	all := make([]ranked, 0, len(uniq))
	for k, f := range uniq {
		h := fnv.New64a()
		_, _ = h.Write([]byte(k))
		all = append(all, ranked{h: h.Sum64(), f: f})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].h == all[j].h {
			return properScoreForecastKey(all[i].f) < properScoreForecastKey(all[j].f)
		}
		return all[i].h < all[j].h
	})
	if limit <= 0 || len(all) <= limit {
		out := make([]properScoreForecast, len(all))
		for i := range all {
			out[i] = all[i].f
		}
		return out
	}
	// Consecutive five-minute slots advance one complete page. Unlike a fresh pseudo-random sample,
	// this guarantees every stable forecast key is selected once before any page repeats.
	pages := (len(all) + limit - 1) / limit
	page := int(properScoreSelectionOrdinal(slot) % int64(pages))
	if page < 0 {
		page += pages
	}
	start := page * limit
	end := start + limit
	if end > len(all) {
		end = len(all)
	}
	out := make([]properScoreForecast, end-start)
	for i := start; i < end; i++ {
		out[i-start] = all[i].f
	}
	return out
}

func properScoreUniqueForecastCount(rows []properScoreForecast) int {
	uniq := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := properScoreForecastKey(row)
		if key != "|||" {
			uniq[key] = struct{}{}
		}
	}
	return len(uniq)
}

// properScoreCurrentHorizonForecasts keeps the fast proof lane aligned with markets that could
// transfer to funded Paper/LIVE today. ResolveHours is immutable time-to-close at signal time, so
// rows with a source timestamp are decayed to their current remaining horizon; otherwise a market
// first seen five hours out could never enter a four-hour lane later. Longer-dated forecasts remain
// in the source file and historical ledger. Limits match funded singles.
func properScoreCurrentHorizonForecasts(rows []properScoreForecast, regular, crypto float64,
	now time.Time) []properScoreForecast {
	out := make([]properScoreForecast, 0, len(rows))
	for _, row := range rows {
		limit := regular
		if isCryptoTicker(row.Ticker + " " + row.Title) {
			limit = crypto
		}
		remaining := row.ResolveHours
		if signalAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(row.SignalTS)); err == nil &&
			row.ResolveHours > 0 {
			remaining = signalAt.UTC().Add(time.Duration(row.ResolveHours * float64(time.Hour))).Sub(now.UTC()).Hours()
		}
		if paperEntryHorizonEligible(remaining, limit) {
			// This is a copied decision-time row; preserve the immutable source file while ensuring
			// downstream capital-seconds and manifests record today's remaining clock, not its age at
			// signal creation.
			row.ResolveHours = remaining
			out = append(out, row)
		}
	}
	return out
}

// properScoreManifestSlot keeps the five-minute persona page stable while giving each immutable
// vector universe its own durable identity. Book lifecycle and model retraining can legitimately
// change the prepared coordinate set inside one slot. Reusing the bare UTC slot key made a retry
// collide with the first immutable manifest and fail every write until the next slot. The complete
// SHA-256 fingerprint is deterministic under row reordering, so an identical
// retry deduplicates while any model/source/coordinate change appends a separate replayable slot.
func properScoreManifestSlot(baseSlot, forecastSource, forecastVersion string,
	rows []properScorePrepared) string {
	keys := make([]string, 0, len(rows))
	for _, row := range rows {
		keys = append(keys, storage.ProperScoreCoordinateKey(storage.ProperScoreCoordinate{
			Platform: row.platform, Ticker: row.forecast.Ticker,
			ForecastOriginSide: row.originSide, ForecastSignal: row.forecast.SignalType,
			ForecastSource: forecastSource, ForecastVersion: forecastVersion, Route: "taker",
		}))
	}
	sort.Strings(keys)
	h := sha256.New()
	_, _ = h.Write([]byte("proper-score-manifest-slot-v2\x00"))
	_, _ = h.Write([]byte(strings.TrimSpace(forecastSource)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strings.TrimSpace(forecastVersion)))
	for _, key := range keys {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(key))
	}
	return strings.TrimSpace(baseSlot) + "|vector=" + fmt.Sprintf("%x", h.Sum(nil))
}

func properScoreSelectionPage(total, limit int, slot string) (page, pages int) {
	if total <= 0 || limit <= 0 || total <= limit {
		if total > 0 {
			return 0, 1
		}
		return 0, 0
	}
	pages = (total + limit - 1) / limit
	page = int(properScoreSelectionOrdinal(slot) % int64(pages))
	if page < 0 {
		page += pages
	}
	return page, pages
}

func (s *Server) sweepProperScore(ctx context.Context) {
	started := time.Now()
	cycleID := started.UTC().Truncate(5 * time.Minute).Format(time.RFC3339)
	live := properScoreLiveness{AsOf: started.UTC().Format(time.RFC3339Nano)}
	exclusions := map[string]int{}
	record := func(status string, expectedZero bool, zeroReason, errorClass, errorText string) {
		live.CycleDurationMS = float64(time.Since(started).Microseconds()) / 1000
		live.WriteBudgetRemainingMS = math.Max(0,
			float64((properScoreLaneBudget-time.Since(started)).Microseconds())/1000)
		transformRows := live.Actions + live.Abstains
		duplicates := transformRows - live.InsertedRows - live.InsertErrors
		if duplicates < 0 {
			duplicates = 0
		}
		durableCtx, cancelDurability := researchDurabilityContext(ctx)
		defer cancelDurability()
		_, _ = s.store.InsertCollectorReceipt(durableCtx, storage.CollectorReceipt{
			CollectorID: "proper-score", CycleID: cycleID, ExperimentID: "proper-score-executor",
			ExperimentVersion: 1, Status: status, Started: started, Completed: time.Now(),
			Eligible: live.CompleteBooks * 6, Attempted: live.BoundedRows * 6,
			Inserted: live.InsertedRows, Duplicates: duplicates, Exclusions: exclusions,
			ExpectedZero: expectedZero, ZeroReason: zeroReason, ErrorClass: errorClass, ErrorText: errorText,
			Source: "ml_predictions book-native-v2 all_scored + current complete venue full-depth books", SchemaVersion: "proper-score-paper-v2",
			ExpectedCadence: 5 * time.Minute, Systems: []string{"proper-score-executor", "forecast-persona-router"},
			Metrics: map[string]any{"source_rows": live.SourceRows, "unique_rows": live.UniqueRows,
				"current_horizon_eligible_rows": live.HorizonEligibleRows,
				"outside_current_horizon_rows":  live.HorizonDeferredRows,
				"current_complete_book_rows":    live.BookEligibleRows,
				"bounded_rows":                  live.BoundedRows, "deferred_rows": live.DeferredRows,
				"budget_deferred_rows": live.BudgetDeferredRows,
				"budget_stage":         live.BudgetStage,
				"completed_transforms": live.CompletedTransforms,
				"selection_page":       live.SelectionPage, "selection_pages": live.SelectionPages,
				"selection_budget_reason":   properScoreBudgetReason,
				"cycle_duration_ms":         live.CycleDurationMS,
				"write_budget_remaining_ms": live.WriteBudgetRemainingMS,
				"error_samples":             live.ErrorSamples,
				"complete_books":            live.CompleteBooks, "actions": live.Actions, "abstains": live.Abstains,
				"insert_errors": live.InsertErrors, "forecast_generated_at": live.GeneratedAt},
		})
		s.researchMu.Lock()
		s.properScoreLive = live
		s.researchMu.Unlock()
	}
	deferForBudget := func(stage string, rows int) {
		if rows < 1 {
			rows = 1
		}
		live.BudgetStage = stage
		live.BudgetDeferredRows += rows
		exclusions["budget_deferred_before_"+stage] += rows
		if live.InsertErrors > 0 {
			record("error", false, "", "storage_write",
				"proper-score work budget ended after one or more storage errors")
			return
		}
		record("deferred", false, "", "", "")
	}
	path := filepath.Join(s.cfg().DataDir, "ml_predictions.json")
	fi, err := os.Stat(path)
	if err != nil {
		record("healthy_empty", true, "unselected forecast file absent", "", "")
		return
	}
	if time.Since(fi.ModTime()) < 0 || time.Since(fi.ModTime()) > properScoreForecastMaxAge {
		record("starved", false, "", "stale_source", "unselected forecast file is stale")
		return
	}
	var file properScoreForecastFile
	if !s.readJSONLoose(path, &file) || file.ForecastSource != "ml-book-v2-calibrated" || file.FeatureSchema != "book-native-v2" ||
		strings.TrimSpace(file.ModelVersion) == "" ||
		file.GeneratedAt <= 0 || math.Abs(time.Since(time.Unix(file.GeneratedAt, 0)).Seconds()) > properScoreForecastMaxAge.Seconds() {
		record("blocked", false, "", "forecast_schema", "unselected forecast manifest failed the frozen schema/version contract")
		return
	}
	// PAPER_PROVISIONAL changes execution authority, not forecast usability for this zero-authority
	// research collector. Both states carry the same book-native schema, current all_scored rows,
	// frozen model version and source clock checks above. Rejecting the provisional state silently
	// starved every new normalized Proper Betting cohort exactly when Paper exploration started.
	if file.ModelStatus != "ACTIVE_RESEARCH" && file.ModelStatus != "PAPER_PROVISIONAL" {
		if file.ModelStatus == "WARMING" && len(file.AllScored) == 0 {
			live.GeneratedAt = file.GeneratedAt
			exclusions["model_warming"]++
			record("healthy_empty", true, "book-native model is warming and emitted no unselected forecasts", "", "")
			return
		}
		record("blocked", false, "", "forecast_status", "unselected forecast manifest is not ACTIVE_RESEARCH/PAPER_PROVISIONAL or the exact WARMING empty state")
		return
	}
	now := time.Now().UTC()
	slot := now.Truncate(properScoreSelectionCadence).Format(time.RFC3339)
	regularHorizon, cryptoHorizon := paperEntryHorizonLimits(s.cfg().Auto)
	horizonRows := properScoreCurrentHorizonForecasts(file.AllScored, regularHorizon, cryptoHorizon, now)
	uniqueRows := properScoreUniqueForecastCount(file.AllScored)
	eligibleRows := properScoreUniqueForecastCount(horizonRows)
	bookPage := properScoreSelectBookPage(horizonRows, slot, properScoreCycleCap,
		func(platform, ticker string) (properScoreBook, bool) {
			return s.properScoreCurrentBook(ctx, platform, ticker)
		})
	bounded := bookPage.forecasts
	live = properScoreLiveness{AsOf: now.Format(time.RFC3339Nano), Slot: slot,
		GeneratedAt: file.GeneratedAt, SourceRows: len(file.AllScored), UniqueRows: uniqueRows,
		HorizonEligibleRows: eligibleRows, BoundedRows: len(bounded),
		InvalidRows: bookPage.invalid, NoBook: bookPage.noBook,
		BookEligibleRows: bookPage.available, CompleteBooks: len(bounded),
		DeferredRows:        max(0, bookPage.available-len(bounded)),
		HorizonDeferredRows: max(0, uniqueRows-eligibleRows),
		SelectionPage:       bookPage.page + 1, SelectionPages: bookPage.pages}
	if live.HorizonDeferredRows > 0 {
		exclusions["outside_current_paper_horizon"] = live.HorizonDeferredRows
	}
	if live.InvalidRows > 0 {
		exclusions["invalid_forecast_row"] = live.InvalidRows
	}
	if live.NoBook > 0 {
		exclusions["no_current_complete_book"] = live.NoBook
	}
	if live.DeferredRows > 0 {
		exclusions["bounded_fair_rotation_deferred"] = live.DeferredRows
	}
	prepared := make([]properScorePrepared, 0, len(bounded))
	for i, f := range bounded {
		if !properScoreCanStartPhase(ctx, started, time.Now()) {
			// A partial page must not become a smaller, biased position vector. Nothing has been
			// manifested yet, so defer the whole selected page rather than silently dropping its tail.
			deferForBudget("page_prepare", len(bounded)-i)
			return
		}
		platform, side := strings.ToLower(strings.TrimSpace(f.Platform)), strings.ToUpper(strings.TrimSpace(f.Side))
		pYes := f.PWin
		if side == "NO" {
			pYes = 1 - f.PWin
		}
		book := bookPage.books[properScoreForecastKey(f)]
		_, identityOK, identityErr := s.ensureExactCatalogCanonicalInstrument(ctx, platform, f.Ticker)
		if identityErr != nil {
			live.InsertErrors++
			exclusions["canonical_identity_ensure_failed"]++
			if len(live.ErrorSamples) < 8 {
				live.ErrorSamples = append(live.ErrorSamples,
					properScoreErrorSample("canonical-identity-ensure", platform, f.Ticker, identityErr))
			}
			continue
		}
		if !identityOK {
			exclusions["canonical_identity_unavailable"]++
		}
		row := properScorePrepared{forecast: f, platform: platform, originSide: side, pYes: pYes,
			latencyMS: bookPage.latencyMS[properScoreForecastKey(f)], canonicalIdentity: identityOK, book: book,
			candidates:  map[string]properScoreCandidate{},
			abstentions: map[string]string{}}
		for _, transform := range []string{"brier", "log", "spherical"} {
			candidate, abstain := properScoreRawCandidate(transform, pYes, book)
			if abstain == "" && !identityOK {
				abstain = "canonical_identity_unavailable"
			}
			if abstain == "" && (strings.TrimSpace(book.sourceClock) == "" || book.sourceClockAt.IsZero()) {
				abstain = "source_clock_unavailable"
			}
			row.candidates[transform] = candidate
			row.abstentions[transform] = abstain
		}
		prepared = append(prepared, row)
	}
	if len(prepared) > 0 {
		// Selection still rotates by the plain five-minute page above. The stored experiment slot also
		// freezes the exact model/source/coordinate universe, preventing a legitimate same-slot
		// lifecycle or retrain change from attempting to mutate an earlier manifest.
		slot = properScoreManifestSlot(slot, file.ForecastSource, file.ModelVersion, prepared)
		live.Slot = slot
	}

	transforms := []string{"brier", "log", "spherical"}
	paperVectors := make([]properScorePaperVector, 0, len(transforms))
	for transformIndex, transform := range transforms {
		if len(prepared) > 0 && !properScoreCanStartPhase(ctx, started, time.Now()) {
			deferForBudget("transform_"+transform,
				len(prepared)*(len(transforms)-transformIndex))
			return
		}
		weights, normalizeErr := properScoreNormalizedWeights(prepared, transform)
		if normalizeErr != nil {
			weights = make([]float64, len(prepared))
		}
		evaluated := make([]properScoreCandidate, len(prepared))
		reasons := make([]string, len(prepared))
		equalTargets, mode4Raw := make([]float64, len(prepared)), make([]float64, len(prepared))
		eligible, maxMarginIndex, maxMargin := 0, -1, math.Inf(-1)
		for i, row := range prepared {
			c, abstain := row.candidates[transform], row.abstentions[transform]
			if abstain == "" {
				if normalizeErr != nil {
					abstain = "portfolio_normalization_zero"
				} else {
					c, abstain = s.properScoreEvaluate(row.platform, row.forecast.Ticker, transform,
						row.pYes, row.book, c, math.Abs(weights[i]))
				}
			}
			evaluated[i], reasons[i] = c, abstain
		}
		// Raw scoring-rule coordinates were normalized above only to make the one-share route
		// probes well-defined.  Fee, spread, depth or lot-size gates can remove coordinates after
		// that first pass.  Re-normalize the actual executable survivors before any trial or
		// comparator row is frozen so each transform/slot still represents one L1 position unit.
		executableWeights, executableNormalizeErr := properScoreExecutableWeights(prepared, transform,
			evaluated, reasons)
		if executableNormalizeErr != nil {
			for i := range evaluated {
				evaluated[i].normalized = 0
				if reasons[i] == "" {
					reasons[i] = "portfolio_normalization_zero"
				}
			}
		} else {
			for i := range evaluated {
				evaluated[i].normalized = math.Abs(executableWeights[i])
			}
		}
		for i, c := range evaluated {
			abstain := reasons[i]
			if abstain == "" && c.qty >= 1-1e-9 {
				eligible++
				if c.expected > maxMargin+1e-12 ||
					(math.Abs(c.expected-maxMargin) <= 1e-12 &&
						(maxMarginIndex < 0 || prepared[i].forecast.Ticker < prepared[maxMarginIndex].forecast.Ticker)) {
					maxMargin, maxMarginIndex = c.expected, i
				}
				effective := adaptiveProofKellyFrac(s.cfg().Risk.LiveKellyMaxFrac, c.expected,
					c.expected, s.liveMirrorEdgeFloor(), 1)
				fstar := c.expected / math.Max(1e-12, 1-c.price)
				mode4Raw[i] = effective * math.Min(1, math.Max(0, fstar))
			}
		}
		if eligible > 0 {
			for i := range evaluated {
				if reasons[i] == "" && evaluated[i].qty >= 1-1e-9 {
					equalTargets[i] = 1 / float64(eligible)
				}
			}
		}
		mode4Targets := make([]float64, len(prepared))
		if normalized, err := properbetting.NormalizeWeights(mode4Raw, 1); err == nil {
			for i := range normalized {
				mode4Targets[i] = math.Abs(normalized[i])
			}
		}
		// Freeze the exact vector universe before the first trial/comparator write.  Trial insertion
		// subsequently requires membership in this manifest, so a failed coordinate remains durably
		// missing instead of shrinking the slot into a falsely "complete" vector.
		manifestCoordinates := make([]storage.ProperScoreCoordinate, 0, len(prepared))
		for _, row := range prepared {
			manifestCoordinates = append(manifestCoordinates, storage.ProperScoreCoordinate{
				Platform: row.platform, Ticker: row.forecast.Ticker,
				ForecastOriginSide: row.originSide, ForecastSignal: row.forecast.SignalType,
				ForecastSource: file.ForecastSource, ForecastVersion: file.ModelVersion, Route: "taker",
			})
		}
		if len(prepared) > 0 && !properScoreCanStartPhase(ctx, started, time.Now()) {
			// Do not freeze an immutable manifest unless this complete transform still has its
			// bounded write window. Earlier transforms, if any, are already complete and durable.
			deferForBudget("manifest_"+transform,
				len(prepared)*(len(transforms)-transformIndex))
			return
		}
		manifestReady := len(manifestCoordinates) > 0
		for _, mode := range []string{"fundamental", "momentum"} {
			if !manifestReady {
				break
			}
			if _, _, manifestErr := s.store.RegisterProperScoreManifest(ctx, storage.ProperScoreManifest{
				Created: now, Slot: slot, Transform: transform, StrategyMode: mode,
				ForecastSource: file.ForecastSource, ForecastVersion: file.ModelVersion,
				Coordinates: manifestCoordinates,
			}); manifestErr != nil {
				live.InsertErrors++
				exclusions["manifest_freeze_failed"]++
				manifestReady = false
			}
		}
		if len(prepared) > 0 && !manifestReady {
			continue
		}
		for i, row := range prepared {
			c, abstain := evaluated[i], reasons[i]
			for _, mode := range []string{"fundamental", "momentum"} {
				modeAbstain := abstain
				qYes := c.qYes
				if qYes <= 0 || qYes >= 1 || math.IsNaN(qYes) || math.IsInf(qYes, 0) {
					qYes = (row.book.yesBid + row.book.yesAsk) / 2
				}
				rescale := c.rescale
				if rescale <= 0 || math.IsNaN(rescale) || math.IsInf(rescale, 0) {
					rescale = 1
				}
				feeSource := c.feeSource
				if feeSource == "" {
					feeSource = "not-applicable-" + firstNonEmpty(abstain, "no-route")
				}
				cohortSide := firstNonEmpty(c.side, "NONE")
				cohort := properScoreCohort(mode, transform, file.ForecastSource, file.ModelVersion,
					row.platform, cohortSide, row.book.source, feeSource)
				comparators := []struct {
					arm      string
					target   float64
					selected bool
				}{
					{arm: "equal-share", target: equalTargets[i], selected: equalTargets[i] > 0},
					{arm: "max-margin", target: map[bool]float64{true: 1, false: 0}[i == maxMarginIndex],
						selected: i == maxMarginIndex},
					{arm: "mode4-normalized", target: mode4Targets[i], selected: mode4Targets[i] > 0},
					{arm: "no-trade", target: 0, selected: false},
				}
				if !row.canonicalIdentity {
					exclusions["comparator_canonical_identity_unavailable"] += len(comparators)
				} else {
					for _, comparator := range comparators {
						if _, comparatorErr := s.properScoreComparatorObservation(ctx, now, slot, mode,
							transform, comparator.arm, comparator.target, comparator.selected,
							row, c, file.ForecastSource, file.ModelVersion); comparatorErr != nil {
							live.InsertErrors++
							exclusions["comparator_write_failed"]++
							if len(live.ErrorSamples) < 8 {
								live.ErrorSamples = append(live.ErrorSamples,
									properScoreErrorSample("comparator-write", row.platform,
										row.forecast.Ticker, comparatorErr))
							}
						}
					}
				}
				actualPrior, _ := s.store.ProperScoreActualPosition(ctx, mode, transform, row.platform,
					row.forecast.Ticker, file.ForecastSource, file.ModelVersion)
				sim := properScoreSimResult{}
				if abstain == "" {
					var simErr error
					sim, simErr = s.simulateProperScoreImmediateTaker(ctx, now, slot, mode, transform,
						row, c, cohort, file.ForecastSource, file.ModelVersion)
					if simErr != nil {
						live.InsertErrors++
						exclusions["simulation_write_failed"]++
						modeAbstain = "simulation_write_failed"
					} else if sim.status == "noop" {
						modeAbstain = "momentum_rebalance_noop"
					} else if len(sim.legs) == 0 {
						modeAbstain = "simulation_route_blocked"
					} else if action, _ := sim.legs[0]["action"].(string); action != "BUY" {
						sellLeg, singleSell := properMomentumSingleSell(sim)
						switch {
						case mode != "momentum":
							modeAbstain = "fundamental_sell_research_only"
						case !singleSell || len(sim.legs) != 1:
							// A close+open cross is two non-atomic actions. Neither Paper nor LIVE may
							// pretend the pair filled just because its simulated legs did.
							modeAbstain = "momentum_cross_blocked_non_atomic"
						case row.platform != "kalshi":
							// PolyUS retail exposes IOC/GTC here and returns only an order ID. A later
							// state read cannot make an already-partial reduction fail closed.
							modeAbstain = "momentum_polyus_sell_blocked_no_fok_receipt"
						case sim.expected <= 0:
							modeAbstain = "momentum_sell_no_positive_expected_delta"
						default:
							_ = sellLeg // the typed observation revalidates every exact leg field.
							observationID, observationErr := s.properScoreMomentumSellObservation(ctx, now,
								slot, transform, row, sim, cohort)
							if observationErr != nil {
								live.InsertErrors++
								exclusions["momentum_sell_observation_write_failed"]++
								modeAbstain = "momentum_sell_observation_write_failed"
							} else {
								_ = observationID // generic terminal grader owns the synthetic SELL payoff.
								modeAbstain = "momentum_kalshi_sell_waiting_sealed_proof"
							}
						}
					}
				}
				trial := storage.ProperScoreTrial{Observed: now, Slot: slot,
					System: "proper-score-" + transform, Transform: transform, StrategyMode: mode,
					Cohort: cohort, Platform: row.platform, Ticker: row.forecast.Ticker,
					Title: row.forecast.Title, Category: row.forecast.Category,
					ResolveHours: row.forecast.ResolveHours, ForecastYes: row.pYes,
					ForecastOriginSide: row.originSide, ForecastSignal: row.forecast.SignalType,
					ForecastSource: file.ForecastSource, ForecastVersion: file.ModelVersion,
					ModelBackend: file.ModelBackend, Calibration: file.Calibration, GeneratedAt: file.GeneratedAt,
					Route: "taker", YesBid: row.book.yesBid, YesAsk: row.book.yesAsk,
					YesBidDepth: row.book.yesBidDepth, YesAskDepth: row.book.yesAskDepth,
					NoBid: row.book.noBid, NoAsk: row.book.noAsk,
					NoBidDepth: row.book.noBidDepth, NoAskDepth: row.book.noAskDepth,
					BookSource: row.book.source, SourceClockID: row.book.sourceClock,
					QuoteAge: row.book.age, DecisionLatencyMS: row.latencyMS, QYes: qYes,
					RawYes: c.rawYes, RawNo: c.rawNo, RawVector: c.rawVector, VectorDim: 2,
					ConstantShift: c.shift, Rescale: rescale,
					NormalizedWeight: c.normalized, CanonicalQty: c.canonical,
					RequestedQty: c.requested, TickSize: c.tick, LotSize: c.lot,
					PriorActualPosition: actualPrior, TargetPosition: actualPrior, ActualFilledDelta: 0,
					CancelledDelta: 0, PostActualPosition: actualPrior,
					FillStatus: "counterfactual", ActualFill: false, AbstainReason: modeAbstain}
				if modeAbstain == "" {
					oneShareBuy := sim.inserted && sim.status == "filled" && len(sim.legs) == 1 &&
						math.Abs(math.Abs(sim.requested)-1) <= 1e-8
					if oneShareBuy {
						action, _ := sim.legs[0]["action"].(string)
						oneShareBuy = action == "BUY"
					}
					if oneShareBuy {
						observationID, observationErr := s.properScoreSystemObservation(ctx, now, slot, mode,
							transform, row, c, cohort)
						if observationErr != nil {
							live.InsertErrors++
							exclusions["common_observation_write_failed"]++
							if len(live.ErrorSamples) < 8 {
								live.ErrorSamples = append(live.ErrorSamples,
									properScoreErrorSample("common-observation-write", row.platform,
										row.forecast.Ticker, observationErr))
							}
						}
						trial.ResearchObservationID = observationID
					}
					live.Actions++
					trial.ExecutableQty, trial.SelectedSide = c.qty, c.side
					trial.EntryPrice, trial.EntryDepth = c.price, c.depth
					trial.IntegratedCost, trial.SpotCost, trial.LiquidityLoss = c.integrated, c.spot, c.liquidityLoss
					trial.DepthCurve = c.depthCurve
					trial.ExactFeeTotal, trial.FeeSource, trial.ExpectedNet = c.fee, c.feeSource, c.expected
				} else {
					live.Abstains++
					exclusions[modeAbstain]++
				}
				if inserted, insertErr := s.store.InsertProperScoreTrial(ctx, trial); insertErr != nil {
					live.InsertErrors++
					exclusions["trial_write_failed"]++
					if len(live.ErrorSamples) < 8 {
						live.ErrorSamples = append(live.ErrorSamples,
							properScoreErrorSample("trial-write", row.platform, row.forecast.Ticker, insertErr))
					}
				} else if inserted {
					live.InsertedRows++
				}
			}
		}
		if len(prepared) > 0 && manifestReady {
			live.CompletedTransforms++
			paperVectors = append(paperVectors, properScorePaperVector{
				transform: transform, rows: append([]properScorePrepared(nil), prepared...),
				candidates: append([]properScoreCandidate(nil), evaluated...),
				reasons:    append([]string(nil), reasons...),
			})
		}
	}
	// A Paper comparison starts only after all three transforms completed the same immutable
	// vector. That keeps Brier/log/spherical simultaneous and prevents a budget-truncated pass from
	// giving one lane a different opportunity set. The helper persists attempts, waits once, and
	// re-reads executable books before any simulated fill.
	if len(paperVectors) == len(transforms) {
		if paperErr := s.runProperScorePaperVectors(ctx, now, slot, paperVectors,
			file.ForecastSource, file.ModelVersion); paperErr != nil {
			live.InsertErrors++
			exclusions["proper_betting_paper_portfolio_failed"]++
			if len(live.ErrorSamples) < 8 {
				live.ErrorSamples = append(live.ErrorSamples,
					properScoreErrorSample("paper-portfolio", "multi", slot, paperErr))
			}
		}
	}
	if live.InsertErrors > 0 {
		record("error", false, "", "storage_write", "one or more proper-score rows failed validation or persistence")
		return
	}
	expectedZero := live.InsertedRows == 0 && live.BoundedRows == 0
	zeroReason := ""
	if expectedZero {
		switch {
		case live.HorizonEligibleRows == 0:
			zeroReason = "unselected forecast cohort had no current-horizon coordinates"
		case live.BookEligibleRows == 0:
			zeroReason = "every current-horizon coordinate lacked a fresh complete executable book"
		default:
			zeroReason = "unselected forecast cohort was empty"
		}
	}
	record("healthy", expectedZero, zeroReason, "", "")
}

func (s *Server) settleProperScoreTrials(ctx context.Context) {
	s.researchMu.Lock()
	if time.Since(s.lastProperScoreSettleAt) < time.Minute {
		s.researchMu.Unlock()
		return
	}
	s.lastProperScoreSettleAt = time.Now()
	s.researchMu.Unlock()
	// Reservation recovery is a background ledger responsibility, not a side effect of finding a
	// new complete three-transform vector. This immediate monitor pass also backstops boot recovery
	// if the process died after claiming a delayed Paper IOC.
	if _, err := s.store.RecoverProperScorePaperAttempts(ctx, time.Now().UTC()); err != nil {
		if s.log != nil {
			s.log.Warn("Proper Betting Paper pending-reservation recovery deferred", "err", err)
		}
		return
	}
	_, _ = s.store.ResolveProperScoreTrials(ctx, 4000)
	_, _ = s.store.ResolveProperScorePaperPortfolios(ctx, 4000)
}

func (s *Server) handleProperScore(w http.ResponseWriter, r *http.Request) {
	report, err := s.store.ProperScoreReport(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	paperPortfolios, paperErr := s.store.ProperScorePaperPortfolioReport(r.Context())
	if paperErr != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": paperErr.Error()})
		return
	}
	report["paper_portfolios"] = paperPortfolios
	report["collector"] = map[string]any{"source": "fresh book-native-v2 ml_predictions.json all_scored (unselected)",
		"current_book_only": true, "full_depth": true, "rest_fallback": false,
		"current_funded_horizon_only":      true,
		"five_minute_forecast_persona_cap": properScoreCycleCap,
		"selection_cadence_seconds":        properScoreSelectionCadence.Seconds(),
		"selection_policy":                 properScoreBudgetReason,
		"portfolio_normalization":          "legacy research: L1 sum(abs(weights))=1 share unit; new Proper Betting Paper: each transform owns an isolated $400 cash ledger and a 5% vector budget",
		"strategy_parity":                  "practical proper-score batch variant: uses the paper's Brier/log/spherical position vectors, but does not claim the paper's full momentum inventory-rebalancing implementation",
		"paper_execution":                  "configured delay, second fresh complete book, original-limit IOC, visible depth, exact aggregate fee; never a venue order"}
	report["persona_router"] = map[string]any{"math_available": []string{"brier", "log", "spherical"},
		"currently_collecting": []string{"brier", "log", "spherical"},
		"paper_method_set":     "Brier, logarithmic and spherical are the paper's three evaluated proper rules; max-margin, inverse-margin, Kelly-like and Kelly are comparator strategies",
		"strategy_modes":       []string{"fundamental", "momentum"}, "selected": "abstain",
		"gate": "persona routing requires a frozen event/day untouched replication with matching code/data/forecast manifests and a positive executable transform lower bound",
		"execution_parity": map[string]any{
			"fundamental_buy":             "generic sealed one-share Paper -> armed LIVE BUY bridge",
			"momentum_kalshi_single_sell": "separate sealed exact-position Paper SELL -> reduce-only fill-or-kill LIVE SELL",
			"momentum_cross":              "BLOCKED: close+open is non-atomic and cannot fail closed after the first fill",
			"momentum_polyus_sell":        "BLOCKED: verified retail writer offers IOC/GTC and returns an order id before full-fill truth; no fill-or-kill receipt",
		}}
	s.researchMu.Lock()
	live := s.properScoreLive
	s.researchMu.Unlock()
	report["liveness"] = map[string]any{"as_of": live.AsOf, "slot": live.Slot,
		"forecast_generated_at": live.GeneratedAt, "source_rows": live.SourceRows,
		"unique_rows": live.UniqueRows, "current_horizon_eligible_rows": live.HorizonEligibleRows,
		"outside_current_horizon_rows": live.HorizonDeferredRows, "bounded_rows": live.BoundedRows,
		"deferred_rows": live.DeferredRows, "budget_deferred_rows": live.BudgetDeferredRows,
		"budget_stage": live.BudgetStage, "completed_transforms": live.CompletedTransforms,
		"selection_page":  live.SelectionPage,
		"selection_pages": live.SelectionPages, "selection_budget_reason": properScoreBudgetReason,
		"cycle_duration_ms":         live.CycleDurationMS,
		"write_budget_remaining_ms": live.WriteBudgetRemainingMS,
		"error_samples":             live.ErrorSamples, "invalid_rows": live.InvalidRows,
		"excluded_no_current_complete_book": live.NoBook, "eligible_complete_books": live.CompleteBooks,
		"transform_actions": live.Actions, "transform_abstains": live.Abstains,
		"new_rows_inserted": live.InsertedRows, "insert_errors": live.InsertErrors}
	writeJSON(w, http.StatusOK, report)
}

// handleProperScorePaper serves only the three funded Paper ledgers. The dashboard must not run
// the much heavier full Proper research report merely to keep Brier/log/spherical balances visible.
func (s *Server) handleProperScorePaper(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	portfolios, err := s.store.ProperScorePaperPortfolioReport(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"paper_portfolios": portfolios})
}
