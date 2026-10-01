package server

// Unfunded, prospective weather research systems.
//
//   weather-curve-residual: validates the mutually-exclusive temperature ladder, converts
//   Kalshi's TRADE-DERIVED Forecast Graph percentiles into an endogenous curve-mass simplex, then
//   measures residuals against contemporaneous YES/NO books and exact one-share taker fees.
//   This is internal-consistency/lag telemetry, never independent probability or fair value.
//
//   weather-curve-revision: detects a genuine change in that trade-derived percentile vector and
//   records the whole executable ladder at honest fixed horizons. A research marker is logged only
//   when the curve median moves into a different bin; it carries no promotion authority.
//
//   independent-probabilistic-weather: consumes the separately fetched official NOAA NBM
//   station bulletin, joins its source-run daily-maximum quantiles to the exact NWS-CLI settlement
//   station/local date and exhaustive ladder, then compares conservative probability intervals
//   to current executable books. It is independent research but still has zero money authority.
//
// Both producers call Store.InsertSignalResult directly. They deliberately bypass Server's
// insertSignal hook, so generic-follow, unit-trial, paper, weather-book, live-mirror and live order
// dispatch cannot run. The audit cohort is the durable prospective ledger; signal_log supplies the
// ordinary Kalshi settlement path for admitted one-share observations.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	wxCurveResidualFamily  = "weather-curve-residual"
	wxCurveRevisionFamily  = "weather-curve-revision"
	wxCurveResidualModel   = "weather-curve-residual-v1"
	wxCurveRevisionModel   = "weather-curve-revision-v1"
	wxResearchSource       = "kalshi-trade-derived-forecast-graph"
	wxForecastIndependence = "MARKET_DERIVED: Kalshi says Forecast Graph values come from collective trades; this is endogenous telemetry, not independent weather fair value"
	wxResearchMaxEvents    = 2
	wxResearchMaxFrameAge  = 2 * time.Hour
	wxResearchUnavailEvery = 6 * time.Hour
)

var (
	wxResearchPercentiles = []int{0, 100, 1000, 2500, 5000, 7500, 9000, 9900, 9999}
	wxRevisionHorizons    = []int64{0, 30 * 60, 60 * 60, 2 * 60 * 60}
	errWxNoExecutableBook = errors.New("no side has a complete fresh executable book and exact fee")
)

type wxResearchBook struct {
	Ticker        string  `json:"ticker"`
	Title         string  `json:"title"`
	LoF           float64 `json:"lo_f"`
	HiF           float64 `json:"hi_f"`
	CurveMass     float64 `json:"curve_mass,omitempty"`
	YesExecutable bool    `json:"yes_executable"`
	NoExecutable  bool    `json:"no_executable"`
	YesBid        float64 `json:"yes_bid,omitempty"`
	YesAsk        float64 `json:"yes_ask,omitempty"`
	NoBid         float64 `json:"no_bid,omitempty"`
	NoAsk         float64 `json:"no_ask,omitempty"`
	YesBidDepth   float64 `json:"yes_bid_depth,omitempty"`
	YesAskDepth   float64 `json:"yes_ask_depth,omitempty"`
	NoBidDepth    float64 `json:"no_bid_depth,omitempty"`
	NoAskDepth    float64 `json:"no_ask_depth,omitempty"`
	QuoteAgeS     float64 `json:"quote_age_s,omitempty"`
	YesMakerTick  float64 `json:"yes_maker_tick,omitempty"`
	YesTakerTick  float64 `json:"yes_taker_tick,omitempty"`
	NoMakerTick   float64 `json:"no_maker_tick,omitempty"`
	NoTakerTick   float64 `json:"no_taker_tick,omitempty"`
	YesMakerFeePC float64 `json:"yes_maker_fee_pc,omitempty"`
	YesTakerFeePC float64 `json:"yes_taker_fee_pc,omitempty"`
	NoMakerFeePC  float64 `json:"no_maker_fee_pc,omitempty"`
	NoTakerFeePC  float64 `json:"no_taker_fee_pc,omitempty"`
	BookSource    string  `json:"book_source,omitempty"`
}

type wxCurveQuantile struct {
	Percentile int     `json:"percentile"`
	ValueF     float64 `json:"value_f"`
	RawValueF  float64 `json:"raw_value_f"`
}

type wxCurveChoice struct {
	Ticker     string  `json:"ticker"`
	Side       string  `json:"side"`
	CurveMass  float64 `json:"curve_mass"`
	Ask        float64 `json:"ask"`
	FeePC      float64 `json:"fee_pc"`
	Depth      float64 `json:"depth"`
	ResidualPC float64 `json:"curve_book_residual_pc"`
}

type wxBookDelta struct {
	Ticker      string  `json:"ticker"`
	YesAskDelta float64 `json:"yes_ask_delta,omitempty"`
	NoAskDelta  float64 `json:"no_ask_delta,omitempty"`
	YesBidDelta float64 `json:"yes_bid_delta,omitempty"`
	NoBidDelta  float64 `json:"no_bid_delta,omitempty"`
}

type wxResearchRecord struct {
	Kind               string            `json:"kind"`
	Model              string            `json:"model"`
	Signal             string            `json:"signal"`
	EventKey           string            `json:"event_key"`
	Series             string            `json:"series"`
	EventTicker        string            `json:"event_ticker"`
	LocalDate          string            `json:"local_date"`
	SettlementStation  string            `json:"settlement_station"`
	SettlementSource   string            `json:"settlement_source"`
	ForecastSource     string            `json:"curve_source"`
	SourceRevisionAt   string            `json:"curve_revision_at,omitempty"`
	ObservedAt         string            `json:"observed_at"`
	RevisionID         string            `json:"revision_id,omitempty"`
	RevisionHash       string            `json:"revision_hash,omitempty"`
	HorizonS           *int64            `json:"horizon_s,omitempty"`
	ActualElapsedS     float64           `json:"actual_elapsed_s,omitempty"`
	InputStatus        string            `json:"input_status"`
	InputReason        string            `json:"input_reason,omitempty"`
	ForecastMedianF    float64           `json:"curve_median_f,omitempty"`
	PriorMedianF       *float64          `json:"prior_curve_median_f,omitempty"`
	TargetTicker       string            `json:"target_ticker,omitempty"`
	PriorTargetTicker  string            `json:"prior_target_ticker,omitempty"`
	Quantiles          []wxCurveQuantile `json:"curve_quantiles,omitempty"`
	Books              []wxResearchBook  `json:"books,omitempty"`
	Deltas             []wxBookDelta     `json:"book_deltas,omitempty"`
	Best               *wxCurveChoice    `json:"best_residual,omitempty"`
	SignalLogged       bool              `json:"signal_logged"`
	Funded             bool              `json:"funded"`
	Execution          string            `json:"execution"`
	SettlementPath     string            `json:"settlement_path"`
	SourceIndependence string            `json:"source_independence"`
	PromotionBlocked   bool              `json:"promotion_blocked"`
}

type wxRevisionPending struct {
	ID       string
	EventKey string
	Series   string
	Event    string
	Date     string
	Station  string
	SourceAt time.Time
	Started  time.Time
	Baseline []wxResearchBook
	Seen     map[int64]bool
}

type wxResearchEventState struct {
	Hash                 string
	Median               float64
	Target               string
	SourceAt             time.Time
	CurveSignalIssued    bool
	RevisionSignalIssued bool
	Pending              []*wxRevisionPending
}

type wxResearchRuntime struct {
	once          sync.Once
	mu            sync.Mutex
	collectorMu   sync.Mutex
	events        map[string]*wxResearchEventState
	unavailable   map[string]time.Time
	collector     *wxResearchCollectorCycle
	lastCollector wxResearchCollectorCycle
}

type wxResearchCollectorCycle struct {
	CycleID, LastErrorClass, LastErrorText      string
	Started                                     time.Time
	Stations, Series, Events                    int
	Requests, Successes, Failures, Frames       int
	ReadyRecords, Records, Signals              int
	NBMForecasts, NBMDateMatches, NBMCandidates int
	Exclusions                                  map[string]int
}

var wxResearchRuntimes sync.Map // *Server -> *wxResearchRuntime

func (s *Server) wxResearchRuntime() *wxResearchRuntime {
	v, _ := wxResearchRuntimes.LoadOrStore(s, &wxResearchRuntime{})
	rt := v.(*wxResearchRuntime)
	rt.once.Do(func() {
		rt.events = map[string]*wxResearchEventState{}
		rt.unavailable = map[string]time.Time{}
		if s == nil || s.store == nil {
			return
		}
		rows, err := s.store.WeatherResearchAudit(context.Background(), 5000)
		if err != nil {
			return
		}
		// Oldest to newest: later response rows can mark an earlier revision's horizon complete.
		for i := len(rows) - 1; i >= 0; i-- {
			var rec wxResearchRecord
			if json.Unmarshal([]byte(rows[i].Detail), &rec) != nil || rec.EventKey == "" {
				continue
			}
			ev := rt.events[rec.EventKey]
			if ev == nil {
				ev = &wxResearchEventState{}
				rt.events[rec.EventKey] = ev
			}
			switch rec.Kind {
			case "curve_baseline", "curve_revision":
				ev.Hash, ev.Median, ev.Target = rec.RevisionHash, rec.ForecastMedianF, rec.TargetTicker
				ev.SourceAt, _ = time.Parse(time.RFC3339Nano, rec.SourceRevisionAt)
				if rec.SignalLogged {
					ev.RevisionSignalIssued = true
				}
				if rec.InputStatus == "ready" && rec.RevisionID != "" && len(rec.Books) > 0 {
					p := &wxRevisionPending{ID: rec.RevisionID, EventKey: rec.EventKey, Series: rec.Series,
						Event: rec.EventTicker, Date: rec.LocalDate, Station: rec.SettlementStation,
						SourceAt: ev.SourceAt, Started: ev.SourceAt, Baseline: rec.Books, Seen: map[int64]bool{}}
					ev.Pending = append(ev.Pending, p)
				}
			case "curve_response", "curve_horizon_missed":
				for _, p := range ev.Pending {
					if p.ID == rec.RevisionID && rec.HorizonS != nil {
						p.Seen[*rec.HorizonS] = true
					}
				}
			case "curve_residual_observation":
				if rec.SignalLogged {
					ev.CurveSignalIssued = true
				}
			}
		}
	})
	return rt
}

func wxResearchNow() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (s *Server) wxResearchCollectorBegin(started time.Time) {
	rt := s.wxResearchRuntime()
	rt.collectorMu.Lock()
	rt.collector = &wxResearchCollectorCycle{
		CycleID: started.UTC().Truncate(30 * time.Minute).Format(time.RFC3339), Started: started,
		Exclusions: map[string]int{},
	}
	rt.collectorMu.Unlock()
}

func (s *Server) wxResearchCollectorUpdate(fn func(*wxResearchCollectorCycle)) {
	rt := s.wxResearchRuntime()
	rt.collectorMu.Lock()
	if rt.collector != nil {
		fn(rt.collector)
	}
	rt.collectorMu.Unlock()
}

func wxSafeForecastError(err error) (class, safe string) {
	if err == nil {
		return "", ""
	}
	msg := strings.ToLower(err.Error())
	if i := strings.Index(msg, "status "); i >= 0 && len(msg) >= i+10 {
		code := msg[i+7 : i+10]
		if _, convErr := strconv.Atoi(code); convErr == nil {
			return "http_" + code, "forecast-history venue response HTTP " + code
		}
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded) || strings.Contains(msg, "deadline exceeded"):
		return "timeout", "forecast-history request timed out"
	case errors.Is(err, context.Canceled) || strings.Contains(msg, "context canceled"):
		return "canceled", "forecast-history request canceled"
	case strings.Contains(msg, "no fresh complete monotone"):
		return "no_usable_frame", "forecast-history returned no fresh complete monotone frame"
	default:
		return "transport_or_decode", "forecast-history transport or response decode failed"
	}
}

func (s *Server) wxResearchCollectorFinish(ctx context.Context) {
	rt := s.wxResearchRuntime()
	rt.collectorMu.Lock()
	c := rt.collector
	if c != nil {
		rt.lastCollector = *c
		rt.lastCollector.Exclusions = cloneCountMap(c.Exclusions)
	}
	rt.collector = nil
	rt.collectorMu.Unlock()
	if c == nil || s == nil || s.store == nil {
		return
	}
	completed := time.Now()
	status, expectedZero, zeroReason := "healthy", false, ""
	errorClass, errorText := "", ""
	switch {
	case ctx != nil && ctx.Err() != nil:
		status, errorClass, errorText = "blocked", "cycle_context", "weather research cycle ended before completion"
	case c.Stations == 0:
		status, errorClass, errorText = "blocked", "empty_station_registry", "no source-verified weather stations were available"
	case c.Failures > 0:
		status, errorClass, errorText = "blocked", c.LastErrorClass, c.LastErrorText
	case c.Records > 0:
		// Independent NBM may produce a complete research record without calling the separate,
		// trade-derived Forecast Graph endpoint. A real inserted row is never an expected-zero cycle.
	case c.Requests == 0:
		status, expectedZero, zeroReason = "healthy_empty", true, "no eligible source-verified station/date event reached forecast history"
	case c.Records == 0:
		status, expectedZero, zeroReason = "healthy_empty", true, "valid forecast responses produced no research observation"
	}
	metrics := map[string]any{
		"stations": c.Stations, "series_scanned": c.Series, "eligible_events": c.Events,
		"forecast_requests": c.Requests, "forecast_successes": c.Successes,
		"forecast_failures": c.Failures, "frames_returned": c.Frames,
		"ready_records": c.ReadyRecords, "research_records": c.Records,
		"signals_logged": c.Signals, "period_interval_minutes": 1,
		"nbm_station_forecasts": c.NBMForecasts, "nbm_station_date_matches": c.NBMDateMatches,
		"nbm_lower_bound_candidates": c.NBMCandidates,
		"time_boundaries":            "UTC minute-aligned", "percentiles_encoding": "repeated query keys",
		"source_independence": wxForecastIndependence,
	}
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	wctx, cancel := context.WithTimeout(base, 2*time.Second)
	defer cancel()
	_, _ = s.store.InsertCollectorReceipt(wctx, storage.CollectorReceipt{
		CollectorID: "weather-research", CycleID: c.CycleID,
		ExperimentID: "deadline-hazard-surface", ExperimentVersion: 1,
		Status: status, Started: c.Started, Completed: completed,
		Eligible: c.Events, Attempted: c.Requests + c.NBMDateMatches, Inserted: c.Records,
		ExpectedZero: expectedZero, ZeroReason: zeroReason,
		ErrorClass: errorClass, ErrorText: errorText,
		Source:        "Kalshi trade-derived forecast diagnostics plus official NOAA NBM station-quantile probability envelopes; no money authority",
		SchemaVersion: "weather-research-r139-v2", ExpectedCadence: 30 * time.Minute,
		Exclusions: c.Exclusions, Metrics: metrics,
		Systems: []string{"deadline-hazard-surface", "independent-probabilistic-weather"},
	})
}

func (s *Server) wxResearchCollectorView() map[string]any {
	rt := s.wxResearchRuntime()
	rt.collectorMu.Lock()
	c := rt.lastCollector
	active := rt.collector != nil
	if active {
		c = *rt.collector
	}
	exclusions := cloneCountMap(c.Exclusions)
	rt.collectorMu.Unlock()
	return map[string]any{
		"cycle_id": c.CycleID, "active": active, "started_at": c.Started.UTC().Format(time.RFC3339Nano),
		"stations": c.Stations, "series_scanned": c.Series, "eligible_events": c.Events,
		"requests": c.Requests, "successes": c.Successes, "failures": c.Failures,
		"frames": c.Frames, "ready_records": c.ReadyRecords, "records": c.Records,
		"signals_logged": c.Signals, "last_error_class": c.LastErrorClass,
		"nbm_station_forecasts": c.NBMForecasts, "nbm_station_date_matches": c.NBMDateMatches,
		"nbm_lower_bound_candidates": c.NBMCandidates,
		"last_error":                 c.LastErrorText, "exclusions": exclusions,
		"wire_contract": map[string]any{"period_interval_minutes": 1,
			"boundaries": "UTC minute-aligned", "percentiles": "repeated query keys; 0..9999"},
	}
}

func (s *Server) wxResearchAudit(ctx context.Context, category, message string, rec wxResearchRecord) bool {
	if rec.ObservedAt == "" {
		rec.ObservedAt = wxResearchNow()
	}
	rec.Funded = false
	rec.SourceIndependence = wxForecastIndependence
	rec.PromotionBlocked = true
	rec.Execution = "research-only; no order, paper fill, portfolio lot, maker attempt, unit trial, or live dispatch"
	rec.SettlementPath = "direct signal_log row; canonical Kalshi settlement worker; no Server.insertSignal hooks"
	b, err := json.Marshal(rec)
	if err != nil {
		return false
	}
	// The parent sweep owns a three-minute network budget. Preserve a completed observation if
	// that deadline expires between the final book read and this tiny local SQLite append.
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	wctx, cancel := context.WithTimeout(base, 2*time.Second)
	defer cancel()
	ok := s.store.Audit(wctx, "info", category, message, string(b)) == nil
	if ok && (category == wxCurveResidualFamily || category == wxCurveRevisionFamily) {
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
			c.Records++
			if rec.InputStatus == "ready" {
				c.ReadyRecords++
			}
			if rec.SignalLogged {
				c.Signals++
			}
			if rec.InputStatus == "unavailable" {
				c.Exclusions["input_unavailable"]++
			}
		})
	}
	return ok
}

func (s *Server) wxResearchUnavailable(ctx context.Context, category, key string, rec wxResearchRecord) {
	rt := s.wxResearchRuntime()
	now := time.Now()
	rt.mu.Lock()
	last := rt.unavailable[category+"|"+key]
	if now.Sub(last) < wxResearchUnavailEvery {
		rt.mu.Unlock()
		return
	}
	rt.mu.Unlock()
	rec.Kind, rec.InputStatus = "input_unavailable", "unavailable"
	if s.wxResearchAudit(ctx, category, "prospective input unavailable", rec) {
		rt.mu.Lock()
		rt.unavailable[category+"|"+key] = now
		rt.mu.Unlock()
	}
}

// wxLadderBooks validates that the event is one complete, mutually-exclusive integer-Fahrenheit
// ladder. Missing books are represented explicitly per rung; they are not reconstructed. At least
// one exact executable side is required before a signal can be admitted.
func (s *Server) wxLadderBooks(mkts []kalshi.Market, idxs []int) ([]wxResearchBook, map[string]kalshi.Market, string, error) {
	if len(idxs) < 2 {
		return nil, nil, "", fmt.Errorf("ladder has fewer than two bins")
	}
	type rung struct {
		m      kalshi.Market
		lo, hi float64
	}
	rungs := make([]rung, 0, len(idxs))
	event := ""
	seen := map[string]bool{}
	for _, i := range idxs {
		if i < 0 || i >= len(mkts) {
			return nil, nil, "", fmt.Errorf("ladder market index outside snapshot")
		}
		m := mkts[i]
		lo, hi, ok := wxParseBin(m.YesSubTitle)
		if !ok || hi < lo || m.Ticker == "" || m.EventTicker == "" {
			return nil, nil, "", fmt.Errorf("ambiguous bin or event identity for %s", m.Ticker)
		}
		if seen[m.Ticker] {
			return nil, nil, "", fmt.Errorf("duplicate ladder ticker %s", m.Ticker)
		}
		seen[m.Ticker] = true
		if event == "" {
			event = m.EventTicker
		} else if event != m.EventTicker {
			return nil, nil, "", fmt.Errorf("mixed event tickers in one city/date ladder")
		}
		rungs = append(rungs, rung{m: m, lo: lo, hi: hi})
	}
	sort.Slice(rungs, func(i, j int) bool { return rungs[i].lo < rungs[j].lo })
	if rungs[0].lo > -900 || rungs[len(rungs)-1].hi < 900 {
		return nil, nil, "", fmt.Errorf("ladder is not exhaustive at both tails")
	}
	for i, r := range rungs {
		if (r.lo > -900 && math.Abs(r.lo-math.Round(r.lo)) > 1e-9) ||
			(r.hi < 900 && math.Abs(r.hi-math.Round(r.hi)) > 1e-9) {
			return nil, nil, "", fmt.Errorf("non-integer CLI bin boundary for %s", r.m.Ticker)
		}
		if i > 0 && math.Abs(r.lo-rungs[i-1].hi-1) > 1e-9 {
			return nil, nil, "", fmt.Errorf("ladder gap or overlap between %s and %s", rungs[i-1].m.Ticker, r.m.Ticker)
		}
	}
	books := make([]wxResearchBook, 0, len(rungs))
	marketByTicker := make(map[string]kalshi.Market, len(rungs))
	executable := 0
	for _, r := range rungs {
		marketByTicker[r.m.Ticker] = r.m
		b := wxResearchBook{Ticker: r.m.Ticker, Title: r.m.Title + " (" + r.m.YesSubTitle + ")", LoF: r.lo, HiF: r.hi}
		yes := storage.Signal{Platform: "kalshi", Ticker: r.m.Ticker, Side: "YES"}
		no := storage.Signal{Platform: "kalshi", Ticker: r.m.Ticker, Side: "NO"}
		s.stampMLBookSnapshot(&yes)
		s.stampMLBookSnapshot(&no)
		if yes.BookFeatureVer == mlBookFeatureVersion && yes.BookBid != nil && yes.BookAsk != nil &&
			yes.BookBidDepth != nil && yes.BookAskDepth != nil && yes.BookQuoteAgeS != nil &&
			yes.BookMakerTick != nil && yes.BookTakerTick != nil && yes.BookMakerFeePC != nil && yes.BookTakerFeePC != nil &&
			*yes.BookAskDepth >= 1 {
			b.YesExecutable = true
			b.YesBid, b.YesAsk = *yes.BookBid, *yes.BookAsk
			b.YesBidDepth, b.YesAskDepth = *yes.BookBidDepth, *yes.BookAskDepth
			b.QuoteAgeS = *yes.BookQuoteAgeS
			b.YesMakerTick, b.YesTakerTick = *yes.BookMakerTick, *yes.BookTakerTick
			b.YesMakerFeePC, b.YesTakerFeePC = *yes.BookMakerFeePC, *yes.BookTakerFeePC
			b.BookSource = yes.BookSource
			executable++
		}
		if no.BookFeatureVer == mlBookFeatureVersion && no.BookBid != nil && no.BookAsk != nil &&
			no.BookBidDepth != nil && no.BookAskDepth != nil && no.BookQuoteAgeS != nil &&
			no.BookMakerTick != nil && no.BookTakerTick != nil && no.BookMakerFeePC != nil && no.BookTakerFeePC != nil &&
			*no.BookAskDepth >= 1 {
			b.NoExecutable = true
			b.NoBid, b.NoAsk = *no.BookBid, *no.BookAsk
			b.NoBidDepth, b.NoAskDepth = *no.BookBidDepth, *no.BookAskDepth
			if !b.YesExecutable || *no.BookQuoteAgeS > b.QuoteAgeS {
				b.QuoteAgeS = *no.BookQuoteAgeS
			}
			b.NoMakerTick, b.NoTakerTick = *no.BookMakerTick, *no.BookTakerTick
			b.NoMakerFeePC, b.NoTakerFeePC = *no.BookMakerFeePC, *no.BookTakerFeePC
			if b.BookSource == "" {
				b.BookSource = no.BookSource
			}
			executable++
		}
		books = append(books, b)
	}
	if executable == 0 {
		return books, marketByTicker, event, errWxNoExecutableBook
	}
	return books, marketByTicker, event, nil
}

func wxFrameQuantiles(frame kalshi.ForecastPercentileFrame) ([]wxCurveQuantile, error) {
	if frame.EndPeriodTS <= 0 || len(frame.Points) != len(wxResearchPercentiles) {
		return nil, fmt.Errorf("trade-derived curve frame missing timestamp or required quantiles")
	}
	pts := append([]kalshi.ForecastPercentilePoint(nil), frame.Points...)
	sort.Slice(pts, func(i, j int) bool { return pts[i].Percentile < pts[j].Percentile })
	out := make([]wxCurveQuantile, 0, len(pts))
	for i, p := range pts {
		if p.Percentile != wxResearchPercentiles[i] || math.IsNaN(p.NumericalForecast) || math.IsInf(p.NumericalForecast, 0) {
			return nil, fmt.Errorf("trade-derived curve frame has missing/non-finite percentile %d", wxResearchPercentiles[i])
		}
		if i > 0 && p.NumericalForecast < pts[i-1].NumericalForecast {
			return nil, fmt.Errorf("trade-derived curve quantiles are not monotone")
		}
		out = append(out, wxCurveQuantile{Percentile: p.Percentile, ValueF: p.NumericalForecast, RawValueF: p.RawNumericalForecast})
	}
	return out, nil
}

// wxQuantileCDF uses piecewise-linear interpolation between trade-derived Forecast Graph
// quantiles. The outer
// 0th and 99.99th percentile points keep the two tail masses explicit; no normal distribution or
// synthetic variance is assumed.
func wxQuantileCDF(q []wxCurveQuantile, x float64) (float64, bool) {
	if len(q) < 2 || q[0].Percentile != 0 || q[len(q)-1].Percentile != 9999 {
		return 0, false
	}
	if x < q[0].ValueF {
		return 0, true
	}
	if x > q[len(q)-1].ValueF {
		return float64(q[len(q)-1].Percentile) / 10000, true
	}
	i := sort.Search(len(q), func(i int) bool { return q[i].ValueF >= x })
	if i < len(q) && q[i].ValueF == x {
		// Several percentiles can share one discrete forecast. The CDF at that atom is the
		// HIGHEST percentile on the value, not the first duplicate.
		for i+1 < len(q) && q[i+1].ValueF == x {
			i++
		}
		return float64(q[i].Percentile) / 10000, true
	}
	if i <= 0 || i >= len(q) {
		return 0, false
	}
	a, b := q[i-1], q[i]
	pa, pb := float64(a.Percentile)/10000, float64(b.Percentile)/10000
	f := (x - a.ValueF) / (b.ValueF - a.ValueF)
	return pa + f*(pb-pa), true
}

func wxCurveMassSimplex(books []wxResearchBook, q []wxCurveQuantile) ([]wxResearchBook, error) {
	if len(books) < 2 {
		return nil, fmt.Errorf("incomplete ladder")
	}
	out := append([]wxResearchBook(nil), books...)
	sum := 0.0
	for i := range out {
		loCDF, hiCDF := 0.0, 1.0
		var ok bool
		if out[i].LoF > -900 {
			loCDF, ok = wxQuantileCDF(q, out[i].LoF-0.5)
			if !ok {
				return nil, fmt.Errorf("invalid trade-derived curve CDF")
			}
		}
		if out[i].HiF < 900 {
			hiCDF, ok = wxQuantileCDF(q, out[i].HiF+0.5)
			if !ok {
				return nil, fmt.Errorf("invalid trade-derived curve CDF")
			}
		}
		p := hiCDF - loCDF
		if p < -1e-9 || p > 1+1e-9 {
			return nil, fmt.Errorf("invalid curve mass for %s", out[i].Ticker)
		}
		if p < 0 {
			p = 0
		}
		out[i].CurveMass = p
		sum += p
	}
	if math.Abs(sum-1) > 1e-6 {
		return nil, fmt.Errorf("trade-derived curve-mass simplex sums to %.9f, not one", sum)
	}
	return out, nil
}

func wxBestCurveResidual(books []wxResearchBook) (wxCurveChoice, error) {
	best := wxCurveChoice{ResidualPC: math.Inf(-1)}
	for _, b := range books {
		if b.YesExecutable {
			c := wxCurveChoice{Ticker: b.Ticker, Side: "YES", CurveMass: b.CurveMass,
				Ask: b.YesAsk, FeePC: b.YesTakerFeePC, Depth: b.YesAskDepth,
				ResidualPC: b.CurveMass - b.YesAsk - b.YesTakerFeePC}
			if c.ResidualPC > best.ResidualPC {
				best = c
			}
		}
		if b.NoExecutable {
			c := wxCurveChoice{Ticker: b.Ticker, Side: "NO", CurveMass: 1 - b.CurveMass,
				Ask: b.NoAsk, FeePC: b.NoTakerFeePC, Depth: b.NoAskDepth,
				ResidualPC: 1 - b.CurveMass - b.NoAsk - b.NoTakerFeePC}
			if c.ResidualPC > best.ResidualPC {
				best = c
			}
		}
	}
	if math.IsInf(best.ResidualPC, -1) {
		return wxCurveChoice{}, errWxNoExecutableBook
	}
	return best, nil
}

func wxQuantileHash(q []wxCurveQuantile) string {
	h := sha256.New()
	for _, p := range q {
		_, _ = fmt.Fprintf(h, "%d=%.10f;", p.Percentile, p.ValueF)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func wxTargetTicker(books []wxResearchBook, median float64) string {
	v := math.Round(median)
	for _, b := range books {
		if v >= b.LoF && v <= b.HiF {
			return b.Ticker
		}
	}
	return ""
}

func wxCopyBookSignalFields(sig *storage.Signal, b wxResearchBook, side string) bool {
	if sig == nil {
		return false
	}
	var bid, ask, bidDepth, askDepth, makerTick, takerTick, makerFee, takerFee float64
	if side == "YES" && b.YesExecutable {
		bid, ask, bidDepth, askDepth = b.YesBid, b.YesAsk, b.YesBidDepth, b.YesAskDepth
		makerTick, takerTick, makerFee, takerFee = b.YesMakerTick, b.YesTakerTick, b.YesMakerFeePC, b.YesTakerFeePC
	} else if side == "NO" && b.NoExecutable {
		bid, ask, bidDepth, askDepth = b.NoBid, b.NoAsk, b.NoBidDepth, b.NoAskDepth
		makerTick, takerTick, makerFee, takerFee = b.NoMakerTick, b.NoTakerTick, b.NoMakerFeePC, b.NoTakerFeePC
	} else {
		return false
	}
	if askDepth < 1 || math.IsNaN(askDepth) || math.IsInf(askDepth, 0) {
		return false // a fractional or invalid top level cannot fill the promised one-share trial
	}
	sig.EntryPrice, sig.FeePC = ask, &takerFee
	sig.BookFeatureVer = mlBookFeatureVersion
	sig.BookBid, sig.BookAsk = &bid, &ask
	sig.BookBidDepth, sig.BookAskDepth = &bidDepth, &askDepth
	age := b.QuoteAgeS
	sig.BookQuoteAgeS = &age
	sig.BookMakerTick, sig.BookTakerTick = &makerTick, &takerTick
	sig.BookMakerFeePC, sig.BookTakerFeePC = &makerFee, &takerFee
	sig.BookSource = b.BookSource
	return true
}

func wxFindResearchBook(books []wxResearchBook, ticker string) (wxResearchBook, bool) {
	for _, b := range books {
		if b.Ticker == ticker {
			return b, true
		}
	}
	return wxResearchBook{}, false
}

func wxResolveHoursFromMarket(m kalshi.Market) float64 {
	raw := m.ExpectedExpiration
	if raw == "" {
		raw = m.CloseTime
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return math.Max(0, time.Until(t).Hours())
	}
	return 0
}

func (s *Server) wxInsertResearchSignal(ctx context.Context, model, side string, b wxResearchBook, m kalshi.Market, curveMass, strength, momentum float64) (bool, error) {
	sig := storage.Signal{Platform: "kalshi", Ticker: b.Ticker, Title: b.Title, Side: side,
		SignalType: model, Underlying: curveMass, Strength: strength, Momentum: momentum,
		ResolveHours: wxResolveHoursFromMarket(m), Category: "Weather", MarketType: "temperature-ladder", Kind: "prop",
		IsLive: -1, ExecExpr: "research-only:taker"} // event phase is not exposed by this curve schema
	if !wxCopyBookSignalFields(&sig, b, side) {
		return false, errWxNoExecutableBook
	}
	// CRITICAL: bypass Server.insertSignal. This is the durable settlement row and nothing else.
	return s.store.InsertSignalResult(ctx, sig)
}

func wxBookDeltas(base, cur []wxResearchBook) []wxBookDelta {
	by := make(map[string]wxResearchBook, len(base))
	for _, b := range base {
		by[b.Ticker] = b
	}
	var out []wxBookDelta
	for _, c := range cur {
		b, ok := by[c.Ticker]
		if !ok {
			continue
		}
		d := wxBookDelta{Ticker: c.Ticker}
		if b.YesExecutable && c.YesExecutable {
			d.YesAskDelta, d.YesBidDelta = c.YesAsk-b.YesAsk, c.YesBid-b.YesBid
		}
		if b.NoExecutable && c.NoExecutable {
			d.NoAskDelta, d.NoBidDelta = c.NoAsk-b.NoAsk, c.NoBid-b.NoBid
		}
		out = append(out, d)
	}
	return out
}

func wxHorizonTolerance(horizonS int64) float64 {
	if horizonS == 0 {
		return 5 * 60 // an "immediate" snapshot more than five minutes late is not h0
	}
	return 10 * 60 // bounded scheduler/feed jitter for the 30m/60m/120m points
}

// wxHorizonDisposition prevents a late first sighting from being backfilled into an earlier
// fixed-horizon bucket. "capture" is only at/after the target and inside its explicit window.
func wxHorizonDisposition(elapsedS float64, horizonS int64) string {
	if elapsedS < float64(horizonS) {
		return "wait"
	}
	if elapsedS <= float64(horizonS)+wxHorizonTolerance(horizonS) {
		return "capture"
	}
	return "missed"
}

func (s *Server) wxLatestCurveFrame(ctx context.Context, series, event string) (kalshi.ForecastPercentileFrame, []wxCurveQuantile, error) {
	if s.kal == nil || !s.kal.HasCredentials() {
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
			c.Failures++
			c.Exclusions["credentials_unavailable"]++
			c.LastErrorClass, c.LastErrorText = "credentials_unavailable", "authenticated Kalshi forecast API unavailable"
		})
		return kalshi.ForecastPercentileFrame{}, nil, fmt.Errorf("authenticated Kalshi forecast API unavailable")
	}
	now := time.Now()
	s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Requests++ })
	history, err := s.kal.ForecastPercentileHistory(ctx, series, event, wxResearchPercentiles,
		now.Add(-4*time.Hour).Unix(), now.Unix(), 1)
	if err != nil {
		class, safe := wxSafeForecastError(err)
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
			c.Failures++
			c.Exclusions[class]++
			c.LastErrorClass, c.LastErrorText = class, safe
		})
		return kalshi.ForecastPercentileFrame{}, nil, errors.New(safe)
	}
	s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Frames += len(history) })
	for i := len(history) - 1; i >= 0; i-- {
		f := history[i]
		if f.EventTicker != "" && !strings.EqualFold(f.EventTicker, event) {
			continue
		}
		at := time.Unix(f.EndPeriodTS, 0)
		if at.After(now.Add(5*time.Minute)) || now.Sub(at) > wxResearchMaxFrameAge {
			continue
		}
		q, qerr := wxFrameQuantiles(f)
		if qerr == nil {
			// The frame timestamp is a period boundary, not an explicit "changed_at" field. Walk
			// backward over the contiguous identical vector so SourceRevisionAt is the earliest
			// observable minute of this revision, not merely the time of our latest fetch.
			hash := wxQuantileHash(q)
			for j := i - 1; j >= 0; j-- {
				prior := history[j]
				pq, perr := wxFrameQuantiles(prior)
				if perr != nil || wxQuantileHash(pq) != hash {
					break
				}
				pat := time.Unix(prior.EndPeriodTS, 0)
				if now.Sub(pat) > wxResearchMaxFrameAge {
					break
				}
				f.EndPeriodTS = prior.EndPeriodTS
			}
			s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Successes++ })
			return f, q, nil
		}
	}
	err = fmt.Errorf("no fresh complete monotone forecast percentile frame")
	class, safe := wxSafeForecastError(err)
	s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
		c.Failures++
		c.Exclusions[class]++
		c.LastErrorClass, c.LastErrorText = class, safe
	})
	return kalshi.ForecastPercentileFrame{}, nil, errors.New(safe)
}

func (s *Server) wxCollectRevisionResponses(ctx context.Context, ev *wxResearchEventState, books []wxResearchBook) {
	now := time.Now()
	for _, p := range ev.Pending {
		elapsed := now.Sub(p.SourceAt).Seconds()
		if elapsed < 0 {
			continue
		}
	horizons:
		for _, horizon := range wxRevisionHorizons {
			if p.Seen[horizon] {
				continue
			}
			switch wxHorizonDisposition(elapsed, horizon) {
			case "wait":
				break horizons // horizons are sorted; every later point is also not due
			case "missed":
				h := horizon
				if s.wxResearchAudit(ctx, wxCurveRevisionFamily, "fixed horizon missed", wxResearchRecord{
					Kind: "curve_horizon_missed", Model: wxCurveRevisionModel, Signal: "trade-derived-curve-revision",
					EventKey: p.EventKey, Series: p.Series, EventTicker: p.Event, LocalDate: p.Date,
					SettlementStation: p.Station, SettlementSource: "NWS CLI source recorded in Kalshi series metadata",
					ForecastSource: wxResearchSource, SourceRevisionAt: p.SourceAt.UTC().Format(time.RFC3339Nano),
					RevisionID: p.ID, HorizonS: &h, ActualElapsedS: elapsed, InputStatus: "missed",
					InputReason: fmt.Sprintf("first available post-revision snapshot exceeded the +%.0fs horizon window", wxHorizonTolerance(horizon)),
				}) {
					p.Seen[horizon] = true
				}
				continue
			}
			h := horizon
			if s.wxResearchAudit(ctx, wxCurveRevisionFamily, "fixed-horizon book response", wxResearchRecord{
				Kind: "curve_response", Model: wxCurveRevisionModel, Signal: "trade-derived-curve-revision",
				EventKey: p.EventKey, Series: p.Series, EventTicker: p.Event, LocalDate: p.Date,
				SettlementStation: p.Station, SettlementSource: "NWS CLI source recorded in Kalshi series metadata",
				ForecastSource: wxResearchSource, SourceRevisionAt: p.SourceAt.UTC().Format(time.RFC3339Nano),
				RevisionID: p.ID, HorizonS: &h, ActualElapsedS: elapsed, InputStatus: "ready",
				Books: books, Deltas: wxBookDeltas(p.Baseline, books),
			}) {
				p.Seen[horizon] = true
				break // one real snapshot cannot honestly stand in for several missed horizons
			}
		}
	}
	// Bound memory and rehydrated state. The audit rows remain durable for 90 days.
	kept := ev.Pending[:0]
	for _, p := range ev.Pending {
		all := true
		for _, h := range wxRevisionHorizons {
			all = all && p.Seen[h]
		}
		if !all && now.Sub(p.SourceAt) < 24*time.Hour {
			kept = append(kept, p)
		}
	}
	ev.Pending = kept
}

// wxResearchObserveSeries is called only from wxScanSeries's 30-minute, source-verified sweep.
// At most two forecast-horizon events per station are allowed to consume authenticated REST reads.
func (s *Server) wxResearchObserveSeries(ctx context.Context, st *wxStation, mkts []kalshi.Market, byDate map[string][]int, periods []wxPeriod) {
	if st == nil || st.Series == "" || st.Issuedby == "" || len(periods) == 0 {
		return
	}
	dates := make([]string, 0, len(byDate))
	for date := range byDate {
		if _, ok := wxHighFor(periods, date); ok { // settlement-station local date is unambiguous
			dates = append(dates, date)
		}
	}
	sort.Strings(dates)
	if len(dates) > wxResearchMaxEvents {
		dates = dates[:wxResearchMaxEvents]
	}
	for _, date := range dates {
		idxs := byDate[date]
		deterministicHigh, deterministicOK := wxHighFor(periods, date)
		if !deterministicOK {
			continue
		}
		books, marketByTicker, event, bookErr := s.wxLadderBooks(mkts, idxs)
		eventKey := st.Series + "|" + date
		baseRec := wxResearchRecord{EventKey: eventKey, Series: st.Series, EventTicker: event, LocalDate: date,
			SettlementStation: st.StationID, SettlementSource: "NWS CLI source recorded in Kalshi series metadata",
			ForecastSource: wxResearchSource}
		if event == "" {
			baseRec.Model, baseRec.Signal, baseRec.InputReason = wxCurveResidualModel, "trade-derived curve/book residual", "ambiguous event identity"
			s.wxResearchUnavailable(ctx, wxCurveResidualFamily, eventKey, baseRec)
			continue
		}
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Events++ })
		frame, quantiles, err := s.wxLatestCurveFrame(ctx, st.Series, event)
		if err != nil {
			baseRec.Model, baseRec.Signal, baseRec.InputReason = wxCurveResidualModel, "trade-derived curve/book residual", err.Error()
			s.wxResearchUnavailable(ctx, wxCurveResidualFamily, eventKey, baseRec)
			baseRec.Model, baseRec.Signal = wxCurveRevisionModel, "trade-derived-curve-revision"
			s.wxResearchUnavailable(ctx, wxCurveRevisionFamily, eventKey, baseRec)
			continue
		}
		sourceAt := time.Unix(frame.EndPeriodTS, 0).UTC()
		baseRec.SourceRevisionAt = sourceAt.Format(time.RFC3339Nano)
		if bookErr != nil && !errors.Is(bookErr, errWxNoExecutableBook) {
			baseRec.Model, baseRec.Signal, baseRec.InputReason = wxCurveResidualModel, "trade-derived curve/book residual", bookErr.Error()
			s.wxResearchUnavailable(ctx, wxCurveResidualFamily, eventKey, baseRec)
			continue
		}
		curveBooks, err := wxCurveMassSimplex(books, quantiles)
		if err != nil {
			baseRec.Model, baseRec.Signal, baseRec.InputReason = wxCurveResidualModel, "trade-derived curve/book residual", err.Error()
			s.wxResearchUnavailable(ctx, wxCurveResidualFamily, eventKey, baseRec)
			continue
		}
		best, bestErr := wxBestCurveResidual(curveBooks)
		if bestErr != nil {
			baseRec.Model, baseRec.Signal, baseRec.InputReason = wxCurveResidualModel, "trade-derived curve/book residual", bestErr.Error()
			s.wxResearchUnavailable(ctx, wxCurveResidualFamily, eventKey, baseRec)
		}
		hash := wxQuantileHash(quantiles)
		median := quantiles[4].ValueF // requested p5000 is index 4
		// The endpoint is generic across forecast events and its two numerical fields are only
		// weakly described. Bind units to this exact NWS-CLI station/date: an implausible Fahrenheit
		// value or a p50 far from the already parsed station forecast is schema ambiguity, not edge.
		if median < -100 || median > 160 || math.Abs(median-deterministicHigh) > 15 {
			baseRec.Model, baseRec.Signal = wxCurveResidualModel, "trade-derived curve/book residual"
			baseRec.InputReason = fmt.Sprintf("curve units/event mapping ambiguous: curve p50 %.2f versus settlement-station high %.2fF", median, deterministicHigh)
			s.wxResearchUnavailable(ctx, wxCurveResidualFamily, eventKey, baseRec)
			baseRec.Model, baseRec.Signal = wxCurveRevisionModel, "trade-derived-curve-revision"
			s.wxResearchUnavailable(ctx, wxCurveRevisionFamily, eventKey, baseRec)
			continue
		}
		target := wxTargetTicker(curveBooks, median)
		if target == "" {
			baseRec.Model, baseRec.Signal, baseRec.InputReason = wxCurveRevisionModel, "trade-derived-curve-revision", "curve median is not covered by one validated bin"
			s.wxResearchUnavailable(ctx, wxCurveRevisionFamily, eventKey, baseRec)
			continue
		}

		rt := s.wxResearchRuntime()
		rt.mu.Lock()
		ev := rt.events[eventKey]
		if ev == nil {
			ev = &wxResearchEventState{}
			rt.events[eventKey] = ev
		}
		priorHash, priorMedian, priorTarget := ev.Hash, ev.Median, ev.Target
		isBaseline := priorHash == ""
		isRevision := priorHash != "" && priorHash != hash
		if isBaseline || isRevision {
			ev.Hash, ev.Median, ev.Target, ev.SourceAt = hash, median, target, sourceAt
		}
		curveMaySignal := !ev.CurveSignalIssued && bestErr == nil && best.ResidualPC > 0
		revisionMaySignal := !ev.RevisionSignalIssued && priorHash != "" && priorTarget != "" && priorTarget != target
		var pending *wxRevisionPending
		if isRevision && bestErr == nil {
			id := eventKey + "|" + strconv.FormatInt(frame.EndPeriodTS, 10) + "|" + hash[:12]
			pending = &wxRevisionPending{ID: id, EventKey: eventKey, Series: st.Series, Event: event,
				Date: date, Station: st.StationID, SourceAt: sourceAt, Started: time.Now(), Baseline: curveBooks, Seen: map[int64]bool{}}
			ev.Pending = append(ev.Pending, pending)
			if len(ev.Pending) > 8 {
				ev.Pending = ev.Pending[len(ev.Pending)-8:]
			}
		}
		rt.mu.Unlock()

		curveLogged := false
		if curveMaySignal {
			if b, ok := wxFindResearchBook(curveBooks, best.Ticker); ok {
				if m, okM := marketByTicker[best.Ticker]; okM {
					curveLogged, _ = s.wxInsertResearchSignal(ctx, wxCurveResidualFamily, best.Side, b, m,
						best.CurveMass, best.ResidualPC, median)
				}
			}
			if curveLogged {
				rt.mu.Lock()
				ev.CurveSignalIssued = true
				rt.mu.Unlock()
			}
		}
		if bestErr == nil {
			s.wxResearchAudit(ctx, wxCurveResidualFamily, "trade-derived curve/book residual observation", wxResearchRecord{
				Kind: "curve_residual_observation", Model: wxCurveResidualModel, Signal: "trade-derived curve/book residual",
				EventKey: eventKey, Series: st.Series, EventTicker: event, LocalDate: date,
				SettlementStation: st.StationID, SettlementSource: "NWS CLI source recorded in Kalshi series metadata",
				ForecastSource: wxResearchSource, SourceRevisionAt: sourceAt.Format(time.RFC3339Nano),
				InputStatus: "ready", ForecastMedianF: median, TargetTicker: target,
				Quantiles: quantiles, Books: curveBooks, Best: &best, SignalLogged: curveLogged,
			})
		}

		revisionLogged := false
		if isRevision && revisionMaySignal {
			if b, ok := wxFindResearchBook(curveBooks, target); ok && b.YesExecutable {
				if m, okM := marketByTicker[target]; okM {
					revisionLogged, _ = s.wxInsertResearchSignal(ctx, wxCurveRevisionFamily, "YES", b, m,
						b.CurveMass, math.Abs(median-priorMedian), median-priorMedian)
				}
			}
			if revisionLogged {
				rt.mu.Lock()
				ev.RevisionSignalIssued = true
				rt.mu.Unlock()
			}
		}
		if isBaseline || isRevision {
			var prior *float64
			if priorHash != "" {
				v := priorMedian
				prior = &v
			}
			revisionID := ""
			if pending != nil {
				revisionID = pending.ID
			}
			kind, message := "curve_revision", "trade-derived curve revision"
			if isBaseline {
				kind, message = "curve_baseline", "trade-derived curve baseline"
			}
			s.wxResearchAudit(ctx, wxCurveRevisionFamily, message, wxResearchRecord{
				Kind: kind, Model: wxCurveRevisionModel, Signal: "trade-derived-curve-revision",
				EventKey: eventKey, Series: st.Series, EventTicker: event, LocalDate: date,
				SettlementStation: st.StationID, SettlementSource: "NWS CLI source recorded in Kalshi series metadata",
				ForecastSource: wxResearchSource, SourceRevisionAt: sourceAt.Format(time.RFC3339Nano),
				RevisionID: revisionID, RevisionHash: hash, InputStatus: map[bool]string{true: "ready", false: "book_unavailable"}[bestErr == nil],
				ForecastMedianF: median, PriorMedianF: prior, TargetTicker: target, PriorTargetTicker: priorTarget,
				Quantiles: quantiles, Books: curveBooks, SignalLogged: revisionLogged,
			})
		}

		if bestErr == nil {
			rt.mu.Lock()
			s.wxCollectRevisionResponses(ctx, ev, curveBooks)
			rt.mu.Unlock()
		}
	}
}

// handleWeatherResearch is a read-only report over the durable audit cohort and canonical
// settlement rows. It exposes the honest input/unavailable states; it never triggers a fetch.
func (s *Server) weatherNBMAdapterStatus(ctx context.Context) map[string]any {
	forecasts, observed := s.weatherNBMCacheCounts()
	out := map[string]any{
		"name": "weather-nbm-independent", "status": "WAITING_FOR_FIRST_RECEIPT",
		"system": wxNBMSystem, "implementation": "PROSPECTIVE_PROBABILITY_ENVELOPE", "funded": false, "execution_authority": false,
		"required_source":          "NOAA National Blend of Models probabilistic station bulletin from NOMADS",
		"source_docs":              []string{"https://vlab.noaa.gov/web/mdl/nbm-text-products", "https://vlab.noaa.gov/web/mdl/nbm-textcard-v5.0", "https://vlab.noaa.gov/web/mdl/nbm-download"},
		"mapping_status":           "IMPLEMENTED_RESEARCH_ONLY",
		"current_blocker":          "no funding or promotion until prospective untouched settlement replication; sparse five-quantile envelopes may correctly abstain",
		"fallback":                 "none; never reinterpret the trade-derived Kalshi Forecast Graph as independent NBM input",
		"probability_method":       "sharp order bounds from p10/p25/p50/p75/p90; no normality or interpolation",
		"cached_station_forecasts": forecasts, "completed_event_run_observations": observed,
	}
	report, err := s.store.SourceClockReport(ctx)
	if err != nil {
		out["status"], out["receipt_error"] = "INPUT_UNAVAILABLE", err.Error()
		return out
	}
	rows, _ := report["sources"].([]storage.SourceClockLiveness)
	for _, row := range rows {
		if row.SourceID != "noaa-nbm" {
			continue
		}
		out["source_clock"] = row
		switch {
		case row.NeverRan:
			out["status"] = "WAITING_FOR_FIRST_RECEIPT"
		case row.Status == "healthy" && !row.Alert:
			if observed > 0 {
				out["status"] = "COLLECTING_PROSPECTIVE_COHORT"
			} else {
				out["status"] = "COLLECTING_INPUTS"
			}
		case row.Status == "healthy":
			out["status"] = "STALE_INPUT"
		default:
			out["status"] = "INPUT_UNAVAILABLE"
		}
		return out
	}
	out["status"] = "SOURCE_SPEC_UNAVAILABLE"
	return out
}

func (s *Server) handleWeatherResearch(w http.ResponseWriter, r *http.Request) {
	limit := 300
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 2000 {
		limit = n
	}
	rows, err := s.store.WeatherResearchAudit(r.Context(), limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	stats, err := s.store.WeatherResearchSignalStats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	records := make([]json.RawMessage, 0, len(rows))
	counts := map[string]int{}
	ready := map[string]int{}
	for _, row := range rows {
		if json.Valid([]byte(row.Detail)) {
			records = append(records, json.RawMessage(row.Detail))
			counts[row.Category]++
			var rec wxResearchRecord
			if json.Unmarshal([]byte(row.Detail), &rec) == nil && rec.InputStatus == "ready" {
				ready[row.Category]++
			}
		}
	}
	status := func(category, waiting string) string {
		if ready[category] > 0 {
			return "COLLECTING"
		}
		if counts[category] > 0 {
			return "INPUT_UNAVAILABLE"
		}
		return waiting
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"as_of":                     time.Now().UTC().Format(time.RFC3339Nano),
		"funded":                    false,
		"execution_authority":       false,
		"source_independence":       wxForecastIndependence,
		"curve_source_docs":         []string{"https://help.kalshi.com/en/articles/13823829-forecast-graph", "https://docs.kalshi.com/api-reference/events/get-event-forecast-percentile-history"},
		"promotion_blocked":         true,
		"forecast_history_liveness": s.wxResearchCollectorView(),
		"independent_adapter":       s.weatherNBMAdapterStatus(r.Context()),
		"systems": []map[string]any{
			{"name": wxNBMSystem, "model": wxNBMModel, "cohort": wxNBMCohort,
				"input":  "official NOAA NBM probabilistic station bulletin",
				"method": "exact settlement station + source-run + valid-UTC/local-date + exhaustive ladder; quantile-order probability interval versus current side ask/depth/tick/exact fee; candidate only when the lower bound is positive",
				"status": status(wxNBMCohort, "WAITING_FOR_EXACT_STATION_DATE_BOOK_JOIN")},
			{"name": wxCurveResidualFamily, "model": wxCurveResidualModel, "signal": "trade-derived curve/book residual",
				"input": wxResearchSource, "method": "trade-derived Forecast Graph percentiles -> endogenous curve-mass simplex -> contemporaneous exact YES/NO books and fees; residual/lag telemetry only, never fair value",
				"status": status(wxCurveResidualFamily, "WAITING_FOR_VALID_CURVE")},
			{"name": wxCurveRevisionFamily, "model": wxCurveRevisionModel, "signal": "trade-derived-curve-revision",
				"input": wxResearchSource, "horizons_s": wxRevisionHorizons,
				"status": status(wxCurveRevisionFamily, "WAITING_FOR_FIRST_CURVE_REVISION")},
		},
		"settlement": stats,
		"records":    records,
		"rules":      "research only. Independent NBM uses exact station/run/valid-time/local-date/bin identity and conservative quantile-order bounds with current side books and exact fees. Endogenous Forecast Graph systems remain separate lag diagnostics. No paper/live/portfolio dispatch or promotion.",
	})
}
