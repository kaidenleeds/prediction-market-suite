package server

// Prospective independent weather research.
//
// NOAA's probabilistic NBM station bulletin supplies five daily-min/max quantiles.  This file
// maps only the daily MAX values (the official bulletin places them at valid 00 UTC) to a Kalshi
// ladder when all of these identities are exact:
//
//   - the NBM station equals the NWS-CLI station named by the venue's settlement metadata;
//   - an NWS hourly period at the same valid instant supplies the station's exact local date;
//   - the ticker date equals that local date; and
//   - the event is one exhaustive, non-overlapping integer-Fahrenheit ladder.
//
// Five quantiles cannot identify a distribution.  Probability envelopes below use only quantile
// order constraints; they never interpolate, fit a normal distribution, or turn a point forecast
// into probability.  A candidate exists only when the conservative payout LOWER bound clears the
// current side-specific taker ask plus its exact one-share fee.  The resulting signal is written
// directly to the research settlement ledger and cannot invoke paper, maker, portfolio, or LIVE
// dispatch.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/nbm"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	wxNBMSystem             = "independent-probabilistic-weather"
	wxNBMCohort             = "weather-nbm-independent"
	wxNBMModel              = "noaa-nbm-qmd-quantile-envelope-v1"
	wxNBMMaxRunAge          = 12 * time.Hour
	wxNBMSourceIndependence = "INDEPENDENT: official NOAA NBM QMD probabilistic station guidance; venue NWS-CLI observation remains settlement truth"
)

var wxNBMPercentiles = []struct {
	Field string
	P     float64
}{
	{"TXNP1", .10}, {"TXNP2", .25}, {"TXNP5", .50}, {"TXNP7", .75}, {"TXNP9", .90},
}

type wxNBMQuantile struct {
	Field      string  `json:"field"`
	Percentile float64 `json:"percentile"`
	ValueF     float64 `json:"value_f"`
}

type wxNBMDailyMax struct {
	ValidUTC time.Time       `json:"valid_utc"`
	FHR      int             `json:"forecast_hour"`
	Values   []wxNBMQuantile `json:"quantiles"`
}

type wxNBMProbabilityBin struct {
	Ticker      string         `json:"ticker"`
	LoF         float64        `json:"lo_f"`
	HiF         float64        `json:"hi_f"`
	ProbLower   float64        `json:"probability_lower"`
	ProbUpper   float64        `json:"probability_upper"`
	NoProbLower float64        `json:"no_probability_lower"`
	NoProbUpper float64        `json:"no_probability_upper"`
	Book        wxResearchBook `json:"book"`
	YesNetLower *float64       `json:"yes_net_lower,omitempty"`
	NoNetLower  *float64       `json:"no_net_lower,omitempty"`
}

type wxNBMChoice struct {
	Ticker     string  `json:"ticker"`
	Side       string  `json:"side"`
	ProbLower  float64 `json:"probability_lower"`
	ProbUpper  float64 `json:"probability_upper"`
	Ask        float64 `json:"ask"`
	FeePC      float64 `json:"fee_pc"`
	Depth      float64 `json:"depth"`
	Tick       float64 `json:"tick"`
	QuoteAgeS  float64 `json:"quote_age_s"`
	NetLower   float64 `json:"net_lower"`
	NetUpper   float64 `json:"net_upper"`
	BookSource string  `json:"book_source"`
}

type wxNBMRecord struct {
	Kind                    string                `json:"kind"`
	System                  string                `json:"system"`
	Model                   string                `json:"model"`
	Cohort                  string                `json:"cohort"`
	EventKey                string                `json:"event_key"`
	Series                  string                `json:"series"`
	EventTicker             string                `json:"event_ticker"`
	LocalDate               string                `json:"local_date"`
	SettlementStation       string                `json:"settlement_station"`
	SettlementSource        string                `json:"settlement_source"`
	ModelRun                string                `json:"model_run"`
	ValidUTC                string                `json:"valid_utc"`
	ForecastHour            int                   `json:"forecast_hour"`
	SourceArtifact          string                `json:"source_artifact"`
	ObservedAt              string                `json:"observed_at"`
	InputStatus             string                `json:"input_status"`
	InputReason             string                `json:"input_reason,omitempty"`
	Quantiles               []wxNBMQuantile       `json:"quantiles"`
	Bins                    []wxNBMProbabilityBin `json:"bins,omitempty"`
	Best                    *wxNBMChoice          `json:"best_lower_bound,omitempty"`
	Candidate               bool                  `json:"candidate"`
	SignalLogged            bool                  `json:"signal_logged"`
	SystemObservationLogged bool                  `json:"system_observation_logged"`
	Funded                  bool                  `json:"funded"`
	PaperAuthority          bool                  `json:"paper_authority"`
	LiveAuthority           bool                  `json:"live_authority"`
	Execution               string                `json:"execution"`
	SettlementPath          string                `json:"settlement_path"`
	SourceIndependence      string                `json:"source_independence"`
	ProbabilityMethod       string                `json:"probability_method"`
	PromotionBlocked        bool                  `json:"promotion_blocked"`
}

type wxNBMRuntime struct {
	once      sync.Once
	mu        sync.Mutex
	forecasts map[string]nbm.StationForecast
	seen      map[string]bool
}

var wxNBMRuntimes sync.Map // *Server -> *wxNBMRuntime

func (s *Server) wxNBMRuntime() *wxNBMRuntime {
	v, _ := wxNBMRuntimes.LoadOrStore(s, &wxNBMRuntime{})
	rt := v.(*wxNBMRuntime)
	rt.once.Do(func() {
		rt.forecasts = map[string]nbm.StationForecast{}
		rt.seen = map[string]bool{}
		if s == nil || s.store == nil {
			return
		}
		rows, err := s.store.WeatherResearchAudit(context.Background(), 5000)
		if err != nil {
			return
		}
		for _, row := range rows {
			if row.Category != wxNBMCohort {
				continue
			}
			var rec wxNBMRecord
			if json.Unmarshal([]byte(row.Detail), &rec) == nil && rec.EventKey != "" && rec.ModelRun != "" && rec.InputStatus == "ready" {
				rt.seen[rec.EventKey+"|"+rec.ModelRun] = true
			}
		}
	})
	return rt
}

// cacheWeatherNBMForecasts bridges the independently scheduled official-source collector to the
// existing source-verified weather sweep. It adds no network request and retains only the newest
// model run per exact station.
func (s *Server) cacheWeatherNBMForecasts(rows []nbm.StationForecast) {
	rt := s.wxNBMRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, row := range rows {
		station := strings.ToUpper(strings.TrimSpace(row.Station))
		if station == "" || row.Run.IsZero() || row.ArtifactURL == "" {
			continue
		}
		if prior, ok := rt.forecasts[station]; !ok || row.Run.After(prior.Run) {
			rt.forecasts[station] = row
		}
	}
}

func (s *Server) weatherNBMCacheCounts() (forecasts, seen int) {
	rt := s.wxNBMRuntime()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return len(rt.forecasts), len(rt.seen)
}

func wxNBMDailyMaxima(f nbm.StationForecast) ([]wxNBMDailyMax, error) {
	fhr, utc := f.Values["FHR"], f.Values["UTC"]
	if f.Run.IsZero() || len(fhr) == 0 || len(utc) != len(fhr) {
		return nil, errors.New("NBM forecast lacks aligned run/FHR/UTC rows")
	}
	for _, q := range wxNBMPercentiles {
		if len(f.Values[q.Field]) != len(fhr) {
			return nil, fmt.Errorf("NBM %s row is not aligned with FHR", q.Field)
		}
	}
	seen := map[string]bool{}
	out := make([]wxNBMDailyMax, 0, len(fhr)/2)
	for i, hour := range fhr {
		if hour < 0 || hour > 360 {
			return nil, fmt.Errorf("NBM forecast hour %d outside 0..360", hour)
		}
		valid := f.Run.UTC().Add(time.Duration(hour) * time.Hour)
		if utc[i] != valid.Hour() {
			return nil, fmt.Errorf("NBM UTC/FHR alignment mismatch at index %d", i)
		}
		if utc[i] != 0 {
			continue // NOAA bulletin contract: minimum at 12 UTC, maximum at 00 UTC.
		}
		point := wxNBMDailyMax{ValidUTC: valid, FHR: hour, Values: make([]wxNBMQuantile, 0, len(wxNBMPercentiles))}
		last := math.Inf(-1)
		for _, q := range wxNBMPercentiles {
			v := float64(f.Values[q.Field][i])
			if v < last {
				return nil, fmt.Errorf("NBM daily-max quantiles are not monotone at %s", valid.Format(time.RFC3339))
			}
			last = v
			point.Values = append(point.Values, wxNBMQuantile{Field: q.Field, Percentile: q.P, ValueF: v})
		}
		key := valid.Format(time.RFC3339)
		if seen[key] {
			return nil, fmt.Errorf("NBM contains duplicate daily-max valid time %s", key)
		}
		seen[key] = true
		out = append(out, point)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ValidUTC.Before(out[j].ValidUTC) })
	if len(out) == 0 {
		return nil, errors.New("NBM forecast contains no 00 UTC daily-maximum values")
	}
	return out, nil
}

// wxNBMLocalDate requires an hourly NWS period at the same instant. Its RFC3339 offset is the
// station's actual offset at that future instant, including a DST transition; a guessed city/time
// zone or today's offset is never substituted.
func wxNBMLocalDate(periods []wxPeriod, validUTC time.Time) (string, bool) {
	validUTC = validUTC.UTC()
	for _, p := range periods {
		if p.start.UTC().Equal(validUTC) {
			return p.start.Format("2006-01-02"), true
		}
	}
	return "", false
}

func wxNBMCDFBounds(q []wxNBMQuantile, threshold float64) (lower, upper float64, ok bool) {
	if len(q) != len(wxNBMPercentiles) {
		return 0, 0, false
	}
	lower, upper = 0, 1
	lastV, lastP := math.Inf(-1), -1.0
	for _, point := range q {
		if point.ValueF < lastV || point.Percentile <= lastP || point.Percentile <= 0 || point.Percentile >= 1 {
			return 0, 0, false
		}
		lastV, lastP = point.ValueF, point.Percentile
		if point.ValueF <= threshold && point.Percentile > lower {
			lower = point.Percentile
		}
		if point.ValueF > threshold && point.Percentile < upper {
			upper = point.Percentile
		}
	}
	return lower, upper, lower <= upper
}

// wxNBMBinProbabilityBounds returns the sharp interval implied by the five reported quantile
// order constraints. For an integer-valued CLI high, P(lo<=X<=hi)=F(hi)-F(lo-1).
func wxNBMBinProbabilityBounds(q []wxNBMQuantile, lo, hi float64) (lower, upper float64, ok bool) {
	if hi < lo {
		return 0, 0, false
	}
	loLower, loUpper := 0.0, 0.0
	hiLower, hiUpper := 1.0, 1.0
	if lo > -900 {
		loLower, loUpper, ok = wxNBMCDFBounds(q, lo-1)
		if !ok {
			return 0, 0, false
		}
	}
	if hi < 900 {
		hiLower, hiUpper, ok = wxNBMCDFBounds(q, hi)
		if !ok {
			return 0, 0, false
		}
	}
	lower = math.Max(0, hiLower-loUpper)
	upper = math.Min(1, hiUpper-loLower)
	if lower > upper+1e-12 {
		return 0, 0, false
	}
	return lower, upper, true
}

func wxNBMEnvelopeBooks(books []wxResearchBook, q []wxNBMQuantile) ([]wxNBMProbabilityBin, *wxNBMChoice, error) {
	if len(books) < 2 {
		return nil, nil, errors.New("NBM mapping requires a complete ladder")
	}
	bins := make([]wxNBMProbabilityBin, 0, len(books))
	best := wxNBMChoice{NetLower: math.Inf(-1)}
	for _, b := range books {
		lo, hi, ok := wxNBMBinProbabilityBounds(q, b.LoF, b.HiF)
		if !ok {
			return nil, nil, fmt.Errorf("could not bound NBM probability for %s", b.Ticker)
		}
		row := wxNBMProbabilityBin{Ticker: b.Ticker, LoF: b.LoF, HiF: b.HiF,
			ProbLower: lo, ProbUpper: hi, NoProbLower: 1 - hi, NoProbUpper: 1 - lo, Book: b}
		if b.YesExecutable {
			net := lo - b.YesAsk - b.YesTakerFeePC
			row.YesNetLower = &net
			choice := wxNBMChoice{Ticker: b.Ticker, Side: "YES", ProbLower: lo, ProbUpper: hi,
				Ask: b.YesAsk, FeePC: b.YesTakerFeePC, Depth: b.YesAskDepth, Tick: b.YesTakerTick,
				QuoteAgeS: b.QuoteAgeS, NetLower: net, NetUpper: hi - b.YesAsk - b.YesTakerFeePC, BookSource: b.BookSource}
			if choice.NetLower > best.NetLower || (choice.NetLower == best.NetLower && choice.Ticker+choice.Side < best.Ticker+best.Side) {
				best = choice
			}
		}
		if b.NoExecutable {
			net := (1 - hi) - b.NoAsk - b.NoTakerFeePC
			row.NoNetLower = &net
			choice := wxNBMChoice{Ticker: b.Ticker, Side: "NO", ProbLower: 1 - hi, ProbUpper: 1 - lo,
				Ask: b.NoAsk, FeePC: b.NoTakerFeePC, Depth: b.NoAskDepth, Tick: b.NoTakerTick,
				QuoteAgeS: b.QuoteAgeS, NetLower: net, NetUpper: 1 - lo - b.NoAsk - b.NoTakerFeePC, BookSource: b.BookSource}
			if choice.NetLower > best.NetLower || (choice.NetLower == best.NetLower && choice.Ticker+choice.Side < best.Ticker+best.Side) {
				best = choice
			}
		}
		bins = append(bins, row)
	}
	if math.IsInf(best.NetLower, -1) {
		return bins, nil, errWxNoExecutableBook
	}
	return bins, &best, nil
}

func (s *Server) wxNBMAudit(ctx context.Context, rec wxNBMRecord) bool {
	rec.Kind, rec.System, rec.Model, rec.Cohort = "probability_envelope", wxNBMSystem, wxNBMModel, wxNBMCohort
	if rec.ObservedAt == "" {
		rec.ObservedAt = time.Now().UTC().Format(time.RFC3339Nano)
	}
	rec.Funded, rec.PaperAuthority, rec.LiveAuthority = false, false, false
	rec.Execution = "research-only direct settlement row; no paper, maker, portfolio, or LIVE dispatch"
	rec.SettlementPath = "exact venue NWS-CLI station/date/bin identity; canonical Kalshi settlement worker grades the research signal"
	rec.SourceIndependence = wxNBMSourceIndependence
	rec.ProbabilityMethod = "quantile-order probability envelope only; no interpolation, normality, synthetic tails, midpoint, or point-price proxy"
	rec.PromotionBlocked = true
	b, err := json.Marshal(rec)
	if err != nil {
		return false
	}
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	wctx, cancel := context.WithTimeout(base, 2*time.Second)
	defer cancel()
	if err := s.store.Audit(wctx, "info", wxNBMCohort, "prospective NBM probability envelope", string(b)); err != nil {
		return false
	}
	s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
		c.Records++
		if rec.InputStatus == "ready" {
			c.ReadyRecords++
		}
		if rec.SignalLogged {
			c.Signals++
		}
	})
	return true
}

func (s *Server) wxNBMObserveSeries(ctx context.Context, st *wxStation, mkts []kalshi.Market,
	byDate map[string][]int, periods []wxPeriod) {
	if s == nil || s.store == nil || st == nil || st.StationID == "" || len(periods) == 0 {
		return
	}
	rt := s.wxNBMRuntime()
	station := strings.ToUpper(strings.TrimSpace(st.StationID))
	rt.mu.Lock()
	forecast, ok := rt.forecasts[station]
	rt.mu.Unlock()
	if !ok || forecast.Run.IsZero() || forecast.ArtifactURL == "" {
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_station_forecast_unavailable"]++ })
		return
	}
	s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.NBMForecasts++ })
	now := time.Now()
	age := now.Sub(forecast.Run)
	if age < 0 || age > wxNBMMaxRunAge {
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_model_run_stale_or_future"]++ })
		return
	}
	points, err := wxNBMDailyMaxima(forecast)
	if err != nil {
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_quantile_schema_invalid"]++ })
		return
	}
	matchedOpenDate := false
	for _, point := range points {
		date, dateOK := wxNBMLocalDate(periods, point.ValidUTC)
		if !dateOK {
			s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_valid_time_local_date_unverified"]++ })
			continue
		}
		idxs, listed := byDate[date]
		if !listed {
			continue
		}
		matchedOpenDate = true
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
			c.Events++
			c.NBMDateMatches++
		})
		eventKey := st.Series + "|" + date
		seenKey := eventKey + "|" + forecast.Run.UTC().Format(time.RFC3339Nano)
		rt.mu.Lock()
		already := rt.seen[seenKey]
		rt.mu.Unlock()
		if already {
			continue
		}
		books, markets, event, bookErr := s.wxLadderBooks(mkts, idxs)
		rec := wxNBMRecord{EventKey: eventKey, Series: st.Series, EventTicker: event, LocalDate: date,
			SettlementStation: station, SettlementSource: "NWS CLI source recorded in Kalshi series metadata",
			ModelRun: forecast.Run.UTC().Format(time.RFC3339Nano), ValidUTC: point.ValidUTC.UTC().Format(time.RFC3339Nano),
			ForecastHour: point.FHR, SourceArtifact: forecast.ArtifactURL, Quantiles: point.Values}
		if event == "" || (bookErr != nil && !errors.Is(bookErr, errWxNoExecutableBook)) {
			s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_event_or_ladder_identity_unavailable"]++ })
			rec.InputStatus = "unavailable"
			if event == "" {
				rec.InputReason = "exact event identity unavailable"
			} else {
				rec.InputReason = bookErr.Error()
			}
			s.wxNBMAudit(ctx, rec) // do not mark seen; a later exact book may repair the input.
			continue
		}
		bins, best, envelopeErr := wxNBMEnvelopeBooks(books, point.Values)
		if envelopeErr != nil {
			s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) {
				if errors.Is(envelopeErr, errWxNoExecutableBook) {
					c.Exclusions["nbm_executable_book_unavailable"]++
				} else {
					c.Exclusions["nbm_probability_envelope_invalid"]++
				}
			})
			rec.InputStatus, rec.InputReason, rec.Bins = "unavailable", envelopeErr.Error(), bins
			s.wxNBMAudit(ctx, rec)
			continue
		}
		rec.InputStatus, rec.Bins, rec.Best = "ready", bins, best
		rec.Candidate = best != nil && best.NetLower > 0
		if rec.Candidate {
			s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.NBMCandidates++ })
		}

		decisionStart := time.Now()
		if rec.Candidate {
			book, bookOK := wxFindResearchBook(books, best.Ticker)
			market, marketOK := markets[best.Ticker]
			fee, feeKnown, feeSource := s.kalFeeExact(best.Ticker, false, 1, best.Ask)
			if !bookOK || !marketOK || !feeKnown || feeSource == "" || math.Abs(fee-best.FeePC) > 1e-9 {
				s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_fee_revalidation_unavailable"]++ })
				rec.Candidate = false
				rec.InputStatus = "unavailable"
				rec.InputReason = "exact current fee authority could not be revalidated"
				s.wxNBMAudit(ctx, rec)
				continue
			}
			logged, insertErr := s.wxInsertResearchSignal(ctx, wxNBMSystem, best.Side, book, market,
				best.ProbLower, best.NetLower, best.ProbUpper-best.ProbLower)
			if insertErr != nil {
				s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_settlement_row_storage_error"]++ })
				rec.InputStatus, rec.InputReason = "unavailable", "canonical research settlement row could not be persisted"
				s.wxNBMAudit(ctx, rec)
				continue
			}
			// inserted=false with nil means the exact system/ticker/side row already exists in this
			// ten-minute settlement slot (for example after an audit write retry). Either way the
			// canonical grade row is present, so this model-run/event can be durably latched.
			rec.SignalLogged = logged || insertErr == nil
			resolveHours := wxResolveHoursFromMarket(market)
			certificate := storage.R138HashJSON(map[string]any{"series": st.Series, "event": event,
				"date": date, "station": station, "ticker": best.Ticker, "side": best.Side,
				"settlement_source": rec.SettlementSource, "bins": bins})
			_, inserted, obsErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
				Observed: time.Now(), SystemID: "deadline-hazard-surface", OpportunityID: wxNBMCohort + "|" + seenKey,
				Kind: "candidate", Cohort: wxNBMCohort, CanonicalEventID: "venue:kalshi:" + event,
				Venue: "kalshi", Route: "taker", Side: best.Side, Ticker: best.Ticker,
				CertificateStatus: "verified", CertificateHash: certificate,
				SourceClockID: "noaa-nbm|" + station + "|" + rec.ModelRun, SourceArtifact: forecast.ArtifactURL,
				BookSource: best.BookSource, FeeSource: feeSource, QuoteAgeMax: best.QuoteAgeS,
				TickMin: best.Tick, Size: 1, Cost: best.Ask, Fee: fee,
				PayoutLower: best.ProbLower, PayoutUpper: best.ProbUpper,
				NetLower: best.NetLower, NetUpper: best.NetUpper, VisibleCapacity: best.Depth,
				CapitalSeconds: math.Max(0, resolveHours*3600), DecisionLatencyMS: time.Since(decisionStart).Seconds() * 1000,
				CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: best.Ask, Fee: fee,
					PayoutFloor: best.ProbLower, NetFloor: best.NetLower}},
				OutcomeStatus: "open", Candidate: true, LatencyKnown: true, QuoteAgeKnown: true,
				TickKnown: true, DepthKnown: true, FeeKnown: true,
				Inputs: map[string]any{"model": wxNBMModel, "model_run": rec.ModelRun,
					"valid_utc": rec.ValidUTC, "forecast_hour": point.FHR, "station": station,
					"local_date": date, "quantiles": point.Values, "probability_method": "order_bounds_no_interpolation",
					"paper_authority": false, "live_authority": false},
			})
			if obsErr == nil {
				rec.SystemObservationLogged = inserted
			}
		}
		if s.wxNBMAudit(ctx, rec) && (!rec.Candidate || rec.SignalLogged) {
			rt.mu.Lock()
			rt.seen[seenKey] = true
			rt.mu.Unlock()
		}
	}
	if !matchedOpenDate {
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Exclusions["nbm_no_open_exact_local_date"]++ })
	}
}
