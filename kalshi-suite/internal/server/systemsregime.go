package server

// Research-only Systems regime leaderboard.  It reads native execution ledgers and never consults
// signal_log prices.  The endpoint has no placement, AUTO, ARM, sizing, or config mutation path.

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

type systemRegimeObservation struct {
	System, Signal, Venue, Route, Source string
	FeeSource                            string
	OriginLayer                          string
	Evidence                             string
	Opened, Closed                       time.Time
	CostPC, FeePC, PnLPC, Depth          float64
	QueueAhead                           float64
	PnLKnown, FeeKnown, DepthKnown       bool
	QueueKnown, Filled, Attempted        bool
	Terminal, Canceled                   bool
	ResearchOnly, VenueLocked            bool
	CompleteHistory                      bool
	ClockKnown                           bool
}

type systemRegimeCoverage struct {
	Source string `json:"source"`
	State  string `json:"state"`
	Rows   int    `json:"rows"`
	Note   string `json:"note"`
}

type systemRegimeRow struct {
	System                   string   `json:"system"`
	Signal                   string   `json:"signal"`
	Layer                    string   `json:"layer"`
	Venue                    string   `json:"venue"`
	Route                    string   `json:"route"`
	Evidence                 string   `json:"evidence"`
	Window                   string   `json:"window"`
	Source                   string   `json:"source"`
	FeeSource                string   `json:"fee_source,omitempty"`
	OriginLayer              string   `json:"origin_layer"`
	State                    string   `json:"state"`
	ResearchOnly             bool     `json:"research_only"`
	FillConditioned          bool     `json:"fill_conditioned"`
	ProfitEvidence           bool     `json:"profit_evidence"`
	VoidReason               string   `json:"void_reason,omitempty"`
	LiveAuthorizes           bool     `json:"live_authorizes"`
	VenueLocked              bool     `json:"venue_locked"`
	HistoryComplete          bool     `json:"history_complete"`
	QualifiesForReview       bool     `json:"qualifies_for_review"`
	Observations             int      `json:"observations"`
	Settled                  int      `json:"settled"`
	Open                     int      `json:"open"`
	Attempts                 int      `json:"attempts"`
	Fills                    int      `json:"fills"`
	Cancels                  int      `json:"cancels"`
	FillRate                 *float64 `json:"fill_rate,omitempty"`
	TotalPnL                 float64  `json:"total_pnl"`
	MeanPnLPC                float64  `json:"mean_pnl_pc"`
	SDPnLPC                  float64  `json:"sd_pnl_pc"`
	ElapsedSeconds           float64  `json:"elapsed_seconds"`
	NetPerDay                float64  `json:"net_per_day"`
	NetPerDayLower           *float64 `json:"net_per_day_lower,omitempty"`
	NetPerDayUpper           *float64 `json:"net_per_day_upper,omitempty"`
	UTCBlockDays             int      `json:"utc_block_days"`
	MatureUTCBlockDays       int      `json:"mature_utc_block_days"`
	ProofSettled             int      `json:"proof_settled"`
	ProofOpen                int      `json:"proof_open"`
	ProofFeeKnownShare       float64  `json:"proof_fee_known_share"`
	OccupiedSeconds          float64  `json:"occupied_seconds"`
	OccupiedClockCoverage    float64  `json:"occupied_clock_coverage"`
	NetPerOccupiedShareDay   *float64 `json:"net_per_occupied_share_day,omitempty"`
	EntryDollarSeconds       float64  `json:"entry_dollar_seconds"`
	ReturnPerEntryDollarDay  *float64 `json:"return_per_entry_dollar_day,omitempty"`
	FeeKnownShare            float64  `json:"fee_known_share"`
	MeanFeePC                *float64 `json:"mean_fee_pc,omitempty"`
	DepthKnownShare          float64  `json:"depth_known_share"`
	MeanDepth                *float64 `json:"mean_depth,omitempty"`
	MedianDepth              *float64 `json:"median_depth,omitempty"`
	MechanicalCapacityNetDay *float64 `json:"mechanical_capacity_net_per_day_at_visible_depth,omitempty"`
	QueueKnownShare          float64  `json:"queue_known_share"`
	MeanQueueAhead           *float64 `json:"mean_queue_ahead,omitempty"`
}

type systemWindow struct {
	name string
	days int
}

var systemRegimeWindows = []systemWindow{
	{name: "7d", days: 7},
	{name: "30d", days: 30},
	{name: "90d", days: 90},
	{name: "all"},
}

func parseSystemTime(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func sysLayer(evidence string) string {
	switch evidence {
	case "maker_attempt", "paper_fill", "paper_combo", "paper_lock", "live_fill":
		return "execution_route"
	case "observed_book_simulated_fill", "historical_assumed_fill_simulation_void":
		return "observed_book"
	case "executable_quote", "research_quote":
		return "research_quote"
	default:
		return "system"
	}
}

func ptrFloat(v float64) *float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return &v
}

func exactSeconds(end, start time.Time) float64 {
	d := end.Sub(start).Seconds()
	if d < 1 {
		return 1
	}
	return d
}

// deterministicDayBounds is a fixed-seed, day-block bootstrap.  The input already includes zero
// UTC days, so quiet days remain in the uncertainty distribution.  It is deterministic across
// boots and does not pretend individual same-day signals are independent.
func deterministicDayBounds(dayNet []float64, key string) (lo, hi float64, ok bool) {
	if len(dayNet) < 2 {
		return 0, 0, false
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	x := h.Sum64()
	if x == 0 {
		x = 1
	}
	const reps = 2048
	means := make([]float64, reps)
	for b := 0; b < reps; b++ {
		sum := 0.0
		for range dayNet {
			x = x*6364136223846793005 + 1442695040888963407
			sum += dayNet[int(x%uint64(len(dayNet)))]
		}
		means[b] = sum / float64(len(dayNet))
	}
	sort.Float64s(means)
	return means[reps/20], means[reps*19/20], true
}

func utcDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func summarizeSystemWindow(all []systemRegimeObservation, win systemWindow, asOf time.Time) (systemRegimeRow, bool) {
	if len(all) == 0 {
		return systemRegimeRow{}, false
	}
	asOf = asOf.UTC()
	earliest := all[0].Opened
	for _, o := range all[1:] {
		if o.Opened.Before(earliest) {
			earliest = o.Opened
		}
	}
	start := earliest
	if win.days > 0 {
		// A regime names exactly N UTC day blocks, including the current partial day. The
		// Net/day denominator still uses exact seconds from this midnight boundary.
		cut := utcDay(asOf).AddDate(0, 0, -(win.days - 1))
		if cut.After(start) {
			start = cut
		}
	}
	var rows []systemRegimeObservation
	for _, o := range all {
		if !o.Opened.Before(start) && !o.Opened.After(asOf) {
			rows = append(rows, o)
		}
	}
	if len(rows) == 0 {
		return systemRegimeRow{}, false
	}
	base := rows[0]
	r := systemRegimeRow{System: base.System, Signal: base.Signal, Layer: sysLayer(base.Evidence),
		Venue: base.Venue, Route: base.Route, Evidence: base.Evidence, Window: win.name,
		Source: base.Source, FeeSource: base.FeeSource, OriginLayer: base.OriginLayer, ResearchOnly: base.ResearchOnly, VenueLocked: base.VenueLocked,
		HistoryComplete: base.CompleteHistory,
		FillConditioned: base.Evidence == "live_fill" || base.Evidence == "paper_fill" || base.Evidence == "paper_combo" || base.Evidence == "paper_lock",
		ProfitEvidence:  base.Evidence == "live_fill",
		LiveAuthorizes:  false,
		Observations:    len(rows), ElapsedSeconds: exactSeconds(asOf, start)}
	if !r.ProfitEvidence {
		r.VoidReason = "not an exchange-observed LIVE fill cohort"
	}
	fees, queueSum := 0.0, 0.0
	feeN, depthN, queueN, clockN := 0, 0, 0, 0
	var depths []float64
	mean, m2, capPnL := 0.0, 0.0, 0.0
	dayStart, dayEnd := utcDay(start), utcDay(asOf)
	blockN := int(dayEnd.Sub(dayStart).Hours()/24) + 1
	for _, o := range rows {
		if o.Attempted {
			r.Attempts++
		}
		if o.Filled {
			r.Fills++
		}
		if o.Canceled {
			r.Cancels++
		}
		if o.PnLKnown {
			r.Settled++
			r.TotalPnL += o.PnLPC
			delta := o.PnLPC - mean
			mean += delta / float64(r.Settled)
			m2 += delta * (o.PnLPC - mean)
			if o.DepthKnown && o.Depth > 0 {
				capPnL += o.PnLPC * math.Floor(o.Depth)
			}
		} else {
			r.Open++
		}
		if o.FeeKnown {
			fees += o.FeePC
			feeN++
		}
		if o.DepthKnown {
			depths = append(depths, o.Depth)
			depthN++
		}
		if o.QueueKnown {
			queueSum += o.QueueAhead
			queueN++
		}
		if o.ClockKnown {
			end := asOf
			if o.Terminal && !o.Closed.IsZero() {
				end = o.Closed
			}
			if end.After(asOf) {
				end = asOf
			}
			secs := exactSeconds(end, o.Opened)
			r.OccupiedSeconds += secs
			r.EntryDollarSeconds += math.Max(o.CostPC, 0) * secs
			clockN++
		}
	}
	r.MeanPnLPC = mean
	if r.Settled > 1 {
		r.SDPnLPC = math.Sqrt(m2 / float64(r.Settled-1))
	}
	elapsedDays := r.ElapsedSeconds / 86400
	r.NetPerDay = r.TotalPnL / elapsedDays
	r.UTCBlockDays = blockN

	// Proof uses only completed UTC days. The first partial collection day and current partial day
	// are excluded; the point estimate above intentionally keeps its exact-seconds live display.
	proofStart := utcDay(earliest)
	if !earliest.Equal(proofStart) {
		proofStart = proofStart.AddDate(0, 0, 1)
	}
	proofEnd := utcDay(asOf)
	if win.days > 0 {
		target := proofEnd.AddDate(0, 0, -win.days)
		if target.After(proofStart) {
			proofStart = target
		}
	}
	if proofEnd.After(proofStart) {
		r.MatureUTCBlockDays = int(proofEnd.Sub(proofStart).Hours() / 24)
	}
	proofDayNet := make([]float64, r.MatureUTCBlockDays)
	proofRows, proofFeeN := 0, 0
	for _, o := range all {
		if o.Opened.Before(proofStart) || !o.Opened.Before(proofEnd) {
			continue
		}
		proofRows++
		if o.FeeKnown {
			proofFeeN++
		}
		if !o.PnLKnown {
			r.ProofOpen++
			continue
		}
		r.ProofSettled++
		di := int(utcDay(o.Opened).Sub(proofStart).Hours() / 24)
		if di >= 0 && di < len(proofDayNet) {
			proofDayNet[di] += o.PnLPC
		}
	}
	if proofRows > 0 {
		r.ProofFeeKnownShare = float64(proofFeeN) / float64(proofRows)
	}
	if lo, hi, ok := deterministicDayBounds(proofDayNet, base.System+"|"+base.Venue+"|"+base.Route+"|"+win.name); ok {
		r.NetPerDayLower, r.NetPerDayUpper = ptrFloat(lo), ptrFloat(hi)
	}
	if r.Attempts > 0 {
		r.FillRate = ptrFloat(float64(r.Fills) / float64(r.Attempts))
	}
	if len(rows) > 0 {
		r.FeeKnownShare = float64(feeN) / float64(len(rows))
		r.DepthKnownShare = float64(depthN) / float64(len(rows))
		r.QueueKnownShare = float64(queueN) / float64(len(rows))
		r.OccupiedClockCoverage = float64(clockN) / float64(len(rows))
	}
	if feeN > 0 {
		r.MeanFeePC = ptrFloat(fees / float64(feeN))
	}
	if depthN > 0 {
		sort.Float64s(depths)
		s := 0.0
		for _, d := range depths {
			s += d
		}
		r.MeanDepth = ptrFloat(s / float64(depthN))
		r.MedianDepth = ptrFloat(depths[len(depths)/2])
		r.MechanicalCapacityNetDay = ptrFloat(capPnL / elapsedDays)
	}
	if queueN > 0 {
		r.MeanQueueAhead = ptrFloat(queueSum / float64(queueN))
		r.QueueKnownShare = float64(queueN) / float64(len(rows))
	}
	if r.OccupiedSeconds > 0 {
		r.NetPerOccupiedShareDay = ptrFloat(r.TotalPnL / (r.OccupiedSeconds / 86400))
	}
	if r.EntryDollarSeconds > 0 {
		r.ReturnPerEntryDollarDay = ptrFloat(r.TotalPnL / (r.EntryDollarSeconds / 86400))
	}
	r.State = "COLLECTING"
	requiredBlocks := 7
	if win.days > 0 {
		requiredBlocks = win.days
	}
	if !r.HistoryComplete {
		r.State = "PARTIAL-HISTORY"
	} else if r.NetPerDayLower != nil && r.Open == 0 && r.ProofOpen == 0 &&
		r.ProofSettled >= 20 && r.MatureUTCBlockDays >= requiredBlocks &&
		r.FeeKnownShare >= .999999 && r.ProofFeeKnownShare >= .999999 {
		if *r.NetPerDayLower > 0 {
			r.State = "LOWER-BOUND+"
		} else if r.NetPerDayUpper != nil && *r.NetPerDayUpper < 0 {
			r.State = "LOWER-BOUND-"
		}
	}
	// This is a research promotion REVIEW flag only.  It cannot place, arm, size, or fund.  The two
	// explicitly unfunded cohorts and venue-locked Poly-int remain ineligible by construction.
	r.QualifiesForReview = win.name == "30d" && r.State == "LOWER-BOUND+" && r.HistoryComplete && r.ProfitEvidence &&
		r.DepthKnownShare >= .999999 && r.MedianDepth != nil && *r.MedianDepth >= 1 &&
		r.OriginLayer == "strategy" && !r.ResearchOnly && !r.VenueLocked
	if base.Evidence == "historical_assumed_fill_simulation_void" {
		r.State = "VOID ASSUMED-FILL HISTORY"
		r.QualifiesForReview = false
	}
	return r, true
}

func systemsRegimeRows(obs []systemRegimeObservation, asOf time.Time) []systemRegimeRow {
	groups := map[string][]systemRegimeObservation{}
	for _, o := range obs {
		if o.Opened.IsZero() || o.Opened.After(asOf) {
			continue
		}
		key := strings.Join([]string{o.System, o.Signal, o.Venue, o.Route, o.Source, o.FeeSource, o.Evidence, o.OriginLayer,
			strconv.FormatBool(o.CompleteHistory),
			strconv.FormatBool(o.ResearchOnly), strconv.FormatBool(o.VenueLocked)}, "\x00")
		groups[key] = append(groups[key], o)
	}
	var out []systemRegimeRow
	for _, g := range groups {
		for _, win := range systemRegimeWindows {
			if r, ok := summarizeSystemWindow(g, win, asOf); ok {
				out = append(out, r)
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		wi, wj := 99, 99
		for k, w := range systemRegimeWindows {
			if out[i].Window == w.name {
				wi = k
			}
			if out[j].Window == w.name {
				wj = k
			}
		}
		if wi != wj {
			return wi < wj
		}
		li, lj := -math.MaxFloat64, -math.MaxFloat64
		if out[i].NetPerDayLower != nil {
			li = *out[i].NetPerDayLower
		}
		if out[j].NetPerDayLower != nil {
			lj = *out[j].NetPerDayLower
		}
		if li != lj {
			return li > lj
		}
		if out[i].NetPerDay != out[j].NetPerDay {
			return out[i].NetPerDay > out[j].NetPerDay
		}
		return out[i].System+out[i].Venue+out[i].Route < out[j].System+out[j].Venue+out[j].Route
	})
	return out
}

func fromNativeObservation(n storage.NativeSystemObservation, evidence string, researchOnly bool) (systemRegimeObservation, bool) {
	opened, ok := parseSystemTime(n.OpenedTS)
	if !ok {
		return systemRegimeObservation{}, false
	}
	o := systemRegimeObservation{System: n.System, Signal: n.Signal, Venue: strings.ToLower(n.Venue),
		Route: n.Route, Source: n.Source, OriginLayer: n.OriginLayer, Evidence: evidence, Opened: opened, CostPC: n.CostPC,
		FeePC: n.FeePC, PnLPC: n.PnLPC, Depth: n.Depth, QueueAhead: n.QueueAhead,
		PnLKnown: n.PnLKnown, FeeKnown: n.FeeKnown, DepthKnown: n.DepthKnown,
		QueueKnown: n.QueueKnown, Filled: n.Filled, Terminal: n.Terminal,
		ResearchOnly: researchOnly, VenueLocked: strings.EqualFold(n.Venue, "polymarket"), CompleteHistory: true}
	if closed, cok := parseSystemTime(n.ClosedTS); cok && !closed.Before(opened) {
		o.Closed, o.ClockKnown = closed, true
	} else if !n.Terminal {
		o.ClockKnown = true // an unresolved row is honestly censored through the API as-of clock
	}
	if strings.Contains(strings.ToLower(n.Route), "maker") {
		o.Attempted = true
		o.Canceled = n.Terminal && !n.Filled
	}
	return o, true
}

func (s *Server) collectStorageSystemObservations(ctx context.Context) ([]systemRegimeObservation, []systemRegimeCoverage) {
	var out []systemRegimeObservation
	var cov []systemRegimeCoverage
	if rows, err := s.store.NativeUnitSystemObservations(ctx); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "unit_trials", State: "unavailable", Note: err.Error()})
	} else {
		unknownDepth, unknownFee := 0, 0
		for _, n := range rows {
			if !n.DepthKnown || n.Depth < 1 {
				unknownDepth++
				continue
			}
			if !n.FeeKnown {
				unknownFee++
				continue
			}
			if o, ok := fromNativeObservation(n, "historical_assumed_fill_simulation_void", true); ok {
				o.Source = "unit_trials" // quote-source variants stay one system+venue+taker route
				out = append(out, o)
			}
		}
		used := len(rows) - unknownDepth - unknownFee
		cov = append(cov, systemRegimeCoverage{Source: "unit_trials", State: "available", Rows: used,
			Note: fmt.Sprintf("observed side ask, exact observation-time fee, visible depth>=1 and settlement; fill is simulated because no order was submitted; unknown_depth_excluded=%d, unknown_fee_excluded=%d coverage-only rows; signal price is never read", unknownDepth, unknownFee)})
	}
	if rows, err := s.store.NativeMakerObservations(ctx); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "maker_fill_stats", State: "unavailable", Note: err.Error()})
	} else {
		used, untagged, unknownFee := 0, 0, 0
		for _, n := range rows {
			if strings.TrimSpace(n.Family) == "" {
				untagged++
				continue // a legacy source string cannot reconstruct direct-vs-inverted system truth
			}
			opened, ok := parseSystemTime(n.OpenedTS)
			if !ok || n.PostPx <= 0 || n.PostPx >= 1 {
				continue
			}
			route := "maker/direct/" + n.Source
			if n.Inverted {
				route = "maker/inverted/" + n.Source
			}
			o := systemRegimeObservation{System: n.Family, Signal: n.Family,
				Venue: strings.ToLower(n.Venue), Route: route, Source: "maker_fill_stats",
				OriginLayer: "strategy",
				Evidence:    "maker_attempt", Opened: opened, CostPC: n.PostPx, Depth: n.Depth,
				DepthKnown: n.Depth >= 1, QueueAhead: n.QueueAhead, QueueKnown: n.QueueKnown,
				Attempted: true, VenueLocked: strings.EqualFold(n.Venue, "polymarket"), CompleteHistory: true}
			if closed, cok := parseSystemTime(n.ClosedTS); cok && !closed.Before(opened) {
				o.Closed, o.ClockKnown = closed, true
			} else if n.State == -1 {
				o.ClockKnown = true
			}
			switch n.State {
			case 0: // an expired/canceled quote is a terminal economic $0 outcome
				o.Terminal, o.Canceled, o.PnLKnown, o.FeeKnown = true, true, true, true
			case 1:
				o.Filled = true
				if !n.FeeKnown {
					unknownFee++
					continue // legacy fill: today's mutable schedule is never substituted
				}
				fee := n.FeePC
				o.FeePC, o.FeeKnown, o.CostPC = fee, true, n.PostPx+fee
				if !n.SettleKnown {
					break
				}
				pay := n.Settle
				if strings.EqualFold(n.Side, "NO") || strings.EqualFold(n.Side, "DOWN") {
					pay = 1 - n.Settle
				}
				o.PnLKnown, o.Terminal = true, true
				o.PnLPC = pay - n.PostPx - fee
			}
			out, used = append(out, o), used+1
		}
		state := "available"
		if used == 0 {
			state = "collecting"
		}
		cov = append(cov, systemRegimeCoverage{Source: "maker_fill_stats", State: state, Rows: used,
			Note: fmt.Sprintf("terminal posted attempts including $0 cancels; visible queue-ahead when present; %d legacy untagged and %d fills without a persisted fill-time fee/rebate receipt excluded from all economics", untagged, unknownFee)})
	}
	if rows, err := s.store.NativePaperParlayObservations(ctx); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "paper_parlays", State: "unavailable", Note: err.Error()})
	} else {
		// This legacy table stores fee-net settlement but has no book-feature version, quote
		// source, per-leg depth, or route receipt. Pre-book and current rows are indistinguishable;
		// exposing their P&L would violate the endpoint's no-proxy contract. xvc/Parlay-Lab rows
		// below are the native combo lanes until this table gains explicit provenance.
		cov = append(cov, systemRegimeCoverage{Source: "paper_parlays", State: "unavailable", Rows: len(rows),
			Note: "fee-net rows exist, but the schema cannot distinguish legacy signal-price entries from book-native quotes; excluded rather than time-cut or guessed"})
	}
	if rows, err := s.store.NativeSubcentGolfObservations(ctx); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "subcent_golf_trials", State: "unavailable", Note: err.Error()})
	} else {
		for _, n := range rows {
			if o, ok := fromNativeObservation(n, "research_quote", true); ok {
				o.Source = "subcent_golf_trials"
				o.Attempted = n.Route == "maker"
				out = append(out, o)
			}
		}
		state := "available"
		if len(rows) == 0 {
			state = "collecting"
		}
		cov = append(cov, systemRegimeCoverage{Source: "subcent_golf_trials", State: state, Rows: len(rows),
			Note: "unfunded phase/status/type/maker-vs-taker cohort; true queue, fill/cancel, fee/rebate and settlement fields"})
	}
	if rows, err := s.store.NativeWeatherResearchObservations(ctx); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "weather-research", State: "unavailable", Note: err.Error()})
	} else {
		for _, n := range rows {
			if o, ok := fromNativeObservation(n, "research_quote", true); ok {
				o.Source = "weather-research"
				out = append(out, o)
			}
		}
		state := "available"
		if len(rows) == 0 {
			state = "collecting"
		}
		cov = append(cov, systemRegimeCoverage{Source: "weather-research", State: state, Rows: len(rows),
			Note: "unfunded exact-book weather cohorts: curve-residual/revision remain endogenous nonpromotable diagnostics; independent-probabilistic-weather uses official NBM quantile-order lower bounds. Rows require bought-side ask, exact fee, positive depth, source and settlement."})
	}
	return out, cov
}

func readNativeJSON(path string, dst any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("parse %s: %w", filepath.Base(path), err)
	}
	return nil
}

type kfSystemIdentity func(kfPos) (system, signal, venue string)

func fixedKFIdentity(system, signal, venue string) kfSystemIdentity {
	return func(p kfPos) (string, string, string) {
		v := strings.ToLower(strings.TrimSpace(p.Platform))
		if v == "" {
			v = venue
		}
		return system, signal, v
	}
}

func appendKFSystemBook(out []systemRegimeObservation, book *kfBook, source string,
	identity kfSystemIdentity, researchOnly bool,
	proofs fundedPaperImmutableProofMap) ([]systemRegimeObservation, int, int) {
	return appendKFSystemBookWhere(out, book, source, identity, researchOnly,
		proofs.accepts)
}

func appendKFSystemBookWhere(out []systemRegimeObservation, book *kfBook, source string,
	identity kfSystemIdentity, researchOnly bool,
	accept func(kfPos) bool) ([]systemRegimeObservation, int, int) {
	used, legacy := 0, 0
	add := func(p kfPos, pnl float64, pnlKnown bool, closed string) {
		if accept != nil && !accept(p) {
			legacy++
			return
		}
		kind := strings.ToLower(strings.TrimSpace(p.FillKind))
		if kind != "maker" && kind != "taker" {
			legacy++ // pre-route rows may have been booked at a signal percentage; never infer
			return
		}
		if !p.FeeKnown || strings.TrimSpace(p.FeeSource) == "" || math.IsNaN(p.Fee) || math.IsInf(p.Fee, 0) ||
			p.Contracts <= 0 || p.Price <= 0 || p.Price >= 1 || p.Fee <= -p.Contracts || p.Fee >= p.Contracts {
			legacy++ // numeric fee alone is arithmetic, not historical schedule authority
			return
		}
		opened, ok := parseSystemTime(p.FillTS)
		if !ok {
			opened, ok = parseSystemTime(p.TS)
		}
		if !ok {
			return
		}
		system, signal, venue := identity(p)
		o := systemRegimeObservation{System: system, Signal: signal, Venue: venue, Route: kind,
			Source: source, FeeSource: strings.TrimSpace(p.FeeSource), OriginLayer: "strategy", Evidence: "paper_fill", Opened: opened,
			CostPC: p.Price + p.Fee/p.Contracts, FeePC: p.Fee / p.Contracts,
			FeeKnown: true, Filled: true, Attempted: true, PnLKnown: pnlKnown,
			Terminal: pnlKnown, ResearchOnly: researchOnly, VenueLocked: venue == "polymarket"}
		if pnlKnown {
			o.PnLPC = pnl / p.Contracts
		}
		if ct, cok := parseSystemTime(closed); cok && !ct.Before(opened) {
			o.Closed, o.ClockKnown = ct, true
		} else if !pnlKnown {
			o.ClockKnown = true
		}
		out, used = append(out, o), used+1
	}
	for _, p := range book.Open {
		add(p, 0, false, "")
	}
	for _, c := range book.Closed {
		add(c.kfPos, c.PnL, true, c.SettledTS)
	}
	return out, used, legacy
}

type nativeMLLot struct {
	Ticker, Side, SignalType, Platform string
	Price, Contracts, Fee, PnL         float64
	Opened, ClosedTS                   int64
	FillKind, FillRule, FeeSource      string
	FeeKnown                           bool
	ResetClose                         any
}

func (l *nativeMLLot) UnmarshalJSON(b []byte) error {
	var raw struct {
		Ticker     string          `json:"ticker"`
		Side       string          `json:"side"`
		SignalType string          `json:"signal_type"`
		Platform   string          `json:"platform"`
		Price      float64         `json:"price"`
		Contracts  float64         `json:"contracts"`
		Fee        float64         `json:"fee"`
		PnL        float64         `json:"pnl"`
		Opened     int64           `json:"opened"`
		ClosedTS   int64           `json:"closed_ts"`
		FillKind   string          `json:"fill_kind"`
		FillRule   string          `json:"fill_rule"`
		FeeKnown   bool            `json:"fee_known"`
		FeeSource  string          `json:"fee_source"`
		ResetClose json.RawMessage `json:"reset_close"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*l = nativeMLLot{Ticker: raw.Ticker, Side: raw.Side, SignalType: raw.SignalType,
		Platform: raw.Platform, Price: raw.Price, Contracts: raw.Contracts, Fee: raw.Fee,
		PnL: raw.PnL, Opened: raw.Opened, ClosedTS: raw.ClosedTS, FillKind: raw.FillKind,
		FillRule: raw.FillRule, FeeKnown: raw.FeeKnown, FeeSource: raw.FeeSource}
	if len(raw.ResetClose) > 0 && string(raw.ResetClose) != "null" {
		l.ResetClose = true
	}
	return nil
}

func collectMLSystemBook(path string) ([]systemRegimeObservation, systemRegimeCoverage) {
	var book struct {
		Open   []nativeMLLot `json:"open"`
		Closed []nativeMLLot `json:"closed"`
	}
	if err := readNativeJSON(path, &book); err != nil {
		state := "unavailable"
		if os.IsNotExist(err) {
			state = "collecting"
		}
		return nil, systemRegimeCoverage{Source: "ml_paper.json", State: state, Note: err.Error()}
	}
	var out []systemRegimeObservation
	legacy := 0
	add := func(l nativeMLLot, known bool) {
		kind, fam := strings.ToLower(strings.TrimSpace(l.FillKind)), strings.TrimSpace(l.SignalType)
		if (kind != "maker" && kind != "taker") || fam == "" || !l.FeeKnown || strings.TrimSpace(l.FeeSource) == "" ||
			l.Contracts <= 0 || l.Price <= 0 || l.Price >= 1 || l.Opened <= 0 || math.IsNaN(l.Fee) ||
			math.IsInf(l.Fee, 0) || l.Fee <= -l.Contracts || l.Fee >= l.Contracts {
			legacy++
			return
		}
		venue := strings.ToLower(strings.TrimSpace(l.Platform))
		if venue == "" {
			venue = "kalshi"
		}
		o := systemRegimeObservation{System: "ml-book/" + fam, Signal: fam, Venue: venue,
			Route: kind, Source: "ml_paper.json", FeeSource: strings.TrimSpace(l.FeeSource), OriginLayer: "model", Evidence: "paper_fill",
			Opened: time.Unix(l.Opened, 0).UTC(), CostPC: l.Price + l.Fee/l.Contracts,
			FeePC: l.Fee / l.Contracts, FeeKnown: true, Filled: true, Attempted: true,
			PnLKnown: known, Terminal: known, VenueLocked: venue == "polymarket"}
		if known {
			o.PnLPC = l.PnL / l.Contracts
			if l.ClosedTS > 0 {
				o.Closed, o.ClockKnown = time.Unix(l.ClosedTS, 0).UTC(), true
			}
		} else {
			o.ClockKnown = true
		}
		out = append(out, o)
	}
	for _, l := range book.Open {
		add(l, false)
	}
	for _, l := range book.Closed {
		if l.ResetClose == nil {
			add(l, true)
		}
	}
	state := "available"
	if len(out) == 0 {
		state = "collecting"
	}
	return out, systemRegimeCoverage{Source: "ml_paper.json", State: state, Rows: len(out),
		Note: fmt.Sprintf("book-native ML paper fills split by concrete originating signal family; %d legacy/non-route/fee-receipt/reset rows excluded; retained closed tail is partial history", legacy)}
}

func lotFamilyTag(p kfPos, fallback string) string {
	r := strings.TrimSpace(p.RouteReason)
	if i := strings.Index(strings.ToLower(r), "fam:"); i >= 0 {
		v := r[i+4:]
		if j := strings.IndexAny(v, " |,;"); j >= 0 {
			v = v[:j]
		}
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return fallback
}

func (s *Server) collectDedicatedSystemBooks(ctx context.Context) ([]systemRegimeObservation, []systemRegimeCoverage) {
	dir := s.cfg().DataDir
	var out []systemRegimeObservation
	var cov []systemRegimeCoverage
	addSimple := func(file, system, signal, venue string, researchOnly bool, identity kfSystemIdentity) {
		var b kfBook
		path := filepath.Join(dir, file)
		if err := readNativeJSON(path, &b); err != nil {
			state := "unavailable"
			if os.IsNotExist(err) {
				state = "collecting"
			}
			cov = append(cov, systemRegimeCoverage{Source: file, State: state, Note: err.Error()})
			return
		}
		if identity == nil {
			identity = fixedKFIdentity(system, signal, venue)
		}
		proofs, proofErr := s.fundedPaperImmutableProofs(ctx, fundedPaperBookLots(&b))
		if proofErr != nil {
			cov = append(cov, systemRegimeCoverage{Source: file, State: "unavailable",
				Note: "immutable execution proof unavailable: " + proofErr.Error()})
			return
		}
		var used, legacy int
		out, used, legacy = appendKFSystemBook(out, &b, file, identity, researchOnly, proofs)
		state := "available"
		if used == 0 {
			state = "collecting"
		}
		cov = append(cov, systemRegimeCoverage{Source: file, State: state, Rows: used,
			Note: fmt.Sprintf("actual routed paper fills with persisted fee authority and settlement; %d pre-route or fee-receipt-less rows excluded; closed array is a retained tail", legacy)})
	}
	addSimple("rawflow_book.json", "rawflow", "kalshi-flow", "kalshi", false, nil)
	addSimple("weather_book.json", "weather-point-forecast", "wxedge", "kalshi", true, nil)
	addSimple("freshinv_book.json", "freshinv", "freshlist", "kalshi", false,
		func(p kfPos) (string, string, string) {
			v := fiLotPlatform(p)
			if v == "polyus" {
				return "freshlist-polyus-follow", "freshlist", v
			}
			return "freshinv-kalshi-fade", "invert:freshlist", v
		})
	addSimple("favlong80_book.json", "favlong80", "favlong", "kalshi", false, nil)
	addSimple("xvgap_book.json", "xvgap-book", "xvgap", "polyus", false, nil)

	var kfb kflowBooks
	if err := readNativeJSON(filepath.Join(dir, "kflow_books.json"), &kfb); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "kflow_books.json", State: "unavailable", Note: err.Error()})
	} else {
		proofs, proofErr := s.fundedPaperImmutableProofs(ctx,
			fundedPaperBookLots(&kfb.Pre, &kfb.Live))
		if proofErr != nil {
			cov = append(cov, systemRegimeCoverage{Source: "kflow_books.json", State: "unavailable",
				Note: "immutable execution proof unavailable: " + proofErr.Error()})
		} else {
			before := len(out)
			var u, l int
			out, u, l = appendKFSystemBook(out, &kfb.Pre, "kflow_books.json", fixedKFIdentity("kflow-pre", "kalshi-flow", "kalshi"), true, proofs)
			used, legacy := u, l
			out, u, l = appendKFSystemBook(out, &kfb.Live, "kflow_books.json", fixedKFIdentity("kflow-live", "kalshi-flow", "kalshi"), true, proofs)
			used, legacy = used+u, legacy+l
			cov = append(cov, systemRegimeCoverage{Source: "kflow_books.json", State: map[bool]string{true: "collecting", false: "available"}[len(out) == before], Rows: used,
				Note: fmt.Sprintf("pre-event/live Kalshi-flow execution split; %d legacy instant rows excluded; retained closed tail is partial history", legacy)})
		}
	}

	var cb cbBook
	if err := readNativeJSON(filepath.Join(dir, "cheapband_book.json"), &cb); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "cheapband_book.json", State: "unavailable", Note: err.Error()})
	} else {
		id := func(defaultVenue string) kfSystemIdentity {
			return func(p kfPos) (string, string, string) {
				fam := lotFamilyTag(p, "unknown")
				return "cheapband/" + fam, fam, defaultVenue
			}
		}
		proofs, proofErr := s.fundedPaperImmutableProofs(ctx, fundedPaperBookLots(&cb.K, &cb.P))
		if proofErr != nil {
			cov = append(cov, systemRegimeCoverage{Source: "cheapband_book.json", State: "unavailable",
				Note: "immutable execution proof unavailable: " + proofErr.Error()})
		} else {
			used, legacy := 0, 0
			var u, l int
			out, u, l = appendKFSystemBook(out, &cb.K, "cheapband_book.json", id("kalshi"), false, proofs)
			used, legacy = used+u, legacy+l
			out, u, l = appendKFSystemBook(out, &cb.P, "cheapband_book.json", id("polyus"), false, proofs)
			used, legacy = used+u, legacy+l
			cov = append(cov, systemRegimeCoverage{Source: "cheapband_book.json", State: map[bool]string{true: "collecting", false: "available"}[used == 0], Rows: used,
				Note: fmt.Sprintf("cheap-price system split by concrete origin signal and venue; %d pre-route rows excluded", legacy)})
		}
	}

	var gf gfBookState
	if err := readNativeJSON(filepath.Join(dir, "genfollow_book.json"), &gf); err != nil {
		cov = append(cov, systemRegimeCoverage{Source: "genfollow_book.json", State: "unavailable", Note: err.Error()})
	} else {
		books := make([]*kfBook, 0, len(gf.Subs))
		for _, book := range gf.Subs {
			books = append(books, book)
		}
		proofs, proofErr := s.fundedPaperImmutableProofs(ctx, fundedPaperBookLots(books...))
		if proofErr != nil {
			cov = append(cov, systemRegimeCoverage{Source: "genfollow_book.json", State: "unavailable",
				Note: "immutable execution proof unavailable: " + proofErr.Error()})
		} else {
			used, legacy := 0, 0
			for key, b := range gf.Subs {
				if b == nil {
					continue
				}
				name := strings.TrimPrefix(key, "gf:")
				venue := "kalshi"
				if strings.HasSuffix(name, "-p") {
					venue, name = "polyus", strings.TrimSuffix(name, "-p")
				} else {
					name = strings.TrimSuffix(name, "-k")
				}
				var u, l int
				out, u, l = appendKFSystemBookWhere(out, b, "genfollow_book.json",
					fixedKFIdentity("follow:"+name, name, venue), false, proofs.accepts)
				used, legacy = used+u, legacy+l
			}
			cov = append(cov, systemRegimeCoverage{Source: "genfollow_book.json", State: map[bool]string{true: "collecting", false: "available"}[used == 0], Rows: used,
				Note: fmt.Sprintf("generic execution systems remain split by their concrete underlying model and venue; only immutable-ledger-joined LIVE-fill-linked Paper rows are money evidence under %s; %d legacy, no-send, zero-fill, stale, or incomplete rows excluded", fundedPaperImmutableReviewPolicyV1, legacy)})
		}
	}
	ml, mlCov := collectMLSystemBook(filepath.Join(dir, "ml_paper.json"))
	out, cov = append(out, ml...), append(cov, mlCov)
	return out, cov
}

func xvlVenueLabel(pair string) string {
	switch pair {
	case "K-PUS":
		return "kalshi+polyus"
	case "K-PINT":
		return "kalshi+polymarket"
	case "PUS-PINT":
		return "polyus+polymarket"
	default:
		return strings.ToLower(strings.ReplaceAll(pair, "-", "+"))
	}
}

func (s *Server) collectLockSystems() ([]systemRegimeObservation, []systemRegimeCoverage) {
	path := filepath.Join(s.cfg().DataDir, "xvlock_book.json")
	var b xvLockBook
	if err := readNativeJSON(path, &b); err != nil {
		state := "unavailable"
		if os.IsNotExist(err) {
			state = "collecting"
		}
		return nil, []systemRegimeCoverage{{Source: "xvlock_book.json", State: state, Note: err.Error()}}
	}
	// This reader can run before the live ledger's first post-boot flush. Reconcile the in-memory
	// copy so legacy mismatch feedback fails closed here immediately as well.
	xvlReconcileQuarantines(&b)
	var out []systemRegimeObservation
	used, incompatible, staked, feeReceiptMissing := 0, 0, 0, 0
	add := func(op xvLockOpp, known bool) {
		if !xvlProofEligible(op) {
			incompatible++
			return
		}
		opened, ok := parseSystemTime(op.FirstSeen)
		if !ok || op.AAsk <= 0 || op.BAsk <= 0 {
			return
		}
		if !op.FeeAKnown || !op.FeeBKnown || strings.TrimSpace(op.FeeASource) == "" || strings.TrimSpace(op.FeeBSource) == "" {
			feeReceiptMissing++
			return // the two fee numbers may be exact arithmetic, but lack historical schedule authority
		}
		venue := xvlVenueLabel(op.Pair)
		o := systemRegimeObservation{System: "xvlock/" + op.Pair, Signal: "cross-venue total below $1",
			Venue: venue, Route: "two-leg-taker", Source: "xvlock_book.json",
			FeeSource:   strings.TrimSpace(op.FeeASource) + "+" + strings.TrimSpace(op.FeeBSource),
			OriginLayer: "strategy",
			Evidence:    "research_quote", Opened: opened, CostPC: op.AAsk + op.BAsk + op.FeeA + op.FeeB,
			FeePC: op.FeeA + op.FeeB, FeeKnown: true, PnLKnown: known, Terminal: known,
			ResearchOnly: true, VenueLocked: strings.Contains(venue, "polymarket")}
		if op.ADepth > 0 && op.BDepth > 0 {
			o.Depth, o.DepthKnown = math.Min(op.ADepth, op.BDepth), true
		}
		if known {
			o.PnLPC = op.RealizedC / 100
			if ct, cok := parseSystemTime(op.SettledTS); cok && !ct.Before(opened) {
				o.Closed, o.ClockKnown = ct, true
			}
		} else {
			o.ClockKnown = true
		}
		out, used = append(out, o), used+1

		if op.Staked <= 0 {
			return
		}
		if !op.StakeFeesExact || !op.StakeFeeAKnown || !op.StakeFeeBKnown ||
			strings.TrimSpace(op.StakeFeeASource) == "" || strings.TrimSpace(op.StakeFeeBSource) == "" {
			feeReceiptMissing++
			return
		}
		so := o
		so.System, so.Route, so.Source = "lockstack/"+op.Pair, "paper-two-leg-taker", "xvlock_book.json/staked"
		so.FeeSource = strings.TrimSpace(op.StakeFeeASource) + "+" + strings.TrimSpace(op.StakeFeeBSource)
		so.Evidence, so.ResearchOnly, so.Attempted, so.Filled = "paper_lock", false, true, true
		so.CostPC = op.stakedCostUSD() / op.Staked
		so.FeePC = (op.StakeFeeA + op.StakeFeeB) / op.Staked
		if known {
			so.PnLPC = op.stakedNetUSD() / op.Staked
		}
		out, staked = append(out, so), staked+1
	}
	for _, op := range b.Open {
		add(op, false)
	}
	for _, op := range b.Closed {
		add(op, true)
	}
	state := "available"
	if used == 0 {
		state = "collecting"
	}
	return out, []systemRegimeCoverage{{Source: "xvlock_book.json", State: state, Rows: used + staked,
		Note: fmt.Sprintf("%d compatible first-sight two-leg quote rows plus %d paper-staked lockstack rows with per-leg fee authority; %d fee-receipt-less and %d settlement-rule-incompatible rows excluded; unstaked xvlock remains unfunded", used, staked, feeReceiptMissing, incompatible)}}
}

func (s *Server) collectComboSystems() ([]systemRegimeObservation, []systemRegimeCoverage) {
	path := filepath.Join(s.cfg().DataDir, xvcFile)
	var b xvcBook
	if err := readNativeJSON(path, &b); err != nil {
		state := "unavailable"
		if os.IsNotExist(err) {
			state = "collecting"
		}
		return nil, []systemRegimeCoverage{{Source: xvcFile, State: state, Note: "no native combo execution rows yet: " + err.Error()}}
	}
	var out []systemRegimeObservation
	feeReceiptMissing := 0
	add := func(p xvcPos, pnl float64, known bool, closed string) {
		if p.Contracts <= 0 || p.Cost <= 0 || p.Fee < 0 || math.IsNaN(p.Fee) || math.IsInf(p.Fee, 0) {
			return
		}
		if !p.FeeKnown || strings.TrimSpace(p.FeeSource) == "" {
			feeReceiptMissing++
			return // modeled/numeric fee is not an accepted combo execution receipt
		}
		opened, ok := parseSystemTime(p.TS)
		if !ok {
			return
		}
		expr := p.Expr
		if expr == "" {
			expr = xvcExprProduct
		}
		system, route, evidence, research := "combo-overlay", expr, "research_quote", true
		switch expr {
		case xvcExprSynth:
			system, route, evidence, research = "combo-synth", "synthetic-leg-stack", "paper_combo", false
		case xvcExprRFQ:
			system, route, evidence, research = "combo-rfq", "venue-rfq", "paper_combo", false
		}
		o := systemRegimeObservation{System: system, Signal: "joint probability plus executable combo quote",
			Venue: "kalshi", Route: route, Source: xvcFile, FeeSource: strings.TrimSpace(p.FeeSource), Evidence: evidence, Opened: opened,
			OriginLayer: "strategy",
			CostPC:      p.Cost + p.Fee/p.Contracts, FeePC: p.Fee / p.Contracts, FeeKnown: true,
			Filled: !research, Attempted: !research, PnLKnown: known, Terminal: known,
			ResearchOnly: research}
		if known {
			o.PnLPC = pnl / p.Contracts
			if ct, cok := parseSystemTime(closed); cok && !ct.Before(opened) {
				o.Closed, o.ClockKnown = ct, true
			}
		} else {
			o.ClockKnown = true
		}
		out = append(out, o)
	}
	for _, p := range b.Open {
		add(p, 0, false, "")
	}
	for _, c := range b.Closed {
		add(c.xvcPos, c.PnL, true, c.SettledTS)
	}
	state := "available"
	if len(out) == 0 {
		state = "collecting"
	}
	return out, []systemRegimeCoverage{{Source: xvcFile, State: state, Rows: len(out),
		Note: fmt.Sprintf("only combo rows with persisted fee authority enter Systems economics; %d numeric-only legacy/modeled rows excluded", feeReceiptMissing)}}
}

// collectLiveComboSystems is the native real-money RFQ lane. Only rows whose AcceptQuote call
// returned success and whose exact execution-time fee+source were persisted enter economics.
// Legacy fee-unknown and transport-ambiguous rows remain counted in coverage for reconciliation.
func (s *Server) collectLiveComboSystems(ctx context.Context) ([]systemRegimeObservation, []systemRegimeCoverage) {
	rows, err := s.store.ListLiveCombosAll(ctx, -1)
	if err != nil {
		return nil, []systemRegimeCoverage{{Source: "live_combos", State: "unavailable", Note: err.Error()}}
	}
	var out []systemRegimeObservation
	legacy, ambiguous, malformed := 0, 0, 0
	for _, row := range rows {
		state, _ := row["accept_state"].(string)
		if state == storage.LiveComboAcceptAmbiguous {
			ambiguous++
			continue
		}
		reportable, _ := row["fee_net_reportable"].(bool)
		feeKnown, _ := row["fee_known"].(bool)
		feeSource, _ := row["fee_source"].(string)
		if !reportable || !feeKnown || state != storage.LiveComboFilled || strings.TrimSpace(feeSource) == "" {
			legacy++
			continue
		}
		openedRaw, _ := row["ts"].(string)
		opened, ok := parseSystemTime(openedRaw)
		quote, quoteOK := row["quote"].(float64)
		contracts, contractsOK := row["contracts"].(float64)
		acceptedFee, feeOK := row["accepted_fee"].(float64)
		if !ok || !quoteOK || !contractsOK || !feeOK || quote <= 0 || quote >= 1 || contracts <= 0 ||
			acceptedFee < 0 || math.IsNaN(acceptedFee) || math.IsInf(acceptedFee, 0) {
			malformed++
			continue
		}
		settled := systemsMapInt(row, "settled") == 1
		o := systemRegimeObservation{
			System: "combo-rfq/live", Signal: "joint probability plus accepted executable combo quote",
			Venue: "kalshi", Route: "venue-rfq-live", Source: "live_combos", FeeSource: feeSource,
			OriginLayer: "strategy", Evidence: "live_fill", Opened: opened,
			CostPC: quote + acceptedFee/contracts, FeePC: acceptedFee / contracts, FeeKnown: true,
			Depth: contracts, DepthKnown: true, Filled: true, Attempted: true,
			Terminal: settled, CompleteHistory: true, ClockKnown: true,
		}
		if settled {
			realized, realizedOK := row["realized"].(float64)
			closedRaw, _ := row["settled_ts"].(string)
			closed, closedOK := parseSystemTime(closedRaw)
			if !realizedOK || !closedOK || closed.Before(opened) || math.IsNaN(realized) || math.IsInf(realized, 0) {
				malformed++
				continue
			}
			o.PnLPC, o.PnLKnown, o.Closed = realized/contracts, true, closed
		}
		out = append(out, o)
	}
	state := "available"
	if len(out) == 0 {
		state = "collecting"
	}
	return out, []systemRegimeCoverage{{Source: "live_combos", State: state, Rows: len(out),
		Note: fmt.Sprintf("only authoritative full RFQ fills with exact cumulative fee form the prospective native live route; %d pending/partial/legacy or fee-unknown, %d accept-ambiguous and %d malformed rows remain reconciliation-only", legacy, ambiguous, malformed)}}
}

type plabJournalCandidate struct {
	Kind, At, ID, Bucket, Class, Legality string
	VenueProd, FeeSynth                   float64
	NLegs                                 int
	Legs                                  []plabLeg
}

type plabJournalGrade struct {
	Kind, At, ID       string
	RealSynth, RealMVE float64
}

func collectParlayLabSystems(path string) ([]systemRegimeObservation, []systemRegimeCoverage) {
	f, err := os.Open(path)
	if err != nil {
		state := "unavailable"
		if os.IsNotExist(err) {
			state = "collecting"
		}
		return nil, []systemRegimeCoverage{{Source: "parlay_lab.jsonl", State: state, Note: err.Error()}}
	}
	defer f.Close()
	cands := map[string]plabJournalCandidate{}
	var grades []plabJournalGrade
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 2*1024*1024)
	for sc.Scan() {
		var head struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(sc.Bytes(), &head) != nil {
			continue
		}
		switch head.Kind {
		case "cand":
			var c struct {
				Kind      string    `json:"kind"`
				At        string    `json:"at"`
				ID        string    `json:"id"`
				Bucket    string    `json:"bucket"`
				Class     string    `json:"class"`
				Legality  string    `json:"legality"`
				VenueProd float64   `json:"venue_prod"`
				FeeSynth  float64   `json:"fee_synth"`
				NLegs     int       `json:"n_legs"`
				Legs      []plabLeg `json:"legs"`
			}
			if json.Unmarshal(sc.Bytes(), &c) == nil && c.ID != "" {
				cands[c.ID] = plabJournalCandidate(c)
			}
		case "grade":
			var g struct {
				Kind      string  `json:"kind"`
				At        string  `json:"at"`
				ID        string  `json:"id"`
				RealSynth float64 `json:"real_synth_per_$1"`
				RealMVE   float64 `json:"real_mve_per_$1"`
			}
			if json.Unmarshal(sc.Bytes(), &g) == nil && g.ID != "" {
				grades = append(grades, plabJournalGrade(g))
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, []systemRegimeCoverage{{Source: "parlay_lab.jsonl", State: "unavailable", Note: err.Error()}}
	}
	missingCand, feeReceiptMissing := 0, 0
	for _, g := range grades {
		c, ok := cands[g.ID]
		if !ok || c.VenueProd <= 0 || c.VenueProd >= 1 {
			missingCand++
			continue // without the candidate row we do not know its executable entry or open clock
		}
		opened, ok1 := parseSystemTime(c.At)
		closed, ok2 := parseSystemTime(g.At)
		if !ok1 || !ok2 || closed.Before(opened) {
			continue
		}
		_ = opened
		_ = closed
		_ = g
		feeReceiptMissing++
		// real_synth_per_$1 is a fee-net aggregate, but neither the fee component nor its venue
		// schedule authority is persisted. Showing that P&L in Systems would make a numeric-only
		// legacy row look execution-proven, so it remains solely in Combo Lab.
	}
	return nil, []systemRegimeCoverage{{Source: "parlay_lab.jsonl", State: "collecting", Rows: 0,
		Note: fmt.Sprintf("%d paired grades excluded because the journal lacks a persisted fee component and fee authority; %d rotated/missing candidate pairs excluded; results remain in Combo Lab only", feeReceiptMissing, missingCand)},
		{Source: "parlay_lab/mve", State: "unavailable", Note: "formula MVE fee/product is not an actual combined-market RFQ quote; no proxy is ranked"}}
}

type systemsRegimeCache struct {
	mu              sync.Mutex
	building        bool
	lastBuildFailed bool
	at              time.Time
	payload         map[string]any
	done            chan struct{}
}

var systemsRegimeCacheByServer sync.Map // *Server -> *systemsRegimeCache

const (
	systemsRegimeCacheTTL    = time.Minute
	systemsRegimeColdWait    = 250 * time.Millisecond
	systemsRegimeBuildMaxAge = 3 * time.Minute
)

type systemsRegimeBuildFunc func(context.Context) (map[string]any, any)

func systemRowIdentity(r systemRegimeRow) string {
	return strings.Join([]string{r.System, r.Signal, r.Venue, r.Route, r.Source, r.FeeSource, r.Evidence,
		r.OriginLayer, strconv.FormatBool(r.HistoryComplete)}, "\x00")
}

func systemsReviewCandidates(rows []systemRegimeRow) []map[string]any {
	by := map[string]map[string]systemRegimeRow{}
	for _, r := range rows {
		m := by[systemRowIdentity(r)]
		if m == nil {
			m = map[string]systemRegimeRow{}
			by[systemRowIdentity(r)] = m
		}
		m[r.Window] = r
	}
	var out []map[string]any
	for _, m := range by {
		cur, ok := m["30d"]
		if !ok || !cur.QualifiesForReview || cur.NetPerDayLower == nil {
			continue // never fall back optimistically from a missing/collecting 30d regime
		}
		var warnings []string
		for _, w := range []string{"7d", "90d"} {
			r, exists := m[w]
			if !exists || r.NetPerDayLower == nil {
				warnings = append(warnings, w+" lower bound unavailable")
			} else if *r.NetPerDayLower <= 0 {
				warnings = append(warnings, w+" lower bound does not clear zero")
			}
		}
		out = append(out, map[string]any{
			"system": cur.System, "signal": cur.Signal, "origin_layer": cur.OriginLayer, "venue": cur.Venue, "route": cur.Route,
			"net_per_day_lower_30d": *cur.NetPerDayLower, "net_per_day_30d": cur.NetPerDay,
			"settled_30d": cur.Settled, "utc_block_days_30d": cur.UTCBlockDays,
			"mature_utc_block_days_30d":                        cur.MatureUTCBlockDays,
			"median_visible_depth":                             cur.MedianDepth,
			"mechanical_capacity_net_per_day_at_visible_depth": cur.MechanicalCapacityNetDay,
			"decay_warnings":                                   warnings,
			"action":                                           "research review only; this endpoint cannot fund, arm, size, or place",
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i]["net_per_day_lower_30d"].(float64) > out[j]["net_per_day_lower_30d"].(float64)
	})
	return out
}

func systemsMapInt(v any, key string) int {
	m, ok := v.(map[string]any)
	if !ok {
		b, err := json.Marshal(v)
		if err != nil || json.Unmarshal(b, &m) != nil {
			return 0
		}
	}
	switch n := m[key].(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}

func (s *Server) buildSystemsRegime(ctx context.Context) map[string]any {
	asOf := time.Now().UTC()
	obs, coverage := s.collectStorageSystemObservations(ctx)
	if x, c := s.collectDedicatedSystemBooks(ctx); true {
		obs, coverage = append(obs, x...), append(coverage, c...)
	}
	if x, c := s.collectLockSystems(); true {
		obs, coverage = append(obs, x...), append(coverage, c...)
	}
	if x, c := s.collectComboSystems(); true {
		obs, coverage = append(obs, x...), append(coverage, c...)
	}
	if x, c := s.collectLiveComboSystems(ctx); true {
		obs, coverage = append(obs, x...), append(coverage, c...)
	}
	if x, c := collectParlayLabSystems(filepath.Join(s.cfg().DataDir, "parlay_lab.jsonl")); true {
		obs, coverage = append(obs, x...), append(coverage, c...)
	}

	queueN := 0
	for _, o := range obs {
		if o.QueueKnown {
			queueN++
		}
	}
	researchReport, researchErr := s.store.ResearchSystemsReport(ctx)
	if researchErr != nil {
		coverage = append(coverage, systemRegimeCoverage{Source: "microstructure-research", State: "unavailable", Note: researchErr.Error()})
		researchReport = map[string]any{"error": researchErr.Error()}
	} else {
		qSamples := systemsMapInt(researchReport["queue-priority"], "samples")
		coverage = append(coverage, systemRegimeCoverage{Source: "kalshi_queue_position", State: map[bool]string{true: "available", false: "collecting"}[qSamples > 0 || queueN > 0], Rows: qSamples + queueN,
			Note: "official account queue positions are sampled for naturally resting real orders; maker_fill_stats separately retains visible queue_ahead. Missing queue is never treated as zero."})
		incN := systemsMapInt(researchReport["incentive-maker"], "observations")
		coverage = append(coverage, systemRegimeCoverage{Source: "kalshi_incentive_programs", State: map[bool]string{true: "available", false: "collecting"}[incN > 0], Rows: incN,
			Note: "program terms and executable maker books are retained as unfunded research; reward remains excluded from EV until competition and earned-payment receipts exist"})
		lifeN := systemsMapInt(researchReport["lifecycle-reopen"], "episodes")
		coverage = append(coverage, systemRegimeCoverage{Source: "lifecycle-reopen", State: map[bool]string{true: "available", false: "collecting"}[lifeN > 0], Rows: lifeN,
			Note: "book-native reopen episodes and deterministic 5s/30s/5m/30m horizons; no settlement P&L row is fabricated"})
		basketN := systemsMapInt(researchReport["event-basket-lock"], "Observations")
		if basketN == 0 {
			basketN = systemsMapInt(researchReport["event-basket-lock"], "observations")
		}
		coverage = append(coverage, systemRegimeCoverage{Source: "event-basket-lock", State: map[bool]string{true: "available", false: "collecting"}[basketN > 0], Rows: basketN,
			Note: "mutual-exclusivity/exhaustiveness, exact fees, payout lower bound, partial-fill loss and visible capacity are stored; unfunded until settlement and fill evidence exist"})
	}
	rows := systemsRegimeRows(obs, asOf)

	// Compact concrete registry: every name here is backed by at least one native row.  It keeps
	// whale-flow, xvgap, fresh, bookskew, weather, ML and follower families identifiable instead of
	// collapsing them into generic infrastructure labels.
	type registryEnt struct {
		System, Signal, OriginLayer, Venue, Route string
	}
	regMap := map[string]registryEnt{}
	for _, o := range obs {
		e := registryEnt{o.System, o.Signal, o.OriginLayer, o.Venue, o.Route}
		regMap[strings.Join([]string{e.System, e.Signal, e.OriginLayer, e.Venue, e.Route}, "\x00")] = e
	}
	registry := make([]map[string]string, 0, len(regMap))
	for _, e := range regMap {
		registry = append(registry, map[string]string{"system": e.System, "signal": e.Signal, "origin_layer": e.OriginLayer, "venue": e.Venue, "route": e.Route})
	}
	sort.Slice(registry, func(i, j int) bool {
		return registry[i]["system"]+registry[i]["venue"]+registry[i]["route"] < registry[j]["system"]+registry[j]["venue"]+registry[j]["route"]
	})
	return map[string]any{
		"name":                    "Systems Regime Leaderboard",
		"generated_at":            asOf.Format(time.RFC3339Nano),
		"research_only":           true,
		"authority":               "read-only evidence surface; never arms, funds, sizes, promotes automatically, or places an order",
		"taxonomy":                "system is the whole trading method; signal is its trigger; model estimates or ranks; strategy chooses an action; execution_route determines maker/taker/combo/lock realization",
		"pricing":                 "native ledgers only. UnitTrial book snapshots are historical assumed-fill simulations and are VOID as profit evidence; only exchange-observed LIVE fill cohorts set profit_evidence=true. Generic signal_log entry prices are never substituted.",
		"windows":                 "7d/30d/90d point views are named UTC blocks including the current partial UTC day; all uses retained history. Point Net/day divides by exact elapsed seconds. Proof separately uses the prior 7/30/90 completed UTC days only.",
		"uncertainty":             "deterministic fixed-seed 5th/95th percentile bootstrap over completed UTC day-net blocks, including zero-opportunity days; same-day observations stay together. The first partial collection day and current partial day never enter proof. P&L is assigned to the opening-day cohort, not represented as close-day cashflow.",
		"review_gate":             "only exchange-observed profit evidence with 30 completed UTC blocks can qualify for research review, with zero open rows, at least 20 terminal one-unit outcomes, complete retained history, complete exact fee inclusion, depth>=1, and Net/day lower bound above zero; assumed-fill quotes never qualify",
		"inversion":               "direct and inverted systems need their own side-specific executable quote and settlement rows. A losing direct model does not imply a profitable inverse after the opposite spread, fees, route fill probability and capacity.",
		"maturity":                "many correlated rows from one UTC day remain one uncertainty block and cannot qualify for review regardless of raw n",
		"capacity_note":           "mechanical_capacity_net_per_day_at_visible_depth multiplies one-unit outcomes by floored displayed touch depth. It is a mechanical visible-depth ceiling, not expected fills: queue priority, overlap, nonlinear fees, impact and replenishment are not netted.",
		"rows":                    rows,
		"review_candidates_30d":   systemsReviewCandidates(rows),
		"native_system_registry":  registry,
		"coverage":                coverage,
		"microstructure_research": researchReport,
	}
}

func (s *Server) buildSystemsRegimeSafe(ctx context.Context) (payload map[string]any, panicValue any) {
	defer func() {
		if v := recover(); v != nil {
			panicValue = v
			payload = nil
		}
	}()
	return s.buildSystemsRegime(ctx), nil
}

// refreshSystemsRegimeCache runs independently of the HTTP request that noticed an expired cache.
// The endpoint is a read-only research surface, so a slow SQLite snapshot must never occupy a
// browser socket or inherit a client cancellation. One builder refreshes the immutable last-good
// payload; every concurrent request serves stale immediately. A deadline prevents a wedged query
// from keeping the cache in building state forever.
func refreshSystemsRegimeCache(c *systemsRegimeCache, build systemsRegimeBuildFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), systemsRegimeBuildMaxAge)
	defer cancel()
	p, panicValue := build(ctx)
	failed := panicValue != nil || p == nil || ctx.Err() != nil

	c.mu.Lock()
	done := c.done
	c.building = false
	c.lastBuildFailed = failed
	if !failed {
		c.payload, c.at = p, time.Now()
	}
	c.done = nil
	c.mu.Unlock()
	if done != nil {
		close(done)
	}
}

func serveSystemsRegimeCache(w http.ResponseWriter, r *http.Request, c *systemsRegimeCache, build systemsRegimeBuildFunc) {
	c.mu.Lock()
	if c.payload != nil && time.Since(c.at) < systemsRegimeCacheTTL {
		p := c.payload
		c.mu.Unlock()
		w.Header().Set("X-Systems-Regime-Cache", "fresh")
		writeJSON(w, http.StatusOK, p)
		return
	}
	p := c.payload
	if !c.building {
		c.building = true
		c.lastBuildFailed = false
		c.done = make(chan struct{})
		go refreshSystemsRegimeCache(c, build)
	}
	done := c.done
	c.mu.Unlock()

	if p != nil {
		w.Header().Set("X-Systems-Regime-Cache", "stale")
		writeJSON(w, http.StatusOK, p)
		return
	}

	// Preserve the old fast-test/cold-small-DB behavior without letting a production request own
	// the build. A genuinely cold cache gets only this bounded grace period; after that the caller
	// receives an explicit warming receipt while the same background builder continues.
	timer := time.NewTimer(systemsRegimeColdWait)
	defer timer.Stop()
	select {
	case <-done:
		c.mu.Lock()
		p, failed := c.payload, c.lastBuildFailed
		c.mu.Unlock()
		if p != nil {
			w.Header().Set("X-Systems-Regime-Cache", "fresh")
			writeJSON(w, http.StatusOK, p)
			return
		}
		state := "warming"
		if failed {
			state = "build_failed"
		}
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"state": state, "building": false, "retry_after_s": 2, "rows": []any{}})
	case <-timer.C:
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"state": "warming", "building": true, "retry_after_s": 2, "rows": []any{}})
	case <-r.Context().Done():
		return
	}
}

func (s *Server) handleSystemsRegime(w http.ResponseWriter, r *http.Request) {
	v, _ := systemsRegimeCacheByServer.LoadOrStore(s, &systemsRegimeCache{})
	serveSystemsRegimeCache(w, r, v.(*systemsRegimeCache), s.buildSystemsRegimeSafe)
}
