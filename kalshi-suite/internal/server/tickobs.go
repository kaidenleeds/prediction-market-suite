package server

// R124 D5+D6 TICK-STRUCTURE OBSERVATORY — log-only, zero placement influence.
//
// D5 (auditor r56 dive-h edge 4; measurement window ends ~Jul 19): Polymarket decimalized World
// Cup markets to 0.25¢ ticks on Jul 2 and the suite had ZERO tick-size observation while trading
// WC twins. Every sweep censuses the PolyUS snapshot: how many markets quote OFF the 1¢ grid
// (sub-cent), whether the off-grid residues sit on the 0.25¢ grid or finer, and the spread
// distribution — WC cohort vs everything else — plus a per-slug first-seen off-grid latch (the
// "tick size per traded slug" record). One NDJSON summary row per sweep → data/tick_obs.jsonl.
//
// D6 (dive-h edge 5; structures visible ~Jul 23, pilots wk of Jul 27): Kalshi sub-cent prep.
// The read/snap plumbing exists since R106/R109 (TickFor/price_ranges/SnapPx, bug 254); what was
// MISSING is rollout DETECTION: (a) kalshi.Market now decodes price_level_structure and this
// sweep latches+logs any ticker whose structure ≠ linear_cent or whose tick reads <1¢ — the
// pilot-ticker log; (b) the WS layer hands any price_level_structure*/tick_size* lifecycle frame
// RAW to tickObsStructureEvent (SetStructureHandler) so the unpublished event schema is captured
// verbatim the day it appears.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
)

const (
	tickObsFile                = "tick_obs.jsonl"
	tickObsStateFile           = "tick_obs_state.json"
	tickObsStateVersion        = 1
	tickObsPilotCap            = 2000
	tickObsPilotPerSeriesCap   = 8
	tickObsPilotRetentionDays  = 21
	tickObsSeriesRetentionDays = 180
)

// tickObsDiskState makes pilot discovery survive boots without letting rolling contracts (crypto
// in particular) permanently consume the old 2,000-ticker memory cap. Pilots is a bounded sample
// of recent tickers; Series is the stable rollout identity. The lifetime fields are deliberately
// named "latches", not "unique", because a ticker/series seen again after retention expiry is a new
// latch. That keeps the counter honest without an unbounded forever-set.
type tickObsDiskState struct {
	Version               int                           `json:"version"`
	TickerLatchesLifetime uint64                        `json:"ticker_latches_lifetime"`
	SeriesLatchesLifetime uint64                        `json:"series_latches_lifetime"`
	Pilots                map[string]tickObsPilotState  `json:"pilots"`
	Series                map[string]tickObsSeriesState `json:"series"`
}

type tickObsPilotState struct {
	Series    string `json:"series"`
	Label     string `json:"label"`
	FirstSeen int64  `json:"first_seen_unix"`
	LastSeen  int64  `json:"last_seen_unix"`
}

type tickObsSeriesState struct {
	Label        string `json:"label"`
	SampleTicker string `json:"sample_ticker"`
	FirstSeen    int64  `json:"first_seen_unix"`
	LastSeen     int64  `json:"last_seen_unix"`
}

type tickObsPilotCandidate struct {
	Ticker string
	Series string
	Label  string
	Close  int64
}

// tickObsSeriesKey derives the venue-stable series identity available on every market payload.
// Event tickers carry the dated event instance, while the prefix before '-' names the series.
func tickObsSeriesKey(eventTicker, ticker string) string {
	v := strings.ToUpper(strings.TrimSpace(eventTicker))
	if v == "" {
		v = strings.ToUpper(strings.TrimSpace(ticker))
	}
	if i := strings.IndexByte(v, '-'); i > 0 {
		v = v[:i]
	}
	return v
}

func tickObsInitState(st *tickObsDiskState) {
	st.Version = tickObsStateVersion
	if st.Pilots == nil {
		st.Pilots = make(map[string]tickObsPilotState)
	}
	if st.Series == nil {
		st.Series = make(map[string]tickObsSeriesState)
	}
}

func tickObsPruneState(st *tickObsDiskState, now time.Time) {
	tickObsInitState(st)
	pilotCut := now.Add(-tickObsPilotRetentionDays * 24 * time.Hour).Unix()
	for ticker, p := range st.Pilots {
		if p.LastSeen <= 0 || p.LastSeen < pilotCut {
			delete(st.Pilots, ticker)
		}
	}
	seriesCut := now.Add(-tickObsSeriesRetentionDays * 24 * time.Hour).Unix()
	for series, p := range st.Series {
		if p.LastSeen <= 0 || p.LastSeen < seriesCut {
			delete(st.Series, series)
		}
	}
}

func tickObsOldestPilot(st *tickObsDiskState, series string) string {
	oldTicker, oldAt := "", int64(^uint64(0)>>1)
	for ticker, p := range st.Pilots {
		if series != "" && p.Series != series {
			continue
		}
		if p.LastSeen < oldAt || (p.LastSeen == oldAt && ticker < oldTicker) {
			oldTicker, oldAt = ticker, p.LastSeen
		}
	}
	return oldTicker
}

// tickObsApplyPilotSweep updates the durable state and returns only newly latched ticker samples.
// It samples at most eight current contracts per stable series, refreshes already-retained samples
// before evicting anything, and always evicts the oldest record instead of refusing discovery at
// the global cap. Thus a rolling series can rotate its own samples but cannot block a new series.
func tickObsApplyPilotSweep(st *tickObsDiskState, candidates []tickObsPilotCandidate, now time.Time) (newPilots []tickObsPilotCandidate) {
	tickObsPruneState(st, now)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Series != candidates[j].Series {
			return candidates[i].Series < candidates[j].Series
		}
		if candidates[i].Close != candidates[j].Close {
			return candidates[i].Close > candidates[j].Close
		}
		return candidates[i].Ticker > candidates[j].Ticker
	})
	selected := make([]tickObsPilotCandidate, 0, len(candidates))
	perSeries := make(map[string]int)
	seenTicker := make(map[string]struct{})
	seriesCurrent := make(map[string]tickObsPilotCandidate)
	for _, c := range candidates {
		if c.Ticker == "" || c.Series == "" {
			continue
		}
		if _, ok := seenTicker[c.Ticker]; ok {
			continue
		}
		seenTicker[c.Ticker] = struct{}{}
		if _, ok := seriesCurrent[c.Series]; !ok {
			seriesCurrent[c.Series] = c
		}
		if perSeries[c.Series] < tickObsPilotPerSeriesCap {
			selected = append(selected, c)
			perSeries[c.Series]++
		}
	}

	nowUnix := now.Unix()
	// Refresh retained selections first so they cannot be chosen as the oldest while new samples
	// from the same sweep are admitted.
	for _, c := range selected {
		if p, ok := st.Pilots[c.Ticker]; ok {
			p.Series, p.Label, p.LastSeen = c.Series, c.Label, nowUnix
			st.Pilots[c.Ticker] = p
		}
	}
	for _, c := range selected {
		if _, ok := st.Pilots[c.Ticker]; ok {
			continue
		}
		nInSeries := 0
		for _, p := range st.Pilots {
			if p.Series == c.Series {
				nInSeries++
			}
		}
		if nInSeries >= tickObsPilotPerSeriesCap {
			delete(st.Pilots, tickObsOldestPilot(st, c.Series))
		}
		if len(st.Pilots) >= tickObsPilotCap {
			delete(st.Pilots, tickObsOldestPilot(st, ""))
		}
		st.Pilots[c.Ticker] = tickObsPilotState{Series: c.Series, Label: c.Label, FirstSeen: nowUnix, LastSeen: nowUnix}
		st.TickerLatchesLifetime++
		newPilots = append(newPilots, c)
	}
	for series, c := range seriesCurrent {
		if p, ok := st.Series[series]; ok {
			p.Label, p.SampleTicker, p.LastSeen = c.Label, c.Ticker, nowUnix
			st.Series[series] = p
		} else {
			st.Series[series] = tickObsSeriesState{Label: c.Label, SampleTicker: c.Ticker, FirstSeen: nowUnix, LastSeen: nowUnix}
			st.SeriesLatchesLifetime++
		}
	}
	return newPilots
}

func tickObsLoadState(path string) (tickObsDiskState, error) {
	var st tickObsDiskState
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		tickObsInitState(&st)
		return st, nil
	}
	if err != nil {
		return st, err
	}
	if err := json.Unmarshal(b, &st); err != nil {
		return tickObsDiskState{}, err
	}
	tickObsInitState(&st)
	return st, nil
}

func tickObsSaveState(path string, st tickObsDiskState) error {
	tickObsInitState(&st)
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// tickObsAppend writes one NDJSON row (log-only; ~6 sweep rows/h + rare latch/event rows — no
// rotation needed at this volume; revisit if pilot-week event traffic surprises).
func (s *Server) tickObsAppend(row map[string]any) {
	row["at"] = time.Now().UTC().Format(time.RFC3339)
	b, err := json.Marshal(row)
	if err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(s.cfg().DataDir, tickObsFile), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
}

// tickObsGrid classifies a dollar price against the venue grids using 1/100¢ integer math
// (bookws precedent): returns (offCent, quarterOnly, finer).
func tickObsGrid(p float64) (offCent, quarterOnly, finer bool) {
	v := math.Round(p * 10000) // 1/100¢ units
	r := math.Mod(v, 100)
	if r == 0 {
		return false, false, false
	}
	if math.Mod(v, 25) == 0 {
		return true, true, false
	}
	return true, false, true
}

// tickObsSweep runs every 10 min from MonitorPaper (immediate first tick, bounded ctx).
func (s *Server) tickObsSweep(ctx context.Context) {
	now := time.Now().UTC()
	// ── D5: PolyUS grid census, WC cohort vs rest ──
	type cohort struct {
		n, offCent, quarter, finer int
		spr                        [6]int // ≤0.25¢ · ≤0.5 · ≤1 · ≤2 · ≤5 · >5
		minSpr                     float64
	}
	wc, rest := &cohort{minSpr: math.Inf(1)}, &cohort{minSpr: math.Inf(1)}
	var newSlugRows []map[string]any
	s.tickObsMu.Lock()
	statePath := filepath.Join(s.cfg().DataDir, tickObsStateFile)
	var stateLoadErr error
	if !s.tickObsLoaded {
		s.tickObsState, stateLoadErr = tickObsLoadState(statePath)
		if stateLoadErr != nil {
			tickObsInitState(&s.tickObsState)
		}
		s.tickObsLoaded = true
	}
	if s.tickObsSlugGrid == nil {
		s.tickObsSlugGrid = map[string]string{}
	}
	for _, m := range s.polyUSSnapshot() {
		if m.Slug == "" || m.Yes <= 0 || m.Yes >= 1 {
			continue
		}
		c := rest
		isWC := strings.Contains(strings.ToLower(m.League), "wc")
		if isWC {
			c = wc
		}
		c.n++
		off, quarter, finer := false, false, false
		for _, p := range []float64{m.Yes, m.Bid, m.Ask} {
			if p <= 0 || p >= 1 {
				continue
			}
			o, q, f := tickObsGrid(p)
			off, quarter, finer = off || o, quarter || q, finer || f
		}
		switch {
		case finer:
			c.offCent, c.finer = c.offCent+1, c.finer+1
		case quarter:
			c.offCent, c.quarter = c.offCent+1, c.quarter+1
		}
		if off && isWC && len(s.tickObsSlugGrid) < 4000 {
			grid := "0.25c"
			if finer {
				grid = "sub-0.25c"
			}
			if prev, seen := s.tickObsSlugGrid[m.Slug]; !seen || (prev == "0.25c" && grid == "sub-0.25c") {
				s.tickObsSlugGrid[m.Slug] = grid
				newSlugRows = append(newSlugRows, map[string]any{"kind": "pus_offgrid_slug", "slug": m.Slug,
					"league": m.League, "grid": grid, "yes": m.Yes, "bid": m.Bid, "ask": m.Ask})
			}
		}
		if m.Ask > 0 && m.Bid > 0 && m.Ask >= m.Bid {
			sp := (m.Ask - m.Bid) * 100 // cents
			if sp < c.minSpr {
				c.minSpr = sp
			}
			switch {
			case sp <= 0.25:
				c.spr[0]++
			case sp <= 0.5:
				c.spr[1]++
			case sp <= 1:
				c.spr[2]++
			case sp <= 2:
				c.spr[3]++
			case sp <= 5:
				c.spr[4]++
			default:
				c.spr[5]++
			}
		}
	}
	// ── D6: Kalshi pilot-ticker latch (structure ≠ linear_cent, or any tick reading <1¢) ──
	var pilotCandidates []tickObsPilotCandidate
	currentTickers := make(map[string]struct{})
	currentSeries := make(map[string]struct{})
	s.metaMu.Lock()
	for tk, km := range s.kmkts {
		label := ""
		if pls := strings.ToLower(strings.TrimSpace(km.PriceLevelStructure)); pls != "" && pls != "linear_cent" {
			label = "structure=" + pls
		}
		if t := km.TickFor(0.5); t > 0 && t < 0.01 {
			if label != "" {
				label += " "
			}
			label += fmt.Sprintf("tick=%.4f", t)
		}
		if label == "" {
			continue
		}
		series := tickObsSeriesKey(km.EventTicker, tk)
		if series == "" {
			continue
		}
		closeUnix := int64(0)
		if t, err := time.Parse(time.RFC3339, km.CloseTime); err == nil {
			closeUnix = t.Unix()
		}
		pilotCandidates = append(pilotCandidates, tickObsPilotCandidate{Ticker: tk, Series: series, Label: label, Close: closeUnix})
		currentTickers[tk] = struct{}{}
		currentSeries[series] = struct{}{}
	}
	s.metaMu.Unlock()
	newPilots := tickObsApplyPilotSweep(&s.tickObsState, pilotCandidates, now)
	pilotRetained := len(s.tickObsState.Pilots)
	seriesRetained := len(s.tickObsState.Series)
	tickerLatches := s.tickObsState.TickerLatchesLifetime
	seriesLatches := s.tickObsState.SeriesLatchesLifetime
	stateSaveErr := tickObsSaveState(statePath, s.tickObsState)
	auditDue := time.Since(s.tickObsAuditAt) > 6*time.Hour
	if auditDue {
		s.tickObsAuditAt = now
	}
	s.tickObsMu.Unlock()

	sprMap := func(c *cohort) map[string]any {
		min := c.minSpr
		if math.IsInf(min, 1) {
			min = -1
		}
		return map[string]any{"n": c.n, "off_cent": c.offCent, "quarter_grid": c.quarter, "finer": c.finer,
			"spread_hist_c": map[string]int{"<=0.25": c.spr[0], "<=0.5": c.spr[1], "<=1": c.spr[2],
				"<=2": c.spr[3], "<=5": c.spr[4], ">5": c.spr[5]},
			"min_spread_c": math.Round(min*100) / 100}
	}
	s.tickObsAppend(map[string]any{"kind": "pus_grid", "wc": sprMap(wc), "rest": sprMap(rest),
		"kal_pilots_current_tickers": len(currentTickers), "kal_pilots_current_series": len(currentSeries),
		"kal_pilot_samples_retained": pilotRetained, "kal_pilot_series_retained": seriesRetained,
		"kal_pilot_ticker_latches_lifetime": tickerLatches, "kal_pilot_series_latches_lifetime": seriesLatches})
	if stateLoadErr != nil {
		s.tickObsAppend(map[string]any{"kind": "state_error", "op": "load", "error": stateLoadErr.Error()})
		_ = s.store.Audit(ctx, "warn", "tickobs", "tick observatory state could not be loaded; started a fresh bounded state", stateLoadErr.Error())
	}
	if stateSaveErr != nil {
		s.tickObsAppend(map[string]any{"kind": "state_error", "op": "save", "error": stateSaveErr.Error()})
		_ = s.store.Audit(ctx, "warn", "tickobs", "tick observatory state could not be persisted", stateSaveErr.Error())
	}
	for _, r := range newSlugRows {
		s.tickObsAppend(r)
	}
	// Every pilot latches to the jsonl; the audit trail gets the first few + a rollup (R124
	// live lesson: the FIRST sweep latched 1,040 tapered_deci_cent golf tickers at once —
	// per-ticker audit rows would have been pure spam).
	for i, p := range newPilots {
		s.tickObsAppend(map[string]any{"kind": "kal_pilot", "ticker": p.Ticker, "series": p.Series, "label": p.Label})
		if i < 5 {
			_ = s.store.Audit(ctx, "info", "tickobs",
				fmt.Sprintf("SUB-CENT PILOT latched: %s (%s; series %s) — recent ticker sample persisted", p.Ticker, p.Label, p.Series), "")
		}
	}
	if len(newPilots) > 5 {
		_ = s.store.Audit(ctx, "info", "tickobs",
			fmt.Sprintf("SUB-CENT PILOTS: +%d more ticker samples this sweep (current %d tickers / %d series; retained %d; full list in %s)", len(newPilots)-5, len(currentTickers), len(currentSeries), pilotRetained, tickObsFile), "")
	}
	if auditDue {
		_ = s.store.Audit(ctx, "info", "tickobs", fmt.Sprintf(
			"tick observatory: PUS WC n=%d off-cent %d (quarter %d, finer %d) · rest n=%d off-cent %d · kalshi pilots current %d tickers / %d series (retained %d; lifetime latches %d/%d)",
			wc.n, wc.offCent, wc.quarter, wc.finer, rest.n, rest.offCent, len(currentTickers), len(currentSeries), pilotRetained, tickerLatches, seriesLatches), "")
	}
}

// tickObsStructureEvent captures a raw price-structure lifecycle frame (SetStructureHandler).
// Log-only: verbatim NDJSON + one audit line per event type per 10 min.
func (s *Server) tickObsStructureEvent(evType string, raw []byte) {
	s.tickObsAppend(map[string]any{"kind": "kal_struct_event", "type": evType, "raw": json.RawMessage(raw)})
	s.tickObsMu.Lock()
	if s.tickObsEvAudit == nil {
		s.tickObsEvAudit = map[string]time.Time{}
	}
	due := time.Since(s.tickObsEvAudit[evType]) > 10*time.Minute
	if due {
		s.tickObsEvAudit[evType] = time.Now()
		if len(s.tickObsEvAudit) > 64 { // bounded — event-type namespace is tiny
			s.tickObsEvAudit = map[string]time.Time{evType: time.Now()}
		}
	}
	s.tickObsMu.Unlock()
	if due {
		_ = s.store.Audit(context.Background(), "info", "tickobs",
			fmt.Sprintf("price-structure lifecycle event captured: %s (raw in %s)", evType, tickObsFile), "")
	}
}

// applyKalshiPriceStructureUpdate keeps the server's warm market snapshot on the same exact grid
// the Kalshi client just accepted. It intentionally does not advance kmktsAt: a schema/lifecycle
// receipt proves the tick structure, not that the market price itself is fresh.
func (s *Server) applyKalshiPriceStructureUpdate(update kalshi.PriceStructureUpdate) {
	ticker := strings.ToUpper(strings.TrimSpace(update.Ticker))
	if ticker == "" {
		return
	}
	s.metaMu.Lock()
	defer s.metaMu.Unlock()
	market, ok := s.kmkts[ticker]
	if !ok {
		return
	}
	s.kmkts[ticker] = kalshi.ApplyPriceStructureUpdate(market, update)
}
