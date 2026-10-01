// weather.go — R27 WXEDGE (LOG-ONLY): NWS forecast vs Kalshi daily-high-temperature ladders.
//
// SETTLEMENT-SOURCE-DRIVEN (operator directive 2026-07-02): "for each market sources may differ
// which can influence the needle by small amts." Kalshi's temp series do NOT all settle the same
// way — KXHIGHNY settles on the NWS Climatological Report for NYC (Central Park, site=OKX,
// issuedby=NYC), KXHIGHCHI on MIDWAY (not O'Hare), KXHIGHTNOLA on issuedby=MSY, while KXHIGHNYD
// settles on an ACCUWEATHER METAR feed and KXHIGHUS on a WPC discussion product (both probed live
// 2026-07-02). So this sweep NEVER assumes a station: it reads each series' settlement_sources[]
// URL from the venue's own series metadata, accepts ONLY forecast.weather.gov CLI products (the
// family our NWS forecast actually predicts), parses the issuing station out of the settlement
// URL, and forecasts THAT station's gridpoint. Non-CLI series are skipped entirely — modeling a
// market against a source that isn't its settling source is how small systematic misses happen.
//
// Signal: the ladder bin containing the NWS hourly-forecast high for the market's local date,
// logged as YES at the bin's implied price. Underlying = forecast high, Momentum = distance of
// the forecast from the bin center (edge-of-bin forecasts are coinflips between neighbors),
// Strength = hours to close. LOG-ONLY: Edge/Backtest grade it against the standard promotion bar.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

// wxRegistryRow is the persisted form of one station (R91: the registry used to be memory-only,
// so a restart during a venue/NWS outage blanked it for up to 12h — the r22 "wxedge dark 14h"
// failure shape). FcstURL != "" round-trips the resolved gridpoint, skipping 2 NWS calls/station.
type wxRegistryRow struct {
	Series    string `json:"series"`
	Issuedby  string `json:"issuedby"`
	StationID string `json:"station_id"`
	FcstURL   string `json:"fcst_url,omitempty"`
}

// wxSaveRegistry snapshots the station registry to data/wx_registry.json (tmp+rename; ~20 rows).
// Called at the end of every sweep — cheap, and the file always reflects the latest resolutions.
func (s *Server) wxSaveRegistry() {
	s.wxMu.Lock()
	rows := make([]wxRegistryRow, 0, len(s.wxStations))
	for _, st := range s.wxStations {
		rows = append(rows, wxRegistryRow{Series: st.Series, Issuedby: st.Issuedby, StationID: st.StationID, FcstURL: st.FcstURL})
	}
	s.wxMu.Unlock()
	if len(rows) == 0 {
		return // never clobber a good file with an empty registry (outage boot)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Series < rows[j].Series })
	// R99 bug 199 (auditor r25): marshal/write/rename failures used to vanish — a perms/disk
	// problem left the registry silently unpersisted forever. WARN once per boot (this is a
	// low-cadence path; one row per failure class is plenty).
	warnOnce := func(stage string, err error) {
		s.wxMu.Lock()
		fired := s.wxSaveWarned
		s.wxSaveWarned = true
		s.wxMu.Unlock()
		if !fired {
			_ = s.store.Audit(context.Background(), "warn", "wxedge", "registry persist FAILED ("+stage+") — wx_registry.json not written (bug 199)", err.Error())
		}
	}
	blob, err := json.MarshalIndent(rows, "", " ")
	if err != nil {
		warnOnce("marshal", err)
		return
	}
	tmp := filepath.Join(s.cfg().DataDir, "wx_registry.json.tmp")
	if err := os.WriteFile(tmp, blob, 0o644); err != nil {
		warnOnce("write", err)
		return
	}
	if err := os.Rename(tmp, filepath.Join(s.cfg().DataDir, "wx_registry.json")); err != nil {
		warnOnce("rename", err)
	}
}

// wxLoadRegistry seeds an EMPTY registry from the last persisted snapshot. Persisted rows never
// override live venue truth — wxRefreshSeries still runs on its own cadence and replaces entries
// whose settlement URLs changed; this only bridges the boot-during-outage gap.
func (s *Server) wxLoadRegistry() {
	var rows []wxRegistryRow
	if !s.readJSONLoose(filepath.Join(s.cfg().DataDir, "wx_registry.json"), &rows) || len(rows) == 0 {
		return
	}
	n := 0
	s.wxMu.Lock()
	if s.wxStations == nil {
		s.wxStations = map[string]*wxStation{}
	}
	for _, r := range rows {
		if r.Series == "" || r.Issuedby == "" {
			continue
		}
		if _, ok := s.wxStations[r.Series]; ok {
			continue
		}
		s.wxStations[r.Series] = &wxStation{Series: r.Series, Issuedby: r.Issuedby, StationID: r.StationID,
			FcstURL: r.FcstURL, resolved: r.FcstURL != ""}
		n++
	}
	s.wxMu.Unlock()
	if n > 0 {
		_ = s.store.Audit(context.Background(), "info", "wxedge",
			fmt.Sprintf("registry loaded from disk: %d stations (R91 persist — restart no longer blanks wxedge)", n), "")
	}
}

// wxStation is one temperature series + the station its settlement source names.
type wxStation struct {
	Series    string // Kalshi series ticker (KXHIGHNY, KXHIGHTSEA, …)
	Issuedby  string // CLI issuing station code from the settlement URL (NYC, MDW, MSY, …)
	StationID string // NWS station id ("K"+issuedby: KNYC, KMDW, …)
	FcstURL   string // gridpoint hourly-forecast URL (resolved via /stations → /points)
	resolved  bool   // station → gridpoint resolution done
	failed    bool   // resolution failed (audit-logged once; retried next series refresh)
	periods   []wxPeriod
	periodsAt time.Time
	// A failed refresh may briefly use the last good forecast, but never indefinitely. This latch
	// keeps a multi-station NWS outage from writing the same stale warning every sweep.
	forecastStaleWarned bool
}

type wxPeriod struct {
	start time.Time
	tempF float64
}

var (
	wxIssuedbyRe = regexp.MustCompile(`issuedby=([A-Za-z0-9]{3,4})`)
	wxNumRe      = regexp.MustCompile(`[-+]?(?:\d+(?:\.\d*)?|\.\d+)`)
	// ticker date segment: KXHIGHNY-26JUL03-B98.5 → "26JUL03"
	wxDateRe = regexp.MustCompile(`-(\d{2})([A-Z]{3})(\d{2})-`)
	wxMonths = map[string]time.Month{"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
)

const wxForecastMaxStale = 90 * time.Minute

// wxSettlementStation reads the complete settlement-source vector. A forecast is comparable only
// when every listed source is an NWS CLI product for the same issuing station. Picking element 0
// can silently model the wrong needle when a series has multiple authorities or fallback rules.
func wxSettlementStation(urls []string) (string, bool) {
	station := ""
	if len(urls) == 0 {
		return "", false
	}
	for _, raw := range urls {
		u := strings.TrimSpace(raw)
		if !strings.Contains(u, "forecast.weather.gov") || !strings.Contains(u, "product=CLI") {
			return "", false
		}
		m := wxIssuedbyRe.FindStringSubmatch(u)
		if m == nil {
			return "", false
		}
		iss := strings.ToUpper(m[1])
		if station == "" {
			station = iss
		} else if station != iss {
			return "", false
		}
	}
	return station, station != ""
}

// wxGetJSON fetches an api.weather.gov resource (requires a User-Agent per NWS policy).
func wxGetJSON(ctx context.Context, url string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "kalshi-suite/1.0 (research)")
	req.Header.Set("Accept", "application/geo+json")
	resp, err := spotHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("nws %s: http %d", url, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// wxSweep is called from the autoTick discovery block; it self-throttles to 30 min and runs the
// real work detached so the tick never blocks on ~20 series × (forecast fetch + market pull).
func (s *Server) wxSweep(ctx context.Context) {
	s.wxMu.Lock()
	if s.wxBusy || time.Since(s.wxSweepAt) < 30*time.Minute {
		s.wxMu.Unlock()
		return
	}
	s.wxSweepAt = time.Now()
	s.wxBusy = true
	s.wxMu.Unlock()
	go func() {
		defer func() {
			s.wxMu.Lock()
			s.wxBusy = false
			s.wxMu.Unlock()
		}()
		// R69 (audit P1): this detached goroutine does NWS HTTP + parsing with no recover — one
		// panic was a process kill (live orders resting). R89 (auditor bug 72): the old
		// `defer s.runGuarded(name, func(){})` idiom was a NO-OP (nested recover never fires);
		// recoverGuard recovers for real. The busy release above stays deferred either way.
		defer s.recoverGuard("wx-sweep")
		defer s.wxSaveRegistry() // R91: persist even when the sweep times out mid-scan (fresh resolutions kept)
		// R99 bug 191 (auditor r25, family dark 25h+ / 3rd run carried): ctx here is the 5s
		// discovery-tick context — autoTick cancel()s it microseconds after this goroutine spawns,
		// so every call below died "context canceled" before doing any work (since R83/84,
		// 5b7fd11): the registry could never populate, wx_registry.json could never exist (empty
		// registries are refused), zero wxedge signals were possible. The sweep is deliberately
		// DETACHED — its lifetime must not chain to the tick that merely scheduled it.
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
		defer cancel()
		s.wxResearchCollectorBegin(time.Now())
		defer s.wxResearchCollectorFinish(wctx)
		s.wxMu.Lock()
		empty := len(s.wxStations) == 0
		s.wxMu.Unlock()
		if empty {
			s.wxLoadRegistry() // R91: bridge a boot-during-outage with the persisted registry
		}
		s.wxMu.Lock()
		needList := time.Since(s.wxListAt) > 12*time.Hour || len(s.wxStations) == 0
		s.wxMu.Unlock()
		if needList {
			s.wxRefreshSeries(wctx)
		}
		s.wxMu.Lock()
		sts := make([]*wxStation, 0, len(s.wxStations))
		for _, st := range s.wxStations {
			sts = append(sts, st)
		}
		s.wxMu.Unlock()
		s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Stations = len(sts) })
		// R143: the old serial scan could spend the entire three-minute cycle on a handful of slow
		// NWS/Kalshi calls, then cancel the remaining stations and report the Forecast Graph
		// collector as broken. Each station is independent and the clients already enforce their
		// venue rate limits, so use a small bounded worker pool. Four workers finish the complete
		// registry without creating an unbounded network or CPU flood.
		workers := 4
		if len(sts) < workers {
			workers = len(sts)
		}
		jobs := make(chan *wxStation)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for st := range jobs {
					if wctx.Err() != nil {
						return
					}
					s.wxScanSeries(wctx, st)
				}
			}()
		}
		for _, st := range sts {
			select {
			case jobs <- st:
			case <-wctx.Done():
				close(jobs)
				wg.Wait()
				return
			}
		}
		close(jobs)
		wg.Wait()
		// R106 (auditor r33 P4 batch, "wxedge ALWAYS-EMIT heartbeat"): the auditor's log-grep for
		// sweeps proved unreliable (r31: a completed boot sweep looked missed) — one audit row per
		// COMPLETED sweep makes the cadence observable regardless of whether any signal fired.
		_ = s.store.Audit(wctx, "info", "wxedge",
			fmt.Sprintf("wxedge sweep heartbeat: %d stations scanned", len(sts)), "")
	}()
}

// wxRefreshSeries pulls the Climate-and-Weather series list and (re)builds the station registry
// from each series' OWN settlement source. Only forecast.weather.gov CLI products qualify.
func (s *Server) wxRefreshSeries(ctx context.Context) {
	list, err := s.kal.SeriesByCategory(ctx, "Climate and Weather")
	if err != nil {
		// R90 bug 120 (auditor DO-THIS 9): this silent return was how wxedge went 14h dark with
		// zero trace — every failure path in the registry rebuild now leaves an audit row.
		// Cadence is naturally bounded (≥12h between rebuild attempts), so no throttle needed.
		_ = s.store.Audit(context.Background(), "warn", "wxedge", "registry refresh FAILED (SeriesByCategory) — retrying next sweep (bug 120)", err.Error())
		return // transient; next sweep retries (wxListAt not advanced)
	}
	found := 0
	s.wxMu.Lock()
	if s.wxStations == nil {
		s.wxStations = map[string]*wxStation{}
	}
	s.wxMu.Unlock()
	for _, sr := range list {
		if !strings.HasPrefix(sr.Ticker, "KXHIGH") || len(sr.SettlementSources) == 0 {
			continue
		}
		urls := make([]string, 0, len(sr.SettlementSources))
		for _, src := range sr.SettlementSources {
			urls = append(urls, src.URL)
		}
		// THE SOURCE GATE: the complete source vector must describe one NWS CLI needle. Mixed
		// authority, mixed station, AccuWeather, and WPC series remain deliberately unmodeled.
		iss, ok := wxSettlementStation(urls)
		if !ok {
			continue
		}
		s.wxMu.Lock()
		if st, ok := s.wxStations[sr.Ticker]; !ok || st.Issuedby != iss {
			s.wxStations[sr.Ticker] = &wxStation{Series: sr.Ticker, Issuedby: iss, StationID: "K" + iss}
		}
		s.wxMu.Unlock()
		found++
	}
	if found > 0 {
		s.wxMu.Lock()
		s.wxListAt = time.Now()
		s.wxMu.Unlock()
		_ = s.store.Audit(context.Background(), "info", "wxedge", fmt.Sprintf("series registry refreshed: %d CLI-settled temp series", found), "")
	} else {
		// R90 bug 120: ZERO stations out of a successful list pull = the category was renamed/
		// emptied or every settlement URL changed shape — the exact dark-family mode that went
		// uninstrumented for 14h at r22. WARN so the famine trail has a cause, not just an absence.
		_ = s.store.Audit(context.Background(), "warn", "wxedge",
			fmt.Sprintf("registry refresh found ZERO CLI-settled series (list had %d rows) — category renamed/empty or settlement URLs changed (bug 120)", len(list)), "")
	}
}

// wxResolve turns a settlement station into its NWS gridpoint hourly-forecast URL:
// /stations/{K···} → coordinates → /points/{lat},{lon} → forecastHourly.
func (s *Server) wxResolve(ctx context.Context, st *wxStation) bool {
	if st.resolved {
		return true
	}
	var stn struct {
		Geometry struct {
			Coordinates []float64 `json:"coordinates"` // [lon, lat]
		} `json:"geometry"`
	}
	if err := wxGetJSON(ctx, "https://api.weather.gov/stations/"+st.StationID, &stn); err != nil || len(stn.Geometry.Coordinates) < 2 {
		if !st.failed {
			st.failed = true
			_ = s.store.Audit(context.Background(), "warn", "wxedge", fmt.Sprintf("station resolve failed for %s (%s): %v", st.Series, st.StationID, err), "")
		}
		return false
	}
	lon, lat := stn.Geometry.Coordinates[0], stn.Geometry.Coordinates[1]
	var pts struct {
		Properties struct {
			ForecastHourly string `json:"forecastHourly"`
		} `json:"properties"`
	}
	if err := wxGetJSON(ctx, fmt.Sprintf("https://api.weather.gov/points/%.4f,%.4f", lat, lon), &pts); err != nil || pts.Properties.ForecastHourly == "" {
		return false
	}
	st.FcstURL, st.resolved, st.failed = pts.Properties.ForecastHourly, true, false
	return true
}

// wxForecast returns the station's hourly forecast periods (cached 30 min).
func (s *Server) wxForecast(ctx context.Context, st *wxStation) []wxPeriod {
	if time.Since(st.periodsAt) < 30*time.Minute && len(st.periods) > 0 {
		return st.periods
	}
	if !s.wxResolve(ctx, st) {
		return nil
	}
	var fc struct {
		Properties struct {
			Periods []struct {
				StartTime       string  `json:"startTime"`
				Temperature     float64 `json:"temperature"`
				TemperatureUnit string  `json:"temperatureUnit"`
			} `json:"periods"`
		} `json:"properties"`
	}
	if err := wxGetJSON(ctx, st.FcstURL, &fc); err != nil {
		if len(st.periods) > 0 && !st.periodsAt.IsZero() && time.Since(st.periodsAt) <= wxForecastMaxStale {
			return st.periods // bounded continuity through a short NWS fault
		}
		if !st.forecastStaleWarned {
			st.forecastStaleWarned = true
			_ = s.store.Audit(context.Background(), "warn", "wxedge",
				fmt.Sprintf("forecast refresh failed for %s and cached forecast is older than %s — signal suppressed", st.Series, wxForecastMaxStale), err.Error())
		}
		return nil
	}
	out := make([]wxPeriod, 0, len(fc.Properties.Periods))
	for _, p := range fc.Properties.Periods {
		t, err := time.Parse(time.RFC3339, p.StartTime) // carries the station's LOCAL offset
		if err != nil {
			continue
		}
		f := p.Temperature
		if strings.EqualFold(p.TemperatureUnit, "C") {
			f = f*9/5 + 32
		}
		out = append(out, wxPeriod{start: t, tempF: f})
	}
	if len(out) > 0 {
		st.periods, st.periodsAt = out, time.Now()
		st.forecastStaleWarned = false
	}
	return st.periods
}

// wxHighFor returns the forecast daily high for a local date ("2006-01-02" in the station's own
// offset — NWS period timestamps carry it, so no tz database is needed).
func wxHighFor(periods []wxPeriod, localDate string) (float64, bool) {
	hi, any := -999.0, false
	for _, p := range periods {
		if p.start.Format("2006-01-02") != localDate {
			continue
		}
		any = true
		if p.tempF > hi {
			hi = p.tempF
		}
	}
	return hi, any
}

// wxParseBin extracts a market's temperature bin from its yes_sub_title:
// "98° to 99°" → [98,99] · "97° or below" → (-∞,97] · "106° or above" → [106,∞).
// Subtitle-driven because tail DIRECTION isn't derivable from the ticker (probed: T98 = "97° or
// below" but T105 = "106° or above").
func wxParseBin(sub string) (lo, hi float64, ok bool) {
	nums := wxNumRe.FindAllString(sub, -1)
	if len(nums) == 0 {
		return 0, 0, false
	}
	v0, _ := strconv.ParseFloat(nums[0], 64)
	ls := strings.ToLower(sub)
	switch {
	case strings.Contains(ls, "below"):
		return -999, v0, true
	case strings.Contains(ls, "above"):
		return v0, 999, true
	case len(nums) >= 2:
		v1, _ := strconv.ParseFloat(nums[1], 64)
		if v1 < v0 {
			v0, v1 = v1, v0
		}
		return v0, v1, true
	}
	return 0, 0, false
}

// wxFindBin picks the ladder bin whose [lo,hi] covers the rounded forecast high — the SUBMARKET↔
// QUANTITY pairing itself, split out of wxScanSeries so the R69 "strike"-family telemetry (and its
// test) run without feeds. Same scan order and outcome as the pre-R69 inline loop: the FIRST bin
// covering fhi wins. Telemetry: every unparseable subtitle rings with the RAW ticker
// ("unparseable-bin"); a ladder whose bins never cover the forecast rings once per series+date+value
// ("no-bin-covering-value", forecast in the slug column); the covering bin counts a hit.
func wxFindBin(series, date string, mkts []kalshi.Market, idxs []int, fhi float64) (mi int, lo, hi float64, ok bool) {
	// R100 (auditor bug 213): family "strike-wxbin" — was "strike", which pooled the temp-ladder
	// bin picks with kthresh's per-rung strike parses, so /api/matchlog's "strike" rate tracked
	// whichever pipeline was busiest.
	for _, i := range idxs {
		m := mkts[i]
		blo, bhi, okB := wxParseBin(m.YesSubTitle)
		if !okB {
			matchTelemetry("strike-wxbin", m.Ticker, "", false, m.Ticker, "unparseable-bin")
			continue
		}
		if fhi < blo || fhi > bhi {
			continue
		}
		matchTelemetry("strike-wxbin", m.Ticker, "", true, m.Ticker, "")
		return i, blo, bhi, true
	}
	matchTelemetry("strike-wxbin", series+" "+date, "", false, fmt.Sprintf("high=%.0f", fhi), "no-bin-covering-value")
	return 0, 0, 0, false
}

// wxScanSeries logs the forecast-bin signal for one series' open ladders (≤1 per series+date / 2h).
func (s *Server) wxScanSeries(ctx context.Context, st *wxStation) {
	s.wxResearchCollectorUpdate(func(c *wxResearchCollectorCycle) { c.Series++ })
	periods := s.wxForecast(ctx, st)
	if len(periods) == 0 {
		return
	}
	forecastObservedAt := st.periodsAt
	mkts, err := s.kal.MarketsBySeries(ctx, st.Series)
	if err != nil {
		// R99 bug 198 (auditor r25): the SCAN half swallowed venue errors bare-return — the exact
		// dark-mode class bug 120 fixed for the refresh half. Same WARN treatment (cadence is
		// bounded by the 30-min sweep throttle, so no extra throttle needed).
		_ = s.store.Audit(context.Background(), "warn", "wxedge", "series scan FAILED (MarketsBySeries "+st.Series+") — no rows this sweep (bug 198)", err.Error())
		return
	}
	if len(mkts) == 0 {
		return
	}
	// Group the ladder by event date parsed from the ticker (26JUL03 → 2026-07-03, station-local).
	byDate := map[string][]int{}
	for i, m := range mkts {
		dm := wxDateRe.FindStringSubmatch(m.Ticker)
		if dm == nil {
			continue
		}
		mon, okM := wxMonths[dm[2]]
		if !okM {
			continue
		}
		d, _ := strconv.Atoi(dm[3])
		y, _ := strconv.Atoi(dm[1])
		date := fmt.Sprintf("20%02d-%02d-%02d", y, int(mon), d)
		byDate[date] = append(byDate[date], i)
	}
	// R135c: two strictly unfunded, MARKET-DERIVED curve-consistency systems share this already
	// source-verified settlement-station registry and market pull. The producer performs at most two
	// bounded Forecast Graph reads per series, uses fresh exact WS books/fees, and writes no positions.
	// R139: the independently scheduled NOAA NBM adapter shares only this already-fetched market
	// ladder and cached NWS periods. It makes no request here and refuses any station/date mismatch.
	s.wxNBMObserveSeries(ctx, st, mkts, byDate, periods)
	s.wxResearchObserveSeries(ctx, st, mkts, byDate, periods)
	for date, idxs := range byDate {
		fh, okH := wxHighFor(periods, date)
		if !okH {
			continue // beyond the hourly horizon (~6 days) or a date with no periods
		}
		key := st.Series + "|" + date
		s.wxMu.Lock()
		if s.wxSigAt == nil {
			s.wxSigAt = map[string]time.Time{}
		}
		if t, ok := s.wxSigAt[key]; ok && time.Since(t) < 2*time.Hour {
			s.wxMu.Unlock()
			continue
		}
		s.wxSigAt[key] = time.Now()
		pruneTimeMap(s.wxSigAt, func(t time.Time) time.Time { return t }, 500, 72*time.Hour)
		s.wxMu.Unlock()
		fhi := math.Round(fh) // the CLI report settles on an integer °F high
		// R69: bin pick + "strike" telemetry via wxFindBin (behavior-identical: first covering bin,
		// px-extreme ladder still skipped — the pairing succeeded, the book just isn't quoted yet).
		bi, lo, hi, okBin := wxFindBin(st.Series, date, mkts, idxs, fhi)
		if !okBin {
			continue
		}
		m := mkts[bi]
		px := m.ImpliedProbability()
		if px <= 0.03 || px >= 0.97 {
			continue // ladder not really quoted yet (probed: next-day ladders open with empty books)
		}
		mid := (lo + hi) / 2
		if lo <= -999 {
			mid = hi // tail bins: distance measured from the boundary
		} else if hi >= 999 {
			mid = lo
		}
		hrs := 0.0
		if ct, err := time.Parse(time.RFC3339, m.CloseTime); err == nil {
			hrs = time.Until(ct).Hours()
		}
		_, rh, _ := s.kalSigMeta(ctx, m.Ticker)
		if rh == 0 {
			rh = hrs
		}
		if err := s.insertSignal(ctx, storage.Signal{Platform: "kalshi", Ticker: m.Ticker, Title: m.Title + " (" + m.YesSubTitle + ")", Side: "YES", SignalType: "wxedge", EntryPrice: px, Underlying: fh, Momentum: fh - mid, Strength: hrs, ResolveHours: rh, Confidence: s.signalConfidence("wxedge", px), ExecExpr: r147InputReceiptExpr("K-NOAA", forecastObservedAt)}); err != nil {
			// R99 bug 196 (latch-after-success): a failed insert must not burn the 2h series+date
			// throttle — clear the latch so the NEXT sweep retries, instead of one transient BUSY
			// silencing this station for a whole window (the family's slowest-cadence failure mode).
			s.wxMu.Lock()
			delete(s.wxSigAt, key)
			s.wxMu.Unlock()
		} else {
			// R106 WEATHER BOOK (operator ask): the SAME just-logged row feeds the Weather paper
			// book (rawFlowPlace pattern — wxedge is log-only elsewhere, the producer is the only
			// source). All rules live in weatherBookPlace (band/one-lot/hedge/Kelly/stop).
			s.weatherBookPlace(ctx, m.Ticker, m.Title+" ("+m.YesSubTitle+")", "YES", px, rh)
		}
	}
	s.weatherBookFlush() // one write per scan, not per lot (rawflow convention)
}
