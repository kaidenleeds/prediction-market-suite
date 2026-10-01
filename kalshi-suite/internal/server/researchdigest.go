package server

// R141 compact research digest.
//
// The research cockpit used to start 3-6 independent SQLite reports whenever a tab opened. A
// cold ledger made those reports compete, miss the browser's 20-second deadline, blank the page,
// and starve unrelated research receipts. This file moves that work behind one background
// singleflight. Default tabs and the phone briefing read the last-good compact snapshot; the full
// APIs remain available only when the operator explicitly expands diagnostics.

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	// The compact digest is deliberately cheap compared with the on-demand ledgers. Keep its
	// decision view current enough for an operator switching tabs, while preserving singleflight.
	// The cached Systems rows are built from the full proof-grade unit-trial report. That report
	// computes frozen event clusters, UTC-block maturity, confidence bounds, observation-time
	// costs, and depth coverage, so it is intentionally refreshed less often than a UI poll.
	// Requests still only read memory and may schedule this single background refresh.
	researchDigestFreshTTL = 60 * time.Second
	// A transient SQLite timeout must not pin an alert for the old five-minute cache window. Retry
	// in the background, but slowly enough that a busy database is never flooded.
	researchDigestRetryDelay = 15 * time.Second
	researchDigestRetryMax   = 5 * time.Minute
	researchDigestBuildLimit = 45 * time.Second
)

type gfFrozenAllocationSnapshot struct {
	Trials  []storage.UnitTrialLeaderboardStat
	AsOf    time.Time
	Day     string
	BuiltAt time.Time
}

// gfAllocationDayStart is the immutable allocation epoch boundary. Every decision during one UTC
// day uses only rows visible at that day's 00:00 boundary; a minute refresh cannot chase winners
// (or losers) that settled later that same day.
func gfAllocationDayStart(now time.Time) time.Time {
	now = now.UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

// loadGFFrozenAllocation is called only from kickResearchDigestRefresh's bounded heavy-research
// callback. UnitTrialLeaderboard applies its as-of clock to both openings and settlements, so the
// returned rows are prior-only even though the live database continues changing underneath it.
func (s *Server) loadGFFrozenAllocation(ctx context.Context, now time.Time) (gfFrozenAllocationSnapshot, error) {
	asOf := gfAllocationDayStart(now)
	snap := gfFrozenAllocationSnapshot{AsOf: asOf, Day: asOf.Format("2006-01-02")}
	rows, err := s.store.UnitTrialLeaderboard(ctx, asOf)
	if err != nil {
		return snap, err
	}
	snap.Trials = append([]storage.UnitTrialLeaderboardStat(nil), rows...)
	snap.BuiltAt = time.Now().UTC()
	return snap, nil
}

// gfFrozenAllocationCached is a strictly in-memory last-good reader. Stale-by-day is deliberate:
// if LIVE pauses heavy research through a UTC rollover, the prior valid snapshot remains safer
// than either current-day evidence or an accidental allocation reset.
func (s *Server) gfFrozenAllocationCached() (gfFrozenAllocationSnapshot, bool) {
	s.researchDigestMu.Lock()
	defer s.researchDigestMu.Unlock()
	if s.gfAllocationAsOf.IsZero() || strings.TrimSpace(s.gfAllocationDay) == "" {
		return gfFrozenAllocationSnapshot{}, false
	}
	return gfFrozenAllocationSnapshot{
		Trials:  append([]storage.UnitTrialLeaderboardStat(nil), s.gfAllocationTrials...),
		AsOf:    s.gfAllocationAsOf,
		Day:     s.gfAllocationDay,
		BuiltAt: s.gfAllocationBuiltAt,
	}, true
}

func researchDigestRetryBackoff(consecutiveFailures int) time.Duration {
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}
	delay := researchDigestRetryDelay
	for attempt := 1; attempt < consecutiveFailures; attempt++ {
		if delay >= researchDigestRetryMax/2 {
			return researchDigestRetryMax
		}
		delay *= 2
	}
	if delay > researchDigestRetryMax {
		return researchDigestRetryMax
	}
	return delay
}

// scheduleResearchDigestRetryLocked advances one bounded exponential retry. The caller holds
// researchDigestMu. A successful complete refresh resets the failure count below.
func (s *Server) scheduleResearchDigestRetryLocked(now time.Time) time.Duration {
	s.researchDigestFailures++
	delay := researchDigestRetryBackoff(s.researchDigestFailures)
	s.researchDigestNext = now.Add(delay)
	return delay
}

func researchDigestRetrySeconds(now, next time.Time) float64 {
	if remaining := next.Sub(now); remaining > 0 {
		return math.Ceil(remaining.Seconds())
	}
	return researchDigestRetryDelay.Seconds()
}

type researchDigestSystemRow struct {
	Family          string   `json:"family"`
	Venue           string   `json:"venue"`
	Side            string   `json:"side"`
	Origin          string   `json:"origin"`
	Route           string   `json:"route"`
	State           string   `json:"state"`
	N               int      `json:"n"`
	Open            int      `json:"open"`
	UniqueMarkets   int      `json:"unique_markets"`
	SettledMarkets  int      `json:"settled_unique_markets"`
	OpenMarkets     int      `json:"open_unique_markets"`
	SampleBucket    string   `json:"sample_bucket"`
	NetPerDay       float64  `json:"net_per_day"`
	NetPerDayLower  *float64 `json:"net_per_day_lower,omitempty"`
	NetPerDayUpper  *float64 `json:"net_per_day_upper,omitempty"`
	CentsPerShare   float64  `json:"cents_per_share"`
	EconomicUnit    string   `json:"economic_unit"`
	ContractMarkets int      `json:"contract_markets"`
	PaperPointOnly  bool     `json:"paper_point_only"`
	EvidenceTier    string   `json:"evidence_tier"`
	FillConditioned bool     `json:"fill_conditioned"`
	ProfitEvidence  bool     `json:"profit_evidence"`
	VoidReason      string   `json:"void_reason,omitempty"`
	LiveAuthorizes  bool     `json:"live_authorizes"`
}

type researchDigestResearchRow struct {
	SystemID              string `json:"system_id"`
	State                 string `json:"state"`
	Reason                string `json:"reason"`
	InputRows             int    `json:"input_rows"`
	Cycles                int    `json:"cycles"`
	ExactRows             int    `json:"exact_rows"`
	Candidates            int    `json:"candidates"`
	Open                  int    `json:"open"`
	Alerts                int    `json:"alerts"`
	CurrentCycleMatches   int    `json:"current_cycle_matches"`
	CurrentCycleNew       int    `json:"current_cycle_new"`
	CurrentCycleDuplicate int    `json:"current_cycle_duplicates"`
	CurrentCycleCandidate int    `json:"current_cycle_candidates"`
}

type researchDigestPayload struct {
	State       string `json:"state"`
	GeneratedAt string `json:"generated_at"`
	Plain       string `json:"plain_language"`
	// SystemCatalog is the immutable full roster. Research.Registered below is only the
	// collector/validation subset loaded into this compact digest and must not be presented as
	// the total number of trading systems.
	SystemCatalog r145SystemCatalogCounts `json:"system_catalog"`
	Status        map[string]any          `json:"status"`
	Ready         map[string]any          `json:"ready"`
	LiveArmed     bool                    `json:"live_armed"`
	LiveAuto      bool                    `json:"live_auto"`
	Systems       struct {
		Measured int                       `json:"measured"`
		Top      []researchDigestSystemRow `json:"top"`
		Worst    []researchDigestSystemRow `json:"worst"`
	} `json:"systems"`
	Research struct {
		RegistryScope       string                      `json:"registry_scope"`
		Registered          int                         `json:"registered"`
		Collecting          int                         `json:"collecting"`
		Partial             int                         `json:"partial"`
		Blocked             int                         `json:"blocked"`
		DetailsOnDemand     int                         `json:"details_on_demand"`
		Alerts              int                         `json:"alerts"`
		ExactRows           int                         `json:"exact_rows"`
		Candidates          int                         `json:"candidates"`
		Open                int                         `json:"open"`
		CurrentCycleMatches int                         `json:"current_cycle_matches"`
		Progress            []researchDigestResearchRow `json:"progress"`
		Attention           []researchDigestResearchRow `json:"attention"`
	} `json:"research"`
	Promotion storage.ResearchPromotionBridgeStatus `json:"promotion"`
	Warnings  []string                              `json:"warnings,omitempty"`
}

func handlerJSON(ctx context.Context, path string, h func(http.ResponseWriter, *http.Request)) (map[string]any, error) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	h(rec, req)
	if rec.Code < 200 || rec.Code >= 300 {
		return nil, fmt.Errorf("%s returned HTTP %d", path, rec.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	return out, nil
}

func digestTrialState(row storage.UnitTrialLeaderboardStat) string {
	rad := csRadius(row.SettledMarkets, row.SDPC)
	if row.SettledMarkets >= 20 && !math.IsInf(rad, 1) && row.MeanPC-rad > 0 {
		return "LOWER-BOUND+"
	}
	if row.SettledMarkets >= 20 && !math.IsInf(rad, 1) && row.MeanPC+rad < 0 {
		return "LOWER-BOUND-"
	}
	return "COLLECTING"
}

func compactTrial(row storage.UnitTrialLeaderboardStat) researchDigestSystemRow {
	mean := row.MeanPC
	route, economicUnit := "taker", "share"
	if row.Side == "BUNDLE" {
		route = "staged"
		economicUnit = "package_unit"
	}
	return researchDigestSystemRow{Family: row.Family, Venue: row.Platform, Side: row.Side,
		Origin: row.OriginLayer, Route: route, State: digestTrialState(row),
		N: row.SettledMarkets, Open: row.OpenMarkets,
		UniqueMarkets:  row.UniqueMarkets,
		SettledMarkets: row.SettledMarkets, OpenMarkets: row.OpenMarkets,
		SampleBucket: briefSampleBucket(row.SettledMarkets),
		NetPerDay:    row.NetPerCalendarDay, CentsPerShare: mean * 100, EconomicUnit: economicUnit,
		ContractMarkets: row.SettledMarkets, PaperPointOnly: false,
		EvidenceTier: "historical_assumed_fill_simulation_void", FillConditioned: false,
		ProfitEvidence: false, VoidReason: "no exchange order, acknowledgement, or fill was observed", LiveAuthorizes: false}
}

func compactBacktestTrial(row leaderboardBacktestRow) researchDigestSystemRow {
	out := compactTrial(row.UnitTrialLeaderboardStat)
	out.State = row.Proof
	out.NetPerDay = row.NetPerCalendarDay
	out.EvidenceTier, out.FillConditioned, out.ProfitEvidence = row.EvidenceTier, row.FillConditioned, row.ProfitEvidence
	out.VoidReason, out.LiveAuthorizes = row.VoidReason, row.LiveAuthorizes
	lo, hi := row.NetPerCalendarDayLo, row.NetPerCalendarDayHi
	out.NetPerDayLower, out.NetPerDayUpper = &lo, &hi
	return out
}

func compactResearch(row storage.ResearchSystemCollectionStat) researchDigestResearchRow {
	return researchDigestResearchRow{SystemID: row.SystemID, State: row.State, Reason: row.Reason,
		InputRows: row.InputRows, Cycles: row.InputCycles, ExactRows: row.EconomicObservations,
		Candidates: row.Candidates, Open: row.Open, Alerts: row.CollectorAlerts,
		CurrentCycleMatches: row.CurrentCycleMatches, CurrentCycleNew: row.CurrentCycleInserted,
		CurrentCycleDuplicate: row.CurrentCycleDuplicates,
		CurrentCycleCandidate: row.CurrentCycleCandidates}
}

func mergeNativeResearch(rows []storage.ResearchSystemCollectionStat, native []nativeLockCollectionView) []storage.ResearchSystemCollectionStat {
	out := append([]storage.ResearchSystemCollectionStat(nil), rows...)
	byID := make(map[string]int, len(out))
	for i := range out {
		byID[out[i].SystemID] = i
	}
	for _, n := range native {
		i, ok := byID[n.SystemID]
		if !ok {
			i = len(out)
			byID[n.SystemID] = i
			out = append(out, storage.ResearchSystemCollectionStat{SystemID: n.SystemID})
		}
		if n.CurrentCycle {
			out[i].CurrentCycleMatches = max(out[i].CurrentCycleMatches, n.CurrentCycleMatched)
			out[i].CurrentCycleInserted = max(out[i].CurrentCycleInserted, n.CurrentCycleInserted)
			out[i].CurrentCycleDuplicates = max(out[i].CurrentCycleDuplicates, n.CurrentCycleDuplicates)
			out[i].CurrentCycleCandidates = max(out[i].CurrentCycleCandidates, n.CurrentCycleCandidates)
		} else {
			// Explicit diagnostics may provide cumulative values. The compact latest-cycle path
			// above must never enter these lifetime economic/open counters.
			if n.Rows > out[i].EconomicObservations {
				out[i].EconomicObservations = n.Rows
			}
			if n.Candidates > out[i].Candidates {
				out[i].Candidates = n.Candidates
			}
			if n.Cycles > out[i].InputCycles {
				out[i].InputCycles = n.Cycles
			}
			if open := max(0, n.Rows-n.Grades); open > out[i].Open {
				out[i].Open = open
			}
			if n.Controls > out[i].Controls {
				out[i].Controls = n.Controls
			}
		}
		if n.Alerts > out[i].CollectorAlerts {
			out[i].CollectorAlerts = n.Alerts
		}
		if n.State != "" {
			out[i].State, out[i].Reason = n.State, n.Reason
		}
	}
	return out
}

func buildResearchDigest(ctx context.Context, s *Server) (researchDigestPayload,
	[]storage.UnitTrialLeaderboardStat, []storage.ResearchSystemCollectionStat, []nativeLockCollectionView) {
	var out researchDigestPayload
	out.State = "READY"
	out.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	out.SystemCatalog = r145CanonicalSystemCatalogCounts()
	out.Plain = "Profit view first: measured systems, promotion blockers, genuine collector alerts, and feed health. Full ledgers remain available on explicit expansion."
	// Unit tests and offline report tools intentionally construct a storage-only Server. Runtime
	// health is absent there, not a reason to dereference venue clients. A production Server always
	// has Kalshi configured and receives the normal status/readiness projection.
	if s.kal != nil {
		if status, err := handlerJSON(ctx, "/api/status", s.handleStatus); err == nil {
			out.Status = status
		} else {
			out.Warnings = append(out.Warnings, err.Error())
		}
		if ready, err := handlerJSON(ctx, "/api/ready", s.handleReady); err == nil {
			out.Ready = ready
		} else {
			out.Warnings = append(out.Warnings, err.Error())
		}
	}
	s.liveMu.Lock()
	out.LiveArmed, out.LiveAuto = s.liveArmed, s.liveAuto
	s.liveMu.Unlock()

	// Systems displays and promotion diagnostics consume proof/maturity/time/cost/depth fields that
	// the compact UnitTrialDigest deliberately does not calculate. Refresh the complete report here,
	// in the bounded background singleflight, then cache the complete rows below. No HTTP request
	// waits for this scan; it always receives the prior last-good snapshot.
	trials, err := s.store.UnitTrialLeaderboard(ctx, time.Now().UTC())
	if err != nil {
		out.Warnings = append(out.Warnings, "economic leaderboard: "+err.Error())
	} else {
		bundleTrials, bundleErr := s.store.ResearchRouteBundleLeaderboard(ctx, time.Now().UTC())
		if bundleErr != nil {
			out.Warnings = append(out.Warnings, "bundle economic leaderboard: "+bundleErr.Error())
		} else {
			trials = append(trials, bundleTrials...)
		}
		comboTrials, comboErr := s.store.ComboSystemLeaderboard(ctx, time.Now().UTC())
		if comboErr != nil {
			out.Warnings = append(out.Warnings, "combo economic leaderboard: "+comboErr.Error())
		} else {
			trials = append(trials, comboTrials...)
		}
		positive := make([]leaderboardBacktestRow, 0, len(trials))
		negative := make([]leaderboardBacktestRow, 0, len(trials))
		for _, row := range leaderboardBacktestRows(trials) {
			// Historical UnitTrial cents/share assumed a fill from visible depth. Keep it in the
			// detailed audit endpoint, but never advertise it as a current Top/Worst profit result.
			if !row.ProfitEvidence {
				continue
			}
			finiteCS := !math.IsInf(csRadius(row.SettledMarkets, row.SDPC), 1) &&
				!math.IsNaN(row.MeanPC) && !math.IsInf(row.MeanPC, 0) &&
				!math.IsNaN(row.SDPC) && !math.IsInf(row.SDPC, 0)
			// The compact economic cards are execution cells, not pooled signals: one tradeable
			// venue, one outcome side, one taker route, at least 20 distinct settled contracts,
			// and enough elapsed time to calculate a defensible Net/d interval.
			singleRoute := (row.Platform == "kalshi" || row.Platform == "polyus") &&
				(row.Side == "YES" || row.Side == "NO") && row.Route == "taker"
			bundleRoute := (row.Platform == "kalshi" || row.Platform == "polyus" || row.Platform == "crossvenue" || row.Platform == "mixed") &&
				row.Side == "BUNDLE" && row.Route != ""
			if (!singleRoute && !bundleRoute) ||
				row.SettledMarkets < 20 || row.TrackedSeconds < 24*60*60 || !finiteCS {
				continue
			}
			if row.MeanPC > 0 {
				positive = append(positive, row)
			} else if row.MeanPC < 0 {
				negative = append(negative, row)
			}
		}
		sort.SliceStable(positive, func(i, j int) bool {
			if positive[i].NetPerCalendarDayLo != positive[j].NetPerCalendarDayLo {
				return positive[i].NetPerCalendarDayLo > positive[j].NetPerCalendarDayLo
			}
			if positive[i].MeanPC != positive[j].MeanPC {
				return positive[i].MeanPC > positive[j].MeanPC
			}
			return positive[i].SettledMarkets > positive[j].SettledMarkets
		})
		sort.SliceStable(negative, func(i, j int) bool {
			// Most confidently negative first: conservative upper bound, not a noisy low tail.
			if negative[i].NetPerCalendarDayHi != negative[j].NetPerCalendarDayHi {
				return negative[i].NetPerCalendarDayHi < negative[j].NetPerCalendarDayHi
			}
			if negative[i].MeanPC != negative[j].MeanPC {
				return negative[i].MeanPC < negative[j].MeanPC
			}
			return negative[i].SettledMarkets > negative[j].SettledMarkets
		})
		out.Systems.Measured = len(positive) + len(negative)
		for i := 0; i < len(positive) && i < 5; i++ {
			out.Systems.Top = append(out.Systems.Top, compactBacktestTrial(positive[i]))
		}
		for i := 0; i < len(negative) && i < 3; i++ {
			out.Systems.Worst = append(out.Systems.Worst, compactBacktestTrial(negative[i]))
		}
	}

	// The compact reader touches one latest receipt plus one materialized counter row per system. It
	// exposes real exact/candidate/open counts without the full collector-lifetime aggregation that
	// previously pinned the WAL. Explicit diagnostics still own lifetime input/cycle/alert detail.
	research, researchErr := s.store.ResearchSystemCollectionStatsCompact(ctx)
	if researchErr != nil {
		out.Warnings = append(out.Warnings, "research collection summary: "+researchErr.Error())
		research = make([]storage.ResearchSystemCollectionStat, 0, len(storage.ResearchExperimentIDs()))
		for _, id := range storage.ResearchExperimentIDs() {
			research = append(research, storage.ResearchSystemCollectionStat{SystemID: id,
				State: "DETAILS_ON_DEMAND", Reason: "compact summary unavailable; full diagnostics remain explicit"})
		}
	}
	native, nativeErr := s.queryNativeLockCollectionViewsCompact(ctx)
	if nativeErr != nil {
		out.Warnings = append(out.Warnings, "native current coverage: "+nativeErr.Error())
	} else {
		research = mergeNativeResearch(research, native)
	}
	if len(research) > 0 {
		out.Research.RegistryScope = "research_hypotheses"
		out.Research.Registered = len(research)
		for _, row := range research {
			state := strings.ToUpper(row.State)
			switch {
			case strings.Contains(state, "DETAILS"):
				// Registered and intentionally not queried; do not call it blocked or collecting.
				out.Research.DetailsOnDemand++
			case strings.Contains(state, "PARTIAL"):
				out.Research.Partial++
			case strings.Contains(state, "COLLECT") || strings.Contains(state, "HEALTHY"):
				out.Research.Collecting++
			default:
				out.Research.Blocked++
			}
			out.Research.Alerts += row.CollectorAlerts
			out.Research.ExactRows += row.EconomicObservations
			out.Research.Candidates += row.Candidates
			out.Research.Open += row.Open
			out.Research.CurrentCycleMatches += row.CurrentCycleMatches
		}
		progress := append([]storage.ResearchSystemCollectionStat(nil), research...)
		sort.SliceStable(progress, func(i, j int) bool {
			if progress[i].EconomicObservations != progress[j].EconomicObservations {
				return progress[i].EconomicObservations > progress[j].EconomicObservations
			}
			if progress[i].Candidates != progress[j].Candidates {
				return progress[i].Candidates > progress[j].Candidates
			}
			if progress[i].CurrentCycleMatches != progress[j].CurrentCycleMatches {
				return progress[i].CurrentCycleMatches > progress[j].CurrentCycleMatches
			}
			return progress[i].InputRows > progress[j].InputRows
		})
		for _, row := range progress {
			if len(out.Research.Progress) >= 3 {
				break
			}
			if row.EconomicObservations > 0 || row.Candidates > 0 || row.CurrentCycleMatches > 0 {
				out.Research.Progress = append(out.Research.Progress, compactResearch(row))
			}
		}
		attention := append([]storage.ResearchSystemCollectionStat(nil), research...)
		sort.SliceStable(attention, func(i, j int) bool {
			if attention[i].CollectorAlerts != attention[j].CollectorAlerts {
				return attention[i].CollectorAlerts > attention[j].CollectorAlerts
			}
			if attention[i].EconomicObservations != attention[j].EconomicObservations {
				return attention[i].EconomicObservations < attention[j].EconomicObservations
			}
			return attention[i].SystemID < attention[j].SystemID
		})
		for _, row := range attention {
			state := strings.ToUpper(row.State)
			if row.CollectorAlerts == 0 && !strings.Contains(state, "BLOCK") &&
				!strings.Contains(state, "ERROR") && !strings.Contains(state, "START") {
				continue
			}
			out.Research.Attention = append(out.Research.Attention, compactResearch(row))
			if len(out.Research.Attention) >= 3 {
				break
			}
		}
	}
	if promotion, err := s.store.ResearchPromotionStatus(ctx); err == nil {
		out.Promotion = promotion
	} else {
		out.Warnings = append(out.Warnings, "promotion status: "+err.Error())
	}
	if len(out.Warnings) > 0 {
		out.State = "PARTIAL"
	}
	return out, trials, research, native
}

func (s *Server) kickResearchDigestRefresh() {
	allocationTarget := gfAllocationDayStart(time.Now())
	s.researchDigestMu.Lock()
	if s.researchDigestBusy || time.Now().Before(s.researchDigestNext) {
		s.researchDigestMu.Unlock()
		return
	}
	// One successful prior-only snapshot is immutable for the whole UTC day. At rollover, keep the
	// last-good rows serving until this worker can publish the next boundary.
	loadAllocation := s.gfAllocationAsOf.IsZero() || !s.gfAllocationAsOf.Equal(allocationTarget)
	s.researchDigestBusy = true
	s.researchDigestMu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), researchDigestBuildLimit)
		defer cancel()
		var payload researchDigestPayload
		var trials []storage.UnitTrialLeaderboardStat
		var research []storage.ResearchSystemCollectionStat
		var native []nativeLockCollectionView
		var encoded []byte
		var err error
		var allocation gfFrozenAllocationSnapshot
		var allocationErr error
		// The compact report joins the full one-share and systems ledgers. It is operator display,
		// never order authority, so it must share the heavy-research coordinator instead of starting
		// a free-standing 45-second reader while LIVE is armed. Last-good JSON remains available.
		ran := s.tryRunHeavyResearch(ctx, "research-digest", 0, func(workCtx context.Context) {
			if loadAllocation {
				allocation, allocationErr = s.loadGFFrozenAllocation(workCtx, allocationTarget)
			}
			payload, trials, research, native = buildResearchDigest(workCtx, s)
			encoded, err = json.Marshal(payload)
		})
		if !ran {
			delay := researchDigestRetryDelay
			if s.researchLiveActive() {
				delay = researchDigestFreshTTL
			}
			s.researchDigestMu.Lock()
			s.researchDigestBusy = false
			s.researchDigestNext = time.Now().Add(delay)
			s.researchDigestMu.Unlock()
			time.AfterFunc(delay, s.kickResearchDigestRefresh)
			return
		}
		s.researchDigestMu.Lock()
		s.researchDigestBusy = false
		if loadAllocation {
			if allocationErr != nil {
				// Do not erase the prior valid day. An absent cache stays absent, which makes the
				// follower split its existing slice equally as exploration.
				s.gfAllocationErr = allocationErr.Error()
			} else {
				s.gfAllocationTrials = append([]storage.UnitTrialLeaderboardStat(nil), allocation.Trials...)
				s.gfAllocationAsOf = allocation.AsOf
				s.gfAllocationDay = allocation.Day
				s.gfAllocationBuiltAt = allocation.BuiltAt
				s.gfAllocationErr = ""
			}
		}
		if err != nil {
			s.researchDigestErr = err.Error()
			delay := s.scheduleResearchDigestRetryLocked(time.Now())
			s.researchDigestMu.Unlock()
			time.AfterFunc(delay, s.kickResearchDigestRefresh)
			return
		}
		// A transient database timeout must not erase a useful prior snapshot. Keep serving the
		// last-good digest and mark its refresh warning; the browser adds the age. On first boot we
		// still publish the partial payload so the operator sees exactly which report is warming.
		if payload.State == "PARTIAL" && len(s.researchDigestJSON) > 0 {
			s.researchDigestErr = strings.Join(payload.Warnings, "; ")
			delay := s.scheduleResearchDigestRetryLocked(time.Now())
			s.researchDigestMu.Unlock()
			time.AfterFunc(delay, s.kickResearchDigestRefresh)
			return
		}
		s.researchDigestJSON = encoded
		s.researchDigestTrials = append([]storage.UnitTrialLeaderboardStat(nil), trials...)
		s.researchDigestSystems = append([]storage.ResearchSystemCollectionStat(nil), research...)
		s.researchDigestNative = append([]nativeLockCollectionView(nil), native...)
		s.researchDigestAt = time.Now()
		s.researchDigestErr = strings.Join(payload.Warnings, "; ")
		if payload.State == "PARTIAL" {
			delay := s.scheduleResearchDigestRetryLocked(time.Now())
			s.researchDigestMu.Unlock()
			time.AfterFunc(delay, s.kickResearchDigestRefresh)
			return
		}
		s.researchDigestFailures = 0
		s.researchDigestNext = time.Now().Add(researchDigestFreshTTL)
		s.researchDigestMu.Unlock()
	}()
}

func (s *Server) researchDigestRows() ([]storage.UnitTrialLeaderboardStat,
	[]storage.ResearchSystemCollectionStat, []nativeLockCollectionView, bool, string) {
	s.kickResearchDigestRefresh()
	return s.researchDigestRowsCached()
}

// researchDigestRowsCached is a strictly in-memory reader. Compact phone counts use it without
// starting database work; the Systems dashboard calls researchDigestRows above, which schedules
// the one bounded singleflight refresh and still returns this same last-good snapshot immediately.
func (s *Server) researchDigestRowsCached() ([]storage.UnitTrialLeaderboardStat,
	[]storage.ResearchSystemCollectionStat, []nativeLockCollectionView, bool, string) {
	s.researchDigestMu.Lock()
	defer s.researchDigestMu.Unlock()
	return append([]storage.UnitTrialLeaderboardStat(nil), s.researchDigestTrials...),
		append([]storage.ResearchSystemCollectionStat(nil), s.researchDigestSystems...),
		append([]nativeLockCollectionView(nil), s.researchDigestNative...),
		len(s.researchDigestJSON) > 0, s.researchDigestErr
}

func (s *Server) researchDigestPromotionStatus() (storage.ResearchPromotionBridgeStatus, bool) {
	s.researchDigestMu.Lock()
	b := append([]byte(nil), s.researchDigestJSON...)
	s.researchDigestMu.Unlock()
	if len(b) == 0 {
		return storage.ResearchPromotionBridgeStatus{State: "WARMING",
			Truth: "compact digest has not completed its first bounded refresh"}, false
	}
	var payload struct {
		Promotion storage.ResearchPromotionBridgeStatus `json:"promotion"`
	}
	if err := json.Unmarshal(b, &payload); err != nil {
		return storage.ResearchPromotionBridgeStatus{State: "RETRYING",
			Truth: "last-good compact digest could not be decoded"}, false
	}
	return payload.Promotion, true
}

func (s *Server) handleResearchDigest(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	s.kickResearchDigestRefresh()
	s.researchDigestMu.Lock()
	b := append([]byte(nil), s.researchDigestJSON...)
	at, next, busy, errText := s.researchDigestAt, s.researchDigestNext, s.researchDigestBusy, s.researchDigestErr
	s.researchDigestMu.Unlock()
	retrySeconds := researchDigestRetrySeconds(time.Now(), next)
	if len(b) == 0 {
		state := "WARMING"
		plain := "Building the compact research summary in the background. Core trading feeds continue independently."
		if !busy && errText == "" {
			plain = "The main data collectors are starting first. This card will retry and update automatically."
		}
		if !busy && errText != "" {
			state = "FAILING"
			plain = "The compact summary could not be built yet. A bounded background retry is scheduled; core trading feeds continue independently."
		}
		writeJSON(w, http.StatusOK, map[string]any{"state": state, "warming": busy,
			"plain_language": plain, "error": errText, "retry_seconds": retrySeconds})
		return
	}
	// Add current cache health without rebuilding any report. The stored payload remains the
	// immutable last-good value; refresh metadata tells the UI whether it is fresh or retrying.
	var payload map[string]any
	if err := json.Unmarshal(b, &payload); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"state": "FAILING", "error": err.Error()})
		return
	}
	age := time.Since(at)
	payload["age_seconds"] = math.Max(0, age.Seconds())
	payload["refreshing"] = busy
	payload["refresh_state"] = "FRESH"
	if busy {
		payload["refresh_state"] = "WARMING"
	}
	if errText != "" {
		payload["refresh_state"] = "RETRYING"
		payload["refresh_warning"] = errText
		payload["retry_seconds"] = retrySeconds
	}
	w.Header().Set("X-Research-Digest-Age", age.Round(time.Second).String())
	writeJSON(w, http.StatusOK, payload)
}
