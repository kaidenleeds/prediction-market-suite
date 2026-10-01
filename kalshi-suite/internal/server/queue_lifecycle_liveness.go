package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	queueCollectorSource     = "Kalshi authenticated resting orders + official queue position"
	lifecycleCollectorSource = "Kalshi sequenced lifecycle events + current complete book"
)

func collectorCycleID(start time.Time) string {
	return start.UTC().Format(time.RFC3339Nano)
}

func collectorErrorClass(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "schema") || strings.Contains(msg, "json") || strings.Contains(msg, "decode"):
		return "schema"
	case strings.Contains(msg, "401") || strings.Contains(msg, "403") || strings.Contains(msg, "credential") || strings.Contains(msg, "sign"):
		return "authentication"
	case strings.Contains(msg, "timeout") || strings.Contains(msg, "deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "database") || strings.Contains(msg, "sqlite"):
		return "storage"
	default:
		return "source_api"
	}
}

type lifecycleFrameDecision struct {
	Accepted, Baseline, GenuineReopen, StateReset bool
	Classification, Exclusion                     string
}

// classify separates a raw valid lifecycle frame from a baseline, duplicate transition, accepted
// transition, and genuine reopen. The classification is reported even when no economic row is
// eligible, so a quiet/duplicate board can never look like a dead collector.
func (f *lifecycleWSFilter) classify(ev kalshi.MarketLifecycleEvent) lifecycleFrameDecision {
	ticker := strings.TrimSpace(ev.Ticker)
	kind := strings.ToLower(strings.TrimSpace(ev.EventType))
	if ticker == "" || (kind != "activated" && kind != "deactivated" && kind != "close_date_updated") {
		return lifecycleFrameDecision{Classification: "invalid", Exclusion: "invalid_lifecycle_frame"}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byTicker == nil {
		f.byTicker = make(map[string]lifecycleWSMarker)
	}
	marker, seen := f.byTicker[ticker]
	reset := false
	if !seen && len(f.byTicker) >= 100_000 {
		// Fail conservative: reset can miss one genuine transition but never fabricates one.
		f.byTicker = make(map[string]lifecycleWSMarker)
		marker, seen, reset = lifecycleWSMarker{}, false, true
	}

	decision := lifecycleFrameDecision{StateReset: reset}
	switch kind {
	case "close_date_updated":
		closeTime := strings.TrimSpace(ev.CloseTime)
		if closeTime == "" {
			decision.Classification, decision.Exclusion = "invalid", "close_date_missing"
			return decision
		}
		if !seen || marker.lastClose == "" {
			marker.lastClose = closeTime
			f.byTicker[ticker] = marker
			decision.Baseline = true
			decision.Classification, decision.Exclusion = "baseline", "first_close_date_baseline"
			return decision
		}
		if closeTime == marker.lastClose {
			f.byTicker[ticker] = marker
			decision.Classification, decision.Exclusion = "duplicate", "unchanged_close_date"
			return decision
		}
		marker.lastClose, marker.lastEvent = closeTime, kind
		f.byTicker[ticker] = marker
		decision.Accepted, decision.Classification = true, "close_date_change"
		return decision
	case "deactivated":
		if seen && marker.lastEvent == kind {
			decision.Classification, decision.Exclusion = "duplicate", "duplicate_deactivated"
			return decision
		}
		marker.lastEvent = kind
		f.byTicker[ticker] = marker
		decision.Accepted, decision.Classification = true, "deactivated_transition"
		return decision
	case "activated":
		if marker.lastEvent == "deactivated" || marker.lastEvent == "close_date_updated" {
			marker.lastEvent = kind
			f.byTicker[ticker] = marker
			decision.Accepted, decision.GenuineReopen, decision.Classification = true, true, "genuine_reopen"
			return decision
		}
		if seen && marker.lastEvent == kind {
			decision.Classification, decision.Exclusion = "duplicate", "duplicate_activated"
			return decision
		}
		marker.lastEvent = kind
		f.byTicker[ticker] = marker
		decision.Baseline = true
		decision.Classification, decision.Exclusion = "baseline", "initial_activation_without_prior_transition"
		return decision
	}
	return decision
}

type lifecycleCollectorCounts struct {
	RawFrames, BaselineFrames, AcceptedTransitions, GenuineReopens int
	TransitionInserted, TransitionDuplicates, HydratedMarkers      int
	HorizonDue, HorizonAttempts, HorizonSamples, HorizonMisses     int
	ReplayGaps, Errors                                             int
	Exclusions                                                     map[string]int
	LastErrorClass, LastErrorText                                  string
}

func (c *lifecycleCollectorCounts) addExclusion(reason string) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return
	}
	if c.Exclusions == nil {
		c.Exclusions = make(map[string]int)
	}
	c.Exclusions[reason]++
}

func (c *lifecycleCollectorCounts) addError(err error) {
	if err == nil {
		return
	}
	c.Errors++
	c.LastErrorClass, c.LastErrorText = collectorErrorClass(err), err.Error()
}

type lifecycleCollectorAccumulator struct {
	mu          sync.Mutex
	periodStart time.Time
	lastFlush   time.Time
	counts      lifecycleCollectorCounts
}

func newLifecycleCollectorAccumulator() *lifecycleCollectorAccumulator {
	return &lifecycleCollectorAccumulator{periodStart: time.Now()}
}

func (a *lifecycleCollectorAccumulator) noteHydrated(n int, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if n > 0 {
		a.counts.HydratedMarkers += n
	}
	a.counts.addError(err)
}

func (a *lifecycleCollectorAccumulator) noteFrame(d lifecycleFrameDecision) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts.RawFrames++
	if d.Baseline {
		a.counts.BaselineFrames++
	}
	if d.Accepted {
		a.counts.AcceptedTransitions++
	}
	if d.StateReset {
		a.counts.ReplayGaps++
		a.counts.addExclusion("filter_state_capacity_reset")
	}
	a.counts.addExclusion(d.Exclusion)
}

func (a *lifecycleCollectorAccumulator) noteTransition(inserted, cohort bool, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.counts.addError(err)
		return
	}
	if inserted {
		a.counts.TransitionInserted++
		if cohort {
			a.counts.GenuineReopens++
		}
	} else {
		a.counts.TransitionDuplicates++
		a.counts.addExclusion("transition_storage_duplicate")
	}
}

func (a *lifecycleCollectorAccumulator) noteQueueDrop() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.counts.ReplayGaps++
	a.counts.addExclusion("transition_worker_queue_full")
}

// noteHorizonDueAttempt records the funnel transition atomically. A fixed horizon is attempted
// as soon as the sweep decides how to handle it, including the honest terminal-missing row used
// when a restart arrives after the capture window. Recording only "due" before that branch made
// a persisted miss look like an insert with zero attempts and caused the collector receipt to be
// rejected even though the collector had done exactly the required work.
func (a *lifecycleCollectorAccumulator) noteHorizonDueAttempt() {
	a.mu.Lock()
	a.counts.HorizonDue++
	a.counts.HorizonAttempts++
	a.mu.Unlock()
}

func (a *lifecycleCollectorAccumulator) noteHorizonSample(inserted bool, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.counts.addError(err)
		return
	}
	if inserted {
		a.counts.HorizonSamples++
	} else {
		a.counts.TransitionDuplicates++
		a.counts.addExclusion("horizon_terminal_duplicate")
	}
}

func (a *lifecycleCollectorAccumulator) noteHorizonMiss(inserted bool, reason string, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		a.counts.addError(err)
		return
	}
	if inserted {
		a.counts.HorizonMisses++
	}
	a.counts.addExclusion(reason)
}

func (a *lifecycleCollectorAccumulator) noteHorizonExclusion(reason string) {
	a.mu.Lock()
	a.counts.addExclusion(reason)
	a.mu.Unlock()
}

func (a *lifecycleCollectorAccumulator) noteError(err error) {
	a.mu.Lock()
	a.counts.addError(err)
	a.mu.Unlock()
}

func (a *lifecycleCollectorAccumulator) drain(now time.Time) (time.Time, lifecycleCollectorCounts, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.lastFlush.IsZero() && now.Sub(a.lastFlush) < time.Minute {
		return time.Time{}, lifecycleCollectorCounts{}, false
	}
	start, counts := a.periodStart, a.counts
	if start.IsZero() {
		start = now
	}
	a.periodStart, a.lastFlush = now, now
	a.counts = lifecycleCollectorCounts{}
	return start, counts, true
}

func (a *lifecycleCollectorAccumulator) restore(start time.Time, c lifecycleCollectorCounts) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.periodStart.After(start) {
		a.periodStart = start
	}
	a.counts.RawFrames += c.RawFrames
	a.counts.BaselineFrames += c.BaselineFrames
	a.counts.AcceptedTransitions += c.AcceptedTransitions
	a.counts.GenuineReopens += c.GenuineReopens
	a.counts.TransitionInserted += c.TransitionInserted
	a.counts.TransitionDuplicates += c.TransitionDuplicates
	a.counts.HydratedMarkers += c.HydratedMarkers
	a.counts.HorizonDue += c.HorizonDue
	a.counts.HorizonAttempts += c.HorizonAttempts
	a.counts.HorizonSamples += c.HorizonSamples
	a.counts.HorizonMisses += c.HorizonMisses
	a.counts.ReplayGaps += c.ReplayGaps
	a.counts.Errors += c.Errors
	if c.LastErrorText != "" {
		a.counts.LastErrorClass, a.counts.LastErrorText = c.LastErrorClass, c.LastErrorText
	}
	for k, n := range c.Exclusions {
		if a.counts.Exclusions == nil {
			a.counts.Exclusions = make(map[string]int)
		}
		a.counts.Exclusions[k] += n
	}
}

func (s *Server) lifecycleAccumulator() *lifecycleCollectorAccumulator {
	s.researchMu.Lock()
	defer s.researchMu.Unlock()
	if s.lifecycleCollector == nil {
		s.lifecycleCollector = newLifecycleCollectorAccumulator()
	}
	return s.lifecycleCollector
}

func (s *Server) flushLifecycleCollector(ctx context.Context) {
	a := s.lifecycleAccumulator()
	now := time.Now()
	started, c, ok := a.drain(now)
	if !ok {
		return
	}
	status, expectedZero, zeroReason := "healthy", false, ""
	switch {
	case c.Errors > 0:
		status = "error"
	case c.ReplayGaps > 0:
		status, zeroReason = "starved", "one or more lifecycle transition frames could not be retained"
	case c.RawFrames == 0 && c.HorizonDue == 0:
		status, expectedZero, zeroReason = "healthy_empty", true, "no lifecycle frames or due fixed horizons in this minute"
	case c.AcceptedTransitions == 0 && c.HorizonDue == 0:
		status, expectedZero, zeroReason = "healthy_empty", true, "all raw lifecycle frames were explicit baselines or duplicates"
	case c.HorizonDue > 0 && c.HorizonSamples+c.HorizonMisses == 0:
		status, zeroReason = "starved", "due horizons had no fresh fee-complete executable book"
	}
	receipt := storage.CollectorReceipt{
		CollectorID: "lifecycle-reopen", CycleID: collectorCycleID(started),
		ExperimentID: "series-roll-anchor", ExperimentVersion: 1,
		Status: status, ExpectedZero: expectedZero, ZeroReason: zeroReason,
		ErrorClass: c.LastErrorClass, ErrorText: c.LastErrorText,
		Source: lifecycleCollectorSource, SchemaVersion: "kalshi-lifecycle-v1",
		Started: started, Completed: now, ExpectedCadence: time.Minute,
		Eligible:   c.RawFrames + c.HorizonDue,
		Attempted:  c.AcceptedTransitions + c.HorizonAttempts,
		Inserted:   c.TransitionInserted + c.HorizonSamples + c.HorizonMisses,
		Duplicates: c.TransitionDuplicates, ReplayGaps: c.ReplayGaps,
		Exclusions: c.Exclusions,
		Systems:    []string{"outcome-set-expansion-shock", "series-roll-anchor", "attention-spillover-graph"},
		Metrics: map[string]any{
			"raw_frames": c.RawFrames, "baseline_frames": c.BaselineFrames,
			"accepted_transitions": c.AcceptedTransitions, "genuine_reopens": c.GenuineReopens,
			"transition_rows_inserted": c.TransitionInserted,
			"hydrated_markers":         c.HydratedMarkers,
			"horizons_due":             c.HorizonDue, "horizon_attempts": c.HorizonAttempts,
			"horizon_samples": c.HorizonSamples, "horizon_misses": c.HorizonMisses,
			"sequence_available": false,
		},
	}
	if _, err := s.store.InsertCollectorReceipt(ctx, receipt); err != nil {
		a.restore(started, c)
		s.log.Warn("lifecycle-reopen collector receipt failed", "err", err)
	}
}
