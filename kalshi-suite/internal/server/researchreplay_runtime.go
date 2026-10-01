package server

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchreplay"
)

const (
	researchReplayCaptureCadence = 5 * time.Minute
	researchReplayFlushGrace     = 5 * time.Second
)

type researchReplayRuntime struct {
	sync.Mutex
	lastCapture time.Time
	lastFlush   time.Time
	writer      *researchreplay.Writer
	openError   string
}

var researchReplayRuntimes sync.Map // map[*Server]*researchReplayRuntime

func researchReplayRuntimeFor(s *Server) *researchReplayRuntime {
	if value, ok := researchReplayRuntimes.Load(s); ok {
		return value.(*researchReplayRuntime)
	}
	created := &researchReplayRuntime{}
	value, _ := researchReplayRuntimes.LoadOrStore(s, created)
	return value.(*researchReplayRuntime)
}

func researchReplayCaptureDue(s *Server, now time.Time) bool {
	state := researchReplayRuntimeFor(s)
	state.Lock()
	defer state.Unlock()
	if !state.lastCapture.IsZero() && now.Sub(state.lastCapture) < researchReplayCaptureCadence {
		return false
	}
	state.lastCapture = now
	return true
}

func (s *Server) researchReplayWriter() (*researchreplay.Writer, error) {
	state := researchReplayRuntimeFor(s)
	state.Lock()
	defer state.Unlock()
	if state.writer != nil {
		return state.writer, nil
	}
	w, err := researchreplay.Open(researchreplay.Config{
		Dir: filepath.Join(s.cfg().DataDir, "research-replay"), MaxSegments: 4032,
		MaxBytes: 2 << 30, MaxBatchFrames: 120, MaxBatchBytes: 8 << 20, MaxFrameBytes: 1 << 20,
	})
	if err != nil {
		state.openError = err.Error()
		return nil, err
	}
	state.writer, state.openError = w, ""
	return w, nil
}

// flushResearchReplayDurably gives a queued replay batch one fresh, bounded durability window.
// Heavy-research cancellation must stop report reads promptly, but it must not leave the writer's
// in-memory batch permanently full: the next capture would otherwise fail QueueBatch before it
// could make any progress. context.WithoutCancel is bounded by the short grace deadline.
func flushResearchReplayDurably(parent context.Context, w *researchreplay.Writer) (researchreplay.SegmentMeta, bool, error) {
	ctx, cancel := researchDurabilityContextWithGrace(parent, researchReplayFlushGrace)
	defer cancel()
	return w.Flush(ctx)
}

// captureResearchReplay persists selected governance/evidence state only. It never records raw
// ticks, headers, credentials, account payloads, or per-book floods. The replay package performs a
// second recursive secret scan and hash-chains the immutable compressed segments.
func (s *Server) captureResearchReplay(ctx context.Context) {
	w, err := s.researchReplayWriter()
	if err != nil {
		if s.log != nil {
			s.log.Warn("research replay open failed", "err", err)
		}
		return
	}
	type reportFrame struct {
		kind, entity, selection string
		payload                 any
	}
	var reports []reportFrame
	if ctx.Err() != nil {
		return
	}
	if foundation, err := s.store.ResearchFoundationReport(ctx); err == nil {
		reports = append(reports, reportFrame{"foundation", "canonical-research-foundation",
			"scheduled immutable checkpoint for identity/registry replay", foundation})
	}
	if collectors, err := s.store.CollectorLivenessReport(ctx); err == nil {
		reports = append(reports, reportFrame{"collector-liveness", "research-collectors",
			"scheduled immutable checkpoint for exclusion/error/zero-row replay", collectors})
	}
	if ctx.Err() != nil {
		return
	}
	if clocks, err := s.store.SourceClockReport(ctx); err == nil {
		reports = append(reports, reportFrame{"source-clocks", "research-source-clocks",
			"scheduled immutable checkpoint for source-time/schema-gap replay", clocks})
	}
	if systems, err := s.store.ResearchSystemsReport(ctx); err == nil {
		reports = append(reports, reportFrame{"system-evidence", "research-systems",
			"scheduled immutable checkpoint for research-system evidence replay", systems})
	}
	if ctx.Err() != nil {
		return
	}
	// The replay contract is wider than market frames: a future audit must be able to reconstruct
	// why an executable route was accepted/rejected, which statistical gate existed at that time,
	// and whether governance/portfolio state could possibly confer authority.  These are bounded
	// reports over append-only tables, so adding them here does not add subscriptions or raw-tick
	// volume and cannot change Paper/LIVE behavior.
	if routes, err := s.store.ResearchRouteReport(ctx, 100); err == nil {
		reports = append(reports, reportFrame{"route-ledger", "research-routes",
			"scheduled immutable checkpoint for decision/quote/lifecycle/fee replay", routes})
	}
	if crossVenue, err := s.store.CrossVenueDecisionReport(ctx, 100); err == nil {
		reports = append(reports, reportFrame{"cross-venue-decisions", "objective-cross-venue",
			"scheduled immutable checkpoint retaining accepted and rejected identity decisions", crossVenue})
	}
	if ctx.Err() != nil {
		return
	}
	if notices, err := s.store.VenueNoticeReport(ctx, 100); err == nil {
		reports = append(reports, reportFrame{"venue-notices", "official-venue-notices",
			"scheduled immutable checkpoint for official rule/schema/fee/maintenance versions", notices})
	}
	if governance, err := s.store.ResearchGovernanceReport(ctx); err == nil {
		reports = append(reports, reportFrame{"governance", "research-governance",
			"scheduled immutable checkpoint for authority/security policy replay", governance})
	}
	if ctx.Err() != nil {
		return
	}
	if inference, err := s.store.LatestResearchInference(ctx); err == nil {
		reports = append(reports, reportFrame{"inference", "research-inference",
			"scheduled immutable checkpoint for exact exclusions and statistical gates", inference})
	}
	if evi, ok, err := s.store.LatestEVIScheduleRun(ctx); err == nil && ok {
		reports = append(reports, reportFrame{"evi-schedule", "research-evi",
			"scheduled immutable checkpoint for frozen-input EVI decisions", evi})
	}
	if causes, ok, err := s.store.LatestCauseGraphRun(ctx); err == nil && ok {
		reports = append(reports, reportFrame{"cause-graph", "research-cause-graph",
			"scheduled immutable checkpoint for shared-cause portfolio replay", causes})
	}
	if ctx.Err() != nil {
		return
	}
	if bundles, legs, states, grades, err := s.store.ResearchRouteBundleCounts(ctx); err == nil {
		if promotion, promotionErr := s.store.ResearchBundlePromotionStatus(ctx); promotionErr == nil {
			reports = append(reports, reportFrame{"multi-leg-routes", "research-route-bundles",
				"scheduled immutable checkpoint for typed payoff states and exact RFQ handoff",
				map[string]any{"bundles": bundles, "ordered_legs": legs, "payoff_states": states,
					"counterfactual_grades": grades, "promotion": promotion}})
		}
	}
	selected := s.drainSelectedResearchReplay()
	frames := make([]researchreplay.Frame, 0, len(reports)+len(selected))
	for _, report := range reports {
		frame, err := researchreplay.NewFrame("kalshi-suite-research", "r138-governance-v1",
			report.kind, report.entity, report.selection, report.payload)
		if err != nil {
			if s.log != nil {
				s.log.Warn("research replay frame rejected", "kind", report.kind, "err", err)
			}
			continue
		}
		frames = append(frames, frame)
	}
	frames = append(frames, selected...)
	if len(frames) == 0 {
		return
	}
	shouldFlush, err := w.QueueBatch(frames)
	if err == researchreplay.ErrBatchFull {
		if _, wrote, flushErr := flushResearchReplayDurably(ctx, w); flushErr == nil && wrote {
			shouldFlush, err = w.QueueBatch(frames)
		}
	}
	if err != nil {
		s.requeueSelectedResearchReplay(selected)
		if s.log != nil {
			s.log.Warn("research replay queue failed", "err", err)
		}
		return
	}
	state := researchReplayRuntimeFor(s)
	state.Lock()
	firstFlush := state.lastFlush.IsZero()
	flushDue := firstFlush || time.Since(state.lastFlush) >= 15*time.Minute
	state.Unlock()
	if !shouldFlush && !flushDue {
		return
	}
	meta, wrote, err := flushResearchReplayDurably(ctx, w)
	if err != nil {
		if s.log != nil {
			s.log.Warn("research replay flush failed", "err", err)
		}
		return
	}
	if wrote {
		state.Lock()
		state.lastFlush = time.Now()
		state.Unlock()
		if s.log != nil {
			s.log.Info("research replay checkpoint", "frames", meta.Frames,
				"segment", fmt.Sprintf("%.16s", meta.SegmentHash))
		}
	}
}
