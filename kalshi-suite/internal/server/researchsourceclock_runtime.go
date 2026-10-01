package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const researchSourceClockCadence = time.Minute

type researchSourceClockRuntime struct {
	sync.Mutex
	lastSweep          time.Time
	tokens             map[string]string
	lastBookSeq        int64
	lastBookGeneration uint64
	lastBookGaps       int64
}

var researchSourceClockRuntimes sync.Map // map[*Server]*researchSourceClockRuntime

func sourceClockRuntimeFor(s *Server) *researchSourceClockRuntime {
	if value, ok := researchSourceClockRuntimes.Load(s); ok {
		return value.(*researchSourceClockRuntime)
	}
	created := &researchSourceClockRuntime{tokens: map[string]string{}}
	value, _ := researchSourceClockRuntimes.LoadOrStore(s, created)
	return value.(*researchSourceClockRuntime)
}

func researchSourceClockSweepDue(s *Server, now time.Time) bool {
	state := sourceClockRuntimeFor(s)
	state.Lock()
	defer state.Unlock()
	if !state.lastSweep.IsZero() && now.Sub(state.lastSweep) < researchSourceClockCadence {
		return false
	}
	state.lastSweep = now
	return true
}

func sourceClockEvidence(value any) string {
	b, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func (s *Server) recordR138SourceClock(ctx context.Context, sourceID, token string, watermark time.Time,
	rows int, sequenceStart, sequenceEnd, sequencePrior, sequenceGap *int64,
	evidence any, errorClass, errorText string) bool {
	state := sourceClockRuntimeFor(s)
	state.Lock()
	if state.tokens[sourceID] == token {
		state.Unlock()
		return true
	}
	state.Unlock()
	spec, ok := storage.R138SourceClockSpec(sourceID)
	if !ok {
		spec, ok = storage.R138Step8SourceClockSpec(sourceID)
	}
	if !ok {
		return false
	}
	receipt := storage.SourceClockReceipt{
		SourceID: sourceID, SpecVersion: spec.Version, SchemaVersion: spec.SchemaVersion,
		SchemaHash: spec.SchemaHash, Received: time.Now().UTC(), Watermark: watermark,
		SequenceStart: sequenceStart, SequenceEnd: sequenceEnd, SequencePrior: sequencePrior,
		SequenceGap: sequenceGap, RowsSeen: rows,
		EvidenceJSON: sourceClockEvidence(evidence), ErrorClass: errorClass, ErrorText: errorText,
	}
	result, err := s.store.RecordSourceClock(ctx, receipt)
	if err != nil {
		if s.log != nil {
			s.log.Warn("research source-clock receipt failed", "source", sourceID, "err", err)
		}
		return false
	}
	meta := map[string]any{"clock_kind": spec.ClockKind}
	if supplied, ok := evidence.(map[string]any); ok {
		for key, value := range supplied {
			meta[key] = value
		}
	}
	exactGap := int64(0)
	if sequenceGap != nil {
		exactGap = *sequenceGap
	}
	s.queueSourceClockReplay(receipt, result, sequencePrior, exactGap, meta)
	state.Lock()
	state.tokens[sourceID] = token
	state.Unlock()
	return true
}

func (s *Server) sweepResearchSourceClocks(ctx context.Context) {
	if s.kal != nil {
		truth := s.kal.BookSourceClockTruth()
		if truth.OK {
			state := sourceClockRuntimeFor(s)
			state.Lock()
			previousSeq, previousGeneration, previousGaps := state.lastBookSeq, state.lastBookGeneration, state.lastBookGaps
			state.Unlock()
			if truth.Sequence != previousSeq || truth.Generation != previousGeneration || truth.Gaps != previousGaps {
				current, prior := truth.Sequence, truth.PriorSequence
				if truth.Gaps > previousGaps && truth.LastGapSequence > 0 {
					current, prior = truth.LastGapSequence, truth.LastGapPrior
				}
				start, end := current, current // one actually observed source frame; never a synthetic range
				var priorPtr *int64
				if prior > 0 && (truth.Generation == previousGeneration || truth.Gaps > previousGaps) {
					priorCopy := prior
					priorPtr = &priorCopy
				}
				gap := int64(0)
				if priorPtr != nil && current > *priorPtr+1 {
					gap = current - *priorPtr - 1
				}
				if s.recordR138SourceClock(ctx, "kalshi-orderbook-ws",
					fmt.Sprintf("%d:%d:%d:%d", truth.Generation, truth.SubscriptionID, truth.Sequence, truth.Gaps),
					time.Time{}, truth.Rows, &start, &end, priorPtr, &gap,
					map[string]any{"channel": truth.Channel, "source_generation": truth.Generation,
						"subscription_id": truth.SubscriptionID, "last_frame_received_at": truth.Received,
						"latest_sequence": truth.Sequence, "actual_prior_sequence": prior,
						"gap_count": truth.Gaps, "last_gap_prior": truth.LastGapPrior,
						"last_gap_sequence": truth.LastGapSequence}, "", "") {
					state.Lock()
					state.lastBookSeq, state.lastBookGeneration, state.lastBookGaps =
						truth.Sequence, truth.Generation, truth.Gaps
					state.Unlock()
				}
			}
		}
		watermark, rows, frames, rejects, lastReject := s.kal.TickerSourceClock()
		token := fmt.Sprintf("%s:%d:%d:%s", watermark.UTC().Format(time.RFC3339Nano), frames,
			rejects, lastReject.UTC().Format(time.RFC3339Nano))
		if watermark.IsZero() {
			token = fmt.Sprintf("missing:%d:%d:%d:%s", rows, frames, rejects,
				lastReject.UTC().Format(time.RFC3339Nano))
		}
		errorClass, errorText := "", ""
		if !lastReject.IsZero() && (watermark.IsZero() || lastReject.After(watermark)) {
			errorClass, errorText = "source_clock_rejected", "latest Kalshi ticker frame had a missing, stale, or future venue timestamp"
		}
		s.recordR138SourceClock(ctx, "kalshi-ticker-ws", token, watermark, rows, nil, nil, nil, nil,
			map[string]any{"websocket_rows": rows, "frames_seen": frames, "clock_rejects": rejects,
				"last_clock_reject_at": lastReject}, errorClass, errorText)

		life := s.kal.LifecycleSequenceTruth()
		lifeToken := fmt.Sprintf("%d:%d:%d:%d:%d:%s", life.Generation, life.SID, life.Sequence,
			life.GapTotal, life.Frames, life.Watermark.UTC().Format(time.RFC3339Nano))
		if life.Frames > 0 && life.Sequence > 0 {
			start, end := life.Sequence, life.Sequence
			var prior *int64
			if life.Prior > 0 {
				p := life.Prior
				prior = &p
			}
			gap := life.LastGap
			s.recordR138SourceClock(ctx, "kalshi-market-lifecycle-ws", lifeToken, life.Watermark,
				int(life.Frames), &start, &end, prior, &gap, map[string]any{"frames": life.Frames,
					"source_generation": life.Generation, "subscription_id": life.SID,
					"optional_timestamp_present": !life.Watermark.IsZero(), "gap_total": life.GapTotal}, "", "")
		}
	}
	if s.polyUSWS != nil {
		watermark, frames := s.polyUSWS.FullSourceClock()
		if frames > 0 {
			s.recordR138SourceClock(ctx, "polyus-market-ws-full",
				fmt.Sprintf("%d:%s", frames, watermark.UTC().Format(time.RFC3339Nano)), watermark,
				int(frames), nil, nil, nil, nil, map[string]any{"full_frames": frames}, "", "")
		}
		arrival, frames := s.polyUSWS.LiteArrivalClock()
		if frames > 0 {
			s.recordR138SourceClock(ctx, "polyus-market-ws-lite",
				fmt.Sprintf("%d:%s", frames, arrival.UTC().Format(time.RFC3339Nano)), time.Time{},
				int(frames), nil, nil, nil, nil, map[string]any{"last_arrival_at": arrival, "lite_frames": frames}, "", "")
		}
	}
	if s.poly != nil {
		watermark, rows, connected := s.poly.CLOBSourceClock()
		if connected || rows > 0 {
			s.recordR138SourceClock(ctx, "polyint-clob-ws",
				fmt.Sprintf("%t:%d:%s", connected, rows, watermark.UTC().Format(time.RFC3339Nano)), watermark,
				rows, nil, nil, nil, nil, map[string]any{"connected": connected, "current_generation_books": rows}, "", "")
		}
	}
}
