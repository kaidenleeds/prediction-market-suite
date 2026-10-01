package server

import (
	"context"
	"strings"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

var r139FunnelBlockers = map[string]string{
	"attention-spillover-graph":            "immutable structural parent-child edges plus exact executable 60s/300s source-clock markouts and explicit misses collect; untouched event-day replication remains pending",
	"clientele-clock-basis":                "rule-certified simultaneous books now collect by venue-local hour, phase and liquidity cell; event-disjoint untouched replication remains pending",
	"collateral-release-rotation":          "authoritative Kalshi credit receipts, linked child books and deterministic structurally matched no-release market controls collect; PolyUS credited cash amount and untouched post-credit horizons remain pending",
	"deadline-hazard-surface":              "certified implication scans and independent NOAA NBM station/run candidates collect with current books; broader verified relation coverage and untouched replication remain pending",
	"flow-direction-integrity":             "authoritative Kalshi trade direction now emits a current exact-book prospective action beside its same-clock opposite control; untouched event-day replication remains pending",
	"proper-score-executor":                "native proper-score input mirror only; untouched event/day lower-bound replication still blocks authority",
	"forecast-persona-router":              "a frozen prior-window persona selector and later selected/opposite controls collect; untouched replication still blocks authority",
	"semantic-complexity-premium":          "deterministic rule-complexity side controls train the existing prior-only freezer; post-freeze selected exact actions collect while untouched event-day replication remains pending",
	"paired-bridge-inversion":              "same-clock canonical direct/inverse executable routes and terminal settlement collect; untouched paired lower-bound replication remains pending",
	"series-roll-anchor":                   "official series lineage now joins prior issue settlement/book state to current same-kind books; independent untouched residual evidence remains pending",
	"side-normalized-crowding-fade":        "same-frame bought-side-normalized direct/fade twins with exact books and fees collect; untouched agreement-bin replication remains pending",
	"settlement-latency-carry":             "prospective exact-book entries freeze prior-only void/dispute and alternative-capital reserves before observation; unknown horizons stay immutably blocked, while PolyUS direct credited cash amount and untouched replication remain pending",
	"identity-challenged-cross-venue-lock": "canonical payoff equality and simultaneous two-venue partial-fill envelopes are not yet complete",
}

func (s *Server) sweepR139SystemSpecificFunnels(ctx context.Context) {
	started := time.Now()
	specs := storage.R139SystemFunnelSpecs()
	if len(specs) == 0 {
		return
	}
	state := s.r138CollectorState()
	state.mu.Lock()
	if !state.systemFunnelCycleActive {
		if !state.lastSystemFunnels.IsZero() && started.Sub(state.lastSystemFunnels) < 5*time.Minute {
			state.mu.Unlock()
			return
		}
		state.systemFunnelCycleActive = true
		state.systemFunnelNext = 0
	}
	idx := state.systemFunnelNext
	if idx < 0 || idx >= len(specs) {
		idx = 0
		state.systemFunnelNext = 0
	}
	state.mu.Unlock()
	spec := specs[idx]
	receipt := storage.CollectorReceipt{CollectorID: spec.CollectorID,
		CycleID: collectorCycleID(started), ExperimentID: spec.ExperimentID,
		ExperimentVersion: spec.ExperimentVersion, Status: "healthy", Started: started,
		Source: spec.Source, SchemaVersion: spec.SchemaVersion, ExpectedCadence: spec.ExpectedCadence,
		Exclusions: map[string]int{}, Metrics: map[string]any{"bounded_input_limit": 20,
			"rotation_index": idx, "rotation_total": len(specs)},
		Systems: append([]string(nil), spec.Systems...)}
	inputs, err := s.store.ResearchSystemFunnelInputs(ctx, spec.ExperimentID, 20)
	if err != nil {
		receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", err.Error()
	} else {
		receipt.Eligible, receipt.Attempted = len(inputs), len(inputs)
		if len(inputs) == 0 {
			receipt.Status, receipt.ExpectedZero = "healthy_empty", true
			receipt.ZeroReason = "bounded source query returned no system-specific input; no generic identity row was substituted"
		}
		for _, input := range inputs {
			observed, ok := parseTime(strings.TrimSpace(input.Observed))
			if !ok {
				observed = started
				receipt.Exclusions["source_timestamp_unparseable_used_receipt_clock"]++
			}
			if input.Inputs == nil {
				input.Inputs = map[string]any{}
			}
			input.Inputs["system_specific_funnel"] = spec.CollectorID
			input.Inputs["economic_candidate"] = false
			_, inserted, insertErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
				Observed: observed, SystemID: spec.ExperimentID, ExperimentVersion: spec.ExperimentVersion,
				OpportunityID: input.OpportunityID,
				Kind:          "control", Cohort: "system-specific-input", CanonicalEventID: input.CanonicalEventID,
				Venue: input.Venue, Ticker: input.Ticker, Route: "observer", CertificateStatus: "not_applicable",
				SourceClockID: spec.CollectorID, SourceArtifact: input.Source,
				PayoutLower: 0, PayoutUpper: 0, NetLower: 0, NetUpper: 0, OutcomeStatus: "open",
				Blocker: r139FunnelBlockers[spec.ExperimentID], Inputs: input.Inputs,
			})
			if insertErr != nil {
				receipt.Exclusions["observation_storage_error"]++
				receipt.Status, receipt.ErrorClass, receipt.ErrorText = "error", "storage", insertErr.Error()
			} else if inserted {
				receipt.Inserted++
			} else {
				receipt.Duplicates++
			}
		}
	}
	receipt.Completed = time.Now()
	durableCtx, cancelDurability := researchDurabilityContext(ctx)
	_, receiptErr := s.store.InsertCollectorReceipt(durableCtx, receipt)
	cancelDurability()

	// A cooperative timeout or failed durable receipt retries this exact spec; neither may silently
	// consume the cursor and make the UI claim a completed rotation without evidence.
	if ctx.Err() != nil || receiptErr != nil {
		return
	}
	state.mu.Lock()
	if state.systemFunnelCycleActive && state.systemFunnelNext == idx {
		state.systemFunnelNext++
		if state.systemFunnelNext >= len(specs) {
			state.systemFunnelNext = 0
			state.systemFunnelCycleActive = false
			state.lastSystemFunnels = time.Now()
		}
	}
	state.mu.Unlock()
}
