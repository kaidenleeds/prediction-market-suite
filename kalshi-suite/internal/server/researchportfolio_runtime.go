package server

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/researchportfolio"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

const (
	researchPortfolioFirstDelay = 47 * time.Second
	researchPortfolioCadence    = 5 * time.Minute
	researchPortfolioRetryDelay = 17 * time.Second
)

// researchPortfolioTick is a research-governance loop only.  It reads the latest immutable frozen
// inputs and appends deterministic receipts.  It has no collector, config, order, bankroll, ARM,
// AUTO, paper, or LIVE dependency and therefore cannot execute a selected task or exposure.
func (s *Server) researchPortfolioTick(ctx context.Context) {
	now := time.Now().UTC()
	if err := s.runFrozenEVISchedule(ctx, now); err != nil {
		s.log.Warn("research EVI scheduler receipt failed", "err", err)
	}
	if err := s.runFrozenCauseGraph(ctx, now); err != nil {
		s.log.Warn("research cause-graph receipt failed", "err", err)
	}
}

func (s *Server) runResearchPortfolioTick(ctx context.Context, wait time.Duration) bool {
	return s.tryRunHeavyResearch(ctx, "portfolio", wait, func(workCtx context.Context) {
		s.researchPortfolioTick(workCtx)
	})
}

// retryDeferredResearch runs at most one synchronous attempt at a time. A false return means the
// shared heavy lane was occupied, not that portfolio work began; waiting here therefore consumes
// neither an EVI/cause receipt nor any collector-specific due state.
func retryDeferredResearch(ctx context.Context, delay time.Duration, attempt func() bool) bool {
	for {
		if attempt() {
			return true
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}

// monitorResearchPortfolio keeps deterministic EVI/cause receipts current without rewriting an
// unchanged manifest every 30 seconds. Forty-seven seconds is deliberately off the 5s/30s phases
// used by liveness and ordinary collectors. The shared heavy gate defers a collision rather than
// allowing governance queries to compete with inference/replay for SQLite and CPU.
func (s *Server) monitorResearchPortfolio(ctx context.Context) {
	timer := time.NewTimer(researchPortfolioFirstDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	run := func() bool {
		return s.runResearchPortfolioTick(ctx, 0)
	}
	if !retryDeferredResearch(ctx, researchPortfolioRetryDelay, run) {
		return
	}
	ticker := time.NewTicker(researchPortfolioCadence)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !retryDeferredResearch(ctx, researchPortfolioRetryDelay, run) {
				return
			}
		}
	}
}

func sameFrozenBudgets(a, b storage.FrozenEVIInput) bool {
	return a.ComputeBudget == b.ComputeBudget && a.APIBudget == b.APIBudget &&
		a.ComputeDollarPerMinute == b.ComputeDollarPerMinute && a.APIDollarPerCall == b.APIDollarPerCall
}

func (s *Server) runFrozenEVISchedule(ctx context.Context, now time.Time) error {
	rows, err := s.store.CurrentFrozenEVIInputs(ctx)
	if err != nil {
		return err
	}
	entries, active, valid := storage.FrozenEVIManifestEntries(rows, now)
	manifest, err := storage.ResearchPortfolioManifestHash("evi-schedule-v1", entries)
	if err != nil {
		return err
	}
	run := storage.ResearchPortfolioRun{ManifestHash: manifest, Observed: now, InputHashes: entries,
		InputCount: active, ValidInputCount: valid, ResultJSON: `{"selected":[],"rejected":{}}`,
		Funded: false, PaperAuthority: false, LiveAuthority: false}
	if active == 0 {
		run.State = "WAITING_FOR_FROZEN_INPUTS"
		run.Reason = "no active immutable task versions; decision-change probability, value, cost, and budgets are never inferred from row count"
		_, _, err = s.store.InsertEVIScheduleRun(ctx, run)
		return err
	}
	if valid != active {
		run.State = "BLOCKED_STALE_OR_INCONSISTENT_INPUTS"
		run.Reason = fmt.Sprintf("%d of %d active frozen task inputs are fresh; the scheduler refuses partial or stale assumptions", valid, active)
		_, _, err = s.store.InsertEVIScheduleRun(ctx, run)
		return err
	}
	var activeRows []storage.FrozenEVIInput
	for _, row := range rows {
		if row.InputState == "active" {
			activeRows = append(activeRows, row)
		}
	}
	sort.Slice(activeRows, func(i, j int) bool { return activeRows[i].TaskID < activeRows[j].TaskID })
	for i := 1; i < len(activeRows); i++ {
		if !sameFrozenBudgets(activeRows[0], activeRows[i]) {
			run.State = "BLOCKED_STALE_OR_INCONSISTENT_INPUTS"
			run.Reason = "active task versions disagree on the explicitly frozen compute/API budget or unit cost"
			_, _, err = s.store.InsertEVIScheduleRun(ctx, run)
			return err
		}
	}
	tasks := make([]researchportfolio.ResearchTask, 0, len(activeRows))
	for _, row := range activeRows {
		tasks = append(tasks, researchportfolio.ResearchTask{ID: row.TaskID,
			ProbabilityChangesDecision: row.ProbabilityChangesDecision,
			DecisionValueDollarsPerDay: row.DecisionValueDollarsPerDay,
			CollectionCostDollars:      row.CollectionCostDollars,
			ComputeMinutes:             row.ComputeMinutes, APICalls: row.APICalls})
	}
	base := activeRows[0]
	schedule, err := researchportfolio.ScheduleEVI(tasks, base.ComputeBudget, base.APIBudget,
		base.ComputeDollarPerMinute, base.APIDollarPerCall)
	if err != nil {
		return err
	}
	b, err := json.Marshal(schedule)
	if err != nil {
		return err
	}
	run.State, run.ResultJSON = "SCHEDULED", string(b)
	run.Reason = "deterministic research queue from active immutable assumptions; selected tasks are recommendations only and change nothing automatically"
	_, _, err = s.store.InsertEVIScheduleRun(ctx, run)
	return err
}

func (s *Server) runFrozenCauseGraph(ctx context.Context, now time.Time) error {
	rows, err := s.store.CurrentFrozenCauseExposures(ctx)
	if err != nil {
		return err
	}
	entries, active, valid := storage.FrozenCauseManifestEntries(rows, now)
	manifest, err := storage.ResearchPortfolioManifestHash("cause-graph-v1", entries)
	if err != nil {
		return err
	}
	run := storage.ResearchPortfolioRun{ManifestHash: manifest, Observed: now, InputHashes: entries,
		InputCount: active, ValidInputCount: valid, ResultJSON: `[]`, Funded: false,
		PaperAuthority: false, LiveAuthority: false}
	if active == 0 {
		run.State = "BLOCKED_NO_REPLICATED_ROUTE_LOWER_BOUNDS"
		run.Reason = "no active untouched-replication executable fee-net route lower bounds are frozen"
		_, _, err = s.store.InsertCauseGraphRun(ctx, run)
		return err
	}
	if valid != active {
		run.State = "BLOCKED_STALE_REPLICATED_ROUTE_LOWER_BOUNDS"
		run.Reason = fmt.Sprintf("%d of %d active replicated route lower bounds are fresh and qualified; point estimates are never substituted", valid, active)
		_, _, err = s.store.InsertCauseGraphRun(ctx, run)
		return err
	}
	var exposures []researchportfolio.SystemExposure
	for _, row := range rows {
		if row.InputState != "active" {
			continue
		}
		exposures = append(exposures, researchportfolio.SystemExposure{SystemID: row.SystemID,
			RouteID: row.RouteID, Venue: row.Venue, CauseIDs: row.CauseIDs,
			NetPerDayLower: row.NetPerDayLower, Capacity: row.Capacity,
			CapitalDollarHoursPerDay: row.CapitalDollarHoursPerDay})
	}
	clusters, err := researchportfolio.CauseGraph(exposures)
	if err != nil {
		return err
	}
	b, err := json.Marshal(clusters)
	if err != nil {
		return err
	}
	run.State, run.ResultJSON = "READY", string(b)
	run.Reason = "shared-cause graph uses only fresh untouched-replication executable fee-net route lower bounds; positive shared causes count once"
	_, _, err = s.store.InsertCauseGraphRun(ctx, run)
	return err
}

func portfolioRunView(run storage.ResearchPortfolioRun, exists bool) map[string]any {
	if !exists {
		return nil
	}
	var result any
	if err := json.Unmarshal([]byte(run.ResultJSON), &result); err != nil {
		result = map[string]any{"decode_error": err.Error()}
	}
	return map[string]any{"manifest_hash": run.ManifestHash, "observed_ts": run.Observed,
		"state": run.State, "input_hashes": run.InputHashes, "input_count": run.InputCount,
		"valid_input_count": run.ValidInputCount, "result": result, "reason": run.Reason,
		"funded": false, "paper_authority": false, "live_authority": false,
		"automatic_collection_changes": false, "automatic_budget_changes": false,
		"order_authority": false}
}
