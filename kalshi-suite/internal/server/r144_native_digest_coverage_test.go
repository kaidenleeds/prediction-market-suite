package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func insertNativeDigestReceipt(t *testing.T, s *Server, system, cycle string, at time.Time,
	inserted, duplicates int, metrics map[string]any,
) {
	t.Helper()
	collectorID := nativeLockCollectorID(system)
	var experiment, source, schema, systemsJSON string
	var experimentVersion int
	var cadenceSeconds float64
	if err := s.store.DBForTest().QueryRowContext(context.Background(), `SELECT experiment_id,
experiment_version,source,schema_version,expected_cadence_s,systems_json
FROM research_collector_specs WHERE collector_id=? AND active=1 ORDER BY version DESC LIMIT 1`, collectorID).
		Scan(&experiment, &experimentVersion, &source, &schema, &cadenceSeconds, &systemsJSON); err != nil {
		t.Fatal(err)
	}
	var systems []string
	if err := json.Unmarshal([]byte(systemsJSON), &systems); err != nil {
		t.Fatal(err)
	}
	if metrics == nil {
		metrics = map[string]any{}
	}
	receipt := storage.CollectorReceipt{CollectorID: collectorID, CycleID: cycle,
		ExperimentID: experiment, ExperimentVersion: experimentVersion,
		Status: "healthy", Started: at.Add(-time.Second), Completed: at,
		Eligible: inserted + duplicates, Attempted: inserted + duplicates,
		Inserted: inserted, Duplicates: duplicates, Metrics: metrics,
		Source: source, SchemaVersion: schema,
		ExpectedCadence: time.Duration(cadenceSeconds * float64(time.Second)), Systems: systems}
	if ok, err := s.store.InsertCollectorReceipt(context.Background(), receipt); err != nil || !ok {
		t.Fatalf("insert %s receipt ok=%v err=%v", system, ok, err)
	}
}

func TestR144NativeCompactCoverageUsesLatestReceiptNotHistory(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	insertNativeDigestReceipt(t, s, "time-nested-lock", "old", now.Add(-time.Minute), 90, 0, nil)
	insertNativeDigestReceipt(t, s, "time-nested-lock", "current", now, 3, 2, nil)
	insertNativeDigestReceipt(t, s, "joint-marginal-lock", "current", now, 4, 1, nil)
	insertNativeDigestReceipt(t, s, "fee-rounding-batch", "current", now, 5, 1,
		map[string]any{"positive_rounding_savings_controls": 2})

	views, err := s.queryNativeLockCollectionViewsCompact(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	wantRows := map[string]int{"time-nested-lock": 5, "joint-marginal-lock": 5, "fee-rounding-batch": 6}
	if len(views) != len(wantRows) {
		t.Fatalf("compact native views=%+v", views)
	}
	for _, view := range views {
		if !view.CurrentCycle || view.CurrentCycleMatched != wantRows[view.SystemID] ||
			view.Rows != 0 || view.Cycles != 0 || view.Grades != 0 || view.Alerts != 0 ||
			view.State != "RESEARCH_ONLY_COLLECTING" || strings.Contains(view.Reason, "lifetime count") {
			t.Fatalf("current-cycle view is not compact/current: %+v", view)
		}
		if view.SystemID == "fee-rounding-batch" && view.CurrentCycleCandidates != 2 {
			t.Fatalf("fee current optimization candidates=%d, want 2", view.CurrentCycleCandidates)
		}
	}
}

func TestR144ResearchDigestCarriesNativeCurrentCoverageIntoSystems(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	for i, system := range nativeLockSystemIDs {
		metrics := map[string]any{}
		if system == "fee-rounding-batch" {
			metrics["positive_rounding_savings_controls"] = 1
		}
		insertNativeDigestReceipt(t, s, system, "current", now, i+2, 1, metrics)
	}
	payload, trials, research, native := buildResearchDigest(context.Background(), s)
	if len(native) != len(nativeLockSystemIDs) {
		t.Fatalf("native digest coverage=%+v", native)
	}
	if payload.Research.Registered != len(storage.ResearchExperimentIDs())+len(nativeLockSystemIDs) {
		t.Fatalf("registered=%d, want base %d + native %d", payload.Research.Registered,
			len(storage.ResearchExperimentIDs()), len(nativeLockSystemIDs))
	}
	if payload.Research.RegistryScope != "research_hypotheses" || payload.Research.CurrentCycleMatches <= 0 ||
		payload.Research.ExactRows != 0 || payload.Research.Open != 0 {
		t.Fatalf("research registry/current-cycle truth is ambiguous: %+v", payload.Research)
	}
	byResearch := map[string]storage.ResearchSystemCollectionStat{}
	for _, row := range research {
		byResearch[row.SystemID] = row
	}
	for _, system := range nativeLockSystemIDs {
		row, ok := byResearch[system]
		if !ok || row.EconomicObservations != 0 || row.Open != 0 || row.InputCycles != 0 ||
			row.CurrentCycleMatches <= 0 ||
			row.State != "RESEARCH_ONLY_COLLECTING" {
			t.Fatalf("native system absent or placeholder-only in digest: %s %+v", system, row)
		}
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	s.researchDigestMu.Lock()
	s.researchDigestJSON = encoded
	s.researchDigestTrials = append([]storage.UnitTrialLeaderboardStat(nil), trials...)
	s.researchDigestSystems = append([]storage.ResearchSystemCollectionStat(nil), research...)
	s.researchDigestNative = append([]nativeLockCollectionView(nil), native...)
	s.researchDigestAt, s.researchDigestNext = now, now.Add(time.Hour)
	s.researchDigestMu.Unlock()
	rows, coverage, _, ready, digestErr := s.cachedSystemLeaderboardRows(nil, false)
	if !ready || digestErr != "" {
		t.Fatalf("cached digest ready=%v err=%q", ready, digestErr)
	}
	byCoverage := map[string]leaderboardCoverageRow{}
	for _, row := range coverage {
		byCoverage[row.Family] = row
	}
	byRow := map[string]leaderboardBacktestRow{}
	for _, row := range rows {
		byRow[row.Family] = row
	}
	for _, system := range nativeLockSystemIDs {
		coverageRow, coverageOK := byCoverage[system]
		row, rowOK := byRow[system]
		if !coverageOK || coverageRow.CollectionN != 0 || coverageRow.CollectionOpen != 0 ||
			coverageRow.CollectionCycles != 0 || coverageRow.CollectionCurrentCycle <= 0 ||
			!strings.Contains(coverageRow.CollectionSource, nativeLockCollectorID(system)) {
			t.Fatalf("native cached coverage missing for %s: %+v", system, coverageRow)
		}
		if !rowOK || row.EconomicsReady || row.CollectionN != 0 || row.CollectionOpen != 0 ||
			row.CollectionCurrentCycle <= 0 || row.CollectionState != "RESEARCH_ONLY_COLLECTING" {
			t.Fatalf("native Systems row is missing or a fabricated economic row for %s: %+v", system, row)
		}
	}
}
