package server

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR135LegacySignalPriceReplayCannotApplySettings(t *testing.T) {
	if legacyReplayMayApply {
		t.Fatal("legacy replay has future-label and route-price leakage; it must remain read-only")
	}
}

func TestR135MLEvalVectorsRequireExactNonEmptyModelVersion(t *testing.T) {
	m := &mlEvalModel{version: "model-v2"}
	if mlEvalVectorVersionOK(m, "") || mlEvalVectorVersionOK(m, "model-v1") || mlEvalVectorVersionOK(nil, "model-v2") {
		t.Fatal("empty, stale, or model-less vector generations must fail closed")
	}
	if !mlEvalVectorVersionOK(m, "model-v2") {
		t.Fatal("matching model/vector generation rejected")
	}
}

func TestR135WeatherSignedDecimalBinsAndFullSettlementSourceVector(t *testing.T) {
	lo, hi, ok := wxParseBin("-5.5° to -3.5°")
	if !ok || math.Abs(lo-(-5.5)) > 1e-12 || math.Abs(hi-(-3.5)) > 1e-12 {
		t.Fatalf("signed decimal weather bin parsed as lo=%v hi=%v ok=%v", lo, hi, ok)
	}
	nyc := "https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=NYC"
	backup := "https://forecast.weather.gov/product.php?site=OKX&product=CLI&issuedby=nyc"
	if station, ok := wxSettlementStation([]string{nyc, backup}); !ok || station != "NYC" {
		t.Fatalf("compatible complete source vector rejected: station=%q ok=%v", station, ok)
	}
	mdw := "https://forecast.weather.gov/product.php?site=LOT&product=CLI&issuedby=MDW"
	if station, ok := wxSettlementStation([]string{nyc, mdw}); ok || station != "" {
		t.Fatalf("mixed settling stations must fail closed: station=%q ok=%v", station, ok)
	}
	if _, ok := wxSettlementStation([]string{nyc, "https://example.com/alternate-observation"}); ok {
		t.Fatal("mixed settlement authority must fail closed")
	}
}

func TestR135C4IsReturnPerEntryDollarDay(t *testing.T) {
	edge, rate, ok := evPerCapitalDay(.55, .50, 0, 1)
	if !ok || math.Abs(edge-.05) > 1e-12 || math.Abs(rate-.10) > 1e-12 {
		t.Fatalf("C4 dimensions wrong: edge=%v rate=%v ok=%v", edge, rate, ok)
	}
}

func TestR135LeaderboardRanksConservativeNetPerDayBeforeCentsPerShare(t *testing.T) {
	lo, hi, netLo, netHi := .15, .25, .10, .30
	rows := leaderboardBacktestRows([]storage.UnitTrialLeaderboardStat{
		{Family: "large-proof", Platform: "kalshi", N: 50, SettledMarkets: 50, SettledEventClusters: 50, TrackedSeconds: 40 * 86400, MeanPC: .20, SDPC: .01, SettledPerDay: 1, NetPerCalendarDay: .20,
			MatureUTCBlocks: 30, ObservedUTCBlocks: 20, EventDayClusters: 20, DayMeanPC: .20,
			DayMeanLoPC: &lo, DayMeanHiPC: &hi, ClusterNetPerDayLo: &netLo, ClusterNetPerDayHi: &netHi},
		{Family: "tiny-fast", Platform: "polyus", N: 1, SettledMarkets: 1, TrackedSeconds: 30, MeanPC: .90, SettledPerDay: 1, NetPerCalendarDay: .90},
	})
	if len(rows) != 2 || rows[0].Family != "large-proof" {
		t.Fatalf("conservative Net/d did not outrank tiny point cents/share: %+v", rows)
	}
	if rows[1].Proof != "VOID ASSUMED-FILL HISTORY" || rows[1].SimulationState != "COLLECTING" || rows[1].NetPerCalendarDayLo != 0 {
		t.Fatalf("seconds-old tiny sample gained a conservative Net/d bound: %+v", rows[1])
	}
	if rows[0].Proof != "VOID ASSUMED-FILL HISTORY" || rows[0].SimulationState != "LOWER-BOUND+" || rows[0].NetPerCalendarDayLo <= 0 {
		t.Fatalf("confidence-qualified row missing: %+v", rows[0])
	}
	if rows[0].Layer != "observed_book" || rows[0].EvidenceTier != "historical_assumed_fill_simulation_void" ||
		rows[0].FillConditioned || rows[0].ProfitEvidence || rows[0].LiveAuthorizes || !rows[0].Counterfactual || !rows[0].ResearchOnly {
		t.Fatalf("an ask-priced unit trial masqueraded as exchange-fill evidence: %+v", rows[0])
	}
}

func TestR135cYoungBurstCannotBecomeProven(t *testing.T) {
	rows := leaderboardBacktestRows([]storage.UnitTrialLeaderboardStat{{
		Family: "burst", Platform: "kalshi", N: 20, SettledMarkets: 20, TrackedSeconds: 30,
		MeanPC: .20, SDPC: .001, SettledPerDay: 57600, NetPerCalendarDay: 11520,
	}})
	if len(rows) != 1 || rows[0].Proof != "VOID ASSUMED-FILL HISTORY" || rows[0].SimulationState != "COLLECTING" || rows[0].ConfidenceReady {
		t.Fatalf("seconds-old burst masqueraded as mature edge proof: %+v", rows)
	}
}

func TestR135cConcreteResearchSystemsStayVisibleWithoutAuthority(t *testing.T) {
	rows := appendUniqueSystemCoverage(nil, researchSystemCoverageRows(), adaptiveWhaleCoverageRows())
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.Family] {
			t.Fatalf("duplicate Systems coverage row %q", r.Family)
		}
		seen[r.Family] = true
		if !r.ResearchOnly || r.LiveAuthorizes || r.Timed {
			t.Fatalf("unproved research system gained authority: %+v", r)
		}
	}
	for _, want := range []string{"behavioral-bias-regime", "subcent-golf", "queue-priority",
		"event-basket-lock@kalshi", "event-basket-lock@polyus", "nested-ladder-lock",
		"raw-flow-observer@kalshi", "adaptive-whale-scorer@polymarket", "time-nested-lock",
		"joint-marginal-lock", "fee-rounding-batch"} {
		if !seen[want] {
			t.Fatalf("concrete system %q missing from Systems coverage", want)
		}
	}
}

func TestR139AllNineteenRegisteredSystemsBecomeVisibleCollectionRows(t *testing.T) {
	ids := storage.SystemIDs()
	stats := make([]storage.ResearchSystemCollectionStat, 0, len(ids))
	for i, id := range ids {
		stats = append(stats, storage.ResearchSystemCollectionStat{SystemID: id,
			State: "COLLECTING_PARTIAL", Reason: "truthful source-specific blocker",
			CollectorIDs: []string{"dedicated-" + id}, InputRows: i + 1, InputCycles: i + 1,
			Candidates: 1, Open: 1})
	}
	coverage := r138SystemCoverageRows(stats)
	raw, err := json.Marshal(coverage[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"\"collection_n\"", "\"collection_cycles\"", "\"collection_open\"", "\"collection_candidates\""} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("coverage JSON lost %s: %s", field, raw)
		}
	}
	rows := appendLeaderboardCollectionPlaceholders(nil, coverage)
	wantCells := 0
	for _, spec := range storage.SystemExecutionSpecs() {
		cells := len(spec.ExactVariants())
		if cells == 0 {
			cells = 1 // one explicit NEEDS_EXECUTION_ADAPTER row, never a fake order cell
		}
		wantCells += cells
	}
	if len(coverage) != wantCells || len(rows) != wantCells {
		t.Fatalf("coverage=%d rendered=%d want %d exact capability cells", len(coverage), len(rows), wantCells)
	}
	seen := map[string]int{}
	for _, row := range rows {
		seen[row.Family]++
		spec, ok := storage.SystemExecutionCapability(row.Family)
		if !ok || row.EconomicsReady || row.ResearchOnly || row.LiveAuthorizes || row.CollectionN <= 0 ||
			row.CollectionSource == "" || row.Route == "" || row.Side == "" {
			t.Fatalf("system visibility changed authority or lost collection truth: %+v", row)
		}
		adapter := storage.SystemExecutionAdapterStatus(spec)
		wantState := adapter.State
		if adapter.Connected {
			wantState = "COLLECTING_PARTIAL"
		}
		if row.ExecutionConnected != adapter.Connected || row.Proof != wantState {
			t.Fatalf("System adapter truth drifted: row=%+v adapter=%+v", row, adapter)
		}
		if adapter.Connected && row.LiveHandoff != spec.LiveHandoff {
			t.Fatalf("connected System lost its typed LIVE handoff: row=%+v spec=%+v", row, spec)
		}
		if !adapter.Connected && row.LiveHandoff != "" {
			t.Fatalf("disconnected System advertised a LIVE handoff: row=%+v adapter=%+v", row, adapter)
		}
	}
	for _, id := range ids {
		spec, _ := storage.SystemExecutionCapability(id)
		want := len(spec.ExactVariants())
		if want == 0 {
			want = 1
		}
		if seen[id] != want {
			t.Fatalf("registered system %s missing from rendered rows", id)
		}
	}
}

func TestR139LeaderboardKeepsYESAndNORoutesDistinctWithoutDuplicatePlaceholders(t *testing.T) {
	stats := []storage.UnitTrialLeaderboardStat{
		{Family: "edge", Platform: "kalshi", OriginLayer: "model", Side: "YES", N: 2, Total: 2},
		{Family: "edge", Platform: "kalshi", OriginLayer: "model", Side: "NO", N: 3, Total: 3},
	}
	rows := leaderboardBacktestRows(stats)
	coverage := []leaderboardCoverageRow{
		{Family: "edge", Group: "signal", Layer: "model", State: "COLLECTING", Venue: "kalshi"},
		{Family: "edge", Side: "YES", Route: "taker", Group: "taker", Layer: "execution_route", State: "COLLECTING", Venue: "kalshi", Timed: true},
		{Family: "edge", Side: "NO", Route: "taker", Group: "taker", Layer: "execution_route", State: "COLLECTING", Venue: "kalshi", Timed: true},
	}
	rows = appendLeaderboardCollectionPlaceholders(rows, coverage)
	if len(rows) != 3 {
		t.Fatalf("want two economic side routes plus one model placeholder, got %+v", rows)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.BookNative {
			identity := leaderboardCoverageIdentity(row.Family, row.Platform, row.Side, row.Route)
			if seen[identity] {
				t.Fatalf("side-specific route ID collapsed: %+v", rows)
			}
			seen[identity] = true
		}
	}
	if !seen[leaderboardCoverageIdentity("edge", "kalshi", "YES", "taker")] ||
		!seen[leaderboardCoverageIdentity("edge", "kalshi", "NO", "taker")] {
		t.Fatalf("missing side-specific route IDs: %+v", seen)
	}
}

func TestR139NativeRegistryShowsZeroNCurrentAndArchivedSystems(t *testing.T) {
	s := testServer(t)
	rows := s.nativeRegistryCoverageRows(nil)
	by := map[string]leaderboardCoverageRow{}
	for _, row := range rows {
		by[row.Family] = row
	}
	for _, family := range []string{"notail", "xvgap2", "xvgapk", "combo-rfq", "parlay-6leg", "auto-ml", "manual"} {
		if _, ok := by[family]; !ok {
			t.Fatalf("registered native system %q disappeared at n=0", family)
		}
	}
	for _, family := range []string{"notail", "xvgap2", "xvgapk"} {
		if by[family].State != "CURRENT" {
			t.Fatalf("active producer %q state=%q", family, by[family].State)
		}
	}
	if by["auto-ml"].State != "ARCHIVED_POLICY" || by["manual"].State != "ARCHIVED_POLICY" {
		t.Fatalf("archived rows falsely collecting: ml=%+v manual=%+v", by["auto-ml"], by["manual"])
	}
}
