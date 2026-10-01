package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/kalshi"
	"github.com/kalshi-suite/kalshi-suite/internal/nbm"
	"github.com/kalshi-suite/kalshi-suite/internal/polymarketus"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR138AttachFlowUsesAuthoritativeAggressorAndNoFutureLeakage(t *testing.T) {
	now := time.Now().UTC()
	frames := []r138PendingBookFrame{{frame: storage.ResearchMicrostructureFrame{Observed: now, Ticker: "KX"}}}
	var yes, no, old, equal, future kalshi.Trade
	for target, raw := range map[*kalshi.Trade]string{
		&yes:    `{"ticker":"KX","count_fp":"3","yes_price_dollars":"0.4","taker_outcome_side":"yes","created_time":"` + now.Add(-5*time.Second).Format(time.RFC3339Nano) + `"}`,
		&no:     `{"ticker":"KX","count_fp":"2","no_price_dollars":"0.6","taker_outcome_side":"no","created_time":"` + now.Add(-20*time.Second).Format(time.RFC3339Nano) + `"}`,
		&old:    `{"ticker":"KX","count_fp":"99","yes_price_dollars":"0.4","taker_outcome_side":"yes","created_time":"` + now.Add(-61*time.Second).Format(time.RFC3339Nano) + `"}`,
		&equal:  `{"ticker":"KX","count_fp":"4","yes_price_dollars":"0.4","taker_outcome_side":"yes","created_time":"` + now.Format(time.RFC3339Nano) + `"}`,
		&future: `{"ticker":"KX","count_fp":"100","yes_price_dollars":"0.4","taker_outcome_side":"yes","created_time":"` + now.Add(time.Nanosecond).Format(time.RFC3339Nano) + `"}`,
	} {
		if err := json.Unmarshal([]byte(raw), target); err != nil {
			t.Fatal(err)
		}
	}
	r138AttachFlow(frames, []kalshi.Trade{yes, no, old, equal, future})
	f := frames[0].frame
	if f.SignedFlow10 != 7 || f.FlowUnits10 != 7 || f.SignedFlow60 != 5 || f.FlowUnits60 != 9 {
		t.Fatalf("flow=%+v", f)
	}
}

func r138SamplerTestFrame(ticker string, ask float64) storage.ResearchMicrostructureFrame {
	return storage.ResearchMicrostructureFrame{Ticker: ticker, TickSize: .01,
		BidLevels: []storage.ResearchBookLevel{{Price: .4, Size: 1}},
		AskLevels: []storage.ResearchBookLevel{{Price: ask, Size: 1}}}
}

func TestR138SamplerBoundsCadenceBudgetCapacityAndRetention(t *testing.T) {
	base := time.Date(2026, 7, 12, 12, 0, 0, 0, time.UTC)
	st := &r138CollectorRuntime{lastCapture: map[string]time.Time{}, lastBookHash: map[string]string{},
		lastBookStored: map[string]time.Time{}, captureExclusions: map[string]int{}}
	if !st.admitR138BookSample("KX-0", base) || !st.enqueueR138BookFrame(r138SamplerTestFrame("KX-0", .42), base) {
		t.Fatal("first bounded frame was rejected")
	}
	if st.admitR138BookSample("KX-0", base.Add(r138BookSampleInterval-time.Nanosecond)) {
		t.Fatal("per-book cadence admitted an early repeat")
	}
	for i := 1; i < r138BookSampleBudget; i++ {
		ticker := fmt.Sprintf("KX-%d", i)
		if !st.admitR138BookSample(ticker, base) || !st.enqueueR138BookFrame(r138SamplerTestFrame(ticker, .42), base) {
			t.Fatalf("budget rejected frame %d before hard limit", i)
		}
	}
	if st.admitR138BookSample("KX-over", base) {
		t.Fatal("global per-minute heavy-path budget did not reject overflow before book/fee work")
	}
	frames, accounting := st.drainR138BookFrames(base)
	if len(frames) != r138BookSampleBudget || accounting.Admitted != r138BookSampleBudget ||
		accounting.Exclusions["sample_interval_coalesced"] != 1 ||
		accounting.Exclusions["heavy_path_budget_exhausted"] != 1 {
		t.Fatalf("bounded accounting frames=%d stats=%+v", len(frames), accounting)
	}
	if st.admitR138BookSample("KX-edge", base.Add(r138BookBudgetWindow-time.Nanosecond)) {
		t.Fatal("rolling one-minute budget allowed a boundary burst")
	}
	if !st.admitR138BookSample("KX-next", base.Add(r138BookBudgetWindow)) ||
		!st.enqueueR138BookFrame(r138SamplerTestFrame("KX-next", .44), base.Add(r138BookBudgetWindow)) {
		t.Fatal("rolling budget did not release the exactly expired window")
	}

	capacity := &r138CollectorRuntime{lastCapture: map[string]time.Time{}, lastBookHash: map[string]string{},
		lastBookStored: map[string]time.Time{}, captureExclusions: map[string]int{}}
	capacity.pending = make([]r138PendingBookFrame, r138BookPendingLimit)
	for i := range capacity.pending {
		capacity.pending[i].capturedAt = base
	}
	if capacity.enqueueR138BookFrame(r138SamplerTestFrame("FULL", .42), base) {
		t.Fatal("pending hard capacity accepted overflow")
	}
	_, capacityAccounting := capacity.drainR138BookFrames(base)
	if capacityAccounting.Exclusions["pending_capacity_rejected"] != 1 {
		t.Fatalf("capacity drop was silent: %+v", capacityAccounting)
	}

	retention := &r138CollectorRuntime{lastCapture: map[string]time.Time{}, lastBookHash: map[string]string{},
		lastBookStored: map[string]time.Time{}, captureExclusions: map[string]int{}}
	retention.pending = []r138PendingBookFrame{
		{capturedAt: base.Add(-r138BookPendingRetention - time.Nanosecond)},
		{capturedAt: base},
	}
	retained, retentionAccounting := retention.drainR138BookFrames(base)
	if len(retained) != 1 || retentionAccounting.Exclusions["pending_retention_expired"] != 1 {
		t.Fatalf("retention drop was silent: retained=%d accounting=%+v", len(retained), retentionAccounting)
	}
}

func TestR138CapacitySizesAreBoundedAndShareBased(t *testing.T) {
	if got := r138CapacitySizes(.9); got != nil {
		t.Fatalf("sub-one contract depth should not fabricate size: %v", got)
	}
	got := r138CapacitySizes(1234)
	if len(got) == 0 || got[0] != 1 || got[len(got)-1] != 1000 {
		t.Fatalf("bounded capacity sizes=%v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i] <= got[i-1] {
			t.Fatalf("sizes not strictly increasing: %v", got)
		}
	}
}

func TestR138DepthCurveUsesExactQuantityFee(t *testing.T) {
	levels := []storage.ResearchBookLevel{{Price: .40, Size: 2}, {Price: .50, Size: 3}}
	curve, ok := r138DepthCostFee([]float64{1, 2, 5}, levels, func(q, px float64) (float64, bool) {
		return q*q*.01 + px*.001, true
	})
	if !ok || len(curve) != 3 {
		t.Fatalf("curve=%+v ok=%v", curve, ok)
	}
	// Five shares consume two at 40c and three at 50c; fees are evaluated at each exact fill qty.
	wantCost := 2*.40 + 3*.50
	wantFee := 4*.01 + .40*.001 + 9*.01 + .50*.001
	if math.Abs(curve[2].Cost-wantCost) > 1e-12 || math.Abs(curve[2].Fee-wantFee) > 1e-12 {
		t.Fatalf("five-share curve=%+v want cost=%v fee=%v", curve[2], wantCost, wantFee)
	}
}

func TestR138GameSelectionIsDeterministicAndRequiresMultipleFamilies(t *testing.T) {
	groups := map[string][]storage.MarketGameRow{
		"one":   {{GameID: "one", MktType: "winner"}},
		"two":   {{GameID: "two", MktType: "winner"}, {GameID: "two", MktType: "spread"}},
		"three": {{GameID: "three", MktType: "winner"}, {GameID: "three", MktType: "total"}},
	}
	a := r138SelectGameGroups(groups, "slot", 10)
	b := r138SelectGameGroups(groups, "slot", 10)
	if len(a) != 2 || len(b) != 2 || a[0] != b[0] || a[1] != b[1] {
		t.Fatalf("selection a=%v b=%v", a, b)
	}
	for _, id := range a {
		if id == "one" {
			t.Fatal("single-family game entered score-state surface")
		}
	}
}

func TestR138DeadlineVoidVerificationFailsClosed(t *testing.T) {
	row := storage.VerifiedDeadlineRelation{VoidPolicy: "unknown until rules attach",
		EvidenceJSON: `{"void_payoff_verified":true}`}
	if r138DeadlineVoidVerified(row) {
		t.Fatal("unknown void policy treated as verified")
	}
	row.VoidPolicy = "canceled contracts return principal"
	row.EventEvidenceJSON = `{"void_payoff_verified":true}`
	if !r138DeadlineVoidVerified(row) {
		t.Fatal("explicit immutable void evidence not recognized")
	}
}

func TestR138RuntimeCoverageMapsExactlyAllNineteenSystems(t *testing.T) {
	rows := r138RuntimeCoverageContracts()
	if len(rows) != 19 {
		t.Fatalf("coverage rows=%d want 19", len(rows))
	}
	ids := make([]string, 0, len(rows))
	seen := map[string]bool{}
	for _, row := range rows {
		if seen[row.SystemID] {
			t.Fatalf("duplicate system %s", row.SystemID)
		}
		seen[row.SystemID] = true
		ids = append(ids, row.SystemID)
		if row.State == "BLOCKED" && len(row.Prerequisites) == 0 {
			t.Fatalf("blocked %s has no exact prerequisites", row.SystemID)
		}
		if row.State != "BLOCKED" && len(row.CollectorIDs) == 0 {
			t.Fatalf("collecting %s has no concrete collector", row.SystemID)
		}
	}
	sort.Strings(ids)
	if want := storage.ResearchExperimentIDs(); !reflect.DeepEqual(ids, want) {
		t.Fatalf("coverage ids=%v want=%v", ids, want)
	}
}

func TestR138RuntimeCoverageSweepPersistsNineteenTruthfulStates(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.EnsureR138Step6To9CollectorBlueprints(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	s.sweepR138SystemCoverage(context.Background())
	report, err := st.ResearchSystemRuntimeReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := report["reported_systems"].(int); got != 19 {
		t.Fatalf("reported systems=%d want 19: %+v", got, report)
	}
	counts := report["counts"].(map[string]int)
	// A fresh database has registered collector contracts but no runtime receipts. Effective
	// coverage must fail closed instead of repeating the static implementation intention.
	if counts["COLLECTING"] != 0 || counts["COLLECTING_PARTIAL"] != 0 || counts["BLOCKED"] != 19 {
		t.Fatalf("runtime states=%v", counts)
	}
	if report["funded"].(bool) || report["paper_authority"].(bool) || report["live_authority"].(bool) {
		t.Fatalf("runtime coverage acquired authority: %+v", report)
	}
}

func TestR139SystemSpecificFunnelsRotateOneDedicatedReceiptPerAdmission(t *testing.T) {
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	if err := st.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st}
	specs := storage.R139SystemFunnelSpecs()
	for i := range specs {
		s.sweepR139SystemSpecificFunnels(ctx)
		var soFar int
		if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*)
FROM research_collector_receipts WHERE collector_id LIKE '%-funnel'`).Scan(&soFar); err != nil {
			t.Fatal(err)
		}
		if soFar != i+1 {
			t.Fatalf("rotation admission %d persisted %d receipts; want exactly %d", i, soFar, i+1)
		}
	}
	var receipts, healthyEmpty int
	if err := st.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(status='healthy_empty'),0)
FROM research_collector_receipts WHERE collector_id LIKE '%-funnel'`).Scan(&receipts, &healthyEmpty); err != nil {
		t.Fatal(err)
	}
	if receipts != len(specs) || healthyEmpty != len(specs) {
		t.Fatalf("dedicated receipts=%d healthy_empty=%d want %d/%d", receipts, healthyEmpty, len(specs), len(specs))
	}
	state := s.r138CollectorState()
	state.mu.Lock()
	active, next, completed := state.systemFunnelCycleActive, state.systemFunnelNext, state.lastSystemFunnels
	state.mu.Unlock()
	if active || next != 0 || completed.IsZero() {
		t.Fatalf("completed rotation active=%t next=%d completed=%v", active, next, completed)
	}
}

func TestR138RuntimeCoverageUsesFreshCollectorHealth(t *testing.T) {
	contracts := []storage.ResearchSystemRuntimeReceipt{
		{SystemID: "proper-score-executor", State: "COLLECTING", Reason: "implemented", CollectorIDs: []string{"proper-score"}},
		{SystemID: "score-state-surface", State: "COLLECTING_PARTIAL", Reason: "partial", CollectorIDs: []string{"score-state-surface", "missing"}},
	}
	views := []storage.CollectorLivenessView{
		{CollectorID: "proper-score", Status: "healthy", NeverRan: false, Alert: false},
		{CollectorID: "score-state-surface", Status: "healthy_empty", NeverRan: false, Alert: false},
	}
	got := r138RuntimeContractsWithLiveness(contracts, views)
	if got[0].State != "COLLECTING" || got[1].State != "COLLECTING_PARTIAL" || len(got[1].Prerequisites) != 1 {
		t.Fatalf("effective liveness=%+v", got)
	}
	views[0].Status, views[0].Alert = "error", true
	got = r138RuntimeContractsWithLiveness(contracts[:1], views)
	if got[0].State != "BLOCKED" || !strings.Contains(got[0].Reason, "proper-score=ERROR") {
		t.Fatalf("erroring collector remained collecting: %+v", got[0])
	}
}

func TestR138PolyUSOutcomeSetHookIsReachableFromCertifiedLoop(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	raw, err := os.ReadFile(strings.TrimSuffix(thisFile, "r138_system_collectors_test.go") + "researchsystems.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	needle := "s.captureR138PolyUSOutcomeSet(ctx, set, now)"
	selection := strings.Index(source, "sets := completePolyUSDutchSets(markets)")
	loop := strings.Index(source, "for _, set := range sets")
	hook := strings.Index(source, needle)
	if selection < 0 || loop < selection || hook < loop || hook-loop > 600 {
		t.Fatalf("PolyUS certified-set collector hook missing/unreachable: selection=%d loop=%d hook=%d", selection, loop, hook)
	}
}

func TestR138VenueNoticeWatcherOutlivesCanceledTickContext(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	watcher, cancelWatcher := venueNoticeRunContext(parent)
	defer cancelWatcher()
	cancelParent()
	select {
	case <-watcher.Done():
		t.Fatalf("venue notice watcher inherited the scheduler tick cancellation: %v", watcher.Err())
	default:
	}
}

func TestR138VenueNoticeWatcherStartsBeforeSynchronousCollectors(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	raw, err := os.ReadFile(strings.TrimSuffix(thisFile, "r138_system_collectors_test.go") + "researchsystems.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	liveStart := strings.Index(source, "func (s *Server) researchLivenessTick")
	systemStart := strings.Index(source, "func (s *Server) researchSystemsTick")
	if liveStart < 0 || systemStart < liveStart {
		t.Fatal("dedicated research liveness/system tick bounds missing")
	}
	liveness := source[liveStart:systemStart]
	notices := strings.Index(liveness, "if doVenueNotices {")
	sourceClocks := strings.Index(liveness, "s.sweepResearchSourceClocks(ctx)")
	replay := strings.Index(liveness, "s.captureResearchReplay(ctx)")
	if notices < 0 || sourceClocks < 0 || replay < 0 || notices > sourceClocks || sourceClocks > replay {
		t.Fatalf("dedicated watcher/source/replay ordering missing: notices=%d clocks=%d replay=%d", notices, sourceClocks, replay)
	}
	systems := source[systemStart:]
	systemsEnd := strings.Index(systems, "func (s *Server) researchLifecycleTick")
	evidenceStart := strings.Index(systems, "func (s *Server) researchEvidenceTick")
	if systemsEnd < 0 || evidenceStart < systemsEnd {
		t.Fatal("dedicated lifecycle/evidence tick bounds missing")
	}
	systems = systems[:systemsEnd]
	heavy := strings.Index(systems, "s.sweepR138Step6To9(ctx)")
	queue := strings.Index(systems, "s.sweepQueuePriority(ctx)")
	promotion := strings.Index(systems, "s.sweepResearchPromotionDispatch(ctx, now)")
	rotation := strings.Index(systems, "s.runNextResearchSystemHeavy(ctx, now)")
	terminalInSystems := strings.Index(systems, "sweepNativeTerminalSettlementsIfDue")
	lifecycleInSystems := strings.Index(systems, "s.sweepLifecycleReopen(ctx)")
	if queue < 0 || promotion < 0 || rotation < 0 || queue > rotation ||
		promotion > rotation || lifecycleInSystems >= 0 || terminalInSystems >= 0 || heavy >= 0 {
		t.Fatalf("fresh/settlement isolation missing: lifecycle-in-systems=%d promotion=%d queue=%d terminal-in-systems=%d rotation=%d evidence-heavy=%d",
			lifecycleInSystems, promotion, queue, terminalInSystems, rotation, heavy)
	}
	rotationRaw, err := os.ReadFile(strings.TrimSuffix(thisFile, "r138_system_collectors_test.go") + "researchsystems_rotation.go")
	if err != nil || !strings.Contains(string(rotationRaw), "s.sweepEventBasketLock(ctx)") ||
		!strings.Contains(string(rotationRaw), "s.sweepR138SystemCoverageIfDue(ctx, now)") {
		t.Fatal("basket/coverage collectors are missing from the bounded heavy rotation")
	}
	evidenceGlobal := strings.Index(source, "func (s *Server) researchEvidenceTick")
	timingStart := strings.Index(source, "func (s *Server) researchEvidenceTimingTick")
	if timingStart < 0 || evidenceGlobal < 0 || timingStart > evidenceGlobal {
		t.Fatal("independent RAM timing tick missing before heavy evidence tick")
	}
	timing := source[timingStart:evidenceGlobal]
	if !strings.Contains(timing, "s.advanceStep7TimedEvidence(time.Now())") ||
		strings.Contains(timing, "tryRunHeavyResearch") || strings.Contains(timing, "sweepR138Step6To9") {
		t.Fatal("RAM timing tick is not isolated from SQLite/heavy research")
	}
	evidence := source[evidenceGlobal:]
	if !strings.Contains(evidence, "s.sweepR138Step6To9(ctx)") {
		t.Fatal("dedicated evidence tick does not run Step6To9")
	}
	if strings.Contains(evidence, "s.advanceStep7TimedEvidence(time.Now())") {
		t.Fatal("RAM timing advance still shares the potentially blocking heavy scheduler")
	}
	serverRaw, err := os.ReadFile(strings.TrimSuffix(thisFile, "r138_system_collectors_test.go") + "server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(serverRaw), `loop("research-liveness", 5*time.Second, 90*time.Second, s.researchLivenessTick)`) {
		t.Fatal("watcher/source/replay liveness lacks its own bounded scheduler")
	}
	if !strings.Contains(string(serverRaw), `loopAfter("research-lifecycle", 2*time.Second, 2*time.Second, 5*time.Second, s.researchLifecycleTick)`) {
		t.Fatal("lifecycle fixed horizons lack an independent bounded serial scheduler")
	}
	if !strings.Contains(string(serverRaw), `loopAfter("research-evidence", 9*time.Second, 5*time.Second, 90*time.Second, s.researchEvidenceTick)`) {
		t.Fatal("heavy research evidence lacks its own bounded scheduler")
	}
	if !strings.Contains(string(serverRaw), `loopAfter("research-evidence-timing", 4*time.Second, 5*time.Second, 2*time.Second, s.researchEvidenceTimingTick)`) {
		t.Fatal("RAM-clocked horizon sampling lacks its own short scheduler")
	}
	if !strings.Contains(string(serverRaw), `loopAfter("research-terminal-settlement", 11*time.Second, 5*time.Second, 90*time.Second, s.researchTerminalSettlementTick)`) {
		t.Fatal("terminal settlement lacks its own ungated bounded scheduler")
	}
}

func TestR138FoundationScanHasDedicatedScheduler(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	dir := strings.TrimSuffix(thisFile, "r138_system_collectors_test.go")
	researchRaw, err := os.ReadFile(dir + "researchsystems.go")
	if err != nil {
		t.Fatal(err)
	}
	research := string(researchRaw)
	start := strings.Index(research, "func (s *Server) researchSystemsTick")
	if start < 0 {
		t.Fatal("research-systems tick start missing")
	}
	end := strings.Index(research[start:], "func (s *Server) handleResearchSystems")
	if end < 0 {
		t.Fatal("research-systems tick bounds missing")
	}
	tick := research[start : start+end]
	if strings.Contains(tick, "s.sweepResearchFoundation(ctx)") || strings.Contains(tick, "s.sweepCatalogFoundation(ctx)") {
		t.Fatal("production-size foundation scan can still starve source-clock/replay liveness")
	}
	serverRaw, err := os.ReadFile(dir + "server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(serverRaw), `loopAfter("research-foundation", 37*time.Second, 30*time.Second, 4*time.Minute, s.researchFoundationTick)`) ||
		!strings.Contains(research, "func (s *Server) researchFoundationTick") {
		t.Fatal("foundation scan lacks its own single-goroutine bounded scheduler")
	}
}

func TestR138OneTickCannotDuplicateIncentiveCrawl(t *testing.T) {
	var calls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/incentive_programs" {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"incentive_programs":[],"next_cursor":""}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer api.Close()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.EnsureR138Step6To9CollectorBlueprints(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := &Server{kal: kalshi.NewClient(api.URL, nil, 1000, time.Second), store: st,
		kmkts: map[string]kalshi.Market{}, kmktsAt: map[string]time.Time{}}
	ctx := context.Background()
	// This is the central tick order: the cadence-owned enhanced crawl runs once, then the
	// internally throttled Step6-9 sweep runs. The latter must never contain a second crawl.
	s.sweepR138IncentiveEconomics(ctx)
	s.sweepR138Step6To9(ctx)
	if got := calls.Load(); got != 1 {
		t.Fatalf("one tick issued %d incentive requests, want exactly 1", got)
	}
	report, err := st.CollectorLivenessReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	collectors := report["collectors"].([]storage.CollectorLivenessView)
	byID := map[string]storage.CollectorLivenessView{}
	for _, row := range collectors {
		byID[row.CollectorID] = row
	}
	legacy, enhanced := byID["incentive-maker"], byID["incentive-economics"]
	if legacy.LifetimeCycles != 1 || enhanced.LifetimeCycles != 1 || legacy.CompletedTS == "" ||
		legacy.CompletedTS != enhanced.CompletedTS || legacy.Status != "healthy_empty" || enhanced.Status != "healthy_empty" {
		t.Fatalf("one snapshot must feed matching receipts: legacy=%+v enhanced=%+v", legacy, enhanced)
	}
	polyUS := byID["polyus-incentive-economics"]
	if polyUS.LifetimeCycles != 1 || polyUS.Status != "blocked" || polyUS.CompletedTS == "" ||
		polyUS.ErrorClass != "client" {
		t.Fatalf("missing PolyUS client must fail closed without hiding the collector: %+v", polyUS)
	}
}

func TestR138IncentiveProgramRotationIsBoundedAndComplete(t *testing.T) {
	programs := []kalshi.IncentiveProgram{
		{ID: "5", MarketTicker: "E"}, {ID: "2", MarketTicker: "B"},
		{ID: "4", MarketTicker: "D"}, {ID: "1", MarketTicker: "A"},
		{ID: "3", MarketTicker: "C"},
	}
	cursor := 0
	seen := map[string]bool{}
	for cycle := 0; cycle < 3; cycle++ {
		var selected []kalshi.IncentiveProgram
		selected, cursor = r138RotateIncentivePrograms(programs, cursor, 2)
		if len(selected) != 2 {
			t.Fatalf("cycle %d selected=%d, want cap 2", cycle, len(selected))
		}
		for _, program := range selected {
			seen[program.MarketTicker] = true
		}
	}
	if len(seen) != len(programs) {
		t.Fatalf("rotation did not cover full universe: seen=%v cursor=%d", seen, cursor)
	}
	if programs[0].MarketTicker != "E" {
		t.Fatal("selection mutated the venue response order")
	}
}

func TestR144PolyUSIncentiveRefreshRetainsOnlyRecentCompleteLastGood(t *testing.T) {
	s := &Server{}
	defer func() {
		r138CollectorStates.Lock()
		delete(r138CollectorStates.byServer, s)
		r138CollectorStates.Unlock()
	}()
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	input := []polymarketus.IncentiveMarket{{MarketSlug: "market",
		TimePeriods: []polymarketus.IncentivePeriod{{ProgramID: "program", Status: "active"}}}}
	snapshot, lastGoodAt, fallback := s.r144PolyUSIncentiveSnapshot(input, now, true)
	if fallback || !lastGoodAt.Equal(now) || len(snapshot) != 1 {
		t.Fatalf("initial snapshot=%+v at=%v fallback=%v", snapshot, lastGoodAt, fallback)
	}
	input[0].TimePeriods[0].ProgramID = "mutated"
	snapshot, _, fallback = s.r144PolyUSIncentiveSnapshot(nil, now.Add(30*time.Minute), false)
	if !fallback || len(snapshot) != 1 || snapshot[0].TimePeriods[0].ProgramID != "program" {
		t.Fatalf("recent fallback was not isolated: %+v fallback=%v", snapshot, fallback)
	}
	snapshot, _, fallback = s.r144PolyUSIncentiveSnapshot(nil, now.Add(r144PolyUSIncentiveLastGoodAge+time.Second), false)
	if fallback || snapshot != nil {
		t.Fatalf("expired last-good escaped: %+v fallback=%v", snapshot, fallback)
	}
}

func TestR144NBMLastGoodStatusExposesAgeAndGuard(t *testing.T) {
	now := time.Date(2026, 7, 14, 18, 0, 0, 0, time.UTC)
	s := &Server{}
	s.cacheWeatherNBMForecasts([]nbm.StationForecast{{Station: "KORD", Run: now.Add(-2 * time.Hour), ArtifactURL: "fixture"}})
	status := s.r144NBMLastGoodStatus(now)
	if status["forecasts"] != 1 || status["usable_under_12h_research_guard"] != true ||
		status["age_seconds"].(float64) != (2*time.Hour).Seconds() {
		t.Fatalf("fresh status=%+v", status)
	}
	stale := &Server{}
	stale.cacheWeatherNBMForecasts([]nbm.StationForecast{{Station: "KORD", Run: now.Add(-13 * time.Hour), ArtifactURL: "fixture"}})
	if got := stale.r144NBMLastGoodStatus(now); got["usable_under_12h_research_guard"] != false {
		t.Fatalf("stale NBM cache advertised usable: %+v", got)
	}
}

func TestR143MissingCanonicalInstrumentClassifiesOnlyIdentityJoinLag(t *testing.T) {
	if !r143MissingCanonicalInstrument(errors.New("no current canonical instrument/event/payoff version for polyus ABC")) {
		t.Fatal("canonical identity join lag was not classified")
	}
	for _, err := range []error{nil, errors.New("database is locked"), errors.New("fee schedule unavailable")} {
		if r143MissingCanonicalInstrument(err) {
			t.Fatalf("unrelated error misclassified as identity join lag: %v", err)
		}
	}
}

func TestR139OfficialReleaseSpecsUseValidatedExactAdapters(t *testing.T) {
	deribitSpec, ok := storage.R138Step8SourceClockSpec("deribit-option-summary")
	if !ok || deribitSpec.Version != 3 || deribitSpec.ClockKind != "source_timestamp" ||
		deribitSpec.TimestampField != "usOut" || deribitSpec.SchemaVersion != "deribit-option-threshold-surface-v3" {
		t.Fatalf("Deribit exact-clock spec=%+v ok=%v", deribitSpec, ok)
	}
	nbmSpec, ok := storage.R138SourceClockSpec("noaa-nbm")
	if !ok || nbmSpec.Version != 2 || !nbmSpec.Active || nbmSpec.ClockKind != "source_timestamp" ||
		nbmSpec.TimestampField != "station bulletin NBM run UTC" || nbmSpec.SchemaVersion != "nbm-probabilistic-text-v2" {
		t.Fatalf("NBM exact-clock spec=%+v ok=%v", nbmSpec, ok)
	}
	found := false
	for _, spec := range storage.R138Step6To9CollectorSpecs() {
		if spec.CollectorID == "official-release-adapters" {
			found = spec.Version == 3 && strings.Contains(spec.Source, "NOAA NBM") &&
				strings.Contains(spec.Source, "risk-neutral threshold surfaces") &&
				!strings.Contains(strings.ToLower(spec.ZeroPolicy), "blocked until")
		}
	}
	if !found {
		t.Fatal("official-release collector v3 contract missing")
	}
}

func TestR138BasketAndNestedCollectorsBlockWhenSourcesUnavailable(t *testing.T) {
	s := testServer(t)
	s.sweepEventBasketLock(context.Background())
	report, err := s.store.CollectorLivenessReport(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rows := report["collectors"].([]storage.CollectorLivenessView)
	want := map[string]bool{"event-basket-lock": true, "nested-ladder-lock": true}
	for _, row := range rows {
		if !want[row.CollectorID] {
			continue
		}
		if row.NeverRan || row.Status != "blocked" || row.ErrorClass != "source_not_ready" || row.ErrorText == "" {
			t.Fatalf("collector %s falsely reported source-unavailable startup as healthy: %+v", row.CollectorID, row)
		}
		delete(want, row.CollectorID)
	}
	if len(want) != 0 {
		t.Fatalf("missing collector receipts: %v", want)
	}
}

func TestR145NestedCollectorDoesNotReplaceTruthDuringCatalogWarmup(t *testing.T) {
	s := testServer(t)
	// The venue client is installed, but its catalog has not completed the first crawl. This is a
	// normal boot phase rather than evidence that the nested-ladder source is unavailable.
	s.kal = kalshi.NewClient("http://127.0.0.1:1", nil, 1, time.Millisecond)
	s.sweepEventBasketLock(context.Background())
	var receipts int
	if err := s.store.DBForTest().QueryRowContext(context.Background(), `SELECT COUNT(*)
FROM research_collector_receipts WHERE collector_id='nested-ladder-lock'`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 {
		t.Fatalf("catalog warm-up persisted %d false nested-ladder receipt(s)", receipts)
	}
}

func r166SetResearchCompleteBoard(s *Server, markets []kalshi.Market, observedAt time.Time) {
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	rotation.completeBoardSnapshotForTest = func() ([]kalshi.Market, time.Time) {
		return markets, observedAt
	}
	rotation.Unlock()
}

func TestR166ReadyCatalogWithoutDueEventsReceiptsDerivedCollectors(t *testing.T) {
	s := testServer(t)
	if err := s.store.EnsureR138Step6To9CollectorBlueprints(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.kal = kalshi.NewClient("http://127.0.0.1:1", nil, 1, time.Millisecond)
	now := time.Now()
	r166SetResearchCompleteBoard(s, []kalshi.Market{{Ticker: "ONLY", EventTicker: "ONE-MEMBER", Status: "active"}}, now)

	s.sweepEventBasketLock(context.Background())
	for _, collector := range []string{"outcome-set-surface", "payoff-envelope-capacity"} {
		var status, zeroReason string
		var expectedZero int
		if err := s.store.DBForTest().QueryRowContext(context.Background(), `SELECT status,expected_zero,zero_reason
FROM research_collector_receipts WHERE collector_id=? ORDER BY id DESC LIMIT 1`, collector).
			Scan(&status, &expectedZero, &zeroReason); err != nil {
			t.Fatalf("%s cycle receipt: %v", collector, err)
		}
		if status != "healthy_empty" || expectedZero != 1 || !strings.Contains(zeroReason, "catalog was ready") {
			t.Fatalf("%s status=%q expected_zero=%d reason=%q", collector, status, expectedZero, zeroReason)
		}
	}
}

func TestR166IneligibleFetchedSnapshotTerminalsDerivedCollectors(t *testing.T) {
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"event":{"event_ticker":"EVENT","mutually_exclusive":true,"markets":[{"ticker":"ONLY","status":"active"}]}}`)
	}))
	defer venue.Close()
	s := testServer(t)
	if err := s.store.EnsureR138Step6To9CollectorBlueprints(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.kal = kalshi.NewClient(venue.URL, nil, 1, time.Second)
	r166SetResearchCompleteBoard(s, []kalshi.Market{
		{Ticker: "A", EventTicker: "EVENT", Status: "active"},
		{Ticker: "B", EventTicker: "EVENT", Status: "active"},
	}, now)

	s.sweepEventBasketLock(context.Background())
	for _, collector := range []string{"outcome-set-surface", "payoff-envelope-capacity"} {
		var status, zeroReason string
		if err := s.store.DBForTest().QueryRowContext(context.Background(), `SELECT status,zero_reason
FROM research_collector_receipts WHERE collector_id=? ORDER BY id DESC LIMIT 1`, collector).
			Scan(&status, &zeroReason); err != nil {
			t.Fatalf("%s cycle receipt: %v", collector, err)
		}
		if status != "healthy_empty" || !strings.Contains(zeroReason, "none returned at least two") {
			t.Fatalf("%s status=%q reason=%q", collector, status, zeroReason)
		}
	}
}

func TestR166PartialSnapshotFailureMakesAggregateNonHealthy(t *testing.T) {
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "EVENT-A") {
			fmt.Fprint(w, `{"event":{"event_ticker":"EVENT-A","mutually_exclusive":true,"markets":[{"ticker":"A1","status":"active"},{"ticker":"A2","status":"active"}]}}`)
			return
		}
		http.Error(w, `{"error":"temporary source failure"}`, http.StatusServiceUnavailable)
	}))
	defer venue.Close()
	s := testServer(t)
	if err := s.store.EnsureR138Step6To9CollectorBlueprints(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	s.kal = kalshi.NewClient(venue.URL, nil, 10, time.Second)
	r166SetResearchCompleteBoard(s, []kalshi.Market{
		{Ticker: "A1", EventTicker: "EVENT-A", Status: "active"},
		{Ticker: "A2", EventTicker: "EVENT-A", Status: "active"},
		{Ticker: "B1", EventTicker: "EVENT-B", Status: "active"},
		{Ticker: "B2", EventTicker: "EVENT-B", Status: "active"},
	}, now)

	s.sweepEventBasketLock(context.Background())
	for _, collector := range []string{"outcome-set-surface", "payoff-envelope-capacity"} {
		var status, errorClass, exclusions string
		var attempted int
		if err := s.store.DBForTest().QueryRowContext(context.Background(), `SELECT status,error_class,attempted,exclusions_json
FROM research_collector_receipts WHERE collector_id=? ORDER BY id DESC LIMIT 1`, collector).
			Scan(&status, &errorClass, &attempted, &exclusions); err != nil {
			t.Fatalf("%s aggregate receipt: %v", collector, err)
		}
		if status != "error" || errorClass != "source_api" || attempted != 2 ||
			!strings.Contains(exclusions, "event_snapshot_error") {
			t.Fatalf("%s status=%q class=%q attempted=%d exclusions=%s", collector, status, errorClass, attempted, exclusions)
		}
	}
}

func TestR166EventBasketDueClockWaitsForCatalogMetadata(t *testing.T) {
	s := testServer(t)
	s.kal = kalshi.NewClient("http://127.0.0.1:1", nil, 1, time.Millisecond)
	now := time.Now()
	if s.researchSystemsPrimaryLaneDue("event-basket", now) {
		t.Fatal("event-basket became due before the authenticated metadata crawl produced a row")
	}
	s.metaMu.Lock()
	s.kmkts = map[string]kalshi.Market{"READY": {Ticker: "READY", EventTicker: "EVENT", Status: "active"}}
	s.kmktsAt = map[string]time.Time{"READY": now}
	s.metaMu.Unlock()
	if s.researchSystemsPrimaryLaneDue("event-basket", now) {
		t.Fatal("a targeted kmkts row falsely stood in for a completed board generation")
	}
	r166SetResearchCompleteBoard(s, []kalshi.Market{{Ticker: "STALE", EventTicker: "EVENT", Status: "active"}},
		now.Add(-r159KalshiMarketCacheMaxAge-time.Second))
	if s.researchSystemsPrimaryLaneDue("event-basket", now) {
		t.Fatal("a stale completed board generation was accepted")
	}
	r166SetResearchCompleteBoard(s, []kalshi.Market{{Ticker: "READY", EventTicker: "EVENT", Status: "active"}}, now)
	if !s.researchSystemsPrimaryLaneDue("event-basket", now) {
		t.Fatal("event-basket did not become due after a fresh completed board generation arrived")
	}
}

func TestR166ActiveCancellationRestoresEventBasketDueClock(t *testing.T) {
	entered := make(chan struct{})
	var first atomic.Bool
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first.CompareAndSwap(false, true) {
			close(entered)
		}
		<-r.Context().Done()
	}))
	defer venue.Close()

	s := testServer(t)
	if err := s.store.EnsureR138Step6To9CollectorBlueprints(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s.kal = kalshi.NewClient(venue.URL, nil, 1, time.Second)
	board := make([]kalshi.Market, 0, 24)
	for i := 0; i < 12; i++ {
		event := fmt.Sprintf("EVENT-%02d", i)
		board = append(board,
			kalshi.Market{Ticker: event + "-A", EventTicker: event, Status: "active"},
			kalshi.Market{Ticker: event + "-B", EventTicker: event, Status: "active"})
	}
	r166SetResearchCompleteBoard(s, board, now)
	for _, receipt := range []storage.CollectorReceipt{
		{CollectorID: "outcome-set-surface", CycleID: "prior-outcome", ExperimentID: "outcome-set-expansion-shock",
			ExperimentVersion: 1, Status: "healthy_empty", Started: now.Add(-time.Minute), Completed: now.Add(-time.Minute),
			ExpectedZero: true, ZeroReason: "prior truthful receipt", Source: "official venue event membership + current side-specific books",
			SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 2 * time.Minute,
			Systems: []string{"outcome-set-expansion-shock", "payoff-constraint-solver"}},
		{CollectorID: "payoff-envelope-capacity", CycleID: "prior-payoff", ExperimentID: "payoff-constraint-solver",
			ExperimentVersion: 1, Status: "healthy_empty", Started: now.Add(-time.Minute), Completed: now.Add(-time.Minute),
			ExpectedZero: true, ZeroReason: "prior truthful receipt", Source: "verified payoff certificate + actual depth levels + exact per-level route fees",
			SchemaVersion: storage.R138EvidenceSchemaVersion(), ExpectedCadence: 2 * time.Minute,
			Systems: []string{"payoff-constraint-solver", "deadline-hazard-surface", "incentive-subsidized-structural-lock"}},
	} {
		if _, err := s.store.InsertCollectorReceipt(context.Background(), receipt); err != nil {
			t.Fatal(err)
		}
	}
	latent := s.r139LatentSchedule()
	latent.Lock()
	latent.registered, latent.lastOutcome, latent.lastScore = true, now, now
	latent.Unlock()
	collectors := s.r138CollectorState()
	collectors.mu.Lock()
	collectors.lastSystemFunnels, collectors.lastCoverage = now, now
	collectors.mu.Unlock()
	native := s.nativeLockScheduler()
	native.Lock()
	native.lastTimeNested, native.lastJointMarginal, native.lastFeeRounding = now, now, now
	native.Unlock()
	concrete := s.concreteSchedule()
	concrete.Lock()
	concrete.last = now
	concrete.Unlock()
	s.researchMu.Lock()
	s.lastBasketResearchAt = time.Time{}
	s.lastProperScoreAt, s.lastIncentiveResearchAt = now, now
	priorCooldown := now.Add(-6 * time.Minute)
	s.researchBasketSeen = map[string]time.Time{"EVENT-03": priorCooldown}
	s.researchBasketCursor = 3
	s.researchMu.Unlock()
	rotation := s.researchSystemsRotationRuntime()
	eventBasketIdx := -1
	rotation.Lock()
	for i, lane := range researchSystemHeavyLaneSpecs {
		if lane.name == "event-basket" {
			eventBasketIdx = i
			rotation.next = i
			break
		}
	}
	rotation.Unlock()
	if eventBasketIdx < 0 {
		t.Fatal("event-basket lane missing from systems rotation")
	}

	done := make(chan bool, 1)
	go func() { done <- s.runNextResearchSystemHeavy(context.Background(), now) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("event-basket REST request did not start")
	}
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveMu.Unlock()
	select {
	case completed := <-done:
		if completed {
			t.Fatal("LIVE-canceled event-basket lane reported completion")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("LIVE-canceled event-basket lane did not return")
	}
	s.liveMu.Lock()
	s.liveArmed = false
	s.liveMu.Unlock()
	s.researchMu.Lock()
	stamp := s.lastBasketResearchAt
	restoredCooldown := s.researchBasketSeen["EVENT-03"]
	restoredCursor := s.researchBasketCursor
	s.researchMu.Unlock()
	if !stamp.IsZero() {
		t.Fatalf("canceled event-basket due clock was not restored: %v", stamp)
	}
	if !restoredCooldown.Equal(priorCooldown) {
		t.Fatalf("canceled event-basket cooldown=%v want prior %v", restoredCooldown, priorCooldown)
	}
	if restoredCursor != 3 {
		t.Fatalf("canceled event-basket cursor=%d want prior 3", restoredCursor)
	}
	rotation.Lock()
	next := rotation.next
	retryAt := rotation.retryAt["event-basket"]
	failures := rotation.failures["event-basket"]
	rotation.Unlock()
	if wantNext := (eventBasketIdx + 1) % len(researchSystemHeavyLaneSpecs); next != wantNext {
		t.Fatalf("canceled event-basket left rotation pinned at %d; got next=%d want %d",
			eventBasketIdx, next, wantNext)
	}
	if failures != 1 || !retryAt.After(time.Now()) {
		t.Fatalf("canceled event-basket retry failures=%d retry_at=%v; want one future backoff",
			failures, retryAt)
	}
	for collector, wantCycle := range map[string]string{
		"outcome-set-surface": "prior-outcome", "payoff-envelope-capacity": "prior-payoff",
	} {
		var cycle, status string
		if err := s.store.DBForTest().QueryRowContext(context.Background(), `SELECT cycle_id,status
FROM research_collector_receipts WHERE collector_id=? ORDER BY id DESC LIMIT 1`, collector).Scan(&cycle, &status); err != nil {
			t.Fatal(err)
		}
		if cycle != wantCycle || status != "healthy_empty" {
			t.Fatalf("%s cancellation replaced prior receipt: cycle=%q status=%q", collector, cycle, status)
		}
	}
}
