package server

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
)

func TestHeavyResearchDefersAtEmergencyWALRailThenResumes(t *testing.T) {
	s := testServer(t)
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	s.mutateCfg(func(c *config.Config) { c.DataDir = dir })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(wal, researchWALEmergencyBytes+1); err != nil {
		t.Fatal(err)
	}
	var ran atomic.Bool
	if s.tryRunHeavyResearch(context.Background(), "wal-pressure", 0, func(context.Context) { ran.Store(true) }) {
		t.Fatal("emergency-size WAL admitted a new heavy research reader")
	}
	if ran.Load() {
		t.Fatal("deferred heavy research callback ran")
	}
	if pressured, bytes := researchWALUnderPressure(dir); !pressured || bytes != researchWALEmergencyBytes+1 {
		t.Fatalf("WAL pressure=%t bytes=%d", pressured, bytes)
	}
	// With no fresh checkpoint receipt, a retained 256 MiB shell is below the real emergency rail.
	if err := os.Truncate(wal, researchWALPressureBytes+1); err != nil {
		t.Fatal(err)
	}
	if !s.tryRunHeavyResearch(context.Background(), "wal-pressure", 0, func(context.Context) { ran.Store(true) }) {
		t.Fatal("heavy research did not resume below the stale-receipt emergency rail")
	}
	if !ran.Load() {
		t.Fatal("resumed heavy research callback did not run")
	}
}

func TestFreshWALCheckpointDebtIgnoresRetainedShell(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// This models the production receipt: SQLite retained the 256 MiB shell, 4,995/5,033
	// frames were checkpointed, then one small write extended the physical file.
	const extension = int64(4 << 10)
	if err := os.Truncate(wal, researchWALPressureBytes); err != nil {
		t.Fatal(err)
	}
	publishResearchWALCheckpointReceipt(dir, time.Now(), 0, 5033, 4995, nil)
	if err := os.Truncate(wal, researchWALPressureBytes+extension); err != nil {
		t.Fatal(err)
	}
	if pressured, bytes := researchWALUnderPressure(dir); pressured ||
		bytes != researchWALPressureBytes+extension {
		t.Fatalf("retained shell pressure=%t bytes=%d", pressured, bytes)
	}
}

func TestFreshWALCheckpointDebtRejectsBusyAndTrueHighDebt(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(wal, researchWALPressureBytes); err != nil {
		t.Fatal(err)
	}

	publishResearchWALCheckpointReceipt(dir, time.Now(), 1, -1, -1, nil)
	if pressured, _ := researchWALUnderPressure(dir); !pressured {
		t.Fatal("fresh busy checkpoint receipt did not block heavy research")
	}

	frameBytes := researchWALDefaultPageBytes + 24
	highFrames := int(researchWALPressureBytes/frameBytes) + 2
	publishResearchWALCheckpointReceipt(dir, time.Now(), 0, highFrames, 0, nil)
	if pressured, _ := researchWALUnderPressure(dir); !pressured {
		t.Fatal("fresh high checkpoint debt did not block heavy research")
	}
}

func TestFreshBusyWALCheckpointUsesValidLogicalDebt(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(wal, 229<<20); err != nil {
		t.Fatal(err)
	}

	// PASSIVE may be busy because the admitted heavy reader pins the checkpoint horizon while
	// still returning trustworthy frame counts. Thirty-eight outstanding 4 KiB frames are nowhere
	// near the 256 MiB logical-debt rail and must not cancel that reader.
	publishResearchWALCheckpointReceipt(dir, time.Now(), 1, 5033, 4995, nil)
	if pressured, bytes := researchWALUnderPressure(dir); pressured || bytes != 229<<20 {
		t.Fatalf("valid low-debt busy receipt pressure=%t bytes=%d", pressured, bytes)
	}

	frameBytes := researchWALDefaultPageBytes + 24
	highFrames := int(researchWALPressureBytes/frameBytes) + 2
	publishResearchWALCheckpointReceipt(dir, time.Now(), 1, highFrames, 0, nil)
	if pressured, _ := researchWALUnderPressure(dir); !pressured {
		t.Fatal("valid busy receipt with true high logical debt did not block heavy research")
	}
}

func TestHeavyAdmissionRefreshesLogicalDebtInsideRetainedShell(t *testing.T) {
	s := testServer(t)
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	s.mutateCfg(func(c *config.Config) { c.DataDir = dir })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(wal, researchWALPressureBytes); err != nil {
		t.Fatal(err)
	}
	// A prior clean receipt and unchanged physical length cannot prove that SQLite did not reuse
	// frames inside the retained shell. Production admission must ask for current logical truth.
	publishResearchWALCheckpointReceipt(dir, time.Now(), 0, 1, 1, nil)
	gate := researchWorkCoordinatorFor(s)
	var probes atomic.Int64
	gate.mu.Lock()
	gate.walProbe = func(context.Context, int64, bool) (bool, int64) {
		probes.Add(1)
		return true, researchWALPressureBytes + 4096
	}
	gate.mu.Unlock()
	var ran atomic.Bool
	if s.tryRunHeavyResearch(context.Background(), "hidden-reused-wal", 0,
		func(context.Context) { ran.Store(true) }) {
		t.Fatal("fresh logical high debt inside retained shell was admitted")
	}
	if probes.Load() != 1 || ran.Load() {
		t.Fatalf("logical probes=%d ran=%t, want one forced probe and no admission",
			probes.Load(), ran.Load())
	}
}

func TestStaleWALReceiptFailsClosedAboveOrdinaryRail(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(wal, researchWALPressureBytes+1); err != nil {
		t.Fatal(err)
	}
	publishResearchWALCheckpointReceipt(dir,
		time.Now().Add(-researchWALCheckpointReceiptFresh-time.Second), 0, 1, 1, nil)
	if pressured, _ := researchWALUnderPressure(dir); !pressured {
		t.Fatal("stale logical receipt dismissed a WAL above the ordinary rail")
	}
}

func TestWALCheckpointSweepPublishesFreshDebtReceipt(t *testing.T) {
	s := testServer(t)
	dir := s.cfg().DataDir
	clearResearchWALCheckpointReceipt(dir)
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })

	s.sweepWALCheckpoint(context.Background())
	value, ok := researchWALCheckpointReceipts.Load(researchWALReceiptKey(dir))
	if !ok {
		t.Fatal("checkpoint sweep did not publish its debt receipt")
	}
	receipt := value.(researchWALCheckpointReceipt)
	if !receipt.Valid || receipt.Busy || time.Since(receipt.ObservedAt) > time.Second {
		t.Fatalf("checkpoint receipt=%+v", receipt)
	}
}

func TestHeavyResearchDefersWhileLiveArmedThenResumes(t *testing.T) {
	s := testServer(t)
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveMu.Unlock()
	var ran atomic.Bool
	if s.tryRunHeavyResearch(context.Background(), "live-armed", 0, func(context.Context) { ran.Store(true) }) {
		t.Fatal("LIVE-armed server admitted heavy research")
	}
	if ran.Load() {
		t.Fatal("deferred LIVE-armed research callback ran")
	}
	s.liveMu.Lock()
	s.liveArmed = false
	s.liveMu.Unlock()
	if !s.tryRunHeavyResearch(context.Background(), "live-armed", 0, func(context.Context) { ran.Store(true) }) {
		t.Fatal("heavy research did not resume after LIVE disarmed")
	}
	if !ran.Load() {
		t.Fatal("resumed research callback did not run")
	}
}

func TestActiveHeavyResearchCancelsWhenLiveArmsAndKeepsTokenUntilReturn(t *testing.T) {
	s := testServer(t)
	gate := researchWorkCoordinatorFor(s)
	gate.mu.Lock()
	gate.walPressurePeriod = 5 * time.Millisecond
	gate.mu.Unlock()

	entered := make(chan struct{})
	canceled := make(chan struct{})
	allowReturn := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		result <- s.tryRunHeavyResearch(context.Background(), "active-before-arm", 0, func(ctx context.Context) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-allowReturn
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("heavy research callback did not enter")
	}
	s.liveMu.Lock()
	s.liveArmed = true
	s.liveMu.Unlock()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("active research context was not canceled after LIVE armed")
	}
	// Even after cancellation, the shared lane stays owned until the callback cooperatively exits.
	s.liveMu.Lock()
	s.liveArmed = false
	s.liveMu.Unlock()
	if s.tryRunHeavyResearch(context.Background(), "must-not-overlap-live", 0, func(context.Context) {}) {
		t.Fatal("research token was released before the canceled callback returned")
	}
	close(allowReturn)
	select {
	case completed := <-result:
		if completed {
			t.Fatal("LIVE-canceled research job reported completion")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled callback did not release the coordinator")
	}
}

func TestActiveHeavyResearchCancelsOnWALPressureAndKeepsTokenUntilReturn(t *testing.T) {
	s := testServer(t)
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	s.mutateCfg(func(c *config.Config) { c.DataDir = dir })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	publishResearchWALCheckpointReceipt(dir, time.Now(), 0, 0, 0, nil)
	gate := researchWorkCoordinatorFor(s)
	gate.mu.Lock()
	gate.walPressureBytes = 4 << 10
	gate.walPressurePeriod = 5 * time.Millisecond
	gate.mu.Unlock()

	entered := make(chan struct{})
	canceled := make(chan struct{})
	allowReturn := make(chan struct{})
	result := make(chan bool, 1)
	go func() {
		result <- s.tryRunHeavyResearch(context.Background(), "active-wal", 0, func(ctx context.Context) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-allowReturn
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("heavy research callback did not enter")
	}
	// Truncate creates a sparse file, so this test does not allocate or write the configured size.
	if err := os.Truncate(wal, 8<<10); err != nil {
		t.Fatal(err)
	}
	publishResearchWALCheckpointReceipt(dir, time.Now(), 0, 2, 0, nil)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("active heavy research context was not canceled after WAL crossed the limit")
	}
	gate.mu.Lock()
	deferred := gate.deferred["active-wal"]
	gate.mu.Unlock()
	if !deferred {
		t.Fatal("WAL-canceled heavy job was not marked deferred for retry")
	}

	// Bring the WAL back below the limit before probing the token. A pre-admission WAL rejection
	// would otherwise hide an early token release.
	if err := os.Truncate(wal, 0); err != nil {
		t.Fatal(err)
	}
	publishResearchWALCheckpointReceipt(dir, time.Now(), 0, 0, 0, nil)
	if s.tryRunHeavyResearch(context.Background(), "must-not-overlap", 0, func(context.Context) {}) {
		t.Fatal("shared token was released before the canceled callback actually returned")
	}
	select {
	case <-result:
		t.Fatal("tryRunHeavyResearch returned while its callback was still running")
	default:
	}

	close(allowReturn)
	select {
	case ranWithoutPressure := <-result:
		if ranWithoutPressure {
			t.Fatal("WAL-canceled job reported a completed admission instead of a deferred retry")
		}
	case <-time.After(time.Second):
		t.Fatal("WAL-canceled callback return did not release the coordinator")
	}
	if !s.tryRunHeavyResearch(context.Background(), "must-not-overlap", 0, func(context.Context) {}) {
		t.Fatal("shared token did not reopen after the canceled callback returned")
	}
}

func TestActiveHeavyResearchBelowWALLimitIsNotCanceled(t *testing.T) {
	s := testServer(t)
	dir := t.TempDir()
	t.Cleanup(func() { clearResearchWALCheckpointReceipt(dir) })
	s.mutateCfg(func(c *config.Config) { c.DataDir = dir })
	wal := filepath.Join(dir, "kalshi.db-wal")
	if err := os.WriteFile(wal, make([]byte, 1<<10), 0o600); err != nil {
		t.Fatal(err)
	}
	publishResearchWALCheckpointReceipt(dir, time.Now(), 0, 0, 0, nil)
	gate := researchWorkCoordinatorFor(s)
	gate.mu.Lock()
	gate.walPressureBytes = 4 << 10
	gate.walPressurePeriod = 5 * time.Millisecond
	gate.mu.Unlock()

	var canceled atomic.Bool
	if !s.tryRunHeavyResearch(context.Background(), "normal-wal", 0, func(ctx context.Context) {
		select {
		case <-ctx.Done():
			canceled.Store(true)
		case <-time.After(40 * time.Millisecond):
		}
	}) {
		t.Fatal("normal heavy research job was not reported as completed")
	}
	if canceled.Load() {
		t.Fatal("normal heavy research context was canceled below the WAL limit")
	}
	gate.mu.Lock()
	deferred := gate.deferred["normal-wal"]
	gate.mu.Unlock()
	if deferred {
		t.Fatal("normal heavy research job was incorrectly marked deferred")
	}
}

func TestResearchHeavySchedulersArePhasedAndGated(t *testing.T) {
	serverSource, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`loopAfter("research-systems", 7*time.Second, 5*time.Second`,
		`loopAfter("research-evidence", 9*time.Second, 5*time.Second`,
		`loopAfter("research-foundation", 37*time.Second, 30*time.Second`,
		`loopAfter("research-rules", 53*time.Second, 30*time.Second`,
		`loopAfter("parlay-lab", 137*time.Second, 2*time.Minute`,
		`go s.monitorResearchPortfolio(ctx)`,
		`s.tryRunHeavyResearch(ctx, "retention"`,
	} {
		if !strings.Contains(string(serverSource), want) {
			t.Fatalf("missing phased heavy scheduler %q", want)
		}
	}
	for file, wants := range map[string][]string{
		"researchsystems.go":           {`"evidence"`, `"foundation"`, `"replay"`},
		"researchsystems_rotation.go":  {`"systems"`},
		"researchrules.go":             {`"rules"`},
		"researchportfolio_runtime.go": {`"portfolio"`},
		"r139_inference.go":            {`"inference"`},
		"parlaylab.go":                 {`"combo-lab-enumerate"`},
	} {
		raw, readErr := os.ReadFile(file)
		if readErr != nil {
			t.Fatal(readErr)
		}
		for _, want := range wants {
			if !strings.Contains(string(raw), "tryRunHeavyResearch") || !strings.Contains(string(raw), want) {
				t.Fatalf("%s missing heavy gate %s", file, want)
			}
		}
	}
}

func TestResearchHeavyGateDefersRatherThanOverlaps(t *testing.T) {
	s := testServer(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		done <- s.tryRunHeavyResearch(context.Background(), "first", 0, func(context.Context) {
			close(entered)
			<-release
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first heavy job did not enter")
	}
	var overlapped atomic.Bool
	if s.tryRunHeavyResearch(context.Background(), "second", 0, func(context.Context) { overlapped.Store(true) }) {
		t.Fatal("second heavy job was admitted concurrently")
	}
	if overlapped.Load() {
		t.Fatal("deferred heavy callback ran")
	}
	close(release)
	if !<-done {
		t.Fatal("first heavy job did not complete")
	}
	if !s.tryRunHeavyResearch(context.Background(), "second", 0, func(context.Context) {}) {
		t.Fatal("gate was not released after the first job")
	}
}

func TestCompetingHeavyTickCannotProbeOrCancelTokenOwner(t *testing.T) {
	s := testServer(t)
	gate := researchWorkCoordinatorFor(s)
	gate.mu.Lock()
	gate.walPressurePeriod = 5 * time.Millisecond
	var forcedProbes atomic.Int64
	var falsePressure atomic.Bool
	gate.walProbe = func(_ context.Context, _ int64, force bool) (bool, int64) {
		if force && forcedProbes.Add(1) > 1 {
			// This models the transient busy/error receipt that a competing PASSIVE probe used to
			// publish. The active owner would observe it on its next monitor tick and self-cancel.
			falsePressure.Store(true)
			return true, researchWALPressureBytes + 1
		}
		if falsePressure.Load() {
			return true, researchWALPressureBytes + 1
		}
		return false, 0
	}
	gate.mu.Unlock()

	entered := make(chan struct{})
	release := make(chan struct{})
	canceled := make(chan struct{}, 1)
	result := make(chan bool, 1)
	go func() {
		result <- s.tryRunHeavyResearch(context.Background(), "owner", 0, func(ctx context.Context) {
			close(entered)
			select {
			case <-ctx.Done():
				canceled <- struct{}{}
			case <-release:
			}
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("token owner did not enter")
	}
	if s.tryRunHeavyResearch(context.Background(), "competitor", 0, func(context.Context) {}) {
		t.Fatal("competing tick entered while the token owner was active")
	}
	time.Sleep(25 * time.Millisecond)
	if got := forcedProbes.Load(); got != 1 {
		t.Fatalf("forced logical probes=%d, want only the token owner's admission probe", got)
	}
	if falsePressure.Load() {
		t.Fatal("competing tick issued a pressure-producing logical probe")
	}
	select {
	case <-canceled:
		t.Fatal("competing tick canceled the legitimate token owner")
	default:
	}
	close(release)
	select {
	case completed := <-result:
		if !completed {
			t.Fatal("healthy token owner did not report completion")
		}
	case <-time.After(time.Second):
		t.Fatal("token owner did not finish")
	}
}

func TestResearchHeavyGateBoundedWaitHonorsContext(t *testing.T) {
	s := testServer(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	go s.tryRunHeavyResearch(context.Background(), "holder", 0, func(context.Context) {
		close(entered)
		<-release
	})
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if s.tryRunHeavyResearch(ctx, "waiter", time.Second, func(context.Context) {}) {
		t.Fatal("context-expired waiter entered the heavy lane")
	}
	close(release)
}

func TestResearchHeavyGateBoundedWaitProbesOnlyAtActualAdmission(t *testing.T) {
	s := testServer(t)
	gate := researchWorkCoordinatorFor(s)
	// Hold the shared token without starting another callback. The waiter must not issue a
	// checkpoint probe until it owns the token at the actual admission boundary.
	<-gate.token
	probed := make(chan struct{})
	var probes atomic.Int64
	gate.mu.Lock()
	gate.walProbe = func(context.Context, int64, bool) (bool, int64) {
		probes.Add(1)
		close(probed)
		return true, researchWALPressureBytes + 1
	}
	gate.mu.Unlock()

	var ran atomic.Bool
	result := make(chan bool, 1)
	go func() {
		result <- s.tryRunHeavyResearch(context.Background(), "stale-waiter", time.Second,
			func(context.Context) { ran.Store(true) })
	}()
	select {
	case <-probed:
		t.Fatal("bounded waiter probed before it owned the shared token")
	case <-time.After(25 * time.Millisecond):
	}
	gate.token <- struct{}{}
	select {
	case admitted := <-result:
		if admitted || ran.Load() {
			t.Fatal("waiter entered after logical WAL debt rose while it waited for the token")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not finish after the token reopened")
	}
	if probes.Load() != 1 {
		t.Fatalf("logical probes=%d, want one probe at actual admission", probes.Load())
	}
	select {
	case <-gate.token:
		gate.token <- struct{}{}
	default:
		t.Fatal("WAL-rejected waiter did not return the shared token")
	}
}

func TestResearchHeavyGateYieldsRepeatedLaneToDeferredPeer(t *testing.T) {
	s := testServer(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		s.tryRunHeavyResearch(context.Background(), "systems", 0, func(context.Context) {
			close(entered)
			<-release
		})
		close(done)
	}()
	<-entered
	if s.tryRunHeavyResearch(context.Background(), "evidence", 0, func(context.Context) {}) {
		t.Fatal("peer unexpectedly overlapped active lane")
	}
	close(release)
	<-done
	if s.tryRunHeavyResearch(context.Background(), "systems", 0, func(context.Context) {}) {
		t.Fatal("repeated lane did not yield to deferred peer")
	}
	if !s.tryRunHeavyResearch(context.Background(), "evidence", 0, func(context.Context) {}) {
		t.Fatal("deferred peer did not acquire after fairness yield")
	}
}

func TestResearchSystemsFreshAndSettlementPathsPrecedeHeavyGate(t *testing.T) {
	raw, err := os.ReadFile("researchsystems.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	start := strings.Index(source, "func (s *Server) researchSystemsTick")
	end := strings.Index(source[start:], "func (s *Server) researchLifecycleTick")
	if start < 0 || end < 0 {
		t.Fatal("could not isolate researchSystemsTick")
	}
	body := source[start : start+end]
	rotation := strings.Index(body, "s.runNextResearchSystemHeavy(ctx, now)")
	if rotation < 0 {
		t.Fatal("research systems fair rotation missing")
	}
	for _, call := range []string{
		"s.sweepResearchPromotionDispatch(ctx, now)",
		"s.sweepQueuePriority(ctx)",
	} {
		at := strings.Index(body, call)
		if at < 0 {
			t.Fatalf("fresh/settlement path missing %q", call)
		}
		if at > rotation {
			t.Fatalf("fresh/settlement path %q is accidentally behind the heavy rotation", call)
		}
	}
	if strings.Contains(body, "sweepNativeTerminalSettlementsIfDue") {
		t.Fatal("terminal settlement can still block the fresh lifecycle/promotion scheduler")
	}
	if strings.Contains(body, "s.sweepLifecycleReopen(ctx)") {
		t.Fatal("slow lifecycle horizon scan can still starve the heavy collector rotation")
	}
	serverRaw, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(serverRaw), `loopAfter("research-terminal-settlement", 11*time.Second, 5*time.Second, 90*time.Second, s.researchTerminalSettlementTick)`) {
		t.Fatal("terminal settlement lacks an independent ungated scheduler")
	}
	if !strings.Contains(string(serverRaw), `loopAfter("research-lifecycle", 2*time.Second, 2*time.Second, 5*time.Second, s.researchLifecycleTick)`) {
		t.Fatal("lifecycle fixed horizons lack an independent bounded serial scheduler")
	}
}

func TestResearchSystemHeavyRotationVisitsEveryDueLane(t *testing.T) {
	seen := map[string]bool{}
	next := 0
	for range researchSystemHeavyLaneSpecs {
		idx, ok := nextDueResearchSystemLane(next, func(string) bool { return true })
		if !ok {
			t.Fatal("all-due rotation returned no lane")
		}
		name := researchSystemHeavyLaneSpecs[idx].name
		if seen[name] {
			t.Fatalf("lane %s repeated before full rotation", name)
		}
		seen[name] = true
		next = (idx + 1) % len(researchSystemHeavyLaneSpecs)
	}
	if len(seen) != len(researchSystemHeavyLaneSpecs) {
		t.Fatalf("rotation coverage=%d want=%d", len(seen), len(researchSystemHeavyLaneSpecs))
	}
}

func TestResearchSystemHeavyLanesAreIndividuallyBounded(t *testing.T) {
	if len(researchSystemHeavyLaneSpecs) < 10 {
		t.Fatalf("incomplete lane registry: %+v", researchSystemHeavyLaneSpecs)
	}
	wantColdStart := []string{"latent-register", "latent-outcome", "event-basket"}
	for i, want := range wantColdStart {
		if researchSystemHeavyLaneSpecs[i].name != want {
			t.Fatalf("cold-start lane %d=%q want %q: %+v", i, researchSystemHeavyLaneSpecs[i].name, want, researchSystemHeavyLaneSpecs)
		}
	}
	seen := map[string]bool{}
	for _, lane := range researchSystemHeavyLaneSpecs {
		if lane.name == "" || seen[lane.name] || lane.timeout <= 0 || lane.timeout > 55*time.Second {
			t.Fatalf("invalid bounded lane %+v", lane)
		}
		seen[lane.name] = true
	}
}

func TestResearchSystemCoverageRunsAfterColdCollectorReceipts(t *testing.T) {
	if got := researchSystemHeavyLaneSpecs[len(researchSystemHeavyLaneSpecs)-1].name; got != "system-coverage" {
		t.Fatalf("cold coverage lane=%q; coverage must consume every first-cycle receipt last", got)
	}
	coverage, required := -1, map[string]int{"event-basket": -1, "proper-score": -1, "incentive-economics": -1}
	for idx, lane := range researchSystemHeavyLaneSpecs {
		if lane.name == "system-coverage" {
			coverage = idx
		}
		if _, ok := required[lane.name]; ok {
			required[lane.name] = idx
		}
	}
	for name, idx := range required {
		if idx < 0 || coverage <= idx {
			t.Fatalf("cold lane order %s=%d coverage=%d", name, idx, coverage)
		}
	}
}

func TestResearchLaneTimeoutIncludesOnlyBoundedDurabilityGrace(t *testing.T) {
	const workLimit, durabilityGrace = 25 * time.Millisecond, 35 * time.Millisecond
	started := time.Now()
	timedOut := runCooperativelyBoundedResearchLane(context.Background(), workLimit, func(laneCtx context.Context) {
		// Model a cancellable store operation that consumes the work deadline, followed by the
		// receipt write that must survive cancellation briefly but must not hold the gate forever.
		<-laneCtx.Done()
		durableCtx, cancelDurability := researchDurabilityContextWithGrace(laneCtx, durabilityGrace)
		defer cancelDurability()
		<-durableCtx.Done()
	})
	elapsed := time.Since(started)
	if !timedOut {
		t.Fatal("blocked cooperative lane did not report its deadline")
	}
	if elapsed < workLimit {
		t.Fatalf("lane returned before work deadline: %s", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("lane exceeded work deadline plus bounded durability grace: %s", elapsed)
	}
}

func TestResearchDurabilityGraceDoesNotInheritUnusedLaneTime(t *testing.T) {
	parent, cancelParent := context.WithTimeout(context.Background(), time.Minute)
	cancelParent()
	started := time.Now()
	durableCtx, cancelDurability := researchDurabilityContextWithGrace(parent, 30*time.Millisecond)
	defer cancelDurability()
	<-durableCtx.Done()
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond || elapsed > 300*time.Millisecond {
		t.Fatalf("cancelled lane did not honor its short durability grace: %s", elapsed)
	}
}

func TestResearchGateStaysHeldDuringDurabilityGrace(t *testing.T) {
	s := testServer(t)
	inDurability, finished := make(chan struct{}), make(chan struct{})
	go func() {
		s.tryRunHeavyResearch(context.Background(), "bounded-holder", 0, func(gateCtx context.Context) {
			runCooperativelyBoundedResearchLane(gateCtx, 20*time.Millisecond, func(laneCtx context.Context) {
				<-laneCtx.Done()
				close(inDurability)
				durableCtx, cancelDurability := researchDurabilityContextWithGrace(laneCtx, 60*time.Millisecond)
				defer cancelDurability()
				<-durableCtx.Done()
			})
		})
		close(finished)
	}()
	<-inDurability
	if s.tryRunHeavyResearch(context.Background(), "must-wait", 0, func(context.Context) {}) {
		t.Fatal("shared gate was released while the bounded durability write was still active")
	}
	<-finished
	if !s.tryRunHeavyResearch(context.Background(), "must-wait", 0, func(context.Context) {}) {
		t.Fatal("shared gate did not reopen after bounded durability work finished")
	}
}

func TestResearchSystemTimeoutRetryHonorsBackoffThenRuns(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
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
	s.researchMu.Unlock()

	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	rotation.retryAt = map[string]time.Time{
		"event-basket":       now.Add(time.Minute),
		"native-time-nested": now.Add(2 * time.Minute),
	}
	rotation.Unlock()
	if s.runNextResearchSystemHeavy(context.Background(), now) {
		t.Fatal("timed-out lane retried before its short backoff elapsed")
	}
	rotation.Lock()
	rotation.retryAt["event-basket"] = now.Add(-time.Second)
	rotation.Unlock()
	if !s.runNextResearchSystemHeavy(context.Background(), now) {
		t.Fatal("timed-out lane did not receive its prioritized retry after backoff")
	}
	s.researchMu.Lock()
	ran := !s.lastBasketResearchAt.IsZero()
	s.researchMu.Unlock()
	if !ran {
		t.Fatal("prioritized retry did not execute the due event-basket lane")
	}
	rotation.Lock()
	_, otherRetryPreserved := rotation.retryAt["native-time-nested"]
	rotation.Unlock()
	if !otherRetryPreserved {
		t.Fatal("successful retry erased another lane's independent timeout backoff")
	}
}

func TestExpiredSystemRetryCannotJumpFairCursorPastDuePeer(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if err := s.store.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	if !s.ensureR139LatentCollectorBlueprints(ctx) {
		t.Fatal("latent collector blueprints unavailable")
	}
	now := time.Now().UTC()
	latent := s.r139LatentSchedule()
	latent.Lock()
	latent.registered, latent.lastOutcome, latent.lastScore = true, now, time.Time{}
	latent.Unlock()
	systemFunnelIdx := -1
	for i, spec := range researchSystemHeavyLaneSpecs {
		if spec.name == "system-funnels" {
			systemFunnelIdx = i
			break
		}
	}
	if systemFunnelIdx < 0 {
		t.Fatal("system-funnels lane missing")
	}
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	rotation.next = systemFunnelIdx
	rotation.retryAt = map[string]time.Time{"latent-score": now.Add(-time.Second)}
	rotation.failures = map[string]int{"latent-score": 1}
	rotation.Unlock()

	if !s.runNextResearchSystemHeavy(ctx, now) {
		t.Fatal("due peer was not admitted")
	}
	var funnels int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*)
FROM research_collector_receipts WHERE collector_id LIKE '%-funnel'`).Scan(&funnels); err != nil {
		t.Fatal(err)
	}
	if funnels != 1 {
		t.Fatalf("expired latent-score retry jumped the fair cursor: funnel receipts=%d want 1", funnels)
	}
	rotation.Lock()
	next := rotation.next
	_, retryPreserved := rotation.retryAt["latent-score"]
	rotation.Unlock()
	if next != (systemFunnelIdx+1)%len(researchSystemHeavyLaneSpecs) {
		t.Fatalf("fair cursor next=%d want %d", next,
			(systemFunnelIdx+1)%len(researchSystemHeavyLaneSpecs))
	}
	if !retryPreserved {
		t.Fatal("fair peer admission erased the still-due latent-score retry")
	}
}

func TestR143ResearchSystemTimeoutBackoffSelfRegulates(t *testing.T) {
	wants := []time.Duration{30 * time.Second, time.Minute, time.Minute,
		time.Minute, time.Minute, time.Minute}
	for i, want := range wants {
		if got := researchSystemRetryDelay(i + 1); got != want {
			t.Fatalf("failure %d retry=%s want %s", i+1, got, want)
		}
	}
}

func TestActiveSystemFunnelCycleYieldsEveryOtherAdmission(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if err := s.store.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	collectors := s.r138CollectorState()
	collectors.mu.Lock()
	collectors.systemFunnelCycleActive = true
	collectors.systemFunnelNext = 0
	collectors.mu.Unlock()
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	rotation.next = len(researchSystemHeavyLaneSpecs) - 1 // prove the global cursor cannot stretch the active cycle
	rotation.initialRotationComplete = true
	rotation.Unlock()

	if !s.runNextResearchSystemHeavy(ctx, time.Now().UTC()) {
		t.Fatal("first active funnel admission was not run")
	}
	var receipts int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*)
FROM research_collector_receipts WHERE collector_id LIKE '%-funnel'`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("first active funnel admission produced %d receipts", receipts)
	}
	// The next admission must go to another due lane. The third may return to the still-active
	// funnel, proving this is a bounded fairness yield rather than a five-minute throttle.
	if !s.runNextResearchSystemHeavy(ctx, time.Now().UTC()) {
		t.Fatal("fair peer admission was not run")
	}
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*)
FROM research_collector_receipts WHERE collector_id LIKE '%-funnel'`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 {
		t.Fatalf("active funnel monopolized the fairness admission: receipts=%d want 1", receipts)
	}
	if !s.runNextResearchSystemHeavy(ctx, time.Now().UTC()) {
		t.Fatal("second active funnel admission was not run after yielding")
	}
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*)
FROM research_collector_receipts WHERE collector_id LIKE '%-funnel'`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 2 {
		t.Fatalf("active funnel did not resume after one peer: receipts=%d want 2", receipts)
	}
	collectors.mu.Lock()
	active, next := collectors.systemFunnelCycleActive, collectors.systemFunnelNext
	collectors.mu.Unlock()
	if !active || next != 2 {
		t.Fatalf("funnel cycle active=%t next=%d; want true/2", active, next)
	}
}

func TestInitialSystemsRotationDoesNotRepeatActiveFunnelAheadOfUnseenPeers(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	if err := s.store.EnsureR138Step6To9CollectorBlueprints(ctx); err != nil {
		t.Fatal(err)
	}
	collectors := s.r138CollectorState()
	collectors.mu.Lock()
	collectors.systemFunnelCycleActive = true
	collectors.systemFunnelNext = 0
	collectors.mu.Unlock()
	funnelIdx := -1
	for i, spec := range researchSystemHeavyLaneSpecs {
		if spec.name == "system-funnels" {
			funnelIdx = i
			break
		}
	}
	if funnelIdx < 0 {
		t.Fatal("system-funnels lane missing")
	}
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	rotation.next = (funnelIdx + 1) % len(researchSystemHeavyLaneSpecs)
	rotation.initialRotationComplete = false
	rotation.Unlock()

	if !s.runNextResearchSystemHeavy(ctx, time.Now().UTC()) {
		t.Fatal("unseen peer did not receive its first bounded admission")
	}
	var receipts int
	if err := s.store.DBForTest().QueryRowContext(ctx, `SELECT COUNT(*)
FROM research_collector_receipts WHERE collector_id LIKE '%-funnel'`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 {
		t.Fatalf("active funnel repeated before the initial lane rotation completed: receipts=%d", receipts)
	}
}

func TestStartupSystemsPriorityDefersWideMaintenanceButNotForever(t *testing.T) {
	s := testServer(t)
	s.bootAt = time.Now()
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	rotation.initialRotationComplete = false
	rotation.Unlock()
	var ran atomic.Bool
	if s.tryRunHeavyResearch(context.Background(), "broad-signal-settlement", 0,
		func(context.Context) { ran.Store(true) }) || ran.Load() {
		t.Fatal("wide maintenance entered before the first systems rotation")
	}
	rotation.Lock()
	rotation.initialRotationComplete = true
	rotation.Unlock()
	collectors := s.r138CollectorState()
	collectors.mu.Lock()
	collectors.systemFunnelCycleActive = false
	collectors.mu.Unlock()
	if !s.tryRunHeavyResearch(context.Background(), "broad-signal-settlement", 0,
		func(context.Context) { ran.Store(true) }) || !ran.Load() {
		t.Fatal("wide maintenance did not resume after startup collector priority cleared")
	}
}

func TestIdleInitialSystemsRotationClearsStartupPriorityWhenEveryLaneIsRecent(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	s.bootAt = now

	latent := s.r139LatentSchedule()
	latent.Lock()
	latent.registered, latent.lastOutcome, latent.lastScore = true, now, now
	latent.Unlock()

	collectors := s.r138CollectorState()
	collectors.mu.Lock()
	collectors.systemFunnelCycleActive = false
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
	s.lastBasketResearchAt = now
	s.lastProperScoreAt = now
	s.lastIncentiveResearchAt = now
	s.researchMu.Unlock()

	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	rotation.initialRotationComplete = false
	rotation.Unlock()

	if s.runNextResearchSystemHeavy(context.Background(), now) {
		t.Fatal("idle systems scheduler admitted work even though every lane was recent")
	}
	rotation.Lock()
	initialComplete := rotation.initialRotationComplete
	rotation.Unlock()
	if !initialComplete {
		t.Fatal("idle systems scheduler left the initial rotation incomplete")
	}
	if s.researchSystemsStartupPriority() {
		t.Fatal("startup broad-work deferral remained active after the idle scheduler cleared the initial rotation")
	}
}

func TestLatentRegistrationOwnsFirstAdmission(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
	if !s.runNextResearchSystemHeavy(context.Background(), now) {
		t.Fatal("cold-start latent registration lane was not admitted")
	}
	latent := s.r139LatentSchedule()
	latent.Lock()
	registered, outcomeRan, scoreRan := latent.registered, !latent.lastOutcome.IsZero(), !latent.lastScore.IsZero()
	latent.Unlock()
	if !registered || outcomeRan || scoreRan {
		t.Fatalf("first admission registered=%t outcome_ran=%t score_ran=%t; registration must be isolated",
			registered, outcomeRan, scoreRan)
	}
}

func TestResearchSystemDueClockWaitsForActualAdmission(t *testing.T) {
	s := testServer(t)
	now := time.Now().UTC()
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
	s.researchMu.Unlock()
	rotation := s.researchSystemsRotationRuntime()
	rotation.Lock()
	nextBefore := rotation.next
	rotation.Unlock()

	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		s.tryRunHeavyResearch(context.Background(), "blocker", 0, func(context.Context) {
			close(entered)
			<-release
		})
		close(done)
	}()
	<-entered
	if s.runNextResearchSystemHeavy(context.Background(), now) {
		t.Fatal("systems lane entered while heavy gate was occupied")
	}
	s.researchMu.Lock()
	stampedWhileDeferred := !s.lastBasketResearchAt.IsZero()
	s.researchMu.Unlock()
	if stampedWhileDeferred {
		t.Fatal("event-basket due clock advanced without admission")
	}
	rotation.Lock()
	nextWhileDeferred := rotation.next
	_, retryRecorded := rotation.retryAt["event-basket"]
	failuresWhileDeferred := rotation.failures["event-basket"]
	rotation.Unlock()
	if nextWhileDeferred != nextBefore || retryRecorded || failuresWhileDeferred != 0 {
		t.Fatalf("never-admitted lane changed rotation: next=%d/%d retry=%t failures=%d",
			nextWhileDeferred, nextBefore, retryRecorded, failuresWhileDeferred)
	}
	close(release)
	<-done
	if !s.runNextResearchSystemHeavy(context.Background(), now) {
		t.Fatal("due event-basket lane was not admitted after gate release")
	}
	s.researchMu.Lock()
	stampedAfterRun := !s.lastBasketResearchAt.IsZero()
	s.researchMu.Unlock()
	if !stampedAfterRun {
		t.Fatal("event-basket due clock did not advance on actual run")
	}
}

func TestNativeTerminalGradesHaveIndependentUngatedCadence(t *testing.T) {
	raw, err := os.ReadFile("native_lock_collectors.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	discoveryStart := strings.Index(source, "func (s *Server) sweepNativePlaceholderSystems")
	terminalStart := strings.Index(source, "func (s *Server) sweepNativeTerminalSettlementsIfDue")
	viewStart := strings.Index(source, "type nativeLockCollectionView")
	if discoveryStart < 0 || terminalStart <= discoveryStart || viewStart <= terminalStart {
		t.Fatal("could not isolate native discovery and terminal schedulers")
	}
	discovery := source[discoveryStart:terminalStart]
	for _, forbidden := range []string{"sweepNativeRouteTerminalGrades", "sweepResearchSystemTerminalGrades"} {
		if strings.Contains(discovery, forbidden) {
			t.Fatalf("candidate discovery still gates terminal work via %s", forbidden)
		}
	}
	terminal := source[terminalStart:viewStart]
	for _, required := range []string{"5*time.Minute", "sweepNativeRouteTerminalGrades", "sweepResearchSystemTerminalGrades", "researchTerminalSettlementTick"} {
		if !strings.Contains(terminal, required) {
			t.Fatalf("independent terminal scheduler missing %s", required)
		}
	}
}

func TestResearchPortfolioDeferredAdmissionRetriesWithoutOverlap(t *testing.T) {
	var attempts, active atomic.Int32
	admitted := retryDeferredResearch(context.Background(), time.Millisecond, func() bool {
		if active.Add(1) != 1 {
			t.Fatal("portfolio retry attempts overlapped")
		}
		defer active.Add(-1)
		return attempts.Add(1) == 3
	})
	if !admitted || attempts.Load() != 3 {
		t.Fatalf("portfolio retry admitted=%v attempts=%d, want true/3", admitted, attempts.Load())
	}
}

func TestResearchPortfolioDeferredAdmissionStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempted := make(chan struct{}, 1)
	done := make(chan bool, 1)
	go func() {
		done <- retryDeferredResearch(ctx, time.Minute, func() bool {
			attempted <- struct{}{}
			return false
		})
	}()
	<-attempted
	cancel()
	select {
	case admitted := <-done:
		if admitted {
			t.Fatal("canceled portfolio retry reported admission")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled portfolio retry did not stop")
	}
}
