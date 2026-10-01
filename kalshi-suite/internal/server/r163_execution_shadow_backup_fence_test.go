package server

import (
	"context"
	"database/sql"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR163ExecutionShadowBackupFenceCapturesAcceptedTailAndRemainsShutdownSafe(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	sig := storage.Signal{
		Platform:   "kalshi",
		Ticker:     "KXR163-FINAL-BACKUP",
		Title:      "accepted before final backup",
		Side:       "YES",
		SignalType: "spotlag",
		EntryPrice: .40,
	}
	if attemptID := s.executionShadowBeginSignal(sig, .08, time.Now()); attemptID == "" {
		t.Fatal("could not enqueue the pre-fence tail")
	}

	fenceCtx, cancelFence := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFence()
	fenced := make(chan error, 1)
	go func() { fenced <- s.FenceExecutionShadowForBackup(fenceCtx) }()

	deadline := time.Now().Add(time.Second)
	for {
		s.executionShadowMu.Lock()
		stopping := s.executionShadowStopping
		occupancy := len(s.executionShadowPending) + s.executionShadowInFlight
		s.executionShadowMu.Unlock()
		if stopping {
			if occupancy == 0 {
				t.Fatal("backup fence did not close admission before draining the accepted tail")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("backup fence did not close admission")
		}
		time.Sleep(time.Millisecond)
	}

	lateCandidate := liveMirrorCandidate{
		Platform: "kalshi",
		Ticker:   "KXR163-AFTER-BACKUP-FENCE",
		Side:     "YES",
		Action:   "BUY",
		Family:   "spotlag",
		Price:    .40,
		At:       time.Now(),
	}
	_, lateAttempt, ok := s.executionShadowBuildCandidate(
		lateCandidate, "late-after-backup-fence", "")
	if !ok {
		t.Fatal("could not build the post-fence admission fixture")
	}
	beforeStoppingDrops := s.executionShadowDropStopping.Load()
	if s.enqueueExecutionShadowWriteGroup(
		[]executionShadowWrite{{attempt: &lateAttempt}}, true) {
		t.Fatal("backup fence accepted a new execution-shadow record")
	}
	if got := s.executionShadowDropStopping.Load(); got != beforeStoppingDrops+1 {
		t.Fatalf("post-fence drop reason=%d want=%d", got, beforeStoppingDrops+1)
	}

	releaseWriter()
	if err := <-fenced; err != nil {
		t.Fatalf("backup fence failed to drain its accepted tail: %v", err)
	}

	backupPath := filepath.Join(t.TempDir(), "execution_shadow.db")
	if err := st.BackupExecutionShadow(fenceCtx, backupPath); err != nil {
		t.Fatalf("snapshot after backup fence: %v", err)
	}
	snapshot, err := sql.Open("sqlite", backupPath)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var accepted, refused int
	if err = snapshot.QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts WHERE ticker=?`,
		sig.Ticker).Scan(&accepted); err != nil {
		t.Fatal(err)
	}
	if err = snapshot.QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts WHERE ticker=?`,
		lateCandidate.Ticker).Scan(&refused); err != nil {
		t.Fatal(err)
	}
	if accepted != 1 || refused != 0 {
		t.Fatalf("fenced snapshot accepted=%d refused=%d want=1,0", accepted, refused)
	}

	// Re-entering the same complete shutdown/fence boundary from Shutdown must be harmless and
	// must not mutate the already-stable execution-shadow rows.
	s.http = &http.Server{}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err = s.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("ordinary shutdown after backup fence: %v", err)
	}
	rows, err := st.ListExecutionShadowAttempts(context.Background(), 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.Ticker != sig.Ticker {
		t.Fatalf("shutdown after fence changed persisted tail: rows=%+v err=%v", rows, err)
	}
	status := st.ExecutionShadowStatus(context.Background())
	if !status.LatestClean {
		t.Fatalf("successful backup fence left writer session unclean: %+v", status)
	}
}

func TestR163PrepareFinalSnapshotDisarmsClearsAndFencesInOrder(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	intent := liveSignalIntent{
		Signal: storage.Signal{
			Platform: "kalshi", Ticker: "KXR163-PREPARE", Title: "queued before prepare",
			Side: "YES", SignalType: "spotlag", EntryPrice: .40,
		},
		Point: .08, At: time.Now().UTC(),
	}
	intent.ShadowAttemptID = s.executionShadowBeginSignal(intent.Signal, intent.Point, intent.At)
	if intent.ShadowAttemptID == "" {
		t.Fatal("could not publish the queued intent's parent attempt")
	}
	s.liveSignalIntentChannel() <- intent

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.PrepareFinalSnapshot(ctx); err != nil {
		t.Fatal(err)
	}
	s.liveMu.Lock()
	armed, auto := s.liveArmed, s.liveAuto
	s.liveMu.Unlock()
	if armed || auto {
		t.Fatalf("final snapshot left armed=%v auto=%v", armed, auto)
	}
	if got := len(s.liveSignalIntentChannel()); got != 0 {
		t.Fatalf("final snapshot left %d queued LIVE intents", got)
	}
	if !s.executionShadowStoppingAtomic.Load() {
		t.Fatal("final snapshot did not fence execution-shadow admission")
	}
	rows, err := st.ListExecutionShadowAttempts(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Attempt.Ticker != intent.Signal.Ticker {
		t.Fatalf("queue-clear lineage was not drained before the fence: %+v", rows)
	}
	if got := st.ExecutionShadowStatus(context.Background()); !got.LatestClean {
		t.Fatalf("final snapshot did not close the shadow session cleanly: %+v", got)
	}
}

func TestR163PrepareFinalSnapshotJoinsRunningPreflightBeforeShadowFence(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	now := time.Now().UTC()
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "KXR163-INFLIGHT-PREFLIGHT",
		Title: "in-flight preflight before final snapshot", Side: "YES",
		SignalType: "spotlag", EntryPrice: .40,
	}
	attemptID := s.executionShadowBeginSignal(sig, .08, now)
	candidate := liveCandidateFromSignalIntent(liveSignalIntent{
		Signal: sig, Point: .08, At: now, ShadowAttemptID: attemptID,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	scheduler := newLiveSignalPreflightScheduler(s, context.Background(), 1, 4,
		func(context.Context, liveSignalIntent) bool {
			close(started)
			<-release // deliberately finish after cancellation to pin the shutdown join
			s.executionShadowRecordMirror(candidate, storage.LivePolicyMirrorRow{
				State:  storage.LivePolicyMirrorRejected,
				Reason: "in-flight-preflight-finished-before-fence", ProcessedAt: time.Now().UTC(),
			})
			close(finished)
			return false
		}, nil)
	if actual, loaded := liveSignalPreflightSchedulers.LoadOrStore(s, scheduler); loaded {
		scheduler.stop()
		t.Fatalf("unexpected existing preflight scheduler: %T", actual)
	}
	var releaseOnce atomic.Bool
	t.Cleanup(func() {
		if releaseOnce.CompareAndSwap(false, true) {
			close(release)
		}
		scheduler.stop()
	})
	scheduler.submit(context.Background(), []liveSignalIntent{{
		Signal: sig, Point: .08, At: now, ShadowAttemptID: attemptID,
	}})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("preflight worker did not start")
	}

	prepareCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prepared := make(chan error, 1)
	go func() { prepared <- s.PrepareFinalSnapshot(prepareCtx) }()
	deadline := time.Now().Add(time.Second)
	for {
		scheduler.mu.Lock()
		stopped := scheduler.closed
		scheduler.mu.Unlock()
		if stopped {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("final snapshot never stopped the in-flight scheduler")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-prepared:
		t.Fatalf("final snapshot crossed the preflight join early: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	if s.executionShadowStoppingAtomic.Load() {
		t.Fatal("execution-shadow fenced while a preflight producer was still running")
	}
	if releaseOnce.CompareAndSwap(false, true) {
		close(release)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("preflight worker did not finish")
	}
	select {
	case err := <-prepared:
		if err != nil {
			t.Fatalf("final snapshot after preflight join: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("final snapshot did not finish after the preflight worker")
	}
	views, err := st.ListExecutionShadowAttempts(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, view := range views {
		if view.Attempt.AttemptID != attemptID {
			continue
		}
		for _, event := range view.Events {
			if event.Stage == "counterfactual-terminal" &&
				event.Reason == "in-flight-preflight-finished-before-fence" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("joined preflight terminal was absent from the fenced shadow generation")
	}
}

func TestR163PrepareFinalSnapshotJoinsWholeLiveSettlementPassBetweenMethods(t *testing.T) {
	s, _ := newExecutionShadowTestServer(t)
	betweenMethods := make(chan struct{})
	releasePass := make(chan struct{})
	passFinished := make(chan struct{})
	var released atomic.Bool
	var reachedLaterMethod atomic.Bool
	var boundaryReturned atomic.Bool
	var writeAfterBoundary atomic.Bool
	s.liveSettlementLaneStep = func(step string) {
		switch step {
		case "after-live-settlements":
			close(betweenMethods)
			<-releasePass
		case "after-live-dispatch-shadow":
			// This checkpoint follows a main-DB settlement method. If the final boundary had
			// returned while the pass was between methods, that method could write afterward.
			if boundaryReturned.Load() {
				writeAfterBoundary.Store(true)
			}
			reachedLaterMethod.Store(true)
		}
	}
	t.Cleanup(func() {
		if released.CompareAndSwap(false, true) {
			close(releasePass)
		}
	})
	go func() {
		s.settleLiveLedgers(context.Background())
		close(passFinished)
	}()
	select {
	case <-betweenMethods:
	case <-time.After(time.Second):
		t.Fatal("LIVE settlement pass did not reach the between-method boundary")
	}

	prepareCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	prepared := make(chan error, 1)
	go func() {
		err := s.PrepareFinalSnapshot(prepareCtx)
		boundaryReturned.Store(true)
		prepared <- err
	}()
	deadline := time.Now().Add(time.Second)
	for {
		s.liveSettlementLaneLifecycleMu.Lock()
		closing := s.liveSettlementLaneClosing
		inFlight := s.liveSettlementLaneInFlight
		s.liveSettlementLaneLifecycleMu.Unlock()
		if closing {
			if inFlight != 1 {
				t.Fatalf("final snapshot lost the in-flight settlement pass: %d", inFlight)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("final snapshot did not close LIVE settlement-lane admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-prepared:
		t.Fatalf("final snapshot crossed the whole-lane join between methods: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	if reachedLaterMethod.Load() {
		t.Fatal("blocked pass advanced while the final snapshot was waiting")
	}

	if released.CompareAndSwap(false, true) {
		close(releasePass)
	}
	select {
	case <-passFinished:
	case <-time.After(time.Second):
		t.Fatal("LIVE settlement pass did not finish after release")
	}
	if !reachedLaterMethod.Load() {
		t.Fatal("fixture never advanced into the later settlement method")
	}
	if writeAfterBoundary.Load() {
		t.Fatal("later settlement method wrote after the final-snapshot boundary returned")
	}
	select {
	case err := <-prepared:
		if err != nil {
			t.Fatalf("final snapshot after whole-lane join: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("final snapshot did not finish after the complete settlement pass")
	}

	// The boundary is irreversible: even a late cadence/direct call cannot begin another pass.
	reachedLaterMethod.Store(false)
	s.settleLiveLedgers(context.Background())
	if reachedLaterMethod.Load() {
		t.Fatal("LIVE settlement pass started after final-snapshot admission closed")
	}
}

func TestR163PreflightWorkerJoinHonorsShutdownDeadline(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	scheduler := newLiveSignalPreflightScheduler(nil, context.Background(), 1, 2,
		func(context.Context, liveSignalIntent) bool {
			close(started)
			<-release
			return false
		}, nil)
	var releaseOnce atomic.Bool
	t.Cleanup(func() {
		if releaseOnce.CompareAndSwap(false, true) {
			close(release)
		}
		scheduler.stop()
	})
	scheduler.submit(context.Background(), []liveSignalIntent{{
		Signal: storage.Signal{Platform: "kalshi", Ticker: "KXR163-BOUNDED-JOIN",
			Side: "YES", SignalType: "spotlag", EntryPrice: .40},
		At: time.Now(),
	}})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("preflight worker did not start")
	}
	joinCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := scheduler.wait(joinCtx); err != context.DeadlineExceeded {
		t.Fatalf("bounded join error=%v want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("bounded preflight join ignored its deadline for %v", elapsed)
	}
	if releaseOnce.CompareAndSwap(false, true) {
		close(release)
	}
	doneCtx, doneCancel := context.WithTimeout(context.Background(), time.Second)
	defer doneCancel()
	if err := scheduler.wait(doneCtx); err != nil {
		t.Fatalf("preflight worker did not become joinable after release: %v", err)
	}
}
