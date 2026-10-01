package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR167TrulyUnselectedDuplicateMintsOneCanonicalOpportunity(t *testing.T) {
	const ticker = "R167-UNSELECTED-DUP"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	r163ArmSelectedSpotlagYES(s) // Spot-lag NO is deliberately outside the exact LIVE allowlist.
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	paperCh := r163InstallPaperCapture(s, 2, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })

	at := time.Now().UTC()
	sig := storage.Signal{Platform: "kalshi", Ticker: ticker, Title: "unselected duplicate",
		Side: "NO", SignalType: "spotlag", EntryPrice: .40, ResolveHours: 1,
		ExecExpr: r147InputReceiptExpr("K-SPOT", at)}
	s.genfollowTriggeredSystemsAt(context.Background(), sig, at)
	s.genfollowTriggeredSystemsAt(context.Background(), sig, at.Add(time.Millisecond))

	var queued genfollowPaperSignal
	select {
	case batch := <-paperCh:
		if len(batch.Signals) != 1 {
			t.Fatalf("canonical Paper batch=%+v", batch.Signals)
		}
		queued = batch.Signals[0]
	case <-time.After(time.Second):
		t.Fatal("unselected canonical opportunity did not reach Paper")
	}
	select {
	case duplicate := <-paperCh:
		t.Fatalf("duplicate unselected opportunity reached Paper: %+v", duplicate)
	case <-time.After(75 * time.Millisecond):
	}
	if queued.ShadowAttemptID == "" {
		t.Fatal("unselected Paper branch lost canonical attempt id")
	}

	time.Sleep(75 * time.Millisecond)
	policyCtx, policyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer policyCancel()
	if err := s.stopLivePolicyMirrorWorker(policyCtx); err != nil {
		t.Fatalf("stop canonical q1 worker: %v", err)
	}
	releaseShadow()
	if err := s.stopExecutionShadowWriter(policyCtx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(policyCtx, 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.AttemptID != queued.ShadowAttemptID {
		t.Fatalf("unselected duplicates produced rows=%+v err=%v", rows, err)
	}
}

func TestR167CanonicalClaimKeepsInputTopologiesIndependent(t *testing.T) {
	s := testServer(t)
	at := time.Now().UTC()
	makeSignal := func(topology string) storage.Signal {
		return storage.Signal{Platform: "kalshi", Ticker: "R167-TOPOLOGY", Side: "YES",
			SignalType: "confluence", EntryPrice: .40,
			ExecExpr: r147InputReceiptExpr(topology, at)}
	}
	pint, pus := makeSignal("K-PINT"), makeSignal("K-PUS")
	if !s.claimCanonicalSignalOpportunity(pint, at) {
		t.Fatal("first K-PINT opportunity was not claimed")
	}
	if s.claimCanonicalSignalOpportunity(pint, at.Add(time.Millisecond)) {
		t.Fatal("duplicate K-PINT opportunity escaped the 500 ms claim")
	}
	if !s.claimCanonicalSignalOpportunity(pus, at.Add(2*time.Millisecond)) {
		t.Fatal("independent K-PUS topology was coalesced with K-PINT")
	}
}

func TestR170InverseKflowProducersShareTheThirtySecondCanonicalEpisode(t *testing.T) {
	s := testServer(t)
	at := time.Now().UTC()
	original := storage.Signal{Platform: "kalshi", Ticker: "R170-INVERSE-SHARED",
		Title: "shared inverse", Side: "YES", SignalType: "kalshi-flow", EntryPrice: .31,
		ResolveHours: 1, ExecExpr: r147InputReceiptExpr("K", at)}
	inverse, ok := inverseKflowShadowSignal(original)
	if !ok {
		t.Fatal("inverse fixture did not bind")
	}
	c := liveCandidateFromSignalIntent(liveSignalIntent{Signal: inverse, At: at})
	key := c.key()
	s.liveMirrorMu.Lock()
	s.liveSignalIntentSeen = map[string]time.Time{key: time.Now().Add(-time.Second)}
	s.liveMirrorMu.Unlock()
	if s.claimCanonicalSignalOpportunity(inverse, at.Add(time.Second)) {
		t.Fatal("inverse producer escaped the shared episode after the old 500 ms window")
	}
	s.liveMirrorMu.Lock()
	s.liveSignalIntentSeen[key] = time.Now().Add(-liveMirrorDedup - time.Millisecond)
	s.liveMirrorMu.Unlock()
	if !s.claimCanonicalSignalOpportunity(inverse, at.Add(liveMirrorDedup+time.Millisecond)) {
		t.Fatal("new inverse economic episode remained coalesced after 30 seconds")
	}

	direct := original
	direct.Ticker = "R170-DIRECT-WINDOW"
	directKey := liveCandidateFromSignalIntent(liveSignalIntent{Signal: direct, At: at}).key()
	s.liveMirrorMu.Lock()
	s.liveSignalIntentSeen[directKey] = time.Now().Add(-time.Second)
	s.liveMirrorMu.Unlock()
	if !s.claimCanonicalSignalOpportunity(direct, at.Add(time.Second)) {
		t.Fatal("ordinary direct signal incorrectly acquired the inverse 30-second window")
	}
}

func TestR170FailedInverseHandoffPoisonsNeitherEpisodeNorCanonicalClaim(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveSystemShadowAllowlist = inverseKflowShadowIdentity
	s.cfgP.Store(&cfg)
	at := time.Now().UTC()
	original := storage.Signal{Platform: "kalshi", Ticker: "R170-INVERSE-QUEUE-FAIL",
		Title: "queue failure", Side: "YES", SignalType: "kalshi-flow", EntryPrice: .31,
		ResolveHours: 1, ExecExpr: r147InputReceiptExpr("K", at)}
	inverse, ok := inverseKflowShadowSignal(original)
	if !ok {
		t.Fatal("inverse fixture did not bind")
	}
	s.livePolicyMirrorMu.Lock()
	s.livePolicyMirrorStopping = true
	s.livePolicyMirrorMu.Unlock()
	if s.queueInverseKflowShadow(original, at) {
		t.Fatal("stopped worker accepted inverse handoff")
	}
	if !s.claimInverseKflowShadowEpisode(inverse.Ticker, time.Now()) {
		t.Fatal("failed handoff left the 30-second producer episode poisoned")
	}
	if !s.claimCanonicalSignalOpportunity(inverse, at) {
		t.Fatal("failed handoff consumed the generic canonical claim")
	}
}

func TestR167ResearchPromotionProducerFansOneIDToEveryBranch(t *testing.T) {
	const ticker = "R167RESEARCH-FANOUT"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	paperCh := r163InstallPaperCapture(s, 2, false)
	t.Cleanup(func() { r163RemovePaperCapture(s) })
	at := time.Now().UTC()
	candidate := storage.ResearchPromotionCandidate{ObservationID: 167,
		Observed: at, SystemID: "proper-score-executor", Venue: "kalshi",
		Cohort: "proper-score-v2|mode=fundamental|transform=brier|fixture=true",
		Ticker: ticker, Title: "research producer canonical fanout", Side: "YES",
		Route: "taker", ObservedPrice: .40, ObservedFeePC: .01}
	if queued, reason := s.enqueueResearchCandidateDelayedPaper(candidate, nil); !queued {
		t.Fatalf("research producer was not queued: %s", reason)
	}
	if queued, reason := s.enqueueResearchCandidateDelayedPaper(candidate, nil); queued ||
		reason != "shared-single:duplicate-coalesced-canonical-opportunity" {
		t.Fatalf("research duplicate queued=%t reason=%q", queued, reason)
	}

	var paper genfollowPaperSignal
	select {
	case batch := <-paperCh:
		if len(batch.Signals) != 1 {
			t.Fatalf("research Paper batch=%+v", batch.Signals)
		}
		paper = batch.Signals[0]
	case <-time.After(time.Second):
		t.Fatal("research producer did not reach realistic Paper")
	}
	if paper.ShadowAttemptID == "" || paper.onTerminal == nil {
		t.Fatalf("research Paper branch lost id/callback: %+v", paper)
	}
	select {
	case duplicate := <-paperCh:
		t.Fatalf("research duplicate reached Paper: %+v", duplicate)
	case <-time.After(75 * time.Millisecond):
	}

	time.Sleep(75 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(ctx); err != nil {
		t.Fatalf("stop canonical q1 worker: %v", err)
	}
	releaseShadow()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.AttemptID != paper.ShadowAttemptID {
		t.Fatalf("research fanout rows=%+v err=%v", rows, err)
	}
	stages := map[string]storage.ExecutionShadowEvent{}
	for _, event := range rows[0].Events {
		stages[event.Stage] = event
	}
	for _, stage := range []string{"detector-branch", "live-selection",
		"funded-paper-branch", "counterfactual-execution-terminal"} {
		if _, ok := stages[stage]; !ok {
			t.Fatalf("research attempt %s missing %s: %+v", paper.ShadowAttemptID, stage, rows[0].Events)
		}
	}
	if stages["live-selection"].LiveState != "not-sent" ||
		stages["live-selection"].VenueAttempted {
		t.Fatalf("research LIVE branch is not an explicit no-send: %+v", stages["live-selection"])
	}
}

func TestR167ResearchPaperQueueFailureRetainsCanonicalReplayLineage(t *testing.T) {
	const ticker = "R167RESEARCH-QUEUE-FAIL"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	s.genfollowPaperWorkerMu.Lock()
	s.genfollowPaperWorkerStopping = true
	s.genfollowPaperWorkerMu.Unlock()
	at := time.Now().UTC()
	candidate := storage.ResearchPromotionCandidate{ObservationID: 168,
		Observed: at, SystemID: "proper-score-executor", Venue: "kalshi", Ticker: ticker,
		Cohort: "proper-score-v2|mode=fundamental|transform=brier|fixture=true",
		Title:  "research queue failure lineage", Side: "YES", Route: "taker",
		ObservedPrice: .40, ObservedFeePC: .01}
	if queued, reason := s.enqueueResearchCandidateDelayedPaper(candidate, nil); queued ||
		reason != "shared-single:delayed-paper-worker-queue-unavailable" {
		t.Fatalf("stopping queue result queued=%t reason=%q", queued, reason)
	}

	time.Sleep(75 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopLivePolicyMirrorWorker(ctx); err != nil {
		t.Fatalf("stop canonical q1 worker: %v", err)
	}
	releaseShadow()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("queue failure canonical rows=%+v err=%v", rows, err)
	}
	stages := map[string]bool{}
	for _, event := range rows[0].Events {
		stages[event.Stage] = true
	}
	for _, stage := range []string{"detector-branch", "live-selection",
		"funded-paper-branch", "funded-paper-scheduler"} {
		if !stages[stage] {
			t.Fatalf("queue failure lost %s under attempt %s: %+v",
				stage, rows[0].Attempt.AttemptID, rows[0].Events)
		}
	}
}

func TestR167PaperTerminalCallbackKeepsCanonicalAndPaperAttemptIDs(t *testing.T) {
	got := make(chan genfollowPaperTerminalResult, 1)
	queued := genfollowPaperSignal{ShadowAttemptID: "exec-r167-canonical",
		onTerminal: func(_ context.Context, result genfollowPaperTerminalResult) { got <- result }}
	completeQueuedGenfollowPaperSignal(queued, genfollowPaperTerminalResult{
		State: "PAPER-FILLED", AttemptID: "gf-r167-paper",
	})
	select {
	case result := <-got:
		if result.AttemptID != "gf-r167-paper" || result.ShadowAttemptID != queued.ShadowAttemptID {
			t.Fatalf("terminal callback lineage=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal callback was lost")
	}
}
