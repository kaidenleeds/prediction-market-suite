package server

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/config"
	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r163SelectSpotlagYES(t *testing.T, s *Server, ticker string) string {
	t.Helper()
	fee, _, ok := s.fillFeeReceipt("kalshi", ticker, false, 1, .40)
	if !ok {
		t.Fatal("spotlag fixture exact fee unavailable")
	}
	injectVerdicts(s, []verdictEnt{{
		Family: "taker:spotlag@kalshi", SourceFamily: "spotlag",
		Platform: "kalshi", OriginLayer: "model", Route: "taker", Side: "YES", Group: "taker",
		N: 25, Mean: .08, MeanAsk: .40, FeePC: fee,
	}})
	key := gfRosterKey("spotlag", "kalshi", "YES", "taker")
	s.swMu.Lock()
	s.swTable = &swState{At: time.Now(), SubShare: map[string]float64{key: 100}}
	s.swMu.Unlock()
	return key
}

func r163ArmSelectedSpotlagYES(s *Server) {
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
		c.Risk.LiveSystemCanaryAllowlist = "kalshi|spotlag|YES|taker"
	})
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
}

// r163RecordDequeuedIntentNoSend models the final dispatcher taking ownership of a raw LIVE
// intent and finishing it before funded Paper may use the shared opportunity. Tests that manually
// dequeue the LIVE channel must publish this exact terminal; removing an in-memory item is not
// durable execution truth.
func r163RecordDequeuedIntentNoSend(t *testing.T, s *Server, intent liveSignalIntent) {
	t.Helper()
	if intent.ShadowAttemptID == "" {
		t.Fatal("test dispatcher received LIVE intent without execution-shadow lineage")
	}
	s.executionShadowRecordDrop(liveCandidateFromSignalIntent(intent),
		"test-live-dispatch-terminal", "hermetic dispatcher chose local no-send",
		time.Now().UTC(), map[string]any{"hermetic_test": true})
	s.wakeExecutionShadowWriter()
}

func r163AwaitDurableIntentNoSend(t *testing.T, s *Server, intent liveSignalIntent) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	terminal, found, err := s.waitForFundedPaperLiveTerminal(ctx, intent.ShadowAttemptID)
	if err != nil || !found {
		t.Fatalf("test dispatcher LIVE terminal was not durable: found=%v terminal=%+v err=%v",
			found, terminal, err)
	}
	if terminal.Kind != fundedPaperLiveTerminalNoSend || terminal.EventID == "" ||
		terminal.State == "" {
		t.Fatalf("test dispatcher produced unsafe LIVE terminal: %+v", terminal)
	}
}

func TestR167UnselectedPositivePaperJoinsDetectorLiveSkipShadowAndPaper(t *testing.T) {
	const ticker = "R163PAPER-ONLY"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	key := gfRosterKey("spotlag", "kalshi", "NO", "taker")
	r163ArmSelectedSpotlagYES(s) // the exact Spot-lag NO route remains research-only here.
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	paperOnlySignal := storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R163 Paper-only positive route",
		Side: "NO", SignalType: "spotlag", EntryPrice: .40, ResolveHours: 1,
	}
	if why := s.liveSystemSelectionReason(liveCandidateFromSignalIntent(
		liveSignalIntent{Signal: paperOnlySignal, At: time.Now()}), "taker"); why == "" {
		t.Fatal("Paper-only fixture unexpectedly has LIVE authority")
	}

	s.genfollowTriggeredSystems(context.Background(), paperOnlySignal)
	releaseShadow()
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopGenfollowPaperWorkers(drainCtx); err != nil {
		t.Fatalf("drain Paper worker: %v", err)
	}
	if err := s.stopLivePolicyMirrorWorker(drainCtx); err != nil {
		t.Fatalf("drain canonical execution shadow: %v", err)
	}
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		t.Fatalf("stop execution-shadow worker: %v", err)
	}

	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	open := 0
	if book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 1 {
		t.Fatalf("unselected positive system did not execute in funded Paper: open=%d book=%+v",
			open, book)
	}
	if s.liveSignalIntentCh != nil && len(s.liveSignalIntentCh) != 0 {
		t.Fatalf("Paper-only system entered LIVE intent queue: len=%d", len(s.liveSignalIntentCh))
	}
	rows, err := s.store.ListExecutionShadowAttempts(drainCtx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("Paper-only joined attempts=%d err=%v", len(rows), err)
	}
	if rows[0].Attempt.SignalContract == "" || rows[0].Attempt.InputTopology == "" {
		t.Fatalf("immutable exact contract was not frozen: %+v", rows[0].Attempt)
	}
	stages := map[string]storage.ExecutionShadowEvent{}
	for _, event := range rows[0].Events {
		stages[event.Stage] = event
	}
	for _, stage := range []string{"detector-branch", "live-selection",
		"counterfactual-execution-terminal", "funded-paper-terminal"} {
		if _, ok := stages[stage]; !ok {
			t.Fatalf("Paper-only opportunity missing %s: %+v", stage, rows[0].Events)
		}
	}
	if stages["live-selection"].LiveState != "not-sent" ||
		stages["live-selection"].VenueAttempted {
		t.Fatalf("Paper-only LIVE branch was not an explicit no-send: %+v", stages["live-selection"])
	}
	if stages["counterfactual-execution-terminal"].ShadowState != livePolicyMirrorModeledFill {
		t.Fatalf("money-free q1 execution result=%+v", stages["counterfactual-execution-terminal"])
	}
}

func TestR170FundedPaperCannotFillAfterCanonicalExecutionMiss(t *testing.T) {
	const ticker = "R170PAPER-CANONICAL-MISS"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	key := gfRosterKey("spotlag", "kalshi", "NO", "taker")
	r163ArmSelectedSpotlagYES(s) // NO remains a research-only Paper route.
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	var reads atomic.Int32
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		if reads.Add(1) == 1 {
			return in, "", false // the one canonical +750ms execution touch is unavailable
		}
		return quoteFor(in, .38, .40, 20) // a later Paper-only retry would look attractive
	}
	s.genfollowTriggeredSystems(context.Background(), storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "canonical miss cannot become Paper profit",
		Side: "NO", SignalType: "spotlag", EntryPrice: .40, ResolveHours: 1,
	})
	releaseShadow()
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopGenfollowPaperWorkers(drainCtx); err != nil {
		t.Fatalf("drain Paper worker: %v", err)
	}
	if err := s.stopLivePolicyMirrorWorker(drainCtx); err != nil {
		t.Fatalf("drain canonical execution shadow: %v", err)
	}
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		t.Fatalf("stop execution-shadow worker: %v", err)
	}
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	open := 0
	if book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 0 {
		t.Fatalf("later Paper book manufactured %d position(s) after canonical miss", open)
	}
	rows, err := s.store.ListExecutionShadowAttempts(drainCtx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("joined attempts=%d err=%v", len(rows), err)
	}
	stages := map[string]storage.ExecutionShadowEvent{}
	for _, event := range rows[0].Events {
		stages[event.Stage] = event
	}
	if stages["counterfactual-execution-terminal"].ShadowState != livePolicyMirrorNotObserved {
		t.Fatalf("canonical execution terminal=%+v", stages["counterfactual-execution-terminal"])
	}
	if stages["funded-paper-terminal"].PaperState != "PAPER-NOT-OBSERVED" {
		t.Fatalf("Paper terminal=%+v", stages["funded-paper-terminal"])
	}
	if reads.Load() != 1 {
		t.Fatalf("Paper retried the old opportunity against %d later book read(s)", reads.Load()-1)
	}
}

func TestR170FundedPaperConsumesCanonicalFillWithoutAThirdBookClock(t *testing.T) {
	const ticker = "R170PAPER-CANONICAL-FILL"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	key := gfRosterKey("spotlag", "kalshi", "NO", "taker")
	r163ArmSelectedSpotlagYES(s) // NO remains a research-only Paper route.
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	var reads atomic.Int32
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		if reads.Add(1) > 2 {
			return in, "", false
		}
		return quoteFor(in, .38, .40, 20)
	}
	s.genfollowTriggeredSystems(context.Background(), storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "canonical fill fans into Paper",
		Side: "NO", SignalType: "spotlag", EntryPrice: .40, ResolveHours: 1,
	})
	releaseShadow()
	drainCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := s.stopGenfollowPaperWorkers(drainCtx); err != nil {
		t.Fatalf("drain Paper worker: %v", err)
	}
	if err := s.stopLivePolicyMirrorWorker(drainCtx); err != nil {
		t.Fatalf("drain canonical execution shadow: %v", err)
	}
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		t.Fatalf("stop execution-shadow worker: %v", err)
	}
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	open := 0
	if book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 1 {
		t.Fatalf("shared canonical fill produced %d Paper positions, want 1", open)
	}
	if reads.Load() != 2 {
		t.Fatalf("funded Paper started a private execution clock: complete-book reads=%d want 2 canonical touches",
			reads.Load())
	}
	rows, err := s.store.ListExecutionShadowAttempts(drainCtx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("joined attempts=%d err=%v", len(rows), err)
	}
	var canonical, paper storage.ExecutionShadowEvent
	for _, event := range rows[0].Events {
		switch event.Stage {
		case "counterfactual-execution-terminal":
			canonical = event
		case "funded-paper-terminal":
			paper = event
		}
	}
	if canonical.ShadowState != livePolicyMirrorModeledFill ||
		paper.PaperState != "PAPER-FILLED" ||
		canonical.ShadowFillPrice == nil || paper.PaperFillPrice == nil ||
		math.Abs(*canonical.ShadowFillPrice-*paper.PaperFillPrice) > 1e-9 ||
		canonical.ShadowFee == nil || paper.PaperFee == nil ||
		math.Abs(*canonical.ShadowFee-*paper.PaperFee) > 1e-9 ||
		canonical.BookSource != paper.PaperBookSource {
		t.Fatalf("Paper did not consume the exact canonical economics: canonical=%+v paper=%+v",
			canonical, paper)
	}
}

func TestR163SelectedCanaryKeepsLiveFirstSharedLineage(t *testing.T) {
	const ticker = "R163SPOTLAG-LINEAGE"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	key := r163SelectSpotlagYES(t, s, ticker)
	r163ArmSelectedSpotlagYES(s)
	s.liveDispatchShadowQuietDelay = -1
	releaseShadow := holdExecutionShadowWriterStart(t, s)

	paperEntered := make(chan struct{})
	unblockPaper := make(chan struct{})
	var calls atomic.Int32
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		if calls.Add(1) == 2 {
			close(paperEntered)
			<-unblockPaper
		}
		return quoteFor(in, .38, .40, 20)
	}
	producerReturned := make(chan struct{})
	go func() {
		s.genfollowTriggeredSystems(context.Background(), storage.Signal{
			Platform: "kalshi", Ticker: ticker, Title: "R163 selected canary lineage",
			Side: "YES", SignalType: "spotlag", EntryPrice: .40, ResolveHours: 1,
		})
		close(producerReturned)
	}()
	select {
	case <-producerReturned:
	case <-time.After(250 * time.Millisecond):
		close(unblockPaper)
		t.Fatal("selected LIVE producer waited behind funded Paper")
	}
	if got := len(s.liveSignalIntentChannel()); got != 1 {
		close(unblockPaper)
		t.Fatalf("selected LIVE intent count=%d want 1 before LIVE terminal", got)
	}
	intent := <-s.liveSignalIntentChannel()
	if intent.ShadowAttemptID == "" {
		close(unblockPaper)
		t.Fatal("selected LIVE intent lost execution-shadow lineage")
	}
	// This retired-cash hermetic seam does not produce the ordinary post-terminal execution
	// comparison on its own. Schedule the same immutable-ID canonical receipt explicitly; funded
	// Paper is now forbidden from substituting a later private book when that receipt is absent.
	if !s.enqueueCanonicalSystemExecutionShadow(intent.Signal, intent.At, intent.ShadowAttemptID) {
		close(unblockPaper)
		t.Fatal("selected fixture could not schedule canonical execution receipt")
	}
	s.executionShadowMu.Lock()
	pendingBeforePaper := len(s.executionShadowPending)
	s.executionShadowMu.Unlock()
	if pendingBeforePaper != 2 {
		close(unblockPaper)
		t.Fatalf("selected detector group rows=%d want immutable attempt + detector event",
			pendingBeforePaper)
	}

	// Taking an intent from the channel is not a terminal. Paper must remain behind the exact
	// preflight terminal; R165's retired cash plan emits a labelled local no-send after the current
	// book/fee receipt and never queues an order.
	select {
	case <-paperEntered:
		close(unblockPaper)
		t.Fatal("funded Paper ran before the selected LIVE attempt had a durable terminal")
	default:
	}
	if s.processLiveAllowlistedSignalIntentJob(context.Background(), intent) {
		close(unblockPaper)
		t.Fatal("R165 cash-retired intent unexpectedly admitted an order")
	}
	releaseShadow()
	r163AwaitDurableIntentNoSend(t, s, intent)
	select {
	case <-paperEntered:
	case <-time.After(3 * time.Second):
		close(unblockPaper)
		t.Fatal("selected signal never reached funded Paper after durable LIVE no-send")
	}
	close(unblockPaper)
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopGenfollowPaperWorkers(drainCtx); err != nil {
		t.Fatalf("drain Paper worker: %v", err)
	}
	s.gfBookMu.Lock()
	book := s.gfLoadLocked().Subs[key]
	open := 0
	if book != nil {
		open = len(book.Open)
	}
	s.gfBookMu.Unlock()
	if open != 1 {
		t.Fatalf("selected signal did not retain funded Paper branch: open=%d book=%+v", open, book)
	}

	shadowCtx, cancelShadow := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShadow()
	if err := s.stopExecutionShadowWriter(shadowCtx); err != nil {
		s.liveMirrorMu.Lock()
		mirrorPending := len(s.liveMirrorQ)
		s.liveMirrorMu.Unlock()
		s.executionShadowMu.Lock()
		shadowPending, shadowInFlight := len(s.executionShadowPending), s.executionShadowInFlight
		s.executionShadowMu.Unlock()
		t.Fatalf("%v (raw=%d mirror=%d active=%d shadow_pending=%d shadow_in_flight=%d)",
			err, len(s.liveSignalIntentChannel()), mirrorPending,
			s.liveDispatchShadowLiveActive.Load(), shadowPending, shadowInFlight)
	}
	rows, err := s.store.ListExecutionShadowAttempts(shadowCtx, 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.AttemptID != intent.ShadowAttemptID {
		t.Fatalf("selected joined rows=%+v err=%v", rows, err)
	}
	stages := map[string]bool{}
	for _, event := range rows[0].Events {
		stages[event.Stage] = true
	}
	if !stages["detector-branch"] || !stages["funded-paper-terminal"] {
		t.Fatalf("selected attempt lost LIVE/Paper shared lineage stages: %+v", stages)
	}
}

func TestR167DuplicateSelectedSignalDoesNotInflateEconomicAttempts(t *testing.T) {
	s := testServer(t)
	r163ArmSelectedSpotlagYES(s)
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "R163SPOTLAG-DUP", Title: "R163 duplicate selected signal",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40,
	}
	if !s.queueLiveSignalIntentAt(sig, .08, time.Now()) {
		t.Fatal("first selected signal was not admitted")
	}
	s.executionShadowMu.Lock()
	afterFirst := len(s.executionShadowPending)
	s.executionShadowMu.Unlock()
	if afterFirst != 2 {
		t.Fatalf("first selected signal detector rows=%d want 2", afterFirst)
	}
	if s.queueLiveSignalIntentAt(sig, .08, time.Now()) {
		t.Fatal("duplicate selected signal was admitted as another LIVE intent")
	}
	s.executionShadowMu.Lock()
	afterDuplicate := len(s.executionShadowPending)
	s.executionShadowMu.Unlock()
	if afterDuplicate != afterFirst {
		t.Fatalf("duplicate selected signal inflated detector rows: before=%d after=%d",
			afterFirst, afterDuplicate)
	}
	if got := len(s.liveSignalIntentChannel()); got != 1 {
		t.Fatalf("duplicate selected signal changed LIVE queue len=%d want 1", got)
	}
	<-s.liveSignalIntentChannel()
	releaseShadow()
	shadowCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(shadowCtx); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err := s.store.ExecutionShadowDBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("duplicate selected signal persisted %d attempts want 1", attempts)
	}
}

func TestR163GenfollowDuplicatePaperHasNoUnpublishedShadowLineage(t *testing.T) {
	const ticker = "R163SPOTLAG-GENFOLLOW-DUP"
	s, _, quoteFor := r154PaperTakerFixture(t, ticker)
	r163SelectSpotlagYES(t, s, ticker)
	r163ArmSelectedSpotlagYES(s)
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	s.completeBookSignalFn = func(in storage.Signal) (storage.Signal, string, bool) {
		return quoteFor(in, .38, .40, 20)
	}
	sig := storage.Signal{
		Platform: "kalshi", Ticker: ticker, Title: "R163 genfollow duplicate",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40, ResolveHours: 1,
	}
	s.genfollowTriggeredSystems(context.Background(), sig)
	s.genfollowTriggeredSystems(context.Background(), sig)
	if got := len(s.liveSignalIntentChannel()); got != 1 {
		t.Fatalf("genfollow duplicate changed LIVE queue len=%d want 1", got)
	}
	intent := <-s.liveSignalIntentChannel()
	if s.processLiveAllowlistedSignalIntentJob(context.Background(), intent) {
		t.Fatal("R165 cash-retired duplicate fixture unexpectedly admitted an order")
	}
	releaseShadow()
	r163AwaitDurableIntentNoSend(t, s, intent)
	drainCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopGenfollowPaperWorkers(drainCtx); err != nil {
		t.Fatalf("drain Paper worker: %v", err)
	}
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(drainCtx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("joined rows=%d err=%v", len(rows), err)
	}
	paperTerminals := 0
	for _, row := range rows {
		for _, event := range row.Events {
			if event.Stage == "funded-paper-terminal" {
				paperTerminals++
			}
		}
	}
	if paperTerminals != 1 {
		t.Fatalf("duplicate Paper callback inflated the canonical sample: terminals=%d rows=%+v",
			paperTerminals, rows)
	}
	if dropped := s.executionShadowDropped.Load(); dropped != 0 {
		t.Fatalf("duplicate Paper branch produced orphan retry/drop traffic: dropped=%d", dropped)
	}
}

func TestR167DisarmedDuplicateDoesNotInflateEconomicAttempts(t *testing.T) {
	s := testServer(t)
	s.mutateCfg(func(c *config.Config) {
		c.Risk.LiveProspectiveAllocation = true
		c.Risk.LiveSystemKalshi = true
		c.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	})
	releaseShadow := holdExecutionShadowWriterStart(t, s)
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "R163SPOTLAG-DISARMED-DUP",
		Title: "R163 disarmed duplicate", Side: "YES", SignalType: "spotlag",
		EntryPrice: .40,
	}
	if s.queueLiveSignalIntentAt(sig, .08, time.Now()) {
		t.Fatal("disarmed selected signal entered LIVE queue")
	}
	s.executionShadowMu.Lock()
	afterFirst := len(s.executionShadowPending)
	s.executionShadowMu.Unlock()
	if afterFirst != 3 {
		t.Fatalf("first disarmed selected signal rows=%d want attempt + detector + durable no-send",
			afterFirst)
	}
	if s.queueLiveSignalIntentAt(sig, .08, time.Now()) {
		t.Fatal("disarmed duplicate entered LIVE queue")
	}
	s.executionShadowMu.Lock()
	afterDuplicate := len(s.executionShadowPending)
	s.executionShadowMu.Unlock()
	if afterDuplicate != afterFirst {
		t.Fatalf("disarmed duplicate inflated detector rows: before=%d after=%d",
			afterFirst, afterDuplicate)
	}
	releaseShadow()
	shadowCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(shadowCtx); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err := s.store.ExecutionShadowDBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("disarmed duplicate persisted %d attempts want 1", attempts)
	}
	rows, err := s.store.ListExecutionShadowAttempts(shadowCtx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("disarmed joined rows=%d err=%v", len(rows), err)
	}
	noSendTerminals := 0
	for _, row := range rows {
		for _, event := range row.Events {
			if event.LiveState == "not-sent" {
				noSendTerminals++
			}
		}
	}
	if noSendTerminals != 1 {
		t.Fatalf("disarmed duplicate durable no-send terminals=%d want 1 rows=%+v",
			noSendTerminals, rows)
	}
}
