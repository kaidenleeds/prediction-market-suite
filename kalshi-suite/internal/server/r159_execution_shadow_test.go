package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func newExecutionShadowTestServer(t *testing.T) (*Server, *storage.Store) {
	t.Helper()
	st, err := storage.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		liveDispatchShadowQuietDelay: -1}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.stopExecutionShadowWriter(ctx)
		_ = st.Close()
	})
	return s, st
}

// holdExecutionShadowWriterStart lets a test enqueue a complete diagnostic fixture before the
// nonblocking writer begins. Production producers intentionally use TryLock and may drop telemetry
// when they race the writer; that behavior is covered separately and must not make lineage tests
// depend on goroutine scheduling.
func holdExecutionShadowWriterStart(t *testing.T, s *Server) func() {
	t.Helper()
	writerCtx, cancel := context.WithCancel(context.Background())
	s.executionShadowMu.Lock()
	if s.executionShadowWake != nil {
		s.executionShadowMu.Unlock()
		cancel()
		t.Fatal("execution-shadow writer already initialized")
	}
	s.executionShadowWake = make(chan struct{}, 1)
	s.executionShadowDone = make(chan struct{})
	s.executionShadowCancel = cancel
	s.executionShadowMu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			go s.runExecutionShadowWriter(writerCtx)
			s.wakeExecutionShadowWriter()
		})
	}
	t.Cleanup(release)
	return release
}

func holdExecutionShadowDBWrite(t *testing.T, st *storage.Store) func() {
	t.Helper()
	conn, err := st.ExecutionShadowDBForTest().Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
			_ = conn.Close()
		})
	}
	t.Cleanup(release)
	return release
}

func TestR159ExecutionShadowHotCaptureIsMemoryOnlyAndJoinsDetectorBranches(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "KXR159-JOIN", Title: "Joined signal", Side: "NO",
		SignalType: "spotlag", EntryPrice: .41, BookSource: "signal-book",
	}
	bid, ask, depth, age, tick := .40, .41, 12.0, .075, .01
	sig.BookBid, sig.BookAsk, sig.BookAskDepth, sig.BookQuoteAgeS, sig.BookTakerTick =
		&bid, &ask, &depth, &age, &tick

	// Hold the diagnostic worker itself. The producer still returns after an in-memory append; it
	// cannot touch either SQLite file or wait for the shadow worker.
	started := time.Now()
	triggerAt := time.Now().UTC().Truncate(time.Millisecond)
	attemptID := s.executionShadowBeginSignal(sig, .08, triggerAt)
	if attemptID == "" || time.Since(started) > 20*time.Millisecond {
		t.Fatalf("hot capture id=%q took %v", attemptID, time.Since(started))
	}
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
		Action: "BUY", Family: sig.SignalType, Source: "auto-cons-" + sig.SignalType,
		Price: sig.EntryPrice, At: triggerAt, ShadowAttemptID: attemptID,
	}
	quote := liveMirrorQuote{
		Price: ask, Depth: depth, Tick: tick, SpreadCents: 1,
		BookSource: "kalshi_ws_full_orderbook:g1:s2:q3",
		SourceAt:   triggerAt, ObservedAt: time.Now().UTC(),
	}
	s.executionShadowRecordLiveResult(candidate, quote, "risk-r159", "order-r159",
		"filled", "test-order-receipt", 1, 1, ask, .01, true, true, true, "", nil)
	s.executionShadowRecordPaperAttempt(attemptID, "PAPER-FILLED", "", "paper-r159",
		time.Now().UTC(), 1, ask, 1, ask, .01, "paper-exact-fee", sig)
	s.executionShadowRecordMirror(candidate, storage.LivePolicyMirrorRow{
		ProcessedAt: time.Now().UTC(), State: storage.LivePolicyMirrorFilled,
		LimitPrice: ask, RequestedQty: 1, FilledQty: 1, FillPrice: ask,
		FeeUSD: .01, FeeSource: "mirror-exact-fee", BookSource: quote.BookSource,
	})
	var before int
	if err := st.ExecutionShadowDBForTest().QueryRow(`SELECT COUNT(*) FROM execution_shadow_attempts`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatal("hot detector branch performed SQLite work before the cash path")
	}
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.AttemptID != attemptID ||
		len(rows[0].Events) != 4 || rows[0].Events[0].Stage != "detector-branch" {
		t.Fatalf("joined branch rows=%+v err=%v", rows, err)
	}
	if rows[0].Attempt.TriggerUnixMS != triggerAt.UnixMilli() {
		t.Fatalf("trigger reset: got=%d want=%d",
			rows[0].Attempt.TriggerUnixMS, triggerAt.UnixMilli())
	}
	stages := map[string]bool{}
	for _, stage := range rows[0].Events {
		stages[stage.Stage] = true
		wantElapsed := stage.At.UnixMilli() - rows[0].Attempt.TriggerUnixMS
		if stage.ElapsedFromTriggerMS != wantElapsed || stage.ElapsedFromTriggerMS < 0 {
			t.Fatalf("stage %s reset detector-relative clock: got=%d want=%d",
				stage.Stage, stage.ElapsedFromTriggerMS, wantElapsed)
		}
	}
	for _, stage := range []string{
		"detector-branch", "live-terminal", "funded-paper-terminal", "counterfactual-terminal",
	} {
		if !stages[stage] {
			t.Fatalf("shared trigger attempt missing %s: %+v", stage, stages)
		}
	}
	event := rows[0].Events[0]
	if event.SideAsk == nil || *event.SideAsk != ask || event.BookAgeMS == nil ||
		*event.BookAgeMS != 75 || event.Evidence["live_published_first"] != true {
		t.Fatalf("model-book branch evidence incomplete: %+v", event)
	}
	response := httptest.NewRecorder()
	s.handleExecutionShadow(response,
		httptest.NewRequest(http.MethodGet, "/api/execution-shadow?limit=10", nil))
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"trigger_unix_ms":`) ||
		!strings.Contains(response.Body.String(), `"elapsed_from_trigger_ms":`) ||
		!strings.Contains(response.Body.String(), `"database_file":"execution_shadow.db"`) ||
		!strings.Contains(response.Body.String(), `"database_bytes":`) ||
		!strings.Contains(response.Body.String(), `"wal_bytes":`) {
		t.Fatalf("API omitted numeric trigger/elapsed contract: status=%d body=%s",
			response.Code, response.Body.String())
	}
}

func TestR163ExecutionShadowPersistIsIndependentOfCashPreflight(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	attempt := storage.ExecutionShadowAttempt{
		AttemptID: "exec-r159-priority", SignalDecisionID: "decision-r159-priority",
		ObservedAt: now, TriggerUnixMS: now.UnixMilli(),
		Venue: "kalshi", Ticker: "KXR159-PRIORITY", Side: "YES", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalSource: "spotlag",
		QualificationBasis: "priority-test",
	}
	endCashPriority := s.beginLiveCashPriority()
	defer endCashPriority()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.persistExecutionShadowWrite(
			ctx, executionShadowWrite{attempt: &attempt})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("isolated diagnostic write waited on cash preflight")
	}
	var after int
	if err := st.ExecutionShadowDBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts WHERE attempt_id=?`,
		attempt.AttemptID).Scan(&after); err != nil || after != 1 {
		t.Fatalf("post-priority diagnostic count=%d err=%v", after, err)
	}
	var leaked int
	if err := st.DBForTest().QueryRow(
		`SELECT COUNT(*) FROM execution_shadow_attempts WHERE attempt_id=?`,
		attempt.AttemptID).Scan(&leaked); err != nil || leaked != 0 {
		t.Fatalf("isolated diagnostic leaked into cash DB: count=%d err=%v", leaked, err)
	}
}

func TestR159CandidateAuditEveryBatchYieldsToCashPreflight(t *testing.T) {
	s := testServer(t)
	s.liveDispatchShadowQuietDelay = -1
	called := make(chan struct{}, 1)
	s.liveCandidateAuditWrite = func(context.Context, []storage.AuditEntry) error {
		called <- struct{}{}
		return nil
	}
	endCashPriority := s.beginLiveCashPriority()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- s.persistLiveCandidateAudit(ctx, []storage.AuditEntry{{
			Level: "info", Category: "r159-priority", Message: "queued diagnostic",
		}})
	}()
	select {
	case <-called:
		t.Fatal("candidate diagnostic acquired the writer while cash preflight was active")
	case <-time.After(25 * time.Millisecond):
	}
	endCashPriority()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("candidate diagnostic did not resume after cash preflight finished")
	}
	select {
	case <-called:
	default:
		t.Fatal("candidate diagnostic never reached its sink after cash priority cleared")
	}
}

func TestR159PreflightSchedulerPreservesAttemptIDAcrossConcurrentSameSignalJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var got []string
	completed := make(chan string, 2)
	scheduler := newLiveSignalPreflightScheduler(nil, ctx, 2, 8,
		func(_ context.Context, intent liveSignalIntent) bool {
			mu.Lock()
			got = append(got, intent.ShadowAttemptID)
			mu.Unlock()
			completed <- intent.ShadowAttemptID
			return true
		}, nil)
	defer scheduler.stop()
	now := time.Now()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR159-SAME", Side: "YES",
		SignalType: "spotlag", EntryPrice: .40}
	batch := scheduler.submit(ctx, []liveSignalIntent{
		{Signal: sig, Point: .08, At: now, ShadowAttemptID: "exec-first"},
		{Signal: sig, Point: .07, At: now, ShadowAttemptID: "exec-retry"},
	})
	_ = batch
	for i := 0; i < 2; i++ {
		select {
		case <-completed:
		case <-time.After(time.Second):
			mu.Lock()
			current := append([]string(nil), got...)
			mu.Unlock()
			t.Fatalf("scheduler did not run both identity fixtures: %v", current)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] == got[1] {
		t.Fatalf("same-ticker concurrent jobs lost immutable attempt lineage: %v", got)
	}
	seen := map[string]bool{got[0]: true, got[1]: true}
	if !seen["exec-first"] || !seen["exec-retry"] {
		t.Fatalf("scheduler re-resolved attempt ids instead of carrying them: %v", got)
	}
}

func TestR159PaperQueueCarriesExplicitAttemptIDInsteadOfLatestTickerLookup(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR159-PAPER", Title: "same ticker",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40, BookSource: "paper-signal"}
	first := s.executionShadowBeginSignal(sig, .08, time.Now())
	second := s.executionShadowBeginSignal(sig, .09, time.Now().Add(time.Nanosecond))
	if first == "" || second == "" || first == second {
		t.Fatalf("bad attempt ids first=%q second=%q", first, second)
	}
	s.genfollowPaperWorkerMu.Lock()
	s.genfollowPaperWorkerStopping = true
	s.genfollowPaperWorkerMu.Unlock()
	if s.enqueueGenfollowPaperIntent([]genfollowPaperSignal{
		{Signal: sig, ShadowAttemptID: first},
		{Signal: sig, ShadowAttemptID: second},
	}) {
		t.Fatal("stopping Paper worker unexpectedly accepted fixture")
	}
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	paperByAttempt := map[string]int{}
	for _, row := range rows {
		for _, event := range row.Events {
			if event.Stage == "funded-paper-scheduler" &&
				event.PaperState == "PAPER-DEFERRED" {
				paperByAttempt[row.Attempt.AttemptID]++
			}
		}
	}
	if paperByAttempt[first] != 1 || paperByAttempt[second] != 1 {
		t.Fatalf("Paper same-ticker decisions attached to latest map entry: %+v", paperByAttempt)
	}
}

func TestR159SameTickerZeroFillNeverInheritsLaterAttemptSettlement(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	// Keep both synthetic detector attempts in the past. Settlement Event.At truthfully records
	// when this test process joins the result and may not precede an invented future trigger.
	now := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Millisecond)
	ticker := "KXR159-SAME-TICKER-SETTLE"
	insertAttempt := func(id string, at time.Time) {
		t.Helper()
		_, err := st.InsertExecutionShadowAttempt(ctx, storage.ExecutionShadowAttempt{
			AttemptID: id, SignalDecisionID: "decision-" + id,
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(),
			Venue: "kalshi", Ticker: ticker, Side: "YES", Action: "BUY",
			SystemID: "kalshi-flow", Route: "taker", SignalSource: "kalshi-flow",
			QualificationBasis: "same-ticker-settlement-test",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	firstAt, secondAt := now, now.Add(time.Second)
	insertAttempt("exec-zero-fill", firstAt)
	insertAttempt("exec-real-fill", secondAt)
	zero := storage.ExecutionShadowEvent{
		EventID: "event-zero-fill", AttemptID: "exec-zero-fill", At: firstAt,
		ElapsedFromTriggerMS: 0, Stage: "live-terminal", Outcome: "unfilled",
		LiveReservationID: "risk-zero-fill", VenueAttempted: true, VenueAck: true,
		VenueOrderID: "order-zero-fill", LiveState: "unfilled", LiveAuthoritative: true,
		LiveFilledQty: floatPtrShadow(0), LiveFee: floatPtrShadow(0),
		LiveFeeSource: "order-scoped-terminal-zero-fill",
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, zero); err != nil {
		t.Fatal(err)
	}
	fill := storage.ExecutionShadowEvent{
		EventID: "event-real-fill", AttemptID: "exec-real-fill", At: secondAt,
		ElapsedFromTriggerMS: 0, Stage: "live-terminal", Outcome: "filled",
		LiveReservationID: "risk-real-fill", VenueAttempted: true, VenueAck: true,
		VenueOrderID: "order-real-fill", LiveState: "filled", LiveAuthoritative: true,
		LiveFilledQty: floatPtrShadow(1), LiveFillPrice: floatPtrShadow(.40),
		LiveFee: floatPtrShadow(.01), LiveFeeSource: "order-scoped-terminal-fill",
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, fill); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.RecordVenueSettlement(ctx, "kalshi", ticker, 1,
		now.Add(time.Hour), "test-final-settlement"); err != nil || !inserted {
		t.Fatalf("settlement inserted=%v err=%v", inserted, err)
	}
	s.settleExecutionShadowLedger(ctx)
	// Settlement telemetry now uses the retained isolated writer instead of a direct database
	// write. Join that writer before asserting durable rows; the production final-snapshot path
	// owns the same drain boundary.
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		cancelDrain()
		t.Fatalf("settlement shadow drain: %v", err)
	}
	cancelDrain()
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	liveNetByAttempt := map[string][]float64{}
	settlementRows := map[string]int{}
	for _, row := range rows {
		for _, event := range row.Events {
			if event.SettlementKnown {
				settlementRows[row.Attempt.AttemptID]++
			}
			if event.LiveNet != nil {
				liveNetByAttempt[row.Attempt.AttemptID] =
					append(liveNetByAttempt[row.Attempt.AttemptID], *event.LiveNet)
			}
		}
	}
	if got := liveNetByAttempt["exec-zero-fill"]; len(got) != 1 || math.Abs(got[0]) > 1e-12 {
		t.Fatalf("zero-fill attempt did not receive its own exact zero economics: %+v", liveNetByAttempt)
	}
	if got := liveNetByAttempt["exec-real-fill"]; len(got) != 1 ||
		math.Abs(got[0]-.59) > 1e-9 {
		t.Fatalf("real filled attempt settlement=%+v want +0.59", got)
	}
	if settlementRows["exec-zero-fill"] != 1 || settlementRows["exec-real-fill"] != 1 {
		t.Fatalf("settlement outcome rows repeated or missing: %+v", settlementRows)
	}
	if open, openErr := st.ListOpenExecutionShadowAttempts(ctx, 10); openErr != nil || len(open) != 0 {
		t.Fatalf("fully settled same-ticker attempts remained open: len=%d err=%v", len(open), openErr)
	}
}

func TestR168TerminalNoFillSettlementKeepsZeroKnownAndNotObservedUnknown(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	fixtures := []struct {
		id, ticker, state string
	}{
		{"exec-shadow-zero", "KXR168-SHADOW-ZERO", "modeled_zero_fill"},
		{"exec-shadow-missing", "KXR168-SHADOW-MISSING", "not_observed"},
	}
	for i, fixture := range fixtures {
		at := now.Add(time.Duration(i) * time.Millisecond)
		attempt := storage.ExecutionShadowAttempt{
			AttemptID: fixture.id, SignalDecisionID: "decision-" + fixture.id,
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: "kalshi",
			Ticker: fixture.ticker, Side: "YES", Action: "BUY", SystemID: "spotlag",
			Route: "taker", SignalSource: "r168-test", SignalContract: "r147sc-v1|spotlag|kalshi|YES|taker|K-SPOT",
			InputTopology: "K-SPOT", QualificationBasis: "r168 no-fill settlement test",
		}
		if inserted, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil || !inserted {
			t.Fatalf("insert %s inserted=%v err=%v", fixture.id, inserted, err)
		}
		terminal := storage.ExecutionShadowEvent{
			EventID: fixture.id + "-terminal", AttemptID: fixture.id, At: at.Add(time.Millisecond),
			ElapsedFromTriggerMS: 1, Stage: "counterfactual-execution-terminal",
			Outcome: fixture.state, ShadowState: fixture.state,
		}
		if inserted, err := st.AppendExecutionShadowEvent(ctx, terminal); err != nil || !inserted {
			t.Fatalf("append %s inserted=%v err=%v", fixture.id, inserted, err)
		}
		if inserted, err := st.RecordVenueSettlement(ctx, "kalshi", fixture.ticker, 1,
			now.Add(time.Hour), "r168-authoritative-settlement"); err != nil || !inserted {
			t.Fatalf("settlement %s inserted=%v err=%v", fixture.id, inserted, err)
		}
	}

	s.settleExecutionShadowLedger(ctx)
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		cancelDrain()
		t.Fatalf("settlement shadow drain: %v", err)
	}
	cancelDrain()

	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	settlements := map[string][]storage.ExecutionShadowEvent{}
	for _, row := range rows {
		for _, event := range row.Events {
			if event.SettlementKnown {
				settlements[row.Attempt.AttemptID] = append(settlements[row.Attempt.AttemptID], event)
			}
		}
	}
	zeroRows, missingRows := settlements["exec-shadow-zero"], settlements["exec-shadow-missing"]
	if len(zeroRows) != 1 || zeroRows[0].ShadowNet == nil || math.Abs(*zeroRows[0].ShadowNet) > 1e-12 {
		t.Fatalf("modeled zero-fill settlement=%+v, want one exact zero", zeroRows)
	}
	if len(missingRows) != 1 || missingRows[0].ShadowNet != nil || missingRows[0].LiveNet != nil ||
		missingRows[0].PaperNet != nil {
		t.Fatalf("not-observed settlement invented economics: %+v", missingRows)
	}
	if open, openErr := st.ListOpenExecutionShadowAttempts(ctx, 10); openErr != nil || len(open) != 0 {
		t.Fatalf("outcome-complete no-fill attempts remained open: len=%d err=%v", len(open), openErr)
	}
	report, err := st.ExecutionShadowProfitFunnel(ctx, storage.ProfitFunnelQuery{
		Systems: []string{"spotlag"}, GroupBy: []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Totals.Settled != 2 || report.Totals.Shadow.ZeroFill != 1 ||
		report.Totals.Shadow.NotObserved != 1 || report.Totals.FeeNet.Shadow.KnownSettled != 1 ||
		report.Totals.FeeNet.Shadow.USD == nil || math.Abs(*report.Totals.FeeNet.Shadow.USD) > 1e-12 ||
		report.Totals.Gaps.Unsettled != 0 {
		t.Fatalf("no-fill funnel truth=%+v", report.Totals)
	}
}

func TestR169ExecutionShadowSettlementCursorResumesPastUnresolvedPrefix(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	targetID, targetTicker := "", ""
	for i := 0; i <= executionShadowSettlementBatch; i++ {
		at := now.Add(time.Duration(i) * time.Millisecond)
		attemptID := fmt.Sprintf("exec-r169-fair-%04d", i)
		ticker := fmt.Sprintf("KXR169-FAIR-%04d", i)
		attempt := storage.ExecutionShadowAttempt{
			AttemptID: attemptID, SignalDecisionID: "decision-" + attemptID,
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: "kalshi",
			Ticker: ticker, Side: "YES", Action: "BUY", SystemID: "spotlag",
			Route: "taker", SignalSource: "r169-test",
			SignalContract: "r147sc-v1|spotlag|kalshi|YES|taker|K-SPOT",
			InputTopology:  "K-SPOT", QualificationBasis: "r169 fair settlement cursor test",
		}
		if inserted, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil || !inserted {
			t.Fatalf("insert attempt %d inserted=%v err=%v", i, inserted, err)
		}
		terminal := storage.ExecutionShadowEvent{
			EventID: attemptID + "-terminal", AttemptID: attemptID, At: at.Add(time.Millisecond),
			ElapsedFromTriggerMS: 1, Stage: "counterfactual-execution-terminal",
			Outcome: "not_observed", ShadowState: "not_observed",
		}
		if i == executionShadowSettlementBatch {
			terminal.Outcome, terminal.ShadowState = "modeled_fill", "modeled_fill"
			terminal.ShadowFilledQty = floatPtrShadow(1)
			terminal.ShadowFillPrice = floatPtrShadow(.40)
			terminal.ShadowFee = floatPtrShadow(.01)
			terminal.ShadowFeeSource = "r169-exact-test-fee"
			targetID, targetTicker = attemptID, ticker
		}
		if inserted, err := st.AppendExecutionShadowEvent(ctx, terminal); err != nil || !inserted {
			t.Fatalf("append terminal %d inserted=%v err=%v", i, inserted, err)
		}
	}
	if inserted, err := st.RecordVenueSettlement(ctx, "kalshi", targetTicker, 1,
		now.Add(time.Hour), "r169 authoritative settlement"); err != nil || !inserted {
		t.Fatalf("target settlement inserted=%v err=%v", inserted, err)
	}

	// The first pass examines and advances past the entire unresolved prefix. It cannot yet see the
	// target just beyond the bounded page.
	s.settleExecutionShadowLedger(ctx)
	targetBefore, err := st.ExecutionShadowAttemptsByIDs(ctx, []string{targetID})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range targetBefore[targetID].Events {
		if event.SettlementKnown {
			t.Fatal("target beyond the first settlement page was graded too early")
		}
	}

	// A new Server value proves the cursor lives with the isolated ledger rather than only in
	// process memory. The resumed pass reaches the already-resolved target instead of restarting
	// forever at the same unresolved head.
	resumed := &Server{store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		liveDispatchShadowQuietDelay: -1}
	resumed.settleExecutionShadowLedger(ctx)
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	if err = resumed.stopExecutionShadowWriter(drainCtx); err != nil {
		cancelDrain()
		t.Fatalf("resumed settlement shadow drain: %v", err)
	}
	cancelDrain()

	targetAfter, err := st.ExecutionShadowAttemptsByIDs(ctx, []string{targetID})
	if err != nil {
		t.Fatal(err)
	}
	var settlementRows int
	for _, event := range targetAfter[targetID].Events {
		if !event.SettlementKnown {
			continue
		}
		settlementRows++
		if event.ShadowNet == nil || math.Abs(*event.ShadowNet-.59) > 1e-9 {
			t.Fatalf("target settlement economics=%+v, want +0.59", event)
		}
	}
	if settlementRows != 1 {
		t.Fatalf("target settlement rows=%d, want exactly one", settlementRows)
	}
}

func TestR169BackfilledSettlementKeepsObservationClockAfterTrigger(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	trigger := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	resolvedAt := trigger.Add(-time.Hour)
	attempt := storage.ExecutionShadowAttempt{
		AttemptID:        "exec-r169-backfilled-settlement",
		SignalDecisionID: "decision-r169-backfilled-settlement",
		ObservedAt:       trigger, TriggerUnixMS: trigger.UnixMilli(), Venue: "kalshi",
		Ticker: "KXR169-BACKFILLED", Side: "YES", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalSource: "r169-test",
		QualificationBasis: "backfilled settlement observation-clock test",
	}
	if inserted, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil || !inserted {
		t.Fatalf("insert attempt inserted=%v err=%v", inserted, err)
	}
	terminal := storage.ExecutionShadowEvent{
		EventID: "exec-r169-backfilled-terminal", AttemptID: attempt.AttemptID,
		At: trigger.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
		Stage: "counterfactual-execution-terminal", Outcome: "modeled_zero_fill",
		ShadowState: "modeled_zero_fill",
	}
	if inserted, err := st.AppendExecutionShadowEvent(ctx, terminal); err != nil || !inserted {
		t.Fatalf("append terminal inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.RecordVenueSettlement(ctx, "kalshi", attempt.Ticker, 1,
		resolvedAt, "r169 historical settlement"); err != nil || !inserted {
		t.Fatalf("record settlement inserted=%v err=%v", inserted, err)
	}

	s.settleExecutionShadowLedger(ctx)
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		cancelDrain()
		t.Fatalf("settlement shadow drain: %v", err)
	}
	cancelDrain()

	rows, err := st.ExecutionShadowAttemptsByIDs(ctx, []string{attempt.AttemptID})
	if err != nil {
		t.Fatal(err)
	}
	var settlement *storage.ExecutionShadowEvent
	for i := range rows[attempt.AttemptID].Events {
		event := &rows[attempt.AttemptID].Events[i]
		if event.SettlementKnown {
			settlement = event
			break
		}
	}
	if settlement == nil {
		t.Fatal("backfilled settlement event missing")
	}
	if settlement.At.Before(trigger) || settlement.ElapsedFromTriggerMS < 0 {
		t.Fatalf("settlement observation clock=%v elapsed=%d precedes trigger %v",
			settlement.At, settlement.ElapsedFromTriggerMS, trigger)
	}
	if !settlement.SettledAt.Equal(resolvedAt) {
		t.Fatalf("venue resolution clock=%v want %v", settlement.SettledAt, resolvedAt)
	}
}

func TestR159ExecutionShadowContentionCannotBlockOrDisplaceLiveIntent(t *testing.T) {
	s := testServer(t)
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	s.cfgP.Store(&cfg)
	s.liveMu.Lock()
	s.liveArmed, s.liveAuto = true, true
	s.liveMu.Unlock()
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR159-LIVE-FIRST",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40}

	// Simulate the background writer holding its mutex. ID generation, the actual LIVE channel
	// publication, and the complete two-row diagnostic group must still complete immediately.
	// The diagnostic group takes the lock-free ingress rather than becoming evidence loss.
	s.executionShadowMu.Lock()
	started := time.Now()
	accepted := s.queueLiveSignalIntentAt(sig, .08, time.Now())
	elapsed := time.Since(started)
	liveQueued := len(s.liveSignalIntentChannel())
	s.executionShadowMu.Unlock()
	if !accepted || liveQueued != 1 {
		t.Fatalf("diagnostic contention displaced LIVE: accepted=%v queued=%d",
			accepted, liveQueued)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("diagnostic mutex blocked the LIVE producer for %v", elapsed)
	}
	if s.liveDispatchShadowLivePriorityAt.Load() <= 0 {
		t.Fatal("successful LIVE intent publication did not stamp diagnostic writer priority")
	}
	intent := <-s.liveSignalIntentChannel()
	if intent.ShadowAttemptID == "" {
		t.Fatal("LIVE intent lost immutable execution-shadow lineage")
	}
	if got := s.executionShadowDropped.Load(); got != 0 {
		t.Fatalf("contended two-row detector group was dropped: %d", got)
	}
	if got := s.executionShadowDropContention.Load(); got != 0 {
		t.Fatalf("contention remained a drop reason: %d", got)
	}
	if got := s.executionShadowRerouted.Load(); got != 2 {
		t.Fatalf("contended detector records rerouted=%d want=2", got)
	}
	if got := s.executionShadowDropCapacity.Load(); got != 0 {
		t.Fatalf("contention was mislabeled as capacity pressure: %d", got)
	}
}

func TestR159ExecutionShadowQueueIsBoundedAndReportsPressure(t *testing.T) {
	s, _ := newExecutionShadowTestServer(t)
	s.executionShadowPendingCap = 2
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	first := storage.Signal{Platform: "kalshi", Ticker: "KXR159-BOUND-1",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	second := first
	second.Ticker = "KXR159-BOUND-2"
	firstAt := time.Now()
	firstID := s.executionShadowSignalAttemptID(first, .08, firstAt)
	s.executionShadowPublishSignal(first, .08, firstAt, firstID, false)
	secondAt := firstAt.Add(time.Nanosecond)
	secondID := s.executionShadowSignalAttemptID(second, .08, secondAt)
	s.executionShadowPublishSignal(second, .08, secondAt, secondID, false)

	s.executionShadowMu.Lock()
	occupancy := len(s.executionShadowPending) + s.executionShadowInFlight
	s.executionShadowMu.Unlock()
	if occupancy != 2 || occupancy > s.executionShadowPendingCap {
		t.Fatalf("bounded telemetry occupancy=%d capacity=%d",
			occupancy, s.executionShadowPendingCap)
	}
	if got := s.executionShadowDropped.Load(); got != 2 {
		t.Fatalf("overflow dropped=%d want=2-row atomic group", got)
	}
	if got := s.executionShadowDropCapacity.Load(); got != 2 {
		t.Fatalf("overflow capacity reason=%d want=2-row atomic group", got)
	}
	if got := s.executionShadowDropContention.Load(); got != 0 {
		t.Fatalf("capacity pressure was mislabeled as contention: %d", got)
	}
	response := httptest.NewRecorder()
	s.handleExecutionShadow(response,
		httptest.NewRequest(http.MethodGet, "/api/execution-shadow?limit=1", nil))
	if response.Code != http.StatusOK ||
		!strings.Contains(response.Body.String(), `"telemetry_dropped":2`) ||
		!strings.Contains(response.Body.String(), `"capacity":2`) ||
		!strings.Contains(response.Body.String(), `"contention":0`) ||
		!strings.Contains(response.Body.String(), `"telemetry_capacity":2`) {
		t.Fatalf("API hid telemetry pressure: status=%d body=%s",
			response.Code, response.Body.String())
	}
	releaseWriter()
}

func TestR163ExecutionShadowProductionCapacityHoldsMeasuredBurst(t *testing.T) {
	s, _ := newExecutionShadowTestServer(t)
	if executionShadowPendingMax != 32768 {
		t.Fatalf("production capacity=%d want=32768", executionShadowPendingMax)
	}
	// Keep the worker unstarted so this tests queue admission deterministically. The measured
	// R163 burst exceeded the former 4,096-row bound before its quiet-period drain began.
	s.executionShadowMu.Lock()
	s.executionShadowWake = make(chan struct{}, 1)
	s.executionShadowDone = make(chan struct{})
	s.executionShadowMu.Unlock()
	event := storage.ExecutionShadowEvent{EventID: "burst-fixture"}
	write := executionShadowWrite{event: &event}
	const burstRows = 5000
	for i := 0; i < burstRows; i++ {
		if !s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{write}, false) {
			t.Fatalf("measured burst row %d was refused", i)
		}
	}
	s.executionShadowMu.Lock()
	pending := len(s.executionShadowPending)
	s.executionShadowPending = nil
	s.executionShadowWake = nil
	s.executionShadowDone = nil
	s.executionShadowOccupancy.Store(0)
	s.executionShadowInFlightCount.Store(0)
	s.executionShadowWakeRef.Store(nil)
	s.executionShadowMu.Unlock()
	if pending != burstRows || s.executionShadowDropped.Load() != 0 {
		t.Fatalf("burst pending=%d dropped=%d want=%d/0",
			pending, s.executionShadowDropped.Load(), burstRows)
	}
}

func TestR169ExecutionShadowRejectsCausallyNegativeGroupBeforeQueue(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	trigger := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	badAttempt := storage.ExecutionShadowAttempt{
		AttemptID: "exec-r169-negative-group", SignalDecisionID: "decision-r169-negative-group",
		ObservedAt: trigger, TriggerUnixMS: trigger.UnixMilli(), Venue: "kalshi",
		Ticker: "KXR169-NEGATIVE", Side: "YES", Action: "BUY", SystemID: "spotlag",
		Route: "taker", SignalSource: "r169-test", QualificationBasis: "negative-group-test",
	}
	badEvent := storage.ExecutionShadowEvent{
		EventID: "exec-r169-negative-event", AttemptID: badAttempt.AttemptID,
		At: trigger.Add(-time.Second), ElapsedFromTriggerMS: executionShadowElapsedUnknownMS,
		Stage: "settlement", Outcome: "settled",
	}
	if s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{
		{attempt: &badAttempt}, {event: &badEvent},
	}, true) {
		t.Fatal("causally negative producer group entered the retained queue")
	}
	if got := s.executionShadowDropInvalid.Load(); got != 2 {
		t.Fatalf("invalid group accounting=%d want 2", got)
	}
	if occupancy := s.executionShadowOccupancy.Load(); occupancy != 0 {
		t.Fatalf("invalid group left occupancy=%d want 0", occupancy)
	}

	goodAttempt := badAttempt
	goodAttempt.AttemptID = "exec-r169-valid-after-negative"
	goodAttempt.SignalDecisionID = "decision-r169-valid-after-negative"
	goodAttempt.Ticker = "KXR169-VALID"
	goodEvent := storage.ExecutionShadowEvent{
		EventID: "exec-r169-valid-event", AttemptID: goodAttempt.AttemptID,
		At: trigger.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
		Stage: "detector", Outcome: "seen",
	}
	if !s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{
		{attempt: &goodAttempt}, {event: &goodEvent},
	}, true) {
		t.Fatal("valid group behind malformed group was refused")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ExecutionShadowAttemptsByIDs(ctx,
		[]string{badAttempt.AttemptID, goodAttempt.AttemptID})
	if err != nil {
		t.Fatal(err)
	}
	if _, found := rows[badAttempt.AttemptID]; found {
		t.Fatal("malformed group was partly persisted")
	}
	if row, found := rows[goodAttempt.AttemptID]; !found || len(row.Events) != 1 {
		t.Fatalf("valid group behind malformed group missing: found=%t row=%+v", found, row)
	}
}

func TestR169ExecutionShadowWriterDropsDerivedNegativeWithoutBlockingBatch(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	trigger := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	parent := storage.ExecutionShadowAttempt{
		AttemptID:        "exec-r169-durable-negative-parent",
		SignalDecisionID: "decision-r169-durable-negative-parent",
		ObservedAt:       trigger, TriggerUnixMS: trigger.UnixMilli(), Venue: "kalshi",
		Ticker: "KXR169-DURABLE-NEGATIVE", Side: "YES", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalSource: "r169-test",
		QualificationBasis: "durable-derived-negative-test",
	}
	if inserted, err := st.InsertExecutionShadowAttempt(ctx, parent); err != nil || !inserted {
		t.Fatalf("insert parent inserted=%v err=%v", inserted, err)
	}
	badEvent := storage.ExecutionShadowEvent{
		EventID: "exec-r169-durable-negative-event", AttemptID: parent.AttemptID,
		At: trigger.Add(-time.Second), ElapsedFromTriggerMS: executionShadowElapsedUnknownMS,
		Stage: "recovery", Outcome: "observed",
	}
	if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &badEvent}) {
		t.Fatal("event-only unknown sentinel was rejected on the producer path")
	}
	goodEvent := storage.ExecutionShadowEvent{
		EventID: "exec-r169-valid-behind-derived-negative", AttemptID: parent.AttemptID,
		At: trigger.Add(time.Second), ElapsedFromTriggerMS: 1000,
		Stage: "recovery", Outcome: "valid",
	}
	if !s.enqueueExecutionShadowWrite(executionShadowWrite{event: &goodEvent}) {
		t.Fatal("valid event behind derived-negative fixture was refused")
	}
	drainCtx, cancelDrain := context.WithTimeout(ctx, 5*time.Second)
	defer cancelDrain()
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		t.Fatal(err)
	}
	if got := s.executionShadowDropInvalid.Load(); got != 1 {
		t.Fatalf("derived-negative invalid drops=%d want 1", got)
	}
	if occupancy := s.executionShadowOccupancy.Load(); occupancy != 0 {
		t.Fatalf("derived-negative batch left occupancy=%d", occupancy)
	}
	row, err := st.ExecutionShadowAttemptsByIDs(ctx, []string{parent.AttemptID})
	if err != nil {
		t.Fatal(err)
	}
	events := row[parent.AttemptID].Events
	if len(events) != 1 || events[0].EventID != goodEvent.EventID {
		t.Fatalf("derived-negative row blocked or displaced valid row: %+v", events)
	}
}

func TestR163ExecutionShadowPopBatchReslicesAndPreservesOrder(t *testing.T) {
	s := &Server{}
	total := executionShadowBatchMax + 3
	s.executionShadowPending = make([]executionShadowWrite, 0, total)
	for i := 0; i < total; i++ {
		event := &storage.ExecutionShadowEvent{EventID: fmt.Sprintf("event-%03d", i)}
		s.executionShadowPending = append(s.executionShadowPending,
			executionShadowWrite{event: event})
	}
	s.executionShadowOccupancy.Store(int64(total))
	firstRemainingSlot := &s.executionShadowPending[executionShadowBatchMax]
	batch := s.executionShadowPopBatch()
	if len(batch) != executionShadowBatchMax ||
		batch[0].event.EventID != "event-000" ||
		batch[len(batch)-1].event.EventID !=
			fmt.Sprintf("event-%03d", executionShadowBatchMax-1) {
		t.Fatalf("popped batch lost order: first=%q last=%q len=%d",
			batch[0].event.EventID, batch[len(batch)-1].event.EventID, len(batch))
	}
	if len(s.executionShadowPending) != 3 ||
		s.executionShadowPending[0].event.EventID !=
			fmt.Sprintf("event-%03d", executionShadowBatchMax) ||
		&s.executionShadowPending[0] != firstRemainingSlot {
		t.Fatalf("remaining queue was shifted or reordered: %+v", s.executionShadowPending)
	}
	s.executionShadowCompleteBatch(len(batch))
	if s.executionShadowInFlight != 0 {
		t.Fatalf("completed resliced batch left in-flight=%d", s.executionShadowInFlight)
	}
}

func TestR159ExecutionShadowFailureAccountingRequeuesAtomically(t *testing.T) {
	s, _ := newExecutionShadowTestServer(t)
	s.executionShadowPendingCap = 3
	frontEvent := storage.ExecutionShadowEvent{EventID: "front"}
	pendingEvent := storage.ExecutionShadowEvent{EventID: "pending"}
	backEvent := storage.ExecutionShadowEvent{EventID: "back"}
	front := executionShadowWrite{event: &frontEvent}
	pending := executionShadowWrite{event: &pendingEvent}
	back := executionShadowWrite{event: &backEvent}
	s.executionShadowMu.Lock()
	s.executionShadowInFlight = 2
	s.executionShadowPending = []executionShadowWrite{pending}
	s.executionShadowOccupancy.Store(3)
	s.executionShadowInFlightCount.Store(2)
	s.executionShadowMu.Unlock()

	s.executionShadowCompleteAndRequeue(2,
		[]executionShadowWrite{front}, []executionShadowWrite{back})
	s.executionShadowMu.Lock()
	defer s.executionShadowMu.Unlock()
	if s.executionShadowInFlight != 0 || len(s.executionShadowPending) != 3 {
		t.Fatalf("failure transition exposed lost accounting: in_flight=%d pending=%d",
			s.executionShadowInFlight, len(s.executionShadowPending))
	}
	got := []string{
		s.executionShadowPending[0].event.EventID,
		s.executionShadowPending[1].event.EventID,
		s.executionShadowPending[2].event.EventID,
	}
	if strings.Join(got, ",") != "front,pending,back" ||
		s.executionShadowDropped.Load() != 0 {
		t.Fatalf("atomic retry order/drop mismatch: got=%v dropped=%d",
			got, s.executionShadowDropped.Load())
	}
}

func TestR163ExecutionShadowStopClosesAdmissionBeforeDrain(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseDB := holdExecutionShadowDBWrite(t, st)
	first := storage.Signal{Platform: "kalshi", Ticker: "KXR163-STOP-DRAIN",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	if attemptID := s.executionShadowBeginSignal(first, .08, time.Now()); attemptID == "" {
		t.Fatal("could not enqueue the drain fixture")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	stopped := make(chan error, 1)
	go func() { stopped <- s.stopExecutionShadowWriter(stopCtx) }()

	deadline := time.Now().Add(time.Second)
	for {
		s.executionShadowMu.Lock()
		stopping := s.executionShadowStopping
		occupancy := len(s.executionShadowPending) + s.executionShadowInFlight
		s.executionShadowMu.Unlock()
		if stopping {
			if occupancy == 0 {
				t.Fatal("stop marked admission closed only after the held drain became empty")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stop did not close admission before waiting for the held drain")
		}
		time.Sleep(time.Millisecond)
	}

	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR163-LATE-DRAIN",
		Side: "YES", Action: "BUY", Family: "spotlag", Price: .40, At: time.Now()}
	_, lateAttempt, ok := s.executionShadowBuildCandidate(candidate, "late-during-stop", "")
	if !ok {
		t.Fatal("could not build late-admission fixture")
	}
	stoppingDrops := s.executionShadowDropStopping.Load()
	accepted := false
	for time.Now().Before(deadline) &&
		s.executionShadowDropStopping.Load() == stoppingDrops {
		if s.enqueueExecutionShadowWriteGroup(
			[]executionShadowWrite{{attempt: &lateAttempt}}, true) {
			accepted = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if accepted || s.executionShadowDropStopping.Load() != stoppingDrops+1 {
		t.Fatalf("late drain admission accepted=%v stopping_drops=%d want=%d",
			accepted, s.executionShadowDropStopping.Load(), stoppingDrops+1)
	}

	releaseDB()
	s.wakeExecutionShadowWriter()
	if err := <-stopped; err != nil {
		t.Fatalf("normal drain stop failed: %v", err)
	}
	rows, err := st.ListExecutionShadowAttempts(context.Background(), 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.Ticker != first.Ticker {
		t.Fatalf("stop persisted a refused late admission: rows=%+v err=%v", rows, err)
	}
}

func TestR163ExecutionShadowStopTimeoutRejectsNewAdmissionAndRetryDrainsAcceptedTail(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseDB := holdExecutionShadowDBWrite(t, st)
	first := storage.Signal{Platform: "kalshi", Ticker: "KXR163-STOP-TIMEOUT",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	if attemptID := s.executionShadowBeginSignal(first, .08, time.Now()); attemptID == "" {
		t.Fatal("could not enqueue the timeout fixture")
	}

	stopCtx, cancelStop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	err := s.stopExecutionShadowWriter(stopCtx)
	cancelStop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("held writer stop error=%v want deadline exceeded", err)
	}
	// modernc SQLite may remain inside its busy handler after the Go context is cancelled.
	// Remove the deliberate database obstruction before requiring the cancelled goroutine to
	// publish its done receipt; admission is already permanently closed above.
	releaseDB()
	s.executionShadowMu.Lock()
	done := s.executionShadowDone
	s.executionShadowMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled execution-shadow writer did not exit")
	}

	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR163-AFTER-TIMEOUT",
		Side: "YES", Action: "BUY", Family: "spotlag", Price: .40, At: time.Now()}
	_, lateAttempt, ok := s.executionShadowBuildCandidate(candidate, "late-after-timeout", "")
	if !ok {
		t.Fatal("could not build post-timeout fixture")
	}
	before := s.executionShadowDropStopping.Load()
	if s.enqueueExecutionShadowWriteGroup(
		[]executionShadowWrite{{attempt: &lateAttempt}}, true) {
		t.Fatal("post-timeout write entered the dead writer queue")
	}
	if got := s.executionShadowDropStopping.Load(); got != before+1 {
		t.Fatalf("post-timeout drop reason=%d want=%d", got, before+1)
	}

	retryCtx, cancelRetry := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRetry()
	if err := s.FenceExecutionShadowForBackup(retryCtx); err != nil {
		t.Fatalf("second fence did not restart the dead worker and drain accepted tail: %v", err)
	}
	rows, err := st.ListExecutionShadowAttempts(context.Background(), 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.Ticker != first.Ticker {
		t.Fatalf("retry lost or duplicated the accepted pre-timeout row: rows=%+v err=%v", rows, err)
	}
	if occupancy := s.executionShadowOccupancy.Load(); occupancy != 0 {
		t.Fatalf("retry left execution-shadow occupancy=%d want=0", occupancy)
	}
	if status := st.ExecutionShadowStatus(context.Background()); !status.LatestClean {
		t.Fatalf("retry did not close the drained writer session cleanly: %+v", status)
	}
}

func TestR159ExecutionShadowOrphanRotatesUntilParentPersistsDuringStop(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	at := time.Now().UTC().Truncate(time.Millisecond)
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR159-ORPHAN",
		Title: "event before parent", Side: "YES", Action: "BUY",
		Family: "spotlag", Source: "auto-cons-spotlag", Price: .40, At: at}
	candidate, attempt, ok := s.executionShadowBuildCandidate(candidate,
		"orphan-order-fixture", "")
	if !ok {
		t.Fatal("could not build orphan-order fixture")
	}
	event := s.executionShadowEvent(candidate.ShadowAttemptID,
		"live-terminal", "filled", "event deliberately queued first", at.Add(time.Millisecond))
	event.LiveFilledQty = floatPtrShadow(1)
	event.LiveFillPrice = floatPtrShadow(.40)
	event.LiveFee = floatPtrShadow(.01)
	event.LiveFeeSource = "test-exact-fee"
	if !s.enqueueExecutionShadowWriteGroup([]executionShadowWrite{
		{event: &event}, {attempt: &attempt},
	}, true) {
		t.Fatal("event-before-parent fixture was not accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatalf("shutdown lost or blocked an orphan retry: %v", err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 || len(rows[0].Events) != 1 ||
		rows[0].Events[0].EventID != event.EventID {
		t.Fatalf("event-before-parent was not retained and rejoined: rows=%+v err=%v",
			rows, err)
	}
}

func TestR159PolicyMirrorRejectionUsesCarriedAttemptNotLatestSignalMap(t *testing.T) {
	s := testServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	s.liveDispatchShadowQuietDelay = -1
	cfg := *s.cfg()
	cfg.Risk.LiveProspectiveAllocation = true
	cfg.Risk.LiveSystemKalshi = true
	cfg.Risk.LiveSystemAllowlist = "kalshi|spotlag|YES|taker"
	s.cfgP.Store(&cfg)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR159-MIRROR-ID",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	at := time.Now().UTC()
	first := s.executionShadowBeginSignal(sig, .08, at)
	second := s.executionShadowBeginSignal(sig, .09, at.Add(time.Nanosecond))
	if first == "" || second == "" || first == second {
		t.Fatalf("bad attempt fixtures first=%q second=%q", first, second)
	}
	s.recordLivePolicyMirrorRejection(context.Background(), sig,
		"exact prepared rejection", at, first)
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := s.store.ListExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	rejectedByAttempt := map[string]int{}
	for _, row := range rows {
		for _, event := range row.Events {
			if event.Stage == "counterfactual-terminal" &&
				event.ShadowState == storage.LivePolicyMirrorRejected {
				rejectedByAttempt[row.Attempt.AttemptID]++
			}
		}
	}
	if rejectedByAttempt[first] != 1 || rejectedByAttempt[second] != 0 {
		t.Fatalf("prepared rejection re-resolved mutable signal map: %+v", rejectedByAttempt)
	}
}

func TestR159PaperShutdownWritesExactRetryableSchedulerReceipt(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	sig := storage.Signal{Platform: "kalshi", Ticker: "KXR159-PAPER-SHUTDOWN",
		Side: "YES", SignalType: "spotlag", EntryPrice: .40}
	attemptID := s.executionShadowBeginSignal(sig, .08, time.Now())
	workerCtx, cancelWorker := context.WithCancel(context.Background())
	cancelWorker()
	in := make(chan genfollowPaperIntent, 1)
	done := make(chan struct{})
	in <- genfollowPaperIntent{Signals: []genfollowPaperSignal{{
		Signal: sig, ShadowAttemptID: attemptID,
	}}, EnqueuedAt: time.Now().UTC()}
	close(in)
	s.runGenfollowPaperWorkers(workerCtx, in, done)
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	found := false
	for _, event := range rows[0].Events {
		found = found || event.Stage == "funded-paper-scheduler" &&
			event.PaperState == "PAPER-DEFERRED" && event.Reason == "paper-worker-shutdown"
	}
	if !found {
		t.Fatalf("Paper shutdown lost exact retryable receipt: %+v", rows[0].Events)
	}
}

func TestR159QueueClearWritesTerminalForEveryRemovedBranch(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	at := time.Now().UTC()
	makeCandidate := func(ticker string) liveMirrorCandidate {
		c := liveMirrorCandidate{Platform: "kalshi", Ticker: ticker, Side: "YES",
			Action: "BUY", Family: "spotlag", Source: "auto-cons-spotlag",
			Price: .40, At: at}
		return s.executionShadowEnsureCandidate(c, "queue-clear-test")
	}
	single, combo, raw := makeCandidate("KXR159-CLEAR-SINGLE"),
		makeCandidate("KXR159-CLEAR-COMBO"), makeCandidate("KXR159-CLEAR-RAW")
	s.liveMirrorMu.Lock()
	s.liveMirrorQ = []liveMirrorCandidate{single}
	s.liveMirrorComboQ = []liveMirrorCandidate{combo}
	s.liveMirrorMu.Unlock()
	s.liveSignalIntentChannel() <- liveSignalIntent{Signal: storage.Signal{
		Platform: "kalshi", Ticker: raw.Ticker, Side: "YES",
		SignalType: "spotlag", EntryPrice: .40,
	}, Point: .08, At: at, ShadowAttemptID: raw.ShadowAttemptID}
	s.clearLiveMirrorCandidates()
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 3 {
		t.Fatalf("queue clear rows=%d err=%v", len(rows), err)
	}
	branches := map[string]bool{}
	for _, row := range rows {
		for _, event := range row.Events {
			if event.Stage == "queue-clear" {
				branch, _ := event.Evidence["queue_branch"].(string)
				branches[branch] = true
			}
		}
	}
	for _, branch := range []string{
		"live-mirror-single", "live-mirror-combo", "live-signal-intent",
	} {
		if !branches[branch] {
			t.Fatalf("queue clear omitted %s terminal: %+v", branch, branches)
		}
	}
}

func TestR159ProperMomentumSellHasTerminalAndActionAwareNet(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR159-SELL",
		Side: "YES", Action: "SELL", Family: "proper-score-momentum",
		Source: "proper-score:test", Price: .60, At: time.Now().UTC()}
	candidate = s.executionShadowEnsureCandidate(candidate, "proper-score-sell-test")
	dispatched, why := s.dispatchProperMomentumLiveSell(context.Background(), candidate,
		storage.ProperMomentumSellIntent{IntentID: "proper-r159",
			Candidate: storage.ProperMomentumSellCandidate{
				Ticker: candidate.Ticker, OwnedSide: "YES", RequiredPrior: 1,
			}})
	if dispatched || why != properMomentumLiveDisabledReason {
		t.Fatalf("unexpected hermetic dispatch=%v why=%q", dispatched, why)
	}
	active, err := st.ActiveLivePendingRisk(context.Background())
	if err != nil || len(active) != 0 {
		t.Fatalf("disabled SELL created LIVE risk: active=%d err=%v", len(active), err)
	}
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.Action != "SELL" {
		t.Fatalf("SELL attempt missing rows=%+v err=%v", rows, err)
	}
	found := false
	for _, event := range rows[0].Events {
		found = found || event.Stage == "live-terminal" &&
			event.LiveState == "rejected-before-venue" && !event.VenueAttempted
	}
	if !found {
		t.Fatalf("proper-score early return bypassed terminal: %+v", rows[0].Events)
	}
	buy := executionShadowSettlementNet("BUY", "YES", 1, 1, .60, .01)
	sell := executionShadowSettlementNet("SELL", "YES", 1, 1, .60, .01)
	if math.Abs(buy-.39) > 1e-9 || math.Abs(sell-(-.41)) > 1e-9 {
		t.Fatalf("action-aware settlement wrong BUY=%v SELL=%v", buy, sell)
	}
}

func TestR167ProperMomentumPaperSellKeepsCanonicalSELLAndHonestShadow(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	now := time.Now().UTC()
	in := storage.ProperMomentumSellIntent{IntentID: strings.Repeat("b", 64),
		SourceID: "r139ms:" + strings.Repeat("b", 64), Created: now.Add(100 * time.Millisecond),
		Candidate: storage.ProperMomentumSellCandidate{ObservationID: 167, Observed: now,
			Cohort: "proper-score-v2|transform=brier|fixture=true", Ticker: "KXR167-SELL",
			Title: "canonical proper SELL", SyntheticSide: "NO", OwnedSide: "YES",
			SourceClockID: "fixture-research-clock", RequiredPrior: 1,
			ObservedSellPrice: .61, ObservedSellFee: .01}}
	candidate := s.ensureProperMomentumCanonicalAttempt(in)
	s.recordProperMomentumFundedPaperBranch(candidate, in.Created)
	executionReceipt := &storage.ProperMomentumSellBookReceipt{ObservedAt: now.Add(time.Second),
		SourceClockAt: now.Add(900 * time.Millisecond), BookSource: "kalshi-ws-owned-bid",
		SourceClockID: "execution-clock", FeeSource: "kalshi-exact-sell-fee",
		VisibleDepth: 7, TickSize: .01, Price: .62, Fee: .01, RequestedQty: 1,
		State: "filled", Reason: "fixture fill"}
	s.recordProperMomentumPaperTerminal(candidate, storage.ProperMomentumSellPaperState{
		State: "filled", SellPrice: .62, Fee: .01}, executionReceipt,
		"preprice-clock", .61, 750, "newer complete bid book filled", now.Add(time.Second))
	s.recordProperMomentumShadowUnavailable(candidate, now.Add(time.Second))
	s.recordProperMomentumLiveNoSend(candidate, properMomentumLiveDisabledReason, now.Add(time.Second))
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("canonical SELL rows=%+v err=%v", rows, err)
	}
	row := rows[0]
	if row.Attempt.Action != "SELL" || row.Attempt.Route != "taker" ||
		row.Attempt.InputTopology != "K-PROPER-BRIER" || !row.Attempt.ObservedAt.Equal(now) {
		t.Fatalf("canonical SELL identity drifted: %+v", row.Attempt)
	}
	registered, ok := r147ProperMomentumSignalContract("YES", "K-PROPER-BRIER")
	if !ok || row.Attempt.SignalContract != r147SignalContractIdentity(registered) {
		t.Fatalf("canonical SELL used an unregistered contract: %+v", row.Attempt)
	}
	stages := map[string]storage.ExecutionShadowEvent{}
	for _, event := range row.Events {
		stages[event.Stage] = event
	}
	for _, stage := range []string{"detector-branch", "funded-paper-branch",
		"funded-paper-terminal", "counterfactual-execution-terminal", "live-selection"} {
		if _, ok := stages[stage]; !ok {
			t.Fatalf("canonical SELL missing %s: %+v", stage, row.Events)
		}
	}
	if stages["funded-paper-terminal"].PaperState != "PAPER-FILLED" ||
		stages["counterfactual-execution-terminal"].ShadowState != "not_observed" ||
		stages["live-selection"].LiveState != "not-sent" ||
		stages["detector-branch"].SideBid == nil || stages["detector-branch"].SideAsk != nil {
		t.Fatalf("canonical SELL branches invented BUY semantics: %+v", stages)
	}
	paperTerminal := stages["funded-paper-terminal"]
	if paperTerminal.BookSource != executionReceipt.BookSource || paperTerminal.VisibleDepth == nil ||
		*paperTerminal.VisibleDepth != executionReceipt.VisibleDepth ||
		paperTerminal.PaperFeeSource != executionReceipt.FeeSource ||
		paperTerminal.FeeQuoteSource != executionReceipt.FeeSource {
		t.Fatalf("Paper terminal lost actual execution receipt: %+v", paperTerminal)
	}
}

func TestR167ProperMomentumCompletedIntentReplayIsIdempotent(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	now := time.Now().UTC()
	in := storage.ProperMomentumSellIntent{IntentID: strings.Repeat("c", 64),
		SourceID: "r139ms:" + strings.Repeat("c", 64), Created: now,
		Preprice: storage.ProperMomentumSellBookReceipt{SourceClockID: "preprice-clock",
			SourceClockAt: now.Add(-time.Second)},
		Candidate: storage.ProperMomentumSellCandidate{Observed: now.Add(-2 * time.Second),
			Cohort: "proper-score-v2|transform=log|fixture=true", Ticker: "KXR167-REPLAY",
			OwnedSide: "NO", ObservedSellPrice: .58, ObservedSellFee: .01}}
	execution := &storage.ProperMomentumSellBookReceipt{ObservedAt: now,
		SourceClockAt: now.Add(-500 * time.Millisecond), BookSource: "kalshi-ws-replay-bid",
		SourceClockID: "execution-clock", FeeSource: "kalshi-exact-replay-fee",
		VisibleDepth: 3, TickSize: .01, Price: .59, Fee: .01, RequestedQty: 1,
		State: "filled", Reason: "durable Paper fill"}
	state := storage.ProperMomentumSellEventState{PaperEvent: "paper_accepted",
		PaperReason: "durable Paper fill", PaperPrice: .59, PaperFee: .01,
		PaperAt: now, LiveEvent: "live_rejected", LiveReason: properMomentumLiveDisabledReason,
		LiveAt: now}
	s.replayProperMomentumCanonicalIntent(in, state, execution, now)
	s.replayProperMomentumCanonicalIntent(in, state, execution, now.Add(time.Second))
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 || rows[0].Attempt.AttemptID != properMomentumCanonicalAttemptID(in) {
		t.Fatalf("completed-intent replay rows=%+v err=%v", rows, err)
	}
	stages := map[string]int{}
	for _, event := range rows[0].Events {
		stages[event.Stage]++
	}
	for _, stage := range []string{"detector-branch", "funded-paper-branch",
		"funded-paper-terminal", "counterfactual-execution-terminal", "live-selection"} {
		if stages[stage] != 1 {
			t.Fatalf("stable replay stage %s count=%d events=%+v", stage, stages[stage], rows[0].Events)
		}
	}
}

func TestR159ExecutionShadowRedactsNewMLCapabilitySource(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	const secretSource = "mlv2-live:super-secret-capability"
	candidate := liveMirrorCandidate{Platform: "kalshi", Ticker: "KXR159-REDACT",
		Side: "YES", Action: "BUY", Family: "ml", Source: secretSource,
		Price: .40, At: time.Now().UTC()}
	candidate = s.executionShadowEnsureCandidate(candidate, "redaction-test")
	s.executionShadowRecordDrop(candidate, "test-terminal", "test", time.Now(),
		map[string]any{"source": secretSource})
	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	if rows[0].Attempt.SignalSource != "mlv2-live:[redacted]" {
		t.Fatalf("stored capability source was not redacted: %q",
			rows[0].Attempt.SignalSource)
	}
	for _, event := range rows[0].Events {
		if event.Evidence["source"] == secretSource {
			t.Fatal("event evidence persisted raw New-ML capability")
		}
	}
	response := httptest.NewRecorder()
	s.handleExecutionShadow(response,
		httptest.NewRequest(http.MethodGet, "/api/execution-shadow?limit=10", nil))
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), secretSource) {
		t.Fatalf("API exposed capability: status=%d body=%s",
			response.Code, response.Body.String())
	}
}

func TestR172QuoteEvidencePersistsTypedResidentBookIdentity(t *testing.T) {
	observed := time.Date(2026, 7, 25, 6, 0, 0, 123, time.UTC)
	checked := observed.Add(17 * time.Millisecond)
	got := liveMirrorQuoteEvidence(liveMirrorQuote{
		Price: .41, Depth: 7, Tick: .01, SpreadCents: 2,
		BookSource: "kalshi_ws_full_orderbook:g17:s23:q42",
		SourceAt:   observed.Add(-time.Millisecond), ObservedAt: observed, CheckedAt: checked,
	}, checked)
	if got["book_generation"] != int64(17) ||
		got["book_subscription_id"] != int64(23) ||
		got["book_sequence"] != int64(42) {
		t.Fatalf("typed pre-POST book identity missing: %+v", got)
	}
	if got["depth"] != float64(7) || got["tick"] != .01 ||
		got["spread_cents"] != float64(2) ||
		got["observed_at"] != observed.Format(time.RFC3339Nano) ||
		got["checked_at"] != checked.Format(time.RFC3339Nano) {
		t.Fatalf("pre-POST book economics/clocks changed: %+v", got)
	}
}
