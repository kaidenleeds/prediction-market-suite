package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r164InstallCancelablePreflightFixture(t *testing.T, s *Server, prefix string) (
	*liveSignalPreflightScheduler, <-chan struct{}, []string) {
	t.Helper()
	now := time.Now().UTC()
	started := make(chan struct{})
	signals := []storage.Signal{
		{Platform: "kalshi", Ticker: prefix + "-ACTIVE", Title: "active admitted preflight",
			Side: "YES", SignalType: "spotlag", EntryPrice: .40},
		{Platform: "kalshi", Ticker: prefix + "-QUEUED", Title: "queued admitted preflight",
			Side: "NO", SignalType: "spotlag", EntryPrice: .41},
	}
	intents := make([]liveSignalIntent, 0, len(signals))
	attemptIDs := make([]string, 0, len(signals))
	for i, sig := range signals {
		signalAt := now.Add(-time.Duration(i) * time.Millisecond)
		attemptID := s.executionShadowBeginSignal(sig, .09-float64(i)*.01, signalAt)
		if attemptID == "" {
			t.Fatalf("could not start execution-shadow attempt for %s", sig.Ticker)
		}
		attemptIDs = append(attemptIDs, attemptID)
		intents = append(intents, liveSignalIntent{
			Signal: sig, Point: .09 - float64(i)*.01, At: signalAt,
			ShadowAttemptID: attemptID, Generation: s.liveSignalIntentGeneration.Load(),
			GenerationBound: true,
		})
	}
	scheduler := newLiveSignalPreflightScheduler(s, context.Background(), 1, 4,
		func(runCtx context.Context, intent liveSignalIntent) bool {
			close(started)
			<-runCtx.Done()
			return s.rejectLiveSignalIntentPreparedMode(intent.Signal,
				"active-preflight-canceled-after-admission", intent.ShadowAttemptID,
				intent.At, intent.PreflightAdmitted)
		},
		func(intent liveSignalIntent, stage, reason string, extra map[string]any) {
			s.recordLiveCandidateDrop(liveCandidateFromSignalIntent(intent),
				"AUTO-LIVE-SIGNAL-DROP", stage, reason, time.Now(), extra)
		})
	if actual, loaded := liveSignalPreflightSchedulers.LoadOrStore(s, scheduler); loaded {
		scheduler.stop()
		t.Fatalf("unexpected existing preflight scheduler: %T", actual)
	}
	t.Cleanup(func() {
		liveSignalPreflightSchedulers.CompareAndDelete(s, scheduler)
		scheduler.stop()
	})
	scheduler.submit(context.Background(), intents)
	return scheduler, started, attemptIDs
}

func r164AssertOneRecoverableNoSendTerminal(t *testing.T, s *Server,
	st *storage.Store, attemptIDs []string) {
	t.Helper()
	views, err := st.ListExecutionShadowAttempts(context.Background(), 20)
	if err != nil {
		t.Fatal(err)
	}
	byAttempt := make(map[string]storage.ExecutionShadowAttemptView, len(views))
	for _, view := range views {
		byAttempt[view.Attempt.AttemptID] = view
	}
	for _, attemptID := range attemptIDs {
		view, ok := byAttempt[attemptID]
		if !ok {
			t.Fatalf("missing admitted attempt %s", attemptID)
		}
		terminals := 0
		for _, event := range view.Events {
			if strings.TrimSpace(event.LiveState) != "" {
				terminals++
			}
		}
		if terminals != 1 {
			t.Fatalf("attempt %s LIVE terminals=%d want exactly 1: %+v",
				attemptID, terminals, view.Events)
		}
		terminal, found, terminalErr := s.fundedPaperLiveTerminalForAttempt(
			context.Background(), attemptID)
		if terminalErr != nil || !found {
			t.Fatalf("attempt %s Paper terminal unavailable: found=%v err=%v",
				attemptID, found, terminalErr)
		}
		if terminal.Kind != fundedPaperLiveTerminalNoSend || terminal.EventID == "" {
			t.Fatalf("attempt %s Paper recovery terminal=%+v", attemptID, terminal)
		}
	}
}

func TestR164AutoOffKeepsOneTerminalForEveryAdmittedPreflight(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	r153ArmAuto(s)
	scheduler, started, attemptIDs :=
		r164InstallCancelablePreflightFixture(t, s, "KXR164-OFF")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("active preflight did not start")
	}
	if _, why := s.transitionLiveAuto(false); why != "" {
		t.Fatalf("AUTO OFF transition: %s", why)
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	if err := scheduler.wait(waitCtx); err != nil {
		t.Fatalf("join canceled preflight scheduler: %v", err)
	}
	if err := s.stopExecutionShadowWriter(waitCtx); err != nil {
		t.Fatalf("drain execution-shadow terminal receipts: %v", err)
	}
	r164AssertOneRecoverableNoSendTerminal(t, s, st, attemptIDs)
}

func TestR164FinalSnapshotKeepsOneTerminalForEveryAdmittedPreflight(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	r153ArmAuto(s)
	_, started, attemptIDs :=
		r164InstallCancelablePreflightFixture(t, s, "KXR164-SHUTDOWN")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("active preflight did not start")
	}
	prepareCtx, prepareCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer prepareCancel()
	if err := s.PrepareFinalSnapshot(prepareCtx); err != nil {
		t.Fatalf("final snapshot: %v", err)
	}
	r164AssertOneRecoverableNoSendTerminal(t, s, st, attemptIDs)
}

func TestR164ComboCleanupCannotRewriteHandlerFillTerminal(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	releaseWriter := holdExecutionShadowWriterStart(t, s)
	triggerAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	sig := storage.Signal{
		Platform: "kalshi", Ticker: "KXR164-FILL-CLEANUP",
		Title: "handler fill followed by combo cleanup", Side: "YES",
		SignalType: "spotlag", EntryPrice: .40,
	}
	attemptID := s.executionShadowBeginSignal(sig, .08, triggerAt)
	if attemptID == "" {
		t.Fatal("could not begin handler-fill attempt")
	}
	candidate := liveMirrorCandidate{
		Platform: "kalshi", Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
		Action: "BUY", Family: sig.SignalType, Source: "auto-cons-" + sig.SignalType,
		Price: sig.EntryPrice, At: triggerAt, ShadowAttemptID: attemptID,
		LivePreflightAdmitted: true,
	}
	quote := liveMirrorQuote{
		Price: .40, Depth: 4, Tick: .01, SpreadCents: 1,
		BookSource: "kalshi_ws_full_orderbook:g1:s2:q3",
		SourceAt:   triggerAt, ObservedAt: triggerAt.Add(2 * time.Millisecond),
	}
	s.executionShadowRecordLiveResult(candidate, quote, "risk-r164-fill",
		"order-r164-fill", "filled", "direct-order-receipt",
		1, 1, .40, .01, true, true, true, "", nil)
	s.logLiveMirrorArbitrationDrop(candidate, "final-dispatch",
		"duplicate-or-unpersisted-claim")
	s.recordAdmittedLiveCandidateHousekeeping(candidate,
		"AUTO-LIVE-COMBO-CANDIDATE-DROP", "combo-candidate-queue",
		"combo-candidate-replaced-by-fresher-same-route", triggerAt.Add(3*time.Millisecond), nil)
	s.executionShadowRecordQueueCleanup(candidate, "live-mirror-combo",
		"operator-auto-boundary-cleared-before-dispatch")

	releaseWriter()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.stopLiveCandidateAudit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.stopExecutionShadowWriter(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("attempt rows=%d err=%v", len(rows), err)
	}
	var handlerEventID string
	liveTerminals, cleanupEvents := 0, 0
	for _, event := range rows[0].Events {
		if strings.TrimSpace(event.LiveState) != "" {
			liveTerminals++
		}
		if event.Stage == "live-terminal" {
			handlerEventID = event.EventID
		}
		if event.Stage == "final-dispatch" || event.Stage == "combo-candidate-queue" ||
			event.Stage == "queue-clear" {
			cleanupEvents++
			if event.LiveState != "" || event.VenueAttempted || event.LiveAuthoritative {
				t.Fatalf("combo cleanup manufactured LIVE terminal fields: %+v", event)
			}
		}
	}
	if liveTerminals != 1 || handlerEventID == "" || cleanupEvents != 3 {
		t.Fatalf("handler/cleanup truth mismatch: terminals=%d handler=%q cleanup=%d events=%+v",
			liveTerminals, handlerEventID, cleanupEvents, rows[0].Events)
	}
	exact, found, err := st.ExecutionShadowLiveTerminal(ctx, attemptID)
	if err != nil || !found || exact.EventID != handlerEventID ||
		exact.LiveState != "filled" {
		t.Fatalf("restart lookup lost exact handler fill: found=%v event=%+v err=%v",
			found, exact, err)
	}
	paperTerminal, found, err := s.fundedPaperLiveTerminalForAttempt(ctx, attemptID)
	if err != nil || !found || paperTerminal.EventID != handlerEventID ||
		paperTerminal.Kind != fundedPaperLiveTerminalFill {
		t.Fatalf("funded Paper read cleanup instead of handler fill: found=%v terminal=%+v err=%v",
			found, paperTerminal, err)
	}
}

func TestR164LiveHandlerFinalDispatchCannotBecomeSecondTerminal(t *testing.T) {
	cases := []struct {
		name, state, orderID, receipt, handlerReason, outerReason string
		filled, fillPrice, fee                                    float64
		feeKnown, authoritative, venueAttempted                   bool
	}{
		{
			name: "no-send", state: "not-sent", receipt: "local-final-guard",
			handlerReason: "payoff identity unavailable",
		},
		{
			name: "IOC-zero", state: "unfilled", orderID: "order-r164-zero",
			receipt: "direct-order-receipt", handlerReason: "IOC completed without a fill",
			outerReason: "authoritative-zero-fill",
			feeKnown:    true, authoritative: true, venueAttempted: true,
		},
		{
			name: "fill", state: "filled", orderID: "order-r164-filled",
			receipt: "direct-order-receipt", handlerReason: "filled",
			filled: 1, fillPrice: .40, fee: .01,
			feeKnown: true, authoritative: true, venueAttempted: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, st := newExecutionShadowTestServer(t)
			releaseWriter := holdExecutionShadowWriterStart(t, s)
			triggerAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
			sig := storage.Signal{
				Platform: "kalshi", Ticker: "KXR164-HANDLER-" + strings.ToUpper(tc.name),
				Title: "handler terminal followed by outer observation", Side: "YES",
				SignalType: "spotlag", EntryPrice: .40,
			}
			attemptID := s.executionShadowBeginSignal(sig, .08, triggerAt)
			if attemptID == "" {
				t.Fatal("could not begin execution-shadow attempt")
			}
			candidate := liveMirrorCandidate{
				Platform: "kalshi", Ticker: sig.Ticker, Title: sig.Title, Side: sig.Side,
				Action: "BUY", Family: sig.SignalType, Source: "auto-cons-" + sig.SignalType,
				Price: sig.EntryPrice, At: triggerAt, ShadowAttemptID: attemptID,
				LivePreflightAdmitted: true, ProspectiveQty: 1,
				ProspectiveTimeInForce: liveProspectiveIOC,
			}
			quote := liveMirrorQuote{
				Price: .40, Depth: 4, Tick: .01, SpreadCents: 1,
				BookSource: "kalshi_ws_full_orderbook:g1:s2:q3",
				SourceAt:   triggerAt, ObservedAt: triggerAt.Add(2 * time.Millisecond),
			}
			s.executionShadowRecordLiveResult(candidate, quote, "risk-r164-handler",
				tc.orderID, tc.state, tc.receipt, 1, tc.filled, tc.fillPrice, tc.fee,
				tc.feeKnown, tc.authoritative, tc.venueAttempted, tc.handlerReason, nil)
			outerReason := tc.outerReason
			if outerReason == "" {
				outerReason = "live-handler:" + tc.handlerReason
			}
			s.logLiveMirrorArbitrationDrop(candidate, "final-dispatch", outerReason)

			releaseWriter()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.stopLiveCandidateAudit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := s.stopExecutionShadowWriter(ctx); err != nil {
				t.Fatal(err)
			}
			rows, err := st.ListExecutionShadowAttempts(ctx, 10)
			if err != nil || len(rows) != 1 {
				t.Fatalf("attempt rows=%d err=%v", len(rows), err)
			}
			liveTerminals, finalHousekeeping := 0, 0
			var handlerEventID string
			for _, event := range rows[0].Events {
				if strings.TrimSpace(event.LiveState) != "" {
					liveTerminals++
				}
				if event.Stage == "live-terminal" {
					handlerEventID = event.EventID
				}
				if event.Stage == "final-dispatch" {
					finalHousekeeping++
					if event.Outcome != "housekeeping" || event.LiveState != "" ||
						event.VenueAttempted || event.LiveAuthoritative {
						t.Fatalf("outer handler observation became economic terminal: %+v", event)
					}
				}
			}
			if liveTerminals != 1 || finalHousekeeping != 1 || handlerEventID == "" {
				t.Fatalf("terminal truth mismatch: terminals=%d housekeeping=%d events=%+v",
					liveTerminals, finalHousekeeping, rows[0].Events)
			}
			exact, found, err := st.ExecutionShadowLiveTerminal(ctx, attemptID)
			if err != nil || !found || exact.EventID != handlerEventID ||
				exact.LiveState != tc.state {
				t.Fatalf("exact handler terminal lost: found=%v event=%+v err=%v",
					found, exact, err)
			}
		})
	}
}
