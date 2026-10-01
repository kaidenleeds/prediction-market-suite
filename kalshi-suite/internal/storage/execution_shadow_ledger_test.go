package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExecutionShadowAttemptsByIDsIsExactAndChunked(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	ids := make([]string, executionShadowExactIDLookupChunk+5)
	for i := range ids {
		ids[i] = fmt.Sprintf("bulk-exact-%03d", i)
		attempt := ExecutionShadowAttempt{
			AttemptID: ids[i], SignalDecisionID: fmt.Sprintf("decision-%03d", i),
			ObservedAt: now, TriggerUnixMS: now.UnixMilli(), Venue: "kalshi",
			Ticker: fmt.Sprintf("KX-BULK-%03d", i), Title: "bulk exact lookup", Side: "YES",
			Action: "BUY", SystemID: "bulk", Route: "taker", SignalSource: "test",
			SignalPrice: .4, QualificationBasis: "reader test",
		}
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("insert %d inserted=%v err=%v", i, inserted, insertErr)
		}
	}
	for _, i := range []int{0, len(ids) - 1} {
		event := ExecutionShadowEvent{EventID: fmt.Sprintf("bulk-event-%03d", i),
			AttemptID: ids[i], At: now, ElapsedFromTriggerMS: 0,
			Stage: "detector-branch", Outcome: "qualified"}
		if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil || !inserted {
			t.Fatalf("append %d inserted=%v err=%v", i, inserted, appendErr)
		}
	}
	requested := append([]string{"", "unknown-attempt", ids[0]}, ids...)
	requested = append(requested, ids[len(ids)-1])
	views, err := st.ExecutionShadowAttemptsByIDs(ctx, requested)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != len(ids) {
		t.Fatalf("exact lookup rows=%d want=%d", len(views), len(ids))
	}
	if _, found := views["unknown-attempt"]; found {
		t.Fatal("unknown attempt appeared in exact lookup")
	}
	for _, i := range []int{0, len(ids) - 1} {
		view, found := views[ids[i]]
		if !found || view.Attempt.AttemptID != ids[i] || len(view.Events) != 1 ||
			view.Events[0].AttemptID != ids[i] {
			t.Fatalf("chunk edge %d incomplete: found=%v view=%+v", i, found, view)
		}
	}
}

func TestExecutionShadowFundedSettlementPendingIDsFiltersBeforeHistoryLoad(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	insertAttempt := func(attemptID string) {
		t.Helper()
		attempt := ExecutionShadowAttempt{
			AttemptID: attemptID, SignalDecisionID: "decision-" + attemptID,
			ObservedAt: now, TriggerUnixMS: now.UnixMilli(), Venue: "kalshi",
			Ticker: "KX-" + strings.ToUpper(attemptID), Side: "YES", Action: "BUY",
			SystemID: "pbridge", Route: "taker", SignalSource: "test",
			SignalPrice: .4, QualificationBasis: "funded settlement filter test",
		}
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("insert %q inserted=%v err=%v", attemptID, inserted, insertErr)
		}
	}
	appendEvent := func(event ExecutionShadowEvent) {
		t.Helper()
		if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil || !inserted {
			t.Fatalf("append %q inserted=%v err=%v", event.EventID, inserted, appendErr)
		}
	}
	paperFill := func(attemptID string, offset time.Duration) {
		t.Helper()
		at := now.Add(offset)
		appendEvent(ExecutionShadowEvent{
			EventID: attemptID + "-paper-fill", AttemptID: attemptID, At: at,
			ElapsedFromTriggerMS: executionShadowElapsed(now, at),
			Stage:                "funded-paper-terminal", Outcome: "paper-filled",
			PaperState: "PAPER-FILLED", PaperFilledQty: executionShadowFloat(1),
			PaperFillPrice: executionShadowFloat(.4), PaperFee: executionShadowFloat(.01),
			PaperFeeSource: "exact-test-fee",
		})
	}
	settlement := func(attemptID string, offset time.Duration, paperNet *float64) {
		t.Helper()
		at := now.Add(offset)
		appendEvent(ExecutionShadowEvent{
			EventID: attemptID + "-settlement", AttemptID: attemptID, At: at,
			ElapsedFromTriggerMS: executionShadowElapsed(now, at),
			Stage:                "settlement", Outcome: "settled", SettlementKnown: true,
			SettlementValue: executionShadowFloat(1), SettledAt: at,
			SettlementSource: "test-final", SettlementHash: "hash-" + attemptID,
			PaperNet: paperNet,
		})
	}

	for _, attemptID := range []string{
		"pending-fill", "pending-zero", "settled-paper", "settled-without-paper-net", "no-paper",
	} {
		insertAttempt(attemptID)
	}
	paperFill("pending-fill", time.Millisecond)
	appendEvent(ExecutionShadowEvent{
		EventID: "pending-zero-paper-zero", AttemptID: "pending-zero",
		At: now.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
		Stage: "funded-paper-terminal", Outcome: "paper-zero-fill",
		PaperState: "PAPER-ZERO-FILL",
	})
	paperFill("settled-paper", time.Millisecond)
	settlement("settled-paper", 2*time.Millisecond, executionShadowFloat(.59))
	paperFill("settled-without-paper-net", time.Millisecond)
	settlement("settled-without-paper-net", 2*time.Millisecond, nil)
	appendEvent(ExecutionShadowEvent{
		EventID: "no-paper-shadow-fill", AttemptID: "no-paper",
		At: now.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
		Stage: "counterfactual-execution-terminal", Outcome: "modeled_fill",
		ShadowState: "modeled_fill", ShadowFilledQty: executionShadowFloat(1),
		ShadowFillPrice: executionShadowFloat(.4), ShadowFee: executionShadowFloat(.01),
		ShadowFeeSource: "exact-test-fee",
	})

	pending, err := st.ExecutionShadowFundedSettlementPendingIDs(ctx, []string{
		"", "settled-paper", "pending-zero", "unknown", "pending-fill", "pending-zero",
		"no-paper", "settled-without-paper-net",
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"pending-zero", "pending-fill", "settled-without-paper-net"}
	if fmt.Sprint(pending) != fmt.Sprint(want) {
		t.Fatalf("pending funded settlements=%v want=%v", pending, want)
	}
}

func executionShadowFloat(v float64) *float64 { return &v }
func executionShadowElapsed(trigger, at time.Time) int64 {
	return at.UnixMilli() - trigger.UnixMilli()
}

func TestExecutionShadowJoinsLivePaperCounterfactualAndSettlementAppendOnly(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	attempt := ExecutionShadowAttempt{
		AttemptID: "exec-test-1", SignalDecisionID: "decision-test-1", ObservedAt: now,
		TriggerUnixMS: now.UnixMilli(),
		Venue:         "kalshi", Ticker: "KX-EXEC-1", Title: "Joined decision", Side: "YES",
		Action: "BUY", SystemID: "spotlag", Route: "taker", SignalSource: "auto-cons-spotlag",
		InputTopology: "coinbase->kalshi", SignalContract: "spotlag-v1",
		InputObservedAt: now.Add(-20 * time.Millisecond), SignalPrice: .39,
		QualificationBasis: "shared detector positive route",
	}
	if inserted, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil || !inserted {
		t.Fatalf("insert attempt inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil || inserted {
		t.Fatalf("exact attempt retry inserted=%v err=%v", inserted, err)
	}
	changed := attempt
	changed.SignalPrice = .40
	if _, err := st.InsertExecutionShadowAttempt(ctx, changed); err == nil {
		t.Fatal("changed signal economics reused immutable attempt id")
	}

	branch := ExecutionShadowEvent{
		EventID: "event-branch", AttemptID: attempt.AttemptID, At: now,
		ElapsedFromTriggerMS: executionShadowElapsed(now, now),
		Stage:                "detector-branch", Outcome: "qualified",
		BookSource: "model-book", SideBid: executionShadowFloat(.38),
		SideAsk: executionShadowFloat(.39), SpreadCents: executionShadowFloat(1),
		VisibleDepth: executionShadowFloat(10), OriginalLimit: executionShadowFloat(.39),
		Evidence: map[string]any{"live_published_first": true},
	}
	if inserted, err := st.AppendExecutionShadowEvent(ctx, branch); err != nil || !inserted {
		t.Fatalf("append branch inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.AppendExecutionShadowEvent(ctx, branch); err != nil || inserted {
		t.Fatalf("event retry inserted=%v err=%v", inserted, err)
	}
	changedBranch := branch
	changedBranch.Reason = "same event id with changed economics"
	changedBranch.SideAsk = executionShadowFloat(.40)
	if _, err := st.AppendExecutionShadowEvent(ctx, changedBranch); err == nil {
		t.Fatal("changed event economics reused immutable event id")
	}
	wrongElapsed := branch
	wrongElapsed.EventID = "event-wrong-elapsed"
	wrongElapsed.ElapsedFromTriggerMS++
	if _, err := st.AppendExecutionShadowEvent(ctx, wrongElapsed); err == nil {
		t.Fatal("event silently reset its detector-relative elapsed time")
	}
	paper := ExecutionShadowEvent{
		EventID: "event-paper", AttemptID: attempt.AttemptID, At: now.Add(time.Second),
		ElapsedFromTriggerMS: executionShadowElapsed(now, now.Add(time.Second)),
		Stage:                "funded-paper-terminal", Outcome: "paper-filled",
		PaperAttemptID: "gf-1", PaperState: "PAPER-FILLED",
		PaperFilledQty: executionShadowFloat(2), PaperFillPrice: executionShadowFloat(.39),
		PaperFee: executionShadowFloat(.04), PaperFeeSource: "paper-exact-fee",
		PaperBookSource: "paper-final-book",
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, paper); err != nil {
		t.Fatal(err)
	}
	shadow := ExecutionShadowEvent{
		EventID: "event-shadow", AttemptID: attempt.AttemptID, At: now.Add(1100 * time.Millisecond),
		ElapsedFromTriggerMS: executionShadowElapsed(now, now.Add(1100*time.Millisecond)),
		Stage:                "counterfactual-terminal", Outcome: "filled", ShadowState: "filled",
		ShadowFilledQty: executionShadowFloat(2), ShadowFillPrice: executionShadowFloat(.40),
		ShadowFee: executionShadowFloat(.05), ShadowFeeSource: "shadow-exact-fee",
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, shadow); err != nil {
		t.Fatal(err)
	}
	if terminal, found, err := st.ExecutionShadowLiveTerminal(ctx, attempt.AttemptID); err != nil || found || terminal.EventID != "" {
		t.Fatalf("Paper/counterfactual event masqueraded as LIVE terminal: found=%v event=%q err=%v",
			found, terminal.EventID, err)
	}
	live := ExecutionShadowEvent{
		EventID: "event-live", AttemptID: attempt.AttemptID, At: now.Add(1200 * time.Millisecond),
		ElapsedFromTriggerMS: executionShadowElapsed(now, now.Add(1200*time.Millisecond)),
		Stage:                "live-terminal", Outcome: "full", LiveReservationID: "risk-1",
		VenueAttempted: true, VenueAck: true, VenueOrderID: "order-1",
		LiveState: "filled", LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(2), LiveFillPrice: executionShadowFloat(.405),
		LiveFee: executionShadowFloat(.051), LiveFeeSource: "order-scoped-fills",
		LiveReceiptSource: "live_pending_risk_events",
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, live); err != nil {
		t.Fatal(err)
	}
	if terminal, found, err := st.ExecutionShadowLiveTerminal(ctx, attempt.AttemptID); err != nil || !found || terminal.EventID != live.EventID ||
		terminal.LiveState != live.LiveState {
		t.Fatalf("exact LIVE terminal lookup found=%v terminal=%+v err=%v", found, terminal, err)
	}
	open, err := st.ListOpenExecutionShadowAttempts(ctx, 10)
	if err != nil || len(open) != 1 || len(open[0].Events) != 4 {
		t.Fatalf("open joined history len=%d events=%d err=%v", len(open), len(open[0].Events), err)
	}
	settle := ExecutionShadowEvent{
		EventID: "event-settle", AttemptID: attempt.AttemptID, At: now.Add(time.Hour),
		ElapsedFromTriggerMS: executionShadowElapsed(now, now.Add(time.Hour)),
		Stage:                "settlement", Outcome: "settled", SettlementKnown: true,
		SettlementValue: executionShadowFloat(1), SettledAt: now.Add(time.Hour),
		SettlementSource: "venue-settlement", SettlementHash: "hash-1",
		LiveNet: executionShadowFloat(1.139), PaperNet: executionShadowFloat(1.18),
		ShadowNet: executionShadowFloat(1.15),
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, settle); err != nil {
		t.Fatal(err)
	}
	views, err := st.ListExecutionShadowAttempts(ctx, 10)
	if err != nil || len(views) != 1 || len(views[0].Events) != 5 {
		t.Fatalf("joined view len=%d events=%d err=%v", len(views), len(views[0].Events), err)
	}
	if views[0].Attempt.SignalDecisionID != "decision-test-1" ||
		views[0].Events[1].PaperAttemptID != "gf-1" ||
		views[0].Events[3].VenueOrderID != "order-1" ||
		views[0].Events[4].LiveNet == nil {
		t.Fatalf("cross-lane join lost exact identity/economics: %+v", views[0])
	}
	if open, err = st.ListOpenExecutionShadowAttempts(ctx, 10); err != nil || len(open) != 0 {
		t.Fatalf("settled attempt remained open len=%d err=%v", len(open), err)
	}
	if _, err := st.ExecutionShadowDBForTest().Exec(`UPDATE execution_shadow_attempts SET ticker='changed'
WHERE attempt_id='exec-test-1'`); err == nil {
		t.Fatal("raw SQL rewrote immutable execution-shadow attempt")
	}
	if _, err := st.ExecutionShadowDBForTest().Exec(`DELETE FROM execution_shadow_events
WHERE event_id='event-branch'`); err == nil {
		t.Fatal("raw SQL deleted append-preserved execution-shadow event")
	}
}

func TestExecutionShadowRejectsOrphanAndUnknownFeeSource(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now().UTC()
	event := ExecutionShadowEvent{
		EventID: "orphan", AttemptID: "missing", At: now,
		Stage: "test", Outcome: "rejected",
	}
	if _, err := st.AppendExecutionShadowEvent(context.Background(), event); err == nil {
		t.Fatal("orphan event bypassed immutable attempt foreign key")
	}
	attempt := ExecutionShadowAttempt{
		AttemptID: "exec-fee", SignalDecisionID: "decision-fee", ObservedAt: now,
		TriggerUnixMS: now.UnixMilli(),
		Venue:         "kalshi", Ticker: "KX-FEE", Side: "NO", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalSource: "spotlag",
		QualificationBasis: "test",
	}
	if _, err := st.InsertExecutionShadowAttempt(context.Background(), attempt); err != nil {
		t.Fatal(err)
	}
	event.AttemptID, event.EventID = attempt.AttemptID, "bad-fee"
	event.FeeQuote = executionShadowFloat(.01)
	if _, err := st.AppendExecutionShadowEvent(context.Background(), event); err == nil {
		t.Fatal("fee without exact source entered execution-shadow ledger")
	}
}

func TestExecutionShadowLiveTerminalPrefersCashStagesOverLegacyCleanup(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	attempt := ExecutionShadowAttempt{
		AttemptID: "exec-terminal-order", SignalDecisionID: "decision-terminal-order",
		ObservedAt: now, TriggerUnixMS: now.UnixMilli(),
		Venue: "kalshi", Ticker: "KX-TERMINAL-ORDER", Side: "YES", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalSource: "spotlag",
		QualificationBasis: "test",
	}
	if _, err = st.InsertExecutionShadowAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	appendEvent := func(event ExecutionShadowEvent) {
		t.Helper()
		event.AttemptID = attempt.AttemptID
		event.ElapsedFromTriggerMS = executionShadowElapsed(now, event.At)
		if _, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil {
			t.Fatal(appendErr)
		}
	}
	local := ExecutionShadowEvent{
		EventID: "event-final-dispatch", At: now.Add(time.Millisecond),
		Stage: "final-dispatch", Outcome: "rejected",
		Reason: "duplicate-or-unpersisted-claim", LiveState: "not-sent",
	}
	appendEvent(local)
	terminal, found, err := st.ExecutionShadowLiveTerminal(ctx, attempt.AttemptID)
	if err != nil || !found || terminal.EventID != local.EventID {
		t.Fatalf("local economic no-send was not the fallback terminal: found=%v event=%+v err=%v",
			found, terminal, err)
	}

	handler := ExecutionShadowEvent{
		EventID: "event-handler-fill", At: now.Add(2 * time.Millisecond),
		Stage: "live-terminal", Outcome: "filled",
		LiveReservationID: "risk-terminal-order", VenueAttempted: true, VenueAck: true,
		VenueOrderID: "order-terminal-order", LiveState: "filled", LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(1), LiveFillPrice: executionShadowFloat(.40),
		LiveFee: executionShadowFloat(.01), LiveFeeSource: "order-scoped-fill",
		LiveReceiptSource: "direct-order-receipt",
	}
	appendEvent(handler)
	legacyCleanup := ExecutionShadowEvent{
		EventID: "event-legacy-combo-cleanup", At: now.Add(3 * time.Millisecond),
		Stage: "combo-candidate-queue", Outcome: "rejected",
		Reason:    "combo-candidate-replaced-by-fresher-same-route",
		LiveState: "not-sent",
	}
	appendEvent(legacyCleanup)
	terminal, found, err = st.ExecutionShadowLiveTerminal(ctx, attempt.AttemptID)
	if err != nil || !found || terminal.EventID != handler.EventID ||
		terminal.LiveState != "filled" {
		t.Fatalf("later legacy cleanup superseded handler cash truth: found=%v event=%+v err=%v",
			found, terminal, err)
	}

	reconcile := ExecutionShadowEvent{
		EventID: "event-exact-reconcile", At: now.Add(4 * time.Millisecond),
		Stage: "live-exact-reconcile", Outcome: "filled",
		LiveReservationID: "risk-terminal-order", VenueAttempted: true,
		LiveState: "filled", LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(1), LiveFillPrice: executionShadowFloat(.40),
		LiveFee: executionShadowFloat(.009), LiveFeeSource: "order-scoped-reconcile",
		LiveReceiptSource: "live-pending-risk-events",
	}
	appendEvent(reconcile)
	terminal, found, err = st.ExecutionShadowLiveTerminal(ctx, attempt.AttemptID)
	if err != nil || !found || terminal.EventID != reconcile.EventID {
		t.Fatalf("exact reconciliation did not supersede the handler receipt: found=%v event=%+v err=%v",
			found, terminal, err)
	}
}

func TestExecutionShadowZeroACKRemainsReconcilableAndLateFeeReopensNetOnly(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	attempt := ExecutionShadowAttempt{
		AttemptID: "exec-late-fee", SignalDecisionID: "decision-late-fee",
		ObservedAt: now, TriggerUnixMS: now.UnixMilli(),
		Venue: "kalshi", Ticker: "KX-LATE-FEE", Side: "YES", Action: "BUY",
		SystemID: "spotlag", Route: "taker", SignalSource: "spotlag",
		QualificationBasis: "test",
	}
	if _, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	zeroACK := ExecutionShadowEvent{
		EventID: "event-zero-ack", AttemptID: attempt.AttemptID, At: now,
		ElapsedFromTriggerMS: 0, Stage: "live-terminal", Outcome: "pending",
		LiveReservationID: "risk-late-fee", VenueAttempted: true, VenueAck: true,
		VenueOrderID: "order-late-fee", LiveState: "pending",
		LiveFilledQty: executionShadowFloat(0),
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, zeroACK); err != nil {
		t.Fatal(err)
	}
	pending, err := st.ListExecutionShadowLiveReceiptPending(ctx, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("zero-at-ACK order became unreconcilable: len=%d err=%v", len(pending), err)
	}
	paper := ExecutionShadowEvent{
		EventID: "event-late-paper", AttemptID: attempt.AttemptID, At: now.Add(time.Second),
		ElapsedFromTriggerMS: 1000, Stage: "funded-paper-terminal", Outcome: "paper-filled",
		PaperFilledQty: executionShadowFloat(1), PaperFillPrice: executionShadowFloat(.40),
		PaperFee: executionShadowFloat(.01), PaperFeeSource: "paper-exact-fee",
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, paper); err != nil {
		t.Fatal(err)
	}
	settlement := ExecutionShadowEvent{
		EventID: "event-paper-settlement", AttemptID: attempt.AttemptID,
		At: now.Add(2 * time.Second), ElapsedFromTriggerMS: 2000,
		Stage: "settlement", Outcome: "settled", SettlementKnown: true,
		SettlementValue: executionShadowFloat(1), SettledAt: now.Add(2 * time.Second),
		SettlementSource: "test-settlement", SettlementHash: "hash-late-fee",
		PaperNet: executionShadowFloat(.59),
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, settlement); err != nil {
		t.Fatal(err)
	}
	if open, err := st.ListOpenExecutionShadowAttempts(ctx, 10); err != nil || len(open) != 0 {
		t.Fatalf("incomplete LIVE fee incorrectly reopened settled attempt: len=%d err=%v",
			len(open), err)
	}
	liveFill := ExecutionShadowEvent{
		EventID: "event-late-live-fill", AttemptID: attempt.AttemptID,
		At: now.Add(3 * time.Second), ElapsedFromTriggerMS: 3000,
		Stage: "live-exact-reconcile", Outcome: "filled",
		LiveReservationID: "risk-late-fee", VenueAttempted: true,
		LiveState: "filled", LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(1), LiveFillPrice: executionShadowFloat(.41),
		LiveFee: executionShadowFloat(.02), LiveFeeSource: "order-scoped-fill",
		LiveReceiptSource: "pending-risk-events",
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, liveFill); err != nil {
		t.Fatal(err)
	}
	if pending, err = st.ListExecutionShadowLiveReceiptPending(ctx, 10); err != nil || len(pending) != 0 {
		t.Fatalf("exact late fee did not close reconciliation: len=%d err=%v",
			len(pending), err)
	}
	open, err := st.ListOpenExecutionShadowAttempts(ctx, 10)
	if err != nil || len(open) != 1 {
		t.Fatalf("late exact fee did not reopen missing LIVE net: len=%d err=%v",
			len(open), err)
	}
	backfill := ExecutionShadowEvent{
		EventID: "event-live-net-backfill", AttemptID: attempt.AttemptID,
		At: now.Add(4 * time.Second), ElapsedFromTriggerMS: 4000,
		Stage: "settlement", Outcome: "net-backfill", SettlementKnown: true,
		SettlementValue: executionShadowFloat(1), SettledAt: now.Add(2 * time.Second),
		SettlementSource: "test-settlement", SettlementHash: "hash-late-fee",
		LiveNet: executionShadowFloat(.57),
	}
	if _, err := st.AppendExecutionShadowEvent(ctx, backfill); err != nil {
		t.Fatal(err)
	}
	if open, err = st.ListOpenExecutionShadowAttempts(ctx, 10); err != nil || len(open) != 0 {
		t.Fatalf("net-only backfill remained open: len=%d err=%v", len(open), err)
	}
}

func TestExecutionShadowExecutionTerminalOwnsCanonicalShadowSettlement(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	observed := make(map[string]time.Time)

	insertAttempt := func(id string, offset time.Duration) {
		t.Helper()
		observedAt := now.Add(offset)
		observed[id] = observedAt
		attempt := ExecutionShadowAttempt{
			AttemptID: id, SignalDecisionID: "decision-" + id,
			ObservedAt: observedAt, TriggerUnixMS: observedAt.UnixMilli(),
			Venue: "kalshi", Ticker: "KX-" + id, Side: "YES", Action: "BUY",
			SystemID: "spotlag", Route: "taker", SignalSource: "spotlag",
			QualificationBasis: "canonical execution comparison settlement test",
		}
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("insert %s inserted=%v err=%v", id, inserted, insertErr)
		}
	}
	appendEvent := func(event ExecutionShadowEvent) {
		t.Helper()
		if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil || !inserted {
			t.Fatalf("append %s inserted=%v err=%v", event.EventID, inserted, appendErr)
		}
	}
	portfolioFill := func(id string, at time.Time) {
		t.Helper()
		appendEvent(ExecutionShadowEvent{
			EventID: id + "-portfolio-fill", AttemptID: id, At: at,
			ElapsedFromTriggerMS: 100, Stage: "counterfactual-terminal", Outcome: "filled",
			ShadowState: "filled", ShadowFilledQty: executionShadowFloat(1),
			ShadowFillPrice: executionShadowFloat(.40), ShadowFee: executionShadowFloat(.01),
			ShadowFeeSource: "test-exact-fee",
		})
	}
	settleShadow := func(eventID, id string, at time.Time, net float64) {
		t.Helper()
		appendEvent(ExecutionShadowEvent{
			EventID: eventID, AttemptID: id, At: at,
			ElapsedFromTriggerMS: executionShadowElapsed(observed[id], at),
			Stage:                "settlement", Outcome: "settled",
			SettlementKnown: true, SettlementValue: executionShadowFloat(1),
			SettledAt: at, SettlementSource: "test-settlement",
			SettlementHash: "hash-" + id, ShadowNet: executionShadowFloat(net),
		})
	}

	// Once an execution-only terminal exists, an older optional portfolio fill is not a
	// canonical Shadow fill. A missing observation remains open only until its authoritative
	// market outcome is attached; its economics stay unknown.
	insertAttempt("exec-not-observed", 0)
	portfolioFill("exec-not-observed", now.Add(100*time.Millisecond))
	appendEvent(ExecutionShadowEvent{
		EventID: "exec-not-observed-terminal", AttemptID: "exec-not-observed",
		At: now.Add(300 * time.Millisecond), ElapsedFromTriggerMS: 300,
		Stage: "counterfactual-execution-terminal", Outcome: "not_observed",
		ShadowState: "not_observed", ShadowReason: "book-not-observed",
	})

	// A true modeled zero-fill arriving after an old optional portfolio settlement needs a
	// post-terminal zero-dollar economics receipt.
	insertAttempt("exec-zero", time.Second)
	portfolioFill("exec-zero", now.Add(1100*time.Millisecond))
	settleShadow("exec-zero-pre-settlement", "exec-zero",
		now.Add(1200*time.Millisecond), .59)
	appendEvent(ExecutionShadowEvent{
		EventID: "exec-zero-terminal", AttemptID: "exec-zero",
		At: now.Add(1300 * time.Millisecond), ElapsedFromTriggerMS: 300,
		Stage: "counterfactual-execution-terminal", Outcome: "modeled_zero_fill",
		ShadowState: "modeled_zero_fill", ShadowReason: "no-depth-at-limit",
	})

	// A later execution-only fill supersedes both the optional portfolio fill and its
	// earlier settlement. It remains open until a post-execution settlement row exists.
	insertAttempt("exec-fill", 2*time.Second)
	portfolioFill("exec-fill", now.Add(2100*time.Millisecond))
	settleShadow("exec-fill-pre-settlement", "exec-fill",
		now.Add(2200*time.Millisecond), .59)
	appendEvent(ExecutionShadowEvent{
		EventID: "exec-fill-terminal", AttemptID: "exec-fill",
		At: now.Add(2300 * time.Millisecond), ElapsedFromTriggerMS: 300,
		Stage: "counterfactual-execution-terminal", Outcome: "modeled_fill",
		ShadowState: "modeled_fill", ShadowFilledQty: executionShadowFloat(1),
		ShadowFillPrice: executionShadowFloat(.43), ShadowFee: executionShadowFloat(.02),
		ShadowFeeSource: "test-exact-fee",
	})

	open, err := st.ListOpenExecutionShadowAttempts(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 3 || open[0].Attempt.AttemptID != "exec-not-observed" ||
		open[1].Attempt.AttemptID != "exec-zero" || open[2].Attempt.AttemptID != "exec-fill" {
		t.Fatalf("canonical execution settlement gaps=%v, want outcome-only, zero, then fill", func() []string {
			ids := make([]string, 0, len(open))
			for _, view := range open {
				ids = append(ids, view.Attempt.AttemptID)
			}
			return ids
		}())
	}

	appendEvent(ExecutionShadowEvent{
		EventID: "exec-not-observed-outcome", AttemptID: "exec-not-observed",
		At: now.Add(2350 * time.Millisecond), ElapsedFromTriggerMS: 2350,
		Stage: "settlement", Outcome: "settled", SettlementKnown: true,
		SettlementValue: executionShadowFloat(1), SettledAt: now.Add(2350 * time.Millisecond),
		SettlementSource: "test-settlement", SettlementHash: "hash-exec-not-observed",
	})
	settleShadow("exec-zero-post-settlement", "exec-zero",
		now.Add(2375*time.Millisecond), 0)
	settleShadow("exec-fill-post-settlement", "exec-fill",
		now.Add(2400*time.Millisecond), .55)
	if open, err = st.ListOpenExecutionShadowAttempts(ctx, 10); err != nil || len(open) != 0 {
		t.Fatalf("post-execution Shadow settlement remained open: len=%d err=%v", len(open), err)
	}
}

func TestExecutionShadowCounterfactualExecutionPendingIsBoundedJoinedAndFillFirst(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	insertAttempt := func(id string, observedAt time.Time) {
		t.Helper()
		attempt := ExecutionShadowAttempt{
			AttemptID: id, SignalDecisionID: "decision-" + id,
			ObservedAt: observedAt, TriggerUnixMS: observedAt.UnixMilli(),
			Venue: "kalshi", Ticker: "KX-" + id, Side: "YES", Action: "BUY",
			SystemID: "spotlag", Route: "taker", SignalSource: "spotlag",
			QualificationBasis: "counterfactual recovery test",
		}
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("insert %s inserted=%v err=%v", id, inserted, insertErr)
		}
	}
	appendEvent := func(event ExecutionShadowEvent) {
		t.Helper()
		if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil || !inserted {
			t.Fatalf("append %s inserted=%v err=%v", event.EventID, inserted, appendErr)
		}
	}

	insertAttempt("older-fill", now.Add(-3*time.Second))
	appendEvent(ExecutionShadowEvent{
		EventID: "older-fill-gate", AttemptID: "older-fill", At: now.Add(-2900 * time.Millisecond),
		ElapsedFromTriggerMS: 100, Stage: "live-first-preflight", Outcome: "passed",
		Evidence: map[string]any{"prospective_qty": float64(1)},
	})
	appendEvent(ExecutionShadowEvent{
		EventID: "older-fill-live", AttemptID: "older-fill", At: now.Add(-2800 * time.Millisecond),
		ElapsedFromTriggerMS: 200, Stage: "live-terminal", Outcome: "filled",
		LiveState: "filled", VenueAttempted: true, LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(1), LiveFillPrice: executionShadowFloat(.40),
		LiveFee: executionShadowFloat(.01), LiveFeeSource: "test-exact-fee",
	})

	insertAttempt("newer-zero", now.Add(-time.Second))
	appendEvent(ExecutionShadowEvent{
		EventID: "newer-zero-gate", AttemptID: "newer-zero", At: now.Add(-950 * time.Millisecond),
		ElapsedFromTriggerMS: 50, Stage: "live-first-preflight", Outcome: "passed",
		Evidence: map[string]any{"prospective_qty": float64(1)},
	})
	appendEvent(ExecutionShadowEvent{
		EventID: "newer-zero-live", AttemptID: "newer-zero", At: now.Add(-900 * time.Millisecond),
		ElapsedFromTriggerMS: 100, Stage: "live-terminal", Outcome: "zero-fill",
		LiveState: "canceled", VenueAttempted: true, LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(0),
	})

	insertAttempt("completed-fill", now.Add(-2*time.Second))
	appendEvent(ExecutionShadowEvent{
		EventID: "completed-live", AttemptID: "completed-fill", At: now.Add(-1900 * time.Millisecond),
		ElapsedFromTriggerMS: 100, Stage: "live-terminal", Outcome: "filled",
		LiveState: "filled", VenueAttempted: true, LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(1), LiveFillPrice: executionShadowFloat(.42),
		LiveFee: executionShadowFloat(.01), LiveFeeSource: "test-exact-fee",
	})
	appendEvent(ExecutionShadowEvent{
		EventID: "completed-shadow", AttemptID: "completed-fill", At: now.Add(-1800 * time.Millisecond),
		ElapsedFromTriggerMS: 200, Stage: "counterfactual-execution-terminal", Outcome: "filled",
		ShadowState: "filled", ShadowFilledQty: executionShadowFloat(1),
		ShadowFillPrice: executionShadowFloat(.42), ShadowFee: executionShadowFloat(.01),
		ShadowFeeSource: "test-exact-fee",
	})

	insertAttempt("no-live", now)
	appendEvent(ExecutionShadowEvent{
		EventID: "no-live-gate", AttemptID: "no-live", At: now,
		ElapsedFromTriggerMS: 0, Stage: "live-first-preflight", Outcome: "passed",
	})

	pending, err := st.ListExecutionShadowCounterfactualExecutionPending(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending len=%d, want 2: %+v", len(pending), pending)
	}
	if pending[0].Attempt.AttemptID != "older-fill" ||
		pending[1].Attempt.AttemptID != "newer-zero" {
		t.Fatalf("pending order=%q,%q, want filled first then zero-fill",
			pending[0].Attempt.AttemptID, pending[1].Attempt.AttemptID)
	}
	if len(pending[0].Events) != 2 ||
		pending[0].Events[0].EventID != "older-fill-gate" ||
		pending[0].Events[1].EventID != "older-fill-live" {
		t.Fatalf("recovery history was not complete/in event order: %+v", pending[0].Events)
	}

	limited, err := st.ListExecutionShadowCounterfactualExecutionPending(ctx, 1)
	if err != nil || len(limited) != 1 || limited[0].Attempt.AttemptID != "older-fill" {
		t.Fatalf("bounded pending=%+v err=%v", limited, err)
	}
}

func TestExecutionShadowFundedPaperGapsTreatsDroppedAsRecoverable(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	observed := make(map[string]time.Time)

	insertAttempt := func(id string, observedAt time.Time) {
		t.Helper()
		observed[id] = observedAt
		attempt := ExecutionShadowAttempt{
			AttemptID: id, SignalDecisionID: "decision-" + id,
			ObservedAt: observedAt, TriggerUnixMS: observedAt.UnixMilli(),
			Venue: "kalshi", Ticker: "KX-" + id, Side: "NO", Action: "BUY",
			SystemID: "kalshi-flow", Route: "taker", SignalSource: "kalshi-flow",
			QualificationBasis: "funded Paper recovery test",
		}
		if inserted, insertErr := st.InsertExecutionShadowAttempt(ctx, attempt); insertErr != nil || !inserted {
			t.Fatalf("insert %s inserted=%v err=%v", id, inserted, insertErr)
		}
	}
	appendEvent := func(event ExecutionShadowEvent) {
		t.Helper()
		if inserted, appendErr := st.AppendExecutionShadowEvent(ctx, event); appendErr != nil || !inserted {
			t.Fatalf("append %s inserted=%v err=%v", event.EventID, inserted, appendErr)
		}
	}
	addOfferedAndLive := func(id string, filled bool) {
		t.Helper()
		base := observed[id]
		appendEvent(ExecutionShadowEvent{
			EventID: id + "-offered", AttemptID: id, At: base.Add(10 * time.Millisecond),
			ElapsedFromTriggerMS: 10, Stage: "funded-paper-branch", Outcome: "offered",
			Evidence: map[string]any{"paper_signal": map[string]any{"ticker": "KX-" + id}},
		})
		live := ExecutionShadowEvent{
			EventID: id + "-live", AttemptID: id, At: base.Add(11 * time.Millisecond),
			ElapsedFromTriggerMS: 11, Stage: "live-terminal", Outcome: "zero-fill",
			LiveState: "canceled", VenueAttempted: true, LiveAuthoritative: true,
			LiveFilledQty: executionShadowFloat(0),
		}
		if filled {
			live.Outcome, live.LiveState = "filled", "filled"
			live.LiveFilledQty = executionShadowFloat(1)
			live.LiveFillPrice = executionShadowFloat(.45)
			live.LiveFee = executionShadowFloat(.01)
			live.LiveFeeSource = "test-exact-fee"
		}
		appendEvent(live)
	}

	insertAttempt("dropped-fill", now.Add(-3*time.Second))
	addOfferedAndLive("dropped-fill", true)
	appendEvent(ExecutionShadowEvent{
		EventID: "dropped-fill-paper", AttemptID: "dropped-fill",
		At:                   observed["dropped-fill"].Add(12 * time.Millisecond),
		ElapsedFromTriggerMS: 12, Stage: "funded-paper-terminal", Outcome: "paper-dropped",
		PaperState: "PAPER-DROPPED", Reason: "bounded-paper-worker-queue-full",
	})

	insertAttempt("open-zero", now.Add(-time.Second))
	addOfferedAndLive("open-zero", false)

	insertAttempt("paper-filled", now.Add(-2*time.Second))
	addOfferedAndLive("paper-filled", true)
	appendEvent(ExecutionShadowEvent{
		EventID: "paper-filled-terminal", AttemptID: "paper-filled",
		At:                   observed["paper-filled"].Add(12 * time.Millisecond),
		ElapsedFromTriggerMS: 12, Stage: "funded-paper-terminal", Outcome: "paper-filled",
		PaperState: "PAPER-FILLED", PaperFilledQty: executionShadowFloat(1),
		PaperFillPrice: executionShadowFloat(.45), PaperFee: executionShadowFloat(.01),
		PaperFeeSource: "test-exact-fee",
	})

	insertAttempt("paper-other-terminal", now.Add(-4*time.Second))
	addOfferedAndLive("paper-other-terminal", false)
	appendEvent(ExecutionShadowEvent{
		EventID: "paper-other-terminal-event", AttemptID: "paper-other-terminal",
		At: observed["paper-other-terminal"].Add(12 * time.Millisecond), ElapsedFromTriggerMS: 12,
		Stage: "funded-paper-terminal", Outcome: "paper-skipped",
		PaperState: "PAPER-SKIPPED",
	})

	insertAttempt("offered-no-live", now)
	appendEvent(ExecutionShadowEvent{
		EventID: "offered-no-live-event", AttemptID: "offered-no-live", At: now,
		ElapsedFromTriggerMS: 0, Stage: "funded-paper-branch", Outcome: "offered",
	})

	insertAttempt("live-no-offer", now.Add(time.Second))
	appendEvent(ExecutionShadowEvent{
		EventID: "live-no-offer-event", AttemptID: "live-no-offer", At: now.Add(time.Second),
		ElapsedFromTriggerMS: 0, Stage: "live-terminal", Outcome: "zero-fill",
		LiveState: "canceled", VenueAttempted: true, LiveAuthoritative: true,
		LiveFilledQty: executionShadowFloat(0),
	})

	gaps, err := st.ListExecutionShadowFundedPaperGaps(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(gaps) != 2 {
		t.Fatalf("gap len=%d, want dropped plus never-run: %+v", len(gaps), gaps)
	}
	if gaps[0].Attempt.AttemptID != "dropped-fill" ||
		gaps[1].Attempt.AttemptID != "open-zero" {
		t.Fatalf("gap order=%q,%q, want filled LIVE first then zero-fill",
			gaps[0].Attempt.AttemptID, gaps[1].Attempt.AttemptID)
	}
	if len(gaps[0].Events) != 3 ||
		gaps[0].Events[0].EventID != "dropped-fill-offered" ||
		gaps[0].Events[2].PaperState != "PAPER-DROPPED" {
		t.Fatalf("dropped recovery history incomplete: %+v", gaps[0].Events)
	}

	limited, err := st.ListExecutionShadowFundedPaperGaps(ctx, 1)
	if err != nil || len(limited) != 1 || limited[0].Attempt.AttemptID != "dropped-fill" {
		t.Fatalf("bounded gaps=%+v err=%v", limited, err)
	}
}

func executionShadowRecoveryQueryPlan(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details = append(details, detail)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(details, "\n")
}

func TestExecutionShadowRecoveryQueriesUseSparseSeedFirstPlans(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	db := st.ExecutionShadowDBForTest()
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "execution comparison",
			query: executionShadowCounterfactualExecutionPendingQuery(),
			want: []string{
				"idx_execution_shadow_events_recovery_seed",
				"idx_execution_shadow_events_live_terminal",
				"idx_execution_shadow_events_attempt_stage_outcome",
				"idx_execution_shadow_events_settlement",
				"idx_execution_shadow_events_live_fill",
			},
		},
		{
			name:  "funded Paper",
			query: executionShadowFundedPaperGapsQuery(),
			want: []string{
				"idx_execution_shadow_events_recovery_seed",
				"idx_execution_shadow_events_live_terminal",
				"idx_execution_shadow_events_attempt_stage_outcome",
				"idx_execution_shadow_events_live_fill",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := executionShadowRecoveryQueryPlan(t, db, tc.query)
			for _, index := range tc.want {
				if !strings.Contains(plan, index) {
					t.Fatalf("query plan omitted %s:\n%s", index, plan)
				}
			}
			if strings.Contains(plan, "SCAN a") ||
				strings.Contains(plan, "SCAN execution_shadow_attempts") {
				t.Fatalf("recovery regressed to a full attempt scan:\n%s", plan)
			}
			if !strings.Contains(plan, "MATERIALIZE seed") {
				t.Fatalf("rare recovery seed was not materialized first:\n%s", plan)
			}
		})
	}
}

// Set KALSHI_EXECUTION_SHADOW_PERF_DB to a copied, WAL-complete execution_shadow.db to run the
// production-scale guard. It is opt-in because the operator ledger is hundreds of megabytes; the
// ordinary query-plan test above pins the same sparse-seed plan on every test run.
func TestExecutionShadowRecoveryQueriesRealLedgerPerformance(t *testing.T) {
	fixture := strings.TrimSpace(os.Getenv("KALSHI_EXECUTION_SHADOW_PERF_DB"))
	if fixture == "" {
		t.Skip("set KALSHI_EXECUTION_SHADOW_PERF_DB to a copied execution_shadow.db")
	}
	absolute, err := filepath.Abs(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(absolute) != ExecutionShadowFileName {
		t.Fatalf("fixture must be named %s: %s", ExecutionShadowFileName, absolute)
	}
	db, _, err := openExecutionShadowDB(filepath.Dir(absolute))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	st := &Store{executionShadowDB: db}
	var attempts, events int64
	if err = db.QueryRow(`SELECT COUNT(*) FROM execution_shadow_attempts`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT COUNT(*) FROM execution_shadow_events`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if attempts < 100_000 || events < 200_000 {
		t.Fatalf("fixture is not production-scale: attempts=%d events=%d", attempts, events)
	}
	ctx := context.Background()
	check := func(name string, run func() error) {
		t.Helper()
		var worst time.Duration
		for i := 0; i < 5; i++ {
			started := time.Now()
			if err := run(); err != nil {
				t.Fatalf("%s query: %v", name, err)
			}
			if elapsed := time.Since(started); elapsed > worst {
				worst = elapsed
			}
		}
		t.Logf("%s attempts=%d events=%d worst_of_5=%s",
			name, attempts, events, worst)
		if worst > 2*time.Second {
			t.Fatalf("%s recovery query took %s; want <=2s", name, worst)
		}
	}
	check("execution comparison", func() error {
		_, err := st.ListExecutionShadowCounterfactualExecutionPending(ctx, 64)
		return err
	})
	check("funded Paper", func() error {
		_, err := st.ListExecutionShadowFundedPaperGaps(ctx, 64)
		return err
	})
}
