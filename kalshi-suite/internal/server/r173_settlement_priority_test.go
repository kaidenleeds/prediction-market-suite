package server

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR173FundedPaperSettlementPriorityBypassesHistoricalCursorBacklog(t *testing.T) {
	s, st := newExecutionShadowTestServer(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)

	// Fill the fair settlement page with older unresolved attempts. Without the funded-close
	// priority lane, the target immediately after this prefix cannot be reached in one pass.
	for i := 0; i < executionShadowSettlementBatch; i++ {
		attemptID := fmt.Sprintf("exec-r173-prefix-%04d", i)
		at := now.Add(time.Duration(i) * time.Millisecond)
		attempt := storage.ExecutionShadowAttempt{
			AttemptID: attemptID, SignalDecisionID: "decision-" + attemptID,
			ObservedAt: at, TriggerUnixMS: at.UnixMilli(), Venue: "kalshi",
			Ticker: fmt.Sprintf("KXR173-PREFIX-%04d", i), Side: "YES", Action: "BUY",
			SystemID: "pbridge", Route: "taker", SignalSource: "r173-test",
			QualificationBasis: "unresolved historical prefix",
		}
		if inserted, err := st.InsertExecutionShadowAttempt(ctx, attempt); err != nil || !inserted {
			t.Fatalf("insert prefix %d inserted=%v err=%v", i, inserted, err)
		}
		terminal := storage.ExecutionShadowEvent{
			EventID: attemptID + "-terminal", AttemptID: attemptID,
			At: at.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
			Stage: "counterfactual-execution-terminal", Outcome: "modeled_fill",
			ShadowState: "modeled_fill", ShadowFilledQty: floatPtrShadow(1),
			ShadowFillPrice: floatPtrShadow(.40), ShadowFee: floatPtrShadow(.01),
			ShadowFeeSource: "r173 exact test fee",
		}
		if inserted, err := st.AppendExecutionShadowEvent(ctx, terminal); err != nil || !inserted {
			t.Fatalf("append prefix %d inserted=%v err=%v", i, inserted, err)
		}
	}

	targetID, targetTicker := "exec-r173-funded-priority", "KXR173-FUNDED-PRIORITY"
	// Keep the synthetic trigger in the causal past. The 500-row fixture can build in less than a
	// second on a warm cache; anchoring this to now+1s made the derived settlement correctly fail
	// its negative elapsed-time guard on fast/full-suite runs.
	targetAt := time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond)
	resolvedAt := targetAt.Add(500 * time.Millisecond)
	target := storage.ExecutionShadowAttempt{
		AttemptID: targetID, SignalDecisionID: "decision-" + targetID,
		ObservedAt: targetAt, TriggerUnixMS: targetAt.UnixMilli(), Venue: "kalshi",
		Ticker: targetTicker, Side: "YES", Action: "BUY", SystemID: "pbridge",
		Route: "taker", SignalSource: "r173-test",
		QualificationBasis: "durable funded Paper close",
	}
	if inserted, err := st.InsertExecutionShadowAttempt(ctx, target); err != nil || !inserted {
		t.Fatalf("insert target inserted=%v err=%v", inserted, err)
	}
	paperFill := storage.ExecutionShadowEvent{
		EventID: targetID + "-paper-fill", AttemptID: targetID,
		At: targetAt.Add(time.Millisecond), ElapsedFromTriggerMS: 1,
		Stage: "funded-paper-terminal", Outcome: "paper-filled",
		PaperState: "PAPER-FILLED", PaperFilledQty: floatPtrShadow(1),
		PaperFillPrice: floatPtrShadow(.40), PaperFee: floatPtrShadow(.01),
		PaperFeeSource: "r173 exact test fee",
	}
	if inserted, err := st.AppendExecutionShadowEvent(ctx, paperFill); err != nil || !inserted {
		t.Fatalf("append target fill inserted=%v err=%v", inserted, err)
	}
	if inserted, err := st.RecordVenueSettlement(ctx, "kalshi", targetTicker, 0,
		resolvedAt, "r173 authoritative final"); err != nil || !inserted {
		t.Fatalf("record target settlement inserted=%v err=%v", inserted, err)
	}

	s.gfBookMu.Lock()
	s.gfBook = &gfBookState{Subs: map[string]*kfBook{
		"gf:pbridge:yes:taker-k": {Closed: []kfClosed{
			{kfPos: kfPos{ExecutionShadowAttemptID: targetID},
				SettledTS: resolvedAt.Format(time.RFC3339Nano), Reason: "settled"},
			{kfPos: kfPos{ExecutionShadowAttemptID: "exec-r173-stop-is-not-settlement"},
				SettledTS: targetAt.Format(time.RFC3339Nano), Reason: "stop-loss"},
		}},
	}}
	s.gfBookMu.Unlock()

	priorityIDs := s.priorityExecutionShadowFundedSettlementAttempts()
	if len(priorityIDs) != 1 || priorityIDs[0] != targetID {
		t.Fatalf("funded settlement priority ids=%v, want [%s]", priorityIDs, targetID)
	}
	before, err := st.ExecutionShadowAttemptsByIDs(ctx, priorityIDs)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, _, pq, pp, pf, paperKnown, _, _, _, _ :=
		executionShadowLatestFills(before[targetID].Events)
	if !paperKnown || pq != 1 || pp != .40 || pf != .01 {
		t.Fatalf("target canonical Paper fill missing before priority join: %+v", before[targetID])
	}
	receipts, err := st.VenueSettlementsForTickers(ctx, "kalshi", []string{targetTicker})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := receipts[targetTicker]; !ok {
		t.Fatalf("authoritative target settlement missing before priority join: %+v", receipts)
	}

	s.settleExecutionShadowLedger(ctx)
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	if err := s.stopExecutionShadowWriter(drainCtx); err != nil {
		cancelDrain()
		t.Fatalf("settlement writer drain: %v", err)
	}
	cancelDrain()

	histories, err := st.ExecutionShadowAttemptsByIDs(ctx, []string{targetID})
	if err != nil {
		t.Fatal(err)
	}
	var paperNet *float64
	for _, event := range histories[targetID].Events {
		if event.SettlementKnown && event.PaperNet != nil {
			paperNet = event.PaperNet
		}
	}
	if paperNet == nil || math.Abs(*paperNet-(-.41)) > 1e-9 {
		t.Fatalf("priority funded settlement paper net=%v, want -0.41; events=%+v dropped=%d invalid=%d stopping=%d capacity=%d orphan=%d rerouted=%d",
			paperNet, histories[targetID].Events, s.executionShadowDropped.Load(),
			s.executionShadowDropInvalid.Load(), s.executionShadowDropStopping.Load(),
			s.executionShadowDropCapacity.Load(), s.executionShadowDropOrphan.Load(),
			s.executionShadowRerouted.Load())
	}
	pendingIDs, err := st.ExecutionShadowFundedSettlementPendingIDs(ctx, priorityIDs)
	if err != nil {
		t.Fatal(err)
	}
	if len(pendingIDs) != 0 {
		t.Fatalf("settled funded Paper close remained in steady-state priority work: %v", pendingIDs)
	}
}
