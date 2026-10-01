package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func r167Float(v float64) *float64 { return &v }

func r167PaperProofFixture(id, system, venue, ticker, side string, qty, price, fee,
	ageMS float64) (storage.ExecutionShadowAttemptView, kfPos) {
	trigger := time.Now().UTC().Truncate(time.Millisecond)
	attempt := storage.ExecutionShadowAttempt{
		AttemptID: id, SignalDecisionID: "decision-" + id, ObservedAt: trigger,
		TriggerUnixMS: trigger.UnixMilli(), Venue: venue, Ticker: ticker, Title: "joined proof",
		Side: side, Action: "BUY", SystemID: system, Route: "taker", SignalSource: "test",
		SignalPrice: price, QualificationBasis: "joined proof fixture", CreatedAt: trigger,
	}
	paperAt := trigger.Add(time.Second)
	paper := storage.ExecutionShadowEvent{
		ID: 1, EventID: "paper-" + id, AttemptID: id, At: paperAt,
		ElapsedFromTriggerMS: paperAt.UnixMilli() - trigger.UnixMilli(),
		Stage:                "funded-paper-terminal", Outcome: "paper-filled",
		BookSource: "book-final-" + id, BookAgeMS: r167Float(ageMS),
		SideBid: r167Float(price - .01), SideAsk: r167Float(price), VisibleDepth: r167Float(qty),
		RequestedQty: r167Float(qty), PaperAttemptID: "paper-attempt-" + id,
		PaperState: "PAPER-FILLED", PaperFilledQty: r167Float(qty),
		PaperFillPrice: r167Float(price), PaperFee: r167Float(fee),
		PaperFeeSource: venue + ":exact-fee", PaperBookSource: "book-final-" + id,
	}
	liveAt := trigger.Add(1500 * time.Millisecond)
	live := storage.ExecutionShadowEvent{
		ID: 2, EventID: "live-" + id, AttemptID: id, At: liveAt,
		ElapsedFromTriggerMS: liveAt.UnixMilli() - trigger.UnixMilli(),
		Stage:                "live-terminal", Outcome: "filled", LiveReservationID: "risk-" + id,
		VenueAttempted: true, VenueAck: true, VenueOrderID: "order-" + id,
		LiveState: "filled", LiveAuthoritative: true, LiveFilledQty: r167Float(qty),
		LiveFillPrice: r167Float(price), LiveFee: r167Float(fee),
		LiveFeeSource: "order-scoped-terminal-fill", LiveReceiptSource: "order-scoped-fills",
	}
	lot := kfPos{Ticker: ticker, Side: side, Platform: venue, Price: price, Contracts: qty,
		Fee: fee, FillKind: "taker", FeeKnown: true, FeeSource: venue + ":exact-fee",
		SignalTS: trigger.Format(time.RFC3339Nano), FillTS: paperAt.Format(time.RFC3339Nano),
		RouteReason: "fam:" + system, ExecutionShadowAttemptID: id,
		ExecutionBookSource: paper.BookSource,
		// Deliberately false copied linkage: immutable events, not these display fields, decide.
		ExecutionGeneration: "forged", ExecutionTruthContract: "forged",
		ExecutionLiveTerminalKind: fundedPaperLiveTerminalZeroFill,
		ExecutionLiveTerminal:     "unfilled", ExecutionLiveTerminalID: "forged-terminal",
	}
	return storage.ExecutionShadowAttemptView{Attempt: attempt,
		Events: []storage.ExecutionShadowEvent{paper, live}}, lot
}

func r167InsertPaperProof(t *testing.T, s *Server, view storage.ExecutionShadowAttemptView) {
	t.Helper()
	ctx := context.Background()
	if inserted, err := s.store.InsertExecutionShadowAttempt(ctx, view.Attempt); err != nil || !inserted {
		t.Fatalf("insert proof attempt inserted=%v err=%v", inserted, err)
	}
	for _, event := range view.Events {
		event.ID = 0
		if inserted, err := s.store.AppendExecutionShadowEvent(ctx, event); err != nil || !inserted {
			t.Fatalf("append proof event %s inserted=%v err=%v", event.Stage, inserted, err)
		}
	}
}

func TestR167PaperEligibilityComesFromImmutableLedgerNotCopiedFields(t *testing.T) {
	s := testServer(t)
	view, lot := r167PaperProofFixture("joined", "spotlag", "kalshi", "KX-JOINED", "YES", 2, .4, .03, 50)
	r167InsertPaperProof(t, s, view)
	proofs, err := s.fundedPaperImmutableProofs(context.Background(), []kfPos{lot})
	if err != nil || !proofs.accepts(lot) {
		t.Fatalf("valid immutable join err=%v proofs=%+v", err, proofs)
	}
	forged := lot
	forged.ExecutionShadowAttemptID = "missing-ledger-attempt"
	forged.ExecutionGeneration = fundedPaperCorrectedExecutionGenerationV1
	forged.ExecutionTruthContract = fundedPaperLiveTruthContractV1
	forged.ExecutionLiveTerminalKind = fundedPaperLiveTerminalFill
	forged.ExecutionLiveTerminal, forged.ExecutionLiveTerminalID = "filled", "made-up"
	forgedProofs, err := s.fundedPaperImmutableProofs(context.Background(), []kfPos{forged})
	if err != nil || forgedProofs.accepts(forged) {
		t.Fatalf("forged copied fields entered review err=%v proofs=%+v", err, forgedProofs)
	}
	for name, edit := range map[string]func(*kfPos){
		"ticker":     func(p *kfPos) { p.Ticker = "KX-OTHER" },
		"system":     func(p *kfPos) { p.RouteReason = "fam:other" },
		"trigger":    func(p *kfPos) { p.SignalTS = view.Attempt.ObservedAt.Add(time.Millisecond).Format(time.RFC3339Nano) },
		"price":      func(p *kfPos) { p.Price += .01 },
		"quantity":   func(p *kfPos) { p.Contracts++ },
		"fee":        func(p *kfPos) { p.Fee += .01 },
		"fee source": func(p *kfPos) { p.FeeSource = "estimate" },
		"book":       func(p *kfPos) { p.ExecutionBookSource = "other-book" },
	} {
		changed := lot
		edit(&changed)
		if proofs.accepts(changed) {
			t.Fatalf("%s mismatch entered money review", name)
		}
	}
}

func TestR167PaperProofFailsClosedOnStaleMissingOrIncompleteEvidence(t *testing.T) {
	valid, _ := r167PaperProofFixture("complete", "spotlag", "kalshi", "KX-COMPLETE", "YES", 1, .4, .02,
		fundedPaperImmutableMaxBookAgeMSV1)
	if _, ok := fundedPaperImmutableProofFromView(valid); !ok {
		t.Fatal("exact preregistered quote-age boundary was rejected")
	}
	for name, edit := range map[string]func(*storage.ExecutionShadowAttemptView){
		"stale age": func(v *storage.ExecutionShadowAttemptView) {
			*v.Events[0].BookAgeMS = fundedPaperImmutableMaxBookAgeMSV1 + .001
		},
		"missing age":     func(v *storage.ExecutionShadowAttemptView) { v.Events[0].BookAgeMS = nil },
		"paper zero fill": func(v *storage.ExecutionShadowAttemptView) { v.Events[0].PaperState = "PAPER-ZERO-FILL" },
		"paper fee":       func(v *storage.ExecutionShadowAttemptView) { v.Events[0].PaperFee = nil },
		"paper book":      func(v *storage.ExecutionShadowAttemptView) { v.Events[0].PaperBookSource = "other" },
		"live authority":  func(v *storage.ExecutionShadowAttemptView) { v.Events[1].LiveAuthoritative = false },
		"live price":      func(v *storage.ExecutionShadowAttemptView) { v.Events[1].LiveFillPrice = nil },
		"live fee":        func(v *storage.ExecutionShadowAttemptView) { v.Events[1].LiveFee = nil },
		"live fee source": func(v *storage.ExecutionShadowAttemptView) { v.Events[1].LiveFeeSource = "" },
		"live order":      func(v *storage.ExecutionShadowAttemptView) { v.Events[1].VenueOrderID = "" },
		"live receipt":    func(v *storage.ExecutionShadowAttemptView) { v.Events[1].LiveReceiptSource = "" },
		"wrong action":    func(v *storage.ExecutionShadowAttemptView) { v.Attempt.Action = "SELL" },
	} {
		view, _ := r167PaperProofFixture("bad-"+name, "spotlag", "kalshi", "KX-BAD", "YES", 1, .4, .02, 50)
		edit(&view)
		if _, ok := fundedPaperImmutableProofFromView(view); ok {
			t.Fatalf("%s incomplete proof was accepted", name)
		}
	}
}

func TestR167ImmutableReaderFailureIsEmptyAndFailClosed(t *testing.T) {
	s := testServer(t)
	_, lot := r167PaperProofFixture("reader-failure", "spotlag", "kalshi", "KX-FAIL", "YES", 1, .4, .02, 50)
	s.store = nil
	proofs, err := s.fundedPaperImmutableProofs(context.Background(), []kfPos{lot})
	if err == nil || len(proofs) != 0 || proofs.accepts(lot) {
		t.Fatalf("reader failure did not fail closed: err=%v proofs=%+v", err, proofs)
	}
}
