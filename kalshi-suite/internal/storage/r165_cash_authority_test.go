package storage

import "testing"

func TestR165ResearchLiveReceiptClassRequiresAuthenticatedTerminalTruth(t *testing.T) {
	full := ResearchLiveExecutionReceipt{
		State: "full", RequestedQty: 1, FilledQty: 1, FeeKnown: true,
		Authoritative: true, ReceiptSource: "kalshi-create-order-receipt",
	}
	if got := researchLiveExecutionReceiptClass(full); got != "fill" {
		t.Fatalf("authoritative fill class=%q", got)
	}
	nonauth := full
	nonauth.Authoritative = false
	if got := researchLiveExecutionReceiptClass(nonauth); got != "incomplete" {
		t.Fatalf("non-authoritative fill class=%q", got)
	}
	unknownFee := full
	unknownFee.FeeKnown = false
	if got := researchLiveExecutionReceiptClass(unknownFee); got != "incomplete" {
		t.Fatalf("fee-unknown fill class=%q", got)
	}
	zero := ResearchLiveExecutionReceipt{
		State: "unfilled", RequestedQty: 1, Authoritative: true,
		ReceiptSource: "kalshi-order-terminal",
	}
	if got := researchLiveExecutionReceiptClass(zero); got != "zero-fill" {
		t.Fatalf("authoritative zero-fill class=%q", got)
	}
	fake := full
	fake.ReceiptSource = "paper-model"
	if got := researchLiveExecutionReceiptClass(fake); got != "" {
		t.Fatalf("Paper/model receipt became exchange evidence: %q", got)
	}
}

func TestR165ResearchLiveCohortMustBeNonemptyAndFullyTerminal(t *testing.T) {
	if (ResearchLiveExecutionCohort{}).Ready() {
		t.Fatal("empty cohort authorized cash")
	}
	if (ResearchLiveExecutionCohort{Attempts: 2, Terminal: 1, Filled: 1, Incomplete: 1}).Ready() {
		t.Fatal("incomplete exchange cohort authorized cash")
	}
	if !(ResearchLiveExecutionCohort{Attempts: 2, Terminal: 2, Filled: 1, ZeroFilled: 1}).Ready() {
		t.Fatal("fully authenticated terminal cohort was not recognized")
	}
}
