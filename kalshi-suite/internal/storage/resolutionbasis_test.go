package storage

import (
	"context"
	"testing"
)

func TestResolutionBasisInputsUseExactImmutableVersionsAndFailClosedOnConflict(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	event := CanonicalEventSpec{EventID: "event-1", EventType: "sports-game", Domain: "nba",
		SettlementSource: "official-box-score", VoidPolicy: "refund-0.5", OutcomeSetStatus: "complete",
		EvidenceJSON: `{"scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`}
	payoff := CanonicalPayoffSpec{PayoffID: "payoff-1", EventID: event.EventID, Label: "home wins",
		PredicateJSON: `{"winner":"home"}`, PayoutCeiling: 1, SettlementSource: "official-box-score",
		IdentityStatus: "verified", EvidenceJSON: `{"scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`}
	instruments := []CanonicalInstrumentSpec{
		{Venue: "kalshi", Ticker: "KX-1", EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", SettlementSource: "official-box-score",
			RulesHash: "rules-1", CrossVenueBasis: "basis-1", IdentityStatus: "verified",
			EvidenceJSON: `{"void_policy":"refund-0.5","scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`},
		{Venue: "polyus", Ticker: "pus-1", EventID: event.EventID, PayoffID: payoff.PayoffID,
			NativeSide: "YES", Orientation: "same", SettlementSource: "official-box-score",
			RulesHash: "rules-1", CrossVenueBasis: "basis-1", IdentityStatus: "verified",
			EvidenceJSON: `{"void_policy":"refund-0.5","scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`},
	}
	if _, err := st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{event}, []CanonicalPayoffSpec{payoff}, instruments); err != nil {
		t.Fatal(err)
	}
	got, err := st.ResolutionBasisInputs(ctx, 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("inputs=%+v err=%v", got, err)
	}
	left := got[ResolutionBasisKey("KALSHI", "KX-1")]
	if left.InstrumentVersion != 1 || left.EventVersion != 1 || left.PayoffVersion != 1 ||
		left.IdentityStatus != "verified" || left.SettlementSource != "official-box-score" ||
		left.VoidPolicy != "refund-0.5" || left.ScalarPolicy != "binary-or-refund" ||
		left.UnknownPolicy != "refund-0.5" {
		t.Fatalf("complete immutable receipt=%+v", left)
	}

	// A venue-specific policy disagreement appends a new immutable instrument version. The newest
	// receipt must expose the disagreement as an empty term, never choose one side.
	bad := instruments[0]
	bad.EvidenceJSON = `{"void_policy":"cancel-as-loss","scalar_policy":"binary-or-refund","unknown_policy":"refund-0.5"}`
	if _, err := st.RegisterCanonicalBatch(ctx, nil, nil, []CanonicalInstrumentSpec{bad}); err != nil {
		t.Fatal(err)
	}
	got, err = st.ResolutionBasisInputs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	left = got[ResolutionBasisKey("kalshi", "KX-1")]
	if left.InstrumentVersion != 2 || left.VoidPolicy != "" {
		t.Fatalf("conflicting newest immutable receipt did not fail closed: %+v", left)
	}
	targeted, err := st.ResolutionBasisInputsForKeys(ctx, []ResolutionBasisInstrumentKey{
		{Venue: " KALSHI ", Ticker: "KX-1"}, {Venue: "kalshi", Ticker: "KX-1"},
		{Venue: "missing", Ticker: "none"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(targeted) != 1 || targeted[ResolutionBasisKey("kalshi", "KX-1")].InstrumentVersion != 2 {
		t.Fatalf("targeted newest-version read=%+v", targeted)
	}
}
