package storage

import (
	"context"
	"testing"
)

func TestSemanticBasisInputsReturnsLatestCrossVenueImmutableVersions(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	e := CanonicalEventSpec{EventID: "sem-event", EventType: "sports", Domain: "sports", Title: "x",
		OutcomeSetStatus: "complete", SourceClockID: "clock", SettlementSource: "source"}
	p := CanonicalPayoffSpec{PayoffID: "sem-pay", EventID: e.EventID, Label: "yes", PredicateJSON: `{}`,
		PayoutCeiling: 1, SettlementSource: "source", IdentityStatus: "verified"}
	i := CanonicalInstrumentSpec{Venue: "kalshi", Ticker: "SEM-K", EventID: e.EventID, PayoffID: p.PayoffID,
		NativeSide: "YES", Orientation: "same", SettlementSource: "source", RulesHash: "rules",
		FeeAuthority: "fee", CrossVenueBasis: "basis", IdentityStatus: "verified"}
	j := i
	j.Venue, j.Ticker = "polyus", "SEM-P"
	if _, err := st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{e}, []CanonicalPayoffSpec{p}, []CanonicalInstrumentSpec{i, j}); err != nil {
		t.Fatal(err)
	}
	instruments, relations, err := st.SemanticBasisInputs(ctx, 10)
	if err != nil || len(instruments) != 2 || len(relations) != 0 || instruments[0].PayoffVersion != 1 {
		t.Fatalf("instruments=%+v relations=%+v err=%v", instruments, relations, err)
	}
}

func TestSemanticBasisInputsExcludesVenueLocalCatalogUniverse(t *testing.T) {
	st, _ := openTemp(t)
	ctx := context.Background()
	e := CanonicalEventSpec{EventID: "only-kalshi", EventType: "venue-event", Domain: "kalshi", Title: "x",
		OutcomeSetStatus: "unknown", SourceClockID: "clock"}
	p := CanonicalPayoffSpec{PayoffID: "only-kalshi-pay", EventID: e.EventID, Label: "yes", PredicateJSON: `{}`,
		PayoutCeiling: 1, IdentityStatus: "unverified"}
	i := CanonicalInstrumentSpec{Venue: "kalshi", Ticker: "ONLY-K", EventID: e.EventID, PayoffID: p.PayoffID,
		NativeSide: "YES", Orientation: "unknown", IdentityStatus: "unverified"}
	if _, err := st.RegisterCanonicalBatch(ctx, []CanonicalEventSpec{e}, []CanonicalPayoffSpec{p}, []CanonicalInstrumentSpec{i}); err != nil {
		t.Fatal(err)
	}
	instruments, relations, err := st.SemanticBasisInputs(ctx, 10)
	if err != nil || len(instruments) != 0 || len(relations) != 0 {
		t.Fatalf("venue-local rows leaked into semantic router: instruments=%+v relations=%+v err=%v", instruments, relations, err)
	}
}
