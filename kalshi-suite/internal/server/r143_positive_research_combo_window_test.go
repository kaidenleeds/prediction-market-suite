package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR143PositiveResearchLegSurvivesDeferredComboTick(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	now := time.Now().UTC()
	eventID, payoffID := "event:r143-combo-window", "payoff:r143-combo-window:YES"
	if _, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{
		EventID: eventID, EventType: "fixture", Domain: "test", Title: "R143 combo window",
		SourceArtifact: "fixture", SourceClockID: "fixture-clock", OutcomeSetStatus: "complete",
	}}, []storage.CanonicalPayoffSpec{{
		PayoffID: payoffID, EventID: eventID, Label: "YES", PredicateJSON: `{"fixture":true}`,
		PayoutFloor: 0, PayoutCeiling: 1, SourceArtifact: "fixture", IdentityStatus: "verified",
	}}, []storage.CanonicalInstrumentSpec{{
		Venue: "kalshi", Ticker: "KXR143WINDOW", EventID: eventID, PayoffID: payoffID,
		NativeSide: "YES", Orientation: "same", MarketKind: "binary", RulesArtifact: "fixture",
		FeeAuthority: "fixture-fee", IdentityStatus: "verified",
	}}); err != nil {
		t.Fatal(err)
	}
	identity, ok, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", "KXR143WINDOW")
	if err != nil || !ok {
		t.Fatalf("identity ok=%v err=%v", ok, err)
	}
	insert := func(observed time.Time, opportunity string) {
		t.Helper()
		_, inserted, insertErr := s.store.InsertResearchSystemObservation(ctx, storage.ResearchSystemObservation{
			Observed: observed, SystemID: "deadline-hazard-surface", OpportunityID: opportunity,
			Kind: "candidate", Cohort: "prospective", CanonicalEventID: identity.EventID,
			EventVersion: identity.EventVersion, CanonicalPayoffID: identity.PayoffID,
			PayoffVersion: identity.PayoffVersion, Venue: "kalshi", Ticker: "KXR143WINDOW",
			Route: "taker", Side: "YES", CertificateStatus: "verified",
			CertificateHash: strings.Repeat("a", 64), SourceClockID: "kalshi-orderbook-ws",
			SourceArtifact: "immutable fixture", BookSource: "kalshi_book_ws",
			FeeSource: "fixture-fee", QuoteAgeMax: .2, TickMin: .01, Size: 1,
			Cost: .40, Fee: .01, PayoutLower: .55, PayoutUpper: 1, NetLower: .14,
			NetUpper: .59, VisibleCapacity: 5, DecisionLatencyMS: 10,
			LatencyKnown: true, QuoteAgeKnown: true, TickKnown: true, DepthKnown: true,
			FeeKnown: true, Candidate: true, OutcomeStatus: "open",
			CapacityCurve: []storage.CapacityPoint{{Size: 1, Cost: .40, Fee: .01,
				PayoutFloor: .55, NetFloor: .14}}, Inputs: map[string]any{"promotion_action": "BUY"},
		})
		if insertErr != nil || !inserted {
			t.Fatalf("inserted=%v err=%v", inserted, insertErr)
		}
	}
	insert(now.Add(-5*time.Minute), "survives-one-deferred-cycle")
	insert(now.Add(-31*time.Minute), "outside-bounded-retry-window")

	legs := s.plabRollingPositiveResearchLegs(ctx)
	if len(legs) != 1 || legs[0].Ticker != "KXR143WINDOW" ||
		len(legs[0].PositiveSystems) != 1 || legs[0].PositiveSystems[0] != "deadline-hazard-surface" {
		t.Fatalf("expected only the 5m-old exact research leg, got %+v", legs)
	}
}
