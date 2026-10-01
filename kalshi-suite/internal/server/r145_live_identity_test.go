package server

import (
	"context"
	"testing"
	"time"

	"github.com/kalshi-suite/kalshi-suite/internal/storage"
)

func TestR145SealedLiveRouteRequiresCurrentCanonicalEventAndPayoffVersions(t *testing.T) {
	s := testServer(t)
	ctx := context.Background()
	eventID, payoffID, ticker := "event:r145-live", "payoff:r145-live", "KXR145LIVE"
	_, err := s.store.RegisterCanonicalBatch(ctx, []storage.CanonicalEventSpec{{
		EventID: eventID, EventType: "fixture", Domain: "test", Title: "R145 live identity",
		SourceArtifact: "fixture", SourceClockID: "fixture", OutcomeSetStatus: "binary",
	}}, []storage.CanonicalPayoffSpec{{
		PayoffID: payoffID, EventID: eventID, Label: "YES", PredicateJSON: `{"yes":true}`,
		PayoutFloor: 0, PayoutCeiling: 1, SourceArtifact: "fixture", IdentityStatus: "verified",
	}}, []storage.CanonicalInstrumentSpec{{
		Venue: "kalshi", Ticker: ticker, EventID: eventID, PayoffID: payoffID,
		NativeSide: "YES", Orientation: "same", RulesArtifact: "fixture", IdentityStatus: "verified",
	}})
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := s.store.CurrentCanonicalInstrument(ctx, "kalshi", ticker)
	if err != nil || !found {
		t.Fatalf("current=%+v found=%v err=%v", current, found, err)
	}
	in := storage.ResearchPromotionIntent{Candidate: storage.ResearchPromotionCandidate{
		Venue: "kalshi", Ticker: ticker, CanonicalEventID: current.EventID,
		EventVersion: current.EventVersion, CanonicalPayoffID: current.PayoffID,
		PayoffVersion: current.PayoffVersion, Observed: time.Now(),
	}}
	if why := researchPaperCandidateFreshReason(in.Candidate, time.Now()); why != "" {
		t.Fatalf("fresh candidate rejected: %s", why)
	}
	stale := in.Candidate
	stale.Observed = time.Now().Add(-researchPaperCandidateMaxAge - time.Second)
	if why := researchPaperCandidateFreshReason(stale, time.Now()); why != "candidate-stale" {
		t.Fatalf("stale candidate reason=%q", why)
	}
	if why := s.liveAutoPromotionCurrentIdentityReason(ctx, in); why != "" {
		t.Fatalf("exact current identity rejected: %s", why)
	}
	in.Candidate.EventVersion--
	if why := s.liveAutoPromotionCurrentIdentityReason(ctx, in); why != "canonical-event-version-changed" {
		t.Fatalf("stale event version reason=%q", why)
	}
	in.Candidate.EventVersion = current.EventVersion
	in.Candidate.PayoffVersion--
	if why := s.liveAutoPromotionCurrentIdentityReason(ctx, in); why != "canonical-payoff-version-changed" {
		t.Fatalf("stale payoff version reason=%q", why)
	}
}
